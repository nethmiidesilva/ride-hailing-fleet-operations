"""Streaming source: publishes vehicle telemetry to Kafka (ingestion, 15 marks).

Responsibilities
----------------
1. Drive :class:`producers.fleet_simulator.FleetSimulator` at ``EMIT_INTERVAL_SEC`` and publish
   one event per vehicle per tick to ``fleet.telemetry``.
2. Key every message by ``vehicle_id``.  This is the single most important ingestion decision in
   the project: Kafka guarantees ordering *within a partition*, so keying by vehicle guarantees
   that a given vehicle's ``idle -> enroute -> on_trip -> idle`` sequence can never be reordered,
   while still spreading 25 vehicles across 6 partitions for parallelism.
3. Inject controlled faults (malformed, late and duplicate events) so the downstream validation,
   watermark and dedup logic has something real to do.
4. Be robust: idempotent producer, ``acks=all``, retries, delivery callbacks, reconnect/backoff
   while Kafka is still starting, and a flush on SIGTERM so nothing is lost on ``docker stop``.
5. Expose Prometheus metrics on port 8001 and emit structured JSON logs.

Run with ``python -m producers.gps_producer``.
"""

from __future__ import annotations

import functools
import json
import signal
import sys
import time
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaException, Producer

from common.config import CFG
from common.logging_setup import RUN_ID, setup_logging
from common.metrics import (
    PRODUCER_BAD_EVENTS,
    PRODUCER_EVENTS_SENT,
    PRODUCER_LAST_SEND,
    PRODUCER_REGISTRY,
    PRODUCER_SEND_ERRORS,
    serve_metrics,
)
from common.sim_clock import get_epoch, sim_now
from producers.fleet_simulator import FleetSimulator

LOG = setup_logging("gps-producer", "ingestion")

#: Set by the SIGTERM/SIGINT handler; the main loop checks it every tick.
_SHUTDOWN = False


def _handle_signal(signum: int, _frame: Any) -> None:
    """Request a graceful stop so the final flush can run (no data loss on `docker stop`)."""
    global _SHUTDOWN
    _SHUTDOWN = True
    LOG.info(
        "shutdown requested",
        extra={"event": "shutdown_signal", "signal": signal.Signals(signum).name},
    )


def build_producer() -> Producer:
    """Create a librdkafka producer configured for durability over raw throughput.

    * ``acks=all`` — the leader waits for the in-sync replicas before acknowledging.  With one
      broker that is a single replica, but the setting is what a production cluster needs and it
      documents the intent.
    * ``enable.idempotence=true`` — librdkafka attaches a producer id and sequence number so a
      retry after a network blip cannot create a duplicate.  Combined with the stream job's
      ``dropDuplicates`` this gives end-to-end de-duplication.
    * ``retries`` + ``retry.backoff.ms`` — survive a broker restart (chaos test TC-FT-003).
    * ``linger.ms=20`` — batch a tick's worth of events into one request; at 25 events every 2 s
      this cuts request count by ~20x for 20 ms of added latency.
    """
    return Producer(
        {
            "bootstrap.servers": CFG.kafka_bootstrap,
            "client.id": f"gps-producer-{RUN_ID}",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "retry.backoff.ms": 500,
            "delivery.timeout.ms": 120000,
            "linger.ms": 20,
            "batch.num.messages": 200,
            "compression.type": "snappy",
            # Keep trying while Kafka is still forming its KRaft quorum.
            "reconnect.backoff.ms": 500,
            "reconnect.backoff.max.ms": 10000,
            "socket.keepalive.enable": True,
        }
    )


def wait_for_kafka(producer: Producer, timeout_s: int = 180) -> bool:
    """Poll broker metadata until Kafka answers, or give up after ``timeout_s``.

    Compose's ``depends_on: service_healthy`` normally makes this unnecessary, but the producer
    is also restarted on its own during the chaos tests, when the broker may be mid-restart.
    """
    deadline = time.time() + timeout_s
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            producer.list_topics(timeout=5.0)
            LOG.info(
                "kafka reachable",
                extra={"event": "kafka_ready", "attempts": attempt, "bootstrap": CFG.kafka_bootstrap},
            )
            return True
        except KafkaException as exc:
            LOG.warning(
                "kafka not reachable yet, backing off",
                extra={"event": "kafka_wait", "attempt": attempt, "error": str(exc)},
            )
            time.sleep(min(10.0, 1.0 * attempt))
    return False


def delivery_report(err: Any, msg: Any, status: str = "unknown") -> None:
    """librdkafka delivery callback — the only place that knows a send truly succeeded.

    Counting here rather than at ``produce()`` time means ``producer_events_sent_total`` reflects
    broker acknowledgements, not optimistic enqueues.  That distinction matters for the
    NoTelemetryReceived alert: if the broker dies, the counter stops even though the application
    keeps calling produce().

    ``status`` is bound per message with :func:`functools.partial` rather than read back from the
    message headers: librdkafka does not guarantee headers survive the round trip into the
    delivery report, and DEFECT-003 was exactly that — every event counted as ``status="unknown"``.
    """
    if err is not None:
        PRODUCER_SEND_ERRORS.inc()
        LOG.error(
            "delivery failed",
            extra={
                "event": "delivery_failed",
                "error": str(err),
                "topic": msg.topic() if msg else None,
                "key": msg.key().decode() if msg and msg.key() else None,
            },
        )
        return

    PRODUCER_EVENTS_SENT.labels(status=status).inc()
    PRODUCER_LAST_SEND.set(time.time())


def send_event(producer: Producer, event: dict[str, Any], topic: str | None = None) -> None:
    """Serialise and enqueue one telemetry event.

    The message key is ``vehicle_id``; ``event_id`` and ``status`` travel as headers so an
    operator can trace a single event through Kafka with ``kafka-console-consumer`` without
    parsing the payload (the "tracing-lite" requirement).
    """
    key = event.get("vehicle_id")
    producer.produce(
        topic=topic or CFG.kafka_topic,
        key=(key or "UNKNOWN").encode(),
        value=json.dumps(event).encode(),
        headers=[
            ("event_id", str(event.get("event_id", "")).encode()),
            ("status", str(event.get("status", "unknown")).encode()),
            ("run_id", RUN_ID.encode()),
        ],
        # Bind the status into the callback so the metric label is reliable (DEFECT-003).
        callback=functools.partial(
            delivery_report, status=str(event.get("status", "unknown"))
        ),
    )


def run() -> int:
    """Main loop.  Returns a process exit code."""
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    serve_metrics(CFG.producer_metrics_port, PRODUCER_REGISTRY)

    # Touching the epoch here means the producer usually wins the race to create it, so the
    # simulated clock starts when telemetry starts.
    epoch = get_epoch()

    simulator = FleetSimulator(
        num_vehicles=CFG.num_vehicles,
        seed=CFG.seed,
        lazy_count=CFG.lazy_vehicles,
        tick_seconds=CFG.emit_interval_sec,
    )
    LOG.info(
        "fleet simulator initialised",
        extra={
            "event": "producer_start",
            "num_vehicles": CFG.num_vehicles,
            "lazy_vehicles": CFG.lazy_vehicles,
            "seed": CFG.seed,
            "emit_interval_sec": CFG.emit_interval_sec,
            "bad_event_rate": CFG.bad_event_rate,
            "late_event_rate": CFG.late_event_rate,
            "duplicate_rate": CFG.duplicate_rate,
            "sim_epoch": epoch,
            "topic": CFG.kafka_topic,
        },
    )

    producer = build_producer()
    if not wait_for_kafka(producer):
        LOG.error("kafka never became reachable", extra={"event": "kafka_unreachable"})
        return 1

    rng = simulator.rng  # reuse the seeded RNG so fault injection is reproducible too
    ticks = 0
    last_tick = time.time()

    while not _SHUTDOWN:
        now_real = time.time()
        dt = now_real - last_tick
        last_tick = now_real

        event_time = datetime.now(UTC)
        events = simulator.tick(event_time=event_time, sim_ts=sim_now(now_real, epoch), dt_s=dt)

        sent = bad = late = dup = 0
        for event in events:
            payload = event
            # --- deliberate corruption ------------------------------------------------------
            if rng.random() < CFG.bad_event_rate:
                payload, kind = simulator.corrupt(event)
                PRODUCER_BAD_EVENTS.labels(kind=kind).inc()
                bad += 1
                LOG.debug(
                    "injected bad event",
                    extra={"event": "bad_event_injected", "kind": kind,
                           "event_id": payload.get("event_id")},
                )
            # --- deliberate lateness --------------------------------------------------------
            elif rng.random() < CFG.late_event_rate:
                payload = simulator.make_late(event)
                late += 1

            send_event(producer, payload)
            sent += 1

            # --- deliberate duplicate -------------------------------------------------------
            # Re-sending the identical event_id is what the stream job's dropDuplicates removes.
            if rng.random() < CFG.duplicate_rate:
                send_event(producer, payload)
                dup += 1

        # poll() drives the delivery callbacks; 0 means "do not block".
        producer.poll(0)
        ticks += 1

        if ticks % 10 == 0:
            LOG.info(
                "telemetry batch published",
                extra={
                    "event": "tick_published",
                    "tick": ticks,
                    "events_sent": sent,
                    "bad_injected": bad,
                    "late_injected": late,
                    "duplicates": dup,
                    "in_flight": len(producer),
                    "sim_ts": sim_now(now_real, epoch).isoformat(),
                },
            )

        # Sleep the remainder of the interval, with jitter so all 25 vehicles do not emit in a
        # perfectly synchronised burst (which would make the 1-minute windows artificially spiky).
        jitter = rng.uniform(-CFG.emit_jitter_sec, CFG.emit_jitter_sec)
        time.sleep(max(0.2, CFG.emit_interval_sec + jitter))

    LOG.info("flushing before exit", extra={"event": "shutdown_flush", "in_flight": len(producer)})
    remaining = producer.flush(timeout=30)
    if remaining:
        LOG.error(
            "messages still undelivered after flush",
            extra={"event": "shutdown_flush_incomplete", "remaining": remaining},
        )
        return 1
    LOG.info("producer stopped cleanly", extra={"event": "producer_stopped", "ticks": ticks})
    return 0


if __name__ == "__main__":
    sys.exit(run())
