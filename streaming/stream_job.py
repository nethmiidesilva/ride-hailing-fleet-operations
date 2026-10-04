"""Speed layer: Spark Structured Streaming job (processing, 15 marks).

Pipeline
--------
::

    Kafka fleet.telemetry (6 partitions, key=vehicle_id)
        |
        +-- parse JSON against the shared Spark schema
        +-- validate with the SHARED rule set  --> invalid --> query (d): rejected_events + DLQ
        +-- dropDuplicatesWithinWatermark(event_id, 2 min)
        +-- enrich: zone (shared function), is_active, is_trip_end
             |
             +-- query (a) master dataset : Parquet partitionBy(sim_date), trigger 30 s
             +-- query (b) zone metrics   : 1-min tumbling windows -> UPSERT realtime_zone_metrics
             +-- query (c) vehicle state  : latest per vehicle -> UPSERT vehicle_status,
                                            open/resolve rows in idle_alerts

Four independent queries, each with its own checkpoint directory, is a deliberate choice: a
failure or a code change in one sink cannot corrupt or stall the others, and each can be
restarted and will resume from exactly where it stopped.

Delivery semantics
------------------
Kafka + Structured Streaming gives **at-least-once**.  Every Postgres write is an
``INSERT ... ON CONFLICT DO UPDATE`` keyed on the row's natural key, so replaying a micro-batch
produces the same final state — *effectively-once storage*.  This is the honest position the
report takes rather than claiming exactly-once end to end.

Run with ``python -m streaming.stream_job``.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import CFG
from common.db import connect, execute, execute_many, wait_for_postgres
from common.logging_setup import RUN_ID, setup_logging
from common.metrics import (
    IDLE_ALERTS_OPEN,
    STREAM_BATCH_DURATION,
    STREAM_BATCHES,
    STREAM_COMMITTED_OFFSET,
    STREAM_EVENTS_VALID,
    STREAM_INPUT_ROWS_PER_SEC,
    STREAM_LAST_BATCH,
    STREAM_PROCESSED_ROWS_PER_SEC,
    STREAM_REGISTRY,
    STREAM_ROWS_PROCESSED,
    STREAM_ROWS_REJECTED,
    serve_metrics,
)
from streaming import transforms as T

LOG = setup_logging("stream-job", "processing")

_SHUTDOWN = False


def _handle_signal(signum: int, _frame: Any) -> None:
    global _SHUTDOWN
    _SHUTDOWN = True
    LOG.info(
        "shutdown requested",
        extra={"event": "shutdown_signal", "signal": signal.Signals(signum).name},
    )


# ============================================================================================
# Spark session
# ============================================================================================
def build_spark() -> SparkSession:
    """Create the local-mode SparkSession.

    Settings worth defending in a viva:

    * ``spark.sql.shuffle.partitions=6`` — the default is 200, which would create 200 tiny tasks
      per micro-batch and dominate the runtime at this data volume.  Matching the Kafka partition
      count keeps one task per partition.
    * ``spark.sql.session.timeZone=UTC`` — every timestamp in the system is UTC; leaving this to
      the container's locale is a classic source of off-by-one-day bugs in the batch layer.
    * ``spark.sql.streaming.metricsEnabled`` — exposes query progress for debugging via the UI.
    * The Kafka connector jars are already inside the image (see streaming/Dockerfile), so no
      ``--packages`` resolution happens at start-up.
    """
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "1400m")
    return (
        SparkSession.builder.master(CFG.spark_master)
        .appName(f"fleet-speed-layer-{RUN_ID}")
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.shuffle.partitions", str(CFG.kafka_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.adaptive.enabled", "false")  # adaptive execution is off for streaming
        .config("spark.sql.streaming.stateStore.compression.codec", "lz4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_telemetry(spark: SparkSession) -> DataFrame:
    """Kafka source.

    ``startingOffsets=latest`` for the live demo (a restart picks up current traffic rather than
    replaying 24 h of history), overridable to ``earliest`` for the replay demonstration.
    ``failOnDataLoss=false`` keeps the job alive if retention deletes a segment the checkpoint
    still references — a real possibility during a long demo with 24 h retention.
    ``maxOffsetsPerTrigger`` bounds a catch-up batch after a restart so the job cannot try to
    process an unbounded backlog in one go.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", CFG.kafka_bootstrap)
        .option("subscribe", CFG.kafka_topic)
        .option("startingOffsets", CFG.stream_starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        # groupIdPrefix (not kafka.group.id): the four queries must NOT share one consumer
        # group or they steal partitions from each other.  Spark appends a unique suffix
        # per query, and kafka-exporter still reports lag for every resulting group, which
        # is what the ConsumerLagHigh alert sums over (DEFECT-005).
        .option("groupIdPrefix", CFG.kafka_consumer_group)
        .load()
    )


# ============================================================================================
# SQL used by the foreachBatch sinks (kept as module constants so they are easy to review)
# ============================================================================================
UPSERT_ZONE_METRICS = """
INSERT INTO realtime_zone_metrics
    (window_start, window_end, zone, active_vehicles, idle_vehicles, total_vehicles,
     idle_ratio, trips_completed, earnings_lkr, avg_speed_kmh, event_count, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (window_start, zone) DO UPDATE SET
    window_end      = EXCLUDED.window_end,
    active_vehicles = EXCLUDED.active_vehicles,
    idle_vehicles   = EXCLUDED.idle_vehicles,
    total_vehicles  = EXCLUDED.total_vehicles,
    idle_ratio      = EXCLUDED.idle_ratio,
    trips_completed = EXCLUDED.trips_completed,
    earnings_lkr    = EXCLUDED.earnings_lkr,
    avg_speed_kmh   = EXCLUDED.avg_speed_kmh,
    event_count     = EXCLUDED.event_count,
    updated_at      = now();
"""

# The CASE keeps idle_since "sticky": it is set the first time a vehicle reports idle and only
# cleared when it moves again, so the alert threshold measures the whole idle spell rather than
# the time since the last event.
UPSERT_VEHICLE_STATUS = """
INSERT INTO vehicle_status
    (vehicle_id, driver_id, status, lat, lon, zone, speed_kmh, last_event_time, last_trip_id,
     idle_since, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
        CASE WHEN %s = 'idle' THEN %s::timestamptz ELSE NULL END, now())
ON CONFLICT (vehicle_id) DO UPDATE SET
    driver_id       = EXCLUDED.driver_id,
    status          = EXCLUDED.status,
    lat             = EXCLUDED.lat,
    lon             = EXCLUDED.lon,
    zone            = EXCLUDED.zone,
    speed_kmh       = EXCLUDED.speed_kmh,
    last_event_time = EXCLUDED.last_event_time,
    last_trip_id    = EXCLUDED.last_trip_id,
    idle_since      = CASE
                        WHEN EXCLUDED.status = 'idle'
                            THEN COALESCE(vehicle_status.idle_since, EXCLUDED.last_event_time)
                        ELSE NULL
                      END,
    updated_at      = now()
-- A late event must never overwrite newer state.
WHERE EXCLUDED.last_event_time >= vehicle_status.last_event_time;
"""

# ON CONFLICT against the PARTIAL unique index (vehicle_id) WHERE status='open' means a vehicle
# can only ever have one unresolved alert, no matter how many times a batch is replayed.
OPEN_IDLE_ALERTS = """
INSERT INTO idle_alerts (vehicle_id, zone, idle_since, detected_at, idle_minutes, status)
SELECT v.vehicle_id, v.zone, v.idle_since, now(),
       round(EXTRACT(EPOCH FROM (now() - v.idle_since)) / 60.0, 2), 'open'
FROM vehicle_status v
WHERE v.idle_since IS NOT NULL
  AND now() - v.idle_since > make_interval(secs => %s)
ON CONFLICT (vehicle_id) WHERE status = 'open' DO NOTHING;
"""

RESOLVE_IDLE_ALERTS = """
UPDATE idle_alerts a
SET status = 'resolved',
    resolved_at = now(),
    idle_minutes = round(EXTRACT(EPOCH FROM (now() - a.idle_since)) / 60.0, 2)
FROM vehicle_status v
WHERE a.vehicle_id = v.vehicle_id
  AND a.status = 'open'
  AND v.idle_since IS NULL;
"""

# DEFECT-019: a vehicle that stops reporting *while idle* never moves again, so the statement
# above can never fire for it and its alert stays open forever.  Retire a vehicle at the wrong
# moment and `ManyVehiclesIdle` -- a business alert -- fires permanently.  The 200-vehicle load
# test made this visible: 31 of 33 open alerts belonged to vehicles that no longer existed.
#
# An alert is only actionable while its vehicle is still reporting, so one that has gone silent is
# closed as `abandoned` rather than `resolved`.  The distinction is kept deliberately: "the driver
# started moving again" and "the vehicle disappeared" are different operational facts, and
# conflating them would overstate how many alerts were actually acted on.
# NOT EXISTS rather than a join to vehicle_status, because the alert must close in BOTH cases:
# the status row exists but is stale, AND the status row is gone entirely (pruned, or the vehicle
# was removed from the fleet). An inner join silently misses the second case and leaves the alert
# orphaned forever -- which is exactly what the first version of this statement did.
ABANDON_STALE_IDLE_ALERTS = """
UPDATE idle_alerts a
SET status = 'abandoned',
    resolved_at = now(),
    idle_minutes = round(EXTRACT(EPOCH FROM (now() - a.idle_since)) / 60.0, 2)
WHERE a.status = 'open'
  AND NOT EXISTS (
      SELECT 1 FROM vehicle_status v
      WHERE v.vehicle_id = a.vehicle_id
        AND v.last_event_time >= now() - make_interval(secs => %s)
  );
"""

INSERT_REJECTED = """
INSERT INTO rejected_events
    (event_id, vehicle_id, reason, raw_payload, batch_id, kafka_partition, kafka_offset)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


# ============================================================================================
# foreachBatch sinks
# ============================================================================================
def _log_batch(sink: str, batch_id: int, rows: int, duration_s: float, **extra: Any) -> None:
    """One structured line per micro-batch — the processing-stage observability contract."""
    STREAM_BATCHES.labels(sink=sink).inc()
    STREAM_BATCH_DURATION.labels(sink=sink).observe(duration_s)
    STREAM_LAST_BATCH.set(time.time())
    LOG.info(
        "micro-batch complete",
        extra={
            "event": "batch_complete",
            "stage": "processing",
            "sink": sink,
            "batch_id": batch_id,
            "rows_written": rows,
            "duration_ms": round(duration_s * 1000, 1),
            **extra,
        },
    )


def sink_zone_metrics(batch_df: DataFrame, batch_id: int) -> None:
    """UPSERT the 1-minute window aggregates.

    ``outputMode("update")`` upstream means an in-flight window is re-emitted every trigger with
    its running totals; the UPSERT overwrites the previous value for the same
    ``(window_start, zone)``.  The dashboard therefore updates every 30 s instead of waiting the
    full watermark, and a replayed batch still converges to the same row.
    """
    started = time.time()
    rows = [
        (
            r["window_start"], r["window_end"], r["zone"], r["active_vehicles"],
            r["idle_vehicles"], r["total_vehicles"], r["idle_ratio"], r["trips_completed"],
            r["earnings_lkr"], r["avg_speed_kmh"], r["event_count"],
        )
        for r in batch_df.collect()  # micro-batch is at most 6 zones x few windows
    ]
    written = execute_many(UPSERT_ZONE_METRICS, rows)
    STREAM_ROWS_PROCESSED.labels(sink="zone_metrics").inc(written)
    _log_batch("zone_metrics", batch_id, written, time.time() - started,
               zones=len({r[2] for r in rows}))


def sink_vehicle_status(batch_df: DataFrame, batch_id: int) -> None:
    """UPSERT the latest state per vehicle, then open/resolve idle alerts.

    Ordering matters: ``vehicle_status`` is updated first so the alert statements below see the
    freshest ``idle_since``.  Both alert statements are set-based SQL rather than per-row Python,
    so the whole fleet is evaluated in two round trips.
    """
    started = time.time()
    # This sink sees the enriched (valid) stream, so its row count IS the number of valid events
    # in the micro-batch — the honest denominator for the reject ratio (DEFECT-015).  The batch is
    # cached because latest_per_vehicle() would otherwise recompute it.
    batch_df.persist()
    valid_events = batch_df.count()
    STREAM_EVENTS_VALID.inc(valid_events)

    latest = T.latest_per_vehicle(batch_df)
    rows = [
        (
            r["vehicle_id"], r["driver_id"], r["status"], r["lat"], r["lon"], r["zone"],
            r["speed_kmh"], r["last_event_time"], r["last_trip_id"],
            r["status"], r["last_event_time"],  # the two CASE parameters
        )
        for r in latest.collect()
    ]
    written = execute_many(UPSERT_VEHICLE_STATUS, rows)

    idle_threshold_s = CFG.idle_alert_minutes * 60.0
    opened = execute(OPEN_IDLE_ALERTS, (idle_threshold_s,))
    resolved = execute(RESOLVE_IDLE_ALERTS)
    # Close alerts for vehicles that have gone silent (DEFECT-019).  The window is deliberately
    # generous -- 5x the idle threshold -- because a merely idle vehicle still reports every
    # EMIT_INTERVAL_SEC, so only a genuinely absent vehicle can trip this.
    abandoned = execute(ABANDON_STALE_IDLE_ALERTS, (idle_threshold_s * 5,))

    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM idle_alerts WHERE status = 'open'")
        open_now = cur.fetchone()[0]
    IDLE_ALERTS_OPEN.set(open_now)

    STREAM_ROWS_PROCESSED.labels(sink="vehicle_status").inc(written)
    batch_df.unpersist()
    _log_batch(
        "vehicle_status", batch_id, written, time.time() - started,
        valid_events=valid_events,
        alerts_opened=opened, alerts_resolved=resolved, alerts_abandoned=abandoned,
        alerts_open_total=open_now,
        idle_threshold_seconds=idle_threshold_s,
    )
    if opened:
        LOG.warning(
            "idle alert(s) opened",
            extra={"event": "idle_alert_opened", "count": opened,
                   "threshold_minutes": CFG.idle_alert_minutes},
        )


def sink_rejected(batch_df: DataFrame, batch_id: int) -> None:
    """Quarantine invalid events: Postgres for querying, Kafka DLQ for replay.

    Writing to both is intentional. ``rejected_events`` answers "what is wrong with our data?"
    with SQL; the DLQ topic keeps the original bytes so a fixed producer could replay them
    through the same pipeline. Neither is a dead end.
    """
    started = time.time()
    collected = batch_df.select(
        "event_id", "vehicle_id", "reject_reason", "raw_payload",
        "kafka_partition", "kafka_offset",
    ).collect()
    if not collected:
        return

    rows = [
        (r["event_id"], r["vehicle_id"], r["reject_reason"], r["raw_payload"],
         batch_id, r["kafka_partition"], r["kafka_offset"])
        for r in collected
    ]
    execute_many(INSERT_REJECTED, rows)

    by_reason: dict[str, int] = {}
    for row in rows:
        by_reason[row[2]] = by_reason.get(row[2], 0) + 1
    for reason, count in by_reason.items():
        STREAM_ROWS_REJECTED.labels(reason=reason).inc(count)

    # Dead-letter the original payload, keyed by vehicle so DLQ ordering matches the main topic.
    try:
        (
            batch_df.select(
                F.coalesce(F.col("vehicle_id"), F.lit("UNKNOWN")).alias("key"),
                F.col("raw_payload").alias("value"),
                F.lit(CFG.kafka_dlq_topic).alias("topic"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", CFG.kafka_bootstrap)
            .save()
        )
    except Exception as exc:  # noqa: BLE001 - the DLQ must never take down the pipeline
        LOG.error(
            "failed to write to DLQ",
            extra={"event": "dlq_write_failed", "batch_id": batch_id, "error": str(exc)},
        )

    _log_batch("rejected", batch_id, len(rows), time.time() - started, reasons=by_reason)


# ============================================================================================
# Query construction
# ============================================================================================
def start_queries(spark: SparkSession) -> list[Any]:
    """Build and start all four streaming queries; returns the query handles."""
    raw = read_telemetry(spark)
    parsed = T.parse_kafka_json(raw)
    valid, rejected = T.split_valid_invalid(parsed)
    deduped = T.deduplicate(valid)
    enriched = T.enrich(deduped)

    trigger_s = CFG.stream_trigger_seconds
    queries = []

    # --- (a) master dataset ------------------------------------------------------------------
    # The immutable, append-only source of truth the batch layer recomputes from.  Written as
    # Parquet partitioned by sim_date so the batch job can read exactly one day with a partition
    # prune rather than a full scan.
    queries.append(
        T.master_dataset_columns(enriched)
        .writeStream.queryName("master_dataset")
        .format("parquet")
        .option("path", f"{CFG.lake_root}/telemetry")
        .option("checkpointLocation", f"{CFG.checkpoint_root}/master")
        .partitionBy("sim_date")
        .outputMode("append")
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    # --- (b) zone metrics --------------------------------------------------------------------
    queries.append(
        # apply_watermark=False: deduplicate() already set it; a second withWatermark in the
        # same plan is rejected by Spark 3.5 (DEFECT-004).
        T.zone_window_metrics(enriched, apply_watermark=False)
        .writeStream.queryName("zone_metrics")
        .foreachBatch(sink_zone_metrics)
        .option("checkpointLocation", f"{CFG.checkpoint_root}/zone_metrics")
        .outputMode("update")
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    # --- (c) vehicle status + idle alerts ----------------------------------------------------
    # A shorter trigger than the other sinks: the idle alert is the demo's headline feature and
    # must react within seconds of the threshold being crossed.
    queries.append(
        enriched.writeStream.queryName("vehicle_status")
        .foreachBatch(sink_vehicle_status)
        .option("checkpointLocation", f"{CFG.checkpoint_root}/vehicle_status")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- (d) quarantine ----------------------------------------------------------------------
    queries.append(
        rejected.writeStream.queryName("rejected_events")
        .foreachBatch(sink_rejected)
        .option("checkpointLocation", f"{CFG.checkpoint_root}/rejected")
        .outputMode("append")
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    LOG.info(
        "streaming queries started",
        extra={
            "event": "queries_started",
            "queries": [q.name for q in queries],
            "topic": CFG.kafka_topic,
            "starting_offsets": CFG.stream_starting_offsets,
            "window_minutes": CFG.stream_window_minutes,
            "watermark_minutes": CFG.stream_watermark_minutes,
            "trigger_seconds": trigger_s,
            "checkpoint_root": CFG.checkpoint_root,
            "lake_root": CFG.lake_root,
        },
    )
    return queries


def publish_query_progress(queries: list[Any]) -> None:
    """Export each query's Spark progress as Prometheus gauges.

    Why this exists (DEFECT-013): Structured Streaming does **not** commit offsets to Kafka -- it
    keeps them in its checkpoint, because that is what gives it its delivery guarantees.  The
    broker therefore knows of no consumer group for this job and kafka-exporter's
    ``kafka_consumergroup_lag`` is permanently empty, so a lag alert written against it could
    never fire.

    Publishing the offsets the job *has* committed lets Prometheus compute true lag as
    ``kafka_topic_partition_current_offset - stream_committed_offset`` -- the broker supplies the
    first term, this function the second.  It also exports Spark's own input and processing rates,
    which is the cheapest early warning that a job is falling behind.
    """
    for query in queries:
        progress = query.lastProgress
        if not progress:
            continue
        name = progress.get("name") or query.name or "unnamed"
        if progress.get("inputRowsPerSecond") is not None:
            STREAM_INPUT_ROWS_PER_SEC.labels(query=name).set(progress["inputRowsPerSecond"])
        if progress.get("processedRowsPerSecond") is not None:
            STREAM_PROCESSED_ROWS_PER_SEC.labels(query=name).set(
                progress["processedRowsPerSecond"]
            )
        for source in progress.get("sources", []):
            end_offset = source.get("endOffset")
            if not isinstance(end_offset, dict):
                continue
            # endOffset is {topic: {partition: offset}}
            for topic, partitions in end_offset.items():
                if not isinstance(partitions, dict):
                    continue
                for partition, offset in partitions.items():
                    if offset is None:
                        continue
                    STREAM_COMMITTED_OFFSET.labels(
                        query=name, topic=topic, partition=str(partition)
                    ).set(float(offset))


def run() -> int:
    """Start Spark, start the queries, then supervise them until shutdown."""
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    serve_metrics(CFG.stream_metrics_port, STREAM_REGISTRY)

    if not wait_for_postgres(timeout_s=180):
        LOG.error("postgres never became reachable", extra={"event": "postgres_unreachable"})
        return 1

    # Seed the freshness gauge so StreamProcessingStalled does not fire before the first batch.
    STREAM_LAST_BATCH.set(time.time())

    spark = build_spark()
    LOG.info(
        "spark session ready",
        extra={
            "event": "spark_ready",
            "spark_version": spark.version,
            "master": CFG.spark_master,
            "app_id": spark.sparkContext.applicationId,
        },
    )

    queries = start_queries(spark)

    try:
        while not _SHUTDOWN:
            publish_query_progress(queries)
            for query in queries:
                if not query.isActive:
                    LOG.error(
                        "streaming query died",
                        extra={
                            "event": "query_dead",
                            "query": query.name,
                            "exception": str(query.exception()),
                        },
                    )
                    return 2
            time.sleep(5)
    finally:
        LOG.info("stopping queries", extra={"event": "queries_stopping"})
        for query in queries:
            try:
                query.stop()
            except Exception as exc:  # noqa: BLE001
                LOG.warning(
                    "query did not stop cleanly",
                    extra={"event": "query_stop_failed", "query": query.name, "error": str(exc)},
                )
        spark.stop()
    LOG.info("stream job stopped cleanly", extra={"event": "stream_job_stopped"})
    return 0


if __name__ == "__main__":
    sys.exit(run())
