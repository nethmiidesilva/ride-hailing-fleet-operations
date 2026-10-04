"""Prometheus metric definitions, in one place so names stay consistent with the alert rules.

Every Python service starts a ``prometheus_client`` HTTP endpoint on its own port
(producer 8001, expense producer 8002, stream job 8003, API 8000 at ``/metrics-prom``) and
Prometheus scrapes them (``monitoring/prometheus.yml``).  Consumer lag comes from
``danielqsj/kafka-exporter`` instead, because lag is a broker-side fact the producer cannot know.

Metric names are referenced verbatim by ``monitoring/alert_rules.yml`` and by the Grafana
"Pipeline Health" dashboard, so they are treated as a public contract: renaming one here means
updating both files, which is why they live in a single module.
"""

from __future__ import annotations

import logging

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------------------------
# Per-service registries (DEFECT-014)
# ---------------------------------------------------------------------------------------------
# Importing this module used to register EVERY metric in prometheus_client's default registry, so
# every service exposed every metric -- including ones it never sets.  An unset Gauge reads 0, so
# `time() - producer_last_send_timestamp` evaluated to ~1.79e9 on the API, the expense producer
# and the stream job, and NoTelemetryReceived, StreamProcessingStalled and ApiUnhealthy fired
# permanently against three phantom instances each.
#
# Each service now exposes only its own collectors.  A metric that is absent is genuinely absent,
# which is what makes `absent()` and staleness handling work, and a scrape of any endpoint now
# describes only that service.
PRODUCER_REGISTRY = CollectorRegistry()
EXPENSE_REGISTRY = CollectorRegistry()
STREAM_REGISTRY = CollectorRegistry()
API_REGISTRY = CollectorRegistry()
BATCH_REGISTRY = CollectorRegistry()

# --- ingestion: gps_producer (port 8001) ----------------------------------------------------
PRODUCER_EVENTS_SENT = Counter(
    "producer_events_sent_total",
    "Telemetry events successfully acknowledged by Kafka, labelled by vehicle status.",
    ["status"],
    registry=PRODUCER_REGISTRY,
)
PRODUCER_SEND_ERRORS = Counter(
    "producer_send_errors_total",
    "Delivery callbacks that reported a failure.",
    registry=PRODUCER_REGISTRY,
)
PRODUCER_BAD_EVENTS = Counter(
    "producer_bad_events_injected_total",
    "Deliberately malformed events injected, labelled by the kind of corruption.",
    ["kind"],
    registry=PRODUCER_REGISTRY,
)
PRODUCER_LAST_SEND = Gauge(
    "producer_last_send_timestamp",
    "UNIX timestamp of the last successful send; NoTelemetryReceived alerts on its age.",
    registry=PRODUCER_REGISTRY,
)

# --- ingestion: expense_producer (port 8002) ------------------------------------------------
EXPENSE_FILES_WRITTEN = Counter(
    "expense_files_written_total",
    "Daily expense CSV files written to the landing directory.",
    registry=EXPENSE_REGISTRY,
)
EXPENSE_LAST_FILE = Gauge(
    "expense_last_file_timestamp",
    "UNIX timestamp of the last expense file written; ExpenseFileLate alerts on its age.",
    registry=EXPENSE_REGISTRY,
)
EXPENSE_ROWS_WRITTEN = Counter(
    "expense_rows_written_total",
    "Rows written across all expense files, labelled clean/dirty.",
    ["kind"],
    registry=EXPENSE_REGISTRY,
)

# --- processing: stream_job (port 8003) -----------------------------------------------------
STREAM_BATCHES = Counter(
    "stream_batches_total",
    "Structured Streaming micro-batches completed, labelled by sink.",
    ["sink"],
    registry=STREAM_REGISTRY,
)
STREAM_ROWS_PROCESSED = Counter(
    "stream_rows_processed_total",
    "Rows that passed validation and were written downstream.",
    ["sink"],
    registry=STREAM_REGISTRY,
)
STREAM_ROWS_REJECTED = Counter(
    "stream_rows_rejected_total",
    "Rows quarantined, labelled by rejection reason.",
    ["reason"],
    registry=STREAM_REGISTRY,
)
# DEFECT-015: stream_rows_processed_total counts rows WRITTEN PER SINK (25 vehicle_status
# upserts, ~6 zone-window upserts per batch), not events ingested.  Using it as the denominator
# of the reject ratio compared two different units and made HighRejectRate fire permanently at
# ~7%.  This counter is the real denominator: valid events that entered the enriched stream.
STREAM_EVENTS_VALID = Counter(
    "stream_events_valid_total",
    "Telemetry events that passed validation and entered the enriched stream. The reject ratio "
    "is rejected / (valid + rejected); both are event counts.",
    registry=STREAM_REGISTRY,
)
STREAM_BATCH_DURATION = Histogram(
    "stream_batch_duration_seconds",
    "Wall-clock duration of a foreachBatch callback.",
    ["sink"],
    # Buckets chosen around the 20 s StreamBatchSlow alert threshold so the p95 is meaningful.
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60),
    registry=STREAM_REGISTRY,
)
STREAM_LAST_BATCH = Gauge(
    "stream_last_batch_timestamp",
    "UNIX timestamp of the last completed micro-batch; used by NoTelemetryReceived.",
    registry=STREAM_REGISTRY,
)
IDLE_ALERTS_OPEN = Gauge(
    "idle_alerts_open",
    "Number of currently unresolved idle alerts.",
    registry=STREAM_REGISTRY,
)

# --- consumer lag, the Structured Streaming way (DEFECT-013) ---------------------------------
# Spark Structured Streaming does NOT commit offsets to Kafka: it keeps them in its own
# checkpoint, because the checkpoint is what gives it its delivery guarantees.  As a result the
# broker knows of no consumer group for this job, and kafka-exporter's
# `kafka_consumergroup_lag` is permanently empty -- the ConsumerLagHigh alert could never fire.
#
# The fix is to publish the offsets the job HAS committed, per partition, and subtract them from
# the broker's end offsets (which kafka-exporter does provide as
# `kafka_topic_partition_current_offset`).  The alert rule does that subtraction in PromQL.
STREAM_COMMITTED_OFFSET = Gauge(
    "stream_committed_offset",
    "Highest Kafka offset the streaming query has committed to its checkpoint, per partition.",
    ["query", "topic", "partition"],
    registry=STREAM_REGISTRY,
)
STREAM_INPUT_ROWS_PER_SEC = Gauge(
    "stream_input_rows_per_second",
    "Rows arriving from Kafka per second, as reported by Spark query progress.",
    ["query"],
    registry=STREAM_REGISTRY,
)
STREAM_PROCESSED_ROWS_PER_SEC = Gauge(
    "stream_processed_rows_per_second",
    "Rows processed per second, as reported by Spark query progress. Below the input rate for a "
    "sustained period means the job is falling behind.",
    ["query"],
    registry=STREAM_REGISTRY,
)

# --- serving: api (port 8000) ---------------------------------------------------------------
API_REQUESTS = Counter(
    "api_requests_total",
    "HTTP requests handled by the FastAPI service.",
    ["method", "path", "status"],
    registry=API_REGISTRY,
)
API_REQUEST_DURATION = Histogram(
    "api_request_duration_seconds",
    "HTTP request latency.",
    ["path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
    registry=API_REGISTRY,
)
API_HEALTH = Gauge(
    "api_health_status",
    "1 when /health is fully healthy, 0 when degraded.",
    registry=API_REGISTRY,
)

# --- orchestration: batch job / airflow ------------------------------------------------------
BATCH_ROWS_WRITTEN = Gauge(
    "batch_rows_written",
    "Rows written by the last profitability batch run, labelled by table.",
    ["table"],
    registry=BATCH_REGISTRY,
)


def serve_metrics(port: int, registry: CollectorRegistry) -> None:
    """Start the Prometheus scrape endpoint for ONE service, exposing only its own registry.

    Args:
        port: scrape port (producer 8001, expense producer 8002, stream job 8003).
        registry: the service's own :class:`CollectorRegistry` -- see the note at the top of this
            module about why a shared default registry produced phantom alerts.

    Failures are logged and swallowed: losing metrics must never take down ingestion, and during
    unit tests the port is often already bound.
    """
    try:
        start_http_server(port, registry=registry)
        LOG.info("prometheus metrics endpoint started", extra={"event": "metrics_up", "port": port})
    except OSError as exc:  # port already in use (e.g. a test re-importing the module)
        LOG.warning(
            "could not start metrics endpoint",
            extra={"event": "metrics_failed", "port": port, "error": str(exc)},
        )


__all__ = [
    "API_HEALTH",
    "API_REGISTRY",
    "BATCH_REGISTRY",
    "EXPENSE_REGISTRY",
    "PRODUCER_REGISTRY",
    "STREAM_REGISTRY",
    "API_REQUESTS",
    "API_REQUEST_DURATION",
    "BATCH_ROWS_WRITTEN",
    "EXPENSE_FILES_WRITTEN",
    "EXPENSE_LAST_FILE",
    "EXPENSE_ROWS_WRITTEN",
    "IDLE_ALERTS_OPEN",
    "PRODUCER_BAD_EVENTS",
    "PRODUCER_EVENTS_SENT",
    "PRODUCER_LAST_SEND",
    "PRODUCER_SEND_ERRORS",
    "STREAM_BATCHES",
    "STREAM_BATCH_DURATION",
    "STREAM_COMMITTED_OFFSET",
    "STREAM_EVENTS_VALID",
    "STREAM_INPUT_ROWS_PER_SEC",
    "STREAM_LAST_BATCH",
    "STREAM_PROCESSED_ROWS_PER_SEC",
    "STREAM_ROWS_PROCESSED",
    "STREAM_ROWS_REJECTED",
    "serve_metrics",
]
