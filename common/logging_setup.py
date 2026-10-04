"""Structured JSON logging used by every service (observability, 10 marks).

Design
------
The brief asks for structured logging across ingestion, processing and storage.  A single
formatter is used everywhere so that ``docker compose logs | jq`` works across services and so a
single event or a single DAG run can be followed end to end ("tracing-lite").

Every record carries a fixed envelope:

===============  =========================================================================
``ts``           ISO-8601 UTC timestamp with milliseconds
``level``        INFO / WARNING / ERROR
``service``      logical service name (gps-producer, stream-job, api, airflow, ...)
``stage``        one of ingestion | processing | storage | serving | orchestration
``event``        short machine-readable event name, e.g. ``batch_complete``
``message``      human sentence
``run_id``       per-process run id (uuid4 prefix) so restarts are distinguishable
===============  =========================================================================

Anything else passed as ``extra={...}`` is merged into the same JSON object, which is how
``event_id``, ``batch_id``, ``vehicle_id`` and ``rows_rejected`` travel with the log line.

The implementation is a ~60 line custom formatter rather than a dependency, because it has to run
inside the PySpark image, the Airflow image and the API image without version conflicts.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

from common.config import CFG

#: Stages named in the brief.  Kept as a frozenset so a typo fails a unit test rather than
#: silently producing an unfilterable log stream.
STAGES = frozenset({"ingestion", "processing", "storage", "serving", "orchestration"})

#: Attributes the stdlib puts on every LogRecord.  Anything *not* in here came from ``extra=``
#: and therefore belongs in the JSON payload.
_RESERVED = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName", "levelname",
        "levelno", "lineno", "module", "msecs", "message", "msg", "name", "pathname", "process",
        "processName", "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)

#: One run id per process.  It lets you separate the logs of a restarted stream job from the logs
#: of the run before it, which matters for the checkpoint-recovery chaos test.
RUN_ID = os.getenv("RUN_ID") or uuid.uuid4().hex[:12]


class JsonFormatter(logging.Formatter):
    """Render a LogRecord as one line of JSON."""

    def __init__(self, service: str, stage: str) -> None:
        super().__init__()
        self.service = service
        self.stage = stage

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003 - stdlib API name
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": getattr(record, "service", self.service),
            # A call site may override the stage, e.g. the producer logging a storage failure.
            "stage": getattr(record, "stage", self.stage),
            "event": getattr(record, "event", record.funcName),
            "run_id": getattr(record, "run_id", RUN_ID),
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and key not in payload:
                payload[key] = _jsonable(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def _jsonable(value: Any) -> Any:
    """Best-effort conversion so a stray datetime or Decimal cannot kill a log line."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


def setup_logging(
    service: str, stage: str, level: str | None = None, force: bool = True
) -> logging.Logger:
    """Configure the root logger for a service and return its named logger.

    Args:
        service: logical service name that appears in every line.
        stage:   pipeline stage; must be one of :data:`STAGES`.
        level:   override for ``LOG_LEVEL``.
        force:   when False, leave an already-configured root logger alone and just return a
            named logger.  This matters inside Airflow, which installs its own handler to capture
            a task's log: replacing it mid-task breaks the task runner (DEFECT-012).  Only an
            application entry point should call this with ``force=True``.

    Set ``LOG_FORMAT=text`` to get human-readable lines instead of JSON when debugging locally.
    """
    if not force and logging.getLogger().handlers:
        return logging.getLogger(service)
    if stage not in STAGES:
        raise ValueError(f"stage must be one of {sorted(STAGES)}, got {stage!r}")

    handler = logging.StreamHandler(sys.stdout)
    if CFG.log_format.lower() == "json":
        handler.setFormatter(JsonFormatter(service, stage))
    else:
        handler.setFormatter(
            logging.Formatter(f"%(asctime)s %(levelname)-7s [{service}/{stage}] %(message)s")
        )

    root = logging.getLogger()
    root.handlers.clear()  # containers restart; never stack duplicate handlers
    root.addHandler(handler)
    root.setLevel((level or CFG.log_level).upper())

    # py4j/kafka libraries are extremely chatty at INFO and would drown the pipeline signal.
    for noisy in ("py4j", "pyspark", "kafka", "urllib3", "botocore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(service)


__all__ = ["RUN_ID", "STAGES", "JsonFormatter", "setup_logging"]
