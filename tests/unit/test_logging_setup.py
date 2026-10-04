"""Unit tests for structured logging (common/logging_setup.py).

Covers TC-OBS-001 .. TC-OBS-004 / REQ-08 (structured logging across all stages).
"""

from __future__ import annotations

import json
import logging

import pytest

from common.logging_setup import RUN_ID, STAGES, setup_logging

pytestmark = pytest.mark.unit

REQUIRED_FIELDS = {"ts", "level", "service", "stage", "event", "run_id", "message"}


def _capture(capsys, fn) -> dict:
    fn()
    out = capsys.readouterr().out.strip().splitlines()[-1]
    return json.loads(out)


def test_tc_obs_001_log_line_is_valid_json_with_the_required_envelope(capsys) -> None:
    """TC-OBS-001: every log line parses as JSON and carries the agreed envelope fields."""
    log = setup_logging("unit-test", "processing")
    record = _capture(capsys, lambda: log.info("hello", extra={"event": "unit_event"}))
    assert REQUIRED_FIELDS.issubset(record.keys())
    assert record["service"] == "unit-test"
    assert record["stage"] == "processing"
    assert record["event"] == "unit_event"
    assert record["message"] == "hello"
    assert record["level"] == "INFO"
    assert record["run_id"] == RUN_ID


def test_tc_obs_002_extra_context_is_merged_for_tracing(capsys) -> None:
    """TC-OBS-002: event_id/batch_id travel with the line — this is the tracing-lite mechanism."""
    log = setup_logging("stream-job", "processing")
    record = _capture(
        capsys,
        lambda: log.info(
            "batch done",
            extra={
                "event": "batch_complete",
                "batch_id": 17,
                "event_id": "abc-123",
                "rows_in": 250,
                "rows_rejected": 4,
            },
        ),
    )
    assert record["batch_id"] == 17
    assert record["event_id"] == "abc-123"
    assert record["rows_in"] == 250
    assert record["rows_rejected"] == 4


def test_tc_obs_003_invalid_stage_is_rejected() -> None:
    """TC-OBS-003: a typo'd stage fails fast instead of producing unfilterable logs."""
    with pytest.raises(ValueError):
        setup_logging("x", "processsing")
    assert {"ingestion", "processing", "storage", "serving", "orchestration"} == STAGES


def test_tc_obs_004_exceptions_are_serialised(capsys) -> None:
    """TC-OBS-004: an exception is captured in the JSON payload, not printed as a raw traceback."""
    log = setup_logging("api", "serving")

    def emit() -> None:
        try:
            raise RuntimeError("db gone")
        except RuntimeError:
            log.error("query failed", extra={"event": "db_error"}, exc_info=True)

    record = _capture(capsys, emit)
    assert record["level"] == "ERROR"
    assert "RuntimeError: db gone" in record["exception"]


def test_tc_obs_005_handlers_are_not_duplicated_on_reconfigure(capsys) -> None:
    """TC-OBS-005: re-running setup (container restart) must not double every log line."""
    setup_logging("svc", "ingestion")
    setup_logging("svc", "ingestion")
    assert len(logging.getLogger().handlers) == 1
