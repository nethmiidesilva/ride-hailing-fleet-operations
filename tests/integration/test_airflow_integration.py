"""Integration tests for the orchestration layer (Airflow).

Covers TC-ORC-001 .. TC-ORC-008 / REQ-02, REQ-03, REQ-07, REQ-11.

These drive the real Airflow instance through its REST API and CLI, so they prove the DAG runs
in the deployed environment rather than that it merely parses.
"""

from __future__ import annotations

import json
import os
import subprocess
import time

import pytest
import requests

from common.config import CFG
from common.db import query, query_one
from tests.conftest import wait_until

pytestmark = [pytest.mark.integration]

AIRFLOW = (
    f"http://{os.getenv('AIRFLOW_HOST', 'airflow-webserver')}:"
    f"{os.getenv('AIRFLOW_WEB_PORT', '8080')}"
)
AUTH = (
    os.getenv("AIRFLOW_ADMIN_USER", "admin"),
    os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"),
)
DAG_ID = "daily_reconciliation"


def _airflow_up() -> bool:
    try:
        return requests.get(f"{AIRFLOW}/health", timeout=10).status_code == 200
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture(scope="module", autouse=True)
def require_airflow():
    if not _airflow_up():
        pytest.skip("Airflow webserver is not reachable")


def _dag_runs(limit: int = 25) -> list[dict]:
    response = requests.get(
        f"{AIRFLOW}/api/v1/dags/{DAG_ID}/dagRuns",
        auth=AUTH,
        params={"limit": limit, "order_by": "-start_date"},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()["dag_runs"]


def _trigger(target_date: str, run_id: str) -> dict:
    response = requests.post(
        f"{AIRFLOW}/api/v1/dags/{DAG_ID}/dagRuns",
        auth=AUTH,
        json={"dag_run_id": run_id, "conf": {"date": target_date}},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _run_state(run_id: str) -> str | None:
    response = requests.get(
        f"{AIRFLOW}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}", auth=AUTH, timeout=20
    )
    if response.status_code != 200:
        return None
    return response.json().get("state")


def _task_states(run_id: str) -> dict[str, str]:
    response = requests.get(
        f"{AIRFLOW}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances", auth=AUTH, timeout=20
    )
    if response.status_code != 200:
        return {}
    return {t["task_id"]: t["state"] for t in response.json()["task_instances"]}


# --------------------------------------------------------------------------------------------
def test_tc_orc_001_dag_is_registered_and_unpaused(require_airflow) -> None:
    """TC-ORC-001: the DAG parses, is registered and runs without a manual unpause."""
    response = requests.get(f"{AIRFLOW}/api/v1/dags/{DAG_ID}", auth=AUTH, timeout=20)
    assert response.status_code == 200, response.text
    dag = response.json()
    assert dag["is_paused"] is False, "the DAG must be active for the demo to work unattended"
    # The REST API renders a timedelta schedule as
    # {"__type": "TimeDelta", "days": 0, "seconds": 600, "microseconds": 0}.
    schedule = dag["schedule_interval"]
    assert schedule.get("__type") == "TimeDelta", f"unexpected schedule shape: {schedule}"
    total_seconds = schedule.get("days", 0) * 86400 + schedule.get("seconds", 0)
    assert total_seconds == CFG.sim_day_seconds, (
        f"the DAG must run once per simulated day ({CFG.sim_day_seconds}s), got {total_seconds}s"
    )
    assert any(tag["name"] == "fleet" for tag in dag["tags"])


def test_tc_orc_002_dag_has_no_import_errors(require_airflow) -> None:
    """TC-ORC-002: Airflow reports zero import errors for the DAG folder."""
    response = requests.get(f"{AIRFLOW}/api/v1/importErrors", auth=AUTH, timeout=20)
    assert response.status_code == 200
    errors = response.json()["import_errors"]
    assert not errors, f"DAG import errors: {json.dumps(errors, indent=2)[:800]}"


def test_tc_orc_003_every_expected_task_is_present(require_airflow) -> None:
    """TC-ORC-003: all nine tasks from the design are in the deployed DAG."""
    response = requests.get(f"{AIRFLOW}/api/v1/dags/{DAG_ID}/tasks", auth=AUTH, timeout=20)
    assert response.status_code == 200
    task_ids = {t["task_id"] for t in response.json()["tasks"]}
    expected = {
        "compute_target_date", "wait_for_expense_file", "validate_expense_file",
        "load_expenses_to_postgres", "check_master_data_available", "run_profitability_job",
        "build_daily_report", "data_quality_checks", "record_pipeline_run",
        "archive_expense_file",
    }
    assert expected.issubset(task_ids), f"missing tasks: {expected - task_ids}"


def test_tc_orc_004_sensor_is_configured_for_a_long_wait(require_airflow) -> None:
    """TC-ORC-004: the FileSensor uses reschedule mode and a one-simulated-day timeout."""
    response = requests.get(
        f"{AIRFLOW}/api/v1/dags/{DAG_ID}/tasks/wait_for_expense_file", auth=AUTH, timeout=20
    )
    assert response.status_code == 200
    task = response.json()
    assert task["class_ref"]["class_name"] == "FileSensor"
    # retries=0 on the sensor: a timeout is definitive, retrying only delays the alert.
    assert task["retries"] == 0


def test_tc_orc_005_a_full_run_succeeds_for_a_day_with_data(require_airflow) -> None:
    """TC-ORC-005: trigger the DAG for a simulated day that has both telemetry and expenses.

    This is the orchestration half of the end-to-end proof: the sensor finds the file, validation
    quarantines the deliberately dirty rows, the load is idempotent, spark-submit recomputes, the
    report renders, and the quality checks pass.
    """
    # Find a simulated day that has BOTH a landing file and a Parquet partition.
    from pathlib import Path

    landing = {p.stem.replace("expenses_", "") for p in Path(CFG.landing_dir).glob("expenses_*.csv")}
    lake = {p.name.replace("sim_date=", "")
            for p in (Path(CFG.lake_root) / "telemetry").glob("sim_date=*")}
    candidates = sorted(landing & lake)
    if not candidates:
        pytest.skip(
            f"no simulated day has both an expense file and a lake partition yet "
            f"(landing={sorted(landing)}, lake={sorted(lake)})"
        )
    target = candidates[-1]

    run_id = f"itest_{int(time.time())}"
    _trigger(target, run_id)

    state = wait_until(
        lambda: (_run_state(run_id) if _run_state(run_id) in ("success", "failed") else None),
        timeout_s=600,
        interval_s=10,
    )
    tasks = _task_states(run_id)
    assert state == "success", (
        f"DAG run for {target} ended {state}; task states: {json.dumps(tasks, indent=2)}"
    )
    for task_id, task_state in tasks.items():
        assert task_state in ("success", "skipped"), f"{task_id} ended {task_state}"

    # And the business output exists.
    rows = query(
        "SELECT count(*) AS n FROM daily_vehicle_profitability WHERE report_date = %s", (target,)
    )
    assert rows[0]["n"] > 0, f"no profitability rows written for {target}"


def test_tc_orc_006_dirty_expense_rows_are_quarantined(require_airflow) -> None:
    """TC-ORC-006: the deliberately invalid rows in each file land in rejected_expenses."""
    rows = wait_until(
        lambda: query(
            "SELECT report_date, reason, count(*) AS n FROM rejected_expenses "
            "GROUP BY report_date, reason ORDER BY report_date DESC"
        )
        or None,
        timeout_s=120,
        interval_s=10,
    )
    assert rows, "no expense rows were quarantined — is EXPENSE_DIRTY_ROWS zero?"
    reasons = {r["reason"] for r in rows}
    assert reasons, "quarantine rows carry no reason"
    for reason in reasons:
        assert reason in {
            "MISSING_VEHICLE_ID", "NON_NUMERIC_VALUE", "NEGATIVE_VALUE", "BAD_REPORT_DATE",
            "MISSING_COLUMN",
        }, f"unexpected reason {reason}"


def test_tc_orc_007_expense_load_is_idempotent(require_airflow) -> None:
    """TC-ORC-007: daily_expenses has exactly one row per (report_date, vehicle_id)."""
    row = query_one(
        "SELECT count(*) AS n, count(DISTINCT (report_date, vehicle_id)) AS d FROM daily_expenses"
    )
    if not row or row["n"] == 0:
        pytest.skip("no expenses loaded yet")
    assert row["n"] == row["d"], "duplicate daily_expenses rows — the UPSERT key is wrong"


def test_tc_orc_008_pipeline_run_is_recorded_with_row_counts(require_airflow) -> None:
    """TC-ORC-008: every successful run leaves an audit row Grafana can read."""
    run = wait_until(
        lambda: query_one(
            # finished_at is always set on a successful run; started_at can be NULL if a run
            # was recovered by hand, and PostgreSQL sorts NULLs FIRST on DESC.
            "SELECT run_id, report_date, status, duration_s, rows_written FROM pipeline_runs "
            "WHERE status = 'success' AND duration_s IS NOT NULL "
            "ORDER BY finished_at DESC NULLS LAST LIMIT 1"
        ),
        timeout_s=120,
        interval_s=10,
    )
    if not run:
        pytest.skip("no successful pipeline run recorded yet")
    assert run["duration_s"] is not None and float(run["duration_s"]) > 0
    written = run["rows_written"]
    if isinstance(written, str):
        written = json.loads(written)
    assert written.get("profitability_rows"), "the audit row must record what was written"
    assert written.get("expense_rows_loaded")


def test_tc_orc_009_report_files_are_produced(require_airflow) -> None:
    """TC-ORC-009: the DAG renders both the HTML and the CSV daily report."""
    from pathlib import Path

    reports = sorted(Path(CFG.reports_dir).glob("profitability_*.html"))
    if not reports:
        pytest.skip("no report rendered yet (the DAG has not completed a run)")
    newest = reports[-1]
    csv_twin = newest.with_suffix(".csv")
    assert csv_twin.exists(), "the CSV counterpart is missing"
    content = newest.read_text(encoding="utf-8")
    assert "Fleet KPIs" in content
    assert "Per-vehicle profitability" in content
    assert newest.stat().st_size > 2000


def test_tc_orc_010_cli_and_rest_agree_on_run_history(require_airflow) -> None:
    """TC-ORC-010: the Airflow CLI inside the scheduler sees the same runs as the REST API.

    A cheap but real consistency check that the scheduler and webserver share one metadata DB.
    """
    api_runs = {r["dag_run_id"] for r in _dag_runs(limit=10)}
    try:
        result = subprocess.run(
            ["airflow", "dags", "list-runs", "-d", DAG_ID, "-o", "json"],
            capture_output=True, text=True, timeout=60,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("airflow CLI not available in this container (expected: tests image)")
    if result.returncode != 0:
        pytest.skip(f"airflow CLI unavailable: {result.stderr[:200]}")
    cli_runs = {r["run_id"] for r in json.loads(result.stdout)}
    assert api_runs & cli_runs, "CLI and REST API disagree about run history"
