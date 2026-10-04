"""Airflow DAG: nightly reconciliation of one simulated day (orchestration).

What it does
------------
Once per simulated day (``SIM_DAY_SECONDS`` of real time) the DAG:

1. works out which simulated day has just finished,
2. **waits** for that day's expense file to land (a real dependency on an external source),
3. validates it row by row, quarantining bad rows and failing the run if the file is mostly junk,
4. loads the good rows into ``daily_expenses`` with an idempotent UPSERT,
5. checks the Parquet master dataset actually has that day's partition,
6. ``spark-submit``s the profitability job, which recomputes revenue from raw events and joins
   it with the costs,
7. renders the consolidated HTML + CSV daily report,
8. runs data-quality assertions over the result,
9. records the run in ``pipeline_runs`` for the dashboard and the report.

Design notes worth defending
----------------------------
* **The sensor is the whole point.** A batch layer is only interesting if it has a real
  dependency it can wait on, time out on, and alert about.  ``mode="reschedule"`` releases the
  worker slot between pokes, so a sensor waiting a whole simulated day costs nothing.
* **The target date is computed in a task, not with ``{{ ds }}``.** Airflow's ``ds`` is the
  *logical* date of the run on the real calendar; this pipeline lives on the simulated calendar,
  so the mapping has to be explicit.  It is passed on by XCom and can be overridden with
  ``--conf '{"date": "2026-09-02"}'`` for a backfill.
* **Every write is idempotent**, so triggering the same date twice produces identical rows.  That
  is the Lambda recomputation property, and TC-BAT-010 asserts it.
* **``on_failure_callback`` writes to ``pipeline_alerts``**, giving orchestration failures the
  same treatment as the Prometheus alerts on the streaming side.
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

# The project modules are bind-mounted at /opt/airflow/project and put on PYTHONPATH by the
# image; this insert keeps the DAG importable even if that env var is lost.
PROJECT_ROOT = "/opt/airflow/project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.config import CFG  # noqa: E402
from common.db import execute, execute_many, query, query_one  # noqa: E402
from common.logging_setup import setup_logging  # noqa: E402
from common.schemas import EXPENSE_FIELDS, expense_reject_reason  # noqa: E402
from common.sim_clock import previous_sim_date  # noqa: E402

# force=False: this module is re-imported inside every task subprocess, where Airflow has
# already installed its own root handler to capture that task's log. Replacing it would
# break the task runner (DEFECT-012). Outside a task the root logger is empty, so this
# still configures JSON logging for DAG parsing.
LOG = setup_logging("airflow", "orchestration", force=False)

DAG_ID = "daily_reconciliation"
TARGET_DATE_TASK = "compute_target_date"


# ============================================================================================
# Helpers
# ============================================================================================
def _target_date(ti: Any) -> str:
    """Pull the simulated date decided by the first task."""
    value = ti.xcom_pull(task_ids=TARGET_DATE_TASK)
    if not value:
        raise AirflowFailException("target date was not computed")
    return str(value)


def _run_id(context: dict[str, Any]) -> str:
    """A stable identifier for this DAG run, propagated into Spark and every table.

    Airflow's ``run_id`` contains characters that are awkward in shell arguments, so it is
    normalised here and then used verbatim everywhere else — this is the "tracing-lite" handle
    that lets one daily run be followed across Airflow, Spark, Postgres and the report.
    """
    raw = context["run_id"]
    return raw.replace(":", "").replace("+", "").replace(".", "").replace("__", "_")[:60]


def alert_on_failure(context: dict[str, Any]) -> None:
    """Record an orchestration failure in ``pipeline_alerts`` and log it as structured ERROR."""
    task = context.get("task_instance")
    exception = context.get("exception")
    payload = {
        "dag_id": context["dag"].dag_id,
        "task_id": task.task_id if task else None,
        "run_id": _run_id(context),
        "try_number": task.try_number if task else None,
        "exception": str(exception)[:2000] if exception else None,
    }
    LOG.error(
        "airflow task failed",
        extra={"event": "dag_task_failed", "stage": "orchestration", **payload},
    )
    try:
        execute(
            """
            INSERT INTO pipeline_alerts
                (alert_name, severity, dag_id, task_id, run_id, message, context)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb);
            """,
            (
                f"AirflowTaskFailed:{payload['task_id']}",
                "critical",
                payload["dag_id"],
                payload["task_id"],
                payload["run_id"],
                str(exception)[:500] if exception else "task failed",
                json.dumps(payload),
            ),
        )
    except Exception as exc:  # noqa: BLE001 - the alert sink must never mask the real failure
        LOG.error(
            "could not record pipeline alert",
            extra={"event": "pipeline_alert_write_failed", "error": str(exc)},
        )


# ============================================================================================
# Task callables
# ============================================================================================
def compute_target_date(**context: Any) -> str:
    """Decide which simulated day to reconcile.

    Priority: an explicit ``--conf '{"date": ...}'`` (manual backfill) beats the clock.  Without
    it, the day that has just finished on the simulated clock is used.
    """
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    override = conf.get("date")
    target = str(override) if override else previous_sim_date().isoformat()
    LOG.info(
        "target simulated date resolved",
        extra={
            "event": "target_date_resolved",
            "stage": "orchestration",
            "report_date": target,
            "source": "dag_run.conf" if override else "sim_clock",
            "run_id": _run_id(context),
        },
    )
    return target


def validate_expense_file(**context: Any) -> dict[str, Any]:
    """Row-by-row validation with quarantine, using the SHARED rule set.

    Fails the task when more than ``MAX_BAD_EXPENSE_ROW_PCT`` of rows are bad: a couple of dirty
    rows is normal operational noise, but a mostly-broken file means the upstream system changed
    and loading it would silently corrupt the profitability numbers.
    """
    ti = context["ti"]
    report_date = _target_date(ti)
    run_id = _run_id(context)
    path = Path(CFG.landing_dir) / f"expenses_{report_date}.csv"

    if not path.exists():
        raise AirflowFailException(f"expense file disappeared before validation: {path}")

    good: list[dict[str, str]] = []
    bad: list[tuple[Any, ...]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing_cols = set(EXPENSE_FIELDS) - set(reader.fieldnames or [])
        if missing_cols:
            raise AirflowFailException(
                f"expense file header is wrong; missing columns: {sorted(missing_cols)}"
            )
        for row in reader:
            reason = expense_reject_reason(row, expected_date=report_date)
            if reason is None:
                good.append(row)
            else:
                bad.append((report_date, json.dumps(row), reason, str(path), run_id))

    total = len(good) + len(bad)
    if total == 0:
        raise AirflowFailException(f"expense file {path} contains no data rows")

    if bad:
        execute_many(
            """
            INSERT INTO rejected_expenses (report_date, raw_row, reason, source_file, run_id)
            VALUES (%s, %s, %s, %s, %s);
            """,
            bad,
        )

    bad_pct = len(bad) / total
    summary = {
        "report_date": report_date,
        "rows_total": total,
        "rows_valid": len(good),
        "rows_rejected": len(bad),
        "bad_pct": round(bad_pct, 4),
        "threshold": CFG.max_bad_expense_row_pct,
        "source_file": str(path),
    }
    LOG.info(
        "expense file validated",
        extra={"event": "expense_validated", "stage": "orchestration", "run_id": run_id, **summary},
    )
    if bad_pct > CFG.max_bad_expense_row_pct:
        raise AirflowFailException(
            f"{bad_pct:.1%} of rows in {path.name} are invalid "
            f"(threshold {CFG.max_bad_expense_row_pct:.0%}); refusing to load"
        )
    return summary


def load_expenses_to_postgres(**context: Any) -> int:
    """Idempotent UPSERT of the validated rows into ``daily_expenses``."""
    ti = context["ti"]
    report_date = _target_date(ti)
    run_id = _run_id(context)
    path = Path(CFG.landing_dir) / f"expenses_{report_date}.csv"

    rows: list[tuple[Any, ...]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if expense_reject_reason(row, expected_date=report_date) is not None:
                continue
            rows.append(
                (
                    report_date,
                    row["vehicle_id"].strip(),
                    float(row["fuel_cost"]),
                    float(row["maintenance_cost"]),
                    float(row["distance_covered"]),
                    int(float(row["service_flag"])),
                    str(path),
                    run_id,
                )
            )

    written = execute_many(
        """
        INSERT INTO daily_expenses
            (report_date, vehicle_id, fuel_cost, maintenance_cost, distance_covered,
             service_flag, source_file, run_id, loaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (report_date, vehicle_id) DO UPDATE SET
            fuel_cost        = EXCLUDED.fuel_cost,
            maintenance_cost = EXCLUDED.maintenance_cost,
            distance_covered = EXCLUDED.distance_covered,
            service_flag     = EXCLUDED.service_flag,
            source_file      = EXCLUDED.source_file,
            run_id           = EXCLUDED.run_id,
            loaded_at        = now();
        """,
        rows,
    )
    LOG.info(
        "expenses loaded",
        extra={
            "event": "expenses_loaded",
            "stage": "storage",
            "report_date": report_date,
            "rows": written,
            "run_id": run_id,
        },
    )
    return written


def archive_expense_file(**context: Any) -> str:
    """Copy the processed file to ``/data/processed``.

    The spec asks for the file to be moved out of the landing area but kept for replay, so it is
    *copied*: the landing copy makes a second (idempotent) DAG run for the same date possible
    without regenerating the source, while the processed copy is the durable audit record.
    """
    ti = context["ti"]
    report_date = _target_date(ti)
    src = Path(CFG.landing_dir) / f"expenses_{report_date}.csv"
    dst_dir = Path(CFG.processed_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    shutil.copy2(src, dst)
    LOG.info(
        "expense file archived",
        extra={
            "event": "expense_archived",
            "stage": "storage",
            "source": str(src),
            "destination": str(dst),
        },
    )
    return str(dst)


def check_master_data_available(**context: Any) -> dict[str, Any]:
    """Assert the Parquet partition for the target day exists and is non-trivial.

    Without this, a missing partition would surface as a confusing Spark error deep inside the
    profitability job.  Failing here says plainly: the speed layer did not produce this day.
    """
    ti = context["ti"]
    report_date = _target_date(ti)
    partition = Path(CFG.lake_root) / "telemetry" / f"sim_date={report_date}"
    if not partition.exists():
        raise AirflowFailException(
            f"master dataset partition missing: {partition}. The streaming job has not written "
            f"simulated day {report_date} (is stream-job running?)."
        )
    files = [p for p in partition.rglob("*.parquet") if p.is_file()]
    total_bytes = sum(p.stat().st_size for p in files)
    summary = {
        "report_date": report_date,
        "partition": str(partition),
        "parquet_files": len(files),
        "bytes": total_bytes,
    }
    LOG.info(
        "master dataset partition verified",
        extra={"event": "master_partition_ok", "stage": "processing", **summary},
    )
    if not files:
        raise AirflowFailException(f"partition {partition} exists but contains no parquet files")
    return summary


def build_daily_report(**context: Any) -> dict[str, str]:
    """Render the consolidated HTML + CSV report for the day."""
    ti = context["ti"]
    report_date = _target_date(ti)
    from batch.report_builder import build_report  # imported lazily: keeps DAG parsing fast

    paths = build_report(report_date, run_id=_run_id(context))
    LOG.info(
        "daily report rendered",
        extra={"event": "report_built", "stage": "serving", "report_date": report_date, **paths},
    )
    return paths


def data_quality_checks(**context: Any) -> dict[str, Any]:
    """Post-load assertions over the reconciled day.

    These are *business* invariants, not schema checks: they are the kind of thing that catches a
    join going wrong in a way that still produces well-typed rows.
    """
    ti = context["ti"]
    report_date = _target_date(ti)
    checks: dict[str, Any] = {"report_date": report_date}
    failures: list[str] = []

    row_count = query_one(
        "SELECT count(*) AS n FROM daily_vehicle_profitability WHERE report_date = %s",
        (report_date,),
    )
    checks["profitability_rows"] = row_count["n"] if row_count else 0
    if checks["profitability_rows"] == 0:
        failures.append("no profitability rows were produced")

    negative_revenue = query_one(
        "SELECT count(*) AS n FROM daily_vehicle_profitability "
        "WHERE report_date = %s AND revenue_lkr < 0",
        (report_date,),
    )
    checks["negative_revenue_rows"] = negative_revenue["n"] if negative_revenue else 0
    if checks["negative_revenue_rows"]:
        failures.append("revenue must never be negative")

    # Every vehicle that produced telemetry must have a profitability row (the left join must
    # not have dropped anybody).
    coverage = query_one(
        """
        SELECT
          (SELECT count(DISTINCT vehicle_id) FROM daily_expenses WHERE report_date = %s) AS expense_vehicles,
          (SELECT count(*) FROM daily_vehicle_profitability WHERE report_date = %s) AS profit_rows,
          (SELECT count(*) FROM daily_vehicle_profitability
             WHERE report_date = %s AND data_quality_flag = 'MISSING_EXPENSE') AS missing_expense
        """,
        (report_date, report_date, report_date),
    )
    checks.update(coverage or {})
    if (coverage or {}).get("profit_rows", 0) < (coverage or {}).get("expense_vehicles", 0):
        failures.append("fewer profitability rows than vehicles with expenses — join lost rows")

    zone_rows = query_one(
        "SELECT count(*) AS n FROM daily_zone_summary WHERE report_date = %s", (report_date,)
    )
    checks["zone_summary_rows"] = zone_rows["n"] if zone_rows else 0
    if checks["zone_summary_rows"] == 0:
        failures.append("no zone x hour summary rows were produced")

    checks["failures"] = failures
    LOG.info(
        "data quality checks complete",
        extra={"event": "dq_checks", "stage": "processing", **checks},
    )
    if failures:
        raise AirflowFailException("; ".join(failures))
    return checks


def record_pipeline_run(**context: Any) -> str:
    """Write the audit row that Grafana's Pipeline Health dashboard reads."""
    ti = context["ti"]
    report_date = _target_date(ti)
    run_id = _run_id(context)

    validation = ti.xcom_pull(task_ids="validate_expense_file") or {}
    master = ti.xcom_pull(task_ids="check_master_data_available") or {}
    dq = ti.xcom_pull(task_ids="data_quality_checks") or {}
    loaded = ti.xcom_pull(task_ids="load_expenses_to_postgres")
    report = ti.xcom_pull(task_ids="build_daily_report") or {}

    dag_run = context["dag_run"]
    started = dag_run.start_date
    now = datetime.now(tz=started.tzinfo) if started else datetime.utcnow()
    duration = (now - started).total_seconds() if started else None

    rows_written = {
        "expense_rows_valid": validation.get("rows_valid"),
        "expense_rows_rejected": validation.get("rows_rejected"),
        "expense_rows_loaded": loaded,
        "parquet_files": master.get("parquet_files"),
        "profitability_rows": dq.get("profitability_rows"),
        "zone_summary_rows": dq.get("zone_summary_rows"),
        "missing_expense": dq.get("missing_expense"),
        "report_html": report.get("html"),
        "report_csv": report.get("csv"),
    }
    execute(
        """
        INSERT INTO pipeline_runs
            (run_id, dag_id, report_date, status, started_at, finished_at, duration_s, rows_written, notes)
        VALUES (%s, %s, %s, 'success', %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            finished_at = EXCLUDED.finished_at,
            duration_s = EXCLUDED.duration_s,
            rows_written = EXCLUDED.rows_written,
            notes = EXCLUDED.notes;
        """,
        (
            run_id,
            DAG_ID,
            report_date,
            started,
            now,
            round(duration, 2) if duration else None,
            json.dumps(rows_written),
            f"reconciled simulated day {report_date}",
        ),
    )
    LOG.info(
        "pipeline run recorded",
        extra={
            "event": "pipeline_run_recorded",
            "stage": "orchestration",
            "run_id": run_id,
            "report_date": report_date,
            **{k: v for k, v in rows_written.items() if v is not None},
        },
    )
    return run_id


# ============================================================================================
# DAG definition
# ============================================================================================
default_args = {
    "owner": "fleet-ops",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": alert_on_failure,
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id=DAG_ID,
    description="Reconcile one simulated day: expenses + telemetry -> per-vehicle profitability",
    # One run per simulated day. With SIM_DAY_SECONDS=600 that is every 10 real minutes.
    schedule=timedelta(seconds=CFG.sim_day_seconds),
    start_date=datetime(2026, 9, 1),
    catchup=False,          # never try to backfill the real calendar; the sim clock drives us
    max_active_runs=1,      # a day must finish before the next starts (Postgres is the shared state)
    default_args=default_args,
    tags=["fleet", "lambda", "batch-layer"],
    doc_md=__doc__,
) as dag:

    t_target_date = PythonOperator(
        task_id=TARGET_DATE_TASK,
        python_callable=compute_target_date,
        doc_md="Resolve the simulated day to reconcile (conf override wins over the sim clock).",
    )

    # mode="reschedule" frees the worker slot between pokes — essential because this sensor may
    # legitimately wait most of a simulated day.
    t_wait_file = FileSensor(
        task_id="wait_for_expense_file",
        fs_conn_id="fs_default",
        filepath=(
            CFG.landing_dir
            + "/expenses_{{ ti.xcom_pull(task_ids='"
            + TARGET_DATE_TASK
            + "') }}.csv"
        ),
        poke_interval=15,
        # One simulated day: if the file has not arrived by the time the next day starts, it is
        # not coming. This is the timeout the SKIP_DAY failure scenario trips.
        timeout=CFG.sim_day_seconds,
        mode="reschedule",
        soft_fail=False,
        # retries=0 overrides the DAG default: a sensor TIMEOUT is a definitive "the file is not
        # coming", so retrying it two more times would just delay the alert by another two
        # simulated days without changing the outcome.
        retries=0,
        doc_md="Wait for the daily expense CSV. Times out after one simulated day, then fails.",
    )

    t_validate = PythonOperator(
        task_id="validate_expense_file",
        python_callable=validate_expense_file,
        doc_md="Row-level validation; quarantines bad rows, fails if >20% are invalid.",
    )

    t_load = PythonOperator(
        task_id="load_expenses_to_postgres",
        python_callable=load_expenses_to_postgres,
        doc_md="Idempotent UPSERT into daily_expenses.",
    )

    t_check_master = PythonOperator(
        task_id="check_master_data_available",
        python_callable=check_master_data_available,
        doc_md="Verify the Parquet master-dataset partition for the day exists.",
    )

    # spark-submit runs the batch job as a separate process so a Spark failure cannot take the
    # scheduler down with it; the exit code is the contract.
    t_profitability = BashOperator(
        task_id="run_profitability_job",
        bash_command=(
            "set -euo pipefail; "
            "spark-submit "
            "  --master local[2] "
            # 512m is ample for one simulated day (~7,500 rows) and, critically, the LocalExecutor
            # runs this process INSIDE the scheduler container, so the driver heap competes
            # with Airflow itself for the scheduler cgroup (DEFECT-012: OOM at 900m).
            "  --driver-memory 512m "
            "  --conf spark.sql.session.timeZone=UTC "
            "  --conf spark.sql.shuffle.partitions=4 "
            "  --conf spark.ui.enabled=false "
            f"  --conf spark.executorEnv.PYTHONPATH={PROJECT_ROOT} "
            f"  {PROJECT_ROOT}/batch/profitability_job.py "
            "  --date {{ ti.xcom_pull(task_ids='" + TARGET_DATE_TASK + "') }} "
            "  --run-id {{ run_id | replace(':','') | replace('+','') | replace('.','') }} "
            "  --summary-out /data/reports/summary_{{ ti.xcom_pull(task_ids='"
            + TARGET_DATE_TASK
            + "') }}.json"
        ),
        env={
            "PYTHONPATH": PROJECT_ROOT,
            "JAVA_HOME": os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"),
            **{k: v for k, v in os.environ.items() if k.isupper()},
        },
        append_env=False,
        doc_md="Recompute revenue from Parquet and join with costs; writes profitability tables.",
    )

    t_report = PythonOperator(
        task_id="build_daily_report",
        python_callable=build_daily_report,
        doc_md="Render /data/reports/profitability_<D>.html and .csv.",
    )

    t_dq = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
        doc_md="Business invariants over the reconciled day.",
    )

    t_archive = PythonOperator(
        task_id="archive_expense_file",
        python_callable=archive_expense_file,
        doc_md="Copy the processed file to /data/processed (landing copy kept for replay).",
    )

    t_record = PythonOperator(
        task_id="record_pipeline_run",
        python_callable=record_pipeline_run,
        trigger_rule="all_success",
        doc_md="Audit row for Grafana and the report.",
    )

    # The master-data check runs in parallel with the file path: they are independent inputs and
    # failing fast on either is better than serialising them.
    t_target_date >> t_wait_file >> t_validate >> t_load >> t_profitability
    t_target_date >> t_check_master >> t_profitability
    t_load >> t_archive
    t_profitability >> t_report >> t_dq >> t_record
