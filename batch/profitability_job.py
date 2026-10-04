"""Batch layer: per-vehicle daily profitability reconciliation (processing, 15 marks).

What this job is for
--------------------
The speed layer can tell you what the fleet is doing *right now*, but it cannot tell you whether
a vehicle is making money, because costs arrive once a day in a file.  This job answers the
second half of the business question:

    *which vehicles are becoming unprofitable once yesterday's fuel/maintenance costs are
    factored in?*

Why it recomputes instead of reading the speed layer's tables
-------------------------------------------------------------
This is the defining property of a Lambda architecture.  The job reads the **immutable Parquet
master dataset** and recomputes revenue, trips and utilisation from raw events.  It therefore:

* is unaffected by anything the speed layer approximated (``approx_count_distinct``) or dropped
  (events later than the 2-minute watermark);
* produces the same answer every time it is run for the same day, so a backfill after a bug fix
  or a corrected cost file simply overwrites the old answer (REQ-11, TC-BAT-010);
* needs no state carried over from the streaming job.

Run: ``python -m batch.profitability_job --date 2026-09-01 [--run-id abc123]``
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import CFG
from common.db import execute_many, wait_for_postgres
from common.logging_setup import RUN_ID, setup_logging
from common.schemas import ACTIVE_STATUSES

LOG = setup_logging("batch-profitability", "processing")

#: How many SIMULATED minutes one telemetry event accounts for.
#:
#: Each vehicle emits every ``EMIT_INTERVAL_SEC`` real seconds, and one real second is
#: ``86400/SIM_DAY_SECONDS`` simulated seconds.  With the defaults that is
#: ``2 * 144 / 60 = 4.8`` simulated minutes per event, so a full simulated day of events for one
#: vehicle (300 events) sums to 1440 minutes = 24 h.  Expressing utilisation in simulated minutes
#: is what makes the daily report read like a real operations report.
SIM_MINUTES_PER_EVENT = CFG.emit_interval_sec * (86400.0 / CFG.sim_day_seconds) / 60.0


# ============================================================================================
# Spark
# ============================================================================================
def build_spark(app_suffix: str) -> SparkSession:
    """Local-mode SparkSession for a one-shot batch run.

    A batch job gets a *smaller* shuffle-partition count than the streaming job: it processes a
    single day (tens of thousands of rows), so more partitions would only add task overhead.
    """
    return (
        SparkSession.builder.master(CFG.spark_master)
        .appName(f"fleet-batch-profitability-{app_suffix}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_master_dataset(spark: SparkSession, report_date: date) -> DataFrame:
    """Read exactly one ``sim_date`` partition of the Parquet master dataset.

    ``basePath`` is set so Spark still materialises ``sim_date`` as a column even though the
    partition path is addressed directly — addressing the path directly is what makes this a
    partition *prune* rather than a full-table scan with a filter.
    """
    base = f"{CFG.lake_root}/telemetry"
    partition = f"{base}/sim_date={report_date.isoformat()}"
    if not Path(partition).exists():
        raise FileNotFoundError(
            f"master dataset partition missing: {partition} — the streaming job has not yet "
            f"written simulated day {report_date}"
        )
    return spark.read.option("basePath", base).parquet(partition)


def read_expenses(spark: SparkSession, report_date: date) -> DataFrame:
    """Read the validated expense rows for ``report_date`` over JDBC.

    JDBC (rather than pulling the rows through Python) keeps the join inside Spark and shows the
    driver jar earning its place.  A pushdown predicate is embedded in the subquery so only one
    day crosses the wire.
    """
    subquery = (
        "(SELECT vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag "
        f"FROM daily_expenses WHERE report_date = DATE '{report_date.isoformat()}') AS e"
    )
    return (
        spark.read.format("jdbc")
        .option("url", CFG.pg_jdbc_url)
        .option("dbtable", subquery)
        .option("user", CFG.postgres_user)
        .option("password", CFG.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def read_previous_profitability(spark: SparkSession, report_date: date) -> DataFrame:
    """Margins for the two preceding days, used by the trend rule.

    Returns columns ``vehicle_id, margin_d1, margin_d2`` (``d1`` = yesterday relative to
    ``report_date``).  Missing days simply come back as NULL, which the trend rule treats as
    "not enough history" -> STABLE.
    """
    d1 = (report_date - timedelta(days=1)).isoformat()
    d2 = (report_date - timedelta(days=2)).isoformat()
    subquery = (
        "(SELECT vehicle_id, "
        f"  max(CASE WHEN report_date = DATE '{d1}' THEN margin END) AS margin_d1, "
        f"  max(CASE WHEN report_date = DATE '{d2}' THEN margin END) AS margin_d2 "
        "FROM daily_vehicle_profitability "
        f"WHERE report_date IN (DATE '{d1}', DATE '{d2}') "
        "GROUP BY vehicle_id) AS p"
    )
    return (
        spark.read.format("jdbc")
        .option("url", CFG.pg_jdbc_url)
        .option("dbtable", subquery)
        .option("user", CFG.postgres_user)
        .option("password", CFG.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


# ============================================================================================
# Transformations (pure; unit-tested against hand-calculated fixtures in TC-BAT-001..009)
# ============================================================================================
def vehicle_day_metrics(events: DataFrame) -> DataFrame:
    """Recompute per-vehicle revenue, trips and utilisation from raw events.

    Revenue is deduplicated by ``trip_id`` before summing.  The producer emits a fare exactly
    once per trip, but the pipeline is at-least-once: without this dedup a replayed Kafka batch
    could count one trip's fare twice and overstate revenue.  Deduplicating on the *business*
    key (``trip_id``) rather than the technical key (``event_id``) is the stronger guarantee.
    """
    # --- revenue and trips ------------------------------------------------------------------
    paying = (
        events.where((F.col("fare") > 0) & F.col("trip_id").isNotNull())
        .select("vehicle_id", "trip_id", "fare")
        .dropDuplicates(["trip_id"])
    )
    revenue = paying.groupBy("vehicle_id").agg(
        F.round(F.sum("fare"), 2).alias("revenue_lkr"),
        F.count(F.lit(1)).cast("int").alias("trips"),
    )

    # --- utilisation -------------------------------------------------------------------------
    activity = events.groupBy("vehicle_id").agg(
        F.first("driver_id", ignorenulls=True).alias("driver_id"),
        F.sum(
            F.when(F.col("status").isin(list(ACTIVE_STATUSES)), F.lit(1)).otherwise(F.lit(0))
        ).alias("active_events"),
        F.sum(
            F.when(F.col("status").isin(list(ACTIVE_STATUSES)), F.lit(0)).otherwise(F.lit(1))
        ).alias("idle_events"),
        F.count(F.lit(1)).alias("total_events"),
    )

    return (
        activity.join(revenue, on="vehicle_id", how="left")
        .withColumn("revenue_lkr", F.coalesce(F.col("revenue_lkr"), F.lit(0.0)))
        .withColumn("trips", F.coalesce(F.col("trips"), F.lit(0)))
        .withColumn(
            "active_minutes",
            F.round(F.col("active_events") * F.lit(SIM_MINUTES_PER_EVENT), 2),
        )
        .withColumn(
            "idle_minutes", F.round(F.col("idle_events") * F.lit(SIM_MINUTES_PER_EVENT), 2)
        )
        .withColumn(
            "utilization",
            # greatest(..., 1) is the divide-by-zero guard: a vehicle with no events at all
            # would otherwise produce NaN and poison every downstream average.
            F.round(
                F.col("active_events") / F.greatest(F.col("total_events"), F.lit(1)), 4
            ),
        )
    )


def zone_hour_summary(events: DataFrame) -> DataFrame:
    """Exact zone x simulated-hour aggregates — the batch counterpart of the speed layer.

    Grouping uses ``sim_hour`` (the simulated clock) rather than ``event_time``: that is what
    makes "earnings by time of day" meaningful after ten real minutes.
    """
    return (
        events.groupBy("zone", "sim_hour")
        .agg(
            F.sum(F.when(F.col("fare") > 0, F.lit(1)).otherwise(F.lit(0))).cast("int").alias("trips"),
            F.round(F.sum("fare"), 2).alias("earnings_lkr"),
            F.sum(
                F.when(F.col("status").isin(list(ACTIVE_STATUSES)), F.lit(1)).otherwise(F.lit(0))
            ).cast("int").alias("active_events"),
            F.sum(
                F.when(F.col("status").isin(list(ACTIVE_STATUSES)), F.lit(0)).otherwise(F.lit(1))
            ).cast("int").alias("idle_events"),
            F.round(F.avg(F.when(F.col("speed") > 0, F.col("speed"))), 2).alias("avg_speed_kmh"),
        )
        .withColumn(
            "utilization",
            F.round(
                F.col("active_events")
                / F.greatest(F.col("active_events") + F.col("idle_events"), F.lit(1)),
                4,
            ),
        )
        .withColumn("avg_speed_kmh", F.coalesce(F.col("avg_speed_kmh"), F.lit(0.0)))
        .na.fill({"zone": "OUT_OF_AREA"})
    )


def join_costs_and_score(
    metrics: DataFrame,
    expenses: DataFrame,
    previous: DataFrame,
    margin_threshold: float = CFG.margin_threshold,
) -> DataFrame:
    """Join telemetry-derived metrics with costs and apply the profitability rules.

    The join is a **left outer join from the telemetry side**.  A vehicle that drove but has no
    cost row must still appear, flagged ``MISSING_EXPENSE``, because silently dropping it would
    understate fleet revenue and hide a data-quality problem.  A ``full_outer`` join additionally
    surfaces cost rows for vehicles that produced no telemetry (``NO_TELEMETRY``).

    Formulae (reproduced verbatim in the report):

    * ``total_cost   = fuel_cost + maintenance_cost``
    * ``profit       = revenue - total_cost``
    * ``margin       = profit / revenue``            (NULL when revenue = 0)
    * ``cost_per_km  = total_cost / distance_km``    (NULL when distance = 0)
    * ``revenue_per_km = revenue / distance_km``
    * ``is_unprofitable = profit < 0``
    * ``trend`` = ``AT_RISK``   when margin < threshold on this day *and* the previous day
                  ``DECLINING`` when margin fell on two consecutive days
                  ``STABLE``    otherwise
      AT_RISK takes precedence: an absolute loss-making level is a stronger signal than a
      downward slope that may still be comfortably profitable.
    """
    joined = metrics.join(expenses, on="vehicle_id", how="full_outer")

    scored = (
        joined.withColumn("revenue_lkr", F.coalesce(F.col("revenue_lkr"), F.lit(0.0)))
        .withColumn("trips", F.coalesce(F.col("trips"), F.lit(0)))
        .withColumn("active_minutes", F.coalesce(F.col("active_minutes"), F.lit(0.0)))
        .withColumn("idle_minutes", F.coalesce(F.col("idle_minutes"), F.lit(0.0)))
        .withColumn("utilization", F.coalesce(F.col("utilization"), F.lit(0.0)))
        .withColumn("fuel_cost", F.coalesce(F.col("fuel_cost"), F.lit(0.0)).cast("double"))
        .withColumn(
            "maintenance_cost", F.coalesce(F.col("maintenance_cost"), F.lit(0.0)).cast("double")
        )
        .withColumn("distance_km", F.coalesce(F.col("distance_covered"), F.lit(0.0)).cast("double"))
        .withColumn(
            "total_cost_lkr", F.round(F.col("fuel_cost") + F.col("maintenance_cost"), 2)
        )
        .withColumn("profit_lkr", F.round(F.col("revenue_lkr") - F.col("total_cost_lkr"), 2))
        # NULLIF-style guards: a zero denominator yields NULL, not NaN or an exception.
        .withColumn(
            "margin",
            F.when(F.col("revenue_lkr") > 0,
                   F.round(F.col("profit_lkr") / F.col("revenue_lkr"), 4)).otherwise(F.lit(None)),
        )
        .withColumn(
            "cost_per_km",
            F.when(F.col("distance_km") > 0,
                   F.round(F.col("total_cost_lkr") / F.col("distance_km"), 4)).otherwise(F.lit(None)),
        )
        .withColumn(
            "revenue_per_km",
            F.when(F.col("distance_km") > 0,
                   F.round(F.col("revenue_lkr") / F.col("distance_km"), 4)).otherwise(F.lit(None)),
        )
        .withColumn("is_unprofitable", F.col("profit_lkr") < 0)
        # The flag is decided by which side of the full outer join supplied the row.  This is
        # checked BEFORE the coalesce above would hide the NULL, which is why distance_covered
        # (the raw joined column) is tested rather than distance_km (the filled one).
        .withColumn(
            "data_quality_flag",
            F.when(F.col("total_events").isNull(), F.lit("NO_TELEMETRY"))
            .when(F.col("distance_covered").isNull(), F.lit("MISSING_EXPENSE"))
            .otherwise(F.lit("OK")),
        )
    )

    with_history = scored.join(previous, on="vehicle_id", how="left")
    threshold = F.lit(margin_threshold)
    at_risk = (
        F.col("margin").isNotNull()
        & F.col("margin_d1").isNotNull()
        & (F.col("margin") < threshold)
        & (F.col("margin_d1") < threshold)
    )
    declining = (
        F.col("margin").isNotNull()
        & F.col("margin_d1").isNotNull()
        & F.col("margin_d2").isNotNull()
        & (F.col("margin") < F.col("margin_d1"))
        & (F.col("margin_d1") < F.col("margin_d2"))
    )
    return with_history.withColumn(
        "trend",
        F.when(at_risk, F.lit("AT_RISK")).when(declining, F.lit("DECLINING")).otherwise(
            F.lit("STABLE")
        ),
    )


# ============================================================================================
# Persistence (idempotent UPSERTs)
# ============================================================================================
UPSERT_PROFITABILITY = """
INSERT INTO daily_vehicle_profitability
    (report_date, vehicle_id, driver_id, revenue_lkr, trips, active_minutes, idle_minutes,
     utilization, distance_km, fuel_cost, maintenance_cost, total_cost_lkr, profit_lkr, margin,
     cost_per_km, revenue_per_km, is_unprofitable, trend, data_quality_flag, run_id, computed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (report_date, vehicle_id) DO UPDATE SET
    driver_id         = EXCLUDED.driver_id,
    revenue_lkr       = EXCLUDED.revenue_lkr,
    trips             = EXCLUDED.trips,
    active_minutes    = EXCLUDED.active_minutes,
    idle_minutes      = EXCLUDED.idle_minutes,
    utilization       = EXCLUDED.utilization,
    distance_km       = EXCLUDED.distance_km,
    fuel_cost         = EXCLUDED.fuel_cost,
    maintenance_cost  = EXCLUDED.maintenance_cost,
    total_cost_lkr    = EXCLUDED.total_cost_lkr,
    profit_lkr        = EXCLUDED.profit_lkr,
    margin            = EXCLUDED.margin,
    cost_per_km       = EXCLUDED.cost_per_km,
    revenue_per_km    = EXCLUDED.revenue_per_km,
    is_unprofitable   = EXCLUDED.is_unprofitable,
    trend             = EXCLUDED.trend,
    data_quality_flag = EXCLUDED.data_quality_flag,
    run_id            = EXCLUDED.run_id,
    computed_at       = now();
"""

UPSERT_ZONE_SUMMARY = """
INSERT INTO daily_zone_summary
    (report_date, zone, sim_hour, trips, earnings_lkr, active_events, idle_events, utilization,
     avg_speed_kmh, run_id, computed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (report_date, zone, sim_hour) DO UPDATE SET
    trips         = EXCLUDED.trips,
    earnings_lkr  = EXCLUDED.earnings_lkr,
    active_events = EXCLUDED.active_events,
    idle_events   = EXCLUDED.idle_events,
    utilization   = EXCLUDED.utilization,
    avg_speed_kmh = EXCLUDED.avg_speed_kmh,
    run_id        = EXCLUDED.run_id,
    computed_at   = now();
"""


def write_profitability(rows: list[dict[str, Any]], report_date: date, run_id: str) -> int:
    """UPSERT the per-vehicle results. Re-running a day overwrites rather than duplicates."""
    payload = [
        (
            report_date, r["vehicle_id"], r.get("driver_id"), r["revenue_lkr"], r["trips"],
            r["active_minutes"], r["idle_minutes"], r["utilization"], r["distance_km"],
            r["fuel_cost"], r["maintenance_cost"], r["total_cost_lkr"], r["profit_lkr"],
            r["margin"], r["cost_per_km"], r["revenue_per_km"], bool(r["is_unprofitable"]),
            r["trend"], r["data_quality_flag"], run_id,
        )
        for r in rows
    ]
    return execute_many(UPSERT_PROFITABILITY, payload)


def write_zone_summary(rows: list[dict[str, Any]], report_date: date, run_id: str) -> int:
    """UPSERT the zone x sim_hour summary."""
    payload = [
        (
            report_date, r["zone"], int(r["sim_hour"]), r["trips"], r["earnings_lkr"],
            r["active_events"], r["idle_events"], r["utilization"], r["avg_speed_kmh"], run_id,
        )
        for r in rows
        if r["sim_hour"] is not None
    ]
    return execute_many(UPSERT_ZONE_SUMMARY, payload)


# ============================================================================================
# Entry point
# ============================================================================================
def run_job(report_date: date, run_id: str) -> dict[str, Any]:
    """Execute the whole batch reconciliation for one simulated day.

    Returns a summary dict which the Airflow task pushes to XCom and writes to ``pipeline_runs``.
    """
    started = time.time()
    if not wait_for_postgres(timeout_s=120):
        raise RuntimeError("PostgreSQL unreachable")

    spark = build_spark(run_id)
    try:
        events = read_master_dataset(spark, report_date)
        event_count = events.count()
        LOG.info(
            "master dataset partition loaded",
            extra={
                "event": "master_loaded",
                "report_date": report_date.isoformat(),
                "rows": event_count,
                "run_id": run_id,
                "path": f"{CFG.lake_root}/telemetry/sim_date={report_date.isoformat()}",
            },
        )
        if event_count == 0:
            raise ValueError(f"master dataset partition for {report_date} is empty")

        metrics = vehicle_day_metrics(events)
        expenses = read_expenses(spark, report_date)
        expense_count = expenses.count()
        previous = read_previous_profitability(spark, report_date)

        scored = join_costs_and_score(metrics, expenses, previous)
        profitability_rows = [r.asDict() for r in scored.collect()]

        zone_rows = [r.asDict() for r in zone_hour_summary(events).collect()]

        written_profit = write_profitability(profitability_rows, report_date, run_id)
        written_zone = write_zone_summary(zone_rows, report_date, run_id)

        unprofitable = [r for r in profitability_rows if r["is_unprofitable"]]
        at_risk = [r for r in profitability_rows if r["trend"] in ("AT_RISK", "DECLINING")]
        missing_expense = [
            r for r in profitability_rows if r["data_quality_flag"] == "MISSING_EXPENSE"
        ]
        total_revenue = round(sum(r["revenue_lkr"] for r in profitability_rows), 2)
        total_cost = round(sum(r["total_cost_lkr"] for r in profitability_rows), 2)

        summary = {
            "report_date": report_date.isoformat(),
            "run_id": run_id,
            "events_read": event_count,
            "expense_rows": expense_count,
            "vehicles": len(profitability_rows),
            "rows_profitability": written_profit,
            "rows_zone_summary": written_zone,
            "total_revenue_lkr": total_revenue,
            "total_cost_lkr": total_cost,
            "total_profit_lkr": round(total_revenue - total_cost, 2),
            "unprofitable_vehicles": len(unprofitable),
            "at_risk_vehicles": len(at_risk),
            "missing_expense_rows": len(missing_expense),
            "duration_s": round(time.time() - started, 2),
        }
        LOG.info("profitability batch complete", extra={"event": "batch_job_complete", **summary})
        return summary
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily per-vehicle profitability reconciliation.")
    parser.add_argument("--date", required=True, help="simulated date to reconcile (YYYY-MM-DD)")
    parser.add_argument(
        "--run-id",
        default=None,
        help="run identifier propagated from Airflow so one daily run can be traced end to end",
    )
    parser.add_argument(
        "--summary-out",
        default=None,
        help="optional path to write the JSON summary (used by the Airflow task)",
    )
    args = parser.parse_args(argv)

    report_date = date.fromisoformat(args.date)
    run_id = args.run_id or RUN_ID

    try:
        summary = run_job(report_date, run_id)
    except Exception as exc:  # noqa: BLE001 - the exit code is the contract with Airflow
        LOG.error(
            "profitability batch failed",
            extra={
                "event": "batch_job_failed",
                "report_date": args.date,
                "run_id": run_id,
                "error": str(exc),
            },
            exc_info=True,
        )
        return 1

    payload = json.dumps(summary, indent=2)
    if args.summary_out:
        Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary_out).write_text(payload, encoding="utf-8")
    # Printed on stdout so `spark-submit` output carries the numbers into the Airflow task log.
    print(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())


UTC = UTC
_ = datetime  # re-exported for tests
