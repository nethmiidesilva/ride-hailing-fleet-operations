"""Unit tests for the batch layer (batch/profitability_job.py).

Covers TC-BAT-001 .. TC-BAT-012 / REQ-03 (join of both sources), REQ-07 (profitability report),
REQ-11 (recomputation) and REQ-12 (data quality).

Every expected number below is calculated by hand in the docstring, so a marker can verify the
test itself rather than trusting the code that produced it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from batch import profitability_job as job
from common.config import CFG

pytestmark = [pytest.mark.unit, pytest.mark.spark]

T0 = datetime(2026, 9, 1, 8, 0, 0, tzinfo=UTC)


# --------------------------------------------------------------------------------------------
# Fixtures built directly as DataFrames (the join/score stage is pure and takes DataFrames)
# --------------------------------------------------------------------------------------------
def metrics_df(spark):
    """Per-vehicle telemetry metrics for three vehicles.

    ===== ======= ===== ============== ============ ===========
    veh   revenue trips active_minutes idle_minutes utilization
    ===== ======= ===== ============== ============ ===========
    V-001 1000.00     5          288.0        192.0        0.60
    V-002  500.00     2          144.0        336.0        0.30
    V-003  800.00     4          240.0        240.0        0.50
    ===== ======= ===== ============== ============ ===========
    """
    return spark.createDataFrame(
        [
            ("V-001", "D-001", 60, 40, 100, 1000.0, 5, 288.0, 192.0, 0.60),
            ("V-002", "D-002", 30, 70, 100, 500.0, 2, 144.0, 336.0, 0.30),
            ("V-003", "D-003", 50, 50, 100, 800.0, 4, 240.0, 240.0, 0.50),
        ],
        "vehicle_id string, driver_id string, active_events int, idle_events int, "
        "total_events int, revenue_lkr double, trips int, active_minutes double, "
        "idle_minutes double, utilization double",
    )


def expenses_df(spark):
    """Cost rows. V-003 is deliberately absent; V-004 has costs but no telemetry."""
    return spark.createDataFrame(
        [
            ("V-001", 200.0, 100.0, 10.0, 0),
            ("V-002", 300.0, 400.0, 20.0, 1),
            ("V-004", 100.0, 50.0, 5.0, 0),
        ],
        "vehicle_id string, fuel_cost double, maintenance_cost double, "
        "distance_covered double, service_flag int",
    )


def previous_df(spark, rows=None):
    """Margins for the two preceding days."""
    return spark.createDataFrame(
        rows if rows is not None else [],
        "vehicle_id string, margin_d1 double, margin_d2 double",
    )


def scored(spark, previous_rows=None):
    result = job.join_costs_and_score(
        metrics_df(spark), expenses_df(spark), previous_df(spark, previous_rows)
    )
    return {r["vehicle_id"]: r.asDict() for r in result.collect()}


# --------------------------------------------------------------------------------------------
# Core formulae
# --------------------------------------------------------------------------------------------
def test_tc_bat_001_profit_margin_and_per_km_match_hand_calculation(spark) -> None:
    """TC-BAT-001: V-001 hand calculation.

    revenue 1000.00, fuel 200.00, maintenance 100.00, distance 10 km
      total_cost   = 200 + 100            = 300.00
      profit       = 1000 - 300           = 700.00
      margin       = 700 / 1000           = 0.70
      cost_per_km  = 300 / 10             = 30.00
      revenue_per_km = 1000 / 10          = 100.00
      is_unprofitable = 700 < 0           = False
    """
    v = scored(spark)["V-001"]
    assert v["total_cost_lkr"] == pytest.approx(300.00)
    assert v["profit_lkr"] == pytest.approx(700.00)
    assert v["margin"] == pytest.approx(0.70)
    assert v["cost_per_km"] == pytest.approx(30.0)
    assert v["revenue_per_km"] == pytest.approx(100.0)
    assert v["is_unprofitable"] is False
    assert v["data_quality_flag"] == "OK"


def test_tc_bat_002_unprofitable_vehicle_is_detected(spark) -> None:
    """TC-BAT-002: V-002 hand calculation.

    revenue 500.00, fuel 300.00, maintenance 400.00, distance 20 km
      total_cost = 700.00 ; profit = -200.00 ; margin = -200/500 = -0.40
      cost_per_km = 700/20 = 35.00 ; revenue_per_km = 500/20 = 25.00
      is_unprofitable = True
    """
    v = scored(spark)["V-002"]
    assert v["total_cost_lkr"] == pytest.approx(700.00)
    assert v["profit_lkr"] == pytest.approx(-200.00)
    assert v["margin"] == pytest.approx(-0.40)
    assert v["cost_per_km"] == pytest.approx(35.0)
    assert v["revenue_per_km"] == pytest.approx(25.0)
    assert v["is_unprofitable"] is True


def test_tc_bat_003_missing_expense_row_is_flagged_not_dropped(spark) -> None:
    """TC-BAT-003: V-003 drove but has no cost row.

    It must survive the join (left from telemetry), keep its revenue of 800.00, carry zero cost,
    and be flagged MISSING_EXPENSE. Dropping it would understate fleet revenue by 800 LKR and
    hide a real data-quality problem.
    """
    v = scored(spark)["V-003"]
    assert v["revenue_lkr"] == pytest.approx(800.0)
    assert v["total_cost_lkr"] == pytest.approx(0.0)
    assert v["profit_lkr"] == pytest.approx(800.0)
    assert v["data_quality_flag"] == "MISSING_EXPENSE"
    assert v["cost_per_km"] is None, "distance is unknown, so cost_per_km must be NULL not 0"


def test_tc_bat_004_expense_row_without_telemetry_is_flagged(spark) -> None:
    """TC-BAT-004: V-004 has costs but never reported.

    revenue 0, cost 150 -> profit -150, is_unprofitable True, margin NULL (0 revenue),
    data_quality_flag NO_TELEMETRY.  A vehicle costing money while producing nothing is exactly
    what the business wants surfaced.
    """
    v = scored(spark)["V-004"]
    assert v["revenue_lkr"] == pytest.approx(0.0)
    assert v["total_cost_lkr"] == pytest.approx(150.0)
    assert v["profit_lkr"] == pytest.approx(-150.0)
    assert v["is_unprofitable"] is True
    assert v["margin"] is None
    assert v["data_quality_flag"] == "NO_TELEMETRY"


def test_tc_bat_005_no_vehicle_is_lost_by_the_join(spark) -> None:
    """TC-BAT-005: the full outer join yields the union of both sources — 4 vehicles."""
    out = scored(spark)
    assert set(out) == {"V-001", "V-002", "V-003", "V-004"}


def test_tc_bat_006_divide_by_zero_is_guarded(spark) -> None:
    """TC-BAT-006: zero revenue and zero distance give NULL, never NaN or an exception."""
    metrics = spark.createDataFrame(
        [("V-009", "D-009", 0, 100, 100, 0.0, 0, 0.0, 480.0, 0.0)],
        "vehicle_id string, driver_id string, active_events int, idle_events int, "
        "total_events int, revenue_lkr double, trips int, active_minutes double, "
        "idle_minutes double, utilization double",
    )
    expenses = spark.createDataFrame(
        [("V-009", 50.0, 25.0, 0.0, 0)],
        "vehicle_id string, fuel_cost double, maintenance_cost double, "
        "distance_covered double, service_flag int",
    )
    row = job.join_costs_and_score(metrics, expenses, previous_df(spark)).collect()[0]
    assert row["margin"] is None
    assert row["cost_per_km"] is None
    assert row["revenue_per_km"] is None
    assert row["profit_lkr"] == pytest.approx(-75.0)
    assert row["is_unprofitable"] is True


# --------------------------------------------------------------------------------------------
# Trend rules
# --------------------------------------------------------------------------------------------
def test_tc_bat_007_declining_trend_needs_two_consecutive_drops(spark) -> None:
    """TC-BAT-007: V-001 margin 0.70 today, 0.80 yesterday, 0.90 the day before -> DECLINING."""
    out = scored(spark, previous_rows=[("V-001", 0.80, 0.90)])
    assert out["V-001"]["trend"] == "DECLINING"


def test_tc_bat_008_a_single_drop_is_not_declining(spark) -> None:
    """TC-BAT-008: 0.70 today after 0.80 yesterday but 0.75 before that is noise, not a trend."""
    out = scored(spark, previous_rows=[("V-001", 0.80, 0.75)])
    assert out["V-001"]["trend"] == "STABLE"


def test_tc_bat_009_at_risk_needs_two_days_below_the_threshold(spark) -> None:
    """TC-BAT-009: V-002 margin -0.40 today and 0.05 yesterday, both < 0.10 -> AT_RISK."""
    out = scored(spark, previous_rows=[("V-002", 0.05, 0.50)])
    assert out["V-002"]["trend"] == "AT_RISK"
    assert CFG.margin_threshold == pytest.approx(0.10)


def test_tc_bat_010_at_risk_takes_precedence_over_declining(spark) -> None:
    """TC-BAT-010: a vehicle that is both falling AND below threshold reports AT_RISK.

    The documented precedence: an absolute loss-making level is a stronger signal than a
    downward slope that may still be comfortably profitable.
    """
    out = scored(spark, previous_rows=[("V-002", 0.05, 0.08)])
    assert out["V-002"]["trend"] == "AT_RISK"


def test_tc_bat_011_no_history_means_stable(spark) -> None:
    """TC-BAT-011: the first day a vehicle appears it cannot have a trend."""
    out = scored(spark)
    assert out["V-003"]["trend"] == "STABLE"
    assert out["V-001"]["trend"] == "STABLE"


# --------------------------------------------------------------------------------------------
# Recomputation from raw events
# --------------------------------------------------------------------------------------------
def raw_events(spark, rows):
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("event_id", StringType()),
            StructField("trip_id", StringType()),
            StructField("driver_id", StringType()),
            StructField("vehicle_id", StringType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("speed", DoubleType()),
            StructField("status", StringType()),
            StructField("fare", DoubleType()),
            StructField("event_time", TimestampType()),
            StructField("sim_ts", TimestampType()),
            StructField("sim_hour", IntegerType()),
            StructField("zone", StringType()),
            StructField("is_active", BooleanType()),
            StructField("is_trip_end", BooleanType()),
            StructField("sim_date", StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _ev(i, vehicle, status, fare=0.0, trip=None, hour=8, zone="Fort", speed=30.0):
    active = status in ("enroute", "on_trip")
    return (
        f"e{i}", trip, vehicle.replace("V-", "D-"), vehicle, 6.93, 79.87,
        speed if active else 0.0, status, fare,
        T0 + timedelta(seconds=i * 2), T0 + timedelta(seconds=i * 2), hour, zone,
        active, fare > 0, "2026-09-01",
    )


def test_tc_bat_012_revenue_is_deduplicated_by_trip_id(spark) -> None:
    """TC-BAT-012: a replayed trip-end event must not be counted twice.

    Input: trip T-1 ends with a 300.00 fare, and the same trip-end event is delivered twice with
    different event_ids (at-least-once delivery).  Expected revenue 300.00 and trips 1, not
    600.00 and 2.
    """
    rows = [
        _ev(1, "V-001", "on_trip"),
        _ev(2, "V-001", "idle", fare=300.0, trip="T-1"),
        _ev(3, "V-001", "idle", fare=300.0, trip="T-1"),  # duplicate delivery
        _ev(4, "V-001", "idle"),
    ]
    out = job.vehicle_day_metrics(raw_events(spark, rows)).collect()[0]
    assert out["revenue_lkr"] == pytest.approx(300.0)
    assert out["trips"] == 1


def test_tc_bat_013_utilisation_is_recomputed_from_event_counts(spark) -> None:
    """TC-BAT-013: 6 active events out of 10 -> utilization 0.6, active_minutes 6 x 4.8 = 28.8.

    One event accounts for EMIT_INTERVAL_SEC (2 s) of real time, and one real second is
    86400/600 = 144 simulated seconds, so one event = 2 x 144 / 60 = 4.8 simulated minutes.
    """
    rows = [_ev(i, "V-001", "on_trip") for i in range(6)] + [
        _ev(i + 10, "V-001", "idle") for i in range(4)
    ]
    out = job.vehicle_day_metrics(raw_events(spark, rows)).collect()[0]
    assert out["total_events"] == 10
    assert out["active_events"] == 6
    assert out["idle_events"] == 4
    assert out["utilization"] == pytest.approx(0.6)
    assert pytest.approx(4.8) == job.SIM_MINUTES_PER_EVENT
    assert out["active_minutes"] == pytest.approx(28.8)
    assert out["idle_minutes"] == pytest.approx(19.2)


def test_tc_bat_014_zone_hour_summary_groups_on_the_simulated_clock(spark) -> None:
    """TC-BAT-014: earnings are bucketed by (zone, sim_hour), not by wall-clock time.

    Fixture: Fort hour 8 earns 100 + 50 = 150 over 2 trips; Pettah hour 9 earns 200 over 1 trip.
    """
    rows = [
        _ev(1, "V-001", "idle", fare=100.0, trip="T-1", hour=8, zone="Fort"),
        _ev(2, "V-002", "idle", fare=50.0, trip="T-2", hour=8, zone="Fort"),
        _ev(3, "V-003", "on_trip", hour=8, zone="Fort"),
        _ev(4, "V-001", "idle", fare=200.0, trip="T-3", hour=9, zone="Pettah"),
    ]
    out = {
        (r["zone"], r["sim_hour"]): r.asDict()
        for r in job.zone_hour_summary(raw_events(spark, rows)).collect()
    }
    assert out[("Fort", 8)]["earnings_lkr"] == pytest.approx(150.0)
    assert out[("Fort", 8)]["trips"] == 2
    assert out[("Fort", 8)]["active_events"] == 1
    assert out[("Fort", 8)]["idle_events"] == 2
    assert out[("Fort", 8)]["utilization"] == pytest.approx(1 / 3, abs=1e-4)
    assert out[("Pettah", 9)]["earnings_lkr"] == pytest.approx(200.0)


def test_tc_bat_015_recomputation_is_deterministic(spark) -> None:
    """TC-BAT-015: running the same computation twice on the same input gives identical output.

    This is the property that makes a Lambda backfill safe: re-running a day cannot change the
    answer, so the idempotent UPSERT converges.
    """
    rows = [_ev(i, "V-001", "on_trip") for i in range(5)] + [
        _ev(20, "V-001", "idle", fare=250.0, trip="T-9")
    ]
    first = [r.asDict() for r in job.vehicle_day_metrics(raw_events(spark, rows)).collect()]
    second = [r.asDict() for r in job.vehicle_day_metrics(raw_events(spark, rows)).collect()]
    assert first == second
    assert first[0]["revenue_lkr"] == pytest.approx(250.0)
