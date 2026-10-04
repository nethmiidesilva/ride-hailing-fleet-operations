"""Unit tests for the speed-layer transforms (streaming/transforms.py).

Covers TC-STR-001 .. TC-STR-014 / REQ-04 (windowed aggregation) and REQ-12 (data quality).

These run against a **local SparkSession** on hand-built micro datasets with exactly
hand-calculated expected values.  That is the only way to claim the windowed aggregates are
correct: asserting "some rows appeared" against live data proves nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from common import schemas
from streaming import transforms as T

pytestmark = [pytest.mark.unit, pytest.mark.spark]

T0 = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)


def event_row(
    event_id: str,
    vehicle_id: str = "V-001",
    status: str = "on_trip",
    fare: float = 0.0,
    speed: float = 30.0,
    lat: float = 6.9271,
    lon: float = 79.8612,
    offset_s: int = 0,
    trip_id: str | None = "T-1",
) -> dict:
    """One parsed telemetry row as the transforms expect it (post parse_kafka_json)."""
    return {
        "kafka_key": vehicle_id,
        "kafka_partition": 0,
        "kafka_offset": 0,
        "raw_payload": "{}",
        "event_id": event_id,
        "trip_id": trip_id,
        "driver_id": vehicle_id.replace("V-", "D-"),
        "vehicle_id": vehicle_id,
        "lat": lat,
        "lon": lon,
        "speed": speed,
        "status": status,
        "fare": fare,
        "event_time": T0 + timedelta(seconds=offset_s),
        "sim_ts": T0 + timedelta(seconds=offset_s),
        "sim_date": "2026-09-01",
        "sim_hour": 12,
    }


def make_df(spark, rows: list[dict]):
    """Build a DataFrame with the exact column order the transforms expect."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("kafka_key", StringType()),
            StructField("kafka_partition", IntegerType()),
            StructField("kafka_offset", LongType()),
            StructField("raw_payload", StringType()),
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
            StructField("sim_date", StringType()),
            StructField("sim_hour", IntegerType()),
        ]
    )
    ordered = [tuple(r[f.name] for f in schema.fields) for r in rows]
    return spark.createDataFrame(ordered, schema)


# --------------------------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------------------------
def test_tc_str_001_valid_rows_pass_and_invalid_rows_are_split(spark) -> None:
    """TC-STR-001: split_valid_invalid partitions the input with nothing lost."""
    rows = [
        event_row("e1"),
        event_row("e2", speed=-5.0),
        event_row("e3", status="flying"),
        event_row("e4", lat=95.0),
        event_row("e5", fare=-1.0),
    ]
    valid, rejected = T.split_valid_invalid(make_df(spark, rows))
    assert valid.count() == 1
    assert rejected.count() == 4
    assert valid.collect()[0]["event_id"] == "e1"
    # Every input row is accounted for exactly once.
    assert valid.count() + rejected.count() == len(rows)


def test_tc_str_002_spark_and_python_validators_agree(spark) -> None:
    """TC-STR-002: the Spark Column rules and the pure-Python rules give the same reason.

    This is the guard against the speed layer and the batch layer drifting apart, which is the
    central risk of a Lambda architecture.
    """
    cases = [
        event_row("a"),
        event_row("b", speed=-1.0),
        event_row("c", speed=999.0),
        event_row("d", status="teleporting"),
        event_row("e", lat=95.0),
        event_row("f", lon=-250.0),
        event_row("g", fare=-10.0),
        event_row("h", vehicle_id=""),
    ]
    annotated = T.with_reject_reason(make_df(spark, cases)).collect()
    by_id = {r["event_id"]: r["reject_reason"] for r in annotated}
    for case in cases:
        # The Python validator works on the raw dict; timestamps are already objects here.
        expected = schemas.reject_reason(case)
        assert by_id[case["event_id"]] == expected, (
            f"disagreement on {case['event_id']}: spark={by_id[case['event_id']]!r} "
            f"python={expected!r}"
        )


def test_tc_str_003_completely_unparseable_row_is_malformed_json(spark) -> None:
    """TC-STR-003: an all-null struct is labelled MALFORMED_JSON, not NULL_EVENT_ID."""
    row = event_row("x")
    row.update(
        {"event_id": None, "vehicle_id": None, "status": None, "event_time": None,
         "raw_payload": "not json at all"}
    )
    annotated = T.with_reject_reason(make_df(spark, [row])).collect()[0]
    assert annotated["reject_reason"] == schemas.REASON_MALFORMED_JSON


# --------------------------------------------------------------------------------------------
# Dedup
# --------------------------------------------------------------------------------------------
def test_tc_str_004_duplicate_event_ids_are_removed(spark) -> None:
    """TC-STR-004: a repeated event_id survives exactly once."""
    rows = [event_row("dup"), event_row("dup"), event_row("dup"), event_row("unique")]
    out = T.deduplicate(make_df(spark, rows)).collect()
    ids = sorted(r["event_id"] for r in out)
    assert ids == ["dup", "unique"]


def test_tc_str_005_dedup_keeps_distinct_events_from_the_same_vehicle(spark) -> None:
    """TC-STR-005: dedup is on event_id, not vehicle_id — a vehicle keeps all its events."""
    rows = [event_row(f"e{i}", offset_s=i * 2) for i in range(10)]
    assert T.deduplicate(make_df(spark, rows)).count() == 10


# --------------------------------------------------------------------------------------------
# Enrichment
# --------------------------------------------------------------------------------------------
def test_tc_str_006_enrich_adds_zone_is_active_and_is_trip_end(spark) -> None:
    """TC-STR-006: the derived columns match the shared zone function and status rules."""
    from common.zones import zone_for_point

    rows = [
        event_row("a", status="idle", speed=0.0, fare=0.0),
        event_row("b", status="enroute"),
        event_row("c", status="on_trip"),
        event_row("d", status="idle", speed=0.0, fare=420.5),  # trip-end event
        event_row("e", lat=0.0, lon=0.0),  # outside the service area
    ]
    out = {r["event_id"]: r for r in T.enrich(make_df(spark, rows)).collect()}

    assert out["a"]["is_active"] is False
    assert out["b"]["is_active"] is True
    assert out["c"]["is_active"] is True
    assert out["d"]["is_trip_end"] is True
    assert out["a"]["is_trip_end"] is False
    assert out["e"]["zone"] == "OUT_OF_AREA"
    assert out["a"]["zone"] == zone_for_point(6.9271, 79.8612)


# --------------------------------------------------------------------------------------------
# Windowed aggregation — hand-calculated expected values
# --------------------------------------------------------------------------------------------
def test_tc_str_007_window_aggregates_match_hand_calculation(spark) -> None:
    """TC-STR-007: exact expected numbers for a hand-built one-minute window.

    Fixture (all inside window 12:00:00-12:01:00, all in one zone):
      V-001 on_trip  speed 40   fare 0
      V-002 on_trip  speed 20   fare 0
      V-003 idle     speed 0    fare 0
      V-004 idle     speed 0    fare 300.00   <- trip end
      V-005 enroute  speed 60   fare 0

    Expected: total 5, active 3 (V-001,002,005), idle 2, idle_ratio 0.4,
              trips_completed 1, earnings 300.00, avg_speed over ACTIVE rows = (40+20+60)/3 = 40.
    """
    rows = [
        event_row("a", "V-001", "on_trip", 0.0, 40.0, offset_s=1),
        event_row("b", "V-002", "on_trip", 0.0, 20.0, offset_s=2),
        event_row("c", "V-003", "idle", 0.0, 0.0, offset_s=3),
        event_row("d", "V-004", "idle", 300.0, 0.0, offset_s=4),
        event_row("e", "V-005", "enroute", 0.0, 60.0, offset_s=5),
    ]
    enriched = T.enrich(make_df(spark, rows))
    result = T.zone_window_metrics(enriched, window_minutes=1).collect()

    assert len(result) == 1, "all five events fall in one window and one zone"
    w = result[0]
    assert w["window_start"].replace(tzinfo=UTC) == T0
    assert w["window_end"].replace(tzinfo=UTC) == T0 + timedelta(minutes=1)
    assert w["total_vehicles"] == 5
    assert w["active_vehicles"] == 3
    assert w["idle_vehicles"] == 2
    assert w["idle_ratio"] == pytest.approx(0.4)
    assert w["trips_completed"] == 1
    assert w["earnings_lkr"] == pytest.approx(300.0)
    assert w["avg_speed_kmh"] == pytest.approx(40.0)
    assert w["event_count"] == 5


def test_tc_str_008_events_are_placed_in_the_correct_window(spark) -> None:
    """TC-STR-008: a 1-minute tumbling window is [start, start+60) — the boundary is exclusive."""
    rows = [
        event_row("in", "V-001", offset_s=59, fare=100.0, status="idle", speed=0.0),
        event_row("next", "V-001", offset_s=60, fare=200.0, status="idle", speed=0.0),
    ]
    out = sorted(
        T.zone_window_metrics(T.enrich(make_df(spark, rows)), window_minutes=1).collect(),
        key=lambda r: r["window_start"],
    )
    assert len(out) == 2
    assert out[0]["earnings_lkr"] == pytest.approx(100.0)
    assert out[1]["earnings_lkr"] == pytest.approx(200.0)
    assert (out[1]["window_start"] - out[0]["window_start"]).total_seconds() == 60


def test_tc_str_009_windows_are_grouped_per_zone(spark) -> None:
    """TC-STR-009: two zones in the same minute produce two rows, not one."""
    from common.zones import zone_centre

    fort_lat, fort_lon = zone_centre("Fort")
    pettah_lat, pettah_lon = zone_centre("Pettah")
    rows = [
        event_row("a", "V-001", lat=fort_lat, lon=fort_lon, fare=100.0, status="idle", speed=0.0),
        event_row("b", "V-002", lat=pettah_lat, lon=pettah_lon, fare=50.0, status="idle", speed=0.0),
    ]
    out = {r["zone"]: r for r in T.zone_window_metrics(T.enrich(make_df(spark, rows)), 1).collect()}
    assert set(out) == {"Fort", "Pettah"}
    assert out["Fort"]["earnings_lkr"] == pytest.approx(100.0)
    assert out["Pettah"]["earnings_lkr"] == pytest.approx(50.0)


def test_tc_str_010_empty_window_does_not_divide_by_zero(spark) -> None:
    """TC-STR-010: a window with only idle vehicles still produces a finite idle_ratio."""
    rows = [event_row("a", "V-001", "idle", 0.0, 0.0)]
    w = T.zone_window_metrics(T.enrich(make_df(spark, rows)), 1).collect()[0]
    assert w["idle_ratio"] == pytest.approx(1.0)
    assert w["avg_speed_kmh"] == pytest.approx(0.0), "no active rows -> 0, not NaN or NULL"


# --------------------------------------------------------------------------------------------
# Latest state per vehicle
# --------------------------------------------------------------------------------------------
def test_tc_str_011_latest_per_vehicle_picks_the_newest_event(spark) -> None:
    """TC-STR-011: the newest event by event_time wins, per vehicle."""
    rows = [
        event_row("old", "V-001", "idle", 0.0, 0.0, offset_s=0),
        event_row("new", "V-001", "on_trip", 0.0, 45.0, offset_s=30),
        event_row("mid", "V-001", "enroute", 0.0, 20.0, offset_s=15),
        event_row("other", "V-002", "idle", 0.0, 0.0, offset_s=5),
    ]
    out = {r["vehicle_id"]: r for r in T.latest_per_vehicle(T.enrich(make_df(spark, rows))).collect()}
    assert len(out) == 2
    assert out["V-001"]["status"] == "on_trip"
    assert out["V-001"]["speed_kmh"] == pytest.approx(45.0)
    assert out["V-001"]["last_event_time"].replace(tzinfo=UTC) == T0 + timedelta(seconds=30)
    assert out["V-002"]["status"] == "idle"


def test_tc_str_012_latest_per_vehicle_carries_zone_and_driver(spark) -> None:
    """TC-STR-012: the row written to vehicle_status has every column the UPSERT needs."""
    rows = [event_row("a", "V-007")]
    row = T.latest_per_vehicle(T.enrich(make_df(spark, rows))).collect()[0]
    for column in (
        "vehicle_id", "last_event_time", "status", "lat", "lon", "zone", "speed_kmh",
        "driver_id", "last_trip_id",
    ):
        assert column in row.asDict(), f"missing {column}"
    assert row["driver_id"] == "D-007"
    assert row["zone"] != "OUT_OF_AREA"


# --------------------------------------------------------------------------------------------
# Master dataset projection
# --------------------------------------------------------------------------------------------
def test_tc_str_013_master_dataset_drops_kafka_internals_and_keeps_sim_date_last(spark) -> None:
    """TC-STR-013: the lake stores cleaned events; sim_date stays last as the partition column."""
    out = T.master_dataset_columns(T.enrich(make_df(spark, [event_row("a")])))
    columns = out.columns
    assert "raw_payload" not in columns and "kafka_offset" not in columns
    assert columns[-1] == "sim_date"
    for required in ("event_id", "vehicle_id", "fare", "event_time", "sim_hour", "zone",
                     "is_active", "is_trip_end"):
        assert required in columns


def test_tc_str_014_zone_udf_matches_the_shared_python_function(spark) -> None:
    """TC-STR-014: the Spark UDF and the pure function give identical answers.

    Both the streaming job and the batch job depend on this equivalence; if it broke, the two
    layers would bucket the same trip into different zones and the reconciliation would never
    balance.
    """
    from common.zones import ZONE_NAMES, zone_centre, zone_for_point

    points = [zone_centre(z) for z in ZONE_NAMES] + [(0.0, 0.0), (6.86, 79.84), (6.98, 79.90)]
    rows = [event_row(f"p{i}", lat=lat, lon=lon) for i, (lat, lon) in enumerate(points)]
    out = T.add_zone(make_df(spark, rows)).collect()
    for i, (lat, lon) in enumerate(points):
        spark_zone = next(r["zone"] for r in out if r["event_id"] == f"p{i}")
        assert spark_zone == zone_for_point(lat, lon)
