"""Unit tests for the shared validation rules (common/schemas.py).

Covers TC-ING-020 .. TC-ING-029 / REQ-12 (data quality handling).  Each deliberately malformed
event shape produced by the GPS producer has a test here asserting the *exact* rejection reason,
because those reason strings are aggregated in the daily report and drive the HighRejectRate
alert.
"""

from __future__ import annotations

import copy

import pytest

from common import schemas

pytestmark = pytest.mark.unit


def valid_event() -> dict:
    """A known-good telemetry event used as the base for every negative case."""
    return {
        "event_id": "8f14e45f-ea3a-4f1b-9f6d-000000000001",
        "trip_id": "T-0001",
        "driver_id": "D-007",
        "vehicle_id": "V-007",
        "lat": 6.9271,
        "lon": 79.8612,
        "speed": 32.5,
        "status": "on_trip",
        "fare": 0.0,
        "event_time": "2026-09-01T00:10:00.000Z",
        "sim_ts": "2026-09-01T10:00:00.000Z",
        "sim_date": "2026-09-01",
        "sim_hour": 10,
    }


def test_tc_ing_020_valid_event_passes() -> None:
    """TC-ING-020: a well-formed event is accepted."""
    assert schemas.reject_reason(valid_event()) is None
    assert schemas.is_valid(valid_event()) is True


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ({"event_id": None}, schemas.REASON_NULL_EVENT_ID),
        ({"event_id": "   "}, schemas.REASON_NULL_EVENT_ID),
        ({"vehicle_id": None}, schemas.REASON_NULL_VEHICLE_ID),
        ({"vehicle_id": ""}, schemas.REASON_NULL_VEHICLE_ID),
        ({"event_time": None}, schemas.REASON_NULL_EVENT_TIME),
        ({"status": "teleporting"}, schemas.REASON_UNKNOWN_STATUS),
        ({"status": None}, schemas.REASON_UNKNOWN_STATUS),
        ({"speed": -5.0}, schemas.REASON_NEGATIVE_SPEED),
        ({"speed": None}, schemas.REASON_NEGATIVE_SPEED),
        ({"speed": 999.0}, schemas.REASON_SPEED_TOO_HIGH),
        ({"lat": 95.0}, schemas.REASON_LAT_OUT_OF_RANGE),
        ({"lat": -91.0}, schemas.REASON_LAT_OUT_OF_RANGE),
        ({"lon": 200.0}, schemas.REASON_LON_OUT_OF_RANGE),
        ({"lon": None}, schemas.REASON_LON_OUT_OF_RANGE),
        ({"fare": -10.0}, schemas.REASON_NEGATIVE_FARE),
    ],
)
def test_tc_ing_021_each_bad_shape_gets_the_right_reason(mutation: dict, expected_reason: str) -> None:
    """TC-ING-021: every injected corruption is rejected with its specific reason."""
    event = valid_event()
    event.update(mutation)
    assert schemas.reject_reason(event) == expected_reason


def test_tc_ing_022_reason_order_is_deterministic() -> None:
    """TC-ING-022: an event breaking several rules reports the first rule in REASON_ORDER."""
    event = valid_event()
    event.update({"vehicle_id": None, "speed": -1.0, "status": "nope"})
    # vehicle_id is checked before status and speed.
    assert schemas.reject_reason(event) == schemas.REASON_NULL_VEHICLE_ID
    assert schemas.REASON_ORDER.index(schemas.REASON_NULL_VEHICLE_ID) < schemas.REASON_ORDER.index(
        schemas.REASON_UNKNOWN_STATUS
    )


def test_tc_ing_023_out_of_area_is_not_a_rejection() -> None:
    """TC-ING-023: a valid GPS point outside the licensed area is kept, tagged OUT_OF_AREA.

    Dropping it would understate fleet size; the business needs to see vehicles that left the
    service area.
    """
    event = valid_event()
    event.update({"lat": 7.5, "lon": 80.5})
    assert schemas.reject_reason(event) is None


def test_tc_ing_024_zero_speed_while_idle_is_valid() -> None:
    """TC-ING-024: speed 0 is legal (it is what idle looks like), only negatives are rejected."""
    event = valid_event()
    event.update({"status": "idle", "speed": 0.0, "trip_id": None})
    assert schemas.reject_reason(event) is None


def test_tc_ing_025_validation_does_not_mutate_its_input() -> None:
    """TC-ING-025: the validator is pure — safe to call inside a Spark task."""
    event = valid_event()
    snapshot = copy.deepcopy(event)
    schemas.reject_reason(event)
    assert event == snapshot


# --------------------------------------------------------------------------------------------
# Expense CSV validation
# --------------------------------------------------------------------------------------------
def valid_expense_row() -> dict:
    return {
        "vehicle_id": "V-007",
        "fuel_cost": "4200.50",
        "maintenance_cost": "800.00",
        "distance_covered": "142.3",
        "service_flag": "0",
        "report_date": "2026-09-01",
    }


def test_tc_ing_026_valid_expense_row_passes() -> None:
    """TC-ING-026: a clean expense row is accepted."""
    assert schemas.expense_reject_reason(valid_expense_row()) is None


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"vehicle_id": ""}, schemas.EXPENSE_REASON_MISSING_VEHICLE),
        ({"fuel_cost": "abc"}, schemas.EXPENSE_REASON_BAD_NUMBER),
        ({"fuel_cost": "-12"}, schemas.EXPENSE_REASON_NEGATIVE),
        ({"maintenance_cost": "-0.5"}, schemas.EXPENSE_REASON_NEGATIVE),
        ({"service_flag": "2"}, schemas.EXPENSE_REASON_BAD_NUMBER),
        ({"report_date": "01/09/2026"}, schemas.EXPENSE_REASON_BAD_DATE),
    ],
)
def test_tc_ing_027_bad_expense_rows_are_quarantined(mutation: dict, expected: str) -> None:
    """TC-ING-027: each dirty-row pattern the producer injects is caught with the right reason."""
    row = valid_expense_row()
    row.update(mutation)
    assert schemas.expense_reject_reason(row) == expected


def test_tc_ing_028_expense_date_must_match_the_file_day() -> None:
    """TC-ING-028: a file whose rows carry a different date is rejected (silent-corruption guard)."""
    row = valid_expense_row()
    assert schemas.expense_reject_reason(row, expected_date="2026-09-01") is None
    assert (
        schemas.expense_reject_reason(row, expected_date="2026-09-02")
        == schemas.EXPENSE_REASON_BAD_DATE
    )


def test_tc_ing_029_missing_column_is_detected() -> None:
    """TC-ING-029: a truncated row (missing column) is rejected rather than silently defaulted."""
    row = valid_expense_row()
    del row["distance_covered"]
    assert schemas.expense_reject_reason(row) == schemas.EXPENSE_REASON_MISSING_COLUMN
