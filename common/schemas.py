"""Event and file schemas plus the validation rules, shared by speed and batch layers.

Why one module
--------------
The same rules must hold in three places: the producer (which deliberately violates them to
create test data), the Spark Structured Streaming job (which quarantines violations into
``rejected_events`` / the Kafka DLQ) and the Airflow validation task (for the expense CSV).
Defining them once removes the possibility that the speed layer and the batch layer disagree
about what "valid" means — the core risk of a Lambda architecture.

Two dialects of the same rules
------------------------------
* :func:`reject_reason` — pure Python over a ``dict``.  Used by unit tests, the API and any
  non-Spark caller.  Easy to read and to defend in a viva.
* :func:`reject_reason_column` — the identical rule set expressed as a Spark ``Column``.  Used
  inside the streaming job so validation runs in the JVM (no Python UDF round-trip per row).

``TC-STR-002`` cross-checks the two implementations against the same fixtures, so they cannot
drift apart silently.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# --------------------------------------------------------------------------------------------
# Domain constants
# --------------------------------------------------------------------------------------------

#: The only statuses a well-formed telemetry event may carry.
VALID_STATUSES: tuple[str, ...] = ("idle", "enroute", "on_trip")

#: Statuses that count as the vehicle earning its keep.  ``idle_ratio`` is 1 - active_ratio.
ACTIVE_STATUSES: tuple[str, ...] = ("enroute", "on_trip")

#: Physically implausible above this; a GPS unit reporting 300 km/h in Colombo traffic is broken.
MAX_SPEED_KMH = 200.0

#: Rejection reasons.  These strings land in ``rejected_events.reason`` and are grouped in the
#: data-quality section of the daily report, so they are part of the public contract.
REASON_MALFORMED_JSON = "MALFORMED_JSON"
REASON_NULL_EVENT_ID = "NULL_EVENT_ID"
REASON_NULL_VEHICLE_ID = "NULL_VEHICLE_ID"
REASON_NULL_EVENT_TIME = "NULL_EVENT_TIME"
REASON_UNKNOWN_STATUS = "UNKNOWN_STATUS"
REASON_NEGATIVE_SPEED = "NEGATIVE_SPEED"
REASON_SPEED_TOO_HIGH = "SPEED_TOO_HIGH"
REASON_LAT_OUT_OF_RANGE = "LAT_OUT_OF_RANGE"
REASON_LON_OUT_OF_RANGE = "LON_OUT_OF_RANGE"
REASON_NEGATIVE_FARE = "NEGATIVE_FARE"

#: Evaluation order matters: the first matching reason is the one recorded, so the counts in the
#: report are unambiguous (one row, one reason).
REASON_ORDER: tuple[str, ...] = (
    REASON_NULL_EVENT_ID,
    REASON_NULL_VEHICLE_ID,
    REASON_NULL_EVENT_TIME,
    REASON_UNKNOWN_STATUS,
    REASON_NEGATIVE_SPEED,
    REASON_SPEED_TOO_HIGH,
    REASON_LAT_OUT_OF_RANGE,
    REASON_LON_OUT_OF_RANGE,
    REASON_NEGATIVE_FARE,
)

#: Field order of a telemetry event.  Used to build the Spark schema and to document the contract.
EVENT_FIELDS: tuple[tuple[str, str], ...] = (
    ("event_id", "string"),  # uuid4, dedup key
    ("trip_id", "string"),  # null while the vehicle is idle
    ("driver_id", "string"),
    ("vehicle_id", "string"),  # Kafka message key -> guarantees per-vehicle ordering
    ("lat", "double"),
    ("lon", "double"),
    ("speed", "double"),  # km/h
    ("status", "string"),  # idle | enroute | on_trip
    ("fare", "double"),  # LKR, non-zero only on the trip-end event
    ("event_time", "timestamp"),  # REAL wall-clock UTC -> streaming windows/watermarks
    ("sim_ts", "timestamp"),  # simulated clock
    ("sim_date", "string"),  # YYYY-MM-DD, Parquet partition column
    ("sim_hour", "int"),  # 0-23, time-of-day analysis
)

#: Columns of the daily expense CSV (the batch source).
EXPENSE_FIELDS: tuple[str, ...] = (
    "vehicle_id",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
    "report_date",
)


# --------------------------------------------------------------------------------------------
# Pure-Python validation
# --------------------------------------------------------------------------------------------
def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _as_float(value: Any) -> float | None:
    """Coerce to float, returning None when the value is missing or not numeric."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def reject_reason(event: Mapping[str, Any]) -> str | None:
    """Return the first failed rule for ``event``, or ``None`` when the event is valid.

    The rules, in evaluation order:

    1. ``event_id`` present — without it dedup is impossible.
    2. ``vehicle_id`` present — it is the Kafka key and the join key for profitability.
    3. ``event_time`` present — without it the row cannot be windowed or watermarked.
    4. ``status`` in :data:`VALID_STATUSES`.
    5. ``speed`` >= 0 and <= :data:`MAX_SPEED_KMH`.
    6. ``lat`` in [-90, 90] and ``lon`` in [-180, 180].
    7. ``fare`` >= 0 — a negative fare would silently reduce revenue.

    Missing numeric fields are treated as failures of their own rule (a null speed cannot be
    averaged), which keeps the downstream aggregations free of null-handling special cases.
    """
    if _is_blank(event.get("event_id")):
        return REASON_NULL_EVENT_ID
    if _is_blank(event.get("vehicle_id")):
        return REASON_NULL_VEHICLE_ID
    if _is_blank(event.get("event_time")):
        return REASON_NULL_EVENT_TIME
    if event.get("status") not in VALID_STATUSES:
        return REASON_UNKNOWN_STATUS

    speed = _as_float(event.get("speed"))
    if speed is None or speed < 0:
        return REASON_NEGATIVE_SPEED
    if speed > MAX_SPEED_KMH:
        return REASON_SPEED_TOO_HIGH

    lat = _as_float(event.get("lat"))
    if lat is None or not (-90.0 <= lat <= 90.0):
        return REASON_LAT_OUT_OF_RANGE
    lon = _as_float(event.get("lon"))
    if lon is None or not (-180.0 <= lon <= 180.0):
        return REASON_LON_OUT_OF_RANGE

    fare = _as_float(event.get("fare"))
    if fare is None or fare < 0:
        return REASON_NEGATIVE_FARE

    return None


def is_valid(event: Mapping[str, Any]) -> bool:
    """Convenience wrapper around :func:`reject_reason`."""
    return reject_reason(event) is None


# --------------------------------------------------------------------------------------------
# Spark dialect (imported lazily so non-Spark services do not need pyspark installed)
# --------------------------------------------------------------------------------------------
def telemetry_spark_schema():  # type: ignore[no-untyped-def]
    """Return the Spark ``StructType`` for a telemetry event.

    Timestamps are parsed as ``string`` first and cast explicitly afterwards.  Letting
    ``from_json`` coerce them directly means one unparseable timestamp nulls the *whole* row,
    which would hide the real rejection reason.
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    spark_types = {
        "string": StringType(),
        "double": DoubleType(),
        "int": IntegerType(),
        "timestamp": StringType(),  # cast later; see docstring
    }
    return StructType(
        [StructField(name, spark_types[kind], True) for name, kind in EVENT_FIELDS]
    )


def reject_reason_column():  # type: ignore[no-untyped-def]
    """Return a Spark ``Column`` holding the rejection reason, or NULL when the row is valid.

    This is :func:`reject_reason` transliterated into Spark SQL, evaluated entirely in the JVM.
    ``TC-STR-002`` asserts both implementations agree on the same fixtures.
    """
    from pyspark.sql import functions as F

    blank_event_id = F.col("event_id").isNull() | (F.trim(F.col("event_id")) == "")
    blank_vehicle_id = F.col("vehicle_id").isNull() | (F.trim(F.col("vehicle_id")) == "")
    blank_event_time = F.col("event_time").isNull()

    return (
        F.when(blank_event_id, F.lit(REASON_NULL_EVENT_ID))
        .when(blank_vehicle_id, F.lit(REASON_NULL_VEHICLE_ID))
        .when(blank_event_time, F.lit(REASON_NULL_EVENT_TIME))
        .when(
            ~F.col("status").isin(list(VALID_STATUSES)) | F.col("status").isNull(),
            F.lit(REASON_UNKNOWN_STATUS),
        )
        .when(F.col("speed").isNull() | (F.col("speed") < 0), F.lit(REASON_NEGATIVE_SPEED))
        .when(F.col("speed") > F.lit(MAX_SPEED_KMH), F.lit(REASON_SPEED_TOO_HIGH))
        .when(
            F.col("lat").isNull() | (F.col("lat") < -90) | (F.col("lat") > 90),
            F.lit(REASON_LAT_OUT_OF_RANGE),
        )
        .when(
            F.col("lon").isNull() | (F.col("lon") < -180) | (F.col("lon") > 180),
            F.lit(REASON_LON_OUT_OF_RANGE),
        )
        .when(F.col("fare").isNull() | (F.col("fare") < 0), F.lit(REASON_NEGATIVE_FARE))
        .otherwise(F.lit(None).cast("string"))
    )


# --------------------------------------------------------------------------------------------
# Expense-file validation (batch source)
# --------------------------------------------------------------------------------------------
EXPENSE_REASON_MISSING_VEHICLE = "MISSING_VEHICLE_ID"
EXPENSE_REASON_BAD_NUMBER = "NON_NUMERIC_VALUE"
EXPENSE_REASON_NEGATIVE = "NEGATIVE_VALUE"
EXPENSE_REASON_BAD_DATE = "BAD_REPORT_DATE"
EXPENSE_REASON_MISSING_COLUMN = "MISSING_COLUMN"


def expense_reject_reason(row: Mapping[str, Any], expected_date: str | None = None) -> str | None:
    """Validate one row of the daily expense CSV.

    Args:
        row: mapping of the CSV columns.
        expected_date: if given, ``report_date`` must equal it — this catches a file that was
            named for one day but filled with another day's data, which would silently corrupt
            the profitability join.

    Returns:
        A reason string, or ``None`` when the row is good.
    """
    missing = [c for c in EXPENSE_FIELDS if c not in row]
    if missing:
        return EXPENSE_REASON_MISSING_COLUMN
    if _is_blank(row.get("vehicle_id")):
        return EXPENSE_REASON_MISSING_VEHICLE

    for numeric in ("fuel_cost", "maintenance_cost", "distance_covered"):
        value = _as_float(row.get(numeric))
        if value is None:
            return EXPENSE_REASON_BAD_NUMBER
        if value < 0:
            return EXPENSE_REASON_NEGATIVE

    flag = _as_float(row.get("service_flag"))
    if flag is None or flag not in (0.0, 1.0):
        return EXPENSE_REASON_BAD_NUMBER

    report_date = str(row.get("report_date") or "").strip()
    if len(report_date) != 10 or report_date.count("-") != 2:
        return EXPENSE_REASON_BAD_DATE
    if expected_date is not None and report_date != expected_date:
        return EXPENSE_REASON_BAD_DATE

    return None


__all__ = [
    "ACTIVE_STATUSES",
    "EVENT_FIELDS",
    "EXPENSE_FIELDS",
    "MAX_SPEED_KMH",
    "REASON_ORDER",
    "VALID_STATUSES",
    "expense_reject_reason",
    "is_valid",
    "reject_reason",
    "reject_reason_column",
    "telemetry_spark_schema",
]
