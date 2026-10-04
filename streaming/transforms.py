"""Pure DataFrame transformations for the speed layer.

Why these live in their own module
----------------------------------
Everything here is a plain function from DataFrame to DataFrame with no Kafka, no Postgres and
no ``SparkSession`` construction.  That makes each stage of the streaming pipeline unit-testable
against a hand-built micro dataset with exactly known expected numbers (TC-STR-001..TC-STR-012),
which is the only honest way to claim the windowed aggregates are correct.

It is also the concrete answer to Lambda's "two code paths" criticism: the batch job imports
:func:`enrich`, :func:`parse_and_validate`'s rule set and :func:`add_zone` from here and from
``common/``, so the two layers cannot drift apart in how they clean, bucket or classify an event.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common.config import CFG
from common.schemas import ACTIVE_STATUSES, reject_reason_column, telemetry_spark_schema
from common.zones import zone_for_point


# --------------------------------------------------------------------------------------------
# 1. Parse
# --------------------------------------------------------------------------------------------
def parse_kafka_json(raw: DataFrame) -> DataFrame:
    """Turn the raw Kafka source DataFrame into typed telemetry columns.

    The Kafka source hands over ``key``/``value`` as binary plus the message coordinates.  Those
    coordinates (``partition``, ``offset``) are carried through deliberately: when a row is
    quarantined, ``rejected_events`` stores them so an operator can go back to the exact message.

    Timestamps are cast *after* ``from_json`` rather than inside the schema, because a single
    unparseable timestamp inside ``from_json`` nulls the entire struct and would disguise the
    real rejection reason as "every field is null".
    """
    parsed = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_payload"),
        F.from_json(F.col("value").cast("string"), telemetry_spark_schema()).alias("data"),
    )
    return parsed.select(
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_payload",
        F.col("data.event_id").alias("event_id"),
        F.col("data.trip_id").alias("trip_id"),
        F.col("data.driver_id").alias("driver_id"),
        F.col("data.vehicle_id").alias("vehicle_id"),
        F.col("data.lat").alias("lat"),
        F.col("data.lon").alias("lon"),
        F.col("data.speed").alias("speed"),
        F.col("data.status").alias("status"),
        F.col("data.fare").alias("fare"),
        F.to_timestamp(F.col("data.event_time")).alias("event_time"),
        F.to_timestamp(F.col("data.sim_ts")).alias("sim_ts"),
        F.col("data.sim_date").alias("sim_date"),
        F.col("data.sim_hour").alias("sim_hour"),
    )


# --------------------------------------------------------------------------------------------
# 2. Validate / quarantine
# --------------------------------------------------------------------------------------------
def with_reject_reason(df: DataFrame) -> DataFrame:
    """Attach a ``reject_reason`` column (NULL = valid) using the shared rule set.

    A completely unparseable payload produces an all-NULL struct, which the rules would report as
    ``NULL_EVENT_ID``; that is relabelled ``MALFORMED_JSON`` so the data-quality report can tell
    "a field was wrong" apart from "this was not our message at all".
    """
    from common.schemas import REASON_MALFORMED_JSON

    unparseable = (
        F.col("event_id").isNull()
        & F.col("vehicle_id").isNull()
        & F.col("status").isNull()
        & F.col("event_time").isNull()
    )
    return df.withColumn(
        "reject_reason",
        F.when(unparseable, F.lit(REASON_MALFORMED_JSON)).otherwise(reject_reason_column()),
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid, rejected)``.

    Nothing is dropped: every input row appears in exactly one of the two outputs, which is what
    lets the report state the reject rate as a real fraction of ingested rows.
    """
    annotated = with_reject_reason(df)
    valid = annotated.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = annotated.where(F.col("reject_reason").isNotNull())
    return valid, rejected


# --------------------------------------------------------------------------------------------
# 3. Deduplicate
# --------------------------------------------------------------------------------------------
def deduplicate(df: DataFrame, watermark_minutes: int | None = None) -> DataFrame:
    """Drop repeated ``event_id`` values within the watermark.

    ``dropDuplicatesWithinWatermark`` (Spark 3.5+) is preferred over plain ``dropDuplicates``:
    the plain version keeps every seen key in state forever, so a long-running job's state store
    grows without bound.  The watermarked version expires keys once they are older than the
    watermark, bounding state at "one watermark's worth of event ids".

    The trade-off is explicit and stated in the report: a duplicate that arrives more than
    ``STREAM_WATERMARK_MINUTES`` after the original will *not* be caught by the speed layer.  The
    batch layer, which re-reads the whole day, removes it — one of the concrete reasons this
    project is Lambda and not Kappa.
    """
    minutes = CFG.stream_watermark_minutes if watermark_minutes is None else watermark_minutes
    watermarked = df.withWatermark("event_time", f"{minutes} minutes")

    # dropDuplicatesWithinWatermark is a STREAMING-only operator: applying it to a bounded
    # DataFrame raises an AnalysisException (DEFECT-009, found by TC-STR-004).  On a bounded
    # input the two are semantically equivalent anyway — there is no unbounded state to expire —
    # so the batch path uses plain dropDuplicates and the unit tests exercise the same rule.
    if df.isStreaming and hasattr(watermarked, "dropDuplicatesWithinWatermark"):
        return watermarked.dropDuplicatesWithinWatermark(["event_id"])
    return watermarked.dropDuplicates(["event_id"])


# --------------------------------------------------------------------------------------------
# 4. Enrich
# --------------------------------------------------------------------------------------------
def zone_udf():  # type: ignore[no-untyped-def]
    """Spark UDF wrapping :func:`common.zones.zone_for_point`.

    A UDF is used rather than re-expressing the grid in Spark SQL precisely *because* the batch
    job calls the same Python function: one definition, no possibility of divergence.  The cost
    (a Python round-trip per row) is acceptable at ~12 events/s and is measured in the NFR tests.
    """
    return F.udf(lambda lat, lon: zone_for_point(lat, lon), StringType())


def add_zone(df: DataFrame) -> DataFrame:
    """Add the ``zone`` column derived from lat/lon."""
    return df.withColumn("zone", zone_udf()(F.col("lat"), F.col("lon")))


def is_active_column() -> Column:
    """``True`` when the vehicle is earning or on its way to earn."""
    return F.col("status").isin(list(ACTIVE_STATUSES))


def enrich(df: DataFrame) -> DataFrame:
    """Add the derived columns every downstream sink needs.

    * ``zone``       — operating area (shared rule).
    * ``is_active``  — enroute or on_trip.
    * ``is_trip_end``— the single event per trip that carries revenue.
    """
    return (
        add_zone(df)
        .withColumn("is_active", is_active_column())
        .withColumn("is_trip_end", F.col("fare") > 0)
    )


# --------------------------------------------------------------------------------------------
# 5. Windowed aggregation (the speed layer's headline output)
# --------------------------------------------------------------------------------------------
def zone_window_metrics(
    df: DataFrame,
    window_minutes: int | None = None,
    watermark_minutes: int | None = None,
    apply_watermark: bool = True,
) -> DataFrame:
    """Tumbling-window utilisation and earnings per zone.

    Windows are defined on ``event_time`` (real wall clock), not ``sim_ts``.  That is deliberate:
    a watermark expressed in simulated time would mean "2 simulated minutes" = 0.83 real seconds,
    which is far too tight to absorb any real network delay.  The simulated clock is used for the
    batch layer's time-of-day analysis instead.

    Returned columns:
        ``window_start, window_end, zone, active_vehicles, idle_vehicles, total_vehicles,
        idle_ratio, trips_completed, earnings_lkr, avg_speed_kmh, event_count``

    ``active_vehicles`` uses ``approx_count_distinct``.  That is an explicit speed-for-accuracy
    trade: the exact count is recomputed by the batch layer, and the difference between the two
    is what the reconciliation test (TC-E2E-002) measures and the report explains.

    ``apply_watermark``:
        Spark 3.5 raises ``AnalysisException: Redefining watermark is disallowed`` when two
        stateful operators in one plan each call ``withWatermark`` (DEFECT-004, found in P3).
        In the streaming job the watermark is already established by :func:`deduplicate`
        upstream, so the job passes ``apply_watermark=False`` and the single watermark flows
        through both stateful operators.  The default stays ``True`` so unit tests can call this
        function on a bare DataFrame.
    """
    minutes = CFG.stream_window_minutes if window_minutes is None else window_minutes
    wm = CFG.stream_watermark_minutes if watermark_minutes is None else watermark_minutes
    source = df.withWatermark("event_time", f"{wm} minutes") if apply_watermark else df

    return (
        source.groupBy(F.window(F.col("event_time"), f"{minutes} minutes"), F.col("zone"))
        .agg(
            F.approx_count_distinct(
                F.when(F.col("is_active"), F.col("vehicle_id"))
            ).alias("active_vehicles"),
            F.approx_count_distinct(
                F.when(~F.col("is_active"), F.col("vehicle_id"))
            ).alias("idle_vehicles"),
            F.approx_count_distinct(F.col("vehicle_id")).alias("total_vehicles"),
            # Event counts, not vehicle counts. idle_ratio is derived from these because the
            # distinct-vehicle counts above legitimately overlap: a vehicle that goes
            # idle -> enroute inside one window is BOTH an idle vehicle and an active vehicle in
            # that window, so idle_vehicles/total_vehicles can exceed 1 (DEFECT-008).  The
            # event-count ratio is the time-weighted share of the window spent idle, which is
            # also exactly how the batch layer defines `utilization` — so the two layers now
            # measure the same quantity and the reconciliation is meaningful.
            F.sum(F.when(F.col("is_active"), F.lit(0)).otherwise(F.lit(1))).alias("idle_events"),
            F.sum(F.when(F.col("is_trip_end"), F.lit(1)).otherwise(F.lit(0))).alias(
                "trips_completed"
            ),
            F.sum(F.col("fare")).alias("earnings_lkr"),
            F.avg(F.when(F.col("is_active"), F.col("speed"))).alias("avg_speed_kmh"),
            F.count(F.lit(1)).alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("zone"),
            F.col("active_vehicles").cast("int"),
            F.col("idle_vehicles").cast("int"),
            F.col("total_vehicles").cast("int"),
            # Time-weighted idle share; greatest(..., 1) guards 0/0 in an empty window rather
            # than emitting NaN into Postgres.
            F.round(
                F.col("idle_events") / F.greatest(F.col("event_count"), F.lit(1)), 4
            ).alias("idle_ratio"),
            F.col("trips_completed").cast("int"),
            F.round(F.coalesce(F.col("earnings_lkr"), F.lit(0.0)), 2).alias("earnings_lkr"),
            F.round(F.coalesce(F.col("avg_speed_kmh"), F.lit(0.0)), 2).alias("avg_speed_kmh"),
            F.col("event_count").cast("int"),
        )
    )


# --------------------------------------------------------------------------------------------
# 6. Latest state per vehicle (feeds vehicle_status and the idle alerts)
# --------------------------------------------------------------------------------------------
def latest_per_vehicle(batch_df: DataFrame) -> DataFrame:
    """Reduce a micro-batch to one row per vehicle: the newest event by ``event_time``.

    Implemented with ``max(struct(...))`` rather than a window function.  A struct comparison in
    Spark is lexicographic over its fields, so putting ``event_time`` first makes
    ``max(struct(event_time, ...))`` exactly "the row with the newest event_time".  This is a
    single shuffle-free aggregation, whereas ``row_number() over (partition by ... order by ...)``
    would trigger a full sort per micro-batch.
    """
    return (
        batch_df.groupBy("vehicle_id")
        .agg(
            F.max(
                F.struct(
                    F.col("event_time"),
                    F.col("status"),
                    F.col("lat"),
                    F.col("lon"),
                    F.col("zone"),
                    F.col("speed"),
                    F.col("driver_id"),
                    F.col("trip_id"),
                )
            ).alias("latest")
        )
        .select(
            F.col("vehicle_id"),
            F.col("latest.event_time").alias("last_event_time"),
            F.col("latest.status").alias("status"),
            F.col("latest.lat").alias("lat"),
            F.col("latest.lon").alias("lon"),
            F.col("latest.zone").alias("zone"),
            F.col("latest.speed").alias("speed_kmh"),
            F.col("latest.driver_id").alias("driver_id"),
            F.col("latest.trip_id").alias("last_trip_id"),
        )
    )


def master_dataset_columns(df: DataFrame) -> DataFrame:
    """Project the columns persisted to the Parquet master dataset.

    Kafka coordinates and the raw payload are dropped: the lake stores *cleaned* events, and the
    raw bytes are still in Kafka (24 h retention) and in ``rejected_events`` for anything invalid.
    ``sim_date`` is kept last because it is the partition column.
    """
    return df.select(
        "event_id",
        "trip_id",
        "driver_id",
        "vehicle_id",
        "lat",
        "lon",
        "speed",
        "status",
        "fare",
        "event_time",
        "sim_ts",
        "sim_hour",
        "zone",
        "is_active",
        "is_trip_end",
        "sim_date",
    )


__all__ = [
    "add_zone",
    "deduplicate",
    "enrich",
    "is_active_column",
    "latest_per_vehicle",
    "master_dataset_columns",
    "parse_kafka_json",
    "split_valid_invalid",
    "with_reject_reason",
    "zone_udf",
    "zone_window_metrics",
]
