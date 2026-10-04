"""Single source of truth for every runtime setting.

Why this module exists
----------------------
The brief requires that configuration lives in one place (``.env`` plus this module) so that no
host, port or threshold is hard-coded in business logic.  Every other module imports :data:`CFG`
and reads attributes from it; that makes the whole system reconfigurable for the demo (for example
``BAD_EVENT_RATE=0.2`` for the chaos test) without touching code, and it makes the configuration
table in the report trivial to generate.

The dataclass is frozen so a service cannot accidentally mutate configuration at runtime.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field, fields
from typing import Any, TypeVar

T = TypeVar("T")


def _get(name: str, default: str, cast: Callable[[str], T]) -> T:
    """Read ``name`` from the environment, falling back to ``default``.

    A bad value is a configuration error we want to see immediately at start-up rather than
    halfway through a streaming batch, so the cast is deliberately not wrapped in try/except.
    """
    raw = os.getenv(name)
    if raw is None or raw == "":
        raw = default
    return cast(raw)


def _as_bool(raw: str) -> bool:
    """Parse the usual truthy spellings people put in a .env file."""
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Config:
    """Immutable snapshot of the environment, built once per process."""

    # --- simulated clock ---------------------------------------------------
    sim_day_seconds: int = field(default_factory=lambda: _get("SIM_DAY_SECONDS", "600", int))
    sim_start_date: str = field(default_factory=lambda: _get("SIM_START_DATE", "2026-09-01", str))
    sim_epoch_file: str = field(
        default_factory=lambda: _get("SIM_EPOCH_FILE", "/shared/sim_epoch.txt", str)
    )

    # --- determinism -------------------------------------------------------
    seed: int = field(default_factory=lambda: _get("SEED", "42", int))

    # --- kafka -------------------------------------------------------------
    kafka_bootstrap: str = field(default_factory=lambda: _get("KAFKA_BOOTSTRAP", "kafka:9092", str))
    kafka_topic: str = field(default_factory=lambda: _get("KAFKA_TOPIC", "fleet.telemetry", str))
    kafka_dlq_topic: str = field(
        default_factory=lambda: _get("KAFKA_DLQ_TOPIC", "fleet.telemetry.dlq", str)
    )
    kafka_partitions: int = field(default_factory=lambda: _get("KAFKA_PARTITIONS", "6", int))
    kafka_replication: int = field(default_factory=lambda: _get("KAFKA_REPLICATION", "1", int))
    kafka_consumer_group: str = field(
        default_factory=lambda: _get("KAFKA_CONSUMER_GROUP", "fleet-stream", str)
    )

    # --- postgres ----------------------------------------------------------
    postgres_host: str = field(default_factory=lambda: _get("POSTGRES_HOST", "postgres", str))
    postgres_port: int = field(default_factory=lambda: _get("POSTGRES_PORT", "5432", int))
    postgres_db: str = field(default_factory=lambda: _get("POSTGRES_DB", "fleet", str))
    postgres_user: str = field(default_factory=lambda: _get("POSTGRES_USER", "fleet", str))
    postgres_password: str = field(default_factory=lambda: _get("POSTGRES_PASSWORD", "fleet", str))

    # --- fleet simulation --------------------------------------------------
    num_vehicles: int = field(default_factory=lambda: _get("NUM_VEHICLES", "25", int))
    emit_interval_sec: float = field(default_factory=lambda: _get("EMIT_INTERVAL_SEC", "2", float))
    emit_jitter_sec: float = field(default_factory=lambda: _get("EMIT_JITTER_SEC", "0.5", float))
    lazy_vehicles: int = field(default_factory=lambda: _get("LAZY_VEHICLES", "3", int))
    bad_event_rate: float = field(default_factory=lambda: _get("BAD_EVENT_RATE", "0.02", float))
    late_event_rate: float = field(default_factory=lambda: _get("LATE_EVENT_RATE", "0.01", float))
    duplicate_rate: float = field(default_factory=lambda: _get("DUPLICATE_RATE", "0.01", float))
    late_event_min_sec: int = field(default_factory=lambda: _get("LATE_EVENT_MIN_SEC", "30", int))
    late_event_max_sec: int = field(default_factory=lambda: _get("LATE_EVENT_MAX_SEC", "90", int))

    # --- fare model (LKR) --------------------------------------------------
    fare_base_lkr: float = field(default_factory=lambda: _get("FARE_BASE_LKR", "150", float))
    fare_per_km_lkr: float = field(default_factory=lambda: _get("FARE_PER_KM_LKR", "100", float))
    fare_noise_pct: float = field(default_factory=lambda: _get("FARE_NOISE_PCT", "0.08", float))

    # --- daily expense file (the batch source) -----------------------------
    # These are calibrated so that a normal vehicle clears a healthy margin while the designated
    # "high-cost" vehicles and the lazy (low-revenue) vehicles fall below zero.  The calibration
    # is documented in the report: without it, either every vehicle is profitable and the
    # business question has no answer, or none is and the answer is trivial.
    expense_fuel_per_km_lkr: float = field(
        default_factory=lambda: _get("EXPENSE_FUEL_PER_KM_LKR", "45", float)
    )
    expense_distance_min_km: float = field(
        default_factory=lambda: _get("EXPENSE_DISTANCE_MIN_KM", "2.5", float)
    )
    expense_distance_max_km: float = field(
        default_factory=lambda: _get("EXPENSE_DISTANCE_MAX_KM", "9.0", float)
    )
    expense_maint_normal_min: float = field(
        default_factory=lambda: _get("EXPENSE_MAINT_NORMAL_MIN", "80", float)
    )
    expense_maint_normal_max: float = field(
        default_factory=lambda: _get("EXPENSE_MAINT_NORMAL_MAX", "420", float)
    )
    expense_maint_high_min: float = field(
        default_factory=lambda: _get("EXPENSE_MAINT_HIGH_MIN", "1500", float)
    )
    expense_maint_high_max: float = field(
        default_factory=lambda: _get("EXPENSE_MAINT_HIGH_MAX", "4200", float)
    )
    expense_high_cost_pct: float = field(
        default_factory=lambda: _get("EXPENSE_HIGH_COST_PCT", "0.15", float)
    )
    expense_dirty_rows: int = field(default_factory=lambda: _get("EXPENSE_DIRTY_ROWS", "2", int))

    # --- zones -------------------------------------------------------------
    zone_lat_min: float = field(default_factory=lambda: _get("ZONE_LAT_MIN", "6.86", float))
    zone_lat_max: float = field(default_factory=lambda: _get("ZONE_LAT_MAX", "6.98", float))
    zone_lon_min: float = field(default_factory=lambda: _get("ZONE_LON_MIN", "79.84", float))
    zone_lon_max: float = field(default_factory=lambda: _get("ZONE_LON_MAX", "79.90", float))

    # --- streaming ---------------------------------------------------------
    spark_master: str = field(default_factory=lambda: _get("SPARK_MASTER", "local[*]", str))
    stream_starting_offsets: str = field(
        default_factory=lambda: _get("STREAM_STARTING_OFFSETS", "latest", str)
    )
    stream_window_minutes: int = field(
        default_factory=lambda: _get("STREAM_WINDOW_MINUTES", "1", int)
    )
    stream_watermark_minutes: int = field(
        default_factory=lambda: _get("STREAM_WATERMARK_MINUTES", "2", int)
    )
    stream_trigger_seconds: int = field(
        default_factory=lambda: _get("STREAM_TRIGGER_SECONDS", "30", int)
    )
    checkpoint_root: str = field(
        default_factory=lambda: _get("CHECKPOINT_ROOT", "/data/checkpoints", str)
    )
    lake_root: str = field(default_factory=lambda: _get("LAKE_ROOT", "/data/lake", str))
    landing_dir: str = field(default_factory=lambda: _get("LANDING_DIR", "/data/landing", str))
    processed_dir: str = field(default_factory=lambda: _get("PROCESSED_DIR", "/data/processed", str))
    reports_dir: str = field(default_factory=lambda: _get("REPORTS_DIR", "/data/reports", str))

    # --- thresholds --------------------------------------------------------
    idle_alert_minutes: float = field(default_factory=lambda: _get("IDLE_ALERT_MINUTES", "3", float))
    margin_threshold: float = field(default_factory=lambda: _get("MARGIN_THRESHOLD", "0.10", float))
    freshness_seconds: int = field(default_factory=lambda: _get("FRESHNESS_SECONDS", "120", int))
    max_bad_expense_row_pct: float = field(
        default_factory=lambda: _get("MAX_BAD_EXPENSE_ROW_PCT", "0.20", float)
    )

    # --- ports -------------------------------------------------------------
    api_port: int = field(default_factory=lambda: _get("API_PORT", "8000", int))
    producer_metrics_port: int = field(
        default_factory=lambda: _get("PRODUCER_METRICS_PORT", "8001", int)
    )
    expense_metrics_port: int = field(
        default_factory=lambda: _get("EXPENSE_METRICS_PORT", "8002", int)
    )
    stream_metrics_port: int = field(
        default_factory=lambda: _get("STREAM_METRICS_PORT", "8003", int)
    )

    # --- logging -----------------------------------------------------------
    log_level: str = field(default_factory=lambda: _get("LOG_LEVEL", "INFO", str))
    log_format: str = field(default_factory=lambda: _get("LOG_FORMAT", "json", str))

    # --- misc --------------------------------------------------------------
    skip_day: str = field(default_factory=lambda: _get("SKIP_DAY", "", str))

    # ----------------------------------------------------------------------
    @property
    def pg_dsn(self) -> str:
        """libpq connection string used by every psycopg client in the project."""
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )

    @property
    def pg_jdbc_url(self) -> str:
        """JDBC URL used by the Spark batch job when it reads ``daily_expenses``."""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def sim_seconds_per_real_second(self) -> float:
        """Simulated seconds elapsed per real second (86400/600 = 144 with the defaults)."""
        return 86400.0 / float(self.sim_day_seconds)

    def as_dict(self) -> dict[str, Any]:
        """Flat dict of settings, used by /health, the report appendix and evidence dumps."""
        out = {f.name: getattr(self, f.name) for f in fields(self)}
        out["postgres_password"] = "***"  # never leak the secret into logs or evidence
        return out


#: Process-wide configuration snapshot.  Import this; do not build another Config().
CFG = Config()

__all__ = ["CFG", "Config"]
