"""Unit tests for the configuration module (common/config.py).

Covers TC-OBS-010 .. TC-OBS-012 / REQ-10 (reproducibility, no secrets, no hard-coded values).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from common.config import CFG, Config

pytestmark = pytest.mark.unit


def test_tc_obs_010_env_overrides_are_read_and_cast(monkeypatch) -> None:
    """TC-OBS-010: an env override reaches the config with the right type."""
    monkeypatch.setenv("NUM_VEHICLES", "200")
    monkeypatch.setenv("BAD_EVENT_RATE", "0.2")
    cfg = Config()
    assert cfg.num_vehicles == 200 and isinstance(cfg.num_vehicles, int)
    assert cfg.bad_event_rate == pytest.approx(0.2)


def test_tc_obs_011_secrets_are_masked_in_dumps() -> None:
    """TC-OBS-011: as_dict() never leaks the database password into logs or evidence files."""
    dump = CFG.as_dict()
    assert dump["postgres_password"] == "***"
    assert CFG.postgres_password != "***", "the real value is still usable in code"


def test_tc_obs_012_derived_values_are_consistent() -> None:
    """TC-OBS-012: the derived DSN/JDBC/clock-ratio properties agree with the raw settings."""
    assert f"dbname={CFG.postgres_db}" in CFG.pg_dsn
    assert CFG.pg_jdbc_url.startswith("jdbc:postgresql://")
    assert CFG.sim_seconds_per_real_second == pytest.approx(86400.0 / CFG.sim_day_seconds)


def test_tc_obs_013_config_is_immutable() -> None:
    """TC-OBS-013: configuration cannot be mutated at runtime by a stray assignment.

    Asserting the *specific* exception matters: a bare ``Exception`` would also pass if the
    attribute did not exist at all, which would hide a renamed setting rather than prove
    immutability.
    """
    with pytest.raises(FrozenInstanceError):
        CFG.num_vehicles = 9999  # type: ignore[misc]
