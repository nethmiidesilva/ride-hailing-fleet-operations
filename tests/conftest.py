"""Shared pytest fixtures.

The SparkSession fixture is session-scoped because starting a JVM costs ~15 s; creating one per
test would make the suite unusable.  It is configured for a single local core with the adaptive
query engine and UI disabled, which is the fastest possible configuration for unit-sized data.
"""

from __future__ import annotations

import os
import socket
import sys
import time
from pathlib import Path

import pytest

# Make the project root importable regardless of where pytest was invoked from.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Absolute path of the repository root."""
    return ROOT


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession reused by every Spark unit test."""
    pyspark = pytest.importorskip("pyspark", reason="pyspark not installed in this interpreter")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("fleet-lambda-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    del pyspark


@pytest.fixture()
def tmp_epoch(tmp_path, monkeypatch):
    """Point the simulated clock at a throwaway epoch file with a known value.

    ``Config`` is frozen and is built once at import time, so setting the environment variable
    alone would not reach ``sim_clock.CFG``.  The fixture therefore swaps in a freshly built
    Config — the same thing a restarted container does.
    """
    from common import sim_clock
    from common.config import Config

    epoch_file = tmp_path / "sim_epoch.txt"
    fixed_epoch = 1_700_000_000.0
    epoch_file.write_text(f"{fixed_epoch}\n")
    monkeypatch.setenv("SIM_EPOCH_FILE", str(epoch_file))
    monkeypatch.setattr(sim_clock, "CFG", Config())
    sim_clock.reset_epoch_cache()
    yield fixed_epoch
    sim_clock.reset_epoch_cache()


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def stack_ready() -> dict[str, str]:
    """Skip integration tests unless the compose stack is reachable.

    Hosts default to the docker-compose service names, so the suite runs unchanged inside the
    ``tests`` container; override with env vars to run it from the host.
    """
    endpoints = {
        "postgres": (os.getenv("POSTGRES_HOST", "postgres"), int(os.getenv("POSTGRES_PORT", "5432"))),
        "kafka": tuple(os.getenv("KAFKA_BOOTSTRAP", "kafka:9092").split(":")),
        "api": (os.getenv("API_HOST", "api"), int(os.getenv("API_PORT", "8000"))),
        "prometheus": (os.getenv("PROM_HOST", "prometheus"), int(os.getenv("PROM_PORT", "9090"))),
    }
    missing = []
    for name, (host, port) in endpoints.items():
        if not _port_open(host, int(port)):
            missing.append(name)
    if missing:
        pytest.skip(f"compose stack not reachable: {', '.join(missing)}")
    return {name: f"{h}:{p}" for name, (h, p) in endpoints.items()}


def wait_until(predicate, timeout_s: float = 60.0, interval_s: float = 2.0):
    """Poll ``predicate`` until it returns a truthy value or the timeout expires.

    Returns the truthy value, or ``None`` on timeout.  Used throughout the integration suite
    instead of fixed sleeps, so the tests are as fast as the system allows.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval_s)
    return None
