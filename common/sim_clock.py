"""Simulated clock shared by every service.

Why a simulated clock?
----------------------
A daily batch layer is pointless in a demo if a "day" takes 24 hours.  We therefore compress one
simulated day into ``SIM_DAY_SECONDS`` real seconds (600 s = 10 real minutes by default), so a
marker can watch several full day-cycles, including the Airflow reconciliation run, inside one
sitting.

The dual-time design
--------------------
Every telemetry event carries **both** clocks, and this is a deliberate architectural decision
that is defended in the report:

* ``event_time`` — real wall-clock UTC.  Structured Streaming windows and watermarks use this, so
  the live dashboard advances at real-world speed and watermark semantics stay intuitive
  (a 2-minute watermark really means 2 minutes).
* ``sim_ts`` / ``sim_date`` / ``sim_hour`` — the simulated clock.  The Parquet master dataset is
  partitioned by ``sim_date`` and the time-of-day analysis groups by ``sim_hour``, so a whole
  "day" of business activity exists to reconcile after only 10 real minutes.

The epoch problem
-----------------
If each container computed its own start instant, the containers would disagree about which
simulated day it is, and the Airflow sensor would wait for a file the producer named differently.
The first service to start therefore writes the real UNIX timestamp of the simulation start into
``SIM_EPOCH_FILE`` on a shared volume; every later service reads it.  Writing is done with
``O_EXCL`` so two services starting at the same moment cannot both win.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from common.config import CFG

# Cached so the file is not stat-ed on every single event (the producer calls sim_now() ~12x/s).
_EPOCH_CACHE: float | None = None


def _parse_start_date(value: str) -> datetime:
    """Interpret ``SIM_START_DATE`` as midnight UTC on that calendar date."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)


def get_epoch(epoch_file: str | None = None) -> float:
    """Return the real UNIX timestamp at which the simulation started.

    The first caller across the whole stack creates the file atomically; every other caller reads
    it.  The value is cached in-process afterwards.
    """
    global _EPOCH_CACHE
    if _EPOCH_CACHE is not None and epoch_file is None:
        return _EPOCH_CACHE

    path = Path(epoch_file or CFG.sim_epoch_file)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # O_EXCL makes this a compare-and-set: exactly one process can create the file.
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, f"{time.time():.6f}\n".encode())
        finally:
            os.close(fd)
    except FileExistsError:
        pass

    value = float(path.read_text().strip())
    if epoch_file is None:
        _EPOCH_CACHE = value
    return value


def reset_epoch_cache() -> None:
    """Clear the in-process cache.  Used by unit tests that point at a temporary epoch file."""
    global _EPOCH_CACHE
    _EPOCH_CACHE = None


def sim_now(real_now: float | None = None, epoch: float | None = None) -> datetime:
    """Convert the current real time into simulated time.

    ``real_elapsed_seconds * (86400 / SIM_DAY_SECONDS)`` simulated seconds are added to
    ``SIM_START_DATE``.  Both arguments are injectable so the behaviour is unit-testable without
    sleeping.
    """
    real_now = time.time() if real_now is None else real_now
    epoch = get_epoch() if epoch is None else epoch
    elapsed_real = max(0.0, real_now - epoch)
    sim_elapsed = elapsed_real * CFG.sim_seconds_per_real_second
    return _parse_start_date(CFG.sim_start_date) + timedelta(seconds=sim_elapsed)


def sim_date(real_now: float | None = None, epoch: float | None = None) -> date:
    """Current simulated calendar date, used to partition the Parquet master dataset."""
    return sim_now(real_now, epoch).date()


def previous_sim_date(real_now: float | None = None, epoch: float | None = None) -> date:
    """The simulated day just finished — the day the Airflow DAG reconciles."""
    return sim_date(real_now, epoch) - timedelta(days=1)


def sim_hour(real_now: float | None = None, epoch: float | None = None) -> int:
    """Hour of the simulated day (0-23), used for the time-of-day earnings analysis."""
    return sim_now(real_now, epoch).hour


def seconds_until_next_sim_day(real_now: float | None = None, epoch: float | None = None) -> float:
    """Real seconds remaining until the simulated date rolls over.

    The expense producer sleeps on this so it writes exactly one CSV per simulated day, at the
    moment that day ends.
    """
    real_now = time.time() if real_now is None else real_now
    epoch = get_epoch() if epoch is None else epoch
    elapsed_real = max(0.0, real_now - epoch)
    return CFG.sim_day_seconds - (elapsed_real % CFG.sim_day_seconds)


def real_seconds_for_sim_date(target: date, epoch: float | None = None) -> tuple[float, float]:
    """Return the real [start, end) UNIX timestamps that map to simulated day ``target``.

    Used by tests and by evidence collection to answer "when in real time did simulated day
    2026-09-02 happen?".
    """
    epoch = get_epoch() if epoch is None else epoch
    day_index = (target - _parse_start_date(CFG.sim_start_date).date()).days
    start = epoch + day_index * CFG.sim_day_seconds
    return start, start + CFG.sim_day_seconds


__all__ = [
    "get_epoch",
    "previous_sim_date",
    "real_seconds_for_sim_date",
    "reset_epoch_cache",
    "seconds_until_next_sim_day",
    "sim_date",
    "sim_hour",
    "sim_now",
]
