"""Unit tests for the simulated clock (common/sim_clock.py).

Covers TC-ING-010 .. TC-ING-014 / REQ-10 (reproducibility of the simulated timeline).
Time is injected rather than slept on, so the whole file runs in milliseconds.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from common import sim_clock
from common.config import CFG

pytestmark = pytest.mark.unit

# 600 real seconds == 1 simulated day with the default configuration.
SIM_DAY = CFG.sim_day_seconds
EPOCH = 1_700_000_000.0
START = datetime.strptime(CFG.sim_start_date, "%Y-%m-%d").replace(tzinfo=UTC)


def test_tc_ing_010_epoch_zero_is_simulation_start() -> None:
    """TC-ING-010: at the epoch, simulated time equals SIM_START_DATE midnight UTC."""
    assert sim_clock.sim_now(real_now=EPOCH, epoch=EPOCH) == START


def test_tc_ing_011_one_sim_day_elapses_after_sim_day_seconds() -> None:
    """TC-ING-011: SIM_DAY_SECONDS of real time advance the simulated clock exactly one day."""
    later = sim_clock.sim_now(real_now=EPOCH + SIM_DAY, epoch=EPOCH)
    assert (later - START).total_seconds() == pytest.approx(86400.0, abs=1e-6)
    assert sim_clock.sim_date(real_now=EPOCH + SIM_DAY, epoch=EPOCH) == START.date().replace(
        day=START.day + 1
    )


@pytest.mark.parametrize(
    ("real_offset", "expected_hour"),
    [
        (0, 0),
        (SIM_DAY / 24, 1),  # 25 s -> 01:00 simulated
        (SIM_DAY / 2, 12),  # half a sim day -> noon
        (SIM_DAY * 23 / 24, 23),
        (SIM_DAY - 0.001, 23),  # just before rollover, still the last hour
    ],
)
def test_tc_ing_012_sim_hour_mapping(real_offset: float, expected_hour: int) -> None:
    """TC-ING-012: real elapsed seconds map onto the correct simulated hour-of-day."""
    assert sim_clock.sim_hour(real_now=EPOCH + real_offset, epoch=EPOCH) == expected_hour


def test_tc_ing_013_day_boundary_and_previous_day() -> None:
    """TC-ING-013: the date rolls over exactly at the boundary and previous_sim_date follows."""
    just_before = sim_clock.sim_date(real_now=EPOCH + SIM_DAY - 0.01, epoch=EPOCH)
    just_after = sim_clock.sim_date(real_now=EPOCH + SIM_DAY + 0.01, epoch=EPOCH)
    assert just_before == START.date()
    assert (just_after - just_before).days == 1

    prev = sim_clock.previous_sim_date(real_now=EPOCH + SIM_DAY + 0.01, epoch=EPOCH)
    assert prev == just_before, "the DAG must reconcile the day that has just finished"


def test_tc_ing_014_seconds_until_next_sim_day() -> None:
    """TC-ING-014: the countdown the expense producer sleeps on is correct and never negative."""
    assert sim_clock.seconds_until_next_sim_day(
        real_now=EPOCH, epoch=EPOCH
    ) == pytest.approx(SIM_DAY)
    assert sim_clock.seconds_until_next_sim_day(
        real_now=EPOCH + SIM_DAY * 0.25, epoch=EPOCH
    ) == pytest.approx(SIM_DAY * 0.75)
    # Immediately after a rollover a full day remains, not zero.
    assert sim_clock.seconds_until_next_sim_day(
        real_now=EPOCH + SIM_DAY, epoch=EPOCH
    ) == pytest.approx(SIM_DAY)


def test_tc_ing_015_real_window_for_a_sim_date() -> None:
    """TC-ING-015: a simulated date maps back to a real time window of one SIM_DAY_SECONDS."""
    target = START.date()
    start, end = sim_clock.real_seconds_for_sim_date(target, epoch=EPOCH)
    assert start == EPOCH
    assert end - start == SIM_DAY
    assert sim_clock.sim_date(real_now=start + 1, epoch=EPOCH) == target


def test_tc_ing_016_epoch_file_is_written_once(tmp_epoch: float, monkeypatch) -> None:
    """TC-ING-016: the shared epoch file pins the clock so all services agree."""
    # tmp_epoch pre-wrote the file; a second caller must read the same value, not overwrite it.
    first = sim_clock.get_epoch()
    sim_clock.reset_epoch_cache()
    second = sim_clock.get_epoch()
    assert first == second == tmp_epoch


def test_tc_ing_017_epoch_created_when_absent(tmp_path, monkeypatch) -> None:
    """TC-ING-017: the first service to start creates the epoch file atomically."""
    target = tmp_path / "nested" / "sim_epoch.txt"
    monkeypatch.setenv("SIM_EPOCH_FILE", str(target))
    sim_clock.reset_epoch_cache()
    value = sim_clock.get_epoch(str(target))
    assert target.exists()
    assert isinstance(value, float) and value > 1_600_000_000
    sim_clock.reset_epoch_cache()


def test_tc_ing_018_clock_is_monotonic_in_real_time() -> None:
    """TC-ING-018: simulated time never goes backwards for increasing real time."""
    samples = [sim_clock.sim_now(real_now=EPOCH + t, epoch=EPOCH) for t in range(0, 1200, 37)]
    assert samples == sorted(samples)
    assert isinstance(samples[0].date(), date)
