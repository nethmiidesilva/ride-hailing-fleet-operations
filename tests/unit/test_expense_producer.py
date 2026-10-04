"""Unit tests for the daily-batch source (producers/expense_producer.py).

Covers TC-ING-045 .. TC-ING-052 / REQ-02 (daily batch source) and REQ-12 (data quality).
"""

from __future__ import annotations

import csv
from datetime import date

import pytest

from common.config import CFG
from common.schemas import EXPENSE_FIELDS, expense_reject_reason
from producers import expense_producer as ep
from producers.fleet_simulator import FleetSimulator

pytestmark = pytest.mark.unit

DAY = date(2026, 9, 1)


def read_csv(path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_tc_ing_045_file_is_written_atomically_with_the_agreed_name(tmp_path) -> None:
    """TC-ING-045: the landing file has the exact name the FileSensor waits for, and no .tmp remains."""
    path = ep.write_expense_file(DAY, landing_dir=str(tmp_path))
    assert path is not None
    assert path.name == "expenses_2026-09-01.csv"
    assert path.exists()
    # The temp file must have been renamed away, never left behind for the sensor to trip over.
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith(".")]
    assert leftovers == [], f"temp files left in the landing directory: {leftovers}"


def test_tc_ing_046_columns_match_the_contract(tmp_path) -> None:
    """TC-ING-046: the CSV header is exactly the agreed schema, in order."""
    path = ep.write_expense_file(DAY, landing_dir=str(tmp_path))
    with open(path, newline="", encoding="utf-8") as handle:
        header = next(csv.reader(handle))
    assert header == list(EXPENSE_FIELDS)


def test_tc_ing_047_one_row_per_vehicle_plus_dirty_rows(tmp_path) -> None:
    """TC-ING-047: every simulated vehicle gets a cost row, plus the injected dirty rows."""
    rows = read_csv(ep.write_expense_file(DAY, landing_dir=str(tmp_path)))
    clean_ids = {r["vehicle_id"] for r in rows if expense_reject_reason(r) is None}
    assert clean_ids == set(ep.vehicle_ids())
    assert len(rows) == CFG.num_vehicles + CFG.expense_dirty_rows


def test_tc_ing_048_vehicle_ids_match_the_gps_producer() -> None:
    """TC-ING-048: both sources agree on vehicle ids, or the profitability join silently misses."""
    simulated = {v.vehicle_id for v in FleetSimulator(num_vehicles=CFG.num_vehicles, seed=1)}
    assert simulated == set(ep.vehicle_ids())


def test_tc_ing_049_dirty_rows_are_caught_by_the_shared_validator(tmp_path) -> None:
    """TC-ING-049: the injected defects are exactly the ones Airflow's validator rejects."""
    rows = read_csv(ep.write_expense_file(DAY, landing_dir=str(tmp_path)))
    reasons = {expense_reject_reason(r) for r in rows} - {None}
    assert len(reasons) == CFG.expense_dirty_rows
    bad_count = sum(1 for r in rows if expense_reject_reason(r) is not None)
    assert bad_count == CFG.expense_dirty_rows
    # The bad-row share must stay under the DAG's fail threshold, or every run would fail.
    assert bad_count / len(rows) < CFG.max_bad_expense_row_pct


def test_tc_ing_050_high_cost_vehicles_exist_and_carry_the_service_flag(tmp_path) -> None:
    """TC-ING-050: ~15% of vehicles are expensive, which is what creates unprofitable vehicles."""
    rows = [r for r in read_csv(ep.write_expense_file(DAY, landing_dir=str(tmp_path)))
            if expense_reject_reason(r) is None]
    flagged = [r for r in rows if r["service_flag"] == "1"]
    expected = max(1, round(CFG.num_vehicles * CFG.expense_high_cost_pct))
    assert len(flagged) == expected
    for row in flagged:
        assert float(row["maintenance_cost"]) >= CFG.expense_maint_high_min
    for row in rows:
        if row["service_flag"] == "0":
            assert float(row["maintenance_cost"]) <= CFG.expense_maint_normal_max


def test_tc_ing_051_generation_is_deterministic_per_day() -> None:
    """TC-ING-051: the same day regenerates identical values (needed for the replay test)."""
    assert ep.build_rows(DAY) == ep.build_rows(DAY)
    assert ep.build_rows(DAY) != ep.build_rows(date(2026, 9, 2)), "different days must differ"


def test_tc_ing_052_skip_day_suppresses_the_file(tmp_path, monkeypatch) -> None:
    """TC-ING-052: SKIP_DAY reproduces the missing-file failure without code changes.

    Config is a frozen dataclass, so the test rebuilds one from a patched environment rather than
    mutating the singleton — which is exactly how the chaos scenario does it (a new container
    with a different env var).
    """
    from common.config import Config

    monkeypatch.setenv("SKIP_DAY", DAY.isoformat())
    monkeypatch.setattr(ep, "CFG", Config())
    assert ep.write_expense_file(DAY, landing_dir=str(tmp_path)) is None
    assert list(tmp_path.iterdir()) == []


def test_tc_ing_053_backfill_writes_one_file_per_day(tmp_path) -> None:
    """TC-ING-053: --backfill produces a contiguous range of files for replay demos."""
    written = ep.backfill(date(2026, 9, 1), date(2026, 9, 3), landing_dir=str(tmp_path))
    assert [p.name for p in written] == [
        "expenses_2026-09-01.csv",
        "expenses_2026-09-02.csv",
        "expenses_2026-09-03.csv",
    ]


def test_tc_ing_054_values_are_realistic_lkr(tmp_path) -> None:
    """TC-ING-054: costs sit in a plausible LKR range, so profitability numbers mean something."""
    rows = [r for r in read_csv(ep.write_expense_file(DAY, landing_dir=str(tmp_path)))
            if expense_reject_reason(r) is None]
    for row in rows:
        distance = float(row["distance_covered"])
        fuel = float(row["fuel_cost"])
        assert CFG.expense_distance_min_km <= distance <= CFG.expense_distance_max_km
        # Fuel tracks distance within the +/-15% noise band applied by the generator.
        implied_rate = fuel / distance
        assert 0.85 * CFG.expense_fuel_per_km_lkr <= implied_rate <= 1.2 * CFG.expense_fuel_per_km_lkr


def test_tc_ing_055_overwriting_a_day_is_safe(tmp_path) -> None:
    """TC-ING-055: re-writing a day replaces the file cleanly (no append, no partial file)."""
    first = ep.write_expense_file(DAY, landing_dir=str(tmp_path))
    size_first = first.stat().st_size
    second = ep.write_expense_file(DAY, landing_dir=str(tmp_path))
    assert second == first
    assert second.stat().st_size == size_first
    assert len(read_csv(second)) == CFG.num_vehicles + CFG.expense_dirty_rows
