"""Unit tests for the vehicle state machine (producers/fleet_simulator.py).

Covers TC-ING-030 .. TC-ING-041 / REQ-01 (streaming source) and REQ-10 (reproducibility).
No Kafka, no Docker: the simulator is pure logic, which is exactly why it was separated from the
producer.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from common.config import CFG
from common.schemas import VALID_STATUSES, reject_reason
from common.zones import OUT_OF_AREA, zone_for_point
from producers.fleet_simulator import (
    ALLOWED_TRANSITIONS,
    BAD_EVENT_KINDS,
    IDLE,
    ON_TRIP,
    FleetSimulator,
)

pytestmark = pytest.mark.unit

T0 = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)


def run_ticks(sim: FleetSimulator, n: int, dt: float = 2.0) -> list[dict]:
    """Drive the simulator ``n`` ticks and return every event produced."""
    events: list[dict] = []
    for i in range(n):
        events.extend(
            sim.tick(
                event_time=T0 + timedelta(seconds=i * dt),
                sim_ts=T0 + timedelta(seconds=i * dt * CFG.sim_seconds_per_real_second),
                dt_s=dt,
            )
        )
    return events


def test_tc_ing_030_only_legal_state_transitions_occur() -> None:
    """TC-ING-030: the state machine never makes an illegal jump (e.g. idle -> on_trip)."""
    sim = FleetSimulator(num_vehicles=10, seed=7, lazy_count=2)
    previous = {v.vehicle_id: v.status for v in sim}
    for i in range(400):
        events = sim.tick(
            event_time=T0 + timedelta(seconds=2 * i),
            sim_ts=T0 + timedelta(seconds=2 * i),
            dt_s=2.0,
        )
        for event in events:
            vid = event["vehicle_id"]
            assert event["status"] in ALLOWED_TRANSITIONS[previous[vid]], (
                f"{vid}: illegal transition {previous[vid]} -> {event['status']}"
            )
            previous[vid] = event["status"]


def test_tc_ing_031_fare_is_non_zero_only_on_trip_end() -> None:
    """TC-ING-031: revenue cannot be double counted — only the on_trip -> idle event carries a fare."""
    sim = FleetSimulator(num_vehicles=8, seed=11, lazy_count=0)
    events = run_ticks(sim, 400)
    paying = [e for e in events if e["fare"] > 0]
    assert paying, "the simulation must complete at least one trip in 400 ticks"
    for event in paying:
        assert event["status"] == IDLE, "a fare is only emitted at the moment a trip ends"
        assert event["trip_id"] is not None, "the trip-end event must name the trip it closes"
    # Every other event is strictly zero.
    assert all(e["fare"] == 0 for e in events if e["status"] != IDLE)


def test_tc_ing_032_each_trip_id_is_paid_exactly_once() -> None:
    """TC-ING-032: a trip_id appears on at most one paying event, so SUM(fare) is exact."""
    sim = FleetSimulator(num_vehicles=8, seed=3, lazy_count=0)
    events = run_ticks(sim, 500)
    paid_trip_ids = [e["trip_id"] for e in events if e["fare"] > 0]
    assert len(paid_trip_ids) == len(set(paid_trip_ids))


def test_tc_ing_033_speed_is_zero_while_idle_and_positive_while_moving() -> None:
    """TC-ING-033: the speed field is consistent with the status field."""
    sim = FleetSimulator(num_vehicles=12, seed=5, lazy_count=3)
    for event in run_ticks(sim, 200):
        if event["status"] == IDLE:
            assert event["speed"] == 0.0
        else:
            assert 0 < event["speed"] <= 60.0


def test_tc_ing_034_lazy_vehicles_stay_idle_much_longer() -> None:
    """TC-ING-034: designated lazy vehicles idle long enough to trigger the idle alert."""
    sim = FleetSimulator(num_vehicles=9, seed=13, lazy_count=3)
    lazy_ids = {v.vehicle_id for v in sim if v.lazy}
    assert len(lazy_ids) == 3

    events = run_ticks(sim, 150)  # 150 ticks x 2 s = 300 real seconds
    idle_share = {}
    for vid in {e["vehicle_id"] for e in events}:
        own = [e for e in events if e["vehicle_id"] == vid]
        idle_share[vid] = sum(1 for e in own if e["status"] == IDLE) / len(own)

    lazy_avg = sum(idle_share[v] for v in lazy_ids) / len(lazy_ids)
    busy_avg = sum(idle_share[v] for v in idle_share if v not in lazy_ids) / (
        len(idle_share) - len(lazy_ids)
    )
    assert lazy_avg > busy_avg + 0.3, f"lazy {lazy_avg:.2f} vs busy {busy_avg:.2f}"
    # And at least one lazy vehicle stays idle beyond the alert threshold within the window.
    assert max(idle_share[v] for v in lazy_ids) > (
        CFG.idle_alert_minutes * 60
    ) / 300.0


def test_tc_ing_035_simulation_is_deterministic_for_a_seed() -> None:
    """TC-ING-035: same seed -> identical event stream (REQ-10 reproducibility)."""
    a = run_ticks(FleetSimulator(num_vehicles=6, seed=99, lazy_count=1), 60)
    b = run_ticks(FleetSimulator(num_vehicles=6, seed=99, lazy_count=1), 60)
    assert a == b
    c = run_ticks(FleetSimulator(num_vehicles=6, seed=100, lazy_count=1), 60)
    assert a != c, "a different seed must produce a different run"


def test_tc_ing_036_emitted_events_pass_the_shared_validator() -> None:
    """TC-ING-036: clean events (no injection) satisfy common.schemas — producer and consumer agree."""
    sim = FleetSimulator(num_vehicles=10, seed=21, lazy_count=2)
    for event in run_ticks(sim, 120):
        assert reject_reason(event) is None, f"simulator produced an invalid event: {event}"
        assert event["status"] in VALID_STATUSES


def test_tc_ing_037_vehicles_stay_inside_the_service_area() -> None:
    """TC-ING-037: the movement model keeps vehicles inside the Colombo bounding box."""
    sim = FleetSimulator(num_vehicles=10, seed=31, lazy_count=0)
    zones = {zone_for_point(e["lat"], e["lon"]) for e in run_ticks(sim, 300)}
    assert OUT_OF_AREA not in zones
    assert len(zones) >= 3, "vehicles should be spread over several zones"


def test_tc_ing_038_event_ids_are_unique() -> None:
    """TC-ING-038: event_id is a usable dedup key — the simulator never repeats one."""
    sim = FleetSimulator(num_vehicles=10, seed=41, lazy_count=0)
    ids = [e["event_id"] for e in run_ticks(sim, 100)]
    assert len(ids) == len(set(ids))


def test_tc_ing_039_dual_timestamps_are_present_and_consistent() -> None:
    """TC-ING-039: every event carries both clocks, and sim_date/sim_hour agree with sim_ts."""
    sim = FleetSimulator(num_vehicles=4, seed=51, lazy_count=0)
    for event in run_ticks(sim, 20):
        assert event["event_time"].endswith("Z")
        assert event["sim_ts"].endswith("Z")
        sim_ts = datetime.fromisoformat(event["sim_ts"].replace("Z", "+00:00"))
        assert event["sim_date"] == sim_ts.date().isoformat()
        assert event["sim_hour"] == sim_ts.hour


@pytest.mark.parametrize("_run", range(20))
def test_tc_ing_040_corrupt_produces_a_rejectable_event(_run: int) -> None:
    """TC-ING-040: every injected corruption is something the validator actually rejects."""
    sim = FleetSimulator(num_vehicles=2, seed=61 + _run, lazy_count=0)
    clean = sim.tick(event_time=T0, sim_ts=T0, dt_s=2.0)[0]
    bad, kind = sim.corrupt(clean)
    assert kind in BAD_EVENT_KINDS
    assert reject_reason(bad) is not None, f"corruption {kind} was not caught: {bad}"


def test_tc_ing_041_make_late_moves_event_time_backwards_only() -> None:
    """TC-ING-041: late events keep all other fields and only shift event_time into the past."""
    sim = FleetSimulator(num_vehicles=2, seed=71, lazy_count=0)
    clean = sim.tick(event_time=T0, sim_ts=T0, dt_s=2.0)[0]
    late = sim.make_late(clean)
    original = datetime.fromisoformat(clean["event_time"].replace("Z", "+00:00"))
    shifted = datetime.fromisoformat(late["event_time"].replace("Z", "+00:00"))
    delay = (original - shifted).total_seconds()
    assert CFG.late_event_min_sec <= delay <= CFG.late_event_max_sec
    assert late["sim_ts"] == clean["sim_ts"], "the simulated clock is untouched"
    assert reject_reason(late) is None, "a late event is still a VALID event"


def test_tc_ing_042_fare_formula_matches_the_documented_rule() -> None:
    """TC-ING-042: fare = (base + per_km x distance) x noise, within the configured noise band."""
    sim = FleetSimulator(num_vehicles=1, seed=81, lazy_count=0)
    distance = 4.0
    expected_mid = CFG.fare_base_lkr + CFG.fare_per_km_lkr * distance
    for _ in range(50):
        fare = sim.compute_fare(distance)
        assert expected_mid * (1 - CFG.fare_noise_pct) <= fare <= expected_mid * (
            1 + CFG.fare_noise_pct
        )
    assert sim.compute_fare(0.0) >= 0.0


def test_tc_ing_043_fleet_size_and_ids_follow_the_shared_convention() -> None:
    """TC-ING-043: vehicle ids are V-001.. so the expense file can join on them."""
    sim = FleetSimulator(num_vehicles=25, seed=1, lazy_count=3)
    ids = [v.vehicle_id for v in sim]
    assert ids[0] == "V-001" and ids[-1] == "V-025"
    assert len(set(ids)) == 25
    assert all(v.driver_id == v.vehicle_id.replace("V-", "D-") for v in sim)


def test_tc_ing_044_trips_actually_complete_and_distance_accumulates() -> None:
    """TC-ING-044: on_trip accumulates distance, so fares vary rather than being constant."""
    sim = FleetSimulator(num_vehicles=6, seed=91, lazy_count=0)
    events = run_ticks(sim, 400)
    fares = sorted({e["fare"] for e in events if e["fare"] > 0})
    assert len(fares) >= 3, "several distinct fares expected"
    assert fares[0] >= CFG.fare_base_lkr * (1 - CFG.fare_noise_pct)
    assert any(e["status"] == ON_TRIP for e in events)
