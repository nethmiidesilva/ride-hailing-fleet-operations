"""Vehicle state machine for the simulated ride-hailing fleet.

This module is deliberately *pure*: it holds no sockets, no Kafka client and no wall-clock
dependency beyond what is passed in.  The Kafka plumbing lives in :mod:`producers.gps_producer`.
Separating them means the interesting business behaviour — state transitions, movement, fare
calculation, fault injection — is unit-testable in milliseconds without any infrastructure
(TC-ING-030..TC-ING-041).

The state machine
-----------------
::

        +--------+  passenger request   +----------+  pickup   +-----------+
        |  idle  | -------------------> | enroute  | --------> |  on_trip  |
        +--------+                      +----------+           +-----------+
             ^                                                        |
             |                    trip ends (fare > 0)                |
             +--------------------------------------------------------+

* ``idle``    — parked, ``speed = 0``, ``trip_id = None``.
* ``enroute`` — driving to the pickup point; moving but earning nothing.  This state is what
  makes "utilisation" more interesting than "is the engine on": enroute time is active but
  unpaid, and the report distinguishes the two.
* ``on_trip`` — carrying a passenger; distance accumulates and determines the fare.

Only the ``on_trip -> idle`` transition carries a fare.  Every other event has ``fare = 0``.
That single rule is what makes revenue computable by a plain ``SUM(fare)`` in both the speed
layer and the batch layer without any risk of double counting — an important property for the
reconciliation test, which compares the two totals.

Fault injection
---------------
Real pipelines deal with malformed, late and duplicated data, so the simulator produces all
three on demand (``BAD_EVENT_RATE``, ``LATE_EVENT_RATE``, ``DUPLICATE_RATE``).  Without them the
validation, watermark and dedup logic in the stream job could not be demonstrated at all.
"""

from __future__ import annotations

import math
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from common.config import CFG
from common.zones import ZONE_NAMES, zone_centre

# --- state names (kept as module constants so tests and the stream job agree) -----------------
IDLE = "idle"
ENROUTE = "enroute"
ON_TRIP = "on_trip"

#: Legal transitions.  Anything else is a bug and is asserted against in TC-ING-030.
ALLOWED_TRANSITIONS: dict[str, tuple[str, ...]] = {
    IDLE: (IDLE, ENROUTE),
    ENROUTE: (ENROUTE, ON_TRIP),
    ON_TRIP: (ON_TRIP, IDLE),
}

#: Degrees of latitude per kilometre (1 deg lat ~ 111 km everywhere).
_DEG_PER_KM_LAT = 1.0 / 111.0

#: Dwell times in REAL seconds.  They are tuned against EMIT_INTERVAL_SEC=2 and
#: IDLE_ALERT_MINUTES=3 so that ordinary vehicles never trigger an idle alert but the designated
#: "lazy" vehicles reliably do, a few minutes into the demo.
NORMAL_IDLE_RANGE_S = (6.0, 24.0)
LAZY_IDLE_RANGE_S = (240.0, 420.0)
ENROUTE_RANGE_S = (8.0, 20.0)
ON_TRIP_RANGE_S = (20.0, 70.0)

#: Speed envelope in km/h while moving; 0 while idle.
SPEED_RANGE_KMH = (10.0, 60.0)

#: The kinds of corruption injected, used as a Prometheus label so the report can show exactly
#: how many of each were produced versus how many the stream job quarantined.
BAD_EVENT_KINDS = (
    "null_vehicle_id",
    "negative_speed",
    "lat_out_of_range",
    "lon_out_of_range",
    "unknown_status",
    "negative_fare",
)


@dataclass
class Vehicle:
    """Mutable state of one simulated vehicle."""

    vehicle_id: str
    driver_id: str
    lat: float
    lon: float
    status: str = IDLE
    speed_kmh: float = 0.0
    trip_id: str | None = None
    #: Kilometres travelled in the current trip; drives the fare on the trip-end event.
    trip_distance_km: float = 0.0
    #: Real seconds left in the current state before a transition is considered.
    dwell_remaining_s: float = 0.0
    #: Heading in radians, kept between ticks so movement looks like driving, not teleporting.
    heading_rad: float = 0.0
    #: "Lazy" vehicles idle for far longer; they exist so idle alerts fire during a short demo.
    lazy: bool = False
    #: Number of completed trips, used by the simulator's own self-check in tests.
    completed_trips: int = 0

    def to_public(self) -> dict[str, Any]:
        """Snapshot used by tests and by the /debug view of the producer."""
        return {
            "vehicle_id": self.vehicle_id,
            "status": self.status,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed_kmh": round(self.speed_kmh, 2),
            "trip_id": self.trip_id,
            "lazy": self.lazy,
        }


@dataclass
class FleetSimulator:
    """Deterministic simulator for ``num_vehicles`` vehicles.

    Args:
        num_vehicles: fleet size.
        seed: RNG seed.  With the same seed the simulator produces the same fleet, the same
            transitions and the same fares, which is what makes the demo and the tests
            reproducible (REQ-10).
        lazy_count: how many vehicles get the long idle profile.
        tick_seconds: nominal real seconds between ticks; used to advance dwell timers and to
            convert speed into distance.
    """

    num_vehicles: int = CFG.num_vehicles
    seed: int = CFG.seed
    lazy_count: int = CFG.lazy_vehicles
    tick_seconds: float = CFG.emit_interval_sec
    rng: random.Random = field(init=False)
    vehicles: list[Vehicle] = field(init=False)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self.vehicles = [self._make_vehicle(i) for i in range(self.num_vehicles)]

    # -- construction ------------------------------------------------------------------------
    def _make_vehicle(self, index: int) -> Vehicle:
        """Create vehicle ``index``, spawned near the centre of one of the zones."""
        zone = ZONE_NAMES[index % len(ZONE_NAMES)]
        centre_lat, centre_lon = zone_centre(zone)
        jitter = 0.004  # ~450 m, keeps the vehicle inside its starting zone
        vehicle = Vehicle(
            vehicle_id=f"V-{index + 1:03d}",
            driver_id=f"D-{index + 1:03d}",
            lat=centre_lat + self.rng.uniform(-jitter, jitter),
            lon=centre_lon + self.rng.uniform(-jitter, jitter),
            heading_rad=self.rng.uniform(0, 2 * math.pi),
            # The first `lazy_count` vehicles are the lazy ones: deterministic, so the demo
            # script can say "watch V-001" and be right every time.
            lazy=index < self.lazy_count,
        )
        vehicle.dwell_remaining_s = self._idle_dwell(vehicle)
        return vehicle

    # -- dwell helpers -----------------------------------------------------------------------
    def _idle_dwell(self, vehicle: Vehicle) -> float:
        low, high = LAZY_IDLE_RANGE_S if vehicle.lazy else NORMAL_IDLE_RANGE_S
        return self.rng.uniform(low, high)

    # -- movement ----------------------------------------------------------------------------
    def _move(self, vehicle: Vehicle, dt_s: float) -> float:
        """Advance ``vehicle`` along its heading and return the distance covered in km.

        The heading drifts a little each tick so the track looks like a road journey rather than
        a straight line, and the vehicle turns back when it reaches the edge of the service area
        (a crude but sufficient stand-in for a road network).
        """
        if vehicle.speed_kmh <= 0:
            return 0.0

        km = vehicle.speed_kmh * dt_s / 3600.0
        vehicle.heading_rad += self.rng.uniform(-0.4, 0.4)

        d_lat = km * _DEG_PER_KM_LAT * math.cos(vehicle.heading_rad)
        # Longitude degrees shrink with latitude; at 6.9 deg N the factor is ~0.993, but the
        # correction is kept for correctness rather than magnitude.
        lon_scale = max(0.1, math.cos(math.radians(vehicle.lat)))
        d_lon = km * _DEG_PER_KM_LAT / lon_scale * math.sin(vehicle.heading_rad)

        new_lat = vehicle.lat + d_lat
        new_lon = vehicle.lon + d_lon

        # Reflect off the bounding box so vehicles stay in the licensed area most of the time.
        if not (CFG.zone_lat_min <= new_lat <= CFG.zone_lat_max):
            vehicle.heading_rad = math.pi - vehicle.heading_rad
            new_lat = min(max(new_lat, CFG.zone_lat_min), CFG.zone_lat_max)
        if not (CFG.zone_lon_min <= new_lon <= CFG.zone_lon_max):
            vehicle.heading_rad = -vehicle.heading_rad
            new_lon = min(max(new_lon, CFG.zone_lon_min), CFG.zone_lon_max)

        vehicle.lat, vehicle.lon = new_lat, new_lon
        return km

    # -- state machine -----------------------------------------------------------------------
    def _advance(self, vehicle: Vehicle, dt_s: float) -> float:
        """Advance one vehicle by ``dt_s`` seconds; return the fare earned on this tick (LKR).

        A non-zero return value happens only on the ``on_trip -> idle`` transition.
        """
        vehicle.dwell_remaining_s -= dt_s
        fare = 0.0

        if vehicle.status == IDLE:
            vehicle.speed_kmh = 0.0
            if vehicle.dwell_remaining_s <= 0:
                # A passenger request arrives: start driving to the pickup point.
                vehicle.status = ENROUTE
                vehicle.speed_kmh = self.rng.uniform(*SPEED_RANGE_KMH)
                vehicle.dwell_remaining_s = self.rng.uniform(*ENROUTE_RANGE_S)
                vehicle.trip_id = None
            return fare

        if vehicle.status == ENROUTE:
            self._move(vehicle, dt_s)
            if vehicle.dwell_remaining_s <= 0:
                vehicle.status = ON_TRIP
                vehicle.trip_id = f"T-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12]}"
                vehicle.trip_distance_km = 0.0
                vehicle.speed_kmh = self.rng.uniform(*SPEED_RANGE_KMH)
                vehicle.dwell_remaining_s = self.rng.uniform(*ON_TRIP_RANGE_S)
            return fare

        # ON_TRIP
        vehicle.trip_distance_km += self._move(vehicle, dt_s)
        if vehicle.dwell_remaining_s <= 0:
            fare = self.compute_fare(vehicle.trip_distance_km)
            vehicle.status = IDLE
            vehicle.speed_kmh = 0.0
            vehicle.completed_trips += 1
            vehicle.dwell_remaining_s = self._idle_dwell(vehicle)
            # trip_id is intentionally kept on this one event so the trip-end can be attributed
            # to its trip; it is cleared on the next idle tick.
        return fare

    def compute_fare(self, distance_km: float) -> float:
        """Fare for a completed trip, in LKR.

        ``base + per_km * distance``, perturbed by up to ``FARE_NOISE_PCT`` to represent surge,
        waiting time and rounding at the meter.  Never negative.
        """
        noise = 1.0 + self.rng.uniform(-CFG.fare_noise_pct, CFG.fare_noise_pct)
        fare = (CFG.fare_base_lkr + CFG.fare_per_km_lkr * distance_km) * noise
        return round(max(0.0, fare), 2)

    # -- event emission ----------------------------------------------------------------------
    def tick(
        self,
        event_time: datetime,
        sim_ts: datetime,
        dt_s: float | None = None,
    ) -> list[dict[str, Any]]:
        """Advance the whole fleet one tick and return one telemetry event per vehicle.

        Args:
            event_time: real wall-clock UTC stamped on every event (streaming windows).
            sim_ts: simulated timestamp (batch partitioning and time-of-day analysis).
            dt_s: seconds since the previous tick; defaults to ``tick_seconds``.
        """
        dt = self.tick_seconds if dt_s is None else dt_s
        events: list[dict[str, Any]] = []
        for vehicle in self.vehicles:
            previous_trip_id = vehicle.trip_id
            fare = self._advance(vehicle, dt)
            trip_id = previous_trip_id if fare > 0 else vehicle.trip_id
            events.append(
                {
                    "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
                    "trip_id": trip_id,
                    "driver_id": vehicle.driver_id,
                    "vehicle_id": vehicle.vehicle_id,
                    "lat": round(vehicle.lat, 6),
                    "lon": round(vehicle.lon, 6),
                    "speed": round(vehicle.speed_kmh, 2),
                    "status": vehicle.status,
                    "fare": fare,
                    "event_time": _iso(event_time),
                    "sim_ts": _iso(sim_ts),
                    "sim_date": sim_ts.date().isoformat(),
                    "sim_hour": sim_ts.hour,
                }
            )
            if fare > 0:
                # Clear the finished trip only after the event has been built.
                vehicle.trip_id = None
        return events

    # -- fault injection ---------------------------------------------------------------------
    def corrupt(self, event: dict[str, Any]) -> tuple[dict[str, Any], str]:
        """Return a deliberately malformed copy of ``event`` and the kind of corruption.

        Every kind maps one-to-one onto a rejection reason in :mod:`common.schemas`, so the
        producer's ``producer_bad_events_injected_total`` and the stream job's
        ``stream_rows_rejected_total`` can be compared directly in the report.
        """
        bad = dict(event)
        kind = self.rng.choice(BAD_EVENT_KINDS)
        if kind == "null_vehicle_id":
            bad["vehicle_id"] = None
        elif kind == "negative_speed":
            bad["speed"] = -abs(self.rng.uniform(1, 40))
        elif kind == "lat_out_of_range":
            bad["lat"] = self.rng.choice([95.0, -120.5])
        elif kind == "lon_out_of_range":
            bad["lon"] = self.rng.choice([200.0, -250.0])
        elif kind == "unknown_status":
            bad["status"] = self.rng.choice(["flying", "unknown", ""])
        elif kind == "negative_fare":
            bad["fare"] = -abs(self.rng.uniform(10, 500))
        return bad, kind

    def make_late(self, event: dict[str, Any]) -> dict[str, Any]:
        """Return a copy whose ``event_time`` is 30-90 s in the past.

        These events exercise the 2-minute watermark: most arrive in time and correct their
        window, while the tail beyond the watermark is dropped — which is exactly the
        approximation the speed layer makes and the batch layer later corrects.
        """
        late = dict(event)
        delay = self.rng.uniform(CFG.late_event_min_sec, CFG.late_event_max_sec)
        original = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        late["event_time"] = _iso(original - timedelta(seconds=delay))
        late["late_by_seconds"] = round(delay, 1)
        return late

    def snapshot(self) -> list[dict[str, Any]]:
        """Current state of every vehicle (used by tests and troubleshooting)."""
        return [v.to_public() for v in self.vehicles]

    def __iter__(self) -> Iterator[Vehicle]:
        return iter(self.vehicles)


def _iso(value: datetime) -> str:
    """ISO-8601 with milliseconds and an explicit UTC marker.

    Spark parses this with ``to_timestamp`` and no format string ambiguity; keeping the format
    identical in every producer avoids a whole class of "why is this column NULL" bugs.
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


__all__ = [
    "ALLOWED_TRANSITIONS",
    "BAD_EVENT_KINDS",
    "ENROUTE",
    "FleetSimulator",
    "IDLE",
    "ON_TRIP",
    "Vehicle",
]
