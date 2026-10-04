"""Latitude/longitude to operating-zone mapping for the Colombo service area.

Why this module is shared
-------------------------
Lambda architecture's best-known weakness is having two code paths that can silently disagree.
Zone assignment is exactly the kind of business rule that would drift: if the speed layer bucketed
a point into Fort and the batch layer into Pettah, the nightly reconciliation would never balance
and nobody would know why.  Both the Spark Structured Streaming job and the Spark batch job import
:func:`zone_for_point` from this one module, so the rule is defined once.  This is cited in the
report as the concrete mitigation for the two-code-path trade-off.

The grid
--------
The service area is the Colombo bounding box (lat 6.86-6.98, lon 79.84-79.90) split into a 3x2
grid of named zones (3 columns of longitude, 2 rows of latitude).  Anything outside the box is
``OUT_OF_AREA`` rather than an error, because a real fleet does drift out of its licensed area and
those trips still need to appear in the rejected/other bucket instead of being dropped.

The function is pure and deterministic, which makes it unit-testable (TC-ING-002..004) and safe to
wrap in a Spark UDF.
"""

from __future__ import annotations

from common.config import CFG

OUT_OF_AREA = "OUT_OF_AREA"

#: Zone names laid out as ``GRID[row][col]`` where row 0 is the *southern* band of latitudes and
#: column 0 is the *western* band of longitudes.  Names are real Colombo neighbourhoods so that
#: the dashboard reads like an operations screen rather than "zone_3".
GRID: tuple[tuple[str, str, str], tuple[str, str, str]] = (
    ("Wellawatte", "Bambalapitiya", "Borella"),  # southern band
    ("Kollupitiya", "Fort", "Pettah"),  # northern band
)

#: Flat list of every valid zone, used for dashboard drop-downs and test parametrisation.
ZONE_NAMES: tuple[str, ...] = tuple(name for row in GRID for name in row)

N_ROWS = len(GRID)
N_COLS = len(GRID[0])

#: Tolerance used when a point falls on a grid line — see the comment in :func:`zone_for_point`.
_BOUNDARY_EPS = 1e-9


def zone_for_point(lat: float | None, lon: float | None) -> str:
    """Map a GPS point to a zone name.

    Args:
        lat: latitude in decimal degrees; ``None`` or non-numeric yields ``OUT_OF_AREA``.
        lon: longitude in decimal degrees.

    Returns:
        One of :data:`ZONE_NAMES`, or :data:`OUT_OF_AREA` when the point is outside the
        configured bounding box or is not a usable number.

    Boundary rule:
        The box is inclusive on all four edges.  A point exactly on an internal grid line belongs
        to the *higher* cell (the cell whose range starts at that line), which is the usual
        half-open convention and keeps the mapping single-valued.
    """
    if lat is None or lon is None:
        return OUT_OF_AREA
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return OUT_OF_AREA

    # NaN fails every comparison, so this also filters NaN out into OUT_OF_AREA.
    if not (CFG.zone_lat_min <= lat_f <= CFG.zone_lat_max):
        return OUT_OF_AREA
    if not (CFG.zone_lon_min <= lon_f <= CFG.zone_lon_max):
        return OUT_OF_AREA

    lat_span = CFG.zone_lat_max - CFG.zone_lat_min
    lon_span = CFG.zone_lon_max - CFG.zone_lon_min

    # Scale the point into [0, N) then floor.  The min() clamps the top edge, which would
    # otherwise land on index N and fall off the end of the grid.
    #
    # _BOUNDARY_EPS: a point computed as "exactly on the grid line" in floating point is often
    # 0.9999999999999926 rather than 1.0 (DEFECT-001 in the test report: TC-ING-004 caught it).
    # Without the epsilon the documented "boundary belongs to the higher cell" rule would be
    # decided by the last bit of the mantissa, so the SAME coordinate could land in a different
    # zone in the speed layer than in the batch layer.  Nudging by 1e-9 degrees (~0.1 mm) makes
    # the rule exact and both layers agree by construction.
    row = min(N_ROWS - 1, int((lat_f - CFG.zone_lat_min) / lat_span * N_ROWS + _BOUNDARY_EPS))
    col = min(N_COLS - 1, int((lon_f - CFG.zone_lon_min) / lon_span * N_COLS + _BOUNDARY_EPS))
    return GRID[row][col]


def zone_centre(zone: str) -> tuple[float, float]:
    """Return the (lat, lon) centre of ``zone``.

    The simulator uses this to spawn vehicles in plausible places and the unit tests use it to
    assert that the centre of every cell maps back to that cell (a round-trip property test).
    """
    for r, row in enumerate(GRID):
        for c, name in enumerate(row):
            if name == zone:
                lat_span = (CFG.zone_lat_max - CFG.zone_lat_min) / N_ROWS
                lon_span = (CFG.zone_lon_max - CFG.zone_lon_min) / N_COLS
                return (
                    CFG.zone_lat_min + lat_span * (r + 0.5),
                    CFG.zone_lon_min + lon_span * (c + 0.5),
                )
    raise ValueError(f"unknown zone: {zone!r}")


def in_service_area(lat: float | None, lon: float | None) -> bool:
    """True when the point is inside the licensed operating area."""
    return zone_for_point(lat, lon) != OUT_OF_AREA


__all__ = ["GRID", "OUT_OF_AREA", "ZONE_NAMES", "in_service_area", "zone_centre", "zone_for_point"]
