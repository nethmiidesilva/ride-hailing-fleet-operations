"""Unit tests for the shared zone mapping (common/zones.py).

Covers TC-ING-002 .. TC-ING-005 / REQ-12.  Because both the speed layer and the batch layer call
this function, a regression here would desynchronise the two layers, which is exactly the Lambda
failure mode the report discusses — hence the property-style round-trip test.
"""

from __future__ import annotations

import pytest

from common.config import CFG
from common.zones import GRID, OUT_OF_AREA, ZONE_NAMES, in_service_area, zone_centre, zone_for_point

pytestmark = pytest.mark.unit


def test_tc_ing_002_every_zone_centre_maps_back_to_itself() -> None:
    """TC-ING-002: the centre of every grid cell resolves to that cell (round trip)."""
    assert len(ZONE_NAMES) == 6
    for zone in ZONE_NAMES:
        lat, lon = zone_centre(zone)
        assert zone_for_point(lat, lon) == zone, f"{zone} centre {(lat, lon)} mis-mapped"


def test_tc_ing_003_corners_of_the_bounding_box_are_inside() -> None:
    """TC-ING-003: the bounding box is inclusive on all four edges."""
    corners = [
        (CFG.zone_lat_min, CFG.zone_lon_min, GRID[0][0]),
        (CFG.zone_lat_min, CFG.zone_lon_max, GRID[0][2]),
        (CFG.zone_lat_max, CFG.zone_lon_min, GRID[1][0]),
        (CFG.zone_lat_max, CFG.zone_lon_max, GRID[1][2]),
    ]
    for lat, lon, expected in corners:
        assert zone_for_point(lat, lon) == expected


def test_tc_ing_004_internal_boundary_belongs_to_the_higher_cell() -> None:
    """TC-ING-004: a point exactly on an internal grid line is single-valued (higher cell)."""
    lat_mid = CFG.zone_lat_min + (CFG.zone_lat_max - CFG.zone_lat_min) / 2
    lon_third = CFG.zone_lon_min + (CFG.zone_lon_max - CFG.zone_lon_min) / 3
    # On the latitude midline -> northern band (row 1); on the first longitude line -> column 1.
    assert zone_for_point(lat_mid, lon_third) == GRID[1][1]
    # Just below the midline stays in the southern band.
    assert zone_for_point(lat_mid - 1e-6, lon_third) == GRID[0][1]


@pytest.mark.parametrize(
    ("lat", "lon"),
    [
        (0.0, 0.0),  # Gulf of Guinea
        (6.85, 79.86),  # just south of the box
        (6.99, 79.86),  # just north of the box
        (6.90, 79.83),  # just west
        (6.90, 79.91),  # just east
        (None, 79.86),  # missing coordinate
        (6.90, None),
        ("abc", 79.86),  # non-numeric
        (float("nan"), 79.86),  # NaN must not raise
    ],
)
def test_tc_ing_005_outside_or_unusable_points_are_out_of_area(lat, lon) -> None:
    """TC-ING-005: anything outside the licensed area or unusable becomes OUT_OF_AREA."""
    assert zone_for_point(lat, lon) == OUT_OF_AREA
    assert in_service_area(lat, lon) is False


def test_tc_ing_006_function_is_deterministic_and_pure() -> None:
    """TC-ING-006: repeated calls with the same input give the same output."""
    samples = [(6.87, 79.85), (6.93, 79.87), (6.97, 79.895)]
    first = [zone_for_point(*s) for s in samples]
    second = [zone_for_point(*s) for s in samples]
    assert first == second
    assert all(z in ZONE_NAMES for z in first)


def test_tc_ing_007_unknown_zone_centre_raises() -> None:
    """TC-ING-007: asking for the centre of a non-existent zone is a programming error."""
    with pytest.raises(ValueError, match="unknown zone"):
        zone_centre("Atlantis")


def test_tc_ing_008_grid_covers_the_box_without_gaps() -> None:
    """TC-ING-008: a dense sweep of the box never produces OUT_OF_AREA (no gaps in the grid)."""
    steps = 20
    lat_step = (CFG.zone_lat_max - CFG.zone_lat_min) / steps
    lon_step = (CFG.zone_lon_max - CFG.zone_lon_min) / steps
    seen = set()
    for i in range(steps + 1):
        for j in range(steps + 1):
            zone = zone_for_point(
                CFG.zone_lat_min + i * lat_step, CFG.zone_lon_min + j * lon_step
            )
            assert zone != OUT_OF_AREA
            seen.add(zone)
    assert seen == set(ZONE_NAMES), "every zone must be reachable inside the box"
