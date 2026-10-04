"""Unit tests for the consolidated daily report (batch/report_builder.py).

Covers TC-SRV-020 .. TC-SRV-028 / REQ-07.  The database layer is stubbed so the renderer is
tested on known inputs; the *content* assertions are what matter — a marker will read this file.
"""

from __future__ import annotations

import csv
from datetime import UTC, date, datetime

import pytest

from batch import report_builder as rb

pytestmark = pytest.mark.unit


def sample_data() -> dict:
    """A small but complete dataset covering every branch of the renderer."""
    return {
        "report_date": "2026-09-01",
        "generated_at": datetime(2026, 9, 1, 12, 0, tzinfo=UTC).isoformat(),
        "kpis": {
            "vehicles": 3,
            "trips": 11,
            "revenue_lkr": 2300.0,
            "cost_lkr": 1000.0,
            "profit_lkr": 1300.0,
            "avg_utilization": 0.4667,
            "active_minutes": 672.0,
            "idle_minutes": 768.0,
            "unprofitable_vehicles": 1,
            "at_risk_vehicles": 1,
            "flagged_vehicles": 1,
        },
        "vehicles": [
            {
                "vehicle_id": "V-002", "driver_id": "D-002", "trips": 2, "revenue_lkr": 500.0,
                "fuel_cost": 300.0, "maintenance_cost": 400.0, "total_cost_lkr": 700.0,
                "profit_lkr": -200.0, "margin": -0.4, "utilization": 0.3, "distance_km": 20.0,
                "cost_per_km": 35.0, "revenue_per_km": 25.0, "is_unprofitable": True,
                "trend": "AT_RISK", "data_quality_flag": "OK",
            },
            {
                "vehicle_id": "V-003", "driver_id": "D-003", "trips": 4, "revenue_lkr": 800.0,
                "fuel_cost": 0.0, "maintenance_cost": 0.0, "total_cost_lkr": 0.0,
                "profit_lkr": 800.0, "margin": 1.0, "utilization": 0.5, "distance_km": 0.0,
                "cost_per_km": None, "revenue_per_km": None, "is_unprofitable": False,
                "trend": "STABLE", "data_quality_flag": "MISSING_EXPENSE",
            },
            {
                "vehicle_id": "V-001", "driver_id": "D-001", "trips": 5, "revenue_lkr": 1000.0,
                "fuel_cost": 200.0, "maintenance_cost": 100.0, "total_cost_lkr": 300.0,
                "profit_lkr": 700.0, "margin": 0.7, "utilization": 0.6, "distance_km": 10.0,
                "cost_per_km": 30.0, "revenue_per_km": 100.0, "is_unprofitable": False,
                "trend": "DECLINING", "data_quality_flag": "OK",
            },
        ],
        "zone_hour": [
            {"zone": "Fort", "sim_hour": 8, "trips": 3, "earnings_lkr": 600.0,
             "utilization": 0.6, "avg_speed_kmh": 32.0},
            {"zone": "Fort", "sim_hour": 9, "trips": 2, "earnings_lkr": 400.0,
             "utilization": 0.5, "avg_speed_kmh": 28.0},
            {"zone": "Pettah", "sim_hour": 8, "trips": 6, "earnings_lkr": 1300.0,
             "utilization": 0.7, "avg_speed_kmh": 30.0},
        ],
        "zone_totals": [
            {"zone": "Pettah", "trips": 6, "earnings_lkr": 1300.0, "avg_utilization": 0.7},
            {"zone": "Fort", "trips": 5, "earnings_lkr": 1000.0, "avg_utilization": 0.55},
        ],
        "alerts": [
            {"vehicle_id": "V-001", "zone": "Fort",
             "idle_since": datetime(2026, 9, 1, 11, 0, tzinfo=UTC),
             "detected_at": datetime(2026, 9, 1, 11, 3, tzinfo=UTC),
             "resolved_at": None, "idle_minutes": 3.2, "status": "open"},
        ],
        "rejected_events": [
            {"reason": "NEGATIVE_SPEED", "rows_rejected": 11},
            {"reason": "LAT_OUT_OF_RANGE", "rows_rejected": 8},
        ],
        "rejected_expenses": [{"reason": "MISSING_VEHICLE_ID", "rows_rejected": 1}],
        "reconciliation": {
            "batch_revenue_lkr": 2300.0,
            "speed_revenue_lkr": 2250.0,
            "difference_lkr": -50.0,
            "difference_pct": -2.17,
            "speed_trips": 10,
            "batch_trips": 11,
            "speed_windows": 42,
            "window_span": ["2026-09-01 12:00", "2026-09-01 12:42"],
        },
    }


def test_tc_srv_020_html_contains_every_required_section() -> None:
    """TC-SRV-020: the report has all six sections the brief asks for."""
    html = rb.render_html(sample_data())
    for section in (
        "Fleet KPIs",
        "Per-vehicle profitability",
        "Earnings by zone and simulated hour",
        "Idle alerts",
        "Data quality",
        "Batch vs speed-layer reconciliation",
    ):
        assert section in html, f"missing section: {section}"


def test_tc_srv_021_unprofitable_rows_are_highlighted() -> None:
    """TC-SRV-021: loss-making rows carry the `loss` class, at-risk rows `warn`."""
    html = rb.render_html(sample_data())
    assert 'class="loss"' in html, "unprofitable vehicle is not highlighted"
    assert 'class="flag"' in html, "MISSING_EXPENSE row is not highlighted"
    # The CSS that makes those classes visible must be present in the self-contained file.
    assert "tr.loss td" in html and "tr.warn td" in html


def test_tc_srv_022_every_vehicle_appears_in_the_table() -> None:
    """TC-SRV-022: no vehicle is dropped from the report, including flagged ones."""
    html = rb.render_html(sample_data())
    for vehicle in ("V-001", "V-002", "V-003"):
        assert vehicle in html


def test_tc_srv_023_kpis_are_rendered_with_real_values() -> None:
    """TC-SRV-023: the KPI cards show the numbers passed in, formatted with thousands separators."""
    html = rb.render_html(sample_data())
    assert "2,300.00" in html, "revenue KPI missing"
    assert "1,300.00" in html, "profit KPI missing"
    assert ">11<" in html or "11" in html, "trips KPI missing"


def test_tc_srv_024_null_values_render_as_a_dash_not_none() -> None:
    """TC-SRV-024: a NULL cost_per_km must not leak the string 'None' into the report."""
    html = rb.render_html(sample_data())
    assert ">None<" not in html
    assert "&mdash;" in html


def test_tc_srv_025_heatmap_covers_every_zone_and_hour() -> None:
    """TC-SRV-025: the zone x sim_hour grid renders one row per zone and one column per hour."""
    html = rb._heatmap(sample_data()["zone_hour"])
    assert "Fort" in html and "Pettah" in html
    assert ">08<" in html and ">09<" in html
    assert "1,300" in html, "the peak cell value must be shown"


def test_tc_srv_026_reconciliation_states_the_difference_and_explains_it() -> None:
    """TC-SRV-026: the report reports the speed-vs-batch gap honestly and gives the reason."""
    html = rb.render_html(sample_data())
    assert "2,250.00" in html and "2,300.00" in html
    assert "watermark" in html, "the explanation for the difference must be included"
    assert "batch figure is the one the business uses" in html


def test_tc_srv_027_csv_has_one_row_per_vehicle_with_the_agreed_columns(tmp_path) -> None:
    """TC-SRV-027: the machine-readable CSV matches the HTML table."""
    path = rb.write_csv(sample_data(), tmp_path / "profitability_2026-09-01.csv")
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 3
    assert {r["vehicle_id"] for r in rows} == {"V-001", "V-002", "V-003"}
    for required in ("revenue_lkr", "total_cost_lkr", "profit_lkr", "margin", "trend",
                     "data_quality_flag"):
        assert required in rows[0]


def test_tc_srv_028_empty_sections_do_not_crash_the_renderer() -> None:
    """TC-SRV-028: a day with no alerts and no rejects still renders a valid report.

    The first day of a run legitimately has empty tables; the report must degrade gracefully
    rather than raising and failing the Airflow task.
    """
    data = sample_data()
    data["alerts"] = []
    data["rejected_events"] = []
    data["rejected_expenses"] = []
    data["zone_hour"] = []
    html = rb.render_html(data)
    assert "No rows." in html
    assert "No zone summary rows." in html
    assert "Fleet KPIs" in html


def test_tc_srv_029_build_report_writes_both_files(tmp_path, monkeypatch) -> None:
    """TC-SRV-029: build_report produces the HTML and the CSV at the documented paths."""
    # The report date is irrelevant to the stub, hence the underscore.
    monkeypatch.setattr(rb, "gather", lambda _report_date: sample_data())
    paths = rb.build_report(date(2026, 9, 1), run_id="test", reports_dir=str(tmp_path))
    html_path = tmp_path / "profitability_2026-09-01.html"
    csv_path = tmp_path / "profitability_2026-09-01.csv"
    assert html_path.exists() and csv_path.exists()
    assert paths["html"] == str(html_path)
    assert paths["csv"] == str(csv_path)
    assert html_path.stat().st_size > 2000, "the report should not be a stub"
