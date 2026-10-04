"""Unit tests for the FastAPI serving layer (api/main.py).

Covers TC-SRV-001 .. TC-SRV-014 / REQ-05 (real-time API), REQ-06 (idle alert), REQ-07 (daily
report) and REQ-09 (health rule).

The database layer is patched rather than mocked at the driver level: each test replaces
``api.main.query`` / ``api.main.query_one`` with a stub returning known rows.  That keeps the
tests fast, deterministic and independent of whether the Docker stack is running, while still
exercising the real routing, validation and response models.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api import main as api_main

pytestmark = pytest.mark.unit


@pytest.fixture()
def client() -> TestClient:
    return TestClient(api_main.app)


@pytest.fixture()
def stub_db(monkeypatch):
    """Install stub query functions; each test declares what SQL fragment returns what."""

    state: dict[str, Any] = {"one": {}, "many": {}, "default_one": None, "default_many": []}

    def fake_query_one(sql: str, params=None, dsn=None):
        for fragment, value in state["one"].items():
            if fragment in sql:
                return value
        return state["default_one"]

    def fake_query(sql: str, params=None, dsn=None):
        for fragment, value in state["many"].items():
            if fragment in sql:
                return value
        return state["default_many"]

    monkeypatch.setattr(api_main, "query_one", fake_query_one)
    monkeypatch.setattr(api_main, "query", fake_query)
    return state


# --------------------------------------------------------------------------------------------
# Health
# --------------------------------------------------------------------------------------------
def test_tc_srv_001_liveness_is_always_200(client: TestClient) -> None:
    """TC-SRV-001: /health/live does not touch any dependency."""
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"


def test_tc_srv_002_health_is_200_when_everything_is_fresh(client, stub_db, monkeypatch) -> None:
    """TC-SRV-002: healthy when Postgres, Kafka and data freshness all pass."""
    monkeypatch.setattr(
        api_main, "_check_kafka", lambda: api_main.HealthComponent(ok=True, latency_ms=3.0)
    )
    stub_db["one"] = {"SELECT 1 AS ok": {"ok": 1}, "EXTRACT(EPOCH": {"age": 4.2}}
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["components"]["postgres"]["ok"] is True
    assert body["components"]["data_freshness"]["ok"] is True
    assert body["freshness_seconds"] == pytest.approx(4.2, abs=0.1)


def test_tc_srv_003_health_is_503_when_the_database_is_down(client, stub_db, monkeypatch) -> None:
    """TC-SRV-003: a Postgres outage degrades the deep health check to 503."""

    def explode(*_a, **_k):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(api_main, "query_one", explode)
    monkeypatch.setattr(
        api_main, "_check_kafka", lambda: api_main.HealthComponent(ok=True, latency_ms=3.0)
    )
    response = client.get("/health")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["components"]["postgres"]["ok"] is False
    assert "connection refused" in body["components"]["postgres"]["detail"]


def test_tc_srv_004_health_is_503_when_data_is_stale(client, stub_db, monkeypatch) -> None:
    """TC-SRV-004: dependencies up but no telemetry for 10 minutes is NOT healthy.

    This is the state the chaos test produces by stopping the GPS producer.
    """
    monkeypatch.setattr(
        api_main, "_check_kafka", lambda: api_main.HealthComponent(ok=True, latency_ms=3.0)
    )
    stub_db["one"] = {"SELECT 1 AS ok": {"ok": 1}, "EXTRACT(EPOCH": {"age": 600.0}}
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["components"]["data_freshness"]["ok"] is False


# --------------------------------------------------------------------------------------------
# Speed-layer endpoints
# --------------------------------------------------------------------------------------------
def test_tc_srv_005_fleet_metrics_schema_and_values(client, stub_db) -> None:
    """TC-SRV-005: /metrics/fleet returns the documented schema with the view's values."""
    stub_db["one"] = {
        "FROM v_fleet_now": {
            "window_start": datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
            "window_end": datetime(2026, 9, 1, 12, 1, tzinfo=UTC),
            "active_vehicles": 18,
            "idle_vehicles": 7,
            "total_vehicles": 25,
            "idle_ratio": 0.28,
            "trips_completed": 6,
            "earnings_lkr": 1159.5,
            "avg_speed_kmh": 35.7,
            "zones_reporting": 6,
        },
        "interval '1 hour'": {"trips": 115, "earnings": 22302.16},
        "make_interval(secs": {"trips": 6, "earnings": 1159.5},
        "FROM idle_alerts WHERE status": {"n": 2},
        "EXTRACT(EPOCH": {"age": 3.1},
    }
    body = client.get("/metrics/fleet").json()
    assert body["source_layer"] == "speed"
    assert body["active_vehicles"] == 18
    assert body["idle_vehicles"] == 7
    assert body["total_vehicles"] == 25
    assert body["active_vehicles"] + body["idle_vehicles"] == body["total_vehicles"]
    assert body["trips_last_hour"] == 115
    assert body["open_idle_alerts"] == 2
    assert body["sim_hour_lookback_seconds"] >= 60


def test_tc_srv_006_zone_metrics_returns_a_list(client, stub_db) -> None:
    """TC-SRV-006: /metrics/zones returns one row per zone with the documented fields."""
    stub_db["many"] = {
        "GROUP BY zone": [
            {
                "zone": "Fort", "windows": 7, "active_vehicles": 5, "idle_vehicles": 4,
                "idle_ratio": 0.42, "trips": 22, "earnings_lkr": 4201.14, "avg_speed_kmh": 33.9,
            }
        ]
    }
    body = client.get("/metrics/zones?minutes=15").json()
    assert len(body) == 1
    assert body[0]["zone"] == "Fort"
    assert body[0]["source_layer"] == "speed"


def test_tc_srv_007_zone_metrics_rejects_an_out_of_range_lookback(client, stub_db) -> None:
    """TC-SRV-007: minutes is validated (422), not silently clamped."""
    assert client.get("/metrics/zones?minutes=0").status_code == 422
    assert client.get("/metrics/zones?minutes=99999").status_code == 422
    assert client.get("/metrics/zones?minutes=abc").status_code == 422


def test_tc_srv_008_idle_alerts_filter_by_status(client, stub_db) -> None:
    """TC-SRV-008: /alerts/idle?status=open returns only unresolved alerts."""
    stub_db["many"] = {
        "FROM idle_alerts WHERE status": [
            {
                "id": 1, "vehicle_id": "V-001", "zone": "Wellawatte",
                "idle_since": datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
                "detected_at": datetime(2026, 9, 1, 12, 3, tzinfo=UTC),
                "resolved_at": None, "idle_minutes": 3.01, "status": "open",
            }
        ]
    }
    body = client.get("/alerts/idle?status=open").json()
    assert body[0]["vehicle_id"] == "V-001"
    assert body[0]["status"] == "open"
    assert body[0]["resolved_at"] is None


def test_tc_srv_009_idle_alerts_rejects_an_unknown_status(client, stub_db) -> None:
    """TC-SRV-009: an unsupported status value is a 422, not an empty list."""
    assert client.get("/alerts/idle?status=maybe").status_code == 422


# --------------------------------------------------------------------------------------------
# Batch-layer endpoints
# --------------------------------------------------------------------------------------------
PROFIT_ROW = {
    "report_date": date(2026, 9, 1),
    "vehicle_id": "V-002",
    "driver_id": "D-002",
    "trips": 2,
    "revenue_lkr": 500.0,
    "fuel_cost": 300.0,
    "maintenance_cost": 400.0,
    "total_cost_lkr": 700.0,
    "profit_lkr": -200.0,
    "margin": -0.4,
    "utilization": 0.3,
    "distance_km": 20.0,
    "cost_per_km": 35.0,
    "revenue_per_km": 25.0,
    "is_unprofitable": True,
    "trend": "AT_RISK",
    "data_quality_flag": "OK",
}


def test_tc_srv_010_profitability_returns_rows_for_a_date(client, stub_db) -> None:
    """TC-SRV-010: /reports/profitability?date= returns the per-vehicle rows."""
    stub_db["many"] = {"FROM daily_vehicle_profitability": [PROFIT_ROW]}
    body = client.get("/reports/profitability?date=2026-09-01").json()
    assert len(body) == 1
    assert body[0]["vehicle_id"] == "V-002"
    assert body[0]["is_unprofitable"] is True
    assert body[0]["source_layer"] == "batch"


def test_tc_srv_011_bad_date_is_422_and_missing_data_is_404(client, stub_db) -> None:
    """TC-SRV-011: a malformed date is the client's fault (422); no data is 404."""
    assert client.get("/reports/profitability?date=01-09-2026").status_code == 422
    stub_db["many"] = {}
    stub_db["default_many"] = []
    assert client.get("/reports/profitability?date=2026-09-01").status_code == 404


def test_tc_srv_012_no_reconciled_day_yet_is_404(client, stub_db) -> None:
    """TC-SRV-012: before the first DAG run, the batch endpoints say so clearly."""
    stub_db["one"] = {"max(report_date)": {"d": None}}
    response = client.get("/reports/profitability")
    assert response.status_code == 404
    assert "Airflow" in response.json()["detail"]


def test_tc_srv_013_unprofitable_endpoint_answers_the_business_question(client, stub_db) -> None:
    """TC-SRV-013: /vehicles/unprofitable returns loss-making or trending vehicles."""
    stub_db["many"] = {"is_unprofitable OR trend": [PROFIT_ROW]}
    body = client.get("/vehicles/unprofitable?date=2026-09-01").json()
    assert body[0]["vehicle_id"] == "V-002"
    assert body[0]["trend"] == "AT_RISK"


def test_tc_srv_014_html_report_404_when_not_generated(client, stub_db) -> None:
    """TC-SRV-014: requesting a report that has not been rendered gives a helpful 404."""
    response = client.get("/reports/profitability/2099-01-01/html")
    assert response.status_code == 404
    assert "has not been generated" in response.json()["detail"]
    assert client.get("/reports/profitability/not-a-date/html").status_code == 422


def test_tc_srv_015_time_of_day_uses_the_batch_layer(client, stub_db) -> None:
    """TC-SRV-015: /metrics/time-of-day reads daily_zone_summary and is labelled batch."""
    stub_db["many"] = {
        "FROM daily_zone_summary": [
            {
                "report_date": date(2026, 9, 1), "sim_hour": 8, "zone": "Fort",
                "trips": 12, "earnings_lkr": 2400.0, "utilization": 0.62, "avg_speed_kmh": 34.1,
            }
        ]
    }
    body = client.get("/metrics/time-of-day?date=2026-09-01").json()
    assert body[0]["sim_hour"] == 8
    assert body[0]["source_layer"] == "batch"


def test_tc_srv_016_prometheus_endpoint_exposes_api_metrics(client, stub_db) -> None:
    """TC-SRV-016: /metrics-prom is scrapeable and contains this service's counters."""
    client.get("/health/live")  # generate at least one request metric
    response = client.get("/metrics-prom")
    assert response.status_code == 200
    assert "api_requests_total" in response.text
    assert "api_request_duration_seconds" in response.text


def test_tc_srv_017_openapi_schema_documents_every_endpoint(client) -> None:
    """TC-SRV-017: /docs is backed by a complete OpenAPI schema (a deliverable in its own right)."""
    schema = client.get("/openapi.json").json()
    paths = set(schema["paths"])
    for required in (
        "/health", "/health/live", "/metrics/fleet", "/metrics/zones", "/metrics/time-of-day",
        "/alerts/idle", "/reports/profitability", "/vehicles/unprofitable", "/metrics-prom",
    ):
        assert required in paths, f"{required} missing from the OpenAPI schema"
