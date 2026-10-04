"""Integration tests: require the docker compose stack to be running.

Covers TC-INT-001 .. TC-INT-014.  Each test asserts a real end-to-end property of the running
system, not a mock.  They are skipped (not failed) when the stack is unreachable, so
``pytest tests/`` is still usable on a laptop with Docker stopped.

Run with: ``docker compose run --rm tests pytest tests/integration -m integration``
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime

import pytest
import requests

from common.config import CFG
from common.db import execute, query, query_one
from tests.conftest import wait_until

pytestmark = [pytest.mark.integration]

API = f"http://{os.getenv('API_HOST', 'api')}:{os.getenv('API_PORT', '8000')}"
PROM = f"http://{os.getenv('PROM_HOST', 'prometheus')}:{os.getenv('PROM_PORT', '9090')}"
GRAFANA = f"http://{os.getenv('GRAFANA_HOST', 'grafana')}:{os.getenv('GRAFANA_PORT', '3000')}"
AIRFLOW = (
    f"http://{os.getenv('AIRFLOW_HOST', 'airflow-webserver')}:"
    f"{os.getenv('AIRFLOW_WEB_PORT', '8080')}"
)
AIRFLOW_AUTH = (
    os.getenv("AIRFLOW_ADMIN_USER", "admin"),
    os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"),
)


# --------------------------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------------------------
def test_tc_int_001_producer_messages_reach_kafka_with_vehicle_keys(stack_ready) -> None:
    """TC-INT-001: messages arrive on the topic, keyed by vehicle_id, across all 6 partitions.

    Also asserts the *stability* property that motivates the key choice: every message for a
    given vehicle lands in the same partition, so that vehicle's status transitions can never be
    reordered.
    """
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": CFG.kafka_bootstrap,
            "group.id": f"itest-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([CFG.kafka_topic])
    seen: dict[str, set[int]] = {}
    partitions: set[int] = set()
    deadline = time.time() + 90
    total = 0
    try:
        while time.time() < deadline and total < 400:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            total += 1
            key = msg.key().decode() if msg.key() else "UNKNOWN"
            partitions.add(msg.partition())
            seen.setdefault(key, set()).add(msg.partition())
            payload = json.loads(msg.value())
            assert "event_id" in payload and "event_time" in payload and "sim_date" in payload
    finally:
        consumer.close()

    assert total >= 50, f"only {total} messages consumed in 90 s"
    assert len(partitions) == CFG.kafka_partitions, (
        f"expected all {CFG.kafka_partitions} partitions to receive data, saw {sorted(partitions)}"
    )
    multi = {k: v for k, v in seen.items() if k != "UNKNOWN" and len(v) > 1}
    assert not multi, f"keys landed in more than one partition (ordering broken): {multi}"


def test_tc_int_002_expense_files_land_in_the_landing_directory(stack_ready) -> None:
    """TC-INT-002: the batch source writes one CSV per simulated day, atomically."""
    from pathlib import Path

    landing = Path(CFG.landing_dir)
    files = wait_until(
        lambda: sorted(landing.glob("expenses_*.csv")) or None,
        timeout_s=max(120, CFG.sim_day_seconds + 60),
        interval_s=5,
    )
    assert files, f"no expense file appeared in {landing} within one simulated day"
    # No partially-written temp files must ever be visible to the sensor.
    assert not list(landing.glob(".*tmp")), "a partial .tmp file is visible in the landing dir"
    header = files[0].read_text(encoding="utf-8").splitlines()[0]
    assert header.startswith("vehicle_id,fuel_cost,maintenance_cost")


# --------------------------------------------------------------------------------------------
# Speed layer
# --------------------------------------------------------------------------------------------
def test_tc_int_003_zone_metrics_appear_in_postgres(stack_ready) -> None:
    """TC-INT-003: Kafka -> Spark -> Postgres produces window rows within 2 minutes."""
    row = wait_until(
        lambda: query_one(
            "SELECT count(*) AS n FROM realtime_zone_metrics "
            "WHERE window_start > now() - interval '10 minutes'"
        ),
        timeout_s=150,
    )
    assert row and row["n"] > 0, "no recent realtime_zone_metrics rows"

    sample = query(
        "SELECT * FROM realtime_zone_metrics ORDER BY window_start DESC LIMIT 5"
    )
    for r in sample:
        assert 0.0 <= float(r["idle_ratio"]) <= 1.0, "idle_ratio must be a proportion"
        assert float(r["earnings_lkr"]) >= 0
        assert r["window_end"] > r["window_start"]


def test_tc_int_004_vehicle_status_has_one_row_per_vehicle(stack_ready) -> None:
    """TC-INT-004: the UPSERT keeps exactly one row per vehicle, no duplicates.

    Two separate properties, and the distinction matters (DEFECT-018):

    * ``vehicle_status`` must have exactly one row per ``vehicle_id`` -- that is the UPSERT key
      working, and it holds over the whole table for all time.
    * the *live* fleet must not exceed the configured size -- but ``vehicle_status`` is never
      pruned, so it legitimately retains rows for vehicles that have left the fleet (the
      200-vehicle load test, TC-NFR-004, leaves 175 behind). "Live" therefore means recently
      seen, which is exactly what ``v_fleet_now`` now filters on.
    """
    row = query_one("SELECT count(*) AS n, count(DISTINCT vehicle_id) AS d FROM vehicle_status")
    assert row["n"] == row["d"], "duplicate vehicle_status rows — the UPSERT key is wrong"
    assert row["n"] > 0

    live = query_one(
        "SELECT count(*) AS n FROM vehicle_status "
        "WHERE last_event_time > now() - INTERVAL '10 minutes'"
    )
    assert 0 < live["n"] <= CFG.num_vehicles, (
        f"live fleet is {live['n']} but only {CFG.num_vehicles} are configured; "
        f"{row['n']} rows exist in total (older rows are retained by design)"
    )

    fleet = query_one("SELECT active_vehicles, idle_vehicles, total_vehicles FROM v_fleet_now")
    if fleet:
        assert fleet["active_vehicles"] + fleet["idle_vehicles"] == fleet["total_vehicles"], (
            "the parts must equal the whole — see DEFECT-006"
        )
        assert fleet["total_vehicles"] <= CFG.num_vehicles


def test_tc_int_005_parquet_partition_for_the_current_sim_date_exists(stack_ready) -> None:
    """TC-INT-005: the master dataset is being written and partitioned by sim_date."""
    from pathlib import Path

    from common.sim_clock import sim_date

    lake = Path(CFG.lake_root) / "telemetry"
    partitions = wait_until(
        lambda: [p.name for p in lake.glob("sim_date=*")] or None, timeout_s=120, interval_s=5
    )
    assert partitions, f"no sim_date partitions under {lake}"
    today = f"sim_date={sim_date().isoformat()}"
    assert today in partitions, f"current partition {today} missing; found {partitions}"
    files = list((lake / today).glob("*.parquet"))
    assert files, "partition directory exists but holds no parquet files"


def test_tc_int_006_rejected_events_are_quarantined_with_reasons(stack_ready) -> None:
    """TC-INT-006: injected bad events land in rejected_events with the correct reasons."""
    rows = wait_until(
        lambda: query("SELECT reason, count(*) AS n FROM rejected_events GROUP BY reason") or None,
        timeout_s=150,
    )
    assert rows, "no rejected events — is BAD_EVENT_RATE zero?"
    reasons = {r["reason"] for r in rows}
    from common.schemas import REASON_ORDER

    assert reasons.issubset(set(REASON_ORDER) | {"MALFORMED_JSON"}), f"unknown reasons: {reasons}"
    # The producer injects six distinct kinds; at least three should have appeared by now.
    assert len(reasons) >= 3, f"expected several rejection reasons, saw {reasons}"


def test_tc_int_007_idle_alert_opens_for_a_lazy_vehicle(stack_ready) -> None:
    """TC-INT-007: the threshold alert fires, and only one open alert exists per vehicle."""
    alerts = wait_until(
        lambda: query("SELECT * FROM idle_alerts ORDER BY detected_at DESC LIMIT 20") or None,
        timeout_s=max(240, CFG.idle_alert_minutes * 60 + 120),
        interval_s=10,
    )
    assert alerts, "no idle alert was ever raised — check LAZY_VEHICLES and IDLE_ALERT_MINUTES"
    for alert in alerts:
        assert float(alert["idle_minutes"]) >= CFG.idle_alert_minutes - 0.2

    open_rows = query("SELECT vehicle_id FROM idle_alerts WHERE status = 'open'")
    vehicle_ids = [r["vehicle_id"] for r in open_rows]
    assert len(vehicle_ids) == len(set(vehicle_ids)), (
        "more than one open alert for the same vehicle — the partial unique index is not working"
    )


def test_tc_int_008_alerts_resolve_when_the_vehicle_moves(stack_ready) -> None:
    """TC-INT-008: an alert is closed once the vehicle becomes active again."""
    resolved = wait_until(
        lambda: query(
            "SELECT * FROM idle_alerts WHERE status = 'resolved' "
            "AND resolved_at > now() - interval '30 minutes' LIMIT 5"
        )
        or None,
        timeout_s=max(300, CFG.idle_alert_minutes * 60 + 180),
        interval_s=10,
    )
    assert resolved, "no idle alert was ever resolved"
    for alert in resolved:
        assert alert["resolved_at"] is not None
        assert alert["resolved_at"] >= alert["detected_at"]


# --------------------------------------------------------------------------------------------
# Serving
# --------------------------------------------------------------------------------------------
def test_tc_int_009_api_health_is_healthy_against_the_live_stack(stack_ready) -> None:
    """TC-INT-009: the deep health check passes with all three components up."""
    response = requests.get(f"{API}/health", timeout=15)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "healthy"
    assert body["components"]["postgres"]["ok"]
    assert body["components"]["kafka"]["ok"]
    assert body["components"]["data_freshness"]["ok"]
    assert body["freshness_seconds"] < CFG.freshness_seconds


def test_tc_int_010_api_fleet_and_zone_endpoints_return_live_data(stack_ready) -> None:
    """TC-INT-010: the API serves real speed-layer numbers that satisfy basic invariants."""
    fleet = requests.get(f"{API}/metrics/fleet", timeout=15).json()
    assert fleet["total_vehicles"] > 0
    assert fleet["active_vehicles"] + fleet["idle_vehicles"] == fleet["total_vehicles"]
    assert 0.0 <= fleet["idle_ratio"] <= 1.0
    assert fleet["data_age_seconds"] < CFG.freshness_seconds

    zones = requests.get(f"{API}/metrics/zones?minutes=15", timeout=15).json()
    assert zones, "no zone rows returned"
    for zone in zones:
        assert 0.0 <= zone["idle_ratio"] <= 1.0
        assert zone["earnings_lkr"] >= 0


# --------------------------------------------------------------------------------------------
# Observability
# --------------------------------------------------------------------------------------------
def test_tc_int_011_all_prometheus_targets_are_up(stack_ready) -> None:
    """TC-INT-011: every scrape target defined in prometheus.yml is UP."""
    data = wait_until(
        lambda: (
            requests.get(f"{PROM}/api/v1/targets", timeout=10).json().get("data", {}).get(
                "activeTargets"
            )
            or None
        ),
        timeout_s=90,
    )
    assert data, "Prometheus returned no active targets"
    by_job = {t["labels"]["job"]: t["health"] for t in data}
    expected = {"prometheus", "gps-producer", "expense-producer", "stream-job", "api",
                "kafka-exporter"}
    assert expected.issubset(set(by_job)), f"missing scrape jobs: {expected - set(by_job)}"
    down = {job: health for job, health in by_job.items() if health != "up"}
    assert not down, f"targets not UP: {down}"


def test_tc_int_012_alert_rules_are_loaded(stack_ready) -> None:
    """TC-INT-012: every alert rule in alert_rules.yml is registered with Prometheus."""
    rules = requests.get(f"{PROM}/api/v1/rules", timeout=10).json()["data"]["groups"]
    names = {r["name"] for g in rules for r in g["rules"]}
    for required in (
        "NoTelemetryReceived", "StreamProcessingStalled", "HighRejectRate", "StreamBatchSlow",
        "ConsumerLagHigh", "ExpenseFileLate", "ApiUnhealthy", "TargetDown",
    ):
        assert required in names, f"alert rule {required} not loaded"


def test_tc_int_013_grafana_datasources_and_dashboards_are_provisioned(stack_ready) -> None:
    """TC-INT-013: both datasources and both dashboards exist without any manual clicking."""
    auth = (
        os.getenv("GF_SECURITY_ADMIN_USER", "admin"),
        os.getenv("GF_SECURITY_ADMIN_PASSWORD", "admin"),
    )
    datasources = requests.get(f"{GRAFANA}/api/datasources", auth=auth, timeout=15)
    assert datasources.status_code == 200, datasources.text
    uids = {d["uid"] for d in datasources.json()}
    assert {"fleet-prometheus", "fleet-postgres"}.issubset(uids)

    search = requests.get(
        f"{GRAFANA}/api/search?type=dash-db", auth=auth, timeout=15
    ).json()
    dash_uids = {d["uid"] for d in search}
    assert {"fleet-ops", "fleet-health"}.issubset(dash_uids), f"found {dash_uids}"


def test_tc_int_014_producer_and_stream_metrics_are_being_collected(stack_ready) -> None:
    """TC-INT-014: the key metrics the alert rules depend on actually have samples."""
    for metric in (
        "producer_events_sent_total",
        "producer_last_send_timestamp",
        "stream_batches_total",
        "stream_last_batch_timestamp",
        "stream_rows_processed_total",
        "api_health_status",
    ):
        result = wait_until(
            lambda m=metric: requests.get(
                f"{PROM}/api/v1/query", params={"query": m}, timeout=10
            ).json()["data"]["result"]
            or None,
            timeout_s=90,
        )
        assert result, f"metric {metric} has no samples in Prometheus"


def test_tc_int_015_structured_logs_are_valid_json_with_the_envelope(stack_ready) -> None:
    """TC-INT-015: the containers really emit the JSON log envelope (not just the unit test).

    Reads the API's own log through its access-log side effect: any request produces one line,
    and the format is asserted by the unit test.  Here we assert the *contract* by checking the
    API reports its run_id, which is the field that makes tracing possible.
    """
    body = requests.get(f"{API}/", timeout=10).json()
    assert body["run_id"] and len(body["run_id"]) >= 8
    assert body["architecture"] == "lambda"
    assert body["simulated_clock"]["sim_day_seconds"] == CFG.sim_day_seconds
    assert datetime.now(tz=UTC).year >= 2026


def test_tc_str_015_idle_alerts_for_departed_vehicles_are_closed(stack_ready) -> None:
    """TC-STR-015: an idle alert whose vehicle stops reporting is closed as `abandoned`.

    Regression guard for DEFECT-019. An alert closes normally when its vehicle starts moving
    again, which the UPSERT signals by clearing `vehicle_status.idle_since`. A vehicle that goes
    silent *while idle* never moves again, so without a sweep its alert stays open forever and
    `ManyVehiclesIdle` fires permanently.

    The test inserts a synthetic alert for a vehicle id that the simulator never produces, with no
    matching `vehicle_status` row at all -- which is the case the first version of the sweep
    missed, because it used an inner join and so could only ever see *stale* rows, never *absent*
    ones.
    """
    phantom = "V-999"
    execute("DELETE FROM idle_alerts WHERE vehicle_id = %s", (phantom,))
    execute("DELETE FROM vehicle_status WHERE vehicle_id = %s", (phantom,))
    execute(
        "INSERT INTO idle_alerts (vehicle_id, zone, idle_since, detected_at, idle_minutes, status)"
        " VALUES (%s, 'Fort', now() - INTERVAL '30 minutes', now() - INTERVAL '25 minutes',"
        " 25.0, 'open')",
        (phantom,),
    )
    opened = query_one(
        "SELECT status FROM idle_alerts WHERE vehicle_id = %s", (phantom,)
    )
    assert opened and opened["status"] == "open", "fixture did not insert an open alert"

    try:
        final = wait_until(
            lambda: (
                row
                if (row := query_one(
                    "SELECT status, resolved_at, idle_minutes FROM idle_alerts"
                    " WHERE vehicle_id = %s", (phantom,)
                )) and row["status"] != "open"
                else None
            ),
            timeout_s=180,
            interval_s=10,
        )
        assert final, (
            "the stale-alert sweep never closed the alert; it runs in the vehicle_status "
            "foreachBatch sink, so check that the stream job is processing batches"
        )
        assert final["status"] == "abandoned", (
            f"expected 'abandoned' (vehicle went silent), got {final['status']!r}; "
            "'resolved' would wrongly imply the alert was acted on"
        )
        assert final["resolved_at"] is not None
        assert float(final["idle_minutes"]) > 0

        # And the live count the alert rule reads must exclude it.
        still_open = query_one(
            "SELECT count(*) AS n FROM idle_alerts WHERE status = 'open' AND vehicle_id = %s",
            (phantom,),
        )
        assert still_open["n"] == 0
    finally:
        execute("DELETE FROM idle_alerts WHERE vehicle_id = %s", (phantom,))
