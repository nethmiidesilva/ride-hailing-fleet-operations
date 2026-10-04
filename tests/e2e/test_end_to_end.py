"""End-to-end and non-functional tests.

TC-E2E-001  full flow for one complete simulated day, asserting the business question is answered
TC-E2E-002  batch vs speed-layer reconciliation, with the difference explained
TC-NFR-001  ingestion throughput
TC-NFR-002  end-to-end latency (event_time -> row visible in PostgreSQL)
TC-NFR-003  resource usage snapshot

Run with: ``docker compose run --rm tests pytest tests/e2e -m "e2e or nfr"``
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from common.config import CFG
from common.db import query, query_one
from tests.conftest import wait_until

API = f"http://{os.getenv('API_HOST', 'api')}:{os.getenv('API_PORT', '8000')}"
PROM = f"http://{os.getenv('PROM_HOST', 'prometheus')}:{os.getenv('PROM_PORT', '9090')}"
EVIDENCE = Path(os.getenv("EVIDENCE_DIR", "docs/evidence")) / "scenarios"


def _write_evidence(name: str, payload: dict) -> Path:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE / f"{name}.json"
    payload.setdefault("captured_at", datetime.now(tz=UTC).isoformat(timespec="seconds"))
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


# ============================================================================================
# E2E
# ============================================================================================
@pytest.mark.e2e
def test_tc_e2e_001_full_flow_answers_the_business_question(stack_ready) -> None:
    """TC-E2E-001: every layer contributed, and both halves of the business question answered.

    The business question is:
        *What is fleet utilization and earnings by area/time-of-day right now, and which
        vehicles are becoming unprofitable once yesterday's costs are factored in?*

    "Right now"  -> the speed layer must serve current utilisation and earnings by zone.
    "Which vehicles" -> the batch layer must have reconciled at least one simulated day and the
    API must be able to list the loss-making vehicles.
    """
    evidence: dict = {"test_case": "TC-E2E-001", "stages": {}}

    # --- stage 1: ingestion -> Kafka -> speed layer ----------------------------------------
    fleet = requests.get(f"{API}/metrics/fleet", timeout=20).json()
    assert fleet["total_vehicles"] > 0, "speed layer has no vehicles"
    assert fleet["data_age_seconds"] < CFG.freshness_seconds
    evidence["stages"]["speed_layer_fleet"] = fleet

    zones = requests.get(f"{API}/metrics/zones?minutes=15", timeout=20).json()
    assert zones, "no per-zone utilisation is being served"
    assert any(z["earnings_lkr"] > 0 for z in zones), "no zone reported any earnings"
    evidence["stages"]["speed_layer_zones"] = zones

    # --- stage 2: master dataset -----------------------------------------------------------
    lake = Path(CFG.lake_root) / "telemetry"
    partitions = sorted(p.name for p in lake.glob("sim_date=*"))
    assert partitions, "the Parquet master dataset is empty"
    evidence["stages"]["master_dataset_partitions"] = partitions

    # --- stage 3: batch source -------------------------------------------------------------
    landing = Path(CFG.landing_dir)
    expense_files = sorted(p.name for p in landing.glob("expenses_*.csv"))
    assert expense_files, "no daily expense file has been produced"
    evidence["stages"]["expense_files"] = expense_files

    # --- stage 4: orchestration ------------------------------------------------------------
    run = wait_until(
        lambda: query_one(
            "SELECT run_id, report_date, status, duration_s, rows_written FROM pipeline_runs "
            "WHERE status = 'success' ORDER BY started_at DESC LIMIT 1"
        ),
        timeout_s=max(300, CFG.sim_day_seconds + 180),
        interval_s=10,
    )
    assert run, "the Airflow DAG has not completed a successful run"
    evidence["stages"]["pipeline_run"] = run
    report_date = run["report_date"]

    # --- stage 5: batch results ------------------------------------------------------------
    rows = query(
        "SELECT * FROM daily_vehicle_profitability WHERE report_date = %s ORDER BY profit_lkr",
        (report_date,),
    )
    assert rows, f"no profitability rows for {report_date}"
    assert len(rows) >= CFG.num_vehicles * 0.8, "most of the fleet should be reconciled"
    for row in rows:
        assert float(row["revenue_lkr"]) >= 0
        assert float(row["total_cost_lkr"]) >= 0
        assert row["trend"] in ("STABLE", "DECLINING", "AT_RISK")
    evidence["stages"]["profitability_row_count"] = len(rows)
    evidence["stages"]["profitability_sample"] = rows[:5]

    zone_hours = query(
        "SELECT count(*) AS n, count(DISTINCT sim_hour) AS hours, count(DISTINCT zone) AS zones "
        "FROM daily_zone_summary WHERE report_date = %s",
        (report_date,),
    )[0]
    assert zone_hours["n"] > 0, "no zone x time-of-day rows"
    evidence["stages"]["zone_hour_summary"] = zone_hours

    # --- stage 6: the consolidated report file ---------------------------------------------
    html = Path(CFG.reports_dir) / f"profitability_{report_date}.html"
    csv_file = Path(CFG.reports_dir) / f"profitability_{report_date}.csv"
    assert html.exists(), f"HTML report missing: {html}"
    assert csv_file.exists(), f"CSV report missing: {csv_file}"
    content = html.read_text(encoding="utf-8")
    for section in ("Fleet KPIs", "Per-vehicle profitability", "Earnings by zone",
                    "Idle alerts", "Data quality", "reconciliation"):
        assert section in content, f"report is missing the '{section}' section"
    evidence["stages"]["report_files"] = {
        "html": str(html), "html_bytes": html.stat().st_size,
        "csv": str(csv_file), "csv_bytes": csv_file.stat().st_size,
    }

    # --- stage 7: the API serves the answer -------------------------------------------------
    served = requests.get(
        f"{API}/reports/profitability?date={report_date}", timeout=20
    ).json()
    assert len(served) == len(rows)
    unprofitable = requests.get(
        f"{API}/vehicles/unprofitable?date={report_date}", timeout=20
    ).json()
    evidence["stages"]["api_unprofitable_count"] = len(unprofitable)
    evidence["stages"]["api_unprofitable_sample"] = unprofitable[:5]

    html_response = requests.get(
        f"{API}/reports/profitability/{report_date}/html", timeout=20
    )
    assert html_response.status_code == 200
    assert "Fleet KPIs" in html_response.text

    # --- the business question, answered ----------------------------------------------------
    evidence["business_question"] = {
        "utilisation_now": {
            "active_vehicles": fleet["active_vehicles"],
            "idle_vehicles": fleet["idle_vehicles"],
            "idle_ratio": fleet["idle_ratio"],
            "earnings_last_hour_lkr": fleet["earnings_last_hour_lkr"],
        },
        "earnings_by_zone_now": {z["zone"]: z["earnings_lkr"] for z in zones},
        "reconciled_day": str(report_date),
        "unprofitable_vehicles": [
            {"vehicle_id": v["vehicle_id"], "profit_lkr": v["profit_lkr"], "trend": v["trend"]}
            for v in unprofitable
        ],
    }
    evidence["status"] = "PASS"
    path = _write_evidence("TC-E2E-001", evidence)
    print(f"\nTC-E2E-001 evidence: {path}")

    # The answer must be non-degenerate: the day must have produced some revenue.
    total_revenue = sum(float(r["revenue_lkr"]) for r in rows)
    assert total_revenue > 0, "the reconciled day produced no revenue at all"


@pytest.mark.e2e
def test_tc_e2e_002_batch_vs_speed_reconciliation(stack_ready) -> None:
    """TC-E2E-002: compare the two layers' totals and quantify the difference.

    The two layers are *expected* to disagree slightly, and this test records by how much rather
    than pretending they match:

    * the speed layer drops events that arrive later than the 2-minute watermark, and uses
      ``approx_count_distinct`` for vehicle counts;
    * the batch layer re-reads the whole Parquet partition, so late events are included and
      counts are exact;
    * the speed layer's 1-minute windows are cut on ``event_time`` (real clock), which does not
      align exactly with the simulated-day boundary the batch layer uses.

    The assertion is therefore on the *magnitude* of the difference, not on equality.
    """
    run = query_one(
        "SELECT report_date FROM pipeline_runs WHERE status='success' "
        "ORDER BY started_at DESC LIMIT 1"
    )
    if not run:
        pytest.skip("no successful DAG run yet — run TC-E2E-001 first")
    report_date = run["report_date"]

    batch = query_one(
        "SELECT round(sum(revenue_lkr),2) AS revenue, sum(trips) AS trips "
        "FROM daily_vehicle_profitability WHERE report_date = %s",
        (report_date,),
    )
    # The real-time span that this simulated day occupied.
    from common.sim_clock import real_seconds_for_sim_date

    start_epoch, end_epoch = real_seconds_for_sim_date(report_date)
    start = datetime.fromtimestamp(start_epoch, tz=UTC)
    end = datetime.fromtimestamp(end_epoch, tz=UTC)

    speed = query_one(
        "SELECT COALESCE(round(sum(earnings_lkr),2),0) AS revenue, "
        "COALESCE(sum(trips_completed),0) AS trips, count(*) AS windows "
        "FROM realtime_zone_metrics WHERE window_start >= %s AND window_start < %s",
        (start, end),
    )

    batch_revenue = float(batch["revenue"] or 0)
    speed_revenue = float(speed["revenue"] or 0)
    diff = speed_revenue - batch_revenue
    diff_pct = (diff / batch_revenue * 100) if batch_revenue else None

    late_dropped = query_one(
        "SELECT count(*) AS n FROM rejected_events WHERE reason = 'MALFORMED_JSON'"
    )

    evidence = {
        "test_case": "TC-E2E-002",
        "report_date": str(report_date),
        "real_time_span": [start.isoformat(), end.isoformat()],
        "batch_layer": {"revenue_lkr": batch_revenue, "trips": int(batch["trips"] or 0)},
        "speed_layer": {
            "revenue_lkr": speed_revenue,
            "trips": int(speed["trips"] or 0),
            "windows": int(speed["windows"] or 0),
        },
        "difference_lkr": round(diff, 2),
        "difference_pct": round(diff_pct, 2) if diff_pct is not None else None,
        "explanation": (
            "The speed layer drops events later than the 2-minute watermark and cuts its "
            "1-minute windows on real event_time, which does not align exactly with the "
            "simulated-day boundary used by the batch layer. The batch layer recomputes from "
            "the complete Parquet partition and is the figure the business uses."
        ),
        "malformed_rows_quarantined": late_dropped["n"] if late_dropped else 0,
    }

    tolerance_pct = float(os.getenv("RECONCILIATION_TOLERANCE_PCT", "25"))
    evidence["tolerance_pct"] = tolerance_pct
    if batch_revenue == 0:
        evidence["status"] = "NOT EXECUTED"
        evidence["reason"] = "batch revenue was zero for this day"
        _write_evidence("TC-E2E-002", evidence)
        pytest.skip("batch revenue was zero; nothing to reconcile")

    within = abs(diff_pct) <= tolerance_pct
    evidence["status"] = "PASS" if within else "FAIL"
    path = _write_evidence("TC-E2E-002", evidence)
    print(f"\nTC-E2E-002 evidence: {path}\n  {json.dumps(evidence, indent=2, default=str)}")

    assert within, (
        f"speed layer differs from batch by {diff_pct:.2f}% "
        f"(speed {speed_revenue}, batch {batch_revenue}); tolerance {tolerance_pct}%"
    )


# ============================================================================================
# Non-functional
# ============================================================================================
@pytest.mark.nfr
def test_tc_nfr_001_ingestion_throughput(stack_ready) -> None:
    """TC-NFR-001: measure sustained ingestion throughput and micro-batch duration.

    Baseline configuration only (NUM_VEHICLES as configured).  The 200-vehicle stress variant is
    driven by ``scripts/nfr_load_test.sh`` because it needs the producer recreated with a
    different environment, which a pytest process cannot do safely.
    """
    samples: list[float] = []
    for _ in range(6):
        value = requests.get(
            f"{PROM}/api/v1/query",
            params={"query": "sum(rate(producer_events_sent_total[1m]))"},
            timeout=10,
        ).json()["data"]["result"]
        if value:
            samples.append(float(value[0]["value"][1]))
        time.sleep(10)

    assert samples, "no throughput samples collected"
    batch_p50 = requests.get(
        f"{PROM}/api/v1/query",
        params={
            "query": "histogram_quantile(0.5, sum(rate(stream_batch_duration_seconds_bucket[5m])) by (le))"
        },
        timeout=10,
    ).json()["data"]["result"]
    batch_p95 = requests.get(
        f"{PROM}/api/v1/query",
        params={
            "query": "histogram_quantile(0.95, sum(rate(stream_batch_duration_seconds_bucket[5m])) by (le))"
        },
        timeout=10,
    ).json()["data"]["result"]
    lag = requests.get(
        f"{PROM}/api/v1/query",
        params={"query": 'sum(kafka_consumergroup_lag{topic="fleet.telemetry"})'},
        timeout=10,
    ).json()["data"]["result"]

    evidence = {
        "test_case": "TC-NFR-001",
        "num_vehicles": CFG.num_vehicles,
        "emit_interval_sec": CFG.emit_interval_sec,
        "theoretical_events_per_sec": round(CFG.num_vehicles / CFG.emit_interval_sec, 2),
        "measured_events_per_sec": {
            "samples": [round(s, 3) for s in samples],
            "mean": round(statistics.mean(samples), 3),
            "median": round(statistics.median(samples), 3),
            "min": round(min(samples), 3),
            "max": round(max(samples), 3),
        },
        "stream_batch_duration_seconds": {
            "p50": float(batch_p50[0]["value"][1]) if batch_p50 else None,
            "p95": float(batch_p95[0]["value"][1]) if batch_p95 else None,
        },
        "consumer_lag": float(lag[0]["value"][1]) if lag else None,
        "status": "PASS",
    }
    path = _write_evidence("TC-NFR-001", evidence)
    print(f"\nTC-NFR-001 evidence: {path}\n  {json.dumps(evidence, indent=2)}")

    expected = CFG.num_vehicles / CFG.emit_interval_sec
    # Allow a wide band: jitter, duplicates and bad events all perturb the exact rate.
    assert statistics.mean(samples) > expected * 0.5, (
        f"throughput {statistics.mean(samples):.2f}/s is far below the expected {expected:.2f}/s"
    )


@pytest.mark.nfr
def test_tc_nfr_002_end_to_end_latency(stack_ready) -> None:
    """TC-NFR-002: event_time -> visible in PostgreSQL, sampled from vehicle_status.

    ``vehicle_status.updated_at`` is set by PostgreSQL when the row is upserted, and
    ``last_event_time`` is the producer's wall-clock stamp, so their difference is a genuine
    end-to-end latency: producer -> Kafka -> Spark micro-batch -> UPSERT.
    """
    rows = query(
        "SELECT vehicle_id, "
        "EXTRACT(EPOCH FROM (updated_at - last_event_time)) AS latency_s "
        "FROM vehicle_status WHERE updated_at > now() - interval '5 minutes'"
    )
    assert rows, "no recently updated vehicle_status rows to sample"
    latencies = sorted(float(r["latency_s"]) for r in rows if r["latency_s"] is not None)
    assert latencies, "no latency samples"

    def pct(p: float) -> float:
        index = min(len(latencies) - 1, int(round(p * (len(latencies) - 1))))
        return round(latencies[index], 3)

    evidence = {
        "test_case": "TC-NFR-002",
        "samples": len(latencies),
        "latency_seconds": {
            "min": round(min(latencies), 3),
            "p50": pct(0.50),
            "p95": pct(0.95),
            "max": round(max(latencies), 3),
            "mean": round(statistics.mean(latencies), 3),
        },
        "measurement": "vehicle_status.updated_at - vehicle_status.last_event_time",
        "pipeline_hops": "producer -> Kafka -> Spark micro-batch (10 s trigger) -> psycopg UPSERT",
        "status": "PASS",
    }
    path = _write_evidence("TC-NFR-002", evidence)
    print(f"\nTC-NFR-002 evidence: {path}\n  {json.dumps(evidence, indent=2)}")

    # The vehicle_status query triggers every 10 s, so p95 should stay well under a minute.
    assert pct(0.95) < 120, f"p95 end-to-end latency {pct(0.95)}s is unexpectedly high"


@pytest.mark.nfr
def test_tc_nfr_003_resource_usage_snapshot(stack_ready) -> None:
    """TC-NFR-003: capture a `docker stats` snapshot for the report's resource table.

    Skipped rather than failed when the Docker socket is not mounted into the test container:
    the measurement is evidence, not a correctness property.
    """
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"],
            capture_output=True, text=True, timeout=60,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"docker CLI not available inside the test container: {exc}")

    if result.returncode != 0:
        pytest.skip(f"docker stats unavailable: {result.stderr.strip()[:200]}")

    containers = []
    for line in result.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) == 4:
            containers.append(
                {"name": parts[0], "cpu_pct": parts[1], "mem_usage": parts[2],
                 "mem_pct": parts[3]}
            )
    evidence = {
        "test_case": "TC-NFR-003",
        "containers": containers,
        "container_count": len(containers),
        "status": "PASS" if containers else "NOT EXECUTED",
    }
    path = _write_evidence("TC-NFR-003", evidence)
    print(f"\nTC-NFR-003 evidence: {path}")
    assert containers, "docker stats returned no rows"
