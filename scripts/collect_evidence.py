"""Capture real system output into ``docs/evidence/`` for the report and test report.

Ground rule 2 of the brief is that no number may be fabricated.  This script is how that rule is
enforced in practice: every table, JSON body and metric quoted in ``docs/REPORT.md`` and
``docs/TEST_REPORT.md`` is copied from a file this script wrote, and each file records the exact
time it was captured.

Outputs
-------
``docs/evidence/api/<endpoint>.json``     raw API responses
``docs/evidence/sql/<query>.txt``         psql-style tables
``docs/evidence/sql/<query>.json``        the same rows, machine readable
``docs/evidence/metrics/*.json``          Prometheus query results and alert state
``docs/evidence/manifest.json``           index with capture timestamps and row counts
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from common.config import CFG
from common.db import query

API = f"http://{os.getenv('API_HOST', 'api')}:{os.getenv('API_PORT', '8000')}"
PROM = f"http://{os.getenv('PROM_HOST', 'prometheus')}:{os.getenv('PROM_PORT', '9090')}"
EVIDENCE = Path(os.getenv("EVIDENCE_DIR", "docs/evidence"))

# --------------------------------------------------------------------------------------------
# What to capture
# --------------------------------------------------------------------------------------------
API_ENDPOINTS: list[tuple[str, str]] = [
    ("index", "/"),
    ("health", "/health"),
    ("metrics_fleet", "/metrics/fleet"),
    ("metrics_zones", "/metrics/zones?minutes=15"),
    ("metrics_time_of_day", "/metrics/time-of-day"),
    ("alerts_idle_open", "/alerts/idle?status=open"),
    ("alerts_idle_all", "/alerts/idle?status=all&limit=20"),
    ("reports_profitability", "/reports/profitability"),
    ("vehicles_unprofitable", "/vehicles/unprofitable"),
    ("reports_list", "/reports"),
]

SQL_QUERIES: dict[str, str] = {
    "fleet_now": "SELECT * FROM v_fleet_now;",
    "zone_metrics_recent": (
        "SELECT window_start, zone, active_vehicles, idle_vehicles, idle_ratio, trips_completed, "
        "earnings_lkr, avg_speed_kmh, event_count FROM realtime_zone_metrics "
        "ORDER BY window_start DESC, zone LIMIT 24;"
    ),
    "vehicle_status": (
        "SELECT vehicle_id, driver_id, status, zone, speed_kmh, last_event_time, idle_since "
        "FROM vehicle_status ORDER BY vehicle_id LIMIT 30;"
    ),
    "idle_alerts": (
        "SELECT id, vehicle_id, zone, idle_since, detected_at, resolved_at, idle_minutes, status "
        "FROM idle_alerts ORDER BY detected_at DESC LIMIT 20;"
    ),
    "rejected_events_by_reason": (
        "SELECT reason, count(*) AS rows_rejected, min(rejected_at) AS first_seen, "
        "max(rejected_at) AS last_seen FROM rejected_events GROUP BY reason "
        "ORDER BY rows_rejected DESC;"
    ),
    "rejected_events_sample": (
        "SELECT event_id, vehicle_id, reason, kafka_partition, kafka_offset, "
        "left(raw_payload, 120) AS raw_payload_head FROM rejected_events "
        "ORDER BY rejected_at DESC LIMIT 10;"
    ),
    "daily_expenses": (
        "SELECT report_date, count(*) AS rows, round(sum(fuel_cost),2) AS fuel, "
        "round(sum(maintenance_cost),2) AS maintenance, round(sum(distance_covered),2) AS km "
        "FROM daily_expenses GROUP BY report_date ORDER BY report_date;"
    ),
    "rejected_expenses": (
        "SELECT report_date, reason, count(*) AS rows FROM rejected_expenses "
        "GROUP BY report_date, reason ORDER BY report_date DESC;"
    ),
    "profitability": (
        "SELECT report_date, vehicle_id, trips, revenue_lkr, total_cost_lkr, profit_lkr, margin, "
        "utilization, cost_per_km, is_unprofitable, trend, data_quality_flag "
        "FROM daily_vehicle_profitability ORDER BY report_date DESC, profit_lkr ASC LIMIT 40;"
    ),
    "profitability_summary": (
        "SELECT report_date, count(*) AS vehicles, sum(trips) AS trips, "
        "round(sum(revenue_lkr),2) AS revenue, round(sum(total_cost_lkr),2) AS cost, "
        "round(sum(profit_lkr),2) AS profit, "
        "count(*) FILTER (WHERE is_unprofitable) AS unprofitable, "
        "count(*) FILTER (WHERE trend <> 'STABLE') AS at_risk "
        "FROM daily_vehicle_profitability GROUP BY report_date ORDER BY report_date;"
    ),
    "zone_summary": (
        "SELECT report_date, zone, sum(trips) AS trips, round(sum(earnings_lkr),2) AS earnings, "
        "round(avg(utilization),4) AS avg_utilization FROM daily_zone_summary "
        "GROUP BY report_date, zone ORDER BY report_date DESC, earnings DESC;"
    ),
    "zone_hour_matrix": (
        "SELECT sim_hour, zone, round(sum(earnings_lkr),2) AS earnings FROM daily_zone_summary "
        "WHERE report_date = (SELECT max(report_date) FROM daily_zone_summary) "
        "GROUP BY sim_hour, zone ORDER BY sim_hour, zone;"
    ),
    "pipeline_runs": (
        "SELECT run_id, report_date, status, started_at, finished_at, duration_s, rows_written "
        "FROM pipeline_runs ORDER BY started_at DESC LIMIT 10;"
    ),
    "pipeline_alerts": (
        "SELECT created_at, alert_name, severity, dag_id, task_id, message FROM pipeline_alerts "
        "ORDER BY created_at DESC LIMIT 10;"
    ),
    "reconciliation": (
        "SELECT p.report_date, round(sum(p.revenue_lkr),2) AS batch_revenue_lkr, "
        "(SELECT round(sum(earnings_lkr),2) FROM realtime_zone_metrics) AS speed_revenue_lkr, "
        "sum(p.trips) AS batch_trips, "
        "(SELECT sum(trips_completed) FROM realtime_zone_metrics) AS speed_trips "
        "FROM daily_vehicle_profitability p GROUP BY p.report_date ORDER BY p.report_date;"
    ),
    "table_row_counts": (
        "SELECT 'realtime_zone_metrics' AS table_name, count(*) AS rows FROM realtime_zone_metrics "
        "UNION ALL SELECT 'vehicle_status', count(*) FROM vehicle_status "
        "UNION ALL SELECT 'idle_alerts', count(*) FROM idle_alerts "
        "UNION ALL SELECT 'rejected_events', count(*) FROM rejected_events "
        "UNION ALL SELECT 'daily_expenses', count(*) FROM daily_expenses "
        "UNION ALL SELECT 'rejected_expenses', count(*) FROM rejected_expenses "
        "UNION ALL SELECT 'daily_vehicle_profitability', count(*) FROM daily_vehicle_profitability "
        "UNION ALL SELECT 'daily_zone_summary', count(*) FROM daily_zone_summary "
        "UNION ALL SELECT 'pipeline_runs', count(*) FROM pipeline_runs "
        "UNION ALL SELECT 'pipeline_alerts', count(*) FROM pipeline_alerts ORDER BY 1;"
    ),
}

PROM_QUERIES: dict[str, str] = {
    "producer_rate": "sum(rate(producer_events_sent_total[1m]))",
    "producer_rate_by_status": "sum by (status) (rate(producer_events_sent_total[1m]))",
    "producer_last_send_age": "time() - producer_last_send_timestamp",
    "bad_events_injected": "sum by (kind) (producer_bad_events_injected_total)",
    "stream_batches": "sum by (sink) (stream_batches_total)",
    "stream_rows_processed": "sum by (sink) (stream_rows_processed_total)",
    "stream_rows_rejected": "sum by (reason) (stream_rows_rejected_total)",
    "stream_batch_p50": "histogram_quantile(0.5, sum(rate(stream_batch_duration_seconds_bucket[5m])) by (le, sink))",
    "stream_batch_p95": "histogram_quantile(0.95, sum(rate(stream_batch_duration_seconds_bucket[5m])) by (le, sink))",
    "stream_last_batch_age": "time() - stream_last_batch_timestamp",
    "reject_ratio": (
        "sum(rate(stream_rows_rejected_total[5m])) / clamp_min(sum(rate(stream_rows_processed_total[5m]))"
        " + sum(rate(stream_rows_rejected_total[5m])), 0.001)"
    ),
    "consumer_lag": 'sum(kafka_consumergroup_lag{topic="fleet.telemetry"})',
    "idle_alerts_open": "idle_alerts_open",
    "api_health": "api_health_status",
    "api_request_rate": "sum by (path) (rate(api_requests_total[5m]))",
    "expense_files": "expense_files_written_total",
    "targets_up": "sum by (job) (up)",
}


# --------------------------------------------------------------------------------------------
def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _table(rows: list[dict[str, Any]]) -> str:
    """Render rows as a fixed-width table, the way psql would."""
    if not rows:
        return "(0 rows)\n"
    columns = list(rows[0])
    widths = {
        c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in columns
    }
    header = " | ".join(c.ljust(widths[c]) for c in columns)
    sep = "-+-".join("-" * widths[c] for c in columns)
    body = "\n".join(
        " | ".join(str(r.get(c, "")).ljust(widths[c]) for c in columns) for r in rows
    )
    return f"{header}\n{sep}\n{body}\n({len(rows)} rows)\n"


def capture_api(manifest: dict[str, Any]) -> None:
    for name, path in API_ENDPOINTS:
        target = EVIDENCE / "api" / f"{name}.json"
        try:
            response = requests.get(f"{API}{path}", timeout=20)
            payload = {
                "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
                "request": f"GET {path}",
                "status_code": response.status_code,
                "body": response.json(),
            }
        except Exception as exc:  # noqa: BLE001
            payload = {
                "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
                "request": f"GET {path}",
                "error": f"{type(exc).__name__}: {exc}",
            }
        _write(target, json.dumps(payload, indent=2, default=str))
        manifest["api"][name] = {
            "path": str(target),
            "request": payload["request"],
            "status": payload.get("status_code", "ERROR"),
        }
        print(f"  api  {name:26s} -> {target}")


def capture_sql(manifest: dict[str, Any]) -> None:
    for name, sql in SQL_QUERIES.items():
        try:
            rows = query(sql)
            text = f"-- {sql}\n-- captured {datetime.now(tz=UTC).isoformat()}\n\n{_table(rows)}"
            _write(EVIDENCE / "sql" / f"{name}.txt", text)
            _write(
                EVIDENCE / "sql" / f"{name}.json",
                json.dumps(
                    {
                        "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
                        "sql": sql,
                        "row_count": len(rows),
                        "rows": rows,
                    },
                    indent=2,
                    default=str,
                ),
            )
            manifest["sql"][name] = {"rows": len(rows), "sql": sql}
            print(f"  sql  {name:26s} -> {len(rows)} rows")
        except Exception as exc:  # noqa: BLE001
            _write(EVIDENCE / "sql" / f"{name}.txt", f"-- {sql}\nERROR: {exc}\n")
            manifest["sql"][name] = {"error": str(exc)}
            print(f"  sql  {name:26s} -> ERROR {exc}")


def capture_metrics(manifest: dict[str, Any]) -> None:
    results: dict[str, Any] = {}
    for name, expr in PROM_QUERIES.items():
        try:
            response = requests.get(
                f"{PROM}/api/v1/query", params={"query": expr}, timeout=15
            ).json()
            results[name] = {"expr": expr, "result": response["data"]["result"]}
        except Exception as exc:  # noqa: BLE001
            results[name] = {"expr": expr, "error": str(exc)}
    _write(
        EVIDENCE / "metrics" / "prometheus_queries.json",
        json.dumps(
            {
                "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
                "queries": results,
            },
            indent=2,
        ),
    )
    manifest["metrics"]["prometheus_queries"] = {"queries": len(results)}
    print(f"  prom {'queries':26s} -> {len(results)} expressions")

    for name, endpoint in (("alerts", "/api/v1/alerts"), ("targets", "/api/v1/targets"),
                           ("rules", "/api/v1/rules")):
        try:
            data = requests.get(f"{PROM}{endpoint}", timeout=15).json()["data"]
        except Exception as exc:  # noqa: BLE001
            data = {"error": str(exc)}
        _write(
            EVIDENCE / "metrics" / f"prometheus_{name}.json",
            json.dumps(
                {
                    "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
                    "endpoint": endpoint,
                    "data": data,
                },
                indent=2,
            ),
        )
        manifest["metrics"][f"prometheus_{name}"] = {"endpoint": endpoint}
        print(f"  prom {name:26s} -> captured")


def capture_environment(manifest: dict[str, Any]) -> None:
    """Record the exact environment so the report's 'Environment' table is reproducible."""
    info: dict[str, Any] = {
        "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "config": CFG.as_dict(),
        "python": sys.version,
    }
    for label, cmd in (("docker_version", ["docker", "--version"]),
                       ("compose_version", ["docker", "compose", "version"])):
        try:
            info[label] = subprocess.run(
                cmd, capture_output=True, text=True, timeout=20
            ).stdout.strip()
        except Exception as exc:  # noqa: BLE001
            info[label] = f"unavailable: {exc}"
    try:
        import pyspark

        info["pyspark_version"] = pyspark.__version__
    except Exception:  # noqa: BLE001
        info["pyspark_version"] = "not installed in this container"

    _write(EVIDENCE / "environment.json", json.dumps(info, indent=2, default=str))
    manifest["environment"] = {"path": str(EVIDENCE / "environment.json")}
    print("  env  environment                -> captured")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Capture real evidence for the reports.")
    parser.add_argument("--skip-api", action="store_true")
    parser.add_argument("--skip-metrics", action="store_true")
    args = parser.parse_args(argv)

    manifest: dict[str, Any] = {
        "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "api": {},
        "sql": {},
        "metrics": {},
    }
    print(f"\ncollecting evidence into {EVIDENCE.resolve()}\n")
    if not args.skip_api:
        capture_api(manifest)
    capture_sql(manifest)
    if not args.skip_metrics:
        capture_metrics(manifest)
    capture_environment(manifest)

    _write(EVIDENCE / "manifest.json", json.dumps(manifest, indent=2, default=str))
    print(f"\nmanifest written to {EVIDENCE / 'manifest.json'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
