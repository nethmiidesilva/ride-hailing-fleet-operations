"""One-shot end-to-end verification that the whole pipeline is alive and producing data.

Written as a script (not a test) so it can be run during a demo, from the Makefile, or from a
terminal while someone watches.  It prints a numbered checklist with real numbers and exits
non-zero if any critical check fails, which makes it usable as a smoke test in CI.

Usage:  ``python scripts/e2e_check.py [--json]``
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from common.config import CFG
from common.db import query_one
from common.sim_clock import previous_sim_date, sim_date

API = f"http://{os.getenv('API_HOST', 'api')}:{os.getenv('API_PORT', '8000')}"
PROM = f"http://{os.getenv('PROM_HOST', 'prometheus')}:{os.getenv('PROM_PORT', '9090')}"

GREEN, RED, YELLOW, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[0m"


class Check:
    """One named verification with a pass/fail result and the real value observed."""

    def __init__(self, name: str, critical: bool = True) -> None:
        self.name = name
        self.critical = critical
        self.ok = False
        self.detail = ""

    def run(self, fn: Callable[[], tuple[bool, str]]) -> Check:
        try:
            self.ok, self.detail = fn()
        except Exception as exc:  # noqa: BLE001 - a failing check must not stop the others
            self.ok, self.detail = False, f"{type(exc).__name__}: {exc}"
        return self

    def as_dict(self) -> dict[str, Any]:
        return {
            "check": self.name,
            "status": "PASS" if self.ok else ("FAIL" if self.critical else "WARN"),
            "detail": self.detail,
            "critical": self.critical,
        }


def _prom_value(expr: str) -> float | None:
    response = requests.get(f"{PROM}/api/v1/query", params={"query": expr}, timeout=10)
    result = response.json()["data"]["result"]
    return float(result[0]["value"][1]) if result else None


def run_checks() -> list[Check]:
    checks: list[Check] = []

    # --- 1. ingestion -----------------------------------------------------------------------
    def producer_alive() -> tuple[bool, str]:
        age = _prom_value("time() - producer_last_send_timestamp")
        return (age is not None and age < 60), f"last telemetry send {age:.1f}s ago" if age else "no samples"

    checks.append(Check("Ingestion: GPS producer is sending").run(producer_alive))

    def events_rate() -> tuple[bool, str]:
        rate = _prom_value("sum(rate(producer_events_sent_total[1m]))")
        return (rate is not None and rate > 0), f"{rate:.2f} events/s" if rate else "no rate"

    checks.append(Check("Ingestion: events/s above zero").run(events_rate))

    # --- 2. processing ----------------------------------------------------------------------
    def stream_alive() -> tuple[bool, str]:
        age = _prom_value("time() - stream_last_batch_timestamp")
        return (age is not None and age < 120), f"last micro-batch {age:.1f}s ago" if age else "no samples"

    checks.append(Check("Processing: Spark micro-batches completing").run(stream_alive))

    def windows_written() -> tuple[bool, str]:
        row = query_one(
            "SELECT count(*) AS n, max(window_start) AS latest FROM realtime_zone_metrics "
            "WHERE window_start > now() - interval '10 minutes'"
        )
        return bool(row and row["n"]), f"{row['n']} windows, newest {row['latest']}"

    checks.append(Check("Processing: realtime_zone_metrics being written").run(windows_written))

    def quarantine_works() -> tuple[bool, str]:
        row = query_one("SELECT count(*) AS n, count(DISTINCT reason) AS r FROM rejected_events")
        return bool(row and row["n"]), f"{row['n']} rejected rows across {row['r']} reasons"

    checks.append(Check("Processing: bad events quarantined").run(quarantine_works))

    # --- 3. master dataset ------------------------------------------------------------------
    def lake_written() -> tuple[bool, str]:
        lake = Path(CFG.lake_root) / "telemetry"
        parts = sorted(p.name for p in lake.glob("sim_date=*")) if lake.exists() else []
        current = f"sim_date={sim_date().isoformat()}"
        return (current in parts), f"partitions: {parts}"

    checks.append(Check("Storage: Parquet master dataset partitioned by sim_date").run(lake_written))

    # --- 4. batch layer ---------------------------------------------------------------------
    def expenses_present() -> tuple[bool, str]:
        landing = Path(CFG.landing_dir)
        files = sorted(p.name for p in landing.glob("expenses_*.csv")) if landing.exists() else []
        return bool(files), f"{len(files)} file(s): {files[-3:]}"

    checks.append(Check("Ingestion: daily expense files landing").run(expenses_present))

    def profitability_present() -> tuple[bool, str]:
        row = query_one(
            "SELECT count(*) AS n, max(report_date) AS d, "
            "count(*) FILTER (WHERE is_unprofitable) AS bad "
            "FROM daily_vehicle_profitability"
        )
        return bool(row and row["n"]), (
            f"{row['n']} rows, latest day {row['d']}, {row['bad']} unprofitable"
        )

    checks.append(
        Check("Batch: daily_vehicle_profitability populated", critical=False).run(
            profitability_present
        )
    )

    def dag_ran() -> tuple[bool, str]:
        row = query_one(
            "SELECT count(*) AS n, max(report_date) AS d FROM pipeline_runs WHERE status='success'"
        )
        return bool(row and row["n"]), f"{row['n']} successful DAG run(s), latest day {row['d']}"

    checks.append(Check("Orchestration: Airflow DAG completed", critical=False).run(dag_ran))

    # --- 5. serving -------------------------------------------------------------------------
    def api_healthy() -> tuple[bool, str]:
        response = requests.get(f"{API}/health", timeout=15)
        body = response.json()
        return response.status_code == 200, f"{body['status']}, data {body.get('freshness_seconds')}s old"

    checks.append(Check("Serving: /health returns 200").run(api_healthy))

    def api_answers_the_question() -> tuple[bool, str]:
        fleet = requests.get(f"{API}/metrics/fleet", timeout=15).json()
        return fleet["total_vehicles"] > 0, (
            f"{fleet['active_vehicles']} active / {fleet['idle_vehicles']} idle, "
            f"idle_ratio {fleet['idle_ratio']}, {fleet['trips_last_hour']} trips last hour, "
            f"{fleet['earnings_last_hour_lkr']} LKR"
        )

    checks.append(Check("Serving: /metrics/fleet answers 'utilisation now'").run(api_answers_the_question))

    # --- 6. observability -------------------------------------------------------------------
    def targets_up() -> tuple[bool, str]:
        targets = requests.get(f"{PROM}/api/v1/targets", timeout=10).json()["data"]["activeTargets"]
        down = [t["labels"]["job"] for t in targets if t["health"] != "up"]
        return not down, f"{len(targets)} targets, down: {down or 'none'}"

    checks.append(Check("Observability: all Prometheus targets UP").run(targets_up))

    def rules_loaded() -> tuple[bool, str]:
        groups = requests.get(f"{PROM}/api/v1/rules", timeout=10).json()["data"]["groups"]
        names = [r["name"] for g in groups for r in g["rules"]]
        return len(names) >= 8, f"{len(names)} alert rules loaded"

    checks.append(Check("Observability: alert rules loaded").run(rules_loaded))

    def alerts_state() -> tuple[bool, str]:
        alerts = requests.get(f"{PROM}/api/v1/alerts", timeout=10).json()["data"]["alerts"]
        firing = [a["labels"]["alertname"] for a in alerts if a["state"] == "firing"]
        return True, f"firing: {firing or 'none (healthy)'}"

    checks.append(Check("Observability: current alert state", critical=False).run(alerts_state))

    return checks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="End-to-end pipeline verification.")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument("--out", default=None, help="also write the JSON result to this path")
    args = parser.parse_args(argv)

    checks = run_checks()
    payload = {
        "checked_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "sim_date": sim_date().isoformat(),
        "previous_sim_date": previous_sim_date().isoformat(),
        "checks": [c.as_dict() for c in checks],
        "passed": sum(1 for c in checks if c.ok),
        "total": len(checks),
        "critical_failures": [c.name for c in checks if not c.ok and c.critical],
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"\n  fleet-lambda end-to-end check  —  {payload['checked_at']}")
        print(f"  simulated day {payload['sim_date']}\n")
        for i, check in enumerate(checks, 1):
            mark = f"{GREEN}PASS{RESET}" if check.ok else (
                f"{RED}FAIL{RESET}" if check.critical else f"{YELLOW}WARN{RESET}"
            )
            print(f"  {i:2d}. [{mark}] {check.name}")
            print(f"        {check.detail}")
        print(f"\n  {payload['passed']}/{payload['total']} checks passed")
        if payload["critical_failures"]:
            print(f"  {RED}critical failures: {payload['critical_failures']}{RESET}")
        print()

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return 1 if payload["critical_failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
