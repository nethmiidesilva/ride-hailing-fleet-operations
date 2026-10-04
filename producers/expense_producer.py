"""Daily-batch source: writes one expense CSV per simulated day (ingestion, 15 marks).

Why this source exists
----------------------
The brief requires *two* sources with genuinely different cadences, and the architecture
decision hinges on it: a file that lands once a day is naturally a batch input, and forcing it
through a stream (the Kappa alternative) buys nothing.  It is also the join partner that turns
raw telemetry into the answer to the business question — revenue alone cannot tell you which
vehicle is unprofitable.

Atomicity
---------
The file is written to ``.expenses_<D>.csv.tmp`` and then ``os.replace``d onto its final name.
``os.replace`` is atomic within a filesystem, so the Airflow ``FileSensor`` can never observe a
half-written file.  Getting this wrong is one of the classic production data-pipeline bugs, so it
is called out explicitly in the report and covered by TC-ING-045.

Deliberate imperfection
-----------------------
About ``EXPENSE_HIGH_COST_PCT`` of vehicles are "high cost" (a big maintenance bill and
``service_flag = 1``), which is what makes some vehicles unprofitable and gives the daily report
something to say.  ``EXPENSE_DIRTY_ROWS`` rows per file are deliberately invalid so the Airflow
validation task has real work to do and ``rejected_expenses`` is never empty.

Modes
-----
* default — run forever, writing one file at the end of every simulated day.
* ``--backfill START END`` — write files for a past date range and exit (replay demos).
* ``--once [DATE]`` — write a single file and exit (used by the e2e tests).
* ``SKIP_DAY=YYYY-MM-DD`` — never write that day's file, to exercise the sensor-timeout path.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import signal
import sys
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from common.config import CFG
from common.logging_setup import RUN_ID, setup_logging
from common.metrics import (
    EXPENSE_FILES_WRITTEN,
    EXPENSE_LAST_FILE,
    EXPENSE_REGISTRY,
    EXPENSE_ROWS_WRITTEN,
    serve_metrics,
)
from common.schemas import EXPENSE_FIELDS, expense_reject_reason
from common.sim_clock import get_epoch, seconds_until_next_sim_day, sim_date

LOG = setup_logging("expense-producer", "ingestion")

_SHUTDOWN = False


def _handle_signal(signum: int, _frame: Any) -> None:
    global _SHUTDOWN
    _SHUTDOWN = True
    LOG.info(
        "shutdown requested",
        extra={"event": "shutdown_signal", "signal": signal.Signals(signum).name},
    )


def vehicle_ids(num_vehicles: int = CFG.num_vehicles) -> list[str]:
    """Vehicle ids must match the GPS producer exactly or the profitability join breaks.

    Both derive them from ``NUM_VEHICLES`` with the same ``V-%03d`` pattern; TC-ING-046 asserts
    the two sets are identical, which is the cheapest possible guard against a silent join miss.
    """
    return [f"V-{i + 1:03d}" for i in range(num_vehicles)]


def build_rows(report_date: date, seed: int | None = None) -> list[dict[str, Any]]:
    """Generate one expense row per vehicle for ``report_date``.

    The RNG is seeded with ``SEED`` **plus the day number**, so:

    * the same day always regenerates byte-identical values (needed by the replay test), and
    * different days differ (needed for the 2-day DECLINING trend rule to be meaningful).
    """
    day_offset = (report_date - datetime.strptime(CFG.sim_start_date, "%Y-%m-%d").date()).days
    rng = random.Random((CFG.seed if seed is None else seed) + day_offset * 7919)

    ids = vehicle_ids()
    # Deterministic choice of which vehicles are expensive this day.
    high_cost_count = max(1, round(len(ids) * CFG.expense_high_cost_pct))
    high_cost = set(rng.sample(ids, high_cost_count))

    rows: list[dict[str, Any]] = []
    for vehicle_id in ids:
        distance = rng.uniform(CFG.expense_distance_min_km, CFG.expense_distance_max_km)
        fuel = distance * CFG.expense_fuel_per_km_lkr * rng.uniform(0.9, 1.15)
        if vehicle_id in high_cost:
            maintenance = rng.uniform(CFG.expense_maint_high_min, CFG.expense_maint_high_max)
            service_flag = 1
        else:
            maintenance = rng.uniform(CFG.expense_maint_normal_min, CFG.expense_maint_normal_max)
            service_flag = 0
        rows.append(
            {
                "vehicle_id": vehicle_id,
                "fuel_cost": f"{fuel:.2f}",
                "maintenance_cost": f"{maintenance:.2f}",
                "distance_covered": f"{distance:.2f}",
                "service_flag": str(service_flag),
                "report_date": report_date.isoformat(),
            }
        )

    rows.extend(_dirty_rows(report_date, rng))
    return rows


def _dirty_rows(report_date: date, rng: random.Random) -> list[dict[str, Any]]:
    """Deliberately invalid rows, one of each supported defect.

    Each maps to a distinct reason in ``common.schemas.expense_reject_reason`` so the Airflow
    validation task can be shown catching different classes of problem, not just one.
    """
    defects: list[dict[str, Any]] = [
        {  # missing vehicle_id -> MISSING_VEHICLE_ID
            "vehicle_id": "",
            "fuel_cost": f"{rng.uniform(100, 400):.2f}",
            "maintenance_cost": "120.00",
            "distance_covered": "5.10",
            "service_flag": "0",
            "report_date": report_date.isoformat(),
        },
        {  # negative cost -> NEGATIVE_VALUE
            "vehicle_id": "V-999",
            "fuel_cost": "-180.00",
            "maintenance_cost": "95.00",
            "distance_covered": "4.00",
            "service_flag": "0",
            "report_date": report_date.isoformat(),
        },
        {  # non-numeric -> NON_NUMERIC_VALUE
            "vehicle_id": "V-998",
            "fuel_cost": "N/A",
            "maintenance_cost": "110.00",
            "distance_covered": "3.20",
            "service_flag": "0",
            "report_date": report_date.isoformat(),
        },
    ]
    return defects[: max(0, CFG.expense_dirty_rows)]


def file_path_for(report_date: date, landing_dir: str | None = None) -> Path:
    """Canonical landing path. The Airflow FileSensor waits on exactly this name."""
    return Path(landing_dir or CFG.landing_dir) / f"expenses_{report_date.isoformat()}.csv"


def write_expense_file(
    report_date: date,
    landing_dir: str | None = None,
    rows: Sequence[dict[str, Any]] | None = None,
) -> Path | None:
    """Write one day's CSV atomically.  Returns the path, or ``None`` if the day was skipped.

    ``SKIP_DAY`` short-circuits here so the missing-file failure scenario needs no code changes
    to the DAG, only an environment variable.
    """
    if CFG.skip_day and CFG.skip_day == report_date.isoformat():
        LOG.warning(
            "skipping expense file on purpose (SKIP_DAY)",
            extra={"event": "expense_file_skipped", "report_date": report_date.isoformat()},
        )
        return None

    target = file_path_for(report_date, landing_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = list(rows) if rows is not None else build_rows(report_date)

    # Write to a hidden temp file in the SAME directory, then rename. Same directory matters:
    # os.replace is only atomic within one filesystem.
    tmp = target.with_name(f".{target.name}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(EXPENSE_FIELDS))
        writer.writeheader()
        writer.writerows(payload)
        handle.flush()
        os.fsync(handle.fileno())  # durability before the rename becomes visible
    os.replace(tmp, target)

    # Count with the SAME validator Airflow will use, so the producer's metric and the DAG's
    # rejected-row count are directly comparable in the report.
    clean = sum(1 for r in payload if expense_reject_reason(r) is None)
    dirty = len(payload) - clean
    EXPENSE_FILES_WRITTEN.inc()
    EXPENSE_LAST_FILE.set(time.time())
    EXPENSE_ROWS_WRITTEN.labels(kind="clean").inc(clean)
    EXPENSE_ROWS_WRITTEN.labels(kind="dirty").inc(dirty)

    LOG.info(
        "expense file written atomically",
        extra={
            "event": "expense_file_written",
            "report_date": report_date.isoformat(),
            "path": str(target),
            "rows": len(payload),
            "clean_rows": clean,
            "dirty_rows": dirty,
            "bytes": target.stat().st_size,
            "run_id": RUN_ID,
        },
    )
    return target


def backfill(start: date, end: date, landing_dir: str | None = None) -> list[Path]:
    """Write files for every day in ``[start, end]`` — used for replay demonstrations."""
    written: list[Path] = []
    current = start
    while current <= end:
        path = write_expense_file(current, landing_dir)
        if path:
            written.append(path)
        current += timedelta(days=1)
    LOG.info(
        "backfill complete",
        extra={
            "event": "backfill_complete",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "files": len(written),
        },
    )
    return written


def run_forever() -> int:
    """Sleep until each simulated day ends, then write that day's file."""
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    serve_metrics(CFG.expense_metrics_port, EXPENSE_REGISTRY)

    epoch = get_epoch()
    # Seed the freshness gauge so ExpenseFileLate does not fire before the first day completes.
    EXPENSE_LAST_FILE.set(time.time())

    LOG.info(
        "expense producer started",
        extra={
            "event": "expense_producer_start",
            "landing_dir": CFG.landing_dir,
            "sim_day_seconds": CFG.sim_day_seconds,
            "num_vehicles": CFG.num_vehicles,
            "skip_day": CFG.skip_day or None,
            "sim_epoch": epoch,
        },
    )

    while not _SHUTDOWN:
        wait_s = seconds_until_next_sim_day(epoch=epoch)
        LOG.info(
            "waiting for end of simulated day",
            extra={
                "event": "expense_wait",
                "current_sim_date": sim_date(epoch=epoch).isoformat(),
                "seconds_to_day_end": round(wait_s, 1),
            },
        )
        # Sleep in short slices so SIGTERM is honoured within a second.
        deadline = time.time() + wait_s
        while time.time() < deadline and not _SHUTDOWN:
            time.sleep(min(1.0, deadline - time.time()))
        if _SHUTDOWN:
            break

        # The day that has just ended is the one to invoice.
        finished_day = sim_date(epoch=epoch) - timedelta(days=1)
        try:
            write_expense_file(finished_day)
        except OSError as exc:
            LOG.error(
                "failed to write expense file",
                extra={
                    "event": "expense_file_failed",
                    "report_date": finished_day.isoformat(),
                    "error": str(exc),
                },
                exc_info=True,
            )
        # Guard against a tight loop if the clock maths ever returns ~0.
        time.sleep(1.0)

    LOG.info("expense producer stopped cleanly", extra={"event": "expense_producer_stopped"})
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily expense file producer (batch source).")
    parser.add_argument(
        "--backfill",
        nargs=2,
        metavar=("START", "END"),
        help="write files for an inclusive past date range (YYYY-MM-DD YYYY-MM-DD) and exit",
    )
    parser.add_argument(
        "--once",
        nargs="?",
        const="",
        metavar="DATE",
        help="write a single file (default: the simulated day just finished) and exit",
    )
    parser.add_argument("--landing-dir", default=None, help="override LANDING_DIR")
    args = parser.parse_args(argv)

    if args.backfill:
        start = date.fromisoformat(args.backfill[0])
        end = date.fromisoformat(args.backfill[1])
        backfill(start, end, args.landing_dir)
        return 0

    if args.once is not None:
        target = (
            date.fromisoformat(args.once)
            if args.once
            else sim_date() - timedelta(days=1)
        )
        path = write_expense_file(target, args.landing_dir)
        print(path or f"skipped {target}")
        return 0

    return run_forever()


if __name__ == "__main__":
    sys.exit(main())


# Re-exported for the tests so they do not need to import datetime themselves.
UTC = UTC
