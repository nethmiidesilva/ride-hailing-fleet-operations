"""Generate the report's charts from REAL data in PostgreSQL (matplotlib).

Ground rule 2 forbids fabricated numbers, and that includes figures.  Every chart here is drawn
from a live query; if a table is empty the chart is skipped and the script says so, rather than
inventing plausible-looking data.

Outputs (PNG, 150 dpi, into ``docs/diagrams/``):
  * ``chart_zone_hour_earnings.png``  earnings by zone and simulated hour (grouped bars)
  * ``chart_vehicle_profit.png``      per-vehicle profit, loss-making bars highlighted
  * ``chart_throughput.png``          ingestion throughput and micro-batch duration over time
  * ``chart_reconciliation.png``      batch vs speed-layer revenue per day
  * ``chart_reject_reasons.png``      data-quality breakdown
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless container: no display
import matplotlib.pyplot as plt  # noqa: E402
import requests  # noqa: E402

from common.db import query, query_one  # noqa: E402

OUT_DIR = Path(os.getenv("CHART_DIR", "docs/diagrams"))
PROM = f"http://{os.getenv('PROM_HOST', 'prometheus')}:{os.getenv('PROM_PORT', '9090')}"

# A restrained, print-friendly palette; the loss colour is reserved for negative profit so the
# reader never has to check the legend to find the bad news.
INK = "#16222e"
MUTED = "#5b6b7a"
GRID = "#e3eaf0"
SERIES = ["#176ba8", "#3fa7d6", "#59c3c3", "#f6ae2d", "#8c6bb1", "#6aa84f"]
LOSS = "#c62828"
PROFIT = "#2e7d32"

plt.rcParams.update(
    {
        "figure.dpi": 150,
        "savefig.dpi": 150,
        "font.size": 9,
        "axes.edgecolor": MUTED,
        "axes.labelcolor": INK,
        "axes.titlesize": 11,
        "axes.titleweight": "600",
        "text.color": INK,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.7,
        "axes.axisbelow": True,
        "figure.facecolor": "white",
        "axes.spines.top": False,
        "axes.spines.right": False,
    }
)


def _save(fig, name: str) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / name
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  wrote {path}")
    return path


def chart_zone_hour_earnings() -> Path | None:
    """Earnings by zone and simulated hour — the 'time-of-day' half of the business question."""
    latest = query_one("SELECT max(report_date) AS d FROM daily_zone_summary")
    if not latest or not latest["d"]:
        print("  SKIP chart_zone_hour_earnings: daily_zone_summary is empty")
        return None
    rows = query(
        "SELECT zone, sim_hour, round(sum(earnings_lkr),2) AS earnings "
        "FROM daily_zone_summary WHERE report_date = %s GROUP BY zone, sim_hour "
        "ORDER BY sim_hour",
        (latest["d"],),
    )
    if not rows:
        print("  SKIP chart_zone_hour_earnings: no rows for the latest day")
        return None

    zones = sorted({r["zone"] for r in rows})
    hours = sorted({int(r["sim_hour"]) for r in rows})
    lookup = {(r["zone"], int(r["sim_hour"])): float(r["earnings"] or 0) for r in rows}

    fig, ax = plt.subplots(figsize=(10, 4.2))
    bottom = [0.0] * len(hours)
    for i, zone in enumerate(zones):
        values = [lookup.get((zone, h), 0.0) for h in hours]
        ax.bar(hours, values, bottom=bottom, label=zone, color=SERIES[i % len(SERIES)],
               width=0.78, edgecolor="white", linewidth=0.4)
        bottom = [b + v for b, v in zip(bottom, values, strict=False)]

    ax.set_title(f"Earnings by zone and simulated hour — {latest['d']}")
    ax.set_xlabel("simulated hour of day")
    ax.set_ylabel("earnings (LKR)")
    ax.set_xticks(hours)
    ax.legend(frameon=False, ncol=min(len(zones), 6), fontsize=8, loc="upper center",
              bbox_to_anchor=(0.5, -0.18))
    return _save(fig, "chart_zone_hour_earnings.png")


def chart_vehicle_profit() -> Path | None:
    """Per-vehicle profit for the latest reconciled day, loss-makers in red."""
    latest = query_one("SELECT max(report_date) AS d FROM daily_vehicle_profitability")
    if not latest or not latest["d"]:
        print("  SKIP chart_vehicle_profit: daily_vehicle_profitability is empty")
        return None
    rows = query(
        "SELECT vehicle_id, profit_lkr, revenue_lkr, total_cost_lkr, trend "
        "FROM daily_vehicle_profitability WHERE report_date = %s ORDER BY profit_lkr",
        (latest["d"],),
    )
    if not rows:
        return None

    ids = [r["vehicle_id"] for r in rows]
    profits = [float(r["profit_lkr"]) for r in rows]
    colours = [LOSS if p < 0 else PROFIT for p in profits]

    fig, ax = plt.subplots(figsize=(10, max(3.2, 0.22 * len(ids))))
    ax.barh(ids, profits, color=colours, height=0.72)
    ax.axvline(0, color=MUTED, linewidth=1)
    ax.set_title(f"Per-vehicle profit after fuel and maintenance — {latest['d']}")
    ax.set_xlabel("profit (LKR)")
    ax.invert_yaxis()
    losses = sum(1 for p in profits if p < 0)
    ax.text(
        0.99, 0.02, f"{losses} of {len(profits)} vehicles unprofitable",
        transform=ax.transAxes, ha="right", va="bottom", fontsize=8, color=MUTED,
    )
    return _save(fig, "chart_vehicle_profit.png")


def chart_throughput() -> Path | None:
    """Ingestion throughput and micro-batch duration over the last 30 minutes (Prometheus)."""
    end = datetime.now(tz=UTC).timestamp()
    start = end - 1800
    series = {}
    for label, expr in (
        ("events/s produced", "sum(rate(producer_events_sent_total[1m]))"),
        ("rows/s processed", "sum(rate(stream_rows_processed_total[1m]))"),
        ("rows/s rejected", "sum(rate(stream_rows_rejected_total[1m]))"),
    ):
        try:
            response = requests.get(
                f"{PROM}/api/v1/query_range",
                params={"query": expr, "start": start, "end": end, "step": 30},
                timeout=20,
            ).json()
            result = response["data"]["result"]
            if result:
                series[label] = [(float(t), float(v)) for t, v in result[0]["values"]]
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN throughput query failed for {label}: {exc}")

    if not series:
        print("  SKIP chart_throughput: Prometheus returned no range data")
        return None

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 5.2), sharex=True,
                                   gridspec_kw={"height_ratios": [2, 1]})
    for i, (label, points) in enumerate(series.items()):
        times = [(t - start) / 60 for t, _ in points]
        values = [v for _, v in points]
        ax1.plot(times, values, label=label, color=SERIES[i % len(SERIES)], linewidth=1.6)
    ax1.set_ylabel("rate (per second)")
    ax1.set_title("Pipeline throughput over the last 30 minutes")
    ax1.legend(frameon=False, fontsize=8)

    try:
        response = requests.get(
            f"{PROM}/api/v1/query_range",
            params={
                "query": "histogram_quantile(0.95, sum(rate(stream_batch_duration_seconds_bucket[5m])) by (le))",
                "start": start, "end": end, "step": 30,
            },
            timeout=20,
        ).json()["data"]["result"]
        if response:
            points = [(float(t), float(v)) for t, v in response[0]["values"] if v != "NaN"]
            ax2.plot([(t - start) / 60 for t, _ in points], [v for _, v in points],
                     color=SERIES[3], linewidth=1.6, label="p95 micro-batch duration")
            ax2.axhline(20, color=LOSS, linestyle="--", linewidth=1,
                        label="StreamBatchSlow threshold (20 s)")
            ax2.legend(frameon=False, fontsize=8)
    except Exception as exc:  # noqa: BLE001
        print(f"  WARN batch-duration query failed: {exc}")

    ax2.set_ylabel("seconds")
    ax2.set_xlabel("minutes ago (0 = 30 min ago)")
    return _save(fig, "chart_throughput.png")


def chart_reconciliation() -> Path | None:
    """Batch-layer revenue per reconciled day versus the speed layer's running total."""
    rows = query(
        "SELECT report_date, round(sum(revenue_lkr),2) AS batch_revenue, sum(trips) AS batch_trips "
        "FROM daily_vehicle_profitability GROUP BY report_date ORDER BY report_date"
    )
    if not rows:
        print("  SKIP chart_reconciliation: no reconciled days")
        return None
    speed = query_one(
        "SELECT round(sum(earnings_lkr),2) AS revenue, sum(trips_completed) AS trips "
        "FROM realtime_zone_metrics"
    ) or {}

    days = [str(r["report_date"]) for r in rows]
    batch_values = [float(r["batch_revenue"] or 0) for r in rows]
    speed_total = float(speed.get("revenue") or 0)

    fig, ax = plt.subplots(figsize=(8, 4))
    positions = range(len(days))
    ax.bar(positions, batch_values, color=SERIES[0], width=0.55, label="batch layer (recomputed)")
    ax.axhline(speed_total, color=SERIES[3], linestyle="--", linewidth=1.6,
               label=f"speed layer running total ({speed_total:,.0f} LKR)")
    ax.set_xticks(list(positions))
    ax.set_xticklabels(days, rotation=0)
    ax.set_ylabel("revenue (LKR)")
    ax.set_title("Batch vs speed layer — revenue reconciliation")
    ax.legend(frameon=False, fontsize=8)
    for i, value in enumerate(batch_values):
        ax.text(i, value, f"{value:,.0f}", ha="center", va="bottom", fontsize=8, color=INK)
    return _save(fig, "chart_reconciliation.png")


def chart_reject_reasons() -> Path | None:
    """Data-quality breakdown: which validation rules actually fired."""
    rows = query(
        "SELECT reason, count(*)::int AS n FROM rejected_events GROUP BY reason ORDER BY n DESC"
    )
    if not rows:
        print("  SKIP chart_reject_reasons: rejected_events is empty")
        return None
    labels = [r["reason"] for r in rows]
    values = [r["n"] for r in rows]

    fig, ax = plt.subplots(figsize=(8, max(2.6, 0.45 * len(labels))))
    ax.barh(labels, values, color=SERIES[0], height=0.6)
    ax.invert_yaxis()
    ax.set_xlabel("rows quarantined")
    ax.set_title("Telemetry rejected by validation rule")
    for i, value in enumerate(values):
        ax.text(value, i, f" {value:,}", va="center", fontsize=8, color=INK)
    return _save(fig, "chart_reject_reasons.png")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate report charts from real data.")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args(argv)
    global OUT_DIR
    if args.out_dir:
        OUT_DIR = Path(args.out_dir)

    print(f"\ngenerating charts into {OUT_DIR.resolve()}")
    produced = [
        chart_zone_hour_earnings(),
        chart_vehicle_profit(),
        chart_throughput(),
        chart_reconciliation(),
        chart_reject_reasons(),
    ]
    made = [p for p in produced if p]
    print(f"\n{len(made)}/{len(produced)} charts generated\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
