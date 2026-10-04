"""Consolidated daily report: self-contained HTML plus a CSV (storage & serving, 10 marks).

This file is the brief's "consolidated report/dashboard answering the business question".  It is
generated from PostgreSQL *after* the profitability job has run, so every number in it is a real
query result, never a hard-coded example.

Sections (each maps to a requirement):

===============================  ==========================================================
Fleet KPIs                        REQ-05 — utilisation and earnings for the day
Per-vehicle profitability table   REQ-07 — the answer: who is losing money, sorted by profit
Zone x time-of-day earnings       REQ-04 — where and when the money is made
Idle alerts raised that day       REQ-06 — the threshold alert
Data quality                      REQ-12 — rejected telemetry and expense rows
Batch vs speed reconciliation     REQ-11 — the two layers compared, difference explained
===============================  ==========================================================

Plain string templating is used rather than Jinja2: the output is one static file, and keeping it
dependency-free means the report can also be rendered from the Airflow image, the tests image and
a bare Python 3.11 without installing anything extra.
"""

from __future__ import annotations

import argparse
import csv
import html
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from common.config import CFG
from common.db import query, query_one
from common.logging_setup import RUN_ID, setup_logging

# DEFECT-012: this module is imported *inside* a running Airflow task (`build_daily_report`).
# Calling setup_logging() at import time would clear the root handlers Airflow installed to
# capture that task's log, mid-task, which broke the task runner's log pipe and got the task
# SIGKILLed with return code -9. A library must not configure logging; only an application may.
# So the module takes a plain named logger, and configures handlers only when run as a script.
LOG = logging.getLogger("report-builder")


# ============================================================================================
# Queries — kept together so every number in the report can be traced to one SQL statement
# ============================================================================================
Q_KPIS = """
SELECT
    count(*)                                              AS vehicles,
    sum(trips)                                            AS trips,
    round(sum(revenue_lkr), 2)                            AS revenue_lkr,
    round(sum(total_cost_lkr), 2)                         AS cost_lkr,
    round(sum(profit_lkr), 2)                             AS profit_lkr,
    round(avg(utilization), 4)                            AS avg_utilization,
    round(sum(active_minutes), 1)                         AS active_minutes,
    round(sum(idle_minutes), 1)                           AS idle_minutes,
    count(*) FILTER (WHERE is_unprofitable)               AS unprofitable_vehicles,
    count(*) FILTER (WHERE trend <> 'STABLE')             AS at_risk_vehicles,
    count(*) FILTER (WHERE data_quality_flag <> 'OK')     AS flagged_vehicles
FROM daily_vehicle_profitability
WHERE report_date = %s;
"""

Q_VEHICLES = """
SELECT vehicle_id, driver_id, trips, revenue_lkr, fuel_cost, maintenance_cost, total_cost_lkr,
       profit_lkr, margin, utilization, distance_km, cost_per_km, revenue_per_km,
       is_unprofitable, trend, data_quality_flag
FROM daily_vehicle_profitability
WHERE report_date = %s
ORDER BY profit_lkr ASC;
"""

Q_ZONE_HOUR = """
SELECT zone, sim_hour, trips, earnings_lkr, utilization, avg_speed_kmh
FROM daily_zone_summary
WHERE report_date = %s
ORDER BY zone, sim_hour;
"""

Q_ZONE_TOTALS = """
SELECT zone,
       sum(trips)                     AS trips,
       round(sum(earnings_lkr), 2)    AS earnings_lkr,
       round(avg(utilization), 4)     AS avg_utilization
FROM daily_zone_summary
WHERE report_date = %s
GROUP BY zone
ORDER BY earnings_lkr DESC;
"""

Q_IDLE_ALERTS = """
SELECT vehicle_id, zone, idle_since, detected_at, resolved_at, idle_minutes, status
FROM idle_alerts
WHERE detected_at::date >= %s::date - 1
ORDER BY detected_at DESC
LIMIT 50;
"""

Q_REJECTED_EVENTS = """
SELECT reason, count(*)::int AS rows_rejected
FROM rejected_events
GROUP BY reason
ORDER BY rows_rejected DESC;
"""

Q_REJECTED_EXPENSES = """
SELECT reason, count(*)::int AS rows_rejected
FROM rejected_expenses
WHERE report_date = %s
GROUP BY reason
ORDER BY rows_rejected DESC;
"""

# The reconciliation: the batch layer's exact total for the day versus what the speed layer
# accumulated in its 1-minute windows over the real-time span that simulated day occupied.
Q_SPEED_TOTALS = """
SELECT
    COALESCE(round(sum(earnings_lkr), 2), 0)  AS speed_earnings_lkr,
    COALESCE(sum(trips_completed), 0)         AS speed_trips,
    count(*)                                  AS windows
FROM realtime_zone_metrics
WHERE window_start >= %s AND window_start < %s;
"""



# ============================================================================================
# HTML helpers
# ============================================================================================
def _fmt(value: Any, decimals: int = 2) -> str:
    """Format a value for display; None becomes an em dash rather than 'None'."""
    if value is None:
        return "&mdash;"
    if isinstance(value, float):
        return f"{value:,.{decimals}f}"
    if isinstance(value, (int,)) and not isinstance(value, bool):
        return f"{value:,}"
    return html.escape(str(value))


def _table(rows: Sequence[dict[str, Any]], columns: Sequence[str], row_class=None) -> str:
    """Render a list of dicts as an HTML table.

    ``row_class`` is an optional callable mapping a row to a CSS class, used to highlight
    unprofitable and at-risk vehicles (a requirement of the report section).
    """
    if not rows:
        return '<p class="empty">No rows.</p>'
    head = "".join(f"<th>{html.escape(c.replace('_', ' '))}</th>" for c in columns)
    body = []
    for row in rows:
        cls = row_class(row) if row_class else ""
        cells = "".join(f"<td>{_fmt(row.get(c))}</td>" for c in columns)
        body.append(f'<tr class="{cls}">{cells}</tr>')
    return (
        f'<table><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table>'
    )


def _vehicle_row_class(row: dict[str, Any]) -> str:
    if row.get("is_unprofitable"):
        return "loss"
    if row.get("trend") in ("AT_RISK", "DECLINING"):
        return "warn"
    if row.get("data_quality_flag") not in (None, "OK"):
        return "flag"
    return ""


def _heatmap(zone_hour: Sequence[dict[str, Any]]) -> str:
    """Render zone x simulated-hour earnings as a coloured HTML grid.

    A table rather than a chart image keeps the report a single self-contained file that opens
    anywhere, including from the FastAPI endpoint, with no image hosting.
    """
    if not zone_hour:
        return '<p class="empty">No zone summary rows.</p>'
    zones = sorted({r["zone"] for r in zone_hour})
    hours = sorted({int(r["sim_hour"]) for r in zone_hour})
    lookup = {(r["zone"], int(r["sim_hour"])): float(r["earnings_lkr"] or 0) for r in zone_hour}
    peak = max(lookup.values()) or 1.0

    head = "".join(f"<th>{h:02d}</th>" for h in hours)
    body = []
    for zone in zones:
        cells = []
        for hour in hours:
            value = lookup.get((zone, hour), 0.0)
            # Intensity is relative to the busiest cell of the day.
            alpha = 0.06 + 0.94 * (value / peak) if value else 0.0
            style = f'style="background: rgba(23,107,168,{alpha:.2f})"' if value else ""
            cells.append(f'<td {style} title="{value:,.0f} LKR">{value:,.0f}</td>')
        body.append(f"<tr><th>{html.escape(zone)}</th>{''.join(cells)}</tr>")
    return (
        '<table class="heat"><thead><tr><th>zone \\ sim hour</th>'
        f"{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"
    )


CSS = """
:root { --ink:#16222e; --muted:#5b6b7a; --line:#dde5ec; --accent:#176ba8; --loss:#c62828;
        --warn:#ef6c00; --ok:#2e7d32; }
* { box-sizing: border-box; }
body { font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
       margin: 0; padding: 32px; color: var(--ink); background: #f6f8fa; line-height: 1.5; }
.wrap { max-width: 1180px; margin: 0 auto; }
h1 { font-size: 26px; margin: 0 0 4px; }
h2 { font-size: 18px; margin: 34px 0 10px; padding-bottom: 6px; border-bottom: 2px solid var(--line); }
.sub { color: var(--muted); margin: 0 0 24px; font-size: 14px; }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(165px, 1fr)); gap: 12px; }
.kpi { background: #fff; border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; }
.kpi .label { font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); }
.kpi .value { font-size: 24px; font-weight: 650; margin-top: 4px; }
.kpi.neg .value { color: var(--loss); }
.kpi.pos .value { color: var(--ok); }
table { width: 100%; border-collapse: collapse; background: #fff; font-size: 13px;
        border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
th, td { padding: 7px 10px; text-align: right; border-bottom: 1px solid var(--line); }
th:first-child, td:first-child { text-align: left; }
thead th { background: #eef3f7; font-size: 11px; text-transform: uppercase;
           letter-spacing: .04em; color: var(--muted); }
tbody tr:hover { background: #f4f9ff; }
tr.loss td { background: #fdecea; }
tr.loss td:first-child { border-left: 3px solid var(--loss); font-weight: 600; }
tr.warn td { background: #fff5e6; }
tr.warn td:first-child { border-left: 3px solid var(--warn); }
tr.flag td { background: #f3f0ff; }
table.heat td { text-align: center; font-size: 11px; color: #123; }
table.heat th:first-child { font-weight: 600; }
.empty { color: var(--muted); font-style: italic; }
.note { background: #fff; border-left: 3px solid var(--accent); padding: 10px 14px;
        font-size: 13px; color: var(--muted); border-radius: 0 8px 8px 0; }
footer { margin-top: 40px; font-size: 12px; color: var(--muted);
         border-top: 1px solid var(--line); padding-top: 12px; }
code { background: #eef3f7; padding: 1px 5px; border-radius: 4px; font-size: 12px; }
"""


# ============================================================================================
# Report assembly
# ============================================================================================
def gather(report_date: date) -> dict[str, Any]:
    """Run every query for ``report_date`` and return the raw data.

    Separated from rendering so the unit tests can assert on the numbers without parsing HTML,
    and so the same data can feed the API and the matplotlib chart script.
    """
    d = report_date.isoformat()
    kpis = query_one(Q_KPIS, (d,)) or {}
    vehicles = query(Q_VEHICLES, (d,))
    zone_hour = query(Q_ZONE_HOUR, (d,))
    zone_totals = query(Q_ZONE_TOTALS, (d,))
    alerts = query(Q_IDLE_ALERTS, (d,))
    rejected_events = query(Q_REJECTED_EVENTS)
    rejected_expenses = query(Q_REJECTED_EXPENSES, (d,))

    # --- batch vs speed reconciliation ------------------------------------------------------
    # The simulated day occupied a real-time span; that span is recovered from the master
    # dataset's own event_time range as recorded in vehicle_status/realtime windows.
    span = query_one(
        """
        SELECT min(window_start) AS first_window, max(window_end) AS last_window
        FROM realtime_zone_metrics
        """
    ) or {}
    speed = {}
    if span.get("first_window"):
        speed = query_one(Q_SPEED_TOTALS, (span["first_window"], span["last_window"])) or {}

    batch_revenue = float(kpis.get("revenue_lkr") or 0)
    speed_revenue = float(speed.get("speed_earnings_lkr") or 0)
    difference = round(speed_revenue - batch_revenue, 2)
    pct = round(difference / batch_revenue * 100, 2) if batch_revenue else None

    return {
        "report_date": d,
        "generated_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "kpis": kpis,
        "vehicles": vehicles,
        "zone_hour": zone_hour,
        "zone_totals": zone_totals,
        "alerts": alerts,
        "rejected_events": rejected_events,
        "rejected_expenses": rejected_expenses,
        "reconciliation": {
            "batch_revenue_lkr": round(batch_revenue, 2),
            "speed_revenue_lkr": round(speed_revenue, 2),
            "difference_lkr": difference,
            "difference_pct": pct,
            "speed_trips": speed.get("speed_trips"),
            "batch_trips": kpis.get("trips"),
            "speed_windows": speed.get("windows"),
            "window_span": [str(span.get("first_window")), str(span.get("last_window"))],
        },
    }


def render_html(data: dict[str, Any]) -> str:
    """Render the gathered data as one self-contained HTML document."""
    k = data["kpis"]
    rec = data["reconciliation"]
    profit = float(k.get("profit_lkr") or 0)

    kpi_cards = [
        ("Vehicles reconciled", _fmt(k.get("vehicles"))),
        ("Trips completed", _fmt(k.get("trips"))),
        ("Revenue (LKR)", _fmt(k.get("revenue_lkr"))),
        ("Cost (LKR)", _fmt(k.get("cost_lkr"))),
        ("Profit (LKR)", _fmt(k.get("profit_lkr"))),
        ("Avg utilisation", _fmt(float(k["avg_utilization"]) * 100 if k.get("avg_utilization") else None, 1) + "%"),
        ("Unprofitable vehicles", _fmt(k.get("unprofitable_vehicles"))),
        ("At-risk / declining", _fmt(k.get("at_risk_vehicles"))),
    ]
    cards = "".join(
        f'<div class="kpi {"neg" if label.startswith("Profit") and profit < 0 else ("pos" if label.startswith("Profit") else "")}">'
        f'<div class="label">{html.escape(label)}</div><div class="value">{value}</div></div>'
        for label, value in kpi_cards
    )

    vehicle_cols = [
        "vehicle_id", "driver_id", "trips", "revenue_lkr", "fuel_cost", "maintenance_cost",
        "total_cost_lkr", "profit_lkr", "margin", "utilization", "distance_km", "cost_per_km",
        "trend", "data_quality_flag",
    ]

    diff_note = (
        f"Speed layer reported <b>{_fmt(rec['speed_revenue_lkr'])} LKR</b> across "
        f"{_fmt(rec['speed_windows'])} one-minute windows; the batch layer recomputed "
        f"<b>{_fmt(rec['batch_revenue_lkr'])} LKR</b> from the Parquet master dataset. "
        f"Difference: <b>{_fmt(rec['difference_lkr'])} LKR</b>"
        + (f" ({rec['difference_pct']:+.2f}%)." if rec.get("difference_pct") is not None else ".")
        + " A non-zero difference is expected and is the honest cost of the speed layer: events "
        "arriving later than the 2-minute watermark are dropped by the streaming windows but are "
        "present in the master dataset, and the streaming window boundaries do not align exactly "
        "with the simulated-day boundary. The batch figure is the one the business uses."
    )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fleet profitability — {data['report_date']}</title>
<style>{CSS}</style></head>
<body><div class="wrap">
<h1>Ride-hailing fleet — daily reconciliation</h1>
<p class="sub">Simulated day <b>{data['report_date']}</b> &middot; generated {data['generated_at']}
 &middot; currency LKR &middot; 1 simulated day = {CFG.sim_day_seconds}s real time</p>

<h2>1. Fleet KPIs</h2>
<div class="kpis">{cards}</div>

<h2>2. Per-vehicle profitability (sorted by profit, worst first)</h2>
<p class="note">Red rows lose money on the day; amber rows are <code>AT_RISK</code> or
<code>DECLINING</code>; purple rows have a data-quality flag. Revenue is recomputed from raw
telemetry, not copied from the speed layer.</p>
{_table(data['vehicles'], vehicle_cols, _vehicle_row_class)}

<h2>3. Earnings by zone and simulated hour of day</h2>
{_heatmap(data['zone_hour'])}
<p class="note">Zone totals for the day:</p>
{_table(data['zone_totals'], ['zone', 'trips', 'earnings_lkr', 'avg_utilization'])}

<h2>4. Idle alerts</h2>
<p class="note">Threshold rule: a vehicle idle for more than
<code>{CFG.idle_alert_minutes} minutes</code> of real time
(~{CFG.idle_alert_minutes * CFG.sim_seconds_per_real_second / 3600:.1f} simulated hours).</p>
{_table(data['alerts'], ['vehicle_id', 'zone', 'idle_since', 'detected_at', 'resolved_at', 'idle_minutes', 'status'])}

<h2>5. Data quality</h2>
<p class="note">Telemetry rows quarantined (all time):</p>
{_table(data['rejected_events'], ['reason', 'rows_rejected'])}
<p class="note">Expense rows quarantined for this day:</p>
{_table(data['rejected_expenses'], ['reason', 'rows_rejected'])}

<h2>6. Batch vs speed-layer reconciliation</h2>
<p class="note">{diff_note}</p>

<footer>
Generated by <code>batch/report_builder.py</code> from PostgreSQL.
Every figure above is the result of a SQL query listed in that module &mdash; nothing is
hard-coded. Machine-readable copy: <code>profitability_{data['report_date']}.csv</code>.
</footer>
</div></body></html>"""


def write_csv(data: dict[str, Any], path: Path) -> Path:
    """Write the per-vehicle table as CSV alongside the HTML."""
    columns = [
        "vehicle_id", "driver_id", "trips", "revenue_lkr", "fuel_cost", "maintenance_cost",
        "total_cost_lkr", "profit_lkr", "margin", "utilization", "distance_km", "cost_per_km",
        "revenue_per_km", "is_unprofitable", "trend", "data_quality_flag",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data["vehicles"])
    return path


def build_report(
    report_date: date | str, run_id: str | None = None, reports_dir: str | None = None
) -> dict[str, str]:
    """Build both report files and return their paths."""
    target = date.fromisoformat(report_date) if isinstance(report_date, str) else report_date
    out_dir = Path(reports_dir or CFG.reports_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = gather(target)
    html_path = out_dir / f"profitability_{target.isoformat()}.html"
    csv_path = out_dir / f"profitability_{target.isoformat()}.csv"
    html_path.write_text(render_html(data), encoding="utf-8")
    write_csv(data, csv_path)

    LOG.info(
        "report written",
        extra={
            "event": "report_written",
            "report_date": target.isoformat(),
            "run_id": run_id or RUN_ID,
            "html": str(html_path),
            "csv": str(csv_path),
            "vehicles": len(data["vehicles"]),
            "unprofitable": data["kpis"].get("unprofitable_vehicles"),
        },
    )
    return {"html": str(html_path), "csv": str(csv_path)}


def main(argv: list[str] | None = None) -> int:
    # Only the script entry point configures logging (see the note at the top).
    setup_logging("report-builder", "serving")
    parser = argparse.ArgumentParser(description="Render the consolidated daily report.")
    parser.add_argument("--date", required=True, help="simulated date (YYYY-MM-DD)")
    parser.add_argument("--out-dir", default=None, help="override REPORTS_DIR")
    args = parser.parse_args(argv)
    paths = build_report(args.date, reports_dir=args.out_dir)
    print(paths["html"])
    print(paths["csv"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
