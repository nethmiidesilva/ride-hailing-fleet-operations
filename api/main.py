"""FastAPI serving layer (storage & serving, 10 marks).

The API is the programmatic face of the business question.  It reads *only* from PostgreSQL —
never from Kafka or the lake — because the serving layer's job is to answer queries in
milliseconds, and both layers have already written their results there:

* ``/metrics/fleet``, ``/metrics/zones``, ``/alerts/idle``  -> speed layer tables (seconds old)
* ``/metrics/time-of-day``, ``/reports/profitability``      -> batch layer tables (one sim day old)

That split is visible in the response models (each carries its ``source_layer``), which makes the
Lambda architecture legible from the outside.

Endpoints
---------
``GET /health``                      deep health: Postgres + Kafka + data freshness
``GET /health/live``                 liveness only (used as the Docker healthcheck)
``GET /metrics/fleet``               active/idle vehicles, idle ratio, trips and earnings
``GET /metrics/zones?minutes=15``    per-zone utilisation and earnings over recent windows
``GET /metrics/time-of-day?date=``   earnings/utilisation by simulated hour (batch layer)
``GET /alerts/idle?status=open``     threshold alerts
``GET /reports/profitability?date=`` per-vehicle profitability rows
``GET /reports/profitability/{date}/html``  the generated HTML report file
``GET /vehicles/unprofitable?date=`` the answer to the second half of the business question
``GET /metrics-prom``                Prometheus scrape endpoint for this service
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from datetime import date as date_type
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from common.config import CFG
from common.db import query, query_one
from common.logging_setup import RUN_ID, setup_logging
from common.metrics import API_HEALTH, API_REGISTRY, API_REQUEST_DURATION, API_REQUESTS

LOG = setup_logging("api", "serving")


# ============================================================================================
# Response models — explicit schemas so /docs is a usable contract, not a guess
# ============================================================================================
class HealthComponent(BaseModel):
    """Status of one dependency."""

    ok: bool
    detail: str | None = None
    latency_ms: float | None = None


class HealthResponse(BaseModel):
    """Deep health check result."""

    status: Literal["healthy", "degraded"]
    checked_at: datetime
    run_id: str
    components: dict[str, HealthComponent]
    freshness_seconds: float | None = Field(
        None, description="Age of the newest telemetry event known to the serving layer."
    )
    freshness_threshold_seconds: int


class FleetMetrics(BaseModel):
    """Live fleet-wide utilisation and earnings (speed layer)."""

    source_layer: Literal["speed"] = "speed"
    window_start: datetime | None
    window_end: datetime | None
    active_vehicles: int
    idle_vehicles: int
    total_vehicles: int
    idle_ratio: float
    trips_last_hour: int
    earnings_last_hour_lkr: float
    trips_last_sim_hour: int = Field(
        description="Trips over the real-time span equal to one simulated hour, clamped to at "
        "least one speed-layer window (see sim_hour_lookback_seconds)."
    )
    earnings_last_sim_hour_lkr: float
    sim_hour_lookback_seconds: float = Field(
        description="Real seconds actually used for the 'last simulated hour' figures."
    )
    avg_speed_kmh: float
    open_idle_alerts: int
    data_age_seconds: float | None


class ZoneMetrics(BaseModel):
    """Per-zone aggregates over a recent time span (speed layer)."""

    source_layer: Literal["speed"] = "speed"
    zone: str
    windows: int
    active_vehicles: int
    idle_vehicles: int
    idle_ratio: float
    trips: int
    earnings_lkr: float
    avg_speed_kmh: float


class TimeOfDayRow(BaseModel):
    """Earnings and utilisation for one simulated hour (batch layer)."""

    source_layer: Literal["batch"] = "batch"
    report_date: date_type
    sim_hour: int
    zone: str
    trips: int
    earnings_lkr: float
    utilization: float
    avg_speed_kmh: float


class IdleAlert(BaseModel):
    """One idle incident."""

    id: int
    vehicle_id: str
    zone: str | None
    idle_since: datetime
    detected_at: datetime
    resolved_at: datetime | None
    idle_minutes: float | None
    status: str


class ProfitabilityRow(BaseModel):
    """One vehicle's reconciled day (batch layer)."""

    source_layer: Literal["batch"] = "batch"
    report_date: date_type
    vehicle_id: str
    driver_id: str | None
    trips: int
    revenue_lkr: float
    fuel_cost: float
    maintenance_cost: float
    total_cost_lkr: float
    profit_lkr: float
    margin: float | None
    utilization: float
    distance_km: float
    cost_per_km: float | None
    revenue_per_km: float | None
    is_unprofitable: bool
    trend: str
    data_quality_flag: str


# ============================================================================================
# App
# ============================================================================================
#: How often the background task re-evaluates health for the Prometheus gauge.
HEALTH_REFRESH_SECONDS = 15


async def _refresh_health_gauge() -> None:
    """Keep ``api_health_status`` current without anyone calling ``/health``.

    DEFECT-016: the gauge used to be written only inside the ``/health`` handler, but Prometheus
    scrapes ``/metrics-prom`` and the container healthcheck uses ``/health/live``.  Nothing called
    the deep check on a schedule, so the gauge sat at its initial 0 and ``ApiUnhealthy`` fired
    permanently while the service was in fact healthy — the mirror image of DEFECT-014.

    Running the check on a timer means the gauge reflects reality continuously, which is the only
    way an alert on it can mean anything.  It runs in a background task so a slow dependency
    delays the gauge, never a request.
    """
    while True:
        try:
            components = {"postgres": _check_postgres(), "kafka": _check_kafka()}
            age = _data_age_seconds()
            components["data_freshness"] = HealthComponent(
                ok=age is not None and age < CFG.freshness_seconds
            )
            healthy = all(c.ok for c in components.values())
            API_HEALTH.set(1 if healthy else 0)
            if not healthy:
                LOG.warning(
                    "background health check degraded",
                    extra={
                        "event": "health_degraded",
                        "failing": [k for k, v in components.items() if not v.ok],
                        "data_age_s": age,
                    },
                )
        except Exception as exc:  # noqa: BLE001 - the refresher must never die
            LOG.error(
                "health refresh failed",
                extra={"event": "health_refresh_failed", "error": str(exc)},
            )
            API_HEALTH.set(0)
        await asyncio.sleep(HEALTH_REFRESH_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Log start-up and shutdown so the serving layer appears in the same log stream."""
    LOG.info(
        "api starting",
        extra={
            "event": "api_start",
            "run_id": RUN_ID,
            "postgres": f"{CFG.postgres_host}:{CFG.postgres_port}/{CFG.postgres_db}",
            "freshness_threshold_s": CFG.freshness_seconds,
            "health_refresh_seconds": HEALTH_REFRESH_SECONDS,
        },
    )
    refresher = asyncio.create_task(_refresh_health_gauge())
    yield
    refresher.cancel()
    LOG.info("api stopping", extra={"event": "api_stop"})


app = FastAPI(
    title="Fleet Operations API",
    version="1.0.0",
    description=__doc__,
    lifespan=lifespan,
    openapi_tags=[
        {"name": "health", "description": "Liveness and deep health checks."},
        {"name": "speed-layer", "description": "Real-time metrics, seconds old."},
        {"name": "batch-layer", "description": "Reconciled daily results."},
        {"name": "observability", "description": "Prometheus metrics for this service."},
    ],
)


@app.middleware("http")
async def observability_middleware(request: Request, call_next) -> Response:
    """Structured access log + Prometheus request metrics for every call."""
    started = time.perf_counter()
    # The route template (not the concrete path) keeps label cardinality bounded.
    route = request.scope.get("route")
    path_label = getattr(route, "path", request.url.path)
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        status = 500
        raise
    finally:
        duration = time.perf_counter() - started
        API_REQUESTS.labels(
            method=request.method, path=path_label, status=str(status)
        ).inc()
        API_REQUEST_DURATION.labels(path=path_label).observe(duration)
        LOG.info(
            "http request",
            extra={
                "event": "http_request",
                "method": request.method,
                "path": request.url.path,
                "status": status,
                "duration_ms": round(duration * 1000, 2),
                "client": request.client.host if request.client else None,
            },
        )
    return response


# ============================================================================================
# Health
# ============================================================================================
def _check_postgres() -> HealthComponent:
    started = time.perf_counter()
    try:
        query_one("SELECT 1 AS ok")
        return HealthComponent(ok=True, latency_ms=round((time.perf_counter() - started) * 1000, 2))
    except Exception as exc:  # noqa: BLE001
        return HealthComponent(ok=False, detail=str(exc)[:300])


def _check_kafka() -> HealthComponent:
    """A metadata request is the cheapest proof the broker is reachable and has our topic."""
    started = time.perf_counter()
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": CFG.kafka_bootstrap, "socket.timeout.ms": 3000})
        metadata = admin.list_topics(timeout=4.0)
        present = CFG.kafka_topic in metadata.topics
        return HealthComponent(
            ok=present,
            detail=None if present else f"topic {CFG.kafka_topic} not found",
            latency_ms=round((time.perf_counter() - started) * 1000, 2),
        )
    except Exception as exc:  # noqa: BLE001
        return HealthComponent(ok=False, detail=str(exc)[:300])


def _data_age_seconds() -> float | None:
    """Seconds since the newest telemetry event reached the serving layer."""
    row = query_one(
        "SELECT EXTRACT(EPOCH FROM (now() - max(last_event_time))) AS age FROM vehicle_status"
    )
    if not row or row.get("age") is None:
        return None
    return float(row["age"])


@app.get("/health/live", tags=["health"], summary="Liveness probe")
def health_live() -> dict[str, str]:
    """Always 200 while the process is running. Used as the Docker healthcheck.

    Deliberately separate from ``/health``: if the container healthcheck used the *deep* check,
    Docker would restart the API whenever Postgres hiccuped, which turns a degraded read path
    into a full outage.
    """
    return {"status": "alive", "run_id": RUN_ID}


@app.get(
    "/health",
    tags=["health"],
    response_model=HealthResponse,
    summary="Deep health check (Postgres + Kafka + data freshness)",
    responses={503: {"description": "One or more dependencies are unhealthy or data is stale."}},
)
def health() -> JSONResponse:
    """Return 200 when everything is healthy, 503 with details when it is not.

    Data freshness is part of health on purpose: a pipeline whose dependencies are all "up" but
    which has not received an event in five minutes is *not* healthy, and that is exactly the
    failure the chaos test injects by stopping the producer.
    """
    components = {"postgres": _check_postgres(), "kafka": _check_kafka()}

    age = None
    try:
        age = _data_age_seconds()
        fresh = age is not None and age < CFG.freshness_seconds
        components["data_freshness"] = HealthComponent(
            ok=fresh,
            detail=(
                "no telemetry rows yet"
                if age is None
                else f"newest event is {age:.1f}s old (threshold {CFG.freshness_seconds}s)"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        components["data_freshness"] = HealthComponent(ok=False, detail=str(exc)[:300])

    healthy = all(c.ok for c in components.values())
    API_HEALTH.set(1 if healthy else 0)
    payload = HealthResponse(
        status="healthy" if healthy else "degraded",
        checked_at=datetime.now(tz=UTC),
        run_id=RUN_ID,
        components=components,
        freshness_seconds=round(age, 2) if age is not None else None,
        freshness_threshold_seconds=CFG.freshness_seconds,
    )
    if not healthy:
        LOG.warning(
            "health check degraded",
            extra={
                "event": "health_degraded",
                "failing": [k for k, v in components.items() if not v.ok],
                "data_age_s": age,
            },
        )
    return JSONResponse(
        status_code=200 if healthy else 503, content=payload.model_dump(mode="json")
    )


# ============================================================================================
# Speed layer
# ============================================================================================
@app.get(
    "/metrics/fleet",
    tags=["speed-layer"],
    response_model=FleetMetrics,
    summary="Live fleet utilisation and earnings",
)
def fleet_metrics() -> FleetMetrics:
    """Answer the first half of the business question: what is the fleet doing right now?

    ``trips_last_sim_hour`` converts one simulated hour back into real seconds
    (``SIM_DAY_SECONDS / 24``), so the number means "an hour of business time" rather than
    "an hour of wall-clock time" — the two differ by 144x in this simulation.

    With the default configuration one simulated hour is only 25 real seconds, which is *below*
    the 1-minute window granularity of the speed layer.  The look-back is therefore clamped to
    one whole window (DEFECT-007: without the clamp the figure was always 0).  The
    ``sim_hour_lookback_seconds`` field reports the value actually used so the number is never
    misleading.
    """
    now_row = query_one("SELECT * FROM v_fleet_now") or {}
    sim_hour_real_seconds = max(CFG.sim_day_seconds / 24.0, CFG.stream_window_minutes * 60)

    hour = query_one(
        """
        SELECT COALESCE(sum(trips_completed), 0)::int AS trips,
               COALESCE(round(sum(earnings_lkr), 2), 0) AS earnings
        FROM realtime_zone_metrics
        WHERE window_start > now() - interval '1 hour'
        """
    ) or {}
    sim_hour = query_one(
        """
        SELECT COALESCE(sum(trips_completed), 0)::int AS trips,
               COALESCE(round(sum(earnings_lkr), 2), 0) AS earnings
        FROM realtime_zone_metrics
        WHERE window_start > now() - make_interval(secs => %s)
        """,
        (sim_hour_real_seconds,),
    ) or {}
    alerts = query_one("SELECT count(*)::int AS n FROM idle_alerts WHERE status = 'open'") or {}

    return FleetMetrics(
        window_start=now_row.get("window_start"),
        window_end=now_row.get("window_end"),
        active_vehicles=int(now_row.get("active_vehicles") or 0),
        idle_vehicles=int(now_row.get("idle_vehicles") or 0),
        total_vehicles=int(now_row.get("total_vehicles") or 0),
        idle_ratio=float(now_row.get("idle_ratio") or 0),
        trips_last_hour=int(hour.get("trips") or 0),
        earnings_last_hour_lkr=float(hour.get("earnings") or 0),
        trips_last_sim_hour=int(sim_hour.get("trips") or 0),
        earnings_last_sim_hour_lkr=float(sim_hour.get("earnings") or 0),
        sim_hour_lookback_seconds=round(sim_hour_real_seconds, 1),
        avg_speed_kmh=float(now_row.get("avg_speed_kmh") or 0),
        open_idle_alerts=int(alerts.get("n") or 0),
        data_age_seconds=_data_age_seconds(),
    )


@app.get(
    "/metrics/zones",
    tags=["speed-layer"],
    response_model=list[ZoneMetrics],
    summary="Per-zone utilisation and earnings over recent windows",
)
def zone_metrics(
    minutes: int = Query(15, ge=1, le=1440, description="look-back window in real minutes"),
) -> list[ZoneMetrics]:
    """Aggregate the 1-minute speed-layer windows over the requested look-back.

    Averaging the per-window vehicle counts (rather than summing them) is correct: each window
    already counts distinct vehicles, so summing would count the same vehicle once per minute.
    """
    rows = query(
        """
        SELECT zone,
               count(*)::int                                        AS windows,
               round(avg(active_vehicles))::int                     AS active_vehicles,
               round(avg(idle_vehicles))::int                       AS idle_vehicles,
               round(avg(idle_ratio), 4)                            AS idle_ratio,
               COALESCE(sum(trips_completed), 0)::int               AS trips,
               COALESCE(round(sum(earnings_lkr), 2), 0)             AS earnings_lkr,
               COALESCE(round(avg(NULLIF(avg_speed_kmh, 0)), 2), 0) AS avg_speed_kmh
        FROM realtime_zone_metrics
        WHERE window_start > now() - make_interval(mins => %s)
        GROUP BY zone
        ORDER BY earnings_lkr DESC
        """,
        (minutes,),
    )
    return [ZoneMetrics(**r) for r in rows]


@app.get(
    "/alerts/idle",
    tags=["speed-layer"],
    response_model=list[IdleAlert],
    summary="Threshold-based idle alerts",
)
def idle_alerts(
    # "abandoned" (DEFECT-019) is a vehicle that stopped reporting while idle, as opposed
    # to "resolved", which means it started moving again.
    status: Literal["open", "resolved", "abandoned", "all"] = Query("open"),
    limit: int = Query(100, ge=1, le=1000),
) -> list[IdleAlert]:
    """List idle incidents. ``status=open`` is the operational view a dispatcher would watch."""
    if status == "all":
        rows = query(
            "SELECT id, vehicle_id, zone, idle_since, detected_at, resolved_at, idle_minutes,"
            " status FROM idle_alerts ORDER BY detected_at DESC LIMIT %s",
            (limit,),
        )
    else:
        rows = query(
            "SELECT id, vehicle_id, zone, idle_since, detected_at, resolved_at, idle_minutes,"
            " status FROM idle_alerts WHERE status = %s ORDER BY detected_at DESC LIMIT %s",
            (status, limit),
        )
    return [IdleAlert(**r) for r in rows]


# ============================================================================================
# Batch layer
# ============================================================================================
def _resolve_report_date(value: str | None) -> date_type:
    """Parse ``?date=`` or fall back to the latest reconciled day.

    A malformed date is a 422 (the client's mistake); no reconciled data at all is a 404.
    """
    if value:
        try:
            return date_type.fromisoformat(value)
        except ValueError as exc:
            raise HTTPException(
                status_code=422, detail=f"date must be YYYY-MM-DD, got {value!r}"
            ) from exc
    row = query_one("SELECT max(report_date) AS d FROM daily_vehicle_profitability")
    if not row or not row.get("d"):
        raise HTTPException(
            status_code=404,
            detail="no reconciled day exists yet — the Airflow DAG has not completed a run",
        )
    return row["d"]


@app.get(
    "/metrics/time-of-day",
    tags=["batch-layer"],
    response_model=list[TimeOfDayRow],
    summary="Earnings and utilisation by simulated hour of day",
)
def time_of_day(date: str | None = Query(None, description="YYYY-MM-DD; default = latest day")) -> list[TimeOfDayRow]:
    """Batch-layer view: where and when the fleet earns, on the simulated clock."""
    target = _resolve_report_date(date)
    rows = query(
        """
        SELECT report_date, sim_hour, zone, trips, earnings_lkr, utilization, avg_speed_kmh
        FROM daily_zone_summary
        WHERE report_date = %s
        ORDER BY sim_hour, zone
        """,
        (target,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"no zone summary for {target.isoformat()}")
    return [TimeOfDayRow(**r) for r in rows]


@app.get(
    "/reports/profitability",
    tags=["batch-layer"],
    response_model=list[ProfitabilityRow],
    summary="Per-vehicle profitability for a reconciled day",
)
def profitability(
    date: str | None = Query(None, description="YYYY-MM-DD; default = latest reconciled day"),
    limit: int = Query(500, ge=1, le=5000),
) -> list[ProfitabilityRow]:
    """Every vehicle's day, worst profit first."""
    target = _resolve_report_date(date)
    rows = query(
        """
        SELECT report_date, vehicle_id, driver_id, trips, revenue_lkr, fuel_cost,
               maintenance_cost, total_cost_lkr, profit_lkr, margin, utilization, distance_km,
               cost_per_km, revenue_per_km, is_unprofitable, trend, data_quality_flag
        FROM daily_vehicle_profitability
        WHERE report_date = %s
        ORDER BY profit_lkr ASC
        LIMIT %s
        """,
        (target, limit),
    )
    if not rows:
        raise HTTPException(
            status_code=404, detail=f"no profitability rows for {target.isoformat()}"
        )
    return [ProfitabilityRow(**r) for r in rows]


@app.get(
    "/vehicles/unprofitable",
    tags=["batch-layer"],
    response_model=list[ProfitabilityRow],
    summary="Vehicles losing money or trending that way",
)
def unprofitable(
    date: str | None = Query(None, description="YYYY-MM-DD; default = latest reconciled day"),
) -> list[ProfitabilityRow]:
    """The direct answer to the second half of the business question."""
    target = _resolve_report_date(date)
    rows = query(
        """
        SELECT report_date, vehicle_id, driver_id, trips, revenue_lkr, fuel_cost,
               maintenance_cost, total_cost_lkr, profit_lkr, margin, utilization, distance_km,
               cost_per_km, revenue_per_km, is_unprofitable, trend, data_quality_flag
        FROM daily_vehicle_profitability
        WHERE report_date = %s AND (is_unprofitable OR trend <> 'STABLE')
        ORDER BY profit_lkr ASC
        """,
        (target,),
    )
    return [ProfitabilityRow(**r) for r in rows]


@app.get(
    "/reports/profitability/{report_date}/html",
    tags=["batch-layer"],
    summary="The generated HTML daily report",
    response_class=FileResponse,
)
def profitability_html(report_date: str) -> FileResponse:
    """Serve the consolidated report file produced by the Airflow DAG."""
    try:
        target = date_type.fromisoformat(report_date)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"date must be YYYY-MM-DD, got {report_date!r}"
        ) from exc
    path = Path(CFG.reports_dir) / f"profitability_{target.isoformat()}.html"
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"report for {target.isoformat()} has not been generated yet ({path})",
        )
    return FileResponse(path, media_type="text/html", filename=path.name)


@app.get("/reports", tags=["batch-layer"], summary="List generated report files")
def list_reports() -> list[dict[str, Any]]:
    """Discoverability helper: which days already have a rendered report?"""
    directory = Path(CFG.reports_dir)
    if not directory.exists():
        return []
    out = []
    for path in sorted(directory.glob("profitability_*.html")):
        stat = path.stat()
        out.append(
            {
                "report_date": path.stem.replace("profitability_", ""),
                "html": f"/reports/profitability/{path.stem.replace('profitability_', '')}/html",
                "bytes": stat.st_size,
                "generated_at": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
            }
        )
    return out


# ============================================================================================
# Observability
# ============================================================================================
@app.get(
    "/metrics-prom",
    tags=["observability"],
    summary="Prometheus scrape endpoint",
    response_class=PlainTextResponse,
)
def prometheus_metrics() -> Response:
    """Expose this service's metrics; scraped by Prometheus job ``api``."""
    # Only this service's own registry: a scrape of /metrics-prom describes the API and
    # nothing else (DEFECT-014).
    return Response(content=generate_latest(API_REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.get("/", tags=["health"], summary="Service index")
def index() -> dict[str, Any]:
    """Small landing payload so a bare ``curl localhost:8000`` is informative."""
    return {
        "service": "fleet-operations-api",
        "version": app.version,
        "run_id": RUN_ID,
        "architecture": "lambda",
        "docs": "/docs",
        "endpoints": [r.path for r in app.routes if getattr(r, "include_in_schema", False)],
        "simulated_clock": {
            "sim_day_seconds": CFG.sim_day_seconds,
            "sim_start_date": CFG.sim_start_date,
        },
    }


# Re-export for tests that build their own TestClient.
__all__ = ["app"]

_ = timedelta  # kept for future look-back helpers; referenced so linters stay quiet
