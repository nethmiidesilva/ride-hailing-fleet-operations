# CLAUDE.md — Project context and ground rules

**Project:** Ride-Hailing Fleet Operations — Lambda Architecture Data Pipeline
**Module:** Applied Big Data Engineering – Mini Project (25% of module grade)
**Deadline:** 28 September 2026
**Spec:** `MASTER_PROMPTk.md` (the authoritative specification; read it before changing anything).

## Business question
*What is fleet utilization and earnings by area/time-of-day right now, and which vehicles are
becoming unprofitable once yesterday's fuel/maintenance costs are factored in?*

## Architecture — LAMBDA (rejected alternative: Kappa)
```
                       +-------------------- SPEED LAYER ---------------------+
gps_producer ---> Kafka (fleet.telemetry, 6 partitions, key=vehicle_id)       |
                       |  Spark Structured Streaming (local[*])              |
                       |    validate -> DLQ/rejected_events                  |
                       |    dedup (watermark 2 min on event_id)              |
                       |    enrich (zone, is_active)                         |
                       |    sink a: Parquet master dataset (partition sim_date)
                       |    sink b: realtime_zone_metrics (1-min windows, UPSERT)
                       |    sink c: vehicle_status + idle_alerts (UPSERT)    |
                       +-----------------------------------------------------+
expense_producer -> /data/landing/expenses_<D>.csv
                       +-------------------- BATCH LAYER ---------------------+
                       | Airflow DAG daily_reconciliation                     |
                       |   sensor -> validate -> load -> spark-submit         |
                       |   profitability_job (Parquet[D] JOIN daily_expenses) |
                       |   -> daily_vehicle_profitability, daily_zone_summary |
                       |   -> report_builder -> HTML + CSV                    |
                       +------------------------------------------------------+
SERVING: PostgreSQL 16  +  FastAPI (:8000)  +  Grafana (:3000)
OBSERVABILITY: JSON logs -> stdout | prometheus_client :8001/:8002/:8003/:8000 |
               kafka-exporter :9308 | Prometheus :9090 + alert_rules | Grafana dashboards
```

## Ground rules (from the spec — follow strictly)
1. Work in phases P0..P10; verify each phase gate before moving on; update `PROGRESS.md` after each.
2. **Never fabricate results.** Every number in `docs/REPORT.md` / `docs/TEST_REPORT.md` must come
   from a real run. Anything not run is marked `NOT EXECUTED` with the reason.
3. Laptop-scale: <= 8 GB Docker RAM, Spark `local[*]`, Airflow `LocalExecutor`.
4. Pin every version. If a pin fails, pick the nearest working one and record it in `PROGRESS.md`.
5. Explainability: docstrings + "why" comments; the student must defend every line in a viva.
6. Configuration lives in `.env` + `common/config.py` only. No hard-coded hosts/ports/thresholds.
7. Shared code between speed and batch layers (`common/zones.py`, `common/schemas.py`) is the
   explicit mitigation for Lambda's "two code paths" weakness.

## Simulated clock
1 simulated day = `SIM_DAY_SECONDS` (default 600 s = 10 real minutes). Epoch is pinned once in
`/shared/sim_epoch.txt` so every container agrees. Events carry **both** `event_time` (real UTC,
used for streaming windows/watermarks) and `sim_ts`/`sim_date`/`sim_hour` (used for batch
partitioning and time-of-day analysis).

## Key conventions
- Currency LKR. Zones: Colombo bounding box lat 6.86–6.98, lon 79.84–79.90, 3x2 grid.
- Fare > 0 **only** on a trip-end event, so revenue never double counts.
- All Postgres writes from Spark go through `psycopg` UPSERTs inside `foreachBatch` (idempotent,
  at-least-once + idempotent write = effectively-once).
- Python 3.11 everywhere inside containers (PySpark 3.5 does not support 3.13).
- Tests run **inside Docker** (`make test-unit`) because the host Python is 3.13.

## Repo map
`common/` shared config, clock, logging, zones, schemas · `producers/` streaming + daily-file
sources · `streaming/` Spark Structured Streaming job + pure transforms · `batch/` Spark batch
profitability job + HTML report builder · `airflow/` image + DAG · `api/` FastAPI ·
`sql/init.sql` schema · `monitoring/` Prometheus + Grafana · `scripts/` e2e, evidence, chaos ·
`tests/{unit,integration,e2e}` · `docs/` REPORT, TEST_REPORT, VIVA_GUIDE, diagrams, evidence.
