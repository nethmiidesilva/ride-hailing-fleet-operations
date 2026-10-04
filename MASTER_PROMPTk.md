# MASTER PROMPT — Ride-Hailing Fleet Operations: Lambda Architecture Data Pipeline

> How to use: put this file in an empty project folder, open Claude Code in that folder, and say:
> **"Read MASTER_PROMPT.md fully and execute it phase by phase. Do not skip verification steps."**

---

## 0. ROLE, GOAL AND GROUND RULES

You are a senior data engineer building a university mini-project called "Applied Big Data Engineering – Mini Project" (25% of the module grade). The deadline is 28 September 2026. Build the **complete, working, end-to-end system**, then produce:

1. a **detailed technical report** (`docs/REPORT.md`, plus a PDF if the tooling works), and
2. a **detailed test report** covering **every test case** (`docs/TEST_REPORT.md`, plus machine-readable pytest output).

### Ground rules (follow strictly)
1. **Work in phases (Section 12).** Finish and **verify** each phase before starting the next. After each phase, update `PROGRESS.md` with: what was built, commands run, verification output (short), known issues.
2. **Never fabricate results.** Every number, table, log line, API response and test result in the reports must come from an actual run in this environment. If something could not be run, say so explicitly and mark it `NOT EXECUTED` with the reason.
3. **Keep it runnable on a laptop.** Target ≤ 8 GB RAM for Docker. Spark runs in `local[*]` mode inside containers, with no Spark cluster. Airflow uses `LocalExecutor`.
4. **Pin every version.** If an image or package version fails to pull or install, pick the nearest working version, pin it, and record the change in `PROGRESS.md`.
5. **Explainability matters.** The student must defend every line in a viva. Write clear, modular code with docstrings and comments that explain *why*, not just *what*. Avoid clever one-liners in core logic.
6. **Configuration lives in one place:** `.env` plus `common/config.py`. No hard-coded hosts, ports or thresholds in business code.
7. Create a `CLAUDE.md` at the start that summarises these rules and the architecture, so context survives if the session is resumed.
8. If you get blocked for real (e.g. Docker is not available), stop, explain exactly what is needed, and do not fake the output.

---

## 1. PROJECT BRIEF (from the assignment)

- Build an end-to-end **Lambda or Kappa** architecture pipeline with:
  - a **streaming source** (Python script emitting events every few seconds), and
  - a **daily-batch source** (Python script dropping one file per simulated day).
- Required stack: **Kafka** (producers, topics, partitions); **Spark Structured Streaming**; **Airflow** (batch/reporting orchestration); storage in **PostgreSQL** and/or **Parquet on a file system**.
- Transformations must be meaningful: cleaning, enrichment, **joins between the two sources**, aggregation, windowing.
- Output: queryable store, plus a consolidated report/dashboard answering the business question.
- Observability: **structured logging across ingestion, processing and storage**, plus **at least one alert/health-check rule**, plus metrics.
- Deliverables: code, observability config, README (Docker Compose), tests, report (8–15 pages), demo.
- Rubric: Architecture decision 20, Tech stack 10, Ingestion 15, Processing 15, Storage & serving 10, Observability 10, Report 15, Code quality 5.

### Chosen use case: Use Case 1 — Ride-Hailing Fleet Operations
**Business question:** *What is fleet utilization and earnings by area/time-of-day right now, and which vehicles are becoming unprofitable once yesterday's fuel/maintenance costs are factored in?*

Required outputs:
- An API endpoint returning real-time fleet utilization metrics: active vehicles, idle ratio, trips/hour, earnings by zone.
- Threshold-based alerts when a vehicle has been idle for a long period.
- A daily per-vehicle profitability reconciliation report.

### Chosen architecture: LAMBDA (the rejected alternative is Kappa)
- **Speed layer:** Kafka → Spark Structured Streaming → PostgreSQL real-time tables, plus idle alerts.
- **Batch layer:** the streaming job also sinks **raw, cleaned events to Parquet** (an immutable master dataset partitioned by `sim_date`). The daily expense CSV is dropped into a landing folder. An **Airflow** DAG waits for the file, validates it, loads it, runs a **Spark batch job** joining that day's trips with expenses, and writes the profitability results and a report file.
- **Serving layer:** PostgreSQL, plus FastAPI, plus Grafana.

---

## 2. SIMULATED CLOCK (state this in README and report)

- **1 simulated day = 10 real minutes** (configurable: `SIM_DAY_SECONDS=600`).
- Simulation start date: `SIM_START_DATE=2026-09-01`. The simulation epoch is stored in a shared file/volume (`/shared/sim_epoch.txt`), written once by the first service that starts, so every service agrees on the same clock.
- Module `common/sim_clock.py` provides:
  - `sim_now()` → simulated datetime (real elapsed seconds × `86400/SIM_DAY_SECONDS` added to start date)
  - `sim_date()` → current simulated date
  - `previous_sim_date()`
  - `sim_hour()` → hour of simulated day (0–23), used for time-of-day analysis
- Each telemetry event carries **both**:
  - `event_time`: real wall-clock UTC. Used for streaming windows and watermarks, so the live dashboard moves in real time.
  - `sim_ts`, `sim_date`, `sim_hour`: used for batch partitioning and time-of-day analysis.
- Document this dual-time design and its trade-off in the report.

---

## 3. TECH STACK (pin versions; verify they work)

| Layer | Choice | Notes |
|---|---|---|
| Messaging | Apache Kafka 3.7.x in KRaft mode (`apache/kafka` image), single broker | Topic `fleet.telemetry`, **6 partitions**, key = `vehicle_id`; topic `fleet.telemetry.dlq` for invalid events |
| Stream processing | PySpark 3.5.x Structured Streaming, `local[*]` | `org.apache.spark:spark-sql-kafka-0-10_2.12:<same spark version>`; custom image based on python:3.11-slim + OpenJDK 17 |
| Batch processing | PySpark 3.5.x (same code base, shared transforms) | Launched by Airflow via `spark-submit` in local mode |
| Orchestration | Apache Airflow 2.9.x, LocalExecutor, own Postgres metadata DB (or a separate schema/database in the same Postgres) | Custom image adds Java 17 + pyspark |
| Master dataset | Parquet on a Docker volume (`/data/lake/telemetry/sim_date=...`) | Simulates S3/HDFS |
| Serving DB | PostgreSQL 16 | Database `fleet` |
| API | FastAPI + Uvicorn + psycopg (v3) or SQLAlchemy | |
| Metrics | `prometheus_client` in every Python service; Prometheus 2.x; `danielqsj/kafka-exporter` for consumer lag / topic offsets | |
| Dashboards/alerts | Grafana 10/11 with **provisioned** datasources and dashboards (Postgres + Prometheus); Prometheus alert rules | Alertmanager optional |
| Logging | Python `logging` with a JSON formatter (`python-json-logger` or custom) to stdout | |
| Tests | pytest, pytest-cov, pytest-html, FastAPI TestClient, pyspark local session | |
| Packaging | Docker Compose v2, Makefile | |

---

## 4. REPOSITORY STRUCTURE (create exactly this, adding files if needed)

```
fleet-lambda/
├── CLAUDE.md
├── PROGRESS.md
├── README.md
├── Makefile
├── docker-compose.yml
├── .env.example            (copy to .env)
├── common/
│   ├── config.py           # reads env vars, single source of truth for settings/thresholds
│   ├── sim_clock.py
│   ├── logging_setup.py    # JSON structured logger: ts, level, service, stage, event, run_id, extra fields
│   ├── zones.py            # lat/lon → zone mapping (shared by stream + batch)
│   └── schemas.py          # event schema (Python + Spark StructType), validation rules
├── producers/
│   ├── Dockerfile
│   ├── gps_producer.py     # streaming source
│   ├── fleet_simulator.py  # vehicle state machine (pure logic, unit-testable)
│   └── expense_producer.py # daily batch source
├── streaming/
│   ├── Dockerfile
│   ├── stream_job.py       # Spark Structured Streaming entrypoint
│   └── transforms.py       # pure transform functions (clean, enrich, window aggregates)
├── batch/
│   ├── profitability_job.py
│   └── report_builder.py   # renders the daily report (HTML + CSV) from Postgres
├── airflow/
│   ├── Dockerfile
│   └── dags/daily_reconciliation_dag.py
├── api/
│   ├── Dockerfile
│   └── main.py
├── sql/init.sql
├── monitoring/
│   ├── prometheus.yml
│   ├── alert_rules.yml
│   └── grafana/provisioning/{datasources,dashboards}/ + dashboard JSON
├── scripts/
│   ├── wait_for_services.sh
│   ├── e2e_check.py        # end-to-end verification script
│   ├── collect_evidence.py # captures API responses, SQL query outputs, metrics into docs/evidence/
│   └── chaos/              # failure-injection scripts used by scenario tests
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── docs/
    ├── REPORT.md
    ├── TEST_REPORT.md
    ├── VIVA_GUIDE.md
    ├── diagrams/           # Graphviz .dot sources + rendered .png
    ├── evidence/           # real outputs captured from runs
    └── screenshots/        # (student adds; list required shots in REPORT)
```

---

## 5. DATA SOURCES (Ingestion — 15 marks)

### 5.1 Zones (`common/zones.py`)
Use a **Colombo, Sri Lanka** bounding box (lat 6.86–6.98, lon 79.84–79.90), split into a 3×2 grid of named zones:
Fort, Pettah, Kollupitiya, Bambalapitiya, Borella, Wellawatte. Points outside the box map to `OUT_OF_AREA`.
The function must be pure, deterministic and unit-tested. Currency is LKR.

### 5.2 Streaming source (`producers/gps_producer.py` + `fleet_simulator.py`)
- Simulate `NUM_VEHICLES=25` vehicles, each with a fixed driver and a **state machine**: `idle → enroute → on_trip → idle`, with realistic dwell times and movement (small lat/lon steps, speed 0 when idle, 10–60 km/h when moving).
- Emit one event per vehicle every `EMIT_INTERVAL_SEC=2` (with jitter) to `fleet.telemetry`, **key = vehicle_id**.
- Event schema: `event_id (uuid), trip_id (nullable when idle), driver_id, vehicle_id, lat, lon, speed, status, fare, event_time, sim_ts, sim_date, sim_hour`.
- **Fare rule:** `fare > 0` only on the **trip-end event** (transition on_trip → idle). Fare = base 150 LKR + 100 LKR/km × trip distance ± noise. All other events have fare = 0. This makes revenue computable without double counting.
- Configurable behaviour to make processing meaningful:
  - `BAD_EVENT_RATE=0.02`: emit malformed events (null vehicle_id, negative speed, lat/lon out of range, unknown status).
  - `LATE_EVENT_RATE=0.01`: events with `event_time` delayed 30–90 s, to demonstrate watermarks.
  - `DUPLICATE_RATE=0.01`: resend an identical `event_id`, to demonstrate dedup.
  - `LAZY_VEHICLES=3`: vehicles that stay idle much longer, so idle alerts fire during a demo.
- Robustness: producer `acks=all`, retries, `enable.idempotence=true`, graceful shutdown on SIGTERM (flush), reconnect/backoff if Kafka is not up yet, delivery callbacks that log failures.
- Metrics on port 8001: `producer_events_sent_total{status}`, `producer_send_errors_total`, `producer_bad_events_injected_total`, `producer_last_send_timestamp`.
- Library: `confluent-kafka` (preferred) or `kafka-python`.

### 5.3 Daily-batch source (`producers/expense_producer.py`)
- Runs continuously. At the end of each simulated day D, it writes `expenses_<D>.csv` into `/data/landing/`, writing to a temp file first and then **atomically renaming** it so the sensor never sees a partial file.
- Columns: `vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag, report_date`.
- Values are realistic in LKR. About 15% of vehicles are "high-cost" (big maintenance, service_flag = 1), so some become unprofitable. Include 1–2 dirty rows (a missing vehicle_id or a negative cost) for validation to catch.
- Optional flag `SKIP_DAY=<date>` to simulate a missing file (used in failure tests).
- A `--backfill <start> <end>` CLI mode generates files for past days, for replay demos.
- Metrics on port 8002: `expense_files_written_total`, `expense_last_file_timestamp`.

---

## 6. PROCESSING (15 marks)

### 6.1 Speed layer — `streaming/stream_job.py`
1. Read from Kafka `fleet.telemetry` (startingOffsets `latest` for the live demo; configurable).
2. Parse the JSON against the Spark schema from `common/schemas.py`.
3. **Validate/clean:** rows failing the rules go to a quarantine sink (Postgres table `rejected_events` with reason, and/or the Kafka DLQ topic). Valid rows continue.
4. **Dedup:** `withWatermark("event_time", "2 minutes").dropDuplicates(["event_id"])` (use `dropDuplicatesWithinWatermark` if available in the pinned version).
5. **Enrich:** add `zone` using a UDF that wraps `common/zones.py`. Also add `is_active = status in (enroute, on_trip)`.
6. **Sinks (separate streaming queries, each with its own checkpoint dir):**
   - **(a) Master dataset:** cleaned raw events → Parquet, `partitionBy("sim_date")`, trigger every 30 s.
   - **(b) Zone metrics:** 1-minute tumbling windows on `event_time` with a 2-minute watermark, grouped by `window, zone`. Compute:
     - `active_vehicles` (approx count distinct vehicle_id where is_active)
     - `idle_vehicles`
     - `idle_ratio`
     - `trips_completed` (count fare > 0)
     - `earnings` (sum fare)
     - `avg_speed`

     Write via `foreachBatch` with an **idempotent UPSERT** into `realtime_zone_metrics` (primary key `window_start, zone`).
   - **(c) Vehicle status and idle alerts:** in `foreachBatch`, take the latest event per vehicle in the micro-batch and UPSERT into `vehicle_status`, maintaining `idle_since` (set when the status becomes idle, cleared otherwise). Then insert into `idle_alerts` for vehicles idle longer than `IDLE_ALERT_MINUTES` (default 3 real minutes, i.e. about 7 simulated hours). Use a partial unique constraint so there is only **one open alert per vehicle**. Resolve the alert when the vehicle becomes active again.
7. Every `foreachBatch` logs a structured line (`stage=processing, batch_id, rows_in, rows_valid, rows_rejected, duration_ms`) and updates Prometheus metrics on port 8003:
   - `stream_batches_total`, `stream_rows_processed_total`, `stream_rows_rejected_total`
   - `stream_batch_duration_seconds` (histogram)
   - `stream_last_batch_timestamp`
   - `idle_alerts_open` (gauge)
8. Put all transformation logic in `streaming/transforms.py` as **pure functions over DataFrames**, so they can be unit tested with a local SparkSession. The batch job **reuses** `zones.py` and the validation rules. This is the explicit mitigation for Lambda's "two code paths" weakness; mention it in the report.

### 6.2 Batch layer — `batch/profitability_job.py` (runs from Airflow with `--date D`)
1. Read the Parquet partition `sim_date = D` (the master dataset, i.e. the source of truth).
2. **Recompute** per vehicle from raw events:
   - `revenue` = sum of fare, after dedup by trip_id
   - `trips`
   - `active_minutes` and `idle_minutes` (from event counts × interval)
   - `utilization` = active / total
   - earnings by `sim_hour` bucket (time-of-day)
3. Read that day's validated expenses from Postgres `daily_expenses`.
4. **Join** trips with expenses on `vehicle_id` (a left join from vehicles, so a vehicle missing an expense row is flagged `MISSING_EXPENSE`, not dropped).
5. Compute:
   - `total_cost` = fuel + maintenance
   - `profit` = revenue − total_cost
   - `margin` = profit / revenue (guard against division by zero)
   - `cost_per_km`
   - `revenue_per_km`
6. **"Becoming unprofitable" rule:** mark `is_unprofitable = profit < 0`. Mark `trend = DECLINING` if the margin has dropped for 2 consecutive days, or `AT_RISK` if margin < `MARGIN_THRESHOLD` (0.10) for 2 consecutive days. Compare against previous rows in `daily_vehicle_profitability`.
7. Write to `daily_vehicle_profitability` with an idempotent UPSERT (primary key `report_date, vehicle_id`), so **re-running a day (backfill/replay) gives the same result**. This is Lambda's recomputation property; demonstrate it in a test.
8. Also write `daily_zone_summary` (zone × sim_hour earnings/utilization from the batch view). The report compares batch vs speed-layer totals for the same day, to show speed-layer approximation vs batch correctness.

### 6.3 Airflow DAG — `airflow/dags/daily_reconciliation_dag.py`
- Schedule: every `SIM_DAY_SECONDS` (timedelta), `catchup=False`, `max_active_runs=1`. Target date = `previous_sim_date()`, computed in a task and passed by XCom. Also support a manual trigger with `conf={"date": "YYYY-MM-DD"}` for backfill.
- Tasks:
  1. `compute_target_date`
  2. `wait_for_expense_file`: FileSensor, `mode="reschedule"`, `poke_interval=15s`, timeout = 1 sim day
  3. `validate_expense_file`: schema/type/range checks; bad rows go to `rejected_expenses`; fail if more than 20% of rows are bad
  4. `load_expenses_to_postgres`: idempotent upsert
  5. `check_master_data_available`: the Parquet partition exists and has at least N rows
  6. `run_profitability_job`: BashOperator with `spark-submit`
  7. `build_daily_report`: writes `/data/reports/profitability_<D>.html` and `.csv`
  8. `data_quality_checks`: e.g. every vehicle seen in telemetry has a row; revenue ≥ 0; row counts are logged
  9. `record_pipeline_run`: writes to the `pipeline_runs` table
- `retries=2`, `retry_delay` short. `on_failure_callback` logs a structured ERROR and inserts into `pipeline_alerts`. Use SLA/timeout on the sensor to represent "file late/missing".
- Move the processed file to `/data/processed/` after loading (keep the original for replay).

---

## 7. STORAGE & SERVING (10 marks)

### 7.1 `sql/init.sql`
Tables with primary keys, indexes and comments:
- `realtime_zone_metrics`
- `vehicle_status`
- `idle_alerts` (id, vehicle_id, idle_since, detected_at, resolved_at, status)
- `rejected_events`
- `daily_expenses`
- `rejected_expenses`
- `daily_vehicle_profitability`
- `daily_zone_summary`
- `pipeline_runs`
- `pipeline_alerts`

Add views:
- `v_fleet_now`: latest window totals
- `v_unprofitable_vehicles`: latest day

### 7.2 FastAPI — `api/main.py` (port 8000, OpenAPI docs at /docs)
- `GET /health`: checks Postgres connectivity, the Kafka broker (metadata request), and data freshness (`now − max(event_time in vehicle_status) < FRESHNESS_SECONDS`). Returns 200 or 503 with details.
- `GET /metrics/fleet`: current active vehicles, idle vehicles, idle ratio, trips in the last real hour and last sim-hour equivalent, earnings last hour.
- `GET /metrics/zones?minutes=15`: per-zone utilization and earnings over recent windows.
- `GET /metrics/time-of-day?date=`: earnings/utilization by sim_hour (from the batch table).
- `GET /alerts/idle?status=open`
- `GET /reports/profitability?date=`: JSON rows. `GET /reports/profitability/{date}/html` serves the report file.
- `GET /vehicles/unprofitable?date=`
- `GET /prometheus`: or mount `prometheus_client` at `/metrics-prom` for API request metrics.
- Use pydantic response models, proper 404/422 handling, and a structured access log.

### 7.3 Daily report (`batch/report_builder.py`)
A clean, self-contained HTML (with a CSV alongside) containing:
- fleet KPIs for the day
- a per-vehicle profitability table sorted by profit, with unprofitable/at-risk rows highlighted
- zone × time-of-day earnings
- the list of idle alerts that day
- data quality summary (rows rejected, missing expenses)
- a batch vs speed-layer reconciliation line

This report file is the "consolidated report" deliverable.

### 7.4 Grafana (provisioned automatically, no manual clicks)
Dashboard "Fleet Operations":
- active vehicles
- idle ratio
- earnings by zone (time series)
- trips/hour
- open idle alerts table
- latest-day unprofitable vehicles table

Dashboard "Pipeline Health":
- producer rate
- consumer lag
- stream batch duration
- rejected rows
- last-event age
- Airflow DAG run status from `pipeline_runs`

---

## 8. OBSERVABILITY (10 marks)

1. **Structured JSON logs** in every service with common fields: `ts, level, service, stage (ingestion|processing|storage|serving|orchestration), event, run_id/batch_id, message`, plus context. Provide `make logs` and example `docker compose logs` + `jq` filters in the README.
2. **Metrics:** the producer, stream job, API and expense producer expose Prometheus metrics. kafka-exporter provides consumer lag. Prometheus scrapes all of them (`monitoring/prometheus.yml`).
3. **Alert rules** (`monitoring/alert_rules.yml`), at least:
   - `NoTelemetryReceived`: `time() - producer_last_send_timestamp > 120` **or** stream last-batch age > 120 s, `for: 30s`
   - `HighRejectRate`: rejected / processed > 5% over 2 m
   - `StreamBatchSlow`: p95 batch duration > 20 s
   - `ConsumerLagHigh`: lag > 5000
   - `ExpenseFileLate`: `time() - expense_last_file_timestamp > 1.5 × SIM_DAY_SECONDS`
4. **Health checks:** Docker `healthcheck` on every service; the `/health` endpoint; the Airflow sensor timeout plus failure callback.
5. **Tracing-lite:** propagate `event_id` from producer logs through to rejected/processed logs, and `run_id` through all Airflow tasks and the Spark batch job, so one event or one daily run can be followed across stages. Show one traced example in the report.

---

## 9. TESTING (every test case must appear in TEST_REPORT.md)

### 9.1 Test ID scheme and requirement mapping
IDs follow `TC-<AREA>-<NNN>`, where AREA is one of: `ING` (ingestion), `STR` (stream processing), `BAT` (batch), `ORC` (Airflow), `SRV` (storage/API), `OBS` (observability), `E2E`, `FT` (failure/resilience), `NFR` (performance/non-functional). Every test maps to a requirement ID `REQ-xx`. Define the requirement list in TEST_REPORT §2, derived from the brief (e.g. REQ-01 streaming source, REQ-02 daily batch source, REQ-03 join of both sources, REQ-04 windowed aggregation, REQ-05 real-time API, REQ-06 idle alert, REQ-07 daily profitability report, REQ-08 structured logging, REQ-09 alert/health rule, REQ-10 reproducibility, REQ-11 replay/recompute, REQ-12 data quality handling). Put the pytest test ID in each test's docstring.

### 9.2 Minimum test cases (add more where useful)

**Unit (pytest, no Docker):**
- `sim_clock`: day boundaries, previous day, sim_hour mapping
- `zones`: each zone centre, the boundaries, out-of-area
- validation rules: each bad-event type is rejected with the correct reason; valid events pass
- fleet simulator: legal state transitions only; fare only on trip end; lazy vehicles stay idle longer; deterministic with a seed
- expense producer: columns, atomic write, dirty-row injection, skip-day flag
- stream transforms with a local SparkSession: dedup by event_id; window aggregates on a hand-made micro dataset give exact expected numbers; late events beyond the watermark are dropped; idle_since logic
- profitability: hand-calculated fixture (e.g. 3 vehicles) where revenue, cost, profit, margin, cost_per_km, is_unprofitable, trend (DECLINING/AT_RISK) and MISSING_EXPENSE match exact expected values; divide-by-zero guard
- expense validation: good file passes; > 20% bad fails; bad rows are quarantined
- report builder: HTML contains the required sections; unprofitable rows are highlighted
- API with TestClient and a mocked/test DB: every endpoint returns the right schema; 404 for a missing date; 422 for a bad date; `/health` returns 503 when the DB is down
- logging: output is valid JSON with the required fields

**Integration (Docker stack up; mark with `@pytest.mark.integration`):**
- producer → Kafka: messages arrive, keys = vehicle_id, spread across the 6 partitions, the same vehicle always lands in the same partition
- Kafka → Spark → Postgres: rows appear in `realtime_zone_metrics` within N seconds
- Parquet partitions for the current sim_date are created
- idle alert opens for a lazy vehicle and resolves when it moves
- rejected events land in `rejected_events`
- Airflow DAG run succeeds for one sim day (trigger via Airflow REST API/CLI, poll until done)
- Prometheus targets are all UP; alert rules are loaded
- Grafana datasources and dashboards are provisioned (Grafana HTTP API)

**E2E:**
- `TC-E2E-001`: full flow for one complete simulated day. Stream → Parquet + real-time tables → expense file → DAG → profitability rows → HTML report → API returns them. Assert the business question is answered (utilization by zone now, plus a list of unprofitable vehicles).
- `TC-E2E-002`: **batch vs speed reconciliation.** Total day earnings from the batch equal the sum of speed-layer windows within a tolerance. Explain any difference (late data, dedup).

**Failure / resilience (`scripts/chaos/`):**
- stop the GPS producer → `NoTelemetryReceived` fires and `/health` returns 503 (degraded) within the expected time; restart → it recovers
- restart the stream job → it resumes from the checkpoint with no duplicate rows (UPSERT idempotency)
- Kafka broker restart → producer retries and no crash
- missing expense file (`SKIP_DAY`) → sensor times out, the DAG fails cleanly, `pipeline_alerts` has a row, `ExpenseFileLate` fires
- corrupt expense file → validation quarantines it or fails the run per the rule
- **replay/backfill:** re-run the DAG for a past date twice → identical results (idempotent recompute)
- high bad-event rate (`BAD_EVENT_RATE=0.2`) → `HighRejectRate` fires

**Non-functional (lightweight):**
- throughput: increase `NUM_VEHICLES` to 200 for 3 minutes; record events/s, batch duration and lag
- end-to-end latency: event_time → row visible in Postgres, p50/p95 from a sample
- resource usage from `docker stats` snapshot

### 9.3 How tests are run and captured
- `make test-unit`, `make test-integration`, `make test-e2e`, `make test-chaos`, `make test-all`
- pytest outputs `docs/evidence/tests/junit-*.xml` and `docs/evidence/tests/report-*.html` (pytest-html), plus coverage (`--cov`, HTML plus a term summary; aim for ≥ 70% on `common/`, `streaming/transforms.py`, `batch/`, `api/`)
- Chaos and non-functional scenario scripts write their own evidence (timestamps, alert states from the Prometheus API, SQL outputs) to `docs/evidence/scenarios/<TC-ID>.json|txt`
- Record the **actual** pass/fail. If a test fails: fix the code and re-run. If it still fails, report it honestly with the root cause.

---

## 10. FINAL DELIVERABLE A — `docs/TEST_REPORT.md` (detailed, every test case)

Structure:
1. **Summary:** date, environment (OS, Docker version, CPU/RAM, image versions), totals (executed/passed/failed/skipped/not executed), coverage %, one-paragraph conclusion.
2. **Requirements list** (REQ-xx) and a **traceability matrix**: requirement ↔ test cases ↔ status.
3. **Test strategy:** levels (unit/integration/E2E/failure/NFR), tools, test data design (seeded simulator, hand-calculated fixtures), environments.
4. **Detailed test cases.** One subsection per test case, **all of them**, using this template:

```
### TC-XXX-NNN — <title>
| Field | Value |
|---|---|
| Requirement(s) | REQ-.. |
| Level / Type | Unit / Integration / E2E / Failure / NFR |
| Component | ... |
| Objective | what this proves and why it matters |
| Preconditions | stack state, config, data |
| Test data / Input | exact input (fixture, env overrides) |
| Steps | 1. ... 2. ... |
| Expected result | exact expected values / behaviour |
| Actual result | real observed output (numbers, excerpt of logs/JSON, ≤ 15 lines) |
| Status | PASS / FAIL / NOT EXECUTED |
| Evidence | path(s) under docs/evidence/, pytest node id |
| Notes | defects found + fix, limitations |
```

5. **Scenario test narratives** (failure tests): a timeline with timestamps of injection → detection (alert fired at T+x s) → recovery.
6. **Performance results** table (plus a chart PNG generated with matplotlib from real numbers).
7. **Defect log:** every bug found during the build/testing, its root cause and fix (this shows engineering rigour).
8. **Coverage report** summary table per module.
9. **Known gaps / untested areas** and why.

Generate this file **from the real results**. A helper script `scripts/build_test_report.py` may parse the JUnit XML and evidence files to fill the status/actual fields, with the narrative written by you. Also export `docs/TEST_REPORT.pdf` if the tooling works.

---

## 11. FINAL DELIVERABLE B — `docs/REPORT.md` (the 8–15 page technical report)

Write in clear academic-technical English, third person or "we". Aim for about 5,000–6,500 words plus figures. Sections:

1. **Title page:** project title, module, student name(s)/ID placeholder `<<NAME>>`, date.
2. **Executive summary** (½ page).
3. **Use case and business requirements:** translate the business question into functional requirements (REQ-xx) and non-functional requirements (latency targets for the speed layer: seconds; batch: once per sim day; correctness, replay, cost, scale). State assumptions and the simulated clock.
4. **Architecture decision: Lambda vs Kappa** (the most important section, 20 marks, about 2–3 pages):
   - explain both architectures briefly, with a small diagram of each
   - a comparison table on **latency, replay/reprocessing, cost, consistency/correctness, operational complexity, fit with a daily file source, fit with the team's skills and timeline**
   - justification for Lambda, tied to *this* use case: two genuinely different latency needs; the daily file is naturally batch; profitability needs complete, correctable, recomputable data (late GPS events, corrected cost files); the Parquet master dataset enables recomputation
   - **the rejected alternative (Kappa):** how it *would* work (the CSV pushed into a Kafka topic, stream–static/stream–stream join, long retention for replay), its genuine advantages (one code path), and why it was rejected here
   - honest trade-offs: two code paths, and how this project mitigates that through shared modules; duplicated compute; eventual consistency between layers (shown by the reconciliation test)
5. **System architecture:** a full diagram (Graphviz, rendered PNG) covering ingestion, speed, batch, serving and observability; a data-flow walk-through of one telemetry event and one daily file; a Kafka topic/partition design and why key = vehicle_id; the storage schema (an ERD-style table list).
6. **Technology stack and justification:** one paragraph per component, **tied to use-case constraints, not popularity**, with the alternatives considered (Storm vs Spark, Cassandra vs Postgres, HDFS/S3 vs local Parquet, Flink as a Kappa option).
7. **Implementation:**
   - producers (state machine, fault injection)
   - stream job (validation, dedup, watermark, windows, foreachBatch upserts, idle state)
   - batch job (recompute, join, profitability and trend rule, with the formula)
   - Airflow DAG (a task graph figure)
   - API and report
   - short code excerpts (≤ 15 lines each) for the key logic
8. **Observability design:** what is measured, how, and **why each signal matters** (a table: signal → failure it detects → alert rule → threshold rationale); a logging schema; a traced example of one event and one DAG run.
9. **Results:**
   - real API JSON outputs
   - a sample of the daily profitability report (a table excerpt with real numbers)
   - unprofitable vehicles found
   - zone × time-of-day earnings chart (matplotlib from real data)
   - the batch vs speed reconciliation result
   - alert firing evidence
   - a summary of the test results (link to TEST_REPORT)
   - performance numbers
   - a **screenshot checklist** with placeholders, e.g. `![Grafana Fleet dashboard](screenshots/grafana_fleet.png)`, and a list telling the student exactly which screenshots to capture during the demo (Grafana x2, Airflow DAG graph + run, Prometheus alerts page firing, FastAPI /docs, HTML report). If headless capture with Playwright works in this environment, capture them automatically; otherwise leave the placeholders.
10. **Limitations, trade-offs and production-scale changes:**
    - single broker (no replication)
    - local Parquet instead of S3/HDFS
    - local-mode Spark
    - at-least-once + idempotent upserts vs true exactly-once
    - no schema registry
    - simulated data realism
    - security (no TLS/auth)

    At scale: a Kafka cluster with RF=3, Avro + Schema Registry, Spark on Kubernetes/EMR/Databricks, Delta Lake/Iceberg for the lake, a TimescaleDB/ClickHouse/Druid serving store, Airflow on K8s/Celery, Alertmanager → PagerDuty/Slack, OpenTelemetry tracing, CI/CD, data contracts. Also: what the team would do differently.
11. **Conclusion.**
12. **References** (official docs: Kafka, Spark, Airflow, Nathan Marz on Lambda, Jay Kreps "Questioning the Lambda Architecture" on Kappa). Only cite real, well-known sources.
13. **Appendices:** how to run (short), the config table (all env vars and defaults), an individual contributions statement template (`<<NAME>> – components`), and an AI-assistance disclosure.

Diagrams: write Graphviz `.dot` files in `docs/diagrams/` and render them to PNG (`dot -Tpng`). Required: overall architecture, Lambda vs Kappa comparison, Airflow DAG, data model, observability flow.

PDF: try `pandoc docs/REPORT.md -o docs/REPORT.pdf` (with a PDF engine such as weasyprint, wkhtmltopdf or xelatex, whichever installs). Make sure the images embed. If none work, produce `docs/REPORT.html` and state that the PDF must be exported from a browser.

---

## 12. EXECUTION PHASES (verify each before moving on)

| Phase | Build | Verification gate |
|---|---|---|
| P0 | CLAUDE.md, PROGRESS.md, repo skeleton, `.env.example`, `common/` modules + unit tests | `pytest tests/unit` passes for common |
| P1 | docker-compose (Kafka, Postgres, Prometheus, Grafana, kafka-exporter), `init.sql`, Makefile | `docker compose up -d`; all healthy; tables exist |
| P2 | GPS producer + simulator + tests; topic creation (6 partitions + DLQ) | consume 20 messages with kafka-console-consumer; partition spread shown |
| P3 | Stream job: validate, dedup, enrich, 3 sinks, metrics, logs + transform unit tests | rows in `realtime_zone_metrics`, `vehicle_status`, Parquet files present, rejected rows present, an idle alert opens |
| P4 | Expense producer; Airflow image + DAG; profitability job; report builder + tests | one DAG run SUCCESS; `daily_vehicle_profitability` filled; HTML report exists; re-run gives identical rows |
| P5 | FastAPI + tests | curl every endpoint; save outputs to docs/evidence/api/ |
| P6 | Prometheus rules, Grafana provisioning, `/health`, tracing fields | all targets UP; stop the producer → the alert fires (capture from the Prometheus API) |
| P7 | Integration, E2E, chaos and NFR tests; `collect_evidence.py` | `make test-all` executed; evidence files written |
| P8 | README (architecture summary, prerequisites, quick start, demo script with timings, troubleshooting, how to reproduce results, simulated clock) | follow the README from `docker compose down -v` → working system |
| P9 | Diagrams, charts, `TEST_REPORT.md`, `REPORT.md`, PDFs | every number cross-checked with evidence files |
| P10 | `docs/VIVA_GUIDE.md`: for each core file, what it does, key design decisions, and likely examiner questions with answers (Lambda vs Kappa, watermark, idempotency, partitioning, exactly-once, why Postgres, how an alert fires). Also a **5–10 minute demo script** (what to show, in what order, what to say) | — |

At the end, print a final checklist mapping every rubric criterion and every brief deliverable to the file(s) that satisfy it, with any remaining TODOs for the student (screenshots, names, demo video).

---

## 13. QUALITY BAR (Code Quality — 5 marks)
- Type hints and docstrings on public functions; modules small and single-purpose.
- `ruff` (or flake8) + `black` config; run them and fix the issues.
- No secrets in code; `.env.example` documents every variable.
- Deterministic seeds for tests/demos (`SEED` env).
- Graceful shutdown and restart safety (checkpoints, idempotent writes).
- `make up / down / reset / logs / demo / test-* / report` targets.

Begin with Phase P0 now.
