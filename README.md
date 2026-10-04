# Ride-Hailing Fleet Operations — Lambda Architecture Data Pipeline

An end-to-end, runnable big-data pipeline that answers one operational question:

> **What is fleet utilization and earnings by area/time-of-day right now, and which vehicles are
> becoming unprofitable once yesterday's fuel/maintenance costs are factored in?**

The question has **two halves with two different latency requirements**, and that is precisely
why the architecture is **Lambda** rather than Kappa. The live half is answered in seconds by a
Spark Structured Streaming speed layer; the profitability half is answered once per (simulated)
day by an Airflow-orchestrated Spark batch layer that *recomputes* from an immutable Parquet
master dataset.

*Applied Big Data Engineering — Mini Project. Student: `<<NAME>>` (`<<STUDENT_ID>>`).*

---

## 1. Architecture at a glance

```
                        ┌──────────────── SPEED LAYER (seconds) ────────────────┐
 gps_producer  ──────▶  Kafka fleet.telemetry (6 partitions, key = vehicle_id)  │
 25 vehicles, 2 s          │                                                    │
 + fault injection         ▼                                                    │
                    Spark Structured Streaming (local[*])                       │
                      ├─ validate  ──▶ rejected_events + Kafka DLQ              │
                      ├─ dedup      (dropDuplicatesWithinWatermark, 2 min)      │
                      ├─ enrich     (zone via SHARED common/zones.py)           │
                      ├─ sink (a) ──▶ Parquet master dataset (sim_date=…)       │
                      ├─ sink (b) ──▶ realtime_zone_metrics  (1-min UPSERT)     │
                      └─ sink (c) ──▶ vehicle_status + idle_alerts              │
                        └───────────────────────────────────────────────────────┘

                        ┌──────────── BATCH LAYER (one simulated day) ──────────┐
 expense_producer ──▶ /data/landing/expenses_<D>.csv (atomic rename)            │
                        Airflow DAG `daily_reconciliation`                      │
                          sensor ▶ validate ▶ load ▶ check lake ▶ spark-submit  │
                            └─▶ profitability_job: Parquet[D] ⋈ daily_expenses  │
                                 ──▶ daily_vehicle_profitability                │
                                 ──▶ daily_zone_summary                         │
                            └─▶ report_builder ──▶ HTML + CSV daily report      │
                        └───────────────────────────────────────────────────────┘

 SERVING        PostgreSQL 16  ·  FastAPI :8000  ·  Grafana :3000
 OBSERVABILITY  JSON logs · Prometheus :9090 (9 alert rules) · kafka-exporter :9308
```

Full diagrams: [`docs/diagrams/`](docs/diagrams/) (Graphviz sources + rendered PNGs).
Architecture rationale, including the rejected Kappa alternative: [`docs/REPORT.md`](docs/REPORT.md) §4.

---

## 2. The simulated clock — read this first

A daily batch layer is meaningless in a demo if a "day" takes 24 hours, so:

| | |
|---|---|
| **1 simulated day** | `SIM_DAY_SECONDS` real seconds (**default 600 s = 10 real minutes**) |
| **Simulation start** | `SIM_START_DATE` (default `2026-09-01`) |
| **Shared epoch** | pinned once in `/shared/sim_epoch.txt` so every container agrees |
| **1 real second** | = 144 simulated seconds |
| **1 simulated hour** | = 25 real seconds |

Every telemetry event carries **both clocks**:

* `event_time` — **real** wall-clock UTC. Structured Streaming windows and watermarks use this,
  so a "2-minute watermark" really means two minutes and the live dashboard moves in real time.
* `sim_ts` / `sim_date` / `sim_hour` — the **simulated** clock. The Parquet lake is partitioned
  by `sim_date` and time-of-day analysis groups by `sim_hour`, so a full 24-hour business profile
  exists after ten real minutes.

This dual-time design and its trade-off are discussed in `docs/REPORT.md` §3.

---

## 3. Prerequisites

| Requirement | Version used | Notes |
|---|---|---|
| Docker Engine | 27.4.0 | |
| Docker Compose | v2.31.0 | `docker compose`, not `docker-compose` |
| Docker memory | **≥ 6 GB** | the stack's `mem_limit`s total **5.83 GB**, measured peak **3.2 GB** (TC-NFR-003) |
| Disk | ~12 GB | images are ~6 GB (PySpark is 317 MB per image) |
| Host RAM | **≥ 12 GB** | see the WSL note below if you are on Windows |
| GNU make | optional | Windows users: `.\make.ps1 <target>` instead |

Nothing needs to be installed on the host: no Python, no Java, no Spark. Everything, **including
the test suite**, runs in containers.

> **Windows / WSL2 users, please read.** By default the WSL2 VM claims a large share of host RAM
> and does not hand it back promptly. On the 15.6 GB machine this project was built on, that drove
> free physical memory below 0.5 GB and made the Docker daemon unresponsive three times. The fix
> is a one-line cap, supplied as [`.wslconfig.recommended`](.wslconfig.recommended):
>
> ```powershell
> Copy-Item .wslconfig.recommended "$env:USERPROFILE\.wslconfig"
> wsl --shutdown          # required for the change to take effect
> ```
>
> It caps the VM at 6 GB with `autoMemoryReclaim=gradual`, which is enough for the whole stack
> (limits total 5.83 GB, measured peak 3.2 GB). To revert, delete the file and run
> `wsl --shutdown` again.

> **First build takes 10–20 minutes** — PySpark is downloaded three times (streaming, Airflow,
> tests images). Later builds are cached.

---

## 4. Quick start

```bash
cp .env.example .env          # or: make env
make build                    # ~10-20 min on a cold cache
make up                       # starts everything and waits for health
make open                     # prints every UI URL
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
.\make.ps1 build
.\make.ps1 up
.\make.ps1 open
```

Without `make`, the equivalent is:

```bash
docker compose build && docker compose up -d && bash scripts/wait_for_services.sh
```

### What to expect, and when

| Elapsed | What happens |
|---|---|
| 0:00 | Kafka forms its KRaft quorum; Postgres runs `sql/*.sql`; topics created (6 partitions) |
| 0:30 | `gps-producer` starts emitting; `data-init` has created `/data` |
| 1:30 | first Spark micro-batches; rows appear in `realtime_zone_metrics` and `vehicle_status` |
| 2:00 | first Parquet files under `/data/lake/telemetry/sim_date=2026-09-01` |
| **3:00** | **first idle alert opens** for a lazy vehicle (`IDLE_ALERT_MINUTES=3`) |
| **10:00** | **first simulated day ends** → expense CSV lands → Airflow DAG runs → profitability + HTML report |
| 20:00 | second day reconciled; `trend` (DECLINING / AT_RISK) becomes meaningful from day 3 |

Verify at any point:

```bash
make e2e-check     # numbered checklist with real numbers, exit code 0 = healthy
```

### URLs

| Service | URL | Credentials |
|---|---|---|
| FastAPI docs | http://localhost:8000/docs | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | — |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| Spark UI | http://localhost:4040 | — |

> Airflow is on **8082**, not the usual 8080, because 8080 is frequently occupied on a developer
> laptop (it was on the build machine). Change `AIRFLOW_PORT` in `.env` if you prefer.

---

## 5. Demo script (5–10 minutes)

```bash
make demo          # or: bash scripts/demo.sh
```

The script pauses between steps and prints *what to show*, *what to say* and *the command*. The
sequence is: topic/partition layout → speed-layer logs and quarantine → live API answer → idle
alert → Airflow DAG and the Parquet lake → unprofitable vehicles → Prometheus/Grafana → stop the
producer and watch two independent detectors fire → the architecture argument.

Full narration and likely examiner questions: [`docs/VIVA_GUIDE.md`](docs/VIVA_GUIDE.md).

---

## 6. Everyday commands

```bash
make ps                       # container status and health
make logs                     # all JSON logs
make logs-stream-job          # one service
make topics                   # Kafka partition layout
make consume                  # 20 live messages with partition + key
make psql                     # interactive psql
make sql Q="select * from v_fleet_now"
make trigger-dag D=2026-09-01 # manual DAG run for a simulated date
make backfill D=2026-09-01    # replay that day twice, prove identical results
make evidence                 # capture API/SQL/metrics into docs/evidence/
make down                     # stop, keep data
make reset                    # stop and DELETE all volumes
```

### Reading the structured logs

Every service emits one JSON object per line with the same envelope
(`ts, level, service, stage, event, run_id, message` + context), so one `jq` filter works
everywhere:

```bash
# every micro-batch, with row counts and duration
docker compose logs stream-job | grep -o '{.*}' | jq -c 'select(.event=="batch_complete")
  | {ts, sink, batch_id, rows_written, duration_ms}'

# everything that happened to one event id (tracing-lite)
docker compose logs | grep -o '{.*}' | jq -c 'select(.event_id=="<uuid>")'

# only the ingestion stage, only warnings and worse
docker compose logs | grep -o '{.*}' | jq -c 'select(.stage=="ingestion" and .level!="INFO")'

# follow one Airflow run across Airflow, Spark and Postgres
docker compose logs | grep -o '{.*}' | jq -c 'select(.run_id=="<run_id>")'
```

---

## 7. Tests

| Command | What it runs | Needs the stack? |
|---|---|---|
| `make test-unit` | pure-Python + local-SparkSession tests, with coverage | no |
| `make test-integration` | real Kafka / Spark / Postgres / Prometheus / Grafana assertions | yes |
| `make test-e2e` | full flow for one simulated day + batch-vs-speed reconciliation | yes |
| `make test-nfr` | throughput, end-to-end latency, resource snapshot | yes |
| `make test-chaos` | 7 failure-injection scenarios (stops/starts real containers) | yes |
| `make test-all` | all of the above | yes |

Results land in `docs/evidence/tests/` (JUnit XML, pytest-html, coverage) and
`docs/evidence/scenarios/` (one JSON per chaos scenario, with detection latencies).
`make report` turns those artefacts into the tables embedded in
[`docs/TEST_REPORT.md`](docs/TEST_REPORT.md).

> Tests run **inside** the `fleet-tests` image because PySpark 3.5 does not support Python 3.13,
> which is what modern hosts ship. This guarantees the unit tests exercise the same interpreter,
> JVM and jar set as production.

---

## 8. Reproducing the reported results

Every number in `docs/REPORT.md` and `docs/TEST_REPORT.md` comes from a file under
`docs/evidence/`. To regenerate them from scratch:

```bash
make reset                 # clean slate: volumes deleted, clock restarts at 2026-09-01
make up
# wait ~25 minutes so at least two simulated days are reconciled
make e2e-check             # confirm every stage is alive
make test-all              # runs every suite, writes evidence
make evidence              # API responses, SQL outputs, Prometheus queries
make charts                # matplotlib figures from the live database
make diagrams              # Graphviz PNGs
make report                # rebuild the test-result tables
```

`SEED=42` makes the simulator deterministic, so the *shape* of the results reproduces; exact
timestamps and therefore exact revenue figures will differ between runs, which is expected and
is why the reports quote captured evidence rather than hard-coded values.

---

## 9. Configuration

All configuration lives in `.env` and is read by `common/config.py`. Nothing is hard-coded.
The most useful knobs:

| Variable | Default | Effect |
|---|---|---|
| `SIM_DAY_SECONDS` | 600 | length of a simulated day; lower = faster demo, higher = more data per day |
| `NUM_VEHICLES` | 25 | fleet size (200 for the throughput test) |
| `EMIT_INTERVAL_SEC` | 2 | telemetry frequency per vehicle |
| `BAD_EVENT_RATE` | 0.02 | malformed-event injection (chaos test raises it to 0.2) |
| `LATE_EVENT_RATE` | 0.01 | events delayed 30–90 s, to exercise the watermark |
| `DUPLICATE_RATE` | 0.01 | repeated `event_id`, to exercise dedup |
| `LAZY_VEHICLES` | 3 | vehicles that idle long enough to trigger alerts |
| `IDLE_ALERT_MINUTES` | 3 | idle-alert threshold (real minutes) |
| `MARGIN_THRESHOLD` | 0.10 | margin below which a vehicle is `AT_RISK` |
| `MAX_BAD_EXPENSE_ROW_PCT` | 0.20 | above this share of bad rows, the DAG refuses the file |
| `SKIP_DAY` | *(empty)* | suppress one day's expense file, to exercise the sensor timeout |

The complete table (all 80+ settings) is in `docs/REPORT.md`, Appendix B.

---

## 10. Repository layout

```
common/      config · sim_clock · zones · schemas · logging_setup · db · metrics
             ↑ shared by BOTH layers — the mitigation for Lambda's two-code-path weakness
producers/   fleet_simulator (pure state machine) · gps_producer (stream) · expense_producer (file)
streaming/   transforms.py (pure DataFrame functions) · stream_job.py (4 queries, 4 checkpoints)
batch/       profitability_job.py (recompute + join + score) · report_builder.py (HTML/CSV)
airflow/     Dockerfile (+ Java 17 + pyspark) · dags/daily_reconciliation_dag.py
api/         FastAPI serving layer
sql/         00_databases.sql · init.sql (10 tables, 3 views, fully commented)
monitoring/  prometheus.yml · alert_rules.yml · grafana provisioning + 2 dashboards
scripts/     wait_for_services · e2e_check · collect_evidence · build_charts ·
             build_test_report · demo.sh · chaos/ (7 scenarios)
tests/       unit/ · integration/ · e2e/
docs/        REPORT.md · TEST_REPORT.md · VIVA_GUIDE.md · diagrams/ · evidence/ · screenshots/
```

---

## 11. Troubleshooting

**`make up` hangs or a service never becomes healthy**
```bash
docker compose ps                     # which one is unhealthy?
docker compose logs --tail=80 <svc>
```
The stream job takes ~60–90 s to start (JVM + Kafka consumer); its healthcheck has a
`start_period` of 90 s, so "starting" for the first minute and a half is normal.

**`Ports are not available: ... bind: Only one usage of each socket address`**
Another process owns that host port. Change it in `.env` (`AIRFLOW_PORT`, `API_PORT`,
`GRAFANA_PORT`, `PROMETHEUS_PORT`) and re-run `make up`. Find the culprit with
`Get-NetTCPConnection -LocalPort 8080 -State Listen` (Windows) or `lsof -i :8080` (Linux/macOS).

**Airflow DAG fails at `check_master_data_available`**
The streaming job has not written a Parquet partition for that simulated day — usually because
the stack was stopped across a day boundary. This is a *correct* loud failure. Let the clock run
one more simulated day, or trigger the DAG for a day that does exist:
```bash
docker compose exec stream-job sh -c 'ls /data/lake/telemetry'
make trigger-dag D=<one of those dates>
```

**Permission denied writing to `/data`**
The `data-init` one-shot container creates the tree with permissive permissions before anything
else starts. If you see this, that container did not run:
```bash
docker compose up data-init
```

**Docker Desktop on Windows fails to start after a crash**
Seen during this project when the host ran out of memory. Recovery sequence:
```powershell
wsl --shutdown                                     # the \\wsl$ share gets wedged
Get-Process | Where-Object { $_.Name -like "*docker*" } | Stop-Process -Force
Rename-Item "$env:LOCALAPPDATA\Docker\run" "run.old"   # orphaned .sock Windows cannot delete
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
```
Named volumes and images survive this; no project data is lost.

**Out of memory / the daemon dies**
Do not run `make test-*` while an image build is in progress — the test container reserves
2.2 GB. Build first, then test.

**Grafana panels are empty**
Panels read PostgreSQL directly. Confirm there is data first:
`make sql Q="select count(*) from realtime_zone_metrics"`. Batch panels stay empty until the
first DAG run completes (~10 minutes after start-up).

---

## 12. Known limitations

Deliberate, laptop-scale simplifications — each is discussed with its production alternative in
`docs/REPORT.md` §10:

* single Kafka broker, replication factor 1 (no fault tolerance)
* local Parquet on a Docker volume instead of S3/HDFS, and no table format (Delta/Iceberg)
* Spark in `local[*]` mode; no cluster, no dynamic allocation
* at-least-once delivery + idempotent UPSERTs (effectively-once **storage**, not exactly-once)
* no schema registry; the JSON contract is enforced by `common/schemas.py` instead
* no TLS, no authentication on Kafka, Postgres, Prometheus or the API
* Alertmanager is not deployed — alert *rules* are evaluated and asserted via the Prometheus API
* simulated data: realistic in shape and units (LKR, Colombo zones) but not real telemetry

---

## 13. Deliverables map

| Deliverable | Where |
|---|---|
| Code | this repository |
| Docker Compose | `docker-compose.yml` (+ `Makefile` / `make.ps1`) |
| Observability config | `monitoring/` |
| Tests | `tests/`, `scripts/chaos/` |
| Technical report (8–15 pages) | [`docs/REPORT.md`](docs/REPORT.md) |
| Detailed test report | [`docs/TEST_REPORT.md`](docs/TEST_REPORT.md) |
| Viva guide + demo script | [`docs/VIVA_GUIDE.md`](docs/VIVA_GUIDE.md), `scripts/demo.sh` |
| Raw evidence | `docs/evidence/` |
| Build log and decisions | [`PROGRESS.md`](PROGRESS.md) |
