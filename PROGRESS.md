# PROGRESS

Live build log for the Lambda-architecture fleet pipeline.  Updated after every phase gate.
**Status key:** ✅ done and verified · 🔄 in progress · ⛔ NOT DONE (reason stated)

| Phase | Scope | Gate | Status |
|---|---|---|---|
| P0 | CLAUDE.md, skeleton, `.env`, `common/` + unit tests | `pytest tests/unit` green | ✅ |
| P1 | docker-compose, init.sql, Makefile, monitoring config | stack healthy, tables exist | ✅ |
| P2 | GPS producer + simulator, topics | 20 messages consumed, partition spread | ✅ |
| P3 | Stream job (validate/dedup/enrich/3 sinks) | rows in PG, Parquet present, alert opens | ✅ |
| P4 | Expense producer, Airflow DAG, profitability job, report | DAG SUCCESS, idempotent re-run | ✅ |
| P5 | FastAPI | every endpoint curl'd, evidence saved | ✅ |
| P6 | Prometheus rules, Grafana, tracing | targets UP, alert fires on producer stop | ✅ |
| P7 | Integration/E2E/chaos/NFR tests, evidence | `make test-all` executed | ✅ |
| P8 | README | clean-slate run from README | ✅ |
| P9 | Diagrams, charts, REPORT.md, TEST_REPORT.md | numbers cross-checked with evidence | ✅ |
| P10 | VIVA_GUIDE.md, demo script, rubric checklist | — | ✅ |

---

## Environment (recorded once, quoted in the reports)

| Item | Value |
|---|---|
| Host OS | Windows 11 Home 10.0.26200 |
| Docker Engine | 27.4.0 |
| Docker Compose | v2.31.0-desktop.2 |
| Docker VM | 20 CPUs, 8,131,035,136 bytes (~7.57 GiB) RAM |
| Host Python | 3.13.2 (tests therefore run **inside** the container, see D-001) |
| Host Java | OpenJDK 21.0.6 (containers use JDK 17) |
| Project root | `D:\bigdata` |

---

## Decisions made (no user input was available; each is recorded with its rationale)

**D-001 — Tests run inside a Docker image, not on the host.**
The host interpreter is Python 3.13 and PySpark 3.5.3 supports at most 3.11, so a host-side test
run could never exercise the Spark transforms. `Dockerfile.test` builds a python:3.11 image with
JDK 17, the Spark connector jars and the union of every service's requirements.
Command used: `docker run --rm -v D:/bigdata:/app -w /app fleet-tests:1.0 pytest ...`.

**D-002 — Project root is `D:\bigdata` itself, not a nested `fleet-lambda/` folder.**
The spec's tree shows `fleet-lambda/` as the repository root; the working directory already *is*
that repository, so the contents were created directly in it rather than nesting one level
deeper. Every relative path in the spec resolves unchanged.

**D-003 — Named Docker volumes for `/data`, not a Windows bind mount.**
Spark Structured Streaming checkpoints rely on POSIX atomic-rename semantics that Docker
Desktop's Windows bind mounts do not reliably provide. `appdata`/`shared` are named volumes;
evidence is copied out with `docker cp` / `docker compose run`. Source code is still bind-mounted
read-only so edits do not need an image rebuild.

**D-004 — Airflow metadata lives in a second database on the same PostgreSQL server.**
A second Postgres container would cost ~200 MB of the 8 GB budget for no architectural benefit.
`sql/00_databases.sql` creates the `airflow` database; the analytics schema stays clean.

**D-005 — Spark/Kafka/JDBC jars are baked into the images at build time.**
`--packages` resolves from Maven Central on every container start: slow, and a single point of
failure during a live demo. The jars are `curl`ed into `$SPARK_HOME/jars` in the Dockerfile
(spark-sql-kafka-0-10 3.5.3, spark-token-provider-kafka-0-10 3.5.3, kafka-clients 3.4.1,
commons-pool2 2.11.1, postgresql 42.7.4).

**D-006 — Postgres writes use `psycopg` UPSERTs inside `foreachBatch`, not the Spark JDBC sink.**
The JDBC sink can only append or overwrite; the project needs `INSERT ... ON CONFLICT DO UPDATE`
to make at-least-once streaming into effectively-once storage and to make backfills idempotent.
JDBC *read* is still used in the batch job, which is why the driver jar is present.

**D-007 — GNU `make` is not installed on this Windows host.**
The `Makefile` is written and is the documented interface (it works on Linux/macOS/WSL and with
`choco install make`). For verification in this environment the equivalent
`docker compose` commands were run directly, and `make.ps1` provides the same targets on Windows.

**D-008 — Expense-file cost levels are calibrated, and the calibration is a config value.**
`EXPENSE_*` variables were added to `.env` so that a normal vehicle clears a healthy margin while
high-cost and lazy (low-revenue) vehicles fall below zero. Without calibration the business
question would have a trivial answer (all profitable or none).

**D-009 — Alertmanager is not deployed.**
The brief requires alert *rules*; routing them to email/Slack adds a container and no marks.
The chaos tests read alert state from the Prometheus HTTP API instead, which is stronger evidence
that a rule fired than a screenshot of a notification. Recorded as a limitation in the report.

**D-010 — Heavy operations are serialised (learned the hard way).**
Running the 2.2 GB test container at the same time as the full stack *and* an image build
starved the host (0.4 GB free of 15.6 GB) and the Docker daemon died. Since then, image builds
run with `stream-job` stopped, and the test container is never run while another build is in
flight. Recorded because it also constrains how the demo should be run on this laptop.

**D-011 — Docker Desktop recovery procedure (documented in the README troubleshooting section).**
Recovering from the crash above needed a specific sequence, worth recording because a marker
reproducing the project on Windows could hit the same thing:
1. `wsl --shutdown` (the `\wsl$\docker-desktop` share was wedged: *"The specified network name
   is no longer available"*).
2. Kill every `*docker*` process.
3. Rename `%LOCALAPPDATA%\Docker\run` aside — it holds an orphaned AF-UNIX socket file
   (`userAnalyticsOtlpHttp.sock`) that Windows refuses to delete ("The file cannot be accessed by
   the system"), and Docker Desktop aborts start-up trying to remove it.
4. Relaunch Docker Desktop.
**No project data was lost**: every named volume (`bigdata_pgdata`, `bigdata_appdata`,
`bigdata_kafkadata`, ...) and every built image survived, which is itself evidence that the
storage design is sound.

**D-012 - A WSL memory cap was installed on the host (user-visible change, reversible).**
The Docker VM starved Windows three times (free physical memory below 0.5 GB of 15.6 GB) and the
daemon became unresponsive each time. Root cause: WSL2 claims memory aggressively and does not
return it. Fix applied:

* `.wslconfig.recommended` was written into the repository (documented, with the rationale), and
* a copy was installed at `%USERPROFILE%\.wslconfig` capping the VM at **6 GB** with
  `autoMemoryReclaim=gradual`.

**There was no pre-existing `.wslconfig`, so nothing was overwritten.** To revert, delete
`%USERPROFILE%\.wslconfig` and run `wsl --shutdown`. This is called out again in
`FINAL_SUMMARY.md` because it is a change outside the project directory.

Alongside it, every container limit was reduced so the stack totals **5.8 GB** instead of 7.6 GB
(Kafka heap 400m, Spark driver 950m, Airflow webserver cut from 4 gunicorn workers to 2 - that
alone was ~450 MB). The pipeline behaves identically; it simply fits.

---

## Phase log

### P0 — foundations ✅ (verified)

Built:
- `CLAUDE.md` (architecture + ground rules), `.env.example` → `.env` (73 settings, all documented)
- `common/config.py` (frozen dataclass, single source of truth), `common/sim_clock.py`
  (dual-clock design + shared epoch file), `common/zones.py` (pure 3×2 Colombo grid),
  `common/logging_setup.py` (JSON formatter), `common/schemas.py` (validation rules in both
  pure-Python and Spark-Column dialects), `common/db.py`, `common/metrics.py`
- `producers/fleet_simulator.py`, `producers/gps_producer.py`, `producers/expense_producer.py`
- `pyproject.toml` (ruff + black + pytest + coverage config), `Dockerfile.test`
- Unit suites: `test_config.py`, `test_sim_clock.py`, `test_zones.py`, `test_schemas.py`,
  `test_logging_setup.py`, `test_fleet_simulator.py`, `test_expense_producer.py`

Commands run:
```
docker build -f Dockerfile.test -t fleet-tests:1.0 .
docker run --rm -v D:/bigdata:/app -w /app fleet-tests:1.0 pytest tests/unit -m unit -q
```

Verification output:
```
........................................................................ [ 64%]
.......................................                                  [100%]
111 passed
```

Problems hit and fixed:

- **P0-1 `heredoc` write of large Python files failed** under the Bash tool's quoting. Switched to
  direct file writes. No impact on the deliverable.
- **DEFECT-001 (real bug, caught by TC-ING-004): zone boundary decided by floating-point noise.**
  `(6.92 - 6.86) / (6.98 - 6.86) * 2` evaluates to `0.9999999999999926`, so a point exactly on
  the internal grid line floored to row 0 instead of row 1. Because `zone_for_point` is shared by
  the speed and batch layers, the *same* coordinate could have been bucketed differently
  depending on how the value reached the function — precisely the silent Lambda divergence the
  shared-module design exists to prevent. Fixed by adding a `1e-9` boundary epsilon
  (`common/zones.py`), with the rationale in a code comment. Re-ran: TC-ING-004 passes.
- **DEFECT-002 (test infrastructure): `tmp_epoch` fixture could not override the frozen Config.**
  `Config` is built at import time, so `monkeypatch.setenv` alone did not reach `sim_clock.CFG`.
  Fixed by swapping in a freshly built `Config` (which is what a restarted container does anyway).

Known issues at end of P0: none.

### P1-P3 - infrastructure, ingestion, speed layer (verified)

**P1 gate:** `docker compose up -d` -> all services healthy; `\dt` shows the 10 tables and 3 views;
the `airflow` database exists.
**P2 gate:** 20 messages consumed with `kafka-console-consumer --property print.partition=true`;
keys are `vehicle_id`; all **6 partitions** received data; a malformed event is visible with key
`UNKNOWN`. Evidence: `docs/evidence/kafka/consume_20_messages.txt`, `topics_describe.txt`.
**P3 gate:** `realtime_zone_metrics`, `vehicle_status` populated; Parquet partitions under
`/data/lake/telemetry/sim_date=...`; `rejected_events` holding all six injected reason types; two
idle alerts opened at exactly 3.01 minutes for lazy vehicles V-001 and V-002.

Defects found and fixed in these phases: DEFECT-003 (producer metric label),
DEFECT-004 (double watermark), DEFECT-005 (shared consumer group), DEFECT-006 (`v_fleet_now`
double counting), DEFECT-007 (sim-hour below window granularity), DEFECT-008 (`idle_ratio` > 1),
DEFECT-009 (`dropDuplicatesWithinWatermark` on bounded input). All documented in
`docs/TEST_REPORT.md` section 7.

### P4 - batch layer (verified)

First fully successful DAG run for simulated day **2026-09-01**:

```
 report_date | status  | rows_written
-------------+---------+-------------------------------------------------------------
 2026-09-01  | success | {"parquet_files": 126, "expense_rows_valid": 25,
                          "expense_rows_rejected": 2, "expense_rows_loaded": 25,
                          "profitability_rows": 25, "zone_summary_rows": 144,
                          "missing_expense": 0,
                          "report_html": "/data/reports/profitability_2026-09-01.html",
                          "report_csv":  "/data/reports/profitability_2026-09-01.csv"}
```

Business result for that day: 25 vehicles, **6 unprofitable**, revenue 30,611.90 LKR,
cost 22,747.82 LKR, profit 7,864.08 LKR. The narrative works as designed - the lazy vehicle
V-001 combines 8.5% utilisation with a 3,488 LKR maintenance bill and loses 3,503.54 LKR, while
busy vehicles at ~0.75-0.86 utilisation clear roughly +600 LKR.

Defects found and fixed: DEFECT-010 (shared `/data` volume writable by only one UID),
DEFECT-011 (`airflow db migrate` does not seed `fs_default`, so the FileSensor died in 0.4 s),
DEFECT-012 (`report_builder` configured logging at import time and so cleared Airflow's task-log
handler mid-task, getting the task SIGKILLed with return code -9).

### P5-P7 - serving, observability, tests (verified)

**Unit suite:** `167 passed, 0 failed` in 37.8 s. Coverage 66-68% overall, with the modules that
carry business logic well above the 70% target: `common/zones.py` 100%, `common/schemas.py` 98%,
`common/sim_clock.py` 96%, `producers/fleet_simulator.py` 96%, `streaming/transforms.py` 91%,
`api/main.py` 85%, `batch/report_builder.py` 79%. The low numbers are the long-running `run()`
loops in the producers and the stream job, which the integration and chaos suites exercise
behaviourally instead.

**Integration suite:** `25 passed, 1 skipped` (the skip is TC-ORC-010, which needs the Airflow CLI
inside the tests image - by design). Two test defects were found and fixed along the way: the
Airflow REST API renders a timedelta schedule as `{"__type":"TimeDelta","seconds":600}` rather
than `{"value": 600}`, and a `pipeline_runs` query ordered by a column that can be NULL, which
PostgreSQL sorts FIRST on DESC.

**E2E + NFR:** `4 passed, 1 skipped`. Real measurements:
- throughput 13.40 events/s mean (12.5 theoretical; the excess is the injected duplicates)
- micro-batch duration p50 2.22 s, p95 4.72 s, against a 20 s alert threshold
- end-to-end latency (producer -> Kafka -> Spark -> UPSERT) 1.70 s
- **reconciliation: batch 30,611.90 LKR vs speed layer 33,545.28 LKR = +9.58%**, within the 25%
  tolerance and explained by watermark drops plus window/day boundary misalignment

**An observability defect cluster (DEFECT-013 to DEFECT-016).** Checking whether the alerts had
*ever* had data turned out to be the single most productive test of the whole build. Four rules
were broken in ways that no code review would catch, because nothing errors and everything renders:

1. **DEFECT-013** - `ConsumerLagHigh` read `kafka_consumergroup_lag`, which is permanently empty:
   Structured Streaming keeps offsets in its checkpoint, not in a Kafka consumer group, so the
   broker has no group to report on. Fixed by publishing `stream_committed_offset` from
   `query.lastProgress` and computing lag against the broker's end offsets in PromQL. Verified:
   24 series (4 queries x 6 partitions), lag 176 messages.
2. **DEFECT-014** - every service exposed *every* metric, because they all import
   `common/metrics.py` and it registered into the default registry. An unset Gauge reads 0, so
   `time() - 0` made three rules fire permanently on phantom instances. Fixed with five
   per-service `CollectorRegistry` objects; the producer endpoint went from ~25 metric families
   to 7.
3. **DEFECT-015** - `HighRejectRate` divided rejected *events* by rows written *per sink* -
   different units, ~5x too high, permanently over threshold. Fixed with a real
   `stream_events_valid_total` counter. Ratio now 0.0156 against a configured `BAD_EVENT_RATE`
   of 0.02.
4. **DEFECT-016** - `api_health_status` was written only inside the `/health` handler, which
   Prometheus never calls. Fixed with a 15 s background refresher in the FastAPI lifespan.

After all four fixes: **zero alerts firing against a healthy stack**, which is the state a real
on-call rotation needs and was not true before.

### P8-P10 - docs, evidence and the final defect cluster (verified)

**Final test position:** `200/203` pytest cases pass with
**0 failures** and 3 deliberate skips, plus **13/13**
scenario runs (chaos, NFR, end-to-end) all PASS. All 12 requirements PASS in the traceability
matrix. `ruff check` is clean. `scripts/e2e_check.py` reports **14/14**.

**The Results sections are generated, not typed.** Four scripts read `docs/evidence/` and write the
documents, so the ground rule "never fabricate results" is enforced mechanically rather than by
discipline:

| Script | Writes |
|---|---|
| `scripts/fill_report_results.py` | `docs/REPORT.md` section 9, including a business narrative *derived from the actual rows* so it cannot cite a figure the reader cannot see |
| `scripts/fill_test_report.py` | `docs/TEST_REPORT.md` sections 1.2, 1.3, 2.2 and 8 |
| `scripts/fill_test_case_statuses.py` | the Status and Actual-result cells of all 143 documented test cases |
| `scripts/fill_scenario_narratives.py` | the seven chaos narratives and the performance table |

All four are **idempotent**, so re-running them after a fresh capture refreshes the documents
instead of requiring hand-editing.

**Three more defects surfaced during the final verification**, all from the same habit of asking
"is this number actually right?" rather than "did the test pass?":

* **DEFECT-017** - `NoTelemetryReceived` could not fire when the producer *died*, because
  Prometheus marks a stopped target's series stale and `time() - <no data>` is an empty vector.
  TC-FT-001 **failed** and that failure was the most valuable single result of the build: it was
  only visible after DEFECT-014 was fixed, because two bugs had been cancelling out and the alert
  had been firing for entirely the wrong reason. Fixed with the dead man's switch (`up == 0`),
  which as a bonus detects a dead target in 52 s rather than the 120 s the age threshold allowed.
* **DEFECT-018** - `v_fleet_now` counted every row of the never-pruned `vehicle_status`, so the
  200-vehicle load test left the live fleet reporting 200. In production a decommissioned vehicle
  would deflate the utilisation percentage the whole business question is about. Fixed with a
  recency filter, applied to all three counts so the `active + idle = total` invariant holds.
* **DEFECT-019** - an idle alert never closed for a vehicle that stopped reporting *while idle*, so
  `ManyVehiclesIdle` fired forever. Fixed with an `abandoned` sweep - and the first version of that
  fix used an inner join, which silently matched zero rows because the case that mattered was an
  *absent* status row, not a stale one. Rewritten with `NOT EXISTS`, and pinned by TC-STR-015 so it
  cannot regress.

Final state: **zero alerts firing against a healthy stack**, and the three open idle alerts are
exactly the three vehicles the simulator marks as lazy, at 3.03-3.08 minutes against the configured
3-minute threshold.
