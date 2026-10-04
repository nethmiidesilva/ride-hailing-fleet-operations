# Viva Guide — defend every line

Student: `<<NAME>>` (`<<STUDENT_ID>>`) · Module: Applied Big Data Engineering

This guide has three parts:

1. **File-by-file walkthrough** — what each file does and the one design decision in it worth
   defending.
2. **Likely examiner questions with answers** — grouped by rubric criterion.
3. **The 5–10 minute demo script** — what to show, in what order, what to say.

---

## Part 1 — File-by-file walkthrough

### `common/` — the shared core (this is the file set to open first)

| File | What it does | The decision to defend |
|---|---|---|
| `config.py` | Frozen dataclass built once per process from `.env`. | *Why frozen?* Configuration cannot drift at runtime, so a threshold quoted in the report is the threshold the code used. `as_dict()` masks the password so evidence dumps never leak it. |
| `sim_clock.py` | Converts real elapsed time into simulated time; pins the epoch in `/shared/sim_epoch.txt` with `O_EXCL`. | *Why a shared file?* If each container computed its own start instant they would disagree about which simulated day it is, and the Airflow sensor would wait for a file the producer named differently. `O_EXCL` makes it a compare-and-set: exactly one process wins the race. |
| `zones.py` | Pure lat/lon → zone mapping over a 3×2 Colombo grid. | *Why is this shared and pure?* It is imported by **both** the speed layer and the batch layer. If they bucketed points differently, the nightly reconciliation would never balance and nobody would know why. It also contains a `1e-9` boundary epsilon because a point computed as exactly on a grid line evaluates to `0.9999999999999926` in floating point (caught by TC-ING-004). |
| `schemas.py` | The validation rules, in **two dialects**: pure Python and a Spark `Column`. | *Why two?* The Spark version runs in the JVM (no per-row Python round trip) for throughput; the Python version is readable and testable. TC-STR-002 asserts the two agree on the same fixtures, so they cannot drift. |
| `logging_setup.py` | One JSON formatter for every service. | *Why not a library?* It must run inside the PySpark, Airflow and API images without version conflicts. 60 lines, no dependency. The `stage` field is validated against a frozenset so a typo fails fast instead of producing unfilterable logs. |
| `db.py` | Thin psycopg helpers. | *Why no ORM and no pool?* Every write is a hand-written `INSERT … ON CONFLICT DO UPDATE` — that statement *is* the idempotency mechanism and should be visible, not generated. Connections are short-lived because `foreachBatch` runs a few times a minute on the driver. |
| `metrics.py` | All Prometheus metric definitions in one module. | Metric names are referenced verbatim by `alert_rules.yml` and the Grafana dashboards, so they are a public contract. One module means renaming one is a single, visible change. |

### `producers/`

| File | What it does | The decision to defend |
|---|---|---|
| `fleet_simulator.py` | Pure vehicle state machine: `idle → enroute → on_trip → idle`. | *Why separate from the Kafka client?* The interesting behaviour (transitions, movement, fares, fault injection) becomes unit-testable in milliseconds without any infrastructure. **The fare rule**: a fare is emitted *only* on the `on_trip → idle` transition, which is what makes `SUM(fare)` safe in both layers with no double counting. |
| `gps_producer.py` | Drives the simulator, publishes to Kafka. | **`key = vehicle_id`** is the single most important ingestion decision: Kafka orders messages within a partition, so a vehicle's transition sequence can never be reordered, while 25 vehicles still spread over 6 partitions. Also: `acks=all`, `enable.idempotence=true`, retries with backoff, delivery callbacks that count *acknowledgements* (not enqueues), SIGTERM flush. |
| `expense_producer.py` | Writes one CSV per simulated day. | **Atomic write**: temp file in the same directory, then `os.replace`. Same directory matters because `os.replace` is only atomic within one filesystem. Without it the `FileSensor` could read a half-written file — a classic production bug. |

### `streaming/`

| File | What it does | The decision to defend |
|---|---|---|
| `transforms.py` | Pure DataFrame → DataFrame functions. | Being pure is what lets TC-STR-007 assert *exact* hand-calculated window aggregates. `zone_window_metrics` takes `apply_watermark` because Spark 3.5 rejects a second `withWatermark` in a plan that already has one (DEFECT-004). |
| `stream_job.py` | Four independent streaming queries, four checkpoints. | *Why four and not one?* A failure or a code change in one sink cannot corrupt or stall the others, and each resumes independently from its own checkpoint. **Output modes differ on purpose**: the window query uses `update` so the dashboard refreshes every 30 s instead of waiting out the watermark — safe only because the write is an idempotent UPSERT. |

### `batch/`

| File | What it does | The decision to defend |
|---|---|---|
| `profitability_job.py` | Reads the Parquet partition, recomputes revenue/utilisation, joins costs, scores. | **It recomputes rather than reading the speed layer's tables.** That is the defining Lambda property: it is unaffected by the speed layer's approximations or watermark drops, and re-running a day converges to the same answer. Revenue is deduplicated by **`trip_id`** (the business key) not `event_id` (the technical key) — the stronger guarantee. |
| `report_builder.py` | Renders the consolidated HTML + CSV. | Plain string templating, no Jinja2, so the same code runs in the Airflow, tests and streaming images with no extra dependency. Every figure traces to one SQL statement listed at the top of the module. |

### `airflow/dags/daily_reconciliation_dag.py`

* The **sensor is the point**: a batch layer is only interesting if it has a real external
  dependency it can wait on, time out on and alert about. `mode="reschedule"` frees the worker
  slot between pokes, so waiting a whole simulated day costs nothing.
* The target date is computed **in a task**, not from `{{ ds }}`: Airflow's logical date is on the
  real calendar, this pipeline lives on the simulated one, so the mapping must be explicit.
* `retries=0` on the sensor only: a timeout is a definitive "the file is not coming", so retrying
  would just delay the alert by two more simulated days.

### `api/main.py`

* Reads **only** PostgreSQL. Each response model carries `source_layer: speed | batch`, which
  makes the Lambda split visible from outside the system.
* `/health` (deep) and `/health/live` (liveness) are separate **on purpose**: if the container
  healthcheck used the deep check, Docker would restart the API whenever Postgres hiccuped,
  turning a degraded read path into a full outage.
* **Data freshness is part of health**: a pipeline whose dependencies are all "up" but which has
  not seen an event in five minutes is not healthy.

---

## Part 2 — Likely examiner questions, with answers

### Architecture decision (20 marks)

**Q: Why Lambda and not Kappa?**
Because this use case has two genuinely different latency requirements *and* a second source
that is natively a file:
1. "Utilisation right now" needs seconds. "Which vehicles are unprofitable" needs yesterday's
   cost file, so it cannot be faster than daily no matter what the architecture is.
2. The expense CSV arrives once a day. Kappa would require pushing it into a Kafka topic —
   building and operating a connector so that a daily file can pretend to be a stream, for no
   latency benefit.
3. Profitability must be **correctable**. Late GPS events and corrected cost files both happen.
   Lambda's immutable master dataset means a day can simply be recomputed; the UPSERT converges.
   In Kappa, correction means replaying a long-retention topic into a parallel job and swapping
   over — heavier operationally for a laptop-scale system.

**Q: What does Kappa do better, and what did you give up?**
One code path, one skill set, and no possibility of the two layers disagreeing. What I gave up is
exactly that: I now have two implementations of "revenue" and could have shipped two different
answers. I mitigated it concretely rather than rhetorically — `common/zones.py` and
`common/schemas.py` are imported by both layers, TC-STR-002 asserts the Spark and Python
validators agree, and TC-E2E-002 *measures* the residual difference and explains it instead of
pretending it is zero.

**Q: Why do the two layers disagree at all?**
Three named reasons: (1) the speed layer drops events arriving later than the 2-minute watermark;
(2) it uses `approx_count_distinct` for vehicle counts; (3) its 1-minute windows are cut on real
`event_time`, which does not align with the simulated-day boundary the batch layer uses. The
batch figure is the one the business uses.

### Ingestion (15 marks)

**Q: Why 6 partitions and why key by `vehicle_id`?**
The key determines the partition, so all of a vehicle's events land in one partition and Kafka's
per-partition ordering guarantee means its `idle → enroute → on_trip → idle` sequence can never
be observed out of order. 6 partitions gives parallelism headroom (2–3× the current consumer
task count) without so many that each is mostly empty. `make consume` shows both properties.

**Q: What happens if the producer sends a duplicate?**
Three defences in depth: `enable.idempotence=true` stops librdkafka's own retries creating
duplicates; `dropDuplicatesWithinWatermark(["event_id"])` removes duplicates inside the 2-minute
watermark; and the batch layer deduplicates by `trip_id`, which catches anything that arrived
later than the watermark.

**Q: Why inject bad data deliberately?**
Because validation, quarantine, watermark and dedup logic cannot be *demonstrated* against clean
data. `BAD_EVENT_RATE=0.02`, `LATE_EVENT_RATE=0.01`, `DUPLICATE_RATE=0.01` and three "lazy"
vehicles make every one of those paths observable during a ten-minute demo.

### Processing (15 marks)

**Q: Explain the watermark.**
A watermark is Spark's statement of "I will not accept event-time data older than this". With
`withWatermark("event_time", "2 minutes")`, once the maximum observed event time is T, state for
windows ending before T − 2 min can be dropped and later arrivals for them are discarded. It is
what bounds state so the job can run forever. The trade-off is explicit: anything more than two
minutes late is lost to the speed layer and recovered only by the batch layer.

**Q: Why `dropDuplicatesWithinWatermark` instead of `dropDuplicates`?**
Plain `dropDuplicates` keeps every seen key in state forever — unbounded growth. The watermarked
version expires keys, bounding state at "one watermark's worth of event ids".

**Q: `update` vs `append` output mode — why different per sink?**
The Parquet sink must be `append` (a file sink cannot rewrite rows). The window aggregate uses
`update` so a window is re-emitted with running totals every trigger and the dashboard moves in
30 s rather than waiting 2+ minutes for the watermark to close it. That is only safe because the
write is an idempotent UPSERT on `(window_start, zone)`.

**Q: Do you have exactly-once?**
No, and the report says so. Kafka + Structured Streaming gives **at-least-once**. Combining it
with `INSERT … ON CONFLICT DO UPDATE` on each table's natural key gives **effectively-once
storage**: replaying a micro-batch converges to the same final state. TC-FT-002 restarts the job
and asserts `row count == distinct natural-key count` afterwards.

### Storage and serving (10 marks)

**Q: Why PostgreSQL and not Cassandra?**
The serving workload is small (tens of thousands of rows), needs **joins and aggregates** for the
profitability report, and needs `ON CONFLICT DO UPDATE` for idempotency. Cassandra gives write
scalability this workload does not need, in exchange for no joins and no transactional upserts.
At 10–100× the scale I would move the time-series half to TimescaleDB or ClickHouse and keep
Postgres for the dimensional data.

**Q: Why Parquet on a volume instead of just keeping it in Postgres?**
The master dataset is append-only, columnar-scanned one partition at a time, and must be cheap to
retain. Parquet + partition pruning on `sim_date` is exactly that access pattern, and it stands in
for S3/HDFS. Putting it in Postgres would conflate the immutable source of truth with the mutable
serving store — the separation is what makes recomputation possible.

**Q: Why is `idle_ratio` computed from event counts, not vehicle counts?**
Because a vehicle that goes `idle → enroute` inside one window is legitimately in *both*
distinct-vehicle counts, so `idle_vehicles / total_vehicles` could exceed 1 (DEFECT-008). The
event-count ratio is the time-weighted share of the window spent idle, and it is also exactly how
the batch layer defines `utilization` — so the two layers now measure the same quantity.

**Q: Why does `v_fleet_now` take counts from `vehicle_status` instead of summing the windows?**
Same reason: summing approximate per-zone distinct counts double-counts vehicles that changed
zone inside the minute (DEFECT-006). `vehicle_status` holds one exact row per vehicle.

### Observability (10 marks)

**Q: How would you know the pipeline broke at 3 a.m.?**
Two independent detectors for the same failure, on purpose: `NoTelemetryReceived` (Prometheus,
producer-side) and `/health` returning 503 on data freshness (serving-side). They fail for
different reasons, so neither is a single point of blindness. `StreamProcessingStalled` further
distinguishes "no data produced" from "data produced but not processed" — a distinction Lambda's
two layers make observable.

**Q: Why is every threshold that number?**
Every rule's rationale is in `monitoring/alert_rules.yml` as a comment and in REPORT §8 as a
table. Example: `NoTelemetryReceived` at 120 s because events are emitted every 2 s, so 120 s is
~60 missed cycles — far beyond any plausible GC pause or broker rebalance.

**Q: Can you trace a single event end to end?**
Yes — "tracing-lite". `event_id` travels from the producer's log line, through the Kafka message
header, into `rejected_events` (if quarantined) and into the stream job's log. `run_id` travels
from Airflow into `spark-submit`, into `daily_vehicle_profitability.run_id` and into
`pipeline_runs`. One `jq` filter follows either. A worked example is in REPORT §8.

### Code quality and testing (5 + 15 marks)

**Q: What did testing actually catch?**
Ten real defects, all listed in TEST_REPORT §7 with root cause and fix. The most interesting is
DEFECT-001: a point exactly on a zone boundary was bucketed by floating-point noise, so the same
coordinate could have been assigned to different zones in the two layers — the exact silent
Lambda divergence the shared-module design exists to prevent. TC-ING-004 caught it.

**Q: How do you know the window aggregates are correct?**
TC-STR-007 builds a five-row micro dataset by hand, states the expected values in the docstring
(3 active, 2 idle, ratio 0.4, 1 trip, 300.00 LKR, avg speed 40 over active rows) and asserts each
one. Asserting "some rows appeared" against live data would prove nothing.

---

## Part 3 — The demo (5–10 minutes)

Run `make demo` (it pauses between steps). Sequence and narration:

| # | Show | Say (one line) | Time |
|---|---|---|---|
| 0 | `docker compose ps` | "Eleven services; one simulated day is ten real minutes, so you will see a full daily cycle." | 0:30 |
| 1 | `make topics`, `make consume` | "Keyed by vehicle_id across 6 partitions — that is what guarantees per-vehicle ordering." | 1:00 |
| 2 | stream-job logs + `rejected_events` | "One structured line per micro-batch. Bad data is quarantined with a reason, never dropped." | 1:30 |
| 3 | `curl /metrics/fleet`, `/metrics/zones` | "This is the first half of the business question, answered in seconds." | 1:00 |
| 4 | `curl /alerts/idle?status=open` | "Threshold alert. A partial unique index guarantees one open alert per vehicle however often a batch replays." | 0:45 |
| 5 | Airflow UI graph + `pipeline_runs` + `ls /data/lake/telemetry` | "The batch layer recomputes from this immutable Parquet dataset — it does not trust the speed layer." | 1:30 |
| 6 | `curl /vehicles/unprofitable` + the HTML report | "The second half of the question — impossible for the speed layer, because costs arrive once a day." | 1:00 |
| 7 | Grafana ×2, Prometheus targets + rules | "Provisioned from disk; no manual clicking. Nine alert rules, each tied to a named failure." | 1:00 |
| 8 | `docker compose stop gps-producer`, wait, show 503 + firing alert, restart | "Two independent detectors, ~2.5 minutes, then automatic recovery." | 1:30 |
| 9 | the Lambda-vs-Kappa slide | "Two latency needs and a file-based source → Lambda. Its weakness is two code paths; here is the shared module that mitigates it and the test that measures what is left." | 0:30 |

**Screenshots to capture during this run** (the report has placeholders for them):

1. `screenshots/grafana_fleet.png` — Fleet Operations dashboard with live data
2. `screenshots/grafana_health.png` — Pipeline Health dashboard
3. `screenshots/airflow_dag_graph.png` — the DAG graph view
4. `screenshots/airflow_dag_run.png` — a successful run, all tasks green
5. `screenshots/prometheus_alerts.png` — the Alerts page with `NoTelemetryReceived` **firing**
6. `screenshots/fastapi_docs.png` — `/docs` with the endpoints expanded
7. `screenshots/html_report.png` — the rendered daily profitability report
8. `screenshots/spark_ui.png` — the Structured Streaming tab at :4040

**If something goes wrong mid-demo**, say what the system is *supposed* to do and show the
evidence file instead — `docs/evidence/scenarios/` contains a captured run of every scenario with
real detection latencies.
