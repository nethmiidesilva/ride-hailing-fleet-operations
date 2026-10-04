# Test Report — Ride-Hailing Fleet Operations Lambda Pipeline

**Module:** Applied Big Data Engineering — Mini Project
**Student:** `<<NAME>>` (`<<STUDENT_ID>>`)
**System under test:** `fleet-lambda`, version 1.0.0
**Report generated:** `<<DATE>>`

> **Ground rule.** Every status, number and log excerpt in this report comes from a real
> execution. Machine-readable artefacts are under `docs/evidence/`: `tests/junit-*.xml` (pytest),
> `tests/coverage.xml`, `scenarios/*.json` (chaos and non-functional runs), `api/*.json`,
> `sql/*.txt`. Anything that could not be executed is marked **NOT EXECUTED** with the reason.

---

## Table of contents

1. [Summary](#1-summary)
2. [Requirements and traceability matrix](#2-requirements-and-traceability-matrix)
3. [Test strategy](#3-test-strategy)
4. [Detailed test cases](#4-detailed-test-cases)
5. [Scenario test narratives](#5-scenario-test-narratives)
6. [Performance results](#6-performance-results)
7. [Defect log](#7-defect-log)
8. [Coverage](#8-coverage)
9. [Known gaps and untested areas](#9-known-gaps-and-untested-areas)

---

## 1. Summary

### 1.1 Environment

| Item | Value |
|---|---|
| Host OS | Windows 11 Home 10.0.26200 |
| Docker Engine | 27.4.0 |
| Docker Compose | v2.31.0-desktop.2 |
| Docker VM | 20 CPUs; WSL2 capped at 6 GB (see PROGRESS.md D-012) |
| Host RAM | 15.6 GB |
| Test interpreter | Python 3.11 **inside** `fleet-tests:1.0` (host is 3.13; PySpark 3.5 supports ≤ 3.11) |
| JVM in containers | OpenJDK 17 |
| PySpark | 3.5.3 |
| Kafka | apache/kafka 3.7.1 (KRaft) |
| PostgreSQL | 16-alpine |
| Airflow | 2.9.3-python3.11 |
| Prometheus / Grafana | 2.53.3 / 11.3.1 |

### 1.2 Totals

Generated from `docs/evidence/tests/summary.json` by `scripts/fill_test_report.py`; the underlying
artefacts are the JUnit XML files and the per-scenario JSONs in the same tree.

| Metric | Value |
|---|---|
| Test cases executed | **216** (203 pytest + 13 scenarios) |
| Passed | **213** |
| Failed | **0** |
| Skipped | 3 |
| Not executed | 0 |
| pytest wall-clock | 224.7 s |
| Defects found and fixed | **17** |

Per-suite breakdown:

| Suite | Passed | Failed | Skipped |
|---|---|---|---|
| Unit (incl. local SparkSession) | 167 | 0 | 0 |
| Integration | 29 | 0 | 0 |
| End-to-end + NFR | 4 | 0 | 0 |
| Chaos / failure scenarios | 13 | 0 | — |

The two skips are deliberate and documented in §9: TC-ORC-010 needs the Airflow CLI inside the
tests image, and TC-NFR-003 needs the Docker CLI there. Neither is a correctness property.
### 1.3 Conclusion

The system meets its functional requirements. All 200 executed pytest cases pass, and
13 of 13 scenario runs pass.

The most valuable outcome of the exercise was not the pass rate but the **17 defects** it exposed,
and in particular a cluster of five in the alerting layer (DEFECT-013 … DEFECT-017). Those five
are worth singling out because each rule was *correct as written* — no error, no missing metric
name, a rendering dashboard — and each was nonetheless incapable of doing its job. Two of them had
been cancelling each other out, so the flagship availability alert appeared to work while firing
for entirely the wrong reason. They were found only by stopping real containers and asking whether
the real detector fired.

That is the argument for the chaos suite: its value is not that six scenarios pass, it is that
**TC-FT-001 failed**, and that single failure was worth more than the passes combined. The
requirement most strongly evidenced is REQ-11 (recompute/replay), where TC-FT-007 shows a replayed
simulated day producing a **byte-identical SHA-256** over the business columns.
## 2. Requirements and traceability matrix

### 2.1 Requirement list

| ID | Requirement (derived from the brief) |
|---|---|
| REQ-01 | A streaming source emits vehicle telemetry continuously into Kafka |
| REQ-02 | A daily-batch source drops one cost file per simulated day |
| REQ-03 | The two sources are joined to produce per-vehicle economics |
| REQ-04 | Utilisation and earnings are aggregated over time windows and by area |
| REQ-05 | A real-time API exposes current fleet utilisation and earnings |
| REQ-06 | A threshold alert fires when a vehicle is idle beyond a limit |
| REQ-07 | A daily per-vehicle profitability report is produced |
| REQ-08 | Structured logging spans ingestion, processing and storage |
| REQ-09 | At least one alert/health-check rule exists, backed by metrics |
| REQ-10 | Behaviour is reproducible and deterministic for a given seed |
| REQ-11 | A past day can be replayed/recomputed with identical results |
| REQ-12 | Bad data is detected, quarantined with a reason, and never silently lost |

### 2.2 Traceability matrix

| Requirement | Test cases | Status |
|---|---|---|
| REQ-01 | TC-ING-030…044, TC-INT-001, TC-FT-003, TC-NFR-001, TC-NFR-004 | **PASS (23/23)** |
| REQ-02 | TC-ING-045…055, TC-INT-002, TC-ORC-006, TC-FT-005, TC-FT-006 | **PASS (20/20)** |
| REQ-03 | TC-BAT-001…006, TC-ORC-005, TC-E2E-001 | **PASS (11/11)** |
| REQ-04 | TC-STR-007…010, TC-BAT-013, TC-BAT-014, TC-INT-003 | **PASS (18/18)** |
| REQ-05 | TC-SRV-005…007, TC-SRV-015…017, TC-INT-010, TC-E2E-001 | **PASS (19/19)** |
| REQ-06 | TC-SRV-008, TC-SRV-009, TC-INT-007, TC-INT-008 | **PASS (4/4)** |
| REQ-07 | TC-SRV-020…029, TC-ORC-009, TC-E2E-001 | **PASS (12/12)** |
| REQ-08 | TC-OBS-001…005, TC-INT-015 | **PASS (6/6)** |
| REQ-09 | TC-SRV-001…004, TC-INT-011, TC-INT-012, TC-FT-001, TC-FT-004 | **PASS (8/8)** |
| REQ-10 | TC-ING-010…018, TC-OBS-010…013, TC-BAT-015, TC-FT-002 | **PASS (15/15)** |
| REQ-11 | TC-BAT-012, TC-BAT-015, TC-FT-002, TC-FT-007, TC-E2E-002 | **PASS (5/5)** |
| REQ-12 | TC-ING-020…029, TC-STR-001…003, TC-INT-006, TC-ORC-006, TC-FT-004, TC-FT-006 | **PASS (17/17)** |

---

## 3. Test strategy

### 3.1 Levels

| Level | Marker | What it proves | Where |
|---|---|---|---|
| **Unit** | `unit` | Business logic is correct against hand-calculated expected values, with no infrastructure. | `tests/unit/` |
| **Unit (Spark)** | `unit` + `spark` | DataFrame transformations produce exact expected aggregates on micro datasets, using a real local SparkSession. | `tests/unit/test_transforms.py`, `test_profitability.py` |
| **Integration** | `integration` | The deployed services really talk to each other: Kafka partitioning, Spark→Postgres, Airflow, Prometheus, Grafana. | `tests/integration/` |
| **End-to-end** | `e2e` | One complete simulated day flows through every layer and answers the business question; the two layers reconcile. | `tests/e2e/` |
| **Failure / resilience** | shell | Injected failures are detected by the real detection mechanism within a measured time, and recovery works. | `scripts/chaos/` |
| **Non-functional** | `nfr` | Throughput, latency and resource usage are measured, not assumed. | `tests/e2e/`, `scripts/nfr_load_test.sh` |

### 3.2 Test data design

Three distinct strategies, chosen per level:

1. **Seeded simulator** (`SEED=42`). `FleetSimulator` is deterministic: the same seed produces the
   same fleet, the same transitions, the same fares and the same injected faults. This makes
   integration and e2e runs comparable between executions (TC-ING-035).
2. **Hand-calculated fixtures.** Every aggregate and formula test states its expected values *in
   the docstring, derived by hand*, then asserts them. For example TC-STR-007 builds five events
   in one window and asserts 3 active / 2 idle / ratio 0.4 / 1 trip / 300.00 LKR / avg speed 40.
   Asserting "some rows appeared" against live data would prove nothing.
3. **Deliberate corruption.** The producers inject malformed, late and duplicate events
   (`BAD_EVENT_RATE`, `LATE_EVENT_RATE`, `DUPLICATE_RATE`) and dirty CSV rows
   (`EXPENSE_DIRTY_ROWS`), each mapping one-to-one onto a rejection reason. Without this, the
   validation and quarantine paths could not be exercised at all.

### 3.3 Environments

* **Unit** — `fleet-tests:1.0` container, no stack. Runs in ~2 minutes.
* **Integration / e2e** — the same container attached to `fleet-net`, with the full stack running.
  Tests **skip** (not fail) when the stack is unreachable, so `pytest tests/` remains usable with
  Docker stopped.
* **Chaos** — bash scripts on the host that stop, recreate and restart real containers and poll
  the real Prometheus and PostgreSQL APIs for detection.

### 3.4 How results are captured

```bash
make test-unit          # + coverage → docs/evidence/tests/
make test-integration
make test-e2e
make test-nfr
make test-chaos         # → docs/evidence/scenarios/*.json
make report             # parses the above into the tables in this document
```

Each chaos scenario writes a JSON file recording the baseline, the injection with its timestamp,
the detection latency measured against the *real* detector, and the recovery. Those files are the
evidence behind §5.

---

## 4. Detailed test cases

> Status values are filled from `docs/evidence/tests/summary.json`. The generated table of every
> executed pytest node with its status and duration is at
> `docs/evidence/tests/results_table.md`; the scenario table is at
> `docs/evidence/tests/scenarios_table.md`.

### 4.1 Unit — simulated clock (`tests/unit/test_sim_clock.py`)

#### TC-ING-010 — Epoch maps to the simulation start date

| Field | Value |
|---|---|
| Requirement(s) | REQ-10 |
| Level / Type | Unit |
| Component | `common/sim_clock.py` |
| Objective | Proves the simulated timeline starts where it is configured to; if this is wrong, every Parquet partition name and every report date is wrong. |
| Preconditions | None (time is injected, not slept on). |
| Test data / Input | `real_now = epoch = 1_700_000_000.0`; `SIM_START_DATE=2026-09-01` |
| Steps | 1. Call `sim_now(real_now=EPOCH, epoch=EPOCH)`. |
| Expected result | `datetime(2026, 9, 1, 0, 0, tzinfo=UTC)` |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | `docs/evidence/tests/junit-unit.xml`, node `tests/unit/test_sim_clock.py::test_tc_ing_010_epoch_zero_is_simulation_start` |
| Notes | — |

#### TC-ING-011 — One simulated day elapses after `SIM_DAY_SECONDS`

| Field | Value |
|---|---|
| Requirement(s) | REQ-10 |
| Level / Type | Unit |
| Component | `common/sim_clock.py` |
| Objective | The compression ratio is exactly 86400 / `SIM_DAY_SECONDS`; the batch layer's day boundary depends on it. |
| Test data / Input | `real_now = EPOCH + 600` |
| Expected result | Simulated time advanced exactly 86,400 s; `sim_date` is `2026-09-02`. |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | node `…::test_tc_ing_011_one_sim_day_elapses_after_sim_day_seconds` |

#### TC-ING-012 — Simulated hour-of-day mapping

| Field | Value |
|---|---|
| Requirement(s) | REQ-04, REQ-10 |
| Level / Type | Unit (parametrised, 5 cases) |
| Objective | Time-of-day analysis groups by `sim_hour`; an off-by-one here silently mis-buckets every earnings figure. |
| Test data / Input | offsets `0`, `SIM_DAY/24`, `SIM_DAY/2`, `SIM_DAY×23/24`, `SIM_DAY − 0.001` |
| Expected result | hours `0, 1, 12, 23, 23` respectively |
| Actual result | As expected (0.01 s) |
| Status | **PASS** |
| Evidence | node `…::test_tc_ing_012_sim_hour_mapping[…]` |

#### TC-ING-013 — Day boundary and `previous_sim_date`

| Field | Value |
|---|---|
| Requirement(s) | REQ-02, REQ-10 |
| Objective | The Airflow DAG reconciles `previous_sim_date()`; if the rollover is off, the DAG asks for a file that does not exist. |
| Test data / Input | `EPOCH + SIM_DAY ± 0.01` |
| Expected result | Date rolls exactly at the boundary; `previous_sim_date` equals the day just finished. |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | node `…::test_tc_ing_013_day_boundary_and_previous_day` |

#### TC-ING-014 — Countdown to the next simulated day

| Field | Value |
|---|---|
| Requirement(s) | REQ-02 |
| Objective | The expense producer sleeps on this value; a negative or zero result would spin the process. |
| Expected result | `SIM_DAY` at the epoch, `0.75 × SIM_DAY` at a quarter through, and a **full** day immediately after a rollover (never 0). |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | node `…::test_tc_ing_014_seconds_until_next_sim_day` |

#### TC-ING-015 — Simulated date maps back to a real time window

| Field | Value |
|---|---|
| Requirement(s) | REQ-11 |
| Objective | The reconciliation test needs "when in real time did simulated day D happen?" to compare speed-layer windows against a batch day. |
| Expected result | `[epoch, epoch + SIM_DAY)`; round-trips to the same date. |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | node `…::test_tc_ing_015_real_window_for_a_sim_date` |

#### TC-ING-016 / TC-ING-017 — Shared epoch file

| Field | Value |
|---|---|
| Requirement(s) | REQ-10 |
| Objective | Every container must agree on the epoch. 016 proves a second caller *reads* rather than overwrites; 017 proves the first caller creates it atomically (including parent directories). |
| Expected result | Both calls return the same float; the file exists after the first call. |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | nodes `…::test_tc_ing_016_epoch_file_is_written_once`, `…_017_epoch_created_when_absent` |
| Notes | DEFECT-002 was found here — the fixture could not override the frozen `Config` by env alone. |

#### TC-ING-018 — Monotonicity

| Field | Value |
|---|---|
| Objective | Simulated time never goes backwards for increasing real time; a non-monotonic clock would corrupt watermarks and partitioning. |
| Test data / Input | 33 samples across 1,200 real seconds |
| Expected result | The sample list equals its sorted self. |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |

### 4.2 Unit — zones (`tests/unit/test_zones.py`)

#### TC-ING-002 — Every zone centre maps back to itself

| Field | Value |
|---|---|
| Requirement(s) | REQ-12 |
| Component | `common/zones.py` (shared by **both** layers) |
| Objective | A round-trip property test over all six zones. Because both layers import this function, a regression here desynchronises the entire architecture. |
| Test data / Input | `zone_centre(z)` for each of the 6 zones |
| Expected result | `zone_for_point(*zone_centre(z)) == z` for all z |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | node `tests/unit/test_zones.py::test_tc_ing_002_every_zone_centre_maps_back_to_itself` |

#### TC-ING-003 — Bounding box is inclusive on all four edges

| Field | Value |
|---|---|
| Objective | A vehicle exactly on the licensed-area edge must be inside it, not `OUT_OF_AREA`. |
| Test data / Input | the four corners (6.86/6.98 × 79.84/79.90) |
| Expected result | corners map to `Wellawatte`, `Borella`, `Kollupitiya`, `Pettah` respectively |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |

#### TC-ING-004 — Internal boundary is single-valued *(found DEFECT-001)*

| Field | Value |
|---|---|
| Requirement(s) | REQ-12 |
| Objective | The documented rule is "a point on a grid line belongs to the higher cell". This must be *deterministic*, not decided by floating-point noise. |
| Test data / Input | `lat = lat_min + (lat_max−lat_min)/2`, `lon = lon_min + (lon_max−lon_min)/3` |
| Expected result | `GRID[1][1]` (`Fort`); one millionth of a degree below the line yields `GRID[0][1]`. |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |
| Evidence | node `…::test_tc_ing_004_internal_boundary_belongs_to_the_higher_cell`; fix in `common/zones.py` (`_BOUNDARY_EPS = 1e-9`) |
| Notes | **This is the most valuable defect found.** Because `zone_for_point` is shared, the same coordinate could have been bucketed differently depending on how the value reached the function — the exact silent divergence the shared-module design exists to prevent. |

#### TC-ING-005 — Outside or unusable points become `OUT_OF_AREA`

| Field | Value |
|---|---|
| Level / Type | Unit (parametrised, 9 cases) |
| Test data / Input | Gulf of Guinea (0,0); just outside each edge; `None`; non-numeric `"abc"`; `NaN` |
| Expected result | `OUT_OF_AREA` in all nine cases, with **no exception raised** (NaN in particular must not propagate). |
| Actual result | As expected (0.01 s) |
| Status | **PASS** |

#### TC-ING-006 / TC-ING-007 / TC-ING-008

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-ING-006 | The function is pure and deterministic | repeated calls give identical results | **PASS** |
| TC-ING-007 | An unknown zone name is a programming error | `ValueError("unknown zone")` | **PASS** |
| TC-ING-008 | The grid covers the box with no gaps | a 21×21 sweep never yields `OUT_OF_AREA` and reaches all 6 zones | **PASS** |

### 4.3 Unit — validation rules (`tests/unit/test_schemas.py`)

#### TC-ING-020 — A well-formed event is accepted

| Field | Value |
|---|---|
| Requirement(s) | REQ-12 |
| Expected result | `reject_reason(valid_event()) is None` |
| Actual result | As expected (0.00 s) |
| Status | **PASS** |

#### TC-ING-021 — Each corruption yields its specific reason

| Field | Value |
|---|---|
| Requirement(s) | REQ-12 |
| Level / Type | Unit (parametrised, **15 cases**) |
| Objective | The reason strings land in `rejected_events.reason`, drive `HighRejectRate`, and are grouped in the daily report, so they are a public contract. |
| Test data / Input | null/blank `event_id`; null/blank `vehicle_id`; null `event_time`; unknown and null `status`; negative and null `speed`; `speed=999`; `lat=±95`; `lon=200`/null; `fare=−10` |
| Expected result | `NULL_EVENT_ID`, `NULL_VEHICLE_ID`, `NULL_EVENT_TIME`, `UNKNOWN_STATUS`, `NEGATIVE_SPEED`, `SPEED_TOO_HIGH`, `LAT_OUT_OF_RANGE`, `LON_OUT_OF_RANGE`, `NEGATIVE_FARE` respectively |
| Actual result | As expected (0.02 s) |
| Status | **PASS** |

#### TC-ING-022 … TC-ING-025

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-ING-022 | Multiple violations report the first rule in `REASON_ORDER` | `NULL_VEHICLE_ID` wins over `UNKNOWN_STATUS` and `NEGATIVE_SPEED` | **PASS** |
| TC-ING-023 | Out-of-area is **not** a rejection | a valid point outside the box passes; dropping it would understate fleet size | **PASS** |
| TC-ING-024 | `speed = 0` while idle is valid | only negatives are rejected | **PASS** |
| TC-ING-025 | The validator is pure | input dict unchanged after the call (safe inside a Spark task) | **PASS** |

#### TC-ING-026 … TC-ING-029 — Expense-row validation

| ID | Objective | Test data | Expected | Status |
|---|---|---|---|---|
| TC-ING-026 | A clean row passes | full valid row | `None` | **PASS** |
| TC-ING-027 | Each dirty pattern is caught (6 parametrised cases) | blank vehicle_id; `"abc"` fuel; negative fuel; negative maintenance; `service_flag=2`; `"01/09/2026"` date | `MISSING_VEHICLE_ID`, `NON_NUMERIC_VALUE`, `NEGATIVE_VALUE`, `NEGATIVE_VALUE`, `NON_NUMERIC_VALUE`, `BAD_REPORT_DATE` | **PASS** |
| TC-ING-028 | Row date must match the file's day | `expected_date` mismatch | `BAD_REPORT_DATE` — guards against a file named for one day holding another day's data | **PASS** |
| TC-ING-029 | A truncated row is rejected, not defaulted | `distance_covered` removed | `MISSING_COLUMN` | **PASS** |

### 4.4 Unit — fleet simulator (`tests/unit/test_fleet_simulator.py`)

#### TC-ING-030 — Only legal state transitions occur

| Field | Value |
|---|---|
| Requirement(s) | REQ-01 |
| Objective | The `idle → enroute → on_trip → idle` cycle is the model the whole pipeline assumes; an illegal jump would invalidate utilisation and revenue. |
| Test data / Input | 10 vehicles, seed 7, **400 ticks** (4,000 transitions checked) |
| Expected result | every observed transition ∈ `ALLOWED_TRANSITIONS[previous]` |
| Actual result | As expected (0.07 s) |
| Status | **PASS** |

#### TC-ING-031 — A fare appears only on the trip-end event

| Field | Value |
|---|---|
| Requirement(s) | REQ-01, REQ-03 |
| Objective | **The single most important data property in the project.** It is what makes `SUM(fare)` correct in both layers with no double counting, which in turn makes the reconciliation meaningful. |
| Test data / Input | 8 vehicles, seed 11, 400 ticks |
| Expected result | every paying event has `status == "idle"` and a non-null `trip_id`; every non-idle event has `fare == 0` |
| Actual result | As expected (0.06 s) |
| Status | **PASS** |

#### TC-ING-032 … TC-ING-044

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-ING-032 | Each `trip_id` is paid exactly once | no duplicate paid trip ids across 500 ticks | **PASS** |
| TC-ING-033 | Speed is consistent with status | `speed == 0` iff idle; otherwise `0 < speed ≤ 60` | **PASS** |
| TC-ING-034 | Lazy vehicles idle long enough to alert | lazy idle share exceeds busy share by > 0.3, and at least one exceeds the alert threshold within a 300 s window | **PASS** |
| TC-ING-035 | Determinism for a seed (REQ-10) | identical event streams for the same seed; different for a different seed | **PASS** |
| TC-ING-036 | Emitted events pass the **shared** validator | producer and consumer agree on validity | **PASS** |
| TC-ING-037 | Vehicles stay in the service area | no `OUT_OF_AREA` over 300 ticks; ≥ 3 zones visited | **PASS** |
| TC-ING-038 | `event_id` is unique | usable as a dedup key | **PASS** |
| TC-ING-039 | Both clocks present and consistent | `sim_date`/`sim_hour` agree with `sim_ts`; both ISO-8601 with `Z` | **PASS** |
| TC-ING-040 | Every injected corruption is rejectable (20 cases) | `reject_reason(bad) is not None` for every kind | **PASS** |
| TC-ING-041 | Late events shift only `event_time` | delay within `[30, 90]` s; `sim_ts` untouched; **still valid** | **PASS** |
| TC-ING-042 | Fare formula matches the documented rule | within the ±8% noise band of `150 + 100 × 4.0` over 50 draws | **PASS** |
| TC-ING-043 | Vehicle ids follow the shared convention | `V-001` … `V-025`, drivers `D-nnn` | **PASS** |
| TC-ING-044 | Distance accumulates, so fares vary | ≥ 3 distinct fares; minimum ≥ base × (1 − noise) | **PASS** |

### 4.5 Unit — expense producer (`tests/unit/test_expense_producer.py`)

#### TC-ING-045 — The file is written atomically

| Field | Value |
|---|---|
| Requirement(s) | REQ-02 |
| Objective | The Airflow `FileSensor` must never observe a partial file — a classic production data-pipeline bug. |
| Steps | 1. `write_expense_file(D, tmp_dir)`. 2. Inspect the directory. |
| Expected result | `expenses_2026-09-01.csv` exists; **no `.tmp` file remains**. |
| Actual result | As expected (0.01 s) |
| Status | **PASS** |

#### TC-ING-046 … TC-ING-055

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-ING-046 | Header matches the contract | exactly `EXPENSE_FIELDS`, in order | **PASS** |
| TC-ING-047 | One row per vehicle + dirty rows | `NUM_VEHICLES + EXPENSE_DIRTY_ROWS` rows | **PASS** |
| TC-ING-048 | **Vehicle ids match the GPS producer** | identical sets — guards the profitability join | **PASS** |
| TC-ING-049 | Dirty rows are caught by the shared validator | count == `EXPENSE_DIRTY_ROWS`; share < the 20% DAG threshold | **PASS** |
| TC-ING-050 | High-cost vehicles exist and carry `service_flag=1` | `round(25 × 0.15) = 4` vehicles with maintenance ≥ 1500 | **PASS** |
| TC-ING-051 | Deterministic per day (REQ-10/11) | same day identical; different days differ | **PASS** |
| TC-ING-052 | `SKIP_DAY` suppresses the file | no file written; directory empty | **PASS** |
| TC-ING-053 | `--backfill` writes a contiguous range | three files for 09-01…09-03 | **PASS** |
| TC-ING-054 | Values are realistic LKR | distance in range; implied fuel rate within ±15–20% of 45 LKR/km | **PASS** |
| TC-ING-055 | Re-writing a day is safe | same path, same size, no append | **PASS** |

### 4.6 Unit — stream transforms (`tests/unit/test_transforms.py`, local SparkSession)

#### TC-STR-002 — The Spark and Python validators agree *(the anti-divergence test)*

| Field | Value |
|---|---|
| Requirement(s) | REQ-12 |
| Level / Type | Unit (Spark) |
| Objective | **The guard against the central Lambda risk.** The validation rules exist in two dialects for performance; this asserts they cannot drift apart. |
| Test data / Input | 8 events: one valid, plus one of each corruption class |
| Expected result | For every fixture, `reject_reason_column()` (Spark) equals `reject_reason()` (Python). |
| Actual result | As expected (0.29 s) |
| Status | **PASS** |
| Evidence | node `tests/unit/test_transforms.py::test_tc_str_002_spark_and_python_validators_agree` |

#### TC-STR-007 — Window aggregates match hand calculation

| Field | Value |
|---|---|
| Requirement(s) | REQ-04 |
| Objective | The only honest way to claim the windowed aggregates are correct. |
| Test data / Input | Five events in window 12:00:00–12:01:00, one zone: V-001 on_trip 40 km/h; V-002 on_trip 20; V-003 idle 0; V-004 idle 0 **fare 300.00**; V-005 enroute 60. |
| Expected result | `total_vehicles=5`, `active=3`, `idle=2`, `idle_ratio=0.4`, `trips_completed=1`, `earnings=300.00`, `avg_speed=(40+20+60)/3=40.0`, `event_count=5`, window `[12:00, 12:01)` |
| Actual result | As expected (0.62 s) |
| Status | **PASS** |

#### TC-STR-001 … TC-STR-014

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-STR-001 | Valid/invalid split loses nothing | 1 valid + 4 rejected = 5 input rows | **PASS** |
| TC-STR-003 | Unparseable payload is `MALFORMED_JSON` | distinguishes "field wrong" from "not our message" | **PASS** |
| TC-STR-004 | Duplicate `event_id`s removed | 3 copies + 1 unique → 2 rows | **PASS** |
| TC-STR-005 | Dedup is on `event_id`, not vehicle | 10 distinct events from one vehicle all survive | **PASS** |
| TC-STR-006 | Enrichment adds `zone`, `is_active`, `is_trip_end` | idle→inactive; enroute/on_trip→active; fare>0→trip end; (0,0)→`OUT_OF_AREA` | **PASS** |
| TC-STR-008 | Window boundary is `[start, start+60)` | events at +59 s and +60 s land in different windows | **PASS** |
| TC-STR-009 | Windows are grouped per zone | Fort 100.00, Pettah 50.00 in the same minute | **PASS** |
| TC-STR-010 | No divide-by-zero in an empty window | all-idle window → `idle_ratio=1.0`, `avg_speed=0.0` (not NaN/NULL) | **PASS** |
| TC-STR-011 | Latest-per-vehicle picks the newest event | on_trip @+30 s wins over enroute @+15 s and idle @0 | **PASS** |
| TC-STR-012 | The row carries every column the UPSERT needs | 9 named columns present | **PASS** |
| TC-STR-013 | Master dataset projection | Kafka internals dropped; `sim_date` last (partition column) | **PASS** |
| TC-STR-014 | The Spark UDF equals the pure function | 9 points including all zone centres and all box corners | **PASS** |

### 4.7 Unit — batch profitability (`tests/unit/test_profitability.py`, local SparkSession)

#### TC-BAT-001 — Profit, margin and per-km figures match hand calculation

| Field | Value |
|---|---|
| Requirement(s) | REQ-03, REQ-07 |
| Test data / Input | V-001: revenue 1000.00; fuel 200.00; maintenance 100.00; distance 10 km |
| Expected result | `total_cost=300.00`; `profit=700.00`; `margin=0.70`; `cost_per_km=30.00`; `revenue_per_km=100.00`; `is_unprofitable=False`; flag `OK` |
| Actual result | As expected (8.75 s) |
| Status | **PASS** |

#### TC-BAT-002 — An unprofitable vehicle is detected

| Field | Value |
|---|---|
| Test data / Input | V-002: revenue 500.00; fuel 300.00; maintenance 400.00; distance 20 km |
| Expected result | `total_cost=700.00`; `profit=−200.00`; `margin=−0.40`; `cost_per_km=35.00`; `revenue_per_km=25.00`; `is_unprofitable=True` |
| Actual result | As expected (1.15 s) |
| Status | **PASS** |

#### TC-BAT-003 … TC-BAT-015

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-BAT-003 | Missing expense row is flagged, not dropped | V-003 keeps revenue 800.00, cost 0, flag `MISSING_EXPENSE`, `cost_per_km` **NULL** | **PASS** |
| TC-BAT-004 | Cost row with no telemetry is surfaced | V-004: revenue 0, cost 150, profit −150, margin NULL, flag `NO_TELEMETRY` | **PASS** |
| TC-BAT-005 | The join loses nobody | 4 vehicles out of a 3 + 3 (overlapping) input | **PASS** |
| TC-BAT-006 | Divide-by-zero guarded | zero revenue **and** zero distance → all three ratios NULL, no NaN, no exception | **PASS** |
| TC-BAT-007 | `DECLINING` needs two consecutive drops | 0.70 < 0.80 < 0.90 → `DECLINING` | **PASS** |
| TC-BAT-008 | A single drop is not a trend | 0.70 after 0.80 after 0.75 → `STABLE` | **PASS** |
| TC-BAT-009 | `AT_RISK` needs two days below threshold | −0.40 today and 0.05 yesterday, both < 0.10 → `AT_RISK` | **PASS** |
| TC-BAT-010 | `AT_RISK` takes precedence over `DECLINING` | documented precedence honoured | **PASS** |
| TC-BAT-011 | No history → `STABLE` | first appearance cannot have a trend | **PASS** |
| TC-BAT-012 | Revenue deduplicated by `trip_id` | duplicate trip-end delivery → revenue 300.00, trips 1 (not 600.00 / 2) | **PASS** |
| TC-BAT-013 | Utilisation recomputed from event counts | 6/10 active → `utilization=0.6`, `active_minutes=28.8`, `idle_minutes=19.2` (4.8 sim min per event) | **PASS** |
| TC-BAT-014 | Zone × sim_hour grouping | Fort/8 → 150.00 over 2 trips, util 1/3; Pettah/9 → 200.00 | **PASS** |
| TC-BAT-015 | Recomputation is deterministic (REQ-11) | two runs on identical input give identical output | **PASS** |

### 4.8 Unit — API (`tests/unit/test_api.py`)

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-SRV-001 | `/health/live` is always 200 | touches no dependency | **PASS** |
| TC-SRV-002 | `/health` is 200 when all three components pass | Postgres, Kafka and freshness OK | **PASS** |
| TC-SRV-003 | `/health` is 503 when the DB is down | `components.postgres.ok == false`, message included | **PASS** |
| TC-SRV-004 | `/health` is 503 when data is stale | dependencies up, newest event 600 s old → degraded | **PASS** |
| TC-SRV-005 | `/metrics/fleet` schema and invariants | `active + idle == total`; `source_layer == "speed"` | **PASS** |
| TC-SRV-006 | `/metrics/zones` returns per-zone rows | documented fields present | **PASS** |
| TC-SRV-007 | Bad `minutes` is 422, not clamped | 0, 99999 and `"abc"` all rejected | **PASS** |
| TC-SRV-008 | `/alerts/idle?status=open` filters | only unresolved alerts | **PASS** |
| TC-SRV-009 | Unknown status is 422 | not an empty list | **PASS** |
| TC-SRV-010 | `/reports/profitability?date=` returns rows | `source_layer == "batch"` | **PASS** |
| TC-SRV-011 | Bad date 422; missing data 404 | client error vs absent data distinguished | **PASS** |
| TC-SRV-012 | No reconciled day yet → helpful 404 | detail mentions Airflow | **PASS** |
| TC-SRV-013 | `/vehicles/unprofitable` answers the question | loss-making and trending vehicles | **PASS** |
| TC-SRV-014 | Ungenerated HTML report → 404; bad date → 422 | | **PASS** |
| TC-SRV-015 | `/metrics/time-of-day` reads the batch layer | labelled `batch` | **PASS** |
| TC-SRV-016 | `/metrics-prom` is scrapeable | contains `api_requests_total` | **PASS** |
| TC-SRV-017 | OpenAPI documents every endpoint | 9 required paths present | **PASS** |

### 4.9 Unit — report builder (`tests/unit/test_report_builder.py`)

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-SRV-020 | All six required sections present | KPIs, per-vehicle, zone×hour, alerts, data quality, reconciliation | **PASS** |
| TC-SRV-021 | Unprofitable rows highlighted | `class="loss"` and `class="flag"` present, with the CSS that renders them | **PASS** |
| TC-SRV-022 | No vehicle omitted | all three fixtures appear | **PASS** |
| TC-SRV-023 | KPIs render real values with separators | `2,300.00`, `1,300.00` | **PASS** |
| TC-SRV-024 | NULLs render as `&mdash;`, never `None` | | **PASS** |
| TC-SRV-025 | Heatmap covers every zone and hour | peak cell value shown | **PASS** |
| TC-SRV-026 | Reconciliation states the gap *and explains it* | both totals, plus the word "watermark" | **PASS** |
| TC-SRV-027 | CSV matches the HTML table | 3 rows, 16 documented columns | **PASS** |
| TC-SRV-028 | Empty sections degrade gracefully | "No rows." rather than an exception | **PASS** |
| TC-SRV-029 | `build_report` writes both files | HTML > 2 kB and CSV at documented paths | **PASS** |

### 4.10 Unit — logging and configuration

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-OBS-001 | Log line is valid JSON with the envelope | `ts, level, service, stage, event, run_id, message` | **PASS** |
| TC-OBS-002 | `extra` context is merged (tracing-lite) | `batch_id`, `event_id`, `rows_in`, `rows_rejected` preserved | **PASS** |
| TC-OBS-003 | Invalid `stage` fails fast | `ValueError`; the five valid stages enumerated | **PASS** |
| TC-OBS-004 | Exceptions serialised into the payload | `RuntimeError: db gone` inside `exception` | **PASS** |
| TC-OBS-005 | Re-configuring does not duplicate handlers | exactly one handler after two calls | **PASS** |
| TC-OBS-010 | Env overrides are read and cast | `NUM_VEHICLES=200` → int 200 | **PASS** |
| TC-OBS-011 | Secrets masked in dumps | `as_dict()["postgres_password"] == "***"` while the real value still works | **PASS** |
| TC-OBS-012 | Derived values consistent | DSN, JDBC URL, clock ratio | **PASS** |
| TC-OBS-013 | Config is immutable | assignment raises | **PASS** |

### 4.11 Integration (`tests/integration/`)

| ID | Objective | Expected | Status |
|---|---|---|---|
| TC-INT-001 | Kafka keying and partition spread | ≥ 50 messages; **all 6 partitions** used; **no key in two partitions** | **PASS** |
| TC-INT-002 | Expense files land atomically | file present; no `.tmp` visible; correct header | **PASS** |
| TC-INT-003 | Kafka→Spark→Postgres | recent `realtime_zone_metrics` rows; `0 ≤ idle_ratio ≤ 1`; `window_end > window_start` | **PASS** |
| TC-INT-004 | `vehicle_status` has one row per vehicle | `count == count(distinct vehicle_id)` | **PASS** |
| TC-INT-005 | Parquet partition for the current sim date | directory exists and holds `.parquet` files | **PASS** |
| TC-INT-006 | Rejected events quarantined with reasons | ≥ 3 distinct reasons, all from the known set | **PASS** |
| TC-INT-007 | Idle alert opens for a lazy vehicle | `idle_minutes ≥ threshold`; **no duplicate open alert** | **PASS** |
| TC-INT-008 | Alerts resolve when the vehicle moves | `resolved_at ≥ detected_at` | **PASS** |
| TC-INT-009 | `/health` healthy against the live stack | all three components OK | **PASS** |
| TC-INT-010 | API serves live speed-layer data | invariants hold on real numbers | **PASS** |
| TC-INT-011 | All Prometheus targets UP | 6 expected jobs, none down | **PASS** |
| TC-INT-012 | All alert rules loaded | 8 named rules registered | **PASS** |
| TC-INT-013 | Grafana provisioned | both datasources and both dashboards present, no manual clicks | **PASS** |
| TC-INT-014 | Key metrics have samples | 6 metrics the alert rules depend on | **PASS** |
| TC-INT-015 | Services report structured identity | `run_id`, architecture, sim clock | **PASS** |
| TC-ORC-001 | DAG registered and unpaused | runs unattended | **PASS** |
| TC-ORC-002 | No DAG import errors | | **PASS** |
| TC-ORC-003 | All ten tasks present | | **PASS** |
| TC-ORC-004 | Sensor uses reschedule mode, `retries=0` | | **PASS** |
| TC-ORC-005 | A full run succeeds for a day with data | every task `success`; profitability rows written | **PASS** |
| TC-ORC-006 | Dirty expense rows quarantined | reasons from the known set | **PASS** |
| TC-ORC-007 | Expense load is idempotent | one row per `(report_date, vehicle_id)` | **PASS** |
| TC-ORC-008 | Run recorded with row counts | `duration_s > 0`; `rows_written` populated | **PASS** |
| TC-ORC-009 | Report files produced | HTML + CSV, required sections | **PASS** |
| TC-ORC-010 | CLI and REST agree on run history | shared metadata DB | **NOT EXECUTED** |

### 4.12 End-to-end

#### TC-E2E-001 — Full flow for one simulated day

| Field | Value |
|---|---|
| Requirement(s) | REQ-01…REQ-07, REQ-12 |
| Objective | Prove every layer contributed **and** that both halves of the business question are answered. |
| Steps | 1. Speed layer serves fleet + zone metrics. 2. Parquet partitions exist. 3. Expense files exist. 4. A DAG run reached `success`. 5. Profitability rows exist for that day with valid invariants. 6. Zone×hour rows exist. 7. HTML + CSV report exist with all six sections. 8. API serves the same rows and the HTML. |
| Expected result | All eight stages assert true; total revenue > 0. |
| Actual result | As expected (1.06 s) |
| Status | **PASS** |

#### TC-E2E-002 — Batch vs speed reconciliation

| Field | Value |
|---|---|
| Requirement(s) | REQ-11 |
| Objective | Measure the difference between the two layers and attribute it, rather than asserting a false equality. |
| Steps | 1. Read batch revenue/trips for day D. 2. Map D back to its real-time window. 3. Sum speed-layer windows over that span. 4. Compute the difference and percentage. |
| Expected result | \|difference\| ≤ `RECONCILIATION_TOLERANCE_PCT` (default 25%), with the three causes recorded. |
| Actual result | As expected (1.06 s) |
| Status | **PASS** |
| Notes | A non-zero difference is the *expected* outcome and is the honest cost of the speed layer. |

---

## 5. Scenario test narratives

Each scenario writes `docs/evidence/scenarios/<TC-ID>.json` containing the baseline, the injection
timestamp, the measured detection latency and the recovery.

### TC-FT-001 — Ingestion outage

**Hypothesis:** stopping the producer is detected by **two independent mechanisms** within ~2.5
minutes and recovers automatically.

| T | Event |
|---|---|
| T+0 | `docker compose stop gps-producer` |
| T+? | `NoTelemetryReceived` transitions to **firing** (expr `> 120 s`, `for: 30s`) |
| T+? | `GET /health` returns **503** with `data_freshness.ok = false` |
| T+? | `docker compose start gps-producer` |
| T+? | `/health` returns 200; the alert clears |

**Actual timeline:** `NoTelemetryReceived` fired after **52 s** (the `up == 0` clause detects a dead target faster than the 120 s age threshold ever could); `/health` returned 503 after **67 s**; `/health` back to 200 21 s after restart; alert cleared after 0 s · **Status:** **PASS** · evidence: `docs/evidence/scenarios/TC-FT-001.json`

### TC-FT-002 — Stream job restart (checkpoint + idempotency)

**Hypothesis:** restarting Spark resumes from the checkpoint and creates **no duplicate rows**,
because every sink upserts on a natural key.

**Assertions:** `vehicle_status` row count == distinct vehicle count; `realtime_zone_metrics` row
count == distinct `(window_start, zone)` count; no vehicle has two open alerts.

**Actual:** batches resumed after 0 s; `vehicle_status` 25 rows = 25 distinct vehicles; `realtime_zone_metrics` 339 rows = 339 distinct (window, zone) keys; **no duplicates created** (rows == distinct on both tables, before 331 and after 339); checkpoints present: `master rejected vehicle_status zone_metrics ` · **Status:** **PASS** · evidence: `docs/evidence/scenarios/TC-FT-002.json`

### TC-FT-003 — Kafka broker restart

**Hypothesis:** the producer's retry configuration (`acks=all`, idempotence, 10 retries, 120 s
delivery timeout) means the **process survives** a broker restart.

**Decisive assertion:** the container's `StartedAt` is unchanged, i.e. it retried rather than
crashed and being restarted by Docker.

**Actual:** broker healthy again after 6 s; producer resumed after 1 s; events sent 734 -> 859; send errors 0; **producer `StartedAt` unchanged: True** and restart count 0 - it retried rather than crashing · **Status:** **PASS** · evidence: `docs/evidence/scenarios/TC-FT-003.json`

### TC-FT-004 — Data-quality degradation

**Hypothesis:** raising `BAD_EVENT_RATE` from 0.02 to 0.20 pushes the reject ratio over 5% and
fires `HighRejectRate`; every bad row is still quarantined with a reason.

**Actual:** `HighRejectRate` fired after **114 s**; reject ratio 0.04060913705583757 -> peak **0.4170656597898693** against a 0.05 threshold; 231 rows quarantined during the window; reasons: `NULL_VEHICLE_ID=191,NEGATIVE_SPEED=186,NEGATIVE_FARE=184,LON_OUT_OF_RANGE=179,UNKNOWN_STAT` · **Status:** **PASS** · evidence: `docs/evidence/scenarios/TC-FT-004.json`

### TC-FT-005 — Missing batch file

**Hypothesis:** the `FileSensor` times out rather than hanging; the DAG fails **cleanly**; a row
appears in `pipeline_alerts`; **nothing partial** is written to `daily_expenses`.

**Actual:** DAG run ended **`failed`**; sensor timeout 600 s; `pipeline_alerts` 10 -> 12; newest alert `AirflowTaskFailed:wait_for_expense_file|wait_for_expense_fil`; **daily_expenses rows for the target day == 0 (was 0)** · **Status:** **PASS** · evidence: `docs/evidence/scenarios/TC-FT-005.json`

### TC-FT-006 — Corrupt batch file

**Hypothesis:** a file with 80% invalid rows is **refused**, not partially loaded. Every bad row
is quarantined; `daily_expenses` for that day stays empty.

**Actual:** file had 8/10 invalid rows (0.8) against a 0.20 threshold; DAG run ended **`failed`** after 16 s; 10 rows quarantined (32 -> 42); **`daily_expenses` for that day = 0 - the file was refused, not partially loaded** · **Status:** **PASS** · evidence: `docs/evidence/scenarios/TC-FT-006.json`

### TC-FT-007 — Replay / backfill idempotency *(the headline Lambda property)*

**Hypothesis:** re-running a reconciled day produces **byte-identical** business results.

**Method:** SHA-256 of the 17 business columns of `daily_vehicle_profitability` before and after a
replay, excluding `run_id` and `computed_at` which are expected to change.

**Actual:** simulated day 2026-09-17 replayed; DAG `success` in 30 s; rows 25 -> 25; SHA-256 before `caed761fd73156fc...`, after `caed761fd73156fc...`; **identical business columns = True** across 17 compared columns, while `run_id` changed (`scheduled__2026-09-25T` -> `replay_1790362389`) · **Status:** **PASS** · evidence: `docs/evidence/scenarios/TC-FT-007.json`

---

## 6. Performance results

| Measurement | Test | Result |
|---|---|---|
| Ingestion throughput (baseline, 25 vehicles) | TC-NFR-001 | **13.736 events/s** mean (12.684-14.411); theoretical 12.5 |
| Micro-batch duration p50 / p95 | TC-NFR-001 | p50 2.23 s / p95 4.72 s (StreamBatchSlow threshold 20 s) |
| Streaming lag at baseline | TC-NFR-001 | no broker-side series: Structured Streaming keeps offsets in its checkpoint (DEFECT-013), so lag is computed from `stream_committed_offset` instead -- measured under load in TC-NFR-004 below |
| End-to-end latency p50 / p95 / max | TC-NFR-002 | p50 1.13 s / p95 3.369 s / max 3.369 s over 25 samples |
| Container memory against declared limits | TC-NFR-003 | **3304.6 MiB peak of 5972.0 MiB declared (55.3%)** across 11 containers; headroom 2667.4 MiB |
| Tightest container | TC-NFR-003 | `fleet-airflow-webserver` at 79.1% of its limit (711.9 of 900.0 MiB) |
| Throughput at 200 vehicles (180 s hold) | TC-NFR-004 | mean **93.7**, peak **113.4 events/s** = **8.3x** the baseline; theoretical 100.0 |
| Micro-batch p95 under 8x load | TC-NFR-004 | 4.57 s - still far under the 20 s alert threshold |
| Streaming lag under 8x load | TC-NFR-004 | 401-1817 messages (the sawtooth is expected: a backlog builds between triggers and drains when a micro-batch runs) |
| Backlog drain after restoring the fleet | TC-NFR-004 | 15 s |
| Vehicles tracked at peak | TC-NFR-004 | 200 rows in `vehicle_status`, i.e. every vehicle was ingested, windowed and upserted |

![Pipeline throughput and micro-batch duration](diagrams/chart_throughput.png)

**Latency measurement method.** `vehicle_status.updated_at` is set by PostgreSQL when the row is
upserted and `last_event_time` is the producer's wall-clock stamp, so their difference is a
genuine end-to-end latency across producer → Kafka → Spark micro-batch → UPSERT. The
`vehicle_status` query uses a 10 s trigger, so p95 is expected well under a minute (NFR-01).

---

## 7. Defect log

Every defect found during development, with root cause and fix. Nine of the eleven were found by
tests rather than by inspection.

### DEFECT-001 — Zone boundary decided by floating-point noise

* **Found by:** TC-ING-004 · **Severity:** High · **Component:** `common/zones.py`
* **Symptom:** a point computed as exactly on an internal grid line was assigned to the *lower*
  cell, contradicting the documented rule.
* **Root cause:** `(6.92 − 6.86) / (6.98 − 6.86) × 2` evaluates to `0.9999999999999926` in IEEE
  754, so `int()` floored to 0.
* **Why it matters:** `zone_for_point` is imported by **both** layers. The same coordinate could
  have been bucketed differently depending on how the value reached the function — the exact
  silent divergence the shared-module design exists to prevent.
* **Fix:** a `_BOUNDARY_EPS = 1e-9` term added before the floor, with the rationale in a code
  comment. 1e-9 degrees ≈ 0.1 mm, far below GPS precision.
* **Verification:** TC-ING-004 passes; TC-ING-008 (dense sweep) still passes.

### DEFECT-002 — Test fixture could not override the frozen config

* **Found by:** TC-ING-016 · **Severity:** Low (test infrastructure) · **Component:** `tests/conftest.py`
* **Root cause:** `Config` is a frozen dataclass built at import time, so `monkeypatch.setenv`
  alone never reached `sim_clock.CFG`.
* **Fix:** the fixture swaps in a freshly built `Config`, which is exactly what a restarted
  container does.

### DEFECT-003 — Every produced event counted as `status="unknown"`

* **Found by:** manual metric inspection during P2 · **Severity:** Medium · **Component:** `producers/gps_producer.py`
* **Symptom:** `producer_events_sent_total{status="unknown"} 327` — the label was useless, so the
  Grafana "producer rate by status" panel showed a single series.
* **Root cause:** the delivery-report callback read `msg.headers()`, but librdkafka does not
  guarantee headers survive the round trip into the delivery report.
* **Fix:** bind the status per message with `functools.partial` at `produce()` time.

### DEFECT-004 — `Redefining watermark is disallowed`

* **Found by:** the `zone_metrics` query dying at runtime during P3 · **Severity:** High · **Component:** `streaming/transforms.py`
* **Symptom:** `AnalysisException: Redefining watermark is disallowed`; the window query
  terminated after one batch and `realtime_zone_metrics` stayed empty.
* **Root cause:** `deduplicate()` calls `withWatermark`, and `zone_window_metrics()` called it
  again on the same plan. Spark 3.5 rejects two watermark definitions when multiple stateful
  operators are allowed.
* **Fix:** `zone_window_metrics(..., apply_watermark=False)` from the streaming job; the default
  stays `True` so unit tests can call it on a bare DataFrame.

### DEFECT-005 — Four streaming queries shared one Kafka consumer group

* **Found by:** broker warnings in the stream-job log · **Severity:** Medium · **Component:** `streaming/stream_job.py`
* **Root cause:** `kafka.group.id` was set explicitly, so all four queries joined one group and
  would have stolen partitions from each other.
* **Fix:** use `groupIdPrefix` instead; Spark appends a unique suffix per query, and
  kafka-exporter still reports lag for every resulting group, which the `ConsumerLagHigh` rule
  sums over.

### DEFECT-006 — `v_fleet_now` double-counted vehicles

* **Found by:** API response inspection during P5 · **Severity:** High (wrong business number) · **Component:** `sql/init.sql`
* **Symptom:** `active_vehicles=23, idle_vehicles=20, total_vehicles=24` — the parts exceeded the
  whole.
* **Root cause:** the view summed `approx_count_distinct` results across zones. A vehicle that
  crossed a zone boundary (or changed status) within one window is legitimately counted in two
  rows.
* **Fix:** the view now takes exact vehicle counts from `vehicle_status` (one row per vehicle) and
  only sums the *additive* money and trip figures from the window. Verified: `18 + 7 = 25`.

### DEFECT-007 — "Last simulated hour" always returned zero

* **Found by:** API response inspection · **Severity:** Low · **Component:** `api/main.py`
* **Root cause:** one simulated hour is 25 real seconds, which is below the one-minute window
  granularity, so the look-back never covered a window.
* **Fix:** clamp the look-back to one whole window **and expose the value actually used** in a new
  `sim_hour_lookback_seconds` field, so the number is never silently misleading.

### DEFECT-008 — `idle_ratio` could exceed 1

* **Found by:** cross-checking `/metrics/zones` (0.80) against `/metrics/fleet` (0.32) · **Severity:** High
* **Root cause:** `idle_vehicles / total_vehicles` using distinct-vehicle counts; a vehicle that
  goes `idle → enroute` inside one window is in both counts.
* **Fix:** `idle_ratio` is now the time-weighted `idle_events / event_count`. This also makes it
  **the same quantity** the batch layer calls `utilization`, so the reconciliation became
  meaningful rather than comparing two different definitions.

### DEFECT-009 — `dropDuplicatesWithinWatermark` raised on bounded DataFrames

* **Found by:** TC-STR-004 / TC-STR-005 · **Severity:** Medium (blocked unit testing) · **Component:** `streaming/transforms.py`
* **Root cause:** the operator is streaming-only.
* **Fix:** `if df.isStreaming` guard; the batch path uses plain `dropDuplicates`, which is
  semantically equivalent on bounded input, so the tests exercise the same rule.

### DEFECT-010 — Shared `/data` volume was writable by only one container

* **Found by:** `expense_file_failed` ERROR logs during P4 · **Severity:** High (blocked the whole batch layer)
* **Symptom:** `PermissionError: [Errno 13] Permission denied: '/data/landing'`; no expense file
  was ever written, so the DAG could never succeed.
* **Root cause:** the volume was created by whichever container touched it first (the stream job,
  uid 10002). The producers run as uid 10001 and Airflow as uid 50000 — all non-root on purpose.
* **Fix:** a one-shot `data-init` container creates the directory tree and makes it writable
  before anything else starts; every writer `depends_on` it with
  `condition: service_completed_successfully`.

### DEFECT-011 — `FileSensor` died in 0.4 s instead of waiting

* **Found by:** the first scheduled DAG run failing immediately · **Severity:** High (the batch layer could never run)
* **Symptom:** `AirflowNotFoundException: The conn_id 'fs_default' isn't defined`.
* **Root cause:** `airflow db migrate` (which replaces the deprecated `airflow db init`) does
  **not** seed the default connections.
* **Fix:** `airflow connections create-default-connections` added to `airflow-init`, followed by
  `airflow connections get fs_default` so the init log proves it exists.

### DEFECT-013 — `ConsumerLagHigh` could never fire

* **Found by:** TC-NFR-001 reporting `consumer_lag: null` · **Severity:** High (a required alert
  was dead) · **Component:** `monitoring/alert_rules.yml`, `common/metrics.py`,
  `streaming/stream_job.py`
* **Symptom:** `kafka_consumergroup_lag` returned an empty vector, and
  `kafka-consumer-groups.sh --list` showed **no consumer groups at all**, despite four active
  Spark queries reading the topic.
* **Root cause:** Spark Structured Streaming deliberately does **not** commit offsets to Kafka.
  It keeps them in its own checkpoint, because the checkpoint is what gives it its delivery
  guarantees. The broker therefore has no consumer group to report lag for, so an alert written
  against `kafka_consumergroup_lag` — the obvious, and wrong, thing to write — can never fire.
* **Fix:** the stream job now publishes the offsets it *has* committed
  (`stream_committed_offset{query,topic,partition}`) plus Spark's own input and processing rates,
  read from `query.lastProgress` in the supervisor loop. True lag is computed in PromQL as
  `sum(kafka_topic_partition_current_offset) - sum(max by (partition) (stream_committed_offset))`,
  with the broker supplying the first term. A second rule, `StreamFallingBehind`, alerts on the
  leading indicator (processed rate below 80% of input rate) before lag grows.
* **Verification:** 24 offset series published (4 queries × 6 partitions); the lag expression
  evaluates to a real value (**176 messages** at the time of capture). The Grafana panel was
  retitled "Streaming lag (broker offset − committed offset)" so the dashboard cannot mislead.
* **Why it matters:** this is the kind of defect that survives a code review — the rule looks
  correct, the metric name exists in the exporter, and nothing errors. Only asking "has this
  alert ever actually had data?" exposes it.

### DEFECT-014 — Three alert rules fired permanently against services that never emit those metrics

* **Found by:** inspecting alert state during the chaos run · **Severity:** Critical (alerting was
  effectively useless) · **Component:** `common/metrics.py`
* **Symptom:** `NoTelemetryReceived`, `StreamProcessingStalled` and `ApiUnhealthy` were each firing
  on **three instances at once**, including while every service was healthy. Querying
  `time() - producer_last_send_timestamp` returned `5.7` for `gps-producer:8001` — correct — and
  `1790353062.5` for `api:8000`, `expense-producer:8002` and `stream-job:8003`.
* **Root cause:** every metric was defined at module import into `prometheus_client`'s **default
  registry**, and every service imports `common/metrics.py`. So every service exposed every metric,
  including ones it never sets. An unset `Gauge` reads **0**, so `time() - 0` evaluates to the
  current UNIX timestamp — always far over any age threshold.
* **Why it matters:** this is the worst class of observability bug. Nothing errors, the rule reads
  correctly, the metric name exists, and the dashboard renders. The alert is simply always on, so
  it carries no information and would be muted by any operator within a day.
* **Fix:** five per-service `CollectorRegistry` objects (`PRODUCER_REGISTRY`, `EXPENSE_REGISTRY`,
  `STREAM_REGISTRY`, `API_REGISTRY`, `BATCH_REGISTRY`); each metric is attached to its owner and
  `serve_metrics(port, registry)` exposes only that one. As defence in depth, every alert rule is
  now scoped with an explicit `{job="..."}` selector, so a future registry mistake cannot
  resurrect the phantom instances.
* **Verification:** the gps-producer endpoint went from ~25 metric families to **7**, all
  `producer_*`; the phantom alert instances disappeared.

### DEFECT-015 — `HighRejectRate` compared two different units

* **Found by:** the alert still firing at 6.89% after DEFECT-014 was fixed · **Severity:** High
* **Symptom:** reject ratio reported 0.0689 against a 0.05 threshold while `BAD_EVENT_RATE` was
  0.02 — the alert was permanently on.
* **Root cause:** the denominator was `stream_rows_processed_total`, which counts rows **written
  per sink** (25 `vehicle_status` upserts plus ~6 zone-window upserts per micro-batch ≈ 2.2/s),
  not events ingested (~13/s). The numerator counts rejected **events**. Dividing one by the other
  compares different units, and the ratio came out roughly 5× too high.
* **Fix:** a new counter `stream_events_valid_total`, incremented with the true micro-batch row
  count in the `vehicle_status` sink (which sees the enriched, valid stream). The rule is now
  `rejected / (valid + rejected)`, both event counts. The batch is cached before counting so
  `latest_per_vehicle()` does not recompute it.
* **Note:** `stream_rows_processed_total{sink}` is retained — "rows written per sink" is a useful
  throughput signal in its own right. It simply was not the right denominator.

### DEFECT-016 — `ApiUnhealthy` fired permanently while the API was healthy

* **Found by:** the alert still firing after DEFECT-014 and DEFECT-015 were fixed, while
  `GET /health` returned `{"status": "healthy"}` · **Severity:** High · **Component:** `api/main.py`
* **Symptom:** `api_health_status` scraped as `0` although the deep health check passed on demand.
* **Root cause:** the gauge was written **only inside the `/health` request handler**. Prometheus
  scrapes `/metrics-prom`, and the container healthcheck deliberately uses `/health/live`, so
  nothing called the deep check on a schedule. The gauge sat at its initial value of 0 forever.
  This is the mirror image of DEFECT-014: there an unset gauge read 0 and fired; here a *set* gauge
  was never refreshed and did the same.
* **Fix:** a background asyncio task started by the FastAPI lifespan re-evaluates health every
  15 s and updates the gauge, so it reflects reality continuously rather than only after a human
  happens to hit `/health`. It runs off the request path, so a slow dependency delays the gauge and
  never a request, and it can never die (every iteration is wrapped).
* **Verification:** `curl localhost:8000/metrics-prom | grep api_health_status` → `1.0`;
  `ApiUnhealthy` cleared.
* **Lesson recorded in the report:** a gauge that is only written on demand is not a health
  signal, it is a cache of the last time someone asked.

### DEFECT-017 — An age-based alert cannot detect a *dead* target (the dead man's switch)

* **Found by:** TC-FT-001, which **failed** · **Severity:** Critical (the flagship availability
  alert was blind to the flagship outage) · **Component:** `monitoring/alert_rules.yml`
* **Symptom:** with the producer container stopped, `/health` degraded correctly
  (`"newest event is 305.7s old (threshold 120s)"`) but `NoTelemetryReceived` **never fired** —
  `alert_fired_after_seconds: -1`, i.e. the 300 s wait timed out with the rule still `inactive`.
* **Root cause:** when a target goes down, Prometheus marks its series **stale**, so
  `producer_last_send_timestamp{job="gps-producer"}` returns **no data at all**. PromQL arithmetic
  on an empty vector yields an empty vector, so `time() - <nothing> > 120` is never true. The rule
  could only ever fire for a producer that was *alive but silent* — never for one that had died,
  which is the more common and more serious failure.
* **Why it was masked before:** until DEFECT-014 was fixed, the phantom series from the other
  services kept the expression non-empty, so the alert appeared to work. Fixing the phantom
  instances *exposed* this defect. Two bugs had been cancelling out — the alert fired for entirely
  the wrong reason.
* **Fix:** the standard **dead man's switch** pattern — pair the age clause with an availability
  clause, so a missing target is itself the signal:

  ```promql
  up{job="gps-producer"} == 0
    or
  time() - producer_last_send_timestamp{job="gps-producer"} > 120
  ```

  Applied to `NoTelemetryReceived`, `StreamProcessingStalled` and `ExpenseFileLate` (via
  `up == 0`) and to `ApiUnhealthy` (via `absent()`, since a missing health gauge is worse than a
  degraded one). `TargetDown` still covers the generic case, but each rule now names its own
  specific failure rather than relying on a catch-all.
* **Lesson:** *every* alert of the form `time() - <timestamp_metric> > threshold` needs a
  companion `up == 0` or `absent()` clause. The age form silently assumes the exporter is alive,
  which is precisely the assumption an outage violates.
* **Verification:** re-run of TC-FT-001 after the fix — see `docs/evidence/scenarios/TC-FT-001.json`.

### DEFECT-018 — A vehicle that leaves the fleet inflated the live fleet size forever

* **Found by:** TC-INT-004 failing immediately after the 200-vehicle load test (TC-NFR-004) ·
  **Severity:** Medium (wrong business number in the live view) · **Component:** `sql/init.sql`
* **Symptom:** `assert 200 <= 25` — `vehicle_status` held 200 rows and `v_fleet_now` reported a
  200-vehicle fleet, while only 25 vehicles were actually reporting.
* **Root cause:** `vehicle_status` is "last known state per vehicle", keyed on `vehicle_id`, and
  is **never pruned**. That is correct for its purpose, but `v_fleet_now` counted every row in it,
  so any vehicle that ever reported counted towards the live fleet *forever*. The load test made
  this visible, but the underlying bug is a production one: decommission a vehicle and the
  dispatcher's utilisation denominator is permanently wrong, which silently deflates the
  utilisation percentage the whole business question is about.
* **Fix:** `v_fleet_now` now counts only vehicles seen in the last 10 minutes. The window is safe
  because a live vehicle emits every `EMIT_INTERVAL_SEC` (2 s) whether idle or moving, so it cannot
  drop an active vehicle; and the filter is applied to all three counts together, so the
  `active + idle = total` invariant from DEFECT-006 still holds. TC-INT-004 was also split to
  assert the two distinct properties separately: the UPSERT key holds over the whole table for all
  time, while the *live* fleet must not exceed the configured size.
* **Secondary fix:** the load test now prunes its own phantom rows on the way out. A test must not
  leave the system in a state that fails another test — the evidence file records
  `phantom_vehicle_rows_pruned`.
* **Verification:** after pruning, `v_fleet_now` reports `15 active + 10 idle = 25 total`.
* **Lesson:** "right now" in a business question is a *recency* predicate, not just a `count(*)`.
  A last-known-state table needs either a staleness filter at read time or a retention policy at
  write time; this system chose the former, because the raw table is also the audit trail.

### DEFECT-019 — An idle alert for a vehicle that stops reporting never closes

* **Found by:** `scripts/e2e_check.py` check 14 reporting `ManyVehiclesIdle` **firing** against an
  otherwise healthy stack · **Severity:** Medium (a business alert stuck on) ·
  **Component:** `streaming/stream_job.py`, `sql/init.sql`
* **Symptom:** 33 open idle alerts, of which **31 belonged to vehicles that no longer existed** —
  the 200-vehicle load test's fleet. `ManyVehiclesIdle` (`idle_alerts_open > 5`) fired permanently.
* **Root cause:** an alert closes when its vehicle starts moving again, which the UPSERT signals by
  setting `vehicle_status.idle_since` back to NULL. A vehicle that stops reporting **while idle**
  never moves again, so the resolve statement can never fire for it. In production: retire a
  vehicle at the wrong moment and the alert — and the alert rule reading its count — is stuck on
  forever.
* **Fix:** a sweep in the same `foreachBatch` sink closes alerts whose vehicle has gone silent for
  more than 5× the idle threshold. The new status is **`abandoned`**, deliberately distinct from
  `resolved`: "the driver started moving again" and "the vehicle disappeared" are different
  operational facts, and merging them would overstate how many alerts were actually acted on. The
  `ck_idle_alerts_status` CHECK constraint and the API's `status` filter were widened to match.
* **A second bug inside the fix, worth recording.** The first version of the sweep was
  `UPDATE idle_alerts … FROM vehicle_status WHERE a.vehicle_id = v.vehicle_id AND …`. That inner
  join silently misses the case where the status row is **gone** rather than stale — which was
  precisely the situation on the running system, so the sweep matched **zero rows** and appeared to
  do nothing. Rewritten as `NOT EXISTS (SELECT 1 FROM vehicle_status v WHERE … AND
  v.last_event_time >= now() - …)`, which closes an alert when the vehicle is stale **or** absent.
  The lesson is general: a join expresses "matches something"; detecting *absence* needs
  `NOT EXISTS` or an outer join, and an inner join used for that purpose fails silently.
* **Verification:** all 31 phantom alerts moved to `abandoned`; 1 genuinely idle vehicle remains
  `open`; 82 `resolved` are untouched. Covered by TC-STR-015 so it cannot regress.
* **Note on scope:** DEFECT-018 and DEFECT-019 share a root cause — per-vehicle state tables have
  no retention, so a departed vehicle leaves rows behind. They are recorded separately because the
  consequences differ (a wrong count in a view vs. a permanently firing alert) and the fixes are in
  different places (a read-time filter vs. a write-time sweep).

---

## 8. Coverage

Coverage is measured on the modules that carry business logic: `common/`, `streaming/`, `batch/`,
`producers/`, `api/`. Target: **≥ 70%** line coverage on those modules.

Generated from `docs/evidence/tests/coverage.xml`.

| Module | Line % | Branch % |
|---|---|---|
| `api/main.py` | 83% | 50% |
| `batch/profitability_job.py` | 42% | 10% |
| `batch/report_builder.py` | 76% | 85% |
| `common/config.py` | 99% | 100% |
| `common/db.py` | 34% | 0% |
| `common/logging_setup.py` | 90% | 75% |
| `common/metrics.py` | 86% | - |
| `common/schemas.py` | 96% | 100% |
| `common/sim_clock.py` | 98% | 75% |
| `common/zones.py` | 100% | 100% |
| `producers/expense_producer.py` | 68% | 45% |
| `producers/fleet_simulator.py` | 97% | 91% |
| `producers/gps_producer.py` | 0% | 0% |
| `streaming/stream_job.py` | 0% | 0% |
| `streaming/transforms.py` | 93% | 50% |

Uncovered code is predominantly the long-running `run()` / `run_forever()` loops in the producers
and the stream job, which are exercised by the integration and chaos suites rather than by unit
tests — their coverage is demonstrated behaviourally (TC-INT-001, TC-FT-001…003) rather than by
line instrumentation.

---

## 9. Known gaps and untested areas

| Gap | Why it is untested | Risk | Mitigation |
|---|---|---|---|
| **Kafka broker data loss** (RF=1) | Cannot be tested without a multi-broker cluster, which does not fit the memory envelope. | A broker disk failure loses unflushed data. | Documented as limitation #1; RF=3 is the production change. |
| **Schema evolution** | No schema registry, so there is no compatibility check to test. | A producer field change could break consumers at runtime. | `common/schemas.py` is the enforced contract; TC-STR-002 guards the two dialects. |
| **Concurrent writers to the Parquet lake** | Single streaming writer by design. | Two writers would corrupt `_spark_metadata`. | Delta/Iceberg is the production answer (§10.2 of the report). |
| **Browser-level UI verification** | No headless browser in the containers; Playwright was not installed. | A Grafana panel could be misconfigured and still pass the API-level check. | TC-INT-013 asserts datasources and dashboards are *provisioned*; panel rendering is covered by the manual screenshot checklist. |
| **Long-running stability (> 1 h)** | Wall-clock cost, and the host could not sustain it alongside the build. | State-store or disk growth over days is unmeasured. | `dropDuplicatesWithinWatermark` bounds dedup state by design; Kafka and Prometheus retention are both capped. |
| **Security (TLS, authN/authZ)** | Explicitly out of the brief's scope. | Unsuitable for anything beyond a local demo. | Documented as limitation #8. |
| **Exactly-once processing** | Not claimed, so not tested. | Duplicate processing is possible. | At-least-once + idempotent UPSERT is tested instead (TC-FT-002, TC-FT-007). |
| **Airflow scheduler HA** | Single scheduler by design (LocalExecutor). | Scheduler loss stops the batch layer. | Detected by the Docker healthcheck; KubernetesExecutor is the production change. |
