-- ============================================================================================
-- fleet-lambda — serving-layer schema (PostgreSQL 16)
--
-- Design principles
-- -----------------
-- 1. EVERY table that a pipeline writes has a natural primary key, so every write can be an
--    idempotent `INSERT ... ON CONFLICT DO UPDATE`.  Spark Structured Streaming gives
--    at-least-once delivery; combining it with idempotent upserts gives effectively-once
--    *storage*, which is what the business actually needs.  It is also what makes a backfill
--    re-run produce byte-identical rows (REQ-11).
-- 2. Speed-layer tables and batch-layer tables are kept separate (`realtime_*` vs `daily_*`).
--    They are two views of the same events at different latencies and accuracies; merging them
--    would hide the approximation the speed layer makes, which the reconciliation test measures.
-- 3. Quarantine tables (`rejected_events`, `rejected_expenses`) are first-class.  Bad data is
--    never silently dropped: it is stored with a reason so the data-quality section of the daily
--    report can be produced from real numbers.
-- 4. Comments are attached to every table/column that a marker might ask about in the viva.
-- ============================================================================================

-- --------------------------------------------------------------------------------------------
-- SPEED LAYER
-- --------------------------------------------------------------------------------------------

-- 1-minute tumbling window aggregates per zone, written by stream_job.py sink (b).
CREATE TABLE IF NOT EXISTS realtime_zone_metrics (
    window_start    TIMESTAMPTZ  NOT NULL,
    window_end      TIMESTAMPTZ  NOT NULL,
    zone            TEXT         NOT NULL,
    active_vehicles INTEGER      NOT NULL DEFAULT 0,
    idle_vehicles   INTEGER      NOT NULL DEFAULT 0,
    total_vehicles  INTEGER      NOT NULL DEFAULT 0,
    idle_ratio      NUMERIC(6,4) NOT NULL DEFAULT 0,
    trips_completed INTEGER      NOT NULL DEFAULT 0,
    earnings_lkr    NUMERIC(14,2) NOT NULL DEFAULT 0,
    avg_speed_kmh   NUMERIC(8,2) NOT NULL DEFAULT 0,
    event_count     INTEGER      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- The window is defined on event_time (REAL wall clock) so the dashboard moves in real time.
    CONSTRAINT pk_realtime_zone_metrics PRIMARY KEY (window_start, zone)
);
COMMENT ON TABLE realtime_zone_metrics IS
    'Speed layer: 1-minute tumbling window per zone, upserted from Spark foreachBatch. '
    'A window can be rewritten when late data arrives within the 2-minute watermark, which is '
    'why the write is an UPSERT rather than an INSERT.';
COMMENT ON COLUMN realtime_zone_metrics.active_vehicles IS
    'approx_count_distinct(vehicle_id) where status in (enroute,on_trip) — an approximation, by '
    'design, traded for speed-layer latency. The batch layer recomputes it exactly.';
COMMENT ON COLUMN realtime_zone_metrics.idle_ratio IS
    'Time-weighted idle share of the window: idle_events / event_count. NOT '
    'idle_vehicles/total_vehicles, because a vehicle that changes state inside a window '
    'appears in BOTH distinct-vehicle counts and that ratio can exceed 1. This definition '
    'also matches the batch layer''s utilization, making the two layers comparable.';

CREATE INDEX IF NOT EXISTS ix_rzm_window_start ON realtime_zone_metrics (window_start DESC);
CREATE INDEX IF NOT EXISTS ix_rzm_zone_window ON realtime_zone_metrics (zone, window_start DESC);

-- Latest known state of each vehicle, written by stream_job.py sink (c).
CREATE TABLE IF NOT EXISTS vehicle_status (
    vehicle_id       TEXT PRIMARY KEY,
    driver_id        TEXT,
    status           TEXT        NOT NULL,
    lat              DOUBLE PRECISION,
    lon              DOUBLE PRECISION,
    zone             TEXT,
    speed_kmh        NUMERIC(8,2),
    last_event_time  TIMESTAMPTZ NOT NULL,
    last_trip_id     TEXT,
    -- Set when the vehicle first reports idle; cleared the moment it moves again.  The idle
    -- alert rule is `now() - idle_since > IDLE_ALERT_MINUTES`, so this one column carries all
    -- the state the alerting needs — no separate state store.
    idle_since       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE vehicle_status IS
    'Speed layer: one row per vehicle, latest event wins. Also the freshness probe used by '
    'GET /health (now() - max(last_event_time) < FRESHNESS_SECONDS).';
COMMENT ON COLUMN vehicle_status.idle_since IS
    'Timestamp the current idle spell began; NULL when the vehicle is active.';

CREATE INDEX IF NOT EXISTS ix_vehicle_status_zone ON vehicle_status (zone);
CREATE INDEX IF NOT EXISTS ix_vehicle_status_idle ON vehicle_status (idle_since)
    WHERE idle_since IS NOT NULL;

-- Threshold-based idle alerts (the brief's required alert rule).
CREATE TABLE IF NOT EXISTS idle_alerts (
    id               BIGSERIAL PRIMARY KEY,
    vehicle_id       TEXT        NOT NULL,
    zone             TEXT,
    idle_since       TIMESTAMPTZ NOT NULL,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at      TIMESTAMPTZ,
    idle_minutes     NUMERIC(8,2),
    status           TEXT        NOT NULL DEFAULT 'open',
    -- 'abandoned' (DEFECT-019) is kept distinct from 'resolved': an alert whose vehicle
    -- started moving again was acted on, whereas one whose vehicle simply stopped
    -- reporting was not. Merging them would overstate how many alerts were addressed.
    CONSTRAINT ck_idle_alerts_status CHECK (status IN ('open', 'resolved', 'abandoned'))
);
COMMENT ON TABLE idle_alerts IS
    'One row per idle incident. Opened when a vehicle exceeds IDLE_ALERT_MINUTES, resolved when '
    'it becomes active again.';
-- Partial unique index: at most ONE open alert per vehicle.  This is what makes the alert
-- insert idempotent — replaying the same micro-batch cannot create duplicate alerts.
CREATE UNIQUE INDEX IF NOT EXISTS ux_idle_alerts_one_open
    ON idle_alerts (vehicle_id) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS ix_idle_alerts_detected ON idle_alerts (detected_at DESC);

-- Quarantine for telemetry that failed validation.
CREATE TABLE IF NOT EXISTS rejected_events (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT,
    vehicle_id   TEXT,
    reason       TEXT        NOT NULL,
    raw_payload  TEXT,
    batch_id     BIGINT,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE rejected_events IS
    'Quarantine sink. Bad events are stored with their reason and Kafka coordinates instead of '
    'being dropped, so the data-quality numbers in the daily report are real and an operator can '
    'replay them after a producer fix.';
CREATE INDEX IF NOT EXISTS ix_rejected_events_reason ON rejected_events (reason, rejected_at DESC);
CREATE INDEX IF NOT EXISTS ix_rejected_events_time ON rejected_events (rejected_at DESC);

-- --------------------------------------------------------------------------------------------
-- BATCH LAYER
-- --------------------------------------------------------------------------------------------

-- Validated rows of the daily expense CSV (the second, file-based source).
CREATE TABLE IF NOT EXISTS daily_expenses (
    report_date       DATE         NOT NULL,
    vehicle_id        TEXT         NOT NULL,
    fuel_cost         NUMERIC(12,2) NOT NULL,
    maintenance_cost  NUMERIC(12,2) NOT NULL,
    distance_covered  NUMERIC(10,2) NOT NULL,
    service_flag      SMALLINT     NOT NULL DEFAULT 0,
    source_file       TEXT,
    run_id            TEXT,
    loaded_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_daily_expenses PRIMARY KEY (report_date, vehicle_id)
);
COMMENT ON TABLE daily_expenses IS
    'Batch source of truth for costs. PK (report_date, vehicle_id) makes the Airflow load task '
    'idempotent: re-running the DAG for a day overwrites rather than duplicates.';

CREATE TABLE IF NOT EXISTS rejected_expenses (
    id          BIGSERIAL PRIMARY KEY,
    report_date DATE,
    raw_row     TEXT,
    reason      TEXT        NOT NULL,
    source_file TEXT,
    run_id      TEXT,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE rejected_expenses IS 'Quarantine for dirty rows of the expense CSV.';

-- The answer to the business question: per-vehicle profitability after costs.
CREATE TABLE IF NOT EXISTS daily_vehicle_profitability (
    report_date       DATE          NOT NULL,
    vehicle_id        TEXT          NOT NULL,
    driver_id         TEXT,
    revenue_lkr       NUMERIC(14,2) NOT NULL DEFAULT 0,
    trips             INTEGER       NOT NULL DEFAULT 0,
    active_minutes    NUMERIC(10,2) NOT NULL DEFAULT 0,
    idle_minutes      NUMERIC(10,2) NOT NULL DEFAULT 0,
    utilization       NUMERIC(6,4)  NOT NULL DEFAULT 0,
    distance_km       NUMERIC(10,2) NOT NULL DEFAULT 0,
    fuel_cost         NUMERIC(12,2) NOT NULL DEFAULT 0,
    maintenance_cost  NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_cost_lkr    NUMERIC(14,2) NOT NULL DEFAULT 0,
    profit_lkr        NUMERIC(14,2) NOT NULL DEFAULT 0,
    margin            NUMERIC(8,4),
    cost_per_km       NUMERIC(12,4),
    revenue_per_km    NUMERIC(12,4),
    is_unprofitable   BOOLEAN       NOT NULL DEFAULT FALSE,
    -- STABLE | DECLINING | AT_RISK — see batch/profitability_job.py for the rule.
    trend             TEXT          NOT NULL DEFAULT 'STABLE',
    data_quality_flag TEXT          NOT NULL DEFAULT 'OK',
    run_id            TEXT,
    computed_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_daily_vehicle_profitability PRIMARY KEY (report_date, vehicle_id),
    CONSTRAINT ck_dvp_trend CHECK (trend IN ('STABLE', 'DECLINING', 'AT_RISK')),
    CONSTRAINT ck_dvp_dq CHECK (data_quality_flag IN ('OK', 'MISSING_EXPENSE', 'NO_TELEMETRY'))
);
COMMENT ON TABLE daily_vehicle_profitability IS
    'Batch layer output: revenue recomputed from the Parquet master dataset, joined with the '
    'validated expense file. Re-running a date produces identical rows (Lambda recomputation).';
COMMENT ON COLUMN daily_vehicle_profitability.data_quality_flag IS
    'MISSING_EXPENSE = the vehicle had telemetry but no cost row (left join kept it, flagged). '
    'NO_TELEMETRY = a cost row with no trips that day.';
COMMENT ON COLUMN daily_vehicle_profitability.trend IS
    'DECLINING = margin fell on 2 consecutive days. AT_RISK = margin below MARGIN_THRESHOLD on '
    '2 consecutive days.';
CREATE INDEX IF NOT EXISTS ix_dvp_date_profit
    ON daily_vehicle_profitability (report_date, profit_lkr ASC);
CREATE INDEX IF NOT EXISTS ix_dvp_unprofitable
    ON daily_vehicle_profitability (report_date) WHERE is_unprofitable;

-- Zone x time-of-day view recomputed by the batch job (uses the SIMULATED hour).
CREATE TABLE IF NOT EXISTS daily_zone_summary (
    report_date   DATE          NOT NULL,
    zone          TEXT          NOT NULL,
    sim_hour      SMALLINT      NOT NULL,
    trips         INTEGER       NOT NULL DEFAULT 0,
    earnings_lkr  NUMERIC(14,2) NOT NULL DEFAULT 0,
    active_events INTEGER       NOT NULL DEFAULT 0,
    idle_events   INTEGER       NOT NULL DEFAULT 0,
    utilization   NUMERIC(6,4)  NOT NULL DEFAULT 0,
    avg_speed_kmh NUMERIC(8,2)  NOT NULL DEFAULT 0,
    run_id        TEXT,
    computed_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_daily_zone_summary PRIMARY KEY (report_date, zone, sim_hour),
    CONSTRAINT ck_dzs_hour CHECK (sim_hour BETWEEN 0 AND 23)
);
COMMENT ON TABLE daily_zone_summary IS
    'Batch layer: earnings/utilization by zone and simulated hour-of-day. This is the exact, '
    'recomputed counterpart of realtime_zone_metrics and is what the reconciliation test '
    'compares against.';

-- --------------------------------------------------------------------------------------------
-- ORCHESTRATION / OBSERVABILITY
-- --------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       TEXT PRIMARY KEY,
    dag_id       TEXT,
    report_date  DATE,
    status       TEXT        NOT NULL,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    duration_s   NUMERIC(10,2),
    rows_written JSONB,
    notes        TEXT,
    CONSTRAINT ck_pipeline_runs_status CHECK (status IN ('running', 'success', 'failed'))
);
COMMENT ON TABLE pipeline_runs IS
    'One row per Airflow DAG run. Grafana "Pipeline Health" reads it, and it gives the report a '
    'real audit trail of which simulated days were reconciled and how long each took.';
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_date ON pipeline_runs (report_date DESC);

CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id         BIGSERIAL PRIMARY KEY,
    alert_name TEXT        NOT NULL,
    severity   TEXT        NOT NULL DEFAULT 'warning',
    dag_id     TEXT,
    task_id    TEXT,
    run_id     TEXT,
    report_date DATE,
    message    TEXT,
    context    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_pipeline_alerts_sev CHECK (severity IN ('info', 'warning', 'critical'))
);
COMMENT ON TABLE pipeline_alerts IS
    'Written by the Airflow on_failure_callback (e.g. the expense-file sensor timing out). '
    'Complements the Prometheus alert rules with orchestration-level failures.';
CREATE INDEX IF NOT EXISTS ix_pipeline_alerts_created ON pipeline_alerts (created_at DESC);

-- --------------------------------------------------------------------------------------------
-- SERVING VIEWS
-- --------------------------------------------------------------------------------------------

-- fleet-wide vehicle counts must NOT be a SUM over the per-zone window rows.
-- Each realtime_zone_metrics row counts DISTINCT vehicles *within that zone*, so a vehicle that
-- crossed a zone boundary (or changed status) inside the same minute is counted in two rows.
-- Summing gave active+idle > total. The exact current fleet state lives in vehicle_status
-- (one row per vehicle, latest event wins), so the counts come from there; the window supplies
-- the money and trip figures, which ARE additive across zones.
CREATE OR REPLACE VIEW v_fleet_now AS
WITH latest AS (
    SELECT max(window_start) AS ws FROM realtime_zone_metrics
),
window_totals AS (
    SELECT m.window_start,
           m.window_end,
           sum(m.trips_completed)::INT               AS trips_completed,
           round(sum(m.earnings_lkr), 2)             AS earnings_lkr,
           round(avg(NULLIF(m.avg_speed_kmh, 0)), 2) AS avg_speed_kmh,
           count(*)::INT                             AS zones_reporting
    FROM realtime_zone_metrics m, latest
    WHERE m.window_start = latest.ws
    GROUP BY m.window_start, m.window_end
),
fleet_state AS (
    -- DEFECT-018: `vehicle_status` is "last known state per vehicle" and is never pruned, so a
    -- vehicle that leaves the fleet keeps a row forever and would inflate `total_vehicles`
    -- indefinitely.  This surfaced when the 200-vehicle load test (TC-NFR-004) left 175 stale
    -- rows behind and TC-INT-004 started reporting a 200-vehicle fleet.
    --
    -- "Right now" must mean recently seen.  A live vehicle emits every EMIT_INTERVAL_SEC (2 s)
    -- whether idle or moving, so a 10-minute window cannot drop an active vehicle, while a
    -- decommissioned or load-test vehicle ages out on its own.  The filter is applied to all
    -- three counts together, so the `active + idle = total` invariant still holds.
    SELECT count(*) FILTER (WHERE status IN ('enroute', 'on_trip'))::INT AS active_vehicles,
           count(*) FILTER (WHERE status = 'idle')::INT                  AS idle_vehicles,
           count(*)::INT                                                 AS total_vehicles
    FROM vehicle_status
    WHERE last_event_time > now() - INTERVAL '10 minutes'
)
SELECT w.window_start,
       w.window_end,
       f.active_vehicles,
       f.idle_vehicles,
       f.total_vehicles,
       CASE WHEN f.total_vehicles > 0
            THEN round(f.idle_vehicles::NUMERIC / f.total_vehicles, 4)
            ELSE 0 END AS idle_ratio,
       w.trips_completed,
       w.earnings_lkr,
       COALESCE(w.avg_speed_kmh, 0) AS avg_speed_kmh,
       w.zones_reporting
FROM window_totals w CROSS JOIN fleet_state f;

COMMENT ON VIEW v_fleet_now IS
    'Fleet snapshot: vehicle counts are exact (from vehicle_status), while trips/earnings are '
    'summed from the most recent 1-minute speed-layer window. Per-zone distinct counts are NOT '
    'summed because a vehicle can appear in two zones within one window.';

-- The "which vehicles are becoming unprofitable" half of the business question.
CREATE OR REPLACE VIEW v_unprofitable_vehicles AS
WITH latest AS (
    SELECT max(report_date) AS d FROM daily_vehicle_profitability
)
SELECT p.*
FROM daily_vehicle_profitability p, latest
WHERE p.report_date = latest.d
  AND (p.is_unprofitable OR p.trend IN ('DECLINING', 'AT_RISK'))
ORDER BY p.profit_lkr ASC;
COMMENT ON VIEW v_unprofitable_vehicles IS
    'Latest reconciled day: vehicles losing money or trending that way.';

-- Convenience view for the data-quality section of the daily report.
CREATE OR REPLACE VIEW v_data_quality_today AS
SELECT reason, count(*)::INT AS rows_rejected, max(rejected_at) AS last_seen
FROM rejected_events
WHERE rejected_at > now() - INTERVAL '24 hours'
GROUP BY reason
ORDER BY rows_rejected DESC;

-- --------------------------------------------------------------------------------------------
-- Grants: the application user owns everything (single-tenant demo stack).
-- --------------------------------------------------------------------------------------------
DO $$
BEGIN
    EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA public TO %I', current_user);
    EXECUTE format('GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO %I', current_user);
END
$$;
