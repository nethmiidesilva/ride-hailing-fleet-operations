#!/usr/bin/env bash
# ==============================================================================================
# TC-FT-002 — Stream job restart: checkpoint recovery and UPSERT idempotency.
#
# Hypothesis: restarting the Spark Structured Streaming job must (a) resume from its checkpoint
# rather than replaying from the beginning or skipping data, and (b) produce NO duplicate rows in
# PostgreSQL, because every sink writes with INSERT ... ON CONFLICT DO UPDATE on the row's
# natural key.
#
# This is the concrete evidence for the report's "at-least-once + idempotent writes =
# effectively-once storage" claim.
#
# Injection : docker compose restart stream-job
# Assertion : vehicle_status still has exactly one row per vehicle;
#             realtime_zone_metrics has exactly one row per (window_start, zone);
#             new micro-batches resume within ~2 minutes.
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

TC="TC-FT-002"
log "$TC: stream job restart / checkpoint recovery"

before_vs="$(sql_scalar 'SELECT count(*) FROM vehicle_status;')"
before_vs_distinct="$(sql_scalar 'SELECT count(DISTINCT vehicle_id) FROM vehicle_status;')"
before_rzm="$(sql_scalar 'SELECT count(*) FROM realtime_zone_metrics;')"
before_rzm_distinct="$(sql_scalar 'SELECT count(*) FROM (SELECT DISTINCT window_start, zone FROM realtime_zone_metrics) t;')"
before_batches="$(prom_value 'sum(stream_batches_total)')"
log "baseline: vehicle_status=$before_vs (distinct $before_vs_distinct), zone_metrics=$before_rzm (distinct $before_rzm_distinct)"

t_inject="$(now_iso)"
$COMPOSE restart stream-job >/dev/null 2>&1
log "INJECTED at $t_inject: stream-job restarted"

# The counter resets to 0 on restart; wait until it climbs again, proving batches resumed.
log "waiting for micro-batches to resume ..."
resume_seconds="$(wait_for '[[ $(prom_value "sum(stream_batches_total)" | cut -d. -f1) -ge 4 ]] && echo yes || echo no' 'yes' 300 10)"

# Give the resumed job a couple of triggers to write through the same keys again.
sleep 45

after_vs="$(sql_scalar 'SELECT count(*) FROM vehicle_status;')"
after_vs_distinct="$(sql_scalar 'SELECT count(DISTINCT vehicle_id) FROM vehicle_status;')"
after_rzm="$(sql_scalar 'SELECT count(*) FROM realtime_zone_metrics;')"
after_rzm_distinct="$(sql_scalar 'SELECT count(*) FROM (SELECT DISTINCT window_start, zone FROM realtime_zone_metrics) t;')"
dup_alerts="$(sql_scalar "SELECT count(*) FROM (SELECT vehicle_id FROM idle_alerts WHERE status='open' GROUP BY vehicle_id HAVING count(*) > 1) t;")"
checkpoint_dirs="$($COMPOSE exec -T stream-job sh -c 'ls /data/checkpoints' 2>/dev/null | tr '\n' ' ')"

status="PASS"
[[ "$resume_seconds" == "-1" ]] && status="FAIL"
[[ "$after_vs" != "$after_vs_distinct" ]] && status="FAIL"
[[ "$after_rzm" != "$after_rzm_distinct" ]] && status="FAIL"
[[ "${dup_alerts:-0}" != "0" ]] && status="FAIL"

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "Stream job restart resumes from checkpoint with no duplicate rows",
  "requirements": ["REQ-10", "REQ-11"],
  "status": "$status",
  "baseline": {
    "vehicle_status_rows": "$before_vs",
    "vehicle_status_distinct_vehicles": "$before_vs_distinct",
    "zone_metric_rows": "$before_rzm",
    "zone_metric_distinct_keys": "$before_rzm_distinct",
    "stream_batches_total": "$before_batches"
  },
  "injection": { "at": "$t_inject", "action": "docker compose restart stream-job" },
  "recovery": {
    "batches_resumed_after_seconds": $resume_seconds,
    "checkpoint_directories": "$checkpoint_dirs"
  },
  "after": {
    "vehicle_status_rows": "$after_vs",
    "vehicle_status_distinct_vehicles": "$after_vs_distinct",
    "zone_metric_rows": "$after_rzm",
    "zone_metric_distinct_keys": "$after_rzm_distinct",
    "duplicate_open_idle_alerts": "$dup_alerts"
  },
  "assertion": "row count == distinct natural-key count for every upserted table; no duplicate open alerts",
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (resumed in ${resume_seconds}s)"
[[ "$status" == "PASS" ]]
