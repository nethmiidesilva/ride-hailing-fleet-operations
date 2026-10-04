#!/usr/bin/env bash
# ==============================================================================================
# TC-FT-007 — Replay / backfill idempotency.  This is the headline Lambda property.
#
# Hypothesis: re-running the batch layer for a simulated day that has already been reconciled
# must produce byte-identical business results.  That is what makes a Lambda architecture safe
# to correct: if a bug is found, or a corrected cost file arrives, the day is simply recomputed
# from the immutable Parquet master dataset and the answer converges.
#
# Method : capture daily_vehicle_profitability for day D, trigger the DAG for D again, capture
#          again, and diff.  Only the bookkeeping columns (run_id, computed_at) may differ.
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

TC="TC-FT-007"
TARGET_DATE="${1:-}"

if [[ -z "$TARGET_DATE" ]]; then
  TARGET_DATE="$(sql_scalar 'SELECT max(report_date) FROM daily_vehicle_profitability;')"
fi
if [[ -z "$TARGET_DATE" || "$TARGET_DATE" == "" ]]; then
  log "$TC: SKIPPED - no reconciled day exists yet (run the DAG at least once first)"
  write_evidence "$TC" "{\"test_case\":\"$TC\",\"status\":\"NOT EXECUTED\",\"reason\":\"no reconciled day exists yet\",\"captured_at\":\"$(now_iso)\"}"
  exit 0
fi

log "$TC: replaying simulated day $TARGET_DATE"

# The business columns only — run_id and computed_at are expected to change.
SNAPSHOT_SQL="SELECT vehicle_id, revenue_lkr, trips, active_minutes, idle_minutes, utilization,
 distance_km, fuel_cost, maintenance_cost, total_cost_lkr, profit_lkr, margin, cost_per_km,
 revenue_per_km, is_unprofitable, trend, data_quality_flag
 FROM daily_vehicle_profitability WHERE report_date = '$TARGET_DATE' ORDER BY vehicle_id"

snapshot() {
  $COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-fleet}" -d "${POSTGRES_DB:-fleet}" \
    -tAF'|' -c "$SNAPSHOT_SQL" 2>/dev/null
}

before="$(snapshot)"
before_rows="$(printf '%s\n' "$before" | grep -c . || true)"
before_hash="$(printf '%s' "$before" | sha256sum | cut -d' ' -f1)"
before_run_id="$(sql_scalar "SELECT run_id FROM daily_vehicle_profitability WHERE report_date='$TARGET_DATE' LIMIT 1;")"
log "before: $before_rows rows, sha256=$before_hash, run_id=$before_run_id"

t_inject="$(now_iso)"
run_id="replay_$(date +%s)"
$COMPOSE exec -T airflow-scheduler airflow dags trigger daily_reconciliation \
  --run-id "$run_id" --conf "{\"date\": \"$TARGET_DATE\"}" >/dev/null 2>&1
log "REPLAY triggered at $t_inject (run_id=$run_id)"

dag_state() {
  $COMPOSE exec -T airflow-scheduler airflow dags list-runs -d daily_reconciliation -o plain 2>/dev/null \
    | awk -v rid="$run_id" '$0 ~ rid {print $3}' | head -1
}

log "waiting for the replay run to succeed ..."
replay_seconds="$(wait_for 'dag_state' 'success' 600 10)"
final_state="$(dag_state)"

after="$(snapshot)"
after_rows="$(printf '%s\n' "$after" | grep -c . || true)"
after_hash="$(printf '%s' "$after" | sha256sum | cut -d' ' -f1)"
after_run_id="$(sql_scalar "SELECT run_id FROM daily_vehicle_profitability WHERE report_date='$TARGET_DATE' LIMIT 1;")"
log "after:  $after_rows rows, sha256=$after_hash, run_id=$after_run_id"

identical="false"
[[ "$before_hash" == "$after_hash" ]] && identical="true"
diff_excerpt=""
if [[ "$identical" == "false" ]]; then
  diff_excerpt="$(diff <(printf '%s' "$before") <(printf '%s' "$after") | head -20 | tr '\n' ' ')"
fi

status="PASS"
[[ "$final_state" != "success" ]] && status="FAIL"
[[ "$identical" != "true" ]] && status="FAIL"
[[ "$before_rows" != "$after_rows" ]] && status="FAIL"

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "Replaying a reconciled simulated day produces identical business results",
  "requirements": ["REQ-10", "REQ-11"],
  "status": "$status",
  "target_simulated_date": "$TARGET_DATE",
  "before": {
    "rows": $before_rows,
    "sha256_of_business_columns": "$before_hash",
    "run_id": "$before_run_id"
  },
  "replay": {
    "at": "$t_inject",
    "airflow_run_id": "$run_id",
    "dag_final_state": "$final_state",
    "completed_after_seconds": $replay_seconds
  },
  "after": {
    "rows": $after_rows,
    "sha256_of_business_columns": "$after_hash",
    "run_id": "$after_run_id"
  },
  "identical_business_columns": $identical,
  "columns_compared": "vehicle_id, revenue_lkr, trips, active_minutes, idle_minutes, utilization, distance_km, fuel_cost, maintenance_cost, total_cost_lkr, profit_lkr, margin, cost_per_km, revenue_per_km, is_unprofitable, trend, data_quality_flag",
  "columns_excluded": "run_id, computed_at (bookkeeping - expected to change)",
  "diff_excerpt": "$diff_excerpt",
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (identical=$identical, replay took ${replay_seconds}s)"
[[ "$status" == "PASS" ]]
