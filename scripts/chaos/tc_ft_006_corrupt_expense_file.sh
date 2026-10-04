#!/usr/bin/env bash
# ==============================================================================================
# TC-FT-006 — Corrupt batch source file.
#
# Hypothesis: a mostly-invalid expense file must be REFUSED, not partially loaded.  The Airflow
# validation task quarantines every bad row in rejected_expenses and then fails the run because
# the bad-row share exceeds MAX_BAD_EXPENSE_ROW_PCT (20%).  daily_expenses must be untouched for
# that day: a half-loaded cost file would silently produce wrong profitability figures, which is
# far worse than a failed run.
#
# Injection : write an expenses_<date>.csv where most rows are invalid, then trigger the DAG
# Detection : validate_expense_file fails; rejected_expenses grows; daily_expenses stays empty
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

TC="TC-FT-006"
CORRUPT_DATE="${CHAOS_CORRUPT_DATE:-2098-11-11}"
log "$TC: corrupt expense file for simulated day $CORRUPT_DATE"

rejected_before="$(sql_scalar 'SELECT count(*) FROM rejected_expenses;')"
loaded_before="$(sql_scalar "SELECT count(*) FROM daily_expenses WHERE report_date = '$CORRUPT_DATE';")"
log "baseline: rejected_expenses=$rejected_before loaded_for_day=$loaded_before"

# 2 good rows out of 10 -> 80% bad, comfortably over the 20% threshold.
t_inject="$(now_iso)"
$COMPOSE exec -T airflow-scheduler python -c "
import csv, os
from pathlib import Path
landing = Path(os.getenv('LANDING_DIR', '/data/landing'))
landing.mkdir(parents=True, exist_ok=True)
target = landing / 'expenses_${CORRUPT_DATE}.csv'
tmp = target.with_name('.' + target.name + '.tmp')
cols = ['vehicle_id','fuel_cost','maintenance_cost','distance_covered','service_flag','report_date']
rows = [
    {'vehicle_id':'V-001','fuel_cost':'300.00','maintenance_cost':'120.00','distance_covered':'6.5','service_flag':'0','report_date':'${CORRUPT_DATE}'},
    {'vehicle_id':'V-002','fuel_cost':'280.00','maintenance_cost':'110.00','distance_covered':'6.1','service_flag':'0','report_date':'${CORRUPT_DATE}'},
]
for i in range(8):
    rows.append({'vehicle_id':'','fuel_cost':'NOT_A_NUMBER','maintenance_cost':'-5','distance_covered':'abc','service_flag':'9','report_date':'31/12/2098'})
with tmp.open('w', newline='', encoding='utf-8') as fh:
    w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(rows)
os.replace(tmp, target)
print('wrote', target, len(rows), 'rows (8 invalid)')
" 2>&1 | tail -2

run_id="chaos_${TC}_$(date +%s)"
$COMPOSE exec -T airflow-scheduler airflow dags trigger daily_reconciliation \
  --run-id "$run_id" --conf "{\"date\": \"$CORRUPT_DATE\"}" >/dev/null 2>&1
log "INJECTED at $t_inject: corrupt file written and DAG triggered (run_id=$run_id)"

dag_state() {
  $COMPOSE exec -T airflow-scheduler airflow dags list-runs -d daily_reconciliation -o plain 2>/dev/null \
    | awk -v rid="$run_id" '$0 ~ rid {print $3}' | head -1
}

log "waiting for the DAG run to fail at validation ..."
fail_seconds="$(wait_for 'dag_state' 'failed' 420 10)"
final_state="$(dag_state)"

rejected_after="$(sql_scalar 'SELECT count(*) FROM rejected_expenses;')"
loaded_after="$(sql_scalar "SELECT count(*) FROM daily_expenses WHERE report_date = '$CORRUPT_DATE';")"
reasons="$(sql_scalar "SELECT string_agg(reason || '=' || n, ', ') FROM (SELECT reason, count(*) AS n FROM rejected_expenses WHERE report_date = '$CORRUPT_DATE' GROUP BY reason) t;")"
alerts_row="$(sql_scalar "SELECT coalesce(task_id,'-') FROM pipeline_alerts ORDER BY created_at DESC LIMIT 1;")"

status="PASS"
[[ "$final_state" != "failed" ]] && status="FAIL"
[[ "${loaded_after:-0}" != "0" ]] && status="FAIL"
[[ "${rejected_after:-0}" -le "${rejected_before:-0}" ]] && status="FAIL"

# Clean up so the file does not linger and re-trigger on a later scheduled run.
$COMPOSE exec -T airflow-scheduler sh -c "rm -f /data/landing/expenses_${CORRUPT_DATE}.csv" >/dev/null 2>&1

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "Corrupt expense file (80% invalid rows) is refused, not partially loaded",
  "requirements": ["REQ-02", "REQ-12"],
  "status": "$status",
  "baseline": {
    "rejected_expense_rows": "$rejected_before",
    "daily_expense_rows_for_target_day": "$loaded_before"
  },
  "injection": {
    "at": "$t_inject",
    "file": "/data/landing/expenses_${CORRUPT_DATE}.csv",
    "rows_written": 10,
    "invalid_rows": 8,
    "bad_row_share": 0.8,
    "threshold": "${MAX_BAD_EXPENSE_ROW_PCT:-0.20}",
    "run_id": "$run_id"
  },
  "detection": {
    "dag_final_state": "$final_state",
    "failed_after_seconds": $fail_seconds,
    "rejected_expense_rows_after": "$rejected_after",
    "rows_quarantined": $(( ${rejected_after:-0} - ${rejected_before:-0} )),
    "quarantine_reasons": "$reasons",
    "failing_task_recorded_in_pipeline_alerts": "$alerts_row"
  },
  "assertions": {
    "no_partial_load": "daily_expenses rows for the target day == $loaded_after (expected 0)",
    "every_bad_row_kept": "rejected_expenses grew, so no bad row was silently dropped"
  },
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (DAG state=$final_state after ${fail_seconds}s)"
[[ "$status" == "PASS" ]]
