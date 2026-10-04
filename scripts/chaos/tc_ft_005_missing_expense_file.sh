#!/usr/bin/env bash
# ==============================================================================================
# TC-FT-005 — Missing batch source file.
#
# Hypothesis: when the daily expense file never arrives, the pipeline must fail *loudly and
# cleanly*: the Airflow FileSensor times out rather than hanging forever, the DAG run is marked
# failed, the on_failure_callback records a row in pipeline_alerts, and the ExpenseFileLate
# Prometheus alert fires.  Nothing partial is written to daily_expenses.
#
# Injection : trigger the DAG for a future simulated date whose file will never be produced,
#             with a short sensor timeout so the scenario completes inside a demo.
# Detection : DAG run state = failed; pipeline_alerts row; ExpenseFileLate (if the producer is
#             also stopped long enough)
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

TC="TC-FT-005"
# A date far enough in the simulated future that no file can exist for it.
MISSING_DATE="${CHAOS_MISSING_DATE:-2099-12-31}"
# The sensor times out after one simulated day (SIM_DAY_SECONDS, 600 s by default) and the
# sensor task carries retries=0, so one timeout is enough to fail the run.
SENSOR_TIMEOUT="${CHAOS_SENSOR_TIMEOUT:-${SIM_DAY_SECONDS:-600}}"

log "$TC: missing expense file for simulated day $MISSING_DATE"

alerts_before="$(sql_scalar 'SELECT count(*) FROM pipeline_alerts;')"
expenses_before="$(sql_scalar "SELECT count(*) FROM daily_expenses WHERE report_date = '$MISSING_DATE';")"
log "baseline: pipeline_alerts=$alerts_before, daily_expenses for that day=$expenses_before"

t_inject="$(now_iso)"
run_id="chaos_${TC}_$(date +%s)"
$COMPOSE exec -T airflow-scheduler airflow dags trigger daily_reconciliation \
  --run-id "$run_id" \
  --conf "{\"date\": \"$MISSING_DATE\"}" >/dev/null 2>&1
log "INJECTED at $t_inject: DAG triggered for $MISSING_DATE (run_id=$run_id)"

dag_state() {
  $COMPOSE exec -T airflow-scheduler airflow dags list-runs -d daily_reconciliation -o plain 2>/dev/null \
    | awk -v rid="$run_id" '$0 ~ rid {print $3}' | head -1
}

log "waiting for the DAG run to reach a terminal state (sensor timeout ${SENSOR_TIMEOUT}s) ..."
fail_seconds="$(wait_for 'dag_state' 'failed' $((SENSOR_TIMEOUT + 240)) 10)"
final_state="$(dag_state)"

alerts_after="$(sql_scalar 'SELECT count(*) FROM pipeline_alerts;')"
expenses_after="$(sql_scalar "SELECT count(*) FROM daily_expenses WHERE report_date = '$MISSING_DATE';")"
alert_row="$(sql_scalar "SELECT alert_name || ' | ' || coalesce(task_id,'-') || ' | ' || coalesce(left(message,120),'-') FROM pipeline_alerts ORDER BY created_at DESC LIMIT 1;")"
sensor_log="$($COMPOSE exec -T airflow-scheduler sh -c "grep -ril 'wait_for_expense_file' /opt/airflow/logs 2>/dev/null | head -1" 2>/dev/null | tr -d '\r')"
expense_file_alert="$(alert_state ExpenseFileLate)"

status="PASS"
[[ "$final_state" != "failed" ]] && status="FAIL"
[[ "${alerts_after:-0}" -le "${alerts_before:-0}" ]] && status="FAIL"
[[ "${expenses_after:-0}" != "0" ]] && status="FAIL"

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "Expense file never arrives - sensor times out, DAG fails cleanly, alert recorded",
  "requirements": ["REQ-02", "REQ-09"],
  "status": "$status",
  "baseline": {
    "pipeline_alert_rows": "$alerts_before",
    "daily_expense_rows_for_target_day": "$expenses_before"
  },
  "injection": {
    "at": "$t_inject",
    "action": "airflow dags trigger daily_reconciliation --conf {\\"date\\": \\"$MISSING_DATE\\"}",
    "run_id": "$run_id",
    "sensor_timeout_seconds": $SENSOR_TIMEOUT
  },
  "detection": {
    "dag_final_state": "$final_state",
    "failed_after_seconds": $fail_seconds,
    "pipeline_alert_rows_after": "$alerts_after",
    "newest_pipeline_alert": "$alert_row",
    "expense_file_late_alert_state": "$expense_file_alert",
    "sensor_log_path": "$sensor_log"
  },
  "assertions": {
    "no_partial_load": "daily_expenses rows for the target day == 0 (was $expenses_after)",
    "loud_failure": "DAG run state == failed and a pipeline_alerts row was written"
  },
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (DAG state=$final_state after ${fail_seconds}s)"
[[ "$status" == "PASS" ]]
