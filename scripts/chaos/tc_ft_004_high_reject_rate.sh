#!/usr/bin/env bash
# ==============================================================================================
# TC-FT-004 — Data-quality degradation.
#
# Hypothesis: if the upstream data source starts producing mostly junk, the pipeline must not
# silently ingest it.  Every bad row is quarantined with a reason, and the HighRejectRate alert
# fires once the reject ratio exceeds 5% over a 2-minute window.
#
# Injection : restart gps-producer with BAD_EVENT_RATE=0.2 (10x the normal 2%)
# Detection : HighRejectRate fires; rejected_events grows; stream_rows_rejected_total climbs
# Recovery  : restore BAD_EVENT_RATE and confirm the alert clears
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

TC="TC-FT-004"
BAD_RATE="${CHAOS_BAD_EVENT_RATE:-0.2}"
log "$TC: raising BAD_EVENT_RATE to $BAD_RATE"

baseline_rejected="$(sql_scalar 'SELECT count(*) FROM rejected_events;')"
baseline_ratio="$(prom_value 'sum(rate(stream_rows_rejected_total[2m])) / clamp_min(sum(rate(stream_rows_processed_total[2m])) + sum(rate(stream_rows_rejected_total[2m])), 0.001)')"
baseline_alert="$(alert_state HighRejectRate)"
log "baseline: rejected_rows=$baseline_rejected ratio=$baseline_ratio alert=$baseline_alert"

t_inject="$(now_iso)"
# Recreating the container with an env override is how a real config change would be rolled out.
BAD_EVENT_RATE="$BAD_RATE" $COMPOSE up -d --force-recreate --no-deps gps-producer >/dev/null 2>&1
log "INJECTED at $t_inject: gps-producer recreated with BAD_EVENT_RATE=$BAD_RATE"

# rate() needs 2 minutes of samples plus the rule's 1-minute `for` clause.
log "waiting for HighRejectRate to fire (expected ~180 s) ..."
alert_seconds="$(wait_for 'alert_state HighRejectRate' 'firing' 420 10)"

peak_ratio="$(prom_value 'sum(rate(stream_rows_rejected_total[2m])) / clamp_min(sum(rate(stream_rows_processed_total[2m])) + sum(rate(stream_rows_rejected_total[2m])), 0.001)')"
after_rejected="$(sql_scalar 'SELECT count(*) FROM rejected_events;')"
reasons="$(sql_scalar "SELECT string_agg(reason || '=' || n, ', ') FROM (SELECT reason, count(*) AS n FROM rejected_events GROUP BY reason ORDER BY n DESC) t;")"

log "RECOVERY: restoring the configured BAD_EVENT_RATE"
t_recover="$(now_iso)"
$COMPOSE up -d --force-recreate --no-deps gps-producer >/dev/null 2>&1
log "waiting for HighRejectRate to clear ..."
clear_seconds="$(wait_for 'alert_state HighRejectRate' 'inactive' 600 15)"

status="PASS"
[[ "$alert_seconds" == "-1" ]] && status="FAIL"
[[ "${after_rejected:-0}" -le "${baseline_rejected:-0}" ]] && status="FAIL"

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "Bad-event rate raised to ${BAD_RATE} - HighRejectRate fires and rows are quarantined",
  "requirements": ["REQ-09", "REQ-12"],
  "status": "$status",
  "baseline": {
    "rejected_event_rows": "$baseline_rejected",
    "reject_ratio": "$baseline_ratio",
    "alert_state": "$baseline_alert",
    "configured_bad_event_rate": "${BAD_EVENT_RATE:-0.02}"
  },
  "injection": {
    "at": "$t_inject",
    "action": "docker compose up -d --force-recreate gps-producer with BAD_EVENT_RATE=$BAD_RATE"
  },
  "detection": {
    "alert_name": "HighRejectRate",
    "alert_threshold": 0.05,
    "alert_fired_after_seconds": $alert_seconds,
    "peak_reject_ratio": "$peak_ratio",
    "rejected_event_rows_after": "$after_rejected",
    "rows_added": $(( ${after_rejected:-0} - ${baseline_rejected:-0} )),
    "reasons_observed": "$reasons"
  },
  "recovery": {
    "at": "$t_recover",
    "action": "recreate gps-producer with the configured rate",
    "alert_cleared_after_seconds": $clear_seconds
  },
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (alert in ${alert_seconds}s, cleared in ${clear_seconds}s)"
[[ "$status" == "PASS" ]]
