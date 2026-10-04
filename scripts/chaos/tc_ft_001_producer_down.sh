#!/usr/bin/env bash
# ==============================================================================================
# TC-FT-001 — Ingestion outage.
#
# Hypothesis: if the GPS producer stops, the system must notice within ~2.5 minutes through TWO
# independent mechanisms (a Prometheus alert and the API's own health check), and must recover
# automatically when ingestion is restored.
#
# Injection : docker compose stop gps-producer
# Detection : NoTelemetryReceived fires (expr: time() - producer_last_send_timestamp > 120, for 30s)
#             AND GET /health returns 503 with data_freshness.ok = false (FRESHNESS_SECONDS=120)
# Recovery  : docker compose start gps-producer -> alert clears, /health returns 200
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

TC="TC-FT-001"
log "$TC: ingestion outage (stop gps-producer)"

baseline_health="$(health_code)"
baseline_age="$(prom_value 'time() - producer_last_send_timestamp')"
baseline_alert="$(alert_state NoTelemetryReceived)"
log "baseline: /health=$baseline_health, telemetry age=${baseline_age}s, alert=$baseline_alert"

t_inject="$(now_iso)"
inject_epoch="$(now_epoch)"
$COMPOSE stop gps-producer >/dev/null 2>&1
log "INJECTED at $t_inject: gps-producer stopped"

# The alert needs >120 s of silence plus a 30 s `for` clause, so allow 300 s.
log "waiting for NoTelemetryReceived to fire (expected ~150 s) ..."
alert_seconds="$(wait_for 'alert_state NoTelemetryReceived' 'firing' 300 5)"

log "waiting for /health to report 503 ..."
health_seconds="$(wait_for 'health_code' '503' 300 5)"

health_body="$(curl -s "${API_URL}/health" 2>/dev/null | head -c 800)"
age_at_detection="$(prom_value 'time() - producer_last_send_timestamp')"

log "RECOVERY: restarting gps-producer"
t_recover="$(now_iso)"
$COMPOSE start gps-producer >/dev/null 2>&1

log "waiting for /health to return to 200 ..."
recover_seconds="$(wait_for 'health_code' '200' 300 5)"
log "waiting for NoTelemetryReceived to clear ..."
alert_clear_seconds="$(wait_for 'alert_state NoTelemetryReceived' 'inactive' 300 5)"

status="PASS"
[[ "$alert_seconds" == "-1" ]] && status="FAIL"
[[ "$health_seconds" == "-1" ]] && status="FAIL"
[[ "$recover_seconds" == "-1" ]] && status="FAIL"

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "GPS producer stopped - NoTelemetryReceived fires and /health degrades",
  "requirements": ["REQ-08", "REQ-09"],
  "status": "$status",
  "baseline": {
    "health_code": "$baseline_health",
    "telemetry_age_seconds": "$baseline_age",
    "alert_state": "$baseline_alert"
  },
  "injection": {
    "at": "$t_inject",
    "action": "docker compose stop gps-producer"
  },
  "detection": {
    "alert_name": "NoTelemetryReceived",
    "alert_fired_after_seconds": $alert_seconds,
    "alert_threshold_seconds": 120,
    "alert_for_clause_seconds": 30,
    "health_503_after_seconds": $health_seconds,
    "telemetry_age_at_detection_seconds": "$age_at_detection",
    "health_body_excerpt": $(python3 -c "import json,sys; print(json.dumps(sys.argv[1]))" "$health_body")
  },
  "recovery": {
    "at": "$t_recover",
    "action": "docker compose start gps-producer",
    "health_200_after_seconds": $recover_seconds,
    "alert_cleared_after_seconds": $alert_clear_seconds
  },
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (alert ${alert_seconds}s, health ${health_seconds}s, recovery ${recover_seconds}s)"
[[ "$status" == "PASS" ]]
