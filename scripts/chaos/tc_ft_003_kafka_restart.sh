#!/usr/bin/env bash
# ==============================================================================================
# TC-FT-003 — Broker restart: producer resilience.
#
# Hypothesis: restarting the single Kafka broker must not crash the producer.  It is configured
# with retries=10, retry.backoff.ms=500, delivery.timeout.ms=120000 and idempotence enabled, so
# in-flight messages are retried and re-delivered exactly once when the broker returns.
#
# Injection : docker compose restart kafka
# Assertion : the gps-producer container never exits; producer_events_sent_total resumes growing;
#             producer_send_errors_total may be > 0 (retries exhausted for some in-flight batch)
#             but the process survives and the offset stream continues.
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"

TC="TC-FT-003"
log "$TC: Kafka broker restart"

producer_started_before="$(docker inspect -f '{{.State.StartedAt}}' fleet-gps-producer 2>/dev/null)"
restarts_before="$(docker inspect -f '{{.RestartCount}}' fleet-gps-producer 2>/dev/null)"
sent_before="$(prom_value 'sum(producer_events_sent_total)')"
errors_before="$(prom_value 'producer_send_errors_total')"
log "baseline: sent=$sent_before errors=$errors_before restarts=$restarts_before"

t_inject="$(now_iso)"
$COMPOSE restart kafka >/dev/null 2>&1
log "INJECTED at $t_inject: kafka restarted"

log "waiting for the broker to report healthy again ..."
broker_seconds="$(wait_for "docker inspect -f '{{.State.Health.Status}}' fleet-kafka 2>/dev/null" 'healthy' 240 5)"

# Has the acknowledged-send counter moved past its pre-injection value?
sent_increased() {
  local current
  current="$(prom_value 'sum(producer_events_sent_total)')"
  python3 -c "
before = float('${sent_before:-0}' or 0)
current = float('${current:-0}' or 0)
print('yes' if current > before else 'no')
" 2>/dev/null || echo "no"
}

log "waiting for the producer to resume sending ..."
resume_seconds="$(wait_for 'sent_increased' 'yes' 300 10)"

sent_after="$(prom_value 'sum(producer_events_sent_total)')"
errors_after="$(prom_value 'producer_send_errors_total')"
producer_started_after="$(docker inspect -f '{{.State.StartedAt}}' fleet-gps-producer 2>/dev/null)"
restarts_after="$(docker inspect -f '{{.RestartCount}}' fleet-gps-producer 2>/dev/null)"
producer_state="$(docker inspect -f '{{.State.Status}}' fleet-gps-producer 2>/dev/null)"

status="PASS"
[[ "$broker_seconds" == "-1" ]] && status="FAIL"
[[ "$resume_seconds" == "-1" ]] && status="FAIL"
[[ "$producer_state" != "running" ]] && status="FAIL"
# The decisive assertion: the producer process survived (same start time, no extra restart).
survived="true"
if [[ "$producer_started_before" != "$producer_started_after" ]]; then survived="false"; status="FAIL"; fi

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "Kafka broker restart - producer retries and does not crash",
  "requirements": ["REQ-01", "REQ-08"],
  "status": "$status",
  "baseline": {
    "events_sent_total": "$sent_before",
    "send_errors_total": "$errors_before",
    "producer_restart_count": "$restarts_before",
    "producer_started_at": "$producer_started_before"
  },
  "injection": { "at": "$t_inject", "action": "docker compose restart kafka" },
  "recovery": {
    "broker_healthy_after_seconds": $broker_seconds,
    "producer_resumed_after_seconds": $resume_seconds
  },
  "after": {
    "events_sent_total": "$sent_after",
    "send_errors_total": "$errors_after",
    "producer_restart_count": "$restarts_after",
    "producer_started_at": "$producer_started_after",
    "producer_state": "$producer_state",
    "producer_process_survived": $survived
  },
  "producer_config": {
    "acks": "all",
    "enable.idempotence": true,
    "retries": 10,
    "retry.backoff.ms": 500,
    "delivery.timeout.ms": 120000
  },
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (broker back in ${broker_seconds}s, producer resumed in ${resume_seconds}s)"
[[ "$status" == "PASS" ]]
