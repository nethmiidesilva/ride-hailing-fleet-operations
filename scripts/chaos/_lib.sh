#!/usr/bin/env bash
# Shared helpers for the chaos scenarios.  Sourced, never executed directly.

COMPOSE="${COMPOSE_CMD:-docker compose}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
API_URL="${API_URL:-http://localhost:8000}"
EVIDENCE_DIR="${EVIDENCE_DIR:-docs/evidence/scenarios}"

now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
now_epoch() { date +%s; }

log() { printf '[%s] %s\n' "$(now_iso)" "$*"; }

# Query Prometheus for the state of one alert. Prints: firing | pending | inactive
alert_state() {
  local name="$1"
  local json
  json="$(curl -sf "${PROM_URL}/api/v1/alerts" 2>/dev/null)" || { echo "unknown"; return; }
  # Any instance of the alert firing counts as firing.
  if echo "$json" | grep -q "\"alertname\":\"${name}\""; then
    if echo "$json" | python3 -c "
import json,sys
data=json.load(sys.stdin)['data']['alerts']
states=[a['state'] for a in data if a['labels'].get('alertname')=='${name}']
print('firing' if 'firing' in states else ('pending' if 'pending' in states else 'inactive'))
" 2>/dev/null; then
      return
    fi
  fi
  echo "inactive"
}

# Evaluate a PromQL expression and print the scalar value (or empty).
prom_value() {
  local expr="$1"
  curl -sf -G "${PROM_URL}/api/v1/query" --data-urlencode "query=${expr}" 2>/dev/null \
    | python3 -c "
import json,sys
try:
    r=json.load(sys.stdin)['data']['result']
    print(r[0]['value'][1] if r else '')
except Exception:
    print('')
" 2>/dev/null
}

# HTTP status code of the deep health check.
health_code() {
  curl -s -o /dev/null -w '%{http_code}' "${API_URL}/health" 2>/dev/null || echo "000"
}

# Run one SQL statement and print the raw scalar result.
sql_scalar() {
  $COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-fleet}" -d "${POSTGRES_DB:-fleet}" \
    -tAc "$1" 2>/dev/null | head -1 | tr -d '[:space:]'
}

# Poll `condition_cmd` until it prints the expected value or the timeout expires.
# Prints the number of seconds it took, or -1 on timeout.
wait_for() {
  local condition_cmd="$1" expected="$2" timeout_s="${3:-180}" interval="${4:-5}"
  local start elapsed value
  start="$(now_epoch)"
  while :; do
    value="$(eval "$condition_cmd")"
    if [[ "$value" == "$expected" ]]; then
      echo $(( $(now_epoch) - start ))
      return 0
    fi
    elapsed=$(( $(now_epoch) - start ))
    if (( elapsed >= timeout_s )); then
      echo "-1"
      return 1
    fi
    sleep "$interval"
  done
}

# Write a scenario's evidence file.  Arguments: <TC-ID> <json-body>
write_evidence() {
  local tc_id="$1" body="$2"
  mkdir -p "$EVIDENCE_DIR"
  printf '%s\n' "$body" > "${EVIDENCE_DIR}/${tc_id}.json"
  log "evidence written to ${EVIDENCE_DIR}/${tc_id}.json"
}
