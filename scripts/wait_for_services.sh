#!/usr/bin/env bash
# ==============================================================================================
# Block until every container that declares a healthcheck reports "healthy".
#
# Used by `make up` and by the README quick start so the very next command (a curl, a psql query,
# a test run) cannot race the stack's start-up.  Prints a per-service table on every poll so a
# marker watching the terminal can see what is still coming up.
# ==============================================================================================
set -uo pipefail

TIMEOUT_S="${WAIT_TIMEOUT_S:-420}"
INTERVAL_S="${WAIT_INTERVAL_S:-5}"
COMPOSE="${COMPOSE_CMD:-docker compose}"

# Services that must reach "healthy". kafka-init and airflow-init are one-shot and excluded.
SERVICES=(kafka postgres gps-producer expense-producer stream-job api prometheus grafana airflow-webserver airflow-scheduler)

deadline=$(( $(date +%s) + TIMEOUT_S ))
printf '[wait] waiting up to %ss for: %s\n' "$TIMEOUT_S" "${SERVICES[*]}"

while :; do
  all_ok=1
  line=""
  for svc in "${SERVICES[@]}"; do
    cid="$($COMPOSE ps -q "$svc" 2>/dev/null | head -1)"
    if [[ -z "$cid" ]]; then
      state="absent"
    else
      state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo unknown)"
    fi
    line+="$(printf '%-20s %s\n' "$svc" "$state")"$'\n'
    [[ "$state" == "healthy" || "$state" == "running" ]] || all_ok=0
  done

  clear 2>/dev/null || true
  printf '[wait] %s\n%s' "$(date -u +%H:%M:%SZ)" "$line"

  if (( all_ok )); then
    echo "[wait] all services healthy"
    exit 0
  fi
  if (( $(date +%s) > deadline )); then
    echo "[wait] TIMEOUT after ${TIMEOUT_S}s — services not healthy:"
    printf '%s' "$line"
    echo "[wait] inspect with: $COMPOSE logs --tail=80 <service>"
    exit 1
  fi
  sleep "$INTERVAL_S"
done
