#!/usr/bin/env bash
# ==============================================================================================
# Scripted 5-10 minute demonstration.
#
# Prints, in order: what to show, what to say, and the command that produces it.  Pauses between
# steps so the presenter controls the pace.  Every number it prints comes from the live system.
#
# Run with:  bash scripts/demo.sh          (interactive, pauses for Enter)
#            bash scripts/demo.sh --auto   (no pauses, for a recorded run-through)
# ==============================================================================================
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

AUTO=0
[[ "${1:-}" == "--auto" ]] && AUTO=1

COMPOSE="docker compose"
API="http://localhost:${API_PORT:-8000}"
PROM="http://localhost:${PROMETHEUS_PORT:-9090}"
GRAFANA="http://localhost:${GRAFANA_PORT:-3000}"
AIRFLOW="http://localhost:${AIRFLOW_PORT:-8080}"

BOLD=$'\033[1m'; CYAN=$'\033[36m'; GREEN=$'\033[32m'; DIM=$'\033[2m'; RESET=$'\033[0m'

step() {
  echo ""
  echo "${CYAN}${BOLD}==============================================================================${RESET}"
  echo "${CYAN}${BOLD}  $1${RESET}"
  echo "${CYAN}${BOLD}==============================================================================${RESET}"
}
say()  { echo "${GREEN}  SAY:${RESET} $*"; }
show() { echo "${DIM}  \$ $*${RESET}"; }
pause() {
  if (( AUTO )); then sleep 2; else
    echo ""
    read -r -p "  [Enter] to continue " _ || true
  fi
}
jqp() { python3 -m json.tool 2>/dev/null || cat; }

# ----------------------------------------------------------------------------------------------
step "0. Orientation (30 s)"
say "This is a Lambda-architecture pipeline for ride-hailing fleet operations."
say "The business question has two halves with two different latency needs, and that is exactly"
say "why the architecture is Lambda and not Kappa:"
say "  (a) what is fleet utilisation and earnings by area RIGHT NOW  -> speed layer, seconds"
say "  (b) which vehicles are unprofitable once yesterday's costs land -> batch layer, daily"
say "One simulated day is compressed into ${SIM_DAY_SECONDS:-600} real seconds so a full daily"
say "cycle, including the Airflow reconciliation, happens while you watch."
show "docker compose ps"
$COMPOSE ps --format "table {{.Service}}\t{{.Status}}" 2>/dev/null | head -20
pause

# ----------------------------------------------------------------------------------------------
step "1. Ingestion — Kafka, keyed by vehicle_id across 6 partitions (1 min)"
say "The GPS producer emits one event per vehicle every 2 s, keyed by vehicle_id."
say "Keying by vehicle guarantees a vehicle's state transitions can never be reordered,"
say "while still spreading the fleet across 6 partitions for parallelism."
show "docker compose exec kafka kafka-topics.sh --describe --topic fleet.telemetry"
MSYS_NO_PATHCONV=1 $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic "${KAFKA_TOPIC:-fleet.telemetry}" 2>/dev/null | head -8
echo ""
say "Here are 10 live messages — note the partition number and the key on each line."
show "kafka-console-consumer.sh --property print.key=true --property print.partition=true"
MSYS_NO_PATHCONV=1 $COMPOSE exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "${KAFKA_TOPIC:-fleet.telemetry}" \
  --property print.key=true --property print.partition=true \
  --max-messages 10 --timeout-ms 30000 2>/dev/null | cut -c1-130
pause

# ----------------------------------------------------------------------------------------------
step "2. Speed layer — validation, dedup, windows, alerts (1.5 min)"
say "The Spark Structured Streaming job validates every event against the SHARED rule set,"
say "quarantines failures with a reason, de-duplicates on event_id inside a 2-minute watermark,"
say "enriches with the zone, and fans out into four independent sinks."
say "Here is one structured log line per micro-batch:"
show "docker compose logs stream-job | grep batch_complete | tail -3"
$COMPOSE logs stream-job --tail=300 2>/dev/null | grep '"event": "batch_complete"' | tail -3 | cut -c1-260
echo ""
say "Bad data is never dropped silently — every rejection is stored with its reason:"
show "SELECT reason, count(*) FROM rejected_events GROUP BY 1"
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-fleet}" -d "${POSTGRES_DB:-fleet}" \
  -c "SELECT reason, count(*) AS rows FROM rejected_events GROUP BY 1 ORDER BY 2 DESC;" 2>/dev/null
pause

# ----------------------------------------------------------------------------------------------
step "3. The live answer — API (1 min)"
say "GET /metrics/fleet answers the first half of the business question."
show "curl $API/metrics/fleet"
curl -s "$API/metrics/fleet" | jqp
echo ""
say "And per-zone utilisation and earnings over the last 15 minutes:"
show "curl '$API/metrics/zones?minutes=15'"
curl -s "$API/metrics/zones?minutes=15" | jqp | head -30
pause

# ----------------------------------------------------------------------------------------------
step "4. The threshold alert — idle vehicles (45 s)"
say "Three vehicles are deliberately 'lazy': they idle far longer than the rest."
say "When a vehicle exceeds IDLE_ALERT_MINUTES (${IDLE_ALERT_MINUTES:-3} min) an alert opens,"
say "and a partial unique index guarantees at most ONE open alert per vehicle no matter how"
say "many times a micro-batch is replayed."
show "curl '$API/alerts/idle?status=open'"
curl -s "$API/alerts/idle?status=open" | jqp | head -25
pause

# ----------------------------------------------------------------------------------------------
step "5. Batch layer — Airflow reconciliation (1.5 min)"
say "Open $AIRFLOW (admin/admin) and show the DAG graph."
say "The DAG waits for the day's expense CSV, validates it row by row, loads it, checks the"
say "Parquet partition exists, then spark-submits the profitability job, which RECOMPUTES"
say "revenue from raw events and joins it with the costs."
show "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 5"
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-fleet}" -d "${POSTGRES_DB:-fleet}" \
  -c "SELECT report_date, status, duration_s, left(run_id, 24) AS run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;" 2>/dev/null
echo ""
say "The master dataset it recomputes from — immutable Parquet, partitioned by simulated day:"
show "ls /data/lake/telemetry"
$COMPOSE exec -T stream-job sh -c 'ls /data/lake/telemetry' 2>/dev/null | head -8
pause

# ----------------------------------------------------------------------------------------------
step "6. The second answer — unprofitable vehicles (1 min)"
say "This is the half of the question the speed layer cannot answer, because costs only"
say "arrive once a day in a file."
show "curl $API/vehicles/unprofitable"
curl -s "$API/vehicles/unprofitable" | jqp | head -40
echo ""
say "And the consolidated HTML report the DAG rendered:"
show "curl $API/reports"
curl -s "$API/reports" | jqp
say "Open the newest one in a browser: $API/reports/profitability/<date>/html"
pause

# ----------------------------------------------------------------------------------------------
step "7. Observability — Prometheus and Grafana (1 min)"
say "Open $GRAFANA — two provisioned dashboards, no manual clicking:"
say "  'Fleet Operations' (business) and 'Pipeline Health' (SRE)."
say "Every scrape target should be UP:"
show "curl $PROM/api/v1/targets"
curl -s "$PROM/api/v1/targets" 2>/dev/null | python3 -c "
import json,sys
try:
    targets = json.load(sys.stdin)['data']['activeTargets']
    for t in targets:
        print(f\"  {t['labels']['job']:20s} {t['health']}\")
except Exception as exc:
    print('  could not read targets:', exc)
"
echo ""
say "And the alert rules that watch it:"
curl -s "$PROM/api/v1/rules" 2>/dev/null | python3 -c "
import json,sys
try:
    groups = json.load(sys.stdin)['data']['groups']
    for g in groups:
        for r in g['rules']:
            print(f\"  {r['name']:26s} {r.get('state','-')}\")
except Exception as exc:
    print('  could not read rules:', exc)
"
pause

# ----------------------------------------------------------------------------------------------
step "8. Failure injection — stop ingestion (1.5 min)"
say "Stopping the producer should be detected TWO independent ways within ~2.5 minutes:"
say "the NoTelemetryReceived Prometheus alert, and the API's own /health check."
show "docker compose stop gps-producer"
$COMPOSE stop gps-producer >/dev/null 2>&1
echo "  stopped at $(date -u +%H:%M:%SZ) — watch the Pipeline Health dashboard"
say "Wait ~2.5 minutes, then:"
show "curl -s -o /dev/null -w '%{http_code}' $API/health   # expect 503"
show "curl $PROM/api/v1/alerts                             # expect NoTelemetryReceived firing"
pause
say "Restarting ingestion — both signals should clear:"
show "docker compose start gps-producer"
$COMPOSE start gps-producer >/dev/null 2>&1
echo "  restarted at $(date -u +%H:%M:%SZ)"
pause

# ----------------------------------------------------------------------------------------------
step "9. Closing — the architecture argument (30 s)"
say "Two things to leave the examiner with:"
say "  1. Lambda was chosen because this use case genuinely has two latency needs and a"
say "     file-based daily source. Kappa would force the CSV into a topic for no benefit."
say "  2. Lambda's weakness is two code paths. This project mitigates it concretely:"
say "     common/zones.py and common/schemas.py are imported by BOTH layers, and the"
say "     reconciliation test measures and explains the residual difference rather than"
say "     hiding it."
echo ""
echo "  URLs:"
echo "    API docs   $API/docs"
echo "    Grafana    $GRAFANA  (admin/admin)"
echo "    Prometheus $PROM"
echo "    Airflow    $AIRFLOW  (admin/admin)"
echo "    Spark UI   http://localhost:4040"
echo ""
