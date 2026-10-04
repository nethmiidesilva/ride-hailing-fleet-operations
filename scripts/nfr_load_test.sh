#!/usr/bin/env bash
# ==============================================================================================
# TC-NFR-004 - throughput stress test (NFR-06: at least 8x headroom over the baseline rate).
#
# The brief asks for "increase NUM_VEHICLES to 200 for 3 minutes; record events/s, batch duration
# and lag".  That needs the producer container recreated with a different environment, which a
# pytest process cannot do safely, so it lives here rather than in tests/.
#
# Method:
#   1. record a baseline at the configured fleet size
#   2. recreate gps-producer with NUM_VEHICLES=200 (8x the default)
#   3. sample throughput, micro-batch duration and TRUE streaming lag every 15 s for the hold
#   4. restore the configured fleet size and confirm the backlog drains
#
# Evidence: docs/evidence/scenarios/TC-NFR-004.json
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/chaos/_lib.sh"

TC="TC-NFR-004"
LOAD_VEHICLES="${NFR_VEHICLES:-200}"
HOLD_S="${NFR_HOLD_SECONDS:-180}"
SAMPLE_EVERY="${NFR_SAMPLE_SECONDS:-15}"
DRAIN_TARGET="${NFR_DRAIN_TARGET:-1000}"

# True streaming lag = the broker's end offset minus the offset the job has committed to its
# checkpoint.  Structured Streaming does NOT register a Kafka consumer group (it keeps offsets in
# the checkpoint, which is what gives it its delivery guarantees), so `kafka_consumergroup_lag` is
# permanently empty for this job -- see DEFECT-013 in docs/TEST_REPORT.md.  kafka-exporter
# supplies the first term; the stream job publishes the second as `stream_committed_offset`.
LAG_EXPR='sum(kafka_topic_partition_current_offset{topic="fleet.telemetry"}) - sum(max by (partition) (stream_committed_offset{topic="fleet.telemetry"}))'

log "$TC: scaling the fleet to $LOAD_VEHICLES vehicles for ${HOLD_S}s"

# Reads the current lag as an integer, or an empty string when the series is unavailable.
current_lag() {
  local raw
  raw="$(prom_value "$LAG_EXPR" 2>/dev/null)"
  [[ -z "$raw" || "$raw" == "null" ]] && { echo ""; return; }
  printf '%.0f' "$raw" 2>/dev/null || echo ""
}

sample() {
  local rate lag p95 rejected
  rate="$(prom_value 'sum(rate(producer_events_sent_total[1m]))')"
  lag="$(current_lag)"
  p95="$(prom_value 'histogram_quantile(0.95, sum(rate(stream_batch_duration_seconds_bucket[2m])) by (le))')"
  rejected="$(prom_value 'sum(rate(stream_rows_rejected_total[1m]))')"
  printf '{"t":"%s","events_per_sec":"%s","streaming_lag":"%s","batch_p95_s":"%s","rejected_per_sec":"%s"}' \
    "$(now_iso)" "${rate:-null}" "${lag:-null}" "${p95:-null}" "${rejected:-null}"
}

baseline="$(sample)"
log "baseline: $baseline"

t_start="$(now_iso)"
NUM_VEHICLES="$LOAD_VEHICLES" $COMPOSE up -d --force-recreate --no-deps gps-producer >/dev/null 2>&1
log "scaled up at $t_start; holding for ${HOLD_S}s"

samples="["
first=1
elapsed=0
peak_lag_seen=0
while (( elapsed < HOLD_S )); do
  sleep "$SAMPLE_EVERY"
  elapsed=$((elapsed + SAMPLE_EVERY))
  s="$(sample)"
  if (( first )); then samples+="$s"; first=0; else samples+=",$s"; fi
  # Track the peak across samples rather than reading it once at the end, when it has drained.
  l="$(current_lag)"
  if [[ -n "$l" ]] && (( l > peak_lag_seen )); then peak_lag_seen="$l"; fi
  log "  t+${elapsed}s $s"
done
samples+="]"

peak_p95="$(prom_value 'histogram_quantile(0.95, sum(rate(stream_batch_duration_seconds_bucket[2m])) by (le))')"
rows_total="$(sql_scalar 'SELECT count(*) FROM vehicle_status;')"

log "restoring the configured fleet size"
t_restore="$(now_iso)"
$COMPOSE up -d --force-recreate --no-deps gps-producer >/dev/null 2>&1

log "waiting for the streaming backlog to drain below ${DRAIN_TARGET} ..."
drain_seconds=0
drain_elapsed=0
while (( drain_elapsed < 420 )); do
  l="$(current_lag)"
  if [[ -n "$l" ]] && (( l < DRAIN_TARGET )); then
    drain_seconds="$drain_elapsed"
    break
  fi
  sleep 15
  drain_elapsed=$((drain_elapsed + 15))
  drain_seconds="$drain_elapsed"
done
(( drain_elapsed >= 420 )) && drain_seconds=-1

# Clean up after ourselves.  The load fleet's vehicle_status rows are keyed by vehicle_id and
# are never pruned, so without this the table keeps 175 phantom vehicles and TC-INT-004 would
# report a 200-vehicle fleet forever (DEFECT-018).  v_fleet_now filters on recency so the live
# view is already correct, but leaving the rows behind would still be sloppy: a test must not
# leave the system in a state that fails another test.
log "pruning vehicle_status rows for vehicles outside the configured fleet"
pruned="$(sql_scalar "WITH gone AS (DELETE FROM vehicle_status WHERE vehicle_id > 'V-$(printf '%03d' "${NUM_VEHICLES:-25}")' RETURNING 1) SELECT count(*) FROM gone;")"
log "  pruned ${pruned:-0} rows"

recovered="$(sample)"

status="PASS"
[[ "$drain_seconds" == "-1" ]] && status="FAIL"

write_evidence "$TC" "$(cat <<JSON
{
  "test_case": "$TC",
  "title": "Throughput at ${LOAD_VEHICLES} vehicles held for ${HOLD_S}s (NFR-06)",
  "requirements": ["REQ-04", "NFR-06"],
  "status": "$status",
  "configuration": {
    "baseline_vehicles": "${NUM_VEHICLES:-25}",
    "load_vehicles": $LOAD_VEHICLES,
    "emit_interval_sec": "${EMIT_INTERVAL_SEC:-2}",
    "theoretical_events_per_sec_at_load": $(python3 -c "print(round($LOAD_VEHICLES / ${EMIT_INTERVAL_SEC:-2}, 2))"),
    "hold_seconds": $HOLD_S,
    "sample_interval_seconds": $SAMPLE_EVERY,
    "drain_target_messages": $DRAIN_TARGET
  },
  "lag_definition": "broker end offset minus the offset the streaming job committed to its checkpoint (see DEFECT-013)",
  "baseline": $baseline,
  "samples_under_load": $samples,
  "peak": {
    "streaming_lag_messages": $peak_lag_seen,
    "batch_p95_seconds": "$peak_p95",
    "vehicle_status_rows": "$rows_total"
  },
  "recovery": {
    "restored_at": "$t_restore",
    "phantom_vehicle_rows_pruned": ${pruned:-0},
    "backlog_drained_below_${DRAIN_TARGET}_after_seconds": $drain_seconds,
    "after_restore": $recovered
  },
  "captured_at": "$(now_iso)"
}
JSON
)"

log "$TC finished with status $status (peak lag ${peak_lag_seen} msgs, drained in ${drain_seconds}s)"
[[ "$status" == "PASS" ]]
