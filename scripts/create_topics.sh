#!/usr/bin/env bash
# ==============================================================================================
# Create the telemetry topic and its dead-letter queue.
#
# Auto-topic-creation is disabled on the broker on purpose: partition count is an architectural
# decision (6 partitions, keyed by vehicle_id -> per-vehicle ordering and a natural unit of
# parallelism for Spark) and must not be left to a default that could silently change.
#
# The script is idempotent: `--if-not-exists` means a `docker compose up` on an existing volume
# is a no-op, and it prints the resulting layout so the P2 verification gate has evidence.
# ==============================================================================================
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-fleet.telemetry}"
DLQ="${KAFKA_DLQ_TOPIC:-fleet.telemetry.dlq}"
PARTITIONS="${KAFKA_PARTITIONS:-6}"
REPLICATION="${KAFKA_REPLICATION:-1}"
KBIN="/opt/kafka/bin"

echo "[kafka-init] waiting for broker at ${BOOTSTRAP} ..."
for attempt in $(seq 1 30); do
  if "${KBIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    echo "[kafka-init] broker is up (attempt ${attempt})"
    break
  fi
  sleep 3
done

create_topic() {
  local name="$1" parts="$2" retention_ms="$3"
  echo "[kafka-init] creating topic ${name} (partitions=${parts}, rf=${REPLICATION})"
  "${KBIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${parts}" \
    --replication-factor "${REPLICATION}" \
    --config retention.ms="${retention_ms}" \
    --config cleanup.policy=delete
}

# 24 h retention on telemetry: long enough to replay a full simulated run into the stream job,
# short enough that a laptop disk survives an all-day demo.
create_topic "${TOPIC}" "${PARTITIONS}" 86400000
# The DLQ keeps data for 3 days — bad events are the evidence for the data-quality report.
create_topic "${DLQ}" 1 259200000

echo "[kafka-init] topic layout:"
"${KBIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${TOPIC}"
"${KBIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${DLQ}"
echo "[kafka-init] done"
