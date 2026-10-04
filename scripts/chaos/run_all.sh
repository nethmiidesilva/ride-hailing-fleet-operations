#!/usr/bin/env bash
# ==============================================================================================
# Failure-injection scenario suite.
#
# Each scenario follows the same shape and writes its own evidence file:
#   1. record the baseline
#   2. inject a failure
#   3. poll the real detection mechanism (Prometheus alert state, /health, SQL) until it reacts
#   4. record the detection latency
#   5. repair, and confirm recovery
#
# Evidence: docs/evidence/scenarios/<TC-ID>.json  — consumed by docs/TEST_REPORT.md.
# These scripts stop and start real containers, so they are NOT run by `make test-unit`.
# ==============================================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
cd "$ROOT"

export EVIDENCE_DIR="${EVIDENCE_DIR:-docs/evidence/scenarios}"
mkdir -p "$EVIDENCE_DIR"

SCENARIOS=(
  "tc_ft_001_producer_down.sh"
  "tc_ft_002_stream_restart.sh"
  "tc_ft_003_kafka_restart.sh"
  "tc_ft_004_high_reject_rate.sh"
  "tc_ft_005_missing_expense_file.sh"
  "tc_ft_006_corrupt_expense_file.sh"
  "replay_backfill.sh"
)

only="${1:-}"
failed=0
for scenario in "${SCENARIOS[@]}"; do
  if [[ -n "$only" && "$scenario" != *"$only"* ]]; then
    continue
  fi
  echo ""
  echo "=============================================================================="
  echo "  running $scenario"
  echo "=============================================================================="
  if bash "$HERE/$scenario"; then
    echo "  -> $scenario COMPLETED"
  else
    echo "  -> $scenario REPORTED A FAILURE (exit $?)"
    failed=$((failed + 1))
  fi
done

echo ""
echo "chaos suite finished; $failed scenario(s) reported a failure"
echo "evidence in $EVIDENCE_DIR"
exit $failed
