#!/usr/bin/env bash
# ==============================================================================================
# TC-NFR-003 - container CPU / memory snapshot (NFR-05: the whole stack must fit a laptop).
#
# Why this is a shell script on the host rather than a pytest case: it needs the Docker CLI and
# the Docker socket, and the tests container deliberately has neither (giving a test container
# the daemon socket is a privilege escalation, and the brief's resource envelope is about the
# host anyway). The pytest version therefore skips, and this captures the same evidence.
#
# Method: sample `docker stats` several times so a transient spike cannot be mistaken for the
# steady state, then record the per-container peak and the stack total against each declared
# `mem_limit` from docker-compose.yml.
#
# Evidence: docs/evidence/scenarios/TC-NFR-003.json
# ==============================================================================================
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/chaos/_lib.sh"

TC="TC-NFR-003"
SAMPLES="${NFR_RESOURCE_SAMPLES:-5}"
INTERVAL="${NFR_RESOURCE_INTERVAL:-6}"

log "$TC: sampling container CPU and memory ${SAMPLES}x every ${INTERVAL}s"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

for i in $(seq 1 "$SAMPLES"); do
  docker stats --no-stream \
    --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' 2>/dev/null >> "$tmp"
  log "  sample $i/$SAMPLES captured"
  (( i < SAMPLES )) && sleep "$INTERVAL"
done

# Declared limits, so the report can state headroom rather than just usage.
limits_json="$(
  grep -oE '^\s+mem_limit: [0-9]+m' "$(dirname "${BASH_SOURCE[0]}")/../docker-compose.yml" \
    | grep -oE '[0-9]+' | paste -sd, - | awk '{print "["$0"]"}'
)"

python3 - "$tmp" "$limits_json" <<'PY' > "$EVIDENCE_DIR/$TC.json"
import json, re, sys, datetime, collections

path, limits_json = sys.argv[1], sys.argv[2]

def to_mib(text):
    """'1.234GiB' / '512MiB' / '123.4kB' -> MiB float."""
    m = re.match(r"([0-9.]+)\s*([A-Za-z]+)", text.strip())
    if not m:
        return None
    value, unit = float(m.group(1)), m.group(2).lower()
    factor = {"b": 1 / 1048576, "kb": 1 / 1024, "kib": 1 / 1024,
              "mb": 1, "mib": 1, "gb": 1024, "gib": 1024}.get(unit)
    return value * factor if factor else None

peak = collections.defaultdict(lambda: {"cpu_pct": 0.0, "mem_mib": 0.0,
                                        "mem_limit_mib": None, "mem_pct": 0.0})
samples = collections.Counter()
with open(path, encoding="utf-8", errors="replace") as fh:
    for line in fh:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 4:
            continue
        name, cpu, mem, mem_pct = parts[0], parts[1], parts[2], parts[3]
        if not name.startswith("fleet-"):
            continue
        samples[name] += 1
        rec = peak[name]
        try:
            rec["cpu_pct"] = max(rec["cpu_pct"], float(cpu.rstrip("%")))
        except ValueError:
            pass
        # docker stats renders MemUsage as "123MiB / 456MiB"; the second half is the limit.
        halves = [h.strip() for h in mem.split("/")]
        used = halves[0]
        limit = halves[1] if len(halves) > 1 else ""
        u = to_mib(used)
        if u is not None:
            rec["mem_mib"] = max(rec["mem_mib"], u)
        l = to_mib(limit) if limit else None
        if l is not None:
            rec["mem_limit_mib"] = l
        try:
            rec["mem_pct"] = max(rec["mem_pct"], float(mem_pct.rstrip("%")))
        except ValueError:
            pass

containers = {
    name: {
        "peak_cpu_pct": round(v["cpu_pct"], 2),
        "peak_mem_mib": round(v["mem_mib"], 1),
        "mem_limit_mib": round(v["mem_limit_mib"], 1) if v["mem_limit_mib"] else None,
        "peak_mem_pct_of_limit": round(v["mem_pct"], 1),
        "samples": samples[name],
    }
    for name, v in sorted(peak.items())
}
total_used = round(sum(c["peak_mem_mib"] for c in containers.values()), 1)
total_limit = round(sum(c["mem_limit_mib"] or 0 for c in containers.values()), 1)
hottest = max(containers.items(), key=lambda kv: kv[1]["peak_mem_pct_of_limit"], default=(None, {}))

out = {
    "test_case": "TC-NFR-003",
    "title": "Container CPU and memory against the declared limits (NFR-05)",
    "requirements": ["NFR-05"],
    "status": "PASS" if containers and total_used < total_limit else "FAIL",
    "method": (
        "docker stats --no-stream sampled several times on the HOST; per-container peak reported. "
        "Run from the host because the tests container is deliberately given neither the Docker "
        "CLI nor the daemon socket."
    ),
    "containers": containers,
    "totals": {
        "containers_observed": len(containers),
        "sum_peak_mem_mib": total_used,
        "sum_declared_limit_mib": total_limit,
        "headroom_mib": round(total_limit - total_used, 1),
        "utilisation_of_declared_limits_pct": round(100 * total_used / total_limit, 1) if total_limit else None,
        "declared_limits_from_compose_mib": json.loads(limits_json),
    },
    "tightest_container": {"name": hottest[0], **hottest[1]} if hottest[0] else None,
    "captured_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
}
json.dump(out, sys.stdout, indent=2)
PY

log "$TC evidence written to $EVIDENCE_DIR/$TC.json"
python3 -c "
import json
d = json.load(open('$EVIDENCE_DIR/$TC.json'))
t = d['totals']
print(f\"  status {d['status']}: {t['containers_observed']} containers, peak total \"
      f\"{t['sum_peak_mem_mib']} MiB of {t['sum_declared_limit_mib']} MiB declared \"
      f\"({t['utilisation_of_declared_limits_pct']}%)\")
h = d['tightest_container']
print(f\"  tightest: {h['name']} at {h['peak_mem_pct_of_limit']}% of its limit\")
"
