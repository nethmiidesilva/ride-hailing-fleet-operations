"""Fill the per-test-case Status / Actual-result placeholders in docs/TEST_REPORT.md.

The detailed test-case tables in section 4 are written by hand (objective, preconditions, inputs,
expected result — the parts a human must author), but their **Status** and **Actual result** cells
are left as ``<<>>`` placeholders so that no result is ever typed in by hand.  This script fills
them from ``docs/evidence/tests/summary.json``.

Two table shapes are handled:

1. **Detailed tables** — a ``#### TC-XXX-NNN — title`` heading followed by a two-column table with
   ``| Status | <<>> |`` and optionally ``| Actual result | <<>> |`` rows.  The TC id comes from
   the nearest preceding heading.
2. **Compact tables** — one row per test, ``| TC-XXX-NNN | objective | expected | <<>> |``, where
   the TC id is the first cell of the row itself.

Anything with no matching executed test becomes ``NOT EXECUTED``, which is what ground rule 2 of
the brief requires.

Usage (after build_test_report.py):

    python scripts/build_test_report.py && python scripts/fill_test_case_statuses.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

SUMMARY = Path("docs/evidence/tests/summary.json")
SCENARIOS = Path("docs/evidence/scenarios")
REPORT = Path("docs/TEST_REPORT.md")

TC_HEADING = re.compile(r"^#{3,4}\s+(TC-[A-Z]+-\d+)")
TC_IN_ROW = re.compile(r"^\|\s*(TC-[A-Z]+-\d+)\s*\|")
#: Some headings cover a range, e.g. "TC-ING-006 / TC-ING-007 / TC-ING-008".
TC_ANY = re.compile(r"TC-[A-Z]+-\d+")

BADGE = {"PASS": "**PASS**", "FAIL": "**FAIL**", "SKIP": "SKIPPED"}

#: A status cell is either an unfilled placeholder or a badge this script wrote earlier.
#: Matching both makes the script idempotent, so re-running it after a fresh test run
#: refreshes the document instead of silently doing nothing.
STATUS_CELL = re.compile(r"`<<>>`|\*\*PASS\*\*|\*\*FAIL\*\*|SKIPPED|\*\*NOT EXECUTED\*\*")
ACTUAL_CELL = re.compile(r"`<<[^>]*>>`|(?<=\| ).*(?= \|$)")


def load() -> tuple[dict[str, str], dict[str, float], dict[str, str]]:
    """Return (status by TC id, duration by TC id, detail by TC id)."""
    if not SUMMARY.exists():
        print(f"ERROR: {SUMMARY} missing — run scripts/build_test_report.py first")
        sys.exit(1)
    raw = json.loads(SUMMARY.read_text(encoding="utf-8"))
    status: dict[str, str] = {}
    duration: dict[str, float] = {}
    detail: dict[str, str] = {}
    for row in raw.get("pytest_results", []):
        tc = row.get("tc_id")
        if not tc:
            continue
        st = row.get("status", "")
        # A TC id can map to several parametrised cases; one failure fails the case.
        if status.get(tc) == "FAIL":
            continue
        if st == "FAIL" or tc not in status or status[tc] == "SKIP":
            status[tc] = st
        duration[tc] = duration.get(tc, 0.0) + float(row.get("duration_s") or 0.0)
        if row.get("detail"):
            detail[tc] = str(row["detail"])[:200]
    for js in sorted(SCENARIOS.glob("*.json")):
        data = json.loads(js.read_text(encoding="utf-8"))
        tc = data.get("test_case") or js.stem
        status[tc] = data.get("status", "UNKNOWN")
    return status, duration, detail


def actual_for(tc: str, status: dict[str, str], duration: dict[str, float],
               detail: dict[str, str]) -> str:
    """A short, honest 'actual result' cell derived from the run."""
    st = status.get(tc)
    if st is None:
        return "**NOT EXECUTED** — no test with this id ran"
    if st == "FAIL":
        return f"**FAILED**: {detail.get(tc, 'see JUnit XML')}"
    if st == "SKIP":
        return f"SKIPPED — {detail.get(tc, 'see §9 known gaps')}"
    if tc.startswith(("TC-FT-", "TC-NFR-", "TC-E2E-")):
        return f"As expected; see `docs/evidence/scenarios/{tc}.json`"
    secs = duration.get(tc, 0.0)
    return f"As expected ({secs:.2f} s)"


def main() -> int:
    status, duration, detail = load()
    lines = REPORT.read_text(encoding="utf-8").splitlines()

    current: str | None = None
    filled_status = filled_actual = filled_rows = 0
    not_executed: set[str] = set()

    for i, line in enumerate(lines):
        heading = TC_HEADING.match(line)
        if heading:
            # For a combined heading, the detailed rows belong to the first id.
            current = heading.group(1)
            continue

        # Shape 2: compact one-row-per-test tables.
        row = TC_IN_ROW.match(line)
        if row and STATUS_CELL.search(line):
            tc = row.group(1)
            st = status.get(tc)
            badge = BADGE.get(st, "**NOT EXECUTED**") if st else "**NOT EXECUTED**"
            if not st:
                not_executed.add(tc)
            lines[i] = STATUS_CELL.sub(badge, line, count=1)
            filled_rows += 1
            continue

        if current is None:
            continue

        # Shape 1: detailed two-column tables.
        if line.startswith("| Status |") and STATUS_CELL.search(line):
            st = status.get(current)
            badge = BADGE.get(st, "**NOT EXECUTED**") if st else "**NOT EXECUTED**"
            if not st:
                not_executed.add(current)
            lines[i] = STATUS_CELL.sub(badge, line, count=1)
            filled_status += 1
        elif line.startswith("| Actual result |"):
            cell = actual_for(current, status, duration, detail)
            # Rewrite the whole value cell so a previously generated line is replaced too.
            lines[i] = f"| Actual result | {cell} |"
            filled_actual += 1

    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"filled {filled_status} Status cells, {filled_actual} Actual-result cells, "
          f"{filled_rows} compact rows")
    if not_executed:
        print("NOT EXECUTED (no matching test id ran):")
        for tc in sorted(not_executed):
            print("  ", tc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
