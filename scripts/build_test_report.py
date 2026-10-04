"""Assemble the machine-readable half of ``docs/TEST_REPORT.md`` from real artefacts.

The narrative sections of the test report are written by hand, but the *status* of every test
case must come from a real run.  This script parses:

  * ``docs/evidence/tests/junit-*.xml``    — pytest results (pass/fail/skip + duration)
  * ``docs/evidence/scenarios/*.json``     — chaos and NFR scenario outcomes
  * ``docs/evidence/tests/coverage.xml``   — coverage per module

and writes:

  * ``docs/evidence/tests/summary.json``   — one machine-readable object with everything
  * ``docs/evidence/tests/results_table.md``  — the markdown table pasted into TEST_REPORT.md
  * ``docs/evidence/tests/coverage_table.md``

Anything it cannot find is reported as ``NOT EXECUTED`` with the reason, never invented.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

EVIDENCE = Path("docs/evidence")
TESTS_DIR = EVIDENCE / "tests"
SCENARIOS_DIR = EVIDENCE / "scenarios"

#: Recover the TC-ID from a pytest node name like ``test_tc_ing_021_each_bad_shape...``.
TC_PATTERN = re.compile(r"tc[_-]([a-z0-9]+)[_-](\d+)", re.IGNORECASE)


def tc_id_from_name(name: str) -> str | None:
    """Map a pytest test function name back to its TC-<AREA>-<NNN> identifier."""
    match = TC_PATTERN.search(name)
    if not match:
        return None
    area, number = match.group(1).upper(), match.group(2)
    return f"TC-{area}-{number}"


def parse_junit(path: Path) -> list[dict[str, Any]]:
    """Read one JUnit XML file into a list of result dicts."""
    if not path.exists():
        return []
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        print(f"  WARN could not parse {path}: {exc}")
        return []

    results: list[dict[str, Any]] = []
    for case in root.iter("testcase"):
        status = "PASS"
        detail = ""
        for child in case:
            if child.tag == "failure" or child.tag == "error":
                status = "FAIL"
                detail = (child.get("message") or child.text or "")[:400]
            elif child.tag == "skipped":
                status = "SKIPPED"
                detail = (child.get("message") or "")[:400]
        name = case.get("name", "")
        results.append(
            {
                "suite": path.stem.replace("junit-", ""),
                "classname": case.get("classname", ""),
                "name": name,
                "node_id": f"{case.get('classname', '').replace('.', '/')}.py::{name}",
                "tc_id": tc_id_from_name(name),
                "status": status,
                "duration_s": round(float(case.get("time", 0) or 0), 3),
                "detail": detail.strip().replace("\n", " ")[:300],
            }
        )
    return results


def parse_scenarios() -> list[dict[str, Any]]:
    """Read the chaos / NFR scenario evidence files."""
    if not SCENARIOS_DIR.exists():
        return []
    out = []
    for path in sorted(SCENARIOS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            out.append(
                {
                    "tc_id": path.stem,
                    "status": "NOT EXECUTED",
                    "detail": f"evidence file is not valid JSON: {exc}",
                    "evidence": str(path),
                }
            )
            continue
        out.append(
            {
                "tc_id": data.get("test_case", path.stem),
                "title": data.get("title", ""),
                "status": data.get("status", "NOT EXECUTED"),
                "requirements": data.get("requirements", []),
                "evidence": str(path),
                "captured_at": data.get("captured_at"),
                "detail": data.get("reason") or data.get("explanation", ""),
            }
        )
    return out


def parse_coverage(path: Path) -> dict[str, Any]:
    """Read coverage.xml into per-package line-rate figures."""
    if not path.exists():
        return {"available": False, "reason": f"{path} not found"}
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        return {"available": False, "reason": str(exc)}

    packages = []
    for package in root.iter("package"):
        packages.append(
            {
                "name": package.get("name", ""),
                "line_rate": round(float(package.get("line-rate", 0)) * 100, 1),
                "branch_rate": round(float(package.get("branch-rate", 0)) * 100, 1),
            }
        )
    return {
        "available": True,
        "overall_line_rate": round(float(root.get("line-rate", 0)) * 100, 1),
        "overall_branch_rate": round(float(root.get("branch-rate", 0)) * 100, 1),
        "packages": sorted(packages, key=lambda p: p["name"]),
    }


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "_No results found._\n"
    header = "| " + " | ".join(columns) + " |"
    sep = "|" + "|".join("---" for _ in columns) + "|"
    body = "\n".join(
        "| " + " | ".join(str(r.get(c, "")).replace("|", "\\|") for c in columns) + " |"
        for r in rows
    )
    return f"{header}\n{sep}\n{body}\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assemble test results from real artefacts.")
    parser.add_argument("--evidence-dir", default=None)
    args = parser.parse_args(argv)
    global EVIDENCE, TESTS_DIR, SCENARIOS_DIR
    if args.evidence_dir:
        EVIDENCE = Path(args.evidence_dir)
        TESTS_DIR = EVIDENCE / "tests"
        SCENARIOS_DIR = EVIDENCE / "scenarios"

    print(f"\nassembling test results from {EVIDENCE.resolve()}")

    pytest_results: list[dict[str, Any]] = []
    junit_files = sorted(TESTS_DIR.glob("junit-*.xml"))
    for path in junit_files:
        parsed = parse_junit(path)
        pytest_results.extend(parsed)
        print(f"  {path.name}: {len(parsed)} cases")
    if not junit_files:
        print("  WARN no junit-*.xml found — run the test suites first")

    scenarios = parse_scenarios()
    print(f"  scenarios: {len(scenarios)} evidence files")

    coverage = parse_coverage(TESTS_DIR / "coverage.xml")
    if coverage["available"]:
        print(f"  coverage: {coverage['overall_line_rate']}% lines overall")
    else:
        print(f"  coverage: unavailable ({coverage['reason']})")

    totals = {
        "pytest_total": len(pytest_results),
        "pytest_passed": sum(1 for r in pytest_results if r["status"] == "PASS"),
        "pytest_failed": sum(1 for r in pytest_results if r["status"] == "FAIL"),
        "pytest_skipped": sum(1 for r in pytest_results if r["status"] == "SKIPPED"),
        "scenarios_total": len(scenarios),
        "scenarios_passed": sum(1 for s in scenarios if s["status"] == "PASS"),
        "scenarios_failed": sum(1 for s in scenarios if s["status"] == "FAIL"),
        "scenarios_not_executed": sum(
            1 for s in scenarios if s["status"] not in ("PASS", "FAIL")
        ),
        "total_duration_s": round(sum(r["duration_s"] for r in pytest_results), 2),
    }

    summary = {
        "generated_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "totals": totals,
        "coverage": coverage,
        "pytest_results": sorted(
            pytest_results, key=lambda r: (r["tc_id"] or "ZZZ", r["name"])
        ),
        "scenarios": scenarios,
        "junit_files": [str(p) for p in junit_files],
    }

    TESTS_DIR.mkdir(parents=True, exist_ok=True)
    (TESTS_DIR / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )

    table_rows = [
        {
            "TC ID": r["tc_id"] or "—",
            "Test": r["name"],
            "Suite": r["suite"],
            "Status": r["status"],
            "Duration (s)": r["duration_s"],
            "Notes": r["detail"][:120] if r["detail"] else "",
        }
        for r in summary["pytest_results"]
    ]
    (TESTS_DIR / "results_table.md").write_text(
        markdown_table(table_rows, ["TC ID", "Test", "Suite", "Status", "Duration (s)", "Notes"]),
        encoding="utf-8",
    )

    scenario_rows = [
        {
            "TC ID": s["tc_id"],
            "Title": s.get("title", "")[:80],
            "Status": s["status"],
            "Evidence": s["evidence"],
        }
        for s in scenarios
    ]
    (TESTS_DIR / "scenarios_table.md").write_text(
        markdown_table(scenario_rows, ["TC ID", "Title", "Status", "Evidence"]), encoding="utf-8"
    )

    if coverage["available"]:
        cov_rows = [
            {"Module": p["name"] or "(root)", "Line %": p["line_rate"], "Branch %": p["branch_rate"]}
            for p in coverage["packages"]
        ]
        cov_rows.append(
            {
                "Module": "**overall**",
                "Line %": coverage["overall_line_rate"],
                "Branch %": coverage["overall_branch_rate"],
            }
        )
        (TESTS_DIR / "coverage_table.md").write_text(
            markdown_table(cov_rows, ["Module", "Line %", "Branch %"]), encoding="utf-8"
        )

    print("\nsummary:")
    for key, value in totals.items():
        print(f"  {key:26s} {value}")
    print(f"\nwrote {TESTS_DIR / 'summary.json'}, results_table.md, scenarios_table.md\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
