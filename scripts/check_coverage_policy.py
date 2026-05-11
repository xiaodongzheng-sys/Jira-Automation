#!/usr/bin/env python3
"""Validate the release coverage policy from a coverage.py JSON report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_PATH = ROOT_DIR / "config" / "coverage_risk_policy.json"


def _as_percent(*, covered_lines: int, num_statements: int) -> float:
    if num_statements <= 0:
        return 100.0
    return round((covered_lines / num_statements) * 100.0, 2)


def _file_summary(report: dict[str, Any], path: str) -> dict[str, int]:
    files = report.get("files") or {}
    summary = (files.get(path) or {}).get("summary") or {}
    return {
        "covered_lines": int(summary.get("covered_lines") or 0),
        "num_statements": int(summary.get("num_statements") or 0),
    }


def _path_summary(report: dict[str, Any], prefixes: list[str]) -> dict[str, int]:
    covered = 0
    statements = 0
    files = report.get("files") or {}
    for path, payload in files.items():
        if not any(str(path).startswith(prefix) for prefix in prefixes):
            continue
        summary = (payload or {}).get("summary") or {}
        covered += int(summary.get("covered_lines") or 0)
        statements += int(summary.get("num_statements") or 0)
    return {"covered_lines": covered, "num_statements": statements}


def evaluate_coverage_policy(
    coverage_report: dict[str, Any],
    policy: dict[str, Any],
    *,
    governed_fail_under: float | None = None,
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []

    governed = policy.get("governed") or {}
    governed_threshold = float(governed_fail_under if governed_fail_under is not None else governed.get("min_percent", 100.0))
    for path in governed.get("files") or []:
        summary = _file_summary(coverage_report, str(path))
        percent = _as_percent(**summary)
        check = {
            "kind": "governed",
            "path": str(path),
            "min_percent": governed_threshold,
            "percent": percent,
            **summary,
        }
        checks.append(check)
        if percent < governed_threshold:
            failures.append(check)

    for module in policy.get("critical_modules") or []:
        path = str(module.get("path") or "")
        threshold = float(module.get("min_percent") or 0.0)
        summary = _file_summary(coverage_report, path)
        percent = _as_percent(**summary)
        check = {
            "kind": "critical_module",
            "path": path,
            "min_percent": threshold,
            "percent": percent,
            **summary,
        }
        checks.append(check)
        if percent < threshold:
            failures.append(check)

    overall = policy.get("overall") or {}
    prefixes = [str(prefix) for prefix in overall.get("paths") or []]
    if prefixes:
        summary = _path_summary(coverage_report, prefixes)
        threshold = float(overall.get("min_percent") or 0.0)
        percent = _as_percent(**summary)
        check = {
            "kind": "overall",
            "label": str(overall.get("label") or ", ".join(prefixes)),
            "paths": prefixes,
            "min_percent": threshold,
            "percent": percent,
            **summary,
        }
        checks.append(check)
        if percent < threshold:
            failures.append(check)

    return {
        "status": "pass" if not failures else "fail",
        "checks": checks,
        "failures": failures,
    }


def _format_failure(failure: dict[str, Any]) -> str:
    name = failure.get("path") or failure.get("label") or failure.get("kind")
    return f"{name}: {failure['percent']:.2f}% < {failure['min_percent']:.2f}%"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-json", required=True, help="Path to a coverage.py JSON report.")
    parser.add_argument("--policy", default=str(DEFAULT_POLICY_PATH), help="Coverage risk policy JSON path.")
    parser.add_argument("--governed-fail-under", type=float, default=None, help="Override governed module threshold.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args(argv)

    coverage_report = json.loads(Path(args.coverage_json).read_text(encoding="utf-8"))
    policy = json.loads(Path(args.policy).read_text(encoding="utf-8"))
    result = evaluate_coverage_policy(
        coverage_report,
        policy,
        governed_fail_under=args.governed_fail_under,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Risk coverage gate: {result['status']}")
        for failure in result["failures"]:
            print(f"- {_format_failure(failure)}")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
