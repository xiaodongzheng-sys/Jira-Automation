#!/usr/bin/env python3
"""Run the read-only system release gate before publishing the portal."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any
from urllib.request import Request, urlopen


ROOT_DIR = Path(__file__).resolve().parents[1]
STATIC_JS_PATHS = sorted((ROOT_DIR / "static").glob("*.js"))


@dataclass
class GateStep:
    name: str
    command: list[str] = field(default_factory=list)
    returncode: int = 0
    stdout: str = ""
    stderr: str = ""
    status: str = "pass"
    details: dict[str, Any] = field(default_factory=dict)


def _run_command(name: str, command: list[str]) -> GateStep:
    env = dict(os.environ)
    env.setdefault("ENV_FILE", os.devnull)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT_DIR) if not existing_pythonpath else f"{ROOT_DIR}{os.pathsep}{existing_pythonpath}"
    completed = subprocess.run(command, cwd=ROOT_DIR, env=env, capture_output=True, text=True, check=False)
    return GateStep(
        name=name,
        command=command,
        returncode=int(completed.returncode),
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
        status="pass" if completed.returncode == 0 else "fail",
    )


def _run_parallel_commands(commands: list[tuple[str, list[str]]], *, max_workers: int) -> list[GateStep]:
    if not commands:
        return []
    workers = max(1, min(max_workers, len(commands)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_run_command, name, command) for name, command in commands]
        return [future.result() for future in futures]


def _join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _fetch_json(url: str) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": "team-portal-release-gate/1.0"}, method="GET")
    with urlopen(request, timeout=10) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError(f"{url} did not return a JSON object")
    return payload


def _smoke_check(*, uat_url: str, live_url: str, expected_revision: str, expect_live_promoted: bool = False) -> GateStep:
    checks: list[dict[str, Any]] = []
    try:
        uat_health_url = _join_url(uat_url, "/healthz/")
        uat_agent_url = _join_url(uat_url, "/api/local-agent/healthz")
        live_health_url = _join_url(live_url, "/healthz")
        live_agent_url = _join_url(live_url, "/api/local-agent/healthz")

        uat_health = _fetch_json(uat_health_url)
        checks.append({"name": "uat_healthz", "method": "GET", "url": uat_health_url, "revision": uat_health.get("revision")})
        uat_agent = _fetch_json(uat_agent_url)
        checks.append({"name": "uat_local_agent_healthz", "method": "GET", "url": uat_agent_url, "status": uat_agent.get("status")})
        live_health = _fetch_json(live_health_url)
        checks.append({"name": "live_healthz", "method": "GET", "url": live_health_url, "revision": live_health.get("revision")})
        live_agent = _fetch_json(live_agent_url)
        checks.append({"name": "live_local_agent_healthz", "method": "GET", "url": live_agent_url, "status": live_agent.get("status")})

        if uat_health.get("revision") != expected_revision:
            raise RuntimeError(f"UAT revision mismatch: {uat_health.get('revision')} != {expected_revision}")
        if expect_live_promoted:
            if live_health.get("revision") != expected_revision:
                raise RuntimeError(f"Live revision mismatch: {live_health.get('revision')} != {expected_revision}")
        elif live_health.get("revision") == expected_revision:
            raise RuntimeError("Live already serves the UAT revision; verify whether promotion was intended.")
    except Exception as error:  # noqa: BLE001 - gate must report the exact failed smoke boundary.
        return GateStep(name="uat_live_read_only_smoke", status="fail", returncode=1, stderr=str(error), details={"checks": checks})
    return GateStep(name="uat_live_read_only_smoke", details={"checks": checks})


def run_gate(
    *,
    skip_smoke: bool,
    smoke_only: bool = False,
    uat_url: str | None,
    live_url: str | None,
    expected_revision: str | None,
    coverage_fail_under: int,
    expect_live_promoted: bool = False,
    parallel_workers: int = 4,
) -> dict[str, Any]:
    steps: list[GateStep] = []

    if smoke_only:
        if not (uat_url and live_url and expected_revision):
            steps.append(
                GateStep(
                    name="uat_live_read_only_smoke",
                    status="fail",
                    returncode=1,
                    stderr="--uat-url, --live-url, and --expected-revision are required for --smoke-only.",
                )
            )
        else:
            steps.append(
                _smoke_check(
                    uat_url=uat_url,
                    live_url=live_url,
                    expected_revision=expected_revision,
                    expect_live_promoted=expect_live_promoted,
                )
            )
        status = "pass" if all(step.status in {"pass", "skipped"} and step.returncode == 0 for step in steps) else "fail"
        failed_steps = [step.name for step in steps if step.status == "fail" or step.returncode != 0]
        return {
            "status": status,
            "failed_steps": failed_steps,
            "steps": [asdict(step) for step in steps],
        }

    coverage_commands = [
        ("coverage_erase", [sys.executable, "-m", "coverage", "erase"]),
        ("python_unittest_coverage", [sys.executable, "-m", "coverage", "run", "-m", "unittest", "discover", "-s", "tests"]),
        ("python_coverage_report", [sys.executable, "-m", "coverage", "report", "--fail-under", str(coverage_fail_under)]),
    ]
    parallel_commands = [("node_check", ["node", "--check", str(path.relative_to(ROOT_DIR))]) for path in STATIC_JS_PATHS]
    parallel_commands.append(("source_code_qa_release_gate", [sys.executable, "scripts/run_source_code_qa_release_gate.py"]))

    for name, command in coverage_commands:
        step = _run_command(name, command)
        steps.append(step)
        if step.returncode != 0:
            break
    if all(step.returncode == 0 for step in steps):
        steps.extend(_run_parallel_commands(parallel_commands, max_workers=parallel_workers))

    if all(step.returncode == 0 for step in steps):
        if skip_smoke:
            steps.append(GateStep(name="uat_live_read_only_smoke", status="skipped", details={"reason": "--skip-smoke"}))
        elif not (uat_url and live_url and expected_revision):
            steps.append(
                GateStep(
                    name="uat_live_read_only_smoke",
                    status="fail",
                    returncode=1,
                    stderr="--uat-url, --live-url, and --expected-revision are required unless --skip-smoke is set.",
                )
            )
        else:
            steps.append(
                _smoke_check(
                    uat_url=uat_url,
                    live_url=live_url,
                    expected_revision=expected_revision,
                    expect_live_promoted=expect_live_promoted,
                )
            )

    status = "pass" if all(step.status in {"pass", "skipped"} and step.returncode == 0 for step in steps) else "fail"
    failed_steps = [step.name for step in steps if step.status == "fail" or step.returncode != 0]
    return {
        "status": status,
        "failed_steps": failed_steps,
        "steps": [asdict(step) for step in steps],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-smoke", action="store_true", help="Skip UAT/Live HTTP smoke checks.")
    parser.add_argument("--smoke-only", action="store_true", help="Run only the read-only UAT/Live HTTP smoke checks.")
    parser.add_argument("--uat-url", default=None, help="Cloud Run UAT tag URL.")
    parser.add_argument("--live-url", default=None, help="Mac-hosted Live portal URL.")
    parser.add_argument("--expected-revision", default=None, help="Git SHA expected on UAT and not yet on Live.")
    parser.add_argument(
        "--expect-live-promoted",
        action="store_true",
        help="Require Live to serve the expected revision, for post-promotion validation.",
    )
    parser.add_argument("--coverage-fail-under", type=int, default=100, help="Coverage percentage required for governed code.")
    parser.add_argument("--parallel-workers", type=int, default=4, help="Workers for independent JS and Source Code QA checks.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args(argv)

    result = run_gate(
        skip_smoke=bool(args.skip_smoke),
        smoke_only=bool(args.smoke_only),
        uat_url=args.uat_url,
        live_url=args.live_url,
        expected_revision=args.expected_revision,
        coverage_fail_under=int(args.coverage_fail_under),
        expect_live_promoted=bool(args.expect_live_promoted),
        parallel_workers=int(args.parallel_workers),
    )
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"System full test gate: {result['status']}")
        for step in result["steps"]:
            suffix = f" ({step['status']})"
            if step.get("returncode"):
                suffix += f" rc={step['returncode']}"
            print(f"- {step['name']}{suffix}")
            if step.get("stderr") and step["status"] == "fail":
                print(f"  {step['stderr'].strip()}")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
