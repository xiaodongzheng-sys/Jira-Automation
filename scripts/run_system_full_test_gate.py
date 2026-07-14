#!/usr/bin/env python3
"""Run the read-only system release gate before publishing the portal."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
from urllib.request import Request, urlopen


ROOT_DIR = Path(__file__).resolve().parents[1]
STATIC_JS_PATHS = sorted((ROOT_DIR / "static").glob("*.js"))
GATE_PROOF_VERSION = 1
GATE_POLICY_VERSION = 2
COVERAGE_JSON_PATH = ROOT_DIR / ".team-portal" / "run" / "system_full_coverage.json"
COVERAGE_POLICY_PATH = ROOT_DIR / "config" / "coverage_risk_policy.json"
VALID_GATE_PROFILES = {"auto", "full", "fast"}


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


def _current_git_sha() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT_DIR), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else ""


def _source_fingerprint() -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT_DIR), "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        capture_output=True,
        check=True,
    )
    digest = hashlib.sha256()
    for raw_path in sorted(path for path in completed.stdout.split(b"\0") if path):
        rel_path = raw_path.decode("utf-8", errors="surrogateescape")
        file_path = ROOT_DIR / rel_path
        if not file_path.is_file():
            continue
        digest.update(rel_path.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        digest.update(file_path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _git_output(args: list[str], *, text: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(ROOT_DIR), *args],
        capture_output=True,
        text=text,
        check=False,
    )


def _changed_files_from_status_z(raw_status: bytes) -> list[str]:
    changed: list[str] = []
    entries = [entry for entry in raw_status.split(b"\0") if entry]
    index = 0
    while index < len(entries):
        entry = entries[index]
        status = entry[:2].decode("utf-8", errors="replace")
        path = entry[3:].decode("utf-8", errors="surrogateescape")
        if path:
            changed.append(path)
        if status[:1] in {"R", "C"} or status[1:2] in {"R", "C"}:
            index += 1
        index += 1
    return sorted(set(changed))


def _changed_files_for_gate() -> tuple[list[str], bool, str]:
    status = _git_output(["status", "--porcelain=v1", "-z"], text=False)
    if status.returncode != 0:
        return [], False, "git_status_failed"
    worktree_files = _changed_files_from_status_z(status.stdout)
    if worktree_files:
        return worktree_files, True, "worktree"

    latest_commit = _git_output(["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"])
    if latest_commit.returncode != 0:
        return [], False, "git_diff_tree_failed"
    files = sorted({line.strip() for line in latest_commit.stdout.splitlines() if line.strip()})
    return files, True, "latest_commit"


def _is_release_tooling_path(path: str) -> bool:
    if path == "docs/release-checklist.md":
        return True
    if path.startswith("scripts/lib/") and path.endswith(".sh"):
        return True
    if path.startswith("scripts/") and path.endswith(".sh"):
        return True
    return path in {
        "scripts/run_system_full_test_gate.py",
        "scripts/check_coverage_policy.py",
        "scripts/report_deploy_timings.py",
    }


def _classify_gate_profile(profile: str, changed_files: list[str], *, reliable: bool) -> tuple[str, str]:
    if profile not in VALID_GATE_PROFILES:
        raise ValueError(f"unsupported gate profile: {profile}")
    if profile in {"full", "fast"}:
        return profile, f"requested {profile}"
    if not reliable or not changed_files:
        return "full", "changed files are unavailable"

    for path in changed_files:
        if path.startswith("tests/"):
            continue
        if path.startswith(("docs/", "static/", "templates/", ".github/")):
            continue
        if path in {"README.md", ".dockerignore", ".gcloudignore", ".env.example"}:
            continue
        if _is_release_tooling_path(path):
            continue
        return "full", f"{path} requires the full release gate"
    return "fast", "all changed files are eligible for the fast release gate"


def _changed_static_js_paths(changed_files: list[str]) -> list[Path]:
    paths = []
    for path in changed_files:
        if path.startswith("static/") and path.endswith(".js"):
            file_path = ROOT_DIR / path
            if file_path.is_file():
                paths.append(file_path)
    return sorted(set(paths))


def _changed_shell_paths(changed_files: list[str]) -> list[str]:
    return sorted({path for path in changed_files if path.startswith("scripts/") and path.endswith(".sh") and (ROOT_DIR / path).is_file()})


def _changed_test_patterns(changed_files: list[str]) -> list[tuple[str, str]]:
    patterns: set[tuple[str, str]] = set()
    for path in changed_files:
        if not path.startswith("tests/") or not path.endswith(".py"):
            continue
        file_path = ROOT_DIR / path
        if not file_path.is_file():
            continue
        parent = str(Path(path).parent)
        patterns.add((parent, Path(path).name))
    return sorted(patterns)


def _fast_gate_commands(changed_files: list[str], *, include_browser_e2e: bool) -> list[tuple[str, list[str]]]:
    commands: list[tuple[str, list[str]]] = []
    for path in _changed_shell_paths(changed_files):
        commands.append(("bash_syntax", ["bash", "-n", path]))
    for path in _changed_static_js_paths(changed_files):
        commands.append(("node_check", ["node", "--check", str(path.relative_to(ROOT_DIR))]))
    for test_dir, pattern in _changed_test_patterns(changed_files):
        commands.append(("python_unittest_targeted", [sys.executable, "-m", "unittest", "discover", "-s", test_dir, "-p", pattern]))
    if any(_is_release_tooling_path(path) for path in changed_files):
        commands.append(
            (
                "python_unittest_release_tooling",
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_team_stack_scripts.py"],
            )
        )
    if "scripts/run_system_full_test_gate.py" in changed_files and not any(
        command[0] == "python_unittest_targeted" and command[1][-1] == "test_system_full_test_gate.py" for command in commands
    ):
        commands.append(
            (
                "python_unittest_system_gate",
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_system_full_test_gate.py"],
            )
        )
    if include_browser_e2e:
        commands.append(("browser_e2e", [sys.executable, "scripts/run_browser_e2e.py"]))
    return commands


def _gate_proof_path() -> Path:
    configured = os.environ.get("SYSTEM_FULL_TEST_GATE_PROOF_PATH")
    if configured:
        return Path(configured)
    return ROOT_DIR / ".team-portal" / "run" / "system_full_test_gate_verified.json"


def _write_gate_proof(*, result: dict[str, Any], coverage_fail_under: int, skip_smoke: bool) -> None:
    if os.environ.get("SYSTEM_FULL_TEST_GATE_WRITE_PROOF", "1") != "1":
        return
    if result.get("status") != "pass":
        return
    if not skip_smoke:
        return
    proof_path = _gate_proof_path()
    proof_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": GATE_PROOF_VERSION,
        "policy_version": GATE_POLICY_VERSION,
        "status": "pass",
        "git_sha": _current_git_sha(),
        "source_fingerprint": _source_fingerprint(),
        "coverage_fail_under": int(coverage_fail_under),
        "profile": result.get("profile") or "full",
        "profile_requested": result.get("profile_requested") or result.get("profile") or "full",
        "changed_files": result.get("changed_files") or [],
        "skip_smoke": True,
        "created_at_epoch": int(time.time()),
    }
    proof_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_reusable_gate_proof(*, coverage_fail_under: int, max_age_seconds: int, profile: str = "full") -> tuple[bool, str]:
    proof_path = _gate_proof_path()
    if not proof_path.exists():
        return False, f"no gate proof at {proof_path}"
    try:
        payload = json.loads(proof_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return False, f"gate proof is unreadable: {error}"

    if payload.get("version") != GATE_PROOF_VERSION or payload.get("status") != "pass":
        return False, "gate proof has unsupported version or non-pass status"
    if payload.get("policy_version") != GATE_POLICY_VERSION:
        return False, "gate proof policy version does not match"
    if int(payload.get("coverage_fail_under", -1)) != int(coverage_fail_under):
        return False, "gate proof coverage threshold does not match"
    proof_profile = str(payload.get("profile") or "full")
    if proof_profile != profile and not (proof_profile == "full" and profile == "fast"):
        return False, f"gate proof profile {proof_profile} does not satisfy requested {profile}"
    created_at = int(payload.get("created_at_epoch") or 0)
    age_seconds = int(time.time()) - created_at
    if max_age_seconds >= 0 and age_seconds > int(max_age_seconds):
        return False, f"gate proof is stale: {age_seconds}s old"
    expected_fingerprint = str(payload.get("source_fingerprint") or "")
    current_fingerprint = _source_fingerprint()
    if not expected_fingerprint or current_fingerprint != expected_fingerprint:
        return False, "gate proof source fingerprint does not match current tree"
    return True, f"reusing full gate proof for {payload.get('git_sha') or 'unknown sha'} ({age_seconds}s old)"


def _run_parallel_commands(commands: list[tuple[str, list[str]]], *, max_workers: int) -> list[GateStep]:
    if not commands:
        return []
    workers = max(1, min(max_workers, len(commands)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_run_command, name, command) for name, command in commands]
        return [future.result() for future in futures]


def _join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _portal_health_url(base_url: str) -> str:
    lowered = (base_url or "").strip().lower()
    health_path = "/cloud-healthz" if ".run.app" in lowered else "/healthz"
    return _join_url(base_url, health_path)


def _fetch_json(url: str) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": "team-portal-release-gate/1.0"}, method="GET")
    with urlopen(request, timeout=10) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError(f"{url} did not return a JSON object")
    return payload


def _smoke_check(*, live_url: str, expected_revision: str) -> GateStep:
    checks: list[dict[str, Any]] = []
    try:
        live_health_url = _portal_health_url(live_url)
        live_agent_url = _join_url(live_url, "/api/local-agent/healthz")

        live_health = _fetch_json(live_health_url)
        checks.append({"name": "live_healthz", "method": "GET", "url": live_health_url, "revision": live_health.get("revision")})
        live_agent = _fetch_json(live_agent_url)
        checks.append({"name": "live_local_agent_healthz", "method": "GET", "url": live_agent_url, "status": live_agent.get("status")})

        if live_health.get("revision") != expected_revision:
            raise RuntimeError(f"Live revision mismatch: {live_health.get('revision')} != {expected_revision}")
    except Exception as error:  # noqa: BLE001 - gate must report the exact failed smoke boundary.
        return GateStep(name="live_read_only_smoke", status="fail", returncode=1, stderr=str(error), details={"checks": checks})
    return GateStep(name="live_read_only_smoke", details={"checks": checks})


def run_gate(
    *,
    skip_smoke: bool,
    include_browser_e2e: bool = False,
    smoke_only: bool = False,
    live_url: str | None,
    expected_revision: str | None,
    coverage_fail_under: int,
    parallel_workers: int = 4,
    profile: str = "full",
) -> dict[str, Any]:
    steps: list[GateStep] = []
    changed_files, changed_files_reliable, changed_files_source = _changed_files_for_gate()
    effective_profile, profile_reason = _classify_gate_profile(profile, changed_files, reliable=changed_files_reliable)

    if smoke_only:
        if not (live_url and expected_revision):
            steps.append(
                GateStep(
                    name="live_read_only_smoke",
                    status="fail",
                    returncode=1,
                    stderr="--live-url and --expected-revision are required for --smoke-only.",
                )
            )
        else:
            steps.append(
                _smoke_check(
                    live_url=live_url,
                    expected_revision=expected_revision,
                )
            )
        status = "pass" if all(step.status in {"pass", "skipped"} and step.returncode == 0 for step in steps) else "fail"
        failed_steps = [step.name for step in steps if step.status == "fail" or step.returncode != 0]
        return {
            "status": status,
            "failed_steps": failed_steps,
            "profile": "smoke",
            "profile_requested": profile,
            "profile_reason": "--smoke-only",
            "changed_files": changed_files,
            "changed_files_source": changed_files_source,
            "steps": [asdict(step) for step in steps],
        }

    if effective_profile == "fast":
        steps.append(
            GateStep(
                name="fast_profile_classification",
                details={
                    "reason": profile_reason,
                    "changed_files": changed_files,
                    "changed_files_source": changed_files_source,
                },
            )
        )
        commands = _fast_gate_commands(changed_files, include_browser_e2e=include_browser_e2e)
        if commands:
            steps.extend(_run_parallel_commands(commands, max_workers=parallel_workers))
        else:
            steps.append(GateStep(name="fast_profile_noop", details={"reason": "no targeted local checks for changed files"}))
    else:
        coverage_commands = [
            ("coverage_erase", [sys.executable, "-m", "coverage", "erase", "--rcfile=/dev/null"]),
            (
                "python_unittest_coverage",
                [
                    sys.executable,
                    "-m",
                    "coverage",
                    "run",
                    "--rcfile=/dev/null",
                    "--source=bpmis_jira_tool,prd_briefing",
                    "-m",
                    "unittest",
                    "discover",
                    "-s",
                    "tests",
                ],
            ),
            (
                "python_coverage_json",
                [
                    sys.executable,
                    "-m",
                    "coverage",
                    "json",
                    "--rcfile=/dev/null",
                    "-o",
                    str(COVERAGE_JSON_PATH.relative_to(ROOT_DIR)),
                ],
            ),
            (
                "risk_coverage_gate",
                [
                    sys.executable,
                    "scripts/check_coverage_policy.py",
                    "--coverage-json",
                    str(COVERAGE_JSON_PATH.relative_to(ROOT_DIR)),
                    "--policy",
                    str(COVERAGE_POLICY_PATH.relative_to(ROOT_DIR)),
                    "--governed-fail-under",
                    str(coverage_fail_under),
                ],
            ),
        ]
        parallel_commands = [("node_check", ["node", "--check", str(path.relative_to(ROOT_DIR))]) for path in STATIC_JS_PATHS]
        parallel_commands.append(("source_code_qa_release_gate", [sys.executable, "scripts/run_source_code_qa_release_gate.py"]))
        if include_browser_e2e:
            parallel_commands.append(("browser_e2e", [sys.executable, "scripts/run_browser_e2e.py"]))

        for name, command in coverage_commands:
            step = _run_command(name, command)
            steps.append(step)
            if step.returncode != 0:
                break
        if all(step.returncode == 0 for step in steps):
            steps.extend(_run_parallel_commands(parallel_commands, max_workers=parallel_workers))

    if all(step.returncode == 0 for step in steps):
        if skip_smoke:
            steps.append(GateStep(name="live_read_only_smoke", status="skipped", details={"reason": "--skip-smoke"}))
        elif not (live_url and expected_revision):
            steps.append(
                GateStep(
                    name="live_read_only_smoke",
                    status="fail",
                    returncode=1,
                    stderr="--live-url and --expected-revision are required unless --skip-smoke is set.",
                )
            )
        else:
            steps.append(
                _smoke_check(
                    live_url=live_url,
                    expected_revision=expected_revision,
                )
            )

    status = "pass" if all(step.status in {"pass", "skipped"} and step.returncode == 0 for step in steps) else "fail"
    failed_steps = [step.name for step in steps if step.status == "fail" or step.returncode != 0]
    return {
        "status": status,
        "failed_steps": failed_steps,
        "profile": effective_profile,
        "profile_requested": profile,
        "profile_reason": profile_reason,
        "changed_files": changed_files,
        "changed_files_source": changed_files_source,
        "steps": [asdict(step) for step in steps],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-smoke", action="store_true", help="Skip Live HTTP smoke checks.")
    parser.add_argument("--include-browser-e2e", action="store_true", help="Run local Playwright browser E2E smoke tests.")
    parser.add_argument("--smoke-only", action="store_true", help="Run only the read-only Live HTTP smoke checks.")
    parser.add_argument("--live-url", default=None, help="Mac-hosted Live portal URL.")
    parser.add_argument("--expected-revision", default=None, help="Git SHA expected on Live.")
    parser.add_argument("--coverage-fail-under", type=int, default=100, help="Coverage percentage required for governed code.")
    parser.add_argument("--parallel-workers", type=int, default=4, help="Workers for independent JS and Source Code QA checks.")
    parser.add_argument(
        "--profile",
        choices=sorted(VALID_GATE_PROFILES),
        default="full",
        help="Release gate profile. Use auto to choose fast/full from changed files.",
    )
    parser.add_argument(
        "--check-proof",
        action="store_true",
        help="Return success when the current source tree already has a recent passing full-gate proof.",
    )
    parser.add_argument(
        "--proof-max-age-seconds",
        type=int,
        default=7200,
        help="Maximum age for --check-proof reuse. Use -1 to disable age checks.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args(argv)

    if args.check_proof:
        changed_files, reliable, _ = _changed_files_for_gate()
        effective_profile, _ = _classify_gate_profile(args.profile, changed_files, reliable=reliable)
        reusable, reason = load_reusable_gate_proof(
            coverage_fail_under=int(args.coverage_fail_under),
            max_age_seconds=int(args.proof_max_age_seconds),
            profile=effective_profile,
        )
        payload = {"status": "pass" if reusable else "miss", "reason": reason, "profile": effective_profile}
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
        else:
            print(f"System full test gate proof: {payload['status']} - {reason}")
        return 0 if reusable else 1

    result = run_gate(
        skip_smoke=bool(args.skip_smoke),
        include_browser_e2e=bool(args.include_browser_e2e),
        smoke_only=bool(args.smoke_only),
        live_url=args.live_url,
        expected_revision=args.expected_revision,
        coverage_fail_under=int(args.coverage_fail_under),
        parallel_workers=int(args.parallel_workers),
        profile=str(args.profile),
    )
    if not args.smoke_only:
        _write_gate_proof(result=result, coverage_fail_under=int(args.coverage_fail_under), skip_smoke=bool(args.skip_smoke))
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"System full test gate: {result['status']} ({result.get('profile', args.profile)})")
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
