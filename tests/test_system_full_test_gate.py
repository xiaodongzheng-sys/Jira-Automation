import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import run_system_full_test_gate as gate


class SystemFullTestGateTests(unittest.TestCase):
    def test_skip_smoke_runs_release_gate_steps_in_order(self):
        commands = []
        parallel_workers = []

        def fake_run_command(name, command):
            commands.append((name, command))
            return gate.GateStep(name=name, command=command)

        def fake_parallel(parallel_commands, *, max_workers):
            parallel_workers.append(max_workers)
            return [fake_run_command(name, command) for name, command in parallel_commands]

        with patch.object(gate, "STATIC_JS_PATHS", [gate.ROOT_DIR / "static" / "a.js", gate.ROOT_DIR / "static" / "b.js"]), patch.object(
            gate,
            "_run_command",
            side_effect=fake_run_command,
        ), patch.object(
            gate,
            "_run_parallel_commands",
            side_effect=fake_parallel,
        ):
            result = gate.run_gate(
                skip_smoke=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
                parallel_workers=3,
            )

        self.assertEqual(result["status"], "pass")
        self.assertEqual(
            [name for name, _ in commands],
            [
                "coverage_erase",
                "python_unittest_coverage",
                "python_coverage_json",
                "risk_coverage_gate",
                "node_check",
                "node_check",
                "source_code_qa_release_gate",
            ],
        )
        self.assertIn("--source=bpmis_jira_tool,prd_briefing", commands[1][1])
        self.assertEqual(commands[3][1][-2:], ["--governed-fail-under", "100"])
        self.assertEqual(commands[4][1], ["node", "--check", "static/a.js"])
        self.assertEqual(commands[5][1], ["node", "--check", "static/b.js"])
        self.assertEqual(parallel_workers, [3])
        self.assertEqual(result["steps"][-1]["status"], "skipped")

    def test_include_browser_e2e_adds_optional_parallel_step(self):
        commands = []

        def fake_run_command(name, command):
            commands.append((name, command))
            return gate.GateStep(name=name, command=command)

        def fake_parallel(parallel_commands, *, max_workers):
            return [fake_run_command(name, command) for name, command in parallel_commands]

        with patch.object(gate, "STATIC_JS_PATHS", []), patch.object(
            gate,
            "_run_command",
            side_effect=fake_run_command,
        ), patch.object(
            gate,
            "_run_parallel_commands",
            side_effect=fake_parallel,
        ):
            result = gate.run_gate(
                skip_smoke=True,
                include_browser_e2e=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
            )

        self.assertEqual(result["status"], "pass")
        self.assertIn(("browser_e2e", [sys.executable, "scripts/run_browser_e2e.py"]), commands)

    def test_auto_profile_uses_full_for_backend_runtime_changes(self):
        commands = []

        def fake_run_command(name, command):
            commands.append((name, command))
            return gate.GateStep(name=name, command=command)

        with patch.object(gate, "_changed_files_for_gate", return_value=(["bpmis_jira_tool/web.py"], True, "worktree")), patch.object(
            gate,
            "STATIC_JS_PATHS",
            [],
        ), patch.object(gate, "_run_command", side_effect=fake_run_command), patch.object(
            gate,
            "_run_parallel_commands",
            side_effect=lambda parallel_commands, max_workers: [fake_run_command(name, command) for name, command in parallel_commands],
        ):
            result = gate.run_gate(
                skip_smoke=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
                profile="auto",
            )

        self.assertEqual(result["profile"], "full")
        self.assertEqual(commands[0][0], "coverage_erase")

    def test_auto_profile_uses_fast_for_docs_and_tests_only_changes(self):
        parallel_commands = []

        def fake_parallel(commands, *, max_workers):
            parallel_commands.extend(commands)
            return [gate.GateStep(name=name, command=command) for name, command in commands]

        with patch.object(
            gate,
            "_changed_files_for_gate",
            return_value=(["docs/release-checklist.md", "tests/test_system_full_test_gate.py"], True, "worktree"),
        ), patch.object(gate, "_run_command") as run_command, patch.object(gate, "_run_parallel_commands", side_effect=fake_parallel):
            result = gate.run_gate(
                skip_smoke=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
                profile="auto",
            )

        self.assertEqual(result["profile"], "fast")
        run_command.assert_not_called()
        command_names = [name for name, _ in parallel_commands]
        self.assertIn("python_unittest_targeted", command_names)
        self.assertIn("python_unittest_release_tooling", command_names)
        self.assertNotIn("coverage_erase", command_names)

    def test_auto_profile_release_tooling_uses_targeted_script_checks(self):
        parallel_commands = []

        def fake_parallel(commands, *, max_workers):
            parallel_commands.extend(commands)
            return [gate.GateStep(name=name, command=command) for name, command in commands]

        with patch.object(
            gate,
            "_changed_files_for_gate",
            return_value=(["scripts/release_uat_fast.sh"], True, "worktree"),
        ), patch.object(gate, "_run_parallel_commands", side_effect=fake_parallel):
            result = gate.run_gate(
                skip_smoke=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
                profile="auto",
            )

        self.assertEqual(result["profile"], "fast")
        self.assertIn(("bash_syntax", ["bash", "-n", "scripts/release_uat_fast.sh"]), parallel_commands)
        self.assertIn(
            (
                "python_unittest_release_tooling",
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_team_stack_scripts.py"],
            ),
            parallel_commands,
        )

    def test_auto_profile_defaults_to_full_for_unknown_paths(self):
        with patch.object(gate, "_changed_files_for_gate", return_value=(["tools/unknown_runtime.txt"], True, "worktree")), patch.object(
            gate,
            "STATIC_JS_PATHS",
            [],
        ), patch.object(gate, "_run_command", return_value=gate.GateStep(name="ok")), patch.object(
            gate,
            "_run_parallel_commands",
            return_value=[],
        ):
            result = gate.run_gate(
                skip_smoke=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
                profile="auto",
            )

        self.assertEqual(result["profile"], "full")

    def test_gate_stops_after_failed_command(self):
        def fake_run_command(name, command):
            if name == "python_unittest_coverage":
                return gate.GateStep(name=name, command=command, returncode=1, status="fail", stderr="failed")
            return gate.GateStep(name=name, command=command)

        with patch.object(gate, "STATIC_JS_PATHS", [gate.ROOT_DIR / "static" / "a.js"]), patch.object(
            gate,
            "_run_command",
            side_effect=fake_run_command,
        ):
            result = gate.run_gate(
                skip_smoke=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
            )

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["failed_steps"], ["python_unittest_coverage"])
        self.assertEqual([step["name"] for step in result["steps"]], ["coverage_erase", "python_unittest_coverage"])

    def test_gate_stops_after_failed_risk_coverage_policy(self):
        def fake_run_command(name, command):
            if name == "risk_coverage_gate":
                return gate.GateStep(name=name, command=command, returncode=1, status="fail", stderr="coverage low")
            return gate.GateStep(name=name, command=command)

        with patch.object(gate, "STATIC_JS_PATHS", [gate.ROOT_DIR / "static" / "a.js"]), patch.object(
            gate,
            "_run_command",
            side_effect=fake_run_command,
        ), patch.object(gate, "_run_parallel_commands") as parallel:
            result = gate.run_gate(
                skip_smoke=True,
                uat_url=None,
                live_url=None,
                expected_revision=None,
                coverage_fail_under=100,
            )

        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["failed_steps"], ["risk_coverage_gate"])
        self.assertEqual(
            [step["name"] for step in result["steps"]],
            ["coverage_erase", "python_unittest_coverage", "python_coverage_json", "risk_coverage_gate"],
        )
        parallel.assert_not_called()

    def test_smoke_only_skips_local_release_gate_commands(self):
        payloads = {
            "https://uat.example/healthz/": {"status": "ok", "revision": "new-sha"},
            "https://uat.example/api/local-agent/healthz": {"status": "ok"},
            "https://live.example/healthz": {"status": "ok", "revision": "old-sha"},
            "https://live.example/api/local-agent/healthz": {"status": "ok"},
        }

        def fake_fetch(url):
            return payloads[url]

        with patch.object(gate, "_run_command") as run_command, patch.object(gate, "_fetch_json", side_effect=fake_fetch):
            result = gate.run_gate(
                skip_smoke=False,
                smoke_only=True,
                uat_url="https://uat.example",
                live_url="https://live.example",
                expected_revision="new-sha",
                coverage_fail_under=100,
            )

        self.assertEqual(result["status"], "pass")
        self.assertEqual([step["name"] for step in result["steps"]], ["uat_live_read_only_smoke"])
        run_command.assert_not_called()

    def test_smoke_uses_get_only_and_checks_uat_and_live_revisions(self):
        calls = []
        payloads = {
            "https://uat.example/healthz/": {"status": "ok", "revision": "new-sha"},
            "https://uat.example/api/local-agent/healthz": {"status": "ok"},
            "https://live.example/healthz": {"status": "ok", "revision": "old-sha"},
            "https://live.example/api/local-agent/healthz": {"status": "ok"},
        }

        def fake_urlopen(request, timeout):
            calls.append((request.full_url, request.get_method(), timeout))
            return _JsonResponse(payloads[request.full_url])

        with patch.object(gate, "urlopen", side_effect=fake_urlopen):
            step = gate._smoke_check(uat_url="https://uat.example", live_url="https://live.example", expected_revision="new-sha")

        self.assertEqual(step.status, "pass")
        self.assertEqual([method for _, method, _ in calls], ["GET", "GET", "GET", "GET"])
        self.assertEqual(
            [url for url, _, _ in calls],
            [
                "https://uat.example/healthz/",
                "https://uat.example/api/local-agent/healthz",
                "https://live.example/healthz",
                "https://live.example/api/local-agent/healthz",
            ],
        )

    def test_smoke_fails_when_live_already_serves_expected_revision(self):
        payloads = {
            "https://uat.example/healthz/": {"status": "ok", "revision": "new-sha"},
            "https://uat.example/api/local-agent/healthz": {"status": "ok"},
            "https://live.example/healthz": {"status": "ok", "revision": "new-sha"},
            "https://live.example/api/local-agent/healthz": {"status": "ok"},
        }

        def fake_urlopen(request, timeout):
            return _JsonResponse(payloads[request.full_url])

        with patch.object(gate, "urlopen", side_effect=fake_urlopen):
            step = gate._smoke_check(uat_url="https://uat.example", live_url="https://live.example", expected_revision="new-sha")

        self.assertEqual(step.status, "fail")
        self.assertIn("Live already serves the UAT revision", step.stderr)

    def test_json_main_returns_failure_when_smoke_arguments_are_missing(self):
        with patch.object(gate, "_run_command", return_value=gate.GateStep(name="ok")), patch.object(
            gate,
            "STATIC_JS_PATHS",
            [],
        ), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            returncode = gate.main(["--json"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(returncode, 1)
        self.assertEqual(payload["status"], "fail")
        self.assertEqual(payload["failed_steps"], ["uat_live_read_only_smoke"])

    def test_main_writes_reusable_gate_proof_for_passed_skip_smoke_gate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_path = Path(temp_dir) / "proof.json"
            result = {
                "status": "pass",
                "failed_steps": [],
                "steps": [{"name": "coverage_erase", "status": "pass", "returncode": 0}],
            }
            with patch.dict(os.environ, {"SYSTEM_FULL_TEST_GATE_PROOF_PATH": str(proof_path)}), patch.object(
                gate,
                "run_gate",
                return_value=result,
            ), patch.object(gate, "_source_fingerprint", return_value="fingerprint-1"), patch.object(
                gate,
                "_current_git_sha",
                return_value="sha-1",
            ), patch.object(
                gate.time,
                "time",
                return_value=1000,
            ), patch("sys.stdout", new_callable=io.StringIO):
                returncode = gate.main(["--skip-smoke", "--coverage-fail-under", "95"])

            self.assertEqual(returncode, 0)
            payload = json.loads(proof_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "pass")
            self.assertEqual(payload["git_sha"], "sha-1")
            self.assertEqual(payload["source_fingerprint"], "fingerprint-1")
            self.assertEqual(payload["coverage_fail_under"], 95)
            self.assertEqual(payload["policy_version"], gate.GATE_POLICY_VERSION)
            self.assertEqual(payload["profile"], "full")

    def test_check_proof_accepts_matching_recent_source_fingerprint(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_path = Path(temp_dir) / "proof.json"
            proof_path.write_text(
                json.dumps(
                    {
                        "version": gate.GATE_PROOF_VERSION,
                        "policy_version": gate.GATE_POLICY_VERSION,
                        "status": "pass",
                        "git_sha": "sha-1",
                        "source_fingerprint": "fingerprint-1",
                        "coverage_fail_under": 100,
                        "profile": "full",
                        "skip_smoke": True,
                        "created_at_epoch": 1000,
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"SYSTEM_FULL_TEST_GATE_PROOF_PATH": str(proof_path)}), patch.object(
                gate,
                "_source_fingerprint",
                return_value="fingerprint-1",
            ), patch.object(
                gate.time,
                "time",
                return_value=1100,
            ), patch("sys.stdout", new_callable=io.StringIO) as stdout:
                returncode = gate.main(["--check-proof", "--proof-max-age-seconds", "200"])

            self.assertEqual(returncode, 0)
            self.assertIn("System full test gate proof: pass", stdout.getvalue())

    def test_check_proof_rejects_stale_or_changed_source_fingerprint(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_path = Path(temp_dir) / "proof.json"
            proof_path.write_text(
                json.dumps(
                    {
                        "version": gate.GATE_PROOF_VERSION,
                        "policy_version": gate.GATE_POLICY_VERSION,
                        "status": "pass",
                        "git_sha": "sha-1",
                        "source_fingerprint": "fingerprint-1",
                        "coverage_fail_under": 100,
                        "profile": "full",
                        "skip_smoke": True,
                        "created_at_epoch": 1000,
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"SYSTEM_FULL_TEST_GATE_PROOF_PATH": str(proof_path)}), patch.object(
                gate,
                "_source_fingerprint",
                return_value="fingerprint-2",
            ), patch.object(
                gate.time,
                "time",
                return_value=1100,
            ), patch("sys.stdout", new_callable=io.StringIO) as stdout:
                returncode = gate.main(["--check-proof", "--proof-max-age-seconds", "200"])

            self.assertEqual(returncode, 1)
            self.assertIn("fingerprint does not match", stdout.getvalue())

    def test_check_proof_allows_full_proof_for_fast_but_not_fast_for_full(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_path = Path(temp_dir) / "proof.json"
            base_payload = {
                "version": gate.GATE_PROOF_VERSION,
                "policy_version": gate.GATE_POLICY_VERSION,
                "status": "pass",
                "git_sha": "sha-1",
                "source_fingerprint": "fingerprint-1",
                "coverage_fail_under": 100,
                "skip_smoke": True,
                "created_at_epoch": 1000,
            }
            proof_path.write_text(json.dumps({**base_payload, "profile": "full"}), encoding="utf-8")
            with patch.dict(os.environ, {"SYSTEM_FULL_TEST_GATE_PROOF_PATH": str(proof_path)}), patch.object(
                gate,
                "_source_fingerprint",
                return_value="fingerprint-1",
            ), patch.object(gate.time, "time", return_value=1100):
                reusable, _ = gate.load_reusable_gate_proof(coverage_fail_under=100, max_age_seconds=200, profile="fast")
            self.assertTrue(reusable)

            proof_path.write_text(json.dumps({**base_payload, "profile": "fast"}), encoding="utf-8")
            with patch.dict(os.environ, {"SYSTEM_FULL_TEST_GATE_PROOF_PATH": str(proof_path)}), patch.object(
                gate,
                "_source_fingerprint",
                return_value="fingerprint-1",
            ), patch.object(gate.time, "time", return_value=1100):
                reusable, reason = gate.load_reusable_gate_proof(coverage_fail_under=100, max_age_seconds=200, profile="full")
            self.assertFalse(reusable)
            self.assertIn("does not satisfy", reason)


class _JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


if __name__ == "__main__":
    unittest.main()
