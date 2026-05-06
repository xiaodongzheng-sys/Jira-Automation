import io
import json
import unittest
from unittest.mock import patch

from scripts import run_system_full_test_gate as gate


class SystemFullTestGateTests(unittest.TestCase):
    def test_skip_smoke_runs_release_gate_steps_in_order(self):
        commands = []

        def fake_run_command(name, command):
            commands.append((name, command))
            return gate.GateStep(name=name, command=command)

        with patch.object(gate, "STATIC_JS_PATHS", [gate.ROOT_DIR / "static" / "a.js", gate.ROOT_DIR / "static" / "b.js"]), patch.object(
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

        self.assertEqual(result["status"], "pass")
        self.assertEqual(
            [name for name, _ in commands],
            [
                "coverage_erase",
                "python_unittest_coverage",
                "python_coverage_report",
                "node_check",
                "node_check",
                "source_code_qa_release_gate",
            ],
        )
        self.assertEqual(commands[2][1][-2:], ["--fail-under", "100"])
        self.assertEqual(commands[3][1], ["node", "--check", "static/a.js"])
        self.assertEqual(commands[4][1], ["node", "--check", "static/b.js"])
        self.assertEqual(result["steps"][-1]["status"], "skipped")

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
