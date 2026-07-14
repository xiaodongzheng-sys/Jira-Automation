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
