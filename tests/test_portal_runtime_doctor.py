from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from scripts import portal_runtime_doctor


class PortalRuntimeDoctorTests(unittest.TestCase):
    def _write_jsonl(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    def test_build_report_summarizes_portal_runtime_signals(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            now = datetime.now(portal_runtime_doctor.SGT)
            now_sgt = now.strftime("%Y-%m-%d %H:%M:%S SGT")
            now_epoch = int(now.timestamp())
            self._write_jsonl(
                data_root / "llm_call_ledger.jsonl",
                [
                    {
                        "timestamp_sgt": now_sgt,
                        "flow": "seatalk",
                        "route": "cheap",
                        "model_id": "gpt-5.4-mini",
                        "provider": "codex_cli_bridge",
                        "status": "ok",
                        "latency_ms": 1200,
                        "estimated_prompt_tokens": 4500,
                        "prompt_mode": "seatalk_7_day_insights_v4",
                    },
                    {
                        "timestamp_sgt": now_sgt,
                        "flow": "unknown",
                        "route": "",
                        "model_id": "codex-cli",
                        "provider": "codex_cli_bridge",
                        "status": "error",
                        "error_category": "api_error_payload",
                        "latency_ms": 188000,
                        "estimated_prompt_tokens": 62000,
                        "prompt_mode": "ad_hoc",
                    },
                    {
                        "timestamp_sgt": now_sgt,
                        "flow": "source_code_qa",
                        "route": "repair",
                        "model_id": "gpt-5.5",
                        "provider": "codex_cli_bridge",
                        "status": "error",
                        "error": "Bad Request",
                        "error_category": "api_error_payload",
                        "latency_ms": 0,
                        "estimated_prompt_tokens": 1349,
                        "prompt_mode": "codex_investigation_brief_v5",
                    },
                ],
            )
            (data_root / "run").mkdir()
            (data_root / "run" / "jobs.json").write_text(
                json.dumps(
                    {
                        "jobs": {
                            "job-1": {
                                "job_id": "job-1",
                                "action": "source-code-qa-query",
                                "state": "failed",
                                "stage": "interrupted",
                                "updated_at": now_epoch,
                                "message": "Interrupted by restart.",
                                "error_retryable": True,
                            },
                            "job-2": {
                                "job_id": "job-2",
                                "action": "meeting-recorder-process",
                                "state": "completed",
                                "stage": "completed",
                                "updated_at": now_epoch,
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            record_dir = data_root / "meeting_records" / "records" / "rec-1"
            record_dir.mkdir(parents=True)
            (record_dir / "metadata.json").write_text(
                json.dumps({"record_id": "rec-1", "status": "failed", "updated_at": now_sgt}),
                encoding="utf-8",
            )

            with patch.object(
                portal_runtime_doctor,
                "_source_code_qa_summary",
                return_value=(["index_health=ready ready=2/2 stale_or_missing=0", "ops_summary_status=pass"], []),
            ), patch.object(
                portal_runtime_doctor,
                "_mac_portal_runtime_summary",
                return_value=({"status": "online", "details": "status=online"}, []),
            ), patch.object(
                portal_runtime_doctor,
                "_shared_session_summary",
                return_value=({"status": "ok", "details": "status=ok"}, []),
            ):
                report = portal_runtime_doctor.build_report(data_root, limit=10)

        self.assertEqual(report["status"], "warn")
        self.assertEqual(report["llm"]["sample_size"], 3)
        self.assertEqual(report["llm"]["actionable_sample_size"], 2)
        self.assertEqual(report["llm"]["test_fixture_rows"], 1)
        self.assertEqual(report["llm"]["flows"]["seatalk"], 1)
        self.assertEqual(report["llm"]["flows"]["unknown"], 1)
        self.assertEqual(report["llm"]["estimated_prompt_tokens"]["max"], 62000)
        self.assertEqual(report["jobs"]["states"]["failed"], 1)
        self.assertEqual(report["meeting_records"]["statuses"]["failed"], 1)
        issue_codes = {issue["code"] for issue in report["issues"]}
        self.assertIn("llm_unknown_flow", issue_codes)
        self.assertIn("llm_high_prompt_tokens", issue_codes)
        self.assertIn("job_failures", issue_codes)
        self.assertIn("meeting_record_failures", issue_codes)

    def test_build_report_ignores_historical_llm_failures_by_default(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            self._write_jsonl(
                data_root / "llm_call_ledger.jsonl",
                [
                    {
                        "timestamp_sgt": "2026-05-16 06:11:43 SGT",
                        "flow": "source_code_qa",
                        "route": "deep",
                        "model_id": "gpt-5.5",
                        "provider": "codex_cli_bridge",
                        "status": "error",
                        "error_category": "nonzero_exit",
                        "latency_ms": 1967990,
                        "estimated_prompt_tokens": 9000,
                        "prompt_mode": "codex_investigation_brief_v5",
                    }
                ],
            )
            (data_root / "run").mkdir()
            (data_root / "run" / "jobs.json").write_text(json.dumps({"jobs": {}}), encoding="utf-8")

            with patch.object(
                portal_runtime_doctor,
                "_source_code_qa_summary",
                return_value=(["telemetry_window=0", "ops_summary_status=pass"], []),
            ), patch.object(
                portal_runtime_doctor,
                "_mac_portal_runtime_summary",
                return_value=({"status": "online", "details": "status=online"}, []),
            ), patch.object(
                portal_runtime_doctor,
                "_shared_session_summary",
                return_value=({"status": "ok", "details": "status=ok"}, []),
            ):
                report = portal_runtime_doctor.build_report(data_root, limit=10)

        self.assertEqual(report["recent_hours"], portal_runtime_doctor.DEFAULT_RECENT_HOURS)
        issue_codes = {issue["code"] for issue in report["issues"]}
        self.assertNotIn("llm_errors", issue_codes)
        self.assertNotIn("llm_slow_calls", issue_codes)

    def test_format_report_exposes_key_sections(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            with patch.object(
                portal_runtime_doctor,
                "_source_code_qa_summary",
                return_value=(["telemetry_window=0", "ops_summary_status=pass"], []),
            ), patch.object(
                portal_runtime_doctor,
                "_mac_portal_runtime_summary",
                return_value=({"status": "online", "details": "status=online"}, []),
            ), patch.object(
                portal_runtime_doctor,
                "_shared_session_summary",
                return_value=({"status": "ok", "details": "status=ok"}, []),
            ):
                report = portal_runtime_doctor.build_report(data_root, limit=5)
            output = "\n".join(portal_runtime_doctor.format_report(report))

        self.assertIn("== Portal Runtime Doctor ==", output)
        self.assertIn("== LLM Ledger ==", output)
        self.assertIn("== Jobs ==", output)
        self.assertIn("== Meeting Records ==", output)
        self.assertIn("== Mac Portal Availability ==", output)
        self.assertIn("== Shared Session Configuration ==", output)
        self.assertIn("== Permission Snapshot ==", output)
        self.assertIn("permission=PRD Briefing Tool visibility=admin only", output)


if __name__ == "__main__":
    unittest.main()
