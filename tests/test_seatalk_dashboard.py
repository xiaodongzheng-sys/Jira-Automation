from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService


class SeaTalkDashboardServiceTests(unittest.TestCase):
    def setUp(self):
        SeaTalkDashboardService.clear_cache()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name) / "SeaTalk"
        self.data_dir.mkdir()
        (self.data_dir / "config.json").write_text('{"LAST_LOGIN_USER_ID":"14420"}', encoding="utf-8")
        self.app_dir = Path(self.temp_dir.name) / "SeaTalk.app"
        self.binary_path = self.app_dir / "Contents/MacOS"
        self.binary_path.mkdir(parents=True)
        (self.binary_path / "SeaTalk").write_text("", encoding="utf-8")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_missing_owner_email_raises_config_error(self):
        with self.assertRaises(ConfigError):
            SeaTalkDashboardService(owner_email="")

    def test_build_overview_uses_local_runner_payload(self):
        payload = {
            "summary": {
                "received_today": 1456,
                "current_unread": 2,
                "read_rate_percent": None,
                "received_period_total": 7697,
                "sent_period_total": 514,
            },
            "trends": {
                "received": [{"date": "2026-04-21", "label": "Apr 21", "count": 1456}],
                "sent": [{"date": "2026-04-21", "label": "Apr 21", "count": 77}],
            },
            "metric_availability": {
                "current_unread": {"available": True, "reason": ""},
                "read_rate_percent": {"available": False, "reason": "Not available from local SeaTalk desktop data for this scope."},
            },
            "generated_at": "2026-04-21T21:00:00+08:00",
            "period_days": 7,
            "data_quality": {"used_fallback_cache": False, "partial_data": False, "status_note": "ok"},
        }
        calls: list[list[str]] = []

        def runner(command: list[str]):
            calls.append(command)
            return subprocess.CompletedProcess(args=command, returncode=0, stdout=json.dumps(payload), stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            command_runner=runner,
        )

        result = service.build_overview(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertEqual(result["summary"]["received_today"], 1456)
        self.assertEqual(result["summary"]["current_unread"], 2)
        self.assertTrue(result["metric_availability"]["current_unread"]["available"])
        self.assertEqual(len(calls), 1)
        self.assertIn("seatalk_local_metrics.js", calls[0][1])

    def test_dashboard_cache_reuses_previous_result(self):
        payload = {
            "summary": {"received_today": 1, "current_unread": 0, "read_rate_percent": None, "received_period_total": 1, "sent_period_total": 0},
            "trends": {"received": [], "sent": []},
            "metric_availability": {
                "current_unread": {"available": True, "reason": ""},
                "read_rate_percent": {"available": False, "reason": "N/A"},
            },
            "generated_at": "2026-04-21T21:00:00+08:00",
            "period_days": 7,
            "data_quality": {"used_fallback_cache": False, "partial_data": False, "status_note": "ok"},
        }
        call_count = 0

        def runner(command: list[str]):
            nonlocal call_count
            call_count += 1
            return subprocess.CompletedProcess(args=command, returncode=0, stdout=json.dumps(payload), stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            command_runner=runner,
        )
        now = datetime(2026, 4, 21, 21, 0).astimezone()

        first = service.build_overview(now=now)
        second = service.build_overview(now=now)

        self.assertEqual(call_count, 1)
        self.assertFalse(first["data_quality"]["used_fallback_cache"])
        self.assertTrue(second["data_quality"]["used_fallback_cache"])

    def test_missing_local_data_raises_config_error(self):
        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir / "missing"),
        )

        with self.assertRaises(ConfigError):
            service.build_overview()

    def test_non_zero_runner_exit_surfaces_tool_error(self):
        def runner(command: list[str]):
            return subprocess.CompletedProcess(args=command, returncode=1, stdout="", stderr="SeaTalk desktop database was not found.")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            command_runner=runner,
        )

        with self.assertRaises(ToolError):
            service.build_overview()

    def test_export_history_text_returns_attachment_content(self):
        calls: list[list[str]] = []

        def runner(command: list[str]):
            calls.append(command)
            if command[1].endswith("seatalk_local_export.js"):
                return subprocess.CompletedProcess(args=command, returncode=0, stdout="demo history", stderr="")
            payload = {
                "summary": {"received_today": 1, "current_unread": 0, "read_rate_percent": None, "received_period_total": 1, "sent_period_total": 0},
                "trends": {"received": [], "sent": []},
                "metric_availability": {
                    "current_unread": {"available": True, "reason": ""},
                    "read_rate_percent": {"available": False, "reason": "N/A"},
                },
                "generated_at": "2026-04-21T21:00:00+08:00",
                "period_days": 7,
                "data_quality": {"used_fallback_cache": False, "partial_data": False, "status_note": "ok"},
            }
            return subprocess.CompletedProcess(args=command, returncode=0, stdout=json.dumps(payload), stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            command_runner=runner,
        )

        content, filename = service.export_history_text(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertEqual(content, "demo history")
        self.assertEqual(filename, "seatalk-history-last-7-days.txt")
        self.assertTrue(any(command[1].endswith("seatalk_local_export.js") for command in calls))

    def test_build_insights_uses_codex_read_only_ephemeral_command(self):
        def local_runner(command: list[str]):
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="SeaTalk Chat History Export\n[2026-04-21 10:00:00] Alice: @Xiaodong please follow up AF rollout.\n",
                stderr="",
            )

        codex_calls = []

        def fake_codex_run(command, **kwargs):
            codex_calls.append((command, kwargs))
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text(
                json.dumps(
                    {
                        "project_updates": [
                            {
                                "domain": "Anti-fraud",
                                "title": "AF rollout",
                                "summary": "AF rollout needs follow-up.",
                                "status": "in_progress",
                                "evidence": "Apr 21, Alice asked Xiaodong to follow up.",
                            }
                        ],
                        "my_todos": [
                            {
                                "task": "Follow up AF rollout",
                                "domain": "Anti-fraud",
                                "priority": "high",
                                "due": "unknown",
                                "evidence": "Apr 21, Alice: @Xiaodong please follow up AF rollout.",
                            }
                        ],
                        "team_todos": [],
                    }
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            codex_model="gpt-5.5",
            command_runner=local_runner,
        )

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            result = service.build_insights(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertEqual(result["project_updates"][0]["title"], "AF rollout")
        self.assertEqual(result["my_todos"][0]["task"], "Follow up AF rollout")
        self.assertIn("id", result["my_todos"][0])
        self.assertEqual(result["model_id"], "codex:gpt-5.5")
        exec_command = codex_calls[-1][0]
        self.assertIn("--sandbox", exec_command)
        self.assertIn("read-only", exec_command)
        self.assertIn("--ephemeral", exec_command)
        self.assertIn("--json", exec_command)
        self.assertIn("--output-last-message", exec_command)

    def test_build_insights_sorts_my_todos_by_priority(self):
        def local_runner(command: list[str]):
            return subprocess.CompletedProcess(args=command, returncode=0, stdout="SeaTalk Chat History Export\nhello", stderr="")

        def fake_codex_run(command, **kwargs):
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text(
                json.dumps(
                    {
                        "project_updates": [],
                        "my_todos": [
                            {"task": "Low task", "domain": "GRC", "priority": "low", "due": "2026-04-30", "evidence": "low"},
                            {"task": "High task", "domain": "Anti-fraud", "priority": "high", "due": "unknown", "evidence": "high"},
                            {"task": "Medium task", "domain": "Credit Risk", "priority": "medium", "due": "2026-04-28", "evidence": "medium"},
                        ],
                        "team_todos": [
                            {"task": "Someone else's task", "domain": "GRC", "priority": "high", "due": "unknown", "evidence": "team"}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            command_runner=local_runner,
        )

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            result = service.build_insights(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertEqual([item["task"] for item in result["my_todos"]], ["High task", "Medium task", "Low task"])
        self.assertEqual(result["team_todos"], [])

    def test_build_insights_reuses_daily_cache(self):
        def local_runner(command: list[str]):
            return subprocess.CompletedProcess(args=command, returncode=0, stdout="SeaTalk Chat History Export\nhello", stderr="")

        codex_exec_count = 0

        def fake_codex_run(command, **kwargs):
            nonlocal codex_exec_count
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            codex_exec_count += 1
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text('{"project_updates":[],"my_todos":[],"team_todos":[]}', encoding="utf-8")
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            command_runner=local_runner,
        )
        now = datetime(2026, 4, 21, 21, 0).astimezone()

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            first = service.build_insights(now=now)
            second = service.build_insights(now=now)

        self.assertEqual(codex_exec_count, 1)
        self.assertFalse(first["cache"]["hit"])
        self.assertTrue(second["cache"]["hit"])

    def test_build_insights_invalid_codex_json_raises_tool_error(self):
        def local_runner(command: list[str]):
            return subprocess.CompletedProcess(args=command, returncode=0, stdout="SeaTalk Chat History Export\nhello", stderr="")

        def fake_codex_run(command, **kwargs):
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text("not json", encoding="utf-8")
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            command_runner=local_runner,
        )

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            with self.assertRaisesRegex(ToolError, "invalid SeaTalk insights JSON"):
                service.build_insights(now=datetime(2026, 4, 21, 21, 0).astimezone())

    def test_build_insights_requires_codex_login(self):
        def local_runner(command: list[str]):
            return subprocess.CompletedProcess(args=command, returncode=0, stdout="SeaTalk Chat History Export\nhello", stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            command_runner=local_runner,
        )

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            return_value=SimpleNamespace(returncode=1, stdout="", stderr="not logged in"),
        ):
            with self.assertRaisesRegex(ToolError, "Codex is unavailable"):
                service.build_insights(now=datetime(2026, 4, 21, 21, 0).astimezone())


if __name__ == "__main__":
    unittest.main()
