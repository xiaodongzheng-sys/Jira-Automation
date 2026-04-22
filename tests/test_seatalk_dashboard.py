from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
