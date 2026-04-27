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
        overrides_path = Path(self.temp_dir.name) / "seatalk" / "name_overrides.json"

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
            name_overrides_path=overrides_path,
            command_runner=runner,
        )

        content, filename = service.export_history_text(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertEqual(content, "demo history")
        self.assertEqual(filename, "seatalk-history-last-7-days.txt")
        self.assertTrue(any(command[1].endswith("seatalk_local_export.js") for command in calls))
        export_command = next(command for command in calls if command[1].endswith("seatalk_local_export.js"))
        self.assertIn("--name-overrides", export_command)
        self.assertIn(str(overrides_path), export_command)

    def test_person_mapping_aliases_work_for_buddy_and_uid_exports(self):
        overrides_path = Path(self.temp_dir.name) / "seatalk" / "name_overrides.json"
        overrides_path.parent.mkdir(parents=True, exist_ok=True)
        overrides_path.write_text(json.dumps({"mappings": {"buddy-456": "Alice"}}), encoding="utf-8")

        script = Path(__file__).resolve().parents[1] / "bpmis_jira_tool" / "seatalk_local_export.js"
        source = script.read_text(encoding="utf-8")

        self.assertIn("personMappingAliases(key)", source)
        self.assertIn("mappings.set(alias, name)", source)
        self.assertIn("slice(0, 520)", source)
        self.assertIn("exampleScore(candidateExample)", source)

    def test_build_name_mappings_parses_unknown_ids(self):
        calls: list[list[str]] = []

        def runner(command: list[str]):
            calls.append(command)
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout=json.dumps(
                    {
                        "unknown_ids": [
                            {
                                "id": "group-123",
                                "type": "group",
                                "count": 9,
                                "example": "2026-04-21: Please review",
                                "priority_reason": "@mentioned me",
                            },
                            {"id": "UID 888", "type": "uid", "count": "4", "example": "2026-04-21: hello"},
                        ],
                        "generated_at": "2026-04-21T21:00:00+08:00",
                        "period_days": 7,
                    }
                ),
                stderr="",
            )

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            command_runner=runner,
        )

        result = service.build_name_mappings(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertEqual(result["unknown_ids"][0]["id"], "group-123")
        self.assertEqual(result["unknown_ids"][0]["count"], 9)
        self.assertEqual(result["unknown_ids"][0]["priority_reason"], "@mentioned me")
        self.assertEqual(result["unknown_ids"][1]["id"], "UID 888")
        self.assertTrue(any("--unknown-ids-json" in command for command in calls))

    def test_build_name_mappings_reuses_daily_disk_cache(self):
        calls: list[list[str]] = []

        def runner(command: list[str]):
            calls.append(command)
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout=json.dumps(
                    {
                        "unknown_ids": [{"id": "group-123", "type": "group", "count": 9, "example": "2026-04-21: Please review"}],
                        "generated_at": "2026-04-21T21:00:00+08:00",
                        "period_days": 7,
                    }
                ),
                stderr="",
            )

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            daily_cache_dir=Path(self.temp_dir.name) / "cache",
            command_runner=runner,
        )
        now = datetime(2026, 4, 21, 21, 0).astimezone()

        first = service.build_name_mappings(now=now)
        second = service.build_name_mappings(now=now)

        self.assertEqual(len(calls), 1)
        self.assertFalse(first["cache"]["hit"])
        self.assertTrue(second["cache"]["hit"])
        self.assertEqual(second["unknown_ids"][0]["id"], "group-123")

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

    def test_build_insights_compacts_large_history_before_codex(self):
        huge_history = "\n".join(
            [
                "SeaTalk Chat History Export",
                *[f"[2026-04-21 09:{index % 60:02d}:00] Noise: routine chat line {index} " + ("x" * 900) for index in range(900)],
                "[2026-04-21 18:00:00] Alice: @Xiaodong please follow up Credit Risk approval by Friday.",
                *[f"[2026-04-21 19:{index % 60:02d}:00] Recent: latest discussion {index}" for index in range(200)],
            ]
        )

        def local_runner(command: list[str]):
            return subprocess.CompletedProcess(args=command, returncode=0, stdout=huge_history, stderr="")

        prompts: list[str] = []

        def fake_codex_run(command, **kwargs):
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            prompts.append(str(kwargs.get("input") or ""))
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

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            service.build_insights(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertTrue(prompts)
        self.assertLess(len(prompts[0]), 700_000)
        self.assertIn("@Xiaodong please follow up Credit Risk approval", prompts[0])
        self.assertIn("[Most recent lines]", prompts[0])

    def test_build_insights_uses_incremental_history_for_todos(self):
        calls: list[list[str]] = []

        def local_runner(command: list[str]):
            calls.append(command)
            if "--since" in command:
                return subprocess.CompletedProcess(
                    args=command,
                    returncode=0,
                    stdout="SeaTalk Chat History Export\nWindow: since 2026-04-27T00:00:00+08:00\n[2026-04-28 10:00:00] Alice: @Xiaodong please review the new rollout note.\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="SeaTalk Chat History Export\nWindow: last 7 days\n[2026-04-22 10:00:00] Alice: @Xiaodong old to-do already processed.\n[2026-04-28 10:00:00] Alice: @Xiaodong please review the new rollout note.\n",
                stderr="",
            )

        prompts: list[str] = []

        def fake_codex_run(command, **kwargs):
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            prompts.append(str(kwargs.get("input") or ""))
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text(
                '{"project_updates":[],"my_todos":[{"task":"Review the new rollout note","domain":"Anti-fraud","priority":"medium","due":"unknown","evidence":"Apr 28 Alice"}],"team_todos":[]}',
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
            result = service.build_insights(
                now=datetime(2026, 4, 29, 9, 0).astimezone(),
                todo_since="2026-04-27T00:00:00+08:00",
            )

        self.assertEqual(len([command for command in calls if command[1].endswith("seatalk_local_export.js")]), 2)
        incremental_command = next(command for command in calls if "--since" in command)
        self.assertIn("2026-04-27T00:00:00+08:00", incremental_command)
        self.assertIn("[Project update history - use for project_updates only]", prompts[0])
        self.assertIn("old to-do already processed", prompts[0].split("[New to-do history - use for my_todos only]")[0])
        self.assertNotIn("old to-do already processed", prompts[0].split("[New to-do history - use for my_todos only]")[1])
        self.assertEqual(result["my_todos"][0]["task"], "Review the new rollout note")
        self.assertEqual(result["todo_processed_from"], "2026-04-27T00:00:00+08:00")
        self.assertTrue(result["todo_processed_until"].startswith("2026-04-29T09:00:00"))

    def test_build_insights_reuses_same_day_cache_after_todo_cursor_advances(self):
        local_call_count = 0

        def local_runner(command: list[str]):
            nonlocal local_call_count
            local_call_count += 1
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="SeaTalk Chat History Export\n[2026-04-29 10:00:00] Alice: @Xiaodong please review the rollout note.\n",
                stderr="",
            )

        codex_exec_count = 0

        def fake_codex_run(command, **kwargs):
            nonlocal codex_exec_count
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            codex_exec_count += 1
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text(
                '{"project_updates":[],"my_todos":[{"task":"Review rollout note","domain":"Anti-fraud","priority":"medium","due":"unknown","evidence":"Apr 29 Alice"}],"team_todos":[]}',
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            daily_cache_dir=Path(self.temp_dir.name) / "cache",
            command_runner=local_runner,
        )
        now = datetime(2026, 4, 29, 11, 0).astimezone()

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            first = service.build_insights(now=now, todo_since="2026-04-28T00:00:00+08:00")
            second = service.build_insights(now=now, todo_since=first["todo_processed_until"])

        self.assertEqual(codex_exec_count, 1)
        self.assertEqual(local_call_count, 2)
        self.assertFalse(first["cache"]["hit"])
        self.assertTrue(second["cache"]["hit"])

    def test_build_insights_filters_system_generated_group_messages(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "Window: last 7 days",
                "=== Risk Group (group-1) ===",
                "[2026-04-21 10:00:00] System Account: Automated reminder: submit weekly report.",
                "[2026-04-21 10:01:00] Alert Bot: Alarm notification: service latency high.",
                "[2026-04-21 10:01:30] Bob: reminder that I will send the PRD later.",
                "[2026-04-21 10:02:00] Alice: @Xiaodong please follow up the CRMS rollout.",
            ]
        )

        def local_runner(command: list[str]):
            return subprocess.CompletedProcess(args=command, returncode=0, stdout=history, stderr="")

        prompts: list[str] = []

        def fake_codex_run(command, **kwargs):
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            prompts.append(str(kwargs.get("input") or ""))
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

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            service.build_insights(now=datetime(2026, 4, 21, 21, 0).astimezone())

        self.assertTrue(prompts)
        self.assertNotIn("Automated reminder: submit weekly report", prompts[0])
        self.assertNotIn("Alarm notification: service latency high", prompts[0])
        self.assertIn("Bob: reminder that I will send the PRD later", prompts[0])
        self.assertIn("@Xiaodong please follow up the CRMS rollout", prompts[0])
        self.assertIn("System-generated alarm/reminder messages removed: 2.", prompts[0])

    def test_insights_prompt_includes_project_tabs_and_general_todo_guidance(self):
        prompt = SeaTalkDashboardService._insights_user_prompt(
            history_text="SeaTalk Chat History Export\nhello",
            days=7,
            now=datetime(2026, 4, 21, 21, 0).astimezone(),
        )
        system_prompt = SeaTalkDashboardService._insights_system_prompt()

        self.assertIn("Anti-fraud, Credit Risk, Ops Risk, General", prompt)
        self.assertIn("AI sharing", prompt)
        self.assertIn("Key Project table", prompt)
        self.assertIn("slide", prompt.lower())
        self.assertIn("leadership", system_prompt)
        self.assertIn("broad awareness radar", system_prompt)
        self.assertIn("anything worth Xiaodong's awareness", prompt)
        self.assertIn("It is OK for a General update to have no Xiaodong-owned todo", prompt)
        self.assertIn("separate General-awareness pass", system_prompt)
        self.assertIn("put 3 to 6 General project_updates first", prompt)

    def test_insights_normalizes_update_and_todo_domains(self):
        parsed = SeaTalkDashboardService._parse_insights_response(
            json.dumps(
                {
                    "project_updates": [
                        {"domain": "AF", "title": "AF", "summary": "", "status": "done", "evidence": ""},
                        {"domain": "Collection", "title": "CR", "summary": "", "status": "done", "evidence": ""},
                        {"domain": "GRC", "title": "GRC", "summary": "", "status": "done", "evidence": ""},
                        {"domain": "Deposit", "title": "Deposit", "summary": "", "status": "done", "evidence": ""},
                    ],
                    "my_todos": [
                        {"task": "Prepare AI sharing", "domain": "Leadership", "priority": "medium", "due": "unknown", "evidence": "boss"},
                    ],
                    "team_todos": [],
                }
            )
        )

        self.assertEqual(
            [item["domain"] for item in parsed["project_updates"]],
            ["Anti-fraud", "Credit Risk", "Ops Risk", "General"],
        )
        self.assertEqual(parsed["my_todos"][0]["domain"], "General")
        self.assertEqual(parsed["my_todos"][0]["task"], "Prepare AI sharing")

    def test_insights_keeps_more_than_twelve_project_updates(self):
        project_updates = [
            {"domain": "Anti-fraud", "title": f"AF {index}", "summary": "", "status": "done", "evidence": ""}
            for index in range(12)
        ]
        project_updates.append({"domain": "Leadership", "title": "General awareness", "summary": "", "status": "done", "evidence": ""})

        parsed = SeaTalkDashboardService._parse_insights_response(
            json.dumps({"project_updates": project_updates, "my_todos": [], "team_todos": []})
        )

        self.assertEqual(len(parsed["project_updates"]), 13)
        self.assertEqual(parsed["project_updates"][-1]["domain"], "General")
        self.assertEqual(parsed["project_updates"][-1]["title"], "General awareness")

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
        local_call_count = 0

        def local_runner(command: list[str]):
            nonlocal local_call_count
            local_call_count += 1
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
            daily_cache_dir=Path(self.temp_dir.name) / "cache",
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
        self.assertEqual(local_call_count, 1)
        self.assertFalse(first["cache"]["hit"])
        self.assertTrue(second["cache"]["hit"])

        SeaTalkDashboardService.clear_cache()
        service_after_restart = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            daily_cache_dir=Path(self.temp_dir.name) / "cache",
            command_runner=local_runner,
        )

        third = service_after_restart.build_insights(now=now)

        self.assertEqual(codex_exec_count, 1)
        self.assertEqual(local_call_count, 1)
        self.assertTrue(third["cache"]["hit"])

    def test_build_insights_daily_cache_is_scoped_to_name_mapping_version(self):
        cache_dir = Path(self.temp_dir.name) / "cache"
        first_overrides = Path(self.temp_dir.name) / "seatalk" / "first_name_overrides.json"
        second_overrides = Path(self.temp_dir.name) / "seatalk" / "second_name_overrides.json"
        first_overrides.parent.mkdir(parents=True, exist_ok=True)
        first_overrides.write_text(json.dumps({"mappings": {"buddy-992470": "Sabrina Chan"}}), encoding="utf-8")
        second_overrides.write_text(json.dumps({"mappings": {"buddy-992470": "Ming Ming"}}), encoding="utf-8")
        local_call_count = 0

        def local_runner(command: list[str]):
            nonlocal local_call_count
            local_call_count += 1
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

        now = datetime(2026, 4, 21, 21, 0).astimezone()
        first_service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            daily_cache_dir=cache_dir,
            name_overrides_path=first_overrides,
            command_runner=local_runner,
        )
        second_service = SeaTalkDashboardService(
            owner_email="xiaodong.zheng@npt.sg",
            seatalk_app_path=str(self.app_dir),
            seatalk_data_dir=str(self.data_dir),
            codex_workspace_root=str(self.temp_dir.name),
            daily_cache_dir=cache_dir,
            name_overrides_path=second_overrides,
            command_runner=local_runner,
        )

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_codex_run,
        ):
            first_service.build_insights(now=now)
            SeaTalkDashboardService.clear_cache()
            second_service.build_insights(now=now)

        self.assertEqual(codex_exec_count, 2)
        self.assertEqual(local_call_count, 2)
        self.assertEqual(len(list(cache_dir.glob("insights_*_last_7_days_2026-04-21.json"))), 2)

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
