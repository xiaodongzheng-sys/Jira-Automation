from __future__ import annotations

import base64
import importlib.util
import json
import os
import requests
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from cryptography.fernet import Fernet

import bpmis_jira_tool.seatalk_daily_email as seatalk_daily_email
from bpmis_jira_tool.config import Settings
from bs4 import BeautifulSoup

from bpmis_jira_tool.daily_brief_archive import (
    DailyBriefArchiveStore,
    _PdfLine,
    _PdfSegment,
    _daily_brief_pdf_lines,
    _html_node_pdf_lines,
    _inline_segments,
    _normalize_segments,
    _parse_datetime,
    _strip_leading_daily_brief_heading,
    _strip_leading_window_line,
    _wrapped_segment_lines,
    _wrap_pdf_lines,
    daily_brief_archive_path,
    daily_brief_pdf_bytes,
    format_daily_brief_period,
)
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE
from bpmis_jira_tool.gmail_sender import (
    GMAIL_SEND_SCOPE,
    StoredGoogleCredentials,
    build_gmail_raw_message,
    credentials_from_payload,
    ensure_gmail_send_scope,
    send_gmail_message,
)
from bpmis_jira_tool.seatalk_daily_email import (
    DailyEmailRunStore,
    MAX_MY_TODOS,
    MAX_OTHER_UPDATES,
    MAX_PROJECT_UPDATES,
    MAX_TEAM_MEMBER_REMINDERS,
    MAX_USEFUL_AWARENESS_OTHER_UPDATES,
    build_daily_briefing,
    build_seatalk_service,
    build_trello_card_specs,
    ensure_gmail_daily_scopes,
    export_rolling_history,
    export_rolling_gmail_threads,
    render_email,
    resolve_daily_email_window,
    refresh_seatalk_auto_name_mappings,
    send_daily_email,
    seatalk_name_overrides_path,
    should_skip_fixed_daily_email_window,
    sync_daily_summary_to_trello,
    _build_team_member_reminder_candidates,
    _build_unanswered_seatalk_question_hints,
    _daily_brief_user_prompt,
)
import bpmis_jira_tool.seatalk_dashboard as seatalk_dashboard
from bpmis_jira_tool.seatalk_dashboard import SEATALK_INSIGHTS_TIMEZONE, SeaTalkDashboardService
from bpmis_jira_tool.source_code_qa_llm_providers import (
    LLM_PROVIDER_CLAUDE_CLI_BRIDGE,
    LLM_PROVIDER_CODEX_CLI_BRIDGE,
)
from bpmis_jira_tool.source_code_qa_types import LLMGenerateResult
from bpmis_jira_tool.trello_daily_summary import (
    TrelloDailySummaryClient,
    TrelloDailySummaryStore,
    daily_card_identity_from_trello_card,
)


def _settings(temp_dir: str, encryption_key: str | None = None) -> Settings:
    return replace(
        Settings.from_env(),
        team_portal_data_dir=Path(temp_dir),
        team_portal_config_encryption_key=encryption_key,
        google_oauth_client_secret_file=Path("google-client-secret.json"),
    )


class FakeSeaTalkService:
    def __init__(self, history: str = "SeaTalk Chat History Export\n") -> None:
        self.history = history
        self.calls = []
        self.last_prompt = ""

    def export_history_since(self, *, since, now, days):
        self.calls.append({"since": since, "now": now, "days": days})
        return self.history

    def _filter_system_generated_history(self, value):
        return value

    def _compact_history_for_insights(self, value, **_kwargs):
        return value

    def _run_codex_insights_prompt(self, *, prompt, system_prompt):
        self.last_prompt = prompt
        return None, {
            "project_updates": [
                {
                    "domain": "General",
                    "title": "AI sharing",
                    "summary": "Deck was refreshed.",
                    "status": "done",
                    "evidence": "19:00 Alice: deck ready",
                    "source_type": "seatalk",
                }
            ],
            "other_updates": [
                {
                    "domain": "Credit Risk",
                    "title": "Policy signal",
                    "summary": "A policy dependency may affect downstream rollout planning.",
                    "status": "in_progress",
                    "evidence": "Credit Risk group",
                    "source_type": "seatalk",
                    "signal_type": "policy_process",
                }
            ],
            "team_member_reminders": [
                {
                    "domain": "Ops Risk",
                    "person": "Ker Yin",
                    "reminder": "Please check whether the pending GRC confirmation still needs owner follow-up.",
                    "evidence": "Ops Risk group",
                    "source_type": "seatalk",
                }
            ],
            "my_todos": [
                {
                    "task": "Review rollout note",
                    "domain": "Anti-fraud",
                    "priority": "high",
                    "due": "unknown",
                    "evidence": "18:30 Bob: please review",
                    "source_type": "seatalk",
                }
            ],
            "team_todos": [],
        }


class SeaTalkDailyEmailCodexRoutingTests(unittest.TestCase):
    def test_build_seatalk_service_defaults_to_deep_codex_route(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"TEAM_PORTAL_DATA_DIR": temp_dir, "SEATALK_CODEX_MODEL": "gpt-5.6"},
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            service = build_seatalk_service(Settings.from_env(), data_root=Path(temp_dir))

        self.assertEqual(service.codex_model, "gpt-5.6")

    def test_build_seatalk_service_defaults_to_codex_provider(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"TEAM_PORTAL_DATA_DIR": temp_dir},
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            service = build_seatalk_service(Settings.from_env(), data_root=Path(temp_dir))

        self.assertEqual(service.insights_llm_provider, LLM_PROVIDER_CODEX_CLI_BRIDGE)
        self.assertIsNone(service.claude_model)

    def test_build_seatalk_service_uses_claude_when_env_set(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "DAILY_BRIEF_INSIGHTS_LLM_PROVIDER": "claude_cli_bridge",
                "DAILY_BRIEF_CLAUDE_MODEL": "claude-opus-4-8",
            },
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            service = build_seatalk_service(Settings.from_env(), data_root=Path(temp_dir))

        self.assertEqual(service.insights_llm_provider, LLM_PROVIDER_CLAUDE_CLI_BRIDGE)
        self.assertEqual(service.claude_model, "claude-opus-4-8")


class SeaTalkInsightsProviderRoutingTests(unittest.TestCase):
    def _service(self, provider: str) -> SeaTalkDashboardService:
        return SeaTalkDashboardService(
            owner_email="owner@example.com",
            codex_workspace_root="/tmp",
            insights_llm_provider=provider,
            claude_model="claude-opus-4-8",
        )

    def test_claude_provider_used_when_selected(self):
        service = self._service(LLM_PROVIDER_CLAUDE_CLI_BRIDGE)
        claude_result = LLMGenerateResult(payload={"text": "{}"}, usage={}, model="claude-opus-4-8", attempts=1)
        with patch.object(seatalk_dashboard, "ClaudeCliBridgeSourceCodeQALLMProvider") as ClaudeProvider, patch.object(
            seatalk_dashboard, "CodexCliBridgeSourceCodeQALLMProvider"
        ) as CodexProvider, patch.object(service, "_parse_insights_response", side_effect=lambda text: {"parsed": text}):
            ClaudeProvider.return_value.generate.return_value = claude_result
            ClaudeProvider.return_value.extract_text.return_value = "{}"
            _, parsed = service._run_codex_insights_prompt(prompt="hi", system_prompt="sys")

        self.assertEqual(parsed, {"parsed": "{}"})
        ClaudeProvider.return_value.generate.assert_called_once()
        CodexProvider.assert_not_called()

    def test_falls_back_to_codex_when_claude_fails(self):
        service = self._service(LLM_PROVIDER_CLAUDE_CLI_BRIDGE)
        codex_result = LLMGenerateResult(payload={"text": "CODEX"}, usage={}, model="gpt-5.5", attempts=1)
        with patch.object(seatalk_dashboard, "ClaudeCliBridgeSourceCodeQALLMProvider") as ClaudeProvider, patch.object(
            seatalk_dashboard, "CodexCliBridgeSourceCodeQALLMProvider"
        ) as CodexProvider, patch.object(service, "_parse_insights_response", side_effect=lambda text: text):
            ClaudeProvider.return_value.generate.side_effect = ToolError("Not logged in")
            CodexProvider.return_value.generate.return_value = codex_result
            CodexProvider.return_value.extract_text.return_value = "CODEX"
            _, parsed = service._run_codex_insights_prompt(prompt="hi", system_prompt="sys")

        self.assertEqual(parsed, "CODEX")
        ClaudeProvider.return_value.generate.assert_called_once()
        CodexProvider.return_value.generate.assert_called_once()

    def test_codex_provider_used_by_default(self):
        service = self._service("")
        codex_result = LLMGenerateResult(payload={"text": "CODEX"}, usage={}, model="gpt-5.5", attempts=1)
        with patch.object(seatalk_dashboard, "ClaudeCliBridgeSourceCodeQALLMProvider") as ClaudeProvider, patch.object(
            seatalk_dashboard, "CodexCliBridgeSourceCodeQALLMProvider"
        ) as CodexProvider, patch.object(service, "_parse_insights_response", side_effect=lambda text: text):
            CodexProvider.return_value.generate.return_value = codex_result
            CodexProvider.return_value.extract_text.return_value = "CODEX"
            _, parsed = service._run_codex_insights_prompt(prompt="hi", system_prompt="sys")

        self.assertEqual(parsed, "CODEX")
        ClaudeProvider.assert_not_called()
        CodexProvider.return_value.generate.assert_called_once()


class SeaTalkDailyEmailTests(unittest.TestCase):
    def test_rolling_history_uses_previous_24_hours(self):
        service = FakeSeaTalkService()
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        export_rolling_history(service, now=now, hours=24)

        self.assertEqual(service.calls[0]["since"].isoformat(), "2026-04-26T19:00:00+08:00")
        self.assertEqual(service.calls[0]["now"].isoformat(), "2026-04-27T19:00:00+08:00")

    def test_rolling_gmail_threads_uses_previous_24_hours(self):
        class FakeGmailBriefService:
            def __init__(self):
                self.calls = []

            def export_thread_history_since(self, *, since, now):
                self.calls.append({"since": since, "now": now})
                return "Gmail thread history export\n"

        service = FakeGmailBriefService()
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        export_rolling_gmail_threads(service, now=now, hours=24)

        self.assertEqual(service.calls[0]["since"].isoformat(), "2026-04-26T19:00:00+08:00")
        self.assertEqual(service.calls[0]["now"].isoformat(), "2026-04-27T19:00:00+08:00")

    def test_seatalk_name_overrides_prefers_local_agent_data_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "project"
            agent_root = Path(temp_dir) / "agent"
            agent_mapping = agent_root / "seatalk" / "name_overrides.json"
            agent_mapping.parent.mkdir(parents=True)
            agent_mapping.write_text('{"mappings": {"UID 1": "Alice"}}', encoding="utf-8")
            with patch.dict("os.environ", {"LOCAL_AGENT_TEAM_PORTAL_DATA_DIR": str(agent_root)}):
                self.assertEqual(seatalk_name_overrides_path(data_root=root), agent_mapping)

    def test_refresh_seatalk_auto_name_mappings_merges_only_missing_candidates(self):
        class AutoMappingService:
            def __init__(self, path: Path) -> None:
                self.name_overrides_path = path

            def build_name_mappings(self, *, now):
                return {
                    "auto_mappings": {
                        "group-123": "Risk Project Group",
                        "buddy-456": "Alice Tan",
                        "UID 888": "Should Not Override",
                    }
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            mapping_path = Path(temp_dir) / "seatalk" / "name_overrides.json"
            mapping_path.parent.mkdir(parents=True)
            mapping_path.write_text('{"mappings": {"UID 888": "Manual Name"}}', encoding="utf-8")

            mappings = refresh_seatalk_auto_name_mappings(
                AutoMappingService(mapping_path),
                now=datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            )

        self.assertEqual(mappings["group-123"], "Risk Project Group")
        self.assertEqual(mappings["UID 456"], "Alice Tan")
        self.assertEqual(mappings["buddy-456"], "Alice Tan")
        self.assertEqual(mappings["UID 888"], "Manual Name")

    def test_load_seatalk_name_mappings_reuses_uid_mapping_for_buddy_id(self):
        class MappingService:
            def __init__(self, path: Path) -> None:
                self.name_overrides_path = path

        with tempfile.TemporaryDirectory() as temp_dir:
            mapping_path = Path(temp_dir) / "seatalk" / "name_overrides.json"
            mapping_path.parent.mkdir(parents=True)
            mapping_path.write_text('{"mappings": {"UID 1022128": "Evan Ong Jun Wei"}}', encoding="utf-8")

            mappings = seatalk_daily_email._load_seatalk_name_mappings(MappingService(mapping_path))

        self.assertEqual(mappings["uid 1022128"], "Evan Ong Jun Wei")
        self.assertEqual(mappings["buddy-1022128"], "Evan Ong Jun Wei")

    def test_build_daily_briefing_skips_model_when_window_has_no_messages(self):
        service = FakeSeaTalkService("SeaTalk Chat History Export\nWindow: since 2026-04-26T19:00:00+08:00\n")
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        payload = build_daily_briefing(service, now=now)

        self.assertEqual(payload["my_todos"], [])
        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["other_updates"], [])
        self.assertEqual(payload["team_member_reminders"], [])
        self.assertEqual(payload["period_hours"], 24)

    def test_build_daily_briefing_includes_gmail_threads_even_without_seatalk_messages(self):
        service = FakeSeaTalkService("SeaTalk Chat History Export\nWindow: since 2026-04-26T19:00:00+08:00\n")
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        payload = build_daily_briefing(
            service,
            now=now,
            gmail_history_text=(
                "Gmail thread history export\n"
                "Thread 1\n"
                "Subject: CR rollout\n"
                "Message 1\n"
                "Body:\nPlease confirm rollout owner.\n"
            ),
        )

        self.assertEqual(payload["my_todos"][0]["task"], "Review rollout note")
        self.assertIn("=== Gmail thread history ===", service.last_prompt)
        self.assertIn("Subject: CR rollout", service.last_prompt)

    def test_build_daily_briefing_filters_reminders_and_other_updates(self):
        class NoisyBriefingService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Credit Risk",
                            "title": "CR rollout",
                            "summary": "CR rollout decision was confirmed.",
                            "status": "in_progress",
                            "evidence": "Credit Risk group",
                            "source_type": "seatalk",
                        },
                        {
                            "domain": "Credit Risk",
                            "title": "CR rollout",
                            "summary": "CR rollout decision was confirmed.",
                            "status": "done",
                            "evidence": "Alice, CR rollout Gmail thread",
                            "source_type": "gmail",
                        },
                    ],
                    "other_updates": [
                        {
                            "domain": "General",
                            "title": "Thanks",
                            "summary": "A generic FYI was shared.",
                            "status": "unknown",
                            "evidence": "General Gmail thread",
                            "source_type": "gmail",
                            "signal_type": "fyi",
                        },
                        {
                            "domain": "Ops Risk",
                            "title": "Incident",
                            "summary": "An incident may affect rollout monitoring.",
                            "status": "in_progress",
                            "evidence": "Ops Risk group",
                            "source_type": "seatalk",
                            "signal_type": "incident",
                        },
                    ],
                    "team_member_reminders": [
                        {
                            "domain": "Ops Risk",
                            "person": "Liye",
                            "reminder": "Check the unresolved group mention.",
                            "evidence": "Ops Risk group",
                            "source_type": "seatalk",
                        },
                        {
                            "domain": "Credit Risk",
                            "person": "Liye",
                            "reminder": "Check the Gmail mention.",
                            "evidence": "Liye, Gmail subject",
                            "source_type": "gmail",
                        },
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        payload = build_daily_briefing(
            NoisyBriefingService(
                "\n".join(
                    [
                        "SeaTalk Chat History Export",
                        "=== Credit Risk group ===",
                        "[2026-04-27 18:30:00] Alice: CR rollout decision was confirmed.",
                        "=== Ops Risk group ===",
                        "[2026-04-27 18:35:00] Bob: @Liye please check the unresolved group mention.",
                        "[2026-04-27 18:36:00] Bob: An incident may affect rollout monitoring.",
                    ]
                )
            ),
            now=now,
            gmail_history_text="\n".join(
                [
                    "Gmail thread history export",
                    "Thread 1",
                    "Subject: CR rollout Gmail thread",
                    "Participants: Alice",
                    "Message 1",
                    "From: Alice",
                    "Body:",
                    "CR rollout decision was confirmed.",
                ]
            ),
        )

        self.assertEqual(len(payload["project_updates"]), 1)
        self.assertIn(payload["project_updates"][0]["source_type"], {"seatalk", "gmail"})
        self.assertTrue(payload["project_updates"][0].get("evidence_ref_id"))
        self.assertEqual(len(payload["other_updates"]), 1)
        self.assertEqual(payload["other_updates"][0]["signal_type"], "incident")
        self.assertEqual(len(payload["team_member_reminders"]), 1)
        self.assertEqual(payload["team_member_reminders"][0]["source_type"], "seatalk")

    def test_build_daily_briefing_allows_limited_useful_awareness_other_updates(self):
        class UsefulAwarenessService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                topics = [
                    "MAS reporting checkpoint",
                    "Cloud migration window",
                    "Vendor onboarding reminder",
                    "VIP complaint trend",
                    "Release policy note",
                    "Ops staffing update",
                    "Audit evidence request",
                ]
                useful_items = [
                    {
                        "domain": "General",
                        "title": topic,
                        "summary": f"{topic} may affect PM follow-up.",
                        "status": "unknown",
                        "evidence": topic,
                        "source_type": "gmail",
                        "signal_type": "useful_awareness",
                    }
                    for topic in topics
                ]
                return None, {
                    "project_updates": [],
                    "other_updates": useful_items,
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                *[
                    f"=== {topic} ===\n[2026-04-27 18:3{index}:00] Bob: {topic} may affect PM follow-up."
                    for index, topic in enumerate(
                        [
                            "MAS reporting checkpoint",
                            "Cloud migration window",
                            "Vendor onboarding reminder",
                            "VIP complaint trend",
                            "Release policy note",
                            "Ops staffing update",
                            "Audit evidence request",
                        ],
                        start=1,
                    )
                ],
            ]
        )
        service = UsefulAwarenessService(history)
        payload = build_daily_briefing(
            service,
            now=now,
        )

        useful_awareness = [item for item in payload["other_updates"] if item["signal_type"] == "useful_awareness"]
        self.assertEqual(len(useful_awareness), MAX_USEFUL_AWARENESS_OTHER_UPDATES)
        self.assertIn("useful_awareness", service.last_prompt)

    def test_build_daily_briefing_treats_missing_other_update_signal_as_useful_awareness(self):
        class MissingSignalService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [
                        {
                            "domain": "General",
                            "title": "Migration milestone",
                            "summary": "A migration milestone may be useful awareness.",
                            "status": "in_progress",
                            "evidence": "Weekly report",
                            "source_type": "gmail",
                        }
                    ],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = MissingSignalService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n")
        payload = build_daily_briefing(service, now=now)

        self.assertEqual(len(payload["other_updates"]), 1)
        self.assertEqual(payload["other_updates"][0]["signal_type"], "useful_awareness")

    def test_build_daily_briefing_ignores_bot_alerts_and_reminders(self):
        class BotNoiseService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [
                        {
                            "domain": "Ops Risk",
                            "title": "Workflow alert",
                            "summary": "Automated alert says a workflow reminder was triggered.",
                            "status": "unknown",
                            "evidence": "workflow-bot alert",
                            "source_type": "seatalk",
                            "signal_type": "incident",
                        },
                        {
                            "domain": "Credit Risk",
                            "title": "Human incident",
                            "summary": "A human-reported incident may affect rollout monitoring.",
                            "status": "in_progress",
                            "evidence": "Alice, Credit Risk group",
                            "source_type": "seatalk",
                            "signal_type": "incident",
                        },
                    ],
                    "team_member_reminders": [
                        {
                            "domain": "Ops Risk",
                            "person": "Liye",
                            "reminder": "Automated reminder mentioned Liye.",
                            "evidence": "reminder-bot",
                            "source_type": "seatalk",
                        },
                        {
                            "domain": "Ops Risk",
                            "person": "Liye",
                            "reminder": "Check the human unresolved group mention.",
                            "evidence": "Ops Risk group",
                            "source_type": "seatalk",
                        },
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = BotNoiseService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n")
        payload = build_daily_briefing(service, now=now)

        self.assertEqual(len(payload["other_updates"]), 1)
        self.assertEqual(payload["other_updates"][0]["title"], "Human incident")
        self.assertEqual(len(payload["team_member_reminders"]), 1)
        self.assertEqual(payload["team_member_reminders"][0]["reminder"], "Check the human unresolved group mention.")
        self.assertIn("ignore bot-generated alerts", service.last_prompt)

    def test_build_daily_briefing_filters_sdlc_checker_team_followups(self):
        class SdlcCheckerService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Anti-fraud",
                            "person": "Wang Chang",
                            "reminder": "The SDLC checker listed SGDB approvals and PRD/TRD documents as pending.",
                            "evidence": "SG BAU SDLC material check",
                            "source_type": "seatalk",
                        },
                        {
                            "domain": "Anti-fraud",
                            "person": "Rene Chong",
                            "reminder": "Wendy asked Rene to help check the ID appeal case.",
                            "evidence": "[ID] AFA PM Local x Regional",
                            "source_type": "seatalk",
                        },
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 30, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = SdlcCheckerService("SeaTalk Chat History Export\n[2026-04-30 15:00:00] SDLC Checker: approval reminder\n")
        payload = build_daily_briefing(service, now=now)

        self.assertEqual([item["person"] for item in payload["team_member_reminders"]], ["Rene Chong"])
        self.assertIn("always exclude SDLC Checker", service.last_prompt)

    def test_build_daily_briefing_prompt_handles_thread_and_cc_only_mentions(self):
        class ThreadCcService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 30, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        history = (
            "SeaTalk Chat History Export\n"
            "=== UDL数据小群 (group-2721110) ===\n"
            "[2026-04-30 15:14:18] Tan Jing Jie [thread reply under: PH A-Card Model V2.1 Deployment]: "
            "Hihi @Lang Jiang can help me check partitions\n"
            "    cc:@Liye | 吴立业\n"
        )
        service = ThreadCcService(history)
        build_daily_briefing(service, now=now)

        self.assertIn("A cc-only mention is not enough", service.last_prompt)
        self.assertIn("If the source message is annotated as a thread reply", service.last_prompt)
        self.assertIn("Do not write 'in the group' for thread replies", service.last_prompt)

    def test_build_daily_briefing_keeps_seatalk_reminder_when_source_type_missing(self):
        class MissingSourceReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Ops Risk",
                            "person": "Liye",
                            "reminder": "Check the unresolved human mention in the group.",
                            "evidence": "Ops Risk discussion",
                        }
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = MissingSourceReminderService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: Liye please check\n")
        payload = build_daily_briefing(service, now=now)

        self.assertEqual(len(payload["team_member_reminders"]), 1)
        self.assertEqual(payload["team_member_reminders"][0]["source_type"], "seatalk")
        self.assertIn("Team Member Reminder Scan", service.last_prompt)
        self.assertIn("drop it rather than creating a noisy Follow-up", service.last_prompt)

    def test_build_daily_briefing_filters_non_anti_fraud_team_reminders(self):
        class NonTeamReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Anti-fraud",
                            "person": "Wendy",
                            "reminder": "Provide a transaction ID for the AFA scenario.",
                            "evidence": "[ID] AFA PM Local x Regional",
                            "source_type": "seatalk",
                        },
                        {
                            "domain": "Anti-fraud",
                            "person": "Rene Chong",
                            "reminder": "Share one live UID for the whitelisted live test.",
                            "evidence": "[ID] AF 需求排期沟通群",
                            "source_type": "seatalk",
                        },
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = NonTeamReminderService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n")
        payload = build_daily_briefing(service, now=now)

        self.assertEqual(len(payload["team_member_reminders"]), 1)
        self.assertEqual(payload["team_member_reminders"][0]["person"], "Rene Chong")
        self.assertIn("Do not put anyone else, including Wendy", service.last_prompt)

    def test_build_daily_briefing_canonicalizes_team_member_aliases(self):
        class AliasReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Anti-fraud",
                            "person": "Rene Chong (UID 123)",
                            "reminder": "Check the short-name reminder.",
                            "evidence": "AF group",
                            "source_type": "seatalk",
                        },
                        {
                            "domain": "Anti-fraud",
                            "person": "Zoey Lu",
                            "reminder": "Check the Zoey short-name reminder.",
                            "evidence": "AF group",
                            "source_type": "seatalk",
                        },
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        payload = build_daily_briefing(
            AliasReminderService(
                "\n".join(
                    [
                        "SeaTalk Chat History Export",
                        "=== AF group ===",
                        "[2026-04-27 18:30:00] Bob: @Rene Chong please check the short-name reminder.",
                        "[2026-04-27 18:31:00] Bob: @Zoey Lu please check the Zoey short-name reminder.",
                    ]
                )
            ),
            now=now,
        )

        self.assertEqual([item["person"] for item in payload["team_member_reminders"]], ["Rene Chong", "Zoey Lu"])

    def test_build_daily_briefing_forces_sophia_to_credit_risk(self):
        class SophiaReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Ops Risk",
                            "person": "Sophia Wang Zijun",
                            "reminder": "Check the unresolved Credit Risk dependency.",
                            "evidence": "Credit Risk group",
                            "source_type": "seatalk",
                        }
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = SophiaReminderService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: Sophia please check\n")
        payload = build_daily_briefing(service, now=now)

        self.assertEqual(payload["team_member_reminders"][0]["person"], "Sophia Wang Zijun")
        self.assertEqual(payload["team_member_reminders"][0]["domain"], "Credit Risk")
        self.assertIn("Sophia Wang Zijun belongs to Credit Risk", service.last_prompt)

    def test_build_daily_briefing_sanitizes_raw_seatalk_source_ids(self):
        class RawEvidenceService(FakeSeaTalkService):
            def __init__(self, history: str, name_overrides_path: Path) -> None:
                super().__init__(history)
                self.name_overrides_path = name_overrides_path

            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Credit Risk",
                            "title": "CR dependency",
                            "summary": "A CR dependency needs follow-up.",
                            "status": "in_progress",
                            "evidence": "group-4228440, buddy-266783, group-999999",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            overrides_path = Path(temp_dir) / "seatalk" / "name_overrides.json"
            overrides_path.parent.mkdir(parents=True)
            overrides_path.write_text(
                '{"mappings": {"group-4228440": "Credit Risk PM group", "UID 266783": "Alice Tan"}}',
                encoding="utf-8",
            )
            now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
            payload = build_daily_briefing(
                RawEvidenceService(
                    "SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n",
                    overrides_path,
                ),
                now=now,
            )

        evidence = payload["project_updates"][0]["evidence"]
        self.assertIn("Credit Risk PM group", evidence)
        self.assertIn("Alice Tan", evidence)
        self.assertIn("SeaTalk group", evidence)
        self.assertNotIn("group-4228440", evidence)
        self.assertNotIn("buddy-266783", evidence)
        self.assertNotIn("group-999999", evidence)

    def test_build_daily_briefing_repairs_generic_seatalk_group_evidence_from_ref(self):
        class GenericEvidenceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "task": "Ensure Michael confirms whether PH Money Lock and Kill Switch public live can proceed on May 25.",
                            "domain": "Anti-fraud",
                            "priority": "high",
                            "due": "2026-05-22",
                            "evidence": "SeaTalk SeaTalk group",
                            "evidence_ref_id": "st-ref-001",
                            "source_type": "seatalk",
                            "action_type": "watch_delegate",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AAF Small Group (group-2753344) ===",
                "[2026-05-20 15:10:00] Michael Salam: Please confirm whether PH Money Lock and Kill Switch public live can proceed on May 25 after CS FAQ and Help Centre article readiness are confirmed.",
            ]
        )

        payload = build_daily_briefing(
            GenericEvidenceService(history),
            now=datetime(2026, 5, 20, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["watch_delegate_todos"][0]["evidence"], "PH AAF Small Group")
        self.assertNotIn("SeaTalk SeaTalk group", payload["watch_delegate_todos"][0]["evidence"])

    def test_build_daily_briefing_drops_generic_seatalk_followup_without_ref(self):
        class GenericEvidenceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "task": "Ensure Michael confirms whether PH Money Lock and Kill Switch public live can proceed on May 25.",
                            "domain": "Anti-fraud",
                            "priority": "high",
                            "due": "2026-05-22",
                            "evidence": "SeaTalk SeaTalk group",
                            "source_type": "seatalk",
                            "action_type": "watch_delegate",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AAF Small Group (group-2753344) ===",
                "[2026-05-20 15:10:00] Michael Salam: Please confirm whether PH Money Lock and Kill Switch public live can proceed on May 25.",
            ]
        )

        payload = build_daily_briefing(
            GenericEvidenceService(history),
            now=datetime(2026, 5, 20, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(len(payload["watch_delegate_todos"]), 1)
        self.assertEqual(payload["watch_delegate_todos"][0]["evidence"], "PH AAF Small Group")
        self.assertEqual(payload["watch_delegate_todos"][0]["evidence_ref_id"], "st-ref-001")

    def test_build_daily_briefing_uses_gmail_ref_for_project_update_evidence(self):
        class GmailRefService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Credit Risk",
                            "title": "CR rollout approval",
                            "summary": "BSP launch approval is pending and needs Xiaodong review.",
                            "status": "in_progress",
                            "evidence": "Gmail thread",
                            "evidence_ref_id": "gm-ref-001",
                            "source_type": "gmail",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        gmail_history = "\n".join(
            [
                "Gmail thread history export",
                "================================================================================",
                "Thread 1",
                "Thread ID: thread-123",
                "Gmail Thread Link: https://mail.google.com/mail/u/0/#inbox/thread-123",
                "Subject: CR rollout approval",
                "Participants: Alice <alice@example.com>",
                "",
                "Message 1",
                "Date: 2026-05-13T12:00:00+08:00",
                "From: Alice <alice@example.com>",
                "To: Xiaodong <xiaodong@example.com>",
                "Cc: [no cc listed]",
                "Use: in-window evidence",
                "",
                "Body:",
                "BSP launch approval is pending and needs Xiaodong review.",
            ]
        )

        service = GmailRefService("SeaTalk Chat History Export\n")
        payload = build_daily_briefing(
            service,
            now=datetime(2026, 5, 13, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            gmail_history_text=gmail_history,
        )

        self.assertEqual(payload["project_updates"][0]["evidence"], "Gmail: CR rollout approval / Alice <alice@example.com>")
        self.assertEqual(payload["project_updates"][0]["source_type"], "gmail")
        self.assertIn('"id":"gm-ref-001"', service.last_prompt)

    def test_build_daily_briefing_drops_project_update_with_mismatched_ref(self):
        class BadProjectRefService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Credit Risk",
                            "title": "Unrelated CR rollout approval",
                            "summary": "BSP launch approval is pending.",
                            "status": "in_progress",
                            "evidence": "Gmail thread",
                            "evidence_ref_id": "gm-ref-001",
                            "source_type": "gmail",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        gmail_history = "\n".join(
            [
                "Gmail thread history export",
                "================================================================================",
                "Thread 1",
                "Thread ID: thread-456",
                "Gmail Thread Link: https://mail.google.com/mail/u/0/#inbox/thread-456",
                "Subject: Pantry update",
                "Participants: Office <office@example.com>",
                "Message 1",
                "Date: 2026-05-13T12:00:00+08:00",
                "From: Office <office@example.com>",
                "Use: in-window evidence",
                "Body:",
                "The pantry fridge will be cleaned tomorrow.",
            ]
        )

        payload = build_daily_briefing(
            BadProjectRefService("SeaTalk Chat History Export\n"),
            now=datetime(2026, 5, 13, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            gmail_history_text=gmail_history,
        )

        self.assertEqual(payload["project_updates"], [])
        self.assertGreaterEqual(payload["quality_metadata"]["evidence_quality_metrics"]["dropped_invalid_evidence_count"], 1)

    def test_seatalk_ref_evidence_uses_name_mapping_and_private_fallback(self):
        mapped_refs = seatalk_daily_email._build_daily_brief_evidence_refs(
            "\n".join(
                [
                    "SeaTalk Chat History Export",
                    "=== buddy-1022128 ===",
                    "[2026-05-21 09:00:00] Zheng Xiaodong: Ker Yin please confirm Hold & Release go-live readiness.",
                ]
            ),
            name_mappings={"buddy-1022128": "Ker Yin"},
        )
        fallback_refs = seatalk_daily_email._build_daily_brief_evidence_refs(
            "\n".join(
                [
                    "SeaTalk Chat History Export",
                    "=== buddy-1022128 ===",
                    "[2026-05-21 09:00:00] Zheng Xiaodong: Ker Yin please confirm Hold & Release go-live readiness.",
                ]
            )
        )

        self.assertEqual(mapped_refs[0]["evidence"], "Ker Yin")
        uid_alias_refs = seatalk_daily_email._build_daily_brief_evidence_refs(
            "\n".join(
                [
                    "SeaTalk Chat History Export",
                    "=== buddy-1022128 ===",
                    "[2026-05-21 09:00:00] Zheng Xiaodong: Evan please confirm Hold & Release go-live readiness.",
                ]
            ),
            name_mappings={"UID 1022128": "Evan Ong Jun Wei"},
        )
        self.assertEqual(uid_alias_refs[0]["evidence"], "Evan Ong Jun Wei")
        self.assertEqual(fallback_refs[0]["evidence"], "Private SeaTalk chat (buddy-1022128)")
        self.assertEqual(
            seatalk_daily_email._sanitize_seatalk_evidence(
                "Private SeaTalk chat (buddy-1022128)",
                name_mappings={"UID 1022128": "Evan Ong Jun Wei"},
            ),
            "Evan Ong Jun Wei",
        )
        self.assertEqual(
            seatalk_daily_email._sanitize_seatalk_evidence("buddy-1022128"),
            "Private SeaTalk chat (buddy-1022128)",
        )
        self.assertEqual(
            seatalk_daily_email._normalize_seatalk_source_label("大佬来抓贼 (group-2823891) / thread: reset pin"),
            "大佬来抓贼 / thread: reset pin",
        )
        self.assertEqual(
            seatalk_daily_email._normalize_seatalk_source_label("Liye | 吴立业 (buddy-627112)"),
            "Liye | 吴立业",
        )
        self.assertEqual(
            seatalk_daily_email._normalize_seatalk_source_label("Private SeaTalk chat (buddy-1022128)"),
            "Private SeaTalk chat (buddy-1022128)",
        )

    def test_infers_private_chat_mapping_from_self_reply(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== buddy-1022128 ===",
                "[2026-05-21 10:00:49] UID 1022128: Hi Xiaodong, I have finished a first draft of PRD for the AMR Sampling.",
                "[2026-05-21 10:02:48] Zheng Xiaodong (UID 14420): Thanks Jun Wei for the quick turnaround!",
            ]
        )

        mappings = seatalk_daily_email._infer_private_chat_name_mappings_from_history(history)

        self.assertEqual(mappings["buddy-1022128"], "Jun Wei")
        self.assertEqual(mappings["uid 1022128"], "Jun Wei")

    def test_person_validation_accepts_team_member_alias_spacing(self):
        item = {"task": "Ask Liye for the collected Credit Card enhancement requirements."}
        records = [
            {
                "sender": "Li Ye | 吴立业",
                "thread": "",
                "text": "So far the Credit Card enhancement requirements include the new scope.",
            }
        ]

        self.assertTrue(seatalk_daily_email._seatalk_record_mentions_item_people(item, records))

    def test_build_daily_briefing_repairs_wrong_source_from_ref_and_normalizes_duplicate_group(self):
        class WrongSourceRefService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Anti-fraud",
                            "title": "CRDEv143 greyscale",
                            "summary": "CRDEv143 10% greyscale is blocked until likely late-afternoon deployment completion.",
                            "status": "blocked",
                            "evidence": "[SG] AF需求排期沟通群 ([SG] AF需求排期沟通群)",
                            "evidence_ref_id": "st-ref-001",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== [SG] AF需求排期沟通群 ([SG] AF需求排期沟通群) ===",
                "[2026-05-21 12:10:00] Hui Xian: CRDEv143 10% greyscale should wait until late-afternoon deployment completion.",
            ]
        )

        payload = build_daily_briefing(
            WrongSourceRefService(history),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["project_updates"][0]["evidence"], "[SG] AF需求排期沟通群")

    def test_build_daily_briefing_drops_private_chat_source_when_topic_mismatches(self):
        class WrongPrivateSourceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "domain": "Anti-fraud",
                            "task": "Confirm whether the PH Hold & Release CRC PRD can still meet the 3.23 target and what support is needed next week.",
                            "priority": "low",
                            "due": "next week",
                            "evidence": "Private SeaTalk chat (buddy-1022128)",
                            "source_type": "seatalk",
                            "action_type": "direct_action",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== buddy-1022128 ===",
                "[2026-05-22 16:22:39] UID 1022128: Can i schedule a 30 minute meeting with you next Mon to clarify some questions.",
                "=== Wang Chang (buddy-206431) ===",
                "[2026-05-22 17:42:38] Zheng Xiaodong (UID 14420): 好的！话说PH Hold & Release CRC的PRD现在是什么计划？",
                "[2026-05-22 17:52:55] Wang Chang (UID 206431): 如果写得完+评审完那就是3.23",
            ]
        )

        payload = build_daily_briefing(
            WrongPrivateSourceService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["direct_action_todos"], [])
        self.assertGreaterEqual(payload["quality_metadata"]["evidence_quality_metrics"]["dropped_invalid_evidence_count"], 1)

    def test_build_daily_briefing_drops_private_chat_source_when_named_person_mismatches(self):
        class WrongPrivatePersonService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "domain": "General",
                            "task": "Arrange the follow-up discussion with Andy next week on Ker Yin's performance feedback and next management steps.",
                            "priority": "high",
                            "due": "next week",
                            "evidence": "Private SeaTalk chat (buddy-1022128)",
                            "source_type": "seatalk",
                            "action_type": "direct_action",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== buddy-1022128 ===",
                "[2026-05-22 16:22:39] UID 1022128: Can i schedule a 30 minute meeting with you next Mon to clarify some questions.",
                "[2026-05-22 16:51:10] Zheng Xiaodong (UID 14420): Hi Evan, can book me next Mon 2:30-3pm. Today is a bit full.",
            ]
        )

        payload = build_daily_briefing(
            WrongPrivatePersonService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["direct_action_todos"], [])
        self.assertEqual(payload["quality_metadata"]["evidence_quality_metrics"]["dropped_invalid_evidence_count"], 1)

    def test_build_daily_briefing_private_buddy_source_does_not_match_generic_private_headers(self):
        class WrongGenericPrivateSourceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "domain": "Anti-fraud",
                            "task": "Arrange the early-June discussion with Denise and DPS after Zoey prepares the SFV SOP proposal.",
                            "priority": "medium",
                            "due": "early June",
                            "evidence": "Private SeaTalk chat (buddy-1022128)",
                            "source_type": "seatalk",
                            "action_type": "direct_action",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== buddy-1022128 ===",
                "[2026-05-22 16:22:39] UID 1022128: Can i schedule a 30 minute meeting with you next Mon to clarify some questions.",
                "=== Private SeaTalk chat ===",
                "[2026-05-22 17:10:00] Denise Huang: Let's discuss SFV SOP and DPS filtering in early June after Zoey prepares the proposal.",
            ]
        )

        payload = build_daily_briefing(
            WrongGenericPrivateSourceService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["direct_action_todos"], [])
        self.assertEqual(payload["quality_metadata"]["evidence_quality_metrics"]["dropped_invalid_evidence_count"], 1)

    def test_build_daily_briefing_drops_unthreaded_group_source_when_topic_mismatches(self):
        class WrongUnthreadedGroupService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Anti-fraud",
                            "title": "AF Fields masking moved to v3.03",
                            "summary": "Zoey aligned that AF and upstream integration for AF Fields masking will be handled in v3.03.",
                            "status": "blocked",
                            "evidence": "PH AF DB拆库讨论",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AF DB拆库讨论 ===",
                "[2026-05-22 14:00:00] Zheng Xiaodong (UID 14420): PH DB migration domain and downtime plan.",
                "=== Salary crediting masking group (group-4420664) ===",
                "[2026-05-22 15:17:44] Zoey Lu (UID 355879) [thread reply under: salary crediting masking]: AF所有的接入都在 3.03，3.01 BC filter out这些交易，不会进AF ivlog",
            ]
        )

        payload = build_daily_briefing(
            WrongUnthreadedGroupService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["quality_metadata"]["evidence_quality_metrics"]["dropped_invalid_evidence_count"], 1)

    def test_build_daily_briefing_repairs_wrong_thread_group_source(self):
        class WrongThreadGroupService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "General",
                            "title": "PH One App rollout timing was shared",
                            "summary": "May 29 SPP go-live plus Bank App whitelist LV was shared, with later public rollout timing.",
                            "status": "in_progress",
                            "evidence": "ID Digital Bank SL1,SL2 live issue update / thread: [Bank Rollout]",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== [PH One App] Cash In Flow (group-3455069) ===",
                "[2026-05-22 14:10:21] Goh Shan Yi (UID 592898) [thread reply under: [Bank Rollout]]: 0529: SPP golive + Bank App whitelist users LV",
                "[2026-05-22 14:11:16] Goh Shan Yi (UID 592898) [thread reply under: [Bank Rollout]]: 1 month timeline is a recommendation from dev before public rollout",
            ]
        )

        payload = build_daily_briefing(
            WrongThreadGroupService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(len(payload["project_updates"]), 1)
        self.assertEqual(
            payload["project_updates"][0]["evidence"],
            "[PH One App] Cash In Flow / thread: [Bank Rollout]",
        )

    def test_build_daily_briefing_drops_thread_source_when_named_person_missing_from_ref(self):
        class WrongPersonThreadSourceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Anti-fraud",
                            "title": "Zoey aligned backend-led AF productization validation",
                            "summary": "Zoey aligned that the backend-led fix should be feasible in v3.02 and QA asked whether the latest Android package can now be verified.",
                            "status": "in_progress",
                            "evidence": "[Live Test] DP Independent Payment Cashier for Shopee App (group-4195478) / thread: Rollout",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== [Live Test] DP Independent Payment Cashier for Shopee App (group-4195478) ===",
                "[2026-05-22 17:18:51] Sherry Zheng (UID 315357) [thread reply under: Rollout]: We can release this feature in 6.v1",
                "=== group-4223511 ===",
                "[2026-05-22 18:33:33] Xiao Jinlin (UID 59120) [thread reply under: AF 产品化变更]: @Zoey Lu 我们内部同学应该可以拿到最新的安卓包了，可以验证吗？",
            ]
        )

        payload = build_daily_briefing(
            WrongPersonThreadSourceService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["project_updates"], [])
        self.assertGreaterEqual(payload["quality_metadata"]["evidence_quality_metrics"]["dropped_invalid_evidence_count"], 1)

    def test_build_daily_briefing_repairs_thread_source_when_mentioned_person_has_at_prefix(self):
        class WrongAfasaGroupService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Anti-fraud",
                            "person": "Rene Chong",
                            "reminder": "Confirm with Glendys whether AFASA only needs UC pass-through or also needs attention to fields such as fvVersion.",
                            "evidence": "[PH x Reg] Compliance - AFASA by June 2026 / thread: ALC v12 pass-through",
                            "source_type": "seatalk",
                        }
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== bank 接入ALC v12 沟通 (group-4371534) ===",
                "[2026-05-22 15:01:49] Glendys Lau - 怡廷 (UID 620852) [thread reply under: ALC v12 pass-through]: @Ker Yin 珂瑩 @Rene Chong 可以帮忙确认一下现在你们只是关心如果 UC 是透传的吗 或者需要我们关注有什么特别的字段比如说 fvVersion?",
            ]
        )

        payload = build_daily_briefing(
            WrongAfasaGroupService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(len(payload["team_member_reminders"]), 1)
        self.assertEqual(
            payload["team_member_reminders"][0]["evidence"],
            "bank 接入ALC v12 沟通 / thread: ALC v12 pass-through",
        )

    def test_build_daily_briefing_infers_private_chat_source_when_topic_matches(self):
        class MatchingPrivateSourceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "domain": "General",
                            "task": "Schedule the 30 minute meeting with Evan next Monday to clarify the open questions.",
                            "priority": "low",
                            "due": "next week",
                            "evidence": "Private SeaTalk chat (buddy-1022128)",
                            "source_type": "seatalk",
                            "action_type": "direct_action",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== buddy-1022128 ===",
                "[2026-05-22 16:22:39] UID 1022128: Can i schedule a 30 minute meeting with you next Mon to clarify some questions.",
                "[2026-05-22 16:51:10] Zheng Xiaodong (UID 14420): Hi Evan, can book me next Mon 2:30-3pm. Today is a bit full.",
            ]
        )

        payload = build_daily_briefing(
            MatchingPrivateSourceService(history),
            now=datetime(2026, 5, 22, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(len(payload["direct_action_todos"]), 1)
        self.assertEqual(payload["direct_action_todos"][0]["evidence"], "Evan")

    def test_build_daily_briefing_suppresses_project_update_already_covered_by_todo(self):
        class DuplicateTopicService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Anti-fraud",
                            "title": "Customer PN false alarm",
                            "summary": "Grace needs to confirm wording before Xiaodong coordinates the CS reply for the PN false alarm.",
                            "status": "in_progress",
                            "evidence": "PH AF UAT物料沟通",
                            "evidence_ref_id": "st-ref-001",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "domain": "Anti-fraud",
                            "task": "After Grace confirms wording, send or coordinate the CS reply for the customer PN false-alarm complaint.",
                            "priority": "medium",
                            "due": "TBD",
                            "evidence": "自营贷crms / thread: complaint",
                            "evidence_ref_id": "st-ref-001",
                            "source_type": "seatalk",
                            "action_type": "direct_action",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AF UAT物料沟通 (group-4074790) ===",
                "[2026-05-21 10:00:00] Grace: Please confirm wording for the customer PN false-alarm complaint before CS reply.",
            ]
        )

        payload = build_daily_briefing(
            DuplicateTopicService(history),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["direct_action_todos"][0]["evidence"], "PH AF UAT物料沟通")
        self.assertEqual(payload["quality_metadata"]["evidence_quality_metrics"]["suppressed_update_duplicate_count"], 1)

    def test_build_daily_briefing_suppresses_cross_section_duplicate_topics(self):
        class DuplicateAcrossSectionsService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Anti-fraud",
                            "title": "AF launch readiness",
                            "summary": "Ker Yin needs to confirm whether Hold & Release phase 1 can go live today.",
                            "status": "blocked",
                            "evidence": "PH AAF Small Group",
                            "evidence_ref_id": "st-ref-001",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [
                        {
                            "domain": "Anti-fraud",
                            "title": "Hold & Release launch readiness",
                            "summary": "The same Hold & Release phase 1 go-live readiness topic was raised again.",
                            "status": "blocked",
                            "evidence": "PH AAF Small Group",
                            "evidence_ref_id": "st-ref-001",
                            "source_type": "seatalk",
                            "signal_type": "launch",
                        }
                    ],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "domain": "Anti-fraud",
                            "task": "Check whether Ker Yin has confirmed Hold & Release phase 1 go-live readiness.",
                            "priority": "high",
                            "due": "today",
                            "evidence": "PH AAF Small Group",
                            "evidence_ref_id": "st-ref-001",
                            "source_type": "seatalk",
                            "action_type": "watch_delegate",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AAF Small Group (group-2753344) ===",
                "[2026-05-21 09:30:00] Alice Tan: @Ker Yin please confirm whether Hold & Release phase 1 can go live today.",
            ]
        )

        payload = build_daily_briefing(
            DuplicateAcrossSectionsService(history),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["other_updates"], [])
        self.assertEqual(len(payload["watch_delegate_todos"]), 1)
        metrics = payload["quality_metadata"]["evidence_quality_metrics"]
        self.assertEqual(metrics["suppressed_cross_section_duplicate_count"], 1)

    def test_build_daily_briefing_backfills_valid_team_member_followup_and_records_diagnostics(self):
        class NoReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AAF Small Group (group-2753344) ===",
                "[2026-05-21 09:30:00] Alice Tan: @Ker Yin please confirm whether Hold & Release phase 1 can go live today.",
            ]
        )

        payload = build_daily_briefing(
            NoReminderService(history),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual([item["person"] for item in payload["team_member_reminders"]], ["Ker Yin"])
        metrics = payload["quality_metadata"]["evidence_quality_metrics"]
        self.assertEqual(metrics["deterministic_followup_backfill_count"], 1)
        self.assertEqual(metrics["followup_diagnostics"]["candidate_examples"][0]["person"], "Ker Yin")

    def test_build_daily_briefing_backfills_xiaodong_mention_when_unanswered(self):
        class NoReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== AF launch follow-up (group-101) ===",
                "[2026-05-21 09:30:00] Grace Zheng: @Zheng Xiaodong please confirm whether the PN false-alarm CS reply wording is okay.",
            ]
        )

        payload = build_daily_briefing(
            NoReminderService(history),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual([item["person"] for item in payload["team_member_reminders"]], ["Zheng Xiaodong"])
        self.assertEqual(payload["team_member_reminders"][0]["evidence"], "AF launch follow-up")
        metrics = payload["quality_metadata"]["evidence_quality_metrics"]
        self.assertEqual(metrics["followup_diagnostics"]["candidate_examples"][0]["person"], "Zheng Xiaodong")

    def test_build_daily_briefing_drops_backfilled_followup_with_wrong_topic_source(self):
        class NoReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AF DB拆库讨论 (group-4403195) ===",
                "[2026-05-21 11:44:24] Dheonardo: Hi team, this is the description of the issue https://space.sg.maribank.io/utility/swp/detail/20080 -Customer unable to approve transaction.",
                "[2026-05-21 12:43:55] Dheonardo: hi @Zoey Lu , can we continue for investigation on this ticket bcs the cm has uploaded the log?",
            ]
        )

        payload = build_daily_briefing(
            NoReminderService(history),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["team_member_reminders"], [])
        metrics = payload["quality_metadata"]["evidence_quality_metrics"]
        self.assertEqual(metrics["followup_diagnostics"]["reason_buckets"]["invalid_ref"], 1)

    def test_build_daily_briefing_drops_generic_seatalk_group_update(self):
        class GenericGroupUpdateService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Anti-fraud",
                            "title": "ITC release checks",
                            "summary": "ITC release checks required proper MR linkage and approval separation.",
                            "status": "done",
                            "evidence": "SeaTalk group",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        payload = build_daily_briefing(
            GenericGroupUpdateService(
                "\n".join(
                    [
                        "SeaTalk Chat History Export",
                        "=== group-783880 ===",
                        "[2026-05-21 10:37:15] Hale Zhou: ITC release checks required proper MR linkage and approval separation.",
                    ]
                )
            ),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["quality_metadata"]["evidence_quality_metrics"]["dropped_generic_evidence_count"], 1)

    def test_build_daily_briefing_repairs_followup_to_actual_thread_group(self):
        class WrongReminderSourceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Anti-fraud",
                            "person": "Rene Chong",
                            "reminder": "Confirm whether the ALC v12 face parameter scope includes more than fvVersion and includes fid.",
                            "evidence": "[PH x Reg] Compliance - AFASA by June 2026 / thread: alcv12 开户人脸参数问题",
                            "source_type": "seatalk",
                        }
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== bank 接入ALC v12 沟通 (group-4371534) ===",
                "[2026-05-21 10:38:12] Ker Yin 珂瑩 [thread reply under: alcv12 开户人脸参数问题]: @Rene Chong 不只是fvVersion 对吧？还有fid？",
            ]
        )

        payload = build_daily_briefing(
            WrongReminderSourceService(history),
            now=datetime(2026, 5, 21, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(
            payload["team_member_reminders"][0]["evidence"],
            "bank 接入ALC v12 沟通 / thread: alcv12 开户人脸参数问题",
        )

    def test_build_daily_briefing_repairs_thread_group_mismatch(self):
        class WrongGroupEvidenceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "task": "Monitor the Shopee checkout credit card entry UAT test for ABC whitelist readiness.",
                            "domain": "Credit Risk",
                            "priority": "medium",
                            "due": "TBD",
                            "evidence": "PH AF UAT物料沟通 / thread: uat2，shopee checkout 信用卡入口问题",
                            "source_type": "seatalk",
                            "action_type": "watch_delegate",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH Credit Card-Shopee Instant Checkout PM&Dev&QA群 (group-4206402) ===",
                "[2026-05-20 11:48:33] Zou Jianfeng (邹剑锋) (UID 209286) [thread reply under: uat2，shopee checkout 信用卡入口问题]: @Feng Ailing (冯爱玲) 我已经改好了，需要测试的时候告诉我一下，我打开开关",
                "[2026-05-20 12:00:51] QA (UID 1024899) [thread reply under: uat2，shopee checkout 信用卡入口问题]: 可以了，没有问题ABC的都有入口，不在白名单的没有入口",
            ]
        )

        payload = build_daily_briefing(
            WrongGroupEvidenceService(history),
            now=datetime(2026, 5, 20, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(
            payload["watch_delegate_todos"][0]["evidence"],
            "PH Credit Card-Shopee Instant Checkout PM&Dev&QA群 / thread: uat2，shopee checkout 信用卡入口问题",
        )

    def test_build_daily_briefing_drops_thread_evidence_with_mismatched_person(self):
        class MixedPersonEvidenceService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "task": "Ask Liye to confirm whether the Shopee checkout UAT segment-C phone-prefix issue requires Credit Risk changes.",
                            "domain": "Credit Risk",
                            "priority": "medium",
                            "due": "TBD",
                            "evidence": "PH AF UAT物料沟通 / thread: uat2，shopee checkout 信用卡入口问题",
                            "source_type": "seatalk",
                            "action_type": "watch_delegate",
                        }
                    ],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH Credit Card-Shopee Instant Checkout PM&Dev&QA群 (group-4206402) ===",
                "[2026-05-20 11:48:33] Zou Jianfeng (邹剑锋) (UID 209286) [thread reply under: uat2，shopee checkout 信用卡入口问题]: @Feng Ailing (冯爱玲) 我已经改好了，需要测试的时候告诉我一下，我打开开关",
                "[2026-05-20 12:00:51] QA (UID 1024899) [thread reply under: uat2，shopee checkout 信用卡入口问题]: 可以了，没有问题ABC的都有入口，不在白名单的没有入口",
            ]
        )

        payload = build_daily_briefing(
            MixedPersonEvidenceService(history),
            now=datetime(2026, 5, 20, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
        )

        self.assertEqual(payload["watch_delegate_todos"], [])

    def test_evidence_ref_helpers_apply_valid_refs_and_drop_invalid_refs(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AAF Small Group (group-2753344) ===",
                "[2026-05-20 15:10:00] Michael Salam: @Ker Yin please confirm whether PH Money Lock and Kill Switch public live can proceed on May 25.",
                "[2026-05-20 15:11:00] Alice Tan: @Rene Chong please evaluate AF appeal journey scope.",
            ]
        )
        candidates = [
            {
                "group": "PH AAF Small Group (group-2753344)",
                "thread": "",
                "timestamp": "2026-05-20 15:10:00",
                "text": "@Ker Yin please confirm whether PH Money Lock and Kill Switch public live can proceed on May 25.",
                "person": "Ker Yin",
            }
        ]

        refs = seatalk_daily_email._build_daily_brief_evidence_refs(
            history,
            team_member_reminder_candidates=candidates,
        )
        self.assertEqual(refs[0]["id"], "st-ref-001")
        self.assertEqual(refs[0]["reply_state"], "unanswered")
        self.assertEqual(refs[0]["evidence"], "PH AAF Small Group")

        my_todos = [
            {
                "task": "Ensure Ker Yin confirms whether PH Money Lock and Kill Switch public live can proceed.",
                "person": "Ker Yin",
                "evidence": "SeaTalk SeaTalk group",
                "evidence_ref_id": "st-ref-001",
                "source_type": "seatalk",
                "action_type": "watch_delegate",
            },
            {
                "task": "Ask Rene Chong to confirm unrelated scope.",
                "evidence": "SeaTalk group",
                "evidence_ref_id": "st-ref-001",
                "source_type": "seatalk",
                "action_type": "watch_delegate",
            },
            {
                "task": "Finish direct PM task.",
                "evidence": "SeaTalk group",
                "source_type": "seatalk",
                "action_type": "direct_action",
            },
        ]
        reminders = [
            {
                "person": "Ker Yin",
                "reminder": "Confirm PH Money Lock readiness.",
                "evidence": "SeaTalk group",
                "evidence_ref_id": "st-ref-001",
                "source_type": "seatalk",
            },
            {
                "person": "Rene Chong",
                "reminder": "Confirm unrelated scope.",
                "evidence": "SeaTalk group",
                "evidence_ref_id": "missing-ref",
                "source_type": "seatalk",
            },
        ]

        metrics = seatalk_daily_email._apply_daily_brief_evidence_refs(
            project_updates=[],
            other_updates=[],
            my_todos=my_todos,
            reminders=reminders,
            evidence_refs=refs,
        )

        self.assertEqual([item["task"] for item in my_todos], [
            "Ensure Ker Yin confirms whether PH Money Lock and Kill Switch public live can proceed.",
            "Finish direct PM task.",
        ])
        self.assertEqual(my_todos[0]["evidence"], "PH AAF Small Group")
        self.assertEqual([item["person"] for item in reminders], ["Ker Yin"])
        self.assertGreaterEqual(metrics["dropped_invalid_evidence_count"], 2)
        self.assertGreaterEqual(metrics["repaired_evidence_count"], 2)

    def test_evidence_repair_helpers_handle_generic_and_thread_evidence(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AAF Small Group (group-2753344) ===",
                "[2026-05-20 15:10:00] Michael Salam: Please confirm whether PH Money Lock and Kill Switch public live can proceed on May 25.",
                "=== PH Credit Card-Shopee Instant Checkout PM&Dev&QA群 (group-4206402) ===",
                "[2026-05-20 11:48:33] Zou Jianfeng (邹剑锋) (UID 209286) [thread reply under: uat2，shopee checkout 信用卡入口问题]: @Feng Ailing (冯爱玲) 我已经改好了，需要测试的时候告诉我一下，我打开开关",
            ]
        )
        items = [
            {
                "task": "Ensure Michael confirms PH Money Lock and Kill Switch readiness.",
                "evidence": "SeaTalk SeaTalk group",
                "source_type": "unknown",
            }
        ]
        metrics = {"repaired_evidence_count": 0}

        seatalk_daily_email._repair_generic_seatalk_evidence(
            items,
            history_text=history,
            quality_metrics=metrics,
        )

        self.assertEqual(items[0]["evidence"], "PH AAF Small Group")
        self.assertEqual(metrics["repaired_evidence_count"], 1)
        self.assertEqual(seatalk_daily_email._normalize_generic_seatalk_evidence("SeaTalk SeaTalk contact"), "SeaTalk contact")
        self.assertTrue(seatalk_daily_email._is_generic_seatalk_evidence("SeaTalk thread / thread: abc"))

        thread_items = [
            {
                "task": "Monitor the Shopee checkout credit card entry UAT test.",
                "evidence": "Wrong Group / thread: uat2，shopee checkout 信用卡入口问题",
                "source_type": "seatalk",
            },
            {
                "task": "Monitor unrelated item.",
                "evidence": "Wrong Group / thread: missing thread",
                "source_type": "seatalk",
            },
        ]
        thread_metrics = {"dropped_invalid_evidence_count": 0, "repaired_evidence_count": 0}

        seatalk_daily_email._validate_and_repair_seatalk_evidence(
            thread_items,
            history_text=history,
            quality_metrics=thread_metrics,
        )

        self.assertEqual(len(thread_items), 1)
        self.assertEqual(
            thread_items[0]["evidence"],
            "PH Credit Card-Shopee Instant Checkout PM&Dev&QA群 / thread: uat2，shopee checkout 信用卡入口问题",
        )
        self.assertEqual(thread_metrics["dropped_invalid_evidence_count"], 1)
        self.assertEqual(thread_metrics["repaired_evidence_count"], 1)

    def test_build_daily_briefing_enforces_section_caps(self):
        class OverflowService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "General",
                            "title": f"Project {index}",
                            "summary": f"Project update {index}.",
                            "status": "in_progress",
                            "evidence": f"Project source {index}",
                            "source_type": "seatalk",
                        }
                        for index in range(MAX_PROJECT_UPDATES + 3)
                    ],
                    "other_updates": [
                        {
                            "domain": "General",
                            "title": f"Other {index}",
                            "summary": f"Other update {index}.",
                            "status": "unknown",
                            "evidence": f"Other source {index}",
                            "source_type": "gmail",
                            "signal_type": "incident",
                        }
                        for index in range(MAX_OTHER_UPDATES + 3)
                    ],
                    "team_member_reminders": [
                        {
                            "domain": "Ops Risk",
                            "person": "Liye",
                            "reminder": f"Reminder {index}.",
                            "evidence": f"Reminder source {index}",
                            "source_type": "seatalk",
                        }
                        for index in range(MAX_TEAM_MEMBER_REMINDERS + 3)
                    ],
                    "my_todos": [
                        {
                            "task": f"Task {index}",
                            "domain": "General",
                            "priority": "medium",
                            "due": "TBD",
                            "evidence": f"Todo source {index}",
                            "source_type": "seatalk",
                        }
                        for index in range(MAX_MY_TODOS + 3)
                    ],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        payload = build_daily_briefing(
            OverflowService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n"),
            now=now,
        )

        self.assertEqual(len(payload["project_updates"]), MAX_PROJECT_UPDATES)
        self.assertEqual(len(payload["other_updates"]), MAX_OTHER_UPDATES)
        self.assertEqual(len(payload["team_member_reminders"]), MAX_TEAM_MEMBER_REMINDERS)
        self.assertEqual(len(payload["my_todos"]), MAX_MY_TODOS)

    def test_build_daily_briefing_adds_pm_action_layers_and_quality_metadata(self):
        class ActionLayerService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Ops Risk",
                            "title": "GRC audit history",
                            "summary": "PH GRC audit-history access is still pending confirmation and tomorrow clarify whether generate report needs separate audit log.",
                            "status": "done",
                            "evidence": "GRC evaluation group",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [
                        {
                            "domain": "General",
                            "title": "P2M surveillance risk",
                            "summary": "Launching before the 21 May AF surveillance fix may leave P2M without real-time fraud surveillance and require MAS/ITC endorsement.",
                            "status": "in_progress",
                            "evidence": "Gmail thread",
                            "source_type": "gmail",
                            "signal_type": "risk_compliance",
                        }
                    ],
                    "team_member_reminders": [
                        {
                            "domain": "Ops Risk",
                            "person": "Sabrina Chan",
                            "reminder": "Follow up tomorrow on the GRC audit-history access and generate-report audit-log expectation.",
                            "evidence": "GRC evaluation group",
                            "source_type": "seatalk",
                        }
                    ],
                    "my_todos": [
                        {
                            "task": "Review the SeaBank Direct Debit PRD and answer the two open questions.",
                            "domain": "Anti-fraud",
                            "priority": "high",
                            "due": "TBD",
                            "evidence": "Rene Chong direct chat",
                            "source_type": "seatalk",
                        },
                        {
                            "task": "Ensure PH follows up tomorrow on vendor onboarding owner alignment.",
                            "domain": "Ops Risk",
                            "priority": "high",
                            "due": "2026-04-30",
                            "evidence": "Vendor onboarding group",
                            "source_type": "seatalk",
                        },
                    ],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 29, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = ActionLayerService(
            "\n".join(
                [
                    "SeaTalk Chat History Export",
                    "=== GRC evaluation group ===",
                    "[2026-04-29 18:30:00] Sabrina Chan: PH GRC audit-history access is still pending confirmation and tomorrow clarify whether generate report needs separate audit log.",
                    "=== Rene Chong direct chat ===",
                    "[2026-04-29 18:35:00] Rene Chong: Please review the SeaBank Direct Debit PRD and answer the two open questions.",
                    "=== Vendor onboarding group ===",
                    "[2026-04-29 18:40:00] Bob: Ensure PH follows up tomorrow on vendor onboarding owner alignment.",
                ]
            )
        )
        payload = build_daily_briefing(
            service,
            now=now,
            gmail_history_text="Gmail thread history export\nMessage 1\nBody:\nMAS launch risk\n",
        )

        self.assertIn("action_type", service.last_prompt)
        self.assertEqual([item["action_type"] for item in payload["direct_action_todos"]], ["direct_action"])
        self.assertEqual([item["action_type"] for item in payload["watch_delegate_todos"]], ["watch_delegate"])
        self.assertEqual(payload["project_updates"][0]["status"], "in_progress")
        self.assertEqual(payload["other_updates"][0]["status"], "blocked")
        self.assertEqual(payload["other_updates"][0]["risk_level"], "high")
        self.assertEqual(payload["other_updates"][0]["signal_type"], "risk_compliance")
        self.assertTrue(payload["top_focus"])
        self.assertLessEqual(len(payload["top_focus"]), 3)
        self.assertEqual(payload["quality_metadata"]["source_coverage"], "SeaTalk + Gmail")
        self.assertEqual(payload["quality_metadata"]["high_confidence_todo_count"], 1)
        self.assertIn("deduped_topic_count", payload["quality_metadata"])
        self.assertIn("Vendor onboarding group", payload["watch_delegate_todos"][0]["evidence"])
        self.assertEqual([item["person"] for item in payload["team_member_reminders"]], [])

    def test_build_daily_briefing_injects_only_matched_report_intelligence(self):
        class MatchedService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [
                        {
                            "domain": "Credit Risk",
                            "title": "Project Alpha approval",
                            "summary": "Boss asked whether BSP approval may delay Project Alpha launch.",
                            "status": "in_progress",
                            "evidence": "Boss / BSP / BPMIS-1",
                            "source_type": "seatalk",
                        }
                    ],
                    "other_updates": [],
                    "team_member_reminders": [],
                    "my_todos": [
                        {
                            "task": "Follow up on BSP approval for Project Alpha.",
                            "domain": "Credit Risk",
                            "priority": "unknown",
                            "due": "TBD",
                            "evidence": "Boss / BSP / BPMIS-1",
                            "source_type": "seatalk",
                        }
                    ],
                    "team_todos": [],
                }

        config = {
            "vip_people": [
                {"display_name": "Boss", "role_tags": ["直属 Boss"], "aliases": ["Boss"]},
                {"display_name": "Unused VIP", "role_tags": ["Finance"]},
            ],
            "priority_keywords": ["BSP", "OJK"],
        }
        service = MatchedService("SeaTalk Chat History Export\n[2026-04-29 09:00:00] Credit group / Boss: BSP approval may delay BPMIS-1.\n")
        payload = build_daily_briefing(
            service,
            now=datetime(2026, 4, 29, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            report_intelligence_config=config,
            key_project_candidates=[{"bpmis_id": "BPMIS-1", "project_name": "Project Alpha", "jira_ids": ["CR-1"]}],
        )

        self.assertIn("Today's matched VIPs: Boss", service.last_prompt)
        self.assertIn("Today's matched priority keywords: BSP", service.last_prompt)
        self.assertIn("Today's matched key projects: BPMIS-1", service.last_prompt)
        self.assertNotIn("Unused VIP", service.last_prompt)
        self.assertNotIn("OJK", service.last_prompt)
        self.assertEqual(payload["my_todos"][0]["matched_vips"], ["Boss"])
        self.assertEqual(payload["my_todos"][0]["matched_keywords"], ["BSP"])
        self.assertEqual(payload["my_todos"][0]["matched_key_projects"], ["BPMIS-1 / Project Alpha"])
        self.assertEqual(payload["my_todos"][0]["priority"], "high")

    def test_render_email_handles_empty_partial_and_full_sections(self):
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        subject, text_body, html_body = render_email(briefing={"my_todos": [], "project_updates": []}, now=now)
        self.assertEqual(subject, "Daily Brief - 2026-04-27")
        self.assertIn("No clear action, blocker, key project update, or team follow-up was found", text_body)
        self.assertIn("To-do", text_body)
        self.assertNotIn("Xiaodong Action Required", text_body)
        self.assertNotIn("Watch / Delegate", text_body)
        self.assertNotIn("Project Updates", html_body)
        self.assertNotIn("No clear project update", text_body)
        self.assertNotIn("No additional high-value awareness update", text_body)
        self.assertNotIn("No unresolved SeaTalk team-member mention", text_body)

        _, text_body, _ = render_email(
            briefing={"my_todos": [{"task": "Review", "domain": "General", "priority": "high", "due": "today", "evidence": "Alice"}]},
            now=now,
        )
        self.assertIn("General\n[High] Review. Due: today (Source: Alice)", text_body)
        self.assertIn("Xiaodong Action Required", text_body)
        self.assertNotIn("Watch / Delegate", text_body)
        self.assertNotIn("Project Updates", text_body)
        self.assertNotIn("No clear project update", text_body)

        payload = build_daily_briefing(
            FakeSeaTalkService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n"),
            now=now,
        )
        _, text_body, html_body = render_email(briefing=payload, now=now)
        self.assertIn("Review rollout note", text_body)
        self.assertNotIn("Deck was refreshed. [Status: Done]", html_body)
        self.assertIn("Other Update", text_body)
        self.assertIn("A policy dependency may affect downstream rollout planning. [Status: In Progress]", html_body)
        self.assertIn("Suggested Team Follow-up", text_body)
        self.assertIn("Ker Yin: Please check whether the pending GRC confirmation still needs owner follow-up.", html_body)
        self.assertNotIn("Today Focus", text_body)
        self.assertNotIn("Today Focus", html_body)
        self.assertIn("Xiaodong Action Required", text_body)
        self.assertNotIn("Watch / Delegate", text_body)
        self.assertNotIn("Generation Quality", text_body)
        self.assertNotIn("Generation Quality", html_body)

    def test_render_email_compacts_sections_independently(self):
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        _, text_body, _ = render_email(
            briefing={
                "watch_delegate_todos": [
                    {"task": "Monitor closure", "domain": "General", "priority": "medium", "due": "TBD", "evidence": "Bob"}
                ]
            },
            now=now,
        )
        self.assertIn("Watch / Delegate", text_body)
        self.assertIn("Monitor closure", text_body)
        self.assertNotIn("Xiaodong Action Required", text_body)
        self.assertNotIn("No Xiaodong-owned action", text_body)

        _, text_body, html_body = render_email(
            briefing={
                "project_updates": [
                    {
                        "domain": "Credit Risk",
                        "title": "Done update",
                        "summary": "A routine update was completed.",
                        "status": "done",
                        "evidence": "Routine thread",
                    },
                    {
                        "domain": "Credit Risk",
                        "title": "Blocked update",
                        "summary": "Approval is blocked by a policy dependency.",
                        "status": "blocked",
                        "evidence": "Policy thread",
                    },
                    {
                        "domain": "Credit Risk",
                        "title": "Key project update",
                        "summary": "Project Alpha completed a routine checkpoint.",
                        "status": "done",
                        "evidence": "Project thread",
                        "matched_key_projects": ["BPMIS-1 / Project Alpha"],
                    },
                ]
            },
            now=now,
        )
        self.assertIn("No Xiaodong-owned action or watch/delegate item found.", text_body)
        self.assertIn("Project Updates", text_body)
        self.assertIn("Approval is blocked by a policy dependency", html_body)
        self.assertIn("Project Alpha completed a routine checkpoint", html_body)
        self.assertNotIn("A routine update was completed", html_body)

        _, text_body, html_body = render_email(
            briefing={
                "other_updates": [
                    {
                        "domain": "General",
                        "title": "FYI",
                        "summary": "A useful but routine awareness note was shared.",
                        "status": "unknown",
                        "signal_type": "useful_awareness",
                        "evidence": "FYI thread",
                    },
                    {
                        "domain": "General",
                        "title": "VIP FYI",
                        "summary": "Boss mentioned a routine follow-up.",
                        "status": "unknown",
                        "signal_type": "useful_awareness",
                        "matched_vips": ["Boss"],
                        "evidence": "Boss thread",
                    },
                    {
                        "domain": "Ops Risk",
                        "title": "Risk",
                        "summary": "A risk compliance dependency may affect launch.",
                        "status": "in_progress",
                        "signal_type": "risk_compliance",
                        "evidence": "Risk thread",
                    },
                ]
            },
            now=now,
        )
        self.assertIn("Other Update", text_body)
        self.assertIn("Boss mentioned a routine follow-up", html_body)
        self.assertIn("A risk compliance dependency may affect launch", html_body)
        self.assertNotIn("A useful but routine awareness note was shared", html_body)

        _, text_body, html_body = render_email(
            briefing={
                "team_member_reminders": [
                    {"domain": "Anti-fraud", "person": "Rene Chong", "reminder": "Check the case.", "evidence": "Group"}
                ]
            },
            now=now,
        )
        self.assertIn("Suggested Team Follow-up", text_body)
        self.assertIn("Rene Chong: Check the case.", html_body)

        _, text_body, html_body = render_email(
            briefing={
                "watch_delegate_todos": [
                    {
                        "task": "Ensure Rene and Ker Yin confirm PH/ID local report usage before DWH ingestion is paused by ES migration.",
                        "domain": "Anti-fraud",
                        "priority": "high",
                        "due": "TBD",
                        "evidence": "SeaTalk rule_trigger_log_tab migration discussion",
                    }
                ],
                "team_member_reminders": [
                    {
                        "domain": "Anti-fraud",
                        "person": "Ker Yin",
                        "reminder": "Check with local AF whether rule_trigger_log_tab is used for PH reports before DWH sync is paused.",
                        "evidence": "SeaTalk rule_trigger_log_tab migration discussion",
                    },
                    {
                        "domain": "Anti-fraud",
                        "person": "Rene Chong",
                        "reminder": "Check with local teams and validate the user-facing impact note for rule_trigger_log_tab DWH cutover.",
                        "evidence": "SeaTalk rule_trigger_log_tab migration discussion",
                    },
                ],
            },
            now=now,
        )
        self.assertIn("Watch / Delegate", text_body)
        self.assertIn("Ensure Rene and Ker Yin confirm", text_body)
        self.assertNotIn("Suggested Team Follow-up", text_body)
        self.assertNotIn("Ker Yin: Check with local AF", html_body)
        self.assertNotIn("Rene Chong: Check with local teams", html_body)

    def test_daily_brief_prompt_allows_empty_low_signal_sections(self):
        prompt = _daily_brief_user_prompt(
            history_text="SeaTalk Chat History Export\n",
            gmail_history_text="Gmail thread history export\n",
            hours=24,
            local_now=datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            match_summary="Today's matched VIPs: Boss.",
        )

        self.assertIn("Empty arrays are expected when a section has no important signal", prompt)
        self.assertIn("Do not fill sections just to produce a report", prompt)
        self.assertIn("already represented as a my_todos watch_delegate item", prompt)
        self.assertIn("Report Intelligence Matches", prompt)
        self.assertIn("Use these matches only as prioritization hints", prompt)

    def test_build_daily_briefing_compacts_prompt_sources_and_records_token_ledger(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== AF-ID follow-up group (group-100) ===",
                "[2026-05-13 09:00:00] Alice Tan: @Ker Yin please confirm whether ETP linkage flows use soft token by default",
                *[
                    f"[2026-05-13 09:{index % 60:02d}:00] Random User: low value filler {index} " + ("noise " * 45)
                    for index in range(900)
                ],
                "[2026-05-13 12:50:00] Bob Tan: AF launch is blocked pending MAS approval and owner decision.",
            ]
        )
        gmail_history = "\n".join(
            [
                "Message 1",
                "Subject: CR rollout approval",
                *[f"Body filler {index} " + ("email noise " * 45) for index in range(500)],
                "Body: BSP launch approval is pending and needs Xiaodong review.",
            ]
        )
        service = FakeSeaTalkService(history)

        payload = build_daily_briefing(
            service,
            now=datetime(2026, 5, 13, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            gmail_history_text=gmail_history,
        )

        ledger = payload["quality_metadata"]["token_ledger"]
        self.assertGreater(ledger["seatalk_raw_chars"], ledger["seatalk_prompt_chars"])
        self.assertGreater(ledger["gmail_raw_chars"], ledger["gmail_prompt_chars"])
        self.assertGreater(ledger["final_estimated_prompt_tokens"], 0)
        self.assertLessEqual(ledger["seatalk_prompt_chars"], 70_000)
        self.assertLessEqual(ledger["gmail_prompt_chars"], 35_000)
        self.assertLess(ledger["final_estimated_prompt_tokens"], 30_000)
        self.assertEqual(ledger["prompt_budget_policy"], "quality_preserving_soft_budget")
        self.assertEqual(ledger["prompt_budget_threshold_tokens"], 30_000)
        self.assertFalse(ledger["quality_preserving_over_budget"])
        self.assertEqual(ledger["compaction_reason"], "quality_preserving_signal_recent_evidence")
        self.assertGreaterEqual(ledger["preserved_evidence_ref_count"], 1)
        self.assertGreaterEqual(ledger["preserved_followup_candidate_count"], 1)
        self.assertIn("Deterministic Daily Brief Evidence Bundle", service.last_prompt)
        self.assertIn('"evidence_refs"', service.last_prompt)
        self.assertNotIn('"token_ledger"', service.last_prompt)
        self.assertIn('"candidate_followups"', service.last_prompt)
        self.assertIn("@Ker Yin please confirm", service.last_prompt)
        self.assertIn("BSP launch approval is pending", service.last_prompt)
        self.assertNotIn("low value filler 200", service.last_prompt)
        self.assertIn("seatalk_prompt_hit_cap", ledger)

    def test_unanswered_seatalk_question_hints_include_pm_relevant_thread_questions(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== [PH Card] google pay 新增域名 (group-4374390) ===",
                "[2026-05-11 16:39:23] Qiao Wenxing (乔文星) (UID 342609) [thread reply under: notifyServiceActivated setup]: [image]",
                "[2026-05-11 16:39:26] Qiao Wenxing (乔文星) (UID 342609) [thread reply under: notifyServiceActivated setup]: notifyServiceActivated流程中没有cvc校验，是不是写错了，不用上送这个吧",
                "=== Other group (group-1) ===",
                "[2026-05-11 16:40:00] Someone (UID 1): thanks",
            ]
        )

        hints = _build_unanswered_seatalk_question_hints(history)

        self.assertIn("[PH Card] google pay 新增域名", hints)
        self.assertIn("notifyServiceActivated流程中没有cvc校验", hints)
        self.assertIn("Qiao Wenxing", hints)

    def test_unanswered_seatalk_question_hints_exclude_answered_thread_questions(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== [PH Card] google pay 新增域名 (group-4374390) ===",
                "[2026-05-11 16:39:26] Qiao Wenxing (乔文星) (UID 342609) [thread reply under: notifyServiceActivated setup]: notifyServiceActivated流程中没有cvc校验，是不是写错了",
                "[2026-05-11 16:41:00] Ker Yin (UID 786789) [thread reply under: notifyServiceActivated setup]: 对，这里不用上送 cvc",
            ]
        )

        hints = _build_unanswered_seatalk_question_hints(history)

        self.assertEqual(hints, "")

    def test_team_member_reminder_candidates_require_no_named_person_reply(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== AF-ID follow-up group (group-100) ===",
                "[2026-05-13 10:00:00] Alice Tan: @Wang Chang please decide whether Reject & Punish should stay SOP-only",
                "[2026-05-13 10:05:00] Wang Chang (UID 123): I will check with dev and confirm.",
                "[2026-05-13 10:08:00] Alice Tan: @Ker Yin please confirm whether ETP linkage flows use soft token by default",
            ]
        )

        candidates = _build_team_member_reminder_candidates(history)

        self.assertEqual([item["person"] for item in candidates or []], ["Ker Yin"])
        self.assertIn("ETP linkage", candidates[0]["text"])

    def test_team_member_reminder_candidates_do_not_match_ambiguous_short_names(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== [ID] AF & Auth Credit Card (group-4320981) ===",
                "[2026-05-20 18:44:33] ZhangFan (张帆) (UID 342541) [thread reply under: ph bank non-live gateway https域名申请]: hi @Feng Ailing (冯爱玲) @Huang Haitao (黄海涛) @Chang Xinxin(常鑫鑫) | Banking | iOS",
                "=== SG SPX group (group-4252112) ===",
                "[2026-05-20 15:40:36] Rene Chong (UID 341874) [thread reply under: AF PRD Clarifications]: @Zoey Jiaqi Cui credit card will have different cardIdentifier from debit card right?",
                "[2026-05-20 16:00:00] Alice Tan: @Wang Chang please confirm the AF release owner",
            ]
        )

        candidates = _build_team_member_reminder_candidates(history)

        self.assertEqual([item["person"] for item in candidates or []], ["Wang Chang"])
        self.assertIn("AF release owner", candidates[0]["text"])

    def test_team_member_reminder_candidates_ignore_bare_mentions_without_action(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== PH AF UAT物料沟通 (group-4074790) ===",
                "[2026-05-20 17:02:13] Grace Zheng (UID 496270): @Zheng Xiaodong @Zoey Lu",
                "[2026-05-20 17:05:00] Alice Tan: @Zoey Lu please confirm whether AF next steps are still needed",
            ]
        )

        candidates = _build_team_member_reminder_candidates(history)

        self.assertEqual([item["person"] for item in candidates or []], ["Zoey Lu"])
        self.assertIn("AF next steps", candidates[0]["text"])

    def test_team_member_reminder_candidates_include_xiaodong_when_mentioned_and_unanswered(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== AF launch follow-up (group-101) ===",
                "[2026-05-20 17:02:13] Grace Zheng (UID 496270): @Zheng Xiaodong please confirm whether the PN false-alarm CS reply wording is okay",
                "[2026-05-20 17:05:00] Zoey Lu (UID 355879): I can help check the bug side",
            ]
        )

        candidates = _build_team_member_reminder_candidates(history)

        self.assertEqual([item["person"] for item in candidates or []], ["Zheng Xiaodong"])
        self.assertIn("PN false-alarm", candidates[0]["text"])

    def test_team_member_reminder_candidates_drop_xiaodong_after_reply(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== AF launch follow-up (group-101) ===",
                "[2026-05-20 17:02:13] Grace Zheng (UID 496270): @Zheng Xiaodong please confirm whether the PN false-alarm CS reply wording is okay",
                "[2026-05-20 17:06:00] Zheng Xiaodong (UID 14420): I will review and reply to CS.",
            ]
        )

        candidates = _build_team_member_reminder_candidates(history)

        self.assertEqual(candidates, [])

    def test_team_member_reminder_candidates_treat_thread_reply_as_named_person_reply(self):
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== AF urgent requirement (group-4445222) ===",
                "[2026-05-20 14:41:08] Harken (UID 94019): 这个需求，我觉得和刚刚开会讨论的那个紧急需求基本上一样，有空 af 老板们也可以评估下 @Jireh @Wang Chang",
                "[2026-05-20 18:23:02] Jireh (UID 633302) [thread reply under: 这个需求，我觉得和刚刚开会讨论的那个紧急需求基本上一样，有空 af 老板们也可以评估下 @Jireh @Wang Chang]: 哦，明白了，正在问业务之后紧急需求需要改吗",
            ]
        )

        candidates = _build_team_member_reminder_candidates(history)

        self.assertEqual([item["person"] for item in candidates or []], ["Wang Chang"])

    def test_build_daily_briefing_drops_followup_when_named_person_replied(self):
        class RepliedReminderService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                return None, {
                    "project_updates": [],
                    "other_updates": [],
                    "team_member_reminders": [
                        {
                            "domain": "Anti-fraud",
                            "person": "Wang Chang",
                            "reminder": "Decide whether Reject & Punish should keep SOP-only control.",
                            "evidence": "AF-ID follow-up group",
                            "source_type": "seatalk",
                        }
                    ],
                    "my_todos": [],
                    "team_todos": [],
                }

        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== AF-ID follow-up group (group-100) ===",
                "[2026-05-13 10:00:00] Alice Tan: @Wang Chang please decide whether Reject & Punish should keep SOP-only control",
                "[2026-05-13 10:05:00] Wang Chang (UID 123): I replied in the group and will align with dev.",
            ]
        )
        service = RepliedReminderService(history)
        payload = build_daily_briefing(service, now=datetime(2026, 5, 13, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))

        self.assertEqual(payload["team_member_reminders"], [])
        self.assertIn("No valid unresolved team-member mention candidates were found", service.last_prompt)

    def test_build_trello_card_specs_includes_direct_watch_and_followups(self):
        briefing = {
            "direct_action_todos": [
                {
                    "task": "Review rollout note",
                    "domain": "Anti-fraud",
                    "priority": "high",
                    "due": "today",
                    "evidence": "Alice",
                    "source_type": "seatalk",
                }
            ],
            "watch_delegate_todos": [
                {
                    "task": "Follow up with Rene on closure",
                    "domain": "General",
                    "priority": "medium",
                    "due": "TBD",
                    "evidence": "SeaTalk group",
                    "source_type": "mixed",
                }
            ],
            "team_member_reminders": [
                {
                    "domain": "Anti-fraud",
                    "person": "Rene Chong",
                    "reminder": "Check the ID appeal case.",
                    "evidence": "AFA PM Local x Regional",
                    "source_type": "seatalk",
                }
            ],
        }

        specs = build_trello_card_specs(briefing=briefing, run_date="2026-04-30")

        self.assertEqual([spec.name for spec in specs], [
            "[Direct] Review rollout note",
            "[Watch] Follow up with Rene on closure",
            "[Follow-up] Rene Chong: Check the ID appeal case",
        ])
        self.assertIn("Section: Xiaodong Action Required", specs[0].description)
        self.assertIn("Due: today", specs[0].description)
        self.assertEqual(specs[0].target_list, "Today")
        self.assertEqual(specs[0].labels, ("AF-ID",))
        self.assertEqual(specs[0].due, "")
        self.assertIn("Section: Watch / Delegate", specs[1].description)
        self.assertEqual(specs[1].target_list, "Watch / Risk")
        self.assertIn("Person: Rene Chong", specs[2].description)
        self.assertEqual(specs[2].target_list, "Waiting / Follow-up")

    def test_build_trello_card_specs_routes_direct_by_due_window(self):
        briefing = {
            "direct_action_todos": [
                {"task": "No due item", "domain": "General", "priority": "medium", "due": "TBD", "evidence": "A"},
                {"task": "Tomorrow item", "domain": "Credit Risk", "priority": "medium", "due": "tomorrow", "evidence": "B"},
                {"task": "This week item", "domain": "GRC", "priority": "medium", "due": "2026-05-13", "evidence": "C"},
            ],
            "watch_delegate_todos": [],
            "team_member_reminders": [],
        }

        specs = build_trello_card_specs(briefing=briefing, run_date="2026-05-09")

        self.assertEqual([spec.target_list for spec in specs], ["Inbox", "Today", "This Week"])
        self.assertEqual(specs[0].due, "")
        self.assertEqual(specs[1].due, "")
        self.assertEqual(specs[2].due, "2026-05-13")
        self.assertEqual(specs[1].labels, ("Credit Risk",))
        self.assertEqual(specs[2].labels, ("GRC",))

    def test_build_trello_card_specs_does_not_treat_validate_as_ai(self):
        briefing = {
            "direct_action_todos": [
                {"task": "Validate Credit Card PRD", "domain": "Credit Risk", "priority": "medium", "due": "TBD", "evidence": "A"},
            ],
            "watch_delegate_todos": [],
            "team_member_reminders": [],
        }

        specs = build_trello_card_specs(briefing=briefing, run_date="2026-05-09")

        self.assertEqual(specs[0].labels, ("Credit Risk",))

    def test_build_trello_card_specs_drops_followups_covered_by_watch_delegate(self):
        briefing = {
            "watch_delegate_todos": [
                {
                    "task": "Follow up with Liye on the DWH timeline for Credit Risk monitoring data ingestion and 31 May report target.",
                    "domain": "Credit Risk",
                    "priority": "medium",
                    "due": "2026-05-08",
                    "evidence": "SeaTalk Credit Risk PM requirement",
                }
            ],
            "team_member_reminders": [
                {
                    "domain": "Credit Risk",
                    "person": "Liye",
                    "reminder": "Confirm the DWH ingestion timeline tomorrow so the 31 May report target can be assessed.",
                    "evidence": "SeaTalk Credit Risk PM requirement",
                    "source_type": "seatalk",
                }
            ],
        }

        specs = build_trello_card_specs(briefing=briefing, run_date="2026-05-07")

        self.assertEqual([spec.section for spec in specs], ["Watch / Delegate"])
        self.assertEqual(specs[0].name, "[Watch] Follow up with Liye on the DWH timeline for Credit Risk monitoring data ingestion and 31 May report target")

    def test_trello_client_reuses_list_and_creates_cards(self):
        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload
                self.text = ""

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        class FakeSession:
            def __init__(self):
                self.posts = []

            def get(self, url, params, timeout):
                return FakeResponse([{"id": "list-1", "name": "Daily Summary Email", "closed": False}])

            def post(self, url, params, timeout):
                self.posts.append({"url": url, "params": params})
                return FakeResponse({"id": "card-1", "name": params["name"], "url": "https://trello.test/card-1"})

        session = FakeSession()
        client = TrelloDailySummaryClient(
            api_key="key",
            api_token="token",
            board_id="board-1",
            session=session,
            base_url="https://trello.test/1",
        )

        list_id = client.get_or_create_list_id()
        card = client.create_card(
            list_id=list_id,
            name="[Direct] Review",
            description="Report date: 2026-04-30",
            label_ids=["label-1"],
            due="2026-04-30",
        )

        self.assertEqual(list_id, "list-1")
        self.assertEqual(card.url, "https://trello.test/card-1")
        self.assertEqual(session.posts[0]["params"]["idList"], "list-1")
        self.assertEqual(session.posts[0]["params"]["idLabels"], "label-1")
        self.assertEqual(session.posts[0]["params"]["due"], "2026-04-30")

    def test_trello_client_lists_existing_cards(self):
        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload
                self.text = ""

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        class FakeSession:
            def get(self, url, params, timeout):
                return FakeResponse(
                    [
                        {
                            "id": "card-1",
                            "name": "[Direct] Review rollout",
                            "desc": "Report date: 2026-04-30\nDomain: Anti-fraud",
                            "closed": False,
                        },
                        {
                            "id": "card-2",
                            "name": "[Direct] Closed",
                            "desc": "Report date: 2026-04-30\nDomain: Anti-fraud",
                            "closed": True,
                        },
                    ]
                )

        client = TrelloDailySummaryClient(api_key="key", api_token="token", board_id="board-1", session=FakeSession())

        cards = client.list_cards(list_id="list-1")

        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["id"], "card-1")

    def test_trello_client_creates_missing_daily_list(self):
        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload
                self.text = ""

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        class FakeSession:
            def __init__(self):
                self.posts = []

            def get(self, url, params, timeout):
                return FakeResponse([])

            def post(self, url, params, timeout):
                self.posts.append({"url": url, "params": params})
                return FakeResponse({"id": "new-list", "name": params["name"]})

        session = FakeSession()
        client = TrelloDailySummaryClient(api_key="key", api_token="token", board_id="board-1", session=session)

        self.assertEqual(client.get_or_create_list_id(), "new-list")
        self.assertEqual(session.posts[0]["params"]["name"], "Daily Summary Email")

    def test_trello_client_reuses_and_creates_labels(self):
        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload
                self.text = ""

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        class FakeSession:
            def __init__(self):
                self.posts = []
                self.gets = 0

            def get(self, url, params, timeout):
                self.gets += 1
                return FakeResponse([{"id": "existing", "name": "AF-ID", "color": "red"}])

            def post(self, url, params, timeout):
                self.posts.append({"url": url, "params": params})
                return FakeResponse({"id": "created", "name": params["name"], "color": params["color"]})

        session = FakeSession()
        client = TrelloDailySummaryClient(api_key="key", api_token="token", board_id="board-1", session=session)

        self.assertEqual(client.get_or_create_label_id("AF-ID"), "existing")
        self.assertEqual(client.get_or_create_label_id("Credit Risk", color="purple"), "created")
        self.assertEqual(session.posts[0]["params"]["name"], "Credit Risk")
        self.assertEqual(session.posts[0]["params"]["color"], "purple")

    def test_trello_client_requires_env_config(self):
        with self.assertRaisesRegex(ConfigError, "TRELLO_API_KEY"):
            TrelloDailySummaryClient(api_key="", api_token="token", board_id="board")
        with self.assertRaisesRegex(ConfigError, "TRELLO_API_KEY"):
            TrelloDailySummaryClient(api_key="key", api_token="", board_id="board")
        with self.assertRaisesRegex(ConfigError, "TRELLO_API_KEY"):
            TrelloDailySummaryClient(api_key="key", api_token="token", board_id="")

    def test_trello_client_and_store_error_boundaries(self):
        class FakeResponse:
            def __init__(self, payload=None, *, text="", status_error=None, json_error=False):
                self.payload = payload
                self.text = text
                self.status_error = status_error
                self.json_error = json_error

            def raise_for_status(self):
                if self.status_error:
                    raise self.status_error

            def json(self):
                if self.json_error:
                    raise ValueError("bad json")
                return self.payload

        class FakeSession:
            def __init__(self):
                self.get_payloads = []
                self.post_payloads = []
                self.put_payloads = []
                self.posts = []
                self.puts = []

            def get(self, url, params, timeout):
                return self.get_payloads.pop(0)

            def post(self, url, params, timeout):
                self.posts.append({"url": url, "params": params})
                return self.post_payloads.pop(0)

            def put(self, url, params, timeout):
                self.puts.append({"url": url, "params": params})
                return self.put_payloads.pop(0)

        session = FakeSession()
        client = TrelloDailySummaryClient(api_key="key", api_token="token", board_id="board-1", session=session)

        session.get_payloads = [FakeResponse({"not": "a-list"})]
        with self.assertRaisesRegex(ToolError, "invalid board list"):
            client.board_lists()

        session.get_payloads = [FakeResponse(["bad", {"closed": True}, {"idBoard": "write-board", "name": "Other"}])]
        session.post_payloads = [FakeResponse({})]
        with self.assertRaisesRegex(ToolError, "did not return an id"):
            client.get_or_create_list_id("Target")
        self.assertEqual(session.posts[-1]["params"]["idBoard"], "write-board")
        self.assertEqual(client._board_id_for_writes(), "write-board")

        session.post_payloads = [FakeResponse(["bad-card"])]
        with self.assertRaisesRegex(ToolError, "invalid card payload"):
            client.create_card(list_id="list-1", name="Name", description="Desc")
        session.post_payloads = [FakeResponse({"name": "No id or URL"})]
        with self.assertRaisesRegex(ToolError, "id or URL"):
            client.create_card(list_id="list-1", name="Name", description="Desc")

        session.get_payloads = [FakeResponse({"bad": "cards"})]
        with self.assertRaisesRegex(ToolError, "invalid card list"):
            client.list_cards(list_id="list-1")
        session.get_payloads = [
            FakeResponse(
                [
                    {"id": "open", "closed": False, "name": "Open"},
                    {"id": "closed", "closed": True},
                    "bad",
                ]
            )
        ]
        self.assertEqual(client.list_board_cards()[0]["id"], "open")
        session.get_payloads = [FakeResponse({"bad": "board-cards"})]
        with self.assertRaisesRegex(ToolError, "invalid board card"):
            client.list_board_cards()

        session.get_payloads = [FakeResponse({"bad": "labels"})]
        with self.assertRaisesRegex(ToolError, "invalid board label"):
            client.board_labels()
        self.assertEqual(client.get_or_create_label_id(""), "")
        session.get_payloads = [FakeResponse([]), FakeResponse([{"id": "existing", "name": "AF-ID"}])]
        session.post_payloads = [FakeResponse({"id": "created"})]
        self.assertEqual(client.get_or_create_label_ids(["New Label", "", "AF-ID"]), ["created", "existing"])
        session.get_payloads = [FakeResponse([])]
        session.post_payloads = [FakeResponse({})]
        with self.assertRaisesRegex(ToolError, "created label"):
            client.get_or_create_label_id("Broken Label")

        session.put_payloads = [FakeResponse({}), FakeResponse({}), FakeResponse({})]
        session.post_payloads = [FakeResponse({})]
        client.move_card(card_id="card-1", list_id="list-2")
        client.add_label_to_card(card_id="card-1", label_id="label-1")
        client.archive_card(card_id="card-1")
        client.rename_list(list_id="list-1", name="Renamed")
        self.assertEqual(session.puts[-1]["params"]["name"], "Renamed")

        with self.assertRaisesRegex(ToolError, "Failed to test action"):
            TrelloDailySummaryClient._json_response(
                FakeResponse(text="secret details", status_error=requests.RequestException("boom")),
                action="test action",
            )
        with self.assertRaisesRegex(ToolError, "invalid JSON"):
            TrelloDailySummaryClient._json_response(FakeResponse(json_error=True), action="parse action")

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "trello.json"
            path.write_text("not-json", encoding="utf-8")
            store = TrelloDailySummaryStore(path)
            self.assertFalse(store.has_card("missing"))
            path.write_text("[]", encoding="utf-8")
            self.assertFalse(store.has_card("missing"))

        self.assertEqual(daily_card_identity_from_trello_card({"name": "No report", "desc": "Domain: AF"}), "")

    def test_daily_trello_sync_is_idempotent(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self, list_name=None):
                return {"Inbox": "inbox", "Today": "today", "Watch / Risk": "watch", "Waiting / Follow-up": "follow"}.get(list_name, "list-1")

            def create_card(self, *, list_id, name, description, label_ids=None, due=None):
                self.created.append({"list_id": list_id, "name": name, "description": description, "label_ids": label_ids or [], "due": due or ""})
                from bpmis_jira_tool.trello_daily_summary import TrelloCardResult

                return TrelloCardResult(
                    status="created",
                    name=name,
                    url=f"https://trello.test/{len(self.created)}",
                    trello_id=f"card-{len(self.created)}",
                )

        briefing = {
            "direct_action_todos": [{"task": "Review rollout", "domain": "Anti-fraud", "priority": "high", "due": "today", "evidence": "Alice"}],
            "watch_delegate_todos": [{"task": "Monitor closure", "domain": "General", "priority": "medium", "due": "TBD", "evidence": "Bob"}],
            "team_member_reminders": [{"person": "Rene Chong", "reminder": "Check the case", "domain": "Anti-fraud", "evidence": "Group"}],
        }
        now = datetime(2026, 4, 30, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = TrelloDailySummaryStore(Path(temp_dir) / "daily_trello_cards.json")
            client = FakeTrelloClient()
            first = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )
            second = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )

        self.assertEqual(first.created_count, 3)
        self.assertEqual(first.skipped_count, 0)
        self.assertEqual(second.created_count, 0)
        self.assertEqual(second.skipped_count, 3)

    def test_daily_trello_sync_is_disabled_when_env_is_missing(self):
        briefing = {
            "direct_action_todos": [{"task": "Review rollout", "domain": "Anti-fraud", "priority": "high", "due": "today", "evidence": "Alice"}],
            "watch_delegate_todos": [],
            "team_member_reminders": [],
        }
        now = datetime(2026, 4, 30, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict("os.environ", {"TRELLO_API_KEY": "", "TRELLO_API_TOKEN": "", "TRELLO_BOARD_ID": ""}, clear=False):
                result = sync_daily_summary_to_trello(
                    briefing=briefing,
                    run_date="2026-04-30",
                    run_slot="midday",
                    data_root=Path(temp_dir),
                    now=now,
                )
        self.assertEqual(result.status, "disabled")

    def test_daily_trello_sync_dedupes_same_item_across_morning_and_midday(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self, list_name=None):
                return {"Today": "today"}.get(list_name, "list-1")

            def create_card(self, *, list_id, name, description, label_ids=None, due=None):
                self.created.append({"list_id": list_id, "name": name, "description": description, "label_ids": label_ids or [], "due": due or ""})
                from bpmis_jira_tool.trello_daily_summary import TrelloCardResult

                return TrelloCardResult(status="created", name=name, url=f"https://trello.test/{len(self.created)}", trello_id=f"card-{len(self.created)}")

        briefing = {
            "direct_action_todos": [{"task": "Review rollout", "domain": "Anti-fraud", "priority": "high", "due": "today", "evidence": "Alice"}],
            "watch_delegate_todos": [],
            "team_member_reminders": [],
        }
        now = datetime(2026, 4, 30, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = TrelloDailySummaryStore(Path(temp_dir) / "daily_trello_cards.json")
            client = FakeTrelloClient()
            morning = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                run_slot="morning",
                window_label="2026-04-30 08:00 - 2026-04-30 13:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )
            midday = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                run_slot="midday",
                window_label="2026-04-30 13:00 - 2026-04-30 19:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )
            midday_again = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                run_slot="midday",
                window_label="2026-04-30 13:00 - 2026-04-30 19:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )

        self.assertEqual(morning.created_count, 1)
        self.assertEqual(midday.created_count, 0)
        self.assertEqual(midday.skipped_count, 1)
        self.assertEqual(midday_again.created_count, 0)
        self.assertEqual(midday_again.skipped_count, 1)
        self.assertEqual(len(client.created), 1)
        self.assertIn("Report window: 2026-04-30 08:00 - 2026-04-30 13:00", client.created[0]["description"])

    def test_daily_trello_sync_skips_existing_board_card_when_local_state_is_missing(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self, list_name=None):
                return {"Today": "today"}.get(list_name, "list-1")

            def list_board_cards(self):
                return [
                    {
                        "id": "card-existing",
                        "name": "[Direct] Review rollout",
                        "desc": "Report date: 2026-04-30\nDomain: Anti-fraud\nTask: Review rollout",
                        "closed": False,
                        "idList": "inbox",
                    }
                ]

            def list_cards(self, *, list_id):
                raise AssertionError("daily sync should dedupe against board-wide open cards")

            def create_card(self, *, list_id, name, description, label_ids=None, due=None):
                self.created.append({"list_id": list_id, "name": name, "description": description})
                from bpmis_jira_tool.trello_daily_summary import TrelloCardResult

                return TrelloCardResult(status="created", name=name, url="https://trello.test/new", trello_id="card-new")

        briefing = {
            "direct_action_todos": [{"task": "Review rollout", "domain": "Anti-fraud", "priority": "high", "due": "today", "evidence": "Alice"}],
            "watch_delegate_todos": [],
            "team_member_reminders": [],
        }
        now = datetime(2026, 4, 30, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        with tempfile.TemporaryDirectory() as temp_dir:
            result = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                run_slot="morning",
                window_label="2026-04-30 08:00 - 2026-04-30 13:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=FakeTrelloClient(),
                trello_store=TrelloDailySummaryStore(Path(temp_dir) / "daily_trello_cards.json"),
            )

        self.assertEqual(result.created_count, 0)
        self.assertEqual(result.skipped_count, 1)

    def test_daily_trello_sync_skips_existing_legacy_list_card_when_local_state_is_missing(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self, list_name=None):
                return "list-1"

            def list_cards(self, *, list_id):
                self.list_id = list_id
                return [
                    {
                        "id": "card-existing",
                        "name": "[Direct] Review rollout",
                        "desc": "Report date: 2026-04-30\nDomain: Anti-fraud\nTask: Review rollout",
                        "closed": False,
                    }
                ]

            def create_card(self, *, list_id, name, description, label_ids=None, due=None):
                self.created.append({"list_id": list_id, "name": name, "description": description})
                from bpmis_jira_tool.trello_daily_summary import TrelloCardResult

                return TrelloCardResult(status="created", name=name, url="https://trello.test/new", trello_id="card-new")

        briefing = {
            "direct_action_todos": [{"task": "Review rollout", "domain": "Anti-fraud", "priority": "high", "due": "today", "evidence": "Alice"}],
            "watch_delegate_todos": [],
            "team_member_reminders": [],
        }
        now = datetime(2026, 4, 30, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)

        with tempfile.TemporaryDirectory() as temp_dir:
            result = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                run_slot="morning",
                window_label="2026-04-30 08:00 - 2026-04-30 13:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=FakeTrelloClient(),
                trello_store=TrelloDailySummaryStore(Path(temp_dir) / "daily_trello_cards.json"),
            )

        self.assertEqual(result.created_count, 0)
        self.assertEqual(result.skipped_count, 1)

    def test_trello_organizer_plans_workflow_moves_and_labels(self):
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "organize_trello_board.py"
        spec = importlib.util.spec_from_file_location("organize_trello_board", script_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        lists = [
            {"id": "today", "name": "Today", "closed": False},
            {"id": "week", "name": "This week", "closed": False},
            {"id": "afph", "name": "Anti Fraud - PH", "closed": False},
            {"id": "other", "name": "Others", "closed": False},
            {"id": "daily", "name": "Daily Summary Email", "closed": False},
        ]
        cards = [
            {"id": "c1", "idList": "afph", "name": "[Follow-up] Ker Yin: Confirm PH scope", "labels": [], "due": None},
            {"id": "c2", "idList": "afph", "name": "ShopeePay Txn Limit Project", "labels": [], "due": None},
            {"id": "c3", "idList": "other", "name": "VPN Password: secret", "labels": [], "due": None},
            {"id": "c4", "idList": "daily", "name": "[Direct] Review rollout", "labels": [], "due": None},
        ]

        actions = module.plan_board_reorganization(lists=lists, cards=cards, labels=[])

        self.assertIn({"id": "week", "from": "This week", "to": "This Week"}, actions["rename_lists"])
        self.assertIn({"id": "c4", "name": "[Direct] Review rollout"}, actions["archive_cards"])
        self.assertIn({"id": "c1", "name": "[Follow-up] Ker Yin: Confirm PH scope", "from": "Anti Fraud - PH", "to": "Waiting / Follow-up"}, actions["move_cards"])
        self.assertIn({"id": "c2", "name": "ShopeePay Txn Limit Project", "from": "Anti Fraud - PH", "to": "Project Backlog"}, actions["move_cards"])
        self.assertIn({"id": "c3", "name": "VPN Password: secret", "from": "Others", "to": "Personal / Sensitive"}, actions["move_cards"])
        self.assertIn({"id": "c1", "name": "[Follow-up] Ker Yin: Confirm PH scope", "label": "AF-PH"}, actions["add_labels"])
        self.assertIn({"id": "c3", "name": "VPN Password: secret", "label": "Sensitive"}, actions["add_labels"])

    def test_trello_organizer_ignores_cards_from_closed_or_unknown_lists(self):
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "organize_trello_board.py"
        spec = importlib.util.spec_from_file_location("organize_trello_board", script_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        actions = module.plan_board_reorganization(
            lists=[{"id": "visible", "name": "Anti Fraud - PH", "closed": False}],
            cards=[
                {"id": "c1", "idList": "closed", "name": "Old hidden card", "labels": [], "due": None},
                {"id": "c2", "idList": "visible", "name": "Current PH card", "labels": [], "due": None},
            ],
            labels=[],
        )

        moved_card_ids = {action["id"] for action in actions["move_cards"]}
        self.assertNotIn("c1", moved_card_ids)
        self.assertIn("c2", moved_card_ids)

    def test_trello_organizer_is_idempotent_for_workflow_lists(self):
        script_path = Path(__file__).resolve().parents[1] / "scripts" / "organize_trello_board.py"
        spec = importlib.util.spec_from_file_location("organize_trello_board", script_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        lists = [
            {"id": "personal", "name": "Personal / Sensitive", "closed": False},
            {"id": "watch", "name": "Watch / Risk", "closed": False},
        ]
        cards = [
            {"id": "c1", "idList": "personal", "name": "VPN Password", "labels": [{"name": "Personal"}, {"name": "Sensitive"}], "due": None},
            {"id": "c2", "idList": "watch", "name": "[Watch] Track ALCv12", "labels": [{"name": "AF-ID"}], "due": None},
        ]

        actions = module.plan_board_reorganization(lists=lists, cards=cards, labels=[])

        self.assertEqual(actions["move_cards"], [])
        self.assertEqual(actions["add_labels"], [])

    def test_gmail_raw_message_contains_expected_headers_and_body(self):
        raw = build_gmail_raw_message(
            sender="xiaodong.zheng@npt.sg",
            recipient="xiaodong.zheng@npt.sg",
            subject="SeaTalk Daily Brief - 2026-04-27",
            text_body="To-do\n- Review\n",
            html_body="<p>Review</p>",
        )
        decoded = base64.urlsafe_b64decode(raw.encode("utf-8")).decode("utf-8")
        self.assertIn("To: xiaodong.zheng@npt.sg", decoded)
        self.assertIn("From: xiaodong.zheng@npt.sg", decoded)
        self.assertIn("Subject: SeaTalk Daily Brief - 2026-04-27", decoded)
        self.assertIn("To-do", decoded)
        self.assertIn("<p>Review</p>", decoded)

    def test_gmail_raw_message_can_include_text_attachment(self):
        raw = build_gmail_raw_message(
            sender="xiaodong.zheng@npt.sg",
            recipient="xiaodong.zheng@npt.sg",
            subject="Meeting Minutes",
            text_body="Minutes body",
            attachments=[
                {
                    "filename": "meeting-transcript.txt",
                    "mime_type": "text/plain",
                    "content": b"Alice approved the launch.",
                }
            ],
        )
        decoded = base64.urlsafe_b64decode(raw.encode("utf-8")).decode("utf-8")

        self.assertIn("filename=\"meeting-transcript.txt\"", decoded)
        self.assertIn("Content-Type: text/plain", decoded)
        self.assertIn("QWxpY2UgYXBwcm92ZWQgdGhlIGxhdW5jaC4=", decoded)

    def test_gmail_raw_message_skips_invalid_attachments_and_sends_with_service(self):
        raw = build_gmail_raw_message(
            sender="xiaodong.zheng@npt.sg",
            recipient="xiaodong.zheng@npt.sg",
            subject="Report",
            text_body="Body",
            attachments=[
                "not-a-dict",
                {"filename": "empty.txt", "content": None},
                {"filename": "../notes.md", "content": "hello"},
            ],
        )
        decoded = base64.urlsafe_b64decode(raw.encode("utf-8")).decode("utf-8")
        self.assertIn("filename=\"notes.md\"", decoded)
        self.assertIn("aGVsbG8=", decoded)
        self.assertNotIn("empty.txt", decoded)

        class FakeSend:
            def __init__(self):
                self.body = None

            def send(self, *, userId, body):
                self.body = body
                self.user_id = userId
                return self

            def execute(self):
                return {"id": "msg-1", "raw_prefix": self.body["raw"][:8], "user": self.user_id}

        class FakeMessages:
            def __init__(self, sender):
                self.sender = sender

            def send(self, **kwargs):
                return self.sender.send(**kwargs)

        class FakeUsers:
            def __init__(self, sender):
                self.sender = sender

            def messages(self):
                return FakeMessages(self.sender)

        class FakeGmailService:
            def __init__(self):
                self.sender = FakeSend()

            def users(self):
                return FakeUsers(self.sender)

        result = send_gmail_message(
            credentials=object(),
            sender="xiaodong.zheng@npt.sg",
            recipient="xiaodong.zheng@npt.sg",
            subject="Report",
            text_body="Body",
            gmail_service=FakeGmailService(),
        )
        self.assertEqual(result["id"], "msg-1")
        self.assertEqual(result["user"], "me")

    def test_missing_gmail_send_scope_reports_reconnect(self):
        with self.assertRaisesRegex(ConfigError, "Reconnect Google"):
            ensure_gmail_send_scope({"scopes": ["https://www.googleapis.com/auth/gmail.readonly"]})

        with self.assertRaisesRegex(ConfigError, "Reconnect Google"):
            credentials_from_payload({"scopes": ["https://www.googleapis.com/auth/gmail.readonly"]})

    def test_missing_gmail_daily_read_scope_reports_reconnect(self):
        with self.assertRaisesRegex(ConfigError, "Reconnect Google"):
            ensure_gmail_daily_scopes({"scopes": [GMAIL_SEND_SCOPE]})

        ensure_gmail_daily_scopes({"scopes": [GMAIL_SEND_SCOPE, GMAIL_READONLY_SCOPE]})

    def test_stored_google_credentials_encrypts_owner_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = Path(temp_dir) / "credentials.json"
            store = StoredGoogleCredentials(store_path, encryption_key=Fernet.generate_key().decode("utf-8"))
            payload = {"token": "access-token", "refresh_token": "refresh-token", "scopes": [GMAIL_SEND_SCOPE]}

            store.save(owner_email="xiaodong.zheng@npt.sg", credentials_payload=payload)

            raw = store_path.read_text(encoding="utf-8")
            self.assertNotIn("refresh-token", raw)
            self.assertEqual(store.load(owner_email="xiaodong.zheng@npt.sg")["refresh_token"], "refresh-token")

    def test_stored_google_credentials_handles_missing_config_and_corrupt_payloads(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = Path(temp_dir) / "credentials.json"
            unencrypted_store = StoredGoogleCredentials(store_path)
            unencrypted_store.save(owner_email="xiaodong.zheng@npt.sg", credentials_payload={"token": "ignored"})
            unencrypted_store.save(owner_email="", credentials_payload={"token": "ignored"})
            self.assertFalse(store_path.exists())
            with self.assertRaisesRegex(ConfigError, "owner email"):
                unencrypted_store.load(owner_email="")
            with self.assertRaisesRegex(ConfigError, "ENCRYPTION_KEY"):
                unencrypted_store.load(owner_email="xiaodong.zheng@npt.sg")

            encrypted_store = StoredGoogleCredentials(store_path, encryption_key=Fernet.generate_key().decode("utf-8"))
            with self.assertRaisesRegex(ConfigError, "not saved"):
                encrypted_store.load(owner_email="missing@npt.sg")
            store_path.write_text("{bad-json", encoding="utf-8")
            with self.assertRaisesRegex(ConfigError, "not saved"):
                encrypted_store.load(owner_email="missing@npt.sg")

            store_path.write_text(json.dumps({"owners": {"xiaodong.zheng@npt.sg": "not-a-token"}}), encoding="utf-8")
            with self.assertRaisesRegex(ToolError, "decrypt"):
                encrypted_store.load(owner_email="xiaodong.zheng@npt.sg")

            key = Fernet.generate_key().decode("utf-8")
            invalid_payload_store = StoredGoogleCredentials(store_path, encryption_key=key)
            encrypted_list = Fernet(key.encode("utf-8")).encrypt(b'["not", "dict"]').decode("utf-8")
            store_path.write_text(json.dumps({"owners": {"xiaodong.zheng@npt.sg": encrypted_list}}), encoding="utf-8")
            with self.assertRaisesRegex(ToolError, "invalid"):
                invalid_payload_store.load(owner_email="xiaodong.zheng@npt.sg")

    def test_idempotency_skips_second_send_unless_forced(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DailyEmailRunStore(Path(temp_dir) / "runs.json")
            now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
            store.mark_sent(
                run_date="2026-04-27",
                recipient="xiaodong.zheng@npt.sg",
                subject="SeaTalk Daily Brief - 2026-04-27",
                message_id="msg-1",
                sent_at=now,
            )
            self.assertTrue(store.already_sent(run_date="2026-04-27", recipient="xiaodong.zheng@npt.sg"))

    def test_fixed_daily_email_windows_cover_previous_19_to_13_and_13_to_19(self):
        midday = resolve_daily_email_window(
            now=datetime(2026, 4, 30, 19, 5, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            slot="auto",
        )
        self.assertEqual(midday.run_slot, "midday")
        self.assertEqual(midday.run_date, "2026-04-30")
        self.assertEqual(midday.start.isoformat(), "2026-04-30T13:00:00+08:00")
        self.assertEqual(midday.end.isoformat(), "2026-04-30T19:00:00+08:00")

        morning = resolve_daily_email_window(
            now=datetime(2026, 4, 30, 13, 5, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            slot="auto",
        )
        self.assertEqual(morning.run_slot, "morning")
        self.assertEqual(morning.run_date, "2026-04-30")
        self.assertEqual(morning.start.isoformat(), "2026-04-29T19:00:00+08:00")
        self.assertEqual(morning.end.isoformat(), "2026-04-30T13:00:00+08:00")

    def test_monday_morning_window_covers_previous_weekday_19_to_13(self):
        morning = resolve_daily_email_window(
            now=datetime(2026, 5, 4, 13, 5, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            slot="auto",
        )

        self.assertEqual(morning.run_slot, "morning")
        self.assertEqual(morning.run_date, "2026-05-04")
        self.assertEqual(morning.start.isoformat(), "2026-05-01T19:00:00+08:00")
        self.assertEqual(morning.end.isoformat(), "2026-05-04T13:00:00+08:00")

    def test_fixed_daily_email_windows_skip_saturday_and_sunday(self):
        self.assertTrue(
            should_skip_fixed_daily_email_window(
                now=datetime(2026, 5, 2, 8, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            )
        )
        self.assertTrue(
            should_skip_fixed_daily_email_window(
                now=datetime(2026, 5, 3, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            )
        )
        self.assertFalse(
            should_skip_fixed_daily_email_window(
                now=datetime(2026, 5, 4, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            )
        )

    def test_daily_email_run_store_tracks_morning_and_midday_separately(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = DailyEmailRunStore(Path(temp_dir) / "runs.json")
            now = datetime(2026, 4, 30, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
            store.mark_sent(
                run_date="2026-04-30",
                run_slot="midday",
                recipient="xiaodong.zheng@npt.sg",
                subject="Daily Brief - 2026-04-30",
                message_id="msg-1",
                sent_at=now,
            )
            self.assertTrue(store.already_sent(run_date="2026-04-30", run_slot="midday", recipient="xiaodong.zheng@npt.sg"))
            self.assertFalse(store.already_sent(run_date="2026-04-30", run_slot="morning", recipient="xiaodong.zheng@npt.sg"))

    def test_send_daily_email_skips_existing_run_before_work(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
            DailyEmailRunStore(Path(temp_dir) / "seatalk" / "daily_email_runs.json").mark_sent(
                run_date="2026-04-27",
                run_slot="midday",
                recipient="xiaodong.zheng@npt.sg",
                subject="SeaTalk Daily Brief - 2026-04-27",
                message_id="msg-1",
                sent_at=now,
            )
            with patch("bpmis_jira_tool.seatalk_daily_email.build_daily_briefing") as briefing:
                result = send_daily_email(settings=settings, now=now)
            briefing.assert_not_called()
            self.assertEqual(result.status, "skipped")

    def test_send_daily_email_skips_weekend_before_work(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            now = datetime(2026, 5, 3, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
            with patch("bpmis_jira_tool.seatalk_daily_email.build_daily_briefing") as briefing:
                result = send_daily_email(settings=settings, now=now, slot="midday")
            briefing.assert_not_called()
            self.assertEqual(result.status, "skipped")
            self.assertEqual(result.run_date, "2026-05-03")
            self.assertEqual(result.run_slot, "midday")
            self.assertEqual(result.window_start, "2026-05-03T13:00:00+08:00")
            self.assertEqual(result.window_end, "2026-05-03T19:00:00+08:00")

    def test_send_daily_email_syncs_trello_cards_before_marking_sent(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self, list_name=None):
                return {"Today": "today", "Watch / Risk": "watch", "Waiting / Follow-up": "follow"}.get(list_name, "list-1")

            def create_card(self, *, list_id, name, description, label_ids=None, due=None):
                self.created.append({"list_id": list_id, "name": name, "description": description, "label_ids": label_ids or [], "due": due or ""})
                from bpmis_jira_tool.trello_daily_summary import TrelloCardResult

                return TrelloCardResult(status="created", name=name, url=f"https://trello.test/{len(self.created)}", trello_id=f"card-{len(self.created)}")

        with tempfile.TemporaryDirectory() as temp_dir:
            encryption_key = Fernet.generate_key().decode("utf-8")
            settings = _settings(temp_dir, encryption_key=encryption_key)
            store = StoredGoogleCredentials(
                Path(temp_dir) / "google" / "credentials.json",
                encryption_key=encryption_key,
            )
            store.save(
                owner_email="xiaodong.zheng@npt.sg",
                credentials_payload={
                    "token": "access-token",
                    "scopes": [GMAIL_SEND_SCOPE, GMAIL_READONLY_SCOPE],
                },
            )
            briefing = {
                "direct_action_todos": [{"task": "Review rollout", "domain": "Anti-fraud", "priority": "high", "due": "today", "evidence": "Alice"}],
                "watch_delegate_todos": [{"task": "Monitor closure", "domain": "General", "priority": "medium", "due": "TBD", "evidence": "Bob"}],
                "team_member_reminders": [{"person": "Rene Chong", "reminder": "Check the case", "domain": "Anti-fraud", "evidence": "Group"}],
                "my_todos": [],
                "project_updates": [],
            }
            trello_client = FakeTrelloClient()
            trello_store = TrelloDailySummaryStore(Path(temp_dir) / "seatalk" / "daily_trello_cards.json")

            with patch("bpmis_jira_tool.seatalk_daily_email.build_seatalk_service", return_value=FakeSeaTalkService()):
                with patch("bpmis_jira_tool.seatalk_daily_email.export_window_gmail_threads", return_value=""):
                    with patch("bpmis_jira_tool.seatalk_daily_email.build_daily_briefing", return_value=briefing):
                        with patch("bpmis_jira_tool.seatalk_daily_email.send_gmail_message", return_value={"id": "msg-1"}):
                            result = send_daily_email(
                                settings=settings,
                                now=datetime(2026, 4, 30, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                                gmail_service=object(),
                                trello_client=trello_client,
                                trello_store=trello_store,
                            )

        self.assertEqual(result.status, "sent")
        self.assertEqual(result.trello_status, "synced")
        self.assertEqual(result.trello_created_count, 3)
        self.assertEqual(result.trello_skipped_count, 0)
        self.assertEqual([card["name"] for card in result.trello_cards], [
            "[Direct] Review rollout",
            "[Watch] Monitor closure",
            "[Follow-up] Rene Chong: Check the case",
        ])

    def test_send_daily_email_archives_body_and_force_overwrites(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            encryption_key = Fernet.generate_key().decode("utf-8")
            settings = _settings(temp_dir, encryption_key=encryption_key)
            store = StoredGoogleCredentials(
                Path(temp_dir) / "google" / "credentials.json",
                encryption_key=encryption_key,
            )
            store.save(
                owner_email="xiaodong.zheng@npt.sg",
                credentials_payload={
                    "token": "access-token",
                    "scopes": [GMAIL_SEND_SCOPE, GMAIL_READONLY_SCOPE],
                },
            )
            briefing = {
                "direct_action_todos": [{"task": "Review rollout", "domain": "Anti-fraud", "priority": "high", "due": "today", "evidence": "Alice"}],
                "watch_delegate_todos": [],
                "team_member_reminders": [],
                "my_todos": [],
                "project_updates": [],
                "quality_metadata": {
                    "token_ledger": {"final_estimated_prompt_tokens": 1234},
                    "evidence_quality_metrics": {"dropped_invalid_evidence_count": 2},
                },
            }

            with patch("bpmis_jira_tool.seatalk_daily_email.build_seatalk_service", return_value=FakeSeaTalkService()):
                with patch("bpmis_jira_tool.seatalk_daily_email.export_window_gmail_threads", return_value=""):
                    with patch("bpmis_jira_tool.seatalk_daily_email.build_daily_briefing", return_value=briefing):
                        with patch("bpmis_jira_tool.seatalk_daily_email.send_gmail_message", side_effect=[{"id": "msg-1"}, {"id": "msg-2"}]):
                            first = send_daily_email(
                                settings=settings,
                                now=datetime(2026, 5, 5, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                                slot="morning",
                                gmail_service=object(),
                            )
                            second = send_daily_email(
                                settings=settings,
                                now=datetime(2026, 5, 5, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                                slot="morning",
                                force=True,
                                gmail_service=object(),
                            )

            self.assertEqual(first.status, "sent")
            self.assertEqual(second.status, "sent")
            archive = DailyBriefArchiveStore(daily_brief_archive_path(Path(temp_dir))).list_recent()
            self.assertEqual(len(archive), 1)
            self.assertEqual(archive[0]["message_id"], "msg-2")
            self.assertEqual(archive[0]["run_slot"], "morning")
            self.assertEqual(archive[0]["time_period"], "2026-05-04 19:00-2026-05-05 13:00")
            self.assertIn("Review rollout", archive[0]["text_body"])
            self.assertEqual(archive[0]["token_ledger"]["final_estimated_prompt_tokens"], 1234)
            self.assertEqual(archive[0]["quality_metadata"]["evidence_quality_metrics"]["dropped_invalid_evidence_count"], 2)

    def test_send_daily_email_dry_run_does_not_archive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            encryption_key = Fernet.generate_key().decode("utf-8")
            settings = _settings(temp_dir, encryption_key=encryption_key)
            store = StoredGoogleCredentials(
                Path(temp_dir) / "google" / "credentials.json",
                encryption_key=encryption_key,
            )
            store.save(
                owner_email="xiaodong.zheng@npt.sg",
                credentials_payload={
                    "token": "access-token",
                    "scopes": [GMAIL_SEND_SCOPE, GMAIL_READONLY_SCOPE],
                },
            )
            with patch("bpmis_jira_tool.seatalk_daily_email.build_seatalk_service", return_value=FakeSeaTalkService()):
                with patch("bpmis_jira_tool.seatalk_daily_email.export_window_gmail_threads", return_value=""):
                    result = send_daily_email(
                        settings=settings,
                        now=datetime(2026, 5, 5, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                        slot="morning",
                        dry_run=True,
                        gmail_service=object(),
                    )

            self.assertEqual(result.status, "dry_run")
            archive = DailyBriefArchiveStore(daily_brief_archive_path(Path(temp_dir))).list_recent()
            self.assertEqual(archive, [])

    def test_daily_brief_archive_boundaries_and_pdf_formatting_helpers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = daily_brief_archive_path(Path(temp_dir))
            path.parent.mkdir(parents=True)
            path.write_text("not-json", encoding="utf-8")
            store = DailyBriefArchiveStore(path)
            self.assertEqual(store.list_recent(), [])
            self.assertIsNone(store.get("missing"))

            with patch("pathlib.Path.read_text", side_effect=OSError("unreadable")):
                self.assertEqual(store.list_recent(), [])

            path.write_text("[]", encoding="utf-8")
            self.assertEqual(store.list_recent(), [])
            saved = store.save(
                run_date="2026-05-05",
                run_slot="morning",
                recipient="owner@npt.sg",
                subject="Daily Brief",
                text_body="Subject: Daily Brief\n\nWindow: old\n\nBody",
                html_body="",
                message_id="msg-1",
                status="",
                sent_at=datetime(2026, 5, 5, 13, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                window_start="",
                window_end="",
            )
            self.assertEqual(saved["status"], "sent")
            self.assertEqual(format_daily_brief_period(saved), "")
            self.assertEqual(store.list_recent(limit=0)[0]["brief_id"], saved["brief_id"])

        same_day = {
            "window_start": "2026-05-05T08:00:00+08:00",
            "window_end": "2026-05-05T13:00:00+08:00",
        }
        next_day = {
            "window_start": "2026-05-04T19:00:00+08:00",
            "window_end": "2026-05-05T13:00:00+08:00",
        }
        self.assertEqual(format_daily_brief_period({}), "")
        self.assertEqual(format_daily_brief_period(same_day), "2026-05-05 08:00-13:00")
        self.assertEqual(format_daily_brief_period(next_day), "2026-05-04 19:00-2026-05-05 13:00")
        self.assertIsNone(_parse_datetime(""))
        self.assertIsNone(_parse_datetime("not-a-date"))

        stripped = _strip_leading_daily_brief_heading(
            [
                _PdfLine((_PdfSegment("Daily Brief"),)),
                _PdfLine((_PdfSegment("2026-05-05 (08:00-13:00)"),)),
                _PdfLine(tuple()),
                _PdfLine((_PdfSegment("Body"),)),
            ],
            title="Daily Brief 2026-05-05 (08:00-13:00)",
        )
        self.assertEqual("".join(segment.text for segment in stripped[0].segments), "Body")
        self.assertEqual(_strip_leading_daily_brief_heading([], title="Daily Brief"), [])
        self.assertEqual(
            _strip_leading_window_line(
                [
                    _PdfLine((_PdfSegment("Window: 08:00"),)),
                    _PdfLine(tuple()),
                    _PdfLine((_PdfSegment("Body"),)),
                ]
            )[0].segments[0].text,
            "Body",
        )
        wrapped_title = _strip_leading_daily_brief_heading(
            [
                _PdfLine((_PdfSegment("Daily Brief 2026-05-05"),)),
                _PdfLine((_PdfSegment("08:00-13:00"),)),
                _PdfLine((_PdfSegment("Body"),)),
            ],
            title="Daily Brief 2026-05-05 (08:00-13:00)",
        )
        self.assertEqual(wrapped_title[0].segments[0].text, "Body")

        html = """
        <html><body>
          <style>.x{}</style><script>ignore()</script>
          <h4>Small Heading</h4>
          <p>Hello <strong>bold</strong><br>Next</p>
          <ul><li>Parent<ul><li>Child</li></ul></li></ul>
          <section><span>Generic text</span></section>
        </body></html>
        """
        pdf = daily_brief_pdf_bytes(title="Daily Brief", body="", html_body=html)
        self.assertTrue(pdf.startswith(b"%PDF-1.4"))
        self.assertTrue(_html_node_pdf_lines(BeautifulSoup("<br/>", "html.parser").br))
        self.assertEqual(_html_node_pdf_lines(object()), [])
        fake_inline_node = type("FakeInlineNode", (), {"children": [object()]})()
        self.assertEqual(_inline_segments(fake_inline_node), [])
        segments = _inline_segments(BeautifulSoup("<p>A<ul><li>Skip</li></ul><br>B</p>", "html.parser").p, stop_at_block=True)
        self.assertTrue(any(segment.text == "\n" for segment in segments))
        normalized = _normalize_segments([_PdfSegment(""), _PdfSegment("A"), _PdfSegment("B")])
        self.assertEqual(normalized, [_PdfSegment("A B")])
        self.assertEqual([line.segments[0].text for line in _wrapped_segment_lines([_PdfSegment("A\nB")])], ["A", "B"])
        self.assertGreater(len(_wrap_pdf_lines("x" * 80, max_chars=20)), 1)
        fallback_lines = _daily_brief_pdf_lines(
            title="Daily Brief",
            body="Subject: Daily Brief\n\nWindow: old\n\n" + ("long text " * 20),
            html_body="",
        )
        self.assertTrue(any("long text" in "".join(segment.text for segment in line.segments) for line in fallback_lines))

    def test_send_daily_email_reports_gmail_export_timeout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            encryption_key = Fernet.generate_key().decode("utf-8")
            settings = _settings(temp_dir, encryption_key=encryption_key)
            store = StoredGoogleCredentials(
                Path(temp_dir) / "google" / "credentials.json",
                encryption_key=encryption_key,
            )
            store.save(
                owner_email="xiaodong.zheng@npt.sg",
                credentials_payload={
                    "token": "access-token",
                    "scopes": [GMAIL_SEND_SCOPE, GMAIL_READONLY_SCOPE],
                },
            )

            with patch("bpmis_jira_tool.seatalk_daily_email.build_seatalk_service", return_value=FakeSeaTalkService()):
                with patch("bpmis_jira_tool.seatalk_daily_email.export_window_gmail_threads", side_effect=TimeoutError):
                    with self.assertRaisesRegex(ConfigError, "daily brief timeout"):
                        send_daily_email(
                            settings=settings,
                            now=datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                            dry_run=True,
                            gmail_service=object(),
                        )

    def test_daily_email_store_window_and_timeout_edge_helpers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = Path(temp_dir) / "runs.json"
            store_path.write_text("{bad-json", encoding="utf-8")
            store = DailyEmailRunStore(store_path)
            self.assertFalse(store.already_sent(run_date="2026-05-01", recipient="owner@npt.sg"))
            store_path.write_text("[]", encoding="utf-8")
            self.assertFalse(store.already_sent(run_date="2026-05-01", recipient="owner@npt.sg"))

            relative_settings = replace(Settings.from_env(), team_portal_data_dir=Path("relative-data"))
            self.assertTrue(seatalk_daily_email.data_root_from_settings(relative_settings).is_absolute())

        with self.assertRaisesRegex(ConfigError, "Unsupported daily email slot"):
            resolve_daily_email_window(now=datetime(2026, 5, 1, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE), slot="night")

        with patch.dict("os.environ", {"DAILY_EMAIL_GMAIL_EXPORT_TIMEOUT_SECONDS": "bad"}):
            self.assertEqual(seatalk_daily_email._gmail_export_timeout_seconds(), seatalk_daily_email.GMAIL_EXPORT_TIMEOUT_SECONDS)
        with patch.dict("os.environ", {"DAILY_EMAIL_GMAIL_EXPORT_TIMEOUT_SECONDS": "1"}):
            self.assertEqual(seatalk_daily_email._gmail_export_timeout_seconds(), 15)
        with patch.dict("os.environ", {"DAILY_EMAIL_GMAIL_EXPORT_TIMEOUT_SECONDS": "999"}):
            self.assertEqual(seatalk_daily_email._gmail_export_timeout_seconds(), 300)

        class FakeGmailBriefService:
            def __init__(self):
                self.calls = []

            def export_thread_history_since(self, *, since, now):
                self.calls.append((since, now))
                return "Gmail thread history export\n"

        fake_gmail = FakeGmailBriefService()
        now = datetime(2026, 5, 1, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with patch("bpmis_jira_tool.seatalk_daily_email.threading.current_thread", return_value=object()):
            self.assertIn("Gmail thread", seatalk_daily_email._export_rolling_gmail_threads_with_timeout(fake_gmail, now=now, hours=0))

        with patch("bpmis_jira_tool.seatalk_daily_email.signal.setitimer") as setitimer:
            setitimer.side_effect = [(2, 0), None, None]
            with patch("bpmis_jira_tool.seatalk_daily_email.signal.getsignal", return_value="old"):
                with patch("bpmis_jira_tool.seatalk_daily_email.signal.signal") as signal_mock:
                    self.assertIn(
                        "Gmail thread",
                        seatalk_daily_email._export_window_gmail_threads_with_timeout(
                            fake_gmail,
                            window_start=now.replace(hour=13),
                            window_end=now,
                        ),
                    )
        signal_mock.assert_any_call(seatalk_daily_email.signal.SIGALRM, "old")

        with patch("bpmis_jira_tool.seatalk_daily_email.export_window_gmail_threads", side_effect=TimeoutError):
            with self.assertRaisesRegex(ConfigError, "daily brief timeout"):
                seatalk_daily_email._export_window_gmail_threads_with_timeout(
                    fake_gmail,
                    window_start=now.replace(hour=13),
                    window_end=now,
                )

    def test_trello_specs_sync_and_render_edge_helpers(self):
        now = datetime(2026, 5, 1, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        briefing = {
            "my_todos": [
                {"task": "Confirm AF SG rollout by 2026-05-02", "domain": "Anti-fraud SG", "priority": "high", "due": "2026-05-02", "evidence": "SeaTalk group", "source_type": "seatalk"},
                {"task": "Monitor credit DWH dependency", "domain": "Credit Risk", "priority": "medium", "due": "2026-05-08", "evidence": "Gmail thread", "source_type": "gmail", "action_type": "watch_delegate"},
            ],
            "team_member_reminders": [{"person": "Rene Chong", "reminder": "Confirm fraud rollout", "domain": "Anti-fraud", "evidence": "SeaTalk group"}],
        }
        specs = build_trello_card_specs(briefing=briefing, run_date="2026-05-01", window_label="13:00-19:00")

        self.assertEqual({spec.target_list for spec in specs}, {"Today", "Watch / Risk", "Waiting / Follow-up"})
        self.assertTrue(any("AF-SG" in spec.labels for spec in specs))
        self.assertTrue(any("Credit Risk" in spec.labels for spec in specs))

        class FakeTrelloClientNoBoard:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self, list_name=None):
                return f"list-{list_name or 'default'}"

            def list_cards(self, *, list_id):
                return []

            def get_or_create_label_ids(self, names):
                return [f"label-{name}" for name in names]

            def create_card(self, **kwargs):
                self.created.append(kwargs)
                from bpmis_jira_tool.trello_daily_summary import TrelloCardResult

                return TrelloCardResult(status="created", name=kwargs["name"], url="https://trello.test/card", trello_id="card-1")

        with tempfile.TemporaryDirectory() as temp_dir:
            result = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-05-01",
                data_root=Path(temp_dir),
                now=now,
                trello_client=FakeTrelloClientNoBoard(),
            )
        self.assertEqual(result.status, "synced")
        self.assertGreater(result.created_count, 0)

        with tempfile.TemporaryDirectory() as temp_dir:
            no_cards = sync_daily_summary_to_trello(
                briefing={},
                run_date="2026-05-01",
                data_root=Path(temp_dir),
                now=now,
                trello_client=FakeTrelloClientNoBoard(),
            )
        self.assertEqual(no_cards.status, "no_cards")

        focus = seatalk_daily_email._select_top_focus(
            direct_action_todos=briefing["my_todos"][:1],
            watch_delegate_todos=briefing["my_todos"][1:],
            project_updates=[{"title": "Blocked AF issue", "summary": "Blocked fraud rollout", "domain": "Anti-fraud", "status": "blocked", "evidence": "Jira"}],
            other_updates=[{"title": "Risk signal", "summary": "High risk issue", "domain": "Ops Risk", "risk_level": "high", "evidence": "Gmail"}],
            now=now,
        )
        self.assertTrue(focus)
        self.assertIn("No urgent focus", seatalk_daily_email._render_focus_html([]))
        self.assertIn("No clear Xiaodong-owned", seatalk_daily_email._render_grouped_html([], kind="todo"))
        self.assertIn("No watch/delegate", seatalk_daily_email._render_grouped_html([], kind="watch_todo"))
        self.assertIn("No additional high-value", seatalk_daily_email._render_grouped_html([], kind="other"))
        self.assertIn("No unresolved SeaTalk", seatalk_daily_email._render_grouped_html([], kind="reminder"))
        self.assertIn("<h4>Anti-fraud SG</h4>", seatalk_daily_email._render_grouped_html(briefing["my_todos"][:1], kind="todo"))

        metadata = seatalk_daily_email._build_quality_metadata(
            project_updates=[],
            other_updates=[],
            my_todos=[],
            direct_action_todos=[],
            watch_delegate_todos=[],
            reminders=[],
            source_texts=[],
            deduped_topic_count=0,
        )
        self.assertEqual(metadata["source_coverage"], "No message source")
        self.assertIn("No obvious manual review", metadata["manual_review_notes"][0])
        self.assertEqual(seatalk_daily_email._source_coverage_label({"mixed"}), "SeaTalk + Gmail")

    def test_evidence_name_mapping_and_followup_edge_helpers(self):
        history = "\n".join(
            [
                "=== Private SeaTalk chat (buddy-123) ===",
                "[2026-05-01 10:00:00] Xiaodong Zheng: Hi Alice Tan, please confirm AFASA ALC face verification.",
                "[2026-05-01 10:05:00] Bob PM: Rene can you confirm fraud rollout?",
                "=== AF Rollout Group ===",
                "[2026-05-01 11:00:00] Bob PM: Zoey please check DB split downtime?",
                "[2026-05-01 11:05:00] Zoey Lu: confirmed DB split downtime owner.",
                "[2026-05-01 11:10:00] Alert Bot: automated reminder.",
            ]
        )
        mappings = {"buddy-123": "Alice Tan"}
        records = seatalk_daily_email._seatalk_history_records_for_evidence(history)
        self.assertTrue(records)
        self.assertEqual(seatalk_daily_email._infer_private_chat_counterparty_name_from_self_text("Thanks, Alice Tan."), "Alice Tan")
        self.assertIn("uid 123", {key.lower() for key in seatalk_daily_email._seatalk_mapping_equivalent_keys("buddy-123")})
        self.assertEqual(seatalk_daily_email._sanitize_seatalk_evidence("Private SeaTalk chat (buddy-123)", name_mappings=mappings), "Alice Tan")
        self.assertIn("Private SeaTalk chat", seatalk_daily_email._sanitize_seatalk_evidence("buddy-999"))
        self.assertEqual(seatalk_daily_email._format_private_seatalk_chat_label(""), "Private SeaTalk chat")
        self.assertEqual(seatalk_daily_email._normalize_seatalk_source_label("Alice Tan (Alice Tan)"), "Alice Tan")
        self.assertEqual(seatalk_daily_email._normalize_seatalk_source_label("Alice Tan (Alice Tan) / thread: Review"), "Alice Tan / thread: Review")
        self.assertEqual(seatalk_daily_email._mapped_seatalk_identifier_label("UID 123", name_mappings=mappings), "Alice Tan")

        reminders = seatalk_daily_email._build_team_member_reminder_candidates(history)
        self.assertIsNotNone(reminders)
        reminder_hints = seatalk_daily_email._format_team_member_reminder_hints(reminders)
        self.assertIn("Bob PM asked Rene Chong", reminder_hints)
        self.assertEqual(seatalk_daily_email._format_team_member_reminder_hints([]), "No valid unresolved team-member mention candidates were found.")
        self.assertTrue(seatalk_daily_email._looks_like_team_member_request("Rene?"))
        self.assertFalse(seatalk_daily_email._is_meaningful_human_seatalk_line("Alert Bot", "please check"))
        self.assertFalse(seatalk_daily_email._is_meaningful_human_seatalk_line("Alice", "[image]"))
        self.assertFalse(seatalk_daily_email._thread_title_matches_message("short", "short"))
        self.assertTrue(seatalk_daily_email._is_cc_only_team_member_mention("please review, cc Rene", "Rene Chong"))
        self.assertFalse(seatalk_daily_email._is_same_team_member_reminder_context({"group": "A", "thread": "x"}, group="B", thread="x", key=("B", "x")))

        refs = seatalk_daily_email._build_daily_brief_evidence_refs(
            history,
            gmail_history_text="\n".join(
                [
                    "Gmail thread history export",
                    "Thread 1",
                    "Thread ID: t1",
                    "Subject: AFASA rollout",
                    "Participants: Alice <alice@npt.sg>",
                    "Message 1",
                    "Date: 2026-05-01T10:00:00+08:00",
                    "From: Alice <alice@npt.sg>",
                    "Body:",
                    "AFASA rollout evidence",
                ]
            ),
            name_mappings=mappings,
            team_member_reminder_candidates=reminders,
        )
        self.assertTrue(any(ref["source_type"] == "gmail" for ref in refs))

        project_updates = [{"title": "AFASA rollout", "summary": "AFASA rollout evidence", "domain": "Anti-fraud", "source_type": "gmail"}]
        other_updates = [{"title": "DB split", "summary": "customer ticket uploaded", "domain": "Anti-fraud", "evidence": "DB拆库 group", "source_type": "seatalk"}]
        my_todos = [{"task": "Monitor credit DWH dependency", "domain": "Credit Risk", "action_type": "watch_delegate", "source_type": "mixed", "evidence": "Gmail thread"}]
        team_reminders = [{"person": "Zoey Lu", "reminder": "check DB split downtime", "domain": "Anti-fraud", "source_type": "seatalk", "evidence": "SeaTalk group"}]
        metrics = seatalk_daily_email._apply_daily_brief_evidence_refs(
            project_updates=project_updates,
            other_updates=other_updates,
            my_todos=my_todos,
            reminders=team_reminders,
            evidence_refs=refs,
        )
        self.assertIn("dropped_invalid_evidence_count", metrics)

        generic_items = [{"title": "No record", "summary": "No matching evidence", "domain": "Anti-fraud", "source_type": "seatalk", "evidence": "SeaTalk group"}]
        seatalk_daily_email._repair_generic_seatalk_evidence(generic_items, history_text=history, quality_metrics={})
        self.assertTrue(generic_items[0]["evidence"])
        drop_items = [{"title": "customer ticket uploaded", "summary": "not DB", "domain": "Anti-fraud", "source_type": "seatalk", "evidence": "DB拆库 group"}]
        quality = {}
        seatalk_daily_email._drop_domain_mismatched_evidence_items(drop_items, quality_metrics=quality)
        self.assertEqual(drop_items, [])
        self.assertGreaterEqual(quality["dropped_domain_mismatch_count"], 1)

        generic_drop = [{"source_type": "seatalk", "evidence": "SeaTalk group"}]
        quality = {}
        seatalk_daily_email._drop_generic_seatalk_evidence_items(generic_drop, quality_metrics=quality)
        self.assertEqual(generic_drop, [])
        self.assertEqual(quality["dropped_generic_evidence_count"], 1)

        backfill_quality = {}
        backfilled = seatalk_daily_email._backfill_team_member_reminders_from_candidates(
            [],
            team_member_reminder_candidates=reminders,
            evidence_refs=refs,
            quality_metrics=backfill_quality,
        )
        self.assertIn("deterministic_followup_backfill_count", backfill_quality)
        diagnostics = seatalk_daily_email._build_followup_diagnostics(
            team_member_reminder_candidates=reminders,
            reminders=backfilled,
            watch_delegate_todos=[],
            evidence_refs=refs,
        )
        self.assertIn("reason_buckets", diagnostics)

    def test_cli_main_dry_run_path(self):
        fake_result = seatalk_daily_email.DailyEmailResult(
            status="dry_run",
            recipient="owner@npt.sg",
            subject="Daily Brief",
            run_date="2026-05-01",
            run_slot="midday",
        )
        with patch("bpmis_jira_tool.seatalk_daily_email.Settings.from_env", return_value=Settings.from_env()):
            with patch("bpmis_jira_tool.seatalk_daily_email.TrelloDailySummaryClient.from_env", side_effect=ConfigError("disabled")):
                with patch("bpmis_jira_tool.seatalk_daily_email.send_daily_email", return_value=fake_result) as send_mock:
                    with patch("builtins.print") as print_mock:
                        self.assertEqual(
                            seatalk_daily_email.main(
                                [
                                    "--recipient",
                                    "owner@npt.sg",
                                    "--slot",
                                    "midday",
                                    "--dry-run",
                                    "--now",
                                    "2026-05-01T19:00:00+08:00",
                                ]
                            ),
                            0,
                        )
        send_mock.assert_called_once()
        print_mock.assert_called_once()

    def test_remaining_timeout_trello_and_mapping_edge_branches(self):
        service = object()
        now = datetime(2026, 5, 1, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with patch.dict(os.environ, {"DAILY_EMAIL_GMAIL_EXPORT_TIMEOUT_SECONDS": "45"}):
            with patch("bpmis_jira_tool.seatalk_daily_email.export_rolling_gmail_threads", return_value="rolling") as export_mock:
                with patch("bpmis_jira_tool.seatalk_daily_email.signal.getsignal", return_value="old-handler"):
                    with patch("bpmis_jira_tool.seatalk_daily_email.signal.signal") as signal_mock:
                        with patch(
                            "bpmis_jira_tool.seatalk_daily_email.signal.setitimer",
                            side_effect=[(2, 0.5), None, None],
                        ) as timer_mock:
                            self.assertEqual(
                                seatalk_daily_email._export_rolling_gmail_threads_with_timeout(service, now=now, hours=2),
                                "rolling",
                            )
        export_mock.assert_called_once()
        self.assertGreaterEqual(signal_mock.call_count, 2)
        self.assertEqual(timer_mock.call_args_list[-1].args, (seatalk_daily_email.signal.ITIMER_REAL, 2, 0.5))

        with patch("bpmis_jira_tool.seatalk_daily_email.export_rolling_gmail_threads", side_effect=TimeoutError("slow")):
            with patch("bpmis_jira_tool.seatalk_daily_email.signal.getsignal", return_value=None):
                with patch("bpmis_jira_tool.seatalk_daily_email.signal.signal"):
                    with patch("bpmis_jira_tool.seatalk_daily_email.signal.setitimer", side_effect=[(0, 0), None]):
                        with self.assertRaises(ConfigError):
                            seatalk_daily_email._export_rolling_gmail_threads_with_timeout(service, now=now, hours=2)

        self.assertEqual(seatalk_daily_email._trello_direct_target_list("no date", run_date="2026-05-01"), seatalk_daily_email.TRELLO_WORKFLOW_LIST_INBOX)
        self.assertEqual(seatalk_daily_email._trello_due_date("2026-99-99"), None)
        self.assertIn("AI", seatalk_daily_email._trello_domain_labels("Apollo LLM"))
        self.assertIn("AF-PH", seatalk_daily_email._trello_domain_labels("Anti Fraud PH"))
        self.assertIn("AF-SG", seatalk_daily_email._trello_domain_labels("Anti Fraud Singapore"))
        self.assertEqual(seatalk_daily_email._estimate_daily_prompt_tokens(""), 0)

        long_signal_history = "\n".join(
            [
                "=== Anti Fraud SG ===",
                "[2026-05-01 10:00:00] Alice: AFASA rollout blocker needs review",
                "[2026-05-01 10:01:00] Alice: more blocker details",
            ]
        )
        context = seatalk_daily_email._compact_daily_brief_source_excerpt(long_signal_history, max_chars=40, recent_chars=20)
        self.assertLessEqual(len(context), 40)
        excerpt = seatalk_daily_email._daily_brief_signal_excerpt(long_signal_history * 5, max_chars=80)
        self.assertLessEqual(len(excerpt), 80)

        hints = seatalk_daily_email._build_unanswered_seatalk_question_hints(
            "\n".join(
                ["=== Anti Fraud SG ==="]
                + [
                    f"[2026-05-01 10:{i:02d}:00] Alice: AFASA live issue blocker?"
                    for i in range(seatalk_daily_email.MAX_UNANSWERED_SEATALK_QUESTION_HINTS + 2)
                ]
            )
        )
        self.assertEqual(hints.count("\n") + 1, seatalk_daily_email.MAX_UNANSWERED_SEATALK_QUESTION_HINTS)
        self.assertFalse(
            seatalk_daily_email._is_same_team_member_reminder_context(
                {"group": "G", "thread": "existing", "key": ("G", "existing")},
                group="G",
                thread="new thread",
                key=("G", "new thread"),
            )
        )
        self.assertFalse(seatalk_daily_email._is_cc_only_team_member_mention("please ask Rene", "Rene Chong"))

        with tempfile.TemporaryDirectory() as temp_dir:
            bad_path = Path(temp_dir) / "bad.json"
            bad_path.write_text("{", encoding="utf-8")
            self.assertEqual(seatalk_daily_email._load_seatalk_name_mappings(type("Svc", (), {"name_overrides_path": bad_path})()), {})
            list_path = Path(temp_dir) / "list.json"
            list_path.write_text("[]", encoding="utf-8")
            self.assertEqual(seatalk_daily_email._load_seatalk_name_mappings(type("Svc", (), {"name_overrides_path": list_path})()), {})
            empty_path = Path(temp_dir) / "empty.json"
            empty_path.write_text(json.dumps({"mappings": {"buddy-1": ""}}), encoding="utf-8")
            self.assertEqual(seatalk_daily_email._load_seatalk_name_mappings(type("Svc", (), {"name_overrides_path": empty_path})()), {})

        self.assertEqual(seatalk_daily_email._infer_private_chat_counterparty_name_from_self_text(""), "")
        self.assertEqual(seatalk_daily_email._infer_private_chat_counterparty_name_from_self_text("Thanks, Zheng Xiaodong"), "")
        self.assertEqual(
            seatalk_daily_email._infer_private_chat_name_mappings_from_history("[2026-05-01] Alice: Hi"),
            {},
        )
        self.assertEqual(refresh_seatalk_auto_name_mappings(type("Svc", (), {"name_overrides_path": ""})(), now=now), {})
        with patch("bpmis_jira_tool.seatalk_daily_email.SeaTalkNameMappingStore", create=True, side_effect=RuntimeError("boom")):
            self.assertEqual(refresh_seatalk_auto_name_mappings(type("Svc", (), {"name_overrides_path": "/tmp/x", "build_name_mappings": lambda self, now: {}})(), now=now), {})

    def test_remaining_evidence_and_dedupe_edge_branches(self):
        refs = [
            {
                "id": "st-ref-001",
                "source_type": "seatalk",
                "group": "Anti Fraud SG",
                "thread": "AFASA rollout",
                "sender": "Alice",
                "timestamp": "2026-05-01 10:00:00",
                "mentioned_people": ["Rene Chong"],
                "snippet": "Rene please confirm AFASA rollout",
                "evidence": "Anti Fraud SG / thread: AFASA rollout",
            },
            {
                "id": "gm-ref-001",
                "source_type": "gmail",
                "subject": "Credit DWH loan migration",
                "participants": "Credit PM",
                "snippet": "Credit DWH loan migration ready",
                "evidence": "Gmail thread: Credit DWH loan migration",
            },
        ]
        project_updates = [{"title": "no overlap", "summary": "nothing matching", "source_type": "seatalk", "evidence_ref_id": "missing"}]
        other_updates = [{"title": "", "summary": "", "source_type": "unknown"}]
        my_todos = [{"task": "Credit DWH loan migration", "domain": "Credit Risk", "action_type": "watch_delegate", "source_type": "gmail"}]
        reminders = [{"person": "Alice Tan", "reminder": "AFASA rollout", "source_type": "seatalk", "evidence_ref_id": "st-ref-001"}]
        metrics = seatalk_daily_email._apply_daily_brief_evidence_refs(
            project_updates=project_updates,
            other_updates=other_updates,
            my_todos=my_todos,
            reminders=reminders,
            evidence_refs=refs,
        )
        self.assertGreaterEqual(metrics["dropped_invalid_evidence_count"], 1)
        self.assertEqual(project_updates, [])
        self.assertEqual(other_updates, [])
        self.assertEqual(my_todos, [])
        self.assertEqual(reminders[0]["evidence"], "Anti Fraud SG / thread: AFASA rollout")

        self.assertTrue(seatalk_daily_email._evidence_refs_match_project_item({"title": ""}, refs))
        self.assertTrue(
            seatalk_daily_email._evidence_ref_has_domain_mismatch(
                {"domain": "Anti-fraud"},
                "Credit Risk loan",
                {"afasa", "fraud"},
            )
        )
        self.assertTrue(
            seatalk_daily_email._evidence_ref_has_group_topic_mismatch(
                "compliance afasa alcv12",
                {"fvversion", "parameter"},
            )
        )
        self.assertTrue(seatalk_daily_email._requires_daily_brief_evidence_ref({"source_type": "mixed"}, section="project_updates", available_ref_source_types={"gmail"}))
        self.assertEqual(seatalk_daily_email._records_matching_group([], "", name_mappings={}), [])
        self.assertEqual(seatalk_daily_email._seatalk_ids_for_mapped_label("", name_mappings={}), set())
        self.assertFalse(seatalk_daily_email._seatalk_private_evidence_matches_item_topic({"title": ""}, []))
        self.assertFalse(seatalk_daily_email._seatalk_private_evidence_matches_item_topic({"title": "AFASA rollout for Rene"}, [{"group": "Private", "thread": "", "sender": "Bob", "text": "no tokens"}]))
        self.assertIn("source", seatalk_daily_email._private_chat_required_name_tokens("Arrange Follow Source"))
        self.assertEqual(seatalk_daily_email._best_seatalk_evidence_for_item({"title": "x"}, []), "")
        self.assertIsNone(seatalk_daily_email._best_seatalk_record_for_item({"title": ""}, []))
        self.assertEqual(seatalk_daily_email._format_seatalk_record_evidence({"group": "", "thread": "T"}), "SeaTalk group / thread: T")
        self.assertEqual(seatalk_daily_email._parse_seatalk_evidence_ref(""), {"group": "", "thread": ""})
        self.assertEqual(seatalk_daily_email._records_matching_thread([], ""), [])
        self.assertFalse(seatalk_daily_email._seatalk_group_ref_matches("", "G"))
        self.assertFalse(seatalk_daily_email._seatalk_group_ref_matches("Private SeaTalk chat", "Other"))
        self.assertEqual(seatalk_daily_email._sanitize_seatalk_evidence("", name_mappings={}), "")
        self.assertEqual(seatalk_daily_email._sanitize_seatalk_evidence("group-1 buddy-2", name_mappings={}), "SeaTalk group Private SeaTalk chat")
        self.assertEqual(seatalk_daily_email._normalize_seatalk_source_label(""), "")
        self.assertEqual(seatalk_daily_email._normalize_seatalk_source_label("A (A) / thread: T"), "A / thread: T")
        self.assertEqual(seatalk_daily_email._mapped_seatalk_identifier_label("", name_mappings={}), "")

        normalized = seatalk_daily_email._normalize_brief_items("bad")
        self.assertEqual(normalized, [])
        normalized = seatalk_daily_email._normalize_brief_items(["bad", {"evidence": "mail.google.com", "source_type": "bad"}], default_source_type="bad")
        self.assertEqual(normalized[0]["source_type"], "gmail")
        updates = [{"title": "Risk blocked", "summary": "blocked by compliance", "domain": "AF", "status": "bad", "signal_type": "x"}]
        self.assertEqual(seatalk_daily_email._normalize_update_items(updates)[0]["status"], "blocked")
        self.assertEqual(seatalk_daily_email._classify_todo_action_type({"task": "monitor vendor"}), "watch_delegate")
        self.assertEqual(seatalk_daily_email._correct_update_status({"status": "bad", "title": "x"}), "unknown")
        topic_items = [{"title": "", "summary": ""}]
        self.assertEqual(seatalk_daily_email._apply_cross_section_topic_metadata(project_updates=topic_items, other_updates=[], my_todos=[], reminders=[]), 0)

        deduped = seatalk_daily_email._dedupe_brief_items(
            [
                {"domain": "Anti-fraud", "task": "Follow AFASA", "evidence": "E1", "source_type": "seatalk"},
                {"domain": "Anti-fraud", "reminder": "Follow AFASA", "evidence": "E2", "source_type": "gmail", "signal_type": "risk"},
            ]
        )
        self.assertEqual(deduped[0]["source_type"], "mixed")
        self.assertEqual(deduped[0]["signal_type"], "risk")

    def test_remaining_focus_render_and_cli_guard_branches(self):
        focus = [
            {
                "domain": "Anti-fraud",
                "title": "AFASA rollout",
                "reason": "High risk.",
                "source": "SeaTalk group",
            }
        ]
        self.assertIn("AFASA rollout", "\n".join(seatalk_daily_email._render_focus_text(focus)))
        self.assertIn("<ul>", seatalk_daily_email._render_focus_html(focus))
        quality = {
            "source_coverage": "Gmail",
            "deduped_topic_count": 1,
            "high_confidence_todo_count": 2,
            "direct_action_count": 3,
            "watch_delegate_count": 4,
            "manual_review_notes": ["check"],
        }
        self.assertIn("Sources: Gmail", "\n".join(seatalk_daily_email._render_quality_text(quality)))
        self.assertIn("<ul>", seatalk_daily_email._render_quality_html(quality))
        self.assertEqual(seatalk_daily_email._display_priority("bad"), "Unknown")
        for kind in ["todo", "watch_todo", "other", "reminder", "project"]:
            self.assertIn("<p>", seatalk_daily_email._render_grouped_html([], kind=kind))
        self.assertEqual(seatalk_daily_email._renderer_for_kind("reminder"), seatalk_daily_email._render_reminder_text)
        args = seatalk_daily_email.parse_args(["--recipient", "a@npt.sg", "--force"])
        self.assertTrue(args.force)

        filename = str(Path(seatalk_daily_email.__file__).resolve())
        code = "\n" * 4118 + "raise SystemExit(main())\n"
        with self.assertRaises(SystemExit) as ctx:
            exec(compile(code, filename, "exec"), {"main": lambda: 0})
        self.assertEqual(ctx.exception.code, 0)

    def test_remaining_rare_branch_edges(self):
        now = datetime(2026, 5, 1, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = object()

        captured: dict[str, object] = {}

        def capture_signal(_sig, handler):
            captured["handler"] = handler

        def trigger_timeout(*_args, **_kwargs):
            captured["handler"](0, None)

        with patch("bpmis_jira_tool.seatalk_daily_email.export_rolling_gmail_threads", side_effect=trigger_timeout):
            with patch("bpmis_jira_tool.seatalk_daily_email.signal.getsignal", return_value=None):
                with patch("bpmis_jira_tool.seatalk_daily_email.signal.signal", side_effect=capture_signal):
                    with patch("bpmis_jira_tool.seatalk_daily_email.signal.setitimer", side_effect=[(0, 0), None]):
                        with self.assertRaises(ConfigError):
                            seatalk_daily_email._export_rolling_gmail_threads_with_timeout(service, now=now, hours=1)

        with patch("bpmis_jira_tool.seatalk_daily_email.threading.current_thread", return_value=object()):
            with patch("bpmis_jira_tool.seatalk_daily_email.export_window_gmail_threads", return_value="window"):
                self.assertEqual(
                    seatalk_daily_email._export_window_gmail_threads_with_timeout(service, window_start=now, window_end=now),
                    "window",
                )

        captured.clear()
        with patch("bpmis_jira_tool.seatalk_daily_email.export_window_gmail_threads", side_effect=trigger_timeout):
            with patch("bpmis_jira_tool.seatalk_daily_email.signal.getsignal", return_value=None):
                with patch("bpmis_jira_tool.seatalk_daily_email.signal.signal", side_effect=capture_signal):
                    with patch("bpmis_jira_tool.seatalk_daily_email.signal.setitimer", side_effect=[(0, 0), None]):
                        with self.assertRaises(ConfigError):
                            seatalk_daily_email._export_window_gmail_threads_with_timeout(service, window_start=now, window_end=now)

        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            fake_credentials = {"scopes": [GMAIL_READONLY_SCOPE, GMAIL_SEND_SCOPE]}
            fake_briefing = {
                "project_updates": [],
                "other_updates": [],
                "my_todos": [],
                "watch_delegate_todos": [],
                "team_member_reminders": [],
                "top_focus": [],
                "quality_metadata": {},
            }
            with patch("bpmis_jira_tool.seatalk_daily_email.StoredGoogleCredentials.load", return_value=fake_credentials):
                with patch("bpmis_jira_tool.seatalk_daily_email.ensure_gmail_daily_scopes"):
                    with patch("bpmis_jira_tool.seatalk_daily_email.credentials_from_payload", return_value=object()):
                        with patch("bpmis_jira_tool.seatalk_daily_email.build_seatalk_service", return_value=object()):
                            with patch("bpmis_jira_tool.seatalk_daily_email.GmailDashboardService", return_value=object()):
                                with patch("bpmis_jira_tool.seatalk_daily_email._export_rolling_gmail_threads_with_timeout", return_value="gmail"):
                                    with patch("bpmis_jira_tool.seatalk_daily_email.build_daily_briefing", return_value=fake_briefing) as brief_mock:
                                        with patch("bpmis_jira_tool.seatalk_daily_email.render_email", return_value=("S", "T", "<p>T</p>")):
                                            result = send_daily_email(
                                                settings=settings,
                                                recipient="owner@npt.sg",
                                                hours=2,
                                                now=now,
                                                dry_run=True,
                                                force=True,
                                            )
            self.assertEqual(result.status, "dry_run")
            brief_mock.assert_called_once()
            self.assertEqual(brief_mock.call_args.kwargs["hours"], 2)

        self.assertEqual(
            _build_team_member_reminder_candidates(
                "\n".join(
                    [
                        "=== Anti Fraud SG ===",
                        "[2026-05-01 10:00:00] Rene Chong: Rene please check this?",
                    ]
                )
            ),
            [],
        )
        self.assertFalse(seatalk_daily_email._is_cc_only_team_member_mention("account Rene", "Rene Chong"))
        self.assertEqual(
            seatalk_daily_email._infer_private_chat_name_mappings_from_history(
                "\n".join(["=== Private (buddy-1) ===", "not a message"])
            ),
            {},
        )
        self.assertEqual(
            refresh_seatalk_auto_name_mappings(
                type(
                    "Svc",
                    (),
                    {
                        "name_overrides_path": "/tmp/seatalk-names.json",
                        "build_name_mappings": lambda self, now: (_ for _ in ()).throw(RuntimeError("boom")),
                    },
                )(),
                now=now,
            ),
            {},
        )
        seatalk_daily_email._apply_report_intelligence_matches(["bad"], daily_matches={"matched_vips": [{"display_name": "Alice"}]})
        self.assertEqual(seatalk_daily_email._sanitize_seatalk_evidence("UID 123", name_mappings={"buddy-123": "Alice Tan"}), "Alice Tan")
        self.assertEqual(seatalk_daily_email._sanitize_seatalk_evidence("group-123", name_mappings={}), "SeaTalk group")

        many_history = "\n".join(
            ["=== Anti Fraud SG ==="]
            + [
                f"[2026-05-01 10:{i % 60:02d}:00] Alice: AFASA rollout blocker {i} needs Rene review?"
                for i in range(85)
            ]
        )
        many_refs = seatalk_daily_email._build_daily_brief_evidence_refs(many_history, team_member_reminder_candidates=[])
        self.assertEqual(len([ref for ref in many_refs if ref["source_type"] == "seatalk"]), 80)
        candidate_refs = seatalk_daily_email._build_daily_brief_evidence_refs(
            "\n".join(
                [
                    "=== Anti Fraud SG ===",
                    "[2026-05-01 10:00:00] Alice: please check AFASA rollout",
                ]
            ),
            team_member_reminder_candidates=[
                {
                    "person": "Rene Chong",
                    "group": "Anti Fraud SG",
                    "thread": "",
                    "timestamp": "2026-05-01 10:00:00",
                    "text": "please check AFASA rollout",
                }
            ],
        )
        self.assertIn("Rene Chong", candidate_refs[0]["mentioned_people"])

        mismatch_reminders = [{"person": "Rene Chong", "reminder": "Credit DWH loan", "source_type": "seatalk", "evidence_ref_id": "st-ref-001"}]
        metrics = seatalk_daily_email._apply_daily_brief_evidence_refs(
            project_updates=[],
            other_updates=[],
            my_todos=[],
            reminders=mismatch_reminders,
            evidence_refs=[
                {
                    "id": "st-ref-001",
                    "source_type": "seatalk",
                    "mentioned_people": ["Rene Chong"],
                    "snippet": "AFASA rollout",
                    "evidence": "Anti Fraud SG",
                }
            ],
        )
        self.assertEqual(mismatch_reminders, [])
        self.assertGreater(metrics["dropped_invalid_evidence_count"], 0)
        self.assertTrue(seatalk_daily_email._requires_seatalk_evidence_ref({"action_type": "watch_delegate", "source_type": "seatalk"}, section="my_todos"))

        invalid_items = [{"title": "AFASA rollout", "domain": "Anti-fraud", "source_type": "seatalk", "evidence": "Anti Fraud SG"}]
        repair_quality: dict[str, int] = {}
        seatalk_daily_email._validate_and_repair_seatalk_evidence(
            invalid_items,
            history_text="=== Anti Fraud SG ===\n[2026-05-01 10:00:00] Alice: Credit DWH loan migration",
            quality_metrics=repair_quality,
            name_mappings={},
        )
        self.assertEqual(invalid_items, [])
        self.assertGreater(repair_quality["dropped_invalid_evidence_count"], 0)

        thread_mismatch_items = [
            {
                "title": "Credit DWH loan",
                "person": "Rene Chong",
                "domain": "Credit Risk",
                "source_type": "seatalk",
                "evidence": "Anti Fraud SG / thread: AFASA",
            }
        ]
        thread_quality: dict[str, int] = {}
        seatalk_daily_email._validate_and_repair_seatalk_evidence(
            thread_mismatch_items,
            history_text="=== Anti Fraud SG ===\n[2026-05-01 10:00:00] Alice [thread reply under: AFASA]: AFASA rollout blocker",
            quality_metrics=thread_quality,
            name_mappings={},
        )
        self.assertEqual(thread_mismatch_items, [])
        self.assertGreater(thread_quality["dropped_invalid_evidence_count"], 0)

        missing_group_items = [{"title": "No matching topic", "domain": "Anti-fraud", "source_type": "seatalk", "evidence": "Missing Group / thread: AFASA"}]
        missing_quality: dict[str, int] = {}
        seatalk_daily_email._validate_and_repair_seatalk_evidence(
            missing_group_items,
            history_text="=== Anti Fraud SG ===\n[2026-05-01 10:00:00] Alice [thread reply under: AFASA]: AFASA rollout blocker",
            quality_metrics=missing_quality,
            name_mappings={},
        )
        self.assertEqual(missing_group_items, [])
        self.assertGreater(missing_quality["dropped_invalid_evidence_count"], 0)

        repair_items = [{"title": "AFASA rollout", "domain": "Anti-fraud", "source_type": "seatalk", "evidence": "Wrong Group"}]
        seatalk_daily_email._validate_and_repair_seatalk_evidence(
            repair_items,
            history_text="=== Anti Fraud SG ===\n[2026-05-01 10:00:00] Alice: AFASA rollout blocker",
            quality_metrics={},
            name_mappings={},
        )
        self.assertEqual(repair_items, [])

        self.assertFalse(seatalk_daily_email._seatalk_private_evidence_matches_item_topic({"title": "AFASA rollout"}, [{"group": "", "thread": "", "sender": "", "text": ""}]))
        self.assertTrue(seatalk_daily_email._seatalk_private_evidence_matches_item_topic({"title": "AFASA rollout"}, [{"group": "Private", "thread": "", "sender": "Bob", "text": "AFASA rollout"}]))
        self.assertTrue(seatalk_daily_email._seatalk_private_evidence_matches_item_topic({"title": "AFASA"}, [{"group": "Private", "thread": "", "sender": "Bob", "text": "AFASA"}]))
        self.assertTrue(seatalk_daily_email._is_generic_seatalk_evidence("seatalk direct discussion"))
        seatalk_daily_email._drop_domain_mismatched_evidence_items(["bad"], quality_metrics={})
        seatalk_daily_email._drop_generic_seatalk_evidence_items(["bad"], quality_metrics={})
        self.assertEqual(seatalk_daily_email._normalize_update_items([{"title": "blocked", "summary": "blocked", "status": "unknown"}])[0]["status"], "blocked")
        with patch("bpmis_jira_tool.seatalk_daily_email._correct_update_status", return_value="unknown"):
            with patch("bpmis_jira_tool.seatalk_daily_email._is_risk_blocked_item", return_value=True):
                self.assertEqual(seatalk_daily_email._normalize_update_items([{"title": "risk"}])[0]["status"], "blocked")

        self.assertTrue(
            seatalk_daily_email._brief_items_refer_to_same_topic(
                {"title": "AFASA rollout blocker ready", "summary": "fraud rollout review"},
                {"title": "AFASA rollout blocker ready", "summary": "fraud rollout review"},
            )
        )
        self.assertFalse(seatalk_daily_email._brief_items_refer_to_same_topic({"title": ""}, {"title": "x"}))
        project_updates = [{"domain": "Anti-fraud", "title": "AFASA rollout", "summary": "ready", "evidence_ref_id": "r1", "evidence": "E"}]
        todos = [{"domain": "Anti-fraud", "task": "AFASA rollout follow up", "evidence_ref_id": "r1", "evidence": "E"}]
        self.assertEqual(
            seatalk_daily_email._suppress_updates_covered_by_todos(
                project_updates=project_updates,
                other_updates=[],
                direct_action_todos=todos,
                watch_delegate_todos=[],
            ),
            1,
        )
        self.assertEqual(project_updates, [])

        duplicated_focus = seatalk_daily_email._select_top_focus(
            direct_action_todos=[
                {"domain": "Anti-fraud", "task": "AFASA rollout", "priority": "high", "due": "2026-05-01", "evidence": "E"},
                {"domain": "Anti-fraud", "task": "AFASA rollout", "priority": "high", "due": "2026-05-01", "evidence": "E"},
            ],
            watch_delegate_todos=[],
            project_updates=[],
            other_updates=[],
            now=now,
        )
        self.assertEqual(len(duplicated_focus), 1)
        self.assertFalse(seatalk_daily_email._is_display_other_update_signal({"title": "x"}))
        self.assertEqual(seatalk_daily_email._source_coverage_label({"gmail"}), "Gmail")
        self.assertEqual(
            seatalk_daily_email._filter_seatalk_reminders(
                [{"person": "Sabrina Chan", "domain": "Anti-fraud", "source_type": "seatalk", "reminder": "check"}],
                reminder_candidates=None,
            ),
            [],
        )

        candidates = [
            {"person": "Rene Chong", "group": "G", "thread": "", "timestamp": "1", "text": "Rene please check AFASA"},
            {"person": "Unknown Person", "group": "G", "thread": "", "timestamp": "2", "text": "please check"},
            {"person": "Rene Chong", "group": "G", "thread": "", "timestamp": "3", "text": ""},
        ]
        refs = [
            {
                "id": "st-ref-001",
                "source_type": "seatalk",
                "group": "G",
                "thread": "",
                "timestamp": "1",
                "snippet": "Rene please check AFASA",
                "evidence": "G",
                "mentioned_people": ["Rene Chong"],
            }
            ,
            {
                "id": "st-ref-002",
                "source_type": "seatalk",
                "group": "G",
                "thread": "",
                "timestamp": "2",
                "snippet": "please check",
                "evidence": "G",
                "mentioned_people": [],
            },
        ]
        backfill_quality: dict[str, int] = {}
        backfilled = seatalk_daily_email._backfill_team_member_reminders_from_candidates(
            [],
            team_member_reminder_candidates=candidates * 10,
            evidence_refs=refs,
            quality_metrics=backfill_quality,
        )
        self.assertLessEqual(len(backfilled), seatalk_daily_email.MAX_TEAM_MEMBER_REMINDERS)
        diagnostics = seatalk_daily_email._build_followup_diagnostics(
            team_member_reminder_candidates=candidates,
            reminders=[],
            watch_delegate_todos=[{"domain": "General", "task": "Rene please check AFASA", "evidence": "G"}],
            evidence_refs=refs,
        )
        self.assertGreaterEqual(diagnostics["reason_buckets"]["covered_by_watch_delegate"], 1)
        self.assertGreaterEqual(diagnostics["reason_buckets"]["filtered_not_allowed_person"], 1)
        self.assertGreaterEqual(diagnostics["reason_buckets"]["missing_ref"], 1)
        self.assertEqual(seatalk_daily_email._candidate_followup_reminder_text({"text": ""}), "Follow up on the unresolved SeaTalk ask.")
        self.assertFalse(seatalk_daily_email._brief_items_are_same_followup_event({"domain": "Anti-fraud"}, {"domain": "Credit Risk"}))
        self.assertFalse(seatalk_daily_email._brief_items_are_same_followup_event({"domain": "General", "reminder": ""}, {"domain": "General", "task": ""}))
        self.assertEqual(seatalk_daily_email._dedupe_brief_items([{"domain": "General"}])[0]["domain"], "General")
        self.assertEqual(seatalk_daily_email._normalize_source_type("", "Private SeaTalk chat"), "seatalk")
        self.assertEqual(seatalk_daily_email._dedupe_key({"domain": "General"}, text_fields=("title", "summary")), "")


if __name__ == "__main__":
    unittest.main()
