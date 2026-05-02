from __future__ import annotations

import base64
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from cryptography.fernet import Fernet

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE
from bpmis_jira_tool.gmail_sender import (
    GMAIL_SEND_SCOPE,
    StoredGoogleCredentials,
    build_gmail_raw_message,
    ensure_gmail_send_scope,
)
from bpmis_jira_tool.seatalk_daily_email import (
    DailyEmailRunStore,
    MAX_MY_TODOS,
    MAX_OTHER_UPDATES,
    MAX_PROJECT_UPDATES,
    MAX_TEAM_MEMBER_REMINDERS,
    MAX_USEFUL_AWARENESS_OTHER_UPDATES,
    build_daily_briefing,
    build_trello_card_specs,
    ensure_gmail_daily_scopes,
    export_rolling_history,
    export_rolling_gmail_threads,
    render_email,
    resolve_daily_email_window,
    send_daily_email,
    seatalk_name_overrides_path,
    sync_daily_summary_to_trello,
)
from bpmis_jira_tool.seatalk_dashboard import SEATALK_INSIGHTS_TIMEZONE
from bpmis_jira_tool.trello_daily_summary import TrelloDailySummaryClient, TrelloDailySummaryStore


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
                            "status": "done",
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
            NoisyBriefingService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n"),
            now=now,
            gmail_history_text="Gmail thread history export\nMessage 1\nBody:\nFYI\n",
        )

        self.assertEqual(len(payload["project_updates"]), 1)
        self.assertEqual(payload["project_updates"][0]["source_type"], "mixed")
        self.assertIn("Credit Risk group; Alice, CR rollout Gmail thread", payload["project_updates"][0]["evidence"])
        self.assertEqual(len(payload["other_updates"]), 1)
        self.assertEqual(payload["other_updates"][0]["signal_type"], "incident")
        self.assertEqual(len(payload["team_member_reminders"]), 1)
        self.assertEqual(payload["team_member_reminders"][0]["source_type"], "seatalk")

    def test_build_daily_briefing_allows_limited_useful_awareness_other_updates(self):
        class UsefulAwarenessService(FakeSeaTalkService):
            def _run_codex_insights_prompt(self, *, prompt, system_prompt):
                self.last_prompt = prompt
                useful_items = [
                    {
                        "domain": "General",
                        "title": f"Awareness {index}",
                        "summary": f"Potentially useful awareness item {index}.",
                        "status": "unknown",
                        "evidence": f"Thread {index}",
                        "source_type": "gmail",
                        "signal_type": "useful_awareness",
                    }
                    for index in range(1, 8)
                ]
                return None, {
                    "project_updates": [],
                    "other_updates": useful_items,
                    "team_member_reminders": [],
                    "my_todos": [],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = UsefulAwarenessService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n")
        payload = build_daily_briefing(
            service,
            now=now,
        )

        self.assertEqual(len(payload["other_updates"]), MAX_USEFUL_AWARENESS_OTHER_UPDATES)
        self.assertTrue(all(item["signal_type"] == "useful_awareness" for item in payload["other_updates"]))
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
        self.assertIn("include one concise reminder rather than dropping it", service.last_prompt)

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
                            "person": "Zoey",
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
            AliasReminderService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: Rene and Zoey please check\n"),
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
                            "evidence": "CrossTeam P2M x MSA/MCC",
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
                            "task": "Ensure PH follows up tomorrow on GRC audit-history access and audit-log expectations.",
                            "domain": "Ops Risk",
                            "priority": "high",
                            "due": "2026-04-30",
                            "evidence": "GRC evaluation group",
                            "source_type": "seatalk",
                        },
                    ],
                    "team_todos": [],
                }

        now = datetime(2026, 4, 29, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        service = ActionLayerService("SeaTalk Chat History Export\n[2026-04-29 18:30:00] Bob: please review\n")
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
        self.assertGreaterEqual(payload["quality_metadata"]["deduped_topic_count"], 1)
        self.assertIn("GRC evaluation group", payload["watch_delegate_todos"][0]["evidence"])
        self.assertIn("GRC evaluation group", payload["team_member_reminders"][0]["evidence"])

    def test_render_email_handles_empty_partial_and_full_sections(self):
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        subject, text_body, html_body = render_email(briefing={"my_todos": [], "project_updates": []}, now=now)
        self.assertEqual(subject, "Daily Brief - 2026-04-27")
        self.assertIn("No clear Xiaodong-owned to-do", text_body)
        self.assertIn("No clear project update", html_body)
        self.assertIn("No additional high-value awareness update", text_body)
        self.assertIn("No unresolved SeaTalk team-member mention", text_body)

        _, text_body, _ = render_email(
            briefing={"my_todos": [{"task": "Review", "domain": "General", "priority": "high", "due": "today", "evidence": "Alice"}]},
            now=now,
        )
        self.assertIn("General\n[High] Review. Due: today (Source: Alice)", text_body)
        self.assertIn("No clear project update", text_body)

        payload = build_daily_briefing(
            FakeSeaTalkService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n"),
            now=now,
        )
        _, text_body, html_body = render_email(briefing=payload, now=now)
        self.assertIn("Review rollout note", text_body)
        self.assertIn("Deck was refreshed. [Status: Done]", html_body)
        self.assertIn("Other Update", text_body)
        self.assertIn("A policy dependency may affect downstream rollout planning. [Status: In Progress]", html_body)
        self.assertIn("Suggested Team Follow-up", text_body)
        self.assertIn("Ker Yin: Please check whether the pending GRC confirmation still needs owner follow-up.", html_body)
        self.assertNotIn("Today Focus", text_body)
        self.assertNotIn("Today Focus", html_body)
        self.assertIn("Xiaodong Action Required", text_body)
        self.assertIn("Watch / Delegate", text_body)
        self.assertNotIn("Generation Quality", text_body)
        self.assertNotIn("Generation Quality", html_body)

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
        self.assertIn("Section: Watch / Delegate", specs[1].description)
        self.assertIn("Person: Rene Chong", specs[2].description)

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
        card = client.create_card(list_id=list_id, name="[Direct] Review", description="Report date: 2026-04-30")

        self.assertEqual(list_id, "list-1")
        self.assertEqual(card.url, "https://trello.test/card-1")
        self.assertEqual(session.posts[0]["params"]["idList"], "list-1")

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

    def test_trello_client_requires_env_config(self):
        with self.assertRaisesRegex(ConfigError, "TRELLO_API_KEY"):
            TrelloDailySummaryClient(api_key="", api_token="token", board_id="board")
        with self.assertRaisesRegex(ConfigError, "TRELLO_API_KEY"):
            TrelloDailySummaryClient(api_key="key", api_token="", board_id="board")
        with self.assertRaisesRegex(ConfigError, "TRELLO_API_KEY"):
            TrelloDailySummaryClient(api_key="key", api_token="token", board_id="")

    def test_daily_trello_sync_is_idempotent(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self):
                return "list-1"

            def create_card(self, *, list_id, name, description):
                self.created.append({"list_id": list_id, "name": name, "description": description})
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

    def test_daily_trello_sync_tracks_morning_and_midday_separately(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self):
                return "list-1"

            def create_card(self, *, list_id, name, description):
                self.created.append({"list_id": list_id, "name": name, "description": description})
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
                window_label="2026-04-29 13:00 - 2026-04-30 08:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )
            midday = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                run_slot="midday",
                window_label="2026-04-30 08:00 - 2026-04-30 13:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )
            midday_again = sync_daily_summary_to_trello(
                briefing=briefing,
                run_date="2026-04-30",
                run_slot="midday",
                window_label="2026-04-30 08:00 - 2026-04-30 13:00",
                data_root=Path(temp_dir),
                now=now,
                trello_client=client,
                trello_store=store,
            )

        self.assertEqual(morning.created_count, 1)
        self.assertEqual(midday.created_count, 1)
        self.assertEqual(midday_again.created_count, 0)
        self.assertEqual(midday_again.skipped_count, 1)
        self.assertIn("Report window: 2026-04-30 08:00 - 2026-04-30 13:00", client.created[1]["description"])

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

    def test_missing_gmail_send_scope_reports_reconnect(self):
        with self.assertRaisesRegex(ConfigError, "Reconnect Google"):
            ensure_gmail_send_scope({"scopes": ["https://www.googleapis.com/auth/gmail.readonly"]})

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

    def test_fixed_daily_email_windows_cover_8_to_13_and_13_to_next_8(self):
        midday = resolve_daily_email_window(
            now=datetime(2026, 4, 30, 13, 5, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            slot="auto",
        )
        self.assertEqual(midday.run_slot, "midday")
        self.assertEqual(midday.run_date, "2026-04-30")
        self.assertEqual(midday.start.isoformat(), "2026-04-30T08:00:00+08:00")
        self.assertEqual(midday.end.isoformat(), "2026-04-30T13:00:00+08:00")

        morning = resolve_daily_email_window(
            now=datetime(2026, 4, 30, 8, 5, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            slot="auto",
        )
        self.assertEqual(morning.run_slot, "morning")
        self.assertEqual(morning.run_date, "2026-04-30")
        self.assertEqual(morning.start.isoformat(), "2026-04-29T13:00:00+08:00")
        self.assertEqual(morning.end.isoformat(), "2026-04-30T08:00:00+08:00")

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

    def test_send_daily_email_syncs_trello_cards_before_marking_sent(self):
        class FakeTrelloClient:
            def __init__(self):
                self.created = []

            def get_or_create_list_id(self):
                return "list-1"

            def create_card(self, *, list_id, name, description):
                self.created.append({"list_id": list_id, "name": name, "description": description})
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


if __name__ == "__main__":
    unittest.main()
