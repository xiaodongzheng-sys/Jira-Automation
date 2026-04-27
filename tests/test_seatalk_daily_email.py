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
from bpmis_jira_tool.gmail_sender import (
    GMAIL_SEND_SCOPE,
    StoredGoogleCredentials,
    build_gmail_raw_message,
    ensure_gmail_send_scope,
)
from bpmis_jira_tool.seatalk_daily_email import (
    DailyEmailRunStore,
    build_daily_briefing,
    export_rolling_history,
    render_email,
    send_daily_email,
    seatalk_name_overrides_path,
)
from bpmis_jira_tool.seatalk_dashboard import SEATALK_INSIGHTS_TIMEZONE


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

    def export_history_since(self, *, since, now, days):
        self.calls.append({"since": since, "now": now, "days": days})
        return self.history

    def _filter_system_generated_history(self, value):
        return value

    def _compact_history_for_insights(self, value, **_kwargs):
        return value

    def _run_codex_insights_prompt(self, *, prompt, system_prompt):
        return None, {
            "project_updates": [
                {
                    "domain": "General",
                    "title": "AI sharing",
                    "summary": "Deck was refreshed.",
                    "status": "done",
                    "evidence": "19:00 Alice: deck ready",
                }
            ],
            "other_updates": [
                {
                    "domain": "Credit Risk",
                    "title": "Policy signal",
                    "summary": "A policy dependency may affect downstream rollout planning.",
                    "status": "in_progress",
                    "evidence": "Credit Risk group",
                }
            ],
            "team_member_reminders": [
                {
                    "domain": "Ops Risk",
                    "person": "Ker Yin",
                    "reminder": "Please check whether the pending GRC confirmation still needs owner follow-up.",
                    "evidence": "Ops Risk group",
                }
            ],
            "my_todos": [
                {
                    "task": "Review rollout note",
                    "domain": "Anti-fraud",
                    "priority": "high",
                    "due": "unknown",
                    "evidence": "18:30 Bob: please review",
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

    def test_render_email_handles_empty_partial_and_full_sections(self):
        now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        subject, text_body, html_body = render_email(briefing={"my_todos": [], "project_updates": []}, now=now)
        self.assertEqual(subject, "SeaTalk Daily Brief - 2026-04-27")
        self.assertIn("No clear Xiaodong-owned to-do", text_body)
        self.assertIn("No clear product update", html_body)
        self.assertIn("No additional high-value awareness update", text_body)
        self.assertIn("No unresolved team-member mention", text_body)

        _, text_body, _ = render_email(
            briefing={"my_todos": [{"task": "Review", "domain": "General", "priority": "high", "due": "today", "evidence": "Alice"}]},
            now=now,
        )
        self.assertIn("General\n[High] Review. Due: today (Source: Alice)", text_body)
        self.assertIn("No clear product update", text_body)

        payload = build_daily_briefing(
            FakeSeaTalkService("SeaTalk Chat History Export\n[2026-04-27 18:30:00] Bob: please review\n"),
            now=now,
        )
        _, text_body, html_body = render_email(briefing=payload, now=now)
        self.assertIn("Review rollout note", text_body)
        self.assertIn("Deck was refreshed. [Status: Done]", html_body)
        self.assertIn("Other Update", text_body)
        self.assertIn("A policy dependency may affect downstream rollout planning. [Status: In Progress]", html_body)
        self.assertIn("Team Member Reminder", text_body)
        self.assertIn("Ker Yin: Please check whether the pending GRC confirmation still needs owner follow-up.", html_body)

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

    def test_missing_gmail_send_scope_reports_reconnect(self):
        with self.assertRaisesRegex(ConfigError, "Reconnect Google"):
            ensure_gmail_send_scope({"scopes": ["https://www.googleapis.com/auth/gmail.readonly"]})

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

    def test_send_daily_email_skips_existing_run_before_work(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            now = datetime(2026, 4, 27, 19, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
            DailyEmailRunStore(Path(temp_dir) / "seatalk" / "daily_email_runs.json").mark_sent(
                run_date="2026-04-27",
                recipient="xiaodong.zheng@npt.sg",
                subject="SeaTalk Daily Brief - 2026-04-27",
                message_id="msg-1",
                sent_at=now,
            )
            with patch("bpmis_jira_tool.seatalk_daily_email.build_daily_briefing") as briefing:
                result = send_daily_email(settings=settings, now=now)
            briefing.assert_not_called()
            self.assertEqual(result.status, "skipped")


if __name__ == "__main__":
    unittest.main()
