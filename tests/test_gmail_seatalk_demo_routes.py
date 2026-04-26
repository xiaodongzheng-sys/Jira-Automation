import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE
from bpmis_jira_tool import web as web_module
from bpmis_jira_tool.web import create_app


class GmailSeaTalkDemoRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "SEATALK_LOCAL_APP_PATH": os.path.join(self.temp_dir.name, "missing", "SeaTalk.app"),
                "SEATALK_LOCAL_DATA_DIR": os.path.join(self.temp_dir.name, "missing", "SeaTalkData"),
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        with web_module._gmail_export_active_users_lock:
            web_module._gmail_export_active_users.clear()
        self.temp_dir.cleanup()

    @staticmethod
    def _login_owner(client, *, scopes=None):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
            session["google_credentials"] = {"token": "x", "scopes": scopes or []}

    @staticmethod
    def _login_teammate(client):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
            session["google_credentials"] = {"token": "x", "scopes": [GMAIL_READONLY_SCOPE]}

    def test_owner_sees_seatalk_summary_tab_on_index(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("SeaTalk Summary", html)
        self.assertIn(b"/gmail-sea-talk-demo", response.data)
        self.assertLess(html.index("Source Code Q&amp;A"), html.index("SeaTalk Summary"))
        self.assertLess(html.index("SeaTalk Summary"), html.index("PRD Briefing Tool"))
        self.assertNotIn("Gmail &amp; SeaTalk Demo", html)

    def test_non_owner_does_not_see_seatalk_summary_tab(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"SeaTalk Summary", response.data)

    def test_owner_can_open_seatalk_summary_page(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
            response = client.get("/gmail-sea-talk-demo")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"SeaTalk Summary", response.data)
        self.assertNotIn(b"Gmail", response.data)
        self.assertNotIn(b"Mailbox Overview", response.data)
        self.assertNotIn(b"Daily Received Volume", response.data)
        self.assertNotIn(b"Daily Sent Volume", response.data)
        self.assertNotIn(b"Received Today", response.data)
        self.assertNotIn(b"Current Unread", response.data)
        self.assertNotIn(b"Read Rate", response.data)
        self.assertNotIn(b"Local Status", response.data)
        self.assertNotIn(b"Data Scope", response.data)
        self.assertIn(b"Download 7-Day Chat History", response.data)
        self.assertIn(b"/api/gmail-sea-talk-demo/seatalk/export", response.data)
        self.assertNotIn(b"Preparing Gmail download batches", response.data)
        self.assertNotIn(b"data-gmail-demo-root", response.data)
        self.assertNotIn(b"data-gmail-export-manifest-url", response.data)
        self.assertNotIn(b"Top 10 Senders", response.data)
        self.assertNotIn(b"SeaTalk Overview", response.data)
        self.assertIn(b"Project Updates", response.data)
        self.assertIn(b"To-do Items", response.data)
        self.assertIn(b"Name Mapping", response.data)
        self.assertIn(b"data-seatalk-name-mappings-url", response.data)
        self.assertIn(b"/api/gmail-sea-talk-demo/seatalk/name-mappings", response.data)
        self.assertLess(response.data.index(b"To-do Items"), response.data.index(b"Project Updates"))
        self.assertIn(b"data-seatalk-insights-url", response.data)
        self.assertIn(b"data-seatalk-todo-complete-url", response.data)
        self.assertNotIn(b"Team / Follow-up To-dos", response.data)
        self.assertIn(b"Desktop data unavailable", response.data)

    def test_owner_page_does_not_require_gmail_scope(self):
        with patch("bpmis_jira_tool.web._google_credentials_have_scopes") as scope_check:
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[])
                response = client.get("/gmail-sea-talk-demo")

        self.assertEqual(response.status_code, 200)
        scope_check.assert_not_called()
        self.assertNotIn(b"Reconnect Google to grant Gmail access", response.data)
        self.assertNotIn(b"Download 7-Day Gmail History", response.data)

    def test_non_owner_page_is_denied(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.get("/gmail-sea-talk-demo", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/")

    def test_non_owner_api_is_forbidden(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.get("/api/gmail-sea-talk-demo/dashboard")

        self.assertEqual(response.status_code, 403)
        self.assertIn("restricted", response.get_json()["message"])

    def test_non_owner_seatalk_insights_api_is_forbidden(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        self.assertEqual(response.status_code, 403)
        self.assertIn("restricted", response.get_json()["message"])

    def test_non_owner_seatalk_name_mappings_api_is_forbidden(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.get("/api/gmail-sea-talk-demo/seatalk/name-mappings")

        self.assertEqual(response.status_code, 403)
        self.assertIn("restricted", response.get_json()["message"])

    def test_api_requires_gmail_scope_refresh(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[])
            response = client.get("/api/gmail-sea-talk-demo/dashboard")

        self.assertEqual(response.status_code, 400)
        self.assertIn("grant Gmail read access", response.get_json()["message"])

    def test_owner_api_returns_dashboard_payload(self):
        fake_dashboard = {
            "summary": {
                "received_today": 8,
                "current_unread": 12,
                "read_rate_percent": 76,
                "received_period_total": 240,
                "sent_period_total": 110,
            },
            "trends": {
                "received": [{"date": "2026-04-21", "label": "Apr 21", "count": 8}],
                "sent": [{"date": "2026-04-21", "label": "Apr 21", "count": 4}],
            },
            "leaderboards": {"top_senders": [], "top_recipients": []},
            "generated_at": "2026-04-21T21:00:00+08:00",
            "period_days": 7,
        }
        fake_service = type("FakeDashboardService", (), {"build_overview": lambda self: fake_dashboard})()

        with patch("bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/dashboard")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["summary"]["received_today"], 8)
        self.assertEqual(payload["leaderboards"]["top_senders"], [])

    def test_owner_network_api_returns_leaderboards(self):
        fake_network = {
            "leaderboards": {
                "top_senders": [{"rank": 1, "label": "alice@example.com", "count": 12}],
                "top_recipients": [{"rank": 1, "label": "bob@example.com", "count": 9}],
            },
            "generated_at": "2026-04-21T21:00:00+08:00",
            "period_days": 2,
            "data_quality": {"used_fallback_cache": False, "truncated": False},
        }
        fake_service = type("FakeDashboardService", (), {"build_network": lambda self: fake_network})()

        with patch("bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/network")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["leaderboards"]["top_senders"][0]["label"], "alice@example.com")

    def test_owner_gmail_export_downloads_text_attachment(self):
        fake_service = type(
            "FakeDashboardService",
            (),
            {
                "export_history_text": lambda self, batch=1: ("gmail export", f"gmail-history-last-7-days-batch-{batch}.txt"),
                "get_cached_export_history_text": lambda self, batch=1: None,
            },
        )()

        with patch("bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/gmail/export?batch=2")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Disposition"], 'attachment; filename=gmail-history-last-7-days-batch-2.txt')
        self.assertEqual(response.get_data(as_text=True), "gmail export")

    def test_owner_gmail_export_serves_cached_batch_when_lock_is_active(self):
        fake_service = type(
            "FakeDashboardService",
            (),
            {
                "get_cached_export_history_text": lambda self, batch=1: ("cached gmail export", f"gmail-history-last-7-days-batch-{batch}.txt"),
                "export_history_text": lambda self, batch=1: ("live gmail export", f"gmail-history-last-7-days-batch-{batch}.txt"),
            },
        )()

        with patch("bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                with web_module._gmail_export_active_users_lock:
                    web_module._gmail_export_active_users.add("xiaodong.zheng@npt.sg")
                response = client.get("/api/gmail-sea-talk-demo/gmail/export?batch=1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(as_text=True), "cached gmail export")

    def test_owner_gmail_export_manifest_returns_batch_metadata(self):
        fake_service = type(
            "FakeDashboardService",
            (),
            {
                "build_export_manifest": lambda self: {
                    "generated_at": "2026-04-22T09:00:00+08:00",
                    "period_days": 7,
                    "total_messages": 205,
                    "batch_size": 50,
                    "batch_count": 5,
                    "excluded_senders": ["reports.dwh@maribank.com.sg"],
                }
            },
        )()

        with patch("bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/gmail/export-manifest")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["batch_count"], 5)
        self.assertEqual(payload["batch_size"], 50)
        self.assertEqual(payload["total_messages"], 205)

    def test_owner_gmail_export_prewarm_returns_ok_when_cache_is_ready(self):
        fake_service = type(
            "FakeDashboardService",
            (),
            {
                "get_cached_export_history_text": lambda self, batch=1: ("gmail export", f"gmail-history-last-7-days-batch-{batch}.txt"),
                "prewarm_export_history_text": lambda self, batch=1: ("gmail export", f"gmail-history-last-7-days-batch-{batch}.txt"),
            },
        )()

        with patch("bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.post("/api/gmail-sea-talk-demo/gmail/export-prewarm?batch=1")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["cached"])

    def test_gmail_export_requires_scope_refresh(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[])
            response = client.get("/api/gmail-sea-talk-demo/gmail/export")

        self.assertEqual(response.status_code, 400)
        self.assertIn("grant Gmail read access", response.get_json()["message"])

    def test_gmail_export_is_rate_limited_while_same_user_export_is_active(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
            with web_module._gmail_export_active_users_lock:
                web_module._gmail_export_active_users.add("xiaodong.zheng@npt.sg")
            response = client.get("/api/gmail-sea-talk-demo/gmail/export?batch=1")

        self.assertEqual(response.status_code, 429)
        self.assertIn("already running", response.get_json()["message"])

    def test_seatalk_api_reports_missing_config_cleanly(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
            response = client.get("/api/gmail-sea-talk-demo/seatalk")

        self.assertEqual(response.status_code, 400)
        self.assertIn("SeaTalk desktop app was not found", response.get_json()["message"])

    def test_owner_seatalk_api_returns_dashboard_payload(self):
        fake_dashboard = {
            "summary": {
                "received_today": 6,
                "current_unread": 2,
                "read_rate_percent": 67,
                "received_period_total": 30,
                "sent_period_total": 12,
            },
            "trends": {
                "received": [{"date": "2026-04-21", "label": "Apr 21", "count": 6}],
                "sent": [{"date": "2026-04-21", "label": "Apr 21", "count": 2}],
            },
            "metric_availability": {
                "current_unread": {"available": True, "reason": ""},
                "read_rate_percent": {"available": False, "reason": "Not available from local SeaTalk desktop data for this scope."},
            },
            "generated_at": "2026-04-21T21:00:00+08:00",
            "period_days": 7,
            "data_quality": {"used_fallback_cache": False, "partial_data": False, "status_note": "ok"},
        }
        fake_service = type("FakeSeaTalkService", (), {"build_overview": lambda self: fake_dashboard})()

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["summary"]["read_rate_percent"], 67)
        self.assertTrue(payload["metric_availability"]["current_unread"]["available"])

    def test_owner_seatalk_export_downloads_text_attachment(self):
        fake_service = type(
            "FakeSeaTalkService",
            (),
            {"export_history_text": lambda self: ("hello history", "seatalk-history-last-7-days.txt")},
        )()

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk/export")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Disposition"], 'attachment; filename=seatalk-history-last-7-days.txt')
        self.assertEqual(response.get_data(as_text=True), "hello history")

    def test_owner_seatalk_insights_api_returns_codex_payload(self):
        fake_payload = {
            "project_updates": [{"domain": "Anti-fraud", "title": "AF rollout", "summary": "Progress", "status": "in_progress", "evidence": "Apr 21"}],
            "my_todos": [{"task": "Follow up rollout", "domain": "Anti-fraud", "priority": "high", "due": "unknown", "evidence": "Apr 21"}],
            "team_todos": [],
            "generated_at": "2026-04-21T21:00:00+08:00",
            "model_id": "codex:gpt-5.5",
            "cache": {"hit": False, "expires_at": "2026-04-22T00:00:00+08:00"},
            "codex": {"latency_ms": 1200, "session_mode": "ephemeral"},
        }
        fake_service = type("FakeSeaTalkService", (), {"build_insights": lambda self: fake_payload})()

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["project_updates"][0]["title"], "AF rollout")
        self.assertEqual(payload["my_todos"][0]["task"], "Follow up rollout")
        self.assertEqual(payload["team_todos"], [])
        self.assertEqual(payload["model_id"], "codex:gpt-5.5")

    def test_owner_can_complete_seatalk_todo_and_filter_it_from_insights(self):
        todo = {"task": "Follow up rollout", "domain": "Anti-fraud", "priority": "high", "due": "2026-04-30", "evidence": "Apr 21"}
        fake_payload = {
            "project_updates": [],
            "my_todos": [todo, {"task": "Review GRC lock", "domain": "GRC", "priority": "medium", "due": "unknown", "evidence": "Apr 22"}],
            "team_todos": [],
            "generated_at": "2026-04-21T21:00:00+08:00",
            "model_id": "codex:gpt-5.5",
            "cache": {"hit": False, "expires_at": "2026-04-22T00:00:00+08:00"},
            "codex": {"latency_ms": 1200, "session_mode": "ephemeral"},
        }
        fake_service = type("FakeSeaTalkService", (), {"build_insights": lambda self: fake_payload})()

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                completed = client.post("/api/gmail-sea-talk-demo/seatalk/todos/complete", json={"todo": todo})
                response = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        self.assertEqual(completed.status_code, 200)
        payload = response.get_json()
        self.assertEqual([item["task"] for item in payload["my_todos"]], ["Review GRC lock"])

    def test_owner_can_load_and_save_seatalk_name_mappings(self):
        class FakeSeaTalkService:
            def build_name_mappings(self):
                return {
                    "unknown_ids": [{"id": "group-123", "type": "group", "count": 12, "example": "2026-04-21: kickoff"}],
                    "generated_at": "2026-04-21T21:00:00+08:00",
                    "period_days": 7,
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                saved = client.post(
                    "/api/gmail-sea-talk-demo/seatalk/name-mappings",
                    json={"mappings": {"group-123": "Risk Project Group", "UID 888": "Alice", "bad": "ignored"}},
                )
                loaded = client.get("/api/gmail-sea-talk-demo/seatalk/name-mappings")

        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.get_json()["mappings"], {"UID 888": "Alice", "group-123": "Risk Project Group"})
        self.assertEqual(loaded.status_code, 200)
        payload = loaded.get_json()
        self.assertEqual(payload["mappings"]["group-123"], "Risk Project Group")
        self.assertEqual(payload["unknown_ids"][0]["id"], "group-123")

    def test_owner_seatalk_name_mappings_api_reports_export_error(self):
        class FakeSeaTalkService:
            def build_name_mappings(self):
                raise web_module.ToolError("SeaTalk desktop database was not found.")

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk/name-mappings")

        self.assertEqual(response.status_code, 400)
        self.assertIn("SeaTalk desktop database", response.get_json()["message"])

    def test_non_owner_cannot_complete_seatalk_todo(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.post(
                "/api/gmail-sea-talk-demo/seatalk/todos/complete",
                json={"todo": {"task": "Follow up rollout", "domain": "Anti-fraud"}},
            )

        self.assertEqual(response.status_code, 403)

    def test_owner_seatalk_insights_api_reports_codex_error(self):
        class FakeSeaTalkService:
            def build_insights(self):
                raise web_module.ToolError("Codex is unavailable. Run `codex login` with ChatGPT on this server before using Codex mode.")

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        self.assertEqual(response.status_code, 400)
        self.assertIn("Codex is unavailable", response.get_json()["message"])


if __name__ == "__main__":
    unittest.main()
