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

    def test_owner_sees_seatalk_management_tab_before_bpmis_on_index(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
            response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("SeaTalk Management", html)
        self.assertIn(b"/gmail-sea-talk-demo", response.data)
        self.assertLess(html.index("Source Code Q&amp;A"), html.index("PRD Briefing Tool"))
        self.assertLess(html.index("PRD Briefing Tool"), html.index("Team Dashboard"))
        self.assertLess(html.index("Team Dashboard"), html.index("SeaTalk Management"))
        self.assertLess(html.index("SeaTalk Management"), html.index("BPMIS Automation Tool"))
        self.assertNotIn("SeaTalk Summary", html)
        self.assertNotIn("Gmail &amp; SeaTalk Demo", html)

    def test_builtin_owner_sees_seatalk_summary_when_env_owner_changes(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "GMAIL_SEATALK_DEMO_OWNER_EMAIL": "other-owner@npt.sg",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"SeaTalk Management", response.data)
        self.assertNotIn(b"SeaTalk Summary", response.data)

    def test_non_owner_does_not_see_seatalk_summary_tab(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"SeaTalk Summary", response.data)
        self.assertNotIn(b"SeaTalk Management", response.data)

    def test_owner_can_open_seatalk_summary_page(self):
        with self.app.test_client() as client:
            self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
            response = client.get("/gmail-sea-talk-demo")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"SeaTalk Management", response.data)
        self.assertNotIn(b"SeaTalk Summary", response.data)
        self.assertNotIn(b"Map SeaTalk source IDs to readable names used in exports and Codex evidence.", response.data)
        self.assertNotIn(b"Gmail", response.data)
        self.assertNotIn(b"Mailbox Overview", response.data)
        self.assertNotIn(b"Daily Received Volume", response.data)
        self.assertNotIn(b"Daily Sent Volume", response.data)
        self.assertNotIn(b"Received Today", response.data)
        self.assertNotIn(b"Current Unread", response.data)
        self.assertNotIn(b"Read Rate", response.data)
        self.assertNotIn(b"Local Status", response.data)
        self.assertNotIn(b"Data Scope", response.data)
        self.assertNotIn(b"Download 7-Day Chat History", response.data)
        self.assertNotIn(b"/api/gmail-sea-talk-demo/seatalk/export", response.data)
        self.assertNotIn(b"Preparing Gmail download batches", response.data)
        self.assertNotIn(b"data-gmail-demo-root", response.data)
        self.assertNotIn(b"data-gmail-export-manifest-url", response.data)
        self.assertNotIn(b"Top 10 Senders", response.data)
        self.assertNotIn(b"SeaTalk Overview", response.data)
        self.assertNotIn(b"Project Updates", response.data)
        self.assertNotIn(b"data-seatalk-project-updates", response.data)
        self.assertNotIn(b"Project update product lines", response.data)
        self.assertNotIn(b"Anti-fraud", response.data)
        self.assertNotIn(b"Credit Risk", response.data)
        self.assertNotIn(b"Ops Risk", response.data)
        self.assertNotIn(b"To-do Items", response.data)
        self.assertNotIn(b"Summary</button>", response.data)
        self.assertIn(b"Name Mapping", response.data)
        self.assertIn(b"Refresh Candidates", response.data)
        self.assertIn(b"data-seatalk-name-mapping-refresh", response.data)
        self.assertIn(b"data-seatalk-name-mappings-url", response.data)
        self.assertIn(b"data-seatalk-name-mapping-save-feedback", response.data)
        self.assertIn(b"/api/gmail-sea-talk-demo/seatalk/name-mappings", response.data)
        self.assertNotIn(b"data-seatalk-insights-url", response.data)
        self.assertNotIn(b"data-seatalk-project-updates-url", response.data)
        self.assertNotIn(b"data-seatalk-todos-url", response.data)
        self.assertNotIn(b"data-seatalk-open-todos-url", response.data)
        self.assertNotIn(b"Generate To-dos", response.data)
        self.assertNotIn(b"Generate Project Updates", response.data)
        self.assertNotIn(b"Loading saved to-dos", response.data)
        self.assertNotIn(b"Click Generate Project Updates to load updates.", response.data)
        self.assertNotIn(b"Loading SeaTalk summary", response.data)
        self.assertNotIn(b"data-seatalk-todo-complete-url", response.data)
        self.assertNotIn(b"Team / Follow-up To-dos", response.data)
        self.assertIn(b"SeaTalk unavailable", response.data)

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

    def test_non_owner_split_seatalk_apis_are_forbidden(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            updates = client.get("/api/gmail-sea-talk-demo/seatalk/project-updates")
            todos = client.get("/api/gmail-sea-talk-demo/seatalk/todos")

        self.assertEqual(updates.status_code, 403)
        self.assertEqual(todos.status_code, 403)
        self.assertIn("restricted", updates.get_json()["message"])
        self.assertIn("restricted", todos.get_json()["message"])

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

    def test_owner_seatalk_project_updates_api_returns_updates_only(self):
        fake_payload = {
            "project_updates": [{"domain": "General", "title": "AI sharing", "summary": "Deck updated", "status": "done", "evidence": "Apr 27"}],
            "my_todos": [{"task": "Should not leak", "domain": "General", "priority": "low", "due": "unknown", "evidence": "Apr 27"}],
            "team_todos": [{"task": "Should not leak", "domain": "General", "owner": "Team", "evidence": "Apr 27"}],
            "generated_at": "2026-04-27T21:00:00+08:00",
            "model_id": "codex:gpt-5.5",
        }

        class FakeSeaTalkService:
            def build_project_updates(self):
                return fake_payload

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk/project-updates")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["project_updates"][0]["title"], "AI sharing")
        self.assertEqual(payload["my_todos"], [])
        self.assertEqual(payload["team_todos"], [])

    def test_owner_seatalk_todos_api_returns_todos_only_and_advances_cursor(self):
        calls: list[str] = []

        class FakeSeaTalkService:
            def build_todos(self, *, todo_since=""):
                calls.append(todo_since)
                return {
                    "project_updates": [{"domain": "General", "title": "Should not leak", "summary": "x", "status": "done", "evidence": "Apr 27"}],
                    "my_todos": [
                        {"task": "Prepare AI sharing", "domain": "General", "priority": "medium", "due": "unknown", "evidence": "Apr 28"}
                    ],
                    "team_todos": [],
                    "generated_at": "2026-04-29T09:00:00+08:00",
                    "todo_processed_until": "2026-04-29T09:00:00+08:00",
                    "model_id": "codex:gpt-5.5",
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                first = client.get("/api/gmail-sea-talk-demo/seatalk/todos")
                second = client.get("/api/gmail-sea-talk-demo/seatalk/todos")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(calls, ["", "2026-04-29T09:00:00+08:00"])
        payload = second.get_json()
        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["team_todos"], [])
        self.assertEqual(payload["my_todos"][0]["task"], "Prepare AI sharing")

    def test_owner_seatalk_open_todos_api_returns_saved_unfinished_todos_without_generation(self):
        saved_todo = {
            "task": "Follow up rollout",
            "domain": "Anti-fraud",
            "priority": "high",
            "due": "2026-04-30",
            "evidence": "Apr 21",
        }

        class FakeSeaTalkService:
            def build_todos(self, *, todo_since=""):
                raise AssertionError("open to-dos should not generate new SeaTalk insight data")

        with self.app.test_client() as client:
            self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
            with patch(
                "bpmis_jira_tool.web._build_seatalk_dashboard_service",
                return_value=type(
                    "SeedService",
                    (),
                    {
                        "build_todos": lambda self, **kwargs: {
                            "project_updates": [],
                            "my_todos": [saved_todo],
                            "team_todos": [],
                            "todo_processed_until": "2026-04-27T21:00:00+08:00",
                        }
                    },
                )(),
            ):
                seeded = client.get("/api/gmail-sea-talk-demo/seatalk/todos")
            with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
                response = client.get("/api/gmail-sea-talk-demo/seatalk/todos/open")

        self.assertEqual(seeded.status_code, 200)
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["team_todos"], [])
        self.assertEqual([item["task"] for item in payload["my_todos"]], ["Follow up rollout"])

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

    def test_uncompleted_seatalk_todos_survive_later_insight_refreshes(self):
        first_todo = {"task": "Follow up rollout", "domain": "Anti-fraud", "priority": "high", "due": "2026-04-30", "evidence": "Apr 21"}
        second_todo = {"task": "Review GRC lock", "domain": "GRC", "priority": "medium", "due": "unknown", "evidence": "Apr 22"}
        payloads = [
            {
                "project_updates": [],
                "my_todos": [first_todo],
                "team_todos": [],
                "generated_at": "2026-04-21T21:00:00+08:00",
                "model_id": "codex:gpt-5.5",
                "cache": {"hit": False, "expires_at": "2026-04-22T00:00:00+08:00"},
                "codex": {"latency_ms": 1200, "session_mode": "ephemeral"},
            },
            {
                "project_updates": [],
                "my_todos": [second_todo],
                "team_todos": [],
                "generated_at": "2026-04-22T21:00:00+08:00",
                "model_id": "codex:gpt-5.5",
                "cache": {"hit": False, "expires_at": "2026-04-23T00:00:00+08:00"},
                "codex": {"latency_ms": 1200, "session_mode": "ephemeral"},
            },
        ]

        class FakeSeaTalkService:
            def build_insights(self):
                return payloads.pop(0)

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                first = client.get("/api/gmail-sea-talk-demo/seatalk/insights")
                second = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(
            [item["task"] for item in second.get_json()["my_todos"]],
            ["Follow up rollout", "Review GRC lock"],
        )

    def test_seatalk_insights_passes_previous_todo_cursor_to_service(self):
        calls: list[str] = []

        class FakeSeaTalkService:
            def build_insights(self, *, todo_since=""):
                calls.append(todo_since)
                if not todo_since:
                    return {
                        "project_updates": [],
                        "my_todos": [
                            {"task": "First task", "domain": "General", "priority": "medium", "due": "unknown", "evidence": "Apr 27"}
                        ],
                        "team_todos": [],
                        "generated_at": "2026-04-27T21:00:00+08:00",
                        "todo_processed_until": "2026-04-27T21:00:00+08:00",
                    }
                return {
                    "project_updates": [],
                    "my_todos": [
                        {"task": "Second task", "domain": "General", "priority": "medium", "due": "unknown", "evidence": "Apr 29"}
                    ],
                    "team_todos": [],
                    "generated_at": "2026-04-29T09:00:00+08:00",
                    "todo_processed_until": "2026-04-29T09:00:00+08:00",
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                first = client.get("/api/gmail-sea-talk-demo/seatalk/insights")
                second = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(calls, ["", "2026-04-27T21:00:00+08:00"])
        self.assertEqual([item["task"] for item in second.get_json()["my_todos"]], ["First task", "Second task"])

    def test_similar_uncompleted_seatalk_todos_are_not_duplicated(self):
        first_todo = {
            "task": "Prepare PM AI sharing session",
            "domain": "General",
            "priority": "medium",
            "due": "unknown",
            "evidence": "Apr 22",
        }
        second_todo = {
            "task": "Prepare follow-up PM AI tool sharing session",
            "domain": "General",
            "priority": "high",
            "due": "2026-05-06",
            "evidence": "Apr 23",
        }
        payloads = [
            {
                "project_updates": [],
                "my_todos": [first_todo],
                "team_todos": [],
                "generated_at": "2026-04-21T21:00:00+08:00",
                "model_id": "codex:gpt-5.5",
                "cache": {"hit": False, "expires_at": "2026-04-22T00:00:00+08:00"},
                "codex": {"latency_ms": 1200, "session_mode": "ephemeral"},
            },
            {
                "project_updates": [],
                "my_todos": [second_todo],
                "team_todos": [],
                "generated_at": "2026-04-22T21:00:00+08:00",
                "model_id": "codex:gpt-5.5",
                "cache": {"hit": False, "expires_at": "2026-04-23T00:00:00+08:00"},
                "codex": {"latency_ms": 1200, "session_mode": "ephemeral"},
            },
        ]

        class FakeSeaTalkService:
            def build_insights(self):
                return payloads.pop(0)

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                first = client.get("/api/gmail-sea-talk-demo/seatalk/insights")
                second = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        first_items = first.get_json()["my_todos"]
        second_items = second.get_json()["my_todos"]
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(second_items), 1)
        self.assertEqual(second_items[0]["id"], first_items[0]["id"])
        self.assertEqual(second_items[0]["task"], "Prepare PM AI sharing session")
        self.assertEqual(second_items[0]["priority"], "high")
        self.assertEqual(second_items[0]["due"], "2026-05-06")

    def test_reworded_meeting_todos_with_same_entities_are_not_duplicated(self):
        first_todo = {
            "task": "Join the Monday discussion on ALC v12 force-upgrade and Android minimum-version timeline if invited, and keep AF position aligned with v3.45/v3.46 plan.",
            "domain": "Anti-fraud",
            "priority": "medium",
            "due": "2026-04-27 16:30",
            "evidence": "FV Upgrade, 2026-04-24 10:50-10:57: Monday 4:30-5PM discussion accepted.",
        }
        second_todo = {
            "task": "Join or support Monday discussion on ID ALC v12, Android minimum version, and force-upgrade timeline.",
            "domain": "Anti-fraud",
            "priority": "medium",
            "due": "2026-04-27 16:30-17:00 SGT",
            "evidence": "2026-04-24 FV Upgrade: Tasya asks for Monday 4:30-5PM discussion; Xiaodong says OK",
        }
        payloads = [
            {"project_updates": [], "my_todos": [first_todo], "team_todos": [], "generated_at": "2026-04-24T21:00:00+08:00"},
            {"project_updates": [], "my_todos": [second_todo], "team_todos": [], "generated_at": "2026-04-25T21:00:00+08:00"},
        ]

        class FakeSeaTalkService:
            def build_insights(self):
                return payloads.pop(0)

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                first = client.get("/api/gmail-sea-talk-demo/seatalk/insights")
                second = client.get("/api/gmail-sea-talk-demo/seatalk/insights")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        second_items = second.get_json()["my_todos"]
        self.assertEqual(len(second_items), 1)
        self.assertEqual(second_items[0]["id"], first.get_json()["my_todos"][0]["id"])
        self.assertIn("ALC v12", second_items[0]["task"])

    def test_owner_can_load_and_save_seatalk_name_mappings(self):
        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
                return {
                    "unknown_ids": [
                        {"id": "group-123", "type": "group", "count": 12, "example": "2026-04-21: kickoff"},
                        {"id": "UID 456", "type": "uid", "count": 6, "example": "2026-04-21: hello"},
                    ],
                    "generated_at": "2026-04-21T21:00:00+08:00",
                    "period_days": 7,
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                first_save = client.post(
                    "/api/gmail-sea-talk-demo/seatalk/name-mappings",
                    json={"mappings": {"buddy-456": "Important DM"}},
                )
                saved = client.post(
                    "/api/gmail-sea-talk-demo/seatalk/name-mappings",
                    json={"mappings": {"group-123": "Risk Project Group", "UID 888": "Alice", "bad": "ignored"}},
                )
                loaded = client.get("/api/gmail-sea-talk-demo/seatalk/name-mappings")

        self.assertEqual(first_save.status_code, 200)
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(
            saved.get_json()["mappings"],
            {
                "UID 456": "Important DM",
                "UID 888": "Alice",
                "buddy-456": "Important DM",
                "buddy-888": "Alice",
                "group-123": "Risk Project Group",
            },
        )
        self.assertEqual(loaded.status_code, 200)
        payload = loaded.get_json()
        self.assertEqual(payload["mappings"]["group-123"], "Risk Project Group")
        self.assertEqual(payload["mappings"]["buddy-456"], "Important DM")
        self.assertEqual(payload["mappings"]["UID 456"], "Important DM")
        self.assertEqual(payload["unknown_ids"], [])

    def test_owner_seatalk_name_mappings_dedupes_buddy_and_uid_candidates(self):
        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
                return {
                    "unknown_ids": [
                        {
                            "id": "buddy-627112",
                            "type": "buddy",
                            "count": 25,
                            "example": "2026-04-30: direct chat",
                            "priority_reason": "Private chat",
                        },
                        {
                            "id": "UID 627112",
                            "type": "uid",
                            "count": 66,
                            "example": "2026-04-30: @mentioned me",
                            "priority_reason": "@mentioned me",
                        },
                        {
                            "id": "buddy-364199",
                            "type": "buddy",
                            "count": 13,
                            "example": "2026-04-29: direct chat",
                            "priority_reason": "Private chat",
                        },
                    ],
                    "generated_at": "2026-04-30T08:30:00+08:00",
                    "period_days": 7,
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk/name-mappings")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual([row["id"] for row in payload["unknown_ids"]], ["UID 627112", "UID 364199"])
        self.assertEqual(payload["unknown_ids"][0]["count"], 91)
        self.assertEqual(payload["unknown_ids"][0]["priority_reason"], "@mentioned me")

    def test_owner_seatalk_name_mappings_refresh_bypasses_cache(self):
        calls = []

        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
                calls.append(force_refresh)
                return {
                    "unknown_ids": [
                        {"id": "UID 456", "type": "uid", "count": 6, "example": "2026-04-21: hello"},
                    ],
                    "generated_at": "2026-04-21T21:00:00+08:00",
                    "period_days": 7,
                    "cache": {"hit": False},
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client, scopes=[GMAIL_READONLY_SCOPE])
                response = client.get("/api/gmail-sea-talk-demo/seatalk/name-mappings?refresh=1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, [True])
        self.assertEqual(response.get_json()["unknown_ids"][0]["id"], "UID 456")

    def test_owner_seatalk_name_mappings_api_reports_export_error(self):
        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
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
