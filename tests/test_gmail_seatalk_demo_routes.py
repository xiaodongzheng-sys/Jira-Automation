import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.web import create_app


TEAM_SEATALK_INSIGHTS_URL = "/api/team-dashboard/seatalk/insights"
TEAM_SEATALK_PROJECT_UPDATES_URL = "/api/team-dashboard/seatalk/project-updates"
TEAM_SEATALK_TODOS_URL = "/api/team-dashboard/seatalk/todos"
TEAM_SEATALK_OPEN_TODOS_URL = "/api/team-dashboard/seatalk/todos/open"
TEAM_SEATALK_TODO_COMPLETE_URL = "/api/team-dashboard/seatalk/todos/complete"
TEAM_SEATALK_NAME_MAPPINGS_URL = "/api/team-dashboard/report-intelligence/seatalk/name-mappings"


class GmailSeaTalkDemoRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "LOCAL_AGENT_MODE": "disabled",
                "LOCAL_AGENT_BASE_URL": "",
                "LOCAL_AGENT_HMAC_SECRET": "",
                "LOCAL_AGENT_SEATALK_ENABLED": "false",
                "SEATALK_LOCAL_APP_PATH": os.path.join(self.temp_dir.name, "missing", "SeaTalk.app"),
                "SEATALK_LOCAL_DATA_DIR": os.path.join(self.temp_dir.name, "missing", "SeaTalkData"),
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def _login_owner(client):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
            session["google_credentials"] = {"token": "x", "scopes": []}

    @staticmethod
    def _login_teammate(client):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
            session["google_credentials"] = {"token": "x", "scopes": []}

    def test_owner_no_longer_sees_seatalk_management_tab_on_index(self):
        with self.app.test_client() as client:
            self._login_owner(client)
            response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertNotIn("SeaTalk Management", html)
        self.assertNotIn(b"/gmail-sea-talk-demo", response.data)
        self.assertLess(html.index(">Source Code<"), html.index(">Meeting<"))
        self.assertLess(html.index(">Meeting<"), html.index(">PRDs<"))
        self.assertLess(html.index(">PRDs<"), html.index(">Projects<"))
        self.assertLess(html.index(">Projects<"), html.index(">Others<"))
        self.assertLess(html.index("Team Dashboard"), html.index("BPMIS Automation Tool"))
        self.assertLess(html.index("BPMIS Automation Tool"), html.index("Reports"))
        self.assertNotIn("VPN Connection", html)
        self.assertNotIn("SeaTalk Summary", html)
        self.assertNotIn("Gmail &amp; SeaTalk Demo", html)

    def test_retired_gmail_seatalk_demo_routes_are_not_registered(self):
        retired_paths = [
            "/gmail-sea-talk-demo",
            "/api/gmail-sea-talk-demo/dashboard",
            "/api/gmail-sea-talk-demo/network",
            "/api/gmail-sea-talk-demo/gmail/export",
            "/api/gmail-sea-talk-demo/seatalk/insights",
            "/api/gmail-sea-talk-demo/seatalk/name-mappings",
        ]
        with self.app.test_client() as client:
            self._login_owner(client)
            statuses = [client.get(path).status_code for path in retired_paths]

        self.assertEqual(statuses, [404] * len(retired_paths))

    def test_non_owner_seatalk_insights_api_is_forbidden(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.get(TEAM_SEATALK_INSIGHTS_URL)

        self.assertEqual(response.status_code, 403)
        self.assertIn("restricted", response.get_json()["message"])

    def test_non_owner_split_seatalk_apis_are_forbidden(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            updates = client.get(TEAM_SEATALK_PROJECT_UPDATES_URL)
            todos = client.get(TEAM_SEATALK_TODOS_URL)

        self.assertEqual(updates.status_code, 403)
        self.assertEqual(todos.status_code, 403)

    def test_non_owner_cannot_complete_seatalk_todo(self):
        with self.app.test_client() as client:
            self._login_teammate(client)
            response = client.post(
                TEAM_SEATALK_TODO_COMPLETE_URL,
                json={"todo": {"task": "Follow up rollout", "domain": "Anti-fraud"}},
            )

        self.assertEqual(response.status_code, 403)

    def test_owner_seatalk_insights_api_returns_codex_payload(self):
        fake_payload = {
            "project_updates": [{"domain": "Anti-fraud", "title": "AF rollout", "summary": "Ready", "status": "on_track", "evidence": "Apr 21"}],
            "my_todos": [{"task": "Follow up rollout", "domain": "Anti-fraud", "priority": "high", "due": "2026-04-30", "evidence": "Apr 21"}],
            "team_todos": [{"task": "Should not leak", "domain": "Anti-fraud", "owner": "Team", "evidence": "Apr 21"}],
            "generated_at": "2026-04-21T21:00:00+08:00",
            "model_id": "codex:gpt-5.5",
        }
        fake_service = type("FakeSeaTalkService", (), {"build_insights": lambda self: fake_payload})()

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client)
                response = client.get(TEAM_SEATALK_INSIGHTS_URL)

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
                self._login_owner(client)
                response = client.get(TEAM_SEATALK_PROJECT_UPDATES_URL)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
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
                    "my_todos": [{"task": "Prepare AI sharing", "domain": "General", "priority": "medium", "due": "unknown", "evidence": "Apr 28"}],
                    "team_todos": [],
                    "generated_at": "2026-04-29T09:00:00+08:00",
                    "todo_processed_until": "2026-04-29T09:00:00+08:00",
                    "model_id": "codex:gpt-5.5",
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                first = client.get(TEAM_SEATALK_TODOS_URL)
                second = client.get(TEAM_SEATALK_TODOS_URL)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(calls, ["", "2026-04-29T09:00:00+08:00"])
        payload = second.get_json()
        self.assertEqual(payload["project_updates"], [])
        self.assertEqual(payload["team_todos"], [])
        self.assertEqual(payload["my_todos"][0]["task"], "Prepare AI sharing")

    def test_owner_seatalk_open_todos_api_returns_saved_unfinished_todos_without_generation(self):
        saved_todo = {"task": "Follow up rollout", "domain": "Anti-fraud", "priority": "high", "due": "2026-04-30", "evidence": "Apr 21"}

        class FakeSeaTalkService:
            def build_todos(self, *, todo_since=""):
                raise AssertionError("open to-dos should not generate new SeaTalk insight data")

        with self.app.test_client() as client:
            self._login_owner(client)
            seed_service = type(
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
            )()
            with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=seed_service):
                seeded = client.get(TEAM_SEATALK_TODOS_URL)
            with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
                response = client.get(TEAM_SEATALK_OPEN_TODOS_URL)

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
        }
        fake_service = type("FakeSeaTalkService", (), {"build_insights": lambda self: fake_payload})()

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=fake_service):
            with self.app.test_client() as client:
                self._login_owner(client)
                completed = client.post(TEAM_SEATALK_TODO_COMPLETE_URL, json={"todo": todo})
                response = client.get(TEAM_SEATALK_INSIGHTS_URL)

        self.assertEqual(completed.status_code, 200)
        self.assertEqual([item["task"] for item in response.get_json()["my_todos"]], ["Review GRC lock"])

    def test_uncompleted_seatalk_todos_survive_later_insight_refreshes(self):
        payloads = [
            {"project_updates": [], "my_todos": [{"task": "Follow up rollout", "domain": "Anti-fraud"}], "team_todos": [], "todo_processed_until": "2026-04-21"},
            {"project_updates": [], "my_todos": [{"task": "Review GRC lock", "domain": "GRC"}], "team_todos": [], "todo_processed_until": "2026-04-22"},
        ]

        class FakeSeaTalkService:
            def build_insights(self, *, todo_since=""):
                return payloads.pop(0)

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                first = client.get(TEAM_SEATALK_INSIGHTS_URL)
                second = client.get(TEAM_SEATALK_INSIGHTS_URL)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual([item["task"] for item in second.get_json()["my_todos"]], ["Follow up rollout", "Review GRC lock"])

    def test_seatalk_insights_passes_previous_todo_cursor_to_service(self):
        calls: list[str] = []

        class FakeSeaTalkService:
            def build_insights(self, *, todo_since=""):
                calls.append(todo_since)
                return {
                    "project_updates": [],
                    "my_todos": [{"task": "Task", "domain": "General"}],
                    "team_todos": [],
                    "todo_processed_until": "2026-04-27T21:00:00+08:00" if not todo_since else "2026-04-29T09:00:00+08:00",
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                first = client.get(TEAM_SEATALK_INSIGHTS_URL)
                second = client.get(TEAM_SEATALK_INSIGHTS_URL)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(calls, ["", "2026-04-27T21:00:00+08:00"])

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
                self._login_owner(client)
                saved = client.post(
                    TEAM_SEATALK_NAME_MAPPINGS_URL,
                    json={"mappings": {"group-123": "Risk Project Group", "UID 456": "Important DM"}},
                )
                loaded = client.get(TEAM_SEATALK_NAME_MAPPINGS_URL)

        self.assertEqual(saved.status_code, 200)
        self.assertEqual(loaded.status_code, 200)
        payload = loaded.get_json()
        self.assertEqual(payload["mappings"]["group-123"], "Risk Project Group")
        self.assertEqual(payload["mappings"]["UID 456"], "Important DM")
        self.assertEqual(payload["mappings"]["buddy-456"], "Important DM")
        self.assertEqual(payload["unknown_ids"], [])

    def test_team_dashboard_name_mapping_uses_existing_seatalk_store_and_payload_shape(self):
        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
                return {
                    "unknown_ids": [{"id": "UID 456", "type": "uid", "count": 6, "example": "2026-04-21: hello"}],
                    "generated_at": "2026-04-21T21:00:00+08:00",
                    "period_days": 7,
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                saved = client.post(TEAM_SEATALK_NAME_MAPPINGS_URL, json={"mappings": {"UID 456": "Important DM"}})
                loaded = client.get(TEAM_SEATALK_NAME_MAPPINGS_URL)

        self.assertEqual(saved.status_code, 200)
        self.assertEqual(loaded.status_code, 200)
        payload = loaded.get_json()
        self.assertEqual(payload["mappings"]["UID 456"], "Important DM")
        self.assertEqual(payload["mappings"]["buddy-456"], "Important DM")
        self.assertEqual(payload["unknown_ids"], [])
        self.assertNotIn("candidates", payload)

    def test_team_dashboard_name_mapping_auto_merges_confident_candidates(self):
        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
                return {
                    "unknown_ids": [
                        {"id": "group-123", "type": "group", "count": 12, "example": "2026-04-21: kickoff"},
                        {"id": "UID 456", "type": "uid", "count": 6, "example": "2026-04-21: hello"},
                        {"id": "UID 777", "type": "uid", "count": 3, "example": "2026-04-21: needs manual"},
                    ],
                    "auto_mappings": {"group-123": "Risk Project Group", "buddy-456": "Alice Tan"},
                    "generated_at": "2026-04-21T21:00:00+08:00",
                    "period_days": 7,
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                loaded = client.get(TEAM_SEATALK_NAME_MAPPINGS_URL)

        self.assertEqual(loaded.status_code, 200)
        payload = loaded.get_json()
        self.assertEqual(payload["mappings"]["group-123"], "Risk Project Group")
        self.assertEqual(payload["mappings"]["UID 456"], "Alice Tan")
        self.assertEqual(payload["mappings"]["buddy-456"], "Alice Tan")
        self.assertEqual([row["id"] for row in payload["unknown_ids"]], ["UID 777"])

    def test_owner_seatalk_name_mappings_dedupes_buddy_and_uid_candidates(self):
        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
                return {
                    "unknown_ids": [
                        {"id": "buddy-627112", "type": "buddy", "count": 25, "example": "direct chat", "priority_reason": "Private chat"},
                        {"id": "UID 627112", "type": "uid", "count": 66, "example": "@mentioned me", "priority_reason": "@mentioned me"},
                        {"id": "buddy-364199", "type": "buddy", "count": 13, "example": "direct chat", "priority_reason": "Private chat"},
                        {"id": "UID 0", "type": "uid", "count": 10, "example": "[custom.missing]", "priority_reason": "Frequent unknown ID"},
                        {"id": "buddy-0", "type": "buddy", "count": 1, "example": "[custom.missing]", "priority_reason": "Frequent unknown ID"},
                    ],
                    "generated_at": "2026-04-30T08:30:00+08:00",
                    "period_days": 7,
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                response = client.get(TEAM_SEATALK_NAME_MAPPINGS_URL)

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
                    "unknown_ids": [{"id": "UID 456", "type": "uid", "count": 6, "example": "2026-04-21: hello"}],
                    "generated_at": "2026-04-21T21:00:00+08:00",
                    "period_days": 7,
                    "cache": {"hit": False},
                }

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                response = client.get(f"{TEAM_SEATALK_NAME_MAPPINGS_URL}?refresh=1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, [True])
        self.assertEqual(response.get_json()["unknown_ids"][0]["id"], "UID 456")

    def test_owner_seatalk_name_mappings_api_reports_export_error(self):
        class FakeSeaTalkService:
            def build_name_mappings(self, *, force_refresh=False):
                raise ToolError("SeaTalk desktop database was not found.")

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                response = client.get(TEAM_SEATALK_NAME_MAPPINGS_URL)

        self.assertEqual(response.status_code, 400)
        self.assertIn("SeaTalk desktop database", response.get_json()["message"])

    def test_owner_seatalk_insights_api_reports_codex_error(self):
        class FakeSeaTalkService:
            def build_insights(self):
                raise ToolError("Codex is unavailable. Run `codex login` with ChatGPT on this server before using Codex mode.")

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FakeSeaTalkService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                response = client.get(TEAM_SEATALK_INSIGHTS_URL)

        self.assertEqual(response.status_code, 400)
        self.assertIn("Codex is unavailable", response.get_json()["message"])

    def test_seatalk_split_api_fallback_and_failure_boundaries_are_sanitized(self):
        class FallbackService:
            def build_insights(self):
                return {"project_updates": [{"title": "Fallback"}], "my_todos": [{"task": "Todo"}], "team_todos": []}

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=FallbackService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                fallback_updates = client.get(TEAM_SEATALK_PROJECT_UPDATES_URL)
                fallback_todos = client.get(TEAM_SEATALK_TODOS_URL)

        self.assertEqual(fallback_updates.status_code, 200)
        self.assertEqual(fallback_todos.status_code, 200)
        self.assertEqual(fallback_updates.get_json()["my_todos"], [])
        self.assertEqual(fallback_todos.get_json()["project_updates"], [])

        class ToolErrorService:
            def build_project_updates(self):
                raise ToolError("SeaTalk export failed.")

            def build_todos(self, *, todo_since=""):
                raise ToolError("SeaTalk todo export failed.")

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=ToolErrorService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                project_tool_error = client.get(TEAM_SEATALK_PROJECT_UPDATES_URL)
                todos_tool_error = client.get(TEAM_SEATALK_TODOS_URL)

        self.assertEqual(project_tool_error.status_code, 400)
        self.assertEqual(todos_tool_error.status_code, 400)

        class BrokenService:
            def build_insights(self):
                raise RuntimeError("token=secret")

        with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=BrokenService()):
            with self.app.test_client() as client:
                self._login_owner(client)
                unexpected = client.get(TEAM_SEATALK_INSIGHTS_URL)

        self.assertEqual(unexpected.status_code, 500)
        self.assertNotIn("secret", unexpected.get_json()["message"])


if __name__ == "__main__":
    unittest.main()
