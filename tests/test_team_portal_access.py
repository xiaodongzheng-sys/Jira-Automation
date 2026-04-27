import os
import time
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.web import _current_release_revision, create_app


class _FakeSyncBPMISClient:
    def list_biz_projects_for_pm_email(self, _email):
        return []

    def get_brd_doc_links_for_projects(self, _issue_ids):
        return {}


class _FakeProxyResponse:
    status_code = 200
    content = b'{"status":"ok","result":[]}'
    headers = {"Content-Type": "application/json", "Content-Length": "27"}


class TeamPortalAccessTests(unittest.TestCase):
    def test_shared_mode_renders_login_gate_for_anonymous_user(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/", follow_redirects=False)
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Sign in to Risk PM Tool", response.data)
                self.assertIn(b"Continue with Google", response.data)

    def test_shared_mode_redirects_protected_route_to_login_gate(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/download/default-sheet-template.xlsx", follow_redirects=False)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["Location"], "/")

    def test_blocked_google_user_is_logged_out_and_shown_index(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "blocked@npt.sg", "name": "Blocked User"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/", follow_redirects=False)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["Location"], "/access-denied")

    def test_access_denied_page_renders(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/access-denied")
                self.assertEqual(response.status_code, 403)
                self.assertIn(b"Access Restricted", response.data)

    def test_allowed_google_user_can_open_index(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "allowed@npt.sg", "name": "Allowed User"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/")
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Allowed User", response.data)
                self.assertIn(b"Logout", response.data)
                self.assertEqual(response.headers.get("Cache-Control"), "no-store, private, max-age=0")
                self.assertEqual(response.headers.get("Pragma"), "no-cache")
                self.assertEqual(response.headers.get("Expires"), "0")

    def test_google_logout_clears_google_session(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.post("/auth/google/logout", follow_redirects=False)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["Location"], "/")

                with client.session_transaction() as session:
                    self.assertNotIn("google_profile", session)
                    self.assertNotIn("google_credentials", session)

    def test_allowed_google_domain_can_open_index(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/")
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Teammate", response.data)

    def test_anonymous_login_gate_response_is_not_marked_no_store(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/", follow_redirects=False)
                self.assertEqual(response.status_code, 200)
                self.assertIsNone(response.headers.get("Pragma"))
                self.assertIsNone(response.headers.get("Expires"))

    def test_healthz_is_public_in_shared_mode(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/healthz")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.get_json(),
                    {"status": "ok", "revision": _current_release_revision()},
                )

    def test_preview_job_requires_google_connection(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.post("/api/jobs/preview")
                self.assertEqual(response.status_code, 400)
                payload = response.get_json()
                self.assertIn("connect Google", payload["message"])

    def test_sync_job_does_not_require_google_connection(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
                "BPMIS_API_ACCESS_TOKEN": "token",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web.build_bpmis_client", return_value=_FakeSyncBPMISClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "sync-user"
                app.config["CONFIG_STORE"].save({"sync_pm_email": "pm@npt.sg"}, "anon:sync-user")
                response = client.post("/api/jobs/sync-bpmis-projects")
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["status"], "queued")
                deadline = time.time() + 2
                while time.time() < deadline:
                    job_payload = client.get(f"/api/jobs/{payload['job_id']}").get_json()
                    if job_payload["state"] == "completed":
                        break
                    time.sleep(0.01)

    def test_public_local_agent_proxy_forwards_signed_request_to_loopback_agent(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
                "LOCAL_AGENT_BASE_URL": "",
                "LOCAL_AGENT_HOST": "127.0.0.1",
                "LOCAL_AGENT_PORT": "8123",
                "LOCAL_AGENT_TIMEOUT_SECONDS": "7",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._LOCAL_AGENT_SESSION.request", return_value=_FakeProxyResponse()) as proxy_request:
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.post(
                    "/api/local-agent/bpmis/call",
                    data=b'{"operation":"ping"}',
                    headers={
                        "Content-Type": "application/json",
                        "X-Local-Agent-Timestamp": "123",
                        "X-Local-Agent-Nonce": "abc",
                        "X-Local-Agent-Signature": "sig",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "ok")
        proxy_request.assert_called_once()
        _method, target_url = proxy_request.call_args.args[:2]
        self.assertEqual(target_url, "http://127.0.0.1:8123/api/local-agent/bpmis/call")
        self.assertEqual(proxy_request.call_args.kwargs["data"], b'{"operation":"ping"}')
        self.assertEqual(proxy_request.call_args.kwargs["headers"]["X-Local-Agent-Signature"], "sig")
        self.assertEqual(proxy_request.call_args.kwargs["timeout"], (7, 7))

    def test_public_local_agent_proxy_forwards_healthz_to_loopback_agent_health(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
                "LOCAL_AGENT_BASE_URL": "",
                "LOCAL_AGENT_HOST": "127.0.0.1",
                "LOCAL_AGENT_PORT": "8123",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._LOCAL_AGENT_SESSION.request", return_value=_FakeProxyResponse()) as proxy_request:
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/api/local-agent/healthz")

        self.assertEqual(response.status_code, 200)
        _method, target_url = proxy_request.call_args.args[:2]
        self.assertEqual(target_url, "http://127.0.0.1:8123/healthz")

    def test_public_local_agent_proxy_prefers_configured_portal_agent_proxy_url(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
                "LOCAL_AGENT_BASE_URL": "https://agent.example.test",
                "LOCAL_AGENT_HOST": "127.0.0.1",
                "LOCAL_AGENT_PORT": "8123",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._LOCAL_AGENT_SESSION.request", return_value=_FakeProxyResponse()) as proxy_request:
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/api/local-agent/healthz")

        self.assertEqual(response.status_code, 200)
        _method, target_url = proxy_request.call_args.args[:2]
        self.assertEqual(target_url, "https://agent.example.test/api/local-agent/healthz")

    def test_default_sheet_template_download_returns_csv(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/download/default-sheet-template.csv")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.mimetype, "text/csv")
                self.assertIn(b"BPMIS ID,Project Name,BRD Link,Market,System,Jira Title,PRD Link,Description,Jira Ticket Link", response.data)

    def test_allowlist_without_shared_portal_host_does_not_block_local_user_sessions(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "xiaodong.zheng1991@gmail.com",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/")

        self.assertEqual(response.status_code, 200)

if __name__ == "__main__":
    unittest.main()
