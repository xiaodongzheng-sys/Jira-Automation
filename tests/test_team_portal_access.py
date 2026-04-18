import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from bpmis_jira_tool.web import create_app


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
                self.assertIn(b"Sign in to open the BPMIS workspace", response.data)
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

    def test_self_check_returns_warn_without_google(self):
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
        ), patch("bpmis_jira_tool.web.build_bpmis_client") as mock_build_client:
            mock_build_client.return_value = MagicMock()
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/api/self-check")
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["status"], "warn")
                self.assertEqual(payload["checks"][0]["name"], "Google Sheets connection")

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

    def test_sync_job_requires_google_connection(self):
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
                response = client.post("/api/jobs/sync-bpmis-projects")
                self.assertEqual(response.status_code, 400)
                payload = response.get_json()
                self.assertIn("connect Google", payload["message"])

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
                self.assertIn(b"BPMIS ID,Project Name,Market,BRD Link,System,Jira Title,PRD Link,Description,Jira Ticket Link", response.data)

if __name__ == "__main__":
    unittest.main()
