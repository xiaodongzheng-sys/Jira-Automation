import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.web import create_app


class TeamPortalAccessTests(unittest.TestCase):
    def test_blocked_google_user_is_logged_out_and_shown_index(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
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
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Connect Google", response.data)
                self.assertIn(b"Local Helper", response.data)

    def test_allowed_google_user_can_open_index(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
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


if __name__ == "__main__":
    unittest.main()
