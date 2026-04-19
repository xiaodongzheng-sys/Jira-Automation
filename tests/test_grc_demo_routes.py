import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.web import create_app


class GRCDemoRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_owner_sees_grc_demo_tab_on_index(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"GRC Demo", response.data)
        self.assertIn(b"/grc-demo/", response.data)

    def test_owner_can_open_grc_demo(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/grc-demo/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"GRC Demo", response.data)
        self.assertIn(b"Incident Overview", response.data)

    def test_non_owner_cannot_open_grc_demo_or_see_tab(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}

            index_response = client.get("/")
            route_response = client.get("/grc-demo/", follow_redirects=False)

        self.assertEqual(index_response.status_code, 200)
        self.assertNotIn(b"GRC Demo", index_response.data)
        self.assertEqual(route_response.status_code, 302)
        self.assertEqual(route_response.headers["Location"], "/")


if __name__ == "__main__":
    unittest.main()
