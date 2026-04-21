import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.web import create_app


class RouteSaveRegressionTests(unittest.TestCase):
    def test_save_route_tolerates_existing_blank_owner_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
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

                app.config["CONFIG_STORE"].save(
                    {
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nDC | SG | Deposit",
                        "component_default_rules_text": (
                            "DBP-Anti-fraud | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2\n"
                            "Deposit |  |  |  | "
                        ),
                    },
                    "google:teammate@npt.sg",
                )

                response = client.post(
                    "/config/save-route",
                    json={
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nDC | SG | Deposit\nBC | SG | Pay",
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertIn("Deposit |  |  |  |", payload["component_default_rules_text"])
        self.assertIn("Pay |  |  |  |", payload["component_default_rules_text"])

    def test_save_route_ignores_malformed_existing_owner_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
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

                app.config["CONFIG_STORE"].save(
                    {
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "AF | SG | DBP-Anti-fraud",
                    },
                    "google:teammate@npt.sg",
                )

                response = client.post(
                    "/config/save-route",
                    json={
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nDC | SG | Deposit",
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(
            payload["component_default_rules_text"],
            "DBP-Anti-fraud |  |  |  |\nDeposit |  |  |  |",
        )

    def test_save_route_prefers_current_editor_defaults_over_stored_owner_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
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

                app.config["CONFIG_STORE"].save(
                    {
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    },
                    "google:teammate@npt.sg",
                )

                response = client.post(
                    "/config/save-route",
                    json={
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nUC | SG | User",
                        "component_default_rules_text": (
                            "DBP-Anti-fraud | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2\n"
                            "User | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2"
                        ),
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertIn(
            "DBP-Anti-fraud | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            payload["component_default_rules_text"],
        )
        self.assertIn(
            "User | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            payload["component_default_rules_text"],
        )


if __name__ == "__main__":
    unittest.main()
