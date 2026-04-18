import io
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from openpyxl import load_workbook

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.web import _resolve_bpmis_access_token, create_app


class WebPortalFeatureTests(unittest.TestCase):
    def test_resolve_bpmis_access_token_prefers_saved_portal_value(self):
        settings = Settings(
            flask_secret_key="test-secret",
            google_oauth_client_secret_file="client.json",
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url=None,
            team_allowed_emails=(),
            team_allowed_email_domains=(),
            team_portal_data_dir=".",
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Projects",
            bpmis_base_url="https://example.com",
            bpmis_api_access_token="env-token",
        )

        resolved = _resolve_bpmis_access_token({"bpmis_api_access_token": "portal-token"}, settings)

        self.assertEqual("portal-token", resolved)

    def test_resolve_bpmis_access_token_falls_back_to_env(self):
        settings = Settings(
            flask_secret_key="test-secret",
            google_oauth_client_secret_file="client.json",
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url=None,
            team_allowed_emails=(),
            team_allowed_email_domains=(),
            team_portal_data_dir=".",
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Projects",
            bpmis_base_url="https://example.com",
            bpmis_api_access_token="env-token",
        )

        resolved = _resolve_bpmis_access_token({"bpmis_api_access_token": ""}, settings)

        self.assertEqual("env-token", resolved)

    def test_default_sheet_template_download_returns_xlsx(self):
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
                response = client.get("/download/default-sheet-template.xlsx")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.mimetype,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        workbook = load_workbook(io.BytesIO(response.data))
        worksheet = workbook.active
        self.assertEqual("Projects", worksheet.title)
        self.assertEqual(
            [cell.value for cell in worksheet[1]],
            [
                "BPMIS ID",
                "Project Name",
                "Market",
                "System",
                "Jira Title",
                "PRD Link",
                "Description",
                "BRD Link",
                "Jira Ticket Link",
            ],
        )
        self.assertEqual("225159", worksheet["A2"].value)
        self.assertEqual("https://confluence/example-prd", worksheet["F2"].value)
        self.assertEqual("Detailed Jira description goes here.", worksheet["G2"].value)
        self.assertEqual("https://docs.google.com/document/d/example", worksheet["H2"].value)

    def test_self_check_uses_saved_bpmis_token(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ), patch("bpmis_jira_tool.web.build_bpmis_client") as mock_build_client:
            mock_build_client.return_value = MagicMock()
            app = create_app()
            app.testing = True
            config_store = app.config["CONFIG_STORE"]
            config_store.save(
                {
                    "bpmis_api_access_token": "portal-token",
                    "spreadsheet_link": "",
                    "input_tab_name": "Projects",
                },
                user_key="anon:test-user",
            )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "test-user"

                response = client.get("/api/self-check")

        self.assertEqual(response.status_code, 200)
        mock_build_client.assert_called_once()
        self.assertEqual(mock_build_client.call_args.kwargs["access_token"], "portal-token")
        payload = response.get_json()
        bpmis_check = next(check for check in payload["checks"] if check["name"] == "BPMIS API")
        self.assertIn("saved BPMIS token", bpmis_check["detail"])

    def test_save_mapping_config_persists_bpmis_token(self):
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
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "save-token-user"

                response = client.post(
                    "/config/save",
                    data={
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Projects",
                        "bpmis_api_access_token": "portal-token",
                        "issue_id_header": "BPMIS ID",
                        "jira_ticket_link_header": "Jira Ticket Link",
                        "sync_pm_email": "pm@example.com",
                        "sync_project_name_header": "Project Name",
                        "sync_market_header": "Market",
                        "sync_brd_link_header": "BRD Link",
                        "market_header": "Market",
                        "system_header": "System",
                        "summary_header": "Jira Title",
                        "prd_links_header": "PRD Link",
                        "description_header": "Description",
                        "task_type_value": "Feature",
                        "priority_value": "P1",
                        "product_manager_value": "pm@example.com",
                        "reporter_value": "reporter@example.com",
                        "biz_pic_value": "biz@example.com",
                    },
                    follow_redirects=False,
                )

            self.assertEqual(response.status_code, 302)
            saved = app.config["CONFIG_STORE"].load("anon:save-token-user")
            self.assertEqual(saved["bpmis_api_access_token"], "portal-token")


if __name__ == "__main__":
    unittest.main()
