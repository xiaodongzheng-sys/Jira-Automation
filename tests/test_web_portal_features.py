import io
import os
import tempfile
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from openpyxl import load_workbook

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.web import (
    _build_team_profiles_for_display,
    _hydrate_setup_defaults,
    _normalize_productization_issue_row,
    _resolve_bpmis_access_token,
    _serialize_productization_version_candidate,
    _summarize_productization_detail,
    create_app,
)


class WebPortalFeatureTests(unittest.TestCase):
    def test_hydrate_setup_defaults_applies_team_defaults_and_logged_in_email(self):
        hydrated = _hydrate_setup_defaults(
            {"pm_team": "AF", "need_uat_by_market": {}},
            {"email": "teammate@npt.sg"},
        )

        self.assertIn("AF | SG | DBP-Anti-fraud", hydrated["component_route_rules_text"])
        self.assertIn(
            "Deposit | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            hydrated["component_default_rules_text"],
        )
        self.assertEqual("Need UAT_by UAT Team", hydrated["need_uat_by_market"]["SG"])
        self.assertEqual("teammate@npt.sg", hydrated["sync_pm_email"])
        self.assertEqual("teammate@npt.sg", hydrated["product_manager_value"])

    def test_build_team_profiles_for_display_replaces_placeholder_with_user_email(self):
        profiles = _build_team_profiles_for_display(
            {"product_manager_value": "fallback@npt.sg"},
            {"email": "teammate@npt.sg"},
        )

        self.assertIn("teammate@npt.sg", profiles["AF"]["component_default_rules_text"])
        self.assertNotIn("__CURRENT_USER_EMAIL__", profiles["AF"]["component_default_rules_text"])

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
            input_tab_name="Sheet1",
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
            input_tab_name="Sheet1",
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
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
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
        self.assertEqual("Sheet1", worksheet.title)
        self.assertEqual(
            [cell.value for cell in worksheet[1]],
            [
                "BPMIS ID",
                "Project Name",
                "Market",
                "BRD Link",
                "System",
                "Jira Title",
                "PRD Link",
                "Description",
                "Jira Ticket Link",
            ],
        )
        self.assertEqual("225159", worksheet["A2"].value)
        self.assertEqual("https://docs.google.com/document/d/example", worksheet["D2"].value)
        self.assertEqual("https://confluence/example-prd", worksheet["G2"].value)
        self.assertEqual("Detailed Jira description goes here.", worksheet["H2"].value)

    def test_shared_mode_blocks_anonymous_save(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.post("/config/save", data={"spreadsheet_link": "sheet-123"}, follow_redirects=False)

        self.assertEqual(response.status_code, 302)

    def test_shared_mode_requires_encryption_key_before_saving_portal_token(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
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

                response = client.post(
                    "/config/save",
                    data={
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Sheet1",
                        "bpmis_api_access_token": "portal-token",
                    },
                    follow_redirects=True,
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"TEAM_PORTAL_CONFIG_ENCRYPTION_KEY", response.data)

    def test_shared_mode_saves_encrypted_portal_token_for_google_user(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": Fernet.generate_key().decode("utf-8"),
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.post(
                    "/config/save",
                    data={
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Sheet1",
                        "bpmis_api_access_token": "portal-token",
                        "pm_team": "AF",
                        "issue_id_header": "BPMIS ID",
                        "jira_ticket_link_header": "Jira Ticket Link",
                        "sync_pm_email": "pm@example.com",
                        "sync_project_name_header": "Project Name",
                        "sync_market_header": "Market",
                        "sync_brd_link_header": "BRD Link",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
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
            raw_row = app.config["CONFIG_STORE"]._fetch_row("google:teammate@npt.sg")
            self.assertIn('"bpmis_api_access_token": "enc:', raw_row)
            saved = app.config["CONFIG_STORE"].load("google:teammate@npt.sg")
            self.assertEqual(saved["bpmis_api_access_token"], "portal-token")

    def test_save_mapping_config_persists_bpmis_token(self):
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
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "save-token-user"

                response = client.post(
                    "/config/save",
                    data={
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Sheet1",
                        "bpmis_api_access_token": "portal-token",
                        "pm_team": "AF",
                        "issue_id_header": "BPMIS ID",
                        "jira_ticket_link_header": "Jira Ticket Link",
                        "sync_pm_email": "pm@example.com",
                        "sync_project_name_header": "Project Name",
                        "sync_market_header": "Market",
                        "sync_brd_link_header": "BRD Link",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
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

    def test_index_renders_setup_run_and_productization_tabs(self):
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
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "setup-run-user"

                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b">Setup<", response.data)
        self.assertIn(b">Run<", response.data)
        self.assertIn(b">Productization Upgrade Summary<", response.data)
        self.assertNotIn(b">Overview<", response.data)
        self.assertNotIn(b">Support<", response.data)
        self.assertNotIn(b"Self-Check", response.data)
        self.assertIn(b"Apply Team Defaults", response.data)
        self.assertIn(b"PM Team Change", response.data)
        self.assertIn(b"Search Version", response.data)
        self.assertIn(b"JIRA Ticket Number", response.data)
        self.assertIn(b"userInitiatedTeamChange", response.data)
        self.assertIn(b"resetConfirmModalState", response.data)
        self.assertIn(b"syncSelectedTeamBaseline", response.data)
        self.assertIn(b"classList.add('is-visible')", response.data)
        self.assertIn(b"classList.remove('is-visible')", response.data)
        self.assertIn(b"classList.add('pm-confirm-visible')", response.data)
        self.assertIn(b"classList.remove('pm-confirm-visible')", response.data)
        self.assertIn(b"data-initial-team=", response.data)
        self.assertIn(b"productization_upgrade_summary.js", response.data)

    def test_productization_versions_api_returns_normalized_candidates(self):
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

            fake_client = type(
                "FakeProductizationClient",
                (),
                {
                    "search_versions": lambda self, query: [
                        {"id": 88, "fullName": "Planning_26Q2", "marketId": {"label": "SG"}}
                    ],
                },
            )()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"

                    response = client.get("/api/productization-upgrade-summary/versions?q=26Q2")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(
            payload["items"],
            [
                {
                    "version_id": "88",
                    "version_name": "Planning_26Q2",
                    "market": "SG",
                    "label": "Planning_26Q2 · SG",
                }
            ],
        )

    def test_productization_issues_api_returns_normalized_rows(self):
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

            fake_client = type(
                "FakeProductizationClient",
                (),
                {
                    "list_issues_for_version": lambda self, version_id: [
                        {
                            "jiraKey": "ABC-101",
                            "jiraLink": "https://jira.shopee.io/browse/ABC-101",
                            "summary": "Upgrade wallet flow",
                            "desc": "<p>Improve rollback handling.</p><p>Support retry.</p>",
                            "jiraRegionalPmPicId": [{"displayName": "Alice PM"}],
                            "jiraPrdLink": "https://confluence/prd-1",
                        }
                    ],
                },
            )()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"

                    response = client.get("/api/productization-upgrade-summary/issues?version_id=88")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["items"][0]["jira_ticket_number"], "ABC-101")
        self.assertEqual(payload["items"][0]["jira_ticket_url"], "https://jira.shopee.io/browse/ABC-101")
        self.assertEqual(payload["items"][0]["feature_summary"], "Upgrade wallet flow")
        self.assertEqual(payload["items"][0]["pm"], "Alice PM")
        self.assertEqual(payload["items"][0]["prd_links"], [{"label": "https://confluence/prd-1", "url": "https://confluence/prd-1"}])

    def test_productization_summary_helpers_normalize_missing_fields(self):
        normalized_version = _serialize_productization_version_candidate(
            {"id": 7, "fullName": "Planning_26Q2", "marketId": {"label": "SG"}}
        )
        normalized_issue = _normalize_productization_issue_row(
            {
                "jiraLink": "https://jira.shopee.io/browse/ABC-88",
                "summary": "Improve onboarding",
                "description": "First line.\nSecond line with details.",
            }
        )

        self.assertEqual(normalized_version["label"], "Planning_26Q2 · SG")
        self.assertEqual(normalized_issue["jira_ticket_number"], "ABC-88")
        self.assertEqual(normalized_issue["pm"], "-")
        self.assertEqual(normalized_issue["prd_links"], [])
        self.assertEqual(_summarize_productization_detail(""), "-")
        self.assertIn("First line.", normalized_issue["detailed_feature"])

    def test_save_mapping_config_fills_email_defaults_for_google_user(self):
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
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.post(
                    "/config/save",
                    data={
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Sheet1",
                        "bpmis_api_access_token": "",
                        "pm_team": "AF",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    },
                    follow_redirects=False,
                )

            self.assertEqual(response.status_code, 302)
            saved = app.config["CONFIG_STORE"].load("google:teammate@npt.sg")
            self.assertEqual(saved["sync_pm_email"], "teammate@npt.sg")
            self.assertEqual(saved["product_manager_value"], "teammate@npt.sg")
            self.assertEqual(saved["reporter_value"], "teammate@npt.sg")
            self.assertEqual(saved["biz_pic_value"], "teammate@npt.sg")

    def test_team_defaults_allow_saving_setup_without_manual_advanced_mapping(self):
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
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.post(
                    "/config/save",
                    data={
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Sheet1",
                        "pm_team": "AF",
                    },
                    follow_redirects=True,
                )

                saved = app.config["CONFIG_STORE"].load("google:teammate@npt.sg")
                self.assertIn("AF | SG | DBP-Anti-fraud", saved["component_route_rules_text"])
                self.assertIn(
                    "Loan&CreditRisk | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
                    saved["component_default_rules_text"],
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Your web Jira config was saved for this user", response.data)

    def test_index_renders_team_templates_with_actual_email_for_google_user(self):
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
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"teammate@npt.sg", response.data)
        self.assertNotIn(b"__CURRENT_USER_EMAIL__", response.data)
        self.assertIn(b"Component | Assignee | Dev PIC | QA PIC | Fix Version", response.data)
        self.assertIn(b"Template Preview", response.data)
        self.assertIn(b"data-mapping-preview-body=\"default\"", response.data)

    def test_index_recovers_legacy_component_defaults_for_google_user(self):
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
            app.config["CONFIG_STORE"]._upsert_row(
                "google:xiaodong.zheng@npt.sg",
                {
                    "spreadsheet_link": "sheet-123",
                    "component_by_market": {
                        "ID": "DBP-Anti-fraud",
                        "SG": "DBP-Anti-fraud",
                        "PH": "DBP-Anti-fraud",
                        "Regional": "Anti-fraud",
                    },
                    "assignee_value": "xiaodong.zheng@npt.sg",
                    "dev_pic_value": "xiaodong.zheng@npt.sg",
                    "qa_pic_value": "xiaodong.zheng@npt.sg",
                    "fix_version_value": "Planning_26Q2",
                },
            )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Planning_26Q2", response.data)
        self.assertNotIn(b"Recovered legacy config", response.data)
        self.assertNotIn(b"Legacy Market to Component", response.data)
        self.assertIn(b"Planning_26Q2", response.data)


if __name__ == "__main__":
    unittest.main()
