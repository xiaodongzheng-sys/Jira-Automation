import io
import os
import tempfile
import time
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from openpyxl import load_workbook

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.models import CreatedTicket
from bpmis_jira_tool.web import (
    _build_team_profiles_for_display,
    _classify_portal_error,
    _load_effective_team_profiles,
    _build_productization_argos_translator,
    _clean_productization_detail_line,
    _clean_productization_detail_text,
    _hydrate_setup_defaults,
    _looks_like_productization_outline_fragment,
    _normalize_productization_issue_row,
    _normalize_productization_ticket_url,
    _results_for_display,
    _resolve_bpmis_access_token,
    _serialize_productization_version_candidate,
    _summarize_productization_detail,
    _translate_productization_text_to_english,
    create_app,
)


class _PortalFakeBPMISClient:
    def list_biz_projects_for_pm_email(self, _email):
        return [{"issue_id": "225159", "project_name": "Fraud Rule Upgrade", "market": "SG"}]

    def get_brd_doc_links_for_projects(self, issue_ids):
        return {issue_id: ["https://docs/brd"] for issue_id in issue_ids}

    def create_jira_ticket(self, project, fields, *, preformatted_summary=False):
        self.last_create = (project, fields, preformatted_summary)
        return CreatedTicket(ticket_key="AF-1", ticket_link="https://jira/browse/AF-1", raw={"ok": True})

    def get_jira_ticket_detail(self, ticket_key):
        return {
            "jiraKey": ticket_key,
            "summary": "Live Fraud Task",
            "status": {"label": "In Progress"},
            "fixVersionId": [{"fullName": "Planning_26Q2"}],
        }


class WebPortalFeatureTests(unittest.TestCase):
    def test_classify_portal_error_categorizes_duplicate_route_rule(self):
        details = _classify_portal_error(
            ToolError(
                "Duplicate System + Market -> Component rule on line 2. Each System + Market pair must map to exactly one Component."
            )
        )

        self.assertEqual(details["error_category"], "config_validation")
        self.assertEqual(details["error_code"], "duplicate_route_rule")
        self.assertFalse(details["error_retryable"])

    def test_hydrate_setup_defaults_applies_team_defaults_and_logged_in_email(self):
        hydrated = _hydrate_setup_defaults(
            {"pm_team": "AF", "need_uat_by_market": {}},
            {"email": "teammate@npt.sg"},
        )

        self.assertIn("AF | SG | DBP-Anti-fraud", hydrated["component_route_rules_text"])
        self.assertIn("AF | ID | DBP-Anti-fraud", hydrated["component_route_rules_text"])
        self.assertIn("AF | PH | DBP-Anti-fraud", hydrated["component_route_rules_text"])
        self.assertIn("UC | SG | User", hydrated["component_route_rules_text"])
        self.assertIn("FE | SG | FE-Anti-fraud,FE-User", hydrated["component_route_rules_text"])
        self.assertIn("CC | SG | CardCenter", hydrated["component_route_rules_text"])
        self.assertIn(
            "Deposit | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            hydrated["component_default_rules_text"],
        )
        self.assertIn(
            "User | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            hydrated["component_default_rules_text"],
        )
        self.assertIn(
            "FE-Anti-fraud,FE-User | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            hydrated["component_default_rules_text"],
        )
        self.assertIn(
            "CardCenter | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            hydrated["component_default_rules_text"],
        )
        self.assertEqual("Need UAT_by UAT Team", hydrated["need_uat_by_market"]["SG"])
        self.assertEqual("P1", hydrated["priority_value"])
        self.assertEqual("teammate@npt.sg", hydrated["sync_pm_email"])
        self.assertEqual("teammate@npt.sg", hydrated["product_manager_value"])

    def test_hydrate_setup_defaults_can_use_admin_team_profile_override(self):
        hydrated = _hydrate_setup_defaults(
            {"pm_team": "AF", "need_uat_by_market": {}},
            {"email": "teammate@npt.sg"},
            team_profiles={
                "AF": {
                    "label": "Anti-fraud",
                    "ready": True,
                    "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nUC | SG | User",
                    "component_default_rules_text": (
                        "DBP-Anti-fraud | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | Planning_26Q2\n"
                        "User | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | Planning_26Q2"
                    ),
                }
            },
        )

        self.assertEqual(
            "AF | SG | DBP-Anti-fraud\nUC | SG | User",
            hydrated["component_route_rules_text"],
        )
        self.assertIn(
            "User | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            hydrated["component_default_rules_text"],
        )

    def test_results_for_display_hides_skipped_rows_by_default(self):
        results = _results_for_display(
            [
                {"row_number": 2, "status": "skipped", "message": "Skipped because Jira Ticket Link already has a value."},
                {"row_number": 14, "status": "created", "message": "Created Jira ticket successfully."},
                {"row_number": 15, "status": "error", "message": "Could not resolve BPMIS Jira user."},
            ]
        )

        self.assertEqual([14, 15], [item["row_number"] for item in results])

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
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
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
                "BRD Link",
                "Market",
                "System",
                "Jira Title",
                "PRD Link",
                "Description",
                "Jira Ticket Link",
            ],
        )
        self.assertEqual("225159", worksheet["A2"].value)
        self.assertEqual("https://docs.google.com/document/d/example", worksheet["C2"].value)
        self.assertEqual("SG", worksheet["D2"].value)
        self.assertEqual("https://confluence/example-prd", worksheet["G2"].value)
        self.assertEqual("Detailed Jira description goes here.", worksheet["H2"].value)
        self.assertIsNone(worksheet["A1"].fill.fill_type)

    def test_index_hides_sheet_template_setup(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
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
        self.assertNotIn(b"Step 2 \xc2\xb7 Sheet Template", response.data)
        self.assertNotIn(b"Create a new Google Sheet from template", response.data)
        self.assertNotIn(b"data-create-template-sheet-button", response.data)
        self.assertNotIn(b"Download the default sheet template", response.data)

    def test_create_template_spreadsheet_endpoint_returns_new_sheet_link(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            app.config["CONFIG_STORE"].save(
                app.config["CONFIG_STORE"]._normalize(
                    {
                        "spreadsheet_link": "",
                        "input_tab_name": "Sheet1",
                    }
                ),
                "google:teammate@npt.sg",
            )

            with patch(
                "bpmis_jira_tool.web.GoogleSheetsService.create_template_spreadsheet",
                return_value={
                    "spreadsheet_id": "sheet-123",
                    "spreadsheet_url": "https://docs.google.com/spreadsheets/d/sheet-123/edit",
                    "input_tab_name": "Sheet1",
                    "spreadsheet_title": "BPMIS Automation Tool",
                },
            ) as mocked_create:
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.post("/api/spreadsheets/create-template")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["spreadsheet_id"], "sheet-123")
        self.assertEqual(payload["input_tab_name"], "Sheet1")
        mocked_create.assert_called_once()

    def test_healthz_sets_request_id_header(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers.get("X-Request-ID"))

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

    def test_shared_mode_index_skips_google_sheet_read_when_spreadsheet_link_is_blank(self):
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
        ), patch("bpmis_jira_tool.web.GoogleSheetsService.read_snapshot") as read_snapshot:
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "new-user@npt.sg", "name": "New User"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Google Sheets request failed", response.data)
        self.assertIn(b'id="spreadsheet_link" name="spreadsheet_link" value=""', response.data)
        self.assertIn(b'id="input_tab_name" name="input_tab_name" value="Sheet1"', response.data)
        self.assertIn(b'name="summary_header" value="Jira Title"', response.data)
        read_snapshot.assert_not_called()

    def test_shared_mode_index_skips_google_sheet_read_with_partial_google_credentials(self):
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
        ), patch("bpmis_jira_tool.web.GoogleSheetsService.read_snapshot") as read_snapshot:
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "new-user@npt.sg", "name": "New User"}
                    session["google_credentials"] = {"token": "x"}

                app.config["CONFIG_STORE"].save(
                    {
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Sheet1",
                    },
                    "google:new-user@npt.sg",
                )
                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        read_snapshot.assert_not_called()

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
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
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
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "setup-run-user"

                response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b">Setup<", response.data)
        self.assertIn(b">My Projects<", response.data)
        self.assertNotIn(b">Run<", response.data)
        self.assertIn(b">Productization Upgrade Summary<", response.data)
        self.assertNotIn(b">Overview<", response.data)
        self.assertNotIn(b">Support<", response.data)
        self.assertNotIn(b"Self-Check", response.data)
        self.assertIn(b"Apply Team Defaults", response.data)
        self.assertIn(b"PM Team Change", response.data)
        self.assertNotIn(b"Search Version", response.data)
        self.assertNotIn(b"Matching Versions", response.data)
        self.assertIn(b"Version Keyword", response.data)
        self.assertIn(b"Type a version keyword", response.data)
        self.assertIn(b"Copy whole table", response.data)
        self.assertIn(b"Jira Link", response.data)
        self.assertIn(b"productization_upgrade_summary.js", response.data)
        self.assertIn(b"userInitiatedTeamChange", response.data)
        self.assertIn(b"resetConfirmModalState", response.data)
        self.assertIn(b"syncSelectedTeamBaseline", response.data)
        self.assertIn(b"classList.add('is-visible')", response.data)
        self.assertIn(b"classList.remove('is-visible')", response.data)
        self.assertIn(b"classList.add('pm-confirm-visible')", response.data)
        self.assertIn(b"classList.remove('pm-confirm-visible')", response.data)
        self.assertIn(b"data-initial-team=", response.data)
        self.assertIn(b"productization_upgrade_summary.js", response.data)

    def test_new_user_defaults_to_setup_then_configured_user_defaults_to_my_projects(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "new-user"
                first_response = client.get("/")

            app.config["CONFIG_STORE"].save({"pm_team": "AF", "sync_pm_email": "owner@npt.sg"}, user_key="anon:configured-user")
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "configured-user"
                second_response = client.get("/")

        self.assertEqual(first_response.status_code, 200)
        self.assertIn(b'data-default-tab="setup"', first_response.data)
        self.assertEqual(second_response.status_code, 200)
        self.assertIn(b'data-default-tab="run"', second_response.data)
        self.assertIn(b">My Projects<", second_response.data)

    def test_index_shows_team_default_admin_tab_only_for_admin_user(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x"}
                admin_response = client.get("/")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                user_response = client.get("/")

        self.assertEqual(admin_response.status_code, 200)
        self.assertIn(b">Team Default Admin<", admin_response.data)
        self.assertIn(b"Save Anti-fraud Defaults", admin_response.data)
        self.assertNotIn(b">Team Default Admin<", user_response.data)

    def test_team_default_admin_save_persists_route_override(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x"}

                response = client.post(
                    "/admin/team-profiles/save",
                    data={
                        "team_key": "AF",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nUC | SG | User",
                    },
                    follow_redirects=False,
                )

                profiles = _load_effective_team_profiles(app.config["CONFIG_STORE"])

        self.assertEqual(response.status_code, 302)
        self.assertEqual("AF | SG | DBP-Anti-fraud\nUC | SG | User", profiles["AF"]["component_route_rules_text"])
        self.assertIn(
            "__CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | __CURRENT_USER_EMAIL__ | Planning_26Q2",
            profiles["AF"]["component_default_rules_text"],
        )

    def test_productization_versions_api_returns_normalized_candidates(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
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
                "TEAM_ALLOWED_EMAILS": "",
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
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.get("/api/productization-upgrade-summary/issues?version_id=88")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["items"][0]["jira_ticket_number"], "ABC-101")
        self.assertEqual(payload["items"][0]["jira_ticket_url"], "https://jira.shopee.io/browse/ABC-101")
        self.assertEqual(payload["items"][0]["feature_summary"], "Upgrade wallet flow")
        self.assertEqual(payload["items"][0]["pm"], "Alice PM")
        self.assertEqual(payload["items"][0]["prd_links"], [{"label": "https://confluence/prd-1", "url": "https://confluence/prd-1"}])

    def test_productization_issues_api_filters_to_anti_fraud_components_for_af_team(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            app.config["CONFIG_STORE"].save(
                app.config["CONFIG_STORE"]._normalize(
                    {
                        "pm_team": "AF",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    }
                ),
                "google:teammate@npt.sg",
            )

            fake_client = type(
                "FakeProductizationClient",
                (),
                {
                    "list_issues_for_version": lambda self, version_id: [
                        {"jiraKey": "AF-1", "summary": "Keep me", "componentId": [{"label": "DBP-Anti-fraud"}]},
                        {"jiraKey": "AF-2", "summary": "Keep me too", "component": "Anti-fraud"},
                        {"jiraKey": "ABC-1", "summary": "Filter me out", "componentId": [{"label": "Payments"}]},
                    ],
                },
            )()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.get("/api/productization-upgrade-summary/issues?version_id=88")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual([item["jira_ticket_number"] for item in payload["items"]], ["AF-1", "AF-2"])
        self.assertTrue(payload["team_filter_applied"])
        self.assertEqual(payload["raw_count"], 3)
        self.assertEqual(payload["filtered_count"], 2)

    def test_productization_issues_api_can_show_all_before_team_filtering_for_af_team(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            app.config["CONFIG_STORE"].save(
                app.config["CONFIG_STORE"]._normalize(
                    {
                        "pm_team": "AF",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    }
                ),
                "anon:productization-user",
            )

            fake_client = type(
                "FakeProductizationClient",
                (),
                {
                    "list_issues_for_version": lambda self, version_id: [
                        {"jiraKey": "AF-1", "summary": "Keep me", "componentId": [{"label": "DBP-Anti-fraud"}]},
                        {"jiraKey": "ABC-1", "summary": "Show me too", "componentId": [{"label": "Payments"}]},
                    ],
                },
            )()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.get("/api/productization-upgrade-summary/issues?version_id=88&show_all_before_team_filtering=1")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual([item["jira_ticket_number"] for item in payload["items"]], ["AF-1", "ABC-1"])
        self.assertFalse(payload["team_filter_applied"])
        self.assertTrue(payload["show_all_before_team_filtering"])
        self.assertEqual(payload["raw_count"], 2)
        self.assertEqual(payload["filtered_count"], 2)

    def test_productization_issues_api_does_not_filter_for_non_af_team(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            app.config["CONFIG_STORE"].save(
                app.config["CONFIG_STORE"]._normalize(
                    {
                        "pm_team": "CRMS",
                        "component_route_rules_text": "CRMS | SG | Loan&CreditRisk",
                        "component_default_rules_text": "Loan&CreditRisk | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    }
                ),
                "anon:productization-user",
            )

            fake_client = type(
                "FakeProductizationClient",
                (),
                {
                    "list_issues_for_version": lambda self, version_id: [
                        {"jiraKey": "CR-1", "summary": "Credit risk", "componentId": [{"label": "Loan&CreditRisk"}]},
                        {"jiraKey": "ABC-1", "summary": "Payments too", "componentId": [{"label": "Payments"}]},
                    ],
                },
            )()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.get("/api/productization-upgrade-summary/issues?version_id=88")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual([item["jira_ticket_number"] for item in payload["items"]], ["CR-1", "ABC-1"])
        self.assertFalse(payload["team_filter_applied"])
        self.assertEqual(payload["raw_count"], 2)
        self.assertEqual(payload["filtered_count"], 2)

    def test_productization_versions_api_returns_json_for_unexpected_errors(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            fake_client = type("BrokenProductizationClient", (), {})()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"

                    response = client.get("/api/productization-upgrade-summary/versions?q=26Q2")

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertIn("Unable to search versions right now", payload["message"])

    def test_productization_issues_api_returns_json_for_unexpected_errors(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            fake_client = type(
                "BrokenProductizationClient",
                (),
                {"list_issues_for_version": lambda self, version_id: (_ for _ in ()).throw(RuntimeError("boom"))},
            )()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.get("/api/productization-upgrade-summary/issues?version_id=88")

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertIn("Unable to load upgrade tickets right now", payload["message"])

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

    def test_clean_productization_detail_text_strips_html(self):
        cleaned = _clean_productization_detail_text("<p>First line.</p><p>Second line.</p>")

        self.assertEqual(cleaned, "First line.\nSecond line.")

    def test_clean_productization_detail_text_removes_links_and_noise_prefixes(self):
        cleaned = _clean_productization_detail_text(
            """
            <p>[Productization effort] Add fields to ivlog, refer to ivlog sheet:</p>
            <p>[https://docs.google.com/spreadsheets/d/example/edit#gid=123]</p>
            <p>PRD: https://confluence.shopee.io/x/abc123</p>
            """
        )

        self.assertEqual(cleaned, "Add fields to ivlog")

    def test_clean_productization_detail_line_removes_reference_phrases(self):
        self.assertEqual(
            _clean_productization_detail_line("Add fields to ivlog, refer to ivlog sheet"),
            "Add fields to ivlog",
        )
        self.assertEqual(
            _clean_productization_detail_line("Drainage改造：2way改造，支持FE展示，详见：PRD"),
            "Drainage改造：2way改造，支持FE展示",
        )

    def test_clean_productization_detail_line_removes_section_and_urls(self):
        cleaned = _clean_productization_detail_line(
            "[https://confluence.shopee.io/x/ww6Itw] section 4.6 PRD: https://confluence.shopee.io/x/ww6Itw"
        )

        self.assertEqual(cleaned, "")

    def test_clean_productization_detail_line_removes_low_value_versions_and_row_references(self):
        cleaned = _clean_productization_detail_line(
            "Upgrade product version: v2.0.33_2060410 PRD: scenarioL1 = MCCManualSplit 4. new scenario for MCA v1.1(FCYConversion for retail and SME, main track Jira: ) new scenarios for card self-uplift Authentication Scenarios Row 499-501 2."
        )

        self.assertNotIn("v2.0.33_2060410", cleaned)
        self.assertNotIn("v1.1", cleaned)
        self.assertNotIn("Row 499-501", cleaned)
        self.assertNotIn("scenarioL1", cleaned)
        self.assertIn("new scenario for MCA", cleaned)

    def test_clean_productization_detail_line_removes_antifraud_version_plan_header(self):
        cleaned = _clean_productization_detail_line(
            "SG AntiFraud verision plan _feature timeline: new scenarios for card self-uplift"
        )

        self.assertEqual(cleaned, "new scenarios for card self-uplift")

    def test_clean_productization_detail_line_keeps_actionable_feature_sentence(self):
        cleaned = _clean_productization_detail_line(
            "F30 function update to add new identifier F30 enhancement to support card self-uplift"
        )

        self.assertEqual(
            cleaned,
            "F30 function update to add new identifier F30 enhancement to support card self-uplift",
        )

    def test_outline_fragment_detection_filters_directory_like_text(self):
        self.assertTrue(_looks_like_productization_outline_fragment("FE UI Pages"))
        self.assertTrue(_looks_like_productization_outline_fragment("UI Data Points"))
        self.assertFalse(
            _looks_like_productization_outline_fragment("Add FE UI pages for multi-currency account settings")
        )

    def test_productization_summary_normalizes_ticket_url_from_issue_key(self):
        normalized_issue = _normalize_productization_issue_row(
            {
                "jiraKey": "ABC-88",
                "jiraLink": "ABC-88",
                "summary": "Improve onboarding",
            }
        )

        self.assertEqual(normalized_issue["jira_ticket_number"], "ABC-88")
        self.assertEqual(normalized_issue["jira_ticket_url"], "https://jira.shopee.io/browse/ABC-88")

    def test_normalize_productization_ticket_url_preserves_full_url(self):
        self.assertEqual(
            _normalize_productization_ticket_url("https://jira.shopee.io/browse/ABC-99"),
            "https://jira.shopee.io/browse/ABC-99",
        )

    def test_productization_summary_returns_full_cleaned_text_without_summarizing(self):
        summary = _summarize_productization_detail("<p>First line.</p><p>Second line.</p>")

        self.assertEqual(summary, "First line.\nSecond line.")

    def test_productization_summary_does_not_append_ellipsis_for_long_text(self):
        long_summary = " ".join(["Detailed"] * 80)
        summary = _summarize_productization_detail(long_summary)

        self.assertEqual(summary, long_summary)
        self.assertNotIn("...", summary)

    def test_productization_summary_keeps_chinese_text_without_translation(self):
        summary = _summarize_productization_detail("支持FE展示；新增字段")

        self.assertEqual(summary, "支持FE展示；新增字段")

    def test_translate_productization_text_to_english_translates_cjk_lines(self):
        fake_translator = type("FakeTranslator", (), {"translate": lambda self, text: "Support FE display"})()

        with patch("bpmis_jira_tool.web._build_productization_argos_translator", return_value=fake_translator):
            translated = _translate_productization_text_to_english("支持FE展示")

        self.assertEqual(translated, "Support FE display")

    def test_translate_productization_text_to_english_keeps_english_lines(self):
        with patch("bpmis_jira_tool.web._build_productization_argos_translator", return_value=None):
            translated = _translate_productization_text_to_english("Add fields to ivlog")

        self.assertEqual(translated, "Add fields to ivlog")

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

    def test_save_mapping_config_forces_sync_email_for_non_allowlisted_google_user(self):
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
                        "bpmis_api_access_token": "",
                        "pm_team": "AF",
                        "sync_pm_email": "someone-else@npt.sg",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                        "market_header": "Market",
                        "system_header": "System",
                    },
                    follow_redirects=False,
                )

            self.assertEqual(response.status_code, 302)
            saved = app.config["CONFIG_STORE"].load("google:teammate@npt.sg")
            self.assertEqual(saved["sync_pm_email"], "teammate@npt.sg")

    def test_save_mapping_config_allows_sync_email_for_allowlisted_user(self):
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
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Owner"}
                    session["google_credentials"] = {"token": "x"}

                response = client.post(
                    "/config/save",
                    data={
                        "bpmis_api_access_token": "",
                        "pm_team": "AF",
                        "sync_pm_email": "other-pm@npt.sg",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                        "market_header": "Market",
                        "system_header": "System",
                    },
                    follow_redirects=False,
                )

            self.assertEqual(response.status_code, 302)
            saved = app.config["CONFIG_STORE"].load("google:xiaodong.zheng@npt.sg")
            self.assertEqual(saved["sync_pm_email"], "other-pm@npt.sg")

    def test_sync_bpmis_projects_job_does_not_require_google_credentials(self):
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
        ), patch("bpmis_jira_tool.web.build_bpmis_client", return_value=_PortalFakeBPMISClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "sync-user"
                app.config["CONFIG_STORE"].save(
                    {
                        "pm_team": "AF",
                        "sync_pm_email": "pm@npt.sg",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    },
                    "anon:sync-user",
                )

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

    def test_bpmis_projects_api_delete_is_user_scoped(self):
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
            store = app.config["BPMIS_PROJECT_STORE"]
            store.upsert_project(user_key="anon:first", bpmis_id="225159", project_name="First", brd_link="", market="SG")
            store.upsert_project(user_key="anon:second", bpmis_id="225159", project_name="Second", brd_link="", market="SG")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "first"

                delete_response = client.delete("/api/bpmis-projects/225159")
                list_response = client.get("/api/bpmis-projects")

            self.assertEqual(delete_response.status_code, 200)
            self.assertEqual(list_response.get_json()["projects"], [])
            self.assertEqual(len(store.list_projects(user_key="anon:second")), 1)

    def test_create_jira_api_returns_partial_results_and_saves_links(self):
        fake_client = _PortalFakeBPMISClient()
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
        ), patch("bpmis_jira_tool.web.build_bpmis_client", return_value=fake_client):
            app = create_app()
            app.testing = True
            app.config["CONFIG_STORE"].save(
                {
                    "pm_team": "AF",
                    "sync_pm_email": "pm@npt.sg",
                    "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                    "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                    "priority_value": "P1",
                    "product_manager_value": "pm@npt.sg",
                    "reporter_value": "pm@npt.sg",
                    "biz_pic_value": "pm@npt.sg",
                },
                "anon:create-user",
            )
            app.config["BPMIS_PROJECT_STORE"].upsert_project(
                user_key="anon:create-user",
                bpmis_id="225159",
                project_name="Fraud Rule Upgrade",
                brd_link="",
                market="SG",
            )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "create-user"

                response = client.post(
                    "/api/bpmis-projects/225159/jira-tickets",
                    json={
                        "items": [
                            {
                                "component": "DBP-Anti-fraud",
                                "market": "SG",
                                "jira_title": "[Feature][AF]Fraud Rule Upgrade",
                                "fix_version": "Planning_26Q2",
                                "prd_link": "",
                                "description": "",
                            }
                        ]
                    },
                )

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["results"][0]["status"], "created")
            self.assertTrue(fake_client.last_create[2])
            self.assertEqual(
                app.config["BPMIS_PROJECT_STORE"].list_projects(user_key="anon:create-user")[0]["jira_tickets"][0]["ticket_link"],
                "https://jira/browse/AF-1",
            )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "create-user"
                tickets_response = client.get("/api/bpmis-projects/225159/jira-tickets")

            self.assertEqual(tickets_response.status_code, 200)
            tickets_payload = tickets_response.get_json()
            self.assertEqual(tickets_payload["tickets"][0]["live_jira_title"], "Live Fraud Task")
            self.assertEqual(tickets_payload["tickets"][0]["live_jira_status"], "In Progress")
            self.assertEqual(tickets_payload["tickets"][0]["live_fix_version"], "Planning_26Q2")

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
                    "FE-Anti-fraud,FE-User | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
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
        self.assertIn(b"Save Step 3A first", response.data)
        self.assertIn(b"Save Route", response.data)
        self.assertIn(b"System Reference", response.data)
        self.assertIn(b"data-component-owner-editor-body", response.data)
        self.assertIn(b"data-route-save-button", response.data)
        self.assertIn(b"data-route-save-status", response.data)
        self.assertNotIn(b"Coverage Check", response.data)

    def test_save_route_endpoint_returns_json_and_aligned_component_defaults(self):
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

                app.config["CONFIG_STORE"].save(
                    {
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                        "component_default_rules_text": "DBP-Anti-fraud | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
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

                saved = app.config["CONFIG_STORE"].load("google:teammate@npt.sg")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["component_route_rules_text"], "AF | SG | DBP-Anti-fraud\nDC | SG | Deposit")
        self.assertIn(
            "DBP-Anti-fraud | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
            payload["component_default_rules_text"],
        )
        self.assertIn("Deposit |  |  |  |", payload["component_default_rules_text"])
        self.assertEqual(saved["component_route_rules_text"], payload["component_route_rules_text"])
        self.assertEqual(saved["component_default_rules_text"], payload["component_default_rules_text"])

    def test_save_route_endpoint_allows_multiple_routes_to_share_one_component(self):
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

                app.config["CONFIG_STORE"].save(
                    {
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "CRMS DWH | ID | DWH_CreditRisk",
                        "component_default_rules_text": "DWH_CreditRisk | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
                    },
                    "google:teammate@npt.sg",
                )

                response = client.post(
                    "/config/save-route",
                    json={
                        "pm_team": "AF",
                        "system_header": "System",
                        "market_header": "Market",
                        "component_route_rules_text": "CRMS DWH | ID | DWH_CreditRisk\nCRMS DWH | PH | DWH_CreditRisk",
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(
            payload["component_default_rules_text"],
            "DWH_CreditRisk | teammate@npt.sg | teammate@npt.sg | teammate@npt.sg | Planning_26Q2",
        )

    def test_save_route_endpoint_logs_route_validation_failures_with_context(self):
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

                with self.assertLogs(app.logger.name, level="WARNING") as captured:
                    response = client.post(
                        "/config/save-route",
                        json={
                            "pm_team": "AF",
                            "system_header": "System",
                            "market_header": "Market",
                            "component_route_rules_text": "AF | SG | DBP-Anti-fraud\nAF | SG | Anti-fraud",
                        },
                    )

        self.assertEqual(response.status_code, 400)
        combined_logs = "\n".join(captured.output)
        self.assertIn("config_save_route_tool_error", combined_logs)
        self.assertIn("\"error_category\": \"config_validation\"", combined_logs)
        self.assertIn("\"error_code\": \"duplicate_route_rule\"", combined_logs)
        self.assertIn("\"error_retryable\": false", combined_logs)
        self.assertIn("\"route_rule_count\": 2", combined_logs)
        self.assertIn("\"pm_team\": \"AF\"", combined_logs)
        self.assertIn("Duplicate System + Market -> Component rule", combined_logs)

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
