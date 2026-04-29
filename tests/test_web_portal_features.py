import io
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography.fernet import Fernet
from openpyxl import load_workbook

import bpmis_jira_tool.web as web_module
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.models import CreatedTicket
from bpmis_jira_tool.user_config import WebConfigStore
from bpmis_jira_tool.web import (
    _build_team_profiles_for_display,
    _classify_portal_error,
    _load_effective_team_profiles,
    _format_productization_description_text,
    _hydrate_setup_defaults,
    _normalize_productization_issue_row,
    _normalize_productization_ticket_url,
    _results_for_display,
    _resolve_bpmis_access_token,
    _parse_codex_json_object,
    _serialize_productization_version_candidate,
    create_app,
)


class _PortalFakeBPMISClient:
    def __init__(self):
        self.detail_calls = []
        self.status_calls = []
        self.version_calls = []
        self.delink_calls = []

    def list_biz_projects_for_pm_email(self, _email):
        return [{"issue_id": "225159", "project_name": "Fraud Rule Upgrade", "market": "SG"}]

    def get_brd_doc_links_for_projects(self, issue_ids):
        return {issue_id: ["https://docs/brd"] for issue_id in issue_ids}

    def create_jira_ticket(self, project, fields, *, preformatted_summary=False):
        self.last_create = (project, fields, preformatted_summary)
        return CreatedTicket(ticket_key="AF-1", ticket_link="https://jira/browse/AF-1", raw={"ok": True})

    def get_jira_ticket_detail(self, ticket_key):
        self.detail_calls.append(ticket_key)
        return {
            "jiraKey": ticket_key,
            "summary": "Live Fraud Task",
            "status": {"label": "In Progress"},
            "fixVersionId": [{"fullName": "Planning_26Q2"}],
        }

    def update_jira_ticket_status(self, ticket_key, status):
        self.status_calls.append((ticket_key, status))
        return {"jiraKey": ticket_key, "status": {"label": status}}

    def update_jira_ticket_fix_version(self, ticket_key, version_name, version_id=None):
        self.version_calls.append((ticket_key, version_name, version_id))
        return {"jiraKey": ticket_key, "fixVersions": [version_name]}

    def delink_jira_ticket_from_project(self, ticket_key, project_issue_id):
        self.delink_calls.append((ticket_key, project_issue_id))
        return {"jiraKey": ticket_key, "parentIds": []}


class _RemoteBPMISConfigClient:
    def __init__(self, data_root):
        self.store = WebConfigStore(data_root)

    def bpmis_config_load(self, *, user_key):
        return self.store.load(user_key)

    def bpmis_config_save(self, *, user_key, config):
        return self.store.save(config, user_key)

    def bpmis_config_migrate(self, *, from_user_key, to_user_key):
        self.store.migrate(from_user_key, to_user_key)

    def bpmis_team_profiles_load(self):
        return self.store.load_team_profiles()

    def bpmis_team_profile_save(self, *, team_key, profile):
        return self.store.save_team_profile(team_key, profile)


class _FakeLocalAgentConfigClient:
    def __init__(self):
        self.configs = {}

    def bpmis_config_load(self, *, user_key):
        return self.configs.get(user_key)

    def bpmis_config_save(self, *, user_key, config):
        self.configs[user_key] = dict(config)
        return self.configs[user_key]

    def bpmis_team_profiles_load(self):
        return {}


class _FakePRDReviewService:
    def __init__(self):
        self.requests = []

    def review(self, request):
        self.requests.append(request)
        if not request.prd_url:
            raise ToolError("PRD link is required.")
        return {
            "status": "ok",
            "cached": False,
            "review": {
                "jira_id": request.jira_id,
                "jira_link": request.jira_link,
                "prd_url": request.prd_url,
                "status": "completed",
                "result_markdown": "### Review\n- Good",
                "updated_at": "2026-04-28T00:00:00Z",
            },
            "prd": {"title": "PRD"},
        }


class _FakePRDReviewLocalAgentClient:
    def prd_review(self, payload):
        return {
            "status": "ok",
            "cached": True,
            "review": {
                "jira_id": payload["jira_id"],
                "status": "completed",
                "result_markdown": "### Cached",
                "updated_at": "2026-04-28T00:00:00Z",
            },
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

    def test_cloud_run_local_agent_mode_saves_setup_in_remote_persistent_store(self):
        with tempfile.TemporaryDirectory() as cloud_dir_one, tempfile.TemporaryDirectory() as cloud_dir_two, tempfile.TemporaryDirectory() as remote_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": cloud_dir_one,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
                "LOCAL_AGENT_MODE": "sync",
                "LOCAL_AGENT_BASE_URL": "https://agent.example",
                "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
                "LOCAL_AGENT_BPMIS_ENABLED": "true",
            },
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            remote_client = _RemoteBPMISConfigClient(Path(remote_dir))
            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=remote_client):
                app = create_app()
                app.testing = True

                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    response = client.post(
                        "/config/save",
                        data={
                            "spreadsheet_link": "",
                            "input_tab_name": "Sheet1",
                            "bpmis_api_access_token": "",
                            "pm_team": "AF",
                            "sync_pm_email": "spoofed@npt.sg",
                            "component_route_rules_text": "AF | SG | DBP-Anti-fraud",
                            "component_default_rules_text": "DBP-Anti-fraud | owner@npt.sg | dev@npt.sg | qa@npt.sg | Planning_26Q2",
                            "market_header": "Market",
                            "system_header": "System",
                            "task_type_value": "Feature",
                            "priority_value": "P1",
                            "product_manager_value": "pm@npt.sg",
                            "reporter_value": "reporter@npt.sg",
                            "biz_pic_value": "biz@npt.sg",
                        },
                        follow_redirects=False,
                    )

                self.assertEqual(response.status_code, 302)
                self.assertIsNone(app.config["CONFIG_STORE"].load("google:teammate@npt.sg"))
                remote_saved = remote_client.store.load("google:teammate@npt.sg")
                self.assertEqual(remote_saved["pm_team"], "AF")
                self.assertEqual(remote_saved["sync_pm_email"], "teammate@npt.sg")

                os.environ["TEAM_PORTAL_DATA_DIR"] = cloud_dir_two
                app_after_redeploy = create_app()
                app_after_redeploy.testing = True
                with app_after_redeploy.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    index_response = client.get("/")

                self.assertEqual(index_response.status_code, 200)
                self.assertIn(b'value="AF" selected', index_response.data)
                self.assertIn(b'data-default-tab="run"', index_response.data)

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
        self.assertIn(b"Generate LLM Description", response.data)
        self.assertIn(b"data-productization-llm-description-button", response.data)
        self.assertNotIn(b"data-productization-generate-button", response.data)
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
        self.assertIn(b">Team Dashboard<", admin_response.data)
        self.assertIn(b'href="/?workspace=team-dashboard"', admin_response.data)
        self.assertIn(b'data-default-tab="setup"', admin_response.data)
        self.assertNotIn(b'data-tab-trigger="team-dashboard"', admin_response.data)
        self.assertIn(b"Team Admin", admin_response.data)
        self.assertNotIn(b">Team Default Admin<", user_response.data)
        self.assertNotIn(b">Team Dashboard<", user_response.data)

    def test_team_dashboard_primary_nav_visible_on_source_code_qa_for_admin_user(self):
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
                source_response = client.get("/source-code-qa")
                dashboard_response = client.get("/?workspace=team-dashboard")

        self.assertEqual(source_response.status_code, 200)
        self.assertIn(b'href="/?workspace=team-dashboard"', source_response.data)
        self.assertIn(b">Team Dashboard<", source_response.data)
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertIn(b'data-default-tab="team-dashboard"', dashboard_response.data)
        self.assertNotIn(b'data-tab-trigger="team-dashboard"', dashboard_response.data)

    def test_team_dashboard_config_defaults_and_save_are_admin_only(self):
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
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                forbidden_response = client.get("/api/team-dashboard/config")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x"}
                config_response = client.get("/api/team-dashboard/config")
                save_response = client.post(
                    "/admin/team-dashboard/members",
                    json={
                        "teams": {
                            "AF": {"member_emails": [" PM1@npt.sg ", "pm1@npt.sg", "pm2@npt.sg"]},
                            "CRMS": {"member_emails": ["cr@npt.sg"]},
                            "GRC": {"member_emails": "ops@npt.sg\nops2@npt.sg"},
                        }
                    },
                )

        self.assertEqual(forbidden_response.status_code, 403)
        self.assertEqual(config_response.status_code, 200)
        config_payload = config_response.get_json()
        self.assertEqual(set(config_payload["config"]["teams"].keys()), {"AF", "CRMS", "GRC"})
        self.assertIn("huixian.nah@npt.sg", config_payload["config"]["teams"]["AF"]["member_emails"])
        self.assertEqual(save_response.status_code, 200)
        saved_payload = save_response.get_json()
        self.assertEqual(saved_payload["config"]["teams"]["AF"]["member_emails"], ["pm1@npt.sg", "pm2@npt.sg"])
        self.assertEqual(saved_payload["config"]["teams"]["GRC"]["member_emails"], ["ops@npt.sg", "ops2@npt.sg"])

    def test_team_dashboard_tasks_api_groups_under_prd_and_pending_live_by_team(self):
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
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["af@npt.sg"]},
                        "CRMS": {"member_emails": ["cr@npt.sg"]},
                        "GRC": {"member_emails": ["ops@npt.sg"]},
                    }
                }
            )

            class FakeTeamDashboardClient:
                def __init__(self):
                    self.calls = []

                def list_jira_tasks_created_by_emails(self, emails):
                    self.calls.append(list(emails))
                    if emails == ["af@npt.sg"]:
                        return [
                            {
                                "jira_id": "AF-1",
                                "jira_link": "",
                                "jira_title": "PRD item",
                                "pm_email": "af@npt.sg",
                                "jira_status": "Waiting",
                                "version": "Planning_26Q2",
                                "prd_links": ["https://docs/prd"],
                                "parent_project": {
                                    "bpmis_id": "225159",
                                    "project_name": "Fraud Project",
                                    "market": "SG",
                                    "priority": "P1",
                                    "regional_pm_pic": "regional@npt.sg",
                                },
                            },
                            {
                                "jira_id": "AF-2",
                                "jira_title": "Pending item",
                                "pm_email": "af@npt.sg",
                                "jira_status": "Testing",
                                "version": "Planning_26Q3",
                                "prd_links": [],
                                "parent_project": {
                                    "bpmis_id": "225159",
                                    "project_name": "Fraud Project",
                                    "market": "SG",
                                    "priority": "P1",
                                    "regional_pm_pic": "regional@npt.sg",
                                },
                            },
                            {
                                "jira_id": "AF-3",
                                "jira_title": "Done item",
                                "pm_email": "af@npt.sg",
                                "jira_status": "Done",
                            },
                        ]
                    if emails == ["cr@npt.sg"]:
                        return [
                            {
                                "jira_id": "CR-1",
                                "jira_title": "Credit PRD",
                                "pm_email": "cr@npt.sg",
                                "jira_status": "PRD in Progress",
                                "parent_project": {
                                    "bpmis_id": "225200",
                                    "project_name": "Credit Project",
                                    "market": "ID",
                                    "priority": "P2",
                                    "regional_pm_pic": "credit@npt.sg",
                                },
                            },
                            {
                                "jira_id": "CR-2",
                                "jira_title": "Icebox item",
                                "pm_email": "cr@npt.sg",
                                "jira_status": "IceBox",
                            },
                        ]
                    return []

            fake_client = FakeTeamDashboardClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.get("/api/team-dashboard/tasks")
                    af_response = client.get("/api/team-dashboard/tasks?team=AF")
                    unknown_response = client.get("/api/team-dashboard/tasks?team=NOPE")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        teams = {team["team_key"]: team for team in payload["teams"]}
        self.assertEqual(teams["AF"]["under_prd"][0]["bpmis_id"], "225159")
        self.assertEqual(teams["AF"]["under_prd"][0]["project_name"], "Fraud Project")
        self.assertEqual(teams["AF"]["under_prd"][0]["market"], "SG")
        self.assertEqual(teams["AF"]["under_prd"][0]["priority"], "P1")
        self.assertEqual(teams["AF"]["under_prd"][0]["regional_pm_pic"], "regional@npt.sg")
        self.assertEqual([item["jira_id"] for item in teams["AF"]["under_prd"][0]["jira_tickets"]], ["AF-1"])
        self.assertEqual([item["jira_id"] for item in teams["AF"]["pending_live"][0]["jira_tickets"]], ["AF-2"])
        self.assertEqual(teams["AF"]["under_prd"][0]["jira_tickets"][0]["jira_link"], "https://jira.shopee.io/browse/AF-1")
        self.assertEqual(teams["CRMS"]["under_prd"][0]["bpmis_id"], "225200")
        self.assertEqual([item["jira_id"] for item in teams["CRMS"]["under_prd"][0]["jira_tickets"]], ["CR-1"])
        self.assertEqual(teams["CRMS"]["pending_live"], [])
        self.assertEqual(teams["GRC"]["under_prd"], [])
        self.assertEqual(af_response.status_code, 200)
        af_payload = af_response.get_json()
        self.assertEqual([team["team_key"] for team in af_payload["teams"]], ["AF"])
        self.assertEqual(af_payload["team"]["team_key"], "AF")
        self.assertEqual([item["jira_id"] for item in af_payload["team"]["pending_live"][0]["jira_tickets"]], ["AF-2"])
        self.assertEqual(unknown_response.status_code, 400)
        self.assertIn(["af@npt.sg"], fake_client.calls)

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
        self.assertEqual(payload["items"][0]["detailed_feature"], "Improve rollback handling.\nSupport retry.")
        self.assertEqual(payload["items"][0]["detailed_feature_source"], "jira_description")
        self.assertFalse(payload["llm_description_generated"])
        self.assertEqual(payload["llm_generated_count"], 0)
        self.assertEqual(payload["items"][0]["pm"], "Alice PM")
        self.assertEqual(payload["items"][0]["prd_links"], [{"label": "https://confluence/prd-1", "url": "https://confluence/prd-1"}])

    def test_productization_llm_descriptions_api_generates_description_separately(self):
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
                            "summary": "Upgrade wallet flow",
                            "desc": "<p>Improve rollback handling.</p><p>Support retry.</p>",
                        }
                    ],
                },
            )()
            codex_items = [{"jira_ticket_number": "ABC-101", "detailed_feature": "Improve wallet rollback handling and retry support."}]

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client), patch(
                "bpmis_jira_tool.web._generate_productization_detailed_features_with_codex",
                return_value=codex_items,
            ):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.get("/api/productization-upgrade-summary/llm-descriptions?version_id=88")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["llm_description_generated"])
        self.assertEqual(payload["llm_generated_count"], 1)
        self.assertTrue(payload["codex_detailed_feature"])
        self.assertEqual(payload["codex_generated_count"], 1)
        self.assertEqual(payload["items"][0]["detailed_feature"], "Improve wallet rollback handling and retry support.")
        self.assertEqual(payload["items"][0]["detailed_feature_source"], "codex")

    def test_productization_llm_description_generator_uses_local_agent_when_enabled(self):
        class FakeLocalAgentClient:
            def productization_llm_descriptions(self, *, items):
                self.items = items
                return [{"jira_ticket_number": "ABC-101", "detailed_feature": "Remote generated feature."}]

        fake_client = FakeLocalAgentClient()
        with patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True), patch(
            "bpmis_jira_tool.web._build_local_agent_client",
            return_value=fake_client,
        ), patch("bpmis_jira_tool.web._generate_productization_detailed_features_with_local_codex") as local_generate:
            items = web_module._generate_productization_detailed_features_with_codex(
                [{"jira_ticket_number": "ABC-101", "jira_description": "Raw Jira description"}],
                settings=object(),
            )

        self.assertEqual(items[0]["detailed_feature"], "Remote generated feature.")
        self.assertEqual(fake_client.items[0]["jira_ticket_number"], "ABC-101")
        local_generate.assert_not_called()

    def test_productization_issues_api_keeps_jira_description_when_codex_param_is_sent(self):
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
                            "summary": "Upgrade wallet flow",
                            "desc": "<p>Jira description stays visible.</p>",
                        }
                    ],
                },
            )()

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client), patch(
                "bpmis_jira_tool.web._generate_productization_detailed_features_with_codex",
                return_value=[{"jira_ticket_number": "ABC-101", "detailed_feature": "Generated text"}],
            ) as generate:
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["anonymous_user_key"] = "productization-user"
                        session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                        session["google_credentials"] = {"token": "x"}

                    response = client.get("/api/productization-upgrade-summary/issues?version_id=88&codex_detailed_feature=1")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertFalse(payload["llm_description_generated"])
        self.assertEqual(payload["llm_generated_count"], 0)
        self.assertFalse(payload["codex_detailed_feature"])
        self.assertEqual(payload["codex_generated_count"], 0)
        self.assertEqual(payload["items"][0]["detailed_feature"], "Jira description stays visible.")
        self.assertEqual(payload["items"][0]["detailed_feature_source"], "jira_description")
        generate.assert_not_called()

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
        self.assertEqual(normalized_issue["detailed_feature_source"], "jira_description")
        self.assertIn("First line.", normalized_issue["detailed_feature"])

    def test_format_productization_description_text_strips_html_without_rule_summary(self):
        cleaned = _format_productization_description_text("<p>[Productization effort] Add fields to ivlog, refer to ivlog sheet:</p><p>PRD: https://confluence/prd</p>")

        self.assertIn("[Productization effort] Add fields to ivlog, refer to ivlog sheet:", cleaned)
        self.assertIn("PRD: https://confluence/prd", cleaned)

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

    def test_parse_codex_json_object_accepts_fenced_json(self):
        payload = _parse_codex_json_object('```json\n{"items":[{"jira_ticket_number":"ABC-1","detailed_feature":"Done"}]}\n```')

        self.assertEqual(payload["items"][0]["jira_ticket_number"], "ABC-1")

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

                comment_response = client.patch("/api/bpmis-projects/225159/comment", json={"pm_comment": "Need PM follow-up"})
                first_projects_after_comment = store.list_projects(user_key="anon:first")
                second_projects_after_comment = store.list_projects(user_key="anon:second")
                delete_response = client.delete("/api/bpmis-projects/225159")
                list_response = client.get("/api/bpmis-projects")

            self.assertEqual(comment_response.status_code, 200)
            self.assertEqual(first_projects_after_comment[0]["pm_comment"], "Need PM follow-up")
            self.assertEqual(second_projects_after_comment[0]["pm_comment"], "")
            self.assertEqual(delete_response.status_code, 200)
            self.assertEqual(delete_response.get_json()["scope"], "portal_only")
            self.assertEqual(list_response.get_json()["projects"], [])
            self.assertEqual(len(store.list_projects(user_key="anon:second")), 1)

    def test_bpmis_projects_api_reorder_is_user_scoped(self):
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
            for bpmis_id in ("225159", "225160", "225161"):
                store.upsert_project(user_key="anon:first", bpmis_id=bpmis_id, project_name=f"First {bpmis_id}", brd_link="", market="SG")
            store.upsert_project(user_key="anon:second", bpmis_id="225159", project_name="Second", brd_link="", market="SG")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "first"

                reorder_response = client.patch("/api/bpmis-projects/order", json={"bpmis_ids": ["225161", "225159", "225160"]})
                list_response = client.get("/api/bpmis-projects")

            self.assertEqual(reorder_response.status_code, 200)
            self.assertEqual(reorder_response.get_json()["scope"], "portal_only")
            self.assertEqual(["225161", "225159", "225160"], [project["bpmis_id"] for project in list_response.get_json()["projects"]])
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
            self.assertNotIn("live_jira_title", tickets_payload["tickets"][0])
            self.assertEqual(fake_client.detail_calls, [])

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "create-user"
                tickets_response = client.get("/api/bpmis-projects/225159/jira-tickets?live=1")

            self.assertEqual(tickets_response.status_code, 200)
            tickets_payload = tickets_response.get_json()
            self.assertEqual(tickets_payload["tickets"][0]["live_jira_title"], "Live Fraud Task")
            self.assertEqual(tickets_payload["tickets"][0]["live_jira_status"], "In Progress")
            self.assertEqual(tickets_payload["tickets"][0]["live_fix_version"], "Planning_26Q2")

            ticket_id = tickets_payload["tickets"][0]["id"]
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "create-user"
                status_response = client.patch(
                    f"/api/bpmis-projects/225159/jira-tickets/{ticket_id}/status",
                    json={"status": "Testing"},
                )

            self.assertEqual(status_response.status_code, 200)
            self.assertEqual(fake_client.status_calls, [("AF-1", "Testing")])
            self.assertEqual(status_response.get_json()["ticket"]["live_jira_status"], "In Progress")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "create-user"
                version_response = client.patch(
                    f"/api/bpmis-projects/225159/jira-tickets/{ticket_id}/version",
                    json={"version_name": "Planning_26Q4", "version_id": "991"},
                )

            self.assertEqual(version_response.status_code, 200)
            self.assertEqual(fake_client.version_calls, [("AF-1", "Planning_26Q4", None)])
            self.assertEqual(
                app.config["BPMIS_PROJECT_STORE"].list_projects(user_key="anon:create-user")[0]["jira_tickets"][0]["fix_version_name"],
                "Planning_26Q4",
            )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["anonymous_user_key"] = "create-user"
                delete_ticket_response = client.delete(f"/api/bpmis-projects/225159/jira-tickets/{ticket_id}")
                tickets_after_delete_response = client.get("/api/bpmis-projects/225159/jira-tickets")

            self.assertEqual(delete_ticket_response.status_code, 200)
            self.assertTrue(delete_ticket_response.get_json()["deleted"])
            self.assertEqual(delete_ticket_response.get_json()["scope"], "bpmis_and_portal")
            self.assertEqual(fake_client.delink_calls, [("AF-1", "225159")])
            self.assertEqual(tickets_after_delete_response.get_json()["tickets"], [])

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

    def test_cloud_run_local_agent_mode_persists_setup_outside_ephemeral_data_dir(self):
        remote_client = _FakeLocalAgentConfigClient()
        env = {
            "FLASK_SECRET_KEY": "test-secret",
            "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
            "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
            "LOCAL_AGENT_MODE": "sync",
            "LOCAL_AGENT_BASE_URL": "https://agent.example.com",
            "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
            "LOCAL_AGENT_BPMIS_ENABLED": "true",
            "BPMIS_CALL_MODE": "local_agent",
            "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
        }
        with tempfile.TemporaryDirectory() as first_temp, patch.dict(
            os.environ,
            {**env, "TEAM_PORTAL_DATA_DIR": first_temp},
            clear=False,
        ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=remote_client):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                save_response = client.post(
                    "/config/save",
                    data={
                        "spreadsheet_link": "sheet-123",
                        "input_tab_name": "Sheet1",
                        "pm_team": "AF",
                    },
                    follow_redirects=False,
                )
            self.assertIsNone(app.config["CONFIG_STORE"].load("google:teammate@npt.sg"))

        with tempfile.TemporaryDirectory() as second_temp, patch.dict(
            os.environ,
            {**env, "TEAM_PORTAL_DATA_DIR": second_temp},
            clear=False,
        ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=remote_client):
            redeployed_app = create_app()
            redeployed_app.testing = True
            with redeployed_app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                page_response = client.get("/")

        self.assertEqual(save_response.status_code, 302)
        self.assertIn("google:teammate@npt.sg", remote_client.configs)
        self.assertIn("AF | SG | DBP-Anti-fraud", remote_client.configs["google:teammate@npt.sg"]["component_route_rules_text"])
        self.assertIn(b'data-default-tab="run"', page_response.data)

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

    def test_team_dashboard_prd_review_requires_admin_access(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
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
                    "/api/team-dashboard/prd-review",
                    json={"jira_id": "AF-1", "prd_url": "https://confluence/prd"},
                )

        self.assertEqual(response.status_code, 403)

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=_FakePRDReviewService())
    def test_team_dashboard_prd_review_returns_portal_result(self, _mock_service):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post(
                    "/api/team-dashboard/prd-review",
                    json={
                        "jira_id": "AF-1",
                        "jira_link": "https://jira/browse/AF-1",
                        "prd_url": "https://confluence/prd",
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["review"]["jira_id"], "AF-1")
        self.assertIn("### Review", payload["review"]["result_markdown"])

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=_FakePRDReviewService())
    def test_team_dashboard_prd_review_validates_required_prd_link(self, _mock_service):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post("/api/team-dashboard/prd-review", json={"jira_id": "AF-1"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("PRD link is required", response.get_json()["message"])

    @patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True)
    @patch("bpmis_jira_tool.web._build_local_agent_client", return_value=_FakePRDReviewLocalAgentClient())
    def test_team_dashboard_prd_review_can_route_to_local_agent(self, _mock_client, _mock_enabled):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post(
                    "/api/team-dashboard/prd-review",
                    json={"jira_id": "AF-1", "prd_url": "https://confluence/prd"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["cached"])
        self.assertEqual(payload["review"]["result_markdown"], "### Cached")


if __name__ == "__main__":
    unittest.main()
