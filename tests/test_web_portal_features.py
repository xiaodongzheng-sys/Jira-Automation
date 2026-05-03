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
from bpmis_jira_tool.errors import BPMISError, ToolError
from bpmis_jira_tool.models import CreatedTicket
from bpmis_jira_tool.monthly_report import DEFAULT_MONTHLY_REPORT_TEMPLATE, MonthlyReportSendResult
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

    def team_dashboard_config_load(self):
        from bpmis_jira_tool.web import TeamDashboardConfigStore

        return TeamDashboardConfigStore(self.store.db_path).load()

    def team_dashboard_config_save(self, config):
        from bpmis_jira_tool.web import TeamDashboardConfigStore

        return TeamDashboardConfigStore(self.store.db_path).save(config)


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

    def team_dashboard_config_load(self):
        return self.configs.get("team_dashboard") or web_module.TeamDashboardConfigStore(
            Path(tempfile.mkdtemp()) / "team_dashboard.db"
        ).default_config()

    def team_dashboard_config_save(self, config):
        self.configs["team_dashboard"] = web_module.TeamDashboardConfigStore(
            Path(tempfile.mkdtemp()) / "team_dashboard.db"
        ).normalize_config(config)
        return self.configs["team_dashboard"]


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

    def summarize(self, request):
        self.requests.append(request)
        if not request.prd_url:
            raise ToolError("PRD link is required.")
        return {
            "status": "ok",
            "cached": False,
            "summary": {
                "jira_id": request.jira_id,
                "jira_link": request.jira_link,
                "prd_url": request.prd_url,
                "status": "completed",
                "result_markdown": "### PRD Summary\n- Short",
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

    def prd_summary(self, payload):
        return {
            "status": "ok",
            "cached": True,
            "summary": {
                "jira_id": payload["jira_id"],
                "status": "completed",
                "result_markdown": "### Cached Summary",
                "updated_at": "2026-04-28T00:00:00Z",
            },
        }


class _FakeMonthlyReportService:
    def generate_draft(self, *, template, team_payloads, progress_callback=None):
        if progress_callback:
            progress_callback("summarizing_seatalk", "Summarizing SeaTalk batch 1/1.", 1, 1, estimated_prompt_tokens=1200, token_risk="normal")
        return {
            "status": "ok",
            "draft_markdown": "## Monthly Report\n- Draft",
            "generated_at": "2026-04-29T10:00:00+08:00",
            "generation_summary": {
                "total_batches": 1,
                "max_batch_estimated_tokens": 1200,
                "final_estimated_tokens": 800,
                "elapsed_seconds": 1.0,
            },
            "evidence_summary": {
                "key_project_count": 1,
                "jira_ticket_count": 1,
            },
        }


class _FakeMonthlyReportLocalAgentClient(_FakePRDReviewLocalAgentClient):
    def __init__(self):
        self.draft_payload = None
        self.send_payload = None
        self.started_payload = None

    def team_dashboard_config_load(self):
        return {}

    def team_dashboard_config_save(self, config):
        return config

    def team_dashboard_monthly_report_draft(self, payload, *, progress_callback=None):
        self.draft_payload = payload
        if progress_callback:
            progress_callback("summarizing_seatalk", "Summarizing remote SeaTalk batch 1/1.", 1, 1, estimated_prompt_tokens=1400, token_risk="normal")
        return {
            "status": "ok",
            "draft_markdown": "## Remote Monthly Report",
            "generated_at": "2026-04-29T10:00:00+08:00",
            "generation_summary": {
                "total_batches": 1,
                "max_batch_estimated_tokens": 1400,
                "final_estimated_tokens": 900,
                "elapsed_seconds": 1.0,
            },
            "evidence_summary": {"key_project_count": 1, "jira_ticket_count": 1},
        }

    def team_dashboard_monthly_report_draft_start(self, payload):
        self.started_payload = payload
        return {"status": "ok", "job_id": "remote-job-1"}

    def team_dashboard_monthly_report_job(self, job_id):
        return {
            "status": "ok",
            "job_id": job_id,
            "action": "team-dashboard-monthly-report-draft",
            "state": "completed",
            "progress": {"stage": "completed", "current": 1, "total": 1, "message": "Finished."},
            "results": [{"draft_markdown": "## Shared Remote Monthly Report"}],
        }

    def team_dashboard_monthly_report_latest_draft(self):
        return {
            "status": "ok",
            "draft_markdown": "## Shared Remote Monthly Report",
            "subject": "Monthly Report - 2026-04",
            "job_id": "remote-job-1",
            "generated_at": 1770000000,
        }

    def team_dashboard_monthly_report_send(self, payload):
        self.send_payload = payload
        return {
            "status": "sent",
            "recipient": payload.get("recipient"),
            "subject": payload.get("subject"),
            "message_id": "remote-message",
        }


class WebPortalFeatureTests(unittest.TestCase):
    def test_background_job_forms_disable_submit_button_while_running(self):
        template = Path("templates/base.html").read_text(encoding="utf-8")

        self.assertIn("form.dataset.jobRunning === 'true'", template)
        self.assertIn("form.dataset.jobRunning = 'true'", template)
        self.assertIn("submitButton.disabled = true", template)
        self.assertIn("submitButton.setAttribute('aria-busy', 'true')", template)
        self.assertIn("Syncing BPMIS Projects", template)
        self.assertIn("delete form.dataset.jobRunning", template)
        self.assertIn("submitButton.disabled = false", template)

    def test_team_dashboard_fetch_stats_exposes_bulk_and_probe_counters(self):
        class FakeBPMISClient:
            request_stats = {
                "jira_live_bulk_lookup_count": 3,
                "jira_live_bulk_issue_count": 220,
                "jira_live_detail_lookup_count": 0,
                "issue_detail_bulk_lookup_count": 2,
                "issue_detail_bulk_issue_count": 72,
                "issue_detail_single_fallback_count": 0,
                "bpmis_release_query_filter_probe_count": 1,
                "bpmis_release_query_filter_enabled_count": 1,
                "bpmis_release_query_filter_used_count": 1,
                "issue_tree_page_count": 2,
                "issue_tree_rows_scanned": 80,
                "issue_tree_fallback_count": 0,
                "release_version_lookup_count": 1,
                "release_version_count": 12,
                "release_version_lookup_failed_count": 0,
                "team_dashboard_zero_jira_fallback_candidate_count": 12,
            }

        stats = web_module._team_dashboard_fetch_stats(FakeBPMISClient())

        self.assertEqual(stats["jira_live_bulk_lookup_count"], 3)
        self.assertEqual(stats["jira_live_bulk_issue_count"], 220)
        self.assertEqual(stats["jira_live_detail_lookup_count"], 0)
        self.assertEqual(stats["issue_detail_bulk_lookup_count"], 2)
        self.assertEqual(stats["issue_detail_bulk_issue_count"], 72)
        self.assertEqual(stats["issue_detail_single_fallback_count"], 0)
        self.assertEqual(stats["bpmis_release_query_filter_probe_count"], 1)
        self.assertEqual(stats["bpmis_release_query_filter_enabled_count"], 1)
        self.assertEqual(stats["bpmis_release_query_filter_used_count"], 1)
        self.assertEqual(stats["issue_tree_page_count"], 2)
        self.assertEqual(stats["issue_tree_rows_scanned"], 80)
        self.assertEqual(stats["release_version_lookup_count"], 1)
        self.assertEqual(stats["release_version_count"], 12)
        self.assertEqual(stats["team_dashboard_zero_jira_fallback_candidate_count"], 12)

    def test_team_dashboard_combined_request_timings_dedupes_clients(self):
        class FakeBPMISClient:
            request_timings = {
                "bpmis_user_lookup": 0.2,
                "release_versions": 0.3,
                "issue_tree_reporter": 1.1,
            }

        class OtherBPMISClient:
            request_timings = {
                "issue_tree_reporter": 0.4,
                "jira_live_bulk": 0.5,
            }

        first = FakeBPMISClient()
        timings = web_module._team_dashboard_combined_request_timings(first, first, OtherBPMISClient())

        self.assertEqual(timings["bpmis_user_lookup"], 0.2)
        self.assertEqual(timings["release_versions"], 0.3)
        self.assertEqual(timings["issue_tree_reporter"], 1.5)
        self.assertEqual(timings["jira_live_bulk"], 0.5)

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
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
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
                response = client.get("/?workspace=run")

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

                response = client.get("/?workspace=run")

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
                response = client.get("/?workspace=run")

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
                    index_response = client.get("/?workspace=run")

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
                admin_response = client.get("/?workspace=setup")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                user_response = client.get("/?workspace=run")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "sophia.wangzj@npt.sg", "name": "Sophia"}
                    session["google_credentials"] = {"token": "x"}
                sophia_response = client.get("/?workspace=run")

        self.assertEqual(admin_response.status_code, 200)
        self.assertIn(b">Team Default Admin<", admin_response.data)
        self.assertIn(b"Save Anti-fraud Defaults", admin_response.data)
        self.assertIn(b">Team Dashboard<", admin_response.data)
        self.assertIn(b'href="/team-dashboard"', admin_response.data)
        self.assertIn(b'data-default-tab="setup"', admin_response.data)
        self.assertNotIn(b'data-tab-trigger="team-dashboard"', admin_response.data)
        self.assertNotIn(b"data-team-dashboard", admin_response.data)
        self.assertNotIn(b">Team Default Admin<", user_response.data)
        self.assertNotIn(b">Team Dashboard<", user_response.data)
        self.assertNotIn(b">Team Default Admin<", sophia_response.data)
        self.assertNotIn(b">Team Dashboard<", sophia_response.data)
        self.assertIn(b">PRD Briefing Tool<", user_response.data)
        self.assertIn(b">PRD Self-Assessment<", user_response.data)
        self.assertIn(b">Source Code Q&amp;A<", user_response.data)

    def test_team_dashboard_is_standalone_page_for_admin_user(self):
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
                legacy_response = client.get("/?workspace=team-dashboard")
                dashboard_response = client.get("/team-dashboard")

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "sophia.wangzj@npt.sg", "name": "Sophia"}
                    session["google_credentials"] = {"token": "x"}
                sophia_dashboard_response = client.get("/team-dashboard")

        self.assertEqual(source_response.status_code, 200)
        self.assertIn(b'href="/team-dashboard"', source_response.data)
        self.assertIn(b">Team Dashboard<", source_response.data)
        self.assertEqual(legacy_response.status_code, 302)
        self.assertEqual(legacy_response.headers["Location"], "/team-dashboard")
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertIn(b"Team Admin", dashboard_response.data)
        self.assertIn(b"Monthly Report", dashboard_response.data)
        self.assertIn(b'data-team-dashboard-tab="monthly-report"', dashboard_response.data)
        self.assertIn(b"data-team-dashboard", dashboard_response.data)
        self.assertIn(b"team-dashboard-track-tabs", dashboard_response.data)
        self.assertNotIn(b"data-team-dashboard-update", dashboard_response.data)
        self.assertNotIn(b"Manage My Projects", dashboard_response.data)
        self.assertNotIn(b'data-default-tab="team-dashboard"', dashboard_response.data)
        self.assertNotIn(b'data-tab-trigger="team-dashboard"', dashboard_response.data)
        self.assertEqual(sophia_dashboard_response.status_code, 302)
        self.assertEqual(sophia_dashboard_response.headers["Location"], "/access-denied")

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

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "sophia.wangzj@npt.sg", "name": "Sophia"}
                    session["google_credentials"] = {"token": "x"}
                sophia_config_response = client.get("/api/team-dashboard/config")
                sophia_save_response = client.post(
                    "/admin/team-dashboard/members",
                    json={"teams": {"AF": {"member_emails": ["sophia.wangzj@npt.sg"]}}},
                )

        self.assertEqual(forbidden_response.status_code, 403)
        self.assertEqual(config_response.status_code, 200)
        config_payload = config_response.get_json()
        self.assertEqual(set(config_payload["config"]["teams"].keys()), {"AF", "CRMS", "GRC"})
        self.assertEqual(
            config_payload["config"]["teams"]["AF"]["member_emails"],
            [
                "jireh.tanyx@npt.sg",
                "keryin.lim@npt.sg",
                "chongzj@npt.sg",
                "chang.wang@npt.sg",
                "zoey.luxy@npt.sg",
                "xiaodong.zheng@npt.sg",
            ],
        )
        self.assertEqual(
            config_payload["config"]["teams"]["CRMS"]["member_emails"],
            ["huixian.nah@npt.sg", "liye.ng@npt.sg", "mingming.yeo@npt.sg", "sophia.wangzj@npt.sg"],
        )
        self.assertEqual(config_payload["config"]["teams"]["GRC"]["member_emails"], ["sabrina.chan@npt.sg"])
        self.assertIn("Monthly Report", config_payload["config"]["monthly_report_template"])
        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(sophia_config_response.status_code, 403)
        self.assertEqual(sophia_save_response.status_code, 403)
        saved_payload = save_response.get_json()
        self.assertEqual(saved_payload["config"]["teams"]["AF"]["member_emails"], ["pm1@npt.sg", "pm2@npt.sg"])
        self.assertEqual(saved_payload["config"]["teams"]["GRC"]["member_emails"], ["ops@npt.sg", "ops2@npt.sg"])

    def test_team_dashboard_legacy_default_members_migrate_even_when_order_changes(self):
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
            legacy_members = list(reversed(web_module.TEAM_DASHBOARD_LEGACY_DEFAULT_MEMBER_EMAILS))
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": legacy_members},
                        "CRMS": {"member_emails": legacy_members},
                        "GRC": {"member_emails": legacy_members},
                    }
                }
            )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x"}
                response = client.get("/api/team-dashboard/config")

        payload = response.get_json()
        self.assertEqual(payload["config"]["teams"]["AF"]["member_emails"], list(web_module.TEAM_DASHBOARD_DEFAULT_MEMBER_EMAILS_BY_TEAM["AF"]))
        self.assertEqual(payload["config"]["teams"]["CRMS"]["member_emails"], list(web_module.TEAM_DASHBOARD_DEFAULT_MEMBER_EMAILS_BY_TEAM["CRMS"]))
        self.assertEqual(payload["config"]["teams"]["GRC"]["member_emails"], list(web_module.TEAM_DASHBOARD_DEFAULT_MEMBER_EMAILS_BY_TEAM["GRC"]))
        self.assertIn("Monthly Report", payload["config"]["monthly_report_template"])

    def test_team_dashboard_monthly_report_template_save_is_admin_only(self):
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
                    session["google_profile"] = {"email": "sophia.wangzj@npt.sg", "name": "Sophia"}
                    session["google_credentials"] = {"token": "x"}
                readonly_response = client.post(
                    "/admin/team-dashboard/monthly-report-template",
                    json={"template": "# Changed"},
                )
                readonly_template_response = client.get("/api/team-dashboard/monthly-report/template")
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x"}
                save_response = client.post(
                    "/admin/team-dashboard/monthly-report-template",
                    json={"template": "# Custom Monthly Report\n- Focus"},
                )
                template_response = client.get("/api/team-dashboard/monthly-report/template")

        self.assertEqual(readonly_response.status_code, 403)
        self.assertEqual(readonly_template_response.status_code, 403)
        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(save_response.get_json()["template"], "# Custom Monthly Report\n- Focus")
        self.assertEqual(template_response.status_code, 200)
        self.assertEqual(template_response.get_json()["template"], "# Custom Monthly Report\n- Focus")

    def test_team_dashboard_monthly_report_actions_are_xiaodong_only(self):
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
                    session["google_profile"] = {"email": "sophia.wangzj@npt.sg", "name": "Sophia"}
                    session["google_credentials"] = {"token": "x"}
                draft_response = client.post("/api/team-dashboard/monthly-report/draft", json={})
                send_response = client.post(
                    "/api/team-dashboard/monthly-report/send",
                    json={"draft_markdown": "draft", "subject": "Monthly Report - 2026-04"},
                )

        self.assertEqual(draft_response.status_code, 403)
        self.assertEqual(send_response.status_code, 403)
        self.assertIn("xiaodong.zheng@npt.sg", draft_response.get_json()["message"])
        self.assertIn("xiaodong.zheng@npt.sg", send_response.get_json()["message"])

    def test_team_dashboard_key_project_defaults_and_manual_overrides_survive_reload(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["af@npt.sg"]},
                        "CRMS": {"member_emails": []},
                        "GRC": {"member_emails": []},
                    }
                }
            )

            class FakeTeamDashboardClient:
                def list_biz_projects_for_pm_email(self, email):
                    if email != "af@npt.sg":
                        return []
                    return [
                        {
                            "issue_id": "SP-100",
                            "project_name": "SP Default Project",
                            "market": "SG",
                            "priority": "SP",
                            "regional_pm_pic": "af@npt.sg",
                            "status": "Confirmed",
                        },
                        {
                            "issue_id": "P0-100",
                            "project_name": "P0 Default Project",
                            "market": "SG",
                            "priority": "P0",
                            "regional_pm_pic": "af@npt.sg",
                            "status": "Confirmed",
                        },
                        {
                            "issue_id": "P1-100",
                            "project_name": "Manual Key Project",
                            "market": "SG",
                            "priority": "P1",
                            "regional_pm_pic": "af@npt.sg",
                            "status": "Confirmed",
                        },
                    ]

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    return []

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=FakeTeamDashboardClient()):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    initial_response = client.get("/api/team-dashboard/tasks?team=AF")
                    manual_off_response = client.post(
                        "/api/team-dashboard/key-projects",
                        json={"bpmis_id": "SP-100", "is_key_project": False, "priority": "SP"},
                    )
                    manual_on_response = client.post(
                        "/api/team-dashboard/key-projects",
                        json={"bpmis_id": "P1-100", "is_key_project": True, "priority": "P1"},
                    )
                    reload_response = client.get("/api/team-dashboard/tasks?team=AF")

        self.assertEqual(initial_response.status_code, 200)
        initial_projects = {
            project["bpmis_id"]: project
            for project in initial_response.get_json()["team"]["under_prd"]
        }
        self.assertTrue(initial_projects["SP-100"]["is_key_project"])
        self.assertEqual(initial_projects["SP-100"]["key_project_source"], "priority_default")
        self.assertTrue(initial_projects["P0-100"]["is_key_project"])
        self.assertEqual(initial_projects["P0-100"]["key_project_source"], "priority_default")
        self.assertFalse(initial_projects["P1-100"]["is_key_project"])
        self.assertEqual(initial_projects["P1-100"]["key_project_source"], "none")
        self.assertEqual(manual_off_response.status_code, 200)
        self.assertFalse(manual_off_response.get_json()["override"]["is_key_project"])
        self.assertEqual(manual_off_response.get_json()["key_project_source"], "manual_off")
        self.assertEqual(manual_on_response.status_code, 200)
        self.assertTrue(manual_on_response.get_json()["override"]["is_key_project"])
        self.assertEqual(manual_on_response.get_json()["key_project_source"], "manual_on")
        reloaded_projects = {
            project["bpmis_id"]: project
            for project in reload_response.get_json()["team"]["under_prd"]
        }
        self.assertFalse(reloaded_projects["SP-100"]["is_key_project"])
        self.assertEqual(reloaded_projects["SP-100"]["key_project_source"], "manual_off")
        self.assertTrue(reloaded_projects["P1-100"]["is_key_project"])
        self.assertEqual(reloaded_projects["P1-100"]["key_project_source"], "manual_on")

    def test_team_dashboard_tasks_persist_until_explicit_reload(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["af@npt.sg"]},
                        "CRMS": {"member_emails": []},
                        "GRC": {"member_emails": []},
                    }
                }
            )

            class FakeTeamDashboardClient:
                def __init__(self):
                    self.calls = 0

                def list_biz_projects_for_pm_email(self, email):
                    return [
                        {
                            "issue_id": "AF-PROJ",
                            "project_name": f"AF Project v{self.calls}",
                            "market": "SG",
                            "priority": "P1",
                            "regional_pm_pic": "af@npt.sg",
                            "status": "Confirmed",
                        }
                    ] if email == "af@npt.sg" else []

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    self.calls += 1
                    return [
                        {
                            "jira_id": f"AF-{self.calls}",
                            "jira_title": f"Cached Jira {self.calls}",
                            "pm_email": "af@npt.sg",
                            "jira_status": "Waiting",
                            "parent_project": {"bpmis_id": "AF-PROJ", "project_name": f"AF Project v{self.calls}"},
                        }
                    ]

                def list_jira_tasks_for_project_created_by_email(self, project_issue_id, email):
                    return []

            fake_client = FakeTeamDashboardClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    first = client.get("/api/team-dashboard/tasks?team=AF")
                    cached = client.get("/api/team-dashboard/tasks?team=AF")
                    reloaded = client.get("/api/team-dashboard/tasks?team=AF&reload=1")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(cached.status_code, 200)
        self.assertEqual(reloaded.status_code, 200)
        self.assertEqual(fake_client.calls, 2)
        self.assertEqual(first.get_json()["team"]["under_prd"][0]["jira_tickets"][0]["jira_id"], "AF-1")
        self.assertEqual(cached.get_json()["team"]["under_prd"][0]["jira_tickets"][0]["jira_id"], "AF-1")
        self.assertEqual(cached.get_json()["team"]["cache_source"], "server")
        self.assertIn("timing_stats", first.get_json()["team"])
        self.assertIn("timing_stats", cached.get_json()["team"])
        self.assertIn("fetch_stats", first.get_json()["team"])
        self.assertEqual(reloaded.get_json()["team"]["under_prd"][0]["jira_tickets"][0]["jira_id"], "AF-2")

    def test_team_dashboard_key_project_read_and_write_are_admin_only(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["af@npt.sg"]},
                        "CRMS": {"member_emails": []},
                        "GRC": {"member_emails": []},
                    },
                    "key_project_overrides": {
                        "P1-100": {
                            "is_key_project": True,
                            "updated_by": "xiaodong.zheng@npt.sg",
                            "updated_at": "2026-04-29T00:00:00Z",
                        }
                    },
                }
            )

            class FakeTeamDashboardClient:
                def list_biz_projects_for_pm_email(self, email):
                    if email != "af@npt.sg":
                        return []
                    return [
                        {
                            "issue_id": "P1-100",
                            "project_name": "Visible Manual Project",
                            "market": "SG",
                            "priority": "P1",
                            "regional_pm_pic": "af@npt.sg",
                            "status": "Confirmed",
                        }
                    ]

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    return []

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=FakeTeamDashboardClient()):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "sophia.wangzj@npt.sg", "name": "Sophia"}
                        session["google_credentials"] = {"token": "x"}
                    write_response = client.post(
                        "/api/team-dashboard/key-projects",
                        json={"bpmis_id": "P1-100", "is_key_project": False},
                    )
                    tasks_response = client.get("/api/team-dashboard/tasks?team=AF")

        self.assertEqual(write_response.status_code, 403)
        self.assertEqual(tasks_response.status_code, 403)

    def test_team_dashboard_config_uses_remote_local_agent_store_when_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
                "BPMIS_CALL_MODE": "local_agent",
                "LOCAL_AGENT_MODE": "sync",
                "LOCAL_AGENT_BPMIS_ENABLED": "true",
                "LOCAL_AGENT_BASE_URL": "https://agent.example.com",
                "LOCAL_AGENT_HMAC_SECRET": "secret",
            },
            clear=True,
        ):
            remote_client = _FakeLocalAgentConfigClient()
            app = create_app()
            app.testing = True

            with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=remote_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    save_response = client.post(
                        "/admin/team-dashboard/members",
                        json={"teams": {"AF": {"member_emails": ["Remote@npt.sg"]}}},
                    )
                    config_response = client.get("/api/team-dashboard/config")

        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(config_response.status_code, 200)
        self.assertEqual(config_response.get_json()["config"]["teams"]["AF"]["member_emails"], ["remote@npt.sg"])
        self.assertEqual(remote_client.configs["team_dashboard"]["teams"]["AF"]["member_emails"], ["remote@npt.sg"])

    def test_team_dashboard_under_prd_projects_sort_by_live_date_then_jira_count(self):
        projects = [
            {
                "bpmis_id": "zero-1",
                "project_name": "No Jira Project",
                "release_date": "-",
                "jira_tickets": [],
            },
            {
                "bpmis_id": "jira-1",
                "project_name": "No Date With Jira",
                "release_date": "-",
                "jira_tickets": [{"jira_id": "AF-1"}],
            },
            {
                "bpmis_id": "late-1",
                "project_name": "Later Live",
                "release_date": "02-07-2026",
                "jira_tickets": [{"jira_id": "AF-2"}],
            },
            {
                "bpmis_id": "early-1",
                "project_name": "Earlier Live",
                "release_date": "21-05-2026",
                "jira_tickets": [{"jira_id": "AF-3"}],
            },
        ]

        web_module._sort_team_dashboard_under_prd_projects(projects)

        self.assertEqual(
            [project["bpmis_id"] for project in projects],
            ["early-1", "late-1", "jira-1", "zero-1"],
        )

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
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["af@npt.sg", "xiaodong.zheng@npt.sg"]},
                        "CRMS": {"member_emails": ["cr@npt.sg"]},
                        "GRC": {"member_emails": ["ops@npt.sg"]},
                    }
                }
            )

            class FakeTeamDashboardClient:
                def __init__(self):
                    self.calls = []
                    self.project_calls = []

                def list_biz_projects_for_pm_email(self, email):
                    self.project_calls.append(email)
                    if email == "af@npt.sg":
                        return [
                            {
                                "issue_id": "225159",
                                "project_name": "Fraud Project",
                                "market": "SG",
                                "priority": "P1",
                                "regional_pm_pic": "regional@npt.sg",
                                "status": "Confirmed",
                            },
                            {
                                "issue_id": "300000",
                                "project_name": "Biz Only Project",
                                "market": "PH",
                                "priority": "P0",
                                "regional_pm_pic": "af@npt.sg",
                                "status": "Pending Review",
                            },
                            {
                                "issue_id": "300100",
                                "project_name": "Developing Biz Only Project",
                                "market": "ID",
                                "priority": "P1",
                                "regional_pm_pic": "af@npt.sg",
                                "status": "Developing",
                            },
                            {
                                "issue_id": "300200",
                                "project_name": "Draft Biz Project",
                                "market": "SG",
                                "priority": "P2",
                                "regional_pm_pic": "af@npt.sg",
                                "status": "Draft",
                            },
                        ]
                    if email == "xiaodong.zheng@npt.sg":
                        return [
                            {
                                "issue_id": "300300",
                                "project_name": "Xiaodong Biz Only Project",
                                "market": "SG",
                                "priority": "P1",
                                "regional_pm_pic": "xiaodong.zheng@npt.sg",
                                "status": "Confirmed",
                            }
                        ]
                    if email == "cr@npt.sg":
                        return [
                            {
                                "issue_id": "225200",
                                "project_name": "Credit Project",
                                "market": "ID",
                                "priority": "P2",
                                "regional_pm_pic": "credit@npt.sg",
                                "status": "Confirmed",
                            }
                        ]
                    return []

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    self.calls.append({"emails": list(emails), "kwargs": kwargs})
                    if emails == ["af@npt.sg", "xiaodong.zheng@npt.sg"]:
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
                                "release_date": "2026-05-20",
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
                                "jira_id": "AF-4",
                                "jira_title": "Earlier pending item",
                                "pm_email": "af@npt.sg",
                                "jira_status": "Testing",
                                "release_date": "2026-05-01",
                                "parent_project": {
                                    "bpmis_id": "225300",
                                    "project_name": "Earlier Live Project",
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
                            {
                                "jira_id": "CR-3",
                                "jira_title": "Unlinked item",
                                "pm_email": "cr@npt.sg",
                                "jira_status": "Testing",
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
        af_under_prd = {project["bpmis_id"]: project for project in teams["AF"]["under_prd"]}
        self.assertEqual(af_under_prd["225159"]["project_name"], "Fraud Project")
        self.assertEqual(af_under_prd["225159"]["market"], "SG")
        self.assertEqual(af_under_prd["225159"]["priority"], "P1")
        self.assertEqual(af_under_prd["225159"]["regional_pm_pic"], "regional@npt.sg")
        self.assertEqual([item["jira_id"] for item in af_under_prd["225159"]["jira_tickets"]], ["AF-1"])
        self.assertEqual(af_under_prd["300000"]["project_name"], "Biz Only Project")
        self.assertEqual(af_under_prd["300000"]["jira_tickets"], [])
        self.assertEqual(af_under_prd["300000"]["matched_pm_emails"], ["af@npt.sg"])
        self.assertEqual(af_under_prd["300300"]["project_name"], "Xiaodong Biz Only Project")
        self.assertEqual(af_under_prd["300300"]["jira_tickets"], [])
        self.assertEqual(af_under_prd["300300"]["matched_pm_emails"], ["xiaodong.zheng@npt.sg"])
        self.assertNotIn("300200", af_under_prd)
        af_pending_live = {project["bpmis_id"]: project for project in teams["AF"]["pending_live"]}
        self.assertEqual([project["bpmis_id"] for project in teams["AF"]["pending_live"][:2]], ["225300", "225159"])
        self.assertEqual([project["release_date"] for project in teams["AF"]["pending_live"][:2]], ["01-05-2026", "20-05-2026"])
        self.assertEqual([item["jira_id"] for item in af_pending_live["225159"]["jira_tickets"]], ["AF-2"])
        self.assertNotIn("300100", af_pending_live)
        self.assertNotIn("300200", af_pending_live)
        self.assertEqual(af_under_prd["225159"]["jira_tickets"][0]["jira_link"], "https://jira.shopee.io/browse/AF-1")
        self.assertEqual(teams["CRMS"]["under_prd"][0]["bpmis_id"], "225200")
        self.assertEqual([item["jira_id"] for item in teams["CRMS"]["under_prd"][0]["jira_tickets"]], ["CR-1"])
        self.assertEqual(teams["CRMS"]["pending_live"][0]["bpmis_id"], "")
        self.assertEqual(teams["CRMS"]["pending_live"][0]["project_name"], "BPMIS unavailable")
        self.assertEqual([item["jira_id"] for item in teams["CRMS"]["pending_live"][0]["jira_tickets"]], ["CR-3"])
        self.assertEqual(teams["GRC"]["under_prd"], [])
        self.assertEqual(af_response.status_code, 200)
        af_payload = af_response.get_json()
        self.assertEqual([team["team_key"] for team in af_payload["teams"]], ["AF"])
        self.assertEqual(af_payload["team"]["team_key"], "AF")
        self.assertEqual([item["jira_id"] for item in af_payload["team"]["pending_live"][0]["jira_tickets"]], ["AF-4"])
        self.assertEqual(unknown_response.status_code, 400)
        self.assertIn(
            {
                "emails": ["af@npt.sg", "xiaodong.zheng@npt.sg"],
                "kwargs": {"max_pages": 5, "enrich_missing_parent": False, "release_after": "2026-04-29"},
            },
            fake_client.calls,
        )
        self.assertIn("af@npt.sg", fake_client.project_calls)
        self.assertIn("xiaodong.zheng@npt.sg", fake_client.project_calls)
        self.assertIn("cr@npt.sg", fake_client.project_calls)

    def test_team_dashboard_tasks_can_load_one_team_at_a_time(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
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

                def list_biz_projects_for_pm_email(self, email):
                    return []

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    self.calls.append({"emails": list(emails), "kwargs": kwargs})
                    return [
                        {
                            "jira_id": "AF-1",
                            "jira_title": "PRD item",
                            "pm_email": "af@npt.sg",
                            "jira_status": "Waiting",
                            "parent_project": {"bpmis_id": "225159", "project_name": "Fraud Project"},
                        }
                    ]

            fake_client = FakeTeamDashboardClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.get("/api/team-dashboard/tasks?team_key=AF")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["team_key"], "AF")
        self.assertEqual([team["team_key"] for team in payload["teams"]], ["AF"])
        self.assertEqual(
            fake_client.calls,
            [
                {
                    "emails": ["af@npt.sg"],
                    "kwargs": {"max_pages": 5, "enrich_missing_parent": False, "release_after": "2026-04-29"},
                }
            ],
        )

    def test_team_dashboard_all_team_reload_merged_query_matches_serial_output(self):
        team_config = {
            "teams": {
                "AF": {"member_emails": ["af@npt.sg", "shared@npt.sg"]},
                "CRMS": {"member_emails": ["cr@npt.sg"]},
                "GRC": {"member_emails": ["ops@npt.sg", "shared@npt.sg"]},
            }
        }

        class FakeMergedTeamDashboardClient:
            def __init__(self):
                self.task_calls = []
                self.biz_bulk_calls = []
                self.fallback_bulk_calls = []
                self.request_stats = {"api_call_count": 0}
                self.tasks = [
                    {
                        "jira_id": "AF-1",
                        "jira_title": "AF PRD",
                        "pm_email": "af@npt.sg",
                        "jira_status": "Waiting",
                        "version": "AF_v1",
                        "release_date": "2026-06-01",
                        "prd_links": ["https://docs/af"],
                        "parent_project": {
                            "bpmis_id": "P-AF",
                            "project_name": "Fraud Project",
                            "market": "SG",
                            "priority": "P1",
                            "regional_pm_pic": "af@npt.sg",
                        },
                    },
                    {
                        "jira_id": "AF-2",
                        "jira_title": "AF Live",
                        "pm_email": "af@npt.sg",
                        "jira_status": "Testing",
                        "version": "AF_v2",
                        "release_date": "2026-06-10",
                        "parent_project": {
                            "bpmis_id": "P-AF",
                            "project_name": "Fraud Project",
                            "market": "SG",
                            "priority": "P1",
                            "regional_pm_pic": "af@npt.sg",
                        },
                    },
                    {
                        "jira_id": "CR-1",
                        "jira_title": "Credit PRD",
                        "pm_email": "cr@npt.sg",
                        "jira_status": "PRD in Progress",
                        "version": "CR_v1",
                        "parent_project": {
                            "bpmis_id": "P-CR",
                            "project_name": "Credit Project",
                            "market": "ID",
                            "priority": "P0",
                            "regional_pm_pic": "cr@npt.sg",
                        },
                    },
                    {
                        "jira_id": "OPS-1",
                        "jira_title": "Ops Live",
                        "pm_email": "ops@npt.sg",
                        "jira_status": "UAT",
                        "release_date": "2026-07-01",
                        "version": "OPS_v1",
                        "parent_project": {
                            "bpmis_id": "P-OPS",
                            "project_name": "Ops Project",
                            "market": "PH",
                            "priority": "P2",
                            "regional_pm_pic": "ops@npt.sg",
                        },
                    },
                    {
                        "jira_id": "SHARED-1",
                        "jira_title": "Shared PRD",
                        "pm_email": "shared@npt.sg",
                        "jira_status": "Waiting",
                        "version": "SHARED_v1",
                        "parent_project": {
                            "bpmis_id": "P-SHARED",
                            "project_name": "Shared Project",
                            "market": "SG",
                            "priority": "P1",
                            "regional_pm_pic": "shared@npt.sg",
                        },
                    },
                ]
                self.projects = [
                    {
                        "issue_id": "P-AF",
                        "project_name": "Fraud Project",
                        "market": "SG",
                        "priority": "P1",
                        "regional_pm_pic": "af@npt.sg",
                        "status": "Confirmed",
                        "matched_pm_emails": ["af@npt.sg"],
                    },
                    {
                        "issue_id": "P-AF-ZERO",
                        "project_name": "AF Zero Project",
                        "market": "SG",
                        "priority": "P0",
                        "regional_pm_pic": "af@npt.sg",
                        "status": "Confirmed",
                        "matched_pm_emails": ["af@npt.sg"],
                    },
                    {
                        "issue_id": "P-CR",
                        "project_name": "Credit Project",
                        "market": "ID",
                        "priority": "P0",
                        "regional_pm_pic": "cr@npt.sg",
                        "status": "Confirmed",
                        "matched_pm_emails": ["cr@npt.sg"],
                    },
                    {
                        "issue_id": "P-CR-ZERO",
                        "project_name": "Credit Zero Live",
                        "market": "ID",
                        "priority": "P1",
                        "regional_pm_pic": "cr@npt.sg",
                        "status": "Developing",
                        "matched_pm_emails": ["cr@npt.sg"],
                    },
                    {
                        "issue_id": "P-OPS",
                        "project_name": "Ops Project",
                        "market": "PH",
                        "priority": "P2",
                        "regional_pm_pic": "ops@npt.sg",
                        "status": "UAT",
                        "matched_pm_emails": ["ops@npt.sg"],
                    },
                    {
                        "issue_id": "P-SHARED",
                        "project_name": "Shared Project",
                        "market": "SG",
                        "priority": "P1",
                        "regional_pm_pic": "shared@npt.sg",
                        "status": "Confirmed",
                        "matched_pm_emails": ["shared@npt.sg"],
                    },
                ]
                self.fallback_rows = {
                    "P-AF-ZERO": [
                        {
                            "jira_id": "AF-Z1",
                            "jira_title": "AF zero fallback",
                            "pm_email": "af@npt.sg",
                            "jira_status": "Waiting",
                            "version": "AF_zero",
                            "release_date": "2026-06-20",
                        }
                    ],
                    "P-CR-ZERO": [
                        {
                            "jira_id": "CR-Z1",
                            "jira_title": "CR zero fallback",
                            "pm_email": "cr@npt.sg",
                            "jira_status": "Testing",
                            "version": "CR_zero",
                            "release_date": "2026-07-20",
                        }
                    ],
                }

            def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                normalized = web_module._normalize_team_dashboard_emails(emails)
                self.task_calls.append({"emails": normalized, "kwargs": kwargs})
                allowed = set(normalized)
                return [task for task in self.tasks if task["pm_email"] in allowed]

            def list_biz_projects_for_pm_emails(self, emails):
                normalized = web_module._normalize_team_dashboard_emails(emails)
                self.biz_bulk_calls.append(normalized)
                allowed = set(normalized)
                rows = []
                for project in self.projects:
                    matches = [email for email in project["matched_pm_emails"] if email in allowed]
                    if not matches:
                        continue
                    rows.append({**project, "matched_pm_emails": matches})
                return rows

            def list_biz_projects_for_pm_email(self, email):
                return self.list_biz_projects_for_pm_emails([email])

            def list_jira_tasks_for_projects_created_by_emails(self, project_issue_ids, emails):
                normalized_ids = [str(project_id) for project_id in project_issue_ids]
                normalized_emails = web_module._normalize_team_dashboard_emails(emails)
                self.fallback_bulk_calls.append((normalized_ids, normalized_emails))
                allowed = set(normalized_emails)
                return {
                    project_id: [
                        row
                        for row in self.fallback_rows.get(project_id, [])
                        if str(row.get("pm_email") or "").lower() in allowed
                    ]
                    for project_id in normalized_ids
                }

        def make_client(fake_client):
            temp_dir = tempfile.TemporaryDirectory()
            patcher = patch.dict(
                os.environ,
                {
                    "FLASK_SECRET_KEY": "test-secret",
                    "TEAM_PORTAL_DATA_DIR": temp_dir.name,
                    "TEAM_PORTAL_BASE_URL": "",
                    "TEAM_ALLOWED_EMAILS": "",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                    "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
                    "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
                },
                clear=True,
            )
            patcher.start()
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(team_config)
            client = app.test_client()
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                session["google_credentials"] = {"token": "x"}
            return temp_dir, patcher, client

        def business_payload(team):
            return {
                "team_key": team["team_key"],
                "label": team["label"],
                "member_emails": team["member_emails"],
                "under_prd": team["under_prd"],
                "pending_live": team["pending_live"],
            }

        serial_fake = FakeMergedTeamDashboardClient()
        serial_temp_dir, serial_env_patch, serial_client = make_client(serial_fake)
        try:
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=serial_fake):
                serial_teams = {}
                for team_key in ("AF", "CRMS", "GRC"):
                    response = serial_client.get(f"/api/team-dashboard/tasks?team_key={team_key}&reload=1")
                    self.assertEqual(response.status_code, 200)
                    serial_teams[team_key] = business_payload(response.get_json()["team"])
        finally:
            serial_env_patch.stop()
            serial_temp_dir.cleanup()

        merged_fake = FakeMergedTeamDashboardClient()
        merged_temp_dir, merged_env_patch, merged_client = make_client(merged_fake)
        try:
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=merged_fake):
                merged_response = merged_client.get("/api/team-dashboard/tasks?reload=1")
                self.assertEqual(merged_response.status_code, 200)
                merged_payload = merged_response.get_json()
                merged_teams = {team["team_key"]: business_payload(team) for team in merged_payload["teams"]}
        finally:
            merged_env_patch.stop()
            merged_temp_dir.cleanup()

        self.assertEqual(merged_teams, serial_teams)
        self.assertEqual(len(serial_fake.task_calls), 3)
        self.assertEqual(len(serial_fake.biz_bulk_calls), 3)
        self.assertEqual(len(serial_fake.fallback_bulk_calls), 2)
        self.assertEqual(len(merged_fake.task_calls), 1)
        self.assertEqual(
            merged_fake.task_calls[0]["emails"],
            ["af@npt.sg", "shared@npt.sg", "cr@npt.sg", "ops@npt.sg"],
        )
        self.assertEqual(len(merged_fake.biz_bulk_calls), 1)
        self.assertEqual(len(merged_fake.fallback_bulk_calls), 1)
        self.assertEqual(merged_fake.fallback_bulk_calls[0][0], ["P-AF-ZERO", "P-CR-ZERO"])

    def test_team_dashboard_link_biz_project_jira_step_does_not_load_bpmis_projects(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
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

            class FakeLinkBizClient:
                def __init__(self):
                    self.project_calls = []

                def list_biz_projects_for_pm_email(self, email):
                    self.project_calls.append(email)
                    raise AssertionError("Jira-only step must not load BPMIS Biz Projects.")

                def list_jira_tasks_created_by_emails(self, emails, **_kwargs):
                    if emails == ["af@npt.sg"]:
                        return [
                            {
                                "jira_id": "AF-1",
                                "jira_title": "[Feature][SG][DBP-Anti-fraud] Fraud Alert Revamp",
                                "pm_email": "af@npt.sg",
                                "jira_status": "Testing",
                            },
                            {
                                "jira_id": "AF-2",
                                "jira_title": "Sync AF productization weekly check",
                                "pm_email": "af@npt.sg",
                                "jira_status": "Testing",
                            },
                        ]
                    if emails == ["cr@npt.sg"]:
                        return [
                            {
                                "jira_id": "CR-1",
                                "jira_title": "[Feature][ID] Credit Scoring Improvement",
                                "pm_email": "cr@npt.sg",
                                "jira_status": "Waiting",
                            }
                        ]
                    return []

            fake_client = FakeLinkBizClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.get("/api/team-dashboard/link-biz-projects/jira")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual([row["jira_id"] for row in payload["rows"]], ["AF-1", "CR-1"])
        self.assertEqual(payload["rows"][0]["suggested_bpmis_id"], "")
        self.assertEqual(fake_client.project_calls, [])

    def test_team_dashboard_link_biz_project_suggestions_are_scoped_to_jira_pm(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
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

            class FakeLinkBizClient:
                def __init__(self):
                    self.keyword_calls = []
                    self.project_calls = []

                def list_biz_projects_for_pm_email(self, email):
                    self.project_calls.append(email)
                    if email == "af@npt.sg":
                        return [
                            {
                                "issue_id": "225159",
                                "project_name": "Fraud Alert Revamp",
                                "market": "SG",
                                "status": "Confirmed",
                                "matched_pm_emails": ["af@npt.sg"],
                            },
                            {
                                "issue_id": "225300",
                                "project_name": "AF Draft Project",
                                "market": "SG",
                                "status": "Draft",
                                "matched_pm_emails": ["af@npt.sg"],
                            },
                        ]
                    if email == "cr@npt.sg":
                        return [
                            {
                                "issue_id": "225200",
                                "project_name": "Fraud Alert Revamp Exact Better Text",
                                "market": "ID",
                                "status": "Confirmed",
                                "matched_pm_emails": ["cr@npt.sg"],
                            }
                        ]
                    return []

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    return []

                def list_jira_tasks_for_project_created_by_email(self, project_issue_id, email):
                    return []

                def search_biz_projects_by_title_keywords(self, keywords, *, max_pages=None):
                    self.keyword_calls.append((keywords, max_pages))
                    return []

            fake_client = FakeLinkBizClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.post(
                        "/api/team-dashboard/link-biz-projects/suggestions",
                        json={
                            "rows": [
                                {
                                    "team_key": "AF",
                                    "jira_id": "AF-1",
                                    "jira_title": "[Feature][SG][DBP-Anti-fraud] Fraud Alert Revamp",
                                    "reporter_email": "af@npt.sg",
                                },
                                {
                                    "team_key": "AF",
                                    "jira_id": "AF-2",
                                    "jira_title": "[Feature] Fraud Alert Revamp Exact Better Text",
                                    "reporter_email": "cr@npt.sg",
                                },
                            ]
                        },
                    )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        suggestions = {row["jira_id"]: row for row in payload["rows"]}
        self.assertEqual(suggestions["AF-1"]["suggested_bpmis_id"], "225159")
        self.assertEqual(suggestions["AF-1"]["suggested_project_title"], "Fraud Alert Revamp")
        self.assertEqual(suggestions["AF-1"]["match_source"], "pm")
        self.assertEqual(suggestions["AF-2"]["suggested_bpmis_id"], "225200")
        self.assertEqual(suggestions["AF-2"]["match_source"], "pm")
        self.assertEqual(payload["keyword_search_count"], 0)
        self.assertEqual(fake_client.keyword_calls, [])
        self.assertGreater(suggestions["AF-1"]["match_score"], 0.8)
        af_option_titles = [item["project_name"] for item in suggestions["AF-1"]["select_biz_project_options"]]
        cr_option_titles = [item["project_name"] for item in suggestions["AF-2"]["select_biz_project_options"]]
        self.assertEqual(af_option_titles, ["Fraud Alert Revamp"])
        self.assertEqual(cr_option_titles, ["Fraud Alert Revamp Exact Better Text"])
        self.assertNotIn("AF Draft Project", af_option_titles)
        self.assertEqual(fake_client.project_calls, ["af@npt.sg", "cr@npt.sg"])

    def test_team_dashboard_link_biz_project_suggestions_can_use_loaded_task_payload_candidates(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            class FakeLinkBizClient:
                def list_biz_projects_for_pm_email(self, email):
                    raise AssertionError("Loaded Task List payloads should provide team candidates.")

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    raise AssertionError("Loaded Task List payloads should avoid Jira reload.")

                def search_biz_projects_by_title_keywords(self, keywords, *, max_pages=None):
                    return []

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=FakeLinkBizClient()):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.post(
                        "/api/team-dashboard/link-biz-projects/suggestions",
                        json={
                            "rows": [
                                {
                                    "team_key": "AF",
                                    "jira_id": "AF-1",
                                    "jira_title": "[Feature] Fraud Alert Revamp",
                                    "reporter_email": "af@npt.sg",
                                }
                            ],
                            "team_payloads": [
                                {
                                    "team_key": "AF",
                                    "under_prd": [
                                        {
                                            "bpmis_id": "225159",
                                            "project_name": "Fraud Alert Revamp",
                                            "status": "Confirmed",
                                            "matched_pm_emails": ["af@npt.sg"],
                                            "jira_tickets": [],
                                        },
                                        {
                                            "bpmis_id": "225160",
                                            "project_name": "Busy Fraud Project",
                                            "status": "Confirmed",
                                            "matched_pm_emails": ["af@npt.sg"],
                                            "jira_tickets": [{"jira_id": "AF-9"}],
                                        },
                                    ],
                                    "pending_live": [],
                                }
                            ],
                        },
                    )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["rows"][0]["suggested_bpmis_id"], "225159")
        self.assertEqual(
            [item["project_name"] for item in payload["rows"][0]["select_biz_project_options"]],
            ["Busy Fraud Project", "Fraud Alert Revamp"],
        )
        self.assertEqual(
            {item["project_name"]: item["team_key"] for item in payload["select_biz_project_options"]},
            {"Busy Fraud Project": "AF", "Fraud Alert Revamp": "AF"},
        )

    def test_team_dashboard_link_biz_project_links_real_bpmis_and_updates_portal_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            class FakeLinkClient:
                def __init__(self):
                    self.link_calls = []

                def list_biz_projects_for_pm_email(self, email):
                    if email == "af@npt.sg":
                        return [
                            {
                                "issue_id": "225200",
                                "project_name": "Selected Credit Project",
                                "market": "SG",
                                "status": "Confirmed",
                                "matched_pm_emails": ["af@npt.sg"],
                            }
                        ]
                    return []

                def link_jira_ticket_to_project(self, ticket_key, project_issue_id):
                    self.link_calls.append((ticket_key, project_issue_id))
                    return {
                        "jiraKey": ticket_key,
                        "summary": "Fraud Alert Revamp",
                        "parentIds": [int(project_issue_id)],
                        "status": {"label": "Testing"},
                    }

                def get_issue_detail(self, issue_id):
                    return {"issue_id": issue_id, "project_name": "Fraud Alert Revamp", "market": "SG"}

            fake_client = FakeLinkClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.post(
                        "/api/team-dashboard/link-biz-projects",
                        json={
                            "jira_id": "AF-1",
                            "jira_link": "https://jira.shopee.io/browse/AF-1",
                            "jira_title": "Fraud Alert Revamp",
                            "reporter_email": "af@npt.sg",
                            "suggested_bpmis_id": "225159",
                            "suggested_project_title": "Fraud Alert Revamp",
                            "selected_bpmis_id": "225200",
                            "selected_project_title": "Selected Credit Project",
                        },
                    )

            projects = app.config["BPMIS_PROJECT_STORE"].list_projects(user_key="google:xiaodong.zheng@npt.sg")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_client.link_calls, [("AF-1", "225200")])
        self.assertEqual(projects[0]["bpmis_id"], "225200")
        self.assertEqual(projects[0]["jira_tickets"][0]["ticket_key"], "AF-1")
        self.assertEqual(projects[0]["jira_tickets"][0]["status"], "linked")

    def test_team_dashboard_link_biz_project_failure_does_not_update_portal_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            class BrokenLinkClient:
                def list_biz_projects_for_pm_email(self, email):
                    if email == "af@npt.sg":
                        return [
                            {
                                "issue_id": "225159",
                                "project_name": "Fraud Alert Revamp",
                                "market": "SG",
                                "status": "Confirmed",
                                "matched_pm_emails": ["af@npt.sg"],
                            }
                        ]
                    return []

                def link_jira_ticket_to_project(self, _ticket_key, _project_issue_id):
                    raise BPMISError("BPMIS link endpoint rejected the ticket.")

            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=BrokenLinkClient()):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.post(
                        "/api/team-dashboard/link-biz-projects",
                        json={"jira_id": "AF-1", "reporter_email": "af@npt.sg", "suggested_bpmis_id": "225159"},
                    )

            projects = app.config["BPMIS_PROJECT_STORE"].list_projects(user_key="google:xiaodong.zheng@npt.sg")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(projects, [])

    def test_team_dashboard_link_biz_project_rejects_cross_pm_project(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

            class FakeLinkClient:
                def __init__(self):
                    self.link_calls = []

                def list_biz_projects_for_pm_email(self, email):
                    if email == "af@npt.sg":
                        return [
                            {
                                "issue_id": "225159",
                                "project_name": "AF Owned Project",
                                "market": "SG",
                                "status": "Confirmed",
                                "matched_pm_emails": ["af@npt.sg"],
                            }
                        ]
                    return []

                def link_jira_ticket_to_project(self, ticket_key, project_issue_id):
                    self.link_calls.append((ticket_key, project_issue_id))
                    return {}

            fake_client = FakeLinkClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.post(
                        "/api/team-dashboard/link-biz-projects",
                        json={
                            "jira_id": "AF-1",
                            "reporter_email": "af@npt.sg",
                            "selected_bpmis_id": "225200",
                            "selected_project_title": "CR Owned Project",
                        },
                    )

            projects = app.config["BPMIS_PROJECT_STORE"].list_projects(user_key="google:xiaodong.zheng@npt.sg")

        self.assertEqual(response.status_code, 400)
        self.assertIn("must belong to the Jira PM", response.get_json()["message"])
        self.assertEqual(fake_client.link_calls, [])
        self.assertEqual(projects, [])

    def test_team_dashboard_link_biz_project_frontend_uses_row_scoped_options(self):
        script = Path("static/team_dashboard.js").read_text(encoding="utf-8")
        self.assertIn("row.select_biz_project_options", script)
        self.assertIn("data-reporter-email", script)
        self.assertIn("reporter_email: button.dataset.reporterEmail", script)
        self.assertIn("Finding...", script)
        self.assertIn("Refreshed at", script)

    def test_team_dashboard_backfills_empty_project_jira_tasks_by_parent_id(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["zoey.luxy@npt.sg"]},
                        "CRMS": {"member_emails": []},
                        "GRC": {"member_emails": []},
                    }
                }
            )

            class FakeTeamDashboardClient:
                def __init__(self):
                    self.project_task_calls = []

                def list_biz_projects_for_pm_email(self, email):
                    if email != "zoey.luxy@npt.sg":
                        return []
                    return [
                        {
                            "issue_id": "214164",
                            "project_name": "AF System - Project CENTUM",
                            "market": "SG",
                            "priority": "P0",
                            "regional_pm_pic": "zoey.luxy@npt.sg",
                            "status": "Confirmed",
                        }
                    ]

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    return []

                def list_jira_tasks_for_project_created_by_email(self, project_issue_id, email):
                    self.project_task_calls.append((project_issue_id, email))
                    return [
                        {
                            "ticket_key": "SPDBP-92169",
                            "ticket_link": "https://jira.shopee.io/browse/SPDBP-92169",
                            "jira_title": "[Feature] AF Function Enhancement",
                            "status": "Waiting",
                            "fix_version_name": "AF_v1.0.77_20260410",
                        }
                    ]

            fake_client = FakeTeamDashboardClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.get("/api/team-dashboard/tasks?team_key=AF")

        self.assertEqual(response.status_code, 200)
        project = response.get_json()["team"]["under_prd"][0]
        self.assertEqual(project["bpmis_id"], "214164")
        self.assertEqual([ticket["jira_id"] for ticket in project["jira_tickets"]], ["SPDBP-92169"])
        self.assertEqual(project["jira_tickets"][0]["jira_status"], "Waiting")
        self.assertEqual(fake_client.project_task_calls, [("214164", "zoey.luxy@npt.sg")])

    def test_team_dashboard_backfills_empty_project_jira_tasks_with_bulk_parent_lookup(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["zoey.luxy@npt.sg"]},
                        "CRMS": {"member_emails": []},
                        "GRC": {"member_emails": []},
                    }
                }
            )

            class FakeTeamDashboardClient:
                def __init__(self):
                    self.bulk_project_task_calls = []
                    self.single_project_task_calls = []
                    self.request_stats = {"api_call_count": 3}

                def list_biz_projects_for_pm_emails(self, emails):
                    return [
                        {
                            "issue_id": "214164",
                            "project_name": "AF System - Project CENTUM",
                            "market": "SG",
                            "priority": "P0",
                            "regional_pm_pic": "zoey.luxy@npt.sg",
                            "matched_pm_emails": emails,
                            "status": "Confirmed",
                        }
                    ]

                def list_biz_projects_for_pm_email(self, email):
                    raise AssertionError("single Biz Project lookup should not be used")

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    return []

                def list_jira_tasks_for_projects_created_by_emails(self, project_issue_ids, emails):
                    self.bulk_project_task_calls.append((project_issue_ids, emails))
                    return {
                        "214164": [
                            {
                                "jira_id": "SPDBP-92169",
                                "jira_title": "[Feature] AF Function Enhancement",
                                "pm_email": "zoey.luxy@npt.sg",
                                "jira_status": "Waiting",
                                "version": "AF_v1.0.77_20260410",
                            }
                        ]
                    }

                def list_jira_tasks_for_project_created_by_email(self, project_issue_id, email):
                    self.single_project_task_calls.append((project_issue_id, email))
                    return []

            fake_client = FakeTeamDashboardClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.get("/api/team-dashboard/tasks?team_key=AF")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        project = payload["team"]["under_prd"][0]
        self.assertEqual([ticket["jira_id"] for ticket in project["jira_tickets"]], ["SPDBP-92169"])
        self.assertEqual(fake_client.bulk_project_task_calls, [(["214164"], ["zoey.luxy@npt.sg"])])
        self.assertEqual(fake_client.single_project_task_calls, [])
        self.assertEqual(payload["team"]["fetch_stats"]["api_call_count"], 3)
        self.assertIn("backfill_zero_jira_projects", payload["team"]["timing_stats"])

    def test_team_dashboard_backfill_skips_single_project_retry_after_bulk_empty_result(self):
        class FakeTeamDashboardClient:
            def __init__(self):
                self.request_stats = {}
                self.bulk_project_task_calls = []
                self.single_project_task_calls = []

            def list_jira_tasks_for_projects_created_by_emails(self, project_issue_ids, emails):
                self.bulk_project_task_calls.append((project_issue_ids, emails))
                return {"214164": []}

            def list_jira_tasks_for_project_created_by_email(self, project_issue_id, email):
                self.single_project_task_calls.append((project_issue_id, email))
                return []

        fake_client = FakeTeamDashboardClient()
        team_payload = {
            "under_prd": [
                {
                    "bpmis_id": "214164",
                    "jira_tickets": [],
                    "matched_pm_emails": ["zoey.luxy@npt.sg"],
                }
            ],
            "pending_live": [],
        }

        web_module._backfill_team_dashboard_empty_project_jira_tasks(fake_client, team_payload)

        self.assertEqual(fake_client.bulk_project_task_calls, [(["214164"], ["zoey.luxy@npt.sg"])])
        self.assertEqual(fake_client.single_project_task_calls, [])
        self.assertEqual(fake_client.request_stats["team_dashboard_zero_jira_fallback_candidate_count"], 1)
        self.assertEqual(fake_client.request_stats["team_dashboard_zero_jira_bulk_project_count"], 1)
        self.assertEqual(fake_client.request_stats["team_dashboard_zero_jira_per_project_fallback_skipped_count"], 1)

    def test_team_dashboard_pending_live_fallback_excludes_done_jira_tasks(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-04-29",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                {
                    "teams": {
                        "AF": {"member_emails": ["zoey.luxy@npt.sg"]},
                        "CRMS": {"member_emails": []},
                        "GRC": {"member_emails": []},
                    }
                }
            )

            class FakeTeamDashboardClient:
                def __init__(self):
                    self.project_task_calls = []

                def list_biz_projects_for_pm_email(self, email):
                    if email != "zoey.luxy@npt.sg":
                        return []
                    return [
                        {
                            "issue_id": "214164",
                            "project_name": "AF System - Project CENTUM",
                            "market": "SG",
                            "priority": "P0",
                            "regional_pm_pic": "zoey.luxy@npt.sg",
                            "status": "Developing",
                        }
                    ]

                def list_jira_tasks_created_by_emails(self, emails, **kwargs):
                    return []

                def list_jira_tasks_for_project_created_by_email(self, project_issue_id, email):
                    self.project_task_calls.append((project_issue_id, email))
                    return [
                        {
                            "ticket_key": "SPDBP-92169",
                            "ticket_link": "https://jira.shopee.io/browse/SPDBP-92169",
                            "jira_title": "[Feature] AF Function Enhancement",
                            "status": "Done",
                            "fix_version_name": "AF_v1.0.77_20260410",
                        }
                    ]

            fake_client = FakeTeamDashboardClient()
            with patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=fake_client):
                with app.test_client() as client:
                    with client.session_transaction() as session:
                        session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                        session["google_credentials"] = {"token": "x"}
                    response = client.get("/api/team-dashboard/tasks?team_key=AF")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["team"]["pending_live"], [])
        self.assertEqual(fake_client.project_task_calls, [("214164", "zoey.luxy@npt.sg")])

    def test_team_dashboard_rejects_unknown_single_team(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAILS": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x"}
                response = client.get("/api/team-dashboard/tasks?team_key=UNKNOWN")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["status"], "error")

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
                page_response = client.get("/?workspace=run")

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

                response = client.get("/?workspace=run")

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

                response = client.get("/?workspace=run")

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

    @patch("bpmis_jira_tool.web._load_all_team_dashboard_task_payloads", return_value=[{"team_key": "AF"}])
    @patch("bpmis_jira_tool.web._build_monthly_report_service", return_value=_FakeMonthlyReportService())
    def test_team_dashboard_monthly_report_draft_returns_portal_result(self, _mock_service, _mock_payloads):
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
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save({"monthly_report_template": "# Template"})
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post("/api/team-dashboard/monthly-report/draft", json={})
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["status"], "queued")
                self.assertTrue(payload["job_id"])
                job_payload = {}
                for _ in range(20):
                    job_payload = client.get(f"/api/jobs/{payload['job_id']}").get_json()
                    if job_payload.get("state") == "completed":
                        break
                    time.sleep(0.05)
                latest_payload = client.get("/api/team-dashboard/monthly-report/latest-draft").get_json()
        self.assertEqual(job_payload["state"], "completed")
        self.assertEqual(job_payload["progress"]["stage"], "completed")
        result = job_payload["results"][0]
        self.assertIn("Monthly Report", result["draft_markdown"])
        self.assertEqual(result["evidence_summary"]["key_project_count"], 1)
        self.assertEqual(result["generation_summary"]["total_batches"], 1)
        self.assertEqual(latest_payload["status"], "ok")
        self.assertEqual(latest_payload["draft_markdown"], result["draft_markdown"])
        self.assertEqual(latest_payload["job_id"], payload["job_id"])

    @patch("bpmis_jira_tool.web.send_monthly_report_email")
    def test_team_dashboard_monthly_report_send_sends_edited_draft(self, mock_send):
        mock_send.return_value = MonthlyReportSendResult(
            status="sent",
            recipient="xiaodong.zheng@npt.sg",
            subject="Monthly Report - 2026-04",
            message_id="msg-1",
        )
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
                    "/api/team-dashboard/monthly-report/send",
                    json={
                        "draft_markdown": "edited draft",
                        "subject": "Monthly Report - 2026-04",
                        "recipient": "xiaodong.zheng@npt.sg",
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message_id"], "msg-1")
        self.assertEqual(mock_send.call_args.kwargs["draft_markdown"], "edited draft")

    @patch("bpmis_jira_tool.web._local_agent_seatalk_enabled", return_value=True)
    @patch("bpmis_jira_tool.web._load_all_team_dashboard_task_payloads", return_value=[{"team_key": "AF"}])
    def test_team_dashboard_monthly_report_draft_can_route_to_local_agent(self, _mock_payloads, _mock_enabled):
        fake_client = _FakeMonthlyReportLocalAgentClient()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
            app = create_app()
            app.testing = True
            app.config["TEAM_DASHBOARD_CONFIG_STORE"].save({"monthly_report_template": "# Template"})
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post("/api/team-dashboard/monthly-report/draft", json={})
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["status"], "queued")
                job_payload = {}
                for _ in range(20):
                    job_payload = client.get(f"/api/jobs/{payload['job_id']}").get_json()
                    if job_payload.get("state") == "completed":
                        break
                    time.sleep(0.05)
        self.assertEqual(job_payload["state"], "completed")
        self.assertEqual(job_payload["results"][0]["draft_markdown"], "## Remote Monthly Report")
        self.assertEqual(fake_client.draft_payload["template"], "# Template")
        self.assertEqual(fake_client.draft_payload["team_payloads"], [{"team_key": "AF"}])

    @patch("bpmis_jira_tool.web._load_all_team_dashboard_task_payloads", return_value=[{"team_key": "AF"}])
    def test_team_dashboard_monthly_report_uses_shared_local_agent_jobs_when_remote_config_enabled(self, _mock_payloads):
        fake_client = _FakeMonthlyReportLocalAgentClient()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "https://uat.example.test",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "LOCAL_AGENT_MODE": "sync",
                "LOCAL_AGENT_BASE_URL": "https://agent.example.test",
                "LOCAL_AGENT_HMAC_SECRET": "secret",
                "LOCAL_AGENT_BPMIS_ENABLED": "true",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post("/api/team-dashboard/monthly-report/draft", json={})
                job_payload = client.get("/api/jobs/remote-job-1").get_json()
                latest_payload = client.get("/api/team-dashboard/monthly-report/latest-draft").get_json()

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["job_id"], "remote-job-1")
        self.assertEqual(payload["job_backend"], "local_agent")
        self.assertEqual(job_payload["state"], "completed")
        self.assertEqual(job_payload["results"][0]["draft_markdown"], "## Shared Remote Monthly Report")
        self.assertEqual(latest_payload["draft_markdown"], "## Shared Remote Monthly Report")
        self.assertEqual(fake_client.started_payload["template"], DEFAULT_MONTHLY_REPORT_TEMPLATE)
        self.assertEqual(fake_client.started_payload["team_payloads"], [{"team_key": "AF"}])

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=_FakePRDReviewService())
    def test_team_dashboard_prd_summary_returns_portal_result(self, _mock_service):
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
                    "/api/team-dashboard/prd-summary",
                    json={
                        "jira_id": "AF-1",
                        "jira_link": "https://jira/browse/AF-1",
                        "prd_url": "https://confluence/prd",
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["summary"]["jira_id"], "AF-1")
        self.assertIn("### PRD Summary", payload["summary"]["result_markdown"])

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

    @patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True)
    @patch("bpmis_jira_tool.web._build_local_agent_client", return_value=_FakePRDReviewLocalAgentClient())
    def test_team_dashboard_prd_summary_can_route_to_local_agent(self, _mock_client, _mock_enabled):
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
                    "/api/team-dashboard/prd-summary",
                    json={"jira_id": "AF-1", "prd_url": "https://confluence/prd"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["cached"])
        self.assertEqual(payload["summary"]["result_markdown"], "### Cached Summary")


if __name__ == "__main__":
    unittest.main()
