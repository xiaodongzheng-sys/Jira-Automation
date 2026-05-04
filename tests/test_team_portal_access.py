import os
import time
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bs4 import BeautifulSoup

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.web import _current_release_revision, _run_background_job, create_app


class _FakeSyncBPMISClient:
    def list_biz_projects_for_pm_email(self, _email):
        return []

    def get_brd_doc_links_for_projects(self, _issue_ids):
        return {}


class _FakeProxyResponse:
    status_code = 200
    content = b'{"status":"ok","result":[]}'
    headers = {"Content-Type": "application/json", "Content-Length": "27"}


class _FakeNonAdminBPMISClient:
    def search_versions(self, _query):
        return [{"id": "88", "name": "Planning_26Q2", "market": "SG"}]

    def list_issues_for_version(self, _version_id):
        return []

    def list_biz_projects_for_pm_email(self, _email):
        return []

    def get_brd_doc_links_for_projects(self, _issue_ids):
        return {}


class _UnavailableLocalAgentClient:
    def bpmis_config_load(self, *, user_key):
        raise ToolError("Mac local-agent is unavailable: connection refused")

    def bpmis_team_profiles_load(self):
        raise ToolError("Mac local-agent is unavailable: connection refused")

    def source_code_qa_config(self, *, llm_provider=None):
        raise ToolError("Mac local-agent is unavailable: connection refused")

    def source_code_qa_model_availability_get(self):
        raise ToolError("Mac local-agent is unavailable: connection refused")


class _NoStartThread:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def start(self):
        return None


class _FailingPortalJobService:
    def __init__(self, message):
        self.message = message

    def preview(self, *, progress_callback):
        progress_callback("bpmis_preview", "Calling BPMIS preview fake.", 1, 1)
        raise ToolError(self.message)

    def run(self, *, dry_run, progress_callback):
        progress_callback("bpmis_run", "Calling BPMIS run fake.", 1, 1)
        raise ToolError(self.message)


class _FailingProjectSyncService:
    def __init__(self, message):
        self.message = message

    def sync_projects(self, *, user_key, pm_email, progress_callback):
        progress_callback("bpmis_project_sync", "Calling BPMIS project sync fake.", 1, 1)
        raise ToolError(self.message)


class _SlowPortalJobService:
    def __init__(self):
        self.calls = 0

    def preview(self, *, progress_callback):
        self.calls += 1
        progress_callback("bpmis_preview", "Slow fake BPMIS preview.", 1, 2)
        time.sleep(0.02)
        return ([{"status": "ready", "jira_title": "Preview row", "timing_step": "bpmis_preview"}], [])

    def run(self, *, dry_run, progress_callback):
        self.calls += 1
        progress_callback("bpmis_run", "Slow fake BPMIS run.", 1, 2)
        time.sleep(0.02)
        return [{"status": "created", "jira_title": "Run row", "timing_step": "bpmis_run"}]


class TeamPortalAccessTests(unittest.TestCase):
    @staticmethod
    def _login_non_admin(client, email="teammate@npt.sg"):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": email, "name": "Teammate"}
            session["google_credentials"] = {"token": "x", "scopes": []}

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
                self.assertIn(b"Risk PM Tool", response.data)
                self.assertIn(b"Continue with Google", response.data)
                self.assertIn(b"risk_pm_login_background.png", response.data)
                self.assertIn(b"NPT Risk PM Workspace", response.data)
                self.assertIn(b"BPMIS Projects", response.data)
                self.assertIn(b"Source Code Q&amp;A", response.data)
                self.assertIn(b"Effort Assessment", response.data)
                self.assertIn(b"Team Dashboard", response.data)
                self.assertNotIn(b"Manage My Projects", response.data)
                self.assertNotIn(b"Report Intelligence", response.data)
                self.assertNotIn(b"PRD Self-Assessment", response.data)

    def test_login_image_gate_css_contract_is_present(self):
        stylesheet = Path("static/style.css").read_text(encoding="utf-8")

        self.assertIn(".page-shell.page-shell-login-image", stylesheet)
        self.assertIn(".login-image-hero", stylesheet)
        self.assertIn(".login-image-stage", stylesheet)
        self.assertIn(".login-image-hero-bg", stylesheet)
        self.assertIn(".login-image-google-button", stylesheet)
        self.assertIn(".login-image-accessibility-copy", stylesheet)

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

    def test_forwarded_https_scheme_is_used_for_slash_redirects(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://breeze-lung-clunky.ngrok-free.dev",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get(
                    "/prd-briefing",
                    follow_redirects=False,
                    headers={
                        "Host": "breeze-lung-clunky.ngrok-free.dev",
                        "X-Forwarded-Proto": "https",
                        "X-Forwarded-Host": "breeze-lung-clunky.ngrok-free.dev",
                    },
                )

        self.assertEqual(response.status_code, 308)
        self.assertEqual(response.headers["Location"], "https://breeze-lung-clunky.ngrok-free.dev/prd-briefing/")

    def test_non_portal_google_user_is_logged_out_and_shown_access_denied(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "xiaodong.zheng1991@gmail.com",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "blocked@example.com", "name": "Blocked User"}
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

                response = client.get("/?workspace=run")
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Allowed User", response.data)
                self.assertIn(b"Logout", response.data)
                self.assertEqual(response.headers.get("Cache-Control"), "no-store, private, max-age=0")
                self.assertEqual(response.headers.get("Pragma"), "no-cache")
                self.assertEqual(response.headers.get("Expires"), "0")

    def test_uat_stage_renders_environment_banner(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_STAGE": "uat",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "allowed@npt.sg", "name": "Allowed User"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"environment-banner-uat", response.data)
        self.assertIn(b">UAT<", response.data)
        self.assertIn(b"Testing environment", response.data)

    def test_uat_stage_renders_environment_banner_before_login(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_STAGE": "uat",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"environment-banner-uat", response.data)
        self.assertIn(b">UAT<", response.data)
        self.assertIn(b"Testing environment", response.data)
        self.assertIn(b"Continue with Google", response.data)

    def test_live_stage_does_not_render_environment_banner_by_default(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_STAGE": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "allowed@npt.sg", "name": "Allowed User"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"environment-banner-uat", response.data)
        self.assertNotIn(b"Testing environment", response.data)

    def test_healthz_returns_pinned_release_revision(self):
        _current_release_revision.cache_clear()
        try:
            with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
                os.environ,
                {
                    "FLASK_SECRET_KEY": "test-secret",
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "TEAM_PORTAL_DATA_DIR": temp_dir,
                    "TEAM_PORTAL_RELEASE_REVISION": "uat-sha-123",
                },
                clear=False,
            ):
                app = create_app()
                app.testing = True

                with app.test_client() as client:
                    response = client.get("/healthz")
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.get_json(), {"status": "ok", "revision": "uat-sha-123"})
        finally:
            _current_release_revision.cache_clear()

    def test_cloud_run_index_degrades_when_local_agent_is_offline(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "allowed@npt.sg",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "LOCAL_AGENT_MODE": "sync",
                "LOCAL_AGENT_BASE_URL": "https://agent.example",
                "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
                "LOCAL_AGENT_BPMIS_ENABLED": "true",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=_UnavailableLocalAgentClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "allowed@npt.sg", "name": "Allowed User"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/?workspace=run")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Allowed User", response.data)
            self.assertIn(b"local-agent backed data and actions are temporarily unavailable", response.data)

    def test_source_code_qa_config_reports_local_agent_unavailable_without_500(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "LOCAL_AGENT_MODE": "sync",
                "LOCAL_AGENT_BASE_URL": "https://agent.example",
                "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
                "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED": "true",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=_UnavailableLocalAgentClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/api/source-code-qa/config")

            self.assertEqual(response.status_code, 503)
            payload = response.get_json()
            self.assertEqual(payload["status"], "error")
            self.assertEqual(payload["error_category"], "local_agent_unavailable")

    def test_non_admin_shared_portal_route_and_api_matrix(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
                "SOURCE_CODE_QA_GEMINI_API_KEY": "",
                "GEMINI_API_KEY": "",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=_FakeNonAdminBPMISClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x", "scopes": []}

                default_response = client.get("/", follow_redirects=False)
                self.assertEqual(default_response.status_code, 302)
                self.assertEqual(default_response.headers["Location"], "/source-code-qa")

                source_page = client.get("/source-code-qa")
                self.assertEqual(source_page.status_code, 200)
                self.assertIn(b"data-source-question", source_page.data)
                self.assertIn(b"data-source-query", source_page.data)
                self.assertIn(b"data-source-session-list", source_page.data)
                self.assertIn(b"Source Code Q&amp;A", source_page.data)
                self.assertIn(b"PRD Self-Assessment", source_page.data)
                self.assertIn(b"BPMIS Automation Tool", source_page.data)
                for admin_marker in (
                    b"Repo Admin",
                    b"Repository Mapping",
                    b"Effort Assessment",
                    b"Sync / Refresh",
                    b"Save Config",
                    b"Model Availability",
                    b"Team Dashboard",
                    b"Meeting Recorder",
                    b"SeaTalk Management",
                ):
                    self.assertNotIn(admin_marker, source_page.data)

                source_config = client.get("/api/source-code-qa/config")
                self.assertEqual(source_config.status_code, 200)
                self.assertFalse(source_config.get_json()["can_manage"])

                session_list = client.get("/api/source-code-qa/sessions")
                self.assertEqual(session_list.status_code, 200)
                self.assertEqual(session_list.get_json()["status"], "ok")

                session_create = client.post(
                    "/api/source-code-qa/sessions",
                    json={"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge", "title": "Probe"},
                )
                self.assertEqual(session_create.status_code, 200)
                self.assertEqual(session_create.get_json()["session"]["owner_email"], "teammate@npt.sg")

                prd_self_assessment = client.get("/prd-self-assessment")
                self.assertEqual(prd_self_assessment.status_code, 200)

                version_search = client.get("/api/productization-upgrade-summary/versions?q=26Q2")
                self.assertEqual(version_search.status_code, 200)
                self.assertEqual(version_search.get_json()["items"][0]["version_id"], "88")

                issue_search = client.get("/api/productization-upgrade-summary/issues?version_id=88")
                self.assertEqual(issue_search.status_code, 200)
                self.assertEqual(issue_search.get_json()["status"], "ok")

                blocked_pages = {
                    "/team-dashboard": "/access-denied",
                    "/gmail-sea-talk-demo": "/",
                    "/meeting-recorder": "/",
                }
                for path, expected_location in blocked_pages.items():
                    with self.subTest(path=path):
                        response = client.get(path, follow_redirects=False)
                        self.assertEqual(response.status_code, 302)
                        self.assertEqual(response.headers["Location"], expected_location)

                blocked_requests = [
                    ("post", "/api/source-code-qa/config", {"pm_team": "AF", "country": "All", "repositories": []}),
                    ("post", "/api/source-code-qa/sync", {"pm_team": "AF", "country": "All"}),
                    ("post", "/api/source-code-qa/model-availability", {"availability": {"codex_cli_bridge": True}}),
                    ("get", "/api/source-code-qa/runtime-evidence?pm_team=AF&country=SG", None),
                    ("post", "/api/source-code-qa/effort-assessment", {"pm_team": "AF", "country": "All", "requirement": "new flow"}),
                    ("get", "/api/team-dashboard/config", None),
                    ("post", "/admin/team-dashboard/members", {"teams": {"AF": {"member_emails": ["teammate@npt.sg"]}}}),
                    ("get", "/api/team-dashboard/monthly-report/template", None),
                    ("post", "/admin/team-dashboard/monthly-report-template", {"template": "x"}),
                    ("post", "/admin/team-dashboard/report-intelligence", {"vip_people": []}),
                    ("get", "/api/gmail-sea-talk-demo/dashboard", None),
                    ("get", "/api/meeting-recorder/diagnostics", None),
                ]
                for method, path, payload in blocked_requests:
                    with self.subTest(method=method, path=path):
                        caller = getattr(client, method)
                        response = caller(path, json=payload) if payload is not None else caller(path)
                        self.assertEqual(response.status_code, 403)
                        self.assertEqual(response.get_json()["status"], "error")

    def test_non_admin_related_routes_are_explicitly_classified(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
                "SOURCE_CODE_QA_GEMINI_API_KEY": "",
                "GEMINI_API_KEY": "",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=_FakeNonAdminBPMISClient()):
            app = create_app()
            app.testing = True

            related_rules = {
                rule.endpoint
                for rule in app.url_map.iter_rules()
                if any(
                    marker in rule.rule
                    for marker in (
                        "source-code-qa",
                        "prd-self-assessment",
                        "productization-upgrade-summary",
                        "/api/jobs",
                        "team-dashboard",
                        "meeting-recorder",
                        "gmail-sea-talk-demo",
                        "local-agent",
                    )
                )
            }
            expected_endpoints = {
                "create_preview_job",
                "create_run_job",
                "create_sync_bpmis_projects_job",
                "get_job",
                "gmail_seatalk_demo",
                "gmail_seatalk_demo_dashboard_api",
                "gmail_seatalk_demo_gmail_export",
                "gmail_seatalk_demo_gmail_export_manifest",
                "gmail_seatalk_demo_gmail_export_prewarm",
                "gmail_seatalk_demo_network_api",
                "gmail_seatalk_demo_seatalk_api",
                "gmail_seatalk_demo_seatalk_export",
                "gmail_seatalk_demo_seatalk_insights_api",
                "gmail_seatalk_demo_seatalk_name_mappings",
                "gmail_seatalk_demo_seatalk_open_todos_api",
                "gmail_seatalk_demo_seatalk_project_updates_api",
                "gmail_seatalk_demo_seatalk_todo_complete",
                "gmail_seatalk_demo_seatalk_todos_api",
                "local_agent_public_proxy",
                "meeting_recorder_asset",
                "meeting_recorder_browser_audio_api",
                "meeting_recorder_delete_api",
                "meeting_recorder_diagnostics_api",
                "meeting_recorder_page",
                "meeting_recorder_process_api",
                "meeting_recorder_process_job_api",
                "meeting_recorder_record_api",
                "meeting_recorder_records_api",
                "meeting_recorder_reminder_telemetry_api",
                "meeting_recorder_reminders_api",
                "meeting_recorder_repair_video_api",
                "meeting_recorder_send_email_api",
                "meeting_recorder_signal_check_api",
                "meeting_recorder_start_api",
                "meeting_recorder_stop_api",
                "meeting_recorder_upcoming_api",
                "prd_self_assessment_page",
                "prd_self_assessment_review_api",
                "prd_self_assessment_summary_api",
                "productization_upgrade_summary_issues",
                "productization_upgrade_summary_llm_descriptions",
                "productization_upgrade_summary_versions",
                "save_team_dashboard_members",
                "save_team_dashboard_monthly_report_template",
                "save_team_dashboard_report_intelligence",
                "source_code_qa",
                "source_code_qa_attachment_api",
                "source_code_qa_attachments_api",
                "source_code_qa_config_api",
                "source_code_qa_effort_assessment_api",
                "source_code_qa_effort_assessment_job_api",
                "source_code_qa_effort_assessment_job_events_api",
                "source_code_qa_effort_assessment_latest_api",
                "source_code_qa_feedback_api",
                "source_code_qa_model_availability_api",
                "source_code_qa_query_api",
                "source_code_qa_query_job_api",
                "source_code_qa_query_job_events_api",
                "source_code_qa_runtime_evidence_api",
                "source_code_qa_runtime_evidence_delete_api",
                "source_code_qa_save_config_api",
                "source_code_qa_session_api",
                "source_code_qa_session_archive_api",
                "source_code_qa_sessions_api",
                "source_code_qa_sync_api",
                "team_dashboard_config",
                "team_dashboard_link_biz_project_jira",
                "team_dashboard_link_biz_project_suggestions",
                "team_dashboard_link_biz_projects",
                "team_dashboard_monthly_report_draft",
                "team_dashboard_monthly_report_latest_draft",
                "team_dashboard_monthly_report_send",
                "team_dashboard_monthly_report_template",
                "team_dashboard_page",
                "team_dashboard_prd_review",
                "team_dashboard_prd_summary",
                "team_dashboard_report_intelligence_seatalk_name_mappings",
                "team_dashboard_tasks",
                "link_team_dashboard_biz_project",
                "save_team_dashboard_key_project",
            }
            self.assertEqual(related_rules - expected_endpoints, set())

            with app.test_client() as client:
                self._login_non_admin(client)
                route_expectations = [
                    ("get", "/source-code-qa", {200}),
                    ("get", "/prd-self-assessment", {200}),
                    ("get", "/api/source-code-qa/config", {200}),
                    ("get", "/api/source-code-qa/sessions", {200}),
                    ("post", "/api/source-code-qa/sessions", {200}, {"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"}),
                    ("post", "/api/source-code-qa/query", {400}, {"pm_team": "AF", "country": "All", "question": "x", "llm_provider": "gemini"}),
                    ("post", "/api/source-code-qa/feedback", {200}, {"rating": "useful", "question": "x"}),
                    ("get", "/api/source-code-qa/attachments/missing", {400}),
                    ("post", "/api/source-code-qa/attachments", {400}),
                    ("get", "/api/productization-upgrade-summary/versions?q=26Q2", {200}),
                    ("get", "/api/productization-upgrade-summary/issues?version_id=88", {200}),
                    ("get", "/api/productization-upgrade-summary/llm-descriptions?version_id=88", {200}),
                    ("post", "/api/jobs/preview", {200}),
                    ("post", "/api/jobs/run", {200}),
                    ("post", "/api/jobs/sync-bpmis-projects", {200}),
                    ("get", "/api/jobs/missing", {404}),
                    ("get", "/team-dashboard", {302}),
                    ("get", "/meeting-recorder", {302}),
                    ("get", "/gmail-sea-talk-demo", {302}),
                    ("get", "/api/team-dashboard/config", {403}),
                    ("post", "/api/team-dashboard/key-projects", {403}, {}),
                    ("get", "/api/team-dashboard/tasks", {403}),
                    ("post", "/admin/team-dashboard/members", {403}, {}),
                    ("get", "/api/meeting-recorder/diagnostics", {403}),
                    ("get", "/api/meeting-recorder/process-jobs/missing", {403}),
                    ("get", "/api/gmail-sea-talk-demo/dashboard", {403}),
                    ("get", "/api/source-code-qa/runtime-evidence?pm_team=AF&country=SG", {403}),
                    ("post", "/api/source-code-qa/sync", {403}, {"pm_team": "AF", "country": "All"}),
                    ("post", "/api/source-code-qa/model-availability", {403}, {"availability": {"codex_cli_bridge": True}}),
                    ("post", "/api/source-code-qa/effort-assessment", {403}, {"pm_team": "AF", "country": "All", "requirement": "x"}),
                    ("get", "/api/source-code-qa/effort-assessment/latest", {403}),
                ]
                with patch("bpmis_jira_tool.web.threading.Thread", side_effect=lambda *args, **kwargs: _NoStartThread(*args, **kwargs)):
                    for item in route_expectations:
                        method, path, expected_statuses = item[:3]
                        payload = item[3] if len(item) > 3 else None
                        with self.subTest(method=method, path=path):
                            caller = getattr(client, method)
                            response = caller(path, json=payload) if payload is not None else caller(path)
                            self.assertIn(response.status_code, expected_statuses)

    def test_non_admin_rendered_pages_have_expected_smoke_contracts(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
                "SOURCE_CODE_QA_GEMINI_API_KEY": "",
                "GEMINI_API_KEY": "",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=_FakeNonAdminBPMISClient()):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                self._login_non_admin(client)
                index_response = client.get("/?workspace=productization-upgrade-summary")
                source_response = client.get("/source-code-qa")
                prd_response = client.get("/prd-self-assessment")

        self.assertEqual(index_response.status_code, 200)
        self.assertEqual(source_response.status_code, 200)
        self.assertEqual(prd_response.status_code, 200)

        index_soup = BeautifulSoup(index_response.get_data(as_text=True), "html.parser")
        self.assertIsNotNone(index_soup.select_one("[data-productization-llm-description-button]"))
        self.assertIsNotNone(index_soup.find("script", src=lambda value: value and "productization_upgrade_summary.js" in value))
        self.assertIsNone(index_soup.select_one("[data-team-dashboard]"))

        source_soup = BeautifulSoup(source_response.get_data(as_text=True), "html.parser")
        for selector in (
            "[data-source-question]",
            "[data-source-query]",
            "[data-source-session-list]",
            "[data-source-new-session]",
            "[data-source-session-messages]",
            "[data-source-llm-provider]",
        ):
            self.assertIsNotNone(source_soup.select_one(selector), selector)
        self.assertIsNone(source_soup.select_one("[data-source-view-tab='admin']"))
        self.assertIsNone(source_soup.select_one("[data-source-view-tab='effort']"))
        self.assertIsNotNone(source_soup.find("script", src=lambda value: value and "source_code_qa.js" in value))

        prd_soup = BeautifulSoup(prd_response.get_data(as_text=True), "html.parser")
        self.assertIsNotNone(prd_soup.select_one("[data-prd-self-assessment-url]"))
        self.assertIsNotNone(prd_soup.select_one("[data-prd-self-assessment-language]"))
        self.assertIsNotNone(prd_soup.find("script", src=lambda value: value and "prd_self_assessment.js" in value))

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

                response = client.get("/?workspace=run")
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

    def test_bpmis_background_job_failure_matrix_returns_pollable_stable_errors(self):
        scenarios = [
            ("preview", True, "BPMIS preview timed out.", "_build_service_from_config", _FailingPortalJobService),
            ("run", False, "BPMIS create returned 403.", "_build_service_from_config", _FailingPortalJobService),
            ("sync-bpmis-projects", False, "BPMIS project sync returned invalid JSON.", "_build_portal_project_sync_service", _FailingProjectSyncService),
        ]
        for action, dry_run, message, patch_target, service_factory in scenarios:
            with self.subTest(action=action), tempfile.TemporaryDirectory() as temp_dir, patch.dict(
                os.environ,
                {
                    "FLASK_SECRET_KEY": "test-secret",
                    "TEAM_PORTAL_DATA_DIR": temp_dir,
                    "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                    "BPMIS_API_ACCESS_TOKEN": "token",
                },
                clear=False,
            ):
                app = create_app()
                app.testing = True
                job = app.config["JOB_STORE"].create(action, title=f"{action} test")
                config_data = {"_user_key": "google:teammate@npt.sg", "pm_team": "AF", "sync_pm_email": "teammate@npt.sg"}
                with patch(f"bpmis_jira_tool.web.{patch_target}", return_value=service_factory(message)):
                    _run_background_job(app, job.job_id, action, app.config["SETTINGS"], config_data, {"token": "x"}, dry_run)
                snapshot = app.config["JOB_STORE"].snapshot(job.job_id)

            self.assertEqual(snapshot["state"], "failed")
            self.assertIn(message, snapshot["error"])
            serialized = str(snapshot).lower()
            self.assertNotIn("traceback", serialized)
            self.assertNotIn("secret", serialized)

    def test_bpmis_background_job_timing_and_polling_do_not_repeat_upstream_work(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "BPMIS_API_ACCESS_TOKEN": "token",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True
            service = _SlowPortalJobService()
            job = app.config["JOB_STORE"].create("preview", title="preview timing test")
            config_data = {"_user_key": "google:teammate@npt.sg", "pm_team": "AF", "sync_pm_email": "teammate@npt.sg"}
            with patch("bpmis_jira_tool.web._build_service_from_config", return_value=service):
                _run_background_job(app, job.job_id, "preview", app.config["SETTINGS"], config_data, {"token": "x"}, True)

            with app.test_client() as client:
                self._login_non_admin(client)
                first_poll = client.get(f"/api/jobs/{job.job_id}")
                second_poll = client.get(f"/api/jobs/{job.job_id}")

        self.assertEqual(first_poll.status_code, 200)
        self.assertEqual(second_poll.status_code, 200)
        first_payload = first_poll.get_json()
        self.assertEqual(first_payload["state"], "completed")
        self.assertEqual(first_payload["results"][0]["timing_step"], "bpmis_preview")
        self.assertGreaterEqual(first_payload["completed_at"] - first_payload["started_at"], 0.02)
        self.assertEqual(service.calls, 1)

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
                "LOCAL_AGENT_CONNECT_TIMEOUT_SECONDS": "3",
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
        self.assertEqual(proxy_request.call_args.kwargs["timeout"], (3, 7))

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

                response = client.get("/?workspace=run")

        self.assertEqual(response.status_code, 200)

if __name__ == "__main__":
    unittest.main()
