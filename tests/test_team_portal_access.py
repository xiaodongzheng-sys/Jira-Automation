import os
import time
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bs4 import BeautifulSoup
from flask import session

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


class _UnavailableBPMISClient:
    def ping(self):
        raise ToolError("Mac local-agent is unavailable: connection refused")


class _GenericFailingBPMISClient:
    def ping(self):
        raise RuntimeError("BPMIS proxy returned an unexpected error")


class _UnavailableLocalAgentClient:
    def bpmis_config_load(self, *, user_key):
        raise ToolError("Mac local-agent is unavailable: connection refused")

    def bpmis_team_profiles_load(self):
        raise ToolError("Mac local-agent is unavailable: connection refused")

    def source_code_qa_config(self, *, llm_provider=None):
        raise ToolError("Mac local-agent is unavailable: connection refused")


class _UnavailableTeamDashboardConfigStore:
    def load(self):
        raise ToolError("Mac local-agent is unavailable: connection refused")


class _NoStartThread:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def start(self):
        return None


class _FailingProjectSyncService:
    def __init__(self, message):
        self.message = message

    def sync_projects(self, *, user_key, pm_email, progress_callback):
        progress_callback("bpmis_project_sync", "Calling BPMIS project sync fake.", 1, 1)
        raise ToolError(self.message)


class _SlowPortalJobService:
    def __init__(self):
        self.calls = 0

    def sync_projects(self, *, user_key, pm_email, progress_callback):
        self.calls += 1
        progress_callback("bpmis_project_sync", "Slow fake BPMIS sync.", 1, 2)
        time.sleep(0.02)
        return [{"status": "created", "jira_title": "Synced project", "timing_step": "bpmis_project_sync"}]


class TeamPortalAccessTests(unittest.TestCase):
    @staticmethod
    def _login_non_admin(client, email="teammate@npt.sg"):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": email, "name": "Teammate"}
            session["google_credentials"] = {"token": "x", "scopes": []}

    @staticmethod
    def _login_admin(client, name="Admin"):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": name}
            session["google_credentials"] = {"token": "x", "scopes": []}

    def test_shared_mode_renders_public_home_for_anonymous_user(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg,monee.com,seamoney.com",
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
                self.assertIn(b"Risk PM Workspace", response.data)
                self.assertIn(b"data-public-home-sign-in", response.data)
                self.assertIn(b"Sign in with Google", response.data)
                self.assertNotIn(b"Open Reports", response.data)
                self.assertNotIn(b"Open Version Plan", response.data)
                self.assertNotIn(b"session-bar", response.data)
                soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
                self.assertEqual(
                    [node.get_text(strip=True) for node in soup.select(".site-switcher-tab")],
                    [],
                )

    def test_cloud_home_renders_public_home_for_anonymous_user(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg,monee.com,seamoney.com",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_PORTAL_MAC_FULL_PORTAL_URL": "https://app.bankpmtool.uk/portal-home",
            },
            clear=False,
        ), patch(
            "bpmis_jira_tool.web._build_local_agent_client",
            side_effect=AssertionError("cloud home should not call local-agent"),
        ), patch(
            "bpmis_jira_tool.web._mac_full_portal_is_available",
            return_value=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/", follow_redirects=False)

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Risk PM Workspace", response.data)
            self.assertIn(b"data-public-home-sign-in", response.data)
            self.assertIn(b"Sign in with Google", response.data)
            self.assertNotIn(b"Open Full Portal", response.data)
            self.assertIn(b"/cloud-static/style.css", response.data)
            self.assertNotIn(b"Checking Mac portal availability", response.data)
            self.assertNotIn(b"fetch('/healthz'", response.data)
            self.assertNotIn(b"session-bar", response.data)
            # Anonymous visitors see no nav tabs (all pages require login).
            soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
            self.assertEqual(
                [node.get_text(strip=True) for node in soup.select(".site-switcher-tab")],
                [],
            )

    def test_cloud_home_shows_only_full_portal_when_full_portal_is_available(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_PORTAL_MAC_FULL_PORTAL_URL": "https://app.bankpmtool.uk/portal-home",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._mac_full_portal_is_available", return_value=True):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                self._login_admin(client)
                response = client.get("/", follow_redirects=False)

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Risk PM Workspace", response.data)
            self.assertIn(b"Full portal is available.", response.data)
            self.assertIn(b"Open Full Portal", response.data)
            self.assertIn(b"https://app.bankpmtool.uk/portal-home?workspace=run", response.data)
            self.assertNotIn(b"https://app.bankpmtool.uk/?workspace=run", response.data)
            self.assertNotIn(b"Open Version Plan", response.data)
            self.assertNotIn(b"Version Plan</h3>", response.data)
            self.assertNotIn(b"Checking Mac portal availability", response.data)
            self.assertNotIn(b"Cloud Run", response.data)
            self.assertIn(b"site-switcher", response.data)

    def test_cloud_home_allows_non_admin_user_to_access_version_plan(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg,monee.com,seamoney.com",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_PORTAL_MAC_FULL_PORTAL_URL": "https://app.bankpmtool.uk/portal-home",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._mac_full_portal_is_available", return_value=False):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                self._login_non_admin(client, email="sophia.wangzj@npt.sg")
                # Non-admin NPT users can now access Version Plan (login required,
                # but allowlisted domain).
                public_plan = client.get("/version-plan", follow_redirects=False)
                public_api = client.get("/api/team-dashboard/version-plan/af")

            self.assertEqual(public_plan.status_code, 200)
            self.assertIn(b"data-version-plan-content", public_plan.data)
            self.assertEqual(public_api.status_code, 200)

    def test_cloud_home_shows_only_version_plan_for_admin_when_full_portal_offline(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_PORTAL_MAC_FULL_PORTAL_URL": "https://app.bankpmtool.uk/portal-home",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._mac_full_portal_is_available", return_value=False):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                self._login_admin(client)
                response = client.get("/", follow_redirects=False)
                dashboard_response = client.get("/team-dashboard", follow_redirects=False)

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Risk PM Workspace", response.data)
            self.assertIn(b"Full portal is offline. Use AF Version Plan.", response.data)
            self.assertIn(b"Open Version Plan", response.data)
            self.assertNotIn(b"Open Full Portal", response.data)
            self.assertNotIn(b"https://app.bankpmtool.uk/portal-home?workspace=run", response.data)
            self.assertNotIn(b'site-switcher-subtab is-active" href="/portal-home?workspace=run"', response.data)
            self.assertIn(b"site-switcher", response.data)
            self.assertEqual(dashboard_response.status_code, 200)
            self.assertIn(b'BPMIS Automation Tool</a>', dashboard_response.data)
            self.assertNotIn(b'href="https://app.bankpmtool.uk/?workspace=run"', dashboard_response.data)

    def test_portal_home_lands_on_version_plan_and_bpmis_is_admin_only(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                # A blocked (non-admin) session is cleared on the first gated request,
                # so sign in again before each probe.
                self._login_non_admin(client, email="teammate@npt.sg")
                default_response = client.get("/portal-home", follow_redirects=False)
                self._login_non_admin(client, email="teammate@npt.sg")
                run_response = client.get("/portal-home?workspace=run", follow_redirects=False)
                self._login_non_admin(client, email="teammate@npt.sg")
                bpmis_response = client.get("/portal-home?workspace=bpmis", follow_redirects=False)
                self._login_non_admin(client, email="teammate@npt.sg")
                productization_response = client.get("/portal-home?workspace=productization-upgrade-summary", follow_redirects=False)

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
                    session["google_credentials"] = {"token": "x", "scopes": []}
                admin_default_response = client.get("/portal-home", follow_redirects=False)
                admin_bpmis_response = client.get("/portal-home?workspace=bpmis", follow_redirects=False)

        # Non-admin allowed users redirect to Version Plan (their default landing).
        self.assertEqual(default_response.status_code, 302)
        self.assertEqual(default_response.headers["Location"], "/version-plan")
        self.assertEqual(run_response.status_code, 302)
        self.assertEqual(run_response.headers["Location"], "/version-plan")
        # BPMIS and Productization workspaces are admin-only.
        self.assertEqual(bpmis_response.status_code, 302)
        self.assertEqual(bpmis_response.headers["Location"], "/access-denied")
        self.assertEqual(productization_response.status_code, 302)
        self.assertEqual(productization_response.headers["Location"], "/access-denied")
        self.assertEqual(admin_default_response.status_code, 302)
        self.assertEqual(admin_default_response.headers["Location"], "/version-plan")
        self.assertEqual(admin_bpmis_response.status_code, 200)
        self.assertIn(b"BPMIS Automation Tool", admin_bpmis_response.data)

    def test_cloud_and_mac_apps_share_login_session_when_secret_matches(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "shared-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_PORTAL_MAC_FULL_PORTAL_URL": "https://app.bankpmtool.uk/portal-home",
            },
            clear=False,
        ):
            cloud_app = create_app()
            cloud_app.testing = True
            mac_app = create_app()
            mac_app.testing = True

            with cloud_app.test_client() as cloud_client:
                self._login_admin(cloud_client)
                session_cookie = cloud_client.get_cookie("session")
                self.assertIsNotNone(session_cookie)

            with mac_app.test_client() as mac_client:
                mac_client.set_cookie("session", session_cookie.value, domain="localhost", path="/")
                portal_response = mac_client.get("/portal-home", follow_redirects=False)
                dashboard_response = mac_client.get("/team-dashboard", follow_redirects=False)

            self.assertEqual(portal_response.status_code, 302)
            self.assertEqual(portal_response.headers["Location"], "/version-plan")
            self.assertEqual(dashboard_response.status_code, 200)
            self.assertIn(b"Version Plan", dashboard_response.data)

    def test_cloud_and_mac_apps_do_not_share_login_session_when_secret_differs(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "cloud-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_PORTAL_MAC_FULL_PORTAL_URL": "https://app.bankpmtool.uk/portal-home",
            },
            clear=False,
        ):
            cloud_app = create_app()
            cloud_app.testing = True
            with cloud_app.test_client() as cloud_client:
                self._login_non_admin(cloud_client, email="jireh.tanyx@npt.sg")
                session_cookie = cloud_client.get_cookie("session")
                self.assertIsNotNone(session_cookie)

        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "mac-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            mac_app = create_app()
            mac_app.testing = True

            with mac_app.test_client() as mac_client:
                mac_client.set_cookie("session", session_cookie.value, domain="localhost", path="/")
                portal_response = mac_client.get("/portal-home", follow_redirects=False)

        self.assertEqual(portal_response.status_code, 302)
        self.assertIn(portal_response.headers["Location"], {"/", "/access-denied"})

    def test_cloud_version_plan_page_requires_login_for_anonymous(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg,monee.com,seamoney.com",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                # Anonymous visitors are redirected to login.
                response = client.get("/version-plan", follow_redirects=False)

            self.assertEqual(response.status_code, 302)

    def test_cloud_version_plan_page_renders_for_signed_in_user(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg,monee.com,seamoney.com",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                self._login_admin(client)
                response = client.get("/version-plan")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Version Plan", response.data)
            self.assertIn(b"data-version-plan-content", response.data)
            self.assertIn(b"/cloud-static/team_dashboard.js", response.data)
            self.assertNotIn(b"data-team-dashboard-tab=\"tasks\"", response.data)

    def test_cloud_version_plan_page_renders_when_team_dashboard_config_unavailable_for_admin(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x", "scopes": []}

                with patch(
                    "bpmis_jira_tool.web._get_team_dashboard_config_store",
                    return_value=_UnavailableTeamDashboardConfigStore(),
                ):
                    response = client.get("/version-plan")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Version Plan", response.data)
        self.assertIn(b"data-version-plan-content", response.data)
        self.assertNotIn(b'data-team-dashboard-panel="admin"', response.data)

    def test_version_plan_auto_sync_uses_cached_data_when_local_agent_unavailable(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "BPMIS_CALL_MODE": "local_agent",
                "LOCAL_AGENT_BPMIS_ENABLED": "true",
                "LOCAL_AGENT_BASE_URL": "https://agent.example",
                "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
                "BPMIS_API_ACCESS_TOKEN": "token",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=_UnavailableBPMISClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x", "scopes": []}

                auto_response = client.get("/api/team-dashboard/version-plan/af")
                manual_response = client.post("/api/team-dashboard/version-plan/af/sync")

        self.assertEqual(auto_response.status_code, 200)
        auto_payload = auto_response.get_json()
        self.assertFalse(auto_payload["sync_queued"])
        self.assertNotEqual(auto_payload["sync_state"]["state"], "error")
        self.assertEqual(manual_response.status_code, 400)
        self.assertIn("local-agent", manual_response.get_json()["sync_state"]["error"])

    def test_version_plan_manual_sync_returns_json_when_unexpected_error_occurs(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "BPMIS_CALL_MODE": "local_agent",
                "LOCAL_AGENT_BPMIS_ENABLED": "true",
                "LOCAL_AGENT_BASE_URL": "https://agent.example",
                "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
                "BPMIS_API_ACCESS_TOKEN": "token",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=_GenericFailingBPMISClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x", "scopes": []}

                response = client.post("/api/team-dashboard/version-plan/af/sync")

        payload = response.get_json()
        self.assertEqual(response.status_code, 500)
        self.assertEqual(payload["status"], "ok")
        self.assertFalse(payload["sync_queued"])
        self.assertEqual(payload["sync_state"]["state"], "error")
        self.assertIn("BPMIS proxy returned an unexpected error", payload["sync_state"]["error"])

    def test_cloud_home_google_login_defaults_to_version_plan_for_admin_without_explicit_next(self):
        def fake_finish_google_oauth(*_args, **_kwargs):
            session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
            session["google_credentials"] = {"token": "x", "scopes": []}

        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web.finish_google_oauth", side_effect=fake_finish_google_oauth):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/cloud-auth/google/callback?code=fake&state=fake", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/version-plan")

    def test_cloud_home_google_login_normalizes_explicit_full_portal_next_to_version_plan(self):
        def fake_finish_google_oauth(*_args, **_kwargs):
            session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong"}
            session["google_credentials"] = {"token": "x", "scopes": []}

        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://app.bankpmtool.uk",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web.finish_google_oauth", side_effect=fake_finish_google_oauth):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                client.get("/cloud-auth/google/login?next=/portal-home?workspace%3Drun", follow_redirects=False)
                response = client.get("/cloud-auth/google/callback?code=fake&state=fake", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/version-plan")

    def test_login_image_gate_css_contract_is_present(self):
        stylesheet = Path("static/style.css").read_text(encoding="utf-8")

        self.assertIn(".page-shell.page-shell-login-image", stylesheet)
        self.assertIn(".login-image-hero", stylesheet)
        self.assertIn(".login-image-stage", stylesheet)
        self.assertIn(".login-image-hero-bg", stylesheet)
        self.assertIn(".login-image-google-button", stylesheet)
        self.assertIn(".login-image-accessibility-copy", stylesheet)
        self.assertIn(".page-shell-login-image .flash-stack", stylesheet)
        self.assertIn("position: fixed", stylesheet)
        self.assertNotIn(".page-shell-login-image .flash-stack {\n  display: none;", stylesheet)

    def test_google_login_config_error_is_visible_on_public_home(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "GOOGLE_OAUTH_CLIENT_SECRET_FILE": str(Path(temp_dir) / "missing-client.json"),
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                response = client.get("/auth/google/login", follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Google OAuth client secret file was not found", response.data)
        self.assertIn(b"Risk PM Workspace", response.data)
        self.assertIn(b"Admin Sign In", response.data)

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
                self.assertEqual(response.status_code, 404)

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
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
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

    def test_admin_google_user_can_open_index(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAILS": "xiaodong.zheng@npt.sg",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Allowed User"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/?workspace=run")
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"Allowed User", response.data)
                self.assertIn(b"Logout", response.data)
                self.assertEqual(response.headers.get("Cache-Control"), "no-store, private, max-age=0")
                self.assertEqual(response.headers.get("Pragma"), "no-cache")
                self.assertEqual(response.headers.get("Expires"), "0")

    def test_static_css_and_js_revalidate_to_avoid_stale_mobile_assets(self):
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
                css_response = client.get("/static/team_dashboard.css")
                js_response = client.get("/static/team_dashboard.js")

        self.assertEqual(css_response.status_code, 200)
        self.assertEqual(js_response.status_code, 200)
        self.assertEqual(css_response.headers.get("Cache-Control"), "no-store, private, max-age=0, must-revalidate")
        self.assertEqual(js_response.headers.get("Cache-Control"), "no-store, private, max-age=0, must-revalidate")
        self.assertEqual(css_response.headers.get("Pragma"), "no-cache")
        self.assertEqual(js_response.headers.get("Pragma"), "no-cache")
        self.assertEqual(css_response.headers.get("Expires"), "0")
        self.assertEqual(js_response.headers.get("Expires"), "0")
        js_text = js_response.get_data(as_text=True)
        self.assertIn("timeZone: 'Asia/Singapore'", js_text)
        self.assertIn("${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} SGT", js_text)
        self.assertIn("formatSingaporeTimestamp(result.updated_at || '')", js_text)

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
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Allowed User"}
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
                    payload = response.get_json()
                    self.assertEqual(payload["status"], "ok")
                    self.assertEqual(payload["revision"], "uat-sha-123")
                    self.assertEqual(payload["live_surface"], "mac_public_live")
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
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Allowed User"}
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
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x"}

                response = client.get("/api/source-code-qa/config")

            self.assertEqual(response.status_code, 503)
            payload = response.get_json()
            self.assertEqual(payload["status"], "error")
            self.assertIn("unavailable", payload["message"].lower())

    def test_non_admin_shared_portal_route_and_api_matrix(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg,monee.com,seamoney.com",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=_FakeNonAdminBPMISClient()):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x", "scopes": []}

                # Signed-in non-admin allowed users redirect to Version Plan
                # (not blocked from the portal anymore).
                default_response = client.get("/", follow_redirects=False)
                self.assertEqual(default_response.status_code, 302)
                self.assertEqual(default_response.headers["Location"], "/version-plan")

                # Allowed non-admin users can access Source Code QA (Repo Download view).
                self._login_non_admin(client)
                source_page = client.get("/source-code-qa", follow_redirects=False)
                self.assertEqual(source_page.status_code, 200)
                self.assertIn(b"Source Code Repo Download", source_page.data)

                self._login_non_admin(client)
                business_insights_response = client.get("/business-insights")
                self.assertEqual(business_insights_response.status_code, 200)
                self.assertNotIn(b"Credit Risk PH - Underwriting Funnel", business_insights_response.data)
                business_insights_api = client.get("/api/business-insights/reports?domain=credit-risk")
                self.assertEqual(business_insights_api.status_code, 200)
                self.assertEqual(business_insights_api.get_json()["reports"], [])
                anti_fraud_api = client.get("/api/business-insights/reports?domain=anti-fraud")
                self.assertEqual(anti_fraud_api.status_code, 200)
                self.assertGreater(len(anti_fraud_api.get_json()["reports"]), 0)

                version_plan_page = client.get("/version-plan", follow_redirects=False)
                self.assertEqual(version_plan_page.status_code, 200)
                version_plan_api = client.get("/api/team-dashboard/version-plan/af")
                self.assertEqual(version_plan_api.status_code, 200)
                sync_status_api = client.get("/api/team-dashboard/version-plan/af/sync-status")
                self.assertEqual(sync_status_api.status_code, 200)

                # Gated pages redirect signed-in non-admins to access-denied (and
                # clear the session, so sign in again before each probe).
                blocked_pages = [
                    "/prd-self-assessment",
                    "/prd-briefing/",
                    "/meeting-recorder",
                    "/meeting-translation",
                    "/reports",
                ]
                for path in blocked_pages:
                    with self.subTest(path=path):
                        self._login_non_admin(client)
                        response = client.get(path, follow_redirects=False)
                        self.assertEqual(response.status_code, 302)
                        self.assertEqual(response.headers["Location"], "/access-denied")
                # The legacy Team Dashboard route remains reachable, but Version Plan is
                # now exposed through the top-level navigation instead of an inner tab.
                self._login_non_admin(client)
                team_dashboard_page_response = client.get("/team-dashboard", follow_redirects=False)
                self.assertEqual(team_dashboard_page_response.status_code, 200)
                self.assertIn(b"data-version-plan-content", team_dashboard_page_response.data)
                self.assertNotIn(b'data-team-dashboard-tab="version-plan"', team_dashboard_page_response.data)

                # Admin-only POST endpoints reject signed-in non-admins with 403.
                for method, path, payload in [
                    ("post", "/admin/team-dashboard/members", {"teams": {"AF": {"member_emails": ["teammate@npt.sg"]}}}),
                    ("post", "/admin/team-dashboard/monthly-report-template", {"template": "x"}),
                    ("post", "/admin/team-dashboard/report-intelligence", {"vip_people": []}),
                ]:
                    with self.subTest(method=method, path=path):
                        self._login_non_admin(client)
                        response = getattr(client, method)(path, json=payload)
                        self.assertEqual(response.status_code, 403)
                        self.assertEqual(response.get_json()["status"], "error")

                # Admin-only APIs reject signed-in non-admins with 403.
                blocked_requests = [
                    ("post", "/api/source-code-qa/config", {"pm_team": "AF", "country": "All", "repositories": []}),
                    ("get", "/api/source-code-qa/sessions", None),
                    ("post", "/api/source-code-qa/sessions", {"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge", "title": "Probe"}),
                    ("post", "/api/source-code-qa/sync", {"pm_team": "AF", "country": "All"}),
                    ("get", "/api/source-code-qa/runtime-evidence?pm_team=AF&country=SG", None),
                    ("post", "/api/source-code-qa/effort-assessment", {"pm_team": "AF", "country": "All", "requirement": "new flow"}),
                    ("get", "/api/team-dashboard/config", None),
                    ("post", "/api/team-dashboard/version-plan/af/sync", {}),
                    ("post", "/api/team-dashboard/version-plan/af/cell", {}),
                    ("post", "/api/team-dashboard/version-plan/af/rows", {}),
                    ("get", "/api/team-dashboard/monthly-report/template", None),
                    ("get", "/api/team-dashboard/seatalk/insights", None),
                    ("get", "/api/meeting-recorder/diagnostics", None),
                    ("post", "/api/meeting-translation/start", {"target_language": "en"}),
                ]
                for method, path, payload in blocked_requests:
                    with self.subTest(method=method, path=path):
                        self._login_non_admin(client)
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
                        "meeting-translation",
                        "local-agent",
                    )
                )
            }
            expected_endpoints = {
                "create_sync_bpmis_projects_job",
                "get_job",
                "local_agent_public_proxy",
                "meeting_recorder_asset",
                "meeting_recorder_delete_api",
                "meeting_recorder_diagnostics_api",
                "meeting_recorder_page",
                "meeting_recorder_process_api",
                "meeting_recorder_process_job_api",
                "meeting_recorder_record_api",
                "meeting_recorder_records_api",
                "meeting_recorder_send_email_api",
                "meeting_recorder_signal_check_api",
                "meeting_recorder_start_api",
                "meeting_recorder_stop_api",
                "meeting_recorder_upcoming_api",
                "meeting_translation_events_api",
                "meeting_translation_page",
                "meeting_translation_start_api",
                "meeting_translation_stop_api",
                "prd_self_assessment_page",
                "prd_self_assessment_latest_api",
                "prd_self_assessment_review_api",
                "prd_self_assessment_sections_api",
                "prd_self_assessment_summary_api",
                "productization_upgrade_summary_issues",
                "productization_upgrade_summary_llm_descriptions",
                "productization_upgrade_summary_versions",
                "reports_page",
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
                "source_code_qa_generated_artifact_api",
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
                "source_code_qa_sync_job_api",
                "team_dashboard_config",
                "team_dashboard_daily_brief_download",
                "team_dashboard_daily_briefs",
                "team_dashboard_link_biz_project_jira",
                "team_dashboard_link_biz_project_suggestions",
                "team_dashboard_link_biz_projects",
                "team_dashboard_monthly_report_draft",
                "team_dashboard_monthly_report_latest_draft",
                "team_dashboard_monthly_report_send",
                "team_dashboard_monthly_report_style_guide_refresh",
                "team_dashboard_monthly_report_template",
                "team_dashboard_page",
                "team_dashboard_prd_review",
                "team_dashboard_prd_summary",
                "team_dashboard_report_intelligence_seatalk_name_mappings",
                "team_dashboard_seatalk_insights_api",
                "team_dashboard_seatalk_open_todos_api",
                "team_dashboard_seatalk_project_updates_api",
                "team_dashboard_seatalk_todo_complete",
                "team_dashboard_seatalk_todos_api",
                "team_dashboard_tasks",
                "team_dashboard_version_plan_af",
                "team_dashboard_version_plan_cell",
                "team_dashboard_version_plan_rows",
                "team_dashboard_version_plan_sync",
                "team_dashboard_version_plan_sync_status",
                "link_team_dashboard_biz_project",
                "save_team_dashboard_key_project",
                "save_team_dashboard_project_status",
                "source_code_qa_repo_download_api",
                "uat_local_agent_health_proxy",
                "uat_local_agent_public_proxy",
            }
            self.assertEqual(related_rules - expected_endpoints, set())

            extracted_route_modules = {
                "save_mapping_config": "bpmis_jira_tool.web_bpmis_routes",
                "create_sync_bpmis_projects_job": "bpmis_jira_tool.web_bpmis_routes",
                "meeting_recorder_page": "bpmis_jira_tool.web_meeting_recorder_routes",
                "meeting_translation_start_api": "bpmis_jira_tool.web_meeting_recorder_routes",
                "prd_self_assessment_page": "bpmis_jira_tool.web_prd_self_assessment_routes",
                "prd_self_assessment_review_api": "bpmis_jira_tool.web_prd_self_assessment_routes",
                "team_dashboard_seatalk_insights_api": "bpmis_jira_tool.web_team_dashboard_seatalk_routes",
                "productization_upgrade_summary_versions": "bpmis_jira_tool.web_productization_routes",
                "productization_upgrade_summary_llm_descriptions": "bpmis_jira_tool.web_productization_routes",
                "team_dashboard_page": "bpmis_jira_tool.web_team_dashboard_routes",
                "team_dashboard_tasks": "bpmis_jira_tool.web_team_dashboard_routes",
                "team_dashboard_monthly_report_draft": "bpmis_jira_tool.web_team_dashboard_routes",
            }
            for endpoint, module_name in extracted_route_modules.items():
                self.assertIn(endpoint, app.view_functions)
                self.assertEqual(app.view_functions[endpoint].__module__, module_name)

            with app.test_client() as client:
                route_expectations = [
                    ("get", "/source-code-qa", {200}),
                    ("get", "/prd-self-assessment", {302}),
                    ("get", "/prd-briefing/", {302}),
                    ("get", "/api/source-code-qa/config", {200}),
                    ("get", "/api/source-code-qa/sessions", {403}),
                    ("post", "/api/source-code-qa/sessions", {403}, {"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"}),
                    ("post", "/api/source-code-qa/query", {403}, {"pm_team": "AF", "country": "All", "question": "x", "llm_provider": "not-real"}),
                    ("post", "/api/source-code-qa/feedback", {403}, {"rating": "useful", "question": "x"}),
                    ("get", "/api/source-code-qa/attachments/missing", {403}),
                    ("post", "/api/source-code-qa/attachments", {403}),
                    ("get", "/api/productization-upgrade-summary/versions?q=26Q2", {200}),
                    ("get", "/api/productization-upgrade-summary/issues?version_id=88", {200}),
                    ("get", "/api/productization-upgrade-summary/llm-descriptions?version_id=88", {200}),
                    ("post", "/api/jobs/sync-bpmis-projects", {200}),
                    ("get", "/api/jobs/missing", {404}),
                    ("get", "/team-dashboard?tab=version-plan", {200}),
                    ("get", "/reports", {302}),
                    ("get", "/business-insights", {200}),
                    ("get", "/api/business-insights/reports?domain=credit-risk", {200}),
                    ("get", "/meeting-recorder", {302}),
                    ("get", "/meeting-translation", {302}),
                    ("get", "/gmail-sea-talk-demo", {404}),
                    ("get", "/api/team-dashboard/config", {403}),
                    ("get", "/api/team-dashboard/version-plan/af", {200}),
                    ("post", "/api/team-dashboard/version-plan/af/sync", {403}, {}),
                    ("post", "/api/team-dashboard/version-plan/af/rows", {403}, {"scope": "pipeline", "action": "add"}),
                    ("post", "/api/team-dashboard/key-projects", {403}, {}),
                    ("get", "/api/team-dashboard/tasks", {403}),
                    ("post", "/admin/team-dashboard/members", {403}, {}),
                    ("get", "/api/meeting-recorder/diagnostics", {403}),
                    ("get", "/api/meeting-recorder/process-jobs/missing", {403}),
                    ("post", "/api/meeting-translation/start", {403}, {"target_language": "en"}),
                    ("get", "/api/team-dashboard/seatalk/insights", {403}),
                    ("get", "/api/gmail-sea-talk-demo/dashboard", {404}),
                    ("get", "/api/source-code-qa/runtime-evidence?pm_team=AF&country=SG", {403}),
                    ("post", "/api/source-code-qa/sync", {403}, {"pm_team": "AF", "country": "All"}),
                    ("post", "/api/source-code-qa/effort-assessment", {403}, {"pm_team": "AF", "country": "All", "requirement": "x"}),
                    ("get", "/api/source-code-qa/effort-assessment/latest", {403}),
                ]
                with patch("bpmis_jira_tool.web.threading.Thread", side_effect=lambda *args, **kwargs: _NoStartThread(*args, **kwargs)):
                    for item in route_expectations:
                        method, path, expected_statuses = item[:3]
                        payload = item[3] if len(item) > 3 else None
                        with self.subTest(method=method, path=path):
                            # Admin-only APIs deny non-admins; re-login is harmless.
                            self._login_non_admin(client)
                            caller = getattr(client, method)
                            response = caller(path, json=payload) if payload is not None else caller(path)
                            self.assertIn(response.status_code, expected_statuses)
                audit_log = app.config["TEAM_DASHBOARD_CONFIG_STORE"].load()["version_plan"]["af"]["audit_log"]
                self.assertFalse(any(entry["action"] == "row_add" for entry in audit_log))

    def test_non_admin_rendered_pages_have_expected_smoke_contracts(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg,monee.com,seamoney.com",
                "TEAM_PORTAL_BASE_URL": "https://jira-tool.example.com",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
            },
            clear=False,
        ), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=_FakeNonAdminBPMISClient()):
            app = create_app()
            app.testing = True
            with app.test_client() as client:
                self._login_non_admin(client, email="jireh.tanyx@npt.sg")
                index_response = client.get("/?workspace=productization-upgrade-summary", follow_redirects=False)
                self._login_non_admin(client, email="jireh.tanyx@npt.sg")
                source_response = client.get("/source-code-qa", follow_redirects=False)
                prd_response = client.get("/prd-self-assessment", follow_redirects=False)
                self._login_non_admin(client, email="jireh.tanyx@npt.sg")
                team_dashboard_response = client.get("/team-dashboard?tab=version-plan", follow_redirects=False)
                version_plan_response = client.get("/version-plan", follow_redirects=False)
                denied_response = client.get("/access-denied")

        # Non-admins are blocked from admin-only portal pages...
        self.assertEqual(prd_response.status_code, 302)
        self.assertEqual(prd_response.headers["Location"], "/access-denied")
        self.assertEqual(team_dashboard_response.status_code, 200)
        self.assertIn(b"data-version-plan-content", team_dashboard_response.data)
        # ...while allowed pages render for signed-in non-admin users.
        self.assertEqual(source_response.status_code, 200)
        self.assertEqual(version_plan_response.status_code, 200)
        self.assertEqual(denied_response.status_code, 403)

        source_soup = BeautifulSoup(source_response.get_data(as_text=True), "html.parser")
        self.assertIn("Source Code Repo Download", source_soup.get_text(" ", strip=True))
        self.assertIsNone(source_soup.select_one("[data-download-password-form]"))
        self.assertIsNone(source_soup.select_one("[data-source-view-tab='chat']"))
        self.assertIsNone(source_soup.select_one("[data-source-question]"))
        self.assertIsNone(source_soup.select_one("[data-source-query]"))
        self.assertIsNone(source_soup.select_one("[data-source-session-list]"))
        self.assertIsNone(source_soup.select_one("[data-source-new-session]"))
        self.assertIsNone(source_soup.select_one("[data-source-session-messages]"))
        self.assertIsNone(source_soup.select_one("[data-source-llm-provider]"))
        self.assertIsNone(source_soup.select_one("[data-source-view-tab='admin']"))
        self.assertIsNone(source_soup.select_one("[data-source-view-tab='effort']"))
        self.assertIsNotNone(source_soup.select_one("[data-source-view-tab='download']"))
        self.assertIsNotNone(source_soup.find("link", href=lambda value: value and "source_code_qa.css" in value))
        self.assertIsNotNone(source_soup.find("script", src=lambda value: value and "source_code_qa_api.js" in value))
        self.assertIsNotNone(source_soup.find("script", src=lambda value: value and "source_code_qa.js" in value))

        version_plan_soup = BeautifulSoup(version_plan_response.get_data(as_text=True), "html.parser")
        self.assertIsNotNone(version_plan_soup.select_one("[data-version-plan-content]"))

        denied_soup = BeautifulSoup(denied_response.get_data(as_text=True), "html.parser")
        denied_text = denied_soup.get_text(" ", strip=True)
        self.assertIn("PRD Self-Assessment is available to signed-in portal users", denied_text)
        self.assertIn("PRD Briefing is restricted to the portal admin", denied_text)

    def test_meeting_translation_tab_follows_meeting_recorder_for_admin(self):
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
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x", "scopes": []}

                response = client.get("/meeting-translation")

        self.assertEqual(response.status_code, 200)
        soup = BeautifulSoup(response.get_data(as_text=True), "html.parser")
        labels = [node.get_text(strip=True) for node in soup.select(".site-switcher-tab")]
        self.assertIn("Meeting", labels)
        meeting_labels = [node.get_text(strip=True) for node in soup.select(".site-switcher-subtab")]
        self.assertIn("Meeting Recorder", meeting_labels)
        self.assertIn("Meeting Translation", meeting_labels)
        self.assertEqual(meeting_labels.index("Meeting Translation"), meeting_labels.index("Meeting Recorder") + 1)
        self.assertIsNotNone(soup.select_one("[data-meeting-translation-root]"))
        self.assertIsNotNone(soup.select_one("[data-translation-language]"))
        self.assertIsNotNone(soup.select_one("[data-translated-transcript]"))
        self.assertIsNotNone(soup.select_one("[data-original-transcript]"))

    def test_meeting_translation_start_validates_language_before_runtime_start(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "MEETING_TRANSLATION_OPENAI_API_KEY": "",
                "OPENAI_API_KEY": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Admin"}
                    session["google_credentials"] = {"token": "x", "scopes": []}

                invalid = client.post("/api/meeting-translation/start", json={"target_language": "fr"})
                missing_key = client.post("/api/meeting-translation/start", json={"target_language": "en"})

        self.assertEqual(invalid.status_code, 400)
        self.assertIn("supported translation language", invalid.get_json()["message"])
        self.assertEqual(missing_key.status_code, 400)
        self.assertIn("OPENAI_API_KEY", missing_key.get_json()["message"])

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

    def test_allowed_google_domain_user_is_not_blocked_from_index(self):
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

                # Allowlisted domain users are no longer blocked from the portal.
                # The index page renders (with productization tab as default for
                # non-admins), and the session is preserved.
                response = client.get("/?workspace=run", follow_redirects=False)
                self.assertEqual(response.status_code, 200)
                with client.session_transaction() as session:
                    self.assertIn("google_profile", session)
                    self.assertIn("google_credentials", session)

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
                payload = response.get_json()
                self.assertEqual(payload["status"], "ok")
                self.assertEqual(payload["revision"], _current_release_revision())

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
            ("sync-bpmis-projects", "BPMIS project sync returned invalid JSON.", "_build_portal_project_sync_service", _FailingProjectSyncService),
        ]
        for action, message, patch_target, service_factory in scenarios:
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
                    _run_background_job(app, job.job_id, action, app.config["SETTINGS"], config_data)
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
            job = app.config["JOB_STORE"].create("sync-bpmis-projects", title="sync timing test")
            config_data = {"_user_key": "google:teammate@npt.sg", "pm_team": "AF", "sync_pm_email": "teammate@npt.sg"}
            with patch("bpmis_jira_tool.web._build_portal_project_sync_service", return_value=service):
                _run_background_job(app, job.job_id, "sync-bpmis-projects", app.config["SETTINGS"], config_data)

            with app.test_client() as client:
                self._login_admin(client)
                first_poll = client.get(f"/api/jobs/{job.job_id}")
                second_poll = client.get(f"/api/jobs/{job.job_id}")

        self.assertEqual(first_poll.status_code, 200)
        self.assertEqual(second_poll.status_code, 200)
        first_payload = first_poll.get_json()
        self.assertEqual(first_payload["state"], "completed")
        self.assertEqual(first_payload["results"][0]["timing_step"], "bpmis_project_sync")
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
                self.assertEqual(response.status_code, 404)

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
