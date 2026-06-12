import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask, session

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool import web as web_module


class _FakeJobStore:
    def __init__(self):
        self.updates = []
        self.completed = []
        self.failed = []

    def update(self, *args, **kwargs):
        self.updates.append((args, kwargs))

    def complete(self, *args, **kwargs):
        self.completed.append((args, kwargs))

    def fail(self, *args, **kwargs):
        self.failed.append((args, kwargs))


class _FakeBriefingStore:
    def __init__(self):
        self.saved = []
        self.latest = {}

    def save_latest_tool_result(self, **kwargs):
        self.saved.append(kwargs)
        self.latest[(kwargs["owner_key"], kwargs["tool_key"])] = kwargs["payload"]

    def get_latest_tool_result(self, **kwargs):
        return self.latest.get((kwargs["owner_key"], kwargs["tool_key"]))


class _ImmediateThread:
    def __init__(self, *, target, args=(), kwargs=None, **_ignored):
        self.target = target
        self.args = args
        self.kwargs = kwargs or {}

    def start(self):
        self.target(*self.args, **self.kwargs)


class _CreatedJobStore(_FakeJobStore):
    def __init__(self):
        super().__init__()
        self.snapshots = {}
        self.created = []

    def snapshot(self, job_id):
        return self.snapshots.get(job_id)

    def create(self, action, title=""):
        job = SimpleNamespace(job_id=f"job-{len(self.created) + 1}", action=action, title=title)
        self.created.append(job)
        self.snapshots[job.job_id] = {"job_id": job.job_id, "action": action, "state": "queued"}
        return job


class WebCoreCoverageTests(unittest.TestCase):
    def _settings(self, temp_dir: str) -> Settings:
        return Settings(
            flask_secret_key="secret",
            google_oauth_client_secret_file=Path(temp_dir) / "client.json",
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url="https://portal.example",
            team_allowed_emails=(),
            team_allowed_email_domains=(),
            team_portal_data_dir=Path(temp_dir),
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Input",
            bpmis_base_url="https://bpmis.example",
            bpmis_api_access_token="token",
        )

    def _app(self, temp_dir: str) -> Flask:
        app = Flask(__name__)
        app.secret_key = "secret"
        app.add_url_rule("/access-denied", "access_denied", lambda: "denied")
        app.add_url_rule("/version-plan", "version_plan_page", lambda: "version")
        app.config["SETTINGS"] = self._settings(temp_dir)
        app.config["JOB_STORE"] = _FakeJobStore()
        app.config["TEAM_DASHBOARD_CONFIG_STORE"] = SimpleNamespace(load=lambda: {})
        app.config["MEETING_RECORD_STORE"] = SimpleNamespace(list_records=lambda **kwargs: [])
        app.config["GOOGLE_CREDENTIAL_STORE"] = SimpleNamespace(save=lambda **kwargs: None)
        app.config["PRD_BRIEFING_STORE"] = _FakeBriefingStore()
        app.config["SEATALK_NAME_MAPPING_STORE"] = SimpleNamespace(storage_path=Path(temp_dir) / "names.json", mappings=lambda: {})
        app.config["SEATALK_DAILY_CACHE_DIR"] = Path(temp_dir) / "seatalk"
        return app

    def test_web_pure_helpers_cover_error_classification_and_normalizers(self):
        with patch.dict(os.environ, {"BOOL_X": "yes", "BOOL_Y": "0"}, clear=False):
            self.assertTrue(web_module._bool_env_from_env("BOOL_X"))
            self.assertFalse(web_module._bool_env_from_env("BOOL_Y", True))
            self.assertTrue(web_module._bool_env_from_env("MISSING_BOOL", True))

        rows = [
            "bad",
            {"id": "", "count": "1"},
            {"id": "buddy-1", "count": "2", "priority_reason": "direct", "example": "ex"},
            {"id": "UID 1", "count": "bad", "priority_reason": "@mentioned", "type": "group"},
            {"id": "UID 2", "count": 1},
            {"id": "buddy-2", "count": 1, "example": "filled"},
        ]
        deduped = web_module._dedupe_seatalk_name_mapping_candidates(rows)
        self.assertEqual(deduped[0]["count"], 2)
        self.assertEqual(deduped[0]["priority_reason"], "@mentioned")
        self.assertEqual(deduped[0]["type"], "group")
        self.assertIsNone(web_module._safe_float(""))
        self.assertIsNone(web_module._safe_float("bad"))
        self.assertEqual(web_module._safe_float("3.5"), 3.5)
        self.assertEqual(web_module._safe_email_identity({"config_key": "Owner@NPT.SG"}), "owner@npt.sg")

        self.assertEqual(web_module._classify_portal_error(None)["error_code"], "unexpected_error")
        self.assertEqual(web_module._classify_portal_error(ConfigError("bad"))["error_code"], "oauth_host_misconfigured")
        tool_cases = {
            "Sign in with your NPT Google account": "auth_required",
            "not authorized for the team portal": "account_not_allowed",
            "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY missing": "portal_encryption_config",
            "duplicate system + market -> component rule": "duplicate_route_rule",
            "invalid system + market -> component rule": "invalid_route_rule_format",
            "invalid component default rule": "invalid_component_default_rule",
            "duplicate component default rule": "duplicate_component_default_rule",
            "component defaults are missing these routed components": "missing_component_defaults",
            "PM team is required": "invalid_pm_team",
            "version keyword is required": "missing_required_parameter",
            "Google Sheets spreadsheet denied": "google_sheet_access",
            "Codex LLM rate limit": "llm_rate_limited",
            "Codex timed out": "llm_timeout",
            "BPMIS failed": "bpmis_request_failed",
        }
        for message, code in tool_cases.items():
            with self.subTest(message=message):
                self.assertEqual(web_module._classify_portal_error(ToolError(message))["error_code"], code)
        self.assertTrue(web_module._classify_portal_error("temporary timeout")["error_retryable"])

        self.assertEqual(web_module._flatten_productization_component_values(None), [])
        self.assertEqual(web_module._flatten_productization_component_values(" A/B;C|D "), ["A", "B", "C", "D"])
        self.assertEqual(web_module._flatten_productization_component_values({"label": "Core", "id": 7}), ["Core", "7"])
        self.assertEqual(web_module._coerce_display_text([{"label": "A"}, "B"]), "A, B")
        self.assertEqual(web_module._coerce_display_text({"unknown": "x"}), "")
        self.assertEqual(web_module._extract_link_values(["https://a", {"href": "https://a"}, "none"]), [{"label": "https://a", "url": "https://a"}])
        self.assertEqual(web_module._extract_person_display({}), "")
        self.assertEqual(web_module._flatten_links(""), [])
        self.assertEqual(web_module._normalize_team_dashboard_task({"prd_link": "https://prd.example/doc\n"})["prd_links"][0]["url"], "https://prd.example/doc")
        self.assertEqual(web_module._safe_email_identity(), "")
        self.assertEqual(web_module._seatalk_mapping_reason_rank(""), 0)
        web_module._gmail_export_active_users.clear()
        self.assertFalse(web_module._try_acquire_gmail_export_lock(""))
        self.assertTrue(web_module._try_acquire_gmail_export_lock("Owner@NPT.SG"))
        self.assertFalse(web_module._try_acquire_gmail_export_lock("owner@npt.sg"))
        web_module._release_gmail_export_lock("owner@npt.sg")
        web_module._release_gmail_export_lock("")
        web_module._current_release_revision.cache_clear()
        with patch("bpmis_jira_tool.web_runtime_status.subprocess.run", side_effect=FileNotFoundError):
            self.assertEqual(web_module._current_release_revision(), "unknown")
        web_module._current_release_revision.cache_clear()
        with patch("bpmis_jira_tool.web.inspect.signature", side_effect=ValueError("bad signature")):
            self.assertTrue(web_module._callable_accepts_keyword(object(), "anything"))
        self.assertEqual(web_module._safe_relative_redirect_target("https://example.com/x"), "")
        self.assertEqual(web_module._safe_relative_redirect_target("//example.com/x"), "")
        self.assertEqual(web_module._safe_relative_redirect_target("/safe"), "/safe")

    def test_access_scope_and_service_builder_helpers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            settings = app.config["SETTINGS"]
            app.config["CONFIG_STORE"] = SimpleNamespace(
                load=lambda user_key=None: {"stored": True},
                save=lambda config, user_key=None: {"saved": user_key, **config},
                migrate=lambda from_user_key, to_user_key: None,
                _normalize=lambda data: dict(data),
            )
            app.config["BPMIS_PROJECT_STORE"] = object()
            fake_remote = SimpleNamespace(
                get_health=lambda: {"capabilities": {"seatalk_configured": True}},
                bpmis_config_save=lambda **kwargs: {"remote_saved": kwargs},
                bpmis_config_migrate=lambda **kwargs: {"remote_migrated": kwargs},
                bpmis_team_profile_save=lambda **kwargs: {"remote_profile": kwargs},
            )
            with app.test_request_context("/api/test"):
                self.assertFalse(web_module._google_session_can_call_live_google_apis())
                session["google_credentials"] = {
                    "token": "token",
                    "refresh_token": "refresh",
                    "token_uri": "https://oauth/token",
                    "client_id": "client",
                    "client_secret": "secret",
                    "scopes": [web_module.GOOGLE_DRIVE_READONLY_SCOPE],
                }
                session["google_profile"] = {"email": "owner@npt.sg"}
                self.assertTrue(web_module._google_session_can_call_live_google_apis())

                with patch("bpmis_jira_tool.web._site_requires_google_login", return_value=True), patch(
                    "bpmis_jira_tool.web._google_session_is_connected", return_value=False
                ):
                    self.assertEqual(web_module._require_google_login(settings, api=True)[1], web_module.HTTPStatus.UNAUTHORIZED)
                with patch("bpmis_jira_tool.web._require_google_login", return_value=("login", 401)):
                    self.assertEqual(web_module._require_seatalk_management_access(settings, api=True), ("login", 401))
                    self.assertEqual(web_module._require_meeting_recorder_access(settings, api=True), ("login", 401))
                    self.assertEqual(web_module._require_team_dashboard_access(settings, api=True), ("login", 401))
                    self.assertEqual(web_module._require_prd_self_assessment_access(settings, api=True), ("login", 401))
                # Version Plan is fully public now: the access helper never blocks.
                self.assertTrue(web_module._can_access_team_dashboard_version_plan({"email": "user@npt.sg"}))
                self.assertTrue(web_module._can_access_team_dashboard_version_plan({"email": ""}))
                self.assertIsNone(web_module._require_team_dashboard_version_plan_access(settings, api=True))
                self.assertIsNone(web_module._require_team_dashboard_version_plan_access(settings, api=False))
                with patch("bpmis_jira_tool.web._require_google_login", return_value=None), patch(
                    "bpmis_jira_tool.web._can_access_seatalk_management", return_value=False
                ):
                    self.assertEqual(web_module._require_seatalk_management_access(settings, api=True)[1], web_module.HTTPStatus.FORBIDDEN)
                with patch("bpmis_jira_tool.web._require_google_login", return_value=None), patch(
                    "bpmis_jira_tool.web._can_access_meeting_recorder", return_value=False
                ):
                    self.assertEqual(web_module._require_meeting_recorder_access(settings, api=True)[1], web_module.HTTPStatus.FORBIDDEN)
                with patch("bpmis_jira_tool.web._require_google_login", return_value=None), patch(
                    "bpmis_jira_tool.web._can_access_team_dashboard", return_value=False
                ), patch("bpmis_jira_tool.web._get_user_identity", return_value={"email": "user@npt.sg"}):
                    self.assertEqual(web_module._require_team_dashboard_access(settings, api=True)[1], web_module.HTTPStatus.FORBIDDEN)
                with patch("bpmis_jira_tool.web._require_google_login", return_value=None), patch(
                    "bpmis_jira_tool.web._can_access_prd_self_assessment", return_value=False
                ):
                    self.assertEqual(web_module._require_prd_self_assessment_access(settings, api=True)[1], web_module.HTTPStatus.FORBIDDEN)

                owner_settings = replace(settings, gmail_seatalk_demo_owner_email="owner@npt.sg")
                app.config["GOOGLE_CREDENTIAL_STORE"] = SimpleNamespace(saved=[], save=lambda **kwargs: app.config["GOOGLE_CREDENTIAL_STORE"].saved.append(kwargs))
                web_module._persist_owner_google_credentials(owner_settings)
                self.assertEqual(app.config["GOOGLE_CREDENTIAL_STORE"].saved[0]["owner_email"], "owner@npt.sg")

                with patch("bpmis_jira_tool.web._load_current_user_config", return_value={"bpmis_api_access_token": "user-token"}), patch(
                    "bpmis_jira_tool.web.build_bpmis_client", return_value="client"
                ) as build_client:
                    self.assertEqual(web_module._build_bpmis_client_for_current_user(settings), "client")
                    self.assertEqual(build_client.call_args.kwargs["access_token"], "user-token")
                with patch("bpmis_jira_tool.web.get_google_credentials", return_value=SimpleNamespace()), patch(
                    "bpmis_jira_tool.web.GmailDashboardService", return_value="gmail"
                ):
                    self.assertEqual(web_module._build_gmail_dashboard_service(), "gmail")
                app.config["TEAM_DASHBOARD_CONFIG_STORE"] = SimpleNamespace(load=lambda: (_ for _ in ()).throw(RuntimeError("config down")))
                with patch("bpmis_jira_tool.web.get_google_credentials", return_value=SimpleNamespace()), patch(
                    "bpmis_jira_tool.web.GmailDashboardService", return_value="gmail"
                ):
                    self.assertEqual(web_module._build_gmail_dashboard_service(), "gmail")
                self.assertIsNotNone(web_module._build_prd_review_service(settings))
                with patch("bpmis_jira_tool.web._build_seatalk_dashboard_service", return_value=SimpleNamespace()):
                    self.assertIsNotNone(web_module._build_monthly_report_service(settings))
                self.assertIsNotNone(web_module._build_meeting_processing_service(settings))
                unknown_profile_store = SimpleNamespace(load_team_profiles=lambda: {"UNKNOWN": {"ready": True}})
                self.assertIn("AF", web_module._load_effective_team_profiles(unknown_profile_store))

                remote_settings = replace(settings, local_agent_base_url="http://127.0.0.1:7007", local_agent_hmac_secret="secret", local_agent_bpmis_enabled=True, local_agent_seatalk_enabled=True)
                with patch("bpmis_jira_tool.web._local_agent_mode_enabled", return_value=True), patch(
                    "bpmis_jira_tool.web._build_local_agent_client", return_value=fake_remote
                ):
                    self.assertTrue(web_module._seatalk_dashboard_is_configured(remote_settings))
                    self.assertIsNotNone(web_module._build_seatalk_dashboard_service(remote_settings))
                    self.assertEqual(web_module._save_user_config_for_identity(remote_settings, {"config_key": "u"}, {"x": 1})["remote_saved"]["user_key"], "u")
                    web_module._migrate_user_config(remote_settings, "old", "new")
                    self.assertEqual(web_module._save_team_profile(remote_settings, object(), "AF", {"ready": True})["remote_profile"]["team_key"], "AF")
                    app.config["SETTINGS"] = remote_settings
                    self.assertIsNotNone(web_module._get_bpmis_project_store())
                    app.config["SETTINGS"] = settings
                    self.assertIsNotNone(web_module._get_seatalk_todo_store(remote_settings))
                with patch("bpmis_jira_tool.web._local_agent_mode_enabled", return_value=True), patch(
                    "bpmis_jira_tool.web._build_local_agent_client", return_value=SimpleNamespace(get_health=lambda: (_ for _ in ()).throw(ToolError("down")))
                ):
                    self.assertFalse(web_module._seatalk_dashboard_is_configured(remote_settings))
                with self.assertRaises(ToolError):
                    web_module._save_user_config_for_identity(settings, {}, {})
                self.assertIsNone(web_module._load_user_config_for_identity(settings, {}))
                web_module._migrate_user_config(settings, "old-local", "new-local")
                self.assertEqual(web_module._normalize_post_google_login_redirect_target(settings, "/other"), "/other")
                with patch("bpmis_jira_tool.web._can_access_source_code_qa", return_value=False), patch(
                    "bpmis_jira_tool.web._can_access_team_dashboard_version_plan", return_value=True
                ):
                    self.assertEqual(web_module._cloud_home_default_post_login_redirect(replace(settings, cloud_home_enabled=True), {"email": "af@npt.sg"}), "/version-plan")
                with patch("bpmis_jira_tool.web._can_access_source_code_qa", return_value=False), patch(
                    "bpmis_jira_tool.web._can_access_team_dashboard_version_plan", return_value=False
                ):
                    self.assertEqual(web_module._cloud_home_default_post_login_redirect(replace(settings, cloud_home_enabled=True), {"email": "af@npt.sg"}), "")
                self.assertEqual(web_module._uat_local_agent_loopback_base_url(), "http://127.0.0.1:7008")
                with patch.dict(os.environ, {"UAT_LOCAL_AGENT_LOOPBACK_BASE_URL": "http://uat-agent/"}, clear=False):
                    self.assertEqual(web_module._uat_local_agent_loopback_base_url(), "http://uat-agent")
                self.assertIsNotNone(web_module._build_local_agent_client(remote_settings))
                with patch("bpmis_jira_tool.web.get_google_credentials", return_value=SimpleNamespace()), patch(
                    "bpmis_jira_tool.web.GoogleCalendarMeetingService", return_value="calendar"
                ):
                    self.assertEqual(web_module._build_calendar_meeting_service(), "calendar")
                app.config["GOOGLE_CREDENTIAL_STORE"] = None
                web_module._persist_owner_google_credentials(replace(settings, meeting_recorder_owner_email="owner@npt.sg"))

    def test_team_dashboard_link_biz_and_manday_helpers(self):
        class FakeBPMISClient:
            def __init__(self):
                self.calls = []

            def list_actual_mandays_for_projects(self, project_ids):
                return {project_id: float(index + 1) for index, project_id in enumerate(project_ids)}

            def list_biz_projects_for_pm_emails(self, emails):
                return [
                    {
                        "bpmis_id": "B1",
                        "project_name": "Alpha Project",
                        "status": "Pending Review",
                        "matched_pm_emails": emails,
                        "regional_pm_pic": emails[0] if emails else "",
                    }
                ]

            def search_versions(self, prefix):
                return [{"id": "101", "fullName": f"{prefix}_Candidate"}, "bad"]

            def list_issues_for_version(self, version_id):
                return [{"parentIds": [{"id": "B2"}]}, {"parentIssueId": "B2"}]

            def get_issue_detail(self, issue_id):
                return {"summary": "Version Parent", "market": "SG"}

        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            with app.app_context():
                with self.assertRaises(ToolError):
                    web_module._validate_team_profile_setup({}, team_profiles={"AF": {"label": "Anti-Fraud", "ready": True}})
                with self.assertRaises(ToolError):
                    web_module._validate_team_profile_setup({"pm_team": "XX"}, team_profiles={"AF": {"label": "Anti-Fraud", "ready": True}})
                with self.assertRaises(ToolError):
                    web_module._validate_team_profile_setup({"pm_team": "AF"}, team_profiles={"AF": {"label": "Anti-Fraud", "ready": False}})
                notice = web_module._build_sync_notice(
                    [
                        SimpleNamespace(status="updated", issue_id="B1", message="ok"),
                        SimpleNamespace(status="error", issue_id="", message="bad"),
                    ]
                )
                self.assertEqual(notice["tone"], "warning")
                skipped_notice = web_module._build_sync_notice([SimpleNamespace(status="skipped", issue_id="S1", message="skip")])
                self.assertEqual(skipped_notice["details"], ["S1: skip"])
                payload = {
                    "under_prd": [{"bpmis_id": "B1"}, {"bpmis_id": ""}],
                    "pending_live": [{"bpmis_id": "B2"}],
                }
                web_module._hydrate_team_dashboard_actual_mandays(FakeBPMISClient(), payload)
                self.assertEqual(payload["under_prd"][0]["actual_mandays"], 1.0)
                self.assertEqual(payload["pending_live"][0]["actual_mandays"], 2.0)
                web_module._hydrate_team_dashboard_actual_mandays(object(), payload)
                web_module._hydrate_team_dashboard_actual_mandays(SimpleNamespace(list_actual_mandays_for_projects=lambda _ids: (_ for _ in ()).throw(RuntimeError("manday failed"))), payload)
                self.assertIsNone(web_module._team_dashboard_parse_timestamp(""))
                self.assertFalse(web_module._team_dashboard_actual_mandays_entry_is_fresh({"value": 1, "cached_at": "not-a-date"}))
                self.assertIsNone(web_module._cached_team_dashboard_task_payload({"task_cache": {"version": 0}}, "AF", ["pm@npt.sg"]))
                self.assertIsNone(
                    web_module._cached_team_dashboard_task_payload(
                        {"task_cache": {"version": web_module.TEAM_DASHBOARD_TASK_CACHE_VERSION, "teams": {"AF": {"email_signature": "other"}}}},
                        "AF",
                        ["pm@npt.sg"],
                    )
                )
                save_store = SimpleNamespace(saved=[], load=lambda: {}, save=lambda config: save_store.saved.append(config))
                web_module._store_team_dashboard_task_payload(save_store, "", ["pm@npt.sg"], {})
                web_module._store_team_dashboard_task_payload(save_store, "AF", ["pm@npt.sg"], {"error": "bad"})

                team_payloads = [
                    {
                        "team_key": "AF",
                        "under_prd": [
                            {"bpmis_id": "B1", "project_name": "Alpha Project", "status": "Pending Review", "matched_pm_emails": ["pm@npt.sg"], "jira_tickets": []},
                            {"project_name": "No BPMIS", "jira_tickets": [{"jira_id": "AF-1", "jira_title": "Alpha work", "pm_email": "pm@npt.sg"}]},
                            "bad-project",
                            {"project_name": "No ID Ticket", "jira_tickets": [{"jira_id": "", "jira_title": "No id"}, {"jira_id": "AF-1", "jira_title": "Duplicate"}]},
                        ],
                        "pending_live": [{"bpmis_id": "B3", "project_name": "No Tickets", "status": "UAT", "jira_tickets": []}],
                    },
                ]
                rows = web_module._build_team_dashboard_link_biz_project_rows(team_payloads)
                self.assertEqual(rows[0]["suggested_bpmis_id"], "B1")
                duplicate_group = web_module._build_team_dashboard_task_group(
                    "AF",
                    "Anti-Fraud",
                    ["pm@npt.sg"],
                    [
                        {"jira_id": "AF-1", "jira_status": "Waiting", "jira_title": "One"},
                        {"jira_id": "AF-1", "jira_status": "Waiting", "jira_title": "Duplicate"},
                    ],
                )
                self.assertEqual(duplicate_group["under_prd"][0]["task_count"], 1)
                self.assertEqual(web_module._team_dashboard_zero_jira_biz_project_options(team_payloads)[0]["bpmis_id"], "B1")
                self.assertEqual(web_module._team_dashboard_link_biz_filter_candidates_for_pm([], ""), [])
                self.assertEqual(web_module._team_dashboard_link_biz_project_options([{"bpmis_id": "", "project_name": ""}]), [])
                with patch.dict(os.environ, {"TEAM_DASHBOARD_LINK_BIZ_KEYWORD_FALLBACK_SCORE": "bad"}, clear=False):
                    self.assertEqual(web_module._team_dashboard_link_biz_keyword_fallback_threshold(), 0.78)
                with patch.dict(os.environ, {"TEAM_DASHBOARD_LINK_BIZ_KEYWORD_FALLBACK_SCORE": "2"}, clear=False):
                    self.assertEqual(web_module._team_dashboard_link_biz_keyword_fallback_threshold(), 1.0)
                self.assertEqual(web_module._team_dashboard_link_biz_keywords("[Feature] jira support Alpha Beta"), "alpha beta")

                options = web_module._team_dashboard_link_biz_version_project_options(FakeBPMISClient(), "1.2.34")
                self.assertEqual(options[0]["bpmis_id"], "B2")
                self.assertEqual(
                    web_module._team_dashboard_link_biz_version_project_options(SimpleNamespace(search_versions=lambda _prefix: [{"id": "", "fullName": "1.2.34 Empty"}]), "1.2.34"),
                    [],
                )
                self.assertEqual(web_module._team_dashboard_link_biz_version_project_options(object(), "1.2.34"), [])
                self.assertEqual(web_module._team_dashboard_link_biz_project_option_from_parent(object(), "") , {})
                self.assertEqual(web_module._team_dashboard_link_biz_project_option_from_parent(SimpleNamespace(get_issue_detail=lambda _id: (_ for _ in ()).throw(RuntimeError("bad"))), "B9")["project_name"], "BPMIS B9")
                fallback_biz_client = SimpleNamespace(list_biz_projects_for_pm_email=lambda email: [{"bpmis_id": "B4", "matched_pm_emails": [email]}])
                self.assertEqual(web_module._team_dashboard_biz_projects_for_emails(fallback_biz_client, ["", "pm@npt.sg"])[0]["bpmis_id"], "B4")

    def test_sent_monthly_report_history_refresh_edges(self):
        class FakeGmailService:
            def __init__(self):
                self.fetches = []

            def _list_message_ids(self, *, query, max_messages):
                self.fetches.append((query, max_messages))
                return ["sent-1", "sent-1", "sent-2", "sent-3"]

            def _fetch_message_full(self, message_id):
                subjects = {
                    "sent-1": "[Banking] Product Update (1 May - 31 May) - Anti-Fraud, Credit Risk & Ops Risk",
                    "sent-2": "Other update",
                    "sent-3": "[Banking] Product Update (1 May - 31 May) - Anti-Fraud, Credit Risk & Ops Risk",
                }
                froms = {
                    "sent-1": "Owner <owner@npt.sg>",
                    "sent-2": "Owner <owner@npt.sg>",
                    "sent-3": "Other <other@npt.sg>",
                }
                return SimpleNamespace(
                    message_id=message_id,
                    headers={"from": froms[message_id], "to": "team@npt.sg", "subject": subjects[message_id]},
                    body_text=f"body {message_id}",
                    internal_date=web_module.datetime(2026, 5, 24, tzinfo=web_module.timezone.utc),
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            with app.app_context(), patch("bpmis_jira_tool.web._google_credentials_have_scopes", return_value=False):
                with self.assertRaises(ConfigError):
                    web_module._refresh_monthly_report_history_from_gmail(app.config["SETTINGS"])

            with app.app_context(), patch("bpmis_jira_tool.web._google_credentials_have_scopes", return_value=True), patch(
                "bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=FakeGmailService()
            ), patch("bpmis_jira_tool.web._current_google_email", return_value="owner@npt.sg"):
                result = web_module._refresh_monthly_report_history_from_gmail(app.config["SETTINGS"])
            self.assertEqual(result["scanned"], 3)
            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["report_count"], 1)
            self.assertEqual(len(result["items"]), 1)

    def test_team_dashboard_jira_fallback_and_background_job_edges(self):
        class FallbackClient:
            def __init__(self, *, bulk=None, bulk_error=None, row_error=False):
                self.bulk = bulk
                self.bulk_error = bulk_error
                self.row_error = row_error
                self.stats = {}

            def list_jira_tasks_for_projects_created_by_emails(self, project_ids, emails):
                if self.bulk_error:
                    raise self.bulk_error
                return self.bulk

            def list_jira_tasks_for_project_created_by_email(self, bpmis_id, email):
                if self.row_error:
                    raise RuntimeError("row lookup failed")
                return [
                    "bad",
                    {"jira_id": f"{bpmis_id}-1", "jira_title": "Task", "pm_email": email, "jira_status": "In Progress"},
                    {"jira_id": f"{bpmis_id}-1", "jira_title": "Duplicate", "pm_email": email, "jira_status": "In Progress"},
                    {"jira_id": f"{bpmis_id}-done", "jira_title": "Done", "pm_email": email, "jira_status": "Done"},
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            project = {"bpmis_id": "B1", "project_name": "Alpha", "matched_pm_emails": [], "regional_pm_pic": "PM@NPT.SG"}
            self.assertEqual(web_module._team_dashboard_project_fallback_emails(project), ["pm@npt.sg"])
            with app.app_context():
                tickets = web_module._team_dashboard_project_fallback_jira_tasks(FallbackClient(bulk={}), project)
            self.assertEqual([ticket["jira_id"] for ticket in tickets], ["B1-1"])

            with app.app_context():
                team_payload = {"under_prd": [dict(project)], "pending_live": [{"bpmis_id": "B2", "matched_pm_emails": ["pm@npt.sg"]}]}
                web_module._backfill_team_dashboard_empty_project_jira_tasks(FallbackClient(bulk={"B1": [{"jira_id": "B1-2", "pm_email": "pm@npt.sg"}], "B2": []}), team_payload)
                no_email_payload = {"under_prd": [{"bpmis_id": "B-no-email", "matched_pm_emails": []}], "pending_live": []}
                web_module._backfill_team_dashboard_empty_project_jira_tasks(FallbackClient(bulk={}), no_email_payload)
            self.assertEqual(team_payload["under_prd"][0]["task_count"], 1)

            with app.app_context():
                team_payload = {"under_prd": [dict(project)], "pending_live": []}
                web_module._backfill_team_dashboard_empty_project_jira_tasks(FallbackClient(bulk_error=RuntimeError("bulk down")), team_payload)
            self.assertEqual(team_payload["under_prd"][0]["task_count"], 1)

            with app.app_context():
                all_payloads = [{"team_key": "AF", "under_prd": [dict(project)], "pending_live": []}, "bad"]
                web_module._backfill_all_team_dashboard_empty_project_jira_tasks(FallbackClient(bulk={"B1": [{"jira_id": "B1-3", "pm_email": "pm@npt.sg"}]}), all_payloads)
                no_email_all_payloads = [{"team_key": "AF", "under_prd": [{"bpmis_id": "B-no-email", "matched_pm_emails": []}], "pending_live": []}]
                web_module._backfill_all_team_dashboard_empty_project_jira_tasks(FallbackClient(bulk={}), no_email_all_payloads)
            self.assertEqual(all_payloads[0]["under_prd"][0]["task_count"], 1)

            with app.app_context():
                all_payloads = [{"team_key": "AF", "under_prd": [dict(project)], "pending_live": []}]
                web_module._backfill_all_team_dashboard_empty_project_jira_tasks(FallbackClient(bulk_error=RuntimeError("bulk down")), all_payloads)
            self.assertEqual(all_payloads[0]["under_prd"][0]["task_count"], 1)

            class SingleOnlyClient:
                request_stats = {}

                def list_jira_tasks_for_project_created_by_email(self, bpmis_id, email):
                    return [{"jira_id": f"{bpmis_id}-single", "jira_title": "Task", "pm_email": email}]

            with app.app_context():
                no_candidate_payloads = [{"team_key": "AF", "under_prd": "bad", "pending_live": [{"bpmis_id": "", "matched_pm_emails": []}]}]
                web_module._backfill_all_team_dashboard_empty_project_jira_tasks(SingleOnlyClient(), no_candidate_payloads)
                fallback_payloads = [{"team_key": "AF", "under_prd": [dict(project)], "pending_live": []}]
                web_module._backfill_all_team_dashboard_empty_project_jira_tasks(SingleOnlyClient(), fallback_payloads)
                list_payloads = [{"team_key": "AF", "under_prd": [dict(project)], "pending_live": []}]
                web_module._backfill_all_team_dashboard_empty_project_jira_tasks(FallbackClient(bulk=[]), list_payloads)
            self.assertEqual(fallback_payloads[0]["under_prd"][0]["task_count"], 1)

            filtered = web_module._normalize_team_dashboard_project_fallback_rows(
                [
                    "bad",
                    {"jira_id": "J-1", "jira_title": "One", "pm_email": "other@npt.sg"},
                    {"jira_id": "J-2", "jira_title": "Two", "pm_email": "pm@npt.sg", "jira_status": "Done"},
                    {"jira_id": "J-3", "jira_title": "Three", "pm_email": "pm@npt.sg"},
                    {"jira_id": "J-3", "jira_title": "Three duplicate", "pm_email": "pm@npt.sg"},
                ],
                project,
                ["pm@npt.sg"],
            )
            self.assertEqual([row["jira_id"] for row in filtered], ["J-3"])
            with app.app_context():
                self.assertEqual(web_module._team_dashboard_project_fallback_jira_tasks(FallbackClient(row_error=True), project), [])
                self.assertEqual(web_module._team_dashboard_project_fallback_jira_tasks(FallbackClient(), {"bpmis_id": "B1"}), [])
                web_module._backfill_team_dashboard_empty_project_jira_tasks(SingleOnlyClient(), {"under_prd": "bad", "pending_live": [{"bpmis_id": "", "matched_pm_emails": []}]})
                payload = {"pending_live": "bad"}
                web_module._remove_team_dashboard_zero_jira_pending_live_projects(payload)
                self.assertEqual(payload["pending_live"], "bad")

            app.config["JOB_STORE"] = _FakeJobStore()
            with app.app_context(), patch("bpmis_jira_tool.web._build_portal_project_sync_service", return_value=SimpleNamespace(sync_projects=lambda **_kwargs: [{"row": 1}])), patch(
                "bpmis_jira_tool.web._serialize_results", return_value=[{"ok": True}]
            ), patch("bpmis_jira_tool.web._build_sync_notice", return_value={"message": "done"}):
                web_module._run_background_job(app, "job-sync", "sync-bpmis-projects", app.config["SETTINGS"], {"pm_team": "AF", "_user_key": "u", "sync_pm_email": "pm@npt.sg"})
            self.assertEqual(app.config["JOB_STORE"].completed[-1][0][0], "job-sync")

            with app.app_context():
                web_module._run_background_job(app, "job-unsupported", "unknown", app.config["SETTINGS"], {})
            self.assertEqual(app.config["JOB_STORE"].failed[-1][0][0], "job-unsupported")

            with app.app_context(), patch("bpmis_jira_tool.web._build_portal_project_sync_service", side_effect=RuntimeError("sync crashed")):
                web_module._run_background_job(app, "job-crash", "sync-bpmis-projects", app.config["SETTINGS"], {})
            self.assertEqual(app.config["JOB_STORE"].failed[-1][0][0], "job-crash")

    def test_team_dashboard_payloads_biz_filter_and_manday_cache_edges(self):
        class FakeConfigStore:
            def __init__(self, config):
                self.config = config
                self.saved = []

            def load(self):
                return self.config

            def save(self, config):
                self.config = config
                self.saved.append(config)

        class FakeClient:
            def __init__(self, *, bulk_error=False):
                self.bulk_error = bulk_error

            def list_jira_tasks_created_by_emails(self, emails, **_kwargs):
                return [{"jira_id": "AF-1", "jira_title": "Task", "pm_email": emails[0] if emails else ""}]

            def list_biz_projects_for_pm_emails(self, emails):
                if self.bulk_error:
                    raise RuntimeError("bulk biz down")
                if not emails:
                    return []
                return [
                    {"bpmis_id": "B1", "project_name": "Alpha", "status": "Pending Review", "regional_pm_pic": emails[0], "matched_pm_emails": []},
                    "bad",
                ]

            def list_biz_projects_for_pm_email(self, email):
                if email == "fail@npt.sg":
                    raise RuntimeError("single biz down")
                return [{"bpmis_id": f"B-{email}", "project_name": "Single", "status": "UAT", "matched_pm_emails": [email]}]

            def list_actual_mandays_for_projects(self, project_ids):
                return {project_id: "2.5" for project_id in project_ids}

        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            config = {"teams": {"AF": {"member_emails": ["pm@npt.sg"]}}, "key_project_overrides": {}}
            with app.app_context(), patch("bpmis_jira_tool.web._team_dashboard_jira_max_pages", return_value=1), patch(
                "bpmis_jira_tool.web._team_dashboard_jira_release_after", return_value=""
            ):
                payloads = web_module._load_team_dashboard_link_biz_project_payloads(app.config["SETTINGS"], config, FakeClient())
            self.assertTrue(payloads)
            self.assertEqual(payloads[0]["team_key"], "AF")

            with app.app_context():
                bulk_projects = web_module._team_dashboard_biz_projects_for_emails(FakeClient(), ["pm@npt.sg"])
                fallback_projects = web_module._team_dashboard_biz_projects_for_emails(FakeClient(bulk_error=True), ["pm@npt.sg", "fail@npt.sg"])
            self.assertEqual(bulk_projects[0]["matched_pm_emails"], ["pm@npt.sg"])
            self.assertTrue(fallback_projects)
            self.assertEqual(
                web_module._filter_team_dashboard_biz_projects_for_emails([bulk_projects[0], "bad"], ["pm@npt.sg"])[0]["matched_pm_emails"],
                ["pm@npt.sg"],
            )

            with patch.dict(os.environ, {"TEAM_DASHBOARD_ACTUAL_MANDAYS_CACHE_TTL_SECONDS": "bad", "TEAM_DASHBOARD_JIRA_MAX_PAGES": "bad"}, clear=False):
                self.assertEqual(web_module._team_dashboard_actual_mandays_cache_ttl_seconds(), 86400)
                self.assertEqual(web_module._team_dashboard_jira_max_pages(), 5)
            with patch.dict(os.environ, {"TEAM_DASHBOARD_ACTUAL_MANDAYS_CACHE_TTL_SECONDS": "0", "TEAM_DASHBOARD_JIRA_MAX_PAGES": "0"}, clear=False):
                self.assertFalse(web_module._team_dashboard_actual_mandays_entry_is_fresh({"value": 1, "cached_at": web_module._team_dashboard_timestamp()}))
                self.assertEqual(web_module._team_dashboard_jira_max_pages(), 1)
            self.assertEqual(web_module._team_dashboard_parse_timestamp("bad"), None)
            self.assertIsInstance(web_module._team_dashboard_parse_timestamp("2026-05-24T00:00:00"), float)
            self.assertEqual(web_module._team_dashboard_manday_value("2.0"), 2)
            self.assertEqual(web_module._team_dashboard_manday_value("2.5"), 2.5)
            self.assertEqual(web_module._team_dashboard_manday_value("bad"), "")

            self.assertEqual(web_module._team_dashboard_project_entries(["bad", {"under_prd": [{"bpmis_id": "B0"}]}])[0]["bpmis_id"], "B0")
            team_payloads = [{"team_key": "AF", "under_prd": [{"bpmis_id": "B1"}, {"bpmis_id": "B2"}], "pending_live": []}]
            stale_config = {"actual_mandays_cache": {"projects": {"B1": {"value": "1.0", "cached_at": "2000-01-01T00:00:00Z"}}}}
            pending = web_module._apply_team_dashboard_actual_mandays_cache(stale_config, team_payloads)
            self.assertEqual(pending, ["B1", "B2"])
            self.assertTrue(team_payloads[0]["under_prd"][0]["actual_mandays_stale"])

            store = FakeConfigStore({"task_cache": {"teams": {"AF": team_payloads[0]}}})
            web_module._store_team_dashboard_actual_mandays_results(store, {"B1": "3.0", "": 1, "B3": "bad"})
            self.assertEqual(store.config["actual_mandays_cache"]["projects"]["B1"]["value"], 3)
            store_with_bad_cached_team = FakeConfigStore({"task_cache": {"teams": {"bad": "not-a-dict"}}})
            web_module._store_team_dashboard_actual_mandays_results(store_with_bad_cached_team, {"B1": "4"})
            web_module._store_team_dashboard_actual_mandays_results(store, {"B9": "bad"})
            self.assertEqual(len(store.saved), 1)

            web_module._team_dashboard_actual_mandays_running.clear()
            with app.app_context(), patch("bpmis_jira_tool.web.threading.Thread", _ImmediateThread):
                started = web_module._start_team_dashboard_actual_mandays_refresh(app.config["SETTINGS"], store, FakeClient(), ["B2", "B2", ""])
            self.assertEqual(started, ["B2"])
            self.assertFalse(web_module._team_dashboard_actual_mandays_running)

            web_module._team_dashboard_actual_mandays_running.clear()
            web_module._team_dashboard_actual_mandays_running.add("B2")
            with app.app_context(), patch("bpmis_jira_tool.web.threading.Thread", _ImmediateThread):
                self.assertEqual(web_module._start_team_dashboard_actual_mandays_refresh(app.config["SETTINGS"], store, FakeClient(), ["B2"]), [])
            web_module._team_dashboard_actual_mandays_running.clear()

            with app.app_context(), patch("bpmis_jira_tool.web.threading.Thread", _ImmediateThread):
                self.assertEqual(
                    web_module._start_team_dashboard_actual_mandays_refresh(
                        app.config["SETTINGS"],
                        store,
                        SimpleNamespace(list_actual_mandays_for_projects=lambda _ids: (_ for _ in ()).throw(RuntimeError("mandays down"))),
                        ["B4"],
                    ),
                    ["B4"],
                )
            with patch("bpmis_jira_tool.web.threading.Thread", _ImmediateThread):
                self.assertEqual(web_module._start_team_dashboard_actual_mandays_refresh(app.config["SETTINGS"], store, FakeClient(), ["B5"]), ["B5"])

            with app.app_context(), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", side_effect=RuntimeError("no config")):
                queued = web_module._queue_team_dashboard_actual_mandays_refresh(app.config["SETTINGS"], store, stale_config, team_payloads)
            self.assertEqual(queued, [])

    def test_team_dashboard_linking_helper_edges(self):
        class LinkClient:
            request_stats = {"api_call_count": 2}
            request_timings = {"a": "1.25", "bad": "x"}

            def __init__(self, *, search_error=False, issue_error=False):
                self.search_error = search_error
                self.issue_error = issue_error

            def list_jira_tasks_created_by_emails(self, emails, **_kwargs):
                return [
                    "bad",
                    {"jira_id": "HAS-PARENT", "jira_status": "In Progress", "parent_project": {"bpmis_id": "B0"}, "pm_email": emails[0] if emails else ""},
                    {"jira_id": "DONE-1", "jira_status": "Done", "pm_email": emails[0] if emails else ""},
                    {"jira_id": "", "jira_status": "In Progress", "pm_email": emails[0] if emails else ""},
                    {"jira_id": "AF-1", "jira_title": "Alpha build", "jira_status": "In Progress", "pm_email": emails[0] if emails else ""},
                    {"jira_id": "AF-1", "jira_title": "Duplicate", "jira_status": "In Progress", "pm_email": emails[0] if emails else ""},
                ]

            def list_biz_projects_for_pm_emails(self, emails):
                return [{"bpmis_id": "B1", "project_name": "Alpha build", "status": "Pending Review", "matched_pm_emails": emails}]

            def search_versions(self, prefix):
                if self.search_error:
                    raise RuntimeError("version down")
                return [{"id": "V1", "fullName": f"{prefix}_Release"}, {"id": "", "fullName": prefix}, "bad"]

            def list_issues_for_version(self, version_id):
                if self.issue_error:
                    raise RuntimeError("issue down")
                return [{"parentIssueId": "B2"}, {"raw_jira": {"parentIds": [{"id": "B2"}, {"issueId": "B3"}]}}, "bad"]

            def get_issue_detail(self, issue_id):
                return {"project_name": f"Project {issue_id}", "status": "Pending Review", "market": "SG"}

        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            settings = app.config["SETTINGS"]
            config = {"teams": {"AF": {"member_emails": ["pm@npt.sg"]}}}
            with app.app_context(), patch("bpmis_jira_tool.web._build_bpmis_client_for_current_user", return_value=LinkClient()), patch(
                "bpmis_jira_tool.web._team_dashboard_jira_release_after", return_value=""
            ):
                rows = web_module._load_team_dashboard_link_biz_jira_rows(settings, config)
                suggestions = web_module._suggest_team_dashboard_link_biz_project_rows(
                    settings,
                    config,
                    [
                        "bad",
                        {"jira_id": "", "jira_title": "No id", "reporter_email": "pm@npt.sg"},
                        {"jira_id": "AF-1", "jira_title": "Alpha build", "reporter_email": "pm@npt.sg", "team_key": "AF"},
                        {"jira_id": "AF-2", "jira_title": "Sync AF Productization 1.2.34", "reporter_email": "pm@npt.sg", "team_key": "AF"},
                    ],
                    team_payloads=[{"team_key": "AF", "under_prd": [{"bpmis_id": "B1", "project_name": "Alpha build", "status": "Pending Review", "matched_pm_emails": ["pm@npt.sg"]}], "pending_live": []}, "bad"],
                )
            self.assertEqual([row["jira_id"] for row in rows], ["AF-1"])
            self.assertEqual(suggestions["matched_count"], 2)
            self.assertEqual(suggestions["version_search_count"], 1)
            self.assertEqual(web_module._team_dashboard_combined_fetch_stats(None, LinkClient(), LinkClient())["api_call_count"], 4)
            self.assertEqual(web_module._team_dashboard_combined_fetch_stats(LinkClient(), LinkClient())["api_call_count"], 4)
            client = LinkClient()
            self.assertEqual(web_module._team_dashboard_combined_fetch_stats(client, client)["api_call_count"], 2)
            self.assertEqual(web_module._team_dashboard_combined_request_timings(None, client, client), {"a": 1.25})
            self.assertEqual(web_module._filter_team_dashboard_tasks_for_emails([], []), [])
            self.assertEqual(web_module._filter_team_dashboard_tasks_for_emails(["bad", {"jira_id": "AF-1", "pm_email": "pm@npt.sg"}], ["pm@npt.sg"])[0]["jira_id"], "AF-1")
            self.assertEqual(web_module._filter_team_dashboard_biz_projects_for_emails([], []), [])
            self.assertEqual(
                web_module._filter_team_dashboard_biz_projects_for_emails(
                    [{"bpmis_id": "B1", "project_name": "Alpha", "status": "Pending Review", "regional_pm_pic": "pm@npt.sg"}],
                    ["pm@npt.sg"],
                )[0]["matched_pm_emails"],
                ["pm@npt.sg"],
            )
            fallback_only_client = SimpleNamespace(list_biz_projects_for_pm_email=lambda email: [{"bpmis_id": f"B-{email}", "matched_pm_emails": [email]}])
            with app.app_context(), patch("bpmis_jira_tool.web._normalize_team_dashboard_emails", side_effect=lambda value: value):
                self.assertEqual(web_module._team_dashboard_biz_projects_for_emails(fallback_only_client, ["", "pm@npt.sg"])[0]["bpmis_id"], "B-pm@npt.sg")
            with patch.dict(os.environ, {"TEAM_DASHBOARD_JIRA_RELEASE_AFTER": ""}, clear=False):
                self.assertRegex(web_module._team_dashboard_jira_release_after(), r"^\d{4}-\d{2}-\d{2}$")
            self.assertEqual(web_module._merge_team_dashboard_biz_projects([], [{"bpmis_id": "", "project_name": "No ID"}]), [])
            self.assertEqual(web_module._team_dashboard_link_biz_candidate_projects_from_payloads(["bad", {"under_prd": [{"bpmis_id": "B1"}], "pending_live": []}])[0]["match_source"], "team")
            self.assertEqual(web_module._team_dashboard_link_biz_filter_candidates_for_pm(["bad"], "pm@npt.sg"), [])
            self.assertEqual(
                web_module._team_dashboard_link_biz_filter_candidates_for_pm(
                    [{"bpmis_id": "B1", "project_name": "Alpha", "status": "Pending Review", "regional_pm_pic": "pm@npt.sg"}],
                    "pm@npt.sg",
                )[0]["bpmis_id"],
                "B1",
            )
            self.assertEqual(web_module._team_dashboard_link_biz_candidate_projects_by_pm(LinkClient(), []), {})
            self.assertEqual(web_module._team_dashboard_link_biz_candidate_projects_by_pm(LinkClient(), ["pm@npt.sg"])["pm@npt.sg"][0]["bpmis_id"], "B1")
            self.assertEqual(web_module._team_dashboard_link_biz_project_options([{"status": "Pending Review"}]), [])
            self.assertEqual(web_module._team_dashboard_zero_jira_biz_project_options(["bad", {"under_prd": ["bad"], "pending_live": []}]), [])
            self.assertEqual(web_module._dedupe_team_dashboard_candidate_projects([{"bpmis_id": ""}, {"bpmis_id": "B1"}, {"bpmis_id": "B1"}])[0]["bpmis_id"], "B1")
            self.assertEqual(web_module._team_dashboard_link_biz_version_project_options(LinkClient(search_error=True), "1.2.34"), [])
            self.assertEqual(web_module._team_dashboard_link_biz_version_project_options(LinkClient(issue_error=True), "1.2.34"), [])
            self.assertTrue(web_module._team_dashboard_link_biz_version_project_options(LinkClient(), "1.2.34"))
            self.assertEqual(web_module._extract_parent_issue_ids_from_any("bad"), set())
            self.assertEqual(web_module._extract_parent_issue_ids_from_any({"raw_jira": {"parentIssueId": "B9"}}), {"B9"})
            self.assertEqual(web_module._suggest_team_dashboard_biz_project({"jira_title": "Alpha"}, [{"bpmis_id": "", "project_name": ""}]), {})
            with patch.dict(os.environ, {"TEAM_DASHBOARD_JIRA_RELEASE_AFTER": "2026-01-01", "TEAM_DASHBOARD_MONTHLY_REPORT_JIRA_RELEASE_AFTER": "2026-02-01"}, clear=False):
                self.assertEqual(web_module._team_dashboard_jira_release_after(), "2026-01-01")
                self.assertEqual(web_module._team_dashboard_monthly_report_jira_release_after(), "2026-02-01")
            self.assertEqual(web_module._team_dashboard_link_items([{"url": "https://a"}, {"label": "https://b"}, "https://a", ""]), [{"label": "https://a", "url": "https://a"}, {"label": "https://b", "url": "https://b"}])
            self.assertEqual(web_module._team_dashboard_link_items("https://a,\nhttps://b"), [{"label": "https://a", "url": "https://a"}, {"label": "https://b", "url": "https://b"}])

    def test_productization_helper_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            settings = app.config["SETTINGS"]
            self.assertEqual(web_module._flatten_productization_component_values(""), [])
            self.assertEqual(web_module._flatten_productization_component_values(123), ["123"])
            self.assertEqual(web_module._normalize_productization_ticket_url("AF-123"), f"{web_module._jira_browse_base_url()}AF-123")
            self.assertEqual(web_module._normalize_productization_ticket_url("plain"), "plain")
            row = {"fields": {"component": [{"name": "Anti-Fraud"}]}, "row": {"pm": {"displayName": "PM"}}}
            self.assertEqual(web_module._extract_first_value(row, "component"), [{"name": "Anti-Fraud"}])
            self.assertEqual(web_module._extract_person_display([{"displayName": "A"}, {"emailAddress": "b@npt.sg"}]), "A, b@npt.sg")
            self.assertEqual(web_module._coerce_display_text(7), "7")
            self.assertEqual(web_module._flatten_links({"url": ["https://a", {"href": "https://b"}]}), ["https://a", "https://b"])
            self.assertEqual(web_module._flatten_links("not a link"), [])
            normalized = [{"jira_ticket_number": "AF-1", "detailed_feature": "", "feature_summary": "Summary"}]
            raw_rows = [{"description": "Description"}]
            with patch("bpmis_jira_tool.web._generate_productization_detailed_features_with_codex", return_value=[{"jira_ticket_number": "AF-1", "detailed_feature": "Generated"}]):
                result = web_module._apply_codex_productization_detailed_features(normalized, raw_rows, settings=settings)
            self.assertEqual(result["codex_generated_count"], 1)
            self.assertEqual(normalized[0]["detailed_feature"], "Generated")
            with patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=False), patch(
                "bpmis_jira_tool.web._generate_productization_detailed_features_with_local_codex", return_value=[{"jira_ticket_number": "AF-1", "detailed_feature": "Local"}]
            ):
                self.assertEqual(web_module._generate_productization_detailed_features_with_codex([{"jira_ticket_number": "AF-1"}], settings=settings)[0]["detailed_feature"], "Local")

    def test_meeting_recorder_and_monthly_report_job_helper_edges(self):
        class FakeMeetingStore:
            def __init__(self):
                self.records = {
                    "r1": {"record_id": "r1", "owner_email": "owner@npt.sg", "status": "recorded"},
                    "r2": {"record_id": "r2", "owner_email": "other@npt.sg", "status": "recorded"},
                    "r3": {"record_id": "r3", "owner_email": "owner@npt.sg", "status": "recording"},
                }
                self.saved = []

            def list_records(self, **_kwargs):
                return list(self.records.values())

            def get_record(self, record_id):
                return dict(self.records[record_id])

            def save_record(self, record):
                self.saved.append(record)
                self.records[record["record_id"]] = record

        class FakeMeetingJobStore(_FakeJobStore):
            def __init__(self):
                super().__init__()
                self.snapshots = {}

            def snapshot(self, job_id):
                return self.snapshots.get(job_id)

        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            settings = app.config["SETTINGS"]
            meeting_store = FakeMeetingStore()
            job_store = FakeMeetingJobStore()
            app.config["MEETING_RECORD_STORE"] = meeting_store
            app.config["JOB_STORE"] = job_store
            fake_local = SimpleNamespace(
                meeting_recorder_diagnostics=lambda: {"status": "ok"},
                meeting_recorder_records=lambda **_kwargs: [{"record_id": "remote"}],
                meeting_recorder_process_start=lambda **_kwargs: {"state": "queued", "job_id": "remote-job"},
            )
            with app.app_context(), patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True), patch(
                "bpmis_jira_tool.web._build_local_agent_client", return_value=fake_local
            ), patch("bpmis_jira_tool.web._current_google_email", return_value="owner@npt.sg"):
                self.assertEqual(web_module._meeting_recorder_diagnostics_payload(settings), {"status": "ok"})
                self.assertEqual(web_module._meeting_recorder_record_summaries_for_current_user(settings)[0]["record_id"], "remote")
                self.assertEqual(web_module._meeting_recorder_auto_process_payload(settings=settings, record_id="r1", owner_email="owner@npt.sg")["job_id"], "remote-job")

            with app.app_context(), patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=True), patch(
                "bpmis_jira_tool.web._build_local_agent_client", side_effect=ToolError("token secret leaked")
            ):
                self.assertEqual(web_module._meeting_recorder_auto_process_payload(settings=settings, record_id="r1", owner_email="owner@npt.sg")["auto_process_error"], "Meeting processing failed. Check server logs for details.")

            with app.app_context(), patch("bpmis_jira_tool.web._current_google_email", return_value="owner@npt.sg"):
                app.config["MEETING_RECORDER_RUNTIME"] = SimpleNamespace(diagnostics=lambda: {"status": "local"})
                with patch("bpmis_jira_tool.web._local_agent_meeting_recorder_enabled", return_value=False):
                    self.assertEqual(web_module._meeting_recorder_diagnostics_payload(settings), {"status": "local"})
                    self.assertEqual(web_module._meeting_recorder_record_summaries_for_current_user(settings)[0]["record_id"], "r1")
                self.assertIsNone(web_module._meeting_recorder_process_job_snapshot_for_current_user("missing"))
                job_store.snapshots["other"] = {"action": web_module.MEETING_RECORDER_PROCESS_ACTION, "owner_email": "other@npt.sg"}
                self.assertIsNone(web_module._meeting_recorder_process_job_snapshot_for_current_user("other"))
                self.assertEqual(web_module._public_meeting_recorder_process_job_snapshot({"state": "queued"})["error_category"], "job_queued")
                self.assertEqual(web_module._public_meeting_recorder_process_job_snapshot({"state": "running"})["error_category"], "job_running")
                self.assertEqual(web_module._meeting_record_for_processing_job("r1", "owner@npt.sg")["record_id"], "r1")
                with self.assertRaises(ToolError):
                    web_module._meeting_record_for_processing_job("r2", "owner@npt.sg")
                with self.assertRaises(ToolError):
                    web_module._meeting_record_for_processing_job("r3", "owner@npt.sg")
                web_module._mark_meeting_record_process_failed("r2", "owner@npt.sg", "failed")
                web_module._mark_meeting_record_email_failed("r2", "owner@npt.sg", "failed")

            with app.app_context(), patch(
                "bpmis_jira_tool.web._build_meeting_processing_service",
                return_value=SimpleNamespace(process_recording=lambda **_kwargs: (_ for _ in ()).throw(ToolError("process failed"))),
            ):
                web_module._run_meeting_recorder_process_job(app, "meeting-job", "r1", "owner@npt.sg")
            self.assertEqual(job_store.failed[-1][0][0], "meeting-job")

            with app.app_context(), patch("bpmis_jira_tool.web._local_agent_seatalk_enabled", return_value=False), patch(
                "bpmis_jira_tool.web._build_monthly_report_service",
                return_value=SimpleNamespace(generate_draft=lambda **_kwargs: (_ for _ in ()).throw(ToolError("draft failed"))),
            ), patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                web_module._run_team_dashboard_monthly_report_draft_job(app, "monthly-job", settings, {}, {"email": "owner@npt.sg"})
            self.assertEqual(job_store.failed[-1][0][0], "monthly-job")

    def test_create_app_route_edges_for_callbacks_jobs_and_vpn(self):
        class FakeLocalAgent:
            def __init__(self, *, fail=False):
                self.fail = fail

            def vpn_profiles(self):
                if self.fail:
                    raise ToolError("vpn unavailable")
                return {"status": "ok", "profiles": []}

            def vpn_save_profile(self, payload):
                if self.fail:
                    raise ToolError("save failed")
                return {"status": "ok", "profile": payload}

            def vpn_delete_profile(self, profile_id):
                if self.fail:
                    raise ToolError("delete failed")
                return {"status": "ok", "deleted": profile_id}

            def vpn_connect(self, profile_id, *, second_password=""):
                return {"status": "ok", "connected": True, "profile_id": profile_id, "second_password_used": bool(second_password)}

            def vpn_disconnect(self):
                if self.fail:
                    raise ToolError("disconnect failed")
                return {"status": "ok", "connected": False}

            def team_dashboard_monthly_report_job(self, job_id):
                return {"job_id": job_id, "state": "remote-done"}

        class FakeVPNProfileStore:
            def __init__(self):
                self.deleted = []
                self.connected = []

            def save_profile(self, payload):
                return {"id": payload.get("id") or "p1", **payload}

            def delete_profile(self, profile_id):
                self.deleted.append(profile_id)

            def get_profile(self, profile_id, include_password=False):
                return {"id": profile_id, "vpn_host": "vpn.example", "username": "user", "password": "secret"}

            def list_profiles(self):
                return [{"id": "p1", "vpn_host": "vpn.example"}]

            def record_connected(self, profile_id):
                self.connected.append(profile_id)

        class FakeCiscoVPNClient:
            def __init__(self, *, host_error=False, connect_ok=True):
                self.host_error = host_error
                self.connect_ok = connect_ok

            def status(self):
                return {"connected": False}

            def hosts(self):
                if self.host_error:
                    raise ToolError("hosts failed")
                return ["vpn.example"]

            def connect(self, **_kwargs):
                return {"connected": self.connect_ok, "message": "not connected" if not self.connect_ok else ""}

            def disconnect(self):
                return {"connected": False, "message": "disconnected"}

        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "ENV_FILE": "/dev/null",
                "FLASK_SECRET_KEY": "",
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "TEAM_PORTAL_DATA_DIR": ".team-portal/test-web-core-route-edges",
                "LOCAL_AGENT_BASE_URL": "http://127.0.0.1:7007",
                "LOCAL_AGENT_HMAC_SECRET": "secret",
                "LOCAL_AGENT_BPMIS_ENABLED": "true",
            },
            clear=False,
        ):
            app = web_module.create_app()
            app.testing = True
            app.config["JOB_STORE"] = _CreatedJobStore()
            app.config["TEAM_DASHBOARD_CONFIG_STORE"] = SimpleNamespace(load=lambda: {"version_plan": {}}, save=lambda config: config)

            admin_profile = {"email": web_module.PORTAL_ADMIN_EMAIL, "name": "Admin"}
            with app.test_client() as client:
                with client.session_transaction() as sess:
                    sess["google_profile"] = admin_profile
                    sess["google_credentials"] = {"token": "x", "scopes": []}

                with patch("bpmis_jira_tool.web._require_google_login", return_value=None), patch(
                    "bpmis_jira_tool.web._current_google_user_is_blocked", return_value=True
                ):
                    # /source-code-qa is public now; a gated page still redirects
                    # blocked users.
                    self.assertEqual(client.get("/team-dashboard").status_code, 302)
                with client.session_transaction() as sess:
                    sess["google_profile"] = admin_profile
                    sess["google_credentials"] = {"token": "x", "scopes": []}
                with patch("bpmis_jira_tool.web._require_google_login", return_value=None), patch(
                    "bpmis_jira_tool.web._current_google_user_is_blocked", return_value=True
                ):
                    self.assertEqual(client.get("/api/jobs/blocked").status_code, 403)
                with client.session_transaction() as sess:
                    sess["google_profile"] = admin_profile
                    sess["google_credentials"] = {"token": "x", "scopes": []}

                with app.test_request_context("/portal-home"):
                    session["google_profile"] = admin_profile
                    session["google_credentials"] = {"token": "x"}
                with patch("bpmis_jira_tool.web._current_google_user_is_blocked", return_value=True):
                    self.assertEqual(app.view_functions["portal_home"]().status_code, 302)

                with app.test_request_context("/portal-home"):
                    with patch("bpmis_jira_tool.web._site_requires_google_login", return_value=True), patch(
                        "bpmis_jira_tool.web._google_session_is_connected", return_value=False
                    ), patch("bpmis_jira_tool.web.render_template", return_value="login"):
                        self.assertEqual(app.view_functions["portal_home"](), "login")
    
                with app.test_request_context("/portal-home?workspace=source-code"):
                    session["google_profile"] = admin_profile
                    session["google_credentials"] = {"token": "x"}
                    with patch("bpmis_jira_tool.web._normalize_portal_landing_redirect", return_value="/source-code-qa"):
                        self.assertEqual(app.view_functions["portal_home"]().headers["Location"], "/source-code-qa")
    
                with app.test_request_context("/portal-home?workspace=bpmis"):
                    session["google_profile"] = admin_profile
                    session["google_credentials"] = {"token": "x"}
                    with patch("bpmis_jira_tool.web._can_access_bpmis_automation_tool", return_value=False), patch(
                        "bpmis_jira_tool.web._can_access_productization_upgrade_summary", return_value=False
                    ):
                        self.assertEqual(app.view_functions["portal_home"]().status_code, 302)
    
                with app.test_request_context("/portal-home?workspace=bpmis"):
                    session["google_profile"] = admin_profile
                    session["google_credentials"] = {"token": "x"}
                    with patch("bpmis_jira_tool.web._can_access_bpmis_automation_tool", return_value=True), patch(
                        "bpmis_jira_tool.web._load_user_config_for_identity", side_effect=ToolError("Mac local-agent is unavailable")
                    ), patch("bpmis_jira_tool.web._is_local_agent_unavailable_error", return_value=True), patch(
                        "bpmis_jira_tool.web.render_template", return_value="portal"
                    ):
                        self.assertEqual(app.view_functions["portal_home"](), "portal")

                with patch.dict(os.environ, {"TEAM_PORTAL_CLOUD_HOME_ENABLED": "false", "TEAM_PORTAL_DATA_DIR": ".team-portal/test-web-core-index-edges"}, clear=False):
                    noncloud_app = web_module.create_app()
                    noncloud_app.testing = True
                    with noncloud_app.test_request_context("/?workspace=bpmis"):
                        session["google_profile"] = admin_profile
                        session["google_credentials"] = {"token": "x"}
                        with patch("bpmis_jira_tool.web._get_user_identity", return_value={"email": web_module.PORTAL_ADMIN_EMAIL, "config_key": "google:admin"}), patch(
                            "bpmis_jira_tool.web._load_user_config_for_identity", side_effect=ToolError("boom")
                        ), patch("bpmis_jira_tool.web._is_local_agent_unavailable_error", return_value=False):
                            with self.assertRaises(ToolError):
                                noncloud_app.view_functions["index"]()

                with app.test_request_context("/portal-home?workspace=bpmis"):
                    session["google_profile"] = admin_profile
                    session["google_credentials"] = {"token": "x"}
                    with patch("bpmis_jira_tool.web._get_user_identity", return_value={"email": web_module.PORTAL_ADMIN_EMAIL, "config_key": "google:admin"}), patch(
                        "bpmis_jira_tool.web._can_access_bpmis_automation_tool", return_value=True
                    ), patch(
                        "bpmis_jira_tool.web._load_user_config_for_identity", side_effect=ToolError("boom")
                    ), patch("bpmis_jira_tool.web._is_local_agent_unavailable_error", return_value=False):
                        with self.assertRaises(ToolError):
                            app.view_functions["portal_home"]()

                with app.test_request_context("/portal-home"):
                    session["google_profile"] = admin_profile
                    session["google_credentials"] = {"token": "x"}
                    session["default_workspace_tab"] = "unknown"
                    with patch("bpmis_jira_tool.web._can_access_bpmis_automation_tool", return_value=True), patch(
                        "bpmis_jira_tool.web._load_user_config_for_identity", return_value=None
                    ), patch("bpmis_jira_tool.web._normalize_portal_landing_redirect", return_value=""), patch(
                        "bpmis_jira_tool.web._can_access_productization_upgrade_summary", return_value=True
                    ), patch("bpmis_jira_tool.web.render_template", side_effect=lambda _template, **kwargs: kwargs):
                        self.assertEqual(app.view_functions["portal_home"]()["default_workspace_tab"], "productization-upgrade-summary")

                with app.test_request_context("/healthz"):
                    self.assertEqual(app.view_functions["healthz"]()[1], web_module.HTTPStatus.OK)
                with app.test_request_context("/cloud-static/team_dashboard.js"):
                    self.assertEqual(app.view_functions["cloud_static"]("team_dashboard.js").status_code, 200)

                with patch("bpmis_jira_tool.web.create_google_authorization_url", return_value="https://accounts.example/auth"):
                    self.assertEqual(client.get("/auth/google/login").headers["Location"], "https://accounts.example/auth")
                    self.assertEqual(client.get("/cloud-auth/google/login").headers["Location"], "https://accounts.example/auth")

                with patch("bpmis_jira_tool.web.create_google_authorization_url", side_effect=ConfigError("oauth missing")):
                    self.assertEqual(client.get("/auth/google/login").status_code, 302)
                    self.assertEqual(client.get("/cloud-auth/google/login").status_code, 302)
    
                previous = {"config_key": "google:old", "email": "old@npt.sg"}
                current = {"config_key": "google:new", "email": web_module.PORTAL_ADMIN_EMAIL}
                with patch("bpmis_jira_tool.web.finish_google_oauth", return_value=None), patch(
                    "bpmis_jira_tool.web._get_user_identity", side_effect=[previous, current, current, previous, current, current]
                ), patch("bpmis_jira_tool.web._current_google_user_is_blocked", return_value=False), patch(
                    "bpmis_jira_tool.web._migrate_user_config"
                ) as migrate_config, patch("bpmis_jira_tool.web._persist_owner_google_credentials"), patch(
                    "bpmis_jira_tool.web._normalize_post_google_login_redirect_target", return_value="/after"
                ):
                    self.assertEqual(client.get("/auth/google/callback?code=1").headers["Location"], "/after")
                    self.assertEqual(client.get("/cloud-auth/google/callback?code=1").headers["Location"], "/after")
                    self.assertEqual(migrate_config.call_count, 2)
    
                with patch("bpmis_jira_tool.web.finish_google_oauth", return_value=None), patch(
                    "bpmis_jira_tool.web._current_google_user_is_blocked", return_value=True
                ):
                    self.assertEqual(client.get("/auth/google/callback?code=blocked").status_code, 302)
    
                with patch("bpmis_jira_tool.web.finish_google_oauth", side_effect=ToolError("oauth failed")), patch(
                    "bpmis_jira_tool.web._normalize_post_google_login_redirect_target", return_value=""
                ), patch("bpmis_jira_tool.web._cloud_home_default_post_login_redirect", return_value="/fallback"):
                    self.assertEqual(client.get("/auth/google/callback?error=1").headers["Location"], "/fallback")
    
                with patch("bpmis_jira_tool.web.finish_google_oauth", return_value=None), patch(
                    "bpmis_jira_tool.web._current_google_user_is_blocked", return_value=True
                ):
                    self.assertEqual(client.get("/cloud-auth/google/callback?code=blocked").status_code, 302)
    
                with patch("bpmis_jira_tool.web.finish_google_oauth", side_effect=ToolError("oauth failed")), patch(
                    "bpmis_jira_tool.web._normalize_post_google_login_redirect_target", return_value=""
                ), patch("bpmis_jira_tool.web._cloud_home_default_post_login_redirect", return_value="/fallback"):
                    self.assertEqual(client.get("/cloud-auth/google/callback?error=1").headers["Location"], "/fallback")
    
                self.assertEqual(client.post("/cloud-auth/google/logout").status_code, 302)
                with client.session_transaction() as sess:
                    sess["google_profile"] = admin_profile
                    sess["google_credentials"] = {"token": "x", "scopes": []}
    
                with patch("bpmis_jira_tool.web._require_vpn_connection_access", return_value=None), patch(
                    "bpmis_jira_tool.web._vpn_connection_uses_local_agent", return_value=True
                ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=FakeLocalAgent()):
                    self.assertEqual(client.get("/api/vpn-connection/profiles").get_json()["status"], "ok")
                    self.assertEqual(client.post("/api/vpn-connection/profiles", json={"id": "p1"}).get_json()["profile"]["id"], "p1")
                    self.assertEqual(client.delete("/api/vpn-connection/profiles/p1").get_json()["deleted"], "p1")
                    self.assertTrue(client.post("/api/vpn-connection/profiles/p1/connect", json={"second_password": "otp"}).get_json()["connected"])
                    self.assertFalse(client.post("/api/vpn-connection/disconnect").get_json()["connected"])
    
                with patch("bpmis_jira_tool.web._require_vpn_connection_access", return_value=None), patch(
                    "bpmis_jira_tool.web._vpn_connection_uses_local_agent", return_value=True
                ), patch("bpmis_jira_tool.web._build_local_agent_client", return_value=FakeLocalAgent(fail=True)):
                    self.assertEqual(client.get("/api/vpn-connection/profiles").status_code, 400)
                    self.assertEqual(client.post("/api/vpn-connection/profiles", json={}).status_code, 400)
                    self.assertEqual(client.delete("/api/vpn-connection/profiles/p1").status_code, 400)
                    self.assertEqual(client.post("/api/vpn-connection/disconnect").status_code, 400)
    
                gate = (web_module.jsonify({"status": "error", "message": "blocked"}), web_module.HTTPStatus.FORBIDDEN)
                with patch("bpmis_jira_tool.web._require_vpn_connection_access", return_value=gate):
                    self.assertEqual(client.post("/api/vpn-connection/profiles", json={}).status_code, 403)
                    self.assertEqual(client.delete("/api/vpn-connection/profiles/p1").status_code, 403)
                    self.assertEqual(client.post("/api/vpn-connection/profiles/p1/connect", json={}).status_code, 403)
                    self.assertEqual(client.post("/api/vpn-connection/disconnect").status_code, 403)
    
                app.config["VPN_PROFILE_STORE"] = FakeVPNProfileStore()
                app.config["CISCO_VPN_CLIENT"] = FakeCiscoVPNClient(host_error=True)
                with patch("bpmis_jira_tool.web._require_vpn_connection_access", return_value=None), patch(
                    "bpmis_jira_tool.web._vpn_connection_uses_local_agent", return_value=False
                ):
                    self.assertEqual(client.get("/api/vpn-connection/profiles").get_json()["hosts"], [])
                    self.assertEqual(client.post("/api/vpn-connection/profiles", json={"id": "p2"}).get_json()["profile"]["id"], "p2")
                    self.assertEqual(client.delete("/api/vpn-connection/profiles/p2").get_json()["profiles"][0]["id"], "p1")
                    self.assertEqual(client.post("/api/vpn-connection/profiles/p1/connect", json={"second_password": "otp"}).get_json()["vpn_status"]["connected"], True)
                    self.assertEqual(client.post("/api/vpn-connection/disconnect").get_json()["vpn_status"]["connected"], False)
    
                app.config["CISCO_VPN_CLIENT"] = FakeCiscoVPNClient(connect_ok=False)
                with patch("bpmis_jira_tool.web._require_vpn_connection_access", return_value=None), patch(
                    "bpmis_jira_tool.web._vpn_connection_uses_local_agent", return_value=False
                ):
                    self.assertEqual(client.post("/api/vpn-connection/profiles/p1/connect", json={}).status_code, 400)
    
                with patch("bpmis_jira_tool.web._remote_bpmis_config_enabled", return_value=True), patch(
                    "bpmis_jira_tool.web._build_local_agent_client", return_value=FakeLocalAgent()
                ):
                    self.assertEqual(client.get("/api/jobs/remote-job").get_json()["state"], "remote-done")
                with patch("bpmis_jira_tool.web._remote_bpmis_config_enabled", return_value=True), patch(
                    "bpmis_jira_tool.web._build_local_agent_client",
                    return_value=SimpleNamespace(team_dashboard_monthly_report_job=lambda _job_id: (_ for _ in ()).throw(ToolError("remote down"))),
                ):
                    self.assertEqual(client.get("/api/jobs/remote-missing").status_code, 404)
                app.config["JOB_STORE"].snapshots["source-job"] = {"job_id": "source-job", "action": "source-code-qa-query"}
                with patch("bpmis_jira_tool.web._require_source_code_qa_access", return_value=("blocked", 403)):
                    self.assertEqual(client.get("/api/jobs/source-job").status_code, 403)
                with patch("bpmis_jira_tool.web._require_source_code_qa_access", return_value=None), patch(
                    "bpmis_jira_tool.web._source_code_qa_job_snapshot_for_current_user", return_value=None
                ):
                    self.assertEqual(client.get("/api/jobs/source-job").status_code, 404)
                app.config["JOB_STORE"].snapshots["effort-job"] = {"job_id": "effort-job", "action": "source-code-qa-effort-assessment"}
                with patch("bpmis_jira_tool.web._require_source_code_qa_manage_access", return_value=("blocked", 403)):
                    self.assertEqual(client.get("/api/jobs/effort-job").status_code, 403)
                with patch("bpmis_jira_tool.web._require_source_code_qa_manage_access", return_value=None), patch(
                    "bpmis_jira_tool.web._public_source_code_qa_job_snapshot", side_effect=lambda snapshot: {"job_id": snapshot["job_id"]}
                ):
                    self.assertEqual(client.get("/api/jobs/effort-job").get_json()["job_id"], "effort-job")
    
                with patch("bpmis_jira_tool.web._require_google_login", return_value=("login", 401)):
                    self.assertEqual(client.post("/api/jobs/sync-bpmis-projects").status_code, 401)

                with app.test_request_context("/api/jobs/sync-bpmis-projects", method="POST"):
                    with patch("bpmis_jira_tool.web._require_google_login", return_value=("login", 401)):
                        self.assertEqual(app.view_functions["create_sync_bpmis_projects_job"](), ("login", 401))

                with app.test_request_context("/uat-local-agent/healthz"):
                    with patch("bpmis_jira_tool.web._LOCAL_AGENT_SESSION.request", side_effect=web_module.requests.RequestException("refused")):
                        self.assertEqual(web_module._proxy_local_agent_request("healthz")[1], web_module.HTTPStatus.BAD_GATEWAY)

    def test_prd_self_assessment_and_generation_job_edges(self):
        class FakePRDService:
            def __init__(self, *, error=None, type_error=False):
                self.error = error
                self.type_error = type_error

            def _result(self, request_model, progress_callback=None):
                if self.error:
                    raise self.error
                if self.type_error and progress_callback is not None:
                    raise TypeError("progress_callback is not supported")
                return {"status": "ok", "cached": False, "url": getattr(request_model, "prd_url", "")}

            def review(self, request_model, progress_callback=None):
                return self._result(request_model, progress_callback)

            def summarize(self, request_model, progress_callback=None):
                return self._result(request_model, progress_callback)

            def review_url(self, request_model, progress_callback=None):
                return self._result(request_model, progress_callback)

            def summarize_url(self, request_model, progress_callback=None):
                return self._result(request_model, progress_callback)

            def list_url_sections(self, request_model):
                if self.error:
                    raise self.error
                return {"status": "ok", "sections": [{"index": 0, "title": "Intro"}], "url": getattr(request_model, "prd_url", "")}

        identity = {"email": "owner@npt.sg", "config_key": "google:owner@npt.sg"}
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._app(temp_dir)
            settings = app.config["SETTINGS"]
            app.config["JOB_STORE"] = _FakeJobStore()

            base_patches = [
                patch("bpmis_jira_tool.web._require_prd_self_assessment_access", return_value=None),
                patch("bpmis_jira_tool.web._get_user_identity", return_value=identity),
                patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=False),
                patch("bpmis_jira_tool.web._is_portal_admin", return_value=True),
                patch("bpmis_jira_tool.web._log_portal_event", return_value=None),
            ]
            with app.test_request_context("/api/prd", method="POST", json={"prd_url": "https://docs/prd", "selected_section_indexes": [0], "force_refresh": True}):
                for patcher in base_patches:
                    patcher.start()
                try:
                    session["google_credentials"] = {"token": "drive", "scopes": [web_module.GOOGLE_DRIVE_READONLY_SCOPE]}
                    with patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDService()):
                        response = web_module._run_prd_self_assessment_action(settings, action="review")
                    self.assertEqual(response.get_json()["status"], "ok")
                    self.assertTrue(app.config["PRD_BRIEFING_STORE"].saved)
                finally:
                    for patcher in reversed(base_patches):
                        patcher.stop()

            with app.test_request_context("/api/prd", method="POST", json={"prd_url": "https://docs/prd"}), patch(
                "bpmis_jira_tool.web._require_prd_self_assessment_access", return_value=("blocked", 403)
            ):
                self.assertEqual(web_module._run_prd_self_assessment_action(settings, action="review"), ("blocked", 403))

            with app.test_request_context("/api/prd", method="POST", json={"prd_url": "https://docs/prd"}), patch(
                "bpmis_jira_tool.web._require_prd_self_assessment_access", return_value=None
            ), patch("bpmis_jira_tool.web._is_portal_admin", return_value=False):
                summary_denied = web_module._run_prd_self_assessment_action(settings, action="summary")
            self.assertEqual(summary_denied[1], web_module.HTTPStatus.FORBIDDEN)

            for service_error, expected_status in ((ToolError("bad prd"), web_module.HTTPStatus.BAD_REQUEST), (RuntimeError("boom"), web_module.HTTPStatus.INTERNAL_SERVER_ERROR)):
                with app.test_request_context("/api/prd", method="POST", json={"prd_url": "https://docs/prd"}), patch(
                    "bpmis_jira_tool.web._require_prd_self_assessment_access", return_value=None
                ), patch("bpmis_jira_tool.web._get_user_identity", return_value=identity), patch(
                    "bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=False
                ), patch("bpmis_jira_tool.web._is_portal_admin", return_value=True), patch(
                    "bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDService(error=service_error)
                ), patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                    response = web_module._run_prd_self_assessment_action(settings, action="review")
                self.assertEqual(response[1], expected_status)

            for service_error, expected_status in ((None, 200), (ToolError("section denied"), web_module.HTTPStatus.BAD_REQUEST), (RuntimeError("section crashed"), web_module.HTTPStatus.INTERNAL_SERVER_ERROR)):
                with app.test_request_context("/api/prd/sections", method="POST", json={"prd_url": "https://docs/prd"}), patch(
                    "bpmis_jira_tool.web._require_prd_self_assessment_access", return_value=None
                ), patch("bpmis_jira_tool.web._get_user_identity", return_value=identity), patch(
                    "bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=False
                ), patch(
                    "bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDService(error=service_error)
                ), patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                    response = web_module._run_prd_self_assessment_sections(settings)
                status_code = response.status_code if hasattr(response, "status_code") else response[1]
                self.assertEqual(status_code, expected_status)

            with app.test_request_context("/api/prd/sections", method="POST", json={"prd_url": "https://docs/prd"}), patch(
                "bpmis_jira_tool.web._require_prd_self_assessment_access", return_value=("blocked", 403)
            ):
                self.assertEqual(web_module._run_prd_self_assessment_sections(settings), ("blocked", 403))

            with app.test_request_context("/api/monthly-report"), patch(
                "bpmis_jira_tool.web._require_google_login", return_value=("login", 401)
            ):
                self.assertEqual(web_module._require_team_dashboard_monthly_report_access(settings, api=True), ("login", 401))

            with app.app_context():
                web_module._save_prd_latest_result(owner_key="", tool_key="prd_self_assessment", payload={})
                self.assertIsNone(web_module._get_prd_latest_result(owner_key="", tool_key="prd_self_assessment"))

            def prd_payload_for(action):
                if action.startswith("team_"):
                    return {"owner_key": "google:owner@npt.sg", "jira_id": "AF-1", "jira_link": "https://jira/AF-1", "prd_url": "https://docs/prd"}
                return {"owner_key": "google:owner@npt.sg", "prd_url": "https://docs/prd", "language": "zh"}

            for action in ("team_review", "team_summary", "self_review", "self_summary"):
                with patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=False), patch(
                    "bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDService(type_error=True)
                ), patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                    web_module._run_prd_generation_job(
                        app,
                        f"job-{action}",
                        settings,
                        action,
                        prd_payload_for(action),
                        identity,
                    )
            self.assertEqual(len(app.config["JOB_STORE"].completed), 4)

            with patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=False), patch(
                "bpmis_jira_tool.web._build_prd_review_service",
                return_value=SimpleNamespace(review=lambda request_model, progress_callback=None: (_ for _ in ()).throw(TypeError("unrelated type error"))),
            ), patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                web_module._run_prd_generation_job(
                    app,
                    "job-prd-type-error",
                    settings,
                    "team_review",
                    prd_payload_for("team_review"),
                    identity,
                )

            fake_local = SimpleNamespace(
                prd_review=lambda payload, progress_callback=None: {"status": "ok", "cached": True},
                prd_summary=lambda payload, progress_callback=None: {"status": "ok", "cached": True},
                prd_self_assessment_review=lambda payload, progress_callback=None: {"status": "ok", "cached": True},
                prd_self_assessment_summary=lambda payload, progress_callback=None: {"status": "ok", "cached": True},
                prd_self_assessment_sections=lambda payload: {"status": "ok", "sections": []},
            )
            for action in ("team_review", "team_summary", "self_review", "self_summary"):
                with patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True), patch(
                    "bpmis_jira_tool.web._build_local_agent_client", return_value=fake_local
                ), patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                    web_module._run_prd_generation_job(
                        app,
                        f"job-local-{action}",
                        settings,
                        action,
                        prd_payload_for(action),
                        identity,
                    )

            with patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=False), patch(
                "bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDService(error=ToolError("generation failed"))
            ), patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                web_module._run_prd_generation_job(
                    app,
                    "job-prd-tool-error",
                    settings,
                    "team_review",
                    prd_payload_for("team_review"),
                    identity,
                )
            with patch("bpmis_jira_tool.web._log_portal_event", return_value=None):
                web_module._run_prd_generation_job(app, "job-prd-unsupported", settings, "unknown", {}, identity)
            self.assertEqual(app.config["JOB_STORE"].failed[-2][0][0], "job-prd-tool-error")
            self.assertEqual(app.config["JOB_STORE"].failed[-1][0][0], "job-prd-unsupported")


if __name__ == "__main__":
    unittest.main()
