from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.job_store import JobStore
from bpmis_jira_tool.monthly_report import MonthlyReportSendResult
from bpmis_jira_tool.team_dashboard_version_plan import mark_version_plan_sync_running, normalize_version_plan_state
from bpmis_jira_tool.web_team_dashboard_routes import build_team_dashboard_handlers, register_team_dashboard_routes


class _Snapshot:
    def __init__(self, config, revision="rev-1", metadata=None):
        self.config = config
        self.revision = revision
        self.metadata = metadata or {
            "revision": revision,
            "source_hash": "hash-1",
            "backend": "fake",
            "environment": "test",
            "updated_at_sgt": "2026-05-24 10:00",
        }


class _VersionPlanStore:
    def __init__(self, config):
        self.config = config
        self.revision = "rev-1"
        self.saved = []

    def load_snapshot(self):
        return _Snapshot(self.config, self.revision)

    def save_config(self, config, *, expected_revision=None):
        self.config = config
        self.saved.append((config, expected_revision))
        self.revision = f"rev-{len(self.saved) + 1}"
        return _Snapshot(self.config, self.revision)


class _FakeConfigStore:
    def __init__(self, config=None):
        self.config = config or {}
        self.saved = []

    def load(self):
        return self.config

    def save(self, config):
        self.config = config
        self.saved.append(config)
        return config


class _FakeMappingStore:
    def __init__(self):
        self.data = {}

    def replace_mappings(self, mappings):
        self.data = dict(mappings)
        return self.data

    def mappings(self):
        return dict(self.data)

    def merge_mappings(self, mappings):
        self.data.update(mappings)
        return dict(self.data)


class _FakeBPMISClient:
    def __init__(self, *, ping_error=None, status_detail=None, link_detail=None, issue_detail=None):
        self.ping_error = ping_error
        self.ping_count = 0
        self.status_detail = {"status": {"label": "Testing"}} if status_detail is None else status_detail
        self.link_detail = {"parentIds": [{"id": "225159"}]} if link_detail is None else link_detail
        self.issue_detail = {"project_name": "Linked Project", "statusId": "10"} if issue_detail is None else issue_detail

    def ping(self):
        self.ping_count += 1
        if self.ping_error:
            raise self.ping_error

    def update_biz_project_status(self, _bpmis_id, _status):
        return self.status_detail

    def link_jira_ticket_to_project(self, _jira_id, _bpmis_id):
        return self.link_detail

    def get_issue_detail(self, _bpmis_id):
        return self.issue_detail


class _FakeLocalAgentClient:
    def __init__(self):
        self.latest_draft = {"status": "ok", "draft_markdown": "Remote draft"}
        self.daily_briefs = [{"brief_id": "b1", "subject": "Daily", "sent_at": "2026"}]

    def team_dashboard_monthly_report_latest_draft(self):
        return self.latest_draft

    def team_dashboard_daily_briefs(self):
        return self.daily_briefs

    def team_dashboard_daily_brief_download(self, _brief_id):
        return SimpleNamespace(status_code=200, content=b"%PDF", headers={"Content-Type": "application/pdf", "Content-Disposition": "attachment; filename=daily.pdf"})

    def team_dashboard_monthly_report_draft_start(self, _payload):
        return {"job_id": "remote-job"}

    def team_dashboard_monthly_report_send(self, _payload):
        return {"message_id": "remote-msg"}

    def prd_review(self, _payload):
        return {"status": "ok", "cached": True}

    def prd_summary(self, _payload):
        return {"status": "ok", "cached": True}


class WebTeamDashboardRouteEdgeTests(unittest.TestCase):
    def setUp(self):
        import bpmis_jira_tool.web_team_dashboard_routes as routes

        routes._VERSION_PLAN_SYNC_RUNNING = False

    def _build_app(self, **overrides):
        config = overrides.pop("config", {"version_plan": normalize_version_plan_state({})})
        config_store = overrides.pop("config_store", _FakeConfigStore(config))
        version_store = overrides.pop("version_store", _VersionPlanStore(config))
        local_agent = overrides.pop("local_agent", _FakeLocalAgentClient())
        mapping_store = overrides.pop("mapping_store", _FakeMappingStore())
        bpmis_client = overrides.pop("bpmis_client", _FakeBPMISClient())
        state = {
            "can_manage": overrides.pop("can_manage", True),
            "remote_enabled": overrides.pop("remote_enabled", False),
            "seatalk_enabled": overrides.pop("seatalk_enabled", False),
            "source_code_qa_enabled": overrides.pop("source_code_qa_enabled", False),
            "has_gmail_scope": overrides.pop("has_gmail_scope", False),
            "current_email": overrides.pop("current_email", "xiaodong.zheng@npt.sg"),
        }

        def identity(_settings):
            return {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong", "config_key": "google:xiaodong.zheng@npt.sg"}

        ctx = SimpleNamespace(
            settings=SimpleNamespace(
                gmail_seatalk_demo_owner_email="owner@npt.sg",
                seatalk_owner_email="owner@npt.sg",
                team_portal_data_dir=Path(tempfile.mkdtemp()),
                version_plan_store_backend="local",
                cloud_run_environment="",
            ),
            _require_team_dashboard_access=lambda _settings, api=False: overrides.get("team_dashboard_access_gate"),
            _require_team_dashboard_version_plan_access=lambda _settings, api=False: overrides.get("version_plan_access_gate"),
            _require_team_dashboard_monthly_report_access=lambda _settings, api=False: overrides.get("monthly_access_gate"),
            _get_user_identity=identity,
            _get_team_dashboard_config_store=lambda: config_store,
            _can_manage_team_dashboard=lambda _user: state["can_manage"],
            _can_access_team_dashboard_version_plan=lambda _user: True,
            _full_portal_navigation_available=lambda: True,
            _can_access_team_dashboard_monthly_report=lambda _user: True,
            _seatalk_dashboard_is_configured=lambda _settings: True,
            _log_portal_event=lambda *args, **kwargs: None,
            _build_request_log_context=lambda _settings, user_identity=None, extra=None: dict(extra or {}),
            _local_agent_seatalk_enabled=lambda _settings: state["seatalk_enabled"],
            _build_local_agent_client=lambda _settings: local_agent,
            _get_daily_brief_archive_store=lambda _settings: overrides.get("daily_store"),
            _get_seatalk_name_mapping_store=lambda _settings: mapping_store,
            _build_seatalk_dashboard_service=lambda _settings: overrides.get("seatalk_service", SimpleNamespace(build_name_mappings=lambda force_refresh=False: {"auto_mappings": {"A": "Alice"}, "unknown_ids": [{"id": "B"}]})),
            _dedupe_seatalk_name_mapping_candidates=lambda rows: rows,
            _classify_portal_error=overrides.get(
                "classify_portal_error",
                lambda error: {"error_category": "local_agent_unavailable" if "local-agent" in str(error).lower() else "tool_error"},
            ),
            _load_team_dashboard_tasks_for_all_teams_merged=lambda *args, **kwargs: (
                (_ for _ in ()).throw(overrides["all_team_reload_error"])
                if "all_team_reload_error" in overrides
                else [{"team_key": "AF", "label": "Anti-Fraud"}]
            ),
            _current_google_email=lambda: state["current_email"],
            _team_dashboard_new_timing=lambda: {},
            _team_dashboard_add_timing=lambda timings, key, started_at: timings.setdefault(key, 0),
            _normalize_team_dashboard_emails=lambda emails: [str(email).lower() for email in emails],
            _cached_team_dashboard_task_payload=lambda _config, _team_key, _emails: overrides.get("cached_team"),
            _build_bpmis_client_for_current_user=lambda _settings: bpmis_client,
            _team_dashboard_load_jira_and_biz_projects=lambda *_args, **_kwargs: (_ for _ in ()).throw(overrides["task_error"]) if "task_error" in overrides else (overrides.get("tasks", []), overrides.get("biz_projects", [])),
            _build_team_dashboard_task_group=lambda team_key, label, emails, tasks, biz_projects, key_project_overrides=None: {"team_key": team_key, "label": label, "member_emails": emails, "under_prd": list(tasks or []), "pending_live": list(biz_projects or [])},
            _backfill_team_dashboard_empty_project_jira_tasks=lambda *_args, **_kwargs: None,
            _remove_team_dashboard_zero_jira_pending_live_projects=lambda *_args, **_kwargs: None,
            _hydrate_team_dashboard_actual_mandays=lambda *_args, **_kwargs: None,
            _queue_team_dashboard_actual_mandays_refresh=lambda *_args, **_kwargs: overrides.get("pending_mandays", []),
            _team_dashboard_combined_request_timings=lambda *_clients: {"api": 0.01},
            _team_dashboard_combined_fetch_stats=lambda *_clients: {"calls": 1},
            _store_team_dashboard_task_payload=lambda *_args, **_kwargs: None,
            _apply_team_dashboard_key_project_state=lambda project, overrides_map: {"is_key_project": bool((overrides_map.get(project["bpmis_id"]) or {}).get("is_key_project")), "key_project_source": "manual"},
            _load_team_dashboard_link_biz_jira_rows=lambda *_args, **_kwargs: (_ for _ in ()).throw(overrides["link_rows_error"]) if "link_rows_error" in overrides else overrides.get("link_rows", [{"jira_id": "AF-1"}]),
            _suggest_team_dashboard_link_biz_project_rows=lambda *_args, **_kwargs: (_ for _ in ()).throw(overrides["suggestion_error"]) if "suggestion_error" in overrides else overrides.get("suggestion_result", {"rows": [], "matched_count": 0, "team_candidate_count": 0, "keyword_candidate_count": 0}),
            _extract_issue_key_from_text=lambda text: str(text).strip().upper() if str(text).strip() else "",
            _team_dashboard_link_biz_candidate_projects_by_pm=lambda _client, emails, team_payloads=None: (_ for _ in ()).throw(overrides["candidate_error"]) if "candidate_error" in overrides else {emails[0]: [{"bpmis_id": "225159"}]},
            _extract_parent_issue_ids_from_any=lambda detail: [str(item.get("id")) for item in detail.get("parentIds", [])] if isinstance(detail, dict) else [],
            _normalize_team_dashboard_project=lambda row: {"bpmis_id": str(row.get("bpmis_id") or row.get("issue_id") or ""), "project_name": str(row.get("project_name") or "")},
            _jira_browse_base_url=lambda: "https://jira/browse/",
            _load_all_team_dashboard_task_payloads=lambda *_args, **_kwargs: (_ for _ in ()).throw(overrides["load_all_error"]) if "load_all_error" in overrides else overrides.get("all_team_payloads", [{"team_key": "AF"}]),
            _remote_bpmis_config_enabled=lambda _settings: state["remote_enabled"],
            _run_team_dashboard_monthly_report_draft_job=lambda app, job_id, settings, payload, user_identity: app.config["JOB_STORE"].complete(
                job_id,
                results=[{"draft_markdown": "Draft", "generation_summary": {"generation_version": "v1"}}],
                notice={},
            ),
            _google_credentials_have_scopes=lambda *_scopes: state["has_gmail_scope"],
            _refresh_monthly_report_history_from_gmail=lambda _settings: (
                (_ for _ in ()).throw(overrides["history_refresh_error"])
                if "history_refresh_error" in overrides
                else overrides.get("history_refresh_result", {"scanned": 1, "matched": 1, "report_count": 1, "items": [{"summary": "Sent report", "content": "Highlights\n- A"}]})
            ),
            _local_agent_source_code_qa_enabled=lambda _settings: state["source_code_qa_enabled"],
            _build_prd_review_service=lambda _settings: overrides.get("prd_service", SimpleNamespace(review=lambda request: {"status": "ok", "cached": False}, summarize=lambda request: {"status": "ok", "cached": False})),
            _queue_prd_generation_job=lambda *_args, **_kwargs: ({"status": "queued", "job_id": "prd-job"}, 202),
            resolve_monthly_report_period=overrides.get("resolve_period"),
            send_monthly_report_email=overrides.get("send_email"),
        )
        from bpmis_jira_tool.monthly_report import resolve_monthly_report_period, send_monthly_report_email

        ctx.resolve_monthly_report_period = ctx.resolve_monthly_report_period or resolve_monthly_report_period
        ctx.send_monthly_report_email = ctx.send_monthly_report_email or send_monthly_report_email

        app = Flask(__name__)
        app.secret_key = "test"
        app.testing = True
        app.config["JOB_STORE"] = JobStore()
        app.config["GOOGLE_CREDENTIAL_STORE"] = object()
        handlers = build_team_dashboard_handlers(ctx)
        register_team_dashboard_routes(app, handlers)
        return app, config_store, version_store

    def test_project_status_status_text_fallbacks_and_errors(self):
        config = {"task_cache": {"teams": {"AF": {"under_prd": [{"bpmis_id": "225159"}, {"bpmis_id": "other"}]}}}}
        cases = [
            ({}, "Testing"),
            ([], "Testing"),
            ({"status": {}}, "Testing"),
            ({"status": [None, {}]}, "Testing"),
            ({"statusId": "4"}, "Pending Review"),
        ]
        for detail, expected_status in cases:
            app, _store, _version_store = self._build_app(config=config, bpmis_client=_FakeBPMISClient(status_detail=detail))
            with self.subTest(detail=detail), app.test_client() as client:
                response = client.post("/api/team-dashboard/project-status", json={"bpmis_id": "225159", "status": "Testing"})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json()["project_status"], expected_status)

        bad_client = _FakeBPMISClient()
        bad_client.update_biz_project_status = lambda _bpmis_id, _status: (_ for _ in ()).throw(RuntimeError("BPMIS down"))
        app, _store, _version_store = self._build_app(bpmis_client=bad_client)
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/project-status", json={"bpmis_id": "225159", "status": "Testing"})
        self.assertEqual(response.status_code, 400)

    def test_admin_gate_and_project_status_edges(self):
        app, _store, _version_store = self._build_app(can_manage=False)
        with app.test_client() as client:
            self.assertEqual(client.get("/api/team-dashboard/config").status_code, 403)
            self.assertEqual(client.post("/api/team-dashboard/project-status", json={}).status_code, 403)

        config = {
            "task_cache": {
                "teams": {
                    "AF": {
                        "under_prd": [{"bpmis_id": "225159", "status": "Old"}, "bad"],
                        "pending_live": "bad",
                    },
                    "CRMS": "bad",
                }
            }
        }
        app, store, _version_store = self._build_app(config=config, bpmis_client=_FakeBPMISClient(status_detail={"status": [{"label": "Confirmed"}]}))
        with app.test_client() as client:
            self.assertEqual(client.post("/api/team-dashboard/project-status", json={}).status_code, 400)
            self.assertEqual(client.post("/api/team-dashboard/project-status", json={"bpmis_id": "225159"}).status_code, 400)
            response = client.post("/api/team-dashboard/project-status", json={"bpmis_id": "225159", "status": "Testing"})

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["project_status"], "Confirmed")
        self.assertEqual(payload["cached_updates"], 1)
        self.assertEqual(store.config["task_cache"]["teams"]["AF"]["under_prd"][0]["status"], "Confirmed")

    def test_version_plan_sync_and_status_error_edges(self):
        local_error_client = _FakeBPMISClient(ping_error=ToolError("Mac local-agent is unavailable"))
        app, _store, _version_store = self._build_app(bpmis_client=local_error_client)
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/version-plan/af/sync")
        self.assertEqual(response.status_code, 400)
        self.assertIn("local-agent", response.get_json()["message"].lower())

        generic_error_client = _FakeBPMISClient(ping_error=RuntimeError("unexpected sync failure"))
        app, _store, _version_store = self._build_app(bpmis_client=generic_error_client)
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/version-plan/af?sync=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["sync_state"]["state"], "error")

    def test_version_plan_store_fallback_and_running_edges(self):
        import bpmis_jira_tool.web_team_dashboard_routes as routes

        class LoadPermissionStore:
            def load_snapshot(self):
                raise PermissionError("gcloud credentials.db permission denied")

            def save_config(self, config, *, expected_revision=None):
                return _Snapshot(config, "saved")

        class SavePermissionStore:
            def __init__(self, config):
                self.config = config

            def load_snapshot(self):
                return _Snapshot(self.config, "remote-rev")

            def save_config(self, config, *, expected_revision=None):
                raise PermissionError("firestore permission denied")

        app, _store, _version_store = self._build_app()
        with patch("bpmis_jira_tool.web_team_dashboard_routes.build_version_plan_store", return_value=LoadPermissionStore()):
            with app.test_client() as client:
                response = client.get("/api/team-dashboard/version-plan/af")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["store_backend"], "team_dashboard_config")

        save_store = SavePermissionStore({"version_plan": normalize_version_plan_state({})})
        app, _store, _version_store = self._build_app()
        with patch("bpmis_jira_tool.web_team_dashboard_routes.build_version_plan_store", return_value=save_store):
            with app.test_client() as client:
                row_id = client.get("/api/team-dashboard/version-plan/af").get_json()["pipeline_rows"][0]["row_id"]
                response = client.post(
                    "/api/team-dashboard/version-plan/af/cell",
                    json={"scope": "pipeline", "row_id": row_id, "field": "remarks", "value": "covered"},
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["store_backend"], "team_dashboard_config")

        class DictSaveStore:
            def __init__(self):
                self.config = {"version_plan": normalize_version_plan_state({})}

            def load_snapshot(self):
                return _Snapshot(self.config, "dict-rev")

            def save_config(self, config, *, expected_revision=None):
                self.config = config
                return config

        dict_store = DictSaveStore()
        app, _store, _version_store = self._build_app()
        with patch("bpmis_jira_tool.web_team_dashboard_routes.build_version_plan_store", return_value=dict_store):
            with app.test_client() as client:
                row_id = client.get("/api/team-dashboard/version-plan/af").get_json()["pipeline_rows"][0]["row_id"]
                response = client.post(
                    "/api/team-dashboard/version-plan/af/cell",
                    json={"scope": "pipeline", "row_id": row_id, "field": "remarks", "value": "dict"},
                )
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("store_backend", response.get_json())

        routes._VERSION_PLAN_SYNC_RUNNING = True
        app, _store, _version_store = self._build_app()
        try:
            with app.test_client() as client:
                response = client.post("/api/team-dashboard/version-plan/af/sync")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.get_json()["sync_queued"])
        finally:
            routes._VERSION_PLAN_SYNC_RUNNING = False

    def test_version_plan_classification_and_background_failure_edges(self):
        class ImmediateThread:
            def __init__(self, *, target, args=(), kwargs=None, **_ignored):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}

            def start(self):
                self.target(*self.args, **self.kwargs)

        raising_classifier = lambda _error: (_ for _ in ()).throw(RuntimeError("classifier failed"))
        app, _store, _version_store = self._build_app(bpmis_client=_FakeBPMISClient(ping_error=ToolError("proxy failed")), classify_portal_error=raising_classifier)
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/version-plan/af/sync")
        self.assertEqual(response.status_code, 400)
        self.assertIn("proxy failed", response.get_json()["sync_state"]["error"])

        local_classifier = lambda _error: {"error_category": "local_agent_unavailable"}
        app, _store, _version_store = self._build_app(bpmis_client=_FakeBPMISClient(ping_error=ToolError("proxy refused")), classify_portal_error=local_classifier)
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/version-plan/af?sync=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["sync_state"]["state"], "idle")

        app, _store, _version_store = self._build_app(bpmis_client=_FakeBPMISClient(ping_error=ToolError("local-agent refused")))
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/version-plan/af?sync=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["sync_state"]["state"], "idle")

        app, _store, _version_store = self._build_app()
        with patch("bpmis_jira_tool.web_team_dashboard_routes.threading.Thread", ImmediateThread), patch(
            "bpmis_jira_tool.web_team_dashboard_routes.version_plan_sync",
            side_effect=RuntimeError("background sync failed"),
        ):
            with app.test_client() as client:
                response = client.post("/api/team-dashboard/version-plan/af/sync")
        self.assertEqual(response.status_code, 200)

        app, _store, _version_store = self._build_app()
        with patch("bpmis_jira_tool.web_team_dashboard_routes.threading.Thread", ImmediateThread), patch(
            "bpmis_jira_tool.web_team_dashboard_routes.version_plan_sync",
            side_effect=lambda config, bpmis_client: config,
        ):
            with app.test_client() as client:
                response = client.get("/api/team-dashboard/version-plan/af?sync=1")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["sync_queued"])

        app, _store, _version_store = self._build_app(bpmis_client=_FakeBPMISClient(ping_error=ToolError("proxy refused")))
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/version-plan/af?sync=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["sync_state"]["state"], "error")

        app, _store, _version_store = self._build_app(
            bpmis_client=_FakeBPMISClient(ping_error=ToolError("proxy refused")),
            classify_portal_error=raising_classifier,
        )
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/version-plan/af?sync=1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["sync_state"]["state"], "error")

    def test_version_plan_store_access_and_sync_status_error_edges(self):
        class RuntimeLoadStore:
            def __init__(self, error):
                self.error = error

            def load_snapshot(self):
                raise self.error

        for message in ("gcloud credentials.db permission denied", "firestore permission denied"):
            app, _store, _version_store = self._build_app()
            with self.subTest(message=message), patch(
                "bpmis_jira_tool.web_team_dashboard_routes.build_version_plan_store",
                return_value=RuntimeLoadStore(RuntimeError(message)),
            ):
                with app.test_client() as client:
                    response = client.get("/api/team-dashboard/version-plan/af")
            self.assertEqual(response.status_code, 200)

        app, _store, _version_store = self._build_app()
        app.config["PROPAGATE_EXCEPTIONS"] = False
        with patch(
            "bpmis_jira_tool.web_team_dashboard_routes.build_version_plan_store",
            return_value=RuntimeLoadStore(RuntimeError("remote store crashed")),
        ):
            with app.test_client() as client:
                response = client.get("/api/team-dashboard/version-plan/af")
        self.assertEqual(response.status_code, 500)

        running_config = mark_version_plan_sync_running({"version_plan": normalize_version_plan_state({})})
        for error, expected_status in ((ToolError("stale sync tool failed"), 200), (RuntimeError("stale sync crashed"), 200)):
            fake_client = _FakeBPMISClient(ping_error=error)
            app, _store, _version_store = self._build_app(config=running_config, bpmis_client=fake_client)
            with self.subTest(error=type(error).__name__), app.test_client() as client:
                response = client.get("/api/team-dashboard/version-plan/af/sync-status")
            self.assertEqual(response.status_code, expected_status)
            self.assertEqual(response.get_json()["sync_state"]["state"], "error")
            self.assertEqual(fake_client.ping_count, 0)
            self.assertIn("interrupted", response.get_json()["sync_state"]["error"])

    def test_version_plan_unrecordable_sync_failure_edges(self):
        class ImmediateThread:
            def __init__(self, *, target, args=(), kwargs=None, **_ignored):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}

            def start(self):
                self.target(*self.args, **self.kwargs)

        class SaveFailureStore:
            def __init__(self, *, fail_after=0):
                self.config = {"version_plan": normalize_version_plan_state({})}
                self.save_calls = 0
                self.fail_after = fail_after

            def load_snapshot(self):
                return _Snapshot(self.config)

            def save_config(self, config, *, expected_revision=None):
                self.save_calls += 1
                if self.save_calls > self.fail_after:
                    raise RuntimeError("save failed")
                self.config = config
                return _Snapshot(self.config, f"rev-{self.save_calls}")

        app, _store, _version_store = self._build_app(bpmis_client=_FakeBPMISClient(ping_error=RuntimeError("sync start failed")))
        app.config["PROPAGATE_EXCEPTIONS"] = False
        with patch("bpmis_jira_tool.web_team_dashboard_routes.build_version_plan_store", return_value=SaveFailureStore(fail_after=0)):
            with app.test_client() as client:
                response = client.post("/api/team-dashboard/version-plan/af/sync")
        self.assertEqual(response.status_code, 500)

        background_store = SaveFailureStore(fail_after=1)
        app, _store, _version_store = self._build_app()
        with patch("bpmis_jira_tool.web_team_dashboard_routes.build_version_plan_store", return_value=background_store), patch(
            "bpmis_jira_tool.web_team_dashboard_routes.threading.Thread",
            ImmediateThread,
        ), patch("bpmis_jira_tool.web_team_dashboard_routes.version_plan_sync", side_effect=RuntimeError("background failed")):
            with app.test_client() as client:
                response = client.post("/api/team-dashboard/version-plan/af/sync")
        self.assertEqual(response.status_code, 200)

    def test_version_plan_success_json_and_validation_edges(self):
        class ImmediateThread:
            def __init__(self, *, target, args=(), kwargs=None, **_ignored):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}

            def start(self):
                self.target(*self.args, **self.kwargs)

        app, _store, _version_store = self._build_app()
        with patch("bpmis_jira_tool.web_team_dashboard_routes.threading.Thread", ImmediateThread), patch(
            "bpmis_jira_tool.web_team_dashboard_routes.version_plan_sync",
            side_effect=lambda config, bpmis_client: config,
        ):
            with app.test_client() as client:
                sync_response = client.post("/api/team-dashboard/version-plan/af/sync")
                status_response = client.get("/api/team-dashboard/version-plan/af/sync-status")
        app, _store, _version_store = self._build_app()
        with app.test_client() as client:
            bad_cell = client.post("/api/team-dashboard/version-plan/af/cell", data="not json", content_type="text/plain")
            bad_rows = client.post("/api/team-dashboard/version-plan/af/rows", data="not json", content_type="text/plain")
            invalid_cell = client.post("/api/team-dashboard/version-plan/af/cell", json={"scope": "pipeline", "row_id": "missing", "field": "owner"})
            invalid_rows = client.post("/api/team-dashboard/version-plan/af/rows", json={"scope": "bad", "action": "add"})

        self.assertEqual(sync_response.status_code, 200)
        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(bad_cell.status_code, 400)
        self.assertEqual(bad_rows.status_code, 400)
        self.assertEqual(invalid_cell.status_code, 400)
        self.assertEqual(invalid_rows.status_code, 400)

        app, _store, _version_store = self._build_app(can_manage=False)
        with app.test_client() as client:
            denied = client.post("/api/team-dashboard/version-plan/af/sync")
        self.assertEqual(denied.status_code, 403)

    def test_monthly_report_style_latest_daily_and_send_edges(self):
        app, _store, _version_store = self._build_app(
            has_gmail_scope=True,
            remote_enabled=True,
            seatalk_enabled=True,
        )
        with app.test_client() as client:
            style_response = client.post("/api/team-dashboard/monthly-report/style-guide/refresh")
            latest_response = client.get("/api/team-dashboard/monthly-report/latest-draft")
            briefs_response = client.get("/api/team-dashboard/daily-briefs")
            download_response = client.get("/api/team-dashboard/daily-briefs/b1/download")
            draft_response = client.post("/api/team-dashboard/monthly-report/draft", json={"highlight_topics": "AF"})
            send_response = client.post("/api/team-dashboard/monthly-report/send", json={"draft_markdown": "Draft"})

        self.assertEqual(style_response.status_code, 200)
        self.assertEqual(latest_response.get_json()["draft_markdown"], "Remote draft")
        self.assertEqual(briefs_response.get_json()["briefs"][0]["brief_id"], "b1")
        self.assertEqual(download_response.status_code, 200)
        self.assertEqual(draft_response.get_json()["job_backend"], "local_agent")
        self.assertEqual(send_response.get_json()["message_id"], "remote-msg")

        app, _store, _version_store = self._build_app(
            has_gmail_scope=True,
            history_refresh_result={"scanned": 8, "matched": 8, "report_count": 8, "items": [{"summary": f"Remote {index}", "content": "Highlights\n- A"} for index in range(1, 9)]},
        )
        with app.test_client() as client:
            duplicate_response = client.post("/api/team-dashboard/monthly-report/style-guide/refresh")
        self.assertEqual(duplicate_response.status_code, 200)

        app, _store, _version_store = self._build_app(
            current_email="",
        )
        with app.test_client() as client:
            no_owner_style = client.post("/api/team-dashboard/monthly-report/style-guide/refresh")
            no_owner_draft = client.post("/api/team-dashboard/monthly-report/draft", json={})
        self.assertEqual(no_owner_style.status_code, 200)
        self.assertEqual(no_owner_draft.status_code, 200)

        app, _store, _version_store = self._build_app(has_gmail_scope=True, history_refresh_error=RuntimeError("style lookup failed"))
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/monthly-report/style-guide/refresh")
        self.assertEqual(response.status_code, 200)

        app, _store, _version_store = self._build_app(monthly_access_gate=("blocked", 403))
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/monthly-report/style-guide/refresh")
        self.assertEqual(response.status_code, 403)

        app, _store, _version_store = self._build_app()
        with patch(
            "bpmis_jira_tool.web_team_dashboard_routes.build_monthly_report_historical_style_guide",
            side_effect=ToolError("style unavailable"),
        ):
            with app.test_client() as client:
                response = client.post("/api/team-dashboard/monthly-report/style-guide/refresh")
        self.assertEqual(response.status_code, 400)

    def test_monthly_report_access_local_send_and_error_edges(self):
        app, _store, _version_store = self._build_app(monthly_access_gate=("blocked", 403))
        with app.test_client() as client:
            self.assertEqual(client.get("/api/team-dashboard/monthly-report/template").status_code, 403)
            self.assertEqual(client.get("/api/team-dashboard/monthly-report/latest-draft").status_code, 403)
            self.assertEqual(client.get("/api/team-dashboard/daily-briefs").status_code, 403)
            self.assertEqual(client.get("/api/team-dashboard/daily-briefs/missing/download").status_code, 403)

        send_calls = []

        def send_email(**kwargs):
            send_calls.append(kwargs)
            return MonthlyReportSendResult(status="ok", recipient=kwargs["recipient"], subject=kwargs["subject"], message_id="local-msg")

        app, _store, _version_store = self._build_app(send_email=send_email, has_gmail_scope=True, history_refresh_error=RuntimeError("history failed"))
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/monthly-report/send", json={"draft_markdown": "Draft", "subject": "Subject", "recipient": "to@npt.sg"})
        self.assertEqual(response.get_json()["message_id"], "local-msg")
        self.assertEqual(response.get_json()["monthly_report_history"], {"scanned": 0, "matched": 0, "report_count": 0})
        self.assertEqual(send_calls[0]["recipient"], "to@npt.sg")

        app, _store, _version_store = self._build_app(send_email=lambda **_kwargs: (_ for _ in ()).throw(ToolError("send rejected")))
        with app.test_client() as client:
            tool_error = client.post("/api/team-dashboard/monthly-report/send", json={"draft_markdown": "Draft"})
        self.assertEqual(tool_error.status_code, 400)

        app, _store, _version_store = self._build_app(send_email=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("smtp down")))
        with app.test_client() as client:
            unexpected = client.post("/api/team-dashboard/monthly-report/send", json={"draft_markdown": "Draft"})
        self.assertEqual(unexpected.status_code, 500)

    def test_monthly_report_draft_remote_thread_and_error_edges(self):
        no_job_agent = _FakeLocalAgentClient()
        no_job_agent.team_dashboard_monthly_report_draft_start = lambda _payload: {}
        app, _store, _version_store = self._build_app(remote_enabled=True, local_agent=no_job_agent)
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/monthly-report/draft", json={})
        self.assertEqual(response.status_code, 400)

        class NoopThread:
            def __init__(self, *, target, args=(), kwargs=None, **_ignored):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}
                self.started = False

            def start(self):
                self.started = True

        app, _store, _version_store = self._build_app()
        app.testing = False
        with patch("bpmis_jira_tool.web_team_dashboard_routes.threading.Thread", NoopThread):
            with app.test_client() as client:
                response = client.post("/api/team-dashboard/monthly-report/draft", json={"highlight_topics": ["AF"]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "queued")

        app, _store, _version_store = self._build_app(load_all_error=ToolError("monthly context unavailable"))
        with app.test_client() as client:
            tool_error = client.post("/api/team-dashboard/monthly-report/draft", json={})
        self.assertEqual(tool_error.status_code, 400)

        app, _store, _version_store = self._build_app(load_all_error=RuntimeError("monthly context crashed"))
        with app.test_client() as client:
            unexpected = client.post("/api/team-dashboard/monthly-report/draft", json={})
        self.assertEqual(unexpected.status_code, 500)

    def test_daily_brief_local_not_found_and_admin_save_forms(self):
        daily_store = SimpleNamespace(list_recent=lambda limit=30: [{"brief_id": "local", "subject": "Local", "sent_at": "now"}], get=lambda brief_id: None)
        app, _store, _version_store = self._build_app(
            daily_store=daily_store,
            config={"task_cache": {"teams": {}}, "version_plan": normalize_version_plan_state({})},
        )
        with app.test_client() as client:
            briefs = client.get("/api/team-dashboard/daily-briefs")
            missing = client.get("/api/team-dashboard/daily-briefs/missing/download")
            members = client.post("/admin/team-dashboard/members", data={"team_dashboard_members_AF": "pm@npt.sg"})
            template = client.post("/admin/team-dashboard/monthly-report-template", data={"monthly_report_template": "# Template"})
            report_intelligence = client.post("/admin/team-dashboard/report-intelligence", json={"vip_people": [{"display_name": "Boss", "emails": ["boss@npt.sg"]}]})

        self.assertEqual(briefs.get_json()["briefs"][0]["brief_id"], "local")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(members.status_code, 200)
        self.assertIn("task_cache", _store.config)
        self.assertEqual(template.status_code, 200)
        self.assertEqual(report_intelligence.status_code, 200)

        app, _store, _version_store = self._build_app(can_manage=False)
        with app.test_client() as client:
            members_denied = client.post("/admin/team-dashboard/members", json={"teams": {}})
            template_denied = client.post("/admin/team-dashboard/monthly-report-template", json={"template": "# Template"})
            report_intelligence_denied = client.post("/admin/team-dashboard/report-intelligence", json={})
        self.assertEqual(members_denied.status_code, 403)
        self.assertEqual(template_denied.status_code, 403)
        self.assertEqual(report_intelligence_denied.status_code, 403)

    def test_monthly_report_local_latest_empty_and_legacy(self):
        app, _store, _version_store = self._build_app()
        with app.test_client() as client:
            empty_response = client.get("/api/team-dashboard/monthly-report/latest-draft")
            app.config["JOB_STORE"].create("team-dashboard-monthly-report-draft", "old")
            job = app.config["JOB_STORE"].create("team-dashboard-monthly-report-draft", "legacy")
            app.config["JOB_STORE"].complete(job.job_id, results=[{"draft_markdown": "Legacy draft"}], notice={})
            legacy_response = client.get("/api/team-dashboard/monthly-report/latest-draft")
        self.assertEqual(empty_response.get_json()["status"], "empty")
        self.assertEqual(legacy_response.get_json()["status"], "empty")

    def test_seatalk_name_mapping_success_and_errors(self):
        auto_service = SimpleNamespace(
            build_name_mappings=lambda force_refresh=False: {
                "auto_mappings": {"buddy-1": "Alice"},
                "unknown_ids": [{"id": "buddy-2"}],
            }
        )
        app, _store, _version_store = self._build_app(seatalk_service=auto_service)
        with app.test_client() as client:
            auto_response = client.get("/api/team-dashboard/report-intelligence/seatalk/name-mappings")
            post_response = client.post("/api/team-dashboard/report-intelligence/seatalk/name-mappings", json={"mappings": {"A": "Alice"}})
            get_response = client.get("/api/team-dashboard/report-intelligence/seatalk/name-mappings?refresh=1")
        self.assertEqual(auto_response.get_json()["mappings"], {"UID 1": "Alice", "buddy-1": "Alice"})
        self.assertEqual(post_response.get_json()["mappings"], {"A": "Alice"})
        self.assertEqual(get_response.status_code, 200)

        failing_service = SimpleNamespace(build_name_mappings=lambda force_refresh=False: (_ for _ in ()).throw(ToolError("mapping unavailable")))
        app, _store, _version_store = self._build_app(seatalk_service=failing_service)
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/report-intelligence/seatalk/name-mappings")
        self.assertEqual(response.status_code, 400)

        app, _store, _version_store = self._build_app(monthly_access_gate=("blocked", 403))
        with app.test_client() as client:
            self.assertEqual(client.get("/api/team-dashboard/report-intelligence/seatalk/name-mappings").status_code, 403)

        crashing_service = SimpleNamespace(build_name_mappings=lambda force_refresh=False: (_ for _ in ()).throw(RuntimeError("mapping crashed")))
        app, _store, _version_store = self._build_app(seatalk_service=crashing_service)
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/report-intelligence/seatalk/name-mappings")
        self.assertEqual(response.status_code, 500)

    def test_tasks_route_cache_reload_success_and_error_edges(self):
        app, _store, _version_store = self._build_app(cached_team={"team_key": "AF", "label": "Anti-Fraud", "member_emails": [], "under_prd": [], "pending_live": []})
        with app.test_client() as client:
            cached = client.get("/api/team-dashboard/tasks?team=AF")
            unknown = client.get("/api/team-dashboard/tasks?team=UNKNOWN")
        self.assertEqual(cached.get_json()["team"]["team_key"], "AF")
        self.assertEqual(unknown.status_code, 400)

        app, _store, _version_store = self._build_app(tasks=[{"bpmis_id": "225159"}], pending_mandays=["225159"])
        with app.test_client() as client:
            reload_response = client.get("/api/team-dashboard/tasks?reload=1")
        self.assertEqual(reload_response.get_json()["status"], "ok")

        app, _store, _version_store = self._build_app()
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/tasks?team=AF&reload=1")
        self.assertIn(response.get_json()["status"], {"ok", "partial"})

        app, _store, _version_store = self._build_app(task_error=RuntimeError("team load failed"))
        with app.test_client() as client:
            partial = client.get("/api/team-dashboard/tasks?team=AF&reload=1")
        self.assertEqual(partial.get_json()["status"], "partial")

        app, _store, _version_store = self._build_app(all_team_reload_error=RuntimeError("all reload failed"), task_error=RuntimeError("team load failed"))
        with app.test_client() as client:
            response = client.get("/api/team-dashboard/tasks?reload=1")
        self.assertEqual(response.get_json()["status"], "partial")

    def test_key_project_validation_edges(self):
        app, _store, _version_store = self._build_app(can_manage=False)
        with app.test_client() as client:
            denied = client.post("/api/team-dashboard/key-projects", json={"bpmis_id": "225159", "is_key_project": True})
        self.assertEqual(denied.status_code, 403)

        app, _store, _version_store = self._build_app()
        with app.test_client() as client:
            missing_id = client.post("/api/team-dashboard/key-projects", json={"is_key_project": True})
            missing_value = client.post("/api/team-dashboard/key-projects", json={"bpmis_id": "225159"})
            saved = client.post("/api/team-dashboard/key-projects", json={"bpmis_id": "225159", "is_key_project": True, "priority": "P0"})
        self.assertEqual(missing_id.status_code, 400)
        self.assertEqual(missing_value.status_code, 400)
        self.assertEqual(saved.status_code, 200)

    def test_link_biz_project_and_prd_routes_edges(self):
        app, _store, _version_store = self._build_app()
        with app.test_client() as client:
            link_rows = client.get("/api/team-dashboard/link-biz-projects")
            self.assertEqual(client.post("/api/team-dashboard/link-biz-projects", json={}).status_code, 400)
            self.assertEqual(client.post("/api/team-dashboard/link-biz-projects", json={"jira_id": "AF-1"}).status_code, 400)
            self.assertEqual(client.post("/api/team-dashboard/link-biz-projects", json={"jira_id": "AF-1", "bpmis_id": "225159"}).status_code, 400)
            linked = client.post("/api/team-dashboard/link-biz-projects", json={"jira_id": "AF-1", "bpmis_id": "225159", "reporter_email": "pm@npt.sg"})
            suggestions = client.post("/api/team-dashboard/link-biz-projects/suggestions", json={"rows": [{"jira_id": "AF-1"}], "team_payloads": []})
            jira_rows = client.get("/api/team-dashboard/link-biz-projects/jira")
            review = client.post("/api/team-dashboard/prd-review", json={"jira_id": "AF-1", "prd_url": "https://docs/prd"})
            summary = client.post("/api/team-dashboard/prd-summary", json={"jira_id": "AF-1", "prd_url": "https://docs/prd"})
            queued = client.post("/api/team-dashboard/prd-review", json={"jira_id": "AF-1", "prd_url": "https://docs/prd", "async": True})

        self.assertEqual(link_rows.status_code, 200)
        self.assertEqual(linked.status_code, 200)
        self.assertEqual(suggestions.status_code, 200)
        self.assertEqual(jira_rows.status_code, 200)
        self.assertEqual(review.get_json()["status"], "ok")
        self.assertEqual(summary.get_json()["status"], "ok")
        self.assertEqual(queued[1] if isinstance(queued, tuple) else queued.status_code, 202)

        app, _store, _version_store = self._build_app(team_dashboard_access_gate=("blocked", 403))
        with app.test_client() as client:
            self.assertEqual(client.post("/api/team-dashboard/link-biz-projects", json={"jira_id": "AF-1", "bpmis_id": "225159", "reporter_email": "pm@npt.sg"}).status_code, 403)

        app, _store, _version_store = self._build_app(can_manage=False)
        with app.test_client() as client:
            self.assertEqual(client.get("/api/team-dashboard/link-biz-projects").status_code, 403)
            self.assertEqual(client.get("/api/team-dashboard/link-biz-projects/jira").status_code, 403)
            self.assertEqual(client.post("/api/team-dashboard/link-biz-projects/suggestions", json={"rows": []}).status_code, 403)

    def test_link_route_load_errors_and_unexpected_link_failure(self):
        app, _store, _version_store = self._build_app(link_rows_error=RuntimeError("load failed"))
        with app.test_client() as client:
            self.assertEqual(client.get("/api/team-dashboard/link-biz-projects").status_code, 500)
            self.assertEqual(client.get("/api/team-dashboard/link-biz-projects/jira").status_code, 500)

        app, _store, _version_store = self._build_app(suggestion_error=RuntimeError("suggest failed"))
        with app.test_client() as client:
            self.assertEqual(client.post("/api/team-dashboard/link-biz-projects/suggestions", json={"rows": []}).status_code, 500)

        app, _store, _version_store = self._build_app(candidate_error=RuntimeError("candidate failed"))
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/link-biz-projects", json={"jira_id": "AF-1", "bpmis_id": "225159", "reporter_email": "pm@npt.sg"})
        self.assertEqual(response.status_code, 500)

        no_detail_client = _FakeBPMISClient(issue_detail={})
        no_detail_client.get_issue_detail = lambda _bpmis_id: (_ for _ in ()).throw(RuntimeError("detail down"))
        app, _store, _version_store = self._build_app(bpmis_client=no_detail_client)
        with app.test_client() as client:
            response = client.post(
                "/api/team-dashboard/link-biz-projects",
                json={"jira_id": "AF-1", "bpmis_id": "225159", "reporter_email": "pm@npt.sg", "selected_project_title": "Fallback Project"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["project"]["project_name"], "Fallback Project")

    def test_link_and_prd_error_edges(self):
        bad_client = _FakeBPMISClient(link_detail={"parentIds": []})
        app, _store, _version_store = self._build_app(bpmis_client=bad_client)
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/link-biz-projects", json={"jira_id": "AF-1", "bpmis_id": "225159", "reporter_email": "pm@npt.sg"})
        self.assertEqual(response.status_code, 400)

        failing_prd = SimpleNamespace(
            review=lambda request: (_ for _ in ()).throw(ToolError("PRD link is required.")),
            summarize=lambda request: (_ for _ in ()).throw(RuntimeError("summary failed")),
        )
        app, _store, _version_store = self._build_app(prd_service=failing_prd)
        with app.test_client() as client:
            review_response = client.post("/api/team-dashboard/prd-review", json={"jira_id": "AF-1"})
            summary_response = client.post("/api/team-dashboard/prd-summary", json={"jira_id": "AF-1"})
        self.assertEqual(review_response.status_code, 400)
        self.assertEqual(summary_response.status_code, 500)

        failing_summary = SimpleNamespace(
            review=lambda request: {"status": "ok"},
            summarize=lambda request: (_ for _ in ()).throw(ToolError("PRD summary link is required.")),
        )
        app, _store, _version_store = self._build_app(prd_service=failing_summary)
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/prd-summary", json={"jira_id": "AF-1"})
        self.assertEqual(response.status_code, 400)

        app, _store, _version_store = self._build_app(can_manage=False)
        with app.test_client() as client:
            self.assertEqual(client.post("/api/team-dashboard/prd-summary", json={"jira_id": "AF-1", "prd_url": "https://docs/prd"}).status_code, 403)

    def test_prd_local_agent_and_google_credentials_edges(self):
        seen_payloads = []

        class RecordingPRDService:
            def review(self, request):
                seen_payloads.append(request)
                return {"status": "ok", "cached": False}

            def summarize(self, request):
                seen_payloads.append(request)
                return {"status": "ok", "cached": False}

        app, _store, _version_store = self._build_app(prd_service=RecordingPRDService(), has_gmail_scope=True)
        with app.test_client() as client:
            with client.session_transaction() as session:
                session["google_credentials"] = {"token": "drive-token"}
            response = client.post("/api/team-dashboard/prd-review", json={"jira_id": "AF-1", "prd_url": "https://docs/prd"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(seen_payloads[0].google_credentials["token"], "drive-token")

        app, _store, _version_store = self._build_app(source_code_qa_enabled=True)
        with app.test_client() as client:
            review = client.post("/api/team-dashboard/prd-review", json={"jira_id": "AF-1", "prd_url": "https://docs/prd"})
            summary = client.post("/api/team-dashboard/prd-summary", json={"jira_id": "AF-1", "prd_url": "https://docs/prd"})
        self.assertEqual(review.get_json()["status"], "ok")
        self.assertEqual(summary.get_json()["status"], "ok")

        app, _store, _version_store = self._build_app(prd_service=SimpleNamespace(review=lambda request: (_ for _ in ()).throw(RuntimeError("review failed")), summarize=lambda request: {"status": "ok"}))
        with app.test_client() as client:
            response = client.post("/api/team-dashboard/prd-review", json={"jira_id": "AF-1", "prd_url": "https://docs/prd"})
        self.assertEqual(response.status_code, 500)


if __name__ == "__main__":
    unittest.main()
