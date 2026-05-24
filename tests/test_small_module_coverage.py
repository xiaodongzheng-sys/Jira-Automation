import tempfile
import time
import unittest
import hashlib
from collections import deque
from datetime import datetime, timezone
import inspect
import re
from pathlib import Path
from unittest.mock import Mock, patch

from flask import Flask, current_app
import json
from types import SimpleNamespace

from bpmis_jira_tool.bpmis_client import build_bpmis_client
from bpmis_jira_tool.background_jobs import fail_job_if_active, start_durable_job_thread
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.job_lifecycle import JobLifecycle
from bpmis_jira_tool.job_store import JobState, JobStore
from bpmis_jira_tool.local_agent_protocol import verify_signature
from bpmis_jira_tool.models import InputRow
from bpmis_jira_tool.seatalk_stores import SeaTalkNameMappingStore, SeaTalkTodoStore
from bpmis_jira_tool import web_source_code_qa_effort as effort_helpers
from bpmis_jira_tool import web_source_code_qa_jobs
from bpmis_jira_tool.web_prd_self_assessment_routes import build_prd_self_assessment_handlers
from bpmis_jira_tool import web_productization_helpers as productization_helpers
from bpmis_jira_tool import web_team_dashboard_helpers as team_dashboard_helpers
from bpmis_jira_tool.web_productization_routes import build_productization_handlers, register_productization_routes
from bpmis_jira_tool.web_gmail_seatalk_routes import build_gmail_seatalk_handlers, register_gmail_seatalk_routes
from bpmis_jira_tool.web_bpmis_routes import build_bpmis_handlers, register_bpmis_routes
from bpmis_jira_tool.source_code_qa_cache_telemetry import (
    _answer_cache_metadata,
    _codex_telemetry_summary,
    _load_cached_answer,
    _log_source_code_qa_timing,
    _record_query_telemetry,
    _store_cached_answer,
)
from bpmis_jira_tool.source_code_qa_codex_refs import (
    codex_candidate_path_layers,
    codex_repo_relative_root,
    codex_resolved_file_ref_payload,
    extract_direct_file_refs,
    resolve_codex_file_ref,
)
from bpmis_jira_tool.source_code_qa_codex_prompts import (
    build_codex_payload,
    build_codex_repair_brief,
    build_codex_sql_generation_brief,
    codex_system_instruction,
)
from bpmis_jira_tool.source_code_qa_codex_answer import (
    _log_source_code_qa_timing as _log_codex_answer_timing,
    build_codex_llm_answer as build_codex_llm_answer_payload,
)
from bpmis_jira_tool.source_code_qa_llm_pipeline import build_codex_initial_plan
from bpmis_jira_tool.source_code_qa_factory import source_code_qa_data_root
from bpmis_jira_tool.source_code_qa_answer_generation import build_llm_answer
from bpmis_jira_tool.source_code_qa_jobs import SourceCodeQAQueryScheduler
from bpmis_jira_tool.source_code_qa_match_grading import evidence_role, match_answer_grade, match_is_definition_only
from bpmis_jira_tool.source_code_qa_retrieval_cache import (
    _clone_jsonish,
    _increment_retrieval_stat,
    _new_retrieval_request_cache,
    _query_phase_timing_stats,
    _record_query_phase_timing,
    _retrieval_cache_stats,
    attach_retrieval_cache_helpers,
)
from bpmis_jira_tool.source_code_qa_sql_artifacts import (
    build_source_code_qa_sql_readme,
    clean_source_code_qa_sql_identifier,
    extract_source_code_qa_sql_blocks,
    extract_source_code_qa_inline_sql,
    format_source_code_qa_sql_text,
    normalize_source_code_qa_sql_text,
    source_code_qa_answer_text_candidates,
    source_code_qa_sql_ctes,
    source_code_qa_sql_logic_summary,
    source_code_qa_sql_tables,
)
from bpmis_jira_tool.source_code_qa_types import SourceCodeQALLMError
from bpmis_jira_tool.team_dashboard_config import (
    TEAM_DASHBOARD_LEGACY_DEFAULT_MEMBER_EMAILS,
    TEAM_DASHBOARD_TASK_CACHE_VERSION,
    TeamDashboardConfigStore,
    normalize_team_dashboard_emails,
)
from bpmis_jira_tool.web_health import build_health_payload
from bpmis_jira_tool.web_redirects import portal_health_url, safe_relative_redirect_target, url_with_query_value


def _settings(**overrides):
    values = {
        "flask_secret_key": "secret",
        "google_oauth_client_secret_file": Path("/tmp/client.json"),
        "google_oauth_redirect_uri": None,
        "team_portal_host": "127.0.0.1",
        "team_portal_port": 5000,
        "team_portal_base_url": None,
        "team_allowed_emails": (),
        "team_allowed_email_domains": (),
        "team_portal_data_dir": Path("/workspace/team-portal-runtime"),
        "spreadsheet_id": "sheet",
        "common_tab_name": "Common",
        "input_tab_name": "Sheet1",
        "bpmis_base_url": "https://bpmis.example",
        "bpmis_api_access_token": None,
    }
    values.update(overrides)
    return Settings(**values)


class SmallModuleCoverageTests(unittest.TestCase):
    def test_productization_helpers_normalize_rows_and_team_filter(self):
        row = {
            "fields": {
                "jiraLink": "AF-123",
                "summary": {"value": "Rule upgrade"},
                "description": "Details",
                "component": [{"name": "DBP-Anti-Fraud"}],
                "prdLinks": "https://confluence/prd, https://confluence/prd",
            },
            "row": {"pm": {"emailAddress": "pm@npt.sg"}},
        }

        normalized = productization_helpers.normalize_productization_issue_row(
            row,
            description_formatter=lambda value: f"formatted:{value}",
        )
        filtered_rows, metadata = productization_helpers.filter_productization_issue_rows_for_pm_team(
            [row, {"component": "Other"}],
            {"pm_team": "AF"},
        )

        self.assertEqual(normalized["jira_ticket_number"], "AF-123")
        self.assertEqual(normalized["jira_ticket_url"], "https://jira.shopee.io/browse/AF-123")
        self.assertEqual(normalized["feature_summary"], "Rule upgrade")
        self.assertEqual(normalized["detailed_feature"], "formatted:Details")
        self.assertEqual(normalized["pm"], "pm@npt.sg")
        self.assertEqual(normalized["prd_links"], [{"label": "https://confluence/prd", "url": "https://confluence/prd"}])
        self.assertEqual(filtered_rows, [row])
        self.assertTrue(metadata["team_filter_applied"])

    def test_team_dashboard_helpers_group_projects_and_cache_mandays(self):
        tasks = [
            team_dashboard_helpers.normalize_team_dashboard_task(
                {
                    "jira_id": "AF-2",
                    "jira_title": "Second",
                    "pm_email": "PM@NPT.SG",
                    "release_date": "2026/06/02",
                    "parent_project": {"bpmis_id": "B1", "project_name": "Beta", "priority": "SP"},
                }
            ),
            team_dashboard_helpers.normalize_team_dashboard_task(
                {
                    "jira_id": "AF-1",
                    "jira_title": "First",
                    "pm_email": "pm@npt.sg",
                    "release_date": "2026-06-01",
                    "parent_project": {"bpmis_id": "B1", "project_name": "Beta"},
                }
            ),
        ]
        projects = team_dashboard_helpers.group_team_dashboard_tasks_by_project(tasks, sort_by_release=True)
        team_dashboard_helpers.apply_team_dashboard_key_project_states(projects, {})
        pending_ids = team_dashboard_helpers.apply_team_dashboard_actual_mandays_cache(
            {"actual_mandays_cache": {"projects": {"B1": {"value": "3.0", "cached_at": team_dashboard_helpers.team_dashboard_timestamp()}}}},
            [{"under_prd": projects, "pending_live": []}],
        )

        self.assertEqual(projects[0]["bpmis_id"], "B1")
        self.assertEqual(projects[0]["jira_tickets"][0]["jira_id"], "AF-1")
        self.assertEqual(projects[0]["release_date"], "2026-06-02")
        self.assertTrue(projects[0]["is_key_project"])
        self.assertEqual(projects[0]["actual_mandays"], 3)
        self.assertEqual(pending_ids, [])

    def test_bpmis_client_factory_uses_direct_client_when_local_agent_disabled(self):
        settings = _settings(
            local_agent_base_url="https://agent.example",
            local_agent_hmac_secret="shared-secret",
            local_agent_bpmis_enabled=False,
            bpmis_call_mode="local_agent",
        )
        with patch("bpmis_jira_tool.bpmis_client.BPMISDirectApiClient", return_value="direct") as direct:
            client = build_bpmis_client(settings, access_token="user-token")

        self.assertEqual(client, "direct")
        direct.assert_called_once_with(settings, access_token="user-token")

    def test_source_code_qa_data_root_resolves_relative_paths_from_repo_root(self):
        settings = _settings(team_portal_data_dir=Path(".team-portal-test"))

        data_root = source_code_qa_data_root(settings)

        self.assertTrue(data_root.is_absolute())
        self.assertEqual(data_root.name, ".team-portal-test")
        self.assertEqual(data_root.parent, Path(__file__).resolve().parents[1])

    def test_local_agent_signature_rejects_missing_secret_bad_timestamp_and_nonce(self):
        with self.assertRaisesRegex(ToolError, "HMAC_SECRET"):
            verify_signature(secret="", method="POST", path="/x", body=b"{}", timestamp=str(int(time.time())), nonce="n", signature="s")
        with self.assertRaisesRegex(ToolError, "timestamp"):
            verify_signature(secret="secret", method="POST", path="/x", body=b"{}", timestamp="bad", nonce="n", signature="s")
        with self.assertRaisesRegex(ToolError, "nonce"):
            verify_signature(secret="secret", method="POST", path="/x", body=b"{}", timestamp=str(int(time.time())), nonce="", signature="s")

    def test_input_row_column_lookup_rejects_invalid_and_out_of_range_columns(self):
        row = InputRow(
            row_number=2,
            values={" Issue ID ": " AF-1 ", "Jira Created?": " yes ", "Jira Ticket Link": " https://jira/AF-1 "},
            ordered_values=("a", " b "),
        )

        self.assertEqual(row.issue_id, "AF-1")
        self.assertEqual(row.jira_created, "yes")
        self.assertEqual(row.jira_ticket_link, "https://jira/AF-1")
        self.assertEqual(row.get_by_column_letter("B"), "b")
        self.assertEqual(row.get_by_column_letter(""), "")
        self.assertEqual(row.get_by_column_letter("A1"), "")
        self.assertEqual(row.get_by_column_letter("ZZ"), "")

    def test_source_code_qa_error_preserves_provider_metadata(self):
        error = SourceCodeQALLMError(
            "provider failed",
            status_code=429,
            provider_status="rate_limited",
            retryable=True,
            retry_after_seconds=1.5,
        )

        self.assertEqual(str(error), "provider failed")
        self.assertEqual(error.status_code, 429)
        self.assertEqual(error.provider_status, "rate_limited")
        self.assertTrue(error.retryable)
        self.assertEqual(error.retry_after_seconds, 1.5)

    def test_match_grading_covers_definition_api_config_test_and_data_source_roles(self):
        self.assertTrue(match_is_definition_only({"path": "src/StatusEnum.java", "snippet": ""}, []))
        self.assertTrue(match_is_definition_only({"path": "src/User.java", "snippet": "interface User"}, []))
        self.assertTrue(match_is_definition_only({"path": "src/User.java", "snippet": "public User"}, []))
        self.assertFalse(match_is_definition_only({"path": "src/UserMapper.java", "snippet": "public UserMapper repository"}, []))
        self.assertEqual(evidence_role("src/UserMapper.xml", "select id from user", ""), "data_source")
        self.assertEqual(evidence_role("src/UserController.java", "@GetMapping", ""), "api")
        self.assertEqual(evidence_role("application.properties", "apollo key", ""), "config")
        self.assertEqual(evidence_role("src/UserTest.java", "assert", ""), "test")
        self.assertEqual(evidence_role("src/Status.java", "constant", ""), "definition")
        self.assertFalse(match_answer_grade({"path": "src/UserTest.java", "snippet": "assert"}))
        self.assertTrue(match_answer_grade({"path": "src/UserController.java", "snippet": "@GetMapping"}, intent_label="data_source"))

    def test_source_code_qa_scheduler_handles_default_runner_and_empty_queues(self):
        job_store = Mock()
        job_store.p95_duration_seconds.return_value = 120
        scheduler = SourceCodeQAQueryScheduler(job_store=job_store, max_running=0)
        app = Flask(__name__)

        scheduler._running = {"job-1"}
        scheduler._running_users = {"owner@npt.sg": 1}
        scheduler._run_job(app, "job-1", {}, "owner@npt.sg", None)
        job_store.fail.assert_called_once()
        self.assertEqual(job_store.fail.call_args.args[0], "job-1")
        self.assertIn("requires a default runner", job_store.fail.call_args.args[1])
        self.assertEqual(job_store.fail.call_args.kwargs["error_code"], "background_worker_unhandled_exception")
        self.assertNotIn("job-1", scheduler._running)
        self.assertNotIn("owner@npt.sg", scheduler._running_users)

        scheduler._user_order = deque(["empty"])
        scheduler._user_queues = {"empty": deque()}
        self.assertIsNone(scheduler._pop_next_locked())

        scheduler._user_order = deque(["missing"])
        scheduler._user_queues = {}
        self.assertIsNone(scheduler._pop_next_locked())

        scheduler._user_order = deque(["user"])
        scheduler._user_queues = {"user": deque([("job-2", app, {}, None), ("job-3", app, {}, None)])}
        selected = scheduler._pop_next_locked()
        self.assertEqual(selected[1], "job-2")
        self.assertIn("user", scheduler._user_order)

        scheduler._user_order = deque(["ghost", "user"])
        scheduler._user_queues = {"user": deque([("job-4", app, {}, None)])}
        self.assertEqual(scheduler._simulated_round_robin_locked(), [("job-4", "user")])

        scheduler._user_order = deque(["empty"])
        scheduler._user_queues = {"empty": deque()}
        self.assertEqual(scheduler._simulated_round_robin_locked(), [])

        scheduler._user_order = deque(["user"])
        scheduler._user_queues = {
            "user": deque([("job-5", app, {}, None), ("job-6", app, {}, None)]),
        }
        self.assertEqual(
            scheduler._simulated_round_robin_locked(),
            [("job-5", "user"), ("job-6", "user")],
        )

    def test_source_code_qa_scheduler_submit_start_and_finish_updates_metadata(self):
        class FakeThread:
            created = []

            def __init__(self, *, target, args, daemon):
                self.target = target
                self.args = args
                self.daemon = daemon
                self.started = False
                FakeThread.created.append(self)

            def start(self):
                self.started = True

        job_store = Mock()
        job_store.p95_duration_seconds.return_value = 90
        scheduler = SourceCodeQAQueryScheduler(job_store=job_store, max_running=1)
        app = Flask(__name__)

        with patch("bpmis_jira_tool.source_code_qa_jobs.threading.Thread", FakeThread):
            scheduler.submit(app=app, job_id="job-1", payload={}, owner_email="")
            scheduler.submit(app=app, job_id="job-2", payload={"query_mode": "balanced"}, owner_email="Owner@NPT.SG")
            scheduler.finish("job-1", "")

        self.assertEqual(job_store.set_owner.call_args_list[0].args, ("job-1", "local"))
        self.assertEqual(job_store.set_query_mode.call_args_list[0].args, ("job-1", "deep"))
        self.assertTrue(FakeThread.created[0].started)
        self.assertTrue(FakeThread.created[1].started)
        self.assertEqual(scheduler._running, {"job-2"})
        self.assertEqual(scheduler._running_users, {"owner@npt.sg": 1})
        queued_messages = [call.kwargs.get("message") for call in job_store.update_queue_metadata.call_args_list]
        self.assertIn("Starting Source Code Q&A job.", queued_messages)

    def test_durable_background_job_thread_marks_unhandled_exception_failed(self):
        class FakeThread:
            def __init__(self, *, target, name=None, daemon=True):
                self.target = target
                self.name = name
                self.daemon = daemon

            def start(self):
                self.target()

        job_store = Mock()
        job_store.snapshot.return_value = {"job_id": "job-1", "state": "running"}
        app = Flask(__name__)

        def broken():
            raise RuntimeError("boom")

        with patch("bpmis_jira_tool.background_jobs.threading.Thread", FakeThread):
            thread = start_durable_job_thread(
                app=app,
                job_store=job_store,
                job_id="job-1",
                target=broken,
                name="durable-test",
            )

        self.assertEqual(thread.name, "durable-test")
        job_store.fail.assert_called_once()
        self.assertEqual(job_store.fail.call_args.args[0], "job-1")
        self.assertIn("boom", job_store.fail.call_args.args[1])
        self.assertEqual(job_store.fail.call_args.kwargs["error_code"], "background_worker_unhandled_exception")

    def test_job_lifecycle_start_complete_and_active_failure(self):
        job_store = Mock()
        job_store.snapshot.return_value = {"job_id": "job-1", "state": "running"}
        lifecycle = JobLifecycle(job_store)

        lifecycle.start("job-1", stage="uploading", message="Uploading.", current=2, total=5)
        failed = lifecycle.fail_if_active(
            "job-1",
            "boom",
            error_category="worker",
            error_code="worker_failed",
            error_retryable=False,
        )
        lifecycle.complete("job-1", results=[{"ok": True}], notice={"message": "done"})

        self.assertTrue(failed)
        job_store.update.assert_called_once_with(
            "job-1",
            state="running",
            stage="uploading",
            message="Uploading.",
            current=2,
            total=5,
        )
        job_store.fail.assert_called_once_with(
            "job-1",
            "boom",
            error_category="worker",
            error_code="worker_failed",
            error_retryable=False,
        )
        job_store.complete.assert_called_once_with("job-1", results=[{"ok": True}], notice={"message": "done"})

    def test_fail_job_if_active_skips_terminal_snapshots(self):
        job_store = Mock()
        job_store.snapshot.return_value = {"job_id": "job-1", "state": "completed"}

        changed = fail_job_if_active(job_store, "job-1", "boom")

        self.assertFalse(changed)
        job_store.fail.assert_not_called()

    def test_job_lifecycle_fails_active_when_snapshot_is_unavailable(self):
        job_store = Mock()
        job_store.snapshot.side_effect = RuntimeError("store down")

        changed = JobLifecycle(job_store).fail_if_active("job-1", "boom")

        self.assertTrue(changed)
        job_store.fail.assert_called_once()

    def test_health_payload_includes_release_manifest_when_present(self):
        payload = build_health_payload(
            lambda: "rev-1",
            environ={
                "TEAM_PORTAL_LIVE_SURFACE": "mac_public_live",
                "TEAM_PORTAL_RELEASE_MANIFEST_ID": "manifest-1",
            },
        )

        self.assertEqual(
            payload,
            {
                "status": "ok",
                "revision": "rev-1",
                "live_surface": "mac_public_live",
                "release_manifest_id": "manifest-1",
            },
        )

    def test_health_payload_defaults_live_surface_and_omits_blank_manifest(self):
        payload = build_health_payload(lambda: "rev-2", environ={"TEAM_PORTAL_RELEASE_MANIFEST_ID": "  "})

        self.assertEqual(payload, {"status": "ok", "revision": "rev-2", "live_surface": "mac_public_live"})

    def test_web_redirect_helpers_reject_external_targets_and_rewrite_query(self):
        self.assertEqual(safe_relative_redirect_target("https://example.com/x"), "")
        self.assertEqual(safe_relative_redirect_target("//example.com/x"), "")
        self.assertEqual(safe_relative_redirect_target("/safe?x=1"), "/safe?x=1")
        self.assertEqual(
            url_with_query_value("https://app.example/portal-home?workspace=bpmis&x=1", "workspace", "run"),
            "https://app.example/portal-home?x=1&workspace=run",
        )
        self.assertEqual(portal_health_url("https://app.example/portal-home?workspace=run"), "https://app.example/healthz")

    def test_source_code_qa_answer_generation_reports_unavailable_llm(self):
        service = Mock()
        service.llm_ready.return_value = False
        service.llm_unavailable_message.return_value = "LLM unavailable"

        with self.assertRaisesRegex(ToolError, "LLM unavailable"):
            build_llm_answer(
                service,
                entries=[],
                key="k",
                pm_team="AF",
                country="SG",
                question="What changed?",
                matches=[],
                llm_budget_mode="auto",
            )

    def test_source_code_qa_answer_generation_routes_normal_and_compact_answers(self):
        class FakeAnswerService:
            llm_budgets = {
                "balanced": {"match_limit": 2},
                "compact_deep": {"match_limit": 1},
            }

            def __init__(self, token_counts):
                self.token_counts = deque(token_counts)
                self.context_limits = []

            def llm_ready(self):
                return True

            def normalize_query_mode(self, query_mode):
                return str(query_mode or "deep").lower()

            def _resolve_llm_budget(self, llm_budget_mode, question, matches):
                return "balanced", self.llm_budgets["balanced"], {"mode": "auto", "reason": "default"}

            def _model_for_role_or_budget(self, role, budget):
                return f"{role}:{budget['match_limit']}"

            def _llm_answer_evidence_context(self, **kwargs):
                self.context_limits.append(kwargs["match_limit"])
                return {
                    "selected_matches": kwargs["matches"][: kwargs["match_limit"]],
                    "evidence_summary": "summary",
                    "quality_gate": {"status": "ok"},
                    "evidence_pack": {"items": kwargs["matches"][: kwargs["match_limit"]]},
                }

            def _answer_context_estimated_tokens(self, answer_context):
                return self.token_counts.popleft()

            def _build_codex_llm_answer(self, **kwargs):
                return kwargs

        normal_service = FakeAnswerService([10])
        normal = build_llm_answer(
            normal_service,
            entries=[],
            key="k",
            pm_team="AF",
            country="SG",
            question="What changed?",
            matches=[{"path": "a.py"}, {"path": "b.py"}],
            llm_budget_mode="auto",
            attachments=[{"name": "a.txt"}],
            runtime_evidence=[{"kind": "browser"}],
            effort_assessment=True,
        )
        compact_service = FakeAnswerService([999_999, 20])
        compact = build_llm_answer(
            compact_service,
            entries=[],
            key="k",
            pm_team="AF",
            country="SG",
            question="What changed?",
            matches=[{"path": "a.py"}, {"path": "b.py"}],
            llm_budget_mode="auto",
        )

        self.assertEqual(normal["llm_route"]["task"], "effort_assessment")
        self.assertEqual(normal["attachments"], [{"name": "a.txt"}])
        self.assertEqual(normal["runtime_evidence"], [{"kind": "browser"}])
        self.assertEqual(compact["routed_budget_mode"], "compact_deep")
        self.assertTrue(compact["llm_route"]["token_pressure"])
        self.assertEqual(compact_service.context_limits, [2, 1])

    def test_source_code_qa_llm_pipeline_builds_initial_plan(self):
        class FakeCodexPlanningService:
            def _codex_initial_candidate_context(self, **kwargs):
                return {
                    "candidate_matches": kwargs["selected_matches"],
                    "candidate_paths": [{"repo": "risk", "path": "src/RiskService.java"}],
                    "candidate_path_layers": [{"path": "src/RiskService.java", "layer": "service"}],
                    "scope_roots": ["src"],
                    "prompt_mode": "path_focused",
                }

            def _codex_initial_route_fields(self, **kwargs):
                return {
                    "selected_model": kwargs["selected_model"],
                    "prompt_mode": kwargs["prompt_mode"],
                    "query_mode": kwargs["query_mode"],
                    "candidate_path_count": len(kwargs["candidate_paths"]),
                }

            def _runtime_evidence_for_budget(self, runtime_evidence, routed_budget_mode):
                self.routed_budget_mode = routed_budget_mode
                return runtime_evidence[:1]

            def _codex_initial_prompt_context(self, **kwargs):
                return kwargs

            def _codex_prompt_stats(self, prompt_context):
                return {"estimated_prompt_tokens": 321, "prompt_chars": 1234}

            def _codex_reasoning_effort_for_route(self, routed_budget_mode):
                return "high" if routed_budget_mode == "deep" else "medium"

        service = FakeCodexPlanningService()

        plan = build_codex_initial_plan(
            service,
            entries=[],
            key="AF-1",
            pm_team="AF",
            country="SG",
            question="What changed?",
            matches=[{"path": "src/RiskService.java"}],
            selected_matches=[{"path": "src/RiskService.java"}],
            evidence_pack={"items": []},
            quality_gate={"status": "ok"},
            llm_route={"provider": "codex"},
            selected_model="gpt-5-codex",
            followup_context={"previous": "q"},
            query_mode="deep",
            routed_budget_mode="deep",
            attachments=[{"name": "trace.txt"}],
            runtime_evidence=[{"kind": "browser"}, {"kind": "log"}],
            effort_assessment=True,
        )

        self.assertEqual(plan.candidate_matches, [{"path": "src/RiskService.java"}])
        self.assertEqual(plan.prompt_runtime_evidence, [{"kind": "browser"}])
        self.assertEqual(plan.prompt_context["runtime_evidence"], [{"kind": "browser"}])
        self.assertEqual(plan.llm_route["provider"], "codex")
        self.assertEqual(plan.llm_route["task"], "effort_assessment")
        self.assertEqual(plan.llm_route["runtime_evidence_count"], 2)
        self.assertEqual(plan.llm_route["prompt_runtime_evidence_count"], 1)
        self.assertEqual(plan.llm_route["initial_prompt_estimated_tokens"], 321)
        self.assertEqual(plan.llm_route["initial_prompt_chars"], 1234)
        self.assertEqual(plan.candidate_repo_count, 1)
        self.assertEqual(plan.reasoning_effort, "high")

    def _bind_source_code_qa_effort_test_globals(self, temp_dir, *, service=None, provider_available=True):
        root = Path(temp_dir)
        dictionary_path = root / "effort_dictionary.json"
        profile_path = root / "domain_profiles.json"
        knowledge_path = root / "domain_knowledge.json"
        dictionary_path.write_text(
            json.dumps(
                {
                    "version": 7,
                    "updated_at": "2026-05-23",
                    "domains": {
                        "AF": {
                            "entries": [
                                {
                                    "id": "case review",
                                    "business_aliases": ["cashline approval"],
                                    "technical_terms": ["risk engine", "loan service"],
                                    "product_terms": ["cashline"],
                                    "limit_terms": ["credit limit"],
                                    "evidence_hints": ["approval api"],
                                    "surfaces": ["backend_service", "configuration", "frontend_surface"],
                                    "country_terms": ["SG"],
                                }
                            ]
                        },
                        "CRMS": {
                            "entries": [
                                {
                                    "id": "income verification",
                                    "business_aliases": ["income"],
                                    "technical_terms": ["bti service"],
                                    "product_terms": ["loan"],
                                }
                            ]
                        },
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        profile_path.write_text("{}", encoding="utf-8")
        knowledge_path.write_text(
            json.dumps(
                {
                    "domains": {
                        "AF": {
                            "module_map": [
                                "bad-module",
                                {
                                    "name": "RiskEngine",
                                    "aliases": ["risk engine"],
                                    "repo_hints": ["af-risk"],
                                    "code_hints": ["RiskService"],
                                    "business_flows": ["case review"],
                                },
                            ],
                            "terminology": [
                                "bad-term",
                                {"term": "Velocity", "aliases": ["velocity rule"], "code_terms": ["VelocityRule"]},
                            ],
                            "retrieval_terms": {"risk": ["approval api", "risk config"]},
                        },
                        "CRMS": {
                            "module_map": [
                                {"name": "BTI", "aliases": ["income"], "code_hints": ["bti service"]},
                            ]
                        },
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        class FakeEffortService:
            def __init__(self):
                self.calls = []
                self.timeout = None

            def with_codex_timeout_seconds(self, seconds):
                self.timeout = seconds
                return self

            def query(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs.get("answer_mode") == "retrieval_only":
                    return {
                        "status": "ok",
                        "summary": "evidence",
                        "matches": [
                            {
                                "repo": "af",
                                "path": "src/RiskService.java",
                                "line_start": 10,
                                "line_end": 15,
                                "reason": "approval api service risk engine test",
                                "snippet": "approval service config frontend report test",
                                "retrieval": "semantic",
                            }
                        ],
                        "citations": [],
                    }
                return {
                    "status": "ok",
                    "summary": "",
                    "llm_answer": "Business Understanding\n- Cashline approval changes.\n\nCode Change Points\n- Backend service update.",
                    "matches": [
                        {"repo": "af", "path": "src/RiskService.java", "line_start": 10, "line_end": 15}
                    ],
                    "citations": [],
                    "llm_route": {"provider": "fake"},
                }

        fake_service = service or FakeEffortService()
        override_globals = {
                "re": re,
                "json": json,
                "Path": Path,
                "hashlib": hashlib,
                "datetime": datetime,
                "timezone": timezone,
                "time": time,
                "ToolError": ToolError,
                "current_app": current_app,
                "SOURCE_CODE_QA_EFFORT_DICTIONARY_PATH": dictionary_path,
                "SOURCE_CODE_QA_DOMAIN_PROFILES_PATH": profile_path,
                "SOURCE_CODE_QA_DOMAIN_KNOWLEDGE_PATH": knowledge_path,
                "SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES": {
                    "AF": ["af", "anti fraud"],
                    "CRMS": ["crms", "credit risk"],
                },
                "SOURCE_CODE_QA_EFFORT_SCOPE_COMMON_TERMS": {"api", "loan", "test"},
                "IDENTIFIER_PATTERN": re.compile(r"[A-Za-z_][A-Za-z0-9_]*"),
                "_team_portal_data_root": lambda settings: settings.team_portal_data_dir,
                "_source_code_qa_provider_available": lambda _provider: provider_available,
                "_build_source_code_qa_service": lambda _provider: fake_service,
                "_resolve_source_code_qa_runtime_evidence": lambda **_kwargs: [
                    {"source_type": "upload", "filename": "requirement.txt", "text": "cashline approval"}
                ],
                "_source_code_qa_public_runtime_evidence": lambda runtime_evidence: runtime_evidence,
                "_classify_source_code_qa_job_error": lambda message: {
                    "retryable": "unavailable" in str(message).lower()
                },
        }
        missing = object()
        original_globals = {key: effort_helpers.__dict__.get(key, missing) for key in override_globals}

        def restore_effort_globals():
            for key, value in original_globals.items():
                if value is missing:
                    effort_helpers.__dict__.pop(key, None)
                else:
                    effort_helpers.__dict__[key] = value
            effort_helpers._load_source_code_qa_effort_dictionaries.cache_clear()
            effort_helpers._load_source_code_qa_domain_profile_config.cache_clear()
            effort_helpers._load_source_code_qa_domain_knowledge_config.cache_clear()

        self.addCleanup(restore_effort_globals)
        effort_helpers.bind_source_code_qa_effort_helpers(override_globals)
        effort_helpers._load_source_code_qa_effort_dictionaries.cache_clear()
        effort_helpers._load_source_code_qa_domain_profile_config.cache_clear()
        effort_helpers._load_source_code_qa_domain_knowledge_config.cache_clear()
        return fake_service

    def test_source_code_qa_effort_helpers_cover_scope_cache_and_normalization_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self._bind_source_code_qa_effort_test_globals(temp_dir)

            self.assertEqual(effort_helpers._source_code_qa_effort_assessment_language("English"), "en")
            self.assertEqual(effort_helpers._source_code_qa_effort_sentences(""), [])
            self.assertEqual(effort_helpers._source_code_qa_effort_unique([None, {"a": 1}, {"a": 1}, "x"]), [{"a": 1}, "x"])
            bad_json_path = Path(temp_dir) / "bad.json"
            bad_json_path.write_text("{bad", encoding="utf-8")
            self.assertEqual(effort_helpers._source_code_qa_load_json_file(bad_json_path), {})
            self.assertEqual(effort_helpers._source_code_qa_effort_country_hint("launch for ID market", "SG"), "ID")
            self.assertEqual(effort_helpers._source_code_qa_effort_country_hint("launch for PH market", ""), "PH")
            self.assertEqual(effort_helpers._source_code_qa_effort_country_hint("launch globally", "MY"), "MY")
            self.assertFalse(effort_helpers._source_code_qa_effort_term_matches("cashline", ""))

            class DisappearingTerm:
                def __init__(self):
                    self.calls = 0

                def __bool__(self):
                    return True

                def __str__(self):
                    self.calls += 1
                    return "visible" if self.calls == 1 else ""

            self.assertFalse(
                effort_helpers._source_code_qa_effort_entry_applies(
                    {"business_aliases": [DisappearingTerm()]},
                    country="SG",
                    requirement="visible",
                )
            )
            business_plan = effort_helpers._build_source_code_qa_effort_business_plan(
                pm_team="AF",
                country="SG",
                language="en",
                requirement="premium good customer cashline conversion guide",
            )
            self.assertIn("qualified or good customers", business_plan["user_segments"])
            self.assertIn("make the limit or product path understandable to customers", business_plan["business_goals"])

            terms_by_team = effort_helpers._source_code_qa_effort_scope_terms_by_team()
            self.assertIn("RiskEngine", terms_by_team["AF"])
            guard = effort_helpers._source_code_qa_effort_scope_guard(
                pm_team="AF",
                country="SG",
                requirement="AF cashline approval risk engine case review",
            )
            self.assertEqual(guard["status"], "ok")
            self.assertIn("AF", guard["matched_terms"]["AF"])
            mismatch = effort_helpers._source_code_qa_effort_scope_guard(
                pm_team="AF",
                country="SG",
                requirement="CRMS income verification bti service for Indonesia",
            )
            self.assertEqual(mismatch["status"], "mismatch")
            mismatch_result = effort_helpers._source_code_qa_effort_scope_mismatch_result(
                pm_team="AF",
                country="SG",
                language="en",
                requirement="CRMS income verification",
                llm_provider="fake",
                business_plan={},
                technical_candidates={},
                estimation_rubric={},
                scope_guard=mismatch,
            )
            self.assertIn("Scope Mismatch", mismatch_result["llm_answer"])

            digest = effort_helpers._source_code_qa_effort_runtime_digest(
                ["bad", {"source_type": "upload", "filename": "req.txt", "text": "secret"}]
            )
            self.assertEqual(digest[0]["source_type"], "upload")
            self.assertEqual(
                effort_helpers._source_code_qa_effort_matrix_quality({"groups": "bad"})["status"],
                "planning_assumption",
            )
            self.assertEqual(
                effort_helpers._source_code_qa_effort_matrix_quality({"groups": ["bad", {"status": "confirmed"}]})[
                    "confirmed_group_count"
                ],
                1,
            )
            matrix = effort_helpers._build_source_code_qa_effort_evidence_matrix(
                evidence_result={
                    "matches": [
                        "bad",
                        {
                            "repo": "af",
                            "path": "src/RiskService.java",
                            "reason": "approval service api config frontend report test",
                            "snippet": "risk engine service api config frontend report test",
                        },
                    ]
                },
                business_plan={"products": ["cashline"], "flow_changes": ["approval"]},
                technical_candidates={
                    "backend_surfaces": ["risk engine"],
                    "frontend_surfaces": ["cashline page"],
                    "configs_or_tables": ["risk config"],
                    "typed_candidates": {"downstream_reporting": ["reporting"]},
                },
            )
            self.assertEqual(matrix["quality"]["status"], "confirmed")
            self.assertEqual(
                effort_helpers._source_code_qa_effort_confidence(
                    {"status": "ok", "matches": [{}, {}, {}]},
                    [],
                ),
                "high",
            )
            self.assertEqual(effort_helpers._source_code_qa_effort_confidence({"status": "ok", "matches": []}, []), "low")

            fallback = effort_helpers._source_code_qa_effort_fallback_answer(
                language="en",
                business_plan={"has_explicit_options": True, "business_goals": ["launch cashline"]},
                technical_candidates={"backend_surfaces": ["risk engine"]},
                estimation_rubric={"option_estimates": [{"label": "Option 1", "be_person_days": "3-5", "fe_person_days": "1-2", "basis": "rules"}]},
                missing_evidence=["none"],
            )
            self.assertIn("Option Code Change Points", fallback)
            self.assertIn("Are the listed options alternatives", fallback)

            structured = effort_helpers._build_source_code_qa_effort_structured_assessment(
                result={
                    "matches": [{"repo": "af", "path": "src/RiskService.java"}],
                    "effort_evidence_matrix": {"groups": [{"key": "tests", "status": "missing"}]},
                },
                language="en",
                business_plan={},
                technical_candidates={},
                estimation_rubric={},
                missing_evidence=[],
                confidence="low",
            )
            qa_point = next(point for point in structured["code_change_points"] if point["workstream"] == "tests")
            self.assertIn("No direct source evidence", qa_point["planning_assumption"])
            self.assertEqual(effort_helpers._source_code_qa_effort_sanitize_visible_answer(""), "")

            normalized = effort_helpers._normalize_source_code_qa_effort_assessment_result(
                result={
                    "status": "ok",
                    "llm_answer": "Business Understanding\n- ok\n\nEvidence\n- hidden\n\nCode Change Points\n- update source/path.py:1",
                    "matches": [{}, {}, {}],
                },
                language="en",
                business_plan={},
                technical_candidates={},
                estimation_rubric={},
            )
            self.assertEqual(normalized["summary"], "Effort assessment completed.")
            self.assertIn("effort_evidence_matrix", normalized)
            self.assertNotIn("hidden", normalized["llm_answer"])

            app = Flask(__name__)
            app.config["SETTINGS"] = _settings(team_portal_data_dir=Path(temp_dir))
            with app.app_context():
                effort_helpers._store_source_code_qa_effort_cached_result(
                    app.config["SETTINGS"],
                    "cache-key",
                    {"status": "ok", "llm_route": {"provider": "fake"}},
                )
                cached = effort_helpers._load_source_code_qa_effort_cached_result(app.config["SETTINGS"], "cache-key")
                self.assertTrue(cached["effort_cache_hit"])
                self.assertEqual(cached["llm_route"]["task"], "effort_assessment")
                invalid_cache_root = effort_helpers._source_code_qa_effort_cache_root(app.config["SETTINGS"])
                invalid_cache_root.mkdir(parents=True, exist_ok=True)
                (invalid_cache_root / "invalid-result.json").write_text(
                    json.dumps({"result": "not-a-dict"}),
                    encoding="utf-8",
                )
                self.assertIsNone(
                    effort_helpers._load_source_code_qa_effort_cached_result(
                        app.config["SETTINGS"],
                        "invalid-result",
                    )
                )
                with patch.object(Path, "write_text", side_effect=OSError("denied")):
                    effort_helpers._store_source_code_qa_effort_cached_result(
                        app.config["SETTINGS"],
                        "cache-key-2",
                        {"status": "ok"},
                    )

    def test_source_code_qa_effort_worker_covers_failures_and_cache_hit(self):
        class CapturingJobStore:
            def __init__(self):
                self.updates = []
                self.completed = None
                self.failed = None

            def update(self, job_id, **kwargs):
                self.updates.append((job_id, kwargs))

            def complete(self, job_id, **kwargs):
                self.completed = (job_id, kwargs)

            def fail(self, job_id, message, **kwargs):
                self.failed = (job_id, message, kwargs)

        with tempfile.TemporaryDirectory() as temp_dir:
            app = Flask(__name__)
            app.config["SETTINGS"] = _settings(team_portal_data_dir=Path(temp_dir))

            unavailable_store = CapturingJobStore()
            app.config["JOB_STORE"] = unavailable_store
            self._bind_source_code_qa_effort_test_globals(temp_dir, provider_available=False)
            effort_helpers._run_source_code_qa_effort_assessment_job(
                app,
                "job-unavailable",
                {"llm_provider": "missing", "pm_team": "AF", "country": "SG", "requirement": "AF cashline approval"},
            )
            self.assertIn("unavailable", unavailable_store.failed[1])
            self.assertTrue(unavailable_store.failed[2]["retryable"])

            empty_store = CapturingJobStore()
            app.config["JOB_STORE"] = empty_store
            self._bind_source_code_qa_effort_test_globals(temp_dir)
            effort_helpers._run_source_code_qa_effort_assessment_job(
                app,
                "job-empty",
                {"llm_provider": "fake", "pm_team": "AF", "country": "SG", "requirement": "   "},
            )
            self.assertIn("Business requirement is empty", empty_store.failed[1])

            cache_store = CapturingJobStore()
            app.config["JOB_STORE"] = cache_store
            service = self._bind_source_code_qa_effort_test_globals(temp_dir)
            cached_result = {
                "status": "ok",
                "summary": "",
                "llm_answer": "Business Understanding\n- cached\n\nCode Change Points\n- Backend update",
                "matches": [{"repo": "af", "path": "src/RiskService.java", "line_start": 1, "line_end": 3}],
                "llm_route": {"provider": "fake", "codex_repair_decision_ms": 4},
                "effort_timing": {"synthesis_ms": 0},
            }
            with patch.object(effort_helpers, "_load_source_code_qa_effort_cached_result", return_value=cached_result):
                effort_helpers._run_source_code_qa_effort_assessment_job(
                    app,
                    "job-cache",
                    {
                        "llm_provider": "fake",
                        "pm_team": "AF",
                        "country": "SG",
                        "language": "en",
                        "requirement": "AF cashline approval risk engine case review",
                    },
                )

            self.assertIsNotNone(cache_store.completed)
            result = cache_store.completed[1]["results"][0]
            self.assertTrue(result["effort_timing"]["cache_hit"])
            self.assertEqual(result["assessment"]["type"], "effort_assessment")
            self.assertEqual(len(service.calls), 1)

    def test_team_dashboard_config_normalizes_invalid_shapes_and_cache_values(self):
        self.assertEqual(normalize_team_dashboard_emails(" A@NPT.SG; a@npt.sg b@npt.sg "), ["a@npt.sg", "b@npt.sg"])
        self.assertEqual(normalize_team_dashboard_emails({"bad": "shape"}), [])
        with tempfile.TemporaryDirectory() as temp_dir:
            store = TeamDashboardConfigStore(Path(temp_dir) / "team_dashboard.sqlite")
            default_config = store.load()
            saved = store.save(default_config)
            loaded_saved = store.load()
            with store.db_path.open("ab"):
                pass
            import sqlite3

            with sqlite3.connect(store.db_path) as connection:
                connection.execute(
                    "UPDATE team_dashboard_configs SET config_json = ? WHERE config_key = ?",
                    ("{bad-json", store.CONFIG_KEY),
                )
                connection.commit()
            self.assertIn("AF", store.load()["teams"])

            legacy_normalized = store.normalize_config(
                {
                    "key_project_overrides": {
                        "": {"is_key_project": True},
                        "225": {"updated_by": "Owner@NPT.SG"},
                        "226": {"is_key_project": False, "updated_by": "Owner@NPT.SG"},
                    },
                    "teams": {"AF": {"member_emails": list(TEAM_DASHBOARD_LEGACY_DEFAULT_MEMBER_EMAILS)}},
                    "task_cache": {"version": 1},
                    "actual_mandays_cache": {"projects": {"P3": {"value": "2.0", "cached_at": "ts"}}},
                }
            )
            normalized = store.normalize_config(
                {
                    "key_project_overrides": {
                        "": {"is_key_project": True},
                        "225": {"updated_by": "Owner@NPT.SG"},
                        "226": {"is_key_project": False, "updated_by": "Owner@NPT.SG"},
                    },
                    "task_cache": {
                        "version": TEAM_DASHBOARD_TASK_CACHE_VERSION,
                        "updated_at": "now",
                        "teams": {"AF": {"email_signature": " sig ", "under_prd": "bad", "pending_live": "bad"}},
                    },
                    "actual_mandays_cache": {
                        "updated_at": "today",
                        "projects": {
                            "": {"value": 1},
                            "P1": "bad",
                            "P2": {"value": "not-a-number", "cached_at": "ts"},
                        },
                    },
                }
            )

        self.assertEqual(saved["version_plan"], default_config["version_plan"])
        self.assertEqual(loaded_saved["version_plan"], default_config["version_plan"])
        self.assertEqual(list(normalized["key_project_overrides"]), ["226"])
        self.assertEqual(normalized["key_project_overrides"]["226"]["updated_by"], "owner@npt.sg")
        self.assertEqual(normalized["task_cache"]["teams"]["AF"]["under_prd"], [])
        self.assertEqual(normalized["actual_mandays_cache"]["projects"], {"P2": {"value": "", "cached_at": "ts"}})
        self.assertEqual(legacy_normalized["teams"]["AF"]["member_emails"], default_config["teams"]["AF"]["member_emails"])
        self.assertEqual(legacy_normalized["task_cache"]["teams"], {})
        self.assertEqual(legacy_normalized["actual_mandays_cache"]["projects"]["P3"]["value"], 2)

    def test_source_code_qa_cache_telemetry_handles_logging_cache_and_errors(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            class FakeCacheService:
                answer_cache_root = Path(temp_dir) / "answers"
                telemetry_path = Path(temp_dir) / "telemetry.jsonl"
                llm_cache_ttl_seconds = 3600
                llm_provider = type("Provider", (), {"name": "codex_cli_bridge"})()

                def _llm_versions(self):
                    return {"v": 1}

                def _now_iso(self):
                    return "2026-05-23T00:00:00Z"

                def normalize_query_mode(self, query_mode):
                    return f"mode:{query_mode}"

                def _atomic_write_json(self, path, payload):
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps(payload), encoding="utf-8")

                def _query_telemetry_match_counts(self, matches):
                    return {"stage": len(matches)}, {"retrieval": len(matches)}

                def _query_telemetry_tool_trace_summary(self, tool_trace):
                    return {"steps": len(tool_trace)}

                def _query_telemetry_evidence_pack_summary(self, evidence_pack):
                    return {"items": len(evidence_pack.get("items") or [])}

                def _query_telemetry_answer_policy_statuses(self, policies):
                    return {str(item.get("name")): str(item.get("status")) for item in policies}

                def _codex_telemetry_summary(self, payload):
                    return {"provider": payload.get("llm_provider")}

            service = FakeCacheService()
            with self.assertLogs("bpmis_jira_tool.source_code_qa_cache_telemetry", level="WARNING") as logs:
                _log_source_code_qa_timing(
                    "component",
                    elapsed_ms=-1,
                    ignored=None,
                    values=list(range(25)),
                    mapping={"ok": True, "bad": object()},
                    fallback=object(),
                )
            self.assertIn('"elapsed_ms": 0', logs.output[0])
            self.assertIn('"ok": true', logs.output[0])
            self.assertIn('"values"', logs.output[0])

            self.assertIsNone(_load_cached_answer(service, "missing"))
            key = "cache-key"
            _store_cached_answer(
                service,
                key,
                answer="answer",
                usage={"tokens": 1},
                provider="codex",
                model="gpt",
                query_mode="deep",
                llm_budget_mode="auto",
            )
            cached = _load_cached_answer(service, key)
            self.assertEqual(cached["answer"], "answer")
            self.assertEqual(_answer_cache_metadata(key, cached)["provider"], "codex")

            expired_path = service.answer_cache_root / "expired.json"
            expired_path.write_text(json.dumps({"expire_at": 1, "versions": service._llm_versions()}), encoding="utf-8")
            self.assertIsNone(_load_cached_answer(service, "expired"))
            self.assertFalse(expired_path.exists())

            stale_path = service.answer_cache_root / "stale.json"
            stale_path.write_text(json.dumps({"expire_at": time.time() + 1000, "versions": {"old": True}}), encoding="utf-8")
            self.assertIsNone(_load_cached_answer(service, "stale"))
            invalid_path = service.answer_cache_root / "invalid.json"
            invalid_path.write_text("{bad-json", encoding="utf-8")
            self.assertIsNone(_load_cached_answer(service, "invalid"))

            _record_query_telemetry(
                service,
                key="q",
                question="How does it work?",
                answer_mode="summary",
                llm_budget_mode="auto",
                payload={
                    "matches": [{"path": "a.py"}],
                    "answer_contract": {"policies": [{"name": "cite", "status": "ok"}]},
                    "evidence_pack": {"items": [1]},
                    "tool_trace": [{"phase": "search"}],
                    "llm_attempt_log": [{"reasoning_effort": "low"}],
                    "llm_provider": "codex_cli_bridge",
                },
                started_at=time.time(),
            )
            self.assertIn('"question_preview": "How does it work?"', service.telemetry_path.read_text(encoding="utf-8"))

            service.telemetry_path = Path(temp_dir) / "missing-parent" / "telemetry.jsonl"
            with patch.object(Path, "mkdir", side_effect=OSError("disk full")):
                _record_query_telemetry(
                    service,
                    key="q",
                    question="x",
                    answer_mode="summary",
                    llm_budget_mode="auto",
                    payload={},
                    started_at=time.time(),
                )

            self.assertEqual(_codex_telemetry_summary({"llm_provider": "gemini"}), {})
            codex_summary = _codex_telemetry_summary(
                {
                    "llm_provider": "codex_cli_bridge",
                    "llm_route": {"prompt_mode": "mode", "candidate_repo_count": 1},
                    "answer_claim_check": {"codex_citation_validation": {"status": "ok", "cited_path_count": 2}},
                    "llm_attempt_log": [{"exit_code": 0}, {"timeout": True}],
                    "codex_cli_trace": {
                        "session_mode": "ephemeral",
                        "command_mode": "exec",
                        "stream_messages": [1, 2],
                        "command_summaries": [1],
                        "probable_inspected_files": ["a.py"],
                    },
                    "llm_latency_ms": 12,
                }
            )
            self.assertEqual(codex_summary["exit_codes"], [0])
            self.assertTrue(codex_summary["timeout"])

    def test_web_source_code_qa_jobs_cover_error_and_slow_paths(self):
        class FakeJobStore:
            def __init__(self):
                self.failures = []
                self.completed = []
                self.updates = []

            def update(self, job_id, **kwargs):
                self.updates.append((job_id, kwargs))

            def complete(self, job_id, **kwargs):
                self.completed.append((job_id, kwargs))

            def fail(self, job_id, message, **kwargs):
                self.failures.append((job_id, message, kwargs))

            def snapshot(self, job_id):
                return {"started_at": time.time() - 130, "stage": "answering"}

        app = Flask(__name__)
        app.config["JOB_STORE"] = FakeJobStore()

        class ToolErrorSyncService:
            def sync(self, **kwargs):
                raise ToolError("sync failed")

        web_source_code_qa_jobs.bind_source_code_qa_job_helpers(
            {
                "ToolError": ToolError,
                "_build_source_code_qa_service": lambda *args, **kwargs: ToolErrorSyncService(),
            }
        )
        web_source_code_qa_jobs._run_source_code_qa_sync_job(app, "sync-job", _settings(), "AF", "SG")
        self.assertEqual(app.config["JOB_STORE"].failures[-1][1], "sync failed")

        class QueryService:
            llm_provider_name = "mock"

            def query(self, **kwargs):
                kwargs["progress_callback"]("retrieving", "Retrieving.", 0, 1)
                return {
                    "status": "warning",
                    "summary": "Done with warnings.",
                    "trace_id": "trace-1",
                    "slow_query_attribution": {"status": "slow", "slow_component": "retrieval", "slow_component_ms": 321},
                }

        web_source_code_qa_jobs.bind_source_code_qa_job_helpers(
            {
                "ToolError": ToolError,
                "current_app": current_app,
                "json": json,
                "time": time,
                "_source_code_qa_provider_available": lambda provider: True,
                "_build_source_code_qa_service": lambda provider=None: QueryService(),
                "_source_code_qa_query_mode": lambda value: str(value or "deep"),
                "_get_source_code_qa_session_store": lambda: Mock(),
                "_resolve_source_code_qa_query_attachments": lambda payload, **kwargs: [{"name": "resolved"}],
                "_resolve_source_code_qa_runtime_evidence": lambda **kwargs: [{"kind": "runtime"}],
                "_prepare_source_code_qa_auto_sync": lambda *args, **kwargs: {"status": "skipped"},
                "_source_code_qa_public_answer_mode": lambda value: str(value or "standard"),
                "_source_code_qa_public_attachments": lambda attachments: attachments,
                "_source_code_qa_public_runtime_evidence": lambda evidence: evidence,
                "_record_source_code_qa_work_memory": lambda **kwargs: None,
                "_classify_source_code_qa_job_error": lambda message: {"retryable": "unavailable" in message},
            }
        )
        web_source_code_qa_jobs._run_source_code_qa_query_job(
            app,
            "query-job",
            {"question": "Q", "pm_team": "AF", "country": "SG", "llm_provider": "mock"},
        )
        self.assertEqual(app.config["JOB_STORE"].completed[-1][0], "query-job")
        self.assertEqual(app.config["JOB_STORE"].completed[-1][1]["notice"]["tone"], "warning")

        web_source_code_qa_jobs.bind_source_code_qa_job_helpers(
            {
                "ToolError": ToolError,
                "time": time,
                "_source_code_qa_provider_available": lambda provider: False,
                "_classify_source_code_qa_job_error": lambda message: {"retryable": True},
            }
        )
        web_source_code_qa_jobs._run_source_code_qa_query_job(app, "unavailable-job", {"llm_provider": "missing"})
        self.assertIn("unavailable", app.config["JOB_STORE"].failures[-1][1])

    def test_prd_self_assessment_latest_covers_local_agent_fallback_and_errors(self):
        settings = _settings()
        calls = []

        class LocalAgentClient:
            def prd_self_assessment_latest(self, *, owner_key):
                return {"status": "ok", "owner_key": owner_key}

        ctx = SimpleNamespace(
            settings=settings,
            _require_prd_self_assessment_access=lambda settings, api=False: None,
            _get_user_identity=lambda settings: {"config_key": "owner-1"},
            _current_release_revision=lambda: "rev",
            _run_prd_self_assessment_action=lambda settings, action: {"action": action},
            _run_prd_self_assessment_sections=lambda settings: {"sections": []},
            web_globals={
                "_is_portal_admin": lambda: True,
                "_local_agent_source_code_qa_enabled": lambda settings: True,
                "_build_local_agent_client": lambda settings: LocalAgentClient(),
                "_get_prd_latest_result": lambda **kwargs: calls.append(kwargs) or {"cached": True},
            },
        )
        app = Flask(__name__)
        with app.test_request_context("/api/prd-self-assessment/latest"):
            response = build_prd_self_assessment_handlers(ctx).prd_self_assessment_latest_api()
            self.assertEqual(response.get_json(), {"status": "ok", "owner_key": "owner-1"})

        ctx._require_prd_self_assessment_access = lambda settings, api=False: ("blocked", 403)
        with app.test_request_context("/api/prd-self-assessment/latest"):
            self.assertEqual(build_prd_self_assessment_handlers(ctx).prd_self_assessment_latest_api(), ("blocked", 403))
        ctx._require_prd_self_assessment_access = lambda settings, api=False: None

        ctx.web_globals["_local_agent_source_code_qa_enabled"] = lambda settings: False
        with app.test_request_context("/api/prd-self-assessment/latest"):
            response = build_prd_self_assessment_handlers(ctx).prd_self_assessment_latest_api()
            self.assertEqual(response.get_json(), {"status": "ok", "latest": {"cached": True}})
            self.assertEqual(calls[-1], {"owner_key": "owner-1", "tool_key": "prd_self_assessment"})

        ctx.web_globals["_get_prd_latest_result"] = lambda **kwargs: (_ for _ in ()).throw(ToolError("bad latest"))
        with app.test_request_context("/api/prd-self-assessment/latest"):
            response, status = build_prd_self_assessment_handlers(ctx).prd_self_assessment_latest_api()
            self.assertEqual(status, 400)
            self.assertEqual(response.get_json()["message"], "bad latest")

        ctx.web_globals["_get_prd_latest_result"] = lambda **kwargs: (_ for _ in ()).throw(RuntimeError(""))
        with app.test_request_context("/api/prd-self-assessment/latest"):
            response, status = build_prd_self_assessment_handlers(ctx).prd_self_assessment_latest_api()
            self.assertEqual(status, 400)
            self.assertIn("Could not load latest", response.get_json()["message"])

    def test_source_code_qa_sql_artifact_helpers_cover_json_inline_and_readme_edges(self):
        self.assertEqual(source_code_qa_answer_text_candidates(""), [])
        self.assertEqual(source_code_qa_answer_text_candidates("{bad json"), ["{bad json"])
        self.assertEqual(source_code_qa_answer_text_candidates('{"sql": "SELECT * FROM users"}'), ["SELECT * FROM users"])
        self.assertEqual(source_code_qa_answer_text_candidates('{"empty": true}'), ['{"empty": true}'])

        self.assertEqual(normalize_source_code_qa_sql_text(""), "")
        self.assertEqual(normalize_source_code_qa_sql_text("not sql"), "")
        self.assertEqual(extract_source_code_qa_inline_sql(""), "")
        self.assertEqual(normalize_source_code_qa_sql_text("SELECT * FROM users;\nDROP TABLE users"), "SELECT * FROM users;")
        self.assertEqual(extract_source_code_qa_inline_sql("SQL: SELECT * FROM users"), "SELECT * FROM users")
        self.assertEqual(extract_source_code_qa_inline_sql("SELECT * FROM users"), "SELECT * FROM users")
        self.assertEqual(extract_source_code_qa_inline_sql("no sql here"), "")
        self.assertEqual(extract_source_code_qa_sql_blocks("```sql\nSELECT * FROM users\n```"), ["SELECT * FROM users"])
        self.assertEqual(extract_source_code_qa_sql_blocks("SQL: SELECT * FROM users"), ["SELECT * FROM users"])
        self.assertEqual(format_source_code_qa_sql_text(""), "")
        self.assertIn("SELECT", format_source_code_qa_sql_text("select * from users"))
        self.assertEqual(clean_source_code_qa_sql_identifier("[schema].[users]"), "schema.users")

        sql = "WITH recent AS (SELECT * FROM app.users) SELECT COUNT(*) FROM recent JOIN app.orders ON true WHERE id > 1 GROUP BY id ORDER BY id LIMIT 5"
        self.assertEqual(source_code_qa_sql_ctes(sql), ["recent"])
        self.assertEqual(source_code_qa_sql_tables(sql), ["app.users", "app.orders"])
        summary = "\n".join(source_code_qa_sql_logic_summary(sql))
        self.assertIn("CTE", summary)
        self.assertIn("JOIN", summary)
        self.assertEqual(source_code_qa_sql_logic_summary(""), ["- No rough SQL logic could be inferred automatically; review `query.sql` directly."])

        readme = build_source_code_qa_sql_readme(
            pm_team="af",
            country="",
            question="Which users?",
            sql="SELECT * FROM users",
            result={"matches": [{"repo": "repo", "path": "mapper.xml", "line_start": 12}, "bad"]},
            runtime_evidence=[{"source_type": "dictionary", "filename": "dict.csv", "pm_team": "AF", "country": "SG"}],
        )
        self.assertIn("AF:All", readme)
        self.assertIn("mapper.xml:12", readme)
        self.assertIn("dict.csv", readme)

        empty_readme = build_source_code_qa_sql_readme(
            pm_team="",
            country="ID",
            question="Q",
            sql="",
            result={},
            runtime_evidence=[],
        )
        self.assertIn("No runtime evidence", empty_readme)
        self.assertIn("No base table", empty_readme)

    def test_source_code_qa_codex_prompt_helpers_include_optional_sections(self):
        instruction = codex_system_instruction()
        self.assertIn("read-only code investigator", instruction)

        progress = Mock()
        payload = build_codex_payload(
            "prompt",
            prompt_mode="mode",
            system_instruction="sys",
            prompt_stats={"prompt_chars": 10, "prompt_bytes": 20, "estimated_prompt_tokens": 3},
            progress_callback=progress,
            codex_cli_session_id="session-1",
            image_paths=["/tmp/a.png"],
            trace_id="trace",
            phase="initial",
            candidate_path_count=2,
            candidate_repo_count=1,
            repair_issue_count=4,
        )
        self.assertEqual(payload["codex_cli_session_id"], "session-1")
        self.assertEqual(payload["_codex_image_paths"], ["/tmp/a.png"])
        self.assertIs(payload["_progress_callback"], progress)

        long_answer = "A" * 7100
        repair = build_codex_repair_brief(
            pm_team="AF",
            country="SG",
            question="Q",
            initial_answer=long_answer,
            scope_roots=[{"repo": "repo", "repo_root": "/repo", "repo_relative_root": "src"}],
            candidate_paths=[
                {"id": "S1", "repo": "repo", "repo_root": "/repo", "repo_relative_root": "src", "path": "a.py", "file_exists": True, "line_start": 1, "line_end": 2}
            ],
            attachment_section="Attachment facts",
            repair_issues=["missing citation"],
        )
        self.assertIn("[initial answer truncated]", repair)
        self.assertIn("Attachment facts", repair)
        self.assertIn("S1", repair)

        sql_brief = build_codex_sql_generation_brief(
            pm_team="AF",
            country="SG",
            question="Q",
            candidate_paths=[
                {
                    "id": "S1",
                    "repo": "repo",
                    "repo_root": "/repo",
                    "repo_relative_root": "src",
                    "path": "mapper.xml",
                    "original_path": "orig.xml",
                    "file_exists": True,
                    "path_status": "ok",
                    "line_start": 1,
                    "line_end": 10,
                    "reason": "table",
                }
            ],
            evidence_pack={
                "tables": ["users"],
                "read_write_points": ["mapper"],
                "entry_points": ["api"],
                "data_sources": ["db"],
                "source_tiers": ["dao"],
                "missing_hops": ["service"],
                "items": ["bad", {"source_id": "S1", "type": "table", "confidence": "high", "hop": "dao", "claim": "uses users"}],
            },
            quality_gate={"status": "ok", "confidence": "high", "missing": []},
            followup_context={"question": "Previous?", "answer": "Previous answer."},
            scope_roots=[{"repo": "repo", "repo_root": "/repo", "repo_relative_root": "src"}],
            attachment_section="Attachment section",
            runtime_section="Runtime section",
        )
        self.assertIn("original_path=orig.xml", sql_brief)
        self.assertIn("Follow-up context", sql_brief)
        self.assertIn("Attachment section", sql_brief)
        self.assertIn("Runtime section", sql_brief)

        fallback_scope = build_codex_sql_generation_brief(
            pm_team="",
            country="",
            question="Q",
            candidate_paths=[],
            evidence_pack={},
            quality_gate={"status": "missing", "confidence": "low", "missing": ["tables"]},
            followup_context=None,
            scope_roots=[],
            attachment_section="",
            runtime_section="",
        )
        self.assertIn("No explicit allowlist", fallback_scope)

    def test_source_code_qa_codex_answer_covers_cached_and_deadline_skip_paths(self):
        with self.assertLogs("bpmis_jira_tool.source_code_qa_codex_answer", level="WARNING") as logs:
            _log_codex_answer_timing(
                "component",
                elapsed_ms=5,
                ignored=None,
                values=("a", "b"),
                mapping={"ok": 1, "bad": object()},
                fallback=object(),
            )
        self.assertIn('"values": ["a", "b"]', logs.output[0])
        self.assertIn('"ok": 1', logs.output[0])

        class FakeProvider:
            name = "codex_cli_bridge"

        class FakeCodexAnswerService:
            llm_provider = FakeProvider()
            codex_repair_deadline_seconds = 1
            codex_repair_prompt_token_limit = 99999

            def __init__(self, *, cached):
                self.cached = cached

            def normalize_query_mode(self, query_mode):
                return str(query_mode or "deep")

            def _codex_initial_candidate_context(self, **kwargs):
                return {
                    "candidate_matches": [{"path": "a.py"}],
                    "candidate_paths": [{"repo": "repo", "path": "a.py"}],
                    "candidate_path_layers": {"direct": ["a.py"]},
                    "scope_roots": [{"repo": "repo", "repo_root": "/repo", "repo_relative_root": ""}],
                    "prompt_mode": "mode",
                }

            def _codex_initial_route_fields(self, **kwargs):
                return {"prompt_mode": kwargs["prompt_mode"]}

            def _runtime_evidence_for_budget(self, evidence, budget):
                return list(evidence)

            def _codex_initial_prompt_context(self, **kwargs):
                return "prompt"

            def _codex_prompt_stats(self, prompt):
                return {"estimated_prompt_tokens": 10, "prompt_chars": len(prompt), "prompt_bytes": len(prompt.encode())}

            def _codex_reasoning_effort_for_route(self, budget):
                return "low"

            def _log_codex_prompt_timing(self, **kwargs):
                return None

            def _answer_cache_key(self, **kwargs):
                return "cache-key"

            def _load_cached_answer(self, key):
                return {"answer": "cached answer"} if self.cached else None

            def _parse_structured_answer(self, answer):
                return {"direct_answer": answer}

            def _cached_codex_answer_payload(self, **kwargs):
                return {"status": "cached", **kwargs}

            def _codex_cli_session_id(self, followup_context):
                return "session-1"

            def _codex_initial_answer_result(self, **kwargs):
                return {
                    "answer": "initial",
                    "structured_answer": {"direct_answer": "initial"},
                    "usage": {"tokens": 1},
                    "effective_model": "gpt",
                    "attempts": 1,
                    "llm_latency_ms": 2,
                    "llm_attempt_log": [],
                    "finish_reason": "stop",
                    "codex_cli_trace": {},
                    "codex_initial_ms": 2,
                    "codex_validation": {"status": "warning"},
                    "claim_check": {},
                    "answer_judge": {},
                }

            def _codex_repair_decision(self, **kwargs):
                return {
                    "severe_repair_reasons": ["missing citations"],
                    "repair_issues": ["missing citations"],
                    "deep_needed": False,
                    "repair_issue_count": 1,
                    "repair_will_run": True,
                    "repair_decision_ms": 3,
                }

            def _codex_repair_remaining_timeout_seconds(self, codex_started_at, reserve_seconds=0):
                return None, ""

            def _model_for_role(self, role):
                return f"model:{role}"

            def _codex_repair_brief(self, **kwargs):
                return "repair prompt"

            def _repair_candidate_paths_for_runtime_evidence(self, candidate_paths, runtime_evidence):
                return candidate_paths

            def _codex_final_answer_payload(self, **kwargs):
                return {"status": "final", **kwargs}

        common_kwargs = {
            "entries": [],
            "key": "k",
            "pm_team": "AF",
            "country": "SG",
            "question": "Q",
            "matches": [{"path": "a.py"}],
            "selected_matches": [{"path": "a.py"}],
            "evidence_summary": {"summary": True},
            "quality_gate": {"status": "ok"},
            "evidence_pack": {"items": []},
            "llm_budget_mode": "auto",
            "routed_budget_mode": "balanced",
            "budget": {},
            "llm_route": {},
            "selected_model": "gpt",
            "followup_context": {"used": True},
            "requested_answer_mode": "standard",
        }
        cached = build_codex_llm_answer_payload(FakeCodexAnswerService(cached=True), **common_kwargs)
        self.assertEqual(cached["status"], "cached")

        with patch("bpmis_jira_tool.source_code_qa_codex_answer.time.time", side_effect=[0, 2, *([2] * 20)]):
            final = build_codex_llm_answer_payload(FakeCodexAnswerService(cached=False), **common_kwargs)
        self.assertEqual(final["status"], "final")
        self.assertFalse(final["repair_attempted"])
        self.assertEqual(final["repair_skipped_reason"], "codex_repair_deadline_after_initial_answer")

        class BudgetSkipCodexAnswerService(FakeCodexAnswerService):
            codex_repair_deadline_seconds = 0
            codex_repair_min_remaining_seconds = 5

            def _codex_repair_remaining_timeout_seconds(self, codex_started_at, reserve_seconds=0):
                return 1, "insufficient_query_budget"

        budget_skip = build_codex_llm_answer_payload(BudgetSkipCodexAnswerService(cached=False), **common_kwargs)
        self.assertEqual(budget_skip["status"], "final")
        self.assertFalse(budget_skip["repair_attempted"])
        self.assertEqual(budget_skip["repair_skipped_reason"], "insufficient_query_budget")

    def test_retrieval_cache_helpers_cover_empty_timing_stats_and_clone_fallbacks(self):
        _increment_retrieval_stat(None, "search_hits")
        _record_query_phase_timing(None, "search", elapsed_ms=10)
        self.assertEqual(
            _query_phase_timing_stats(None),
            {"components": {}, "events": [], "slowest_component": "", "slowest_component_ms": 0},
        )

        cache = _new_retrieval_request_cache()
        _increment_retrieval_stat(cache, "search_hits")
        _record_query_phase_timing(
            cache,
            "",
            elapsed_ms=-5,
            skipped=None,
            labels={"a", "b"},
            metadata={"ok": True, "nested": {"ignored": True}},
            unsupported=object(),
        )
        for index in range(125):
            _record_query_phase_timing(cache, "phase", elapsed_ms=index, index=index)

        timing = _query_phase_timing_stats(cache)
        self.assertEqual(timing["slowest_component"], "phase")
        self.assertEqual(len(timing["events"]), 120)
        self.assertEqual(timing["events"][0]["component"], "phase")
        self.assertEqual(timing["events"][-1]["index"], 124)

        stats = _retrieval_cache_stats(cache)
        self.assertEqual(stats["search_hits"], 1)
        self.assertEqual(stats["search_entries"], 0)
        self.assertGreaterEqual(stats["elapsed_ms"], 0)

        self.assertEqual(list(_clone_jsonish([{"a": object()}, "x"])[0].keys()), ["a"])
        self.assertEqual(list(_clone_jsonish({"a": object()}).keys()), ["a"])
        marker = object()
        self.assertIs(_clone_jsonish(marker), marker)

    def test_retrieval_cache_helpers_attach_to_service_class(self):
        class FakeService:
            pass

        attach_retrieval_cache_helpers(FakeService)
        cache = FakeService._new_retrieval_request_cache()
        FakeService._increment_retrieval_stat(cache, "fts_misses")
        self.assertEqual(cache["stats"]["fts_misses"], 1)

    def test_productization_route_handlers_cover_login_and_validation_boundaries(self):
        login_response = object()
        calls = []

        class FakeBPMISClient:
            def search_versions(self, query):
                calls.append(("versions", query))
                return [{"id": "v1"}]

            def list_issues_for_version(self, version_id):
                calls.append(("issues", version_id))
                return [{"id": "issue-1"}]

        ctx = SimpleNamespace(
            settings=_settings(),
            web_globals={},
            _require_google_login=lambda settings, api=False: login_response,
            _serialize_productization_version_candidate=lambda item: {"version": item["id"]},
            _load_current_user_config=lambda settings: {"pm_team": "AF"},
            _filter_productization_issue_rows_for_pm_team=lambda rows, config, show_all_before_team_filtering=False: (
                rows,
                {"show_all_before_team_filtering": show_all_before_team_filtering},
            ),
            _normalize_productization_issue_row=lambda item: {"issue": item["id"]},
            _classify_portal_error=lambda error: {"error_code": "tool_error"},
            _log_portal_event=lambda *args, **kwargs: calls.append(("log", args, kwargs)),
            _build_request_log_context=lambda settings, extra=None: {"extra": extra or {}},
            _build_bpmis_client_for_current_user=lambda settings: FakeBPMISClient(),
            _apply_codex_productization_detailed_features=lambda normalized, rows, settings: {
                "codex_detailed_feature": True,
                "codex_generated_count": len(normalized),
            },
        )
        handlers = build_productization_handlers(ctx)
        app = Flask(__name__)

        with app.test_request_context("/api/productization-upgrade-summary/versions?q=26Q2"):
            self.assertIs(handlers.productization_upgrade_summary_versions(), login_response)
        with app.test_request_context("/api/productization-upgrade-summary/issues?version_id=88"):
            self.assertIs(handlers.productization_upgrade_summary_issues(), login_response)
        with app.test_request_context("/api/productization-upgrade-summary/llm-descriptions?version_id=88"):
            self.assertIs(handlers.productization_upgrade_summary_llm_descriptions(), login_response)

        ctx._require_google_login = lambda settings, api=False: None
        handlers = build_productization_handlers(ctx)

        with app.test_request_context("/api/productization-upgrade-summary/versions"):
            response, status = handlers.productization_upgrade_summary_versions()
            self.assertEqual(status, 400)
            self.assertIn("Version keyword", response.get_json()["message"])

        with app.test_request_context("/api/productization-upgrade-summary/issues"):
            response, status = handlers.productization_upgrade_summary_issues()
            self.assertEqual(status, 400)
            self.assertIn("version_id", response.get_json()["message"])

        with app.test_request_context("/api/productization-upgrade-summary/llm-descriptions"):
            response, status = handlers.productization_upgrade_summary_llm_descriptions()
            self.assertEqual(status, 400)
            self.assertIn("version_id", response.get_json()["message"])

        with app.test_request_context("/api/productization-upgrade-summary/versions?q=26Q2"):
            response = handlers.productization_upgrade_summary_versions()
            self.assertEqual(response.get_json()["items"], [{"version": "v1"}])

        with app.test_request_context("/api/productization-upgrade-summary/issues?version_id=88&show_all_before_team_filtering=yes"):
            response = handlers.productization_upgrade_summary_issues()
            payload = response.get_json()
            self.assertEqual(payload["items"], [{"issue": "issue-1"}])
            self.assertTrue(payload["show_all_before_team_filtering"])

        with app.test_request_context("/api/productization-upgrade-summary/llm-descriptions?version_id=88"):
            response = handlers.productization_upgrade_summary_llm_descriptions()
            self.assertTrue(response.get_json()["codex_detailed_feature"])

        route_app = Flask("routes")
        register_productization_routes(route_app, handlers)
        self.assertIn("/api/productization-upgrade-summary/versions", {str(rule) for rule in route_app.url_map.iter_rules()})

    def test_productization_route_handlers_cover_tool_and_unexpected_errors(self):
        events = []

        class ToolErrorBPMISClient:
            def search_versions(self, query):
                raise ToolError("version failed")

            def list_issues_for_version(self, version_id):
                raise ToolError("issues failed")

        class UnexpectedBPMISClient:
            def search_versions(self, query):
                raise RuntimeError("version exploded")

            def list_issues_for_version(self, version_id):
                raise RuntimeError("issues exploded")

        def build_ctx(client):
            return SimpleNamespace(
                settings=_settings(),
                web_globals={},
                _require_google_login=lambda settings, api=False: None,
                _serialize_productization_version_candidate=lambda item: item,
                _load_current_user_config=lambda settings: {},
                _filter_productization_issue_rows_for_pm_team=lambda rows, config, show_all_before_team_filtering=False: (rows, {}),
                _normalize_productization_issue_row=lambda item: item,
                _classify_portal_error=lambda error: {"error_code": "classified"},
                _log_portal_event=lambda *args, **kwargs: events.append((args, kwargs)),
                _build_request_log_context=lambda settings, extra=None: {"extra": extra or {}},
                _build_bpmis_client_for_current_user=lambda settings: client,
                _apply_codex_productization_detailed_features=lambda normalized, rows, settings: (_ for _ in ()).throw(
                    RuntimeError("codex exploded")
                ),
            )

        app = Flask(__name__)
        tool_handlers = build_productization_handlers(build_ctx(ToolErrorBPMISClient()))
        with app.test_request_context("/api/productization-upgrade-summary/versions?q=26Q2"):
            response, status = tool_handlers.productization_upgrade_summary_versions()
            self.assertEqual(status, 400)
            self.assertIn("version failed", response.get_json()["message"])
        with app.test_request_context("/api/productization-upgrade-summary/issues?version_id=88"):
            response, status = tool_handlers.productization_upgrade_summary_issues()
            self.assertEqual(status, 400)
            self.assertIn("issues failed", response.get_json()["message"])
        with app.test_request_context("/api/productization-upgrade-summary/llm-descriptions?version_id=88"):
            response, status = tool_handlers.productization_upgrade_summary_llm_descriptions()
            self.assertEqual(status, 400)
            self.assertEqual(response.get_json()["error_code"], "classified")

        unexpected_handlers = build_productization_handlers(build_ctx(UnexpectedBPMISClient()))
        with app.test_request_context("/api/productization-upgrade-summary/versions?q=26Q2"):
            response, status = unexpected_handlers.productization_upgrade_summary_versions()
            self.assertEqual(status, 500)
            self.assertIn("Unable to search", response.get_json()["message"])
        with app.test_request_context("/api/productization-upgrade-summary/issues?version_id=88"):
            response, status = unexpected_handlers.productization_upgrade_summary_issues()
            self.assertEqual(status, 500)
            self.assertIn("Unable to load", response.get_json()["message"])
        with app.test_request_context("/api/productization-upgrade-summary/llm-descriptions?version_id=88"):
            response, status = unexpected_handlers.productization_upgrade_summary_llm_descriptions()
            self.assertEqual(status, 500)
            self.assertEqual(response.get_json()["error_code"], "server_error")
        self.assertTrue(events)

    def test_seatalk_todo_store_persistence_and_similarity_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "todos.json"
            path.write_text("not-json", encoding="utf-8")
            store = SeaTalkTodoStore(path)
            self.assertEqual(store.completed_ids(owner_email="owner@npt.sg"), set())
            self.assertEqual(store.processed_until(owner_email="owner@npt.sg"), "")
            path.write_text("[]", encoding="utf-8")
            self.assertEqual(SeaTalkTodoStore(path).open_todos(owner_email="owner@npt.sg"), [])

            self.assertEqual(store.merge_open_todos(owner_email="", todos=[{"task": "x"}]), [])
            store.mark_processed_until(owner_email="", processed_until="2026-05-02")
            store.mark_processed_until(owner_email="owner@npt.sg", processed_until="")
            store.mark_processed_until(owner_email="owner@npt.sg", processed_until="2026-05-02")
            store.mark_processed_until(owner_email="owner@npt.sg", processed_until="2026-05-01")
            self.assertEqual(store.processed_until(owner_email="OWNER@npt.sg"), "2026-05-02")

            original = {
                "domain": "AF",
                "task": "Prepare AF productization review with backend and QA owners",
                "due": "2026-05-23",
                "priority": "low",
                "evidence": "",
            }
            duplicate = {
                "domain": "AF",
                "task": "Prepare AF productization review with backend and QA owners",
                "due": "2026-05-23",
                "priority": "high",
                "evidence": "SeaTalk thread",
            }
            open_items = store.merge_open_todos(owner_email="owner@npt.sg", todos=[original, "bad", duplicate])
            self.assertEqual(len(open_items), 1)
            self.assertEqual(open_items[0]["priority"], "high")
            self.assertEqual(open_items[0]["evidence"], "SeaTalk thread")

            todo_id = open_items[0]["id"]
            completed = store.mark_completed(owner_email="owner@npt.sg", todo=open_items[0])
            self.assertEqual(completed["todo_id"], todo_id)
            self.assertEqual(store.completed_ids(owner_email="owner@npt.sg"), {todo_id})
            self.assertEqual(store.open_todos(owner_email="owner@npt.sg"), [])
            self.assertEqual(store.merge_open_todos(owner_email="owner@npt.sg", todos=[open_items[0]]), [])

            with self.assertRaisesRegex(ToolError, "signed-in owner"):
                store.mark_completed(owner_email="", todo={})

            store._payload["owners"]["owner@npt.sg"]["open"] = {"bad": "not-a-dict"}
            self.assertIsNone(SeaTalkTodoStore._find_similar_open_todo_id(open_items={"bad": "x"}, todo=original))

            no_path_store = SeaTalkTodoStore()
            no_path_store.mark_processed_until(owner_email="owner@npt.sg", processed_until="2026-05-03")

            with patch("bpmis_jira_tool.seatalk_stores.os.replace", side_effect=OSError):
                store.mark_processed_until(owner_email="owner@npt.sg", processed_until="2026-05-04")

        self.assertFalse(SeaTalkTodoStore._todos_are_similar({"task": ""}, {"task": "x"}))
        self.assertFalse(
            SeaTalkTodoStore._todos_are_similar(
                {"domain": "AF", "task": "Prepare review one", "due": "2026-05-01"},
                {"domain": "Credit", "task": "Prepare review two", "due": "2026-05-01"},
            )
        )
        self.assertFalse(
            SeaTalkTodoStore._todos_are_similar(
                {"domain": "AF", "task": "prepare review", "due": "2026-05-01"},
                {"domain": "AF", "task": "prepare review followup", "due": "2026-05-02"},
            )
        )
        self.assertTrue(
            SeaTalkTodoStore._todos_are_similar(
                {
                    "domain": "AF",
                    "task": "Review release candidate evidence",
                    "due": "unknown",
                    "evidence": "risk control regression smoke deployment approval",
                },
                {
                    "domain": "AF",
                    "task": "Check production validation notes",
                    "due": "",
                    "evidence": "risk control regression smoke deployment checklist",
                },
            )
        )
        self.assertFalse(
            SeaTalkTodoStore._todos_are_similar(
                {"domain": "AF", "task": "x", "due": "unknown", "evidence": ""},
                {"domain": "AF", "task": "y", "due": "unknown", "evidence": ""},
            )
        )
        with patch.object(SeaTalkTodoStore, "_similarity_text", return_value="release-risk normal"):
            self.assertIn("release", SeaTalkTodoStore._informative_todo_tokens({}))
        self.assertEqual(
            SeaTalkTodoStore._merge_similar_open_todo(
                existing={"priority": "medium", "due": "unknown"},
                incoming={"priority": "low", "due": "2026-05-23"},
                todo_id="todo-1",
            )["due"],
            "2026-05-23",
        )
        self.assertTrue(SeaTalkTodoStore._todo_due_compatible("unknown", "tomorrow"))
        self.assertTrue(SeaTalkTodoStore._todo_due_compatible("tomorrow", "tomorrow"))
        self.assertEqual(SeaTalkTodoStore._token_overlap_score("", "x"), 0.0)

    def test_seatalk_name_mapping_store_normalization_and_persistence_boundaries(self):
        self.assertEqual(SeaTalkNameMappingStore.normalize_key("UID 123"), "UID 123")
        self.assertEqual(SeaTalkNameMappingStore.normalize_key("unknown"), "")
        self.assertTrue(SeaTalkNameMappingStore.is_ignored_key("0"))
        self.assertEqual(SeaTalkNameMappingStore.person_aliases("buddy-123"), {"UID 123"})
        self.assertEqual(SeaTalkNameMappingStore.person_aliases("UID 123"), {"buddy-123"})
        self.assertEqual(SeaTalkNameMappingStore.canonical_display_key("buddy-123"), "UID 123")
        self.assertEqual(SeaTalkNameMappingStore.canonical_display_key("group-1"), "group-1")
        self.assertEqual(SeaTalkNameMappingStore.normalize_mappings(["bad"]), {})
        self.assertEqual(
            SeaTalkNameMappingStore.missing_mappings({"UID 123": "Alice"}, {"buddy-123": "Alice", "group-1": "Group"}),
            {"group-1": "Group"},
        )
        self.assertEqual(SeaTalkNameMappingStore().replace_mappings({"group-2": "No Disk"}), {"group-2": "No Disk"})

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "names.json"
            path.write_text(json.dumps({"buddy-123": " Alice   Tan ", "UID 0": "Ignored"}), encoding="utf-8")
            store = SeaTalkNameMappingStore(path)
            self.assertEqual(store.mappings()["UID 123"], "Alice Tan")
            replaced = store.replace_mappings({"group-1": "Team Chat"})
            self.assertEqual(replaced, {"group-1": "Team Chat"})
            merged = store.merge_mappings({"UID 456": "Bob"})
            self.assertEqual(merged["buddy-456"], "Bob")

            corrupt_path = Path(temp_dir) / "bad.json"
            corrupt_path.write_text("not-json", encoding="utf-8")
            self.assertEqual(SeaTalkNameMappingStore(corrupt_path).mappings(), {})
            list_path = Path(temp_dir) / "list.json"
            list_path.write_text("[]", encoding="utf-8")
            self.assertEqual(SeaTalkNameMappingStore(list_path).mappings(), {})

            with patch("bpmis_jira_tool.seatalk_stores.os.replace", side_effect=OSError):
                store.replace_mappings({"group-3": "Persist Error"})

    def test_job_store_covers_missing_storage_refresh_and_history_helpers(self):
        memory_store = JobStore()
        self.assertIsNone(memory_store.snapshot("missing"))
        memory_job = memory_store.create("memory-action", "Memory")
        memory_store.set_owner(memory_job.job_id, "OWNER@NPT.SG")
        memory_store.set_query_mode(memory_job.job_id, " DEEP ")
        memory_store.set_record_id(memory_job.job_id, " record-1 ")
        memory_store.update_queue_metadata(
            memory_job.job_id,
            queued_position=-1,
            eta_seconds_range=[-3, 9, 20],
            running_user_count=-2,
            message="Queued",
        )
        snapshot = memory_store.snapshot(memory_job.job_id)
        self.assertEqual(snapshot["owner_email"], "owner@npt.sg")
        self.assertEqual(snapshot["query_mode"], "deep")
        self.assertEqual(snapshot["record_id"], "record-1")
        self.assertEqual(snapshot["queued_position"], 0)
        self.assertEqual(snapshot["eta_seconds_range"], [0, 9])
        self.assertEqual(snapshot["running_user_count"], 0)

        memory_store.update(memory_job.job_id, state="running", stage="work", message="Running")
        memory_store._jobs[memory_job.job_id].last_progress_at = time.time() - 240
        stalled = memory_store.snapshot(memory_job.job_id)
        self.assertTrue(stalled["stalled_retryable"])
        self.assertTrue(stalled["error_retryable"])
        self.assertIsNotNone(memory_store.active_for_record("memory-action", owner_email="owner@npt.sg", record_id="record-1"))

        memory_store.complete(memory_job.job_id, results=[{"status": "ok"}], notice={"summary": "done"})
        self.assertEqual(memory_store.latest_completed_result("memory-action")["status"], "ok")
        self.assertEqual(memory_store.p95_duration_seconds("missing", default_seconds=77), 77)
        self.assertEqual(memory_store.list_snapshots(action="memory-action", owner_email="owner@npt.sg", limit=999)[0]["state"], "completed")
        self.assertIsNone(memory_store.active_for_record("memory-action", owner_email="owner@npt.sg", record_id="missing"))

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "jobs.json"
            path.write_text("not-json", encoding="utf-8")
            self.assertEqual(JobStore(path)._jobs, {})

            pending_job = JobState(job_id="pending", action="report", state="queued").__dict__
            path.write_text(
                json.dumps(
                    {
                        "jobs": {
                            "bad": "x",
                            "invalid": {"job_id": "invalid"},
                            "pending": pending_job,
                            "done": {
                                **JobState(job_id="done", action="report", state="completed", results=["bad"]).__dict__,
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            disk_store = JobStore(path)
            self.assertIsNone(disk_store.latest_completed_result("report"))
            self.assertEqual(disk_store.snapshot("pending")["state"], "failed")

            refresh_store = JobStore(path)
            refresh_store._jobs = {}
            refresh_store.set_owner("done", "OWNER@npt.sg")
            refresh_store._jobs = {}
            refresh_store.set_query_mode("done", "balanced")
            refresh_store._jobs = {}
            refresh_store.set_record_id("done", "record-x")
            refresh_store._jobs = {}
            refresh_store.update_queue_metadata("done", queued_position=1)
            refresh_store._jobs = {}
            refresh_store.update(
                "done",
                state="running",
                estimated_prompt_tokens=123,
                token_risk="medium",
            )
            refresh_store._jobs = {}
            refresh_store.complete("done", results=[{"status": "fresh"}], notice={})
            refresh_store._jobs = {}
            refresh_store.fail("done", "failed", error_category="system", error_code="boom", error_retryable=False)

            with patch("bpmis_jira_tool.job_store.os.replace", side_effect=OSError):
                disk_store.create("persist-error", "Persist Error")

    def test_source_code_qa_codex_refs_resolve_path_boundaries(self):
        self.assertEqual(extract_direct_file_refs("See app.py:1 and lib/util.ts:2-4"), ["app.py:1", "lib/util.ts:2-4"])
        self.assertEqual(codex_repo_relative_root(Path("/tmp/outside"), Path("/tmp/parent")), "")

        layers = codex_candidate_path_layers(
            [
                {"repo": "repo", "path": "seen.py", "file_exists": True},
                {"repo": "repo", "path": "direct.py", "file_exists": True, "trace_stage": "direct"},
                {"repo": "repo", "path": "support.py", "file_exists": True, "trace_stage": "weak"},
                {"repo": "repo", "path": "missing.py", "file_exists": False},
            ],
            {
                "codex_inspected_paths": [{"repo": "repo", "path": "seen.py"}],
                "codex_candidate_paths": [{"repo": "repo", "path": "memory.py", "trace_stage": "followup_memory"}],
            },
        )
        self.assertEqual([item["path"] for item in layers["confirmed_previous_paths"]], ["seen.py"])
        self.assertEqual([item["path"] for item in layers["current_high_confidence_paths"]], ["direct.py"])
        self.assertEqual([item["path"] for item in layers["current_supporting_paths"]], ["support.py"])
        self.assertEqual([item["path"] for item in layers["maybe_relevant_paths"]], ["missing.py"])

        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir)
            repo = parent / "repo"
            repo.mkdir()
            src = repo / "src"
            src.mkdir()
            target = src / "app.py"
            target.write_text("one\ntwo\nthree\n", encoding="utf-8")
            outside = parent / "outside.py"
            outside.write_text("outside\n", encoding="utf-8")
            outside_dir = parent / "outside-dir"
            outside_dir.mkdir()
            (outside_dir / "escaped.py").write_text("escaped\n", encoding="utf-8")
            (repo / "linked").symlink_to(outside_dir, target_is_directory=True)
            candidates = [
                {
                    "repo": "repo",
                    "repo_root": str(repo),
                    "repo_relative_root": "repo",
                    "path": "src/app.py",
                    "file_exists": True,
                },
                {
                    "repo": "repo-copy",
                    "repo_root": str(repo),
                    "repo_relative_root": "repo",
                    "path": "src/app.py",
                    "file_exists": True,
                }
            ]

            self.assertEqual(resolve_codex_file_ref("bad-ref", candidates, repo_root=parent)["status"], "invalid")
            self.assertEqual(resolve_codex_file_ref("src/app.py:0", candidates, repo_root=parent)["reason"], "invalid line range")
            self.assertEqual(resolve_codex_file_ref("../secret.py:1", candidates, repo_root=parent)["reason"], "unsafe path")
            self.assertEqual(resolve_codex_file_ref("other:src/app.py:1", candidates, repo_root=parent)["reason"], "no scoped repo root matched")
            self.assertEqual(resolve_codex_file_ref("src/app.py:1", [], repo_root=parent)["reason"], "no scoped repo root matched")
            self.assertEqual(
                resolve_codex_file_ref(
                    "src/app.py:1",
                    [],
                    repo_root=parent,
                    scope_roots=[{"repo": "empty", "repo_root": ""}],
                )["reason"],
                "no scoped repo root matched",
            )

            absolute_ok = resolve_codex_file_ref(str(target) + ":2-3", candidates, repo_root=parent)
            self.assertEqual(absolute_ok["status"], "ok")
            self.assertEqual(absolute_ok["path"], "src/app.py")
            self.assertEqual(
                resolve_codex_file_ref(str(outside) + ":1", candidates, repo_root=parent)["reason"],
                "absolute path outside selected scope",
            )

            relative_root_ok = resolve_codex_file_ref("repo/src/app.py:1", candidates, repo_root=parent)
            self.assertEqual(relative_root_ok["repo_relative_root"], "repo")
            self.assertEqual(
                resolve_codex_file_ref("repo:repo/src/app.py:1", candidates, repo_root=parent)["status"],
                "ok",
            )
            self.assertEqual(
                resolve_codex_file_ref(
                    "repo/src/app.py:1",
                    [],
                    repo_root=parent,
                    scope_roots=[
                        {"repo": "other", "repo_root": str(parent / "other"), "repo_relative_root": "other"},
                        {"repo": "repo", "repo_root": str(repo), "repo_relative_root": "repo"},
                    ],
                )["status"],
                "ok",
            )
            self.assertEqual(
                resolve_codex_file_ref(
                    "repo/src/app.py:1",
                    [
                        {
                            "repo": "repo",
                            "repo_root": str(repo),
                            "path": "src/app.py",
                            "file_exists": True,
                        }
                    ],
                    repo_root=parent,
                )["status"],
                "ok",
            )
            self.assertEqual(
                resolve_codex_file_ref("src/app.py:1", candidates, repo_root=repo)["status"],
                "ok",
            )
            self.assertEqual(
                resolve_codex_file_ref("outside.py:1", candidates, repo_root=parent)["reason"],
                "relative repo path outside selected scope",
            )
            self.assertEqual(
                resolve_codex_file_ref("linked/escaped.py:1", candidates, repo_root=parent)["reason"],
                "file or line range not found",
            )
            self.assertEqual(resolve_codex_file_ref("src/app.py:4", candidates, repo_root=parent)["reason"], "file or line range not found")
            self.assertEqual(resolve_codex_file_ref("missing.py:1", candidates, repo_root=parent)["reason"], "file or line range not found")

            scoped = resolve_codex_file_ref(
                "src/app.py:1",
                [],
                repo_root=parent,
                scope_roots=[{"repo": "repo", "repo_root": str(repo), "repo_relative_root": "repo"}],
            )
            self.assertEqual(scoped["status"], "ok")

            self.assertEqual(
                codex_resolved_file_ref_payload(
                    ref="outside.py:1",
                    candidate=outside,
                    relative_path=Path("outside.py"),
                    root=repo,
                    repo="repo",
                    repo_relative_root="repo",
                    start=1,
                    end=1,
                )["reason"],
                "resolved path outside selected scope",
            )
            self.assertEqual(
                codex_resolved_file_ref_payload(
                    ref="missing.py:1",
                    candidate=repo / "missing.py",
                    relative_path=Path("missing.py"),
                    root=repo,
                    repo="repo",
                    repo_relative_root="repo",
                    start=1,
                    end=1,
                )["reason"],
                "file not found",
            )
            with patch("pathlib.Path.read_text", side_effect=OSError("nope")):
                unreadable = codex_resolved_file_ref_payload(
                    ref="src/app.py:1",
                    candidate=target,
                    relative_path=Path("src/app.py"),
                    root=repo,
                    repo="repo",
                    repo_relative_root="repo",
                    start=1,
                    end=1,
                )
            self.assertIn("file unreadable", unreadable["reason"])

    def test_gmail_seatalk_split_handlers_cover_access_scope_and_fallback_boundaries(self):
        app = Flask(__name__)

        class FakeTodoStore:
            def processed_until(self, *, owner_email):
                return "2026-05-01"

            def completed_ids(self, *, owner_email):
                return set()

            def open_todos(self, *, owner_email):
                return [{"task": "Open saved todo", "domain": "AF"}]

            def merge_open_todos(self, *, owner_email, todos):
                return list(todos)

            def mark_processed_until(self, *, owner_email, processed_until):
                self.processed_until_value = processed_until

            def mark_completed(self, *, owner_email, todo):
                return {"status": "ok", "todo_id": "todo-1"}

        class FakeNameStore:
            def __init__(self):
                self.saved = {}

            def mappings(self):
                return dict(self.saved)

            def merge_mappings(self, mappings):
                self.saved.update(mappings)
                return dict(self.saved)

        class FakeDashboardService:
            def __init__(self, mode="ok"):
                self.mode = mode

            def get_cached_export_history_text(self, batch=1):
                return None

            def export_history_text(self, batch=1):
                return ("content", f"gmail-{batch}.txt")

            def prewarm_export_history_text(self, batch=1):
                return None

            def build_insights(self, *, todo_since=""):
                if self.mode == "broken_insights":
                    raise RuntimeError("token=secret-insights")
                return {
                    "todo_processed_until": "2026-05-02",
                    "my_todos": [{"task": "Follow up", "domain": "AF"}],
                    "project_updates": [],
                    "team_todos": [{"task": "hidden"}],
                }

            def build_name_mappings(self, *, force_refresh=False):
                return {
                    "auto_mappings": {"UID 123": "Alice"},
                    "unknown_ids": [{"id": "UID 123"}, {"id": "UID 456"}],
                }

            def export_history_text(self):
                return ("seatalk", "seatalk.txt")

        name_store = FakeNameStore()
        releases = []
        scope_enabled = True
        gate_enabled = False

        ctx = SimpleNamespace(
            settings=SimpleNamespace(gmail_seatalk_demo_owner_email="owner@npt.sg"),
            web_globals={
                "_build_gmail_dashboard_service": lambda: FakeDashboardService(),
                "_build_seatalk_dashboard_service": lambda settings: FakeDashboardService(),
            },
            GMAIL_READONLY_SCOPE=("gmail.readonly",),
            _require_gmail_seatalk_demo_access=lambda settings, api=False: (("blocked", 403) if gate_enabled else None),
            _google_credentials_have_scopes=lambda scopes: scope_enabled,
            _classify_portal_error=lambda error: {"error_message": str(error)},
            _log_portal_event=lambda *args, **kwargs: None,
            _build_request_log_context=lambda *args, **kwargs: {},
            _get_user_identity=lambda settings: "OWNER@NPT.SG",
            _safe_email_identity=lambda value: str(value).strip().lower(),
            _try_acquire_gmail_export_lock=lambda email: True,
            _release_gmail_export_lock=lambda email: releases.append(email),
            _current_google_email=lambda: "owner@npt.sg",
            _get_seatalk_todo_store=lambda settings: FakeTodoStore(),
            _get_seatalk_name_mapping_store=lambda settings: name_store,
            _callable_accepts_keyword=lambda func, keyword: keyword in inspect.signature(func).parameters,
            _dedupe_seatalk_name_mapping_candidates=lambda rows: rows,
        )
        handlers = build_gmail_seatalk_handlers(ctx)

        with app.test_request_context("/api/gmail-sea-talk-demo/network"):
            scope_enabled = False
            response, status = handlers.gmail_seatalk_demo_network_api()
            self.assertEqual(status, 400)
            self.assertIn("Gmail access", response.get_json()["message"])
        with app.test_request_context("/api/gmail-sea-talk-demo/gmail/export-manifest"):
            response, status = handlers.gmail_seatalk_demo_gmail_export_manifest()
            self.assertEqual(status, 400)
        with app.test_request_context("/api/gmail-sea-talk-demo/gmail/export-prewarm", method="POST"):
            response, status = handlers.gmail_seatalk_demo_gmail_export_prewarm()
            self.assertEqual(status, 400)

        scope_enabled = True
        gate_enabled = True
        for path, method, handler in [
            ("/api/gmail-sea-talk-demo/gmail/export", "GET", handlers.gmail_seatalk_demo_gmail_export),
            ("/api/gmail-sea-talk-demo/network", "GET", handlers.gmail_seatalk_demo_network_api),
            ("/api/gmail-sea-talk-demo/gmail/export-manifest", "GET", handlers.gmail_seatalk_demo_gmail_export_manifest),
            ("/api/gmail-sea-talk-demo/gmail/export-prewarm", "POST", handlers.gmail_seatalk_demo_gmail_export_prewarm),
            ("/api/gmail-sea-talk-demo/seatalk", "GET", handlers.gmail_seatalk_demo_seatalk_api),
            ("/api/gmail-sea-talk-demo/seatalk/insights", "GET", handlers.gmail_seatalk_demo_seatalk_insights_api),
            ("/api/gmail-sea-talk-demo/seatalk/project-updates", "GET", handlers.gmail_seatalk_demo_seatalk_project_updates_api),
            ("/api/gmail-sea-talk-demo/seatalk/todos/open", "GET", handlers.gmail_seatalk_demo_seatalk_open_todos_api),
            ("/api/gmail-sea-talk-demo/seatalk/todos", "GET", handlers.gmail_seatalk_demo_seatalk_todos_api),
            ("/api/gmail-sea-talk-demo/seatalk/todos/complete", "POST", handlers.gmail_seatalk_demo_seatalk_todo_complete),
            ("/api/gmail-sea-talk-demo/seatalk/name-mappings", "GET", handlers.gmail_seatalk_demo_seatalk_name_mappings),
            ("/api/gmail-sea-talk-demo/seatalk/export", "GET", handlers.gmail_seatalk_demo_seatalk_export),
        ]:
            with app.test_request_context(path, method=method):
                self.assertEqual(handler(), ("blocked", 403))

        gate_enabled = False
        with app.test_request_context("/api/gmail-sea-talk-demo/gmail/export-prewarm?batch=2", method="POST"):
            response, status = handlers.gmail_seatalk_demo_gmail_export_prewarm()
            self.assertEqual(status, 200)
            self.assertTrue(response.get_json()["cached"])
        self.assertEqual(releases[-1], "owner@npt.sg")

        with app.test_request_context("/api/gmail-sea-talk-demo/seatalk/todos"):
            response = handlers.gmail_seatalk_demo_seatalk_todos_api()
            self.assertEqual(response.get_json()["my_todos"][0]["task"], "Follow up")

        ctx.web_globals["_build_seatalk_dashboard_service"] = lambda settings: FakeDashboardService("broken_insights")
        with app.test_request_context("/api/gmail-sea-talk-demo/seatalk/insights"):
            response, status = handlers.gmail_seatalk_demo_seatalk_insights_api()
            self.assertEqual(status, 500)
            self.assertNotIn("secret", response.get_json()["message"])

        ctx.web_globals["_build_seatalk_dashboard_service"] = lambda settings: FakeDashboardService()
        with app.test_request_context("/api/gmail-sea-talk-demo/seatalk/name-mappings?refresh=1"):
            response = handlers.gmail_seatalk_demo_seatalk_name_mappings()
            payload = response.get_json()
            self.assertEqual(payload["mappings"]["UID 123"], "Alice")
            self.assertEqual([row["id"] for row in payload["unknown_ids"]], ["UID 456"])

        registered = []
        fake_app = SimpleNamespace(add_url_rule=lambda *args, **kwargs: registered.append((args, kwargs)))
        register_gmail_seatalk_routes(fake_app, handlers)
        self.assertEqual(len(registered), 14)

    def test_bpmis_split_handlers_cover_config_admin_and_jira_boundaries(self):
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.add_url_rule("/", endpoint="index", view_func=lambda: "index")
        app.add_url_rule("/access-denied", endpoint="access_denied", view_func=lambda: "denied")
        calls = []
        login_gate = None
        user_identity = {"config_key": "user-1", "email": "owner@npt.sg"}
        admin_enabled = True

        class FakeConfigStore:
            def _normalize(self, config):
                normalized = dict(config)
                normalized.setdefault("component_by_market", {})
                normalized.setdefault("component_default_rules_text", "")
                return normalized

            def _parse_component_route_rules(self, text):
                calls.append(("parse", text))
                if "bad-route" in text:
                    raise ToolError("bad route")

            def align_component_defaults_to_routes(self, route_text, seed_text):
                calls.append(("align", route_text, seed_text))
                return f"aligned:{seed_text or 'empty'}"

            def build_field_mappings(self, config):
                calls.append(("field_mappings", config.get("pm_team")))

        class FakeProjectStore:
            def list_projects(self, *, user_key):
                calls.append(("list_projects", user_key))
                return [{"bpmis_id": "225159"}]

            def soft_delete_project(self, *, user_key, bpmis_id):
                calls.append(("delete_project", user_key, bpmis_id))
                return True

            def reorder_projects(self, *, user_key, bpmis_ids):
                calls.append(("reorder_projects", user_key, bpmis_ids))
                return [{"bpmis_id": bpmis_ids[0]}] if bpmis_ids else []

            def update_project_comment(self, *, user_key, bpmis_id, pm_comment):
                calls.append(("comment", user_key, bpmis_id, pm_comment))
                return {"bpmis_id": bpmis_id, "pm_comment": pm_comment}

        class FakeJiraService:
            def __init__(self):
                self.fail_next = None

            def _maybe_fail(self, method):
                if self.fail_next == method:
                    self.fail_next = None
                    raise ToolError(f"{method} failed")

            def jira_options(self, *, user_key, bpmis_id):
                self._maybe_fail("jira_options")
                return {"statuses": ["Open"], "versions": [{"id": "v1"}]}

            def list_tickets(self, *, user_key, bpmis_id, include_live):
                self._maybe_fail("list_tickets")
                return [{"ticket_id": "T1", "include_live": include_live}]

            def delete_ticket(self, *, user_key, bpmis_id, ticket_id):
                self._maybe_fail("delete_ticket")
                return True

            def update_ticket_status(self, *, user_key, bpmis_id, ticket_id, status):
                self._maybe_fail("update_ticket_status")
                return {"ticket_id": ticket_id, "status": status}

            def update_ticket_version(self, *, user_key, bpmis_id, ticket_id, version_name, version_id):
                self._maybe_fail("update_ticket_version")
                return {"ticket_id": ticket_id, "version_name": version_name, "version_id": version_id}

            def create_tickets(self, *, user_key, bpmis_id, items):
                self._maybe_fail("create_tickets")
                return items

        config_store = FakeConfigStore()
        project_store = FakeProjectStore()
        jira_service = FakeJiraService()
        saved_configs = []
        team_profiles = {"AF": {"label": "Anti-Fraud", "component_default_rules_text": ""}}

        def require_login(settings, api=False):
            return login_gate

        def load_user_config(settings, identity):
            return {
                "pm_team": "AF",
                "component_route_rules_text": "AF | SG | Existing",
                "component_default_rules_text": "Existing | owner | dev | qa | plan",
                "component_by_market": {"SG": "Existing"},
            }

        def validate_security(settings, config):
            if config.get("pm_team") == "BADSEC":
                raise ToolError("security failed")

        def save_team_profile(settings, config_store_arg, team_key, profile):
            calls.append(("save_team_profile", team_key, profile["component_route_rules_text"]))
            if "bad-route" in profile["component_route_rules_text"]:
                raise ToolError("profile failed")
            return {"component_route_rules_text": profile["component_route_rules_text"], "component_default_rules_text": "default"}

        ctx = SimpleNamespace(
            settings=_settings(),
            config_store=config_store,
            MARKET_KEYS=("SG", "ID"),
            _require_google_login=require_login,
            _get_user_identity=lambda settings: user_identity,
            _load_user_config_for_identity=load_user_config,
            _apply_sync_email_policy=lambda config, identity: config.setdefault("sync_pm_email", identity["email"]),
            _hydrate_setup_defaults=lambda config, identity, team_profiles=None: config,
            _load_effective_team_profiles=lambda store: team_profiles,
            _validate_config_security=validate_security,
            _save_user_config_for_identity=lambda settings, identity, config: saved_configs.append((identity, config)),
            _log_portal_event=lambda *args, **kwargs: calls.append(("log", args[0], kwargs)),
            _build_request_log_context=lambda settings, user_identity=None, extra=None: {"user": user_identity, "extra": extra or {}},
            _build_mapping_log_summary=lambda config, save_mode=None: {"pm_team": config.get("pm_team"), "save_mode": save_mode},
            _classify_portal_error=lambda error: {"error_message": str(error)},
            _validate_team_profile_setup=lambda config, team_profiles=None: calls.append(("validate_team", config.get("pm_team"))),
            _is_team_profile_admin=lambda identity: admin_enabled,
            _save_team_profile=save_team_profile,
            _count_configured_lines=lambda text: len([line for line in str(text).splitlines() if line.strip()]),
            _start_job=lambda job_type: (calls.append(("start_job", job_type)) or {"job_type": job_type}),
            _get_bpmis_project_store=lambda: project_store,
            _build_portal_jira_creation_service=lambda settings: jira_service,
        )
        handlers = build_bpmis_handlers(ctx)

        login_gate = ("blocked", 403)
        with app.test_request_context("/config/save", method="POST"):
            self.assertEqual(handlers.save_mapping_config(), ("blocked", 403))
        with app.test_request_context("/config/save-route", method="POST", json={}):
            self.assertEqual(handlers.save_mapping_route_only(), ("blocked", 403))
        with app.test_request_context("/admin/team-profiles/save", method="POST"):
            self.assertEqual(handlers.save_team_profile_admin(), ("blocked", 403))
        for path, method, handler, args in [
            ("/api/bpmis-projects", "GET", handlers.bpmis_projects, ()),
            ("/api/bpmis-projects/225159", "DELETE", handlers.delete_bpmis_project, ("225159",)),
            ("/api/bpmis-projects/order", "PATCH", handlers.reorder_bpmis_projects, ()),
            ("/api/bpmis-projects/225159/comment", "PATCH", handlers.update_bpmis_project_comment, ("225159",)),
            ("/api/bpmis-projects/225159/jira-options", "GET", handlers.bpmis_project_jira_options, ("225159",)),
            ("/api/bpmis-projects/225159/jira-tickets?live=true", "GET", handlers.bpmis_project_jira_tickets, ("225159",)),
            ("/api/bpmis-projects/225159/jira-tickets/T1", "DELETE", handlers.delete_bpmis_project_jira_ticket, ("225159", "T1")),
            ("/api/bpmis-projects/225159/jira-tickets/T1/status", "PATCH", handlers.update_bpmis_project_jira_ticket_status, ("225159", "T1")),
            ("/api/bpmis-projects/225159/jira-tickets/T1/version", "PATCH", handlers.update_bpmis_project_jira_ticket_version, ("225159", "T1")),
            ("/api/bpmis-projects/225159/jira-tickets", "POST", handlers.create_bpmis_project_jira_tickets, ("225159",)),
        ]:
            with app.test_request_context(path, method=method, json={}):
                self.assertEqual(handler(*args), ("blocked", 403))

        login_gate = None
        with app.test_request_context(
            "/config/save",
            method="POST",
            data={"save_mode": "route_only", "pm_team": "AF", "component_route_rules_text": "AF | SG | Component"},
        ):
            self.assertEqual(handlers.save_mapping_config().status_code, 302)
        self.assertEqual(saved_configs[-1][1]["component_default_rules_text"], "aligned:Existing | owner | dev | qa | plan")

        with app.test_request_context("/config/save", method="POST", data={"pm_team": "AF"}):
            self.assertEqual(handlers.save_mapping_config().status_code, 302)
        self.assertIn(("field_mappings", "AF"), calls)

        with app.test_request_context("/config/save", method="POST", data={"pm_team": "BADSEC"}):
            self.assertEqual(handlers.save_mapping_config().status_code, 302)
            self.assertEqual(calls[-1][1], "config_save_tool_error")

        with app.test_request_context(
            "/config/save-route",
            method="POST",
            json={"pm_team": "AF", "component_route_rules_text": "AF | SG | Component", "component_default_rules_text": "Seed"},
        ):
            response = handlers.save_mapping_route_only()
            self.assertEqual(response.get_json()["component_default_rules_text"], "aligned:Seed")

        original_loader = ctx._load_user_config_for_identity
        ctx._load_user_config_for_identity = lambda settings, identity: (_ for _ in ()).throw(ToolError("load failed"))
        handlers = build_bpmis_handlers(ctx)
        with app.test_request_context("/config/save-route", method="POST", json={"pm_team": "AF"}):
            response, status = handlers.save_mapping_route_only()
            self.assertEqual(status, 400)
            self.assertEqual(response.get_json()["message"], "load failed")
        ctx._load_user_config_for_identity = original_loader
        handlers = build_bpmis_handlers(ctx)

        admin_enabled = False
        with app.test_request_context("/admin/team-profiles/save", method="POST", data={"team_key": "AF"}):
            self.assertEqual(handlers.save_team_profile_admin().status_code, 302)
        admin_enabled = True
        with app.test_request_context("/admin/team-profiles/save", method="POST", data={"team_key": "NOPE"}):
            self.assertEqual(handlers.save_team_profile_admin().status_code, 302)
            self.assertEqual(calls[-1][1], "team_profile_admin_save_tool_error")
        with app.test_request_context("/admin/team-profiles/save", method="POST", data={"team_key": "AF", "component_route_rules_text": "bad-route"}):
            self.assertEqual(handlers.save_team_profile_admin().status_code, 302)
            self.assertEqual(calls[-1][1], "team_profile_admin_save_tool_error")
        with app.test_request_context("/admin/team-profiles/save", method="POST", data={"team_key": "AF", "component_route_rules_text": "AF | SG | Component"}):
            self.assertEqual(handlers.save_team_profile_admin().status_code, 302)
            self.assertEqual(calls[-1][1], "team_profile_admin_save_success")

        self.assertEqual(handlers.create_sync_bpmis_projects_job(), {"job_type": "sync-bpmis-projects"})
        with app.test_request_context("/api/bpmis-projects"):
            self.assertEqual(handlers.bpmis_projects().get_json()["projects"], [{"bpmis_id": "225159"}])
        with app.test_request_context("/api/bpmis-projects/225159", method="DELETE"):
            self.assertTrue(handlers.delete_bpmis_project("225159").get_json()["deleted"])
        with app.test_request_context("/api/bpmis-projects/order", method="PATCH", json={"bpmis_ids": ["225160", None]}):
            self.assertEqual(handlers.reorder_bpmis_projects().get_json()["projects"][0]["bpmis_id"], "225160")
        with app.test_request_context("/api/bpmis-projects/225159/comment", method="PATCH", json={"pm_comment": "Follow up"}):
            self.assertEqual(handlers.update_bpmis_project_comment("225159").get_json()["updated"]["pm_comment"], "Follow up")

        with app.test_request_context("/api/bpmis-projects/225159/jira-options"):
            self.assertEqual(handlers.bpmis_project_jira_options("225159").get_json()["statuses"], ["Open"])
        jira_service.fail_next = "jira_options"
        with app.test_request_context("/api/bpmis-projects/225159/jira-options"):
            response, status = handlers.bpmis_project_jira_options("225159")
            self.assertEqual(status, 400)
            self.assertIn("jira_options failed", response.get_json()["message"])

        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets?live=yes"):
            self.assertTrue(handlers.bpmis_project_jira_tickets("225159").get_json()["tickets"][0]["include_live"])
        jira_service.fail_next = "list_tickets"
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets"):
            self.assertEqual(handlers.bpmis_project_jira_tickets("225159")[1], 400)

        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1", method="DELETE"):
            self.assertTrue(handlers.delete_bpmis_project_jira_ticket("225159", "T1").get_json()["deleted"])
        jira_service.fail_next = "delete_ticket"
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1", method="DELETE"):
            self.assertEqual(handlers.delete_bpmis_project_jira_ticket("225159", "T1")[1], 400)

        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1/status", method="PATCH", json={}):
            self.assertEqual(handlers.update_bpmis_project_jira_ticket_status("225159", "T1")[1], 400)
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1/status", method="PATCH", json={"status": "Done"}):
            self.assertEqual(handlers.update_bpmis_project_jira_ticket_status("225159", "T1").get_json()["ticket"]["status"], "Done")
        jira_service.fail_next = "update_ticket_status"
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1/status", method="PATCH", json={"status": "Done"}):
            self.assertEqual(handlers.update_bpmis_project_jira_ticket_status("225159", "T1")[1], 400)

        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1/version", method="PATCH", json={}):
            self.assertEqual(handlers.update_bpmis_project_jira_ticket_version("225159", "T1")[1], 400)
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1/version", method="PATCH", json={"version_id": "v1"}):
            self.assertEqual(handlers.update_bpmis_project_jira_ticket_version("225159", "T1").get_json()["ticket"]["version_id"], "v1")
        jira_service.fail_next = "update_ticket_version"
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets/T1/version", method="PATCH", json={"version_name": "26Q2"}):
            self.assertEqual(handlers.update_bpmis_project_jira_ticket_version("225159", "T1")[1], 400)

        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets", method="POST", json={"items": "bad"}):
            self.assertEqual(handlers.create_bpmis_project_jira_tickets("225159")[1], 400)
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets", method="POST", json={"items": [{"status": "skipped"}]}):
            response, status = handlers.create_bpmis_project_jira_tickets("225159")
            self.assertEqual(status, 400)
            self.assertEqual(response.get_json()["status"], "error")
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets", method="POST", json={"items": [{"status": "created"}]}):
            response, status = handlers.create_bpmis_project_jira_tickets("225159")
            self.assertEqual(status, 200)
            self.assertEqual(response.get_json()["status"], "ok")
        jira_service.fail_next = "create_tickets"
        with app.test_request_context("/api/bpmis-projects/225159/jira-tickets", method="POST", json={"items": [{"status": "created"}]}):
            self.assertEqual(handlers.create_bpmis_project_jira_tickets("225159")[1], 400)

        registered = []
        fake_app = SimpleNamespace(add_url_rule=lambda *args, **kwargs: registered.append((args, kwargs)))
        register_bpmis_routes(fake_app, handlers)
        self.assertEqual(len(registered), 14)


if __name__ == "__main__":
    unittest.main()
