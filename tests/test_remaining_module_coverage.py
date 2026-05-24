import io
import json
import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flask import Flask

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.job_lifecycle import JobLifecycle
from bpmis_jira_tool.job_store_registry import extract_job_snapshots
from bpmis_jira_tool.release_manifest import (
    _dirty_material,
    _git_value,
    build_release_manifest,
    load_release_manifest,
    main as release_manifest_main,
    manifest_file_sha256,
    write_release_manifest,
)
from bpmis_jira_tool.seatalk_daily_email import _build_daily_brief_evidence_context
from bpmis_jira_tool.source_code_qa import SourceCodeQAService
from bpmis_jira_tool.source_code_qa_codex_answer import build_codex_llm_answer
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS
from bpmis_jira_tool import web as web_module
from bpmis_jira_tool import web_runtime_status


def _settings(temp_dir: str, **overrides) -> Settings:
    values = {
        "flask_secret_key": "secret",
        "google_oauth_client_secret_file": Path(temp_dir) / "client.json",
        "google_oauth_redirect_uri": None,
        "team_portal_host": "127.0.0.1",
        "team_portal_port": 5000,
        "team_portal_base_url": "https://portal.example",
        "team_allowed_emails": (),
        "team_allowed_email_domains": (),
        "team_portal_data_dir": Path(temp_dir),
        "spreadsheet_id": "sheet",
        "common_tab_name": "Common",
        "input_tab_name": "Input",
        "bpmis_base_url": "https://bpmis.example",
        "bpmis_api_access_token": "token",
    }
    values.update(overrides)
    return Settings(**values)


def _fake_git_completed(stdout: str = ""):
    return SimpleNamespace(stdout=stdout)


def _git_side_effect(args, **_kwargs):
    joined = " ".join(args)
    if "rev-parse HEAD" in joined:
        return _fake_git_completed("abc123\n")
    if "diff --name-only" in joined:
        return _fake_git_completed("changed.py\n")
    if "diff --no-ext-diff" in joined:
        return _fake_git_completed("diff-bytes")
    if "ls-files --others" in joined:
        return _fake_git_completed(".venv/cache.py\nnew.py\n.team-portal/run.json\n")
    return _fake_git_completed("")


def _clean_git_side_effect(args, **_kwargs):
    joined = " ".join(args)
    if "rev-parse HEAD" in joined:
        return _fake_git_completed("clean123\n")
    return _fake_git_completed("")


class RemainingModuleCoverageTests(unittest.TestCase):
    def test_release_manifest_build_write_load_helpers_and_main(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output = root / "run" / "manifest.json"
            source_file = root / "source.txt"
            source_file.write_text("payload", encoding="utf-8")

            with patch("bpmis_jira_tool.release_manifest.subprocess.run", side_effect=_git_side_effect), patch(
                "bpmis_jira_tool.web_runtime_status.subprocess.run", side_effect=_git_side_effect
            ):
                manifest = build_release_manifest(root, surface="", host_root=root / "host", python_executable="python-test")

            self.assertTrue(manifest["dirty"])
            self.assertEqual(manifest["surface"], "mac_public_live")
            self.assertEqual(manifest["git_head"], "abc123")
            self.assertEqual(manifest["changed_files"], ["changed.py"])
            self.assertEqual(manifest["untracked_files"], ["new.py"])
            self.assertIn("-dirty-", manifest["release_revision"])
            self.assertEqual(manifest["python_executable"], "python-test")

            written = write_release_manifest(output, manifest)
            self.assertEqual(written["manifest_id"], manifest["manifest_id"])
            self.assertEqual(load_release_manifest(output)["manifest_id"], manifest["manifest_id"])
            self.assertIsNone(load_release_manifest(root / "missing.json"))
            bad_json = root / "bad.json"
            bad_json.write_text("[1, 2, 3]", encoding="utf-8")
            self.assertIsNone(load_release_manifest(bad_json))
            self.assertTrue(manifest_file_sha256(source_file))
            self.assertEqual(manifest_file_sha256(root / "missing.bin"), "")

            with patch("bpmis_jira_tool.release_manifest.subprocess.run", side_effect=FileNotFoundError):
                self.assertEqual(_git_value(root, "rev-parse", "HEAD"), "")
                self.assertEqual(_dirty_material(root), "")
            with patch("bpmis_jira_tool.release_manifest.subprocess.run", side_effect=_clean_git_side_effect):
                self.assertEqual(_dirty_material(root), "")

            with patch("bpmis_jira_tool.release_manifest.build_release_manifest", return_value={"manifest_id": "manifest-1"}) as build, patch(
                "bpmis_jira_tool.release_manifest.write_release_manifest"
            ) as write, patch("sys.stdout", new_callable=io.StringIO) as stdout:
                exit_code = release_manifest_main(["--root", str(root), "--output", str(output), "--surface", "uat", "--print-id"])
            self.assertEqual(exit_code, 0)
            build.assert_called_once()
            write.assert_called_once()
            self.assertEqual(stdout.getvalue().strip(), "manifest-1")

    def test_release_manifest_and_seatalk_daily_email_module_help_entrypoints(self):
        for module_name, argv0 in (
            ("bpmis_jira_tool.release_manifest", "release_manifest.py"),
            ("bpmis_jira_tool.seatalk_daily_email", "seatalk_daily_email.py"),
        ):
            with self.subTest(module=module_name), patch.object(sys, "argv", [argv0, "--help"]), patch("sys.stdout", new_callable=io.StringIO):
                with self.assertRaises(SystemExit) as raised:
                    runpy.run_module(module_name, run_name="__main__")
                self.assertEqual(raised.exception.code, 0)

    def test_runtime_status_revision_secret_and_untracked_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            web_runtime_status.current_release_revision.cache_clear()
            with patch.dict("os.environ", {"TEAM_PORTAL_RELEASE_REVISION": "pinned"}, clear=False):
                self.assertEqual(web_runtime_status.current_release_revision(root), "pinned")
            web_runtime_status.current_release_revision.cache_clear()

            with patch("bpmis_jira_tool.web_runtime_status.subprocess.run", side_effect=FileNotFoundError):
                self.assertEqual(web_runtime_status.source_tree_revision(root), "unknown")

            def diff_fails(args, **_kwargs):
                if "rev-parse" in args:
                    return _fake_git_completed("head-only\n")
                raise subprocess.CalledProcessError(1, args)

            with patch("bpmis_jira_tool.web_runtime_status.subprocess.run", side_effect=diff_fails):
                self.assertEqual(web_runtime_status.source_tree_revision(root), "head-only")

            with patch("bpmis_jira_tool.web_runtime_status.subprocess.run", side_effect=_git_side_effect):
                self.assertIn("-dirty-", web_runtime_status.source_tree_revision(root))
            with patch("bpmis_jira_tool.web_runtime_status.subprocess.run", side_effect=_clean_git_side_effect):
                self.assertEqual(web_runtime_status.source_tree_revision(root), "clean123")

            self.assertEqual(web_runtime_status.filtered_untracked_paths("\n.venv/x\nkeep.py\n.pytest_cache/y\n"), ["keep.py"])
            self.assertTrue(web_runtime_status.default_flask_session_secret(" dev-secret-key "))
            self.assertFalse(web_runtime_status.default_flask_session_secret("real-secret"))

    def test_job_lifecycle_and_registry_unhappy_paths(self):
        job_store = Mock()
        job_store.snapshot.side_effect = RuntimeError("store down")
        lifecycle = JobLifecycle(job_store)
        self.assertIsNone(lifecycle.snapshot("job-1"))
        self.assertFalse(lifecycle.is_terminal("job-1"))

        store_path = Path("/tmp/jobs.json")
        self.assertEqual(extract_job_snapshots({"jobs": "bad"}, store_key="portal", store_path=store_path), [])
        rows = extract_job_snapshots({"jobs": [{"job_id": "a"}, "skip"]}, store_key="portal", store_path=store_path, owner="portal")
        self.assertEqual(rows, [{"job_id": "a", "_store": "jobs.json", "_store_key": "portal", "_store_owner": "portal"}])

    def test_cloud_home_mac_full_portal_health_edges(self):
        app = Flask(__name__)
        app.add_url_rule("/healthz", "healthz", lambda: "ok")
        app.add_url_rule("/", "portal_home", lambda: "home")
        with tempfile.TemporaryDirectory() as temp_dir, app.test_request_context("/"):
            disabled = _settings(temp_dir, cloud_home_enabled=False)
            self.assertTrue(web_module._mac_full_portal_is_available(disabled, ""))
            self.assertTrue(web_module._full_portal_navigation_available(disabled))
            self.assertEqual(web_module._mac_full_portal_health_url(disabled, ""), "")
            self.assertEqual(web_module._mac_full_portal_health_url(disabled, "/local"), "http://localhost/healthz")
            self.assertEqual(
                web_module._mac_full_portal_health_url(disabled, "https://portal.example/team?x=1"),
                "https://portal.example/healthz",
            )
            self.assertEqual(web_module._mac_full_portal_health_url(disabled, "not-a-url"), "")

            cloud_missing = _settings(temp_dir, cloud_home_enabled=True, mac_full_portal_url="")
            self.assertFalse(web_module._mac_full_portal_is_available(cloud_missing, "https://fallback.example"))
            self.assertFalse(
                web_module._mac_full_portal_is_available(
                    _settings(temp_dir, cloud_home_enabled=True, mac_full_portal_url="not-a-url"),
                    "https://fallback.example",
                )
            )

            response = Mock()
            response.status_code = 503
            with patch("bpmis_jira_tool.web.requests.get", return_value=response):
                self.assertFalse(
                    web_module._mac_full_portal_is_available(
                        _settings(temp_dir, cloud_home_enabled=True, mac_full_portal_url="https://portal.example"),
                        "https://fallback.example",
                    )
                )
            response.close.assert_called_once()

            response_ok = Mock()
            response_ok.status_code = 200
            response_ok.json.return_value = {"status": "ok"}
            with patch("bpmis_jira_tool.web.requests.get", return_value=response_ok):
                self.assertTrue(
                    web_module._full_portal_navigation_available(
                        _settings(temp_dir, cloud_home_enabled=True, mac_full_portal_url="https://portal.example")
                    )
                )

            response_bad_json = Mock()
            response_bad_json.status_code = 200
            response_bad_json.json.side_effect = ValueError("bad json")
            with patch("bpmis_jira_tool.web.requests.get", return_value=response_bad_json):
                self.assertFalse(
                    web_module._mac_full_portal_is_available(
                        _settings(temp_dir, cloud_home_enabled=True, mac_full_portal_url="https://portal.example"),
                        "https://fallback.example",
                    )
                )

    def test_source_code_qa_structured_json_and_first_pass_skip_edges(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = SourceCodeQAService(
                data_root=Path(temp_dir),
                team_profiles=TEAM_PROFILE_DEFAULTS,
                gitlab_token="secret-token",
                git_timeout_seconds=5,
                max_file_bytes=200_000,
            )

            self.assertIsNone(service._structured_json_payload_from_text("{bad json"))
            with patch("bpmis_jira_tool.source_code_qa.json.loads", return_value=[]):
                self.assertIsNone(service._structured_json_payload_from_text('{"direct_answer":"x"}'))
            answer, structured = service._normalize_codex_answer_text("Plain answer", {})
            self.assertEqual(answer, "Plain answer")
            self.assertEqual(structured["direct_answer"], "Plain answer")

            base = {
                "question": "Can we confirm this source behavior?",
                "answer": "This bounded answer has enough detail to be meaningful and cites source behavior while noting a missing runtime check.",
                "structured_answer": {"claims": [{"text": "Source path exists", "citations": ["S1"]}], "confidence": "medium"},
                "quality_gate": {"status": "sufficient", "confidence": "medium"},
                "answer_judge": {},
                "codex_validation": {},
            }
            self.assertEqual(service._codex_first_pass_soft_repair_skip_reason(**base, repair_reasons=["unknown_soft_reason"]), "")
            self.assertEqual(
                service._codex_first_pass_soft_repair_skip_reason(
                    **{**base, "answer": "short", "structured_answer": {"confidence": "medium"}},
                    repair_reasons=["high_risk_claims_missing_scoped_file_evidence"],
                ),
                "",
            )
            self.assertEqual(
                service._codex_first_pass_soft_repair_skip_reason(
                    **{**base, "quality_gate": {"status": "sufficient", "confidence": "low"}, "structured_answer": {"confidence": "low"}},
                    repair_reasons=["high_risk_claims_missing_scoped_file_evidence"],
                ),
                "",
            )
            self.assertEqual(
                service._codex_first_pass_soft_repair_skip_reason(
                    **{
                        **base,
                        "answer": "This information is not present in the provided source code or runtime evidence. Please verify if the feature is implemented.",
                        "structured_answer": {"claims": [], "confidence": "medium"},
                    },
                    repair_reasons=["high_risk_claims_missing_scoped_file_evidence"],
                ),
                "",
            )
            self.assertEqual(
                service._codex_first_pass_soft_repair_skip_reason(
                    **{
                        **base,
                        "structured_answer": {
                            "not_found": ["Runtime evidence is not found for this bounded claim."],
                            "claims": [{"text": "Source path exists", "citations": ["S1"]}],
                            "confidence": "medium",
                        },
                    },
                    repair_reasons=["not_found_answer_conflicts_with_retrieval_hints"],
                ),
                "codex_repair_skipped:first_pass_bounded_negative_answer",
            )
            self.assertEqual(
                service._codex_first_pass_soft_repair_skip_reason(
                    **base,
                    repair_reasons=["high_risk_claims_missing_scoped_file_evidence"],
                ),
                "codex_repair_skipped:first_pass_sufficient_scoped_answer",
            )
            self.assertEqual(
                service._codex_first_pass_soft_repair_skip_reason(
                    **{
                        **base,
                        "structured_answer": {"claims": [{"text": "Uncited but specific claim"}], "confidence": "medium"},
                    },
                    repair_reasons=["high_risk_claims_missing_scoped_file_evidence"],
                ),
                "",
            )

    def test_seatalk_daily_brief_evidence_context_includes_source_caps(self):
        payload = json.loads(
            _build_daily_brief_evidence_context(
                unanswered_question_hints="- Please follow up\nPlain mention",
                team_member_reminder_candidates=None,
                evidence_refs=None,
                source_token_ledger={"seatalk_prompt_hit_cap": True, "gmail_prompt_hit_cap": False},
            )
        )

        self.assertEqual(payload["unanswered_mentions"], ["Please follow up", "Plain mention"])
        self.assertEqual(payload["source_caps"], {"seatalk_prompt_hit_cap": True, "gmail_prompt_hit_cap": False})

    def test_codex_answer_deep_budget_skip_and_timeout_update_edges(self):
        class FakeCodexAnswerService:
            def __init__(self, after_deep_result):
                self.llm_provider = SimpleNamespace(name="codex")
                self.llm_budgets = {"repair": {"model": "repair-model"}}
                self.codex_repair_deadline_seconds = 0
                self.codex_deep_repair_reserve_seconds = 0
                self.codex_repair_min_remaining_seconds = 5
                self.codex_repair_prompt_token_limit = 10_000
                self.after_deep_result = after_deep_result
                self.remaining_calls = 0

            def normalize_query_mode(self, query_mode):
                return query_mode

            def _codex_scope_roots(self, candidate_paths):
                return ["repo"]

            def _codex_prompt_mode(self, **_kwargs):
                return "prompt-mode"

            def _codex_initial_candidate_context(self, **kwargs):
                candidate_paths = [{"path": item.get("path", "a.py"), "repo": "repo"} for item in kwargs["selected_matches"]]
                return {
                    "candidate_matches": list(kwargs["selected_matches"]),
                    "candidate_paths": candidate_paths,
                    "candidate_path_layers": [{"path": item["path"], "layer": "selected"} for item in candidate_paths],
                    "scope_roots": ["repo"],
                    "prompt_mode": "prompt-mode",
                }

            def _codex_initial_route_fields(self, **_kwargs):
                return {"route": "initial"}

            def _runtime_evidence_for_budget(self, evidence, _budget):
                return list(evidence)

            def _codex_initial_prompt_context(self, **_kwargs):
                return "prompt"

            def _codex_prompt_stats(self, _prompt):
                return {"estimated_prompt_tokens": 10, "prompt_chars": 6, "prompt_bytes": 6}

            def _codex_reasoning_effort_for_route(self, _budget):
                return "low"

            def _log_codex_prompt_timing(self, **_kwargs):
                return None

            def _answer_cache_key(self, **_kwargs):
                return "cache"

            def _load_cached_answer(self, _key):
                return None

            def _parse_structured_answer(self, answer):
                return {"direct_answer": answer}

            def _codex_cli_session_id(self, _followup_context):
                return "session"

            def _codex_initial_answer_result(self, **_kwargs):
                return {
                    "answer": "initial answer",
                    "structured_answer": {"direct_answer": "initial answer"},
                    "usage": {},
                    "effective_model": "initial-model",
                    "attempts": 1,
                    "llm_latency_ms": 1,
                    "llm_attempt_log": [],
                    "finish_reason": "stop",
                    "codex_cli_trace": {},
                    "codex_initial_ms": 1,
                    "codex_validation": {},
                    "claim_check": {},
                    "answer_judge": {},
                }

            def _codex_repair_decision(self, **_kwargs):
                return {
                    "severe_repair_reasons": ["deep gap"],
                    "repair_issues": ["deep gap"],
                    "deep_needed": True,
                    "repair_issue_count": 1,
                    "repair_will_run": True,
                    "repair_decision_ms": 1,
                }

            def _codex_repair_remaining_timeout_seconds(self, _started_at, reserve_seconds=0):
                self.remaining_calls += 1
                if self.remaining_calls == 1:
                    return None, ""
                return self.after_deep_result

            def _model_for_role(self, role):
                return f"model:{role}"

            def _repair_candidate_paths_for_runtime_evidence(self, candidate_paths, _runtime_evidence):
                return candidate_paths

            def _codex_repair_brief(self, **_kwargs):
                return "repair prompt"

            def _codex_deep_investigation_context(self, **kwargs):
                return {
                    "candidate_matches": kwargs["candidate_matches"],
                    "candidate_paths": kwargs["candidate_paths"],
                    "candidate_path_layers": kwargs["candidate_path_layers"],
                    "llm_route": kwargs["llm_route"],
                    "evidence_summary": kwargs["evidence_summary"],
                    "quality_gate": kwargs["quality_gate"],
                    "evidence_pack": kwargs["evidence_pack"],
                    "deep_investigation_rounds": 1,
                    "deep_investigation_terms": ["term"],
                    "deep_investigation_added": 1,
                }

            def _codex_repair_answer_context(self, **kwargs):
                return {
                    "answer": "repaired",
                    "structured_answer": {"direct_answer": "repaired"},
                    "codex_validation": kwargs["codex_validation"],
                    "claim_check": kwargs["claim_check"],
                    "answer_judge": kwargs["answer_judge"],
                    "usage": kwargs["usage"],
                    "effective_model": "repair-model",
                    "attempts": kwargs["attempts"] + 1,
                    "llm_latency_ms": kwargs["llm_latency_ms"],
                    "llm_attempt_log": kwargs["llm_attempt_log"],
                    "finish_reason": kwargs["finish_reason"],
                    "codex_cli_trace": kwargs["codex_cli_trace"],
                    "repair_attempted": True,
                    "repair_skipped_reason": "",
                }

            def _codex_final_answer_payload(self, **kwargs):
                return kwargs

        common = {
            "entries": [],
            "key": "k",
            "pm_team": "AF",
            "country": "SG",
            "question": "Q",
            "matches": [{"path": "a.py"}],
            "selected_matches": [{"path": "a.py"}],
            "evidence_summary": {},
            "quality_gate": {},
            "evidence_pack": {"items": []},
            "llm_budget_mode": "auto",
            "routed_budget_mode": "deep",
            "budget": {},
            "llm_route": {},
            "selected_model": "gpt",
            "followup_context": None,
            "requested_answer_mode": "standard",
        }
        skipped = build_codex_llm_answer(FakeCodexAnswerService((1, "after_deep_budget")), **common)
        self.assertFalse(skipped["repair_attempted"])
        self.assertEqual(skipped["repair_skipped_reason"], "after_deep_budget")

        service = FakeCodexAnswerService((7, ""))
        repaired = build_codex_llm_answer(service, **common)
        self.assertTrue(repaired["repair_attempted"])
        self.assertEqual(service._codex_answer_timeout_seconds, 7)
