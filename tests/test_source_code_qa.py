import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from bpmis_jira_tool.source_code_qa import LLMGenerateResult, RepositoryEntry, SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS
from bpmis_jira_tool.web import create_app
from scripts.run_source_code_qa_evals import _build_fixture_repositories, _evaluate_case
from scripts.source_code_qa_feedback_to_eval import build_eval_candidates


class SourceCodeQARouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
                "SOURCE_CODE_QA_GEMINI_API_KEY": "",
                "GEMINI_API_KEY": "",
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def _login(client, email="teammate@npt.sg"):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": email, "name": "Portal User"}
            session["google_credentials"] = {"token": "x", "scopes": []}

    def test_npt_user_sees_source_code_tab_after_bpmis(self):
        with self.app.test_client() as client:
            self._login(client)
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Source Code Q&amp;A", html)
        self.assertLess(html.index("BPMIS Automation Tool"), html.index("Source Code Q&amp;A"))

    def test_whitelisted_gmail_user_also_sees_source_code_tab(self):
        with self.app.test_client() as client:
            self._login(client, "xiaodong.zheng1991@gmail.com")
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Source Code Q&amp;A", response.data)

    def test_non_npt_user_is_blocked_from_page_and_api(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@example.com", "name": "External User"}
                session["google_credentials"] = {"token": "x", "scopes": []}
            page_response = client.get("/source-code-qa", follow_redirects=False)
            api_response = client.post("/api/source-code-qa/query", json={"pm_team": "AF", "country": "All", "question": "test"})

        self.assertIn(page_response.status_code, {302, 403})
        self.assertEqual(api_response.status_code, 403)

    def test_owner_sees_admin_controls_but_teammate_does_not(self):
        with self.app.test_client() as client:
            self._login(client, "xiaodong.zheng@npt.sg")
            owner_response = client.get("/source-code-qa")
            self._login(client, "teammate@npt.sg")
            teammate_response = client.get("/source-code-qa")

        self.assertIn(b"Repository Mapping", owner_response.data)
        self.assertNotIn(b"Repository Mapping", teammate_response.data)
        self.assertIn(b"Ask The Codebase", teammate_response.data)
        self.assertNotIn(b"LLM Budget", teammate_response.data)

    def test_config_save_is_owner_only_and_validates_https(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            forbidden = client.post(
                "/api/source-code-qa/config",
                json={"pm_team": "AF", "country": "All", "repositories": [{"url": "https://git.example.com/team/repo.git"}]},
            )
            self._login(client, "xiaodong.zheng@npt.sg")
            invalid = client.post(
                "/api/source-code-qa/config",
                json={"pm_team": "AF", "country": "All", "repositories": [{"url": "git@git.example.com:team/repo.git"}]},
            )
            valid = client.post(
                "/api/source-code-qa/config",
                json={
                    "pm_team": "AF",
                    "country": "SG",
                    "repositories": [{"display_name": "Repo One", "url": "https://git.example.com/team/repo.git"}],
                },
            )

        self.assertEqual(forbidden.status_code, 403)
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(valid.status_code, 200)
        payload = valid.get_json()
        self.assertEqual(payload["key"], "AF:All")
        self.assertEqual(payload["repositories"][0]["display_name"], "Repo One")

    def test_query_empty_config_is_controlled(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.post(
                "/api/source-code-qa/query",
                json={"pm_team": "CRMS", "country": "SG", "question": "where is approval logic"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "empty_config")
        self.assertEqual(payload["answer_mode"], "retrieval_only")

    def test_query_api_defaults_llm_budget_to_auto(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "answer_mode": "retrieval_only", "matches": []}

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={"pm_team": "AF", "country": "All", "question": "where is createIssue", "answer_mode": "auto"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["llm_budget_mode"], "auto")

    def test_query_api_ignores_client_llm_budget_selection(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "answer_mode": "auto", "matches": []}

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "pm_team": "AF",
                        "country": "All",
                        "question": "where is createIssue",
                        "answer_mode": "auto",
                        "llm_budget_mode": "deep",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["llm_budget_mode"], "auto")

    def test_config_api_reports_llm_not_ready_by_default(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.get("/api/source-code-qa/config")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertFalse(payload["llm_ready"])
        self.assertEqual(payload["llm_provider"], "gemini")
        self.assertEqual(payload["llm_policy"]["provider"]["provider"], "gemini")
        self.assertEqual(payload["llm_policy"]["router"]["version"], 6)
        self.assertEqual(payload["llm_policy"]["versions"]["cache"], 7)
        self.assertEqual(payload["llm_policy"]["versions"]["runtime"], 1)
        self.assertEqual(payload["llm_policy"]["runtime"]["max_retries"], 2)
        self.assertEqual(payload["llm_policy"]["model_policy"]["answer"]["model"], "gemini-2.5-flash")
        self.assertTrue(payload["llm_policy"]["judge"]["enabled"])
        self.assertEqual(payload["llm_policy"]["planner_tools"]["version"], 1)
        self.assertEqual(payload["llm_policy"]["semantic_retrieval"]["model"], "local-token-hybrid-v1")
        self.assertEqual(payload["llm_policy"]["semantic_retrieval"]["embedding_provider"]["provider"], "local_token_hybrid")
        self.assertEqual(payload["options"]["answer_modes"][0]["value"], "auto")
        self.assertNotIn("llm_budget_modes", payload["options"])

    def test_sync_api_runs_as_background_job(self):
        sync_result = {
            "status": "ok",
            "key": "AF:All",
            "results": [{"state": "ok", "display_name": "Repo One"}],
            "repo_status": [{"display_name": "Repo One", "state": "synced", "message": "ready"}],
        }
        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.sync", return_value=sync_result):
            with self.app.test_client() as client:
                self._login(client, "xiaodong.zheng@npt.sg")
                response = client.post("/api/source-code-qa/sync", json={"pm_team": "AF", "country": "All"})
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["status"], "queued")
                job_id = payload["job_id"]
                snapshot = {}
                for _ in range(20):
                    job_response = client.get(f"/api/jobs/{job_id}")
                    snapshot = job_response.get_json()
                    if snapshot.get("state") == "completed":
                        break
                    time.sleep(0.05)

        self.assertEqual(snapshot.get("state"), "completed")
        self.assertEqual(snapshot["results"][0]["repo_status"][0]["state"], "synced")

    def test_feedback_api_saves_user_signal(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.post(
                "/api/source-code-qa/feedback",
                json={
                    "rating": "too_vague",
                    "pm_team": "AF",
                    "country": "All",
                    "question": "where is createIssue",
                    "top_paths": ["controller/IssueController.java"],
                    "trace_id": "trace-123",
                    "replay_context": {
                        "trace_id": "trace-123",
                        "answer_mode": "gemini_flash",
                        "llm_model": "gemini-2.5-flash-lite",
                        "rendered_answer": "createIssue is handled by IssueController.",
                        "answer_contract": {"status": "satisfied"},
                        "evidence_pack": {"items": [{"type": "entry_point", "claim": "IssueController"}]},
                        "matches_snapshot": [
                            {
                                "repo": "Portal Repo",
                                "path": "controller/IssueController.java",
                                "line_start": 10,
                                "line_end": 20,
                                "snippet": "public Issue createIssue() {}",
                            }
                        ],
                    },
                },
            )

        self.assertEqual(response.status_code, 200)
        feedback_path = Path(self.temp_dir.name) / "source_code_qa" / "feedback.jsonl"
        self.assertTrue(feedback_path.exists())
        feedback = json.loads(feedback_path.read_text(encoding="utf-8").strip())
        self.assertEqual(feedback["rating"], "too_vague")
        self.assertEqual(feedback["trace_id"], "trace-123")
        self.assertEqual(feedback["replay_context"]["answer_contract"]["status"], "satisfied")
        self.assertEqual(feedback["replay_context"]["matches_snapshot"][0]["path"], "controller/IssueController.java")

    def test_feedback_records_can_be_promoted_to_eval_candidates(self):
        candidates = build_eval_candidates(
            [
                {
                    "rating": "needs_deeper_trace",
                    "pm_team": "AF",
                    "country": "All",
                    "question_preview": "where does createIssue load data from",
                    "question_sha1": "abc123456789",
                    "trace_id": "trace-abc",
                    "top_paths": ["controller/IssueController.java", "repository/IssueRepository.java"],
                    "comment": "missed repository",
                    "replay_context": {
                        "trace_id": "trace-abc",
                        "answer_mode": "gemini_flash",
                        "llm_model": "gemini-2.5-flash-lite",
                        "answer_contract": {"status": "satisfied"},
                        "evidence_pack": {"items": [{"type": "entry_point", "claim": "IssueController"}]},
                        "matches_snapshot": [
                            {"path": "controller/IssueController.java"},
                            {"path": "repository/IssueRepository.java"},
                        ],
                    },
                },
                {
                    "rating": "useful",
                    "pm_team": "AF",
                    "country": "All",
                    "question_preview": "where is createIssue",
                },
            ]
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["id"], "feedback-needs_deeper_trace-abc1234567")
        self.assertEqual(candidates[0]["expected_paths"], [])
        self.assertEqual(candidates[0]["observed_paths"], ["controller/IssueController.java", "repository/IssueRepository.java"])
        self.assertEqual(candidates[0]["draft_status"], "needs_human_expected_evidence")
        self.assertEqual(candidates[0]["review_context"]["trace_id"], "trace-abc")

    def test_useful_feedback_can_become_positive_smoke_eval(self):
        candidates = build_eval_candidates(
            [
                {
                    "rating": "useful",
                    "pm_team": "AF",
                    "country": "All",
                    "question_preview": "where is createIssue",
                    "question_sha1": "def123456789",
                    "replay_context": {
                        "matches_snapshot": [{"path": "controller/IssueController.java"}],
                    },
                }
            ],
            include_useful=True,
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["expected_paths"], ["controller/IssueController.java"])
        self.assertEqual(candidates[0]["draft_status"], "ready_positive_smoke")


class SourceCodeQAServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _build_index_for_entry(self, key, entry):
        repo_entry = type("Entry", (), entry)()
        repo_path = self.service._repo_path(key, repo_entry)
        self.service._build_repo_index(key, repo_entry, repo_path)
        return repo_path

    def _build_all_indexes(self, service=None):
        target_service = service or self.service
        for key, entries in (target_service.load_config().get("mappings") or {}).items():
            for entry in entries:
                repo_entry = type("Entry", (), entry)()
                repo_path = target_service._repo_path(key, repo_entry)
                if (repo_path / ".git").exists():
                    target_service._build_repo_index(key, repo_entry, repo_path)

    def test_sync_subprocess_timeout_is_controlled(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Repo One", "url": "https://git.example.com/team/repo.git"}],
        )
        import subprocess

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(["git"], timeout=5)):
            payload = self.service.sync(pm_team="AF", country="All")

        self.assertEqual(payload["status"], "partial")
        self.assertIn("timed out", payload["results"][0]["message"])
        self.assertEqual(payload["job"]["status"], "partial")

    def test_search_repo_request_cache_reuses_identical_search(self):
        entry = RepositoryEntry(display_name="Portal Repo", url="https://git.example.com/team/portal.git")
        repo_path = Path(self.temp_dir.name) / "portal"
        (repo_path / ".git").mkdir(parents=True)
        request_cache = self.service._new_retrieval_request_cache()
        match = {
            "repo": "Portal Repo",
            "path": "src/App.java",
            "line_start": 1,
            "line_end": 2,
            "score": 99,
            "snippet": "class App {}",
            "reason": "test match",
            "trace_stage": "direct",
            "retrieval": "persistent_index",
        }

        with patch.object(self.service, "_require_ready_repo_index", return_value={"state": "ready"}) as mocked_ensure, patch.object(
            self.service,
            "_search_repo_index",
            return_value=[dict(match)],
        ) as mocked_index:
            first = self.service._search_repo(
                entry,
                repo_path,
                ["app"],
                question="where is app",
                request_cache=request_cache,
            )
            first[0]["reason"] = "mutated by caller"
            second = self.service._search_repo(
                entry,
                repo_path,
                ["app"],
                question="where is app",
                request_cache=request_cache,
            )

        self.assertEqual(mocked_ensure.call_count, 1)
        self.assertEqual(mocked_index.call_count, 1)
        self.assertEqual(second[0]["reason"], "test match")
        self.assertEqual(request_cache["stats"]["search_hits"], 1)
        self.assertEqual(request_cache["stats"]["search_misses"], 1)
        self.assertEqual(request_cache["stats"]["index_ensure_hits"], 1)
        self.assertEqual(request_cache["stats"]["index_ensure_misses"], 1)

    def test_rank_matches_reuses_rerank_scores_within_request_cache(self):
        request_cache = self.service._new_retrieval_request_cache()
        matches = [
            {
                "repo": "Portal Repo",
                "path": "src/main/java/com/example/IssueService.java",
                "line_start": 10,
                "line_end": 12,
                "score": 100,
                "snippet": "class IssueService { void createIssue() {} }",
                "reason": "test match",
                "trace_stage": "direct",
                "retrieval": "persistent_index",
            }
        ]

        with patch.object(self.service, "_rerank_score", wraps=self.service._rerank_score) as mocked_rerank:
            first = self.service._rank_matches("where is IssueService createIssue", matches, request_cache=request_cache)
            first[0]["rerank_score"] = -1
            second = self.service._rank_matches("where is IssueService createIssue", matches, request_cache=request_cache)

        self.assertEqual(mocked_rerank.call_count, 1)
        self.assertGreater(second[0]["rerank_score"], 0)
        self.assertNotEqual(second[0]["rerank_score"], -1)
        self.assertEqual(request_cache["stats"]["rerank_hits"], 1)
        self.assertEqual(request_cache["stats"]["rerank_misses"], 1)
        self.assertEqual(request_cache["stats"]["question_feature_hits"], 1)
        self.assertEqual(request_cache["stats"]["question_feature_misses"], 1)

    def test_question_tokens_cache_returns_caller_safe_lists(self):
        first = self.service._question_tokens("where is IssueService createIssue")
        first.append("mutated")
        second = self.service._question_tokens("where is IssueService createIssue")

        self.assertIn("issueservice", second)
        self.assertNotIn("mutated", second)

    def test_index_rows_are_reused_within_request_cache(self):
        index_path = Path(self.temp_dir.name) / "index.sqlite3"
        with sqlite3.connect(index_path) as connection:
            connection.execute("create table files (path text, lower_path text, symbols text)")
            connection.execute(
                "create table lines (file_path text, line_no integer, line_text text, lower_text text, symbols text, is_declaration integer, has_pathish integer)"
            )
            connection.execute(
                "create table semantic_chunks (file_path text, start_line integer, end_line integer, chunk_text text, lower_text text, tokens text, symbols text, embedding text)"
            )
            connection.execute("insert into files values ('src/App.java', 'src/app.java', '[]')")
            connection.execute("insert into lines values ('src/App.java', 1, 'class App {}', 'class app {}', '[]', 1, 0)")
            connection.execute("insert into semantic_chunks values ('src/App.java', 1, 1, 'class App {}', 'class app {}', '[]', '[]', '[]')")
            connection.commit()
        request_cache = self.service._new_retrieval_request_cache()
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            first = self.service._cached_index_rows(connection, index_path, request_cache=request_cache)
            second = self.service._cached_index_rows(connection, index_path, request_cache=request_cache)

        self.assertIs(first, second)
        self.assertEqual(first["lines_by_path"]["src/App.java"], ["class App {}"])
        self.assertEqual(request_cache["stats"]["index_rows_hits"], 1)
        self.assertEqual(request_cache["stats"]["index_rows_misses"], 1)

    def test_match_from_index_location_uses_cached_lines_without_changing_snippet(self):
        entry = RepositoryEntry(display_name="Portal Repo", url="https://git.example.com/team/portal.git")
        index_path = Path(self.temp_dir.name) / "index.sqlite3"
        with sqlite3.connect(index_path) as connection:
            connection.execute("create table files (path text, lower_path text, symbols text)")
            connection.execute(
                "create table lines (file_path text, line_no integer, line_text text, lower_text text, symbols text, is_declaration integer, has_pathish integer)"
            )
            connection.execute(
                "create table semantic_chunks (file_path text, start_line integer, end_line integer, chunk_text text, lower_text text, tokens text, symbols text, embedding text)"
            )
            connection.execute("insert into files values ('src/App.java', 'src/app.java', '[]')")
            connection.execute("insert into lines values ('src/App.java', 1, 'class App {', 'class app {', '[]', 1, 0)")
            connection.execute("insert into lines values ('src/App.java', 2, '  void run() {}', '  void run() {}', '[]', 1, 0)")
            connection.execute("insert into lines values ('src/App.java', 3, '}', '}', '[]', 0, 0)")
            connection.execute("insert into semantic_chunks values ('src/App.java', 1, 3, 'class App {}', 'class app {}', '[]', '[]', '[]')")
            connection.commit()
        request_cache = self.service._new_retrieval_request_cache()
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            uncached = self.service._match_from_index_location(
                entry,
                connection,
                "src/App.java",
                2,
                score=100,
                reason="uncached",
                question="where is run",
                trace_stage="test",
                retrieval="test",
            )
            cached = self.service._match_from_index_location(
                entry,
                connection,
                "src/App.java",
                2,
                score=100,
                reason="cached",
                question="where is run",
                trace_stage="test",
                retrieval="test",
                index_path=index_path,
                request_cache=request_cache,
            )

        self.assertIsNotNone(uncached)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["snippet"], uncached["snippet"])
        self.assertEqual(request_cache["stats"]["file_lines_misses"], 1)

    def test_trace_paths_are_reused_within_request_cache(self):
        entry = RepositoryEntry(display_name="Portal Repo", url="https://git.example.com/team/portal.git")
        key = "AF::All"
        repo_path = self.service._repo_path(key, entry)
        (repo_path / ".git").mkdir(parents=True)
        index_path = self.service._index_path(repo_path)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(index_path) as connection:
            connection.execute(
                """
                create table flow_edges (
                    edge_kind text,
                    from_file text,
                    from_line integer,
                    from_name text,
                    to_file text,
                    to_line integer,
                    to_name text,
                    evidence text
                )
                """
            )
            connection.execute(
                "insert into flow_edges values ('service', 'src/Controller.java', 10, 'Controller.handle', 'src/Service.java', 20, 'Service.run', 'controller calls service')"
            )
            connection.commit()
        matches = [
            {
                "repo": "Portal Repo",
                "path": "src/Controller.java",
                "line_start": 10,
                "line_end": 12,
                "score": 100,
                "snippet": "class Controller {}",
                "reason": "seed",
                "trace_stage": "direct",
                "retrieval": "persistent_index",
            }
        ]
        request_cache = self.service._new_retrieval_request_cache()
        with patch.object(self.service, "_require_ready_repo_index", return_value={"state": "ready"}) as mocked_ensure:
            first = self.service._build_trace_paths(
                entries=[entry],
                key=key,
                matches=matches,
                question="trace controller",
                request_cache=request_cache,
            )
            first[0]["confidence"] = -1
            second = self.service._build_trace_paths(
                entries=[entry],
                key=key,
                matches=matches,
                question="trace controller",
                request_cache=request_cache,
            )

        self.assertEqual(mocked_ensure.call_count, 1)
        self.assertGreater(second[0]["confidence"], 0)
        self.assertNotEqual(second[0]["confidence"], -1)
        self.assertEqual(request_cache["stats"]["trace_paths_hits"], 1)
        self.assertEqual(request_cache["stats"]["trace_paths_misses"], 1)

    def test_index_lock_reports_concurrent_indexing(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class BPMISClient:\n    pass\n", encoding="utf-8")
        lock_path = self.service._index_lock_path(repo_path)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text("busy", encoding="utf-8")

        with self.assertRaisesRegex(Exception, "already being indexed"):
            self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        lock_path.unlink(missing_ok=True)

    def test_retrieval_query_returns_path_line_and_snippet(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="batchCreateJiraIssue BPMIS API")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["answer_mode"], "retrieval_only")
        self.assertEqual(payload["matches"][0]["path"], "bpmis/jira_client.py")
        self.assertIn("batchCreateJiraIssue", payload["matches"][0]["snippet"])

    def test_chinese_business_intent_detection(self):
        intent = self.service._question_intent("IssueService 改了会影响哪些下游，测试有没有覆盖，事务缓存边界是什么")

        self.assertTrue(intent["impact_analysis"])
        self.assertTrue(intent["test_coverage"])
        self.assertTrue(intent["operational_boundary"])
        self.assertTrue(any("影响" in token for token in self.service._question_tokens("IssueService 改了会影响哪些下游")))

    def test_chinese_data_source_query_uses_source_trace(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="CRMS",
            country="ID",
            question="Term Loan Pre Check 1 的数据源是哪张表",
        )

        self.assertEqual(payload["status"], "ok")
        self.assertIn("repository/CustomerRepository.java", {match["path"] for match in payload["matches"]})
        self.assertEqual(payload["answer_quality"]["policies"][0]["name"], "data_source")
        self.assertEqual(payload["answer_quality"]["policies"][0]["status"], "satisfied")
        self.assertIn("cr_customer_profile", json.dumps(payload["evidence_pack"], ensure_ascii=False))

    def test_followup_context_uses_evidence_boundaries(self):
        augmented, followup = self.service._apply_conversation_context(
            "继续看下游影响",
            {
                "question": "what is impacted if IssueService createIssue changes",
                "matches": [],
                "trace_paths": [],
                "structured_answer": {},
                "answer_contract": {
                    "confirmed_sources": ["Portal Repo:repository/IssueRepository.java:3-5: issue_table [S1]"],
                    "missing_links": ["consumer caller evidence"],
                },
                "evidence_pack": {
                    "confirmed_facts": ["IssueRepository writes issue_table"],
                    "impact_surfaces": ["downstream callee: IssueRepository.findIssue"],
                },
            },
        )

        self.assertTrue(followup["used"])
        self.assertIn("issuerepository", augmented)
        self.assertIn("issue_table", augmented)

    def test_query_uses_ready_persistent_index_and_citations(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where is batchCreateJiraIssue implemented")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(self.service._index_path(repo_path).exists())
        self.assertIn("persistent_index", {match.get("retrieval") for match in payload["matches"]})
        self.assertEqual(payload["citations"][0]["id"], "S1")
        self.assertEqual(payload["citations"][0]["path"], "bpmis/jira_client.py")
        self.assertEqual(payload["index_freshness"]["status"], "fresh")
        self.assertIn("git_revisions", payload["index_freshness"])
        self.assertEqual(self.service.repo_status("AF:All")[0]["index"]["state"], "ready")

    def test_query_does_not_build_missing_index_synchronously(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class BPMISClient: pass\n", encoding="utf-8")

        payload = self.service.query(pm_team="AF", country="All", question="where is BPMISClient")

        self.assertEqual(payload["status"], "no_match")
        self.assertFalse(self.service._index_path(repo_path).exists())
        self.assertEqual(payload["index_freshness"]["status"], "stale_or_missing")

    def test_structure_index_extracts_definitions_references_and_telemetry(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "@RequestMapping(\"/api/v1/issues\")\n"
            "public class IssueController {\n"
            "    private IssueRepository issueRepository;\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from bpmis_issue\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where is IssueController createIssue API")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(any("structure matched" in match["reason"] for match in payload["matches"]))
        index_path = self.service._index_path(repo_path)
        with sqlite3.connect(index_path) as connection:
            metadata = dict(connection.execute("select key, value from metadata").fetchall())
            definition_count = connection.execute("select count(*) from definitions").fetchone()[0]
            reference_count = connection.execute("select count(*) from references_index").fetchone()[0]
            entity_count = connection.execute("select count(*) from code_entities").fetchone()[0]
            entity_edge_count = connection.execute("select count(*) from entity_edges").fetchone()[0]
            edge_count = connection.execute("select count(*) from graph_edges").fetchone()[0]
            flow_edge_count = connection.execute("select count(*) from flow_edges").fetchone()[0]
            semantic_chunk_count = connection.execute("select count(*) from semantic_chunks").fetchone()[0]
            fts_count = connection.execute("select count(*) from lines_fts").fetchone()[0]
        self.assertGreaterEqual(definition_count, 3)
        self.assertGreaterEqual(reference_count, 3)
        self.assertGreaterEqual(entity_count, 3)
        self.assertGreaterEqual(entity_edge_count, 1)
        self.assertGreaterEqual(edge_count, 1)
        self.assertGreaterEqual(flow_edge_count, 1)
        self.assertGreaterEqual(semantic_chunk_count, 2)
        self.assertGreaterEqual(fts_count, 1)
        self.assertEqual(metadata["parser_backend"], "tree_sitter+regex")
        self.assertIn("java", metadata["parser_languages"])
        self.assertGreaterEqual(int(metadata["tree_sitter_files"]), 2)
        self.assertIn("semantic_chunk", {match.get("retrieval") for match in payload["matches"]})
        telemetry = self.service.telemetry_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertTrue(telemetry)
        self.assertIn("IssueController", telemetry[-1])

    def test_planner_tool_loop_adds_code_graph_matches(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="trace issue create API data source")

        retrievals = {match.get("retrieval") for match in payload["matches"]}
        paths = {match["path"] for match in payload["matches"]}
        trace_stages = {match.get("trace_stage") for match in payload["matches"]}
        self.assertTrue({"planner_definition", "planner_reference", "code_graph", "flow_graph", "entity_graph"} & retrievals)
        self.assertIn("repository/IssueRepository.java", paths)
        self.assertTrue(any(str(stage).startswith("tool_loop_") for stage in trace_stages) or "semantic_chunk" in retrievals)
        self.assertTrue(payload["tool_trace"])
        self.assertTrue(any(step.get("phase") in {"tool_loop", "agent_plan"} for step in payload["tool_trace"]))
        telemetry = json.loads(self.service.telemetry_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertGreaterEqual(telemetry["tool_trace_summary"]["steps"], 1)

    def test_python_ast_index_extracts_entities_and_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "services" / "issue_service.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "from bpmis.client import BPMISClient\n"
            "class IssueService:\n"
            "    def create_issue(self):\n"
            "        client = BPMISClient()\n"
            "        return client.batchCreateJiraIssue()\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where does create_issue call BPMIS")
        index_path = self.service._index_path(repo_path)

        self.assertEqual(payload["status"], "ok")
        with sqlite3.connect(index_path) as connection:
            entity_names = {row[0] for row in connection.execute("select name from code_entities")}
            edge_targets = {row[0] for row in connection.execute("select to_name from entity_edges")}
        self.assertIn("IssueService", entity_names)
        self.assertIn("create_issue", entity_names)
        self.assertTrue(any("batchCreateJiraIssue" in target for target in edge_targets))

    def test_flow_graph_can_trace_controller_service_repository_table_chain(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "@PostMapping(\"/issue/create\")\n"
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which API flow reaches issue data source table")

        self.assertEqual(payload["status"], "ok")
        self.assertIn("repository/IssueRepository.java", {match["path"] for match in payload["matches"]})
        self.assertTrue({"flow_graph", "entity_graph"} & {match.get("retrieval") for match in payload["matches"]})
        self.assertTrue(any("issue_table" in match["snippet"] for match in payload["matches"]))
        self.assertTrue(payload["trace_paths"])
        self.assertIn("issue_table", str(payload["trace_paths"]))

    def test_framework_adapters_extract_spring_mybatis_and_feign_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    @PostMapping(\"/issue/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueClient.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        client_file = repo_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"issue-service\", url = \"https://issue.example.com\")\n"
            "public interface IssueClient {\n"
            "    @PostMapping(\"/remote/issue\")\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        mapper_file = repo_path / "mapper" / "IssueMapper.xml"
        mapper_file.parent.mkdir(parents=True)
        mapper_file.write_text(
            "<mapper namespace=\"com.example.IssueMapper\">\n"
            "  <select id=\"findIssue\">\n"
            "    select * from issue_table\n"
            "  </select>\n"
            "</mapper>\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="trace create issue API to downstream service and table")
        index_path = self.service._index_path(repo_path)

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["trace_paths"])
        with sqlite3.connect(index_path) as connection:
            edge_rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertIn(("downstream_api", "issue-service"), edge_rows)
        self.assertIn(("http_endpoint", "https://issue.example.com"), edge_rows)
        self.assertTrue(any(kind == "mapper_statement" and "findIssue" in target for kind, target in edge_rows))
        self.assertTrue(any(kind == "sql_table" and target == "issue_table" for kind, target in edge_rows))

    def test_cross_repo_graph_links_client_to_service_repo(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"issue-service\")\n"
            "public interface IssueClient { Issue createIssue(); }\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@PostMapping(\"/issue/create\")\n"
            "public class IssueController { public Issue createIssue() { return new Issue(); } }\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service does issue client call")

        self.assertEqual(payload["repo_graph"]["version"], 2)
        edges = payload["repo_graph"]["edges"]
        graph_edge = next(
            edge for edge in edges if edge["from_repo"] == "Portal Repo" and edge["to_repo"] == "Issue Service"
        )
        self.assertIn("confidence", graph_edge)
        self.assertIn("match_reason", graph_edge)
        self.assertGreaterEqual(graph_edge["confidence"], 0.7)

    def test_tree_sitter_java_extracts_injection_routes_and_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "@RestController\n"
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    @PostMapping(\"/issue/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "tree_sitter+regex")
        self.assertIn("java", index_info["parser_languages"])
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertIn(("injects", "IssueService"), rows)
        self.assertIn(("route", "/issue/create"), rows)
        self.assertTrue(any(kind == "call" and "createIssue" in target for kind, target in rows))

    def test_spring_route_prefix_and_constructor_injection_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/api/issues\")\n"
            "public class IssueController {\n"
            "    private final IssueService issueService;\n"
            "    public IssueController(IssueService issueService) {\n"
            "        this.issueService = issueService;\n"
            "    }\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertIn(("route", "/api/issues/create"), rows)
        self.assertIn(("injects", "IssueService"), rows)

    def test_java_member_calls_resolve_to_qualified_repository_methods(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            edge_rows = list(
                connection.execute(
                    """
                    select edge_kind, to_name, to_file
                    from entity_edges
                    where from_file = 'service/IssueService.java'
                    """
                )
            )
            flow_rows = list(
                connection.execute(
                    """
                    select edge_kind, to_name, to_file
                    from flow_edges
                    where from_file = 'service/IssueService.java'
                    """
                )
            )
        self.assertIn(("call", "IssueRepository.findIssue", "repository/IssueRepository.java"), edge_rows)
        self.assertTrue(
            any(kind == "repository" and target == "IssueRepository.findIssue" for kind, target, _to_file in flow_rows)
        )

    def test_java_interface_and_mybatis_namespace_connect_to_implementations(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text(
            "public interface IssueService {\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        mapper_interface = repo_path / "mapper" / "IssueMapper.java"
        mapper_interface.parent.mkdir(parents=True)
        mapper_interface.write_text(
            "public interface IssueMapper {\n"
            "    Issue findIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        mapper_xml = repo_path / "mapper" / "IssueMapper.xml"
        mapper_xml.write_text(
            "<mapper namespace=\"com.example.IssueMapper\">\n"
            "  <select id=\"findIssue\">select * from issue_table</select>\n"
            "</mapper>\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            edge_rows = list(connection.execute("select edge_kind, to_name, to_file from entity_edges"))
            flow_rows = list(connection.execute("select edge_kind, to_name, to_file from flow_edges"))
        self.assertIn(("implements", "IssueService", "service/IssueService.java"), edge_rows)
        self.assertIn(("mapper_interface", "IssueMapper", "mapper/IssueMapper.java"), edge_rows)
        self.assertTrue(any(kind == "mapper" and target == "IssueMapper" for kind, target, _to_file in flow_rows))

    def test_interface_method_calls_resolve_to_implementation_methods(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text(
            "public interface IssueService {\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"primaryIssueService\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(connection.execute("select edge_kind, to_name, to_file from entity_edges"))
            flow_edges = list(connection.execute("select edge_kind, to_name, to_file from flow_edges"))

        self.assertIn(("bean_name", "primaryIssueService", ""), entity_edges)
        self.assertIn(("implementation_call", "IssueServiceImpl.createIssue", "service/IssueServiceImpl.java"), entity_edges)
        self.assertTrue(
            any(kind == "service" and target == "IssueServiceImpl.createIssue" for kind, target, _to_file in flow_edges)
        )

    def test_qualifier_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    @Qualifier(\"fastIssueService\")\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text(
            "public interface IssueService {\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, to_file, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn(
            ("implementation_call", "FastIssueService.createIssue", "service/FastIssueService.java"),
            [(kind, target, to_file) for kind, target, to_file, _evidence in implementation_edges],
        )
        self.assertFalse(any(target == "SlowIssueService.createIssue" for _kind, target, _to_file, _evidence in implementation_edges))
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _kind, _target, _to_file, evidence in implementation_edges))

    def test_variable_qualifier_filters_each_interface_call_independently(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    @Qualifier(\"fastIssueService\")\n"
            "    private IssueService fastIssueService;\n"
            "    @Qualifier(\"slowIssueService\")\n"
            "    private IssueService slowIssueService;\n"
            "    public Issue createFastIssue() { return fastIssueService.createIssue(); }\n"
            "    public Issue createSlowIssue() { return slowIssueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select from_line, to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    order by from_line, to_name
                    """
                )
            )

        fast_edges = [(target, evidence) for line_no, target, evidence in implementation_edges if line_no == 6]
        slow_edges = [(target, evidence) for line_no, target, evidence in implementation_edges if line_no == 7]
        self.assertEqual(["FastIssueService.createIssue"], [target for target, _evidence in fast_edges])
        self.assertEqual(["SlowIssueService.createIssue"], [target for target, _evidence in slow_edges])
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _target, evidence in fast_edges))
        self.assertTrue(any("qualifier=slowIssueService" in evidence for _target, evidence in slow_edges))

    def test_constructor_parameter_qualifiers_resolve_assigned_fields_independently(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService fastIssueService;\n"
            "    private IssueService slowIssueService;\n"
            "    public IssueController(@Qualifier(\"fastIssueService\") IssueService fastDelegate, @Qualifier(\"slowIssueService\") IssueService slowDelegate) {\n"
            "        this.fastIssueService = fastDelegate;\n"
            "        this.slowIssueService = slowDelegate;\n"
            "    }\n"
            "    public Issue createFastIssue() { return fastIssueService.createIssue(); }\n"
            "    public Issue createSlowIssue() { return slowIssueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    order by from_line, to_name
                    """
                )
            )

        fast_edges = [(target, evidence) for target, evidence in implementation_edges if "fastIssueService.createIssue" in evidence]
        slow_edges = [(target, evidence) for target, evidence in implementation_edges if "slowIssueService.createIssue" in evidence]
        self.assertEqual(["FastIssueService.createIssue"], [target for target, _evidence in fast_edges])
        self.assertEqual(["SlowIssueService.createIssue"], [target for target, _evidence in slow_edges])
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _target, evidence in fast_edges))
        self.assertTrue(any("qualifier=slowIssueService" in evidence for _target, evidence in slow_edges))

    def test_generic_collection_injection_resolves_lambda_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.List;\n"
            "public class IssueController {\n"
            "    private List<IssueService> issueServices;\n"
            "    public void createAllIssues() {\n"
            "        issueServices.forEach(issueService -> issueService.createIssue());\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        targets = {target for target, _evidence in implementation_edges}
        self.assertIn("FastIssueService.createIssue", targets)
        self.assertIn("SlowIssueService.createIssue", targets)
        self.assertTrue(any("issueService.createIssue" in evidence for _target, evidence in implementation_edges))

    def test_qualified_generic_collection_injection_filters_lambda_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.List;\n"
            "public class IssueController {\n"
            "    @Qualifier(\"fastIssueService\")\n"
            "    private List<IssueService> issueServices;\n"
            "    public void createFastIssues() {\n"
            "        issueServices.forEach(issueService -> issueService.createIssue());\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _target, evidence in implementation_edges))

    def test_generic_map_values_lambda_resolves_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.Map;\n"
            "public class IssueController {\n"
            "    private Map<String, IssueService> issueServiceMap;\n"
            "    public void createAllIssues() {\n"
            "        issueServiceMap.values().forEach(issueService -> issueService.createIssue());\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        targets = {target for target, _evidence in implementation_edges}
        self.assertIn("FastIssueService.createIssue", targets)
        self.assertIn("SlowIssueService.createIssue", targets)
        self.assertTrue(any("issueService.createIssue" in evidence for _target, evidence in implementation_edges))

    def test_generic_stream_map_lambda_resolves_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.List;\n"
            "public class IssueController {\n"
            "    private List<IssueService> issueServices;\n"
            "    public void createAllIssues() {\n"
            "        issueServices.stream().map(issueService -> issueService.createIssue()).toList();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("IssueServiceImpl.createIssue", [target for target, _evidence in implementation_edges])
        self.assertTrue(any("issueService.createIssue" in evidence for _target, evidence in implementation_edges))

    def test_object_provider_chain_resolves_interface_call(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import org.springframework.beans.factory.ObjectProvider;\n"
            "public class IssueController {\n"
            "    private ObjectProvider<IssueService> issueServiceProvider;\n"
            "    public Issue createIssue() {\n"
            "        return issueServiceProvider.getIfAvailable().createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("IssueServiceImpl.createIssue", [target for target, _evidence in implementation_edges])
        self.assertTrue(any("issueServiceProvider.getIfAvailable().createIssue" in evidence for _target, evidence in implementation_edges))

    def test_spring_aop_edges_are_indexed_as_framework_flow(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        aspect_file = repo_path / "aspect" / "IssueAuditAspect.java"
        aspect_file.parent.mkdir(parents=True)
        aspect_file.write_text(
            "@Aspect\n"
            "@Component\n"
            "public class IssueAuditAspect {\n"
            "    @Pointcut(\"execution(* *..IssueService.createIssue(..))\")\n"
            "    public void issueCreatePointcut() {}\n"
            "    @Around(\"issueCreatePointcut()\")\n"
            "    public Object auditIssueCreate(ProceedingJoinPoint joinPoint) { return joinPoint.proceed(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from entity_edges
                    where from_file = 'aspect/IssueAuditAspect.java'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from flow_edges
                    where from_file = 'aspect/IssueAuditAspect.java'
                    """
                )
            )

        self.assertIn(("framework_binding", "Aspect"), entity_edges)
        self.assertTrue(any(kind == "aop_pointcut" and "IssueService.createIssue" in target for kind, target in entity_edges))
        self.assertTrue(any(kind == "aop_advice" and "issueCreatePointcut" in target for kind, target in entity_edges))
        self.assertTrue(any(kind == "aop_applies_to" and target == "IssueService.createIssue" for kind, target in entity_edges))
        self.assertTrue(any(kind == "aop_applies_to" and target == "IssueServiceImpl.createIssue" for kind, target in entity_edges))
        self.assertTrue(any(kind == "framework" and "issueCreatePointcut" in target for kind, target in flow_edges))
        self.assertTrue(any(kind == "service" and target == "IssueServiceImpl.createIssue" for kind, target in flow_edges))

    def test_scheduled_job_edges_are_indexed_as_framework_flow(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        job_file = repo_path / "job" / "IssueRetryJob.java"
        job_file.parent.mkdir(parents=True)
        job_file.write_text(
            "@Component\n"
            "public class IssueRetryJob {\n"
            "    private IssueService issueService;\n"
            "    @Scheduled(cron = \"0 0/5 * * * ?\")\n"
            "    public void retryIssues() { issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from entity_edges
                    where from_file = 'job/IssueRetryJob.java'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from flow_edges
                    where from_file = 'job/IssueRetryJob.java'
                    """
                )
            )

        self.assertTrue(any(kind == "scheduled_job" and "cron=0 0/5 * * * ?" in target for kind, target in entity_edges))
        self.assertTrue(any(kind == "implementation_call" and target == "IssueServiceImpl.createIssue" for kind, target in entity_edges))
        self.assertTrue(any(kind == "framework" and "cron=0 0/5 * * * ?" in target for kind, target in flow_edges))

    def test_handler_interceptor_edges_are_indexed_as_framework_flow(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        interceptor_file = repo_path / "web" / "IssueAuthInterceptor.java"
        interceptor_file.parent.mkdir(parents=True)
        interceptor_file.write_text(
            "public class IssueAuthInterceptor implements HandlerInterceptor {\n"
            "    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {\n"
            "        return true;\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from entity_edges
                    where from_file = 'web/IssueAuthInterceptor.java'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from flow_edges
                    where from_file = 'web/IssueAuthInterceptor.java'
                    """
                )
            )

        self.assertTrue(any(kind == "web_interceptor" and target in {"IssueAuthInterceptor", "HandlerInterceptor"} for kind, target in entity_edges))
        self.assertTrue(any(kind == "framework" and target in {"IssueAuthInterceptor", "HandlerInterceptor"} for kind, target in flow_edges))

    def test_primary_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        primary_impl = repo_path / "service" / "PrimaryIssueService.java"
        primary_impl.write_text(
            "@Primary\n"
            "@Service(\"primaryIssueService\")\n"
            "public class PrimaryIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        fallback_impl = repo_path / "service" / "FallbackIssueService.java"
        fallback_impl.write_text(
            "@Service(\"fallbackIssueService\")\n"
            "public class FallbackIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("PrimaryIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "FallbackIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("primary=true" in evidence for _target, evidence in implementation_edges))

    def test_active_profile_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.properties").write_text("spring.profiles.active=prod\n", encoding="utf-8")
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        prod_impl = repo_path / "service" / "ProdIssueService.java"
        prod_impl.write_text(
            "@Profile(\"prod\")\n"
            "@Service(\"prodIssueService\")\n"
            "public class ProdIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        dev_impl = repo_path / "service" / "DevIssueService.java"
        dev_impl.write_text(
            "@Profile(\"dev\")\n"
            "@Service(\"devIssueService\")\n"
            "public class DevIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("ProdIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "DevIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("profile=prod" in evidence for _target, evidence in implementation_edges))

    def test_conditional_on_property_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.properties").write_text("issue.fast.enabled=true\n", encoding="utf-8")
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"true\")\n"
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"false\")\n"
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.fast.enabled=true" in evidence for _target, evidence in implementation_edges))

    def test_profile_specific_config_overrides_conditional_on_property_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.properties").write_text(
            "spring.profiles.active=prod\n"
            "issue.fast.enabled=false\n",
            encoding="utf-8",
        )
        (repo_path / "application-prod.properties").write_text("issue.fast.enabled=true\n", encoding="utf-8")
        (repo_path / "application-dev.properties").write_text("issue.fast.enabled=false\n", encoding="utf-8")
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"true\")\n"
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"false\")\n"
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.fast.enabled=true" in evidence for _target, evidence in implementation_edges))

    def test_yaml_on_profile_document_overrides_conditional_on_property_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.yml").write_text(
            "spring:\n"
            "  profiles:\n"
            "    active: prod\n"
            "issue:\n"
            "  fast:\n"
            "    enabled: false\n"
            "---\n"
            "spring:\n"
            "  config:\n"
            "    activate:\n"
            "      on-profile: prod\n"
            "issue:\n"
            "  fast:\n"
            "    enabled: true\n"
            "---\n"
            "spring:\n"
            "  config:\n"
            "    activate:\n"
            "      on-profile: dev\n"
            "issue:\n"
            "  fast:\n"
            "    enabled: false\n",
            encoding="utf-8",
        )
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"true\")\n"
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"false\")\n"
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.fast.enabled=true" in evidence for _target, evidence in implementation_edges))

    def test_conditional_on_property_match_if_missing_selects_default_implementation(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        default_impl = repo_path / "service" / "DefaultIssueService.java"
        default_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.optional.enabled\", matchIfMissing = true)\n"
            "@Service(\"defaultIssueService\")\n"
            "public class DefaultIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        disabled_impl = repo_path / "service" / "DisabledIssueService.java"
        disabled_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.optional.enabled\", havingValue = \"false\")\n"
            "@Service(\"disabledIssueService\")\n"
            "public class DisabledIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("DefaultIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "DisabledIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.optional.enabled=<missing:true>" in evidence for _target, evidence in implementation_edges))

    def test_java_package_imports_create_fully_qualified_symbol_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        service_file = repo_path / "src" / "main" / "java" / "com" / "example" / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "package com.example.service;\n"
            "import com.example.repository.IssueRepository;\n"
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "src" / "main" / "java" / "com" / "example" / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "package com.example.repository;\n"
            "public class IssueRepository {\n"
            "    public Issue findIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            definitions = list(connection.execute("select name, kind from definitions"))
            edges = list(connection.execute("select edge_kind, to_name, to_file from entity_edges"))
        self.assertIn(("com.example.service.IssueService", "class"), definitions)
        self.assertIn(("com.example.repository.IssueRepository.findIssue", "call", str(repository_file.relative_to(repo_path))), [(to_name, kind, to_file) for kind, to_name, to_file in edges])

    def test_tree_sitter_python_extracts_import_route_and_call(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "api" / "routes.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "from flask import Blueprint\n"
            "from service.issue_service import create_issue\n\n"
            "bp = Blueprint('issue', __name__)\n\n"
            "@bp.route('/api/issues/create', methods=['POST'])\n"
            "def create_issue_route():\n"
            "    return create_issue()\n",
            encoding="utf-8",
        )

        index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "tree_sitter+regex")
        self.assertIn("python", index_info["parser_languages"])
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertTrue(any(kind == "import" and "service.issue_service" in target for kind, target in rows))
        self.assertIn(("route", "/api/issues/create"), rows)
        self.assertTrue(any(kind == "call" and target == "create_issue" for kind, target in rows))

    def test_tree_sitter_typescript_extracts_import_export_and_api_call(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "web" / "issue_client.ts"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "import axios from 'axios';\n"
            "export class IssueClient {\n"
            "  createIssue(payload: unknown) {\n"
            "    return axios.post('/api/issues/create', payload);\n"
            "  }\n"
            "}\n",
            encoding="utf-8",
        )

        index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "tree_sitter+regex")
        self.assertIn("typescript", index_info["parser_languages"])
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            definitions = list(connection.execute("select name, kind from definitions"))
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertTrue(any(name == "IssueClient" and "class" in kind for name, kind in definitions))
        self.assertTrue(any(kind == "import" and "axios" in target for kind, target in rows))
        self.assertIn(("http_endpoint", "/api/issues/create"), rows)

    def test_tree_sitter_unavailable_falls_back_to_regex_index(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "public class IssueController { public Issue createIssue() { return new Issue(); } }\n",
            encoding="utf-8",
        )

        with patch.object(self.service, "_tree_sitter_parser_for_language", return_value=None):
            index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "regex")
        self.assertEqual(index_info["tree_sitter_files"], 0)
        self.assertGreaterEqual(index_info["definitions"], 1)

    def test_cross_repo_graph_matches_http_path_to_controller_route(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "web" / "issue_client.ts"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "export function createIssue(payload: unknown) {\n"
            "  return fetch('/issue/create', { method: 'POST', body: JSON.stringify(payload) });\n"
            "}\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/issue\")\n"
            "public class IssueController {\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service handles issue create API")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["edge_kind"] == "http_path" and "/issue/create" in edge["match_reason"]
        )

        self.assertEqual(edge["from_repo"], "Portal Repo")
        self.assertEqual(edge["to_repo"], "Issue Service")
        self.assertGreaterEqual(edge["confidence"], 0.95)
        self.assertEqual(edge["to_file"], "controller/IssueController.java")
        self.assertGreater(edge["to_line"], 0)
        self.assertFalse(
            any(
                graph_edge["from_repo"] == "Issue Service"
                and graph_edge["to_repo"] == "Portal Repo"
                and graph_edge["edge_kind"] == "http_path"
                for graph_edge in payload["repo_graph"]["edges"]
            )
        )

    def test_cross_repo_graph_resolves_feign_config_placeholders(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"${issue.service.name}\", url = \"${issue.service.url}\")\n"
            "public interface IssueClient {\n"
            "    @PostMapping(\"/create\")\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        config_file = portal_path / "application.properties"
        config_file.write_text(
            "issue.service.name=issue-service\n"
            "issue.service.url=http://issue-service/issue\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/issue\")\n"
            "public class IssueController {\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service does feign issue client call")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["edge_kind"] == "http_path" and "/issue/create" in edge["match_reason"]
        )

        self.assertEqual(edge["from_repo"], "Portal Repo")
        self.assertEqual(edge["to_repo"], "Issue Service")
        self.assertGreaterEqual(edge["confidence"], 0.95)
        self.assertIn("/issue/create", edge["match_reason"])

    def test_cross_repo_graph_resolves_feign_yaml_config_placeholders(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"${issue.service.name}\", url = \"${issue.service.url}\")\n"
            "public interface IssueClient {\n"
            "    @PostMapping(\"/create\")\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        config_file = portal_path / "application.yml"
        config_file.write_text(
            "issue:\n"
            "  service:\n"
            "    name: issue-service\n"
            "    url: http://issue-service/issue\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/issue\")\n"
            "public class IssueController {\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service does feign issue client call")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["edge_kind"] == "http_path" and "/issue/create" in edge["match_reason"]
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            config_edges = list(connection.execute("select edge_kind, to_name from entity_edges"))

        self.assertEqual(edge["from_repo"], "Portal Repo")
        self.assertEqual(edge["to_repo"], "Issue Service")
        self.assertGreaterEqual(edge["confidence"], 0.95)
        self.assertIn(("config", "issue.service.url"), config_edges)
        self.assertIn(("config_value", "http://issue-service/issue"), config_edges)

    def test_build_files_create_module_dependency_edges_and_repo_graph(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        (portal_path / "pom.xml").write_text(
            "<project>\n"
            "  <groupId>com.example</groupId>\n"
            "  <artifactId>portal-web</artifactId>\n"
            "  <dependencies>\n"
            "    <dependency>\n"
            "      <groupId>com.example</groupId>\n"
            "      <artifactId>issue-service-api</artifactId>\n"
            "      <version>1.0.0</version>\n"
            "    </dependency>\n"
            "  </dependencies>\n"
            "</project>\n",
            encoding="utf-8",
        )
        (service_path / "pom.xml").write_text(
            "<project>\n"
            "  <groupId>com.example</groupId>\n"
            "  <artifactId>issue-service-api</artifactId>\n"
            "</project>\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which repo does portal-web depend on")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue Service"
            and edge["edge_kind"] == "module_dependency"
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            flow_edges = list(connection.execute("select edge_kind, to_name, to_file from flow_edges"))

        self.assertGreaterEqual(edge["confidence"], 0.84)
        self.assertIn("exact build artifact match", edge["match_reason"])
        self.assertTrue(
            any(kind == "module_dependency" and target == "com.example:issue-service-api" for kind, target, _to_file in flow_edges)
        )

    def test_gradle_multi_module_edges_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "settings.gradle").write_text(
            "rootProject.name = 'portal'\n"
            "include ':issue-api', ':issue-service'\n",
            encoding="utf-8",
        )
        (repo_path / "issue-service" / "build.gradle").parent.mkdir(parents=True)
        (repo_path / "issue-service" / "build.gradle").write_text(
            "dependencies {\n"
            "    implementation project(':issue-api')\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, from_file
                    from entity_edges
                    where edge_kind in ('gradle_module', 'gradle_project_dependency', 'module_dependency', 'module_artifact')
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, from_file
                    from flow_edges
                    where edge_kind = 'module_dependency'
                    """
                )
            )

        self.assertIn(("gradle_module", "issue-api", "settings.gradle"), entity_edges)
        self.assertIn(("gradle_module", ":issue-api", "settings.gradle"), entity_edges)
        self.assertIn(("gradle_project_dependency", "issue-api", "issue-service/build.gradle"), entity_edges)
        self.assertIn(("gradle_project_dependency", ":issue-api", "issue-service/build.gradle"), entity_edges)
        self.assertIn(("module_dependency", "issue-api", "issue-service/build.gradle"), entity_edges)
        self.assertTrue(any(kind == "module_dependency" and target == "issue-api" for kind, target, _from_file in flow_edges))

    def test_runtime_trace_jsonl_creates_dynamic_flow_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        trace_path = repo_path / "runtime-traces" / "source-code-qa.jsonl"
        trace_path.parent.mkdir(parents=True)
        trace_path.write_text(
            "\n".join(
                json.dumps(row)
                for row in [
                    {
                        "kind": "call",
                        "from": "IssueController.createIssue",
                        "to": "IssueServiceImpl.createIssue",
                        "evidence": "observed trace id t1",
                    },
                    {"kind": "sql", "from": "IssueServiceImpl.createIssue", "table": "issue_table"},
                    {"kind": "message", "from": "IssueEventPublisher.publish", "topic": "issue.created"},
                    {"kind": "config", "key": "feature.issue.fast.enabled", "value": True},
                    {"kind": "route", "path": "/api/issues/create", "handler": "IssueController.createIssue"},
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, from_file
                    from entity_edges
                    where edge_kind like 'runtime_%'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, evidence
                    from flow_edges
                    where evidence like '%observed trace id t1%'
                       or to_name in ('issue_table', 'issue.created', 'feature.issue.fast.enabled', '/api/issues/create')
                    """
                )
            )

        self.assertIn(("runtime_call", "IssueServiceImpl.createIssue", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_sql", "issue_table", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_message", "issue.created", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_config", "feature.issue.fast.enabled", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_route", "/api/issues/create", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertTrue(any(kind == "runtime" and target == "IssueServiceImpl.createIssue" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "db_runtime" and target == "issue_table" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "message_runtime" and target == "issue.created" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "config" and target == "feature.issue.fast.enabled" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "route" and target == "/api/issues/create" for kind, target, _ in flow_edges))

    def test_package_json_dependencies_create_module_dependency_repo_graph_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue SDK", "url": "https://git.example.com/team/issue-sdk.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        sdk_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        sdk_path = self.service._repo_path("AF:All", sdk_entry)
        (portal_path / ".git").mkdir(parents=True)
        (sdk_path / ".git").mkdir(parents=True)
        (portal_path / "package.json").write_text(
            json.dumps(
                {
                    "name": "@example/portal-web",
                    "dependencies": {"@example/issue-sdk": "^1.2.3"},
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (sdk_path / "package.json").write_text(
            json.dumps({"name": "@example/issue-sdk"}, indent=2),
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which npm package does portal-web depend on")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue SDK"
            and edge["edge_kind"] == "module_dependency"
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            flow_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))

        self.assertGreaterEqual(edge["confidence"], 0.84)
        self.assertIn(("module_dependency", "@example/issue-sdk"), flow_edges)

    def test_messaging_publish_consume_edges_create_cross_repo_graph(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        publisher_file = portal_path / "events" / "IssueEventPublisher.java"
        publisher_file.parent.mkdir(parents=True)
        publisher_file.write_text(
            "public class IssueEventPublisher {\n"
            "    public void publish(IssueCreatedEvent event) {\n"
            "        kafkaTemplate.send(\"issue.created\", event);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        listener_file = service_path / "events" / "IssueEventListener.java"
        listener_file.parent.mkdir(parents=True)
        listener_file.write_text(
            "public class IssueEventListener {\n"
            "    @KafkaListener(topics = \"issue.created\")\n"
            "    public void consume(IssueCreatedEvent event) { }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service consumes issue created event")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue Service"
            and edge["edge_kind"] == "message_topic"
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            publish_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))
        with sqlite3.connect(self.service._index_path(service_path)) as connection:
            consume_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))

        self.assertGreaterEqual(edge["confidence"], 0.93)
        self.assertIn(("message_publish", "issue.created"), publish_edges)
        self.assertIn(("message_consume", "issue.created"), consume_edges)

    def test_configured_message_topic_placeholders_create_cross_repo_graph(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        publisher_file = portal_path / "events" / "IssueEventPublisher.java"
        publisher_file.parent.mkdir(parents=True)
        publisher_file.write_text(
            "public class IssueEventPublisher {\n"
            "    public void publish(IssueCreatedEvent event) {\n"
            "        kafkaTemplate.send(\"${issue.topic.name}\", event);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        (portal_path / "application.properties").write_text("issue.topic.name=issue.created\n", encoding="utf-8")
        listener_file = service_path / "events" / "IssueEventListener.java"
        listener_file.parent.mkdir(parents=True)
        listener_file.write_text(
            "public class IssueEventListener {\n"
            "    @KafkaListener(topics = \"${issue.topic.name}\")\n"
            "    public void consume(IssueCreatedEvent event) { }\n"
            "}\n",
            encoding="utf-8",
        )
        (service_path / "application.properties").write_text("issue.topic.name=issue.created\n", encoding="utf-8")
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service consumes configured issue topic")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue Service"
            and edge["edge_kind"] == "message_topic"
        )

        self.assertGreaterEqual(edge["confidence"], 0.93)
        self.assertIn("issue.created", edge["match_reason"])

    def test_exact_build_artifact_matching_links_repo_without_display_alias(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Payments Core", "url": "https://git.example.com/team/payments-core.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        target = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        target_path = self.service._repo_path("AF:All", target)
        (portal_path / ".git").mkdir(parents=True)
        (target_path / ".git").mkdir(parents=True)
        (portal_path / "pom.xml").write_text(
            "<project><dependencies><dependency>"
            "<groupId>com.example</groupId><artifactId>fraud-ledger-api</artifactId><version>1.0.0</version>"
            "</dependency></dependencies></project>",
            encoding="utf-8",
        )
        (target_path / "pom.xml").write_text(
            "<project><groupId>com.example</groupId><artifactId>fraud-ledger-api</artifactId></project>",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which repo provides fraud-ledger-api dependency")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Payments Core"
            and edge["edge_kind"] == "module_dependency"
        )

        self.assertGreaterEqual(edge["confidence"], 0.97)
        self.assertIn("exact build artifact match", edge["match_reason"])

    def test_shared_table_write_read_creates_cross_repo_lineage_edge(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Issue Writer", "url": "https://git.example.com/team/issue-writer.git"},
                {"display_name": "Issue Reporting", "url": "https://git.example.com/team/issue-reporting.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        writer = type("Entry", (), entries[0])()
        reader = type("Entry", (), entries[1])()
        writer_path = self.service._repo_path("AF:All", writer)
        reader_path = self.service._repo_path("AF:All", reader)
        (writer_path / ".git").mkdir(parents=True)
        (reader_path / ".git").mkdir(parents=True)
        writer_file = writer_path / "repository" / "IssueWriterRepository.java"
        writer_file.parent.mkdir(parents=True)
        writer_file.write_text(
            "public class IssueWriterRepository {\n"
            "    public void saveIssue() { jdbcTemplate.update(\"insert into shared_issue_table(id) values (?)\", 1); }\n"
            "}\n",
            encoding="utf-8",
        )
        reader_file = reader_path / "repository" / "IssueReportRepository.java"
        reader_file.parent.mkdir(parents=True)
        reader_file.write_text(
            "public class IssueReportRepository {\n"
            "    public Issue loadIssue() { return jdbcTemplate.queryForObject(\"select * from shared_issue_table\", mapper); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which repo reads shared_issue_table after it is written")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Issue Writer"
            and edge["to_repo"] == "Issue Reporting"
            and edge["edge_kind"] == "shared_table"
        )

        self.assertGreaterEqual(edge["confidence"], 0.86)
        self.assertIn("db write/read table overlap", edge["match_reason"])

    def test_sql_read_write_and_spring_condition_edges_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "@Profile(\"prod\")\n"
            "public class IssueRepository {\n"
            "    @Value(\"${issue.topic.name:issue.created}\")\n"
            "    private String topicName;\n"
            "    @Qualifier(\"primaryJdbcTemplate\")\n"
            "    private JdbcTemplate jdbcTemplate;\n"
            "    public Issue loadIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "    public void saveIssue() {\n"
            "        jdbcTemplate.update(\"insert into issue_table(id) values (?)\", 1);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(connection.execute("select edge_kind, to_name from entity_edges"))
            flow_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))

        self.assertIn(("spring_profile", "prod"), entity_edges)
        self.assertIn(("config", "issue.topic.name"), entity_edges)
        self.assertIn(("bean_qualifier", "primaryJdbcTemplate"), entity_edges)
        self.assertIn(("db_read", "issue_table"), flow_edges)
        self.assertIn(("db_write", "issue_table"), flow_edges)

    def test_followup_context_augments_short_question(self):
        question, context = self.service._apply_conversation_context(
            "继续找这个表哪里写入",
            {
                "question": "what table does issue creation use",
                "matches": [{"path": "repository/IssueRepository.java", "snippet": "select * from issue_table"}],
                "trace_paths": [{"edges": [{"to_name": "issue_table", "to_file": ""}]}],
                "structured_answer": {"claims": [{"text": "IssueRepository reads issue_table"}]},
            },
        )

        self.assertTrue(context["used"])
        self.assertIn("issue_table", question)

    def test_domain_profile_terms_can_boost_configured_data_source_names(self):
        profile_path = Path(self.temp_dir.name) / "profiles.json"
        profile_path.write_text(
            '{'
            '"default": {"source_terms": ["LedgerGateway"], "data_carriers": ["LedgerDTO"]},'
            '"AF": {"source_terms": ["FraudLedgerRepository"]}'
            '}',
            encoding="utf-8",
        )
        self.service.domain_profile_path = profile_path
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "FraudLedgerRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class FraudLedgerRepository {\n"
            "    public LedgerDTO loadLedger() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from fraud_ledger\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="what data source is used for ledger")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["matches"][0]["path"], "repository/FraudLedgerRepository.java")

    def test_retrieval_prefers_symbol_definition_over_noisy_readme(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        readme = repo_path / "README.md"
        readme.write_text(
            "batchCreateJiraIssue batchCreateJiraIssue batchCreateJiraIssue\n"
            "This repo mentions batchCreateJiraIssue many times in docs.\n",
            encoding="utf-8",
        )

        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        payload = {'route': '/api/v1/issues/batchCreateJiraIssue'}\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue', json=payload)\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where is batchCreateJiraIssue implemented")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["matches"][0]["path"], "bpmis/jira_client.py")
        self.assertTrue(
            "symbol matched" in payload["matches"][0]["reason"]
            or "planner_definition" in payload["matches"][0]["reason"]
            or "structure matched" in payload["matches"][0]["reason"]
        )

    def test_dependency_question_expands_to_service_and_integration_matches(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "decision" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    private ScreeningService screeningService;\n"
            "    private EcInfoIntegration ecInfoIntegration;\n"
            "    public void runPreCheck1() {\n"
            "        screeningService.screenApplicant();\n"
            "        ecInfoIntegration.fetchEcInfo();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        service_file = repo_path / "service" / "ScreeningService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class ScreeningService {\n"
            "    public void screenApplicant() {\n"
            "        // data screening source logic\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        integration_file = repo_path / "integration" / "EcInfoIntegration.java"
        integration_file.parent.mkdir(parents=True)
        integration_file.write_text(
            "public class EcInfoIntegration {\n"
            "    public void fetchEcInfo() {\n"
            "        // upstream ecinfo source\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="What data sources and integrations are checked for Term Loan Pre Check 1",
        )

        self.assertEqual(payload["status"], "ok")
        paths = [match["path"] for match in payload["matches"]]
        self.assertIn("decision/TermLoanPreCheckEngine.java", paths)
        self.assertIn("service/ScreeningService.java", paths)
        self.assertIn("integration/EcInfoIntegration.java", paths)
        dependency_reasons = [match["reason"] for match in payload["matches"] if match.get("trace_stage") == "dependency"]
        self.assertTrue(any("dependency trace" in reason for reason in dependency_reasons))

    def test_two_hop_trace_follows_downstream_service_from_entry_point(self):
        self.service.save_mapping(
            pm_team="CRMS",
            country="ID",
            repositories=[{"display_name": "Credit Risk", "url": "https://git.example.com/team/credit-risk.git"}],
        )
        entry = self.service.load_config()["mappings"]["CRMS:ID"][0]
        repo_path = self.service._repo_path("CRMS:ID", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "underwriting" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    private EcInfoService ecInfoService;\n"
            "    public void runPreCheck1() {\n"
            "        ecInfoService.loadEcInfo();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        service_file = repo_path / "service" / "EcInfoService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class EcInfoService {\n"
            "    public CustomerEcInfo loadEcInfo() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from cr_ec_info\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="CRMS",
            country="ID",
            question="What data sources are checked for Term Loan Pre Check 1",
        )

        paths = [match["path"] for match in payload["matches"]]
        self.assertIn("underwriting/TermLoanPreCheckEngine.java", paths)
        self.assertIn("service/EcInfoService.java", paths)
        two_hop_reasons = [match["reason"] for match in payload["matches"] if match.get("trace_stage") == "two_hop"]
        self.assertTrue(any("two-hop trace" in reason for reason in two_hop_reasons))

    def test_agent_trace_can_follow_service_to_repository(self):
        self.service.save_mapping(
            pm_team="CRMS",
            country="ID",
            repositories=[{"display_name": "Credit Risk", "url": "https://git.example.com/team/credit-risk.git"}],
        )
        entry = self.service.load_config()["mappings"]["CRMS:ID"][0]
        repo_path = self.service._repo_path("CRMS:ID", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "engine" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    private EligibilityService eligibilityService;\n"
            "    public void runPreCheck1() {\n"
            "        eligibilityService.evaluateEligibility();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        service_file = repo_path / "service" / "EligibilityService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class EligibilityService {\n"
            "    private CustomerRepository customerRepository;\n"
            "    public CustomerProfile evaluateEligibility() {\n"
            "        return customerRepository.findCustomerProfile();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        repository_file = repo_path / "repository" / "CustomerRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class CustomerRepository {\n"
            "    public CustomerProfile findCustomerProfile() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        base_matches = self.service._search_repo(
            type("Entry", (), entry)(),
            repo_path,
            self.service._question_tokens("Term Loan Pre Check 1"),
            question="What data source does Term Loan Pre Check 1 check?",
        )
        agent_matches = self.service._expand_agent_trace_matches(
            entries=[type("Entry", (), entry)()],
            key="CRMS:ID",
            question="What data source does Term Loan Pre Check 1 check?",
            base_matches=base_matches,
            limit=12,
        )

        paths = [match["path"] for match in agent_matches]
        self.assertIn("repository/CustomerRepository.java", paths)
        agent_reasons = [match["reason"] for match in agent_matches if str(match.get("trace_stage") or "").startswith("agent_trace")]
        self.assertTrue(any("agent trace" in reason for reason in agent_reasons))

    def test_evidence_compressor_extracts_concrete_data_sources(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "repository/CustomerRepository.java",
                "line_start": 10,
                "line_end": 14,
                "score": 100,
                "trace_stage": "agent_trace_2",
                "reason": "agent trace",
                "snippet": (
                    "public CustomerProfile loadProfile() {\n"
                    "    return jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper);\n"
                    "}\n"
                ),
            }
        ]

        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        self.assertTrue(compressed["data_sources"])
        self.assertIn("cr_customer_profile", " ".join(compressed["data_sources"]))
        self.assertEqual(quality["status"], "sufficient")

    def test_evidence_pack_structures_tables_and_missing_hops(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "repository/CustomerRepository.java",
                "line_start": 10,
                "line_end": 14,
                "score": 100,
                "trace_stage": "agent_trace_2",
                "reason": "agent trace",
                "snippet": (
                    "public CustomerProfile loadProfile() {\n"
                    "    return jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper);\n"
                    "}\n"
                ),
            }
        ]
        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        pack = self.service._build_evidence_pack(
            question="What data source does Term Loan Pre Check 1 check?",
            evidence_summary=compressed,
            matches=matches,
            trace_paths=[],
            quality_gate=quality,
        )

        self.assertEqual(pack["version"], 2)
        self.assertTrue(any(item["type"] == "table" for item in pack["items"]))
        self.assertIn("cr_customer_profile", " ".join(pack["tables"]))
        self.assertTrue(pack["read_write_points"])
        self.assertFalse(pack["missing_hops"])

    def test_planner_tool_dsl_finds_tables_routes_and_callees(self):
        _build_fixture_repositories(self.service)
        af_entry = self.service.load_config()["mappings"]["AF:All"][0]
        entry = type("Entry", (), af_entry)()
        repo_path = self.service._repo_path("AF:All", entry)
        self.service._build_repo_index("AF:All", entry, repo_path)

        table_matches = self.service._tool_find_tables(
            entry,
            repo_path,
            ["issue"],
            "what table does issue creation use",
            1,
        )
        route_matches = self.service._tool_find_api_routes(
            entry,
            repo_path,
            ["issue"],
            "which API handles issue create",
            1,
        )
        base_matches = self.service._search_repo(
            entry,
            repo_path,
            self.service._question_tokens("IssueController createIssue"),
            question="which service or repository does issue creation call",
        )
        callee_matches = self.service._tool_find_callees(
            entry,
            repo_path,
            base_matches,
            ["Issue"],
            "which service or repository does issue creation call",
            1,
        )

        self.assertIn("issue_table", " ".join(match["snippet"] for match in table_matches))
        self.assertTrue(any(match.get("retrieval") == "planner_api_route" for match in route_matches))
        self.assertTrue(any(match.get("retrieval") == "planner_callee" for match in callee_matches))

    def test_quality_gate_requests_deeper_trace_when_data_source_is_missing(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "engine/TermLoanPreCheckEngine.java",
                "line_start": 1,
                "line_end": 5,
                "score": 80,
                "trace_stage": "direct",
                "reason": "content matched",
                "snippet": (
                    "public class TermLoanPreCheckEngine {\n"
                    "    public void run(EngineTermLoanPreCheckLayer1Input input) {}\n"
                    "}\n"
                ),
            }
        ]

        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)
        trace_terms = self.service._quality_gate_trace_terms(
            "What data source does Term Loan Pre Check 1 check?",
            compressed,
            quality,
            matches,
        )

        self.assertEqual(quality["status"], "needs_more_trace")
        self.assertIn("concrete upstream source/table/API/repository evidence beyond DTO fields", quality["missing"])
        self.assertIn("repository", trace_terms)

    def test_quality_gate_trace_can_find_repository_after_weak_entry_match(self):
        self.service.save_mapping(
            pm_team="CRMS",
            country="ID",
            repositories=[{"display_name": "Credit Risk", "url": "https://git.example.com/team/credit-risk.git"}],
        )
        entry = self.service.load_config()["mappings"]["CRMS:ID"][0]
        repo_path = self.service._repo_path("CRMS:ID", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "engine" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    public void run(EngineTermLoanPreCheckLayer1Input input) {}\n"
            "}\n",
            encoding="utf-8",
        )

        repository_file = repo_path / "repository" / "DataSourceResultRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class DataSourceResultRepository {\n"
            "    public DataSourceResult loadDataSourceResult() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from cr_data_source_result\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        base_matches = self.service._search_repo(
            type("Entry", (), entry)(),
            repo_path,
            self.service._question_tokens("Term Loan Pre Check 1"),
            question="What data source does Term Loan Pre Check 1 check?",
        )
        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", base_matches[:1])
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)
        quality_matches = self.service._expand_quality_gate_matches(
            entries=[type("Entry", (), entry)()],
            key="CRMS:ID",
            question="What data source does Term Loan Pre Check 1 check?",
            base_matches=base_matches[:1],
            evidence_summary=compressed,
            quality_gate=quality,
            limit=12,
        )

        self.assertIn("repository/DataSourceResultRepository.java", [match["path"] for match in quality_matches])
        self.assertTrue(any(match.get("trace_stage") == "quality_gate" for match in quality_matches))

    def test_agent_plan_includes_data_source_trace_steps(self):
        compressed = {
            "intent": self.service._question_intent("What data source does Term Loan Pre Check 1 check?"),
            "entry_points": ["Credit Risk:engine/TermLoanPreCheckEngine.java:1-5"],
            "data_carriers": ["EngineTermLoanPreCheckLayer1Input"],
            "field_population": [],
            "downstream_components": [],
            "data_sources": [],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 1,
        }
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        plan = self.service._build_agent_plan("What data source does Term Loan Pre Check 1 check?", compressed, quality)

        step_names = [step["name"] for step in plan["steps"]]
        self.assertIn("trace_data_carriers", step_names)
        self.assertIn("trace_field_population", step_names)
        self.assertIn("trace_downstream_sources", step_names)
        self.assertIn("trace_dao_mapper_methods", step_names)
        self.assertIn("fill_quality_gap", step_names)
        flattened_terms = " ".join(term for step in plan["steps"] for term in step["terms"])
        self.assertIn("EngineTermLoanPreCheckLayer1Input", flattened_terms)
        self.assertNotIn("UnderwritingInitiationDTO", flattened_terms)

    def test_answer_self_check_retries_weak_data_source_answer(self):
        compressed = {
            "intent": self.service._question_intent("What data source does Term Loan Pre Check 1 check?"),
            "entry_points": ["Credit Risk:engine/TermLoanPreCheckEngine.java:1-5"],
            "data_carriers": ["DataSourceResult"],
            "field_population": ["UnderwritingInitiationProvider populates UnderwritingInitiationDTO"],
            "downstream_components": ["DataSourceResultRepository"],
            "data_sources": ["Credit Risk:repository/DataSourceResultRepository.java:10-14: select * from cr_data_source_result"],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 3,
        }
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        check = self.service._answer_self_check(
            "What data source does Term Loan Pre Check 1 check?",
            "The evidence does not specify the actual data source.",
            compressed,
            quality,
        )

        self.assertEqual(check["status"], "retry")
        self.assertTrue(check["issues"])

    def test_answer_self_check_rejects_dto_only_data_source_answer(self):
        compressed = {
            "intent": self.service._question_intent("What data source does Term Loan Pre Check 1 check?"),
            "entry_points": ["Credit Risk:engine/TermLoanPreCheckEngine.java:1-5"],
            "data_carriers": ["UnderwritingInitiationDTO", "CustomerInfo", "LoanInfo", "CreditRiskInfo"],
            "field_population": ["UnderwritingInitiationProvider populates UnderwritingInitiationDTO"],
            "downstream_components": ["DataSourceResultRepository"],
            "data_sources": ["Credit Risk:repository/DataSourceResultRepository.java:10-14: select * from cr_data_source_result"],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 4,
        }
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        check = self.service._answer_self_check(
            "What data source does Term Loan Pre Check 1 check?",
            "Term Loan Precheck 1 uses data from UnderwritingInitiationDTO, CustomerInfo, LoanInfo, and CreditRiskInfo.",
            compressed,
            quality,
        )

        self.assertEqual(check["status"], "retry")
        self.assertIn("answer stops at DTO/carrier layer instead of tracing upstream source", check["issues"])

    def test_answer_judge_requests_repair_for_uncited_thin_answer(self):
        evidence_pack = {
            "version": 2,
            "intent": self.service._question_intent("What data source does issue creation use?"),
            "items": [
                {
                    "type": "table",
                    "claim": "issue_table is referenced in Portal Repo:repository/IssueRepository.java:1-5",
                    "source_id": "S1",
                    "confidence": "high",
                    "hop": "source",
                    "supports_answer": True,
                }
            ],
            "tables": ["issue_table (Portal Repo:repository/IssueRepository.java:1-5)"],
            "apis": [],
            "missing_hops": [],
        }

        judge = self.service._judge_answer(
            "What data source does issue creation use?",
            "It uses a repository.",
            evidence_pack,
            {"status": "needs_citation", "issues": ["concrete answer claims need citation-backed evidence"]},
        )

        self.assertEqual(judge["status"], "repair")
        self.assertTrue(judge["repair_targets"])

    def test_llm_answer_judge_can_request_repair(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            judge_model="judge-lite",
            llm_judge_enabled=True,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        evidence_pack = {
            "version": 2,
            "intent": service._question_intent("What data source does issue creation use?"),
            "items": [
                {
                    "type": "table",
                    "claim": "issue_table is referenced in Portal Repo:repository/IssueRepository.java:1-5",
                    "source_id": "S1",
                    "confidence": "high",
                    "supports_answer": True,
                }
            ],
            "tables": ["issue_table (Portal Repo:repository/IssueRepository.java:1-5)"],
            "apis": [],
            "missing_hops": [],
        }
        result = LLMGenerateResult(
            payload={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "status": "repair",
                                            "confidence": "high",
                                            "issues": ["answer omits issue_table"],
                                            "repair_targets": ["include table source"],
                                        }
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 8},
            },
            usage={"promptTokenCount": 10, "candidatesTokenCount": 8},
            model="judge-lite",
            attempts=1,
        )

        with patch.object(service.llm_provider, "generate", return_value=result) as mocked_generate:
            judge = service._run_answer_judge(
                "What data source does issue creation use?",
                "It uses a repository.",
                evidence_pack,
                {"status": "ok", "issues": []},
            )

        self.assertEqual(judge["mode"], "llm_evidence_judge")
        self.assertEqual(judge["status"], "repair")
        self.assertEqual(judge["model"], "judge-lite")
        self.assertIn("answer omits issue_table", judge["issues"])
        self.assertEqual(mocked_generate.call_args.kwargs["primary_model"], "judge-lite")

    def test_llm_answer_judge_is_cached(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            judge_model="gemini-2.5-flash-lite",
            llm_judge_enabled=True,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        evidence_pack = {
            "version": 2,
            "intent": service._question_intent("What data source does issue creation use?"),
            "items": [
                {
                    "type": "table",
                    "claim": "issue_table is referenced in Portal Repo:repository/IssueRepository.java:1-5",
                    "source_id": "S1",
                    "confidence": "high",
                    "supports_answer": True,
                }
            ],
            "tables": ["issue_table (Portal Repo:repository/IssueRepository.java:1-5)"],
            "apis": [],
            "missing_hops": [],
        }
        result = LLMGenerateResult(
            payload={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "status": "ok",
                                            "confidence": "high",
                                            "issues": [],
                                            "repair_targets": [],
                                        }
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 8},
            },
            usage={"promptTokenCount": 10, "candidatesTokenCount": 8},
            model="gemini-2.5-flash-lite",
            attempts=1,
        )

        with patch.object(service.llm_provider, "generate", return_value=result) as mocked_generate:
            first = service._run_answer_judge(
                "What data source does issue creation use?",
                "IssueRepository reads issue_table [S1].",
                evidence_pack,
                {"status": "ok", "issues": []},
            )
            second = service._run_answer_judge(
                "What data source does issue creation use?",
                "IssueRepository reads issue_table [S1].",
                evidence_pack,
                {"status": "ok", "issues": []},
            )

        self.assertEqual(first["mode"], "llm_evidence_judge")
        self.assertEqual(second["mode"], "llm_evidence_judge")
        self.assertTrue(second["cached"])
        self.assertEqual(mocked_generate.call_count, 1)

    def test_llm_finalizer_blocks_dto_only_source_answer_when_source_missing(self):
        compressed = {
            "intent": self.service._question_intent("What data sources does Term Loan Precheck 1 underwriting call?"),
            "entry_points": ["Credit Risk:engine/Layer4TermLoanPreCheckEngineStrategy.java:1-8"],
            "data_carriers": ["DataSourceResult (Credit Risk:engine/Layer4TermLoanPreCheckEngineStrategy.java:1-8)"],
            "field_population": ["Credit Risk:engine/Layer4TermLoanPreCheckEngineStrategy.java:1-8: input.setDataSourceResult(result)"],
            "downstream_components": [],
            "data_sources": [],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 2,
        }
        quality = self.service._quality_gate("What data sources does Term Loan Precheck 1 underwriting call?", compressed)
        final = self.service._finalize_llm_answer(
            question="What data sources does Term Loan Precheck 1 underwriting call?",
            answer="It likely uses internal or external financial providers through integrations.",
            structured_answer=self.service._parse_structured_answer("It likely uses internal or external financial providers through integrations."),
            evidence_summary=compressed,
            quality_gate=quality,
            claim_check={"status": "needs_citation", "issues": ["concrete answer claims need citation-backed evidence"]},
            selected_matches=[
                {
                    "repo": "Credit Risk",
                    "path": "engine/Layer4TermLoanPreCheckEngineStrategy.java",
                    "line_start": 1,
                    "line_end": 8,
                }
            ],
        )

        self.assertEqual(final["answer_contract"]["status"], "blocked_missing_source")
        self.assertNotIn("likely", final["answer"].lower())
        self.assertIn("I cannot confirm the final upstream data source", final["answer"])
        self.assertIn("Missing link", final["answer"])

    def test_imported_dao_is_not_treated_as_concrete_data_source(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "service/UnderwritingInitiationService.java",
                "line_start": 1,
                "line_end": 6,
                "score": 100,
                "trace_stage": "agent_plan_3",
                "reason": "agent plan trace",
                "snippet": (
                    "import com.shopee.banking.crm.dao.CustomerInfoDAO;\n"
                    "public class UnderwritingInitiationService {\n"
                    "    private CustomerInfoDAO customerInfoDAO;\n"
                    "}\n"
                ),
            }
        ]

        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        self.assertFalse(compressed["data_sources"])
        self.assertEqual(quality["status"], "needs_more_trace")
        self.assertEqual(quality["policies"][0]["name"], "data_source")
        self.assertEqual(quality["policies"][0]["status"], "missing")

    def test_field_level_data_flow_edges_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    public EngineInput buildInput(DataSourceResult result) {\n"
            "        EngineInput input = new EngineInput();\n"
            "        input.setDataSourceResult(result);\n"
            "        return input;\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = {row[0] for row in connection.execute("select edge_kind from entity_edges")}
            flow_edges = {row[0] for row in connection.execute("select edge_kind from flow_edges")}

        self.assertIn("data_flow", entity_edges)
        self.assertIn("field_population", flow_edges)

    def test_static_qa_query_finds_security_and_quality_findings(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        risky_file = repo_path / "service" / "RiskyIssueService.java"
        risky_file.parent.mkdir(parents=True)
        risky_file.write_text(
            "public class RiskyIssueService {\n"
            "    private static final String PASSWORD = \"plain-secret\";\n"
            "    public void load(String issueId) {\n"
            "        String sql = \"select * from issue_table where id = \" + issueId;\n"
            "        try { callRemote(); } catch (Exception e) { e.printStackTrace(); }\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="what static QA security risks exist")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(any(match.get("retrieval") == "static_qa" for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("hardcoded_secret", evidence_text)
        self.assertIn("sql_string_concatenation", evidence_text)
        self.assertEqual(payload["answer_quality"]["policies"][0]["name"], "static_qa")
        self.assertEqual(payload["answer_quality"]["policies"][0]["status"], "satisfied")
        self.assertIn("static QA findings", payload["summary"])

    def test_impact_analysis_query_finds_upstream_and_downstream_surfaces(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="what is impacted if IssueService createIssue changes",
        )

        paths = {match["path"] for match in payload["matches"]}
        self.assertIn("controller/IssueController.java", paths)
        self.assertIn("repository/IssueRepository.java", paths)
        self.assertTrue(any(match.get("retrieval") in {"planner_caller", "planner_callee", "flow_graph", "entity_graph"} for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("impact_surfaces", payload["evidence_pack"])
        self.assertIn("IssueRepository", evidence_text)
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["impact_analysis"], "satisfied")

    def test_test_coverage_query_finds_tests_and_assertions(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="is IssueService createIssue covered by tests",
        )

        paths = {match["path"] for match in payload["matches"]}
        self.assertIn("src/test/java/IssueServiceTest.java", paths)
        self.assertTrue(any(match.get("retrieval") == "test_coverage" for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("test_coverage", payload["evidence_pack"])
        self.assertIn("IssueServiceTest", evidence_text)
        self.assertTrue("verify" in evidence_text or "assert" in evidence_text)
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["test_coverage"], "satisfied")

    def test_operational_boundary_query_finds_transaction_and_cache_annotations(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="is IssueService createIssue transactional or cached",
        )

        paths = {match["path"] for match in payload["matches"]}
        self.assertIn("service/IssueService.java", paths)
        self.assertTrue(any(match.get("retrieval") == "operational_boundary" for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("operational_boundaries", payload["evidence_pack"])
        self.assertIn("Transactional", evidence_text)
        self.assertIn("Cacheable", evidence_text)
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["operational_boundary"], "satisfied")

    def test_claim_verifier_flags_uncited_concrete_claims(self):
        evidence_summary = {
            "data_sources": ["Portal Repo:repository/IssueRepository.java:3-5: select * from issue_table"],
            "api_or_config": [],
        }
        selected_matches = [
            {
                "repo": "Portal Repo",
                "path": "repository/IssueRepository.java",
                "line_start": 1,
                "line_end": 5,
            }
        ]

        check = self.service._verify_answer_claims(
            "Issue creation reads from issue_table through IssueRepository.",
            evidence_summary,
            selected_matches,
        )

        self.assertEqual(check["status"], "needs_citation")
        self.assertTrue(check["unsupported_claims"])

    def test_structured_answer_parser_accepts_json_and_fallback_prose(self):
        parsed = self.service._parse_structured_answer(
            '{"direct_answer":"Uses issue_table","claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],"missing_evidence":[],"confidence":"high"}'
        )
        fallback = self.service._parse_structured_answer("IssueRepository reads issue_table [S1].")

        self.assertEqual(parsed["format"], "json")
        self.assertEqual(parsed["claims"][0]["citations"], ["S1"])
        self.assertEqual(fallback["format"], "prose_fallback")
        self.assertEqual(fallback["claims"][0]["citations"], ["S1"])

    def test_eval_runner_checks_trace_paths_and_structured_claims(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        result = _evaluate_case(
            self.service,
            {
                "id": "trace-path-eval",
                "pm_team": "AF",
                "country": "All",
                "question": "what table does IssueRepository use",
                "expected_paths": ["repository/IssueRepository.java"],
                "min_trace_paths": 1,
                "expected_trace_path_terms": ["issue_table"],
                "expected_answer_policy_statuses": {"data_source": "satisfied"},
                "expected_evidence_pack_terms": ["issue_table"],
                "category": "data_source",
            },
        )

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["category"], "data_source")
        self.assertEqual(result["answer_policies"]["data_source"], "satisfied")

    def test_gemini_query_returns_answer_and_usage(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )

        fake_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"Short answer: batchCreateJiraIssue is in BPMISClient.",'
                                        '"claims":[{"text":"BPMISClient defines batchCreateJiraIssue","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 123, "candidatesTokenCount": 45},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "gemini_flash")
        self.assertIn("Short answer", payload["llm_answer"])
        self.assertEqual(payload["llm_usage"]["promptTokenCount"], 123)
        self.assertEqual(payload["llm_provider"], "gemini")
        self.assertEqual(payload["llm_model"], "gemini-2.5-flash")
        self.assertFalse(payload["llm_cached"])
        request_payload = mocked_post.call_args_list[0].kwargs["json"]
        self.assertEqual(request_payload["generationConfig"]["responseMimeType"], "application/json")
        self.assertIn("responseSchema", request_payload["generationConfig"])
        self.assertEqual(request_payload["generationConfig"]["thinkingConfig"]["thinkingBudget"], 512)
        self.assertIn(payload["llm_thinking_budget"], {512, 2048})
        self.assertEqual(payload["llm_route"]["mode"], "manual")
        telemetry = json.loads(service.telemetry_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertEqual(telemetry["llm_provider"], "gemini")
        self.assertEqual(telemetry["llm_model"], "gemini-2.5-flash")
        self.assertIn(telemetry["llm_thinking_budget"], {512, 2048})
        self.assertEqual(telemetry["llm_route"]["mode"], "manual")
        self.assertEqual(telemetry["versions"]["cache"], 7)
        self.assertIn("llm_latency_ms", telemetry)
        self.assertIn("llm_attempt_log", telemetry)
        self.assertIn("answer_contract", telemetry)
        self.assertIn("evidence_pack_summary", telemetry)
        self.assertIn("answer_judge", telemetry)
        self.assertEqual(payload["evidence_pack"]["version"], 2)
        self.assertIn("answer_judge", payload)

    def test_llm_answer_cache_requires_current_versions(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        cache_key = service._answer_cache_key(
            provider="gemini",
            model="gemini-2.5-flash",
            question="where is createIssue",
            answer_mode="gemini_flash",
            llm_budget_mode="balanced",
            context="S1 createIssue",
        )
        service._store_cached_answer(
            cache_key,
            answer="createIssue is in IssueController [S1]",
            usage={"totalTokenCount": 12},
            answer_quality={"status": "sufficient"},
            provider="gemini",
            model="gemini-2.5-flash",
        )
        cached = service._load_cached_answer(cache_key)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["versions"]["cache"], 7)
        cache_path = service.answer_cache_root / f"{cache_key}.json"
        stale_payload = json.loads(cache_path.read_text(encoding="utf-8"))
        stale_payload["versions"]["router"] = -1
        cache_path.write_text(json.dumps(stale_payload), encoding="utf-8")
        self.assertIsNone(service._load_cached_answer(cache_key))

    def test_openai_compatible_provider_returns_answer(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="openai_compatible",
            openai_api_key="openai-key",
            openai_model="code-balanced",
            openai_fallback_model="code-lite",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"direct_answer":"batchCreateJiraIssue is in BPMISClient.",'
                                '"claims":[{"text":"BPMISClient defines batchCreateJiraIssue","citations":["S1"]}],'
                                '"missing_evidence":[],"confidence":"high"}'
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 50, "completion_tokens": 20, "total_tokens": 70},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["llm_provider"], "openai_compatible")
        self.assertEqual(payload["llm_model"], "code-balanced")
        self.assertEqual(payload["llm_usage"]["total_tokens"], 70)
        self.assertIn("batchCreateJiraIssue", payload["llm_answer"])
        self.assertIn("/chat/completions", mocked_post.call_args.args[0])
        request_payload = mocked_post.call_args.kwargs["json"]
        self.assertEqual(request_payload["model"], "code-balanced")
        self.assertEqual(request_payload["response_format"], {"type": "json_object"})
        self.assertEqual(request_payload["messages"][0]["role"], "system")

    def test_auto_simple_lookup_uses_flash_lite_with_zero_thinking(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"batchCreateJiraIssue is in BPMISClient.",'
                                        '"claims":[{"text":"BPMISClient defines batchCreateJiraIssue","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 20, "candidatesTokenCount": 8, "totalTokenCount": 28},
            },
            text='{"ok":true}',
            raise_for_status=lambda: None,
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="auto",
                llm_budget_mode="auto",
            )

        self.assertEqual(payload["llm_budget_mode"], "cheap")
        self.assertEqual(payload["llm_model"], "gemini-2.5-flash-lite")
        self.assertEqual(payload["llm_thinking_budget"], 0)
        self.assertIn("gemini-2.5-flash-lite", mocked_post.call_args.args[0])
        self.assertEqual(mocked_post.call_args.kwargs["json"]["generationConfig"]["thinkingConfig"]["thinkingBudget"], 0)

    def test_gemini_thinking_budget_is_clamped_to_supported_minimum(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

        self.assertEqual(service._normalize_thinking_budget_for_provider(0), 0)
        self.assertEqual(service._normalize_thinking_budget_for_provider(256), 512)
        self.assertEqual(service._normalize_thinking_budget_for_provider(999999), 24576)

    def test_auto_answer_mode_routes_data_source_questions_to_deep_budget(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"IssueRepository reads issue_table.",'
                                        '"claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 123, "candidatesTokenCount": 45, "totalTokenCount": 168},
            },
            text='{"ok":true}',
            raise_for_status=lambda: None,
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="what data source does issue creation use",
                answer_mode="auto",
                llm_budget_mode="auto",
            )

        self.assertEqual(payload["answer_mode"], "auto")
        self.assertEqual(payload["llm_budget_mode"], "deep")
        self.assertEqual(payload["llm_route"]["mode"], "auto")
        self.assertIn("data_source_trace", payload["llm_route"]["reason"])
        self.assertIn("IssueRepository reads issue_table", payload["llm_answer"])
        self.assertIn("gemini-2.5-flash", mocked_post.call_args_list[0].args[0])
        self.assertEqual(mocked_post.call_args_list[0].kwargs["json"]["generationConfig"]["thinkingConfig"]["thinkingBudget"], 1024)
        self.assertIn(payload["llm_thinking_budget"], {1024, 2048})

    def test_gemini_failure_falls_back_to_retrieval_only(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )

        class FakeErrorResponse:
            text = '{"error":"quota exceeded"}'

            def raise_for_status(self):
                import requests
                raise requests.HTTPError(response=self)

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=FakeErrorResponse()):
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "retrieval_only")
        self.assertIn("Showing code-search results instead", payload["fallback_notice"]["message"])
        self.assertTrue(payload["matches"])

    def test_gemini_503_retries_and_falls_back_to_lite_model(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gemini_model="gemini-2.5-flash",
            gemini_fallback_model="gemini-2.5-flash-lite",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )

        class RetryableResponse:
            ok = False
            status_code = 503
            text = '{"error":{"code":503,"status":"UNAVAILABLE"}}'

            def raise_for_status(self):
                import requests
                raise requests.HTTPError(response=self)

        success_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [{"content": {"parts": [{"text": "Recovered answer: batchCreateJiraIssue is handled in the BPMIS client method and grounded by the retrieved source reference."}]}}],
                "usageMetadata": {"promptTokenCount": 50, "candidatesTokenCount": 20},
            },
            text='{"ok":true}',
            raise_for_status=lambda: None,
        )

        with patch("bpmis_jira_tool.source_code_qa.time.sleep"), patch(
            "bpmis_jira_tool.source_code_qa.requests.post",
            side_effect=[RetryableResponse(), RetryableResponse(), RetryableResponse(), success_response],
        ) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "gemini_flash")
        self.assertEqual(payload["llm_model"], "gemini-2.5-flash-lite")
        self.assertEqual(payload["llm_attempts"], 4)
        self.assertIn("Recovered answer", payload["llm_answer"])
        self.assertEqual(mocked_post.call_count, 4)

    def test_llm_runtime_honors_retry_after_and_records_attempts(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            llm_timeout_seconds=45,
            llm_max_retries=1,
            llm_backoff_seconds=1.0,
            llm_max_backoff_seconds=5.0,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

        class RateLimitedResponse:
            ok = False
            status_code = 429
            text = '{"error":{"code":429,"status":"RESOURCE_EXHAUSTED"}}'
            headers = {"Retry-After": "0.25"}

        success_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [{"content": {"parts": [{"text": '{"direct_answer":"ok","claims":[],"missing_evidence":[],"confidence":"high"}'}]}}],
                "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2, "totalTokenCount": 5},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.time.sleep") as mocked_sleep, patch(
            "bpmis_jira_tool.source_code_qa.requests.post",
            side_effect=[RateLimitedResponse(), success_response],
        ) as mocked_post:
            result = service.llm_provider.generate(
                payload={"contents": [{"parts": [{"text": "hi"}]}], "generationConfig": {"maxOutputTokens": 20}},
                primary_model="gemini-2.5-flash",
                fallback_model="",
            )

        self.assertEqual(result.attempts, 2)
        mocked_sleep.assert_called_once_with(0.25)
        self.assertEqual(mocked_post.call_args.kwargs["timeout"], 45)
        self.assertEqual(result.attempt_log[0]["status"], 429)
        self.assertEqual(result.attempt_log[1]["status"], "ok")
        self.assertEqual(result.usage["totalTokenCount"], 5)


if __name__ == "__main__":
    unittest.main()
