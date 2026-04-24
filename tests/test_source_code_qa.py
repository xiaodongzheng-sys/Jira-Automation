import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from bpmis_jira_tool.source_code_qa import SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS
from bpmis_jira_tool.web import create_app


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

    def test_config_api_reports_llm_not_ready_by_default(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.get("/api/source-code-qa/config")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertFalse(payload["llm_ready"])

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
                },
            )

        self.assertEqual(response.status_code, 200)
        feedback_path = Path(self.temp_dir.name) / "source_code_qa" / "feedback.jsonl"
        self.assertTrue(feedback_path.exists())
        self.assertIn("too_vague", feedback_path.read_text(encoding="utf-8"))


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

        payload = self.service.query(pm_team="AF", country="All", question="batchCreateJiraIssue BPMIS API")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["answer_mode"], "retrieval_only")
        self.assertEqual(payload["matches"][0]["path"], "bpmis/jira_client.py")
        self.assertIn("batchCreateJiraIssue", payload["matches"][0]["snippet"])

    def test_query_builds_persistent_index_and_citations(self):
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

        payload = self.service.query(pm_team="AF", country="All", question="where is batchCreateJiraIssue implemented")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(self.service._index_path(repo_path).exists())
        self.assertIn("persistent_index", {match.get("retrieval") for match in payload["matches"]})
        self.assertEqual(payload["citations"][0]["id"], "S1")
        self.assertEqual(payload["citations"][0]["path"], "bpmis/jira_client.py")
        self.assertEqual(self.service.repo_status("AF:All")[0]["index"]["state"], "ready")

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

        payload = self.service.query(pm_team="AF", country="All", question="where is IssueController createIssue API")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(any("structure matched" in match["reason"] for match in payload["matches"]))
        index_path = self.service._index_path(repo_path)
        with sqlite3.connect(index_path) as connection:
            definition_count = connection.execute("select count(*) from definitions").fetchone()[0]
            reference_count = connection.execute("select count(*) from references_index").fetchone()[0]
            edge_count = connection.execute("select count(*) from graph_edges").fetchone()[0]
            fts_count = connection.execute("select count(*) from lines_fts").fetchone()[0]
        self.assertGreaterEqual(definition_count, 3)
        self.assertGreaterEqual(reference_count, 3)
        self.assertGreaterEqual(edge_count, 1)
        self.assertGreaterEqual(fts_count, 1)
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

        payload = self.service.query(pm_team="AF", country="All", question="trace issue create API data source")

        retrievals = {match.get("retrieval") for match in payload["matches"]}
        paths = {match["path"] for match in payload["matches"]}
        self.assertTrue({"planner_definition", "planner_reference", "code_graph"} & retrievals)
        self.assertIn("repository/IssueRepository.java", paths)

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
                                {"text": "Short answer\n- bpmis/jira_client.py:1-3"}
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 123, "candidatesTokenCount": 45},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response):
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
        self.assertFalse(payload["llm_cached"])

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
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "retrieval_only")
        self.assertIn("Showing retrieval-only results instead", payload["fallback_notice"]["message"])
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


if __name__ == "__main__":
    unittest.main()
