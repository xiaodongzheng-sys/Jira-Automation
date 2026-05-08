import tempfile
import time
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import Mock, patch

from flask import Flask

from bpmis_jira_tool.bpmis_client import build_bpmis_client
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_protocol import verify_signature
from bpmis_jira_tool.models import InputRow
from bpmis_jira_tool.source_code_qa_factory import source_code_qa_data_root
from bpmis_jira_tool.source_code_qa_jobs import SourceCodeQAQueryScheduler
from bpmis_jira_tool.source_code_qa_match_grading import evidence_role, match_answer_grade, match_is_definition_only
from bpmis_jira_tool.source_code_qa_types import SourceCodeQALLMError


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

        with self.assertRaisesRegex(RuntimeError, "requires a default runner"):
            scheduler._run_job(app, "job-1", {}, "owner@npt.sg", None)

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


if __name__ == "__main__":
    unittest.main()
