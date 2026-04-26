import json
import os
import tempfile
import time
import unittest
from unittest.mock import patch

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_protocol import sign_headers, verify_signature
from bpmis_jira_tool.local_agent_server import create_local_agent_app


class LocalAgentProtocolTests(unittest.TestCase):
    def test_signature_round_trip_and_tamper_rejection(self):
        body = b'{"question":"hello"}'
        headers = sign_headers(secret="shared-secret", method="POST", path="/api/local-agent/source-code-qa/query", body=body)

        verify_signature(
            secret="shared-secret",
            method="POST",
            path="/api/local-agent/source-code-qa/query",
            body=body,
            timestamp=headers["X-Local-Agent-Timestamp"],
            nonce=headers["X-Local-Agent-Nonce"],
            signature=headers["X-Local-Agent-Signature"],
        )

        with self.assertRaises(ToolError):
            verify_signature(
                secret="shared-secret",
                method="POST",
                path="/api/local-agent/source-code-qa/query",
                body=b'{"question":"changed"}',
                timestamp=headers["X-Local-Agent-Timestamp"],
                nonce=headers["X-Local-Agent-Nonce"],
                signature=headers["X-Local-Agent-Signature"],
            )

    def test_stale_signature_is_rejected(self):
        with self.assertRaises(ToolError):
            verify_signature(
                secret="shared-secret",
                method="POST",
                path="/api/local-agent/source-code-qa/query",
                body=b"{}",
                timestamp=str(int(time.time()) - 1000),
                nonce="abc",
                signature="bad",
                max_skew_seconds=10,
            )


class LocalAgentServerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        env = {
            "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
            "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
            "SOURCE_CODE_QA_LLM_PROVIDER": "codex_cli_bridge",
        }
        self.env_patch = patch.dict(os.environ, env, clear=True)
        self.env_patch.start()
        self.dotenv_patch = patch("bpmis_jira_tool.config.find_dotenv", return_value="")
        self.dotenv_patch.start()
        self.app = create_local_agent_app()

    def tearDown(self):
        self.dotenv_patch.stop()
        self.env_patch.stop()
        self.temp_dir.cleanup()

    def _post_signed(self, path, payload):
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        headers = sign_headers(secret="shared-secret", method="POST", path=path, body=body)
        headers["Content-Type"] = "application/json"
        return self.app.test_client().post(path, data=body, headers=headers)

    def test_healthz_is_public_and_reports_capabilities(self):
        response = self.app.test_client().get("/healthz")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["capabilities"]["source_code_qa"])

    def test_signed_source_code_query_delegates_to_local_service(self):
        with patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
            return_value={"status": "ok", "summary": "agent answer", "matches": []},
        ) as query:
            response = self._post_signed(
                "/api/local-agent/source-code-qa/query",
                {"pm_team": "AF", "country": "All", "question": "where is createIssue", "answer_mode": "auto", "llm_budget_mode": "auto"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["summary"], "agent answer")
        query.assert_called_once()

    def test_unsigned_source_code_query_is_rejected(self):
        response = self.app.test_client().post("/api/local-agent/source-code-qa/query", json={"question": "hello"})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["status"], "error")


if __name__ == "__main__":
    unittest.main()
