from __future__ import annotations

import json
import subprocess
import unittest
from unittest.mock import patch

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa_llm_providers import (
    DEFAULT_CLAUDE_CLI_MODEL,
    ClaudeCliBridgeSourceCodeQALLMProvider,
)


def _payload() -> dict:
    return {
        "codex_prompt_mode": "seatalk_7_day_insights_v4",
        "systemInstruction": {"parts": [{"text": "Return only JSON."}]},
        "contents": [{"parts": [{"text": "Summarize the day."}]}],
        "_llm_ledger_flow": "seatalk",
        "_llm_ledger_route": "cheap",
    }


def _completed(returncode: int, stdout: str, stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["claude"], returncode=returncode, stdout=stdout, stderr=stderr)


class ClaudeCliBridgeProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = ClaudeCliBridgeSourceCodeQALLMProvider(
            workspace_root="/tmp", model="claude-opus-4-8", claude_binary="claude"
        )
        # Avoid touching the on-disk LLM call ledger during tests.
        self._ledger_patch = patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.record_llm_call", return_value=None
        )
        self._ledger_patch.start()
        self.addCleanup(self._ledger_patch.stop)

    def test_defaults_model_to_opus(self) -> None:
        provider = ClaudeCliBridgeSourceCodeQALLMProvider(workspace_root="/tmp", claude_binary="claude")
        self.assertEqual(provider.model, DEFAULT_CLAUDE_CLI_MODEL)

    def test_generate_extracts_result_field(self) -> None:
        stdout = json.dumps({"type": "result", "is_error": False, "result": '{"summary": "ok"}'})
        with patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.shutil.which", return_value="/opt/homebrew/bin/claude"
        ), patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.subprocess.run", return_value=_completed(0, stdout)
        ) as run_mock:
            result = self.provider.generate(payload=_payload(), primary_model="", fallback_model="")

        self.assertEqual(result.payload["text"], '{"summary": "ok"}')
        self.assertEqual(result.model, "claude-opus-4-8")
        command = run_mock.call_args.args[0]
        self.assertIn("-p", command)
        self.assertIn("--output-format", command)
        self.assertEqual(command[command.index("--model") + 1], "claude-opus-4-8")
        # The user prompt is piped on stdin, not embedded in argv.
        self.assertEqual(run_mock.call_args.kwargs["input"], "Summarize the day.")

    def test_generate_raises_on_is_error_payload(self) -> None:
        stdout = json.dumps({"type": "result", "is_error": True, "result": "Not logged in · Please run /login"})
        with patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.shutil.which", return_value="/opt/homebrew/bin/claude"
        ), patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.subprocess.run", return_value=_completed(0, stdout)
        ):
            with self.assertRaises(ToolError) as ctx:
                self.provider.generate(payload=_payload(), primary_model="", fallback_model="")
        self.assertIn("Not logged in", str(ctx.exception))

    def test_generate_raises_on_nonzero_exit(self) -> None:
        with patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.shutil.which", return_value="/opt/homebrew/bin/claude"
        ), patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.subprocess.run",
            return_value=_completed(1, "", "boom"),
        ):
            with self.assertRaises(ToolError):
                self.provider.generate(payload=_payload(), primary_model="", fallback_model="")

    def test_generate_raises_when_cli_missing(self) -> None:
        with patch("bpmis_jira_tool.source_code_qa_llm_providers.shutil.which", return_value=None):
            with self.assertRaises(ToolError):
                self.provider.generate(payload=_payload(), primary_model="", fallback_model="")

    def test_generate_raises_on_timeout(self) -> None:
        with patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.shutil.which", return_value="/opt/homebrew/bin/claude"
        ), patch(
            "bpmis_jira_tool.source_code_qa_llm_providers.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="claude", timeout=10),
        ):
            with self.assertRaises(ToolError):
                self.provider.generate(payload=_payload(), primary_model="", fallback_model="")


if __name__ == "__main__":
    unittest.main()
