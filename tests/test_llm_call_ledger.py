from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bpmis_jira_tool.llm_call_ledger import infer_llm_flow, llm_call_ledger_path
from bpmis_jira_tool.source_code_qa_llm_providers import CodexCliBridgeSourceCodeQALLMProvider


class LLMCallLedgerTests(unittest.TestCase):
    def test_infers_known_portal_flows_from_prompt_mode(self):
        self.assertEqual(infer_llm_flow("monthly_report_v4_final"), "monthly_report")
        self.assertEqual(infer_llm_flow("seatalk_7_day_insights_v4"), "seatalk")
        self.assertEqual(infer_llm_flow("productization_detailed_feature_v1"), "productization")
        self.assertEqual(infer_llm_flow("prd_reviewer_delivery_review_v1"), "prd_reviewer")
        self.assertEqual(infer_llm_flow("meeting_recorder_minutes_codex"), "meeting_recorder")
        self.assertEqual(infer_llm_flow("codex_investigation_brief_v5"), "source_code_qa")

    def test_codex_provider_records_success_without_prompt_text(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            workspace = Path(temp_dir) / "workspace"
            provider = CodexCliBridgeSourceCodeQALLMProvider(workspace_root=workspace, codex_binary="codex")
            provider.ready = lambda: True  # type: ignore[method-assign]

            def fake_run(command, **kwargs):
                output_path = Path(command[command.index("--output-last-message") + 1])
                output_path.write_text("ledger answer", encoding="utf-8")
                return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

            with patch.dict(os.environ, {"TEAM_PORTAL_DATA_DIR": str(data_root)}, clear=True), patch(
                "bpmis_jira_tool.source_code_qa_llm_providers.subprocess.run",
                side_effect=fake_run,
            ):
                result = provider.generate(
                    payload={
                        "codex_prompt_mode": "monthly_report_v4_final",
                        "systemInstruction": {"parts": [{"text": "system"}]},
                        "contents": [{"parts": [{"text": "do not store this prompt text"}]}],
                        "_codex_reasoning_effort": "medium",
                        "_codex_estimated_prompt_tokens": 123,
                        "_llm_ledger_flow": "monthly_report",
                        "_llm_ledger_route": "balanced",
                    },
                    primary_model="gpt-5.4",
                    fallback_model="gpt-5.4",
                )

                ledger_path = llm_call_ledger_path()

            rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(result.model, "gpt-5.4")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["flow"], "monthly_report")
            self.assertEqual(rows[0]["route"], "balanced")
            self.assertEqual(rows[0]["model_id"], "gpt-5.4")
            self.assertEqual(rows[0]["reasoning_effort"], "medium")
            self.assertEqual(rows[0]["status"], "ok")
            self.assertEqual(rows[0]["estimated_prompt_tokens"], 123)
            self.assertNotIn("do not store this prompt text", ledger_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
