from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.productization_codex import (
    clean_codex_productization_detailed_feature,
    format_productization_description_text,
    generate_productization_detailed_features_with_local_codex,
    parse_codex_json_object,
)
from prd_briefing.text_generation import CodexTextGenerationClient


class _FakeCodexProvider:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        self.ready_value = True
        _FakeCodexProvider.instances.append(self)

    def ready(self):
        return self.ready_value

    def generate(self, *, payload, primary_model, fallback_model):
        self.calls.append((payload, primary_model, fallback_model))
        return type("Result", (), {"payload": {"text": "codex ok"}, "model": primary_model})()

    def extract_text(self, payload):
        return payload["text"]


class _FakeJsonCodexProvider(_FakeCodexProvider):
    def generate(self, *, payload, primary_model, fallback_model):
        self.calls.append((payload, primary_model, fallback_model))
        return type("Result", (), {"payload": {"text": '{"items":[]}'}, "model": primary_model})()


class TextGenerationClientTests(unittest.TestCase):
    def test_codex_client_uses_provider_payload_and_model_defaults(self):
        _FakeCodexProvider.instances.clear()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "PRD_BRIEFING_CODEX_MODEL": "gpt-5.5",
                "SOURCE_CODE_QA_CODEX_BINARY": "/usr/local/bin/codex",
            },
            clear=False,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "prd_briefing.text_generation.CodexCliBridgeSourceCodeQALLMProvider",
            _FakeCodexProvider,
        ):
            settings = Settings.from_env()
            client = CodexTextGenerationClient(settings=settings, workspace_root=Path(temp_dir), prompt_mode="")
            answer = client.create_answer(system_prompt="sys", user_prompt="user")

        provider = _FakeCodexProvider.instances[0]
        payload, primary_model, fallback_model = provider.calls[0]
        self.assertTrue(client.is_configured())
        self.assertEqual(client.model_id, "codex:gpt-5.5")
        self.assertEqual(answer, "codex ok")
        self.assertEqual(primary_model, "gpt-5.5")
        self.assertEqual(fallback_model, "gpt-5.5")
        self.assertEqual(payload["systemInstruction"]["parts"][0]["text"], "sys")
        self.assertEqual(payload["contents"][0]["parts"][0]["text"], "user")
        self.assertEqual(payload["codex_prompt_mode"], "prd_briefing_codex")
        self.assertEqual(provider.kwargs["codex_binary"], "/usr/local/bin/codex")

    def test_productization_codex_defaults_to_cheap_route(self):
        _FakeCodexProvider.instances.clear()
        with patch.dict(os.environ, {"SOURCE_CODE_QA_CODEX_MODEL": "gpt-5.5"}, clear=True), patch(
            "bpmis_jira_tool.config.find_dotenv",
            return_value="",
        ), patch(
            "bpmis_jira_tool.productization_codex.CodexCliBridgeSourceCodeQALLMProvider",
            _FakeJsonCodexProvider,
        ):
            settings = Settings.from_env()
            items = generate_productization_detailed_features_with_local_codex(
                [{"jira_ticket_number": "ABC-1", "jira_description": "Add retry"}],
                settings=settings,
            )

        payload, primary_model, fallback_model = _FakeJsonCodexProvider.instances[0].calls[0]
        self.assertEqual(primary_model, "gpt-5.4-mini")
        self.assertEqual(fallback_model, "gpt-5.4-mini")
        self.assertEqual(payload["codex_prompt_mode"], "productization_detailed_feature_v1")
        self.assertEqual(items, [])

    def test_productization_codex_parses_and_cleans_llm_payloads(self):
        self.assertEqual(parse_codex_json_object('```json\n{"items": []}\n```'), {"items": []})
        self.assertEqual(parse_codex_json_object('prefix {"items": [{"jira_ticket_number": "AF-1"}]} suffix')["items"][0]["jira_ticket_number"], "AF-1")
        with self.assertRaisesRegex(ToolError, "unreadable"):
            parse_codex_json_object("no-json-here")
        with self.assertRaisesRegex(ToolError, "unreadable"):
            parse_codex_json_object("{bad json}")
        with self.assertRaisesRegex(ToolError, "invalid"):
            parse_codex_json_object('["not", "object"]')

        self.assertEqual(format_productization_description_text("<p>Hello&nbsp;team</p><br>Next"), "Hello team\nNext")
        self.assertEqual(format_productization_description_text(""), "-")
        self.assertEqual(clean_codex_productization_detailed_feature("```json\nDone\n```"), "Done")
        self.assertEqual(clean_codex_productization_detailed_feature("   "), "-")

    def test_productization_codex_rejects_invalid_items_shape_and_filters_non_objects(self):
        class InvalidItemsProvider(_FakeCodexProvider):
            def generate(self, *, payload, primary_model, fallback_model):
                self.calls.append((payload, primary_model, fallback_model))
                return type("Result", (), {"payload": {"text": '{"items": {}}'}, "model": primary_model})()

        class MixedItemsProvider(_FakeCodexProvider):
            def generate(self, *, payload, primary_model, fallback_model):
                self.calls.append((payload, primary_model, fallback_model))
                return type(
                    "Result",
                    (),
                    {"payload": {"text": '{"items": ["bad", {"jira_ticket_number": " AF-1 ", "detailed_feature": "<b>Feature</b>"}]}'}, "model": primary_model},
                )()

        with patch.dict(os.environ, {}, clear=True), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "bpmis_jira_tool.productization_codex.CodexCliBridgeSourceCodeQALLMProvider",
            InvalidItemsProvider,
        ):
            with self.assertRaisesRegex(ToolError, "invalid"):
                generate_productization_detailed_features_with_local_codex([], settings=Settings.from_env())

        with patch.dict(os.environ, {}, clear=True), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "bpmis_jira_tool.productization_codex.CodexCliBridgeSourceCodeQALLMProvider",
            MixedItemsProvider,
        ):
            items = generate_productization_detailed_features_with_local_codex([], settings=Settings.from_env())

        self.assertEqual(items, [{"jira_ticket_number": "AF-1", "detailed_feature": "Feature"}])

    def test_codex_client_defaults_prd_generation_to_balanced_route(self):
        _FakeCodexProvider.instances.clear()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {
                "SOURCE_CODE_QA_CODEX_BINARY": "/usr/local/bin/codex",
                "SOURCE_CODE_QA_CODEX_MODEL": "gpt-5.5",
            },
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "prd_briefing.text_generation.CodexCliBridgeSourceCodeQALLMProvider",
            _FakeCodexProvider,
        ):
            settings = Settings.from_env()
            client = CodexTextGenerationClient(settings=settings, workspace_root=Path(temp_dir), prompt_mode="")
            client.create_answer(system_prompt="sys", user_prompt="user")

        payload, primary_model, fallback_model = _FakeCodexProvider.instances[0].calls[0]
        self.assertEqual(client.model_id, "codex:gpt-5.4")
        self.assertEqual(primary_model, "gpt-5.4")
        self.assertEqual(fallback_model, "gpt-5.4")
        self.assertEqual(payload["codex_prompt_mode"], "prd_briefing_codex")

    def test_codex_client_marks_meeting_recorder_ledger_flow(self):
        _FakeCodexProvider.instances.clear()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {},
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "prd_briefing.text_generation.CodexCliBridgeSourceCodeQALLMProvider",
            _FakeCodexProvider,
        ):
            settings = Settings.from_env()
            client = CodexTextGenerationClient(
                settings=settings,
                workspace_root=Path(temp_dir),
                prompt_mode="meeting_recorder_minutes_codex",
            )
            client.create_answer(system_prompt="sys", user_prompt="user")

        payload, _, _ = _FakeCodexProvider.instances[0].calls[0]
        self.assertEqual(payload["_llm_ledger_flow"], "meeting_recorder")
        self.assertEqual(payload["_llm_ledger_route"], "balanced")

    def test_prd_reviewer_defaults_to_deep_route_without_source_code_qa_fallback(self):
        from prd_briefing.reviewer import _generate_with_codex

        _FakeCodexProvider.instances.clear()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"SOURCE_CODE_QA_CODEX_MODEL": "gpt-5.4-mini"},
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "prd_briefing.reviewer.CodexCliBridgeSourceCodeQALLMProvider",
            _FakeCodexProvider,
        ):
            result = _generate_with_codex(
                prompt="review",
                settings=Settings.from_env(),
                workspace_root=Path(temp_dir),
                system_text="sys",
                prompt_mode="prd_reviewer_test",
            )

        payload, primary_model, fallback_model = _FakeCodexProvider.instances[0].calls[0]
        self.assertEqual(result["model_id"], "gpt-5.6")
        self.assertEqual(primary_model, "gpt-5.6")
        self.assertEqual(fallback_model, "gpt-5.6")
        self.assertEqual(payload["codex_prompt_mode"], "prd_reviewer_test")


class _FakeClaudeProvider:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        _FakeClaudeProvider.instances.append(self)

    def ready(self):
        return True

    def generate(self, *, payload, primary_model, fallback_model):
        self.calls.append((payload, primary_model, fallback_model))
        return type("Result", (), {"payload": {"text": "claude ok"}, "model": primary_model})()

    def extract_text(self, payload):
        return payload["text"]


class _UnavailableClaudeProvider(_FakeClaudeProvider):
    def generate(self, *, payload, primary_model, fallback_model):
        raise ToolError("Claude Code CLI is unavailable.")


class ClaudeFirstTextGenerationClientTests(unittest.TestCase):
    def _make_client(self, *, claude_provider_cls, temp_dir):
        from prd_briefing.text_generation import ClaudeFirstTextGenerationClient

        return ClaudeFirstTextGenerationClient(
            settings=Settings.from_env(),
            workspace_root=Path(temp_dir),
            prompt_mode="meeting_recorder_minutes_codex",
            claude_model="claude-opus-4-8",
        ), ClaudeFirstTextGenerationClient

    def test_prefers_claude_when_available(self):
        _FakeCodexProvider.instances.clear()
        _FakeClaudeProvider.instances.clear()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ, {}, clear=True
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "prd_briefing.text_generation.CodexCliBridgeSourceCodeQALLMProvider",
            _FakeCodexProvider,
        ), patch(
            "prd_briefing.text_generation.ClaudeCliBridgeSourceCodeQALLMProvider",
            _FakeClaudeProvider,
        ):
            client, _ = self._make_client(claude_provider_cls=_FakeClaudeProvider, temp_dir=temp_dir)
            answer = client.create_answer(system_prompt="sys", user_prompt="user")

        self.assertEqual(answer, "claude ok")
        self.assertEqual(client.model_id, "claude:claude-opus-4-8")
        # Claude was called; Codex fallback untouched.
        self.assertEqual(len(_FakeClaudeProvider.instances[0].calls), 1)
        self.assertEqual(_FakeCodexProvider.instances[0].calls, [])
        payload, primary_model, _ = _FakeClaudeProvider.instances[0].calls[0]
        self.assertEqual(primary_model, "claude-opus-4-8")
        self.assertEqual(payload["systemInstruction"]["parts"][0]["text"], "sys")
        self.assertEqual(payload["_llm_ledger_flow"], "meeting_recorder")

    def test_falls_back_to_codex_on_claude_tool_error(self):
        _FakeCodexProvider.instances.clear()
        _UnavailableClaudeProvider.instances.clear()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ, {}, clear=True
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
            "prd_briefing.text_generation.CodexCliBridgeSourceCodeQALLMProvider",
            _FakeCodexProvider,
        ), patch(
            "prd_briefing.text_generation.ClaudeCliBridgeSourceCodeQALLMProvider",
            _UnavailableClaudeProvider,
        ):
            client, _ = self._make_client(claude_provider_cls=_UnavailableClaudeProvider, temp_dir=temp_dir)
            answer = client.create_answer(system_prompt="sys", user_prompt="user")

        self.assertEqual(answer, "codex ok")
        # Codex received the same payload after Claude failed.
        self.assertEqual(len(_FakeCodexProvider.instances[0].calls), 1)
        payload, _, _ = _FakeCodexProvider.instances[0].calls[0]
        self.assertEqual(payload["contents"][0]["parts"][0]["text"], "user")


if __name__ == "__main__":
    unittest.main()
