from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.productization_codex import generate_productization_detailed_features_with_local_codex
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
        return type("Result", (), {"payload": {"text": "codex ok"}})()

    def extract_text(self, payload):
        return payload["text"]


class _FakeJsonCodexProvider(_FakeCodexProvider):
    def generate(self, *, payload, primary_model, fallback_model):
        self.calls.append((payload, primary_model, fallback_model))
        return type("Result", (), {"payload": {"text": '{"items":[]}'}})()


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
        with patch.dict(os.environ, {}, clear=True), patch("bpmis_jira_tool.config.find_dotenv", return_value=""), patch(
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

    def test_codex_client_defaults_prd_generation_to_balanced_route(self):
        _FakeCodexProvider.instances.clear()
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"SOURCE_CODE_QA_CODEX_BINARY": "/usr/local/bin/codex"},
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


if __name__ == "__main__":
    unittest.main()
