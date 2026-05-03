from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from prd_briefing.text_generation import CodexTextGenerationClient, TextGenerationClient


class _FakeOpenAIProvider:
    def __init__(self, *, configured: bool, model_id: str, response: str | None = None, error: Exception | None = None):
        self._configured = configured
        self.text_model = model_id
        self.response = response
        self.error = error
        self.calls = 0

    def is_configured(self):
        return self._configured

    def create_answer(self, *, system_prompt: str, user_prompt: str):
        self.calls += 1
        if self.error:
            raise self.error
        return self.response


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


class TextGenerationClientTests(unittest.TestCase):
    def test_uses_openai_when_configured(self):
        openai = _FakeOpenAIProvider(configured=True, model_id="gpt-4.1-mini", response="openai ok")
        client = TextGenerationClient(primary_openai=openai)

        result = client.create_answer(system_prompt="sys", user_prompt="user")

        self.assertEqual(result, "openai ok")
        self.assertEqual(openai.calls, 1)

    def test_reports_not_configured_when_openai_missing(self):
        openai = _FakeOpenAIProvider(configured=False, model_id="gpt-4.1-mini")
        client = TextGenerationClient(primary_openai=openai)

        with self.assertRaisesRegex(RuntimeError, "OPENAI_API_KEY is not configured"):
            client.create_answer(system_prompt="sys", user_prompt="user")

    def test_model_id_reflects_openai_only(self):
        openai = _FakeOpenAIProvider(configured=True, model_id="gpt-4.1-mini", response="openai ok")
        client = TextGenerationClient(primary_openai=openai)

        self.assertEqual(client.model_id, "openai:gpt-4.1-mini")

    def test_model_id_reports_openai_none_when_unconfigured(self):
        openai = _FakeOpenAIProvider(configured=False, model_id="gpt-4.1-mini")
        client = TextGenerationClient(primary_openai=openai)

        self.assertFalse(client.is_configured())
        self.assertEqual(client.model_id, "openai:none")

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


if __name__ == "__main__":
    unittest.main()
