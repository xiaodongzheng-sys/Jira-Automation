from __future__ import annotations

import unittest

from prd_briefing.text_generation import TextGenerationClient


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


if __name__ == "__main__":
    unittest.main()
