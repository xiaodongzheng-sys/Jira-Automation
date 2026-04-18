from __future__ import annotations

import unittest

from prd_briefing.text_generation import TextGenerationClient


class _FakeProvider:
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
    def test_falls_back_to_gemini_when_openai_fails(self):
        openai = _FakeProvider(configured=True, model_id="gpt-4.1-mini", error=RuntimeError("429"))
        gemini = _FakeProvider(configured=True, model_id="gemini-2.5-flash", response="gemini ok")
        client = TextGenerationClient(primary_openai=openai, secondary_gemini=gemini)

        result = client.create_answer(system_prompt="sys", user_prompt="user")

        self.assertEqual(result, "gemini ok")
        self.assertEqual(openai.calls, 1)
        self.assertEqual(gemini.calls, 1)

    def test_prefers_openai_when_it_succeeds(self):
        openai = _FakeProvider(configured=True, model_id="gpt-4.1-mini", response="openai ok")
        gemini = _FakeProvider(configured=True, model_id="gemini-2.5-flash", response="gemini ok")
        client = TextGenerationClient(primary_openai=openai, secondary_gemini=gemini)

        result = client.create_answer(system_prompt="sys", user_prompt="user")

        self.assertEqual(result, "openai ok")
        self.assertEqual(openai.calls, 1)
        self.assertEqual(gemini.calls, 0)

    def test_prefers_gemini_when_priority_is_gemini_first(self):
        openai = _FakeProvider(configured=True, model_id="gpt-4.1-mini", response="openai ok")
        gemini = _FakeProvider(configured=True, model_id="gemini-2.5-flash", response="gemini ok")
        client = TextGenerationClient(
            primary_openai=openai,
            secondary_gemini=gemini,
            priority="gemini_first",
        )

        result = client.create_answer(system_prompt="sys", user_prompt="user")

        self.assertEqual(result, "gemini ok")
        self.assertEqual(openai.calls, 0)
        self.assertEqual(gemini.calls, 1)


if __name__ == "__main__":
    unittest.main()
