import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from prd_briefing.confluence import IngestedConfluencePage, ParsedSection
from prd_briefing.service import (
    PRDBriefingService,
    build_heuristic_session_overview,
    overview_is_low_signal,
    parse_session_overview,
    select_sections_for_overview,
)
from prd_briefing.storage import BriefingStore


class FakeOpenAIClient:
    def __init__(self):
        self.last_system_prompt = None
        self.last_user_prompt = None
        self.answer_calls = 0
        self.chat_model = "gpt-4.1-mini"

    def is_configured(self):
        return False

    def embed_texts(self, texts):
        raise AssertionError("Embeddings should not be called in this test.")

    def create_answer(self, system_prompt, user_prompt):
        self.last_system_prompt = system_prompt
        self.last_user_prompt = user_prompt
        self.answer_calls += 1
        return "LLM answer"


class FakeVoiceService:
    def __init__(self):
        self.transcribed = "what changed"

    def synthesize(self, **kwargs):
        return None

    def transcribe(self, audio_path: Path):
        return self.transcribed

    def enroll(self, **kwargs):
        return {"provider": "stored_samples", "consent_status": "granted", "sample_language": "en"}


@dataclass
class FakeConnector:
    page: IngestedConfluencePage

    def ingest_page(self, page_ref, session_id):
        return self.page


class PRDBriefingServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = BriefingStore(Path(self.temp_dir.name))
        self.openai_client = FakeOpenAIClient()
        page = IngestedConfluencePage(
            page_id="123",
            title="Payments PRD",
            source_url="https://example.atlassian.net/wiki/pages/123",
            updated_at="2026-04-15T10:00:00Z",
            language="en",
            sections=[
                ParsedSection(
                    title="Overview",
                    section_path="Overview",
                    content="This PRD introduces approval workflow and reviewer assignment.",
                    html_content="<p>This PRD introduces approval workflow and reviewer assignment.</p>",
                    image_refs=[],
                ),
                ParsedSection(
                    title="Rollout",
                    section_path="Rollout",
                    content="Rollout will happen in three phases with developer handoff notes.",
                    html_content="<p>Rollout will happen in three phases with developer handoff notes.</p>",
                    image_refs=[],
                ),
            ],
        )
        self.service = PRDBriefingService(
            store=self.store,
            confluence=FakeConnector(page),
            openai_client=self.openai_client,
            voice_service=FakeVoiceService(),
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_create_session_persists_sections(self):
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        self.assertEqual(payload["session"]["title"], "Payments PRD")
        self.assertEqual(len(payload["sections"]), 2)
        self.assertTrue(payload["session_overview"]["overview"])
        self.assertIn("unclear_rules", payload["session_overview"])
        self.assertIn("missing_edge_cases", payload["session_overview"])
        self.assertIn("unclear_ownership", payload["session_overview"])
        self.assertEqual(payload["sections"][0]["section_path"], "Overview")
        self.assertIn("<p>", payload["sections"][0]["html_content"])
        self.assertTrue(payload["sections"][0]["briefing_notes"])
        self.assertTrue(payload["sections"][0]["briefing_summary"])

    def test_answer_question_uses_prd_and_kb_context(self):
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.service.upload_kb_document(
            owner_key="anon:test",
            filename="kb.txt",
            content=b"Approval workflow requires reviewer assignment and audit notes.",
        )

        answer = self.service.answer_question(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            question="What is the approval workflow?",
        )

        self.assertIn("根据目前可用的来源内容", answer["answer_text"])
        self.assertEqual(answer["groundedness"], "grounded")
        self.assertGreaterEqual(len(answer["citations"]), 1)

    def test_unsupported_question_is_declined(self):
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        answer = self.service.answer_question(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            question="What is the pricing model in Brazil?",
        )

        self.assertEqual(answer["groundedness"], "unsupported")
        self.assertIn("找到这个答案", answer["answer_text"])

    def test_developer_walkthrough_prompt_is_engineering_focused(self):
        self.openai_client.is_configured = lambda: True

        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.openai_client.answer_calls = 0
        self.openai_client.last_system_prompt = None
        self.openai_client.last_user_prompt = None

        result = self.service._compose_walkthrough_section(  # noqa: SLF001
            owner_key="anon:prompt-test",
            section=payload["sections"][0],
        )

        self.assertEqual(result, "LLM answer")
        self.assertIn("software engineers", self.openai_client.last_system_prompt)
        self.assertIn("validation rules", self.openai_client.last_system_prompt)
        self.assertIn("implementation", self.openai_client.last_user_prompt)
        self.assertIn("这一块主要是", self.openai_client.last_user_prompt)

    def test_walkthrough_script_is_cached_after_first_openai_call(self):
        self.openai_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.openai_client.answer_calls = 0
        self.openai_client.last_system_prompt = None
        self.openai_client.last_user_prompt = None

        first = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            section_index=0,
            include_audio=False,
        )
        second = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            section_index=0,
            include_audio=False,
        )

        self.assertEqual(first["script"], "LLM answer")
        self.assertEqual(second["script"], "LLM answer")
        self.assertEqual(self.openai_client.answer_calls, 0)

    def test_walkthrough_script_requires_text_provider(self):
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        with self.assertRaisesRegex(RuntimeError, "configured OpenAI text model"):
            self.service.narrate_section(
                session_id=payload["session"]["session_id"],
                owner_key="anon:test",
                section_index=0,
                include_audio=False,
            )

    def test_session_overview_filters_metadata_noise(self):
        parsed = parse_session_overview(
            """
            {
              "overview": "这是一个总览",
              "scope": ["Version: v0.1", "Add New Assessment 页面和提交流程"],
              "impacted_modules": ["Date: 24 Nov 2025", "搜索区和详情区"],
              "developer_focus": ["PIC: test@example.com", "状态切换和自动回填"],
              "frontend_focus": [],
              "backend_focus": [],
              "risks": [],
              "unclear_rules": [],
              "missing_edge_cases": [],
              "unclear_ownership": [],
              "open_questions": []
            }
            """
        )
        self.assertEqual(parsed["scope"], ["Add New Assessment 页面和提交流程"])
        self.assertEqual(parsed["impacted_modules"], ["搜索区和详情区"])
        self.assertEqual(parsed["developer_focus"], ["状态切换和自动回填"])

    def test_session_overview_prioritizes_feature_sections(self):
        ranked = select_sections_for_overview([
            {"section_path": "1.2 People Involved", "content": "Role Regional Requester PIC someone@example.com"},
            {"section_path": "2.1 Background", "content": "Background and context"},
            {"section_path": "3.5.2 Add New Assessment", "content": "User can click Add New Assessment button and system auto populate Event"},
            {"section_path": "3.6.8 Reopen SSA", "content": "User can click Reopen and status changes to Draft"},
        ])
        self.assertEqual(ranked[0]["section_path"], "3.5.2 Add New Assessment")
        self.assertIn(ranked[1]["section_path"], {"3.6.8 Reopen SSA", "3.5.2 Add New Assessment"})

    def test_heuristic_overview_extracts_implementation_details(self):
        overview = build_heuristic_session_overview(
            "Payments PRD",
            [
                {"section_path": "1.2 People Involved", "content": "Version v0.1 PIC someone@example.com"},
                {"section_path": "3.5.2 Add New Assessment", "content": "User can click Add New Assessment button and system auto populate Event. By default only seven search criteria are shown."},
                {"section_path": "3.6.8 Reopen SSA", "content": "User can click Reopen and status changes to Draft. Reopen Date is mandatory when Reopen Date is filled."},
            ],
        )
        self.assertNotIn("Version", "".join(overview["developer_focus"]))
        self.assertTrue(any("Add New Assessment" in item or "auto populate" in item or "seven search criteria" in item for item in overview["developer_focus"]))

    def test_developer_overview_uses_single_summary_shape(self):
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.assertTrue(payload["session_overview"]["overview"])
        self.assertIn("background_goal", payload["session_overview"])
        self.assertIn("implementation_overview", payload["session_overview"])
        self.assertEqual(payload["session_overview"]["scope"], [])
        self.assertEqual(payload["session_overview"]["developer_focus"], [])

    def test_low_signal_overview_is_detected(self):
        overview = {
            "overview": "这个 PRD 主要围绕核心业务流程、页面操作和规则约束展开。",
            "scope": ["1.1 Version Control"],
            "impacted_modules": ["流程和页面跳转"],
            "developer_focus": ["Version: v0.1", "Date: 24 Nov 2025"],
            "frontend_focus": ["页面展示、字段显隐、按钮状态和交互顺序要先对齐。"],
            "backend_focus": ["重点确认状态流转、默认值、系统自动回填和接口出参约束。"],
            "risks": ["部分规则和边界情况可能分散在多个 section 里，开发实现前需要先对齐。"],
            "unclear_rules": [],
            "missing_edge_cases": [],
            "unclear_ownership": [],
            "open_questions": [],
        }
        self.assertTrue(overview_is_low_signal(overview))

    def test_create_session_uses_heuristic_when_openai_overview_is_low_signal(self):
        self.openai_client.is_configured = lambda: True
        self.openai_client.create_answer = lambda *args, **kwargs: """
        {
          "overview": "这个 PRD 主要围绕核心业务流程、页面操作和规则约束展开。",
          "scope": ["1.1 Version Control", "1.2 People Involved"],
          "impacted_modules": ["流程和页面跳转"],
          "developer_focus": ["Version: v0.1", "Date: 24 Nov 2025"],
          "frontend_focus": ["页面展示、字段显隐、按钮状态和交互顺序要先对齐。"],
          "backend_focus": ["重点确认状态流转、默认值、系统自动回填和接口出参约束。"],
          "risks": ["部分规则和边界情况可能分散在多个 section 里，开发实现前需要先对齐。"],
          "unclear_rules": [],
          "missing_edge_cases": [],
          "unclear_ownership": [],
          "open_questions": []
        }
        """

        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        self.assertNotIn("Version", "".join(payload["session_overview"]["developer_focus"]))
        self.assertNotIn("Version Control", "".join(payload["session_overview"]["scope"]))
        self.assertTrue(payload["session_overview"]["overview"])


if __name__ == "__main__":
    unittest.main()
