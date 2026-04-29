import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
import json

from prd_briefing.confluence import IngestedConfluencePage, ParsedSection
from prd_briefing.reviewer import PRD_REVIEW_PROMPT_VERSION, build_prd_review_prompt
from prd_briefing.service import (
    PRDBriefingService,
    build_pm_briefing_blocks,
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
        self.cached_texts = set()

    def synthesize(self, **kwargs):
        return None

    def get_cached_audio_for_text(self, *, owner_key, text, language_code):
        return "audio/cached.mp3" if text in self.cached_texts else None


@dataclass
class FakeConnector:
    page: IngestedConfluencePage
    calls: int = 0

    def ingest_page(self, page_ref, session_id):
        self.calls += 1
        return self.page


class PRDBriefingServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = BriefingStore(Path(self.temp_dir.name))
        self.openai_client = FakeOpenAIClient()
        self.voice_service = FakeVoiceService()
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
            voice_service=self.voice_service,
            walkthrough_prewarm_enabled=False,
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
        self.assertEqual(self.service.confluence.calls, 1)
        self.assertIn("briefing_blocks", payload)
        self.assertTrue(payload["briefing_blocks"])
        self.assertIn("section_indexes", payload["briefing_blocks"][0])

    def test_recreating_same_prd_keeps_previous_session_chunks(self):
        first = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        second = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        first_payload = self.service.get_session_payload(
            session_id=first["session"]["session_id"],
            owner_key="anon:test",
        )
        second_payload = self.service.get_session_payload(
            session_id=second["session"]["session_id"],
            owner_key="anon:test",
        )

        self.assertEqual(len(first_payload["sections"]), 2)
        self.assertEqual(len(second_payload["sections"]), 2)
        self.assertNotEqual(first["session"]["session_id"], second["session"]["session_id"])

    def test_answer_question_uses_prd_context(self):
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        answer = self.service.answer_question(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            question="What is the approval workflow?",
        )

        self.assertIn("根据当前检索到的 PRD 内容", answer["answer_text"])
        self.assertIn("可优先查看", answer["answer_text"])
        self.assertEqual(answer["groundedness"], "grounded")
        self.assertGreaterEqual(len(answer["citations"]), 1)

    def test_prd_review_prompt_contains_review_dimensions_and_prd_sections(self):
        page = self.service.confluence.page

        prompt = build_prd_review_prompt(
            jira_id="AF-123",
            jira_link="https://jira/browse/AF-123",
            prd_url=page.source_url,
            page=page,
        )

        self.assertIn("业务目标一致性", prompt)
        self.assertIn("异常场景补漏", prompt)
        self.assertIn("AF-123", prompt)
        self.assertIn("This PRD introduces approval workflow", prompt)

    def test_prd_review_result_cache_uses_prd_updated_at_and_prompt_version(self):
        saved = self.store.save_prd_review_result(
            owner_key="anon:test",
            jira_id="AF-123",
            jira_link="https://jira/browse/AF-123",
            prd_url="https://example/prd",
            prd_updated_at="2026-04-28T00:00:00Z",
            prompt_version=PRD_REVIEW_PROMPT_VERSION,
            status="completed",
            result_markdown="### Review",
            model_id="codex-cli",
            trace={"session_id": "s1"},
        )

        cached = self.store.get_prd_review_result(
            owner_key="anon:test",
            jira_id="AF-123",
            prd_url="https://example/prd",
            prd_updated_at="2026-04-28T00:00:00Z",
            prompt_version=PRD_REVIEW_PROMPT_VERSION,
        )
        stale = self.store.get_prd_review_result(
            owner_key="anon:test",
            jira_id="AF-123",
            prd_url="https://example/prd",
            prd_updated_at="2026-04-29T00:00:00Z",
            prompt_version=PRD_REVIEW_PROMPT_VERSION,
        )

        self.assertEqual(saved["result_markdown"], "### Review")
        self.assertEqual(cached["trace"]["session_id"], "s1")
        self.assertIsNone(stale)

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

        result, cached = self.service._compose_walkthrough_section(  # noqa: SLF001
            owner_key="anon:prompt-test",
            section=payload["sections"][0],
        )

        self.assertEqual(result, "LLM answer")
        self.assertFalse(cached)
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
        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(self.openai_client.answer_calls, 1)

    def test_pm_briefing_blocks_filter_metadata_and_merge_related_sections(self):
        sections = [
            {"section_path": "1.1 Version Control", "content": "Version v0.1 PIC someone@example.com"},
            {"section_path": "3.5.1 Search Layout", "content": "The page shows search criteria and default fields. User can click Search button."},
            {"section_path": "3.5.2 Detail Fields", "content": "The detail page displays readonly fields and required form values."},
            {"section_path": "3.6.1 Submit Review", "content": "User can submit for review and status changes from Draft to Pending Review."},
            {"section_path": "3.6.2 Reopen", "content": "User can click Reopen and status changes back to Draft."},
        ]

        blocks = build_pm_briefing_blocks(sections)

        self.assertFalse(any(0 in block["section_indexes"] for block in blocks))
        ui_block = next(block for block in blocks if block["title"] == "页面布局和字段规则")
        state_block = next(block for block in blocks if block["title"] == "状态流转和操作动作")
        self.assertEqual(ui_block["section_indexes"], [1, 2])
        self.assertEqual(state_block["section_indexes"], [3, 4])
        self.assertTrue(ui_block["source_refs"])

    def test_pm_briefing_blocks_split_large_related_groups(self):
        sections = [
            {
                "section_path": f"3.8.{index} Report Download",
                "content": "User can download report history and audit export.",
                "html_content": "<table>" + ("<tr><td>Report history</td></tr>" * 20) + "</table>",
            }
            for index in range(1, 7)
        ]

        blocks = build_pm_briefing_blocks(sections)
        reporting_blocks = [block for block in blocks if block["title"].startswith("报表、下载和历史记录")]

        self.assertGreaterEqual(len(reporting_blocks), 2)
        self.assertTrue(all(len(block["section_indexes"]) <= 4 for block in reporting_blocks))
        self.assertEqual(
            [index for block in reporting_blocks for index in block["section_indexes"]],
            list(range(6)),
        )

    def test_narrate_briefing_block_uses_block_payload_cache(self):
        self.openai_client.is_configured = lambda: True
        page = IngestedConfluencePage(
            page_id="456",
            title="Assessment PRD",
            source_url="https://example.atlassian.net/wiki/pages/456",
            updated_at="2026-04-16T10:00:00Z",
            language="en",
            sections=[
                ParsedSection(
                    title="Search Layout",
                    section_path="3.5.1 Search Layout",
                    content="The page shows search criteria and default fields. User can click Search button.",
                    html_content="<p>The page shows search criteria and default fields.</p>",
                    image_refs=[],
                ),
                ParsedSection(
                    title="Detail Fields",
                    section_path="3.5.2 Detail Fields",
                    content="The detail page displays readonly fields and required form values.",
                    html_content="<p>The detail page displays readonly fields and required form values.</p>",
                    image_refs=[],
                ),
            ],
        )
        self.service.confluence = FakeConnector(page)
        payload = self.service.create_session(
            owner_key="anon:block-test",
            page_ref="https://example.atlassian.net/wiki/pages/456",
            mode="walkthrough",
        )
        block_id = payload["briefing_blocks"][0]["block_id"]
        self.openai_client.answer_calls = 0

        first = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:block-test",
            briefing_block_id=block_id,
            include_audio=False,
        )
        second = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:block-test",
            briefing_block_id=block_id,
            include_audio=False,
        )

        self.assertEqual(first["script"], "LLM answer")
        self.assertEqual(first["briefing_block_id"], block_id)
        self.assertEqual(first["section_indexes"], [0, 1])
        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(self.openai_client.answer_calls, 1)

    def test_walkthrough_script_reuses_legacy_cached_entry_from_old_model_id(self):
        self.openai_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        sections = payload["sections"]
        notes = sections[0].get("briefing_notes") or []
        prompt = (
            "You are a product manager briefing a PRD section to software engineers in Mandarin Chinese. "
            "Speak the way PMs normally align requirements with developers during grooming or walkthrough sessions. "
            "Be direct, practical, and structured. First explain the purpose of this section, then the main flow, "
            "then what developers need to build or pay attention to. Call out scope, user actions, system behavior, "
            "validation rules, dependencies, edge cases, and any implementation-sensitive details when present. "
            "Do not sound like a keynote presenter. Do not read the PRD word for word. "
            "Do not mechanically read every field, bullet, or table row. Summarize dense tables into what engineering should understand. "
            "Use spoken PM phrasing that feels normal in a dev sync, for example framing the goal first, then saying what the flow is, "
            "what changes on the page, what gets triggered, what should be validated, and what cases developers need to pay attention to."
        )
        body = (
            f"Section: {sections[0]['section_path']}\n\n"
            f"Presenter summary:\n{sections[0].get('briefing_summary', '')}\n\n"
            f"Presenter notes:\n- " + "\n- ".join(notes) + "\n\n"
            f"Source:\n{sections[0]['content']}\n\n"
        )
        body += (
            "Write a natural spoken script of around 5 to 9 sentences in Mandarin. "
            "The first sentence should explain why this section matters to implementation. "
            "Then explain the intended flow in order. "
            "After that, highlight the key engineering takeaways, such as important rules, triggers, state changes, "
            "input or output expectations, and any edge cases or risks implied by the source. "
            "If the section is mostly UI fields, summarize the pattern and only name the most important fields. "
            "Make it sound like live PM speech rather than written prose. "
            "Natural phrasing is encouraged, such as: "
            "'这一块主要是...', '开发这里重点看...', '实际 flow 是...', '这里需要注意...', "
            "'这个字段很多，但本质上是为了...', '异常情况主要是...'. "
            "Do not force all phrases in every answer, but keep the overall tone close to that style."
        )
        section_payload = json.dumps(
            {
                "section_path": sections[0]["section_path"],
                "briefing_summary": sections[0].get("briefing_summary", ""),
                "briefing_notes": notes,
                "content": sections[0]["content"],
                "prompt": prompt,
                "body": body,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        with self.store.connect() as conn:
            conn.execute(
                """
                delete from briefing_script_cache
                where owner_key = ? and audience = ? and prompt_version = ?
                """,
                ("anon:test", "developer_zh", "v1_openai_only_pm_briefing"),
            )
        self.store.cache_script(
            owner_key="anon:test",
            audience="developer_zh",
            model_id="openai:gpt-4.1-mini|gemini:gemini-2.5-flash",
            prompt_version="v1_openai_only_pm_briefing",
            section_payload=section_payload,
            script="legacy cached script",
        )
        self.openai_client.answer_calls = 0

        result = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            section_index=0,
            include_audio=False,
        )

        self.assertEqual(result["script"], "legacy cached script")
        self.assertTrue(result["cached"])
        self.assertFalse(result["audio_cached"])
        self.assertEqual(self.openai_client.answer_calls, 0)

    def test_get_session_payload_marks_cached_sections(self):
        self.openai_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            section_index=0,
            include_audio=False,
        )

        refreshed = self.service.get_session_payload(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
        )

        self.assertTrue(refreshed["sections"][0]["walkthrough_cached"])
        self.assertFalse(refreshed["sections"][0]["walkthrough_audio_cached"])
        self.assertFalse(refreshed["sections"][1]["walkthrough_cached"])
        self.assertFalse(refreshed["sections"][1]["walkthrough_audio_cached"])

    def test_get_session_payload_marks_cached_audio_sections(self):
        self.openai_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )

        result = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            section_index=0,
            include_audio=False,
        )
        self.voice_service.cached_texts.add(result["script"])

        refreshed = self.service.get_session_payload(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
        )

        self.assertTrue(refreshed["sections"][0]["walkthrough_cached"])
        self.assertTrue(refreshed["sections"][0]["walkthrough_audio_cached"])

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

    def test_developer_overview_does_not_block_on_text_model(self):
        self.openai_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.openai_client.answer_calls = 0

        refreshed = self.service.get_session_payload(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
        )

        self.assertTrue(refreshed["session_overview"]["background_goal"])
        self.assertTrue(refreshed["session_overview"]["implementation_overview"])
        self.assertEqual(self.openai_client.answer_calls, 0)

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
