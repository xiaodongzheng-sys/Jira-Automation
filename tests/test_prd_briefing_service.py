import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch
import json

from bpmis_jira_tool.config import Settings

from prd_briefing.confluence import IngestedConfluencePage, ParsedSection
from prd_briefing.reviewer import (
    PRD_BRIEFING_REVIEW_CACHE_KEY,
    PRD_REVIEW_PROMPT_VERSION,
    PRD_URL_SUMMARY_CACHE_KEY,
    PRDBriefingReviewRequest,
    PRDReviewService,
    build_prd_review_prompt,
    build_prd_summary_prompt,
    prd_briefing_review_prompt_version,
    prd_summary_prompt_version,
)
from prd_briefing.service import (
    PRDBriefingService,
    WALKTHROUGH_SCRIPT_PROMPT_VERSION,
    VoiceService,
    attach_presentation_media,
    build_pm_briefing_blocks,
    build_heuristic_session_overview,
    overview_is_low_signal,
    optimize_tts_text,
    parse_presentation_chunks,
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
        self.model_id = "fake:gpt-4.1-mini"
        self.answer_response = "LLM answer"

    def is_configured(self):
        return False

    def embed_texts(self, texts):
        raise AssertionError("Embeddings should not be called in this test.")

    def create_answer(self, system_prompt, user_prompt):
        self.last_system_prompt = system_prompt
        self.last_user_prompt = user_prompt
        self.answer_calls += 1
        return self.answer_response


class FakeVoiceService:
    def __init__(self):
        self.cached_texts = set()
        self.synthesize_calls = []

    def synthesize(self, **kwargs):
        self.synthesize_calls.append(kwargs)
        return None

    def get_cached_audio_for_text(self, *, owner_key, text, language_code):
        return "audio/cached.mp3" if text in self.cached_texts else None

    def synthesize_presentation_chunk(self, **kwargs):
        chunk = kwargs["chunk"]
        self.synthesize_calls.append(kwargs)
        return {
            "id": chunk["id"],
            "title": chunk["title"],
            "content": chunk["content"],
            "audioUrl": "/prd-briefing/assets/audio/test.mp3",
            "duration": 1.5,
            "timestamps": [{"sentence": chunk["content"], "start": 0, "end": 1.5}],
            "imageUrls": chunk.get("imageUrls") or [],
            "media": chunk.get("media") or {"type": "none", "content": ""},
            "cacheKey": kwargs.get("presentation_cache_key") or "",
        }


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
            version_number="5",
            media_dict={
                "MEDIA_ID_1": {
                    "type": "table",
                    "content": "<table><tr><th>Field</th></tr><tr><td>Status</td></tr></table>",
                }
            },
            presentation_source_text="## Section 1: Overview\nThis PRD introduces approval workflow.\n[MEDIA_ID_1]",
        )
        self.service = PRDBriefingService(
            store=self.store,
            confluence=FakeConnector(page),
            openai_client=self.openai_client,
            voice_service=self.voice_service,
            walkthrough_prewarm_enabled=False,
        )

    def test_parse_presentation_chunks_extracts_json_array(self):
        chunks = parse_presentation_chunks(
            'Here is the JSON:\n```json\n[{"id":"chunk-1","title":"开场","content":"研发先看主流程。"}]\n```'
        )

        self.assertEqual(chunks[0]["id"], "chunk-1")
        self.assertEqual(chunks[0]["title"], "开场")

    def test_parse_presentation_chunks_rejects_invalid_json(self):
        with self.assertRaises(ValueError):
            parse_presentation_chunks('{"id":"chunk-1"}')

    def test_presentation_cache_hit_reuses_outline_without_second_llm_call(self):
        self.openai_client.is_configured = lambda: True
        self.openai_client.answer_response = json.dumps([
            {
                "id": "chunk-1",
                "title": "主流程",
                "content": "这一段说明审批主流程。",
                "media_ref": "MEDIA_ID_1",
            }
        ])

        first = self.service.process_prd_for_presentation(
            owner_key="anon:presentation",
            page_ref="https://example.atlassian.net/wiki/pages/123",
        )
        calls_after_first = self.openai_client.answer_calls
        second = self.service.process_prd_for_presentation(
            owner_key="anon:presentation",
            page_ref="https://example.atlassian.net/wiki/pages/123",
        )

        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(calls_after_first, 1)
        self.assertEqual(self.openai_client.answer_calls, 1)
        self.assertEqual(second["chunks"][0]["media"]["type"], "table")
        self.assertEqual(second["session"]["version_number"], "5")

    def test_presentation_cache_misses_when_page_version_changes(self):
        self.openai_client.is_configured = lambda: True
        self.openai_client.answer_response = json.dumps([
            {"id": "chunk-1", "title": "主流程", "content": "这一段说明审批主流程。"}
        ])

        self.service.process_prd_for_presentation(
            owner_key="anon:presentation",
            page_ref="https://example.atlassian.net/wiki/pages/123",
        )
        self.service.confluence.page.version_number = "6"
        second = self.service.process_prd_for_presentation(
            owner_key="anon:presentation",
            page_ref="https://example.atlassian.net/wiki/pages/123",
        )

        self.assertFalse(second["cached"])
        self.assertEqual(self.openai_client.answer_calls, 2)

    def test_media_ref_restores_image_table_or_none(self):
        chunks = attach_presentation_media(
            [
                {"id": "chunk-1", "title": "图", "content": "看图。", "media_ref": "MEDIA_ID_1", "imageUrls": []},
                {"id": "chunk-2", "title": "表", "content": "看表。", "media_ref": "MEDIA_ID_2", "imageUrls": []},
                {"id": "chunk-3", "title": "无", "content": "无媒体。", "media_ref": "MEDIA_ID_99", "imageUrls": []},
            ],
            {
                "MEDIA_ID_1": {"type": "image", "content": "/prd-briefing/image-proxy?src=x"},
                "MEDIA_ID_2": {"type": "table", "content": "<table><tr><th>A</th></tr></table>"},
            },
        )

        self.assertEqual(chunks[0]["media"]["type"], "image")
        self.assertEqual(chunks[0]["imageUrls"], ["/prd-briefing/image-proxy?src=x"])
        self.assertEqual(chunks[1]["media"]["type"], "table")
        self.assertEqual(chunks[2]["media"], {"type": "none", "content": ""})

    def test_generate_audio_preserves_chunk_media_and_uses_version_cache_key(self):
        self.openai_client.is_configured = lambda: True
        self.openai_client.answer_response = json.dumps([
            {
                "id": "chunk-1",
                "title": "主流程",
                "content": "这一段说明审批主流程。",
                "media_ref": "MEDIA_ID_1",
            }
        ])
        payload = self.service.process_prd_for_presentation(
            owner_key="anon:presentation",
            page_ref="https://example.atlassian.net/wiki/pages/123",
        )
        edited_chunk = dict(payload["chunks"][0])
        edited_chunk["content"] = "这一段编辑后只需要重新生成本段音频。"

        result = self.service.generate_presentation_audio(
            owner_key="anon:presentation",
            session_id=payload["session"]["session_id"],
            chunk=edited_chunk,
        )

        self.assertEqual(result["chunk"]["media"]["type"], "table")
        self.assertEqual(self.voice_service.synthesize_calls[-1]["presentation_cache_key"], "123_5")

    def test_presentation_audio_cache_is_scoped_by_page_version_and_chunk(self):
        voice = VoiceService(
            store=self.store,
            openai_client=self.openai_client,
            tts_provider="edge",
            edge_mandarin_voice="zh-CN-XiaozhenNeural",
            edge_english_voice="en-US-JennyNeural",
            edge_rate="-12%",
            openai_mandarin_voice="alloy",
            openai_voice_speed=1.0,
            openai_custom_voice_enabled=False,
            openai_tts_fallback_enabled=False,
            elevenlabs_api_key=None,
            elevenlabs_mandarin_model_id="eleven_multilingual_v2",
            elevenlabs_mandarin_voice_id=None,
        )
        calls = []
        voice._synthesize_with_edge_tts_with_boundaries = lambda **kwargs: (calls.append(kwargs) or (b"mp3", []))
        chunk = {
            "id": "chunk-1",
            "title": "主流程",
            "content": "同一段文本用于验证缓存隔离。",
            "media": {"type": "none", "content": ""},
        }

        first = voice.synthesize_presentation_chunk(
            session_id="session-1",
            owner_key="anon:presentation-audio",
            chunk=chunk,
            language_code="zh",
            presentation_cache_key="123_5",
        )
        second = voice.synthesize_presentation_chunk(
            session_id="session-2",
            owner_key="anon:presentation-audio",
            chunk=chunk,
            language_code="zh",
            presentation_cache_key="123_6",
        )
        third = voice.synthesize_presentation_chunk(
            session_id="session-3",
            owner_key="anon:presentation-audio",
            chunk=chunk,
            language_code="zh",
            presentation_cache_key="123_5",
        )

        self.assertEqual(len(calls), 2)
        self.assertIn("audio/123_5/chunk-1-", first["audioUrl"])
        self.assertIn("audio/123_6/chunk-1-", second["audioUrl"])
        self.assertEqual(third["audioUrl"], first["audioUrl"])

    def _settings(self) -> Settings:
        return Settings(
            flask_secret_key="secret",
            google_oauth_client_secret_file=Path(self.temp_dir.name) / "client.json",
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url=None,
            team_allowed_emails=(),
            team_allowed_email_domains=(),
            team_portal_data_dir=Path(self.temp_dir.name),
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Input",
            bpmis_base_url="https://example.com",
            bpmis_api_access_token="token",
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

        self.assertIn("不问商业价值", prompt)
        self.assertIn("不管技术细节", prompt)
        self.assertIn("实事求是，拉开分差", prompt)
        self.assertIn("Scoring Rubric", prompt)
        self.assertIn("主流程闭环", prompt)
        self.assertIn("异常分支与逆向", prompt)
        self.assertIn("规则冲突", prompt)
        self.assertIn("逻辑严密度评估", prompt)
        self.assertIn("最终得分", prompt)
        self.assertIn("Critical Blockers", prompt)
        self.assertIn("必须补齐的异常分支", prompt)
        self.assertIn("后勤与兜底确认", prompt)
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

    def test_prd_briefing_review_prompt_can_be_url_only(self):
        page = self.service.confluence.page

        prompt = build_prd_review_prompt(
            jira_id="",
            jira_link="",
            prd_url=page.source_url,
            page=page,
        )

        self.assertIn("Jira ID: -", prompt)
        self.assertIn("逻辑严密度评估", prompt)
        self.assertIn("This PRD introduces approval workflow", prompt)

    def test_prd_briefing_review_english_prompt_requests_english_output(self):
        page = self.service.confluence.page

        prompt = build_prd_review_prompt(
            jira_id="",
            jira_link="",
            prd_url=page.source_url,
            page=page,
            language="en",
        )

        self.assertIn("Return only concise Markdown in English", prompt)
        self.assertIn("Logic Rigor Assessment", prompt)
        self.assertNotIn("逻辑严密度评估", prompt)

    def test_prd_summary_english_prompt_requests_english_output(self):
        page = self.service.confluence.page

        prompt = build_prd_summary_prompt(
            jira_id="",
            jira_link="",
            prd_url=page.source_url,
            page=page,
            language="en",
        )

        self.assertIn("concise but complete English summary", prompt)
        self.assertIn("Jira ID: -", prompt)
        self.assertIn("PRD Summary", prompt)
        self.assertNotIn("中文摘要", prompt)

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_briefing_review_cache_varies_by_language_and_updated_at(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        service = PRDReviewService(
            store=self.store,
            confluence=self.service.confluence,
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        first = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="zh",
            )
        )
        cached = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="zh",
            )
        )
        english = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
            )
        )

        self.assertFalse(first["cached"])
        self.assertTrue(cached["cached"])
        self.assertFalse(english["cached"])
        self.assertEqual(mock_generate.call_count, 2)
        self.assertEqual(first["review"]["jira_id"], PRD_BRIEFING_REVIEW_CACHE_KEY)
        self.assertEqual(first["review"]["prompt_version"], prd_briefing_review_prompt_version("zh"))
        self.assertEqual(english["review"]["prompt_version"], prd_briefing_review_prompt_version("en"))

        stale = self.store.get_prd_review_result(
            owner_key="anon:test",
            jira_id=PRD_BRIEFING_REVIEW_CACHE_KEY,
            prd_url=self.service.confluence.page.source_url,
            prd_updated_at="2026-04-16T10:00:00Z",
            prompt_version=prd_briefing_review_prompt_version("zh"),
        )
        self.assertIsNone(stale)

    @patch("prd_briefing.reviewer.generate_prd_summary_with_codex")
    def test_prd_url_summary_cache_varies_by_language_and_updated_at(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Summary",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        service = PRDReviewService(
            store=self.store,
            confluence=self.service.confluence,
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        first = service.summarize_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="zh",
            )
        )
        cached = service.summarize_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="zh",
            )
        )
        english = service.summarize_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
            )
        )

        self.assertFalse(first["cached"])
        self.assertTrue(cached["cached"])
        self.assertFalse(english["cached"])
        self.assertEqual(mock_generate.call_count, 2)
        self.assertEqual(first["summary"]["jira_id"], PRD_URL_SUMMARY_CACHE_KEY)
        self.assertEqual(first["summary"]["prompt_version"], prd_summary_prompt_version("zh"))
        self.assertEqual(english["summary"]["prompt_version"], prd_summary_prompt_version("en"))

        stale = self.store.get_prd_review_result(
            owner_key="anon:test",
            jira_id=PRD_URL_SUMMARY_CACHE_KEY,
            prd_url=self.service.confluence.page.source_url,
            prd_updated_at="2026-04-16T10:00:00Z",
            prompt_version=prd_summary_prompt_version("zh"),
        )
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

    def test_create_session_supports_english_walkthrough_prompt_and_cache(self):
        self.openai_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:english",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
            language="en",
        )
        self.openai_client.answer_calls = 0
        self.openai_client.last_system_prompt = None
        self.openai_client.last_user_prompt = None

        first = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:english",
            section_index=0,
            include_audio=False,
        )
        second = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:english",
            section_index=0,
            include_audio=False,
        )

        self.assertEqual(payload["session"]["audience"], "developer_en")
        self.assertEqual(first["language"], "en")
        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(self.openai_client.answer_calls, 1)
        self.assertIn("software engineers in English", self.openai_client.last_system_prompt)
        self.assertIn("around 5 to 9 sentences in English", self.openai_client.last_user_prompt)

    def test_english_walkthrough_audio_uses_english_language_code(self):
        self.openai_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:english-audio",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
            language="en",
        )

        result = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:english-audio",
            section_index=0,
            include_audio=True,
        )

        self.assertEqual(result["language"], "en")
        self.assertEqual(self.voice_service.synthesize_calls[-1]["language_code"], "en")

    def test_tts_text_is_not_truncated_for_edge_voice(self):
        long_text = " ".join([f"Sentence {index} keeps implementation detail." for index in range(80)])

        optimized = optimize_tts_text(long_text, language_code="en")

        self.assertEqual(optimized, long_text)
        self.assertGreater(len(optimized), 520)

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

    def test_pm_briefing_blocks_use_english_labels_for_english_walkthrough(self):
        sections = [
            {"section_path": "3.5.1 Search Layout", "content": "The page shows search criteria and default fields. User can click Search button."},
            {"section_path": "3.6.1 Submit Review", "content": "User can submit for review and status changes from Draft to Pending Review."},
        ]

        blocks = build_pm_briefing_blocks(sections, language="en")

        self.assertTrue(any(block["title"] == "Page Layout and Field Rules" for block in blocks))
        self.assertTrue(any(block["title"] == "Status Transitions and Actions" for block in blocks))
        self.assertTrue(all("：" not in block["merged_summary"] for block in blocks))
        self.assertTrue(all("说明" not in block["briefing_goal"] for block in blocks))

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
                ("anon:test", "developer_zh", WALKTHROUGH_SCRIPT_PROMPT_VERSION),
            )
        self.store.cache_script(
            owner_key="anon:test",
            audience="developer_zh",
            model_id="openai:gpt-4.1-mini|gemini:gemini-2.5-flash",
            prompt_version=WALKTHROUGH_SCRIPT_PROMPT_VERSION,
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

        with self.assertRaisesRegex(RuntimeError, "Codex to be configured"):
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
