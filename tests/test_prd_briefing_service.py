import tempfile
import unittest
from dataclasses import dataclass
import io
from pathlib import Path
from unittest.mock import patch
import json

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError

from prd_briefing.confluence import ConfluenceConnector, IngestedConfluencePage, ParsedSection, SpreadsheetLink
from prd_briefing.reviewer import (
    PRD_BRIEFING_REVIEW_CACHE_KEY,
    PRD_REVIEW_PROMPT_VERSION,
    PRD_SUMMARY_PROMPT_VERSION,
    PRD_URL_SUMMARY_CACHE_KEY,
    PRDBriefingReviewRequest,
    PRDReviewRequest,
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
    build_presentation_system_prompt,
    build_pm_briefing_blocks,
    build_heuristic_session_overview,
    overview_is_low_signal,
    optimize_tts_text,
    parse_presentation_chunks,
    parse_session_overview,
    select_sections_for_overview,
)
from prd_briefing.storage import BriefingStore


class FakeTextClient:
    def __init__(self):
        self.last_system_prompt = None
        self.last_user_prompt = None
        self.answer_calls = 0
        self.model_id = "codex:test"
        self.answer_response = "LLM answer"

    def is_configured(self):
        return False

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


class FakeConfluenceResponse:
    def __init__(self, content: bytes):
        self.content = content


class FakeAttachmentConnector(FakeConnector):
    def __init__(self, page: IngestedConfluencePage, attachment_content: bytes):
        super().__init__(page)
        self.attachment_content = attachment_content
        self.requested_urls = []

    def _request(self, url, accept=None, **_kwargs):
        self.requested_urls.append((url, accept))
        return FakeConfluenceResponse(self.attachment_content)


class FakeDriveExecute:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeDriveFiles:
    def __init__(self, workbook_content: bytes, *, modified_time: str = "2026-05-12T10:00:00.000Z"):
        self.workbook_content = workbook_content
        self.modified_time = modified_time
        self.get_calls = 0
        self.export_calls = 0

    def get(self, **_kwargs):
        self.get_calls += 1
        return FakeDriveExecute(
            {
                "id": "sheet123",
                "name": "MAS Outsourcing Register",
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "modifiedTime": self.modified_time,
            }
        )

    def export_media(self, **_kwargs):
        self.export_calls += 1
        return FakeDriveExecute(self.workbook_content)

    def get_media(self, **_kwargs):
        self.export_calls += 1
        return FakeDriveExecute(self.workbook_content)


class FakeDriveService:
    def __init__(self, workbook_content: bytes, *, modified_time: str = "2026-05-12T10:00:00.000Z"):
        self.files_resource = FakeDriveFiles(workbook_content, modified_time=modified_time)

    def files(self):
        return self.files_resource


def _xlsx_bytes(*, sheet_count: int = 1, marker: str = "MAS format") -> bytes:
    from openpyxl import Workbook

    workbook = Workbook()
    first = workbook.active
    first.title = "Register Format"
    first.append(["Field", "Required", "Format"])
    first.append(["Outsourcing Arrangement ID", "Yes", marker])
    for index in range(2, sheet_count + 1):
        sheet = workbook.create_sheet(f"Sheet {index}")
        sheet.append(["Column", "Rule"])
        sheet.append([f"field_{index}", f"rule_{index}"])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _xlsx_template_risk_bytes() -> bytes:
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Register Format"
    sheet.merge_cells("A1:C1")
    sheet["A1"] = "Merged Header"
    sheet.append(["Field", "Field", ""])
    sheet.append(["Amount", "=SUM(D4:D5)", ""])
    sheet.column_dimensions["C"].hidden = True
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


class PRDBriefingServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = BriefingStore(Path(self.temp_dir.name))
        self.text_client = FakeTextClient()
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
            text_client=self.text_client,
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

    def test_confluence_parser_keeps_media_inside_content_layout_with_toc(self):
        connector = ConfluenceConnector(
            base_url="https://confluence.example",
            email=None,
            api_token=None,
            bearer_token=None,
            store=self.store,
        )
        media_dict = {}

        sections = connector._parse_sections(
            html="""
            <div class="contentLayout2">
              <div class="toc-macro">Table of contents</div>
              <h2>Feature Details</h2>
              <p>Main implementation flow.</p>
              <p><img class="confluence-embedded-image" src="/download/attachments/123/flow.png" width="720"></p>
              <table>
                <tr><th>Status</th><th>Meaning</th></tr>
                <tr><td>NEW</td><td>Create a new outsourced case.</td></tr>
              </table>
            </div>
            """,
            base_url="https://confluence.example",
            source_url="https://confluence.example/pages/viewpage.action?pageId=123",
            session_id="session-1",
            media_dict=media_dict,
        )

        self.assertEqual(len(sections), 1)
        self.assertIn("Main implementation flow.", sections[0].content)
        self.assertGreaterEqual(len(sections[0].image_refs), 1)
        self.assertGreaterEqual(len(sections[0].media_refs), 2)
        self.assertTrue(any(item["type"] == "image" for item in media_dict.values()))
        self.assertTrue(any(item["type"] == "table" for item in media_dict.values()))
        source_text = connector._build_source_text_with_media(sections)
        self.assertIn("[MEDIA_ID_", source_text)
        self.assertIn("[IMAGE] https://confluence.example/download/attachments/123/flow.png", source_text)

    def test_confluence_parser_drops_unresolved_macro_image_placeholders(self):
        connector = ConfluenceConnector(
            base_url="https://confluence.example",
            email=None,
            api_token=None,
            bearer_token=None,
            store=self.store,
        )
        media_dict = {}

        sections = connector._parse_sections(
            html="""
            <table>
              <tr>
                <th>JIRA</th>
                <td>
                  <img src="$iconUrl">SPSK-264073
                  (<img src="/$statusIcon">)
                  <img src="/download/attachments/123/real.png" width="720">
                </td>
              </tr>
            </table>
            """,
            base_url="https://confluence.example",
            source_url="https://confluence.example/pages/viewpage.action?pageId=123",
            session_id="session-1",
            media_dict=media_dict,
        )

        html = sections[0].html_content
        self.assertNotIn("$iconUrl", html)
        self.assertNotIn("$statusIcon", html)
        self.assertIn("SPSK-264073", html)
        self.assertIn("/prd-briefing/image-proxy?src=", html)
        self.assertEqual([item["type"] for item in media_dict.values()], ["table"])

    def test_presentation_prompts_include_language_specific_script_rules(self):
        english_prompt = build_presentation_system_prompt("en")
        chinese_prompt = build_presentation_system_prompt("zh")

        self.assertIn("Singapore-neutral English", english_prompt)
        self.assertIn("subject-verb-object", english_prompt)
        self.assertIn("Avoid US or UK slang", english_prompt)
        self.assertIn("中文汉字和英文单词", chinese_prompt)
        self.assertIn("QPS", chinese_prompt)
        self.assertIn("如果...那么", chinese_prompt)

    def test_presentation_cache_hit_reuses_outline_without_second_llm_call(self):
        self.text_client.is_configured = lambda: True
        self.text_client.answer_response = json.dumps([
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
        calls_after_first = self.text_client.answer_calls
        second = self.service.process_prd_for_presentation(
            owner_key="anon:presentation",
            page_ref="https://example.atlassian.net/wiki/pages/123",
        )

        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(calls_after_first, 1)
        self.assertEqual(self.text_client.answer_calls, 1)
        self.assertEqual(second["chunks"][0]["media"]["type"], "table")
        self.assertEqual(second["session"]["version_number"], "5")

    def test_presentation_cache_misses_when_page_version_changes(self):
        self.text_client.is_configured = lambda: True
        self.text_client.answer_response = json.dumps([
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
        self.assertEqual(self.text_client.answer_calls, 2)

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
        self.text_client.is_configured = lambda: True
        self.text_client.answer_response = json.dumps([
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
        self.assertEqual(self.voice_service.synthesize_calls[-1]["language_code"], "zh")

    def test_presentation_audio_uses_english_session_language(self):
        self.text_client.is_configured = lambda: True
        self.text_client.answer_response = json.dumps([
            {
                "id": "chunk-1",
                "title": "Main Flow",
                "content": "We will build a new button.",
            }
        ])
        payload = self.service.process_prd_for_presentation(
            owner_key="anon:presentation-en",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            language="en",
        )

        self.service.generate_presentation_audio(
            owner_key="anon:presentation-en",
            session_id=payload["session"]["session_id"],
            chunk=dict(payload["chunks"][0]),
        )

        self.assertEqual(self.voice_service.synthesize_calls[-1]["language_code"], "en")

    def test_presentation_audio_cache_is_scoped_by_page_version_and_chunk(self):
        voice = VoiceService(
            store=self.store,
            tts_provider="edge",
            edge_mandarin_voice="zh-CN-XiaozhenNeural",
            edge_english_voice="en-US-JennyNeural",
            edge_rate="-12%",
            edge_mandarin_rate="+0%",
            edge_english_rate="-5%",
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
        self.assertEqual(calls[0]["rate"], "+0%")
        self.assertIn("audio/123_5/chunk-1-", first["audioUrl"])
        self.assertIn("audio/123_6/chunk-1-", second["audioUrl"])
        self.assertEqual(third["audioUrl"], first["audioUrl"])

    def test_presentation_audio_cache_is_scoped_by_language_rate(self):
        voice = VoiceService(
            store=self.store,
            tts_provider="edge",
            edge_mandarin_voice="zh-CN-XiaoxiaoNeural",
            edge_english_voice="en-SG-LunaNeural",
            edge_rate="-12%",
            edge_mandarin_rate="+0%",
            edge_english_rate="-5%",
        )
        calls = []
        voice._synthesize_with_edge_tts_with_boundaries = lambda **kwargs: (calls.append(kwargs) or (b"mp3", []))
        chunk = {
            "id": "chunk-1",
            "title": "Main Flow",
            "content": "We will build a new button.",
            "media": {"type": "none", "content": ""},
        }

        voice.synthesize_presentation_chunk(
            session_id="session-1",
            owner_key="anon:presentation-audio-language",
            chunk=chunk,
            language_code="en",
            presentation_cache_key="123_5",
        )

        self.assertEqual(calls[0]["voice_id"], "en-SG-LunaNeural")
        self.assertEqual(calls[0]["rate"], "-5%")

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

    def test_prd_review_skill_artifact_contains_section_guidance(self):
        skill_path = Path("/Users/NPTSG0388/.codex/skills/prd-review/SKILL.md")
        content = skill_path.read_text(encoding="utf-8")

        self.assertIn("name: prd-review", content)
        self.assertIn("description:", content)
        self.assertIn("Section-by-Section Improvement Suggestions", content)
        self.assertIn("Suggested PRD update", content)
        self.assertIn("Acceptance check", content)
        self.assertIn("Source not found in selected sections", content)

    def test_prd_review_prompt_contains_section_improvement_guidance_and_prd_sections(self):
        page = self.service.confluence.page

        prompt = build_prd_review_prompt(
            jira_id="AF-123",
            jira_link="https://jira/browse/AF-123",
            prd_url=page.source_url,
            page=page,
        )

        self.assertEqual(PRD_REVIEW_PROMPT_VERSION, "v7_prd_review_report_template_feasibility")
        self.assertIn("prd-review", prompt)
        self.assertIn("Executive Verdict", prompt)
        self.assertIn("Top Must-Fix Delivery Blockers", prompt)
        self.assertIn("Section Patch Suggestions", prompt)
        self.assertIn("Evidence Coverage", prompt)
        self.assertIn("Report Generation Feasibility", prompt)
        self.assertIn("Report Template Risks", prompt)
        self.assertIn("PRD-to-Template Mapping Gaps", prompt)
        self.assertIn("Generation feasibility", prompt)
        self.assertIn("Technical generation risk", prompt)
        self.assertIn("PRD mapping gap", prompt)
        self.assertIn("Used in findings", prompt)
        self.assertIn("优先级", prompt)
        self.assertIn("Suggested PRD patch", prompt)
        self.assertIn("Evidence basis", prompt)
        self.assertIn("建议补写", prompt)
        self.assertIn("验收检查", prompt)
        self.assertIn("PM Decision Checklist", prompt)
        self.assertIn("Source not found in selected sections", prompt)
        self.assertIn("最终得分", prompt)
        self.assertNotIn("Previous " + "PRD Evidence", prompt)
        self.assertNotIn("previous_" + "prd_url", prompt)
        self.assertNotIn("毒舌", prompt)
        self.assertNotIn("逻辑严密度评估", prompt)
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
        old_prompt_cached = self.store.get_prd_review_result(
            owner_key="anon:test",
            jira_id="AF-123",
            prd_url="https://example/prd",
            prd_updated_at="2026-04-28T00:00:00Z",
            prompt_version="v3_strict_delivery_logic_review_codex",
        )

        self.assertEqual(saved["result_markdown"], "### Review")
        self.assertEqual(cached["trace"]["session_id"], "s1")
        self.assertIsNone(stale)
        self.assertIsNone(old_prompt_cached)

    def test_prd_briefing_review_prompt_can_be_url_only(self):
        page = self.service.confluence.page

        prompt = build_prd_review_prompt(
            jira_id="",
            jira_link="",
            prd_url=page.source_url,
            page=page,
        )

        self.assertIn("Jira ID: -", prompt)
        self.assertIn("Executive Verdict", prompt)
        self.assertIn("Top Must-Fix Delivery Blockers", prompt)
        self.assertIn("Section Patch Suggestions", prompt)
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
        self.assertIn("Executive Verdict", prompt)
        self.assertIn("Top Must-Fix Delivery Blockers", prompt)
        self.assertIn("Section Patch Suggestions", prompt)
        self.assertIn("Suggested PRD patch", prompt)
        self.assertIn("Acceptance check", prompt)
        self.assertIn("Evidence basis", prompt)
        self.assertNotIn("Previous " + "PRD Evidence", prompt)
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

    def test_prd_self_assessment_lists_sections_in_source_order(self):
        service = PRDReviewService(
            store=self.store,
            confluence=self.service.confluence,
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        payload = service.list_url_sections(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
            )
        )

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["prd"]["title"], "Payments PRD")
        self.assertEqual([section["index"] for section in payload["sections"]], [1, 2])
        self.assertEqual(payload["sections"][0]["title"], "Overview")
        self.assertGreater(payload["sections"][0]["char_count"], 0)
        self.assertFalse(payload["sections"][0]["long"])

    def test_prd_self_assessment_lists_spreadsheet_link_counts(self):
        page = self.service.confluence.page
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="MAS Outsourcing Register.xlsx",
                url="https://example.atlassian.net/download/attachments/123/MAS%20Outsourcing%20Register.xlsx",
                source_section="Rollout",
                filename="MAS Outsourcing Register.xlsx",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=self.service.confluence,
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        payload = service.list_url_sections(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
            )
        )

        self.assertEqual(payload["sections"][0]["linked_spreadsheet_count"], 0)
        self.assertEqual(payload["sections"][1]["linked_spreadsheet_count"], 1)

    def test_confluence_parser_extracts_spreadsheet_links_by_section(self):
        connector = ConfluenceConnector(base_url="", email="", api_token="", bearer_token="", store=self.store)

        sections = connector._parse_sections(
            html="""
            <h2>Register</h2>
            <p><a href="/download/attachments/123/MAS%20Outsourcing%20Register.xlsx">MAS Register format</a></p>
            <h2>Appendix</h2>
            <ac:link><ri:attachment ri:filename="Fallback Format.xlsm" /></ac:link>
            """,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/pages/viewpage.action?pageId=123",
            page_id="123",
            session_id="s1",
            media_dict={},
        )

        self.assertEqual(sections[0].spreadsheet_links[0].title, "MAS Register format")
        self.assertIn("/download/attachments/123/MAS%20Outsourcing%20Register.xlsx", sections[0].spreadsheet_links[0].url)
        self.assertEqual(sections[1].spreadsheet_links[0].filename, "Fallback Format.xlsm")

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_selected_sections_limit_prompt_and_cache(self, mock_generate):
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
                language="en",
                selected_section_indexes=[2],
            )
        )
        second = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )
        different_selection = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[1],
            )
        )

        first_prompt = mock_generate.call_args_list[0].kwargs["prompt"]
        self.assertIn("Rollout will happen", first_prompt)
        self.assertNotIn("approval workflow", first_prompt)
        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertFalse(different_selection["cached"])
        self.assertEqual(mock_generate.call_count, 2)
        self.assertTrue(first["review"]["jira_id"].startswith(f"{PRD_BRIEFING_REVIEW_CACHE_KEY}:sections:"))
        self.assertEqual(first["coverage"]["sections_total"], 2)
        self.assertEqual(first["coverage"]["selected_section_indexes"], [2])
        self.assertEqual(first["coverage"]["selected_section_titles"], ["Rollout"])

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_all_selected_reuses_full_review_cache_key(self, mock_generate):
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
                selected_section_indexes=[1, 2],
            )
        )
        cached = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="zh",
            )
        )

        self.assertFalse(first["cached"])
        self.assertTrue(cached["cached"])
        self.assertEqual(first["review"]["jira_id"], PRD_BRIEFING_REVIEW_CACHE_KEY)
        self.assertEqual(mock_generate.call_count, 1)

    def test_prd_self_assessment_selected_sections_validate_indexes(self):
        service = PRDReviewService(
            store=self.store,
            confluence=self.service.confluence,
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        with self.assertRaisesRegex(ToolError, "at least one"):
            service.review_url(
                PRDBriefingReviewRequest(
                    owner_key="anon:test",
                    prd_url="https://example.atlassian.net/wiki/pages/123",
                    selected_section_indexes=[],
                )
            )
        with self.assertRaisesRegex(ToolError, "out of range"):
            service.review_url(
                PRDBriefingReviewRequest(
                    owner_key="anon:test",
                    prd_url="https://example.atlassian.net/wiki/pages/123",
                    selected_section_indexes=[3],
                )
            )
        with self.assertRaisesRegex(ToolError, "must be a list"):
            service.review_url(
                PRDBriefingReviewRequest(
                    owner_key="anon:test",
                    prd_url="https://example.atlassian.net/wiki/pages/123",
                    selected_section_indexes="2",
                )
            )

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_includes_selected_linked_spreadsheet_evidence(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="MAS Outsourcing Register.xlsx",
                url="https://example.atlassian.net/download/attachments/123/MAS%20Outsourcing%20Register.xlsx",
                source_section="Rollout",
                filename="MAS Outsourcing Register.xlsx",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, _xlsx_bytes(marker="MAS field format")),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        result = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )

        prompt = mock_generate.call_args.kwargs["prompt"]
        self.assertIn("# Linked Spreadsheet Evidence", prompt)
        self.assertIn("MAS Outsourcing Register.xlsx", prompt)
        self.assertIn("Outsourcing Arrangement ID", prompt)
        self.assertIn("MAS field format", prompt)
        self.assertNotIn("approval workflow", prompt)
        self.assertEqual(result["coverage"]["linked_artifacts_total"], 1)
        self.assertEqual(result["coverage"]["linked_artifacts_reviewed"], 1)
        self.assertEqual(result["coverage"]["linked_artifacts"][0]["sheets_extracted"], ["Register Format"])
        self.assertEqual(result["coverage"]["report_templates_reviewed"], 1)

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_skips_linked_spreadsheets_in_non_report_sections(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "Rollout Plan"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="Random Tracker.xlsx",
                url="https://example.atlassian.net/download/attachments/123/Random%20Tracker.xlsx",
                source_section="Rollout",
                filename="Random Tracker.xlsx",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, _xlsx_bytes(marker="should not enter prompt")),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        result = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )

        self.assertEqual(result["coverage"]["report_templates_total"], 0)
        prompt = mock_generate.call_args.kwargs["prompt"]
        self.assertNotIn("should not enter prompt", prompt)
        self.assertIn("No report-template spreadsheet links", prompt)

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_reads_all_report_section_spreadsheets_without_file_limit(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title=f"Report Template {index}.xlsx",
                url=f"https://example.atlassian.net/download/attachments/123/Report%20Template%20{index}.xlsx",
                source_section="Report Layout",
                filename=f"Report Template {index}.xlsx",
            )
            for index in range(1, 8)
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, _xlsx_bytes(marker="all templates")),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        result = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )

        self.assertEqual(result["coverage"]["report_templates_total"], 7)
        self.assertEqual(result["coverage"]["report_templates_reviewed"], 7)
        self.assertNotIn("max_linked_spreadsheet_limit", mock_generate.call_args.kwargs["prompt"])

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_linked_workbook_extracts_first_ten_sheets(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="Large Register.xlsx",
                url="https://example.atlassian.net/download/attachments/123/Large%20Register.xlsx",
                source_section="Rollout",
                filename="Large Register.xlsx",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, _xlsx_bytes(sheet_count=12)),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        result = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )

        artifact = result["coverage"]["linked_artifacts"][0]
        self.assertEqual(len(artifact["sheets_extracted"]), 10)
        self.assertEqual(artifact["skipped_sheet_count"], 2)
        prompt = mock_generate.call_args.kwargs["prompt"]
        self.assertIn("[Sheet: Sheet 10]", prompt)
        self.assertNotIn("[Sheet: Sheet 11]", prompt)

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_linked_workbook_extracts_template_metadata(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="Risky Register.xlsx",
                url="https://example.atlassian.net/download/attachments/123/Risky%20Register.xlsx",
                source_section="Report Layout",
                filename="Risky Register.xlsx",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, _xlsx_template_risk_bytes()),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        result = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )

        artifact = result["coverage"]["report_templates"][0]
        metadata = artifact["template_metadata"]["sheets"][0]
        self.assertGreater(metadata["merged_range_count"], 0)
        self.assertGreater(metadata["formula_cell_count"], 0)
        self.assertGreater(metadata["empty_header_count"], 0)
        self.assertIn("Field", metadata["duplicate_headers"])
        self.assertGreater(metadata["hidden_columns_count"], 0)
        prompt = mock_generate.call_args.kwargs["prompt"]
        self.assertIn("Report template metadata", prompt)
        self.assertIn("formula_cells=", prompt)

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_linked_spreadsheet_hash_splits_cache(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="MAS Outsourcing Register.xlsx",
                url="https://example.atlassian.net/download/attachments/123/MAS%20Outsourcing%20Register.xlsx",
                source_section="Rollout",
                filename="MAS Outsourcing Register.xlsx",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, _xlsx_bytes(marker="initial format")),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        first = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )
        service.confluence = FakeAttachmentConnector(page, _xlsx_bytes(marker="updated format"))
        second = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )

        self.assertFalse(first["cached"])
        self.assertFalse(second["cached"])
        self.assertEqual(mock_generate.call_count, 2)
        self.assertNotEqual(first["review"]["jira_id"], second["review"]["jira_id"])

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_linked_spreadsheet_failure_is_coverage_gap(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="Legacy Register.xls",
                url="https://example.atlassian.net/download/attachments/123/Legacy%20Register.xls",
                source_section="Rollout",
                filename="Legacy Register.xls",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, b"not-used"),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        result = service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            )
        )

        self.assertEqual(result["coverage"]["linked_artifacts_reviewed"], 0)
        self.assertEqual(result["coverage"]["linked_artifacts_failed"], 1)
        self.assertEqual(result["coverage"]["linked_artifacts"][0]["reason"], "unsupported_xls_format")
        self.assertIn("Linked artifact not reviewed", mock_generate.call_args.kwargs["prompt"])

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_google_sheet_link_uses_google_credentials(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="MAS Outsourcing Register Google Sheet",
                url="https://docs.google.com/spreadsheets/d/sheet123/edit#gid=0",
                source_section="Rollout",
                filename="",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeConnector(page),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        with patch(
            "bpmis_jira_tool.gmail_dashboard.build_drive_api_service",
            return_value=FakeDriveService(_xlsx_bytes(marker="Google Sheet format")),
        ):
            result = service.review_url(
                PRDBriefingReviewRequest(
                    owner_key="anon:test",
                    prd_url="https://example.atlassian.net/wiki/pages/123",
                    language="en",
                    selected_section_indexes=[2],
                    google_credentials={
                        "token": "x",
                        "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
                    },
                )
            )

        self.assertEqual(result["coverage"]["linked_artifacts_reviewed"], 1)
        self.assertIn("Google Sheet format", mock_generate.call_args.kwargs["prompt"])

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_self_assessment_google_sheet_artifact_cache_skips_second_export_when_unchanged(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="MAS Outsourcing Register Google Sheet",
                url="https://docs.google.com/spreadsheets/d/sheet123/edit#gid=0",
                source_section="Rollout",
                filename="",
            )
        ]
        drive_service = FakeDriveService(_xlsx_bytes(marker="Cached Google Sheet format"))
        service = PRDReviewService(
            store=self.store,
            confluence=FakeConnector(page),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )
        request = PRDBriefingReviewRequest(
            owner_key="anon:test",
            prd_url="https://example.atlassian.net/wiki/pages/123",
            language="en",
            selected_section_indexes=[2],
            force_refresh=True,
            google_credentials={
                "token": "x",
                "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
            },
        )

        with patch("bpmis_jira_tool.gmail_dashboard.build_drive_api_service", return_value=drive_service):
            first = service.review_url(request)
            second = service.review_url(request)

        self.assertEqual(drive_service.files_resource.export_calls, 1)
        self.assertFalse(first["coverage"]["linked_artifacts"][0]["cache_hit"])
        self.assertTrue(second["coverage"]["linked_artifacts"][0]["cache_hit"])
        self.assertIn("Cached Google Sheet format", mock_generate.call_args.kwargs["prompt"])

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_review_progress_reports_prd_template_metadata_and_generation_stages(self, mock_generate):
        mock_generate.return_value = {
            "result_markdown": "### Review",
            "model_id": "codex-cli",
            "trace": {"session_id": "s1"},
        }
        page = self.service.confluence.page
        page.sections[1].section_path = "3.11.1 Report Layout and Field Name & Format"
        page.sections[1].spreadsheet_links = [
            SpreadsheetLink(
                title="MAS Outsourcing Register.xlsx",
                url="https://example.atlassian.net/download/attachments/123/MAS%20Outsourcing%20Register.xlsx",
                source_section="Rollout",
                filename="MAS Outsourcing Register.xlsx",
            )
        ]
        service = PRDReviewService(
            store=self.store,
            confluence=FakeAttachmentConnector(page, _xlsx_bytes(marker="progress format")),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )
        progress_events = []

        service.review_url(
            PRDBriefingReviewRequest(
                owner_key="anon:test",
                prd_url="https://example.atlassian.net/wiki/pages/123",
                language="en",
                selected_section_indexes=[2],
            ),
            progress_callback=lambda stage, message, current, total: progress_events.append((stage, message, current, total)),
        )

        stages = [event[0] for event in progress_events]
        messages = [event[1] for event in progress_events]
        self.assertIn("reading_prd", stages)
        self.assertIn("reading_report_templates", stages)
        self.assertIn("analyzing_template_metadata", stages)
        self.assertIn("generating_review", stages)
        self.assertTrue(any("Reading 1 report templates" in message for message in messages))

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_prd_review_persists_generation_failure_and_validates_inputs(self, mock_generate):
        mock_generate.side_effect = RuntimeError("model unavailable")
        service = PRDReviewService(
            store=self.store,
            confluence=self.service.confluence,
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        with self.assertRaisesRegex(ToolError, "model unavailable"):
            service.review(
                PRDReviewRequest(
                    owner_key=" anon:test ",
                    jira_id=" AF-123 ",
                    jira_link=" https://jira.example/browse/AF-123 ",
                    prd_url=" https://example.atlassian.net/wiki/pages/123 ",
                )
            )

        failed = self.store.get_prd_review_result(
            owner_key="anon:test",
            jira_id="AF-123",
            prd_url=self.service.confluence.page.source_url,
            prd_updated_at=self.service.confluence.page.updated_at,
            prompt_version=PRD_REVIEW_PROMPT_VERSION,
        )
        self.assertEqual(failed["status"], "failed")
        self.assertIn("model unavailable", failed["error"])

        invalid_requests = [
            PRDReviewRequest(owner_key="", jira_id="AF-123", jira_link="", prd_url="https://example.com"),
            PRDReviewRequest(owner_key="anon:test", jira_id="", jira_link="", prd_url="https://example.com"),
            PRDReviewRequest(owner_key="anon:test", jira_id="AF-123", jira_link="", prd_url="not-a-url"),
            PRDBriefingReviewRequest(owner_key="", prd_url="https://example.com"),
            PRDBriefingReviewRequest(owner_key="anon:test", prd_url="file:///tmp/prd.html"),
        ]
        for request_model in invalid_requests:
            with self.assertRaises(ToolError):
                if isinstance(request_model, PRDBriefingReviewRequest):
                    service.review_url(request_model)
                else:
                    service.review(request_model)

    def test_prd_review_rejects_pages_without_readable_sections(self):
        empty_page = IngestedConfluencePage(
            page_id="empty",
            title="Empty PRD",
            source_url="https://example.atlassian.net/wiki/pages/empty",
            updated_at="2026-05-01T00:00:00Z",
            language="en",
            sections=[],
        )
        service = PRDReviewService(
            store=self.store,
            confluence=FakeConnector(empty_page),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        with self.assertRaisesRegex(ToolError, "readable sections"):
            service.review_url(PRDBriefingReviewRequest(owner_key="anon:test", prd_url=empty_page.source_url))
        with self.assertRaisesRegex(ToolError, "readable sections"):
            service.summarize_url(PRDBriefingReviewRequest(owner_key="anon:test", prd_url=empty_page.source_url))

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

    @patch("prd_briefing.reviewer.generate_prd_summary_with_codex")
    def test_prd_summary_failure_is_saved_for_retry_context(self, mock_generate):
        mock_generate.side_effect = RuntimeError("summary failed")
        service = PRDReviewService(
            store=self.store,
            confluence=self.service.confluence,
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        with self.assertRaisesRegex(ToolError, "summary failed"):
            service.summarize(
                PRDReviewRequest(
                    owner_key="anon:test",
                    jira_id="AF-456",
                    jira_link="",
                    prd_url="https://example.atlassian.net/wiki/pages/123",
                    force_refresh=True,
                )
            )

        failed = self.store.get_prd_review_result(
            owner_key="anon:test",
            jira_id="AF-456",
            prd_url=self.service.confluence.page.source_url,
            prd_updated_at=self.service.confluence.page.updated_at,
            prompt_version=PRD_SUMMARY_PROMPT_VERSION,
        )
        self.assertEqual(failed["status"], "failed")
        self.assertIn("summary failed", failed["error"])

    @patch("prd_briefing.reviewer.generate_prd_summary_with_codex")
    def test_long_prd_summary_uses_hybrid_batches_and_covers_late_sections(self, mock_generate):
        long_page = IngestedConfluencePage(
            page_id="long",
            title="Long PRD",
            source_url="https://example.atlassian.net/wiki/pages/long",
            updated_at="2026-05-10T00:00:00Z",
            language="en",
            sections=[
                ParsedSection(title="Early", section_path="1 Early", content="A" * 95_000),
                ParsedSection(title="Late", section_path="2 Late", content="LATE_SECTION_MARKER"),
            ],
        )
        service = PRDReviewService(
            store=self.store,
            confluence=FakeConnector(long_page),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        def generate_side_effect(*, prompt, **kwargs):
            if "# Batch Summaries" in prompt:
                self.assertIn("late batch summary", prompt)
                return {"result_markdown": "### Final Summary", "model_id": "codex-cli", "trace": {"mode": "final"}}
            if "LATE_SECTION_MARKER" in prompt:
                return {"result_markdown": "late batch summary", "model_id": "codex-cli", "trace": {"mode": "batch"}}
            return {"result_markdown": "early batch summary", "model_id": "codex-cli", "trace": {"mode": "batch"}}

        mock_generate.side_effect = generate_side_effect
        result = service.summarize(
            PRDReviewRequest(owner_key="anon:test", jira_id="AF-999", jira_link="", prd_url=long_page.source_url)
        )

        self.assertEqual(result["coverage"]["mode"], "hybrid")
        self.assertEqual(result["coverage"]["sections_covered"], 2)
        self.assertFalse(result["coverage"]["truncated"])
        self.assertEqual(result["summary"]["result_markdown"], "### Final Summary")
        self.assertGreaterEqual(mock_generate.call_count, 3)

    @patch("prd_briefing.reviewer.generate_prd_review_with_codex")
    def test_long_prd_review_uses_hybrid_batches_and_synthesizes_priorities(self, mock_generate):
        long_page = IngestedConfluencePage(
            page_id="long-review",
            title="Long PRD Review",
            source_url="https://example.atlassian.net/wiki/pages/long-review",
            updated_at="2026-05-10T00:00:00Z",
            language="en",
            sections=[
                ParsedSection(title="Early", section_path="1 Early", content="A" * 95_000),
                ParsedSection(title="Late", section_path="2 Late", content="LATE_REVIEW_MARKER"),
            ],
        )
        service = PRDReviewService(
            store=self.store,
            confluence=FakeConnector(long_page),
            settings=self._settings(),
            workspace_root=Path(self.temp_dir.name),
        )

        def generate_side_effect(*, prompt, **kwargs):
            if "# Batch Reviews" in prompt:
                self.assertIn("late review gap", prompt)
                return {"result_markdown": "### Executive Verdict", "model_id": "codex-cli", "trace": {"mode": "final"}}
            if "LATE_REVIEW_MARKER" in prompt:
                return {"result_markdown": "late review gap", "model_id": "codex-cli", "trace": {"mode": "batch"}}
            return {"result_markdown": "early review gap", "model_id": "codex-cli", "trace": {"mode": "batch"}}

        mock_generate.side_effect = generate_side_effect
        result = service.review_url(
            PRDBriefingReviewRequest(owner_key="anon:test", prd_url=long_page.source_url, language="en")
        )

        self.assertEqual(result["coverage"]["mode"], "hybrid")
        self.assertEqual(result["coverage"]["sections_covered"], 2)
        self.assertEqual(result["review"]["result_markdown"], "### Executive Verdict")
        self.assertGreaterEqual(mock_generate.call_count, 3)

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
        self.text_client.is_configured = lambda: True

        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.text_client.answer_calls = 0
        self.text_client.last_system_prompt = None
        self.text_client.last_user_prompt = None

        result, cached = self.service._compose_walkthrough_section(  # noqa: SLF001
            owner_key="anon:prompt-test",
            section=payload["sections"][0],
        )

        self.assertEqual(result, "LLM answer")
        self.assertFalse(cached)
        self.assertIn("software engineers", self.text_client.last_system_prompt)
        self.assertIn("validation rules", self.text_client.last_system_prompt)
        self.assertIn("implementation", self.text_client.last_user_prompt)
        self.assertIn("这一块主要是", self.text_client.last_user_prompt)

    def test_walkthrough_script_is_cached_after_first_text_model_call(self):
        self.text_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.text_client.answer_calls = 0
        self.text_client.last_system_prompt = None
        self.text_client.last_user_prompt = None

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
        self.assertEqual(self.text_client.answer_calls, 1)

    def test_create_session_supports_english_walkthrough_prompt_and_cache(self):
        self.text_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:english",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
            language="en",
        )
        self.text_client.answer_calls = 0
        self.text_client.last_system_prompt = None
        self.text_client.last_user_prompt = None

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
        self.assertEqual(self.text_client.answer_calls, 1)
        self.assertIn("software engineers in English", self.text_client.last_system_prompt)
        self.assertIn("around 5 to 9 sentences in English", self.text_client.last_user_prompt)

    def test_english_walkthrough_audio_uses_english_language_code(self):
        self.text_client.is_configured = lambda: True
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
        self.text_client.is_configured = lambda: True
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
        self.text_client.answer_calls = 0

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
        self.assertEqual(self.text_client.answer_calls, 1)

    def test_walkthrough_script_reuses_legacy_cached_entry_from_old_model_id(self):
        self.text_client.is_configured = lambda: True
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
            model_id="legacy:text-model",
            prompt_version=WALKTHROUGH_SCRIPT_PROMPT_VERSION,
            section_payload=section_payload,
            script="legacy cached script",
        )
        self.text_client.answer_calls = 0

        result = self.service.narrate_section(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
            section_index=0,
            include_audio=False,
        )

        self.assertEqual(result["script"], "legacy cached script")
        self.assertTrue(result["cached"])
        self.assertFalse(result["audio_cached"])
        self.assertEqual(self.text_client.answer_calls, 0)

    def test_get_session_payload_marks_cached_sections(self):
        self.text_client.is_configured = lambda: True
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
        self.text_client.is_configured = lambda: True
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
        self.text_client.is_configured = lambda: True
        payload = self.service.create_session(
            owner_key="anon:test",
            page_ref="https://example.atlassian.net/wiki/pages/123",
            mode="walkthrough",
        )
        self.text_client.answer_calls = 0

        refreshed = self.service.get_session_payload(
            session_id=payload["session"]["session_id"],
            owner_key="anon:test",
        )

        self.assertTrue(refreshed["session_overview"]["background_goal"])
        self.assertTrue(refreshed["session_overview"]["implementation_overview"])
        self.assertEqual(self.text_client.answer_calls, 0)

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

    def test_create_session_uses_heuristic_when_overview_is_low_signal(self):
        self.text_client.is_configured = lambda: True
        self.text_client.create_answer = lambda *args, **kwargs: """
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
