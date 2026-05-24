import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from prd_briefing.models import ChunkRecord
from prd_briefing.service import VoiceService, normalize_prd_briefing_script_text, optimize_tts_text
from prd_briefing.storage import BriefingStore, citation_from_chunk, re_safe_filename


class PRDBriefingAudioTests(unittest.TestCase):
    def test_voice_service_defaults_to_edge_tts(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            store = BriefingStore(Path(temp_dir.name))
            service = VoiceService(
                store=store,
                tts_provider="edge",
                edge_mandarin_voice="zh-CN-XiaoxiaoNeural",
                edge_english_voice="en-SG-LunaNeural",
                edge_rate="-12%",
                edge_mandarin_rate="+0%",
                edge_english_rate="-5%",
            )

            with patch.object(service, "_synthesize_with_edge_tts", return_value=b"edge-audio") as edge_tts:
                audio_path = service.synthesize(
                    session_id="s3",
                    text="中文讲解",
                    language_code="zh",
                    owner_key="anon:test",
                )

            self.assertIsNotNone(audio_path)
            edge_tts.assert_called_once()
            self.assertEqual(edge_tts.call_args.kwargs["voice_id"], "zh-CN-XiaoxiaoNeural")
            self.assertEqual(edge_tts.call_args.kwargs["rate"], "+0%")
            self.assertEqual(
                service.get_cached_audio_for_text(owner_key="anon:test", text="中文讲解", language_code="zh"),
                audio_path,
            )
        finally:
            temp_dir.cleanup()

    def test_legacy_tts_provider_value_is_normalized_to_edge(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            store = BriefingStore(Path(temp_dir.name))
            service = VoiceService(
                store=store,
                tts_provider="legacy-provider",
                edge_mandarin_voice="zh-CN-XiaoxiaoNeural",
                edge_english_voice="en-SG-LunaNeural",
                edge_rate="-12%",
                edge_mandarin_rate="+0%",
                edge_english_rate="-5%",
            )

            with patch.object(service, "_synthesize_with_edge_tts", return_value=b"edge-audio") as edge_tts:
                audio_path = service.synthesize(
                    session_id="s1",
                    text="中文讲解",
                    language_code="zh",
                    owner_key="anon:test",
                )

            self.assertIsNotNone(audio_path)
            edge_tts.assert_called_once()
            self.assertEqual(service.tts_provider, "edge")
        finally:
            temp_dir.cleanup()

    def test_edge_tts_english_uses_singapore_voice_and_slow_rate(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            store = BriefingStore(Path(temp_dir.name))
            service = VoiceService(
                store=store,
                tts_provider="edge",
                edge_mandarin_voice="zh-CN-XiaoxiaoNeural",
                edge_english_voice="en-SG-LunaNeural",
                edge_rate="-12%",
                edge_mandarin_rate="+0%",
                edge_english_rate="-5%",
            )

            with patch.object(service, "_synthesize_with_edge_tts", return_value=b"edge-audio") as edge_tts:
                audio_path = service.synthesize(
                    session_id="s4",
                    text="We will build a new button.",
                    language_code="en",
                    owner_key="anon:test",
                )

            self.assertIsNotNone(audio_path)
            self.assertEqual(edge_tts.call_args.kwargs["voice_id"], "en-SG-LunaNeural")
            self.assertEqual(edge_tts.call_args.kwargs["rate"], "-5%")
        finally:
            temp_dir.cleanup()

    def test_optimize_tts_text_truncates_long_scripts(self):
        text = "这一块主要是说明字段规则。" * 80
        optimized = optimize_tts_text(text, language_code="zh")
        self.assertLessEqual(len(optimized), 421)
        self.assertTrue(optimized.endswith("。"))

    def test_chinese_tts_script_normalization_spaces_english_terms_and_acronyms(self):
        optimized = normalize_prd_briefing_script_text(
            "调用api时，如果qps超过限制，那么返回json错误。正常flow继续。",
            language_code="zh",
        )

        self.assertIn("调用 API 时", optimized)
        self.assertIn("如果 QPS 超过限制", optimized)
        self.assertIn("返回 JSON 错误", optimized)
        self.assertIn("正常 flow 继续", optimized)

    def test_briefing_store_migrates_legacy_schema_and_handles_json_boundaries(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            root = Path(temp_dir.name)
            db_path = root / "prd_briefing.db"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    create table briefing_sessions (
                        session_id text primary key,
                        owner_key text not null,
                        confluence_page_id text not null,
                        confluence_page_url text not null,
                        audience text not null,
                        mode text not null,
                        title text not null,
                        status text not null,
                        created_at text not null,
                        updated_at text not null
                    );
                    create table briefing_chunks (
                        id integer primary key autoincrement,
                        source_id integer not null,
                        owner_key text not null,
                        session_id text,
                        source_type text not null,
                        title text not null,
                        section_path text not null,
                        content text not null,
                        image_refs_json text not null default '[]',
                        source_url text not null,
                        updated_at text not null,
                        embedding_json text
                    );
                    """
                )

            store = BriefingStore(root)
            session_id = store.create_session(
                owner_key="owner",
                confluence_page_id="123",
                confluence_page_url="https://confluence/page",
                audience="developer",
                mode="walkthrough",
                title="PRD",
                metadata={"ok": True},
            )
            with store.connect() as conn:
                conn.execute("update briefing_sessions set metadata_json = ? where session_id = ?", ("[1]", session_id))
            self.assertEqual(store.get_session(session_id, "owner")["metadata"], {})
            with store.connect() as conn:
                conn.execute("update briefing_sessions set metadata_json = ? where session_id = ?", ("{bad", session_id))
            self.assertEqual(store.get_session(session_id, "owner")["metadata"], {})

            source_id = store.upsert_source(
                owner_key="owner",
                session_id=session_id,
                source_type="confluence",
                external_id="123",
                title="Old",
                language="zh",
                source_url="https://confluence/old",
                updated_at="2026-05-01T00:00:00Z",
                metadata={"old": True},
            )
            store.replace_chunks([])
            store.replace_chunks(
                [
                    ChunkRecord(
                        source_id=source_id,
                        owner_key="owner",
                        session_id=session_id,
                        source_type="confluence",
                        title="Old",
                        section_path="A",
                        content="Body",
                        html_content="<p>Body</p>",
                        image_refs=["img"],
                        source_url="https://confluence/old",
                        updated_at="2026-05-01T00:00:00Z",
                        embedding=[0.1],
                    )
                ]
            )
            same_source_id = store.upsert_source(
                owner_key="owner",
                session_id=session_id,
                source_type="confluence",
                external_id="123",
                title="New",
                language="en",
                source_url="https://confluence/new",
                updated_at="2026-05-02T00:00:00Z",
                metadata={"new": True},
            )
            self.assertEqual(same_source_id, source_id)
            self.assertEqual(store.list_session_chunks(session_id, "owner"), [])

            self.assertEqual(store.save_latest_tool_result(owner_key="", tool_key="x", payload={}), {})
            self.assertIsNone(store.get_latest_tool_result(owner_key="", tool_key="x"))
            self.assertIsNone(store.get_latest_tool_result(owner_key="owner", tool_key="missing"))
            store.save_latest_tool_result(owner_key="owner", tool_key="tool", payload={"value": 1})
            with store.connect() as conn:
                conn.execute(
                    "update latest_tool_results set payload_json = ? where owner_key = ? and tool_key = ?",
                    ("not-json", "owner", "tool"),
                )
            self.assertEqual(store.get_latest_tool_result(owner_key="owner", tool_key="tool")["payload"], {})

            result = store.save_prd_review_result(
                owner_key="owner",
                jira_id="AF-1",
                jira_link="https://jira/AF-1",
                prd_url="https://confluence/prd",
                prd_updated_at="2026-05-01T00:00:00Z",
                prompt_version="v1",
                status="completed",
                result_markdown="ok",
                trace={"model": "codex"},
            )
            with store.connect() as conn:
                conn.execute("update prd_review_results set trace_json = ? where id = ?", ("not-json", result["id"]))
            self.assertEqual(
                store.get_prd_review_result(
                    owner_key="owner",
                    jira_id="AF-1",
                    prd_url="https://confluence/prd",
                    prd_updated_at="2026-05-01T00:00:00Z",
                    prompt_version="v1",
                )["trace"],
                {},
            )

            store.save_presentation_outline_cache(
                owner_key="owner",
                page_id="123",
                version_number="1",
                prompt_version="v1",
                model_id="codex",
                title="Title",
                source_url="https://confluence/prd",
                updated_at="2026-05-01T00:00:00Z",
                chunks=[{"id": "chunk-1"}],
                media={"type": "image"},
            )
            with store.connect() as conn:
                conn.execute("update presentation_outline_cache set chunks_json = ?, media_json = ?",
                             ("not-json", "null"))
            cache = store.get_presentation_outline_cache(
                owner_key="owner",
                page_id="123",
                version_number="1",
                prompt_version="v1",
                model_id="codex",
            )
            self.assertEqual(cache["chunks"], [])
            self.assertEqual(cache["media"], {})

            asset_path = store.save_asset("session-1", "mock.png", b"png")
            self.assertEqual((root / asset_path).read_bytes(), b"png")
            self.assertEqual(re_safe_filename(" .. "), "cache")
            citation = citation_from_chunk(
                ChunkRecord(
                    source_id=1,
                    owner_key="owner",
                    session_id=session_id,
                    source_type="confluence",
                    title="Long",
                    section_path="Section",
                    content="x" * 260,
                    html_content="",
                    image_refs=[],
                    source_url="https://confluence/prd",
                    updated_at="now",
                )
            )
            self.assertTrue(citation.snippet.endswith("..."))
        finally:
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
