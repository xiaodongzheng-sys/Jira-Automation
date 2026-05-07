import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from prd_briefing.service import VoiceService, normalize_prd_briefing_script_text, optimize_tts_text
from prd_briefing.storage import BriefingStore


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


if __name__ == "__main__":
    unittest.main()
