import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from prd_briefing.openai_client import OpenAIClient
from prd_briefing.service import VoiceService, optimize_tts_text
from prd_briefing.storage import BriefingStore


class OpenAIAudioTests(unittest.TestCase):
    @patch("prd_briefing.openai_client.requests.post")
    def test_synthesize_speech_sends_instructions_and_speed(self, mock_post):
        response = Mock()
        response.content = b"audio"
        response.raise_for_status.return_value = None
        mock_post.return_value = response

        client = OpenAIClient(
            api_key="test-key",
            base_url="https://api.openai.com/v1",
            text_model="gpt-4.1-mini",
            embedding_model="text-embedding-3-large",
            transcription_model="gpt-4o-mini-transcribe",
            tts_model="gpt-4o-mini-tts",
        )

        client.synthesize_speech(
            text="Hello team",
            voice="coral",
            instructions="Speak warmly.",
            speed=0.95,
        )

        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["voice"], "coral")
        self.assertEqual(payload["instructions"], "Speak warmly.")
        self.assertEqual(payload["speed"], 0.95)

    def test_voice_service_selects_language_specific_voice(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            store = BriefingStore(Path(temp_dir.name))
            client = Mock()
            client.is_configured.return_value = True
            client.synthesize_speech.return_value = (b"audio", "mp3")

            service = VoiceService(
                store=store,
                openai_client=client,
                tts_provider="openai",
                edge_mandarin_voice="zh-CN-XiaozhenNeural",
                edge_english_voice="en-US-JennyNeural",
                edge_rate="-12%",
                openai_mandarin_voice="sage",
                openai_voice_speed=0.96,
                openai_custom_voice_enabled=False,
                openai_tts_fallback_enabled=True,
                elevenlabs_api_key=None,
                elevenlabs_mandarin_model_id="eleven_multilingual_v2",
                elevenlabs_mandarin_voice_id="JBFqnCBsd6RMkjVDRZzb",
            )

            service.synthesize(session_id="s2", text="中文讲解", language_code="zh", owner_key="anon:test")
            mandarin_voice = client.synthesize_speech.call_args.kwargs["voice"]
            mandarin_instructions = client.synthesize_speech.call_args.kwargs["instructions"]

            self.assertEqual(mandarin_voice, "sage")
            self.assertIn("Mandarin", mandarin_instructions)
        finally:
            temp_dir.cleanup()

    def test_voice_service_does_not_use_openai_when_fallback_disabled(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            store = BriefingStore(Path(temp_dir.name))
            client = Mock()
            client.is_configured.return_value = True

            service = VoiceService(
                store=store,
                openai_client=client,
                tts_provider="openai",
                edge_mandarin_voice="zh-CN-XiaozhenNeural",
                edge_english_voice="en-US-JennyNeural",
                edge_rate="-12%",
                openai_mandarin_voice="sage",
                openai_voice_speed=0.96,
                openai_custom_voice_enabled=False,
                openai_tts_fallback_enabled=False,
                elevenlabs_api_key=None,
                elevenlabs_mandarin_model_id="eleven_multilingual_v2",
                elevenlabs_mandarin_voice_id="JBFqnCBsd6RMkjVDRZzb",
            )

            audio_path = service.synthesize(
                session_id="s1",
                text="中文讲解",
                language_code="zh",
                owner_key="anon:test",
            )

            self.assertIsNone(audio_path)
            client.synthesize_speech.assert_not_called()
        finally:
            temp_dir.cleanup()

    def test_voice_service_defaults_to_edge_tts(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            store = BriefingStore(Path(temp_dir.name))
            client = Mock()
            client.is_configured.return_value = True

            service = VoiceService(
                store=store,
                openai_client=client,
                tts_provider="edge",
                edge_mandarin_voice="zh-CN-XiaozhenNeural",
                edge_english_voice="en-US-JennyNeural",
                edge_rate="-12%",
                openai_mandarin_voice="sage",
                openai_voice_speed=0.96,
                openai_custom_voice_enabled=False,
                openai_tts_fallback_enabled=True,
                elevenlabs_api_key="test-eleven-key",
                elevenlabs_mandarin_model_id="eleven_multilingual_v2",
                elevenlabs_mandarin_voice_id="JBFqnCBsd6RMkjVDRZzb",
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
            self.assertEqual(edge_tts.call_args.kwargs["voice_id"], "zh-CN-XiaozhenNeural")
            client.synthesize_speech.assert_not_called()
            self.assertEqual(
                service.get_cached_audio_for_text(owner_key="anon:test", text="中文讲解", language_code="zh"),
                audio_path,
            )
        finally:
            temp_dir.cleanup()

    def test_optimize_tts_text_truncates_long_scripts(self):
        text = "这一块主要是说明字段规则。" * 80
        optimized = optimize_tts_text(text, language_code="zh")
        self.assertLessEqual(len(optimized), 421)
        self.assertTrue(optimized.endswith("。"))


if __name__ == "__main__":
    unittest.main()
