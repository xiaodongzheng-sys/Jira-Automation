import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.config import Settings


class ConfigTests(unittest.TestCase):
    def test_from_env_defaults_blank_api_token(self):
        with patch.dict(os.environ, {"BPMIS_API_ACCESS_TOKEN": "   "}, clear=False):
            settings = Settings.from_env()
            self.assertIsNone(settings.bpmis_api_access_token)

    def test_from_env_loads_team_portal_stage(self):
        with patch.dict(os.environ, {"TEAM_PORTAL_STAGE": "uat"}, clear=True), patch(
            "bpmis_jira_tool.config.find_dotenv",
            return_value="",
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.team_portal_stage, "uat")

    def test_from_env_accepts_blank_env_file_and_default_google_secret(self):
        with patch.dict(os.environ, {"ENV_FILE": ""}, clear=True), patch(
            "bpmis_jira_tool.config.find_dotenv",
            side_effect=AssertionError("ENV_FILE should bypass find_dotenv"),
        ):
            settings = Settings.from_env()

        self.assertEqual(str(settings.google_oauth_client_secret_file), "google-client-secret.json")
        self.assertEqual(settings.bpmis_call_mode, "direct")

    def test_prd_briefing_mandarin_edge_voice_defaults_to_xiaoxiao(self):
        with patch.dict(os.environ, {}, clear=True), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            settings = Settings.from_env()

        self.assertEqual(settings.prd_briefing_edge_mandarin_voice, "zh-CN-XiaoxiaoNeural")

    def test_spreadsheet_id_defaults_blank(self):
        with patch.dict(os.environ, {}, clear=True), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            settings = Settings.from_env()

        self.assertEqual(settings.spreadsheet_id, "")

    def test_from_env_loads_meeting_recorder_transcription_performance_settings(self):
        with patch.dict(
            os.environ,
            {
                "MEETING_RECORDER_TRANSCRIPT_SEGMENT_WORKERS": "4",
                "MEETING_RECORDER_WHISPER_THREADS": "2",
                "MEETING_RECORDER_BACKGROUND_NICE": "12",
                "MEETING_RECORDER_CAPTURE_STATUS_EVERY_BUFFERS": "300",
                "MEETING_RECORDER_STARTUP_SILENCE_GRACE_SECONDS": "180",
            },
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            settings = Settings.from_env()

        self.assertEqual(settings.meeting_recorder_transcript_segment_workers, 4)
        self.assertEqual(settings.meeting_recorder_whisper_threads, 2)
        self.assertEqual(settings.meeting_recorder_background_nice, 12)
        self.assertEqual(settings.meeting_recorder_capture_status_every_buffers, 300)
        self.assertEqual(settings.meeting_recorder_startup_silence_grace_seconds, 180)

    def test_from_env_loads_dotenv_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                with open(".env", "w", encoding="utf-8") as handle:
                    handle.write("TEAM_PORTAL_PORT=5111\n")
                    handle.write("BPMIS_API_ACCESS_TOKEN=test-token\n")
                    handle.write("TEAM_PORTAL_CONFIG_ENCRYPTION_KEY=shared-key\n")

                with patch.dict(os.environ, {}, clear=True):
                    settings = Settings.from_env()

                self.assertEqual(settings.team_portal_port, 5111)
                self.assertEqual(settings.bpmis_api_access_token, "test-token")
                self.assertEqual(settings.team_portal_config_encryption_key, "shared-key")
            finally:
                os.chdir(original_cwd)

    def test_source_code_qa_defaults_to_codex_only(self):
        with patch.dict(os.environ, {}, clear=True), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            settings = Settings.from_env()

        self.assertEqual(settings.source_code_qa_llm_provider, "codex_cli_bridge")
        self.assertEqual(settings.source_code_qa_embedding_model, "local-token-hybrid-v1")
        self.assertTrue(settings.source_code_qa_semantic_index_enabled)
        self.assertEqual(settings.source_code_qa_llm_timeout_seconds, 90)
        self.assertEqual(settings.source_code_qa_codex_timeout_seconds, 240)
        self.assertEqual(settings.source_code_qa_effort_codex_timeout_seconds, 600)
        self.assertEqual(settings.source_code_qa_codex_concurrency, 2)
        self.assertEqual(settings.source_code_qa_codex_top_path_limit, 30)
        self.assertTrue(settings.source_code_qa_codex_repair_enabled)
        self.assertEqual(settings.source_code_qa_codex_session_mode, "ephemeral")
        self.assertEqual(settings.source_code_qa_codex_session_max_turns, 8)
        self.assertFalse(settings.source_code_qa_codex_cache_followups)
        self.assertEqual(settings.monthly_report_codex_timeout_seconds, 600)
        self.assertEqual(settings.local_agent_connect_timeout_seconds, 10)
        self.assertEqual(settings.meeting_recorder_audio_input, "Meeting Recorder Aggregate")
        self.assertEqual(settings.meeting_recorder_transcript_segment_workers, 2)
        self.assertEqual(settings.meeting_recorder_background_nice, 10)
        self.assertEqual(settings.meeting_recorder_capture_status_every_buffers, 250)
        self.assertEqual(settings.meeting_recorder_startup_silence_grace_seconds, 300)
        self.assertEqual(settings.meeting_translation_owner_email, "xiaodong.zheng@npt.sg")
        self.assertEqual(settings.meeting_translation_model, "gpt-realtime-translate")
        self.assertIsNone(settings.meeting_translation_openai_api_key)

    def test_meeting_translation_openai_settings_from_env(self):
        with patch.dict(
            os.environ,
            {
                "MEETING_RECORDER_OWNER_EMAIL": "recorder@npt.sg",
                "MEETING_TRANSLATION_OPENAI_API_KEY": "translation-key",
                "MEETING_TRANSLATION_MODEL": "gpt-realtime-translate",
            },
            clear=True,
        ), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            settings = Settings.from_env()

        self.assertEqual(settings.meeting_translation_owner_email, "recorder@npt.sg")
        self.assertEqual(settings.meeting_translation_openai_api_key, "translation-key")
        self.assertEqual(settings.meeting_translation_model, "gpt-realtime-translate")

    def test_local_agent_connect_timeout_from_env(self):
        with patch.dict(os.environ, {"LOCAL_AGENT_CONNECT_TIMEOUT_SECONDS": "4"}, clear=True), patch(
            "bpmis_jira_tool.config.find_dotenv",
            return_value="",
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.local_agent_connect_timeout_seconds, 4)

    def test_invalid_integer_helper_env_value_falls_back_to_default(self):
        with patch.dict(
            os.environ,
            {
                "MEETING_RECORDER_TRANSCRIPT_SEGMENT_WORKERS": "not-a-number",
                "LOCAL_AGENT_TIMEOUT_SECONDS": "not-a-number",
                "LOCAL_AGENT_CONNECT_TIMEOUT_SECONDS": "not-a-number",
            },
            clear=True,
        ), patch(
            "bpmis_jira_tool.config.find_dotenv",
            return_value="",
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.meeting_recorder_transcript_segment_workers, 2)
        self.assertEqual(settings.local_agent_timeout_seconds, 300)
        self.assertEqual(settings.local_agent_connect_timeout_seconds, 10)

    def test_meeting_recorder_audio_input_from_env(self):
        env = {
            "MEETING_RECORDER_AUDIO_INPUT": "MacBook Air Microphone",
        }
        with patch.dict(os.environ, env, clear=True), patch(
            "bpmis_jira_tool.config.find_dotenv",
            return_value="",
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.meeting_recorder_audio_input, "MacBook Air Microphone")

    def test_source_code_qa_codex_concurrency_from_env(self):
        with patch.dict(os.environ, {"SOURCE_CODE_QA_CODEX_CONCURRENCY": "2"}, clear=True), patch(
            "bpmis_jira_tool.config.find_dotenv",
            return_value="",
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.source_code_qa_codex_concurrency, 2)

    def test_source_code_qa_codex_chat_optimization_config_from_env(self):
        env = {
            "SOURCE_CODE_QA_CODEX_SESSION_MODE": "resume",
            "SOURCE_CODE_QA_CODEX_SESSION_MAX_TURNS": "6",
            "SOURCE_CODE_QA_CODEX_CACHE_FOLLOWUPS": "true",
        }
        with patch.dict(os.environ, env, clear=True), patch(
            "bpmis_jira_tool.config.find_dotenv",
            return_value="",
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.source_code_qa_codex_session_mode, "resume")
        self.assertEqual(settings.source_code_qa_codex_session_max_turns, 6)
        self.assertTrue(settings.source_code_qa_codex_cache_followups)

    def test_source_code_qa_model_role_overrides_from_env(self):
        env = {
            "SOURCE_CODE_QA_QUERY_REWRITE_MODEL": "rewrite-lite",
            "SOURCE_CODE_QA_PLANNER_MODEL": "planner-lite",
            "SOURCE_CODE_QA_ANSWER_MODEL": "answer-balanced",
            "SOURCE_CODE_QA_REPAIR_MODEL": "repair-deep",
            "SOURCE_CODE_QA_LLM_TIMEOUT_SECONDS": "45",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = Settings.from_env()

        self.assertEqual(settings.source_code_qa_query_rewrite_model, "rewrite-lite")
        self.assertEqual(settings.source_code_qa_planner_model, "planner-lite")
        self.assertEqual(settings.source_code_qa_answer_model, "answer-balanced")
        self.assertEqual(settings.source_code_qa_repair_model, "repair-deep")
        self.assertEqual(settings.source_code_qa_llm_timeout_seconds, 45)


if __name__ == "__main__":
    unittest.main()
