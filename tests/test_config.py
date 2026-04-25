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

    def test_source_code_qa_gemini_key_falls_back_to_shared_gemini_key(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "shared-gemini-key", "OPENAI_API_KEY": "", "SOURCE_CODE_QA_EMBEDDING_API_KEY": ""}, clear=True):
            settings = Settings.from_env()

        self.assertEqual(settings.source_code_qa_gemini_api_key, "shared-gemini-key")
        self.assertEqual(settings.source_code_qa_llm_provider, "gemini")
        self.assertEqual(settings.source_code_qa_gemini_api_base_url, "https://generativelanguage.googleapis.com/v1beta")
        self.assertEqual(settings.source_code_qa_gemini_fast_model, "gemini-2.5-flash-lite")
        self.assertEqual(settings.source_code_qa_gemini_model, "gemini-2.5-flash")
        self.assertEqual(settings.source_code_qa_openai_api_base_url, "https://api.openai.com/v1")
        self.assertEqual(settings.source_code_qa_openai_model, "gpt-4.1-mini")
        self.assertEqual(settings.source_code_qa_embedding_model, "local-token-hybrid-v1")
        self.assertEqual(settings.source_code_qa_embedding_provider, "local_token_hybrid")
        self.assertIsNone(settings.source_code_qa_embedding_api_key)
        self.assertTrue(settings.source_code_qa_semantic_index_enabled)
        self.assertTrue(settings.source_code_qa_llm_judge_enabled)
        self.assertEqual(settings.source_code_qa_llm_timeout_seconds, 90)
        self.assertEqual(settings.source_code_qa_llm_max_retries, 2)
        self.assertEqual(settings.source_code_qa_llm_backoff_seconds, 1.0)
        self.assertEqual(settings.source_code_qa_llm_max_backoff_seconds, 8.0)

    def test_source_code_qa_model_role_overrides_from_env(self):
        env = {
            "SOURCE_CODE_QA_QUERY_REWRITE_MODEL": "rewrite-lite",
            "SOURCE_CODE_QA_PLANNER_MODEL": "planner-lite",
            "SOURCE_CODE_QA_ANSWER_MODEL": "answer-balanced",
            "SOURCE_CODE_QA_JUDGE_MODEL": "judge-lite",
            "SOURCE_CODE_QA_REPAIR_MODEL": "repair-deep",
            "SOURCE_CODE_QA_LLM_JUDGE_ENABLED": "true",
            "SOURCE_CODE_QA_LLM_TIMEOUT_SECONDS": "45",
            "SOURCE_CODE_QA_LLM_MAX_RETRIES": "4",
            "SOURCE_CODE_QA_LLM_BACKOFF_SECONDS": "0.5",
            "SOURCE_CODE_QA_LLM_MAX_BACKOFF_SECONDS": "3.5",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = Settings.from_env()

        self.assertEqual(settings.source_code_qa_query_rewrite_model, "rewrite-lite")
        self.assertEqual(settings.source_code_qa_planner_model, "planner-lite")
        self.assertEqual(settings.source_code_qa_answer_model, "answer-balanced")
        self.assertEqual(settings.source_code_qa_judge_model, "judge-lite")
        self.assertEqual(settings.source_code_qa_repair_model, "repair-deep")
        self.assertTrue(settings.source_code_qa_llm_judge_enabled)
        self.assertEqual(settings.source_code_qa_llm_timeout_seconds, 45)
        self.assertEqual(settings.source_code_qa_llm_max_retries, 4)
        self.assertEqual(settings.source_code_qa_llm_backoff_seconds, 0.5)
        self.assertEqual(settings.source_code_qa_llm_max_backoff_seconds, 3.5)


if __name__ == "__main__":
    unittest.main()
