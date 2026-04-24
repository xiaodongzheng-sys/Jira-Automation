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
        with patch.dict(os.environ, {"GEMINI_API_KEY": "shared-gemini-key"}, clear=True):
            settings = Settings.from_env()

        self.assertEqual(settings.source_code_qa_gemini_api_key, "shared-gemini-key")
        self.assertEqual(settings.source_code_qa_gemini_fast_model, "gemini-2.5-flash-lite")
        self.assertEqual(settings.source_code_qa_gemini_model, "gemini-2.5-flash")


if __name__ == "__main__":
    unittest.main()
