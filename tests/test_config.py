import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.config import Settings


class ConfigTests(unittest.TestCase):
    def test_from_env_defaults_blank_cdp_url(self):
        with patch.dict(os.environ, {"BPMIS_BROWSER_CDP_URL": "   "}, clear=False):
            settings = Settings.from_env()
            self.assertEqual(settings.bpmis_browser_cdp_url, "http://127.0.0.1:9222")

    def test_from_env_loads_dotenv_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                with open(".env", "w", encoding="utf-8") as handle:
                    handle.write("TEAM_PORTAL_PORT=5111\n")
                    handle.write("BPMIS_BROWSER_CDP_URL=http://127.0.0.1:9333\n")

                with patch.dict(os.environ, {}, clear=True):
                    settings = Settings.from_env()

                self.assertEqual(settings.team_portal_port, 5111)
                self.assertEqual(settings.bpmis_browser_cdp_url, "http://127.0.0.1:9333")
            finally:
                os.chdir(original_cwd)


if __name__ == "__main__":
    unittest.main()
