import unittest
from unittest.mock import patch

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS
from tests import test_source_code_qa_market_teams as market_fixtures


class DigitalOpsTests(unittest.TestCase):
    setUp = market_fixtures.MarketTeamTests.setUp
    login = staticmethod(market_fixtures.MarketTeamTests.login)
    seed = market_fixtures.MarketTeamTests.seed

    def test_option_and_country_validation_are_source_code_only(self):
        options = {item["code"]: item for item in self.service.options_payload()["pm_teams"]}
        self.assertEqual(options["DIGITAL_OPS"]["label"], "Digital Ops")
        self.assertTrue(options["DIGITAL_OPS"]["all_country_only"])
        self.assertNotIn("DIGITAL_OPS", TEAM_PROFILE_DEFAULTS)
        self.assertEqual(self.service.mapping_key("DIGITAL_OPS", "All"), "DIGITAL_OPS:All")
        for country in ("SG", "ID", "PH"):
            with self.subTest(country=country), self.assertRaises(ToolError):
                self.service.mapping_key("DIGITAL_OPS", country)

    def test_admin_mapping_is_independent_and_can_sync(self):
        origin = self.seed("AF")
        af_before = self.service.load_config()["mappings"]["AF:All"]
        with self.app.test_client() as client:
            self.login(client)
            page = client.get("/source-code-qa")
            self.assertIn(b'value="DIGITAL_OPS">Digital Ops</option>', page.data)
            payload = {"pm_team": "DIGITAL_OPS", "country": "All", "repositories": [
                {"display_name": "Digital Ops Repo", "url": "https://gitlab.npt.seabank.io/digital-ops/repo.git"}
            ]}
            result = client.post("/api/source-code-qa/config", json=payload)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json["key"], "DIGITAL_OPS:All")
            payload["country"] = "SG"
            self.assertEqual(client.post("/api/source-code-qa/config", json=payload).status_code, 400)
            self.login(client, "weilin.ang@npt.sg")
            payload["country"] = "All"
            self.assertEqual(client.post("/api/source-code-qa/config", json=payload).status_code, 403)
            self.assertEqual(client.post("/api/source-code-qa/sync", json=payload).status_code, 403)
        self.assertEqual(self.service.load_config()["mappings"]["AF:All"], af_before)
        with patch.object(self.service, "_authenticated_git_url", return_value=str(origin)):
            result = self.service.sync(pm_team="DIGITAL_OPS", country="All")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["key"], "DIGITAL_OPS:All")
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(self.service.load_config()["mappings"]["AF:All"], af_before)


if __name__ == "__main__":
    unittest.main()
