from __future__ import annotations

import unittest

from bpmis_jira_tool.report_intelligence import (
    DEFAULT_PRIORITY_KEYWORDS,
    build_daily_match_summary,
    match_report_intelligence,
    normalize_report_intelligence_config,
)


class ReportIntelligenceTests(unittest.TestCase):
    def test_default_keywords_include_regulators(self):
        config = normalize_report_intelligence_config({})

        self.assertIn("BSP", config["priority_keywords"])
        self.assertIn("OJK", config["priority_keywords"])
        self.assertIn("BSP", DEFAULT_PRIORITY_KEYWORDS)
        self.assertIn("OJK", DEFAULT_PRIORITY_KEYWORDS)

    def test_daily_summary_only_includes_matched_config_items(self):
        config = normalize_report_intelligence_config(
            {
                "vip_people": [
                    {"display_name": "Boss", "role_tags": ["直属 Boss"], "emails": ["boss@npt.sg"]},
                    {"display_name": "Unused VIP", "role_tags": ["Finance"]},
                ],
                "priority_keywords": ["BSP", "OJK", "延期"],
            }
        )
        matches = match_report_intelligence(
            "Boss asked whether BSP launch approval blocks Project Alpha BPMIS-1.",
            config=config,
            key_projects=[{"bpmis_id": "BPMIS-1", "project_name": "Project Alpha", "jira_ids": ["AF-1"]}],
        )
        summary = build_daily_match_summary(matches)

        self.assertIn("Boss", summary)
        self.assertIn("BSP", summary)
        self.assertIn("BPMIS-1", summary)
        self.assertNotIn("Unused VIP", summary)
        self.assertNotIn("OJK", summary)


if __name__ == "__main__":
    unittest.main()
