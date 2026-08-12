from __future__ import annotations

import os
import subprocess
import unittest
from unittest.mock import patch

from scripts.run_business_insights_all_sheet_refresh import (
    ID_SHEET_URL,
    PH_REPORT_IDS,
    PH_SHEET_URL,
    REFRESH_SCRIPT,
    SG_REPORT_IDS,
    SG_SHEET_URL,
    main,
    market_configs,
    refresh_command,
)


class BusinessInsightsAllRefreshTests(unittest.TestCase):
    def test_market_order_and_source_configuration(self) -> None:
        self.assertEqual(
            [(market, url, report_ids) for market, url, report_ids in market_configs()],
            [
                ("PH", PH_SHEET_URL, PH_REPORT_IDS),
                ("SG", SG_SHEET_URL, SG_REPORT_IDS),
                ("ID", ID_SHEET_URL, ("anti-fraud-id-scenarios-auth-rules-features",)),
            ],
        )

    def test_market_sheet_urls_can_be_overridden_without_cross_wiring(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BUSINESS_INSIGHTS_PH_GOOGLE_SHEET_URL": "ph-url",
                "BUSINESS_INSIGHTS_SG_GOOGLE_SHEET_URL": "sg-url",
                "BUSINESS_INSIGHTS_ID_GOOGLE_SHEET_URL": "id-url",
            },
            clear=False,
        ):
            configs = market_configs()
        self.assertEqual([url for _market, url, _report_ids in configs], ["ph-url", "sg-url", "id-url"])

    def test_refresh_command_keeps_all_report_ids(self) -> None:
        command = refresh_command("sheet-url", PH_REPORT_IDS)
        self.assertEqual(command[:4], [os.fspath(__import__("sys").executable), os.fspath(REFRESH_SCRIPT), "--sheet-url", "sheet-url"])
        self.assertEqual(command.count("--report-id"), len(PH_REPORT_IDS))
        self.assertEqual(command[-1], PH_REPORT_IDS[-1])

    @patch("scripts.run_business_insights_all_sheet_refresh.subprocess.run")
    def test_one_market_failure_does_not_block_later_markets(self, run: object) -> None:
        run.side_effect = [
            subprocess.CompletedProcess([], 1),
            subprocess.CompletedProcess([], 0),
            subprocess.CompletedProcess([], 0),
        ]
        self.assertEqual(main(), 1)
        self.assertEqual(run.call_count, 3)


if __name__ == "__main__":
    unittest.main()
