#!/usr/bin/env python3
"""Refresh PH, SG, and ID Business Insights reports in one Cloud Run Job."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT_DIR = Path(__file__).resolve().parents[1]
REFRESH_SCRIPT = ROOT_DIR / "scripts" / "refresh_business_insights_from_google_sheet.py"

PH_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1F5MSUwnxg8AbGr3rQN1l8nXYkxrBU680FJYhTGzL9qo/"
    "edit?gid=2125394335#gid=2125394335"
)
SG_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1YanQTsmi5s467uWRVRccqfJtr8xgj2kDujM4kG0r-eI/"
    "edit?gid=1061387676#gid=1061387676"
)
ID_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1423Y-u5kl24TD62yScFqatEl3tzB9RXGY6vI4cxeMOg/"
    "edit?gid=0#gid=0"
)

PH_REPORT_IDS = (
    "anti-fraud-ph-scenarios-actions-auth-steps",
    "anti-fraud-ph-rules-features",
    "anti-fraud-ph-rule-effectiveness",
    "anti-fraud-ph-fraud-loss-cases",
    "anti-fraud-ph-facial-verification",
    "anti-fraud-ph-device-identity-risk",
    "anti-fraud-ph-card-3ds",
    "anti-fraud-ph-blacklist-whitelist-greylist",
)
SG_REPORT_IDS = ("anti-fraud-sg-scenarios-auth-steps-features",)
ID_REPORT_IDS = ("anti-fraud-id-scenarios-auth-rules-features",)


def market_configs() -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    return (
        (
            "PH",
            os.getenv("BUSINESS_INSIGHTS_PH_GOOGLE_SHEET_URL", PH_SHEET_URL),
            PH_REPORT_IDS,
        ),
        (
            "SG",
            os.getenv("BUSINESS_INSIGHTS_SG_GOOGLE_SHEET_URL", SG_SHEET_URL),
            SG_REPORT_IDS,
        ),
        (
            "ID",
            os.getenv("BUSINESS_INSIGHTS_ID_GOOGLE_SHEET_URL", ID_SHEET_URL),
            ID_REPORT_IDS,
        ),
    )


def refresh_command(sheet_url: str, report_ids: tuple[str, ...]) -> list[str]:
    command = [sys.executable, str(REFRESH_SCRIPT), "--sheet-url", sheet_url]
    for report_id in report_ids:
        command.extend(("--report-id", report_id))
    return command


def main() -> int:
    environment = os.environ.copy()
    for market, sheet_url, report_ids in market_configs():
        print(f"business-insights-{market.lower()}: starting refresh", flush=True)
        completed = subprocess.run(
            refresh_command(sheet_url, report_ids),
            cwd=ROOT_DIR,
            env=environment,
            check=False,
        )
        if completed.returncode != 0:
            print(
                f"business-insights-{market.lower()}: failed with exit code {completed.returncode}; "
                "later markets were not run",
                flush=True,
            )
            return completed.returncode
        print(f"business-insights-{market.lower()}: completed", flush=True)
    print("business-insights-all: completed PH, SG, and ID refreshes", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
