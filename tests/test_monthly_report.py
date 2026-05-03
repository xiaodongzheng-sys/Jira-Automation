from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.monthly_report import (
    MONTHLY_REPORT_BATCH_MAX_TOKENS,
    MONTHLY_REPORT_FINAL_MAX_TOKENS,
    MONTHLY_REPORT_MERGE_MAX_TOKENS,
    MonthlyReportService,
    _estimate_token_count,
    monthly_report_markdown_to_html,
    normalize_monthly_report_template,
)
from bpmis_jira_tool.seatalk_dashboard import SEATALK_INSIGHTS_TIMEZONE


def _settings(temp_dir: str) -> Settings:
    return replace(
        Settings.from_env(),
        team_portal_data_dir=Path(temp_dir),
        google_oauth_client_secret_file=Path("google-client-secret.json"),
    )


class _FakeSeaTalkService:
    def __init__(self):
        self.calls = []

    def export_history_since(self, *, since, now, days):
        self.calls.append({"since": since, "now": now, "days": days})
        return "[2026-04-10] AF Group / Alice: AF launch is blocked by approval.\n"

    def _filter_system_generated_history(self, value):
        return value

    def _compact_history_for_insights(self, value, **_kwargs):
        return value


class _FakeConfluence:
    def __init__(self):
        self.urls = []

    def ingest_page(self, url, kind):
        self.urls.append((url, kind))
        return SimpleNamespace(
            title="PRD Title",
            source_url=url,
            updated_at="2026-04-10T00:00:00Z",
            page_id="123",
            sections=[SimpleNamespace(section_path="Overview", content="PRD says rollout needs approval.")],
        )


class MonthlyReportTests(unittest.TestCase):
    def test_generate_draft_uses_30_day_seatalk_key_projects_and_prd_context(self):
        seatalk = _FakeSeaTalkService()
        confluence = _FakeConfluence()
        now = datetime(2026, 4, 29, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        team_payloads = [
            {
                "team_key": "AF",
                "label": "Anti-fraud",
                "member_emails": ["owner@npt.sg"],
                "under_prd": [
                    {
                        "bpmis_id": "BPMIS-1",
                        "project_name": "Key Fraud Project",
                        "priority": "SP",
                        "is_key_project": True,
                        "jira_tickets": [
                            {
                                "jira_id": "AF-1",
                                "jira_title": "Build fraud rule",
                                "pm_email": "owner@npt.sg",
                                "description": "Jira description",
                                "prd_links": [{"url": "https://confluence/prd"}],
                            },
                            {
                                "jira_id": "AF-2",
                                "jira_title": "Other owner item",
                                "pm_email": "other@npt.sg",
                            },
                        ],
                    },
                    {
                        "bpmis_id": "BPMIS-2",
                        "project_name": "Not Key",
                        "is_key_project": False,
                        "jira_tickets": [{"jira_id": "AF-3", "pm_email": "owner@npt.sg"}],
                    },
                ],
                "pending_live": [],
            }
        ]
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.generate_monthly_report_with_codex") as mock_generate:
            mock_generate.return_value = {"result_markdown": "# Draft", "model_id": "codex-cli", "trace": {}}
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=seatalk,
                confluence=confluence,
                now=now,
            )
            result = service.generate_draft(template="# Template", team_payloads=team_payloads)

        self.assertEqual(result["draft_markdown"], "# Draft")
        self.assertEqual(seatalk.calls[0]["since"].isoformat(), "2026-03-30T10:00:00+08:00")
        self.assertEqual(seatalk.calls[0]["days"], 32)
        self.assertEqual(confluence.urls, [("https://confluence/prd", "monthly-report")])
        prompts = [call.kwargs["prompt"] for call in mock_generate.call_args_list]
        prompt_modes = [call.kwargs.get("prompt_mode", "") for call in mock_generate.call_args_list]
        joined_prompts = "\n".join(prompts)
        self.assertGreaterEqual(len(prompts), 4)
        self.assertTrue(any("Key Fraud Project" in prompt for prompt in prompts))
        self.assertTrue(any("AF-1" in prompt for prompt in prompts))
        self.assertNotIn("AF-2", joined_prompts)
        self.assertNotIn("Not Key", joined_prompts)
        self.assertTrue(any("PRD says rollout needs approval" in prompt for prompt in prompts))
        batch_prompts = [
            call.kwargs["prompt"]
            for call in mock_generate.call_args_list
            if "_batch_" in call.kwargs.get("prompt_mode", "")
        ]
        self.assertLessEqual(max(_estimate_token_count(prompt) for prompt in batch_prompts), MONTHLY_REPORT_BATCH_MAX_TOKENS)
        merge_prompt = prompts[prompt_modes.index("v1_team_dashboard_monthly_report_merge")]
        self.assertLessEqual(_estimate_token_count(merge_prompt), MONTHLY_REPORT_MERGE_MAX_TOKENS)
        self.assertLessEqual(_estimate_token_count(prompts[-1]), MONTHLY_REPORT_FINAL_MAX_TOKENS)
        self.assertEqual(result["evidence_summary"]["key_project_count"], 1)
        self.assertEqual(result["evidence_summary"]["jira_ticket_count"], 1)
        self.assertGreater(result["evidence_summary"]["report_intelligence_evidence_count"], 0)
        self.assertIn("Report Intelligence matched evidence", joined_prompts)
        self.assertNotIn("Today's matched VIPs", joined_prompts)
        self.assertGreater(result["generation_summary"]["prompt_chars"], 0)
        self.assertGreater(result["generation_summary"]["estimated_prompt_tokens"], 0)
        self.assertEqual(result["generation_summary"]["token_risk"], "normal")
        self.assertTrue(result["generation_summary"]["batch_mode"])
        self.assertGreaterEqual(result["generation_summary"]["total_batches"], 3)
        self.assertIn("elapsed_seconds", result["generation_summary"])

    def test_generate_draft_splits_large_seatalk_history_into_multiple_batches(self):
        seatalk = _FakeSeaTalkService()
        seatalk.export_history_since = lambda **_kwargs: "\n".join(
            f"[2026-04-{(index % 28) + 1:02d}] AF Group / Alice: launch update {index} " + ("x" * 500)
            for index in range(500)
        )
        now = datetime(2026, 4, 29, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.generate_monthly_report_with_codex") as mock_generate:
            mock_generate.return_value = {"result_markdown": "# Summary", "model_id": "codex-cli", "trace": {}}
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=seatalk,
                confluence=None,
                now=now,
            )
            result = service.generate_draft(template="# Template", team_payloads=[])

        seatalk_batch_calls = [
            call for call in mock_generate.call_args_list
            if call.kwargs.get("prompt_mode", "").endswith("_batch_seatalk")
        ]
        self.assertGreater(len(seatalk_batch_calls), 1)
        for call in seatalk_batch_calls:
            self.assertLessEqual(_estimate_token_count(call.kwargs["prompt"]), MONTHLY_REPORT_BATCH_MAX_TOKENS)
        self.assertGreater(result["generation_summary"]["total_batches"], 1)

    def test_template_normalization_and_markdown_html(self):
        self.assertIn("Monthly Report", normalize_monthly_report_template(""))
        html = monthly_report_markdown_to_html("# Report\n- **Done** `AF-1`")
        self.assertIn("<strong>Done</strong>", html)
        self.assertIn("<code>AF-1</code>", html)
        table_html = monthly_report_markdown_to_html(
            "## Updates\n"
            "| Region | Priority | Project | Current Status | Target Tech Live Date |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| SG | SP | Multi-Currency Account | Dev | July 2026 |\n"
            "| PH | P0 | Incoming Transaction Hold | Dev | Support Reject: May 2026 |\n"
        )
        self.assertIn("<table", table_html)
        self.assertIn("<th", table_html)
        self.assertIn("<td", table_html)
        self.assertIn("Multi-Currency Account", table_html)
        self.assertIn("Support Reject: May 2026", table_html)


if __name__ == "__main__":
    unittest.main()
