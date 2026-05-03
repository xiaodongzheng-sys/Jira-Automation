from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.monthly_report import (
    MONTHLY_REPORT_BATCH_MAX_TOKENS,
    MONTHLY_REPORT_FINAL_MAX_TOKENS,
    MONTHLY_REPORT_MERGE_MAX_TOKENS,
    MonthlyReportService,
    _estimate_token_count,
    build_monthly_project_evidence_brief,
    monthly_report_markdown_to_html,
    normalize_monthly_report_template,
    resolve_monthly_report_period,
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


class _FakeGmailService:
    def __init__(self, text: str = ""):
        self.calls = []
        self.text = text

    def export_contact_thread_history_since(self, *, since, now, contact_emails, max_threads):
        self.calls.append(
            {
                "since": since,
                "now": now,
                "contact_emails": contact_emails,
                "max_threads": max_threads,
            }
        )
        return {
            "text": self.text,
            "thread_count": 1 if self.text else 0,
            "message_count": 1 if self.text else 0,
        }


class MonthlyReportTests(unittest.TestCase):
    def test_report_period_resolves_four_week_anchors(self):
        first = resolve_monthly_report_period(datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        second = resolve_monthly_report_period(datetime(2026, 5, 11, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        third = resolve_monthly_report_period(datetime(2026, 6, 8, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))

        self.assertEqual((first.start_date, first.end_date), ("2026-04-13", "2026-05-03"))
        self.assertEqual((first.scheduled_start_date, first.scheduled_end_date), ("2026-04-13", "2026-05-08"))
        self.assertEqual((second.start_date, second.end_date), ("2026-05-11", "2026-05-11"))
        self.assertEqual((second.scheduled_start_date, second.scheduled_end_date), ("2026-05-11", "2026-06-05"))
        self.assertEqual((third.start_date, third.end_date), ("2026-06-08", "2026-06-08"))
        self.assertEqual((third.scheduled_start_date, third.scheduled_end_date), ("2026-06-08", "2026-07-03"))
        self.assertEqual(first.end_exclusive.isoformat(), "2026-05-04T00:00:00+08:00")

    def test_generate_draft_uses_period_product_scope_key_projects_prd_and_vip_gmail(self):
        seatalk = _FakeSeaTalkService()
        confluence = _FakeConfluence()
        gmail = _FakeGmailService(
            "VIP Gmail thread history export\n"
            "================================================================================\n"
            "Thread 1\nSubject: AF launch approval\nBody:\nSiew Ghee approved Anti-fraud launch scope.\n"
            "================================================================================\n"
            "Thread 2\nSubject: Hiring\nBody:\nSiew Ghee discussed hiring plan.\n"
        )
        now = datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
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
            },
            {
                "team_key": "OTHER",
                "label": "Other",
                "member_emails": ["owner@npt.sg"],
                "under_prd": [
                    {
                        "bpmis_id": "BPMIS-31",
                        "project_name": "Hiring Plan",
                        "is_key_project": True,
                        "jira_tickets": [{"jira_id": "HR-1", "jira_title": "Hiring update", "pm_email": "owner@npt.sg"}],
                    }
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
                gmail_service=gmail,
                now=now,
                report_intelligence_config={
                    "vip_people": [{"display_name": "Siew Ghee", "emails": ["siewghee.kunglim@shopee.com"]}],
                    "priority_keywords": ["approval"],
                },
            )
            result = service.generate_draft(template="# Template", team_payloads=team_payloads)

        self.assertEqual(result["draft_markdown"], "# Draft")
        self.assertEqual(seatalk.calls[0]["since"].isoformat(), "2026-04-13T00:00:00+08:00")
        self.assertEqual(seatalk.calls[0]["now"].isoformat(), "2026-05-04T00:00:00+08:00")
        self.assertEqual(seatalk.calls[0]["days"], 22)
        self.assertEqual(gmail.calls[0]["since"].isoformat(), "2026-04-13T00:00:00+08:00")
        self.assertEqual(gmail.calls[0]["now"].isoformat(), "2026-05-04T00:00:00+08:00")
        self.assertEqual(gmail.calls[0]["contact_emails"], ["siewghee.kunglim@shopee.com"])
        self.assertEqual(confluence.urls, [("https://confluence/prd", "monthly-report")])
        prompts = [call.kwargs["prompt"] for call in mock_generate.call_args_list]
        prompt_modes = [call.kwargs.get("prompt_mode", "") for call in mock_generate.call_args_list]
        joined_prompts = "\n".join(prompts)
        self.assertGreaterEqual(len(prompts), 4)
        self.assertIn("Hard scope: include only Xiaodong-owned Anti-fraud, Credit Risk, Ops Risk product updates", joined_prompts)
        self.assertTrue(any("Key Fraud Project" in prompt for prompt in prompts))
        self.assertTrue(any("AF-1" in prompt for prompt in prompts))
        self.assertNotIn("AF-2", joined_prompts)
        self.assertNotIn("Not Key", joined_prompts)
        self.assertNotIn("Hiring Plan", joined_prompts)
        self.assertNotIn("Subject: Hiring", joined_prompts)
        self.assertTrue(any("PRD says rollout needs approval" in prompt for prompt in prompts))
        self.assertTrue(any("_prd_scope_summary" in mode for mode in prompt_modes))
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
        self.assertEqual(result["evidence_summary"]["vip_gmail_thread_count"], 1)
        self.assertEqual(result["evidence_summary"]["vip_gmail_message_count"], 1)
        self.assertEqual(result["evidence_summary"]["gmail_error_count"], 0)
        self.assertEqual(result["evidence_summary"]["prd_scope_summary_count"], 1)
        self.assertGreater(result["evidence_summary"]["product_scope_filtered_count"], 0)
        self.assertGreater(result["evidence_summary"]["report_intelligence_evidence_count"], 0)
        self.assertIn("Report Intelligence matched evidence", joined_prompts)
        self.assertNotIn("Today's matched VIPs", joined_prompts)
        self.assertEqual(result["subject"], "Monthly Report - 2026-04-13 to 2026-05-03")
        self.assertEqual(result["generation_summary"]["period_start"], "2026-04-13")
        self.assertEqual(result["generation_summary"]["period_end"], "2026-05-03")
        self.assertEqual(result["generation_summary"]["period_end_exclusive"], "2026-05-04T00:00:00+08:00")
        self.assertEqual(result["generation_summary"]["scheduled_period_end"], "2026-05-08")
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

    def test_key_project_cap_allows_thirty_and_excludes_thirty_first(self):
        seatalk = _FakeSeaTalkService()
        projects = [
            {
                "bpmis_id": f"BPMIS-{index:02d}",
                "project_name": f"Anti-fraud Project {index:02d}",
                "is_key_project": True,
                "priority": "P1",
                "jira_tickets": [{"jira_id": f"AF-{index}", "jira_title": "Anti-fraud scope", "jira_status": "Developing", "pm_email": "owner@npt.sg"}],
            }
            for index in range(31)
        ]
        team_payloads = [{"team_key": "AF", "label": "Anti-fraud", "member_emails": ["owner@npt.sg"], "under_prd": projects, "pending_live": []}]
        now = datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.generate_monthly_report_with_codex") as mock_generate:
            mock_generate.return_value = {"result_markdown": "# Summary", "model_id": "codex-cli", "trace": {}}
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=seatalk,
                confluence=None,
                now=now,
            )
            result = service.generate_draft(template="# Template", team_payloads=team_payloads)

        joined_prompts = "\n".join(call.kwargs["prompt"] for call in mock_generate.call_args_list)
        self.assertEqual(result["evidence_summary"]["key_project_count"], 30)
        self.assertIn("Anti-fraud Project 29", joined_prompts)
        self.assertNotIn("Anti-fraud Project 30", joined_prompts)

    def test_project_evidence_brief_groups_alc_and_requires_direct_risk_evidence(self):
        period = resolve_monthly_report_period(datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        projects = [
            {
                "bpmis_id": "180213",
                "project_name": "Support ALC v12 facial verification model upgrade",
                "teams": ["Anti-fraud"],
                "jira_tickets": [
                    {
                        "jira_id": "SPDBK-129831",
                        "jira_title": "Scenarios in live using FV auth step to switch from old to new ALCv12 model",
                        "jira_status": "Waiting",
                        "release_date": "2026-06-22T16:00:00.000Z",
                        "version": "DBPID_v3.46_0623",
                    }
                ],
            },
            {
                "bpmis_id": "SHADOW-1",
                "project_name": "Productization - Strategy Shadow Run",
                "teams": ["Credit Risk"],
                "jira_tickets": [{"jira_id": "CR-1", "jira_title": "Strategy Shadow Run"}],
            },
        ]
        brief = build_monthly_project_evidence_brief(
            key_projects=projects,
            seatalk_history_text=(
                "2026-04-27 group-4371534 bank 接入ALC v12 沟通: force-upgrade handling needs PM confirmation.\n"
                "2026-04-28 PH Credit Card / CCIC capacity needs prioritization across unrelated requests."
            ),
            vip_gmail_text="",
            prd_scope_summaries=[],
            report_period=period,
        )

        alc = next(item for item in brief if item["project_id"] == "180213")
        shadow = next(item for item in brief if item["project_id"] == "SHADOW-1")
        self.assertTrue(alc["include"])
        self.assertIn("group-4371534", alc["seatalk_group_ids"])
        self.assertGreater(alc["material_update_score"], 0)
        self.assertFalse(shadow["include"])
        self.assertEqual(shadow["risks"], [])

    def test_project_evidence_brief_excludes_project_with_name_only_jira(self):
        period = resolve_monthly_report_period(datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        brief = build_monthly_project_evidence_brief(
            key_projects=[
                {
                    "bpmis_id": "TERM-1",
                    "project_name": "Term Loan Optional Income Document Submission",
                    "teams": ["Credit Risk"],
                    "jira_tickets": [{"jira_id": "SPDBK-130111", "jira_title": "Term Loan Optional Income Document Submission"}],
                }
            ],
            seatalk_history_text="2026-04-30 PH Credit Card / CCIC capacity needs prioritization.",
            vip_gmail_text="",
            prd_scope_summaries=[],
            report_period=period,
        )

        self.assertFalse(brief[0]["include"])
        self.assertEqual(brief[0]["exclude_reason"], "No material in-period project evidence found.")

    def test_vip_gmail_failure_does_not_fail_draft(self):
        class BrokenGmailService:
            def export_contact_thread_history_since(self, **_kwargs):
                raise ToolError("Gmail unavailable")

        now = datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.generate_monthly_report_with_codex") as mock_generate:
            mock_generate.return_value = {"result_markdown": "# Summary", "model_id": "codex-cli", "trace": {}}
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=_FakeSeaTalkService(),
                confluence=None,
                gmail_service=BrokenGmailService(),
                now=now,
                report_intelligence_config={"vip_people": [{"display_name": "Boss", "emails": ["boss@npt.sg"]}]},
            )
            result = service.generate_draft(template="# Template", team_payloads=[])

        self.assertEqual(result["draft_markdown"], "# Summary")
        self.assertEqual(result["evidence_summary"]["gmail_error_count"], 1)

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
