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
    MONTHLY_REPORT_SEATALK_HIGHLIGHT_CONVERSATION_SCOPE,
    MonthlyReportService,
    _estimate_token_count,
    build_monthly_highlight_deep_evidence,
    build_monthly_highlight_evidence_map,
    build_monthly_project_evidence_brief,
    build_monthly_report_final_prompt,
    generate_monthly_report_with_codex,
    match_monthly_report_highlight_topics,
    monthly_report_markdown_to_html,
    normalize_monthly_report_highlight_topics,
    normalize_monthly_report_template,
    resolve_monthly_report_period_from_user_range,
    resolve_monthly_report_period,
    _sanitize_monthly_report_output,
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

    def export_history_since(self, *, since, now, days, conversation_scope=None):
        self.calls.append({"since": since, "now": now, "days": days, "conversation_scope": conversation_scope})
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
    def __init__(
        self,
        text: str = "",
        drive_links: list[str] | None = None,
        google_sheet_evidence: list[dict[str, str]] | None = None,
        monthly_requirements_text: str = "",
    ):
        self.calls = []
        self.text = text
        self.drive_links = drive_links or []
        self.google_sheet_evidence = google_sheet_evidence or []
        self.monthly_requirements_text = monthly_requirements_text

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

    def export_monthly_requirements_thread_history_since(self, *, since, now, configs, max_threads):
        self.calls.append(
            {
                "since": since,
                "now": now,
                "monthly_requirements_configs": configs,
                "max_threads": max_threads,
            }
        )
        return {
            "text": self.monthly_requirements_text,
            "thread_count": 1 if self.monthly_requirements_text else 0,
            "message_count": 1 if self.monthly_requirements_text else 0,
        }

    def export_google_sheet_link_texts(self, links, *, max_links=4):
        self.calls.append({"google_sheet_links": list(links), "max_links": max_links})
        return self.google_sheet_evidence

    def export_topic_thread_history_since(self, *, since, now, topic, max_threads):
        self.calls.append(
            {
                "since": since,
                "now": now,
                "topic": topic,
                "max_threads": max_threads,
            }
        )
        return {
            "text": self.text,
            "thread_count": 1 if self.text else 0,
            "message_count": 1 if self.text else 0,
            "drive_links": self.drive_links,
        }


class MonthlyReportTests(unittest.TestCase):
    def test_codex_generation_uses_monthly_report_timeout(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.CodexCliBridgeSourceCodeQALLMProvider") as mock_provider:
            instance = mock_provider.return_value
            instance.generate.return_value = SimpleNamespace(payload={"text": "# Draft"}, model="codex-cli")
            instance.extract_text.return_value = "# Draft"

            result = generate_monthly_report_with_codex(
                prompt="Summarize monthly work.",
                settings=replace(_settings(temp_dir), monthly_report_codex_timeout_seconds=777),
                workspace_root=Path(temp_dir),
            )

        self.assertEqual(result["result_markdown"], "# Draft")
        self.assertEqual(mock_provider.call_args.kwargs["timeout_seconds"], 777)

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

    def test_highlight_topics_and_user_date_range_are_validated(self):
        self.assertEqual(normalize_monthly_report_highlight_topics([" AF ", "", "CRMS", "AF"]), ["AF", "CRMS"])
        with self.assertRaises(ToolError):
            normalize_monthly_report_highlight_topics([])
        self.assertEqual(
            normalize_monthly_report_highlight_topics(["one", "two", "three", "four", "five", "six"]),
            ["one", "two", "three", "four", "five", "six"],
        )
        with self.assertRaises(ToolError):
            normalize_monthly_report_highlight_topics(["one", "two", "three", "four", "five", "six", "seven"])

        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-01", period_end="2026-04-30")
        self.assertEqual(period.start.isoformat(), "2026-04-01T00:00:00+08:00")
        self.assertEqual(period.end_date, "2026-04-30")
        self.assertEqual(period.end_exclusive.isoformat(), "2026-05-01T00:00:00+08:00")
        with self.assertRaises(ToolError):
            resolve_monthly_report_period_from_user_range(period_start="2026-05-01", period_end="2026-04-30")

    def test_highlight_gmail_topic_cache_reuses_results_and_preserves_topic_order(self):
        report_period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        gmail = _FakeGmailService("Thread\nBody: AF and GRC update.")
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=_FakeSeaTalkService(),
                gmail_service=gmail,
            )

            first_items, first_summary = service._highlight_gmail_history(report_period, ["GRC Phase 1", "AF CIB"])
            second_items, second_summary = service._highlight_gmail_history(report_period, ["GRC Phase 1", "AF CIB"])

        self.assertEqual([item["topic"] for item in first_items], ["GRC Phase 1", "AF CIB"])
        self.assertEqual([item["topic"] for item in second_items], ["GRC Phase 1", "AF CIB"])
        self.assertEqual(len([call for call in gmail.calls if "topic" in call]), 2)
        self.assertEqual(first_summary["cache_hit_count"], 0)
        self.assertEqual(second_summary["cache_hit_count"], 2)

    def test_prd_scope_summary_cache_reuses_codex_result_and_invalidates_on_updated_at(self):
        report_period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        source = {
            "jira_id": "AF-1",
            "title": "AF PRD",
            "url": "https://confluence/prd",
            "updated_at": "2026-04-10T00:00:00Z",
            "content": "PRD scope",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=_FakeSeaTalkService(),
            )
            with patch.object(
                service,
                "_guarded_generate",
                return_value={"result_markdown": "Cached PRD summary"},
            ) as mock_generate:
                first = service._prd_scope_summaries(
                    prd_sources=[source],
                    generated_at=datetime(2026, 5, 8, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                    report_period=report_period,
                    progress_callback=None,
                )
                second = service._prd_scope_summaries(
                    prd_sources=[source],
                    generated_at=datetime(2026, 5, 8, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                    report_period=report_period,
                    progress_callback=None,
                )
                changed = dict(source, updated_at="2026-04-11T00:00:00Z")
                service._prd_scope_summaries(
                    prd_sources=[changed],
                    generated_at=datetime(2026, 5, 8, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                    report_period=report_period,
                    progress_callback=None,
                )

        self.assertEqual(mock_generate.call_count, 2)
        self.assertEqual(first[0]["scope_summary"], "Cached PRD summary")
        self.assertFalse(first[0]["cache_hit"])
        self.assertTrue(second[0]["cache_hit"])

    def test_batch_summary_cache_reuses_codex_result_for_same_evidence_fingerprint(self):
        report_period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        payload = [{"topic": "AF launch", "seatalk_evidence": ["AF launch owner confirmed."]}]
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=_FakeSeaTalkService(),
            )
            with patch.object(
                service,
                "_guarded_generate",
                return_value={"result_markdown": "Cached batch summary", "model_id": "codex-cli", "trace": {"id": "trace-1"}},
            ) as mock_generate:
                first = service._batch_summaries(
                    template="# Template",
                    generated_at=datetime(2026, 5, 8, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                    report_period=report_period,
                    highlight_topics=["AF launch"],
                    monthly_evidence_brief=[],
                    highlight_deep_evidence=payload,
                    prd_errors=[],
                    evidence_sidecar=[],
                    progress_callback=None,
                )
                second = service._batch_summaries(
                    template="# Template",
                    generated_at=datetime(2026, 5, 9, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                    report_period=report_period,
                    highlight_topics=["AF launch"],
                    monthly_evidence_brief=[],
                    highlight_deep_evidence=payload,
                    prd_errors=[],
                    evidence_sidecar=[],
                    progress_callback=None,
                )

        self.assertEqual(mock_generate.call_count, 1)
        self.assertEqual(first[0]["summary_markdown"], "Cached batch summary")
        self.assertFalse(first[0]["cache_hit"])
        self.assertTrue(second[0]["cache_hit"])
        self.assertEqual(second[0]["summary_markdown"], "Cached batch summary")

    def test_generate_draft_uses_period_product_scope_key_projects_prd_and_vip_gmail(self):
        seatalk = _FakeSeaTalkService()
        confluence = _FakeConfluence()
        gmail = _FakeGmailService(
            "VIP Gmail thread history export\n"
            "================================================================================\n"
            "Thread 1\nSubject: AF launch approval\nBody:\nSiew Ghee approved Anti-fraud launch scope.\n"
            "================================================================================\n"
            "Thread 2\nSubject: Hiring\nBody:\nSiew Ghee discussed hiring plan.\n",
            drive_links=["https://docs.google.com/spreadsheets/d/sheet123/edit"],
            google_sheet_evidence=[
                {
                    "title": "AF Launch Sheet",
                    "text": "Sheet confirms Anti-fraud launch dependency and management status.",
                    "access_status": "ok",
                    "url": "https://docs.google.com/spreadsheets/d/sheet123/edit",
                }
            ],
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
            progress_events = []
            result = service.generate_draft(
                template="# Template",
                team_payloads=team_payloads,
                highlight_topics=["Key Fraud Project", "GRC Phase 1"],
                progress_callback=lambda stage, message, current, total, **kwargs: progress_events.append(
                    {
                        "stage": stage,
                        "message": message,
                        "current": current,
                        "total": total,
                        **kwargs,
                    }
                ),
            )

        self.assertEqual(result["draft_markdown"], "# Draft")
        self.assertEqual(seatalk.calls[0]["since"].isoformat(), "2026-04-20T00:00:00+08:00")
        self.assertEqual(seatalk.calls[0]["now"].isoformat(), "2026-05-04T00:00:00+08:00")
        self.assertEqual(seatalk.calls[0]["days"], 15)
        self.assertEqual(seatalk.calls[0]["conversation_scope"], MONTHLY_REPORT_SEATALK_HIGHLIGHT_CONVERSATION_SCOPE)
        self.assertEqual(gmail.calls[0]["since"].isoformat(), "2026-04-20T00:00:00+08:00")
        self.assertEqual(gmail.calls[0]["now"].isoformat(), "2026-05-04T00:00:00+08:00")
        self.assertEqual(gmail.calls[0]["contact_emails"], ["siewghee.kunglim@shopee.com"])
        topic_calls = [call for call in gmail.calls if "topic" in call]
        self.assertCountEqual([call["topic"] for call in topic_calls], ["Key Fraud Project", "GRC Phase 1"])
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
        self.assertTrue(any("google_sheet_links" in call for call in gmail.calls))
        self.assertTrue(any("Sheet confirms Anti-fraud launch dependency" in prompt for prompt in prompts))
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
        self.assertEqual(result["evidence_summary"]["highlight_google_sheet_count"], 2)
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
        self.assertEqual(result["generation_summary"]["evidence_period_start"], "2026-04-20")
        self.assertEqual(result["generation_summary"]["evidence_period_end"], "2026-05-03")
        self.assertEqual(result["generation_summary"]["seatalk_conversation_scope"], MONTHLY_REPORT_SEATALK_HIGHLIGHT_CONVERSATION_SCOPE)
        self.assertEqual(result["generation_summary"]["highlight_topics"], ["Key Fraud Project", "GRC Phase 1"])
        progress_stages = [item["stage"] for item in progress_events]
        for stage in (
            "preparing_sources",
            "collecting_seatalk",
            "searching_vip_gmail",
            "searching_requirements_gmail",
            "searching_topic_gmail",
            "ingesting_prd",
            "summarizing_prd_scope",
            "building_evidence",
            "merging_summaries",
            "generating_final_draft",
        ):
            self.assertIn(stage, progress_stages)
        self.assertTrue(any(item["stage"] == "searching_topic_gmail" and item["total"] == 2 for item in progress_events))
        self.assertEqual(result["evidence_summary"]["highlight_topic_count"], 2)
        self.assertEqual(result["evidence_summary"]["highlight_project_topic_count"], 1)
        self.assertIn("highlight_confidence_counts", result["evidence_summary"])
        self.assertEqual(len(result["highlight_evidence_map"]), 2)
        self.assertTrue({item["topic"] for item in result["highlight_evidence_map"]}.issuperset({"Key Fraud Project", "GRC Phase 1"}))
        self.assertIn("batch_summary_cache_hit_count", result["generation_summary"])
        self.assertEqual(result["generation_summary"]["scheduled_period_end"], "2026-05-08")
        self.assertGreater(result["generation_summary"]["prompt_chars"], 0)
        self.assertGreater(result["generation_summary"]["estimated_prompt_tokens"], 0)
        self.assertEqual(result["generation_summary"]["token_risk"], "normal")
        self.assertTrue(result["generation_summary"]["batch_mode"])
        self.assertGreaterEqual(result["generation_summary"]["total_batches"], 3)
        self.assertIn("elapsed_seconds", result["generation_summary"])
        for key in ("seatalk_export", "vip_gmail", "requirements_gmail", "topic_gmail", "prd_ingest", "prd_summary", "batch_summary", "merge", "final", "total"):
            self.assertIn(key, result["generation_summary"]["timings"])

    def test_generate_draft_does_not_batch_full_seatalk_history_for_non_project_highlight(self):
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
            result = service.generate_draft(template="# Template", team_payloads=[], highlight_topics=["AF launch"])

        seatalk_batch_calls = [
            call for call in mock_generate.call_args_list
            if call.kwargs.get("prompt_mode", "").endswith("_batch_seatalk")
        ]
        self.assertEqual(seatalk_batch_calls, [])
        self.assertGreaterEqual(result["generation_summary"]["total_batches"], 1)

    def test_generate_draft_preserves_highlight_seatalk_topic_matches_outside_product_scope(self):
        seatalk = _FakeSeaTalkService()
        seatalk.export_history_since = lambda **_kwargs: "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== ID Ops Room ===",
                "[2026-04-25 10:01:00] Alice: generic implementation note should not matter.",
                "[2026-04-25 10:02:00] Bob: database capacity issue triggered system downgrade last night.",
                "[2026-04-25 10:03:00] Xiaodong Zheng: confirm impact and follow up actions by today.",
                "[2026-04-25 10:04:00] Alice: unrelated office move update.",
            ]
        )
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.generate_monthly_report_with_codex") as mock_generate:
            mock_generate.return_value = {"result_markdown": "# Draft", "model_id": "codex-cli", "trace": {}}
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=seatalk,
                confluence=_FakeConfluence(),
                gmail_service=_FakeGmailService(),
                now=datetime(2026, 5, 9, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            )

            result = service.generate_draft(
                template="# Template",
                team_payloads=[],
                highlight_topics=["ID database capacity issue impact and follow up actions"],
                period_start="2026-04-13",
                period_end="2026-05-08",
            )

        joined_prompts = "\n".join(call.kwargs["prompt"] for call in mock_generate.call_args_list)
        self.assertIn("database capacity issue triggered system downgrade", joined_prompts)
        self.assertIn("confirm impact and follow up actions", joined_prompts)
        self.assertGreater(result["evidence_summary"]["highlight_seatalk_raw_match_count"], 0)
        self.assertGreater(result["evidence_summary"]["highlight_seatalk_line_match_count"], 0)

    def test_generate_draft_splits_large_project_evidence_brief_batches(self):
        seatalk = _FakeSeaTalkService()
        projects = []
        for project_index in range(30):
            projects.append(
                {
                    "bpmis_id": f"BPMIS-{project_index:02d}",
                    "project_name": f"Anti-fraud Evidence Heavy Project {project_index:02d}",
                    "is_key_project": True,
                    "priority": "P0",
                    "market": "SG",
                    "jira_tickets": [
                        {
                            "jira_id": f"AF-{project_index:02d}-{ticket_index:02d}",
                            "jira_title": "Anti-fraud launch readiness and control validation " + ("scope detail " * 18),
                            "jira_status": "In UAT",
                            "release_date": "2026-05-20T16:00:00.000Z",
                            "version": f"AF_v{project_index}_{ticket_index}",
                            "pm_email": "owner@npt.sg",
                        }
                        for ticket_index in range(18)
                    ],
                }
            )
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
            result = service.generate_draft(template="# Template", team_payloads=team_payloads, highlight_topics=["AF launch"])

        evidence_batch_calls = [
            call for call in mock_generate.call_args_list
            if call.kwargs.get("prompt_mode", "").endswith("_batch_monthly_evidence_brief")
        ]
        self.assertGreater(len(evidence_batch_calls), 1)
        for call in evidence_batch_calls:
            self.assertLessEqual(_estimate_token_count(call.kwargs["prompt"]), MONTHLY_REPORT_BATCH_MAX_TOKENS)
        self.assertEqual(result["evidence_summary"]["key_project_count"], 30)

    def test_generate_draft_does_not_batch_full_vip_gmail_threads(self):
        gmail = _FakeGmailService(
            "\n".join(
                f"Thread {index} Subject: Anti-fraud launch approval Body: " + ("approval rollout evidence " * 260)
                for index in range(36)
            )
        )
        now = datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.generate_monthly_report_with_codex") as mock_generate:
            mock_generate.return_value = {"result_markdown": "# Summary", "model_id": "codex-cli", "trace": {}}
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=_FakeSeaTalkService(),
                confluence=None,
                gmail_service=gmail,
                now=now,
                report_intelligence_config={"vip_people": [{"display_name": "Boss", "emails": ["boss@npt.sg"]}]},
            )
            service.generate_draft(template="# Template", team_payloads=[], highlight_topics=["AF launch"])

        gmail_batch_calls = [
            call for call in mock_generate.call_args_list
            if call.kwargs.get("prompt_mode", "").endswith("_batch_vip_gmail")
        ]
        self.assertEqual(gmail_batch_calls, [])
        highlight_calls = [
            call for call in mock_generate.call_args_list
            if call.kwargs.get("prompt_mode", "").endswith("_batch_highlight_deep_evidence")
        ]
        self.assertGreaterEqual(len(highlight_calls), 1)
        for call in highlight_calls:
            self.assertLessEqual(_estimate_token_count(call.kwargs["prompt"]), MONTHLY_REPORT_BATCH_MAX_TOKENS)

    def test_final_prompt_compacts_included_project_evidence(self):
        period = resolve_monthly_report_period(datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        evidence = []
        for project_index in range(30):
            evidence.append(
                {
                    "include": True,
                    "project_id": f"BPMIS-{project_index:02d}",
                    "bpmis_id": f"BPMIS-{project_index:02d}",
                    "project_name": f"Anti-fraud Evidence Heavy Project {project_index:02d}",
                    "product_area": "Anti-fraud",
                    "market": "SG",
                    "priority": "P0",
                    "jira_ids": [f"AF-{project_index:02d}-{ticket_index:02d}" for ticket_index in range(40)],
                    "seatalk_group_ids": [f"group-{ticket_index}" for ticket_index in range(20)],
                    "material_update_score": 9,
                    "status_facts": ["UAT validation completed " + ("status detail " * 80) for _ in range(20)],
                    "timeline_facts": ["Release planned for 2026-05-20 " + ("timeline detail " * 80) for _ in range(20)],
                    "risks": ["Approval dependency remains open " + ("risk detail " * 80) for _ in range(20)],
                    "decisions_needed": ["Confirm live rollout owner " + ("decision detail " * 80) for _ in range(20)],
                    "matched_prd_summaries": ["PRD scope summary " + ("prd detail " * 180) for _ in range(8)],
                    "aliases": ["alias " * 240 for _ in range(40)],
                    "matched_seatalk_messages": ["seatalk raw transcript " * 240 for _ in range(12)],
                    "matched_vip_gmail_threads": ["gmail raw thread " * 240 for _ in range(12)],
                    "evidence_sources": {"seatalk": ["raw source " * 240 for _ in range(12)]},
                }
            )
        evidence.append(
            {
                "include": False,
                "project_id": "BPMIS-EXCLUDED",
                "project_name": "Excluded Project",
                "matched_seatalk_messages": ["excluded raw transcript " * 100],
            }
        )

        prompt = build_monthly_report_final_prompt(
            template="# Template",
            generated_at=datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            report_period=period,
            evidence_brief="Compact brief",
            monthly_evidence_brief=evidence,
            highlight_topics=["CIB Phase 2"],
        )

        self.assertLessEqual(_estimate_token_count(prompt), MONTHLY_REPORT_FINAL_MAX_TOKENS)
        self.assertIn("BPMIS-00", prompt)
        self.assertIn("Anti-fraud Evidence Heavy Project 29", prompt)
        self.assertNotIn("BPMIS-EXCLUDED", prompt)
        self.assertNotIn("aliases", prompt)
        self.assertNotIn("matched_seatalk_messages", prompt)
        self.assertNotIn("matched_vip_gmail_threads", prompt)
        self.assertNotIn("evidence_sources", prompt)
        self.assertNotIn("AF-00-00", prompt)
        self.assertIn("Highlight Deep Evidence", prompt)
        self.assertIn("Other Key Project Updates", prompt)
        self.assertIn('"current_status"', prompt)
        self.assertIn("The audience is Xiaodong's manager", prompt)
        self.assertIn("executive product update", prompt)
        self.assertIn("Do not expose raw evidence mechanics in Highlights", prompt)
        self.assertIn("confidence/recommended_tone", prompt)
        self.assertIn("no confirmed evidence", prompt)
        self.assertIn("pending confirmation", prompt)

    def test_highlight_topic_matching_and_deep_evidence_layers_sources(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        projects = [
            {
                "bpmis_id": "BPMIS-1",
                "project_name": "Anti-Fraud CIB Phase 2",
                "market": "SG",
                "priority": "SP",
                "jira_tickets": [
                    {
                        "jira_id": "AF-100",
                        "jira_title": "CIB Phase 2 transfer rule",
                        "jira_status": "Developing",
                        "release_date": "2026-05-20",
                    }
                ],
            },
            {
                "bpmis_id": "BPMIS-2",
                "project_name": "Non Highlight Ops Update",
                "market": "ID",
                "priority": "P1",
                "jira_tickets": [{"jira_id": "OPS-1", "jira_title": "Ops email", "jira_status": "Waiting"}],
            },
        ]
        matches = match_monthly_report_highlight_topics(["CIB Phase 2", "General Risk Narrative"], projects)
        self.assertEqual(matches[0]["project_ids"], ["BPMIS-1"])
        self.assertEqual(matches[1]["project_ids"], [])

        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=["CIB Phase 2", "General Risk Narrative"],
            key_projects=projects,
            topic_project_matches=matches,
            seatalk_history_text=(
                "2026-04-20 group-4371534 CIB Phase 2 needs launch confirmation.\n"
                "2026-04-21 Non Highlight Ops Update should stay light."
            ),
            topic_gmail_evidence=[
                {
                    "topic": "CIB Phase 2",
                    "text": "Gmail topic thread history export\n================================================================================\nThread 1\nSubject: CIB Phase 2\nBody:\nCIB Phase 2 release owner confirmed.",
                    "thread_count": 1,
                    "message_count": 1,
                },
                {
                    "topic": "General Risk Narrative",
                    "text": "Gmail topic thread history export\n================================================================================\nThread 1\nSubject: General Risk Narrative\nBody:\nRisk narrative was discussed.",
                    "thread_count": 1,
                    "message_count": 1,
                },
            ],
            prd_scope_summaries=[
                {
                    "jira_id": "AF-100",
                    "scope_summary": "PRD says CIB Phase 2 covers transfer and payment rules.",
                }
            ],
            report_period=period,
        )

        self.assertEqual(deep[0]["topic_type"], "project_update")
        self.assertEqual(deep[0]["project_updates"][0]["current_status"], "Dev")
        self.assertTrue(any("PRD says CIB Phase 2" in item for item in deep[0]["prd_scope_summaries"]))
        self.assertTrue(any("group-4371534" in item for item in deep[0]["seatalk_evidence"]))
        self.assertTrue(any("release owner confirmed" in item for item in deep[0]["gmail_evidence"]))
        self.assertEqual(deep[0]["confidence"], "high")
        self.assertEqual(deep[0]["evidence_map"]["source_counts"]["seatalk"], 1)
        self.assertIn("Write as a confident executive progress update", deep[0]["recommended_tone"])
        self.assertEqual(deep[1]["topic_type"], "general_topic")
        self.assertEqual(deep[1]["project_updates"], [])
        self.assertEqual(deep[1]["confidence"], "low")
        evidence_map = build_monthly_highlight_evidence_map(deep)
        self.assertEqual([item["topic"] for item in evidence_map], ["CIB Phase 2", "General Risk Narrative"])
        self.assertEqual(evidence_map[0]["confidence"], "high")

    def test_non_highlight_project_evidence_stays_light(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        brief = build_monthly_project_evidence_brief(
            key_projects=[
                {
                    "bpmis_id": "BPMIS-1",
                    "project_name": "Highlight Project",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [{"jira_id": "AF-1", "jira_title": "Highlight", "jira_status": "Developing"}],
                },
                {
                    "bpmis_id": "BPMIS-2",
                    "project_name": "Other Project",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [{"jira_id": "AF-2", "jira_title": "Other", "jira_status": "Developing"}],
                },
            ],
            seatalk_history_text="2026-04-20 Other Project has SeaTalk evidence.",
            vip_gmail_text="VIP Gmail thread history export\n================================================================================\nThread 1\nSubject: Other Project\nBody:\nOther Project VIP update.",
            prd_scope_summaries=[{"jira_id": "AF-2", "scope_summary": "Other Project PRD details."}],
            report_period=period,
            highlight_project_ids={"BPMIS-1"},
        )

        by_id = {item["project_id"]: item for item in brief}
        self.assertEqual(by_id["BPMIS-2"]["matched_seatalk_messages"], [])
        self.assertEqual(by_id["BPMIS-2"]["matched_prd_summaries"], [])
        self.assertTrue(by_id["BPMIS-2"]["matched_vip_gmail_threads"])

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
            result = service.generate_draft(template="# Template", team_payloads=team_payloads, highlight_topics=["AF launch"])

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
        self.assertTrue(shadow["include"])
        self.assertEqual(shadow["current_status"], "BRD")
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

        self.assertTrue(brief[0]["include"])
        self.assertEqual(brief[0]["current_status"], "BRD")

    def test_project_evidence_brief_normalizes_current_status(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-01", period_end="2026-04-30")
        brief = build_monthly_project_evidence_brief(
            key_projects=[
                {
                    "bpmis_id": "WAITING",
                    "project_name": "Anti-fraud Waiting",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [{"jira_id": "AF-1", "jira_title": "Waiting item", "jira_status": "Waiting"}],
                },
                {
                    "bpmis_id": "PRD",
                    "project_name": "Anti-fraud PRD",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [{"jira_id": "AF-2", "jira_title": "PRD item", "jira_status": "PRD Reviewed"}],
                },
                {
                    "bpmis_id": "DEV",
                    "project_name": "Anti-fraud Dev",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [{"jira_id": "AF-3", "jira_title": "Dev item", "jira_status": "Tech Design"}],
                },
                {
                    "bpmis_id": "UAT",
                    "project_name": "Anti-fraud UAT",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [{"jira_id": "AF-4", "jira_title": "UAT item", "jira_status": "Pen Test", "version": "AF UAT wave"}],
                },
                {
                    "bpmis_id": "RELEASED",
                    "project_name": "Anti-fraud Released",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [{"jira_id": "AF-5", "jira_title": "Released item", "jira_status": "Waiting", "release_date": "2026-04-15"}],
                },
                {
                    "bpmis_id": "TARGET",
                    "project_name": "Anti-fraud Target Date",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [
                        {
                            "jira_id": "AF-6",
                            "jira_title": "Planning item",
                            "jira_status": "Testing",
                            "release_date": "2026-12-31",
                            "version": "Planning_26Q4",
                        },
                        {
                            "jira_id": "AF-7",
                            "jira_title": "Earlier tech live",
                            "jira_status": "Testing",
                            "release_date": "2026-05-15",
                            "version": "AF_v1.0_0515",
                        },
                        {
                            "jira_id": "AF-8",
                            "jira_title": "Latest tech live",
                            "jira_status": "Testing",
                            "release_date": "2026-06-20",
                            "version": "AF_v1.1_0620",
                        },
                    ],
                },
            ],
            seatalk_history_text="",
            vip_gmail_text="",
            prd_scope_summaries=[],
            report_period=period,
        )

        by_id = {item["project_id"]: item["current_status"] for item in brief}
        self.assertEqual(by_id["WAITING"], "BRD")
        self.assertEqual(by_id["PRD"], "PRD")
        self.assertEqual(by_id["DEV"], "Dev")
        self.assertEqual(by_id["UAT"], "UAT")
        self.assertEqual(by_id["RELEASED"], "UAT")
        target = next(item for item in brief if item["project_id"] == "TARGET")
        self.assertEqual(target["target_tech_live_date"], "Jun 2026")
        self.assertEqual(target["target_tech_live_version"], "AF_v1.1_0620")
        self.assertFalse(any("Planning_26Q4" in fact for fact in target["timeline_facts"]))

    def test_sp_p0_target_tech_live_date_prefers_monthly_requirements_email(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        requirements_text = (
            "Monthly Requirements Gmail thread history export\n"
            "================================================================================\n"
            "Thread 1\n"
            "Market: SG\n"
            "Subject: SG_2026 Monthly Requirements Biweekly Update\n"
            "Message 1\n"
            "From: Xinni Oon <xinni.oon@npt.sg>\n"
            "Body:\n"
            "| Region | Priority | Project | Target Tech Live Date |\n"
            "| SG | SP | Balance Transfer open to NTB application | Sep 2026 |\n"
            "| SG | P1 | SME RCF drawdown check | Oct 2026 |\n"
        )
        brief = build_monthly_project_evidence_brief(
            key_projects=[
                {
                    "bpmis_id": "SP-EMAIL",
                    "project_name": "Balance Transfer open to NTB application",
                    "market": "SG",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "CR-1",
                            "jira_title": "Balance Transfer open to NTB application",
                            "jira_status": "Testing",
                            "release_date": "2026-06-15",
                            "version": "CR_v1_0615",
                        }
                    ],
                },
                {
                    "bpmis_id": "P1-JIRA",
                    "project_name": "SME RCF drawdown check",
                    "market": "SG",
                    "priority": "P1",
                    "jira_tickets": [
                        {
                            "jira_id": "CR-2",
                            "jira_title": "SME RCF drawdown check",
                            "jira_status": "Testing",
                            "release_date": "2026-07-20",
                            "version": "CR_v2_0720",
                        }
                    ],
                },
            ],
            seatalk_history_text="",
            vip_gmail_text="",
            monthly_requirements_text=requirements_text,
            prd_scope_summaries=[],
            report_period=period,
        )

        by_id = {item["project_id"]: item for item in brief}
        self.assertEqual(by_id["SP-EMAIL"]["target_tech_live_date"], "Sep 2026")
        self.assertEqual(by_id["SP-EMAIL"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["P1-JIRA"]["target_tech_live_date"], "Jul 2026")
        self.assertEqual(by_id["P1-JIRA"]["target_tech_live_source"], "jira_version")

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
            result = service.generate_draft(template="# Template", team_payloads=[], highlight_topics=["AF launch"])

        self.assertEqual(result["draft_markdown"], "# Summary")
        self.assertEqual(result["evidence_summary"]["gmail_error_count"], 2)

    def test_template_normalization_and_markdown_html(self):
        sanitized = _sanitize_monthly_report_output(
            "No confirmed project delivery evidence is available for this scope.\n"
            "## Key Follow-Ups\n- Confirm owner"
        )
        self.assertIn("pending confirmation", sanitized)
        self.assertNotIn("No confirmed project delivery evidence", sanitized)
        self.assertNotIn("Key Follow-Ups", sanitized)
        self.assertIn("Monthly Report", normalize_monthly_report_template(""))
        html = monthly_report_markdown_to_html("# Report\n- **Done** `AF-1`")
        self.assertIn("<strong>Done</strong>", html)
        self.assertIn("<code>AF-1</code>", html)
        table_html = monthly_report_markdown_to_html(
            "## Updates\n"
            "| Region | Priority | Project | Current Status | Target Tech Live Date |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| SG | SP | Multi-Currency Account | Dev | Jul 2026 |\n"
            "| PH | P0 | Incoming Transaction Hold | Dev | Support Reject: May 2026 |\n"
        )
        self.assertIn("<table", table_html)
        self.assertIn("<th", table_html)
        self.assertIn("<td", table_html)
        self.assertIn("Multi-Currency Account", table_html)
        self.assertIn("Jul 2026", table_html)
        self.assertIn("Support Reject: May 2026", table_html)
        self.assertIn("table-layout:fixed", table_html)
        self.assertIn('<col style="width:12%;">', table_html)
        self.assertIn('<col style="width:11%;">', table_html)
        self.assertIn('<col style="width:39%;">', table_html)
        self.assertIn('<col style="width:16%;">', table_html)
        self.assertIn('<col style="width:22%;">', table_html)
        self.assertIn('style="border:1px solid #111827;padding:6px 8px;text-align:left;vertical-align:top;white-space:normal;word-break:normal;overflow-wrap:anywhere;font-weight:700;background:#f8fafc;width:39%;"', table_html)


if __name__ == "__main__":
    unittest.main()
