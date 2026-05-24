from __future__ import annotations

import tempfile
import unittest
import os
from dataclasses import replace
from datetime import date, datetime
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
    _highlight_topic_aliases,
    _monthly_report_highlight_qualifier_marker_groups,
    _monthly_report_text_matches_qualifier_marker_groups,
    _apply_monthly_report_project_tables,
    build_monthly_report_evidence_review,
    build_monthly_highlight_deep_evidence,
    build_monthly_highlight_evidence_map,
    build_monthly_highlight_topic_narrative_prompt,
    build_monthly_report_historical_style_guide,
    build_monthly_requirements_target_map,
    build_monthly_project_evidence_brief,
    build_monthly_report_final_prompt,
    build_monthly_report_project_tables,
    build_monthly_report_query_plan,
    generate_monthly_report_with_codex,
    match_monthly_report_highlight_topics,
    monthly_report_business_glossary_summary,
    monthly_report_subject,
    normalize_monthly_report_highlight_topic_sources,
    monthly_report_markdown_to_html,
    normalize_monthly_report_highlight_topics,
    normalize_monthly_report_template,
    read_monthly_report_historical_style_guide_cache,
    resolve_monthly_report_period_from_user_range,
    resolve_monthly_report_period,
    _sanitize_monthly_report_output,
    write_monthly_report_historical_style_guide_cache,
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
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(
            os.environ,
            {"MONTHLY_REPORT_CODEX_MODEL": "", "SOURCE_CODE_QA_CODEX_MODEL": "gpt-5.5"},
            clear=False,
        ), patch("bpmis_jira_tool.monthly_report.CodexCliBridgeSourceCodeQALLMProvider") as mock_provider:
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
        self.assertEqual(instance.generate.call_args.kwargs["primary_model"], "gpt-5.5")
        payload = instance.generate.call_args.kwargs["payload"]
        self.assertEqual(payload["_codex_reasoning_effort"], "high")
        self.assertEqual(payload["_llm_ledger_route"], "deep")

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

    def test_monthly_report_subject_uses_banking_product_update_format(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-03-02", period_end="2026-03-13")
        self.assertEqual(
            monthly_report_subject(period=period),
            "[Banking] Product Update (2 Mar - 13 Mar) - Anti-Fraud, Credit Risk & Ops Risk",
        )

    def test_historical_sent_reports_feed_final_prompt_style_guide(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-03-14", period_end="2026-04-10")
        style_guide = build_monthly_report_historical_style_guide(
            [
                {
                    "source_type": "gmail_sent_monthly_report",
                    "item_type": "curated_report",
                    "summary": "[Banking] Product Update (14 Mar - 10 Apr) - Anti-Fraud, Credit Risk & Ops Risk",
                    "content": (
                        "# Highlights\n"
                        "- Anti-Fraud: PH launch risk was aligned with business impact and next action.\n"
                        "- Credit Risk: SG credit policy rollout moved to UAT with owner follow-up.\n"
                    ),
                },
                {
                    "source_type": "team_dashboard",
                    "item_type": "project_update",
                    "summary": "Should not be used",
                    "content": "Ignore this item.",
                },
            ]
        )
        self.assertEqual(style_guide["report_count"], 1)
        prompt = build_monthly_report_final_prompt(
            template="# Template",
            generated_at=datetime(2026, 4, 10, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            report_period=period,
            highlight_topics=["PH launch risk"],
            evidence_brief="Compact brief",
            monthly_evidence_brief=[],
            historical_report_style_guide=style_guide,
        )
        self.assertIn("Historical Sent Report Style Guide", prompt)
        self.assertIn("[Banking] Product Update (14 Mar - 10 Apr)", prompt)
        self.assertIn("Use the historical reports as writing-style references", prompt)
        self.assertIn("Start the email body directly with Highlights", prompt)
        self.assertIn("do not add a '0. Critical Updates' heading", prompt)
        self.assertNotIn("Ignore this item", prompt)

    def test_historical_style_guide_cache_round_trips_once_per_owner(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            style_guide = build_monthly_report_historical_style_guide(
                [
                    {
                        "source_type": "gmail_sent_monthly_report",
                        "item_type": "curated_report",
                        "summary": "[Banking] Product Update (2 Mar - 13 Mar) - Anti-Fraud, Credit Risk & Ops Risk",
                        "content": "# Highlights\n- Anti-Fraud: Launch decision and next action were aligned.",
                    }
                ]
            )
            write_monthly_report_historical_style_guide_cache(
                settings,
                owner_email="xiaodong.zheng@npt.sg",
                style_guide=style_guide,
            )

            cached = read_monthly_report_historical_style_guide_cache(settings, owner_email="xiaodong.zheng@npt.sg")

        self.assertIsNotNone(cached)
        self.assertEqual(cached["report_count"], 1)
        self.assertEqual(cached["observed_subjects"], ["[Banking] Product Update (2 Mar - 13 Mar) - Anti-Fraud, Credit Risk & Ops Risk"])

    def test_highlight_topics_and_user_date_range_are_validated(self):
        self.assertEqual(normalize_monthly_report_highlight_topics([" AF ", "", "CRMS", "AF"]), ["AF", "CRMS"])
        self.assertEqual(normalize_monthly_report_highlight_topics([]), [])
        self.assertEqual(normalize_monthly_report_highlight_topics(" \n "), [])
        self.assertEqual(
            normalize_monthly_report_highlight_topics(["one", "two", "three", "four", "five", "six"]),
            ["one", "two", "three", "four", "five", "six"],
        )
        with self.assertRaises(ToolError):
            normalize_monthly_report_highlight_topics(["one", "two", "three", "four", "five", "six", "seven"])

        sources = normalize_monthly_report_highlight_topic_sources(
            [{"topic": "AF", "sources": ["seatalk", "team-dashboard"]}],
            ["AF", "CRMS"],
        )
        self.assertEqual(sources["AF"], ["seatalk", "team_dashboard"])
        self.assertEqual(sources["CRMS"], ["seatalk", "gmail", "team_dashboard"])
        with self.assertRaises(ToolError):
            normalize_monthly_report_highlight_topic_sources([{"topic": "AF", "sources": []}], ["AF"])

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

    def test_highlight_topic_narrative_cache_reuses_same_topic_evidence(self):
        report_period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        topic_evidence = [
            {
                "topic": "AF launch",
                "confidence": "high",
                "recommended_tone": "Write as a confident executive progress update.",
                "seatalk_evidence": ["AF launch owner confirmed."],
                "gmail_evidence": [],
                "project_updates": [],
                "evidence_map": {"source_counts": {"seatalk": 1}},
            }
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=_FakeSeaTalkService(),
            )
            with patch.object(
                service,
                "_guarded_generate",
                return_value={"result_markdown": "AF launch remains on track with owner confirmation.", "model_id": "codex-cli", "trace": {"id": "trace-1"}},
            ) as mock_generate:
                first = service._highlight_topic_narratives(
                    generated_at=datetime(2026, 5, 8, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                    report_period=report_period,
                    highlight_deep_evidence=topic_evidence,
                    progress_callback=None,
                )
                second = service._highlight_topic_narratives(
                    generated_at=datetime(2026, 5, 9, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                    report_period=report_period,
                    highlight_deep_evidence=topic_evidence,
                    progress_callback=None,
                )

        self.assertEqual(mock_generate.call_count, 1)
        self.assertFalse(first[0]["cache_hit"])
        self.assertTrue(second[0]["cache_hit"])
        self.assertEqual(second[0]["narrative_markdown"], "AF launch remains on track with owner confirmation.")

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

        self.assertTrue(result["draft_markdown"].startswith("# Draft"))
        self.assertIn("## 1. Anti-Fraud Updates", result["draft_markdown"])
        self.assertIn("Key Fraud Project", result["draft_markdown"])
        self.assertIn("Regards", result["draft_markdown"])
        self.assertEqual(seatalk.calls[0]["since"].isoformat(), "2026-04-20T00:00:00+08:00")
        self.assertEqual(seatalk.calls[0]["now"].isoformat(), "2026-05-04T00:00:00+08:00")
        self.assertEqual(seatalk.calls[0]["days"], 15)
        self.assertIsNone(seatalk.calls[0]["conversation_scope"])
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
        self.assertEqual(result["subject"], "[Banking] Product Update (13 Apr - 3 May) - Anti-Fraud, Credit Risk & Ops Risk")
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
            "generating_highlight_narrative",
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
        self.assertEqual(len(result["highlight_narratives"]), 2)
        self.assertIn("generation_diagnostics", result)
        self.assertIn("target_tech_live_source_counts", result["generation_diagnostics"])
        self.assertIn("batch_summary_cache_hit_count", result["generation_summary"])
        self.assertIn("highlight_narrative_cache_hit_count", result["generation_summary"])
        self.assertEqual(result["generation_summary"]["scheduled_period_end"], "2026-05-08")
        self.assertGreater(result["generation_summary"]["prompt_chars"], 0)
        self.assertGreater(result["generation_summary"]["estimated_prompt_tokens"], 0)
        self.assertEqual(result["generation_summary"]["token_risk"], "normal")
        self.assertTrue(result["generation_summary"]["batch_mode"])
        self.assertGreaterEqual(result["generation_summary"]["total_batches"], 3)
        self.assertIn("elapsed_seconds", result["generation_summary"])
        for key in ("seatalk_export", "vip_gmail", "requirements_gmail", "topic_gmail", "prd_ingest", "prd_summary", "highlight_narrative", "batch_summary", "merge", "final", "total"):
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

    def test_generate_draft_prioritizes_seatalk_phrase_for_descriptive_credit_risk_highlight(self):
        topic = "SG Credit Risk a more flexible workflow"
        aliases = _highlight_topic_aliases(topic)
        self.assertIn("flexible workflow", aliases)
        self.assertNotIn("workflow", aliases)
        self.assertNotIn("flexible", aliases)
        self.assertNotIn("credit", aliases)

        seatalk = _FakeSeaTalkService()
        seatalk.export_history_since = lambda **_kwargs: "\n".join(
            [
                "SeaTalk Chat History Export",
                "=== SG Credit Risk PM ===",
                "[2026-05-02 09:59:00] PM: previous discussion covered suspension fallback options and owner alignment.",
                "[2026-05-02 10:01:00] GitHub Bot: Build Cloud Run image workflow run failed.",
                "[2026-05-02 10:02:00] Alice: SG Credit Risk needs a more flexible workflow for the BTI suspension feature.",
                "[2026-05-02 10:03:00] Xiaodong Zheng: align the fallback and exception handling with local PM before PRD review.",
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
                highlight_topics=[topic],
                period_start="2026-04-13",
                period_end="2026-05-08",
            )

        joined_prompts = "\n".join(call.kwargs["prompt"] for call in mock_generate.call_args_list)
        self.assertIn("previous discussion covered suspension fallback options", joined_prompts)
        self.assertIn("more flexible workflow for the BTI suspension feature", joined_prompts)
        self.assertIn("align the fallback and exception handling", joined_prompts)
        self.assertNotIn("Build Cloud Run image workflow run failed", joined_prompts)
        self.assertGreater(result["evidence_summary"]["highlight_seatalk_raw_match_count"], 0)
        self.assertGreater(result["evidence_summary"]["highlight_seatalk_line_match_count"], 0)

    def test_generate_draft_compacts_large_project_evidence_brief_batch(self):
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
        self.assertGreaterEqual(len(evidence_batch_calls), 1)
        for call in evidence_batch_calls:
            self.assertLessEqual(_estimate_token_count(call.kwargs["prompt"]), MONTHLY_REPORT_BATCH_MAX_TOKENS)
            self.assertNotIn("Monthly Report Template For Orientation", call.kwargs["prompt"])
            self.assertNotIn("scope detail " * 12, call.kwargs["prompt"])
        self.assertLess(
            result["generation_summary"]["stage_token_ledger"]["monthly_evidence_brief_batch_estimated_tokens"],
            result["generation_summary"]["stage_token_ledger"]["monthly_evidence_brief_estimated_tokens"],
        )
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

    def test_generate_draft_respects_highlight_source_checkboxes_for_external_search(self):
        gmail = _FakeGmailService("VIP evidence that should not be searched")
        seatalk = _FakeSeaTalkService()
        team_payloads = [
            {
                "team_key": "AF",
                "label": "Anti-fraud",
                "member_emails": ["owner@npt.sg"],
                "under_prd": [
                    {
                        "bpmis_id": "AF-TEAM",
                        "project_name": "Team Dashboard Only Project",
                        "is_key_project": True,
                        "priority": "SP",
                        "jira_tickets": [
                            {
                                "jira_id": "AF-1",
                                "jira_title": "Team Dashboard Only Project",
                                "jira_status": "Developing",
                            }
                        ],
                    }
                ],
                "pending_live": [],
            }
        ]
        now = datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.generate_monthly_report_with_codex") as mock_generate:
            mock_generate.return_value = {"result_markdown": "# Summary", "model_id": "codex-cli", "trace": {}}
            service = MonthlyReportService(
                settings=_settings(temp_dir),
                workspace_root=Path(temp_dir),
                seatalk_service=seatalk,
                confluence=None,
                gmail_service=gmail,
                now=now,
                report_intelligence_config={"vip_people": [{"display_name": "Boss", "emails": ["boss@npt.sg"]}]},
            )
            result = service.generate_draft(
                template="# Template",
                team_payloads=team_payloads,
                highlight_topics=["Team Dashboard Only Project"],
                highlight_topic_sources=[{"topic": "Team Dashboard Only Project", "sources": ["team_dashboard"]}],
            )

        self.assertEqual(seatalk.calls, [])
        self.assertFalse(any("contact_emails" in call for call in gmail.calls))
        self.assertTrue(any("monthly_requirements_configs" in call for call in gmail.calls))
        self.assertEqual(result["evidence_summary"]["vip_gmail_thread_count"], 0)
        self.assertEqual(result["evidence_summary"]["highlight_gmail_thread_count"], 0)
        self.assertEqual(result["evidence_summary"]["highlight_seatalk_raw_match_count"], 0)

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
            highlight_narratives=[{"topic": "CIB Phase 2", "narrative_markdown": "CIB Phase 2 is moving through UAT."}],
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
        self.assertIn("Highlight Narrative Candidates", prompt)
        self.assertIn("CIB Phase 2 is moving through UAT", prompt)
        self.assertIn("Other Key Project Updates", prompt)
        self.assertIn('"current_status"', prompt)
        self.assertIn("The audience is Xiaodong's manager", prompt)
        self.assertIn("executive product update", prompt)
        self.assertIn("Do not create a '0. Critical Updates' heading", prompt)
        self.assertIn("Do not expose raw evidence mechanics in Highlights", prompt)
        self.assertIn("confidence/recommended_tone", prompt)
        self.assertIn("go_live_outcome", prompt)
        self.assertIn("no confirmed evidence", prompt)
        self.assertIn("pending confirmation", prompt)

    def test_final_prompt_uses_compact_highlight_deep_evidence(self):
        period = resolve_monthly_report_period(datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        prompt = build_monthly_report_final_prompt(
            template="# Template",
            generated_at=datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            report_period=period,
            evidence_brief="Compact brief",
            monthly_evidence_brief=[],
            highlight_topics=["ID issue follow up"],
            highlight_deep_evidence=[
                {
                    "topic": "ID issue follow up",
                    "topic_intent": "issue_followup",
                    "confidence": "high",
                    "recommended_tone": "state confirmed mitigation",
                    "seatalk_evidence": ["raw transcript " * 500],
                    "gmail_evidence": ["raw email " * 500],
                    "issue_followup_facts": {
                        "impact": ["25 onboarding applications were affected."],
                        "root_cause": ["Risk Database read path was overloaded."],
                        "long_term_solution": ["Migrate risk identification data to Codis cache."],
                    },
                    "evidence_map": {"topic_intent": "issue_followup", "confidence": "high"},
                }
            ],
            highlight_narratives=[{"topic": "ID issue follow up", "narrative_markdown": "Mitigation is in progress."}],
        )

        self.assertIn("Highlight Deep Evidence", prompt)
        self.assertIn("issue_followup_facts", prompt)
        self.assertIn("Risk Database read path was overloaded", prompt)
        self.assertNotIn("raw transcript raw transcript raw transcript", prompt)
        self.assertNotIn("raw email raw email raw email", prompt)

    def test_go_live_highlight_intent_does_not_promote_generic_progress(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        self.assertEqual(resolve_monthly_report_period(datetime(2026, 4, 1, 10, tzinfo=SEATALK_INSIGHTS_TIMEZONE)).start.date(), date(2026, 4, 13))
        self.assertEqual(resolve_monthly_report_period_from_user_range(period_start="", period_end="", fallback=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE)).start.date(), date(2026, 4, 13))
        projects = [
            {
                "bpmis_id": "CCIC",
                "project_name": "PH Credit Card Instant Checkout",
                "market": "PH",
                "priority": "SP",
                "jira_tickets": [
                    {
                        "jira_id": "CC-1",
                        "jira_title": "Credit Card Instant Checkout risk tier update",
                        "jira_status": "Developing",
                    }
                ],
            }
        ]
        matches = match_monthly_report_highlight_topics(["PH Credit Card Employee Go Live"], projects)
        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=["PH Credit Card Employee Go Live"],
            key_projects=projects,
            topic_project_matches=matches,
            seatalk_history_text="2026-05-01 PH Credit Card Instant Checkout development and testing continued.",
            topic_gmail_evidence=[
                {
                    "topic": "PH Credit Card Employee Go Live",
                    "text": "Subject: PH Credit Card Instant Checkout\nBody:\nRisk tier and tenor tier development remains in progress.",
                }
            ],
            prd_scope_summaries=[
                {
                    "jira_id": "CC-1",
                    "scope_summary": "PRD covers risk tier, tenor tier, whitelist and CRIF fallback scope.",
                }
            ],
            report_period=period,
        )

        self.assertEqual(deep[0]["topic_intent"], "go_live_outcome")
        self.assertEqual(deep[0]["evidence_map"]["intent_signal_count"], 0)
        self.assertIn("go_live_outcome_evidence", deep[0]["evidence_map"]["gaps"])
        self.assertEqual(deep[0]["confidence"], "low")
        self.assertIn("do not substitute generic development", deep[0]["recommended_tone"])
        prompt = build_monthly_highlight_topic_narrative_prompt(
            generated_at=datetime(2026, 5, 8, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            report_period=period,
            topic_evidence=deep[0],
        )
        self.assertIn("go-live happened", prompt)
        self.assertIn("Do not replace missing go-live outcome evidence", prompt)

    def test_employee_live_testing_topic_uses_concrete_seatalk_launch_evidence(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        topic = "PH Credit Card MVP Employee Live Testing Status"
        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=[topic],
            key_projects=[],
            topic_project_matches=[{"topic": topic, "project_ids": []}],
            seatalk_history_text=(
                "=== group-4012584 ===\n"
                "[2026-04-21 12:27:29] Haoshuo Liu: We will get EPFS by this week. "
                "SEA employee LV will have 3000 users to be whitelisted. "
                "On Jun 8 we will whitelist 50k users.\n"
                "[2026-04-21 12:39:16] Zheng Xiaodong: Can I check if Credit Card Limit Increase has been internally tested before May 7?\n"
                "[2026-04-21 12:58:43] PM: Positive case can be tested prior the employee live, or we can try actual employees on May 7.\n"
                "=== group-9999999 ===\n"
                "[2026-04-17 16:37:18] PM: ID Credit Card has a later launch and unrelated testing progress.\n"
                "[2026-04-18 11:20:10] PM: PH GRC live testing finished last Friday, unrelated to card launch.\n"
                "=== group-4285581 ===\n"
                "[2026-04-21 16:34:28] Liang Chen: PH Credit Card completed EPFS. "
                "Plan is Sea Group Employee Live Testing on May 7 and Public Launch on Jun 8. "
                "2nd Live Testing May 7-Jun 7: Sea group employee 3000 users. "
                "Public Launch Jun 8: Whitelist 50k users.\n"
            ),
            topic_gmail_evidence=[],
            prd_scope_summaries=[],
            report_period=period,
            highlight_topic_sources={topic: ["seatalk"]},
        )

        self.assertEqual(deep[0]["topic_intent"], "go_live_outcome")
        self.assertEqual(deep[0]["selected_sources"], ["seatalk"])
        self.assertGreaterEqual(deep[0]["evidence_map"]["intent_signal_count"], 1)
        self.assertEqual(deep[0]["confidence"], "high")
        self.assertTrue(any("Sea Group Employee Live Testing" in item for item in deep[0]["seatalk_evidence"]))
        self.assertTrue(any("Whitelist 50k users" in item for item in deep[0]["seatalk_evidence"]))
        self.assertFalse(any("ID Credit Card" in item for item in deep[0]["seatalk_evidence"]))
        self.assertFalse(any("PH GRC" in item for item in deep[0]["seatalk_evidence"]))
        self.assertEqual(deep[0]["evidence_debug"]["source_counts"]["seatalk"], len(deep[0]["seatalk_evidence"]))
        self.assertIn("mari_credit_card", [item["id"] for item in deep[0]["evidence_debug"]["glossary_matches"]])
        self.assertTrue(any("group-4285581" in item for item in deep[0]["evidence_debug"]["seatalk_conversation_labels"]))
        review = build_monthly_report_evidence_review(deep)
        self.assertEqual(review[0]["status"], "ready")
        self.assertEqual(review[0]["source_policy"], {"seatalk": "conversation_level"})
        self.assertTrue(any("group-4285581" in item for item in review[0]["seatalk_conversation_labels"]))

    def test_mcc_qualifier_disambiguates_mari_credit_card_from_merchant_category_code(self):
        topic = "PH MCC Employee Live Testing Status"
        marker_groups = _monthly_report_highlight_qualifier_marker_groups(topic)

        self.assertTrue(
            _monthly_report_text_matches_qualifier_marker_groups(
                "=== Maribank [Temp] SEA Group MCC Whitelisted (group-4417575) ===",
                marker_groups,
            )
        )
        self.assertTrue(
            _monthly_report_text_matches_qualifier_marker_groups(
                "PH Mari Credit Card employee live testing has optional income doc users.",
                marker_groups,
            )
        )
        self.assertFalse(
            _monthly_report_text_matches_qualifier_marker_groups(
                "PH risk rule updated the merchant category code MCC mapping for payments.",
                marker_groups,
            )
        )

    def test_monthly_report_business_glossary_loads_repo_derived_terms(self):
        summary = monthly_report_business_glossary_summary()

        self.assertGreaterEqual(summary["entry_count"], 3)
        self.assertIn("AF", summary["domains"])
        self.assertIn("CRMS", summary["domains"])
        self.assertIn("GRC", summary["domains"])
        self.assertGreater(summary["derived_source_counts"].get("source_code_qa_domain_profiles.json", 0), 0)
        self.assertIn("mari credit card", _highlight_topic_aliases("PH MCC Employee Live Testing Status"))

    def test_query_plan_keeps_context_only_terms_out_of_primary_search(self):
        plan = build_monthly_report_query_plan("PH afasa ShopeePay Transaction limit Bank AF")

        self.assertEqual(plan["intent"], "general_progress")
        self.assertEqual(plan["product_area_scope"], "Anti-fraud")
        self.assertIn(["bank", "maribank", "seabank"], plan["qualifier_marker_groups"])
        self.assertIn("bank", plan["qualifiers"]["context_only_terms"])
        self.assertNotIn("Bank AF", plan["primary_topic"])
        self.assertNotIn("bank af", plan["aliases"])

    def test_query_plan_disambiguates_mcc_for_mari_credit_card(self):
        plan = build_monthly_report_query_plan("PH MCC Employee Live Testing Status", selected_sources=["seatalk"])

        self.assertEqual(plan["source_policy"], {"seatalk": "conversation_level"})
        self.assertEqual(plan["product_area_scope"], "Credit Risk")
        self.assertIn("merchant category", plan["forbidden_meanings"])
        self.assertIn("mari_credit_card", [item["id"] for item in plan["glossary_matches"]])
        self.assertIn("mari credit card", plan["aliases"])

    def test_highlight_product_area_scope_keeps_credit_risk_only(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        projects = [
            {
                "bpmis_id": "AF-SCL",
                "project_name": "Standalone Cash Loan - retail consumer",
                "market": "SG",
                "priority": "SP",
                "teams": ["Anti-fraud"],
                "jira_tickets": [{"jira_id": "AF-1", "jira_title": "Standalone Cash Loan anti-fraud rules", "jira_status": "Developing"}],
            },
            {
                "bpmis_id": "CR-SCL",
                "project_name": "SG Standalone Cash Loan and Retail Limit Assignment",
                "market": "SG",
                "priority": "P1",
                "teams": ["Credit Risk"],
                "jira_tickets": [{"jira_id": "CR-1", "jira_title": "Retail limit assignment for Standalone Cash Loan", "jira_status": "PRD Reviewed"}],
            },
        ]
        topic = "[Credit Risk] SG Standalone Cash Loan and Retail Limit Assignment"
        matches = match_monthly_report_highlight_topics([topic], projects)
        self.assertEqual(matches[0]["product_area_scope"], "Credit Risk")
        self.assertEqual(matches[0]["project_ids"], ["CR-SCL"])

        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=[topic],
            key_projects=projects,
            topic_project_matches=matches,
            seatalk_history_text=(
                "2026-05-01 Anti-Fraud Standalone Cash Loan scam rule testing continued.\n"
                "2026-05-02 Credit Risk Standalone Cash Loan retail limit assignment timeline moved through PRD review."
            ),
            topic_gmail_evidence=[],
            prd_scope_summaries=[],
            report_period=period,
        )

        self.assertEqual(deep[0]["product_area_scope"], "Credit Risk")
        self.assertEqual([project["bpmis_id"] for project in deep[0]["project_updates"]], ["CR-SCL"])
        self.assertTrue(any("Credit Risk Standalone Cash Loan" in item for item in deep[0]["seatalk_evidence"]))
        self.assertFalse(any("Anti-Fraud" in item for item in deep[0]["seatalk_evidence"]))
        prompt = build_monthly_highlight_topic_narrative_prompt(
            generated_at=datetime(2026, 5, 8, 12, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            report_period=period,
            topic_evidence=deep[0],
        )
        self.assertIn("product_area_scope", prompt)
        self.assertIn("focus only on that product area's changes and timeline", prompt)

    def test_highlight_bank_af_scope_keeps_bank_anti_fraud_only(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        topic = "PH afasa ShopeePay Transaction limit Bank AF"
        projects = [
            {
                "bpmis_id": "BANK-AF",
                "project_name": "PH AFASA ShopeePay Transaction Limit Bank AF",
                "market": "PH",
                "priority": "SP",
                "teams": ["Anti-fraud"],
                "jira_tickets": [{"jira_id": "AF-1", "jira_title": "ShopeePay transaction limit for Bank AF"}],
            },
            {
                "bpmis_id": "WALLET-AF",
                "project_name": "PH ShopeePay Wallet Transaction Limit Anti-Fraud",
                "market": "PH",
                "priority": "SP",
                "teams": ["Anti-fraud"],
                "jira_tickets": [{"jira_id": "AF-2", "jira_title": "ShopeePay transaction risk rules"}],
            },
            {
                "bpmis_id": "CR-TXN",
                "project_name": "PH Credit Card Transaction Limit",
                "market": "PH",
                "priority": "P1",
                "teams": ["Credit Risk"],
                "jira_tickets": [{"jira_id": "CR-1", "jira_title": "Credit transaction limit"}],
            },
        ]

        matches = match_monthly_report_highlight_topics([topic], projects)
        self.assertEqual(matches[0]["product_area_scope"], "Anti-fraud")
        self.assertEqual(matches[0]["project_ids"], ["BANK-AF"])
        self.assertEqual(matches[0]["qualifier_marker_groups"], [["bank", "maribank", "seabank"]])

        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=[topic],
            key_projects=projects,
            topic_project_matches=matches,
            seatalk_history_text=(
                "2026-05-01 PH ShopeePay wallet transaction risk rule continued.\n"
                "2026-05-02 PH ShopeePay transaction limit Bank AF UAT completed.\n"
                "2026-05-03 PH Credit Card transaction limit alignment continued.\n"
                "2026-05-04 Banking Product Update covered unrelated ShopeePay wallet status."
            ),
            topic_gmail_evidence=[],
            prd_scope_summaries=[],
            report_period=period,
        )

        self.assertEqual([project["bpmis_id"] for project in deep[0]["project_updates"]], ["BANK-AF"])
        self.assertTrue(any("Bank AF" in item for item in deep[0]["seatalk_evidence"]))
        self.assertFalse(any("wallet transaction" in item for item in deep[0]["seatalk_evidence"]))
        self.assertFalse(any("Credit Card" in item for item in deep[0]["seatalk_evidence"]))
        self.assertFalse(any("Banking Product Update" in item for item in deep[0]["seatalk_evidence"]))

    def test_highlight_bank_af_qualifier_does_not_search_by_bank_only_or_clear_primary_match(self):
        topic = "PH afasa ShopeePay Transaction limit Bank AF"
        projects = [
            {
                "bpmis_id": "AFASA-SP",
                "project_name": "PH AFASA ShopeePay Transaction Limit",
                "market": "PH",
                "priority": "SP",
                "teams": ["Anti-fraud"],
                "jira_tickets": [{"jira_id": "AF-1", "jira_title": "AFASA ShopeePay transaction limit"}],
            },
            {
                "bpmis_id": "BANK-GENERIC",
                "project_name": "Bank AF Generic Monitoring",
                "market": "PH",
                "priority": "P1",
                "teams": ["Anti-fraud"],
                "jira_tickets": [{"jira_id": "AF-2", "jira_title": "Bank AF dashboard cleanup"}],
            },
        ]

        matches = match_monthly_report_highlight_topics([topic], projects)
        self.assertEqual(matches[0]["project_ids"], ["AFASA-SP"])

    def test_highlight_source_selection_gates_deep_evidence_sources(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        topics = ["AF launch", "CRMS workflow"]
        projects = [
            {
                "bpmis_id": "AF-1",
                "project_name": "AF launch",
                "market": "SG",
                "priority": "SP",
                "teams": ["Anti-fraud"],
                "jira_tickets": [{"jira_id": "AF-1", "jira_title": "AF launch", "jira_status": "Developing"}],
            },
            {
                "bpmis_id": "CR-1",
                "project_name": "CRMS workflow",
                "market": "SG",
                "priority": "SP",
                "teams": ["Credit Risk"],
                "jira_tickets": [{"jira_id": "CR-1", "jira_title": "CRMS workflow", "jira_status": "Developing"}],
            },
        ]
        matches = match_monthly_report_highlight_topics(topics, projects)
        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=topics,
            key_projects=projects,
            topic_project_matches=matches,
            seatalk_history_text="=== AF launch group ===\nAF launch SeaTalk decision was aligned.\n=== CRMS workflow group ===\nCRMS workflow SeaTalk discussion continued.",
            topic_gmail_evidence=[
                {"topic": "AF launch", "text": "Subject: AF launch\nBody:\nAF launch Gmail approval.", "thread_count": 1, "message_count": 1},
                {"topic": "CRMS workflow", "text": "Subject: CRMS workflow\nBody:\nCRMS workflow Gmail approval.", "thread_count": 1, "message_count": 1},
            ],
            prd_scope_summaries=[
                {"jira_id": "AF-1", "scope_summary": "PRD says AF launch scope is ready."},
                {"jira_id": "CR-1", "scope_summary": "PRD says CRMS workflow scope is ready."},
            ],
            report_period=period,
            highlight_topic_sources={
                "AF launch": ["seatalk"],
                "CRMS workflow": ["gmail", "team_dashboard"],
            },
        )

        self.assertEqual(deep[0]["selected_sources"], ["seatalk"])
        self.assertTrue(deep[0]["seatalk_evidence"])
        self.assertEqual(deep[0]["gmail_evidence"], [])
        self.assertEqual(deep[0]["project_updates"], [])
        self.assertEqual(deep[0]["prd_scope_summaries"], [])
        self.assertEqual(deep[0]["matched_project_ids"], [])
        self.assertEqual(deep[1]["seatalk_evidence"], [])
        self.assertTrue(deep[1]["gmail_evidence"])
        self.assertTrue(deep[1]["project_updates"])
        self.assertTrue(deep[1]["prd_scope_summaries"])

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

    def test_issue_highlight_preserves_root_cause_and_solution_facts(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        topic = "ID anti-fraud recent issues, database capacity, system downgrade, impact and follow up actions"
        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=[topic],
            key_projects=[],
            topic_project_matches=[{"topic": topic, "project_ids": []}],
            seatalk_history_text=(
                "Impact: intermittent login issue to SeaBank App; around 25 onboarding applications failed to call AF.\n"
                "Root cause: high QPS directed at the Risk Database overloaded running threads and caused risk-service timeouts.\n"
                "Short-term solution: dbp-antifraud-batch-service scaled down from 10 to 5 and UC QPS limit reduced by 50%.\n"
                "Long-term solution: move risk identification data to Codis cache and AF rule logs to ES Index by 30 Jun.\n"
            ),
            topic_gmail_evidence=[],
            prd_scope_summaries=[],
            report_period=period,
        )

        facts = deep[0]["issue_followup_facts"]
        self.assertTrue(any("onboarding applications" in item for item in facts["impact"]))
        self.assertTrue(any("Risk Database" in item for item in facts["root_cause"]))
        self.assertTrue(any("scaled down" in item for item in facts["short_term_solution"]))
        self.assertTrue(any("Codis cache" in item for item in facts["long_term_solution"]))
        self.assertEqual(deep[0]["evidence_map"]["topic_intent"], "issue_followup")
        self.assertIn("issue_followup_facts", deep[0]["evidence_map"])

        prompt = build_monthly_report_final_prompt(
            template="# Template",
            generated_at=datetime(2026, 5, 3, 10, 0, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            report_period=period,
            evidence_brief="Compact brief",
            monthly_evidence_brief=[],
            highlight_topics=[topic],
            highlight_deep_evidence=deep,
            highlight_narratives=[],
        )
        self.assertIn("issue_followup_facts", prompt)
        self.assertIn("root cause", prompt)
        self.assertIn("long-term solution", prompt)

    def test_issue_highlight_captures_chinese_seatalk_evidence_from_english_topic(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        topic = "ID anti-fraud recent issues, database capacity, system downgrade, impact and follow up actions"
        deep = build_monthly_highlight_deep_evidence(
            highlight_topics=[topic],
            key_projects=[],
            topic_project_matches=[{"topic": topic, "project_ids": []}],
            seatalk_history_text=(
                "=== ID AF group ===\n"
                "[2026-05-05] PM: 这次数据库容量问题触发了系统降级。\n"
                "影响：部分用户登录失败，约 25 个 onboarding application 调用 AF 失败。\n"
                "根因：大促流量带来高并发 QPS，风险数据库读写过载，线程堆积。\n"
                "短期方案：临时限流，关闭 dual writing，通过热修减少写入，并扩容 batch service。\n"
                "长期方案：risk identification 数据迁移到 Codis 缓存，rule logs 迁移到 ES 索引。\n"
                "下一步：30 Jun 完成迁移，Q4 拆分 AF database library。\n"
            ),
            topic_gmail_evidence=[],
            prd_scope_summaries=[],
            report_period=period,
        )

        facts = deep[0]["issue_followup_facts"]
        self.assertTrue(any("登录失败" in item for item in facts["impact"]))
        self.assertTrue(any("风险数据库读写过载" in item for item in facts["root_cause"]))
        self.assertTrue(any("临时限流" in item for item in facts["short_term_solution"]))
        self.assertTrue(any("ES 索引" in item for item in facts["long_term_solution"]))
        self.assertTrue(any("30 Jun" in item for item in facts["next_action"]))

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
        self.assertEqual(by_id["BPMIS-2"]["matched_vip_gmail_threads"], [])

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
            fallback_reference_date=date(2026, 5, 9),
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
        fallback = next(item for item in brief if item["project_id"] == "WAITING")
        self.assertEqual(fallback["target_tech_live_date"], "Q3 2026")
        self.assertEqual(fallback["target_tech_live_source"], "next_quarter_fallback")
        target = next(item for item in brief if item["project_id"] == "TARGET")
        self.assertEqual(target["target_tech_live_date"], "Jun 2026")
        self.assertEqual(target["target_tech_live_version"], "AF_v1.1_0620")
        self.assertFalse(any("Planning_26Q4" in fact for fact in target["timeline_facts"]))

    def test_current_status_uses_jira_only_and_non_highlight_skips_external_evidence(self):
        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        brief = build_monthly_project_evidence_brief(
            key_projects=[
                {
                    "bpmis_id": "HL",
                    "project_name": "Highlight External Evidence Project",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [],
                },
                {
                    "bpmis_id": "NON-HL",
                    "project_name": "Non Highlight External Evidence Project",
                    "teams": ["Anti-fraud"],
                    "jira_tickets": [],
                },
            ],
            seatalk_history_text="Highlight External Evidence Project entered UAT and is live testing.",
            vip_gmail_text="Subject: Non Highlight External Evidence Project\nBody: Non Highlight External Evidence Project entered UAT.",
            prd_scope_summaries=[
                {"jira_id": "", "scope_summary": "Highlight External Evidence Project PRD scope is complete."}
            ],
            report_period=period,
            highlight_project_ids={"HL"},
        )

        by_id = {item["project_id"]: item for item in brief}
        self.assertEqual(by_id["HL"]["current_status"], "BRD")
        self.assertGreater(by_id["HL"]["material_update_score"], 0)
        self.assertTrue(by_id["HL"]["matched_seatalk_messages"])
        self.assertEqual(by_id["NON-HL"]["current_status"], "BRD")
        self.assertEqual(by_id["NON-HL"]["matched_vip_gmail_threads"], [])
        self.assertEqual(by_id["NON-HL"]["matched_seatalk_messages"], [])

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
            "Subject: PH_2026 Monthly Requirements Biweekly Update_0415\n"
            "From: Yuanfang Zhou <yuanfang.zhou@npt.sg>\n"
            "[Strategic Project] [PH] MariBank Card on Google Pay - On Track\n"
            "Timeline: Tech GoLive: 2026.09.01, LV: 2026.09.02 ~ 2026.10.02, Public: 2026.10.15\n"
            "Subject: PH_2026 Monthly Requirements Biweekly Update_0430\n"
            "From: Yuanfang Zhou <yuanfang.zhou@npt.sg>\n"
            "| PH | P0 | SPL Cash Advance disbursal to MariBank | Aug 2026 |\n"
            "[Strategic Project] [PH] MariBank Card on Google Pay - On Track\n"
            "Timeline: PRD: 2026.02.02 ~ 2026.03.20, DEV: 2026.03.02 ~ 2026.04.24\n"
            ", SIT: 2026.03.16 ~ 2026.05.08, UAT: 2026.03.23 ~ 2026.05.15, REG:\n"
            "2026.03.30 ~ 2026.05.20, Tech GoLive: 2026.05.21 -> 2026.06.09, LV:\n"
            "2026.05.18 ~ 2026.07.17, Public: 2026.07.24\n"
            "Dec 2025 SP [PH] MariBank Card on Google Pay - Manual Provisioning\n"
            "Subject: SG_2026 Monthly Requirements Biweekly Update_0430\n"
            "From: Xinni Oon <xinni.oon@npt.sg>\n"
            "[Strategic Project] [SG] Multi Currency Account - On Track\n"
            "Timeline: Tech Live: 260324, Public Live: 2026.05.15\n"
            "Subject: SG_2026 Monthly Requirements Biweekly Update_ Wk 0508\n"
            "From: Xinni Oon <xinni.oon@npt.sg>\n"
            "[Strategic Project] Corporate Internet Banking - Go Live date revised\n"
            "Go Live: 0827 -> 1013\n"
            "[Strategic Project] SG Last Date Wins - Go Live date revised\n"
            "Go Live: 1013 -> 0827\n"
            "[Strategic Project] Line Split Live Project - On track\n"
            "PRD ETC: 0327, Dev/SIT ETC: 0529, UAT ETC: 0619, Go\n"
            "Live: 0703 -> 0709\n"
            "[P0] Split Bill - On track\n"
            "PRD ETC: 0403, Dev/SIT ETC: 0529, UAT ETC: 0612, Go Live: 0618\n"
            "2 Strategic Projects Special Projects Governance, Risk & Compliance Xiaodong Zheng\n"
            "Phase 2: RCSA\n"
            "pending UAT, Go Live 0918\n"
            "Phase 3: Outsourcing\n"
            "Subject: ID_Monthly Requirements Biweekly Update_260515\n"
            "From: Graceful Xiong <graceful.xiong@npt.sg>\n"
            "[ID] [TP] Credit Card - Timeline revised\n"
            "Regression: 1019-1127 Tech live: 1217 -> TBC\n"
        )
        requirements_targets = build_monthly_requirements_target_map(requirements_text)
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
                    "bpmis_id": "P0-EMAIL",
                    "project_name": "[PH] SPL Cash Advance disbursal to MariBank",
                    "market": "PH",
                    "priority": "P0",
                    "jira_tickets": [
                        {
                            "jira_id": "AF-1",
                            "jira_title": "SPL Cash Advance disbursal to MariBank",
                            "jira_status": "Testing",
                            "release_date": "2026-05-15",
                            "version": "AF_v1_0515",
                        }
                    ],
                },
                {
                    "bpmis_id": "PH-GPAY",
                    "project_name": "PH Google Pay in-app auth / token management",
                    "market": "PH",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "AF-2",
                            "jira_title": "Google Pay in-app auth and token management",
                            "jira_status": "Testing",
                            "release_date": "2026-05-21",
                            "version": "AF_v1_0521",
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
                {
                    "bpmis_id": "SG-MCA",
                    "project_name": "SG Multi Currency Account",
                    "market": "SG",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "AF-3",
                            "jira_title": "Multi Currency Account",
                            "jira_status": "Testing",
                            "release_date": "2026-07-10",
                            "version": "AF_v1_0710",
                        }
                    ],
                },
                {
                    "bpmis_id": "SG-CIB",
                    "project_name": "Corporate Internet Banking (Singapore)",
                    "market": "SG",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "AF-4",
                            "jira_title": "Corporate Internet Banking",
                            "jira_status": "Testing",
                            "release_date": "2026-07-10",
                            "version": "AF_v1_0710",
                        }
                    ],
                },
                {
                    "bpmis_id": "SG-LAST",
                    "project_name": "SG Last Date Wins",
                    "market": "SG",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "AF-5",
                            "jira_title": "SG Last Date Wins",
                            "jira_status": "Testing",
                            "release_date": "2026-07-10",
                            "version": "AF_v1_0710",
                        }
                    ],
                },
                {
                    "bpmis_id": "SG-LINE",
                    "project_name": "Line Split Live Project",
                    "market": "SG",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "AF-6",
                            "jira_title": "Line Split Live Project",
                            "jira_status": "Testing",
                            "release_date": "2026-05-10",
                            "version": "AF_v1_0510",
                        }
                    ],
                },
                {
                    "bpmis_id": "SG-BILL",
                    "project_name": "Bill Split - extension of MCC Split Payment function",
                    "market": "SG",
                    "priority": "P0",
                    "jira_tickets": [
                        {
                            "jira_id": "AF-7",
                            "jira_title": "Bill Split",
                            "jira_status": "Testing",
                            "release_date": "2026-05-10",
                            "version": "AF_v1_0510",
                        }
                    ],
                },
                {
                    "bpmis_id": "GRC-P2",
                    "project_name": "Governance, Risk & Compliance (GRC) - Phase 2 (RCSA + Supporting Modules)",
                    "market": "SG",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "GRC-2",
                            "jira_title": "GRC Phase 2 RCSA",
                            "jira_status": "Testing",
                            "release_date": "2026-03-27",
                            "version": "GRC_v1_0327",
                        }
                    ],
                },
                {
                    "bpmis_id": "GRC-P3",
                    "project_name": "Governance, Risk & Compliance (GRC) - Phase 3 (Outsourcing)",
                    "market": "SG",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "GRC-3",
                            "jira_title": "GRC Phase 3 Outsourcing",
                            "jira_status": "Testing",
                            "release_date": "2026-08-14",
                            "version": "GRC_v1_0814",
                        }
                    ],
                },
                {
                    "bpmis_id": "ID-TBC",
                    "project_name": "[ID] [TP] Credit Card",
                    "market": "ID",
                    "priority": "SP",
                    "jira_tickets": [
                        {
                            "jira_id": "ID-1",
                            "jira_title": "[ID] [TP] Credit Card",
                            "jira_status": "Testing",
                            "release_date": "2026-08-14",
                            "version": "ID_v1_0814",
                        }
                    ],
                },
            ],
            seatalk_history_text="",
            vip_gmail_text="",
            monthly_requirements_targets=requirements_targets,
            prd_scope_summaries=[],
            report_period=period,
        )

        by_id = {item["project_id"]: item for item in brief}
        self.assertGreaterEqual(len(requirements_targets), 4)
        self.assertEqual(by_id["SP-EMAIL"]["target_tech_live_date"], "Sep 2026")
        self.assertEqual(by_id["SP-EMAIL"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["SP-EMAIL"]["target_tech_live_source_detail"]["sender"], "xinni.oon@npt.sg")
        self.assertIn("Balance Transfer", by_id["SP-EMAIL"]["target_tech_live_source_detail"]["matched_line"])
        self.assertTrue(by_id["SP-EMAIL"]["target_tech_live_source_detail"]["matched_alias"])
        self.assertEqual(by_id["P0-EMAIL"]["target_tech_live_date"], "Aug 2026")
        self.assertEqual(by_id["P0-EMAIL"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["P0-EMAIL"]["target_tech_live_source_detail"]["sender"], "yuanfang.zhou@npt.sg")
        self.assertEqual(by_id["PH-GPAY"]["target_tech_live_date"], "Jun 2026")
        self.assertEqual(by_id["PH-GPAY"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["PH-GPAY"]["target_tech_live_source_detail"]["source_date_hint"], "2026-04-30")
        self.assertIn("Tech GoLive", by_id["PH-GPAY"]["target_tech_live_source_detail"]["matched_line"])
        self.assertIn("google pay", by_id["PH-GPAY"]["target_tech_live_source_detail"]["matched_alias"])
        self.assertEqual(by_id["P1-JIRA"]["target_tech_live_date"], "Jul 2026")
        self.assertEqual(by_id["P1-JIRA"]["target_tech_live_source"], "jira_version")
        self.assertEqual(by_id["SG-MCA"]["target_tech_live_date"], "Mar 2026")
        self.assertEqual(by_id["SG-MCA"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["SG-MCA"]["target_tech_live_source_detail"]["target_label"], "tech_live")
        self.assertEqual(by_id["SG-CIB"]["target_tech_live_date"], "Oct 2026")
        self.assertEqual(by_id["SG-CIB"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["SG-CIB"]["target_tech_live_source_detail"]["target_label"], "tech_live")
        self.assertIn("Go Live", by_id["SG-CIB"]["target_tech_live_source_detail"]["matched_line"])
        self.assertEqual(by_id["SG-LAST"]["target_tech_live_date"], "Aug 2026")
        self.assertEqual(by_id["SG-LAST"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["SG-LINE"]["target_tech_live_date"], "Jul 2026")
        self.assertEqual(by_id["SG-LINE"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertIn("Go Live", by_id["SG-LINE"]["target_tech_live_source_detail"]["matched_line"])
        self.assertEqual(by_id["SG-BILL"]["target_tech_live_date"], "Jun 2026")
        self.assertEqual(by_id["SG-BILL"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertEqual(by_id["SG-BILL"]["target_tech_live_source_detail"]["matched_alias"], "split bill")
        self.assertEqual(by_id["GRC-P2"]["target_tech_live_date"], "Sep 2026")
        self.assertEqual(by_id["GRC-P2"]["target_tech_live_source"], "monthly_requirements_email")
        self.assertIn("Phase 2", by_id["GRC-P2"]["target_tech_live_source_detail"]["matched_line"])
        self.assertEqual(by_id["GRC-P3"]["target_tech_live_date"], "Aug 2026")
        self.assertEqual(by_id["GRC-P3"]["target_tech_live_source"], "jira_version")
        self.assertEqual(by_id["ID-TBC"]["target_tech_live_date"], "TBC")
        self.assertEqual(by_id["ID-TBC"]["target_tech_live_source"], "monthly_requirements_email")

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

    def test_monthly_report_project_tables_are_deterministic_from_included_json(self):
        evidence = [
            {
                "include": True,
                "project_id": "165707",
                "project_name": "Governance, Risk & Compliance (GRC) - Phase 2 (RCSA + Supporting Modules)",
                "product_area": "Anti-fraud",
                "market": "Regional",
                "priority": "SP",
                "current_status": "UAT",
                "target_tech_live_date": "2026-07-01",
            },
            {
                "include": True,
                "project_id": "201377",
                "project_name": "Governance, Risk & Compliance (GRC) - Phase 3 (Outsourcing)",
                "product_area": "Ops Risk",
                "market": "Regional",
                "priority": "SP",
                "current_status": "Dev",
                "target_tech_live_date": "Q3 2026",
            },
            {
                "include": True,
                "project_id": "211471",
                "project_name": "Credit Risk Productization Phase 1",
                "product_area": "Credit Risk",
                "market": "SG",
                "priority": "P1",
                "current_status": "PRD",
                "target_tech_live_date": "May 2026",
            },
            {
                "include": True,
                "project_id": "205938",
                "project_name": "[PH] MariBank Credit Card Shopee Instant Checkout",
                "product_area": "Anti-fraud",
                "teams": ["Anti-fraud", "Credit Risk"],
                "market": "PH",
                "priority": "SP",
                "current_status": "Dev",
                "target_tech_live_date": "Jun 2026",
            },
            {
                "include": True,
                "project_id": "217733",
                "project_name": "[ID] [TP] Credit Card",
                "product_area": "Credit Risk",
                "market": "ID",
                "priority": "SP",
                "current_status": "Dev",
                "target_tech_live_date": "TBC",
            },
            {
                "include": False,
                "project_id": "239510",
                "project_name": "Viber as Primary Channel with SMS Fallback AAF Requirements",
                "product_area": "Anti-fraud",
                "market": "PH",
                "priority": "P0",
                "current_status": "Dev",
                "target_tech_live_date": "Jun 2026",
            },
        ]

        tables = build_monthly_report_project_tables(evidence)

        self.assertIn("## 2. Credit Risk Updates", tables)
        self.assertIn("## 3. Ops Risk (GRC System) Updates", tables)
        self.assertIn("Governance, Risk & Compliance (GRC) - Phase 2", tables)
        self.assertIn("Governance, Risk & Compliance (GRC) - Phase 3", tables)
        self.assertIn("Credit Risk Productization Phase 1", tables)
        self.assertIn("[PH] MariBank Credit Card Shopee Instant Checkout", tables)
        self.assertIn("| Regional | SP | Governance, Risk & Compliance (GRC) - Phase 2", tables)
        self.assertIn("| PH | SP | [PH] MariBank Credit Card Shopee Instant Checkout | Dev | Jun 2026 |", tables)
        self.assertIn("| ID | SP | [ID] [TP] Credit Card | Dev | TBC |", tables)
        self.assertIn("| SG | P1 | Credit Risk Productization Phase 1 | PRD | May 2026 |", tables)
        self.assertNotIn("Viber as Primary Channel with SMS Fallback AAF Requirements", tables)

    def test_monthly_report_project_tables_replace_llm_generated_update_sections(self):
        draft = (
            "Subject: [Banking] Product Update\n\n"
            "Highlights\n"
            "- GRC delivery remains the main Ops Risk update.\n\n"
            "## 1. Anti-Fraud Updates\n\n"
            "| Region | Priority | Project | Current Status | Target Tech Live Date |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| PH | P0 | Model invented row | Dev | Jun 2026 |\n\n"
            "## 3. Ops Risk (GRC System) Updates\n\n"
            "| Region | Priority | Project | Current Status | Target Tech Live Date |\n"
            "| --- | --- | --- | --- | --- |\n"
            "| Regional | SP | Governance, Risk & Compliance (GRC) - Phase 3 (Outsourcing) | Dev | Q3 2026 |\n\n"
            "Regards\n"
            "Xiaodong"
        )
        evidence = [
            {
                "include": True,
                "project_id": "165707",
                "project_name": "Governance, Risk & Compliance (GRC) - Phase 2 (RCSA + Supporting Modules)",
                "product_area": "Ops Risk",
                "market": "Regional",
                "priority": "SP",
                "current_status": "UAT",
                "target_tech_live_date": "Jul 2026",
            },
            {
                "include": True,
                "project_id": "211471",
                "project_name": "Credit Risk Productization Phase 1",
                "product_area": "Credit Risk",
                "market": "SG",
                "priority": "P1",
                "current_status": "PRD",
                "target_tech_live_date": "May 2026",
            },
        ]

        result = _apply_monthly_report_project_tables(draft, evidence)

        self.assertIn("Highlights", result)
        self.assertIn("Credit Risk Productization Phase 1", result)
        self.assertIn("Governance, Risk & Compliance (GRC) - Phase 2", result)
        self.assertNotIn("Model invented row", result)
        self.assertNotIn("Governance, Risk & Compliance (GRC) - Phase 3", result)
        self.assertTrue(result.endswith("Regards\nXiaodong"))

    def test_monthly_report_private_edge_helpers_cover_boundary_branches(self):
        from bpmis_jira_tool import monthly_report as mr

        self.assertEqual(
            normalize_monthly_report_highlight_topic_sources({"Skip": ["gmail"], "AF": ["email"]}, ["AF"]),
            {"AF": ["gmail"]},
        )
        with self.assertRaises(ToolError):
            normalize_monthly_report_highlight_topic_sources([{"topic": "AF", "sources": []}], ["AF"])
        style = build_monthly_report_historical_style_guide(
            [
                "bad",
                {"source_type": "other", "content": "Highlights\nIgnore"},
                {"item_type": "other", "content": "Highlights\nIgnore"},
                {"summary": "Subject", "content": "Highlights\nAF launch\n\nCredit Risk live"},
                {"summary": "Subject", "content": "Highlights\nDuplicate"},
            ]
        )
        self.assertEqual(style["report_count"], 1)
        max_style = build_monthly_report_historical_style_guide(
            [{"summary": f"Subject {index}", "content": "Highlights\nAF launch"} for index in range(20)]
        )
        self.assertEqual(max_style["report_count"], mr.MONTHLY_REPORT_STYLE_GUIDE_MAX_REPORTS)
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            cache_path = mr._monthly_report_style_guide_cache_path(settings, owner_email="owner@npt.sg")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text("[]", encoding="utf-8")
            self.assertIsNone(read_monthly_report_historical_style_guide_cache(settings, owner_email="owner@npt.sg"))
            mr._write_monthly_report_json_cache(cache_path, {"version": -1, "style_guide": style})
            self.assertIsNone(read_monthly_report_historical_style_guide_cache(settings, owner_email="owner@npt.sg"))
            mr._write_monthly_report_json_cache(cache_path, {"version": mr.MONTHLY_REPORT_STYLE_GUIDE_CACHE_VERSION, "style_guide": {"report_count": 0}})
            self.assertIsNone(read_monthly_report_historical_style_guide_cache(settings, owner_email="owner@npt.sg"))
        excerpt = mr._monthly_report_historical_excerpt("Highlights\n\nAF update\n" + "\n".join(f"line {i}" for i in range(500)))
        self.assertIn("AF update", excerpt)
        compacted = mr._compact_monthly_report_style_guide(
            {
                "report_count": 10,
                "observed_subjects": [f"s{i}" for i in range(20)],
                "style_rules": [f"rule {i}" for i in range(20)],
                "examples": ["bad", {"subject": "bad", "excerpt": ""}, *[{"subject": f"s{i}", "excerpt": "x" * 3000} for i in range(12)]],
            }
        )
        self.assertLessEqual(len(compacted["examples"]), 3)

        period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
        self.assertEqual(resolve_monthly_report_period(datetime(2026, 5, 4, 10, tzinfo=SEATALK_INSIGHTS_TIMEZONE)).start.date(), date(2026, 4, 13))
        with self.assertRaises(ToolError):
            resolve_monthly_report_period_from_user_range(period_start="", period_end="2026-05-08")
        with self.assertRaises(ToolError):
            resolve_monthly_report_period_from_user_range(period_start="bad", period_end="2026-05-08")
        self.assertEqual(
            mr._monthly_report_period_from_payload(
                period_start="2026-04-13T10:00:00+08:00",
                period_end="2026-05-08",
                period_end_exclusive="2026-05-09",
                fallback=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            ).end.date(),
            date(2026, 5, 8),
        )
        with self.assertRaises(ToolError):
            mr._monthly_report_period_from_payload(period_start="2026-05-08", period_end="2026-04-13", period_end_exclusive=None, fallback=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        self.assertEqual(mr._parse_monthly_report_datetime(datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE)).date(), date(2026, 5, 1))
        with self.assertRaises(ValueError):
            mr._parse_monthly_report_datetime("bad")

        self.assertEqual(mr._safe_int("bad"), 0)
        self.assertEqual(mr._safe_int("7"), 7)
        self.assertEqual(mr._normalize_google_sheet_evidence([{"title": "T", "url": "u", "text": "x" * 5000}, "bad"])[0]["title"], "T")
        self.assertEqual(mr._normalize_google_sheet_evidence([{}, *[{"title": str(i)} for i in range(5)]])[-1]["title"], "3")
        self.assertEqual(normalize_monthly_report_highlight_topics({"bad": "type"}), [])
        self.assertEqual(build_monthly_report_historical_style_guide([{"summary": "s", "content": ""}])["report_count"], 0)
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            write_monthly_report_historical_style_guide_cache(settings, owner_email="owner@npt.sg", style_guide={})
            self.assertFalse(mr._monthly_report_style_guide_cache_path(settings, owner_email="owner@npt.sg").exists())
        self.assertEqual(mr._monthly_report_historical_excerpt(""), "")
        self.assertTrue(build_monthly_highlight_evidence_map(["bad", {"topic": "AF", "seatalk_evidence": ["AF public launch"]}]))
        self.assertEqual(build_monthly_report_evidence_review(["bad"]), [])
        self.assertEqual(mr._monthly_report_confidence_counts([{"confidence": "weird"}])["none"], 1)
        self.assertEqual(mr._monthly_report_target_source_counts(["bad", {"include": False}]), {})
        with self.assertRaises(ToolError):
            resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="", fallback=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        with self.assertRaises(ToolError):
            mr._monthly_report_period_from_payload(period_start="2026-04-13", period_end=None, period_end_exclusive=None, fallback=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        with self.assertRaises(ToolError):
            mr._monthly_report_period_from_payload(period_start="bad", period_end="2026-05-08", period_end_exclusive="bad", fallback=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        with self.assertRaises(ToolError):
            mr._monthly_report_period_from_payload(period_start="2026-05-10", period_end="2026-05-08", period_end_exclusive="2026-05-09", fallback=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE))
        with self.assertRaises(ValueError):
            mr._parse_monthly_report_datetime("")
        self.assertEqual(mr._parse_monthly_report_datetime("2026-05-01T10:00:00").tzinfo, SEATALK_INSIGHTS_TIMEZONE)
        self.assertEqual(mr._parse_monthly_report_datetime(date(2026, 5, 1)).date(), date(2026, 5, 1))
        self.assertEqual(mr._parse_monthly_report_datetime("2026-05-01").date(), date(2026, 5, 1))
        self.assertIn(
            "Monthly Report Template",
            mr.build_monthly_report_prompt(
                template="# T",
                generated_at=datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
                seatalk_history_text="",
                key_projects=[],
                prd_sources=[],
                prd_errors=[],
            ),
        )
        self.assertEqual(mr._compact_monthly_project_evidence_for_batch([{"include": False}]), [])
        self.assertEqual(mr._compact_highlight_deep_evidence_for_prompt(["bad"], include_source_evidence=True), [])
        self.assertEqual(mr._compact_highlight_project_updates(["bad"]), [])
        self.assertEqual(mr._compact_highlight_evidence_map({}), {})
        self.assertEqual(mr._compact_text_list(["", "x"], limit=2, max_chars=5), ["x"])
        self.assertEqual(mr._monthly_report_table_cell(""), "TBD")
        self.assertEqual(mr._monthly_report_token_risk(mr.MONTHLY_REPORT_TOKEN_RISK_HIGH), "high")
        self.assertEqual(mr._monthly_report_token_risk(mr.MONTHLY_REPORT_TOKEN_RISK_WARNING), "warning")
        self.assertEqual(mr._payload_block("raw"), "raw")
        sanitized = _sanitize_monthly_report_output("No confirmed source evidence is available.\n## Key Follow-ups\nremove\n## Next\nkeep")
        self.assertIn("This item remains", sanitized)
        self.assertNotIn("remove", sanitized)
        self.assertEqual(mr._monthly_report_project_table_area({"product_area": "ops"}), "Ops Risk")
        self.assertEqual(mr._monthly_report_project_table_area({"product_area": "credit"}), "Credit Risk")
        self.assertEqual(mr._monthly_report_project_table_area({"product_area": "anti fraud"}), "Anti-Fraud")
        self.assertEqual(mr._monthly_report_project_table_area({"project_name": "Neutral"}), "Credit Risk")
        self.assertIn(
            "AF Launch",
            _apply_monthly_report_project_tables(
                "Intro\n\nRegards\nMe",
                [{"include": True, "product_area": "anti fraud", "project_name": "AF Launch", "market": "SG", "priority": "SP", "current_status": "UAT", "target_tech_live_date": "2026-05-01"}],
            ),
        )

        self.assertEqual(mr._matched_lines_for_project("short\nAF launch will go live", {"AF launch"}, limit=2), ["AF launch will go live"])
        self.assertEqual(mr._matched_conversation_context_lines_for_project("", {"AF"}, limit=2), [])
        conversation = "\n".join(
            [
                "preamble before marker",
                "================================================================================",
                "=== AF Launch Group ===",
                "github bot: ignore this",
                "AF launch topic",
                "Decision: public launch approved",
                "================================================================================",
                "=== Credit Group ===",
                "Credit card topic",
                "UAT is delayed",
            ]
        )
        self.assertTrue(mr._matched_conversation_context_lines_for_project(conversation, {"AF launch"}, limit=2))
        self.assertEqual(mr._matched_qualified_conversation_context_lines_for_project("", {"AF"}, qualifier_marker_groups=[("public launch",)], topic_intent="launch", topic="AF", limit=2, context_lines=1), [])
        self.assertTrue(
            mr._matched_qualified_conversation_context_lines_for_project(
                conversation,
                {"AF launch"},
                qualifier_marker_groups=[("public launch",)],
                topic_intent="general_progress",
                topic="SG AF launch",
                limit=3,
                context_lines=1,
            )
        )
        conflicting_conversation = "\n".join(
            [
                "=== SG AF Launch ===",
                "PH AF launch unrelated market line",
                "SG AF launch public launch approved",
                "SG AF launch follow-up one",
                "SG AF launch follow-up two",
                "SG AF launch follow-up three",
                "SG AF launch follow-up four",
                "SG AF launch follow-up five",
            ]
        )
        self.assertTrue(
            mr._matched_qualified_conversation_context_lines_for_project(
                conflicting_conversation,
                {"AF launch"},
                qualifier_marker_groups=[("public launch",)],
                topic_intent="general_progress",
                topic="SG AF launch",
                limit=3,
                context_lines=5,
            )
        )
        self.assertFalse(mr._monthly_report_text_has_conflicting_market("SG launch", set()))
        self.assertTrue(mr._monthly_report_text_has_conflicting_market("PH launch", {"sg"}))
        self.assertTrue(mr._monthly_report_text_has_unrelated_product_signal("GRC RCSA update", "AF topic"))
        self.assertTrue(mr._matched_forward_context_lines_for_project(conversation, {"AF launch"}, limit=3, context_lines=2))
        self.assertTrue(mr._matched_context_lines_for_project("before marker\n" + conversation, {"AF launch"}, limit=3, context_lines=1))
        self.assertTrue(mr._matched_sections_for_project("\n\nshort\n\nAF launch section has enough detail", {"AF launch"}, limit=1))
        self.assertEqual(mr._index_prd_summaries_by_jira(["bad", {"jira_id": "AF-1", "url": "u"}])["AF-1"][0]["url"], "u")
        self.assertEqual(len(mr._matched_prd_summaries_for_project([{"jira_id": "AF-1"}, {"jira_id": "AF-1"}], {"AF-1": [{"jira_id": "AF-1", "url": "u"}]})), 1)

        self.assertEqual(mr._monthly_report_next_quarter_label(date(2026, 12, 1)), "Q1 2027")
        self.assertEqual(mr._monthly_report_target_tech_live_date([{"release_date": "bad", "version": "v1"}], fallback_reference_date=date(2026, 5, 1))[2], "next_quarter_fallback")
        requirements_text = "\n".join(
            [
                "Market: PH",
                "Subject: PH_Monthly Requirements Update_260501",
                "From: Owner <owner@npt.sg>",
                "",
                "Date: 2026-05-01",
                "Planning row should skip",
                "Project AF Launch",
                "Phase 2 scope",
                "Tech go",
                "live: Jun 2026",
                "| PH | SP | AF Launch | Tech Live | Jul 2026 |",
                "| XX | SP | Bad | Tech Live | Jul 2026 |",
                "| PH | X | Bad | Tech Live | Jul 2026 |",
            ]
        )
        targets = build_monthly_requirements_target_map(requirements_text)
        self.assertTrue(targets)
        project = {"project_name": "AF Launch Phase 2", "market": "PH", "priority": "SP", "jira_tickets": [{"jira_title": "Phase 2"}]}
        self.assertTrue(mr._monthly_requirements_target_tech_live_date(project, targets))
        self.assertIsNone(mr._monthly_requirements_target_tech_live_date({"priority": "P1"}, targets))
        self.assertEqual(mr._monthly_requirements_entries_for_project({}, targets), [])
        self.assertTrue(mr._monthly_requirements_line_matches_project("AF Launch target", {"", "AF Launch"}))
        self.assertEqual(mr._first_matching_alias("No match", {"", "AF Launch"}), "")
        self.assertEqual(mr._monthly_requirements_market_from_subject("PH_custom"), "PH")
        self.assertEqual(mr._monthly_requirements_sender_from_line("From: owner@npt.sg"), "owner@npt.sg")
        self.assertEqual(mr._monthly_requirements_entry_date({"target_date": date(2026, 5, 1)}), date(2026, 5, 1))
        self.assertIsNone(mr._monthly_requirements_entry_date({"target_date": "bad"}))
        self.assertIsNone(mr._monthly_requirements_entry_source_date({"source_date_hint": "bad"}))
        self.assertIsNone(mr._monthly_requirements_source_date_from_subject("bad_991399"))
        self.assertIsNone(mr._monthly_requirements_year_from_subject("bad_aa0101"))
        self.assertIsNone(mr._monthly_requirements_year_from_subject("no year"))
        self.assertEqual(mr._monthly_requirements_market_for_project({"market": "PH"}), "PH")
        self.assertEqual(mr._monthly_requirements_market_for_project({"project_name": "SG launch"}), "SG")
        self.assertTrue(mr._monthly_requirements_ordered_month_candidates("Planning skip\nJun 2026 2026-07-01 260801 bad 991399 0501 late Sep", source_year=2026))
        self.assertEqual(mr._monthly_requirements_ordered_month_candidates("Jun 0000 0000-07-01 late Sep", source_year="bad"), [])
        self.assertTrue(mr._monthly_requirements_ordered_month_candidates("Badmonth 2026 2026-99-01 991399 Sep", source_year=2026))
        self.assertFalse(mr._monthly_requirements_is_target_table_row("| PH | SP | Only three |"))
        self.assertFalse(mr._monthly_requirements_is_target_table_row("| XX | SP | Bad | Jul 2026 |"))
        self.assertFalse(mr._monthly_requirements_is_target_table_row("| PH | X | Bad | Jul 2026 |"))
        self.assertEqual(mr._monthly_requirements_relevant_phase_markers_for_target_line("Phase II tech live Jun 2026"), {"2"})
        self.assertIsNone(mr._monthly_requirements_year_from_subject("bad_aa0101"))
        self.assertIsNone(mr._monthly_requirements_year_from_subject("bad 20x6"))
        self.assertEqual(mr._monthly_report_month_label("nonsense"), "TBD")
        self.assertEqual(mr._monthly_report_current_status([{"version": "dev"}], report_period=period, material_update_score=1), "Dev")
        self.assertFalse(mr._release_date_reached({"release_date": "bad"}, period))
        self.assertEqual(mr._filter_text_by_product_scope("Random\nAnti-Fraud launch\n\nOther")[1], 2)
        self.assertEqual(mr._filter_text_by_product_scope_or_highlight_aliases("Random\nAF launch\n\nOther", set())[1], 2)
        self.assertGreaterEqual(mr._filter_text_by_product_scope_or_highlight_aliases(conversation + "\n", {"AF launch"})[2], 1)
        self.assertGreaterEqual(mr._filter_text_by_product_scope_or_highlight_aliases("=== H ===\nAF launch\n\nNext useful line", {"AF launch"})[2], 1)
        self.assertEqual(mr._filter_text_by_product_scope_or_highlight_aliases("Random\n\nOther", {"AF launch"})[2], 0)
        self.assertEqual(mr._filter_text_by_product_scope_or_highlight_aliases("Anti-Fraud launch\n\nRandom", {"Missing"})[0], "Anti-Fraud launch")
        self.assertEqual(mr._filter_thread_export_by_product_scope("")[1], 0)
        self.assertIn("No material", mr._filter_thread_export_by_product_scope("Header\n" + "=" * 80 + "\nRandom only")[0])
        self.assertGreater(len(mr._split_text_for_token_limit("x" * 5000 + "\nshort", 1)), 1)
        self.assertEqual(mr._split_text_for_token_limit("", 1), [])
        self.assertGreater(len(mr._split_text_for_token_limit(("line\n" * 1500), 1)), 1)
        self.assertGreater(len(mr._split_json_items_for_token_limit([{"id": "1", "content": "x" * 5000}, {"id": "2"}], 1)), 1)
        self.assertGreater(len(mr._split_json_items_for_token_limit([{"id": "1"}, {"id": "2", "content": "x" * 5000}], 1)), 1)
        self.assertEqual(mr._split_large_json_item({"id": "small"}, 10000), [{"id": "small"}])
        self.assertGreater(len(mr._split_large_json_item({"bpmis_id": "B1", "content": "x" * 5000}, 1)), 1)
        fallback_progress: list[tuple[object, ...]] = []

        def legacy_progress(stage, message, current, total):
            fallback_progress.append((stage, message, current, total))

        mr._emit_monthly_report_progress(legacy_progress, "stage", "message", 1, 1, estimated_prompt_tokens=1)
        self.assertEqual(fallback_progress[0], ("stage", "message", 1, 1))

        html = monthly_report_markdown_to_html("# Title\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\nnot table\n\n- item\nplain")
        self.assertIn("<table", html)
        self.assertIn("<p>not table</p>", html)
        self.assertIn("<li>item</li>", html)
        self.assertEqual(mr._monthly_report_table_column_widths(["Other"], 2), ["50.0000%", "50.0000%"])
        self.assertEqual(mr._equal_widths(2), ["50.0000%", "50.0000%"])
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {"LOCAL_AGENT_TEAM_PORTAL_DATA_DIR": str(Path(temp_dir) / "agent")}):
            self.assertEqual(mr._monthly_report_data_root(replace(_settings(temp_dir), team_portal_data_dir=Path("relative"))), Path(temp_dir) / "agent")
        with tempfile.TemporaryDirectory() as temp_dir, patch("bpmis_jira_tool.monthly_report.os.getenv", return_value=""):
            self.assertEqual(mr._monthly_report_data_root(replace(_settings(temp_dir), team_portal_data_dir=Path("relative"))), Path("relative"))
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = Path(temp_dir) / "cache.json"
            cache.write_text('{"ok": true}', encoding="utf-8")
            os.utime(cache, (0, 0))
            self.assertIsNone(mr._read_monthly_report_json_cache(cache, max_age_seconds=1))
            cache.write_text("{bad", encoding="utf-8")
            self.assertIsNone(mr._read_monthly_report_json_cache(cache))
            cache.write_text("[]", encoding="utf-8")
            self.assertIsNone(mr._read_monthly_report_json_cache(cache))
            with patch.object(Path, "write_text", side_effect=OSError("denied")):
                mr._write_monthly_report_json_cache(cache, {"ok": True})
        self.assertEqual(mr._vip_emails({"vip_people": ["bad", {"emails": ["A@NPT.SG"]}]}), ["a@npt.sg"])
        self.assertEqual(mr._project_product_area({"teams": ["Ops"], "project_name": "Process"}), "Ops Risk")
        self.assertEqual(mr._monthly_report_read_json_file(Path("/no/such/file.json")), {})
        self.assertIn("name", mr._monthly_report_collect_terms_from_value([{"name": "name", "term": "term", "aliases": ["alias"], "code_terms": ["code"]}]))
        self.assertIn("x", mr._monthly_report_collect_terms_from_value("x"))
        self.assertIn("credit card", mr._monthly_report_highlight_qualifier_marker_groups_from_entries("PH MCC credit card", [] )[1])
        self.assertIn("bank", mr._monthly_report_highlight_qualifier_marker_groups_from_entries("Bank AF", [] )[-1])
        self.assertFalse(mr._monthly_report_glossary_term_matches_text("text", "text", ""))
        self.assertEqual(mr._dedupe_marker_groups([(), ("ph",), ("ph",)]), [("ph",)])
        self.assertFalse(mr._monthly_report_text_matches_qualifier_marker("text", "text", ""))
        self.assertTrue(mr._monthly_report_text_matches_qualifier_marker("cc text", "cctext", "cc"))
        with patch("bpmis_jira_tool.monthly_report._monthly_report_business_glossary", return_value={"entries": ["bad"]}):
            self.assertTrue(mr._monthly_report_glossary_context_for_marker("mcc")["requires_context_any"])
        self.assertEqual(mr._filter_monthly_report_texts_by_qualifier_marker_groups(["a"], []), ["a"])
        self.assertFalse(mr._monthly_report_text_allowed_by_product_area_scope("", "Credit Risk"))
        self.assertFalse(mr._monthly_report_text_allowed_by_product_area_scope("anti fraud only", "Credit Risk"))
        self.assertFalse(mr._monthly_report_text_allowed_by_product_area_scope("scam only", "Credit Risk"))
        self.assertIn("af-1", mr._project_aliases({"jira_tickets": ["bad", {"jira_id": "AF-1"}]}))
        self.assertEqual(mr._monthly_report_highlight_topic_intent("employee rollout feedback after pilot"), "go_live_outcome")
        self.assertEqual(mr._monthly_report_highlight_topic_intent("release readiness"), "release_readiness")
        self.assertEqual(mr._monthly_report_highlight_topic_intent("decision approval"), "decision_needed")
        self.assertEqual(mr._monthly_report_highlight_intent_focus("release_readiness")["label"], "Release readiness")
        self.assertEqual(mr._monthly_report_highlight_intent_focus("decision_needed")["label"], "Decision needed")
        self.assertEqual(mr._monthly_report_intent_signal_count("issue_followup", []), 0)
        self.assertFalse(mr._monthly_report_intent_term_matches("", "", ""))
        self.assertFalse(mr._monthly_report_intent_term_matches("product", "product", "prod"))
        self.assertFalse(mr._highlight_topic_phrase_aliases(["sg", "ph"]))
        self.assertFalse(mr._highlight_topic_phrase_aliases(["alpha", "", "beta"]))
        self.assertFalse(mr._highlight_topic_phrase_aliases(["status", "update"]))
        self.assertFalse(mr._highlight_topic_phrase_aliases(["sg", "ph", "id"]))
        self.assertFalse(mr._highlight_topic_phrase_aliases(["new", "old"]))
        self.assertTrue(mr._highlight_topic_phrase_aliases(["recent", "status", "update"]))
        self.assertFalse(mr._is_useful_seatalk_highlight_alias(""))
        self.assertFalse(mr._is_useful_seatalk_highlight_alias("123"))
        self.assertFalse(mr._is_useful_seatalk_highlight_alias("workflow"))
        self.assertFalse(mr._is_useful_seatalk_highlight_alias("status"))
        self.assertFalse(mr._highlight_topic_matches_project(set(), {"project_name": "AF"}))
        self.assertTrue(mr._monthly_report_text_has_conflicting_market("Seabank PH", {"sg"}))
        self.assertTrue(mr._monthly_report_text_has_conflicting_market("Philippines", {"sg"}))
        self.assertTrue(mr._matched_conversation_context_lines_for_project(conversation, {"AF launch"}, limit=1))
        noisy_forward = "AF launch topic\n\nGithub bot: ignored\nNext useful line"
        self.assertEqual(mr._matched_forward_context_lines_for_project(noisy_forward, {"AF launch"}, limit=1, context_lines=3), ["AF launch topic"])
        self.assertEqual(mr._matched_forward_context_lines_for_project("github bot: AF launch\nNext useful line", {"AF launch"}, limit=3, context_lines=1), ["Next useful line"])
        self.assertEqual(mr._matched_context_lines_for_project("AF launch topic\n\nNext", {"AF launch"}, limit=1, context_lines=2), ["AF launch topic"])
        self.assertEqual(mr._matched_context_lines_for_project("AF launch topic\n\nNext useful line", {"AF launch"}, limit=3, context_lines=1), ["AF launch topic"])
        self.assertTrue(mr._monthly_report_issue_followup_aliases("risk database qps", {"id", "abc", "db"}))
        self.assertIsNone(
            mr._monthly_requirements_target_tech_live_date(
                {"project_name": "AF Launch", "market": "PH", "priority": "SP"},
                [{"market": "PH", "matched_line": "AF Launch", "target_date": "bad", "target_label": "tech_live"}],
            )
        )
        self.assertIsNone(
            mr._monthly_requirements_target_tech_live_date(
                {"project_name": "AF Launch", "market": "PH", "priority": "SP"},
                [{"market": "PH", "matched_line": "AF Launch", "target_date": "2026-07-01", "target_label": "date"}],
            )
        )
        self.assertEqual(mr._monthly_requirements_relevant_phase_markers_for_target_line("Tech live Phase II"), {"2"})
        self.assertTrue(mr._filter_text_by_product_scope_or_highlight_aliases("AF launch\n\nRandom", {"AF launch"})[0].splitlines()[1] == "")
        self.assertGreater(len(mr._split_json_items_for_token_limit([{"id": "1", "content": "x" * 1200}, {"id": "2", "content": "y" * 1200}], 500)), 1)

    def test_monthly_report_prompt_dry_run_omits_empty_optional_context(self):
        from bpmis_jira_tool import monthly_report as mr

        generated_at = datetime(2026, 5, 1, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
        period = resolve_monthly_report_period(generated_at)
        optimized = mr.build_monthly_report_prompt(
            template="# T",
            generated_at=generated_at,
            seatalk_history_text="AF launch approved.",
            key_projects=[{"project_name": "AF Launch"}],
            prd_sources=[],
            prd_errors=[],
        )
        legacy_optional = "\n\n# PRD / Confluence Enrichment\n[]\n\n# PRD Enrichment Gaps\n[]"
        self.assertIn("AF Launch", optimized)
        self.assertIn("AF launch approved.", optimized)
        self.assertNotIn("# PRD / Confluence Enrichment", optimized)
        self.assertNotIn("# PRD Enrichment Gaps", optimized)
        self.assertLess(len(optimized), len(optimized + legacy_optional))

        batch_prompt = mr.build_monthly_report_batch_prompt(
            template="# T",
            generated_at=generated_at,
            report_period=period,
            highlight_topics=[],
            source="seatalk",
            payload={"fact": "AF launch approved"},
            prd_errors=[],
        )
        self.assertIn("AF launch approved", batch_prompt)
        self.assertNotIn("# User-Provided Highlight Topics", batch_prompt)
        self.assertNotIn("# PRD Enrichment Gaps", batch_prompt)

        merge_prompt = mr.build_monthly_report_merge_prompt(
            generated_at=generated_at,
            report_period=period,
            highlight_topics=[],
            batch_summaries=[{"summary": "AF launch approved"}],
            prd_errors=[],
        )
        self.assertIn("AF launch approved", merge_prompt)
        self.assertNotIn("# User-Provided Highlight Topics", merge_prompt)
        self.assertNotIn("# PRD Enrichment Gaps", merge_prompt)

    def test_monthly_report_service_generation_edges_cover_caches_and_limits(self):
        from bpmis_jira_tool import monthly_report as mr

        with tempfile.TemporaryDirectory() as temp_dir:
            settings = _settings(temp_dir)
            service = MonthlyReportService(
                settings=settings,
                workspace_root=Path(temp_dir),
                seatalk_service=_FakeSeaTalkService(),
                gmail_service=_FakeGmailService(),
                now=datetime(2026, 5, 3, 10, tzinfo=SEATALK_INSIGHTS_TIMEZONE),
            )
            period = resolve_monthly_report_period_from_user_range(period_start="2026-04-13", period_end="2026-05-08")
            calls: list[str] = []

            def fake_guarded_generate(**kwargs):
                calls.append(kwargs["prompt_mode"])
                return {"result_markdown": f"generated {kwargs['prompt_mode']}", "model_id": "m", "trace": {"ok": True}}

            with patch.object(service, "_guarded_generate", side_effect=fake_guarded_generate):
                self.assertEqual(
                    service._batch_summaries(
                        template="# T",
                        generated_at=service.now,
                        report_period=period,
                        highlight_topics=[],
                        monthly_evidence_brief=[],
                        highlight_deep_evidence=[],
                        prd_errors=[],
                        evidence_sidecar=[],
                        progress_callback=None,
                    )[0]["source"],
                    "empty",
                )
                payload = [{"bpmis_id": "B1", "project_name": "AF", "content": "x"}]
                summaries = service._batch_summaries(
                    template="# T",
                    generated_at=service.now,
                    report_period=period,
                    highlight_topics=["AF"],
                    monthly_evidence_brief=payload,
                    highlight_deep_evidence=[],
                    prd_errors=[],
                    evidence_sidecar=[],
                    progress_callback=None,
                )
                self.assertFalse(summaries[0]["cache_hit"])
                cached = service._batch_summaries(
                    template="# T",
                    generated_at=service.now,
                    report_period=period,
                    highlight_topics=["AF"],
                    monthly_evidence_brief=payload,
                    highlight_deep_evidence=[],
                    prd_errors=[],
                    evidence_sidecar=[],
                    progress_callback=None,
                )
                self.assertTrue(cached[0]["cache_hit"])
                with patch("bpmis_jira_tool.monthly_report._estimate_token_count", side_effect=[MONTHLY_REPORT_MERGE_MAX_TOKENS + 1, 1]):
                    self.assertTrue(
                        service._merge_batch_summaries(
                            generated_at=service.now,
                            report_period=period,
                            highlight_topics=[],
                            batch_summaries=[{"source": "x", "index": 1, "summary_markdown": "y" * 7000}],
                            prd_errors=[],
                            progress_callback=None,
                        )
                    )
                with patch("bpmis_jira_tool.monthly_report._estimate_token_count", return_value=MONTHLY_REPORT_MERGE_MAX_TOKENS + 1):
                    with self.assertRaises(ToolError):
                        service._merge_batch_summaries(generated_at=service.now, report_period=period, highlight_topics=[], batch_summaries=[{"summary_markdown": "x"}], prd_errors=[], progress_callback=None)
                self.assertTrue(service._compress_evidence_brief(generated_at=service.now, evidence_brief="evidence", progress_callback=None))
                with patch("bpmis_jira_tool.monthly_report._estimate_token_count", return_value=MONTHLY_REPORT_MERGE_MAX_TOKENS + 1):
                    with self.assertRaises(ToolError):
                        service._compress_evidence_brief(generated_at=service.now, evidence_brief="x" * 5000, progress_callback=None)
                with patch("bpmis_jira_tool.monthly_report._estimate_token_count", return_value=MONTHLY_REPORT_BATCH_MAX_TOKENS + 1):
                    with self.assertRaises(ToolError):
                        MonthlyReportService._guarded_generate(service, prompt="x", prompt_mode="too_big", max_tokens=MONTHLY_REPORT_BATCH_MAX_TOKENS, progress_callback=None)
                self.assertEqual(service._highlight_topic_narratives(generated_at=service.now, report_period=period, highlight_deep_evidence=[], progress_callback=None), [])
                evidence = [{"topic": "AF", "confidence": "high", "topic_intent": "launch", "evidence": ["x"]}]
                narratives = service._highlight_topic_narratives(generated_at=service.now, report_period=period, highlight_deep_evidence=evidence, progress_callback=None)
                self.assertFalse(narratives[0]["cache_hit"])
                self.assertTrue(service._highlight_topic_narratives(generated_at=service.now, report_period=period, highlight_deep_evidence=evidence, progress_callback=None)[0]["cache_hit"])
            self.assertTrue(calls)

            with patch("bpmis_jira_tool.monthly_report._estimate_token_count", side_effect=[MONTHLY_REPORT_FINAL_MAX_TOKENS + 1, MONTHLY_REPORT_FINAL_MAX_TOKENS + 1]), patch.object(
                service,
                "_batch_summaries",
                return_value=[],
            ), patch.object(service, "_merge_batch_summaries", return_value="large evidence"), patch.object(service, "_compress_evidence_brief", return_value="still large"):
                with self.assertRaises(ToolError):
                    service.generate_draft(
                        template="# T",
                        team_payloads=[],
                        report_intelligence_config={"vip_people": []},
                        period_start="2026-04-13",
                        period_end="2026-05-08",
                    )

            payloads = [
                {
                    "team_key": "AF",
                    "label": "Anti-Fraud",
                    "member_emails": ["pm@npt.sg"],
                    "under_prd": [
                        {"is_key_project": True, "project_name": "No id"},
                        {
                            "is_key_project": True,
                            "bpmis_id": "B1",
                            "project_name": "AF Project",
                            "priority": "SP",
                            "jira_tickets": [
                                "bad",
                                {"jira_id": "AF-1", "pm_email": "other@npt.sg"},
                                {"jira_id": "AF-2", "pm_email": "pm@npt.sg"},
                                {"jira_id": "AF-2", "pm_email": "pm@npt.sg"},
                            ],
                        },
                    ],
                    "pending_live": [],
                }
            ]
            projects = service._key_projects(payloads)
            self.assertEqual(projects[0]["jira_tickets"][0]["jira_id"], "AF-2")
            project = {"project_name": "", "market": "", "priority": "", "regional_pm_pic": "", "status": "", "release_date": "", "key_project_source": ""}
            service._merge_project_fields(project, {"project_name": "Name"})
            self.assertEqual(project["project_name"], "Name")

            class EmptyConfluence:
                def ingest_page(self, url, kind):
                    if "empty" in url:
                        return SimpleNamespace(title="Empty", source_url=url, updated_at="", sections=[])
                    if "error" in url:
                        raise RuntimeError("bad page")
                    return SimpleNamespace(title="PRD", source_url=url, updated_at="now", sections=[SimpleNamespace(section_path="Overview", content="body")])

            service.confluence = EmptyConfluence()
            sources, errors = service._prd_sources(
                [
                    {"bpmis_id": "skip", "jira_tickets": [{"jira_id": "S", "prd_links": [{"url": "https://skip"}]}]},
                    {
                        "bpmis_id": "B1",
                        "jira_tickets": [
                            {
                                "jira_id": "AF-1",
                                "prd_links": [
                                    {},
                                    {"url": "https://empty"},
                                    {"url": "https://error"},
                                    {"url": "https://ok"},
                                    {"url": "https://ok"},
                                ],
                            }
                        ],
                    },
                ],
                project_ids={"B1"},
            )
            self.assertEqual(len(sources), 1)
            self.assertTrue(errors)

            class FakeCredentialStore:
                def __init__(self, payload):
                    self.payload = payload

                def load(self, *, owner_email):
                    return self.payload

            with self.assertRaises(ToolError):
                mr.send_monthly_report_email(credential_store=FakeCredentialStore({}), owner_email="owner@npt.sg", recipient="to@npt.sg", subject="S", draft_markdown="")
            with self.assertRaises(Exception):
                mr.send_monthly_report_email(credential_store=FakeCredentialStore({}), owner_email="", recipient="to@npt.sg", subject="S", draft_markdown="Body")
            with self.assertRaises(Exception):
                mr.send_monthly_report_email(credential_store=FakeCredentialStore({"scopes": []}), owner_email="owner@npt.sg", recipient="to@npt.sg", subject="S", draft_markdown="Body")
            with patch("bpmis_jira_tool.monthly_report.credentials_from_payload", return_value=object()), patch(
                "bpmis_jira_tool.monthly_report.send_gmail_message",
                return_value={"id": "msg-1"},
            ) as send_message:
                sent = mr.send_monthly_report_email(
                    credential_store=FakeCredentialStore({"scopes": ["https://www.googleapis.com/auth/gmail.send"]}),
                    owner_email="OWNER@NPT.SG",
                    recipient="",
                    subject="Subject",
                    draft_markdown="- Body",
                )
            self.assertEqual(sent.message_id, "msg-1")
            self.assertEqual(send_message.call_args.kwargs["sender"], "owner@npt.sg")

            with patch("bpmis_jira_tool.monthly_report.StoredGoogleCredentials") as credential_cls, patch("bpmis_jira_tool.monthly_report.Credentials", return_value=object()), patch(
                "bpmis_jira_tool.monthly_report.GmailDashboardService",
                return_value="gmail-service",
            ):
                credential_cls.return_value.load.return_value = {"scopes": []}
                with self.assertRaises(Exception):
                    service._build_gmail_service()
                credential_cls.return_value.load.return_value = {"scopes": ["https://www.googleapis.com/auth/gmail.readonly"]}
                self.assertEqual(service._build_gmail_service(), "gmail-service")

            mr._monthly_report_business_glossary.cache_clear()
            with patch("bpmis_jira_tool.monthly_report._monthly_report_read_json_file", side_effect=[None, None, None, {"domains": {"BAD": {}, "AF": {"aliases": ["fraud"]}}}]):
                glossary = mr._monthly_report_business_glossary()
            self.assertIn("_derived_source_counts", glossary)
            mr._monthly_report_business_glossary.cache_clear()


if __name__ == "__main__":
    unittest.main()
