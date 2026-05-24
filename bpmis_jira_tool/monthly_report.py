from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import hashlib
import html
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any

from google.oauth2.credentials import Credentials

from bpmis_jira_tool.codex_model_router import CODEX_ROUTE_DEEP, resolve_codex_model, resolve_codex_reasoning_effort
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE, GmailDashboardService
from bpmis_jira_tool.gmail_sender import GMAIL_SEND_SCOPE, StoredGoogleCredentials, credentials_from_payload, send_gmail_message
from bpmis_jira_tool.report_intelligence import (
    build_monthly_evidence_sidecar,
    filter_text_by_noise,
    normalize_report_intelligence_config,
)
from bpmis_jira_tool.seatalk_dashboard import SEATALK_INSIGHTS_TIMEZONE, SeaTalkDashboardService
from bpmis_jira_tool.source_code_qa import CodexCliBridgeSourceCodeQALLMProvider
from prd_briefing.confluence import ConfluenceConnector
from prd_briefing.reviewer import _build_prd_source


DEFAULT_MONTHLY_REPORT_RECIPIENT = "xiaodong.zheng@npt.sg"
MONTHLY_REPORT_PROMPT_VERSION = "v1_team_dashboard_monthly_report"
MONTHLY_REPORT_GENERATION_VERSION = "v8_historical_style_guide_v2"
MONTHLY_REPORT_PERIOD_ANCHOR_START = date(2026, 4, 13)
MONTHLY_REPORT_PERIOD_ANCHOR_END = date(2026, 5, 8)
MONTHLY_REPORT_PERIOD_DAYS = 28
MONTHLY_REPORT_EVIDENCE_DAYS = 14
MONTHLY_REPORT_SEATALK_HIGHLIGHT_CONVERSATION_SCOPE = "monthly-highlight"
MONTHLY_REPORT_PRODUCT_SCOPE = ("Anti-fraud", "Credit Risk", "Ops Risk")
MONTHLY_REPORT_SEATALK_DAYS = 28
MONTHLY_REPORT_MAX_SEATALK_CHARS = 640_000
MONTHLY_REPORT_MAX_PROJECTS = 30
MONTHLY_REPORT_MAX_TICKETS_PER_PROJECT = 18
MONTHLY_REPORT_MAX_PRD_PAGES = 10
MONTHLY_REPORT_MAX_PRD_CHARS_PER_PAGE = 8_000
MONTHLY_REPORT_MAX_DESCRIPTION_CHARS = 4_000
MONTHLY_REPORT_MAX_HIGHLIGHT_TOPICS = 6
MONTHLY_REPORT_TOKEN_CHARS_PER_TOKEN = 4
MONTHLY_REPORT_TOKEN_RISK_WARNING = 120_000
MONTHLY_REPORT_TOKEN_RISK_HIGH = 180_000
MONTHLY_REPORT_BATCH_TARGET_TOKENS = 55_000
MONTHLY_REPORT_TEXT_BATCH_TARGET_TOKENS = 28_000
MONTHLY_REPORT_GMAIL_BATCH_TARGET_TOKENS = 18_000
MONTHLY_REPORT_BATCH_MAX_TOKENS = 80_000
MONTHLY_REPORT_MERGE_MAX_TOKENS = 120_000
MONTHLY_REPORT_FINAL_MAX_TOKENS = 80_000
MONTHLY_REPORT_SUMMARY_MAX_CHARS = 14_000
MONTHLY_REPORT_BRIEF_MAX_CHARS = 64_000
MONTHLY_REPORT_MAX_VIP_GMAIL_THREADS = 60
MONTHLY_REPORT_MAX_REQUIREMENTS_GMAIL_THREADS = 12
MONTHLY_REPORT_MAX_HIGHLIGHT_GMAIL_THREADS_PER_TOPIC = 8
MONTHLY_REPORT_HIGHLIGHT_EVIDENCE_MAX_LINES = 24
MONTHLY_REPORT_EVIDENCE_WORKERS = 2
MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK = "seatalk"
MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL = "gmail"
MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD = "team_dashboard"
MONTHLY_REPORT_HIGHLIGHT_SOURCES = (
    MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK,
    MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL,
    MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD,
)
MONTHLY_REPORT_GMAIL_TOPIC_CACHE_VERSION = "v3"
MONTHLY_REPORT_GMAIL_TOPIC_CACHE_TTL_SECONDS = 6 * 60 * 60
MONTHLY_REPORT_PRD_SCOPE_CACHE_VERSION = "v1"
MONTHLY_REPORT_BATCH_SUMMARY_CACHE_VERSION = "v2"
MONTHLY_REPORT_TOPIC_NARRATIVE_CACHE_VERSION = "v1"
MONTHLY_REPORT_STYLE_GUIDE_CACHE_VERSION = "v2"
MONTHLY_REPORT_TOPIC_NARRATIVE_MAX_TOKENS = 32_000
MONTHLY_REPORT_STYLE_GUIDE_MAX_REPORTS = 12
MONTHLY_REPORT_STYLE_GUIDE_MAX_EXCERPT_CHARS = 2_400
MONTHLY_REPORT_STYLE_GUIDE_MAX_CHARS = 24_000
MONTHLY_REPORT_REQUIREMENTS_EMAILS = {
    "SG": {
        "subject": "SG_2026 Monthly Requirements Biweekly Update",
        "sender": "xinni.oon@npt.sg",
    },
    "ID": {
        "subject": "ID_Monthly Requirements Biweekly Update",
        "sender": "sisi.liang@npt.sg",
    },
    "PH": {
        "subject": "PH_2026 Monthly Requirements Biweekly Update",
        "sender": "yuanfang.zhou@npt.sg",
    },
}
MONTHLY_REPORT_PRODUCT_SCOPE_TERMS = (
    "anti-fraud",
    "antifraud",
    "anti fraud",
    "fraud",
    "afa",
    "af ",
    "af-",
    "credit risk",
    "credit-risk",
    "creditrisk",
    "crms",
    "crs",
    "loan",
    "collection",
    "ops risk",
    "operational risk",
    "grc",
    "rcsa",
    "risk control self assessment",
    "alc",
    "alcv12",
    "facial verification",
    "fv",
    "slik",
)
MONTHLY_REPORT_PROJECT_EVIDENCE_MAX_LINES = 8
MONTHLY_REPORT_PROJECT_EVIDENCE_MAX_GMAIL = 5
MONTHLY_REPORT_EXCLUDED_ASK_TERMS = (
    "db instability",
    "database instability",
    "local registration monitoring",
    "shopee acquisition",
    "onboarding health",
)
MONTHLY_REPORT_DIRECT_RISK_TERMS = (
    "capacity",
    "resource",
    "prioritization",
    "priority conflict",
    "blocked",
    "blocker",
    "delay",
    "延期",
    "risk",
)
MONTHLY_REPORT_DECISION_TERMS = (
    "confirm",
    "decide",
    "decision",
    "approval",
    "approve",
    "open question",
    "need confirmation",
    "to be confirmed",
    "是否",
    "确认",
)
MONTHLY_REPORT_CONFIG_ROOT = Path(__file__).resolve().parents[1] / "config"
MONTHLY_REPORT_BUSINESS_GLOSSARY_PATH = MONTHLY_REPORT_CONFIG_ROOT / "monthly_report_business_glossary.json"
MONTHLY_REPORT_SOURCE_CODE_QA_GLOSSARY_SOURCE_PATHS = (
    MONTHLY_REPORT_CONFIG_ROOT / "source_code_qa_domain_profiles.json",
    MONTHLY_REPORT_CONFIG_ROOT / "source_code_qa_effort_dictionaries.json",
    MONTHLY_REPORT_CONFIG_ROOT / "source_code_qa_domain_knowledge_packs.json",
)

DEFAULT_MONTHLY_REPORT_TEMPLATE = """# Monthly Report

## Highlights
- Cover only the user-provided highlight topics. Keep each highlight concise and evidence-backed.

## Key Project Progress
- Use this table structure:

| Region | Priority | Project | Current Status | Target Tech Live Date |
| --- | --- | --- | --- | --- |

## Blockers / Risks
- Highlight unresolved blockers, cross-team dependencies, delayed decisions, production or compliance risks, and owners where clear.

## Delivery Outlook
- Explain expected next steps and upcoming milestones for the next month.
"""


@dataclass(frozen=True)
class MonthlyReportSendResult:
    status: str
    recipient: str
    subject: str
    message_id: str = ""


@dataclass(frozen=True)
class MonthlyReportPeriod:
    start: datetime
    end: datetime
    end_exclusive: datetime
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    scheduled_end_exclusive: datetime | None = None

    @property
    def start_date(self) -> str:
        return self.start.date().isoformat()

    @property
    def end_date(self) -> str:
        return self.end.date().isoformat()

    @property
    def days(self) -> int:
        return max(1, (self.end_exclusive.date() - self.start.date()).days)

    @property
    def scheduled_start_date(self) -> str:
        return (self.scheduled_start or self.start).date().isoformat()

    @property
    def scheduled_end_date(self) -> str:
        return (self.scheduled_end or self.end).date().isoformat()


class MonthlyReportService:
    def __init__(
        self,
        *,
        settings: Settings,
        workspace_root: Path,
        seatalk_service: SeaTalkDashboardService,
        confluence: ConfluenceConnector | None = None,
        gmail_service: GmailDashboardService | None = None,
        now: datetime | None = None,
        report_intelligence_config: dict[str, Any] | None = None,
    ) -> None:
        self.settings = settings
        self.workspace_root = Path(workspace_root)
        self.seatalk_service = seatalk_service
        self.confluence = confluence
        self.gmail_service = gmail_service
        self.now = (now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)).astimezone(SEATALK_INSIGHTS_TIMEZONE)
        self.report_intelligence_config = normalize_report_intelligence_config(report_intelligence_config)

    def generate_draft(
        self,
        *,
        template: str,
        team_payloads: list[dict[str, Any]],
        report_intelligence_config: dict[str, Any] | None = None,
        period_start: str | None = None,
        period_end: str | None = None,
        period_end_exclusive: str | None = None,
        highlight_topics: list[str] | str | None = None,
        highlight_topic_sources: Any | None = None,
        product_scope: list[str] | None = None,
        historical_monthly_reports: list[dict[str, Any]] | None = None,
        historical_report_style_guide: dict[str, Any] | None = None,
        progress_callback: Any | None = None,
    ) -> dict[str, Any]:
        started_at = time.monotonic()
        timings: dict[str, float] = {}
        if report_intelligence_config is not None:
            self.report_intelligence_config = normalize_report_intelligence_config(report_intelligence_config)
        normalized_highlight_topics = normalize_monthly_report_highlight_topics(highlight_topics)
        report_period = _monthly_report_period_from_payload(
            period_start=period_start,
            period_end=period_end,
            period_end_exclusive=period_end_exclusive,
            fallback=self.now,
        )
        evidence_period = _monthly_report_evidence_period(report_period)
        effective_template = normalize_monthly_report_template(template)
        normalized_highlight_sources = normalize_monthly_report_highlight_topic_sources(
            highlight_topic_sources,
            normalized_highlight_topics,
        )
        style_guide = (
            historical_report_style_guide
            if isinstance(historical_report_style_guide, dict) and historical_report_style_guide.get("report_count")
            else build_monthly_report_historical_style_guide(historical_monthly_reports or [])
        )
        _emit_monthly_report_progress(progress_callback, "preparing_sources", "Preparing Key Projects, Jira, PRD, and SeaTalk sources.", 0, 0)
        key_projects = self._key_projects(team_payloads)
        highlight_project_matches = match_monthly_report_highlight_topics(normalized_highlight_topics, key_projects)
        highlight_project_ids = {
            str(project_id).strip()
            for match in highlight_project_matches
            if _monthly_report_topic_uses_source(
                str(match.get("topic") or ""),
                normalized_highlight_sources,
                MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD,
            )
            for project_id in (match.get("project_ids") or [])
            if str(project_id).strip()
        }
        seatalk_highlight_topics = [
            topic for topic in normalized_highlight_topics
            if _monthly_report_topic_uses_source(topic, normalized_highlight_sources, MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK)
        ]
        gmail_highlight_topics = [
            topic for topic in normalized_highlight_topics
            if _monthly_report_topic_uses_source(topic, normalized_highlight_sources, MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL)
        ]
        seatalk_project_matches = [
            match
            if _monthly_report_topic_uses_source(
                str(match.get("topic") or ""),
                normalized_highlight_sources,
                MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK,
            )
            else {**match, "project_ids": []}
            for match in highlight_project_matches
        ]
        highlight_seatalk_aliases = _highlight_seatalk_aliases(
            seatalk_highlight_topics,
            key_projects=key_projects,
            topic_project_matches=seatalk_project_matches,
        )
        step_started = time.monotonic()
        if seatalk_highlight_topics or highlight_seatalk_aliases:
            _emit_monthly_report_progress(progress_callback, "collecting_seatalk", "Exporting SeaTalk history for highlighted topics.", 0, 0)
            history_text, product_scope_filtered_count, seatalk_highlight_raw_match_count = self._seatalk_history(
                evidence_period,
                highlight_aliases=highlight_seatalk_aliases,
            )
        else:
            history_text = ""
            product_scope_filtered_count = 0
            seatalk_highlight_raw_match_count = 0
        _record_monthly_report_timing(timings, "seatalk_export", step_started)
        step_started = time.monotonic()
        if gmail_highlight_topics:
            _emit_monthly_report_progress(progress_callback, "searching_vip_gmail", "Searching VIP Gmail evidence for highlighted topics.", 0, 0)
            vip_gmail_text, vip_gmail_summary = self._vip_gmail_history(evidence_period)
        else:
            vip_gmail_text, vip_gmail_summary = "", {"thread_count": 0, "message_count": 0, "error_count": 0, "product_scope_filtered_count": 0}
        _record_monthly_report_timing(timings, "vip_gmail", step_started)
        step_started = time.monotonic()
        _emit_monthly_report_progress(progress_callback, "searching_requirements_gmail", "Searching Monthly Requirements Gmail evidence for target dates.", 0, 0)
        requirements_gmail_text, requirements_gmail_summary = self._requirements_gmail_history(evidence_period)
        monthly_requirements_targets = build_monthly_requirements_target_map(requirements_gmail_text)
        _record_monthly_report_timing(timings, "requirements_gmail", step_started)
        step_started = time.monotonic()
        highlight_gmail_evidence, highlight_gmail_summary = self._highlight_gmail_history(
            evidence_period,
            gmail_highlight_topics,
            progress_callback=progress_callback,
        )
        _record_monthly_report_timing(timings, "topic_gmail", step_started)
        step_started = time.monotonic()
        _emit_monthly_report_progress(progress_callback, "ingesting_prd", "Collecting PRD context for highlight projects.", 0, 0)
        prd_sources, prd_errors = self._prd_sources(key_projects, project_ids=highlight_project_ids)
        _record_monthly_report_timing(timings, "prd_ingest", step_started)
        step_started = time.monotonic()
        _emit_monthly_report_progress(
            progress_callback,
            "summarizing_prd_scope",
            "Summarizing PRD scope evidence.",
            0,
            len(prd_sources),
        )
        prd_scope_summaries = self._prd_scope_summaries(
            prd_sources=prd_sources,
            generated_at=self.now,
            report_period=evidence_period,
            progress_callback=progress_callback,
        )
        _record_monthly_report_timing(timings, "prd_summary", step_started)
        _emit_monthly_report_progress(progress_callback, "building_evidence", "Building Monthly Report evidence from collected sources.", 0, 0)
        monthly_evidence_brief = build_monthly_project_evidence_brief(
            key_projects=key_projects,
            seatalk_history_text=history_text,
            vip_gmail_text=vip_gmail_text,
            monthly_requirements_targets=monthly_requirements_targets,
            prd_scope_summaries=prd_scope_summaries,
            report_period=evidence_period,
            highlight_project_ids=highlight_project_ids,
            fallback_reference_date=self.now.date(),
        )
        highlight_deep_evidence = build_monthly_highlight_deep_evidence(
            highlight_topics=normalized_highlight_topics,
            key_projects=key_projects,
            topic_project_matches=highlight_project_matches,
            seatalk_history_text=history_text,
            topic_gmail_evidence=highlight_gmail_evidence,
            monthly_requirements_targets=monthly_requirements_targets,
            prd_scope_summaries=prd_scope_summaries,
            report_period=evidence_period,
            highlight_topic_sources=normalized_highlight_sources,
            fallback_reference_date=self.now.date(),
        )
        compact_highlight_deep_evidence = _compact_highlight_deep_evidence_for_prompt(
            highlight_deep_evidence,
            include_source_evidence=True,
        )
        highlight_evidence_map = build_monthly_highlight_evidence_map(highlight_deep_evidence)
        step_started = time.monotonic()
        highlight_narratives = self._highlight_topic_narratives(
            generated_at=self.now,
            report_period=report_period,
            highlight_deep_evidence=highlight_deep_evidence,
            progress_callback=progress_callback,
        )
        _record_monthly_report_timing(timings, "highlight_narrative", step_started)
        included_project_briefs = [
            item for item in monthly_evidence_brief if item.get("include")
        ]
        compact_project_briefs_for_batch = _compact_monthly_project_evidence_for_batch(included_project_briefs)
        evidence_sidecar = build_monthly_evidence_sidecar(
            seatalk_history_text="\n".join(
                item
                for item in [
                    _highlight_deep_evidence_text(highlight_deep_evidence),
                    vip_gmail_text,
                ]
                if item.strip()
            ),
            key_projects=[_project_from_evidence_item(item) for item in included_project_briefs],
            prd_sources=prd_scope_summaries,
            config=self.report_intelligence_config,
        )
        step_started = time.monotonic()
        batch_summaries = self._batch_summaries(
            template=effective_template,
            generated_at=self.now,
            report_period=report_period,
            highlight_topics=normalized_highlight_topics,
            monthly_evidence_brief=compact_project_briefs_for_batch,
            highlight_deep_evidence=compact_highlight_deep_evidence,
            prd_errors=prd_errors,
            evidence_sidecar=evidence_sidecar,
            progress_callback=progress_callback,
        )
        _record_monthly_report_timing(timings, "batch_summary", step_started)
        step_started = time.monotonic()
        evidence_brief = self._merge_batch_summaries(
            generated_at=self.now,
            report_period=report_period,
            highlight_topics=normalized_highlight_topics,
            batch_summaries=batch_summaries,
            prd_errors=prd_errors,
            progress_callback=progress_callback,
        )
        _record_monthly_report_timing(timings, "merge", step_started)
        prompt = build_monthly_report_final_prompt(
            template=effective_template,
            generated_at=self.now,
            report_period=report_period,
            highlight_topics=normalized_highlight_topics,
            evidence_brief=evidence_brief,
            monthly_evidence_brief=monthly_evidence_brief,
            highlight_deep_evidence=compact_highlight_deep_evidence,
            highlight_narratives=highlight_narratives,
            historical_report_style_guide=style_guide,
        )
        final_estimated_tokens = _estimate_token_count(prompt)
        if final_estimated_tokens > MONTHLY_REPORT_FINAL_MAX_TOKENS:
            evidence_brief = self._compress_evidence_brief(
                generated_at=self.now,
                evidence_brief=evidence_brief,
                progress_callback=progress_callback,
            )
            prompt = build_monthly_report_final_prompt(
                template=effective_template,
                generated_at=self.now,
                report_period=report_period,
                highlight_topics=normalized_highlight_topics,
                evidence_brief=evidence_brief,
                monthly_evidence_brief=monthly_evidence_brief,
                highlight_deep_evidence=compact_highlight_deep_evidence,
                highlight_narratives=highlight_narratives,
                historical_report_style_guide=style_guide,
            )
            final_estimated_tokens = _estimate_token_count(prompt)
        if final_estimated_tokens > MONTHLY_REPORT_FINAL_MAX_TOKENS:
            raise ToolError(
                "Monthly Report evidence is still too large after batching and compression. "
                f"Estimated final prompt tokens: {final_estimated_tokens}."
            )
        _emit_monthly_report_progress(
            progress_callback,
            "generating_final_draft",
            "Generating final Monthly Report draft from compressed evidence.",
            1,
            1,
            estimated_prompt_tokens=final_estimated_tokens,
        )
        step_started = time.monotonic()
        generated = self._guarded_generate(
            prompt=prompt,
            prompt_mode=f"{MONTHLY_REPORT_PROMPT_VERSION}_final",
            max_tokens=MONTHLY_REPORT_FINAL_MAX_TOKENS,
            progress_callback=progress_callback,
        )
        _record_monthly_report_timing(timings, "final", step_started)
        draft_markdown = _apply_monthly_report_project_tables(
            _sanitize_monthly_report_output(str(generated.get("result_markdown") or "")),
            included_project_briefs,
        )
        elapsed_seconds = round(time.monotonic() - started_at, 1)
        timings["total"] = elapsed_seconds
        prompt_chars = len(prompt)
        estimated_prompt_tokens = final_estimated_tokens
        batch_token_counts = [
            int(item.get("estimated_prompt_tokens") or 0)
            for item in batch_summaries
            if isinstance(item, dict)
        ]
        stage_token_ledger = {
            "seatalk_compact_estimated_tokens": _estimate_token_count(history_text),
            "vip_gmail_estimated_tokens": _estimate_token_count(vip_gmail_text),
            "requirements_gmail_estimated_tokens": _estimate_token_count(requirements_gmail_text),
            "highlight_deep_evidence_estimated_tokens": _estimate_token_count(_json_block(highlight_deep_evidence)),
            "highlight_deep_evidence_prompt_estimated_tokens": _estimate_token_count(_json_block(compact_highlight_deep_evidence)),
            "monthly_evidence_brief_estimated_tokens": _estimate_token_count(_json_block(monthly_evidence_brief)),
            "monthly_evidence_brief_batch_estimated_tokens": _estimate_token_count(_json_block(compact_project_briefs_for_batch)),
            "batch_summary_total_estimated_tokens": sum(batch_token_counts),
            "merge_input_estimated_tokens": _estimate_token_count(_json_block(batch_summaries)),
            "final_estimated_tokens": final_estimated_tokens,
        }
        confidence_counts = _monthly_report_confidence_counts(highlight_evidence_map)
        target_source_counts = _monthly_report_target_source_counts(monthly_evidence_brief)
        diagnostics = build_monthly_report_generation_diagnostics(
            highlight_evidence_map=highlight_evidence_map,
            monthly_evidence_brief=monthly_evidence_brief,
            batch_summaries=batch_summaries,
            highlight_narratives=highlight_narratives,
            timings=timings,
        )
        highlight_evidence_debug = [
            item.get("evidence_debug")
            for item in highlight_deep_evidence
            if isinstance(item, dict) and isinstance(item.get("evidence_debug"), dict)
        ]
        evidence_review = build_monthly_report_evidence_review(highlight_deep_evidence)
        return {
            "status": "ok",
            "draft_markdown": draft_markdown,
            "generated_at": self.now.isoformat(),
            "subject": monthly_report_subject(period=report_period),
            "highlight_topics": normalized_highlight_topics,
            "highlight_topic_sources": normalized_highlight_sources,
            "generation_version": MONTHLY_REPORT_GENERATION_VERSION,
            "model_id": generated["model_id"],
            "trace": generated["trace"],
            "highlight_evidence_map": highlight_evidence_map,
            "highlight_evidence_debug": highlight_evidence_debug,
            "evidence_debug": highlight_evidence_debug,
            "evidence_review": evidence_review,
            "highlight_narratives": highlight_narratives,
            "generation_diagnostics": diagnostics,
            "generation_summary": {
                "generation_version": MONTHLY_REPORT_GENERATION_VERSION,
                "period_start": report_period.start_date,
                "period_end": report_period.end_date,
                "period_end_exclusive": report_period.end_exclusive.isoformat(),
                "highlight_topics": normalized_highlight_topics,
                "highlight_topic_sources": normalized_highlight_sources,
                "historical_style_report_count": int(style_guide.get("report_count") or 0),
                "scheduled_period_start": report_period.scheduled_start_date,
                "scheduled_period_end": report_period.scheduled_end_date,
                "effective_period_start": report_period.start_date,
                "effective_period_end": report_period.end_date,
                "evidence_period_start": evidence_period.start_date,
                "evidence_period_end": evidence_period.end_date,
                "elapsed_seconds": elapsed_seconds,
                "prompt_chars": prompt_chars,
                "estimated_prompt_tokens": estimated_prompt_tokens,
                "token_risk": _monthly_report_token_risk(estimated_prompt_tokens),
                "seatalk_history_chars": len(history_text),
                "seatalk_conversation_scope": MONTHLY_REPORT_SEATALK_HIGHLIGHT_CONVERSATION_SCOPE,
                "max_seatalk_chars": MONTHLY_REPORT_MAX_SEATALK_CHARS,
                "evidence_debug_topic_count": len(highlight_evidence_debug),
                "evidence_review_topic_count": len(evidence_review),
                "total_batches": len(batch_summaries),
                "batch_summary_cache_hit_count": len([item for item in batch_summaries if item.get("cache_hit")]),
                "highlight_narrative_cache_hit_count": len([item for item in highlight_narratives if item.get("cache_hit")]),
                "max_batch_estimated_tokens": max(batch_token_counts) if batch_token_counts else 0,
                "final_estimated_tokens": final_estimated_tokens,
                "stage_token_ledger": stage_token_ledger,
                "batch_mode": True,
                "timings": timings,
            },
            "evidence_summary": {
                "seatalk_days": evidence_period.days,
                "key_project_count": len(included_project_briefs),
                "candidate_key_project_count": len(key_projects),
                "excluded_project_count": len([item for item in monthly_evidence_brief if not item.get("include")]),
                "jira_ticket_count": sum(len(project.get("jira_ids") or []) for project in included_project_briefs),
                "prd_page_count": len(prd_sources),
                "prd_error_count": len(prd_errors),
                "prd_scope_summary_count": len(prd_scope_summaries),
                "report_intelligence_evidence_count": len(evidence_sidecar),
                "highlight_topic_count": len(normalized_highlight_topics),
                "highlight_topic_sources": normalized_highlight_sources,
                "highlight_project_topic_count": len([item for item in highlight_project_matches if item.get("project_ids")]),
                "highlight_confidence_counts": confidence_counts,
                "highlight_low_confidence_count": int(confidence_counts.get("low") or 0),
                "target_tech_live_source_counts": target_source_counts,
                "highlight_seatalk_line_match_count": sum(
                    len(item.get("seatalk_evidence") or [])
                    for item in highlight_deep_evidence
                    if isinstance(item, dict)
                ),
                "highlight_seatalk_raw_match_count": seatalk_highlight_raw_match_count,
                "highlight_gmail_thread_count": int(highlight_gmail_summary.get("thread_count") or 0),
                "highlight_gmail_message_count": int(highlight_gmail_summary.get("message_count") or 0),
                "highlight_gmail_cache_hit_count": int(highlight_gmail_summary.get("cache_hit_count") or 0),
                "highlight_google_sheet_count": int(highlight_gmail_summary.get("google_sheet_count") or 0),
                "historical_style_report_count": int(style_guide.get("report_count") or 0),
                "vip_gmail_thread_count": int(vip_gmail_summary.get("thread_count") or 0),
                "vip_gmail_message_count": int(vip_gmail_summary.get("message_count") or 0),
                "monthly_requirements_gmail_thread_count": int(requirements_gmail_summary.get("thread_count") or 0),
                "monthly_requirements_gmail_message_count": int(requirements_gmail_summary.get("message_count") or 0),
                "monthly_requirements_target_row_count": len(monthly_requirements_targets),
                "monthly_requirements_target_date_count": len([
                    item for item in monthly_evidence_brief
                    if item.get("target_tech_live_source") == "monthly_requirements_email"
                ]),
                "gmail_error_count": int(vip_gmail_summary.get("error_count") or 0) + int(highlight_gmail_summary.get("error_count") or 0) + int(requirements_gmail_summary.get("error_count") or 0),
                "product_scope_filtered_count": product_scope_filtered_count + int(vip_gmail_summary.get("product_scope_filtered_count") or 0),
                "prd_scope_cache_hit_count": len([item for item in prd_scope_summaries if item.get("cache_hit")]),
            },
        }

    def _batch_summaries(
        self,
        *,
        template: str,
        generated_at: datetime,
        report_period: MonthlyReportPeriod,
        highlight_topics: list[str],
        monthly_evidence_brief: list[dict[str, Any]],
        highlight_deep_evidence: list[dict[str, Any]],
        prd_errors: list[str],
        evidence_sidecar: list[dict[str, Any]],
        progress_callback: Any | None,
    ) -> list[dict[str, Any]]:
        batches: list[dict[str, Any]] = []
        for index, chunk in enumerate(_split_json_items_for_token_limit(highlight_deep_evidence, MONTHLY_REPORT_BATCH_TARGET_TOKENS), start=1):
            batches.append({"source": "highlight_deep_evidence", "index": index, "payload": chunk})
        for index, chunk in enumerate(_split_json_items_for_token_limit(monthly_evidence_brief, MONTHLY_REPORT_BATCH_TARGET_TOKENS), start=1):
            batches.append({"source": "monthly_evidence_brief", "index": index, "payload": chunk})
        for index, chunk in enumerate(_split_json_items_for_token_limit(evidence_sidecar, MONTHLY_REPORT_BATCH_TARGET_TOKENS), start=1):
            batches.append({"source": "report_intelligence", "index": index, "payload": chunk})
        if not batches:
            batches.append({"source": "empty", "index": 1, "payload": "No readable monthly report evidence was found."})

        summaries: list[dict[str, Any]] = []
        total = len(batches)
        for current, batch in enumerate(batches, start=1):
            prompt = build_monthly_report_batch_prompt(
                template=template,
                generated_at=generated_at,
                report_period=report_period,
                highlight_topics=highlight_topics,
                source=str(batch.get("source") or ""),
                payload=batch.get("payload"),
                prd_errors=prd_errors,
            )
            estimated_tokens = _estimate_token_count(prompt)
            source_label = _monthly_report_source_label(str(batch.get("source") or ""))
            cache_path = _monthly_report_batch_summary_cache_path(
                self.settings,
                report_period=report_period,
                source=str(batch.get("source") or ""),
                index=_safe_int(batch.get("index")),
                highlight_topics=highlight_topics,
                template=template,
                payload=batch.get("payload"),
                prd_errors=prd_errors,
            )
            cached = _read_monthly_report_json_cache(cache_path)
            if isinstance(cached, dict) and str(cached.get("summary_markdown") or "").strip():
                _emit_monthly_report_progress(
                    progress_callback,
                    f"summarizing_{batch.get('source')}",
                    f"Using cached {source_label} batch {current}/{total}.",
                    current,
                    total,
                    estimated_prompt_tokens=estimated_tokens,
                )
                summaries.append(
                    {
                        "source": batch.get("source"),
                        "index": batch.get("index"),
                        "summary_markdown": str(cached.get("summary_markdown") or "").strip()[:MONTHLY_REPORT_SUMMARY_MAX_CHARS],
                        "estimated_prompt_tokens": estimated_tokens,
                        "model_id": str(cached.get("model_id") or ""),
                        "trace": cached.get("trace") if isinstance(cached.get("trace"), dict) else {},
                        "cache_hit": True,
                    }
                )
                continue
            _emit_monthly_report_progress(
                progress_callback,
                f"summarizing_{batch.get('source')}",
                f"Summarizing {source_label} batch {current}/{total}.",
                current,
                total,
                estimated_prompt_tokens=estimated_tokens,
            )
            generated = self._guarded_generate(
                prompt=prompt,
                prompt_mode=f"{MONTHLY_REPORT_PROMPT_VERSION}_batch_{batch.get('source')}",
                max_tokens=MONTHLY_REPORT_BATCH_MAX_TOKENS,
                progress_callback=progress_callback,
            )
            summary = str(generated.get("result_markdown") or "").strip()
            item = {
                "source": batch.get("source"),
                "index": batch.get("index"),
                "summary_markdown": summary[:MONTHLY_REPORT_SUMMARY_MAX_CHARS],
                "estimated_prompt_tokens": estimated_tokens,
                "model_id": generated.get("model_id"),
                "trace": generated.get("trace") or {},
                "cache_hit": False,
            }
            _write_monthly_report_json_cache(cache_path, item)
            summaries.append(item)
        return summaries

    def _merge_batch_summaries(
        self,
        *,
        generated_at: datetime,
        report_period: MonthlyReportPeriod,
        highlight_topics: list[str],
        batch_summaries: list[dict[str, Any]],
        prd_errors: list[str],
        progress_callback: Any | None,
    ) -> str:
        prompt = build_monthly_report_merge_prompt(
            generated_at=generated_at,
            report_period=report_period,
            highlight_topics=highlight_topics,
            batch_summaries=batch_summaries,
            prd_errors=prd_errors,
        )
        estimated_tokens = _estimate_token_count(prompt)
        if estimated_tokens > MONTHLY_REPORT_MERGE_MAX_TOKENS:
            compacted = [
                {
                    "source": item.get("source"),
                    "index": item.get("index"),
                    "summary_markdown": str(item.get("summary_markdown") or "")[:6_000],
                }
                for item in batch_summaries
            ]
            prompt = build_monthly_report_merge_prompt(
                generated_at=generated_at,
                report_period=report_period,
                highlight_topics=highlight_topics,
                batch_summaries=compacted,
                prd_errors=prd_errors,
            )
            estimated_tokens = _estimate_token_count(prompt)
        if estimated_tokens > MONTHLY_REPORT_MERGE_MAX_TOKENS:
            raise ToolError(
                "Monthly Report batch summaries are still too large to merge safely. "
                f"Estimated merge prompt tokens: {estimated_tokens}."
            )
        _emit_monthly_report_progress(
            progress_callback,
            "merging_summaries",
            "Merging batch summaries into a compact evidence brief.",
            1,
            1,
            estimated_prompt_tokens=estimated_tokens,
        )
        generated = self._guarded_generate(
            prompt=prompt,
            prompt_mode=f"{MONTHLY_REPORT_PROMPT_VERSION}_merge",
            max_tokens=MONTHLY_REPORT_MERGE_MAX_TOKENS,
            progress_callback=progress_callback,
        )
        return str(generated.get("result_markdown") or "").strip()[:MONTHLY_REPORT_BRIEF_MAX_CHARS]

    def _compress_evidence_brief(
        self,
        *,
        generated_at: datetime,
        evidence_brief: str,
        progress_callback: Any | None,
    ) -> str:
        prompt = build_monthly_report_compress_prompt(generated_at=generated_at, evidence_brief=evidence_brief)
        estimated_tokens = _estimate_token_count(prompt)
        if estimated_tokens > MONTHLY_REPORT_MERGE_MAX_TOKENS:
            prompt = build_monthly_report_compress_prompt(
                generated_at=generated_at,
                evidence_brief=evidence_brief[:MONTHLY_REPORT_BRIEF_MAX_CHARS],
            )
            estimated_tokens = _estimate_token_count(prompt)
        if estimated_tokens > MONTHLY_REPORT_MERGE_MAX_TOKENS:
            raise ToolError(
                "Monthly Report evidence brief is too large to compress safely. "
                f"Estimated compression prompt tokens: {estimated_tokens}."
            )
        _emit_monthly_report_progress(
            progress_callback,
            "compressing_evidence",
            "Compressing evidence brief before final draft generation.",
            1,
            1,
            estimated_prompt_tokens=estimated_tokens,
        )
        generated = self._guarded_generate(
            prompt=prompt,
            prompt_mode=f"{MONTHLY_REPORT_PROMPT_VERSION}_compress",
            max_tokens=MONTHLY_REPORT_MERGE_MAX_TOKENS,
            progress_callback=progress_callback,
        )
        return str(generated.get("result_markdown") or "").strip()[:MONTHLY_REPORT_BRIEF_MAX_CHARS]

    def _guarded_generate(
        self,
        *,
        prompt: str,
        prompt_mode: str,
        max_tokens: int,
        progress_callback: Any | None,
    ) -> dict[str, Any]:
        estimated_tokens = _estimate_token_count(prompt)
        if estimated_tokens > max_tokens:
            raise ToolError(
                "Monthly Report prompt exceeded the safe per-call token limit before model invocation. "
                f"Prompt mode: {prompt_mode}. Estimated tokens: {estimated_tokens}. Limit: {max_tokens}."
            )
        return generate_monthly_report_with_codex(
            prompt=prompt,
            settings=self.settings,
            workspace_root=self.workspace_root,
            prompt_mode=prompt_mode,
            progress_callback=progress_callback,
        )

    def _seatalk_history(self, report_period: MonthlyReportPeriod, *, highlight_aliases: set[str] | None = None) -> tuple[str, int, int]:
        history = self.seatalk_service.export_history_since(
            since=report_period.start,
            now=report_period.end_exclusive,
            days=report_period.days + 1,
            conversation_scope=None if highlight_aliases else MONTHLY_REPORT_SEATALK_HIGHLIGHT_CONVERSATION_SCOPE,
        )
        history = self.seatalk_service._filter_system_generated_history(history)
        history = filter_text_by_noise(history, config=self.report_intelligence_config, source="seatalk")
        history, filtered_count, highlight_match_count = _filter_text_by_product_scope_or_highlight_aliases(
            history,
            highlight_aliases or set(),
        )
        compacted = self.seatalk_service._compact_history_for_insights(
            history,
            max_chars=MONTHLY_REPORT_MAX_SEATALK_CHARS,
            signal_max_chars=420_000,
            recent_max_chars=220_000,
        )
        return compacted, filtered_count, highlight_match_count

    def _vip_gmail_history(self, report_period: MonthlyReportPeriod) -> tuple[str, dict[str, int]]:
        vip_emails = _vip_emails(self.report_intelligence_config)
        if not vip_emails:
            return "", {"thread_count": 0, "message_count": 0, "error_count": 0, "product_scope_filtered_count": 0}
        try:
            gmail_service = self.gmail_service or self._build_gmail_service()
            payload = gmail_service.export_contact_thread_history_since(
                since=report_period.start,
                now=report_period.end_exclusive,
                contact_emails=vip_emails,
                max_threads=MONTHLY_REPORT_MAX_VIP_GMAIL_THREADS,
            )
        except Exception as error:  # noqa: BLE001 - Gmail evidence should not block monthly report generation.
            return (
                "\n".join(
                    [
                        "VIP Gmail evidence gap",
                        f"Gmail VIP evidence could not be loaded for {report_period.start_date} to {report_period.end_date}: {error}",
                    ]
                ),
                {"thread_count": 0, "message_count": 0, "error_count": 1, "product_scope_filtered_count": 0},
            )
        text, filtered_count = _filter_thread_export_by_product_scope(str((payload or {}).get("text") or ""))
        return text, {
            "thread_count": int((payload or {}).get("thread_count") or 0),
            "message_count": int((payload or {}).get("message_count") or 0),
            "error_count": 0,
            "product_scope_filtered_count": filtered_count,
        }

    def _requirements_gmail_history(self, report_period: MonthlyReportPeriod) -> tuple[str, dict[str, int]]:
        try:
            gmail_service = self.gmail_service or self._build_gmail_service()
            if not hasattr(gmail_service, "export_monthly_requirements_thread_history_since"):
                return "", {"thread_count": 0, "message_count": 0, "error_count": 0}
            payload = gmail_service.export_monthly_requirements_thread_history_since(
                since=report_period.start,
                now=report_period.end_exclusive,
                configs=MONTHLY_REPORT_REQUIREMENTS_EMAILS,
                max_threads=MONTHLY_REPORT_MAX_REQUIREMENTS_GMAIL_THREADS,
            )
        except Exception as error:  # noqa: BLE001 - target date email evidence should not block report generation.
            return (
                "\n".join(
                    [
                        "Monthly Requirements Gmail evidence gap",
                        f"Monthly Requirements evidence could not be loaded for {report_period.start_date} to {report_period.end_date}: {error}",
                    ]
                ),
                {"thread_count": 0, "message_count": 0, "error_count": 1},
            )
        return str((payload or {}).get("text") or "").strip(), {
            "thread_count": int((payload or {}).get("thread_count") or 0),
            "message_count": int((payload or {}).get("message_count") or 0),
            "error_count": 0,
        }

    def _highlight_gmail_history(
        self,
        report_period: MonthlyReportPeriod,
        highlight_topics: list[str],
        *,
        progress_callback: Any | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        if not highlight_topics:
            return [], {"thread_count": 0, "message_count": 0, "error_count": 0, "cache_hit_count": 0}
        total_topics = len(highlight_topics)
        _emit_monthly_report_progress(
            progress_callback,
            "searching_topic_gmail",
            "Searching Gmail evidence for highlight topics.",
            0,
            total_topics,
        )

        def load_topic(index: int, topic: str) -> tuple[int, dict[str, Any]]:
            cache_path = _monthly_report_gmail_topic_cache_path(
                self.settings,
                owner_email=_monthly_report_gmail_owner_email(self.settings),
                report_period=report_period,
                topic=topic,
            )
            cached = _read_monthly_report_json_cache(cache_path, max_age_seconds=MONTHLY_REPORT_GMAIL_TOPIC_CACHE_TTL_SECONDS)
            if isinstance(cached, dict):
                return index, {
                    "topic": topic,
                    "text": str(cached.get("text") or "").strip(),
                    "thread_count": _safe_int(cached.get("thread_count")),
                    "message_count": _safe_int(cached.get("message_count")),
                    "query": str(cached.get("query") or ""),
                    "drive_links": _compact_text_list(cached.get("drive_links"), limit=8, max_chars=500),
                    "google_sheet_evidence": _normalize_google_sheet_evidence(cached.get("google_sheet_evidence")),
                    "cache_hit": True,
                }
            try:
                gmail_service = self.gmail_service or self._build_gmail_service()
                payload = gmail_service.export_topic_thread_history_since(
                    since=report_period.start,
                    now=report_period.end_exclusive,
                    topic=topic,
                    max_threads=MONTHLY_REPORT_MAX_HIGHLIGHT_GMAIL_THREADS_PER_TOPIC,
                )
                text = str((payload or {}).get("text") or "").strip()
                item_thread_count = int((payload or {}).get("thread_count") or 0)
                item_message_count = int((payload or {}).get("message_count") or 0)
                drive_links = _compact_text_list((payload or {}).get("drive_links"), limit=8, max_chars=500)
                google_sheet_evidence = []
                if item_thread_count > 0 and drive_links and hasattr(gmail_service, "export_google_sheet_link_texts"):
                    google_sheet_evidence = _normalize_google_sheet_evidence(
                        gmail_service.export_google_sheet_link_texts(drive_links)
                    )
                item = {
                    "topic": topic,
                    "text": text,
                    "thread_count": item_thread_count,
                    "message_count": item_message_count,
                    "query": str((payload or {}).get("query") or ""),
                    "drive_links": drive_links,
                    "google_sheet_evidence": google_sheet_evidence,
                    "cache_hit": False,
                }
                _write_monthly_report_json_cache(cache_path, item)
                return index, item
            except Exception as error:  # noqa: BLE001 - per-topic Gmail failures should not block report generation.
                return index, {
                    "topic": topic,
                    "text": "",
                    "thread_count": 0,
                    "message_count": 0,
                    "cache_hit": False,
                    "error": f"Gmail topic evidence could not be loaded: {error}",
                }

        ordered: list[dict[str, Any] | None] = [None] * len(highlight_topics)
        workers = min(MONTHLY_REPORT_EVIDENCE_WORKERS, len(highlight_topics))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(load_topic, index, topic)
                for index, topic in enumerate(highlight_topics)
            ]
            for completed, future in enumerate(as_completed(futures), start=1):
                index, item = future.result()
                ordered[index] = item
                topic_label = str(item.get("topic") or "highlight topic").strip()
                _emit_monthly_report_progress(
                    progress_callback,
                    "searching_topic_gmail",
                    f"Loaded Gmail evidence for highlight topic {completed}/{total_topics}: {topic_label}.",
                    completed,
                    total_topics,
                )
        items = [item for item in ordered if item is not None]
        return items, {
            "thread_count": sum(_safe_int(item.get("thread_count")) for item in items),
            "message_count": sum(_safe_int(item.get("message_count")) for item in items),
            "error_count": len([item for item in items if item.get("error")]),
            "cache_hit_count": len([item for item in items if item.get("cache_hit")]),
            "google_sheet_count": sum(len(item.get("google_sheet_evidence") or []) for item in items),
        }

    def _highlight_topic_narratives(
        self,
        *,
        generated_at: datetime,
        report_period: MonthlyReportPeriod,
        highlight_deep_evidence: list[dict[str, Any]],
        progress_callback: Any | None,
    ) -> list[dict[str, Any]]:
        if not highlight_deep_evidence:
            return []
        narratives: list[dict[str, Any]] = []
        total = len(highlight_deep_evidence)
        for current, item in enumerate(highlight_deep_evidence, start=1):
            topic = str(item.get("topic") or f"Highlight {current}").strip()
            cache_path = _monthly_report_topic_narrative_cache_path(
                self.settings,
                report_period=report_period,
                topic=topic,
                evidence=item,
            )
            cached = _read_monthly_report_json_cache(cache_path)
            if isinstance(cached, dict) and str(cached.get("narrative_markdown") or "").strip():
                _emit_monthly_report_progress(
                    progress_callback,
                    "generating_highlight_narrative",
                    f"Using cached Highlight narrative {current}/{total}: {topic}.",
                    current,
                    total,
                )
                narratives.append(
                    {
                        "topic": topic,
                        "confidence": str(cached.get("confidence") or item.get("confidence") or "").strip(),
                        "topic_intent": str(cached.get("topic_intent") or item.get("topic_intent") or "").strip(),
                        "narrative_markdown": str(cached.get("narrative_markdown") or "").strip()[:MONTHLY_REPORT_SUMMARY_MAX_CHARS],
                        "cache_hit": True,
                        "model_id": str(cached.get("model_id") or ""),
                        "trace": cached.get("trace") if isinstance(cached.get("trace"), dict) else {},
                    }
                )
                continue
            prompt = build_monthly_highlight_topic_narrative_prompt(
                generated_at=generated_at,
                report_period=report_period,
                topic_evidence=item,
            )
            estimated_tokens = _estimate_token_count(prompt)
            _emit_monthly_report_progress(
                progress_callback,
                "generating_highlight_narrative",
                f"Generating Highlight narrative {current}/{total}: {topic}.",
                current,
                total,
                estimated_prompt_tokens=estimated_tokens,
            )
            generated = self._guarded_generate(
                prompt=prompt,
                prompt_mode=f"{MONTHLY_REPORT_PROMPT_VERSION}_highlight_topic_narrative",
                max_tokens=MONTHLY_REPORT_TOPIC_NARRATIVE_MAX_TOKENS,
                progress_callback=progress_callback,
            )
            narrative = _sanitize_monthly_report_output(str(generated.get("result_markdown") or "")).strip()
            narrative_item = {
                "topic": topic,
                "confidence": str(item.get("confidence") or "").strip(),
                "topic_intent": str(item.get("topic_intent") or "").strip(),
                "narrative_markdown": narrative[:MONTHLY_REPORT_SUMMARY_MAX_CHARS],
                "cache_hit": False,
                "model_id": generated.get("model_id"),
                "trace": generated.get("trace") or {},
            }
            _write_monthly_report_json_cache(cache_path, narrative_item)
            narratives.append(narrative_item)
        return narratives

    def _build_gmail_service(self) -> GmailDashboardService:
        data_root = _monthly_report_data_root(self.settings)
        owner_email = str(self.settings.gmail_seatalk_demo_owner_email or self.settings.seatalk_owner_email or "").strip().lower()
        credential_store = StoredGoogleCredentials(
            data_root / "google" / "credentials.json",
            encryption_key=self.settings.team_portal_config_encryption_key,
        )
        credentials_payload = credential_store.load(owner_email=owner_email)
        scopes = {str(scope).strip() for scope in (credentials_payload.get("scopes") or []) if str(scope).strip()}
        if GMAIL_READONLY_SCOPE not in scopes:
            raise ConfigError("Gmail read permission is missing. Reconnect Google once to grant gmail.readonly.")
        credentials = Credentials(**credentials_payload)
        return GmailDashboardService(
            credentials=credentials,
            cache_key=owner_email,
            report_intelligence_config=self.report_intelligence_config,
        )

    def _key_projects(self, team_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        projects: dict[str, dict[str, Any]] = {}
        for team in team_payloads:
            team_key = str(team.get("team_key") or "").strip()
            team_label = str(team.get("label") or team_key).strip()
            member_emails = _normalized_email_set(team.get("member_emails") or [])
            for section_key in ("under_prd", "pending_live"):
                for raw_project in team.get(section_key) or []:
                    if not isinstance(raw_project, dict) or not raw_project.get("is_key_project"):
                        continue
                    if not _is_project_in_product_scope(raw_project, team_key=team_key, team_label=team_label):
                        continue
                    bpmis_id = str(raw_project.get("bpmis_id") or "").strip()
                    if not bpmis_id:
                        continue
                    project = projects.setdefault(
                        bpmis_id,
                        {
                            "bpmis_id": bpmis_id,
                            "project_name": str(raw_project.get("project_name") or "").strip(),
                            "market": str(raw_project.get("market") or "").strip(),
                            "priority": str(raw_project.get("priority") or "").strip(),
                            "regional_pm_pic": str(raw_project.get("regional_pm_pic") or "").strip(),
                            "status": str(raw_project.get("status") or "").strip(),
                            "release_date": str(raw_project.get("release_date") or "").strip(),
                            "key_project_source": str(raw_project.get("key_project_source") or "").strip(),
                            "teams": [],
                            "jira_tickets": [],
                        },
                    )
                    if team_label and team_label not in project["teams"]:
                        project["teams"].append(team_label)
                    self._merge_project_fields(project, raw_project)
                    seen_tickets = {
                        str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip()
                        for ticket in project["jira_tickets"]
                        if isinstance(ticket, dict)
                    }
                    for ticket in raw_project.get("jira_tickets") or []:
                        if not isinstance(ticket, dict):
                            continue
                        pm_email = str(ticket.get("pm_email") or "").strip().lower()
                        if member_emails and pm_email and pm_email not in member_emails:
                            continue
                        ticket_key = str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip()
                        if ticket_key and ticket_key in seen_tickets:
                            continue
                        if ticket_key:
                            seen_tickets.add(ticket_key)
                        project["jira_tickets"].append(_compact_ticket(ticket))
        ordered = sorted(
            projects.values(),
            key=lambda item: (
                _priority_rank(item.get("priority")),
                str(item.get("release_date") or "9999-99-99"),
                str(item.get("project_name") or "").casefold(),
            ),
        )
        for project in ordered:
            project["jira_tickets"] = project["jira_tickets"][:MONTHLY_REPORT_MAX_TICKETS_PER_PROJECT]
        return ordered[:MONTHLY_REPORT_MAX_PROJECTS]

    def _prd_scope_summaries(
        self,
        *,
        prd_sources: list[dict[str, str]],
        generated_at: datetime,
        report_period: MonthlyReportPeriod,
        progress_callback: Any | None,
    ) -> list[dict[str, str]]:
        total = len(prd_sources)
        if not total:
            return []

        def summarize_source(index: int, source: dict[str, str]) -> tuple[int, dict[str, str]]:
            cache_path = _monthly_report_prd_scope_cache_path(self.settings, report_period=report_period, prd_source=source)
            cached = _read_monthly_report_json_cache(cache_path)
            if isinstance(cached, dict):
                return index, {
                    "jira_id": str(source.get("jira_id") or cached.get("jira_id") or ""),
                    "title": str(source.get("title") or cached.get("title") or ""),
                    "url": str(source.get("url") or cached.get("url") or ""),
                    "updated_at": str(source.get("updated_at") or cached.get("updated_at") or ""),
                    "scope_summary": str(cached.get("scope_summary") or "").strip()[:MONTHLY_REPORT_SUMMARY_MAX_CHARS],
                    "cache_hit": True,
                }
            _emit_monthly_report_progress(
                progress_callback,
                "summarizing_prd_scope",
                f"Summarizing PRD scope changes {index + 1}/{total}.",
                index + 1,
                total,
            )
            prompt = build_monthly_report_prd_scope_prompt(
                generated_at=generated_at,
                report_period=report_period,
                prd_source=source,
            )
            generated = self._guarded_generate(
                prompt=prompt,
                prompt_mode=f"{MONTHLY_REPORT_PROMPT_VERSION}_prd_scope_summary",
                max_tokens=MONTHLY_REPORT_BATCH_MAX_TOKENS,
                progress_callback=progress_callback,
            )
            item = {
                "jira_id": str(source.get("jira_id") or ""),
                "title": str(source.get("title") or ""),
                "url": str(source.get("url") or ""),
                "updated_at": str(source.get("updated_at") or ""),
                "scope_summary": str(generated.get("result_markdown") or "").strip()[:MONTHLY_REPORT_SUMMARY_MAX_CHARS],
                "cache_hit": False,
            }
            _write_monthly_report_json_cache(cache_path, item)
            return index, item

        ordered: list[dict[str, str] | None] = [None] * total
        workers = min(MONTHLY_REPORT_EVIDENCE_WORKERS, total)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(summarize_source, index, source)
                for index, source in enumerate(prd_sources)
            ]
            for future in as_completed(futures):
                index, item = future.result()
                ordered[index] = item
        return [item for item in ordered if item is not None]

    @staticmethod
    def _merge_project_fields(project: dict[str, Any], raw_project: dict[str, Any]) -> None:
        for key in ("project_name", "market", "priority", "regional_pm_pic", "status", "release_date", "key_project_source"):
            value = str(raw_project.get(key) or "").strip()
            if value and not project.get(key):
                project[key] = value

    def _prd_sources(self, key_projects: list[dict[str, Any]], *, project_ids: set[str] | None = None) -> tuple[list[dict[str, str]], list[str]]:
        if self.confluence is None:
            return [], []
        sources: list[dict[str, str]] = []
        errors: list[str] = []
        seen: set[str] = set()
        allowed_ids = {str(item or "").strip() for item in project_ids if str(item or "").strip()} if project_ids is not None else None
        for project in key_projects:
            project_id = str(project.get("bpmis_id") or "").strip()
            if allowed_ids is not None and project_id not in allowed_ids:
                continue
            for ticket in project.get("jira_tickets") or []:
                for link in ticket.get("prd_links") or []:
                    url = str((link or {}).get("url") or "").strip()
                    if not url or url in seen or len(sources) >= MONTHLY_REPORT_MAX_PRD_PAGES:
                        continue
                    seen.add(url)
                    try:
                        page = self.confluence.ingest_page(url, "monthly-report")
                        if not page.sections:
                            continue
                        source = _build_prd_source(page)[:MONTHLY_REPORT_MAX_PRD_CHARS_PER_PAGE]
                        sources.append(
                            {
                                "jira_id": str(ticket.get("jira_id") or ""),
                                "title": page.title,
                                "url": page.source_url,
                                "updated_at": page.updated_at,
                                "content": source,
                            }
                        )
                    except Exception as error:  # noqa: BLE001 - PRD enrichment should not block the report.
                        errors.append(f"{url}: {error}")
        return sources, errors[:8]


def normalize_monthly_report_template(value: Any) -> str:
    template = str(value or "").strip()
    return template or DEFAULT_MONTHLY_REPORT_TEMPLATE


def normalize_monthly_report_highlight_topics(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = re.split(r"[\n\r]+", value)
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []
    topics = _dedupe_preserve_order([str(item or "").strip() for item in raw_items if str(item or "").strip()])
    if len(topics) > MONTHLY_REPORT_MAX_HIGHLIGHT_TOPICS:
        raise ToolError(f"Monthly Report supports at most {MONTHLY_REPORT_MAX_HIGHLIGHT_TOPICS} highlight topics.")
    return topics


def normalize_monthly_report_highlight_topic_sources(value: Any, highlight_topics: list[str]) -> dict[str, list[str]]:
    default_sources = list(MONTHLY_REPORT_HIGHLIGHT_SOURCES)
    normalized: dict[str, list[str]] = {topic: list(default_sources) for topic in highlight_topics}
    if value in (None, "", []):
        return normalized

    entries: list[dict[str, Any]] = []
    if isinstance(value, dict):
        for topic, sources in value.items():
            entries.append({"topic": topic, "sources": sources})
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                entries.append(item)
    for entry in entries:
        topic = str(entry.get("topic") or "").strip()
        if topic not in normalized:
            continue
        sources = _normalize_monthly_report_highlight_source_list(entry.get("sources"))
        if not sources:
            raise ToolError(f"Monthly Report highlight topic '{topic}' must select at least one source.")
        normalized[topic] = sources
    return normalized


def _normalize_monthly_report_highlight_source_list(value: Any) -> list[str]:
    raw_items = value if isinstance(value, list) else [value]
    sources: list[str] = []
    aliases = {
        "seatalk": MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK,
        "sea_talk": MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK,
        "gmail": MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL,
        "email": MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL,
        "team_dashboard": MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD,
        "team-dashboard": MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD,
        "dashboard": MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD,
        "jira": MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD,
        "prd": MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD,
    }
    for item in raw_items:
        key = str(item or "").strip().casefold().replace(" ", "_")
        source = aliases.get(key)
        if source and source not in sources:
            sources.append(source)
    return sources


def build_monthly_report_historical_style_guide(sent_reports: list[dict[str, Any]] | None) -> dict[str, Any]:
    examples: list[dict[str, str]] = []
    subjects: list[str] = []
    for item in sent_reports or []:
        if not isinstance(item, dict):
            continue
        source_type = str(item.get("source_type") or "").strip()
        item_type = str(item.get("item_type") or "").strip()
        if source_type and source_type != "gmail_sent_monthly_report":
            continue
        if item_type and item_type != "curated_report":
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        subject = str(item.get("summary") or item.get("subject") or metadata.get("subject") or "").strip()
        content = str(item.get("content") or item.get("body") or "").strip()
        excerpt = _monthly_report_historical_excerpt(content)
        if not excerpt:
            continue
        if subject and subject in subjects:
            continue
        if subject:
            subjects.append(subject)
        examples.append({"subject": subject, "excerpt": excerpt})
        if len(examples) >= MONTHLY_REPORT_STYLE_GUIDE_MAX_REPORTS:
            break
    if not examples:
        return {"report_count": 0, "subject_pattern": monthly_report_subject_pattern(), "style_rules": [], "examples": []}
    return {
        "report_count": len(examples),
        "subject_pattern": monthly_report_subject_pattern(),
        "observed_subjects": subjects[:MONTHLY_REPORT_STYLE_GUIDE_MAX_REPORTS],
        "style_rules": [
            "Use the historical reports as writing-style references, not as factual evidence for the current period.",
            "Keep the email subject in the Banking Product Update format.",
            "Start the email body directly with Highlights; do not add a '0. Critical Updates' heading.",
            "Write highlights as concise manager-facing product updates, with business impact before implementation detail.",
            "Select Highlights only for material recent discussions, launch outcomes, delays, risks, decisions, management alignment, cross-team dependencies, or next actions worth updating Xiaodong's manager.",
            "Prefer direct progress, risk, decision, and next-action wording over investigation-log or tool-facing language.",
            "Use the pattern '[Product Area] [Market or Region] Project/Topic: impact/status/decision/next action' when the evidence supports it.",
            "Keep each Highlight to one compact paragraph unless timeline or impact needs two to three short bullets.",
            "For delays or resource constraints, state the affected phase, revised timeline, business impact, and owner/next step.",
            "For go-live or UAT updates, state the launch/UAT status, observed issue or feedback if any, and what will happen next.",
            "Keep Anti-Fraud, Credit Risk, and Ops Risk separated when the evidence supports separate product areas.",
            "Avoid exposing Jira IDs, source mechanics, raw chat phrasing, or internal evidence labels in the final email.",
        ],
        "examples": examples,
    }


def read_monthly_report_historical_style_guide_cache(settings: Settings, *, owner_email: str) -> dict[str, Any] | None:
    cached = _read_monthly_report_json_cache(_monthly_report_style_guide_cache_path(settings, owner_email=owner_email))
    if not isinstance(cached, dict):
        return None
    if cached.get("version") != MONTHLY_REPORT_STYLE_GUIDE_CACHE_VERSION:
        return None
    style_guide = cached.get("style_guide")
    if isinstance(style_guide, dict) and style_guide.get("report_count"):
        return style_guide
    return None


def write_monthly_report_historical_style_guide_cache(settings: Settings, *, owner_email: str, style_guide: dict[str, Any]) -> None:
    if not isinstance(style_guide, dict) or not style_guide.get("report_count"):
        return
    _write_monthly_report_json_cache(
        _monthly_report_style_guide_cache_path(settings, owner_email=owner_email),
        {
            "version": MONTHLY_REPORT_STYLE_GUIDE_CACHE_VERSION,
            "owner_email": str(owner_email or "").strip().lower(),
            "cached_at": datetime.now(SEATALK_INSIGHTS_TIMEZONE).isoformat(),
            "style_guide": _compact_monthly_report_style_guide(style_guide),
        },
    )


def monthly_report_subject_pattern() -> str:
    return "[Banking] Product Update (dd mmm - dd mmm) - Anti-Fraud, Credit Risk & Ops Risk"


def _monthly_report_historical_excerpt(content: str) -> str:
    text = str(content or "").strip()
    if not text:
        return ""
    text = _strip_jira_issue_keys_for_report(text)
    text = re.sub(r"https?://\S+", "[link]", text)
    text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[email]", text)
    lines = [line.rstrip() for line in text.splitlines()]
    selected: list[str] = []
    capturing = False
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if selected and selected[-1]:
                selected.append("")
            continue
        lower = line.casefold()
        if any(marker in lower for marker in ("highlight", "anti-fraud", "anti fraud", "credit risk", "ops risk", "operational risk", "key update", "product update")):
            capturing = True
        if capturing:
            selected.append(raw_line)
        if len("\n".join(selected)) >= MONTHLY_REPORT_STYLE_GUIDE_MAX_EXCERPT_CHARS:
            break
    excerpt = "\n".join(selected).strip() or "\n".join(lines[:40]).strip()
    return excerpt[:MONTHLY_REPORT_STYLE_GUIDE_MAX_EXCERPT_CHARS].rstrip()


def _compact_monthly_report_style_guide(style_guide: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(style_guide, dict) or not style_guide.get("report_count"):
        return {"report_count": 0, "subject_pattern": monthly_report_subject_pattern(), "style_rules": [], "examples": []}
    compacted: dict[str, Any] = {
        "report_count": int(style_guide.get("report_count") or 0),
        "subject_pattern": str(style_guide.get("subject_pattern") or monthly_report_subject_pattern()).strip(),
        "observed_subjects": [
            str(item or "").strip()
            for item in (style_guide.get("observed_subjects") or [])
            if str(item or "").strip()
        ][:MONTHLY_REPORT_STYLE_GUIDE_MAX_REPORTS],
        "style_rules": [
            str(item or "").strip()
            for item in (style_guide.get("style_rules") or [])
            if str(item or "").strip()
        ][:12],
        "examples": [],
    }
    for item in style_guide.get("examples") or []:
        if not isinstance(item, dict):
            continue
        excerpt = str(item.get("excerpt") or "").strip()
        if not excerpt:
            continue
        compacted["examples"].append(
            {
                "subject": str(item.get("subject") or "").strip(),
                "excerpt": excerpt[:MONTHLY_REPORT_STYLE_GUIDE_MAX_EXCERPT_CHARS].rstrip(),
            }
        )
        if len(compacted["examples"]) >= MONTHLY_REPORT_STYLE_GUIDE_MAX_REPORTS:
            break
    serialized = json.dumps(compacted, ensure_ascii=False)
    if len(serialized) <= MONTHLY_REPORT_STYLE_GUIDE_MAX_CHARS:
        return compacted
    compacted["examples"] = compacted["examples"][:3]
    for example in compacted["examples"]:
        example["excerpt"] = str(example.get("excerpt") or "")[:1_200].rstrip()
    return compacted


def _monthly_report_topic_uses_source(topic: str, source_map: dict[str, list[str]], source: str) -> bool:
    return source in (source_map.get(str(topic or "").strip()) or list(MONTHLY_REPORT_HIGHLIGHT_SOURCES))


def match_monthly_report_highlight_topics(highlight_topics: list[str], key_projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for topic in highlight_topics:
        query_plan = build_monthly_report_query_plan(topic)
        topic_aliases = set(query_plan.get("aliases") or []) or _highlight_topic_aliases(topic)
        product_area_scope = str(query_plan.get("product_area_scope") or "").strip()
        qualifier_marker_groups = [
            tuple(str(marker or "").strip() for marker in group if str(marker or "").strip())
            for group in (query_plan.get("qualifier_marker_groups") or [])
            if isinstance(group, list)
        ]
        matched_projects: list[dict[str, Any]] = []
        for project in key_projects:
            if product_area_scope and _project_product_area(project) != product_area_scope:
                continue
            project_text = _monthly_report_project_match_text(project)
            if _highlight_topic_matches_project(topic_aliases, project):
                matched_projects.append(project)
        qualified_projects = [
            project
            for project in matched_projects
            if _monthly_report_text_matches_qualifier_marker_groups(
                _monthly_report_project_match_text(project),
                qualifier_marker_groups,
            )
        ]
        if qualifier_marker_groups and qualified_projects:
            matched_projects = qualified_projects
        project_ids: list[str] = []
        project_names: list[str] = []
        for project in matched_projects:
            project_id = str(project.get("bpmis_id") or "").strip()
            if project_id and project_id not in project_ids:
                project_ids.append(project_id)
                project_names.append(str(project.get("project_name") or "").strip())
        matches.append(
            {
                "topic": topic,
                "query_plan": query_plan,
                "product_area_scope": product_area_scope,
                "qualifier_marker_groups": [list(group) for group in qualifier_marker_groups],
                "project_ids": project_ids,
                "project_names": [name for name in project_names if name],
            }
        )
    return matches


def build_monthly_highlight_deep_evidence(
    *,
    highlight_topics: list[str],
    key_projects: list[dict[str, Any]],
    topic_project_matches: list[dict[str, Any]],
    seatalk_history_text: str,
    topic_gmail_evidence: list[dict[str, Any]],
    prd_scope_summaries: list[dict[str, Any]],
    report_period: MonthlyReportPeriod,
    monthly_requirements_targets: list[dict[str, Any]] | None = None,
    highlight_topic_sources: dict[str, list[str]] | None = None,
    fallback_reference_date: date | None = None,
) -> list[dict[str, Any]]:
    projects_by_id = {str(project.get("bpmis_id") or "").strip(): project for project in key_projects if str(project.get("bpmis_id") or "").strip()}
    prd_by_jira = _index_prd_summaries_by_jira(prd_scope_summaries)
    gmail_by_topic = {str(item.get("topic") or "").strip(): item for item in topic_gmail_evidence if isinstance(item, dict)}
    match_by_topic = {str(item.get("topic") or "").strip(): item for item in topic_project_matches if isinstance(item, dict)}
    evidence: list[dict[str, Any]] = []
    for topic in highlight_topics:
        selected_sources = (highlight_topic_sources or {}).get(topic) or list(MONTHLY_REPORT_HIGHLIGHT_SOURCES)
        use_seatalk = MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK in selected_sources
        use_gmail = MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL in selected_sources
        use_team_dashboard = MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD in selected_sources
        match = match_by_topic.get(topic) or {"topic": topic, "project_ids": []}
        query_plan = match.get("query_plan") if isinstance(match.get("query_plan"), dict) else build_monthly_report_query_plan(topic, selected_sources=selected_sources)
        product_area_scope = str(match.get("product_area_scope") or query_plan.get("product_area_scope") or _monthly_report_highlight_product_area_scope(topic) or "").strip()
        qualifier_marker_groups = [
            tuple(str(marker or "").strip() for marker in group if str(marker or "").strip())
            for group in (
                match.get("qualifier_marker_groups")
                or match.get("required_marker_groups")
                or query_plan.get("qualifier_marker_groups")
                or _monthly_report_highlight_qualifier_marker_groups(topic)
                or []
            )
            if isinstance(group, (list, tuple)) and any(str(marker or "").strip() for marker in group)
        ]
        project_ids = [str(project_id or "").strip() for project_id in (match.get("project_ids") or []) if str(project_id or "").strip()]
        projects = [projects_by_id[project_id] for project_id in project_ids if project_id in projects_by_id] if use_team_dashboard else []
        topic_intent = str(query_plan.get("intent") or _monthly_report_highlight_topic_intent(topic))
        intent_focus = _monthly_report_highlight_intent_focus(topic_intent)
        aliases = set(query_plan.get("aliases") or []) or _highlight_topic_aliases(topic)
        for project in projects:
            aliases.update(_project_aliases(project))
        seatalk_match_limit = MONTHLY_REPORT_HIGHLIGHT_EVIDENCE_MAX_LINES * (80 if qualifier_marker_groups else 1)
        matched_seatalk = [] if not use_seatalk else (
            _matched_context_lines_for_project(
                seatalk_history_text,
                _monthly_report_issue_followup_aliases(topic, aliases),
                limit=seatalk_match_limit,
                context_lines=6,
            )
            if topic_intent == "issue_followup"
            else (
                _matched_conversation_context_lines_for_project(
                    seatalk_history_text,
                    aliases,
                    limit=seatalk_match_limit,
                )
            )
        )
        raw_matched_seatalk_count = len(matched_seatalk)
        project_updates: list[dict[str, Any]] = []
        prd_facts: list[str] = []
        for project in projects:
            jira_tickets = [ticket for ticket in (project.get("jira_tickets") or []) if isinstance(ticket, dict)]
            status_facts, timeline_facts, _jira_sources, jira_score = _jira_evidence_facts(jira_tickets)
            matched_prd = _matched_prd_summaries_for_project(jira_tickets, prd_by_jira)
            project_prd_facts = [
                str(item.get("scope_summary") or "").strip()[:1_200]
                for item in matched_prd
                if str(item.get("scope_summary") or "").strip()
            ]
            prd_facts.extend(project_prd_facts)
            target_tech_live_date, target_tech_live_version, target_tech_live_source, target_tech_live_source_detail = _monthly_report_target_tech_live_date(
                jira_tickets,
                project=project,
                monthly_requirements_targets=monthly_requirements_targets or [],
                fallback_reference_date=fallback_reference_date or report_period.end.date(),
            )
            project_updates.append(
                {
                    "bpmis_id": str(project.get("bpmis_id") or "").strip(),
                    "project_name": str(project.get("project_name") or "").strip(),
                    "market": str(project.get("market") or "").strip(),
                    "priority": str(project.get("priority") or "").strip(),
                    "target_tech_live_date": target_tech_live_date,
                    "target_tech_live_version": target_tech_live_version,
                    "target_tech_live_source": target_tech_live_source,
                    "target_tech_live_source_detail": target_tech_live_source_detail,
                    "current_status": _monthly_report_current_status(
                        jira_tickets,
                        report_period=report_period,
                        material_update_score=jira_score,
                    ),
                    "status_facts": _compact_report_text_list(status_facts, limit=6, max_chars=320),
                    "timeline_facts": _compact_report_text_list(timeline_facts, limit=5, max_chars=240),
                    "prd_scope_summaries": _compact_report_text_list(project_prd_facts, limit=4, max_chars=700),
                }
            )
        gmail_item = gmail_by_topic.get(topic) or {}
        gmail_text = str(gmail_item.get("text") or "").strip() if use_gmail else ""
        matched_seatalk = _filter_monthly_report_texts_by_product_area_scope(matched_seatalk, product_area_scope)
        product_qualifier_marker_groups = _monthly_report_product_qualifier_marker_groups(qualifier_marker_groups)
        if product_qualifier_marker_groups:
            matched_seatalk = (
                _matched_qualified_conversation_context_lines_for_project(
                    seatalk_history_text,
                    aliases,
                    qualifier_marker_groups=qualifier_marker_groups,
                    topic_intent=topic_intent,
                    topic=topic,
                    limit=seatalk_match_limit,
                    context_lines=2,
                )
                or _filter_monthly_report_texts_by_qualifier_marker_groups(matched_seatalk, qualifier_marker_groups)
            )
        elif not qualifier_marker_groups:
            matched_seatalk = _prefer_monthly_report_texts_by_qualifier_marker_groups(matched_seatalk, qualifier_marker_groups)
        compact_seatalk = _compact_report_text_list(matched_seatalk, limit=16, max_chars=600)
        compact_gmail = _compact_report_text_list(_matched_sections_for_project(gmail_text, aliases, limit=8), limit=8, max_chars=900)
        compact_sheets = _compact_google_sheet_evidence(gmail_item.get("google_sheet_evidence")) if use_gmail else []
        compact_prd = _compact_report_text_list(prd_facts, limit=6, max_chars=700)
        compact_gmail = _filter_monthly_report_texts_by_product_area_scope(compact_gmail, product_area_scope)
        compact_sheets = [
            sheet
            for sheet in compact_sheets
            if _monthly_report_text_allowed_by_product_area_scope(str(sheet.get("text") or ""), product_area_scope)
        ]
        compact_prd = _filter_monthly_report_texts_by_product_area_scope(compact_prd, product_area_scope)
        compact_gmail = _prefer_monthly_report_texts_by_qualifier_marker_groups(compact_gmail, qualifier_marker_groups)
        compact_sheets = _prefer_monthly_report_sheet_evidence_by_qualifier_marker_groups(compact_sheets, qualifier_marker_groups)
        compact_prd = _prefer_monthly_report_texts_by_qualifier_marker_groups(compact_prd, qualifier_marker_groups)
        intent_signal_count = _monthly_report_intent_signal_count(
            topic_intent,
            [
                *compact_seatalk,
                *compact_gmail,
                *[str(sheet.get("text") or "") for sheet in compact_sheets],
                *compact_prd,
                *[str(fact) for project in project_updates for fact in (project.get("status_facts") or [])],
                *[str(fact) for project in project_updates for fact in (project.get("timeline_facts") or [])],
            ],
        )
        issue_followup_facts = (
            _monthly_report_issue_followup_facts(
                [
                    *compact_seatalk,
                    *compact_gmail,
                    *[str(sheet.get("text") or "") for sheet in compact_sheets],
                ]
            )
            if topic_intent == "issue_followup"
            else {}
        )
        evidence_map = _monthly_highlight_topic_evidence_map(
            topic=topic,
            topic_type="project_update" if projects else "general_topic",
            topic_intent=topic_intent,
            product_area_scope=product_area_scope,
            project_updates=project_updates,
            seatalk_evidence=compact_seatalk,
            gmail_evidence=compact_gmail,
            google_sheet_evidence=compact_sheets,
            prd_scope_summaries=compact_prd,
            gmail_error=str(gmail_item.get("error") or "").strip(),
            intent_signal_count=intent_signal_count,
            issue_followup_facts=issue_followup_facts,
            selected_sources=selected_sources,
        )
        evidence_debug = _monthly_report_topic_evidence_debug(
            topic=topic,
            query_plan=query_plan,
            selected_sources=selected_sources,
            topic_intent=topic_intent,
            product_area_scope=product_area_scope,
            aliases=aliases,
            qualifier_marker_groups=qualifier_marker_groups,
            raw_matched_seatalk_count=raw_matched_seatalk_count,
            matched_seatalk_count=len(matched_seatalk),
            seatalk_evidence=compact_seatalk,
            gmail_evidence=compact_gmail,
            google_sheet_evidence=compact_sheets,
            project_updates=project_updates,
            prd_scope_summaries=compact_prd,
            evidence_map=evidence_map,
        )
        evidence.append(
            {
                "topic": topic,
                "selected_sources": selected_sources,
                "query_plan": query_plan,
                "topic_intent": topic_intent,
                "intent_focus": intent_focus,
                "product_area_scope": product_area_scope,
                "topic_type": "project_update" if projects else "general_topic",
                "matched_project_ids": project_ids if use_team_dashboard else [],
                "matched_project_names": [str(project.get("project_name") or "").strip() for project in projects if str(project.get("project_name") or "").strip()],
                "project_updates": project_updates,
                "seatalk_evidence": compact_seatalk,
                "gmail_evidence": compact_gmail,
                "google_sheet_evidence": compact_sheets,
                "gmail_error": str(gmail_item.get("error") or "").strip(),
                "prd_scope_summaries": compact_prd,
                "issue_followup_facts": issue_followup_facts,
                "evidence_map": evidence_map,
                "evidence_debug": evidence_debug,
                "confidence": evidence_map["confidence"],
                "recommended_tone": evidence_map["recommended_tone"],
            }
        )
    return evidence


def build_monthly_highlight_evidence_map(highlight_deep_evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    maps: list[dict[str, Any]] = []
    for item in highlight_deep_evidence:
        if not isinstance(item, dict):
            continue
        evidence_map = item.get("evidence_map")
        if isinstance(evidence_map, dict):
            maps.append(evidence_map)
            continue
        maps.append(
            _monthly_highlight_topic_evidence_map(
                topic=str(item.get("topic") or "").strip(),
                topic_type=str(item.get("topic_type") or "").strip(),
                topic_intent=str(item.get("topic_intent") or "").strip(),
                product_area_scope=str(item.get("product_area_scope") or "").strip(),
                project_updates=[project for project in (item.get("project_updates") or []) if isinstance(project, dict)],
                seatalk_evidence=[str(value) for value in (item.get("seatalk_evidence") or []) if str(value).strip()],
                gmail_evidence=[str(value) for value in (item.get("gmail_evidence") or []) if str(value).strip()],
                google_sheet_evidence=[sheet for sheet in (item.get("google_sheet_evidence") or []) if isinstance(sheet, dict)],
                prd_scope_summaries=[str(value) for value in (item.get("prd_scope_summaries") or []) if str(value).strip()],
                gmail_error=str(item.get("gmail_error") or "").strip(),
                intent_signal_count=_safe_int(item.get("intent_signal_count")),
                issue_followup_facts=item.get("issue_followup_facts") if isinstance(item.get("issue_followup_facts"), dict) else {},
                selected_sources=[str(source) for source in (item.get("selected_sources") or []) if str(source).strip()],
            )
        )
    return maps


def build_monthly_report_evidence_review(highlight_deep_evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    review: list[dict[str, Any]] = []
    for item in highlight_deep_evidence:
        if not isinstance(item, dict):
            continue
        evidence_map = item.get("evidence_map") if isinstance(item.get("evidence_map"), dict) else {}
        query_plan = item.get("query_plan") if isinstance(item.get("query_plan"), dict) else {}
        debug = item.get("evidence_debug") if isinstance(item.get("evidence_debug"), dict) else {}
        confidence = str(evidence_map.get("confidence") or item.get("confidence") or "none").strip().lower()
        gaps = [str(gap) for gap in (evidence_map.get("gaps") or []) if str(gap).strip()]
        active_sources = [str(source) for source in (evidence_map.get("active_sources") or []) if str(source).strip()]
        status = "ready"
        if confidence in {"none", "low"} or gaps:
            status = "needs_review"
        if confidence == "none":
            status = "blocked"
        review.append(
            {
                "topic": str(item.get("topic") or "").strip(),
                "status": status,
                "confidence": confidence,
                "intent": str(query_plan.get("intent") or item.get("topic_intent") or "").strip(),
                "primary_topic": str(query_plan.get("primary_topic") or item.get("topic") or "").strip(),
                "product_area_scope": str(query_plan.get("product_area_scope") or item.get("product_area_scope") or "").strip(),
                "selected_sources": [str(source) for source in (item.get("selected_sources") or []) if str(source).strip()],
                "active_sources": active_sources,
                "source_counts": evidence_map.get("source_counts") or {},
                "gaps": gaps,
                "seatalk_conversation_labels": debug.get("seatalk_conversation_labels") or [],
                "glossary_matches": query_plan.get("glossary_matches") or [],
                "qualifiers": query_plan.get("qualifiers") or {},
                "forbidden_meanings": query_plan.get("forbidden_meanings") or [],
                "source_policy": query_plan.get("source_policy") or {},
            }
        )
    return review


def _monthly_report_topic_evidence_debug(
    *,
    topic: str,
    query_plan: dict[str, Any],
    selected_sources: list[str],
    topic_intent: str,
    product_area_scope: str,
    aliases: set[str],
    qualifier_marker_groups: list[tuple[str, ...]],
    raw_matched_seatalk_count: int,
    matched_seatalk_count: int,
    seatalk_evidence: list[str],
    gmail_evidence: list[str],
    google_sheet_evidence: list[dict[str, Any]],
    project_updates: list[dict[str, Any]],
    prd_scope_summaries: list[str],
    evidence_map: dict[str, Any],
) -> dict[str, Any]:
    glossary_entries = _monthly_report_glossary_entries_for_topic(topic)
    conversation_labels = []
    for item in seatalk_evidence:
        clean = str(item or "").strip()
        if clean.startswith("==="):
            conversation_labels.append(clean.strip("= ").strip())
    return {
        "topic": topic,
        "query_plan": query_plan,
        "selected_sources": selected_sources,
        "topic_intent": topic_intent,
        "product_area_scope": product_area_scope,
        "confidence": evidence_map.get("confidence"),
        "confidence_score": evidence_map.get("confidence_score"),
        "source_counts": evidence_map.get("source_counts") or {},
        "active_sources": evidence_map.get("active_sources") or [],
        "gaps": evidence_map.get("gaps") or [],
        "alias_sample": sorted(alias for alias in aliases if alias)[:24],
        "qualifier_marker_groups": [list(group) for group in qualifier_marker_groups],
        "glossary_matches": [
            {
                "id": str(entry.get("id") or ""),
                "domain": str(entry.get("domain") or ""),
                "canonical": str(entry.get("canonical") or ""),
            }
            for entry in glossary_entries
        ],
        "seatalk_raw_match_count": max(0, int(raw_matched_seatalk_count or 0)),
        "seatalk_filtered_match_count": max(0, int(matched_seatalk_count or 0)),
        "seatalk_compact_count": len(seatalk_evidence),
        "seatalk_conversation_labels": _dedupe_preserve_order(conversation_labels)[:8],
        "gmail_evidence_count": len(gmail_evidence),
        "google_sheet_count": len(google_sheet_evidence),
        "team_dashboard_project_count": len(project_updates),
        "prd_scope_count": len(prd_scope_summaries),
    }


def _monthly_highlight_topic_evidence_map(
    *,
    topic: str,
    topic_type: str,
    topic_intent: str,
    project_updates: list[dict[str, Any]],
    seatalk_evidence: list[str],
    gmail_evidence: list[str],
    google_sheet_evidence: list[dict[str, Any]],
    prd_scope_summaries: list[str],
    product_area_scope: str = "",
    gmail_error: str = "",
    intent_signal_count: int = 0,
    issue_followup_facts: dict[str, list[str]] | None = None,
    selected_sources: list[str] | None = None,
) -> dict[str, Any]:
    source_selection = selected_sources or list(MONTHLY_REPORT_HIGHLIGHT_SOURCES)
    project_count = len(project_updates)
    source_counts = {
        "project": project_count,
        "seatalk": len(seatalk_evidence),
        "gmail": len(gmail_evidence),
        "google_sheet": len(google_sheet_evidence),
        "prd": len(prd_scope_summaries),
    }
    active_sources = [source for source, count in source_counts.items() if count > 0]
    score = 0
    score += min(3, source_counts["project"]) * 2
    score += min(5, source_counts["seatalk"]) * 2
    score += min(4, source_counts["gmail"]) * 2
    score += min(3, source_counts["google_sheet"]) * 2
    score += min(3, source_counts["prd"])
    project_statuses = [
        str(project.get("current_status") or "").strip()
        for project in project_updates
        if str(project.get("current_status") or "").strip()
    ]
    concrete_project_progress = any(status in {"Dev", "UAT"} for status in project_statuses)
    if concrete_project_progress:
        score += 2
    intent = topic_intent or _monthly_report_highlight_topic_intent(topic)
    if intent != "general_progress" and intent_signal_count <= 0:
        score = min(score, 4)
    selected_source_set = set(source_selection)
    has_concrete_single_source_evidence = (
        len(active_sources) == 1
        and (score >= 8 or (intent == "go_live_outcome" and source_counts["seatalk"] >= 1))
        and intent_signal_count >= 1
        and (
            selected_source_set == set(active_sources)
            or selected_source_set == {MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK}
        )
    )
    if has_concrete_single_source_evidence or (len(active_sources) >= 3 and score >= 8):
        confidence = "high"
    elif len(active_sources) >= 2 and score >= 5:
        confidence = "medium"
    elif score > 0:
        confidence = "low"
    else:
        confidence = "none"
    gaps: list[str] = []
    if MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK in source_selection and not seatalk_evidence:
        gaps.append("seatalk")
    if MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL in source_selection and not gmail_evidence and not google_sheet_evidence:
        gaps.append("email_or_sheet")
    if MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD in source_selection and topic_type == "project_update" and not prd_scope_summaries:
        gaps.append("prd")
    if MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL in source_selection and gmail_error:
        gaps.append("gmail_error")
    if intent != "general_progress" and intent_signal_count <= 0:
        gaps.append(f"{intent}_evidence")
    tone_by_confidence = {
        "high": "Write as a confident executive progress update with clear business impact and next movement.",
        "medium": "Write as a cautious progress update; mention pending confirmation only if it affects management attention.",
        "low": "Write as a monitoring item, avoid over-claiming, and avoid raw phrases like no confirmed evidence.",
        "none": "Do not position as material progress; state that the item remains pending confirmation in manager-ready wording.",
    }
    intent_focus = _monthly_report_highlight_intent_focus(intent)
    recommended_tone = tone_by_confidence[confidence]
    if intent_focus.get("tone_suffix"):
        recommended_tone = f"{recommended_tone} {intent_focus['tone_suffix']}"
    return {
        "topic": topic,
        "selected_sources": source_selection,
        "topic_type": topic_type,
        "topic_intent": intent,
        "product_area_scope": product_area_scope,
        "intent_signal_count": intent_signal_count,
        "intent_focus": intent_focus,
        "confidence": confidence,
        "confidence_score": score,
        "source_counts": source_counts,
        "active_sources": active_sources,
        "matched_project_names": [
            str(project.get("project_name") or "").strip()
            for project in project_updates
            if str(project.get("project_name") or "").strip()
        ],
        "project_statuses": _dedupe_preserve_order(project_statuses),
        "issue_followup_facts": issue_followup_facts or {},
        "gaps": gaps,
        "recommended_tone": recommended_tone,
    }


def _monthly_report_confidence_counts(highlight_evidence_map: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for item in highlight_evidence_map:
        confidence = str(item.get("confidence") or "none").strip().lower()
        if confidence not in counts:
            confidence = "none"
        counts[confidence] += 1
    return counts


def _monthly_report_target_source_counts(monthly_evidence_brief: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in monthly_evidence_brief:
        if not isinstance(item, dict) or not item.get("include"):
            continue
        source = str(item.get("target_tech_live_source") or "unknown").strip() or "unknown"
        counts[source] = counts.get(source, 0) + 1
    return counts


def build_monthly_report_generation_diagnostics(
    *,
    highlight_evidence_map: list[dict[str, Any]],
    monthly_evidence_brief: list[dict[str, Any]],
    batch_summaries: list[dict[str, Any]],
    highlight_narratives: list[dict[str, Any]],
    timings: dict[str, float],
) -> dict[str, Any]:
    low_confidence = [
        str(item.get("topic") or "").strip()
        for item in highlight_evidence_map
        if str(item.get("confidence") or "").strip().lower() in {"low", "none"}
    ]
    target_sources = [
        {
            "project_name": str(item.get("project_name") or "").strip(),
            "priority": str(item.get("priority") or "").strip(),
            "target_tech_live_date": str(item.get("target_tech_live_date") or "").strip(),
            "target_tech_live_source": str(item.get("target_tech_live_source") or "").strip(),
            "target_tech_live_source_detail": item.get("target_tech_live_source_detail") if isinstance(item.get("target_tech_live_source_detail"), dict) else {},
        }
        for item in monthly_evidence_brief
        if isinstance(item, dict) and item.get("include")
    ]
    return {
        "highlight_confidence_counts": _monthly_report_confidence_counts(highlight_evidence_map),
        "low_or_none_confidence_topics": [topic for topic in low_confidence if topic],
        "highlight_source_counts": [
            {
                "topic": str(item.get("topic") or "").strip(),
                "topic_intent": str(item.get("topic_intent") or "").strip(),
                "product_area_scope": str(item.get("product_area_scope") or "").strip(),
                "confidence": str(item.get("confidence") or "").strip(),
                "source_counts": item.get("source_counts") if isinstance(item.get("source_counts"), dict) else {},
                "intent_signal_count": _safe_int(item.get("intent_signal_count")),
                "gaps": item.get("gaps") if isinstance(item.get("gaps"), list) else [],
            }
            for item in highlight_evidence_map
            if isinstance(item, dict)
        ],
        "target_tech_live_source_counts": _monthly_report_target_source_counts(monthly_evidence_brief),
        "target_tech_live_sources": target_sources,
        "batch_summary_cache_hit_count": len([item for item in batch_summaries if item.get("cache_hit")]),
        "highlight_narrative_cache_hit_count": len([item for item in highlight_narratives if item.get("cache_hit")]),
        "timings": dict(timings),
    }


def build_monthly_project_evidence_brief(
    *,
    key_projects: list[dict[str, Any]],
    seatalk_history_text: str,
    vip_gmail_text: str,
    prd_scope_summaries: list[dict[str, Any]],
    report_period: MonthlyReportPeriod,
    monthly_requirements_targets: list[dict[str, Any]] | None = None,
    highlight_project_ids: set[str] | None = None,
    fallback_reference_date: date | None = None,
) -> list[dict[str, Any]]:
    prd_by_jira = _index_prd_summaries_by_jira(prd_scope_summaries)
    deep_project_ids = {str(item or "").strip() for item in highlight_project_ids if str(item or "").strip()} if highlight_project_ids is not None else None
    items: list[dict[str, Any]] = []
    for project in key_projects:
        project_id = str(project.get("bpmis_id") or "").strip()
        is_highlight_project = deep_project_ids is None or project_id in deep_project_ids
        aliases = _project_aliases(project)
        jira_tickets = [ticket for ticket in (project.get("jira_tickets") or []) if isinstance(ticket, dict)]
        matched_seatalk = (
            _matched_lines_for_project(seatalk_history_text, aliases, limit=MONTHLY_REPORT_PROJECT_EVIDENCE_MAX_LINES)
            if is_highlight_project
            else []
        )
        matched_gmail = (
            _matched_sections_for_project(vip_gmail_text, aliases, limit=MONTHLY_REPORT_PROJECT_EVIDENCE_MAX_GMAIL)
            if is_highlight_project
            else []
        )
        matched_prd = _matched_prd_summaries_for_project(jira_tickets, prd_by_jira) if is_highlight_project else []
        status_facts, timeline_facts, jira_sources, jira_score = _jira_evidence_facts(jira_tickets)
        prd_facts = [
            str(item.get("scope_summary") or "").strip()[:1_000]
            for item in matched_prd
            if str(item.get("scope_summary") or "").strip()
        ]
        status_facts.extend(_message_status_facts(matched_seatalk + matched_gmail))
        risks = _direct_project_risks(matched_seatalk + matched_gmail)
        decisions_needed = _direct_project_decisions(matched_seatalk + matched_gmail)
        score = jira_score + len(matched_seatalk) * 3 + len(matched_gmail) * 3 + len(matched_prd) * 2
        current_status = _monthly_report_current_status(jira_tickets, report_period=report_period, material_update_score=jira_score)
        target_tech_live_date, target_tech_live_version, target_tech_live_source, target_tech_live_source_detail = _monthly_report_target_tech_live_date(
            jira_tickets,
            project=project,
            monthly_requirements_targets=monthly_requirements_targets or [],
            fallback_reference_date=fallback_reference_date or report_period.end.date(),
        )
        include = True
        evidence_sources = {
            "jira": jira_sources,
            "seatalk": matched_seatalk,
            "vip_gmail": matched_gmail,
            "prd_scope_summary": prd_facts,
            "monthly_requirements": _monthly_requirements_entries_for_project(project, monthly_requirements_targets or [], limit=3),
        }
        items.append(
            {
                "include": include,
                "exclude_reason": "",
                "product_area": _project_product_area(project),
                "project_id": str(project.get("bpmis_id") or "").strip(),
                "bpmis_id": str(project.get("bpmis_id") or "").strip(),
                "project_name": str(project.get("project_name") or "").strip(),
                "teams": _compact_text_list(project.get("teams"), limit=5, max_chars=80),
                "market": str(project.get("market") or "").strip(),
                "priority": str(project.get("priority") or "").strip(),
                "aliases": sorted(aliases)[:40],
                "jira_ids": [str(ticket.get("jira_id") or "").strip() for ticket in jira_tickets if str(ticket.get("jira_id") or "").strip()],
                "seatalk_group_ids": _matched_seatalk_group_ids(matched_seatalk),
                "matched_seatalk_messages": matched_seatalk,
                "matched_vip_gmail_threads": matched_gmail,
                "matched_prd_summaries": prd_facts,
                "material_update_score": score,
                "current_status": current_status,
                "target_tech_live_date": target_tech_live_date,
                "target_tech_live_version": target_tech_live_version,
                "target_tech_live_source": target_tech_live_source,
                "target_tech_live_source_detail": target_tech_live_source_detail,
                "status_facts": _dedupe_preserve_order(status_facts)[:10],
                "timeline_facts": _dedupe_preserve_order(timeline_facts)[:8],
                "risks": risks[:6],
                "decisions_needed": [
                    item for item in decisions_needed[:6] if not _is_excluded_ask(item)
                ],
                "evidence_sources": evidence_sources,
            }
        )
    return items


def resolve_monthly_report_period(moment: datetime | None = None) -> MonthlyReportPeriod:
    local_moment = (moment or datetime.now(SEATALK_INSIGHTS_TIMEZONE)).astimezone(SEATALK_INSIGHTS_TIMEZONE)
    local_date = local_moment.date()
    if local_date < MONTHLY_REPORT_PERIOD_ANCHOR_START:
        period_index = 0
    else:
        period_index = (local_date - MONTHLY_REPORT_PERIOD_ANCHOR_START).days // MONTHLY_REPORT_PERIOD_DAYS
    start_date = MONTHLY_REPORT_PERIOD_ANCHOR_START + timedelta(days=period_index * MONTHLY_REPORT_PERIOD_DAYS)
    scheduled_end_date = MONTHLY_REPORT_PERIOD_ANCHOR_END + timedelta(days=period_index * MONTHLY_REPORT_PERIOD_DAYS)
    end_date = min(scheduled_end_date, max(local_date, start_date))
    start = datetime.combine(start_date, datetime_time.min, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
    end = datetime.combine(end_date, datetime_time.min, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
    end_exclusive = end + timedelta(days=1)
    scheduled_end = datetime.combine(scheduled_end_date, datetime_time.min, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
    return MonthlyReportPeriod(
        start=start,
        end=end,
        end_exclusive=end_exclusive,
        scheduled_start=start,
        scheduled_end=scheduled_end,
        scheduled_end_exclusive=scheduled_end + timedelta(days=1),
    )


def resolve_monthly_report_period_from_user_range(
    *,
    period_start: str | None,
    period_end: str | None,
    fallback: datetime | None = None,
) -> MonthlyReportPeriod:
    start_text = str(period_start or "").strip()
    end_text = str(period_end or "").strip()
    if not start_text and not end_text:
        return resolve_monthly_report_period(fallback)
    if not start_text or not end_text:
        raise ToolError("Monthly Report start date and end date are both required.")
    try:
        start = _parse_monthly_report_datetime(start_text).astimezone(SEATALK_INSIGHTS_TIMEZONE)
        end_date = date.fromisoformat(end_text[:10])
    except (TypeError, ValueError) as error:
        raise ToolError("Monthly Report date range must use YYYY-MM-DD dates.") from error
    end = datetime.combine(end_date, datetime_time.min, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
    end_exclusive = end + timedelta(days=1)
    if start.date() > end.date():
        raise ToolError("Monthly Report start date cannot be later than end date.")
    return MonthlyReportPeriod(
        start=start,
        end=end,
        end_exclusive=end_exclusive,
        scheduled_start=start,
        scheduled_end=end,
        scheduled_end_exclusive=end_exclusive,
    )


def _monthly_report_period_from_payload(
    *,
    period_start: str | None,
    period_end: str | None,
    period_end_exclusive: str | None,
    fallback: datetime,
) -> MonthlyReportPeriod:
    if period_start or period_end or period_end_exclusive:
        if not (period_start and period_end):
            raise ToolError("Monthly Report start date and end date are both required.")
        if period_start and period_end and not period_end_exclusive:
            return resolve_monthly_report_period_from_user_range(period_start=period_start, period_end=period_end, fallback=fallback)
        try:
            if period_start and period_end and period_end_exclusive:
                start = _parse_monthly_report_datetime(period_start)
                end_exclusive = _parse_monthly_report_datetime(period_end_exclusive)
                end_date = date.fromisoformat(str(period_end)[:10])
                end = datetime.combine(end_date, datetime_time.min, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
                if start < end_exclusive:
                    return MonthlyReportPeriod(
                        start=start.astimezone(SEATALK_INSIGHTS_TIMEZONE),
                        end=end.astimezone(SEATALK_INSIGHTS_TIMEZONE),
                        end_exclusive=end_exclusive.astimezone(SEATALK_INSIGHTS_TIMEZONE),
                        scheduled_start=start.astimezone(SEATALK_INSIGHTS_TIMEZONE),
                        scheduled_end=end.astimezone(SEATALK_INSIGHTS_TIMEZONE),
                        scheduled_end_exclusive=end_exclusive.astimezone(SEATALK_INSIGHTS_TIMEZONE),
                    )
        except (TypeError, ValueError) as error:
            raise ToolError("Monthly Report date range must use valid dates.") from error
        raise ToolError("Monthly Report start date cannot be later than end date.")
    return resolve_monthly_report_period(fallback)


def _monthly_report_evidence_period(report_period: MonthlyReportPeriod) -> MonthlyReportPeriod:
    evidence_start = max(
        report_period.start,
        report_period.end_exclusive - timedelta(days=MONTHLY_REPORT_EVIDENCE_DAYS),
    )
    return MonthlyReportPeriod(
        start=evidence_start,
        end=report_period.end,
        end_exclusive=report_period.end_exclusive,
        scheduled_start=evidence_start,
        scheduled_end=report_period.end,
        scheduled_end_exclusive=report_period.end_exclusive,
    )


def _parse_monthly_report_datetime(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty monthly report datetime")
    if len(text) == 10:
        return datetime.combine(date.fromisoformat(text), datetime_time.min, tzinfo=SEATALK_INSIGHTS_TIMEZONE)
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SEATALK_INSIGHTS_TIMEZONE)
    return parsed


def _monthly_optional_json_prompt_section(title: str, value: Any) -> list[str]:
    if not value:
        return []
    return [f"# {title}\n{_json_block(value)}"]


def build_monthly_report_prompt(
    *,
    template: str,
    generated_at: datetime,
    seatalk_history_text: str,
    key_projects: list[dict[str, Any]],
    prd_sources: list[dict[str, str]],
    prd_errors: list[str],
) -> str:
    sections = [
        (
            "# Task\n"
            "Generate Xiaodong Zheng's monthly team report as concise, business-ready Markdown.\n"
            "Use the configured template as the required structure. Do not invent facts; when evidence is weak, state the gap or mark as TBD.\n"
            "Synthesize the configured report-period SeaTalk history with Key Project Biz Project and Jira evidence. Prefer concrete project names, decisions, risks, owners, and dates.\n"
            "Do not include raw transcripts, long PRD excerpts, tool logs, or confidential implementation chatter that is not needed for a monthly business report.\n\n"
            "# Output Rules\n"
            "- Return only the final Markdown draft.\n"
            "- Keep it suitable to send by email after light PM editing.\n"
            "- Follow the template headings unless the evidence clearly requires a small additional subsection.\n"
            "- Start the report body directly with 'Highlights'. Do not create a '0. Critical Updates' heading or any numbered critical-update wrapper before Highlights.\n"
            "- If the configured template contains Markdown tables, preserve those table structures and fill rows from evidence; use TBD for missing cells instead of converting the table to bullets.\n"
            "- Do not include Jira ticket IDs or Jira links in the final report.\n\n"
            f"# Generated At\n{generated_at.isoformat()}\n\n"
            f"# Monthly Report Template\n{normalize_monthly_report_template(template)}\n\n"
            "# Key Project / Jira Evidence\n"
            f"{_json_block(key_projects)}"
        )
    ]
    sections.extend(_monthly_optional_json_prompt_section("PRD / Confluence Enrichment", prd_sources))
    sections.extend(_monthly_optional_json_prompt_section("PRD Enrichment Gaps", prd_errors))
    sections.append(
        "# SeaTalk History From Report Period\n"
        f"{seatalk_history_text or 'No readable SeaTalk messages were found in the report period.'}"
    )
    return "\n\n".join(sections)


def build_monthly_report_batch_prompt(
    *,
    template: str,
    generated_at: datetime,
    report_period: MonthlyReportPeriod,
    highlight_topics: list[str],
    source: str,
    payload: Any,
    prd_errors: list[str],
) -> str:
    source_label = _monthly_report_source_label(source)
    sections = [
        "# Task\n"
        f"Summarize one Monthly Report evidence batch from {source_label}.\n"
        "Do not write the final report. Extract only facts useful for the final monthly business report.\n"
        "For Highlight deep evidence, preserve narrative facts for the user-provided topics. For Monthly project evidence brief, keep other Key Projects concise and do not expand them into highlights.\n"
        f"Hard scope: include only Xiaodong-owned {', '.join(MONTHLY_REPORT_PRODUCT_SCOPE)} product updates. Exclude unrelated general awareness, HR, hiring, personal chat, random live issue, and generic IT/process/material-check updates even if a VIP or priority keyword appears.\n"
        "Use concise Markdown with these headings exactly: Highlights, Decisions, Risks, Owners, Project References, Open Asks, Evidence Gaps.\n"
        "This is an evidence-summary stage only. Do not repeat or infer the final email template; keep only structured facts, owner/date/status/risk evidence, and gaps needed by the final drafting call.\n"
        "For each highlight topic, respect its topic_intent/intent_focus/confidence/recommended_tone when present. High-confidence topics can be written as progress; low-confidence topics should remain monitoring items without over-claiming.\n"
        "If a highlight topic has product_area_scope, preserve only that product area's changes and timeline for that topic. Do not mix similarly named projects from other product areas into the highlight narrative.\n"
        "For go-live outcome topics, preserve launch result, employee/pilot feedback, production issue, stabilization, and post-live next-action facts. Do not substitute generic development or PRD progress for missing go-live outcome evidence.\n"
        "For issue follow-up topics, preserve the structured impact, root cause, completed mitigation, long-term solution, and next action when provided. Do not substitute broad project progress for missing incident follow-up evidence.\n"
        "Preserve concrete project names, Jira IDs, owners, markets, dates, decisions, blockers, and launch/status facts.\n"
        "If this batch has no material in-scope evidence, return only: No material update found.\n"
        "Do not include raw transcripts or long excerpts.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}\n\n"
        f"# Evidence Source\n{source_label}"
    ]
    sections.extend(_monthly_optional_json_prompt_section("User-Provided Highlight Topics", highlight_topics))
    sections.extend(_monthly_optional_json_prompt_section("PRD Enrichment Gaps", prd_errors))
    sections.append("# Batch Payload\n" f"{_payload_block(payload)}")
    return "\n\n".join(sections)


def build_monthly_highlight_topic_narrative_prompt(
    *,
    generated_at: datetime,
    report_period: MonthlyReportPeriod,
    topic_evidence: dict[str, Any],
) -> str:
    safe_topic_evidence = _strip_jira_issue_keys_from_data(topic_evidence)
    return (
        "# Task\n"
        "Write one manager-ready Monthly Report Highlight paragraph for the single topic below.\n"
        "Use only this topic's evidence. Do not use or invent facts from other topics.\n"
        "The audience is Xiaodong's manager. Write as an executive product update, not as an investigation log.\n"
        "Use the topic_intent, intent_focus, confidence, and recommended_tone fields to decide what kind of update this is.\n"
        "If product_area_scope is present, focus only on that product area's changes and timeline. Do not include similarly named project progress from other product areas.\n"
        "If topic_intent is go_live_outcome, focus on whether go-live happened, post-go-live result, employee/pilot feedback, production issues, stabilization, and next rollout/remediation. Do not replace missing go-live outcome evidence with generic development/testing/PRD progress; use generic project progress only as brief background.\n"
        "If topic_intent is issue_followup, use issue_followup_facts first: explain the business impact, root cause, completed containment, long-term remediation, and next action. Do not replace incident follow-up evidence with generic project progress.\n"
        "- high: state progress, business impact, risk/decision, and next movement clearly.\n"
        "- medium: describe directional progress and pending confirmation carefully.\n"
        "- low or none: frame as a monitoring item or pending confirmation; do not imply committed delivery.\n"
        "Do not expose source mechanics. Do not say SeaTalk, Gmail, evidence gap, query, thread, ticket, or no confirmed evidence.\n"
        "Do not include Jira ticket IDs or links. Return one compact paragraph only.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}\n\n"
        "# Topic Evidence\n"
        f"{_json_block(safe_topic_evidence)}"
    )


def build_monthly_report_merge_prompt(
    *,
    generated_at: datetime,
    report_period: MonthlyReportPeriod,
    highlight_topics: list[str],
    batch_summaries: list[dict[str, Any]],
    prd_errors: list[str],
) -> str:
    sections = [
        "# Task\n"
        "Merge Monthly Report batch summaries into one compact evidence brief for final drafting.\n"
        "Do not write the final report. Deduplicate repeated facts and keep the strongest concrete evidence.\n"
        "Keep Highlight deep evidence separate from Other Key Project Updates. Do not let non-highlight project updates become highlight narrative.\n"
        f"Hard scope: keep only Xiaodong-owned {', '.join(MONTHLY_REPORT_PRODUCT_SCOPE)} product updates. Drop unrelated updates even if they mention VIPs, approval, risk, launch, urgent, BSP, or OJK.\n"
        "Use these headings exactly: Executive Themes, Key Project Progress, Delivery Evidence, Risks And Blockers, Decisions Needed, Evidence Gaps.\n"
        "Carry forward highlight confidence and evidence-map gaps as drafting guidance, but do not turn source mechanics into report wording.\n"
        "Keep the user-provided highlight topics visible as the final draft's required Highlights scope.\n"
        "Keep the brief concise enough for one final model call.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}"
    ]
    sections.extend(_monthly_optional_json_prompt_section("User-Provided Highlight Topics", highlight_topics))
    sections.extend(_monthly_optional_json_prompt_section("PRD Enrichment Gaps", prd_errors))
    sections.append("# Batch Summaries\n" f"{_json_block(batch_summaries)}")
    return "\n\n".join(sections)


def build_monthly_report_compress_prompt(
    *,
    generated_at: datetime,
    evidence_brief: str,
) -> str:
    return (
        "# Task\n"
        "Compress this Monthly Report evidence brief before final drafting.\n"
        "Do not write the final report. Preserve concrete project names, owners, dates, decisions, risks, and asks.\n"
        "Remove repetition and low-value detail. Return concise Markdown only.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        "# Evidence Brief\n"
        f"{evidence_brief}"
    )


def build_monthly_report_final_prompt(
    *,
    template: str,
    generated_at: datetime,
    report_period: MonthlyReportPeriod,
    highlight_topics: list[str],
    evidence_brief: str,
    monthly_evidence_brief: list[dict[str, Any]],
    highlight_deep_evidence: list[dict[str, Any]] | None = None,
    highlight_narratives: list[dict[str, Any]] | None = None,
    historical_report_style_guide: dict[str, Any] | None = None,
) -> str:
    included_project_evidence = _strip_jira_issue_keys_from_data(_compact_monthly_evidence_for_final(monthly_evidence_brief))
    safe_highlight_deep_evidence = _strip_jira_issue_keys_from_data(
        _compact_highlight_deep_evidence_for_prompt(
            highlight_deep_evidence or [],
            include_source_evidence=False,
        )
    )
    safe_highlight_narratives = _strip_jira_issue_keys_from_data(highlight_narratives or [])
    safe_style_guide = _compact_monthly_report_style_guide(historical_report_style_guide)
    safe_evidence_brief = re.sub(
        r"No material update found",
        "No material update; use BRD status",
        _strip_jira_issue_keys_for_report(evidence_brief),
        flags=re.IGNORECASE,
    )
    return (
        "# Task\n"
        "Generate Xiaodong Zheng's monthly team report as concise, business-ready Markdown.\n"
        "Use the configured template as the required structure. Do not invent facts; when evidence is weak, state the gap or mark as TBD.\n"
        "Use only the Other Key Project Updates JSON below as the authoritative project allowlist. The compact evidence brief is supplemental context only.\n"
        "Use Highlight Narrative Candidates as the primary source for the Highlights section, and use Highlight Deep Evidence only to resolve missing nuance. If no user-provided Highlight Topics are listed, do not invent Highlights.\n"
        "Do not generate Anti-Fraud, Credit Risk, or Ops Risk project update tables; the backend renders those tables deterministically from included project JSON after this draft.\n"
        "The audience is Xiaodong's manager. Write Highlights as an executive product update, not as an investigation log: emphasize business impact, delivery progress, material risk, decision needed, and next action.\n"
        "Use Historical Sent Report Style Guide for tone, section rhythm, subject format, and wording style only. Do not copy old facts into the current report.\n"
        "Use calm, factual, ownership-oriented wording. Avoid alarmist language, raw technical incident wording, internal tool/process details, chat-style phrasing, and over-hedged phrases unless the uncertainty itself is the management point.\n"
        "Use the topic_intent/intent_focus/confidence/recommended_tone fields inside Highlight Deep Evidence to calibrate wording: high confidence can state progress directly; medium confidence should be framed as directional progress with pending confirmation; low or none should be framed as a watch item or pending confirmation, not as a failure to find evidence.\n"
        "If a Highlight Deep Evidence item has product_area_scope, write that Highlight only from that product area's changes and timeline. For example, a Credit Risk topic should not include Anti-Fraud progress even if the project names overlap.\n"
        "For go_live_outcome highlights, do not let general project development/testing progress replace launch result or feedback. If launch outcome evidence is missing, write that the go-live outcome remains pending confirmation in manager-ready language.\n"
        "For issue_followup highlights, use issue_followup_facts when available and cover impact, root cause, completed mitigation, long-term solution, and next action in manager-ready wording. Do not dilute these topics into generic project progress.\n"
        f"Hard scope: the final report must contain only Xiaodong-owned {', '.join(MONTHLY_REPORT_PRODUCT_SCOPE)} product updates. Do not include unrelated general awareness, HR, hiring, personal chat, random live issue, or generic IT/process/material-check updates. VIP or priority-keyword mentions are not enough unless the evidence is in-scope.\n"
        "Never include a project-table row unless it is attached to an item where include=true in Other Key Project Updates JSON. Non-project highlight topics may appear only in Highlights when supported by Highlight Deep Evidence.\n"
        "Do not write 'Evidence-limited' unless that exact wording appears in the structured status_facts for an included project.\n"
        "Do not write 'prioritization pressure', capacity pressure, or resource pressure unless that exact project has a direct risk entry containing capacity, resource, or prioritization evidence.\n"
        "Exclude random live incidents, DB instability, local registration monitoring, Shopee acquisition, and onboarding health unless they are explicitly tied to an included project and a Xiaodong decision/action, or they are one of the user-provided highlight topics with direct Highlight Deep Evidence.\n"
        "Do not write 'No material update found' as a project status. Use BRD in the Current Status column when a project has no material update.\n"
        "Do not include raw transcripts, long PRD excerpts, tool logs, or confidential implementation chatter that is not needed for a monthly business report.\n\n"
        "# Output Rules\n"
        "- Return only the final Markdown draft.\n"
        "- Keep it suitable to send by email after light PM editing.\n"
        f"- Email subject must follow: {monthly_report_subject_pattern()}.\n"
        "- Follow the template headings unless the evidence clearly requires a small additional subsection.\n"
        "- Start the report body directly with 'Highlights' when user-provided Highlight Topics exist. Do not create a '0. Critical Updates' heading or any numbered critical-update wrapper before Highlights. If no Highlight Topics are provided, skip the Highlights section and start with the normal report body before backend-rendered project update tables.\n"
        "- Do not generate the project update tables. The backend will append deterministic Markdown tables from Other Key Project Updates after your Highlights narrative.\n"
        "- Highlights must cover only the user-provided highlight topics below; do not add unrelated highlight topics. If the list is empty, write no highlight bullets.\n"
        "- Each Highlight should be manager-ready: one compact paragraph per topic, focused on what changed, why it matters, current risk or decision, and the expected next movement.\n"
        "- Do not expose raw evidence mechanics in Highlights. Do not say 'SeaTalk says', 'Gmail says', 'evidence gap', 'no confirmed evidence', 'query', 'thread', 'ticket', or similar source/tool terms.\n"
        "- For weakly supported topics, prefer manager-ready wording such as 'This remains pending confirmation before it can be positioned as material progress' instead of tool-facing wording.\n"
        "- Do not include Jira ticket IDs, Jira links, or issue-key references in the report.\n"
        "- Do not include a Key Follow-Ups section.\n"
        "- Current Status must be exactly one of: BRD, PRD, Dev, UAT. Do not add explanations in that cell.\n\n"
        "- Target Tech Live Date must use target_tech_live_date from Other Key Project Updates exactly. It may be MMM YYYY, such as May 2026, backend fallback quarter, such as Q3 2026, or TBC when Monthly Requirements marks the tech-live date as TBC/TBD. Do not infer a target date from timeline_facts or any version starting with Planning.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}\n\n"
        f"# User-Provided Highlight Topics\n{_json_block(highlight_topics)}\n\n"
        f"# Monthly Report Template\n{normalize_monthly_report_template(template)}\n\n"
        "# Historical Sent Report Style Guide\n"
        f"{_json_block(safe_style_guide)}\n\n"
        "# Highlight Narrative Candidates\n"
        f"{_json_block(safe_highlight_narratives)}\n\n"
        "# Highlight Deep Evidence\n"
        f"{_json_block(safe_highlight_deep_evidence)}\n\n"
        "# Other Key Project Updates\n"
        f"{_json_block(included_project_evidence)}\n\n"
        "# Compact Evidence Brief\n"
        f"{safe_evidence_brief or 'No readable evidence was found for this monthly report.'}"
    )


def _compact_monthly_evidence_for_final(monthly_evidence_brief: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for item in monthly_evidence_brief:
        if not item.get("include"):
            continue
        compacted.append(
            {
                "include": True,
                "project_id": str(item.get("project_id") or item.get("bpmis_id") or "").strip(),
                "bpmis_id": str(item.get("bpmis_id") or item.get("project_id") or "").strip(),
                "project_name": str(item.get("project_name") or "").strip(),
                "product_area": str(item.get("product_area") or "").strip(),
                "teams": _compact_text_list(item.get("teams"), limit=5, max_chars=80),
                "market": str(item.get("market") or "").strip(),
                "priority": str(item.get("priority") or "").strip(),
                "seatalk_group_ids": _compact_text_list(item.get("seatalk_group_ids"), limit=8, max_chars=80),
                "material_update_score": _safe_int(item.get("material_update_score")),
                "current_status": _monthly_report_status_label(item.get("current_status")),
                "target_tech_live_date": _monthly_report_month_label(item.get("target_tech_live_date")),
                "target_tech_live_version": str(item.get("target_tech_live_version") or "").strip(),
                "target_tech_live_source": str(item.get("target_tech_live_source") or "").strip(),
                "target_tech_live_source_detail": item.get("target_tech_live_source_detail") if isinstance(item.get("target_tech_live_source_detail"), dict) else {},
                "status_facts": _compact_report_text_list(item.get("status_facts"), limit=6, max_chars=320),
                "timeline_facts": _compact_report_text_list(item.get("timeline_facts"), limit=5, max_chars=240),
                "risks": _compact_report_text_list(item.get("risks"), limit=4, max_chars=320),
                "decisions_needed": _compact_report_text_list(item.get("decisions_needed"), limit=4, max_chars=320),
                "matched_prd_summaries": _compact_report_text_list(item.get("matched_prd_summaries"), limit=3, max_chars=500),
            }
        )
    return compacted


def _compact_monthly_project_evidence_for_batch(monthly_evidence_brief: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for item in monthly_evidence_brief:
        if not item.get("include"):
            continue
        compacted.append(
            {
                "include": True,
                "project_id": str(item.get("project_id") or item.get("bpmis_id") or "").strip(),
                "bpmis_id": str(item.get("bpmis_id") or item.get("project_id") or "").strip(),
                "project_name": str(item.get("project_name") or "").strip(),
                "product_area": str(item.get("product_area") or "").strip(),
                "teams": _compact_text_list(item.get("teams"), limit=5, max_chars=80),
                "market": str(item.get("market") or "").strip(),
                "priority": str(item.get("priority") or "").strip(),
                "jira_ids": _compact_text_list(item.get("jira_ids"), limit=8, max_chars=40),
                "material_update_score": _safe_int(item.get("material_update_score")),
                "current_status": _monthly_report_status_label(item.get("current_status")),
                "target_tech_live_date": _monthly_report_month_label(item.get("target_tech_live_date")),
                "target_tech_live_source": str(item.get("target_tech_live_source") or "").strip(),
                "status_facts": _compact_report_text_list(item.get("status_facts"), limit=4, max_chars=140),
                "timeline_facts": _compact_report_text_list(item.get("timeline_facts"), limit=4, max_chars=220),
                "risks": _compact_report_text_list(item.get("risks"), limit=4, max_chars=260),
                "decisions_needed": _compact_report_text_list(item.get("decisions_needed"), limit=4, max_chars=260),
                "matched_prd_summaries": _compact_report_text_list(item.get("matched_prd_summaries"), limit=4, max_chars=520),
                "monthly_requirements": [
                    {
                        "matched_line": str(entry.get("matched_line") or "").strip()[:300],
                        "target_month": str(entry.get("target_month") or "").strip(),
                        "source_subject": str(entry.get("source_subject") or "").strip()[:160],
                    }
                    for entry in (item.get("monthly_requirements") or [])
                    if isinstance(entry, dict)
                ][:3],
            }
        )
    return compacted


def _compact_highlight_deep_evidence_for_prompt(
    highlight_deep_evidence: list[dict[str, Any]],
    *,
    include_source_evidence: bool,
) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for item in highlight_deep_evidence:
        if not isinstance(item, dict):
            continue
        evidence_map = item.get("evidence_map") if isinstance(item.get("evidence_map"), dict) else {}
        compact_item: dict[str, Any] = {
            "topic": str(item.get("topic") or "").strip(),
            "topic_intent": str(item.get("topic_intent") or evidence_map.get("topic_intent") or "").strip(),
            "intent_focus": str(item.get("intent_focus") or "").strip(),
            "topic_type": str(item.get("topic_type") or "").strip(),
            "product_area_scope": str(item.get("product_area_scope") or evidence_map.get("product_area_scope") or "").strip(),
            "matched_project_ids": _compact_text_list(item.get("matched_project_ids"), limit=8, max_chars=60),
            "matched_project_names": _compact_text_list(item.get("matched_project_names"), limit=8, max_chars=120),
            "project_updates": _compact_highlight_project_updates(item.get("project_updates")),
            "issue_followup_facts": _compact_issue_followup_facts(item.get("issue_followup_facts")),
            "confidence": str(item.get("confidence") or evidence_map.get("confidence") or "").strip(),
            "recommended_tone": str(item.get("recommended_tone") or evidence_map.get("recommended_tone") or "").strip(),
            "evidence_map": _compact_highlight_evidence_map(evidence_map),
        }
        if include_source_evidence:
            compact_item.update(
                {
                    "seatalk_evidence": _compact_report_text_list(item.get("seatalk_evidence"), limit=8, max_chars=420),
                    "gmail_evidence": _compact_report_text_list(item.get("gmail_evidence"), limit=5, max_chars=520),
                    "google_sheet_evidence": _compact_google_sheet_evidence(item.get("google_sheet_evidence"))[:3],
                    "prd_scope_summaries": _compact_report_text_list(item.get("prd_scope_summaries"), limit=4, max_chars=520),
                    "gmail_error": str(item.get("gmail_error") or "").strip()[:240],
                }
            )
        compacted.append(compact_item)
    return compacted


def _compact_highlight_project_updates(value: Any) -> list[dict[str, Any]]:
    updates = value if isinstance(value, list) else []
    compacted: list[dict[str, Any]] = []
    for project in updates:
        if not isinstance(project, dict):
            continue
        compacted.append(
            {
                "bpmis_id": str(project.get("bpmis_id") or "").strip(),
                "project_name": str(project.get("project_name") or "").strip(),
                "market": str(project.get("market") or "").strip(),
                "priority": str(project.get("priority") or "").strip(),
                "current_status": _monthly_report_status_label(project.get("current_status")),
                "target_tech_live_date": _monthly_report_month_label(project.get("target_tech_live_date")),
                "target_tech_live_source": str(project.get("target_tech_live_source") or "").strip(),
                "status_facts": _compact_report_text_list(project.get("status_facts"), limit=4, max_chars=260),
                "timeline_facts": _compact_report_text_list(project.get("timeline_facts"), limit=3, max_chars=220),
                "prd_scope_summaries": _compact_report_text_list(project.get("prd_scope_summaries"), limit=2, max_chars=360),
            }
        )
    return compacted[:8]


def _compact_issue_followup_facts(value: Any) -> dict[str, list[str]]:
    facts = value if isinstance(value, dict) else {}
    compacted: dict[str, list[str]] = {}
    for key in ("impact", "root_cause", "short_term_solution", "long_term_solution", "next_action"):
        compacted[key] = _compact_report_text_list(facts.get(key), limit=3, max_chars=320)
    return {key: items for key, items in compacted.items() if items}


def _compact_highlight_evidence_map(value: dict[str, Any]) -> dict[str, Any]:
    if not value:
        return {}
    keys = (
        "topic",
        "topic_type",
        "topic_intent",
        "product_area_scope",
        "confidence",
        "recommended_tone",
        "evidence_gaps",
        "selected_sources",
    )
    compacted = {key: value.get(key) for key in keys if key in value}
    if isinstance(value.get("issue_followup_facts"), dict):
        compacted["issue_followup_facts"] = _compact_issue_followup_facts(value.get("issue_followup_facts"))
    return compacted


def _compact_text_list(value: Any, *, limit: int, max_chars: int) -> list[str]:
    items = value if isinstance(value, list) else []
    compacted: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        compacted.append(text[:max_chars])
        if len(compacted) >= limit:
            break
    return compacted


def _compact_report_text_list(value: Any, *, limit: int, max_chars: int) -> list[str]:
    return [_strip_jira_issue_keys_for_report(item) for item in _compact_text_list(value, limit=limit, max_chars=max_chars)]


def _normalize_google_sheet_evidence(value: Any) -> list[dict[str, str]]:
    items = value if isinstance(value, list) else []
    normalized: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()[:240]
        text = str(item.get("text") or "").strip()[:2_000]
        status = str(item.get("access_status") or "").strip()[:80]
        url = str(item.get("url") or "").strip()[:500]
        if not (title or text or status or url):
            continue
        normalized.append({"title": title, "text": text, "access_status": status, "url": url})
        if len(normalized) >= 4:
            break
    return normalized


def _compact_google_sheet_evidence(value: Any) -> list[dict[str, str]]:
    return [
        {
            "title": item.get("title", ""),
            "text": _strip_jira_issue_keys_for_report(item.get("text", ""))[:1_200],
            "access_status": item.get("access_status", ""),
        }
        for item in _normalize_google_sheet_evidence(value)
    ]


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _record_monthly_report_timing(timings: dict[str, float], key: str, started_at: float) -> None:
    timings[key] = round(max(0.0, time.monotonic() - started_at), 3)


def build_monthly_report_prd_scope_prompt(
    *,
    generated_at: datetime,
    report_period: MonthlyReportPeriod,
    prd_source: dict[str, str],
) -> str:
    return (
        "# Task\n"
        "Summarize this PRD/Confluence page for Monthly Report evidence.\n"
        f"Hard scope: keep only Xiaodong-owned {', '.join(MONTHLY_REPORT_PRODUCT_SCOPE)} product impact.\n"
        "Focus on affected product scope, explicit requirement changes, market or user-flow impact, delivery status signals, and open questions.\n"
        "If the PRD does not explicitly describe a historical change, summarize the current scope only and do not invent a diff.\n"
        "Return concise Markdown only, not the final report.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}\n\n"
        "# PRD Source\n"
        f"{_json_block(prd_source)}"
    )


def generate_monthly_report_with_codex(
    *,
    prompt: str,
    settings: Settings,
    workspace_root: Path,
    prompt_mode: str = MONTHLY_REPORT_PROMPT_VERSION,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    provider = CodexCliBridgeSourceCodeQALLMProvider(
        workspace_root=workspace_root,
        timeout_seconds=settings.monthly_report_codex_timeout_seconds,
        concurrency_limit=settings.source_code_qa_codex_concurrency,
        session_mode="ephemeral",
        codex_binary=os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or None,
    )
    codex_model = resolve_codex_model(
        CODEX_ROUTE_DEEP,
        legacy_env_names=("MONTHLY_REPORT_CODEX_MODEL",),
    )
    result = provider.generate(
        payload={
            "systemInstruction": {
                "parts": [
                    {
                        "text": (
                            "You are a senior Digital Banking product leader preparing a monthly status report. "
                            "Return only polished Markdown. Be concise, factual, and action-oriented."
                        )
                    }
                ]
            },
            "contents": [{"parts": [{"text": prompt}]}],
            "codex_prompt_mode": prompt_mode,
            "_progress_callback": progress_callback,
            "_codex_reasoning_effort": resolve_codex_reasoning_effort(CODEX_ROUTE_DEEP),
            "_codex_estimated_prompt_tokens": _estimate_token_count(prompt),
            "_llm_ledger_flow": "monthly_report",
            "_llm_ledger_route": CODEX_ROUTE_DEEP,
        },
        primary_model=codex_model,
        fallback_model=codex_model,
    )
    return {
        "result_markdown": provider.extract_text(result.payload),
        "model_id": result.model,
        "trace": result.payload.get("codex_cli_trace") if isinstance(result.payload, dict) else {},
    }


def send_monthly_report_email(
    *,
    credential_store: StoredGoogleCredentials,
    owner_email: str,
    recipient: str,
    subject: str,
    draft_markdown: str,
    gmail_service: Any | None = None,
) -> MonthlyReportSendResult:
    owner = str(owner_email or "").strip().lower()
    target = str(recipient or DEFAULT_MONTHLY_REPORT_RECIPIENT).strip().lower()
    body = str(draft_markdown or "").strip()
    if not body:
        raise ToolError("Monthly Report draft is empty.")
    if not owner:
        raise ConfigError("Gmail sender owner email is missing.")
    credentials_payload = credential_store.load(owner_email=owner)
    scopes = {str(scope).strip() for scope in (credentials_payload.get("scopes") or []) if str(scope).strip()}
    if GMAIL_SEND_SCOPE not in scopes:
        raise ConfigError("Gmail send permission is missing. Reconnect Google once to grant gmail.send.")
    credentials: Credentials = credentials_from_payload(credentials_payload)
    response = send_gmail_message(
        credentials=credentials,
        sender=owner,
        recipient=target,
        subject=subject,
        text_body=body + "\n",
        html_body=monthly_report_markdown_to_html(body),
        gmail_service=gmail_service,
    )
    return MonthlyReportSendResult(
        status="sent",
        recipient=target,
        subject=subject,
        message_id=str((response or {}).get("id") or ""),
    )


def monthly_report_subject(now: datetime | None = None, *, period: MonthlyReportPeriod | None = None) -> str:
    report_period = period or resolve_monthly_report_period(now)
    start_label = _monthly_report_day_month_label(report_period.start)
    end_label = _monthly_report_day_month_label(report_period.end_exclusive - timedelta(days=1))
    return f"[Banking] Product Update ({start_label} - {end_label}) - Anti-Fraud, Credit Risk & Ops Risk"


def _monthly_report_day_month_label(value: datetime) -> str:
    return f"{value.day} {value.strftime('%b')}"


def monthly_report_markdown_to_html(markdown_text: str) -> str:
    lines = []
    in_list = False
    table: dict[str, list[list[str]] | list[str]] | None = None

    def close_list() -> None:
        nonlocal in_list
        if in_list:
            lines.append("</ul>")
            in_list = False

    def close_table() -> None:
        nonlocal table
        if table is not None:
            lines.append(_render_markdown_table_html(table["headers"], table["rows"]))
            table = None

    raw_lines = str(markdown_text or "").splitlines()
    for index, raw_line in enumerate(raw_lines):
        line = raw_line.strip()
        if not line:
            close_list()
            close_table()
            continue
        next_line = raw_lines[index + 1].strip() if index + 1 < len(raw_lines) else ""
        if table is None and "|" in line and _is_markdown_table_separator(next_line):
            close_list()
            table = {"headers": _split_markdown_table_row(line), "rows": []}
            continue
        if table is not None:
            if _is_markdown_table_separator(line):
                continue
            if "|" in line:
                table["rows"].append(_split_markdown_table_row(line))
                continue
            close_table()
        heading = re.match(r"^(#{1,4})\s+(.+)$", line)
        if heading:
            close_list()
            close_table()
            level = min(4, len(heading.group(1)) + 1)
            lines.append(f"<h{level}>{_inline_markdown(heading.group(2))}</h{level}>")
            continue
        item = re.match(r"^(?:[-*]|\d+[.)])\s+(.+)$", line)
        if item:
            close_table()
            if not in_list:
                lines.append("<ul>")
                in_list = True
            lines.append(f"<li>{_inline_markdown(item.group(1))}</li>")
            continue
        close_list()
        close_table()
        lines.append(f"<p>{_inline_markdown(line)}</p>")
    close_list()
    close_table()
    return "<html><body>" + "\n".join(lines) + "</body></html>"


def _inline_markdown(value: str) -> str:
    text = html.escape(str(value or ""))
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"`(.+?)`", r"<code>\1</code>", text)
    return text


def _split_markdown_table_row(line: str) -> list[str]:
    text = str(line or "").strip()
    if text.startswith("|"):
        text = text[1:]
    if text.endswith("|"):
        text = text[:-1]
    return [cell.strip() for cell in text.split("|")]


def _is_markdown_table_separator(line: str) -> bool:
    cells = _split_markdown_table_row(line)
    return len(cells) > 1 and all(re.fullmatch(r":?-{3,}:?", cell.replace(" ", "")) for cell in cells)


def _render_markdown_table_html(headers: list[str], rows: list[list[str]]) -> str:
    column_count = max([len(headers), *(len(row) for row in rows), 1])
    table_style = "border-collapse:collapse;width:100%;table-layout:fixed;margin:12px 0;"
    cell_style = (
        "border:1px solid #111827;padding:6px 8px;text-align:left;vertical-align:top;"
        "white-space:normal;word-break:normal;overflow-wrap:anywhere;"
    )
    column_widths = _monthly_report_table_column_widths(headers, column_count)

    def render_cells(cells: list[str], tag: str) -> str:
        base_style = cell_style + ("font-weight:700;background:#f8fafc;" if tag == "th" else "")
        return "".join(
            f'<{tag} style="{base_style}width:{column_widths[index]};">{_inline_markdown(cells[index] if index < len(cells) else "")}</{tag}>'
            for index in range(column_count)
        )

    colgroup = "".join(f'<col style="width:{width};">' for width in column_widths)
    body = "".join(f"<tr>{render_cells(row, 'td')}</tr>" for row in rows)
    return (
        f'<table style="{table_style}">'
        f"<colgroup>{colgroup}</colgroup>"
        f"<thead><tr>{render_cells(headers, 'th')}</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )


def _monthly_report_table_column_widths(headers: list[str], column_count: int) -> list[str]:
    normalized_headers = [_normalize_monthly_report_table_header(header) for header in headers]
    project_update_headers = [
        "region",
        "priority",
        "project",
        "current status",
        "target tech live date",
    ]
    if normalized_headers[:5] == project_update_headers:
        return ["12%", "11%", "39%", "16%", "22%"] + _equal_widths(max(0, column_count - 5))
    return _equal_widths(column_count)


def _normalize_monthly_report_table_header(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().casefold())


def _equal_widths(column_count: int) -> list[str]:
    if column_count <= 0:
        return []
    width = f"{100 / column_count:.4f}%"
    return [width] * column_count


def _monthly_report_data_root(settings: Settings) -> Path:
    data_root = settings.team_portal_data_dir
    if data_root.is_absolute():
        return data_root
    local_agent_data_dir = str(os.getenv("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR") or "").strip()
    if local_agent_data_dir:
        return Path(local_agent_data_dir).expanduser()
    return data_root.expanduser()


def _monthly_report_cache_root(settings: Settings) -> Path:
    return _monthly_report_data_root(settings) / "monthly_report" / "cache"


def _monthly_report_gmail_owner_email(settings: Settings) -> str:
    return str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower()


def _monthly_report_cache_digest(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _monthly_report_gmail_topic_cache_path(
    settings: Settings,
    *,
    owner_email: str,
    report_period: MonthlyReportPeriod,
    topic: str,
) -> Path:
    digest = _monthly_report_cache_digest(
        {
            "version": MONTHLY_REPORT_GMAIL_TOPIC_CACHE_VERSION,
            "owner_email": owner_email,
            "period_start": report_period.start_date,
            "period_end": report_period.end_date,
            "period_end_exclusive": report_period.end_exclusive.isoformat(),
            "topic": str(topic or "").strip(),
            "max_threads": MONTHLY_REPORT_MAX_HIGHLIGHT_GMAIL_THREADS_PER_TOPIC,
        }
    )
    return _monthly_report_cache_root(settings) / "gmail_topic" / f"{digest}.json"


def _monthly_report_prd_scope_cache_path(
    settings: Settings,
    *,
    report_period: MonthlyReportPeriod,
    prd_source: dict[str, str],
) -> Path:
    digest = _monthly_report_cache_digest(
        {
            "version": MONTHLY_REPORT_PRD_SCOPE_CACHE_VERSION,
            "prompt_version": MONTHLY_REPORT_PROMPT_VERSION,
            "period_start": report_period.start_date,
            "period_end": report_period.end_date,
            "period_end_exclusive": report_period.end_exclusive.isoformat(),
            "prd_url": str(prd_source.get("url") or "").strip(),
            "updated_at": str(prd_source.get("updated_at") or "").strip(),
        }
    )
    return _monthly_report_cache_root(settings) / "prd_scope" / f"{digest}.json"


def _monthly_report_batch_summary_cache_path(
    settings: Settings,
    *,
    report_period: MonthlyReportPeriod,
    source: str,
    index: int,
    highlight_topics: list[str],
    template: str,
    payload: Any,
    prd_errors: list[str],
) -> Path:
    digest = _monthly_report_cache_digest(
        {
            "version": MONTHLY_REPORT_BATCH_SUMMARY_CACHE_VERSION,
            "prompt_version": MONTHLY_REPORT_PROMPT_VERSION,
            "period_start": report_period.start_date,
            "period_end": report_period.end_date,
            "period_end_exclusive": report_period.end_exclusive.isoformat(),
            "source": str(source or "").strip(),
            "index": int(index or 0),
            "highlight_topics": [str(topic or "").strip() for topic in highlight_topics],
            "template_digest": _monthly_report_cache_digest({"template": normalize_monthly_report_template(template)}),
            "payload_digest": _monthly_report_cache_digest({"payload": payload}),
            "prd_errors": [str(error or "").strip() for error in prd_errors if str(error or "").strip()],
        }
    )
    return _monthly_report_cache_root(settings) / "batch_summary" / f"{digest}.json"


def _monthly_report_topic_narrative_cache_path(
    settings: Settings,
    *,
    report_period: MonthlyReportPeriod,
    topic: str,
    evidence: dict[str, Any],
) -> Path:
    digest = _monthly_report_cache_digest(
        {
            "version": MONTHLY_REPORT_TOPIC_NARRATIVE_CACHE_VERSION,
            "prompt_version": MONTHLY_REPORT_PROMPT_VERSION,
            "period_start": report_period.start_date,
            "period_end": report_period.end_date,
            "period_end_exclusive": report_period.end_exclusive.isoformat(),
            "topic": str(topic or "").strip(),
            "evidence_digest": _monthly_report_cache_digest({"evidence": evidence}),
        }
    )
    return _monthly_report_cache_root(settings) / "highlight_narrative" / f"{digest}.json"


def _monthly_report_style_guide_cache_path(settings: Settings, *, owner_email: str) -> Path:
    digest = _monthly_report_cache_digest(
        {
            "version": MONTHLY_REPORT_STYLE_GUIDE_CACHE_VERSION,
            "owner_email": str(owner_email or "").strip().lower(),
            "subject_pattern": monthly_report_subject_pattern(),
            "max_reports": MONTHLY_REPORT_STYLE_GUIDE_MAX_REPORTS,
        }
    )
    return _monthly_report_cache_root(settings) / "historical_style_guide" / f"{digest}.json"


def _read_monthly_report_json_cache(path: Path, *, max_age_seconds: int | None = None) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        if max_age_seconds is not None and max_age_seconds > 0:
            age_seconds = max(0.0, time.time() - path.stat().st_mtime)
            if age_seconds > max_age_seconds:
                return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _write_monthly_report_json_cache(path: Path, payload: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, path)
    except OSError:
        return


def _vip_emails(config: dict[str, Any]) -> list[str]:
    emails: list[str] = []
    for person in config.get("vip_people") or []:
        if not isinstance(person, dict):
            continue
        for email in person.get("emails") or []:
            normalized = str(email or "").strip().lower()
            if normalized and "@" in normalized and normalized not in emails:
                emails.append(normalized)
    return emails


def _project_from_evidence_item(item: dict[str, Any]) -> dict[str, Any]:
    jira_ids = [str(value).strip() for value in (item.get("jira_ids") or []) if str(value).strip()]
    return {
        "bpmis_id": str(item.get("bpmis_id") or item.get("project_id") or "").strip(),
        "project_name": str(item.get("project_name") or "").strip(),
        "market": str(item.get("market") or "").strip(),
        "priority": str(item.get("priority") or "").strip(),
        "jira_tickets": [{"jira_id": jira_id, "jira_title": ""} for jira_id in jira_ids],
    }


def _highlight_deep_evidence_text(items: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for item in items:
        topic = str(item.get("topic") or "").strip()
        if topic:
            lines.append(f"Highlight topic: {topic}")
        for key in ("seatalk_evidence", "gmail_evidence", "google_sheet_evidence", "prd_scope_summaries"):
            for value in item.get(key) or []:
                if isinstance(value, dict):
                    text = "\n".join(
                        str(part or "").strip()
                        for part in (value.get("title"), value.get("text"), value.get("access_status"))
                        if str(part or "").strip()
                    )
                else:
                    text = str(value or "").strip()
                if text:
                    lines.append(text)
    return "\n".join(lines)


def _project_product_area(project: dict[str, Any]) -> str:
    teams = " ".join(str(item or "") for item in (project.get("teams") or []))
    project_name = str(project.get("project_name") or "")
    project_text = project_name.casefold()
    text = f"{teams} {project_name}".casefold()
    if "grc" in project_text or "rcsa" in project_text or "operational risk" in project_text:
        return "Ops Risk"
    if _monthly_report_project_has_credit_risk_scope(project_text, teams.casefold()):
        return "Credit Risk"
    if "anti" in text or "fraud" in text or "afasa" in text or "scam" in text or re.search(r"\baf\b", text):
        return "Anti-fraud"
    if "grc" in text or "ops" in text or "operational risk" in text or "rcsa" in text:
        return "Ops Risk"
    return "Credit Risk"


@lru_cache(maxsize=1)
def _monthly_report_business_glossary() -> dict[str, Any]:
    glossary = _monthly_report_read_json_file(MONTHLY_REPORT_BUSINESS_GLOSSARY_PATH)
    if not isinstance(glossary, dict):
        glossary = {}
    source_counts: dict[str, int] = {}
    derived_terms: dict[str, list[str]] = {}
    for path in MONTHLY_REPORT_SOURCE_CODE_QA_GLOSSARY_SOURCE_PATHS:
        payload = _monthly_report_read_json_file(path)
        if not isinstance(payload, dict):
            source_counts[path.name] = 0
            continue
        terms_by_domain = _monthly_report_extract_domain_terms(payload)
        source_counts[path.name] = sum(len(terms) for terms in terms_by_domain.values())
        for domain, terms in terms_by_domain.items():
            derived_terms.setdefault(domain, [])
            derived_terms[domain].extend(terms)
    glossary["_derived_source_counts"] = source_counts
    glossary["_derived_terms"] = {
        domain: _dedupe_preserve_order(terms)[:80]
        for domain, terms in derived_terms.items()
    }
    return glossary


def monthly_report_business_glossary_summary() -> dict[str, Any]:
    glossary = _monthly_report_business_glossary()
    entries = [entry for entry in (glossary.get("entries") or []) if isinstance(entry, dict)]
    return {
        "version": glossary.get("version"),
        "entry_count": len(entries),
        "domains": sorted((glossary.get("domains") or {}).keys()),
        "derived_source_counts": glossary.get("_derived_source_counts") or {},
    }


def build_monthly_report_query_plan(topic: Any, *, selected_sources: list[str] | None = None) -> dict[str, Any]:
    text = str(topic or "").strip()
    selected = selected_sources or list(MONTHLY_REPORT_HIGHLIGHT_SOURCES)
    glossary_entries = _monthly_report_glossary_entries_for_topic(text)
    qualifier_marker_groups = _monthly_report_highlight_qualifier_marker_groups_from_entries(text, glossary_entries)
    aliases = _highlight_topic_aliases_without_query_plan(text)
    aliases.update(_monthly_report_glossary_aliases_for_entries(glossary_entries))
    qualifiers = {
        "markets": sorted(_monthly_report_requested_markets(text)),
        "domains": _dedupe_preserve_order([
            str(entry.get("domain") or "")
            for entry in glossary_entries
            if str(entry.get("domain") or "").strip()
        ]),
        "context_only_terms": _monthly_report_context_only_terms_for_entries(glossary_entries, text),
    }
    forbidden_meanings = _dedupe_preserve_order(
        [
            str(term or "").strip()
            for entry in glossary_entries
            for term in (entry.get("reject_context_any") or [])
            if str(term or "").strip()
        ]
    )
    return {
        "topic": text,
        "primary_topic": _monthly_report_primary_topic(text, glossary_entries),
        "intent": _monthly_report_highlight_topic_intent(text),
        "product_area_scope": _monthly_report_highlight_product_area_scope(text) or _monthly_report_product_area_scope_from_glossary(glossary_entries),
        "selected_sources": selected,
        "source_policy": _monthly_report_query_source_policy(selected),
        "qualifiers": qualifiers,
        "qualifier_marker_groups": [list(group) for group in qualifier_marker_groups],
        "glossary_matches": [
            {
                "id": str(entry.get("id") or ""),
                "domain": str(entry.get("domain") or ""),
                "canonical": str(entry.get("canonical") or ""),
                "context_only": bool(entry.get("context_only_terms")),
            }
            for entry in glossary_entries
        ],
        "forbidden_meanings": forbidden_meanings,
        "aliases": sorted(alias for alias in aliases if alias),
    }


def _monthly_report_query_source_policy(selected_sources: list[str]) -> dict[str, str]:
    selected = set(selected_sources or [])
    policy: dict[str, str] = {}
    if MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK in selected:
        policy[MONTHLY_REPORT_HIGHLIGHT_SOURCE_SEATALK] = "conversation_level"
    if MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL in selected:
        policy[MONTHLY_REPORT_HIGHLIGHT_SOURCE_GMAIL] = "thread_level"
    if MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD in selected:
        policy[MONTHLY_REPORT_HIGHLIGHT_SOURCE_TEAM_DASHBOARD] = "project_level"
    return policy


def _monthly_report_product_area_scope_from_glossary(entries: list[dict[str, Any]]) -> str:
    domain_to_scope = {"AF": "Anti-fraud", "CRMS": "Credit Risk", "GRC": "Ops Risk"}
    scopes = _dedupe_preserve_order([
        domain_to_scope.get(str(entry.get("domain") or "").strip(), "")
        for entry in entries
        if str(entry.get("domain") or "").strip()
    ])
    return scopes[0] if len(scopes) == 1 else ""


def _monthly_report_primary_topic(topic: str, glossary_entries: list[dict[str, Any]]) -> str:
    tokens = [part for part in re.split(r"(\W+)", str(topic or "")) if part]
    context_only = {
        _normalize_alias_token(term)
        for entry in glossary_entries
        for term in (entry.get("context_only_terms") or [])
        if str(term or "").strip()
    }
    if not context_only:
        return str(topic or "").strip()
    kept: list[str] = []
    for token in tokens:
        if _normalize_alias_token(token) in context_only:
            continue
        kept.append(token)
    primary = " ".join("".join(kept).split()).strip()
    return primary or str(topic or "").strip()


def _monthly_report_context_only_terms_for_entries(entries: list[dict[str, Any]], topic: str) -> list[str]:
    lowered = str(topic or "").casefold()
    compact = _normalize_alias_token(lowered)
    terms: list[str] = []
    for entry in entries:
        for term in entry.get("context_only_terms") or []:
            clean = str(term or "").strip()
            if clean and _monthly_report_glossary_term_matches_text(lowered, compact, clean):
                terms.append(clean)
    return _dedupe_preserve_order(terms)


def _monthly_report_read_json_file(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _monthly_report_extract_domain_terms(payload: dict[str, Any]) -> dict[str, list[str]]:
    domains = payload.get("domains")
    if not isinstance(domains, dict):
        domains = {
            key: value
            for key, value in payload.items()
            if key in {"AF", "CRMS", "GRC"} and isinstance(value, dict)
        }
    results: dict[str, list[str]] = {}
    for domain, value in domains.items():
        if domain not in {"AF", "CRMS", "GRC"} or not isinstance(value, dict):
            continue
        terms: list[str] = []
        terms.extend(_monthly_report_collect_terms_from_value(value.get("aliases")))
        terms.extend(_monthly_report_collect_terms_from_value(value.get("terms")))
        terms.extend(_monthly_report_collect_terms_from_value(value.get("data_carriers")))
        terms.extend(_monthly_report_collect_terms_from_value(value.get("source_terms")))
        terms.extend(_monthly_report_collect_terms_from_value(value.get("api_terms")))
        terms.extend(_monthly_report_collect_terms_from_value(value.get("config_terms")))
        terms.extend(_monthly_report_collect_terms_from_value(value.get("logic_terms")))
        terms.extend(_monthly_report_collect_terms_from_value(value.get("field_population_terms")))
        for entry in value.get("entries") or []:
            if isinstance(entry, dict):
                terms.extend(_monthly_report_collect_terms_from_value(entry.get("business_aliases")))
                terms.extend(_monthly_report_collect_terms_from_value(entry.get("technical_terms")))
                terms.extend(_monthly_report_collect_terms_from_value(entry.get("product_terms")))
                terms.extend(_monthly_report_collect_terms_from_value(entry.get("limit_terms")))
        for module in value.get("module_map") or []:
            if isinstance(module, dict):
                terms.append(str(module.get("name") or ""))
                terms.extend(_monthly_report_collect_terms_from_value(module.get("aliases")))
                terms.extend(_monthly_report_collect_terms_from_value(module.get("code_hints")))
        for term in value.get("terminology") or []:
            if isinstance(term, dict):
                terms.append(str(term.get("term") or ""))
                terms.extend(_monthly_report_collect_terms_from_value(term.get("aliases")))
                terms.extend(_monthly_report_collect_terms_from_value(term.get("code_terms")))
        results[domain] = [
            term for term in _dedupe_preserve_order(str(item).strip() for item in terms)
            if _monthly_report_useful_glossary_term(term)
        ]
    return results


def _monthly_report_collect_terms_from_value(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        terms: list[str] = []
        for item in value:
            if isinstance(item, str):
                terms.append(item)
            elif isinstance(item, dict):
                terms.extend(_monthly_report_collect_terms_from_value(item.get("name")))
                terms.extend(_monthly_report_collect_terms_from_value(item.get("term")))
                terms.extend(_monthly_report_collect_terms_from_value(item.get("aliases")))
                terms.extend(_monthly_report_collect_terms_from_value(item.get("code_terms")))
        return terms
    return []


def _monthly_report_useful_glossary_term(value: str) -> bool:
    text = str(value or "").strip()
    compact = _normalize_alias_token(text)
    if not compact or compact.isdigit():
        return False
    if len(compact) <= 2:
        return False
    return True


def _monthly_report_highlight_product_area_scope(topic: Any) -> str:
    text = str(topic or "").strip().casefold()
    compact = _normalize_alias_token(text)
    if "anti-fraud" in text or "anti fraud" in text or "antifraud" in compact or "afasa" in compact or re.search(r"\baf\b", text):
        return "Anti-fraud"
    if "ops risk" in text or "operational risk" in text or "grc" in text:
        return "Ops Risk"
    if "credit risk" in text or "creditrisk" in compact:
        return "Credit Risk"
    return ""


def _monthly_report_highlight_qualifier_marker_groups(topic: Any) -> list[tuple[str, ...]]:
    return _monthly_report_highlight_qualifier_marker_groups_from_entries(
        topic,
        _monthly_report_glossary_entries_for_topic(topic),
    )


def _monthly_report_highlight_qualifier_marker_groups_from_entries(topic: Any, glossary_entries: list[dict[str, Any]]) -> list[tuple[str, ...]]:
    text = str(topic or "").strip().casefold()
    compact = _normalize_alias_token(text)
    anti_fraud_signal = (
        "anti-fraud" in text
        or "anti fraud" in text
        or "antifraud" in compact
        or "afasa" in compact
        or bool(re.search(r"\baf\b", text))
    )
    marker_groups: list[tuple[str, ...]] = []
    bank_anti_fraud_topic = anti_fraud_signal and bool(re.search(r"\bbank\b", text))
    if not bank_anti_fraud_topic:
        if re.search(r"\bph\b", text) or "philippine" in text or "philippines" in text:
            marker_groups.append(("ph", "philippine", "philippines", "maribank", "seabank ph"))
        if re.search(r"\bsg\b", text) or "singapore" in text:
            marker_groups.append(("sg", "singapore", "seabank sg"))
        if re.search(r"\bid\b", text) or "indonesia" in text:
            marker_groups.append(("id", "indonesia", "seabank id"))
    for entry in glossary_entries:
        for group in entry.get("qualifier_groups") or []:
            if isinstance(group, list):
                clean_group = tuple(str(marker or "").strip().casefold() for marker in group if str(marker or "").strip())
                if clean_group:
                    marker_groups.append(clean_group)
    if not any("mcc" in group or "credit card" in group for group in marker_groups):
        if "credit card" in text or "creditcard" in compact or re.search(r"\bmcc\b", text):
            marker_groups.append(("credit card", "creditcard", "mari credit card", "mcc", "mcc whitelisted", "sea group mcc"))
    if anti_fraud_signal and re.search(r"\bbank\b", text) and not any("bank" in group for group in marker_groups):
        marker_groups.append(("bank", "maribank", "seabank"))
    return _dedupe_marker_groups(marker_groups)


def _monthly_report_glossary_entries_for_topic(topic: Any) -> list[dict[str, Any]]:
    glossary = _monthly_report_business_glossary()
    entries = [entry for entry in (glossary.get("entries") or []) if isinstance(entry, dict)]
    lowered = str(topic or "").casefold()
    compact = _normalize_alias_token(lowered)
    matches: list[dict[str, Any]] = []
    for entry in entries:
        trigger_terms = entry.get("trigger_terms") or []
        aliases = [] if entry.get("context_only_terms") else (entry.get("aliases") or [])
        terms = [str(term or "") for term in [entry.get("canonical"), *trigger_terms, *aliases] if str(term or "").strip()]
        if any(_monthly_report_glossary_term_matches_text(lowered, compact, term) for term in terms):
            matches.append(entry)
    return matches


def _monthly_report_glossary_aliases_for_topic(topic: Any) -> set[str]:
    return _monthly_report_glossary_aliases_for_entries(_monthly_report_glossary_entries_for_topic(topic))


def _monthly_report_glossary_aliases_for_entries(entries: list[dict[str, Any]]) -> set[str]:
    aliases: set[str] = set()
    for entry in entries:
        if entry.get("context_only_terms"):
            continue
        for term in [entry.get("canonical"), *(entry.get("aliases") or [])]:
            clean = str(term or "").strip()
            if clean and clean.casefold() not in {"bank", "af"}:
                aliases.add(clean.casefold())
                aliases.add(_normalize_alias_token(clean))
    return {alias for alias in aliases if alias}


def _monthly_report_glossary_term_matches_text(lowered_text: str, compact_text: str, term: str) -> bool:
    clean = str(term or "").strip().casefold()
    if not clean:
        return False
    if _normalize_alias_token(clean) in {"af", "cr", "cc"}:
        return bool(re.search(rf"\b{re.escape(clean)}\b", lowered_text))
    if clean in {"mcc", "ph", "sg", "id"}:
        return bool(re.search(rf"\b{re.escape(clean)}\b", lowered_text))
    compact = _normalize_alias_token(clean)
    return clean in lowered_text or bool(len(compact) >= 4 and compact in compact_text)


def _dedupe_marker_groups(marker_groups: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    seen: set[tuple[str, ...]] = set()
    deduped: list[tuple[str, ...]] = []
    for group in marker_groups:
        clean_group = tuple(str(marker or "").strip().casefold() for marker in group if str(marker or "").strip())
        if not clean_group or clean_group in seen:
            continue
        seen.add(clean_group)
        deduped.append(clean_group)
    return deduped


def _monthly_report_text_matches_qualifier_marker_groups(text: str, marker_groups: list[tuple[str, ...]]) -> bool:
    if not marker_groups:
        return True
    lowered = str(text or "").casefold()
    compact = _normalize_alias_token(lowered)
    for group in marker_groups:
        if not any(_monthly_report_text_matches_qualifier_marker(lowered, compact, marker) for marker in group):
            return False
    return True


def _monthly_report_text_matches_qualifier_marker(lowered_text: str, compact_text: str, marker: str) -> bool:
    normalized_marker = str(marker or "").casefold().strip()
    if not normalized_marker:
        return False
    if normalized_marker in {"ph", "sg", "id"}:
        return bool(re.search(rf"\b{re.escape(normalized_marker)}\b", lowered_text))
    if normalized_marker == "mcc":
        if not re.search(r"\bmcc\b", lowered_text):
            return False
        context = _monthly_report_glossary_context_for_marker("mcc")
        if any(term in lowered_text for term in context.get("reject_context_any", [])):
            return False
        return any(term in lowered_text for term in context.get("requires_context_any", []))
    if normalized_marker == "bank":
        return bool(re.search(r"\bbank\b", lowered_text))
    if normalized_marker == "cc":
        return bool(re.search(r"\bcc\b", lowered_text))
    compact_marker = _normalize_alias_token(normalized_marker)
    return normalized_marker in lowered_text or bool(compact_marker and compact_marker in compact_text)


def _monthly_report_glossary_context_for_marker(marker: str) -> dict[str, list[str]]:
    clean_marker = str(marker or "").strip().casefold()
    requires: list[str] = []
    rejects: list[str] = []
    for entry in (_monthly_report_business_glossary().get("entries") or []):
        if not isinstance(entry, dict):
            continue
        terms = [
            str(term or "").casefold()
            for term in [
                entry.get("canonical"),
                *(entry.get("trigger_terms") or []),
                *(entry.get("aliases") or []),
            ]
            if str(term or "").strip()
        ]
        if clean_marker not in {_normalize_alias_token(term) for term in terms} and clean_marker not in terms:
            continue
        requires.extend(str(term or "").casefold() for term in (entry.get("requires_context_any") or []) if str(term or "").strip())
        rejects.extend(str(term or "").casefold() for term in (entry.get("reject_context_any") or []) if str(term or "").strip())
    if clean_marker == "mcc" and not requires:
        requires.extend(["mari", "maribank", "credit card", "card launch", "employee", "whitelist", "whitelisted", "live testing", "public launch", "sea group"])
        rejects.extend(["merchant category", "merchant-category"])
    return {
        "requires_context_any": _dedupe_preserve_order(requires),
        "reject_context_any": _dedupe_preserve_order(rejects),
    }


def _prefer_monthly_report_texts_by_qualifier_marker_groups(items: list[str], marker_groups: list[tuple[str, ...]]) -> list[str]:
    if not marker_groups:
        return items
    matched = _filter_monthly_report_texts_by_qualifier_marker_groups(items, marker_groups)
    return matched or items


def _filter_monthly_report_texts_by_qualifier_marker_groups(items: list[str], marker_groups: list[tuple[str, ...]]) -> list[str]:
    if not marker_groups:
        return items
    return [
        item
        for item in items
        if _monthly_report_text_matches_qualifier_marker_groups(item, marker_groups)
    ]


def _prefer_monthly_report_sheet_evidence_by_qualifier_marker_groups(
    items: list[dict[str, str]],
    marker_groups: list[tuple[str, ...]],
) -> list[dict[str, str]]:
    if not marker_groups:
        return items
    matched = [
        item
        for item in items
        if _monthly_report_text_matches_qualifier_marker_groups(
            " ".join(str(item.get(key) or "") for key in ("title", "text", "access_status")),
            marker_groups,
        )
    ]
    return matched or items


def _filter_monthly_report_texts_by_product_area_scope(items: list[str], product_area_scope: str) -> list[str]:
    return [
        item
        for item in items
        if _monthly_report_text_allowed_by_product_area_scope(item, product_area_scope)
    ]


def _monthly_report_text_allowed_by_product_area_scope(text: str, product_area_scope: str) -> bool:
    scope = str(product_area_scope or "").strip()
    if not scope:
        return True
    lowered = str(text or "").casefold()
    if not lowered.strip():
        return False
    markers = {
        "Credit Risk": ("credit risk", "credit-risk", "underwriting", "retail limit", "cash loan", "credit card", "ccic", "sme rcf", "experian"),
        "Anti-fraud": ("anti-fraud", "anti fraud", "antifraud", "afasa", "fraud", "scam", "risk identification"),
        "Ops Risk": ("ops risk", "operational risk", "grc", "rcsa", "outsourcing"),
    }
    primary_markers = {
        "Credit Risk": ("credit risk", "credit-risk"),
        "Anti-fraud": ("anti-fraud", "anti fraud", "antifraud"),
        "Ops Risk": ("ops risk", "operational risk", "grc"),
    }
    scope_markers = markers.get(scope, ())
    if any(marker in lowered for marker in primary_markers.get(scope, ())):
        return True
    other_primary_markers = [
        marker
        for area, values in primary_markers.items()
        if area != scope
        for marker in values
    ]
    if any(marker in lowered for marker in other_primary_markers):
        return False
    other_markers = [
        marker
        for area, values in markers.items()
        if area != scope
        for marker in values
    ]
    if any(marker in lowered for marker in scope_markers):
        return True
    if any(marker in lowered for marker in other_markers):
        return False
    return True


def _project_aliases(project: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()

    def add(value: Any) -> None:
        text = str(value or "").strip()
        if len(text) >= 3:
            aliases.add(text.casefold())
            aliases.add(_normalize_alias_token(text))
            for part in re.split(r"[\s/_:()[\],.-]+", text):
                token = part.strip().casefold()
                if _is_useful_alias_token(token):
                    aliases.add(token)

    add(project.get("bpmis_id"))
    add(project.get("project_name"))
    for ticket in project.get("jira_tickets") or []:
        if not isinstance(ticket, dict):
            continue
        add(ticket.get("jira_id") or ticket.get("issue_id"))
        add(ticket.get("jira_title"))
    combined_text = " ".join(str(value or "") for value in aliases).casefold()
    if "google" in combined_text and "pay" in combined_text:
        aliases.update({"google pay", "card on google pay", "maribank card on google pay"})
    expanded: set[str] = set()
    for alias in aliases:
        expanded.add(alias)
        if "alcv12" in alias.replace(" ", ""):
            expanded.update({"alc v12", "alcv12", "alc"})
    return {item for item in expanded if item}


def _monthly_report_project_match_text(project: dict[str, Any]) -> str:
    return " ".join(
        [
            str(project.get("bpmis_id") or ""),
            str(project.get("project_name") or ""),
            str(project.get("market") or ""),
            str(project.get("priority") or ""),
            str(project.get("regional_pm_pic") or ""),
            " ".join(str(item or "") for item in (project.get("teams") or [])),
            *[
                " ".join(
                    [
                        str(ticket.get("jira_id") or ticket.get("issue_id") or ""),
                        str(ticket.get("jira_title") or ""),
                        str(ticket.get("description") or ""),
                    ]
                )
                for ticket in (project.get("jira_tickets") or [])
                if isinstance(ticket, dict)
            ],
        ]
    )


def _monthly_report_highlight_topic_intent(topic: Any) -> str:
    text = _monthly_report_topic_intent_text(topic)
    compact = _normalize_alias_token(text)
    if any(term in text for term in (
        "go live",
        "went live",
        "post live",
        "post-live",
        "post launch",
        "post-launch",
        "public launch",
        "live testing",
        "live test",
        "employee live",
        "employee lv",
    )) or "golive" in compact:
        return "go_live_outcome"
    if any(term in text for term in ("employee rollout", "pilot rollout", "rollout result", "launch result", "user feedback", "feedback after")):
        return "go_live_outcome"
    if any(term in text for term in ("issue", "incident", "capacity", "downgrade", "follow up", "follow-up", "risk", "blocker", "problem")):
        return "issue_followup"
    if any(term in text for term in ("release readiness", "launch readiness", "release window", "uat readiness", "sit readiness")):
        return "release_readiness"
    if any(term in text for term in ("decision", "confirm", "alignment", "approval", "sign off", "sign-off")):
        return "decision_needed"
    return "general_progress"


def _monthly_report_topic_intent_text(topic: Any) -> str:
    text = str(topic or "").strip().casefold()
    text = re.sub(r"\bcredit\s+risk\b", " ", text)
    text = re.sub(r"\bops\s+risk\b|\boperational\s+risk\b", " ", text)
    text = re.sub(r"\banti[-\s]?fraud\b", " ", text)
    return " ".join(text.split())


def _monthly_report_highlight_intent_focus(intent: str) -> dict[str, Any]:
    normalized = str(intent or "general_progress").strip() or "general_progress"
    if normalized == "go_live_outcome":
        return {
            "label": "Go-live outcome and feedback",
            "evidence_priority": [
                "confirmed go-live date or rollout status",
                "employee or pilot user feedback",
                "post-live production issues or stability",
                "usage, adoption, support, or blocker signals",
                "next rollout or remediation action",
            ],
            "tone_suffix": "For go-live topics, do not substitute generic development or PRD progress for post-go-live outcome or feedback; if outcome evidence is missing, say the launch outcome remains pending confirmation.",
        }
    if normalized == "issue_followup":
        return {
            "label": "Issue follow-up and mitigation",
            "evidence_priority": [
                "impact and affected flow",
                "containment or mitigation completed",
                "remaining risk or decision",
                "owner and next corrective action",
            ],
            "tone_suffix": "For issue topics, focus on impact, containment, residual risk, and next action instead of broad project progress.",
        }
    if normalized == "release_readiness":
        return {
            "label": "Release readiness",
            "evidence_priority": [
                "SIT/UAT or release readiness status",
                "release dependency or blocker",
                "target launch window",
                "next readiness checkpoint",
            ],
            "tone_suffix": "For readiness topics, focus on whether the release is ready, blocked, or pending confirmation.",
        }
    if normalized == "decision_needed":
        return {
            "label": "Decision needed",
            "evidence_priority": [
                "decision required",
                "options or tradeoff",
                "business impact",
                "next owner or forum",
            ],
            "tone_suffix": "For decision topics, emphasize the management decision and consequence, not general delivery activity.",
        }
    return {
        "label": "General progress",
        "evidence_priority": [
            "delivery progress",
            "business impact",
            "risk or decision",
            "next movement",
        ],
        "tone_suffix": "",
    }


def _monthly_report_issue_followup_facts(evidence_texts: list[str]) -> dict[str, list[str]]:
    facts: dict[str, list[str]] = {
        "impact": [],
        "root_cause": [],
        "short_term_solution": [],
        "long_term_solution": [],
        "next_action": [],
    }
    category_terms = {
        "impact": (
            "impact",
            "affected",
            "login",
            "transactional",
            "failure",
            "failed",
            "customers",
            "applications",
            "影响",
            "用户",
            "客户",
            "失败",
            "登录",
        ),
        "root_cause": (
            "root cause",
            "caused by",
            "qps",
            "database",
            "db ",
            "overload",
            "overloaded",
            "running threads",
            "capacity",
            "traffic",
            "原因",
            "根因",
            "容量",
            "高并发",
            "读写",
            "过载",
            "线程",
        ),
        "short_term_solution": (
            "resolution",
            "short-term",
            "short term",
            "stabilize",
            "scaled down",
            "qps limit",
            "restored",
            "hotfix",
            "disabled",
            "expanded",
            "done",
            "mitigation",
            "缓解",
            "修复",
            "短期",
            "临时",
            "限流",
            "热修",
            "关闭",
            "扩容",
            "完成",
        ),
        "long_term_solution": (
            "long-term",
            "long term",
            "codis",
            "cache",
            "es index",
            "indexing",
            "migrated",
            "reduce db load",
            "sop",
            "dismantled",
            "q4",
            "长期",
            "方案",
            "迁移",
            "索引",
            "告警",
            "拆分",
        ),
        "next_action": (
            "eta",
            "30 june",
            "30 jun",
            "21 may",
            "q4",
            "next",
            "follow up",
            "follow-up",
            "owner",
            "confirm",
            "待",
            "下一步",
            "计划",
            "负责人",
            "确认",
        ),
    }
    for raw_text in evidence_texts:
        for line in str(raw_text or "").splitlines():
            clean = line.strip()
            if len(clean) < 8:
                continue
            lowered = clean.casefold()
            for category, terms in category_terms.items():
                if any(term in lowered for term in terms):
                    facts[category].append(clean[:600])
    return {category: _dedupe_preserve_order(values)[:6] for category, values in facts.items() if values}


def _monthly_report_intent_signal_count(intent: str, evidence_texts: list[str]) -> int:
    normalized = str(intent or "general_progress").strip()
    if normalized == "general_progress":
        return 1
    text = "\n".join(str(item or "") for item in evidence_texts if str(item or "").strip()).casefold()
    compact = _normalize_alias_token(text)
    if not text:
        return 0
    terms_by_intent = {
        "go_live_outcome": [
            "go live",
            "went live",
            "post live",
            "post-live",
            "post launch",
            "post-launch",
            "employee",
            "pilot",
            "feedback",
            "production",
            "live issue",
            "stability",
            "adoption",
            "usage",
            "rollout",
            "launched",
            "launch outcome",
        ],
        "issue_followup": [
            "issue",
            "incident",
            "capacity",
            "downgrade",
            "mitigation",
            "root cause",
            "impact",
            "blocked",
            "blocker",
            "follow up",
            "follow-up",
        ],
        "release_readiness": [
            "readiness",
            "sit",
            "uat",
            "release",
            "launch",
            "dependency",
            "blocker",
            "checkpoint",
        ],
        "decision_needed": [
            "decision",
            "confirm",
            "alignment",
            "approval",
            "sign off",
            "sign-off",
            "option",
            "tradeoff",
        ],
    }
    count = 0
    for term in terms_by_intent.get(normalized, []):
        clean = term.casefold()
        if _monthly_report_intent_term_matches(text, compact, clean):
            count += 1
    return count


def _monthly_report_intent_term_matches(text: str, compact: str, term: str) -> bool:
    clean = str(term or "").casefold().strip()
    if not clean:
        return False
    if clean in {"prod"}:
        return bool(re.search(rf"\b{re.escape(clean)}\b", text))
    return clean in text or _normalize_alias_token(clean) in compact


def _highlight_topic_aliases(topic: Any) -> set[str]:
    aliases = _highlight_topic_aliases_without_query_plan(topic)
    aliases.update(_monthly_report_glossary_aliases_for_topic(topic))
    return {alias for alias in aliases if alias}


def _highlight_topic_aliases_without_query_plan(topic: Any) -> set[str]:
    aliases: set[str] = set()
    text = str(topic or "").strip()
    if len(text) >= 3:
        aliases.add(text.casefold())
        aliases.add(_normalize_alias_token(text))
    tokens: list[str] = []
    for part in re.split(r"[\s/_:()[\],.-]+", text):
        token = part.strip().casefold()
        if token:
            tokens.append(token)
        if _is_useful_alias_token(token):
            aliases.add(token)
    aliases.update(_highlight_topic_phrase_aliases(tokens))
    return {alias for alias in aliases if alias}


def _highlight_topic_phrase_aliases(tokens: list[str]) -> set[str]:
    aliases: set[str] = set()
    for size in (3, 2):
        for index in range(0, max(0, len(tokens) - size + 1)):
            phrase_tokens = [token for token in tokens[index : index + size] if token]
            if len(phrase_tokens) != size:
                continue
            has_descriptor_anchor = any(token in {"workflow", "flow", "process"} for token in phrase_tokens)
            normalized_phrase = _normalize_alias_token(" ".join(phrase_tokens))
            known_product_phrase = normalized_phrase in {
                "creditcard",
                "cashloan",
                "termloan",
                "standalonecashloan",
                "retaillimit",
                "limitassignment",
            }
            if not any(_is_useful_alias_token(token) for token in phrase_tokens) and not has_descriptor_anchor:
                if not known_product_phrase:
                    continue
            if all(_is_monthly_report_scope_token(token) for token in phrase_tokens):  # pragma: no cover
                continue
            if all(_is_monthly_report_generic_descriptor_token(token) for token in phrase_tokens):
                if not has_descriptor_anchor:  # pragma: no cover
                    continue
                phrase = " ".join(phrase_tokens)
                if len(_normalize_alias_token(phrase)) >= 10:
                    aliases.add(phrase)
                    aliases.add(_normalize_alias_token(phrase))
                continue
            phrase = " ".join(phrase_tokens)
            if len(_normalize_alias_token(phrase)) >= 8:
                aliases.add(phrase)
                aliases.add(_normalize_alias_token(phrase))
    return aliases


def _highlight_seatalk_aliases(
    highlight_topics: list[str],
    *,
    key_projects: list[dict[str, Any]],
    topic_project_matches: list[dict[str, Any]],
) -> set[str]:
    projects_by_id = {
        str(project.get("bpmis_id") or "").strip(): project
        for project in key_projects
        if str(project.get("bpmis_id") or "").strip()
    }
    aliases: set[str] = set()
    for topic in highlight_topics:
        aliases.update(_highlight_topic_aliases(topic))
    for match in topic_project_matches:
        for project_id in match.get("project_ids") or []:
            project = projects_by_id.get(str(project_id or "").strip())
            if project:
                aliases.update(_project_aliases(project))
    return {
        alias
        for alias in aliases
        if _is_useful_seatalk_highlight_alias(alias)
    }


def _is_useful_seatalk_highlight_alias(alias: str) -> bool:
    text = str(alias or "").strip().casefold()
    if not text:
        return False
    compact = _normalize_alias_token(text)
    if not compact or compact.isdigit():
        return False
    if _is_monthly_report_scope_token(text) or _is_monthly_report_generic_descriptor_token(text):
        return False
    if any(character.isdigit() for character in compact):
        return True
    if len(compact) < 8:
        return False
    if compact in {
        "business",
        "delivery",
        "followup",
        "progress",
        "project",
        "release",
        "actions",
        "impact",
        "issues",
        "recent",
        "status",
        "update",
    }:
        return False
    return True


def _highlight_topic_matches_project(topic_aliases: set[str], project: dict[str, Any]) -> bool:
    if not topic_aliases:
        return False
    project_text = " ".join(
        [
            str(project.get("bpmis_id") or ""),
            str(project.get("project_name") or ""),
            str(project.get("market") or ""),
            str(project.get("priority") or ""),
            *[
                " ".join(
                    [
                        str(ticket.get("jira_id") or ticket.get("issue_id") or ""),
                        str(ticket.get("jira_title") or ""),
                    ]
                )
                for ticket in (project.get("jira_tickets") or [])
                if isinstance(ticket, dict)
            ],
        ]
    )
    if _text_matches_aliases(project_text, topic_aliases):
        return True
    topic_text = " ".join(sorted(topic_aliases))
    return _text_matches_aliases(topic_text, _project_aliases(project))


def _is_useful_alias_token(token: str) -> bool:
    if not token or token.isdigit():
        return False
    if _is_monthly_report_scope_token(token) or _is_monthly_report_generic_descriptor_token(token):
        return False
    if token in {
        "feature",
        "monthly",
        "model",
        "project",
        "productization",
        "report",
        "phase",
        "status",
        "strategy",
        "support",
        "system",
        "transaction",
        "transactions",
        "update",
        "upgrade",
    }:
        return False
    return any(character.isdigit() for character in token) or len(token) >= 6


def _is_monthly_report_scope_token(token: str) -> bool:
    text = str(token or "").strip().casefold()
    return text in {
        "sg",
        "id",
        "ph",
        "regional",
        "credit",
        "risk",
        "cr",
        "crms",
        "anti",
        "fraud",
        "af",
        "afasa",
        "ops",
        "grc",
        "bank",
    }


def _is_monthly_report_generic_descriptor_token(token: str) -> bool:
    text = str(token or "").strip().casefold()
    return text in {
        "a",
        "an",
        "the",
        "more",
        "less",
        "new",
        "old",
        "better",
        "flexible",
        "workflow",
        "flow",
        "process",
        "support",
        "change",
        "changes",
        "enhance",
        "enhancement",
        "improve",
        "improvement",
        "optimize",
        "optimization",
    }


def _normalize_alias_token(value: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value or "").casefold())


def _text_matches_aliases(text: str, aliases: set[str]) -> bool:
    lowered = str(text or "").casefold()
    compact = _normalize_alias_token(lowered)
    for alias in aliases:
        clean = _normalize_alias_token(alias)
        if alias and alias in lowered:
            return True
        if clean and clean in compact:
            return True
    return False


def _matched_lines_for_project(text: str, aliases: set[str], *, limit: int) -> list[str]:
    matches: list[str] = []
    for line in str(text or "").splitlines():
        clean = line.strip()
        if not clean or len(clean) < 8:
            continue
        if _text_matches_aliases(clean, aliases):
            matches.append(clean[:800])
        if len(matches) >= limit:
            break
    return matches


def _matched_conversation_context_lines_for_project(text: str, aliases: set[str], *, limit: int) -> list[str]:
    raw_lines = str(text or "").splitlines()
    if not raw_lines:
        return []
    if not any(line.strip().startswith("===") for line in raw_lines):
        return _matched_lines_for_project(text, aliases, limit=limit)
    groups = _monthly_report_seatalk_conversation_groups(raw_lines)
    matches: list[str] = []
    for _header, start, end in groups:
        conversation_lines = raw_lines[start:end]
        if not any(_text_matches_aliases(line.strip(), aliases) for line in conversation_lines if line.strip()):
            continue
        clean_header = str(_header or "").strip()
        if clean_header:
            matches.append(clean_header[:800])
            if len(matches) >= limit:
                return matches
        for line in conversation_lines:
            clean = line.strip()
            if not clean or len(clean) < 8 or clean.startswith("==="):
                continue
            if _is_monthly_report_context_noise_line(clean):
                continue
            matches.append(clean[:800])
            if len(matches) >= limit:
                return matches
    return matches


def _matched_qualified_conversation_context_lines_for_project(
    text: str,
    aliases: set[str],
    *,
    qualifier_marker_groups: list[tuple[str, ...]],
    topic_intent: str,
    topic: Any,
    limit: int,
    context_lines: int,
) -> list[str]:
    raw_lines = str(text or "").splitlines()
    if not raw_lines or not qualifier_marker_groups:
        return []
    if not any(line.strip().startswith("===") for line in raw_lines):
        matched = _matched_context_lines_for_project(text, aliases, limit=limit, context_lines=context_lines)
        return _filter_monthly_report_texts_by_qualifier_marker_groups(matched, qualifier_marker_groups)

    requested_markets = _monthly_report_requested_markets(topic)
    product_marker_groups = _monthly_report_product_qualifier_marker_groups(qualifier_marker_groups)
    group_candidates: list[tuple[int, int, list[str]]] = []
    for header, start, end in _monthly_report_seatalk_conversation_groups(raw_lines):
        conversation_lines = [line.strip() for line in raw_lines[start:end] if line.strip() and not line.strip().startswith("===")]
        if not conversation_lines:
            continue
        conversation_text = "\n".join([header, *conversation_lines])
        strict_qualifier_match = any(
            _monthly_report_text_matches_qualifier_marker_groups(line, qualifier_marker_groups)
            for line in conversation_lines
        )
        product_context_match = bool(product_marker_groups) and all(
            _monthly_report_text_matches_qualifier_marker_groups(conversation_text, [group])
            for group in product_marker_groups
        )
        intent_context_match = _monthly_report_intent_signal_count(topic_intent, [conversation_text]) > 0
        alias_context_match = (
            _text_matches_aliases(str(header or "").strip(), aliases)
            or product_context_match
            or any(_text_matches_aliases(line, aliases) for line in conversation_lines)
        )
        conflicting_market = _monthly_report_text_has_conflicting_market(conversation_text, requested_markets)
        anchor_indexes = _monthly_report_qualified_conversation_anchor_indexes(
            raw_lines,
            start=start,
            end=end,
            aliases=aliases,
            qualifier_marker_groups=qualifier_marker_groups,
            product_marker_groups=product_marker_groups,
            topic_intent=topic_intent,
            window_lines=3,
        )
        if not anchor_indexes and product_context_match:
            anchor_indexes = {
                index
                for index in range(start, end)
                if _monthly_report_intent_signal_count(topic_intent, [raw_lines[index].strip()]) > 0
            }
        if topic_intent != "general_progress" and not anchor_indexes:
            continue
        if not strict_qualifier_match and not (
            product_context_match
            and intent_context_match
            and alias_context_match
            and not conflicting_market
        ):
            continue

        selected_indexes: set[int] = set()
        for index in sorted(anchor_indexes):
            selected_start = max(start, index - max(0, int(context_lines or 0)))
            selected_end = min(end, index + max(0, int(context_lines or 0)) + 1)
            selected_indexes.update(range(selected_start, selected_end))

        group_lines: list[str] = []
        clean_header = str(header or "").strip()
        if clean_header:
            group_lines.append(clean_header[:800])
        per_group_limit = max(4, min(6, limit // 3 if limit >= 3 else limit))
        for index in sorted(selected_indexes):
            clean = raw_lines[index].strip()
            if not clean or clean.startswith("===") or _is_monthly_report_context_noise_line(clean):
                continue
            if _monthly_report_text_has_conflicting_market(clean, requested_markets) and not _monthly_report_text_matches_qualifier_marker_groups(clean, qualifier_marker_groups):
                continue
            group_lines.append(clean[:800])
            if len(group_lines) >= per_group_limit:
                break
        if group_lines:
            group_candidates.append((_monthly_report_qualified_conversation_score(conversation_text, group_lines, strict_qualifier_match), start, group_lines))
    matches: list[str] = []
    for _score, _start, group_lines in sorted(group_candidates, key=lambda item: (-item[0], item[1])):
        for line in group_lines:
            matches.append(line)
            if len(matches) >= limit:
                return matches
    return matches


def _monthly_report_qualified_conversation_score(conversation_text: str, selected_lines: list[str], strict_qualifier_match: bool) -> int:
    text = "\n".join([conversation_text, *selected_lines]).casefold()
    score = 20 if strict_qualifier_match else 0
    for term, weight in (
        ("sea group employee live testing", 30),
        ("employee live testing", 28),
        ("2nd live testing", 24),
        ("public launch", 24),
        ("whitelist 50k", 24),
        ("employee whitelist", 18),
        ("mari credit card", 42),
        ("mcc whitelisted", 120),
        ("sea group mcc", 120),
        ("sea employee lv", 18),
        ("epfs", 16),
        ("credit card launch", 12),
    ):
        if term in text:
            score += weight
    return score


def _monthly_report_product_qualifier_marker_groups(marker_groups: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    market_markers = {"ph", "philippine", "philippines", "maribank", "seabank ph", "sg", "singapore", "seabank sg", "id", "indonesia", "seabank id"}
    product_groups: list[tuple[str, ...]] = []
    for group in marker_groups:
        clean_group = tuple(str(marker or "").strip().casefold() for marker in group if str(marker or "").strip())
        if clean_group and not all(marker in market_markers for marker in clean_group):
            product_groups.append(clean_group)
    return product_groups


def _monthly_report_qualified_conversation_anchor_indexes(
    raw_lines: list[str],
    *,
    start: int,
    end: int,
    aliases: set[str],
    qualifier_marker_groups: list[tuple[str, ...]],
    product_marker_groups: list[tuple[str, ...]],
    topic_intent: str,
    window_lines: int,
) -> set[int]:
    line_features: dict[int, tuple[bool, bool, bool]] = {}
    for index in range(start, end):
        clean = raw_lines[index].strip()
        if not clean or clean.startswith("===") or _is_monthly_report_context_noise_line(clean):
            continue
        qualifier_hit = _monthly_report_text_matches_qualifier_marker_groups(clean, qualifier_marker_groups)
        product_hit = (
            _monthly_report_text_matches_any_qualifier_marker_group(clean, product_marker_groups)
            if product_marker_groups
            else qualifier_hit
        )
        alias_hit = _text_matches_aliases(clean, aliases)
        intent_hit = topic_intent == "general_progress" or _monthly_report_intent_signal_count(topic_intent, [clean]) > 0
        line_features[index] = (qualifier_hit or product_hit, alias_hit, intent_hit)

    anchors: set[int] = set()
    window = max(0, int(window_lines or 0))
    for index, (product_hit, alias_hit, intent_hit) in line_features.items():
        topic_hit = product_hit or alias_hit
        if topic_hit and intent_hit:
            anchors.add(index)
            continue
        if not topic_hit:
            continue
        nearby_intent_indexes = [
            other_index
            for other_index, (_other_product, _other_alias, other_intent) in line_features.items()
            if other_intent and abs(other_index - index) <= window
        ]
        if nearby_intent_indexes:
            anchors.add(index)
            anchors.update(nearby_intent_indexes)
    return anchors


def _monthly_report_requested_markets(topic: Any) -> set[str]:
    text = str(topic or "").casefold()
    markets: set[str] = set()
    if re.search(r"\bph\b", text) or "philippine" in text or "philippines" in text:
        markets.add("ph")
    if re.search(r"\bsg\b", text) or "singapore" in text:
        markets.add("sg")
    if re.search(r"\bid\b", text) or "indonesia" in text:
        markets.add("id")
    return markets


def _monthly_report_text_has_conflicting_market(text: str, requested_markets: set[str]) -> bool:
    if not requested_markets:
        return False
    lowered = str(text or "").casefold()
    market_terms = {
        "ph": (r"\bph\b", "philippine", "philippines", "maribank", "seabank ph"),
        "sg": (r"\bsg\b", "singapore", "seabank sg"),
        "id": (r"\bid\b", "indonesia", "seabank id"),
    }
    present: set[str] = set()
    for market, terms in market_terms.items():
        for term in terms:
            if term.startswith(r"\b"):
                if re.search(term, lowered):
                    present.add(market)
                    break
            elif term in lowered:
                present.add(market)
                break
    return bool(present - requested_markets)


def _monthly_report_text_matches_any_qualifier_marker_group(text: str, marker_groups: list[tuple[str, ...]]) -> bool:
    return any(_monthly_report_text_matches_qualifier_marker_groups(text, [group]) for group in marker_groups)


def _monthly_report_text_has_unrelated_product_signal(text: str, topic: Any) -> bool:
    lowered = str(text or "").casefold()
    topic_text = str(topic or "").casefold()
    unrelated_terms = []
    if "grc" not in topic_text and "ops risk" not in topic_text and "operational risk" not in topic_text:
        unrelated_terms.extend(["grc", "operational risk", "ops risk", "rcsa"])
    if "anti-fraud" not in topic_text and "anti fraud" not in topic_text and "afasa" not in _normalize_alias_token(topic_text):
        unrelated_terms.extend(["anti-fraud", "anti fraud", "afasa", "scam rule"])
    return any(term in lowered for term in unrelated_terms)


def _matched_forward_context_lines_for_project(text: str, aliases: set[str], *, limit: int, context_lines: int) -> list[str]:
    raw_lines = str(text or "").splitlines()
    selected_indexes: set[int] = set()
    has_conversation_markers = any(line.strip().startswith("===") for line in raw_lines)
    within_conversation = not has_conversation_markers
    for index, line in enumerate(raw_lines):
        clean = line.strip()
        if clean.startswith("==="):
            within_conversation = True
        if not within_conversation:
            continue
        if not clean or len(clean) < 8:
            continue
        if not _text_matches_aliases(clean, aliases):
            continue
        selected_indexes.add(index)
        follow_count = 0
        for next_index in range(index + 1, len(raw_lines)):
            if follow_count >= max(0, int(context_lines or 0)):
                break
            next_clean = raw_lines[next_index].strip()
            if next_clean.startswith("==="):
                break
            if not next_clean or len(next_clean) < 8:
                continue
            if _is_monthly_report_context_noise_line(next_clean):
                continue
            selected_indexes.add(next_index)
            follow_count += 1
    matches: list[str] = []
    for index in sorted(selected_indexes):
        clean = raw_lines[index].strip()
        if not clean or len(clean) < 8 or clean.startswith("==="):
            continue
        if _is_monthly_report_context_noise_line(clean):
            continue
        matches.append(clean[:800])
        if len(matches) >= limit:
            break
    return matches


def _is_monthly_report_context_noise_line(line: str) -> bool:
    text = str(line or "").strip().casefold()
    return any(
        marker in text
        for marker in (
            "github bot:",
            "github actions",
            "build cloud run image workflow run",
            "workflow run failed",
        )
    )


def _matched_context_lines_for_project(text: str, aliases: set[str], *, limit: int, context_lines: int) -> list[str]:
    raw_lines = str(text or "").splitlines()
    selected_indexes: set[int] = set()
    has_conversation_markers = any(line.strip().startswith("===") for line in raw_lines)
    within_conversation = not has_conversation_markers
    for index, line in enumerate(raw_lines):
        clean = line.strip()
        if clean.startswith("==="):
            within_conversation = True
        if not within_conversation:
            continue
        if not clean or len(clean) < 8:
            continue
        if not _text_matches_aliases(clean, aliases):
            continue
        start = max(0, index - max(0, int(context_lines or 0)))
        end = min(len(raw_lines), index + max(0, int(context_lines or 0)) + 1)
        selected_indexes.update(range(start, end))
    matches: list[str] = []
    for index in sorted(selected_indexes):
        clean = raw_lines[index].strip()
        if not clean or len(clean) < 8:
            continue
        matches.append(clean[:800])
        if len(matches) >= limit:
            break
    return matches


def _monthly_report_issue_followup_aliases(topic: Any, aliases: set[str]) -> set[str]:
    focused: set[str] = set()
    ignored_tokens = {
        "id",
        "sg",
        "ph",
        "issue",
        "issues",
        "recent",
        "system",
        "impact",
        "follow",
        "up",
        "actions",
        "action",
        "risk",
    }
    strong_terms = {
        "database",
        "capacity",
        "downgrade",
        "qps",
        "db",
        "root cause",
        "resolution",
        "mitigation",
        "dbp-antifraud",
        "risk database",
        "codis",
        "es index",
        "数据库",
        "容量",
        "压力",
        "降级",
        "根因",
        "原因",
        "解决方案",
        "处理方案",
        "缓解",
        "修复",
        "限流",
        "告警",
        "迁移",
    }
    equivalent_terms = {
        "database": ("数据库",),
        "risk database": ("风险库", "风控库", "数据库"),
        "capacity": ("容量", "压力"),
        "downgrade": ("降级",),
        "qps": ("qps", "限流"),
        "db": ("db", "数据库"),
        "root cause": ("根因", "原因"),
        "resolution": ("解决方案", "处理方案"),
        "mitigation": ("缓解", "修复"),
        "codis": ("codis", "缓存"),
        "es index": ("es index", "索引"),
    }
    topic_text = str(topic or "").casefold()
    for term in strong_terms:
        if term in topic_text or _normalize_alias_token(term) in _normalize_alias_token(topic_text):
            focused.add(term)
            focused.add(_normalize_alias_token(term))
            for equivalent in equivalent_terms.get(term, ()):
                focused.add(equivalent)
                focused.add(_normalize_alias_token(equivalent))
    for alias in aliases:
        clean = _normalize_alias_token(alias)
        lowered = str(alias or "").casefold().strip()
        if not lowered or clean in ignored_tokens or lowered in ignored_tokens:
            continue
        if len(clean) <= 3 and clean not in {"qps", "db", "dbp"}:
            continue
        if lowered in strong_terms or clean in {_normalize_alias_token(term) for term in strong_terms}:
            focused.add(lowered)
            focused.add(clean)
    return focused or {alias for alias in aliases if len(_normalize_alias_token(alias)) > 3}


def _matched_sections_for_project(text: str, aliases: set[str], *, limit: int) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    separator = "=" * 80
    sections = raw.split(separator) if separator in raw else raw.split("\n\n")
    matches: list[str] = []
    for section in sections:
        clean = section.strip()
        if not clean or len(clean) < 8:
            continue
        if _text_matches_aliases(clean, aliases):
            matches.append(clean[:1_000])
        if len(matches) >= limit:
            break
    return matches


def _index_prd_summaries_by_jira(prd_scope_summaries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = {}
    for summary in prd_scope_summaries:
        if not isinstance(summary, dict):
            continue
        jira_id = str(summary.get("jira_id") or "").strip()
        if jira_id:
            indexed.setdefault(jira_id, []).append(summary)
    return indexed


def _matched_prd_summaries_for_project(jira_tickets: list[dict[str, Any]], prd_by_jira: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ticket in jira_tickets:
        jira_id = str(ticket.get("jira_id") or "").strip()
        for summary in prd_by_jira.get(jira_id, []):
            key = f"{summary.get('jira_id')}/{summary.get('url')}"
            if key in seen:
                continue
            seen.add(key)
            matched.append(summary)
    return matched


def _jira_evidence_facts(jira_tickets: list[dict[str, Any]]) -> tuple[list[str], list[str], list[dict[str, str]], int]:
    status_facts: list[str] = []
    timeline_facts: list[str] = []
    sources: list[dict[str, str]] = []
    score = 0
    for ticket in jira_tickets:
        jira_id = str(ticket.get("jira_id") or "").strip()
        title = str(ticket.get("jira_title") or "").strip()
        status = str(ticket.get("jira_status") or "").strip()
        release_date = str(ticket.get("release_date") or "").strip()
        version = str(ticket.get("version") or "").strip()
        has_material = bool(status or release_date or version or ticket.get("prd_links"))
        if not has_material:
            continue
        score += 1
        if jira_id or title or status:
            status_facts.append(" ".join(item for item in [jira_id, title, f"is {status}" if status else ""] if item).strip())
        if not _is_monthly_report_planning_version(version) and (release_date or version):
            timeline_facts.append(" ".join(item for item in [jira_id, f"release {release_date}" if release_date else "", f"version {version}" if version else ""] if item).strip())
        sources.append(
            {
                "jira_id": jira_id,
                "title": title[:240],
                "status": status,
                "release_date": release_date,
                "version": version,
            }
        )
    return status_facts, timeline_facts, sources, score


def _monthly_report_target_tech_live_date(
    jira_tickets: list[dict[str, Any]],
    *,
    project: dict[str, Any] | None = None,
    monthly_requirements_targets: list[dict[str, Any]] | None = None,
    fallback_reference_date: date | None = None,
) -> tuple[str, str, str, dict[str, Any]]:
    email_target = _monthly_requirements_target_tech_live_date(project or {}, monthly_requirements_targets or [])
    if email_target:
        return email_target[0], email_target[1], "monthly_requirements_email", email_target[2]
    latest: tuple[date, str] | None = None
    for ticket in jira_tickets:
        version = str(
            ticket.get("version")
            or ticket.get("fix_version_name")
            or ticket.get("fixVersion")
            or ticket.get("fix_version")
            or ""
        ).strip()
        if _is_monthly_report_planning_version(version):
            continue
        release_date = str(ticket.get("release_date") or "").strip()
        if not release_date:
            continue
        try:
            parsed = date.fromisoformat(release_date[:10])
        except ValueError:
            continue
        if latest is None or parsed > latest[0]:
            latest = (parsed, version)
    if latest is None:
        reference = fallback_reference_date or date.today()
        return _monthly_report_next_quarter_label(reference), "", "next_quarter_fallback", {"reference_date": reference.isoformat()}
    return _monthly_report_month_label(latest[0]), latest[1], "jira_version", {"version": latest[1]}


def _monthly_report_next_quarter_label(reference: date) -> str:
    month = int(reference.month)
    quarter = ((month - 1) // 3) + 1
    next_quarter = quarter + 1
    year = int(reference.year)
    if next_quarter > 4:
        next_quarter = 1
        year += 1
    return f"Q{next_quarter} {year}"


def _monthly_requirements_target_tech_live_date(project: dict[str, Any], monthly_requirements_targets: list[dict[str, Any]]) -> tuple[str, str, dict[str, Any]] | None:
    priority = str(project.get("priority") or "").strip().upper()
    if priority not in {"SP", "P0"}:
        return None
    entries = _monthly_requirements_entries_for_project(project, monthly_requirements_targets, limit=10)
    dated_entries: list[tuple[date | None, dict[str, Any]]] = []
    for entry in entries:
        if str(entry.get("target_date") or "").strip().upper() in {"TBC", "TBD"}:
            dated_entries.append((None, entry))
            continue
        parsed = _monthly_requirements_entry_date(entry)
        if parsed is None:
            continue
        dated_entries.append((parsed, entry))
    if not dated_entries:
        return None
    preferred_entries = [
        item
        for item in dated_entries
        if str(item[1].get("target_label") or "") in {"tech_live", "table_target"}
        or _monthly_requirements_has_tech_live_label(str(item[1].get("matched_line") or ""))
    ]
    if not preferred_entries:
        return None
    latest: tuple[date | None, dict[str, Any]] | None = None
    latest_rank: tuple[date, int, date] | None = None
    for parsed, entry in (preferred_entries or dated_entries):
        source_date = _monthly_requirements_entry_source_date(entry)
        entry_index = _safe_int(entry.get("entry_index"))
        rank = (source_date or date.min, entry_index, parsed or date.max)
        if latest is None or latest_rank is None or rank > latest_rank:
            latest = (parsed, entry)
            latest_rank = rank
    detail = {
        "market": str(latest[1].get("market") or "").strip(),
        "source_subject": str(latest[1].get("source_subject") or "").strip(),
        "sender": str(latest[1].get("sender") or "").strip(),
        "matched_line": str(latest[1].get("matched_line") or "").strip()[:500],
        "matched_alias": str(latest[1].get("matched_alias") or "").strip(),
        "source_date_hint": str(latest[1].get("source_date_hint") or "").strip(),
        "target_label": str(latest[1].get("target_label") or "").strip(),
    }
    target_label = "TBC" if latest[0] is None else _monthly_report_month_label(latest[0])
    return target_label, "Monthly Requirements Biweekly Update", detail


def build_monthly_requirements_target_map(monthly_requirements_text: str) -> list[dict[str, Any]]:
    raw = str(monthly_requirements_text or "").strip()
    if not raw:
        return []
    entries: list[dict[str, Any]] = []
    current_market = ""
    current_subject = ""
    current_sender = ""
    previous_context_line = ""
    for line in _monthly_requirements_prepared_lines(raw):
        clean = line.strip()
        if not clean:
            continue
        if clean.casefold().startswith("market:"):
            current_market = clean.partition(":")[2].strip().upper()
            continue
        if clean.casefold().startswith("subject:"):
            current_subject = clean.partition(":")[2].strip()
            inferred_market = _monthly_requirements_market_from_subject(current_subject)
            if inferred_market:
                current_market = inferred_market
            continue
        if clean.casefold().startswith("from:"):
            current_sender = _monthly_requirements_sender_from_line(clean)
            continue
        if clean.casefold().startswith(
            (
                "generated at:",
                "window:",
                "scope:",
                "configured sources:",
                "queries:",
                "max body length",
                "thread id:",
                "gmail thread link:",
                "participants:",
                "message-id:",
                "labels:",
                "to:",
                "cc:",
                "use:",
                "date:",
            )
        ):
            continue
        if "planning" in clean.casefold():
            continue
        line_is_context = _monthly_requirements_is_project_context_line(clean)
        source_year = _monthly_requirements_year_from_subject(current_subject)
        candidates = _monthly_requirements_target_month_candidates(clean, source_year=source_year)
        if not candidates:
            if line_is_context:
                previous_context_line = clean
            elif previous_context_line and _monthly_requirements_is_subproject_context_line(clean):
                previous_context_line = f"{previous_context_line} {clean}"
            continue
        latest = max((candidate for candidate in candidates if candidate is not None), default=None)
        matched_line = clean
        if line_is_context:
            previous_context_line = clean
        elif previous_context_line:
            matched_line = f"{previous_context_line} {clean}"
        entries.append(
            {
                "market": current_market,
                "source_subject": current_subject,
                "sender": current_sender,
                "matched_line": matched_line,
                "target_month": "TBC" if latest is None else _monthly_report_month_label(latest),
                "target_date": "TBC" if latest is None else latest.isoformat(),
                "target_label": _monthly_requirements_target_label(clean, matched_line),
                "source_date_hint": _monthly_requirements_source_date_hint(current_subject),
                "entry_index": len(entries),
            }
        )
    return entries


def _monthly_requirements_prepared_lines(raw: str) -> list[str]:
    source_lines = [str(line or "").rstrip() for line in str(raw or "").splitlines()]
    prepared: list[str] = []
    index = 0
    while index < len(source_lines):
        clean = source_lines[index].strip()
        if (
            clean
            and index + 1 < len(source_lines)
            and re.search(r"\b(?:go|tech|public)\s*$", clean, flags=re.IGNORECASE)
            and re.match(r"^\s*live\b", source_lines[index + 1], flags=re.IGNORECASE)
        ):
            prepared.append(f"{clean} {source_lines[index + 1].strip()}")
            index += 2
            continue
        prepared.append(source_lines[index])
        index += 1
    return prepared


def _monthly_requirements_is_project_context_line(text: str) -> bool:
    clean = str(text or "").strip()
    if len(clean) < 8:
        return False
    lowered = clean.casefold()
    if lowered.startswith(("thread ", "message ", "body:", "market:", "subject:", "from:")):
        return False
    if set(clean) <= {"=", "-", "_"}:
        return False
    if "|" in clean and re.search(r"\bregion\b|\bpriority\b|\bproject\b", clean, flags=re.IGNORECASE):
        return False
    return bool(
        re.search(
            r"\[[A-Z]{2}\]|\[(?:SP|P0|P1|P2)\]|\b(?:strategic\s+projects?|special\s+projects?|projects?|project)\b",
            clean,
            flags=re.IGNORECASE,
        )
    )


def _monthly_requirements_is_subproject_context_line(text: str) -> bool:
    return bool(re.search(r"\bphase\s*[0-9ivx]+\b|\bRCSA\b|\bOutsourcing\b", str(text or ""), flags=re.IGNORECASE))


def _monthly_requirements_target_label(line: str, matched_line: str) -> str:
    if _monthly_requirements_has_tech_live_label(matched_line):
        return "tech_live"
    if _monthly_requirements_is_target_table_row(line):
        return "table_target"
    return "date"


def _monthly_requirements_is_target_table_row(text: str) -> bool:
    clean = str(text or "").strip()
    if "|" not in clean:
        return False
    parts = [part.strip() for part in clean.strip("|").split("|")]
    if len(parts) < 4:
        return False
    if parts[0].strip().upper() not in {"SG", "ID", "PH", "REGIONAL"}:
        return False
    if parts[1].strip().upper() not in {"SP", "P0", "P1", "P2"}:
        return False
    return bool(_monthly_requirements_month_candidates(parts[-1]))


def _monthly_requirements_target_month_candidates(text: str, *, source_year: int | None = None) -> list[date | None]:
    clean = str(text or "")
    tech_live_candidates = _monthly_requirements_tech_live_month_candidates(clean, source_year=source_year)
    if tech_live_candidates:
        return tech_live_candidates
    return _monthly_requirements_month_candidates(clean)


def _monthly_requirements_has_tech_live_label(text: str) -> bool:
    return bool(re.search(r"\b(?:tech\s*)?(?:go\s*live|golive|tech\s+live)\b", str(text or ""), flags=re.IGNORECASE))


def _monthly_requirements_tech_live_month_candidates(text: str, *, source_year: int | None = None) -> list[date | None]:
    clean = str(text or "")
    candidates: list[date | None] = []
    label_pattern = re.compile(r"\b(?:tech\s*(?:go\s*)?live|tech\s+live|go\s*live|golive)\b\s*[:：]?", flags=re.IGNORECASE)
    next_label_pattern = re.compile(
        r"\b(?:PRD|DEV|SIT|UAT|REG|LV|Public(?:\s+(?:Live|Launch))?|Planning|Requirement|Timeline)\b\s*[:：]",
        flags=re.IGNORECASE,
    )
    for match in label_pattern.finditer(clean):
        segment = clean[match.end() :]
        next_label = next_label_pattern.search(segment)
        if next_label:
            segment = segment[: next_label.start()]
        if re.search(r"(?:->|→|—>|=>)\s*(?:TBC|TBD)\b", segment, flags=re.IGNORECASE):
            candidates.append(None)
            continue
        parsed = _monthly_requirements_ordered_month_candidates(segment, source_year=source_year)
        if parsed:
            candidates.append(parsed[-1][1])
    return candidates


def _monthly_requirements_entries_for_project(project: dict[str, Any], monthly_requirements_targets: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    aliases = _monthly_requirements_match_aliases(project)
    if not aliases:
        return []
    market = _monthly_requirements_market_for_project(project)
    matched: list[dict[str, Any]] = []
    for entry in monthly_requirements_targets:
        if market and str(entry.get("market") or "").strip().upper() != market:
            continue
        line = str(entry.get("matched_line") or "").strip()
        if not line or not _monthly_requirements_line_matches_project(line, aliases):
            continue
        if not _monthly_requirements_line_phase_compatible(project, line):
            continue
        enriched = dict(entry)
        enriched["matched_alias"] = _first_matching_alias(line, aliases)
        matched.append(enriched)
        if len(matched) >= limit:
            break
    return matched


def _monthly_requirements_line_matches_project(text: str, aliases: set[str]) -> bool:
    normalized_text = str(text or "").casefold()
    compact_text = _normalize_alias_token(normalized_text)
    token_matches = 0
    for alias in aliases:
        clean_alias = str(alias or "").strip().casefold()
        compact_alias = _normalize_alias_token(clean_alias)
        if not clean_alias or not compact_alias:
            continue
        has_separator = bool(re.search(r"[\s/_:()[\],.-]+", clean_alias))
        if has_separator and len(compact_alias) >= 8 and (clean_alias in normalized_text or compact_alias in compact_text):
            return True
        if not has_separator and len(compact_alias) >= 5 and re.search(rf"\b{re.escape(clean_alias)}\b", normalized_text):
            token_matches += 1
    return token_matches >= 2


def _monthly_requirements_line_phase_compatible(project: dict[str, Any], line: str) -> bool:
    project_phases = _monthly_requirements_phase_markers(
        " ".join(
            [
                str(project.get("project_name") or ""),
                " ".join(str(ticket.get("jira_title") or "") for ticket in (project.get("jira_tickets") or []) if isinstance(ticket, dict)),
            ]
        )
    )
    if not project_phases:
        return True
    line_phases = _monthly_requirements_relevant_phase_markers_for_target_line(line)
    return not line_phases or bool(project_phases & line_phases)


def _monthly_requirements_relevant_phase_markers_for_target_line(text: str) -> set[str]:
    clean = str(text or "")
    label_match = re.search(r"\b(?:tech\s*)?(?:go\s*live|golive|tech\s+live)\b", clean, flags=re.IGNORECASE)
    if label_match:
        phase_matches = list(re.finditer(r"\bphase\s*([0-9]+|i{1,3}|iv|v)\b", clean[: label_match.start()], flags=re.IGNORECASE))
        if phase_matches:
            return _monthly_requirements_phase_markers(phase_matches[-1].group(0))
    return _monthly_requirements_phase_markers(clean)


def _monthly_requirements_phase_markers(text: str) -> set[str]:
    markers: set[str] = set()
    roman_map = {"i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5"}
    for match in re.finditer(r"\bphase\s*([0-9]+|i{1,3}|iv|v)\b", str(text or ""), flags=re.IGNORECASE):
        token = match.group(1).casefold()
        markers.add(roman_map.get(token, token))
    return markers


def _monthly_requirements_match_aliases(project: dict[str, Any]) -> set[str]:
    ignored_aliases = {
        "bank",
        "maribank",
        "seabank",
        "project",
        "strategic",
        "requirement",
        "requirements",
        "system",
    }
    aliases = set()
    for alias in _project_aliases(project):
        clean = _normalize_alias_token(alias)
        lowered = str(alias or "").casefold().strip()
        if not clean or clean in ignored_aliases or lowered in ignored_aliases:
            continue
        aliases.add(alias)
        phrase_aliases = {lowered}
        phrase_aliases.update(part.strip() for part in re.split(r"\s+-\s+", lowered) if part.strip())
        for phrase in phrase_aliases:
            if _normalize_alias_token(phrase) and phrase not in ignored_aliases:
                aliases.add(phrase)
            words = re.findall(r"[A-Za-z][A-Za-z0-9]+", phrase)
            if len(words) == 2:
                aliases.add(f"{words[1]} {words[0]}")
    return aliases


def _first_matching_alias(text: str, aliases: set[str]) -> str:
    normalized_text = str(text or "").casefold()
    compact_text = _normalize_alias_token(normalized_text)
    for alias in sorted(aliases, key=len, reverse=True):
        clean_alias = str(alias or "").strip().casefold()
        if not clean_alias:
            continue
        compact_alias = _normalize_alias_token(clean_alias)
        if clean_alias in normalized_text or (compact_alias and compact_alias in compact_text):
            return alias
    return ""


def _monthly_requirements_market_from_subject(subject: str) -> str:
    text = str(subject or "").casefold()
    for market, config in MONTHLY_REPORT_REQUIREMENTS_EMAILS.items():
        configured_subject = str(config.get("subject") or "").casefold()
        if configured_subject and configured_subject in text:
            return market
    prefix = str(subject or "").split("_", 1)[0].strip().upper()
    return prefix if prefix in MONTHLY_REPORT_REQUIREMENTS_EMAILS else ""


def _monthly_requirements_sender_from_line(line: str) -> str:
    text = str(line or "").strip()
    match = re.search(r"<([^<>@\s]+@[^<>\s]+)>", text)
    if match:
        return match.group(1).strip().lower()
    email_match = re.search(r"\b[^@\s<>]+@[^@\s<>]+\b", text)
    return email_match.group(0).strip().lower() if email_match else ""


def _monthly_requirements_entry_date(entry: dict[str, Any]) -> date | None:
    raw = entry.get("target_date")
    if isinstance(raw, date):
        return raw
    try:
        return date.fromisoformat(str(raw or "")[:10])
    except ValueError:
        return None


def _monthly_requirements_entry_source_date(entry: dict[str, Any]) -> date | None:
    raw = str((entry or {}).get("source_date_hint") or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _monthly_requirements_source_date_hint(subject: str) -> str:
    parsed = _monthly_requirements_source_date_from_subject(subject)
    return parsed.isoformat() if parsed else ""


def _monthly_requirements_source_date_from_subject(subject: str) -> date | None:
    text = str(subject or "")
    year_match = re.search(r"(?<!\d)(20\d{2})(?!\d)", text)
    full_suffix_match = re.search(r"_(\d{2})(\d{2})(\d{2})(?!\d)", text)
    if full_suffix_match:
        try:
            return date(
                2000 + int(full_suffix_match.group(1)),
                int(full_suffix_match.group(2)),
                int(full_suffix_match.group(3)),
            )
        except ValueError:
            pass
    suffix_matches = list(re.finditer(r"_(\d{2})(\d{2})(?!\d)", text))
    if not year_match or not suffix_matches:
        return None
    for suffix_match in reversed(suffix_matches):
        try:
            return date(int(year_match.group(1)), int(suffix_match.group(1)), int(suffix_match.group(2)))
        except ValueError:
            continue
    return None


def _monthly_requirements_year_from_subject(subject: str) -> int | None:
    text = str(subject or "")
    full_suffix_match = re.search(r"_(\d{2})(\d{2})(\d{2})(?!\d)", text)
    if full_suffix_match:
        try:
            return 2000 + int(full_suffix_match.group(1))
        except ValueError:  # pragma: no cover
            return None
    match = re.search(r"(?<!\d)(20\d{2})(?!\d)", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:  # pragma: no cover
        return None


def _monthly_requirements_market_for_project(project: dict[str, Any]) -> str:
    text = " ".join(
        [
            str(project.get("market") or ""),
            str(project.get("region") or ""),
            str(project.get("country") or ""),
            str(project.get("project_name") or ""),
        ]
    ).casefold()
    country_markets = {
        "singapore": "SG",
        "sg": "SG",
        "indonesia": "ID",
        "id": "ID",
        "philippines": "PH",
        "philippine": "PH",
        "ph": "PH",
    }
    for token, market in country_markets.items():
        if re.search(rf"\b{re.escape(token)}\b", text):
            return market
    for market in ("SG", "ID", "PH"):
        if re.search(rf"\b{re.escape(market.casefold())}\b", text):  # pragma: no cover
            return market
    return ""


def _monthly_requirements_month_candidates(text: str, *, source_year: int | None = None) -> list[date]:
    return [candidate for _, candidate in _monthly_requirements_ordered_month_candidates(text, source_year=source_year)]


def _monthly_requirements_ordered_month_candidates(text: str, *, source_year: int | None = None) -> list[tuple[int, date]]:
    if re.search(r"\bplanning\b", text, flags=re.IGNORECASE):
        # Planning rows do not carry a committed tech-live window.
        text = "\n".join(line for line in text.splitlines() if "planning" not in line.casefold())
    month_numbers = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }
    candidates: list[tuple[int, date]] = []
    for match in re.finditer(
        r"\b(?:\d{1,2}\s*)?([A-Za-z]{3,9})(?:\s*[-/–—]\s*(?:\d{1,2}\s*)?([A-Za-z]{3,9}))?\s+(\d{4})\b",
        text,
    ):
        month_text = (match.group(2) or match.group(1) or "").casefold()
        month = month_numbers.get(month_text)
        if not month:
            continue
        try:
            candidates.append((match.start(), date(int(match.group(3)), month, 1)))
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{4})[-/.](\d{1,2})(?:[-/.]\d{1,2})?\b", text):
        try:
            candidates.append((match.start(), date(int(match.group(1)), int(match.group(2)), 1)))
        except ValueError:
            continue
    for match in re.finditer(r"\b(\d{2})(\d{2})(\d{2})\b", text):
        try:
            year = 2000 + int(match.group(1))
            candidates.append((match.start(), date(year, int(match.group(2)), 1)))
        except ValueError:
            continue
    if source_year:
        for match in re.finditer(r"(?<!\d)(\d{2})(\d{2})(?!\d)", text):
            try:
                candidates.append((match.start(), date(int(source_year), int(match.group(1)), 1)))
            except ValueError:
                continue
        for match in re.finditer(r"\b(?:early|mid|late)?\s*([A-Za-z]{3,9})\b", text, flags=re.IGNORECASE):
            month = month_numbers.get(match.group(1).casefold())
            if not month:
                continue
            try:
                candidates.append((match.start(), date(int(source_year), month, 1)))
            except ValueError:
                continue
    return sorted(candidates, key=lambda item: item[0])


def _monthly_report_month_label(value: Any) -> str:
    if isinstance(value, date):
        return value.strftime("%b %Y")
    text = str(value or "").strip()
    if not text or text.upper() == "TBD":
        return "TBD"
    if text.upper() == "TBC":
        return "TBC"
    quarter_match = re.fullmatch(r"Q([1-4])\s+(\d{4})", text, flags=re.IGNORECASE)
    if quarter_match:
        return f"Q{quarter_match.group(1)} {quarter_match.group(2)}"
    for candidate in (text[:10], text):
        try:
            return date.fromisoformat(candidate).strftime("%b %Y")
        except ValueError:
            continue
    month_match = re.fullmatch(r"([A-Za-z]{3,9})\s+(\d{4})", text)
    if month_match:
        month = month_match.group(1)[:3].title()
        return f"{month} {month_match.group(2)}"
    return "TBD"


def _is_monthly_report_planning_version(value: Any) -> bool:
    return str(value or "").strip().casefold().startswith("planning")


def _monthly_report_current_status(
    jira_tickets: list[dict[str, Any]],
    *,
    report_period: MonthlyReportPeriod,
    material_update_score: int,
) -> str:
    if material_update_score <= 0:
        return "BRD"
    statuses = [str(ticket.get("jira_status") or "").strip().casefold() for ticket in jira_tickets if str(ticket.get("jira_status") or "").strip()]
    version_text = " ".join(
        str(ticket.get(field) or "")
        for ticket in jira_tickets
        for field in ("version", "fix_version_name", "version_status", "fix_version_status", "release_phase")
    ).casefold()
    if "uat" in version_text or any(_release_date_reached(ticket, report_period) for ticket in jira_tickets):
        return "UAT"
    if any(term in version_text for term in ("dev", "qa testing", "qa-testing", "qatesting")):
        return "Dev"
    if any(any(term in status for term in ("tech design", "developing", "development", "testing")) for status in statuses):
        return "Dev"
    if statuses and all("waiting" in status for status in statuses):
        return "BRD"
    if any(any(term in status for term in ("prd reviewed", "prd in progress")) for status in statuses):
        return "PRD"
    return "BRD"


def _monthly_report_status_label(value: Any) -> str:
    text = str(value or "").strip()
    return text if text in {"BRD", "PRD", "Dev", "UAT"} else "BRD"


def _release_date_reached(ticket: dict[str, Any], report_period: MonthlyReportPeriod) -> bool:
    release_date = str(ticket.get("release_date") or "").strip()
    if not release_date:
        return False
    try:
        return date.fromisoformat(release_date[:10]) <= report_period.end.date()
    except ValueError:
        return False


def _message_status_facts(messages: list[str]) -> list[str]:
    facts: list[str] = []
    for message in messages:
        if any(term in message.casefold() for term in ("live", "上线", "uat", "testing", "develop", "prd", "scope", "target", "timeline")):
            facts.append(message[:500])
    return facts[:8]


def _direct_project_risks(messages: list[str]) -> list[str]:
    risks: list[str] = []
    for message in messages:
        lowered = message.casefold()
        if any(term in lowered for term in MONTHLY_REPORT_DIRECT_RISK_TERMS):
            risks.append(message[:500])
    return _dedupe_preserve_order(risks)


def _direct_project_decisions(messages: list[str]) -> list[str]:
    decisions: list[str] = []
    for message in messages:
        lowered = message.casefold()
        if any(term in lowered for term in MONTHLY_REPORT_DECISION_TERMS):
            decisions.append(message[:500])
    return _dedupe_preserve_order(decisions)


def _matched_seatalk_group_ids(messages: list[str]) -> list[str]:
    ids: list[str] = []
    for message in messages:
        for match in re.findall(r"\bgroup-\d+\b", message):
            if match not in ids:
                ids.append(match)
    return ids


def _is_excluded_ask(value: str) -> bool:
    lowered = str(value or "").casefold()
    return any(term in lowered for term in MONTHLY_REPORT_EXCLUDED_ASK_TERMS)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result


def _is_project_in_product_scope(project: dict[str, Any], *, team_key: str = "", team_label: str = "") -> bool:
    if str(team_key or "").strip().upper() in {"AF", "CRMS", "GRC"}:
        return True
    return _is_product_scope_text(
        " ".join(
            [
                str(team_label or ""),
                str(project.get("project_name") or ""),
                str(project.get("market") or ""),
                str(project.get("status") or ""),
                str(project.get("priority") or ""),
                str(project.get("regional_pm_pic") or ""),
                *[
                    " ".join(
                        [
                            str(ticket.get("jira_id") or ticket.get("issue_id") or ""),
                            str(ticket.get("jira_title") or ""),
                            str(ticket.get("description") or ""),
                        ]
                    )
                    for ticket in (project.get("jira_tickets") or [])
                    if isinstance(ticket, dict)
                ],
            ]
        )
    )


def _is_product_scope_text(value: str) -> bool:
    normalized = f" {str(value or '').casefold()} "
    return any(term in normalized for term in MONTHLY_REPORT_PRODUCT_SCOPE_TERMS)


def _filter_text_by_product_scope(text: str) -> tuple[str, int]:
    kept: list[str] = []
    filtered = 0
    for line in str(text or "").splitlines():
        if not line.strip():
            kept.append(line)
            continue
        if _is_product_scope_text(line):
            kept.append(line)
        else:
            filtered += 1
    return "\n".join(kept).strip(), filtered


def _filter_text_by_product_scope_or_highlight_aliases(text: str, highlight_aliases: set[str]) -> tuple[str, int, int]:
    lines = str(text or "").splitlines()
    if not highlight_aliases:
        filtered_text, filtered_count = _filter_text_by_product_scope(text)
        return filtered_text, filtered_count, 0

    keep = [False] * len(lines)
    highlight_match_count = 0
    if not any(line.strip().startswith("===") for line in lines):
        for index, line in enumerate(lines):
            clean = line.strip()
            if not clean:
                continue
            if _is_product_scope_text(clean):
                keep[index] = True
            if _text_matches_aliases(clean, highlight_aliases):
                highlight_match_count += 1
                start = max(0, index - 2)
                end = min(len(lines), index + 4)
                for nearby_index in range(start, end):
                    keep[nearby_index] = True
    else:
        for _header, start, end in _monthly_report_seatalk_conversation_groups(lines):
            section_matches = False
            for index in range(start, end):
                clean = lines[index].strip()
                if not clean:
                    continue
                if _is_product_scope_text(clean):
                    keep[index] = True
                if _text_matches_aliases(clean, highlight_aliases):
                    highlight_match_count += 1
                    section_matches = True
            if section_matches:
                for index in range(start, end):
                    keep[index] = True

    kept: list[str] = []
    filtered = 0
    last_kept_blank = False
    for index, line in enumerate(lines):
        clean = line.strip()
        if keep[index]:
            kept.append(line)
            last_kept_blank = False
            continue
        if not clean:
            if kept and not last_kept_blank:
                kept.append(line)
                last_kept_blank = True
            continue
        filtered += 1
    return "\n".join(kept).strip(), filtered, highlight_match_count


def _monthly_report_seatalk_conversation_groups(lines: list[str]) -> list[tuple[str, int, int]]:
    groups: list[tuple[str, int, int]] = []
    current_header = ""
    current_start = 0
    for index, line in enumerate(lines):
        clean = str(line or "").strip()
        if not clean.startswith("==="):
            continue
        if index > current_start:
            groups.append((current_header, current_start, index))
        current_header = clean
        current_start = index
    if len(lines) > current_start:
        groups.append((current_header, current_start, len(lines)))
    return groups or [("", 0, len(lines))]


def _filter_thread_export_by_product_scope(text: str) -> tuple[str, int]:
    raw = str(text or "").strip()
    if not raw:
        return "", 0
    separator = "=" * 80
    if separator not in raw:
        return (raw, 0) if _is_product_scope_text(raw) else ("", 1)
    header, *sections = raw.split(separator)
    kept_sections = [section.strip() for section in sections if _is_product_scope_text(section)]
    filtered = len(sections) - len(kept_sections)
    if not kept_sections:
        return "\n".join([header.strip(), "No material in-scope VIP Gmail threads were found in this window."]).strip(), filtered
    return "\n\n".join([header.strip(), *[f"{separator}\n{section}" for section in kept_sections]]).strip(), filtered


def _compact_ticket(ticket: dict[str, Any]) -> dict[str, Any]:
    return {
        "jira_id": str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip(),
        "jira_link": str(ticket.get("jira_link") or "").strip(),
        "jira_title": str(ticket.get("jira_title") or "").strip(),
        "pm_email": str(ticket.get("pm_email") or "").strip().lower(),
        "jira_status": str(ticket.get("jira_status") or "").strip(),
        "release_date": str(ticket.get("release_date") or "").strip(),
        "version": str(ticket.get("version") or "").strip(),
        "fix_version_name": str(ticket.get("fix_version_name") or "").strip(),
        "version_status": str(ticket.get("version_status") or ticket.get("fix_version_status") or ticket.get("release_phase") or "").strip(),
        "description": str(ticket.get("description") or "").strip()[:MONTHLY_REPORT_MAX_DESCRIPTION_CHARS],
        "prd_links": [
            {"label": str(item.get("label") or item.get("url") or "").strip(), "url": str(item.get("url") or "").strip()}
            for item in (ticket.get("prd_links") or [])
            if isinstance(item, dict) and str(item.get("url") or "").strip()
        ],
    }


def _normalized_email_set(value: Any) -> set[str]:
    items = value if isinstance(value, list) else []
    return {str(item or "").strip().lower() for item in items if str(item or "").strip()}


def _priority_rank(value: Any) -> int:
    text = str(value or "").strip().casefold()
    return {"sp": 0, "p0": 1, "p1": 2, "p2": 3}.get(text, 9)


def _json_block(value: Any) -> str:
    import json

    return "```json\n" + json.dumps(value, ensure_ascii=False, indent=2) + "\n```"


def _payload_block(value: Any) -> str:
    if isinstance(value, str):
        return value
    return _json_block(value)


def _strip_jira_issue_keys_for_report(value: Any) -> str:
    text = str(value or "")
    return re.sub(r"\b(?!BPMIS\b)[A-Z][A-Z0-9]{1,15}-\d+\b", "[ticket]", text)


def _strip_jira_issue_keys_from_data(value: Any) -> Any:
    if isinstance(value, str):
        return _strip_jira_issue_keys_for_report(value)
    if isinstance(value, list):
        return [_strip_jira_issue_keys_from_data(item) for item in value]
    if isinstance(value, dict):
        return {key: _strip_jira_issue_keys_from_data(item) for key, item in value.items()}
    return value


def _sanitize_monthly_report_output(value: str) -> str:
    text = re.sub(
        r"No material update found",
        "BRD",
        _strip_jira_issue_keys_for_report(value),
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"No confirmed(?:\s+\w+){0,5}\s+evidence is available(?:\s+for [^.]+)?\.",
        "This item remains pending confirmation before it can be positioned as material progress.",
        text,
        flags=re.IGNORECASE,
    )
    lines = text.splitlines()
    cleaned: list[str] = []
    skipping_followups = False
    for line in lines:
        heading = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if heading:
            title = heading.group(2).strip().casefold()
            if title in {"key follow-ups", "key follow ups", "follow-ups", "follow ups"}:
                skipping_followups = True
                continue
            if skipping_followups:
                skipping_followups = False
        if not skipping_followups:
            cleaned.append(line)
    return "\n".join(cleaned).strip()


def build_monthly_report_project_tables(monthly_evidence_brief: list[dict[str, Any]]) -> str:
    included = _compact_monthly_evidence_for_final(monthly_evidence_brief)
    grouped: dict[str, list[dict[str, Any]]] = {
        "Anti-Fraud": [],
        "Credit Risk": [],
        "Ops Risk": [],
    }
    for item in included:
        area = _monthly_report_project_table_area(item)
        grouped.setdefault(area, []).append(item)

    lines: list[str] = []
    headings = [
        ("Anti-Fraud", "## 1. Anti-Fraud Updates"),
        ("Credit Risk", "## 2. Credit Risk Updates"),
        ("Ops Risk", "## 3. Ops Risk (GRC System) Updates"),
    ]
    for area, heading in headings:
        rows = grouped.get(area) or []
        if not rows:
            continue
        rows = sorted(rows, key=_monthly_report_project_table_sort_key)
        if lines:
            lines.append("")
        lines.extend(
            [
                heading,
                "",
                "| Region | Priority | Project | Current Status | Target Tech Live Date |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for item in rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        _monthly_report_table_cell(item.get("market") or "TBD"),
                        _monthly_report_table_cell(item.get("priority") or "TBD"),
                        _monthly_report_table_cell(item.get("project_name") or "TBD"),
                        _monthly_report_table_cell(_monthly_report_status_label(item.get("current_status"))),
                        _monthly_report_table_cell(_monthly_report_month_label(item.get("target_tech_live_date"))),
                    ]
                )
                + " |"
            )
    return "\n".join(lines).strip()


def _apply_monthly_report_project_tables(draft_markdown: str, monthly_evidence_brief: list[dict[str, Any]]) -> str:
    table_markdown = build_monthly_report_project_tables(monthly_evidence_brief)
    draft = str(draft_markdown or "").strip()
    if not table_markdown:
        return draft

    first_project_section = _monthly_report_first_project_section_match(draft)
    signoff_match = re.search(r"(?im)^\s*Regards\s*(?:\n|$)", draft)
    signoff = "Regards  \nXiaodong"
    if signoff_match:
        signoff = draft[signoff_match.start():].strip()

    if first_project_section:
        prefix = draft[: first_project_section.start()].rstrip()
    elif signoff_match:
        prefix = draft[: signoff_match.start()].rstrip()
    else:
        prefix = draft.rstrip()

    sections = [section for section in [prefix, table_markdown, signoff] if section]
    return "\n\n".join(sections).strip()


def _monthly_report_first_project_section_match(text: str) -> re.Match[str] | None:
    return re.search(
        r"(?im)^\s*(?:#{1,6}\s*)?(?:[1-3]\.\s*)?(?:Anti[- ]Fraud|Credit Risk|Ops Risk(?:\s*\(GRC System\))?) Updates\s*$",
        text,
    )


def _monthly_report_project_table_area(item: dict[str, Any]) -> str:
    name = str(item.get("project_name") or "").casefold()
    area = str(item.get("product_area") or "").strip().casefold()
    teams = " ".join(str(value or "") for value in (item.get("teams") or [])).casefold()
    if any(term in name for term in ("grc", "rcsa", "operational risk", "outsourcing")):
        return "Ops Risk"
    if _monthly_report_project_has_credit_risk_scope(name, f"{area} {teams}"):
        return "Credit Risk"
    if "ops" in area or "grc" in area:
        return "Ops Risk"
    if "credit" in area:
        return "Credit Risk"
    if "anti" in area or "fraud" in area:
        return "Anti-Fraud"
    return "Credit Risk"


def _monthly_report_project_name_is_credit_risk(text: str) -> bool:
    return any(
        term in text
        for term in (
            "credit card",
            "credit risk",
            "cash loan",
            "loan limit",
            "loan session",
            "balance transfer",
            "sme rcf",
            "rcf loan",
        )
    )


def _monthly_report_project_has_credit_risk_scope(project_text: str, scope_text: str) -> bool:
    if not _monthly_report_project_name_is_credit_risk(project_text):
        return False
    scope = str(scope_text or "").casefold()
    if "credit" in scope or "crms" in scope:
        return True
    return not any(term in scope for term in ("anti", "fraud"))


def _monthly_report_project_table_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    priority_rank = {"SP": 0, "P0": 1, "P1": 2, "P2": 3}
    status_rank = {"UAT": 0, "Dev": 1, "PRD": 2, "BRD": 3}
    priority = str(item.get("priority") or "").strip().upper()
    status = _monthly_report_status_label(item.get("current_status"))
    return (
        priority_rank.get(priority, 9),
        status_rank.get(status, 9),
        str(item.get("project_name") or "").casefold(),
    )


def _monthly_report_table_cell(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return "TBD"
    return text.replace("|", "\\|")


def _estimate_token_count(text: str) -> int:
    return max(1, (len(str(text or "")) + MONTHLY_REPORT_TOKEN_CHARS_PER_TOKEN - 1) // MONTHLY_REPORT_TOKEN_CHARS_PER_TOKEN)


def _monthly_report_token_risk(estimated_tokens: int) -> str:
    if estimated_tokens >= MONTHLY_REPORT_TOKEN_RISK_HIGH:
        return "high"
    if estimated_tokens >= MONTHLY_REPORT_TOKEN_RISK_WARNING:
        return "warning"
    return "normal"


def _monthly_report_source_label(source: str) -> str:
    return {
        "highlight_deep_evidence": "Highlight deep evidence",
        "seatalk": "SeaTalk history",
        "vip_gmail": "VIP Gmail threads",
        "report_intelligence": "Report Intelligence matched evidence",
        "monthly_evidence_brief": "Monthly project evidence brief",
        "projects_jira": "Key Projects and Jira",
        "prd": "PRD and Confluence",
        "prd_scope_summary": "PRD and Confluence scope summaries",
        "empty": "empty evidence",
    }.get(str(source or "").strip(), str(source or "evidence"))


def _split_text_for_token_limit(text: str, target_tokens: int) -> list[str]:
    source = str(text or "").strip()
    if not source:
        return []
    max_chars = max(1_000, int(target_tokens) * MONTHLY_REPORT_TOKEN_CHARS_PER_TOKEN)
    chunks: list[str] = []
    current: list[str] = []
    current_chars = 0
    for line in source.splitlines():
        line_text = line.rstrip()
        line_chars = len(line_text) + 1
        if current and current_chars + line_chars > max_chars:
            chunks.append("\n".join(current).strip())
            current = []
            current_chars = 0
        if line_chars > max_chars:
            for start in range(0, len(line_text), max_chars):
                segment = line_text[start : start + max_chars].strip()
                if segment:
                    chunks.append(segment)
            continue
        current.append(line_text)
        current_chars += line_chars
    if current:
        chunks.append("\n".join(current).strip())
    return [chunk for chunk in chunks if chunk]


def _split_json_items_for_token_limit(items: list[dict[str, Any]], target_tokens: int) -> list[list[dict[str, Any]]]:
    if not items:
        return []
    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for item in items:
        candidate = [*current, item]
        if current and _estimate_token_count(_json_block(candidate)) > target_tokens:
            chunks.append(current)
            current = [item]
        else:
            current = candidate
        if _estimate_token_count(_json_block(current)) > target_tokens:
            split_item = _split_large_json_item(item, target_tokens)
            if len(current) == 1:
                current = []
            for part in split_item:
                chunks.append([part])
    if current:
        chunks.append(current)
    return chunks


def _split_large_json_item(item: dict[str, Any], target_tokens: int) -> list[dict[str, Any]]:
    text = _json_block(item)
    if _estimate_token_count(text) <= target_tokens:
        return [item]
    max_chars = max(1_000, int(target_tokens) * MONTHLY_REPORT_TOKEN_CHARS_PER_TOKEN)
    chunks = [text[start : start + max_chars] for start in range(0, len(text), max_chars)]
    return [
        {
            "split_from": str(item.get("bpmis_id") or item.get("jira_id") or item.get("url") or "large_item"),
            "split_index": index,
            "content": chunk,
        }
        for index, chunk in enumerate(chunks, start=1)
        if chunk.strip()
    ]


def _emit_monthly_report_progress(
    progress_callback: Any | None,
    stage: str,
    message: str,
    current: int,
    total: int,
    *,
    estimated_prompt_tokens: int = 0,
) -> None:
    if not callable(progress_callback):
        return
    try:
        progress_callback(
            stage,
            message,
            current,
            total,
            estimated_prompt_tokens=estimated_prompt_tokens,
            token_risk=_monthly_report_token_risk(estimated_prompt_tokens) if estimated_prompt_tokens else "",
        )
    except TypeError:
        progress_callback(stage, message, current, total)
