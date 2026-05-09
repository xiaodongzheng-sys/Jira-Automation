from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
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
MONTHLY_REPORT_GENERATION_VERSION = "v4_target_live_date"
MONTHLY_REPORT_PERIOD_ANCHOR_START = date(2026, 4, 13)
MONTHLY_REPORT_PERIOD_ANCHOR_END = date(2026, 5, 8)
MONTHLY_REPORT_PERIOD_DAYS = 28
MONTHLY_REPORT_EVIDENCE_DAYS = 14
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
MONTHLY_REPORT_MAX_HIGHLIGHT_GMAIL_THREADS_PER_TOPIC = 8
MONTHLY_REPORT_HIGHLIGHT_EVIDENCE_MAX_LINES = 24
MONTHLY_REPORT_EVIDENCE_WORKERS = 2
MONTHLY_REPORT_GMAIL_TOPIC_CACHE_VERSION = "v2"
MONTHLY_REPORT_GMAIL_TOPIC_CACHE_TTL_SECONDS = 6 * 60 * 60
MONTHLY_REPORT_PRD_SCOPE_CACHE_VERSION = "v1"
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
        product_scope: list[str] | None = None,
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
        _emit_monthly_report_progress(progress_callback, "preparing_sources", "Preparing Key Projects, Jira, PRD, and SeaTalk sources.", 0, 0)
        key_projects = self._key_projects(team_payloads)
        step_started = time.monotonic()
        _emit_monthly_report_progress(progress_callback, "collecting_seatalk", "Exporting SeaTalk history for the report period.", 0, 0)
        history_text, product_scope_filtered_count = self._seatalk_history(evidence_period)
        _record_monthly_report_timing(timings, "seatalk_export", step_started)
        step_started = time.monotonic()
        _emit_monthly_report_progress(progress_callback, "searching_vip_gmail", "Searching VIP Gmail evidence for the report period.", 0, 0)
        vip_gmail_text, vip_gmail_summary = self._vip_gmail_history(evidence_period)
        _record_monthly_report_timing(timings, "vip_gmail", step_started)
        highlight_project_matches = match_monthly_report_highlight_topics(normalized_highlight_topics, key_projects)
        highlight_project_ids = {
            str(project_id).strip()
            for match in highlight_project_matches
            for project_id in (match.get("project_ids") or [])
            if str(project_id).strip()
        }
        step_started = time.monotonic()
        highlight_gmail_evidence, highlight_gmail_summary = self._highlight_gmail_history(
            evidence_period,
            normalized_highlight_topics,
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
            prd_scope_summaries=prd_scope_summaries,
            report_period=evidence_period,
            highlight_project_ids=highlight_project_ids,
        )
        highlight_deep_evidence = build_monthly_highlight_deep_evidence(
            highlight_topics=normalized_highlight_topics,
            key_projects=key_projects,
            topic_project_matches=highlight_project_matches,
            seatalk_history_text=history_text,
            topic_gmail_evidence=highlight_gmail_evidence,
            prd_scope_summaries=prd_scope_summaries,
            report_period=evidence_period,
        )
        included_project_briefs = [
            item for item in monthly_evidence_brief if item.get("include")
        ]
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
            monthly_evidence_brief=included_project_briefs,
            highlight_deep_evidence=highlight_deep_evidence,
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
            highlight_deep_evidence=highlight_deep_evidence,
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
                highlight_deep_evidence=highlight_deep_evidence,
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
        draft_markdown = _sanitize_monthly_report_output(str(generated.get("result_markdown") or ""))
        elapsed_seconds = round(time.monotonic() - started_at, 1)
        timings["total"] = elapsed_seconds
        prompt_chars = len(prompt)
        estimated_prompt_tokens = final_estimated_tokens
        batch_token_counts = [
            int(item.get("estimated_prompt_tokens") or 0)
            for item in batch_summaries
            if isinstance(item, dict)
        ]
        return {
            "status": "ok",
            "draft_markdown": draft_markdown,
            "generated_at": self.now.isoformat(),
            "subject": monthly_report_subject(period=report_period),
            "highlight_topics": normalized_highlight_topics,
            "generation_version": MONTHLY_REPORT_GENERATION_VERSION,
            "model_id": generated["model_id"],
            "trace": generated["trace"],
            "generation_summary": {
                "generation_version": MONTHLY_REPORT_GENERATION_VERSION,
                "period_start": report_period.start_date,
                "period_end": report_period.end_date,
                "period_end_exclusive": report_period.end_exclusive.isoformat(),
                "highlight_topics": normalized_highlight_topics,
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
                "max_seatalk_chars": MONTHLY_REPORT_MAX_SEATALK_CHARS,
                "total_batches": len(batch_summaries),
                "max_batch_estimated_tokens": max(batch_token_counts) if batch_token_counts else 0,
                "final_estimated_tokens": final_estimated_tokens,
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
                "highlight_project_topic_count": len([item for item in highlight_project_matches if item.get("project_ids")]),
                "highlight_gmail_thread_count": int(highlight_gmail_summary.get("thread_count") or 0),
                "highlight_gmail_message_count": int(highlight_gmail_summary.get("message_count") or 0),
                "highlight_gmail_cache_hit_count": int(highlight_gmail_summary.get("cache_hit_count") or 0),
                "vip_gmail_thread_count": int(vip_gmail_summary.get("thread_count") or 0),
                "vip_gmail_message_count": int(vip_gmail_summary.get("message_count") or 0),
                "gmail_error_count": int(vip_gmail_summary.get("error_count") or 0) + int(highlight_gmail_summary.get("error_count") or 0),
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
            summaries.append(
                {
                    "source": batch.get("source"),
                    "index": batch.get("index"),
                    "summary_markdown": summary[:MONTHLY_REPORT_SUMMARY_MAX_CHARS],
                    "estimated_prompt_tokens": estimated_tokens,
                    "model_id": generated.get("model_id"),
                    "trace": generated.get("trace") or {},
                }
            )
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

    def _seatalk_history(self, report_period: MonthlyReportPeriod) -> tuple[str, int]:
        history = self.seatalk_service.export_history_since(
            since=report_period.start,
            now=report_period.end_exclusive,
            days=report_period.days + 1,
        )
        history = self.seatalk_service._filter_system_generated_history(history)
        history = filter_text_by_noise(history, config=self.report_intelligence_config, source="seatalk")
        history, filtered_count = _filter_text_by_product_scope(history)
        compacted = self.seatalk_service._compact_history_for_insights(
            history,
            max_chars=MONTHLY_REPORT_MAX_SEATALK_CHARS,
            signal_max_chars=420_000,
            recent_max_chars=220_000,
        )
        return compacted, filtered_count

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
                item = {
                    "topic": topic,
                    "text": text,
                    "thread_count": item_thread_count,
                    "message_count": item_message_count,
                    "query": str((payload or {}).get("query") or ""),
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
        }

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
    if not topics:
        raise ToolError(f"Monthly Report highlight topics are required. Add 1 to {MONTHLY_REPORT_MAX_HIGHLIGHT_TOPICS} topics before generating.")
    if len(topics) > MONTHLY_REPORT_MAX_HIGHLIGHT_TOPICS:
        raise ToolError(f"Monthly Report supports at most {MONTHLY_REPORT_MAX_HIGHLIGHT_TOPICS} highlight topics.")
    return topics


def match_monthly_report_highlight_topics(highlight_topics: list[str], key_projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for topic in highlight_topics:
        topic_aliases = _highlight_topic_aliases(topic)
        project_ids: list[str] = []
        project_names: list[str] = []
        for project in key_projects:
            if _highlight_topic_matches_project(topic_aliases, project):
                project_id = str(project.get("bpmis_id") or "").strip()
                if project_id and project_id not in project_ids:
                    project_ids.append(project_id)
                    project_names.append(str(project.get("project_name") or "").strip())
        matches.append({"topic": topic, "project_ids": project_ids, "project_names": [name for name in project_names if name]})
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
) -> list[dict[str, Any]]:
    projects_by_id = {str(project.get("bpmis_id") or "").strip(): project for project in key_projects if str(project.get("bpmis_id") or "").strip()}
    prd_by_jira = _index_prd_summaries_by_jira(prd_scope_summaries)
    gmail_by_topic = {str(item.get("topic") or "").strip(): item for item in topic_gmail_evidence if isinstance(item, dict)}
    match_by_topic = {str(item.get("topic") or "").strip(): item for item in topic_project_matches if isinstance(item, dict)}
    evidence: list[dict[str, Any]] = []
    for topic in highlight_topics:
        match = match_by_topic.get(topic) or {"topic": topic, "project_ids": []}
        project_ids = [str(project_id or "").strip() for project_id in (match.get("project_ids") or []) if str(project_id or "").strip()]
        projects = [projects_by_id[project_id] for project_id in project_ids if project_id in projects_by_id]
        aliases = _highlight_topic_aliases(topic)
        for project in projects:
            aliases.update(_project_aliases(project))
        matched_seatalk = _matched_lines_for_project(seatalk_history_text, aliases, limit=MONTHLY_REPORT_HIGHLIGHT_EVIDENCE_MAX_LINES)
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
            target_tech_live_date, target_tech_live_version = _monthly_report_target_tech_live_date(jira_tickets)
            project_updates.append(
                {
                    "bpmis_id": str(project.get("bpmis_id") or "").strip(),
                    "project_name": str(project.get("project_name") or "").strip(),
                    "market": str(project.get("market") or "").strip(),
                    "priority": str(project.get("priority") or "").strip(),
                    "target_tech_live_date": target_tech_live_date,
                    "target_tech_live_version": target_tech_live_version,
                    "current_status": _monthly_report_current_status(
                        jira_tickets,
                        report_period=report_period,
                        material_update_score=jira_score + len(project_prd_facts) * 2 + len(matched_seatalk) * 3,
                    ),
                    "status_facts": _compact_report_text_list(status_facts, limit=6, max_chars=320),
                    "timeline_facts": _compact_report_text_list(timeline_facts, limit=5, max_chars=240),
                    "prd_scope_summaries": _compact_report_text_list(project_prd_facts, limit=4, max_chars=700),
                }
            )
        gmail_item = gmail_by_topic.get(topic) or {}
        gmail_text = str(gmail_item.get("text") or "").strip()
        evidence.append(
            {
                "topic": topic,
                "topic_type": "project_update" if projects else "general_topic",
                "matched_project_ids": project_ids,
                "matched_project_names": [str(project.get("project_name") or "").strip() for project in projects if str(project.get("project_name") or "").strip()],
                "project_updates": project_updates,
                "seatalk_evidence": _compact_report_text_list(matched_seatalk, limit=16, max_chars=600),
                "gmail_evidence": _compact_report_text_list(_matched_sections_for_project(gmail_text, aliases, limit=8), limit=8, max_chars=900),
                "gmail_error": str(gmail_item.get("error") or "").strip(),
                "prd_scope_summaries": _compact_report_text_list(prd_facts, limit=6, max_chars=700),
            }
        )
    return evidence


def build_monthly_project_evidence_brief(
    *,
    key_projects: list[dict[str, Any]],
    seatalk_history_text: str,
    vip_gmail_text: str,
    prd_scope_summaries: list[dict[str, Any]],
    report_period: MonthlyReportPeriod,
    highlight_project_ids: set[str] | None = None,
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
        matched_gmail = _matched_sections_for_project(vip_gmail_text, aliases, limit=MONTHLY_REPORT_PROJECT_EVIDENCE_MAX_GMAIL)
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
        current_status = _monthly_report_current_status(jira_tickets, report_period=report_period, material_update_score=score)
        target_tech_live_date, target_tech_live_version = _monthly_report_target_tech_live_date(jira_tickets)
        include = True
        evidence_sources = {
            "jira": jira_sources,
            "seatalk": matched_seatalk,
            "vip_gmail": matched_gmail,
            "prd_scope_summary": prd_facts,
        }
        items.append(
            {
                "include": include,
                "exclude_reason": "",
                "product_area": _project_product_area(project),
                "project_id": str(project.get("bpmis_id") or "").strip(),
                "bpmis_id": str(project.get("bpmis_id") or "").strip(),
                "project_name": str(project.get("project_name") or "").strip(),
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


def build_monthly_report_prompt(
    *,
    template: str,
    generated_at: datetime,
    seatalk_history_text: str,
    key_projects: list[dict[str, Any]],
    prd_sources: list[dict[str, str]],
    prd_errors: list[str],
) -> str:
    return (
        "# Task\n"
        "Generate Xiaodong Zheng's monthly team report as concise, business-ready Markdown.\n"
        "Use the configured template as the required structure. Do not invent facts; when evidence is weak, state the gap or mark as TBD.\n"
        "Synthesize the configured report-period SeaTalk history with Key Project Biz Project and Jira evidence. Prefer concrete project names, decisions, risks, owners, and dates.\n"
        "Do not include raw transcripts, long PRD excerpts, tool logs, or confidential implementation chatter that is not needed for a monthly business report.\n\n"
        "# Output Rules\n"
        "- Return only the final Markdown draft.\n"
        "- Keep it suitable to send by email after light PM editing.\n"
        "- Follow the template headings unless the evidence clearly requires a small additional subsection.\n"
        "- If the configured template contains Markdown tables, preserve those table structures and fill rows from evidence; use TBD for missing cells instead of converting the table to bullets.\n"
        "- Do not include Jira ticket IDs or Jira links in the final report.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Monthly Report Template\n{normalize_monthly_report_template(template)}\n\n"
        "# Key Project / Jira Evidence\n"
        f"{_json_block(key_projects)}\n\n"
        "# PRD / Confluence Enrichment\n"
        f"{_json_block(prd_sources)}\n\n"
        "# PRD Enrichment Gaps\n"
        f"{_json_block(prd_errors)}\n\n"
        "# SeaTalk History From Report Period\n"
        f"{seatalk_history_text or 'No readable SeaTalk messages were found in the report period.'}"
    )


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
    return (
        "# Task\n"
        f"Summarize one Monthly Report evidence batch from {source_label}.\n"
        "Do not write the final report. Extract only facts useful for the final monthly business report.\n"
        "For Highlight deep evidence, preserve narrative facts for the user-provided topics. For Monthly project evidence brief, keep other Key Projects concise and do not expand them into highlights.\n"
        f"Hard scope: include only Xiaodong-owned {', '.join(MONTHLY_REPORT_PRODUCT_SCOPE)} product updates. Exclude unrelated general awareness, HR, hiring, personal chat, random live issue, and generic IT/process/material-check updates even if a VIP or priority keyword appears.\n"
        "Use concise Markdown with these headings exactly: Highlights, Decisions, Risks, Owners, Project References, Open Asks, Evidence Gaps.\n"
        "Preserve concrete project names, Jira IDs, owners, markets, dates, decisions, blockers, and launch/status facts.\n"
        "If this batch has no material in-scope evidence, return only: No material update found.\n"
        "Do not include raw transcripts or long excerpts.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}\n\n"
        f"# User-Provided Highlight Topics\n{_json_block(highlight_topics)}\n\n"
        f"# Monthly Report Template For Orientation\n{normalize_monthly_report_template(template)}\n\n"
        f"# Evidence Source\n{source_label}\n\n"
        "# PRD Enrichment Gaps\n"
        f"{_json_block(prd_errors)}\n\n"
        "# Batch Payload\n"
        f"{_payload_block(payload)}"
    )


def build_monthly_report_merge_prompt(
    *,
    generated_at: datetime,
    report_period: MonthlyReportPeriod,
    highlight_topics: list[str],
    batch_summaries: list[dict[str, Any]],
    prd_errors: list[str],
) -> str:
    return (
        "# Task\n"
        "Merge Monthly Report batch summaries into one compact evidence brief for final drafting.\n"
        "Do not write the final report. Deduplicate repeated facts and keep the strongest concrete evidence.\n"
        "Keep Highlight deep evidence separate from Other Key Project Updates. Do not let non-highlight project updates become highlight narrative.\n"
        f"Hard scope: keep only Xiaodong-owned {', '.join(MONTHLY_REPORT_PRODUCT_SCOPE)} product updates. Drop unrelated updates even if they mention VIPs, approval, risk, launch, urgent, BSP, or OJK.\n"
        "Use these headings exactly: Executive Themes, Key Project Progress, Delivery Evidence, Risks And Blockers, Decisions Needed, Evidence Gaps.\n"
        "Keep the user-provided highlight topics visible as the final draft's required Highlights scope.\n"
        "Keep the brief concise enough for one final model call.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}\n\n"
        f"# User-Provided Highlight Topics\n{_json_block(highlight_topics)}\n\n"
        "# PRD Enrichment Gaps\n"
        f"{_json_block(prd_errors)}\n\n"
        "# Batch Summaries\n"
        f"{_json_block(batch_summaries)}"
    )


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
) -> str:
    included_project_evidence = _strip_jira_issue_keys_from_data(_compact_monthly_evidence_for_final(monthly_evidence_brief))
    safe_highlight_deep_evidence = _strip_jira_issue_keys_from_data(highlight_deep_evidence or [])
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
        "Use only the Other Key Project Updates JSON below as the authoritative project-table allowlist. The compact evidence brief is supplemental context only.\n"
        "Use Highlight Deep Evidence as the primary source for the Highlights narrative. Use Other Key Project Updates only for the project table and concise non-highlight updates.\n"
        "The audience is Xiaodong's manager. Write Highlights as an executive product update, not as an investigation log: emphasize business impact, delivery progress, material risk, decision needed, and next action.\n"
        "Use calm, factual, ownership-oriented wording. Avoid alarmist language, raw technical incident wording, internal tool/process details, chat-style phrasing, and over-hedged phrases unless the uncertainty itself is the management point.\n"
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
        "- Follow the template headings unless the evidence clearly requires a small additional subsection.\n"
        "- If the configured template contains Markdown tables, preserve those table structures and fill rows from evidence; use TBD for missing cells instead of converting the table to bullets.\n"
        "- Highlights must cover only the user-provided highlight topics below; do not add unrelated highlight topics.\n"
        "- Each Highlight should be manager-ready: one compact paragraph per topic, focused on what changed, why it matters, current risk or decision, and the expected next movement.\n"
        "- Do not expose raw evidence mechanics in Highlights. Do not say 'SeaTalk says', 'Gmail says', 'evidence gap', 'query', 'thread', 'ticket', or similar source/tool terms.\n"
        "- Do not include Jira ticket IDs, Jira links, or issue-key references in the report.\n"
        "- Do not include a Key Follow-Ups section.\n"
        "- Current Status must be exactly one of: BRD, PRD, Dev, UAT. Do not add explanations in that cell.\n\n"
        "- Target Tech Live Date must use target_tech_live_date from Other Key Project Updates exactly. It must be MMM YYYY, such as May 2026, or TBD. Do not infer a target date from timeline_facts or any version starting with Planning.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Report Period\n{report_period.start_date} to {report_period.end_date}\n\n"
        f"# User-Provided Highlight Topics\n{_json_block(highlight_topics)}\n\n"
        f"# Monthly Report Template\n{normalize_monthly_report_template(template)}\n\n"
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
                "market": str(item.get("market") or "").strip(),
                "priority": str(item.get("priority") or "").strip(),
                "seatalk_group_ids": _compact_text_list(item.get("seatalk_group_ids"), limit=8, max_chars=80),
                "material_update_score": _safe_int(item.get("material_update_score")),
                "current_status": _monthly_report_status_label(item.get("current_status")),
                "target_tech_live_date": _monthly_report_month_label(item.get("target_tech_live_date")),
                "target_tech_live_version": str(item.get("target_tech_live_version") or "").strip(),
                "status_facts": _compact_report_text_list(item.get("status_facts"), limit=6, max_chars=320),
                "timeline_facts": _compact_report_text_list(item.get("timeline_facts"), limit=5, max_chars=240),
                "risks": _compact_report_text_list(item.get("risks"), limit=4, max_chars=320),
                "decisions_needed": _compact_report_text_list(item.get("decisions_needed"), limit=4, max_chars=320),
                "matched_prd_summaries": _compact_report_text_list(item.get("matched_prd_summaries"), limit=3, max_chars=500),
            }
        )
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
        },
        primary_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
        fallback_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
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
    return f"Monthly Report - {report_period.start_date} to {report_period.end_date}"


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
        for key in ("seatalk_evidence", "gmail_evidence", "prd_scope_summaries"):
            for value in item.get(key) or []:
                text = str(value or "").strip()
                if text:
                    lines.append(text)
    return "\n".join(lines)


def _project_product_area(project: dict[str, Any]) -> str:
    teams = " ".join(str(item or "") for item in (project.get("teams") or []))
    text = f"{teams} {project.get('project_name') or ''}".casefold()
    if "anti" in text or "fraud" in text or "af" in text:
        return "Anti-fraud"
    if "grc" in text or "ops" in text or "operational risk" in text or "rcsa" in text:
        return "Ops Risk"
    return "Credit Risk"


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
    expanded: set[str] = set()
    for alias in aliases:
        expanded.add(alias)
        if "alcv12" in alias.replace(" ", ""):
            expanded.update({"alc v12", "alcv12", "alc"})
    return {item for item in expanded if item}


def _highlight_topic_aliases(topic: Any) -> set[str]:
    aliases: set[str] = set()
    text = str(topic or "").strip()
    if len(text) >= 3:
        aliases.add(text.casefold())
        aliases.add(_normalize_alias_token(text))
    for part in re.split(r"[\s/_:()[\],.-]+", text):
        token = part.strip().casefold()
        if _is_useful_alias_token(token):
            aliases.add(token)
    return {alias for alias in aliases if alias}


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
    if token in {"feature", "support", "model", "upgrade", "project", "system", "productization", "strategy", "phase", "update", "status"}:
        return False
    return any(character.isdigit() for character in token) or len(token) >= 6


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


def _monthly_report_target_tech_live_date(jira_tickets: list[dict[str, Any]]) -> tuple[str, str]:
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
        return "TBD", ""
    return _monthly_report_month_label(latest[0]), latest[1]


def _monthly_report_month_label(value: Any) -> str:
    if isinstance(value, date):
        return value.strftime("%b %Y")
    text = str(value or "").strip()
    if not text or text.upper() == "TBD":
        return "TBD"
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
