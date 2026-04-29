from __future__ import annotations

import html
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from google.oauth2.credentials import Credentials

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.gmail_sender import GMAIL_SEND_SCOPE, StoredGoogleCredentials, credentials_from_payload, send_gmail_message
from bpmis_jira_tool.seatalk_dashboard import SEATALK_INSIGHTS_TIMEZONE, SeaTalkDashboardService
from bpmis_jira_tool.source_code_qa import CodexCliBridgeSourceCodeQALLMProvider
from prd_briefing.confluence import ConfluenceConnector
from prd_briefing.reviewer import _build_prd_source


DEFAULT_MONTHLY_REPORT_RECIPIENT = "xiaodong.zheng@npt.sg"
MONTHLY_REPORT_PROMPT_VERSION = "v1_team_dashboard_monthly_report"
MONTHLY_REPORT_SEATALK_DAYS = 30
MONTHLY_REPORT_MAX_SEATALK_CHARS = 480_000
MONTHLY_REPORT_MAX_PROJECTS = 20
MONTHLY_REPORT_MAX_TICKETS_PER_PROJECT = 18
MONTHLY_REPORT_MAX_PRD_PAGES = 10
MONTHLY_REPORT_MAX_PRD_CHARS_PER_PAGE = 8_000
MONTHLY_REPORT_MAX_DESCRIPTION_CHARS = 4_000
MONTHLY_REPORT_TOKEN_CHARS_PER_TOKEN = 4
MONTHLY_REPORT_TOKEN_RISK_WARNING = 120_000
MONTHLY_REPORT_TOKEN_RISK_HIGH = 180_000
MONTHLY_REPORT_BATCH_TARGET_TOKENS = 45_000
MONTHLY_REPORT_BATCH_MAX_TOKENS = 60_000
MONTHLY_REPORT_MERGE_MAX_TOKENS = 80_000
MONTHLY_REPORT_FINAL_MAX_TOKENS = 100_000
MONTHLY_REPORT_SUMMARY_MAX_CHARS = 14_000
MONTHLY_REPORT_BRIEF_MAX_CHARS = 48_000

DEFAULT_MONTHLY_REPORT_TEMPLATE = """# Monthly Report

## Executive Summary
- Summarize the most important delivery progress, decisions, and risks across Anti-fraud, Credit Risk, and Ops Risk.

## Key Project Progress
- For each Key Project, include Biz Project ID, project name, market, current stage, Jira progress, and notable PRD/business changes.

## Blockers / Risks
- Highlight unresolved blockers, cross-team dependencies, delayed decisions, production or compliance risks, and owners where clear.

## Delivery Outlook
- Explain expected next steps and upcoming milestones for the next month.

## Asks / Decisions Needed
- List decisions, approvals, or follow-ups needed from Xiaodong or stakeholders.
"""


@dataclass(frozen=True)
class MonthlyReportSendResult:
    status: str
    recipient: str
    subject: str
    message_id: str = ""


class MonthlyReportService:
    def __init__(
        self,
        *,
        settings: Settings,
        workspace_root: Path,
        seatalk_service: SeaTalkDashboardService,
        confluence: ConfluenceConnector | None = None,
        now: datetime | None = None,
    ) -> None:
        self.settings = settings
        self.workspace_root = Path(workspace_root)
        self.seatalk_service = seatalk_service
        self.confluence = confluence
        self.now = (now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)).astimezone(SEATALK_INSIGHTS_TIMEZONE)

    def generate_draft(
        self,
        *,
        template: str,
        team_payloads: list[dict[str, Any]],
        progress_callback: Any | None = None,
    ) -> dict[str, Any]:
        started_at = time.monotonic()
        effective_template = normalize_monthly_report_template(template)
        _emit_monthly_report_progress(progress_callback, "preparing_sources", "Preparing Key Projects, Jira, PRD, and SeaTalk sources.", 0, 0)
        key_projects = self._key_projects(team_payloads)
        history_text = self._seatalk_history()
        prd_sources, prd_errors = self._prd_sources(key_projects)
        batch_summaries = self._batch_summaries(
            template=effective_template,
            generated_at=self.now,
            seatalk_history_text=history_text,
            key_projects=key_projects,
            prd_sources=prd_sources,
            prd_errors=prd_errors,
            progress_callback=progress_callback,
        )
        evidence_brief = self._merge_batch_summaries(
            generated_at=self.now,
            batch_summaries=batch_summaries,
            prd_errors=prd_errors,
            progress_callback=progress_callback,
        )
        prompt = build_monthly_report_final_prompt(
            template=effective_template,
            generated_at=self.now,
            evidence_brief=evidence_brief,
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
                evidence_brief=evidence_brief,
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
        generated = self._guarded_generate(
            prompt=prompt,
            prompt_mode=f"{MONTHLY_REPORT_PROMPT_VERSION}_final",
            max_tokens=MONTHLY_REPORT_FINAL_MAX_TOKENS,
            progress_callback=progress_callback,
        )
        elapsed_seconds = round(time.monotonic() - started_at, 1)
        prompt_chars = len(prompt)
        estimated_prompt_tokens = final_estimated_tokens
        batch_token_counts = [
            int(item.get("estimated_prompt_tokens") or 0)
            for item in batch_summaries
            if isinstance(item, dict)
        ]
        return {
            "status": "ok",
            "draft_markdown": generated["result_markdown"],
            "generated_at": self.now.isoformat(),
            "model_id": generated["model_id"],
            "trace": generated["trace"],
            "generation_summary": {
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
            },
            "evidence_summary": {
                "seatalk_days": MONTHLY_REPORT_SEATALK_DAYS,
                "key_project_count": len(key_projects),
                "jira_ticket_count": sum(len(project.get("jira_tickets") or []) for project in key_projects),
                "prd_page_count": len(prd_sources),
                "prd_error_count": len(prd_errors),
            },
        }

    def _batch_summaries(
        self,
        *,
        template: str,
        generated_at: datetime,
        seatalk_history_text: str,
        key_projects: list[dict[str, Any]],
        prd_sources: list[dict[str, str]],
        prd_errors: list[str],
        progress_callback: Any | None,
    ) -> list[dict[str, Any]]:
        batches: list[dict[str, Any]] = []
        for index, chunk in enumerate(_split_text_for_token_limit(seatalk_history_text, MONTHLY_REPORT_BATCH_TARGET_TOKENS), start=1):
            batches.append({"source": "seatalk", "index": index, "payload": chunk})
        for index, chunk in enumerate(_split_json_items_for_token_limit(key_projects, MONTHLY_REPORT_BATCH_TARGET_TOKENS), start=1):
            batches.append({"source": "projects_jira", "index": index, "payload": chunk})
        for index, chunk in enumerate(_split_json_items_for_token_limit(prd_sources, MONTHLY_REPORT_BATCH_TARGET_TOKENS), start=1):
            batches.append({"source": "prd", "index": index, "payload": chunk})
        if not batches:
            batches.append({"source": "empty", "index": 1, "payload": "No readable monthly report evidence was found."})

        summaries: list[dict[str, Any]] = []
        total = len(batches)
        for current, batch in enumerate(batches, start=1):
            prompt = build_monthly_report_batch_prompt(
                template=template,
                generated_at=generated_at,
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
        batch_summaries: list[dict[str, Any]],
        prd_errors: list[str],
        progress_callback: Any | None,
    ) -> str:
        prompt = build_monthly_report_merge_prompt(
            generated_at=generated_at,
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

    def _seatalk_history(self) -> str:
        since = self.now - timedelta(days=MONTHLY_REPORT_SEATALK_DAYS)
        history = self.seatalk_service.export_history_since(
            since=since,
            now=self.now,
            days=MONTHLY_REPORT_SEATALK_DAYS + 2,
        )
        history = self.seatalk_service._filter_system_generated_history(history)
        return self.seatalk_service._compact_history_for_insights(
            history,
            max_chars=MONTHLY_REPORT_MAX_SEATALK_CHARS,
            signal_max_chars=320_000,
            recent_max_chars=160_000,
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

    @staticmethod
    def _merge_project_fields(project: dict[str, Any], raw_project: dict[str, Any]) -> None:
        for key in ("project_name", "market", "priority", "regional_pm_pic", "status", "release_date", "key_project_source"):
            value = str(raw_project.get(key) or "").strip()
            if value and not project.get(key):
                project[key] = value

    def _prd_sources(self, key_projects: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[str]]:
        if self.confluence is None:
            return [], []
        sources: list[dict[str, str]] = []
        errors: list[str] = []
        seen: set[str] = set()
        for project in key_projects:
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
        "Synthesize the last 30 days of SeaTalk history with Key Project Biz Project and Jira evidence. Prefer concrete project names, Jira IDs, decisions, risks, owners, and dates.\n"
        "Do not include raw transcripts, long PRD excerpts, tool logs, or confidential implementation chatter that is not needed for a monthly business report.\n\n"
        "# Output Rules\n"
        "- Return only the final Markdown draft.\n"
        "- Keep it suitable to send by email after light PM editing.\n"
        "- Follow the template headings unless the evidence clearly requires a small additional subsection.\n"
        "- If the configured template contains Markdown tables, preserve those table structures and fill rows from evidence; use TBD for missing cells instead of converting the table to bullets.\n"
        "- Include Jira IDs in parentheses when referencing concrete delivery items.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Monthly Report Template\n{normalize_monthly_report_template(template)}\n\n"
        "# Key Project / Jira Evidence\n"
        f"{_json_block(key_projects)}\n\n"
        "# PRD / Confluence Enrichment\n"
        f"{_json_block(prd_sources)}\n\n"
        "# PRD Enrichment Gaps\n"
        f"{_json_block(prd_errors)}\n\n"
        "# SeaTalk History From Previous 30 Days\n"
        f"{seatalk_history_text or 'No readable SeaTalk messages were found in the previous 30 days.'}"
    )


def build_monthly_report_batch_prompt(
    *,
    template: str,
    generated_at: datetime,
    source: str,
    payload: Any,
    prd_errors: list[str],
) -> str:
    source_label = _monthly_report_source_label(source)
    return (
        "# Task\n"
        f"Summarize one Monthly Report evidence batch from {source_label}.\n"
        "Do not write the final report. Extract only facts useful for the final monthly business report.\n"
        "Use concise Markdown with these headings exactly: Highlights, Decisions, Risks, Owners, Project References, Open Asks, Evidence Gaps.\n"
        "Preserve concrete project names, Jira IDs, owners, markets, dates, decisions, blockers, and launch/status facts.\n"
        "Do not include raw transcripts or long excerpts.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
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
    batch_summaries: list[dict[str, Any]],
    prd_errors: list[str],
) -> str:
    return (
        "# Task\n"
        "Merge Monthly Report batch summaries into one compact evidence brief for final drafting.\n"
        "Do not write the final report. Deduplicate repeated facts and keep the strongest concrete evidence.\n"
        "Use these headings exactly: Executive Themes, Key Project Progress, Delivery Evidence, Risks And Blockers, Decisions Needed, Evidence Gaps.\n"
        "Keep the brief concise enough for one final model call.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
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
        "Do not write the final report. Preserve concrete project names, Jira IDs, owners, dates, decisions, risks, and asks.\n"
        "Remove repetition and low-value detail. Return concise Markdown only.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        "# Evidence Brief\n"
        f"{evidence_brief}"
    )


def build_monthly_report_final_prompt(
    *,
    template: str,
    generated_at: datetime,
    evidence_brief: str,
) -> str:
    return (
        "# Task\n"
        "Generate Xiaodong Zheng's monthly team report as concise, business-ready Markdown.\n"
        "Use the configured template as the required structure. Do not invent facts; when evidence is weak, state the gap or mark as TBD.\n"
        "Use only the compact evidence brief below, which was produced from batched SeaTalk, Key Project, Jira, and PRD evidence.\n"
        "Do not include raw transcripts, long PRD excerpts, tool logs, or confidential implementation chatter that is not needed for a monthly business report.\n\n"
        "# Output Rules\n"
        "- Return only the final Markdown draft.\n"
        "- Keep it suitable to send by email after light PM editing.\n"
        "- Follow the template headings unless the evidence clearly requires a small additional subsection.\n"
        "- If the configured template contains Markdown tables, preserve those table structures and fill rows from evidence; use TBD for missing cells instead of converting the table to bullets.\n"
        "- Include Jira IDs in parentheses when referencing concrete delivery items.\n\n"
        f"# Generated At\n{generated_at.isoformat()}\n\n"
        f"# Monthly Report Template\n{normalize_monthly_report_template(template)}\n\n"
        "# Compact Evidence Brief\n"
        f"{evidence_brief or 'No readable evidence was found for this monthly report.'}"
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
        timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
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


def monthly_report_subject(now: datetime | None = None) -> str:
    local_now = (now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)).astimezone(SEATALK_INSIGHTS_TIMEZONE)
    return f"Monthly Report - {local_now.strftime('%Y-%m')}"


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
    table_style = "border-collapse:collapse;width:100%;margin:12px 0;"
    cell_style = "border:1px solid #111827;padding:6px 8px;text-align:left;vertical-align:top;"

    def render_cells(cells: list[str], tag: str) -> str:
        style = cell_style + ("font-weight:700;background:#f8fafc;" if tag == "th" else "")
        return "".join(
            f'<{tag} style="{style}">{_inline_markdown(cells[index] if index < len(cells) else "")}</{tag}>'
            for index in range(column_count)
        )

    body = "".join(f"<tr>{render_cells(row, 'td')}</tr>" for row in rows)
    return (
        f'<table style="{table_style}">'
        f"<thead><tr>{render_cells(headers, 'th')}</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )


def _compact_ticket(ticket: dict[str, Any]) -> dict[str, Any]:
    return {
        "jira_id": str(ticket.get("jira_id") or ticket.get("issue_id") or "").strip(),
        "jira_link": str(ticket.get("jira_link") or "").strip(),
        "jira_title": str(ticket.get("jira_title") or "").strip(),
        "pm_email": str(ticket.get("pm_email") or "").strip().lower(),
        "jira_status": str(ticket.get("jira_status") or "").strip(),
        "release_date": str(ticket.get("release_date") or "").strip(),
        "version": str(ticket.get("version") or "").strip(),
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
        "seatalk": "SeaTalk history",
        "projects_jira": "Key Projects and Jira",
        "prd": "PRD and Confluence",
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
