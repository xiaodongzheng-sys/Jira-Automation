from __future__ import annotations

import html
import os
import re
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
    ) -> dict[str, Any]:
        effective_template = normalize_monthly_report_template(template)
        key_projects = self._key_projects(team_payloads)
        history_text = self._seatalk_history()
        prd_sources, prd_errors = self._prd_sources(key_projects)
        prompt = build_monthly_report_prompt(
            template=effective_template,
            generated_at=self.now,
            seatalk_history_text=history_text,
            key_projects=key_projects,
            prd_sources=prd_sources,
            prd_errors=prd_errors,
        )
        generated = generate_monthly_report_with_codex(
            prompt=prompt,
            settings=self.settings,
            workspace_root=self.workspace_root,
        )
        return {
            "status": "ok",
            "draft_markdown": generated["result_markdown"],
            "generated_at": self.now.isoformat(),
            "model_id": generated["model_id"],
            "trace": generated["trace"],
            "evidence_summary": {
                "seatalk_days": MONTHLY_REPORT_SEATALK_DAYS,
                "key_project_count": len(key_projects),
                "jira_ticket_count": sum(len(project.get("jira_tickets") or []) for project in key_projects),
                "prd_page_count": len(prd_sources),
                "prd_error_count": len(prd_errors),
            },
        }

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


def generate_monthly_report_with_codex(
    *,
    prompt: str,
    settings: Settings,
    workspace_root: Path,
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
            "codex_prompt_mode": MONTHLY_REPORT_PROMPT_VERSION,
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
