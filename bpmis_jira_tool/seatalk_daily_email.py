from __future__ import annotations

import argparse
import html
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials, credentials_from_payload, send_gmail_message
from bpmis_jira_tool.seatalk_dashboard import (
    SEATALK_DASHBOARD_DEFAULT_DAYS,
    SEATALK_INSIGHTS_TIMEZONE,
    SeaTalkDashboardService,
)


DEFAULT_RECIPIENT = "xiaodong.zheng@npt.sg"
DEFAULT_HOURS = 24


@dataclass(frozen=True)
class DailyEmailResult:
    status: str
    recipient: str
    subject: str
    run_date: str
    message_id: str = ""


class DailyEmailRunStore:
    def __init__(self, storage_path: Path) -> None:
        self.storage_path = storage_path

    def already_sent(self, *, run_date: str, recipient: str) -> bool:
        return self._key(run_date=run_date, recipient=recipient) in self._load().get("sent", {})

    def mark_sent(self, *, run_date: str, recipient: str, subject: str, message_id: str, sent_at: datetime) -> None:
        payload = self._load()
        sent = payload.setdefault("sent", {})
        sent[self._key(run_date=run_date, recipient=recipient)] = {
            "recipient": recipient,
            "subject": subject,
            "message_id": message_id,
            "sent_at": sent_at.isoformat(),
        }
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self.storage_path)

    def _load(self) -> dict[str, Any]:
        if not self.storage_path.exists():
            return {"sent": {}}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"sent": {}}
        return payload if isinstance(payload, dict) else {"sent": {}}

    @staticmethod
    def _key(*, run_date: str, recipient: str) -> str:
        return f"{run_date}:{recipient.strip().lower()}"


def data_root_from_settings(settings: Settings) -> Path:
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (Path(__file__).resolve().parent.parent / data_root).resolve()
    return data_root


def seatalk_name_overrides_path(*, data_root: Path) -> Path:
    local_agent_data_dir = str(os.getenv("LOCAL_AGENT_TEAM_PORTAL_DATA_DIR") or "").strip()
    if local_agent_data_dir:
        candidate = Path(local_agent_data_dir).expanduser() / "seatalk" / "name_overrides.json"
        if candidate.exists():
            return candidate
    return data_root / "seatalk" / "name_overrides.json"


def build_seatalk_service(settings: Settings, *, data_root: Path) -> SeaTalkDashboardService:
    return SeaTalkDashboardService(
        owner_email=settings.seatalk_owner_email,
        seatalk_app_path=settings.seatalk_local_app_path,
        seatalk_data_dir=settings.seatalk_local_data_dir,
        codex_workspace_root=Path(__file__).resolve().parent.parent,
        codex_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
        codex_timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        name_overrides_path=seatalk_name_overrides_path(data_root=data_root),
        daily_cache_dir=data_root / "seatalk" / "cache",
    )


def export_rolling_history(
    service: SeaTalkDashboardService,
    *,
    now: datetime,
    hours: int = DEFAULT_HOURS,
) -> str:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    since = local_now - timedelta(hours=max(1, int(hours)))
    days = max(SEATALK_DASHBOARD_DEFAULT_DAYS, int(hours / 24) + 2)
    return service.export_history_since(since=since, now=local_now, days=days)


def build_daily_briefing(
    service: SeaTalkDashboardService,
    *,
    now: datetime,
    hours: int = DEFAULT_HOURS,
) -> dict[str, Any]:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    history_text = service._filter_system_generated_history(export_rolling_history(service, now=local_now, hours=hours))
    if not any(line.startswith("[") for line in history_text.splitlines()):
        return {
            "project_updates": [],
            "other_updates": [],
            "my_todos": [],
            "team_member_reminders": [],
            "team_todos": [],
            "generated_at": local_now.isoformat(),
            "period_hours": hours,
        }
    history_text = service._compact_history_for_insights(
        history_text,
        max_chars=620_000,
        signal_max_chars=400_000,
        recent_max_chars=180_000,
    )
    _, parsed = service._run_codex_insights_prompt(
        system_prompt=(
            "You are an expert Digital Banking Product Manager helping Xiaodong Zheng prepare a clean, scannable Daily Brief email. "
            "Produce only valid JSON. Synthesize the raw SeaTalk logs into clear decisions, progress updates, and Xiaodong-owned action items. "
            "Do not copy-paste raw chat transcripts or conversational play-by-plays. "
            "Every item must retain traceability by ending its evidence field with a concise source label: a group ID/name or the key people involved. "
            "Prefer real names over UIDs whenever a real name is available in the raw text or mapping. Do not include tasks owned by other people."
        ),
        prompt=(
            "Return a JSON object with exactly these top-level keys: project_updates, other_updates, my_todos, team_member_reminders, team_todos.\n"
            "project_updates must be an array of objects with keys: domain, title, summary, status, evidence.\n"
            "other_updates must be an array of objects with keys: domain, title, summary, status, evidence.\n"
            "my_todos must be an array of objects with keys: task, domain, priority, due, evidence.\n"
            "team_member_reminders must be an array of objects with keys: domain, person, reminder, evidence.\n"
            "team_todos must always be an empty array.\n"
            "For every domain use exactly one of: Anti-fraud, Credit Risk, Ops Risk, General.\n"
            "Allowed status values: done, in_progress, blocked, unknown. Allowed priority values: high, medium, low, unknown.\n"
            "Use project_updates for updates from conversations where Xiaodong is involved, mentioned, directly asked, or clearly participating.\n"
            "Use other_updates for useful awareness signals from conversations where Xiaodong is not directly involved but the information may matter to a Digital Banking Product Manager, such as incidents, launches, policy/process changes, dependencies, leadership direction, risk/compliance signals, or cross-product milestones.\n"
            "Do not include generic chatter, greetings, meeting logistics with no decision, or low-value status noise in other_updates.\n"
            "Use team_member_reminders only when one of these people is mentioned by someone in a group conversation and neither that person nor Xiaodong follows up later in the available window: Ker Yin, Rene Chong, Sabrina Chan, Liye, Hui Xian, Sophia Wang Zijun, Ming Ming, Zoey Lu, Wang Chang, Jireh.\n"
            "For team_member_reminders, do not include private chats, do not include items where the named person replied/acknowledged/handled it later, and do not include items where Xiaodong already followed up. Write reminder as one concise sentence explaining what appears to need follow-up.\n"
            "For my_todos.task, write one synthesized action sentence, not a chat transcript. For due, extract a real date/deadline if present; otherwise use TBD.\n"
            "For project_updates.summary, write one synthesized sentence covering the update, decision, or milestone, not who said what.\n"
            "For other_updates.summary, write one synthesized sentence explaining why the signal may be useful awareness.\n"
            "For evidence, provide only the source label to show traceability, such as a group ID/name or key people involved. Prefer real names over UIDs. Do not include long snippets.\n"
            f"Window: previous {hours} hours. Generated at: {local_now.isoformat()}.\n\n"
            f"{history_text}"
        ),
    )
    return {
        "project_updates": parsed["project_updates"],
        "other_updates": parsed.get("other_updates", []),
        "my_todos": SeaTalkDashboardService._sort_todos(parsed["my_todos"]),
        "team_member_reminders": parsed.get("team_member_reminders", []),
        "team_todos": [],
        "generated_at": local_now.isoformat(),
        "period_hours": hours,
    }


def render_email(*, briefing: dict[str, Any], now: datetime) -> tuple[str, str, str]:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    subject = f"SeaTalk Daily Brief - {local_now.date().isoformat()}"
    todos = [item for item in briefing.get("my_todos") or [] if isinstance(item, dict)]
    updates = [item for item in briefing.get("project_updates") or [] if isinstance(item, dict)]
    other_updates = [item for item in briefing.get("other_updates") or [] if isinstance(item, dict)]
    reminders = [item for item in briefing.get("team_member_reminders") or [] if isinstance(item, dict)]
    text_lines = [
        f"Subject: {subject}",
        "",
        "To-do",
    ]
    if not todos:
        text_lines.append("- No clear Xiaodong-owned to-do found in the last 24 hours.")
    else:
        text_lines.extend(_render_grouped_text(todos, kind="todo"))
    text_lines.extend(["", "Product Update"])
    if not updates:
        text_lines.append("- No clear product update found in the last 24 hours.")
    else:
        text_lines.extend(_render_grouped_text(updates, kind="update"))
    text_lines.extend(["", "Other Update"])
    if not other_updates:
        text_lines.append("- No additional high-value awareness update found in the last 24 hours.")
    else:
        text_lines.extend(_render_grouped_text(other_updates, kind="update"))
    text_lines.extend(["", "Team Member Reminder"])
    if not reminders:
        text_lines.append("- No unresolved team-member mention found in the last 24 hours.")
    else:
        text_lines.extend(_render_grouped_text(reminders, kind="reminder"))
    text_body = "\n".join(text_lines).strip() + "\n"
    html_body = (
        "<html><body>"
        f"<h2>{html.escape(subject)}</h2>"
        "<h3>To-do</h3>"
        f"{_render_grouped_html(todos, kind='todo')}"
        "<h3>Product Update</h3>"
        f"{_render_grouped_html(updates, kind='update')}"
        "<h3>Other Update</h3>"
        f"{_render_grouped_html(other_updates, kind='other')}"
        "<h3>Team Member Reminder</h3>"
        f"{_render_grouped_html(reminders, kind='reminder')}"
        "</body></html>"
    )
    return subject, text_body, html_body


def send_daily_email(
    *,
    settings: Settings,
    recipient: str = DEFAULT_RECIPIENT,
    hours: int = DEFAULT_HOURS,
    now: datetime | None = None,
    force: bool = False,
    dry_run: bool = False,
    gmail_service: Any | None = None,
) -> DailyEmailResult:
    local_now = (now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)).astimezone(SEATALK_INSIGHTS_TIMEZONE)
    run_date = local_now.date().isoformat()
    data_root = data_root_from_settings(settings)
    run_store = DailyEmailRunStore(data_root / "seatalk" / "daily_email_runs.json")
    subject = f"SeaTalk Daily Brief - {run_date}"
    if not force and run_store.already_sent(run_date=run_date, recipient=recipient):
        return DailyEmailResult(status="skipped", recipient=recipient, subject=subject, run_date=run_date)
    service = build_seatalk_service(settings, data_root=data_root)
    briefing = build_daily_briefing(service, now=local_now, hours=hours)
    subject, text_body, html_body = render_email(briefing=briefing, now=local_now)
    if dry_run:
        return DailyEmailResult(status="dry_run", recipient=recipient, subject=subject, run_date=run_date)
    credential_store = StoredGoogleCredentials(
        data_root / "google" / "credentials.json",
        encryption_key=settings.team_portal_config_encryption_key,
    )
    owner_email = str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower()
    credentials_payload = credential_store.load(owner_email=owner_email)
    credentials = credentials_from_payload(credentials_payload)
    response = send_gmail_message(
        credentials=credentials,
        sender=owner_email,
        recipient=recipient,
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        gmail_service=gmail_service,
    )
    message_id = str((response or {}).get("id") or "")
    run_store.mark_sent(
        run_date=run_date,
        recipient=recipient,
        subject=subject,
        message_id=message_id,
        sent_at=local_now,
    )
    return DailyEmailResult(status="sent", recipient=recipient, subject=subject, run_date=run_date, message_id=message_id)


def _render_todo_text(item: dict[str, Any]) -> str:
    return (
        f"[{_display_priority(item.get('priority'))}] {_sentence_text(item.get('task'), 'Untitled')} "
        f"Due: {_display_due(item.get('due'))} (Source: {item.get('evidence') or 'Unknown'})"
    )


def _render_update_text(item: dict[str, Any]) -> str:
    return (
        f"{_sentence_text(item.get('summary') or item.get('title'), 'Untitled')} "
        f"[Status: {_display_status(item.get('status'))}] (Source: {item.get('evidence') or 'Unknown'})"
    )


def _render_reminder_text(item: dict[str, Any]) -> str:
    return (
        f"{item.get('person') or 'Unknown'}: {_sentence_text(item.get('reminder'), 'Follow-up may be needed')} "
        f"(Source: {item.get('evidence') or 'Unknown'})"
    )


def _domain_order(domain: str) -> tuple[int, str]:
    order = {"Ops Risk": 0, "Anti-fraud": 1, "Credit Risk": 2, "General": 3}
    clean = _display_domain(domain)
    return order.get(clean, 99), clean


def _display_domain(value: Any) -> str:
    text = str(value or "").strip()
    aliases = {
        "anti-fraud": "Anti-fraud",
        "anti fraud": "Anti-fraud",
        "credit risk": "Credit Risk",
        "ops risk": "Ops Risk",
        "general": "General",
    }
    return aliases.get(text.lower(), text or "General")


def _display_priority(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"high": "High", "medium": "Medium", "low": "Low", "unknown": "Unknown"}.get(text, str(value or "Unknown"))


def _display_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"done": "Done", "in_progress": "In Progress", "blocked": "Blocked", "unknown": "Unknown"}.get(text, str(value or "Unknown"))


def _display_due(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"unknown", "none", "n/a", "na"}:
        return "TBD"
    return text


def _sentence_text(value: Any, fallback: str) -> str:
    text = str(value or "").strip() or fallback
    return text if text.endswith((".", "!", "?")) else f"{text}."


def _group_items_by_domain(items: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        grouped.setdefault(_display_domain(item.get("domain")), []).append(item)
    return sorted(grouped.items(), key=lambda pair: _domain_order(pair[0]))


def _render_grouped_text(items: list[dict[str, Any]], *, kind: str) -> list[str]:
    lines: list[str] = []
    for domain, domain_items in _group_items_by_domain(items):
        lines.extend(["", domain])
        renderer = _renderer_for_kind(kind)
        lines.extend(renderer(item) for item in domain_items)
    return lines


def _render_grouped_html(items: list[dict[str, Any]], *, kind: str) -> str:
    if not items:
        fallback = (
            "No clear Xiaodong-owned to-do found in the last 24 hours."
            if kind == "todo"
            else (
                "No additional high-value awareness update found in the last 24 hours."
                if kind == "other"
                else (
                    "No unresolved team-member mention found in the last 24 hours."
                    if kind == "reminder"
                    else "No clear product update found in the last 24 hours."
                )
            )
        )
        return f"<p>{html.escape(fallback)}</p>"
    sections: list[str] = []
    renderer = _renderer_for_kind(kind)
    for domain, domain_items in _group_items_by_domain(items):
        rows = "".join(f"<li>{html.escape(renderer(item))}</li>" for item in domain_items)
        sections.append(f"<h4>{html.escape(domain)}</h4><ul>{rows}</ul>")
    return "".join(sections)


def _renderer_for_kind(kind: str):
    if kind == "todo":
        return _render_todo_text
    if kind == "reminder":
        return _render_reminder_text
    return _render_update_text


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send the daily SeaTalk briefing email.")
    parser.add_argument("--recipient", default=DEFAULT_RECIPIENT)
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--now", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.fromisoformat(args.now).astimezone(SEATALK_INSIGHTS_TIMEZONE) if args.now else None
    result = send_daily_email(
        settings=Settings.from_env(),
        recipient=args.recipient,
        hours=args.hours,
        now=now,
        force=args.force,
        dry_run=args.dry_run,
    )
    print(json.dumps(result.__dict__, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
