from __future__ import annotations

import argparse
import html
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import re

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE, GmailDashboardService
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials, credentials_from_payload, send_gmail_message
from bpmis_jira_tool.seatalk_dashboard import (
    SEATALK_DASHBOARD_DEFAULT_DAYS,
    SEATALK_INSIGHTS_TIMEZONE,
    SeaTalkDashboardService,
)


DEFAULT_RECIPIENT = "xiaodong.zheng@npt.sg"
DEFAULT_HOURS = 24
MAX_MY_TODOS = 8
MAX_PROJECT_UPDATES = 10
MAX_OTHER_UPDATES = 8
MAX_TEAM_MEMBER_REMINDERS = 8
MAX_USEFUL_AWARENESS_OTHER_UPDATES = 5
ALLOWED_OTHER_UPDATE_SIGNAL_TYPES = {
    "incident",
    "launch",
    "policy_process",
    "risk_compliance",
    "cross_team_dependency",
    "leadership_decision",
    "cross_product_milestone",
    "useful_awareness",
}
STRONG_OTHER_UPDATE_SIGNAL_TYPES = ALLOWED_OTHER_UPDATE_SIGNAL_TYPES - {"useful_awareness"}
BOT_SOURCE_HINTS = (
    "bot",
    "robot",
    "机器人",
    "jira_confluence_support",
    "jira confluence support",
    "calendar-notification",
    "notification",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
    "system",
    "workflow",
)
BOT_ALERT_REMINDER_HINTS = (
    "alert",
    "reminder",
    "提醒",
    "告警",
    "notification",
    "automated",
    "auto-generated",
    "system generated",
)
TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE = {
    "keryin": "Ker Yin",
    "ker yin": "Ker Yin",
    "rene": "Rene Chong",
    "renee": "Rene Chong",
    "rene chong": "Rene Chong",
    "sabrina": "Sabrina Chan",
    "sabrina chan": "Sabrina Chan",
    "li ye": "Liye",
    "liye": "Liye",
    "huixian": "Hui Xian",
    "hui xian": "Hui Xian",
    "sophia": "Sophia Wang Zijun",
    "sophia wang": "Sophia Wang Zijun",
    "wang zijun": "Sophia Wang Zijun",
    "sophia wang zijun": "Sophia Wang Zijun",
    "mingming": "Ming Ming",
    "ming ming": "Ming Ming",
    "zoey": "Zoey Lu",
    "zoey lu": "Zoey Lu",
    "chang": "Wang Chang",
    "wang chang": "Wang Chang",
    "jireh": "Jireh",
}
ANTI_FRAUD_TEAM_MEMBERS = {
    "ker yin",
    "rene chong",
    "zoey lu",
    "wang chang",
    "jireh",
}
TEAM_MEMBER_REMINDER_DOMAIN_OVERRIDES = {
    "sophia wang zijun": "Credit Risk",
}
RAW_SEATALK_ID_PATTERN = re.compile(r"\b(?:group|buddy)-\d+\b|\bUID\s+\d+\b", re.IGNORECASE)


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


def export_rolling_gmail_threads(
    service: GmailDashboardService,
    *,
    now: datetime,
    hours: int = DEFAULT_HOURS,
) -> str:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    since = local_now - timedelta(hours=max(1, int(hours)))
    return service.export_thread_history_since(since=since, now=local_now)


def ensure_gmail_daily_scopes(credentials_payload: dict[str, Any]) -> None:
    scopes = {str(scope).strip() for scope in (credentials_payload.get("scopes") or []) if str(scope).strip()}
    missing = [scope for scope in (GMAIL_READONLY_SCOPE, "https://www.googleapis.com/auth/gmail.send") if scope not in scopes]
    if missing:
        raise ConfigError("Gmail daily brief permission is missing. Reconnect Google once to grant Gmail read and send access.")


def build_daily_briefing(
    service: SeaTalkDashboardService,
    *,
    now: datetime,
    hours: int = DEFAULT_HOURS,
    gmail_history_text: str = "",
) -> dict[str, Any]:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    history_text = service._filter_system_generated_history(export_rolling_history(service, now=local_now, hours=hours))
    gmail_history_text = str(gmail_history_text or "").strip()
    seatalk_has_messages = any(line.startswith("[") for line in history_text.splitlines())
    gmail_has_messages = any(line.startswith("Message ") for line in gmail_history_text.splitlines())
    if not seatalk_has_messages and not gmail_has_messages:
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
    if gmail_history_text:
        gmail_history_text = gmail_history_text[:360_000]
    _, parsed = service._run_codex_insights_prompt(
        system_prompt=_daily_brief_system_prompt(),
        prompt=_daily_brief_user_prompt(
            history_text=history_text,
            gmail_history_text=gmail_history_text,
            hours=hours,
            local_now=local_now,
        ),
    )
    name_mappings = _load_seatalk_name_mappings(service)
    project_updates = _dedupe_brief_items(_normalize_brief_items(parsed.get("project_updates", []), name_mappings=name_mappings))[:MAX_PROJECT_UPDATES]
    other_updates = _dedupe_brief_items(_filter_other_updates(_normalize_brief_items(parsed.get("other_updates", []), name_mappings=name_mappings)))[
        :MAX_OTHER_UPDATES
    ]
    my_todos = _dedupe_brief_items(_normalize_brief_items(parsed.get("my_todos", []), name_mappings=name_mappings), text_fields=("task",))[:MAX_MY_TODOS]
    reminders = _dedupe_brief_items(
        _filter_seatalk_reminders(
            _normalize_brief_items(parsed.get("team_member_reminders", []), default_source_type="seatalk", name_mappings=name_mappings)
        ),
        text_fields=("person", "reminder"),
    )[:MAX_TEAM_MEMBER_REMINDERS]
    return {
        "project_updates": project_updates,
        "other_updates": other_updates,
        "my_todos": SeaTalkDashboardService._sort_todos(my_todos),
        "team_member_reminders": reminders,
        "team_todos": [],
        "generated_at": local_now.isoformat(),
        "period_hours": hours,
    }


def render_email(*, briefing: dict[str, Any], now: datetime) -> tuple[str, str, str]:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    subject = f"Daily Brief - {local_now.date().isoformat()}"
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
        text_lines.append("- No clear Xiaodong-owned to-do found in the briefing window.")
    else:
        text_lines.extend(_render_grouped_text(todos, kind="todo"))
    text_lines.extend(["", "Project Updates"])
    if not updates:
        text_lines.append("- No clear project update found in the briefing window.")
    else:
        text_lines.extend(_render_grouped_text(updates, kind="update"))
    text_lines.extend(["", "Other Update"])
    if not other_updates:
        text_lines.append("- No additional high-value awareness update found in the briefing window.")
    else:
        text_lines.extend(_render_grouped_text(other_updates, kind="update"))
    text_lines.extend(["", "Team Member Reminder"])
    if not reminders:
        text_lines.append("- No unresolved SeaTalk team-member mention found in the briefing window.")
    else:
        text_lines.extend(_render_grouped_text(reminders, kind="reminder"))
    text_body = "\n".join(text_lines).strip() + "\n"
    html_body = (
        "<html><body>"
        f"<h2>{html.escape(subject)}</h2>"
        "<h3>To-do</h3>"
        f"{_render_grouped_html(todos, kind='todo')}"
        "<h3>Project Updates</h3>"
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
    subject = f"Daily Brief - {run_date}"
    if not force and run_store.already_sent(run_date=run_date, recipient=recipient):
        return DailyEmailResult(status="skipped", recipient=recipient, subject=subject, run_date=run_date)
    credential_store = StoredGoogleCredentials(
        data_root / "google" / "credentials.json",
        encryption_key=settings.team_portal_config_encryption_key,
    )
    owner_email = str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower()
    credentials_payload = credential_store.load(owner_email=owner_email)
    ensure_gmail_daily_scopes(credentials_payload)
    credentials = credentials_from_payload(credentials_payload)
    service = build_seatalk_service(settings, data_root=data_root)
    gmail_brief_service = GmailDashboardService(credentials=credentials, gmail_service=gmail_service, cache_key=owner_email)
    gmail_history_text = export_rolling_gmail_threads(gmail_brief_service, now=local_now, hours=hours)
    briefing = build_daily_briefing(service, now=local_now, hours=hours, gmail_history_text=gmail_history_text)
    subject, text_body, html_body = render_email(briefing=briefing, now=local_now)
    if dry_run:
        return DailyEmailResult(status="dry_run", recipient=recipient, subject=subject, run_date=run_date)
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


def _daily_brief_system_prompt() -> str:
    return (
        "You are an expert Digital Banking Product Manager preparing Xiaodong Zheng's Daily Brief. "
        "Return only valid JSON. Synthesize SeaTalk logs and Gmail threads into clear actions, project updates, awareness updates, and unresolved team-member reminders. "
        "Do not copy raw transcripts or write conversational play-by-plays. Prefer precise, concise, business-readable sentences. "
        "Every item must keep traceability through a short evidence field. Prefer real names over UIDs whenever names are available."
    )


def _daily_brief_user_prompt(
    *,
    history_text: str,
    gmail_history_text: str,
    hours: int,
    local_now: datetime,
) -> str:
    return (
        "## Output Contract\n"
        "Return a JSON object with exactly these top-level keys: project_updates, other_updates, my_todos, team_member_reminders, team_todos.\n"
        "project_updates: array of objects with keys domain, title, summary, status, evidence, source_type.\n"
        "other_updates: array of objects with keys domain, title, summary, status, evidence, source_type, signal_type.\n"
        "my_todos: array of objects with keys task, domain, priority, due, evidence, source_type.\n"
        "team_member_reminders: array of objects with keys domain, person, reminder, evidence, source_type.\n"
        "team_todos must always be an empty array.\n\n"
        "## Allowed Values\n"
        "domain: Anti-fraud, Credit Risk, Ops Risk, General.\n"
        "status: done, in_progress, blocked, unknown.\n"
        "priority: high, medium, low, unknown.\n"
        "source_type: seatalk, gmail, mixed. Use mixed only when one synthesized item is supported by both SeaTalk and Gmail.\n"
        "other_updates.signal_type: incident, launch, policy_process, risk_compliance, cross_team_dependency, leadership_decision, cross_product_milestone, useful_awareness.\n"
        "If an other_updates item is useful but does not fit a stronger signal type, set signal_type to useful_awareness. Do not omit signal_type.\n\n"
        "## Section Rules\n"
        "my_todos: include only Xiaodong-owned actions, decisions needed from Xiaodong, or follow-ups Xiaodong clearly needs to drive. Do not include tasks owned by other people. Max 8 items. Sort high priority first, then earliest due date, then most actionable.\n"
        "project_updates: include updates from SeaTalk or Gmail where Xiaodong is involved, mentioned, directly asked, or clearly participating. Summarize the decision, milestone, blocker, or current state. Max 10 items. Sort blocked and in_progress before done.\n"
        "other_updates: include useful awareness from SeaTalk or Gmail where Xiaodong is not directly involved but the information may matter to a Digital Banking PM. Prioritize incident, launch, policy/process, risk/compliance, cross-team dependency, leadership decision, and cross-product milestone. Include at most 5 useful_awareness items and at most 8 other_updates total. Do not include generic chatter, greetings, pure thanks, meeting logistics with no decision, or low-value FYI.\n"
        "team_member_reminders: use SeaTalk only. Never create these from Gmail. Only include people from the explicit allowed reminder list below. Max 8 items. Sort by most actionable first.\n\n"
        "## Team Member Reminder Scan\n"
        "Before writing team_member_reminders, scan every SeaTalk group conversation for human mentions of these people: Ker Yin, Rene Chong, Sabrina Chan, Liye, Hui Xian, Sophia Wang Zijun, Ming Ming, Zoey Lu, Wang Chang, Jireh.\n"
        "Sophia Wang Zijun belongs to Credit Risk. Do not classify Sophia Wang Zijun as Ops Risk.\n"
        "For Anti-fraud domain reminders, only these people are Xiaodong's Anti-fraud team: Ker Yin, Rene Chong, Zoey Lu, Wang Chang, Jireh. Do not put anyone else, including Wendy, under Anti-fraud team_member_reminders.\n"
        "Do not create team_member_reminders for people outside the allowed reminder list, even if they appear in SeaTalk.\n"
        "A valid reminder exists when a human in a SeaTalk group asks, mentions, assigns, blocks on, or appears to need follow-up from one of those people, and neither the named person nor Xiaodong follows up later in that same group during the available window.\n"
        "Mentions may appear as direct @ mentions, plain names, mapped display names, name variants, or quoted text. Prefer real names in the person field.\n"
        "Do not include private chats. Do not include bot/system alerts or automated reminders. Do not include items where the named person replied, acknowledged, handled it, or Xiaodong already followed up later.\n"
        "If the mention looks human and action-relevant but you are unsure whether a later follow-up resolved it, include one concise reminder rather than dropping it. Set source_type to seatalk.\n\n"
        "## Source And Evidence Rules\n"
        "For SeaTalk evidence, use the group ID/name or the key people involved. For Gmail evidence, use sender or key participants plus subject and thread link when available.\n"
        "Do not output unresolved raw SeaTalk IDs such as group-123, buddy-123, or UID 123 in evidence. Use mapped display names when visible; otherwise use a generic label such as SeaTalk group or SeaTalk contact.\n"
        "For Gmail thread messages marked context only, use them only to understand the in-window message; never summarize context-only messages as new To-do, Project Updates, or Other Updates.\n"
        "Merge duplicate items across SeaTalk and Gmail when they refer to the same project, owner, decision, task, or milestone. Keep one synthesized item and use source_type mixed when both sources support it.\n\n"
        "## Exclusions\n"
        "For other_updates and team_member_reminders, ignore bot-generated alerts, automated reminders, system notifications, Jira/Confluence/calendar reminders, and no-reply notification emails unless a human adds meaningful follow-up in the same thread.\n\n"
        "## Formatting Inside JSON\n"
        "For my_todos.task, write one synthesized action sentence. For due, extract a real deadline if present; otherwise use TBD.\n"
        "For project_updates.summary and other_updates.summary, write one synthesized sentence, not a transcript.\n"
        "For evidence, provide only the source label. Do not include long snippets.\n\n"
        f"Window: previous {hours} hours. Generated at: {local_now.isoformat()}.\n\n"
        "=== SeaTalk history ===\n"
        f"{history_text}\n\n"
        "=== Gmail thread history ===\n"
        f"{gmail_history_text or 'No Gmail messages were found in this window.'}"
    )


def _load_seatalk_name_mappings(service: Any) -> dict[str, str]:
    path = getattr(service, "name_overrides_path", None)
    if not path:
        return {}
    try:
        payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    mappings = payload.get("mappings") if isinstance(payload, dict) and "mappings" in payload else payload
    if not isinstance(mappings, dict):
        return {}
    normalized: dict[str, str] = {}
    for raw_key, raw_name in mappings.items():
        name = " ".join(str(raw_name or "").split())
        if not name:
            continue
        for key in _seatalk_mapping_equivalent_keys(raw_key):
            normalized[key.lower()] = name[:180]
    return normalized


def _seatalk_mapping_equivalent_keys(value: Any) -> set[str]:
    key = str(value or "").strip()
    if key.startswith("group-"):
        return {key}
    if key.startswith("buddy-"):
        suffix = key.removeprefix("buddy-").strip()
        return {key, f"UID {suffix}"} if suffix else {key}
    uid_match = re.match(r"^UID\s+(.+)$", key, re.IGNORECASE)
    if uid_match and uid_match.group(1).strip():
        suffix = uid_match.group(1).strip()
        return {f"UID {suffix}", f"buddy-{suffix}"}
    return set()


def _sanitize_seatalk_evidence(value: Any, *, name_mappings: dict[str, str] | None = None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    mappings = {str(key).lower(): str(name) for key, name in (name_mappings or {}).items() if str(name).strip()}
    saw_unmapped_group = False
    saw_unmapped_contact = False

    def replace_raw_id(match: re.Match[str]) -> str:
        nonlocal saw_unmapped_group, saw_unmapped_contact
        raw = match.group(0).strip()
        mapped = mappings.get(raw.lower())
        if mapped:
            return mapped
        equivalent_keys = _seatalk_mapping_equivalent_keys(raw)
        mapped = next((mappings.get(key.lower()) for key in equivalent_keys if mappings.get(key.lower())), "")
        if mapped:
            return mapped
        if raw.lower().startswith("group-"):
            saw_unmapped_group = True
            return "SeaTalk group"
        saw_unmapped_contact = True
        return "SeaTalk contact"

    cleaned = RAW_SEATALK_ID_PATTERN.sub(replace_raw_id, text)
    cleaned = re.sub(r"\b(SeaTalk group)(?:\s*[,;/]\s*\1)+\b", r"\1", cleaned)
    cleaned = re.sub(r"\b(SeaTalk contact)(?:\s*[,;/]\s*\1)+\b", r"\1", cleaned)
    cleaned = " ".join(cleaned.split())
    if not cleaned or RAW_SEATALK_ID_PATTERN.fullmatch(text):
        if saw_unmapped_group and saw_unmapped_contact:
            return "SeaTalk conversation"
        if saw_unmapped_group:
            return "SeaTalk group"
        if saw_unmapped_contact:
            return "SeaTalk contact"
    return cleaned or "SeaTalk conversation"


def _normalize_brief_items(
    items: Any,
    *,
    default_source_type: str = "unknown",
    name_mappings: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    if not isinstance(items, list):
        return normalized
    for item in items:
        if not isinstance(item, dict):
            continue
        clean = dict(item)
        clean["evidence"] = _sanitize_seatalk_evidence(clean.get("evidence"), name_mappings=name_mappings)
        clean["source_type"] = _normalize_source_type(clean.get("source_type"), clean.get("evidence"), default_source_type=default_source_type)
        if "signal_type" in clean:
            clean["signal_type"] = _normalize_signal_type(clean.get("signal_type"))
        normalized.append(clean)
    return normalized


def _filter_seatalk_reminders(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in items:
        if item.get("source_type") != "seatalk" or _is_bot_alert_or_reminder_item(item):
            continue
        canonical_person = _canonical_team_member_name(item.get("person"))
        if not canonical_person:
            continue
        domain = TEAM_MEMBER_REMINDER_DOMAIN_OVERRIDES.get(_normalize_person_key(canonical_person), _display_domain(item.get("domain")))
        if domain == "Anti-fraud" and _normalize_person_key(canonical_person) not in ANTI_FRAUD_TEAM_MEMBERS:
            continue
        item["person"] = canonical_person
        item["domain"] = domain
        filtered.append(item)
    return filtered


def _filter_other_updates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    strong: list[dict[str, Any]] = []
    useful_awareness: list[dict[str, Any]] = []
    for item in items:
        if _is_bot_alert_or_reminder_item(item):
            continue
        raw_signal_type = str(item.get("signal_type") or "").strip()
        signal_type = _normalize_signal_type(raw_signal_type)
        if signal_type not in ALLOWED_OTHER_UPDATE_SIGNAL_TYPES:
            if raw_signal_type:
                continue
            signal_type = "useful_awareness"
        item["signal_type"] = signal_type
        if signal_type in STRONG_OTHER_UPDATE_SIGNAL_TYPES:
            strong.append(item)
        elif signal_type == "useful_awareness" and len(useful_awareness) < MAX_USEFUL_AWARENESS_OTHER_UPDATES:
            useful_awareness.append(item)
    return strong + useful_awareness


def _is_bot_alert_or_reminder_item(item: dict[str, Any]) -> bool:
    evidence = str(item.get("evidence") or "").lower()
    title = str(item.get("title") or "").lower()
    summary = str(item.get("summary") or "").lower()
    reminder = str(item.get("reminder") or "").lower()
    person = str(item.get("person") or "").lower()
    combined = " ".join([evidence, title, summary, reminder, person])
    source_looks_bot = any(hint in evidence for hint in BOT_SOURCE_HINTS) or any(
        hint in combined for hint in (" bot ", "[bot]", "(bot)", "机器人")
    )
    alert_or_reminder = any(hint in combined for hint in BOT_ALERT_REMINDER_HINTS)
    if source_looks_bot and alert_or_reminder:
        return True
    return any(
        phrase in combined
        for phrase in (
            "automated alert",
            "automated reminder",
            "system alert",
            "system reminder",
            "bot alert",
            "bot reminder",
        )
    )


def _canonical_team_member_name(value: Any) -> str:
    key = _normalize_person_key(value)
    if key in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE:
        return TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE[key]
    return ""


def _normalize_person_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\buid\b\s*[:#-]?\s*\d+\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _dedupe_brief_items(items: list[dict[str, Any]], *, text_fields: tuple[str, ...] = ("title", "summary")) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for item in items:
        key = _dedupe_key(item, text_fields=text_fields)
        if not key:
            key = f"fallback:{len(order)}"
        if key not in deduped:
            deduped[key] = dict(item)
            order.append(key)
            continue
        existing = deduped[key]
        existing["evidence"] = _merge_evidence(existing.get("evidence"), item.get("evidence"))
        existing["source_type"] = _merge_source_type(existing.get("source_type"), item.get("source_type"))
        if not existing.get("signal_type") and item.get("signal_type"):
            existing["signal_type"] = item.get("signal_type")
    return [deduped[key] for key in order]


def _normalize_source_type(value: Any, evidence: Any, *, default_source_type: str = "unknown") -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"seatalk", "gmail", "mixed"}:
        return text
    evidence_text = str(evidence or "").strip().lower()
    if "mail.google.com" in evidence_text or "thread id" in evidence_text or "gmail" in evidence_text:
        return "gmail"
    if evidence_text.startswith("group-") or " group" in evidence_text or "uid " in evidence_text:
        return "seatalk"
    return default_source_type if default_source_type in {"seatalk", "gmail", "mixed", "unknown"} else "unknown"


def _normalize_signal_type(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "policy": "policy_process",
        "process": "policy_process",
        "policy_change": "policy_process",
        "process_change": "policy_process",
        "risk": "risk_compliance",
        "compliance": "risk_compliance",
        "dependency": "cross_team_dependency",
        "milestone": "cross_product_milestone",
        "leadership": "leadership_decision",
        "decision": "leadership_decision",
    }
    return aliases.get(text, text)


def _merge_evidence(left: Any, right: Any) -> str:
    parts: list[str] = []
    for value in (left, right):
        text = str(value or "").strip()
        if text and text not in parts:
            parts.append(text)
    return "; ".join(parts) or "Unknown"


def _merge_source_type(left: Any, right: Any) -> str:
    values = {str(value or "").strip().lower() for value in (left, right) if str(value or "").strip()}
    values.discard("unknown")
    if len(values) > 1 or "mixed" in values:
        return "mixed"
    return next(iter(values), "unknown")


def _dedupe_key(item: dict[str, Any], *, text_fields: tuple[str, ...]) -> str:
    domain = _display_domain(item.get("domain")).lower()
    pieces = [str(item.get(field) or "") for field in text_fields]
    if text_fields == ("title", "summary") and not any(piece.strip() for piece in pieces):
        pieces = [str(item.get("task") or ""), str(item.get("reminder") or "")]
    normalized = _normalize_dedupe_text(" ".join(pieces))
    if not normalized:
        return ""
    return f"{domain}:{normalized}"


def _normalize_dedupe_text(value: str) -> str:
    text = str(value or "").lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\b(source|status|due|tbd|done|blocked|in progress|unknown)\b", " ", text)
    tokens = re.findall(r"[a-z0-9]+", text)
    stopwords = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "to",
        "for",
        "of",
        "on",
        "in",
        "is",
        "are",
        "was",
        "were",
        "please",
        "follow",
        "up",
    }
    useful = [token for token in tokens if token not in stopwords]
    return " ".join(useful[:16])


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
            "No clear Xiaodong-owned to-do found in the briefing window."
            if kind == "todo"
            else (
                "No additional high-value awareness update found in the briefing window."
                if kind == "other"
                else (
                    "No unresolved SeaTalk team-member mention found in the briefing window."
                    if kind == "reminder"
                    else "No clear project update found in the briefing window."
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
    parser = argparse.ArgumentParser(description="Send the daily SeaTalk + Gmail briefing email.")
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
