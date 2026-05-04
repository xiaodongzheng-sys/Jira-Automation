from __future__ import annotations

import argparse
import html
import json
import os
import signal
import threading
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any
import re

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ConfigError
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE, GmailDashboardService
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials, credentials_from_payload, send_gmail_message
from bpmis_jira_tool.report_intelligence import (
    build_daily_match_summary,
    filter_text_by_noise,
    key_project_candidates_from_team_config,
    load_report_intelligence_config_from_data_root,
    load_team_dashboard_config_from_data_root,
    match_report_intelligence,
    normalize_report_intelligence_config,
)
from bpmis_jira_tool.seatalk_dashboard import (
    SEATALK_DASHBOARD_DEFAULT_DAYS,
    SEATALK_INSIGHTS_TIMEZONE,
    SeaTalkDashboardService,
)
from bpmis_jira_tool.trello_daily_summary import (
    TrelloCardSpec,
    TrelloDailySummaryClient,
    TrelloDailySummaryStore,
    TrelloSyncResult,
    fingerprint_daily_card,
)


DEFAULT_RECIPIENT = "xiaodong.zheng@npt.sg"
DEFAULT_HOURS = 24
MORNING_SLOT = "morning"
MIDDAY_SLOT = "midday"
LEGACY_SLOT = "daily"
DAILY_EMAIL_SLOTS = {MORNING_SLOT, MIDDAY_SLOT}
DAILY_EMAIL_WEEKDAY_RUNS = {0, 1, 2, 3, 4}
GMAIL_EXPORT_TIMEOUT_SECONDS = 90
MAX_MY_TODOS = 8
MAX_PROJECT_UPDATES = 10
MAX_OTHER_UPDATES = 8
MAX_TEAM_MEMBER_REMINDERS = 8
MAX_USEFUL_AWARENESS_OTHER_UPDATES = 5
MAX_TOP_FOCUS_ITEMS = 3
LOW_SIGNAL_EMAIL_SUMMARY = "No clear action, blocker, key project update, or team follow-up was found in this window."
EMPTY_TODO_SECTION_SUMMARY = "No Xiaodong-owned action or watch/delegate item found."
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
TODO_ACTION_TYPES = {"direct_action", "watch_delegate"}
WATCH_DELEGATE_HINTS = (
    "ensure ",
    "follow up with",
    "check with",
    "monitor",
    "confirm team",
    "confirm with",
    "make sure",
)
DIRECT_ACTION_HINTS = (
    "answer",
    "review",
    "attend",
    "drive",
    "decide",
    "approve",
    "reply",
    "send",
    "prepare",
    "provide",
    "join",
)
PENDING_STATUS_HINTS = (
    "pending confirmation",
    "still pending",
    "tomorrow clarify",
    "tomorrow's location meeting",
    "no fixed date",
    "not fixed",
    "awaiting confirmation",
)
RISK_BLOCKED_HINTS = (
    "blocked",
    "mas",
    "launching before",
    "launch before",
    "risk endorsement",
    "itc endorsement",
    "real-time fraud surveillance",
    "without real-time",
)


@dataclass(frozen=True)
class DailyEmailResult:
    status: str
    recipient: str
    subject: str
    run_date: str
    run_slot: str = LEGACY_SLOT
    window_start: str = ""
    window_end: str = ""
    message_id: str = ""
    trello_status: str = "skipped"
    trello_created_count: int = 0
    trello_skipped_count: int = 0
    trello_cards: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class DailyEmailWindow:
    run_date: str
    run_slot: str
    start: datetime
    end: datetime

    @property
    def label(self) -> str:
        return f"{_format_window_endpoint(self.start)} - {_format_window_endpoint(self.end)}"


class DailyEmailRunStore:
    def __init__(self, storage_path: Path) -> None:
        self.storage_path = storage_path

    def already_sent(self, *, run_date: str, recipient: str, run_slot: str = LEGACY_SLOT) -> bool:
        return self._key(run_date=run_date, recipient=recipient, run_slot=run_slot) in self._load().get("sent", {})

    def mark_sent(
        self,
        *,
        run_date: str,
        recipient: str,
        subject: str,
        message_id: str,
        sent_at: datetime,
        run_slot: str = LEGACY_SLOT,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> None:
        payload = self._load()
        sent = payload.setdefault("sent", {})
        sent[self._key(run_date=run_date, recipient=recipient, run_slot=run_slot)] = {
            "recipient": recipient,
            "subject": subject,
            "message_id": message_id,
            "sent_at": sent_at.isoformat(),
            "run_slot": run_slot,
            "window_start": window_start.isoformat() if window_start else "",
            "window_end": window_end.isoformat() if window_end else "",
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
    def _key(*, run_date: str, recipient: str, run_slot: str = LEGACY_SLOT) -> str:
        return f"{run_date}:{run_slot}:{recipient.strip().lower()}"


def resolve_daily_email_window(*, now: datetime, slot: str = "auto") -> DailyEmailWindow:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    normalized_slot = str(slot or "auto").strip().lower()
    if normalized_slot == "auto":
        normalized_slot = MIDDAY_SLOT if local_now >= _local_datetime(local_now.date(), 13) else MORNING_SLOT
    if normalized_slot == MIDDAY_SLOT:
        start = _local_datetime(local_now.date(), 8)
        end = _local_datetime(local_now.date(), 13)
    elif normalized_slot == MORNING_SLOT:
        previous_report_day_offset = 3 if local_now.weekday() == 0 else 1
        start = _local_datetime(local_now.date() - timedelta(days=previous_report_day_offset), 13)
        end = _local_datetime(local_now.date(), 8)
    else:
        raise ConfigError(f"Unsupported daily email slot: {slot}. Use auto, morning, or midday.")
    return DailyEmailWindow(
        run_date=local_now.date().isoformat(),
        run_slot=normalized_slot,
        start=start,
        end=end,
    )


def should_skip_fixed_daily_email_window(*, now: datetime) -> bool:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    return local_now.weekday() not in DAILY_EMAIL_WEEKDAY_RUNS


def _local_datetime(value: Any, hour: int) -> datetime:
    return datetime.combine(value, time(hour=hour), tzinfo=SEATALK_INSIGHTS_TIMEZONE)


def _format_window_endpoint(value: datetime) -> str:
    local_value = value.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    return local_value.strftime("%Y-%m-%d %H:%M")


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


def export_window_history(
    service: SeaTalkDashboardService,
    *,
    window_start: datetime,
    window_end: datetime,
) -> str:
    local_start = window_start.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    local_end = window_end.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    span_days = max(1, (local_end.date() - local_start.date()).days + 1)
    days = max(SEATALK_DASHBOARD_DEFAULT_DAYS, span_days + 1)
    return service.export_history_since(since=local_start, now=local_end, days=days)


def export_rolling_gmail_threads(
    service: GmailDashboardService,
    *,
    now: datetime,
    hours: int = DEFAULT_HOURS,
) -> str:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    since = local_now - timedelta(hours=max(1, int(hours)))
    return service.export_thread_history_since(since=since, now=local_now)


def export_window_gmail_threads(
    service: GmailDashboardService,
    *,
    window_start: datetime,
    window_end: datetime,
) -> str:
    return service.export_thread_history_since(
        since=window_start.astimezone(SEATALK_INSIGHTS_TIMEZONE),
        now=window_end.astimezone(SEATALK_INSIGHTS_TIMEZONE),
    )


def _gmail_export_timeout_seconds() -> int:
    raw_value = str(os.getenv("DAILY_EMAIL_GMAIL_EXPORT_TIMEOUT_SECONDS") or "").strip()
    if not raw_value:
        return GMAIL_EXPORT_TIMEOUT_SECONDS
    try:
        value = int(raw_value)
    except ValueError:
        return GMAIL_EXPORT_TIMEOUT_SECONDS
    return max(15, min(value, 300))


def _export_rolling_gmail_threads_with_timeout(
    service: GmailDashboardService,
    *,
    now: datetime,
    hours: int = DEFAULT_HOURS,
) -> str:
    timeout_seconds = _gmail_export_timeout_seconds()
    if threading.current_thread() is not threading.main_thread() or not hasattr(signal, "setitimer"):
        return export_rolling_gmail_threads(service, now=now, hours=hours)

    def _raise_timeout(signum: int, frame: Any) -> None:
        raise TimeoutError(f"Gmail thread export exceeded {timeout_seconds} seconds.")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    signal.signal(signal.SIGALRM, _raise_timeout)
    try:
        return export_rolling_gmail_threads(service, now=now, hours=hours)
    except TimeoutError as error:
        raise ConfigError("Gmail data could not be loaded within the daily brief timeout. Please try again shortly.") from error
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def _export_window_gmail_threads_with_timeout(
    service: GmailDashboardService,
    *,
    window_start: datetime,
    window_end: datetime,
) -> str:
    timeout_seconds = _gmail_export_timeout_seconds()
    if threading.current_thread() is not threading.main_thread() or not hasattr(signal, "setitimer"):
        return export_window_gmail_threads(service, window_start=window_start, window_end=window_end)

    def _raise_timeout(signum: int, frame: Any) -> None:
        raise TimeoutError(f"Gmail thread export exceeded {timeout_seconds} seconds.")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    signal.signal(signal.SIGALRM, _raise_timeout)
    try:
        return export_window_gmail_threads(service, window_start=window_start, window_end=window_end)
    except TimeoutError as error:
        raise ConfigError("Gmail data could not be loaded within the daily brief timeout. Please try again shortly.") from error
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


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
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    report_intelligence_config: dict[str, Any] | None = None,
    key_project_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    intelligence_config = normalize_report_intelligence_config(report_intelligence_config)
    local_window_start = window_start.astimezone(SEATALK_INSIGHTS_TIMEZONE) if window_start else None
    local_window_end = window_end.astimezone(SEATALK_INSIGHTS_TIMEZONE) if window_end else None
    if local_window_start and local_window_end:
        history_text = service._filter_system_generated_history(
            export_window_history(service, window_start=local_window_start, window_end=local_window_end)
        )
        period_hours = max(1, int((local_window_end - local_window_start).total_seconds() // 3600))
        window_label = f"{_format_window_endpoint(local_window_start)} - {_format_window_endpoint(local_window_end)}"
    else:
        history_text = service._filter_system_generated_history(export_rolling_history(service, now=local_now, hours=hours))
        period_hours = hours
        window_label = f"previous {hours} hours"
    history_text = filter_text_by_noise(history_text, config=intelligence_config, source="seatalk")
    gmail_history_text = str(gmail_history_text or "").strip()
    seatalk_has_messages = any(line.startswith("[") for line in history_text.splitlines())
    gmail_has_messages = any(line.startswith("Message ") for line in gmail_history_text.splitlines())
    if not seatalk_has_messages and not gmail_has_messages:
        quality_metadata = _build_quality_metadata(
            project_updates=[],
            other_updates=[],
            my_todos=[],
            direct_action_todos=[],
            watch_delegate_todos=[],
            reminders=[],
            source_texts=[history_text, gmail_history_text],
            deduped_topic_count=0,
        )
        return {
            "project_updates": [],
            "other_updates": [],
            "my_todos": [],
            "direct_action_todos": [],
            "watch_delegate_todos": [],
            "top_focus": [],
            "team_member_reminders": [],
            "team_todos": [],
            "quality_metadata": quality_metadata,
            "generated_at": local_now.isoformat(),
            "period_hours": period_hours,
            "window_start": local_window_start.isoformat() if local_window_start else "",
            "window_end": local_window_end.isoformat() if local_window_end else "",
            "window_label": window_label,
        }
    history_text = service._compact_history_for_insights(
        history_text,
        max_chars=620_000,
        signal_max_chars=400_000,
        recent_max_chars=180_000,
    )
    if gmail_history_text:
        gmail_history_text = gmail_history_text[:360_000]
    daily_matches = match_report_intelligence(
        f"{history_text}\n\n{gmail_history_text}",
        config=intelligence_config,
        key_projects=key_project_candidates or [],
    )
    daily_match_summary = build_daily_match_summary(daily_matches)
    _, parsed = service._run_codex_insights_prompt(
        system_prompt=_daily_brief_system_prompt(),
        prompt=_daily_brief_user_prompt(
            history_text=history_text,
            gmail_history_text=gmail_history_text,
            hours=period_hours,
            local_now=local_now,
            window_label=window_label,
            match_summary=daily_match_summary,
        ),
    )
    name_mappings = _load_seatalk_name_mappings(service)
    project_updates = _dedupe_brief_items(
        _normalize_update_items(_normalize_brief_items(parsed.get("project_updates", []), name_mappings=name_mappings))
    )[:MAX_PROJECT_UPDATES]
    other_updates = _dedupe_brief_items(
        _filter_other_updates(_normalize_update_items(_normalize_brief_items(parsed.get("other_updates", []), name_mappings=name_mappings)))
    )[:MAX_OTHER_UPDATES]
    my_todos = _dedupe_brief_items(
        _normalize_todo_items(_normalize_brief_items(parsed.get("my_todos", []), name_mappings=name_mappings)),
        text_fields=("task",),
    )[:MAX_MY_TODOS]
    reminders = _dedupe_brief_items(
        _filter_seatalk_reminders(
            _normalize_brief_items(parsed.get("team_member_reminders", []), default_source_type="seatalk", name_mappings=name_mappings)
        ),
        text_fields=("person", "reminder"),
    )[:MAX_TEAM_MEMBER_REMINDERS]
    my_todos = SeaTalkDashboardService._sort_todos(my_todos)
    _apply_report_intelligence_matches(
        [*project_updates, *other_updates, *my_todos],
        daily_matches=daily_matches,
    )
    project_updates = _sort_report_intelligence_items(project_updates)
    other_updates = _sort_report_intelligence_items(other_updates)
    my_todos = _sort_report_intelligence_items(my_todos)
    direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(my_todos)
    deduped_topic_count = _apply_cross_section_topic_metadata(
        project_updates=project_updates,
        other_updates=other_updates,
        my_todos=my_todos,
        reminders=reminders,
    )
    top_focus = _select_top_focus(
        direct_action_todos=direct_action_todos,
        watch_delegate_todos=watch_delegate_todos,
        project_updates=project_updates,
        other_updates=other_updates,
        now=local_now,
    )
    quality_metadata = _build_quality_metadata(
        project_updates=project_updates,
        other_updates=other_updates,
        my_todos=my_todos,
        direct_action_todos=direct_action_todos,
        watch_delegate_todos=watch_delegate_todos,
        reminders=reminders,
        source_texts=[history_text, gmail_history_text],
        deduped_topic_count=deduped_topic_count,
    )
    return {
        "project_updates": project_updates,
        "other_updates": other_updates,
        "my_todos": my_todos,
        "direct_action_todos": direct_action_todos,
        "watch_delegate_todos": watch_delegate_todos,
        "top_focus": top_focus,
        "team_member_reminders": reminders,
        "team_todos": [],
        "quality_metadata": quality_metadata,
        "generated_at": local_now.isoformat(),
        "period_hours": period_hours,
        "window_start": local_window_start.isoformat() if local_window_start else "",
        "window_end": local_window_end.isoformat() if local_window_end else "",
        "window_label": window_label,
    }


def render_email(*, briefing: dict[str, Any], now: datetime, window_label: str = "") -> tuple[str, str, str]:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    label = str(window_label or briefing.get("window_label") or "").strip()
    subject = f"Daily Brief - {local_now.date().isoformat()}"
    if label:
        subject = f"{subject} ({label})"
    todos = [item for item in briefing.get("my_todos") or [] if isinstance(item, dict)]
    direct_action_todos = [item for item in briefing.get("direct_action_todos") or [] if isinstance(item, dict)]
    watch_delegate_todos = [item for item in briefing.get("watch_delegate_todos") or [] if isinstance(item, dict)]
    if not direct_action_todos and not watch_delegate_todos:
        direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(_normalize_todo_items(todos))
    updates = [
        item
        for item in (briefing.get("project_updates") or [])
        if isinstance(item, dict) and _is_display_project_update_signal(item)
    ]
    other_updates = [
        item
        for item in (briefing.get("other_updates") or [])
        if isinstance(item, dict) and _is_display_other_update_signal(item)
    ]
    reminders = [item for item in briefing.get("team_member_reminders") or [] if isinstance(item, dict)]
    has_any_display_signal = bool(direct_action_todos or watch_delegate_todos or updates or other_updates or reminders)
    text_lines = [
        f"Subject: {subject}",
        f"Window: {label}" if label else "",
        "",
        "To-do",
    ]
    if not has_any_display_signal:
        text_lines.append(f"- {LOW_SIGNAL_EMAIL_SUMMARY}")
    elif not direct_action_todos and not watch_delegate_todos:
        text_lines.append(f"- {EMPTY_TODO_SECTION_SUMMARY}")
    if direct_action_todos:
        text_lines.extend(["", "Xiaodong Action Required"])
        text_lines.extend(_render_grouped_text(direct_action_todos, kind="todo"))
    if watch_delegate_todos:
        text_lines.extend(["", "Watch / Delegate"])
        text_lines.extend(_render_grouped_text(watch_delegate_todos, kind="todo"))
    if updates:
        text_lines.extend(["", "Project Updates"])
        text_lines.extend(_render_grouped_text(updates, kind="update"))
    if other_updates:
        text_lines.extend(["", "Other Update"])
        text_lines.extend(_render_grouped_text(other_updates, kind="update"))
    if reminders:
        text_lines.extend(["", "Suggested Team Follow-up"])
        text_lines.extend(_render_grouped_text(reminders, kind="reminder"))
    text_body = "\n".join(text_lines).strip() + "\n"
    html_body = "<html><body>" f"<h2>{html.escape(subject)}</h2>"
    if label:
        html_body += f"<p><strong>Window:</strong> {html.escape(label)}</p>"
    html_body += "<h3>To-do</h3>"
    if not has_any_display_signal:
        html_body += f"<p>{html.escape(LOW_SIGNAL_EMAIL_SUMMARY)}</p>"
    elif not direct_action_todos and not watch_delegate_todos:
        html_body += f"<p>{html.escape(EMPTY_TODO_SECTION_SUMMARY)}</p>"
    if direct_action_todos:
        html_body += "<h4>Xiaodong Action Required</h4>" + _render_grouped_html(direct_action_todos, kind="todo")
    if watch_delegate_todos:
        html_body += "<h4>Watch / Delegate</h4>" + _render_grouped_html(watch_delegate_todos, kind="watch_todo")
    if updates:
        html_body += "<h3>Project Updates</h3>" + _render_grouped_html(updates, kind="update")
    if other_updates:
        html_body += "<h3>Other Update</h3>" + _render_grouped_html(other_updates, kind="other")
    if reminders:
        html_body += "<h3>Suggested Team Follow-up</h3>" + _render_grouped_html(reminders, kind="reminder")
    html_body += "</body></html>"
    return subject, text_body, html_body


def send_daily_email(
    *,
    settings: Settings,
    recipient: str = DEFAULT_RECIPIENT,
    hours: int | None = None,
    slot: str = "auto",
    now: datetime | None = None,
    force: bool = False,
    dry_run: bool = False,
    gmail_service: Any | None = None,
    trello_client: TrelloDailySummaryClient | None = None,
    trello_store: TrelloDailySummaryStore | None = None,
) -> DailyEmailResult:
    local_now = (now or datetime.now(SEATALK_INSIGHTS_TIMEZONE)).astimezone(SEATALK_INSIGHTS_TIMEZONE)
    email_window = resolve_daily_email_window(now=local_now, slot=slot) if hours is None else None
    run_date = email_window.run_date if email_window else local_now.date().isoformat()
    run_slot = email_window.run_slot if email_window else LEGACY_SLOT
    window_start = email_window.start if email_window else None
    window_end = email_window.end if email_window else None
    window_label = email_window.label if email_window else ""
    subject = f"Daily Brief - {run_date}"
    if window_label:
        subject = f"{subject} ({window_label})"
    if email_window and should_skip_fixed_daily_email_window(now=local_now):
        return DailyEmailResult(
            status="skipped",
            recipient=recipient,
            subject=subject,
            run_date=run_date,
            run_slot=run_slot,
            window_start=window_start.isoformat() if window_start else "",
            window_end=window_end.isoformat() if window_end else "",
        )
    data_root = data_root_from_settings(settings)
    run_store = DailyEmailRunStore(data_root / "seatalk" / "daily_email_runs.json")
    team_dashboard_config = load_team_dashboard_config_from_data_root(data_root)
    report_intelligence_config = load_report_intelligence_config_from_data_root(data_root)
    key_project_candidates = key_project_candidates_from_team_config(team_dashboard_config)
    if not force and run_store.already_sent(run_date=run_date, recipient=recipient, run_slot=run_slot):
        return DailyEmailResult(
            status="skipped",
            recipient=recipient,
            subject=subject,
            run_date=run_date,
            run_slot=run_slot,
            window_start=window_start.isoformat() if window_start else "",
            window_end=window_end.isoformat() if window_end else "",
        )
    credential_store = StoredGoogleCredentials(
        data_root / "google" / "credentials.json",
        encryption_key=settings.team_portal_config_encryption_key,
    )
    owner_email = str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower()
    credentials_payload = credential_store.load(owner_email=owner_email)
    ensure_gmail_daily_scopes(credentials_payload)
    credentials = credentials_from_payload(credentials_payload)
    service = build_seatalk_service(settings, data_root=data_root)
    gmail_brief_service = GmailDashboardService(
        credentials=credentials,
        gmail_service=gmail_service,
        cache_key=owner_email,
        report_intelligence_config=report_intelligence_config,
    )
    if email_window:
        gmail_history_text = _export_window_gmail_threads_with_timeout(
            gmail_brief_service,
            window_start=email_window.start,
            window_end=email_window.end,
        )
        briefing = build_daily_briefing(
            service,
            now=local_now,
            gmail_history_text=gmail_history_text,
            window_start=email_window.start,
            window_end=email_window.end,
            report_intelligence_config=report_intelligence_config,
            key_project_candidates=key_project_candidates,
        )
    else:
        effective_hours = hours if hours is not None else DEFAULT_HOURS
        gmail_history_text = _export_rolling_gmail_threads_with_timeout(gmail_brief_service, now=local_now, hours=effective_hours)
        briefing = build_daily_briefing(
            service,
            now=local_now,
            hours=effective_hours,
            gmail_history_text=gmail_history_text,
            report_intelligence_config=report_intelligence_config,
            key_project_candidates=key_project_candidates,
        )
    subject, text_body, html_body = render_email(briefing=briefing, now=local_now, window_label=window_label)
    if dry_run:
        return DailyEmailResult(
            status="dry_run",
            recipient=recipient,
            subject=subject,
            run_date=run_date,
            run_slot=run_slot,
            window_start=window_start.isoformat() if window_start else "",
            window_end=window_end.isoformat() if window_end else "",
        )
    trello_result = sync_daily_summary_to_trello(
        briefing=briefing,
        run_date=run_date,
        run_slot=run_slot,
        window_label=window_label,
        data_root=data_root,
        now=local_now,
        trello_client=trello_client,
        trello_store=trello_store,
    )
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
        run_slot=run_slot,
        window_start=window_start,
        window_end=window_end,
    )
    return DailyEmailResult(
        status="sent",
        recipient=recipient,
        subject=subject,
        run_date=run_date,
        run_slot=run_slot,
        window_start=window_start.isoformat() if window_start else "",
        window_end=window_end.isoformat() if window_end else "",
        message_id=message_id,
        trello_status=trello_result.status,
        trello_created_count=trello_result.created_count,
        trello_skipped_count=trello_result.skipped_count,
        trello_cards=trello_result.cards,
    )


def sync_daily_summary_to_trello(
    *,
    briefing: dict[str, Any],
    run_date: str,
    run_slot: str = LEGACY_SLOT,
    window_label: str = "",
    data_root: Path,
    now: datetime,
    trello_client: TrelloDailySummaryClient | None = None,
    trello_store: TrelloDailySummaryStore | None = None,
) -> TrelloSyncResult:
    try:
        client = trello_client or TrelloDailySummaryClient.from_env()
    except ConfigError:
        return TrelloSyncResult(status="disabled")
    store = trello_store or TrelloDailySummaryStore(data_root / "seatalk" / "daily_trello_cards.json")
    specs = build_trello_card_specs(briefing=briefing, run_date=run_date, window_label=window_label)
    if not specs:
        return TrelloSyncResult(status="no_cards")

    list_id = client.get_or_create_list_id()
    created = 0
    skipped = 0
    cards: list[dict[str, str]] = []
    created_at = now.astimezone(SEATALK_INSIGHTS_TIMEZONE).isoformat()
    run_key = f"{run_date}:{run_slot}"
    for spec in specs:
        fingerprint = fingerprint_daily_card(
            run_date=run_key,
            section=spec.section,
            item_text=spec.fingerprint_text,
            domain=spec.domain,
        )
        if store.has_card(fingerprint):
            skipped += 1
            continue
        card = client.create_card(list_id=list_id, name=spec.name, description=spec.description)
        store.mark_card(
            fingerprint=fingerprint,
            name=card.name,
            url=card.url,
            trello_id=card.trello_id,
            created_at=created_at,
        )
        created += 1
        cards.append({"name": card.name, "url": card.url, "id": card.trello_id})
    return TrelloSyncResult(
        status="synced",
        created_count=created,
        skipped_count=skipped,
        cards=cards,
    )


def build_trello_card_specs(*, briefing: dict[str, Any], run_date: str, window_label: str = "") -> list[TrelloCardSpec]:
    direct_action_todos = [item for item in briefing.get("direct_action_todos") or [] if isinstance(item, dict)]
    watch_delegate_todos = [item for item in briefing.get("watch_delegate_todos") or [] if isinstance(item, dict)]
    if not direct_action_todos and not watch_delegate_todos:
        direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(
            _normalize_todo_items([item for item in briefing.get("my_todos") or [] if isinstance(item, dict)])
        )
    reminders = [item for item in briefing.get("team_member_reminders") or [] if isinstance(item, dict)]

    specs: list[TrelloCardSpec] = []
    for item in direct_action_todos:
        task = _sentence_text(item.get("task"), "Untitled").rstrip(".")
        specs.append(
            TrelloCardSpec(
                section="Xiaodong Action Required",
                name=f"[Direct] {task}",
                description=_trello_todo_description(
                    item,
                    run_date=run_date,
                    section="Xiaodong Action Required",
                    window_label=window_label,
                ),
                fingerprint_text=task,
                domain=_display_domain(item.get("domain")),
            )
        )
    for item in watch_delegate_todos:
        task = _sentence_text(item.get("task"), "Untitled").rstrip(".")
        specs.append(
            TrelloCardSpec(
                section="Watch / Delegate",
                name=f"[Watch] {task}",
                description=_trello_todo_description(
                    item,
                    run_date=run_date,
                    section="Watch / Delegate",
                    window_label=window_label,
                ),
                fingerprint_text=task,
                domain=_display_domain(item.get("domain")),
            )
        )
    for item in reminders:
        person = str(item.get("person") or "Unknown").strip()
        reminder = _sentence_text(item.get("reminder"), "Follow-up may be needed").rstrip(".")
        specs.append(
            TrelloCardSpec(
                section="Suggested Team Follow-up",
                name=f"[Follow-up] {person}: {reminder}",
                description=_trello_reminder_description(item, run_date=run_date, window_label=window_label),
                fingerprint_text=f"{person} {reminder}",
                domain=_display_domain(item.get("domain")),
            )
        )
    return specs


def _trello_todo_description(item: dict[str, Any], *, run_date: str, section: str, window_label: str = "") -> str:
    lines = [
        f"Report date: {run_date}",
        f"Section: {section}",
        f"Domain: {_display_domain(item.get('domain'))}",
        f"Task: {_sentence_text(item.get('task'), 'Untitled')}",
        f"Priority: {_display_priority(item.get('priority'))}",
        f"Due: {_display_due(item.get('due'))}",
        f"Source: {item.get('evidence') or 'Unknown'}",
    ]
    if window_label:
        lines.insert(1, f"Report window: {window_label}")
    source_type = str(item.get("source_type") or "").strip()
    if source_type:
        lines.append(f"Source type: {source_type}")
    return "\n".join(lines)


def _trello_reminder_description(item: dict[str, Any], *, run_date: str, window_label: str = "") -> str:
    lines = [
        f"Report date: {run_date}",
        "Section: Suggested Team Follow-up",
        f"Domain: {_display_domain(item.get('domain'))}",
        f"Person: {item.get('person') or 'Unknown'}",
        f"Reminder: {_sentence_text(item.get('reminder'), 'Follow-up may be needed')}",
        f"Source: {item.get('evidence') or 'Unknown'}",
    ]
    if window_label:
        lines.insert(1, f"Report window: {window_label}")
    source_type = str(item.get("source_type") or "").strip()
    if source_type:
        lines.append(f"Source type: {source_type}")
    return "\n".join(lines)


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
    window_label: str = "",
    match_summary: str = "",
) -> str:
    window_text = window_label or f"previous {hours} hours"
    match_block = (
        "## Today's Report Intelligence Matches\n"
        f"{match_summary}\n"
        "Use these matches only as prioritization hints. Do not create items unless the source evidence supports them. "
        "For matching output items, fill matched_vips, matched_keywords, matched_key_projects, and priority_reason.\n\n"
        if str(match_summary or "").strip()
        else ""
    )
    return (
        "## Output Contract\n"
        "Return a JSON object with exactly these top-level keys: project_updates, other_updates, my_todos, team_member_reminders, team_todos.\n"
        "project_updates: array of objects with keys domain, title, summary, status, evidence, source_type, matched_vips, matched_keywords, matched_key_projects, priority_reason.\n"
        "other_updates: array of objects with keys domain, title, summary, status, evidence, source_type, signal_type, matched_vips, matched_keywords, matched_key_projects, priority_reason.\n"
        "my_todos: array of objects with keys task, domain, priority, due, evidence, source_type, action_type, matched_vips, matched_keywords, matched_key_projects, priority_reason.\n"
        "team_member_reminders: array of objects with keys domain, person, reminder, evidence, source_type.\n"
        "team_todos must always be an empty array.\n"
        "Empty arrays are expected when a section has no important signal. Do not fill sections just to produce a report.\n\n"
        "## Allowed Values\n"
        "domain: Anti-fraud, Credit Risk, Ops Risk, General.\n"
        "status: done, in_progress, blocked, unknown.\n"
        "priority: high, medium, low, unknown.\n"
        "action_type: direct_action or watch_delegate.\n"
        "source_type: seatalk, gmail, mixed. Use mixed only when one synthesized item is supported by both SeaTalk and Gmail.\n"
        "other_updates.signal_type: incident, launch, policy_process, risk_compliance, cross_team_dependency, leadership_decision, cross_product_milestone, useful_awareness.\n"
        "If an other_updates item is useful but does not fit a stronger signal type, set signal_type to useful_awareness. Do not omit signal_type.\n\n"
        "## Section Rules\n"
        "my_todos: include only Xiaodong-owned actions, decisions needed from Xiaodong, follow-ups Xiaodong clearly needs to drive, or watch/delegate items where Xiaodong should ensure another owner follows through. Do not include tasks fully owned by other people with no Xiaodong follow-up value. Max 8 items. Sort high priority first, then earliest due date, then most actionable.\n"
        "For each my_todos item, set action_type=direct_action only when Xiaodong must personally reply, decide, review, approve, attend, provide, or drive the next step. Set action_type=watch_delegate when Xiaodong mainly needs to monitor, ensure, follow up with someone, check with a team, or confirm another owner follows through.\n"
        "project_updates: include updates from SeaTalk or Gmail where Xiaodong is involved, mentioned, directly asked, or clearly participating. Summarize the decision, milestone, blocker, or current state. Max 10 items. Sort blocked and in_progress before done.\n"
        "other_updates: include useful awareness from SeaTalk or Gmail where Xiaodong is not directly involved but the information may matter to a Digital Banking PM. Prioritize incident, launch, policy/process, risk/compliance, cross-team dependency, leadership decision, and cross-product milestone. useful_awareness should be rare and only included when genuinely PM-relevant, especially when matched to a VIP, priority keyword, or key project. Include at most 5 useful_awareness items and at most 8 other_updates total. Do not include generic chatter, greetings, pure thanks, meeting logistics with no decision, or low-value FYI.\n"
        "team_member_reminders: use SeaTalk only. Never create these from Gmail. Only include people from the explicit allowed reminder list below. Max 8 items. Sort by most actionable first.\n\n"
        "## Team Member Reminder Scan\n"
        "Before writing team_member_reminders, scan every SeaTalk group conversation for human mentions of these people: Ker Yin, Rene Chong, Sabrina Chan, Liye, Hui Xian, Sophia Wang Zijun, Ming Ming, Zoey Lu, Wang Chang, Jireh.\n"
        "Sophia Wang Zijun belongs to Credit Risk. Do not classify Sophia Wang Zijun as Ops Risk.\n"
        "For Anti-fraud domain reminders, only these people are Xiaodong's Anti-fraud team: Ker Yin, Rene Chong, Zoey Lu, Wang Chang, Jireh. Do not put anyone else, including Wendy, under Anti-fraud team_member_reminders.\n"
        "Do not create team_member_reminders for people outside the allowed reminder list, even if they appear in SeaTalk.\n"
        "A valid reminder exists when a human in a SeaTalk group asks, mentions, assigns, blocks on, or appears to need follow-up from one of those people, and neither the named person nor Xiaodong follows up later in that same group during the available window.\n"
        "Mentions may appear as direct @ mentions, plain names, mapped display names, name variants, or quoted text. Prefer real names in the person field.\n"
        "A cc-only mention is not enough. If a person is only copied after 'cc' and the actual ask is addressed to someone else, do not create a reminder for the cc'd person. If the direct assignee is outside the allowed list and an allowed teammate is only cc'd, produce no team_member_reminders item for that message.\n"
        "Do not include private chats. Do not include bot/system alerts, automated reminders, SDLC Checker output, or SDLC material/approval reminder messages. Do not include items where the named person replied, acknowledged, handled it, or Xiaodong already followed up later.\n"
        "If the source message is annotated as a thread reply, make the reminder and evidence say thread, for example 'UDL数据小群 / thread: PH A-Card Model V2.1 Deployment'. Do not write 'in the group' for thread replies.\n"
        "If the mention looks human and action-relevant but you are unsure whether a later follow-up resolved it, include one concise reminder rather than dropping it. Set source_type to seatalk.\n\n"
        "## Source And Evidence Rules\n"
        "For SeaTalk evidence, use the group ID/name or the key people involved. For Gmail evidence, use sender or key participants plus subject and thread link when available.\n"
        "Do not output unresolved raw SeaTalk IDs such as group-123, buddy-123, or UID 123 in evidence. Use mapped display names when visible; otherwise use a generic label such as SeaTalk group or SeaTalk contact.\n"
        "For Gmail thread messages marked context only, use them only to understand the in-window message; never summarize context-only messages as new To-do, Project Updates, or Other Updates.\n"
        "Merge duplicate items across SeaTalk and Gmail when they refer to the same project, owner, decision, task, or milestone. Keep one synthesized item and use source_type mixed when both sources support it.\n\n"
        "## Quality Rules\n"
        "Use status=done only when the outcome is fully complete. If an item says pending confirmation, still pending, tomorrow clarify, no fixed date, or awaiting confirmation, use status=in_progress or unknown, not done.\n"
        "If an item mentions MAS, launch before a fix, risk endorsement, ITC endorsement, blocked, or missing real-time fraud surveillance, treat it as high-risk and prefer status=blocked or signal_type=risk_compliance when appropriate.\n"
        "Avoid repeating the same topic across sections unless each section has a distinct role: Xiaodong next action, project state, or team member follow-up.\n\n"
        f"{match_block}"
        "## Exclusions\n"
        "For other_updates and team_member_reminders, ignore bot-generated alerts, automated reminders, system notifications, Jira/Confluence/calendar reminders, and no-reply notification emails unless a human adds meaningful follow-up in the same thread.\n\n"
        "For team_member_reminders, always exclude SDLC Checker and SG BAU SDLC material check content; those are automated release hygiene signals, not human team follow-up requests.\n\n"
        "## Formatting Inside JSON\n"
        "For my_todos.task, write one synthesized action sentence. For due, extract a real deadline if present; otherwise use TBD.\n"
        "For project_updates.summary and other_updates.summary, write one synthesized sentence, not a transcript.\n"
        "For evidence, provide only the source label. Do not include long snippets.\n\n"
        f"Window: {window_text}. Generated at: {local_now.isoformat()}.\n\n"
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


def _apply_report_intelligence_matches(items: list[dict[str, Any]], *, daily_matches: dict[str, Any]) -> None:
    matched_vips = daily_matches.get("matched_vips") if isinstance(daily_matches, dict) else []
    matched_keywords = daily_matches.get("matched_keywords") if isinstance(daily_matches, dict) else []
    matched_key_projects = daily_matches.get("matched_key_projects") if isinstance(daily_matches, dict) else []
    if not (matched_vips or matched_keywords or matched_key_projects):
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        text = _item_text(item)
        item_vips = _matching_labels(text, matched_vips, "display_name")
        item_keywords = [keyword for keyword in (matched_keywords or []) if str(keyword).casefold() in text]
        item_key_projects = [
            _key_project_match_label(project)
            for project in (matched_key_projects or [])
            if _key_project_item_matches(text, project)
        ]
        if item_vips:
            item["matched_vips"] = item_vips
        else:
            item.setdefault("matched_vips", [])
        if item_keywords:
            item["matched_keywords"] = item_keywords
        else:
            item.setdefault("matched_keywords", [])
        if item_key_projects:
            item["matched_key_projects"] = item_key_projects
        else:
            item.setdefault("matched_key_projects", [])
        reasons = []
        if item_vips:
            reasons.append("VIP")
        if item_keywords:
            reasons.append("priority keyword")
        if item_key_projects:
            reasons.append("Key Project")
        if reasons:
            item["priority_reason"] = ", ".join(reasons)
            if str(item.get("priority") or "").strip().lower() in {"", "unknown", "low"}:
                item["priority"] = "high" if item_vips or item_key_projects else "medium"
        else:
            item.setdefault("priority_reason", "")


def _sort_report_intelligence_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    priority_order = {"high": 0, "medium": 1, "low": 2, "unknown": 3}
    status_order = {"blocked": 0, "in_progress": 1, "unknown": 2, "done": 3}

    def key(item: dict[str, Any]) -> tuple[int, int, int, str]:
        signal = 0
        if item.get("matched_vips"):
            signal -= 4
        if item.get("matched_key_projects"):
            signal -= 3
        if item.get("matched_keywords"):
            signal -= 2
        return (
            signal,
            priority_order.get(str(item.get("priority") or "unknown").lower(), 3),
            status_order.get(str(item.get("status") or "unknown").lower(), 2),
            str(item.get("due") or item.get("title") or item.get("task") or "").casefold(),
        )

    return sorted(items, key=key)


def _matching_labels(text: str, rows: Any, field: str) -> list[str]:
    lowered = str(text or "").casefold()
    labels = []
    for row in rows or []:
        label = str((row or {}).get(field) or "").strip()
        if label and label.casefold() in lowered:
            labels.append(label)
    return labels


def _key_project_match_label(project: Any) -> str:
    bpmis_id = str((project or {}).get("bpmis_id") or "").strip()
    name = str((project or {}).get("project_name") or "").strip()
    return " / ".join(item for item in (bpmis_id, name) if item)


def _key_project_item_matches(text: str, project: Any) -> bool:
    lowered = str(text or "").casefold()
    terms = [
        (project or {}).get("bpmis_id"),
        (project or {}).get("project_name"),
        *((project or {}).get("jira_ids") or []),
    ]
    return any(str(term or "").strip().casefold() in lowered for term in terms if str(term or "").strip())


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


def _normalize_todo_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items:
        clean = dict(item)
        clean["domain"] = _display_domain(clean.get("domain"))
        clean["priority"] = _normalize_priority(clean.get("priority"))
        clean["due"] = _display_due(clean.get("due"))
        clean["action_type"] = _classify_todo_action_type(clean)
        normalized.append(clean)
    return normalized


def _normalize_update_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items:
        clean = dict(item)
        clean["domain"] = _display_domain(clean.get("domain"))
        clean["status"] = _correct_update_status(clean)
        if _is_risk_blocked_item(clean):
            clean["risk_level"] = "high"
            if clean.get("status") in {"done", "unknown"}:
                clean["status"] = "blocked"
            if "signal_type" in clean:
                clean["signal_type"] = "risk_compliance"
        normalized.append(clean)
    return normalized


def _classify_todo_action_type(item: dict[str, Any]) -> str:
    raw = str(item.get("action_type") or "").strip().lower().replace("-", "_")
    if raw in TODO_ACTION_TYPES:
        return raw
    task = _item_text(item, fields=("task", "title", "summary"))
    if "join or monitor" in task or "monitor" in task:
        return "watch_delegate"
    if any(hint in task for hint in WATCH_DELEGATE_HINTS) and not any(
        hint in task for hint in ("answer", "review", "attend", "decide", "approve", "reply")
    ):
        return "watch_delegate"
    if any(hint in task for hint in DIRECT_ACTION_HINTS):
        return "direct_action"
    return "direct_action"


def _split_todos_by_action_type(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    direct: list[dict[str, Any]] = []
    watch: list[dict[str, Any]] = []
    for item in _normalize_todo_items(items):
        if item.get("action_type") == "watch_delegate":
            watch.append(item)
        else:
            direct.append(item)
    return SeaTalkDashboardService._sort_todos(direct), SeaTalkDashboardService._sort_todos(watch)


def _correct_update_status(item: dict[str, Any]) -> str:
    status = str(item.get("status") or "unknown").strip().lower().replace("-", "_").replace(" ", "_")
    if status not in {"done", "in_progress", "blocked", "unknown"}:
        status = "unknown"
    text = _item_text(item)
    if any(hint in text for hint in PENDING_STATUS_HINTS) and status == "done":
        return "in_progress"
    if _is_risk_blocked_item(item):
        return "blocked"
    return status


def _is_risk_blocked_item(item: dict[str, Any]) -> bool:
    text = _item_text(item)
    return any(hint in text for hint in RISK_BLOCKED_HINTS)


def _apply_cross_section_topic_metadata(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    my_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
) -> int:
    sectioned_items = [
        ("project_updates", item) for item in project_updates
    ] + [
        ("other_updates", item) for item in other_updates
    ] + [
        ("my_todos", item) for item in my_todos
    ] + [
        ("team_member_reminders", item) for item in reminders
    ]
    topics: dict[str, dict[str, Any]] = {}
    for section, item in sectioned_items:
        key = _topic_key(item)
        if not key:
            continue
        item["topic_key"] = key
        topic = topics.setdefault(key, {"sections": set(), "evidence": ""})
        topic["sections"].add(section)
        topic["evidence"] = _merge_evidence(topic.get("evidence"), item.get("evidence"))
    deduped_topic_count = 0
    for section, item in sectioned_items:
        key = item.get("topic_key")
        if not key or key not in topics:
            continue
        topic = topics[key]
        if len(topic["sections"]) > 1:
            item["evidence"] = topic["evidence"]
            item["cross_section_duplicate"] = True
            deduped_topic_count += 1
    return deduped_topic_count


def _topic_key(item: dict[str, Any]) -> str:
    evidence_key = _normalize_dedupe_text(str(item.get("evidence") or ""))
    if evidence_key:
        return f"{_display_domain(item.get('domain')).lower()}:source:{evidence_key[:80]}"
    text = " ".join(
        str(item.get(field) or "")
        for field in ("domain", "title", "summary", "task", "reminder", "evidence")
    )
    normalized = _normalize_dedupe_text(text)
    if not normalized:
        return ""
    return f"{_display_domain(item.get('domain')).lower()}:{normalized[:120]}"


def _select_top_focus(
    *,
    direct_action_todos: list[dict[str, Any]],
    watch_delegate_todos: list[dict[str, Any]],
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    now: datetime,
) -> list[dict[str, Any]]:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for item in direct_action_todos:
        score = 70
        if _normalize_priority(item.get("priority")) == "high":
            score += 30
        if _due_is_today_or_tomorrow(item.get("due"), now=now):
            score += 25
        candidates.append((score, _focus_from_todo(item, section="Xiaodong Action Required")))
    for item in watch_delegate_todos:
        score = 35
        if _normalize_priority(item.get("priority")) == "high":
            score += 15
        if _due_is_today_or_tomorrow(item.get("due"), now=now):
            score += 20
        candidates.append((score, _focus_from_todo(item, section="Watch / Delegate")))
    for section, items in (("Project Updates", project_updates), ("Other Update", other_updates)):
        for item in items:
            if item.get("status") == "blocked" or item.get("risk_level") == "high":
                candidates.append((90, _focus_from_update(item, section=section)))
    seen: set[str] = set()
    focus: list[dict[str, Any]] = []
    for _score, item in sorted(candidates, key=lambda pair: pair[0], reverse=True):
        key = _topic_key(item) or _normalize_dedupe_text(item.get("title") or item.get("summary") or "")
        if key in seen:
            continue
        seen.add(key)
        focus.append(item)
        if len(focus) >= MAX_TOP_FOCUS_ITEMS:
            break
    return focus


def _focus_from_todo(item: dict[str, Any], *, section: str) -> dict[str, Any]:
    due = _display_due(item.get("due"))
    reason = f"{_display_priority(item.get('priority'))} priority"
    if due != "TBD":
        reason = f"{reason}; due {due}"
    return {
        "domain": _display_domain(item.get("domain")),
        "title": _sentence_text(item.get("task"), "Untitled"),
        "reason": reason,
        "source": item.get("evidence") or "Unknown",
        "section": section,
    }


def _focus_from_update(item: dict[str, Any], *, section: str) -> dict[str, Any]:
    reason = "Blocked or high-risk update"
    return {
        "domain": _display_domain(item.get("domain")),
        "title": _sentence_text(item.get("summary") or item.get("title"), "Untitled"),
        "reason": reason,
        "source": item.get("evidence") or "Unknown",
        "section": section,
    }


def _has_report_intelligence_match(item: dict[str, Any]) -> bool:
    if item.get("matched_vips") or item.get("matched_keywords") or item.get("matched_key_projects"):
        return True
    return bool(str(item.get("priority_reason") or "").strip())


def _is_display_project_update_signal(item: dict[str, Any]) -> bool:
    if _has_report_intelligence_match(item):
        return True
    status = _correct_update_status(item)
    return status in {"blocked", "in_progress"} or item.get("risk_level") == "high"


def _is_display_other_update_signal(item: dict[str, Any]) -> bool:
    if _has_report_intelligence_match(item):
        return True
    signal_type = _normalize_signal_type(item.get("signal_type"))
    if not signal_type:
        signal_type = "useful_awareness"
    return signal_type in STRONG_OTHER_UPDATE_SIGNAL_TYPES


def _build_quality_metadata(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    my_todos: list[dict[str, Any]],
    direct_action_todos: list[dict[str, Any]],
    watch_delegate_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
    source_texts: list[str],
    deduped_topic_count: int,
) -> dict[str, Any]:
    source_types = {
        str(item.get("source_type") or "").strip().lower()
        for item in [*project_updates, *other_updates, *my_todos, *reminders]
        if str(item.get("source_type") or "").strip()
    }
    joined_sources = "\n".join(source_texts).lower()
    if "seatalk" in joined_sources:
        source_types.add("seatalk")
    if "gmail" in joined_sources or "message " in joined_sources:
        source_types.add("gmail")
    high_confidence = sum(1 for item in direct_action_todos if _normalize_priority(item.get("priority")) == "high")
    manual_notes: list[str] = []
    if any(_display_due(item.get("due")) == "TBD" for item in my_todos):
        manual_notes.append("Some to-do due dates are TBD.")
    if any(item.get("status") in {"unknown", "in_progress"} and any(hint in _item_text(item) for hint in PENDING_STATUS_HINTS) for item in [*project_updates, *other_updates]):
        manual_notes.append("Some updates need confirmation before they can be treated as done.")
    if not manual_notes:
        manual_notes.append("No obvious manual review flag.")
    return {
        "source_coverage": _source_coverage_label(source_types),
        "deduped_topic_count": int(deduped_topic_count),
        "high_confidence_todo_count": int(high_confidence),
        "direct_action_count": len(direct_action_todos),
        "watch_delegate_count": len(watch_delegate_todos),
        "manual_review_notes": manual_notes[:3],
    }


def _source_coverage_label(source_types: set[str]) -> str:
    if "mixed" in source_types or {"seatalk", "gmail"}.issubset(source_types):
        return "SeaTalk + Gmail"
    if "seatalk" in source_types:
        return "SeaTalk"
    if "gmail" in source_types:
        return "Gmail"
    return "No message source"


def _filter_seatalk_reminders(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in items:
        if item.get("source_type") != "seatalk" or _is_bot_alert_or_reminder_item(item) or _is_sdlc_checker_reminder_item(item):
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


def _is_sdlc_checker_reminder_item(item: dict[str, Any]) -> bool:
    combined = " ".join(
        str(item.get(field) or "").lower()
        for field in ("evidence", "title", "summary", "reminder", "person")
    )
    return any(
        phrase in combined
        for phrase in (
            "sdlc checker",
            "sdlc material check",
            "sg bau sdlc material check",
            "sdlc material and approval reminders",
            "approval reminders",
            "prd/trd document",
            "sg-prd-approval",
            "sg-trd-approval",
        )
    )


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
    reason = str(item.get("why") or item.get("reason") or "").strip()
    reason_text = f" Why it matters: {_sentence_text(reason, '').strip()}" if reason else ""
    return (
        f"{item.get('person') or 'Unknown'}: {_sentence_text(item.get('reminder'), 'Follow-up may be needed')} "
        f"{reason_text}(Source: {item.get('evidence') or 'Unknown'})"
    )


def _render_focus_text(items: list[dict[str, Any]]) -> list[str]:
    return [
        f"- [{_display_domain(item.get('domain'))}] {_sentence_text(item.get('title'), 'Untitled')} "
        f"({_sentence_text(item.get('reason'), 'Focus item').rstrip('.')}; Source: {item.get('source') or 'Unknown'})"
        for item in items
    ]


def _render_focus_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p>No urgent focus item found in the briefing window.</p>"
    rows = "".join(f"<li>{html.escape(line.removeprefix('- '))}</li>" for line in _render_focus_text(items))
    return f"<ul>{rows}</ul>"


def _render_quality_text(metadata: dict[str, Any]) -> list[str]:
    notes = metadata.get("manual_review_notes") if isinstance(metadata.get("manual_review_notes"), list) else []
    note_text = "; ".join(str(note) for note in notes if str(note).strip()) or "No obvious manual review flag."
    return [
        f"- Sources: {metadata.get('source_coverage') or 'Unknown'}",
        f"- Deduped topics: {int(metadata.get('deduped_topic_count') or 0)}",
        f"- High-confidence direct to-dos: {int(metadata.get('high_confidence_todo_count') or 0)}",
        f"- Direct actions: {int(metadata.get('direct_action_count') or 0)}; Watch/delegate: {int(metadata.get('watch_delegate_count') or 0)}",
        f"- Manual review: {note_text}",
    ]


def _render_quality_html(metadata: dict[str, Any]) -> str:
    rows = "".join(f"<li>{html.escape(line.removeprefix('- '))}</li>" for line in _render_quality_text(metadata))
    return f"<ul>{rows}</ul>"


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
    return _display_priority_label(_normalize_priority(value))


def _normalize_priority(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"high", "medium", "low", "unknown"}:
        return text
    return "unknown"


def _display_priority_label(value: Any) -> str:
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


def _item_text(item: dict[str, Any], *, fields: tuple[str, ...] = ("title", "summary", "task", "reminder", "evidence")) -> str:
    return " ".join(str(item.get(field) or "").strip().lower() for field in fields if str(item.get(field) or "").strip())


def _due_is_today_or_tomorrow(value: Any, *, now: datetime) -> bool:
    text = str(value or "").strip().lower()
    if not text or text in {"tbd", "unknown", "none", "n/a", "na"}:
        return False
    if text in {"today", "tomorrow"}:
        return True
    today = now.date()
    tomorrow = today + timedelta(days=1)
    for candidate in (today.isoformat(), tomorrow.isoformat()):
        if candidate in text:
            return True
    return False


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
                "No watch/delegate item found."
                if kind == "watch_todo"
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
        )
        return f"<p>{html.escape(fallback)}</p>"
    sections: list[str] = []
    renderer = _renderer_for_kind(kind)
    for domain, domain_items in _group_items_by_domain(items):
        rows = "".join(f"<li>{html.escape(renderer(item))}</li>" for item in domain_items)
        sections.append(f"<h4>{html.escape(domain)}</h4><ul>{rows}</ul>")
    return "".join(sections)


def _renderer_for_kind(kind: str):
    if kind in {"todo", "watch_todo"}:
        return _render_todo_text
    if kind == "reminder":
        return _render_reminder_text
    return _render_update_text


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send the daily SeaTalk + Gmail briefing email.")
    parser.add_argument("--recipient", default=DEFAULT_RECIPIENT)
    parser.add_argument("--hours", type=int, default=None, help="Legacy rolling window override. Omit to use the 8am/1pm fixed schedule.")
    parser.add_argument("--slot", choices=["auto", MORNING_SLOT, MIDDAY_SLOT], default="auto")
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
        slot=args.slot,
        now=now,
        force=args.force,
        dry_run=args.dry_run,
    )
    print(json.dumps(result.__dict__, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
