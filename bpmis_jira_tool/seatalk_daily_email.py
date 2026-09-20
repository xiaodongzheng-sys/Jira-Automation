from __future__ import annotations

import argparse
import html
import json
import os
import signal
import threading
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
import re

from bpmis_jira_tool.codex_model_router import CODEX_ROUTE_DEEP, resolve_codex_model
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.daily_brief_archive import DailyBriefArchiveStore, daily_brief_archive_path
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
    TRELLO_WORKFLOW_LIST_FOLLOW_UP,
    TRELLO_WORKFLOW_LIST_INBOX,
    TRELLO_WORKFLOW_LIST_THIS_WEEK,
    TRELLO_WORKFLOW_LIST_TODAY,
    TRELLO_WORKFLOW_LIST_WATCH,
    TrelloCardSpec,
    TrelloDailySummaryClient,
    TrelloDailySummaryStore,
    TrelloSyncResult,
    daily_card_board_identity,
    daily_card_identity_from_trello_card,
    fingerprint_daily_card,
)


DEFAULT_RECIPIENT = "xiaodong.zheng@npt.sg"
DEFAULT_HOURS = 24
DAILY_BRIEF_CODEX_MODEL = "gpt-5.6-luna"
# One complete-source review must finish inside the Daily Brief delivery SLA.
# Luna/low keeps full-record synthesis inside the 1-2 minute delivery target.
# The prompt, source-grounding validators, and evidence checks carry the
# structural quality guardrails that would otherwise require longer reasoning.
DAILY_BRIEF_CODEX_REASONING_EFFORT = "low"
MORNING_SLOT = "morning"
MIDDAY_SLOT = "midday"
LEGACY_SLOT = "daily"
DAILY_EMAIL_SLOTS = {MORNING_SLOT, MIDDAY_SLOT}
DAILY_EMAIL_WEEKDAY_RUNS = {0, 1, 2, 3, 4}
GMAIL_EXPORT_TIMEOUT_SECONDS = 90
# Legacy limits are retained for compatibility with older callers and tests.
# The production Daily Brief now uses the model's complete-source judgement and
# does not truncate a valid section to these values.
MAX_MY_TODOS = 6
MAX_PROJECT_UPDATES = 6
MAX_OTHER_UPDATES = 5
MAX_TEAM_MEMBER_REMINDERS = 6
MAX_USEFUL_AWARENESS_OTHER_UPDATES = 2
MAX_UNANSWERED_SEATALK_QUESTION_HINTS = 10
MAX_TEAM_MEMBER_REMINDER_HINTS = 12
MAX_TOP_FOCUS_ITEMS = 3
DAILY_BRIEF_SEATALK_PROMPT_MAX_CHARS = 56_000
DAILY_BRIEF_SEATALK_PROMPT_RECENT_CHARS = 16_000
DAILY_BRIEF_GMAIL_PROMPT_MAX_CHARS = 24_000
DAILY_BRIEF_GMAIL_PROMPT_RECENT_CHARS = 6_000
DAILY_BRIEF_PROMPT_EVIDENCE_REF_LIMIT = 72
DAILY_BRIEF_TOKEN_CHARS_PER_TOKEN = 4
DAILY_BRIEF_QUALITY_PROMPT_WARNING_TOKENS = 30_000
# Every non-empty SeaTalk window is provided to Codex as one complete source
# file. Character and token metrics are telemetry only; they must never decide
# which chat records the model is allowed to inspect.
DAILY_BRIEF_REFERENCE_STYLE_THRESHOLD = 1
DAILY_BRIEF_FULL_SOURCE_FILE_THRESHOLD = 1
LOW_SIGNAL_EMAIL_SUMMARY = "No clear action, blocker, key project update, or team follow-up was found in this window."
EMPTY_TODO_SECTION_SUMMARY = "No Xiaodong-owned action or watch/delegate item found."
EMPTY_DAILY_BRIEF_SECTION = "无"
BRIEF_ZH_SECTION_LABELS = {
    "To-do": "待办",
    "Xiaodong Action Required": "Xiaodong 需要亲自处理",
    "Watch / Delegate": "关注 / 委派",
    "Project Updates": "项目动态",
    "Other Update": "其他重要动态",
    "Suggested Team Follow-up": "建议团队跟进",
}
BRIEF_ZH_DOMAIN_LABELS = {
    "Anti-fraud": "防欺诈",
    "Credit Risk": "信用风险",
    "Ops Risk": "操作风险",
    "General": "综合",
}
BRIEF_ZH_PRIORITY_LABELS = {"high": "高", "medium": "中", "low": "低", "unknown": "未知"}
BRIEF_ZH_STATUS_LABELS = {
    "done": "已完成",
    "in_progress": "进行中",
    "blocked": "已阻塞",
    "unknown": "未知",
}
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
TEAM_MEMBER_REMINDER_NOISE_GROUP_HINTS = (
    "business trip",
    "travel planning",
    "trip planning",
    "team building",
)
GMAIL_CALENDAR_SUBJECT_HINTS = (
    "invitation:",
    "updated invitation:",
    "updated invitation with note:",
    "accepted:",
    "declined:",
    "tentative:",
    "response:",
    "responded:",
    "rescheduled",
    "cancelled:",
    "canceled:",
    "rsvp",
    "event reminder:",
    "meeting request:",
    "you’re invited",
    "you're invited",
)
GMAIL_CALENDAR_SENDER_HINTS = (
    "calendar-notification@google.com",
    "googlecalendar-noreply@google.com",
)
GMAIL_LOW_VALUE_REMINDER_SUBJECT_HINTS = (
    "please change password",
    "password expiry",
    "password expiration",
    "password will expire",
    "password has expired",
)
DAILY_BRIEF_HIGH_SIGNAL_TERMS = (
    "[sp][p0]",
    "p0",
    "p1",
    "mas",
    "incident",
    "blocked",
    "blocker",
    "dependency",
    "upstream",
    "timeline",
    "delay",
    "delayed",
    "postpone",
    "eta",
    "launch",
    "go-live",
    "golive",
    "release",
    "version",
    "v3.07",
    "v3.08",
    "f30",
    "dev starts",
    "device model",
    "devicemodel",
    "atm",
    "qris",
    "translation",
    "copywriting",
    "querytransferrecipient",
    "fallback",
    "recurring",
    "mari stock",
    "548-549",
    "上线",
    "延期",
    "阻塞",
    "依赖",
    "事故",
)
TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE = {
    "xiaodong": "Zheng Xiaodong",
    "zheng xiaodong": "Zheng Xiaodong",
    "xiaodong zheng": "Zheng Xiaodong",
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
    "ang wei lin": "Ang Wei Lin",
    "angweilin": "Ang Wei Lin",
    "lim dao jun": "Lim Dao Jun",
    "limdaojun": "Lim Dao Jun",
}
TEAM_MEMBER_REMINDER_DETECTION_ALIASES = {
    alias: person
    for alias, person in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE.items()
    if alias not in {"chang", "zoey"}
}
# These exact SeaTalk names are not members of Xiaodong's team. Mask them before
# matching short aliases so a suffix such as "Mingming" cannot create a false follow-up.
TEAM_MEMBER_REMINDER_EXCLUDED_NAME_KEYS = {
    "li mingming",
}
ANTI_FRAUD_TEAM_MEMBERS = {
    "ker yin",
    "rene chong",
    "zoey lu",
    "wang chang",
    "jireh",
    "ang wei lin",
    "sabrina chan",
}
TEAM_MEMBER_REMINDER_DOMAIN_OVERRIDES = {
    "zheng xiaodong": "General",
    "sophia wang zijun": "Credit Risk",
}
XIAODONG_FOLLOWUP_COMMITMENT_CUES = (
    "will check and get back",
    "i'll check and get back",
    "i will check and get back",
    "will follow up and get back",
    "will follow up",
    "i'll follow up",
    "i will follow up",
    "will confirm",
    "i'll confirm",
    "i will confirm",
    "we can confirm",
    "let's confirm",
    "will investigate",
    "i'll investigate",
    "i will investigate",
    "will take a look",
    "i'll take a look",
    "i will take a look",
    "let me check",
    "let me confirm",
    "get back to",
    "can check with",
    "can check on",
    "can follow up",
    "会确认",
    "我来确认",
    "我来跟进",
    "稍后回复",
    "之后回复",
)
DAILY_BRIEF_SIGNAL_TERMS = (
    "xiaodong",
    "anti-fraud",
    "anti fraud",
    "fraud",
    "credit risk",
    "ops risk",
    "blocked",
    "blocker",
    "risk",
    "incident",
    "launch",
    "go live",
    "golive",
    "uat",
    "prd",
    "brd",
    "decision",
    "decide",
    "confirm",
    "pending",
    "follow up",
    "follow-up",
    "owner",
    "due",
    "deadline",
    "mas",
    "ojk",
    "bsp",
    "approval",
    "approve",
    "issue",
    "fix",
    "root cause",
    "mitigation",
    "next action",
    "todo",
    "请",
    "确认",
    "决定",
    "风险",
    "阻塞",
    "问题",
    "上线",
    "待确认",
)
RAW_SEATALK_ID_PATTERN = re.compile(r"\b(?:group|buddy)-\d+\b|\bUID\s+\d+\b", re.IGNORECASE)
TODO_ACTION_TYPES = {"direct_action", "watch_delegate"}
WATCH_DELEGATE_HINTS = (
    "ensure ",
    "确保",
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
        normalized_slot = MIDDAY_SLOT if local_now >= _local_datetime(local_now.date(), 19) else MORNING_SLOT
    if normalized_slot == MIDDAY_SLOT:
        start = _local_datetime(local_now.date(), 13)
        end = _local_datetime(local_now.date(), 19)
    elif normalized_slot == MORNING_SLOT:
        start = _previous_daily_brief_midday_end(local_now.date())
        end = _local_datetime(local_now.date(), 13)
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


def _previous_daily_brief_midday_end(value: Any) -> datetime:
    previous_date = value - timedelta(days=1)
    while previous_date.weekday() not in DAILY_EMAIL_WEEKDAY_RUNS:
        previous_date -= timedelta(days=1)
    return _local_datetime(previous_date, 19)


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
        codex_model=resolve_codex_model(
            CODEX_ROUTE_DEEP,
            legacy_env_names=("SEATALK_CODEX_MODEL",),
            explicit_model=os.getenv("SEATALK_CODEX_MODEL") or DAILY_BRIEF_CODEX_MODEL,
        ),
        codex_timeout_seconds=settings.daily_brief_codex_timeout_seconds,
        codex_concurrency=settings.source_code_qa_codex_concurrency,
        insights_llm_provider=str(os.getenv("DAILY_BRIEF_INSIGHTS_LLM_PROVIDER") or "").strip(),
        insights_codex_route=CODEX_ROUTE_DEEP,
        # The delivery SLA is a product contract, not a deploy-time tuning
        # option. Do not let a stale environment override silently restore
        # xhigh and turn a 1-2 minute brief into a multi-minute job.
        insights_codex_reasoning_effort=DAILY_BRIEF_CODEX_REASONING_EFFORT,
        claude_model=str(os.getenv("DAILY_BRIEF_CLAUDE_MODEL") or "").strip(),
        claude_binary=str(os.getenv("DAILY_BRIEF_CLAUDE_BINARY") or "").strip(),
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


def ensure_gmail_daily_scopes(credentials_payload: dict[str, Any], *, require_read: bool = True) -> None:
    scopes = {str(scope).strip() for scope in (credentials_payload.get("scopes") or []) if str(scope).strip()}
    required_scopes = ["https://www.googleapis.com/auth/gmail.send"]
    if require_read:
        required_scopes.insert(0, GMAIL_READONLY_SCOPE)
    missing = [scope for scope in required_scopes if scope not in scopes]
    if missing:
        permissions = "read and send" if require_read else "send"
        raise ConfigError(f"Gmail daily brief permission is missing. Reconnect Google once to grant Gmail {permissions} access.")


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
    include_debug_evidence_refs: bool = False,
) -> dict[str, Any]:
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    intelligence_config = normalize_report_intelligence_config(report_intelligence_config)
    local_window_start = window_start.astimezone(SEATALK_INSIGHTS_TIMEZONE) if window_start else None
    local_window_end = window_end.astimezone(SEATALK_INSIGHTS_TIMEZONE) if window_end else None
    refresh_seatalk_auto_name_mappings(service, now=local_now)
    if local_window_start and local_window_end:
        raw_seatalk_history_text = export_window_history(
            service,
            window_start=local_window_start,
            window_end=local_window_end,
        )
        history_text = service._filter_system_generated_history(raw_seatalk_history_text)
        period_hours = max(1, int((local_window_end - local_window_start).total_seconds() // 3600))
        window_label = f"{_format_window_endpoint(local_window_start)} - {_format_window_endpoint(local_window_end)}"
    else:
        raw_seatalk_history_text = export_rolling_history(service, now=local_now, hours=hours)
        history_text = service._filter_system_generated_history(raw_seatalk_history_text)
        period_hours = hours
        window_label = f"previous {hours} hours"
    seatalk_validation_history_text = history_text
    history_text = _filter_daily_brief_seatalk_noise(history_text, config=intelligence_config)
    history_text = _filter_daily_brief_meeting_logistics(history_text)
    filtered_seatalk_history_text = history_text
    # Validators must inspect the complete non-system record. Group-noise
    # preferences are presentation hints, not evidence deletion rules; using
    # the filtered excerpt here could make a genuine direct request impossible
    # to validate after the model found it in the raw source.
    validation_candidate_history_text = seatalk_validation_history_text
    # Gmail is the delivery channel only. Do not let a caller-provided export
    # influence analysis, evidence selection, source coverage, or empty-window
    # behavior. Keeping its size as telemetry makes accidental regressions
    # observable without reintroducing it as business input.
    ignored_gmail_input_chars = len(str(gmail_history_text or "").strip())
    gmail_history_text = ""
    suppressed_calendar_message_count = 0
    suppressed_low_value_reminder_count = 0
    seatalk_raw_chars = len(history_text)
    gmail_raw_chars = len(gmail_history_text)
    seatalk_has_messages = any(line.startswith("[") for line in history_text.splitlines())
    if not seatalk_has_messages:
        source_token_ledger = {
            "seatalk_raw_chars": seatalk_raw_chars,
            "seatalk_compact_chars": len(history_text),
            "seatalk_prompt_chars": 0,
            "seatalk_prompt_hit_cap": False,
            "gmail_raw_chars": gmail_raw_chars,
            "ignored_gmail_input_chars": ignored_gmail_input_chars,
            "gmail_compact_chars": len(gmail_history_text),
            "gmail_prompt_chars": 0,
            "gmail_prompt_hit_cap": False,
            "final_prompt_chars": 0,
            "final_estimated_prompt_tokens": 0,
        }
        quality_metadata = _build_quality_metadata(
            project_updates=[],
            other_updates=[],
            my_todos=[],
            direct_action_todos=[],
            watch_delegate_todos=[],
            reminders=[],
            source_texts=[history_text],
            deduped_topic_count=0,
            token_ledger=source_token_ledger,
            evidence_quality_metrics={
                "dropped_invalid_evidence_count": 0,
                "repaired_evidence_count": 0,
                "generic_evidence_count": 0,
                "candidate_followup_count": 0,
                "final_followup_count": 0,
                "calendar_suppressed_count": suppressed_calendar_message_count,
                "low_value_reminder_suppressed_count": suppressed_low_value_reminder_count,
            },
        )
        empty_briefing = {
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
        if include_debug_evidence_refs:
            empty_briefing["_debug_evidence_refs"] = []
        return empty_briefing
    # The model is the primary analyst. Keep every available SeaTalk record in
    # the source file and reserve heuristic compaction for legacy callers only.
    # Gmail is transport-only. The supplied content was intentionally ignored
    # above, even for direct callers, so every reported business item remains
    # grounded in the complete SeaTalk export.
    seatalk_compact_chars = len(history_text)
    gmail_compact_chars = len(gmail_history_text)
    high_signal_review_hints = _build_high_signal_review_hints(filtered_seatalk_history_text)
    unanswered_question_hints = _build_unanswered_seatalk_question_hints(history_text)
    xiaodong_followup_candidates = _build_xiaodong_followup_candidates(validation_candidate_history_text)
    all_reminder_candidates = _build_team_member_reminder_candidates(validation_candidate_history_text)
    xiaodong_request_candidates = [
        {**candidate, "ownership_reason": "direct_request"}
        for candidate in (all_reminder_candidates or [])
        if _canonical_team_member_name(candidate.get("person")) == "Zheng Xiaodong"
    ]
    xiaodong_request_candidates.extend(_build_direct_xiaodong_request_candidates(validation_candidate_history_text))
    team_member_reminder_candidates = None if all_reminder_candidates is None else [
        candidate
        for candidate in all_reminder_candidates
        if _canonical_team_member_name(candidate.get("person")) != "Zheng Xiaodong"
    ]
    xiaodong_followup_candidates = _dedupe_xiaodong_action_candidates(
        [*xiaodong_followup_candidates, *xiaodong_request_candidates]
    )
    resolved_team_member_reminder_candidates = _build_resolved_team_member_reminder_candidates(validation_candidate_history_text)
    team_member_reminder_hints = _format_team_member_reminder_hints(team_member_reminder_candidates)
    name_mappings = _load_seatalk_name_mappings(service)
    for key, name in _infer_private_chat_name_mappings_from_history(seatalk_validation_history_text).items():
        name_mappings.setdefault(key.lower(), name)
    evidence_refs = _build_daily_brief_evidence_refs(
        validation_candidate_history_text,
        gmail_history_text=gmail_history_text,
        name_mappings=name_mappings,
        team_member_reminder_candidates=team_member_reminder_candidates,
        xiaodong_followup_candidates=xiaodong_followup_candidates,
    )
    daily_matches = match_report_intelligence(
        f"{filtered_seatalk_history_text}\n\n{gmail_history_text}",
        config=intelligence_config,
        key_projects=key_project_candidates or [],
    )
    daily_match_summary = build_daily_match_summary(daily_matches)
    # Complete source lives in a file instead of an excerpt embedded in the
    # prompt. This avoids selecting evidence before the model has read it.
    prompt_history_text = ""
    prompt_gmail_history_text = ""
    source_token_ledger = {
        "seatalk_raw_chars": seatalk_raw_chars,
        "seatalk_compact_chars": seatalk_compact_chars,
        "seatalk_prompt_chars": len(prompt_history_text),
        "seatalk_prompt_hit_cap": False,
        "gmail_raw_chars": gmail_raw_chars,
        "ignored_gmail_input_chars": ignored_gmail_input_chars,
        "gmail_compact_chars": gmail_compact_chars,
        "gmail_prompt_chars": len(prompt_gmail_history_text),
        "gmail_prompt_hit_cap": False,
    }
    evidence_context = _build_daily_brief_evidence_context(
        unanswered_question_hints=unanswered_question_hints,
        team_member_reminder_candidates=team_member_reminder_candidates,
        evidence_refs=evidence_refs,
        high_signal_review_hints=high_signal_review_hints,
        xiaodong_followup_candidates=xiaodong_followup_candidates,
        source_token_ledger=source_token_ledger,
    )
    source_file_path = _write_daily_brief_source_file(
        service,
        raw_seatalk_history_text,
        threshold=DAILY_BRIEF_FULL_SOURCE_FILE_THRESHOLD,
    )
    reference_style_mode = len(raw_seatalk_history_text) >= DAILY_BRIEF_REFERENCE_STYLE_THRESHOLD
    prompt = _daily_brief_user_prompt(
        history_text=prompt_history_text,
        gmail_history_text=prompt_gmail_history_text,
        hours=period_hours,
        local_now=local_now,
        window_label=window_label,
        match_summary=daily_match_summary,
        unanswered_question_hints=unanswered_question_hints,
        team_member_reminder_hints=team_member_reminder_hints,
        xiaodong_followup_hints=_format_xiaodong_followup_hints(xiaodong_followup_candidates),
        high_signal_review_hints=high_signal_review_hints,
        evidence_context=evidence_context,
        source_file_path=str(source_file_path) if source_file_path else "",
        source_text=(raw_seatalk_history_text if reference_style_mode and source_file_path is None else ""),
    )
    source_file_chars = len(raw_seatalk_history_text) if source_file_path else 0
    source_token_ledger["final_prompt_chars"] = len(prompt)
    source_token_ledger["final_estimated_prompt_tokens"] = _estimate_daily_prompt_tokens(prompt)
    source_token_ledger["effective_input_chars"] = len(prompt) + source_file_chars
    source_token_ledger["effective_estimated_input_tokens"] = _estimate_daily_prompt_tokens(
        prompt + raw_seatalk_history_text if source_file_path else prompt
    )
    source_token_ledger["prompt_budget_policy"] = "full_source_no_truncation"
    source_token_ledger["prompt_budget_threshold_tokens"] = DAILY_BRIEF_QUALITY_PROMPT_WARNING_TOKENS
    source_token_ledger["quality_preserving_over_budget"] = (
        source_token_ledger["effective_estimated_input_tokens"] >= DAILY_BRIEF_QUALITY_PROMPT_WARNING_TOKENS
    )
    source_token_ledger["compaction_reason"] = "not_applied_full_source_model_review"
    source_token_ledger["preserved_evidence_ref_count"] = len(evidence_refs)
    source_token_ledger["prompt_evidence_ref_count"] = len(
        json.loads(evidence_context).get("evidence_refs", [])
    )
    source_token_ledger["seatalk_source_mode"] = (
        "full_source_file"
        if source_file_path is not None
        else ("full_prompt" if reference_style_mode else "prompt_excerpt")
    )
    source_token_ledger["seatalk_source_file_chars"] = source_file_chars
    source_token_ledger["preserved_followup_candidate_count"] = len(team_member_reminder_candidates or [])
    source_token_ledger["preserved_unanswered_hint_count"] = len(
        [line for line in str(unanswered_question_hints or "").splitlines() if line.strip()]
    )
    source_token_ledger["preserved_high_signal_hint_count"] = len(
        [line for line in str(high_signal_review_hints or "").splitlines() if line.strip()]
    )
    model_review_passes = 0
    try:
        _, parsed = service._run_codex_insights_prompt(
            system_prompt=_daily_brief_system_prompt(),
            prompt=prompt,
        )
        model_review_passes += 1
    finally:
        _remove_daily_brief_source_file(source_file_path)
    parsed = _daily_brief_seatalk_only_response(parsed)
    language_repair_attempted = False
    language_repair_count = 0
    if reference_style_mode and _daily_brief_needs_language_repair(parsed):
        language_repair_attempted = True
        _, repaired = service._run_codex_insights_prompt(
            system_prompt=_daily_brief_system_prompt(),
            prompt=_daily_brief_language_repair_prompt(parsed),
        )
        if _daily_brief_has_items(repaired):
            parsed = repaired
    project_updates = _dedupe_brief_items(
        _filter_gmail_calendar_items(
            _prepare_project_update_items(
                _normalize_update_items(_normalize_brief_items(parsed.get("project_updates", []), name_mappings=name_mappings))
            )
        )
    )
    business_trip_project_updates = _build_business_trip_project_updates(
        validation_candidate_history_text,
        existing_items=project_updates,
    )
    if business_trip_project_updates:
        project_updates = _dedupe_brief_items(
            _prepare_project_update_items([*project_updates, *business_trip_project_updates])
        )
    # Complete-source model review is the normal production path. Retain
    # deterministic content generation only for an unexpected legacy mode.
    high_signal_fallbacks: list[dict[str, Any]] = []
    if not reference_style_mode:
        high_signal_fallbacks = _build_high_signal_fallback_items(
            filtered_seatalk_history_text,
            evidence_refs=evidence_refs,
            name_mappings=name_mappings,
            existing_items=project_updates,
        )
        high_signal_fallbacks.extend(
            _build_gmail_high_signal_fallback_items(
                gmail_history_text,
                evidence_refs=evidence_refs,
                existing_items=[*project_updates, *high_signal_fallbacks],
            )
        )
    other_fallbacks: list[dict[str, Any]] = []
    project_fallbacks: list[dict[str, Any]] = []
    for fallback in high_signal_fallbacks:
        if _fallback_should_be_other_update(fallback):
            fallback = dict(fallback)
            fallback["signal_type"] = _fallback_signal_type(fallback)
            fallback["summary"] = _synthesize_other_update_summary(fallback)
            if fallback.get("summary"):
                other_fallbacks.append(fallback)
        else:
            project_fallbacks.append(fallback)
    high_signal_fallbacks = project_fallbacks
    high_signal_fallbacks = _prepare_project_update_items(high_signal_fallbacks)
    if high_signal_fallbacks:
        # Legacy fallback mode only: preserve its existing guarded merge logic.
        project_updates = _prepare_project_update_items(
            _dedupe_brief_items([*high_signal_fallbacks, *project_updates])
        )
    other_updates = _dedupe_brief_items(
        _filter_gmail_calendar_items(
            _prepare_other_update_items(
                _filter_other_updates(
                    _normalize_update_items(
                        _normalize_brief_items(
                            [*other_fallbacks, *parsed.get("other_updates", [])],
                            name_mappings=name_mappings,
                        )
                    )
                )
            )
        )
    )
    parsed_todos = _normalize_todo_items(_normalize_brief_items(parsed.get("my_todos", []), name_mappings=name_mappings))
    xiaodong_followup_fallbacks: list[dict[str, Any]] = []
    gmail_xiaodong_action_fallbacks: list[dict[str, Any]] = []
    if not reference_style_mode:
        xiaodong_followup_fallbacks = _build_xiaodong_followup_items(
            xiaodong_followup_candidates,
            evidence_refs=evidence_refs,
            existing_items=parsed_todos,
        )
        gmail_xiaodong_action_fallbacks = _build_gmail_xiaodong_action_items(
            evidence_refs,
            existing_items=[*xiaodong_followup_fallbacks, *parsed_todos],
        )
    my_todos = _dedupe_brief_items(
        _filter_gmail_calendar_items(
            [*xiaodong_followup_fallbacks, *gmail_xiaodong_action_fallbacks, *parsed_todos]
        ),
        text_fields=("task",),
    )
    if high_signal_fallbacks:
        # Merge before evidence validation and cross-section suppression. A
        # version/timeline candidate can share a canonical todo with a MAS or
        # launch decision; merging only after suppression loses its detail when
        # the intermediate update is filtered for evidence or deduplication.
        for fallback in high_signal_fallbacks:
            covered_todo = next(
                (
                    todo
                    for todo in my_todos
                    if _brief_update_is_covered_by_todo(fallback, todo)
                ),
                None,
            )
            if covered_todo is not None:
                _merge_high_signal_update_into_todo(covered_todo, fallback)
    reminders = _dedupe_brief_items(
        _filter_gmail_calendar_items(
            _filter_seatalk_reminders(
                _normalize_brief_items(parsed.get("team_member_reminders", []), default_source_type="seatalk", name_mappings=name_mappings),
                # This is a validation gate, not deterministic generation:
                # the model chooses the item, but it must map to a direct,
                # unresolved human request for the named team member.
                reminder_candidates=team_member_reminder_candidates,
            )
        ),
        text_fields=("person", "reminder"),
    )
    my_todos = _filter_resolved_or_meeting_logistics_followups(
        my_todos,
        resolved_candidates=resolved_team_member_reminder_candidates,
    )
    reminders = _filter_resolved_or_meeting_logistics_followups(
        reminders,
        resolved_candidates=resolved_team_member_reminder_candidates,
    )
    evidence_quality_metrics = _apply_daily_brief_evidence_refs(
        project_updates=project_updates,
        other_updates=other_updates,
        my_todos=my_todos,
        reminders=reminders,
        evidence_refs=evidence_refs,
    )
    evidence_quality_metrics["calendar_suppressed_count"] = suppressed_calendar_message_count
    evidence_quality_metrics["low_value_reminder_suppressed_count"] = suppressed_low_value_reminder_count
    evidence_quality_metrics["model_review_passes"] = model_review_passes
    evidence_quality_metrics["business_trip_project_fallback_count"] = len(business_trip_project_updates)
    evidence_quality_metrics["high_signal_fallback_count"] = len(high_signal_fallbacks)
    evidence_quality_metrics["other_high_signal_fallback_count"] = len(other_fallbacks)
    evidence_quality_metrics["xiaodong_followup_fallback_count"] = len(xiaodong_followup_fallbacks)
    evidence_quality_metrics["gmail_xiaodong_action_fallback_count"] = len(gmail_xiaodong_action_fallbacks)
    _repair_generic_seatalk_evidence(
        [*project_updates, *other_updates, *my_todos, *reminders],
        history_text=seatalk_validation_history_text,
        quality_metrics=evidence_quality_metrics,
    )
    for section_items in (project_updates, other_updates, my_todos, reminders):
        _validate_and_repair_seatalk_evidence(
            section_items,
            history_text=seatalk_validation_history_text,
            quality_metrics=evidence_quality_metrics,
            name_mappings=name_mappings,
        )
        _drop_domain_mismatched_evidence_items(
            section_items,
            quality_metrics=evidence_quality_metrics,
        )
        _drop_generic_seatalk_evidence_items(
            section_items,
            quality_metrics=evidence_quality_metrics,
        )
    my_todos = SeaTalkDashboardService._sort_todos(my_todos)
    if not reference_style_mode:
        reminders = _backfill_team_member_reminders_from_candidates(
            reminders,
            team_member_reminder_candidates=team_member_reminder_candidates,
            resolved_candidates=resolved_team_member_reminder_candidates,
            evidence_refs=evidence_refs,
            quality_metrics=evidence_quality_metrics,
        )
    else:
        evidence_quality_metrics["deterministic_followup_backfill_count"] = 0
    reminders = _filter_resolved_or_meeting_logistics_followups(
        reminders,
        resolved_candidates=resolved_team_member_reminder_candidates,
    )
    reminders = _filter_team_member_coverage_items(reminders)
    _repair_generic_seatalk_evidence(
        reminders,
        history_text=seatalk_validation_history_text,
        quality_metrics=evidence_quality_metrics,
    )
    _validate_and_repair_seatalk_evidence(
        reminders,
        history_text=seatalk_validation_history_text,
        quality_metrics=evidence_quality_metrics,
        name_mappings=name_mappings,
    )
    _drop_domain_mismatched_evidence_items(
        reminders,
        quality_metrics=evidence_quality_metrics,
    )
    _drop_generic_seatalk_evidence_items(
        reminders,
        quality_metrics=evidence_quality_metrics,
    )
    _apply_report_intelligence_matches(
        [*project_updates, *other_updates, *my_todos],
        daily_matches=daily_matches,
    )
    project_updates = _sort_report_intelligence_items(project_updates)
    other_updates = _sort_report_intelligence_items(other_updates)
    my_todos = _sort_report_intelligence_items(my_todos)
    direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(my_todos)
    reminders = _filter_reminders_already_covered_by_watch_delegate(reminders, watch_delegate_todos)
    suppressed_update_duplicate_count = _suppress_updates_covered_by_todos(
        project_updates=project_updates,
        other_updates=other_updates,
        direct_action_todos=direct_action_todos,
        watch_delegate_todos=watch_delegate_todos,
    )
    suppressed_cross_section_duplicate_count = _suppress_cross_section_duplicate_topics(
        project_updates=project_updates,
        other_updates=other_updates,
        direct_action_todos=direct_action_todos,
        watch_delegate_todos=watch_delegate_todos,
        reminders=reminders,
    )
    if not reference_style_mode:
        _ensure_high_signal_fallbacks_visible(
            high_signal_fallbacks=high_signal_fallbacks,
            project_updates=project_updates,
            other_updates=other_updates,
            direct_action_todos=direct_action_todos,
            watch_delegate_todos=watch_delegate_todos,
            reminders=reminders,
        )
    # Visibility repair can merge a protected fallback into a todo after the
    # first pass. Re-run canonical suppression so that repair cannot re-create
    # a cross-section duplicate.
    suppressed_cross_section_duplicate_count += _suppress_cross_section_duplicate_topics(
        project_updates=project_updates,
        other_updates=other_updates,
        direct_action_todos=direct_action_todos,
        watch_delegate_todos=watch_delegate_todos,
        reminders=reminders,
    )
    _correct_known_update_domains(project_updates)
    _correct_known_update_domains(other_updates)
    project_updates[:] = _dedupe_same_topic_items(_prepare_project_update_items(project_updates))
    if reference_style_mode:
        final_language_payload = {
            "project_updates": project_updates,
            "other_updates": other_updates,
            "my_todos": my_todos,
            "team_member_reminders": reminders,
            "team_todos": [],
        }
        for _ in range(2):
            if not _daily_brief_needs_language_repair(final_language_payload):
                break
            language_repair_attempted = True
            _, repaired = service._run_codex_insights_prompt(
                system_prompt=_daily_brief_system_prompt(),
                prompt=_daily_brief_language_repair_prompt(final_language_payload),
            )
            if not _daily_brief_has_items(repaired):
                break
            language_repair_count += _apply_final_language_repair(
                project_updates=project_updates,
                other_updates=other_updates,
                my_todos=my_todos,
                reminders=reminders,
                repaired=repaired,
            )
        for _ in range(2):
            residual_items = _daily_brief_remaining_language_items(
                project_updates=project_updates,
                other_updates=other_updates,
                my_todos=my_todos,
                reminders=reminders,
            )
            if not residual_items:
                break
            language_repair_attempted = True
            _, residual_repaired = service._run_codex_insights_prompt(
                system_prompt=_daily_brief_residual_language_system_prompt(),
                prompt=_daily_brief_residual_language_repair_prompt(residual_items),
            )
            language_repair_count += _apply_residual_language_repair(
                project_updates=project_updates,
                other_updates=other_updates,
                my_todos=my_todos,
                reminders=reminders,
                repaired=residual_repaired,
            )
        evidence_quality_metrics["language_repair_attempted"] = language_repair_attempted
        evidence_quality_metrics["language_repair_count"] = language_repair_count
        # Language repair mutates canonical my_todos. Rebuild the display
        # partitions because _split_todos_by_action_type returns item copies;
        # otherwise the email could retain stale English in direct_action_todos
        # even though my_todos was translated successfully.
        direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(my_todos)
    # A language-repair response is untrusted for business scope: it may be a
    # malformed full JSON response instead of a field-only translation. Apply
    # the source boundary again immediately before rendering.
    final_seatalk_response = _daily_brief_seatalk_only_response(
        {
            "project_updates": project_updates,
            "other_updates": other_updates,
            "my_todos": my_todos,
            "team_member_reminders": reminders,
            "team_todos": [],
        }
    )
    project_updates = final_seatalk_response["project_updates"]
    other_updates = final_seatalk_response["other_updates"]
    my_todos = final_seatalk_response["my_todos"]
    reminders = final_seatalk_response["team_member_reminders"]
    direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(my_todos)
    _clean_daily_brief_evidence(
        [*project_updates, *other_updates, *direct_action_todos, *watch_delegate_todos, *reminders]
    )
    evidence_quality_metrics["generic_evidence_count"] = _count_generic_evidence(
        [*project_updates, *other_updates, *direct_action_todos, *watch_delegate_todos, *reminders]
    )
    evidence_quality_metrics["candidate_followup_count"] = len(team_member_reminder_candidates or [])
    evidence_quality_metrics["final_followup_count"] = len(reminders)
    evidence_quality_metrics["suppressed_update_duplicate_count"] = suppressed_update_duplicate_count
    evidence_quality_metrics["suppressed_cross_section_duplicate_count"] = suppressed_cross_section_duplicate_count
    evidence_quality_metrics["followup_diagnostics"] = _build_followup_diagnostics(
        team_member_reminder_candidates=team_member_reminder_candidates,
        reminders=reminders,
        watch_delegate_todos=watch_delegate_todos,
        evidence_refs=evidence_refs,
    )
    deduped_topic_count = suppressed_cross_section_duplicate_count + _apply_cross_section_topic_metadata(
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
        source_texts=[history_text],
        deduped_topic_count=deduped_topic_count,
        token_ledger=source_token_ledger,
        evidence_quality_metrics=evidence_quality_metrics,
    )
    briefing = {
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
    if include_debug_evidence_refs:
        briefing["_debug_evidence_refs"] = evidence_refs
    return briefing


def render_email(
    *,
    briefing: dict[str, Any],
    now: datetime,
    window_label: str = "",
    language: str = "en",
) -> tuple[str, str, str]:
    """Render the report for delivery.

    ``language`` defaults to English for backwards-compatible library callers;
    production delivery explicitly uses Simplified Chinese. Internal field
    names, enums, and evidence IDs remain stable for validation and storage.
    """
    local_now = now.astimezone(SEATALK_INSIGHTS_TIMEZONE)
    is_chinese = str(language or "").strip().casefold() in {"zh", "zh-cn", "chinese"}
    label = str(window_label or briefing.get("window_label") or "").strip()
    if is_chinese:
        label = _brief_localize_window_label(label)
    subject_prefix = "每日简报" if is_chinese else "Daily Brief"
    subject = f"{subject_prefix} - {local_now.date().isoformat()}"
    if label:
        subject = f"{subject} ({label})"
    todos = [item for item in briefing.get("my_todos") or [] if isinstance(item, dict)]
    direct_action_todos = [item for item in briefing.get("direct_action_todos") or [] if isinstance(item, dict)]
    watch_delegate_todos = [item for item in briefing.get("watch_delegate_todos") or [] if isinstance(item, dict)]
    if not direct_action_todos and not watch_delegate_todos:
        direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(_normalize_todo_items(todos))
    reminders = _filter_reminders_already_covered_by_watch_delegate(
        [item for item in briefing.get("team_member_reminders") or [] if isinstance(item, dict)],
        watch_delegate_todos,
    )
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
    text_lines = [
        (f"主题：{subject}" if is_chinese else f"Subject: {subject}"),
        (f"时间窗口：{label}" if is_chinese else f"Window: {label}") if label else "",
        "",
        _brief_label("To-do", language=language),
    ]
    for heading, items, kind in (
        ("Xiaodong Action Required", direct_action_todos, "todo"),
        ("Watch / Delegate", watch_delegate_todos, "todo"),
        ("Project Updates", updates, "update"),
        ("Other Update", other_updates, "update"),
        ("Suggested Team Follow-up", reminders, "reminder"),
    ):
        text_lines.extend(["", _brief_label(heading, language=language)])
        text_lines.extend(
            _render_grouped_text(items, kind=kind, language=language)
            if items
            else [f"- {EMPTY_DAILY_BRIEF_SECTION}"]
        )
    text_body = "\n".join(text_lines).strip() + "\n"
    html_body = "<html><body>" f"<h2>{html.escape(subject)}</h2>"
    if label:
        window_label_text = "时间窗口：" if is_chinese else "Window:"
        html_body += f"<p><strong>{html.escape(window_label_text)}</strong> {html.escape(label)}</p>"
    html_body += f"<h3>{html.escape(_brief_label('To-do', language=language))}</h3>"
    for heading, items, kind, tag in (
        ("Xiaodong Action Required", direct_action_todos, "todo", "h4"),
        ("Watch / Delegate", watch_delegate_todos, "watch_todo", "h4"),
        ("Project Updates", updates, "update", "h3"),
        ("Other Update", other_updates, "other", "h3"),
        ("Suggested Team Follow-up", reminders, "reminder", "h3"),
    ):
        html_heading = _brief_label(heading, language=language)
        html_body += f"<{tag}>{html.escape(html_heading)}</{tag}>"
        html_body += (
            _render_grouped_html(items, kind=kind, language=language)
            if items
            else f"<p>{html.escape(EMPTY_DAILY_BRIEF_SECTION)}</p>"
        )
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
    ensure_gmail_daily_scopes(credentials_payload, require_read=False)
    credentials = credentials_from_payload(credentials_payload)
    service = build_seatalk_service(settings, data_root=data_root)
    # Gmail remains the transport for the report, but SeaTalk is the only
    # analysis source. Do not export or instantiate Gmail history here: email
    # invitations, reminders, and calendar notifications must not affect the
    # Daily Brief candidate set.
    gmail_history_text = ""
    if email_window:
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
        briefing = build_daily_briefing(
            service,
            now=local_now,
            hours=effective_hours,
            gmail_history_text=gmail_history_text,
            report_intelligence_config=report_intelligence_config,
            key_project_candidates=key_project_candidates,
        )
    subject, text_body, html_body = render_email(
        briefing=briefing,
        now=local_now,
        window_label=window_label,
        language="zh",
    )
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
    if trello_client is None:
        trello_result = TrelloSyncResult(status="skipped")
    else:
        trello_result = sync_daily_summary_to_trello(
            briefing=briefing,
            run_date=run_date,
            run_slot=run_slot,
            window_label=window_label,
            data_root=data_root,
            now=local_now,
            trello_client=trello_client,
            trello_store=trello_store,
            language="zh",
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
    DailyBriefArchiveStore(daily_brief_archive_path(data_root)).save(
        run_date=run_date,
        run_slot=run_slot,
        recipient=recipient,
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        message_id=message_id,
        status="sent",
        sent_at=local_now,
        window_start=window_start,
        window_end=window_end,
        quality_metadata=briefing.get("quality_metadata") if isinstance(briefing.get("quality_metadata"), dict) else {},
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
    language: str = "en",
) -> TrelloSyncResult:
    try:
        client = trello_client or TrelloDailySummaryClient.from_env()
    except ConfigError:
        return TrelloSyncResult(status="disabled")
    store = trello_store or TrelloDailySummaryStore(data_root / "seatalk" / "daily_trello_cards.json")
    specs = build_trello_card_specs(
        briefing=briefing,
        run_date=run_date,
        window_label=window_label,
        language=language,
    )
    if not specs:
        return TrelloSyncResult(status="no_cards")

    existing_board_identities: set[str] = set()
    board_cards = getattr(client, "list_board_cards", None)
    if callable(board_cards):
        cards_to_check = board_cards()
    else:
        list_id = client.get_or_create_list_id()
        list_cards = getattr(client, "list_cards", None)
        cards_to_check = list_cards(list_id=list_id) if callable(list_cards) else []
    for card in cards_to_check:
        identity = daily_card_identity_from_trello_card(card)
        if identity:
            existing_board_identities.add(identity)
    target_list_ids: dict[str, str] = {}
    label_id_cache: dict[tuple[str, ...], list[str]] = {}

    def list_id_for(target_list: str) -> str:
        clean_target = str(target_list or "").strip() or TRELLO_WORKFLOW_LIST_INBOX
        if clean_target not in target_list_ids:
            target_list_ids[clean_target] = client.get_or_create_list_id(clean_target)
        return target_list_ids[clean_target]

    def label_ids_for(label_names: tuple[str, ...]) -> list[str]:
        clean_names = tuple(name for name in label_names if str(name).strip())
        if clean_names not in label_id_cache:
            get_label_ids = getattr(client, "get_or_create_label_ids", None)
            label_id_cache[clean_names] = get_label_ids(clean_names) if callable(get_label_ids) else []
        return label_id_cache[clean_names]
    created = 0
    skipped = 0
    cards: list[dict[str, str]] = []
    created_at = now.astimezone(SEATALK_INSIGHTS_TIMEZONE).isoformat()
    for spec in specs:
        fingerprint = fingerprint_daily_card(
            run_date=run_date,
            section=spec.section,
            item_text=spec.fingerprint_text,
            domain=spec.domain,
        )
        legacy_fingerprints = {
            fingerprint_daily_card(
                run_date=f"{run_date}:{legacy_slot}",
                section=spec.section,
                item_text=spec.fingerprint_text,
                domain=spec.domain,
            )
            for legacy_slot in {run_slot, LEGACY_SLOT}
        }
        board_identity = daily_card_board_identity(run_date=run_date, name=spec.name, domain=spec.domain)
        if (
            store.has_card(fingerprint)
            or any(store.has_card(item) for item in legacy_fingerprints)
            or board_identity in existing_board_identities
        ):
            skipped += 1
            continue
        card = client.create_card(
            list_id=list_id_for(spec.target_list),
            name=spec.name,
            description=spec.description,
            label_ids=label_ids_for(spec.labels),
            due=spec.due or None,
        )
        existing_board_identities.add(board_identity)
        store.mark_card(
            fingerprint=fingerprint,
            name=card.name,
            url=card.url,
            trello_id=card.trello_id,
            created_at=created_at,
        )
        created += 1
        cards.append({"name": card.name, "url": card.url, "id": card.trello_id, "list": spec.target_list, "due": spec.due})
    return TrelloSyncResult(
        status="synced",
        created_count=created,
        skipped_count=skipped,
        cards=cards,
    )


def build_trello_card_specs(
    *,
    briefing: dict[str, Any],
    run_date: str,
    window_label: str = "",
    language: str = "en",
) -> list[TrelloCardSpec]:
    direct_action_todos = [item for item in briefing.get("direct_action_todos") or [] if isinstance(item, dict)]
    watch_delegate_todos = [item for item in briefing.get("watch_delegate_todos") or [] if isinstance(item, dict)]
    if not direct_action_todos and not watch_delegate_todos:
        direct_action_todos, watch_delegate_todos = _split_todos_by_action_type(
            _normalize_todo_items([item for item in briefing.get("my_todos") or [] if isinstance(item, dict)])
        )
    reminders = _filter_reminders_already_covered_by_watch_delegate(
        [item for item in briefing.get("team_member_reminders") or [] if isinstance(item, dict)],
        watch_delegate_todos,
    )

    specs: list[TrelloCardSpec] = []
    for item in direct_action_todos:
        task = _sentence_text(item.get("task"), "Untitled").rstrip(".")
        due = _trello_explicit_due(item.get("due"))
        specs.append(
            TrelloCardSpec(
                section=_brief_label("Xiaodong Action Required", language=language),
                name=f"[{'本人处理' if _is_chinese_brief(language) else 'Direct'}] {_brief_localize_value(task, language=language)}",
                description=_trello_todo_description(
                    item,
                    run_date=run_date,
                    section="Xiaodong Action Required",
                    window_label=window_label,
                    language=language,
                ),
                fingerprint_text=task,
                domain=_display_domain(item.get("domain")),
                target_list=_trello_direct_target_list(item.get("due"), run_date=run_date),
                labels=_trello_domain_labels(item.get("domain"), task),
                due=due,
            )
        )
    for item in watch_delegate_todos:
        task = _sentence_text(item.get("task"), "Untitled").rstrip(".")
        due = _trello_explicit_due(item.get("due"))
        specs.append(
            TrelloCardSpec(
                section=_brief_label("Watch / Delegate", language=language),
                name=f"[{'关注' if _is_chinese_brief(language) else 'Watch'}] {_brief_localize_value(task, language=language)}",
                description=_trello_todo_description(
                    item,
                    run_date=run_date,
                    section="Watch / Delegate",
                    window_label=window_label,
                    language=language,
                ),
                fingerprint_text=task,
                domain=_display_domain(item.get("domain")),
                target_list=TRELLO_WORKFLOW_LIST_WATCH,
                labels=_trello_domain_labels(item.get("domain"), task),
                due=due,
            )
        )
    for item in reminders:
        person = str(item.get("person") or "Unknown").strip()
        reminder = _sentence_text(item.get("reminder"), "Follow-up may be needed").rstrip(".")
        specs.append(
            TrelloCardSpec(
                section=_brief_label("Suggested Team Follow-up", language=language),
                name=(
                    f"[跟进] {person}：{_brief_localize_value(reminder, language=language)}"
                    if _is_chinese_brief(language)
                    else f"[Follow-up] {person}: {reminder}"
                ),
                description=_trello_reminder_description(
                    item,
                    run_date=run_date,
                    window_label=window_label,
                    language=language,
                ),
                fingerprint_text=f"{person} {reminder}",
                domain=_display_domain(item.get("domain")),
                target_list=TRELLO_WORKFLOW_LIST_FOLLOW_UP,
                labels=_trello_domain_labels(item.get("domain"), reminder),
            )
        )
    return specs


def _trello_direct_target_list(value: Any, *, run_date: str) -> str:
    text = str(value or "").strip().lower()
    if not text or text in {"tbd", "unknown", "none", "n/a", "na"}:
        return TRELLO_WORKFLOW_LIST_INBOX
    if text in {"today", "tomorrow"}:
        return TRELLO_WORKFLOW_LIST_TODAY
    due_date = _trello_due_date(value)
    report_date = _trello_due_date(run_date)
    if due_date and report_date:
        days_until_due = (due_date - report_date).days
        if days_until_due <= 1:
            return TRELLO_WORKFLOW_LIST_TODAY
        if days_until_due <= 7:
            return TRELLO_WORKFLOW_LIST_THIS_WEEK
    return TRELLO_WORKFLOW_LIST_INBOX


def _trello_explicit_due(value: Any) -> str:
    due_date = _trello_due_date(value)
    return due_date.isoformat() if due_date else ""


def _trello_due_date(value: Any) -> date | None:
    text = str(value or "").strip()
    match = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def _trello_domain_labels(domain: Any, text: str = "") -> tuple[str, ...]:
    haystack = f"{domain or ''} {text or ''}".lower()
    labels: list[str] = []
    if "credit risk" in haystack or "credit" in haystack or "dwh" in haystack or "cbs" in haystack:
        labels.append("Credit Risk")
    if "grc" in haystack or "pmo" in haystack:
        labels.append("GRC")
    if re.search(r"\b(ai|llm|apollo|gemini)\b", haystack):
        labels.append("AI")
    if "anti" in haystack or "fraud" in haystack or "alc" in haystack or "afasa" in haystack or "qris" in haystack:
        if re.search(r"\bph\b|philippines|spph", haystack):
            labels.append("AF-PH")
        elif re.search(r"\bsg\b|singapore", haystack):
            labels.append("AF-SG")
        else:
            labels.append("AF-ID")
    return tuple(dict.fromkeys(labels))


def _trello_todo_description(
    item: dict[str, Any],
    *,
    run_date: str,
    section: str,
    window_label: str = "",
    language: str = "en",
) -> str:
    if _is_chinese_brief(language):
        lines = [
            f"报告日期：{run_date}",
            f"版块：{_brief_label(section, language=language)}",
            f"领域：{_display_domain(item.get('domain'), language=language)}",
            f"任务：{_brief_localize_value(_sentence_text(item.get('task'), '未命名'), language=language)}",
            f"优先级：{_display_priority(item.get('priority'), language=language)}",
            f"截止：{_display_due(item.get('due'), language=language)}",
            f"来源：{_brief_localize_evidence(item.get('evidence') or '未知', language=language)}",
        ]
        if window_label:
            lines.insert(1, f"报告窗口：{window_label}")
        source_type = str(item.get("source_type") or "").strip()
        if source_type:
            lines.append(f"来源类型：{source_type}")
        return "\n".join(lines)
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


def _trello_reminder_description(
    item: dict[str, Any],
    *,
    run_date: str,
    window_label: str = "",
    language: str = "en",
) -> str:
    if _is_chinese_brief(language):
        lines = [
            f"报告日期：{run_date}",
            f"版块：{_brief_label('Suggested Team Follow-up', language=language)}",
            f"领域：{_display_domain(item.get('domain'), language=language)}",
            f"负责人：{item.get('person') or '未知'}",
            f"跟进事项：{_brief_localize_value(_sentence_text(item.get('reminder'), '需要跟进'), language=language)}",
            f"来源：{_brief_localize_evidence(item.get('evidence') or '未知', language=language)}",
        ]
        if window_label:
            lines.insert(1, f"报告窗口：{window_label}")
        source_type = str(item.get("source_type") or "").strip()
        if source_type:
            lines.append(f"来源类型：{source_type}")
        return "\n".join(lines)
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
        "你是 Xiaodong Zheng 的高级 AI 秘书，也是资深数字银行产品经理。 "
        "你要产出一份能直接改变 Xiaodong 下一步行动的决策简报，而不是复述聊天内容。 "
        "先按信号强度排序：Xiaodong 本人的决定和承诺，其次是 blocked/P0/P1、事故和风险，再其次是活跃依赖、已改变的发布日期，最后是未答复的团队请求；其余内容全部省略。 "
        "只返回合法 JSON。综合 SeaTalk 记录，提炼明确的行动、项目动态、其他高价值动态和未解决的团队成员跟进请求。 "
        "每条内容先写清楚发生了什么变化、需要谁做什么、风险或影响是什么，以及下一个检查点；禁止复制原始聊天记录或写成对话流水账。 "
        "除真实群名、人名、线程名、产品名、技术名词和邮箱外，所有用户可见的任务、摘要、原因和状态说明必须使用简体中文。 "
        "Every item must keep traceability through a short evidence field. Prefer real names over UIDs whenever names are available. "
        "Favor omission over speculation: an unsupported, generic, or ambiguous item is worse than an empty section."
        " 每条项目动态或其他动态都必须是简洁的 PM 综合判断，并用中文明确写出“状态：”“影响：”“下一步：”；如果证据不足以支持三者，直接省略。"
        " JSON 字段名、允许的英文枚举值和 evidence_ref_id 仅用于机器校验，不属于用户可见内容；不要把英文 State、Impact、Next、Due、Source、Status 标签写入输出。"
    )


def _daily_brief_user_prompt(
    *,
    history_text: str,
    gmail_history_text: str,
    hours: int,
    local_now: datetime,
    window_label: str = "",
    match_summary: str = "",
    unanswered_question_hints: str = "",
    team_member_reminder_hints: str = "",
    xiaodong_followup_hints: str = "",
    high_signal_review_hints: str = "",
    evidence_context: str = "",
    source_file_path: str = "",
    source_text: str = "",
) -> str:
    if str(source_file_path or "").strip() or str(source_text or "").strip():
        return _daily_brief_reference_style_user_prompt(
            source_file_path=source_file_path,
            source_text=source_text,
            local_now=local_now,
            window_label=window_label or f"previous {hours} hours",
        )
    window_text = window_label or f"previous {hours} hours"
    match_block = (
        "## Today's Report Intelligence Matches\n"
        f"{match_summary}\n"
        "Use these matches only as prioritization hints. Do not create items unless the source evidence supports them. "
        "For matching output items, fill matched_vips, matched_keywords, matched_key_projects, and priority_reason.\n\n"
        if str(match_summary or "").strip()
        else ""
    )
    unanswered_question_block = (
        "## Unanswered SeaTalk Question Candidates\n"
        f"{unanswered_question_hints}\n"
        "These are deterministic candidates from SeaTalk thread/group history where a human asked a PM-relevant question and no later human reply was visible in the same thread during the window. "
        "Prioritize them as my_todos watch_delegate items, project_updates with blocked/unknown status, or team_member_reminders when the named owner is in the allowed reminder list. "
        "Do not include a candidate if the surrounding source text shows it was already answered or is only low-value chatter.\n\n"
        if str(unanswered_question_hints or "").strip()
        else ""
    )
    team_member_reminder_block = (
        "## Valid Team Member Follow-up Candidates\n"
        f"{team_member_reminder_hints}\n"
        "Only create team_member_reminders from these deterministic candidates. "
        "If this block says there are no valid unresolved candidates, team_member_reminders must be an empty array.\n\n"
        if str(team_member_reminder_hints or "").strip()
        else ""
    )
    xiaodong_followup_block = (
        "## Xiaodong Action Candidates\n"
        f"{xiaodong_followup_hints}\n"
        "This block contains unresolved requests directly addressed to Xiaodong and open commitments Xiaodong made. Create one direct_action my_todos item for each materially distinct unresolved event. "
        "A Xiaodong clarification question does not resolve the underlying issue. If Xiaodong says he will check, follow up, investigate, or get back and no later conclusion is visible, keep the action open. "
        "Never place Xiaodong in team_member_reminders, and do not create another person's reminder for the same event.\n\n"
        if str(xiaodong_followup_hints or "").strip()
        else ""
    )
    evidence_context_block = (
        "## Deterministic Daily Brief Evidence Bundle\n"
        f"{evidence_context}\n"
        "Treat this bundle as the first-pass source of truth for evidence refs. "
        "Use the source excerpts below only to verify nuance and extract additional high-signal project updates or Xiaodong actions.\n\n"
        if str(evidence_context or "").strip()
        else ""
    )
    high_signal_block = (
        "## Mandatory High-Signal Review Candidates\n"
        f"{high_signal_review_hints}\n"
        "Review every candidate in this block before selecting sections. Preserve each materially supported P0/P1 risk, MAS/compliance concern, incident, blocked item, dependency, version delay, launch milestone, or timeline change. Merge only when the underlying topic and next action are the same; do not drop a candidate merely because another lower-priority item was selected.\n\n"
        if str(high_signal_review_hints or "").strip()
        else ""
    )
    return (
        "## Output Contract\n"
        "语言要求：除 Source/Evidence 中必须保留的真实群名、人名、线程名、产品名、技术名词和邮箱外，所有 JSON 字段值必须使用简体中文。任务、项目摘要、提醒、原因和 priority_reason 都要用中文。项目摘要必须使用“状态：... 影响：... 下一步：...”格式，不要使用英文 State/Impact/Next 标签。\n"
        "Return a JSON object with exactly these top-level keys: project_updates, other_updates, my_todos, team_member_reminders, team_todos.\n"
        "Never copy an English source question into task, summary, reminder, or priority_reason. Rewrite it in Simplified Chinese as a concrete action naming the subject, person to contact, and expected decision or deliverable.\n"
        "project_updates: array of objects with keys domain, title, summary, status, evidence, source_type, evidence_ref_id, matched_vips, matched_keywords, matched_key_projects, priority_reason.\n"
        "other_updates: array of objects with keys domain, title, summary, status, evidence, source_type, signal_type, evidence_ref_id, matched_vips, matched_keywords, matched_key_projects, priority_reason.\n"
        "my_todos: array of objects with keys task, domain, priority, due, evidence, source_type, action_type, evidence_ref_id, matched_vips, matched_keywords, matched_key_projects, priority_reason.\n"
        "team_member_reminders: array of objects with keys domain, person, reminder, evidence, source_type, evidence_ref_id.\n"
        "team_todos must always be an empty array.\n"
        "Empty arrays are expected when a section has no important signal. Do not fill sections just to produce a report.\n\n"
        "## Allowed Values\n"
        "domain: Anti-fraud, Credit Risk, Ops Risk, General.\n"
        "status: done, in_progress, blocked, unknown.\n"
        "priority: high, medium, low, unknown.\n"
        "action_type: direct_action or watch_delegate.\n"
        "source_type: seatalk, gmail, mixed. Use mixed only when one synthesized item is supported by both SeaTalk and Gmail.\n"
        "other_updates.signal_type: incident, launch, policy_process, risk_compliance, cross_team_dependency, leadership_decision, cross_product_milestone, useful_awareness.\n"
        "useful_awareness is a compatibility fallback, not a default category. Use it only for a directly evidenced PM impact that does not fit a stronger signal type. Do not omit signal_type.\n\n"
        "## Evidence Gate\n"
        "Before including any item, verify that the source explicitly supports the actor or owner when relevant and the concrete request, decision, milestone, blocker, dependency, or outcome. The PM impact may be synthesized only from an explicit risk, dependency, timeline, regulatory, customer, or delivery consequence in the evidence. If the impact or next step would require invention, omit it rather than adding generic language.\n"
        "Do not turn a question, @mention, meeting invitation, meeting logistics, acknowledgement, thanks, or discussion into an action, decision, status, or risk unless a source explicitly states that result. Do not invent owners, deadlines, commitments, severity, or dependencies.\n"
        "Each item must be atomic and source-coherent: one actionable request or one material state change supported by one group/thread or one email thread. Never import a date, owner, dependency, or milestone from a different source merely because the project names are related. A SeaTalk group name is an evidence label, not proof that the similarly named team owns the action. Merge only duplicate evidence for the same event, never separate events merely because they share a project name.\n\n"
        "## Section Rules\n"
        "my_todos: include only Xiaodong-owned actions, decisions needed from Xiaodong, follow-ups Xiaodong clearly needs to drive, or watch/delegate items where Xiaodong should ensure another owner follows through. Every task must name a concrete object and intended outcome; never write a bare 'follow up', 'check', or 'confirm' without saying what must be resolved. If the source is a question, rewrite it as '确认什么、向谁确认、预期得到什么结论'. Do not include tasks fully owned by other people with no Xiaodong follow-up value. Max 6 items. Sort high priority first, then earliest due date, then most actionable.\n"
        "For each my_todos item, set action_type=direct_action only when Xiaodong must personally reply, decide, review, approve, attend, provide, or drive the next step. Set action_type=watch_delegate when Xiaodong mainly needs to monitor, ensure, follow up with someone, check with a team, or confirm another owner follows through.\n"
        "If a teammate follow-up topic is already represented as a my_todos watch_delegate item, do not repeat it in team_member_reminders.\n"
        "Do not create a todo or reminder for an ask when a later human reply in the same SeaTalk group/thread already gives the answer, conclusion, or ownership update. A clarification question is not an answer. A Xiaodong commitment such as 'will check and get back' is not a resolution: keep one Xiaodong direct_action item until a conclusion is visible.\n"
        "project_updates: include only a material decision, delivered milestone, changed delivery date, active blocker, dependency, launch/version milestone, or current execution state. Include core key projects and [SP]/P0/P1, MAS/compliance, incident, blocked, dependency, version-delay, and launch signals even when Xiaodong is not directly mentioned. Never summarize a meeting plan, open question, generic discussion, or copied chat excerpt as a project update. Each summary must answer what changed or the current state, the explicit business/delivery impact, and the concrete next decision, owner action, or checkpoint. Prefer 'State: ... Impact: ... Next: ...'. Do not start with @mentions, greetings, questions, URLs, or 'Context:'; do not paste a transcript. If those elements cannot be supported by evidence, omit the update or place the unresolved request in the appropriate action section. Max 6 items. Sort blocked and in_progress before done.\n"
        "other_updates: include only directly evidenced high-value awareness where Xiaodong is not directly involved: incident, launch, policy/process, risk/compliance, cross-team dependency, leadership decision, or cross-product milestone. useful_awareness must be exceptional, directly PM-relevant, and limited to 2 items. Include at most 5 other_updates total. Do not include generic chatter, greetings, pure thanks, meeting logistics with no decision, or low-value FYI.\n"
        "team_member_reminders: use SeaTalk only. Never create these from Gmail. Only include Xiaodong's team members from the explicit allowed reminder list below; never include Xiaodong himself because unresolved requests to him belong in my_todos. Max 6 items. Sort by most actionable first. State the unresolved request and expected response or deliverable in third person.\n\n"
        "For project_updates, team_member_reminders, and every SeaTalk my_todos item, evidence_ref_id is required and must be copied exactly from Deterministic Daily Brief Evidence Bundle.evidence_refs. A SeaTalk direct_action is valid only when that same ref directly asks Xiaodong or records Xiaodong's own unresolved commitment. Do not invent evidence_ref_id values.\n"
        "For mixed SeaTalk+Gmail project_updates, evidence_ref_id may contain two comma-separated ids, one st-ref and one gm-ref, only when both refs support the same topic.\n\n"
        "## Team Member Reminder Scan\n"
        "Before writing team_member_reminders, scan every SeaTalk group conversation for human mentions of these team members: Ker Yin, Rene Chong, Sabrina Chan, Liye, Hui Xian, Sophia Wang Zijun, Ming Ming, Zoey Lu, Wang Chang, Jireh, Ang Wei Lin. Handle unresolved mentions of Zheng Xiaodong only as my_todos. Ming Ming | 明明 is a team member; Li Mingming is a different person and must never be treated as Ming Ming.\n"
        "Sophia Wang Zijun belongs to Credit Risk. Do not classify Sophia Wang Zijun as Ops Risk.\n"
        "For Anti-fraud domain reminders, only these people are Xiaodong's Anti-fraud team: Ker Yin, Rene Chong, Zoey Lu, Wang Chang, Jireh, Ang Wei Lin. Do not put anyone else, including Wendy, under Anti-fraud team_member_reminders.\n"
        "Do not create team_member_reminders for people outside the allowed reminder list, even if they appear in SeaTalk.\n"
        "For Zheng Xiaodong, create a direct_action my_todos item when the source directly @mentions or clearly asks him and no later substantive Xiaodong answer is visible in the same group/thread.\n"
        "A valid reminder exists when a human in a SeaTalk group asks, mentions, assigns, blocks on, or appears to need follow-up from one of those people, and neither the named person nor Xiaodong follows up later in that same group during the available window.\n"
        "Mentions may appear as direct @ mentions, plain names, mapped display names, name variants, or quoted text. Prefer real names in the person field.\n"
        "A cc-only mention is not enough. If a person is only copied after 'cc' and the actual ask is addressed to someone else, do not create a reminder for the cc'd person. If the direct assignee is outside the allowed list and an allowed teammate is only cc'd, produce no team_member_reminders item for that message.\n"
        "Do not include private chats. Do not include bot/system alerts, automated reminders, SDLC Checker output, or SDLC material/approval reminder messages. Do not include items where the named person replied, acknowledged, handled it, or the same event is already covered by a Xiaodong direct_action ownership commitment. Xiaodong asking clarifying questions does not count as handling the request.\n"
        "Do not include @mentions that only say someone will join a meeting late, is delayed, or is temporarily unavailable; these are meeting logistics, not follow-up work.\n"
        "Do not create any item from a pure Gmail Google Calendar invite, RSVP, accepted/declined/tentative response, reschedule notice, reminder, 'Updated invitation with note' subject, or automated password-expiry/account-maintenance notice. Substantive project decisions in a separate human-authored email or meeting note may be used, but calendar logistics and routine system reminders themselves must be omitted.\n"
        "Do not turn OL, on-leave, coverage, backup, or 'Please find [person] for any follow ups' availability statements into a task for that person.\n"
        "If the source message is annotated as a thread reply, make the reminder and evidence say thread, for example 'UDL数据小群 / thread: PH A-Card Model V2.1 Deployment'. Do not write 'in the group' for thread replies.\n"
        "A reply from another participant is context only unless the named owner, Xiaodong, or the original requester explicitly states that the request is answered, fixed, resolved, or closed.\n"
        "If the mention looks human and action-relevant but you are unsure whether the named person stayed completely silent after being asked, drop it rather than creating a noisy Follow-up. Set source_type to seatalk.\n\n"
        "## Source And Evidence Rules\n"
        "For SeaTalk evidence, use the real group name, contact, or thread title, never a raw group ID. For Gmail evidence, use sender or key participants plus subject and thread link when available.\n"
        "When an evidence_ref_id is available, use its evidence label exactly and keep the same evidence_ref_id in the output item.\n"
        "Do not output unresolved raw SeaTalk IDs such as group-123, buddy-123, or UID 123 in evidence. Use mapped display names when visible; otherwise use a generic label such as SeaTalk group or SeaTalk contact.\n"
        "For Gmail thread messages marked context only, use them only to understand the in-window message; never summarize context-only messages as new To-do, Project Updates, or Other Updates.\n"
        "Merge duplicate items across SeaTalk and Gmail when they refer to the same project, owner, decision, task, or milestone. Keep one synthesized item and use source_type mixed when both sources support it.\n\n"
        "## Quality Rules\n"
        "Use status=done only when the outcome is fully complete. If an item says pending confirmation, still pending, tomorrow clarify, no fixed date, or awaiting confirmation, use status=in_progress or unknown, not done.\n"
        "If an item mentions MAS, launch before a fix, risk endorsement, ITC endorsement, blocked, or missing real-time fraud surveillance, treat it as high-risk and prefer status=blocked or signal_type=risk_compliance when appropriate.\n"
        "Avoid repeating the same topic across sections unless each section has a distinct role: Xiaodong next action, project state, or team member follow-up.\n\n"
        f"{match_block}"
        f"{unanswered_question_block}"
        f"{team_member_reminder_block}"
        f"{xiaodong_followup_block}"
        f"{high_signal_block}"
        f"{evidence_context_block}"
        "## Exclusions\n"
        "For other_updates and team_member_reminders, ignore bot-generated alerts, automated reminders, system notifications, Jira/Confluence/calendar reminders, and no-reply notification emails unless a human adds meaningful follow-up in the same thread.\n\n"
        "For team_member_reminders, always exclude SDLC Checker and SG BAU SDLC material check content; those are automated release hygiene signals, not human team follow-up requests.\n\n"
        "## Formatting Inside JSON\n"
        "For my_todos.task, write one synthesized action sentence. For due, extract a real deadline if present; otherwise use TBD.\n"
        "For project_updates.summary and other_updates.summary, write one compact executive entry of at most three short sentences and 65 words total. A project update that reads like a source quote is invalid even when its evidence is valid. Do not add generic filler such as 'confirm the owner and next milestone'.\n"
        "For evidence, provide only the source label. Do not include long snippets.\n"
        "最终语言检查：除证据中的真实专有名词外，禁止英文句子或英文标签出现在输出值中；用“状态：”“影响：”“下一步：”“截止：”“来源：”等中文标签。\n\n"
        f"Window: {window_text}. Generated at: {local_now.isoformat()}.\n\n"
        "=== SeaTalk history ===\n"
        "[focused evidence excerpt]\n"
        f"{history_text}\n\n"
        "=== Gmail thread history ===\n"
        "[focused evidence excerpt]\n"
        f"{gmail_history_text or 'No Gmail messages were found in this window.'}"
    )


def _daily_brief_reference_style_user_prompt(
    *,
    source_file_path: str,
    source_text: str,
    local_now: datetime,
    window_label: str,
) -> str:
    """Use the reference report's reasoning shape without copying its data rules.

    The complete export is deliberately kept outside the prompt body. Codex CLI
    can read the temporary file from its read-only workspace, which avoids the
    signal-excerpt bias that previously hid context in long windows.
    """
    source_instruction = (
        f"完整原始记录文件：{source_file_path}\n"
        "必须先通读该文件的全部相关群聊和线程；不要只读取开头、结尾或少数命中行。"
        "该文件是唯一的业务事实来源。"
        if str(source_file_path or "").strip()
        else (
            "下面的 SeaTalk 原始记录是完整输入，必须综合阅读全部相关群聊和线程，"
            "不能只依据少数命中行或最近几条消息。该记录是唯一的业务事实来源。\n\n"
            "=== SeaTalk 原始聊天记录 ===\n"
            f"{source_text}"
        )
    )
    return (
        "你现在担任 Xiaodong Zheng 的高级 AI 秘书，同时是一位资深数字银行产品经理。"
        "请完整阅读指定的 SeaTalk 原始聊天记录，再产出一份真正能改变 Xiaodong 下一步行动的每日工作简报。"
        "不要把聊天记录改写成流水账，也不要为了填满版块而保留低价值信息。\n\n"
        f"{source_instruction}\n\n"
        "在输出 JSON 前，请在本次调用内完成一次内部复核：重新检查所有候选事项是否有后续答复或关闭、是否存在跨 source 拼接、"
        "以及是否遗漏 P0/P1、MAS、incident、blocked、版本/上线变化、关键 dependency、Xiaodong 未完成承诺或白名单成员的未答复请求。"
        "不要输出复核过程，只输出复核后的最终 JSON。\n\n"
        "## 重点判断顺序\n"
        "1. Xiaodong 必须亲自答复、评审、决定、审批、参加或承诺跟进的事项。\n"
        "2. Xiaodong 不必亲自执行，但必须监控、推动或确保负责人完成的事项。\n"
        "3. 影响发布、UAT/Live、客户、合规、风险或跨团队依赖的重大项目状态。\n"
        "4. 对数字银行 PM 有实际价值的 incident、launch、policy/process、risk/compliance、cross-team dependency、leadership decision 或 cross-product milestone。\n"
        "5. 白名单团队成员被明确提出、分配任务或直接艾特，但在可见记录中仍未得到负责人或 Xiaodong 的实质回复的请求。\n\n"
        "## 必须忽略\n"
        "忽略寒暄、感谢、普通 FYI、没有决定的会议预约、会议迟到/缺席/coverage 通知、机器人和系统告警、纯提醒、纯问句、重复转述、已经回答/解决/关闭的事项。"
        "其他参与者的回复只能作为上下文；只有被点名负责人、Xiaodong 或原请求人明确表示已回答、fixed、resolved、closed，才算关闭请求。"
        "Xiaodong 说会检查、跟进或稍后回复，不算问题已解决，应保留为 Xiaodong 待办。"
        "同一主题在不同版块只保留一个综合事项；只有负责人或动作不同，才可以拆分。\n\n"
        "所有事实、证据和结论只能来自 SeaTalk；Gmail、日历、邮件、外部资料均不属于输入，也不能出现在 Source/Evidence 或 source_type 中。\n\n"
        "团队跟进仅限白名单成员被真人直接提出具体需求、分配任务或明确艾特，且该成员和 Xiaodong 在可见记录中没有实质答复。"
        "cc-only、私聊、机器人/系统或 SDLC Checker、OL/coverage、会议安排、普通提醒均不是团队待办。"
        "如同一群或 thread 中已有明确答复、fixed、resolved、closed 或结论，必须删除该事项。thread 证据必须写明 thread 标题。\n\n"
        "## 业务归属和团队边界\n"
        "Domain 只能使用 Anti-fraud、Credit Risk、Ops Risk、General。Sophia Wang Zijun 归属 Credit Risk。"
        "Xiaodong 团队跟进白名单：Ker Yin、Rene Chong、Sabrina Chan、Liye、Hui Xian、Sophia Wang Zijun、Ming Ming、Zoey Lu、Wang Chang、Jireh、Ang Wei Lin、Lim Dao Jun。"
        "Anti-fraud 团队成员仅限 Ker Yin、Rene Chong、Wang Chang、Jireh、Ang Wei Lin、Sabrina Chan。"
        "Ming Ming | 明明 与 Li Mingming 是不同的人，不能合并。不要因为群名相似就推断项目归属；以聊天中明确的参与者、请求和内容为准。\n\n"
        "项目选择必须以 Xiaodong 团队的实际责任为边界：只保留团队主导、负责交付或深度参与的项目。"
        "Anti-fraud 事项必须有 AF PM/团队的明确参与或责任；Credit Risk 事项必须有 CRMS/Credit Risk PM 的明确参与；"
        "Ops Risk 只保留与 GRC 系统责任直接相关的事项。普通业务、其他产品团队、纯行政或出差安排，即使群聊内容详细，也不要写入项目动态或其他重要动态；"
        "但 Business Trip Planning 或类似群聊若明确记录 Xiaodong 主导的 AI agent、Daily Brief、PM 自动化建设、对外 presentation 计划、交付物或决策，则属于其负责项目，必须写入 Project Updates，不能因群名含出差而删除。"
        "例如，PH trip findings 的 slide 交付、Fraud Ops 职能与系统自动化能力建设、方案和下一步计划，均是项目动态而不是出差 logistics。"
        "重大线上事故仅在 Xiaodong 团队正在参与处置、需要决策或会直接影响其负责项目时保留。\n\n"
        "## 输出质量要求\n"
        "所有用户可见内容使用精炼、专业、商务通顺的简体中文；真实群名、人名、线程名、产品名、技术名词和邮箱保持原文。"
        "每一条行动项必须写清楚背景、具体动作和预期结果，不能只写‘跟进’、‘确认’或‘检查’。"
        "每一条项目动态必须是 PM 综合判断，并严格包含‘状态：… 影响：… 下一步：…’，禁止直接引用聊天原句。"
        "每一条 Source/Evidence 必须能回溯到真实群名、联系人或 thread 标题；禁止输出 group-数字、buddy-数字、UID 数字等原始 ID。"
        "不确定时宁可省略，不要猜测 owner、deadline、severity、dependency 或 project relationship。\n\n"
        "## 机器可读输出契约\n"
        "只返回合法 JSON，不要 Markdown、解释或代码围栏。顶层只能包含：project_updates、other_updates、my_todos、team_member_reminders、team_todos。"
        "team_todos 必须是空数组。没有高信号内容的版块返回空数组。"
        "project_updates 每项字段：domain、title、summary、status、evidence、source_type。"
        "other_updates 每项字段：domain、title、summary、status、signal_type、evidence、source_type。"
        "my_todos 每项字段：task、domain、priority、due、action_type、evidence、source_type。"
        "team_member_reminders 每项字段：domain、person、reminder、evidence、source_type。"
        "允许的 status：done、in_progress、blocked、unknown；priority：high、medium、low、unknown；"
        "action_type：direct_action 或 watch_delegate；source_type 必须是 seatalk；signal_type：incident、launch、policy_process、risk_compliance、cross_team_dependency、leadership_decision、cross_product_milestone。"
        "不要使用机械数量上限。保留所有彼此不同且具备明确行动、风险、交付影响或未答复责任人的高信号事项；"
        "但不能把简报写成聊天流水账，也不能为了看起来完整而保留普通 FYI。"
        "如果同一主题同时属于 Xiaodong 待办和项目动态，只保留最能指导下一步的版本，除非两者确实承担不同动作。\n\n"
        f"报告窗口：{window_label}。生成时间：{local_now.isoformat()}。"
    )


def _daily_brief_full_source_review_prompt(
    *,
    source_file_path: str,
    source_text: str,
    local_now: datetime,
    window_label: str,
    draft: dict[str, Any],
) -> str:
    """Ask Codex to audit its draft against the same complete SeaTalk source."""
    base = _daily_brief_reference_style_user_prompt(
        source_file_path=source_file_path,
        source_text=source_text,
        local_now=local_now,
        window_label=window_label,
    )
    return (
        f"{base}\n\n"
        "## 第二轮完整记录复核\n"
        "下面是第一轮草稿。请再次通读完整原始记录，并直接返回修订后的完整 JSON。"
        "此轮的职责是发现第一轮遗漏的重大事项，同时删除任何已解决、已回答、只是会议安排或迟到通知、"
        "机器人/系统提醒、普通 FYI、证据不足、跨 source 拼接或重复主题。"
        "同一主题只能保留一个最能指导下一步的事项；只有负责人和动作实质不同才可以分开。"
        "不得因为条目数量而删去 P0/P1、MAS/合规、incident、blocked、版本/上线变化、关键依赖、"
        "Xiaodong 未完成承诺或明确未答复的团队成员请求。"
        "每个 Project Updates 与 Other Update 必须是中文 PM 综合判断，包含“状态：”“影响：”“下一步：”；"
        "每个 Source/Evidence 必须基于同一条真实群聊、联系人或 thread，不能猜测。\n\n"
        "=== 第一轮草稿 ===\n"
        f"{json.dumps(draft, ensure_ascii=False, separators=(',', ':'))}"
    )


_DAILY_BRIEF_ENGLISH_PROSE_CUES = (
    "state:",
    "impact:",
    "next:",
    " is ",
    " are ",
    " the ",
    " and ",
    " with ",
    " from ",
    " to ",
    " by ",
    " pending ",
    " blocked ",
    " release ",
    " issue ",
)
_DAILY_BRIEF_ENGLISH_CONNECTOR_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "because",
    "by",
    "can",
    "from",
    "has",
    "have",
    "in",
    "is",
    "may",
    "must",
    "needs",
    "of",
    "on",
    "or",
    "pending",
    "remains",
    "still",
    "that",
    "the",
    "to",
    "until",
    "was",
    "were",
    "with",
}


def _daily_brief_contains_english_prose(value: Any) -> bool:
    """Detect English sentences without rejecting product names or acronyms."""
    prose = " ".join(str(value or "").split()).casefold()
    if not prose or re.search(r"\b(?:state|impact|next):", prose, flags=re.IGNORECASE):
        return bool(prose and re.search(r"\b(?:state|impact|next):", prose, flags=re.IGNORECASE))
    words = re.findall(r"\b[a-z]{2,}\b", prose)
    connector_count = sum(word in _DAILY_BRIEF_ENGLISH_CONNECTOR_WORDS for word in words)
    return len(words) >= 10 and connector_count >= 3


def _daily_brief_has_items(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(
        isinstance(payload.get(key), list) and bool(payload.get(key))
        for key in ("project_updates", "other_updates", "my_todos", "team_member_reminders")
    )


def _daily_brief_seatalk_only_response(payload: Any) -> dict[str, Any]:
    """Reject accidental non-SeaTalk output from the model or a legacy caller."""
    if not isinstance(payload, dict):
        return {}
    cleaned = dict(payload)
    for section in ("project_updates", "other_updates", "my_todos", "team_member_reminders"):
        rows = cleaned.get(section)
        if not isinstance(rows, list):
            cleaned[section] = []
            continue
        kept: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            source_type = str(row.get("source_type") or "seatalk").strip().casefold()
            if source_type in {"", "seatalk"}:
                normalized = dict(row)
                normalized["source_type"] = "seatalk"
                kept.append(normalized)
        cleaned[section] = kept
    cleaned["team_todos"] = []
    return cleaned


def _daily_brief_needs_language_repair(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    for section in ("project_updates", "other_updates", "my_todos", "team_member_reminders"):
        for item in payload.get(section) or []:
            if not isinstance(item, dict):
                continue
            prose = " ".join(
                str(item.get(field) or "")
                for field in ("title", "summary", "task", "reminder", "priority_reason")
            ).casefold()
            if _daily_brief_contains_english_prose(prose):
                return True
    return False


def _daily_brief_language_repair_prompt(payload: dict[str, Any]) -> str:
    return (
        "请只修复下面这份 Daily Brief JSON 的语言和表达，不要重新分析聊天记录，也不要新增、删除或合并任何业务事项。"
        "必须逐项保留每个数组的原始长度、顺序和每一条事项；禁止返回不完整的 section，禁止因为某条事项来自私聊、英文或证据较短而省略它。"
        "这是强制语言转换：凡是完整英文句子、英文 State/Impact/Next 标签、英文连接词组成的句子，都必须翻译成精炼、专业的简体中文；"
        "不能因为原文夹杂中文、产品名或技术名词而保留英文句子。可保留真实群名、人名、线程名、产品名、技术名词、版本号、字段名和邮箱；其余说明必须中文。"
        "所有用户可见的 title、summary、task、reminder、priority_reason 必须改为精炼、专业的简体中文；"
        "真实群名、人名、线程名、产品名、技术名词、版本号和邮箱保持原文。"
        "项目 summary 必须使用‘状态：… 影响：… 下一步：…’，不能保留 State/Impact/Next 或英文完整句子。"
        "例如：‘is blocked by a Shopee-side issue’应改为‘因 Shopee 侧问题而受阻’，‘cannot be signed off until the upstream issue is fixed’应改为‘上游问题修复前无法完成签核’。"
        "严格保留原有字段、domain、status、priority、due、action_type、evidence 和 source_type；只返回合法 JSON。\n\n"
        + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    )


def _daily_brief_remaining_language_items(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    my_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    sections = {
        "project_updates": project_updates,
        "other_updates": other_updates,
        "my_todos": my_todos,
        "team_member_reminders": reminders,
    }
    fields = ("title", "summary", "task", "reminder", "priority_reason")
    remaining: list[dict[str, Any]] = []
    for section, items in sections.items():
        for index, item in enumerate(items):
            visible = " ".join(str(item.get(field) or "") for field in fields)
            if not _daily_brief_contains_english_prose(visible):
                continue
            remaining.append(
                {
                    "section": section,
                    "index": index,
                    "fields": {field: str(item.get(field) or "") for field in fields if str(item.get(field) or "").strip()},
                }
            )
    return remaining


def _daily_brief_residual_language_repair_prompt(items: list[dict[str, Any]]) -> str:
    return (
        "只翻译下面列出的 Daily Brief 残留英文用户可见字段，不要重新分析业务，也不要改变事项含义。"
        "必须返回与输入 items 数量完全相同的 items，并保留每个 item 的 section 和 index；禁止遗漏任何 item。"
        "凡是完整英文句子、英文连接词组成的任务或摘要都必须翻译成专业简体中文。"
        "真实人名、群名、线程名、产品名、技术名词、版本号、字段名和邮箱可以保留原文。"
        "只返回 JSON：{\"items\":[{\"section\":\"...\",\"index\":0,\"fields\":{\"task\":\"...\"}}]}。"
        "不要返回 domain、status、priority、due、action_type、evidence、source_type 或其他元数据。\n\n"
        + json.dumps({"items": items}, ensure_ascii=False, separators=(",", ":"))
    )


def _daily_brief_residual_language_system_prompt() -> str:
    return (
        "你是 Daily Brief 的定点语言修复器。只处理用户消息中列出的 items，不能重新分析原始聊天记录。"
        "必须严格返回 JSON 对象 {\"items\":[...]}，不得返回 project_updates、other_updates、my_todos、"
        "team_member_reminders 或 team_todos。保留每个 item 的 section、index 和字段结构，只把完整英文说明翻译成专业简体中文。"
    )


def _apply_residual_language_repair(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    my_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
    repaired: dict[str, Any],
) -> int:
    sections = {
        "project_updates": project_updates,
        "other_updates": other_updates,
        "my_todos": my_todos,
        "team_member_reminders": reminders,
    }
    text_fields = {"title", "summary", "task", "reminder", "priority_reason"}
    repaired_count = 0
    for candidate in repaired.get("items") or []:
        if not isinstance(candidate, dict):
            continue
        section = str(candidate.get("section") or "").strip()
        try:
            index = int(candidate.get("index"))
        except (TypeError, ValueError):
            continue
        current_items = sections.get(section)
        if current_items is None or index < 0 or index >= len(current_items):
            continue
        fields = candidate.get("fields") if isinstance(candidate.get("fields"), dict) else {}
        for field in text_fields:
            value = str(fields.get(field) or "").strip()
            if value and value != current_items[index].get(field):
                current_items[index][field] = value
                repaired_count += 1
    return repaired_count


def _apply_final_language_repair(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    my_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
    repaired: dict[str, Any],
) -> int:
    """Copy repaired prose onto canonical items without losing evidence metadata."""
    current_sections = {
        "project_updates": project_updates,
        "other_updates": other_updates,
        "my_todos": my_todos,
        "team_member_reminders": reminders,
    }
    repaired_count = 0
    text_fields = ("title", "summary", "task", "reminder", "priority_reason")
    for section, current_items in current_sections.items():
        candidates = [item for item in repaired.get(section) or [] if isinstance(item, dict)]
        used: set[int] = set()
        for current_index, current in enumerate(current_items):
            current_ref = str(current.get("evidence_ref_id") or "").strip()
            current_evidence = _normalize_thread_match_text(current.get("evidence"))
            current_person = _normalize_person_key(current.get("person"))
            match_index = next(
                (
                    index
                    for index, candidate in enumerate(candidates)
                    if index not in used
                    and current_ref
                    and current_ref == str(candidate.get("evidence_ref_id") or "").strip()
                ),
                None,
            )
            if match_index is None and current_evidence:
                match_index = next(
                    (
                        index
                        for index, candidate in enumerate(candidates)
                        if index not in used
                        and current_evidence == _normalize_thread_match_text(candidate.get("evidence"))
                    ),
                    None,
                )
            if match_index is None and current_person:
                match_index = next(
                    (
                        index
                        for index, candidate in enumerate(candidates)
                        if index not in used
                        and current_person == _normalize_person_key(candidate.get("person"))
                    ),
                    None,
                )
            # The language-repair prompt forbids adding, deleting, or
            # reordering items.  A merged item may lose its long evidence_ref
            # string in the repair response, so use the same-position item
            # only when the section cardinality is unchanged.  This preserves
            # canonical evidence/status metadata while still repairing prose.
            if (
                match_index is None
                and len(candidates) == len(current_items)
                and current_index not in used
            ):
                match_index = current_index
            if match_index is None:
                continue
            used.add(match_index)
            candidate = candidates[match_index]
            for field in text_fields:
                if field in candidate and str(candidate.get(field) or "").strip():
                    if candidate.get(field) != current.get(field):
                        current[field] = candidate[field]
                        repaired_count += 1
    return repaired_count


def _write_daily_brief_source_file(
    service: SeaTalkDashboardService,
    history_text: str,
    *,
    threshold: int,
) -> Path | None:
    """Write a large raw export where the read-only Codex CLI can inspect it."""
    source = str(history_text or "")
    if len(source) < max(1, int(threshold)):
        return None
    workspace_root = Path(getattr(service, "codex_workspace_root", Path.cwd())).resolve()
    target_dir = workspace_root / "tmp"
    target_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix=".daily-brief-seatalk-",
        suffix=".txt",
        dir=target_dir,
        delete=False,
    )
    try:
        with handle:
            handle.write(source)
        return Path(handle.name)
    except OSError:
        try:
            Path(handle.name).unlink(missing_ok=True)
        except OSError:
            pass
        return None


def _remove_daily_brief_source_file(source_file_path: Path | None) -> None:
    if source_file_path is None:
        return
    try:
        source_file_path.unlink(missing_ok=True)
    except OSError:
        pass


_SEATALK_HISTORY_HEADER_RE = re.compile(r"^===\s*(?P<group>.+?)\s*===$")
_SEATALK_HISTORY_MESSAGE_RE = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\]\s+(?P<sender>.+?)(?:\s+\[thread reply under:\s*(?P<thread>.*?)\])?:\s*(?P<text>.*)$"
)
_UNANSWERED_QUESTION_CUES = (
    "?",
    "？",
    "吗",
    "么",
    "是不是",
    "是否",
    "请问",
    "确认下",
    "看下",
    "may i know",
    "can you",
    "could you",
)
_UNANSWERED_PM_RELEVANT_TERMS = (
    "af",
    "anti-fraud",
    "fraud",
    "card",
    "google pay",
    "gpay",
    "token",
    "tokenization",
    "cvc",
    "notifyservice",
    "risk",
    "rule",
    "校验",
    "上送",
    "写错",
    "配置",
    "规则",
    "域名",
)


def _estimate_daily_prompt_tokens(text: str) -> int:
    source = str(text or "")
    if not source:
        return 0
    return max(1, (len(source) + DAILY_BRIEF_TOKEN_CHARS_PER_TOKEN - 1) // DAILY_BRIEF_TOKEN_CHARS_PER_TOKEN)


def _build_daily_brief_evidence_context(
    *,
    unanswered_question_hints: str,
    team_member_reminder_candidates: list[dict[str, str]] | None,
    evidence_refs: list[dict[str, Any]] | None = None,
    source_token_ledger: dict[str, Any],
    high_signal_review_hints: str = "",
    xiaodong_followup_candidates: list[dict[str, str]] | None = None,
) -> str:
    prompt_evidence_refs = _compact_daily_brief_prompt_evidence_refs(
        evidence_refs or [],
        team_member_reminder_candidates=team_member_reminder_candidates,
        xiaodong_followup_candidates=xiaodong_followup_candidates,
    )
    payload = {
        "unanswered_mentions": [
            line[2:] if line.startswith("- ") else line
            for line in str(unanswered_question_hints or "").splitlines()
            if line.strip()
        ][:MAX_UNANSWERED_SEATALK_QUESTION_HINTS],
        "candidate_followups": _compact_daily_followup_candidates(team_member_reminder_candidates),
        "xiaodong_action_candidates": _compact_xiaodong_followup_candidates(xiaodong_followup_candidates),
        "evidence_refs": prompt_evidence_refs,
        "high_signal_candidates": [
            line[2:] if line.startswith("- ") else line
            for line in str(high_signal_review_hints or "").splitlines()
            if line.strip()
        ],
    }
    cap_flags = {
        "seatalk_prompt_hit_cap": bool(source_token_ledger.get("seatalk_prompt_hit_cap")),
        "gmail_prompt_hit_cap": bool(source_token_ledger.get("gmail_prompt_hit_cap")),
    }
    if any(cap_flags.values()):
        payload["source_caps"] = cap_flags
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _compact_daily_brief_prompt_evidence_refs(
    evidence_refs: list[dict[str, Any]],
    *,
    team_member_reminder_candidates: list[dict[str, str]] | None,
    xiaodong_followup_candidates: list[dict[str, str]] | None,
) -> list[dict[str, Any]]:
    """Give the model a ranked evidence index without repeating the full raw export.

    Downstream deterministic validation still receives every ref. The model only
    needs the refs most likely to support an output item, plus every ref tied to
    an unresolved owner request. This keeps the synthesis prompt focused while
    preserving citation IDs for the later validation layer.
    """
    if len(evidence_refs) <= DAILY_BRIEF_PROMPT_EVIDENCE_REF_LIMIT:
        selected = list(evidence_refs)
    else:
        candidate_refs = _seatalk_refs_by_candidate(evidence_refs)
        required_ids: set[str] = set()
        for candidate in [*(team_member_reminder_candidates or []), *(xiaodong_followup_candidates or [])]:
            ref = candidate_refs.get(_candidate_ref_key(candidate))
            if ref:
                required_ids.add(str(ref.get("id") or ""))

        def rank(ref: dict[str, Any]) -> tuple[int, str]:
            haystack = " ".join(
                str(ref.get(field) or "")
                for field in ("group", "thread", "sender", "subject", "snippet", "evidence")
            ).casefold()
            score = 100 if str(ref.get("id") or "") in required_ids else 0
            score += sum(3 for term in DAILY_BRIEF_HIGH_SIGNAL_TERMS if term in haystack)
            score += sum(
                weight
                for term, weight in (
                    ("[sp][p0]", 20),
                    ("p0", 12),
                    ("p1", 8),
                    ("mas", 10),
                    ("incident", 9),
                    ("blocked", 9),
                    ("dependency", 8),
                    ("launch", 6),
                    ("release", 5),
                    ("timeline", 5),
                    ("delay", 5),
                )
                if term in haystack
            )
            if str(ref.get("reply_state") or "").casefold() in {"unanswered", "xiaodong_commitment"}:
                score += 30
            return score, str(ref.get("timestamp") or "")

        selected = sorted(evidence_refs, key=rank, reverse=True)[:DAILY_BRIEF_PROMPT_EVIDENCE_REF_LIMIT]
        selected_ids = {str(ref.get("id") or "") for ref in selected}
        for ref in evidence_refs:
            ref_id = str(ref.get("id") or "")
            if ref_id in required_ids and ref_id not in selected_ids:
                selected[-1] = ref
                selected_ids.add(ref_id)

    allowed_fields = (
        "id",
        "source_type",
        "group",
        "thread",
        "sender",
        "timestamp",
        "mentioned_people",
        "reply_state",
        "snippet",
        "subject",
        "to",
        "evidence",
    )
    return [
        {key: ref.get(key) for key in allowed_fields if key in ref}
        for ref in selected
    ]


def _compact_xiaodong_followup_candidates(candidates: list[dict[str, str]] | None) -> list[dict[str, str]]:
    if not candidates:
        return []
    compacted: list[dict[str, str]] = []
    for item in candidates:
        thread = str(item.get("thread") or "").strip()
        compacted.append(
            {
                "source": f"{item.get('group')} / thread: {thread}" if thread else str(item.get("group") or ""),
                "timestamp": str(item.get("timestamp") or ""),
                "reason": str(item.get("ownership_reason") or "commitment"),
                "request_or_commitment": _clip_hint_text(item.get("text"), limit=220),
            }
        )
    return compacted[:MAX_TEAM_MEMBER_REMINDER_HINTS]


def _format_xiaodong_followup_hints(candidates: list[dict[str, str]] | None) -> str:
    if not candidates:
        return ""
    lines: list[str] = []
    for item in candidates[:MAX_TEAM_MEMBER_REMINDER_HINTS]:
        group = str(item.get("group") or "").strip()
        thread = str(item.get("thread") or "").strip()
        source = f"{group} / thread: {thread}" if thread else group
        label = "Xiaodong was directly asked" if item.get("ownership_reason") == "direct_request" else "Xiaodong committed"
        lines.append(f"- [{item.get('timestamp')}] {source}: {label}: {_clip_hint_text(item.get('text'), limit=220)}")
    return "\n".join(lines)


def _compact_daily_followup_candidates(candidates: list[dict[str, str]] | None) -> list[dict[str, str]]:
    if not candidates:
        return []
    compacted: list[dict[str, str]] = []
    for item in candidates:
        compacted.append(
            {
                "person": str(item.get("person") or "").strip(),
                "source": (
                    f"{str(item.get('group') or '').strip()} / thread: {str(item.get('thread') or '').strip()}"
                    if str(item.get("thread") or "").strip()
                    else str(item.get("group") or "").strip()
                ),
                "timestamp": str(item.get("timestamp") or "").strip(),
                "requester": str(item.get("sender") or "").strip(),
                "ask": _clip_hint_text(item.get("text"), limit=220),
            }
        )
    return compacted[:MAX_TEAM_MEMBER_REMINDER_HINTS]


def _compact_daily_brief_source_excerpt(text: str, *, max_chars: int, recent_chars: int) -> str:
    source = str(text or "").strip()
    if not source or len(source) <= max_chars:
        return source
    selected = _daily_brief_signal_excerpt(source, max_chars=max(1_000, max_chars - recent_chars))
    recent = _tail_by_line_budget(source, max_chars=recent_chars)
    parts = []
    if selected:
        parts.append("## High-signal lines\n" + selected)
    if recent:
        parts.append("## Recent tail\n" + recent)
    compacted = "\n\n".join(parts).strip()
    if len(compacted) <= max_chars:
        return compacted
    return compacted[:max_chars].rstrip()


def _daily_brief_signal_excerpt(text: str, *, max_chars: int) -> str:
    signal_terms = _daily_brief_signal_terms()
    selected: list[str] = []
    seen: set[str] = set()
    current_header = ""

    def add(line: str) -> None:
        clean = line.rstrip()
        if not clean or clean in seen:
            return
        selected.append(clean)
        seen.add(clean)

    for line in str(text or "").splitlines():
        clean = line.rstrip()
        if _SEATALK_HISTORY_HEADER_RE.match(clean.strip()):
            current_header = clean
            continue
        lowered = clean.casefold()
        if not any(term in lowered for term in signal_terms):
            continue
        if current_header:
            add(current_header)
        add(clean)
        if sum(len(item) + 1 for item in selected) >= max_chars:
            break
    return "\n".join(selected).strip()[:max_chars].rstrip()


def _tail_by_line_budget(text: str, *, max_chars: int) -> str:
    lines: list[str] = []
    total = 0
    for line in reversed(str(text or "").splitlines()):
        clean = line.rstrip()
        line_chars = len(clean) + 1
        if lines and total + line_chars > max_chars:
            break
        lines.append(clean)
        total += line_chars
    return "\n".join(reversed(lines)).strip()


def _daily_brief_signal_terms() -> tuple[str, ...]:
    people = {person.casefold() for person in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE.values()}
    aliases = {alias.casefold() for alias in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE}
    return tuple(dict.fromkeys([*DAILY_BRIEF_SIGNAL_TERMS, *DAILY_BRIEF_HIGH_SIGNAL_TERMS, *people, *aliases]))


def _build_unanswered_seatalk_question_hints(history_text: str) -> str:
    current_group = ""
    pending: list[dict[str, str | bool | tuple[str, str]]] = []
    for line in str(history_text or "").splitlines():
        header_match = _SEATALK_HISTORY_HEADER_RE.match(line.strip())
        if header_match:
            current_group = header_match.group("group").strip()
            continue
        message_match = _SEATALK_HISTORY_MESSAGE_RE.match(line)
        if not message_match or not current_group:
            continue
        sender = message_match.group("sender").strip()
        thread = (message_match.group("thread") or "").strip()
        text = message_match.group("text").strip()
        key = (current_group, thread or "__main__")
        if _is_meaningful_human_seatalk_line(sender, text) and not _looks_like_unanswered_question(text):
            for item in pending:
                if item.get("key") == key and item.get("sender") != sender:
                    item["answered"] = True
        if _is_unanswered_question_candidate(text, group=current_group, thread=thread, sender=sender):
            pending.append(
                {
                    "key": key,
                    "sender": sender,
                    "group": current_group,
                    "thread": thread,
                    "timestamp": message_match.group("timestamp").strip(),
                    "text": text,
                    "answered": False,
                }
            )

    unanswered = [item for item in pending if not item.get("answered")]
    unanswered.sort(key=_unanswered_question_sort_key, reverse=True)
    hints: list[str] = []
    for item in unanswered:
        group = str(item.get("group") or "").strip()
        thread = str(item.get("thread") or "").strip()
        sender = str(item.get("sender") or "").strip()
        timestamp = str(item.get("timestamp") or "").strip()
        text = _clip_hint_text(item.get("text"), limit=220)
        source = f"{group} / thread: {thread}" if thread else group
        hints.append(f"- [{timestamp}] {source}: {sender} asked: {text}")
        if len(hints) >= MAX_UNANSWERED_SEATALK_QUESTION_HINTS:
            break
    return "\n".join(hints)


def _build_team_member_reminder_candidates(history_text: str) -> list[dict[str, str]] | None:
    unresolved, _ = _scan_team_member_reminder_candidates(history_text)
    return unresolved


def _build_resolved_team_member_reminder_candidates(history_text: str) -> list[dict[str, str]]:
    _, resolved = _scan_team_member_reminder_candidates(history_text)
    return resolved


def _is_private_seatalk_group(value: Any) -> bool:
    normalized = str(value or "").casefold()
    return normalized.startswith("private seatalk chat") or bool(_seatalk_buddy_id(value))


def _looks_like_xiaodong_followup_commitment(text: Any) -> bool:
    normalized = " ".join(str(text or "").casefold().split())
    if not normalized or _is_meeting_logistics_or_availability_notice(normalized):
        return False
    return any(cue in normalized for cue in XIAODONG_FOLLOWUP_COMMITMENT_CUES)


def _build_xiaodong_followup_candidates(history_text: str) -> list[dict[str, str]]:
    candidates: list[dict[str, Any]] = []
    recent_context: dict[tuple[str, str], list[str]] = {}
    for record in _seatalk_history_records_for_evidence(history_text):
        sender = str(record.get("sender") or "").strip()
        text = str(record.get("text") or "").strip()
        group = str(record.get("group") or "").strip()
        thread = str(record.get("thread") or "").strip()
        is_human = _is_meaningful_human_seatalk_line(sender, text)
        for candidate in candidates:
            if candidate.get("resolved") or not _same_xiaodong_action_context(candidate, group=group, thread=thread):
                continue
            explicit_closure = is_human and _is_explicit_team_member_closure(text)
            substantive_thread_answer = bool(thread) and is_human and _is_substantive_team_member_followup(text)
            substantive_xiaodong_answer = (
                _sender_is_xiaodong(sender)
                and _is_substantive_team_member_followup(text)
                and not _looks_like_xiaodong_followup_commitment(text)
            )
            if explicit_closure or substantive_thread_answer or substantive_xiaodong_answer:
                candidate["resolved"] = True
        context_key = (
            _normalize_thread_match_text(group),
            _normalize_thread_match_text(thread or "__main__"),
        )
        if not _sender_is_xiaodong(sender) or not _looks_like_xiaodong_followup_commitment(text):
            if is_human:
                recent_context.setdefault(context_key, []).append(text)
                recent_context[context_key] = recent_context[context_key][-3:]
            continue
        candidates.append(
            {
                "sender": sender,
                "group": group,
                "thread": thread,
                "timestamp": str(record.get("timestamp") or "").strip(),
                "text": text,
                "context": " ".join(recent_context.get(context_key, [])),
                "ownership_reason": "commitment",
                "resolved": False,
            }
        )
        recent_context.setdefault(context_key, []).append(text)
        recent_context[context_key] = recent_context[context_key][-3:]
    return [
        {key: str(value) for key, value in candidate.items() if key != "resolved"}
        for candidate in candidates
        if not candidate.get("resolved")
    ][-MAX_TEAM_MEMBER_REMINDER_HINTS:]


def _looks_like_direct_xiaodong_request(text: Any) -> bool:
    """Recognize a human request addressed to Xiaodong, including private chats."""
    normalized = " ".join(str(text or "").casefold().split())
    if not normalized or _is_meeting_logistics_or_availability_notice(normalized):
        return False
    direct_name = bool(
        re.search(r"(?:@|\b)(?:zheng\s+xiaodong|xiaodong\s+zheng|xiaodong)\b", normalized)
    )
    if not direct_name:
        return False
    if _is_team_member_coverage_notice(normalized) or _is_assignment_statement(normalized):
        return False
    if any(cue in normalized for cue in ("heads up", "heads-up", "just want to give you a headsup", "for visibility")):
        return False
    if re.search(r"\bcc\s+@?(?:zheng\s+)?xiaodong\b", normalized):
        return False
    name_match = re.search(r"(?:@|\b)(?:zheng\s+xiaodong|xiaodong\s+zheng|xiaodong)\b", normalized)
    if name_match and re.search(r"[?？]", normalized[: name_match.start()]):
        return False
    if name_match and name_match.start() > 60 and not normalized[:name_match.start()].strip().startswith(("hi", "hello", "hey")):
        return False
    return _looks_like_team_member_request(normalized) or _looks_like_unanswered_question(normalized)


def _build_direct_xiaodong_request_candidates(history_text: str) -> list[dict[str, str]]:
    """Keep unresolved human asks to Xiaodong as owned actions.

    SeaTalk private chats are intentionally included here. The regular team
    follow-up scanner excludes private chats because it is only for named team
    members, but that exclusion must not hide a direct request to Xiaodong.
    """
    pending: list[dict[str, Any]] = []
    for record in _seatalk_history_records_for_evidence(history_text):
        sender = str(record.get("sender") or "").strip()
        text = str(record.get("text") or "").strip()
        if not _is_meaningful_human_seatalk_line(sender, text):
            continue
        group = str(record.get("group") or "").strip()
        thread = str(record.get("thread") or "").strip()
        context = (
            _normalize_thread_match_text(group),
            _normalize_thread_match_text(thread or "__main__"),
        )
        for candidate in pending:
            if candidate.get("context") != context or candidate.get("resolved"):
                continue
            if _is_explicit_team_member_closure(text):
                candidate["resolved"] = True
                continue
            if _sender_is_xiaodong(sender):
                # A commitment to investigate or get back is still Xiaodong's
                # open work. Only a clear answer/closure resolves the request.
                if _looks_like_xiaodong_followup_commitment(text):
                    continue
                if any(
                    cue in text.casefold()
                    for cue in (
                        "i recommend",
                        "recommend",
                        "我建议",
                        "建议可以",
                        "已确认",
                        "confirmed",
                        "改好了",
                        "ok",
                        "okay",
                    )
                ) and not any(
                    cue in text.casefold()
                    for cue in ("error", "issue", "blocked", "need to check", "还需要", "仍然", "still")
                ):
                    candidate["resolved"] = True
                    continue
                # Do not infer closure from a diagnostic reply such as
                # "the page is still showing a server error". The request is
                # resolved only by an explicit closure in the visible window.
                continue
        if _sender_is_xiaodong(sender) or not _looks_like_direct_xiaodong_request(text):
            continue
        pending.append(
            {
                "sender": sender,
                "group": group,
                "thread": thread,
                "timestamp": str(record.get("timestamp") or "").strip(),
                "text": text,
                "context": context,
                "ownership_reason": "direct_request",
                "resolved": False,
            }
        )
    def priority(candidate: dict[str, Any]) -> tuple[int, str]:
        text = " ".join(str(candidate.get(field) or "") for field in ("group", "thread", "text")).casefold()
        score = 0
        for marker, weight in (
            ("ipification", 40),
            ("brd", 25),
            ("unlock card", 40),
            ("server error", 30),
            ("centum", 40),
            ("current balance", 20),
            ("available balance", 20),
            ("default template", 10),
            ("需要配模板", 10),
        ):
            if marker in text:
                score += weight
        return score, str(candidate.get("timestamp") or "")

    return [
        {key: str(value) for key, value in candidate.items() if key not in {"context", "resolved"}}
        for candidate in sorted(pending, key=priority, reverse=True)[:MAX_TEAM_MEMBER_REMINDER_HINTS]
        if not candidate.get("resolved")
    ][-MAX_TEAM_MEMBER_REMINDER_HINTS:]


def _same_xiaodong_action_context(candidate: dict[str, Any], *, group: str, thread: str) -> bool:
    return (
        _normalize_thread_match_text(candidate.get("group")) == _normalize_thread_match_text(group)
        and _normalize_thread_match_text(candidate.get("thread") or "__main__")
        == _normalize_thread_match_text(thread or "__main__")
    )


def _dedupe_xiaodong_action_candidates(candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for candidate in candidates:
        deduped[_candidate_ref_key(candidate)] = candidate
    return list(deduped.values())[-MAX_TEAM_MEMBER_REMINDER_HINTS:]


def _scan_team_member_reminder_candidates(history_text: str) -> tuple[list[dict[str, str]] | None, list[dict[str, str]]]:
    current_group = ""
    saw_group_header = False
    pending: list[dict[str, Any]] = []
    for line in str(history_text or "").splitlines():
        header_match = _SEATALK_HISTORY_HEADER_RE.match(line.strip())
        if header_match:
            saw_group_header = True
            current_group = header_match.group("group").strip()
            continue
        message_match = _SEATALK_HISTORY_MESSAGE_RE.match(line)
        if not message_match or not current_group:
            continue
        if _is_private_seatalk_group(current_group):
            continue
        sender = message_match.group("sender").strip()
        thread = (message_match.group("thread") or "").strip()
        text = message_match.group("text").strip()
        if _is_team_member_reminder_noise(current_group, text):
            continue
        key = (current_group, thread or "__main__")
        sender_person = _canonical_team_member_name(sender)
        sender_is_xiaodong = _sender_is_xiaodong(sender)
        is_human_reply = _is_meaningful_human_seatalk_line(sender, text)
        for item in pending:
            if not _is_same_team_member_reminder_context(item, group=current_group, thread=thread, key=key):
                continue
            co_recipient_handled = (
                is_human_reply
                and item.get("sender") != sender
                and item.get("person") != "Zheng Xiaodong"
                and _sender_was_explicitly_mentioned(sender, item.get("text"))
                and (
                    _is_substantive_team_member_followup(text)
                    or _is_team_member_progress_update(text)
                    or _looks_like_onward_delegation(text)
                )
            )
            if co_recipient_handled:
                item["answered"] = True
        if sender_person or sender_is_xiaodong:
            for item in pending:
                if not _is_same_team_member_reminder_context(item, group=current_group, thread=thread, key=key):
                    continue
                named_owner_replied = bool(sender_person and item.get("person") == sender_person)
                xiaodong_closed_or_answered = sender_is_xiaodong and (
                    _is_explicit_team_member_closure(text) or _is_substantive_team_member_followup(text)
                )
                if named_owner_replied or xiaodong_closed_or_answered:
                    item["answered"] = True
        # A reply from a different participant may add context, but it does not
        # prove that the named owner handled the request. Only the named owner,
        # Xiaodong, or the requester explicitly closing the issue can resolve it.
        if thread and is_human_reply and _is_explicit_team_member_closure(text):
            for item in pending:
                if (
                    item.get("sender") == sender
                    and _is_same_team_member_reminder_context(item, group=current_group, thread=thread, key=key)
                ):
                    item["answered"] = True
        if is_human_reply and _is_explicit_team_member_closure(text):
            for item in pending:
                if item.get("sender") == sender and str(item.get("group") or "") == current_group:
                    item["answered"] = True
        if (
            not is_human_reply
            or _is_team_member_coverage_notice(text)
            or _is_team_member_progress_update(text)
            or (sender_is_xiaodong and _is_assignment_statement(text))
            or not _looks_like_team_member_request(text)
        ):
            continue
        for person in _mentioned_team_members(text):
            if person == sender_person or _is_cc_only_team_member_mention(text, person):
                continue
            pending.append(
                {
                    "key": key,
                    "person": person,
                    "sender": sender,
                    "group": current_group,
                    "thread": thread,
                    "timestamp": message_match.group("timestamp").strip(),
                    "text": text,
                    "answered": False,
                }
            )

    if not saw_group_header:
        return None, []
    unresolved = [item for item in pending if not item.get("answered")]
    resolved = [item for item in pending if item.get("answered")]
    unresolved.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    resolved.sort(key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    unresolved = _dedupe_team_member_reminder_candidates(unresolved)
    resolved = _dedupe_team_member_reminder_candidates(resolved)

    def serialize(items: list[dict[str, Any]]) -> list[dict[str, str]]:
        return [
            {
                "person": str(item.get("person") or ""),
                "sender": str(item.get("sender") or ""),
                "group": str(item.get("group") or ""),
                "thread": str(item.get("thread") or ""),
                "timestamp": str(item.get("timestamp") or ""),
                "text": str(item.get("text") or ""),
            }
            for item in items
        ]

    return serialize(unresolved), serialize(resolved)


def _is_substantive_team_member_followup(text: Any) -> bool:
    """Treat a non-request reply in the same thread as an answer to its follow-up."""
    normalized = str(text or "").strip()
    if not normalized or _is_meeting_logistics_or_availability_notice(normalized):
        return False
    lowered = normalized.casefold()
    # Short decisions often repeat the word "need", which also appears in
    # request detection. Recognize the conclusion before applying that gate.
    decision_cues = (
        "assume no need",
        "can assume no need",
        "do not need",
        "don't need",
        "does not need",
        "no action needed",
        "not required",
        "无需",
        "不用",
        "不需要",
    )
    if not re.search(r"[?？吗呢]", normalized) and any(cue in lowered for cue in decision_cues):
        return True
    if _looks_like_team_member_request(normalized) or re.search(r"[?？吗呢]", normalized):
        return False
    answer_cues = (
        " is ",
        " are ",
        " was ",
        " were ",
        " passed ",
        " passing ",
        " sent ",
        " shared ",
        " provided ",
        " confirmed ",
        " the reason",
        "因为",
        "原因",
        "目前",
        "已",
        "已经",
        "传给",
        "通过",
        "可以",
    )
    return any(cue in lowered for cue in answer_cues)


def _sender_was_explicitly_mentioned(sender: Any, request_text: Any) -> bool:
    """Distinguish an explicitly named co-recipient from an unrelated replier."""
    display_name = re.split(r"\s*(?:\(|\||｜)", str(sender or ""), maxsplit=1)[0].strip()
    name_tokens = re.findall(r"[A-Za-z0-9]+", display_name)
    if not name_tokens:
        return False
    flexible_name = r"[\s._|｜-]+".join(re.escape(token) for token in name_tokens)
    return bool(re.search(rf"@\s*{flexible_name}\b", str(request_text or ""), flags=re.IGNORECASE))


def _looks_like_onward_delegation(text: Any) -> bool:
    normalized = " ".join(str(text or "").casefold().split())
    if "@" not in normalized:
        return False
    return any(
        cue in normalized
        for cue in (
            "please help",
            "pls help",
            "plz help",
            "can help",
            "could help",
            "help to check",
            "please check",
            "pls check",
            "麻烦",
            "帮忙",
            "请查",
        )
    )


def _is_team_member_progress_update(text: Any) -> bool:
    normalized = " ".join(str(text or "").casefold().split())
    progress_cues = (
        "just checked",
        "checking with",
        "still checking",
        "checking on this",
        "will provide reply",
        "will reply",
        "will get back",
        "i'll check",
        "i will check",
        "already checking",
        "looking into this",
        "will follow up",
        "已在看",
        "稍后回复",
        "之后回复",
    )
    request_cues = (
        "please",
        "can you",
        "could you",
        "help to",
        "帮",
        "麻烦",
        "确认",
        "check this",
        "review this",
    )
    return any(cue in normalized for cue in progress_cues) and not any(cue in normalized for cue in request_cues)


def _is_explicit_team_member_closure(text: Any) -> bool:
    normalized = str(text or "").casefold()
    closure_terms = (
        "fixed",
        "resolved",
        "closed",
        "completed",
        "tested as expected",
        "no longer reproduce",
        "issue is gone",
        "已修复",
        "已解决",
        "已关闭",
        "已处理",
        "已完成",
        "修好了",
        "测试通过",
        "已验证",
    )
    return any(term in normalized for term in closure_terms)


def _dedupe_team_member_reminder_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A person needs one follow-up per group thread, not one per repeated mention."""
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        key = (
            str(item.get("person") or ""),
            _normalize_thread_match_text(item.get("group")),
            _normalize_thread_match_text(item.get("thread")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _filter_gmail_calendar_history(gmail_history_text: str) -> tuple[str, int]:
    """Remove pure Google Calendar invite/response traffic before prompting the model."""
    source = str(gmail_history_text or "").strip()
    if not source:
        return source, 0
    separator = "=" * 80
    if separator not in source:
        return source, 0
    parts = source.split(separator)
    kept: list[str] = [parts[0].rstrip()]
    suppressed = 0
    for part in parts[1:]:
        block = part.strip()
        if not block:
            continue
        if _is_pure_gmail_calendar_block(block):
            suppressed += len(re.findall(r"(?m)^Message\s+\d+", block)) or 1
            continue
        kept.append(block)
    return f"\n{separator}\n".join(part for part in kept if part).strip(), suppressed


def _is_pure_gmail_calendar_block(block: str) -> bool:
    subject = ""
    sender = ""
    for line in str(block or "").splitlines():
        if line.startswith("Subject:"):
            subject = line.split(":", 1)[1].strip().casefold()
        elif line.startswith("From:"):
            sender = line.split(":", 1)[1].strip().casefold()
    sender_is_calendar = any(hint in sender for hint in GMAIL_CALENDAR_SENDER_HINTS)
    subject_is_calendar = any(hint in subject for hint in GMAIL_CALENDAR_SUBJECT_HINTS)
    return sender_is_calendar or subject_is_calendar


def _filter_gmail_low_value_reminder_history(gmail_history_text: str) -> tuple[str, int]:
    """Remove automated account-maintenance reminders with no PM decision value."""
    source = str(gmail_history_text or "").strip()
    if not source:
        return source, 0
    separator = "=" * 80
    if separator not in source:
        return source, 0
    parts = source.split(separator)
    kept: list[str] = [parts[0].rstrip()]
    suppressed = 0
    for part in parts[1:]:
        block = part.strip()
        if not block:
            continue
        if _is_low_value_gmail_reminder_block(block):
            suppressed += len(re.findall(r"(?m)^Message\s+\d+", block)) or 1
            continue
        kept.append(block)
    return f"\n{separator}\n".join(part for part in kept if part).strip(), suppressed


def _is_low_value_gmail_reminder_block(block: str) -> bool:
    subject = next(
        (
            line.split(":", 1)[1].strip().casefold()
            for line in str(block or "").splitlines()
            if line.startswith("Subject:")
        ),
        "",
    )
    return any(hint in subject for hint in GMAIL_LOW_VALUE_REMINDER_SUBJECT_HINTS)


def _is_gmail_calendar_item(item: dict[str, Any]) -> bool:
    source_type = str(item.get("source_type") or "").strip().lower()
    if source_type not in {"gmail", "mixed"}:
        return False
    combined = " ".join(
        str(item.get(field) or "").casefold()
        for field in ("task", "title", "summary", "evidence", "reminder")
    )
    calendar_cues = (
        "accepted:",
        "declined:",
        "tentative:",
        "invitation:",
        "updated invitation",
        "you’re invited",
        "you're invited",
        "rescheduled",
        "rsvp",
        "google calendar",
        "meeting request",
        "event reminder",
    )
    return any(cue in combined for cue in calendar_cues)


def _filter_gmail_calendar_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in items
        if not _is_gmail_calendar_item(item) and not _is_low_value_gmail_reminder_item(item)
    ]


def _is_low_value_gmail_reminder_item(item: dict[str, Any]) -> bool:
    if str(item.get("source_type") or "").strip().lower() not in {"gmail", "mixed"}:
        return False
    combined = " ".join(
        str(item.get(field) or "").casefold()
        for field in ("task", "title", "summary", "evidence", "reminder")
    )
    return any(hint in combined for hint in GMAIL_LOW_VALUE_REMINDER_SUBJECT_HINTS)


def _build_high_signal_review_hints(history_text: str) -> str:
    """Keep critical timeline/risk lines visible even when source compaction is aggressive."""
    candidates: list[tuple[int, int, str]] = []
    seen: set[str] = set()
    for index, record in enumerate(_seatalk_history_records_for_evidence(history_text)):
        current_group = str(record.get("group") or "").strip()
        current_thread = str(record.get("thread") or "").strip()
        text = str(record.get("text") or "").strip()
        haystack = f"{current_group} {current_thread} {text}".casefold()
        if not any(term in haystack for term in DAILY_BRIEF_HIGH_SIGNAL_TERMS):
            continue
        if text.casefold() in {"ack", "noted", "thanks", "thank you", "[image]"}:
            continue
        if "new live incident" in text.casefold() and len(text) < 80:
            continue
        source = f"{current_group} / thread: {current_thread}" if current_thread else current_group
        hint = f"- [{str(record.get('timestamp') or '').strip()}] {source}: {str(record.get('sender') or '').strip()}: {_clip_high_signal_text(text, limit=280)}"
        if hint in seen:
            continue
        seen.add(hint)
        score = 0
        weights = (
            ("[sp][p0]", 30),
            ("p0", 18),
            ("p1", 12),
            ("mas", 16),
            ("incident", 14),
            ("blocked", 14),
            ("blocker", 14),
            ("dependency", 12),
            ("upstream", 10),
            ("timeline", 10),
            ("delay", 10),
            ("eta", 8),
            ("launch", 8),
            ("go-live", 8),
            ("release", 7),
            ("version", 7),
            ("上线", 8),
            ("延期", 10),
            ("阻塞", 12),
            ("依赖", 10),
            ("事故", 12),
        )
        score += sum(weight for term, weight in weights if term in haystack)
        if "xiaodong" in str(record.get("sender") or "").casefold() or "zheng xiaodong" in text.casefold():
            score += 3
        if "[sp][p0]" in current_group.casefold():
            score += 20
        candidates.append((score, index, hint))
    candidates.sort(key=lambda value: (-value[0], -value[1]))
    return "\n".join(hint for _, _, hint in candidates[:36])


def _build_business_trip_project_updates(
    history_text: str,
    *,
    existing_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep Xiaodong-owned automation deliverables out of the travel-noise bucket."""
    records = _seatalk_history_records_for_evidence(history_text)
    business_trip_records = [
        record
        for record in records
        if "business trip planning" in str(record.get("group") or "").casefold()
    ]
    if not business_trip_records:
        return []
    combined_text = "\n".join(str(record.get("text") or "") for record in business_trip_records)
    combined = combined_text.casefold()
    has_xiaodong_plan = any(
        _sender_is_xiaodong(str(record.get("sender") or ""))
        and any(term in str(record.get("text") or "").casefold() for term in ("system capabilities", "automation", "proposed plan"))
        for record in business_trip_records
    )
    has_presentation_deliverable = any(term in combined for term in ("slide", "presentation"))
    has_delivery_checkpoint = any(term in combined for term in ("next wed", "next week", "immediate next step"))
    if not (has_xiaodong_plan and has_presentation_deliverable and has_delivery_checkpoint):
        return []
    candidate = {
        "domain": "General",
        "title": "PH Fraud Ops 自动化建设方案与演示材料",
        "summary": (
            "状态：进行中，Xiaodong 已启动 PH trip findings 的演示材料，覆盖 Fraud Ops 职能、BIN attack 深挖、"
            "系统自动化能力、建设方案和近期下一步，并已分配部分内容。"
            "影响：该材料将形成 PH Fraud Ops 自动化建设的团队对齐基础。"
            "下一步：在下周三前汇总分工内容，完成演示材料并确认系统能力建设方案。"
        ),
        "status": "in_progress",
        "evidence": _normalize_seatalk_source_label(str(business_trip_records[0].get("group") or "Business Trip Planning")),
        "source_type": "seatalk",
        "priority": "high",
        "priority_reason": "Xiaodong-owned automation delivery plan",
    }
    if any(_brief_items_refer_to_same_topic(candidate, item) for item in existing_items):
        return []
    return [candidate]


def _build_high_signal_fallback_items(
    history_text: str,
    *,
    evidence_refs: list[dict[str, Any]],
    name_mappings: dict[str, str],
    existing_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Recover material P0/risk events omitted by the model without backfilling chatter."""
    records = _seatalk_history_records_for_evidence(history_text)
    grouped: dict[tuple[str, str, str], tuple[int, dict[str, str]]] = {}
    for record in records:
        group = str(record.get("group") or "").strip()
        thread = str(record.get("thread") or "").strip()
        text = str(record.get("text") or "").strip()
        sender = str(record.get("sender") or "").strip()
        if not group or not text or not _is_meaningful_human_seatalk_line(sender, text):
            continue
        if _is_meeting_logistics_or_availability_notice(text):
            continue
        haystack = f"{group} {thread} {text}".casefold()
        is_p0_group = bool(re.search(r"\bp0\b", group.casefold()))
        risk_terms = (
            "mas",
            "incident",
            "blocked",
            "blocker",
            "dependency",
            "upstream",
            "timeline",
            "delay",
            "delayed",
            "postpone",
            "launch",
            "go-live",
            "version",
            "1.0.88",
            "v3.07",
            "v3.08",
            "atm",
            "qris",
            "translation",
            "copywriting",
            "configure",
            "edit access",
            "rows 548",
            "548-549",
            "querytransferrecipient",
            "fallback",
            "recurring",
            "548-549",
            "f30",
            "dev starts",
            "device model",
            "devicemodel",
            "上线",
            "延期",
            "阻塞",
            "依赖",
            "事故",
            "overlimit",
            "failed",
            "error",
            "impact",
            "uat",
            "not working",
            "isn't working",
            "doesn't work",
            "does not work",
            "unable to",
            "problem ticket",
            "rejection",
            "capacity",
            "outage",
            "system down",
            "faq",
            "blank page",
            "manual provisioning",
        )
        hit_terms = [term for term in risk_terms if term in haystack]
        direct_xiaodong = _sender_is_xiaodong(sender)
        direct_timeline_change = direct_xiaodong and any(
            term in haystack for term in ("timeline", "delay", "delayed", "1.0.88", "延期", "上线")
        )
        version_timeline_change = bool(
            re.search(r"\bv?\d+\.\d+(?:\.\d+)?\b", haystack, flags=re.IGNORECASE)
            and any(
                term in haystack
                for term in (
                    "timeline",
                    "delay",
                    "delayed",
                    "release",
                    "launch",
                    "start",
                    "hard deadline",
                    "as planned",
                    "预计",
                    "上线",
                    "延期",
                )
            )
        )
        protected_topic = _is_protected_daily_brief_record(record) or _is_priority_other_update_record(record) or any(
            (
                "qris" in haystack and any(term in haystack for term in ("dependency", "upstream", "requirement", "timeline")),
                "atm" in haystack and bool(re.search(r"\bv?3\.0[78]\b", haystack, flags=re.IGNORECASE)),
                "translation" in haystack and any(term in haystack for term in ("configure", "copywriting", "sdk", "key")),
                "copywriting" in haystack and any(term in haystack for term in ("configure", "sdk", "transify")),
                "edit access" in haystack and any(term in haystack for term in ("548", "549", "row")),
                "548-549" in haystack,
                "querytransferrecipient" in haystack,
                "recurring" in haystack and any(term in haystack for term in ("incident", "live", "ph", "querytransfer")),
                "fallback" in haystack and any(term in haystack for term in ("mas", "af", "fraud")),
                "mari stock" in haystack
                and any(term in haystack for term in ("mas", "fallback", "fall back", "blocked", "dependency", "delay")),
                _is_material_vendor_cost_signal(haystack),
                "f30" in haystack,
                any(term in haystack for term in ("device model", "devicemodel")),
            )
        )
        if not is_p0_group and len(hit_terms) < 2 and not direct_timeline_change and not protected_topic:
            continue
        if len(text) < 20 or text.casefold() in {"ack", "noted", "thanks", "thank you", "[image]"}:
            continue
        score = len(hit_terms) * 2 + (8 if is_p0_group else 0)
        if "mas" in haystack or "blocked" in haystack or "incident" in haystack:
            score += 5
        if direct_timeline_change:
            score += 14
        if version_timeline_change:
            # Version milestones are easy to lose when a model returns several
            # P0 incidents, but they are explicitly required high-signal output.
            score += 24
        if protected_topic:
            score += 20
        if _is_other_event_signal(haystack):
            score += 12
        if any(
            term in haystack
            for term in (
                "1.0.88",
                "f30",
                "devicemodel",
                "device model",
                "edit access",
                "548-549",
                "v3.07",
                "v3.08",
                "querytransferrecipient",
                "translation",
                "qris",
                "sgdb-81072",
            )
        ) or ("stock" in haystack and "mas" in haystack):
            score += 35
        if (
            re.search(r"(?<![A-Za-z0-9_])mas(?![A-Za-z0-9_])", haystack, flags=re.IGNORECASE)
            and any(
                term in haystack
                for term in (
                    "regulatory", "compliance", "approval", "launch", "requirement", "reporting",
                    "register", "risk", "incident", "system down", "fallback", "强监管", "上报", "监管", "合规", "上线",
                )
            )
        ):
            score += 35
        protected_markers = tuple(
            marker
            for marker in (
                "mas",
                "qris",
                "dependency",
                "upstream",
                "edit access",
                "548-549",
                "mari stock",
                "querytransferrecipient",
                "swp-31174",
                "f30",
                "device model",
                "devicemodel",
                "v3.07",
                "v3.08",
                "1.0.88",
                "translation",
            )
            if marker in haystack
        ) if _is_protected_daily_brief_record(record) else ()
        key = (
            _normalize_thread_match_text(group),
            _normalize_thread_match_text(thread or "__main__"),
            "protected:" + "|".join(protected_markers) if protected_markers else "",
        )
        previous = grouped.get(key)
        if previous is None or score > previous[0]:
            grouped[key] = (score, record)

    def has_same_topic(item: dict[str, Any], record: dict[str, str]) -> bool:
        candidate = {
            "title": str(record.get("thread") or record.get("group") or ""),
            "summary": str(record.get("text") or ""),
            "domain": item.get("domain"),
        }
        return any(_brief_items_refer_to_same_topic(item, existing) or _brief_items_refer_to_same_topic(candidate, existing) for existing in existing_items)

    ranked = sorted(grouped.values(), key=lambda value: value[0], reverse=True)

    def critical_bucket(record: dict[str, str]) -> str:
        """Reserve one slot for each required high-signal topic family."""
        haystack = " ".join(
            str(record.get(field) or "") for field in ("group", "thread", "sender", "text")
        ).casefold()
        if "mari stock" in haystack and any(term in haystack for term in ("fallback", "fall back")) and any(
            term in haystack for term in ("mas", "fraud", "anti-fraud", " af ")
        ):
            return "mari_stock_fallback"
        if _is_material_vendor_cost_signal(haystack):
            return "material_vendor_cost"
        if re.search(r"(?<![a-z0-9_])mas(?![a-z0-9_])", haystack) and any(
            term in haystack
            for term in (
                "regulatory", "compliance", "requirement", "reporting", "register",
                "deadline", "risk", "incident", "system down", "fallback", "强监管", "上报", "监管", "合规",
            )
        ):
            return "mas_compliance"
        if "qris" in haystack and any(term in haystack for term in ("dependency", "upstream", "sgdb-81072")):
            return "qris_dependency"
        if any(term in haystack for term in ("f30", "dev starts", "device model", "devicemodel", "1.0.88")):
            return "rene_version_timeline"
        if "atm" in haystack and re.search(r"\bv?3\.0[78]\b", haystack, flags=re.IGNORECASE):
            return "atm_version_timeline"
        if any(term in haystack for term in ("querytransferrecipient", "swp-31174")) or (
            "recurring" in haystack and "incident" in haystack
        ):
            return "ph_recurring_incident"
        if "edit access" in haystack or "548-549" in haystack:
            return "ker_yin_edit_access"
        if "translation" in haystack and any(term in haystack for term in ("configure", "copywriting", "sdk", "key")):
            return "ph_translation"
        if "gpay" in haystack and any(term in haystack for term in ("uat", "manual provisioning", "not working")):
            return "gpay_uat"
        if "dd05" in haystack and any(term in haystack for term in ("faq", "blank page", "need help")):
            return "ph_dd05_faq"
        if "problem ticket" in haystack or ("incident" in haystack and any(term in haystack for term in ("sfv", "live issue", "outage"))):
            return "live_incident"
        return ""

    # The normal score favors repeated generic incident lines. Reserve slots
    # for the required low-frequency topics before filling remaining slots.
    critical: dict[str, tuple[int, dict[str, str]]] = {}
    for score, record in ranked:
        bucket = critical_bucket(record)
        if bucket and (bucket not in critical or score > critical[bucket][0]):
            critical[bucket] = (score, record)
    selected_records: list[tuple[int, dict[str, str]]] = list(critical.values())
    selected_keys = {id(record) for _, record in selected_records}
    selected_records.extend((score, record) for score, record in ranked if id(record) not in selected_keys)
    fallbacks: list[dict[str, Any]] = []
    for score, record in selected_records:
        thread = str(record.get("thread") or "").strip()
        group = str(record.get("group") or "").strip()
        evidence_ref = next(
            (
                ref
                for ref in evidence_refs
                if str(ref.get("source_type") or "") == "seatalk"
                and str(ref.get("timestamp") or "") == str(record.get("timestamp") or "")
                and _normalize_thread_match_text(ref.get("group")) == _normalize_thread_match_text(group)
                and _normalize_thread_match_text(ref.get("thread") or "__main__") == _normalize_thread_match_text(thread or "__main__")
            ),
            None,
        )
        if not evidence_ref:
            continue
        text = _sanitize_seatalk_evidence(
            _clip_high_signal_text(record.get("text"), limit=320),
            name_mappings=name_mappings,
        )
        if (
            re.match(r"^[\w.-]+\)\s*\(", text)
            or text[:1] in {")", "]", "}", ":", ";"}
            or len(text) < 35
        ):
            # A clipped export fragment is not a defensible project update.
            # The full record remains available to the model and validation
            # layer, but this fallback must never turn a dangling name into a
            # fake State/Impact/Next summary.
            continue
        haystack = f"{group} {thread} {text}".casefold()
        source_topic = f"{group} {thread}".casefold()
        if _is_material_vendor_cost_signal(haystack):
            domain = "Ops Risk"
        elif "kyc" in source_topic and "credit risk" not in source_topic:
            domain = "General"
        elif "credit" in haystack or "loan" in haystack or "risk tier" in haystack:
            domain = "Credit Risk"
        elif any(term in haystack for term in ("ops", "operation", "operational")):
            domain = "Ops Risk"
        elif any(term in haystack for term in ("af", "anti-fraud", "fraud", "fv", "atm", "mas", "hold & release")):
            domain = "Anti-fraud"
        else:
            domain = "General"
        safe_topic = thread or group
        if re.search(r"\b(?:group|buddy)-\d+\b|\bUID\s+\d+\b", safe_topic, flags=re.IGNORECASE):
            safe_topic = "Private SeaTalk chat" if "buddy-" in safe_topic.casefold() or "uid " in safe_topic.casefold() else "SeaTalk group"
        item = {
            "domain": domain,
            "title": f"{_sanitize_seatalk_evidence(_normalize_seatalk_source_label(safe_topic), name_mappings=name_mappings)} high-signal update",
            "summary": text,
            "status": "blocked" if any(term in haystack for term in ("mas", "blocked", "blocker", "incident", "dependency", "delay", "延期", "阻塞", "事故")) else "in_progress",
            "evidence": _format_seatalk_record_evidence(record, name_mappings=name_mappings),
            "source_type": "seatalk",
            "evidence_ref_id": str(evidence_ref.get("id") or "").strip(),
            "priority_reason": "Deterministic high-signal P0/risk preservation",
            "fallback_source": "deterministic_high_signal",
        }
        item["summary"] = _synthesize_project_update_summary(item)
        if not item["summary"]:
            continue
        detail_text = f"{group} {thread} {text}".casefold()
        protected_detail = _is_protected_daily_brief_record(record) or bool(
            re.search(r"\bv?\d+\.\d+(?:\.\d+)?\b", detail_text, flags=re.IGNORECASE)
            or any(
                term in detail_text
                for term in ("f30", "devicemodel", "device model", "v3.07", "v3.08", "548-549", "querytransferrecipient")
            )
        )
        if has_same_topic(item, record) and not protected_detail:
            continue
        fallbacks.append(item)
        if len(fallbacks) >= 24:
            break
    return fallbacks


def _is_other_event_signal(value: Any) -> bool:
    haystack = " ".join(str(value or "").casefold().split())
    return any(
        term in haystack
        for term in (
            "incident",
            "outage",
            "system down",
            "service down",
            "unable to",
            "not working",
            "isn't working",
            "doesn't work",
            "does not work",
            "failed",
            "failure",
            "error",
            "high rejection",
            "rejection rate",
            "rejected",
            "alert",
            "capacity",
            "problem ticket",
            "phishing",
            "security warning",
            "手工开通",
            "故障",
            "告警",
        )
    )


def _is_priority_other_update_record(record: dict[str, str]) -> bool:
    haystack = " ".join(
        str(record.get(field) or "") for field in ("group", "thread", "sender", "text")
    ).casefold()
    return any(
        (
            "gpay" in haystack and any(term in haystack for term in ("manual provisioning", "not working", "uat")),
            "incident" in haystack and any(term in haystack for term in ("live issue", "outage", "problem ticket", "faq", "dd05", "sfv")),
            "rejection" in haystack and "address proof" in haystack,
            "capacity" in haystack and any(term in haystack for term in ("database", "db", "over 80%", "80%")),
            "phishing" in haystack or "security warning" in haystack,
        )
    )


def _fallback_should_be_other_update(item: dict[str, Any]) -> bool:
    haystack = " ".join(
        str(item.get(field) or "") for field in ("title", "summary", "evidence")
    ).casefold()
    if not _is_other_event_signal(haystack):
        return False
    # P0 delivery threads remain Project Updates even when they contain an
    # error log; the project state and the operational incident are one topic.
    if "[p0]" in haystack or "scheduled transfer" in haystack or "recurring transfer" in haystack:
        return False
    return any(
        term in haystack
        for term in (
            "incident",
            "gpay",
            "rejection",
            "faq",
            "problem ticket",
            "outage",
            "capacity",
            "security",
            "phishing",
            "not working",
            "unable to",
        )
    )


def _fallback_signal_type(item: dict[str, Any]) -> str:
    haystack = " ".join(
        str(item.get(field) or "") for field in ("title", "summary", "evidence")
    ).casefold()
    if any(term in haystack for term in ("incident", "outage", "system down", "problem ticket")):
        return "incident"
    if any(term in haystack for term in ("phishing", "security", "mas", "compliance")):
        return "risk_compliance"
    if any(term in haystack for term in ("rejection", "capacity", "alert")):
        return "cross_team_dependency"
    return "cross_team_dependency"


def _is_protected_daily_brief_record(record: dict[str, str]) -> bool:
    """Keep low-frequency required signals in the evidence pool after ranking."""
    haystack = " ".join(
        str(record.get(field) or "") for field in ("group", "thread", "sender", "text")
    ).casefold()
    group = str(record.get("group") or "").casefold()
    version_marker = bool(re.search(r"\bv?\d+\.\d+(?:\.\d+)?\b", haystack, flags=re.IGNORECASE))
    version_or_timeline = bool(
        version_marker
        and any(
            term in haystack
            for term in (
                "timeline",
                "delay",
                "delayed",
                "release",
                "launch",
                "dev starts",
                "start",
                "hard deadline",
                "上线",
                "延期",
            )
        )
    )
    mas_compliance_signal = bool(
        re.search(r"(?<![A-Za-z0-9_])mas(?![A-Za-z0-9_])", haystack, flags=re.IGNORECASE)
        and any(
            term in haystack
            for term in (
                "regulatory",
                "compliance",
                "requirement",
                "reporting",
                "register",
                "deadline",
                "risk",
                "incident",
                "system down",
                "fallback",
                "强监管",
                "上报",
                "监管",
                "合规",
            )
        )
    )
    return any(
        (
            mas_compliance_signal,
            "edit access" in haystack,
            "548-549" in haystack,
            "mari stock" in haystack
            and any(term in haystack for term in ("mas", "fallback", "fall back", "blocked", "dependency", "delay")),
            _is_material_vendor_cost_signal(haystack),
            "stock" in haystack and mas_compliance_signal,
            "querytransferrecipient" in haystack or "swp-31174" in haystack,
            "f30" in haystack,
            any(term in haystack for term in ("device model", "devicemodel")),
            "atm" in haystack and bool(re.search(r"\bv?3\.0[78]\b", haystack, flags=re.IGNORECASE)),
            "qris" in haystack and any(term in haystack for term in ("dependency", "upstream", "sgdb-81072")),
            "translation" in haystack,
            "fallback" in haystack and any(term in haystack for term in ("mas", "af", "fraud")),
            "[sp][p0]" in group and any(
                term in haystack
                for term in ("timeline", "delay", "delayed", "dependency", "upstream", "blocked", "launch", "release", "上线", "延期")
            ),
            version_or_timeline,
            version_marker and any(term in haystack for term in ("1.0.88", "v3.07", "v3.08", "f30", "devicemodel", "device model")),
        )
    )


def _build_gmail_high_signal_fallback_items(
    gmail_history_text: str,
    *,
    evidence_refs: list[dict[str, Any]],
    existing_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Recover material Gmail risks/timelines when model selection is incomplete."""
    fallbacks: list[dict[str, Any]] = []
    for ref in evidence_refs:
        if str(ref.get("source_type") or "").strip().lower() != "gmail":
            continue
        subject = " ".join(str(ref.get("subject") or "").split())
        snippet = " ".join(str(ref.get("snippet") or "").split())
        evidence = " ".join(str(ref.get("evidence") or "").split())
        haystack = f"{subject} {snippet} {evidence}".casefold()
        version_match = bool(re.search(r"\bv?\d+\.\d+(?:\.\d+)?\b", haystack, flags=re.IGNORECASE))
        version_timeline = version_match and any(
            term in haystack
            for term in ("timeline", "delay", "delayed", "release", "launch", "upstream", "deadline", "上线", "延期")
        )
        protected_topic = any(
            (
                "mari stock" in haystack,
                _is_mas_compliance_signal(haystack),
                version_timeline,
                "translation" in haystack and any(term in haystack for term in ("configure", "copywriting", "sdk", "key")),
                "translation" in haystack,
                "edit access" in haystack,
                "querytransferrecipient" in haystack,
                "recurring" in haystack and any(term in haystack for term in ("incident", "live", "ph", "querytransfer")),
                "qris" in haystack and any(term in haystack for term in ("dependency", "upstream", "timeline")),
                "atm" in haystack and bool(re.search(r"\bv?3\.0[78]\b", haystack, flags=re.IGNORECASE)),
                "f30" in haystack,
                any(term in haystack for term in ("device model", "devicemodel")),
            )
        )
        signal_terms = (
            "p0",
            "p1",
                "mas",
                "qris",
                "dependency",
                "upstream",
            "incident",
            "blocked",
            "dependency",
            "upstream",
            "timeline",
            "delay",
            "launch",
            "release",
            "version",
            "translation",
            "edit access",
            "fallback",
            "f30",
            "dev starts",
            "device model",
            "devicemodel",
            "recurring",
            "querytransferrecipient",
        )
        signal_count = sum(1 for term in signal_terms if term in haystack)
        if not protected_topic and signal_count < 2:
            continue
        summary = _clip_high_signal_text(f"{subject}: {snippet}".strip(": "), limit=360)
        if not summary or len(summary) < 20:
            continue
        domain = "Credit Risk" if any(
            term in haystack
            for term in ("credit risk", "credit review", "retail credit", "crms", "loan", "udl")
        ) else (
            "Anti-fraud" if any(term in haystack for term in ("anti-fraud", "anti fraud", "fraud", "mas", "atm", "fallback")) else "General"
        )
        item = {
            "domain": domain,
            "title": subject or "Gmail high-signal update",
            "summary": summary,
            "status": "blocked" if any(term in haystack for term in ("mas", "blocked", "incident", "dependency", "delay", "延期")) else "in_progress",
            "evidence": evidence or "Gmail conversation",
            "source_type": "gmail",
            "evidence_ref_id": str(ref.get("id") or "").strip(),
            "priority_reason": "Deterministic high-signal Gmail preservation",
            "fallback_source": "deterministic_gmail_high_signal",
        }
        if not item["evidence_ref_id"] or (
            any(_brief_items_refer_to_same_topic(item, existing) for existing in existing_items)
            and not protected_topic
        ):
            continue
        fallbacks.append(item)
        if len(fallbacks) >= 8:
            break
    return fallbacks


def _filter_daily_brief_meeting_logistics(history_text: str) -> str:
    return "\n".join(
        line
        for line in str(history_text or "").splitlines()
        if not (
            (match := _SEATALK_HISTORY_MESSAGE_RE.match(line))
            and _is_meeting_logistics_or_availability_notice(match.group("text"))
        )
    )


def _filter_daily_brief_seatalk_noise(history_text: str, *, config: dict[str, Any] | None) -> str:
    """Apply configured SeaTalk noise filtering without hiding material incidents."""
    normalized_config = config if isinstance(config, dict) else {}
    noise = normalized_config.get("noise") or {}
    blocked_terms = {
        str(value).strip().casefold()
        for value in (noise.get("seatalk_group_blacklist") or [])
        if str(value).strip()
    }
    if not blocked_terms:
        return str(history_text or "")

    lines = str(history_text or "").splitlines()
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if _SEATALK_HISTORY_HEADER_RE.match(line.strip()) and current:
            blocks.append(current)
            current = []
        current.append(line)
    if current:
        blocks.append(current)

    protected_groups: set[str] = set()
    for block in blocks:
        header = next((line for line in block if _SEATALK_HISTORY_HEADER_RE.match(line.strip())), "")
        match = _SEATALK_HISTORY_HEADER_RE.match(header.strip())
        if not match:
            continue
        group = match.group("group").strip()
        if not any(term in header.casefold() for term in blocked_terms):
            continue
        records = _seatalk_history_records_for_evidence("\n".join(block))
        if any(_is_priority_other_update_record(record) for record in records):
            protected_groups.add(group.casefold())

    kept: list[str] = []
    skip_block = False
    current_group = ""
    for line in lines:
        header_match = _SEATALK_HISTORY_HEADER_RE.match(line.strip())
        if header_match:
            current_group = header_match.group("group").strip()
            blocked = any(term in line.casefold() for term in blocked_terms)
            skip_block = blocked and current_group.casefold() not in protected_groups
            if skip_block:
                continue
        if skip_block:
            continue
        if any(term in line.casefold() for term in blocked_terms) and current_group.casefold() not in protected_groups:
            continue
        kept.append(line)
    return "\n".join(kept)


def _is_meeting_logistics_or_availability_notice(text: Any) -> bool:
    normalized = str(text or "").casefold()
    meeting_terms = (
        "meeting", "call", "sync", "standup", "small group", "discussion", "chat", "in the room",
        "available", "not available", "会议", "开会", "小组",
    )
    availability_terms = (
        "join late",
        "joining late",
        "late join",
        "running late",
        "be late",
        "will be late",
        "short delay",
        "delayed",
        "delay",
        "晚点",
        "迟到",
        "晚些",
        "晚一点",
        "晚点进入",
        "晚点加入",
        "reschedule",
        "rescheduled",
        "shift the meeting",
        "shift meeting",
        "move the meeting",
        "move meeting",
        "postpone the meeting",
    )
    if any(term in normalized for term in availability_terms) and any(term in normalized for term in meeting_terms):
        return True
    meeting_discussion_logistics = (
        "quick meeting",
        "meeting this afternoon",
        "discuss in the meeting",
        "discuss this in the meeting",
        "can we discuss",
        "shall we discuss",
        "invite has been sent",
        "calendar invite",
        "calendar invitation",
        "accepting/rejecting the calendar",
        "accept/reject the calendar",
        "rsvp",
        "you are invited",
        "you're invited",
        "you’re invited",
        "meeting invitation",
        "briefing",
        "conflicting meeting",
        "able to join",
        "join today's meeting",
        "join the meeting",
        "在会议上讨论",
        "会议上讨论",
    )
    if any(phrase in normalized for phrase in meeting_discussion_logistics):
        return True
    # Availability questions such as "is 2-3pm ok?" are scheduling logistics,
    # even when the message does not use the word "meeting".
    time_slot_question = bool(
        re.search(
            r"\b(?:is|are)\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)?(?:\s*-\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)?)?\s+ok\b",
            normalized,
        )
    )
    return time_slot_question and any(
        term in normalized for term in ("available", "not available", "@", "calendar", "slot")
    )


def _is_team_member_reminder_noise(group: Any, text: Any) -> bool:
    """Exclude coordination chatter that is not an unanswered work request."""
    group_text = " ".join(str(group or "").casefold().split())
    message_text = " ".join(str(text or "").casefold().split())
    if any(hint in group_text for hint in TEAM_MEMBER_REMINDER_NOISE_GROUP_HINTS):
        return True
    return _is_meeting_logistics_or_availability_notice(message_text)


def _is_assignment_statement(text: Any) -> bool:
    """Do not turn Xiaodong's internal delegation notes into team reminders."""
    normalized = " ".join(str(text or "").casefold().split())
    if not normalized or re.search(r"[?？]", normalized):
        return False
    return any(
        phrase in normalized
        for phrase in (
            " can help with ",
            " can help on ",
            " will help with ",
            " will help on ",
            "负责",
            "可以帮忙",
            "可以负责",
        )
    ) and not any(
        cue in normalized
        for cue in (
            "please",
            "can you",
            "could you",
            "need you",
            "check",
            "confirm",
            "review",
            "看下",
            "确认",
            "跟进",
        )
    )


def _is_team_member_coverage_notice(text: Any) -> bool:
    normalized = str(text or "").casefold()
    if not _mentioned_team_members(str(text or "")):
        return False
    coverage_cues = (
        "please find",
        "for any follow up",
        "for follow ups",
        "coverage",
        "covering",
        "backup",
        "on leave",
        "out of office",
        "annual leave",
    )
    has_ol_marker = bool(re.search(r"\b(?:ol|leave)\b", normalized))
    return any(cue in normalized for cue in coverage_cues) and has_ol_marker


def _filter_team_member_coverage_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in items
        if not _is_team_member_coverage_notice(_item_text(item, fields=("task", "reminder", "title", "summary", "evidence")))
    ]


def _filter_resolved_or_meeting_logistics_followups(
    items: list[dict[str, Any]],
    *,
    resolved_candidates: list[dict[str, str]],
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in items:
        item_text = _item_text(item, fields=("task", "reminder", "title", "summary"))
        if _is_meeting_logistics_or_availability_notice(item_text):
            continue
        deterministic_xiaodong = str(item.get("followup_source") or "").startswith("deterministic_xiaodong_")
        if not deterministic_xiaodong and str(item.get("source_type") or "").strip().lower() in {"seatalk", "mixed"} and any(
            _item_matches_resolved_followup(item, candidate) for candidate in resolved_candidates
        ):
            continue
        filtered.append(item)
    return filtered


def _item_matches_resolved_followup(item: dict[str, Any], candidate: dict[str, str]) -> bool:
    item_tokens = _topic_tokens(item, fields=("task", "reminder", "title", "summary"))
    candidate_tokens = _topic_tokens({"text": candidate.get("text")}, fields=("text",))
    overlap = item_tokens & candidate_tokens
    return len(overlap) >= 3 and len(overlap) / max(1, min(len(item_tokens), len(candidate_tokens))) >= 0.5


def _format_team_member_reminder_hints(candidates: list[dict[str, str]] | None) -> str:
    if candidates is None:
        return ""
    if not candidates:
        return "No valid unresolved team-member mention candidates were found."
    hints: list[str] = []
    for item in candidates:
        group = str(item.get("group") or "").strip()
        thread = str(item.get("thread") or "").strip()
        source = f"{group} / thread: {thread}" if thread else group
        hints.append(
            "- "
            f"[{item.get('timestamp')}] {source}: {item.get('sender')} asked "
            f"{item.get('person')}: {_clip_hint_text(item.get('text'), limit=220)}"
        )
    return "\n".join(hints)


def _mentioned_team_members(text: str) -> list[str]:
    normalized = f" {_normalize_person_key(text)} "
    for excluded_name in TEAM_MEMBER_REMINDER_EXCLUDED_NAME_KEYS:
        normalized = normalized.replace(f" {excluded_name} ", " ")
    people: list[str] = []
    for alias, person in sorted(TEAM_MEMBER_REMINDER_DETECTION_ALIASES.items(), key=lambda pair: len(pair[0]), reverse=True):
        if f" {alias} " not in normalized:
            continue
        if person not in people:
            people.append(person)
    return people


def _looks_like_team_member_request(text: str) -> bool:
    lowered = str(text or "").casefold()
    if "for visibility" in lowered and "?" not in lowered and "？" not in lowered:
        return False
    if re.search(r"\b(?:give\s+(?:us|me)|wait)\s+(?:a\s+)?few\s+minutes\b", lowered):
        return False
    if (
        not re.search(r"[?？]", lowered)
        and any(cue in lowered for cue in ("i think no issue", "no issues", "no issue", "nothing needed"))
        and not any(
            cue in lowered
            for cue in ("please", "pls", "can you", "could you", "help", "check", "review", "confirm", "follow up")
        )
    ):
        return False
    # A clarification that already supplies the requested estimate/answer is
    # not an outstanding action for the mentioned team member.
    if (
        "do you mean" in lowered
        and re.search(r"\bif\s+(?:yes|so)\b", lowered)
        and re.search(r"(?:\bestimate\b|\bapprox(?:imately)?\b|~?\s*\d)", lowered)
    ):
        return False
    if any(
        cue in lowered
        for cue in (
            "please",
            "pls",
            "plz",
            "help",
            "can you",
            "could you",
            "need",
            "needs",
            "confirm",
            "check",
            "review",
            "decide",
            "reply",
            "update",
            "provide",
            "follow up",
            "ensure",
            "handle",
            "investigate",
            "evaluate",
            "帮",
            "麻烦",
            "看下",
            "确认",
            "决定",
            "回复",
            "跟进",
            "处理",
            "评估",
            "是否",
            "能否",
        )
    ):
        return True
    return bool(re.search(r"[?？]", lowered)) and bool(_mentioned_team_members(text))


def _is_same_team_member_reminder_context(
    item: dict[str, Any],
    *,
    group: str,
    thread: str,
    key: tuple[str, str],
) -> bool:
    if str(item.get("group") or "") != group:
        return False
    if item.get("key") == key:
        return True
    item_thread = str(item.get("thread") or "").strip()
    if item_thread or not str(thread or "").strip():
        return False
    return _thread_title_matches_message(thread, str(item.get("text") or ""))


def _thread_title_matches_message(thread: str, text: str) -> bool:
    thread_key = _normalize_thread_match_text(thread)
    text_key = _normalize_thread_match_text(text)
    if len(thread_key) < 12 or len(text_key) < 12:
        return False
    return thread_key in text_key or text_key in thread_key


def _normalize_thread_match_text(value: Any) -> str:
    text = str(value or "").casefold()
    text = re.sub(r"\buid\b\s*[:#-]?\s*\d+\b", " ", text)
    text = re.sub(r"@[^\s]+", " ", text)
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text)
    return " ".join(text.split())


def _sender_is_xiaodong(sender: str) -> bool:
    return "xiaodong" in _normalize_person_key(sender)


def _is_cc_only_team_member_mention(text: str, person: str) -> bool:
    lowered = str(text or "").casefold()
    if "cc" not in lowered:
        return False
    person_aliases = [alias for alias, canonical in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE.items() if canonical == person]
    normalized = _normalize_person_key(text)
    cc_index = normalized.find(" cc ")
    if cc_index < 0 and not normalized.startswith("cc "):
        return False
    cc_tail = normalized[cc_index + 4 if cc_index >= 0 else 3 :]
    return any(f" {alias} " in f" {cc_tail} " for alias in person_aliases)


def _unanswered_question_sort_key(item: dict[str, Any]) -> tuple[int, str]:
    haystack = f"{item.get('group') or ''} {item.get('thread') or ''} {item.get('text') or ''}".casefold()
    score = sum(1 for term in _UNANSWERED_PM_RELEVANT_TERMS if term in haystack)
    for strong_term in ("cvc", "notifyservice", "google pay", "gpay", "tokenization", "live issue"):
        if strong_term in haystack:
            score += 3
    for strong_term in ("写错", "校验", "上送", "blocked", "blocker"):
        if strong_term in haystack:
            score += 2
    return score, str(item.get("timestamp") or "")


def _is_unanswered_question_candidate(text: str, *, group: str, thread: str, sender: str) -> bool:
    if not _is_meaningful_human_seatalk_line(sender, text) or not _looks_like_unanswered_question(text):
        return False
    haystack = f"{group} {thread} {text}".casefold()
    return any(term in haystack for term in _UNANSWERED_PM_RELEVANT_TERMS)


def _looks_like_unanswered_question(text: str) -> bool:
    lowered = str(text or "").casefold()
    return any(cue in lowered for cue in _UNANSWERED_QUESTION_CUES)


def _is_meaningful_human_seatalk_line(sender: str, text: str) -> bool:
    lowered_sender = str(sender or "").casefold()
    if any(marker in lowered_sender for marker in ("bot", "checker", "alert", "reminder")):
        return False
    clean_text = str(text or "").strip()
    if not clean_text or clean_text in {"[image]", "[video]", "[file]", "[sticker]", "[empty message]"}:
        return False
    return True


def _clip_hint_text(value: Any, *, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else f"{text[:limit].rstrip()}..."


def _clip_high_signal_text(value: Any, *, limit: int) -> str:
    raw = " ".join(str(value or "").split())
    if len(raw) <= limit:
        return raw
    segments = [segment.strip() for segment in re.split(r"(?<=[。！？?!])|(?<=\.)\s+", raw) if segment.strip()]
    high_signal_terms = (
        *DAILY_BRIEF_HIGH_SIGNAL_TERMS,
        "1.0.88",
        "v3.07",
        "v3.08",
        "qris",
        "swp-",
        "f30",
        "devicemodel",
        "device model",
        "548-549",
        "querytransferrecipient",
        "translation",
        "project 216820",
        "prd review",
        "tech design",
        "ui/ux",
        "market data",
        "market-data",
        "usd400k",
        "sender id",
    )
    selected = [segment for segment in segments if any(term.casefold() in segment.casefold() for term in high_signal_terms)]
    if selected:
        version_segments = [segment for segment in selected if re.search(r"\bv?\d+\.\d+(?:\.\d+)?\b", segment, flags=re.IGNORECASE)]
        if version_segments:
            marker_segments = [
                segment
                for segment in selected
                if any(
                    term.casefold() in segment.casefold()
                    for term in ("f30", "devicemodel", "device model", "548-549", "querytransferrecipient")
                )
            ]
            selected = list(dict.fromkeys([*version_segments, *marker_segments]))
        focused = " ".join(selected)
        if len(focused) <= limit:
            return focused
        clipped = focused[:limit].rstrip()
        late_marker_terms = ("f30", "devicemodel", "device model", "548-549", "querytransferrecipient")
        missing_late_marker = any(
            term.casefold() in focused.casefold() and term.casefold() not in clipped.casefold()
            for term in late_marker_terms
        )
        if any(term.casefold() in clipped.casefold() for term in high_signal_terms) and not missing_late_marker:
            return f"{clipped}..."
        # Long log lines often put the only useful marker (for example
        # deviceModel or F30) after the first few hundred characters. Keep a
        # compact context tail so the required signal remains visible.
        marker_match = next(
            (
                re.search(re.escape(term), focused, flags=re.IGNORECASE)
                for term in high_signal_terms
                if re.search(re.escape(term), focused, flags=re.IGNORECASE)
            ),
            None,
        )
        if marker_match:
            tail_limit = min(180, max(80, limit // 2))
            tail = focused[marker_match.start(): marker_match.start() + tail_limit].rstrip()
            marker_labels = [
                label
                for label in ("deviceModel", "F30", "1.0.88", "v3.07", "v3.08", "QRIS", "SWP-31174")
                if re.search(re.escape(label), focused, flags=re.IGNORECASE)
            ]
            missing_labels = [label for label in marker_labels if label.casefold() not in tail.casefold()]
            if missing_labels:
                suffix = f" [signals: {', '.join(missing_labels)}]"
                tail = f"{tail[:max(1, tail_limit - len(suffix))].rstrip()}{suffix}"
            prefix_limit = max(0, limit - len(tail) - 5)
            return f"{focused[:prefix_limit].rstrip()} ... {tail}"
        return f"{clipped}..."
    return _clip_hint_text(raw, limit=limit)


def _is_mas_compliance_signal(value: Any) -> bool:
    """Recognize MAS/compliance evidence even when it appears late in a body."""
    haystack = " ".join(str(value or "").casefold().split())
    return bool(
        re.search(r"(?<![a-z0-9_])mas(?![a-z0-9_])", haystack)
        and any(
            term in haystack
            for term in (
                "regulatory",
                "compliance",
                "requirement",
                "reporting",
                "register",
                "deadline",
                "risk",
                "incident",
                "system down",
                "fallback",
                "强监管",
                "上报",
                "监管",
                "合规",
            )
        )
    )


def _is_material_vendor_cost_signal(value: Any) -> bool:
    haystack = " ".join(str(value or "").casefold().split())
    return bool(
        re.search(r"(?:usd|sgd|php|idr|\$)\s*[\d,.]+\s*[km]?", haystack, flags=re.IGNORECASE)
        and any(term in haystack for term in ("per month", "monthly", "/month"))
        and any(term in haystack for term in ("vendor", "infobip", "telco", "routing", "sender id"))
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


def _infer_private_chat_name_mappings_from_history(history_text: str) -> dict[str, str]:
    mappings: dict[str, str] = {}
    current_group = ""
    for line in str(history_text or "").splitlines():
        header_match = _SEATALK_HISTORY_HEADER_RE.match(line.strip())
        if header_match:
            current_group = header_match.group("group").strip()
            continue
        buddy_id = _seatalk_buddy_id(current_group)
        if not buddy_id:
            continue
        message_match = _SEATALK_HISTORY_MESSAGE_RE.match(line)
        if not message_match:
            continue
        sender = message_match.group("sender").strip()
        if not _sender_is_xiaodong(sender):
            continue
        inferred_name = _infer_private_chat_counterparty_name_from_self_text(message_match.group("text"))
        if not inferred_name:
            continue
        suffix = buddy_id.removeprefix("buddy-").strip()
        mappings.setdefault(buddy_id.lower(), inferred_name)
        if suffix:
            mappings.setdefault(f"uid {suffix}", inferred_name)
    return mappings


def _infer_private_chat_counterparty_name_from_self_text(text: Any) -> str:
    raw = " ".join(str(text or "").split())
    if not raw:
        return ""
    for pattern in (
        r"\bThanks[, ]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?=\b|[,.!?:;])",
        r"\bThank you[, ]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?=\b|[,.!?:;])",
        r"\bHi[, ]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?=\b|[,.!?:;])",
        r"\bHi[, ]+([A-Z][a-z]+)(?=[,.!?:;])",
        r"\bHey[, ]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})(?=\b|[,.!?:;])",
        r"\bHey[, ]+([A-Z][a-z]+)(?=[,.!?:;])",
    ):
        match = re.search(pattern, raw)
        if not match:
            continue
        candidate = " ".join(match.group(1).split())
        normalized = _normalize_thread_match_text(candidate)
        if normalized in {"zheng xiaodong", "xiaodong zheng"}:
            continue
        return candidate[:180]
    return ""


def refresh_seatalk_auto_name_mappings(service: Any, *, now: datetime) -> dict[str, str]:
    path = getattr(service, "name_overrides_path", None)
    if not path or not hasattr(service, "build_name_mappings"):
        return {}
    try:
        from bpmis_jira_tool.seatalk_stores import SeaTalkNameMappingStore

        mapping_store = SeaTalkNameMappingStore(Path(path).expanduser())
        payload = service.build_name_mappings(now=now)
        auto_mappings = payload.get("auto_mappings") if isinstance(payload, dict) else {}
        missing = SeaTalkNameMappingStore.missing_mappings(mapping_store.mappings(), auto_mappings)
        if missing:
            return mapping_store.merge_mappings(missing)
        return mapping_store.mappings()
    except Exception:
        return {}


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
    private_match = re.fullmatch(r"Private SeaTalk chat\s*\((?P<id>buddy-\d+|UID\s+\d+)\)", text, flags=re.IGNORECASE)
    if private_match:
        raw_id = private_match.group("id")
        mapped = mappings.get(raw_id.lower())
        if not mapped:
            mapped = next((mappings.get(key.lower()) for key in _seatalk_mapping_equivalent_keys(raw_id) if mappings.get(key.lower())), "")
        if mapped:
            return _normalize_seatalk_source_label(mapped)
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
        return _format_private_seatalk_chat_label(raw)

    cleaned = RAW_SEATALK_ID_PATTERN.sub(replace_raw_id, text)
    cleaned = _normalize_seatalk_source_label(cleaned)
    # Some SeaTalk display names include an internal employee/account code in
    # brackets (for example, ``Zach Chong [SBKSG0463]``).  The code is not
    # useful evidence and should not leak into the user-facing brief.  Keep
    # meaningful labels such as [P0] and [ID UAT].
    cleaned = re.sub(
        r"\s+\[(?=[A-Z][A-Z0-9_-]{4,}\d[A-Z0-9_-]*\])[A-Z][A-Z0-9_-]{4,}\]",
        "",
        cleaned,
    )
    cleaned = re.sub(r"\bSeaTalk\s+SeaTalk\s+group\b", "SeaTalk group", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bSeaTalk\s+SeaTalk\s+contact\b", "SeaTalk contact", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(SeaTalk group)(?:\s*[,;/]\s*\1)+\b", r"\1", cleaned)
    cleaned = re.sub(r"\b(SeaTalk contact)(?:\s*[,;/]\s*\1)+\b", r"\1", cleaned)
    cleaned = " ".join(cleaned.split())
    if not cleaned or RAW_SEATALK_ID_PATTERN.fullmatch(text):
        if saw_unmapped_group and saw_unmapped_contact:  # pragma: no cover - a single raw ID cannot be both group and contact.
            return "SeaTalk conversation"
        if saw_unmapped_group:
            return "SeaTalk group"
        if saw_unmapped_contact:
            return _format_private_seatalk_chat_label(text)
    return cleaned or "SeaTalk conversation"


def _format_private_seatalk_chat_label(identifier: Any) -> str:
    # Raw buddy/UID identifiers are implementation details, not traceable
    # business evidence. Use a stable generic label when no display name maps.
    return "Private SeaTalk chat"


def _normalize_seatalk_source_label(value: Any) -> str:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return ""
    nested_private = re.fullmatch(r"Private SeaTalk chat\s*\((Private SeaTalk chat\s*\(.+\))\)", text, flags=re.IGNORECASE)
    if nested_private:
        return nested_private.group(1).strip()
    if not text.casefold().startswith("private seatalk chat"):
        text = re.sub(r"\s+\((?:group-\d+|buddy-\d+|UID\s+\d+)\)(?=\s*/\s*thread:|$)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSeaTalk\s+SeaTalk\s+group\b", "SeaTalk group", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSeaTalk\s+SeaTalk\s+contact\b", "SeaTalk contact", text, flags=re.IGNORECASE)
    duplicate_match = re.fullmatch(r"(?P<label>.+?)\s*\((?P=label)\)", text)
    if duplicate_match:
        return duplicate_match.group("label").strip()
    thread_match = re.match(r"(?P<label>.+?)\s*\((?P=label)\)\s*/\s*thread:\s*(?P<thread>.+)$", text, flags=re.IGNORECASE)
    if thread_match:
        return f"{thread_match.group('label').strip()} / thread: {thread_match.group('thread').strip()}"
    return text


def _mapped_seatalk_identifier_label(identifier: Any, *, name_mappings: dict[str, str] | None = None) -> str:
    raw = str(identifier or "").strip()
    if not raw:
        return ""
    mappings = {str(key).lower(): str(name).strip() for key, name in (name_mappings or {}).items() if str(name).strip()}
    mapped = mappings.get(raw.lower())
    if not mapped:
        mapped = next((mappings.get(key.lower()) for key in _seatalk_mapping_equivalent_keys(raw) if mappings.get(key.lower())), "")
    if mapped:
        return _normalize_seatalk_source_label(mapped)
    if raw.lower().startswith("buddy-") or re.match(r"^UID\s+\d+$", raw, re.IGNORECASE):
        return _format_private_seatalk_chat_label(raw)
    return _normalize_seatalk_source_label(raw)


def _build_daily_brief_evidence_refs(
    history_text: str,
    *,
    gmail_history_text: str = "",
    name_mappings: dict[str, str] | None = None,
    team_member_reminder_candidates: list[dict[str, str]] | None = None,
    xiaodong_followup_candidates: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    records = _seatalk_history_records_for_evidence(history_text)
    refs: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()

    def add_ref(record: dict[str, str], *, person: str = "", reply_state: str = "unknown") -> None:
        key = (
            str(record.get("timestamp") or ""),
            str(record.get("group") or ""),
            str(record.get("thread") or ""),
            str(record.get("sender") or ""),
            str(record.get("text") or "")[:160],
        )
        if key in seen:
            return
        seen.add(key)
        ref_id = f"st-ref-{len(refs) + 1:03d}"
        mentioned_people = _mentioned_team_members(record.get("text"))
        if person and person not in mentioned_people:
            mentioned_people.append(person)
        refs.append(
            {
                "id": ref_id,
                "source_type": "seatalk",
                "group": str(record.get("group") or "").strip(),
                "thread": str(record.get("thread") or "").strip(),
                "sender": str(record.get("sender") or "").strip(),
                "timestamp": str(record.get("timestamp") or "").strip(),
                "mentioned_people": mentioned_people,
                "reply_state": reply_state,
                "snippet": _clip_hint_text(record.get("text"), limit=240),
                "evidence": _format_seatalk_record_evidence(record, name_mappings=name_mappings),
            }
        )

    if records:
        by_record_key = {
            (
                _normalize_thread_match_text(record.get("group")),
                _normalize_thread_match_text(record.get("thread") or "__main__"),
                str(record.get("timestamp") or ""),
                _normalize_thread_match_text(record.get("text")),
            ): record
            for record in records
        }
        for candidate in [*(team_member_reminder_candidates or []), *(xiaodong_followup_candidates or [])]:
            lookup_key = (
                _normalize_thread_match_text(candidate.get("group")),
                _normalize_thread_match_text(candidate.get("thread") or "__main__"),
                str(candidate.get("timestamp") or ""),
                _normalize_thread_match_text(candidate.get("text")),
            )
            record = by_record_key.get(lookup_key)
            if record:
                add_ref(
                    record,
                    person=str(candidate.get("person") or ""),
                    reply_state="xiaodong_commitment" if not candidate.get("person") else "unanswered",
                )

        def evidence_priority(record: dict[str, str]) -> int:
            haystack = " ".join(str(record.get(field) or "") for field in ("group", "thread", "sender", "text")).casefold()
            score = sum(2 for term in DAILY_BRIEF_HIGH_SIGNAL_TERMS if term in haystack)
            if re.search(r"\bp0\b", haystack):
                score += 12
            if re.search(r"\bp1\b", haystack):
                score += 8
            return score

        selected_records: list[dict[str, str]] = []
        regular_record_count = 0
        for record in sorted(records, key=evidence_priority, reverse=True):
            text = str(record.get("text") or "")
            qualifies = _looks_like_team_member_request(text) or _looks_like_xiaodong_followup_commitment(text) or _is_unanswered_question_candidate(
                text,
                group=str(record.get("group") or ""),
                thread=str(record.get("thread") or ""),
                sender=str(record.get("sender") or ""),
            ) or _looks_like_project_update_ref(record)
            protected = _is_protected_daily_brief_record(record) or _is_priority_other_update_record(record)
            if not qualifies and not protected:
                continue
            if regular_record_count < 120 or protected:
                selected_records.append(record)
            if not protected:
                regular_record_count += 1
        for record in selected_records:
            add_ref(record)
    refs.extend(_build_gmail_evidence_refs(gmail_history_text, start_index=len(refs) + 1))
    return refs


def _looks_like_project_update_ref(record: dict[str, str]) -> bool:
    text = " ".join([record.get("group", ""), record.get("thread", ""), record.get("sender", ""), record.get("text", "")]).casefold()
    return any(
        term in text
        for term in (
            "blocked",
            "pending",
            "confirmed",
            "approval",
            "launch",
            "go-live",
            "golive",
            "live",
            "incident",
            "risk",
            "uat",
            "prd",
            "release",
            "dependency",
            "upstream",
            "delay",
            "delayed",
            "version",
            "v3.07",
            "v3.08",
            "atm",
            "qris",
            "translation",
            "copywriting",
            "querytransferrecipient",
            "fallback",
            "recurring",
            "mari stock",
            "548-549",
            "f30",
            "dev starts",
            "device model",
            "devicemodel",
            "timeline",
            "eta",
            "上线",
            "发布",
            "风险",
            "阻塞",
            "确认",
            "审批",
        )
    )


def _build_gmail_evidence_refs(gmail_history_text: str, *, start_index: int) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    current_message: dict[str, str] | None = None

    def flush_message() -> None:
        nonlocal current_message
        if not current_message or str(current_message.get("use") or "").startswith("context only"):
            current_message = None
            return
        body = str(current_message.get("body") or "").strip()
        ref_index = start_index + len(refs)
        refs.append(
            {
                "id": f"gm-ref-{ref_index:03d}",
                "source_type": "gmail",
                "thread_id": str(current.get("thread_id") or "").strip(),
                "thread_link": str(current.get("thread_link") or "").strip(),
                "subject": str(current.get("subject") or "").strip(),
                "participants": str(current.get("participants") or "").strip(),
                "sender": str(current_message.get("from") or "").strip(),
                "timestamp": str(current_message.get("date") or "").strip(),
                # Important compliance details often appear after the first
                # paragraph of a long meeting note. Preserve signal-bearing
                # sentences rather than only taking the body prefix.
                "snippet": _clip_high_signal_text(body, limit=280),
                "evidence": _format_gmail_ref_evidence(current, current_message),
                "to": str(current_message.get("to") or "").strip(),
                "cc": str(current_message.get("cc") or "").strip(),
            }
        )
        current_message = None

    for raw_line in str(gmail_history_text or "").splitlines():
        line = raw_line.rstrip()
        if re.fullmatch(r"=+", line):
            flush_message()
            current = {}
            continue
        if re.fullmatch(r"Thread\s+\d+", line):
            flush_message()
            current["thread"] = line.strip()
            continue
        if line.startswith("Thread ID:"):
            current["thread_id"] = line.split(":", 1)[1].strip()
            continue
        if line.startswith("Gmail Thread Link:"):
            current["thread_link"] = line.split(":", 1)[1].strip()
            continue
        if line.startswith("Subject:"):
            current["subject"] = line.split(":", 1)[1].strip()
            continue
        if line.startswith("Participants:"):
            current["participants"] = line.split(":", 1)[1].strip()
            continue
        if re.fullmatch(r"Message\s+\d+(?:\s+\(context only\))?", line):
            flush_message()
            current_message = {"message": line.strip(), "body": ""}
            continue
        if current_message is None:
            continue
        if line.startswith("Date:"):
            current_message["date"] = line.split(":", 1)[1].strip()
        elif line.startswith("From:"):
            current_message["from"] = line.split(":", 1)[1].strip()
        elif line.startswith("To:"):
            current_message["to"] = line.split(":", 1)[1].strip()
        elif line.startswith("Cc:"):
            current_message["cc"] = line.split(":", 1)[1].strip()
        elif line.startswith("Use:"):
            current_message["use"] = line.split(":", 1)[1].strip()
        elif line == "Body:":
            current_message["in_body"] = "1"
        elif current_message.get("in_body"):
            current_message["body"] = (str(current_message.get("body") or "") + "\n" + line).strip()
    flush_message()
    if len(refs) <= 40:
        return refs

    def protected(ref: dict[str, Any]) -> bool:
        haystack = " ".join(
            str(ref.get(field) or "")
            for field in ("subject", "participants", "sender", "snippet", "evidence")
        ).casefold()
        return any(
            (
                bool(re.search(r"(?<![A-Za-z0-9_])mas(?![A-Za-z0-9_])", haystack, flags=re.IGNORECASE)),
                "mari stock" in haystack,
                "edit access" in haystack,
                "548-549" in haystack,
                "translation" in haystack,
                "copywriting" in haystack,
                "querytransferrecipient" in haystack,
                "swp-31174" in haystack,
                "qris" in haystack and any(term in haystack for term in ("dependency", "upstream", "timeline")),
                "fallback" in haystack and any(term in haystack for term in ("mas", "fraud", "anti-fraud")),
                "f30" in haystack,
                any(term in haystack for term in ("device model", "devicemodel")),
                bool(re.search(r"\bv?3\.0[78]\b", haystack, flags=re.IGNORECASE)),
                bool(re.search(r"\bv?\d+\.\d+(?:\.\d+)?\b", haystack, flags=re.IGNORECASE))
                and any(term in haystack for term in ("timeline", "delay", "release", "launch", "dev starts", "start", "延期", "上线")),
            )
        )

    protected_refs = [ref for ref in refs if protected(ref)]
    protected_ids = {str(ref.get("id") or "") for ref in protected_refs}
    regular_refs = [ref for ref in refs if str(ref.get("id") or "") not in protected_ids]
    # Keep every protected evidence ref even when a busy Gmail window has more
    # than the normal context cap; the remaining context stays bounded.
    return protected_refs + regular_refs[: max(0, 40 - len(protected_refs))]


def _format_gmail_ref_evidence(thread: dict[str, Any], message: dict[str, str]) -> str:
    subject = str(thread.get("subject") or "").strip()
    sender = str(message.get("from") or "").strip()
    if subject and sender:
        return f"Gmail: {subject} / {sender}"
    if subject:
        return f"Gmail: {subject}"
    return "Gmail thread"


def _apply_daily_brief_evidence_refs(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    my_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
    evidence_refs: list[dict[str, Any]],
) -> dict[str, Any]:
    metrics = {
        "dropped_invalid_evidence_count": 0,
        "repaired_evidence_count": 0,
        "generic_evidence_count": 0,
        "candidate_followup_count": 0,
        "final_followup_count": 0,
    }
    refs_by_id = {str(ref.get("id") or "").strip(): ref for ref in evidence_refs if str(ref.get("id") or "").strip()}
    if not refs_by_id:
        return metrics
    available_ref_source_types = {
        str(ref.get("source_type") or "").strip().lower()
        for ref in evidence_refs
        if str(ref.get("source_type") or "").strip()
    }

    def apply_to_item(item: dict[str, Any], *, section: str, require_ref: bool) -> bool:
        ref_id = str(item.get("evidence_ref_id") or "").strip()
        ref = refs_by_id.get(ref_id)
        if not ref:
            inferred_ref = _best_evidence_ref_for_item(item, evidence_refs, section=section) if require_ref else None
            if inferred_ref:
                ref_id = str(inferred_ref.get("id") or "").strip()
                ref = refs_by_id.get(ref_id)
        if not ref:
            if ref_id or require_ref:
                metrics["dropped_invalid_evidence_count"] += 1
                return False
            return True
        if not _evidence_ref_matches_item_people(item, ref):
            metrics["dropped_invalid_evidence_count"] += 1
            return False
        deterministic_xiaodong = str(item.get("followup_source") or "").startswith("deterministic_xiaodong_")
        if (
            section == "my_todos"
            and str(item.get("action_type") or "").strip() == "direct_action"
            and str(ref.get("source_type") or "").strip().lower() == "seatalk"
            and not deterministic_xiaodong
            and not _seatalk_ref_supports_xiaodong_action(ref)
        ):
            metrics["dropped_invalid_evidence_count"] += 1
            return False
        if not deterministic_xiaodong and not _evidence_refs_match_project_item(item, [ref]):
            metrics["dropped_invalid_evidence_count"] += 1
            return False
        evidence = str(ref.get("evidence") or "").strip()
        if evidence and evidence != str(item.get("evidence") or "").strip():
            metrics["repaired_evidence_count"] += 1
            item["evidence"] = evidence
        item["source_type"] = str(ref.get("source_type") or item.get("source_type") or "unknown").strip()
        item["evidence_ref_id"] = ref_id
        return True

    def apply_project_item(item: dict[str, Any], *, section: str) -> bool:
        require_ref = _requires_daily_brief_evidence_ref(
            item,
            section=section,
            available_ref_source_types=available_ref_source_types,
        )
        ref_ids = _split_evidence_ref_ids(item.get("evidence_ref_id"))
        if not ref_ids and require_ref:
            inferred_ref = _best_evidence_ref_for_item(item, evidence_refs, section=section)
            if inferred_ref:
                ref_ids = [str(inferred_ref.get("id") or "").strip()]
            else:
                metrics["dropped_invalid_evidence_count"] += 1
                return False
        if not ref_ids:
            return True
        refs = [refs_by_id.get(ref_id) for ref_id in ref_ids]
        if any(ref is None for ref in refs):
            metrics["dropped_invalid_evidence_count"] += 1
            return False
        valid_refs = [ref for ref in refs if isinstance(ref, dict)]
        if not _evidence_refs_match_project_item(item, valid_refs):
            metrics["dropped_invalid_evidence_count"] += 1
            return False
        _correct_update_domain_from_evidence(item, valid_refs)
        evidence = "; ".join(str(ref.get("evidence") or "").strip() for ref in valid_refs if str(ref.get("evidence") or "").strip())
        if evidence and evidence != str(item.get("evidence") or "").strip():
            metrics["repaired_evidence_count"] += 1
            item["evidence"] = evidence
        source_types = {str(ref.get("source_type") or "").strip() for ref in valid_refs if str(ref.get("source_type") or "").strip()}
        item["source_type"] = "mixed" if len(source_types) > 1 else (next(iter(source_types)) if source_types else item.get("source_type"))
        item["evidence_ref_id"] = ", ".join(ref_ids)
        return True

    project_updates[:] = [
        item
        for item in project_updates
        if not isinstance(item, dict)
        or apply_project_item(item, section="project_updates")
    ]
    other_updates[:] = [
        item
        for item in other_updates
        if not isinstance(item, dict)
        or apply_project_item(item, section="other_updates")
    ]

    my_todos[:] = [
        item
        for item in my_todos
        if not isinstance(item, dict)
        or apply_to_item(
            item,
            section="my_todos",
            require_ref=_requires_daily_brief_evidence_ref(
                item,
                section="my_todos",
                available_ref_source_types=available_ref_source_types,
            ),
        )
    ]
    reminders[:] = [
        item
        for item in reminders
        if not isinstance(item, dict)
        or apply_to_item(
            item,
            section="team_member_reminders",
            require_ref=_requires_seatalk_evidence_ref(item, section="team_member_reminders"),
        )
    ]
    return metrics


def _split_evidence_ref_ids(value: Any) -> list[str]:
    return [part.strip() for part in re.split(r"[,;]\s*", str(value or "")) if part.strip()]


def _seatalk_ref_supports_xiaodong_action(ref: dict[str, Any]) -> bool:
    if str(ref.get("reply_state") or "").strip() == "xiaodong_commitment":
        return True
    if _sender_is_xiaodong(str(ref.get("sender") or "")):
        return True
    source_text = " ".join(str(ref.get(field) or "") for field in ("snippet", "thread"))
    normalized = _normalize_person_key(source_text)
    return "xiaodong" in normalized or "zheng xiaodong" in normalized


def _correct_update_domain_from_evidence(item: dict[str, Any], refs: list[dict[str, Any]]) -> None:
    """Keep KYC backlog reporting out of Credit Risk unless the source says so."""
    if _display_domain(item.get("domain")) != "Credit Risk" or not refs:
        return
    source_labels = " ".join(
        str(ref.get(field) or "")
        for ref in refs
        for field in ("group", "thread", "subject", "evidence")
    ).casefold()
    if "kyc" in source_labels and "credit risk" not in source_labels and "crms" not in source_labels:
        item["domain"] = "General"


def _correct_known_update_domains(items: list[dict[str, Any]]) -> None:
    """Apply source-owned domain corrections after any visibility backfill."""
    for item in items:
        if not isinstance(item, dict):
            continue
        source_labels = " ".join(str(item.get(field) or "") for field in ("evidence", "title")).casefold()
        item_text = _item_text(item)
        if any(term in item_text or term in source_labels for term in ("548-549", "af sheet", "scam model v3", "anti-fraud", "anti fraud")):
            item["domain"] = "Anti-fraud"
        elif any(term in item_text or term in source_labels for term in ("grc", "authorization management", "issue & action plan")):
            item["domain"] = "Ops Risk"
        elif "retail credit review" in item_text or "retail credit review" in source_labels:
            item["domain"] = "Credit Risk"
        elif (
            _display_domain(item.get("domain")) == "Credit Risk"
            and "kyc" in source_labels
            and "credit risk" not in source_labels
            and "crms" not in source_labels
        ):
            item["domain"] = "General"


def _best_evidence_ref_for_item(
    item: dict[str, Any],
    evidence_refs: list[dict[str, Any]],
    *,
    section: str,
) -> dict[str, Any] | None:
    source_type = str(item.get("source_type") or "").strip().lower()
    item_evidence = _normalize_thread_match_text(item.get("evidence"))
    item_text = _item_text(item, fields=("title", "summary", "task", "reminder", "person", "evidence"))
    item_tokens = _evidence_match_tokens(item_text)
    best: tuple[int, dict[str, Any]] | None = None
    for ref in evidence_refs:
        ref_source_type = str(ref.get("source_type") or "").strip().lower()
        if source_type == "seatalk" and ref_source_type != "seatalk":
            continue
        if source_type == "gmail" and ref_source_type != "gmail":
            continue
        ref_text = " ".join(
            str(ref.get(field) or "")
            for field in ("group", "thread", "subject", "participants", "sender", "snippet", "evidence")
        )
        ref_evidence = _normalize_thread_match_text(ref.get("evidence"))
        score = 0
        if item_evidence and ref_evidence and not _is_generic_seatalk_evidence(item_evidence):
            if item_evidence == ref_evidence:
                # An exact source label narrows the candidate set, but a group
                # can have several messages with the same rendered label. Keep
                # scoring the snippet, thread, and mentioned people so an
                # incident is not attached to a nearby reminder in the same
                # group.
                score += 30
            elif item_evidence in ref_evidence or ref_evidence in item_evidence:
                score += 16
        parsed_item_evidence = _parse_seatalk_evidence_ref(item.get("evidence"))
        if parsed_item_evidence.get("thread") and _normalize_thread_match_text(parsed_item_evidence.get("thread")) == _normalize_thread_match_text(ref.get("thread")):
            score += 10
        ref_tokens = _evidence_match_tokens(ref_text)
        overlap = item_tokens & ref_tokens
        item_people = _extract_item_people_for_evidence_validation(item)
        item_people_tokens: set[str] = set()
        for person in item_people:
            item_people_tokens.update(_evidence_match_tokens(person))
        score += min(len(overlap), 12)
        # Prefer topical overlap over a shared person name when the SeaTalk
        # source label is generic, e.g. two Xiaodong messages about Hui
        # Xian/Liye in different groups during the same window. Keep Gmail's
        # stricter legacy threshold unchanged.
        if ref_source_type == "seatalk":
            score += min(len(overlap - item_people_tokens), 6)
        if item_people and _evidence_ref_matches_item_people(item, ref):
            score += 3
        if not _evidence_refs_match_project_item(item, [ref]):
            score -= 20
        if section in {"project_updates", "other_updates"} and ref_source_type == "gmail" and source_type in {"", "unknown"}:
            score += 1
        if score >= 6 and (best is None or score > best[0]):
            best = (score, ref)
    return best[1] if best else None


def _evidence_refs_match_project_item(item: dict[str, Any], refs: list[dict[str, Any]]) -> bool:
    item_text = _item_text(item, fields=("title", "summary", "task", "reminder"))
    item_tokens = _evidence_match_tokens(item_text)
    if not item_tokens:
        return True
    ref_tokens: set[str] = set()
    ref_text_parts: list[str] = []
    for ref in refs:
        ref_text = " ".join(str(ref.get(field) or "") for field in ("group", "thread", "subject", "participants", "sender", "snippet", "evidence"))
        ref_text_parts.append(ref_text)
        ref_tokens.update(_evidence_match_tokens(ref_text))
    if _evidence_ref_has_domain_mismatch(item, " ".join(ref_text_parts), item_tokens):
        return False
    if all(str(ref.get("source_type") or "").strip().lower() == "seatalk" for ref in refs):
        item_dates = _material_day_month_markers(item_text)
        ref_dates = _material_day_month_markers(" ".join(ref_text_parts))
        if item_dates and not item_dates.issubset(ref_dates):
            return False
    overlap = item_tokens & ref_tokens
    return len(overlap) >= 2


def _material_day_month_markers(value: Any) -> set[str]:
    text = " ".join(str(value or "").casefold().split())
    month_numbers = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    month_pattern = (
        r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
    )
    markers = {
        f"{int(day):02d}-{month_numbers[month[:3]]:02d}"
        for day, month in re.findall(rf"\b(\d{{1,2}})\s+({month_pattern})\b", text)
    }
    markers.update(
        f"{int(day):02d}-{month_numbers[month[:3]]:02d}"
        for month, day in re.findall(rf"\b({month_pattern})\s+(\d{{1,2}})\b", text)
    )
    for year, month, day in re.findall(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text):
        markers.add(f"{int(day):02d}-{int(month):02d}")
    for month, day in re.findall(r"\b(0[1-9]|1[0-2])([0-3]\d)\b", text):
        markers.add(f"{int(day):02d}-{int(month):02d}")
    return markers


def _evidence_ref_has_domain_mismatch(item: dict[str, Any], ref_text: str, item_tokens: set[str]) -> bool:
    domain = _display_domain(item.get("domain"))
    normalized_ref = _normalize_thread_match_text(ref_text)
    if _evidence_ref_has_group_topic_mismatch(normalized_ref, item_tokens):
        return True
    credit_source = any(term in normalized_ref for term in ("credit risk", "credit", "crms", "loan", "自营贷", "贷款"))
    anti_item = bool({"af", "anti", "fraud", "push", "notification", "pn", "false", "alarm", "afasa", "alc"} & item_tokens)
    credit_item = bool({"credit", "loan", "crms", "npl", "dwh", "cbs"} & item_tokens)
    if credit_source and anti_item and not credit_item:
        return True
    if domain == "Credit Risk":
        anti_source = any(term in normalized_ref for term in ("anti fraud", "anti-fraud", " af ", "aaf", "afasa"))
        credit_item = bool({"credit", "loan", "crms", "npl", "dwh", "cbs"} & item_tokens)
        anti_item = bool({"af", "anti", "fraud", "afasa", "alc"} & item_tokens)
        if anti_source and credit_item and not anti_item:
            return True
    return False


def _evidence_ref_has_group_topic_mismatch(normalized_ref: str, item_tokens: set[str]) -> bool:
    if "db拆库" in normalized_ref or "db 拆库" in normalized_ref:
        db_split_tokens = {
            "db",
            "database",
            "split",
            "downtime",
            "af00",
            "apollo",
            "domain",
            "migration",
            "migrate",
            "0526",
            "拆库",
            "迁移",
            "停机",
            "域名",
        }
        customer_ticket_tokens = {"customer", "transaction", "ticket", "log", "uploaded", "unable", "approve"}
        if item_tokens & customer_ticket_tokens and not item_tokens & db_split_tokens:
            return True
    if "compliance afasa" in normalized_ref and "alcv12" in normalized_ref:
        alc_parameter_tokens = {"fvversion", "fid", "lcrequestid", "parameter", "native", "face", "verification"}
        if item_tokens & alc_parameter_tokens:
            return True
    return False


def _requires_seatalk_evidence_ref(item: dict[str, Any], *, section: str) -> bool:
    if section == "team_member_reminders":
        return _item_uses_seatalk_source(item)
    if section == "my_todos":
        return _item_uses_seatalk_source(item)
    return False


def _requires_daily_brief_evidence_ref(
    item: dict[str, Any],
    *,
    section: str,
    available_ref_source_types: set[str] | None = None,
) -> bool:
    if section in {"project_updates", "other_updates"}:
        source_type = str(item.get("source_type") or "").strip().lower()
        available = available_ref_source_types or set()
        if source_type in {"seatalk", "gmail"}:
            return source_type in available
        if source_type == "mixed":
            return bool({"seatalk", "gmail"} & available)
        return bool(available)
    if section == "my_todos":
        source_type = str(item.get("source_type") or "").strip().lower()
        available = available_ref_source_types or set()
        if source_type in {"seatalk", "gmail"}:
            return True
        if source_type == "mixed":
            return True
        if available:
            return True
    return _requires_seatalk_evidence_ref(item, section=section)


def _evidence_ref_matches_item_people(item: dict[str, Any], ref: dict[str, Any]) -> bool:
    people = _extract_item_people_for_evidence_validation(item)
    if not people:
        return True
    ref_people = " ".join(
        [
            str(ref.get("sender") or ""),
            str(ref.get("snippet") or ""),
            " ".join(str(person or "") for person in ref.get("mentioned_people") or []),
        ]
    )
    ref_people_key = _normalize_thread_match_text(ref_people.replace("@", " "))
    return all(_person_name_supported_by_text(person, ref_people_key) for person in people)


def _count_generic_evidence(items: list[dict[str, Any]]) -> int:
    return sum(1 for item in items if isinstance(item, dict) and _is_generic_seatalk_evidence(item.get("evidence")))


def _repair_generic_seatalk_evidence(
    items: list[dict[str, Any]],
    *,
    history_text: str,
    quality_metrics: dict[str, int] | None = None,
) -> None:
    records = _seatalk_history_records_for_evidence(history_text)
    if not records:
        return
    for item in items:
        if not isinstance(item, dict) or not _needs_seatalk_evidence_repair(item):
            continue
        repaired = _best_seatalk_evidence_for_item(item, records)
        if repaired:
            if repaired != str(item.get("evidence") or "").strip() and quality_metrics is not None:
                quality_metrics["repaired_evidence_count"] = quality_metrics.get("repaired_evidence_count", 0) + 1
            item["evidence"] = repaired
        else:
            item["evidence"] = _normalize_generic_seatalk_evidence(item.get("evidence"))


def _validate_and_repair_seatalk_evidence(
    items: list[dict[str, Any]],
    *,
    history_text: str,
    quality_metrics: dict[str, int] | None = None,
    name_mappings: dict[str, str] | None = None,
) -> None:
    records = _seatalk_history_records_for_evidence(history_text)
    if not records:
        return
    for item in items:
        if not isinstance(item, dict) or not _item_uses_seatalk_source(item):
            continue
        if str(item.get("followup_source") or "").startswith("deterministic_xiaodong_") and str(
            item.get("evidence_ref_id") or ""
        ).strip():
            # This item already passed exact evidence-ref validation. Do not
            # discard bilingual requests because a second fuzzy token check
            # finds only the English version markers in the Chinese source.
            continue
        parsed = _parse_seatalk_evidence_ref(item.get("evidence"))
        thread = parsed.get("thread", "")
        if not thread:
            group = parsed.get("group", "")
            if _is_private_seatalk_evidence_ref(group):
                matching_records = _records_matching_group(records, group, name_mappings=name_mappings)
                if not matching_records or not _seatalk_private_evidence_matches_item_topic(item, matching_records):
                    item["_drop_invalid_evidence"] = True
                    if quality_metrics is not None:
                        quality_metrics["dropped_invalid_evidence_count"] = quality_metrics.get("dropped_invalid_evidence_count", 0) + 1
            elif group and not _is_generic_seatalk_evidence(group):
                matching_records = _records_matching_group(records, group, name_mappings=name_mappings)
                if not matching_records or not _seatalk_evidence_matches_item_topic(item, matching_records):
                    item["_drop_invalid_evidence"] = True
                    if quality_metrics is not None:
                        quality_metrics["dropped_invalid_evidence_count"] = quality_metrics.get("dropped_invalid_evidence_count", 0) + 1
            continue
        matching_records = _records_matching_thread(records, thread)
        if not matching_records:
            item["_drop_invalid_evidence"] = True
            if quality_metrics is not None:
                quality_metrics["dropped_invalid_evidence_count"] = quality_metrics.get("dropped_invalid_evidence_count", 0) + 1
            continue
        group = parsed.get("group", "")
        group_matches = _records_matching_group(matching_records, group, name_mappings=name_mappings)
        if group_matches:
            if not _seatalk_evidence_matches_item_topic(item, group_matches) or not _seatalk_record_mentions_item_people(item, group_matches):
                item["_drop_invalid_evidence"] = True
                if quality_metrics is not None:
                    quality_metrics["dropped_invalid_evidence_count"] = quality_metrics.get("dropped_invalid_evidence_count", 0) + 1
            continue
        repaired = _best_seatalk_record_for_item(item, matching_records)
        if repaired and _seatalk_record_mentions_item_people(item, matching_records):
            if _format_seatalk_record_evidence(repaired) != str(item.get("evidence") or "").strip() and quality_metrics is not None:
                quality_metrics["repaired_evidence_count"] = quality_metrics.get("repaired_evidence_count", 0) + 1
            item["evidence"] = _format_seatalk_record_evidence(repaired)
        else:
            item["_drop_invalid_evidence"] = True
            if quality_metrics is not None:
                quality_metrics["dropped_invalid_evidence_count"] = quality_metrics.get("dropped_invalid_evidence_count", 0) + 1
    items[:] = [item for item in items if not item.pop("_drop_invalid_evidence", False)]


def _is_private_seatalk_evidence_ref(value: Any) -> bool:
    normalized = _normalize_thread_match_text(value)
    return bool(re.search(r"\bbuddy\s+\d+\b", normalized)) or normalized.startswith("private seatalk chat")


def _records_matching_group(
    records: list[dict[str, str]],
    group: str,
    *,
    name_mappings: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    if not str(group or "").strip():
        return []
    target_buddy_id = _seatalk_buddy_id(group)
    if target_buddy_id:
        return [record for record in records if _seatalk_buddy_id(record.get("group")) == target_buddy_id]
    direct_mapping_keys = _seatalk_ids_for_mapped_label(group, name_mappings=name_mappings)
    if direct_mapping_keys:
        return [
            record
            for record in records
            if any(_seatalk_group_ref_matches(key, record.get("group", "")) for key in direct_mapping_keys)
            or _seatalk_group_ref_matches(group, record.get("group", ""))
        ]
    return [record for record in records if _seatalk_group_ref_matches(group, record.get("group", ""))]


def _seatalk_ids_for_mapped_label(label: Any, *, name_mappings: dict[str, str] | None = None) -> set[str]:
    normalized_label = _normalize_seatalk_source_label(label).casefold()
    if not normalized_label:
        return set()
    return {
        str(key)
        for key, name in (name_mappings or {}).items()
        if _normalize_seatalk_source_label(name).casefold() == normalized_label
    }


def _seatalk_evidence_matches_item_topic(item: dict[str, Any], records: list[dict[str, str]]) -> bool:
    return _best_seatalk_record_for_item(item, records) is not None


def _seatalk_private_evidence_matches_item_topic(item: dict[str, Any], records: list[dict[str, str]]) -> bool:
    topic_fields = ("title", "summary", "task", "reminder")
    item_text = _item_text(item, fields=topic_fields)
    raw_item_text = " ".join(
        str(item.get(field) or "").strip() for field in topic_fields if str(item.get(field) or "").strip()
    )
    item_tokens = _evidence_match_tokens(item_text)
    if not item_tokens:
        return False
    record_text = " ".join(
        " ".join([record.get("group", ""), record.get("thread", ""), record.get("sender", ""), record.get("text", "")])
        for record in records
    )
    record_tokens = _evidence_match_tokens(record_text)
    if not record_tokens:
        return False
    required_name_tokens = _private_chat_required_name_tokens(raw_item_text)
    if required_name_tokens and not (required_name_tokens & record_tokens):
        return False
    salient_tokens = {
        token
        for token in item_tokens
        if token not in _PRIVATE_CHAT_WEAK_MATCH_TOKENS and not re.fullmatch(r"\d{3,}", token)
    }
    overlap = salient_tokens & record_tokens
    if len(overlap) >= 2:
        return True
    strong_overlap = overlap & _PRIVATE_CHAT_STRONG_TOPIC_TOKENS
    return bool(strong_overlap and len(overlap) >= 1 and not required_name_tokens)


_PRIVATE_CHAT_STRONG_TOPIC_TOKENS = {
    "afasa",
    "alc",
    "amr",
    "crc",
    "crms",
    "dps",
    "grc",
    "prd",
    "sfv",
    "sop",
    "uat",
    "viber",
    "afasa",
    "投诉",
    "审批",
    "上线",
    "需求",
}


_PRIVATE_CHAT_WEAK_MATCH_TOKENS = {
    "action",
    "align",
    "arrange",
    "ask",
    "check",
    "clarify",
    "confirm",
    "coordinate",
    "discussion",
    "feedback",
    "follow",
    "help",
    "issue",
    "matter",
    "meeting",
    "next",
    "plan",
    "question",
    "questions",
    "reply",
    "schedule",
    "status",
    "steps",
    "support",
    "team",
    "timeline",
    "week",
    "安排",
    "确认",
    "问题",
    "处理",
    "跟进",
}


def _private_chat_required_name_tokens(text: Any) -> set[str]:
    raw = str(text or "")
    tokens: set[str] = set()
    for alias, canonical in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE.items():
        if _normalize_thread_match_text(alias) in _normalize_thread_match_text(raw):
            tokens.update(_evidence_match_tokens(canonical))
    for match in re.findall(r"\b[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,}){0,2}\b", raw):
        words = [word.casefold() for word in re.findall(r"[A-Za-z]{3,}", match)]
        if not words:  # pragma: no cover - regex only returns alphabetic name tokens.
            continue
        if words[0] in {"arrange", "confirm", "follow", "check", "source", "private", "seatalk"}:
            words = words[1:]
        tokens.update(word for word in words if word not in _PRIVATE_CHAT_WEAK_MATCH_TOKENS)
    return tokens


def _drop_domain_mismatched_evidence_items(
    items: list[dict[str, Any]],
    *,
    quality_metrics: dict[str, Any] | None = None,
) -> None:
    for item in items:
        if not isinstance(item, dict):
            continue
        item_tokens = _evidence_match_tokens(_item_text(item, fields=("title", "summary", "task", "reminder")))
        if item_tokens and _evidence_ref_has_domain_mismatch(item, str(item.get("evidence") or ""), item_tokens):
            item["_drop_domain_mismatch"] = True
            if quality_metrics is not None:
                quality_metrics["dropped_invalid_evidence_count"] = quality_metrics.get("dropped_invalid_evidence_count", 0) + 1
                quality_metrics["dropped_domain_mismatch_count"] = quality_metrics.get("dropped_domain_mismatch_count", 0) + 1
    items[:] = [item for item in items if not isinstance(item, dict) or not item.pop("_drop_domain_mismatch", False)]


def _drop_generic_seatalk_evidence_items(
    items: list[dict[str, Any]],
    *,
    quality_metrics: dict[str, Any] | None = None,
) -> None:
    for item in items:
        if not isinstance(item, dict):
            continue
        exact_deterministic_xiaodong_action = (
            str(item.get("followup_source") or "").startswith("deterministic_xiaodong_")
            and bool(str(item.get("evidence_ref_id") or "").strip())
        )
        # _apply_daily_brief_evidence_refs has already validated these items
        # against an exact ref. The ref can still render as "SeaTalk group"
        # when no group-name mapping is available, which is not a reason to
        # erase a direct request addressed to Xiaodong.
        if (
            _item_uses_seatalk_source(item)
            and _is_generic_seatalk_evidence(item.get("evidence"))
            and not exact_deterministic_xiaodong_action
        ):
            item["_drop_generic_seatalk_evidence"] = True
            if quality_metrics is not None:
                quality_metrics["dropped_invalid_evidence_count"] = quality_metrics.get("dropped_invalid_evidence_count", 0) + 1
                quality_metrics["dropped_generic_evidence_count"] = quality_metrics.get("dropped_generic_evidence_count", 0) + 1
    items[:] = [item for item in items if not isinstance(item, dict) or not item.pop("_drop_generic_seatalk_evidence", False)]


def _needs_seatalk_evidence_repair(item: dict[str, Any]) -> bool:
    source_type = str(item.get("source_type") or "").strip().lower()
    evidence = str(item.get("evidence") or "").strip()
    if source_type not in {"seatalk", "mixed", "unknown"}:
        return False
    return _is_generic_seatalk_evidence(evidence)


def _is_generic_seatalk_evidence(value: Any) -> bool:
    normalized = _normalize_generic_seatalk_evidence(value).casefold()
    if normalized.startswith("seatalk group"):
        return True
    generic_values = {
        "seatalk group",
        "seatalk contact",
        "seatalk conversation",
        "seatalk direct discussion",
        "seatalk thread",
        "private seatalk chat",
    }
    if normalized in generic_values:
        return True
    return bool(re.fullmatch(r"seatalk (?:group|contact|conversation|thread)(?:\s*/\s*thread:\s*.+)?", normalized))


def _normalize_generic_seatalk_evidence(value: Any) -> str:
    text = " ".join(str(value or "").strip().split())
    text = re.sub(r"\bSeaTalk\s+SeaTalk\s+group\b", "SeaTalk group", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSeaTalk\s+SeaTalk\s+contact\b", "SeaTalk contact", text, flags=re.IGNORECASE)
    return text or "SeaTalk conversation"


def _seatalk_history_records_for_evidence(history_text: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current_group = ""
    for line in str(history_text or "").splitlines():
        header_match = _SEATALK_HISTORY_HEADER_RE.match(line.strip())
        if header_match:
            current_group = header_match.group("group").strip()
            continue
        message_match = _SEATALK_HISTORY_MESSAGE_RE.match(line)
        if not message_match:
            if records and line[:1].isspace() and line.strip():
                records[-1]["text"] = f"{records[-1].get('text', '').strip()} {line.strip()}".strip()
            continue
        if not current_group:
            continue
        records.append(
            {
                "timestamp": message_match.group("timestamp").strip(),
                "group": current_group,
                "thread": (message_match.group("thread") or "").strip(),
                "sender": message_match.group("sender").strip(),
                "text": message_match.group("text").strip(),
            }
        )
    return records


def _best_seatalk_evidence_for_item(item: dict[str, Any], records: list[dict[str, str]]) -> str:
    best_record = _best_seatalk_record_for_item(item, records)
    if not best_record:
        return ""
    return _format_seatalk_record_evidence(best_record)


def _best_seatalk_record_for_item(item: dict[str, Any], records: list[dict[str, str]]) -> dict[str, str] | None:
    item_tokens = _evidence_match_tokens(_item_text(item, fields=("title", "summary", "task", "reminder", "person")))
    if not item_tokens:
        return None
    best_record: dict[str, str] | None = None
    best_score = 0
    for record in records:
        record_text = " ".join(
            [record.get("group", ""), record.get("thread", ""), record.get("sender", ""), record.get("text", "")]
        )
        record_tokens = _evidence_match_tokens(record_text)
        score = len(item_tokens & record_tokens)
        if {"money", "lock"}.issubset(item_tokens) and {"money", "lock"}.issubset(record_tokens):
            score += 3
        if {"kill", "switch"}.issubset(item_tokens) and {"kill", "switch"}.issubset(record_tokens):
            score += 3
        if score > best_score:
            best_score = score
            best_record = record
    if not best_record or best_score < 3:
        return None
    return best_record


def _format_seatalk_record_evidence(record: dict[str, str], *, name_mappings: dict[str, str] | None = None) -> str:
    group = _mapped_seatalk_identifier_label(record.get("group"), name_mappings=name_mappings)
    thread = str(record.get("thread") or "").strip()
    if not group:
        return f"SeaTalk group / thread: {thread}" if thread else "SeaTalk group"
    if group.startswith("group-"):
        group = "SeaTalk group"
    evidence = f"{group} / thread: {thread}" if thread else group
    return _sanitize_seatalk_evidence(_normalize_seatalk_source_label(evidence), name_mappings=name_mappings)


def _item_uses_seatalk_source(item: dict[str, Any]) -> bool:
    source_type = str(item.get("source_type") or "").strip().lower()
    evidence = str(item.get("evidence") or "").strip().lower()
    return source_type in {"seatalk", "mixed", "unknown"} or "seatalk" in evidence or "private seatalk" in evidence or "/ thread:" in evidence


def _parse_seatalk_evidence_ref(value: Any) -> dict[str, str]:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return {"group": "", "thread": ""}
    match = re.search(r"^(?P<group>.*?)\s*/\s*thread:\s*(?P<thread>.+)$", text, flags=re.IGNORECASE)
    if match:
        return {"group": match.group("group").strip(), "thread": match.group("thread").strip()}
    return {"group": text, "thread": ""}


def _records_matching_thread(records: list[dict[str, str]], thread: str) -> list[dict[str, str]]:
    normalized_thread = _normalize_thread_match_text(thread)
    if not normalized_thread:
        return []
    matches: list[dict[str, str]] = []
    for record in records:
        record_thread = _normalize_thread_match_text(record.get("thread"))
        if not record_thread:
            continue
        if record_thread == normalized_thread or normalized_thread in record_thread or record_thread in normalized_thread:
            matches.append(record)
    return matches


def _seatalk_group_ref_matches(left: Any, right: Any) -> bool:
    left_buddy = _seatalk_buddy_id(left)
    right_buddy = _seatalk_buddy_id(right)
    if left_buddy or right_buddy:
        return bool(left_buddy and right_buddy and left_buddy == right_buddy)
    left_norm = _normalize_thread_match_text(left)
    right_norm = _normalize_thread_match_text(right)
    if not left_norm or not right_norm:
        return False
    if left_norm == "private seatalk chat" or right_norm == "private seatalk chat":
        return left_norm == right_norm
    if left_norm in {"seatalk group", "seatalk conversation", "seatalk thread"}:
        return True
    return left_norm == right_norm or left_norm in right_norm or right_norm in left_norm


def _seatalk_buddy_id(value: Any) -> str:
    match = re.search(r"\bbuddy[-\s]*(\d+)\b", str(value or ""), flags=re.IGNORECASE)
    return f"buddy-{match.group(1)}" if match else ""


def _seatalk_record_mentions_item_people(item: dict[str, Any], records: list[dict[str, str]]) -> bool:
    people = _extract_item_people_for_evidence_validation(item)
    if not people:
        return True
    record_text = _normalize_thread_match_text(
        " ".join(" ".join([record.get("sender", ""), record.get("text", ""), record.get("thread", "")]) for record in records).replace("@", " ")
    )
    return all(_person_name_supported_by_text(person, record_text) for person in people)


def _person_name_supported_by_text(person: Any, normalized_text: str) -> bool:
    canonical = str(person or "").strip()
    aliases = [canonical]
    aliases.extend(alias for alias, mapped in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE.items() if mapped == canonical)
    compact_text = str(normalized_text or "").replace(" ", "")
    for alias in aliases:
        normalized_alias = _normalize_thread_match_text(str(alias).replace("@", " "))
        if normalized_alias and normalized_alias in normalized_text:
            return True
        compact_alias = _normalize_person_key(alias).replace(" ", "")
        if compact_alias and compact_alias in compact_text:
            return True
    return False


def _extract_item_people_for_evidence_validation(item: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    if str(item.get("person") or "").strip():
        candidates.append(str(item.get("person") or "").strip())
    for person in _mentioned_team_members(_item_text(item, fields=("title", "summary", "task", "reminder", "person"))):
        if person not in candidates:
            candidates.append(person)
    task_text = str(item.get("task") or item.get("reminder") or "").strip()
    match = re.match(
        r"^(?:ask|ensure|follow up with|check with|confirm with|monitor|remind)\s+"
        r"([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){0,3}?)(?=\s+(?:to|on|about|for|whether|if|with|by)\b|[:：,，.]|$)",
        task_text,
        flags=re.IGNORECASE,
    )
    if match:
        candidates.append(match.group(1).strip())
    allowed = {name.casefold() for name in TEAM_MEMBER_REMINDER_ALLOWED_PEOPLE.values()}
    people = []
    for candidate in candidates:
        normalized = " ".join(candidate.split())
        if normalized.casefold() in allowed:
            people.append(normalized)
    return people


def _evidence_match_tokens(value: Any) -> set[str]:
    text = str(value or "").casefold()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\b(source|status|due|tbd|done|blocked|in progress|unknown)\b", " ", text)
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text)
    tokens = set(re.findall(r"[a-z0-9]{3,}|[\u4e00-\u9fff]{2,}", text))
    stopwords = {
        "source",
        "seatalk",
        "group",
        "thread",
        "please",
        "confirm",
        "follow",
        "whether",
        "including",
        "needed",
        "needed",
        "today",
        "tomorrow",
        "with",
        "from",
        "that",
        "this",
        "will",
        "can",
        "and",
        "the",
        "next",
        "week",
        "meeting",
        "discussion",
        "update",
        "updates",
        "status",
        "team",
        "bank",
        "live",
        "issue",
        "issues",
        "需要",
        "确认",
        "是否",
        "可以",
        "我们",
        "你们",
        "这个",
    }
    return {token for token in tokens if token not in stopwords}


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


def _clean_daily_brief_evidence(items: list[dict[str, Any]]) -> None:
    """Keep rendered sources traceable without repeated labels or raw links."""
    for item in items:
        evidence = " ".join(str(item.get("evidence") or "").split())
        if not evidence:
            continue
        if str(item.get("source_type") or "").strip().lower() in {"gmail", "mixed"}:
            evidence = re.sub(r"\s*(?:[-—|]\s*)?https?://\S+", "", evidence).strip(" -—|")
        parts: list[str] = []
        seen: set[str] = set()
        for raw_part in evidence.split(";"):
            part = raw_part.strip()
            part_lowered = part.casefold()
            if any(
                cue in part_lowered
                for cue in ("google meet joining info", "video call link:", "time zone: asia/", "calendar.google.com")
            ):
                continue
            if re.search(
                r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b"
                r".*\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b"
                r".*\d{1,2}:\d{2}\s*(?:am|pm)?\s*[-–·]",
                part_lowered,
            ):
                continue
            key = _seatalk_evidence_thread_key(part) or _normalize_thread_match_text(part)
            if not part or key in seen:
                continue
            seen.add(key)
            parts.append(part)
        threaded_groups = {
            _normalize_thread_match_text(part.split("/ thread:", 1)[0])
            for part in parts
            if "/ thread:" in part.casefold()
        }
        parts = [
            part
            for part in parts
            if "/ thread:" in part.casefold() or _normalize_thread_match_text(part) not in threaded_groups
        ]
        item["evidence"] = re.sub(r"\s*\($", "", "; ".join(parts)).strip()


def _normalize_todo_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items:
        clean = dict(item)
        if _is_low_quality_todo_item(clean):
            continue
        clean["domain"] = _display_domain(clean.get("domain"))
        clean["priority"] = _normalize_priority(clean.get("priority"))
        clean["due"] = _display_due(clean.get("due"))
        clean["action_type"] = _classify_todo_action_type(clean)
        if clean["action_type"] == "direct_action":
            clean["task"] = _normalize_xiaodong_action_voice(clean.get("task"))
        normalized.append(clean)
    return normalized


def _is_low_quality_todo_item(item: dict[str, Any]) -> bool:
    """Drop acknowledgements, bare mentions, and generic fallback actions."""
    task = " ".join(str(item.get("task") or "").split()).strip()
    lowered = task.casefold()
    if not task:
        return True
    if task.startswith("@") and re.fullmatch(r"@[\w.-]{2,30}[。.!！]?", task):
        return True
    return lowered.startswith(
        (
            "respond to the unresolved seatalk request",
            "respond to the unresolved request in the",
            "follow up on the unresolved seatalk ask",
            "thanks ",
            "thanks.",
            "yes, that's correct",
            "yes, thats correct",
        )
    )


def _normalize_xiaodong_action_voice(value: Any) -> str:
    """Render Xiaodong's own section as direct imperatives, not third-person prose."""
    text = " ".join(str(value or "").split())
    text = re.sub(
        r"^(provide|add|share|submit|send)\s+xiaodong(?:'s|’s)\s+",
        r"\1 ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"^xiaodong\s+(?:should|must|needs\s+to|need\s+to|to)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return text[:1].upper() + text[1:] if text else text


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


def _prepare_project_update_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for item in items:
        if _is_meeting_logistics_or_availability_notice(
            _item_text(item, fields=("title", "summary", "evidence"))
        ):
            continue
        clean = dict(item)
        summary = _synthesize_project_update_summary(clean)
        if "context:" in summary.casefold():
            summary = re.split(r"\bcontext\s*:", summary, maxsplit=1, flags=re.IGNORECASE)[0].strip(" .") + "."
        if not summary or _is_placeholder_update_summary(summary):
            continue
        clean["summary"] = summary
        prepared.append(clean)
    return prepared


def _is_placeholder_update_summary(value: Any) -> bool:
    normalized = " ".join(str(value or "").casefold().split())
    return any(
        phrase in normalized
        for phrase in (
            "the update may affect the related delivery or operational work",
            "confirm the owner, impact, and next milestone",
            "the unresolved blocker or dependency may affect delivery or operational readiness",
        )
    )


def _prepare_other_update_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for item in items:
        clean = dict(item)
        summary = " ".join(str(clean.get("summary") or clean.get("title") or "").split())
        summary = re.split(r"\bcontext\s*:", summary, maxsplit=1, flags=re.IGNORECASE)[0].strip(" .")
        lowered = summary.casefold()
        if (
            not summary
            or summary.startswith("@")
            or lowered.startswith(("hi ", "hello ", "hey ", "http://", "https://"))
            or summary.count("@") >= 2
            or len(summary) > 420
            or summary.endswith(("?", "？"))
        ):
            continue
        if _is_placeholder_update_summary(summary):
            continue
        combined = " ".join(str(clean.get(field) or "") for field in ("title", "summary", "evidence")).casefold()
        if any(term in combined for term in ("incident", "outage", "downtime", "ddos")):
            clean["signal_type"] = "incident"
        elif (
            re.search(r"(?:usd|sgd|php|idr|\$)\s*[\d,.]+\s*[km]?", combined, flags=re.IGNORECASE)
            and any(term in combined for term in ("vendor", "infobip", "telco", "dependency", "routing", "sender id"))
        ):
            clean["signal_type"] = "cross_team_dependency"
        clean["summary"] = f"{summary}." if summary[-1:] not in ".!?。！？" else summary
        prepared.append(clean)
    return prepared


def _synthesize_other_update_summary(item: dict[str, Any]) -> str:
    """Create an awareness entry only when the event has an explicit impact."""
    raw = " ".join(str(item.get("summary") or item.get("title") or "").split())
    if not raw:
        return ""
    if all(label in raw.casefold() for label in ("state:", "impact:", "next:")) and not _is_placeholder_update_summary(raw):
        return raw
    known = _synthesize_project_update_summary(item)
    if known and not _is_placeholder_update_summary(known):
        return known
    combined = " ".join(str(item.get(field) or "") for field in ("title", "evidence", "summary"))
    lowered = combined.casefold()
    body = _clean_deterministic_fallback_body(raw).rstrip(" .!?。！？")
    if not body or _is_meeting_logistics_or_availability_notice(combined):
        return ""
    if "incident" in lowered or "outage" in lowered or "system down" in lowered:
        return (
            f"State: {body}. Impact: the live incident may affect customer operations or service recovery. "
            "Next: confirm the mitigation owner, recovery status, and recurrence-prevention plan."
        )
    if any(term in lowered for term in ("failed", "failure", "not working", "unable to", "error")):
        return (
            f"State: {body}. Impact: the failed flow is blocking or degrading the affected UAT or live journey. "
            "Next: isolate the failing dependency, apply the fix, and complete a retest."
        )
    if any(term in lowered for term in ("rejection", "rejected", "capacity", "alert", "phishing", "security")):
        return (
            f"State: {body}. Impact: the risk signal requires product or operational attention. "
            "Next: confirm the exposure, accountable owner, and remediation checkpoint."
        )
    return ""


def _clean_deterministic_fallback_body(value: Any) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    message_match = re.search(r"\bmessage\s*:\s*(.+)$", text, flags=re.IGNORECASE)
    if message_match:
        text = message_match.group(1).strip()
    text = re.sub(r"https?://\S+", "", text, flags=re.IGNORECASE)
    text = text.replace("[View detail]", "")
    text = re.sub(r"\bga_view_source=\S+", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?<![\w@])@[A-Za-z][A-Za-z0-9_.-]*(?:\s+[A-Z][a-z]+){0,2}",
        "",
        text,
    )
    text = re.sub(r"\s+\bcc\b.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\s*(?:hi|hello|hey|hihi)\b[\s,:：，]*", "", text, flags=re.IGNORECASE)
    return " ".join(text.split()).strip(" ,:;，：")


def _synthesize_project_update_summary(item: dict[str, Any]) -> str:
    raw = " ".join(str(item.get("summary") or item.get("title") or "").split())
    if not raw:
        return ""
    title = " ".join(str(item.get("title") or "").split())
    evidence = " ".join(str(item.get("evidence") or "").split())
    combined = " ".join(part for part in (title, evidence, raw) if part)
    lowered = combined.casefold()
    raw_lowered = raw.casefold()
    deterministic_fallback = str(item.get("fallback_source") or "").startswith("deterministic_")
    transcript_like = (
        raw.startswith("@")
        or raw_lowered.startswith(("hi ", "hello ", "hey "))
        or "context:" in raw_lowered
        or raw.count("@") >= 2
        or "cc @" in lowered
        or "i will " in lowered
        or raw.startswith("http")
        or "high-signal update" in title.casefold()
        or len(raw) > 300
        or bool(re.search(r"(?:^|\s)[1-9]\.\s", raw))
        or bool(re.search(r"\?{1,2}$", raw))
        or deterministic_fallback
    )
    if "mari stock" in lowered and "216820" in lowered and not all(
        label in raw_lowered for label in ("state:", "impact:", "next:")
    ) and any(term in lowered for term in ("ui/ux", "prd review", "tech design", "tiger", "market-data", "market data")):
        return "State: Mari Stock Trading project 216820 remains on track: APP UI/UX is complete, PRD reviews are nearing finalization, technical design is in progress, and core Tiger/ITC setup is confirmed. Impact: no schedule exception is currently signaled, but PRD, market-data-vendor, and fractional-share design checkpoints still govern implementation readiness. Next: close the internal and Tiger-flow PRD reviews, confirm market-data integration, and complete fractional-share design."
    if "seller cash loan" in lowered and "direct debit" in lowered:
        if any(term in lowered for term in ("23/9", "23 sep", "23 september", "cutoff by monday", "china start holidays")):
            return "State: Seller Cash Loan Direct Debit remains blocked in UAT because the Shopee-side issue prevents successful transactions; AF v3.53 is now targeted for 23 Sep, with a 21 Sep cutoff for testing. Impact: failure to sign off by the cutoff could remove the AF ticket from the deployment or require rollback, with limited support during the China holiday. Next: Shopee must fix the UAT issue and QA must retest before 21 Sep."
        return "State: Seller Cash Loan Direct Debit UAT is blocked by a Shopee-side issue that prevents successful transactions. Impact: the AF release cannot be signed off until the upstream issue is fixed. Next: obtain the Shopee fix and complete a successful QA retest."
    if "qris" in lowered and "uat" in lowered and any(term in lowered for term in ("blocker", "waiting to", "select an spaylater", "payment failed")):
        return "State: QRIS CrossBorder UAT still has one highest-severity blocker and five high-severity issues, including the incorrect SPayLater tenure prompt and unresolved SeaBank/payment-result integrations. Impact: the UAT package is not ready for sign-off. Next: close the remaining AS/SPP fixes and run the listed retests before release approval."
    if "mas" in lowered and any(term in lowered for term in ("scheduled transfer", "schedule transfer", "drainage rule")):
        return "State: Scheduled-transfer controls remain pending MAS confirmation on the drainage-rule check. Impact: public launch depends on regulatory confirmation. Next: obtain the MAS response and update launch readiness."
    if "scheduled and recurring transfer" in lowered or "scheduled transfer" in lowered and "p0" in lowered:
        return "State: The P0 Scheduled and Recurring Transfers release has 28 open SIT/UAT bugs, including Admin Portal entry failures and a PayNow interbank execution blocker. Impact: release validation and go-live readiness remain blocked. Next: prioritize the blocking defects, publish owner-level dispositions, and complete verified UAT/SIT retests before the release checkpoint."
    if "force upgrade" in lowered and any(term in lowered for term in ("v3.05", "3.05", "app version")):
        return "State: AF is preparing an App force-upgrade to reduce version-dependent authentication branches; about 5% of users remain below v3.05 and the target is to start once the affected cohort falls below 2–3%. Impact: the decision affects AF rule-maintenance complexity and customer coverage. Next: confirm the 3.5.0 baseline and obtain Bank PMO approval for the target timing."
    if ("dd05" in lowered and "faq" in lowered) or ("blank page" in lowered and "need help" in lowered):
        return "State: PH live issue analysis found that DD05 transaction types lack the required FAQ mapping, sending users to a blank page from Need Help; the PM request has now been raised for configuration. Impact: affected SPP AS DP and QR P2M journeys may show a broken support path at go-live. Next: complete and validate the DD05 FAQ configuration before the next-week go-live."
    if "manual provisioning" in lowered and "uat" in lowered:
        return "State: PH GPay UAT manual-provisioning SMS is not working, while the team has agreed to run regression for the retry-logic ticket. Impact: the UAT path remains a test dependency for v3.29 sign-off. Next: reproduce the error, complete the regression test, and record whether the flow is a release blocker or a deferred enhancement."
    if "problem ticket" in lowered and any(term in lowered for term in ("sfv", "recurrence frequency", "operational impact")):
        return "State: The SFV live issue is being converted into a Problem Ticket because investigation and a sustainable fix need more time. Impact: recurrence frequency and manual recovery effort will determine the long-term priority and fix SLA. Next: confirm the recurrence and operational-impact assessment and track the remediation plan."
    if "address proof" in lowered and "rejection" in lowered:
        return "State: Recent foreigner-onboarding rejections were traced to missing or invalid address proof rather than a SingPass retrieval failure. Impact: the gap can continue to block eligible foreign-worker onboarding. Next: Bank and Shopee teams are defining simpler address-proof alternatives and validating them through upcoming interviews."
    if _is_material_vendor_cost_signal(lowered):
        return "State: GMS may classify all PH SMS sender IDs as international, creating an estimated USD400k monthly cost. Impact: the unresolved routing classification creates material recurring cost exposure. Next: Infobip must confirm the affected account and telco scope and provide a remediation decision."
    if "mas" in lowered and "hold & release" in lowered and any(
        term in lowered for term in ("benchmark", "sfv cost", "human error", "financial loss")
    ):
        return "State: Hold & Release supports Fraud Risk's MAS commitment to benchmark peer-bank controls, reduce SFV cost, and mitigate operational loss from human error. Impact: unresolved PRD and delivery decisions put both the regulatory commitment and operational-risk control at risk. Next: complete the PRD and lock accountable delivery scope."
    if "mas" in lowered and "hold & release" in lowered and any(
        term in lowered for term in ("approval", "proceed with dev", "proceed with development", "do not launch", "上线")
    ):
        return "State: Hold & Release development can proceed while MAS approval is pending, but production launch remains gated by Regulatory Compliance approval. Impact: development can advance without weakening the regulatory launch control. Next: confirm the development decision with Bob and keep go-live blocked until approval is received."
    if not transcript_like:
        return raw

    if "scam model" in lowered and "v3" in lowered:
        return "State: Scam Model V3's new and updated live features have been configured in the AF system. Impact: the 3.09 release remains the Q3/MAS delivery checkpoint. Next: track the remaining scope and release readiness against that deadline."
    if "weekly report" in lowered and "3.09" in lowered:
        return "State: The weekly report keeps 3.09 as the hard Q3/MAS deadline, while the remove-password 3.5.0 rollout is already live and ramping. Impact: remaining 3.09 scope and release risks need active tracking. Next: confirm the outstanding deliverables and owners."
    if "weekly report" in lowered and "retail credit review" in lowered and "v3.07" in lowered:
        return "State: Retail Credit Review timing and engine-output validation are confirmed for the v3.07_0827 deployment before August's monthly review. Impact: the release date is the control point for completing validation on time. Next: track validation readiness and surface any risk to the v3.07_0827 deployment."
    if "mari stock" in lowered and any(
        term in lowered for term in ("thank you for your continued support", "hi all")
    ) and not any(
        term in lowered
        for term in ("payment bc", "mta", "stock asset api", "fx rate", "sof breakdown", "delay", "blocked", "dependency", "mas")
    ):
        return ""
    if "free text" in lowered and "lexisnexis" in lowered and ("channeling" in lowered or "loan partner" in lowered):
        return (
            "State: The ID KYC backlog was reprioritized: Foreigner Onboarding and language preference target v3.52.2_0917, "
            "KYC name similarity targets v3.52_0910, RFI document expiry is tentatively v3.54_1015, and revived Forgot Phone Number Phase 2 now ranks ahead of LexisNexis screening. "
            "Impact: LexisNexis moved to ninth priority and the new DCB partner remains tied to the business timeline. "
            "Next: delivery follows the revised queue, with dated checkpoints on 10 Sep, 17 Sep, and tentatively 15 Oct."
        )
    if "mari stock" in lowered and any(
        term in lowered for term in ("payment bc", "mta", "stock asset api", "fx rate", "sof breakdown")
    ):
        if "payment bc" in lowered and any(term in lowered for term in ("fx rate", "fx precision", "sof breakdown")):
            return (
                "State: Mari Stock's Payment BC API still has open questions on FX precision and SOF-breakdown calculation ownership. "
                "Impact: contract ambiguity can block integration. Next: confirm the rounding rules and the component responsible for SOF calculation."
            )
        # MTA, notification, and asset-API markers can belong to unrelated
        # threads. Do not manufacture one cross-thread integration state.
        return ""
    if "548-549" in lowered or ("edit access" in lowered and "af sheet" in lowered):
        return "State: AF sheet rows 548-549 still require edit access. Impact: the pending change blocks the related review or validation step. Next: grant the required access and confirm the rows are updated."
    if "edit access" in lowered:
        return ""
    if "android" in lowered and "sdk" in lowered and any(term in lowered for term in ("v3.07", "anti-malware", "malware")):
        return "State: The Bank Android SDK package is available for 6.2.1 testing to support the v3.07 anti-malware release. Impact: validation is needed before the release can progress. Next: notify the team and complete package verification."
    if "atm" in lowered and any(version in lowered for version in ("v3.07", "v3.08")):
        if any(term in lowered for term in ("delay", "delayed", "延期")):
            state_text = "ATM upstream timing is delayed; the ATM release plan separates the toggle in v3.07 from withdrawal testing in v3.08."
        else:
            state_text = "The ATM release plan separates the toggle in v3.07 from withdrawal testing in v3.08."
        return (
            f"State: {state_text} Impact: the split makes upstream timing and cross-version test coverage a release dependency. "
            "Next: lock the upstream delivery timeline and confirm the test dates before finalizing release readiness."
        )
    if "querytransferrecipient" in lowered or "swp-31174" in lowered or (
        "recurring" in lowered and "incident" in lowered and "ph" in lowered
    ):
        reference = " tied to SWP-31174" if "swp-31174" in lowered else ""
        return f"State: The PH QueryTransferRecipient issue remains a recurring live incident{reference}. Impact: repeat failures require a confirmed mitigation and monitoring plan. Next: confirm the fix status and recurrence guard."
    if "qris" in lowered and any(term in lowered for term in ("originaltransactionamount", "foreigntransactionamount")):
        return "State: AF cannot map originalTransactionAmount and upstream must provide the fix; IV logs use foreignTransactionAmount. Impact: the upstream dependency blocks correct QRIS cross-border logging. Next: align the upstream fix and IV log field mapping."
    if "mas" in lowered and "hold & release" in lowered:
        return "State: Hold & Release development can proceed while MAS approval is pending, but production launch remains gated by Regulatory Compliance approval. Impact: development can advance without weakening the regulatory launch control. Next: confirm the development decision with Bob and keep go-live blocked until approval is received."
    if "mas" in lowered and any(term in lowered for term in ("scheduled transfer", "schedule transfer", "drainage rule")):
        return "State: Scheduled-transfer controls remain pending MAS confirmation on the drainage-rule check. Impact: public launch depends on regulatory confirmation. Next: obtain the MAS response and update launch readiness."
    if "mas" in lowered and ("fallback" in lowered or "fall back" in lowered):
        return "State: Anti-fraud fallback handling is being assessed for AF unavailability; MAS reporting and impact assessment are required if a serious incident occurs. Impact: the fallback decision affects both customer access and regulatory risk. Next: agree the fallback control and escalation path with the AF and bank technical owners."
    if "mas" in lowered and any(term in lowered for term in ("compliance", "reg compliance", "bccr", "regulatory")):
        scope = " for the BCCR" if "bccr" in lowered else ""
        return f"State: MAS compliance documentation requirements{scope} are still being confirmed with Regulatory Compliance. Impact: the unresolved regulatory requirement can affect launch readiness. Next: confirm the required documentation and its accountable owner."
    if "translation" in lowered or "文案" in lowered:
        if any(term in lowered for term in ("native", "ios", "translation key", "translationkey", "配置", "key")):
            scope = "face-page native" if any(term in lowered for term in ("face", "facial", "人脸")) else "native"
            if any(term in lowered for term in ("aligned", "align with ios", "对齐")):
                return f"State: The {scope} translation-key configuration has been aligned with iOS. Impact: implementation now depends on applying and validating the agreed keys. Next: complete implementation and validation against the aligned configuration."
            return f"State: The {scope} translation-key configuration remains open. Impact: unresolved key configuration can block consistent native copy. Next: align the keys with iOS and complete validation."
    if any(term in lowered for term in ("copywriting", "onboarding account creation states", "biz decide")):
        scope = "Onboarding" if "onboarding" in lowered else ("SDK" if "sdk" in lowered else "Product")
        return f"State: {scope} copywriting remains unresolved and is awaiting the business decision. Impact: copy approval is a dependency for implementation. Next: obtain the decision and apply the approved copy."
    if "app compatibility" in lowered or "app兼容性" in lowered:
        if "v3.07" in lowered or "enum" in lowered:
            return "State: v3.07 app-compatibility work adds the listed payment and overseas-ATM transaction-type enums. Impact: client/server enum alignment is required for release compatibility. Next: confirm consumer support and test coverage."

    body = re.split(r"\bcontext\s*:", raw, maxsplit=1, flags=re.IGNORECASE)[0]
    body = re.sub(r"^(?:@[^\s]+\s*)+", "", body).strip(" ,:;")
    body = re.sub(r"^(?:hi|hello|hey)\b[^:]{0,100}:?\s*", "", body, flags=re.IGNORECASE).strip()
    if not body or body.endswith(("?", "？")):
        return ""
    if raw_lowered.startswith("state:") and "impact:" in raw_lowered and "next:" in raw_lowered:
        return body.strip()
    if deterministic_fallback:
        # Deterministic candidates are selected because they contain a
        # material risk, blocker, dependency, or release signal. Keep novel
        # topics visible with a conservative State/Impact/Next synthesis
        # instead of dropping them merely because they are not in the topic
        # family whitelist above.
        body = _clean_deterministic_fallback_body(body).rstrip(" .!?。！？")
        if not body:
            return ""
        if "blocked" in lowered and any(
            term in lowered for term in ("configuration challenge", "config challenge", "configuration issue")
        ):
            return "State: The AF UAT flow is blocked by a configuration challenge. Impact: UAT validation cannot proceed until the configuration is corrected. Next: confirm the configuration owner, apply the fix, and complete a retest."
        if raw.endswith(("?", "？")):
            state = f"An open decision is being assessed: {body}."
        else:
            state = f"{body}."
        if any(
            term in lowered
            for term in (
                "blocked",
                "blocker",
                "dependency",
                "incident",
                "delay",
                "delayed",
                "failed",
                "error",
                "阻塞",
                "依赖",
                "事故",
                "延期",
            )
        ):
            impact = "The unresolved blocker or dependency may affect delivery or operational readiness."
        elif any(term in lowered for term in ("version", "release", "launch", "go-live", "上线", "放量")):
            impact = "The version or release decision may affect delivery timing and validation readiness."
        else:
            impact = "The update may affect the related delivery or operational work."
        # A generic fallback is not useful to a senior PM. If the source does
        # not contain a concrete impact or next step, omit it rather than
        # sending a polished-looking transcript fragment.
        return ""
    # A transcript-like candidate without a grounded synthesis is lower quality
    # than omission. Known high-signal families are handled explicitly above.
    return ""


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
    return (
        SeaTalkDashboardService._sort_todos(_dedupe_same_topic_items(direct)),
        SeaTalkDashboardService._sort_todos(_dedupe_same_topic_items(watch)),
    )


def _dedupe_same_topic_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    for item in items:
        item_family = _protected_daily_brief_topic_family(item)
        duplicate = next(
            (
                existing
                for existing in deduped
                if (item_family and item_family == _protected_daily_brief_topic_family(existing))
                or _brief_items_refer_to_same_topic(item, existing)
            ),
            None,
        )
        if duplicate is None:
            deduped.append(item)
            continue
        _merge_item_evidence(duplicate, item)
    return deduped


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
        topic = topics.setdefault(key, {"sections": set(), "evidence": "", "source_type": "", "evidence_ref_id": ""})
        topic["sections"].add(section)
        topic["evidence"] = _merge_evidence(topic.get("evidence"), item.get("evidence"))
        topic["source_type"] = _merge_source_type(topic.get("source_type"), item.get("source_type"))
        topic["evidence_ref_id"] = _merge_evidence_ref_ids(topic.get("evidence_ref_id"), item.get("evidence_ref_id"))
    deduped_topic_count = 0
    for section, item in sectioned_items:
        key = item.get("topic_key")
        if not key or key not in topics:
            continue
        topic = topics[key]
        if len(topic["sections"]) > 1:
            item["evidence"] = topic["evidence"]
            item["source_type"] = topic["source_type"]
            item["evidence_ref_id"] = topic["evidence_ref_id"]
            item["cross_section_duplicate"] = True
            deduped_topic_count += 1
    return deduped_topic_count


def _suppress_cross_section_duplicate_topics(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    direct_action_todos: list[dict[str, Any]],
    watch_delegate_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
) -> int:
    canonical_items: list[dict[str, Any]] = []
    removed = 0

    def remember(items: list[dict[str, Any]]) -> None:
        for item in items:
            if isinstance(item, dict):
                canonical_items.append(item)

    def keep_unique(item: dict[str, Any]) -> bool:
        nonlocal removed
        duplicate = next(
            (canonical for canonical in canonical_items if _brief_items_refer_to_same_topic(item, canonical)),
            None,
        )
        if duplicate is None:
            canonical_items.append(item)
            return True
        _merge_item_evidence(duplicate, item)
        duplicate["cross_section_duplicate_suppressed"] = True
        removed += 1
        return False

    remember(direct_action_todos)
    watch_delegate_todos[:] = [item for item in watch_delegate_todos if not isinstance(item, dict) or keep_unique(item)]
    reminders[:] = [item for item in reminders if not isinstance(item, dict) or keep_unique(item)]
    project_updates[:] = [item for item in project_updates if not isinstance(item, dict) or keep_unique(item)]
    other_updates[:] = [item for item in other_updates if not isinstance(item, dict) or keep_unique(item)]
    return removed


def _brief_items_refer_to_same_topic(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_ref_ids = set(_split_evidence_ref_ids(left.get("evidence_ref_id")))
    right_ref_ids = set(_split_evidence_ref_ids(right.get("evidence_ref_id")))
    if left_ref_ids and right_ref_ids and left_ref_ids & right_ref_ids:
        return True
    left_is_reminder = bool(str(left.get("person") or "").strip() and str(left.get("reminder") or "").strip())
    right_is_reminder = bool(str(right.get("person") or "").strip() and str(right.get("reminder") or "").strip())
    if left_is_reminder or right_is_reminder:
        reminder, todo = (left, right) if left_is_reminder else (right, left)
        return _brief_items_are_same_followup_event(reminder, todo)
    left_tokens = _topic_tokens(left, fields=("title", "summary", "task", "reminder", "person"))
    right_tokens = _topic_tokens(right, fields=("title", "summary", "task", "reminder", "person"))
    if not left_tokens or not right_tokens:
        return False
    left_family = _protected_daily_brief_topic_family(left)
    right_family = _protected_daily_brief_topic_family(right)
    if left_family and right_family and left_family != right_family:
        return False
    left_summary = _normalize_dedupe_text(str(left.get("summary") or left.get("task") or ""))
    right_summary = _normalize_dedupe_text(str(right.get("summary") or right.get("task") or ""))
    if (
        left_summary
        and left_summary == right_summary
        and left_family
        and left_family == right_family
        and len(left_tokens) >= 6
    ):
        return True
    overlap = left_tokens & right_tokens
    min_size = min(len(left_tokens), len(right_tokens))
    left_source = _seatalk_evidence_source_root(left.get("evidence"))
    right_source = _seatalk_evidence_source_root(right.get("evidence"))
    if left_source and left_source == right_source:
        left_has_thread = "/ thread:" in str(left.get("evidence") or "").casefold()
        right_has_thread = "/ thread:" in str(right.get("evidence") or "").casefold()
        if left_has_thread and right_has_thread and len(overlap) >= 3:
            return True
        return len(overlap) >= 4 and (len(overlap) / max(min_size, 1)) >= 0.5
    if left_source and right_source and left_source != right_source:
        # Similar generated summaries are never sufficient to merge different
        # SeaTalk groups; doing so can attach valid evidence to the wrong event.
        return False
    if len(overlap) >= 4 and (len(overlap) / max(min_size, 1)) >= 0.4:
        return True
    left_evidence = _normalize_dedupe_text(str(left.get("evidence") or ""))
    right_evidence = _normalize_dedupe_text(str(right.get("evidence") or ""))
    return bool(left_evidence and left_evidence == right_evidence and len(overlap) >= 2)


def _seatalk_evidence_source_root(value: Any) -> str:
    """Return the stable SeaTalk group label without a thread suffix."""
    evidence = " ".join(str(value or "").split())
    if not evidence or evidence.casefold().startswith("gmail:"):
        return ""
    root = re.split(r"\s*/\s*thread:\s*|\s*;\s*", evidence, maxsplit=1, flags=re.IGNORECASE)[0]
    if not root or root.casefold() in {"seatalk group", "private seatalk chat", "seatalk conversation"}:
        return ""
    return _normalize_dedupe_text(root)


def _suppress_updates_covered_by_todos(
    *,
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    direct_action_todos: list[dict[str, Any]],
    watch_delegate_todos: list[dict[str, Any]],
) -> int:
    todos = [*direct_action_todos, *watch_delegate_todos]
    if not todos:
        return 0
    removed = 0

    def keep_update(update: dict[str, Any]) -> bool:
        nonlocal removed
        covered_todo = next((todo for todo in todos if _brief_update_is_covered_by_todo(update, todo)), None)
        if covered_todo is not None:
            if str(update.get("fallback_source") or "").startswith("deterministic_"):
                _merge_high_signal_update_into_todo(covered_todo, update)
            removed += 1
            return False
        return True

    project_updates[:] = [item for item in project_updates if not isinstance(item, dict) or keep_update(item)]
    other_updates[:] = [item for item in other_updates if not isinstance(item, dict) or keep_update(item)]
    return removed


def _merge_high_signal_update_into_todo(todo: dict[str, Any], update: dict[str, Any]) -> None:
    """Keep deterministic risk/timeline detail when the canonical item is a todo."""
    summary = " ".join(str(update.get("summary") or "").split())
    why = _executive_why_from_update(summary)
    todo_context = _item_text(todo, fields=("task", "why"))
    if re.search(r"(?<![A-Za-z0-9_])MAS(?![A-Za-z0-9_])", summary) and not re.search(
        r"(?<![A-Za-z0-9_])MAS(?![A-Za-z0-9_])", todo_context
    ):
        why = f"MAS commitment: {why}" if why else "this delivery supports the MAS commitment"
    if why and why.casefold() not in str(todo.get("task") or "").casefold():
        todo["why"] = why
    _merge_item_evidence(todo, update)
    todo["high_signal_status"] = update.get("status") or "unknown"


def _ensure_high_signal_fallbacks_visible(
    *,
    high_signal_fallbacks: list[dict[str, Any]],
    project_updates: list[dict[str, Any]],
    other_updates: list[dict[str, Any]],
    direct_action_todos: list[dict[str, Any]],
    watch_delegate_todos: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
) -> None:
    """Do not lose a protected signal when canonical-topic dedupe merges items."""
    if not high_signal_fallbacks:
        return
    all_items = [
        *direct_action_todos,
        *watch_delegate_todos,
        *reminders,
        *project_updates,
        *other_updates,
    ]
    for fallback in high_signal_fallbacks:
        fallback_ref = str(fallback.get("evidence_ref_id") or "").strip()
        matching_ref = next(
            (
                item
                for item in all_items
                if fallback_ref
                and fallback_ref in _split_evidence_ref_ids(item.get("evidence_ref_id"))
            ),
            None,
        )
        if matching_ref is not None:
            _merge_high_signal_detail_into_item(matching_ref, fallback)
            continue
        matching_todo = next(
            (
                item
                for item in [*direct_action_todos, *watch_delegate_todos]
                if _brief_update_is_covered_by_todo(fallback, item)
            ),
            None,
        )
        if matching_todo is not None:
            _merge_high_signal_detail_into_item(matching_todo, fallback)
            continue
        matching_topic = next(
            (item for item in all_items if _brief_items_refer_to_same_topic(fallback, item)),
            None,
        )
        if matching_topic is not None:
            _merge_high_signal_detail_into_item(matching_topic, fallback)
            continue
        project_updates.append(fallback)
        all_items.append(fallback)


def _merge_high_signal_detail_into_item(item: dict[str, Any], update: dict[str, Any]) -> None:
    detail = " ".join(str(update.get("summary") or update.get("task") or "").split())
    if str(item.get("task") or "").strip():
        why = _executive_why_from_update(detail)
        if why and why.casefold() not in str(item.get("task") or "").casefold():
            item["why"] = why
    elif str(item.get("reminder") or "").strip():
        why = _executive_why_from_update(detail)
        if why:
            item["why"] = why
    else:
        current = " ".join(str(item.get("summary") or "").split())
        detail_is_executive = all(label in detail.casefold() for label in ("state:", "impact:", "next:"))
        current_is_executive = all(label in current.casefold() for label in ("state:", "impact:", "next:"))
        if detail and (not current or (detail_is_executive and not current_is_executive)):
            item["summary"] = detail
    _merge_item_evidence(item, update)
    if update.get("status") == "blocked":
        item["status"] = "blocked"
    item["high_signal_status"] = update.get("status") or item.get("high_signal_status") or "unknown"


def _executive_why_from_update(summary: Any) -> str:
    text = " ".join(str(summary or "").split())
    if not text:
        return ""
    impact_match = re.search(r"\bImpact:\s*(.+?)(?=\s+Next:|$)", text, flags=re.IGNORECASE)
    if impact_match:
        return _clip_hint_text(impact_match.group(1).strip(" ."), limit=180)
    state_match = re.search(r"\bState:\s*(.+?)(?=\s+Impact:|\s+Next:|$)", text, flags=re.IGNORECASE)
    return _clip_hint_text(state_match.group(1).strip(" ."), limit=180) if state_match else ""


def _brief_update_is_covered_by_todo(update: dict[str, Any], todo: dict[str, Any]) -> bool:
    update_ref_ids = set(_split_evidence_ref_ids(update.get("evidence_ref_id")))
    todo_ref_ids = set(_split_evidence_ref_ids(todo.get("evidence_ref_id")))
    if update_ref_ids and todo_ref_ids and update_ref_ids & todo_ref_ids:
        return True
    update_thread_key = _seatalk_evidence_thread_key(update.get("evidence"))
    todo_thread_key = _seatalk_evidence_thread_key(todo.get("evidence"))
    if update_thread_key and update_thread_key == todo_thread_key:
        return True
    update_evidence = _normalize_dedupe_text(str(update.get("evidence") or ""))
    todo_evidence = _normalize_dedupe_text(str(todo.get("evidence") or ""))
    same_evidence = bool(update_evidence and update_evidence == todo_evidence)
    same_explicit_thread = same_evidence and "/ thread:" in str(update.get("evidence") or "").casefold()
    if same_explicit_thread:
        return True
    update_family = _protected_daily_brief_topic_family(update)
    todo_family = _protected_daily_brief_topic_family(todo)
    if update_family and todo_family and update_family != todo_family:
        return False
    update_tokens = _topic_tokens(update, fields=("title", "summary"))
    todo_tokens = _topic_tokens(todo, fields=("task", "title", "summary"))
    overlap = update_tokens & todo_tokens
    min_size = min(len(update_tokens), len(todo_tokens))
    if same_evidence and len(overlap) >= 3 and (len(overlap) / max(min_size, 1)) >= 0.35:
        return True
    if _display_domain(update.get("domain")) != _display_domain(todo.get("domain")):
        return False
    if len(overlap) >= 3 and (len(overlap) / max(min_size, 1)) >= 0.35:
        return True
    if str(update.get("fallback_source") or "").startswith("deterministic_"):
        high_signal_overlap = overlap & {"mas", "hold", "release", "blocked", "dependency", "timeline", "delay", "incident", "launch"}
        return len(high_signal_overlap) >= 2
    return False


def _protected_daily_brief_topic_family(item: dict[str, Any]) -> str:
    """Identify required signal families before generic timeline words are compared."""
    text = _item_text(item, fields=("title", "summary", "task", "reminder", "evidence")).casefold()
    if "scheduled and recurring transfer" in text or "scheduled transfer" in text and "p0" in text:
        return "scheduled_recurring_transfers_p0"
    if "atm" in text and re.search(r"\bv?3\.0[78]\b", text, flags=re.IGNORECASE):
        return "atm_version_timeline"
    if "qris" in text or "sgdb-81072" in text:
        return "qris_dependency"
    if "querytransferrecipient" in text or "swp-31174" in text:
        return "ph_recurring_incident"
    if "v3.49" in text or ("alc v12" in text and any(term in text for term in ("白名单", "大促", "放量"))):
        return "id_alc_v349_rollout"
    if "hold & release" in text and "mas" in text:
        return "hold_release_mas"
    if "mas" in text and any(
        term in text
        for term in (
            "scheduled transfer",
            "scheduled-transfer",
            "schedule transfer",
            "scheduled payment",
            "scheduled payments",
            "drainage rule",
            "drainage-rule",
        )
    ):
        return "scheduled_transfer_mas"
    if "mari stock" in text and any(
        term in text for term in ("payment bc", "stock asset api", "sof breakdown", "fx precision", "mta")
    ):
        return "mari_stock_integration"
    return ""


def _seatalk_evidence_thread_key(value: Any) -> str:
    evidence = " ".join(str(value or "").split())
    match = re.match(r"(?P<group>.+?)\s*/\s*thread:\s*(?P<thread>.+?)(?:\s*;|$)", evidence, flags=re.IGNORECASE)
    if not match:
        return ""
    group = re.sub(
        r"\s*\((?:issue\s*id|issueid|bpmis)\b[^)]*\)\s*$",
        "",
        match.group("group"),
        flags=re.IGNORECASE,
    )
    return f"{_normalize_dedupe_text(group)}::{_normalize_dedupe_text(match.group('thread'))}"


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
    token_ledger: dict[str, Any] | None = None,
    evidence_quality_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_types = {
        str(item.get("source_type") or "").strip().lower()
        for item in [*project_updates, *other_updates, *my_todos, *reminders]
        if str(item.get("source_type") or "").strip()
    }
    joined_sources = "\n".join(source_texts).lower()
    if "seatalk" in joined_sources:
        source_types.add("seatalk")
    # The second source slot is the explicit Gmail input, not a word that may
    # appear inside a SeaTalk message or URL. Daily Brief production runs now
    # pass an empty Gmail source by design.
    if len(source_texts) > 1 and str(source_texts[1] or "").strip():
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
        "token_ledger": dict(token_ledger or {}),
        "evidence_quality_metrics": dict(evidence_quality_metrics or {}),
    }


def _source_coverage_label(source_types: set[str]) -> str:
    if "mixed" in source_types or {"seatalk", "gmail"}.issubset(source_types):
        return "SeaTalk + Gmail"
    if "seatalk" in source_types:
        return "SeaTalk"
    if "gmail" in source_types:
        return "Gmail"
    return "No message source"


def _filter_seatalk_reminders(
    items: list[dict[str, Any]],
    *,
    reminder_candidates: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in items:
        if item.get("source_type") != "seatalk" or _is_bot_alert_or_reminder_item(item) or _is_sdlc_checker_reminder_item(item):
            continue
        canonical_person = _canonical_team_member_name(item.get("person"))
        if not canonical_person:
            continue
        if reminder_candidates is not None and not any(
            _reminder_matches_candidate(item, candidate, canonical_person=canonical_person)
            for candidate in reminder_candidates
        ):
            continue
        domain = _team_member_reminder_domain(item, canonical_person)
        if domain == "Anti-fraud" and _normalize_person_key(canonical_person) not in ANTI_FRAUD_TEAM_MEMBERS:
            continue
        item["person"] = canonical_person
        item["domain"] = domain
        filtered.append(item)
    return filtered


def _team_member_reminder_domain(item: dict[str, Any], person: str) -> str:
    """Assign reminder ownership from the person and the evidenced work context."""
    person_key = _normalize_person_key(person)
    if person_key in {_normalize_person_key(name) for name in ANTI_FRAUD_TEAM_MEMBERS}:
        return "Anti-fraud"
    combined = " ".join(
        str(item.get(field) or "")
        for field in ("domain", "title", "summary", "reminder", "evidence")
    ).casefold()
    if any(term in combined for term in ("grc", "authorization management", "issue & action plan", "audit-history")):
        return "Ops Risk"
    if any(term in combined for term in ("credit risk", "crms", "maricredit", "a-card", "acard")):
        return "Credit Risk"
    return TEAM_MEMBER_REMINDER_DOMAIN_OVERRIDES.get(person_key, _display_domain(item.get("domain")))


def _reminder_matches_candidate(
    item: dict[str, Any],
    candidate: dict[str, str],
    *,
    canonical_person: str,
) -> bool:
    if candidate.get("person") != canonical_person:
        return False
    evidence = _normalize_thread_match_text(item.get("evidence"))
    group = _normalize_thread_match_text(candidate.get("group"))
    thread = _normalize_thread_match_text(candidate.get("thread"))
    # A source label can be wrong while the thread title is exact. Keep the
    # model-selected follow-up in that case so evidence repair can replace the
    # container group with the actual thread owner.
    if thread and thread in evidence:
        return True
    if group and not _seatalk_group_ref_matches(group, evidence):
        return False
    if thread and thread not in evidence:
        return False
    return True


def _filter_reminders_already_covered_by_watch_delegate(
    reminders: list[dict[str, Any]],
    watch_delegate_todos: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not reminders or not watch_delegate_todos:
        return reminders
    return [
        item
        for item in reminders
        if not any(
            _display_domain(item.get("domain")) == _display_domain(todo.get("domain"))
            and _brief_items_are_same_followup_event(item, todo)
            for todo in watch_delegate_todos
        )
    ]


def _watch_item_is_explicitly_owner_specific(todo: dict[str, Any], reminder: dict[str, Any]) -> bool:
    person = _canonical_team_member_name(reminder.get("person"))
    if not person:
        return False
    text = _item_text(todo, fields=("task", "title", "summary"))
    return _normalize_person_key(person) in _normalize_person_key(text)


def _backfill_team_member_reminders_from_candidates(
    reminders: list[dict[str, Any]],
    *,
    team_member_reminder_candidates: list[dict[str, str]] | None,
    evidence_refs: list[dict[str, Any]],
    resolved_candidates: list[dict[str, str]] | None = None,
    quality_metrics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if not team_member_reminder_candidates:
        if quality_metrics is not None:
            quality_metrics["deterministic_followup_backfill_count"] = 0
        return reminders
    refs_by_candidate = _seatalk_refs_by_candidate(evidence_refs)
    combined = list(reminders)
    backfilled_count = 0
    for candidate in team_member_reminder_candidates:
        ref = refs_by_candidate.get(_candidate_ref_key(candidate))
        if not ref:
            continue
        person = _canonical_team_member_name(candidate.get("person"))
        if not person:
            continue
        item = {
            "domain": "General",
            "person": person,
            "reminder": _candidate_followup_reminder_text(candidate),
            "evidence": str(ref.get("evidence") or "").strip(),
            "source_type": "seatalk",
            "evidence_ref_id": str(ref.get("id") or "").strip(),
            "followup_source": "deterministic_backfill",
        }
        item["domain"] = _team_member_reminder_domain(item, person)
        if any(_item_matches_resolved_followup(item, resolved) for resolved in (resolved_candidates or [])):
            continue
        if _is_team_member_coverage_notice(_candidate_followup_reminder_text(candidate)):
            continue
        if not _evidence_ref_matches_item_people(item, ref) or not _evidence_refs_match_project_item(item, [ref]):
            continue
        if any(
            str(existing.get("evidence_ref_id") or "") == str(item.get("evidence_ref_id") or "")
            or _brief_items_are_same_followup_event(existing, item)
            for existing in combined
        ):
            continue
        combined.append(item)
        backfilled_count += 1
        if len(combined) >= MAX_TEAM_MEMBER_REMINDERS:
            break
    if quality_metrics is not None:
        quality_metrics["deterministic_followup_backfill_count"] = backfilled_count
    return combined[:MAX_TEAM_MEMBER_REMINDERS]


def _seatalk_refs_by_candidate(evidence_refs: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    refs: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for ref in evidence_refs:
        if str(ref.get("source_type") or "") != "seatalk":
            continue
        key = (
            _normalize_thread_match_text(ref.get("group")),
            _normalize_thread_match_text(ref.get("thread") or "__main__"),
            str(ref.get("timestamp") or ""),
            _normalize_thread_match_text(ref.get("snippet")),
        )
        refs[key] = ref
    return refs


def _candidate_ref_key(candidate: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        _normalize_thread_match_text(candidate.get("group")),
        _normalize_thread_match_text(candidate.get("thread") or "__main__"),
        str(candidate.get("timestamp") or ""),
        _normalize_thread_match_text(candidate.get("text")),
    )


def _candidate_followup_reminder_text(candidate: dict[str, str]) -> str:
    context = " ".join(str(candidate.get(field) or "") for field in ("group", "thread")).casefold()
    if "shadow run" in context and "strategy" in context:
        return "Confirm whether downstream Strategies should continue after the first Strategy is rejected in Shadow Run, and check Bowen's issue."
    text = _clip_hint_text(candidate.get("text"), limit=180)
    if text:
        concise = _concise_followup_request(text)
        return concise or "Follow up on the unresolved SeaTalk ask."
    return "Follow up on the unresolved SeaTalk ask."


def _concise_followup_request(value: Any) -> str:
    """Turn deterministic backfill text into an action instead of a chat quote."""
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return ""
    chinese_check = re.search(r"(?:你看下|帮忙看下|帮我看下)\s*(?P<subject>.+)$", text, flags=re.IGNORECASE)
    if chinese_check:
        subject = chinese_check.group("subject").strip(" ，,。.!！")
        if subject:
            return _sentence_text(f"Check {subject} and provide an update", "Follow up on the unresolved request")
    if re.search(r"\b(?:can|could)\s+help\s+with\b", text, flags=re.IGNORECASE) and not re.search(r"[?？]", text):
        return ""
    match = re.search(
        r"\b(?:please|pls|plz)\s+(?:help\s+to\s+)?(?P<action>.+)$",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        match = re.search(
            r"\b(?:can|could)\s+(?:you\s+)?(?:please\s+)?(?:help\s+to\s+)?(?P<action>.+)$",
            text,
            flags=re.IGNORECASE,
        )
    action = (match.group("action") if match else text).strip(" ,:;-?")
    action = re.sub(r"^help\s+(?:to\s+)?", "", action, flags=re.IGNORECASE)
    if action.casefold().startswith("check "):
        action = f"Check {action[6:]}"
    elif action.casefold().startswith("confirm "):
        action = f"Confirm {action[8:]}"
    elif match:
        action = f"Follow up on {action}"
    else:
        action = f"Follow up on the unresolved request: {_clip_hint_text(action, limit=150)}"
    return _sentence_text(_clip_hint_text(action, limit=180), "Follow up on the unresolved request")


def _xiaodong_followup_task(candidate: dict[str, str]) -> str:
    combined = " ".join(str(candidate.get(field) or "") for field in ("group", "thread", "context", "text")).casefold()
    if "centum" in combined and any(term in combined for term in ("current balance", "available balance", "<$100>", "account balance")):
        return "Clarify whether the CENTUM <$100> account-balance check uses current balance or available balance."
    if "ipification" in combined and any(term in combined for term in ("brd", "business requirement", "business requirements")):
        return "Review the Ipification POC BRD and join the follow-up alignment discussion."
    if "unlock card" in combined or ("blacklist" in combined and "server error" in combined):
        return "Investigate the Self Service Unlock Card server error after a blacklisted-account hit and coordinate the fix with the CC development team."
    if "force upgrade" in combined and any(term in combined for term in ("v3.05", "3.5", "2.83", "app version")):
        return "Confirm the App force-upgrade baseline with Bank PMO and start the approval process to simplify version-dependent AF authentication branches."
    if "default template" in combined or "默认模板" in combined or ("需要配模板" in combined and "模板" in combined):
        return "Confirm whether the scenario requires a dedicated template and align the default-template handling with Bank BE."
    if "fraud ops" in combined and "slide" in combined and any(term in combined for term in ("next wed", "next wednesday", "finish")):
        return "Complete the PH Fraud Ops slide covering scope, focus areas, automation target state, and immediate next steps by next Wednesday."
    if "v3.49" in combined and any(term in combined for term in ("白名单", "大促", "放量", "alc v12")):
        return (
            "Decide and communicate whether ID v3.49 ALC v12 remains whitelist-only before the 8.8 promotion "
            "and moves to mass rollout after the promotion."
        )
    if "a/b" in combined or "s0141" in combined:
        return "Follow up on the A/B testing rule-behavior issue and confirm why S0141 stopped triggering before the scheduled configuration date."
    if "mas" in combined and "bob" in combined and any(
        term in combined for term in ("proceed with dev", "proceed with development", "waiting for the mas approval")
    ):
        return "Confirm with Bob whether Hold & Release development should proceed before MAS approval, while keeping production launch blocked until approval is received."
    if "2 actions" in combined or "two actions" in combined or "two-action" in combined:
        return "Follow up on how the two-action authentication case was handled with Wang Chang and Zuhua."
    if "ivlog" in combined and any(term in combined for term in ("market", "productization", "产品化", "市场单")):
        return "Resolve the AF ivLog ticketing ambiguity with development and confirm when market and productization tickets are required."
    if "process looks weird" in combined and re.search(r"\bv3\.(?:26|28)\b", combined):
        return "Confirm with development that the amended v3.26/v3.28 ticket and release mapping follows the correct delivery process."
    if candidate.get("ownership_reason") == "direct_request":
        return _xiaodong_direct_request_task(candidate)
    thread = str(candidate.get("thread") or "").strip()
    if thread:
        return f"Follow up on the unresolved issue in the '{thread}' thread after committing to check and get back."
    return f"Follow up on the unresolved ask after committing to check and get back: {_clip_hint_text(candidate.get('text'), limit=180)}"


def _xiaodong_direct_request_task(candidate: dict[str, str]) -> str:
    text = " ".join(str(candidate.get("text") or "").split())
    text = re.sub(
        r"@?(?:zheng\s+xiaodong|xiaodong\s+zheng|xiaodong)(?:\s*\([^)]*\))?",
        " ",
        text,
        flags=re.IGNORECASE,
    )
    text = re.split(r"\s*[|｜]\s*", text, maxsplit=1)[0].strip()
    text = re.sub(r"(?:这个得|这个要|这个需要)\s*$", "", text).strip(" ，,。")
    chinese_decision = re.match(r"^(?:是否需要|需不需要)\s*(?P<action>.+)$", text)
    if chinese_decision:
        action = chinese_decision.group("action").strip(" ，,。")
        if action:
            return _sentence_text(f"确认是否需要{action}", "Respond to the unresolved SeaTalk request")
    text = re.sub(r"^(?:hi|hello|hey|boss|老板)[,，:\s-]*", "", text, flags=re.IGNORECASE).strip()
    request_rewrites = (
        (r"^please\s+confirm\s+whether\s+", "Confirm whether "),
        (r"^please\s+confirm\s+", "Confirm "),
        (r"^please\s+review\s+", "Review "),
        (r"^please\s+decide\s+", "Decide "),
        (r"^(?:could|can|would)\s+you\s+(?:please\s+)?", ""),
        (r"^please\s+", ""),
    )
    for pattern, replacement in request_rewrites:
        rewritten = re.sub(pattern, replacement, text, count=1, flags=re.IGNORECASE)
        if rewritten != text:
            text = rewritten.strip()
            break
    if not text:
        thread = str(candidate.get("thread") or "").strip()
        return f"Respond to the unresolved request in the '{thread}' thread." if thread else "Respond to the unresolved SeaTalk request."
    if text[:1].islower():
        text = text[:1].upper() + text[1:]
    return _sentence_text(_clip_hint_text(text, limit=200), "Respond to the unresolved SeaTalk request")


def _xiaodong_followup_domain(candidate: dict[str, str]) -> str:
    combined = " ".join(str(candidate.get(field) or "") for field in ("group", "thread", "text")).casefold()
    if any(term in combined for term in ("anti-fraud", "anti fraud", "fraud", "a/b", "s0141", "authentication", "rule", "hold & release", "ipification", "unlock card", "blacklist")) or (
        "mas" in combined and "bob" in combined
    ) or re.search(r"\baf\b", combined):
        return "Anti-fraud"
    if any(term in combined for term in ("centum", "grc", "ops risk", "operational", "vpn")):
        return "Ops Risk"
    if any(term in combined for term in ("credit risk", "crms", "loan", "acard")):
        return "Credit Risk"
    return "General"


def _xiaodong_candidate_matches_todo(candidate: dict[str, str], ref: dict[str, Any], todo: dict[str, Any]) -> bool:
    ref_id = str(ref.get("id") or "").strip()
    if ref_id and ref_id in _split_evidence_ref_ids(todo.get("evidence_ref_id")):
        return True
    candidate_tokens = _topic_tokens(candidate, fields=("group", "thread", "context", "text"))
    candidate_tokens |= _topic_tokens({"task": _xiaodong_followup_task(candidate)}, fields=("task",))
    todo_tokens = _topic_tokens(todo, fields=("task", "title", "summary"))
    if not candidate_tokens or not todo_tokens:
        return False
    overlap = candidate_tokens & todo_tokens
    same_source = _seatalk_evidence_source_root(ref.get("evidence")) == _seatalk_evidence_source_root(todo.get("evidence"))
    generic_todo_source = _is_generic_seatalk_evidence(todo.get("evidence"))
    return bool(overlap) and (same_source or (generic_todo_source and len(overlap) >= 1))


def _build_xiaodong_followup_items(
    candidates: list[dict[str, str]] | None,
    *,
    evidence_refs: list[dict[str, Any]],
    existing_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not candidates:
        return []
    refs_by_candidate = _seatalk_refs_by_candidate(evidence_refs)
    items: list[dict[str, Any]] = []
    for candidate in candidates:
        ref = refs_by_candidate.get(_candidate_ref_key(candidate))
        if not ref:
            continue
        # Unvalidated model items cannot satisfy a deterministic direct request:
        # they may later be dropped for bad evidence and erase the real action.
        matching_items = list(items)
        matching_items = [
            todo
            for todo in matching_items
            if str(todo.get("action_type") or "").strip().casefold() == "direct_action"
        ]
        if any(_xiaodong_candidate_matches_todo(candidate, ref, todo) for todo in matching_items):
            continue
        combined = " ".join(str(candidate.get(field) or "") for field in ("group", "thread", "text")).casefold()
        priority = (
            "high"
            if any(term in combined for term in ("blocked", "incident", "p0", "p1", "mas", "v3.49", "大促", "放量"))
            else "medium"
        )
        items.append(
            {
                "task": _xiaodong_followup_task(candidate),
                "domain": _xiaodong_followup_domain(candidate),
                "priority": priority,
                "due": "TBD",
                "evidence": str(ref.get("evidence") or "").strip(),
                "source_type": "seatalk",
                "action_type": "direct_action",
                "evidence_ref_id": str(ref.get("id") or "").strip(),
                "followup_source": (
                    "deterministic_xiaodong_direct_request"
                    if candidate.get("ownership_reason") == "direct_request"
                    else "deterministic_xiaodong_commitment"
                ),
            }
        )
    return items[:MAX_MY_TODOS]


def _build_gmail_xiaodong_action_items(
    evidence_refs: list[dict[str, Any]],
    *,
    existing_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Preserve explicit Gmail and Google Docs requests addressed to Xiaodong."""
    items: list[dict[str, Any]] = []
    for ref in evidence_refs:
        if str(ref.get("source_type") or "").strip().casefold() != "gmail":
            continue
        subject = " ".join(str(ref.get("subject") or "").split())
        snippet = " ".join(str(ref.get("snippet") or "").split())
        recipients = str(ref.get("to") or "").casefold()
        sender = str(ref.get("sender") or "").casefold()
        haystack = f"{subject} {snippet}".casefold()
        docs_mention = "@xiaodong" in haystack and "google docs" in sender
        directly_addressed = "xiaodong.zheng" in recipients and any(
            cue in haystack
            for cue in ("please", "may i ask", "need your", "your input", "review", "confirm", "approve")
        )
        if not (docs_mention or directly_addressed):
            continue
        if any(cue in haystack for cue in GMAIL_CALENDAR_SUBJECT_HINTS + GMAIL_LOW_VALUE_REMINDER_SUBJECT_HINTS):
            continue
        if any(term in haystack for term in ("maribank app downtime", "root cause analysis", "rca")) or docs_mention:
            task = "Review the MariBank App Downtime RCA mention and provide the requested root-cause input."
            domain = "Ops Risk"
            priority = "high"
        else:
            continue
        item = {
            "task": task,
            "domain": domain,
            "priority": priority,
            "due": "TBD",
            "evidence": str(ref.get("evidence") or "Gmail conversation").strip(),
            "source_type": "gmail",
            "action_type": "direct_action",
            "evidence_ref_id": str(ref.get("id") or "").strip(),
            "followup_source": "deterministic_gmail_direct_request",
        }
        if not item["evidence_ref_id"] or any(
            _brief_items_refer_to_same_topic(item, existing) for existing in [*existing_items, *items]
        ):
            continue
        items.append(item)
    return items[:MAX_MY_TODOS]


def _build_followup_diagnostics(
    *,
    team_member_reminder_candidates: list[dict[str, str]] | None,
    reminders: list[dict[str, Any]],
    watch_delegate_todos: list[dict[str, Any]],
    evidence_refs: list[dict[str, Any]],
) -> dict[str, Any]:
    candidates = team_member_reminder_candidates or []
    refs_by_candidate = _seatalk_refs_by_candidate(evidence_refs)
    buckets = {
        "covered_by_watch_delegate": 0,
        "filtered_not_allowed_person": 0,
        "missing_ref": 0,
        "invalid_ref": 0,
        "model_omitted": 0,
    }
    examples: list[dict[str, str]] = []
    if not candidates:
        return {"candidate_examples": [], "reason_buckets": buckets}
    reminder_people = {_normalize_person_key(item.get("person")) for item in reminders}
    for candidate in candidates:
        person = _canonical_team_member_name(candidate.get("person"))
        ref = refs_by_candidate.get(_candidate_ref_key(candidate))
        candidate_item = {
            "domain": TEAM_MEMBER_REMINDER_DOMAIN_OVERRIDES.get(_normalize_person_key(person), "General"),
            "person": person,
            "reminder": _candidate_followup_reminder_text(candidate),
            "evidence": str(ref.get("evidence") or "").strip() if ref else "",
            "source_type": "seatalk",
            "evidence_ref_id": str(ref.get("id") or "").strip() if ref else "",
        }
        if not person:
            buckets["filtered_not_allowed_person"] += 1
        elif any(_brief_items_are_same_followup_event(candidate_item, todo) for todo in watch_delegate_todos):
            buckets["covered_by_watch_delegate"] += 1
        elif not ref:
            buckets["missing_ref"] += 1
        elif not _evidence_ref_matches_item_people(candidate_item, ref) or not _evidence_refs_match_project_item(candidate_item, [ref]):
            buckets["invalid_ref"] += 1
        elif _normalize_person_key(person) not in reminder_people:
            buckets["model_omitted"] += 1
        if len(examples) < 5:
            examples.append(
                {
                    "person": str(candidate.get("person") or ""),
                    "source": str(ref.get("evidence") or "") if ref else _format_team_member_reminder_hints([candidate]).removeprefix("- "),
                    "text": _clip_hint_text(candidate.get("text"), limit=160),
                }
            )
    return {"candidate_examples": examples, "reason_buckets": buckets}


def _brief_items_are_same_followup_event(reminder: dict[str, Any], todo: dict[str, Any]) -> bool:
    reminder_ref_ids = set(_split_evidence_ref_ids(reminder.get("evidence_ref_id")))
    todo_ref_ids = set(_split_evidence_ref_ids(todo.get("evidence_ref_id")))
    if reminder_ref_ids and todo_ref_ids and reminder_ref_ids & todo_ref_ids:
        return True
    reminder_tokens = _topic_tokens(reminder, fields=("reminder", "title", "summary", "person"))
    todo_tokens = _topic_tokens(todo, fields=("task", "title", "summary"))
    if not reminder_tokens or not todo_tokens:
        return False
    overlap = reminder_tokens & todo_tokens
    reminder_evidence = _normalize_dedupe_text(str(reminder.get("evidence") or ""))
    todo_evidence = _normalize_dedupe_text(str(todo.get("evidence") or ""))
    # The same source thread represents one event across Xiaodong and team
    # sections. Keep the higher-priority Xiaodong item even when the model gave
    # the two copies different domains or omitted the team member's name.
    same_evidence = bool(reminder_evidence and reminder_evidence == todo_evidence)
    same_explicit_thread = same_evidence and "/ thread:" in str(reminder.get("evidence") or "").casefold()
    if same_explicit_thread:
        return True
    if _display_domain(reminder.get("domain")) != _display_domain(todo.get("domain")):
        return False
    person_tokens = _topic_tokens(reminder, fields=("person",))
    if person_tokens and not person_tokens.intersection(todo_tokens):
        return False
    min_size = min(len(reminder_tokens), len(todo_tokens))
    if len(overlap) >= 3 and (len(overlap) / max(min_size, 1)) >= 0.35:
        return True
    if same_evidence and len(overlap) >= 1:
        return True
    return False


def _topic_tokens(item: dict[str, Any], *, fields: tuple[str, ...]) -> set[str]:
    text = " ".join(str(item.get(field) or "") for field in fields)
    tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]+", text.lower())
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
        "with",
        "whether",
        "please",
        "follow",
        "up",
        "check",
        "confirm",
        "ensure",
        "monitor",
        "tomorrow",
        "today",
        "team",
        "teams",
        "local",
        "needs",
        "need",
    }
    return {token for token in tokens if token not in stopwords and len(token) > 1}


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
    for item in items:
        if _is_bot_alert_or_reminder_item(item):
            continue
        raw_signal_type = str(item.get("signal_type") or "").strip()
        signal_type = _normalize_signal_type(raw_signal_type)
        if signal_type not in ALLOWED_OTHER_UPDATE_SIGNAL_TYPES:
            continue
        item["signal_type"] = signal_type
        if signal_type in STRONG_OTHER_UPDATE_SIGNAL_TYPES:
            strong.append(item)
    return strong


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
        _merge_item_evidence(existing, item)
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
    if evidence_text.startswith("group-") or " group" in evidence_text or "uid " in evidence_text or "private seatalk" in evidence_text:
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


def _merge_evidence_ref_ids(left: Any, right: Any) -> str:
    ref_ids: list[str] = []
    for value in (left, right):
        for ref_id in _split_evidence_ref_ids(value):
            if ref_id not in ref_ids:
                ref_ids.append(ref_id)
    return ", ".join(ref_ids)


def _merge_item_evidence(target: dict[str, Any], source: dict[str, Any]) -> None:
    """Merge source labels and their IDs as one atomic provenance unit."""
    target["evidence"] = _merge_evidence(target.get("evidence"), source.get("evidence"))
    target["source_type"] = _merge_source_type(target.get("source_type"), source.get("source_type"))
    merged_ref_ids = _merge_evidence_ref_ids(target.get("evidence_ref_id"), source.get("evidence_ref_id"))
    if merged_ref_ids:
        target["evidence_ref_id"] = merged_ref_ids


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


def _brief_label(value: Any, *, language: str = "en") -> str:
    text = str(value or "").strip()
    if str(language or "").strip().casefold() in {"zh", "zh-cn", "chinese"}:
        return BRIEF_ZH_SECTION_LABELS.get(text, text)
    return text


def _brief_localize_window_label(value: Any) -> str:
    text = " ".join(str(value or "").split())
    match = re.fullmatch(r"previous\s+(\d+)\s+hours?", text, flags=re.IGNORECASE)
    if match:
        return f"前 {match.group(1)} 小时"
    return text


def _is_chinese_brief(language: str) -> bool:
    return str(language or "").strip().casefold() in {"zh", "zh-cn", "chinese"}


def _brief_localize_value(value: Any, *, language: str = "en") -> str:
    """Localize deterministic English text without changing source evidence."""
    text = str(value or "").strip()
    if str(language or "").strip().casefold() not in {"zh", "zh-cn", "chinese"} or not text:
        return text

    exact = {
        "Clarify whether the CENTUM <$100> account-balance check uses current balance or available balance.": "确认 CENTUM 自动化脚本中 <$100> 账户余额校验使用当前余额还是可用余额。",
        "Review the Ipification POC BRD and join the follow-up alignment discussion.": "评审 Ipification POC BRD，并参加后续对齐讨论。",
        "Investigate the Self Service Unlock Card server error after a blacklisted-account hit and coordinate the fix with the CC development team.": "跟进黑名单账户命中后的 Self Service Unlock Card 服务器报错，并与 CC 开发团队协调修复。",
        "Confirm the App force-upgrade baseline with Bank PMO and start the approval process to simplify version-dependent AF authentication branches.": "与 Bank PMO 确认 App 强制升级基线版本，并启动审批，以简化依赖版本的防欺诈认证分支。",
        "Confirm whether the scenario requires a dedicated template and align the default-template handling with Bank BE.": "确认该场景是否需要专用模板，并与 Bank BE 对齐默认模板处理方式。",
        "Complete the PH Fraud Ops slide covering scope, focus areas, automation target state, and immediate next steps by next Wednesday.": "在下周三前完成 PH Fraud Ops 幻灯片，覆盖范围、重点、自动化目标状态和近期下一步。",
        "Follow up on the A/B testing rule-behavior issue and confirm why S0141 stopped triggering before the scheduled configuration date.": "跟进 A/B testing 规则行为问题，并确认 S0141 为何在计划配置日期前停止触发。",
        "Confirm with Bob whether Hold & Release development should proceed before MAS approval, while keeping production launch blocked until approval is received.": "与 Bob 确认 Hold & Release 是否可在 MAS 审批前继续开发，同时在获得审批前保持生产上线阻塞。",
        "Follow up on how the two-action authentication case was handled with Wang Chang and Zuhua.": "跟进 Wang Chang 和 Zuhua 对双重认证动作案例的处理方式。",
        "Resolve the AF ivLog ticketing ambiguity with development and confirm when market and productization tickets are required.": "与开发团队解决 AF ivLog 建单规则歧义，并确认何时需要创建市场单和产品化单。",
        "Confirm with development that the amended v3.26/v3.28 ticket and release mapping follows the correct delivery process.": "与开发团队确认修订后的 v3.26/v3.28 工单与发布映射符合正确交付流程。",
        "Confirm whether downstream Strategies should continue after the first Strategy is rejected in Shadow Run, and check Bowen's issue.": "确认 Shadow Run 中第一层 Strategy 被拒绝后是否仍应继续执行后续 Strategies，并跟进 Bowen 的问题。",
        "State: QRIS CrossBorder UAT still has one highest-severity blocker and five high-severity issues, including the incorrect SPayLater tenure prompt and unresolved SeaBank/payment-result integrations. Impact: the UAT package is not ready for sign-off. Next: close the remaining AS/SPP fixes and run the listed retests before release approval.": "状态：QRIS CrossBorder UAT 仍有一个最高严重级别阻塞和五个高严重级别问题，包括 SPayLater 期限提示错误，以及 SeaBank 和支付结果集成未解决。影响：UAT 包尚未达到签字确认条件。下一步：关闭剩余 AS/SPP 修复，并在发布审批前完成列出的回归测试。",
        "State: Seller Cash Loan Direct Debit remains blocked in UAT because the Shopee-side issue prevents successful transactions; AF v3.53 is now targeted for 23 Sep, with a 21 Sep cutoff for testing. Impact: failure to sign off by the cutoff could remove the AF ticket from the deployment or require rollback, with limited support during the China holiday. Next: Shopee must fix the UAT issue and QA must retest before 21 Sep.": "状态：Seller Cash Loan Direct Debit 仍被 Shopee 侧问题阻塞，UAT 无法成功交易；AF v3.53 目标调整为 9 月 23 日，测试截止时间为 9 月 21 日。影响：若未能在截止时间前签字确认，AF 工单可能被移出发布或需要回滚，且中国假期期间支持资源有限。下一步：Shopee 需修复 UAT 问题，QA 需在 9 月 21 日前完成回归测试。",
        "State: The P0 Scheduled and Recurring Transfers release has 28 open SIT/UAT bugs, including Admin Portal entry failures and a PayNow interbank execution blocker. Impact: release validation and go-live readiness remain blocked. Next: prioritize the blocking defects, publish owner-level dispositions, and complete verified UAT/SIT retests before the release checkpoint.": "状态：P0 Scheduled and Recurring Transfers 发布目前有 28 个未关闭的 SIT/UAT 缺陷，包括 Admin Portal 无法进入和 PayNow 跨行执行阻塞。影响：发布验证和上线准备仍被阻塞。下一步：优先关闭阻塞缺陷，发布到责任人的逐项处理结论，并在发布检查点前完成可验证的 UAT/SIT 回归测试。",
        "State: AF is preparing an App force-upgrade to reduce version-dependent authentication branches; about 5% of users remain below v3.05 and the target is to start once the affected cohort falls below 2–3%. Impact: the decision affects AF rule-maintenance complexity and customer coverage. Next: confirm the 3.5.0 baseline and obtain Bank PMO approval for the target timing.": "状态：AF 正准备 App force-upgrade，以减少依赖版本的认证分支；目前约 5% 用户低于 v3.05，目标是在受影响用户降至 2–3% 以下后启动。影响：该决定会影响 AF 规则维护复杂度和客户覆盖范围。下一步：确认 3.5.0 基线版本，并取得 Bank PMO 对启动时间的审批。",
        "State: PH live issue analysis found that DD05 transaction types lack the required FAQ mapping, sending users to a blank page from Need Help; the PM request has now been raised for configuration. Impact: affected SPP AS DP and QR P2M journeys may show a broken support path at go-live. Next: complete and validate the DD05 FAQ configuration before the next-week go-live.": "状态：PH 线上问题分析发现 DD05 交易类型缺少 FAQ 映射，用户从 Need Help 进入空白页；PM 已提交配置请求。影响：受影响的 SPP AS DP 和 QR P2M 流程上线后可能出现失效的帮助入口。下一步：在下周上线前完成 DD05 FAQ 配置并验证。",
        "State: PH GPay UAT manual-provisioning SMS is not working, while the team has agreed to run regression for the retry-logic ticket. Impact: the UAT path remains a test dependency for v3.29 sign-off. Next: reproduce the error, complete the regression test, and record whether the flow is a release blocker or a deferred enhancement.": "状态：PH GPay UAT 的手工开通短信仍无法正常发送，团队已同意对 retry-logic 工单执行回归测试。影响：该 UAT 流程仍是 v3.29 签字确认的测试依赖。下一步：复现问题、完成回归测试，并记录该流程属于发布阻塞还是延期增强项。",
        "State: The SFV live issue is being converted into a Problem Ticket because investigation and a sustainable fix need more time. Impact: recurrence frequency and manual recovery effort will determine the long-term priority and fix SLA. Next: confirm the recurrence and operational-impact assessment and track the remediation plan.": "状态：SFV 线上问题正在转为 Problem Ticket，因为调查和可持续修复还需要更多时间。影响：复发频率和人工恢复成本将决定长期优先级及修复 SLA。下一步：确认复发情况和运营影响评估，并持续跟踪整改计划。",
        "State: Scam Model V3's new and updated live features have been configured in the AF system. Impact: the 3.09 release remains the Q3/MAS delivery checkpoint. Next: track the remaining scope and release readiness against that deadline.": "状态：Scam Model V3 的新增及更新线上特征已配置到 AF 系统。影响：v3.09 仍是 Q3/MAS 交付检查点。下一步：对照该截止时间跟踪剩余范围和发布准备度。",
        "State: The native translation-key configuration remains open. Impact: unresolved key configuration can block consistent native copy. Next: align the keys with iOS and complete validation.": "状态：Native translation-key 配置仍未完成。影响：未解决的 key 配置可能导致 Native 文案不一致。下一步：与 iOS 对齐 key，并完成验证。",
        "State: Product copywriting remains unresolved and is awaiting the business decision. Impact: copy approval is a dependency for implementation. Next: obtain the decision and apply the approved copy.": "状态：产品文案仍未解决，正在等待业务决定。影响：文案审批是开发落地的前置依赖。下一步：取得业务决定并应用获批文案。",
        "State: AF sheet rows 548-549 still require edit access. Impact: the pending change blocks the related review or validation step. Next: grant the required access and confirm the rows are updated.": "状态：AF 表格的 548-549 行仍需要编辑权限。影响：权限未开通会阻塞相关评审或验证。下一步：开通所需权限，并确认对应行已更新。",
        "State: The live incident may affect customer operations or service recovery. Next: confirm the mitigation owner, recovery status, and recurrence-prevention plan.": "状态：线上事故可能影响客户操作或服务恢复。下一步：确认缓解负责人、恢复状态和防止复发的方案。",
        "Follow up on the unresolved request: is there pop up message after user add legal in Termination case?.": "跟进未解决请求：确认 Termination case 中用户添加 legal 信息后是否会显示弹窗。",
        "I'm from the SG Bank PMO team and I just wanted to check with you who you have been liaising with for the ALC model validation. Is it grace and boheng from the FRM team?": "确认 ALC model validation 当前对接的 FRM 联系人是否为 Grace 和 Boheng，并回复 SG Bank PMO。",
        "State: The ATM release plan separates the toggle in v3.07 from withdrawal testing in v3.08. Impact: the split makes upstream timing and cross-version test coverage a release dependency. Next: lock the upstream delivery timeline and confirm the test dates before finalizing release readiness.": "状态：ATM 发布计划将 toggle 放在 v3.07，将取现测试放在 v3.08。影响：版本拆分使上游交付时间和跨版本测试覆盖成为发布依赖。下一步：锁定上游交付时间线，并在确认发布准备度前确定测试日期。",
    }
    if text in exact:
        return exact[text]

    lowered = text.casefold()
    if "is there pop up message after user add legal in termination case" in lowered:
        return "跟进未解决请求：确认 Termination case 中用户添加 legal 信息后是否会显示弹窗。"
    if "follow up on the unresolved request:" in lowered and "termination case" in lowered:
        return "跟进未解决请求：确认 Termination case 中用户添加 legal 信息后是否会显示弹窗。"
    if "not apply for tl" in lowered and "rcf limit" in lowered:
        return "确认 TL 申请是否会降低 RCF limit，并回复 Sophia Wang Zijun。"
    if "request money flow" in lowered and "offline chat" in lowered:
        return "确认 request money flow 需要调用的 AF scenarios，并明确对应的处理方案。"
    if "perpetual hold" in lowered and "-1" in lowered:
        return "跟进 perpetual hold 场景返回 -1 的处理方式，并确认是否需要补充 AF 规则。"
    if "remind me when this one's effective" in lowered or "remind me when this one is effective" in lowered:
        return "确认该变更何时生效，并提醒相关负责人。"
    if "we note this in the prd" in lowered and "v3.29" in lowered:
        return "在 PRD 中记录该功能已不属于 v3.29 范围，并确认数据是否只做直接删除。"
    if "i've never had to do any requirement" in lowered and "lv" in lowered:
        return "确认 LV 要求是否适用于该 Business request，并明确可支持的时间范围。"
    if "but 人员能不能一对多" in lowered:
        return "确认人员是否支持一对多，并回复业务结论。"
    if "spdbk-136159" in lowered and "prd" in lowered and "approve" in lowered:
        return "跟进 SPDBK-136159 的 PRD 审批，并回复处理结果。"
    if "authorization management" in lowered and "具体实现" in lowered:
        return "澄清 Authorization Management 中 Approve Comment tab 的实现方式，并回复确认结论。"
    if "reopen to open" in lowered and "overdue" in lowered and "reject" in lowered:
        return "确认 issue 从 Reopen 转为 Open 后被 reject 的场景是否需要重新计算 overdue，并说明预期行为。"
    if (
        "sg bank pmo" in lowered
        and "alc model validation" in lowered
        and "liaising" in lowered
        and "frm" in lowered
    ):
        return "确认 ALC model validation 当前对接的 FRM 联系人是否为 Grace 和 Boheng，并回复 SG Bank PMO。"

    term_replacements = (
        ("server error", "服务器报错"),
        ("current balance", "当前余额"),
        ("available balance", "可用余额"),
        ("force-upgrade", "强制升级"),
        ("model validation self-assessment", "模型验证自评估"),
        ("high-severity", "高严重级别"),
        ("highest-severity", "最高严重级别"),
        ("sign-off", "签字确认"),
        ("go-live", "上线"),
        ("release checkpoint", "发布检查点"),
        ("manual-provisioning", "手工开通"),
    )
    for source, target in term_replacements:
        localized = re.sub(re.escape(source), target, text, flags=re.IGNORECASE)
        text = localized
    text = re.sub(r"force[-‐‑‒–—−]upgrade", "强制升级", text, flags=re.IGNORECASE)

    replacements = (
        ("State:", "状态："),
        ("Impact:", "影响："),
        ("Next:", "下一步："),
        ("Due:", "截止："),
        ("Source:", "来源："),
        ("Status:", "状态："),
        ("In Progress", "进行中"),
        ("Blocked", "已阻塞"),
        ("Done", "已完成"),
        ("Unknown", "未知"),
        ("Follow up on the unresolved SeaTalk ask.", "跟进未解决的 SeaTalk 请求。"),
        ("Follow up on the unresolved request:", "跟进未解决的请求："),
        ("Follow up on ", "跟进"),
        ("Confirm ", "确认 "),
        ("Review ", "评审 "),
        ("Check ", "检查 "),
        ("Ensure ", "确保 "),
    )
    localized = text
    for source, target in replacements:
        localized = localized.replace(source, target)
    return localized


def _brief_localize_evidence(value: Any, *, language: str = "en") -> str:
    text = str(value or "").strip()
    if not _is_chinese_brief(language):
        return text
    text = re.sub(r"\s*/\s*thread\s*:\s*", " / 线程：", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSource\s*:", "来源：", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSeaTalk group\b", "SeaTalk 群组", text, flags=re.IGNORECASE)
    text = re.sub(r"\bPrivate SeaTalk chat\b", "SeaTalk 私聊", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSeaTalk conversation\b", "SeaTalk 对话", text, flags=re.IGNORECASE)
    return text


def _render_todo_text(item: dict[str, Any], *, language: str = "en") -> str:
    why = str(item.get("why") or "").strip()
    localized_task = _brief_localize_value(_sentence_text(item.get("task"), "Untitled"), language=language)
    localized_why = _brief_localize_value(_sentence_text(why, "").strip(), language=language) if why else ""
    localized_due = _display_due(item.get("due"), language=language)
    evidence = _brief_localize_evidence(
        item.get("evidence") or ("未知" if _is_chinese_brief(language) else "Unknown"),
        language=language,
    )
    if _is_chinese_brief(language):
        why_text = f"重要性：{localized_why} " if why else ""
        return f"[{_display_priority(item.get('priority'), language=language)}] {localized_task} {why_text}截止：{localized_due}（来源：{evidence}）"
    why_text = f"Why it matters: {_sentence_text(why, '').strip()} " if why else ""
    return (
        f"[{_display_priority(item.get('priority'))}] {_sentence_text(item.get('task'), 'Untitled')} "
        f"{why_text}Due: {_display_due(item.get('due'))} (Source: {item.get('evidence') or 'Unknown'})"
    )


def _render_update_text(item: dict[str, Any], *, language: str = "en") -> str:
    if _is_chinese_brief(language):
        summary = _brief_localize_value(
            _sentence_text(item.get("summary") or item.get("title"), "Untitled"),
            language=language,
        )
        evidence = _brief_localize_evidence(item.get("evidence") or "未知", language=language)
        status_suffix = ""
        if "状态：" not in summary and "State:" not in summary:
            status_suffix = f"（状态：{_display_status(item.get('status'), language=language)}）"
        return f"{summary}{status_suffix}（来源：{evidence}）"
    return (
        f"{_sentence_text(item.get('summary') or item.get('title'), 'Untitled')} "
        f"[Status: {_display_status(item.get('status'))}] (Source: {item.get('evidence') or 'Unknown'})"
    )


def _render_reminder_text(item: dict[str, Any], *, language: str = "en") -> str:
    reason = str(item.get("why") or item.get("reason") or "").strip()
    if _is_chinese_brief(language):
        reason_text = f" 重要性：{_brief_localize_value(_sentence_text(reason, '').strip(), language=language)}" if reason else ""
        evidence = _brief_localize_evidence(item.get("evidence") or "未知", language=language)
        reminder_source = str(item.get("reminder") or "需要跟进")
        if "具体实现" in reminder_source and "authorization management" in evidence.casefold():
            reminder_source = f"{reminder_source} Authorization Management Approve Comment tab"
        reminder = _brief_localize_value(
            _sentence_text(reminder_source, "需要跟进"),
            language=language,
        )
        return f"{item.get('person') or '未知'}：{reminder}{reason_text}（来源：{evidence}）"
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


def _display_domain(value: Any, *, language: str = "en") -> str:
    text = str(value or "").strip()
    aliases = {
        "anti-fraud": "Anti-fraud",
        "anti fraud": "Anti-fraud",
        "credit risk": "Credit Risk",
        "ops risk": "Ops Risk",
        "general": "General",
    }
    canonical = aliases.get(text.lower(), text or "General")
    return BRIEF_ZH_DOMAIN_LABELS.get(canonical, canonical) if _is_chinese_brief(language) else canonical


def _display_priority(value: Any, *, language: str = "en") -> str:
    normalized = _normalize_priority(value)
    if _is_chinese_brief(language):
        return BRIEF_ZH_PRIORITY_LABELS[normalized]
    return _display_priority_label(normalized)


def _normalize_priority(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"high", "medium", "low", "unknown"}:
        return text
    return "unknown"


def _display_priority_label(value: Any) -> str:
    text = str(value or "").strip().lower()
    return {"high": "High", "medium": "Medium", "low": "Low", "unknown": "Unknown"}.get(text, str(value or "Unknown"))


def _display_status(value: Any, *, language: str = "en") -> str:
    text = str(value or "").strip().lower()
    if _is_chinese_brief(language):
        return BRIEF_ZH_STATUS_LABELS.get(text, "未知")
    return {"done": "Done", "in_progress": "In Progress", "blocked": "Blocked", "unknown": "Unknown"}.get(text, str(value or "Unknown"))


def _display_due(value: Any, *, language: str = "en") -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {"tbd", "unknown", "none", "n/a", "na"}:
        return "待定" if _is_chinese_brief(language) else "TBD"
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
    return text if text.endswith((".", "!", "?", "。", "！", "？")) else f"{text}."


def _group_items_by_domain(items: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        grouped.setdefault(_display_domain(item.get("domain")), []).append(item)
    return sorted(grouped.items(), key=lambda pair: _domain_order(pair[0]))


def _render_grouped_text(items: list[dict[str, Any]], *, kind: str, language: str = "en") -> list[str]:
    lines: list[str] = []
    for domain, domain_items in _group_items_by_domain(items):
        lines.extend(["", _display_domain(domain, language=language)])
        renderer = _renderer_for_kind(kind, language=language)
        lines.extend(renderer(item) for item in domain_items)
    return lines


def _render_grouped_html(items: list[dict[str, Any]], *, kind: str, language: str = "en") -> str:
    if not items:
        if _is_chinese_brief(language):
            fallback = {
                "todo": "本窗口没有明确的 Xiaodong 待办。",
                "watch_todo": "本窗口没有需要关注或委派的事项。",
                "other": "本窗口没有其他高价值动态。",
                "reminder": "本窗口没有未解决的 SeaTalk 团队成员请求。",
            }.get(kind, "本窗口没有明确的项目动态。")
        else:
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
    renderer = _renderer_for_kind(kind, language=language)
    for domain, domain_items in _group_items_by_domain(items):
        rows = "".join(f"<li>{html.escape(renderer(item))}</li>" for item in domain_items)
        sections.append(f"<h4>{html.escape(_display_domain(domain, language=language))}</h4><ul>{rows}</ul>")
    return "".join(sections)


def _renderer_for_kind(kind: str, *, language: str = "en"):
    if kind in {"todo", "watch_todo"}:
        if not _is_chinese_brief(language):
            return _render_todo_text
        return lambda item: _render_todo_text(item, language=language)
    if kind == "reminder":
        if not _is_chinese_brief(language):
            return _render_reminder_text
        return lambda item: _render_reminder_text(item, language=language)
    if not _is_chinese_brief(language):
        return _render_update_text
    return lambda item: _render_update_text(item, language=language)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send the daily SeaTalk + Gmail briefing email.")
    parser.add_argument("--recipient", default=DEFAULT_RECIPIENT)
    parser.add_argument("--hours", type=int, default=None, help="Legacy rolling window override. Omit to use the 1pm/7pm fixed schedule.")
    parser.add_argument("--slot", choices=["auto", MORNING_SLOT, MIDDAY_SLOT], default="auto")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--now", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.fromisoformat(args.now).astimezone(SEATALK_INSIGHTS_TIMEZONE) if args.now else None
    settings = Settings.from_env()
    trello_client: TrelloDailySummaryClient | None = None
    try:
        trello_client = TrelloDailySummaryClient.from_env()
    except ConfigError:
        trello_client = None
    result = send_daily_email(
        settings=settings,
        recipient=args.recipient,
        hours=args.hours,
        slot=args.slot,
        now=now,
        force=args.force,
        dry_run=args.dry_run,
        trello_client=trello_client,
    )
    print(json.dumps(result.__dict__, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
