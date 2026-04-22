from __future__ import annotations

import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from email.utils import getaddresses
import html
from threading import Lock
from typing import Any
import re
import socket

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from bpmis_jira_tool.errors import ToolError


GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
GMAIL_METADATA_FETCH_WORKERS = 1
GMAIL_DASHBOARD_CACHE_TTL_SECONDS = 300
GMAIL_DASHBOARD_DEFAULT_DAYS = 7
GMAIL_METADATA_MAX_MESSAGES = 200
GMAIL_METADATA_FAILURE_RATIO_THRESHOLD = 0.4
GMAIL_EXPORT_BATCH_SIZE = 50
GMAIL_EXPORT_MAX_TOTAL_MESSAGES = 200
GMAIL_EXPORT_MAX_BODY_CHARS = 4000
GMAIL_EXPORT_FETCH_WORKERS = 1
GMAIL_EXPORT_INTERNAL_DOMAINS = (
    "maribank.com.sg",
    "maribank.com.ph",
    "seabank.co.id",
    "seabank.com.ph",
    "npt.sg",
    "shopee.com",
    "seamoney.com",
    "singpass.gov.sg",
)
GMAIL_EXPORT_EXCLUDED_SENDERS = (
    "reports.dwh@maribank.com.sg",
    "jira_confluence_support@shopee.com",
    "sdlc@maribank.com.ph",
    "sdlc@uat.seabank.co.id",
    "autotest@maribank.com.sg",
)
GMAIL_EXPORT_MARKETING_SUBJECT_HINTS = (
    "newsletter",
    "webinar",
    "conference",
    "events",
    "latest it",
    "strategic collaboration",
    "fraud prevention",
    "advanced solutions",
    "banking news",
)
GMAIL_EXPORT_ACCESS_REQUEST_HINTS = (
    "requests access to an item",
    "wants access",
    "via google sheets",
    "via google docs",
    "via google drive",
)
GMAIL_EXPORT_CALENDAR_SUBJECT_HINTS = (
    "invitation:",
    "updated invitation:",
)


@dataclass
class GmailMessageRecord:
    message_id: str
    internal_date: datetime
    label_ids: set[str]
    headers: dict[str, str]


@dataclass
class GmailExportRecord:
    internal_date: datetime
    headers: dict[str, str]
    body_text: str
    body_truncated: bool = False


@dataclass
class GmailDashboardCacheEntry:
    dashboard: dict[str, Any]
    expires_at: datetime


@dataclass
class GmailExportCacheEntry:
    payload: Any
    expires_at: datetime


def _normalize_header_map(headers: list[dict[str, str]] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for header in headers or []:
        name = str(header.get("name") or "").strip().lower()
        value = str(header.get("value") or "").strip()
        if name and value and name not in normalized:
            normalized[name] = value
    return normalized


def _start_of_local_day(moment: datetime) -> datetime:
    return datetime.combine(moment.date(), time.min, tzinfo=moment.tzinfo)


def _safe_datetime_from_epoch_ms(value: str | int | None, fallback_tz) -> datetime:
    try:
        epoch_ms = int(value or 0)
    except (TypeError, ValueError):
        epoch_ms = 0
    if epoch_ms <= 0:
        return datetime.now(tz=fallback_tz)
    return datetime.fromtimestamp(epoch_ms / 1000, tz=fallback_tz)


def _format_contact_label(name: str, address: str, raw: str) -> str:
    clean_name = " ".join(str(name or "").split())
    clean_address = str(address or "").strip().lower()
    if clean_name and clean_address:
        return f"{clean_name} <{clean_address}>"
    if clean_address:
        return clean_address
    return " ".join(str(raw or "").split()) or "Unknown"


def _extract_contacts(header_value: str) -> list[tuple[str, str]]:
    contacts: list[tuple[str, str]] = []
    for name, address in getaddresses([header_value or ""]):
        normalized_address = str(address or "").strip().lower()
        normalized_name = " ".join(str(name or "").split())
        if normalized_address:
            contacts.append((normalized_name, normalized_address))
    return contacts


def _first_contact_address(header_value: str) -> str:
    contacts = _extract_contacts(header_value)
    return contacts[0][1] if contacts else ""


def _build_export_query(period_start: datetime) -> str:
    excluded_clause = " ".join(f"-from:{sender}" for sender in GMAIL_EXPORT_EXCLUDED_SENDERS)
    return f"after:{int(period_start.timestamp())} -from:me in:inbox {excluded_clause}".strip()


def _decode_gmail_body_data(data: str | None) -> str:
    encoded = str(data or "").strip()
    if not encoded:
        return ""
    padded = encoded + ("=" * (-len(encoded) % 4))
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8"))
    except (ValueError, TypeError):
        return ""
    return decoded.decode("utf-8", errors="replace")


def _html_to_text(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|li|tr|h1|h2|h3|h4|h5|h6)>", "\n", text)
    text = re.sub(r"(?i)<li\b[^>]*>", "- ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_export_body_text(value: str) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return "[body unavailable]"
    lines = text.split("\n")
    kept: list[str] = []
    signature_markers = (
        "best regards",
        "thanks and regards",
        "regards",
        "sincerely",
        "many thanks",
        "thanks,",
    )
    disclaimer_markers = (
        "confidentiality notice",
        "you are receiving this email because",
        "invitation from google calendar",
        "this email was sent to you by",
        "privacy policy",
        "unsubscribe",
    )
    for line in lines:
        stripped = line.strip()
        lowered = stripped.lower()
        if re.match(r"^On .+wrote:\s*$", stripped):
            break
        if stripped.startswith(">"):
            break
        if lowered.startswith("from: ") and kept:
            break
        if "-----original message-----" in lowered:
            break
        if any(lowered.startswith(marker) for marker in signature_markers) and kept:
            break
        if any(marker in lowered for marker in disclaimer_markers):
            break
        kept.append(line)
    cleaned = "\n".join(kept).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned or "[body unavailable]"


def _is_export_noise(headers: dict[str, str]) -> bool:
    sender = _first_contact_address(headers.get("from", ""))
    subject = str(headers.get("subject") or "").strip().lower()
    if sender in GMAIL_EXPORT_EXCLUDED_SENDERS:
        return True
    if sender == "drive-shares-dm-noreply@google.com":
        return True
    if any(hint in subject for hint in GMAIL_EXPORT_ACCESS_REQUEST_HINTS):
        return True
    if any(hint in subject for hint in GMAIL_EXPORT_CALENDAR_SUBJECT_HINTS):
        return True
    if "calendar" in sender and "invitation" in subject:
        return True
    if "newsletter" in sender or any(hint in subject for hint in GMAIL_EXPORT_MARKETING_SUBJECT_HINTS):
        domain = sender.partition("@")[2]
        if domain and domain not in GMAIL_EXPORT_INTERNAL_DOMAINS:
            return True
    return False


def _trim_preview_text(value: str, *, max_chars: int = GMAIL_EXPORT_MAX_BODY_CHARS * 2) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars].rstrip()}\n..."


def _extract_message_text_from_payload(payload: dict[str, Any] | None) -> str:
    first_html_candidate: str = ""

    def walk(part: dict[str, Any] | None) -> str:
        nonlocal first_html_candidate
        if not isinstance(part, dict):
            return ""
        mime_type = str(part.get("mimeType") or "").lower()
        filename = str(part.get("filename") or "").strip()
        body = part.get("body") or {}
        data = _decode_gmail_body_data(body.get("data"))
        if mime_type.startswith("text/plain") and data:
            return _trim_preview_text(data)
        if mime_type.startswith("text/html") and data and not first_html_candidate:
            first_html_candidate = _trim_preview_text(_html_to_text(data))
        if not mime_type and data and not filename:
            return _trim_preview_text(data)
        for child in part.get("parts") or []:
            candidate = walk(child)
            if candidate.strip():
                return candidate
        return ""

    plain_candidate = walk(payload or {})
    if plain_candidate.strip():
        return plain_candidate.strip()
    if first_html_candidate.strip():
        return first_html_candidate.strip()
    return "[body unavailable]"


class GmailDashboardService:
    _dashboard_cache: dict[tuple[str, int, str], GmailDashboardCacheEntry] = {}
    _dashboard_cache_lock = Lock()
    _export_manifest_cache: dict[tuple[str, int, str], GmailExportCacheEntry] = {}
    _export_content_cache: dict[tuple[str, int, str], GmailExportCacheEntry] = {}
    _export_candidate_cache: dict[tuple[str, int, str], GmailExportCacheEntry] = {}
    _export_cache_lock = Lock()

    def __init__(self, credentials, *, gmail_service=None, cache_key: str | None = None) -> None:
        self.credentials = credentials
        self.service = gmail_service or build("gmail", "v1", credentials=credentials, cache_discovery=False)
        self.cache_key = str(cache_key or "").strip().lower()

    def build_dashboard(self, *, days: int = GMAIL_DASHBOARD_DEFAULT_DAYS, now: datetime | None = None) -> dict[str, Any]:
        overview = self.build_overview(days=days, now=now)
        network = self.build_network(days=days, now=now)
        return {
            **overview,
            "leaderboards": network.get("leaderboards", {}),
            "network_quality": network.get("data_quality", {}),
        }

    def build_overview(self, *, days: int = GMAIL_DASHBOARD_DEFAULT_DAYS, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now().astimezone()
        cached = self._get_cached_dashboard(kind="overview", days=days, now=now)
        if cached is not None:
            return cached
        stale = self._get_stale_dashboard(kind="overview", days=days)
        period_start = _start_of_local_day(now - timedelta(days=days - 1))
        try:
            received_series = self._build_query_count_series(start=period_start, days=days, query_suffix="-from:me")
            sent_series = self._build_query_count_series(start=period_start, days=days, query_suffix="in:sent")
            unread_count = self._count_messages(query="is:unread in:inbox")
            received_total = sum(row["count"] for row in received_series)
            sent_total = sum(row["count"] for row in sent_series)
            received_today = received_series[-1]["count"] if received_series else 0
            inbox_received_total = self._count_messages(query=f"after:{int(period_start.timestamp())} -from:me in:inbox")
            inbox_read_count = self._count_messages(query=f"after:{int(period_start.timestamp())} -from:me in:inbox -is:unread")
        except ToolError:
            if stale is not None:
                return self._decorate_fallback_payload(stale, now=now)
            raise
        read_rate = round((inbox_read_count / inbox_received_total) * 100) if inbox_received_total else 0

        overview = {
            "summary": {
                "received_today": received_today,
                "current_unread": unread_count,
                "read_rate_percent": read_rate,
                "received_period_total": received_total,
                "sent_period_total": sent_total,
            },
            "trends": {
                "received": received_series,
                "sent": sent_series,
            },
            "leaderboards": {
                "top_senders": [],
                "top_recipients": [],
            },
            "generated_at": now.isoformat(),
            "period_days": days,
            "data_quality": {
                "used_fallback_cache": False,
                "truncated": False,
            },
        }
        self._store_cached_dashboard(kind="overview", days=days, now=now, dashboard=overview)
        return overview

    def build_network(self, *, days: int = GMAIL_DASHBOARD_DEFAULT_DAYS, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now().astimezone()
        cached = self._get_cached_dashboard(kind="network", days=days, now=now)
        if cached is not None:
            return cached
        stale = self._get_stale_dashboard(kind="network", days=days)
        period_start = _start_of_local_day(now - timedelta(days=days - 1))
        try:
            received_messages = self._list_message_metadata(
                query=f"after:{int(period_start.timestamp())} -from:me",
            )
            sent_messages = self._list_message_metadata(
                query=f"after:{int(period_start.timestamp())} in:sent",
            )
        except ToolError:
            if stale is not None:
                return self._decorate_fallback_payload(stale, now=now)
            raise

        network = {
            "leaderboards": {
                "top_senders": self._rank_senders(received_messages),
                "top_recipients": self._rank_recipients(sent_messages),
            },
            "generated_at": now.isoformat(),
            "period_days": days,
            "data_quality": {
                "used_fallback_cache": False,
                "truncated": False,
            },
        }
        self._store_cached_dashboard(kind="network", days=days, now=now, dashboard=network)
        return network

    def build_export_manifest(self, *, days: int = GMAIL_DASHBOARD_DEFAULT_DAYS, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now().astimezone()
        cached = self._get_cached_export_manifest(days=days, now=now)
        if cached is not None:
            return cached
        period_start = _start_of_local_day(now - timedelta(days=days - 1))
        total_messages = len(self._list_message_ids(query=_build_export_query(period_start), max_messages=GMAIL_EXPORT_MAX_TOTAL_MESSAGES))
        batch_count = max(1, (total_messages + GMAIL_EXPORT_BATCH_SIZE - 1) // GMAIL_EXPORT_BATCH_SIZE) if total_messages else 0
        manifest = {
            "generated_at": now.isoformat(),
            "period_days": days,
            "total_messages": total_messages,
            "batch_size": GMAIL_EXPORT_BATCH_SIZE,
            "batch_count": batch_count,
            "excluded_senders": list(GMAIL_EXPORT_EXCLUDED_SENDERS),
            "max_total_messages": GMAIL_EXPORT_MAX_TOTAL_MESSAGES,
            "capped": total_messages >= GMAIL_EXPORT_MAX_TOTAL_MESSAGES,
            "estimated": True,
        }
        self._store_cached_export_manifest(days=days, now=now, manifest=manifest)
        return manifest

    def export_history_text(
        self,
        *,
        days: int = GMAIL_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
        batch: int = 1,
    ) -> tuple[str, str]:
        now = now or datetime.now().astimezone()
        cached = self._get_cached_export_content(days=days, batch=batch, now=now)
        if cached is not None:
            return cached
        period_start = _start_of_local_day(now - timedelta(days=days - 1))
        if batch < 1:
            raise ToolError("Invalid Gmail export batch. Please refresh and try again.")
        messages, metadata = self._list_export_messages(
            query=_build_export_query(period_start),
            batch=batch,
            days=days,
            now=now,
        )
        lines = [
            "Gmail history export",
            f"Generated at: {now.isoformat()}",
            f"Window: last {days} days",
            "Scope: inbox / received messages only",
            f"Batch: {metadata['batch']}",
            f"Batch size: {GMAIL_EXPORT_BATCH_SIZE}",
            f"Included messages: {metadata['included_messages']}",
            f"Max body length per message: {GMAIL_EXPORT_MAX_BODY_CHARS} characters",
            "",
        ]
        if metadata["total_messages"]:
            if metadata.get("estimated"):
                lines.append(
                    f"At least {metadata['total_messages']} exportable messages were identified so far in the last {days} days after sender exclusions."
                )
            else:
                lines.append(
                    f"Total exportable messages in last {days} days after sender exclusions: {metadata['total_messages']}"
                )
        if metadata["capped"]:
            lines.append(
                f"Note: export scanning is capped at the first {GMAIL_EXPORT_MAX_TOTAL_MESSAGES} matching emails to keep downloads stable."
            )
        if metadata["batch_count"] > 1 and not metadata.get("estimated"):
            lines.append(f"Total batches: {metadata['batch_count']}")
        if metadata["truncated_bodies"]:
            lines.append(
                f"Note: {metadata['truncated_bodies']} message bodies were truncated to keep the download stable."
            )
        if metadata["batch_count"] > 1 or metadata["truncated_bodies"] or metadata["capped"]:
            lines.append("")
        separator = "=" * 80
        for index, message in enumerate(messages, start=1):
            headers = message.headers
            lines.extend(
                [
                    f"{separator}",
                    f"Message {index}",
                    f"Date: {message.internal_date.isoformat()}",
                    f"From: {headers.get('from') or '[unknown sender]'}",
                    f"To: {headers.get('to') or '[no recipients listed]'}",
                    f"Subject: {headers.get('subject') or '[no subject]'}",
                    "",
                    "Body:",
                    message.body_text or "[body unavailable]",
                ]
            )
            if message.body_truncated:
                lines.extend(["", "[body truncated]"])
            lines.append("")
        if not messages:
            lines.append("No inbox messages were found in this export batch.")
            lines.append("")
        filename = f"gmail-history-last-{days}-days-batch-{batch}.txt"
        payload = ("\n".join(lines), filename)
        self._store_cached_export_content(days=days, batch=batch, now=now, payload=payload)
        return payload

    def get_cached_export_history_text(
        self,
        *,
        days: int = GMAIL_DASHBOARD_DEFAULT_DAYS,
        batch: int = 1,
        now: datetime | None = None,
    ) -> tuple[str, str] | None:
        now = now or datetime.now().astimezone()
        return self._get_cached_export_content(days=days, batch=batch, now=now)

    def prewarm_export_history_text(
        self,
        *,
        days: int = GMAIL_DASHBOARD_DEFAULT_DAYS,
        batch: int = 1,
        now: datetime | None = None,
    ) -> tuple[str, str]:
        return self.export_history_text(days=days, batch=batch, now=now)

    def _cache_token(self, *, kind: str, days: int) -> tuple[str, int, str] | None:
        if not self.cache_key:
            return None
        return (self.cache_key, days, kind)

    def _get_cached_export_manifest(self, *, days: int, now: datetime) -> dict[str, Any] | None:
        cache_token = self._cache_token(kind="export_manifest", days=days)
        if cache_token is None:
            return None
        with self._export_cache_lock:
            entry = self._export_manifest_cache.get(cache_token)
            if entry is None or entry.expires_at <= now:
                return None
            return dict(entry.payload)

    def _store_cached_export_manifest(self, *, days: int, now: datetime, manifest: dict[str, Any]) -> None:
        cache_token = self._cache_token(kind="export_manifest", days=days)
        if cache_token is None:
            return
        with self._export_cache_lock:
            self._export_manifest_cache[cache_token] = GmailExportCacheEntry(
                payload=dict(manifest),
                expires_at=now + timedelta(seconds=GMAIL_DASHBOARD_CACHE_TTL_SECONDS),
            )

    def _get_cached_export_content(self, *, days: int, batch: int, now: datetime) -> tuple[str, str] | None:
        cache_token = self._cache_token(kind=f"export_batch_{batch}", days=days)
        if cache_token is None:
            return None
        with self._export_cache_lock:
            entry = self._export_content_cache.get(cache_token)
            if entry is None or entry.expires_at <= now:
                return None
            payload = entry.payload
            if isinstance(payload, tuple) and len(payload) == 2:
                return str(payload[0]), str(payload[1])
            return None

    def _store_cached_export_content(
        self,
        *,
        days: int,
        batch: int,
        now: datetime,
        payload: tuple[str, str],
    ) -> None:
        cache_token = self._cache_token(kind=f"export_batch_{batch}", days=days)
        if cache_token is None:
            return
        with self._export_cache_lock:
            self._export_content_cache[cache_token] = GmailExportCacheEntry(
                payload=(payload[0], payload[1]),
                expires_at=now + timedelta(seconds=GMAIL_DASHBOARD_CACHE_TTL_SECONDS),
            )

    def _get_cached_export_candidates(self, *, days: int, now: datetime) -> dict[str, Any] | None:
        cache_token = self._cache_token(kind="export_candidates", days=days)
        if cache_token is None:
            return None
        with self._export_cache_lock:
            entry = self._export_candidate_cache.get(cache_token)
            if entry is None or entry.expires_at <= now:
                return None
            payload = entry.payload
            return dict(payload) if isinstance(payload, dict) else None

    def _store_cached_export_candidates(self, *, days: int, now: datetime, payload: dict[str, Any]) -> None:
        cache_token = self._cache_token(kind="export_candidates", days=days)
        if cache_token is None:
            return
        with self._export_cache_lock:
            self._export_candidate_cache[cache_token] = GmailExportCacheEntry(
                payload=dict(payload),
                expires_at=now + timedelta(seconds=GMAIL_DASHBOARD_CACHE_TTL_SECONDS),
            )

    def _get_cached_dashboard(self, *, kind: str, days: int, now: datetime) -> dict[str, Any] | None:
        cache_token = self._cache_token(kind=kind, days=days)
        if cache_token is None:
            return None
        with self._dashboard_cache_lock:
            entry = self._dashboard_cache.get(cache_token)
            if entry is None:
                return None
            if entry.expires_at <= now:
                return None
            return entry.dashboard

    def _get_stale_dashboard(self, *, kind: str, days: int) -> dict[str, Any] | None:
        cache_token = self._cache_token(kind=kind, days=days)
        if cache_token is None:
            return None
        with self._dashboard_cache_lock:
            entry = self._dashboard_cache.get(cache_token)
            return None if entry is None else entry.dashboard

    def _store_cached_dashboard(self, *, kind: str, days: int, now: datetime, dashboard: dict[str, Any]) -> None:
        cache_token = self._cache_token(kind=kind, days=days)
        if cache_token is None:
            return
        with self._dashboard_cache_lock:
            self._dashboard_cache[cache_token] = GmailDashboardCacheEntry(
                dashboard=dashboard,
                expires_at=now + timedelta(seconds=GMAIL_DASHBOARD_CACHE_TTL_SECONDS),
            )

    def _list_message_metadata(self, *, query: str) -> list[GmailMessageRecord]:
        message_ids = self._list_message_ids(query=query, max_messages=GMAIL_METADATA_MAX_MESSAGES)
        if not message_ids:
            return []
        workers = min(GMAIL_METADATA_FETCH_WORKERS, len(message_ids))
        messages: list[GmailMessageRecord] = []
        failures = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self._fetch_message_metadata, message_id): message_id for message_id in message_ids}
            for future in as_completed(futures):
                try:
                    record = future.result()
                except ToolError as error:
                    failures += 1
                    if not self._can_tolerate_metadata_failures(total=len(message_ids), failures=failures):
                        raise error
                except Exception as error:
                    failures += 1
                    if not self._can_tolerate_metadata_failures(total=len(message_ids), failures=failures):
                        raise ToolError("Gmail data could not be loaded right now. Please try again shortly.") from error
                else:
                    messages.append(record)
        return sorted(messages, key=lambda item: item.internal_date, reverse=True)

    def _list_export_messages(
        self,
        *,
        query: str,
        batch: int,
        days: int = GMAIL_DASHBOARD_DEFAULT_DAYS,
        now: datetime | None = None,
    ) -> tuple[list[GmailExportRecord], dict[str, int | bool]]:
        now = now or datetime.now().astimezone()
        all_message_ids = self._list_export_candidate_ids(
            query=query,
            required_count=(batch * GMAIL_EXPORT_BATCH_SIZE) + 1,
            days=days,
            now=now,
        )
        candidate_state = self._get_cached_export_candidates(days=days, now=now) or {}
        estimated = not bool(candidate_state.get("fully_scanned"))
        total_messages = len(all_message_ids)
        batch_count = max(1, (total_messages + GMAIL_EXPORT_BATCH_SIZE - 1) // GMAIL_EXPORT_BATCH_SIZE) if total_messages else 0
        if batch_count and batch > batch_count:
            raise ToolError("This Gmail export batch is no longer available. Please refresh and try again.")
        start_index = (batch - 1) * GMAIL_EXPORT_BATCH_SIZE
        end_index = start_index + GMAIL_EXPORT_BATCH_SIZE
        message_ids = all_message_ids[start_index:end_index]
        if not message_ids:
            return [], {
                "included_messages": 0,
                "batch": batch,
                "batch_count": batch_count,
                "total_messages": total_messages,
                "truncated_bodies": 0,
                "capped": total_messages >= GMAIL_EXPORT_MAX_TOTAL_MESSAGES,
                "estimated": estimated,
            }
        messages = self._fetch_message_full_many(message_ids)
        truncated_bodies = sum(1 for record in messages if record.body_truncated)
        return sorted(messages, key=lambda item: item.internal_date, reverse=True), {
            "included_messages": len(messages),
            "batch": batch,
            "batch_count": batch_count,
            "total_messages": total_messages,
            "truncated_bodies": truncated_bodies,
            "capped": total_messages >= GMAIL_EXPORT_MAX_TOTAL_MESSAGES,
            "estimated": estimated,
        }

    @staticmethod
    def _can_tolerate_metadata_failures(*, total: int, failures: int) -> bool:
        return failures / max(total, 1) <= GMAIL_METADATA_FAILURE_RATIO_THRESHOLD

    def _fetch_message_metadata(self, message_id: str) -> GmailMessageRecord:
        users_api = self.service.users().messages()
        try:
            payload = users_api.get(
                userId="me",
                id=message_id,
                format="metadata",
                metadataHeaders=["From", "To", "Cc", "Bcc", "Subject"],
                fields="id,internalDate,labelIds,payload/headers",
            ).execute()
        except HttpError as error:
            raise self._build_gmail_error(error) from error
        except (TimeoutError, socket.timeout, OSError) as error:
            raise ToolError("Gmail data could not be loaded right now. Please try again shortly.") from error
        headers = _normalize_header_map((payload.get("payload") or {}).get("headers"))
        return GmailMessageRecord(
            message_id=str(message_id),
            internal_date=_safe_datetime_from_epoch_ms(payload.get("internalDate"), datetime.now().astimezone().tzinfo),
            label_ids=set(payload.get("labelIds") or []),
            headers=headers,
        )

    def _fetch_message_full(self, message_id: str) -> GmailExportRecord:
        users_api = self.service.users().messages()
        try:
            payload = users_api.get(
                userId="me",
                id=message_id,
                format="full",
                fields="id,internalDate,payload(mimeType,filename,headers,body/data,parts)",
            ).execute()
        except HttpError as error:
            raise self._build_gmail_error(error) from error
        except (TimeoutError, socket.timeout, OSError) as error:
            raise ToolError("Gmail mail history could not be exported right now. Please try again shortly.") from error
        message_payload = payload.get("payload") or {}
        headers = _normalize_header_map(message_payload.get("headers"))
        body_text = _clean_export_body_text(_extract_message_text_from_payload(message_payload))
        body_truncated = len(body_text) > GMAIL_EXPORT_MAX_BODY_CHARS
        if body_truncated:
            body_text = f"{body_text[:GMAIL_EXPORT_MAX_BODY_CHARS].rstrip()}\n..."
        return GmailExportRecord(
            internal_date=_safe_datetime_from_epoch_ms(payload.get("internalDate"), datetime.now().astimezone().tzinfo),
            headers=headers,
            body_text=body_text,
            body_truncated=body_truncated,
        )

    def _list_message_ids(self, *, query: str, max_messages: int | None) -> list[str]:
        users_api = self.service.users().messages()
        message_ids: list[str] = []
        page_token: str | None = None
        while True:
            try:
                payload = users_api.list(
                    userId="me",
                    q=query,
                    maxResults=500,
                    pageToken=page_token,
                    fields="messages/id,nextPageToken",
                ).execute()
            except HttpError as error:
                raise self._build_gmail_error(error) from error
            except (TimeoutError, socket.timeout, OSError) as error:
                raise ToolError("Gmail data could not be loaded right now. Please try again shortly.") from error
            message_ids.extend(str(item.get("id") or "") for item in payload.get("messages") or [] if item.get("id"))
            if max_messages is not None and len(message_ids) >= max_messages:
                return message_ids[:max_messages]
            page_token = payload.get("nextPageToken")
            if not page_token:
                break
        return message_ids

    def _list_export_candidate_ids(
        self,
        *,
        query: str,
        required_count: int | None = None,
        now: datetime | None = None,
        days: int = GMAIL_DASHBOARD_DEFAULT_DAYS,
    ) -> list[str]:
        now = now or datetime.now().astimezone()
        state = self._get_cached_export_candidates(days=days, now=now) or {}
        accepted_ids = list(state.get("accepted_ids") or [])
        source_ids = list(state.get("source_ids") or [])
        scanned_count = int(state.get("scanned_count") or 0)
        fully_scanned = bool(state.get("fully_scanned"))
        if not source_ids:
            source_ids = self._list_message_ids(query=query, max_messages=GMAIL_EXPORT_MAX_TOTAL_MESSAGES)
        target_count = min(max(required_count or len(source_ids), 0), len(source_ids))
        if not fully_scanned and len(accepted_ids) < target_count:
            chunk_size = max(GMAIL_EXPORT_BATCH_SIZE, 25)
            while scanned_count < len(source_ids) and len(accepted_ids) < target_count:
                next_ids = source_ids[scanned_count:scanned_count + chunk_size]
                scanned_count += len(next_ids)
                for record in self._fetch_message_metadata_many(next_ids):
                    if not _is_export_noise(record.headers):
                        accepted_ids.append(record.message_id)
            fully_scanned = scanned_count >= len(source_ids)
            self._store_cached_export_candidates(
                days=days,
                now=now,
                payload={
                    "accepted_ids": accepted_ids,
                    "source_ids": source_ids,
                    "scanned_count": scanned_count,
                    "fully_scanned": fully_scanned,
                },
            )
        if fully_scanned:
            return accepted_ids
        return accepted_ids[:target_count]

    def _fetch_message_metadata_many(self, message_ids: list[str]) -> list[GmailMessageRecord]:
        if not message_ids:
            return []
        workers = min(GMAIL_METADATA_FETCH_WORKERS, len(message_ids))
        records: list[GmailMessageRecord] = []
        failures = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self._fetch_message_metadata, message_id): message_id for message_id in message_ids}
            for future in as_completed(futures):
                try:
                    record = future.result()
                except ToolError as error:
                    failures += 1
                    if not self._can_tolerate_metadata_failures(total=len(message_ids), failures=failures):
                        raise error
                except Exception as error:
                    failures += 1
                    if not self._can_tolerate_metadata_failures(total=len(message_ids), failures=failures):
                        raise ToolError("Gmail export candidates could not be prepared right now. Please try again shortly.") from error
                else:
                    records.append(record)
        records.sort(key=lambda item: item.internal_date, reverse=True)
        return records

    def _fetch_message_full_many(self, message_ids: list[str]) -> list[GmailExportRecord]:
        if not message_ids:
            return []
        workers = min(GMAIL_EXPORT_FETCH_WORKERS, len(message_ids))
        records: list[GmailExportRecord] = []
        failures = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self._fetch_message_full, message_id): message_id for message_id in message_ids}
            for future in as_completed(futures):
                try:
                    record = future.result()
                except ToolError as error:
                    failures += 1
                    if not self._can_tolerate_metadata_failures(total=len(message_ids), failures=failures):
                        raise error
                except Exception as error:
                    failures += 1
                    if not self._can_tolerate_metadata_failures(total=len(message_ids), failures=failures):
                        raise ToolError("Gmail mail history could not be exported right now. Please try again shortly.") from error
                else:
                    records.append(record)
        return records

    def _count_messages(self, *, query: str) -> int:
        return len(self._list_message_ids(query=query, max_messages=GMAIL_METADATA_MAX_MESSAGES))

    def _decorate_fallback_payload(self, dashboard: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        payload = {
            **dashboard,
            "generated_at": now.isoformat(),
            "data_quality": {
                "used_fallback_cache": True,
                "truncated": bool((dashboard.get("data_quality") or {}).get("truncated")),
            },
        }
        return payload

    def _build_query_count_series(self, *, start: datetime, days: int, query_suffix: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for index in range(days):
            day_start = start + timedelta(days=index)
            day_end = day_start + timedelta(days=1)
            count = self._count_messages(
                query=f"after:{int(day_start.timestamp())} before:{int(day_end.timestamp())} {query_suffix}".strip()
            )
            rows.append(
                {
                    "date": day_start.date().isoformat(),
                    "label": day_start.strftime("%b %d"),
                    "count": count,
                }
            )
        return rows

    def _build_daily_series(self, messages: list[GmailMessageRecord], *, start: datetime, days: int) -> list[dict[str, Any]]:
        buckets = {start.date() + timedelta(days=index): 0 for index in range(days)}
        for message in messages:
            message_day = message.internal_date.astimezone(start.tzinfo).date()
            if message_day in buckets:
                buckets[message_day] += 1
        return [
            {
                "date": day.isoformat(),
                "label": day.strftime("%b %d"),
                "count": buckets[day],
            }
            for day in sorted(buckets.keys())
        ]

    def _rank_senders(self, messages: list[GmailMessageRecord]) -> list[dict[str, Any]]:
        counts: Counter[str] = Counter()
        labels: dict[str, str] = {}
        for message in messages:
            raw_sender = message.headers.get("from", "")
            for name, address in _extract_contacts(raw_sender):
                key = address.lower()
                counts[key] += 1
                labels.setdefault(key, _format_contact_label(name, address, raw_sender))
        return self._render_rankings(counts, labels)

    def _rank_recipients(self, messages: list[GmailMessageRecord]) -> list[dict[str, Any]]:
        counts: Counter[str] = Counter()
        labels: dict[str, str] = {}
        for message in messages:
            for header_name in ("to", "cc", "bcc"):
                raw_value = message.headers.get(header_name, "")
                for name, address in _extract_contacts(raw_value):
                    key = address.lower()
                    counts[key] += 1
                    labels.setdefault(key, _format_contact_label(name, address, raw_value))
        return self._render_rankings(counts, labels)

    @staticmethod
    def _render_rankings(counts: Counter[str], labels: dict[str, str]) -> list[dict[str, Any]]:
        rows = [
            {
                "rank": index + 1,
                "label": labels[key],
                "count": count,
            }
            for index, (key, count) in enumerate(
                sorted(counts.items(), key=lambda item: (-item[1], labels.get(item[0], item[0]).lower()))[:10]
            )
        ]
        return rows

    @staticmethod
    def _build_gmail_error(error: HttpError) -> ToolError:
        content = getattr(error, "content", b"") or b""
        text = content.decode("utf-8", errors="ignore")
        normalized = text.lower()
        status = getattr(getattr(error, "resp", None), "status", None)
        if "access_token_scope_insufficient" in normalized or "insufficientpermissions" in normalized:
            return ToolError(
                "Gmail access is not available for this Google session yet. Please sign in with Google again to grant Gmail read access."
            )
        if (
            "accessnotconfigured" in normalized
            or "service_disabled" in normalized
            or "api has not been used" in normalized
            or "gmail api has not been used" in normalized
        ):
            return ToolError(
                "The Google sign-in succeeded, but the Gmail API is not enabled for this Google Cloud project yet. "
                "Enable the Gmail API for the OAuth project and try again."
            )
        if "admin_policy_enforced" in normalized or "access blocked by admin" in normalized:
            return ToolError(
                "The Google sign-in succeeded, but this Gmail access is blocked by Google Workspace admin policy. "
                "Allow the app's Gmail readonly scope in Workspace and try again."
            )
        if status in {401, 403}:
            return ToolError(
                "The Google sign-in succeeded, but Gmail API access was still denied. "
                "This is usually caused by Google Cloud API settings or Workspace admin restrictions."
            )
        return ToolError("Gmail data could not be loaded right now. Please try again shortly.")

    @classmethod
    def clear_cache(cls) -> None:
        with cls._dashboard_cache_lock:
            cls._dashboard_cache.clear()
        with cls._export_cache_lock:
            cls._export_manifest_cache.clear()
            cls._export_content_cache.clear()
            cls._export_candidate_cache.clear()
