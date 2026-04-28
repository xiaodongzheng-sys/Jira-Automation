#!/usr/bin/env python3
"""Forward selected WeChat macOS notifications to a SeaTalk webhook."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import plistlib
import re
import shutil
import signal
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


WECHAT_BUNDLE_ID = "com.tencent.xinWeChat"
WECHAT_BUNDLE_IDS = {WECHAT_BUNDLE_ID.lower(), "com.tencent.xinwechat"}
DEFAULT_POLL_SECONDS = 2.0
DEFAULT_DEDUPE_MINUTES = 10
DEFAULT_STATE_PATH = ".team-portal/wechat_notification_to_seatalk/state.json"
DEFAULT_REPLY_MAP_PATH = ".team-portal/wechat_notification_to_seatalk/replies.json"
DEFAULT_NOTIFICATION_LOOKBACK_MINUTES = 30
DEFAULT_REPLY_TTL_HOURS = 24
DEFAULT_REPLY_SERVER_HOST = "127.0.0.1"
DEFAULT_REPLY_SERVER_PORT = 8797
DEFAULT_FILE_FALLBACK_STATE = ".team-portal/wechat_notification_to_seatalk/file_fallback.json"


@dataclass(frozen=True)
class NotificationEvent:
    event_id: str
    app_id: str
    title: str
    subtitle: str
    body: str
    delivered_at: float
    source: str

    @property
    def combined_text(self) -> str:
        return " ".join(part for part in (self.title, self.subtitle, self.body) if part).strip()


@dataclass(frozen=True)
class ClassifiedAlert:
    event: NotificationEvent
    event_type: str
    conversation: str
    preview: str


@dataclass(frozen=True)
class ReplyTarget:
    reply_id: str
    conversation: str
    event_type: str
    preview: str
    created_at: float
    expires_at: float


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _float_env(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _now() -> float:
    return time.time()


def _format_local_timestamp(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return _strings_from_blob(value)
    return str(value).strip()


def _strings_from_blob(blob: bytes) -> str:
    parts: list[str] = []
    for pattern in (rb"[\x20-\x7e]{3,}", rb"(?:[\x20-\x7e]\x00){3,}"):
        for match in re.finditer(pattern, blob):
            raw = match.group(0)
            try:
                text = raw.decode("utf-16le" if b"\x00" in raw else "utf-8", errors="ignore")
            except UnicodeDecodeError:
                continue
            text = text.replace("\x00", "").strip()
            if text and text not in parts:
                parts.append(text)
    return " ".join(parts)


def _sqlite_timestamp_to_epoch(value: object) -> float:
    if value is None:
        return _now()
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 10_000_000_000:
            return number / 1000.0
        if number > 1_000_000_000:
            return number
        if number > 100_000_000:
            return number + 978_307_200
        return _now()
    text = str(value).strip()
    if not text:
        return _now()
    with contextlib.suppress(ValueError):
        return _sqlite_timestamp_to_epoch(float(text))
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(text[:19], fmt).replace(tzinfo=timezone.utc).timestamp()
    return _now()


def _extract_payload_fields(payload: object) -> tuple[str, str, str]:
    if isinstance(payload, bytes):
        with contextlib.suppress(Exception):
            plist = plistlib.loads(payload)
            if isinstance(plist, dict):
                req = plist.get("req")
                if isinstance(req, dict):
                    title = _safe_text(req.get("titl"))
                    subtitle = _safe_text(req.get("subt"))
                    body = _safe_text(req.get("body"))
                    if title or subtitle or body:
                        return title, subtitle, body

    text_values: list[str] = []

    def walk(value: object) -> None:
        if isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, (str, bytes)):
            text = _safe_text(value)
            if text:
                text_values.append(text)

    if isinstance(payload, bytes):
        with contextlib.suppress(Exception):
            walk(plistlib.loads(payload))
        if not text_values:
            text_values.append(_strings_from_blob(payload))
    else:
        walk(payload)

    filtered = []
    for text in text_values:
        if not text or text in filtered:
            continue
        if text in {WECHAT_BUNDLE_ID, "WeChat", "微信"}:
            continue
        if len(text) > 500:
            text = text[:500]
        filtered.append(text)

    title = filtered[0] if filtered else ""
    subtitle = filtered[1] if len(filtered) > 1 else ""
    body = " ".join(filtered[2:]) if len(filtered) > 2 else ""
    return title, subtitle, body


def discover_notification_databases() -> list[Path]:
    explicit = os.environ.get("WECHAT_ALERT_NOTIFICATION_DB", "").strip()
    candidates: list[Path] = []
    if explicit:
        explicit_path = Path(explicit).expanduser()
        candidates.append(explicit_path)
        if explicit_path.is_file():
            return [explicit_path]

    home = Path.home()
    candidates.extend(
        [
            home / "Library/Application Support/NotificationCenter",
            home / "Library/Group Containers/group.com.apple.usernoted",
            home / "Library/Group Containers/group.com.apple.UserNotifications",
        ]
    )

    for root in Path("/private/var/folders").glob("*/*"):
        candidates.extend(
            [
                root / "0/com.apple.notificationcenter/db2/db",
                root / "C/com.apple.notificationcenter/db2/db",
                root / "0/com.apple.notificationcenter/db/db",
                root / "C/com.apple.notificationcenter/db/db",
            ]
        )

    paths: list[Path] = []
    for candidate in candidates:
        try:
            is_file = candidate.is_file()
            is_dir = candidate.is_dir()
        except OSError:
            continue
        if is_file and os.access(candidate, os.R_OK) and candidate not in paths:
            paths.append(candidate)
        elif is_dir:
            try:
                children = list(candidate.rglob("*"))
            except OSError:
                continue
            for child in children:
                try:
                    readable_file = child.is_file() and os.access(child, os.R_OK)
                except OSError:
                    continue
                if readable_file and child.name.lower() in {"db", "db2", "notifications.db", "notifications.sqlite"}:
                    if child not in paths:
                        paths.append(child)
    return paths


def discover_wechat_activity_files() -> list[Path]:
    explicit = os.environ.get("WECHAT_ALERT_ACTIVITY_FILES", "").strip()
    if explicit:
        return [Path(item).expanduser() for item in explicit.split(":") if item.strip()]
    root = Path.home() / "Library/Containers/com.tencent.xinWeChat/Data/Documents/xwechat_files"
    if not root.exists():
        return []
    patterns = [
        "*/db_storage/message/message_0.db",
        "*/db_storage/message/message_0.db-wal",
        "*/db_storage/session/session.db",
        "*/db_storage/session/session.db-wal",
    ]
    files: list[Path] = []
    for pattern in patterns:
        for path in root.glob(pattern):
            if path.is_file() and path not in files:
                files.append(path)
    return files


class WeChatActivityFileFallback:
    def __init__(self, path: Path, *, quiet_seconds: int = 20) -> None:
        self.path = path
        self.quiet_seconds = quiet_seconds
        self.last_sent_mtime = 0.0
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if isinstance(payload, dict):
            self.last_sent_mtime = float(payload.get("last_sent_mtime", 0.0) or 0.0)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"last_sent_mtime": self.last_sent_mtime}, indent=2), encoding="utf-8")

    def poll(self) -> bool:
        newest = 0.0
        for path in discover_wechat_activity_files():
            try:
                newest = max(newest, path.stat().st_mtime)
            except OSError:
                continue
        if newest <= 0:
            return False
        if newest <= self.last_sent_mtime:
            return False
        if _now() - newest < self.quiet_seconds:
            return False
        self.last_sent_mtime = newest
        self.save()
        return True


class NotificationDatabaseReader:
    def __init__(self, db_paths: Iterable[Path] | None = None) -> None:
        self.db_paths = list(db_paths) if db_paths is not None else discover_notification_databases()
        self._fixed_paths = db_paths is not None

    def read_events(self, since_epoch: float) -> list[NotificationEvent]:
        if not self._fixed_paths:
            self.db_paths = discover_notification_databases()
        events: list[NotificationEvent] = []
        for db_path in self.db_paths:
            events.extend(self._read_db(db_path, since_epoch))
        unique: dict[str, NotificationEvent] = {}
        for event in events:
            unique[event.event_id] = event
        return sorted(unique.values(), key=lambda item: item.delivered_at)

    def _read_db(self, db_path: Path, since_epoch: float) -> list[NotificationEvent]:
        if not db_path.exists():
            return []
        with tempfile.TemporaryDirectory(prefix="wechat-alert-db-") as tmpdir:
            tmp_path = Path(tmpdir) / "notifications.db"
            shutil.copy2(db_path, tmp_path)
            for suffix in ("-wal", "-shm"):
                sidecar = Path(f"{db_path}{suffix}")
                if sidecar.exists():
                    shutil.copy2(sidecar, Path(f"{tmp_path}{suffix}"))
            try:
                conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
            except sqlite3.Error:
                return []
            with conn:
                return self._read_connected_db(conn, db_path, since_epoch)

    def _read_connected_db(self, conn: sqlite3.Connection, db_path: Path, since_epoch: float) -> list[NotificationEvent]:
        tables = self._table_names(conn)
        if {"app", "record"}.issubset(tables):
            return self._read_app_record_schema(conn, db_path, since_epoch)
        return self._read_generic_schema(conn, db_path, since_epoch)

    @staticmethod
    def _table_names(conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        return {str(row[0]) for row in rows}

    @staticmethod
    def _columns(conn: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [(str(row[1]), str(row[2]).upper()) for row in rows]

    def _read_app_record_schema(self, conn: sqlite3.Connection, db_path: Path, since_epoch: float) -> list[NotificationEvent]:
        app_columns = {name for name, _ in self._columns(conn, "app")}
        record_columns = {name for name, _ in self._columns(conn, "record")}
        identifier_col = "identifier" if "identifier" in app_columns else "bundleid" if "bundleid" in app_columns else None
        if not identifier_col:
            return []

        time_col = next((name for name in ("delivered_date", "delivery_date", "request_date", "date") if name in record_columns), None)
        uuid_col = next((name for name in ("uuid", "identifier", "request_identifier", "rec_id") if name in record_columns), "rowid")
        payload_col = next((name for name in ("data", "request", "content", "blob") if name in record_columns), None)
        if not payload_col:
            return []

        query = (
            f"SELECT r.{uuid_col}, r.{payload_col}, a.{identifier_col}, "
            f"{'r.' + time_col if time_col else 'NULL'} "
            "FROM record r JOIN app a ON r.app_id = a.app_id "
            f"WHERE lower(a.{identifier_col}) IN ({','.join('?' for _ in WECHAT_BUNDLE_IDS)})"
        )
        events = []
        for row in conn.execute(query, tuple(sorted(WECHAT_BUNDLE_IDS))):
            delivered_at = _sqlite_timestamp_to_epoch(row[3])
            if delivered_at < since_epoch:
                continue
            title, subtitle, body = _extract_payload_fields(row[1])
            event_id = f"{db_path}:{row[0]}"
            events.append(NotificationEvent(event_id, _safe_text(row[2]) or WECHAT_BUNDLE_ID, title, subtitle, body, delivered_at, str(db_path)))
        return events

    def _read_generic_schema(self, conn: sqlite3.Connection, db_path: Path, since_epoch: float) -> list[NotificationEvent]:
        events: list[NotificationEvent] = []
        for table in self._table_names(conn):
            if table.startswith("sqlite_"):
                continue
            columns = self._columns(conn, table)
            names = [name for name, _ in columns]
            if not names:
                continue
            try:
                rows = conn.execute(f"SELECT rowid, * FROM {table} ORDER BY rowid DESC LIMIT 300").fetchall()
            except sqlite3.Error:
                continue
            for row in rows:
                rowid = row[0]
                values = dict(zip(names, row[1:]))
                joined = " ".join(_safe_text(value) for value in values.values())
                if WECHAT_BUNDLE_ID not in joined and "WeChat" not in joined and "微信" not in joined:
                    continue
                time_value = next((values[name] for name in names if name.lower() in {"date", "time", "timestamp", "delivered_date", "delivery_date"}), None)
                delivered_at = _sqlite_timestamp_to_epoch(time_value)
                if delivered_at < since_epoch:
                    continue
                title, subtitle, body = _extract_payload_fields(list(values.values()))
                events.append(
                    NotificationEvent(
                        f"{db_path}:{table}:{rowid}",
                        WECHAT_BUNDLE_ID,
                        title,
                        subtitle,
                        body,
                        delivered_at,
                        str(db_path),
                    )
                )
        return events


PRIVATE_PREVIEW_RE = re.compile(r"^[^:：]{1,50}[:：]\s+")
MENTION_TOKENS = ("@我", "@你", "有人@我", "有人 @ 我", "mentioned you", "@you", "@ me")
GENERIC_WECHAT_TITLES = {"wechat", "微信"}


def classify_wechat_notification(event: NotificationEvent) -> ClassifiedAlert | None:
    combined = event.combined_text
    if not combined:
        return None
    lowered = combined.lower()
    is_mention = any(token.lower() in lowered for token in MENTION_TOKENS)
    body_has_sender_prefix = bool(PRIVATE_PREVIEW_RE.match(event.body or event.subtitle))

    if is_mention:
        conversation = event.title or event.subtitle or "WeChat group"
        preview = event.body or event.subtitle or combined
        return ClassifiedAlert(event, "group_mention", conversation, preview)

    title = (event.title or "").strip()
    if not title or title.lower() in GENERIC_WECHAT_TITLES:
        return None
    if body_has_sender_prefix:
        return None
    preview = event.body or event.subtitle or combined
    return ClassifiedAlert(event, "private_message", title, preview)


class DedupeStore:
    def __init__(self, path: Path, ttl_seconds: int) -> None:
        self.path = path
        self.ttl_seconds = ttl_seconds
        self.seen: dict[str, float] = {}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if isinstance(payload, dict):
            raw_seen = payload.get("seen", {})
            if isinstance(raw_seen, dict):
                self.seen = {str(key): float(value) for key, value in raw_seen.items() if isinstance(value, (int, float))}
        self.prune()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"seen": self.seen}, sort_keys=True, indent=2), encoding="utf-8")

    def prune(self) -> None:
        cutoff = _now() - self.ttl_seconds
        self.seen = {key: value for key, value in self.seen.items() if value >= cutoff}

    def key_for(self, alert: ClassifiedAlert) -> str:
        material = "|".join(
            [
                alert.event.event_id,
                alert.event_type,
                alert.conversation,
                alert.preview,
                str(int(alert.event.delivered_at // 60)),
            ]
        )
        return _sha1_text(material)

    def mark_if_new(self, alert: ClassifiedAlert) -> bool:
        self.prune()
        key = self.key_for(alert)
        if key in self.seen:
            return False
        self.seen[key] = _now()
        self.save()
        return True


class ReplyTargetStore:
    def __init__(self, path: Path, ttl_seconds: int) -> None:
        self.path = path
        self.ttl_seconds = ttl_seconds
        self.targets: dict[str, ReplyTarget] = {}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        raw_targets = payload.get("targets", {}) if isinstance(payload, dict) else {}
        if not isinstance(raw_targets, dict):
            return
        targets: dict[str, ReplyTarget] = {}
        for reply_id, raw in raw_targets.items():
            if not isinstance(raw, dict):
                continue
            conversation = str(raw.get("conversation", "")).strip()
            if not conversation:
                continue
            created_at = float(raw.get("created_at", _now()))
            expires_at = float(raw.get("expires_at", created_at + self.ttl_seconds))
            targets[str(reply_id)] = ReplyTarget(
                reply_id=str(reply_id),
                conversation=conversation,
                event_type=str(raw.get("event_type", "")),
                preview=str(raw.get("preview", "")),
                created_at=created_at,
                expires_at=expires_at,
            )
        self.targets = targets
        self.prune()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "targets": {
                reply_id: {
                    "conversation": target.conversation,
                    "event_type": target.event_type,
                    "preview": target.preview,
                    "created_at": target.created_at,
                    "expires_at": target.expires_at,
                }
                for reply_id, target in sorted(self.targets.items())
            }
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")

    def prune(self) -> None:
        now = _now()
        self.targets = {reply_id: target for reply_id, target in self.targets.items() if target.expires_at >= now}

    def reply_id_for(self, alert: ClassifiedAlert) -> str:
        material = "|".join([alert.event.event_id, alert.event_type, alert.conversation, alert.preview])
        return f"wx_{_sha1_text(material)[:10]}"

    def remember(self, alert: ClassifiedAlert) -> ReplyTarget:
        self.prune()
        reply_id = self.reply_id_for(alert)
        created_at = _now()
        target = ReplyTarget(
            reply_id=reply_id,
            conversation=alert.conversation,
            event_type=alert.event_type,
            preview=alert.preview,
            created_at=created_at,
            expires_at=created_at + self.ttl_seconds,
        )
        self.targets[reply_id] = target
        self.save()
        return target

    def get(self, reply_id: str) -> ReplyTarget | None:
        self.prune()
        target = self.targets.get(reply_id)
        if target is None:
            return None
        if target.expires_at < _now():
            self.targets.pop(reply_id, None)
            self.save()
            return None
        return target


class SeaTalkWebhookClient:
    def __init__(self, webhook_url: str, timeout_seconds: float = 10.0) -> None:
        self.webhook_url = webhook_url.strip()
        self.timeout_seconds = timeout_seconds

    def send_text(self, text: str) -> None:
        payload = {"tag": "text", "text": {"content": text[:4000]}}
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            response.read()


def format_alert(alert: ClassifiedAlert, *, include_preview: bool, reply_id: str | None = None) -> str:
    label = "WX @ mention" if alert.event_type == "group_mention" else "WX DM"
    lines = [
        f"{label} | {alert.conversation}",
    ]
    if include_preview and alert.preview:
        lines.extend(["", alert.preview])
    lines.extend(["", f"Time: {_format_local_timestamp(alert.event.delivered_at)}"])
    if reply_id:
        lines.extend(["", f"Reply: /reply {reply_id} "])
    return "\n".join(lines)


def diagnose() -> int:
    paths = discover_notification_databases()
    print(f"WeChat bundle id: {WECHAT_BUNDLE_ID}")
    print(f"Notification databases found: {len(paths)}")
    for path in paths:
        print(f"- {path}")
    if not paths:
        print("No readable macOS notification database was found yet.")
        print("Enable WeChat notifications, generate one test notification, then run --diagnose again.")
    return 0


def run_once(
    reader: NotificationDatabaseReader,
    store: DedupeStore,
    reply_store: ReplyTargetStore,
    client: SeaTalkWebhookClient | None,
    *,
    include_preview: bool,
    since_epoch: float,
    dry_run: bool,
) -> int:
    sent = 0
    for event in reader.read_events(since_epoch):
        alert = classify_wechat_notification(event)
        if not alert:
            continue
        if not store.mark_if_new(alert):
            continue
        reply_target = reply_store.remember(alert)
        text = format_alert(alert, include_preview=include_preview, reply_id=reply_target.reply_id)
        if dry_run or client is None:
            print(text)
            print()
        else:
            client.send_text(text)
            print(f"Forwarded {alert.event_type} from {alert.conversation}", flush=True)
        sent += 1
    return sent


def run_file_fallback_once(fallback: WeChatActivityFileFallback, client: SeaTalkWebhookClient | None, *, dry_run: bool) -> int:
    if not fallback.poll():
        return 0
    text = (
        "[WeChat activity]\n"
        "WeChat local message/session data changed, but macOS notification content was not readable on this system.\n"
        f"Time: {_format_local_timestamp(_now())}"
    )
    if dry_run or client is None:
        print(text)
        print()
    else:
        client.send_text(text)
        print("Forwarded WeChat activity fallback alert")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Forward WeChat macOS notifications to SeaTalk.")
    parser.add_argument("--diagnose", action="store_true", help="List notification sources and exit.")
    parser.add_argument("--once", action="store_true", help="Poll once and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Print alerts instead of sending to SeaTalk.")
    parser.add_argument("--state-path", default=os.environ.get("WECHAT_ALERT_STATE_PATH", DEFAULT_STATE_PATH))
    parser.add_argument("--reply-map-path", default=os.environ.get("WECHAT_REPLY_MAP_PATH", DEFAULT_REPLY_MAP_PATH))
    parser.add_argument("--file-fallback-state", default=os.environ.get("WECHAT_ALERT_FILE_FALLBACK_STATE", DEFAULT_FILE_FALLBACK_STATE))
    parser.add_argument("--lookback-minutes", type=int, default=_int_env("WECHAT_ALERT_LOOKBACK_MINUTES", DEFAULT_NOTIFICATION_LOOKBACK_MINUTES))
    args = parser.parse_args(argv)

    if args.diagnose:
        return diagnose()

    webhook_url = os.environ.get("SEATALK_WEBHOOK_URL", "").strip()
    dry_run = args.dry_run
    if not webhook_url and not dry_run:
        print("SEATALK_WEBHOOK_URL is required unless --dry-run is used.", file=sys.stderr)
        return 2

    poll_seconds = _float_env("WECHAT_ALERT_POLL_SECONDS", DEFAULT_POLL_SECONDS)
    dedupe_minutes = _int_env("WECHAT_ALERT_DEDUPE_MINUTES", DEFAULT_DEDUPE_MINUTES)
    reply_ttl_hours = _int_env("WECHAT_REPLY_TTL_HOURS", DEFAULT_REPLY_TTL_HOURS)
    include_preview = _bool_env("WECHAT_ALERT_INCLUDE_PREVIEW", True)
    file_fallback_enabled = _bool_env("WECHAT_ALERT_FILE_FALLBACK", True)
    state_path = Path(args.state_path).expanduser()
    if not state_path.is_absolute():
        state_path = Path.cwd() / state_path
    reply_map_path = Path(args.reply_map_path).expanduser()
    if not reply_map_path.is_absolute():
        reply_map_path = Path.cwd() / reply_map_path
    file_fallback_state = Path(args.file_fallback_state).expanduser()
    if not file_fallback_state.is_absolute():
        file_fallback_state = Path.cwd() / file_fallback_state

    lookback_seconds = max(args.lookback_minutes, 1) * 60
    dedupe_seconds = max(dedupe_minutes * 60, lookback_seconds + 60)
    store = DedupeStore(state_path, ttl_seconds=dedupe_seconds)
    reply_store = ReplyTargetStore(reply_map_path, ttl_seconds=reply_ttl_hours * 60 * 60)
    file_fallback = WeChatActivityFileFallback(file_fallback_state)
    client = None if dry_run else SeaTalkWebhookClient(webhook_url)
    reader = NotificationDatabaseReader()
    since_epoch = _now() - max(args.lookback_minutes, 1) * 60

    stop = False

    def handle_stop(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    while True:
        try:
            sent = run_once(reader, store, reply_store, client, include_preview=include_preview, since_epoch=since_epoch, dry_run=dry_run)
            if sent == 0 and file_fallback_enabled:
                run_file_fallback_once(file_fallback, client, dry_run=dry_run)
        except urllib.error.URLError as error:
            print(f"SeaTalk webhook send failed: {error}", file=sys.stderr, flush=True)
        except Exception as error:  # noqa: BLE001 - daemon should stay alive after transient macOS DB issues.
            print(f"WeChat notification watcher error: {error}", file=sys.stderr, flush=True)

        if args.once or stop:
            return 0
        since_epoch = _now() - max(args.lookback_minutes, 1) * 60
        time.sleep(poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
