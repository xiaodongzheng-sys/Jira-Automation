#!/usr/bin/env python3
"""Receive SeaTalk bot replies and send them through WeChat Desktop."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    from wechat_notification_to_seatalk import (
        DEFAULT_REPLY_MAP_PATH,
        DEFAULT_REPLY_SERVER_HOST,
        DEFAULT_REPLY_SERVER_PORT,
        DEFAULT_REPLY_TTL_HOURS,
        ReplyTarget,
        ReplyTargetStore,
        SeaTalkWebhookClient,
        _bool_env,
        _int_env,
    )
except ModuleNotFoundError:
    from scripts.wechat_notification_to_seatalk import (
        DEFAULT_REPLY_MAP_PATH,
        DEFAULT_REPLY_SERVER_HOST,
        DEFAULT_REPLY_SERVER_PORT,
        DEFAULT_REPLY_TTL_HOURS,
        ReplyTarget,
        ReplyTargetStore,
        SeaTalkWebhookClient,
        _bool_env,
        _int_env,
    )


REPLY_COMMAND_RE = re.compile(r"(?:^|\s)/(?:reply|r)\s+(wx_[a-f0-9]{6,32})\s+(.+)$", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class ParsedSeaTalkReply:
    reply_id: str
    message: str
    sender: str
    raw_text: str


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _walk_strings(value: Any) -> list[str]:
    strings: list[str] = []
    if isinstance(value, dict):
        for item in value.values():
            strings.extend(_walk_strings(item))
    elif isinstance(value, list):
        for item in value:
            strings.extend(_walk_strings(item))
    elif isinstance(value, str):
        text = value.strip()
        if text:
            strings.append(text)
    return strings


def extract_text(payload: dict[str, Any]) -> str:
    preferred_keys = ("plain_text", "content", "text", "message", "msg", "body")

    def find(value: Any) -> str:
        if isinstance(value, dict):
            for key in preferred_keys:
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
                if isinstance(candidate, dict):
                    nested = find(candidate)
                    if nested:
                        return nested
            for item in value.values():
                nested = find(item)
                if nested:
                    return nested
        elif isinstance(value, list):
            for item in value:
                nested = find(item)
                if nested:
                    return nested
        return ""

    text = find(payload)
    if text:
        return text
    for value in _walk_strings(payload):
        if "/reply " in value or "/r " in value:
            return value
    return ""


def extract_sender(payload: dict[str, Any]) -> str:
    sender_keys = ("email", "sender_email", "user_email", "employee_email", "open_id", "sender_id", "user_id")

    def find(value: Any) -> str:
        if isinstance(value, dict):
            for key in sender_keys:
                candidate = value.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
            for item in value.values():
                nested = find(item)
                if nested:
                    return nested
        elif isinstance(value, list):
            for item in value:
                nested = find(item)
                if nested:
                    return nested
        return ""

    return find(payload)


def extract_seatalk_challenge(payload: dict[str, Any]) -> str:
    def find(value: Any) -> str:
        if isinstance(value, dict):
            candidate = value.get("seatalk_challenge")
            if isinstance(candidate, str) and candidate:
                return candidate
            for item in value.values():
                nested = find(item)
                if nested:
                    return nested
        elif isinstance(value, list):
            for item in value:
                nested = find(item)
                if nested:
                    return nested
        return ""

    return find(payload)


def parse_reply_payload(payload: dict[str, Any]) -> ParsedSeaTalkReply | None:
    text = extract_text(payload)
    if not text:
        return None
    match = REPLY_COMMAND_RE.search(text.strip())
    if not match:
        return None
    return ParsedSeaTalkReply(
        reply_id=match.group(1),
        message=match.group(2).strip(),
        sender=extract_sender(payload),
        raw_text=text,
    )


class WeChatDesktopSender:
    def __init__(
        self,
        *,
        dry_run: bool = False,
        restore_front_app: bool = True,
        hide_wechat_after_send: bool = False,
    ) -> None:
        self.dry_run = dry_run
        self.restore_front_app = restore_front_app
        self.hide_wechat_after_send = hide_wechat_after_send

    def send(self, conversation: str, message: str) -> None:
        if self.dry_run:
            print(f"[dry-run] Would send to WeChat conversation {conversation!r}: {message}")
            return
        previous_app = self._frontmost_application_name() if self.restore_front_app else ""
        old_clipboard = self._get_clipboard()
        try:
            self._set_clipboard(conversation)
            completed = subprocess.run(["osascript", "-e", self._select_conversation_script()], capture_output=True, text=True, timeout=30)
            if completed.returncode != 0:
                detail = completed.stderr.strip() or completed.stdout.strip() or "unknown osascript failure"
                raise RuntimeError(detail)
            self._set_clipboard(message)
            completed = subprocess.run(["osascript", "-e", self._paste_and_send_script()], capture_output=True, text=True, timeout=30)
            if completed.returncode != 0:
                detail = completed.stderr.strip() or completed.stdout.strip() or "unknown osascript failure"
                raise RuntimeError(detail)
        finally:
            time.sleep(0.2)
            self._set_clipboard(old_clipboard)
            self._restore_desktop(previous_app)

    @staticmethod
    def _get_clipboard() -> str:
        completed = subprocess.run(["pbpaste"], capture_output=True, text=True, timeout=5)
        if completed.returncode != 0:
            return ""
        return completed.stdout

    @staticmethod
    def _set_clipboard(value: str) -> None:
        subprocess.run(["pbcopy"], input=value, text=True, check=True, timeout=5)

    @staticmethod
    def _frontmost_application_name() -> str:
        script = '''
tell application "System Events"
  set frontApp to first application process whose frontmost is true
  return name of frontApp
end tell
'''
        completed = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=5)
        if completed.returncode != 0:
            return ""
        return completed.stdout.strip()

    def _restore_desktop(self, previous_app: str) -> None:
        if self.hide_wechat_after_send:
            subprocess.run(
                [
                    "osascript",
                    "-e",
                    'tell application "System Events" to if exists process "WeChat" then set visible of process "WeChat" to false',
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
        if previous_app and previous_app != "WeChat":
            script = '''
on run argv
  tell application (item 1 of argv) to activate
end run
'''
            subprocess.run(["osascript", "-e", script, previous_app], capture_output=True, text=True, timeout=5)

    @staticmethod
    def _select_conversation_script() -> str:
        return '''
tell application "WeChat" to activate
delay 0.8
tell application "System Events"
  tell process "WeChat"
    set frontmost to true
    keystroke "f" using {{command down}}
    delay 0.4
    keystroke "v" using {{command down}}
    delay 0.8
    key code 36
    delay 0.8
  end tell
end tell
'''

    @staticmethod
    def _paste_and_send_script() -> str:
        return '''
tell application "System Events"
  tell process "WeChat"
    keystroke "v" using {{command down}}
    delay 0.2
    key code 36
  end tell
end tell
'''


class ReplyServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        reply_store: ReplyTargetStore,
        sender: WeChatDesktopSender,
        callback_token: str,
        allowed_senders: set[str],
        confirmation_webhook_url: str,
    ) -> None:
        self.host = host
        self.port = port
        self.reply_store = reply_store
        self.sender = sender
        self.callback_token = callback_token
        self.allowed_senders = allowed_senders
        self.confirmation_client = SeaTalkWebhookClient(confirmation_webhook_url) if confirmation_webhook_url else None

    def serve_forever(self) -> None:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                print(f"{self.address_string()} - {format % args}")

            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path == "/healthz":
                    _json_response(self, 200, {"ok": True})
                    return
                _json_response(self, 404, {"ok": False, "error": "not_found"})

            def do_POST(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path not in {"/seatalk/wechat-reply", "/seatalk/callback"}:
                    _json_response(self, 404, {"ok": False, "error": "not_found"})
                    return
                if outer.callback_token:
                    query_token = parse_qs(parsed.query).get("token", [""])[0]
                    header_token = self.headers.get("X-WeChat-Reply-Token", "")
                    if outer.callback_token not in {query_token, header_token}:
                        _json_response(self, 403, {"ok": False, "error": "invalid_token"})
                        return
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length)
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    _json_response(self, 400, {"ok": False, "error": "invalid_json"})
                    return
                if isinstance(payload, dict):
                    challenge = extract_seatalk_challenge(payload)
                    if challenge:
                        body = json.dumps({"seatalk_challenge": challenge}, ensure_ascii=False).encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                parsed_reply = parse_reply_payload(payload if isinstance(payload, dict) else {})
                if parsed_reply is None:
                    _json_response(self, 200, {"ok": True, "ignored": True, "reason": "not_reply_command"})
                    return
                sender = parsed_reply.sender.lower()
                if outer.allowed_senders and sender not in outer.allowed_senders:
                    _json_response(self, 403, {"ok": False, "error": "sender_not_allowed", "sender": parsed_reply.sender})
                    return
                target = outer.reply_store.get(parsed_reply.reply_id)
                if target is None:
                    _json_response(self, 404, {"ok": False, "error": "reply_id_not_found", "reply_id": parsed_reply.reply_id})
                    return
                try:
                    outer.sender.send(target.conversation, parsed_reply.message)
                except Exception as error:  # noqa: BLE001 - preserve HTTP response with clear failure.
                    _json_response(self, 500, {"ok": False, "error": "wechat_send_failed", "detail": str(error)})
                    return
                outer._confirm_sent(target, parsed_reply.message)
                _json_response(self, 200, {"ok": True, "reply_id": parsed_reply.reply_id, "conversation": target.conversation})

        httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        print(f"SeaTalk reply callback server listening on http://{self.host}:{self.port}/seatalk/wechat-reply")
        httpd.serve_forever()

    def _confirm_sent(self, target: ReplyTarget, message: str) -> None:
        if not self.confirmation_client:
            return
        with contextlib.suppress(Exception):
            self.confirmation_client.send_text(f"[WeChat reply sent]\nConversation: {target.conversation}\nReply ID: {target.reply_id}\nMessage: {message}")


def _allowed_senders_from_env() -> set[str]:
    raw = os.environ.get("WECHAT_REPLY_ALLOWED_SENDERS", "")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Receive SeaTalk bot replies and send them through WeChat Desktop.")
    parser.add_argument("--host", default=os.environ.get("WECHAT_REPLY_SERVER_HOST", DEFAULT_REPLY_SERVER_HOST))
    parser.add_argument("--port", type=int, default=_int_env("WECHAT_REPLY_SERVER_PORT", DEFAULT_REPLY_SERVER_PORT))
    parser.add_argument("--reply-map-path", default=os.environ.get("WECHAT_REPLY_MAP_PATH", DEFAULT_REPLY_MAP_PATH))
    parser.add_argument("--dry-run", action="store_true", default=_bool_env("WECHAT_REPLY_DRY_RUN", False))
    parser.add_argument("--no-restore-front-app", action="store_true", help="Leave WeChat in front after sending.")
    parser.add_argument("--hide-wechat-after-send", action="store_true", default=_bool_env("WECHAT_REPLY_HIDE_WECHAT_AFTER_SEND", False))
    parser.add_argument("--send-test", nargs=2, metavar=("CONVERSATION", "MESSAGE"), help="Send one test WeChat message and exit.")
    args = parser.parse_args(argv)

    if args.send_test:
        WeChatDesktopSender(
            dry_run=args.dry_run,
            restore_front_app=not args.no_restore_front_app,
            hide_wechat_after_send=args.hide_wechat_after_send,
        ).send(args.send_test[0], args.send_test[1])
        return 0

    reply_map_path = Path(args.reply_map_path).expanduser()
    if not reply_map_path.is_absolute():
        reply_map_path = Path.cwd() / reply_map_path
    store = ReplyTargetStore(reply_map_path, ttl_seconds=_int_env("WECHAT_REPLY_TTL_HOURS", DEFAULT_REPLY_TTL_HOURS) * 60 * 60)
    server = ReplyServer(
        host=args.host,
        port=args.port,
        reply_store=store,
        sender=WeChatDesktopSender(
            dry_run=args.dry_run,
            restore_front_app=not args.no_restore_front_app,
            hide_wechat_after_send=args.hide_wechat_after_send,
        ),
        callback_token=os.environ.get("WECHAT_REPLY_CALLBACK_TOKEN", "").strip(),
        allowed_senders=_allowed_senders_from_env(),
        confirmation_webhook_url=os.environ.get("SEATALK_WEBHOOK_URL", "").strip(),
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
