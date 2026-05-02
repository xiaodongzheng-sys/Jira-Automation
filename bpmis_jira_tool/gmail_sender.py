from __future__ import annotations

import base64
import json
from email.message import EmailMessage
import mimetypes
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from google.oauth2.credentials import Credentials

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.gmail_dashboard import build_gmail_api_service


GMAIL_SEND_SCOPE = "https://www.googleapis.com/auth/gmail.send"


class StoredGoogleCredentials:
    def __init__(self, storage_path: Path, *, encryption_key: str | None = None) -> None:
        self.storage_path = storage_path
        self.encryption_key = (encryption_key or "").strip()
        self._fernet = Fernet(self.encryption_key.encode("utf-8")) if self.encryption_key else None

    def save(self, *, owner_email: str, credentials_payload: dict[str, Any]) -> None:
        owner = str(owner_email or "").strip().lower()
        if not owner:
            return
        if self._fernet is None:
            return
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._load_raw()
        owners = payload.setdefault("owners", {})
        raw = json.dumps(credentials_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        owners[owner] = self._fernet.encrypt(raw).decode("utf-8")
        temp_path = self.storage_path.with_name(f".{self.storage_path.name}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.storage_path)

    def load(self, *, owner_email: str) -> dict[str, Any]:
        owner = str(owner_email or "").strip().lower()
        if not owner:
            raise ConfigError("Google credential owner email is missing.")
        if self._fernet is None:
            raise ConfigError("TEAM_PORTAL_CONFIG_ENCRYPTION_KEY is required before stored Gmail credentials can be used.")
        payload = self._load_raw()
        owners = payload.get("owners") if isinstance(payload.get("owners"), dict) else {}
        encrypted = str(owners.get(owner) or "").strip()
        if not encrypted:
            raise ConfigError(
                f"Gmail send credentials are not saved for {owner}. Reconnect Google once after gmail.send is enabled."
            )
        try:
            decoded = self._fernet.decrypt(encrypted.encode("utf-8")).decode("utf-8")
            credentials_payload = json.loads(decoded)
        except (InvalidToken, json.JSONDecodeError) as error:
            raise ToolError("Could not decrypt saved Gmail credentials. Check TEAM_PORTAL_CONFIG_ENCRYPTION_KEY.") from error
        if not isinstance(credentials_payload, dict):
            raise ToolError("Saved Gmail credentials are invalid.")
        return credentials_payload

    def _load_raw(self) -> dict[str, Any]:
        if not self.storage_path.exists():
            return {"owners": {}}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"owners": {}}
        return payload if isinstance(payload, dict) else {"owners": {}}


def ensure_gmail_send_scope(credentials_payload: dict[str, Any]) -> None:
    scopes = {str(scope).strip() for scope in (credentials_payload.get("scopes") or []) if str(scope).strip()}
    if GMAIL_SEND_SCOPE not in scopes:
        raise ConfigError("Gmail send permission is missing. Reconnect Google once to grant gmail.send.")


def credentials_from_payload(credentials_payload: dict[str, Any]) -> Credentials:
    ensure_gmail_send_scope(credentials_payload)
    return Credentials(**credentials_payload)


def build_gmail_raw_message(
    *,
    sender: str,
    recipient: str,
    subject: str,
    text_body: str,
    html_body: str | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> str:
    message = EmailMessage()
    message["To"] = recipient
    message["From"] = sender
    message["Subject"] = subject
    message.set_content(text_body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
    for attachment in attachments or []:
        if not isinstance(attachment, dict):
            continue
        filename = Path(str(attachment.get("filename") or "attachment")).name or "attachment"
        content = attachment.get("content")
        if content is None:
            continue
        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
        else:
            content_bytes = bytes(content)
        mime_type = str(attachment.get("mime_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream")
        maintype, _, subtype = mime_type.partition("/")
        message.add_attachment(
            content_bytes,
            maintype=maintype or "application",
            subtype=subtype or "octet-stream",
            filename=filename,
        )
    return base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")


def send_gmail_message(
    *,
    credentials: Credentials,
    sender: str,
    recipient: str,
    subject: str,
    text_body: str,
    html_body: str | None = None,
    gmail_service: Any | None = None,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    service = gmail_service or build_gmail_api_service(credentials)
    raw = build_gmail_raw_message(
        sender=sender,
        recipient=recipient,
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        attachments=attachments,
    )
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()
