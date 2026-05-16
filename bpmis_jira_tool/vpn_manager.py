from __future__ import annotations

import re
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from bpmis_jira_tool.errors import ToolError


DEFAULT_CISCO_VPN_BIN = "/opt/cisco/secureclient/bin/vpn"
_ENCRYPTED_PREFIX = "enc:"


class VPNProfileStore:
    def __init__(self, db_path: Path, *, encryption_key: str | None = None) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._fernet = Fernet(encryption_key.encode("utf-8")) if str(encryption_key or "").strip() else None
        self._ensure_db()

    def list_profiles(self) -> list[dict[str, Any]]:
        with sqlite3.connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, display_name, vpn_host, username, updated_at, last_connected_at
                FROM vpn_profiles
                ORDER BY LOWER(display_name), LOWER(vpn_host)
                """
            ).fetchall()
        return [
            {
                "id": row[0],
                "display_name": row[1],
                "vpn_host": row[2],
                "username": row[3],
                "updated_at": row[4],
                "last_connected_at": row[5],
                "has_password": True,
            }
            for row in rows
        ]

    def save_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw_profile_id = str(payload.get("id") or "").strip()
        profile_id = self._normalize_id(raw_profile_id) if raw_profile_id else ""
        display_name = str(payload.get("display_name") or "").strip()
        vpn_host = str(payload.get("vpn_host") or "").strip()
        username = str(payload.get("username") or "").strip()
        password = str(payload.get("password") or "")
        if not display_name:
            raise ToolError("VPN display name is required.")
        if not vpn_host:
            raise ToolError("Cisco VPN host/profile name is required.")
        if not username:
            raise ToolError("VPN username is required.")

        existing_password = self._encrypted_password_for_profile(profile_id) if profile_id else ""
        password_encrypted = existing_password
        if password:
            password_encrypted = self._encrypt_secret(password)
        elif not password_encrypted:
            raise ToolError("VPN password is required for a new profile.")

        if not profile_id:
            profile_id = self._new_profile_id(display_name, vpn_host)

        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO vpn_profiles (
                    id, display_name, vpn_host, username, password_encrypted, updated_at
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    display_name = excluded.display_name,
                    vpn_host = excluded.vpn_host,
                    username = excluded.username,
                    password_encrypted = excluded.password_encrypted,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (profile_id, display_name, vpn_host, username, password_encrypted),
            )
            connection.commit()
        return self.get_profile(profile_id, include_password=False)

    def get_profile(self, profile_id: str, *, include_password: bool = False) -> dict[str, Any]:
        normalized_id = self._normalize_id(profile_id)
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT id, display_name, vpn_host, username, password_encrypted, updated_at, last_connected_at
                FROM vpn_profiles
                WHERE id = ?
                """,
                (normalized_id,),
            ).fetchone()
        if row is None:
            raise ToolError("VPN profile was not found.")
        profile = {
            "id": row[0],
            "display_name": row[1],
            "vpn_host": row[2],
            "username": row[3],
            "updated_at": row[5],
            "last_connected_at": row[6],
            "has_password": bool(row[4]),
        }
        if include_password:
            profile["password"] = self._decrypt_secret(str(row[4] or ""))
        return profile

    def delete_profile(self, profile_id: str) -> None:
        normalized_id = self._normalize_id(profile_id)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("DELETE FROM vpn_profiles WHERE id = ?", (normalized_id,))
            connection.commit()

    def record_connected(self, profile_id: str) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "UPDATE vpn_profiles SET last_connected_at = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), self._normalize_id(profile_id)),
            )
            connection.commit()

    def _ensure_db(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS vpn_profiles (
                    id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    vpn_host TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password_encrypted TEXT NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_connected_at TEXT
                )
                """
            )
            connection.commit()

    def _encrypted_password_for_profile(self, profile_id: str) -> str:
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT password_encrypted FROM vpn_profiles WHERE id = ?",
                (self._normalize_id(profile_id),),
            ).fetchone()
        return str(row[0] or "") if row else ""

    def _encrypt_secret(self, value: str) -> str:
        if self._fernet is None:
            raise ToolError("TEAM_PORTAL_CONFIG_ENCRYPTION_KEY is required before saving VPN passwords.")
        token = self._fernet.encrypt(value.encode("utf-8")).decode("utf-8")
        return f"{_ENCRYPTED_PREFIX}{token}"

    def _decrypt_secret(self, value: str) -> str:
        if not value.startswith(_ENCRYPTED_PREFIX):
            raise ToolError("Saved VPN password is not encrypted. Re-save the profile with encryption enabled.")
        if self._fernet is None:
            raise ToolError("TEAM_PORTAL_CONFIG_ENCRYPTION_KEY is required to read saved VPN passwords.")
        try:
            return self._fernet.decrypt(value[len(_ENCRYPTED_PREFIX) :].encode("utf-8")).decode("utf-8")
        except InvalidToken as error:
            raise ToolError("Could not decrypt the saved VPN password. Check TEAM_PORTAL_CONFIG_ENCRYPTION_KEY.") from error

    @staticmethod
    def _new_profile_id(display_name: str, vpn_host: str) -> str:
        seed = f"{display_name}-{vpn_host}".strip().lower()
        base = re.sub(r"[^a-z0-9]+", "-", seed).strip("-")[:48] or "vpn-profile"
        suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        return f"{base}-{suffix}"

    @staticmethod
    def _normalize_id(value: Any) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ToolError("VPN profile id is required.")
        if not re.fullmatch(r"[A-Za-z0-9._:-]+", normalized):
            raise ToolError("VPN profile id is invalid.")
        return normalized


class CiscoVPNClient:
    def __init__(self, *, vpn_bin: str = DEFAULT_CISCO_VPN_BIN, timeout_seconds: int = 120) -> None:
        self.vpn_bin = str(vpn_bin or DEFAULT_CISCO_VPN_BIN)
        self.timeout_seconds = max(10, int(timeout_seconds or 120))

    def status(self) -> dict[str, Any]:
        completed = self._run(["state"], timeout_seconds=20)
        output = self._combined_output(completed)
        return {
            "status": "ok" if completed.returncode == 0 else "error",
            "connected": "state: connected" in output.lower(),
            "state": self._parse_state(output),
            "message": self._sanitize_output(output),
        }

    def hosts(self) -> list[str]:
        completed = self._run(["hosts"], timeout_seconds=20)
        if completed.returncode != 0:
            raise ToolError(self._sanitize_output(self._combined_output(completed)) or "Could not read Cisco VPN hosts.")
        hosts: list[str] = []
        for raw_line in self._combined_output(completed).splitlines():
            line = raw_line.strip()
            if line.startswith(">"):
                host = line[1:].strip()
                if host:
                    hosts.append(host)
        return hosts

    def connect(self, *, host: str, username: str, password: str) -> dict[str, Any]:
        if not host.strip():
            raise ToolError("Cisco VPN host/profile name is required.")
        if not username.strip():
            raise ToolError("VPN username is required.")
        if not password:
            raise ToolError("VPN password is required.")
        response_text = "\n".join([username, password, "y", ""])
        completed = self._run(["-s", "connect", host], input_text=response_text, timeout_seconds=self.timeout_seconds)
        output = self._sanitize_output(self._combined_output(completed), secrets=[username, password])
        connected = completed.returncode == 0 and "state: connected" in output.lower()
        if completed.returncode != 0 and not connected:
            raise ToolError(output or "Cisco Secure Client failed to connect.")
        return {
            "status": "ok",
            "connected": connected,
            "state": self._parse_state(output),
            "message": output,
        }

    def disconnect(self) -> dict[str, Any]:
        completed = self._run(["disconnect"], timeout_seconds=60)
        output = self._sanitize_output(self._combined_output(completed))
        if completed.returncode != 0:
            raise ToolError(output or "Cisco Secure Client failed to disconnect.")
        return {
            "status": "ok",
            "connected": False,
            "state": self._parse_state(output) or "Disconnected",
            "message": output,
        }

    def _run(
        self,
        args: list[str],
        *,
        input_text: str | None = None,
        timeout_seconds: int | None = None,
    ) -> subprocess.CompletedProcess[str]:
        vpn_path = Path(self.vpn_bin)
        if not vpn_path.exists():
            raise ToolError(f"Cisco Secure Client CLI was not found at {self.vpn_bin}.")
        try:
            return subprocess.run(
                [self.vpn_bin, *args],
                input=input_text,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds or self.timeout_seconds,
            )
        except subprocess.TimeoutExpired as error:
            raise ToolError("Cisco Secure Client command timed out.") from error
        except OSError as error:
            raise ToolError(f"Cisco Secure Client command failed: {error}") from error

    @staticmethod
    def _combined_output(completed: subprocess.CompletedProcess[str]) -> str:
        return "\n".join(part for part in [completed.stdout, completed.stderr] if part).strip()

    @staticmethod
    def _parse_state(output: str) -> str:
        matches = re.findall(r"state:\s*([^\r\n]+)", output or "", flags=re.IGNORECASE)
        return matches[-1].strip() if matches else ""

    @staticmethod
    def _sanitize_output(output: str, *, secrets: list[str] | None = None) -> str:
        sanitized = output.replace("\r", "\n")
        for secret in secrets or []:
            if secret:
                sanitized = sanitized.replace(secret, "[redacted]")
        return sanitized.strip()


def vpn_payload(profile: dict[str, Any], *, status: dict[str, Any] | None = None, hosts: list[str] | None = None) -> dict[str, Any]:
    return {
        "profile": {key: value for key, value in profile.items() if key != "password"},
        "status": status or {},
        "hosts": hosts or [],
    }


def json_response_payload(*, profiles: list[dict[str, Any]], status: dict[str, Any], hosts: list[str]) -> dict[str, Any]:
    return {"status": "ok", "profiles": profiles, "vpn_status": status, "hosts": hosts}
