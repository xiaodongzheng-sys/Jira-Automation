from __future__ import annotations

import re
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from cryptography.fernet import Fernet, InvalidToken

from bpmis_jira_tool.errors import ToolError


DEFAULT_CISCO_VPN_BIN = "/opt/cisco/secureclient/bin/vpn"
DEFAULT_CISCO_APP_PATH = "/Applications/Cisco/Cisco Secure Client.app"
_ENCRYPTED_PREFIX = "enc:"
_CISCO_GUI_CAPABILITY_ERROR = "connect capability is unavailable"


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
    def __init__(
        self,
        *,
        vpn_bin: str = DEFAULT_CISCO_VPN_BIN,
        cisco_app_path: str = DEFAULT_CISCO_APP_PATH,
        timeout_seconds: int = 120,
        process_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        app_restarter: Callable[[], None] | None = None,
        sleeper: Callable[[float], None] | None = None,
        poll_interval_seconds: float = 2.0,
        connect_verify_timeout_seconds: float | None = None,
    ) -> None:
        self.vpn_bin = str(vpn_bin or DEFAULT_CISCO_VPN_BIN)
        self.cisco_app_path = str(cisco_app_path or DEFAULT_CISCO_APP_PATH)
        self.timeout_seconds = max(10, int(timeout_seconds or 120))
        self._process_runner = process_runner or subprocess.run
        self._app_restarter = app_restarter
        self._sleeper = sleeper or time.sleep
        self.poll_interval_seconds = max(0.05, float(poll_interval_seconds or 2.0))
        self.connect_verify_timeout_seconds = (
            max(0.1, float(connect_verify_timeout_seconds))
            if connect_verify_timeout_seconds is not None
            else float(self.timeout_seconds)
        )

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

    def connect(self, *, host: str, username: str, password: str, second_password: str = "") -> dict[str, Any]:
        if not host.strip():
            raise ToolError("Cisco VPN host/profile name is required.")
        if not username.strip():
            raise ToolError("VPN username is required.")
        if not password:
            raise ToolError("VPN password is required.")
        responses = [username, password]
        if second_password:
            responses.append(second_password)
        responses.extend(["y", ""])
        response_text = "\n".join(responses)
        last_output = ""
        restarted_gui = False
        for attempt in range(2):
            completed = self._run(["-s", "connect", host], input_text=response_text, timeout_seconds=self.timeout_seconds)
            output = self._sanitize_output(self._combined_output(completed), secrets=[username, password, second_password])
            last_output = self._append_output(last_output, output)
            if self._is_gui_capability_error(output) and attempt == 0:
                self._restart_cisco_gui()
                restarted_gui = True
                last_output = self._append_output(last_output, "Restarted Cisco Secure Client and retried the VPN connection.")
                continue
            if completed.returncode != 0:
                raise ToolError(self._connection_failure_message(last_output))
            verified = self._wait_for_connected(secrets=[username, password, second_password], previous_output=last_output)
            if verified.get("connected"):
                message = self._append_output(last_output, str(verified.get("message") or ""))
                return {
                    "status": "ok",
                    "connected": True,
                    "state": verified.get("state") or "Connected",
                    "message": message,
                }
            last_output = self._append_output(last_output, str(verified.get("message") or ""))
            if self._is_gui_capability_error(last_output) and attempt == 0:
                self._restart_cisco_gui()
                restarted_gui = True
                last_output = self._append_output(last_output, "Restarted Cisco Secure Client and retried the VPN connection.")
                continue
            break
        if restarted_gui:
            last_output = self._append_output(last_output, "Cisco Secure Client was restarted once, but VPN still did not connect.")
        raise ToolError(self._connection_failure_message(last_output))

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
            return self._process_runner(
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
        matches = re.findall(r"(?:state|connection state):\s*([^\r\n]+)", output or "", flags=re.IGNORECASE)
        return matches[-1].strip() if matches else ""

    @staticmethod
    def _sanitize_output(output: str, *, secrets: list[str] | None = None) -> str:
        sanitized = output.replace("\r", "\n")
        for secret in secrets or []:
            if secret:
                sanitized = sanitized.replace(secret, "[redacted]")
        return sanitized.strip()

    def _wait_for_connected(self, *, secrets: list[str], previous_output: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.connect_verify_timeout_seconds
        last_status = self._status_from_output(previous_output)
        while time.monotonic() <= deadline:
            for status in (self._poll_status(secrets=secrets), self._poll_stats(secrets=secrets)):
                if status.get("message"):
                    last_status = status
                if status.get("connected"):
                    return status
            self._sleeper(self.poll_interval_seconds)
        if not last_status.get("message"):
            last_status["message"] = "Cisco Secure Client did not report a VPN connection before the timeout."
        return last_status

    def _poll_status(self, *, secrets: list[str]) -> dict[str, Any]:
        completed = self._run(["state"], timeout_seconds=20)
        output = self._sanitize_output(self._combined_output(completed), secrets=secrets)
        return self._status_from_output(output, returncode=completed.returncode)

    def _poll_stats(self, *, secrets: list[str]) -> dict[str, Any]:
        completed = self._run(["stats"], timeout_seconds=20)
        output = self._sanitize_output(self._combined_output(completed), secrets=secrets)
        return self._status_from_output(output, returncode=completed.returncode)

    def _status_from_output(self, output: str, *, returncode: int = 0) -> dict[str, Any]:
        state = self._parse_state(output)
        connected = state.strip().lower() == "connected"
        return {
            "status": "ok" if returncode == 0 else "error",
            "connected": connected,
            "state": state,
            "message": output,
        }

    def _restart_cisco_gui(self) -> None:
        if self._app_restarter is not None:
            self._app_restarter()
            return
        subprocess.run(
            ["osascript", "-e", 'tell application "Cisco Secure Client" to quit'],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if not self._wait_for_cisco_gui_exit(timeout_seconds=8.0):
            subprocess.run(["pkill", "-x", "Cisco Secure Client"], capture_output=True, text=True, check=False, timeout=10)
            self._wait_for_cisco_gui_exit(timeout_seconds=5.0)

    def _wait_for_cisco_gui_exit(self, *, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + max(0.1, timeout_seconds)
        while time.monotonic() <= deadline:
            completed = subprocess.run(
                ["pgrep", "-x", "Cisco Secure Client"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
            )
            if completed.returncode != 0:
                return True
            self._sleeper(0.5)
        return False

    @staticmethod
    def _is_gui_capability_error(output: str) -> bool:
        return _CISCO_GUI_CAPABILITY_ERROR in str(output or "").casefold()

    @staticmethod
    def _append_output(existing: str, addition: str) -> str:
        parts = [part.strip() for part in (existing, addition) if str(part or "").strip()]
        return "\n\n".join(parts)

    def _connection_failure_message(self, output: str) -> str:
        state = self._parse_state(output) or "Unknown"
        detail = output.strip()
        if detail:
            return f"Cisco Secure Client did not reach Connected state. Last state: {state}.\n\n{detail}"
        return f"Cisco Secure Client did not reach Connected state. Last state: {state}."


def vpn_payload(profile: dict[str, Any], *, status: dict[str, Any] | None = None, hosts: list[str] | None = None) -> dict[str, Any]:
    return {
        "profile": {key: value for key, value in profile.items() if key != "password"},
        "status": status or {},
        "hosts": hosts or [],
    }


def json_response_payload(*, profiles: list[dict[str, Any]], status: dict[str, Any], hosts: list[str]) -> dict[str, Any]:
    return {"status": "ok", "profiles": profiles, "vpn_status": status, "hosts": hosts}
