from __future__ import annotations

import base64
import hashlib
import hmac
import time
import uuid

from bpmis_jira_tool.errors import ToolError


SIGNATURE_HEADER = "X-Local-Agent-Signature"
TIMESTAMP_HEADER = "X-Local-Agent-Timestamp"
NONCE_HEADER = "X-Local-Agent-Nonce"


def sign_headers(*, secret: str, method: str, path: str, body: bytes) -> dict[str, str]:
    timestamp = str(int(time.time()))
    nonce = uuid.uuid4().hex
    signature = _signature(secret=secret, method=method, path=path, timestamp=timestamp, nonce=nonce, body=body)
    return {
        TIMESTAMP_HEADER: timestamp,
        NONCE_HEADER: nonce,
        SIGNATURE_HEADER: signature,
    }


def verify_signature(
    *,
    secret: str,
    method: str,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
    signature: str,
    max_skew_seconds: int = 300,
) -> None:
    if not secret:
        raise ToolError("LOCAL_AGENT_HMAC_SECRET is required for local-agent requests.")
    try:
        timestamp_value = int(str(timestamp or "").strip())
    except ValueError as error:
        raise ToolError("Invalid local-agent timestamp.") from error
    if abs(int(time.time()) - timestamp_value) > max_skew_seconds:
        raise ToolError("Local-agent request timestamp is outside the allowed window.")
    if not str(nonce or "").strip():
        raise ToolError("Missing local-agent nonce.")
    expected = _signature(secret=secret, method=method, path=path, timestamp=str(timestamp_value), nonce=nonce, body=body)
    if not hmac.compare_digest(expected, str(signature or "")):
        raise ToolError("Invalid local-agent signature.")


def _signature(*, secret: str, method: str, path: str, timestamp: str, nonce: str, body: bytes) -> str:
    body_hash = hashlib.sha256(body or b"").hexdigest()
    message = "\n".join([method.upper(), path, timestamp, nonce, body_hash]).encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")
