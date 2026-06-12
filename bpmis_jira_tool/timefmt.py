"""Shared timestamp display formatting for the portal UI.

Canonical display format across pages: ``YYYY-MM-DD HH:MM:SS (GMT+8)``
(Singapore time). Accepts ISO-8601 strings (with ``Z``, ``+00:00``, ``+08:00``,
or microseconds) or datetimes; naive inputs are treated as UTC.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

GMT8 = ZoneInfo("Asia/Singapore")
GMT8_DISPLAY = "%Y-%m-%d %H:%M:%S (GMT+8)"


def format_gmt8(value: Any) -> str:
    """Return ``value`` as ``YYYY-MM-DD HH:MM:SS (GMT+8)``; "" if unparseable."""
    if value is None or value == "":
        return ""
    dt: datetime | None = None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return ""
        normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(GMT8).strftime(GMT8_DISPLAY)
