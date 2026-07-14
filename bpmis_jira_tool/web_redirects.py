from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit


def _portal_health_path(parsed) -> str:
    hostname = (parsed.hostname or "").strip().lower()
    if hostname.endswith(".run.app"):
        return "/cloud-healthz"
    return "/healthz"


def safe_relative_redirect_target(value: Any) -> str:
    target = str(value or "").strip()
    if not target or not target.startswith("/") or target.startswith("//"):
        return ""
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return ""
    return target


def url_with_query_value(url: str, key: str, value: str) -> str:
    parsed = urlsplit(str(url or "").strip())
    query_items = [(item_key, item_value) for item_key, item_value in parse_qsl(parsed.query, keep_blank_values=True) if item_key != key]
    query_items.append((key, value))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_items), parsed.fragment))


def portal_health_url(target_url: str) -> str:
    parsed = urlsplit(str(target_url or "").strip())
    if parsed.scheme and parsed.netloc:
        return urlunsplit((parsed.scheme, parsed.netloc, _portal_health_path(parsed), "", ""))
    return ""
