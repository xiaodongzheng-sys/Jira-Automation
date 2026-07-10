from __future__ import annotations

import os
from typing import Any


CODEX_ROUTE_CHEAP = "cheap"
CODEX_ROUTE_BALANCED = "balanced"
CODEX_ROUTE_DEEP = "deep"
CODEX_ROUTE_COMPACT_DEEP = "compact_deep"
CODEX_ROUTE_REPAIR = "repair"

CODEX_MODEL_ROUTE_DEFAULTS = {
    CODEX_ROUTE_CHEAP: "gpt-5.4-mini",
    CODEX_ROUTE_BALANCED: "gpt-5.4",
    CODEX_ROUTE_DEEP: "gpt-5.6",
    CODEX_ROUTE_COMPACT_DEEP: "gpt-5.4",
    CODEX_ROUTE_REPAIR: "gpt-5.5",
}

CODEX_REASONING_ROUTE_DEFAULTS = {
    CODEX_ROUTE_CHEAP: "low",
    CODEX_ROUTE_BALANCED: "medium",
    CODEX_ROUTE_DEEP: "high",
    CODEX_ROUTE_COMPACT_DEEP: "medium",
    CODEX_ROUTE_REPAIR: "high",
}

CODEX_REASONING_EFFORTS = {"low", "medium", "high", "xhigh"}


def _env_value(name: str) -> str:
    return str(os.getenv(name) or "").strip()


def normalize_codex_route(route: str | None) -> str:
    normalized = str(route or CODEX_ROUTE_BALANCED).strip().lower() or CODEX_ROUTE_BALANCED
    return normalized if normalized in CODEX_MODEL_ROUTE_DEFAULTS else CODEX_ROUTE_BALANCED


def codex_route_env_name(route: str, *, prefix: str | None = None) -> str:
    route_name = normalize_codex_route(route).upper()
    base = f"CODEX_MODEL_{route_name}"
    scoped_prefix = str(prefix or "").strip().upper()
    return f"{scoped_prefix}_{base}" if scoped_prefix else base


def codex_reasoning_env_name(route: str, *, prefix: str | None = None) -> str:
    route_name = normalize_codex_route(route).upper()
    base = f"CODEX_REASONING_{route_name}"
    scoped_prefix = str(prefix or "").strip().upper()
    return f"{scoped_prefix}_{base}" if scoped_prefix else base


def resolve_codex_model(
    route: str | None,
    *,
    prefix: str | None = None,
    legacy_env_names: tuple[str, ...] = (),
    explicit_model: str | None = None,
) -> str:
    explicit = str(explicit_model or "").strip()
    if explicit:
        return explicit
    normalized = normalize_codex_route(route)
    env_names: list[str] = []
    if prefix:
        env_names.append(codex_route_env_name(normalized, prefix=prefix))
    env_names.append(codex_route_env_name(normalized))
    env_names.extend(str(name or "").strip() for name in legacy_env_names if str(name or "").strip())
    for name in env_names:
        value = _env_value(name)
        if value:
            return value
    return CODEX_MODEL_ROUTE_DEFAULTS[normalized]


def resolve_codex_reasoning_effort(
    route: str | None,
    *,
    prefix: str | None = None,
    explicit_effort: str | None = None,
) -> str:
    explicit = str(explicit_effort or "").strip().lower()
    if explicit in CODEX_REASONING_EFFORTS:
        return explicit
    normalized = normalize_codex_route(route)
    env_names: list[str] = []
    if prefix:
        env_names.append(codex_reasoning_env_name(normalized, prefix=prefix))
    env_names.append(codex_reasoning_env_name(normalized))
    for name in env_names:
        value = _env_value(name).lower()
        if value in CODEX_REASONING_EFFORTS:
            return value
    return CODEX_REASONING_ROUTE_DEFAULTS[normalized]


def codex_route_policy_payload(
    *,
    prefix: str | None = None,
    legacy_env_names: tuple[str, ...] = (),
) -> dict[str, Any]:
    routes: dict[str, dict[str, Any]] = {}
    for route in CODEX_MODEL_ROUTE_DEFAULTS:
        routes[route] = {
            "model": resolve_codex_model(route, prefix=prefix, legacy_env_names=legacy_env_names),
            "default_model": CODEX_MODEL_ROUTE_DEFAULTS[route],
            "reasoning_effort": resolve_codex_reasoning_effort(route, prefix=prefix),
            "default_reasoning_effort": CODEX_REASONING_ROUTE_DEFAULTS[route],
            "scoped_env": codex_route_env_name(route, prefix=prefix) if prefix else "",
            "global_env": codex_route_env_name(route),
            "scoped_reasoning_env": codex_reasoning_env_name(route, prefix=prefix) if prefix else "",
            "global_reasoning_env": codex_reasoning_env_name(route),
        }
    return {
        "routes": routes,
        "legacy_env_names": list(legacy_env_names),
        "reasoning_control": "codex_cli_model_reasoning_effort",
    }
