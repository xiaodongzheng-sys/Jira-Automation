from __future__ import annotations

import os
from typing import Any, Callable


def build_health_payload(current_revision: Callable[[], str], *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    env = environ if environ is not None else os.environ
    payload: dict[str, Any] = {
        "status": "ok",
        "revision": current_revision(),
        "live_surface": env.get("TEAM_PORTAL_LIVE_SURFACE") or "mac_public_live",
    }
    manifest_id = str(env.get("TEAM_PORTAL_RELEASE_MANIFEST_ID") or "").strip()
    if manifest_id:
        payload["release_manifest_id"] = manifest_id
    return payload
