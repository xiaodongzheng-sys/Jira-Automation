#!/usr/bin/env bash

if [[ -n "${RELEASE_WINDOW_POLICY_HELPERS_LOADED:-}" ]]; then
  return 0
fi
RELEASE_WINDOW_POLICY_HELPERS_LOADED=1

release_window_policy_json() {
  "$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

timezone_name = os.environ.get("RELEASE_WINDOW_POLICY_TIMEZONE", "Asia/Singapore")
try:
    tz = ZoneInfo(timezone_name)
except Exception as exc:
    print(f"Invalid RELEASE_WINDOW_POLICY_TIMEZONE={timezone_name!r}: {exc}", file=sys.stderr)
    raise SystemExit(1)

fixed_now = os.environ.get("RELEASE_WINDOW_POLICY_NOW", "").strip()
if fixed_now:
    try:
        now = datetime.fromisoformat(fixed_now.replace("Z", "+00:00"))
    except ValueError as exc:
        print(f"Invalid RELEASE_WINDOW_POLICY_NOW={fixed_now!r}: {exc}", file=sys.stderr)
        raise SystemExit(1)
    if now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)
else:
    now = datetime.now(tz)

minutes = now.hour * 60 + now.minute
business_start = 10 * 60
business_end = 19 * 60
is_business_hours = now.weekday() < 5 and business_start <= minutes < business_end
is_weekday = now.weekday() < 5
is_live_window = True
target = "uat_live"
allowed_targets = ["uat", "live"]

print(
    json.dumps(
        {
            "allowed_targets": allowed_targets,
            "policy": "unrestricted",
            "is_live_window": is_live_window,
            "target": target,
            "is_business_hours": is_business_hours,
            "is_weekday": is_weekday,
            "now": now.isoformat(timespec="seconds"),
            "timezone": timezone_name,
            "business_hours": "not enforced",
        },
        sort_keys=True,
    )
)
PY
}

release_window_target() {
  release_window_policy_json | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin)["target"])'
}

release_window_summary() {
  release_window_policy_json | "$PYTHON_BIN" -c 'import json, sys; p=json.load(sys.stdin); print("{} {}; release policy: {}; default target: {}; allowed targets: {}".format(p["now"], p["timezone"], p["policy"], p["target"], ",".join(p["allowed_targets"])))'
}

enforce_release_window_target() {
  local requested_target="$1"
  local policy_json
  policy_json="$(release_window_policy_json)"
  if printf '%s' "$policy_json" | REQUESTED_TARGET="$requested_target" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); raise SystemExit(0 if os.environ["REQUESTED_TARGET"] in p["allowed_targets"] else 1)'; then
    return 0
  fi

  {
    echo "Unknown release target '$requested_target'."
    echo "Allowed targets: uat,live"
  } >&2
  return 1
}
