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
target = "uat" if is_business_hours else "live"

print(
    json.dumps(
        {
            "target": target,
            "is_business_hours": is_business_hours,
            "now": now.isoformat(timespec="seconds"),
            "timezone": timezone_name,
            "business_hours": "Mon-Fri 10:00-19:00",
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
  release_window_policy_json | "$PYTHON_BIN" -c 'import json, sys; p=json.load(sys.stdin); print("{} {}; allowed target: {}; business hours: {}".format(p["now"], p["timezone"], p["target"], p["business_hours"]))'
}

enforce_release_window_target() {
  local requested_target="$1"
  if [[ "${RELEASE_WINDOW_POLICY_BYPASS:-0}" == "1" ]]; then
    echo "Release window policy bypassed for target '$requested_target' because RELEASE_WINDOW_POLICY_BYPASS=1."
    return 0
  fi

  local policy_json allowed_target now timezone business_hours
  policy_json="$(release_window_policy_json)"
  allowed_target="$(printf '%s' "$policy_json" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin)["target"])')"
  if [[ "$requested_target" == "$allowed_target" ]]; then
    return 0
  fi

  now="$(printf '%s' "$policy_json" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin)["now"])')"
  timezone="$(printf '%s' "$policy_json" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin)["timezone"])')"
  business_hours="$(printf '%s' "$policy_json" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin)["business_hours"])')"
  {
    echo "Release window policy blocked '$requested_target' release."
    echo "Current time: $now ($timezone)"
    echo "Allowed target: $allowed_target"
    echo "Business hours: $business_hours"
    echo "Policy: business hours publish UAT only; outside business hours publish Live only."
    echo "Set RELEASE_WINDOW_POLICY_BYPASS=1 only for an explicitly approved exception."
  } >&2
  return 1
}
