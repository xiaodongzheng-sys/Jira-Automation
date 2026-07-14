#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

STARTED_AT="$(date +%s)"
HOST_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"

record_live_timing() {
  local status=$?
  local finished_at
  finished_at="$(date +%s)"
  record_deploy_timing "promote_live.sh" "script" "$STARTED_AT" "$finished_at" "$status" "host=$HOST_ROOT" || true
  return "$status"
}
trap record_live_timing EXIT

read_healthz_revision() {
  local url="$1"
  curl -fsS --max-time 10 "$url" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin).get("revision", ""))'
}

wait_for_revision() {
  local label="$1"
  local url="$2"
  local expected="$3"
  local timeout_seconds="${4:-60}"
  local deadline=$(( $(date +%s) + timeout_seconds ))
  local served=""
  echo "Verifying $label at $url for up to ${timeout_seconds}s."
  while (( $(date +%s) < deadline )); do
    served="$(read_healthz_revision "$url" 2>/dev/null || true)"
    if [[ "$served" == "$expected" ]]; then
      return 0
    fi
    sleep 3
  done
  echo "$label did not serve expected revision." >&2
  echo "Served:   ${served:-<none>}" >&2
  echo "Expected: $expected" >&2
  return 1
}

if [[ ! -d "$HOST_ROOT/.git" ]]; then
  echo "Live host workspace is missing or is not a git checkout: $HOST_ROOT" >&2
  exit 1
fi

git -C "$ROOT_DIR" fetch origin >/dev/null
TARGET_COMMIT="$(git -C "$ROOT_DIR" rev-parse origin/main)"
git -C "$HOST_ROOT" fetch origin >/dev/null

if ! git -C "$HOST_ROOT" diff --quiet --no-ext-diff --exit-code || ! git -C "$HOST_ROOT" diff --cached --quiet --no-ext-diff --exit-code; then
  echo "Live host workspace has uncommitted changes. Clean or stash them before promotion." >&2
  exit 1
fi

PREVIOUS_HEAD="$(git -C "$HOST_ROOT" rev-parse HEAD)"
git -C "$HOST_ROOT" checkout main >/dev/null
git -C "$HOST_ROOT" pull --ff-only origin main

if [[ "$(git -C "$HOST_ROOT" rev-parse HEAD)" != "$TARGET_COMMIT" ]]; then
  echo "Live host workspace did not reach origin/main $TARGET_COMMIT." >&2
  exit 1
fi

echo "Promoting origin/main $TARGET_COMMIT to Mac-hosted Live."
echo "Previous host revision: $PREVIOUS_HEAD"
echo "Live host workspace: $HOST_ROOT"

if [[ -x "$HOST_ROOT/scripts/run_local_agent.sh" ]]; then
  "$HOST_ROOT/scripts/run_local_agent.sh" restart
fi
"$HOST_ROOT/scripts/run_team_stack.sh" restart-guard

if [[ -x "$HOST_ROOT/scripts/run_team_portal_slot.sh" ]]; then
  TEAM_PORTAL_SLOT=candidate TEAM_PORTAL_SLOT_PORT=5001 TEAM_PORTAL_SLOT_REVISION="$TARGET_COMMIT" TEAM_PORTAL_SLOT_REPLACE_STALE=1 \
    "$HOST_ROOT/scripts/run_team_portal_slot.sh" restart
  TEAM_PORTAL_SLOT=candidate TEAM_PORTAL_SLOT_PORT=5001 "$HOST_ROOT/scripts/run_team_portal_slot.sh" stop >/dev/null 2>&1 || true
fi

wait_for_revision "live_loopback_health" "http://127.0.0.1:5000/healthz" "$TARGET_COMMIT" 60

HOST_ENV_FILE="$HOST_ROOT/.env"
PUBLIC_URL="$(ENV_FILE="$HOST_ENV_FILE" read_env_value TEAM_PORTAL_BASE_URL)"
if [[ -n "$PUBLIC_URL" ]]; then
  wait_for_revision "live_public_health" "${PUBLIC_URL%/}/healthz" "$TARGET_COMMIT" 90
fi

echo "Mac-hosted Live now serves origin/main $TARGET_COMMIT."
