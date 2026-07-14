#!/usr/bin/env bash

# Live release: promotes origin/main to the Mac-hosted public Live portal.
#
# Live is served by the Mac host checking out origin/main and restarting the
# portal/local-agent, so the only prerequisite is that the commit you want live
# has been pushed to origin/main.
#
# Env knobs:
#   RELEASE_LIVE_ONLY_SKIP_GATE=1     skip the system full test gate
#   RELEASE_LIVE_ONLY_STRICT_DOCTOR=1 fail the release if the post-deploy doctor
#                                     reports issues (default: report-only)
#   RELEASE_LIVE_URL / TEAM_PORTAL_BASE_URL  public Live base URL for the
#                                     "already current" short-circuit

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
source "$ROOT_DIR/scripts/lib/release_window_policy.sh"

STARTED_AT="$(date +%s)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

current_sha() {
  git -C "$ROOT_DIR" rev-parse HEAD
}

run_gate() {
  if [[ "${RELEASE_LIVE_ONLY_SKIP_GATE:-0}" == "1" ]]; then
    echo "Skipping system full test gate because skip-gate is set."
    return 0
  fi
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" \
    --skip-smoke \
    --profile "${RELEASE_LIVE_GATE_PROFILE:-auto}" \
    --parallel-workers "${RELEASE_LIVE_GATE_WORKERS:-4}" \
    --coverage-fail-under "${RELEASE_LIVE_COVERAGE_FAIL_UNDER:-100}"
}

resolve_live_url() {
  printf '%s\n' "${RELEASE_LIVE_URL:-${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}}"
}

live_revision() {
  local live_url="$1"
  curl -fsS --max-time 10 "${live_url%/}/healthz" \
    | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin).get("revision", ""))'
}

cd "$ROOT_DIR"
SHA="$(current_sha)"

# Live serves origin/main from the Mac host; require the target commit pushed.
git -C "$ROOT_DIR" fetch origin >/dev/null
ORIGIN_MAIN="$(git -C "$ROOT_DIR" rev-parse origin/main)"
if [[ "$ORIGIN_MAIN" != "$SHA" ]]; then
  echo "Local HEAD is not the current origin/main; push it before a live-only deploy."
  echo "HEAD:        $SHA"
  echo "origin/main: $ORIGIN_MAIN"
  exit 1
fi

echo "Live release for origin/main $SHA."
run_gate

LIVE_URL="$(resolve_live_url)"
if [[ -n "$LIVE_URL" && "$(live_revision "$LIVE_URL" || true)" == "$SHA" ]]; then
  echo "Live already serves $SHA; nothing to promote."
  FINISHED_AT="$(date +%s)"
  echo "Live-only release confirmed Live already current in $((FINISHED_AT - STARTED_AT))s"
  exit 0
fi

"$ROOT_DIR/scripts/promote_live.sh"

# promote_live.sh already verified Live loopback + public health. Run the host
# doctor as a final report; by default it does not fail the release.
TEAM_STACK_HOST_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"
doctor_status=0
"$TEAM_STACK_HOST_ROOT/scripts/run_team_stack.sh" doctor || doctor_status=$?
if (( doctor_status != 0 )); then
  if [[ "${RELEASE_LIVE_ONLY_STRICT_DOCTOR:-0}" == "1" ]]; then
    echo "Post-deploy doctor reported issues and RELEASE_LIVE_ONLY_STRICT_DOCTOR=1; failing." >&2
    exit "$doctor_status"
  fi
  echo "Warning: post-deploy doctor reported issues (status $doctor_status); Live deploy itself succeeded." >&2
fi

FINISHED_AT="$(date +%s)"
echo "Live-only release completed for $SHA in $((FINISHED_AT - STARTED_AT))s"
exit 0
