#!/usr/bin/env bash

# Live-only release: runs the full test gate, then promotes origin/main to the
# Mac-hosted public Live portal. The Cloud Run UAT environment is intentionally
# skipped end to end (no UAT deploy, no UAT health checks, no gcloud required).
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
  if [[ "${RELEASE_LIVE_ONLY_SKIP_GATE:-${RELEASE_UAT_LIVE_SKIP_GATE:-0}}" == "1" ]]; then
    echo "Skipping system full test gate because skip-gate is set."
    return 0
  fi
  if [[ "${RELEASE_UAT_LIVE_REUSE_VERIFIED_GATE:-1}" == "1" ]]; then
    if "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" \
      --check-proof \
      --proof-max-age-seconds "${RELEASE_UAT_LIVE_GATE_PROOF_MAX_AGE_SECONDS:-7200}" \
      --profile "${RELEASE_UAT_LIVE_GATE_PROFILE:-auto}" \
      --coverage-fail-under "${RELEASE_UAT_LIVE_COVERAGE_FAIL_UNDER:-100}"; then
      return 0
    fi
  fi
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" \
    --skip-smoke \
    --profile "${RELEASE_UAT_LIVE_GATE_PROFILE:-auto}" \
    --parallel-workers "${RELEASE_UAT_LIVE_GATE_WORKERS:-4}" \
    --coverage-fail-under "${RELEASE_UAT_LIVE_COVERAGE_FAIL_UNDER:-100}"
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

echo "Live-only release for origin/main $SHA (UAT skipped)."
run_gate

LIVE_URL="$(resolve_live_url)"
if [[ -n "$LIVE_URL" && "$(live_revision "$LIVE_URL" || true)" == "$SHA" ]]; then
  echo "Live already serves $SHA; nothing to promote."
  FINISHED_AT="$(date +%s)"
  echo "Live-only release confirmed Live already current in $((FINISHED_AT - STARTED_AT))s"
  exit 0
fi

PROMOTE_LIVE_TARGET=origin_main "$ROOT_DIR/scripts/promote_uat_to_live.sh"

# promote_uat_to_live.sh already verified Live loopback + public health. Run the
# host doctor as a final report; by default it does not fail the live deploy
# (set RELEASE_LIVE_ONLY_STRICT_DOCTOR=1 to make doctor findings fatal).
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
