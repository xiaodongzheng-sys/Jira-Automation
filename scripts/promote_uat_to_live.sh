#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
source "$ROOT_DIR/scripts/lib/release_window_policy.sh"

SCRIPT_STARTED_AT="$(date +%s)"
SERVICE="${CLOUD_RUN_SERVICE:-team-portal}"
REGION="${CLOUD_RUN_REGION:-asia-southeast1}"
UAT_TAG="${CLOUD_RUN_UAT_TAG:-uat}"
HOST_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"
GCLOUD_BIN="${GCLOUD_BIN:-$(command -v gcloud || true)}"
if [[ -z "$GCLOUD_BIN" && -x "$HOME/google-cloud-sdk/bin/gcloud" ]]; then
  GCLOUD_BIN="$HOME/google-cloud-sdk/bin/gcloud"
fi
if [[ -z "$GCLOUD_BIN" ]]; then
  echo "gcloud is not installed. Install Google Cloud SDK first."
  exit 1
fi
if [[ -x "/opt/homebrew/bin/python3.12" && -z "${CLOUDSDK_PYTHON:-}" ]]; then
  export CLOUDSDK_PYTHON="/opt/homebrew/bin/python3.12"
fi

PROJECT_ARGS=()
GOOGLE_CLOUD_PROJECT_RESOLVED="${GOOGLE_CLOUD_PROJECT:-$(read_env_value GOOGLE_CLOUD_PROJECT)}"
if [[ -n "$GOOGLE_CLOUD_PROJECT_RESOLVED" ]]; then
  PROJECT_ARGS=(--project "$GOOGLE_CLOUD_PROJECT_RESOLVED")
fi
ACCOUNT_ARGS=()
CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED="${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}"
if [[ -n "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED" ]]; then
  ACCOUNT_ARGS=(--account "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED")
fi

enforce_release_window_target live

record_promote_timing_on_exit() {
  local status=$?
  local finished_at
  finished_at="$(date +%s)"
  record_deploy_timing "promote_uat_to_live.sh" "script" "$SCRIPT_STARTED_AT" "$finished_at" "$status" "service=$SERVICE region=$REGION tag=$UAT_TAG host=$HOST_ROOT" || true
  return "$status"
}
trap record_promote_timing_on_exit EXIT

json_from_gcloud_service() {
  "$GCLOUD_BIN" run services describe "$SERVICE" \
    ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --region "$REGION" \
    --format=json
}

json_from_gcloud_revision() {
  local revision="$1"
  "$GCLOUD_BIN" run revisions describe "$revision" \
    ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --region "$REGION" \
    --format=json
}

classify_live_restart_mode() {
  local previous_commit="$1"
  local target_commit="$2"
  local requested="${PROMOTE_UAT_RESTART_MODE:-auto}"
  case "$requested" in
    full|portal)
      printf '%s\n' "$requested"
      return 0
      ;;
    auto) ;;
    *)
      echo "PROMOTE_UAT_RESTART_MODE must be auto, full, or portal." >&2
      return 1
      ;;
  esac

  if [[ -z "$previous_commit" || "$previous_commit" == "$target_commit" ]]; then
    printf 'portal\n'
    return 0
  fi

  local changed_files
  if ! changed_files="$(git -C "$HOST_ROOT" diff --name-only "$previous_commit" "$target_commit")"; then
    printf 'full\n'
    return 0
  fi
  if [[ -z "$changed_files" ]]; then
    printf 'portal\n'
    return 0
  fi

  while IFS= read -r changed_file; do
    [[ -n "$changed_file" ]] || continue
    case "$changed_file" in
      app.py|bpmis_jira_tool/web.py|bpmis_jira_tool/web_*.py|static/*|templates/*|tests/*|docs/*|README.md)
        ;;
      *)
        printf 'full\n'
        return 0
        ;;
    esac
  done <<<"$changed_files"

  printf 'portal\n'
}

live_local_agent_restart_requires_file() {
  local changed_file="$1"
  case "$changed_file" in
    local_agent.py|bpmis_jira_tool/local_agent*|bpmis_jira_tool/source_code_qa*|source_code_qa/*|config/source_code_qa*|scripts/run_local_agent*|requirements*.txt|prd_briefing/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

classify_live_local_agent_restart_mode() {
  local previous_commit="$1"
  local target_commit="$2"
  local requested="${PROMOTE_UAT_LOCAL_AGENT_RESTART_MODE:-auto}"
  case "$requested" in
    restart|skip)
      printf '%s\n' "$requested"
      return 0
      ;;
    auto) ;;
    *)
      echo "PROMOTE_UAT_LOCAL_AGENT_RESTART_MODE must be auto, restart, or skip." >&2
      return 1
      ;;
  esac

  local changed_files
  if ! changed_files="$(git -C "$HOST_ROOT" diff --name-only "$previous_commit" "$target_commit")"; then
    printf 'restart\n'
    return 0
  fi
  while IFS= read -r changed_file; do
    [[ -n "$changed_file" ]] || continue
    if live_local_agent_restart_requires_file "$changed_file"; then
      printf 'restart\n'
      return 0
    fi
  done <<<"$changed_files"
  printf 'skip\n'
}

validate_live_candidate_slot() {
  local target_commit="$1"
  if [[ "${PROMOTE_UAT_BLUE_GREEN_VALIDATE:-1}" != "1" ]]; then
    return 0
  fi
  if [[ ! -x "$HOST_ROOT/scripts/run_team_portal_slot.sh" ]]; then
    echo "Live candidate slot script is missing; skipping inactive slot validation."
    return 0
  fi
  local slot_port="${PROMOTE_UAT_BLUE_GREEN_PORT:-5001}"
  echo "Validating live candidate slot on port $slot_port before switching public live."
  TEAM_PORTAL_SLOT=candidate \
  TEAM_PORTAL_SLOT_PORT="$slot_port" \
  TEAM_PORTAL_SLOT_REVISION="$target_commit" \
  TEAM_PORTAL_SLOT_REPLACE_STALE=1 \
  "$HOST_ROOT/scripts/run_team_portal_slot.sh" restart
}

SERVICE_JSON="$(json_from_gcloud_service)"
UAT_REVISION="$(printf '%s' "$SERVICE_JSON" | UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); tag=os.environ["UAT_TAG_VALUE"]; matches=[t for t in p.get("status", {}).get("traffic", []) if t.get("tag")==tag]; print(matches[0].get("revisionName", "") if matches else "")')"
UAT_URL="$(printf '%s' "$SERVICE_JSON" | UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); tag=os.environ["UAT_TAG_VALUE"]; matches=[t for t in p.get("status", {}).get("traffic", []) if t.get("tag")==tag]; print(matches[0].get("url", "") if matches else "")')"
if [[ -z "$UAT_REVISION" ]]; then
  echo "No Cloud Run revision is tagged '$UAT_TAG'. Deploy UAT first."
  exit 1
fi

REVISION_JSON="$(json_from_gcloud_revision "$UAT_REVISION")"
UAT_COMMIT="$(printf '%s' "$REVISION_JSON" | "$PYTHON_BIN" -c 'import json, sys; p=json.load(sys.stdin); env=p.get("spec", {}).get("containers", [{}])[0].get("env", []); values={item.get("name"): item.get("value") for item in env}; print(values.get("TEAM_PORTAL_RELEASE_REVISION", "") or "")')"
if [[ -z "$UAT_COMMIT" || "$UAT_COMMIT" == *"-dirty-"* || "$UAT_COMMIT" == "unknown" ]]; then
  echo "UAT revision $UAT_REVISION does not contain a clean TEAM_PORTAL_RELEASE_REVISION."
  echo "Value: ${UAT_COMMIT:-<missing>}"
  exit 1
fi

if [[ ! -d "$HOST_ROOT/.git" ]]; then
  echo "Host workspace is missing or is not a git checkout: $HOST_ROOT"
  exit 1
fi

git -C "$HOST_ROOT" fetch origin >/dev/null
ORIGIN_MAIN="$(git -C "$HOST_ROOT" rev-parse origin/main)"
if [[ "$ORIGIN_MAIN" != "$UAT_COMMIT" ]]; then
  echo "UAT commit is not the current origin/main. Re-deploy UAT from the latest pushed commit before promoting."
  echo "UAT commit:  $UAT_COMMIT"
  echo "origin/main: $ORIGIN_MAIN"
  exit 1
fi

if ! git -C "$HOST_ROOT" diff --quiet --no-ext-diff --exit-code || ! git -C "$HOST_ROOT" diff --cached --quiet --no-ext-diff --exit-code; then
  echo "Host workspace has uncommitted changes. Clean or stash them before promoting UAT to Live."
  exit 1
fi

echo "Promoting Cloud Run UAT tag '$UAT_TAG' to Mac-hosted Live."
echo "UAT revision: $UAT_REVISION"
echo "UAT URL: ${UAT_URL:-<not reported>}"
echo "Git commit: $UAT_COMMIT"
echo "Host workspace: $HOST_ROOT"
if [[ "${PROMOTE_UAT_DRY_RUN:-0}" == "1" ]]; then
  echo "Dry run only; set PROMOTE_UAT_DRY_RUN=0 or unset it to update Mac-hosted Live."
  exit 0
fi

PREVIOUS_HEAD="$(git -C "$HOST_ROOT" rev-parse HEAD)"
git -C "$HOST_ROOT" checkout main >/dev/null
git -C "$HOST_ROOT" pull --ff-only origin main

HEAD_COMMIT="$(git -C "$HOST_ROOT" rev-parse HEAD)"
if [[ "$HEAD_COMMIT" != "$UAT_COMMIT" ]]; then
  echo "Host workspace did not end at the UAT commit after pull."
  echo "HEAD:       $HEAD_COMMIT"
  echo "UAT commit: $UAT_COMMIT"
  exit 1
fi

RESTART_MODE="$(classify_live_restart_mode "$PREVIOUS_HEAD" "$UAT_COMMIT")"
LOCAL_AGENT_RESTART_MODE="$(classify_live_local_agent_restart_mode "$PREVIOUS_HEAD" "$UAT_COMMIT")"
echo "Live restart mode: $RESTART_MODE"
echo "Live local-agent restart mode: $LOCAL_AGENT_RESTART_MODE"
validate_live_candidate_slot "$UAT_COMMIT"
if [[ "$LOCAL_AGENT_RESTART_MODE" == "restart" ]]; then
  assert_no_active_meeting_recording_before_local_agent_restart "restart live Mac local-agent during UAT promotion" \
    "${LOCAL_AGENT_TEAM_PORTAL_DATA_DIR:-${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}}"
  "$HOST_ROOT/scripts/run_local_agent.sh" restart
else
  echo "Skipping live local-agent restart; changed files do not affect local-agent-backed workflows."
fi
"$HOST_ROOT/scripts/run_team_stack.sh" restart-guard
TEAM_PORTAL_SLOT=candidate TEAM_PORTAL_SLOT_PORT="${PROMOTE_UAT_BLUE_GREEN_PORT:-5001}" "$HOST_ROOT/scripts/run_team_portal_slot.sh" stop >/dev/null 2>&1 || true
SERVED_REVISION="$(curl -fsS --max-time 10 "http://127.0.0.1:5000/healthz" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin).get("revision", ""))')"
if [[ "$SERVED_REVISION" != "$UAT_COMMIT" ]]; then
  echo "Live portal loopback revision mismatch after restart."
  echo "Served: $SERVED_REVISION"
  echo "UAT:    $UAT_COMMIT"
  exit 1
fi

HOST_ENV_FILE="$HOST_ROOT/.env"
PUBLIC_URL="$(ENV_FILE="$HOST_ENV_FILE" read_env_value TEAM_PORTAL_BASE_URL)"
if [[ -n "$PUBLIC_URL" ]]; then
  PUBLIC_REVISION="$(curl -fsS --max-time 15 "${PUBLIC_URL%/}/healthz" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin).get("revision", ""))')"
  if [[ "$PUBLIC_REVISION" != "$UAT_COMMIT" ]]; then
    echo "Live portal public revision mismatch after restart."
    echo "Public URL: $PUBLIC_URL"
    echo "Served:     $PUBLIC_REVISION"
    echo "UAT:        $UAT_COMMIT"
    exit 1
  fi
fi

echo "Mac-hosted Live now serves UAT commit $UAT_COMMIT."
