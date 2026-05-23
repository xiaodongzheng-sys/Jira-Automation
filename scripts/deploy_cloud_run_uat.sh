#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
source "$ROOT_DIR/scripts/lib/release_window_policy.sh"

SCRIPT_STARTED_AT="$(date +%s)"
SERVICE="${CLOUD_RUN_SERVICE:-$(read_env_value CLOUD_RUN_SERVICE)}"
SERVICE="${SERVICE:-team-portal}"
REGION="${CLOUD_RUN_REGION:-$(read_env_value CLOUD_RUN_REGION)}"
REGION="${REGION:-asia-southeast1}"
UAT_TAG="${CLOUD_RUN_UAT_TAG:-$(read_env_value CLOUD_RUN_UAT_TAG)}"
UAT_TAG="${UAT_TAG:-uat}"
UAT_LOCAL_AGENT_PORT="${CLOUD_RUN_UAT_LOCAL_AGENT_PORT:-7008}"
UAT_LOCAL_AGENT_DATA_DIR="${CLOUD_RUN_UAT_LOCAL_AGENT_DATA_DIR:-.team-portal-uat}"
UAT_LOCAL_AGENT_SCREEN_SESSION="${CLOUD_RUN_UAT_LOCAL_AGENT_SCREEN_SESSION:-bpmis-local-agent-uat}"
UAT_LOCAL_AGENT_SECRET_NAME="${CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_NAME:-local-agent-uat-hmac-secret}"
CLOUD_RUN_ARTIFACT_REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
CLOUD_RUN_IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
CLOUD_RUN_IMAGE="${CLOUD_RUN_IMAGE:-$(read_env_value CLOUD_RUN_IMAGE)}"
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
if [[ -z "$CLOUD_RUN_IMAGE" && -n "${CLOUD_RUN_UAT_PREBUILT_IMAGE_TAG:-}" ]]; then
  if [[ -z "$GOOGLE_CLOUD_PROJECT_RESOLVED" ]]; then
    echo "CLOUD_RUN_UAT_PREBUILT_IMAGE_TAG requires GOOGLE_CLOUD_PROJECT."
    exit 1
  fi
  CLOUD_RUN_IMAGE="${REGION}-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT_RESOLVED}/${CLOUD_RUN_ARTIFACT_REPOSITORY}/${CLOUD_RUN_IMAGE_NAME}:${CLOUD_RUN_UAT_PREBUILT_IMAGE_TAG}"
fi
ACCOUNT_ARGS=()
CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED="${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}"
if [[ -n "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED" ]]; then
  ACCOUNT_ARGS=(--account "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED")
fi
if [[ "${CLOUD_RUN_UAT_DRY_RUN:-0}" != "1" ]]; then
  require_gcloud_noninteractive_deploy_auth "$GCLOUD_BIN" "$GOOGLE_CLOUD_PROJECT_RESOLVED" "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED"
fi
UAT_SYNC_PID=""
UAT_SYNC_LOG=""
UAT_SYNC_STARTED_AT=""
UAT_LOCAL_AGENT_SECRET_SOURCE="${CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE:-secret_manager}"

enforce_release_window_target uat

record_uat_stage_timing() {
  local phase="$1"
  local started_at="$2"
  local finished_at="$3"
  local status="$4"
  local details="${5:-}"
  record_deploy_timing "deploy_cloud_run_uat.sh" "$phase" "$started_at" "$finished_at" "$status" "service=$SERVICE region=$REGION tag=$UAT_TAG $details" || true
}

record_uat_deploy_timing_on_exit() {
  local status=$?
  if [[ -n "${UAT_SYNC_PID:-}" ]]; then
    local sync_status=0
    local sync_finished_at
    wait "$UAT_SYNC_PID" >/dev/null 2>&1 || sync_status=$?
    sync_finished_at="$(date +%s)"
    if [[ -n "${UAT_SYNC_LOG:-}" && -f "$UAT_SYNC_LOG" ]]; then
      cat "$UAT_SYNC_LOG" || true
      rm -f "$UAT_SYNC_LOG"
    fi
    if [[ -n "${UAT_SYNC_STARTED_AT:-}" ]]; then
      record_uat_stage_timing "uat_host_sync" "$UAT_SYNC_STARTED_AT" "$sync_finished_at" "$sync_status" "mode=parallel exit_trap=1"
      UAT_SYNC_STARTED_AT=""
    fi
  fi
  local finished_at
  finished_at="$(date +%s)"
  record_deploy_timing "deploy_cloud_run_uat.sh" "script" "$SCRIPT_STARTED_AT" "$finished_at" "$status" "service=$SERVICE region=$REGION tag=$UAT_TAG image=${CLOUD_RUN_IMAGE:-source}" || true
  return "$status"
}
trap record_uat_deploy_timing_on_exit EXIT

require_clean_pushed_main() {
  if [[ "${CLOUD_RUN_UAT_SKIP_GIT_CHECK:-0}" == "1" ]]; then
    return 0
  fi
  if ! git -C "$ROOT_DIR" diff --quiet --no-ext-diff --exit-code || ! git -C "$ROOT_DIR" diff --cached --quiet --no-ext-diff --exit-code; then
    echo "UAT deploy requires a clean working tree. Commit and push the release candidate first."
    exit 1
  fi
  if [[ -n "$(git -C "$ROOT_DIR" ls-files --others --exclude-standard)" ]]; then
    echo "UAT deploy requires no untracked release files. Commit or ignore them first."
    exit 1
  fi
  git -C "$ROOT_DIR" fetch origin >/dev/null
  local head origin_main
  head="$(git -C "$ROOT_DIR" rev-parse HEAD)"
  origin_main="$(git -C "$ROOT_DIR" rev-parse origin/main)"
  if [[ "$head" != "$origin_main" ]]; then
    echo "UAT deploy requires HEAD to match origin/main."
    echo "HEAD:        $head"
    echo "origin/main: $origin_main"
    exit 1
  fi
}

json_field() {
  local expression="$1"
  "$PYTHON_BIN" -c "import json,sys; p=json.load(sys.stdin); v=$expression; print(v or '')"
}

tag_url_from_service_url() {
  local service_url="$1"
  SERVICE_URL_VALUE="$service_url" UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" - <<'PY'
import os
from urllib.parse import urlsplit, urlunsplit

service_url = os.environ.get("SERVICE_URL_VALUE", "").strip()
tag = os.environ.get("UAT_TAG_VALUE", "").strip()
parts = urlsplit(service_url)
if not parts.scheme or not parts.netloc or not tag:
    raise SystemExit(1)
print(urlunsplit((parts.scheme, f"{tag}---{parts.netloc}", "", "", "")))
PY
}

describe_service() {
  "$GCLOUD_BIN" run services describe "$SERVICE" \
    ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --region "$REGION" \
    --format=json
}

describe_revision() {
  local revision="$1"
  "$GCLOUD_BIN" run revisions describe "$revision" \
    ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --region "$REGION" \
    --format=json
}

prebuilt_sha_image_uri() {
  local image_tag="$1"
  printf '%s\n' "${REGION}-docker.pkg.dev/${GOOGLE_CLOUD_PROJECT_RESOLVED}/${CLOUD_RUN_ARTIFACT_REPOSITORY}/${CLOUD_RUN_IMAGE_NAME}:${image_tag}"
}

artifact_image_exists() {
  local image_uri="$1"
  local image_package="${image_uri%:*}"
  local image_tag="${image_uri##*:}"
  "$GCLOUD_BIN" artifacts docker tags list "$image_package" \
    ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --filter="tag:$image_tag" \
    --format="value(tag)" \
    2>/dev/null | grep -Fx "$image_tag" >/dev/null
}

select_prebuilt_sha_image_if_available() {
  local git_sha="$1"
  local started_at finished_at image_uri
  if [[ -n "$CLOUD_RUN_IMAGE" || "${CLOUD_RUN_UAT_AUTO_PREBUILT_IMAGE:-1}" == "0" ]]; then
    return 0
  fi
  if [[ -z "$GOOGLE_CLOUD_PROJECT_RESOLVED" ]]; then
    echo "Skipping UAT prebuilt SHA image lookup because GOOGLE_CLOUD_PROJECT is not set."
    return 0
  fi

  image_uri="$(prebuilt_sha_image_uri "$git_sha")"
  started_at="$(date +%s)"
  if artifact_image_exists "$image_uri"; then
    CLOUD_RUN_IMAGE="$image_uri"
    finished_at="$(date +%s)"
    record_uat_stage_timing "prebuilt_image_lookup" "$started_at" "$finished_at" 0 "image=$image_uri result=hit"
    echo "Using prebuilt UAT image for current SHA: $image_uri"
  else
    finished_at="$(date +%s)"
    record_uat_stage_timing "prebuilt_image_lookup" "$started_at" "$finished_at" 0 "image=$image_uri result=miss"
    echo "No prebuilt UAT image found for current SHA; falling back to Cloud Run source deploy."
  fi
}

uat_secret_manager_version_accessible() {
  "$GCLOUD_BIN" secrets versions access latest \
    --secret "$UAT_LOCAL_AGENT_SECRET_NAME" \
    ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    >/dev/null 2>&1
}

select_uat_local_agent_secret_source() {
  local started_at finished_at
  if [[ "$UAT_LOCAL_AGENT_SECRET_SOURCE" != "secret_manager" ]]; then
    return 0
  fi
  if [[ "${CLOUD_RUN_UAT_AUTO_ENV_FALLBACK_ON_MISSING_SECRET:-1}" == "0" ]]; then
    return 0
  fi

  started_at="$(date +%s)"
  if uat_secret_manager_version_accessible; then
    finished_at="$(date +%s)"
    record_uat_stage_timing "uat_secret_check" "$started_at" "$finished_at" 0 "secret=$UAT_LOCAL_AGENT_SECRET_NAME result=secret_manager"
    return 0
  fi

  finished_at="$(date +%s)"
  record_uat_stage_timing "uat_secret_check" "$started_at" "$finished_at" 0 "secret=$UAT_LOCAL_AGENT_SECRET_NAME result=env_fallback"
  UAT_LOCAL_AGENT_SECRET_SOURCE="env"
  echo "UAT Secret Manager secret $UAT_LOCAL_AGENT_SECRET_NAME is missing or inaccessible; using UAT env fallback."
}

resolve_uat_host_workspace() {
  local configured="${CLOUD_RUN_UAT_HOST_WORKSPACE:-}"
  if [[ -z "$configured" ]]; then
    configured="$(recommended_uat_team_stack_root)"
  fi
  printf '%s\n' "$configured"
}

resolve_uat_local_agent_data_path() {
  local host_workspace="$1"
  local data_dir="${CLOUD_RUN_UAT_LOCAL_AGENT_DATA_DIR:-$UAT_LOCAL_AGENT_DATA_DIR}"
  local data_path
  data_path="$(HOST_WORKSPACE="$host_workspace" DATA_DIR_VALUE="$data_dir" "$PYTHON_BIN" - <<'PY'
import os
from pathlib import Path

host = Path(os.environ["HOST_WORKSPACE"]).expanduser().resolve()
data_dir = os.environ.get("DATA_DIR_VALUE", ".team-portal-uat").strip() or ".team-portal-uat"
data_path = Path(data_dir).expanduser()
if not data_path.is_absolute():
    data_path = host / data_path
print(data_path)
PY
)"
  printf '%s\n' "$data_path"
}

resolve_uat_local_agent_url() {
  local explicit_url="${CLOUD_RUN_UAT_LOCAL_AGENT_BASE_URL:-}"
  if [[ -n "$explicit_url" ]]; then
    printf '%s\n' "$explicit_url"
    return 0
  fi

  local portal_url="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"
  if [[ -n "$portal_url" ]] && ! is_loopback_http_url "$portal_url"; then
    printf '%s/uat-local-agent\n' "${portal_url%/}"
    return 0
  fi

  printf '%s\n' "$portal_url"
}

ensure_host_prd_store_schema() {
  local host_workspace="$1"
  local host_python="$host_workspace/.venv/bin/python"
  if [[ ! -x "$host_python" ]]; then
    echo "Mac local-agent venv is missing: $host_python"
    echo "Create the host venv first, or set CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0 to skip this guard."
    exit 1
  fi

  local data_path
  data_path="$(resolve_uat_local_agent_data_path "$host_workspace")"
  HOST_WORKSPACE="$host_workspace" UAT_DATA_PATH="$data_path" "$host_python" - <<'PY'
import os
from pathlib import Path
from prd_briefing.storage import BriefingStore

data_path = Path(os.environ["UAT_DATA_PATH"]).expanduser().resolve()
BriefingStore(data_path / "prd_briefing")
print(data_path / "prd_briefing")
PY
}

verify_uat_source_code_qa_ops() {
  local host_workspace="$1"
  if [[ "${CLOUD_RUN_UAT_VERIFY_SOURCE_CODE_QA_OPS:-1}" == "0" ]]; then
    echo "Skipping UAT Source Code QA ops guard because CLOUD_RUN_UAT_VERIFY_SOURCE_CODE_QA_OPS=0."
    return 0
  fi

  local host_python="$host_workspace/.venv/bin/python"
  if [[ ! -x "$host_python" ]]; then
    echo "Mac local-agent venv is missing: $host_python"
    echo "Create the host venv first, or set CLOUD_RUN_UAT_VERIFY_SOURCE_CODE_QA_OPS=0 only if Source Code QA is intentionally out of scope."
    exit 1
  fi
  if [[ ! -f "$host_workspace/scripts/source_code_qa_ops_summary.py" ]]; then
    echo "Source Code QA ops guard is missing: $host_workspace/scripts/source_code_qa_ops_summary.py"
    exit 1
  fi

  local data_path
  data_path="$(resolve_uat_local_agent_data_path "$host_workspace")"
  echo "Verifying UAT Source Code QA active config and indexes: $data_path"
  ENV_FILE=/dev/null \
  PYTHONPATH="$host_workspace" \
  TEAM_PORTAL_DATA_DIR="$data_path" \
  "$host_python" "$host_workspace/scripts/source_code_qa_ops_summary.py" --strict
}

uat_local_agent_sync_requires_file() {
  local changed_file="$1"
  case "$changed_file" in
    static/*|templates/*|tests/*|docs/*|README.md|.dockerignore|.github/*)
      return 1
      ;;
    app.py|bpmis_jira_tool/web.py|bpmis_jira_tool/web_*.py)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

classify_uat_local_agent_sync_mode() {
  local host_workspace="$1"
  local target_commit="$2"
  local requested="${CLOUD_RUN_UAT_LOCAL_AGENT_SYNC_MODE:-auto}"
  case "$requested" in
    full|skip)
      printf '%s\n' "$requested"
      return 0
      ;;
    auto) ;;
    *)
      echo "CLOUD_RUN_UAT_LOCAL_AGENT_SYNC_MODE must be auto, full, or skip." >&2
      return 1
      ;;
  esac

  local previous_commit
  previous_commit="$(git -C "$host_workspace" rev-parse HEAD 2>/dev/null || true)"
  if [[ -z "$previous_commit" || "$previous_commit" == "$target_commit" ]]; then
    printf 'skip\n'
    return 0
  fi

  local changed_files
  if ! changed_files="$(git -C "$ROOT_DIR" diff --name-only "$previous_commit" "$target_commit")"; then
    printf 'full\n'
    return 0
  fi
  if [[ -z "$changed_files" ]]; then
    printf 'skip\n'
    return 0
  fi

  while IFS= read -r changed_file; do
    [[ -n "$changed_file" ]] || continue
    if uat_local_agent_sync_requires_file "$changed_file"; then
      printf 'full\n'
      return 0
    fi
  done <<<"$changed_files"

  printf 'skip\n'
}

verify_uat_public_local_agent_health() {
  local local_agent_base="${1%/}"
  local timeout_seconds="${CLOUD_RUN_UAT_PUBLIC_LOCAL_AGENT_HEALTH_TIMEOUT_SECONDS:-45}"
  local poll_seconds="${CLOUD_RUN_UAT_PUBLIC_LOCAL_AGENT_HEALTH_POLL_SECONDS:-3}"
  local started_at finished_at deadline

  started_at="$(date +%s)"
  deadline=$(( started_at + timeout_seconds ))
  echo "Verifying public Mac local-agent health for up to ${timeout_seconds}s: $local_agent_base"
  while true; do
    if curl -fsS --max-time 10 "$local_agent_base/api/local-agent/healthz" >/dev/null 2>&1; then
      finished_at="$(date +%s)"
      record_uat_stage_timing "uat_public_local_agent_health" "$started_at" "$finished_at" 0 "url=$local_agent_base/api/local-agent/healthz"
      return 0
    fi
    if curl -fsS --max-time 10 "$local_agent_base/healthz" >/dev/null 2>&1; then
      finished_at="$(date +%s)"
      record_uat_stage_timing "uat_public_local_agent_health" "$started_at" "$finished_at" 0 "url=$local_agent_base/healthz"
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      break
    fi
    sleep "$poll_seconds"
  done

  finished_at="$(date +%s)"
  record_uat_stage_timing "uat_public_local_agent_health" "$started_at" "$finished_at" 1 "url=$local_agent_base"
  echo "Mac local-agent public health check failed for $local_agent_base after ${timeout_seconds}s"
  return 1
}

sync_mac_local_agent_for_uat() {
  if [[ "${CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY:-1}" == "0" ]]; then
    echo "Skipping Mac local-agent sync because CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0."
    return 0
  fi

  local host_workspace
  host_workspace="$(resolve_uat_host_workspace)"
  if [[ ! -d "$host_workspace/.git" ]]; then
    echo "Mac local-agent host workspace was not found: $host_workspace"
    echo "Set CLOUD_RUN_UAT_HOST_WORKSPACE to the checkout that runs local-agent, or set CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0 to skip this guard."
    exit 1
  fi

  local host_branch
  host_branch="$(git -C "$host_workspace" rev-parse --abbrev-ref HEAD)"
  if [[ "$host_branch" != "main" ]]; then
    echo "Mac local-agent host workspace must be on main before UAT sync."
    echo "Workspace: $host_workspace"
    echo "Branch:    $host_branch"
    exit 1
  fi

  if ! git -C "$host_workspace" diff --quiet --no-ext-diff --exit-code || ! git -C "$host_workspace" diff --cached --quiet --no-ext-diff --exit-code; then
    echo "Mac local-agent host workspace has tracked changes; refusing to overwrite them."
    echo "Workspace: $host_workspace"
    echo "Commit/stash the host changes, or set CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0 only if you intentionally accept stale local-agent risk."
    exit 1
  fi

  local sync_mode
  sync_mode="$(classify_uat_local_agent_sync_mode "$host_workspace" "$GIT_SHA")"
  if [[ "$sync_mode" == "skip" ]]; then
    echo "Skipping UAT Mac local-agent sync/restart; changed files do not affect local-agent-backed workflows."
    if [[ "${CLOUD_RUN_UAT_VERIFY_PUBLIC_LOCAL_AGENT:-1}" != "0" && -n "$LOCAL_AGENT_URL" ]]; then
      local local_agent_base="${LOCAL_AGENT_URL%/}"
      verify_uat_public_local_agent_health "$local_agent_base"
    fi
    return 0
  fi

  echo "Syncing Mac local-agent host workspace for UAT: $host_workspace"
  git -C "$host_workspace" fetch origin main >/dev/null
  git -C "$host_workspace" merge --ff-only "$GIT_SHA" >/dev/null

  local host_head
  host_head="$(git -C "$host_workspace" rev-parse HEAD)"
  if [[ "$host_head" != "$GIT_SHA" ]]; then
    echo "Mac local-agent host workspace did not reach the UAT commit."
    echo "Expected: $GIT_SHA"
    echo "Actual:   $host_head"
    exit 1
  fi

  if [[ "${CLOUD_RUN_UAT_INSTALL_HOST_DEPS:-1}" != "0" ]]; then
    if [[ ! -x "$host_workspace/.venv/bin/pip" ]]; then
      echo "Mac local-agent venv pip is missing: $host_workspace/.venv/bin/pip"
      exit 1
    fi
    local data_path requirements_path requirements_hash deps_marker previous_hash
    data_path="$(resolve_uat_local_agent_data_path "$host_workspace")"
    requirements_path="$host_workspace/requirements.txt"
    deps_marker="$data_path/run/requirements.sha256"
    mkdir -p "$(dirname "$deps_marker")"
    requirements_hash="$(shasum -a 256 "$requirements_path" | awk '{print $1}')"
    previous_hash="$(cat "$deps_marker" 2>/dev/null || true)"
    if [[ "${CLOUD_RUN_UAT_FORCE_INSTALL_HOST_DEPS:-0}" != "1" && "$previous_hash" == "$requirements_hash" ]]; then
      echo "Skipping Mac local-agent Python dependency install; requirements.txt is unchanged."
    else
      echo "Installing Mac local-agent Python dependencies from requirements.txt"
      "$host_workspace/.venv/bin/pip" install -r "$requirements_path" >/dev/null
      printf '%s\n' "$requirements_hash" >"$deps_marker"
    fi
  fi

  local prd_store_path
  prd_store_path="$(ensure_host_prd_store_schema "$host_workspace")"
  echo "Mac local-agent PRD briefing store ready: $prd_store_path"

  if [[ "${CLOUD_RUN_UAT_RESTART_LOCAL_AGENT:-1}" != "0" ]]; then
    if [[ ! -x "$host_workspace/scripts/run_local_agent.sh" ]]; then
      echo "Mac local-agent restart script is missing: $host_workspace/scripts/run_local_agent.sh"
      exit 1
    fi
    local uat_local_agent_hmac_secret
    uat_local_agent_hmac_secret="$(ENV_FILE="$host_workspace/.env" read_env_value LOCAL_AGENT_HMAC_SECRET)"
    if [[ -z "$uat_local_agent_hmac_secret" ]]; then
      echo "UAT host .env is missing LOCAL_AGENT_HMAC_SECRET; refusing to restart local-agent with an ambiguous signing key."
      exit 1
    fi
    echo "Restarting isolated UAT Mac local-agent on port $UAT_LOCAL_AGENT_PORT"
    (
      cd "$host_workspace"
      ROOT_DIR="$host_workspace" \
      PYTHON_BIN="$host_workspace/.venv/bin/python" \
      LOCAL_AGENT_PORT="$UAT_LOCAL_AGENT_PORT" \
      LOCAL_AGENT_TEAM_PORTAL_DATA_DIR="$UAT_LOCAL_AGENT_DATA_DIR" \
      TEAM_PORTAL_DATA_DIR="$UAT_LOCAL_AGENT_DATA_DIR" \
      assert_no_active_meeting_recording_before_local_agent_restart "restart isolated UAT Mac local-agent" "$UAT_LOCAL_AGENT_DATA_DIR"
    )
    (
      cd "$host_workspace"
      LOCAL_AGENT_HMAC_SECRET="$uat_local_agent_hmac_secret" \
      LOCAL_AGENT_PORT="$UAT_LOCAL_AGENT_PORT" \
      LOCAL_AGENT_TEAM_PORTAL_DATA_DIR="$UAT_LOCAL_AGENT_DATA_DIR" \
      TEAM_PORTAL_DATA_DIR="$UAT_LOCAL_AGENT_DATA_DIR" \
      LOCAL_AGENT_SCREEN_SESSION="$UAT_LOCAL_AGENT_SCREEN_SESSION" \
      ./scripts/run_local_agent.sh restart >/dev/null
    )
  fi

  verify_uat_source_code_qa_ops "$host_workspace"

  if [[ "${CLOUD_RUN_UAT_VERIFY_PUBLIC_LOCAL_AGENT:-1}" != "0" && -n "$LOCAL_AGENT_URL" ]]; then
    local local_agent_base="${LOCAL_AGENT_URL%/}"
    verify_uat_public_local_agent_health "$local_agent_base"
  fi

  echo "Mac local-agent revision aligned with UAT commit: $GIT_SHA"
}

start_uat_host_sync_async() {
  if [[ "${CLOUD_RUN_UAT_PARALLEL_HOST_SYNC:-0}" != "1" ]]; then
    return 1
  fi
  UAT_SYNC_LOG="$(mktemp "${TMPDIR:-/tmp}/uat-host-sync.XXXXXX.log")"
  UAT_SYNC_STARTED_AT="$(date +%s)"
  echo "Starting UAT Mac local-agent sync in parallel with Cloud Run deploy."
  sync_mac_local_agent_for_uat >"$UAT_SYNC_LOG" 2>&1 &
  UAT_SYNC_PID="$!"
  return 0
}

finish_uat_host_sync() {
  if [[ -n "${UAT_SYNC_PID:-}" ]]; then
    local sync_status=0
    local finished_at
    wait "$UAT_SYNC_PID" || sync_status=$?
    finished_at="$(date +%s)"
    UAT_SYNC_PID=""
    if [[ -n "${UAT_SYNC_LOG:-}" && -f "$UAT_SYNC_LOG" ]]; then
      cat "$UAT_SYNC_LOG"
      rm -f "$UAT_SYNC_LOG"
      UAT_SYNC_LOG=""
    fi
    if [[ -n "${UAT_SYNC_STARTED_AT:-}" ]]; then
      record_uat_stage_timing "uat_host_sync" "$UAT_SYNC_STARTED_AT" "$finished_at" "$sync_status" "mode=parallel"
      UAT_SYNC_STARTED_AT=""
    fi
    return "$sync_status"
  fi
  local started_at finished_at sync_status=0
  started_at="$(date +%s)"
  sync_mac_local_agent_for_uat || sync_status=$?
  finished_at="$(date +%s)"
  record_uat_stage_timing "uat_host_sync" "$started_at" "$finished_at" "$sync_status" "mode=serial"
  return "$sync_status"
}

require_clean_pushed_main

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse HEAD)"
select_prebuilt_sha_image_if_available "$GIT_SHA"
SERVICE_DESCRIBE_STARTED_AT="$(date +%s)"
SERVICE_DESCRIBE_JSON="$(describe_service 2>/dev/null || true)"
SERVICE_DESCRIBE_FINISHED_AT="$(date +%s)"
if [[ -n "$SERVICE_DESCRIBE_JSON" ]]; then
  record_uat_stage_timing "describe_service" "$SERVICE_DESCRIBE_STARTED_AT" "$SERVICE_DESCRIBE_FINISHED_AT" 0 "scope=pre_deploy"
else
  record_uat_stage_timing "describe_service" "$SERVICE_DESCRIBE_STARTED_AT" "$SERVICE_DESCRIBE_FINISHED_AT" 1 "scope=pre_deploy"
fi
if [[ -z "$SERVICE_DESCRIBE_JSON" ]]; then
  echo "Could not describe Cloud Run service $SERVICE in $REGION."
  echo "Deploy the service once before creating a tagged UAT revision."
  exit 1
fi
SERVICE_URL="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | json_field "p.get('status', {}).get('url', '')")"
if [[ -z "$SERVICE_URL" ]]; then
  echo "Could not resolve existing Cloud Run service URL for $SERVICE in $REGION."
  echo "Deploy the service once before creating a tagged UAT revision."
  exit 1
fi
UAT_URL="$(tag_url_from_service_url "$SERVICE_URL")"
INVOKER_IAM_DISABLED="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | json_field "p.get('metadata', {}).get('annotations', {}).get('run.googleapis.com/invoker-iam-disabled', '')")"

LOCAL_AGENT_URL="$(resolve_uat_local_agent_url)"
if is_loopback_http_url "$LOCAL_AGENT_URL"; then
  echo "Cloud Run UAT cannot reach a localhost UAT local-agent URL."
  echo "Set TEAM_PORTAL_BASE_URL to the fixed live portal URL or set CLOUD_RUN_UAT_LOCAL_AGENT_BASE_URL."
  exit 1
fi
if [[ -z "$LOCAL_AGENT_URL" ]]; then
  echo "Could not resolve the UAT local-agent public URL."
  echo "Set TEAM_PORTAL_BASE_URL to the fixed live portal URL or set CLOUD_RUN_UAT_LOCAL_AGENT_BASE_URL."
  exit 1
fi

select_uat_local_agent_secret_source
VERSION_PLAN_FIRESTORE_PROJECT_RESOLVED="${VERSION_PLAN_FIRESTORE_PROJECT:-$(read_env_value VERSION_PLAN_FIRESTORE_PROJECT)}"
if [[ -z "$VERSION_PLAN_FIRESTORE_PROJECT_RESOLVED" && -n "$GOOGLE_CLOUD_PROJECT_RESOLVED" ]]; then
  VERSION_PLAN_FIRESTORE_PROJECT_RESOLVED="$GOOGLE_CLOUD_PROJECT_RESOLVED"
fi

ENV_VARS=(
  "TEAM_ALLOWED_EMAIL_DOMAINS=${TEAM_ALLOWED_EMAIL_DOMAINS:-$(read_env_value TEAM_ALLOWED_EMAIL_DOMAINS)}"
  "TEAM_ALLOWED_EMAILS=${TEAM_ALLOWED_EMAILS:-$(read_env_value TEAM_ALLOWED_EMAILS)}"
  "TEAM_PORTAL_DATA_DIR=${CLOUD_RUN_UAT_TEAM_PORTAL_DATA_DIR:-/workspace/team-portal-uat-runtime}"
  "GOOGLE_OAUTH_CLIENT_SECRET_FILE=${GOOGLE_OAUTH_CLIENT_SECRET_FILE:-/secrets/google/client_secret.json}"
  "BPMIS_BASE_URL=${BPMIS_BASE_URL:-$(read_env_value BPMIS_BASE_URL)}"
  "SOURCE_CODE_QA_OWNER_EMAIL=${SOURCE_CODE_QA_OWNER_EMAIL:-xiaodong.zheng@npt.sg}"
  "SOURCE_CODE_QA_ADMIN_EMAILS=${SOURCE_CODE_QA_ADMIN_EMAILS:-xiaodong.zheng@npt.sg}"
  "SOURCE_CODE_QA_QUERY_SYNC_MODE=${SOURCE_CODE_QA_QUERY_SYNC_MODE:-disabled}"
  "BPMIS_CALL_MODE=${BPMIS_CALL_MODE:-local_agent}"
  "LOCAL_AGENT_MODE=${LOCAL_AGENT_MODE:-sync}"
  "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=${LOCAL_AGENT_SOURCE_CODE_QA_ENABLED:-true}"
  "LOCAL_AGENT_SEATALK_ENABLED=${LOCAL_AGENT_SEATALK_ENABLED:-true}"
  "LOCAL_AGENT_BPMIS_ENABLED=${LOCAL_AGENT_BPMIS_ENABLED:-true}"
  "GUNICORN_WORKERS=${GUNICORN_WORKERS:-1}"
  "TEAM_PORTAL_STAGE=uat"
  "TEAM_PORTAL_BASE_URL=$UAT_URL"
  "TEAM_PORTAL_CLOUD_HOME_ENABLED=true"
  "TEAM_PORTAL_MAC_FULL_PORTAL_URL=${TEAM_PORTAL_MAC_FULL_PORTAL_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)/portal-home}"
  "VERSION_PLAN_STORE_BACKEND=firestore"
  "VERSION_PLAN_FIRESTORE_ENVIRONMENT=uat"
  "VERSION_PLAN_FIRESTORE_PROJECT=$VERSION_PLAN_FIRESTORE_PROJECT_RESOLVED"
  "TEAM_PORTAL_RELEASE_REVISION=$GIT_SHA"
)
append_optional_env_var() {
  local key="$1"
  local value="${!key:-$(read_env_value "$key")}"
  if [[ -n "$value" ]]; then
    ENV_VARS+=("$key=$value")
  fi
}
append_optional_env_var TRELLO_API_KEY
append_optional_env_var TRELLO_API_TOKEN
append_optional_env_var TRELLO_BOARD_ID
append_optional_env_var TRELLO_DAILY_LIST_NAME
append_optional_env_var PRD_BRIEFING_EDGE_MANDARIN_VOICE
if [[ -n "$LOCAL_AGENT_URL" ]]; then
  ENV_VARS+=("LOCAL_AGENT_BASE_URL=$LOCAL_AGENT_URL")
fi

BASE_SECRET_BINDINGS="FLASK_SECRET_KEY=team-portal-flask-secret:latest,TEAM_PORTAL_CONFIG_ENCRYPTION_KEY=team-portal-config-encryption-key:latest,/secrets/google/client_secret.json=google-oauth-client-secret-json:latest"
DEPLOY_SECRET_ARGS=(--update-secrets "LOCAL_AGENT_HMAC_SECRET=$UAT_LOCAL_AGENT_SECRET_NAME:latest")
ENV_DEPLOY_MODE="set"
ENV_REMOVE_ARGS=()
ENV_SECRET_PRECLEAR_REQUIRED=0
if [[ "$UAT_LOCAL_AGENT_SECRET_SOURCE" == "env" ]]; then
  uat_hmac_secret="${CLOUD_RUN_UAT_LOCAL_AGENT_HMAC_SECRET:-}"
  if [[ -z "$uat_hmac_secret" ]]; then
    uat_host_workspace="$(resolve_uat_host_workspace)"
    uat_env_file="$uat_host_workspace/.env"
    if [[ -f "$uat_env_file" ]]; then
      uat_hmac_secret="$(ENV_FILE="$uat_env_file" read_env_value LOCAL_AGENT_HMAC_SECRET)"
    fi
  fi
  if [[ -z "$uat_hmac_secret" ]]; then
    echo "CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE=env requires CLOUD_RUN_UAT_LOCAL_AGENT_HMAC_SECRET or LOCAL_AGENT_HMAC_SECRET in the UAT host .env."
    exit 1
  fi
  ENV_VARS+=("LOCAL_AGENT_HMAC_SECRET=$uat_hmac_secret")
  DEPLOY_SECRET_ARGS=(--set-secrets "$BASE_SECRET_BINDINGS")
  ENV_DEPLOY_MODE="set"
  ENV_SECRET_PRECLEAR_REQUIRED=1
  echo "Using UAT local-agent HMAC from env fallback because UAT local-agent secret source is env (CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE=env or automatic fallback)."
fi
if [[ "$ENV_SECRET_PRECLEAR_REQUIRED" == "1" && "${CLOUD_RUN_UAT_ENV_FALLBACK_PRECLEAR:-0}" == "1" ]]; then
  if ! SERVICE_JSON_VALUE="$SERVICE_DESCRIBE_JSON" "$PYTHON_BIN" - <<'PY'
import json
import os
import sys

payload = json.loads(os.environ.get("SERVICE_JSON_VALUE", "{}"))
containers = payload.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
for container in containers:
    for env in container.get("env") or []:
        if env.get("name") != "LOCAL_AGENT_HMAC_SECRET":
            continue
        if isinstance(env.get("valueFrom"), dict):
            raise SystemExit(0)
raise SystemExit(1)
PY
  then
    ENV_SECRET_PRECLEAR_REQUIRED=0
  fi
else
  ENV_SECRET_PRECLEAR_REQUIRED=0
fi

IFS='|'
ENV_VARS_JOINED_WITHOUT_HASH="${ENV_VARS[*]}"
unset IFS

RUNTIME_ARGS=()
if [[ -n "${CLOUD_RUN_MIN_INSTANCES:-}" ]]; then
  RUNTIME_ARGS+=(--min-instances="$CLOUD_RUN_MIN_INSTANCES")
fi
if [[ -n "${CLOUD_RUN_CPU:-}" ]]; then
  RUNTIME_ARGS+=(--cpu="$CLOUD_RUN_CPU")
fi
if [[ -n "${CLOUD_RUN_MEMORY:-}" ]]; then
  RUNTIME_ARGS+=(--memory="$CLOUD_RUN_MEMORY")
fi
if [[ -n "${CLOUD_RUN_CONCURRENCY:-}" ]]; then
  RUNTIME_ARGS+=(--concurrency="$CLOUD_RUN_CONCURRENCY")
fi
if [[ -n "${CLOUD_RUN_CPU_BOOST:-}" ]]; then
  RUNTIME_ARGS+=(--cpu-boost="$CLOUD_RUN_CPU_BOOST")
fi
if [[ -n "${CLOUD_RUN_TIMEOUT:-}" ]]; then
  RUNTIME_ARGS+=(--timeout="$CLOUD_RUN_TIMEOUT")
fi
IFS='|'
RUNTIME_ARGS_JOINED="${RUNTIME_ARGS[*]-}"
DEPLOY_SECRET_ARGS_JOINED="${DEPLOY_SECRET_ARGS[*]-}"
unset IFS
UAT_DEPLOY_HASH="$(printf '%s\n%s\n%s\n%s\n%s\n' "$GIT_SHA" "$ENV_VARS_JOINED_WITHOUT_HASH" "$CLOUD_RUN_IMAGE" "$RUNTIME_ARGS_JOINED" "$DEPLOY_SECRET_ARGS_JOINED" | hash_text)"
ENV_VARS+=("TEAM_PORTAL_DEPLOY_HASH=$UAT_DEPLOY_HASH")
IFS='|'
ENV_VARS_JOINED="${ENV_VARS[*]}"
unset IFS
ENV_DEPLOY_ARGS=(--set-env-vars "^|^$ENV_VARS_JOINED")
if [[ "$ENV_DEPLOY_MODE" == "update" ]]; then
  ENV_DEPLOY_ARGS=(--update-env-vars "^|^$ENV_VARS_JOINED")
fi

DEPLOY_SOURCE_ARGS=(--source .)
if [[ -n "$CLOUD_RUN_IMAGE" ]]; then
  DEPLOY_SOURCE_ARGS=(--image "$CLOUD_RUN_IMAGE")
fi
AUTH_ARGS=(--allow-unauthenticated)
if [[ "${CLOUD_RUN_ALLOW_UNAUTHENTICATED:-auto}" == "0" || "${CLOUD_RUN_ALLOW_UNAUTHENTICATED:-auto}" == "false" ]]; then
  AUTH_ARGS=()
elif [[ "${CLOUD_RUN_ALLOW_UNAUTHENTICATED:-auto}" == "auto" && "$INVOKER_IAM_DISABLED" == "true" ]]; then
  AUTH_ARGS=()
  echo "Cloud Run invoker IAM check is disabled; skipping --allow-unauthenticated IAM binding update."
fi

echo "Cloud Run UAT service: $SERVICE"
echo "Cloud Run UAT region: $REGION"
echo "Cloud Run UAT tag: $UAT_TAG"
echo "Cloud Run UAT commit: $GIT_SHA"
echo "Cloud Run UAT deploy hash: $UAT_DEPLOY_HASH"
if [[ -n "$CLOUD_RUN_IMAGE" ]]; then
  echo "Cloud Run UAT image: $CLOUD_RUN_IMAGE"
fi
echo "Cloud Run UAT URL: $UAT_URL"
if [[ "${CLOUD_RUN_UAT_DRY_RUN:-0}" == "1" ]]; then
  echo "Dry run only; set CLOUD_RUN_UAT_DRY_RUN=0 or unset it to deploy."
  exit 0
fi
if [[ "${CLOUD_RUN_UAT_SKIP_UNCHANGED:-0}" == "1" && "$UAT_DEPLOY_HASH" != "unknown" ]]; then
  EXISTING_UAT_REVISION="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); tag=os.environ["UAT_TAG_VALUE"]; matches=[t for t in p.get("status", {}).get("traffic", []) if t.get("tag")==tag]; print(matches[0].get("revisionName", "") if matches else "")')"
  EXISTING_UAT_DEPLOY_HASH=""
  if [[ -n "$EXISTING_UAT_REVISION" ]]; then
    EXISTING_UAT_DEPLOY_HASH="$(describe_revision "$EXISTING_UAT_REVISION" | "$PYTHON_BIN" -c 'import json, sys; p=json.load(sys.stdin); env=p.get("spec", {}).get("containers", [{}])[0].get("env", []); values={item.get("name"): item.get("value") for item in env}; print(values.get("TEAM_PORTAL_DEPLOY_HASH", "") or "")' 2>/dev/null || true)"
  fi
  if [[ "$EXISTING_UAT_DEPLOY_HASH" == "$UAT_DEPLOY_HASH" ]]; then
    echo "Cloud Run UAT deploy skipped: source and deploy env hash are unchanged ($UAT_DEPLOY_HASH)."
    sync_mac_local_agent_for_uat
    exit 0
  fi
fi

cd "$ROOT_DIR"
start_uat_host_sync_async || true
if [[ "$ENV_SECRET_PRECLEAR_REQUIRED" == "1" ]]; then
  CURRENT_IMAGE="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | json_field "p.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [{}])[0].get('image', '')")"
  if [[ -z "$CURRENT_IMAGE" ]]; then
    echo "Could not resolve current Cloud Run image for env fallback preclear."
    exit 1
  fi
  echo "Pre-clearing LOCAL_AGENT_HMAC_SECRET secret binding for UAT env fallback with a no-traffic revision."
  "$GCLOUD_BIN" run deploy "$SERVICE" \
    ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --region "$REGION" \
    --image "$CURRENT_IMAGE" \
    ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} \
    --max-instances="${CLOUD_RUN_MAX_INSTANCES:-1}" \
    ${RUNTIME_ARGS[@]+"${RUNTIME_ARGS[@]}"} \
    --no-traffic \
    --tag "${UAT_TAG}-secret-clear" \
    --remove-secrets "LOCAL_AGENT_HMAC_SECRET" \
    --remove-env-vars "LOCAL_AGENT_HMAC_SECRET"
fi

UAT_CLOUD_RUN_DEPLOY_STARTED_AT="$(date +%s)"
set +e
"$GCLOUD_BIN" run deploy "$SERVICE" \
  ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
  --region "$REGION" \
  "${DEPLOY_SOURCE_ARGS[@]}" \
  ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"} \
  --max-instances="${CLOUD_RUN_MAX_INSTANCES:-1}" \
  ${RUNTIME_ARGS[@]+"${RUNTIME_ARGS[@]}"} \
  --no-traffic \
  --tag "$UAT_TAG" \
  ${DEPLOY_SECRET_ARGS[@]+"${DEPLOY_SECRET_ARGS[@]}"} \
  ${ENV_REMOVE_ARGS[@]+"${ENV_REMOVE_ARGS[@]}"} \
  "${ENV_DEPLOY_ARGS[@]}"
UAT_CLOUD_RUN_DEPLOY_STATUS=$?
set -e
UAT_CLOUD_RUN_DEPLOY_FINISHED_AT="$(date +%s)"
record_uat_stage_timing "cloud_run_deploy" "$UAT_CLOUD_RUN_DEPLOY_STARTED_AT" "$UAT_CLOUD_RUN_DEPLOY_FINISHED_AT" "$UAT_CLOUD_RUN_DEPLOY_STATUS" "image=${CLOUD_RUN_IMAGE:-source}"
if [[ "$UAT_CLOUD_RUN_DEPLOY_STATUS" != "0" ]]; then
  exit "$UAT_CLOUD_RUN_DEPLOY_STATUS"
fi

POST_DEPLOY_DESCRIBE_STARTED_AT="$(date +%s)"
SERVICE_DESCRIBE_JSON="$(describe_service)"
POST_DEPLOY_DESCRIBE_FINISHED_AT="$(date +%s)"
record_uat_stage_timing "describe_service" "$POST_DEPLOY_DESCRIBE_STARTED_AT" "$POST_DEPLOY_DESCRIBE_FINISHED_AT" 0 "scope=post_deploy"
UAT_REVISION="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); tag=os.environ["UAT_TAG_VALUE"]; matches=[t for t in p.get("status", {}).get("traffic", []) if t.get("tag")==tag]; print(matches[0].get("revisionName", "") if matches else "")')"
DESCRIBED_UAT_URL="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); tag=os.environ["UAT_TAG_VALUE"]; matches=[t for t in p.get("status", {}).get("traffic", []) if t.get("tag")==tag]; print(matches[0].get("url", "") if matches else "")')"
if [[ -z "$UAT_REVISION" ]]; then
  echo "Cloud Run UAT deploy finished, but tag '$UAT_TAG' was not found in service traffic status."
  exit 1
fi

finish_uat_host_sync

SCRIPT_FINISHED_AT="$(date +%s)"
echo "Cloud Run UAT revision: $UAT_REVISION"
echo "Cloud Run UAT URL: ${DESCRIBED_UAT_URL:-$UAT_URL}"
echo "Cloud Run UAT keeps live traffic unchanged because it was deployed with --no-traffic."
echo "Cloud Run UAT script completed in $((SCRIPT_FINISHED_AT - SCRIPT_STARTED_AT))s"
