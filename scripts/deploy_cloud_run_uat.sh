#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

SCRIPT_STARTED_AT="$(date +%s)"
SERVICE="${CLOUD_RUN_SERVICE:-team-portal}"
REGION="${CLOUD_RUN_REGION:-asia-southeast1}"
UAT_TAG="${CLOUD_RUN_UAT_TAG:-uat}"
CLOUD_RUN_IMAGE="${CLOUD_RUN_IMAGE:-}"
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
if [[ -n "${GOOGLE_CLOUD_PROJECT:-}" ]]; then
  PROJECT_ARGS=(--project "$GOOGLE_CLOUD_PROJECT")
fi
ACCOUNT_ARGS=()
if [[ -n "${CLOUD_RUN_DEPLOY_ACCOUNT:-}" ]]; then
  ACCOUNT_ARGS=(--account "$CLOUD_RUN_DEPLOY_ACCOUNT")
fi

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

require_clean_pushed_main

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse HEAD)"
SERVICE_DESCRIBE_JSON="$(describe_service 2>/dev/null || true)"
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

LOCAL_AGENT_URL="$(resolve_cloud_run_local_agent_url)"
if is_loopback_http_url "$LOCAL_AGENT_URL"; then
  echo "Cloud Run UAT cannot reach a localhost LOCAL_AGENT_BASE_URL."
  echo "Set CLOUD_RUN_LOCAL_AGENT_BASE_URL or LOCAL_AGENT_PUBLIC_URL to the Mac local-agent public URL."
  exit 1
fi

ENV_VARS=(
  "TEAM_ALLOWED_EMAIL_DOMAINS=${TEAM_ALLOWED_EMAIL_DOMAINS:-$(read_env_value TEAM_ALLOWED_EMAIL_DOMAINS)}"
  "TEAM_ALLOWED_EMAILS=${TEAM_ALLOWED_EMAILS:-$(read_env_value TEAM_ALLOWED_EMAILS)}"
  "TEAM_PORTAL_DATA_DIR=${CLOUD_RUN_UAT_TEAM_PORTAL_DATA_DIR:-/workspace/team-portal-uat-runtime}"
  "GOOGLE_OAUTH_CLIENT_SECRET_FILE=${GOOGLE_OAUTH_CLIENT_SECRET_FILE:-/secrets/google/client_secret.json}"
  "BPMIS_BASE_URL=${BPMIS_BASE_URL:-$(read_env_value BPMIS_BASE_URL)}"
  "SOURCE_CODE_QA_OWNER_EMAIL=${SOURCE_CODE_QA_OWNER_EMAIL:-xiaodong.zheng@npt.sg}"
  "SOURCE_CODE_QA_ADMIN_EMAILS=${SOURCE_CODE_QA_ADMIN_EMAILS:-xiaodong.zheng@npt.sg,xiaodong.zheng1991@gmail.com}"
  "SOURCE_CODE_QA_QUERY_SYNC_MODE=${SOURCE_CODE_QA_QUERY_SYNC_MODE:-background}"
  "BPMIS_CALL_MODE=${BPMIS_CALL_MODE:-local_agent}"
  "LOCAL_AGENT_MODE=${LOCAL_AGENT_MODE:-sync}"
  "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=${LOCAL_AGENT_SOURCE_CODE_QA_ENABLED:-true}"
  "LOCAL_AGENT_SEATALK_ENABLED=${LOCAL_AGENT_SEATALK_ENABLED:-true}"
  "LOCAL_AGENT_BPMIS_ENABLED=${LOCAL_AGENT_BPMIS_ENABLED:-true}"
  "GUNICORN_WORKERS=${GUNICORN_WORKERS:-1}"
  "TEAM_PORTAL_STAGE=uat"
  "TEAM_PORTAL_BASE_URL=$UAT_URL"
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
if [[ -n "$LOCAL_AGENT_URL" ]]; then
  ENV_VARS+=("LOCAL_AGENT_BASE_URL=$LOCAL_AGENT_URL")
fi

IFS='|'
ENV_VARS_JOINED="${ENV_VARS[*]}"
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
echo "Cloud Run UAT URL: $UAT_URL"
if [[ "${CLOUD_RUN_UAT_DRY_RUN:-0}" == "1" ]]; then
  echo "Dry run only; set CLOUD_RUN_UAT_DRY_RUN=0 or unset it to deploy."
  exit 0
fi

cd "$ROOT_DIR"
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
  --set-env-vars "^|^$ENV_VARS_JOINED"

SERVICE_DESCRIBE_JSON="$(describe_service)"
UAT_REVISION="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); tag=os.environ["UAT_TAG_VALUE"]; matches=[t for t in p.get("status", {}).get("traffic", []) if t.get("tag")==tag]; print(matches[0].get("revisionName", "") if matches else "")')"
DESCRIBED_UAT_URL="$(printf '%s' "$SERVICE_DESCRIBE_JSON" | UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" -c 'import json, os, sys; p=json.load(sys.stdin); tag=os.environ["UAT_TAG_VALUE"]; matches=[t for t in p.get("status", {}).get("traffic", []) if t.get("tag")==tag]; print(matches[0].get("url", "") if matches else "")')"
if [[ -z "$UAT_REVISION" ]]; then
  echo "Cloud Run UAT deploy finished, but tag '$UAT_TAG' was not found in service traffic status."
  exit 1
fi

SCRIPT_FINISHED_AT="$(date +%s)"
echo "Cloud Run UAT revision: $UAT_REVISION"
echo "Cloud Run UAT URL: ${DESCRIBED_UAT_URL:-$UAT_URL}"
echo "Cloud Run UAT keeps live traffic unchanged because it was deployed with --no-traffic."
echo "Cloud Run UAT script completed in $((SCRIPT_FINISHED_AT - SCRIPT_STARTED_AT))s"
