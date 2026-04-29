#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

SCRIPT_STARTED_AT="$(date +%s)"
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
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$("$GCLOUD_BIN" config get-value project 2>/dev/null || true)}"
PROJECT_ID="${PROJECT_ID:-}"
if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
  echo "Google Cloud project is not configured. Run: $GCLOUD_BIN config set project PROJECT_ID"
  exit 1
fi

ACCOUNT="$("$GCLOUD_BIN" auth list --filter='status:ACTIVE' --format='value(account)' 2>/dev/null || true)"
if [[ -z "$ACCOUNT" ]]; then
  echo "No active Google Cloud account. Run: $GCLOUD_BIN auth login"
  exit 1
fi

SERVICE="${CLOUD_RUN_SERVICE:-team-portal}"
REGION="${CLOUD_RUN_REGION:-asia-southeast1}"
CLOUD_RUN_IMAGE="${CLOUD_RUN_IMAGE:-}"
EXISTING_SERVICE_URL="$("$GCLOUD_BIN" run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)' 2>/dev/null || true)"
BASE_URL="${CLOUD_RUN_TEAM_PORTAL_BASE_URL:-${EXISTING_SERVICE_URL:-}}"
PROJECT_NUMBER="$("$GCLOUD_BIN" projects describe "$PROJECT_ID" --format='value(projectNumber)')"
RUNTIME_SERVICE_ACCOUNT="${CLOUD_RUN_SERVICE_ACCOUNT:-$PROJECT_NUMBER-compute@developer.gserviceaccount.com}"
LOCAL_AGENT_URL="$(resolve_cloud_run_local_agent_url)"
LOCAL_AGENT_SECRET="${LOCAL_AGENT_HMAC_SECRET:-$(read_env_value LOCAL_AGENT_HMAC_SECRET)}"
GOOGLE_SECRET_FILE="${GOOGLE_OAUTH_CLIENT_SECRET_FILE:-$(read_env_value GOOGLE_OAUTH_CLIENT_SECRET_FILE)}"
CONFIG_KEY="${TEAM_PORTAL_CONFIG_ENCRYPTION_KEY:-$(read_env_value TEAM_PORTAL_CONFIG_ENCRYPTION_KEY)}"
TEAM_ALLOWED_DOMAINS="${TEAM_ALLOWED_EMAIL_DOMAINS:-$(read_env_value TEAM_ALLOWED_EMAIL_DOMAINS)}"
TEAM_ALLOWED_EMAILS="${TEAM_ALLOWED_EMAILS:-$(read_env_value TEAM_ALLOWED_EMAILS)}"
BPMIS_BASE_URL="${BPMIS_BASE_URL:-$(read_env_value BPMIS_BASE_URL)}"

if [[ -z "$LOCAL_AGENT_URL" || -z "$LOCAL_AGENT_SECRET" ]]; then
  echo "CLOUD_RUN_LOCAL_AGENT_BASE_URL or LOCAL_AGENT_PUBLIC_URL, plus LOCAL_AGENT_HMAC_SECRET, are required."
  exit 1
fi
if is_loopback_http_url "$LOCAL_AGENT_URL"; then
  echo "Cloud Run cannot reach a localhost LOCAL_AGENT_BASE_URL."
  echo "Set CLOUD_RUN_LOCAL_AGENT_BASE_URL or LOCAL_AGENT_PUBLIC_URL to the Mac local-agent public URL."
  echo "If the Mac portal ngrok proxies /api/local-agent/*, TEAM_PORTAL_BASE_URL can be used as the fallback."
  exit 1
fi
if [[ -z "$GOOGLE_SECRET_FILE" || ! -f "$GOOGLE_SECRET_FILE" ]]; then
  echo "GOOGLE_OAUTH_CLIENT_SECRET_FILE must point to an existing JSON file."
  exit 1
fi
if [[ -z "$CONFIG_KEY" ]]; then
  echo "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY is required."
  exit 1
fi

secret_exists() {
  local name="$1"
  "$GCLOUD_BIN" secrets describe "$name" --project "$PROJECT_ID" >/dev/null 2>&1
}

create_or_update_secret() {
  local name="$1"
  local value="$2"
  if secret_exists "$name"; then
    if [[ "${CLOUD_RUN_FORCE_SECRET_VERSION:-0}" != "1" ]]; then
      local current
      current="$("$GCLOUD_BIN" secrets versions access latest --secret "$name" --project "$PROJECT_ID" 2>/dev/null || true)"
      if [[ "$current" == "$value" ]]; then
        echo "Secret unchanged: $name"
        return 0
      fi
    fi
    printf '%s' "$value" | "$GCLOUD_BIN" secrets versions add "$name" --project "$PROJECT_ID" --data-file=-
  else
    printf '%s' "$value" | "$GCLOUD_BIN" secrets create "$name" --project "$PROJECT_ID" --replication-policy=automatic --data-file=-
  fi
}

create_or_update_secret_file() {
  local name="$1"
  local path="$2"
  if secret_exists "$name"; then
    if [[ "${CLOUD_RUN_FORCE_SECRET_VERSION:-0}" != "1" ]]; then
      local current
      current="$("$GCLOUD_BIN" secrets versions access latest --secret "$name" --project "$PROJECT_ID" 2>/dev/null || true)"
      if [[ "$current" == "$(<"$path")" ]]; then
        echo "Secret unchanged: $name"
        return 0
      fi
    fi
    "$GCLOUD_BIN" secrets versions add "$name" --project "$PROJECT_ID" --data-file="$path"
  else
    "$GCLOUD_BIN" secrets create "$name" --project "$PROJECT_ID" --replication-policy=automatic --data-file="$path"
  fi
}

FLASK_SECRET=""
if secret_exists "team-portal-flask-secret" && [[ "${CLOUD_RUN_ROTATE_FLASK_SECRET:-0}" != "1" ]]; then
  echo "Secret unchanged: team-portal-flask-secret"
else
  if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to create or rotate the Flask secret."
    exit 1
  fi
  FLASK_SECRET="$(openssl rand -base64 48)"
fi

if [[ "${CLOUD_RUN_SKIP_SERVICE_ENABLE:-0}" == "1" ]]; then
  echo "Skipping service enable preflight because CLOUD_RUN_SKIP_SERVICE_ENABLE=1."
else
  SERVICES_STARTED_AT="$(date +%s)"
  "$GCLOUD_BIN" services enable \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    secretmanager.googleapis.com \
    --project "$PROJECT_ID"
  SERVICES_FINISHED_AT="$(date +%s)"
  echo "Cloud Run service enable preflight completed in $((SERVICES_FINISHED_AT - SERVICES_STARTED_AT))s"
fi

if [[ "${CLOUD_RUN_SKIP_IAM_BINDINGS:-0}" == "1" ]]; then
  echo "Skipping IAM binding preflight because CLOUD_RUN_SKIP_IAM_BINDINGS=1."
else
  IAM_STARTED_AT="$(date +%s)"
  for role in \
    roles/storage.objectViewer \
    roles/logging.logWriter \
    roles/artifactregistry.writer \
    roles/secretmanager.secretAccessor
  do
    "$GCLOUD_BIN" projects add-iam-policy-binding "$PROJECT_ID" \
      --member="serviceAccount:$RUNTIME_SERVICE_ACCOUNT" \
      --role="$role" \
      --condition=None >/dev/null
  done
  IAM_FINISHED_AT="$(date +%s)"
  echo "Cloud Run IAM preflight completed in $((IAM_FINISHED_AT - IAM_STARTED_AT))s"
fi

SECRETS_STARTED_AT="$(date +%s)"
if [[ -n "$FLASK_SECRET" ]]; then
  create_or_update_secret "team-portal-flask-secret" "$FLASK_SECRET"
fi
create_or_update_secret "team-portal-config-encryption-key" "$CONFIG_KEY"
create_or_update_secret "local-agent-hmac-secret" "$LOCAL_AGENT_SECRET"
create_or_update_secret_file "google-oauth-client-secret-json" "$GOOGLE_SECRET_FILE"
SECRETS_FINISHED_AT="$(date +%s)"
echo "Cloud Run secret preflight completed in $((SECRETS_FINISHED_AT - SECRETS_STARTED_AT))s"

ENV_VARS=(
  "TEAM_ALLOWED_EMAIL_DOMAINS=$TEAM_ALLOWED_DOMAINS"
  "TEAM_ALLOWED_EMAILS=$TEAM_ALLOWED_EMAILS"
  "TEAM_PORTAL_DATA_DIR=${CLOUD_RUN_TEAM_PORTAL_DATA_DIR:-/workspace/team-portal-runtime}"
  "BPMIS_CALL_MODE=${BPMIS_CALL_MODE:-local_agent}"
  "BPMIS_BASE_URL=$BPMIS_BASE_URL"
  "SOURCE_CODE_QA_OWNER_EMAIL=${SOURCE_CODE_QA_OWNER_EMAIL:-xiaodong.zheng@npt.sg}"
  "SOURCE_CODE_QA_ADMIN_EMAILS=${SOURCE_CODE_QA_ADMIN_EMAILS:-xiaodong.zheng@npt.sg,xiaodong.zheng1991@gmail.com}"
  "SOURCE_CODE_QA_QUERY_SYNC_MODE=${SOURCE_CODE_QA_QUERY_SYNC_MODE:-background}"
  "LOCAL_AGENT_MODE=sync"
  "LOCAL_AGENT_BASE_URL=$LOCAL_AGENT_URL"
  "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=true"
  "LOCAL_AGENT_SEATALK_ENABLED=true"
  "LOCAL_AGENT_BPMIS_ENABLED=${LOCAL_AGENT_BPMIS_ENABLED:-true}"
  "GUNICORN_WORKERS=${GUNICORN_WORKERS:-1}"
  "GOOGLE_OAUTH_CLIENT_SECRET_FILE=/secrets/google/client_secret.json"
)
if [[ -n "$BASE_URL" ]]; then
  ENV_VARS+=("TEAM_PORTAL_BASE_URL=$BASE_URL")
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

cd "$ROOT_DIR"
DEPLOY_SOURCE_ARGS=(--source .)
if [[ -n "$CLOUD_RUN_IMAGE" ]]; then
  DEPLOY_SOURCE_ARGS=(--image "$CLOUD_RUN_IMAGE")
fi
PREFLIGHT_FINISHED_AT="$(date +%s)"
echo "Cloud Run full preflight completed in $((PREFLIGHT_FINISHED_AT - SCRIPT_STARTED_AT))s"
DEPLOY_STARTED_AT="$(date +%s)"
"$GCLOUD_BIN" run deploy "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  "${DEPLOY_SOURCE_ARGS[@]}" \
  --allow-unauthenticated \
  --max-instances="${CLOUD_RUN_MAX_INSTANCES:-1}" \
  ${RUNTIME_ARGS[@]+"${RUNTIME_ARGS[@]}"} \
  --set-env-vars "^|^$ENV_VARS_JOINED" \
  --set-secrets "/secrets/google/client_secret.json=google-oauth-client-secret-json:latest,FLASK_SECRET_KEY=team-portal-flask-secret:latest,TEAM_PORTAL_CONFIG_ENCRYPTION_KEY=team-portal-config-encryption-key:latest,LOCAL_AGENT_HMAC_SECRET=local-agent-hmac-secret:latest"
DEPLOY_FINISHED_AT="$(date +%s)"
echo "Cloud Run deploy completed in $((DEPLOY_FINISHED_AT - DEPLOY_STARTED_AT))s"

SERVICE_URL="${BASE_URL:-}"
if [[ -z "$SERVICE_URL" ]]; then
  SERVICE_URL="$("$GCLOUD_BIN" run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"
  UPDATE_STARTED_AT="$(date +%s)"
  "$GCLOUD_BIN" run services update "$SERVICE" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --update-env-vars "TEAM_PORTAL_BASE_URL=$SERVICE_URL"
  UPDATE_FINISHED_AT="$(date +%s)"
  echo "Cloud Run base URL update completed in $((UPDATE_FINISHED_AT - UPDATE_STARTED_AT))s"
else
  UPDATE_FINISHED_AT="$(date +%s)"
  echo "Cloud Run base URL update skipped because TEAM_PORTAL_BASE_URL is already known."
fi

echo "Cloud Run deployed: $SERVICE_URL"
echo "Cloud Run full script completed in $((UPDATE_FINISHED_AT - SCRIPT_STARTED_AT))s"
echo "Add this Google OAuth redirect URI if it is not already configured:"
echo "$SERVICE_URL/auth/google/callback"
