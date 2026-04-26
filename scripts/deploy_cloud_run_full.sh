#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

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
PROJECT_NUMBER="$("$GCLOUD_BIN" projects describe "$PROJECT_ID" --format='value(projectNumber)')"
RUNTIME_SERVICE_ACCOUNT="${CLOUD_RUN_SERVICE_ACCOUNT:-$PROJECT_NUMBER-compute@developer.gserviceaccount.com}"
LOCAL_AGENT_URL="${LOCAL_AGENT_BASE_URL:-$(read_env_value LOCAL_AGENT_BASE_URL)}"
LOCAL_AGENT_SECRET="${LOCAL_AGENT_HMAC_SECRET:-$(read_env_value LOCAL_AGENT_HMAC_SECRET)}"
GOOGLE_SECRET_FILE="${GOOGLE_OAUTH_CLIENT_SECRET_FILE:-$(read_env_value GOOGLE_OAUTH_CLIENT_SECRET_FILE)}"
CONFIG_KEY="${TEAM_PORTAL_CONFIG_ENCRYPTION_KEY:-$(read_env_value TEAM_PORTAL_CONFIG_ENCRYPTION_KEY)}"
TEAM_ALLOWED_DOMAINS="${TEAM_ALLOWED_EMAIL_DOMAINS:-$(read_env_value TEAM_ALLOWED_EMAIL_DOMAINS)}"
TEAM_ALLOWED_EMAILS="${TEAM_ALLOWED_EMAILS:-$(read_env_value TEAM_ALLOWED_EMAILS)}"
BPMIS_BASE_URL="${BPMIS_BASE_URL:-$(read_env_value BPMIS_BASE_URL)}"

if [[ -z "$LOCAL_AGENT_URL" || -z "$LOCAL_AGENT_SECRET" ]]; then
  echo "LOCAL_AGENT_BASE_URL and LOCAL_AGENT_HMAC_SECRET are required."
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

create_or_update_secret() {
  local name="$1"
  local value="$2"
  if "$GCLOUD_BIN" secrets describe "$name" --project "$PROJECT_ID" >/dev/null 2>&1; then
    printf '%s' "$value" | "$GCLOUD_BIN" secrets versions add "$name" --project "$PROJECT_ID" --data-file=-
  else
    printf '%s' "$value" | "$GCLOUD_BIN" secrets create "$name" --project "$PROJECT_ID" --replication-policy=automatic --data-file=-
  fi
}

create_or_update_secret_file() {
  local name="$1"
  local path="$2"
  if "$GCLOUD_BIN" secrets describe "$name" --project "$PROJECT_ID" >/dev/null 2>&1; then
    "$GCLOUD_BIN" secrets versions add "$name" --project "$PROJECT_ID" --data-file="$path"
  else
    "$GCLOUD_BIN" secrets create "$name" --project "$PROJECT_ID" --replication-policy=automatic --data-file="$path"
  fi
}

FLASK_SECRET="$(openssl rand -base64 48)"

"$GCLOUD_BIN" services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  --project "$PROJECT_ID"

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

create_or_update_secret "team-portal-flask-secret" "$FLASK_SECRET"
create_or_update_secret "team-portal-config-encryption-key" "$CONFIG_KEY"
create_or_update_secret "local-agent-hmac-secret" "$LOCAL_AGENT_SECRET"
create_or_update_secret_file "google-oauth-client-secret-json" "$GOOGLE_SECRET_FILE"

ENV_VARS=(
  "TEAM_ALLOWED_EMAIL_DOMAINS=$TEAM_ALLOWED_DOMAINS"
  "TEAM_ALLOWED_EMAILS=$TEAM_ALLOWED_EMAILS"
  "TEAM_PORTAL_DATA_DIR=/tmp/team-portal"
  "BPMIS_CALL_MODE=direct"
  "BPMIS_BASE_URL=$BPMIS_BASE_URL"
  "LOCAL_AGENT_MODE=sync"
  "LOCAL_AGENT_BASE_URL=$LOCAL_AGENT_URL"
  "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=true"
  "LOCAL_AGENT_SEATALK_ENABLED=true"
  "LOCAL_AGENT_BPMIS_ENABLED=false"
  "GOOGLE_OAUTH_CLIENT_SECRET_FILE=/secrets/google/client_secret.json"
)
IFS=,
ENV_VARS_JOINED="${ENV_VARS[*]}"
unset IFS

cd "$ROOT_DIR"
"$GCLOUD_BIN" run deploy "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --source . \
  --allow-unauthenticated \
  --set-env-vars "$ENV_VARS_JOINED" \
  --set-secrets "/secrets/google/client_secret.json=google-oauth-client-secret-json:latest,FLASK_SECRET_KEY=team-portal-flask-secret:latest,TEAM_PORTAL_CONFIG_ENCRYPTION_KEY=team-portal-config-encryption-key:latest,LOCAL_AGENT_HMAC_SECRET=local-agent-hmac-secret:latest"

SERVICE_URL="$("$GCLOUD_BIN" run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"

"$GCLOUD_BIN" run services update "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --update-env-vars "TEAM_PORTAL_BASE_URL=$SERVICE_URL"

echo "Cloud Run deployed: $SERVICE_URL"
echo "Add this Google OAuth redirect URI if it is not already configured:"
echo "$SERVICE_URL/auth/google/callback"
