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

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$(read_env_value GOOGLE_CLOUD_PROJECT)}"
PROJECT_ID="${PROJECT_ID:-$("$GCLOUD_BIN" config get-value project 2>/dev/null || true)}"
PROJECT_ID="${PROJECT_ID:-}"
if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
  echo "Google Cloud project is not configured. Set GOOGLE_CLOUD_PROJECT or run: $GCLOUD_BIN config set project PROJECT_ID"
  exit 1
fi

CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED="${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}"
ACCOUNT_ARGS=()
if [[ -n "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED" ]]; then
  ACCOUNT_ARGS=(--account "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED")
fi
require_gcloud_noninteractive_deploy_auth "$GCLOUD_BIN" "$PROJECT_ID" "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED"

PORTAL_DATA_DIR="${BUSINESS_INSIGHTS_GOOGLE_CREDENTIAL_DATA_DIR:-${LOCAL_AGENT_TEAM_PORTAL_DATA_DIR:-$(read_env_value LOCAL_AGENT_TEAM_PORTAL_DATA_DIR)}}"
PORTAL_DATA_DIR="${PORTAL_DATA_DIR:-${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}}"
if [[ -z "$PORTAL_DATA_DIR" ]]; then
  echo "TEAM_PORTAL_DATA_DIR is required to find saved Google OAuth credentials."
  exit 1
fi
OWNER_EMAIL="${BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL:-${GOOGLE_OWNER_EMAIL:-$(read_env_value BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL)}}"
OWNER_EMAIL="${OWNER_EMAIL:-$(read_env_value GOOGLE_OWNER_EMAIL)}"
if [[ -z "$OWNER_EMAIL" ]]; then
  echo "BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL or GOOGLE_OWNER_EMAIL is required."
  exit 1
fi
CONFIG_ENCRYPTION_KEY="${TEAM_PORTAL_CONFIG_ENCRYPTION_KEY:-$(read_env_value TEAM_PORTAL_CONFIG_ENCRYPTION_KEY)}"
if [[ -z "$CONFIG_ENCRYPTION_KEY" ]]; then
  echo "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY is required to decrypt saved Google OAuth credentials."
  exit 1
fi

SECRET_NAME="${BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON_SECRET:-$(read_env_value BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON_SECRET)}"
SECRET_NAME="${SECRET_NAME:-business-insights-google-oauth-credentials-json}"
TEMP_FILE="$(mktemp)"
cleanup() {
  rm -f "$TEMP_FILE"
}
trap cleanup EXIT

TEAM_PORTAL_DATA_DIR="$PORTAL_DATA_DIR" \
BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL="$OWNER_EMAIL" \
TEAM_PORTAL_CONFIG_ENCRYPTION_KEY="$CONFIG_ENCRYPTION_KEY" \
"$PYTHON_BIN" - "$TEMP_FILE" <<'PY'
import json
import os
import sys
from pathlib import Path

from bpmis_jira_tool.business_insights_sheet_refresh import GOOGLE_SHEETS_SCOPE, load_stored_google_sheets_credentials

output = Path(sys.argv[1])
credentials = load_stored_google_sheets_credentials(
    portal_data_dir=Path(os.environ["TEAM_PORTAL_DATA_DIR"]).expanduser(),
    owner_email=os.environ["BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL"],
    encryption_key=os.environ["TEAM_PORTAL_CONFIG_ENCRYPTION_KEY"],
)
payload = {
    "token": credentials.token,
    "refresh_token": credentials.refresh_token,
    "token_uri": credentials.token_uri,
    "client_id": credentials.client_id,
    "client_secret": credentials.client_secret,
    "scopes": list(credentials.scopes or [GOOGLE_SHEETS_SCOPE]),
}
if not payload.get("refresh_token"):
    raise SystemExit("Saved Google OAuth credentials do not include a refresh_token. Reconnect Google once.")
output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
PY

if ! "$GCLOUD_BIN" secrets describe "$SECRET_NAME" \
  --project "$PROJECT_ID" \
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
  >/dev/null 2>&1; then
  "$GCLOUD_BIN" secrets create "$SECRET_NAME" \
    --project "$PROJECT_ID" \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --replication-policy automatic
fi

"$GCLOUD_BIN" secrets versions add "$SECRET_NAME" \
  --project "$PROJECT_ID" \
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
  --data-file "$TEMP_FILE"

echo "Synced Google OAuth credentials to Secret Manager secret: $SECRET_NAME"
echo "Deploy with: BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON_SECRET=$SECRET_NAME CLOUD_RUN_IMAGE=... ./scripts/deploy_business_insights_sheet_refresh_cloud_run_job.sh"
