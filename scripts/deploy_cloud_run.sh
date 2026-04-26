#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

SERVICE="${CLOUD_RUN_SERVICE:-team-portal}"
REGION="${CLOUD_RUN_REGION:-asia-southeast1}"
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

BASE_URL="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"
LOCAL_AGENT_URL="${LOCAL_AGENT_BASE_URL:-$(read_env_value LOCAL_AGENT_BASE_URL)}"

ENV_VARS=(
  "TEAM_ALLOWED_EMAIL_DOMAINS=${TEAM_ALLOWED_EMAIL_DOMAINS:-$(read_env_value TEAM_ALLOWED_EMAIL_DOMAINS)}"
  "TEAM_PORTAL_DATA_DIR=/tmp/team-portal"
  "BPMIS_CALL_MODE=${BPMIS_CALL_MODE:-direct}"
  "LOCAL_AGENT_MODE=${LOCAL_AGENT_MODE:-sync}"
  "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=${LOCAL_AGENT_SOURCE_CODE_QA_ENABLED:-true}"
  "LOCAL_AGENT_SEATALK_ENABLED=${LOCAL_AGENT_SEATALK_ENABLED:-true}"
  "LOCAL_AGENT_BPMIS_ENABLED=${LOCAL_AGENT_BPMIS_ENABLED:-false}"
)
if [[ -n "$BASE_URL" ]]; then
  ENV_VARS+=("TEAM_PORTAL_BASE_URL=$BASE_URL")
fi
if [[ -n "$LOCAL_AGENT_URL" ]]; then
  ENV_VARS+=("LOCAL_AGENT_BASE_URL=$LOCAL_AGENT_URL")
fi

IFS=,
ENV_VARS_JOINED="${ENV_VARS[*]}"
unset IFS

cd "$ROOT_DIR"
"$GCLOUD_BIN" run deploy "$SERVICE" \
  "${PROJECT_ARGS[@]}" \
  --region "$REGION" \
  --source . \
  --allow-unauthenticated \
  --set-env-vars "$ENV_VARS_JOINED"
