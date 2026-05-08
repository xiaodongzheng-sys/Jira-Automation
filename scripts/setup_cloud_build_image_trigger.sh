#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
source "$ROOT_DIR/scripts/lib/cloud_run_image_policy.sh"

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
if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
  echo "Google Cloud project is not configured. Set GOOGLE_CLOUD_PROJECT or run: $GCLOUD_BIN config set project PROJECT_ID"
  exit 1
fi

TRIGGER_NAME="${CLOUD_BUILD_IMAGE_TRIGGER_NAME:-team-portal-cloud-run-image}"
TRIGGER_REGION="${CLOUD_BUILD_TRIGGER_REGION:-global}"
REPO_OWNER="${CLOUD_BUILD_GITHUB_REPO_OWNER:-xiaodongzheng-sys}"
REPO_NAME="${CLOUD_BUILD_GITHUB_REPO_NAME:-Jira-Automation}"
BRANCH_PATTERN="${CLOUD_BUILD_IMAGE_BRANCH_PATTERN:-^main$}"
CLOUD_RUN_REGION_RESOLVED="${CLOUD_RUN_REGION:-$(read_env_value CLOUD_RUN_REGION)}"
CLOUD_RUN_REGION_RESOLVED="${CLOUD_RUN_REGION_RESOLVED:-asia-southeast1}"
REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
SERVICE_ACCOUNT="${CLOUD_BUILD_TRIGGER_SERVICE_ACCOUNT:-${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}}"
INCLUDED_FILES="$(cloud_run_image_trigger_included_files_csv)"

project_args=(--project "$PROJECT_ID")
service_account_args=()
if [[ -n "$SERVICE_ACCOUNT" ]]; then
  if [[ "$SERVICE_ACCOUNT" == projects/* ]]; then
    service_account_args=(--service-account "$SERVICE_ACCOUNT")
  else
    service_account_args=(--service-account "projects/$PROJECT_ID/serviceAccounts/$SERVICE_ACCOUNT")
  fi
fi

common_args=(
  --region "$TRIGGER_REGION"
  --description "Prebuild Team Portal Cloud Run image for runtime changes"
  --repo-owner "$REPO_OWNER"
  --repo-name "$REPO_NAME"
  --branch-pattern "$BRANCH_PATTERN"
  --build-config cloudbuild.yaml
  --included-files "$INCLUDED_FILES"
  --include-logs-with-status
  --no-require-approval
  --substitutions "_REGION=$CLOUD_RUN_REGION_RESOLVED,_REPOSITORY=$REPOSITORY,_IMAGE_NAME=$IMAGE_NAME,_TAG=\$COMMIT_SHA"
  "${service_account_args[@]}"
)

existing_trigger_id="$("$GCLOUD_BIN" builds triggers list \
  "${project_args[@]}" \
  --filter="name=$TRIGGER_NAME" \
  --format="value(id)" \
  --limit=1 2>/dev/null | head -n 1 || true)"

cd "$ROOT_DIR"
if [[ -n "$existing_trigger_id" ]]; then
  echo "Updating Cloud Build image trigger: $TRIGGER_NAME ($existing_trigger_id)"
  "$GCLOUD_BIN" builds triggers update github "$existing_trigger_id" \
    "${project_args[@]}" \
    "${common_args[@]}"
else
  echo "Creating Cloud Build image trigger: $TRIGGER_NAME"
  "$GCLOUD_BIN" builds triggers create github \
    "${project_args[@]}" \
    --name "$TRIGGER_NAME" \
    "${common_args[@]}"
fi

echo "Cloud Build image trigger is configured for $REPO_OWNER/$REPO_NAME on $BRANCH_PATTERN."
echo "Included files: $INCLUDED_FILES"
