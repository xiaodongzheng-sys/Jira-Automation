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
TRIGGER_REGION="${CLOUD_BUILD_TRIGGER_REGION:-asia-southeast1}"
CONNECTION_NAME="${CLOUD_BUILD_GITHUB_CONNECTION_NAME:-jira-automation-github}"
REPOSITORY_NAME="${CLOUD_BUILD_GITHUB_REPOSITORY_NAME:-jira-automation}"
REPO_OWNER="${CLOUD_BUILD_GITHUB_REPO_OWNER:-xiaodongzheng-sys}"
REPO_NAME="${CLOUD_BUILD_GITHUB_REPO_NAME:-Jira-Automation}"
REMOTE_URI="${CLOUD_BUILD_GITHUB_REMOTE_URI:-https://github.com/$REPO_OWNER/$REPO_NAME.git}"
BRANCH_PATTERN="${CLOUD_BUILD_IMAGE_BRANCH_PATTERN:-^main$}"
CLOUD_RUN_REGION_RESOLVED="${CLOUD_RUN_REGION:-$(read_env_value CLOUD_RUN_REGION)}"
CLOUD_RUN_REGION_RESOLVED="${CLOUD_RUN_REGION_RESOLVED:-asia-southeast1}"
REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
SERVICE_ACCOUNT="${CLOUD_BUILD_TRIGGER_SERVICE_ACCOUNT:-${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}}"
INCLUDED_FILES="$(cloud_run_image_trigger_included_files_csv)"

project_args=(--project "$PROJECT_ID")
repository_resource="projects/$PROJECT_ID/locations/$TRIGGER_REGION/connections/$CONNECTION_NAME/repositories/$REPOSITORY_NAME"
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
  --repository "$repository_resource"
  --branch-pattern "$BRANCH_PATTERN"
  --build-config cloudbuild.yaml
  --included-files "$INCLUDED_FILES"
  --include-logs-with-status
  --no-require-approval
  "${service_account_args[@]}"
)
substitutions="_REGION=$CLOUD_RUN_REGION_RESOLVED,_REPOSITORY=$REPOSITORY,_IMAGE_NAME=$IMAGE_NAME,_TAG=\$COMMIT_SHA"

if ! "$GCLOUD_BIN" builds connections describe "$CONNECTION_NAME" \
  "${project_args[@]}" \
  --region "$TRIGGER_REGION" \
  >/dev/null 2>&1; then
  echo "Cloud Build GitHub connection is missing: $CONNECTION_NAME ($TRIGGER_REGION)"
  echo "Create and authorize it first:"
  echo "$GCLOUD_BIN builds connections create github $CONNECTION_NAME --project $PROJECT_ID --region $TRIGGER_REGION"
  exit 1
fi

if ! "$GCLOUD_BIN" builds repositories describe "$REPOSITORY_NAME" \
  "${project_args[@]}" \
  --region "$TRIGGER_REGION" \
  --connection "$CONNECTION_NAME" \
  >/dev/null 2>&1; then
  echo "Creating Cloud Build repository mapping: $REPOSITORY_NAME -> $REMOTE_URI"
  "$GCLOUD_BIN" builds repositories create "$REPOSITORY_NAME" \
    "${project_args[@]}" \
    --region "$TRIGGER_REGION" \
    --connection "$CONNECTION_NAME" \
    --remote-uri "$REMOTE_URI"
fi

existing_trigger_id="$("$GCLOUD_BIN" builds triggers list \
  "${project_args[@]}" \
  --region "$TRIGGER_REGION" \
  --filter="name=$TRIGGER_NAME" \
  --format="value(id)" \
  --limit=1 2>/dev/null | head -n 1 || true)"

cd "$ROOT_DIR"
if [[ -n "$existing_trigger_id" && "${CLOUD_BUILD_IMAGE_TRIGGER_RECREATE:-0}" != "1" ]]; then
  echo "Cloud Build image trigger already exists: $TRIGGER_NAME ($existing_trigger_id)"
  echo "Set CLOUD_BUILD_IMAGE_TRIGGER_RECREATE=1 to delete and recreate it from this script."
elif [[ -n "$existing_trigger_id" ]]; then
  echo "Deleting existing Cloud Build image trigger before recreation: $TRIGGER_NAME ($existing_trigger_id)"
  "$GCLOUD_BIN" builds triggers delete "$existing_trigger_id" \
    "${project_args[@]}" \
    --region "$TRIGGER_REGION" \
    --quiet
  echo "Creating Cloud Build image trigger: $TRIGGER_NAME"
  "$GCLOUD_BIN" builds triggers create github \
    "${project_args[@]}" \
    --name "$TRIGGER_NAME" \
    "${common_args[@]}" \
    --substitutions "$substitutions"
else
  echo "Creating Cloud Build image trigger: $TRIGGER_NAME"
  "$GCLOUD_BIN" builds triggers create github \
    "${project_args[@]}" \
    --name "$TRIGGER_NAME" \
    "${common_args[@]}" \
    --substitutions "$substitutions"
fi

echo "Cloud Build image trigger is configured for $REPO_OWNER/$REPO_NAME on $BRANCH_PATTERN."
echo "Repository resource: $repository_resource"
echo "Included files: $INCLUDED_FILES"
