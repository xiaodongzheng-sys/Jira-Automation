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

REGION="${BUSINESS_INSIGHTS_REFRESH_REGION:-${CLOUD_RUN_REGION:-asia-southeast1}}"
JOB_NAME="${BUSINESS_INSIGHTS_REFRESH_JOB:-$(read_env_value BUSINESS_INSIGHTS_REFRESH_JOB)}"
JOB_NAME="${JOB_NAME:-business-insights-sheet-refresh}"
SCHEDULER_NAME="${BUSINESS_INSIGHTS_REFRESH_SCHEDULER:-$(read_env_value BUSINESS_INSIGHTS_REFRESH_SCHEDULER)}"
SCHEDULER_NAME="${SCHEDULER_NAME:-business-insights-sheet-refresh-daily}"
SCHEDULE="${BUSINESS_INSIGHTS_REFRESH_SCHEDULE:-$(read_env_value BUSINESS_INSIGHTS_REFRESH_SCHEDULE)}"
SCHEDULE="${SCHEDULE:-0 10 * * *}"
TIME_ZONE="${BUSINESS_INSIGHTS_REFRESH_TIME_ZONE:-$(read_env_value BUSINESS_INSIGHTS_REFRESH_TIME_ZONE)}"
TIME_ZONE="${TIME_ZONE:-Asia/Singapore}"
SHEET_URL="${BUSINESS_INSIGHTS_GOOGLE_SHEET_URL:-$(read_env_value BUSINESS_INSIGHTS_GOOGLE_SHEET_URL)}"
SHEET_URL="${SHEET_URL:-https://docs.google.com/spreadsheets/d/1F5MSUwnxg8AbGr3rQN1l8nXYkxrBU680FJYhTGzL9qo/edit?gid=2125394335#gid=2125394335}"
PORTAL_DATA_DIR="${BUSINESS_INSIGHTS_REFRESH_PORTAL_DATA_DIR:-/tmp/team-portal-runtime}"
PUBLIC_GCS_BUCKET="${TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET:-${CLOUD_RUN_PUBLIC_GCS_BUCKET:-$(read_env_value CLOUD_RUN_PUBLIC_GCS_BUCKET)}}"

CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED="${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}"
ACCOUNT_ARGS=()
if [[ -n "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED" ]]; then
  ACCOUNT_ARGS=(--account "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED")
fi

PROJECT_ARGS=(--project "$PROJECT_ID")
DRY_RUN="${BUSINESS_INSIGHTS_REFRESH_DRY_RUN:-0}"
if [[ "$DRY_RUN" != "1" ]]; then
  require_gcloud_noninteractive_deploy_auth "$GCLOUD_BIN" "$PROJECT_ID" "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED"
fi

run_cmd() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

existing_portal_service_account() {
  local service="${CLOUD_RUN_SERVICE:-team-portal}"
  "$GCLOUD_BIN" run services describe "$service" \
    "${PROJECT_ARGS[@]}" \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --region "$REGION" \
    --format='value(spec.template.spec.serviceAccountName)' 2>/dev/null || true
}

default_compute_service_account() {
  local project_number
  project_number="$("$GCLOUD_BIN" projects describe "$PROJECT_ID" \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --format='value(projectNumber)' 2>/dev/null || true)"
  if [[ -n "$project_number" ]]; then
    printf '%s-compute@developer.gserviceaccount.com\n' "$project_number"
  fi
}

JOB_SERVICE_ACCOUNT="${BUSINESS_INSIGHTS_REFRESH_SERVICE_ACCOUNT:-$(read_env_value BUSINESS_INSIGHTS_REFRESH_SERVICE_ACCOUNT)}"
JOB_SERVICE_ACCOUNT="${JOB_SERVICE_ACCOUNT:-$(existing_portal_service_account)}"
JOB_SERVICE_ACCOUNT="${JOB_SERVICE_ACCOUNT:-$(default_compute_service_account)}"
if [[ -z "$JOB_SERVICE_ACCOUNT" ]]; then
  echo "Could not resolve a Cloud Run Job service account. Set BUSINESS_INSIGHTS_REFRESH_SERVICE_ACCOUNT."
  exit 1
fi
SCHEDULER_SERVICE_ACCOUNT="${BUSINESS_INSIGHTS_REFRESH_SCHEDULER_SERVICE_ACCOUNT:-$(read_env_value BUSINESS_INSIGHTS_REFRESH_SCHEDULER_SERVICE_ACCOUNT)}"
SCHEDULER_SERVICE_ACCOUNT="${SCHEDULER_SERVICE_ACCOUNT:-$JOB_SERVICE_ACCOUNT}"

IMAGE_URI="${CLOUD_RUN_IMAGE:-}"
if [[ -z "$IMAGE_URI" ]]; then
  REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
  IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
  TAG="${BUSINESS_INSIGHTS_REFRESH_IMAGE_TAG:-${CLOUD_RUN_IMAGE_TAG:-$(git -C "$ROOT_DIR" rev-parse HEAD)}}"
  IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY_RUN CLOUD_RUN_IMAGE_TAG=$TAG ./scripts/build_cloud_run_image.sh"
  else
    CLOUD_RUN_IMAGE_TAG="$TAG" "$ROOT_DIR/scripts/build_cloud_run_image.sh"
  fi
fi

ENV_VARS=(
  "TEAM_PORTAL_DATA_DIR=$PORTAL_DATA_DIR"
  "BUSINESS_INSIGHTS_GOOGLE_SHEET_URL=$SHEET_URL"
  "GOOGLE_CLOUD_PROJECT=$PROJECT_ID"
)
if [[ -n "$PUBLIC_GCS_BUCKET" ]]; then
  ENV_VARS+=("TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET=$PUBLIC_GCS_BUCKET")
fi

SECRET_ARGS=()
OAUTH_JSON_SECRET="${BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON_SECRET:-$(read_env_value BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON_SECRET)}"
SA_JSON_SECRET="${BUSINESS_INSIGHTS_GOOGLE_SERVICE_ACCOUNT_JSON_SECRET:-$(read_env_value BUSINESS_INSIGHTS_GOOGLE_SERVICE_ACCOUNT_JSON_SECRET)}"
if [[ -n "$OAUTH_JSON_SECRET" ]]; then
  SECRET_ARGS=(--set-secrets "BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON=$OAUTH_JSON_SECRET:latest")
elif [[ -n "$SA_JSON_SECRET" ]]; then
  SECRET_ARGS=(--set-secrets "BUSINESS_INSIGHTS_GOOGLE_SERVICE_ACCOUNT_JSON=$SA_JSON_SECRET:latest")
else
  ENV_VARS+=("BUSINESS_INSIGHTS_GOOGLE_USE_APPLICATION_DEFAULT=1")
fi

IFS='|'
ENV_VARS_JOINED="${ENV_VARS[*]}"
unset IFS

echo "Business Insights refresh Job: $JOB_NAME"
echo "Cloud Run region: $REGION"
echo "Cloud Run image: $IMAGE_URI"
echo "Job service account: $JOB_SERVICE_ACCOUNT"
echo "Scheduler: $SCHEDULER_NAME ($SCHEDULE, $TIME_ZONE)"
if [[ -n "$PUBLIC_GCS_BUCKET" ]]; then
  echo "Publish bucket: gs://$PUBLIC_GCS_BUCKET"
fi
if [[ -n "$OAUTH_JSON_SECRET" ]]; then
  echo "Google Sheets auth: Secret Manager OAuth credentials ($OAUTH_JSON_SECRET)."
elif [[ -z "$SA_JSON_SECRET" ]]; then
  echo "Google Sheets auth: Application Default Credentials. Share the Sheet with: $JOB_SERVICE_ACCOUNT"
else
  echo "Google Sheets auth: Secret Manager service account JSON ($SA_JSON_SECRET)."
fi

run_cmd "$GCLOUD_BIN" run jobs deploy "$JOB_NAME" \
  "${PROJECT_ARGS[@]}" \
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
  --region "$REGION" \
  --image "$IMAGE_URI" \
  --service-account "$JOB_SERVICE_ACCOUNT" \
  --command python \
  --args scripts/refresh_business_insights_from_google_sheet.py \
  --tasks 1 \
  --max-retries "${BUSINESS_INSIGHTS_REFRESH_MAX_RETRIES:-1}" \
  --task-timeout "${BUSINESS_INSIGHTS_REFRESH_TASK_TIMEOUT:-1800s}" \
  --cpu "${BUSINESS_INSIGHTS_REFRESH_CPU:-1}" \
  --memory "${BUSINESS_INSIGHTS_REFRESH_MEMORY:-1Gi}" \
  --set-env-vars "^|^$ENV_VARS_JOINED" \
  ${SECRET_ARGS[@]+"${SECRET_ARGS[@]}"}

if [[ -n "$PUBLIC_GCS_BUCKET" ]]; then
  if ! run_cmd "$GCLOUD_BIN" storage buckets add-iam-policy-binding "gs://$PUBLIC_GCS_BUCKET" \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --member "serviceAccount:$JOB_SERVICE_ACCOUNT" \
    --role roles/storage.objectAdmin \
    --quiet; then
    echo "Warning: could not grant roles/storage.objectAdmin on gs://$PUBLIC_GCS_BUCKET to $JOB_SERVICE_ACCOUNT."
    echo "The refresh Job can still be scheduled, but publishing refreshed files needs this bucket permission."
  fi
fi

run_cmd "$GCLOUD_BIN" run jobs add-iam-policy-binding "$JOB_NAME" \
  "${PROJECT_ARGS[@]}" \
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
  --region "$REGION" \
  --member "serviceAccount:$SCHEDULER_SERVICE_ACCOUNT" \
  --role roles/run.invoker \
  --quiet

SCHEDULER_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"
if "$GCLOUD_BIN" scheduler jobs describe "$SCHEDULER_NAME" \
  "${PROJECT_ARGS[@]}" \
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
  --location "$REGION" >/dev/null 2>&1; then
  run_cmd "$GCLOUD_BIN" scheduler jobs update http "$SCHEDULER_NAME" \
    "${PROJECT_ARGS[@]}" \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --location "$REGION" \
    --schedule "$SCHEDULE" \
    --time-zone "$TIME_ZONE" \
    --uri "$SCHEDULER_URI" \
    --http-method POST \
    --oauth-service-account-email "$SCHEDULER_SERVICE_ACCOUNT"
else
  run_cmd "$GCLOUD_BIN" scheduler jobs create http "$SCHEDULER_NAME" \
    "${PROJECT_ARGS[@]}" \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --location "$REGION" \
    --schedule "$SCHEDULE" \
    --time-zone "$TIME_ZONE" \
    --uri "$SCHEDULER_URI" \
    --http-method POST \
    --oauth-service-account-email "$SCHEDULER_SERVICE_ACCOUNT"
fi

echo "Cloud Scheduler will run $JOB_NAME daily at $SCHEDULE ($TIME_ZONE)."
echo "Manual run: $GCLOUD_BIN run jobs execute $JOB_NAME --project $PROJECT_ID --region $REGION --wait"
