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
ACCOUNT_ARGS=()
CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED="${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}"
if [[ -n "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED" ]]; then
  ACCOUNT_ARGS=(--account "$CLOUD_RUN_DEPLOY_ACCOUNT_RESOLVED")
fi

REGION="${CLOUD_RUN_REGION:-asia-southeast1}"
REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
TAG="${CLOUD_RUN_IMAGE_TAG:-$(git -C "$ROOT_DIR" rev-parse HEAD)}"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

STARTED_AT="$(date +%s)"
echo "Cloud Build image: $IMAGE_URI"
echo "Cloud Build config: cloudbuild.yaml"

if [[ "${CLOUD_RUN_BUILD_IMAGE_DRY_RUN:-0}" == "1" ]]; then
  echo "Dry run only; unset CLOUD_RUN_BUILD_IMAGE_DRY_RUN to submit the build."
  echo "Deploy after build with: CLOUD_RUN_IMAGE=$IMAGE_URI ./scripts/deploy_cloud_run.sh"
  echo "Deploy UAT after build with: CLOUD_RUN_IMAGE=$IMAGE_URI ./scripts/deploy_cloud_run_uat.sh"
  exit 0
fi

cd "$ROOT_DIR"
if ! "$GCLOUD_BIN" artifacts repositories describe "$REPOSITORY" \
  --project "$PROJECT_ID" \
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
  --location "$REGION" \
  >/dev/null 2>&1; then
  echo "Creating Artifact Registry Docker repository: $REPOSITORY ($REGION)"
  "$GCLOUD_BIN" artifacts repositories create "$REPOSITORY" \
    --project "$PROJECT_ID" \
    ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
    --repository-format docker \
    --location "$REGION" \
    --description "Team portal Cloud Run prebuilt images"
fi

BUILD_ARGS=()
BUILD_MACHINE_TYPE="${CLOUD_RUN_BUILD_MACHINE_TYPE:-E2_HIGHCPU_8}"
if [[ -n "$BUILD_MACHINE_TYPE" && "$BUILD_MACHINE_TYPE" != "default" ]]; then
  BUILD_ARGS+=(--machine-type "$BUILD_MACHINE_TYPE")
fi
if [[ -n "${CLOUD_RUN_BUILD_TIMEOUT:-}" ]]; then
  BUILD_ARGS+=(--timeout "$CLOUD_RUN_BUILD_TIMEOUT")
fi
BUILD_DISK_SIZE="${CLOUD_RUN_BUILD_DISK_SIZE:-100}"
if [[ -n "$BUILD_DISK_SIZE" && "$BUILD_DISK_SIZE" != "default" ]]; then
  BUILD_ARGS+=(--disk-size "$BUILD_DISK_SIZE")
fi

SUBMIT_ARGS=(
  builds submit
  --project "$PROJECT_ID"
  ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"}
  --config cloudbuild.yaml
  --substitutions "_REGION=$REGION,_REPOSITORY=$REPOSITORY,_IMAGE_NAME=$IMAGE_NAME,_TAG=$TAG"
  ${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}
)

if [[ "${CLOUD_RUN_BUILD_STREAM_LOGS:-0}" == "1" ]]; then
  "$GCLOUD_BIN" "${SUBMIT_ARGS[@]}" .
else
  BUILD_ID="$("$GCLOUD_BIN" "${SUBMIT_ARGS[@]}" --async --format="value(id)" . | awk 'NF { value=$0 } END { print value }')"
  if [[ -z "$BUILD_ID" ]]; then
    echo "Cloud Build did not return a build ID."
    exit 1
  fi
  echo "Cloud Build ID: $BUILD_ID"
  while true; do
    BUILD_STATUS="$("$GCLOUD_BIN" builds describe "$BUILD_ID" \
      --project "$PROJECT_ID" \
      ${ACCOUNT_ARGS[@]+"${ACCOUNT_ARGS[@]}"} \
      --format="value(status)")"
    case "$BUILD_STATUS" in
      SUCCESS)
        break
        ;;
      FAILURE|INTERNAL_ERROR|TIMEOUT|CANCELLED|EXPIRED)
        echo "Cloud Build failed with status: $BUILD_STATUS"
        exit 1
        ;;
      *)
        echo "Cloud Build status: ${BUILD_STATUS:-unknown}"
        sleep "${CLOUD_RUN_BUILD_POLL_SECONDS:-5}"
        ;;
    esac
  done
fi

FINISHED_AT="$(date +%s)"
echo "Cloud Build image completed in $((FINISHED_AT - STARTED_AT))s"
echo "Deploy with: CLOUD_RUN_IMAGE=$IMAGE_URI ./scripts/deploy_cloud_run.sh"
echo "Deploy UAT with: CLOUD_RUN_IMAGE=$IMAGE_URI ./scripts/deploy_cloud_run_uat.sh"
