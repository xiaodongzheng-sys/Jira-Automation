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

REGION="${CLOUD_RUN_REGION:-asia-southeast1}"
REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
TAG="${CLOUD_RUN_IMAGE_TAG:-$(date +%Y%m%d-%H%M%S)}"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${TAG}"

STARTED_AT="$(date +%s)"
echo "Cloud Build image: $IMAGE_URI"
echo "Cloud Build config: cloudbuild.yaml"

if [[ "${CLOUD_RUN_BUILD_IMAGE_DRY_RUN:-0}" == "1" ]]; then
  echo "Dry run only; unset CLOUD_RUN_BUILD_IMAGE_DRY_RUN to submit the build."
  echo "Deploy after build with: CLOUD_RUN_IMAGE=$IMAGE_URI ./scripts/deploy_cloud_run.sh"
  exit 0
fi

cd "$ROOT_DIR"
"$GCLOUD_BIN" builds submit \
  --project "$PROJECT_ID" \
  --config cloudbuild.yaml \
  --substitutions "_REGION=$REGION,_REPOSITORY=$REPOSITORY,_IMAGE_NAME=$IMAGE_NAME,_TAG=$TAG" \
  .

FINISHED_AT="$(date +%s)"
echo "Cloud Build image completed in $((FINISHED_AT - STARTED_AT))s"
echo "Deploy with: CLOUD_RUN_IMAGE=$IMAGE_URI ./scripts/deploy_cloud_run.sh"
