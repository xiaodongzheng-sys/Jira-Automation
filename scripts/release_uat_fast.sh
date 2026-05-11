#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
source "$ROOT_DIR/scripts/lib/release_window_policy.sh"

STARTED_AT="$(date +%s)"
GCLOUD_BIN="${GCLOUD_BIN:-$(command -v gcloud || true)}"
if [[ -z "$GCLOUD_BIN" && -x "$HOME/google-cloud-sdk/bin/gcloud" ]]; then
  GCLOUD_BIN="$HOME/google-cloud-sdk/bin/gcloud"
fi

run_gate() {
  if [[ "${RELEASE_UAT_FAST_SKIP_GATE:-0}" == "1" ]]; then
    echo "Skipping system full test gate because RELEASE_UAT_FAST_SKIP_GATE=1."
    return 0
  fi
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" --skip-smoke --parallel-workers "${RELEASE_UAT_FAST_GATE_WORKERS:-4}"
}

resolve_image_uri() {
  local project_id="${GOOGLE_CLOUD_PROJECT:-$(read_env_value GOOGLE_CLOUD_PROJECT)}"
  if [[ -z "$project_id" && -n "$GCLOUD_BIN" ]]; then
    project_id="$("$GCLOUD_BIN" config get-value project 2>/dev/null || true)"
  fi
  if [[ -z "$project_id" || "$project_id" == "(unset)" ]]; then
    echo "Google Cloud project is not configured. Set GOOGLE_CLOUD_PROJECT or run: $GCLOUD_BIN config set project PROJECT_ID" >&2
    return 1
  fi
  local region="${CLOUD_RUN_REGION:-$(read_env_value CLOUD_RUN_REGION)}"
  region="${region:-asia-southeast1}"
  local repository="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
  local image_name="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
  local tag="${CLOUD_RUN_IMAGE_TAG:-$(git -C "$ROOT_DIR" rev-parse HEAD)}"
  printf '%s\n' "${region}-docker.pkg.dev/${project_id}/${repository}/${image_name}:${tag}"
}

build_image_if_needed() {
  if [[ -n "${CLOUD_RUN_IMAGE:-}" ]]; then
    printf '%s\n' "$CLOUD_RUN_IMAGE"
    return 0
  fi
  if [[ "${RELEASE_UAT_FAST_BUILD_IMAGE:-1}" != "1" ]]; then
    return 0
  fi
  local image_uri
  image_uri="$(resolve_image_uri)"
  CLOUD_RUN_IMAGE_TAG="${image_uri##*:}" "$ROOT_DIR/scripts/build_cloud_run_image.sh" >&2
  printf '%s\n' "$image_uri"
}

print_timing_tail() {
  local timing_file
  timing_file="$(team_deploy_timing_file)"
  if [[ -f "$timing_file" ]]; then
    echo "Recent deploy timings: $timing_file"
    tail -n "${RELEASE_UAT_FAST_TIMING_LINES:-5}" "$timing_file" || true
  fi
}

cd "$ROOT_DIR"
enforce_release_window_target uat
run_gate
IMAGE_URI="$(build_image_if_needed)"
if [[ -n "$IMAGE_URI" ]]; then
  echo "Deploying UAT with prebuilt image: $IMAGE_URI"
  CLOUD_RUN_IMAGE="$IMAGE_URI" \
  CLOUD_RUN_UAT_SKIP_UNCHANGED="${CLOUD_RUN_UAT_SKIP_UNCHANGED:-1}" \
  CLOUD_RUN_UAT_PARALLEL_HOST_SYNC="${CLOUD_RUN_UAT_PARALLEL_HOST_SYNC:-1}" \
  "$ROOT_DIR/scripts/deploy_cloud_run_uat.sh"
else
  echo "Deploying UAT from source because RELEASE_UAT_FAST_BUILD_IMAGE=0 and CLOUD_RUN_IMAGE is empty."
  CLOUD_RUN_UAT_SKIP_UNCHANGED="${CLOUD_RUN_UAT_SKIP_UNCHANGED:-1}" \
  CLOUD_RUN_UAT_PARALLEL_HOST_SYNC="${CLOUD_RUN_UAT_PARALLEL_HOST_SYNC:-1}" \
  "$ROOT_DIR/scripts/deploy_cloud_run_uat.sh"
fi
print_timing_tail

FINISHED_AT="$(date +%s)"
echo "Fast UAT release completed in $((FINISHED_AT - STARTED_AT))s"
