#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
source "$ROOT_DIR/scripts/lib/release_window_policy.sh"
source "$ROOT_DIR/scripts/lib/cloud_run_image_policy.sh"

STARTED_AT="$(date +%s)"
GCLOUD_BIN="${GCLOUD_BIN:-$(command -v gcloud || true)}"
GH_BIN="${GH_BIN:-$(command -v gh || true)}"
if [[ -z "$GCLOUD_BIN" && -x "$HOME/google-cloud-sdk/bin/gcloud" ]]; then
  GCLOUD_BIN="$HOME/google-cloud-sdk/bin/gcloud"
fi

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$(read_env_value GOOGLE_CLOUD_PROJECT)}"
if [[ -z "$PROJECT_ID" && -n "$GCLOUD_BIN" ]]; then
  PROJECT_ID="$("$GCLOUD_BIN" config get-value project 2>/dev/null || true)"
fi
if [[ "$PROJECT_ID" == "(unset)" ]]; then
  PROJECT_ID=""
fi
REGION="${CLOUD_RUN_REGION:-$(read_env_value CLOUD_RUN_REGION)}"
REGION="${REGION:-asia-southeast1}"
ARTIFACT_REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
WORKFLOW_NAME="${RELEASE_UAT_FAST_IMAGE_WORKFLOW:-Build Cloud Run image}"

project_args=()
if [[ -n "$PROJECT_ID" ]]; then
  project_args=(--project "$PROJECT_ID")
fi
account_args=()
DEPLOY_ACCOUNT="${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}"
if [[ -n "$DEPLOY_ACCOUNT" ]]; then
  account_args=(--account "$DEPLOY_ACCOUNT")
fi
require_gcloud_noninteractive_deploy_auth "$GCLOUD_BIN" "$PROJECT_ID" "$DEPLOY_ACCOUNT"

current_sha() {
  git -C "$ROOT_DIR" rev-parse HEAD
}

image_uri_for_sha() {
  local sha="$1"
  if [[ -z "$PROJECT_ID" ]]; then
    return 1
  fi
  printf '%s\n' "${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPOSITORY}/${IMAGE_NAME}:${sha}"
}

artifact_image_exists() {
  local image_uri="$1"
  [[ -n "$GCLOUD_BIN" ]] || return 1
  local image_package="${image_uri%:*}"
  local image_tag="${image_uri##*:}"
  "$GCLOUD_BIN" artifacts docker tags list "$image_package" \
    ${project_args[@]+"${project_args[@]}"} \
    ${account_args[@]+"${account_args[@]}"} \
    --filter="tag:$image_tag" \
    --format="value(tag)" \
    2>/dev/null | grep -Fx "$image_tag" >/dev/null
}

find_reusable_image_without_runtime_changes() {
  local sha="$1"
  if [[ "${RELEASE_UAT_FAST_REUSE_IMAGE_WITHOUT_RUNTIME_CHANGES:-1}" != "1" ]]; then
    return 1
  fi

  local scan_limit="${RELEASE_UAT_FAST_IMAGE_REUSE_SCAN_LIMIT:-50}"
  local candidate image_uri
  while IFS= read -r candidate; do
    [[ -n "$candidate" ]] || continue
    image_uri="$(image_uri_for_sha "$candidate")"
    if ! artifact_image_exists "$image_uri"; then
      continue
    fi
    if cloud_run_image_changed_between "$candidate" "$sha"; then
      echo "Nearest reusable UAT image candidate $candidate is too old; Cloud Run image inputs changed before $sha." >&2
      return 1
    fi
    echo "Reusing prebuilt UAT image from $candidate because Cloud Run image inputs did not change through $sha." >&2
    printf '%s\n' "$image_uri"
    return 0
  done < <(git -C "$ROOT_DIR" rev-list --first-parent --max-count="$scan_limit" "$sha")
  return 1
}

wait_for_github_image_workflow() {
  local sha="$1"
  if [[ "${RELEASE_UAT_FAST_WAIT_FOR_GITHUB_IMAGE:-1}" != "1" || -z "$GH_BIN" ]]; then
    return 0
  fi

  local run_id
  for _ in {1..12}; do
    run_id="$("$GH_BIN" run list \
      --workflow "$WORKFLOW_NAME" \
      --branch main \
      --json databaseId,headSha,status \
      --jq ".[] | select(.headSha == \"$sha\") | .databaseId" \
      --limit 20 2>/dev/null | head -n 1 || true)"
    if [[ -n "$run_id" ]]; then
      break
    fi
    sleep 5
  done

  if [[ -z "$run_id" ]]; then
    echo "No GitHub image workflow run found for $sha; continuing with UAT image fallback." >&2
    return 0
  fi

  local wait_seconds="${RELEASE_UAT_FAST_GITHUB_IMAGE_WAIT_SECONDS:-90}"
  local poll_seconds="${RELEASE_UAT_FAST_GITHUB_IMAGE_POLL_SECONDS:-5}"
  local deadline=$(( $(date +%s) + wait_seconds ))
  local state status conclusion
  echo "Waiting up to ${wait_seconds}s for GitHub image workflow run $run_id for $sha." >&2
  while (( $(date +%s) < deadline )); do
    state="$("$GH_BIN" run view "$run_id" --json status,conclusion --jq '[.status, (.conclusion // "")] | @tsv' 2>/dev/null || true)"
    status="${state%%$'\t'*}"
    conclusion="${state#*$'\t'}"
    if [[ "$status" == "completed" ]]; then
      if [[ "$conclusion" == "success" ]]; then
        echo "GitHub image workflow completed successfully for $sha." >&2
      else
        echo "GitHub image workflow completed with conclusion '${conclusion:-unknown}' for $sha; continuing with UAT image fallback." >&2
      fi
      return 0
    fi
    sleep "$poll_seconds"
  done

  echo "Timed out after ${wait_seconds}s waiting for GitHub image workflow run $run_id; continuing with UAT image fallback." >&2
}

run_timed_gate() {
  local started_at finished_at status=0
  started_at="$(date +%s)"
  run_gate || status=$?
  finished_at="$(date +%s)"
  record_deploy_timing "release_uat_fast.sh" "release_gate" "$started_at" "$finished_at" "$status" "profile=${RELEASE_UAT_FAST_GATE_PROFILE:-auto}" || true
  return "$status"
}

run_timed_image_prepare() {
  local sha="$1"
  local started_at finished_at status=0
  started_at="$(date +%s)"
  ensure_prebuilt_image "$sha" || status=$?
  finished_at="$(date +%s)"
  record_deploy_timing "release_uat_fast.sh" "image_prepare" "$started_at" "$finished_at" "$status" "sha=$sha wait_seconds=${RELEASE_UAT_FAST_GITHUB_IMAGE_WAIT_SECONDS:-90}" || true
  return "$status"
}

ensure_prebuilt_image() {
  local sha="$1"
  if [[ -n "${CLOUD_RUN_IMAGE:-}" ]]; then
    printf '%s\n' "$CLOUD_RUN_IMAGE"
    return 0
  fi

  local image_uri
  if ! image_uri="$(image_uri_for_sha "$sha")"; then
    echo "Skipping UAT prebuilt image lookup because Google Cloud project is not configured; UAT will deploy from source." >&2
    return 0
  fi
  if artifact_image_exists "$image_uri"; then
    echo "Prebuilt UAT image already exists: $image_uri" >&2
    printf '%s\n' "$image_uri"
    return 0
  fi

  local reusable_image_uri
  if reusable_image_uri="$(find_reusable_image_without_runtime_changes "$sha")"; then
    printf '%s\n' "$reusable_image_uri"
    return 0
  fi

  wait_for_github_image_workflow "$sha"
  if artifact_image_exists "$image_uri"; then
    echo "Prebuilt UAT image is ready: $image_uri" >&2
    printf '%s\n' "$image_uri"
    return 0
  fi

  if [[ "${RELEASE_UAT_FAST_BUILD_IMAGE:-1}" != "1" ]]; then
    echo "Prebuilt UAT image is not available and RELEASE_UAT_FAST_BUILD_IMAGE=0; UAT will deploy from source." >&2
    return 0
  fi
  if [[ "${RELEASE_UAT_FAST_BUILD_IMAGE_FALLBACK:-1}" != "1" ]]; then
    echo "Prebuilt UAT image is not available for $sha and local build fallback is disabled." >&2
    return 1
  fi

  echo "Building prebuilt UAT image locally for $sha." >&2
  GOOGLE_CLOUD_PROJECT="$PROJECT_ID" \
  CLOUD_RUN_IMAGE_TAG="$sha" \
  "$ROOT_DIR/scripts/build_cloud_run_image.sh" >&2
  printf '%s\n' "$image_uri"
}

run_gate() {
  if [[ "${RELEASE_UAT_FAST_SKIP_GATE:-0}" == "1" ]]; then
    echo "Skipping system full test gate because RELEASE_UAT_FAST_SKIP_GATE=1."
    return 0
  fi
  if [[ "${RELEASE_UAT_FAST_REUSE_VERIFIED_GATE:-1}" == "1" ]]; then
    if "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" \
      --check-proof \
      --proof-max-age-seconds "${RELEASE_UAT_FAST_GATE_PROOF_MAX_AGE_SECONDS:-7200}" \
      --profile "${RELEASE_UAT_FAST_GATE_PROFILE:-auto}" \
      --coverage-fail-under "${RELEASE_UAT_FAST_COVERAGE_FAIL_UNDER:-100}"; then
      return 0
    fi
  fi
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" \
    --skip-smoke \
    --profile "${RELEASE_UAT_FAST_GATE_PROFILE:-auto}" \
    --parallel-workers "${RELEASE_UAT_FAST_GATE_WORKERS:-4}" \
    --coverage-fail-under "${RELEASE_UAT_FAST_COVERAGE_FAIL_UNDER:-100}"
}

run_gate_and_image_in_parallel() {
  local sha="$1"
  local work_dir gate_log image_log image_file gate_status=0 image_status=0
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/uat-fast-release.XXXXXX")"
  gate_log="$work_dir/gate.log"
  image_log="$work_dir/image.log"
  image_file="$work_dir/image.txt"

  run_timed_gate >"$gate_log" 2>&1 &
  local gate_pid="$!"
  run_timed_image_prepare "$sha" >"$image_file" 2>"$image_log" &
  local image_pid="$!"

  wait "$gate_pid" || gate_status=$?
  cat "$gate_log" || true
  wait "$image_pid" || image_status=$?
  cat "$image_log" || true

  if (( gate_status != 0 )); then
    echo "UAT release gate failed; stopping before UAT deploy." >&2
    rm -rf "$work_dir"
    return "$gate_status"
  fi
  if (( image_status != 0 )); then
    echo "UAT prebuilt image preparation failed; stopping before UAT deploy." >&2
    rm -rf "$work_dir"
    return "$image_status"
  fi
  IMAGE_URI="$(tail -n 1 "$image_file" | tr -d '\r\n')"
  rm -rf "$work_dir"
  export IMAGE_URI
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
SHA="$(current_sha)"
run_gate_and_image_in_parallel "$SHA"
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
