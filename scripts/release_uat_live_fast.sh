#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
source "$ROOT_DIR/scripts/lib/cloud_run_image_policy.sh"

STARTED_AT="$(date +%s)"
GCLOUD_BIN="${GCLOUD_BIN:-$(command -v gcloud || true)}"
GH_BIN="${GH_BIN:-$(command -v gh || true)}"
if [[ -z "$GCLOUD_BIN" && -x "$HOME/google-cloud-sdk/bin/gcloud" ]]; then
  GCLOUD_BIN="$HOME/google-cloud-sdk/bin/gcloud"
fi

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$(read_env_value GOOGLE_CLOUD_PROJECT)}"
REGION="${CLOUD_RUN_REGION:-$(read_env_value CLOUD_RUN_REGION)}"
REGION="${REGION:-asia-southeast1}"
SERVICE="${CLOUD_RUN_SERVICE:-$(read_env_value CLOUD_RUN_SERVICE)}"
SERVICE="${SERVICE:-team-portal}"
UAT_TAG="${CLOUD_RUN_UAT_TAG:-$(read_env_value CLOUD_RUN_UAT_TAG)}"
UAT_TAG="${UAT_TAG:-uat}"
ARTIFACT_REPOSITORY="${CLOUD_RUN_ARTIFACT_REPOSITORY:-team-portal}"
IMAGE_NAME="${CLOUD_RUN_IMAGE_NAME:-team-portal}"
WORKFLOW_NAME="${RELEASE_UAT_LIVE_IMAGE_WORKFLOW:-Build Cloud Run image}"

project_args=()
if [[ -n "$PROJECT_ID" ]]; then
  project_args=(--project "$PROJECT_ID")
fi

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
    --filter="tag:$image_tag" \
    --format="value(tag)" \
    2>/dev/null | grep -Fx "$image_tag" >/dev/null
}

find_reusable_image_without_runtime_changes() {
  local sha="$1"
  if [[ "${RELEASE_UAT_LIVE_REUSE_IMAGE_WITHOUT_RUNTIME_CHANGES:-1}" != "1" ]]; then
    return 1
  fi

  local scan_limit="${RELEASE_UAT_LIVE_IMAGE_REUSE_SCAN_LIMIT:-50}"
  local candidate image_uri
  while IFS= read -r candidate; do
    [[ -n "$candidate" ]] || continue
    image_uri="$(image_uri_for_sha "$candidate")"
    if ! artifact_image_exists "$image_uri"; then
      continue
    fi
    if cloud_run_image_changed_between "$candidate" "$sha"; then
      echo "Nearest reusable image candidate $candidate is too old; Cloud Run image inputs changed before $sha." >&2
      return 1
    fi
    echo "Reusing prebuilt image from $candidate because Cloud Run image inputs did not change through $sha." >&2
    printf '%s\n' "$image_uri"
    return 0
  done < <(git -C "$ROOT_DIR" rev-list --first-parent --max-count="$scan_limit" "$sha")
  return 1
}

wait_for_github_image_workflow() {
  local sha="$1"
  if [[ "${RELEASE_UAT_LIVE_WAIT_FOR_GITHUB_IMAGE:-1}" != "1" || -z "$GH_BIN" ]]; then
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
      echo "Waiting for GitHub image workflow run $run_id for $sha." >&2
      "$GH_BIN" run watch "$run_id" --exit-status >&2
      return 0
    fi
    sleep 5
  done

  echo "No GitHub image workflow run found for $sha; continuing with image fallback." >&2
}

ensure_prebuilt_image() {
  local sha="$1"
  local image_uri
  image_uri="$(image_uri_for_sha "$sha")"
  if artifact_image_exists "$image_uri"; then
    echo "Prebuilt image already exists: $image_uri" >&2
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
    echo "Prebuilt image is ready: $image_uri" >&2
    printf '%s\n' "$image_uri"
    return 0
  fi

  if [[ "${RELEASE_UAT_LIVE_BUILD_IMAGE_FALLBACK:-1}" != "1" ]]; then
    echo "Prebuilt image is not available for $sha and local build fallback is disabled." >&2
    return 1
  fi

  echo "Building prebuilt image locally for $sha." >&2
  GOOGLE_CLOUD_PROJECT="$PROJECT_ID" \
  CLOUD_RUN_IMAGE_TAG="$sha" \
  "$ROOT_DIR/scripts/build_cloud_run_image.sh" >&2
  printf '%s\n' "$image_uri"
}

tag_url_from_service_url() {
  local service_url="$1"
  SERVICE_URL_VALUE="$service_url" UAT_TAG_VALUE="$UAT_TAG" "$PYTHON_BIN" - <<'PY'
import os
from urllib.parse import urlsplit, urlunsplit

service_url = os.environ.get("SERVICE_URL_VALUE", "").strip()
tag = os.environ.get("UAT_TAG_VALUE", "").strip()
parts = urlsplit(service_url)
if not parts.scheme or not parts.netloc or not tag:
    raise SystemExit(1)
print(urlunsplit((parts.scheme, f"{tag}---{parts.netloc}", "", "", "")))
PY
}

resolve_uat_url() {
  if [[ -n "${RELEASE_UAT_URL:-}" ]]; then
    printf '%s\n' "$RELEASE_UAT_URL"
    return 0
  fi
  local service_url
  service_url="$("$GCLOUD_BIN" run services describe "$SERVICE" \
    ${project_args[@]+"${project_args[@]}"} \
    --region "$REGION" \
    --format="value(status.url)")"
  tag_url_from_service_url "$service_url"
}

resolve_live_url() {
  local live_url="${RELEASE_LIVE_URL:-${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}}"
  if [[ -z "$live_url" ]]; then
    echo "Set RELEASE_LIVE_URL or TEAM_PORTAL_BASE_URL before running live smoke checks." >&2
    return 1
  fi
  printf '%s\n' "$live_url"
}

run_gate() {
  if [[ "${RELEASE_UAT_LIVE_SKIP_GATE:-0}" == "1" ]]; then
    echo "Skipping system full test gate because RELEASE_UAT_LIVE_SKIP_GATE=1."
    return 0
  fi
  if [[ "${RELEASE_UAT_LIVE_REUSE_VERIFIED_GATE:-1}" == "1" ]]; then
    if "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" \
      --check-proof \
      --proof-max-age-seconds "${RELEASE_UAT_LIVE_GATE_PROOF_MAX_AGE_SECONDS:-7200}" \
      --coverage-fail-under "${RELEASE_UAT_LIVE_COVERAGE_FAIL_UNDER:-100}"; then
      return 0
    fi
  fi
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" --skip-smoke --parallel-workers "${RELEASE_UAT_LIVE_GATE_WORKERS:-4}"
}

run_gate_and_image_in_parallel() {
  local sha="$1"
  local work_dir gate_log image_log image_file gate_status=0 image_status=0
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/uat-live-release.XXXXXX")"
  gate_log="$work_dir/gate.log"
  image_log="$work_dir/image.log"
  image_file="$work_dir/image.txt"

  run_gate >"$gate_log" 2>&1 &
  local gate_pid="$!"
  ensure_prebuilt_image "$sha" >"$image_file" 2>"$image_log" &
  local image_pid="$!"

  wait "$gate_pid" || gate_status=$?
  cat "$gate_log" || true
  wait "$image_pid" || image_status=$?
  cat "$image_log" || true

  if (( gate_status != 0 )); then
    echo "Release gate failed; stopping before UAT deploy." >&2
    rm -rf "$work_dir"
    return "$gate_status"
  fi
  if (( image_status != 0 )); then
    echo "Prebuilt image preparation failed; stopping before UAT deploy." >&2
    rm -rf "$work_dir"
    return "$image_status"
  fi
  IMAGE_URI="$(tail -n 1 "$image_file" | tr -d '\r\n')"
  rm -rf "$work_dir"
  if [[ -z "$IMAGE_URI" ]]; then
    echo "Prebuilt image preparation did not return an image URI." >&2
    return 1
  fi
  export IMAGE_URI
}

smoke() {
  local expected_revision="$1"
  shift
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_system_full_test_gate.py" --smoke-only \
    --uat-url "$UAT_URL" \
    --live-url "$LIVE_URL" \
    --expected-revision "$expected_revision" \
    "$@"
}

live_revision() {
  curl -fsS --max-time 10 "${LIVE_URL%/}/healthz" | "$PYTHON_BIN" -c 'import json, sys; print(json.load(sys.stdin).get("revision", ""))'
}

require_gcloud_noninteractive_auth() {
  if [[ -z "$GCLOUD_BIN" ]]; then
    echo "gcloud is not installed. Install Google Cloud SDK first." >&2
    return 1
  fi
  if "$GCLOUD_BIN" auth print-access-token >/dev/null 2>&1; then
    return 0
  fi
  {
    echo "gcloud credentials are not usable non-interactively."
    echo "Run: gcloud auth login"
    echo "Or configure a non-interactive deploy identity before promotion."
  } >&2
  return 1
}

print_timing_report() {
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/report_deploy_timings.py" --limit "${RELEASE_UAT_LIVE_TIMING_LIMIT:-20}" || true
}

cd "$ROOT_DIR"
SHA="$(current_sha)"
LIVE_URL="$(resolve_live_url)"
if [[ "$(live_revision || true)" == "$SHA" && "${RELEASE_UAT_LIVE_SKIP_GCLOUD_WHEN_LIVE_CURRENT:-1}" == "1" ]]; then
  echo "Live already serves $SHA; skipping Cloud Run/UAT gcloud steps."
  TEAM_STACK_HOST_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"
  "$TEAM_STACK_HOST_ROOT/scripts/run_team_stack.sh" doctor
  print_timing_report
  FINISHED_AT="$(date +%s)"
  echo "Fast UAT/live release confirmed Live already current in $((FINISHED_AT - STARTED_AT))s"
  exit 0
fi

require_gcloud_noninteractive_auth
IMAGE_URI=""
run_gate_and_image_in_parallel "$SHA"

echo "Deploying UAT with prebuilt image: $IMAGE_URI"
CLOUD_RUN_IMAGE="$IMAGE_URI" \
CLOUD_RUN_UAT_SKIP_UNCHANGED="${CLOUD_RUN_UAT_SKIP_UNCHANGED:-1}" \
CLOUD_RUN_UAT_PARALLEL_HOST_SYNC="${CLOUD_RUN_UAT_PARALLEL_HOST_SYNC:-1}" \
"$ROOT_DIR/scripts/deploy_cloud_run_uat.sh"

UAT_URL="$(resolve_uat_url)"

if [[ "$(live_revision || true)" == "$SHA" ]]; then
  echo "Live already serves $SHA; skipping pre-promotion smoke and promote step."
else
  smoke "$SHA"
  GOOGLE_CLOUD_PROJECT="$PROJECT_ID" \
  CLOUD_RUN_DEPLOY_ACCOUNT="${CLOUD_RUN_DEPLOY_ACCOUNT:-$(read_env_value CLOUD_RUN_DEPLOY_ACCOUNT)}" \
  "$ROOT_DIR/scripts/promote_uat_to_live.sh"
fi

smoke "$SHA" --expect-live-promoted
TEAM_STACK_HOST_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"
"$TEAM_STACK_HOST_ROOT/scripts/run_team_stack.sh" doctor
print_timing_report

FINISHED_AT="$(date +%s)"
echo "Fast UAT/live release completed in $((FINISHED_AT - STARTED_AT))s"
