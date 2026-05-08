#!/usr/bin/env bash

if [[ -n "${CLOUD_RUN_IMAGE_POLICY_LOADED:-}" ]]; then
  return 0
fi
CLOUD_RUN_IMAGE_POLICY_LOADED=1

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

cloud_run_image_runtime_path_requires_image() {
  local changed_file="$1"
  case "$changed_file" in
    Dockerfile|requirements-cloud-run.txt|app.py|local_agent.py)
      return 0
      ;;
    bpmis_jira_tool/*|config/*|prd_briefing/*|static/*|templates/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

cloud_run_image_changed_between() {
  local base_commit="$1"
  local target_commit="$2"
  if [[ -z "$base_commit" || -z "$target_commit" || "$base_commit" == "$target_commit" ]]; then
    return 1
  fi
  if ! git -C "$ROOT_DIR" rev-parse --verify "$base_commit^{commit}" >/dev/null 2>&1; then
    return 0
  fi
  if ! git -C "$ROOT_DIR" rev-parse --verify "$target_commit^{commit}" >/dev/null 2>&1; then
    return 0
  fi

  local changed_files
  if ! changed_files="$(git -C "$ROOT_DIR" diff --name-only "$base_commit" "$target_commit" --)"; then
    return 0
  fi
  while IFS= read -r changed_file; do
    [[ -n "$changed_file" ]] || continue
    if cloud_run_image_runtime_path_requires_image "$changed_file"; then
      return 0
    fi
  done <<<"$changed_files"
  return 1
}

cloud_run_image_trigger_included_files_csv() {
  printf '%s\n' \
    ".github/workflows/cloud-run-image.yml" \
    "Dockerfile" \
    "cloudbuild.yaml" \
    "requirements-cloud-run.txt" \
    "app.py" \
    "local_agent.py" \
    "bpmis_jira_tool/**" \
    "config/**" \
    "prd_briefing/**" \
    "static/**" \
    "templates/**" \
    "scripts/build_cloud_run_image.sh" \
    "scripts/lib/team_env.sh" \
    "scripts/lib/cloud_run_image_policy.sh" \
    | paste -sd, -
}
