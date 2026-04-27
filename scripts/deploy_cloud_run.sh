#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

SCRIPT_STARTED_AT="$(date +%s)"
SERVICE="${CLOUD_RUN_SERVICE:-team-portal}"
REGION="${CLOUD_RUN_REGION:-asia-southeast1}"
CLOUD_RUN_IMAGE="${CLOUD_RUN_IMAGE:-}"
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

cloud_run_hash() {
  if [[ ! -x "$PYTHON_BIN" ]]; then
    printf 'unknown\n'
    return 0
  fi
  CLOUD_RUN_HASH_ROOT="$ROOT_DIR" "$PYTHON_BIN" - <<'PY'
import hashlib
import os
import subprocess
from pathlib import Path

root = Path(os.environ["CLOUD_RUN_HASH_ROOT"])
included_roots = {
    "bpmis_jira_tool",
    "config",
    "prd_briefing",
    "static",
    "templates",
}
included_files = {
    "Dockerfile",
    "requirements-cloud-run.txt",
    "app.py",
    "local_agent.py",
    "jira_web_config.json",
    "scripts/deploy_cloud_run.sh",
    "scripts/deploy_cloud_run_full.sh",
}
excluded_parts = {
    ".git",
    ".venv",
    ".team-portal",
    "__pycache__",
    "node_modules",
    "tmp",
}

def iter_files():
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            capture_output=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return []
    paths = []
    for raw in completed.stdout.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode("utf-8", errors="surrogateescape")
        parts = set(Path(rel).parts)
        first_part = Path(rel).parts[0] if Path(rel).parts else ""
        if rel not in included_files and first_part not in included_roots:
            continue
        if parts & excluded_parts:
            continue
        if rel in {".env", ".env.example"} or rel.startswith(".env."):
            continue
        paths.append(rel)
    return sorted(paths)

digest = hashlib.sha256()
paths = iter_files()
if not paths:
    print("unknown")
    raise SystemExit(0)
for rel in paths:
    path = root / rel
    if not path.is_file():
        continue
    digest.update(rel.encode("utf-8", errors="surrogateescape") + b"\0")
    digest.update(path.read_bytes())
    digest.update(b"\0")
print(digest.hexdigest()[:24])
PY
}

hash_text() {
  if [[ ! -x "$PYTHON_BIN" ]]; then
    printf 'unknown\n'
    return 0
  fi
  local payload
  payload="$(cat)"
  HASH_TEXT_PAYLOAD="$payload" "$PYTHON_BIN" - <<'PY'
import hashlib
import os

print(hashlib.sha256(os.environ.get("HASH_TEXT_PAYLOAD", "").encode("utf-8")).hexdigest()[:24])
PY
}

DESCRIBE_STARTED_AT="$(date +%s)"
EXISTING_SERVICE_URL="$("$GCLOUD_BIN" run services describe "$SERVICE" ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} --region "$REGION" --format='value(status.url)' 2>/dev/null || true)"
DESCRIBE_FINISHED_AT="$(date +%s)"
echo "Cloud Run describe completed in $((DESCRIBE_FINISHED_AT - DESCRIBE_STARTED_AT))s"
BASE_URL="${CLOUD_RUN_TEAM_PORTAL_BASE_URL:-${EXISTING_SERVICE_URL:-}}"
LOCAL_AGENT_URL="${CLOUD_RUN_LOCAL_AGENT_BASE_URL:-${LOCAL_AGENT_PUBLIC_URL:-$(read_env_value LOCAL_AGENT_PUBLIC_URL)}}"
LOCAL_AGENT_URL="${LOCAL_AGENT_URL:-${LOCAL_AGENT_BASE_URL:-$(read_env_value LOCAL_AGENT_BASE_URL)}}"
if [[ "$LOCAL_AGENT_URL" =~ ^https?://(127\.0\.0\.1|localhost)(:|/) ]]; then
  echo "Cloud Run cannot reach a localhost LOCAL_AGENT_BASE_URL."
  echo "Set CLOUD_RUN_LOCAL_AGENT_BASE_URL or LOCAL_AGENT_PUBLIC_URL to the Mac local-agent public URL."
  exit 1
fi

ENV_VARS=(
  "TEAM_ALLOWED_EMAIL_DOMAINS=${TEAM_ALLOWED_EMAIL_DOMAINS:-$(read_env_value TEAM_ALLOWED_EMAIL_DOMAINS)}"
  "TEAM_ALLOWED_EMAILS=${TEAM_ALLOWED_EMAILS:-$(read_env_value TEAM_ALLOWED_EMAILS)}"
  "TEAM_PORTAL_DATA_DIR=/tmp/team-portal"
  "GOOGLE_OAUTH_CLIENT_SECRET_FILE=${GOOGLE_OAUTH_CLIENT_SECRET_FILE:-/secrets/google/client_secret.json}"
  "BPMIS_BASE_URL=${BPMIS_BASE_URL:-$(read_env_value BPMIS_BASE_URL)}"
  "SOURCE_CODE_QA_OWNER_EMAIL=${SOURCE_CODE_QA_OWNER_EMAIL:-xiaodong.zheng@npt.sg}"
  "SOURCE_CODE_QA_ADMIN_EMAILS=${SOURCE_CODE_QA_ADMIN_EMAILS:-xiaodong.zheng@npt.sg,xiaodong.zheng1991@gmail.com}"
  "SOURCE_CODE_QA_QUERY_SYNC_MODE=${SOURCE_CODE_QA_QUERY_SYNC_MODE:-background}"
  "BPMIS_CALL_MODE=${BPMIS_CALL_MODE:-local_agent}"
  "LOCAL_AGENT_MODE=${LOCAL_AGENT_MODE:-sync}"
  "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=${LOCAL_AGENT_SOURCE_CODE_QA_ENABLED:-true}"
  "LOCAL_AGENT_SEATALK_ENABLED=${LOCAL_AGENT_SEATALK_ENABLED:-true}"
  "LOCAL_AGENT_BPMIS_ENABLED=${LOCAL_AGENT_BPMIS_ENABLED:-true}"
  "GUNICORN_WORKERS=${GUNICORN_WORKERS:-1}"
)
if [[ -n "$BASE_URL" ]]; then
  ENV_VARS+=("TEAM_PORTAL_BASE_URL=$BASE_URL")
fi
if [[ -n "$LOCAL_AGENT_URL" ]]; then
  ENV_VARS+=("LOCAL_AGENT_BASE_URL=$LOCAL_AGENT_URL")
fi

IFS='|'
ENV_VARS_JOINED="${ENV_VARS[*]}"
unset IFS

SOURCE_HASH="$(cloud_run_hash)"
DEPLOY_HASH="$(printf '%s\n%s\n%s\n' "$SOURCE_HASH" "$ENV_VARS_JOINED" "$CLOUD_RUN_IMAGE" | hash_text)"
ENV_VARS+=("TEAM_PORTAL_DEPLOY_HASH=$DEPLOY_HASH")
IFS='|'
ENV_VARS_JOINED="${ENV_VARS[*]}"
unset IFS

if [[ "${CLOUD_RUN_SKIP_UNCHANGED:-0}" == "1" && "$DEPLOY_HASH" != "unknown" ]]; then
  EXISTING_DEPLOY_HASH="$("$GCLOUD_BIN" run services describe "$SERVICE" ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} --region "$REGION" --format='value(spec.template.spec.containers[0].env[?name="TEAM_PORTAL_DEPLOY_HASH"].value)' 2>/dev/null || true)"
  if [[ "$EXISTING_DEPLOY_HASH" == "$DEPLOY_HASH" ]]; then
    echo "Cloud Run deploy skipped: source and deploy env hash are unchanged ($DEPLOY_HASH)."
    exit 0
  fi
fi

echo "Cloud Run service: $SERVICE"
echo "Cloud Run region: $REGION"
if [[ -n "$CLOUD_RUN_IMAGE" ]]; then
  echo "Cloud Run image: $CLOUD_RUN_IMAGE"
fi
echo "Cloud Run source hash: $SOURCE_HASH"
echo "Cloud Run deploy hash: $DEPLOY_HASH"
PREFLIGHT_FINISHED_AT="$(date +%s)"
echo "Cloud Run preflight completed in $((PREFLIGHT_FINISHED_AT - SCRIPT_STARTED_AT))s"
if [[ "${CLOUD_RUN_DEPLOY_DRY_RUN:-0}" == "1" ]]; then
  echo "Dry run only; set CLOUD_RUN_DEPLOY_DRY_RUN=0 or unset it to deploy."
  exit 0
fi

cd "$ROOT_DIR"
DEPLOY_SOURCE_ARGS=(--source .)
if [[ -n "$CLOUD_RUN_IMAGE" ]]; then
  DEPLOY_SOURCE_ARGS=(--image "$CLOUD_RUN_IMAGE")
fi
DEPLOY_STARTED_AT="$(date +%s)"
"$GCLOUD_BIN" run deploy "$SERVICE" \
  ${PROJECT_ARGS[@]+"${PROJECT_ARGS[@]}"} \
  --region "$REGION" \
  "${DEPLOY_SOURCE_ARGS[@]}" \
  --allow-unauthenticated \
  --max-instances="${CLOUD_RUN_MAX_INSTANCES:-1}" \
  --set-env-vars "^|^$ENV_VARS_JOINED"
DEPLOY_FINISHED_AT="$(date +%s)"
echo "Cloud Run deploy completed in $((DEPLOY_FINISHED_AT - DEPLOY_STARTED_AT))s"
echo "Cloud Run script completed in $((DEPLOY_FINISHED_AT - SCRIPT_STARTED_AT))s"
