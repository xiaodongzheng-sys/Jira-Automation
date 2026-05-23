#!/usr/bin/env bash

if [[ -n "${TEAM_ENV_HELPERS_LOADED:-}" ]]; then
  return 0
fi
TEAM_ENV_HELPERS_LOADED=1

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]] && command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
fi
case ":${PYTHONPATH:-}:" in
  *":$ROOT_DIR:"*) ;;
  *) export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}" ;;
esac

current_release_revision() {
  if command -v git >/dev/null 2>&1 && [[ -x "$PYTHON_BIN" ]]; then
    ROOT_DIR_VALUE="$ROOT_DIR" "$PYTHON_BIN" - <<'PY'
import hashlib
import os
import subprocess
import sys

root_dir = os.environ.get("ROOT_DIR_VALUE", "")
if not root_dir:
    print("unknown")
    raise SystemExit(0)

def run_git(*args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", root_dir, *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout

try:
    head = run_git("rev-parse", "HEAD").strip()
except (FileNotFoundError, subprocess.CalledProcessError):
    print("unknown")
    raise SystemExit(0)

try:
    diff_text = run_git("diff", "--no-ext-diff", "--binary", "HEAD", "--", ".")
    untracked = run_git("ls-files", "--others", "--exclude-standard")
except subprocess.CalledProcessError:
    print(head or "unknown")
    raise SystemExit(0)

dirty_material = diff_text
if untracked.strip():
    dirty_material += "\n--UNTRACKED--\n" + untracked

if dirty_material.strip():
    fingerprint = hashlib.sha1(dirty_material.encode("utf-8")).hexdigest()[:12]
    print(f"{head}-dirty-{fingerprint}")
else:
    print(head or "unknown")
PY
    return 0
  fi
  printf 'unknown\n'
}

fetch_healthz_field() {
  local host="$1"
  local port="$2"
  local field_name="$3"
  local payload
  payload="$(curl -fsS --max-time 5 "http://$host:$port/healthz" 2>/dev/null)" || return 1
  if [[ ! -x "$PYTHON_BIN" ]]; then
    return 1
  fi
  HEALTHZ_PAYLOAD="$payload" HEALTHZ_FIELD="$field_name" "$PYTHON_BIN" - <<'PY'
import json
import os
import sys

payload = os.environ.get("HEALTHZ_PAYLOAD", "")
field_name = os.environ.get("HEALTHZ_FIELD", "")
try:
    data = json.loads(payload)
except json.JSONDecodeError:
    sys.exit(1)

value = data.get(field_name)
if value is None:
    sys.exit(1)
print(value)
PY
}

portal_revision_matches() {
  local host="$1"
  local port="$2"
  local expected_revision="${3:-$(current_release_revision)}"
  local served_revision
  served_revision="$(fetch_healthz_field "$host" "$port" revision 2>/dev/null)" || return 1
  [[ "$served_revision" == "$expected_revision" ]]
}

read_env_value() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" || ! -x "$PYTHON_BIN" ]]; then
    return 0
  fi
  "$PYTHON_BIN" - <<PY
from dotenv import dotenv_values
values = dotenv_values("$ENV_FILE")
value = values.get("$key", "")
print(value if value is not None else "")
PY
}

is_loopback_http_url() {
  local value="${1:-}"
  [[ "$value" =~ ^https?://(127\.0\.0\.1|localhost)(:|/) ]]
}

resolve_cloud_run_local_agent_url() {
  local explicit_url="${CLOUD_RUN_LOCAL_AGENT_BASE_URL:-${LOCAL_AGENT_PUBLIC_URL:-$(read_env_value LOCAL_AGENT_PUBLIC_URL)}}"
  if [[ -n "$explicit_url" ]]; then
    printf '%s\n' "$explicit_url"
    return 0
  fi

  local local_agent_url="${LOCAL_AGENT_BASE_URL:-$(read_env_value LOCAL_AGENT_BASE_URL)}"
  if [[ -n "$local_agent_url" ]] && ! is_loopback_http_url "$local_agent_url"; then
    printf '%s\n' "$local_agent_url"
    return 0
  fi

  local portal_url="${TEAM_PORTAL_BASE_URL:-$(read_env_value TEAM_PORTAL_BASE_URL)}"
  if [[ -n "$portal_url" ]] && ! is_loopback_http_url "$portal_url"; then
    printf '%s\n' "$portal_url"
    return 0
  fi

  printf '%s\n' "$local_agent_url"
}

read_env_values() {
  if [[ $# -eq 0 ]]; then
    return 0
  fi
  if [[ ! -f "$ENV_FILE" || ! -x "$PYTHON_BIN" ]]; then
    for _ in "$@"; do
      printf '\n'
    done
    return 0
  fi
  local keys=("$@")
  local keys_blob
  keys_blob="$(printf '%s\n' "${keys[@]}")"
  TEAM_ENV_KEYS="$keys_blob" "$PYTHON_BIN" - <<PY
import os
from dotenv import dotenv_values

values = dotenv_values("$ENV_FILE")
keys = os.environ.get("TEAM_ENV_KEYS", "").splitlines()
for key in keys:
    value = values.get(key, "")
    print(value if value is not None else "")
PY
}

export_env_file() {
  if [[ ! -f "$ENV_FILE" || ! -x "$PYTHON_BIN" ]]; then
    return 0
  fi
  while IFS= read -r -d '' pair; do
    if [[ -n "$pair" ]]; then
      export "$pair"
    fi
  done < <("$PYTHON_BIN" - <<PY
import os
import re
import sys
from dotenv import dotenv_values

values = dotenv_values("$ENV_FILE")
name_pattern = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for key, value in values.items():
    if not key or value is None or not name_pattern.match(str(key)):
        continue
    if key in os.environ:
        continue
    os.write(1, f"{key}={value}".encode("utf-8") + b"\0")
PY
)
}

resolve_team_data_dir() {
  local data_dir="${1:-}"
  data_dir="${data_dir:-$ROOT_DIR/.team-portal}"
  if [[ "$data_dir" != /* ]]; then
    data_dir="$ROOT_DIR/$data_dir"
  fi
  printf '%s\n' "$data_dir"
}

meeting_recorder_active_recordings() {
  local data_dir="${1:-${LOCAL_AGENT_TEAM_PORTAL_DATA_DIR:-${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}}}"
  data_dir="$(resolve_team_data_dir "$data_dir")"
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Python is required to inspect active Meeting Recorder sessions: $PYTHON_BIN"
    return 2
  fi

  local output
  local active_status
  set +e
  output="$(
    TEAM_PORTAL_DATA_DIR_VALUE="$data_dir" \
    MEETING_RECORDER_ACTIVE_RECORDING_STALE_SECONDS="${MEETING_RECORDER_ACTIVE_RECORDING_STALE_SECONDS:-43200}" \
    "$PYTHON_BIN" - <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

data_dir = Path(os.environ["TEAM_PORTAL_DATA_DIR_VALUE"])
records_dir = data_dir / "meeting_records" / "records"
try:
    stale_seconds = int(os.environ.get("MEETING_RECORDER_ACTIVE_RECORDING_STALE_SECONDS") or "43200")
except ValueError:
    stale_seconds = 43200

def parse_timestamp(value):
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed

def process_alive(pid):
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
    except OSError:
        return False
    return True

def resolve_record_path(record_dir, value):
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return data_dir / value

active = []
now = datetime.now(timezone.utc)
if records_dir.exists():
    for metadata_path in sorted(records_dir.glob("*/metadata.json")):
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if str(metadata.get("status") or "").strip().lower() != "recording":
            continue
        if str(metadata.get("recording_stopped_at") or "").strip():
            continue

        media = metadata.get("media") if isinstance(metadata.get("media"), dict) else {}
        reasons = []
        status_path = resolve_record_path(metadata_path.parent, media.get("screencapture_status_path"))
        if status_path and status_path.exists():
            try:
                capture_status = json.loads(status_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                capture_status = {}
            capture_state = str(capture_status.get("status") or "").strip().lower()
            if capture_state == "recording":
                reasons.append("ScreenCaptureKit status is recording")
            elif capture_state == "stopped":
                continue

        if process_alive(media.get("recorder_pid")):
            reasons.append(f"recorder pid {media.get('recorder_pid')} is alive")

        updated_at = parse_timestamp(metadata.get("updated_at") or metadata.get("recording_started_at") or metadata.get("created_at"))
        if not reasons and updated_at is not None and stale_seconds > 0:
            age = (now - updated_at).total_seconds()
            if 0 <= age <= stale_seconds:
                reasons.append(f"metadata status is recording and was updated {int(age)}s ago")

        if not reasons:
            continue

        record_id = str(metadata.get("record_id") or metadata_path.parent.name)
        title = str(metadata.get("title") or "Untitled meeting")
        started_at = str(metadata.get("recording_started_at") or metadata.get("created_at") or "")
        active.append(f"- {record_id}: {title} (started {started_at}; {', '.join(reasons)})")

if active:
    print("\n".join(active))
    sys.exit(10)
sys.exit(0)
PY
  )"
  active_status=$?
  set -e

  if [[ "$active_status" == "10" ]]; then
    printf '%s\n' "$output"
    return 0
  fi
  if [[ "$active_status" != "0" ]]; then
    if [[ -n "$output" ]]; then
      printf '%s\n' "$output"
    else
      echo "Failed to inspect active Meeting Recorder sessions under $data_dir."
    fi
    return 2
  fi
  return 1
}

assert_no_active_meeting_recording_before_restart() {
  local action="${1:-restart service}"
  local data_dir="${2:-${LOCAL_AGENT_TEAM_PORTAL_DATA_DIR:-${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}}}"
  data_dir="$(resolve_team_data_dir "$data_dir")"
  if [[ "${MEETING_RECORDER_ALLOW_RESTART_DURING_RECORDING:-0}" == "1" || "${MEETING_RECORDER_ALLOW_LOCAL_AGENT_RESTART_DURING_RECORDING:-0}" == "1" ]]; then
    echo "Bypassing active Meeting Recorder guard because restart during recording was explicitly allowed."
    return 0
  fi

  local active_output
  local active_status
  set +e
  active_output="$(meeting_recorder_active_recordings "$data_dir")"
  active_status=$?
  set -e

  if [[ "$active_status" == "0" ]]; then
    echo "Refusing to $action because Meeting Recorder is actively recording."
    echo "Stop the recording from the portal before restarting portal, team stack, launchd, or Mac local-agent services."
    printf '%s\n' "$active_output"
    return 1
  fi
  if [[ "$active_status" == "2" ]]; then
    echo "Refusing to $action because active Meeting Recorder sessions could not be verified."
    printf '%s\n' "$active_output"
    return 1
  fi
  return 0
}

assert_no_active_meeting_recording_before_local_agent_restart() {
  assert_no_active_meeting_recording_before_restart "$@"
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

team_deploy_timing_file() {
  if [[ -n "${TEAM_DEPLOY_TIMING_FILE:-}" ]]; then
    printf '%s\n' "$TEAM_DEPLOY_TIMING_FILE"
    return 0
  fi
  local data_dir="${TEAM_DEPLOY_TIMING_DATA_DIR:-${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}}"
  data_dir="$(resolve_team_data_dir "$data_dir")"
  printf '%s\n' "$data_dir/run/deploy_timings.jsonl"
}

record_deploy_timing() {
  local script_name="$1"
  local phase="$2"
  local started_at="$3"
  local finished_at="$4"
  local status="$5"
  local details="${6:-}"
  local timing_file
  timing_file="$(team_deploy_timing_file)" || return 0
  mkdir -p "$(dirname "$timing_file")"
  if [[ ! -x "$PYTHON_BIN" ]]; then
    return 0
  fi
  DEPLOY_TIMING_FILE="$timing_file" \
  DEPLOY_TIMING_SCRIPT="$script_name" \
  DEPLOY_TIMING_PHASE="$phase" \
  DEPLOY_TIMING_STARTED="$started_at" \
  DEPLOY_TIMING_FINISHED="$finished_at" \
  DEPLOY_TIMING_STATUS="$status" \
  DEPLOY_TIMING_DETAILS="$details" \
  ROOT_DIR_VALUE="$ROOT_DIR" \
  "$PYTHON_BIN" - <<'PY'
import json
import os
import time

path = os.environ["DEPLOY_TIMING_FILE"]
started = int(os.environ.get("DEPLOY_TIMING_STARTED") or 0)
finished = int(os.environ.get("DEPLOY_TIMING_FINISHED") or time.time())
record = {
    "script": os.environ.get("DEPLOY_TIMING_SCRIPT", ""),
    "phase": os.environ.get("DEPLOY_TIMING_PHASE", ""),
    "status": int(os.environ.get("DEPLOY_TIMING_STATUS") or 0),
    "started_at_unix": started,
    "finished_at_unix": finished,
    "duration_seconds": max(0, finished - started),
    "details": os.environ.get("DEPLOY_TIMING_DETAILS", ""),
    "repo_root": os.environ.get("ROOT_DIR_VALUE", ""),
}
with open(path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(record, sort_keys=True) + "\n")
PY
}

is_path_within() {
  local path_value="$1"
  local parent_value="$2"
  [[ -n "$path_value" && -n "$parent_value" ]] || return 1
  case "$path_value" in
    "$parent_value"|"$parent_value"/*) return 0 ;;
    *) return 1 ;;
  esac
}

is_protected_mac_path() {
  local path_value="${1:-}"
  local home_dir="$HOME"
  [[ -n "$path_value" ]] || return 1
  case "$path_value" in
    "$home_dir/Documents"|"$home_dir/Documents"/*) return 0 ;;
    "$home_dir/Desktop"|"$home_dir/Desktop"/*) return 0 ;;
    "$home_dir/Downloads"|"$home_dir/Downloads"/*) return 0 ;;
    "$home_dir/Library/Mobile Documents"|"$home_dir/Library/Mobile Documents"/*) return 0 ;;
    *) return 1 ;;
  esac
}

recommended_team_stack_root() {
  printf '%s\n' "$HOME/Workspace/jira-creation-stack-host"
}

recommended_uat_team_stack_root() {
  printf '%s\n' "$HOME/Workspace/jira-creation-stack-uat-host"
}
