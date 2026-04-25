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
