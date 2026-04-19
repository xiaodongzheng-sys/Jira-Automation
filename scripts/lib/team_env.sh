#!/usr/bin/env bash

if [[ -n "${TEAM_ENV_HELPERS_LOADED:-}" ]]; then
  return 0
fi
TEAM_ENV_HELPERS_LOADED=1

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

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

resolve_team_data_dir() {
  local data_dir="${1:-}"
  data_dir="${data_dir:-$ROOT_DIR/.team-portal}"
  if [[ "$data_dir" != /* ]]; then
    data_dir="$ROOT_DIR/$data_dir"
  fi
  printf '%s\n' "$data_dir"
}
