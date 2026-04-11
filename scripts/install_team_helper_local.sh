#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
PIP_BIN="$ROOT_DIR/.venv/bin/pip"
HELPER_SCRIPT="$ROOT_DIR/scripts/run_team_helper.sh"
STACK_SCRIPT="$ROOT_DIR/scripts/run_team_stack.sh"
REQUIREMENTS_FILE="$ROOT_DIR/requirements.txt"
ENV_FILE="$ROOT_DIR/.env"
ENV_EXAMPLE_FILE="$ROOT_DIR/.env.example"

read_env_value() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    return 0
  fi
  grep -E "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2-
}

ensure_python() {
  if command -v python3 >/dev/null 2>&1; then
    return 0
  fi
  echo "python3 was not found. Please install Python 3 first."
  exit 1
}

ensure_virtualenv() {
  if [[ -x "$PYTHON_BIN" ]]; then
    return 0
  fi

  echo "Creating local Python environment..."
  (cd "$ROOT_DIR" && python3 -m venv .venv)
}

install_requirements() {
  echo "Installing Python packages..."
  "$PIP_BIN" install --upgrade pip >/dev/null
  "$PIP_BIN" install -r "$REQUIREMENTS_FILE"
}

ensure_env_file() {
  if [[ -f "$ENV_FILE" ]]; then
    return 0
  fi

  if [[ -f "$ENV_EXAMPLE_FILE" ]]; then
    cp "$ENV_EXAMPLE_FILE" "$ENV_FILE"
    echo "Created .env from .env.example"
    echo "Please update GOOGLE_OAUTH_CLIENT_SECRET_FILE in $ENV_FILE before connecting Google."
    echo
  fi
}

check_google_client_secret() {
  local client_secret
  client_secret="$(read_env_value "GOOGLE_OAUTH_CLIENT_SECRET_FILE")"
  if [[ -z "${client_secret:-}" ]] || [[ "$client_secret" == "/absolute/path/to/google-client-secret.json" ]]; then
    echo "Google OAuth client secret is not configured yet."
    echo "Please edit $ENV_FILE and set GOOGLE_OAUTH_CLIENT_SECRET_FILE to your local JSON file path."
    echo
    return 1
  fi
  if [[ ! -f "$client_secret" ]]; then
    echo "Google OAuth client secret file was not found:"
    echo "  $client_secret"
    echo "Please update GOOGLE_OAUTH_CLIENT_SECRET_FILE in $ENV_FILE to a valid JSON file path."
    echo
    return 1
  fi
  return 0
}

ensure_python

if ! command -v /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome >/dev/null 2>&1 && ! command -v open >/dev/null 2>&1; then
  echo "Google Chrome was not found. Please install Chrome and sign in to BPMIS first."
  exit 1
fi

ensure_env_file
ensure_virtualenv
install_requirements
check_google_client_secret || true

echo
echo "Local setup is ready."
echo
echo "Daily commands:"
echo "  $STACK_SCRIPT start"
echo "  $STACK_SCRIPT status"
echo "  $STACK_SCRIPT logs"
echo "  $STACK_SCRIPT stop"
echo
echo "Recommended first-time flow:"
echo "  1. Log in to BPMIS in Chrome"
echo "  2. Make sure GOOGLE_OAUTH_CLIENT_SECRET_FILE is correct in $ENV_FILE"
echo "  3. Run: $STACK_SCRIPT start"
echo "  4. Open: http://127.0.0.1:5000"
