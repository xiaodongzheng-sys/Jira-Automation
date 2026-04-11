#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
HELPER_SCRIPT="$ROOT_DIR/scripts/run_team_helper.sh"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing Python virtual environment at $PYTHON_BIN"
  echo "Please create it first:"
  echo "  python3 -m venv .venv"
  echo "  ./.venv/bin/pip install -r requirements.txt"
  exit 1
fi

if ! command -v /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome >/dev/null 2>&1 && ! command -v open >/dev/null 2>&1; then
  echo "Google Chrome was not found. Please install Chrome and sign in to BPMIS first."
  exit 1
fi

echo "Local helper prerequisites look good."
echo
echo "Daily commands:"
echo "  $HELPER_SCRIPT start"
echo "  $HELPER_SCRIPT status"
echo "  $HELPER_SCRIPT logs"
echo "  $HELPER_SCRIPT stop"
echo
echo "Recommended first-time flow:"
echo "  1. Log in to BPMIS in Chrome"
echo "  2. Run: $HELPER_SCRIPT start"
echo "  3. Confirm http://127.0.0.1:8787/health returns ok"
echo "  4. Open the team portal"
