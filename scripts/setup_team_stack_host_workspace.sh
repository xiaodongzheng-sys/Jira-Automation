#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

TARGET_ROOT="${TEAM_STACK_HOST_ROOT:-$(recommended_team_stack_root)}"
TARGET_ROOT="${TARGET_ROOT/#\~/$HOME}"
target_parent="$(dirname "$TARGET_ROOT")"
mkdir -p "$target_parent"
TARGET_ROOT="$(cd "$target_parent" && pwd)/$(basename "$TARGET_ROOT")"

if is_protected_mac_path "$TARGET_ROOT"; then
  echo "Target host workspace must not be under a protected macOS folder:"
  echo "  $TARGET_ROOT"
  exit 1
fi

clone_source="${TEAM_STACK_HOST_CLONE_SOURCE:-$ROOT_DIR}"

if [[ ! -d "$TARGET_ROOT/.git" ]]; then
  echo "Creating host workspace at:"
  echo "  $TARGET_ROOT"
  git clone "$clone_source" "$TARGET_ROOT"
else
  echo "Host workspace already exists:"
  echo "  $TARGET_ROOT"
fi

if [[ -f "$ROOT_DIR/.env" && ! -f "$TARGET_ROOT/.env" ]]; then
  cp "$ROOT_DIR/.env" "$TARGET_ROOT/.env"
  echo "Copied .env into host workspace."
fi

if [[ -f "$TARGET_ROOT/.env" ]]; then
  target_env="$TARGET_ROOT/.env"
  google_secret_path="$(
    ENV_FILE="$target_env" ROOT_DIR="$TARGET_ROOT" PYTHON_BIN="$PYTHON_BIN" bash -lc '
      source "'"$ROOT_DIR"'/scripts/lib/team_env.sh"
      read_env_value GOOGLE_OAUTH_CLIENT_SECRET_FILE
    ' 2>/dev/null || true
  )"
  if [[ -n "${google_secret_path:-}" ]] && is_protected_mac_path "$google_secret_path"; then
    echo
    echo "Warning: GOOGLE_OAUTH_CLIENT_SECRET_FILE is still under a protected folder:"
    echo "  $google_secret_path"
    echo "launchd may also be blocked from reading that file."
  fi
fi

echo
echo "Installing launchd from host workspace..."
(cd "$TARGET_ROOT" && ./scripts/install_team_stack_launchd.sh)

echo
echo "Starting launchd job..."
launchctl kickstart -k "gui/$(id -u)/${TEAM_STACK_LAUNCHD_LABEL:-io.npt.jira-creation-stack}" >/dev/null 2>&1 || \
launchctl start "${TEAM_STACK_LAUNCHD_LABEL:-io.npt.jira-creation-stack}" >/dev/null 2>&1 || true

echo
echo "Host workspace ready:"
echo "  $TARGET_ROOT"
echo
echo "Next checks:"
echo "  cd \"$TARGET_ROOT\" && ./scripts/run_team_stack.sh doctor"
