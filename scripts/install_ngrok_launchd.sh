#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
LABEL="${TEAM_NGROK_LAUNCHD_LABEL:-io.npt.jira-creation-ngrok}"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_TARGET="$LAUNCH_AGENTS_DIR/$LABEL.plist"
TEMPLATE_PATH="$ROOT_DIR/deploy/launchd/team.portal.plist.template"

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "Missing launchd template: $TEMPLATE_PATH"
  exit 1
fi

mkdir -p "$LAUNCH_AGENTS_DIR"

DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
mkdir -p "$DATA_DIR/logs"

RUNNER_SCRIPT="$ROOT_DIR/scripts/run_ngrok_tunnel_foreground.sh"
STDOUT_LOG="$DATA_DIR/logs/ngrok.launchd.out.log"
STDERR_LOG="$DATA_DIR/logs/ngrok.launchd.err.log"
TEAM_STACK_USE_CAFFEINATE_VALUE="${TEAM_STACK_USE_CAFFEINATE:-0}"

sed \
  -e "s|__LABEL__|$LABEL|g" \
  -e "s|__ROOT_DIR__|$ROOT_DIR|g" \
  -e "s|__ENV_FILE__|$ENV_FILE|g" \
  -e "s|__TEAM_STACK_USE_CAFFEINATE__|$TEAM_STACK_USE_CAFFEINATE_VALUE|g" \
  -e "s|__PORTAL_SCRIPT__|$RUNNER_SCRIPT|g" \
  -e "s|__STDOUT_LOG__|$STDOUT_LOG|g" \
  -e "s|__STDERR_LOG__|$STDERR_LOG|g" \
  "$TEMPLATE_PATH" >"$PLIST_TARGET"

launchctl unload "$PLIST_TARGET" >/dev/null 2>&1 || true
launchctl load "$PLIST_TARGET"

echo "Installed ngrok launchd job: $LABEL"
echo "Plist: $PLIST_TARGET"
echo "Runner script: $RUNNER_SCRIPT"
echo "Logs:"
echo "  $STDOUT_LOG"
echo "  $STDERR_LOG"
echo
echo "Next step:"
echo "  launchctl start $LABEL"
