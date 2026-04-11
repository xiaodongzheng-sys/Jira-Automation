#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
LABEL="${TEAM_PORTAL_LAUNCHD_LABEL:-io.npt.jira-creation-portal}"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_TARGET="$LAUNCH_AGENTS_DIR/$LABEL.plist"
TEMPLATE_PATH="$ROOT_DIR/deploy/launchd/team.portal.plist.template"

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "Missing launchd template: $TEMPLATE_PATH"
  exit 1
fi

mkdir -p "$LAUNCH_AGENTS_DIR"

DATA_DIR="${TEAM_PORTAL_DATA_DIR:-$ROOT_DIR/.team-portal}"
if [[ "$DATA_DIR" != /* ]]; then
  DATA_DIR="$ROOT_DIR/$DATA_DIR"
fi
mkdir -p "$DATA_DIR/logs"

PORTAL_SCRIPT="$ROOT_DIR/scripts/run_team_portal_prod.sh"
STDOUT_LOG="$DATA_DIR/logs/launchd.out.log"
STDERR_LOG="$DATA_DIR/logs/launchd.err.log"

sed \
  -e "s|__LABEL__|$LABEL|g" \
  -e "s|__ROOT_DIR__|$ROOT_DIR|g" \
  -e "s|__ENV_FILE__|$ENV_FILE|g" \
  -e "s|__PORTAL_SCRIPT__|$PORTAL_SCRIPT|g" \
  -e "s|__STDOUT_LOG__|$STDOUT_LOG|g" \
  -e "s|__STDERR_LOG__|$STDERR_LOG|g" \
  "$TEMPLATE_PATH" >"$PLIST_TARGET"

launchctl unload "$PLIST_TARGET" >/dev/null 2>&1 || true
launchctl load "$PLIST_TARGET"

echo "Installed launchd job: $LABEL"
echo "Plist: $PLIST_TARGET"
echo "Portal script: $PORTAL_SCRIPT"
echo "Logs:"
echo "  $STDOUT_LOG"
echo "  $STDERR_LOG"
echo
echo "Next step:"
echo "  launchctl start $LABEL"
