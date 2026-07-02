#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

LABEL="${BUSINESS_INSIGHTS_SHEET_REFRESH_LAUNCHD_LABEL:-io.npt.business-insights-sheet-refresh}"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_TARGET="$LAUNCH_AGENTS_DIR/$LABEL.plist"
TEMPLATE_PATH="$ROOT_DIR/deploy/launchd/business-insights-sheet-refresh.plist.template"
RUNNER_SCRIPT="$ROOT_DIR/scripts/run_business_insights_sheet_refresh.sh"

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "Missing launchd template: $TEMPLATE_PATH"
  exit 1
fi

OWNER_EMAIL="${BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL:-$(read_env_value BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL)}"
if [[ -z "$OWNER_EMAIL" ]]; then
  echo "BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL is required in $ENV_FILE or the current environment."
  exit 1
fi

DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
mkdir -p "$LAUNCH_AGENTS_DIR" "$DATA_DIR/logs"

STDOUT_LOG="$DATA_DIR/logs/business-insights-sheet-refresh.out.log"
STDERR_LOG="$DATA_DIR/logs/business-insights-sheet-refresh.err.log"

sed \
  -e "s|__LABEL__|$LABEL|g" \
  -e "s|__ROOT_DIR__|$ROOT_DIR|g" \
  -e "s|__ENV_FILE__|$ENV_FILE|g" \
  -e "s|__RUNNER_SCRIPT__|$RUNNER_SCRIPT|g" \
  -e "s|__STDOUT_LOG__|$STDOUT_LOG|g" \
  -e "s|__STDERR_LOG__|$STDERR_LOG|g" \
  "$TEMPLATE_PATH" >"$PLIST_TARGET"

launchctl unload "$PLIST_TARGET" >/dev/null 2>&1 || true
launchctl load "$PLIST_TARGET"

echo "Installed Business Insights Sheet refresh launchd job: $LABEL"
echo "Runs daily at 10:00 local time."
echo "Plist: $PLIST_TARGET"
echo "Logs:"
echo "  $STDOUT_LOG"
echo "  $STDERR_LOG"
