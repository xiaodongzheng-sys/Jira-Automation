#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

LABEL="${WECHAT_REPLY_LAUNCHD_LABEL:-io.npt.seatalk-reply-to-wechat}"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_TARGET="$LAUNCH_AGENTS_DIR/$LABEL.plist"
TEMPLATE_PATH="$ROOT_DIR/deploy/launchd/seatalk.reply.to.wechat.plist.template"

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "Missing launchd template: $TEMPLATE_PATH"
  exit 1
fi

mkdir -p "$LAUNCH_AGENTS_DIR"

DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
mkdir -p "$DATA_DIR/logs" "$DATA_DIR/wechat_notification_to_seatalk"

SERVER_SCRIPT="$ROOT_DIR/scripts/run_seatalk_reply_to_wechat_foreground.sh"
STDOUT_LOG="$DATA_DIR/logs/seatalk-reply-to-wechat.out.log"
STDERR_LOG="$DATA_DIR/logs/seatalk-reply-to-wechat.err.log"

sed \
  -e "s|__LABEL__|$LABEL|g" \
  -e "s|__ROOT_DIR__|$ROOT_DIR|g" \
  -e "s|__ENV_FILE__|$ENV_FILE|g" \
  -e "s|__SERVER_SCRIPT__|$SERVER_SCRIPT|g" \
  -e "s|__STDOUT_LOG__|$STDOUT_LOG|g" \
  -e "s|__STDERR_LOG__|$STDERR_LOG|g" \
  "$TEMPLATE_PATH" >"$PLIST_TARGET"

launchctl unload "$PLIST_TARGET" >/dev/null 2>&1 || true
launchctl load "$PLIST_TARGET"

echo "Installed launchd job: $LABEL"
echo "Plist: $PLIST_TARGET"
echo "Server script: $SERVER_SCRIPT"
echo "Logs:"
echo "  $STDOUT_LOG"
echo "  $STDERR_LOG"
echo
echo "Next step:"
echo "  launchctl start $LABEL"
