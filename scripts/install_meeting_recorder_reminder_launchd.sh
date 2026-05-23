#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"

LABEL="${MEETING_RECORDER_REMINDER_LAUNCHD_LABEL:-io.npt.meeting-recorder-reminder}"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_TARGET="$LAUNCH_AGENTS_DIR/$LABEL.plist"
TEMPLATE_PATH="$ROOT_DIR/deploy/launchd/meeting-recorder-reminder.plist.template"

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "Missing launchd template: $TEMPLATE_PATH"
  exit 1
fi

if is_protected_mac_path "$ROOT_DIR" && [[ "${TEAM_STACK_ALLOW_PROTECTED_ROOT:-0}" != "1" ]]; then
  echo "Meeting Recorder reminder launchd install blocked: repo is under a macOS protected folder:"
  echo "  $ROOT_DIR"
  echo
  echo "Recommended fix:"
  echo "  ./scripts/setup_team_stack_host_workspace.sh"
  echo
  echo "If you really want to force install from here:"
  echo "  TEAM_STACK_ALLOW_PROTECTED_ROOT=1 ./scripts/install_meeting_recorder_reminder_launchd.sh"
  exit 1
fi

mkdir -p "$LAUNCH_AGENTS_DIR"

DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
mkdir -p "$DATA_DIR/logs"

REMINDER_SCRIPT="$ROOT_DIR/scripts/run_meeting_recorder_reminder.sh"
STDOUT_LOG="$DATA_DIR/logs/meeting_recorder_reminder.launchd.out.log"
STDERR_LOG="$DATA_DIR/logs/meeting_recorder_reminder.launchd.err.log"

sed \
  -e "s|__LABEL__|$LABEL|g" \
  -e "s|__ROOT_DIR__|$ROOT_DIR|g" \
  -e "s|__ENV_FILE__|$ENV_FILE|g" \
  -e "s|__REMINDER_SCRIPT__|$REMINDER_SCRIPT|g" \
  -e "s|__STDOUT_LOG__|$STDOUT_LOG|g" \
  -e "s|__STDERR_LOG__|$STDERR_LOG|g" \
  "$TEMPLATE_PATH" >"$PLIST_TARGET"

assert_no_active_meeting_recording_before_restart "reload Meeting Recorder reminder launchd job" "$DATA_DIR"
launchctl unload "$PLIST_TARGET" >/dev/null 2>&1 || true
launchctl load "$PLIST_TARGET"

echo "Installed Meeting Recorder reminder launchd job: $LABEL"
echo "Plist: $PLIST_TARGET"
echo "Runner script: $REMINDER_SCRIPT"
echo "Logs:"
echo "  $STDOUT_LOG"
echo "  $STDERR_LOG"
echo
echo "Next step:"
echo "  launchctl start $LABEL"
