#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/team_env.sh"
export_env_file

DATA_DIR="$(resolve_team_data_dir "${TEAM_PORTAL_DATA_DIR:-$(read_env_value TEAM_PORTAL_DATA_DIR)}")"
mkdir -p "$DATA_DIR/wechat_notification_to_seatalk" "$DATA_DIR/logs"

export WECHAT_ALERT_STATE_PATH="${WECHAT_ALERT_STATE_PATH:-$DATA_DIR/wechat_notification_to_seatalk/state.json}"

exec "$PYTHON_BIN" "$ROOT_DIR/scripts/wechat_notification_to_seatalk.py" "$@"
