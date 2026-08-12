#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# One production Job refreshes each market sequentially at 14:00 SGT.
export BUSINESS_INSIGHTS_REFRESH_JOB="business-insights-all-sheet-refresh"
export BUSINESS_INSIGHTS_REFRESH_SCHEDULER="business-insights-all-sheet-refresh-daily"
export BUSINESS_INSIGHTS_REFRESH_SCHEDULE="0 14 * * *"
export BUSINESS_INSIGHTS_REFRESH_TIME_ZONE="Asia/Singapore"
export BUSINESS_INSIGHTS_REFRESH_ENTRYPOINT="scripts/run_business_insights_all_sheet_refresh.py"
export BUSINESS_INSIGHTS_REFRESH_REPORT_IDS=""
export BUSINESS_INSIGHTS_GOOGLE_SHEET_URL=""

exec "$ROOT_DIR/scripts/deploy_business_insights_sheet_refresh_cloud_run_job.sh" "$@"
