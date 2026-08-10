#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Keep the ID production configuration in one reviewed entrypoint. This avoids
# accidentally pointing the Job at the PH/SG Sheet or changing the schedule.
export BUSINESS_INSIGHTS_REFRESH_JOB="business-insights-id-sheet-refresh"
export BUSINESS_INSIGHTS_REFRESH_SCHEDULER="business-insights-id-sheet-refresh-daily"
export BUSINESS_INSIGHTS_REFRESH_SCHEDULE="0 14 * * *"
export BUSINESS_INSIGHTS_REFRESH_TIME_ZONE="Asia/Singapore"
export BUSINESS_INSIGHTS_GOOGLE_SHEET_URL="https://docs.google.com/spreadsheets/d/1423Y-u5kl24TD62yScFqatEl3tzB9RXGY6vI4cxeMOg/edit?gid=0#gid=0"
export BUSINESS_INSIGHTS_REFRESH_REPORT_IDS="anti-fraud-id-scenarios-auth-rules-features"

exec "$ROOT_DIR/scripts/deploy_business_insights_sheet_refresh_cloud_run_job.sh" "$@"
