#!/usr/bin/env python3
"""Refresh Business Insights Anti-fraud artifacts from scheduled Google Sheet output."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from google.auth.exceptions import RefreshError

from bpmis_jira_tool.business_insights_sheet_refresh import (
    ANTI_FRAUD_SHEET_REPORT_IDS,
    DEFAULT_BUSINESS_INSIGHTS_SHEET_URL,
    build_sheets_service,
    load_application_default_google_sheets_credentials,
    load_oauth_google_sheets_credentials,
    load_service_account_google_sheets_credentials,
    load_stored_google_sheets_credentials,
    refresh_anti_fraud_reports_from_google_sheet,
)
from bpmis_jira_tool.public_artifacts_gcs import (
    gcs_read_bytes,
    public_gcs_read_bucket,
)
from scripts.generate_business_insights_live_reports import DEFAULT_PORTAL_DATA_DIR, _publish_to_public_gcs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sheet-url",
        default=os.getenv("BUSINESS_INSIGHTS_GOOGLE_SHEET_URL") or DEFAULT_BUSINESS_INSIGHTS_SHEET_URL,
        help="Google Sheet URL containing scheduled SparkSQL output tabs.",
    )
    parser.add_argument(
        "--owner-email",
        default=os.getenv("BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL") or os.getenv("GOOGLE_OWNER_EMAIL") or "",
        help="Google account whose saved OAuth credentials should read the Sheet.",
    )
    parser.add_argument(
        "--portal-data-dir",
        default=os.getenv("TEAM_PORTAL_DATA_DIR") or str(DEFAULT_PORTAL_DATA_DIR),
        help="Portal data dir containing business_insights/reports.json.",
    )
    parser.add_argument(
        "--report-id",
        action="append",
        default=[],
        help="Anti-fraud report id to refresh. Repeatable. Defaults to all sheet-backed Anti-fraud reports.",
    )
    parser.add_argument("--no-publish", action="store_true", help="Do not publish refreshed artifacts to public GCS.")
    return parser.parse_args()


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _hydrate_shared_metadata_or_fail(portal_data_dir: Path) -> None:
    """Never publish a partial refresh from a stale local reports.json copy."""
    bucket_name = public_gcs_read_bucket()
    if not bucket_name:
        return

    metadata_bytes = gcs_read_bytes(bucket_name, "business_insights/reports.json")
    if not metadata_bytes:
        raise SystemExit(
            f"Could not read the shared Business Insights metadata from gs://{bucket_name}/business_insights/reports.json; "
            "refresh aborted before publishing to protect the other market reports."
        )
    try:
        metadata = json.loads(metadata_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(
            f"Shared Business Insights metadata is invalid in gs://{bucket_name}/business_insights/reports.json; "
            "refresh aborted before publishing."
        ) from error
    if not isinstance(metadata, dict) or not isinstance(metadata.get("artifacts"), dict):
        raise SystemExit(
            f"Shared Business Insights metadata is missing its artifacts map in gs://{bucket_name}/business_insights/reports.json; "
            "refresh aborted before publishing."
        )

    metadata_path = portal_data_dir / "business_insights" / "reports.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = metadata_path.with_name(f".{metadata_path.name}.{os.getpid()}.gcs.tmp")
    temp_path.write_bytes(metadata_bytes)
    os.replace(temp_path, metadata_path)


def load_google_sheets_credentials(args: argparse.Namespace):
    oauth_credentials_json = os.getenv("BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON", "").strip()
    if oauth_credentials_json:
        return load_oauth_google_sheets_credentials(oauth_credentials_json)

    service_account_json = os.getenv("BUSINESS_INSIGHTS_GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    service_account_file = os.getenv("BUSINESS_INSIGHTS_GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    if service_account_json or service_account_file:
        return load_service_account_google_sheets_credentials(
            service_account_json=service_account_json,
            service_account_file=service_account_file,
        )
    if _truthy(os.getenv("BUSINESS_INSIGHTS_GOOGLE_USE_APPLICATION_DEFAULT", "")):
        return load_application_default_google_sheets_credentials()

    owner_email = str(args.owner_email or "").strip().lower()
    if not owner_email:
        raise SystemExit(
            "BUSINESS_INSIGHTS_GOOGLE_OWNER_EMAIL is required unless "
            "BUSINESS_INSIGHTS_GOOGLE_USE_APPLICATION_DEFAULT=1 or a service account credential is configured."
        )
    encryption_key = os.getenv("TEAM_PORTAL_CONFIG_ENCRYPTION_KEY", "").strip()
    return load_stored_google_sheets_credentials(
        portal_data_dir=Path(args.portal_data_dir).expanduser().resolve(),
        owner_email=owner_email,
        encryption_key=encryption_key,
    )


def main() -> int:
    args = parse_args()
    portal_data_dir = Path(args.portal_data_dir).expanduser().resolve()
    # Cloud Run Jobs use an ephemeral directory. Hydrate the shared index before
    # a partial refresh, and fail closed if it cannot be read so a stale local
    # reports.json can never overwrite another market's latest artifacts.
    _hydrate_shared_metadata_or_fail(portal_data_dir)
    credentials = load_google_sheets_credentials(args)
    service = build_sheets_service(credentials)
    configured_report_ids = [
        value.strip()
        for value in os.getenv("BUSINESS_INSIGHTS_REFRESH_REPORT_IDS", "").split(",")
        if value.strip()
    ]
    report_ids = args.report_id or configured_report_ids or list(ANTI_FRAUD_SHEET_REPORT_IDS)
    try:
        result = refresh_anti_fraud_reports_from_google_sheet(
            portal_data_dir=portal_data_dir,
            sheets_service=service,
            sheet_url=args.sheet_url,
            report_ids=report_ids,
        )
    except RefreshError as error:
        raise SystemExit(
            "Google credentials could not be refreshed. Reconnect Google in the Portal once, "
            f"then rerun this refresh. Details: {error}"
        ) from error
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    if not args.no_publish:
        _publish_to_public_gcs(portal_data_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
