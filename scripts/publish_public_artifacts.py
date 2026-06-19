"""Publish the public portal artifacts from this host to the public GCS bucket.

Run on the Mac (the data producer). Uploads:
- Business Insights reports.json + artifact workbooks/visualizations
- Repo Download source bundles for every scope (built from the synced repos,
  reusing the on-disk cache when fresh)

Usage:
  ./scripts/project_python.sh scripts/publish_public_artifacts.py [--bucket NAME]
  (defaults to TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET or CLOUD_RUN_PUBLIC_GCS_BUCKET from the environment/.env)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from bpmis_jira_tool.config import Settings  # noqa: E402
from bpmis_jira_tool.public_artifacts_gcs import (  # noqa: E402
    CLOUD_RUN_PUBLIC_GCS_BUCKET_ENV,
    PUBLIC_GCS_PUBLISH_BUCKET_ENV,
    public_gcs_publish_bucket,
    publish_business_insights_dir,
    publish_repo_download_archive,
)
from bpmis_jira_tool.source_code_qa_factory import build_source_code_qa_service_from_settings  # noqa: E402
from bpmis_jira_tool.source_code_qa_repo_downloads import (  # noqa: E402
    REPO_DOWNLOAD_SCOPES,
    build_repo_download_zip,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default="", help="Override the publish bucket name.")
    parser.add_argument("--skip-repo-bundles", action="store_true", help="Only publish Business Insights files.")
    args = parser.parse_args()

    if args.bucket:
        os.environ[PUBLIC_GCS_PUBLISH_BUCKET_ENV] = args.bucket

    settings = Settings.from_env()
    bucket = public_gcs_publish_bucket()
    if not bucket:
        print(
            f"Set {PUBLIC_GCS_PUBLISH_BUCKET_ENV} or {CLOUD_RUN_PUBLIC_GCS_BUCKET_ENV} (or pass --bucket).",
            file=sys.stderr,
        )
        return 2

    data_root = Path(settings.team_portal_data_dir).expanduser()

    uploaded = publish_business_insights_dir(data_root / "business_insights")
    print(f"business_insights_files_uploaded={uploaded}")

    if not args.skip_repo_bundles:
        service = build_source_code_qa_service_from_settings(settings)
        for scope in REPO_DOWNLOAD_SCOPES:
            try:
                metadata, content = build_repo_download_zip(service, scope["scope_key"])
            except Exception as error:  # noqa: BLE001 - report and continue with other scopes
                print(f"repo_bundle_skipped scope={scope['scope_key']} reason={error}")
                continue
            ok = publish_repo_download_archive(metadata, content)
            print(f"repo_bundle_published scope={scope['scope_key']} ok={ok} bytes={len(content)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
