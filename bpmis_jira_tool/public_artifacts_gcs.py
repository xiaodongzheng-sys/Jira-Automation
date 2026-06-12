"""Google Cloud Storage layer for the public portal surfaces.

The Mac host remains the producer: Business Insights refreshes and repo
source-bundle builds publish their outputs to a GCS bucket (best-effort,
gated by ``TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET``). Cloud Run serves those
public surfaces directly from the bucket (gated by
``TEAM_PORTAL_PUBLIC_GCS_BUCKET``) so they keep working while the Mac is off.

Remote layout inside the bucket:
- ``business_insights/reports.json``
- ``business_insights/artifacts/<filename>``
- ``repo-downloads/<scope filename>.zip`` plus ``<scope filename>.json`` metadata
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PUBLIC_GCS_READ_BUCKET_ENV = "TEAM_PORTAL_PUBLIC_GCS_BUCKET"
PUBLIC_GCS_PUBLISH_BUCKET_ENV = "TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET"
# gcloud account used for CLI-fallback publishing on hosts without Application
# Default Credentials (the Mac publishes via the gcloud credential store).
PUBLIC_GCS_PUBLISH_ACCOUNT_ENV = "TEAM_PORTAL_PUBLIC_GCS_PUBLISH_ACCOUNT"
# How long a hydrated copy of reports.json is trusted before re-checking GCS.
PUBLIC_GCS_METADATA_TTL_SECONDS = 60.0

_CLIENT_LOCK = threading.Lock()
_CLIENT: Any = None


def public_gcs_read_bucket() -> str:
    return str(os.environ.get(PUBLIC_GCS_READ_BUCKET_ENV) or "").strip()


def public_gcs_publish_bucket() -> str:
    return str(os.environ.get(PUBLIC_GCS_PUBLISH_BUCKET_ENV) or "").strip()


def _storage_client() -> Any:
    global _CLIENT
    with _CLIENT_LOCK:
        if _CLIENT is None:
            from google.cloud import storage  # imported lazily; optional dependency

            _CLIENT = storage.Client()
        return _CLIENT


def _bucket(name: str) -> Any:
    return _storage_client().bucket(name)


def gcs_read_bytes(bucket_name: str, remote_path: str) -> bytes | None:
    """Return blob bytes, or None when the blob does not exist."""
    try:
        blob = _bucket(bucket_name).blob(remote_path)
        if not blob.exists():
            return None
        return blob.download_as_bytes()
    except Exception:
        logger.warning("Could not read gs://%s/%s", bucket_name, remote_path, exc_info=True)
        return None


def gcs_fetch_to_file(bucket_name: str, remote_path: str, local_path: Path, *, max_age_seconds: float | None = None) -> bool:
    """Download ``remote_path`` into ``local_path`` unless a fresh copy exists.

    Returns True when ``local_path`` exists afterwards (downloaded or cached).
    """
    local_path = Path(local_path)
    if local_path.exists() and max_age_seconds is not None:
        age = time.time() - local_path.stat().st_mtime
        if age < max_age_seconds:
            return True
    try:
        blob = _bucket(bucket_name).blob(remote_path)
        if not blob.exists():
            return local_path.exists()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = local_path.with_name(f".{local_path.name}.{os.getpid()}.gcs.tmp")
        blob.download_to_filename(str(temp_path))
        os.replace(temp_path, local_path)
        return True
    except Exception:
        logger.warning("Could not fetch gs://%s/%s", bucket_name, remote_path, exc_info=True)
        return local_path.exists()


def _gcloud_binary() -> str:
    for candidate in (os.path.expanduser("~/google-cloud-sdk/bin/gcloud"), shutil.which("gcloud") or ""):
        if candidate and os.path.exists(candidate):
            return candidate
    return ""


def _gcloud_upload(local_path: Path, bucket_name: str, remote_path: str) -> bool:
    """CLI fallback for hosts without ADC: publish via the gcloud credential store."""
    gcloud = _gcloud_binary()
    if not gcloud:
        return False
    command = [gcloud, "storage", "cp", str(local_path), f"gs://{bucket_name}/{remote_path}"]
    account = str(os.environ.get(PUBLIC_GCS_PUBLISH_ACCOUNT_ENV) or "").strip()
    if account:
        command += ["--account", account]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=600, check=False)
        if completed.returncode == 0:
            return True
        logger.warning("gcloud upload to gs://%s/%s failed: %s", bucket_name, remote_path, completed.stderr.strip()[-400:])
        return False
    except (OSError, subprocess.SubprocessError):
        logger.warning("gcloud upload to gs://%s/%s failed", bucket_name, remote_path, exc_info=True)
        return False


def gcs_upload_file(bucket_name: str, local_path: Path, remote_path: str) -> bool:
    try:
        blob = _bucket(bucket_name).blob(remote_path)
        blob.upload_from_filename(str(local_path))
        return True
    except Exception:
        if _gcloud_upload(Path(local_path), bucket_name, remote_path):
            return True
        logger.warning("Could not upload %s to gs://%s/%s", local_path, bucket_name, remote_path, exc_info=True)
        return False


def gcs_upload_bytes(bucket_name: str, content: bytes, remote_path: str, *, content_type: str = "application/octet-stream") -> bool:
    try:
        blob = _bucket(bucket_name).blob(remote_path)
        blob.upload_from_string(content, content_type=content_type)
        return True
    except Exception:
        try:
            with tempfile.NamedTemporaryFile(delete=False) as handle:
                handle.write(content)
                temp_name = handle.name
            try:
                if _gcloud_upload(Path(temp_name), bucket_name, remote_path):
                    return True
            finally:
                os.unlink(temp_name)
        except OSError:
            pass
        logger.warning("Could not upload bytes to gs://%s/%s", bucket_name, remote_path, exc_info=True)
        return False


def publish_repo_download_archive(metadata: dict[str, Any], content: bytes) -> bool:
    """Best-effort publish of a freshly built repo source bundle (Mac side)."""
    bucket_name = public_gcs_publish_bucket()
    if not bucket_name:
        return False
    filename = str(metadata.get("filename") or "").strip()
    if not filename:
        return False
    ok = gcs_upload_bytes(bucket_name, content, f"repo-downloads/{filename}", content_type="application/zip")
    if ok:
        gcs_upload_bytes(
            bucket_name,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"),
            f"repo-downloads/{filename}.json",
            content_type="application/json",
        )
        logger.info("Published repo download bundle %s to gs://%s", filename, bucket_name)
    return ok


def fetch_repo_download_status(filename: str) -> dict[str, Any] | None:
    """Cheap status for a published bundle (size + generated_at) without
    downloading the zip. None when the read bucket is not configured."""
    bucket_name = public_gcs_read_bucket()
    if not bucket_name:
        return None
    status: dict[str, Any] = {"available": False, "size_bytes": None, "generated_at": None}
    try:
        blob = _bucket(bucket_name).blob(f"repo-downloads/{filename}")
        if blob.exists():
            blob.reload()
            status["available"] = True
            status["size_bytes"] = int(blob.size or 0)
    except Exception:
        logger.warning("Could not stat gs://%s/repo-downloads/%s", bucket_name, filename, exc_info=True)
    metadata_bytes = gcs_read_bytes(bucket_name, f"repo-downloads/{filename}.json")
    if metadata_bytes:
        try:
            parsed = json.loads(metadata_bytes.decode("utf-8"))
            if isinstance(parsed, dict):
                status["generated_at"] = parsed.get("generated_at")
        except (ValueError, UnicodeDecodeError):
            pass
    return status


def fetch_repo_download_archive(filename: str) -> tuple[dict[str, Any], bytes] | None:
    """Serve a repo source bundle from GCS (Cloud Run side). None = not available."""
    bucket_name = public_gcs_read_bucket()
    if not bucket_name:
        return None
    content = gcs_read_bytes(bucket_name, f"repo-downloads/{filename}")
    if content is None:
        return None
    metadata: dict[str, Any] = {"filename": filename}
    metadata_bytes = gcs_read_bytes(bucket_name, f"repo-downloads/{filename}.json")
    if metadata_bytes:
        try:
            parsed = json.loads(metadata_bytes.decode("utf-8"))
            if isinstance(parsed, dict):
                metadata.update(parsed)
        except (ValueError, UnicodeDecodeError):
            pass
    return metadata, content


def publish_business_insights_dir(root_dir: Path) -> int:
    """Upload reports.json plus all current artifacts (Mac side). Returns file count."""
    bucket_name = public_gcs_publish_bucket()
    if not bucket_name:
        return 0
    root_dir = Path(root_dir)
    uploaded = 0
    metadata_path = root_dir / "reports.json"
    if metadata_path.exists() and gcs_upload_file(bucket_name, metadata_path, "business_insights/reports.json"):
        uploaded += 1
    artifacts_dir = root_dir / "artifacts"
    if artifacts_dir.is_dir():
        for path in sorted(artifacts_dir.iterdir()):
            if not path.is_file() or path.name.startswith("."):
                continue
            if gcs_upload_file(bucket_name, path, f"business_insights/artifacts/{path.name}"):
                uploaded += 1
    logger.info("Published %d Business Insights files to gs://%s", uploaded, bucket_name)
    return uploaded


def hydrate_business_insights_metadata(root_dir: Path) -> bool:
    """Refresh the local reports.json copy from GCS (Cloud Run side)."""
    bucket_name = public_gcs_read_bucket()
    if not bucket_name:
        return False
    return gcs_fetch_to_file(
        bucket_name,
        "business_insights/reports.json",
        Path(root_dir) / "reports.json",
        max_age_seconds=PUBLIC_GCS_METADATA_TTL_SECONDS,
    )


def hydrate_business_insights_artifact(root_dir: Path, filename: str) -> bool:
    """Ensure one artifact file exists locally, fetching from GCS if needed."""
    bucket_name = public_gcs_read_bucket()
    if not bucket_name or not filename:
        return False
    local_path = Path(root_dir) / "artifacts" / filename
    if local_path.exists():
        return True
    return gcs_fetch_to_file(bucket_name, f"business_insights/artifacts/{filename}", local_path)
