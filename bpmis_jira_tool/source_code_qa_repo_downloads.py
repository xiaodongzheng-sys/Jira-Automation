from __future__ import annotations

import io
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import SourceCodeQAService


REPO_DOWNLOAD_SCOPES: tuple[dict[str, str], ...] = (
    {"label": "AF-All", "scope_key": "AF:All", "filename": "source-code-repos-AF-All.zip"},
    {"label": "Credit Risk-ID", "scope_key": "CRMS:ID", "filename": "source-code-repos-Credit-Risk-ID.zip"},
    {"label": "Credit Risk-SG", "scope_key": "CRMS:SG", "filename": "source-code-repos-Credit-Risk-SG.zip"},
    {"label": "Credit Risk-PH", "scope_key": "CRMS:PH", "filename": "source-code-repos-Credit-Risk-PH.zip"},
    {"label": "GRC-All", "scope_key": "GRC:All", "filename": "source-code-repos-GRC-All.zip"},
)

_SKIP_DIR_NAMES = {
    ".git",
    ".pytest_cache",
    ".mypy_cache",
    "__pycache__",
    ".idea",
    ".vscode",
    ".team_portal",
}
_SKIP_FILE_NAMES = {
    ".DS_Store",
}


def _slugify_scope_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(label or "").strip().lower()).strip("-")


_SCOPE_BY_SLUG = {
    _slugify_scope_label(item["label"]): item
    for item in REPO_DOWNLOAD_SCOPES
}


def repo_download_scope_definitions() -> list[dict[str, str]]:
    return [dict(item) for item in REPO_DOWNLOAD_SCOPES]


def resolve_repo_download_scope(scope_key: str) -> dict[str, str]:
    normalized = str(scope_key or "").strip()
    if not normalized:
        raise ToolError("Repo download scope key is required.")
    for item in REPO_DOWNLOAD_SCOPES:
        if normalized == item["scope_key"] or normalized == item["label"]:
            return dict(item)
    slug_match = _SCOPE_BY_SLUG.get(_slugify_scope_label(normalized))
    if slug_match:
        return dict(slug_match)
    raise ToolError("Unknown repo download scope.")


def build_repo_download_zip(service: SourceCodeQAService, scope_key: str) -> tuple[dict[str, Any], bytes]:
    scope = resolve_repo_download_scope(scope_key)
    key = scope["scope_key"]
    entries = service._load_entries_for_key(key)
    if not entries:
        raise ToolError(f"No repositories are configured for {scope['label']}.")

    repo_items: list[dict[str, Any]] = []
    for entry in entries:
        repo_path = service._repo_path(key, entry)
        git_dir = repo_path / ".git"
        if not git_dir.exists():
            raise ToolError(f"{entry.display_name} is not synced yet for {scope['label']}. Run Sync / Refresh first.")
        revision = service._repo_git_revision(repo_path)
        if not revision:
            raise ToolError(f"{entry.display_name} does not have a readable git revision for {scope['label']}.")
        fingerprint = service._repo_fingerprint(repo_path)
        repo_items.append(
            {
                "entry": entry,
                "path": repo_path,
                "revision": revision,
                "fingerprint": fingerprint,
                "folder": _repo_archive_folder_name(entry.display_name),
            }
        )

    manifest = {
        "scope_key": key,
        "scope_label": scope["label"],
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "repos": [
            {
                "display_name": item["entry"].display_name,
                "source_url": _sanitize_source_url(item["entry"].url),
                "git_revision": item["revision"],
                "fingerprint": item["fingerprint"],
                "archive_root": item["folder"],
            }
            for item in repo_items
        ],
    }
    cache_root = service.data_root / "repo_download_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    signature = _manifest_signature(manifest)
    archive_path = cache_root / f"{_slugify_scope_label(scope['label'])}-{signature}.zip"
    metadata = {
        "scope_key": key,
        "scope_label": scope["label"],
        "filename": scope["filename"],
        "generated_at": manifest["generated_at"],
    }
    if archive_path.exists():
        content = archive_path.read_bytes()
        _publish_archive_best_effort(metadata, content)
        return metadata, content

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        for item in repo_items:
            _write_repo_to_archive(archive, repo_path=item["path"], archive_root=item["folder"])
    archive_path.write_bytes(buffer.getvalue())
    _publish_archive_best_effort(metadata, buffer.getvalue())
    return metadata, buffer.getvalue()


def _publish_archive_best_effort(metadata: dict[str, Any], content: bytes) -> None:
    # Mirror freshly built bundles to the public GCS bucket (no-op unless the
    # publish bucket env is configured on the producing host).
    try:
        from bpmis_jira_tool.public_artifacts_gcs import publish_repo_download_archive

        publish_repo_download_archive(metadata, content)
    except Exception:  # noqa: BLE001 - publishing must never break the download
        pass


def _manifest_signature(manifest: dict[str, Any]) -> str:
    payload = json.dumps(manifest.get("repos") or [], sort_keys=True, separators=(",", ":")).encode("utf-8")
    import hashlib

    return hashlib.sha1(payload).hexdigest()[:16]


def _repo_archive_folder_name(display_name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", str(display_name or "").strip()).strip("-")
    return slug[:80] or "repo"


def _sanitize_source_url(url: str) -> str:
    parsed = urlsplit(str(url or "").strip())
    return urlunsplit((parsed.scheme, parsed.hostname or parsed.netloc.split("@")[-1], parsed.path, "", ""))


def _write_repo_to_archive(archive: zipfile.ZipFile, *, repo_path: Path, archive_root: str) -> None:
    for path in sorted(repo_path.rglob("*")):
        relative = path.relative_to(repo_path)
        if _should_skip_path(relative, path.is_dir()):
            continue
        if path.is_dir():
            continue
        if not path.is_file():
            continue
        target = Path(archive_root) / relative
        archive.write(path, arcname=target.as_posix())


def _should_skip_path(relative: Path, is_dir: bool) -> bool:
    parts = relative.parts
    if any(part in _SKIP_DIR_NAMES for part in parts):
        return True
    name = parts[-1] if parts else ""
    if name in _SKIP_FILE_NAMES:
        return True
    if is_dir and name.startswith(".git"):
        return True
    return False
