from __future__ import annotations

import io
import hashlib
import json
import os
import re
import sqlite3
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
    "node_modules", "vendor", "target", "dist", "build", "out",
    "coverage", ".gradle", ".next", ".nuxt", ".venv", "venv",
    "docs", "doc", "documentation", "uml",
}
_SKIP_FILE_NAMES = {
    ".DS_Store",
    "package-lock.json", "npm-shrinkwrap.json", "composer.lock",
}

BUNDLE_FORMAT = "source-and-index-v2"
MAX_BUNDLE_BYTES = 20_000_000
_SOURCE_SUFFIXES = {
    ".c", ".cc", ".cpp", ".h", ".hpp", ".cs", ".fs", ".go",
    ".java", ".kt", ".kts", ".scala", ".groovy", ".gradle",
    ".py", ".pyi", ".rb", ".php", ".rs", ".swift", ".m", ".mm",
    ".js", ".jsx", ".ts", ".tsx", ".vue", ".svelte", ".dart",
    ".html", ".htm", ".css", ".scss", ".sass", ".less", ".jsp", ".jspx",
    ".sh", ".bash", ".zsh", ".ps1", ".lua", ".pl", ".pm", ".r",
    ".sql", ".graphql", ".gql", ".proto", ".thrift", ".xsd", ".wsdl",
    ".xml", ".json", ".yaml", ".yml", ".properties", ".toml", ".ini",
    ".conf", ".cfg", ".tf", ".hcl", ".ftl", ".vm", ".j2", ".tpl",
    ".clj", ".cljs", ".ex", ".exs", ".erl", ".hrl", ".sol",
}
_SOURCE_FILENAMES = {"Dockerfile", "Makefile", "Jenkinsfile", "Gemfile", "Rakefile", "Procfile"}


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
    used_archive_folders: set[str] = set()
    for entry in entries:
        repo_path = service._repo_path(key, entry)
        git_dir = repo_path / ".git"
        if not git_dir.exists():
            raise ToolError(f"{entry.display_name} is not synced yet for {scope['label']}. Run Sync / Refresh first.")
        revision = service._repo_git_revision(repo_path)
        if not revision:
            raise ToolError(f"{entry.display_name} does not have a readable git revision for {scope['label']}.")
        fingerprint = service._repo_fingerprint(repo_path)
        index_info = service._repo_index_info(key, entry, repo_path)
        if index_info.get("state") != "ready":
            raise ToolError(f"{entry.display_name} needs a current code index. Run Sync / Refresh first.")
        archive_folder = _repo_archive_folder_name(entry.display_name)
        if archive_folder in used_archive_folders:
            suffix = 2
            while f"{archive_folder}-{suffix}" in used_archive_folders:
                suffix += 1
            archive_folder = f"{archive_folder}-{suffix}"
        used_archive_folders.add(archive_folder)
        repo_items.append(
            {
                "entry": entry,
                "path": repo_path,
                "revision": revision,
                "fingerprint": fingerprint,
                "folder": archive_folder,
                "index_path": service._index_path(repo_path),
                "index_version": index_info.get("index_version"),
            }
        )

    manifest = {
        "format": BUNDLE_FORMAT,
        "contents": "Source code, build/configuration files, file indexes and symbol indexes. No documentation, images, dependency packages or compiled output.",
        "max_archive_bytes": MAX_BUNDLE_BYTES,
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
                "index_version": item["index_version"],
                "file_index": f"indexes/{item['folder']}.files.jsonl",
                "symbol_index": f"indexes/{item['folder']}.symbols.jsonl",
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
        "format": BUNDLE_FORMAT,
        "repo_count": len(repo_items),
    }
    if archive_path.exists():
        content = archive_path.read_bytes()
        _require_bundle_size(content)
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            cached_manifest = json.loads(archive.read("manifest.json"))
        metadata["generated_at"] = cached_manifest["generated_at"]
        metadata["size_bytes"] = len(content)
        return metadata, content

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for item, repo_manifest in zip(repo_items, manifest["repos"]):
            files = _write_repo_to_archive(archive, repo_path=item["path"], archive_root=item["folder"])
            archive.writestr(repo_manifest["file_index"], _json_lines(files))
            symbols = _source_symbols(item["index_path"], files, item["folder"])
            archive.writestr(repo_manifest["symbol_index"], _json_lines(symbols))
            repo_manifest["source_files"] = len(files)
            repo_manifest["symbols"] = len(symbols)
        archive.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
    content = buffer.getvalue()
    _require_bundle_size(content)
    temp_path = archive_path.with_name(f".{archive_path.name}.{os.getpid()}.tmp")
    temp_path.write_bytes(content)
    os.replace(temp_path, archive_path)
    metadata["size_bytes"] = len(content)
    return metadata, content


def existing_repo_download_archive(service: SourceCodeQAService, scope_key: str) -> tuple[dict[str, Any], Path] | None:
    scope = resolve_repo_download_scope(scope_key)
    data_root = getattr(service, "data_root", None)
    if not isinstance(data_root, Path):
        return None
    archive_path = data_root / "repo_downloads" / scope["filename"]
    if not archive_path.is_file():
        return None
    metadata: dict[str, Any] = {
        "scope_key": scope["scope_key"],
        "scope_label": scope["label"],
        "filename": scope["filename"],
    }
    try:
        with zipfile.ZipFile(archive_path) as archive:
            if "manifest.json" in archive.namelist():
                parsed = json.loads(archive.read("manifest.json").decode("utf-8"))
                if isinstance(parsed, dict):
                    generated_at = parsed.get("generated_at")
                    if generated_at:
                        metadata["generated_at"] = generated_at
                    repos = parsed.get("repos")
                    if isinstance(repos, list):
                        metadata["repo_count"] = len(repos)
    except (OSError, ValueError, zipfile.BadZipFile):
        return None
    return metadata, archive_path


def _manifest_signature(manifest: dict[str, Any]) -> str:
    payload = json.dumps({"format": manifest.get("format"), "repos": manifest.get("repos") or []}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    import hashlib

    return hashlib.sha1(payload).hexdigest()[:16]


def _repo_archive_folder_name(display_name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", str(display_name or "").strip()).strip("-")
    return slug[:80] or "repo"


def _sanitize_source_url(url: str) -> str:
    parsed = urlsplit(str(url or "").strip())
    return urlunsplit((parsed.scheme, parsed.hostname or parsed.netloc.split("@")[-1], parsed.path, "", ""))


def _write_repo_to_archive(archive: zipfile.ZipFile, *, repo_path: Path, archive_root: str) -> list[dict[str, Any]]:
    files = []
    for directory, directories, names in os.walk(repo_path, followlinks=False):
        directories[:] = sorted(name for name in directories if not _should_skip_path(Path(directory).relative_to(repo_path) / name, True))
        for name in sorted(names):
            path = Path(directory) / name
            relative = path.relative_to(repo_path)
            if _should_skip_path(relative, False) or path.is_symlink() or not path.is_file():
                continue
            if path.suffix.lower() not in _SOURCE_SUFFIXES and path.name not in _SOURCE_FILENAMES:
                continue
            if path.name.endswith((".min.js", ".min.css")):
                continue
            content = path.read_bytes()
            if b"\x00" in content:
                continue
            target = (Path(archive_root) / relative).as_posix()
            archive.writestr(target, content)
            files.append({"path": target, "bytes": len(content), "lines": len(content.splitlines()), "sha256": hashlib.sha256(content).hexdigest()})
    return files


def _source_symbols(index_path: Path, files: list[dict[str, Any]], folder: str) -> list[dict[str, Any]]:
    allowed = {item["path"] for item in files}
    symbols = []
    with sqlite3.connect(f"{index_path.as_uri()}?mode=ro", uri=True) as connection:
        for name, kind, file_path, line in connection.execute("select name, kind, file_path, line_no from definitions order by file_path, line_no, name, kind"):
            target = f"{folder}/{file_path}"
            if target in allowed:
                symbols.append({"path": target, "line": line, "name": name, "kind": kind})
    return symbols


def _json_lines(rows: list[dict[str, Any]]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows)


def _require_bundle_size(content: bytes) -> None:
    if len(content) >= MAX_BUNDLE_BYTES:
        raise ToolError(f"Source bundle is {len(content) / 1_000_000:.2f} MB; it must be smaller than 20 MB. No oversized bundle was published.")


def _should_skip_path(relative: Path, is_dir: bool) -> bool:
    parts = relative.parts
    if any(part.lower() in _SKIP_DIR_NAMES for part in parts):
        return True
    name = parts[-1] if parts else ""
    if name in _SKIP_FILE_NAMES:
        return True
    if is_dir and name.startswith(".git"):
        return True
    return False
