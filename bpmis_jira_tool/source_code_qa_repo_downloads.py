from __future__ import annotations

import io
import hashlib
import json
import os
import re
import base64
import zlib
import zipfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import SourceCodeQAService


REPO_DOWNLOAD_SCOPES: tuple[dict[str, str], ...] = (
    {"label": "AF-All", "scope_key": "AF:All", "filename": "source-code-repos-AF-All.zip"},
    {"label": "CRMS-All", "scope_key": "CRMS:All", "filename": "source-code-repos-CRMS-All.zip"},
    {"label": "GRC-All", "scope_key": "GRC:All", "filename": "source-code-repos-GRC-All.zip"},
    {"label": "SG Market", "scope_key": "SG_MARKET:All", "filename": "source-code-repos-SG-Market-All.zip"},
    {"label": "ID Market", "scope_key": "ID_MARKET:All", "filename": "source-code-repos-ID-Market-All.zip"},
    {"label": "PH Market", "scope_key": "PH_MARKET:All", "filename": "source-code-repos-PH-Market-All.zip"},
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
    "uml",
}
_SKIP_FILE_NAMES = {
    ".DS_Store",
    "package-lock.json", "npm-shrinkwrap.json", "composer.lock", "pnpm-lock.yaml",
}

BUNDLE_FORMAT = "source-and-index-v4"
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
_SOURCE_FILENAMES = {"Dockerfile", "Makefile", "Jenkinsfile", "Gemfile", "Rakefile", "Procfile", "go.mod", "go.sum", ".gitmodules"}
_CONTEXT_SUFFIXES = {".md", ".rst", ".adoc", ".txt"}
_CONTEXT_NAME_PATTERN = re.compile(r"^(readme|agents|architecture|design|overview|flow|domain|api|openapi|swagger)(?:[._-]|$)", re.I)
_CONFIG_SUFFIXES = {".xml", ".json", ".yaml", ".yml", ".properties", ".toml", ".ini", ".conf", ".cfg"}


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
    if normalized in {"CRMS:SG", "CRMS:ID", "CRMS:PH"} or _slugify_scope_label(normalized) in {"credit-risk-sg", "credit-risk-id", "credit-risk-ph", "credit-risk-all"}:
        normalized = "CRMS:All"
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
                "index_version": "download-definitions-v1",
                "working_tree_clean": service._repo_worktree_clean(repo_path),
            }
        )

    manifest = {
        "format": BUNDLE_FORMAT,
        "contents": "Source code, file and symbol indexes, build/configuration files and key code documents. Excludes images, runtime datasets, dependencies and compiled output.",
        "index_kind": "Download file and symbol definitions; full Q&A search indexes are separate.",
        "selection": {"priority": "Source coverage for every repository, production source across repositories, build/configuration, key documents, tests/examples", "omitted_files": 0},
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
                "working_tree_clean": item["working_tree_clean"],
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
        try:
            cached_manifest = validate_repo_download_archive(content, expected_scope=key)
        except ToolError:
            pass  # Replace a corrupt or obsolete cache with a fresh source bundle.
        else:
            metadata["generated_at"] = cached_manifest["generated_at"]
            metadata.update(repo_download_validation_metadata(content, cached_manifest))
            return metadata, content

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for item, repo_manifest in zip(repo_items, manifest["repos"]):
            files = _write_repo_to_archive(archive, repo_path=item["path"], archive_root=item["folder"])
            archive.writestr(repo_manifest["file_index"], _json_lines(files))
            symbols = _source_symbols(service, item["path"], files, item["folder"])
            archive.writestr(repo_manifest["symbol_index"], _json_lines(symbols))
            repo_manifest["source_files"] = len(files)
            repo_manifest["symbols"] = len(symbols)
            _record_source_coverage(repo_manifest, files)
        archive.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
    content = _fit_bundle_to_limit(buffer.getvalue(), manifest)
    validated = validate_repo_download_archive(content, expected_scope=key)
    metadata.update(repo_download_validation_metadata(content, validated))
    for item in repo_items:
        if (service._repo_git_revision(item["path"]) != item["revision"]
                or service._repo_fingerprint(item["path"]) != item["fingerprint"]):
            raise ToolError("Repository changed during export: " + item["entry"].display_name)
    if service._load_entries_for_key(key) != entries:
        raise ToolError("Repository mapping changed during export. Retry the download.")
    temp_path = archive_path.with_name(f".{archive_path.name}.{os.getpid()}.tmp")
    temp_path.write_bytes(content)
    os.replace(temp_path, archive_path)
    metadata["size_bytes"] = len(content)
    metadata["omitted_files"] = manifest["selection"]["omitted_files"]
    return metadata, content


def existing_repo_download_archive(service: SourceCodeQAService, scope_key: str) -> tuple[dict[str, Any], Path] | None:
    scope = resolve_repo_download_scope(scope_key)
    # The remote QA wrapper still has a local service with published bundles.
    # Read those files before trying GCS or rebuilding through the local agent.
    local_service = getattr(service, "fallback_service", service)
    data_root = getattr(local_service, "data_root", None)
    if not isinstance(data_root, Path):
        return None
    archive_path = data_root / "repo_downloads" / scope["filename"]
    if not archive_path.is_file():
        return None
    if archive_path.stat().st_size >= MAX_BUNDLE_BYTES:
        # Rebuild old oversized archives using the current selection policy.
        return None
    try:
        stat = archive_path.stat()
        manifest = _validated_local_archive(archive_path, stat.st_ino, stat.st_size, stat.st_mtime_ns, scope["scope_key"])
    except (OSError, ToolError):
        return None
    return {
        **scope,
        "scope_label": scope["label"],
        "generated_at": manifest.get("generated_at"),
        "repo_count": len(manifest["repos"]),
        "format": manifest["format"],
    }, archive_path


@lru_cache(maxsize=24)
def _validated_local_archive(path: Path, inode: int, size: int, mtime_ns: int, scope: str) -> dict[str, Any]:
    # Stat values invalidate the validation result after atomic publication.
    return validate_repo_download_archive(path, expected_scope=scope)


def _manifest_signature(manifest: dict[str, Any]) -> str:
    payload = json.dumps({"format": manifest.get("format"), "max_archive_bytes": manifest.get("max_archive_bytes"), "repos": manifest.get("repos") or []}, sort_keys=True, separators=(",", ":")).encode("utf-8")
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
            is_context = path.suffix.lower() in _CONTEXT_SUFFIXES and _CONTEXT_NAME_PATTERN.match(path.name)
            if path.suffix.lower() not in _SOURCE_SUFFIXES and path.name not in _SOURCE_FILENAMES and not is_context:
                continue
            if path.name.endswith((".min.js", ".min.css")):
                continue
            if path.suffix.lower() == ".json" and "resources/data/" in relative.as_posix():
                continue
            content = path.read_bytes()
            if b"\x00" in content:
                continue
            target = (Path(archive_root) / relative).as_posix()
            archive.writestr(target, content)
            files.append({"path": target, "bytes": len(content), "lines": len(content.splitlines()), "sha256": hashlib.sha256(content).hexdigest()})
    return files


def _source_symbols(service: SourceCodeQAService, repo_path: Path, files: list[dict[str, Any]], folder: str) -> list[dict[str, Any]]:
    # Download indexes do not need the full Q&A SQLite/reference graph.
    kinds = {
        "class_declaration": "class", "interface_declaration": "interface",
        "enum_declaration": "enum", "record_declaration": "record",
        "annotation_type_declaration": "annotation", "method_declaration": "method",
        "constructor_declaration": "constructor", "function_declaration": "function",
        "function_definition": "function", "method_definition": "method",
        "type_alias_declaration": "type", "type_declaration": "type",
    }
    fallback = re.compile(r"\b(class|interface|enum|record|struct|trait|object|protocol|typealias|def|function|fun|func|fn)\s+([A-Za-z_][A-Za-z_0-9]*)")
    symbols = []
    for row in files:
        path = row["path"]
        if not _is_code_file(row):
            continue
        source = (repo_path / Path(path).relative_to(folder)).read_bytes()
        language = service._tree_sitter_language_for_suffix(Path(path).suffix)
        parser = service._tree_sitter_parser_for_language(language) if language else None
        if parser is not None:
            pending = [parser.parse(source).root_node]
            while pending:
                node = pending.pop()
                if node.type in kinds:
                    name = service._tree_sitter_name_for_node(source, node)
                    if name:
                        symbols.append({"path": path, "line": node.start_point[0] + 1, "name": name, "kind": kinds[node.type]})
                pending.extend(reversed(node.named_children))
        else:
            for line_no, line in enumerate(source.decode("utf-8", errors="replace").splitlines(), 1):
                for match in fallback.finditer(line):
                    symbols.append({"path": path, "line": line_no, "name": match[2], "kind": match[1]})
    return symbols


def _is_code_file(row: dict[str, Any]) -> bool:
    return row.get("bytes", 0) > 0 and Path(row["path"]).suffix.lower() in _SOURCE_SUFFIXES - _CONFIG_SUFFIXES - {".gradle"}


def _record_source_coverage(repo: dict[str, Any], files: list[dict[str, Any]]) -> None:
    code_files = sum(_is_code_file(row) for row in files)
    repo.setdefault("original_code_files", code_files)
    repo["code_files"] = code_files
    repo.setdefault("original_source_files", len(files))
    repo["source_files"] = len(files)
    repo["omitted_files"] = repo["original_source_files"] - len(files)
    repo["source_kind"] = "code" if repo["original_code_files"] else "configuration"
    if not files or (repo["original_code_files"] and not code_files):
        raise ToolError("Download must retain source from repository: " + repo["display_name"])
    if not code_files and not any(row.get("bytes", 0) and (Path(row["path"]).suffix.lower() in _SOURCE_SUFFIXES or Path(row["path"]).name in _SOURCE_FILENAMES) for row in files):
        raise ToolError("Repository has no downloadable source or configuration: " + repo["display_name"])


def _json_lines(rows: list[dict[str, Any]]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows)


def _bundle_file_priority(filename: str) -> tuple[int, str]:
    path = Path(filename)
    parts = {part.lower() for part in path.parts[1:-1]}
    if parts & {"test", "tests", "__tests__", "fixtures", "examples", "samples"} or re.search(r"(?:[._-](?:test|spec)|_test)\.", path.name):
        return 3, filename
    if path.suffix.lower() in _CONTEXT_SUFFIXES or parts & {"docs", "doc", "documentation"}:
        return 2, filename
    if path.suffix.lower() in _CONFIG_SUFFIXES or path.name in _SOURCE_FILENAMES:
        return 1, filename
    return 0, filename


def _fit_bundle_to_limit(content: bytes, manifest: dict[str, Any]) -> bytes:
    if len(content) < MAX_BUNDLE_BYTES:
        return content
    with zipfile.ZipFile(io.BytesIO(content)) as original:
        groups = {}
        required = []
        for repo in manifest["repos"]:
            by_path = {}
            for line in original.read(repo["symbol_index"]).splitlines():
                symbol = json.loads(line)
                by_path.setdefault(symbol["path"], []).append(symbol)
            files = [json.loads(line) for line in original.read(repo["file_index"]).splitlines()]
            _record_source_coverage(repo, files)
            candidates = []
            for row in files:
                name = row["path"]
                symbols = by_path.get(name, [])
                info = original.getinfo(name)
                # Charge the matching file/symbol index as well as ZIP entry overhead.
                index_bytes = (_json_lines([row]) + _json_lines(symbols)).encode()
                cost = info.compress_size + 76 + 2 * len(name.encode()) + len(zlib.compress(index_bytes))
                item = (name, row, symbols, cost)
                priority = _bundle_file_priority(name)[0]
                relative = Path(name).relative_to(repo["archive_root"])
                critical = {"pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "package.json", "go.mod", "cargo.toml", "dockerfile", "makefile", "readme.md", "readme.rst", "readme"}
                if len(relative.parts) <= 3 and relative.name.lower() in critical and cost <= 16_000:
                    priority = -1
                groups.setdefault((priority, repo["archive_root"]), []).append(item)
                is_source = _is_code_file(row) if repo["original_code_files"] else row.get("bytes", 0) and Path(name).suffix.lower() not in _CONTEXT_SUFFIXES
                if is_source:
                    candidates.append((priority, cost, name, item))
            if not candidates:
                raise ToolError("Repository has no downloadable source: " + repo["display_name"])
            # Reserve a whole source file per repository before any other allocation.
            required.append(min(candidates, key=lambda item: item[:3])[3])
        ranked = []
        for (priority, folder), items in groups.items():
            for rank, item in enumerate(sorted(items, key=lambda row: row[0])):
                ranked.append((priority, rank, folder, item))
        ranked.sort(key=lambda row: row[:3])
        margin = min(256_000, MAX_BUNDLE_BYTES // 20)
        budget = MAX_BUNDLE_BYTES - margin - 2 * len(json.dumps(manifest).encode())
        for _ in range(6):
            selected = {item[0]: item for item in required}
            remaining = budget - sum(item[3] for item in selected.values())
            if remaining < 0:
                raise ToolError("Required source coverage cannot fit below 20 MB.")
            for _, _, _, item in ranked:
                name, _, _, cost = item
                if name not in selected and cost <= remaining:
                    selected[name] = item
                    remaining -= cost
            manifest["selection"]["omitted_files"] = sum(len(items) for items in groups.values()) - len(selected)
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as output:
                for name in selected:
                    output.writestr(name, original.read(name))
                for repo in manifest["repos"]:
                    rows = [item for name, item in selected.items() if name.startswith(repo["archive_root"] + "/")]
                    files = [row for _, row, _, _ in rows]
                    symbols = [symbol for _, _, items, _ in rows for symbol in items]
                    _record_source_coverage(repo, files)
                    repo["symbols"] = len(symbols)
                    output.writestr(repo["file_index"], _json_lines(files))
                    output.writestr(repo["symbol_index"], _json_lines(symbols))
                output.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
            result = buffer.getvalue()
            if len(result) < MAX_BUNDLE_BYTES:
                return result
            budget -= len(result) - MAX_BUNDLE_BYTES + margin
    raise ToolError("Could not fit a source download bundle below 20 MB.")


def validate_repo_download_archive(content: bytes | Path, *, expected_scope: str | None = None) -> dict[str, Any]:
    """Reject old, index-only, corrupt, or source-less bundles before serving/publishing."""
    require_repo_download_size(len(content) if isinstance(content, bytes) else content.stat().st_size)
    try:
        with zipfile.ZipFile(io.BytesIO(content) if isinstance(content, bytes) else content) as archive:
            manifest = json.loads(archive.read("manifest.json"))
            if manifest.get("format") != BUNDLE_FORMAT or not manifest.get("repos"):
                raise ValueError("obsolete or empty bundle")
            if expected_scope and manifest.get("scope_key") != expected_scope:
                raise ValueError("wrong scope")
            names = set(archive.namelist())
            if len(names) != len(archive.namelist()):
                raise ValueError("duplicate ZIP entries")
            roots = set()
            allowed_names = {"manifest.json"}
            for repo in manifest["repos"]:
                root = repo["archive_root"]
                if root in roots or not root or "/" in root:
                    raise ValueError("invalid repository root")
                roots.add(root)
                files = [json.loads(line) for line in archive.read(repo["file_index"]).splitlines()]
                paths = {row["path"] for row in files}
                if paths != {name for name in names if name.startswith(root + "/")} or len(paths) != len(files):
                    raise ValueError("file index mismatch")
                for row in files:
                    if ".." in Path(row["path"]).parts:
                        raise ValueError("invalid source path")
                    raw = archive.read(row["path"])
                    if len(raw) != row["bytes"] or hashlib.sha256(raw).hexdigest() != row["sha256"]:
                        raise ValueError("source checksum mismatch")
                if repo["original_code_files"] < repo["code_files"] or repo["original_source_files"] < len(files):
                    raise ValueError("invalid original source counts")
                checked = dict(repo)
                _record_source_coverage(checked, files)
                if checked["code_files"] != repo.get("code_files") or len(files) != repo.get("source_files"):
                    raise ValueError("source coverage mismatch")
                symbols = [json.loads(line) for line in archive.read(repo["symbol_index"]).splitlines()]
                line_counts = {row["path"]: row["lines"] for row in files}
                if len(symbols) != repo["symbols"] or any(row["path"] not in paths or not 1 <= row["line"] <= line_counts[row["path"]] for row in symbols):
                    raise ValueError("symbol index mismatch")
                allowed_names.update(paths | {repo["file_index"], repo["symbol_index"]})
            if allowed_names != names:
                raise ValueError("unexpected content outside source and indexes")
            return manifest
    except (OSError, ValueError, TypeError, KeyError, zipfile.BadZipFile) as error:
        raise ToolError("Invalid source download bundle; rebuild it with the current source policy.") from error


def repo_download_validation_metadata(content: bytes, manifest: dict[str, Any] | None = None) -> dict[str, Any]:
    manifest = manifest or validate_repo_download_archive(content)
    return {
        "format": BUNDLE_FORMAT,
        "scope_key": manifest["scope_key"],
        "scope_label": manifest["scope_label"],
        "generated_at": manifest["generated_at"],
        "source_verified": True,
        "sha256": hashlib.sha256(content).hexdigest(),
        "md5_base64": base64.b64encode(hashlib.md5(content).digest()).decode(),
        "size_bytes": len(content),
        "repo_count": len(manifest["repos"]),
        "code_files": sum(repo["code_files"] for repo in manifest["repos"]),
        "source_files": sum(repo["source_files"] for repo in manifest["repos"]),
        "symbols": sum(repo["symbols"] for repo in manifest["repos"]),
        "omitted_files": manifest.get("selection", {}).get("omitted_files", 0),
    }


def require_repo_download_size(size_bytes: int) -> None:
    if size_bytes >= MAX_BUNDLE_BYTES:
        raise ToolError(f"Source bundle is {size_bytes / 1_000_000:.2f} MB; it must be smaller than 20 MB. Run Sync / Refresh to rebuild it.")


def _require_bundle_size(content: bytes) -> None:
    require_repo_download_size(len(content))


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
