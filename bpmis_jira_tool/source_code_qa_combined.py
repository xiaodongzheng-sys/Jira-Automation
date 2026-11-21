"""Build and verify an optional complete ZIP from one published source set."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
import shutil
import tempfile
import zipfile

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa_repo_downloads import (
    existing_repo_download_archive,
    resolve_repo_download_scope,
    validate_repo_download_archive,
)

COMBINED_FORMAT = "combined-source-and-index-v1"


def combined_filename(filename: str) -> str:
    if not filename.endswith(".zip"):
        raise ToolError("Invalid source bundle name.")
    return filename[:-4] + "-combined.zip"


def file_digests(path: Path) -> tuple[str, str]:
    sha = hashlib.sha256()
    md5 = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha.update(chunk)
            md5.update(chunk)
    return sha.hexdigest(), base64.b64encode(md5.digest()).decode("ascii")


def build_combined_archive(artifacts: list[tuple[dict, Path]], destination: Path) -> dict:
    """Merge verified independent parts without a size limit or lost files."""
    if not artifacts:
        raise ToolError("No source parts are available to combine.")
    artifacts = sorted(artifacts, key=lambda item: item[0]["part_number"])
    first = artifacts[0][0]
    scope_key, set_id = first.get("scope_key"), first.get("set_id")
    count = len(artifacts)
    scope = resolve_repo_download_scope(scope_key)
    if not set_id or [p[0].get("part_number") for p in artifacts] != list(range(1, count + 1)):
        raise ToolError("The source parts do not form a complete set.")
    if destination.name != combined_filename(scope["filename"]):
        raise ToolError("Invalid combined source bundle name.")

    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=".combined-", suffix=".zip", dir=destination.parent)
    os.close(fd)
    temporary = Path(temporary_name)
    seen: set[str] = set()
    source_parts = []
    try:
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9, allowZip64=True) as output:
            for metadata, path in artifacts:
                if metadata.get("scope_key") != scope_key or metadata.get("set_id") != set_id or metadata.get("part_count") != count:
                    raise ToolError("Source parts belong to different sets.")
                manifest = validate_repo_download_archive(path, expected_scope=scope_key)
                bundle_set = manifest.get("bundle_set") or {}
                if bundle_set.get("id") != set_id or bundle_set.get("part_number") != metadata.get("part_number") or bundle_set.get("part_count") != count:
                    raise ToolError("Source part manifest does not match its set.")
                if file_digests(path)[0] != metadata.get("sha256"):
                    raise ToolError("Source part checksum does not match its metadata.")
                with zipfile.ZipFile(path) as source:
                    for entry in source.infolist():
                        if entry.filename in {"manifest.json", "READ-ME-FIRST.txt"}:
                            continue
                        if entry.filename in seen:
                            raise ToolError("Source parts contain a duplicate path: " + entry.filename)
                        seen.add(entry.filename)
                        with source.open(entry) as source_file, output.open(copy.copy(entry), "w", force_zip64=True) as output_file:
                            shutil.copyfileobj(source_file, output_file, length=1024 * 1024)
                source_parts.append({"part_number": metadata["part_number"], "filename": path.name, "sha256": metadata["sha256"], "manifest": manifest["part_manifest"]})
            output.writestr("READ-ME-FIRST.txt", "This ZIP combines all source and indexes from the listed numbered parts. It has no size limit.\nThe numbered manifests are in manifests/; file and symbol indexes are in indexes/part-XX/.\nThe smaller numbered ZIPs contain the same source and can be extracted together.\n")
            output.writestr("manifest.json", json.dumps({"format": COMBINED_FORMAT, "scope_key": scope_key, "set_id": set_id, "part_count": count, "source_parts": source_parts}, indent=2))
        with zipfile.ZipFile(temporary) as combined:
            if combined.testzip() is not None or set(combined.namelist()) != seen | {"manifest.json", "READ-ME-FIRST.txt"}:
                raise ToolError("Combined source ZIP validation failed.")
        sha256, md5_base64 = file_digests(temporary)
        metadata = {"format": COMBINED_FORMAT, "filename": destination.name, "scope_key": scope_key, "set_id": set_id, "part_count": count, "generated_at": first.get("generated_at"), "size_bytes": temporary.stat().st_size, "sha256": sha256, "md5_base64": md5_base64, "source_verified": True}
        temporary.chmod(0o600)
        os.replace(temporary, destination)
        meta_temp = destination.with_name("." + destination.name + f".{os.getpid()}.json.tmp")
        meta_temp.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        meta_temp.chmod(0o600)
        os.replace(meta_temp, destination.with_suffix(".zip.json"))
        return metadata
    finally:
        temporary.unlink(missing_ok=True)


@lru_cache(maxsize=24)
def _valid_combined_file(path: Path, inode: int, size: int, mtime_ns: int, sha256: str, scope_key: str, set_id: str) -> bool:
    del inode, size, mtime_ns
    try:
        if file_digests(path)[0] != sha256:
            return False
        with zipfile.ZipFile(path) as archive:
            manifest = json.loads(archive.read("manifest.json"))
            return (manifest.get("format") == COMBINED_FORMAT and manifest.get("scope_key") == scope_key and manifest.get("set_id") == set_id and archive.testzip() is None)
    except (OSError, ValueError, TypeError, KeyError, zipfile.BadZipFile):
        return False


def existing_combined_archive(service: object, scope_key: str) -> tuple[dict, Path] | None:
    """Only serve a combined ZIP when it matches the current numbered set."""
    primary = existing_repo_download_archive(service, scope_key)
    if primary is None:
        return None
    primary_metadata, primary_path = primary
    set_id = primary_metadata.get("set_id")
    scope = resolve_repo_download_scope(scope_key)
    if not set_id or scope.get("part_number", 1) != 1:
        return None
    path = primary_path.parent / combined_filename(scope["filename"])
    try:
        metadata = json.loads(path.with_suffix(".zip.json").read_text(encoding="utf-8"))
        stat = path.stat()
        if not valid_combined_metadata(metadata, scope_key=scope["scope_key"], set_id=set_id, filename=path.name, size=stat.st_size):
            return None
        if not _valid_combined_file(path, stat.st_ino, stat.st_size, stat.st_mtime_ns, metadata["sha256"], scope["scope_key"], set_id):
            return None
        return metadata, path
    except (OSError, ValueError, TypeError, KeyError):
        return None


def install_combined_archive(service: object, path: Path) -> None:
    target_dir = service.data_root / "repo_downloads"
    target_dir.mkdir(parents=True, exist_ok=True)
    for original in (path, path.with_suffix(".zip.json")):
        target = target_dir / original.name
        temporary = target.with_name("." + target.name + f".{os.getpid()}.tmp")
        try:
            shutil.copyfile(original, temporary)
            temporary.chmod(0o600)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)


def valid_combined_metadata(metadata: object, *, scope_key: str, set_id: str, filename: str, size: int, md5_base64: str | None = None) -> bool:
    if not isinstance(metadata, dict):
        return False
    sha = metadata.get("sha256")
    return bool(set_id and metadata.get("format") == COMBINED_FORMAT and metadata.get("source_verified") is True and metadata.get("scope_key") == scope_key and metadata.get("set_id") == set_id and metadata.get("filename") == filename and metadata.get("size_bytes") == size and size > 0 and isinstance(sha, str) and len(sha) == 64 and metadata.get("md5_base64") and (md5_base64 is None or metadata["md5_base64"] == md5_base64))
