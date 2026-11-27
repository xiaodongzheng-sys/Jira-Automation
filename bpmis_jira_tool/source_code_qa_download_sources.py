"""Lossless export of Git text blobs and pinned submodules for Market downloads."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import subprocess
import threading
from urllib.parse import urljoin, urlsplit, urlunsplit
import zipfile

from bpmis_jira_tool.errors import ToolError

FULL_SOURCE_POLICY = "all-available-git-text-with-pinned-runtime-submodules-v1"
FULL_SOURCE_SCOPES = {"SG_MARKET:All", "ID_MARKET:All", "PH_MARKET:All", "AF:All", "CRMS:All", "GRC:All", "DIGITAL_OPS:All"}
AF_REFERENCE = re.compile(rb"fraud|risk|ivlog|fingerprint|blackbox|threatmetrix|tongdun|(?:\b|_)af(?:\b|_)", re.I)


def part_filename(filename: str, number: int) -> str:
    if not 1 <= number <= 999:
        raise ToolError("Unknown repo download part.")
    return filename if number == 1 else filename[:-4] + f"-part-{number:02d}.zip"


def https_git_url(value: str, parent_url: str = "") -> str:
    value = value.strip()
    if value.startswith("."):
        value = urljoin(parent_url.rstrip("/") + "/", value)
    if re.match(r"^[^/@]+@[^/:]+:", value):
        value = "https://" + value.split("@", 1)[1].replace(":", "/", 1)
    parsed = urlsplit(value)
    if parsed.scheme in {"ssh", "https", "http"}:
        return urlunsplit(("https", parsed.hostname or "", parsed.path, "", ""))
    # Local paths are useful for isolated tests; never interpreted by a shell.
    return value


def submodule_cache_path(cache_root: Path, url: str, revision: str) -> Path:
    digest = hashlib.sha256(url.encode()).hexdigest()[:16]
    return cache_root / "repo_download_submodules" / f"{digest}-{revision}"


def git_tree(repo: Path, revision: str = "HEAD") -> list[tuple[str, str, str]]:
    result = subprocess.run(["git", "-C", str(repo), "ls-tree", "-r", "-z", revision], capture_output=True, check=True)
    rows = []
    for row in result.stdout.split(b"\0"):
        if row:
            info, path = row.split(b"\t", 1)
            mode, _, oid = info.decode().split()
            rows.append((mode, oid, os.fsdecode(path)))
    return rows


def submodule_urls(repo: Path, revision: str = "HEAD") -> dict[str, str]:
    result = subprocess.run(["git", "-C", str(repo), "config", "--blob", revision + ":.gitmodules", "--get-regexp", r"^submodule\..*\.(path|url)$"], capture_output=True, text=True)
    fields = {}
    for line in result.stdout.splitlines():
        key, value = line.split(None, 1)
        section, field = key.rsplit(".", 1)
        fields.setdefault(section, {})[field] = value
    origin = subprocess.run(["git", "-C", str(repo), "remote", "get-url", "origin"], capture_output=True, text=True).stdout.strip()
    origin = https_git_url(origin)
    return {entry["path"]: https_git_url(entry["url"], origin) for entry in fields.values() if "path" in entry and "url" in entry}


def iter_git_blobs(repo: Path, rows: list[tuple[str, str, str]]):
    """Use committed blobs so case-only filenames and local edits cannot lose code."""
    blobs = [row for row in rows if row[0] != "160000"]
    process = subprocess.Popen(["git", "-C", str(repo), "cat-file", "--batch"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    errors = []

    def feed():
        try:
            process.stdin.write("".join(oid + "\n" for _, oid, _ in blobs).encode())
            process.stdin.close()
        except (OSError, ValueError) as error:
            errors.append(error)

    writer = threading.Thread(target=feed, daemon=True)
    writer.start()
    try:
        for mode, oid, relative in blobs:
            header = process.stdout.readline().split()
            if len(header) != 3 or header[1] != b"blob":
                raise ToolError("A Git source blob is unavailable: " + relative)
            remaining = int(header[2])
            chunks = []
            while remaining:
                chunk = process.stdout.read(remaining)
                if not chunk:
                    raise ToolError("Incomplete Git source blob: " + relative)
                chunks.append(chunk)
                remaining -= len(chunk)
            if process.stdout.read(1) != b"\n":
                raise ToolError("Invalid Git source stream.")
            yield mode, oid, relative, b"".join(chunks)
        writer.join(timeout=10)
        if errors or writer.is_alive() or process.wait(timeout=10):
            raise ToolError("Git source export failed.")
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        if process.stdin and not process.stdin.closed:
            process.stdin.close()
        process.stdout.close()
        writer.join(timeout=10)


def is_text_blob(content: bytes) -> bool:
    return b"\0" not in content or content.startswith((b"\xff\xfe", b"\xfe\xff", b"\x00\x00\xfe\xff"))


def write_git_sources(archive: zipfile.ZipFile, *, repo_path: Path, archive_root: str, cache_root: Path) -> tuple[list[dict], dict]:
    files = []
    audit = {"policy": FULL_SOURCE_POLICY, "binary_files_excluded": 0, "submodules": [], "unavailable_submodules": [], "af_reference_files": 0}

    def write(repo: Path, revision: str, prefix: str, ancestors: tuple[str, ...] = ()):
        if len(ancestors) >= 16 or revision in ancestors:
            raise ToolError("Cyclic or excessively deep Git submodule: " + prefix)
        rows = git_tree(repo, revision)
        for mode, oid, relative, content in iter_git_blobs(repo, rows):
            if not is_text_blob(content):
                audit["binary_files_excluded"] += 1
                continue
            target = prefix + "/" + relative
            if ".." in Path(target).parts or Path(target).is_absolute():
                raise ToolError("Invalid Git source path.")
            info = zipfile.ZipInfo(target)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = int(mode, 8) << 16
            archive.writestr(info, content, compresslevel=9)
            af_reference = bool(AF_REFERENCE.search(target.encode() + b"\n" + content))
            files.append({"path": target, "bytes": len(content), "lines": len(content.splitlines()), "sha256": hashlib.sha256(content).hexdigest(), "git_blob": oid, "git_mode": mode, "af_reference": af_reference})
            audit["af_reference_files"] += af_reference
        urls = submodule_urls(repo, revision)
        for mode, oid, relative in rows:
            if mode != "160000":
                continue
            url = urls.get(relative)
            if not url:
                raise ToolError("Missing Git submodule mapping: " + relative)
            checkout = repo / relative
            actual = subprocess.run(["git", "-C", str(checkout), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip() if checkout.exists() else ""
            if actual != oid:
                checkout = submodule_cache_path(cache_root, url, oid)
            actual = subprocess.run(["git", "-C", str(checkout), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip() if checkout.exists() else ""
            if actual != oid:
                if relative == "fastlane" and url == "https://gitlab.npt.seabank.io/bfe-horizontal-products/app/ios/libs/fastlane.git":
                    audit["unavailable_submodules"].append({"path": prefix + "/" + relative, "source_url": url, "git_revision": oid, "state": "unavailable", "category": "ios_build_tooling"})
                    continue
                raise ToolError("Pinned runtime submodule source is missing; sync it before export: " + relative)
            sub_prefix = prefix + "/" + relative
            audit["submodules"].append({"path": sub_prefix, "source_url": url, "git_revision": oid})
            write(checkout, oid, sub_prefix, (*ancestors, revision))

    revision = subprocess.run(["git", "-C", str(repo_path), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    write(repo_path, revision, archive_root)
    return files, audit


def submodule_cache_version(cache_root: Path) -> str:
    snapshots = []
    for ref in sorted((cache_root / "repo_download_submodules").glob("*/refs/heads/snapshot")):
        snapshots.append((ref.parent.parent.parent.name, ref.read_text().strip()))
    return hashlib.sha256(repr(snapshots).encode()).hexdigest()[:16]
