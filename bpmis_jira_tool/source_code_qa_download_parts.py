"""Independent ZIP parts that preserve every exported Market source file."""
from __future__ import annotations

import copy
import io
import json
import os
from pathlib import Path
import zipfile
import zlib

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa_download_sources import FULL_SOURCE_POLICY, part_filename


def _split_and_cache_unlocked(content: bytes, manifest: dict, metadata: dict, directory: Path, requested_part: int = 1):
    from bpmis_jira_tool import source_code_qa_repo_downloads as b

    with zipfile.ZipFile(io.BytesIO(content)) as original:
        items = []
        expected = set()
        repo_by_root = {repo["archive_root"]: repo for repo in manifest["repos"]}
        for root, repo in repo_by_root.items():
            by_path = {}
            for line in original.read(repo["symbol_index"]).splitlines():
                symbol = json.loads(line)
                by_path.setdefault(symbol["path"], []).append(symbol)
            for line in original.read(repo["file_index"]).splitlines():
                row = json.loads(line)
                name = row["path"]
                if name in expected:
                    raise ToolError("Duplicate Git source path in export.")
                expected.add(name)
                symbols = by_path.get(name, [])
                info = original.getinfo(name)
                cost = info.compress_size + 100 + 2 * len(name.encode()) + len(zlib.compress((b._json_lines([row]) + b._json_lines(symbols)).encode()))
                items.append((root, row, symbols, cost))
        margin = min(512_000, b.MAX_BUNDLE_BYTES // 8)
        budget = b.MAX_BUNDLE_BYTES - margin - 2 * len(json.dumps(manifest).encode())
        if budget <= 0:
            raise ToolError("Source manifest cannot fit within the download limit.")
        # Whole files move into the next independent ZIP. No file is discarded.
        groups = [[]]
        used = 0
        for item in items:
            if item[3] > budget:
                raise ToolError("A complete source file cannot fit in one download part: " + item[1]["path"])
            if groups[-1] and used + item[3] > budget:
                groups.append([])
                used = 0
            groups[-1].append(item)
            used += item[3]
        # A part containing only documents still gets real source from another part.
        scope_code_files = sum(b._is_code_file(item[1]) for item in items)
        for group in groups:
            if not scope_code_files or any(b._is_code_file(item[1]) for item in group):
                continue
            choices = [(item[3], source, i) for source in groups if sum(b._is_code_file(item[1]) for item in source) > 1 for i, item in enumerate(source) if b._is_code_file(item[1])]
            if not choices:
                raise ToolError("Every source ZIP must contain source code.")
            _, source, i = min(choices, key=lambda choice: choice[0])
            group.append(source.pop(i))
        if len(groups) > 999 or not 1 <= requested_part <= len(groups):
            raise ToolError("Unknown repo download part.")
        filenames = [part_filename(metadata["filename"], n) for n in range(1, len(groups) + 1)]
        set_id = directory.name
        full_summary = {
            "id": set_id,
            "policy": FULL_SOURCE_POLICY,
            "part_count": len(groups),
            "filenames": filenames,
            "scope_repo_count": len(manifest["repos"]),
            "scope_source_files": len(expected),
            "scope_code_files": scope_code_files,
            "unavailable_submodules": [missing for repo in manifest["repos"] for missing in repo.get("full_source_audit", {}).get("unavailable_submodules", [])],
            "scope_af_reference_files": sum(bool(item[1].get("af_reference")) for item in items),
            "omitted_source_files": 0,
        }
        directory.mkdir(parents=True, exist_ok=True)
        written = set()
        summaries = []
        paths = []
        for number, group in enumerate(groups, 1):
            current = copy.deepcopy(manifest)
            current["contents"] = "All available Git-tracked text source, configuration, dependency source and documents, including pinned runtime submodules. Binary assets are excluded. Inaccessible submodules are listed explicitly."
            current["bundle_set"] = {**full_summary, "part_number": number}
            current["selection"] = {"priority": "All Git-tracked text source, configuration and documents, including pinned submodules. No source selection or omission.", "omitted_files": 0}
            current["repos"] = []
            current["part_manifest"] = f"manifests/part-{number:02d}.json"
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as output:
                for root, row, _, _ in group:
                    name = row["path"]
                    if name in written:
                        raise ToolError("Source file was assigned to more than one part.")
                    written.add(name)
                    output.writestr(copy.copy(original.getinfo(name)), original.read(name), compresslevel=9)
                for root, source_repo in repo_by_root.items():
                    selected = [item for item in group if item[0] == root]
                    if not selected:
                        continue
                    files = [item[1] for item in selected]
                    symbols = [symbol for item in selected for symbol in item[2]]
                    repo = copy.deepcopy(source_repo)
                    repo.update({"repository_source_files": source_repo["source_files"], "repository_code_files": source_repo["code_files"], "original_source_files": len(files), "original_code_files": sum(b._is_code_file(row) for row in files), "file_index": f"indexes/part-{number:02d}/{root}.files.jsonl", "symbol_index": f"indexes/part-{number:02d}/{root}.symbols.jsonl", "symbols": len(symbols)})
                    b._record_source_coverage(repo, files, allow_documents=True)
                    output.writestr(repo["file_index"], b._json_lines(files))
                    output.writestr(repo["symbol_index"], b._json_lines(symbols))
                    current["repos"].append(repo)
                readme = "Download ALL " + str(len(groups)) + " ZIP parts for this Market. Each ZIP is independent and below 20,000,000 bytes.\nExtract all parts into the same folder. Source paths and numbered indexes are unique across parts.\nEach numbered manifest is kept under manifests/. manifest.json is the current ZIP's manifest.\nAll available Git-tracked text source, configuration and documents are retained, including pinned runtime Git submodules. Binary assets are excluded. Any inaccessible external build submodule is listed in the manifest. No AF keyword filter removes files.\n\nFiles in this set:\n" + "\n".join(filenames) + "\n"
                output.writestr("READ-ME-FIRST.txt", readme)
                encoded = json.dumps(current, indent=2, sort_keys=True)
                output.writestr(current["part_manifest"], encoded)
                output.writestr("manifest.json", encoded)
            payload = buffer.getvalue()
            checked = b.validate_repo_download_archive(payload, expected_scope=manifest["scope_key"])
            if scope_code_files and not any(repo["code_files"] for repo in checked["repos"]):
                raise ToolError("A download part does not contain source code.")
            summary = {**metadata, **b.repo_download_validation_metadata(payload, checked), "filename": filenames[number - 1], "part_number": number, "part_count": len(groups), "set_id": set_id, "full_source": True, "unavailable_submodules": full_summary["unavailable_submodules"]}
            summary.pop("parts", None)
            target = directory / summary["filename"]
            temporary = target.with_name("." + target.name + f".{os.getpid()}.tmp")
            temporary.write_bytes(payload)
            temporary.chmod(0o600)
            os.replace(temporary, target)
            summaries.append(summary)
            paths.append(target)
        if written != expected:
            raise ToolError("Source coverage across download parts is incomplete.")
        for target, summary in zip(paths, summaries):
            target.with_suffix(".zip.json").write_text(json.dumps({**summary, "parts": summaries}, indent=2))
        result = {**summaries[requested_part - 1], "parts": summaries}
        return result, paths[requested_part - 1].read_bytes()


def cached_part_artifacts(service, metadata: dict):
    """Return validated cached parts for publication without rebuilding the Git export."""
    from bpmis_jira_tool import source_code_qa_repo_downloads as b

    parts = metadata.get("parts") or []
    directory = service.data_root / "repo_download_cache" / metadata["set_id"]
    for part in parts:
        path = directory / part["filename"]
        b.validate_repo_download_archive(path, expected_scope=metadata["scope_key"])
        yield {**part, "parts": parts}, path


def install_local_parts(service, metadata: dict) -> None:
    """Install secondary parts first; publish the primary manifest last."""
    import shutil

    artifacts = list(cached_part_artifacts(service, metadata))
    target_directory = service.data_root / "repo_downloads"
    target_directory.mkdir(parents=True, exist_ok=True)
    for part, source in sorted(artifacts, key=lambda item: item[0]["part_number"] == 1):
        for original in [source, source.with_suffix(".zip.json")]:
            target = target_directory / original.name
            temporary = target.with_name("." + target.name + f".{os.getpid()}.tmp")
            shutil.copyfile(original, temporary)
            temporary.chmod(0o600)
            os.replace(temporary, target)


def split_and_cache(content: bytes, manifest: dict, metadata: dict, directory: Path, requested_part: int = 1):
    import fcntl
    import hashlib
    from bpmis_jira_tool import source_code_qa_repo_downloads as b

    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".build.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        target = directory / part_filename(metadata["filename"], requested_part)
        try:
            cached = json.loads(target.with_suffix(".zip.json").read_text())
            payload = target.read_bytes()
            b.validate_repo_download_archive(payload, expected_scope=manifest["scope_key"])
            if cached["sha256"] == hashlib.sha256(payload).hexdigest():
                return cached, payload
        except (OSError, ValueError, KeyError, ToolError):
            pass
        return _split_and_cache_unlocked(content, manifest, metadata, directory, requested_part)
