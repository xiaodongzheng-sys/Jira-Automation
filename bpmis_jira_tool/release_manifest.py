from __future__ import annotations

from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
import os
import subprocess
import sys
from typing import Any

from bpmis_jira_tool.web_runtime_status import filtered_untracked_paths, source_tree_revision


def _run_git(project_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(project_root), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout


def _git_value(project_root: Path, *args: str) -> str:
    try:
        return _run_git(project_root, *args).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""


def _dirty_material(project_root: Path) -> str:
    try:
        diff_text = _run_git(project_root, "diff", "--no-ext-diff", "--full-index", "--binary", "HEAD", "--", ".")
        untracked = _run_git(project_root, "ls-files", "--others", "--exclude-standard")
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""
    untracked_paths = filtered_untracked_paths(untracked)
    if untracked_paths:
        return diff_text + "\n--UNTRACKED--\n" + "\n".join(untracked_paths) + "\n"
    return diff_text


def _file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_release_manifest(
    project_root: Path,
    *,
    surface: str = "mac_public_live",
    host_root: Path | None = None,
    python_executable: str = "",
) -> dict[str, Any]:
    project_root = project_root.resolve()
    dirty_material = _dirty_material(project_root)
    tracked_revision = _git_value(project_root, "rev-parse", "HEAD") or "unknown"
    untracked = filtered_untracked_paths(_git_value(project_root, "ls-files", "--others", "--exclude-standard"))
    changed = [
        line.strip()
        for line in (_git_value(project_root, "diff", "--name-only", "HEAD", "--", ".") or "").splitlines()
        if line.strip()
    ]
    release_revision = source_tree_revision(project_root)
    material_hash = hashlib.sha256(dirty_material.encode("utf-8")).hexdigest() if dirty_material else ""
    payload: dict[str, Any] = {
        "schema_version": 1,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "surface": str(surface or "mac_public_live"),
        "project_root": str(project_root),
        "host_root": str((host_root or project_root).resolve()),
        "release_revision": release_revision,
        "git_head": tracked_revision,
        "dirty": bool(dirty_material.strip()),
        "dirty_material_sha256": material_hash,
        "changed_files": changed,
        "untracked_files": untracked,
        "python_executable": python_executable or sys.executable,
        "python_version": sys.version.split()[0],
    }
    manifest_material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["manifest_id"] = hashlib.sha256(manifest_material.encode("utf-8")).hexdigest()[:16]
    return payload


def write_release_manifest(path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp_path, path)
    return manifest


def load_release_manifest(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def manifest_file_sha256(path: Path) -> str:
    try:
        return _file_sha256(path)
    except OSError:
        return ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a Team Portal release manifest.")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]), help="Project checkout root.")
    parser.add_argument("--output", required=True, help="Path to write the manifest JSON.")
    parser.add_argument("--surface", default="mac_public_live", help="Release surface name.")
    parser.add_argument("--host-root", default="", help="Host checkout root recorded in the manifest.")
    parser.add_argument("--print-id", action="store_true", help="Print the generated manifest id.")
    args = parser.parse_args(argv)

    project_root = Path(args.root)
    host_root = Path(args.host_root) if args.host_root else project_root
    manifest = build_release_manifest(project_root, surface=args.surface, host_root=host_root)
    write_release_manifest(Path(args.output), manifest)
    if args.print_id:
        print(manifest.get("manifest_id") or "")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
