from __future__ import annotations

from functools import lru_cache
import hashlib
from pathlib import Path
import os
import subprocess
from typing import Any


DEFAULT_FLASK_SESSION_SECRET_VALUES = {"", "dev-secret-key", "local-dev-secret-change-me"}
UNTRACKED_RELEASE_REVISION_EXCLUDES = (".venv", ".venv-", ".venv.", ".team-portal", ".pytest_cache", ".claude")


@lru_cache(maxsize=1)
def current_release_revision(project_root: Path) -> str:
    pinned_revision = str(os.environ.get("TEAM_PORTAL_RELEASE_REVISION") or "").strip()
    if pinned_revision:
        return pinned_revision
    return source_tree_revision(project_root)


def source_tree_revision(project_root: Path) -> str:
    def run_git(*args: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(project_root), *args],
            capture_output=True,
            text=True,
            check=True,
        )
        return completed.stdout

    try:
        head = run_git("rev-parse", "HEAD").strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return "unknown"
    try:
        diff_text = run_git("diff", "--no-ext-diff", "--full-index", "--binary", "HEAD", "--", ".")
        untracked = run_git("ls-files", "--others", "--exclude-standard")
    except subprocess.CalledProcessError:
        return head or "unknown"
    dirty_material = diff_text
    untracked_paths = filtered_untracked_paths(untracked)
    if untracked_paths:
        dirty_material += "\n--UNTRACKED--\n" + "\n".join(untracked_paths) + "\n"
    if dirty_material.strip():
        fingerprint = hashlib.sha1(dirty_material.encode("utf-8")).hexdigest()[:12]
        return f"{head}-dirty-{fingerprint}"
    return head or "unknown"


def filtered_untracked_paths(untracked_output: str) -> list[str]:
    paths: list[str] = []
    for raw_line in str(untracked_output or "").splitlines():
        path = raw_line.strip()
        if not path:
            continue
        first_part = path.split("/", 1)[0]
        if any(first_part.startswith(prefix) for prefix in UNTRACKED_RELEASE_REVISION_EXCLUDES):
            continue
        paths.append(path)
    return paths


def default_flask_session_secret(value: Any) -> bool:
    return str(value or "").strip() in DEFAULT_FLASK_SESSION_SECRET_VALUES
