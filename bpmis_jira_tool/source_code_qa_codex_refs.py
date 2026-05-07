from __future__ import annotations

from pathlib import Path
import re
from typing import Any


def codex_repo_relative_root(repo_root: Path, repo_parent: Path) -> str:
    try:
        return str(Path(repo_root).resolve().relative_to(Path(repo_parent).resolve()))
    except ValueError:
        return ""


def extract_direct_file_refs(text: str) -> list[str]:
    refs = []
    for match in re.finditer(r"([A-Za-z0-9_./$@-]+\.[A-Za-z0-9_]+:\d+(?:-\d+)?)", str(text or "")):
        refs.append(match.group(1))
    return refs


def resolve_codex_file_ref(
    raw_ref: str,
    candidate_paths: list[dict[str, Any]],
    *,
    repo_root: Path,
    scope_roots: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    ref = str(raw_ref or "").strip().strip("[]`'\"")
    match = re.match(r"^(?:(?P<repo>[^:]+):)?(?P<path>.+):(?P<start>\d+)(?:-(?P<end>\d+))?$", ref)
    if not match:
        return {"status": "invalid", "reason": "missing file line range", "ref": ref}
    path = match.group("path").strip()
    start = int(match.group("start"))
    end = int(match.group("end") or start)
    if start <= 0 or end < start:
        return {"status": "invalid", "reason": "invalid line range", "ref": ref}
    if ".." in Path(path).parts:
        return {"status": "invalid", "reason": "unsafe path", "ref": ref}
    repo_hint = str(match.group("repo") or "").strip()
    root_items: list[dict[str, str]] = []
    if scope_roots:
        root_items = [dict(item) for item in scope_roots if isinstance(item, dict)]
    else:
        seen_roots: set[str] = set()
        for item in candidate_paths:
            root = str(item.get("repo_root") or "").strip()
            if not root or root in seen_roots:
                continue
            seen_roots.add(root)
            root_items.append(
                {
                    "repo": str(item.get("repo") or ""),
                    "repo_root": root,
                    "repo_relative_root": str(item.get("repo_relative_root") or ""),
                }
            )
    normalized_roots: list[dict[str, Any]] = []
    repo_parent = Path(repo_root).resolve()
    for item in root_items:
        root = str(item.get("repo_root") or "").strip()
        if not root:
            continue
        root_path = Path(root).resolve()
        relative_root = str(item.get("repo_relative_root") or codex_repo_relative_root(root_path, repo_parent)).strip()
        repo = str(item.get("repo") or root_path.name).strip()
        hint_values = {repo, root_path.name, relative_root, relative_root.replace("/", ":")}
        if repo_hint and repo_hint not in hint_values:
            continue
        normalized_roots.append(
            {
                "repo": repo,
                "root": root_path,
                "repo_relative_root": relative_root,
            }
        )
    if not normalized_roots:
        return {"status": "invalid", "reason": "no scoped repo root matched", "ref": ref}

    requested_path = Path(path).expanduser()
    if requested_path.is_absolute():
        resolved_requested = requested_path.resolve()
        for item in normalized_roots:
            root = item["root"]
            try:
                relative_path = resolved_requested.relative_to(root)
            except ValueError:
                continue
            return codex_resolved_file_ref_payload(
                ref=ref,
                candidate=resolved_requested,
                relative_path=relative_path,
                root=root,
                repo=str(item["repo"]),
                repo_relative_root=str(item["repo_relative_root"]),
                start=start,
                end=end,
            )
        return {"status": "out_of_scope", "reason": "absolute path outside selected scope", "ref": ref, "path": path}

    known_relative_roots = {str(item["repo_relative_root"]) for item in normalized_roots if item.get("repo_relative_root")}
    for relative_root in known_relative_roots:
        prefix = f"{relative_root.rstrip('/')}/"
        if path == relative_root or path.startswith(prefix):
            trimmed_path = path[len(prefix):] if path.startswith(prefix) else ""
            for item in normalized_roots:
                if str(item.get("repo_relative_root") or "") != relative_root:
                    continue
                candidate = (item["root"] / trimmed_path).resolve()
                return codex_resolved_file_ref_payload(
                    ref=ref,
                    candidate=candidate,
                    relative_path=Path(trimmed_path),
                    root=item["root"],
                    repo=str(item["repo"]),
                    repo_relative_root=str(item["repo_relative_root"]),
                    start=start,
                    end=end,
                )
    candidate_from_parent = (repo_parent / path).resolve()
    for item in normalized_roots:
        try:
            relative_path = candidate_from_parent.relative_to(item["root"])
        except ValueError:
            continue
        return codex_resolved_file_ref_payload(
            ref=ref,
            candidate=candidate_from_parent,
            relative_path=relative_path,
            root=item["root"],
            repo=str(item["repo"]),
            repo_relative_root=str(item["repo_relative_root"]),
            start=start,
            end=end,
        )
    if candidate_from_parent.exists():
        return {"status": "out_of_scope", "reason": "relative repo path outside selected scope", "ref": ref, "path": path}

    for item in normalized_roots:
        root = item["root"]
        candidate = (root / path).resolve()
        try:
            relative_path = candidate.relative_to(root)
        except ValueError:
            continue
        payload = codex_resolved_file_ref_payload(
            ref=ref,
            candidate=candidate,
            relative_path=relative_path,
            root=root,
            repo=str(item["repo"]),
            repo_relative_root=str(item["repo_relative_root"]),
            start=start,
            end=end,
        )
        if payload.get("status") == "ok":
            return payload
    return {"status": "invalid", "reason": "file or line range not found", "ref": ref}


def codex_resolved_file_ref_payload(
    *,
    ref: str,
    candidate: Path,
    relative_path: Path,
    root: Path,
    repo: str,
    repo_relative_root: str,
    start: int,
    end: int,
) -> dict[str, Any]:
    try:
        candidate.relative_to(root)
    except ValueError:
        return {"status": "out_of_scope", "reason": "resolved path outside selected scope", "ref": ref, "path": str(candidate)}
    if not candidate.exists() or not candidate.is_file():
        return {"status": "invalid", "reason": "file not found", "ref": ref}
    try:
        line_count = len(candidate.read_text(encoding="utf-8", errors="ignore").splitlines())
    except OSError as error:
        return {"status": "invalid", "reason": f"file unreadable: {error}", "ref": ref}
    if end > line_count:
        return {"status": "invalid", "reason": "line range not found", "ref": ref}
    return {
        "status": "ok",
        "ref": ref,
        "repo": repo,
        "repo_relative_root": repo_relative_root,
        "path": str(relative_path),
        "absolute_path": str(candidate),
        "line_start": start,
        "line_end": end,
    }
