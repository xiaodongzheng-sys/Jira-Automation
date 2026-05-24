from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json


@dataclass(frozen=True)
class JobStoreSpec:
    key: str
    path: Path
    owner: str


def job_store_specs(data_root: Path) -> list[JobStoreSpec]:
    return [
        JobStoreSpec("portal", data_root / "run" / "jobs.json", "portal"),
        JobStoreSpec("team_dashboard", data_root / "run" / "team_dashboard_jobs.json", "team_dashboard"),
        JobStoreSpec("meeting_recorder", data_root / "run" / "meeting_recorder_jobs.json", "meeting_recorder"),
        JobStoreSpec("source_code_qa_sync", data_root / "source_code_qa" / "sync_jobs.json", "source_code_qa"),
    ]


def load_job_store_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def extract_job_snapshots(payload: Any, *, store_key: str, store_path: Path, owner: str = "") -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw_jobs = payload.get("jobs", payload)
    if isinstance(raw_jobs, dict):
        iterable = raw_jobs.values()
    elif isinstance(raw_jobs, list):
        iterable = raw_jobs
    else:
        return []
    jobs: list[dict[str, Any]] = []
    for item in iterable:
        if isinstance(item, dict):
            row = dict(item)
            row["_store"] = store_path.name
            row["_store_key"] = store_key
            row["_store_owner"] = owner
            jobs.append(row)
    return jobs


def load_all_job_snapshots(data_root: Path) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for spec in job_store_specs(data_root):
        payload = load_job_store_payload(spec.path)
        jobs.extend(extract_job_snapshots(payload, store_key=spec.key, store_path=spec.path, owner=spec.owner))
    return jobs
