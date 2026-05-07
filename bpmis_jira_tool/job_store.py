from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import os
from pathlib import Path
import threading
import time
from typing import Any
import uuid


@dataclass
class JobState:
    job_id: str
    action: str
    state: str = "queued"
    title: str = ""
    message: str = ""
    stage: str = "queued"
    current: int = 0
    total: int = 0
    estimated_prompt_tokens: int = 0
    token_risk: str = ""
    error_category: str = ""
    error_code: str = ""
    error_retryable: bool = False
    owner_email: str = ""
    record_id: str = ""
    query_mode: str = ""
    queued_position: int = 0
    eta_seconds_range: list[int] = field(default_factory=list)
    running_user_count: int = 0
    last_progress_at: float = 0
    stalled_retryable: bool = False
    started_at: float = 0
    completed_at: float = 0
    results: list[dict[str, Any]] = field(default_factory=list)
    notice: dict[str, Any] | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class JobStore:
    def __init__(self, storage_path: Path | None = None) -> None:
        self.storage_path = storage_path
        self._loaded_jobs_interrupted = False
        self._jobs: dict[str, JobState] = self._load(mark_interrupted=True)
        self._lock = threading.Lock()
        if self._loaded_jobs_interrupted:
            with self._lock:
                self._persist_locked()

    def _load(self, *, mark_interrupted: bool = False) -> dict[str, JobState]:
        if self.storage_path is None or not self.storage_path.exists():
            return {}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        jobs: dict[str, JobState] = {}
        for job_id, raw_job in (payload.get("jobs") or {}).items():
            if not isinstance(raw_job, dict):
                continue
            try:
                job = JobState(**raw_job)
            except TypeError:
                continue
            if mark_interrupted and job.state in {"queued", "running"}:
                job.state = "failed"
                job.stage = "failed"
                job.message = "This job was interrupted by a server restart. Please start it again."
                job.error = job.message
                job.updated_at = time.time()
                self._loaded_jobs_interrupted = True
            jobs[str(job_id)] = job
        return jobs

    def _refresh_locked(self) -> None:
        if self.storage_path is None:
            return
        loaded_jobs = self._load(mark_interrupted=False)
        for job_id, loaded_job in loaded_jobs.items():
            current_job = self._jobs.get(job_id)
            if current_job is None or loaded_job.updated_at >= current_job.updated_at:
                self._jobs[job_id] = loaded_job

    def _persist_locked(self) -> None:
        if self.storage_path is None:
            return
        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": time.time(),
                "jobs": {job_id: asdict(job) for job_id, job in self._jobs.items()},
            }
            temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.storage_path)
        except OSError:
            return

    def create(self, action: str, title: str, *, owner_email: str = "", record_id: str = "") -> JobState:
        with self._lock:
            job = JobState(
                job_id=uuid.uuid4().hex,
                action=action,
                title=title,
                message="Queued and waiting to start.",
                owner_email=str(owner_email or "").strip().lower(),
                record_id=str(record_id or "").strip(),
            )
            self._jobs[job.job_id] = job
            self._persist_locked()
            return job

    def set_owner(self, job_id: str, owner_email: str) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.owner_email = str(owner_email or "").strip().lower()
            job.updated_at = time.time()
            self._persist_locked()

    def set_query_mode(self, job_id: str, query_mode: str) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.query_mode = str(query_mode or "").strip().lower()
            job.updated_at = time.time()
            self._persist_locked()

    def set_record_id(self, job_id: str, record_id: str) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.record_id = str(record_id or "").strip()
            job.updated_at = time.time()
            self._persist_locked()

    def update_queue_metadata(
        self,
        job_id: str,
        *,
        queued_position: int = 0,
        eta_seconds_range: list[int] | None = None,
        running_user_count: int = 0,
        message: str | None = None,
    ) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.queued_position = max(0, int(queued_position or 0))
            job.eta_seconds_range = [max(0, int(value or 0)) for value in (eta_seconds_range or [])[:2]]
            job.running_user_count = max(0, int(running_user_count or 0))
            if message is not None:
                job.message = message
            job.updated_at = time.time()
            self._persist_locked()

    def update(
        self,
        job_id: str,
        *,
        state: str | None = None,
        stage: str | None = None,
        message: str | None = None,
        current: int | None = None,
        total: int | None = None,
        estimated_prompt_tokens: int | None = None,
        token_risk: str | None = None,
    ) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            if state is not None:
                if state == "running" and job.state != "running":
                    job.started_at = job.started_at or time.time()
                    job.queued_position = 0
                    job.eta_seconds_range = []
                job.state = state
            if stage is not None:
                job.stage = stage
            if message is not None:
                job.message = message
            if current is not None:
                job.current = current
            if total is not None:
                job.total = total
            if estimated_prompt_tokens is not None:
                job.estimated_prompt_tokens = estimated_prompt_tokens
            if token_risk is not None:
                job.token_risk = token_risk
            job.last_progress_at = time.time()
            job.stalled_retryable = False
            job.updated_at = time.time()
            self._persist_locked()

    def complete(self, job_id: str, *, results: list[dict[str, Any]], notice: dict[str, Any]) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.state = "completed"
            job.stage = "completed"
            job.message = "Finished."
            job.results = results
            job.notice = notice
            job.completed_at = time.time()
            job.queued_position = 0
            job.eta_seconds_range = []
            job.last_progress_at = job.completed_at
            job.stalled_retryable = False
            job.updated_at = time.time()
            self._persist_locked()

    def fail(
        self,
        job_id: str,
        error: str,
        *,
        error_category: str = "",
        error_code: str = "",
        error_retryable: bool = True,
    ) -> None:
        with self._lock:
            if job_id not in self._jobs:
                self._refresh_locked()
            job = self._jobs[job_id]
            job.state = "failed"
            job.stage = "failed"
            job.message = error
            job.error = error
            job.error_category = error_category
            job.error_code = error_code
            job.error_retryable = error_retryable
            job.completed_at = time.time()
            job.queued_position = 0
            job.eta_seconds_range = []
            job.last_progress_at = job.completed_at
            job.stalled_retryable = False
            job.updated_at = time.time()
            self._persist_locked()

    def get(self, job_id: str) -> JobState | None:
        with self._lock:
            self._refresh_locked()
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return JobState(**asdict(job))

    def snapshot(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if not job:
            return None
        payload = asdict(job)
        if payload.get("state") == "running":
            last_progress_at = float(payload.get("last_progress_at") or payload.get("updated_at") or 0)
            stalled = bool(last_progress_at and time.time() - last_progress_at > 180)
            payload["stalled_retryable"] = stalled
            if stalled:
                payload["error_retryable"] = True
        payload["progress"] = {
            "stage": job.stage,
            "current": job.current,
            "total": job.total,
            "message": job.message,
            "estimated_prompt_tokens": job.estimated_prompt_tokens,
            "token_risk": job.token_risk,
        }
        return payload

    def p95_duration_seconds(self, action: str, *, default_seconds: int = 212) -> int:
        with self._lock:
            self._refresh_locked()
            durations = []
            for job in self._jobs.values():
                if job.action != action or job.state != "completed":
                    continue
                started_at = float(job.started_at or job.created_at or 0)
                completed_at = float(job.completed_at or job.updated_at or 0)
                if started_at and completed_at and completed_at >= started_at:
                    durations.append(completed_at - started_at)
            if not durations:
                return default_seconds
            durations.sort()
            index = min(len(durations) - 1, int(round(0.95 * (len(durations) - 1))))
            return max(30, int(durations[index]))

    def latest_completed_result(self, action: str) -> dict[str, Any] | None:
        with self._lock:
            self._refresh_locked()
            candidates = [
                job
                for job in self._jobs.values()
                if job.action == action and job.state == "completed" and job.results
            ]
            if not candidates:
                return None
            latest = max(candidates, key=lambda item: item.updated_at)
            result = latest.results[0] if latest.results else {}
            if not isinstance(result, dict):
                return None
            return {
                **result,
                "job_id": latest.job_id,
                "generated_at": latest.updated_at,
            }

    def list_snapshots(self, *, action: str = "", owner_email: str = "", limit: int = 20) -> list[dict[str, Any]]:
        normalized_action = str(action or "").strip()
        normalized_owner = str(owner_email or "").strip().lower()
        with self._lock:
            self._refresh_locked()
            jobs = [
                job
                for job in self._jobs.values()
                if (not normalized_action or job.action == normalized_action)
                and (not normalized_owner or str(job.owner_email or "").strip().lower() == normalized_owner)
            ]
            jobs.sort(key=lambda item: item.updated_at, reverse=True)
            job_ids = [job.job_id for job in jobs[: max(1, min(int(limit or 20), 100))]]
        return [snapshot for job_id in job_ids if (snapshot := self.snapshot(job_id)) is not None]

    def active_for_record(self, action: str, *, owner_email: str, record_id: str) -> dict[str, Any] | None:
        normalized_owner = str(owner_email or "").strip().lower()
        normalized_record_id = str(record_id or "").strip()
        with self._lock:
            self._refresh_locked()
            candidates = [
                job
                for job in self._jobs.values()
                if job.action == action
                and job.state in {"queued", "running"}
                and str(job.owner_email or "").strip().lower() == normalized_owner
                and str(job.record_id or "").strip() == normalized_record_id
            ]
            if not candidates:
                return None
            latest = max(candidates, key=lambda item: item.updated_at)
            return asdict(latest)

