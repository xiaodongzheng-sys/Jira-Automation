from __future__ import annotations

from typing import Any


TERMINAL_JOB_STATES = {"completed", "failed"}


class JobLifecycle:
    def __init__(self, job_store: Any) -> None:
        self.job_store = job_store

    def snapshot(self, job_id: str) -> dict[str, Any] | None:
        try:
            snapshot = self.job_store.snapshot(job_id) if hasattr(self.job_store, "snapshot") else None
        except Exception:
            return None
        return snapshot if isinstance(snapshot, dict) else None

    def is_terminal(self, job_id: str) -> bool:
        snapshot = self.snapshot(job_id)
        return bool(snapshot and str(snapshot.get("state") or "") in TERMINAL_JOB_STATES)

    def fail_if_active(
        self,
        job_id: str,
        error: str,
        *,
        error_category: str = "unexpected_internal",
        error_code: str = "background_worker_unhandled_exception",
        error_retryable: bool = True,
    ) -> bool:
        if self.is_terminal(job_id):
            return False
        self.job_store.fail(
            job_id,
            error,
            error_category=error_category,
            error_code=error_code,
            error_retryable=error_retryable,
        )
        return True

    def start(
        self,
        job_id: str,
        *,
        stage: str = "running",
        message: str = "Running.",
        current: int = 0,
        total: int = 0,
    ) -> None:
        self.job_store.update(job_id, state="running", stage=stage, message=message, current=current, total=total)

    def complete(self, job_id: str, *, results: list[dict[str, Any]], notice: dict[str, Any]) -> None:
        self.job_store.complete(job_id, results=results, notice=notice)
