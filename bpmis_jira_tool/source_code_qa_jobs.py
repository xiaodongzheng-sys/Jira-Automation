from __future__ import annotations

from collections import deque
import threading
from typing import Any, Callable, Optional

from flask import Flask

from bpmis_jira_tool.background_jobs import fail_job_if_active
from bpmis_jira_tool.job_store import JobStore


QueryJobRunner = Callable[[Flask, str, dict[str, Any]], None]
QueryModeNormalizer = Callable[[Optional[str]], str]


def _default_query_mode(_query_mode: str | None) -> str:
    return "deep"


def _missing_default_runner(_app: Flask, _job_id: str, _payload: dict[str, Any]) -> None:
    raise RuntimeError("SourceCodeQAQueryScheduler requires a default runner or per-job runner.")


class SourceCodeQAQueryScheduler:
    def __init__(
        self,
        *,
        job_store: JobStore,
        max_running: int = 2,
        query_mode_normalizer: QueryModeNormalizer | None = None,
        default_runner: QueryJobRunner | None = None,
    ) -> None:
        self.job_store = job_store
        self.max_running = max(1, int(max_running or 2))
        self.query_mode_normalizer = query_mode_normalizer or _default_query_mode
        self.default_runner = default_runner or _missing_default_runner
        self._lock = threading.Lock()
        self._user_queues: dict[str, deque[tuple[str, Flask, dict[str, Any], QueryJobRunner | None]]] = {}
        self._user_order: deque[str] = deque()
        self._running: set[str] = set()
        self._running_users: dict[str, int] = {}

    def submit(
        self,
        *,
        app: Flask,
        job_id: str,
        payload: dict[str, Any],
        owner_email: str,
        runner: QueryJobRunner | None = None,
    ) -> None:
        user_key = str(owner_email or "local").strip().lower() or "local"
        with self._lock:
            if user_key not in self._user_queues:
                self._user_queues[user_key] = deque()
                self._user_order.append(user_key)
            self._user_queues[user_key].append((job_id, app, dict(payload), runner))
            self.job_store.set_owner(job_id, user_key)
            self.job_store.set_query_mode(job_id, self.query_mode_normalizer(payload.get("query_mode")))
            self._refresh_queue_metadata_locked()
            self._start_available_locked()

    def finish(self, job_id: str, owner_email: str) -> None:
        user_key = str(owner_email or "local").strip().lower() or "local"
        with self._lock:
            self._running.discard(job_id)
            if user_key in self._running_users:
                self._running_users[user_key] = max(0, self._running_users[user_key] - 1)
                if self._running_users[user_key] <= 0:
                    self._running_users.pop(user_key, None)
            self._refresh_queue_metadata_locked()
            self._start_available_locked()

    def _start_available_locked(self) -> None:
        while len(self._running) < self.max_running:
            next_item = self._pop_next_locked()
            if next_item is None:
                break
            user_key, job_id, app, payload, runner = next_item
            self._running.add(job_id)
            self._running_users[user_key] = self._running_users.get(user_key, 0) + 1
            self.job_store.update_queue_metadata(
                job_id,
                queued_position=0,
                eta_seconds_range=[],
                running_user_count=len(self._running_users),
                message="Starting Source Code Q&A job.",
            )
            thread = threading.Thread(
                target=self._run_job,
                args=(app, job_id, payload, user_key, runner),
                daemon=True,
            )
            thread.start()
        self._refresh_queue_metadata_locked()

    def _pop_next_locked(self) -> tuple[str, str, Flask, dict[str, Any], QueryJobRunner | None] | None:
        self._user_order = deque(user_key for user_key in self._user_order if self._user_queues.get(user_key))
        if not self._user_order:
            return None
        ordered = list(self._user_order)
        selected_user = min(ordered, key=lambda user_key: (self._running_users.get(user_key, 0), ordered.index(user_key)))
        self._user_order.remove(selected_user)
        queue = self._user_queues.get(selected_user)
        if not queue:  # pragma: no cover - filtered above while holding the same lock.
            self._user_queues.pop(selected_user, None)
            return None
        job_id, app, payload, runner = queue.popleft()
        if queue:
            self._user_order.append(selected_user)
        else:
            self._user_queues.pop(selected_user, None)
        return selected_user, job_id, app, payload, runner

    def _refresh_queue_metadata_locked(self) -> None:
        ordered_jobs = self._simulated_round_robin_locked()
        p95_seconds = self.job_store.p95_duration_seconds("source-code-qa-query", default_seconds=212)
        for index, (job_id, _user_key) in enumerate(ordered_jobs, start=1):
            waves_ahead = max(0, (index - 1) // self.max_running)
            lower = waves_ahead * p95_seconds
            upper = max(lower + 60, (waves_ahead + 1) * p95_seconds)
            self.job_store.update_queue_metadata(
                job_id,
                queued_position=index,
                eta_seconds_range=[lower, upper],
                running_user_count=len(self._running_users),
                message=f"Queued behind {max(0, index - 1)} Source Code Q&A job(s).",
            )

    def _simulated_round_robin_locked(self) -> list[tuple[str, str]]:
        queues = {
            user_key: deque((job_id, user_key) for job_id, _app, _payload, _runner in queue)
            for user_key, queue in self._user_queues.items()
            if queue
        }
        order = deque(user_key for user_key in self._user_order if user_key in queues)
        result: list[tuple[str, str]] = []
        while order:
            user_key = order.popleft()
            queue = queues.get(user_key)
            if not queue:  # pragma: no cover - local queue map only stores non-empty queues.
                continue
            result.append(queue.popleft())
            if queue:
                order.append(user_key)
        return result

    def _run_job(
        self,
        app: Flask,
        job_id: str,
        payload: dict[str, Any],
        owner_email: str,
        runner: QueryJobRunner | None = None,
    ) -> None:
        try:
            (runner or self.default_runner)(app, job_id, payload)
        except Exception as error:
            try:
                fail_job_if_active(
                    self.job_store,
                    job_id,
                    f"Source Code Q&A worker failed unexpectedly: {error}",
                    error_category="unexpected_internal",
                    error_code="background_worker_unhandled_exception",
                    error_retryable=True,
                )
            except Exception:  # pragma: no cover - preserve scheduler cleanup even if persistence fails.
                pass
            try:
                app.logger.exception("Source Code Q&A scheduler worker failed unexpectedly.")
            except Exception:  # pragma: no cover - Flask logger should be available.
                pass
        finally:
            self.finish(job_id, owner_email)
