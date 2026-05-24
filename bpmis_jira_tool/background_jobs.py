from __future__ import annotations

import threading
from typing import Any, Callable

from flask import Flask

from bpmis_jira_tool.job_lifecycle import JobLifecycle


def fail_job_if_active(
    job_store: Any,
    job_id: str,
    error: str,
    *,
    error_category: str = "unexpected_internal",
    error_code: str = "background_worker_unhandled_exception",
    error_retryable: bool = True,
) -> bool:
    return JobLifecycle(job_store).fail_if_active(
        job_id,
        error,
        error_category=error_category,
        error_code=error_code,
        error_retryable=error_retryable,
    )


def start_durable_job_thread(
    *,
    app: Flask,
    job_store: Any,
    job_id: str,
    target: Callable[..., Any],
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    name: str = "",
    error_prefix: str = "Background job failed unexpectedly",
    error_category: str = "unexpected_internal",
    error_code: str = "background_worker_unhandled_exception",
    error_retryable: bool = True,
) -> threading.Thread:
    def run() -> None:
        try:
            target(*args, **(kwargs or {}))
        except Exception as error:
            message = f"{error_prefix}: {error}"
            try:
                fail_job_if_active(
                    job_store,
                    job_id,
                    message,
                    error_category=error_category,
                    error_code=error_code,
                    error_retryable=error_retryable,
                )
            except Exception:  # pragma: no cover - preserve worker termination if persistence fails.
                pass
            try:
                app.logger.exception("%s.", error_prefix)
            except Exception:  # pragma: no cover - Flask logger should be available.
                pass

    thread = threading.Thread(target=run, name=name or None, daemon=True)
    thread.start()
    return thread
