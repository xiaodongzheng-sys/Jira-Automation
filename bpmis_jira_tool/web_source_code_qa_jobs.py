"""Source Code QA sync and query background job helpers."""
from __future__ import annotations


def _bind_web_globals(functions: list[object], global_context: dict[str, object]) -> None:
    for function in functions:
        target = getattr(function, "__wrapped__", function)
        globals_dict = getattr(target, "__globals__", None)
        if globals_dict is not None:
            globals_dict.update(global_context)


def _run_source_code_qa_sync_job(
    app: Flask,
    job_id: str,
    settings: Settings,
    pm_team: str,
    country: str,
) -> None:
    del settings
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]
        job_store.update(
            job_id,
            state="running",
            stage="syncing",
            message="Syncing repositories and rebuilding the source-code index.",
            current=0,
            total=1,
        )
        try:
            result = _build_source_code_qa_service().sync(pm_team=pm_team, country=country)
            status = str(result.get("status") or "ok")
            summary = "Source repositories are synced." if status == "ok" else "Source repository sync completed with issues."
            job_store.complete(
                job_id,
                results=[result],
                notice={
                    "title": "Source Code Sync",
                    "tone": "success" if status == "ok" else "warning",
                    "summary": summary,
                    "details": [f"Status: {status}", f"Repositories: {len(result.get('results') or [])}"],
                },
            )
        except ToolError as error:
            job_store.fail(job_id, str(error))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            app.logger.exception("Source code QA sync job failed unexpectedly.")
            job_store.fail(job_id, f"Unexpected error: {error}")


def _run_source_code_qa_query_job(app: Flask, job_id: str, payload: dict[str, Any]) -> None:
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]

        def progress_callback(stage: str, message: str, current: int, total: int) -> None:
            job_store.update(
                job_id,
                state="running",
                stage=stage,
                message=message,
                current=current,
                total=total,
            )

        try:
            if not _source_code_qa_provider_available(payload.get("llm_provider")):
                raise ToolError("Selected Source Code Q&A model is unavailable.")
            service = _build_source_code_qa_service(payload.get("llm_provider"))
            pm_team = str(payload.get("pm_team") or "")
            country = str(payload.get("country") or "")
            query_mode = _source_code_qa_query_mode(payload.get("query_mode"))
            session_store = _get_source_code_qa_session_store()
            session_id = str(payload.get("session_id") or "").strip()
            owner_email = str(payload.get("_session_owner_email") or "").strip().lower() or "local"
            conversation_context = payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None
            if conversation_context is None and session_id:
                conversation_context = session_store.get_context(session_id, owner_email=owner_email)
            if isinstance(payload.get("_resolved_attachments"), list):
                attachments = payload.get("_resolved_attachments") or []
            else:
                attachments = _resolve_source_code_qa_query_attachments(payload, owner_email=owner_email, session_id=session_id)
            runtime_evidence = _resolve_source_code_qa_runtime_evidence(pm_team=pm_team, country=country)
            auto_sync = _prepare_source_code_qa_auto_sync(
                service,
                pm_team=pm_team,
                country=country,
                progress_callback=progress_callback,
            )
            def run_query() -> dict[str, Any]:
                return service.query(
                    pm_team=pm_team,
                    country=country,
                    question=str(payload.get("question") or ""),
                    answer_mode=_source_code_qa_public_answer_mode(payload.get("answer_mode")),
                    llm_budget_mode="auto",
                    query_mode=query_mode,
                    conversation_context=conversation_context,
                    attachments=attachments,
                    runtime_evidence=runtime_evidence,
                    progress_callback=progress_callback,
                )

            if service.llm_provider_name == "codex_cli_bridge" and session_id:
                progress_callback("codex_session_lock", "Waiting for this chat's Codex session slot.", 0, 1)
                with _source_code_qa_codex_session_lock(session_id):
                    result = run_query()
            else:
                result = run_query()
            result["auto_sync"] = auto_sync
            result["attachments"] = _source_code_qa_public_attachments(attachments)
            result["runtime_evidence"] = _source_code_qa_public_runtime_evidence(runtime_evidence)
            if session_id:
                result["generated_artifacts"] = _build_source_code_qa_generated_artifacts(
                    owner_email=owner_email,
                    session_id=session_id,
                    pm_team=pm_team,
                    country=country,
                    question=str(payload.get("question") or ""),
                    result=result,
                    runtime_evidence=runtime_evidence,
                )
                session_write_started = time.perf_counter()
                session_payload = session_store.append_exchange(
                    session_id,
                    owner_email=owner_email,
                    pm_team=pm_team,
                    country=country,
                    llm_provider=str(payload.get("llm_provider") or ""),
                    question=str(payload.get("question") or ""),
                    result=result,
                    context=_build_source_code_qa_session_context(result, payload),
                    attachments=attachments,
                )
                current_app.logger.warning(
                    "source_code_qa_timing %s",
                    json.dumps(
                        {
                            "event": "source_code_qa_timing",
                            "component": "session_write",
                            "elapsed_ms": int((time.perf_counter() - session_write_started) * 1000),
                            "job_id": job_id,
                            "trace_id": str(result.get("trace_id") or ""),
                            "session_id": session_id,
                            "owner_email": owner_email,
                            "message_count": len(session_payload.get("messages") or []) if isinstance(session_payload, dict) else 0,
                            "status": "ok" if session_payload is not None else "missing_session",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                if session_payload is not None:
                    result["session"] = session_payload
                    result["session_id"] = session_id
            status = str(result.get("status") or "ok")
            started_at = float((job_store.snapshot(job_id) or {}).get("started_at") or time.time())
            elapsed_seconds = max(0.0, time.time() - started_at)
            if elapsed_seconds > 120:
                attribution = result.get("slow_query_attribution") if isinstance(result.get("slow_query_attribution"), dict) else {}
                current_app.logger.warning(
                    "source_code_qa_slow_query job_id=%s owner=%s elapsed_seconds=%.1f stage=%s attribution=%s",
                    job_id,
                    owner_email,
                    elapsed_seconds,
                    str((job_store.snapshot(job_id) or {}).get("stage") or ""),
                    json.dumps(attribution, ensure_ascii=False, sort_keys=True),
                )
            _record_source_code_qa_work_memory(
                owner_email=owner_email,
                pm_team=pm_team,
                country=country,
                question=str(payload.get("question") or ""),
                result=result,
                session_id=session_id,
                job_id=job_id,
            )
            job_store.complete(
                job_id,
                results=[result],
                notice={
                    "title": "Source Code Q&A",
                    "tone": "success" if status == "ok" else "warning",
                    "summary": result.get("summary") or "Source Code Q&A completed.",
                    "details": [
                        f"Status: {status}",
                        f"Trace: {result.get('trace_id') or 'n/a'}",
                        *(
                            [
                                "Slowest: "
                                + str((result.get("slow_query_attribution") or {}).get("slow_component") or "unknown")
                                + " "
                                + str((result.get("slow_query_attribution") or {}).get("slow_component_ms") or 0)
                                + "ms"
                            ]
                            if isinstance(result.get("slow_query_attribution"), dict)
                            and (result.get("slow_query_attribution") or {}).get("status") == "slow"
                            else []
                        ),
                    ],
                },
            )
        except ToolError as error:
            job_store.fail(job_id, str(error), **_classify_source_code_qa_job_error(str(error)))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            app.logger.exception("Source code QA query job failed unexpectedly.")
            message = f"Unexpected error: {error}"
            job_store.fail(job_id, message, **_classify_source_code_qa_job_error(message))


def bind_source_code_qa_job_helpers(global_context: dict[str, object]) -> None:
    helpers = [
        _run_source_code_qa_sync_job,
        _run_source_code_qa_query_job,
    ]
    _bind_web_globals(helpers, global_context)
