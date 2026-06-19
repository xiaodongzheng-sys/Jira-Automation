"""Source Code QA web runtime, attachment, generated artifact, access, and job payload helpers."""
from __future__ import annotations


def _bind_web_globals(functions: list[object], global_context: dict[str, object]) -> None:
    for function in functions:
        target = getattr(function, "__wrapped__", function)
        globals_dict = getattr(target, "__globals__", None)
        if globals_dict is not None:
            globals_dict.update(global_context)


def _source_code_qa_codex_session_lock(session_id: str) -> threading.Lock:
    normalized = str(session_id or "").strip()
    if not normalized:
        normalized = "_no_session"
    with _source_code_qa_codex_session_locks_guard:
        lock = _source_code_qa_codex_session_locks.get(normalized)
        if lock is None:
            lock = threading.Lock()
            _source_code_qa_codex_session_locks[normalized] = lock
        return lock


def _get_source_code_qa_session_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQASessionStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_SESSION_STORE"]


def _get_source_code_qa_attachment_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQAAttachmentStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_ATTACHMENT_STORE"]


def _get_source_code_qa_generated_artifact_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQAGeneratedArtifactStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_GENERATED_ARTIFACT_STORE"]


def _get_source_code_qa_runtime_evidence_store():
    settings: Settings = current_app.config["SETTINGS"]
    if _local_agent_source_code_qa_enabled(settings):
        return RemoteSourceCodeQARuntimeEvidenceStore(_build_local_agent_client(settings))
    return current_app.config["SOURCE_CODE_QA_RUNTIME_EVIDENCE_STORE"]


def _can_access_source_code_qa(settings: Settings) -> bool:
    if not _shared_portal_enabled(settings):
        return True
    return _is_portal_user(settings=settings)


def _can_manage_source_code_qa(settings: Settings) -> bool:
    return _is_portal_admin()


def _can_use_source_code_qa_chat(settings: Settings) -> bool:
    return _is_portal_admin()


def _source_code_qa_auth_payload(settings: Settings) -> dict[str, Any]:
    email = _current_google_email()
    owner_email = settings.source_code_qa_owner_email.strip().lower()
    normalized_admins = {PORTAL_ADMIN_EMAIL}
    if _is_portal_admin(email):
        match_source = "portal_admin"
    else:
        match_source = ""
    return {
        "signed_in_email": email,
        "can_manage": _is_portal_admin(email),
        "owner_email": owner_email,
        "admin_email_count": len(normalized_admins),
        "admin_match_source": match_source,
    }


def _source_code_qa_git_auth_ready(service: Any, settings: Settings) -> bool:
    if hasattr(service, "git_auth_ready"):
        return bool(service.git_auth_ready())
    return bool(settings.source_code_qa_gitlab_token)


def _build_source_code_qa_service(llm_provider: str | None = None) -> SourceCodeQAService:
    service: SourceCodeQAService = current_app.config["SOURCE_CODE_QA_SERVICE"]
    normalized_provider = SourceCodeQAService.normalize_query_llm_provider(llm_provider)
    resolved = service if llm_provider is None else service.with_llm_provider(normalized_provider)
    if _local_agent_source_code_qa_enabled(current_app.config["SETTINGS"]):
        return RemoteSourceCodeQAService(_build_local_agent_client(current_app.config["SETTINGS"]), service, llm_provider=normalized_provider or resolved.llm_provider_name)
    return resolved


def _source_code_qa_query_sync_mode(settings: Settings) -> str:
    mode = str(os.getenv("SOURCE_CODE_QA_QUERY_SYNC_MODE") or "").strip().lower()
    if mode in {"blocking", "background", "disabled"}:
        return mode
    return "disabled"


def _source_code_qa_scope_has_queryable_index(service: Any, key: str) -> bool:
    try:
        health = service.index_health_payload()
    except Exception:  # noqa: BLE001
        current_app.logger.warning("Source Code Q&A index health check failed before query auto-sync.", exc_info=True)
        return True
    scope = (health.get("keys") or {}).get(key) if isinstance(health, dict) else None
    repos = scope.get("repos") if isinstance(scope, dict) else []
    if not isinstance(repos, list):
        return True
    return any(
        (repo.get("index") or {}).get("queryable")
        and str((repo.get("index") or {}).get("state") or "").lower() in {"ready", "stale"}
        for repo in repos
        if isinstance(repo, dict)
    )


def _prepare_source_code_qa_auto_sync(
    service: Any,
    *,
    pm_team: str,
    country: str,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    settings: Settings = current_app.config["SETTINGS"]
    mode = _source_code_qa_query_sync_mode(settings)
    key = service.mapping_key(pm_team, country) if hasattr(service, "mapping_key") else f"{pm_team}:{country}"
    if mode == "disabled":
        return {
            "attempted": False,
            "status": "skipped",
            "reason": "query-time repository auto-sync is disabled",
            "key": key,
        }
    if mode == "background":
        if not _source_code_qa_scope_has_queryable_index(service, key):
            if progress_callback:
                progress_callback("auto_sync_check", "Preparing the first repository index for this scope.", 0, 1)
            result = service.ensure_synced_today(pm_team=pm_team, country=country)
            if progress_callback:
                progress_callback("auto_sync_completed", "Repository index is ready; starting code search.", 1, 1)
            return result
        if progress_callback:
            progress_callback("auto_sync_queued", "Repository freshness check is running in the background.", 0, 1)
        if hasattr(service, "ensure_synced_today_background"):
            return service.ensure_synced_today_background(pm_team=pm_team, country=country)

        app_obj = current_app._get_current_object()
        logger = current_app.logger

        def run_background_sync() -> None:
            with app_obj.app_context():
                try:
                    service.ensure_synced_today(pm_team=pm_team, country=country)
                except Exception:
                    logger.exception("Source Code Q&A background auto-sync failed for %s.", key)

        threading.Thread(target=run_background_sync, daemon=True).start()
        return {
            "attempted": False,
            "status": "background_queued",
            "reason": "repository freshness check queued in the background",
            "key": key,
        }
    if progress_callback:
        progress_callback("auto_sync_check", "Checking repository sync schedule.", 0, 1)
    result = service.ensure_synced_today(pm_team=pm_team, country=country)
    if progress_callback:
        if result.get("attempted"):
            progress_callback("auto_sync_completed", "Repository auto-sync completed; starting code search.", 1, 1)
        else:
            progress_callback("auto_sync_completed", "Repository indexes do not need scheduled sync; starting code search.", 1, 1)
    return result


def _local_agent_source_code_qa_enabled(settings: Settings) -> bool:
    return bool(
        _local_agent_mode_enabled(settings)
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
        and settings.local_agent_source_code_qa_enabled
    )


def _source_code_qa_options_payload(service: SourceCodeQAService) -> dict[str, Any]:
    options = service.options_payload()
    providers = []
    for provider in options.get("llm_providers") or []:
        provider_payload = dict(provider)
        value = str(provider_payload.get("value") or "")
        available = value in LLM_PROVIDER_ALLOWED_QUERY_CHOICES
        provider_payload["available"] = available
        provider_payload["disabled"] = not available
        base_label = str(provider_payload.get("label") or value).replace(" (Unavailable)", "")
        provider_payload["label"] = base_label if available else f"{base_label} (Unavailable)"
        providers.append(provider_payload)
    options["llm_providers"] = providers
    options["runtime_capabilities"] = _source_code_qa_runtime_capabilities_payload()
    return options


def _source_code_qa_runtime_capabilities_payload() -> dict[str, dict[str, dict[str, bool]]]:
    teams = ("AF", "GRC", "CRMS")
    countries = (ALL_COUNTRY, *tuple(CRMS_COUNTRIES))
    capabilities = {
        team: {
            country: {"hasConfig": False, "hasDB": False, "hasDictionary": False}
            for country in countries
        }
        for team in teams
    }
    try:
        store = _get_source_code_qa_runtime_evidence_store()
        for team in teams:
            for country in countries:
                try:
                    evidence_items = store.list(pm_team=team, country=country)
                except ToolError:
                    evidence_items = []
                for item in evidence_items:
                    source_type = str(item.get("source_type") or "").strip().lower()
                    if source_type == "apollo":
                        capabilities[team][country]["hasConfig"] = True
                    elif source_type == "db":
                        capabilities[team][country]["hasDB"] = True
                    elif source_type == "data_dictionary":
                        capabilities[team][country]["hasDictionary"] = True
    except Exception:  # noqa: BLE001 - capability badges are advisory only.
        return capabilities
    return capabilities


def _source_code_qa_provider_available(llm_provider: str | None) -> bool:
    provider = str(llm_provider or LLM_PROVIDER_CODEX_CLI_BRIDGE).strip().lower() or LLM_PROVIDER_CODEX_CLI_BRIDGE
    return provider in LLM_PROVIDER_ALLOWED_QUERY_CHOICES


def _source_code_qa_public_answer_mode(answer_mode: str | None) -> str:
    mode = str(answer_mode or "auto").strip()
    return mode if mode == "auto" else "auto"


def _source_code_qa_query_mode(query_mode: str | None) -> str:
    return "deep"


def _source_code_qa_attachment_ids(payload: dict[str, Any]) -> list[str]:
    raw_ids = payload.get("attachment_ids") if isinstance(payload, dict) else []
    if raw_ids is None:
        return []
    if not isinstance(raw_ids, list):
        raise ToolError("attachment_ids must be a list.")
    attachment_ids = [str(item or "").strip() for item in raw_ids if str(item or "").strip()]
    if len(attachment_ids) > SourceCodeQAAttachmentStore.MAX_ATTACHMENTS:
        raise ToolError(f"At most {SourceCodeQAAttachmentStore.MAX_ATTACHMENTS} attachments are supported per Source Code Q&A question.")
    return attachment_ids


def _resolve_source_code_qa_query_attachments(
    payload: dict[str, Any],
    *,
    owner_email: str,
    session_id: str,
) -> list[dict[str, Any]]:
    attachment_ids = _source_code_qa_attachment_ids(payload)
    if not attachment_ids:
        return []
    if not session_id:
        raise ToolError("A Source Code Q&A session is required before sending attachments.")
    session_payload = _get_source_code_qa_session_store().get(session_id, owner_email=owner_email)
    if session_payload is None:
        raise ToolError("Source Code Q&A session was not found for these attachments.")
    return _get_source_code_qa_attachment_store().resolve_many(
        owner_email=owner_email,
        session_id=session_id,
        attachment_ids=attachment_ids,
    )


def _source_code_qa_public_attachments(attachments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        SourceCodeQAAttachmentStore.public_metadata(item)
        for item in attachments
        if isinstance(item, dict)
    ]


def _resolve_source_code_qa_runtime_evidence(*, pm_team: str, country: str) -> list[dict[str, Any]]:
    normalized_country = str(country or "").strip().upper()
    if normalized_country in {"", ALL_COUNTRY.upper()}:
        return []
    try:
        return _get_source_code_qa_runtime_evidence_store().resolve_scope(pm_team=pm_team, country=country)
    except ToolError:
        raise
    except Exception as error:  # noqa: BLE001 - runtime evidence must not break code Q&A.
        current_app.logger.warning("Source Code Q&A runtime evidence could not be loaded: %s", error)
        return []


def _source_code_qa_public_runtime_evidence(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        SourceCodeQARuntimeEvidenceStore.public_metadata(item)
        for item in evidence
        if isinstance(item, dict)
    ]


def _source_code_qa_public_generated_artifacts(artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        SourceCodeQAGeneratedArtifactStore.public_metadata(item)
        for item in artifacts
        if isinstance(item, dict)
    ]


def _build_source_code_qa_generated_artifacts(
    *,
    owner_email: str,
    session_id: str,
    pm_team: str,
    country: str,
    question: str,
    result: dict[str, Any],
    runtime_evidence: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    answer_text = str(result.get("llm_answer") or result.get("answer") or result.get("rendered_answer") or "")
    sql_blocks = _extract_source_code_qa_sql_blocks(answer_text)
    if not sql_blocks:
        return []
    try:
        artifact = _get_source_code_qa_generated_artifact_store().save_sql_package(
            owner_email=owner_email,
            session_id=session_id,
            pm_team=pm_team,
            country=country,
            question=question,
            sql=sql_blocks[0],
            readme=_build_source_code_qa_sql_readme(
                pm_team=pm_team,
                country=country,
                question=question,
                sql=sql_blocks[0],
                result=result,
                runtime_evidence=runtime_evidence,
            ),
        )
    except Exception as error:  # noqa: BLE001 - artifact packaging must not fail the answer.
        current_app.logger.warning("Source Code Q&A generated SQL artifact could not be saved: %s", error)
        return []
    return [artifact]


def _build_source_code_qa_session_context(result: dict[str, Any], request_payload: dict[str, Any]) -> dict[str, Any]:
    compact = _compact_source_code_qa_session_payload(result)
    matches = compact.get("matches") or []
    codex_trace = compact.get("codex_cli_trace") if isinstance(compact.get("codex_cli_trace"), dict) else {}
    candidate_paths = (compact.get("llm_route") or {}).get("candidate_paths") or []
    inspected_paths: list[dict[str, Any]] = []
    for raw_path in codex_trace.get("probable_inspected_files") or []:
        raw_text = str(raw_path or "")
        matched = None
        for candidate in candidate_paths:
            if isinstance(candidate, dict) and str(candidate.get("path") or "") and str(candidate.get("path") or "") in raw_text:
                matched = candidate
                break
        if matched:
            inspected_paths.append({**matched, "source": "codex_cli_trace"})
    if not inspected_paths:
        inspected_paths = [
            item for item in candidate_paths[:5]
            if isinstance(item, dict) and str(item.get("trace_stage") or "") == "followup_memory"
        ]
    session_id = str(codex_trace.get("session_id") or "").strip()
    return {
        "key": f"{request_payload.get('pm_team') or ''}:{request_payload.get('country') or ALL_COUNTRY}",
        "pm_team": request_payload.get("pm_team") or "",
        "country": request_payload.get("country") or ALL_COUNTRY,
        "question": request_payload.get("question") or "",
        "trace_id": compact.get("trace_id") or "",
        "summary": compact.get("summary") or "",
        "answer": compact.get("llm_answer") or "",
        "rendered_answer": compact.get("llm_answer") or "",
        "attachments": compact.get("attachments") or [],
        "llm_provider": compact.get("llm_provider") or "",
        "llm_model": compact.get("llm_model") or "",
        "llm_route": compact.get("llm_route") or {},
        "codex_session_max_turns": (compact.get("llm_route") or {}).get("codex_session_max_turns") or 8,
        "codex_cli_summary": compact.get("codex_cli_summary") or {},
        "codex_cli_trace": codex_trace,
        "codex_cli_session": {
            "session_id": session_id,
            "mode": codex_trace.get("session_mode") or "",
            "last_used_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        } if session_id else {},
        "codex_inspected_paths": inspected_paths[:12],
        "codex_citation_validation": compact.get("codex_citation_validation") or {},
        "codex_candidate_paths": candidate_paths,
        "repo_scope": list(dict.fromkeys([match.get("repo") for match in matches if match.get("repo")]))[:8],
        "matches": matches[:8],
        "matches_snapshot": matches[:10],
        "trace_paths": (result.get("trace_paths") or [])[:5],
        "query_mode": compact.get("query_mode") or "",
        "deadline_seconds": compact.get("deadline_seconds") or 0,
        "deadline_hit": bool(compact.get("deadline_hit")),
        "fallback_used": bool(compact.get("fallback_used")),
        "fallback_answer_quality": compact.get("fallback_answer_quality") or "",
        "fallback_evidence_count": compact.get("fallback_evidence_count") or 0,
        "fallback_claim_count": compact.get("fallback_claim_count") or 0,
        "deadline_fallback_reason": compact.get("deadline_fallback_reason") or "",
        "structured_answer": compact.get("structured_answer") or {},
        "answer_contract": compact.get("answer_contract") or {},
        "evidence_pack": result.get("evidence_pack") or {},
        "answer_quality": compact.get("answer_quality") or {},
    }


def _source_code_qa_release_gate_payload(settings: Settings) -> dict[str, Any]:
    data_root = settings.team_portal_data_dir
    if not data_root.is_absolute():
        data_root = (PROJECT_ROOT / data_root).resolve()
    gate = _read_json_file(data_root / "run" / "source_code_qa_release_gate.json")
    latest_eval = _read_json_file(data_root / "source_code_qa" / "eval_runs" / "latest.json")
    status = str(gate.get("status") or latest_eval.get("status") or "missing")
    updated_at = gate.get("timestamp") or latest_eval.get("timestamp")
    return {
        "status": status,
        "updated_at": updated_at,
        "summary": gate.get("summary") or "",
        "thresholds": gate.get("thresholds") or {},
        "checks": gate.get("checks") or {},
        "latest_eval": {
            "status": latest_eval.get("status"),
            "eval": latest_eval.get("eval") or {},
            "llm_smoke": latest_eval.get("llm_smoke") or {},
            "report_path": latest_eval.get("report_path"),
        },
    }


def _require_source_code_qa_access(settings: Settings, *, api: bool = False):
    if _shared_portal_enabled(settings):
        login_gate = _require_google_login(settings, api=api)
        if login_gate is not None:
            return login_gate
    message = "Source Code Q&A is restricted to signed-in NPT users."
    if not _can_access_source_code_qa(settings):
        if api:
            return jsonify({"status": "error", "message": message}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("index"))
    return None


def _require_source_code_qa_manage_access(settings: Settings, *, api: bool = False):
    access_gate = _require_source_code_qa_access(settings, api=api)
    if access_gate is not None:
        return access_gate
    auth_payload = _source_code_qa_auth_payload(settings)
    message = (
        f"Source Code Q&A repository admin is restricted to {PORTAL_ADMIN_EMAIL}. "
        f"Signed in as {auth_payload['signed_in_email'] or 'unknown'}."
    )
    if not _can_manage_source_code_qa(settings):
        if api:
            return jsonify({"status": "error", "message": message, "auth": auth_payload}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("source_code_qa"))
    return None


def _require_source_code_qa_chat_access(settings: Settings, *, api: bool = False):
    access_gate = _require_source_code_qa_access(settings, api=api)
    if access_gate is not None:
        return access_gate
    auth_payload = _source_code_qa_auth_payload(settings)
    message = (
        f"Source Code Q&A chat is restricted to {PORTAL_ADMIN_EMAIL}. "
        f"Signed in as {auth_payload['signed_in_email'] or 'unknown'}."
    )
    if not _can_use_source_code_qa_chat(settings):
        if api:
            return jsonify({"status": "error", "message": message, "auth": auth_payload}), HTTPStatus.FORBIDDEN
        flash(message, "error")
        return redirect(url_for("source_code_qa"))
    return None


def _classify_source_code_qa_job_error(message: str) -> dict[str, Any]:
    normalized = str(message or "").lower()
    if "local-agent" in normalized or "local agent" in normalized or "connection refused" in normalized:
        return {"error_category": "local_agent_offline", "error_code": "local_agent_unavailable", "error_retryable": True}
    if "ngrok" in normalized or "err_ngrok_3200" in normalized or "gateway" in normalized or "html error" in normalized:
        return {"error_category": "gateway_disconnected", "error_code": "gateway_disconnected", "error_retryable": True}
    if "rate limit" in normalized or "quota" in normalized:
        return {"error_category": "codex_timeout_or_rate_limit", "error_code": "llm_rate_limited", "error_retryable": True}
    if "timeout" in normalized or "timed out" in normalized:
        return {"error_category": "codex_timeout_or_rate_limit", "error_code": "llm_timeout", "error_retryable": True}
    return {"error_category": "job_failed", "error_code": "source_code_qa_job_failed", "error_retryable": True}


def _public_source_code_qa_job_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = dict(snapshot)
    payload.pop("owner_email", None)
    payload.setdefault("status", "ok")
    payload["progress"] = snapshot.get("progress") if isinstance(snapshot.get("progress"), dict) else {
        "stage": snapshot.get("stage") or "",
        "current": snapshot.get("current") or 0,
        "total": snapshot.get("total") or 0,
        "message": snapshot.get("message") or "",
    }
    state = str(payload.get("state") or "")
    payload["query_mode"] = _source_code_qa_query_mode(payload.get("query_mode"))
    payload["queued_position"] = int(payload.get("queued_position") or 0)
    payload["eta_seconds_range"] = [
        max(0, int(value or 0))
        for value in (payload.get("eta_seconds_range") if isinstance(payload.get("eta_seconds_range"), list) else [])
    ][:2]
    payload["running_user_count"] = int(payload.get("running_user_count") or 0)
    payload["last_progress_at"] = float(payload.get("last_progress_at") or payload.get("updated_at") or 0)
    if payload.get("stalled_retryable"):
        payload["error_category"] = payload.get("error_category") or "job_stalled"
        payload["error_code"] = payload.get("error_code") or "job_stalled_retryable"
        payload["error_retryable"] = True
    if state == "running":
        payload.setdefault("error_category", "job_running")
        payload.setdefault("error_code", "")
        payload.setdefault("error_retryable", True)
    if state == "queued":
        payload.setdefault("error_category", "job_queued")
        payload.setdefault("error_code", "")
        payload.setdefault("error_retryable", True)
    if state == "failed":
        classification = _classify_source_code_qa_job_error(str(payload.get("error") or payload.get("message") or ""))
        for key, value in classification.items():
            if not payload.get(key):
                payload[key] = value
    return payload


def _source_code_qa_job_snapshot_for_current_user(job_id: str) -> dict[str, Any] | None:
    snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
    if snapshot is None or snapshot.get("action") != "source-code-qa-query":
        return None
    owner_email = str(snapshot.get("owner_email") or "").strip().lower()
    current_email = _current_google_email()
    if owner_email and owner_email != current_email and not _can_manage_source_code_qa(current_app.config["SETTINGS"]):
        return None
    return snapshot


def bind_source_code_qa_runtime_helpers(global_context: dict[str, object]) -> None:
    helpers = [
        _source_code_qa_codex_session_lock,
        _get_source_code_qa_session_store,
        _get_source_code_qa_attachment_store,
        _get_source_code_qa_generated_artifact_store,
        _get_source_code_qa_runtime_evidence_store,
        _can_access_source_code_qa,
        _can_manage_source_code_qa,
        _source_code_qa_auth_payload,
        _source_code_qa_git_auth_ready,
        _build_source_code_qa_service,
        _source_code_qa_query_sync_mode,
        _source_code_qa_scope_has_queryable_index,
        _prepare_source_code_qa_auto_sync,
        _local_agent_source_code_qa_enabled,
        _source_code_qa_options_payload,
        _source_code_qa_runtime_capabilities_payload,
        _source_code_qa_provider_available,
        _source_code_qa_public_answer_mode,
        _source_code_qa_query_mode,
        _source_code_qa_attachment_ids,
        _resolve_source_code_qa_query_attachments,
        _source_code_qa_public_attachments,
        _resolve_source_code_qa_runtime_evidence,
        _source_code_qa_public_runtime_evidence,
        _source_code_qa_public_generated_artifacts,
        _build_source_code_qa_generated_artifacts,
        _build_source_code_qa_session_context,
        _source_code_qa_release_gate_payload,
        _require_source_code_qa_access,
        _require_source_code_qa_manage_access,
        _classify_source_code_qa_job_error,
        _public_source_code_qa_job_snapshot,
        _source_code_qa_job_snapshot_for_current_user,
    ]
    _bind_web_globals(helpers, global_context)
