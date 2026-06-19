"""Source Code QA Flask route registration."""
from __future__ import annotations

import shutil
import tempfile
from urllib.parse import urlsplit

from flask import after_this_request, redirect

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa_patterns import PLACEHOLDER_HOST_PATTERN


def _reject_placeholder_repo_urls(repositories: object) -> None:
    """Block the demo/example clone URLs shown as Repo Admin hints (e.g.
    https://git.example.com/team/repo.git) from being saved as a real mapping.
    Runs only on the human "Save Config" path so loading legacy data and
    isolated eval/test seeding stay unaffected."""
    if not isinstance(repositories, list):
        return
    for repo in repositories:
        if not isinstance(repo, dict):
            continue
        url = str(repo.get("url") or "").strip()
        if not url:
            continue
        host = urlsplit(url).hostname or ""
        if host and PLACEHOLDER_HOST_PATTERN.search(host):
            raise ToolError(
                f"'{url}' uses a placeholder host ({host}); enter a real HTTPS clone URL "
                "such as https://gitlab.npt.seabank.io/group/repo.git."
            )


def register_source_code_qa_routes(app: object, settings: object, global_context: dict[str, object]) -> None:
    def _refresh_source_code_qa_globals() -> None:
        globals().update(global_context)
        for binder_name in (
            "bind_source_code_qa_runtime_helpers",
            "bind_source_code_qa_effort_helpers",
            "bind_source_code_qa_job_helpers",
        ):
            binder = global_context.get(binder_name)
            if callable(binder):
                binder(global_context)

    _refresh_source_code_qa_globals()

    def _human_size(num_bytes: object) -> str:
        try:
            size = float(num_bytes)
        except (TypeError, ValueError):
            return ""
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024 or unit == "GB":
                return (f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}")
            size /= 1024
        return ""

    def _format_generated_at(value: object) -> str:
        from bpmis_jira_tool.timefmt import format_gmt8

        return format_gmt8(value)

    def _repo_download_scopes_with_status() -> list:
        scopes = repo_download_scope_definitions()
        for scope in scopes:
            scope["meta"] = ""
            status = None
            try:
                from bpmis_jira_tool.public_artifacts_gcs import fetch_repo_download_status, public_gcs_read_bucket

                if public_gcs_read_bucket():
                    status = fetch_repo_download_status(scope["filename"])
            except Exception:
                status = None
            if status is None:
                continue
            if not status.get("available"):
                scope["meta"] = "Not published yet"
                continue
            parts = []
            generated = _format_generated_at(status.get("generated_at"))
            if generated:
                parts.append(f"Updated {generated}")
            size = _human_size(status.get("size_bytes"))
            if size:
                parts.append(size)
            scope["meta"] = " · ".join(parts)
        return scopes

    def _download_local_agent_archive(response, *, fallback_name: str):
        content_type = response.headers.get("Content-Type") or "application/zip"
        disposition = response.headers.get("Content-Disposition") or ""
        match = re.search(r'filename="?([^";]+)"?', disposition)
        if match:
            fallback_name = match.group(1)
        temp_dir = tempfile.mkdtemp(prefix="source-code-local-agent-download-")
        temp_path = None
        try:
            from pathlib import Path

            temp_path = Path(temp_dir) / fallback_name
            with temp_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        finally:
            response.close()
        return temp_dir, temp_path, content_type, fallback_name

    @app.before_request
    def _refresh_source_code_qa_route_module_globals():
        path = str(getattr(request, "path", "") or "")
        if path == "/source-code-qa" or path.startswith("/api/source-code-qa/"):
            _refresh_source_code_qa_globals()

    @app.get("/source-code-qa")
    def source_code_qa():
        # Login required: only allowed portal users (admin + allowlisted
        # emails/domains) may access Source Code QA.
        access_gate = _require_source_code_qa_access(settings)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        service = _build_source_code_qa_service()
        return render_template(
            "source_code_qa.html",
            page_title="Source Code Q&A",
            user_identity=user_identity,
            options=_source_code_qa_options_payload(service),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            country_options=list(CRMS_COUNTRIES),
            all_country=ALL_COUNTRY,
            repo_download_scopes=_repo_download_scopes_with_status(),
            can_use_source_code_qa_chat=_can_use_source_code_qa_chat(settings),
            can_manage_source_code_qa=_can_manage_source_code_qa(settings),
            # Assets and auth links go through the Cloud Run surface so the
            # public Repo Download page keeps working while the Mac is offline.
            cloud_auth_mode=True,
            asset_revision=_current_release_revision(),
        )

    @app.get("/api/source-code-qa/repo-downloads/<scope_key>")
    def source_code_qa_repo_download_api(scope_key: str):
        try:
            scope = resolve_repo_download_scope(scope_key)
            if _local_agent_source_code_qa_enabled(settings):
                response = _build_local_agent_client(settings).source_code_qa_repo_download(scope["scope_key"])
                temp_dir, temp_path, content_type, download_name = _download_local_agent_archive(
                    response,
                    fallback_name=scope["filename"],
                )

                @after_this_request
                def _cleanup_local_agent_temp(response_obj):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    return response_obj

                return send_file(
                    temp_path,
                    mimetype=content_type,
                    download_name=download_name,
                    as_attachment=True,
                )
            if public_gcs_read_bucket():
                # Cloud Run: hydrate the bundle from GCS onto a temporary file
                # only as a fallback. The preferred path is to redirect the
                # browser to a short-lived signed GCS URL so larger archives do
                # not travel through the Cloud Run response path at all.
                from pathlib import Path

                from bpmis_jira_tool.public_artifacts_gcs import (
                    fetch_repo_download_archive_to_file,
                    fetch_repo_download_signed_url,
                )

                signed = fetch_repo_download_signed_url(scope["filename"])
                if signed is not None:
                    _, signed_url = signed
                    return redirect(signed_url, code=302)

                temp_dir = tempfile.mkdtemp(prefix="source-code-repo-download-")
                temp_path = Path(temp_dir) / scope["filename"]
                metadata = fetch_repo_download_archive_to_file(scope["filename"], temp_path)
                if metadata is not None:
                    @after_this_request
                    def _cleanup_repo_download_temp(response):
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        return response

                    return send_file(
                        temp_path,
                        mimetype="application/zip",
                        download_name=str(metadata.get("filename") or scope["filename"]),
                        as_attachment=True,
                    )
                return jsonify({
                    "status": "error",
                    "message": "This source bundle has not been published yet. Ask the admin to run a sync.",
                }), HTTPStatus.NOT_FOUND
            metadata, content = build_repo_download_zip(_build_source_code_qa_service(), scope["scope_key"])
            return send_file(
                io.BytesIO(content),
                mimetype="application/zip",
                download_name=str(metadata.get("filename") or scope["filename"]),
                as_attachment=True,
            )
        except ToolError as error:
            status_code = HTTPStatus.SERVICE_UNAVAILABLE if _is_local_agent_unavailable_error(error) else HTTPStatus.BAD_REQUEST
            return jsonify({"status": "error", "message": str(error), "error_category": _tool_error_category(error)}), status_code

    @app.get("/api/source-code-qa/config")
    def source_code_qa_config_api():
        access_gate = _require_source_code_qa_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            service = _build_source_code_qa_service()
            codex_service = _build_source_code_qa_service("codex_cli_bridge")
            return jsonify(
                {
                    "status": "ok",
                    "answer_mode": "auto",
                    "query_mode": "deep",
                    "can_manage": _can_manage_source_code_qa(settings),
                    "auth": _source_code_qa_auth_payload(settings),
                    "git_auth_ready": _source_code_qa_git_auth_ready(service, settings),
                    "llm_ready": service.llm_ready(),
                    "llm_provider": settings.source_code_qa_llm_provider,
                    "llm_providers": {
                        "codex_cli_bridge": {"ready": codex_service.llm_ready(), "label": "Codex", "available": codex_service.llm_ready()},
                    },
                    "llm_model": service.llm_budgets["balanced"]["model"],
                    "llm_cheap_model": service.llm_budgets["cheap"]["model"],
                    "llm_deep_model": service.llm_budgets["deep"]["model"],
                    "llm_fallback_model": service._llm_fallback_model(),
                    "llm_policy": service.llm_policy_payload(),
                    "index_health": service.index_health_payload(),
                    "release_gate": _source_code_qa_release_gate_payload(settings),
                    "domain_knowledge": service.domain_knowledge_payload(),
                    "options": _source_code_qa_options_payload(service),
                    "config": service.load_config(),
                }
            )
        except ToolError as error:
            status_code = HTTPStatus.SERVICE_UNAVAILABLE if _is_local_agent_unavailable_error(error) else HTTPStatus.BAD_REQUEST
            return jsonify({"status": "error", "message": str(error), "error_category": _tool_error_category(error)}), status_code
        except Exception as error:  # noqa: BLE001 - keep API clients on JSON even for unexpected failures.
            request_id = getattr(g, "request_id", "")
            current_app.logger.exception("Source Code Q&A config failed unexpectedly")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Source Code Q&A config failed unexpectedly. Please refresh; if it repeats, share the request ID.",
                        "request_id": request_id,
                        "error_category": "source_code_qa_internal",
                        "error_retryable": True,
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/source-code-qa/config")
    def source_code_qa_save_config_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = {}
        try:
            repositories = payload.get("repositories") or []
            _reject_placeholder_repo_urls(repositories)
            result = _build_source_code_qa_service().save_mapping(
                pm_team=str(payload.get("pm_team") or ""),
                country=str(payload.get("country") or ""),
                repositories=repositories,
            )
            return jsonify({"status": "ok", **result})
        except ToolError as error:
            status_code = HTTPStatus.SERVICE_UNAVAILABLE if _is_local_agent_unavailable_error(error) else HTTPStatus.BAD_REQUEST
            return jsonify({"status": "error", "message": str(error), "error_category": _tool_error_category(error)}), status_code

    @app.post("/api/source-code-qa/sync")
    def source_code_qa_sync_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        pm_team = str(payload.get("pm_team") or "")
        country = str(payload.get("country") or "")
        job_store: JobStore = current_app.config["JOB_STORE"]
        job = job_store.create("source-code-qa-sync", title="Sync Source Code Repositories")
        app_obj = current_app._get_current_object()
        thread = threading.Thread(
            target=_run_source_code_qa_sync_job,
            args=(app_obj, job.job_id, settings, pm_team, country),
            daemon=True,
        )
        thread.start()
        return jsonify({"status": "queued", "job_id": job.job_id})

    @app.get("/api/source-code-qa/sync-jobs/<job_id>")
    def source_code_qa_sync_job_api(job_id: str):
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
        if snapshot is None or snapshot.get("action") != "source-code-qa-sync":
            return jsonify(
                {
                    "status": "error",
                    "message": "Source Code Q&A sync job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }
            ), HTTPStatus.NOT_FOUND
        return jsonify(_public_source_code_qa_job_snapshot(snapshot))

    @app.route("/api/source-code-qa/sessions", methods=["GET", "POST"])
    def source_code_qa_sessions_api():
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_session_store()
        owner_email = _current_google_email() or "local"
        if request.method == "GET":
            limit = request.args.get("limit", "30")
            try:
                limit_value = int(limit)
            except ValueError:
                limit_value = 30
            return jsonify({"status": "ok", "sessions": store.list(owner_email=owner_email, limit=limit_value)})

        payload = request.get_json(silent=True) or {}
        session_payload = store.create(
            owner_email=owner_email,
            pm_team=str(payload.get("pm_team") or ""),
            country=str(payload.get("country") or ""),
            llm_provider=str(payload.get("llm_provider") or ""),
            title=str(payload.get("title") or ""),
        )
        return jsonify({"status": "ok", "session": session_payload})

    @app.get("/api/source-code-qa/sessions/<session_id>")
    def source_code_qa_session_api(session_id: str):
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_session_store()
        session_payload = store.get(session_id, owner_email=_current_google_email() or "local")
        if session_payload is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        return jsonify({"status": "ok", "session": session_payload})

    @app.post("/api/source-code-qa/sessions/<session_id>/archive")
    def source_code_qa_session_archive_api(session_id: str):
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_session_store()
        archived = store.archive(session_id, owner_email=_current_google_email() or "local")
        if archived is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        return jsonify(archived)

    @app.post("/api/source-code-qa/attachments")
    def source_code_qa_attachments_api():
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email() or "local"
        session_id = str(request.form.get("session_id") or "").strip()
        if not session_id:
            return jsonify({"status": "error", "message": "A Source Code Q&A session is required before uploading attachments."}), HTTPStatus.BAD_REQUEST
        if _get_source_code_qa_session_store().get(session_id, owner_email=owner_email) is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        uploaded = request.files.get("file")
        if uploaded is None:
            return jsonify({"status": "error", "message": "Upload a file field named file."}), HTTPStatus.BAD_REQUEST
        try:
            content = uploaded.read()
            attachment = _get_source_code_qa_attachment_store().save_bytes(
                owner_email=owner_email,
                session_id=session_id,
                filename=uploaded.filename or "attachment",
                mime_type=uploaded.mimetype or "",
                content=content,
            )
            return jsonify({"status": "ok", "attachment": attachment})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.get("/api/source-code-qa/attachments/<attachment_id>")
    def source_code_qa_attachment_api(attachment_id: str):
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email() or "local"
        session_id = str(request.args.get("session_id") or "").strip()
        if not session_id:
            return jsonify({"status": "error", "message": "session_id is required."}), HTTPStatus.BAD_REQUEST
        if _get_source_code_qa_session_store().get(session_id, owner_email=owner_email) is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        try:
            metadata, content = _get_source_code_qa_attachment_store().get_bytes(
                owner_email=owner_email,
                session_id=session_id,
                attachment_id=attachment_id,
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.NOT_FOUND
        return send_file(
            io.BytesIO(content),
            mimetype=metadata.get("mime_type") or "application/octet-stream",
            download_name=metadata.get("filename") or "attachment",
            as_attachment=False,
        )

    @app.get("/api/source-code-qa/generated-artifacts/<artifact_id>")
    def source_code_qa_generated_artifact_api(artifact_id: str):
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email() or "local"
        session_id = str(request.args.get("session_id") or "").strip()
        if not session_id:
            return jsonify({"status": "error", "message": "session_id is required."}), HTTPStatus.BAD_REQUEST
        if _get_source_code_qa_session_store().get(session_id, owner_email=owner_email) is None:
            return jsonify({"status": "error", "message": "Source Code Q&A session was not found."}), HTTPStatus.NOT_FOUND
        try:
            metadata, content = _get_source_code_qa_generated_artifact_store().get_bytes(
                owner_email=owner_email,
                session_id=session_id,
                artifact_id=artifact_id,
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.NOT_FOUND
        return send_file(
            io.BytesIO(content),
            mimetype=metadata.get("mime_type") or "application/zip",
            download_name=metadata.get("filename") or "source-code-qa-sql-package.zip",
            as_attachment=True,
        )

    @app.route("/api/source-code-qa/runtime-evidence", methods=["GET", "POST"])
    def source_code_qa_runtime_evidence_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        store = _get_source_code_qa_runtime_evidence_store()
        if request.method == "GET":
            try:
                evidence = store.list(
                    pm_team=str(request.args.get("pm_team") or ""),
                    country=str(request.args.get("country") or ""),
                )
                return jsonify({"status": "ok", "evidence": evidence})
            except ToolError as error:
                return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

        uploaded = request.files.get("file")
        if uploaded is None:
            return jsonify({"status": "error", "message": "Upload a file field named file."}), HTTPStatus.BAD_REQUEST
        try:
            evidence = store.save_bytes(
                pm_team=str(request.form.get("pm_team") or ""),
                country=str(request.form.get("country") or ""),
                source_type=str(request.form.get("source_type") or "other"),
                uploaded_by=_current_google_email() or "local",
                filename=uploaded.filename or "runtime-evidence",
                mime_type=uploaded.mimetype or "",
                content=uploaded.read(),
            )
            return jsonify({"status": "ok", "evidence": evidence, "items": store.list(pm_team=evidence["pm_team"], country=evidence["country"])})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.delete("/api/source-code-qa/runtime-evidence/<evidence_id>")
    def source_code_qa_runtime_evidence_delete_api(evidence_id: str):
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            pm_team = str(request.args.get("pm_team") or "")
            country = str(request.args.get("country") or "")
            deleted = _get_source_code_qa_runtime_evidence_store().delete(
                pm_team=pm_team,
                country=country,
                evidence_id=evidence_id,
            )
            return jsonify(
                {
                    "status": "ok",
                    "deleted": deleted,
                    "evidence": _get_source_code_qa_runtime_evidence_store().list(pm_team=pm_team, country=country),
                }
            )
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    @app.post("/api/source-code-qa/query")
    def source_code_qa_query_api():
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        if payload.get("async"):
            if not _source_code_qa_provider_available(payload.get("llm_provider")):
                return jsonify({"status": "error", "message": "Selected Source Code Q&A model is unavailable."}), HTTPStatus.BAD_REQUEST
            job_store: JobStore = current_app.config["JOB_STORE"]
            job = job_store.create("source-code-qa-query", title="Source Code Q&A Query")
            app_obj = current_app._get_current_object()
            async_payload = dict(payload)
            async_payload["query_mode"] = _source_code_qa_query_mode(payload.get("query_mode"))
            owner_email = _current_google_email() or "local"
            async_payload["_session_owner_email"] = owner_email
            session_id = str(payload.get("session_id") or "").strip()
            if session_id:
                try:
                    attachments = _resolve_source_code_qa_query_attachments(payload, owner_email=owner_email, session_id=session_id)
                    session_payload = _get_source_code_qa_session_store().append_pending_question(
                        session_id,
                        owner_email=owner_email,
                        pm_team=str(payload.get("pm_team") or ""),
                        country=str(payload.get("country") or ""),
                        llm_provider=str(payload.get("llm_provider") or ""),
                        question=str(payload.get("question") or ""),
                        job_id=job.job_id,
                        attachments=attachments,
                    )
                    if session_payload is not None:
                        async_payload["_resolved_attachments"] = attachments
                except ToolError:
                    pass
            scheduler: SourceCodeQAQueryScheduler = current_app.config["SOURCE_CODE_QA_QUERY_SCHEDULER"]
            scheduler.submit(app=app_obj, job_id=job.job_id, payload=async_payload, owner_email=owner_email)
            snapshot = _public_source_code_qa_job_snapshot(job_store.snapshot(job.job_id) or {})
            return jsonify({**snapshot, "status": "queued", "job_id": job.job_id, "session_id": session_id})
        try:
            if not _source_code_qa_provider_available(payload.get("llm_provider")):
                raise ToolError("Selected Source Code Q&A model is unavailable.")
            service = _build_source_code_qa_service(payload.get("llm_provider"))
            session_store = _get_source_code_qa_session_store()
            session_id = str(payload.get("session_id") or "").strip()
            owner_email = _current_google_email() or "local"
            conversation_context = payload.get("conversation_context") if isinstance(payload.get("conversation_context"), dict) else None
            if conversation_context is None and session_id:
                conversation_context = session_store.get_context(session_id, owner_email=owner_email)
            if isinstance(payload.get("_resolved_attachments"), list):
                attachments = payload.get("_resolved_attachments") or []
            else:
                attachments = _resolve_source_code_qa_query_attachments(payload, owner_email=owner_email, session_id=session_id)
            pm_team = str(payload.get("pm_team") or "")
            country = str(payload.get("country") or "")
            query_mode = _source_code_qa_query_mode(payload.get("query_mode"))
            runtime_evidence = _resolve_source_code_qa_runtime_evidence(pm_team=pm_team, country=country)
            auto_sync = _prepare_source_code_qa_auto_sync(service, pm_team=pm_team, country=country)
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
                )

            if service.llm_provider_name == "codex_cli_bridge" and session_id:
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
                session_payload = session_store.append_exchange(
                    session_id,
                    owner_email=owner_email,
                    pm_team=str(payload.get("pm_team") or ""),
                    country=str(payload.get("country") or ""),
                    llm_provider=str(payload.get("llm_provider") or ""),
                    question=str(payload.get("question") or ""),
                    result=result,
                    context=_build_source_code_qa_session_context(result, payload),
                    attachments=attachments,
                )
                if session_payload is not None:
                    result["session"] = session_payload
                    result["session_id"] = session_id
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
        except Exception as error:  # noqa: BLE001 - keep API clients on JSON even for unexpected failures.
            request_id = getattr(g, "request_id", "")
            current_app.logger.exception("Source Code Q&A query failed unexpectedly")
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Source Code Q&A failed unexpectedly. Please retry; if it repeats, share the request ID.",
                        "request_id": request_id,
                        "error_category": "source_code_qa_internal",
                        "error_retryable": True,
                    }
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    @app.post("/api/source-code-qa/effort-assessment")
    def source_code_qa_effort_assessment_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        requirement = str(payload.get("requirement") or "").strip()
        if not requirement:
            return jsonify({"status": "error", "message": "Business requirement is empty."}), HTTPStatus.BAD_REQUEST
        if not _source_code_qa_provider_available(payload.get("llm_provider")):
            return jsonify({"status": "error", "message": "Selected Source Code Q&A model is unavailable."}), HTTPStatus.BAD_REQUEST

        job_store: JobStore = current_app.config["JOB_STORE"]
        job = job_store.create("source-code-qa-effort-assessment", title="Source Code Q&A Effort Assessment")
        app_obj = current_app._get_current_object()
        assessment_payload = {
            "pm_team": str(payload.get("pm_team") or ""),
            "country": str(payload.get("country") or ""),
            "language": _source_code_qa_effort_assessment_language(payload.get("language")),
            "requirement": requirement,
            "llm_provider": str(payload.get("llm_provider") or ""),
            "answer_mode": "auto",
            "query_mode": "deep",
            "_session_owner_email": _current_google_email() or "local",
        }
        scheduler: SourceCodeQAQueryScheduler = current_app.config["SOURCE_CODE_QA_QUERY_SCHEDULER"]
        scheduler.submit(
            app=app_obj,
            job_id=job.job_id,
            payload=assessment_payload,
            owner_email=assessment_payload["_session_owner_email"],
            runner=_run_source_code_qa_effort_assessment_job,
        )
        snapshot = _public_source_code_qa_job_snapshot(job_store.snapshot(job.job_id) or {})
        return jsonify({**snapshot, "status": "queued", "job_id": job.job_id})

    @app.get("/api/source-code-qa/effort-assessment/latest")
    def source_code_qa_effort_assessment_latest_api():
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        result = current_app.config["JOB_STORE"].latest_completed_result("source-code-qa-effort-assessment")
        if not result:
            return jsonify({"status": "empty", "result": {}})
        return jsonify({"status": "ok", "result": result})

    @app.post("/api/source-code-qa/feedback")
    def source_code_qa_feedback_api():
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            result = _build_source_code_qa_service().save_feedback(
                user_email=_current_google_email() or "",
                payload=payload,
            )
            return jsonify(result)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST
    @app.get("/api/source-code-qa/query-jobs/<job_id>")
    def source_code_qa_query_job_api(job_id: str):
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        snapshot = _source_code_qa_job_snapshot_for_current_user(job_id)
        if snapshot is None:
            return jsonify(
                {
                    "status": "error",
                    "message": "Source Code Q&A job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }
            ), HTTPStatus.NOT_FOUND
        return jsonify(_public_source_code_qa_job_snapshot(snapshot))

    @app.get("/api/source-code-qa/query-jobs/<job_id>/events")
    def source_code_qa_query_job_events_api(job_id: str):
        access_gate = _require_source_code_qa_chat_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _source_code_qa_job_snapshot_for_current_user(job_id) is None:
            return jsonify(
                {
                    "status": "error",
                    "message": "Source Code Q&A job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }
            ), HTTPStatus.NOT_FOUND

        def event_stream():
            last_payload = ""
            deadline = time.time() + 900
            while time.time() < deadline:
                snapshot = _source_code_qa_job_snapshot_for_current_user(job_id)
                if snapshot is None:
                    payload = {
                        "status": "error",
                        "state": "failed",
                        "message": "Source Code Q&A job was not found.",
                        "error": "Source Code Q&A job was not found.",
                        "error_category": "job_not_found",
                        "error_code": "job_not_found",
                        "error_retryable": False,
                    }
                    yield f"event: failed\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    return
                payload = _public_source_code_qa_job_snapshot(snapshot)
                payload_text = json.dumps(payload, ensure_ascii=False)
                if payload_text != last_payload:
                    event_name = "message"
                    if payload.get("state") == "completed":
                        event_name = "completed"
                    elif payload.get("state") == "failed":
                        event_name = "failed"
                    yield f"event: {event_name}\ndata: {payload_text}\n\n"
                    last_payload = payload_text
                    if payload.get("state") in {"completed", "failed"}:
                        return
                else:
                    yield ": keepalive\n\n"
                time.sleep(0.9)

        return Response(
            stream_with_context(event_stream()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/api/source-code-qa/effort-assessment-jobs/<job_id>")
    def source_code_qa_effort_assessment_job_api(job_id: str):
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
        if snapshot is None or snapshot.get("action") != "source-code-qa-effort-assessment":
            return jsonify(
                {
                    "status": "error",
                    "message": "Source Code Q&A effort assessment job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }
            ), HTTPStatus.NOT_FOUND
        return jsonify(_public_source_code_qa_job_snapshot(snapshot))

    @app.get("/api/source-code-qa/effort-assessment-jobs/<job_id>/events")
    def source_code_qa_effort_assessment_job_events_api(job_id: str):
        access_gate = _require_source_code_qa_manage_access(settings, api=True)
        if access_gate is not None:
            return access_gate

        def event_stream():
            last_payload = ""
            deadline = time.time() + 900
            while time.time() < deadline:
                snapshot = current_app.config["JOB_STORE"].snapshot(job_id)
                if snapshot is None or snapshot.get("action") != "source-code-qa-effort-assessment":
                    payload = {
                        "status": "error",
                        "state": "failed",
                        "message": "Source Code Q&A effort assessment job was not found.",
                        "error": "Source Code Q&A effort assessment job was not found.",
                        "error_category": "job_not_found",
                        "error_code": "job_not_found",
                        "error_retryable": False,
                    }
                    yield f"event: failed\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    return
                payload = _public_source_code_qa_job_snapshot(snapshot)
                payload_text = json.dumps(payload, ensure_ascii=False)
                if payload_text != last_payload:
                    event_name = "message"
                    if payload.get("state") == "completed":
                        event_name = "completed"
                    elif payload.get("state") == "failed":
                        event_name = "failed"
                    yield f"event: {event_name}\ndata: {payload_text}\n\n"
                    last_payload = payload_text
                    if payload.get("state") in {"completed", "failed"}:
                        return
                else:
                    yield ": keepalive\n\n"
                time.sleep(0.9)

        return Response(
            stream_with_context(event_stream()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
