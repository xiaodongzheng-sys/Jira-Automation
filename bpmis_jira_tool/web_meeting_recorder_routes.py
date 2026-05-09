"""Meeting Recorder and Meeting Translation route handlers."""
from __future__ import annotations

import logging
from http import HTTPStatus
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

from flask import Response, current_app, jsonify, render_template, request, send_file, stream_with_context
import requests

from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.meeting_recorder import (
    CALENDAR_READONLY_SCOPE,
    meeting_platform_from_link,
    normalize_meeting_transcript_language,
)
from bpmis_jira_tool.meeting_translation import MEETING_TRANSLATION_LANGUAGES


def _add_route(app: Any, rule: str, view_func: Callable[..., Any], *, methods: list[str] | None = None) -> None:
    app.add_url_rule(rule, endpoint=view_func.__name__, view_func=view_func, methods=methods)


def build_meeting_recorder_handlers(ctx: Any) -> Any:
    settings = ctx.settings
    _require_meeting_recorder_access = ctx._require_meeting_recorder_access
    _get_user_identity = ctx._get_user_identity
    _google_credentials_have_scopes = ctx._google_credentials_have_scopes
    _current_release_revision = ctx._current_release_revision
    _current_google_email = ctx._current_google_email
    _local_agent_meeting_recorder_enabled = ctx._local_agent_meeting_recorder_enabled
    _log_portal_event = ctx._log_portal_event
    _build_local_agent_client = ctx._build_local_agent_client
    _get_meeting_translation_runtime = ctx._get_meeting_translation_runtime
    _meeting_translation_sse_events = ctx._meeting_translation_sse_events
    _get_meeting_recorder_runtime = ctx._get_meeting_recorder_runtime
    _build_request_log_context = ctx._build_request_log_context
    _classify_portal_error = ctx._classify_portal_error
    _build_calendar_meeting_service = ctx._build_calendar_meeting_service
    _get_meeting_record_store = ctx._get_meeting_record_store
    _meeting_record_summary = ctx._meeting_record_summary
    _meeting_recorder_auto_process_payload = ctx._meeting_recorder_auto_process_payload
    _queue_meeting_recorder_process_job = ctx._queue_meeting_recorder_process_job
    _meeting_recorder_process_job_snapshot_for_current_user = ctx._meeting_recorder_process_job_snapshot_for_current_user
    _public_meeting_recorder_process_job_snapshot = ctx._public_meeting_recorder_process_job_snapshot
    _build_meeting_processing_service = ctx._build_meeting_processing_service
    MEETING_RECORDER_UPCOMING_DISPLAY_LIMIT = ctx.MEETING_RECORDER_UPCOMING_DISPLAY_LIMIT

    def meeting_recorder_page():
        access_gate = _require_meeting_recorder_access(settings)
        if access_gate is not None:
            return access_gate
        user_identity = _get_user_identity(settings)
        return render_template(
            "meeting_recorder.html",
            page_title="Meeting Recorder",
            user_identity=user_identity,
            calendar_connected=_google_credentials_have_scopes(CALENDAR_READONLY_SCOPE),
            gmail_send_connected=_google_credentials_have_scopes("https://www.googleapis.com/auth/gmail.send"),
            selected_record_id=str(request.args.get("record") or "").strip(),
            asset_revision=_current_release_revision(),
        )


    def meeting_translation_page():
        access_gate = _require_meeting_recorder_access(settings)
        if access_gate is not None:
            return access_gate
        return render_template(
            "meeting_translation.html",
            page_title="Meeting Translation",
            languages=MEETING_TRANSLATION_LANGUAGES,
            default_language="en",
            asset_revision=_current_release_revision(),
        )


    def meeting_translation_start_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        owner_email = _current_google_email()
        using_local_agent = _local_agent_meeting_recorder_enabled(settings)
        _log_portal_event(
            "meeting_translation_start_requested",
            level=logging.WARNING,
            target_language=str(payload.get("target_language") or ""),
            using_local_agent=using_local_agent,
        )
        try:
            if using_local_agent:
                result = _build_local_agent_client(settings).meeting_translation_start(
                    {
                        "owner_email": owner_email,
                        "target_language": payload.get("target_language"),
                    }
                )
            else:
                result = _get_meeting_translation_runtime().start_session(
                    owner_email=owner_email,
                    target_language=payload.get("target_language"),
                )
        except ToolError as error:
            status = HTTPStatus.BAD_GATEWAY if "local-agent" in str(error).lower() or "unavailable" in str(error).lower() else HTTPStatus.BAD_REQUEST
            _log_portal_event(
                "meeting_translation_start_failed",
                level=logging.WARNING,
                status_code=int(status),
                target_language=str(payload.get("target_language") or ""),
                using_local_agent=using_local_agent,
                error_type=type(error).__name__,
                error_message=str(error)[:500],
            )
            return jsonify({"status": "error", "message": str(error)}), status
        _log_portal_event(
            "meeting_translation_start_completed",
            level=logging.WARNING,
            session_id=str((result.get("session") or {}).get("session_id") or "")[:8] if isinstance(result, dict) else "",
            session_status=str((result.get("session") or {}).get("status") or "") if isinstance(result, dict) else "",
            target_language=str((result.get("session") or {}).get("target_language") or "") if isinstance(result, dict) else "",
            using_local_agent=using_local_agent,
        )
        return jsonify(result)


    def meeting_translation_stop_api(session_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email()
        using_local_agent = _local_agent_meeting_recorder_enabled(settings)
        _log_portal_event(
            "meeting_translation_stop_requested",
            level=logging.WARNING,
            session_id=str(session_id or "")[:8],
            using_local_agent=using_local_agent,
        )
        try:
            if using_local_agent:
                result = _build_local_agent_client(settings).meeting_translation_stop(
                    session_id=session_id,
                    owner_email=owner_email,
                )
            else:
                result = _get_meeting_translation_runtime().stop_session(session_id=session_id, owner_email=owner_email)
        except ToolError as error:
            status = HTTPStatus.BAD_GATEWAY if "local-agent" in str(error).lower() or "unavailable" in str(error).lower() else HTTPStatus.NOT_FOUND
            _log_portal_event(
                "meeting_translation_stop_failed",
                level=logging.WARNING,
                status_code=int(status),
                session_id=str(session_id or "")[:8],
                using_local_agent=using_local_agent,
                error_type=type(error).__name__,
                error_message=str(error)[:500],
            )
            return jsonify({"status": "error", "message": str(error)}), status
        _log_portal_event(
            "meeting_translation_stop_completed",
            level=logging.WARNING,
            session_id=str(session_id or "")[:8],
            session_status=str((result.get("session") or {}).get("status") or "") if isinstance(result, dict) else "",
            using_local_agent=using_local_agent,
        )
        return jsonify(result)


    def meeting_translation_events_api(session_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email()
        using_local_agent = _local_agent_meeting_recorder_enabled(settings)
        _log_portal_event(
            "meeting_translation_events_requested",
            level=logging.WARNING,
            session_id=str(session_id or "")[:8],
            using_local_agent=using_local_agent,
        )
        if using_local_agent:
            try:
                upstream = _build_local_agent_client(settings).meeting_translation_events_response(
                    session_id=session_id,
                    owner_email=owner_email,
                )
            except ToolError as error:
                _log_portal_event(
                    "meeting_translation_events_failed",
                    level=logging.WARNING,
                    session_id=str(session_id or "")[:8],
                    using_local_agent=using_local_agent,
                    error_type=type(error).__name__,
                    error_message=str(error)[:500],
                )
                return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_GATEWAY
            _log_portal_event(
                "meeting_translation_events_proxy_opened",
                level=logging.WARNING,
                session_id=str(session_id or "")[:8],
                upstream_status_code=int(upstream.status_code),
                upstream_content_type=str(upstream.headers.get("Content-Type") or ""),
            )
            return Response(
                stream_with_context(upstream.iter_content(chunk_size=None)),
                status=upstream.status_code,
                content_type=upstream.headers.get("Content-Type") or "text/event-stream",
            )
        try:
            event_iter = _get_meeting_translation_runtime().event_stream(session_id=session_id, owner_email=owner_email)
        except ToolError as error:
            _log_portal_event(
                "meeting_translation_events_failed",
                level=logging.WARNING,
                session_id=str(session_id or "")[:8],
                using_local_agent=using_local_agent,
                error_type=type(error).__name__,
                error_message=str(error)[:500],
            )
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.NOT_FOUND
        return Response(_meeting_translation_sse_events(event_iter), mimetype="text/event-stream")


    def meeting_recorder_diagnostics_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_meeting_recorder_enabled(settings):
            try:
                return jsonify({"status": "ok", **_build_local_agent_client(settings).meeting_recorder_diagnostics()})
            except ToolError as error:
                return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_GATEWAY
        return jsonify({"status": "ok", **_get_meeting_recorder_runtime().diagnostics()})


    def meeting_recorder_upcoming_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if not _google_credentials_have_scopes(CALENDAR_READONLY_SCOPE):
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Google Calendar access is not available yet. Sign in with Google again to grant calendar read access.",
                    }
                ),
                HTTPStatus.BAD_REQUEST,
            )
        try:
            meetings = _build_calendar_meeting_service().upcoming_meetings()
            return jsonify({"status": "ok", "meetings": meetings[:MEETING_RECORDER_UPCOMING_DISPLAY_LIMIT]})
        except Exception as error:  # noqa: BLE001
            _log_portal_event(
                "meeting_recorder_calendar_unexpected_error",
                level=logging.ERROR,
                **_build_request_log_context(settings, user_identity=_get_user_identity(settings), extra=_classify_portal_error(error)),
            )
            current_app.logger.exception("Meeting Recorder calendar load failed.")
            return jsonify({"status": "error", "message": "Upcoming meetings could not be loaded right now."}), HTTPStatus.INTERNAL_SERVER_ERROR


    def meeting_recorder_records_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        if _local_agent_meeting_recorder_enabled(settings):
            try:
                records = _build_local_agent_client(settings).meeting_recorder_records(owner_email=_current_google_email())
                return jsonify({"status": "ok", "records": records})
            except ToolError as error:
                return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_GATEWAY
        records = _get_meeting_record_store().list_records(owner_email=_current_google_email())
        return jsonify({"status": "ok", "records": [_meeting_record_summary(record) for record in records]})


    def meeting_recorder_record_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                record_payload = _build_local_agent_client(settings).meeting_recorder_record(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({"status": "ok", "record": record_payload.get("record") or {}})
            record = _get_meeting_record_store().get_record(record_id)
            if str(record.get("owner_email") or "").strip().lower() != _current_google_email():
                return jsonify({"status": "error", "message": "Meeting record is not available for this Google account."}), HTTPStatus.FORBIDDEN
            return jsonify({"status": "ok", "record": record})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_start_api():
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            meeting_link = str(payload.get("meeting_link") or payload.get("meetingLink") or "").strip()
            recording_mode = str(payload.get("recording_mode") or payload.get("recordingMode") or "").strip()
            transcript_language = normalize_meeting_transcript_language(payload.get("transcript_language") or payload.get("transcriptLanguage"))
            if not recording_mode:
                recording_mode = "audio_only"
            if _local_agent_meeting_recorder_enabled(settings):
                remote_payload = dict(payload)
                remote_payload.update(
                    {
                        "owner_email": _current_google_email(),
                        "meeting_link": meeting_link,
                        "recording_mode": recording_mode,
                        "transcript_language": transcript_language,
                        "platform": str(payload.get("platform") or meeting_platform_from_link(meeting_link) or "unknown").strip(),
                    }
                )
                result = _build_local_agent_client(settings).meeting_recorder_start(remote_payload)
                return jsonify({"status": "ok", "record": result.get("record") or {}})
            record = _get_meeting_recorder_runtime().start_recording(
                owner_email=_current_google_email(),
                title=str(payload.get("title") or "Untitled meeting").strip(),
                platform=str(payload.get("platform") or meeting_platform_from_link(meeting_link) or "unknown").strip(),
                meeting_link=meeting_link,
                recording_mode=recording_mode,
                calendar_event_id=str(payload.get("calendar_event_id") or payload.get("calendarEventId") or "").strip(),
                scheduled_start=str(payload.get("scheduled_start") or payload.get("scheduledStart") or "").strip(),
                scheduled_end=str(payload.get("scheduled_end") or payload.get("scheduledEnd") or "").strip(),
                attendees=payload.get("attendees") if isinstance(payload.get("attendees"), list) else [],
                transcript_language=transcript_language,
            )
            return jsonify({"status": "ok", "record": _meeting_record_summary(record)})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_stop_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        owner_email = _current_google_email()
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_stop(
                    record_id=record_id,
                    owner_email=owner_email,
                )
                process_payload = _meeting_recorder_auto_process_payload(
                    settings=settings,
                    record_id=record_id,
                    owner_email=owner_email,
                )
                return jsonify({"status": "ok", "record": result.get("record") or {}, **process_payload})
            record = _get_meeting_recorder_runtime().stop_recording(record_id=record_id, owner_email=owner_email)
            process_payload = _meeting_recorder_auto_process_payload(
                settings=settings,
                record_id=record_id,
                owner_email=owner_email,
            )
            return jsonify({"status": "ok", "record": _meeting_record_summary(record), **process_payload})
        except (ToolError, requests.RequestException) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_signal_check_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_signal_check(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({"status": "ok", "record": result.get("record") or {}})
            record = _get_meeting_recorder_runtime().check_recording_signal(record_id=record_id, owner_email=_current_google_email())
            return jsonify({"status": "ok", "record": _meeting_record_summary(record)})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_process_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_process_start(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({
                    "status": "queued",
                    "state": result.get("state") or "queued",
                    "job_id": result.get("job_id") or "",
                    "record": result.get("record") or {},
                })
            payload = _queue_meeting_recorder_process_job(
                app=current_app._get_current_object(),
                settings=settings,
                record_id=record_id,
                owner_email=_current_google_email(),
            )
            return jsonify(payload)
        except (ConfigError, ToolError, requests.RequestException) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_process_job_api(job_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                snapshot = _build_local_agent_client(settings).meeting_recorder_process_job(
                    job_id=job_id,
                    owner_email=_current_google_email(),
                )
                return jsonify(_public_meeting_recorder_process_job_snapshot(snapshot))
            snapshot = _meeting_recorder_process_job_snapshot_for_current_user(job_id)
            if snapshot is None:
                return jsonify({
                    "status": "error",
                    "message": "Meeting Recorder process job was not found.",
                    "error_category": "job_not_found",
                    "error_code": "job_not_found",
                    "error_retryable": False,
                }), HTTPStatus.NOT_FOUND
            return jsonify(_public_meeting_recorder_process_job_snapshot(snapshot))
        except (ConfigError, ToolError, requests.RequestException) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_send_email_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        payload = request.get_json(silent=True) or {}
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                result = _build_local_agent_client(settings).meeting_recorder_send_email(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                    recipient=str(payload.get("recipient") or "").strip() or _current_google_email(),
                )
                return jsonify({"status": "ok", "email": result.get("email") or {}})
            email_payload = _build_meeting_processing_service(settings).send_minutes_email(
                record_id=record_id,
                owner_email=_current_google_email(),
                recipient=str(payload.get("recipient") or "").strip() or _current_google_email(),
            )
            return jsonify({"status": "ok", "email": email_payload})
        except (ConfigError, ToolError) as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_delete_api(record_id: str):
        access_gate = _require_meeting_recorder_access(settings, api=True)
        if access_gate is not None:
            return access_gate
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                _build_local_agent_client(settings).meeting_recorder_delete(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                )
                return jsonify({"status": "ok"})
            _get_meeting_record_store().delete_record(record_id=record_id, owner_email=_current_google_email())
            return jsonify({"status": "ok"})
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST


    def meeting_recorder_asset(record_id: str, relative_path: str):
        access_gate = _require_meeting_recorder_access(settings)
        if access_gate is not None:
            return access_gate
        as_download = str(request.args.get("download") or "").strip().lower() in {"1", "true", "yes"}
        try:
            if _local_agent_meeting_recorder_enabled(settings):
                upstream = _build_local_agent_client(settings).meeting_recorder_asset_response(
                    record_id=record_id,
                    owner_email=_current_google_email(),
                    relative_path=relative_path,
                    range_header=str(request.headers.get("Range") or ""),
                    method=request.method,
                    download=as_download,
                )
                content_type = str(upstream.headers.get("Content-Type") or "")
                if as_download and "text/html" in content_type.lower():
                    upstream.close()
                    return jsonify({
                        "status": "error",
                        "message": "Meeting audio download returned an HTML response instead of the requested file. Refresh the page and sign in again, then retry.",
                    }), HTTPStatus.BAD_GATEWAY
                excluded_headers = {"content-encoding", "connection", "transfer-encoding"}
                headers = [
                    (key, value)
                    for key, value in upstream.headers.items()
                    if key.lower() not in excluded_headers and (not as_download or key.lower() != "content-disposition")
                ]
                if as_download:
                    filename = Path(upstream.headers.get("X-Meeting-Recorder-Filename") or relative_path).name or "meeting-recording.mp4"
                    headers.append(("Content-Disposition", f'attachment; filename="{filename}"'))
                if request.method == "HEAD":
                    upstream.close()
                    return Response(status=upstream.status_code, headers=headers)

                def stream_upstream():
                    try:
                        for chunk in upstream.iter_content(chunk_size=1024 * 256):
                            if chunk:
                                yield chunk
                    finally:
                        upstream.close()

                return Response(stream_upstream(), status=upstream.status_code, headers=headers, direct_passthrough=True)
            record = _get_meeting_record_store().get_record(record_id)
            if str(record.get("owner_email") or "").strip().lower() != _current_google_email():
                return jsonify({"status": "error", "message": "Meeting record is not available for this Google account."}), HTTPStatus.FORBIDDEN
            root_dir = _get_meeting_record_store().record_dir(record_id).resolve()
            asset_path = (root_dir / relative_path).resolve()
            if root_dir not in asset_path.parents and asset_path != root_dir:
                return jsonify({"status": "error", "message": "Invalid meeting asset path."}), HTTPStatus.BAD_REQUEST
            if not asset_path.exists():
                return jsonify({"status": "error", "message": "Meeting asset not found."}), HTTPStatus.NOT_FOUND
            media = record.get("media") if isinstance(record.get("media"), dict) else {}
            active_media_paths = [str(media.get("audio_path") or "").strip()]
            active_asset_paths = {
                (_get_meeting_record_store().root_dir / media_path).resolve()
                for media_path in active_media_paths
                if media_path
            }
            active_asset_paths.update(
                (root_dir / Path(media_path).name).resolve()
                for media_path in active_media_paths
                if media_path
            )
            if str(record.get("status") or "") == "recording" and asset_path in active_asset_paths:
                return jsonify({
                    "status": "error",
                    "message": "Stop the recording before downloading the meeting media file.",
                }), HTTPStatus.CONFLICT
            return send_file(asset_path, conditional=True, as_attachment=as_download, download_name=asset_path.name)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), HTTPStatus.BAD_REQUEST

    return SimpleNamespace(
        meeting_recorder_page=meeting_recorder_page,
        meeting_translation_page=meeting_translation_page,
        meeting_translation_start_api=meeting_translation_start_api,
        meeting_translation_stop_api=meeting_translation_stop_api,
        meeting_translation_events_api=meeting_translation_events_api,
        meeting_recorder_diagnostics_api=meeting_recorder_diagnostics_api,
        meeting_recorder_upcoming_api=meeting_recorder_upcoming_api,
        meeting_recorder_records_api=meeting_recorder_records_api,
        meeting_recorder_record_api=meeting_recorder_record_api,
        meeting_recorder_start_api=meeting_recorder_start_api,
        meeting_recorder_stop_api=meeting_recorder_stop_api,
        meeting_recorder_signal_check_api=meeting_recorder_signal_check_api,
        meeting_recorder_process_api=meeting_recorder_process_api,
        meeting_recorder_process_job_api=meeting_recorder_process_job_api,
        meeting_recorder_send_email_api=meeting_recorder_send_email_api,
        meeting_recorder_delete_api=meeting_recorder_delete_api,
        meeting_recorder_asset=meeting_recorder_asset,
    )


def register_meeting_recorder_routes(app: Any, handlers: Any) -> None:
    _add_route(app, "/meeting-recorder", handlers.meeting_recorder_page)
    _add_route(app, "/meeting-translation", handlers.meeting_translation_page)
    _add_route(app, "/api/meeting-translation/start", handlers.meeting_translation_start_api, methods=["POST"])
    _add_route(app, "/api/meeting-translation/sessions/<session_id>/stop", handlers.meeting_translation_stop_api, methods=["POST"])
    _add_route(app, "/api/meeting-translation/sessions/<session_id>/events", handlers.meeting_translation_events_api)
    _add_route(app, "/api/meeting-recorder/diagnostics", handlers.meeting_recorder_diagnostics_api)
    _add_route(app, "/api/meeting-recorder/calendar/upcoming", handlers.meeting_recorder_upcoming_api)
    _add_route(app, "/api/meeting-recorder/records", handlers.meeting_recorder_records_api)
    _add_route(app, "/api/meeting-recorder/records/<record_id>", handlers.meeting_recorder_record_api)
    _add_route(app, "/api/meeting-recorder/start", handlers.meeting_recorder_start_api, methods=["POST"])
    _add_route(app, "/api/meeting-recorder/records/<record_id>/stop", handlers.meeting_recorder_stop_api, methods=["POST"])
    _add_route(app, "/api/meeting-recorder/records/<record_id>/signal-check", handlers.meeting_recorder_signal_check_api, methods=["POST"])
    _add_route(app, "/api/meeting-recorder/records/<record_id>/process", handlers.meeting_recorder_process_api, methods=["POST"])
    _add_route(app, "/api/meeting-recorder/process-jobs/<job_id>", handlers.meeting_recorder_process_job_api)
    _add_route(app, "/api/meeting-recorder/records/<record_id>/send-email", handlers.meeting_recorder_send_email_api, methods=["POST"])
    _add_route(app, "/api/meeting-recorder/records/<record_id>", handlers.meeting_recorder_delete_api, methods=["DELETE"])
    _add_route(app, "/meeting-recorder/assets/<record_id>/<path:relative_path>", handlers.meeting_recorder_asset, methods=["GET", "HEAD"])
