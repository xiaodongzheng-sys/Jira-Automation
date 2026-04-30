from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from flask import Blueprint, Response, current_app, flash, jsonify, redirect, render_template, request, send_file, url_for

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_client import LocalAgentClient

from .confluence import ConfluenceConnector
from .openai_client import OpenAIClient
from .reviewer import PRDBriefingReviewRequest, PRDReviewService
from .service import PRDBriefingService, VoiceService
from .storage import BriefingStore
from .text_generation import CodexTextGenerationClient


def create_prd_briefing_blueprint() -> Blueprint:
    blueprint = Blueprint("prd_briefing", __name__, url_prefix="/prd-briefing")

    @blueprint.before_request
    def enforce_owner_access():
        if request.endpoint == "prd_briefing.image_proxy":
            return None
        identity = current_app.config["GET_USER_IDENTITY"]()
        email = str(identity.get("email") or "").strip().lower()
        allowed = bool(current_app.config["CAN_ACCESS_PRD_BRIEFING"]())
        if allowed:
            return None

        if request.path.startswith("/prd-briefing/api/"):
            if not email:
                return jsonify({"status": "error", "message": "Sign in with your NPT Google account first."}), 401
            return jsonify({"status": "error", "message": "PRD Briefing Tool is available to signed-in npt.sg users and the configured test account."}), 403

        if not email:
            return redirect(url_for("index"))
        flash("PRD Briefing Tool is available to signed-in npt.sg users and the configured test account.", "error")
        return redirect(url_for("index"))

    @blueprint.get("/")
    def portal() -> str:
        return render_template("prd_briefing.html", page_title="PRD Briefing Tool")

    @blueprint.post("/api/session")
    def create_session():
        try:
            owner_key = current_app.config["GET_USER_IDENTITY"]()["config_key"]
            payload = request.get_json(force=True)
            service = _build_service()
            data = service.create_session(
                owner_key=owner_key,
                page_ref=str(payload.get("page_ref") or ""),
                mode=str(payload.get("mode") or "walkthrough"),
                language=str(payload.get("language") or "zh"),
            )
            return jsonify(data)
        except Exception as error:  # noqa: BLE001
            return jsonify({"status": "error", "message": str(error)}), 400

    @blueprint.post("/api/review")
    def review_prd():
        try:
            owner_key = current_app.config["GET_USER_IDENTITY"]()["config_key"]
            payload = request.get_json(force=True)
            review_payload = {
                "owner_key": owner_key,
                "prd_url": str(payload.get("prd_url") or payload.get("page_ref") or ""),
                "language": str(payload.get("language") or "zh"),
                "force_refresh": bool(payload.get("force_refresh")),
            }
            settings = current_app.config["SETTINGS"]
            if _local_agent_source_code_qa_enabled(settings):
                data = _build_local_agent_client(settings).prd_briefing_review(review_payload)
            else:
                data = _build_prd_review_service().review_url(PRDBriefingReviewRequest(**review_payload))
            return jsonify(data)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), 400
        except Exception as error:  # noqa: BLE001
            return jsonify({"status": "error", "message": str(error)}), 400

    @blueprint.post("/api/process-prd")
    def process_prd():
        try:
            owner_key = current_app.config["GET_USER_IDENTITY"]()["config_key"]
            payload = request.get_json(force=True)
            service = _build_service()
            data = service.process_prd_for_presentation(
                owner_key=owner_key,
                page_ref=str(payload.get("page_ref") or payload.get("prd_url") or "").strip(),
                text=str(payload.get("text") or "").strip(),
            )
            return jsonify(data)
        except ToolError as error:
            return jsonify({"status": "error", "message": str(error)}), 400
        except RuntimeError as error:
            return jsonify({"status": "error", "message": str(error)}), 400
        except Exception as error:  # noqa: BLE001
            message = str(error) or "Could not process this PRD."
            if any(token in message.lower() for token in ("unauthorized", "forbidden", "permission", "401", "403")):
                message = "Confluence access failed. Your token may be expired or this page may not be shared with the configured account."
            return jsonify({"status": "error", "message": message}), 400

    @blueprint.post("/api/generate-audio")
    def generate_audio():
        try:
            owner_key = current_app.config["GET_USER_IDENTITY"]()["config_key"]
            payload = request.get_json(force=True)
            chunk = payload.get("chunk") if isinstance(payload.get("chunk"), dict) else payload
            service = _build_service()
            data = service.generate_presentation_audio(
                owner_key=owner_key,
                session_id=str(payload.get("session_id") or payload.get("sessionId") or "").strip(),
                chunk=chunk,
            )
            return jsonify(data)
        except Exception as error:  # noqa: BLE001
            return jsonify({"status": "error", "message": str(error) or "Could not generate audio for this chunk."}), 400

    @blueprint.get("/api/session/<session_id>")
    def get_session(session_id: str):
        try:
            owner_key = current_app.config["GET_USER_IDENTITY"]()["config_key"]
            service = _build_service()
            data = service.get_session_payload(session_id=session_id, owner_key=owner_key)
            return jsonify(data)
        except Exception as error:  # noqa: BLE001
            return jsonify({"status": "error", "message": str(error)}), 400

    @blueprint.post("/api/session/<session_id>/answer")
    def answer_question(session_id: str):
        try:
            owner_key = current_app.config["GET_USER_IDENTITY"]()["config_key"]
            payload = request.get_json(force=True)
            service = _build_service()
            data = service.answer_question(
                session_id=session_id,
                owner_key=owner_key,
                question=str(payload.get("question") or "").strip(),
            )
            return jsonify(data)
        except Exception as error:  # noqa: BLE001
            return jsonify({"status": "error", "message": str(error)}), 400

    @blueprint.post("/api/session/<session_id>/narrate")
    def narrate_section(session_id: str):
        try:
            owner_key = current_app.config["GET_USER_IDENTITY"]()["config_key"]
            payload = request.get_json(force=True)
            service = _build_service()
            data = service.narrate_section(
                session_id=session_id,
                owner_key=owner_key,
                section_index=int(payload.get("section_index") or 0),
                briefing_block_id=str(payload.get("briefing_block_id") or "").strip() or None,
                include_audio=bool(payload.get("include_audio", True)),
            )
            return jsonify(data)
        except Exception as error:  # noqa: BLE001
            return jsonify({"status": "error", "message": str(error)}), 400

    @blueprint.get("/assets/<path:relative_path>")
    def serve_asset(relative_path: str):
        root_dir: Path = current_app.config["PRD_BRIEFING_STORE"].root_dir
        asset_path = (root_dir / relative_path).resolve()
        if root_dir.resolve() not in asset_path.parents and asset_path != root_dir.resolve():
            return jsonify({"status": "error", "message": "Invalid asset path."}), 404
        if not asset_path.exists():
            return jsonify({"status": "error", "message": "Asset not found."}), 404
        return send_file(asset_path)

    @blueprint.get("/image-proxy")
    def image_proxy():
        src = str(request.args.get("src") or "").strip()
        if not src:
            return jsonify({"status": "error", "message": "Missing image source."}), 400
        parsed = urlparse(src)
        if parsed.scheme not in {"http", "https"}:
            return jsonify({"status": "error", "message": "Unsupported image source."}), 400
        service = _build_service()
        if not _is_allowed_confluence_image_source(src, service.confluence.base_url):
            return jsonify({"status": "error", "message": "Unsupported image source."}), 400
        connector = service.confluence
        response = connector._request(src, accept="image/*,*/*;q=0.8")
        return Response(
            response.content,
            status=response.status_code,
            content_type=response.headers.get("content-type", "application/octet-stream"),
            headers={"Cache-Control": "private, max-age=300"},
        )

    return blueprint


def _is_allowed_confluence_image_source(src: str, confluence_base_url: str | None) -> bool:
    parsed = urlparse(src)
    base = urlparse(str(confluence_base_url or ""))
    if parsed.scheme not in {"http", "https"}:
        return False
    if not base.scheme or not base.netloc:
        return False
    if parsed.scheme != base.scheme or parsed.netloc.lower() != base.netloc.lower():
        return False
    path = parsed.path or ""
    return path.startswith(("/download/attachments/", "/download/thumbnails/"))


def _local_agent_mode_enabled(settings: Settings) -> bool:
    mode = (settings.local_agent_mode or "").strip().lower()
    return mode in {"sync", "remote", "cloud_run", "enabled"}


def _local_agent_source_code_qa_enabled(settings: Settings) -> bool:
    return bool(
        _local_agent_mode_enabled(settings)
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
        and settings.local_agent_source_code_qa_enabled
    )


def _build_local_agent_client(settings: Settings) -> LocalAgentClient:
    return LocalAgentClient(
        base_url=settings.local_agent_base_url or "",
        hmac_secret=settings.local_agent_hmac_secret or "",
        timeout_seconds=settings.local_agent_timeout_seconds,
        connect_timeout_seconds=settings.local_agent_connect_timeout_seconds,
    )


def _build_prd_review_service() -> PRDReviewService:
    settings = current_app.config["SETTINGS"]
    store: BriefingStore = current_app.config["PRD_BRIEFING_STORE"]
    confluence = ConfluenceConnector(
        base_url=settings.confluence_base_url,
        email=settings.confluence_email,
        api_token=settings.confluence_api_token,
        bearer_token=settings.confluence_bearer_token,
        store=store,
    )
    return PRDReviewService(
        store=store,
        confluence=confluence,
        settings=settings,
        workspace_root=Path(__file__).resolve().parent.parent,
    )


def _build_service() -> PRDBriefingService:
    settings = current_app.config["SETTINGS"]
    store: BriefingStore = current_app.config["PRD_BRIEFING_STORE"]
    openai_client = OpenAIClient(
        api_key=settings.openai_api_key,
        base_url=settings.openai_api_base_url,
        text_model=settings.prd_briefing_text_model,
        embedding_model=settings.prd_briefing_embedding_model,
        transcription_model=settings.prd_briefing_transcription_model,
        tts_model=settings.prd_briefing_tts_model,
    )
    text_client = CodexTextGenerationClient(
        settings=settings,
        workspace_root=Path(__file__).resolve().parent.parent,
        prompt_mode="prd_briefing_presentation_chunks_codex",
        codex_model=settings.prd_briefing_codex_model,
    )
    confluence = ConfluenceConnector(
        base_url=settings.confluence_base_url,
        email=settings.confluence_email,
        api_token=settings.confluence_api_token,
        bearer_token=settings.confluence_bearer_token,
        store=store,
    )
    voice_service = VoiceService(
        store=store,
        openai_client=openai_client,
        tts_provider=settings.prd_briefing_tts_provider,
        edge_mandarin_voice=settings.prd_briefing_edge_mandarin_voice,
        edge_english_voice=settings.prd_briefing_edge_english_voice,
        edge_rate=settings.prd_briefing_edge_rate,
        openai_mandarin_voice=settings.prd_briefing_openai_mandarin_voice,
        openai_voice_speed=settings.prd_briefing_openai_voice_speed,
        openai_custom_voice_enabled=settings.prd_briefing_openai_custom_voice_enabled,
        openai_tts_fallback_enabled=settings.prd_briefing_openai_tts_fallback_enabled,
        elevenlabs_api_key=settings.elevenlabs_api_key,
        elevenlabs_mandarin_model_id=settings.elevenlabs_mandarin_model_id,
        elevenlabs_mandarin_voice_id=settings.elevenlabs_mandarin_voice_id,
    )
    return PRDBriefingService(
        store=store,
        confluence=confluence,
        openai_client=openai_client,
        text_client=text_client,
        voice_service=voice_service,
        answer_audio_enabled=settings.prd_briefing_answer_audio_enabled,
    )
