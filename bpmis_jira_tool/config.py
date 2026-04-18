from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv


DEFAULT_SPREADSHEET_ID = "1KKlqDosv2QjCZrY8If-JreuVa_ALHZUIrxhMbAa_y_Q"


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str) -> tuple[str, ...]:
    value = os.getenv(name, "").strip()
    if not value:
        return ()
    return tuple(
        item.strip().lower()
        for item in value.split(",")
        if item.strip()
    )


def _env_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value or default


@dataclass(frozen=True)
class Settings:
    flask_secret_key: str
    google_oauth_client_secret_file: Path
    google_oauth_redirect_uri: str | None
    team_portal_host: str
    team_portal_port: int
    team_portal_base_url: str | None
    team_allowed_emails: tuple[str, ...]
    team_allowed_email_domains: tuple[str, ...]
    team_portal_data_dir: Path
    spreadsheet_id: str
    common_tab_name: str
    input_tab_name: str
    bpmis_base_url: str
    bpmis_api_access_token: str | None
    prd_briefing_owner_email: str = "xiaodong.zheng@npt.sg"
    team_portal_config_encryption_key: str | None = None
    confluence_email: str | None = None
    confluence_api_token: str | None = None
    confluence_bearer_token: str | None = None
    confluence_base_url: str | None = None
    openai_api_key: str | None = None
    openai_api_base_url: str = "https://api.openai.com/v1"
    prd_briefing_text_model: str = "gpt-4.1-mini"
    gemini_api_key: str | None = None
    gemini_api_base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    prd_briefing_gemini_text_model: str = "gemini-2.5-flash"
    prd_briefing_text_provider_priority: str = "gemini_first"
    prd_briefing_embedding_model: str = "text-embedding-3-large"
    prd_briefing_transcription_model: str = "gpt-4o-mini-transcribe"
    prd_briefing_tts_model: str = "gpt-4o-mini-tts"
    prd_briefing_openai_default_voice: str = "alloy"
    prd_briefing_openai_english_voice: str = "coral"
    prd_briefing_openai_mandarin_voice: str = "sage"
    prd_briefing_openai_voice_speed: float = 0.96
    prd_briefing_openai_custom_voice_enabled: bool = False
    prd_briefing_answer_audio_enabled: bool = False
    elevenlabs_api_key: str | None = None
    elevenlabs_tts_model_id: str = "eleven_flash_v2_5"
    elevenlabs_english_model_id: str = "eleven_flash_v2_5"
    elevenlabs_mandarin_model_id: str = "eleven_multilingual_v2"
    elevenlabs_default_voice_id: str = "JBFqnCBsd6RMkjVDRZzb"
    elevenlabs_english_voice_id: str = "21m00Tcm4TlvDq8ikWAM"
    elevenlabs_mandarin_voice_id: str = "JBFqnCBsd6RMkjVDRZzb"

    @classmethod
    def from_env(cls) -> "Settings":
        dotenv_path = find_dotenv(usecwd=True)
        if dotenv_path:
            load_dotenv(dotenv_path, override=False)
        client_secret = _env_str("GOOGLE_OAUTH_CLIENT_SECRET_FILE", "")
        if not client_secret:
            client_secret = "google-client-secret.json"

        return cls(
            flask_secret_key=_env_str("FLASK_SECRET_KEY", "dev-secret-key"),
            google_oauth_client_secret_file=Path(client_secret),
            google_oauth_redirect_uri=_env_str("GOOGLE_OAUTH_REDIRECT_URI"),
            team_portal_host=_env_str("TEAM_PORTAL_HOST", "127.0.0.1"),
            team_portal_port=int(_env_str("TEAM_PORTAL_PORT", "5000")),
            team_portal_base_url=_env_str("TEAM_PORTAL_BASE_URL"),
            team_allowed_emails=_env_csv("TEAM_ALLOWED_EMAILS"),
            team_allowed_email_domains=_env_csv("TEAM_ALLOWED_EMAIL_DOMAINS"),
            team_portal_data_dir=Path(_env_str("TEAM_PORTAL_DATA_DIR", ".")),
            prd_briefing_owner_email=_env_str("PRD_BRIEFING_OWNER_EMAIL", "xiaodong.zheng@npt.sg"),
            team_portal_config_encryption_key=_env_str("TEAM_PORTAL_CONFIG_ENCRYPTION_KEY"),
            spreadsheet_id=_env_str("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
            common_tab_name=_env_str("COMMON_TAB_NAME", "Common"),
            input_tab_name=_env_str("INPUT_TAB_NAME", "Projects"),
            bpmis_base_url=_env_str("BPMIS_BASE_URL", "https://bpmis-uat1.uat.npt.seabank.io"),
            bpmis_api_access_token=_env_str("BPMIS_API_ACCESS_TOKEN"),
            confluence_email=_env_str("CONFLUENCE_EMAIL"),
            confluence_api_token=_env_str("CONFLUENCE_API_TOKEN"),
            confluence_bearer_token=_env_str("CONFLUENCE_BEARER_TOKEN"),
            confluence_base_url=_env_str("CONFLUENCE_BASE_URL"),
            openai_api_key=_env_str("OPENAI_API_KEY"),
            openai_api_base_url=_env_str("OPENAI_API_BASE_URL", "https://api.openai.com/v1"),
            prd_briefing_text_model=_env_str("PRD_BRIEFING_TEXT_MODEL", "gpt-4.1-mini"),
            gemini_api_key=_env_str("GEMINI_API_KEY"),
            gemini_api_base_url=_env_str("GEMINI_API_BASE_URL", "https://generativelanguage.googleapis.com/v1beta"),
            prd_briefing_gemini_text_model=_env_str("PRD_BRIEFING_GEMINI_TEXT_MODEL", "gemini-2.5-flash"),
            prd_briefing_text_provider_priority=_env_str("PRD_BRIEFING_TEXT_PROVIDER_PRIORITY", "gemini_first"),
            prd_briefing_embedding_model=_env_str("PRD_BRIEFING_EMBEDDING_MODEL", "text-embedding-3-large"),
            prd_briefing_transcription_model=_env_str("PRD_BRIEFING_TRANSCRIPTION_MODEL", "gpt-4o-mini-transcribe"),
            prd_briefing_tts_model=_env_str("PRD_BRIEFING_TTS_MODEL", "gpt-4o-mini-tts"),
            prd_briefing_openai_default_voice=_env_str("PRD_BRIEFING_OPENAI_DEFAULT_VOICE", "alloy"),
            prd_briefing_openai_english_voice=_env_str("PRD_BRIEFING_OPENAI_ENGLISH_VOICE", "coral"),
            prd_briefing_openai_mandarin_voice=_env_str("PRD_BRIEFING_OPENAI_MANDARIN_VOICE", "sage"),
            prd_briefing_openai_voice_speed=float(_env_str("PRD_BRIEFING_OPENAI_VOICE_SPEED", "0.96")),
            prd_briefing_openai_custom_voice_enabled=_env_bool("PRD_BRIEFING_OPENAI_CUSTOM_VOICE_ENABLED", False),
            prd_briefing_answer_audio_enabled=_env_bool("PRD_BRIEFING_ANSWER_AUDIO_ENABLED", False),
            elevenlabs_api_key=_env_str("ELEVENLABS_API_KEY"),
            elevenlabs_tts_model_id=_env_str("ELEVENLABS_TTS_MODEL_ID", "eleven_flash_v2_5"),
            elevenlabs_english_model_id=_env_str("ELEVENLABS_ENGLISH_MODEL_ID", _env_str("ELEVENLABS_TTS_MODEL_ID", "eleven_flash_v2_5")),
            elevenlabs_mandarin_model_id=_env_str("ELEVENLABS_MANDARIN_MODEL_ID", "eleven_multilingual_v2"),
            elevenlabs_default_voice_id=_env_str("ELEVENLABS_DEFAULT_VOICE_ID", "JBFqnCBsd6RMkjVDRZzb"),
            elevenlabs_english_voice_id=_env_str("ELEVENLABS_ENGLISH_VOICE_ID", "21m00Tcm4TlvDq8ikWAM"),
            elevenlabs_mandarin_voice_id=_env_str("ELEVENLABS_MANDARIN_VOICE_ID", "JBFqnCBsd6RMkjVDRZzb"),
        )
