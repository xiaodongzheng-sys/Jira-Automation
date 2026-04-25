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
    gmail_seatalk_demo_owner_email: str = "xiaodong.zheng@npt.sg"
    source_code_qa_owner_email: str = "xiaodong.zheng@npt.sg"
    source_code_qa_git_timeout_seconds: int = 90
    source_code_qa_max_file_bytes: int = 500_000
    source_code_qa_gitlab_token: str | None = None
    source_code_qa_gitlab_username: str = "oauth2"
    source_code_qa_llm_provider: str = "codex_cli_bridge"
    source_code_qa_gemini_api_key: str | None = None
    source_code_qa_gemini_api_base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    source_code_qa_openai_api_key: str | None = None
    source_code_qa_openai_api_base_url: str = "https://api.openai.com/v1"
    source_code_qa_openai_model: str = "gpt-4.1-mini"
    source_code_qa_openai_fast_model: str = "gpt-4.1-mini"
    source_code_qa_openai_deep_model: str = "gpt-4.1"
    source_code_qa_openai_fallback_model: str = "gpt-4.1-mini"
    source_code_qa_gemini_model: str = "gemini-2.5-flash"
    source_code_qa_gemini_fast_model: str = "gemini-2.5-flash-lite"
    source_code_qa_gemini_deep_model: str = "gemini-2.5-flash"
    source_code_qa_gemini_fallback_model: str = "gemini-2.5-flash-lite"
    source_code_qa_vertex_credentials_file: str | None = None
    source_code_qa_vertex_project_id: str | None = None
    source_code_qa_vertex_location: str = "global"
    source_code_qa_vertex_model: str = "gemini-2.5-flash"
    source_code_qa_vertex_fast_model: str = "gemini-2.5-flash-lite"
    source_code_qa_vertex_deep_model: str = "gemini-2.5-flash"
    source_code_qa_vertex_fallback_model: str = "gemini-2.5-flash-lite"
    source_code_qa_query_rewrite_model: str | None = None
    source_code_qa_planner_model: str | None = None
    source_code_qa_answer_model: str | None = None
    source_code_qa_judge_model: str | None = None
    source_code_qa_repair_model: str | None = None
    source_code_qa_llm_judge_enabled: bool = True
    source_code_qa_embedding_model: str = "local-token-hybrid-v1"
    source_code_qa_embedding_provider: str = "local_token_hybrid"
    source_code_qa_embedding_api_key: str | None = None
    source_code_qa_embedding_api_base_url: str = "https://api.openai.com/v1"
    source_code_qa_semantic_index_enabled: bool = True
    source_code_qa_llm_cache_ttl_seconds: int = 1800
    source_code_qa_llm_timeout_seconds: int = 90
    source_code_qa_codex_timeout_seconds: int = 240
    source_code_qa_codex_top_path_limit: int = 30
    source_code_qa_codex_repair_enabled: bool = True
    source_code_qa_llm_max_retries: int = 2
    source_code_qa_llm_backoff_seconds: float = 1.0
    source_code_qa_llm_max_backoff_seconds: float = 8.0
    seatalk_openapi_base_url: str = "https://openapi.seatalk.io"
    seatalk_app_id: str | None = None
    seatalk_app_secret: str | None = None
    seatalk_owner_email: str = "xiaodong.zheng@npt.sg"
    seatalk_local_app_path: str = "/Applications/SeaTalk.app"
    seatalk_local_data_dir: str = "~/Library/Application Support/SeaTalk"
    team_portal_config_encryption_key: str | None = None
    confluence_email: str | None = None
    confluence_api_token: str | None = None
    confluence_bearer_token: str | None = None
    confluence_base_url: str | None = None
    openai_api_key: str | None = None
    openai_api_base_url: str = "https://api.openai.com/v1"
    prd_briefing_text_model: str = "gpt-4.1-mini"
    prd_briefing_embedding_model: str = "text-embedding-3-large"
    prd_briefing_transcription_model: str = "gpt-4o-mini-transcribe"
    prd_briefing_tts_model: str = "gpt-4o-mini-tts"
    prd_briefing_tts_provider: str = "edge"
    prd_briefing_edge_mandarin_voice: str = "zh-CN-XiaoxiaoNeural"
    prd_briefing_edge_english_voice: str = "en-US-JennyNeural"
    prd_briefing_edge_rate: str = "-8%"
    prd_briefing_openai_mandarin_voice: str = "sage"
    prd_briefing_openai_voice_speed: float = 0.96
    prd_briefing_openai_custom_voice_enabled: bool = False
    prd_briefing_openai_tts_fallback_enabled: bool = False
    prd_briefing_answer_audio_enabled: bool = False
    elevenlabs_api_key: str | None = None
    elevenlabs_mandarin_model_id: str = "eleven_multilingual_v2"
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
            gmail_seatalk_demo_owner_email=_env_str("GMAIL_SEATALK_DEMO_OWNER_EMAIL", "xiaodong.zheng@npt.sg"),
            source_code_qa_owner_email=_env_str("SOURCE_CODE_QA_OWNER_EMAIL", "xiaodong.zheng@npt.sg"),
            source_code_qa_git_timeout_seconds=int(_env_str("SOURCE_CODE_QA_GIT_TIMEOUT_SECONDS", "90")),
            source_code_qa_max_file_bytes=int(_env_str("SOURCE_CODE_QA_MAX_FILE_BYTES", "500000")),
            source_code_qa_gitlab_token=_env_str("SOURCE_CODE_QA_GITLAB_TOKEN"),
            source_code_qa_gitlab_username=_env_str("SOURCE_CODE_QA_GITLAB_USERNAME", "oauth2"),
            source_code_qa_llm_provider=_env_str("SOURCE_CODE_QA_LLM_PROVIDER", "codex_cli_bridge"),
            source_code_qa_gemini_api_key=_env_str("SOURCE_CODE_QA_GEMINI_API_KEY") or _env_str("GEMINI_API_KEY"),
            source_code_qa_gemini_api_base_url=_env_str("SOURCE_CODE_QA_GEMINI_API_BASE_URL", _env_str("GEMINI_API_BASE_URL", "https://generativelanguage.googleapis.com/v1beta")),
            source_code_qa_openai_api_key=_env_str("SOURCE_CODE_QA_OPENAI_API_KEY") or _env_str("OPENAI_API_KEY"),
            source_code_qa_openai_api_base_url=_env_str("SOURCE_CODE_QA_OPENAI_API_BASE_URL", _env_str("OPENAI_API_BASE_URL", "https://api.openai.com/v1")),
            source_code_qa_openai_model=_env_str("SOURCE_CODE_QA_OPENAI_MODEL", "gpt-4.1-mini"),
            source_code_qa_openai_fast_model=_env_str("SOURCE_CODE_QA_OPENAI_FAST_MODEL", _env_str("SOURCE_CODE_QA_OPENAI_MODEL", "gpt-4.1-mini")),
            source_code_qa_openai_deep_model=_env_str("SOURCE_CODE_QA_OPENAI_DEEP_MODEL", _env_str("SOURCE_CODE_QA_OPENAI_MODEL", "gpt-4.1")),
            source_code_qa_openai_fallback_model=_env_str("SOURCE_CODE_QA_OPENAI_FALLBACK_MODEL", "gpt-4.1-mini"),
            source_code_qa_gemini_model=_env_str("SOURCE_CODE_QA_GEMINI_MODEL", "gemini-2.5-flash"),
            source_code_qa_gemini_fast_model=_env_str("SOURCE_CODE_QA_GEMINI_FAST_MODEL", "gemini-2.5-flash-lite"),
            source_code_qa_gemini_deep_model=_env_str("SOURCE_CODE_QA_GEMINI_DEEP_MODEL", _env_str("SOURCE_CODE_QA_GEMINI_MODEL", "gemini-2.5-flash")),
            source_code_qa_gemini_fallback_model=_env_str("SOURCE_CODE_QA_GEMINI_FALLBACK_MODEL", "gemini-2.5-flash-lite"),
            source_code_qa_vertex_credentials_file=_env_str("SOURCE_CODE_QA_VERTEX_CREDENTIALS_FILE") or _env_str("GOOGLE_APPLICATION_CREDENTIALS"),
            source_code_qa_vertex_project_id=_env_str("SOURCE_CODE_QA_VERTEX_PROJECT_ID") or _env_str("GOOGLE_CLOUD_PROJECT"),
            source_code_qa_vertex_location=_env_str("SOURCE_CODE_QA_VERTEX_LOCATION", _env_str("GOOGLE_CLOUD_LOCATION", "global")),
            source_code_qa_vertex_model=_env_str("SOURCE_CODE_QA_VERTEX_MODEL", "gemini-2.5-flash"),
            source_code_qa_vertex_fast_model=_env_str("SOURCE_CODE_QA_VERTEX_FAST_MODEL", "gemini-2.5-flash-lite"),
            source_code_qa_vertex_deep_model=_env_str("SOURCE_CODE_QA_VERTEX_DEEP_MODEL", _env_str("SOURCE_CODE_QA_VERTEX_MODEL", "gemini-2.5-flash")),
            source_code_qa_vertex_fallback_model=_env_str("SOURCE_CODE_QA_VERTEX_FALLBACK_MODEL", "gemini-2.5-flash-lite"),
            source_code_qa_query_rewrite_model=_env_str("SOURCE_CODE_QA_QUERY_REWRITE_MODEL"),
            source_code_qa_planner_model=_env_str("SOURCE_CODE_QA_PLANNER_MODEL"),
            source_code_qa_answer_model=_env_str("SOURCE_CODE_QA_ANSWER_MODEL"),
            source_code_qa_judge_model=_env_str("SOURCE_CODE_QA_JUDGE_MODEL"),
            source_code_qa_repair_model=_env_str("SOURCE_CODE_QA_REPAIR_MODEL"),
            source_code_qa_llm_judge_enabled=_env_bool("SOURCE_CODE_QA_LLM_JUDGE_ENABLED", True),
            source_code_qa_embedding_model=_env_str("SOURCE_CODE_QA_EMBEDDING_MODEL", "local-token-hybrid-v1"),
            source_code_qa_embedding_provider=_env_str("SOURCE_CODE_QA_EMBEDDING_PROVIDER", "local_token_hybrid"),
            source_code_qa_embedding_api_key=_env_str("SOURCE_CODE_QA_EMBEDDING_API_KEY") or _env_str("OPENAI_API_KEY"),
            source_code_qa_embedding_api_base_url=_env_str("SOURCE_CODE_QA_EMBEDDING_API_BASE_URL", _env_str("OPENAI_API_BASE_URL", "https://api.openai.com/v1")),
            source_code_qa_semantic_index_enabled=_env_bool("SOURCE_CODE_QA_SEMANTIC_INDEX_ENABLED", True),
            source_code_qa_llm_cache_ttl_seconds=int(_env_str("SOURCE_CODE_QA_LLM_CACHE_TTL_SECONDS", "1800")),
            source_code_qa_llm_timeout_seconds=int(_env_str("SOURCE_CODE_QA_LLM_TIMEOUT_SECONDS", "90")),
            source_code_qa_codex_timeout_seconds=int(_env_str("SOURCE_CODE_QA_CODEX_TIMEOUT_SECONDS", "240")),
            source_code_qa_codex_top_path_limit=int(_env_str("SOURCE_CODE_QA_CODEX_TOP_PATH_LIMIT", "30")),
            source_code_qa_codex_repair_enabled=_env_bool("SOURCE_CODE_QA_CODEX_REPAIR_ENABLED", True),
            source_code_qa_llm_max_retries=int(_env_str("SOURCE_CODE_QA_LLM_MAX_RETRIES", "2")),
            source_code_qa_llm_backoff_seconds=float(_env_str("SOURCE_CODE_QA_LLM_BACKOFF_SECONDS", "1.0")),
            source_code_qa_llm_max_backoff_seconds=float(_env_str("SOURCE_CODE_QA_LLM_MAX_BACKOFF_SECONDS", "8.0")),
            seatalk_openapi_base_url=_env_str("SEATALK_OPENAPI_BASE_URL", "https://openapi.seatalk.io"),
            seatalk_app_id=_env_str("SEATALK_APP_ID"),
            seatalk_app_secret=_env_str("SEATALK_APP_SECRET"),
            seatalk_owner_email=_env_str("SEATALK_OWNER_EMAIL", "xiaodong.zheng@npt.sg"),
            seatalk_local_app_path=_env_str("SEATALK_LOCAL_APP_PATH", "/Applications/SeaTalk.app"),
            seatalk_local_data_dir=_env_str("SEATALK_LOCAL_DATA_DIR", "~/Library/Application Support/SeaTalk"),
            team_portal_config_encryption_key=_env_str("TEAM_PORTAL_CONFIG_ENCRYPTION_KEY"),
            spreadsheet_id=_env_str("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
            common_tab_name=_env_str("COMMON_TAB_NAME", "Common"),
            input_tab_name=_env_str("INPUT_TAB_NAME", "Sheet1"),
            bpmis_base_url=_env_str("BPMIS_BASE_URL", "https://bpmis-uat1.uat.npt.seabank.io"),
            bpmis_api_access_token=_env_str("BPMIS_API_ACCESS_TOKEN"),
            confluence_email=_env_str("CONFLUENCE_EMAIL"),
            confluence_api_token=_env_str("CONFLUENCE_API_TOKEN"),
            confluence_bearer_token=_env_str("CONFLUENCE_BEARER_TOKEN"),
            confluence_base_url=_env_str("CONFLUENCE_BASE_URL"),
            openai_api_key=_env_str("OPENAI_API_KEY"),
            openai_api_base_url=_env_str("OPENAI_API_BASE_URL", "https://api.openai.com/v1"),
            prd_briefing_text_model=_env_str("PRD_BRIEFING_TEXT_MODEL", "gpt-4.1-mini"),
            prd_briefing_embedding_model=_env_str("PRD_BRIEFING_EMBEDDING_MODEL", "text-embedding-3-large"),
            prd_briefing_transcription_model=_env_str("PRD_BRIEFING_TRANSCRIPTION_MODEL", "gpt-4o-mini-transcribe"),
            prd_briefing_tts_model=_env_str("PRD_BRIEFING_TTS_MODEL", "gpt-4o-mini-tts"),
            prd_briefing_tts_provider=_env_str("PRD_BRIEFING_TTS_PROVIDER", "edge"),
            prd_briefing_edge_mandarin_voice=_env_str("PRD_BRIEFING_EDGE_MANDARIN_VOICE", "zh-CN-XiaoxiaoNeural"),
            prd_briefing_edge_english_voice=_env_str("PRD_BRIEFING_EDGE_ENGLISH_VOICE", "en-US-JennyNeural"),
            prd_briefing_edge_rate=_env_str("PRD_BRIEFING_EDGE_RATE", "-8%"),
            prd_briefing_openai_mandarin_voice=_env_str("PRD_BRIEFING_OPENAI_MANDARIN_VOICE", "sage"),
            prd_briefing_openai_voice_speed=float(_env_str("PRD_BRIEFING_OPENAI_VOICE_SPEED", "0.96")),
            prd_briefing_openai_custom_voice_enabled=_env_bool("PRD_BRIEFING_OPENAI_CUSTOM_VOICE_ENABLED", False),
            prd_briefing_openai_tts_fallback_enabled=_env_bool("PRD_BRIEFING_OPENAI_TTS_FALLBACK_ENABLED", False),
            prd_briefing_answer_audio_enabled=_env_bool("PRD_BRIEFING_ANSWER_AUDIO_ENABLED", False),
            elevenlabs_api_key=_env_str("ELEVENLABS_API_KEY"),
            elevenlabs_mandarin_model_id=_env_str("ELEVENLABS_MANDARIN_MODEL_ID", "eleven_multilingual_v2"),
            elevenlabs_mandarin_voice_id=_env_str("ELEVENLABS_MANDARIN_VOICE_ID", "JBFqnCBsd6RMkjVDRZzb"),
        )
