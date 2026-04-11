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
    bpmis_api_search_url_template: str | None
    bpmis_api_create_url_template: str | None
    bpmis_api_search_method: str
    bpmis_api_create_method: str
    bpmis_api_search_response_path: str | None
    bpmis_api_created_ticket_path: str | None
    bpmis_browser_base_url: str
    bpmis_browser_project_url_template: str | None
    bpmis_browser_search_input_selector: str | None
    bpmis_browser_search_submit_selector: str | None
    bpmis_browser_project_link_selector: str | None
    bpmis_browser_create_button_selector: str | None
    bpmis_browser_task_item_selector: str | None
    bpmis_browser_fix_version_selector: str | None
    bpmis_browser_submit_selector: str | None
    bpmis_browser_executable_path: str | None
    bpmis_browser_cdp_url: str | None
    bpmis_browser_token_storage_key: str
    bpmis_browser_headless: bool
    bpmis_browser_ticket_url_regex: str | None

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
            spreadsheet_id=_env_str("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
            common_tab_name=_env_str("COMMON_TAB_NAME", "Common"),
            input_tab_name=_env_str("INPUT_TAB_NAME", "Input"),
            bpmis_base_url=_env_str("BPMIS_BASE_URL", "https://bpmis-uat1.uat.npt.seabank.io"),
            bpmis_api_search_url_template=_env_str("BPMIS_API_SEARCH_URL_TEMPLATE"),
            bpmis_api_create_url_template=_env_str("BPMIS_API_CREATE_URL_TEMPLATE"),
            bpmis_api_search_method=_env_str("BPMIS_API_SEARCH_METHOD", "GET"),
            bpmis_api_create_method=_env_str("BPMIS_API_CREATE_METHOD", "POST"),
            bpmis_api_search_response_path=_env_str("BPMIS_API_SEARCH_RESPONSE_PATH"),
            bpmis_api_created_ticket_path=_env_str("BPMIS_API_CREATED_TICKET_PATH"),
            bpmis_browser_base_url=_env_str("BPMIS_BROWSER_BASE_URL", "https://bpmis-uat1.uat.npt.seabank.io/me"),
            bpmis_browser_project_url_template=_env_str("BPMIS_BROWSER_PROJECT_URL_TEMPLATE"),
            bpmis_browser_search_input_selector=_env_str("BPMIS_BROWSER_SEARCH_INPUT_SELECTOR"),
            bpmis_browser_search_submit_selector=_env_str("BPMIS_BROWSER_SEARCH_SUBMIT_SELECTOR"),
            bpmis_browser_project_link_selector=_env_str("BPMIS_BROWSER_PROJECT_LINK_SELECTOR"),
            bpmis_browser_create_button_selector=_env_str("BPMIS_BROWSER_CREATE_BUTTON_SELECTOR"),
            bpmis_browser_task_item_selector=_env_str("BPMIS_BROWSER_TASK_ITEM_SELECTOR"),
            bpmis_browser_fix_version_selector=_env_str("BPMIS_BROWSER_FIX_VERSION_SELECTOR"),
            bpmis_browser_submit_selector=_env_str("BPMIS_BROWSER_SUBMIT_SELECTOR"),
            bpmis_browser_executable_path=_env_str("BPMIS_BROWSER_EXECUTABLE_PATH"),
            bpmis_browser_cdp_url=_env_str("BPMIS_BROWSER_CDP_URL", "http://127.0.0.1:9222"),
            bpmis_browser_token_storage_key=_env_str("BPMIS_BROWSER_TOKEN_STORAGE_KEY", "access_token"),
            bpmis_browser_headless=_env_bool("BPMIS_BROWSER_HEADLESS", False),
            bpmis_browser_ticket_url_regex=_env_str("BPMIS_BROWSER_TICKET_URL_REGEX"),
        )
