from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_SPREADSHEET_ID = "1KKlqDosv2QjCZrY8If-JreuVa_ALHZUIrxhMbAa_y_Q"


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    flask_secret_key: str
    google_oauth_client_secret_file: Path
    google_oauth_redirect_uri: str | None
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
        client_secret = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET_FILE", "").strip()
        if not client_secret:
            client_secret = "google-client-secret.json"

        return cls(
            flask_secret_key=os.getenv("FLASK_SECRET_KEY", "dev-secret-key"),
            google_oauth_client_secret_file=Path(client_secret),
            google_oauth_redirect_uri=os.getenv("GOOGLE_OAUTH_REDIRECT_URI"),
            spreadsheet_id=os.getenv("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
            common_tab_name=os.getenv("COMMON_TAB_NAME", "Common"),
            input_tab_name=os.getenv("INPUT_TAB_NAME", "Input"),
            bpmis_base_url=os.getenv("BPMIS_BASE_URL", "https://bpmis-uat1.uat.npt.seabank.io"),
            bpmis_api_search_url_template=os.getenv("BPMIS_API_SEARCH_URL_TEMPLATE"),
            bpmis_api_create_url_template=os.getenv("BPMIS_API_CREATE_URL_TEMPLATE"),
            bpmis_api_search_method=os.getenv("BPMIS_API_SEARCH_METHOD", "GET"),
            bpmis_api_create_method=os.getenv("BPMIS_API_CREATE_METHOD", "POST"),
            bpmis_api_search_response_path=os.getenv("BPMIS_API_SEARCH_RESPONSE_PATH"),
            bpmis_api_created_ticket_path=os.getenv("BPMIS_API_CREATED_TICKET_PATH"),
            bpmis_browser_base_url=os.getenv("BPMIS_BROWSER_BASE_URL", "https://bpmis-uat1.uat.npt.seabank.io/me"),
            bpmis_browser_project_url_template=os.getenv("BPMIS_BROWSER_PROJECT_URL_TEMPLATE"),
            bpmis_browser_search_input_selector=os.getenv("BPMIS_BROWSER_SEARCH_INPUT_SELECTOR"),
            bpmis_browser_search_submit_selector=os.getenv("BPMIS_BROWSER_SEARCH_SUBMIT_SELECTOR"),
            bpmis_browser_project_link_selector=os.getenv("BPMIS_BROWSER_PROJECT_LINK_SELECTOR"),
            bpmis_browser_create_button_selector=os.getenv("BPMIS_BROWSER_CREATE_BUTTON_SELECTOR"),
            bpmis_browser_task_item_selector=os.getenv("BPMIS_BROWSER_TASK_ITEM_SELECTOR"),
            bpmis_browser_fix_version_selector=os.getenv("BPMIS_BROWSER_FIX_VERSION_SELECTOR"),
            bpmis_browser_submit_selector=os.getenv("BPMIS_BROWSER_SUBMIT_SELECTOR"),
            bpmis_browser_executable_path=os.getenv("BPMIS_BROWSER_EXECUTABLE_PATH"),
            bpmis_browser_cdp_url=os.getenv("BPMIS_BROWSER_CDP_URL"),
            bpmis_browser_token_storage_key=os.getenv("BPMIS_BROWSER_TOKEN_STORAGE_KEY", "access_token"),
            bpmis_browser_headless=_env_bool("BPMIS_BROWSER_HEADLESS", False),
            bpmis_browser_ticket_url_regex=os.getenv("BPMIS_BROWSER_TICKET_URL_REGEX"),
        )
