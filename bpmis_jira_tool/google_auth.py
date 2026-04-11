from __future__ import annotations

from typing import Any

from flask import session, url_for
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import AuthenticationError, ConfigError


GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def build_google_flow(settings: Settings) -> Flow:
    if not settings.google_oauth_client_secret_file.exists():
        raise ConfigError(
            "Google OAuth client secret file was not found. "
            "Set GOOGLE_OAUTH_CLIENT_SECRET_FILE to a valid JSON file."
        )

    flow = Flow.from_client_secrets_file(
        str(settings.google_oauth_client_secret_file),
        scopes=GOOGLE_SCOPES,
    )
    return flow


def create_google_authorization_url(settings: Settings) -> str:
    flow = build_google_flow(settings)
    redirect_uri = settings.google_oauth_redirect_uri or url_for("google_callback", _external=True)
    flow.redirect_uri = redirect_uri
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    session["google_oauth_state"] = state
    return authorization_url


def finish_google_oauth(settings: Settings, authorization_response: str) -> None:
    state = session.get("google_oauth_state")
    if not state:
        raise AuthenticationError("Missing OAuth state. Start the Google sign-in flow again.")

    flow = build_google_flow(settings)
    flow.redirect_uri = settings.google_oauth_redirect_uri or url_for("google_callback", _external=True)
    flow.fetch_token(authorization_response=authorization_response)
    session["google_credentials"] = credentials_to_dict(flow.credentials)


def get_google_credentials() -> Credentials:
    payload = session.get("google_credentials")
    if not payload:
        raise AuthenticationError("Google Sheets is not connected yet.")
    return Credentials(**payload)


def credentials_to_dict(credentials: Credentials) -> dict[str, Any]:
    return {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes,
    }

