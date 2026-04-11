from __future__ import annotations

import os
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from flask import session, url_for
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import AuthenticationError, ConfigError


GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/spreadsheets",
]


def _resolve_google_redirect_uri(settings: Settings) -> str:
    if settings.google_oauth_redirect_uri:
        return settings.google_oauth_redirect_uri
    if settings.team_portal_base_url:
        return urljoin(settings.team_portal_base_url.rstrip("/") + "/", "auth/google/callback")
    return url_for("google_callback", _external=True)


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


def _allow_localhost_oauth_http(redirect_uri: str) -> None:
    parsed = urlparse(redirect_uri)
    if parsed.scheme != "http":
        return
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        return
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"


def create_google_authorization_url(settings: Settings) -> str:
    flow = build_google_flow(settings)
    redirect_uri = _resolve_google_redirect_uri(settings)
    _allow_localhost_oauth_http(redirect_uri)
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
    flow.redirect_uri = _resolve_google_redirect_uri(settings)
    _allow_localhost_oauth_http(flow.redirect_uri)
    flow.fetch_token(authorization_response=authorization_response)
    session["google_credentials"] = credentials_to_dict(flow.credentials)
    session["google_profile"] = fetch_google_profile(flow.credentials)


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


def fetch_google_profile(credentials: Credentials) -> dict[str, Any]:
    try:
        response = requests.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {credentials.token}"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as error:
        raise AuthenticationError("Google sign-in succeeded, but user profile lookup failed. Please try again.") from error
    return {
        "sub": payload.get("sub"),
        "email": payload.get("email"),
        "name": payload.get("name"),
        "picture": payload.get("picture"),
    }
