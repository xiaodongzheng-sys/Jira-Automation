import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.google_auth import (
    _allow_localhost_oauth_http,
    _normalize_authorization_response,
    _resolve_google_redirect_uri,
    GOOGLE_SCOPES,
    create_google_authorization_url,
    fetch_google_profile,
    finish_google_oauth,
)


class GoogleAuthTests(unittest.TestCase):
    def test_google_scopes_include_gmail_readonly(self):
        self.assertIn("https://www.googleapis.com/auth/gmail.readonly", GOOGLE_SCOPES)
        self.assertIn("https://www.googleapis.com/auth/calendar.readonly", GOOGLE_SCOPES)

    def test_allow_localhost_oauth_http_sets_insecure_transport(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OAUTHLIB_INSECURE_TRANSPORT", None)
            _allow_localhost_oauth_http("http://127.0.0.1:5000/auth/google/callback")
            self.assertEqual(os.environ.get("OAUTHLIB_INSECURE_TRANSPORT"), "1")

    def test_non_localhost_oauth_http_does_not_set_insecure_transport(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OAUTHLIB_INSECURE_TRANSPORT", None)
            _allow_localhost_oauth_http("https://example.com/auth/google/callback")
            self.assertIsNone(os.environ.get("OAUTHLIB_INSECURE_TRANSPORT"))

    def test_create_authorization_url_allows_localhost_http(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch.dict(os.environ, {}, clear=False):
            secret_file = os.path.join(temp_dir, "client_secret.json")
            with open(secret_file, "w", encoding="utf-8") as handle:
                handle.write(
                    '{"web": {"client_id": "x", "project_id": "p", "auth_uri": "https://accounts.google.com/o/oauth2/auth",'
                    ' "token_uri": "https://oauth2.googleapis.com/token", "client_secret": "y",'
                    ' "redirect_uris": ["http://127.0.0.1:5000/auth/google/callback"]}}'
                )

            settings = Settings(
                flask_secret_key="secret",
                google_oauth_client_secret_file=Path(secret_file),
                google_oauth_redirect_uri="http://127.0.0.1:5000/auth/google/callback",
                team_portal_host="127.0.0.1",
                team_portal_port=5000,
                team_portal_base_url=None,
                team_allowed_emails=(),
                team_allowed_email_domains=(),
                team_portal_data_dir=Path(temp_dir),
                spreadsheet_id="sheet",
                common_tab_name="Common",
                input_tab_name="Input",
                bpmis_base_url="https://example.com",
                bpmis_api_access_token=None,
            )

            os.environ.pop("OAUTHLIB_INSECURE_TRANSPORT", None)
            with patch("bpmis_jira_tool.google_auth.session", {}), patch(
                "bpmis_jira_tool.google_auth.Flow.authorization_url",
                return_value=("https://accounts.google.com/o/oauth2/auth", "state"),
            ):
                create_google_authorization_url(settings)

            self.assertEqual(os.environ.get("OAUTHLIB_INSECURE_TRANSPORT"), "1")

    def test_resolve_google_redirect_uri_prefers_team_portal_base_url(self):
        settings = Settings(
            flask_secret_key="secret",
            google_oauth_client_secret_file=Path("client.json"),
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url="https://jira-tool.example.com",
            team_allowed_emails=(),
            team_allowed_email_domains=("npt.sg",),
            team_portal_data_dir=Path("."),
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Projects",
            bpmis_base_url="https://example.com",
            bpmis_api_access_token=None,
        )

        self.assertEqual(
            _resolve_google_redirect_uri(settings),
            "https://jira-tool.example.com/auth/google/callback",
        )

    def test_normalize_authorization_response_rewrites_local_proxy_callback_to_https_redirect(self):
        normalized = _normalize_authorization_response(
            "http://127.0.0.1:5000/auth/google/callback?state=abc&code=xyz",
            "https://jira-tool.example.com/auth/google/callback",
        )

        self.assertEqual(
            normalized,
            "https://jira-tool.example.com/auth/google/callback?state=abc&code=xyz",
        )

    def test_fetch_google_profile_closes_response(self):
        response = Mock()
        response.json.return_value = {
            "sub": "1",
            "email": "user@npt.sg",
            "name": "User",
            "picture": "https://example.com/p.png",
        }
        response.raise_for_status.return_value = None
        response.close = Mock()
        credentials = Mock(token="token")

        with patch("bpmis_jira_tool.google_auth.requests.get", return_value=response):
            payload = fetch_google_profile(credentials)

        self.assertEqual(payload["email"], "user@npt.sg")
        response.close.assert_called_once()

    def test_finish_google_oauth_relaxes_partial_scope_warning_only_during_token_fetch(self):
        settings = Settings(
            flask_secret_key="secret",
            google_oauth_client_secret_file=Path("client.json"),
            google_oauth_redirect_uri="https://jira-tool.example.com/auth/google/callback",
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url=None,
            team_allowed_emails=(),
            team_allowed_email_domains=("npt.sg",),
            team_portal_data_dir=Path("."),
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Projects",
            bpmis_base_url="https://example.com",
            bpmis_api_access_token=None,
        )
        flow = Mock()
        flow.credentials = Mock(
            token="token",
            refresh_token="refresh",
            token_uri="https://oauth2.googleapis.com/token",
            client_id="client",
            client_secret="secret",
            scopes=["openid", "https://www.googleapis.com/auth/userinfo.email"],
        )

        def fetch_token(**_kwargs):
            self.assertEqual(os.environ.get("OAUTHLIB_RELAX_TOKEN_SCOPE"), "1")

        flow.fetch_token.side_effect = fetch_token

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OAUTHLIB_RELAX_TOKEN_SCOPE", None)
            with patch("bpmis_jira_tool.google_auth.session", {"google_oauth_state": "state"}) as fake_session:
                with patch("bpmis_jira_tool.google_auth.build_google_flow", return_value=flow):
                    with patch("bpmis_jira_tool.google_auth.fetch_google_profile", return_value={"email": "user@npt.sg"}):
                        finish_google_oauth(
                            settings,
                            "https://jira-tool.example.com/auth/google/callback?state=state&code=code",
                        )

            self.assertIsNone(os.environ.get("OAUTHLIB_RELAX_TOKEN_SCOPE"))

        self.assertEqual(fake_session["google_profile"]["email"], "user@npt.sg")
        self.assertEqual(fake_session["google_credentials"]["scopes"], ["openid", "https://www.googleapis.com/auth/userinfo.email"])


if __name__ == "__main__":
    unittest.main()
