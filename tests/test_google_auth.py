import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.google_auth import (
    _allow_localhost_oauth_http,
    _normalize_authorization_response,
    _resolve_google_redirect_uri,
    create_google_authorization_url,
)


class GoogleAuthTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
