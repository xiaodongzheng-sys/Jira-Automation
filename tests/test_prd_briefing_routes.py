import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.web import create_app


class FakeBriefingService:
    def create_session(self, **kwargs):
        return {
            "session": {
                "session_id": "session-1",
                "title": "PRD",
                "audience": "developer_zh",
            },
            "session_overview": {
                "overview": "overview",
                "scope": ["scope"],
                "impacted_modules": ["module"],
                "developer_focus": ["focus"],
                "frontend_focus": ["frontend"],
                "backend_focus": ["backend"],
                "risks": ["risk"],
                "unclear_rules": ["rule"],
                "missing_edge_cases": ["edge"],
                "unclear_ownership": ["ownership"],
                "open_questions": ["question"],
            },
            "sections": [{"section_path": "Overview", "content": "Body", "html_content": "<p>Body</p>", "image_refs": []}],
            "kb_sources": [],
            "voice_profile": None,
            "messages": [],
        }

    def get_session_payload(self, **kwargs):
        return self.create_session(page_ref="", mode="walkthrough")

    def answer_question(self, **kwargs):
        return {
            "answer_text": "Grounded answer",
            "answer_language": "zh",
            "groundedness": "grounded",
            "citations": [],
            "audio_url": None,
        }

    def upload_kb_document(self, **kwargs):
        return {"title": "kb.txt", "chunk_count": 1}

    def enroll_voice(self, **kwargs):
        return {"provider": "stored_samples", "consent_status": "granted", "sample_language": "en"}

    def transcribe_audio(self, **kwargs):
        return {"text": "transcribed"}

    def narrate_section(self, **kwargs):
        return {"script": "Section script", "audio_url": None}


class PRDBriefingRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_portal_route_renders(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.get("/prd-briefing/")
            self.assertEqual(response.status_code, 200)
            self.assertIn("PRD Briefing Tool".encode("utf-8"), response.data)
            self.assertIn(b"data-image-lightbox", response.data)
            self.assertNotIn(b"Team Knowledge Base", response.data)

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_create_session_endpoint_returns_payload(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/session",
                json={
                    "page_ref": "https://example.atlassian.net/wiki/pages/123",
                    "mode": "walkthrough",
                },
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["session"]["session_id"], "session-1")
            self.assertEqual(payload["session"]["audience"], "developer_zh")

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_answer_endpoint_returns_grounded_payload(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/session/session-1/answer",
                json={"question": "What changed?"},
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["groundedness"], "grounded")
            self.assertEqual(payload["answer_text"], "Grounded answer")

    def test_portal_route_blocks_non_owner_google_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/")

    def test_portal_route_redirects_anonymous_user_to_google_login(self):
        with self.app.test_client() as client:
            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/")


if __name__ == "__main__":
    unittest.main()
