import os
import tempfile
import unittest
from unittest.mock import patch

from bpmis_jira_tool.web import create_app
from prd_briefing import blueprint as prd_blueprint_module


class FakeBriefingService:
    def __init__(self):
        self.create_session_kwargs = []

    def create_session(self, **kwargs):
        self.create_session_kwargs.append(kwargs)
        audience = "developer_en" if kwargs.get("language") == "en" else "developer_zh"
        return {
            "session": {
                "session_id": "session-1",
                "title": "PRD",
                "audience": audience,
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
            "sections": [{"section_path": "Overview", "content": "Body", "html_content": "<p>Body</p>", "image_refs": [], "walkthrough_cached": True, "walkthrough_audio_cached": True}],
            "briefing_blocks": [
                {
                    "block_id": "block-1-feature",
                    "title": "核心功能说明",
                    "briefing_goal": "Goal",
                    "merged_summary": "Summary",
                    "section_indexes": [0],
                    "source_refs": [{"section_index": 0, "section_path": "Overview"}],
                    "developer_focus": ["focus"],
                    "walkthrough_cached": True,
                    "walkthrough_audio_cached": True,
                }
            ],
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

    def narrate_section(self, **kwargs):
        return {
            "script": "Section script",
            "audio_url": None,
            "cached": True,
            "audio_cached": True,
            "briefing_block_id": kwargs.get("briefing_block_id"),
            "section_indexes": [0],
        }


class FakePRDReviewService:
    def __init__(self):
        self.requests = []

    def review_url(self, request):
        self.requests.append(request)
        if not request.prd_url:
            from bpmis_jira_tool.errors import ToolError

            raise ToolError("PRD link is required.")
        if not request.prd_url.lower().startswith(("http://", "https://")):
            from bpmis_jira_tool.errors import ToolError

            raise ToolError("PRD link must be an HTTP or HTTPS URL.")
        return {
            "status": "ok",
            "cached": False,
            "language": request.language,
            "review": {
                "prd_url": request.prd_url,
                "status": "completed",
                "result_markdown": "### Review\n- Good",
                "updated_at": "2026-04-30T00:00:00Z",
            },
            "prd": {"title": "PRD"},
        }


class FakePRDReviewLocalAgentClient:
    def __init__(self):
        self.payload = None

    def prd_briefing_review(self, payload):
        self.payload = payload
        return {
            "status": "ok",
            "cached": True,
            "language": payload["language"],
            "review": {
                "status": "completed",
                "result_markdown": "### Cached Review",
                "updated_at": "2026-04-30T00:00:00Z",
            },
            "prd": {"title": "Remote PRD"},
        }


class FakeCodexTextGenerationClient:
    init_kwargs = None

    def __init__(self, **kwargs):
        FakeCodexTextGenerationClient.init_kwargs = kwargs

    @property
    def model_id(self):
        return "codex:test"

    def is_configured(self):
        return True

    def create_answer(self, **kwargs):
        return "Codex walkthrough"


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
            self.assertIn(b"page-shell-briefing", response.data)
            self.assertIn(b"data-image-lightbox", response.data)
            self.assertIn(b"data-no-image-mode-toggle", response.data)
            self.assertIn(b"data-prd-review-generate", response.data)
            self.assertIn(b"data-briefing-language", response.data)
            self.assertIn(b"PRD Details", response.data)
            self.assertIn(b"Output Language", response.data)
            self.assertIn(b"developer walkthrough or an AI PRD review", response.data)
            self.assertIn(b"No PRD output yet", response.data)
            self.assertNotIn(b"data-prd-review-language", response.data)
            self.assertNotIn(b"No walkthrough yet", response.data)
            self.assertNotIn("3 分钟".encode("utf-8"), response.data)
            self.assertNotIn(b"Team Knowledge Base", response.data)

    def test_build_service_uses_codex_for_walkthrough_generation(self):
        FakeCodexTextGenerationClient.init_kwargs = None
        with self.app.app_context(), patch.object(
            prd_blueprint_module,
            "CodexTextGenerationClient",
            FakeCodexTextGenerationClient,
        ):
            service = prd_blueprint_module._build_service()  # noqa: SLF001

        self.assertIsInstance(service.text_client, FakeCodexTextGenerationClient)
        self.assertEqual(FakeCodexTextGenerationClient.init_kwargs["prompt_mode"], "prd_briefing_developer_walkthrough_codex")

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_create_session_endpoint_returns_payload(self, mock_service):
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
            self.assertEqual(payload["briefing_blocks"][0]["block_id"], "block-1-feature")
            self.assertEqual(mock_service.return_value.create_session_kwargs[-1]["language"], "zh")

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_create_session_endpoint_accepts_english_briefing_language(self, mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/session",
                json={
                    "page_ref": "https://example.atlassian.net/wiki/pages/123",
                    "mode": "walkthrough",
                    "language": "en",
                },
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()

        self.assertEqual(payload["session"]["audience"], "developer_en")
        self.assertEqual(mock_service.return_value.create_session_kwargs[-1]["language"], "en")

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_narrate_endpoint_accepts_briefing_block_id(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/session/session-1/narrate",
                json={"briefing_block_id": "block-1-feature", "include_audio": False},
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["briefing_block_id"], "block-1-feature")
            self.assertEqual(payload["section_indexes"], [0])

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

    @patch("prd_briefing.blueprint._build_prd_review_service", return_value=FakePRDReviewService())
    def test_review_endpoint_returns_chinese_markdown(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/review",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "zh"},
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["language"], "zh")
            self.assertIn("### Review", payload["review"]["result_markdown"])

    @patch("prd_briefing.blueprint._build_prd_review_service", return_value=FakePRDReviewService())
    def test_review_endpoint_passes_english_language(self, mock_service):
        service = mock_service.return_value
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/review",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
            )
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()

        self.assertEqual(payload["language"], "en")
        self.assertEqual(service.requests[-1].language, "en")

    @patch("prd_briefing.blueprint._build_prd_review_service", return_value=FakePRDReviewService())
    def test_review_endpoint_validates_required_prd_link(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post("/prd-briefing/api/review", json={"language": "zh"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("PRD link is required", response.get_json()["message"])

    @patch("prd_briefing.blueprint._build_prd_review_service", return_value=FakePRDReviewService())
    def test_review_endpoint_validates_http_prd_link(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post("/prd-briefing/api/review", json={"prd_url": "not-a-url"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("HTTP or HTTPS", response.get_json()["message"])

    @patch("prd_briefing.blueprint._local_agent_source_code_qa_enabled", return_value=True)
    def test_review_endpoint_can_route_to_local_agent(self, _mock_enabled):
        fake_client = FakePRDReviewLocalAgentClient()
        with patch("prd_briefing.blueprint._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post(
                    "/prd-briefing/api/review",
                    json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["cached"])
        self.assertEqual(payload["review"]["result_markdown"], "### Cached Review")
        self.assertEqual(fake_client.payload["language"], "en")

    def test_portal_route_allows_npt_google_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"PRD Briefing Tool", response.data)

    def test_portal_route_allows_test_gmail_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng1991@gmail.com", "name": "Test User"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"PRD Briefing Tool", response.data)

    def test_portal_route_blocks_unapproved_google_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "outsider@gmail.com", "name": "Outsider"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/access-denied")

    def test_portal_route_redirects_anonymous_user_to_google_login(self):
        with self.app.test_client() as client:
            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/")


if __name__ == "__main__":
    unittest.main()
