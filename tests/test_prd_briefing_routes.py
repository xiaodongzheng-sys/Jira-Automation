import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bpmis_jira_tool.errors import ToolError
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

    def process_prd_for_presentation(self, **kwargs):
        return {
            "status": "ok",
            "session": {
                "session_id": "session-1",
                "title": "PRD",
                "model_id": "codex:gpt-5.5",
                "prompt_version": "v5_codex_gpt55_prd_presentation_chunks_media",
                "page_id": "123",
                "version_number": "5",
            },
            "cached": False,
            "chunks": [
                {
                    "id": "chunk-1",
                    "title": "开场",
                    "content": "这一段给开发说明主流程。",
                    "imageUrls": ["/prd-briefing/image-proxy?src=x"],
                    "media": {"type": "image", "content": "/prd-briefing/image-proxy?src=x"},
                    "cacheKey": "123_5",
                    "audioStatus": "draft",
                }
            ],
        }

    def generate_presentation_audio(self, **kwargs):
        return {
            "status": "ok",
            "chunk": {
                "id": kwargs["chunk"]["id"],
                "title": kwargs["chunk"]["title"],
                "content": kwargs["chunk"]["content"],
                "audioUrl": "/prd-briefing/assets/audio/session-1/mock.mp3",
                "duration": 3.2,
                "timestamps": [{"sentence": kwargs["chunk"]["content"], "start": 0, "end": 3.2}],
                "imageUrls": kwargs["chunk"].get("imageUrls") or [],
                "media": kwargs["chunk"].get("media") or {"type": "none", "content": ""},
                "cacheKey": kwargs["chunk"].get("cacheKey") or "",
            },
        }


class FakePRDReviewService:
    def __init__(self):
        self.requests = []
        self.summary_requests = []

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

    def summarize_url(self, request):
        self.summary_requests.append(request)
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
            "summary": {
                "prd_url": request.prd_url,
                "status": "completed",
                "result_markdown": "### Summary\n- Good",
                "updated_at": "2026-04-30T00:00:00Z",
            },
            "prd": {"title": "PRD"},
        }

    def list_url_sections(self, request):
        self.requests.append(request)
        if not request.prd_url:
            from bpmis_jira_tool.errors import ToolError

            raise ToolError("PRD link is required.")
        return {
            "status": "ok",
            "prd": {"title": "PRD", "updated_at": "2026-04-30T00:00:00Z"},
            "sections": [
                {"index": 1, "title": "Overview", "char_count": 1200, "long": False},
                {"index": 2, "title": "Workflow", "char_count": 9200, "long": True},
            ],
        }


class FakePRDReviewLocalAgentClient:
    def __init__(self):
        self.payload = None
        self.image_src = None

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

    def prd_self_assessment_review(self, payload):
        self.payload = payload
        return {
            "status": "ok",
            "cached": True,
            "language": payload["language"],
            "review": {
                "status": "completed",
                "result_markdown": "### Remote Review",
                "updated_at": "2026-04-30T00:00:00Z",
            },
            "prd": {"title": "Remote PRD"},
        }

    def prd_self_assessment_summary(self, payload):
        self.payload = payload
        return {
            "status": "ok",
            "cached": True,
            "language": payload["language"],
            "summary": {
                "status": "completed",
                "result_markdown": "### Remote Summary",
                "updated_at": "2026-04-30T00:00:00Z",
            },
            "prd": {"title": "Remote PRD"},
        }

    def prd_self_assessment_sections(self, payload):
        self.payload = payload
        return {
            "status": "ok",
            "prd": {"title": "Remote PRD"},
            "sections": [{"index": 1, "title": "Remote Section", "char_count": 100, "long": False}],
        }

    def prd_self_assessment_latest(self, *, owner_key):
        self.payload = {"owner_key": owner_key}
        return {
            "status": "ok",
            "latest": {
                "payload": {
                    "action": "review",
                    "payload": {
                        "status": "ok",
                        "language": "en",
                        "review": {"status": "completed", "result_markdown": "### Latest Remote Review"},
                        "prd": {"title": "Remote PRD"},
                    },
                }
            },
        }

    def prd_briefing_latest(self, *, owner_key):
        self.payload = {"owner_key": owner_key}
        return {
            "status": "ok",
            "latest": {
                "payload": {
                    "payload": {
                        "status": "ok",
                        "session": {"session_id": "session-remote", "title": "Remote PRD"},
                        "chunks": [{"id": "chunk-1", "title": "Remote", "content": "Body"}],
                    }
                }
            },
        }

    def prd_briefing_process_prd(self, payload):
        self.payload = payload
        return {
            "status": "ok",
            "session": {"session_id": "session-remote", "title": "Remote PRD"},
            "cached": True,
            "chunks": [{"id": "chunk-remote", "title": "Remote", "content": "Body"}],
        }

    def prd_briefing_generate_audio(self, payload):
        self.payload = payload
        return {
            "status": "ok",
            "chunk": {
                "id": payload["chunk"]["id"],
                "title": payload["chunk"].get("title") or "",
                "content": payload["chunk"].get("content") or "",
                "audioUrl": "/prd-briefing/assets/audio/session-remote/mock.mp3",
            },
        }

    def prd_briefing_image_proxy(self, src):
        self.image_src = src

        class Response:
            content = b"local-agent-png"
            status_code = 200
            headers = {"content-type": "image/png"}

        return Response()


class FakeImageProxyConnector:
    base_url = "https://confluence.shopee.io"

    def __init__(self):
        self.requested_url = None

    def _request(self, url, accept):
        self.requested_url = url

        class Response:
            content = b"png-bytes"
            status_code = 200
            headers = {"content-type": "image/png"}

        return Response()


class FakeImageProxyService:
    def __init__(self):
        self.confluence = FakeImageProxyConnector()


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
                "ENV_FILE": os.devnull,
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
            self.assertIn("AI PRD Briefing Tool".encode("utf-8"), response.data)
            self.assertIn(b"Generate Briefing", response.data)
            self.assertIn(b"page-shell-briefing", response.data)
            self.assertIn(b"data-image-lightbox", response.data)
            self.assertNotIn(b"data-prd-review-generate", response.data)
            self.assertIn(b"data-briefing-language", response.data)
            self.assertIn(b"data-briefing-page-ref", response.data)
            self.assertIn(b"/prd-briefing/api/latest", response.data)
            self.assertIn(b"data-presenter-view", response.data)
            self.assertIn(b"data-theater-toggle", response.data)
            self.assertIn(b"Start Presentation Mode", response.data)
            self.assertIn(b"AI Briefing Player", response.data)
            self.assertIn(b"PRD Details", response.data)
            self.assertIn(b"No PRD output yet", response.data)
            self.assertNotIn(b"Developer Walkthrough", response.data)
            self.assertNotIn(b"Developer Follow-up", response.data)
            self.assertNotIn(b"data-prd-review-language", response.data)
            self.assertNotIn(b"No walkthrough yet", response.data)
            self.assertNotIn("3 分钟".encode("utf-8"), response.data)
            self.assertNotIn("生成宣讲".encode("utf-8"), response.data)
            self.assertNotIn("开启宣讲模式".encode("utf-8"), response.data)
            self.assertNotIn(b"Team Knowledge Base", response.data)

    def test_build_service_uses_codex_for_presentation_generation(self):
        FakeCodexTextGenerationClient.init_kwargs = None
        with self.app.app_context(), patch.object(
            prd_blueprint_module,
            "CodexTextGenerationClient",
            FakeCodexTextGenerationClient,
        ):
            service = prd_blueprint_module._build_service()  # noqa: SLF001

        self.assertIsInstance(service.text_client, FakeCodexTextGenerationClient)
        self.assertEqual(FakeCodexTextGenerationClient.init_kwargs["prompt_mode"], "prd_briefing_presentation_chunks_codex")
        self.assertEqual(
            FakeCodexTextGenerationClient.init_kwargs["codex_model"],
            self.app.config["SETTINGS"].prd_briefing_codex_model,
        )

    def test_build_prd_review_service_wires_store_confluence_and_workspace(self):
        created = {}

        class FakeConfluenceConnector:
            def __init__(self, **kwargs):
                created["confluence_kwargs"] = kwargs

        class FakeReviewService:
            def __init__(self, **kwargs):
                created["review_kwargs"] = kwargs

        with self.app.app_context(), patch.object(
            prd_blueprint_module,
            "ConfluenceConnector",
            FakeConfluenceConnector,
        ), patch.object(
            prd_blueprint_module,
            "PRDReviewService",
            FakeReviewService,
        ):
            service = prd_blueprint_module._build_prd_review_service()  # noqa: SLF001

        self.assertIsInstance(service, FakeReviewService)
        self.assertIs(created["confluence_kwargs"]["store"], self.app.config["PRD_BRIEFING_STORE"])
        self.assertIs(created["review_kwargs"]["store"], self.app.config["PRD_BRIEFING_STORE"])
        self.assertIsInstance(created["review_kwargs"]["workspace_root"], Path)

    def test_build_local_agent_client_uses_prd_settings(self):
        settings = SimpleNamespace(
            local_agent_base_url="https://agent.example.test",
            local_agent_hmac_secret="secret",
            local_agent_timeout_seconds=9.5,
            local_agent_connect_timeout_seconds=1.5,
        )

        client = prd_blueprint_module._build_local_agent_client(settings)  # noqa: SLF001

        self.assertEqual(client.base_url, "https://agent.example.test/")
        self.assertEqual(client.hmac_secret, "secret")
        self.assertEqual(client.timeout_seconds, 9)
        self.assertEqual(client.connect_timeout_seconds, 1)

    def test_prd_pages_use_distinct_local_storage_keys(self):
        root = Path(__file__).resolve().parent.parent
        briefing_js = (root / "static" / "prd_briefing.js").read_text()
        self_assessment_js = (root / "static" / "prd_self_assessment.js").read_text()

        self.assertIn("prd-briefing:last-form:v1", briefing_js)
        self.assertIn("prd-self-assessment:last-form:v1", self_assessment_js)
        self.assertIn("/prd-briefing/api/latest", briefing_js)
        self.assertIn("/api/prd-self-assessment/latest", self_assessment_js)
        self.assertIn("timeZone: 'Asia/Singapore'", self_assessment_js)
        self.assertIn("formatSingaporeTimestamp(result.updated_at || '')", self_assessment_js)
        self.assertNotIn("prd-self-assessment:last-form:v1", briefing_js)
        self.assertNotIn("prd-briefing:last-form:v1", self_assessment_js)

    def test_theater_table_cells_keep_dark_text_on_light_background(self):
        root = Path(__file__).resolve().parent.parent
        stylesheet = (root / "static" / "style.css").read_text()

        self.assertIn(".briefing-presenter-media-table th,", stylesheet)
        self.assertIn(".briefing-presenter-media-table td", stylesheet)
        self.assertIn("color: var(--ink);", stylesheet)
        self.assertIn(".briefing-presenter-layout.is-theater .briefing-presenter-media-table td", stylesheet)
        self.assertIn("color: #0f172a", stylesheet)
        self.assertIn(".briefing-presenter-layout.is-theater .briefing-presenter-media-table td *", stylesheet)
        self.assertIn("color: #0f172a !important;", stylesheet)

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_process_prd_endpoint_returns_presentation_chunks(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/process-prd",
                json={"page_ref": "https://example.atlassian.net/wiki/pages/123"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["session"]["model_id"], "codex:gpt-5.5")
        self.assertFalse(payload["cached"])
        self.assertEqual(payload["chunks"][0]["id"], "chunk-1")
        self.assertEqual(payload["chunks"][0]["imageUrls"], ["/prd-briefing/image-proxy?src=x"])
        self.assertEqual(payload["chunks"][0]["media"]["type"], "image")

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_process_prd_endpoint_saves_latest_presentation(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/process-prd",
                json={"page_ref": "https://example.atlassian.net/wiki/pages/123"},
            )
            self.assertEqual(response.status_code, 200)
            latest_response = client.get("/prd-briefing/api/latest")

        self.assertEqual(latest_response.status_code, 200)
        latest = latest_response.get_json()["latest"]
        self.assertEqual(latest["payload"]["payload"]["session"]["session_id"], "session-1")
        self.assertEqual(latest["payload"]["payload"]["chunks"][0]["id"], "chunk-1")

    @patch("prd_briefing.blueprint._build_service", return_value=FakeBriefingService())
    def test_generate_audio_endpoint_returns_presentation_chunk(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/generate-audio",
                json={
                    "session_id": "session-1",
                    "chunk": {
                        "id": "chunk-1",
                        "title": "开场",
                        "content": "这一段给开发说明主流程。",
                        "media": {"type": "table", "content": "<table><tr><th>A</th></tr></table>"},
                    },
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["chunk"]["audioUrl"], "/prd-briefing/assets/audio/session-1/mock.mp3")
        self.assertEqual(payload["chunk"]["timestamps"][0]["start"], 0)
        self.assertEqual(payload["chunk"]["media"]["type"], "table")

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
    def test_get_session_endpoint_returns_payload(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.get("/prd-briefing/api/session/session-1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["session"]["session_id"], "session-1")

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

    @patch("prd_briefing.blueprint._local_agent_prd_briefing_enabled", return_value=True)
    def test_prd_briefing_latest_endpoint_can_route_to_local_agent(self, _mock_enabled):
        fake_client = FakePRDReviewLocalAgentClient()
        with patch("prd_briefing.blueprint._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                response = client.get("/prd-briefing/api/latest")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["latest"]["payload"]["payload"]["session"]["session_id"], "session-remote")
        self.assertEqual(fake_client.payload["owner_key"], "google:xiaodong.zheng@npt.sg")

    def test_portal_route_blocks_non_admin_npt_google_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {
                    "token": "x",
                    "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
                }

            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/access-denied")

    def test_portal_route_blocks_test_gmail_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng1991@gmail.com", "name": "Test User"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/prd-briefing/", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/access-denied")

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

    def test_prd_briefing_api_access_gate_returns_stable_json(self):
        with self.app.test_client() as client:
            anonymous = client.post("/prd-briefing/api/session", json={"page_ref": "https://example.test/prd"})
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}
            signed_in = client.post("/prd-briefing/api/session", json={"page_ref": "https://example.test/prd"})

        self.assertEqual(anonymous.status_code, 401)
        self.assertIn("Sign in", anonymous.get_json()["message"])
        self.assertEqual(signed_in.status_code, 403)
        self.assertIn("signed-in portal users", signed_in.get_json()["message"])

    @patch("prd_briefing.blueprint._build_service")
    def test_prd_briefing_session_and_followup_errors_return_json(self, mock_service):
        class BrokenBriefingService:
            def create_session(self, **_kwargs):
                raise RuntimeError("session failed")

            def get_session_payload(self, **_kwargs):
                raise RuntimeError("load failed")

            def answer_question(self, **_kwargs):
                raise RuntimeError("answer failed")

            def narrate_section(self, **_kwargs):
                raise RuntimeError("narrate failed")

        mock_service.return_value = BrokenBriefingService()
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            create_response = client.post("/prd-briefing/api/session", json={"page_ref": "https://example.test/prd"})
            get_response = client.get("/prd-briefing/api/session/session-1")
            answer_response = client.post("/prd-briefing/api/session/session-1/answer", json={"question": "What?"})
            narrate_response = client.post(
                "/prd-briefing/api/session/session-1/narrate",
                json={"section_index": "bad-index"},
            )

        self.assertEqual(create_response.status_code, 400)
        self.assertIn("session failed", create_response.get_json()["message"])
        self.assertEqual(get_response.status_code, 400)
        self.assertIn("load failed", get_response.get_json()["message"])
        self.assertEqual(answer_response.status_code, 400)
        self.assertIn("answer failed", answer_response.get_json()["message"])
        self.assertEqual(narrate_response.status_code, 400)
        self.assertIn("invalid literal", narrate_response.get_json()["message"])

    @patch("prd_briefing.blueprint._build_prd_review_service")
    def test_prd_briefing_review_unexpected_error_returns_json(self, mock_service):
        class BrokenReviewService:
            def review_url(self, _request):
                raise RuntimeError("review exploded")

        mock_service.return_value = BrokenReviewService()
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/review",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("review exploded", response.get_json()["message"])

    def test_process_prd_error_boundaries_are_sanitized(self):
        class ToolErrorBriefingService:
            def process_prd_for_presentation(self, **_kwargs):
                raise ToolError("PRD content is unavailable")

        class RuntimeErrorBriefingService:
            def process_prd_for_presentation(self, **_kwargs):
                raise RuntimeError("processing failed")

        class PermissionErrorBriefingService:
            def process_prd_for_presentation(self, **_kwargs):
                raise ValueError("403 forbidden token=secret")

        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            with patch("prd_briefing.blueprint._build_service", return_value=ToolErrorBriefingService()):
                tool_error = client.post("/prd-briefing/api/process-prd", json={"page_ref": "https://example.test/prd"})
            with patch("prd_briefing.blueprint._build_service", return_value=RuntimeErrorBriefingService()):
                runtime_error = client.post("/prd-briefing/api/process-prd", json={"page_ref": "https://example.test/prd"})
            with patch("prd_briefing.blueprint._build_service", return_value=PermissionErrorBriefingService()):
                permission_error = client.post("/prd-briefing/api/process-prd", json={"page_ref": "https://example.test/prd"})

        self.assertEqual(tool_error.status_code, 400)
        self.assertIn("PRD content is unavailable", tool_error.get_json()["message"])
        self.assertEqual(runtime_error.status_code, 400)
        self.assertIn("processing failed", runtime_error.get_json()["message"])
        self.assertEqual(permission_error.status_code, 400)
        self.assertIn("Confluence access failed", permission_error.get_json()["message"])
        self.assertNotIn("secret", permission_error.get_json()["message"])

    @patch("prd_briefing.blueprint._local_agent_prd_briefing_enabled", return_value=True)
    def test_process_prd_and_audio_can_route_to_local_agent(self, _mock_enabled):
        fake_client = FakePRDReviewLocalAgentClient()
        with patch("prd_briefing.blueprint._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                    session["google_credentials"] = {"token": "x"}
                process_response = client.post(
                    "/prd-briefing/api/process-prd",
                    json={"page_ref": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
                )
                audio_response = client.post(
                    "/prd-briefing/api/generate-audio",
                    json={"session_id": "session-remote", "chunk": {"id": "chunk-remote", "title": "Remote", "content": "Body"}},
                )

        self.assertEqual(process_response.status_code, 200)
        self.assertEqual(process_response.get_json()["chunks"][0]["id"], "chunk-remote")
        self.assertEqual(audio_response.status_code, 200)
        self.assertIn("audioUrl", audio_response.get_json()["chunk"])
        self.assertEqual(fake_client.payload["chunk"]["id"], "chunk-remote")

    @patch("prd_briefing.blueprint._local_agent_prd_briefing_enabled", return_value=True)
    def test_prd_briefing_latest_local_agent_errors_return_json(self, _mock_enabled):
        class ToolErrorLocalAgentClient:
            def prd_briefing_latest(self, *, owner_key):
                raise ToolError(f"local-agent unavailable for {owner_key}")

        class BrokenLocalAgentClient:
            def prd_briefing_latest(self, *, owner_key):
                raise RuntimeError("latest exploded")

        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            with patch("prd_briefing.blueprint._build_local_agent_client", return_value=ToolErrorLocalAgentClient()):
                tool_error = client.get("/prd-briefing/api/latest")
            with patch("prd_briefing.blueprint._build_local_agent_client", return_value=BrokenLocalAgentClient()):
                unexpected = client.get("/prd-briefing/api/latest")

        self.assertEqual(tool_error.status_code, 400)
        self.assertIn("local-agent unavailable", tool_error.get_json()["message"])
        self.assertEqual(unexpected.status_code, 400)
        self.assertIn("latest exploded", unexpected.get_json()["message"])

    @patch("prd_briefing.blueprint._build_service")
    def test_generate_audio_error_returns_json(self, mock_service):
        class BrokenAudioService:
            def generate_presentation_audio(self, **_kwargs):
                raise RuntimeError("audio failed")

        mock_service.return_value = BrokenAudioService()
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/prd-briefing/api/generate-audio",
                json={"session_id": "session-1", "chunk": {"id": "chunk-1", "content": "Body"}},
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("audio failed", response.get_json()["message"])

    def test_prd_briefing_asset_route_blocks_escape_and_missing_assets(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            missing = client.get("/prd-briefing/assets/audio/session-1/missing.mp3")
            escaped = client.get("/prd-briefing/assets/../secret.txt")

        self.assertEqual(missing.status_code, 404)
        self.assertIn("Asset not found", missing.get_json()["message"])
        self.assertEqual(escaped.status_code, 404)
        self.assertIn("Invalid asset path", escaped.get_json()["message"])

    def test_prd_briefing_asset_route_serves_existing_store_file(self):
        store_root = self.app.config["PRD_BRIEFING_STORE"].root_dir
        audio_file = store_root / "audio" / "session-1" / "mock.mp3"
        audio_file.parent.mkdir(parents=True, exist_ok=True)
        audio_file.write_bytes(b"mp3-bytes")

        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.get("/prd-briefing/assets/audio/session-1/mock.mp3")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"mp3-bytes")

    def test_prd_briefing_latest_without_owner_key_skips_store(self):
        with self.app.app_context():
            self.assertIsNone(prd_blueprint_module._get_latest_result(owner_key="", tool_key="prd_briefing"))  # noqa: SLF001
            prd_blueprint_module._save_latest_result(owner_key="", tool_key="prd_briefing", payload={"payload": {}})  # noqa: SLF001

    def test_prd_briefing_local_agent_helper_predicates(self):
        settings = SimpleNamespace(
            local_agent_mode="cloud_run",
            local_agent_base_url="https://agent.example.test",
            local_agent_hmac_secret="secret",
            local_agent_source_code_qa_enabled=True,
        )
        self.assertTrue(prd_blueprint_module._local_agent_prd_briefing_enabled(settings))  # noqa: SLF001
        self.assertTrue(prd_blueprint_module._local_agent_source_code_qa_enabled(settings))  # noqa: SLF001
        self.assertFalse(prd_blueprint_module._is_loopback_url("https://agent.example.test"))  # noqa: SLF001
        self.assertTrue(prd_blueprint_module._is_loopback_url("http://127.0.0.1:8787"))  # noqa: SLF001

        settings.local_agent_hmac_secret = ""
        self.assertFalse(prd_blueprint_module._local_agent_prd_briefing_enabled(settings))  # noqa: SLF001
        settings.local_agent_hmac_secret = "secret"
        settings.local_agent_source_code_qa_enabled = False
        self.assertFalse(prd_blueprint_module._local_agent_source_code_qa_enabled(settings))  # noqa: SLF001
        settings.local_agent_mode = "disabled"
        self.assertFalse(prd_blueprint_module._local_agent_prd_briefing_enabled(settings))  # noqa: SLF001

    def test_prd_self_assessment_route_allows_npt_google_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {
                    "token": "x",
                    "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
                }

            response = client.get("/prd-self-assessment/", follow_redirects=False)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"PRD Self-Assessment", response.data)
            self.assertIn(b"data-prd-self-assessment-url", response.data)
            self.assertIn(b"data-prd-self-assessment-language", response.data)
            self.assertNotIn(b"Generate PRD Summary", response.data)
            self.assertIn(b"Generate AI PRD Review", response.data)
            self.assertIn(b"data-latest-url", response.data)

    def test_prd_self_assessment_route_allows_test_gmail_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng1991@gmail.com", "name": "Test User"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/prd-self-assessment/", follow_redirects=False)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"PRD Self-Assessment", response.data)

    def test_prd_self_assessment_route_blocks_unapproved_google_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "outsider@gmail.com", "name": "Outsider"}
                session["google_credentials"] = {"token": "x"}

            response = client.get("/prd-self-assessment/", follow_redirects=False)
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/access-denied")

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDReviewService())
    def test_prd_self_assessment_review_endpoint_returns_chinese_markdown(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/api/prd-self-assessment/review",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "zh"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["language"], "zh")
        self.assertIn("### Review", payload["review"]["result_markdown"])

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDReviewService())
    def test_prd_self_assessment_review_endpoint_can_queue_async_job(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/api/prd-self-assessment/review",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "zh", "async": True},
            )
            self.assertEqual(response.status_code, 200)
            queued = response.get_json()
            self.assertEqual(queued["status"], "queued")
            job_payload = {}
            for _ in range(20):
                job_payload = client.get(f"/api/jobs/{queued['job_id']}").get_json()
                if job_payload.get("state") == "completed":
                    break
                time.sleep(0.05)

        self.assertEqual(job_payload["state"], "completed")
        self.assertEqual(job_payload["results"][0]["review"]["result_markdown"], "### Review\n- Good")

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDReviewService())
    def test_prd_self_assessment_sections_endpoint_returns_metadata(self, mock_service):
        service = mock_service.return_value
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/api/prd-self-assessment/sections",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual([section["index"] for section in payload["sections"]], [1, 2])
        self.assertTrue(payload["sections"][1]["long"])
        self.assertEqual(service.requests[-1].language, "en")

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDReviewService())
    def test_prd_self_assessment_review_endpoint_passes_selected_sections(self, mock_service):
        service = mock_service.return_value
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {
                    "token": "x",
                    "scopes": ["https://www.googleapis.com/auth/drive.readonly"],
                }
            response = client.post(
                "/api/prd-self-assessment/review",
                json={
                    "prd_url": "https://example.atlassian.net/wiki/pages/123",
                    "language": "en",
                    "selected_section_indexes": [2, 2, 1],
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(service.requests[-1].selected_section_indexes, [2, 2, 1])
        self.assertEqual(service.requests[-1].google_credentials["token"], "x")

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDReviewService())
    def test_prd_self_assessment_summary_endpoint_passes_english_language(self, mock_service):
        service = mock_service.return_value
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/api/prd-self-assessment/summary",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["language"], "en")
        self.assertIn("### Summary", payload["summary"]["result_markdown"])
        self.assertEqual(service.summary_requests[-1].language, "en")

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDReviewService())
    def test_prd_self_assessment_endpoint_saves_latest_result(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/api/prd-self-assessment/summary",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
            )
            self.assertEqual(response.status_code, 200)
            latest_response = client.get("/api/prd-self-assessment/latest")

        self.assertEqual(latest_response.status_code, 200)
        latest = latest_response.get_json()["latest"]
        self.assertEqual(latest["payload"]["action"], "summary")
        self.assertEqual(latest["payload"]["payload"]["summary"]["result_markdown"], "### Summary\n- Good")

    @patch("bpmis_jira_tool.web._build_prd_review_service", return_value=FakePRDReviewService())
    def test_prd_self_assessment_endpoint_validates_http_prd_link(self, _mock_service):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Xiaodong Zheng"}
                session["google_credentials"] = {"token": "x"}
            response = client.post("/api/prd-self-assessment/summary", json={"prd_url": "not-a-url"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("HTTP or HTTPS", response.get_json()["message"])

    def test_prd_self_assessment_summary_endpoint_blocks_non_admin_user(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                session["google_credentials"] = {"token": "x"}
            response = client.post(
                "/api/prd-self-assessment/summary",
                json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["status"], "error")

    @patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True)
    def test_prd_self_assessment_endpoint_can_route_to_local_agent(self, _mock_enabled):
        fake_client = FakePRDReviewLocalAgentClient()
        with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post(
                    "/api/prd-self-assessment/review",
                    json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["cached"])
        self.assertEqual(payload["review"]["result_markdown"], "### Remote Review")
        self.assertEqual(fake_client.payload["language"], "en")

    @patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True)
    def test_prd_self_assessment_sections_endpoint_can_route_to_local_agent(self, _mock_enabled):
        fake_client = FakePRDReviewLocalAgentClient()
        with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post(
                    "/api/prd-self-assessment/sections",
                    json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["sections"][0]["title"], "Remote Section")
        self.assertEqual(fake_client.payload["owner_key"], "google:teammate@npt.sg")

    @patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True)
    def test_prd_self_assessment_latest_endpoint_can_route_to_local_agent(self, _mock_enabled):
        fake_client = FakePRDReviewLocalAgentClient()
        with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                response = client.get("/api/prd-self-assessment/latest")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["latest"]["payload"]["payload"]["review"]["result_markdown"], "### Latest Remote Review")
        self.assertEqual(fake_client.payload["owner_key"], "google:teammate@npt.sg")

    def test_prd_self_assessment_external_failure_matrix_returns_stable_json(self):
        class FailingService:
            def __init__(self, message):
                self.message = message

            def review_url(self, _request):
                raise ToolError(self.message)

            def summarize_url(self, _request):
                raise ToolError(self.message)

        scenarios = [
            ("review", "/api/prd-self-assessment/review", "PRD upstream timed out."),
            ("summary", "/api/prd-self-assessment/summary", "PRD upstream returned 403."),
            ("summary", "/api/prd-self-assessment/summary", "PRD upstream returned an empty response."),
        ]
        for action, path, message in scenarios:
            with self.subTest(action=action, message=message), patch(
                "bpmis_jira_tool.web._build_prd_review_service",
                return_value=FailingService(message),
            ):
                with self.app.test_client() as client:
                    with client.session_transaction() as session:
                        email = "xiaodong.zheng@npt.sg" if action == "summary" else "teammate@npt.sg"
                        name = "Xiaodong Zheng" if action == "summary" else "Teammate"
                        session["google_profile"] = {"email": email, "name": name}
                        session["google_credentials"] = {"token": "x"}
                    response = client.post(path, json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"})

            self.assertEqual(response.status_code, 400)
            payload = response.get_json()
            self.assertEqual(payload["status"], "error")
            self.assertIn(message, payload["message"])
            serialized = str(payload).lower()
            self.assertNotIn("traceback", serialized)
            self.assertNotIn("secret", serialized)
            self.assertNotIn("token", serialized)

    @patch("bpmis_jira_tool.web._local_agent_source_code_qa_enabled", return_value=True)
    def test_prd_self_assessment_local_agent_timeout_returns_stable_json(self, _mock_enabled):
        class TimeoutLocalAgentClient:
            def prd_self_assessment_review(self, _payload):
                raise ToolError("local-agent request timed out.")

        with patch("bpmis_jira_tool.web._build_local_agent_client", return_value=TimeoutLocalAgentClient()):
            with self.app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "teammate@npt.sg", "name": "Teammate"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post(
                    "/api/prd-self-assessment/review",
                    json={"prd_url": "https://example.atlassian.net/wiki/pages/123", "language": "en"},
                )

        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertIn("timed out", payload["message"])
        self.assertNotIn("traceback", str(payload).lower())

    @patch("prd_briefing.blueprint._build_service", return_value=FakeImageProxyService())
    def test_image_proxy_allows_confluence_attachment_without_session(self, mock_service):
        with self.app.test_client() as client:
            response = client.get(
                "/prd-briefing/image-proxy",
                query_string={
                    "src": "https://confluence.shopee.io/download/attachments/123/mock.png?api=v2"
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, "image/png")
        self.assertEqual(response.data, b"png-bytes")
        self.assertEqual(
            mock_service.return_value.confluence.requested_url,
            "https://confluence.shopee.io/download/attachments/123/mock.png?api=v2",
        )

    @patch("prd_briefing.blueprint._local_agent_prd_briefing_enabled", return_value=True)
    @patch("prd_briefing.blueprint._is_loopback_url", return_value=False)
    @patch("prd_briefing.blueprint._build_service", return_value=FakeImageProxyService())
    def test_image_proxy_routes_to_signed_local_agent(self, _mock_service, _mock_loopback, _mock_enabled):
        fake_client = FakePRDReviewLocalAgentClient()
        source_url = "https://confluence.shopee.io/download/attachments/123/mock.png?api=v2"
        with patch("prd_briefing.blueprint._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                response = client.get(
                    "/prd-briefing/image-proxy",
                    query_string={"src": source_url},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, "image/png")
        self.assertEqual(response.data, b"local-agent-png")
        self.assertEqual(fake_client.image_src, source_url)

    @patch("prd_briefing.blueprint._build_service", return_value=FakeImageProxyService())
    def test_image_proxy_rejects_non_confluence_source(self, _mock_service):
        with self.app.test_client() as client:
            response = client.get(
                "/prd-briefing/image-proxy",
                query_string={"src": "https://example.com/download/attachments/123/mock.png"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported image source", response.get_json()["message"])

    @patch("prd_briefing.blueprint._build_service", return_value=FakeImageProxyService())
    def test_image_proxy_rejects_missing_or_non_http_source(self, _mock_service):
        with self.app.test_client() as client:
            missing = client.get("/prd-briefing/image-proxy")
            non_http = client.get("/prd-briefing/image-proxy", query_string={"src": "file:///tmp/mock.png"})

        self.assertEqual(missing.status_code, 400)
        self.assertIn("Missing image source", missing.get_json()["message"])
        self.assertEqual(non_http.status_code, 400)
        self.assertIn("Unsupported image source", non_http.get_json()["message"])

    def test_image_source_allowlist_rejects_missing_base_and_wrong_paths(self):
        self.assertFalse(
            prd_blueprint_module._is_allowed_confluence_image_source(  # noqa: SLF001
                "file:///tmp/mock.png",
                "https://confluence.shopee.io",
            )
        )
        self.assertFalse(
            prd_blueprint_module._is_allowed_confluence_image_source(  # noqa: SLF001
                "https://confluence.shopee.io/download/attachments/123/mock.png",
                "",
            )
        )
        self.assertFalse(
            prd_blueprint_module._is_allowed_confluence_image_source(  # noqa: SLF001
                "https://confluence.shopee.io/pages/viewpage.action?pageId=123",
                "https://confluence.shopee.io",
            )
        )
        self.assertTrue(
            prd_blueprint_module._is_allowed_confluence_image_source(  # noqa: SLF001
                "https://confluence.shopee.io/download/thumbnails/123/mock.png",
                "https://confluence.shopee.io",
            )
        )

    @patch("prd_briefing.blueprint._local_agent_prd_briefing_enabled", return_value=True)
    @patch("prd_briefing.blueprint._is_loopback_url", return_value=False)
    @patch("prd_briefing.blueprint._build_service")
    def test_image_proxy_uses_default_confluence_base_for_remote_local_agent(self, mock_service, _mock_loopback, _mock_enabled):
        class NoBaseImageProxyService:
            def __init__(self):
                self.confluence = type("Connector", (), {"base_url": ""})()

        fake_client = FakePRDReviewLocalAgentClient()
        mock_service.return_value = NoBaseImageProxyService()
        with patch("prd_briefing.blueprint._build_local_agent_client", return_value=fake_client):
            with self.app.test_client() as client:
                response = client.get(
                    "/prd-briefing/image-proxy",
                    query_string={"src": "https://confluence.shopee.io/download/thumbnails/123/mock.png"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_client.image_src, "https://confluence.shopee.io/download/thumbnails/123/mock.png")


if __name__ == "__main__":
    unittest.main()
