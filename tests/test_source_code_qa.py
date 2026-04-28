import json
import io
import os
from datetime import date, timedelta
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
import zipfile
from unittest.mock import patch
from types import SimpleNamespace

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import (
    CodexCliBridgeSourceCodeQALLMProvider,
    LLMGenerateResult,
    RepositoryEntry,
    SourceCodeQALLMError,
    SourceCodeQAService,
    VertexAIEmbeddingProvider,
    VertexAISourceCodeQALLMProvider,
)
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS
from bpmis_jira_tool.web import JobStore, SourceCodeQASessionStore, create_app
from scripts.promote_source_code_qa_eval_candidates import promote_candidates
from scripts.run_source_code_qa_evals import _build_fixture_repositories, _evaluate_case, _guard_fixture_data_root
from scripts.source_code_qa_feedback_to_eval import build_eval_candidates


class SourceCodeQARouteTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
                "SOURCE_CODE_QA_GEMINI_API_KEY": "",
                "GEMINI_API_KEY": "",
            },
            clear=False,
        ):
            self.app = create_app()
            self.app.testing = True

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def _login(client, email="teammate@npt.sg"):
        with client.session_transaction() as session:
            session["google_profile"] = {"email": email, "name": "Portal User"}
            session["google_credentials"] = {"token": "x", "scopes": []}

    def test_npt_user_sees_source_code_tab_after_bpmis(self):
        with self.app.test_client() as client:
            self._login(client)
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Source Code Q&amp;A", html)
        self.assertLess(html.index("BPMIS Automation Tool"), html.index("Source Code Q&amp;A"))

    def test_whitelisted_gmail_user_also_sees_source_code_tab(self):
        with self.app.test_client() as client:
            self._login(client, "xiaodong.zheng1991@gmail.com")
            response = client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Source Code Q&amp;A", response.data)

    def test_non_npt_user_is_blocked_from_page_and_api(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session["google_profile"] = {"email": "teammate@example.com", "name": "External User"}
                session["google_credentials"] = {"token": "x", "scopes": []}
            page_response = client.get("/source-code-qa", follow_redirects=False)
            api_response = client.post("/api/source-code-qa/query", json={"pm_team": "AF", "country": "All", "question": "test"})

        self.assertIn(page_response.status_code, {302, 403})
        self.assertEqual(api_response.status_code, 403)

    def test_owner_sees_admin_controls_but_teammate_does_not(self):
        with self.app.test_client() as client:
            self._login(client, "xiaodong.zheng@npt.sg")
            owner_response = client.get("/source-code-qa")
            self._login(client, "teammate@npt.sg")
            teammate_response = client.get("/source-code-qa")

        self.assertIn(b"Repository Mapping", owner_response.data)
        self.assertIn(b"Repo Admin", owner_response.data)
        self.assertIn(b'data-tab-trigger="admin"', owner_response.data)
        self.assertIn(b'data-source-view-tab="admin"', owner_response.data)
        self.assertIn(b'data-tab-panel="admin"', owner_response.data)
        self.assertIn(b'data-source-view-panel="admin"', owner_response.data)
        self.assertIn(b"Save Config", owner_response.data)
        self.assertIn(b"Sync / Refresh", owner_response.data)
        self.assertNotIn(b"Repository Mapping", teammate_response.data)
        self.assertNotIn(b"Repo Admin", teammate_response.data)
        self.assertNotIn(b'data-tab-trigger="admin"', teammate_response.data)
        self.assertNotIn(b'data-source-view-tab="admin"', teammate_response.data)
        self.assertNotIn(b"Save Config", teammate_response.data)
        self.assertNotIn(b"Sync / Refresh", teammate_response.data)
        self.assertIn(b"data-source-question", teammate_response.data)
        self.assertIn(b"data-source-live-answer", teammate_response.data)
        self.assertNotIn(b"LLM Budget", teammate_response.data)
        self.assertIn(b"data-source-llm-provider", teammate_response.data)
        self.assertIn(b"data-source-session-list", teammate_response.data)
        self.assertIn(b"data-source-new-session", teammate_response.data)
        self.assertIn(b"data-source-session-messages", teammate_response.data)
        self.assertIn(b"Codex", teammate_response.data)
        self.assertIn(b'value="gemini" disabled', teammate_response.data)
        self.assertIn(b"Vertex AI", teammate_response.data)
        self.assertIn(b"Model Availability", owner_response.data)
        self.assertNotIn(b"Model Availability", teammate_response.data)

    def test_source_code_qa_admin_allowlist_can_manage_repositories(self):
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "SOURCE_CODE_QA_ADMIN_EMAILS": "other-owner@npt.sg,xiaodong.zheng1991@gmail.com",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

        with app.test_client() as client:
            self._login(client, "xiaodong.zheng1991@gmail.com")
            page_response = client.get("/source-code-qa")
            config_response = client.get("/api/source-code-qa/config")

        self.assertIn(b"Repository Mapping", page_response.data)
        self.assertTrue(config_response.get_json()["can_manage"])

    def test_source_code_qa_builtin_owner_can_manage_when_env_owner_changes(self):
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "SOURCE_CODE_QA_OWNER_EMAIL": "temporary-owner@npt.sg",
                "SOURCE_CODE_QA_ADMIN_EMAILS": "",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

        with app.test_client() as client:
            self._login(client, "xiaodong.zheng@npt.sg")
            page_response = client.get("/source-code-qa")
            config_response = client.get("/api/source-code-qa/config")

        config_payload = config_response.get_json()
        self.assertIn(b"Repository Mapping", page_response.data)
        self.assertTrue(config_payload["can_manage"])
        self.assertEqual(config_payload["auth"]["admin_match_source"], "builtin_admin")

    def test_source_code_qa_default_owner_alias_can_manage_repositories(self):
        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "SOURCE_CODE_QA_GITLAB_TOKEN": "secret-token",
            },
            clear=True,
        ):
            app = create_app()
            app.testing = True

        sync_result = {
            "status": "ok",
            "key": "AF:All",
            "results": [{"state": "ok", "display_name": "Repo One"}],
            "repo_status": [{"display_name": "Repo One", "state": "synced", "message": "ready"}],
        }
        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.sync", return_value=sync_result):
            with app.test_client() as client:
                self._login(client, "xiaodong.zheng1991@gmail.com")
                page_response = client.get("/source-code-qa")
                config_response = client.get("/api/source-code-qa/config")
                save_response = client.post(
                    "/api/source-code-qa/config",
                    json={"pm_team": "AF", "country": "All", "repositories": [{"url": "https://git.example.com/team/repo.git"}]},
                )
                sync_response = client.post("/api/source-code-qa/sync", json={"pm_team": "AF", "country": "All"})
                sync_payload = sync_response.get_json()
                sync_snapshot = {}
                for _ in range(20):
                    job_response = client.get(f"/api/jobs/{sync_payload['job_id']}")
                    sync_snapshot = job_response.get_json()
                    if sync_snapshot.get("state") == "completed":
                        break
                    time.sleep(0.05)

        config_payload = config_response.get_json()
        self.assertIn(b"Repository Mapping", page_response.data)
        self.assertTrue(config_payload["can_manage"])
        self.assertEqual(config_payload["auth"]["signed_in_email"], "xiaodong.zheng1991@gmail.com")
        self.assertEqual(config_payload["auth"]["admin_match_source"], "admin_allowlist")
        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(sync_response.status_code, 200)
        self.assertEqual(sync_payload["status"], "queued")
        self.assertEqual(sync_snapshot.get("state"), "completed")

    def test_source_code_qa_manage_forbidden_reports_signed_in_email(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.post(
                "/api/source-code-qa/config",
                json={"pm_team": "AF", "country": "All", "repositories": [{"url": "https://git.example.com/team/repo.git"}]},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(payload["auth"]["signed_in_email"], "teammate@npt.sg")
        self.assertFalse(payload["auth"]["can_manage"])
        self.assertIn("Signed in as teammate@npt.sg", payload["message"])

    def test_config_save_is_owner_only_and_validates_https(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            forbidden = client.post(
                "/api/source-code-qa/config",
                json={"pm_team": "AF", "country": "All", "repositories": [{"url": "https://git.example.com/team/repo.git"}]},
            )
            self._login(client, "xiaodong.zheng@npt.sg")
            invalid = client.post(
                "/api/source-code-qa/config",
                json={"pm_team": "AF", "country": "All", "repositories": [{"url": "git@git.example.com:team/repo.git"}]},
            )
            valid = client.post(
                "/api/source-code-qa/config",
                json={
                    "pm_team": "AF",
                    "country": "SG",
                    "repositories": [{"display_name": "Repo One", "url": "https://git.example.com/team/repo.git"}],
                },
            )

        self.assertEqual(forbidden.status_code, 403)
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(valid.status_code, 200)
        payload = valid.get_json()
        self.assertEqual(payload["key"], "AF:All")
        self.assertEqual(payload["repositories"][0]["display_name"], "Repo One")

    def test_model_availability_is_owner_only_and_disables_provider_option(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            forbidden = client.post(
                "/api/source-code-qa/model-availability",
                json={"availability": {"codex_cli_bridge": True, "gemini": False, "vertex_ai": False}},
            )
            self._login(client, "xiaodong.zheng@npt.sg")
            saved = client.post(
                "/api/source-code-qa/model-availability",
                json={"availability": {"codex_cli_bridge": True, "gemini": False, "vertex_ai": False}},
            )
            self._login(client, "teammate@npt.sg")
            config_response = client.get("/api/source-code-qa/config")
            page_response = client.get("/source-code-qa")

        self.assertEqual(forbidden.status_code, 403)
        self.assertEqual(saved.status_code, 200)
        payload = config_response.get_json()
        vertex_option = next(item for item in payload["options"]["llm_providers"] if item["value"] == "vertex_ai")
        codex_option = next(item for item in payload["options"]["llm_providers"] if item["value"] == "codex_cli_bridge")
        self.assertTrue(vertex_option["disabled"])
        self.assertFalse(codex_option["disabled"])
        self.assertIn(b'value="vertex_ai" disabled', page_response.data)

    def test_query_api_rejects_unavailable_model_provider(self):
        with self.app.test_client() as client:
            self._login(client, "xiaodong.zheng@npt.sg")
            client.post(
                "/api/source-code-qa/model-availability",
                json={"availability": {"codex_cli_bridge": True, "gemini": False, "vertex_ai": False}},
            )
            self._login(client, "teammate@npt.sg")
            response = client.post(
                "/api/source-code-qa/query",
                json={
                    "pm_team": "AF",
                    "country": "All",
                    "question": "where is createIssue",
                    "answer_mode": "auto",
                    "llm_provider": "vertex_ai",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["message"], "Selected Source Code Q&A model is unavailable.")

    def test_source_code_qa_session_api_creates_lists_and_loads_session(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            created = client.post(
                "/api/source-code-qa/sessions",
                json={"pm_team": "AF", "country": "All", "llm_provider": "vertex_ai"},
            )
            self.assertEqual(created.status_code, 200)
            session_payload = created.get_json()["session"]
            listed = client.get("/api/source-code-qa/sessions")
            loaded = client.get(f"/api/source-code-qa/sessions/{session_payload['id']}")

        self.assertEqual(listed.status_code, 200)
        self.assertEqual(loaded.status_code, 200)
        self.assertEqual(listed.get_json()["sessions"][0]["id"], session_payload["id"])
        self.assertEqual(loaded.get_json()["session"]["llm_provider"], "vertex_ai")

    def test_source_code_qa_session_archive_hides_but_preserves_session(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            created = client.post(
                "/api/source-code-qa/sessions",
                json={"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"},
            )
            session_id = created.get_json()["session"]["id"]
            archived = client.post(f"/api/source-code-qa/sessions/{session_id}/archive")
            listed = client.get("/api/source-code-qa/sessions")
            loaded = client.get(f"/api/source-code-qa/sessions/{session_id}")

        self.assertEqual(archived.status_code, 200)
        self.assertEqual(archived.get_json()["status"], "ok")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.get_json()["sessions"], [])
        self.assertEqual(loaded.status_code, 200)
        self.assertEqual(loaded.get_json()["session"]["archived_at"], archived.get_json()["archived_at"])

    def test_source_code_qa_session_archive_rejects_other_owner(self):
        with self.app.test_client() as client:
            self._login(client, "owner@npt.sg")
            created = client.post(
                "/api/source-code-qa/sessions",
                json={"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"},
            )
            session_id = created.get_json()["session"]["id"]
            self._login(client, "other@npt.sg")
            archived = client.post(f"/api/source-code-qa/sessions/{session_id}/archive")

        self.assertEqual(archived.status_code, 404)

    def test_query_api_uses_session_context_and_appends_exchange(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {
                "status": "ok",
                "answer_mode": "auto",
                "summary": "answer summary",
                "llm_answer": "direct answer",
                "llm_provider": "codex_cli_bridge",
                "llm_model": "codex-cli",
                "trace_id": "trace-123",
                "structured_answer": {"direct_answer": "direct answer", "claims": [], "citations": [], "missing_evidence": [], "confidence": "high"},
                "matches": [{"repo": "Repo", "path": "src/App.java", "line_start": 1, "line_end": 3, "score": 9, "reason": "hit"}],
            }

        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            created = client.post("/api/source-code-qa/sessions", json={"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"})
            session_id = created.get_json()["session"]["id"]
            store: SourceCodeQASessionStore = self.app.config["SOURCE_CODE_QA_SESSION_STORE"]
            store.append_exchange(
                session_id,
                owner_email="teammate@npt.sg",
                pm_team="AF",
                country="All",
                llm_provider="codex_cli_bridge",
                question="first question",
                result={"status": "ok", "summary": "first", "llm_answer": "first answer", "trace_id": "trace-1", "matches": []},
                context={"question": "first question", "trace_id": "trace-1", "matches_snapshot": [{"path": "src/First.java"}]},
            )
            with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today", return_value={"attempted": False, "status": "fresh"}), patch(
                "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
                side_effect=fake_query,
            ):
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "session_id": session_id,
                        "pm_team": "AF",
                        "country": "All",
                        "question": "follow up",
                        "answer_mode": "auto",
                        "llm_provider": "codex_cli_bridge",
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(captured["conversation_context"]["trace_id"], "trace-1")
        self.assertEqual(payload["session_id"], session_id)
        self.assertEqual(payload["session"]["message_count"], 4)
        self.assertEqual(payload["session"]["last_context"]["question"], "follow up")
        self.assertEqual(payload["session"]["last_context"]["recent_turns"][0]["question"], "first question")
        self.assertEqual(payload["session"]["last_context"]["recent_turns"][0]["trace_id"], "trace-1")

    def test_source_code_qa_attachment_upload_is_session_scoped(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            created = client.post("/api/source-code-qa/sessions", json={"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"})
            session_id = created.get_json()["session"]["id"]
            upload = client.post(
                "/api/source-code-qa/attachments",
                data={
                    "session_id": session_id,
                    "file": (io.BytesIO(b"ticket field notes"), "notes.txt"),
                },
                content_type="multipart/form-data",
            )
            attachment = upload.get_json()["attachment"]
            downloaded = client.get(f"/api/source-code-qa/attachments/{attachment['id']}?session_id={session_id}")

            self._login(client, "other@npt.sg")
            blocked = client.get(f"/api/source-code-qa/attachments/{attachment['id']}?session_id={session_id}")

        self.assertEqual(upload.status_code, 200)
        self.assertEqual(attachment["filename"], "notes.txt")
        self.assertEqual(attachment["kind"], "text")
        self.assertEqual(downloaded.status_code, 200)
        self.assertEqual(downloaded.data, b"ticket field notes")
        self.assertEqual(blocked.status_code, 404)

    def test_query_passes_attachment_metadata_and_text_to_service(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {
                "status": "ok",
                "answer_mode": "auto",
                "summary": "answer summary",
                "llm_answer": "direct answer",
                "llm_provider": "codex_cli_bridge",
                "llm_model": "codex-cli",
                "trace_id": "trace-attachment",
                "matches": [],
            }

        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            created = client.post("/api/source-code-qa/sessions", json={"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"})
            session_id = created.get_json()["session"]["id"]
            uploaded = client.post(
                "/api/source-code-qa/attachments",
                data={
                    "session_id": session_id,
                    "file": (io.BytesIO(b"uploaded source question context"), "context.md"),
                },
                content_type="multipart/form-data",
            ).get_json()["attachment"]
            with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today", return_value={"attempted": False, "status": "fresh"}), patch(
                "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
                side_effect=fake_query,
            ):
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "session_id": session_id,
                        "pm_team": "AF",
                        "country": "All",
                        "question": "use attachment",
                        "llm_provider": "codex_cli_bridge",
                        "attachment_ids": [uploaded["id"]],
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(captured["attachments"][0]["filename"], "context.md")
        self.assertIn("uploaded source question context", captured["attachments"][0]["text"])
        self.assertEqual(payload["attachments"][0]["id"], uploaded["id"])
        self.assertEqual(payload["session"]["messages"][-2]["attachments"][0]["filename"], "context.md")

    def test_runtime_evidence_upload_is_scoped_and_passed_to_query(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {
                "status": "ok",
                "answer_mode": "auto",
                "summary": "answer summary",
                "llm_answer": "direct answer",
                "llm_provider": "codex_cli_bridge",
                "llm_model": "codex-cli",
                "trace_id": "trace-runtime",
                "matches": [],
            }

        with self.app.test_client() as client:
            self._login(client, "xiaodong.zheng@npt.sg")
            sg_upload = client.post(
                "/api/source-code-qa/runtime-evidence",
                data={
                    "pm_team": "AF",
                    "country": "SG",
                    "source_type": "apollo",
                    "file": (io.BytesIO(b"apollo.sg.rule.enabled=true"), "apollo.properties"),
                },
                content_type="multipart/form-data",
            )
            ph_upload = client.post(
                "/api/source-code-qa/runtime-evidence",
                data={
                    "pm_team": "AF",
                    "country": "PH",
                    "source_type": "db",
                    "file": (io.BytesIO(b"rule_id,status\nC0204v2,online"), "rules.csv"),
                },
                content_type="multipart/form-data",
            )
            listed = client.get("/api/source-code-qa/runtime-evidence?pm_team=AF&country=SG")
            self._login(client, "teammate@npt.sg")
            with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today", return_value={"attempted": False, "status": "fresh"}), patch(
                "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
                side_effect=fake_query,
            ):
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "pm_team": "AF",
                        "country": "All",
                        "question": "compare runtime config",
                        "llm_provider": "codex_cli_bridge",
                    },
                )

        self.assertEqual(sg_upload.status_code, 200)
        self.assertEqual(ph_upload.status_code, 200)
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.get_json()["evidence"][0]["country"], "SG")
        self.assertEqual(response.status_code, 200)
        countries = {item["country"] for item in captured["runtime_evidence"]}
        self.assertEqual(countries, {"SG", "PH"})
        self.assertTrue(any("apollo.sg.rule.enabled" in item.get("text", "") for item in captured["runtime_evidence"]))
        self.assertEqual(response.get_json()["runtime_evidence"][0]["pm_team"], "AF")

    def test_runtime_evidence_prompt_marks_apollo_as_uat_reference_only(self):
        section = SourceCodeQAService._runtime_evidence_prompt_section(
            [
                {
                    "id": "e1",
                    "filename": "apollo.properties",
                    "source_type": "apollo",
                    "pm_team": "AF",
                    "country": "SG",
                    "kind": "text",
                    "text": "feature.enabled=true",
                }
            ]
        )

        self.assertIn("UAT/non-Live", section)
        self.assertIn("never use them as confirmed Live/production configuration facts", section)
        self.assertNotIn("production DB/Apollo/config snapshots", section)

    def test_runtime_evidence_apollo_zip_extracts_nested_text_configs(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as archive:
            archive.writestr("sg/application.properties", "apollo.sg.rule.enabled=true")
            archive.writestr("sg/nested/rules.yaml", "challenge: C0204v2")
            archive.writestr("sg/ignore.bin", b"\x00\x01binary")
        zip_buffer.seek(0)

        with self.app.test_client() as client:
            self._login(client, "xiaodong.zheng@npt.sg")
            upload = client.post(
                "/api/source-code-qa/runtime-evidence",
                data={
                    "pm_team": "AF",
                    "country": "SG",
                    "source_type": "apollo",
                    "file": (zip_buffer, "apollo-config.zip"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(upload.status_code, 200)
        evidence = upload.get_json()["evidence"]
        self.assertEqual(evidence["filename"], "apollo-config.zip")
        self.assertEqual(evidence["kind"], "archive")
        self.assertIn("sg/application.properties", evidence["summary"])
        self.assertIn("apollo.sg.rule.enabled=true", evidence["summary"])
        self.assertIn("sg/nested/rules.yaml", evidence["summary"])

    def test_regular_source_code_qa_attachment_still_rejects_zip(self):
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as archive:
            archive.writestr("notes.txt", "hello")
        zip_buffer.seek(0)

        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            created = client.post("/api/source-code-qa/sessions", json={"pm_team": "AF", "country": "All", "llm_provider": "codex_cli_bridge"})
            session_id = created.get_json()["session"]["id"]
            upload = client.post(
                "/api/source-code-qa/attachments",
                data={
                    "session_id": session_id,
                    "file": (zip_buffer, "notes.zip"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(upload.status_code, 400)
        self.assertIn("archive", upload.get_json()["message"].lower())

    def test_query_empty_config_is_controlled(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.post(
                "/api/source-code-qa/query",
                json={"pm_team": "CRMS", "country": "SG", "question": "where is approval logic"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "empty_config")
        self.assertEqual(payload["answer_mode"], "auto")

    def test_query_api_defaults_llm_budget_to_auto(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "answer_mode": "retrieval_only", "matches": []}

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ) as ensure_synced:
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={"pm_team": "AF", "country": "All", "question": "where is createIssue", "answer_mode": "auto"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["llm_budget_mode"], "auto")
        ensure_synced.assert_called_once()
        self.assertEqual(response.get_json()["auto_sync"]["status"], "fresh")

    def test_query_api_coerces_legacy_retrieval_only_to_auto(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "answer_mode": kwargs["answer_mode"], "matches": []}

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={"pm_team": "AF", "country": "All", "question": "where is createIssue", "answer_mode": "retrieval_only"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["answer_mode"], "auto")
        self.assertEqual(response.get_json()["answer_mode"], "auto")

    def test_query_api_uses_requested_llm_provider(self):
        selected = []

        def fake_query(**kwargs):
            return {"status": "ok", "answer_mode": "auto", "matches": [], "llm_provider": "codex_cli_bridge"}

        original = SourceCodeQAService.with_llm_provider

        def fake_with_provider(service, provider):
            selected.append(provider)
            return original(service, provider)

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.with_llm_provider", new=fake_with_provider), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ), patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "pm_team": "AF",
                        "country": "All",
                        "question": "where is createIssue",
                        "answer_mode": "auto",
                        "llm_provider": "codex_cli_bridge",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("codex_cli_bridge", selected)

    def test_query_api_accepts_vertex_ai_llm_provider(self):
        selected = []

        def fake_query(**kwargs):
            return {"status": "ok", "answer_mode": "auto", "matches": [], "llm_provider": "vertex_ai"}

        original = SourceCodeQAService.with_llm_provider

        def fake_with_provider(service, provider):
            selected.append(provider)
            return original(service, provider)

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.with_llm_provider", new=fake_with_provider), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ), patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "pm_team": "AF",
                        "country": "All",
                        "question": "where is createIssue",
                        "answer_mode": "auto",
                        "llm_provider": "vertex_ai",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertIn("vertex_ai", selected)

    def test_query_api_invalid_llm_provider_falls_back_to_codex(self):
        selected = []

        def fake_query(**kwargs):
            return {"status": "ok", "answer_mode": "auto", "matches": [], "llm_provider": "codex_cli_bridge"}

        original = SourceCodeQAService.with_llm_provider

        def fake_with_provider(service, provider):
            selected.append(provider)
            return original(service, provider)

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.with_llm_provider", new=fake_with_provider), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ), patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "pm_team": "AF",
                        "country": "All",
                        "question": "where is createIssue",
                        "answer_mode": "auto",
                        "llm_provider": "not-real",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(SourceCodeQAService.normalize_query_llm_provider(selected[-1]), "codex_cli_bridge")

    def test_query_api_ignores_client_llm_budget_selection(self):
        captured = {}

        def fake_query(**kwargs):
            captured.update(kwargs)
            return {"status": "ok", "answer_mode": "auto", "matches": []}

        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.query", side_effect=fake_query), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "pm_team": "AF",
                        "country": "All",
                        "question": "where is createIssue",
                        "answer_mode": "auto",
                        "llm_budget_mode": "deep",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["llm_budget_mode"], "auto")

    def test_query_api_returns_json_for_unexpected_failures(self):
        with patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
            side_effect=RuntimeError("boom"),
        ):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={"pm_team": "CRMS", "country": "SG", "question": "where is income logic"},
                )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.content_type, "application/json")
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["error_category"], "source_code_qa_internal")
        self.assertTrue(payload["error_retryable"])

    def test_query_api_syncs_before_answer_when_not_refreshed_today(self):
        with patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": True, "status": "ok", "reason": "synced before query"},
        ) as ensure_synced, patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
            return_value={"status": "ok", "answer_mode": "retrieval_only", "matches": []},
        ):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={"pm_team": "AF", "country": "All", "question": "where is createIssue"},
                )

        self.assertEqual(response.status_code, 200)
        ensure_synced.assert_called_once_with(pm_team="AF", country="All")
        self.assertTrue(response.get_json()["auto_sync"]["attempted"])

    def test_cloud_run_local_agent_query_queues_auto_sync_in_background(self):
        calls = []

        class FakeLocalAgentClient:
            def source_code_qa_ensure_synced_today(self, *, pm_team, country, background=False):
                calls.append(("ensure", pm_team, country, background))
                return {"status": "background_queued", "attempted": False, "key": f"{pm_team}:{country}"}

            def source_code_qa_query(self, payload, *, progress_callback=None):
                calls.append(("query", payload["pm_team"], payload["country"], payload.get("question")))
                if progress_callback:
                    progress_callback("codex_stream", "Reading repo files.", 0, 0)
                return {"status": "ok", "answer_mode": "auto", "summary": "agent answer", "matches": []}

            def source_code_qa_runtime_evidence_resolve(self, *, pm_team, country):
                return []

        with patch.dict(
            os.environ,
            {
                "FLASK_SECRET_KEY": "test-secret",
                "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
                "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
                "LOCAL_AGENT_MODE": "sync",
                "LOCAL_AGENT_BASE_URL": "https://agent.example",
                "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
                "LOCAL_AGENT_SOURCE_CODE_QA_ENABLED": "true",
                "SOURCE_CODE_QA_QUERY_SYNC_MODE": "background",
            },
            clear=False,
        ):
            app = create_app()
            app.testing = True

        with patch("bpmis_jira_tool.web._source_code_qa_provider_available", return_value=True), patch(
            "bpmis_jira_tool.web._build_local_agent_client",
            return_value=FakeLocalAgentClient(),
        ):
            with app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={"pm_team": "AF", "country": "All", "question": "where is createIssue", "llm_provider": "codex_cli_bridge"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls[0], ("ensure", "AF", "All", True))
        self.assertEqual(calls[1], ("query", "AF", "All", "where is createIssue"))
        self.assertEqual(response.get_json()["auto_sync"]["status"], "background_queued")

    def test_config_api_reports_llm_not_ready_by_default(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.get("/api/source-code-qa/config")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertFalse(payload["llm_ready"])
        self.assertEqual(payload["llm_provider"], "codex_cli_bridge")
        self.assertEqual(payload["llm_policy"]["provider"]["provider"], "codex_cli_bridge")
        self.assertEqual(payload["llm_policy"]["router"]["version"], 7)
        self.assertEqual(payload["llm_policy"]["versions"]["cache"], 14)
        self.assertEqual(payload["llm_policy"]["versions"]["runtime"], 2)
        self.assertEqual(payload["llm_policy"]["runtime"]["max_retries"], 2)
        self.assertEqual(payload["llm_policy"]["model_policy"]["answer"]["model"], os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"))
        self.assertTrue(payload["llm_policy"]["judge"]["enabled"])
        self.assertEqual(payload["llm_policy"]["planner_tools"]["version"], 1)
        self.assertEqual(payload["llm_policy"]["semantic_retrieval"]["model"], "local-token-hybrid-v1")
        self.assertEqual(payload["llm_policy"]["semantic_retrieval"]["embedding_provider"]["provider"], "local_token_hybrid")
        self.assertEqual(payload["index_health"]["status"], "not_configured")
        self.assertEqual(payload["release_gate"]["status"], "missing")
        self.assertEqual(payload["domain_knowledge"]["domains"]["CRMS"]["label"], "Credit Risk")
        self.assertIn("GRC", payload["domain_knowledge"]["domains"])
        self.assertEqual(payload["options"]["answer_modes"][0]["value"], "auto")
        self.assertEqual(payload["options"]["llm_providers"][0]["value"], "codex_cli_bridge")
        gemini_option = next(item for item in payload["options"]["llm_providers"] if item["value"] == "gemini")
        self.assertTrue(gemini_option["disabled"])
        self.assertEqual(gemini_option["label"], "Gemini (Unavailable)")
        self.assertIn("codex_cli_bridge", payload["llm_providers"])
        self.assertIn("vertex_ai", payload["llm_providers"])
        self.assertNotIn("llm_budget_modes", payload["options"])

    def test_sync_api_runs_as_background_job(self):
        sync_result = {
            "status": "ok",
            "key": "AF:All",
            "results": [{"state": "ok", "display_name": "Repo One"}],
            "repo_status": [{"display_name": "Repo One", "state": "synced", "message": "ready"}],
        }
        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.sync", return_value=sync_result):
            with self.app.test_client() as client:
                self._login(client, "xiaodong.zheng@npt.sg")
                response = client.post("/api/source-code-qa/sync", json={"pm_team": "AF", "country": "All"})
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["status"], "queued")
                job_id = payload["job_id"]
                snapshot = {}
                for _ in range(20):
                    job_response = client.get(f"/api/jobs/{job_id}")
                    snapshot = job_response.get_json()
                    if snapshot.get("state") == "completed":
                        break
                    time.sleep(0.05)

        self.assertEqual(snapshot.get("state"), "completed")
        self.assertEqual(snapshot["results"][0]["repo_status"][0]["state"], "synced")

    def test_query_api_async_reports_real_backend_progress(self):
        captured = {}
        selected = []

        def fake_query(**kwargs):
            captured.update(kwargs)
            kwargs["progress_callback"]("direct_search", "Searching direct matches in Repo One.", 1, 2)
            return {"status": "ok", "answer_mode": "retrieval_only", "summary": "done", "matches": []}

        original = SourceCodeQAService.with_llm_provider

        def fake_with_provider(service, provider):
            selected.append(provider)
            return original(service, provider)

        with patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.with_llm_provider",
            new=fake_with_provider,
        ), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": False, "status": "fresh"},
        ) as ensure_synced, patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
            side_effect=fake_query,
        ):
            with self.app.test_client() as client:
                self._login(client, "teammate@npt.sg")
                response = client.post(
                    "/api/source-code-qa/query",
                    json={
                        "pm_team": "AF",
                        "country": "All",
                        "question": "where is createIssue",
                        "answer_mode": "auto",
                        "llm_provider": "codex_cli_bridge",
                        "async": True,
                    },
                )
                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["status"], "queued")
                snapshot = {}
                for _ in range(20):
                    job_response = client.get(f"/api/jobs/{payload['job_id']}")
                    snapshot = job_response.get_json()
                    if snapshot.get("state") == "completed":
                        break
                    time.sleep(0.05)

        self.assertEqual(snapshot.get("state"), "completed")
        self.assertEqual(snapshot["results"][0]["summary"], "done")
        self.assertIn("progress_callback", captured)
        self.assertIn("codex_cli_bridge", selected)
        ensure_synced.assert_called_once_with(pm_team="AF", country="All")

    def test_job_store_persists_background_job_snapshots(self):
        path = Path(self.temp_dir.name) / "run" / "jobs.json"
        first_store = JobStore(path)
        job = first_store.create("source-code-qa-sync", "Sync Source Code Repositories")
        first_store.update(job.job_id, state="running", message="Indexing.")
        first_store.complete(job.job_id, results=[{"status": "ok"}], notice={"summary": "done"})

        second_store = JobStore(path)
        snapshot = second_store.snapshot(job.job_id)

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["state"], "completed")
        self.assertEqual(snapshot["results"][0]["status"], "ok")

    def test_job_store_refreshes_snapshots_written_by_another_worker(self):
        path = Path(self.temp_dir.name) / "run" / "jobs.json"
        first_store = JobStore(path)
        second_store = JobStore(path)
        job = first_store.create("source-code-qa-sync", "Sync Source Code Repositories")
        first_store.update(job.job_id, state="running", message="Indexing.")
        first_store.complete(job.job_id, results=[{"status": "ok"}], notice={"summary": "done"})

        snapshot = second_store.snapshot(job.job_id)

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["state"], "completed")
        self.assertEqual(snapshot["results"][0]["status"], "ok")

    def test_job_store_marks_loaded_running_jobs_interrupted(self):
        path = Path(self.temp_dir.name) / "run" / "jobs.json"
        first_store = JobStore(path)
        job = first_store.create("source-code-qa-query", "Answer Source Code Question")
        first_store.update(job.job_id, state="running", stage="codex_stream", message="Calling Codex.")

        second_store = JobStore(path)
        snapshot = second_store.snapshot(job.job_id)

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["state"], "failed")
        self.assertEqual(snapshot["stage"], "failed")
        self.assertIn("interrupted by a server restart", snapshot["error"])

    def test_feedback_api_saves_user_signal(self):
        with self.app.test_client() as client:
            self._login(client, "teammate@npt.sg")
            response = client.post(
                "/api/source-code-qa/feedback",
                json={
                    "rating": "too_vague",
                    "pm_team": "AF",
                    "country": "All",
                    "question": "where is createIssue",
                    "top_paths": ["controller/IssueController.java"],
                    "trace_id": "trace-123",
                    "replay_context": {
                        "trace_id": "trace-123",
                        "answer_mode": "gemini_flash",
                        "llm_model": "gemini-2.5-flash-lite",
                        "rendered_answer": "createIssue is handled by IssueController.",
                        "answer_contract": {"status": "satisfied"},
                        "evidence_pack": {"items": [{"type": "entry_point", "claim": "IssueController"}]},
                        "matches_snapshot": [
                            {
                                "repo": "Portal Repo",
                                "path": "controller/IssueController.java",
                                "line_start": 10,
                                "line_end": 20,
                                "snippet": "public Issue createIssue() {}",
                            }
                        ],
                    },
                },
            )

        self.assertEqual(response.status_code, 200)
        feedback_path = Path(self.temp_dir.name) / "source_code_qa" / "feedback.jsonl"
        self.assertTrue(feedback_path.exists())
        feedback = json.loads(feedback_path.read_text(encoding="utf-8").strip())
        self.assertEqual(feedback["rating"], "too_vague")
        self.assertEqual(feedback["trace_id"], "trace-123")
        self.assertEqual(feedback["replay_context"]["answer_contract"]["status"], "satisfied")
        self.assertEqual(feedback["replay_context"]["matches_snapshot"][0]["path"], "controller/IssueController.java")

    def test_feedback_records_can_be_promoted_to_eval_candidates(self):
        candidates = build_eval_candidates(
            [
                {
                    "rating": "needs_deeper_trace",
                    "pm_team": "AF",
                    "country": "All",
                    "question_preview": "where does createIssue load data from",
                    "question_sha1": "abc123456789",
                    "trace_id": "trace-abc",
                    "top_paths": ["controller/IssueController.java", "repository/IssueRepository.java"],
                    "comment": "missed repository",
                    "replay_context": {
                        "trace_id": "trace-abc",
                        "answer_mode": "gemini_flash",
                        "llm_model": "gemini-2.5-flash-lite",
                        "answer_contract": {"status": "satisfied"},
                        "evidence_pack": {"items": [{"type": "entry_point", "claim": "IssueController"}]},
                        "matches_snapshot": [
                            {"path": "controller/IssueController.java"},
                            {"path": "repository/IssueRepository.java"},
                        ],
                    },
                },
                {
                    "rating": "useful",
                    "pm_team": "AF",
                    "country": "All",
                    "question_preview": "where is createIssue",
                },
            ]
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["id"], "feedback-needs_deeper_trace-abc1234567")
        self.assertEqual(candidates[0]["expected_paths"], [])
        self.assertEqual(candidates[0]["observed_paths"], ["controller/IssueController.java", "repository/IssueRepository.java"])
        self.assertEqual(candidates[0]["draft_status"], "needs_human_expected_evidence")
        self.assertEqual(candidates[0]["review_context"]["trace_id"], "trace-abc")

    def test_useful_feedback_can_become_positive_smoke_eval(self):
        candidates = build_eval_candidates(
            [
                {
                    "rating": "useful",
                    "pm_team": "AF",
                    "country": "All",
                    "question_preview": "where is createIssue",
                    "question_sha1": "def123456789",
                    "replay_context": {
                        "matches_snapshot": [{"path": "controller/IssueController.java"}],
                    },
                }
            ],
            include_useful=True,
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["expected_paths"], ["controller/IssueController.java"])
        self.assertEqual(candidates[0]["draft_status"], "ready_positive_smoke")

    def test_reviewed_feedback_candidates_promote_to_real_eval_cases(self):
        merged, summary = promote_candidates(
            [
                {
                    "id": "feedback-needs_deeper_trace-abc1234567",
                    "pm_team": "AF",
                    "country": "All",
                    "question": "where does createIssue load data from",
                    "answer_mode": "retrieval_only",
                    "draft_status": "approved",
                    "expected_paths": ["repository/IssueRepository.java"],
                    "observed_paths": ["controller/IssueController.java"],
                    "review_context": {"trace_id": "trace-abc"},
                },
                {
                    "id": "feedback-too_vague-def1234567",
                    "pm_team": "AF",
                    "country": "All",
                    "question": "where is createIssue",
                    "draft_status": "needs_human_expected_evidence",
                    "expected_paths": [],
                },
            ],
            [],
        )

        self.assertEqual(summary["promoted"], 1)
        self.assertEqual(summary["rejected"], 1)
        self.assertEqual(merged[0]["expected_paths"], ["repository/IssueRepository.java"])
        self.assertNotIn("review_context", merged[0])
        self.assertNotIn("observed_paths", merged[0])

    def test_positive_smoke_candidates_can_be_promoted_when_allowed(self):
        merged, summary = promote_candidates(
            [
                {
                    "id": "feedback-useful-def1234567",
                    "pm_team": "AF",
                    "country": "All",
                    "question": "where is createIssue",
                    "draft_status": "ready_positive_smoke",
                    "expected_paths": ["controller/IssueController.java"],
                }
            ],
            [],
            allow_positive_smoke=True,
        )

        self.assertEqual(summary["promoted"], 1)
        self.assertEqual(merged[0]["id"], "feedback-useful-def1234567")


class SourceCodeQAServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _build_index_for_entry(self, key, entry):
        repo_entry = type("Entry", (), entry)()
        repo_path = self.service._repo_path(key, repo_entry)
        self.service._build_repo_index(key, repo_entry, repo_path)
        return repo_path

    def _build_all_indexes(self, service=None):
        target_service = service or self.service
        for key, entries in (target_service.load_config().get("mappings") or {}).items():
            for entry in entries:
                repo_entry = type("Entry", (), entry)()
                repo_path = target_service._repo_path(key, repo_entry)
                if (repo_path / ".git").exists():
                    target_service._build_repo_index(key, repo_entry, repo_path)

    def test_attachment_prompt_and_gemini_parts_keep_attachment_evidence_separate(self):
        image_path = Path(self.temp_dir.name) / "screen.png"
        image_path.write_bytes(b"fake-image")
        attachment = {
            "id": "att-1",
            "filename": "screen.png",
            "mime_type": "image/png",
            "kind": "image",
            "size": 10,
            "sha256": "a" * 64,
            "path": str(image_path),
        }

        section = self.service._attachment_prompt_section([attachment])
        parts = self.service._llm_payload_parts("Question prompt", [attachment])

        self.assertIn("User attachments", section)
        self.assertIn("not repository facts", section)
        self.assertEqual(parts[0], {"text": "Question prompt"})
        self.assertEqual(parts[1]["inlineData"]["mimeType"], "image/png")
        self.assertTrue(parts[1]["inlineData"]["data"])

    def test_sync_subprocess_timeout_is_controlled(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Repo One", "url": "https://git.example.com/team/repo.git"}],
        )
        import subprocess

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(["git"], timeout=5)):
            payload = self.service.sync(pm_team="AF", country="All")

        self.assertEqual(payload["status"], "partial")
        self.assertIn("timed out", payload["results"][0]["message"])
        self.assertEqual(payload["job"]["status"], "partial")

    def test_ensure_synced_today_skips_before_scheduled_start_date(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Repo One", "url": "https://git.example.com/team/repo.git"}],
        )

        with patch.object(self.service, "_today", return_value=date(2026, 4, 25)), patch.object(
            self.service,
            "sync",
            return_value={"status": "ok", "results": []},
        ) as sync:
            payload = self.service.ensure_synced_today(pm_team="AF", country="All")

        self.assertFalse(payload["attempted"])
        self.assertEqual(payload["status"], "scheduled")
        self.assertEqual(payload["next_sync_date"], "2026-05-08")
        sync.assert_not_called()

    def test_ensure_synced_today_runs_sync_on_scheduled_start_date(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Repo One", "url": "https://git.example.com/team/repo.git"}],
        )

        with patch.object(self.service, "_today", return_value=date(2026, 5, 8)), patch.object(
            self.service,
            "sync",
            return_value={"status": "ok", "results": []},
        ) as sync:
            payload = self.service.ensure_synced_today(pm_team="AF", country="All")

        self.assertTrue(payload["attempted"])
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["next_sync_date"], "2026-05-22")
        sync.assert_called_once_with(pm_team="AF", country="All")

    def test_ensure_synced_today_skips_fresh_current_schedule_window(self):
        entry = RepositoryEntry(display_name="Repo One", url="https://git.example.com/team/repo.git")
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": entry.display_name, "url": entry.url}],
        )
        repo_path = self.service._repo_path("AF:All", entry)
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "IssueService.java").write_text("class IssueService {}\n", encoding="utf-8")
        self.service._build_repo_index("AF:All", entry, repo_path)
        scheduled_time = date(2026, 5, 8)
        index_path = self.service._index_path(repo_path)
        with sqlite3.connect(index_path) as connection:
            connection.execute(
                "update metadata set value = ? where key = 'updated_at'",
                [f"{scheduled_time.isoformat()}T01:00:00+00:00"],
            )

        with patch.object(self.service, "_today", return_value=scheduled_time + timedelta(days=3)), patch.object(
            self.service,
            "sync",
            return_value={"status": "ok"},
        ) as sync:
            payload = self.service.ensure_synced_today(pm_team="AF", country="All")

        self.assertFalse(payload["attempted"])
        self.assertEqual(payload["status"], "fresh")
        self.assertEqual(payload["next_sync_date"], "2026-05-22")
        sync.assert_not_called()

    def test_search_repo_request_cache_reuses_identical_search(self):
        entry = RepositoryEntry(display_name="Portal Repo", url="https://git.example.com/team/portal.git")
        repo_path = Path(self.temp_dir.name) / "portal"
        (repo_path / ".git").mkdir(parents=True)
        request_cache = self.service._new_retrieval_request_cache()
        match = {
            "repo": "Portal Repo",
            "path": "src/App.java",
            "line_start": 1,
            "line_end": 2,
            "score": 99,
            "snippet": "class App {}",
            "reason": "test match",
            "trace_stage": "direct",
            "retrieval": "persistent_index",
        }

        with patch.object(self.service, "_require_ready_repo_index", return_value={"state": "ready"}) as mocked_ensure, patch.object(
            self.service,
            "_search_repo_index",
            return_value=[dict(match)],
        ) as mocked_index:
            first = self.service._search_repo(
                entry,
                repo_path,
                ["app"],
                question="where is app",
                request_cache=request_cache,
            )
            first[0]["reason"] = "mutated by caller"
            second = self.service._search_repo(
                entry,
                repo_path,
                ["app"],
                question="where is app",
                request_cache=request_cache,
            )

        self.assertEqual(mocked_ensure.call_count, 1)
        self.assertEqual(mocked_index.call_count, 1)
        self.assertEqual(second[0]["reason"], "test match")
        self.assertEqual(request_cache["stats"]["search_hits"], 1)
        self.assertEqual(request_cache["stats"]["search_misses"], 1)
        self.assertEqual(request_cache["stats"]["index_ensure_hits"], 1)
        self.assertEqual(request_cache["stats"]["index_ensure_misses"], 1)

    def test_rank_matches_reuses_rerank_scores_within_request_cache(self):
        request_cache = self.service._new_retrieval_request_cache()
        matches = [
            {
                "repo": "Portal Repo",
                "path": "src/main/java/com/example/IssueService.java",
                "line_start": 10,
                "line_end": 12,
                "score": 100,
                "snippet": "class IssueService { void createIssue() {} }",
                "reason": "test match",
                "trace_stage": "direct",
                "retrieval": "persistent_index",
            }
        ]

        with patch.object(self.service, "_rerank_score", wraps=self.service._rerank_score) as mocked_rerank:
            first = self.service._rank_matches("where is IssueService createIssue", matches, request_cache=request_cache)
            first[0]["rerank_score"] = -1
            second = self.service._rank_matches("where is IssueService createIssue", matches, request_cache=request_cache)

        self.assertEqual(mocked_rerank.call_count, 1)
        self.assertGreater(second[0]["rerank_score"], 0)
        self.assertNotEqual(second[0]["rerank_score"], -1)
        self.assertEqual(request_cache["stats"]["rerank_hits"], 1)
        self.assertEqual(request_cache["stats"]["rerank_misses"], 1)
        self.assertEqual(request_cache["stats"]["question_feature_hits"], 1)
        self.assertEqual(request_cache["stats"]["question_feature_misses"], 1)

    def test_question_tokens_cache_returns_caller_safe_lists(self):
        first = self.service._question_tokens("where is IssueService createIssue")
        first.append("mutated")
        second = self.service._question_tokens("where is IssueService createIssue")

        self.assertIn("issueservice", second)
        self.assertNotIn("mutated", second)

    def test_index_rows_are_reused_within_request_cache(self):
        index_path = Path(self.temp_dir.name) / "index.sqlite3"
        with sqlite3.connect(index_path) as connection:
            connection.execute("create table files (path text, lower_path text, symbols text)")
            connection.execute(
                "create table lines (file_path text, line_no integer, line_text text, lower_text text, symbols text, is_declaration integer, has_pathish integer)"
            )
            connection.execute(
                "create table semantic_chunks (file_path text, start_line integer, end_line integer, chunk_text text, lower_text text, tokens text, symbols text, embedding text)"
            )
            connection.execute("insert into files values ('src/App.java', 'src/app.java', '[]')")
            connection.execute("insert into lines values ('src/App.java', 1, 'class App {}', 'class app {}', '[]', 1, 0)")
            connection.execute("insert into semantic_chunks values ('src/App.java', 1, 1, 'class App {}', 'class app {}', '[]', '[]', '[]')")
            connection.commit()
        request_cache = self.service._new_retrieval_request_cache()
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            first = self.service._cached_index_rows(connection, index_path, request_cache=request_cache)
            second = self.service._cached_index_rows(connection, index_path, request_cache=request_cache)

        self.assertIs(first, second)
        self.assertEqual(first["lines_by_path"]["src/App.java"], ["class App {}"])
        self.assertEqual(request_cache["stats"]["index_rows_hits"], 1)
        self.assertEqual(request_cache["stats"]["index_rows_misses"], 1)

    def test_targeted_index_rows_keep_late_matches_without_loading_broad_snapshot(self):
        index_path = Path(self.temp_dir.name) / "targeted.sqlite3"
        with sqlite3.connect(index_path) as connection:
            connection.execute("create table files (path text, lower_path text, symbols text)")
            connection.execute(
                "create table lines (file_path text, line_no integer, line_text text, lower_text text, symbols text, is_declaration integer, has_pathish integer)"
            )
            connection.execute("create table file_tokens (token text, file_path text)")
            connection.execute("create table line_tokens (token text, file_path text, line_no integer)")
            connection.execute(
                "create table semantic_chunks (chunk_id text, file_path text, start_line integer, end_line integer, chunk_text text, lower_text text, tokens text, symbols text, embedding text)"
            )
            connection.execute("create table semantic_chunk_tokens (token text, chunk_id text, file_path text)")
            for index in range(300):
                path = f"src/noise/Noise{index}.java"
                connection.execute("insert into files values (?, ?, ?)", (path, path.lower(), "[]"))
                connection.execute(
                    "insert into lines values (?, ?, ?, ?, ?, ?, ?)",
                    (path, 1, "class Noise {}", "class noise {}", "[]", 1, 0),
                )
            connection.execute("insert into files values ('src/service/Late.java', 'src/service/late.java', '[]')")
            connection.execute("insert into file_tokens values ('needlevalue', 'src/service/Late.java')")
            connection.execute(
                "insert into lines values ('src/service/Late.java', 9001, 'return needleValue;', 'return needlevalue;', '[\"needlevalue\"]', 0, 0)"
            )
            connection.execute("insert into line_tokens values ('needlevalue', 'src/service/Late.java', 9001)")
            connection.execute(
                "insert into semantic_chunks values ('late-1', 'src/service/Late.java', 9001, 9001, 'return needleValue;', 'return needlevalue;', '[\"needlevalue\"]', '[\"needlevalue\"]', '[]')"
            )
            connection.execute("insert into semantic_chunk_tokens values ('needlevalue', 'late-1', 'src/service/Late.java')")
            connection.commit()
        request_cache = self.service._new_retrieval_request_cache()
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = self.service._targeted_index_rows(
                connection,
                index_path,
                tokens=["needlevalue"],
                focus_terms=[],
                intent={},
                request_cache=request_cache,
            )

        self.assertIn("src/service/Late.java", rows["files_by_path"])
        self.assertTrue(any(row["file_path"] == "src/service/Late.java" for row in rows["lines"]))
        self.assertLessEqual(len(rows["files"]), 220)
        self.assertLessEqual(len(rows["lines"]), 1200)
        self.assertEqual(request_cache["stats"]["targeted_index_rows_misses"], 1)

    def test_evidence_outline_summarizes_support_and_primary_sources(self):
        outline = self.service._build_evidence_outline(
            {
                "items": [
                    {"type": "table", "source_id": "S1", "support_level": "confirmed", "claim": "issue_table"},
                    {"type": "call_chain", "source_id": "S2", "support_level": "inferred", "claim": "IssueService"},
                ],
                "confirmed_facts": ["issue_table"],
                "inferred_facts": ["IssueService"],
                "evidence_limits": ["No runtime trace."],
            },
            [
                {"repo": "Portal Repo", "path": "repository/IssueRepository.java", "line_start": 1, "line_end": 3, "retrieval": "persistent_index"},
                {"repo": "Portal Repo", "path": "service/IssueService.java", "line_start": 5, "line_end": 8, "retrieval": "code_graph"},
            ],
        )

        self.assertEqual(outline["type_counts"]["table"], 1)
        self.assertEqual(outline["support_counts"]["confirmed"], 1)
        self.assertEqual(outline["primary_sources"][0]["evidence_items"], 1)
        self.assertEqual(outline["evidence_limits"], ["No runtime trace."])

    def test_match_from_index_location_uses_cached_lines_without_changing_snippet(self):
        entry = RepositoryEntry(display_name="Portal Repo", url="https://git.example.com/team/portal.git")
        index_path = Path(self.temp_dir.name) / "index.sqlite3"
        with sqlite3.connect(index_path) as connection:
            connection.execute("create table files (path text, lower_path text, symbols text)")
            connection.execute(
                "create table lines (file_path text, line_no integer, line_text text, lower_text text, symbols text, is_declaration integer, has_pathish integer)"
            )
            connection.execute(
                "create table semantic_chunks (file_path text, start_line integer, end_line integer, chunk_text text, lower_text text, tokens text, symbols text, embedding text)"
            )
            connection.execute("insert into files values ('src/App.java', 'src/app.java', '[]')")
            connection.execute("insert into lines values ('src/App.java', 1, 'class App {', 'class app {', '[]', 1, 0)")
            connection.execute("insert into lines values ('src/App.java', 2, '  void run() {}', '  void run() {}', '[]', 1, 0)")
            connection.execute("insert into lines values ('src/App.java', 3, '}', '}', '[]', 0, 0)")
            connection.execute("insert into semantic_chunks values ('src/App.java', 1, 3, 'class App {}', 'class app {}', '[]', '[]', '[]')")
            connection.commit()
        request_cache = self.service._new_retrieval_request_cache()
        with sqlite3.connect(index_path) as connection:
            connection.row_factory = sqlite3.Row
            uncached = self.service._match_from_index_location(
                entry,
                connection,
                "src/App.java",
                2,
                score=100,
                reason="uncached",
                question="where is run",
                trace_stage="test",
                retrieval="test",
            )
            cached = self.service._match_from_index_location(
                entry,
                connection,
                "src/App.java",
                2,
                score=100,
                reason="cached",
                question="where is run",
                trace_stage="test",
                retrieval="test",
                index_path=index_path,
                request_cache=request_cache,
            )

        self.assertIsNotNone(uncached)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["snippet"], uncached["snippet"])
        self.assertEqual(request_cache["stats"]["file_lines_misses"], 1)

    def test_trace_paths_are_reused_within_request_cache(self):
        entry = RepositoryEntry(display_name="Portal Repo", url="https://git.example.com/team/portal.git")
        key = "AF::All"
        repo_path = self.service._repo_path(key, entry)
        (repo_path / ".git").mkdir(parents=True)
        index_path = self.service._index_path(repo_path)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(index_path) as connection:
            connection.execute(
                """
                create table flow_edges (
                    edge_kind text,
                    from_file text,
                    from_line integer,
                    from_name text,
                    to_file text,
                    to_line integer,
                    to_name text,
                    evidence text
                )
                """
            )
            connection.execute(
                "insert into flow_edges values ('service', 'src/Controller.java', 10, 'Controller.handle', 'src/Service.java', 20, 'Service.run', 'controller calls service')"
            )
            connection.commit()
        matches = [
            {
                "repo": "Portal Repo",
                "path": "src/Controller.java",
                "line_start": 10,
                "line_end": 12,
                "score": 100,
                "snippet": "class Controller {}",
                "reason": "seed",
                "trace_stage": "direct",
                "retrieval": "persistent_index",
            }
        ]
        request_cache = self.service._new_retrieval_request_cache()
        with patch.object(self.service, "_require_ready_repo_index", return_value={"state": "ready"}) as mocked_ensure:
            first = self.service._build_trace_paths(
                entries=[entry],
                key=key,
                matches=matches,
                question="trace controller",
                request_cache=request_cache,
            )
            first[0]["confidence"] = -1
            second = self.service._build_trace_paths(
                entries=[entry],
                key=key,
                matches=matches,
                question="trace controller",
                request_cache=request_cache,
            )

        self.assertEqual(mocked_ensure.call_count, 1)
        self.assertGreater(second[0]["confidence"], 0)
        self.assertNotEqual(second[0]["confidence"], -1)
        self.assertEqual(request_cache["stats"]["trace_paths_hits"], 1)
        self.assertEqual(request_cache["stats"]["trace_paths_misses"], 1)

    def test_index_lock_reports_concurrent_indexing(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class BPMISClient:\n    pass\n", encoding="utf-8")
        lock_path = self.service._index_lock_path(repo_path)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text("busy", encoding="utf-8")

        with self.assertRaisesRegex(Exception, "already being indexed"):
            self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        lock_path.unlink(missing_ok=True)

    def test_index_lock_recovers_stale_lock(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class BPMISClient:\n    pass\n", encoding="utf-8")
        lock_path = self.service._index_lock_path(repo_path)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text("2000-01-01T00:00:00+00:00", encoding="utf-8")

        info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(info["state"], "ready")
        self.assertFalse(lock_path.exists())

    def test_retrieval_query_returns_path_line_and_snippet(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="batchCreateJiraIssue BPMIS API")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["answer_mode"], "retrieval_only")
        self.assertEqual(payload["matches"][0]["path"], "bpmis/jira_client.py")
        self.assertIn("batchCreateJiraIssue", payload["matches"][0]["snippet"])

    def test_chinese_business_intent_detection(self):
        intent = self.service._question_intent("IssueService 改了会影响哪些下游，测试有没有覆盖，事务缓存边界是什么")

        self.assertTrue(intent["impact_analysis"])
        self.assertTrue(intent["test_coverage"])
        self.assertTrue(intent["operational_boundary"])
        self.assertTrue(any("影响" in token for token in self.service._question_tokens("IssueService 改了会影响哪些下游")))

    def test_domain_labels_do_not_trigger_unrelated_quality_intents(self):
        crms_intent = self.service._question_intent("which SG Credit Risk API runs credit application precheck")
        self.assertTrue(crms_intent["api"])
        self.assertFalse(crms_intent["static_qa"])

        grc_config_intent = self.service._question_intent("GRC globallock config is where and which table backs it")
        self.assertTrue(grc_config_intent["config"])
        self.assertTrue(grc_config_intent["data_source"])
        self.assertFalse(grc_config_intent["operational_boundary"])

        grc_table_intent = self.service._question_intent("where does GRC read and write bcf_global_lock table")
        self.assertTrue(grc_table_intent["data_source"])
        self.assertFalse(grc_table_intent["operational_boundary"])

    def test_chinese_data_source_query_uses_source_trace(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="CRMS",
            country="ID",
            question="Term Loan Pre Check 1 的数据源是哪张表",
        )

        self.assertEqual(payload["status"], "ok")
        self.assertIn("repository/CustomerRepository.java", {match["path"] for match in payload["matches"]})
        self.assertEqual(payload["answer_quality"]["policies"][0]["name"], "data_source")
        self.assertEqual(payload["answer_quality"]["policies"][0]["status"], "satisfied")
        self.assertIn("cr_customer_profile", json.dumps(payload["evidence_pack"], ensure_ascii=False))

    def test_followup_context_uses_evidence_boundaries(self):
        augmented, followup = self.service._apply_conversation_context(
            "继续看下游影响",
            {
                "key": "AF:All",
                "trace_id": "trace-prev",
                "question": "what is impacted if IssueService createIssue changes",
                "answer": "Previous answer mentions IssueRepository.",
                "codex_candidate_paths": [
                    {
                        "repo": "Portal Repo",
                        "repo_root": "/tmp/portal",
                        "path": "repository/IssueRepository.java",
                        "line_start": 3,
                        "line_end": 5,
                    }
                ],
                "matches": [],
                "trace_paths": [],
                "structured_answer": {},
                "answer_contract": {
                    "confirmed_sources": ["Portal Repo:repository/IssueRepository.java:3-5: issue_table [S1]"],
                    "missing_links": ["consumer caller evidence"],
                },
                "evidence_pack": {
                    "confirmed_facts": ["IssueRepository writes issue_table"],
                    "impact_surfaces": ["downstream callee: IssueRepository.findIssue"],
                },
            },
            current_key="AF:All",
        )

        self.assertTrue(followup["used"])
        self.assertIn("issuerepository", augmented)
        self.assertIn("issue_table", augmented)
        self.assertEqual(followup["trace_id"], "trace-prev")
        self.assertIn("Previous answer", followup["answer"])
        self.assertEqual(followup["codex_candidate_paths"][0]["path"], "repository/IssueRepository.java")

    def test_codex_followup_terms_do_not_augment_non_codex_provider(self):
        augmented, followup = self.service._apply_conversation_context(
            "continue with this",
            {
                "key": "AF:All",
                "question": "previous Codex question",
                "matches": [],
                "trace_paths": [],
                "structured_answer": {},
                "answer_contract": {},
                "evidence_pack": {},
                "codex_candidate_paths": [
                    {
                        "repo": "Portal Repo",
                        "repo_root": "/tmp/portal",
                        "path": "repository/CodexOnlyRepository.java",
                        "reason": "previous Codex-only path",
                    }
                ],
                "codex_citation_validation": {
                    "direct_file_refs": [{"path": "repository/CodexOnlyRepository.java"}],
                },
            },
            current_key="AF:All",
        )

        self.assertTrue(followup["used"])
        self.assertEqual(followup["terms"], [])
        self.assertEqual(augmented, "continue with this")

    def test_codex_followup_terms_are_used_for_codex_provider(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="codex_cli_bridge",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        augmented, followup = service._apply_conversation_context(
            "continue with this",
            {
                "key": "AF:All",
                "question": "previous Codex question",
                "matches": [],
                "trace_paths": [],
                "structured_answer": {},
                "answer_contract": {},
                "evidence_pack": {},
                "codex_candidate_paths": [
                    {
                        "repo": "Portal Repo",
                        "repo_root": "/tmp/portal",
                        "path": "repository/CodexOnlyRepository.java",
                        "reason": "previous Codex-only path",
                    }
                ],
            },
            current_key="AF:All",
        )

        self.assertTrue(followup["used"])
        self.assertIn("codexonlyrepository", augmented)

    def test_followup_context_does_not_cross_repository_scope(self):
        augmented, followup = self.service._apply_conversation_context(
            "继续看这个表哪里写入",
            {
                "key": "AF:All",
                "pm_team": "AF",
                "country": "All",
                "question": "what table does issue creation use",
                "matches": [{"repo": "Anti Fraud", "path": "repository/IssueRepository.java", "snippet": "select * from issue_table"}],
                "trace_paths": [],
                "structured_answer": {},
                "answer_contract": {"confirmed_sources": ["Anti Fraud:repository/IssueRepository.java:1-3: issue_table [S1]"]},
                "evidence_pack": {"confirmed_facts": ["IssueRepository reads issue_table"]},
            },
            current_key="CRMS:SG",
        )

        self.assertFalse(followup["used"])
        self.assertEqual(followup["reason"], "scope_mismatch")
        self.assertEqual(augmented, "继续看这个表哪里写入")

    def test_followup_context_rejects_legacy_scope_fields_when_changed(self):
        augmented, followup = self.service._apply_conversation_context(
            "continue checking this method",
            {
                "pm_team": "AF",
                "country": "All",
                "question": "where is createIssue",
                "matches": [{"repo": "Anti Fraud", "path": "service/IssueService.java", "snippet": "createIssue()"}],
                "trace_paths": [],
                "structured_answer": {},
            },
            current_key="CRMS:ID",
        )

        self.assertFalse(followup["used"])
        self.assertEqual(followup["reason"], "scope_mismatch")
        self.assertEqual(augmented, "continue checking this method")

    def test_followup_context_does_not_match_substrings_inside_normal_questions(self):
        augmented, followup = self.service._apply_conversation_context(
            "When will system query CBS report when performing monthly credit review?",
            {
                "question": "Which table stores payslip extracted fields?",
                "matches": [{"path": "CardIncomeScreeningFlowStatusDAO-ext.xml", "snippet": "process_info"}],
                "trace_paths": [],
                "structured_answer": {"claims": [{"text": "Payslip fields are stored in process_info."}]},
                "answer_contract": {"confirmed_sources": ["card_income_screening_flow_status_tab"]},
                "evidence_pack": {"confirmed_facts": ["extract_record_tab response_body"]},
            },
        )

        self.assertFalse(followup["used"])
        self.assertEqual(augmented, "When will system query CBS report when performing monthly credit review?")

    def test_followup_context_same_scope_session_is_carried_without_query_pollution(self):
        augmented, followup = self.service._apply_conversation_context(
            "Which table stores payslip extracted fields?",
            {
                "key": "CRMS:PH",
                "question": "When is payslip parsed?",
                "answer": "Previous answer mentioned ExtractRecord.",
                "matches": [{"path": "repository/ExtractRecordDAO.xml", "snippet": "response_body"}],
                "trace_paths": [],
                "structured_answer": {"claims": [{"text": "ExtractRecord stores response_body."}]},
                "answer_contract": {"confirmed_sources": ["extract_record_tab.response_body"]},
                "evidence_pack": {"confirmed_facts": ["raw response is stored in extract_record_tab"]},
            },
            current_key="CRMS:PH",
        )

        self.assertTrue(followup["used"])
        self.assertTrue(followup["implicit"])
        self.assertEqual(followup["reason"], "same_scope_session")
        self.assertEqual(augmented, "Which table stores payslip extracted fields?")
        self.assertIn("Previous answer", followup["answer"])

    def test_followup_context_chinese_clarification_uses_previous_terms(self):
        augmented, followup = self.service._apply_conversation_context(
            "我问的是是否要approve，要的话哪些role需要approve？不需要名字",
            {
                "key": "GRC:All",
                "question": "When can an incident be withdrawn?",
                "answer": "Previous answer mentioned AuthorizationContent.",
                "matches": [
                    {
                        "path": "src/pages/AuthorizationManagement/Components/DetailPage/AuthorizationContent.tsx",
                        "snippet": "IncidentApprove IncidentReview pendingWithdraw Approver Reviewer",
                    }
                ],
                "trace_paths": [],
                "structured_answer": {"claims": [{"text": "pendingWithdraw is mapped to IncidentApprove."}]},
                "answer_contract": {"confirmed_sources": ["AuthorizationContent.tsx:241-262"]},
                "evidence_pack": {"confirmed_facts": ["Approver handles IncidentApprove"]},
            },
            current_key="GRC:All",
        )

        self.assertTrue(followup["used"])
        self.assertFalse(followup["implicit"])
        self.assertEqual(followup["reason"], "followup_marker")
        self.assertIn("Previous Source Code Q&A context terms", augmented)
        self.assertIn("authorizationcontent", augmented)
        self.assertIn("incidentapprove", augmented)

    def test_followup_context_does_not_pollute_specific_short_lookup(self):
        augmented, followup = self.service._apply_conversation_context(
            "Is fdMaturityDate used in any function?",
            {
                "question": "Where is F44 configured?",
                "matches": [{"path": "apollo.properties", "snippet": "dbp.antifraud.function.F44"}],
                "trace_paths": [],
                "structured_answer": {},
                "answer_contract": {"confirmed_sources": ["dbp.antifraud.function.F44"]},
                "evidence_pack": {},
            },
        )

        self.assertFalse(followup["used"])
        self.assertEqual(augmented, "Is fdMaturityDate used in any function?")

    def test_followup_context_does_not_cross_explicit_repo_scope(self):
        repositories = [
            RepositoryEntry(display_name="Anti Fraud API", url="https://git.example.com/team/anti-fraud-api.git"),
            RepositoryEntry(display_name="GRC Portal", url="https://git.example.com/team/grc-portal.git"),
        ]

        augmented, followup = self.service._apply_conversation_context(
            "继续看 GRC Portal 这个方法",
            {
                "key": "AF:All",
                "repo_scope": ["Anti Fraud API"],
                "question": "where is createIssue",
                "matches": [{"repo": "Anti Fraud API", "path": "service/IssueService.java", "snippet": "createIssue()"}],
                "trace_paths": [],
                "structured_answer": {},
            },
            current_key="AF:All",
            current_repositories=repositories,
        )

        self.assertFalse(followup["used"])
        self.assertEqual(followup["reason"], "repo_scope_mismatch")
        self.assertEqual(augmented, "继续看 GRC Portal 这个方法")

    def test_followup_context_keeps_same_explicit_repo_scope(self):
        repositories = [
            RepositoryEntry(display_name="Anti Fraud API", url="https://git.example.com/team/anti-fraud-api.git"),
            RepositoryEntry(display_name="GRC Portal", url="https://git.example.com/team/grc-portal.git"),
        ]

        augmented, followup = self.service._apply_conversation_context(
            "继续看 Anti Fraud 这个方法下游",
            {
                "key": "AF:All",
                "repo_scope": ["Anti Fraud API"],
                "question": "where is createIssue",
                "matches": [{"repo": "Anti Fraud API", "path": "service/IssueService.java", "snippet": "createIssue()"}],
                "trace_paths": [],
                "structured_answer": {"claims": [{"text": "IssueService handles createIssue."}]},
                "answer_contract": {"confirmed_sources": ["Anti Fraud API:service/IssueService.java:1-3 [S1]"]},
            },
            current_key="AF:All",
            current_repositories=repositories,
        )

        self.assertTrue(followup["used"])
        self.assertIn("issueservice", augmented)

    def test_query_limits_retrieval_to_explicit_repository_scope(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Anti Fraud API", "url": "https://git.example.com/team/anti-fraud-api.git"},
                {"display_name": "GRC Portal", "url": "https://git.example.com/team/grc-portal.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        anti_entry = entries[0]
        grc_entry = entries[1]
        anti_path = self.service._repo_path("AF:All", type("Entry", (), anti_entry)())
        grc_path = self.service._repo_path("AF:All", type("Entry", (), grc_entry)())
        (anti_path / ".git").mkdir(parents=True)
        (grc_path / ".git").mkdir(parents=True)
        anti_file = anti_path / "service" / "IssueService.java"
        grc_file = grc_path / "service" / "IssueService.java"
        anti_file.parent.mkdir(parents=True)
        grc_file.parent.mkdir(parents=True)
        anti_file.write_text(
            "class IssueService {\n"
            "    void createIssue() { antiFraudOnly(); }\n"
            "}\n",
            encoding="utf-8",
        )
        grc_file.write_text(
            "class IssueService {\n"
            "    void createIssue() { grcPortalOnly(); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", anti_entry)
        self._build_index_for_entry("AF:All", grc_entry)

        payload = self.service.query(pm_team="AF", country="All", question="where is createIssue in GRC Portal")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["repository_scope"]["active"])
        self.assertEqual(payload["repository_scope"]["selected_repositories"], ["GRC Portal"])
        self.assertGreaterEqual(payload["retrieval_runtime"].get("repository_scope_filters", 0), 1)
        self.assertEqual({match["repo"] for match in payload["matches"]}, {"GRC Portal"})
        self.assertIn("grcPortalOnly", "\n".join(match["snippet"] for match in payload["matches"]))

    def test_repository_scope_prefers_specific_alias_over_generic_token(self):
        repositories = [
            RepositoryEntry(display_name="Anti Fraud API", url="https://git.example.com/team/anti-fraud-api.git"),
            RepositoryEntry(display_name="GRC Portal", url="https://git.example.com/team/grc-portal.git"),
            RepositoryEntry(display_name="Audit Admin", url="https://git.example.com/team/audit-admin.git"),
        ]

        selected, scope = self.service._filter_entries_for_question_repository_scope(
            "where is fraud rule handled in GRC Portal",
            repositories,
        )

        self.assertTrue(scope["active"])
        self.assertEqual([entry.display_name for entry in selected], ["GRC Portal"])
        self.assertEqual(scope["selected_repositories"], ["GRC Portal"])
        self.assertNotIn("Anti Fraud API", scope["selected_repositories"])

    def test_question_specific_terms_cover_cbs_credit_review_questions(self):
        terms = self.service._question_specific_retrieval_terms(
            "When will system query CBS report when performing monthly credit review?"
        )

        self.assertIn("CreditReviewCbsServiceImpl", terms)
        self.assertIn("CreditReviewCbsBureauReportProvider", terms)
        self.assertIn("CR_CBS_REPORT", terms)

    def test_query_uses_ready_persistent_index_and_citations(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where is batchCreateJiraIssue implemented")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(self.service._index_path(repo_path).exists())
        self.assertIn("persistent_index", {match.get("retrieval") for match in payload["matches"]})
        self.assertEqual(payload["citations"][0]["id"], "S1")
        self.assertEqual(payload["citations"][0]["path"], "bpmis/jira_client.py")
        self.assertEqual(payload["index_freshness"]["status"], "fresh")
        self.assertIn("git_revisions", payload["index_freshness"])
        self.assertEqual(self.service.repo_status("AF:All")[0]["index"]["state"], "ready")

    def test_soft_exact_lookup_miss_falls_back_to_broad_search(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "src" / "RiskEngineTimeoutConfig.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "package com.example.risk.engine;\n"
            "public class RiskEngineTimeoutConfig {\n"
            "    public int timeoutSeconds() { return 30; }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)

        payload = self.service.query(pm_team="AF", country="All", question="where is risk.engine.timeout configured")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["exact_lookup"]["terms"], ["risk.engine.timeout"])
        self.assertEqual(payload["exact_lookup"]["matched_terms"], [])
        self.assertGreaterEqual(payload["retrieval_runtime"].get("exact_lookup_soft_misses", 0), 1)
        self.assertIn("src/RiskEngineTimeoutConfig.java", {match["path"] for match in payload["matches"]})

    def test_exact_lookup_miss_still_stops_for_strict_table_terms(self):
        self.assertTrue(self.service._exact_lookup_miss_should_stop(["missing_schema.dwd_missing_table_di"]))
        self.assertTrue(self.service._exact_lookup_miss_should_stop(["mapper/MissingMapper.xml"]))
        self.assertFalse(self.service._exact_lookup_miss_should_stop(["risk.engine.timeout"]))

    def test_query_does_not_build_missing_index_synchronously(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class BPMISClient: pass\n", encoding="utf-8")

        payload = self.service.query(pm_team="AF", country="All", question="where is BPMISClient")

        self.assertEqual(payload["status"], "index_not_ready")
        self.assertFalse(self.service._index_path(repo_path).exists())
        self.assertEqual(payload["index_freshness"]["status"], "stale_or_missing")
        self.assertIn("ready queryable index", payload["summary"])
        self.assertGreaterEqual(payload["retrieval_runtime"].get("index_not_ready_scopes", 0), 1)

    def test_query_reports_not_synced_when_no_local_clone_is_available(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )

        payload = self.service.query(pm_team="AF", country="All", question="where is BPMISClient")

        self.assertEqual(payload["status"], "not_synced")
        self.assertEqual(payload["matches"], [])
        self.assertEqual(payload["index_freshness"]["status"], "stale_or_missing")
        self.assertIn("Sync / Refresh", payload["summary"])

    def test_query_reports_not_synced_for_unsynced_explicit_repository_scope(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Anti Fraud API", "url": "https://git.example.com/team/anti-fraud-api.git"},
                {"display_name": "GRC Portal", "url": "https://git.example.com/team/grc-portal.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        anti_entry = entries[0]
        anti_path = self.service._repo_path("AF:All", type("Entry", (), anti_entry)())
        (anti_path / ".git").mkdir(parents=True)
        source_file = anti_path / "service" / "IssueService.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class IssueService { void createIssue() {} }\n", encoding="utf-8")
        self._build_index_for_entry("AF:All", anti_entry)

        payload = self.service.query(pm_team="AF", country="All", question="where is createIssue in GRC Portal")

        self.assertEqual(payload["status"], "not_synced")
        self.assertTrue(payload["repository_scope"]["active"])
        self.assertEqual(payload["repository_scope"]["selected_repositories"], ["GRC Portal"])
        self.assertEqual(payload["matches"], [])
        self.assertGreaterEqual(payload["retrieval_runtime"].get("repository_scope_filters", 0), 1)

    def test_query_reports_index_not_ready_for_explicit_scope_with_clone_but_no_index(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Anti Fraud API", "url": "https://git.example.com/team/anti-fraud-api.git"},
                {"display_name": "GRC Portal", "url": "https://git.example.com/team/grc-portal.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        anti_entry = entries[0]
        grc_entry = entries[1]
        anti_path = self.service._repo_path("AF:All", type("Entry", (), anti_entry)())
        grc_path = self.service._repo_path("AF:All", type("Entry", (), grc_entry)())
        (anti_path / ".git").mkdir(parents=True)
        (grc_path / ".git").mkdir(parents=True)
        anti_file = anti_path / "service" / "IssueService.java"
        grc_file = grc_path / "service" / "IssueService.java"
        anti_file.parent.mkdir(parents=True)
        grc_file.parent.mkdir(parents=True)
        anti_file.write_text("class IssueService { void createIssue() {} }\n", encoding="utf-8")
        grc_file.write_text("class IssueService { void createIssue() {} }\n", encoding="utf-8")
        self._build_index_for_entry("AF:All", anti_entry)

        payload = self.service.query(pm_team="AF", country="All", question="where is createIssue in GRC Portal")

        self.assertEqual(payload["status"], "index_not_ready")
        self.assertTrue(payload["repository_scope"]["active"])
        self.assertEqual(payload["repository_scope"]["selected_repositories"], ["GRC Portal"])
        self.assertEqual(payload["matches"], [])
        self.assertGreaterEqual(payload["retrieval_runtime"].get("index_not_ready_scopes", 0), 1)

    def test_query_can_use_stale_but_schema_compatible_index(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_entry = type("Entry", (), entry)()
        repo_path = self.service._repo_path("AF:All", repo_entry)
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        self.service._build_repo_index("AF:All", repo_entry, repo_path)
        index_path = self.service._index_path(repo_path)
        indexed_mtime = index_path.stat().st_mtime_ns

        time.sleep(0.01)
        (repo_path / "new_module.py").write_text("class NewModule: pass\n", encoding="utf-8")

        payload = self.service.query(pm_team="AF", country="All", question="where is batchCreateJiraIssue implemented")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["index_freshness"]["status"], "stale_or_missing")
        self.assertIn("Portal Repo", payload["index_freshness"]["stale_repos"])
        self.assertIn("persistent_index", {match.get("retrieval") for match in payload["matches"]})
        self.assertEqual(index_path.stat().st_mtime_ns, indexed_mtime)
        index_info = self.service.repo_status("AF:All")[0]["index"]
        self.assertEqual(index_info["state"], "stale")
        self.assertTrue(index_info["schema_compatible"])
        self.assertTrue(index_info["queryable"])

    def test_query_can_use_previous_queryable_index_without_token_tables(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_entry = type("Entry", (), entry)()
        repo_path = self.service._repo_path("AF:All", repo_entry)
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        self.service._build_repo_index("AF:All", repo_entry, repo_path)
        index_path = self.service._index_path(repo_path)
        with sqlite3.connect(index_path) as connection:
            connection.execute("update metadata set value = '28' where key = 'version'")
            connection.execute("drop table file_tokens")
            connection.execute("drop table line_tokens")
            connection.execute("drop table semantic_chunk_tokens")
            connection.commit()

        payload = self.service.query(pm_team="AF", country="All", question="where is batchCreateJiraIssue implemented")

        self.assertEqual(payload["status"], "ok")
        index_info = self.service.repo_status("AF:All")[0]["index"]
        self.assertEqual(index_info["state"], "stale")
        self.assertFalse(index_info["schema_compatible"])
        self.assertTrue(index_info["queryable"])

    def test_structure_index_extracts_definitions_references_and_telemetry(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "@RequestMapping(\"/api/v1/issues\")\n"
            "public class IssueController {\n"
            "    private IssueRepository issueRepository;\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from bpmis_issue\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where is IssueController createIssue API")

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(any("structure matched" in match["reason"] for match in payload["matches"]))
        index_path = self.service._index_path(repo_path)
        with sqlite3.connect(index_path) as connection:
            metadata = dict(connection.execute("select key, value from metadata").fetchall())
            definition_count = connection.execute("select count(*) from definitions").fetchone()[0]
            reference_count = connection.execute("select count(*) from references_index").fetchone()[0]
            entity_count = connection.execute("select count(*) from code_entities").fetchone()[0]
            entity_edge_count = connection.execute("select count(*) from entity_edges").fetchone()[0]
            edge_count = connection.execute("select count(*) from graph_edges").fetchone()[0]
            flow_edge_count = connection.execute("select count(*) from flow_edges").fetchone()[0]
            semantic_chunk_count = connection.execute("select count(*) from semantic_chunks").fetchone()[0]
            file_fts_count = connection.execute("select count(*) from files_fts").fetchone()[0]
            fts_count = connection.execute("select count(*) from lines_fts").fetchone()[0]
            semantic_fts_count = connection.execute("select count(*) from semantic_chunks_fts").fetchone()[0]
        self.assertGreaterEqual(definition_count, 3)
        self.assertGreaterEqual(reference_count, 3)
        self.assertGreaterEqual(entity_count, 3)
        self.assertGreaterEqual(entity_edge_count, 1)
        self.assertGreaterEqual(edge_count, 1)
        self.assertGreaterEqual(flow_edge_count, 1)
        self.assertGreaterEqual(semantic_chunk_count, 2)
        self.assertGreaterEqual(file_fts_count, 2)
        self.assertGreaterEqual(fts_count, 1)
        self.assertGreaterEqual(semantic_fts_count, 1)
        self.assertEqual(metadata["file_fts_enabled"], "1")
        self.assertEqual(metadata["semantic_fts_enabled"], "1")
        self.assertEqual(metadata["parser_backend"], "tree_sitter+regex")
        self.assertIn("java", metadata["parser_languages"])
        self.assertGreaterEqual(int(metadata["tree_sitter_files"]), 2)
        self.assertIn("semantic_chunk", {match.get("retrieval") for match in payload["matches"]})
        telemetry = self.service.telemetry_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertTrue(telemetry)
        self.assertIn("IssueController", telemetry[-1])

    def test_query_prioritizes_exact_table_lookup_before_broad_search(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Anti Fraud Repo", "url": "https://git.example.com/team/anti-fraud.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        mapper_file = repo_path / "mapper" / "IncomingTransferMapper.xml"
        mapper_file.parent.mkdir(parents=True)
        mapper_file.write_text(
            "<mapper namespace=\"IncomingTransferMapper\">\n"
            "  <select id=\"readDetail\">\n"
            "    select * from bmart_antifraud.dwd_antifraud_incoming_transfer_detailed_di\n"
            "    join tmp_dwd_antifraud_incoming_transfer_detailed_df on id = transfer_id\n"
            "  </select>\n"
            "</mapper>\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)

        with patch.object(self.service, "_search_repo", wraps=self.service._search_repo) as broad_search:
            payload = self.service.query(
                pm_team="AF",
                country="All",
                question=(
                    "relation between bmart_antifraud.dwd_antifraud_incoming_transfer_detailed_di "
                    "and tmp_dwd_antifraud_incoming_transfer_detailed_df"
                ),
            )

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["exact_lookup"]["sufficient"])
        self.assertEqual(
            set(payload["exact_lookup"]["matched_terms"]),
            {
                "bmart_antifraud.dwd_antifraud_incoming_transfer_detailed_di",
                "tmp_dwd_antifraud_incoming_transfer_detailed_df",
            },
        )
        self.assertEqual(broad_search.call_count, 0)
        self.assertIn("exact_table_path_lookup", {match.get("retrieval") for match in payload["matches"]})
        self.assertGreaterEqual(payload["retrieval_runtime"].get("exact_lookup_hits", 0), 2)

    def test_exact_table_lookup_keeps_schema_qualified_table_hits_in_evidence(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "anti-fraud-admin", "url": "https://git.example.com/team/anti-fraud-admin.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        transfer_job = repo_path / "anti-fraud-admin-app/src/main/java/com/shopee/banking/af/admin/app/job/xxl/BaseSyncIncomingDataToIvlog.java"
        account_job = repo_path / "anti-fraud-admin-app/src/main/java/com/shopee/banking/af/admin/app/job/xxl/OneTimesSyncDwhIncomingAccountInfoJob.java"
        config_doc = repo_path / "spec/arch/config.md"
        transfer_job.parent.mkdir(parents=True)
        account_job.parent.mkdir(parents=True, exist_ok=True)
        config_doc.parent.mkdir(parents=True)
        transfer_job.write_text(
            "public class BaseSyncIncomingDataToIvlog {\n"
            "    /** dwd_antifraud_incoming_transfer_detailed_di */\n"
            "    @Value(\"${mkt-opt.dwh.incoming.transfer.tab.name:dwd_antifraud_incoming_transfer_detailed_di}\")\n"
            "    protected String incomingTransferTabName;\n"
            "}\n",
            encoding="utf-8",
        )
        account_job.write_text(
            "public class OneTimesSyncDwhIncomingAccountInfoJob {\n"
            "    /** tmp_dwd_antifraud_incoming_transfer_detailed_df */\n"
            "    @Value(\"${mkt-opt.dwh.incoming.account.tab.name:tmp_dwd_antifraud_incoming_transfer_detailed_df}\")\n"
            "    protected String incomingAccountTabName;\n"
            "}\n",
            encoding="utf-8",
        )
        config_doc.write_text(
            "| mkt-opt.dwh.incoming.account.tab.name | tmp_dwd_antifraud_incoming_transfer_detailed_df |\n"
            "| mkt-opt.dwh.incoming.transfer.tab.name | dwd_antifraud_incoming_transfer_detailed_di |\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("AF:All", entry)

        with patch.object(self.service, "_search_repo", wraps=self.service._search_repo) as broad_search:
            payload = self.service.query(
                pm_team="AF",
                country="All",
                question=(
                    "Can you check the relation between "
                    "bmart_antifraud.dwd_antifraud_incoming_transfer_detailed_di and "
                    "tmp_dwd_antifraud_incoming_transfer_detailed_df?"
                ),
            )

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["exact_lookup"]["sufficient"])
        self.assertEqual(
            set(payload["exact_lookup"]["matched_terms"]),
            {
                "bmart_antifraud.dwd_antifraud_incoming_transfer_detailed_di",
                "tmp_dwd_antifraud_incoming_transfer_detailed_df",
            },
        )
        self.assertEqual(broad_search.call_count, 0)
        paths = [match["path"] for match in payload["matches"]]
        self.assertIn(
            "anti-fraud-admin-app/src/main/java/com/shopee/banking/af/admin/app/job/xxl/BaseSyncIncomingDataToIvlog.java",
            paths,
        )
        self.assertIn(
            "anti-fraud-admin-app/src/main/java/com/shopee/banking/af/admin/app/job/xxl/OneTimesSyncDwhIncomingAccountInfoJob.java",
            paths,
        )
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("dwd_antifraud_incoming_transfer_detailed_di", evidence_text)
        self.assertIn("tmp_dwd_antifraud_incoming_transfer_detailed_df", evidence_text)

    def test_myinfo_income_logic_does_not_trigger_data_source_from_preposition(self):
        intent = self.service._question_intent("What is the logic to calculate income from MyInfo CPF and NOA?")

        self.assertTrue(intent["rule_logic"])
        self.assertFalse(intent["data_source"])

    def test_sufficient_myinfo_income_logic_skips_agent_plan(self):
        self.service.save_mapping(
            pm_team="CRMS",
            country="SG",
            repositories=[{"display_name": "credit-risk-model", "url": "https://git.example.com/team/credit-risk-model.git"}],
        )
        entry = self.service.load_config()["mappings"]["CRMS:SG"][0]
        repo_path = self.service._repo_path("CRMS:SG", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        feature_file = repo_path / "feature" / "retail_feature_engineering.py"
        feature_file.parent.mkdir(parents=True)
        feature_file.write_text(
            "class RetailFeatureEngineering:\n"
            "    @classmethod\n"
            "    def get_myinfo_monthly_income(self, myInfoData, **kwargs):\n"
            "        myInfoData_dict = myInfoData.copy()\n"
            "        myinfo_monthly_income = self.convert_to_float(myInfoData_dict.get('monthlyIncome'))\n"
            "        if np.isnan(myinfo_monthly_income):\n"
            "            myinfo_monthly_income = 0\n"
            "        return myinfo_monthly_income\n",
            encoding="utf-8",
        )
        self._build_index_for_entry("CRMS:SG", entry)

        with patch.object(self.service, "_run_agent_plan", wraps=self.service._run_agent_plan) as agent_plan:
            payload = self.service.query(
                pm_team="CRMS",
                country="SG",
                question="What is the logic to calculate income from MyInfo CPF and NOA?",
            )

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(agent_plan.call_count, 0)
        self.assertTrue(any(match["path"] == "feature/retail_feature_engineering.py" for match in payload["matches"]))

    def test_index_rebuild_reuses_unchanged_file_rows(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_entry = type("Entry", (), entry)()
        repo_path = self.service._repo_path("AF:All", repo_entry)
        (repo_path / ".git").mkdir(parents=True)
        alpha_file = repo_path / "service" / "AlphaService.java"
        beta_file = repo_path / "service" / "BetaService.java"
        alpha_file.parent.mkdir(parents=True)
        alpha_file.write_text(
            "public class AlphaService {\n"
            "    public String alpha() { return \"alpha\"; }\n"
            "}\n",
            encoding="utf-8",
        )
        beta_file.write_text(
            "public class BetaService {\n"
            "    public String beta() { return \"beta\"; }\n"
            "}\n",
            encoding="utf-8",
        )

        first_result = self.service._build_repo_index("AF:All", repo_entry, repo_path)
        self.assertEqual(first_result["reused_files"], 0)
        self.assertEqual(first_result["reparsed_files"], 2)

        time.sleep(0.01)
        beta_file.write_text(
            "public class BetaServiceV2 {\n"
            "    public String beta() { return \"beta-v2\"; }\n"
            "    public String extra() { return \"extra\"; }\n"
            "}\n",
            encoding="utf-8",
        )
        os.utime(beta_file, None)

        original_extract = self.service._extract_structure_rows
        with patch.object(self.service, "_extract_structure_rows", wraps=original_extract) as mocked_extract:
            second_result = self.service._build_repo_index("AF:All", repo_entry, repo_path)

        self.assertEqual(mocked_extract.call_count, 1)
        self.assertEqual(second_result["reused_files"], 1)
        self.assertEqual(second_result["reparsed_files"], 1)
        index_info = self.service._repo_index_info("AF:All", repo_entry, repo_path)
        self.assertEqual(index_info["reused_files"], 1)
        self.assertEqual(index_info["reparsed_files"], 1)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            names = {
                row[0]
                for row in connection.execute(
                    "select name from definitions where name in ('AlphaService', 'BetaServiceV2')"
                ).fetchall()
            }
            line_count = connection.execute("select count(*) from lines").fetchone()[0]
        self.assertIn("AlphaService", names)
        self.assertIn("BetaServiceV2", names)
        self.assertGreaterEqual(line_count, 7)

    def test_ops_summary_reports_recent_source_qa_signals(self):
        from scripts.source_code_qa_ops_summary import build_summary

        data_root = Path(self.temp_dir.name)
        source_root = data_root / "source_code_qa"
        run_root = data_root / "run"
        source_root.mkdir(parents=True)
        run_root.mkdir(parents=True)
        (source_root / "telemetry.jsonl").write_text(
            json.dumps(
                {
                    "created_at": "2026-04-25T10:00:00+08:00",
                    "status": "ok",
                    "latency_ms": 120,
                    "llm_route": {"mode": "hybrid"},
                    "answer_contract": {"status": "satisfied"},
                }
            )
            + "\n"
            + json.dumps(
                {
                    "created_at": "2026-04-25T10:01:00+08:00",
                    "status": "no_match",
                    "latency_ms": 360,
                    "llm_route": {"mode": "retrieval"},
                    "answer_contract": {"status": "insufficient_evidence"},
                    "index_freshness": {"status": "stale_or_missing"},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        (source_root / "feedback.jsonl").write_text(
            json.dumps({"created_at": "2026-04-25T10:02:00+08:00", "rating": "needs_deeper_trace"}) + "\n",
            encoding="utf-8",
        )
        (run_root / "source_code_qa_eval_status.json").write_text(
            json.dumps({"state": "passed", "updated_unix": int(time.time())}),
            encoding="utf-8",
        )

        summary = "\n".join(build_summary(data_root, limit=20))

        self.assertIn("telemetry_window=2", summary)
        self.assertIn("query_status=ok=1, no_match=1", summary)
        self.assertIn("no_match_rate=1/2", summary)
        self.assertIn("feedback_window=1", summary)
        self.assertIn("review_queue=0", summary)
        self.assertIn("latest_eval_state=passed", summary)

    def test_ops_summary_flags_fixture_repo_config_in_strict_mode(self):
        from scripts.source_code_qa_ops_summary import build_summary

        data_root = Path(self.temp_dir.name)
        source_root = data_root / "source_code_qa"
        source_root.mkdir(parents=True)
        (source_root / "config.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "mappings": {
                        "CRMS:SG": [
                            {"display_name": "Credit Risk SG", "url": "https://git.example.com/team/credit-risk-sg.git"}
                        ]
                    },
                }
            ),
            encoding="utf-8",
        )

        summary = "\n".join(build_summary(data_root, limit=20, strict=True))

        self.assertIn("active_config_demo_repos=1", summary)
        self.assertIn("ops_summary_status=fail", summary)
        self.assertIn("fixture/demo repositories", summary)

    def test_domain_profiles_include_domain_knowledge_pack_terms(self):
        crms_profile = self.service._domain_profile("CRMS", "SG")
        af_profile = self.service._domain_profile("AF", "All")
        grc_profile = self.service._domain_profile("GRC", "All")
        knowledge = self.service.domain_knowledge_payload()

        self.assertIn("TermLoanPreCheckEngine", crms_profile["source_terms"])
        self.assertIn("Term Loan PreCheck", crms_profile["knowledge_terms"])
        self.assertIn("BlackWhiteList", af_profile["data_carriers"])
        self.assertIn("CaseReviewController", af_profile["api_terms"])
        self.assertIn("mysql-isolate.globallock", grc_profile["config_terms"])
        self.assertIn("ApprovalController", grc_profile["api_terms"])
        self.assertEqual(knowledge["domains"]["CRMS"]["label"], "Credit Risk")
        self.assertGreaterEqual(knowledge["domains"]["AF"]["module_count"], 5)
        self.assertGreaterEqual(knowledge["domains"]["GRC"]["question_count"], 4)

    def test_llm_domain_context_includes_team_rules_and_answer_blueprint(self):
        crms_context = self.service._llm_domain_context(
            pm_team="CRMS",
            country="SG",
            question="What is the logic to calculate income from MyInfo CPF and NOA?",
            evidence_summary={"intent": self.service._question_intent("What data source is used for MyInfo income?")},
        )
        af_context = self.service._llm_domain_context(
            pm_team="AF",
            country="All",
            question="Can you check the relation between dwd_antifraud_incoming_transfer_detailed_di and tmp_dwd_antifraud_incoming_transfer_detailed_df?",
            evidence_summary={"intent": self.service._question_intent("what data source relation exists between two tables?")},
        )
        grc_context = self.service._llm_domain_context(
            pm_team="GRC",
            country="All",
            question="GRC globallock 配置在哪里？",
            evidence_summary={"intent": self.service._question_intent("where is globallock config?")},
        )

        self.assertIn("Domain guidance: Credit Risk", crms_context)
        self.assertIn("SG MyInfo Income", crms_context)
        self.assertIn("final source/table/API", crms_context)
        self.assertIn("Domain guidance: Anti-Fraud", af_context)
        self.assertIn("co-occurrence in the same flow", af_context)
        self.assertIn("Domain guidance: Ops Risk", grc_context)
        self.assertIn("mysql-isolate.globallock", grc_context)
        self.assertIn("Evidence priority", grc_context)

    def test_index_health_payload_summarizes_ready_indexes(self):
        entry = RepositoryEntry(display_name="Portal Repo", url="https://git.example.com/team/portal.git")
        self.service.save_mapping(pm_team="AF", country="All", repositories=[{"display_name": entry.display_name, "url": entry.url}])
        repo_path = self.service._repo_path("AF:All", entry)
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "IssueService.java").write_text(
            "class IssueService { void createIssue() { repository.save(); } }\n",
            encoding="utf-8",
        )
        self.service._build_repo_index("AF:All", entry, repo_path)

        health = self.service.index_health_payload()

        self.assertEqual(health["status"], "ready")
        self.assertEqual(health["totals"]["repos"], 1)
        self.assertEqual(health["totals"]["ready"], 1)
        self.assertIn("AF:All", health["keys"])

    def test_release_gate_evaluator_enforces_case_floor(self):
        from scripts.run_source_code_qa_release_gate import evaluate_release_gate

        gate = evaluate_release_gate(
            {
                "status": "pass",
                "eval": {
                    "status": "pass",
                    "total": 4,
                    "failed": 0,
                    "team_buckets": {"AF": {"total": 4, "failed": 0}},
                    "segment_buckets": {"AF:ALL": {"total": 4, "failed": 0}},
                },
                "llm_smoke": {"status": "pass", "failed": 0},
                "review_queue": {"returncode": 0},
            },
            thresholds={"min_eval_cases": 10, "required_eval_teams": ["AF"], "required_eval_segments": ["AF:ALL"]},
        )

        self.assertEqual(gate["status"], "fail")
        self.assertIn("min_eval_cases", gate["failed_checks"])

    def test_release_gate_evaluator_requires_team_balanced_coverage(self):
        from scripts.run_source_code_qa_release_gate import evaluate_release_gate

        gate = evaluate_release_gate(
            {
                "status": "pass",
                "eval": {
                    "status": "pass",
                    "total": 8,
                    "failed": 0,
                    "team_buckets": {
                        "AF": {"total": 4, "failed": 0},
                        "CRMS": {"total": 4, "failed": 0},
                    },
                    "segment_buckets": {
                        "AF:ALL": {"total": 4, "failed": 0},
                        "CRMS:ID": {"total": 4, "failed": 0},
                    },
                },
                "llm_smoke": {"status": "pass", "failed": 0},
                "review_queue": {"returncode": 0},
            },
            thresholds={
                "min_eval_cases": 8,
                "min_eval_cases_per_team": 4,
                "required_eval_segments": ["AF:ALL", "CRMS:ID"],
                "required_eval_teams": ["AF", "CRMS", "GRC"],
            },
        )

        self.assertEqual(gate["status"], "fail")
        self.assertIn("eval_team_coverage", gate["failed_checks"])
        self.assertEqual(gate["missing_or_thin_teams"], ["GRC"])

    def test_release_gate_evaluator_requires_country_segment_coverage(self):
        from scripts.run_source_code_qa_release_gate import evaluate_release_gate

        gate = evaluate_release_gate(
            {
                "status": "pass",
                "eval": {
                    "status": "pass",
                    "total": 12,
                    "failed": 0,
                    "team_buckets": {
                        "AF": {"total": 4, "failed": 0},
                        "CRMS": {"total": 4, "failed": 0},
                        "GRC": {"total": 4, "failed": 0},
                    },
                    "segment_buckets": {
                        "AF:ALL": {"total": 4, "failed": 0},
                        "CRMS:ID": {"total": 4, "failed": 0},
                        "GRC:ALL": {"total": 4, "failed": 0},
                    },
                },
                "llm_smoke": {"status": "pass", "failed": 0},
                "review_queue": {"returncode": 0},
            },
            thresholds={
                "min_eval_cases": 12,
                "min_eval_cases_per_segment": 2,
                "required_eval_segments": ["AF:ALL", "CRMS:ID", "CRMS:SG", "CRMS:PH", "GRC:ALL"],
            },
        )

        self.assertEqual(gate["status"], "fail")
        self.assertIn("eval_segment_coverage", gate["failed_checks"])
        self.assertEqual(gate["missing_or_thin_segments"], ["CRMS:SG", "CRMS:PH"])

    def test_release_gate_evaluator_accepts_all_required_teams(self):
        from scripts.run_source_code_qa_release_gate import evaluate_release_gate

        gate = evaluate_release_gate(
            {
                "status": "pass",
                "eval": {
                    "status": "pass",
                    "total": 12,
                    "failed": 0,
                    "team_buckets": {
                        "AF": {"total": 4, "failed": 0},
                        "CRMS": {"total": 6, "failed": 0},
                        "GRC": {"total": 4, "failed": 0},
                    },
                    "segment_buckets": {
                        "AF:ALL": {"total": 4, "failed": 0},
                        "CRMS:ID": {"total": 2, "failed": 0},
                        "CRMS:SG": {"total": 2, "failed": 0},
                        "CRMS:PH": {"total": 2, "failed": 0},
                        "GRC:ALL": {"total": 4, "failed": 0},
                    },
                },
                "llm_smoke": {"status": "pass", "failed": 0},
                "review_queue": {"returncode": 0},
            },
            thresholds={"min_eval_cases": 12, "min_eval_cases_per_team": 4},
        )

        self.assertEqual(gate["status"], "pass")
        self.assertEqual(gate["missing_or_thin_teams"], [])
        self.assertEqual(gate["missing_or_thin_segments"], [])

    def test_nightly_fixture_eval_uses_isolated_data_root(self):
        from scripts.run_source_code_qa_nightly_eval import run_nightly_eval

        calls = []

        def fake_run(args):
            calls.append(args)
            if any(str(arg).endswith("source_code_qa_evals.py") for arg in args):
                return {
                    "status": "pass",
                    "total": 1,
                    "failed": 0,
                    "team_buckets": {"AF": {"total": 1, "failed": 0}},
                    "segment_buckets": {"AF:ALL": {"total": 1, "failed": 0}},
                }, "{}", "", 0
            return {"status": "ok", "review_items": 0}, "{}", "", 0

        output_dir = Path(self.temp_dir.name) / "eval_runs"
        with patch("scripts.run_source_code_qa_nightly_eval._run_json_command", side_effect=fake_run):
            report = run_nightly_eval(output_dir=output_dir, cases=["evals/source_code_qa/golden.jsonl"], fixture=True, include_useful_feedback=False)

        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["eval"]["team_buckets"]["AF"]["total"], 1)
        self.assertEqual(report["eval"]["segment_buckets"]["AF:ALL"]["total"], 1)
        eval_call = calls[0]
        self.assertIn("--data-root", eval_call)
        self.assertEqual(Path(eval_call[eval_call.index("--data-root") + 1]), output_dir / "fixture_data")

    def test_release_gate_uses_mock_llm_by_default(self):
        from scripts.run_source_code_qa_release_gate import run_release_gate

        captured = {}

        def fake_run_nightly_eval(**kwargs):
            captured.update(kwargs)
            return {
                "status": "pass",
                "eval": {
                    "status": "pass",
                    "total": 1,
                    "failed": 0,
                    "team_buckets": {"AF": {"total": 1, "failed": 0}},
                    "segment_buckets": {"AF:ALL": {"total": 1, "failed": 0}},
                },
                "llm_smoke": {"status": "pass", "failed": 0},
                "review_queue": {"returncode": 0},
            }

        with patch("scripts.run_source_code_qa_release_gate.run_nightly_eval", side_effect=fake_run_nightly_eval):
            gate = run_release_gate(
                data_root=Path(self.temp_dir.name),
                cases=["evals/source_code_qa/llm_smoke.jsonl"],
                fixture=True,
                include_useful_feedback=False,
                thresholds={
                    "min_eval_cases": 1,
                    "min_eval_cases_per_team": 1,
                    "min_eval_cases_per_segment": 1,
                    "required_eval_teams": ["AF"],
                    "required_eval_segments": ["AF:ALL"],
                },
            )

        self.assertEqual(gate["status"], "pass")
        self.assertTrue(captured["mock_llm"])

    def test_nightly_eval_can_pass_mock_llm_to_main_eval(self):
        from scripts.run_source_code_qa_nightly_eval import run_nightly_eval

        calls = []

        def fake_run(args):
            calls.append(args)
            if any(str(arg).endswith("source_code_qa_evals.py") for arg in args):
                return {
                    "status": "pass",
                    "total": 1,
                    "failed": 0,
                    "team_buckets": {"AF": {"total": 1, "failed": 0}},
                    "segment_buckets": {"AF:ALL": {"total": 1, "failed": 0}},
                }, "{}", "", 0
            return {"status": "ok", "review_items": 0}, "{}", "", 0

        output_dir = Path(self.temp_dir.name) / "eval_runs"
        with patch("scripts.run_source_code_qa_nightly_eval._run_json_command", side_effect=fake_run):
            report = run_nightly_eval(
                output_dir=output_dir,
                cases=["evals/source_code_qa/golden.jsonl"],
                fixture=True,
                include_useful_feedback=False,
                mock_llm=True,
            )

        self.assertTrue(report["mock_llm"])
        self.assertIn("--mock-llm", calls[0])

    def test_fixture_eval_requires_isolated_data_root(self):
        data_root = Path(self.temp_dir.name)

        with self.assertRaises(SystemExit):
            _guard_fixture_data_root(data_root=data_root, main_data_root=data_root, data_root_explicit=False)

        with self.assertRaises(SystemExit):
            _guard_fixture_data_root(data_root=data_root, main_data_root=data_root, data_root_explicit=True)

        _guard_fixture_data_root(data_root=data_root / "fixture_data", main_data_root=data_root, data_root_explicit=True)

    def test_review_queue_collects_feedback_and_telemetry_risks(self):
        from scripts.source_code_qa_review_queue import build_review_queue

        data_root = Path(self.temp_dir.name)
        source_root = data_root / "source_code_qa"
        source_root.mkdir(parents=True)
        (source_root / "feedback.jsonl").write_text(
            json.dumps(
                {
                    "timestamp": "2026-04-25T10:05:00+08:00",
                    "rating": "wrong_file",
                    "pm_team": "AF",
                    "country": "All",
                    "question_preview": "where is batch create",
                    "question_sha1": "abc123",
                    "trace_id": "trace-feedback",
                    "replay_context": {
                        "answer_contract": {"status": "grounded"},
                        "matches_snapshot": [{"path": "wrong/File.java"}],
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        (source_root / "telemetry.jsonl").write_text(
            json.dumps(
                {
                    "timestamp": "2026-04-25T10:06:00+08:00",
                    "status": "no_match",
                    "key": "AF:All",
                    "question_preview": "where is missing flow",
                    "question_sha1": "def456",
                    "trace_id": "trace-telemetry",
                    "top_paths": [],
                    "index_freshness": {"status": "fresh"},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        queue = build_review_queue(data_root)

        self.assertEqual(len(queue), 2)
        self.assertEqual(queue[0]["priority"], "high")
        self.assertEqual({item["source"] for item in queue}, {"feedback", "telemetry"})
        feedback_item = next(item for item in queue if item["source"] == "feedback")
        self.assertEqual(feedback_item["observed_paths"], ["wrong/File.java"])
        self.assertEqual(feedback_item["draft_eval"]["draft_status"], "needs_human_expected_evidence")

    def test_planner_tool_loop_adds_code_graph_matches(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="trace issue create API data source")

        retrievals = {match.get("retrieval") for match in payload["matches"]}
        paths = {match["path"] for match in payload["matches"]}
        trace_stages = {match.get("trace_stage") for match in payload["matches"]}
        self.assertTrue({"planner_definition", "planner_reference", "code_graph", "flow_graph", "entity_graph"} & retrievals)
        self.assertIn("repository/IssueRepository.java", paths)
        self.assertTrue(any(str(stage).startswith("tool_loop_") for stage in trace_stages) or "semantic_chunk" in retrievals)
        self.assertTrue(payload["tool_trace"])
        self.assertTrue(any(step.get("phase") in {"tool_loop", "agent_plan"} for step in payload["tool_trace"]))
        telemetry = json.loads(self.service.telemetry_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertGreaterEqual(telemetry["tool_trace_summary"]["steps"], 1)

    def test_python_ast_index_extracts_entities_and_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "services" / "issue_service.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "from bpmis.client import BPMISClient\n"
            "class IssueService:\n"
            "    def create_issue(self):\n"
            "        client = BPMISClient()\n"
            "        return client.batchCreateJiraIssue()\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where does create_issue call BPMIS")
        index_path = self.service._index_path(repo_path)

        self.assertEqual(payload["status"], "ok")
        with sqlite3.connect(index_path) as connection:
            entity_names = {row[0] for row in connection.execute("select name from code_entities")}
            edge_targets = {row[0] for row in connection.execute("select to_name from entity_edges")}
        self.assertIn("IssueService", entity_names)
        self.assertIn("create_issue", entity_names)
        self.assertTrue(any("batchCreateJiraIssue" in target for target in edge_targets))

    def test_flow_graph_can_trace_controller_service_repository_table_chain(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "@PostMapping(\"/issue/create\")\n"
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which API flow reaches issue data source table")

        self.assertEqual(payload["status"], "ok")
        self.assertIn("repository/IssueRepository.java", {match["path"] for match in payload["matches"]})
        self.assertTrue({"flow_graph", "entity_graph"} & {match.get("retrieval") for match in payload["matches"]})
        self.assertTrue(any("issue_table" in match["snippet"] for match in payload["matches"]))
        self.assertTrue(payload["trace_paths"])
        self.assertIn("issue_table", str(payload["trace_paths"]))

    def test_framework_adapters_extract_spring_mybatis_and_feign_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    @PostMapping(\"/issue/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueClient.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        client_file = repo_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"issue-service\", url = \"https://issue.example.com\")\n"
            "public interface IssueClient {\n"
            "    @PostMapping(\"/remote/issue\")\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        mapper_file = repo_path / "mapper" / "IssueMapper.xml"
        mapper_file.parent.mkdir(parents=True)
        mapper_file.write_text(
            "<mapper namespace=\"com.example.IssueMapper\">\n"
            "  <resultMap id=\"IssueResult\" type=\"com.example.IssueEntity\">\n"
            "    <result property=\"id\" column=\"id\" javaType=\"java.lang.Long\" />\n"
            "  </resultMap>\n"
            "  <sql id=\"BaseColumns\">id, name</sql>\n"
            "  <select id=\"findIssue\" parameterType=\"com.example.IssueQuery\" resultMap=\"IssueResult\">\n"
            "    <include refid=\"BaseColumns\" />\n"
            "    select * from issue_table\n"
            "  </select>\n"
            "</mapper>\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="trace create issue API to downstream service and table")
        index_path = self.service._index_path(repo_path)

        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["trace_paths"])
        with sqlite3.connect(index_path) as connection:
            edge_rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertIn(("downstream_api", "issue-service"), edge_rows)
        self.assertIn(("http_endpoint", "https://issue.example.com"), edge_rows)
        self.assertTrue(any(kind == "mapper_statement" and "findIssue" in target for kind, target in edge_rows))
        self.assertTrue(any(kind == "result_map" and "IssueResult" in target for kind, target in edge_rows))
        self.assertTrue(any(kind == "mybatis_result_map_ref" and target == "IssueResult" for kind, target in edge_rows))
        self.assertTrue(any(kind == "mybatis_include_refid" and target == "BaseColumns" for kind, target in edge_rows))
        self.assertTrue(any(kind == "mybatis_type_ref" and target == "com.example.IssueEntity" for kind, target in edge_rows))
        self.assertTrue(any(kind == "sql_table" and target == "issue_table" for kind, target in edge_rows))

    def test_cross_repo_graph_links_client_to_service_repo(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"issue-service\")\n"
            "public interface IssueClient { Issue createIssue(); }\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@PostMapping(\"/issue/create\")\n"
            "public class IssueController { public Issue createIssue() { return new Issue(); } }\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service does issue client call")

        self.assertEqual(payload["repo_graph"]["version"], 2)
        edges = payload["repo_graph"]["edges"]
        graph_edge = next(
            edge for edge in edges if edge["from_repo"] == "Portal Repo" and edge["to_repo"] == "Issue Service"
        )
        self.assertIn("confidence", graph_edge)
        self.assertIn("match_reason", graph_edge)
        self.assertGreaterEqual(graph_edge["confidence"], 0.7)

    def test_tree_sitter_java_extracts_injection_routes_and_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "@RestController\n"
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    @PostMapping(\"/issue/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "tree_sitter+regex")
        self.assertIn("java", index_info["parser_languages"])
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertIn(("injects", "IssueService"), rows)
        self.assertIn(("route", "/issue/create"), rows)
        self.assertTrue(any(kind == "call" and "createIssue" in target for kind, target in rows))

    def test_spring_route_prefix_and_constructor_injection_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/api/issues\")\n"
            "public class IssueController {\n"
            "    private final IssueService issueService;\n"
            "    public IssueController(IssueService issueService) {\n"
            "        this.issueService = issueService;\n"
            "    }\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertIn(("route", "/api/issues/create"), rows)
        self.assertIn(("injects", "IssueService"), rows)

    def test_java_member_calls_resolve_to_qualified_repository_methods(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            edge_rows = list(
                connection.execute(
                    """
                    select edge_kind, to_name, to_file
                    from entity_edges
                    where from_file = 'service/IssueService.java'
                    """
                )
            )
            flow_rows = list(
                connection.execute(
                    """
                    select edge_kind, to_name, to_file
                    from flow_edges
                    where from_file = 'service/IssueService.java'
                    """
                )
            )
        self.assertIn(("call", "IssueRepository.findIssue", "repository/IssueRepository.java"), edge_rows)
        self.assertTrue(
            any(kind == "repository" and target == "IssueRepository.findIssue" for kind, target, _to_file in flow_rows)
        )

    def test_java_interface_and_mybatis_namespace_connect_to_implementations(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text(
            "public interface IssueService {\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        mapper_interface = repo_path / "mapper" / "IssueMapper.java"
        mapper_interface.parent.mkdir(parents=True)
        mapper_interface.write_text(
            "public interface IssueMapper {\n"
            "    Issue findIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        mapper_xml = repo_path / "mapper" / "IssueMapper.xml"
        mapper_xml.write_text(
            "<mapper namespace=\"com.example.IssueMapper\">\n"
            "  <select id=\"findIssue\">select * from issue_table</select>\n"
            "</mapper>\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            edge_rows = list(connection.execute("select edge_kind, to_name, to_file from entity_edges"))
            flow_rows = list(connection.execute("select edge_kind, to_name, to_file from flow_edges"))
        self.assertIn(("implements", "IssueService", "service/IssueService.java"), edge_rows)
        self.assertIn(("mapper_interface", "IssueMapper", "mapper/IssueMapper.java"), edge_rows)
        self.assertTrue(any(kind == "mapper" and target == "IssueMapper" for kind, target, _to_file in flow_rows))

    def test_interface_method_calls_resolve_to_implementation_methods(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text(
            "public interface IssueService {\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"primaryIssueService\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(connection.execute("select edge_kind, to_name, to_file from entity_edges"))
            flow_edges = list(connection.execute("select edge_kind, to_name, to_file from flow_edges"))

        self.assertIn(("bean_name", "primaryIssueService", ""), entity_edges)
        self.assertIn(("implementation_call", "IssueServiceImpl.createIssue", "service/IssueServiceImpl.java"), entity_edges)
        self.assertTrue(
            any(kind == "service" and target == "IssueServiceImpl.createIssue" for kind, target, _to_file in flow_edges)
        )

    def test_qualifier_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    @Qualifier(\"fastIssueService\")\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() {\n"
            "        return issueService.createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text(
            "public interface IssueService {\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, to_file, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn(
            ("implementation_call", "FastIssueService.createIssue", "service/FastIssueService.java"),
            [(kind, target, to_file) for kind, target, to_file, _evidence in implementation_edges],
        )
        self.assertFalse(any(target == "SlowIssueService.createIssue" for _kind, target, _to_file, _evidence in implementation_edges))
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _kind, _target, _to_file, evidence in implementation_edges))

    def test_variable_qualifier_filters_each_interface_call_independently(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    @Qualifier(\"fastIssueService\")\n"
            "    private IssueService fastIssueService;\n"
            "    @Qualifier(\"slowIssueService\")\n"
            "    private IssueService slowIssueService;\n"
            "    public Issue createFastIssue() { return fastIssueService.createIssue(); }\n"
            "    public Issue createSlowIssue() { return slowIssueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select from_line, to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    order by from_line, to_name
                    """
                )
            )

        fast_edges = [(target, evidence) for line_no, target, evidence in implementation_edges if line_no == 6]
        slow_edges = [(target, evidence) for line_no, target, evidence in implementation_edges if line_no == 7]
        self.assertEqual(["FastIssueService.createIssue"], [target for target, _evidence in fast_edges])
        self.assertEqual(["SlowIssueService.createIssue"], [target for target, _evidence in slow_edges])
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _target, evidence in fast_edges))
        self.assertTrue(any("qualifier=slowIssueService" in evidence for _target, evidence in slow_edges))

    def test_constructor_parameter_qualifiers_resolve_assigned_fields_independently(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService fastIssueService;\n"
            "    private IssueService slowIssueService;\n"
            "    public IssueController(@Qualifier(\"fastIssueService\") IssueService fastDelegate, @Qualifier(\"slowIssueService\") IssueService slowDelegate) {\n"
            "        this.fastIssueService = fastDelegate;\n"
            "        this.slowIssueService = slowDelegate;\n"
            "    }\n"
            "    public Issue createFastIssue() { return fastIssueService.createIssue(); }\n"
            "    public Issue createSlowIssue() { return slowIssueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    order by from_line, to_name
                    """
                )
            )

        fast_edges = [(target, evidence) for target, evidence in implementation_edges if "fastIssueService.createIssue" in evidence]
        slow_edges = [(target, evidence) for target, evidence in implementation_edges if "slowIssueService.createIssue" in evidence]
        self.assertEqual(["FastIssueService.createIssue"], [target for target, _evidence in fast_edges])
        self.assertEqual(["SlowIssueService.createIssue"], [target for target, _evidence in slow_edges])
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _target, evidence in fast_edges))
        self.assertTrue(any("qualifier=slowIssueService" in evidence for _target, evidence in slow_edges))

    def test_generic_collection_injection_resolves_lambda_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.List;\n"
            "public class IssueController {\n"
            "    private List<IssueService> issueServices;\n"
            "    public void createAllIssues() {\n"
            "        issueServices.forEach(issueService -> issueService.createIssue());\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        targets = {target for target, _evidence in implementation_edges}
        self.assertIn("FastIssueService.createIssue", targets)
        self.assertIn("SlowIssueService.createIssue", targets)
        self.assertTrue(any("issueService.createIssue" in evidence for _target, evidence in implementation_edges))

    def test_qualified_generic_collection_injection_filters_lambda_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.List;\n"
            "public class IssueController {\n"
            "    @Qualifier(\"fastIssueService\")\n"
            "    private List<IssueService> issueServices;\n"
            "    public void createFastIssues() {\n"
            "        issueServices.forEach(issueService -> issueService.createIssue());\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("qualifier=fastIssueService" in evidence for _target, evidence in implementation_edges))

    def test_generic_map_values_lambda_resolves_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.Map;\n"
            "public class IssueController {\n"
            "    private Map<String, IssueService> issueServiceMap;\n"
            "    public void createAllIssues() {\n"
            "        issueServiceMap.values().forEach(issueService -> issueService.createIssue());\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        targets = {target for target, _evidence in implementation_edges}
        self.assertIn("FastIssueService.createIssue", targets)
        self.assertIn("SlowIssueService.createIssue", targets)
        self.assertTrue(any("issueService.createIssue" in evidence for _target, evidence in implementation_edges))

    def test_generic_stream_map_lambda_resolves_interface_calls(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import java.util.List;\n"
            "public class IssueController {\n"
            "    private List<IssueService> issueServices;\n"
            "    public void createAllIssues() {\n"
            "        issueServices.stream().map(issueService -> issueService.createIssue()).toList();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("IssueServiceImpl.createIssue", [target for target, _evidence in implementation_edges])
        self.assertTrue(any("issueService.createIssue" in evidence for _target, evidence in implementation_edges))

    def test_object_provider_chain_resolves_interface_call(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "import org.springframework.beans.factory.ObjectProvider;\n"
            "public class IssueController {\n"
            "    private ObjectProvider<IssueService> issueServiceProvider;\n"
            "    public Issue createIssue() {\n"
            "        return issueServiceProvider.getIfAvailable().createIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("IssueServiceImpl.createIssue", [target for target, _evidence in implementation_edges])
        self.assertTrue(any("issueServiceProvider.getIfAvailable().createIssue" in evidence for _target, evidence in implementation_edges))

    def test_spring_aop_edges_are_indexed_as_framework_flow(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        aspect_file = repo_path / "aspect" / "IssueAuditAspect.java"
        aspect_file.parent.mkdir(parents=True)
        aspect_file.write_text(
            "@Aspect\n"
            "@Component\n"
            "public class IssueAuditAspect {\n"
            "    @Pointcut(\"execution(* *..IssueService.createIssue(..))\")\n"
            "    public void issueCreatePointcut() {}\n"
            "    @Around(\"issueCreatePointcut()\")\n"
            "    public Object auditIssueCreate(ProceedingJoinPoint joinPoint) { return joinPoint.proceed(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from entity_edges
                    where from_file = 'aspect/IssueAuditAspect.java'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from flow_edges
                    where from_file = 'aspect/IssueAuditAspect.java'
                    """
                )
            )

        self.assertIn(("framework_binding", "Aspect"), entity_edges)
        self.assertTrue(any(kind == "aop_pointcut" and "IssueService.createIssue" in target for kind, target in entity_edges))
        self.assertTrue(any(kind == "aop_advice" and "issueCreatePointcut" in target for kind, target in entity_edges))
        self.assertTrue(any(kind == "aop_applies_to" and target == "IssueService.createIssue" for kind, target in entity_edges))
        self.assertTrue(any(kind == "aop_applies_to" and target == "IssueServiceImpl.createIssue" for kind, target in entity_edges))
        self.assertTrue(any(kind == "framework" and "issueCreatePointcut" in target for kind, target in flow_edges))
        self.assertTrue(any(kind == "service" and target == "IssueServiceImpl.createIssue" for kind, target in flow_edges))

    def test_scheduled_job_edges_are_indexed_as_framework_flow(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        job_file = repo_path / "job" / "IssueRetryJob.java"
        job_file.parent.mkdir(parents=True)
        job_file.write_text(
            "@Component\n"
            "public class IssueRetryJob {\n"
            "    private IssueService issueService;\n"
            "    @Scheduled(cron = \"0 0/5 * * * ?\")\n"
            "    public void retryIssues() { issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        impl_file = repo_path / "service" / "IssueServiceImpl.java"
        impl_file.write_text(
            "@Service(\"issueServiceImpl\")\n"
            "public class IssueServiceImpl implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from entity_edges
                    where from_file = 'job/IssueRetryJob.java'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from flow_edges
                    where from_file = 'job/IssueRetryJob.java'
                    """
                )
            )

        self.assertTrue(any(kind == "scheduled_job" and "cron=0 0/5 * * * ?" in target for kind, target in entity_edges))
        self.assertTrue(any(kind == "implementation_call" and target == "IssueServiceImpl.createIssue" for kind, target in entity_edges))
        self.assertTrue(any(kind == "framework" and "cron=0 0/5 * * * ?" in target for kind, target in flow_edges))

    def test_handler_interceptor_edges_are_indexed_as_framework_flow(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        interceptor_file = repo_path / "web" / "IssueAuthInterceptor.java"
        interceptor_file.parent.mkdir(parents=True)
        interceptor_file.write_text(
            "public class IssueAuthInterceptor implements HandlerInterceptor {\n"
            "    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {\n"
            "        return true;\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from entity_edges
                    where from_file = 'web/IssueAuthInterceptor.java'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name
                    from flow_edges
                    where from_file = 'web/IssueAuthInterceptor.java'
                    """
                )
            )

        self.assertTrue(any(kind == "web_interceptor" and target in {"IssueAuthInterceptor", "HandlerInterceptor"} for kind, target in entity_edges))
        self.assertTrue(any(kind == "framework" and target in {"IssueAuthInterceptor", "HandlerInterceptor"} for kind, target in flow_edges))

    def test_primary_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        primary_impl = repo_path / "service" / "PrimaryIssueService.java"
        primary_impl.write_text(
            "@Primary\n"
            "@Service(\"primaryIssueService\")\n"
            "public class PrimaryIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        fallback_impl = repo_path / "service" / "FallbackIssueService.java"
        fallback_impl.write_text(
            "@Service(\"fallbackIssueService\")\n"
            "public class FallbackIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("PrimaryIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "FallbackIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("primary=true" in evidence for _target, evidence in implementation_edges))

    def test_active_profile_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.properties").write_text("spring.profiles.active=prod\n", encoding="utf-8")
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        prod_impl = repo_path / "service" / "ProdIssueService.java"
        prod_impl.write_text(
            "@Profile(\"prod\")\n"
            "@Service(\"prodIssueService\")\n"
            "public class ProdIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        dev_impl = repo_path / "service" / "DevIssueService.java"
        dev_impl.write_text(
            "@Profile(\"dev\")\n"
            "@Service(\"devIssueService\")\n"
            "public class DevIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("ProdIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "DevIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("profile=prod" in evidence for _target, evidence in implementation_edges))

    def test_conditional_on_property_filters_interface_implementation_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.properties").write_text("issue.fast.enabled=true\n", encoding="utf-8")
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"true\")\n"
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"false\")\n"
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.fast.enabled=true" in evidence for _target, evidence in implementation_edges))

    def test_profile_specific_config_overrides_conditional_on_property_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.properties").write_text(
            "spring.profiles.active=prod\n"
            "issue.fast.enabled=false\n",
            encoding="utf-8",
        )
        (repo_path / "application-prod.properties").write_text("issue.fast.enabled=true\n", encoding="utf-8")
        (repo_path / "application-dev.properties").write_text("issue.fast.enabled=false\n", encoding="utf-8")
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"true\")\n"
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"false\")\n"
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.fast.enabled=true" in evidence for _target, evidence in implementation_edges))

    def test_yaml_on_profile_document_overrides_conditional_on_property_resolution(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "application.yml").write_text(
            "spring:\n"
            "  profiles:\n"
            "    active: prod\n"
            "issue:\n"
            "  fast:\n"
            "    enabled: false\n"
            "---\n"
            "spring:\n"
            "  config:\n"
            "    activate:\n"
            "      on-profile: prod\n"
            "issue:\n"
            "  fast:\n"
            "    enabled: true\n"
            "---\n"
            "spring:\n"
            "  config:\n"
            "    activate:\n"
            "      on-profile: dev\n"
            "issue:\n"
            "  fast:\n"
            "    enabled: false\n",
            encoding="utf-8",
        )
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        fast_impl = repo_path / "service" / "FastIssueService.java"
        fast_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"true\")\n"
            "@Service(\"fastIssueService\")\n"
            "public class FastIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        slow_impl = repo_path / "service" / "SlowIssueService.java"
        slow_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.fast.enabled\", havingValue = \"false\")\n"
            "@Service(\"slowIssueService\")\n"
            "public class SlowIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("FastIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "SlowIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.fast.enabled=true" in evidence for _target, evidence in implementation_edges))

    def test_conditional_on_property_match_if_missing_selects_default_implementation(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        controller_file = repo_path / "controller" / "IssueController.java"
        controller_file.parent.mkdir(parents=True)
        controller_file.write_text(
            "public class IssueController {\n"
            "    private IssueService issueService;\n"
            "    public Issue createIssue() { return issueService.createIssue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        interface_file = repo_path / "service" / "IssueService.java"
        interface_file.parent.mkdir(parents=True)
        interface_file.write_text("public interface IssueService { Issue createIssue(); }\n", encoding="utf-8")
        default_impl = repo_path / "service" / "DefaultIssueService.java"
        default_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.optional.enabled\", matchIfMissing = true)\n"
            "@Service(\"defaultIssueService\")\n"
            "public class DefaultIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        disabled_impl = repo_path / "service" / "DisabledIssueService.java"
        disabled_impl.write_text(
            "@ConditionalOnProperty(name = \"issue.optional.enabled\", havingValue = \"false\")\n"
            "@Service(\"disabledIssueService\")\n"
            "public class DisabledIssueService implements IssueService {\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            implementation_edges = list(
                connection.execute(
                    """
                    select to_name, evidence
                    from entity_edges
                    where edge_kind = 'implementation_call'
                      and from_file = 'controller/IssueController.java'
                    """
                )
            )

        self.assertIn("DefaultIssueService.createIssue", [target for target, _evidence in implementation_edges])
        self.assertFalse(any(target == "DisabledIssueService.createIssue" for target, _evidence in implementation_edges))
        self.assertTrue(any("condition=issue.optional.enabled=<missing:true>" in evidence for _target, evidence in implementation_edges))

    def test_java_package_imports_create_fully_qualified_symbol_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        service_file = repo_path / "src" / "main" / "java" / "com" / "example" / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "package com.example.service;\n"
            "import com.example.repository.IssueRepository;\n"
            "public class IssueService {\n"
            "    private IssueRepository issueRepository;\n"
            "    public Issue createIssue() {\n"
            "        return issueRepository.findIssue();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        repository_file = repo_path / "src" / "main" / "java" / "com" / "example" / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "package com.example.repository;\n"
            "public class IssueRepository {\n"
            "    public Issue findIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            definitions = list(connection.execute("select name, kind from definitions"))
            edges = list(connection.execute("select edge_kind, to_name, to_file from entity_edges"))
        self.assertIn(("com.example.service.IssueService", "class"), definitions)
        self.assertIn(("com.example.repository.IssueRepository.findIssue", "call", str(repository_file.relative_to(repo_path))), [(to_name, kind, to_file) for kind, to_name, to_file in edges])

    def test_tree_sitter_python_extracts_import_route_and_call(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "api" / "routes.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "from flask import Blueprint\n"
            "from service.issue_service import create_issue\n\n"
            "bp = Blueprint('issue', __name__)\n\n"
            "@bp.route('/api/issues/create', methods=['POST'])\n"
            "def create_issue_route():\n"
            "    return create_issue()\n",
            encoding="utf-8",
        )

        index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "tree_sitter+regex")
        self.assertIn("python", index_info["parser_languages"])
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertTrue(any(kind == "import" and "service.issue_service" in target for kind, target in rows))
        self.assertIn(("route", "/api/issues/create"), rows)
        self.assertTrue(any(kind == "call" and target == "create_issue" for kind, target in rows))

    def test_tree_sitter_typescript_extracts_import_export_and_api_call(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "web" / "issue_client.ts"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "import axios from 'axios';\n"
            "export class IssueClient {\n"
            "  createIssue(payload: unknown) {\n"
            "    return axios.post('/api/issues/create', payload);\n"
            "  }\n"
            "}\n",
            encoding="utf-8",
        )

        index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "tree_sitter+regex")
        self.assertIn("typescript", index_info["parser_languages"])
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            definitions = list(connection.execute("select name, kind from definitions"))
            rows = list(connection.execute("select edge_kind, to_name from entity_edges"))
        self.assertTrue(any(name == "IssueClient" and "class" in kind for name, kind in definitions))
        self.assertTrue(any(kind == "import" and "axios" in target for kind, target in rows))
        self.assertIn(("http_endpoint", "/api/issues/create"), rows)

    def test_tree_sitter_unavailable_falls_back_to_regex_index(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "controller" / "IssueController.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "public class IssueController { public Issue createIssue() { return new Issue(); } }\n",
            encoding="utf-8",
        )

        with patch.object(self.service, "_tree_sitter_parser_for_language", return_value=None):
            index_info = self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        self.assertEqual(index_info["parser_backend"], "regex")
        self.assertEqual(index_info["tree_sitter_files"], 0)
        self.assertGreaterEqual(index_info["definitions"], 1)

    def test_cross_repo_graph_matches_http_path_to_controller_route(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "web" / "issue_client.ts"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "export function createIssue(payload: unknown) {\n"
            "  return fetch('/issue/create', { method: 'POST', body: JSON.stringify(payload) });\n"
            "}\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/issue\")\n"
            "public class IssueController {\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service handles issue create API")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["edge_kind"] == "http_path" and "/issue/create" in edge["match_reason"]
        )

        self.assertEqual(edge["from_repo"], "Portal Repo")
        self.assertEqual(edge["to_repo"], "Issue Service")
        self.assertGreaterEqual(edge["confidence"], 0.95)
        self.assertEqual(edge["to_file"], "controller/IssueController.java")
        self.assertGreater(edge["to_line"], 0)
        self.assertFalse(
            any(
                graph_edge["from_repo"] == "Issue Service"
                and graph_edge["to_repo"] == "Portal Repo"
                and graph_edge["edge_kind"] == "http_path"
                for graph_edge in payload["repo_graph"]["edges"]
            )
        )

    def test_cross_repo_graph_resolves_feign_config_placeholders(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"${issue.service.name}\", url = \"${issue.service.url}\")\n"
            "public interface IssueClient {\n"
            "    @PostMapping(\"/create\")\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        config_file = portal_path / "application.properties"
        config_file.write_text(
            "issue.service.name=issue-service\n"
            "issue.service.url=http://issue-service/issue\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/issue\")\n"
            "public class IssueController {\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service does feign issue client call")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["edge_kind"] == "http_path" and "/issue/create" in edge["match_reason"]
        )

        self.assertEqual(edge["from_repo"], "Portal Repo")
        self.assertEqual(edge["to_repo"], "Issue Service")
        self.assertGreaterEqual(edge["confidence"], 0.95)
        self.assertIn("/issue/create", edge["match_reason"])

    def test_cross_repo_graph_resolves_feign_yaml_config_placeholders(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        client_file = portal_path / "client" / "IssueClient.java"
        client_file.parent.mkdir(parents=True)
        client_file.write_text(
            "@FeignClient(name = \"${issue.service.name}\", url = \"${issue.service.url}\")\n"
            "public interface IssueClient {\n"
            "    @PostMapping(\"/create\")\n"
            "    Issue createIssue();\n"
            "}\n",
            encoding="utf-8",
        )
        config_file = portal_path / "application.yml"
        config_file.write_text(
            "issue:\n"
            "  service:\n"
            "    name: issue-service\n"
            "    url: http://issue-service/issue\n",
            encoding="utf-8",
        )
        service_file = service_path / "controller" / "IssueController.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "@RestController\n"
            "@RequestMapping(\"/issue\")\n"
            "public class IssueController {\n"
            "    @PostMapping(\"/create\")\n"
            "    public Issue createIssue() { return new Issue(); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service does feign issue client call")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["edge_kind"] == "http_path" and "/issue/create" in edge["match_reason"]
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            config_edges = list(connection.execute("select edge_kind, to_name from entity_edges"))

        self.assertEqual(edge["from_repo"], "Portal Repo")
        self.assertEqual(edge["to_repo"], "Issue Service")
        self.assertGreaterEqual(edge["confidence"], 0.95)
        self.assertIn(("config", "issue.service.url"), config_edges)
        self.assertIn(("config_value", "http://issue-service/issue"), config_edges)

    def test_build_files_create_module_dependency_edges_and_repo_graph(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        (portal_path / "pom.xml").write_text(
            "<project>\n"
            "  <groupId>com.example</groupId>\n"
            "  <artifactId>portal-web</artifactId>\n"
            "  <dependencies>\n"
            "    <dependency>\n"
            "      <groupId>com.example</groupId>\n"
            "      <artifactId>issue-service-api</artifactId>\n"
            "      <version>1.0.0</version>\n"
            "    </dependency>\n"
            "  </dependencies>\n"
            "</project>\n",
            encoding="utf-8",
        )
        (service_path / "pom.xml").write_text(
            "<project>\n"
            "  <groupId>com.example</groupId>\n"
            "  <artifactId>issue-service-api</artifactId>\n"
            "</project>\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which repo does portal-web depend on")
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["module_dependency"], "satisfied")
        self.assertIn(payload["matches"][0]["path"], {"pom.xml", "package.json", "settings.gradle", "build.gradle"})
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue Service"
            and edge["edge_kind"] == "module_dependency"
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            flow_edges = list(connection.execute("select edge_kind, to_name, to_file from flow_edges"))

        self.assertGreaterEqual(edge["confidence"], 0.84)
        self.assertIn("exact build artifact match", edge["match_reason"])
        self.assertTrue(
            any(kind == "module_dependency" and target == "com.example:issue-service-api" for kind, target, _to_file in flow_edges)
        )

    def test_gradle_multi_module_edges_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        (repo_path / "settings.gradle").write_text(
            "rootProject.name = 'portal'\n"
            "include ':issue-api', ':issue-service'\n",
            encoding="utf-8",
        )
        (repo_path / "issue-service" / "build.gradle").parent.mkdir(parents=True)
        (repo_path / "issue-service" / "build.gradle").write_text(
            "dependencies {\n"
            "    implementation project(':issue-api')\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, from_file
                    from entity_edges
                    where edge_kind in ('gradle_module', 'gradle_project_dependency', 'module_dependency', 'module_artifact')
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, from_file
                    from flow_edges
                    where edge_kind = 'module_dependency'
                    """
                )
            )

        self.assertIn(("gradle_module", "issue-api", "settings.gradle"), entity_edges)
        self.assertIn(("gradle_module", ":issue-api", "settings.gradle"), entity_edges)
        self.assertIn(("gradle_project_dependency", "issue-api", "issue-service/build.gradle"), entity_edges)
        self.assertIn(("gradle_project_dependency", ":issue-api", "issue-service/build.gradle"), entity_edges)
        self.assertIn(("module_dependency", "issue-api", "issue-service/build.gradle"), entity_edges)
        self.assertTrue(any(kind == "module_dependency" and target == "issue-api" for kind, target, _from_file in flow_edges))

    def test_runtime_trace_jsonl_creates_dynamic_flow_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        trace_path = repo_path / "runtime-traces" / "source-code-qa.jsonl"
        trace_path.parent.mkdir(parents=True)
        trace_path.write_text(
            "\n".join(
                json.dumps(row)
                for row in [
                    {
                        "kind": "call",
                        "from": "IssueController.createIssue",
                        "to": "IssueServiceImpl.createIssue",
                        "evidence": "observed trace id t1",
                    },
                    {"kind": "sql", "from": "IssueServiceImpl.createIssue", "table": "issue_table"},
                    {"kind": "message", "from": "IssueEventPublisher.publish", "topic": "issue.created"},
                    {"kind": "config", "key": "feature.issue.fast.enabled", "value": True},
                    {"kind": "route", "path": "/api/issues/create", "handler": "IssueController.createIssue"},
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, from_file
                    from entity_edges
                    where edge_kind like 'runtime_%'
                    """
                )
            )
            flow_edges = list(
                connection.execute(
                    """
                    select edge_kind, to_name, evidence
                    from flow_edges
                    where evidence like '%observed trace id t1%'
                       or to_name in ('issue_table', 'issue.created', 'feature.issue.fast.enabled', '/api/issues/create')
                    """
                )
            )

        self.assertIn(("runtime_call", "IssueServiceImpl.createIssue", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_sql", "issue_table", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_message", "issue.created", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_config", "feature.issue.fast.enabled", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertIn(("runtime_route", "/api/issues/create", "runtime-traces/source-code-qa.jsonl"), entity_edges)
        self.assertTrue(any(kind == "runtime" and target == "IssueServiceImpl.createIssue" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "db_runtime" and target == "issue_table" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "message_runtime" and target == "issue.created" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "config" and target == "feature.issue.fast.enabled" for kind, target, _ in flow_edges))
        self.assertTrue(any(kind == "route" and target == "/api/issues/create" for kind, target, _ in flow_edges))

    def test_package_json_dependencies_create_module_dependency_repo_graph_edges(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue SDK", "url": "https://git.example.com/team/issue-sdk.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        sdk_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        sdk_path = self.service._repo_path("AF:All", sdk_entry)
        (portal_path / ".git").mkdir(parents=True)
        (sdk_path / ".git").mkdir(parents=True)
        (portal_path / "package.json").write_text(
            json.dumps(
                {
                    "name": "@example/portal-web",
                    "dependencies": {"@example/issue-sdk": "^1.2.3"},
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (portal_path / "pom.xml").write_text(
            "<project><artifactId>portal-web</artifactId><dependencies>"
            "<dependency><groupId>com.example</groupId><artifactId>unrelated-service-api</artifactId></dependency>"
            "</dependencies></project>\n",
            encoding="utf-8",
        )
        (sdk_path / "package.json").write_text(
            json.dumps({"name": "@example/issue-sdk"}, indent=2),
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which npm package does portal-web depend on")
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["module_dependency"], "satisfied")
        self.assertEqual(payload["matches"][0]["path"], "package.json")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue SDK"
            and edge["edge_kind"] == "module_dependency"
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            flow_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))

        self.assertGreaterEqual(edge["confidence"], 0.84)
        self.assertIn(("module_dependency", "@example/issue-sdk"), flow_edges)

    def test_messaging_publish_consume_edges_create_cross_repo_graph(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        publisher_file = portal_path / "events" / "IssueEventPublisher.java"
        publisher_file.parent.mkdir(parents=True)
        publisher_file.write_text(
            "public class IssueEventPublisher {\n"
            "    public void publish(IssueCreatedEvent event) {\n"
            "        kafkaTemplate.send(\"issue.created\", event);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        listener_file = service_path / "events" / "IssueEventListener.java"
        listener_file.parent.mkdir(parents=True)
        listener_file.write_text(
            "public class IssueEventListener {\n"
            "    @KafkaListener(topics = \"issue.created\")\n"
            "    public void consume(IssueCreatedEvent event) { }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service consumes issue created event")
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["message_flow"], "satisfied")
        self.assertIn(payload["matches"][0]["path"], {"events/IssueEventPublisher.java", "events/IssueEventListener.java"})
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue Service"
            and edge["edge_kind"] == "message_topic"
        )
        with sqlite3.connect(self.service._index_path(portal_path)) as connection:
            publish_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))
        with sqlite3.connect(self.service._index_path(service_path)) as connection:
            consume_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))

        self.assertGreaterEqual(edge["confidence"], 0.93)
        self.assertIn(("message_publish", "issue.created"), publish_edges)
        self.assertIn(("message_consume", "issue.created"), consume_edges)

    def test_configured_message_topic_placeholders_create_cross_repo_graph(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        service_entry = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        service_path = self.service._repo_path("AF:All", service_entry)
        (portal_path / ".git").mkdir(parents=True)
        (service_path / ".git").mkdir(parents=True)
        publisher_file = portal_path / "events" / "IssueEventPublisher.java"
        publisher_file.parent.mkdir(parents=True)
        publisher_file.write_text(
            "public class IssueEventPublisher {\n"
            "    public void publish(IssueCreatedEvent event) {\n"
            "        kafkaTemplate.send(\"${issue.topic.name}\", event);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        (portal_path / "application.properties").write_text("issue.topic.name=issue.created\n", encoding="utf-8")
        listener_file = service_path / "events" / "IssueEventListener.java"
        listener_file.parent.mkdir(parents=True)
        listener_file.write_text(
            "public class IssueEventListener {\n"
            "    @KafkaListener(topics = \"${issue.topic.name}\")\n"
            "    public void consume(IssueCreatedEvent event) { }\n"
            "}\n",
            encoding="utf-8",
        )
        (service_path / "application.properties").write_text("issue.topic.name=issue.created\n", encoding="utf-8")
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which service consumes configured issue topic")
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["message_flow"], "satisfied")
        self.assertIn(payload["matches"][0]["path"], {"events/IssueEventPublisher.java", "events/IssueEventListener.java", "application.properties"})
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Issue Service"
            and edge["edge_kind"] == "message_topic"
        )

        self.assertGreaterEqual(edge["confidence"], 0.93)
        self.assertIn("issue.created", edge["match_reason"])

    def test_exact_build_artifact_matching_links_repo_without_display_alias(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
                {"display_name": "Payments Core", "url": "https://git.example.com/team/payments-core.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        portal = type("Entry", (), entries[0])()
        target = type("Entry", (), entries[1])()
        portal_path = self.service._repo_path("AF:All", portal)
        target_path = self.service._repo_path("AF:All", target)
        (portal_path / ".git").mkdir(parents=True)
        (target_path / ".git").mkdir(parents=True)
        (portal_path / "pom.xml").write_text(
            "<project><dependencies><dependency>"
            "<groupId>com.example</groupId><artifactId>fraud-ledger-api</artifactId><version>1.0.0</version>"
            "</dependency></dependencies></project>",
            encoding="utf-8",
        )
        (target_path / "pom.xml").write_text(
            "<project><groupId>com.example</groupId><artifactId>fraud-ledger-api</artifactId></project>",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which repo provides fraud-ledger-api dependency")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Portal Repo"
            and edge["to_repo"] == "Payments Core"
            and edge["edge_kind"] == "module_dependency"
        )

        self.assertGreaterEqual(edge["confidence"], 0.97)
        self.assertIn("exact build artifact match", edge["match_reason"])

    def test_shared_table_write_read_creates_cross_repo_lineage_edge(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[
                {"display_name": "Issue Writer", "url": "https://git.example.com/team/issue-writer.git"},
                {"display_name": "Issue Reporting", "url": "https://git.example.com/team/issue-reporting.git"},
            ],
        )
        entries = self.service.load_config()["mappings"]["AF:All"]
        writer = type("Entry", (), entries[0])()
        reader = type("Entry", (), entries[1])()
        writer_path = self.service._repo_path("AF:All", writer)
        reader_path = self.service._repo_path("AF:All", reader)
        (writer_path / ".git").mkdir(parents=True)
        (reader_path / ".git").mkdir(parents=True)
        writer_file = writer_path / "repository" / "IssueWriterRepository.java"
        writer_file.parent.mkdir(parents=True)
        writer_file.write_text(
            "public class IssueWriterRepository {\n"
            "    public void saveIssue() { jdbcTemplate.update(\"insert into shared_issue_table(id) values (?)\", 1); }\n"
            "}\n",
            encoding="utf-8",
        )
        reader_file = reader_path / "repository" / "IssueReportRepository.java"
        reader_file.parent.mkdir(parents=True)
        reader_file.write_text(
            "public class IssueReportRepository {\n"
            "    public Issue loadIssue() { return jdbcTemplate.queryForObject(\"select * from shared_issue_table\", mapper); }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="which repo reads shared_issue_table after it is written")
        edge = next(
            edge
            for edge in payload["repo_graph"]["edges"]
            if edge["from_repo"] == "Issue Writer"
            and edge["to_repo"] == "Issue Reporting"
            and edge["edge_kind"] == "shared_table"
        )

        self.assertGreaterEqual(edge["confidence"], 0.86)
        self.assertIn("db write/read table overlap", edge["match_reason"])

    def test_sql_read_write_and_spring_condition_edges_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "@Profile(\"prod\")\n"
            "public class IssueRepository {\n"
            "    @Value(\"${issue.topic.name:issue.created}\")\n"
            "    private String topicName;\n"
            "    @Qualifier(\"primaryJdbcTemplate\")\n"
            "    private JdbcTemplate jdbcTemplate;\n"
            "    public Issue loadIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "    public void saveIssue() {\n"
            "        jdbcTemplate.update(\"insert into issue_table(id) values (?)\", 1);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)

        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = list(connection.execute("select edge_kind, to_name from entity_edges"))
            flow_edges = list(connection.execute("select edge_kind, to_name from flow_edges"))

        self.assertIn(("spring_profile", "prod"), entity_edges)
        self.assertIn(("config", "issue.topic.name"), entity_edges)
        self.assertIn(("bean_qualifier", "primaryJdbcTemplate"), entity_edges)
        self.assertIn(("db_read", "issue_table"), flow_edges)
        self.assertIn(("db_write", "issue_table"), flow_edges)

    def test_followup_context_augments_short_question(self):
        question, context = self.service._apply_conversation_context(
            "继续找这个表哪里写入",
            {
                "question": "what table does issue creation use",
                "matches": [{"path": "repository/IssueRepository.java", "snippet": "select * from issue_table"}],
                "trace_paths": [{"edges": [{"to_name": "issue_table", "to_file": ""}]}],
                "structured_answer": {"claims": [{"text": "IssueRepository reads issue_table"}]},
            },
        )

        self.assertTrue(context["used"])
        self.assertIn("issue_table", question)

    def test_domain_profile_terms_can_boost_configured_data_source_names(self):
        profile_path = Path(self.temp_dir.name) / "profiles.json"
        profile_path.write_text(
            '{'
            '"default": {"source_terms": ["LedgerGateway"], "data_carriers": ["LedgerDTO"]},'
            '"AF": {"source_terms": ["FraudLedgerRepository"]}'
            '}',
            encoding="utf-8",
        )
        self.service.domain_profile_path = profile_path
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "FraudLedgerRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class FraudLedgerRepository {\n"
            "    public LedgerDTO loadLedger() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from fraud_ledger\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="what data source is used for ledger")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["matches"][0]["path"], "repository/FraudLedgerRepository.java")

    def test_retrieval_prefers_symbol_definition_over_noisy_readme(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        readme = repo_path / "README.md"
        readme.write_text(
            "batchCreateJiraIssue batchCreateJiraIssue batchCreateJiraIssue\n"
            "This repo mentions batchCreateJiraIssue many times in docs.\n",
            encoding="utf-8",
        )

        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        payload = {'route': '/api/v1/issues/batchCreateJiraIssue'}\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue', json=payload)\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="where is batchCreateJiraIssue implemented")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["matches"][0]["path"], "bpmis/jira_client.py")
        self.assertTrue(
            "symbol matched" in payload["matches"][0]["reason"]
            or "planner_definition" in payload["matches"][0]["reason"]
            or "structure matched" in payload["matches"][0]["reason"]
        )

    def test_dependency_question_expands_to_service_and_integration_matches(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "decision" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    private ScreeningService screeningService;\n"
            "    private EcInfoIntegration ecInfoIntegration;\n"
            "    public void runPreCheck1() {\n"
            "        screeningService.screenApplicant();\n"
            "        ecInfoIntegration.fetchEcInfo();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        service_file = repo_path / "service" / "ScreeningService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class ScreeningService {\n"
            "    public void screenApplicant() {\n"
            "        // data screening source logic\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        integration_file = repo_path / "integration" / "EcInfoIntegration.java"
        integration_file.parent.mkdir(parents=True)
        integration_file.write_text(
            "public class EcInfoIntegration {\n"
            "    public void fetchEcInfo() {\n"
            "        // upstream ecinfo source\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="What data sources and integrations are checked for Term Loan Pre Check 1",
        )

        self.assertEqual(payload["status"], "ok")
        paths = [match["path"] for match in payload["matches"]]
        self.assertIn("decision/TermLoanPreCheckEngine.java", paths)
        self.assertIn("service/ScreeningService.java", paths)
        self.assertIn("integration/EcInfoIntegration.java", paths)
        dependency_reasons = [match["reason"] for match in payload["matches"] if match.get("trace_stage") == "dependency"]
        self.assertTrue(any("dependency trace" in reason for reason in dependency_reasons))

    def test_two_hop_trace_follows_downstream_service_from_entry_point(self):
        self.service.save_mapping(
            pm_team="CRMS",
            country="ID",
            repositories=[{"display_name": "Credit Risk", "url": "https://git.example.com/team/credit-risk.git"}],
        )
        entry = self.service.load_config()["mappings"]["CRMS:ID"][0]
        repo_path = self.service._repo_path("CRMS:ID", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "underwriting" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    private EcInfoService ecInfoService;\n"
            "    public void runPreCheck1() {\n"
            "        ecInfoService.loadEcInfo();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        service_file = repo_path / "service" / "EcInfoService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class EcInfoService {\n"
            "    public CustomerEcInfo loadEcInfo() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from cr_ec_info\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="CRMS",
            country="ID",
            question="What data sources are checked for Term Loan Pre Check 1",
        )

        paths = [match["path"] for match in payload["matches"]]
        self.assertIn("underwriting/TermLoanPreCheckEngine.java", paths)
        self.assertIn("service/EcInfoService.java", paths)
        two_hop_reasons = [match["reason"] for match in payload["matches"] if match.get("trace_stage") == "two_hop"]
        self.assertTrue(any("two-hop trace" in reason for reason in two_hop_reasons))

    def test_agent_trace_can_follow_service_to_repository(self):
        self.service.save_mapping(
            pm_team="CRMS",
            country="ID",
            repositories=[{"display_name": "Credit Risk", "url": "https://git.example.com/team/credit-risk.git"}],
        )
        entry = self.service.load_config()["mappings"]["CRMS:ID"][0]
        repo_path = self.service._repo_path("CRMS:ID", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "engine" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    private EligibilityService eligibilityService;\n"
            "    public void runPreCheck1() {\n"
            "        eligibilityService.evaluateEligibility();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        service_file = repo_path / "service" / "EligibilityService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class EligibilityService {\n"
            "    private CustomerRepository customerRepository;\n"
            "    public CustomerProfile evaluateEligibility() {\n"
            "        return customerRepository.findCustomerProfile();\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        repository_file = repo_path / "repository" / "CustomerRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class CustomerRepository {\n"
            "    public CustomerProfile findCustomerProfile() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        base_matches = self.service._search_repo(
            type("Entry", (), entry)(),
            repo_path,
            self.service._question_tokens("Term Loan Pre Check 1"),
            question="What data source does Term Loan Pre Check 1 check?",
        )
        agent_matches = self.service._expand_agent_trace_matches(
            entries=[type("Entry", (), entry)()],
            key="CRMS:ID",
            question="What data source does Term Loan Pre Check 1 check?",
            base_matches=base_matches,
            limit=12,
        )

        paths = [match["path"] for match in agent_matches]
        self.assertIn("repository/CustomerRepository.java", paths)
        agent_reasons = [match["reason"] for match in agent_matches if str(match.get("trace_stage") or "").startswith("agent_trace")]
        self.assertTrue(any("agent trace" in reason for reason in agent_reasons))

    def test_evidence_compressor_extracts_concrete_data_sources(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "repository/CustomerRepository.java",
                "line_start": 10,
                "line_end": 14,
                "score": 100,
                "trace_stage": "agent_trace_2",
                "reason": "agent trace",
                "snippet": (
                    "public CustomerProfile loadProfile() {\n"
                    "    return jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper);\n"
                    "}\n"
                ),
            }
        ]

        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        self.assertTrue(compressed["data_sources"])
        self.assertIn("cr_customer_profile", " ".join(compressed["data_sources"]))
        self.assertEqual(quality["status"], "sufficient")
        self.assertIn("production:2", compressed["data_source_tiers"])

    def test_test_only_data_source_requires_production_evidence(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "src/test/java/repository/CustomerRepositoryTest.java",
                "line_start": 10,
                "line_end": 14,
                "score": 100,
                "trace_stage": "direct",
                "reason": "test fixture matched",
                "snippet": (
                    "public void loadsProfile() {\n"
                    "    jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper);\n"
                    "}\n"
                ),
            }
        ]

        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)
        pack = self.service._build_evidence_pack(
            question="What data source does Term Loan Pre Check 1 check?",
            evidence_summary=compressed,
            matches=matches,
            trace_paths=[],
            quality_gate=quality,
        )

        self.assertIn("test:2", compressed["data_source_tiers"])
        self.assertTrue(compressed["source_conflicts"])
        self.assertEqual(quality["status"], "needs_more_trace")
        self.assertIn("production repository/mapper/client/table evidence beyond test/docs/generated files", quality["missing"])
        self.assertTrue(pack["source_conflicts"])
        self.assertIn("Production source evidence was not found", " ".join(pack["missing_hops"]))

    def test_evidence_pack_structures_tables_and_missing_hops(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "repository/CustomerRepository.java",
                "line_start": 10,
                "line_end": 14,
                "score": 100,
                "trace_stage": "agent_trace_2",
                "reason": "agent trace",
                "snippet": (
                    "public CustomerProfile loadProfile() {\n"
                    "    return jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper);\n"
                    "}\n"
                ),
            }
        ]
        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        pack = self.service._build_evidence_pack(
            question="What data source does Term Loan Pre Check 1 check?",
            evidence_summary=compressed,
            matches=matches,
            trace_paths=[],
            quality_gate=quality,
        )

        self.assertEqual(pack["version"], 2)
        self.assertTrue(any(item["type"] == "table" for item in pack["items"]))
        self.assertIn("cr_customer_profile", " ".join(pack["tables"]))
        self.assertTrue(pack["read_write_points"])
        self.assertFalse(pack["missing_hops"])

    def test_planner_tool_dsl_finds_tables_routes_and_callees(self):
        _build_fixture_repositories(self.service)
        af_entry = self.service.load_config()["mappings"]["AF:All"][0]
        entry = type("Entry", (), af_entry)()
        repo_path = self.service._repo_path("AF:All", entry)
        self.service._build_repo_index("AF:All", entry, repo_path)

        table_matches = self.service._tool_find_tables(
            entry,
            repo_path,
            ["issue"],
            "what table does issue creation use",
            1,
        )
        route_matches = self.service._tool_find_api_routes(
            entry,
            repo_path,
            ["issue"],
            "which API handles issue create",
            1,
        )
        base_matches = self.service._search_repo(
            entry,
            repo_path,
            self.service._question_tokens("IssueController createIssue"),
            question="which service or repository does issue creation call",
        )
        callee_matches = self.service._tool_find_callees(
            entry,
            repo_path,
            base_matches,
            ["Issue"],
            "which service or repository does issue creation call",
            1,
        )

        self.assertIn("issue_table", " ".join(match["snippet"] for match in table_matches))
        self.assertTrue(any(match.get("retrieval") == "planner_api_route" for match in route_matches))
        self.assertTrue(any(match.get("retrieval") == "planner_callee" for match in callee_matches))

    def test_quality_gate_requests_deeper_trace_when_data_source_is_missing(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "engine/TermLoanPreCheckEngine.java",
                "line_start": 1,
                "line_end": 5,
                "score": 80,
                "trace_stage": "direct",
                "reason": "content matched",
                "snippet": (
                    "public class TermLoanPreCheckEngine {\n"
                    "    public void run(EngineTermLoanPreCheckLayer1Input input) {}\n"
                    "}\n"
                ),
            }
        ]

        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)
        trace_terms = self.service._quality_gate_trace_terms(
            "What data source does Term Loan Pre Check 1 check?",
            compressed,
            quality,
            matches,
        )

        self.assertEqual(quality["status"], "needs_more_trace")
        self.assertIn("concrete upstream source/table/API/repository evidence beyond DTO fields", quality["missing"])
        self.assertIn("repository", trace_terms)

    def test_quality_gate_trace_can_find_repository_after_weak_entry_match(self):
        self.service.save_mapping(
            pm_team="CRMS",
            country="ID",
            repositories=[{"display_name": "Credit Risk", "url": "https://git.example.com/team/credit-risk.git"}],
        )
        entry = self.service.load_config()["mappings"]["CRMS:ID"][0]
        repo_path = self.service._repo_path("CRMS:ID", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)

        engine_file = repo_path / "engine" / "TermLoanPreCheckEngine.java"
        engine_file.parent.mkdir(parents=True)
        engine_file.write_text(
            "public class TermLoanPreCheckEngine {\n"
            "    public void run(EngineTermLoanPreCheckLayer1Input input) {}\n"
            "}\n",
            encoding="utf-8",
        )

        repository_file = repo_path / "repository" / "DataSourceResultRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class DataSourceResultRepository {\n"
            "    public DataSourceResult loadDataSourceResult() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from cr_data_source_result\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        base_matches = self.service._search_repo(
            type("Entry", (), entry)(),
            repo_path,
            self.service._question_tokens("Term Loan Pre Check 1"),
            question="What data source does Term Loan Pre Check 1 check?",
        )
        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", base_matches[:1])
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)
        quality_matches = self.service._expand_quality_gate_matches(
            entries=[type("Entry", (), entry)()],
            key="CRMS:ID",
            question="What data source does Term Loan Pre Check 1 check?",
            base_matches=base_matches[:1],
            evidence_summary=compressed,
            quality_gate=quality,
            limit=12,
        )

        self.assertIn("repository/DataSourceResultRepository.java", [match["path"] for match in quality_matches])
        self.assertTrue(any(match.get("trace_stage") == "quality_gate" for match in quality_matches))

    def test_agent_plan_includes_data_source_trace_steps(self):
        compressed = {
            "intent": self.service._question_intent("What data source does Term Loan Pre Check 1 check?"),
            "entry_points": ["Credit Risk:engine/TermLoanPreCheckEngine.java:1-5"],
            "data_carriers": ["EngineTermLoanPreCheckLayer1Input"],
            "field_population": [],
            "downstream_components": [],
            "data_sources": [],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 1,
        }
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        plan = self.service._build_agent_plan("What data source does Term Loan Pre Check 1 check?", compressed, quality)

        step_names = [step["name"] for step in plan["steps"]]
        self.assertIn("trace_entry_to_source_lineage", step_names)
        self.assertIn("trace_data_carriers", step_names)
        self.assertIn("trace_field_population", step_names)
        self.assertIn("trace_downstream_sources", step_names)
        self.assertIn("trace_dao_mapper_methods", step_names)
        self.assertIn("fill_quality_gap", step_names)
        flattened_terms = " ".join(term for step in plan["steps"] for term in step["terms"])
        self.assertIn("EngineTermLoanPreCheckLayer1Input", flattened_terms)
        self.assertNotIn("UnderwritingInitiationDTO", flattened_terms)

    def test_answer_self_check_retries_weak_data_source_answer(self):
        compressed = {
            "intent": self.service._question_intent("What data source does Term Loan Pre Check 1 check?"),
            "entry_points": ["Credit Risk:engine/TermLoanPreCheckEngine.java:1-5"],
            "data_carriers": ["DataSourceResult"],
            "field_population": ["UnderwritingInitiationProvider populates UnderwritingInitiationDTO"],
            "downstream_components": ["DataSourceResultRepository"],
            "data_sources": ["Credit Risk:repository/DataSourceResultRepository.java:10-14: select * from cr_data_source_result"],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 3,
        }
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        check = self.service._answer_self_check(
            "What data source does Term Loan Pre Check 1 check?",
            "The evidence does not specify the actual data source.",
            compressed,
            quality,
        )

        self.assertEqual(check["status"], "retry")
        self.assertTrue(check["issues"])

    def test_answer_self_check_rejects_dto_only_data_source_answer(self):
        compressed = {
            "intent": self.service._question_intent("What data source does Term Loan Pre Check 1 check?"),
            "entry_points": ["Credit Risk:engine/TermLoanPreCheckEngine.java:1-5"],
            "data_carriers": ["UnderwritingInitiationDTO", "CustomerInfo", "LoanInfo", "CreditRiskInfo"],
            "field_population": ["UnderwritingInitiationProvider populates UnderwritingInitiationDTO"],
            "downstream_components": ["DataSourceResultRepository"],
            "data_sources": ["Credit Risk:repository/DataSourceResultRepository.java:10-14: select * from cr_data_source_result"],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 4,
        }
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        check = self.service._answer_self_check(
            "What data source does Term Loan Pre Check 1 check?",
            "Term Loan Precheck 1 uses data from UnderwritingInitiationDTO, CustomerInfo, LoanInfo, and CreditRiskInfo.",
            compressed,
            quality,
        )

        self.assertEqual(check["status"], "retry")
        self.assertIn("answer stops at DTO/carrier layer instead of tracing upstream source", check["issues"])

    def test_answer_judge_requests_repair_for_uncited_thin_answer(self):
        evidence_pack = {
            "version": 2,
            "intent": self.service._question_intent("What data source does issue creation use?"),
            "items": [
                {
                    "type": "table",
                    "claim": "issue_table is referenced in Portal Repo:repository/IssueRepository.java:1-5",
                    "source_id": "S1",
                    "confidence": "high",
                    "hop": "source",
                    "supports_answer": True,
                }
            ],
            "tables": ["issue_table (Portal Repo:repository/IssueRepository.java:1-5)"],
            "apis": [],
            "missing_hops": [],
        }

        judge = self.service._judge_answer(
            "What data source does issue creation use?",
            "It uses a repository.",
            evidence_pack,
            {"status": "needs_citation", "issues": ["concrete answer claims need citation-backed evidence"]},
        )

        self.assertEqual(judge["status"], "repair")
        self.assertTrue(judge["repair_targets"])

    def test_llm_answer_judge_can_request_repair(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            judge_model="judge-lite",
            llm_judge_enabled=True,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        evidence_pack = {
            "version": 2,
            "intent": service._question_intent("What data source does issue creation use?"),
            "items": [
                {
                    "type": "table",
                    "claim": "issue_table is referenced in Portal Repo:repository/IssueRepository.java:1-5",
                    "source_id": "S1",
                    "confidence": "high",
                    "supports_answer": True,
                }
            ],
            "tables": ["issue_table (Portal Repo:repository/IssueRepository.java:1-5)"],
            "apis": [],
            "missing_hops": [],
        }
        result = LLMGenerateResult(
            payload={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "status": "repair",
                                            "confidence": "high",
                                            "issues": ["answer omits issue_table"],
                                            "repair_targets": ["include table source"],
                                        }
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 8},
            },
            usage={"promptTokenCount": 10, "candidatesTokenCount": 8},
            model="judge-lite",
            attempts=1,
        )

        with patch.object(service.llm_provider, "generate", return_value=result) as mocked_generate:
            judge = service._run_answer_judge(
                "What data source does issue creation use?",
                "It uses a repository.",
                evidence_pack,
                {"status": "ok", "issues": []},
            )

        self.assertEqual(judge["mode"], "llm_evidence_judge")
        self.assertEqual(judge["status"], "repair")
        self.assertEqual(judge["model"], "judge-lite")
        self.assertIn("answer omits issue_table", judge["issues"])
        self.assertEqual(mocked_generate.call_args.kwargs["primary_model"], "judge-lite")

    def test_llm_answer_judge_is_cached(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            judge_model="gemini-2.5-flash-lite",
            llm_judge_enabled=True,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        evidence_pack = {
            "version": 2,
            "intent": service._question_intent("What data source does issue creation use?"),
            "items": [
                {
                    "type": "table",
                    "claim": "issue_table is referenced in Portal Repo:repository/IssueRepository.java:1-5",
                    "source_id": "S1",
                    "confidence": "high",
                    "supports_answer": True,
                }
            ],
            "tables": ["issue_table (Portal Repo:repository/IssueRepository.java:1-5)"],
            "apis": [],
            "missing_hops": [],
        }
        result = LLMGenerateResult(
            payload={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "status": "ok",
                                            "confidence": "high",
                                            "issues": [],
                                            "repair_targets": [],
                                        }
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 8},
            },
            usage={"promptTokenCount": 10, "candidatesTokenCount": 8},
            model="gemini-2.5-flash-lite",
            attempts=1,
        )

        with patch.object(service.llm_provider, "generate", return_value=result) as mocked_generate:
            first = service._run_answer_judge(
                "What data source does issue creation use?",
                "IssueRepository reads issue_table [S1].",
                evidence_pack,
                {"status": "ok", "issues": []},
            )
            second = service._run_answer_judge(
                "What data source does issue creation use?",
                "IssueRepository reads issue_table [S1].",
                evidence_pack,
                {"status": "ok", "issues": []},
            )

        self.assertEqual(first["mode"], "llm_evidence_judge")
        self.assertEqual(second["mode"], "llm_evidence_judge")
        self.assertTrue(second["cached"])
        self.assertEqual(mocked_generate.call_count, 1)

    def test_llm_finalizer_blocks_dto_only_source_answer_when_source_missing(self):
        compressed = {
            "intent": self.service._question_intent("What data sources does Term Loan Precheck 1 underwriting call?"),
            "entry_points": ["Credit Risk:engine/Layer4TermLoanPreCheckEngineStrategy.java:1-8"],
            "data_carriers": ["DataSourceResult (Credit Risk:engine/Layer4TermLoanPreCheckEngineStrategy.java:1-8)"],
            "field_population": ["Credit Risk:engine/Layer4TermLoanPreCheckEngineStrategy.java:1-8: input.setDataSourceResult(result)"],
            "downstream_components": [],
            "data_sources": [],
            "api_or_config": [],
            "rule_or_error_logic": [],
            "source_count": 2,
        }
        quality = self.service._quality_gate("What data sources does Term Loan Precheck 1 underwriting call?", compressed)
        final = self.service._finalize_llm_answer(
            question="What data sources does Term Loan Precheck 1 underwriting call?",
            answer="It likely uses internal or external financial providers through integrations.",
            structured_answer=self.service._parse_structured_answer("It likely uses internal or external financial providers through integrations."),
            evidence_summary=compressed,
            quality_gate=quality,
            claim_check={"status": "needs_citation", "issues": ["concrete answer claims need citation-backed evidence"]},
            selected_matches=[
                {
                    "repo": "Credit Risk",
                    "path": "engine/Layer4TermLoanPreCheckEngineStrategy.java",
                    "line_start": 1,
                    "line_end": 8,
                }
            ],
        )

        self.assertEqual(final["answer_contract"]["status"], "blocked_missing_source")
        self.assertNotIn("likely", final["answer"].lower())
        self.assertIn("I cannot confirm the final upstream data source", final["answer"])
        self.assertIn("Missing link", final["answer"])

    def test_imported_dao_is_not_treated_as_concrete_data_source(self):
        matches = [
            {
                "repo": "Credit Risk",
                "path": "service/UnderwritingInitiationService.java",
                "line_start": 1,
                "line_end": 6,
                "score": 100,
                "trace_stage": "agent_plan_3",
                "reason": "agent plan trace",
                "snippet": (
                    "import com.shopee.banking.crm.dao.CustomerInfoDAO;\n"
                    "public class UnderwritingInitiationService {\n"
                    "    private CustomerInfoDAO customerInfoDAO;\n"
                    "}\n"
                ),
            }
        ]

        compressed = self.service._compress_evidence("What data source does Term Loan Pre Check 1 check?", matches)
        quality = self.service._quality_gate("What data source does Term Loan Pre Check 1 check?", compressed)

        self.assertFalse(compressed["data_sources"])
        self.assertEqual(quality["status"], "needs_more_trace")
        self.assertEqual(quality["policies"][0]["name"], "data_source")
        self.assertEqual(quality["policies"][0]["status"], "missing")

    def test_field_level_data_flow_edges_are_indexed(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        service_file = repo_path / "service" / "IssueService.java"
        service_file.parent.mkdir(parents=True)
        service_file.write_text(
            "public class IssueService {\n"
            "    public EngineInput buildInput(DataSourceResult result) {\n"
            "        EngineInput input = new EngineInput();\n"
            "        input.setDataSourceResult(result);\n"
            "        return input;\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )

        self.service._build_repo_index("AF:All", type("Entry", (), entry)(), repo_path)
        with sqlite3.connect(self.service._index_path(repo_path)) as connection:
            entity_edges = {row[0] for row in connection.execute("select edge_kind from entity_edges")}
            flow_edges = {row[0] for row in connection.execute("select edge_kind from flow_edges")}

        self.assertIn("data_flow", entity_edges)
        self.assertIn("field_population", flow_edges)

    def test_static_qa_query_finds_security_and_quality_findings(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        risky_file = repo_path / "service" / "RiskyIssueService.java"
        risky_file.parent.mkdir(parents=True)
        risky_file.write_text(
            "public class RiskyIssueService {\n"
            "    private static final String PASSWORD = \"plain-secret\";\n"
            "    public void load(String issueId) {\n"
            "        String sql = \"select * from issue_table where id = \" + issueId;\n"
            "        try { callRemote(); } catch (Exception e) { e.printStackTrace(); }\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        payload = self.service.query(pm_team="AF", country="All", question="what static QA security risks exist")

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["matches"][0]["path"], "service/RiskyIssueService.java")
        self.assertTrue(any(match.get("retrieval") == "static_qa" for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("hardcoded_secret", evidence_text)
        self.assertIn("sql_string_concatenation", evidence_text)
        self.assertEqual(payload["answer_quality"]["policies"][0]["name"], "static_qa")
        self.assertEqual(payload["answer_quality"]["policies"][0]["status"], "satisfied")
        self.assertIn("static QA findings", payload["summary"])

    def test_impact_analysis_query_finds_upstream_and_downstream_surfaces(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="what is impacted if IssueService createIssue changes",
        )

        paths = {match["path"] for match in payload["matches"]}
        self.assertIn("controller/IssueController.java", paths)
        self.assertIn("repository/IssueRepository.java", paths)
        self.assertTrue(any(match.get("retrieval") in {"planner_caller", "planner_callee", "flow_graph", "entity_graph"} for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("impact_surfaces", payload["evidence_pack"])
        self.assertIn("IssueRepository", evidence_text)
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["impact_analysis"], "satisfied")

    def test_test_coverage_query_finds_tests_and_assertions(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="is IssueService createIssue covered by tests",
        )

        self.assertEqual(payload["matches"][0]["path"], "src/test/java/IssueServiceTest.java")
        paths = {match["path"] for match in payload["matches"]}
        self.assertIn("src/test/java/IssueServiceTest.java", paths)
        self.assertTrue(any(match.get("retrieval") == "test_coverage" for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("test_coverage", payload["evidence_pack"])
        self.assertIn("IssueServiceTest", evidence_text)
        self.assertTrue("verify" in evidence_text or "assert" in evidence_text)
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["test_coverage"], "satisfied")

    def test_operational_boundary_query_finds_transaction_and_cache_annotations(self):
        _build_fixture_repositories(self.service)
        self._build_all_indexes()

        payload = self.service.query(
            pm_team="AF",
            country="All",
            question="is IssueService createIssue transactional or cached",
        )

        self.assertEqual(payload["matches"][0]["path"], "service/IssueService.java")
        paths = {match["path"] for match in payload["matches"]}
        self.assertIn("service/IssueService.java", paths)
        self.assertTrue(any(match.get("retrieval") == "operational_boundary" for match in payload["matches"]))
        evidence_text = json.dumps(payload["evidence_pack"], ensure_ascii=False)
        self.assertIn("operational_boundaries", payload["evidence_pack"])
        self.assertIn("Transactional", evidence_text)
        self.assertIn("Cacheable", evidence_text)
        policy_statuses = {policy["name"]: policy["status"] for policy in payload["answer_quality"]["policies"]}
        self.assertEqual(policy_statuses["operational_boundary"], "satisfied")

    def test_claim_verifier_flags_uncited_concrete_claims(self):
        evidence_summary = {
            "data_sources": ["Portal Repo:repository/IssueRepository.java:3-5: select * from issue_table"],
            "api_or_config": [],
        }
        selected_matches = [
            {
                "repo": "Portal Repo",
                "path": "repository/IssueRepository.java",
                "line_start": 1,
                "line_end": 5,
            }
        ]

        check = self.service._verify_answer_claims(
            "Issue creation reads from issue_table through IssueRepository.",
            evidence_summary,
            selected_matches,
        )

        self.assertEqual(check["status"], "needs_citation")
        self.assertTrue(check["unsupported_claims"])

    def test_claim_verifier_requires_citation_to_support_claim_terms(self):
        evidence_summary = {
            "data_sources": ["Portal Repo:repository/IssueRepository.java:3-5: select * from issue_table"],
            "api_or_config": [],
        }
        selected_matches = [
            {
                "repo": "Portal Repo",
                "path": "controller/IssueController.java",
                "line_start": 1,
                "line_end": 5,
                "snippet": "public class IssueController { public void createIssue() {} }",
            }
        ]

        check = self.service._verify_answer_claims(
            '{"direct_answer":"Uses issue_table","claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],"missing_evidence":[],"confidence":"high"}',
            evidence_summary,
            selected_matches,
        )

        self.assertEqual(check["status"], "needs_citation")
        self.assertIn("IssueRepository reads issue_table", check["unsupported_claims"][0])

    def test_claim_verifier_accepts_structured_supported_citation(self):
        evidence_summary = {
            "data_sources": ["Portal Repo:repository/IssueRepository.java:3-5: select * from issue_table"],
            "api_or_config": [],
        }
        selected_matches = [
            {
                "repo": "Portal Repo",
                "path": "repository/IssueRepository.java",
                "line_start": 1,
                "line_end": 5,
                "snippet": "class IssueRepository { void read(){ jdbcTemplate.queryForObject(\"select * from issue_table\", mapper); } }",
            }
        ]

        check = self.service._verify_answer_claims(
            '{"direct_answer":"Uses issue_table","claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],"missing_evidence":[],"confidence":"high"}',
            evidence_summary,
            selected_matches,
        )

        self.assertEqual(check["status"], "ok")

    def test_structured_answer_parser_accepts_json_and_fallback_prose(self):
        parsed = self.service._parse_structured_answer(
            '{"direct_answer":"Uses issue_table","confirmed_from_code":["IssueRepository reads issue_table [S1]"],'
            '"investigation_steps":{"candidate_evidence":["Opened IssueRepository"],"gap_verification":["Checked mapper"],"certainty_split":["Confirmed table only"]},'
            '"inferred_from_code":["IssueService calls the repository"],"not_found":["No caller evidence"],'
            '"claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],"missing_evidence":[],"confidence":"high"}'
        )
        fallback = self.service._parse_structured_answer("IssueRepository reads issue_table [S1].")

        self.assertEqual(parsed["format"], "json")
        self.assertEqual(parsed["claims"][0]["citations"], ["S1"])
        self.assertEqual(parsed["confirmed_from_code"], ["IssueRepository reads issue_table [S1]"])
        self.assertEqual(parsed["investigation_steps"]["gap_verification"], ["Checked mapper"])
        self.assertEqual(parsed["inferred_from_code"], ["IssueService calls the repository"])
        self.assertEqual(parsed["not_found"], ["No caller evidence"])
        self.assertEqual(fallback["format"], "prose_fallback")
        self.assertEqual(fallback["claims"][0]["citations"], ["S1"])
        self.assertEqual(fallback["confirmed_from_code"], [])

    def test_finalizer_preserves_structured_json_for_display(self):
        structured = {
            "direct_answer": "Issue creation reads issue_table.",
            "confirmed_from_code": ["IssueRepository reads issue_table [S1]"],
            "inferred_from_code": [],
            "not_found": [],
            "claims": [{"text": "IssueRepository reads issue_table", "citations": ["S1"]}],
            "missing_evidence": ["No service caller evidence."],
            "confidence": "medium",
            "format": "json",
        }

        final = self.service._finalize_llm_answer(
            question="what table does IssueRepository use",
            answer=json.dumps(structured),
            structured_answer=structured,
            evidence_summary={
                "intent": self.service._question_intent("what table does IssueRepository use"),
                "data_sources": ["Portal Repo:repository/IssueRepository.java:1-5: issue_table"],
            },
            quality_gate={"status": "sufficient", "confidence": "medium", "missing": []},
            claim_check={"status": "ok", "issues": []},
            selected_matches=[
                {
                    "repo": "Portal Repo",
                    "path": "repository/IssueRepository.java",
                    "line_start": 1,
                    "line_end": 5,
                    "snippet": "jdbcTemplate.queryForObject(\"select * from issue_table\", mapper)",
                }
            ],
        )

        self.assertEqual(final["structured_answer"]["format"], "json")
        self.assertEqual(final["structured_answer"]["direct_answer"], "Issue creation reads issue_table.")
        self.assertEqual(final["structured_answer"]["claims"][0]["text"], "IssueRepository reads issue_table")
        self.assertNotIn("Missing evidence:", final["structured_answer"]["direct_answer"])

    def test_trusted_model_finalizer_keeps_explicit_missing_evidence(self):
        structured = {
            "direct_answer": "Only the status migration is confirmed; the full rule expression is not present.",
            "investigation_steps": {
                "candidate_evidence": ["Opened migration SQL"],
                "gap_verification": ["Searched for rule_config_tab inserts"],
                "certainty_split": ["Confirmed status update; full expression missing"],
            },
            "confirmed_from_code": ["Migration updates C0204v2 status [S1]"],
            "inferred_from_code": ["Rule likely maps to the v2 challenge rule family"],
            "not_found": ["No full rule_config_tab row or feature_expr for C0204v2 was found"],
            "claims": [{"text": "Migration updates C0204v2 status", "citations": ["S1"]}],
            "missing_evidence": ["Live rule_config_tab row is required to confirm feature_expr"],
            "confidence": "medium",
            "format": "json",
        }

        final = self.service._finalize_trusted_model_answer(
            question="why are C0204v2 and C0205v2 both needed",
            answer=json.dumps(structured),
            structured_answer=structured,
            evidence_summary={"intent": self.service._question_intent("why are C0204v2 and C0205v2 both needed")},
            quality_gate={"status": "sufficient", "confidence": "medium", "missing": []},
            claim_check={"status": "skipped", "issues": []},
            selected_matches=[{"repo": "AF", "path": "dml.sql", "line_start": 1, "line_end": 2, "snippet": "update rule_config_tab"}],
        )

        self.assertIn("No full rule_config_tab row", final["answer"])
        self.assertIn("Live rule_config_tab row is required to confirm feature_expr", final["answer_contract"]["missing_links"])
        self.assertEqual(final["answer_contract"]["investigation_steps"]["gap_verification"], ["Searched for rule_config_tab inserts"])

    def test_eval_runner_checks_trace_paths_and_structured_claims(self):
        self.service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = self.service.load_config()["mappings"]["AF:All"][0]
        repo_path = self.service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes()

        result = _evaluate_case(
            self.service,
            {
                "id": "trace-path-eval",
                "pm_team": "AF",
                "country": "All",
                "question": "what table does IssueRepository use",
                "expected_paths": ["repository/IssueRepository.java"],
                "min_trace_paths": 1,
                "expected_trace_path_terms": ["issue_table"],
                "expected_answer_policy_statuses": {"data_source": "satisfied"},
                "expected_evidence_pack_terms": ["issue_table"],
                "category": "data_source",
            },
        )

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["category"], "data_source")
        self.assertEqual(result["answer_policies"]["data_source"], "satisfied")

    def test_gemini_query_returns_answer_and_usage(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )

        fake_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"Short answer: batchCreateJiraIssue is in BPMISClient.",'
                                        '"claims":[{"text":"BPMISClient defines batchCreateJiraIssue","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 123, "candidatesTokenCount": 45},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "gemini_flash")
        self.assertIn("Short answer", payload["llm_answer"])
        self.assertEqual(payload["llm_usage"]["promptTokenCount"], 123)
        self.assertEqual(payload["llm_provider"], "gemini")
        self.assertEqual(payload["llm_model"], "gemini-2.5-flash")
        self.assertFalse(payload["llm_cached"])
        request_payload = mocked_post.call_args_list[0].kwargs["json"]
        self.assertEqual(request_payload["generationConfig"]["responseMimeType"], "application/json")
        self.assertIn("responseSchema", request_payload["generationConfig"])
        self.assertEqual(request_payload["generationConfig"]["thinkingConfig"]["thinkingBudget"], 512)
        self.assertIn(payload["llm_thinking_budget"], {512, 2048})
        self.assertEqual(payload["llm_route"]["mode"], "manual")
        telemetry = json.loads(service.telemetry_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertEqual(telemetry["llm_provider"], "gemini")
        self.assertEqual(telemetry["llm_model"], "gemini-2.5-flash")
        self.assertIn(telemetry["llm_thinking_budget"], {512, 2048})
        self.assertEqual(telemetry["llm_route"]["mode"], "manual")
        self.assertEqual(telemetry["versions"]["cache"], 14)
        self.assertIn("llm_latency_ms", telemetry)
        self.assertIn("llm_attempt_log", telemetry)
        self.assertIn("answer_contract", telemetry)
        self.assertIn("evidence_pack_summary", telemetry)
        self.assertIn("answer_judge", telemetry)
        self.assertEqual(payload["evidence_pack"]["version"], 2)
        self.assertIn("answer_judge", payload)

    def test_gemini_max_tokens_uses_compact_repair_retry(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        capped_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [{"content": {"parts": [{"text": "{"}]}, "finishReason": "MAX_TOKENS"}],
                "usageMetadata": {"promptTokenCount": 20000, "candidatesTokenCount": 312, "totalTokenCount": 20312},
            },
            text='{"ok":true}',
        )
        repaired_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"BPMISClient defines batchCreateJiraIssue.",'
                                        '"claims":[{"text":"BPMISClient defines batchCreateJiraIssue","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        },
                        "finishReason": "STOP",
                    }
                ],
                "usageMetadata": {"promptTokenCount": 4000, "candidatesTokenCount": 140, "totalTokenCount": 4140},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", side_effect=[capped_response, repaired_response]) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(mocked_post.call_count, 2)
        first_request = mocked_post.call_args_list[0].kwargs["json"]
        retry_request = mocked_post.call_args_list[1].kwargs["json"]
        self.assertEqual(first_request["generationConfig"]["thinkingConfig"]["thinkingBudget"], 512)
        self.assertEqual(retry_request["generationConfig"]["thinkingConfig"]["thinkingBudget"], 0)
        self.assertEqual(retry_request["generationConfig"]["maxOutputTokens"], 2400)
        self.assertLess(
            len(retry_request["contents"][0]["parts"][0]["text"]),
            len(first_request["contents"][0]["parts"][0]["text"]) + 1,
        )
        self.assertIn("BPMISClient defines batchCreateJiraIssue", payload["llm_answer"])
        self.assertEqual(payload["llm_finish_reason"], "STOP")
        self.assertNotEqual(payload["answer_contract"]["status"], "unreliable_llm_answer")
        self.assertEqual(payload["llm_thinking_budget"], 0)

    def test_simple_function_usage_no_hit_still_calls_llm(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Risk Config", "url": "https://git.example.com/team/risk-config.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        config_file = repo_path / "anti-fraud-service" / "apollo.properties"
        config_file.parent.mkdir(parents=True)
        config_file.write_text(
            "dbp.antifraud.function.F44={\"functionName\":\"Compare\",\"field\":\"fdMaturityDate\"}\n",
            encoding="utf-8",
        )
        spec_file = repo_path / "spec" / "fields.md"
        spec_file.parent.mkdir(parents=True)
        spec_file.write_text("fdMaturityDate is exposed as a rule form field.\n", encoding="utf-8")
        self._build_all_indexes(service)

        fake_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"fdMaturityDate only appears in config/spec candidates.",'
                                        '"claims":[{"text":"fdMaturityDate appears in config/spec evidence","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"medium"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 123, "candidatesTokenCount": 45},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            payload = service.query(
                pm_team="AF",
                country="All",
                question="Is fdMaturityDate used in any function?",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "gemini_flash")
        self.assertEqual(payload["llm_provider"], "gemini")
        self.assertIn("fdMaturityDate only appears", payload["llm_answer"])
        self.assertEqual(mocked_post.call_count, 1)

    def test_simple_function_usage_with_method_body_still_calls_llm(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Risk Service", "url": "https://git.example.com/team/risk-service.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "src" / "main" / "java" / "LoanChecker.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "public class LoanChecker {\n"
            "    public boolean validate() {\n"
            "        return fdMaturityDate != null;\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        self._build_all_indexes(service)
        fake_response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"fdMaturityDate is used in LoanChecker.validate.",'
                                        '"claims":[{"text":"LoanChecker.validate checks fdMaturityDate","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 90, "candidatesTokenCount": 20},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            payload = service.query(
                pm_team="AF",
                country="All",
                question="Is fdMaturityDate used in any function?",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "gemini_flash")
        self.assertNotIn("llm_cost_skip", payload)
        self.assertIn("LoanChecker.validate", payload["llm_answer"])
        mocked_post.assert_called()

    def test_crms_ph_card_income_extraction_question_prioritizes_storage_tables(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="CRMS",
            country="PH",
            repositories=[{"display_name": "credit-risk", "url": "https://git.example.com/team/credit-risk.git"}],
        )
        entry = service.load_config()["mappings"]["CRMS:PH"][0]
        repo_path = service._repo_path("CRMS:PH", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        extract_mapper = repo_path / "credit-risk-infra" / "src" / "main" / "resources" / "mapper" / "ExtractRecordDAO-ext.xml"
        extract_mapper.parent.mkdir(parents=True)
        extract_mapper.write_text(
            "<mapper>\n"
            "  <update id=\"updateByBizIdAndSceneSelectiveCas\">\n"
            "    update extract_record_tab set response_body = #{updateDO.responseBody} where biz_id = #{bizId}\n"
            "  </update>\n"
            "</mapper>\n",
            encoding="utf-8",
        )
        status_mapper = repo_path / "credit-risk-infra" / "src" / "main" / "resources" / "mapper" / "CardIncomeScreeningFlowStatusDAO-ext.xml"
        status_mapper.parent.mkdir(parents=True, exist_ok=True)
        status_mapper.write_text(
            "<mapper>\n"
            "  <update id=\"updateAllByVersionAndStatusCas\">\n"
            "    update card_income_screening_flow_status_tab set process_info = #{updateDO.processInfo} where underwriting_id = #{underwritingId}\n"
            "  </update>\n"
            "</mapper>\n",
            encoding="utf-8",
        )
        strategy_file = repo_path / "credit-risk-infra" / "src" / "main" / "java" / "PayslipProcessStrategy.java"
        strategy_file.parent.mkdir(parents=True)
        strategy_file.write_text(
            "class PayslipProcessStrategy {\n"
            "  void process(IesResultDTO result, CardIncomeScreenProcessInfo info) {\n"
            "    info.getPayslip().setGrossPay(result.getResult().getGrossPay());\n"
            "  }\n"
            "}\n",
            encoding="utf-8",
        )
        noise_file = repo_path / "credit-risk-experian" / "src" / "main" / "java" / "ExperianEvaluator.java"
        noise_file.parent.mkdir(parents=True)
        noise_file.write_text("class ExperianEvaluator { void query() {} }\n", encoding="utf-8")
        self._build_all_indexes(service)

        payload = service.query(
            pm_team="CRMS",
            country="PH",
            question="Which table stores the Credit Card LLM extracted payslip fields? And which table stores the Ops extracted fields?",
            answer_mode="retrieval_only",
        )

        paths = [match["path"] for match in payload["matches"][:8]]
        self.assertTrue(any("ExtractRecordDAO-ext.xml" in path for path in paths), paths)
        self.assertTrue(any("CardIncomeScreeningFlowStatusDAO-ext.xml" in path for path in paths), paths)
        self.assertIn("extract_record_tab", payload["exact_lookup"]["matched_terms"])
        self.assertIn("card_income_screening_flow_status_tab", payload["exact_lookup"]["matched_terms"])

    def test_malformed_or_capped_llm_answer_is_downgraded(self):
        question = "Which table stores the Credit Card LLM extracted payslip fields?"
        raw_answer = '{ "direct_answer": "The provided evidence does not specify the table"'
        structured = self.service._parse_structured_answer(raw_answer)

        final = self.service._finalize_llm_answer(
            question=question,
            answer=raw_answer,
            structured_answer=structured,
            evidence_summary={
                "intent": self.service._question_intent(question),
                "data_sources": ["credit-risk:mapper/ExtractRecordDAO-ext.xml:1-3: update extract_record_tab set response_body = ..."],
            },
            quality_gate={"status": "sufficient", "confidence": "high", "missing": []},
            claim_check={"status": "needs_citation", "issues": ["concrete answer claims need citation-backed evidence"]},
            selected_matches=[
                {
                    "repo": "credit-risk",
                    "path": "mapper/ExtractRecordDAO-ext.xml",
                    "line_start": 1,
                    "line_end": 3,
                    "snippet": "update extract_record_tab set response_body = #{updateDO.responseBody}",
                }
            ],
            answer_judge={"status": "repair", "issues": ["answer omits typed table/API/client source evidence"]},
            finish_reason="MAX_TOKENS",
        )

        self.assertFalse(final["answer"].lstrip().startswith("{"))
        self.assertIn("could not produce a reliable final answer", final["answer"])
        self.assertEqual(final["answer_contract"]["status"], "unreliable_llm_answer")
        self.assertEqual(final["answer_contract"]["confidence"], "low")
        self.assertEqual(final["structured_answer"]["confidence"], "low")

    def test_llm_answer_cache_requires_current_versions(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        cache_key = service._answer_cache_key(
            provider="gemini",
            model="gemini-2.5-flash",
            question="where is createIssue",
            answer_mode="gemini_flash",
            llm_budget_mode="balanced",
            context="S1 createIssue",
        )
        service._store_cached_answer(
            cache_key,
            answer="createIssue is in IssueController [S1]",
            usage={"totalTokenCount": 12},
            answer_quality={"status": "sufficient"},
            provider="gemini",
            model="gemini-2.5-flash",
        )
        cached = service._load_cached_answer(cache_key)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["versions"]["cache"], 14)
        cache_path = service.answer_cache_root / f"{cache_key}.json"
        stale_payload = json.loads(cache_path.read_text(encoding="utf-8"))
        stale_payload["versions"]["router"] = -1
        cache_path.write_text(json.dumps(stale_payload), encoding="utf-8")
        self.assertIsNone(service._load_cached_answer(cache_key))

    def test_openai_compatible_provider_returns_answer(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="openai_compatible",
            openai_api_key="openai-key",
            openai_model="code-balanced",
            openai_fallback_model="code-lite",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"direct_answer":"batchCreateJiraIssue is in BPMISClient.",'
                                '"claims":[{"text":"BPMISClient defines batchCreateJiraIssue","citations":["S1"]}],'
                                '"missing_evidence":[],"confidence":"high"}'
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 50, "completion_tokens": 20, "total_tokens": 70},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["llm_provider"], "openai_compatible")
        self.assertEqual(payload["llm_model"], "code-balanced")
        self.assertEqual(payload["llm_usage"]["total_tokens"], 70)
        self.assertIn("batchCreateJiraIssue", payload["llm_answer"])
        self.assertIn("/chat/completions", mocked_post.call_args.args[0])
        request_payload = mocked_post.call_args.kwargs["json"]
        self.assertEqual(request_payload["model"], "code-balanced")
        self.assertEqual(request_payload["response_format"], {"type": "json_object"})
        self.assertEqual(request_payload["messages"][0]["role"], "system")

    def test_vertex_ai_provider_returns_answer_with_service_account_token(self):
        credentials_path = Path(self.temp_dir.name) / "vertex-service-account.json"
        credentials_path.write_text(json.dumps({"project_id": "demo-project"}), encoding="utf-8")
        provider = VertexAISourceCodeQALLMProvider(
            credentials_file=str(credentials_path),
            location="us-central1",
            timeout_seconds=45,
            max_retries=0,
        )

        class FakeCredentials:
            token = ""

            def refresh(self, request):
                self.token = "ya29.vertex-token"

        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"Vertex found the answer.",'
                                        '"claims":[{"text":"Vertex found the answer","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 11, "candidatesTokenCount": 7, "totalTokenCount": 18},
            },
            text='{"ok":true}',
        )

        with patch(
            "bpmis_jira_tool.source_code_qa.service_account.Credentials.from_service_account_file",
            return_value=FakeCredentials(),
        ) as mocked_credentials, patch(
            "bpmis_jira_tool.source_code_qa.requests.post",
            return_value=fake_response,
        ) as mocked_post:
            result = provider.generate(
                payload={"contents": [{"parts": [{"text": "Question: where is createIssue"}]}]},
                primary_model="gemini-2.5-flash",
                fallback_model="gemini-2.5-flash-lite",
            )

        self.assertEqual(result.model, "gemini-2.5-flash")
        self.assertEqual(result.usage["totalTokenCount"], 18)
        self.assertIn("Vertex found the answer", provider.extract_text(result.payload))
        mocked_credentials.assert_called_once_with(
            str(credentials_path),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.assertIn(
            "https://us-central1-aiplatform.googleapis.com/v1/projects/demo-project/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent",
            mocked_post.call_args.args[0],
        )
        self.assertEqual(mocked_post.call_args.kwargs["headers"]["Authorization"], "Bearer ya29.vertex-token")
        self.assertEqual(mocked_post.call_args.kwargs["timeout"], 45)
        self.assertEqual(mocked_post.call_args.kwargs["json"]["contents"][0]["role"], "user")

    def test_vertex_ai_embedding_provider_uses_task_type_and_predict_endpoint(self):
        credentials_path = Path(self.temp_dir.name) / "vertex-service-account.json"
        credentials_path.write_text(json.dumps({"project_id": "demo-project"}), encoding="utf-8")
        provider = VertexAIEmbeddingProvider(
            credentials_file=str(credentials_path),
            location="us-central1",
            model="gemini-embedding-001",
            output_dimensionality=768,
        )

        class FakeCredentials:
            token = ""

            def refresh(self, request):
                self.token = "ya29.vertex-token"

        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {"predictions": [{"embeddings": {"values": [0.1, 0.2, 0.3]}}]},
            text='{"ok":true}',
        )

        with patch(
            "bpmis_jira_tool.source_code_qa.service_account.Credentials.from_service_account_file",
            return_value=FakeCredentials(),
        ), patch(
            "bpmis_jira_tool.source_code_qa.requests.post",
            return_value=fake_response,
        ) as mocked_post:
            rows = provider.embed_texts(["find createIssue"], task_type="CODE_RETRIEVAL_QUERY")

        self.assertEqual(rows, [[0.1, 0.2, 0.3]])
        self.assertIn(
            "https://us-central1-aiplatform.googleapis.com/v1/projects/demo-project/locations/us-central1/publishers/google/models/gemini-embedding-001:predict",
            mocked_post.call_args.args[0],
        )
        request_payload = mocked_post.call_args.kwargs["json"]
        self.assertEqual(request_payload["instances"][0]["task_type"], "CODE_RETRIEVAL_QUERY")
        self.assertEqual(request_payload["parameters"]["outputDimensionality"], 768)
        self.assertEqual(mocked_post.call_args.kwargs["headers"]["Authorization"], "Bearer ya29.vertex-token")

    def test_vertex_ai_gemini_3_router_uses_preview_models_and_thinking_level(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="vertex_ai",
            vertex_model="gemini-3.1-pro-preview",
            vertex_fast_model="gemini-3-flash-preview",
            vertex_deep_model="gemini-3.1-pro-preview",
            vertex_fallback_model="gemini-3.1-pro-preview",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

        self.assertEqual(service.llm_budgets["cheap"]["model"], "gemini-3-flash-preview")
        self.assertEqual(service.llm_budgets["balanced"]["model"], "gemini-3.1-pro-preview")
        self.assertEqual(service.llm_budgets["deep"]["model"], "gemini-3.1-pro-preview")
        self.assertGreaterEqual(service.llm_budgets["balanced"]["match_limit"], 18)
        self.assertGreaterEqual(service.llm_budgets["balanced"]["snippet_char_budget"], 48_000)
        self.assertGreaterEqual(service.llm_budgets["balanced"]["max_output_tokens"], 2_800)
        self.assertGreaterEqual(service.llm_budgets["deep"]["max_output_tokens"], 4_800)
        self.assertEqual(service._llm_fallback_model(), "gemini-3.1-pro-preview")
        routed_mode, routed_budget, route = service._resolve_llm_budget("cheap", "where is createIssue", [])
        self.assertEqual(routed_mode, "balanced")
        self.assertEqual(routed_budget["model"], "gemini-3.1-pro-preview")
        self.assertEqual(route["reason"], "vertex_quality_floor")
        auto_mode, _auto_budget, auto_route = service._resolve_llm_budget(
            "auto",
            "why does createIssue fail and what is the root cause",
            [{"trace_stage": "direct", "retrieval": "file_scan", "score": 20}],
        )
        self.assertEqual(auto_mode, "deep")
        self.assertIn("root_cause_or_cross_repo", auto_route["reason"])
        self.assertEqual(service._llm_prompt_pressure_for_provider(50_000), "normal")
        self.assertEqual(service._llm_prompt_pressure_for_provider(80_000), "compact")
        embedding_service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="vertex_ai",
            embedding_provider="vertex_ai",
            semantic_index_model="local-token-hybrid-v1",
            vertex_credentials_file="/tmp/missing.json",
            vertex_project_id="demo-project",
            vertex_location="us-central1",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        self.assertEqual(embedding_service.embedding_provider.public_config()["provider"], "vertex_ai")
        self.assertEqual(embedding_service.embedding_provider.public_config()["model"], "gemini-embedding-001")
        second_pass_terms = service._vertex_second_pass_terms(
            question="what data source does createIssue use",
            draft_answer="It mentions CreateIssueRequest but misses IssueRepository.",
            evidence_summary={"intent": {"data_source": True}, "data_sources": ["IssueRepository reads issue_table [S1]"]},
            quality_gate={"missing": ["repository mapper evidence"]},
            answer_check={"issues": ["answer lacks repository source marker"]},
            claim_check={"unsupported_claims": ["CreateIssueRequest is the final source"]},
            answer_judge={"repair_targets": ["include table evidence"]},
            matches=[{"path": "repository/IssueRepository.java", "reason": "semantic chunk matched"}],
        )
        self.assertIn("issuerepository", second_pass_terms)
        self.assertIn("mapper", second_pass_terms)
        self.assertEqual(
            service._thinking_config_for_provider(
                512,
                model="gemini-3-flash-preview",
                role="answer",
                budget_mode="cheap",
            ),
            {"thinkingLevel": "MEDIUM"},
        )
        self.assertEqual(
            service._thinking_config_for_provider(
                2048,
                model="gemini-3.1-pro-preview",
                role="answer",
                budget_mode="deep",
            ),
            {"thinkingLevel": "HIGH"},
        )
        self.assertEqual(
            service._thinking_config_for_provider(
                0,
                model="gemini-3.1-pro-preview",
                role="judge",
                budget_mode="cheap",
            ),
            {"thinkingLevel": "HIGH"},
        )

    def test_vertex_ai_answer_uses_draft_retrieval_then_structured_final(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="vertex_ai",
            vertex_credentials_file="/tmp/vertex-ready.json",
            vertex_project_id="demo-project",
            vertex_location="global",
            vertex_model="gemini-3.1-pro-preview",
            vertex_fast_model="gemini-3-flash-preview",
            vertex_deep_model="gemini-3.1-pro-preview",
            vertex_fallback_model="gemini-3.1-pro-preview",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        match = {
            "repo": "Credit Risk",
            "path": "src/CreditReviewCbsServiceImpl.java",
            "line_start": 10,
            "line_end": 20,
            "trace_stage": "direct",
            "retrieval": "file_scan",
            "reason": "symbol match",
            "score": 90,
            "snippet": "class CreditReviewCbsServiceImpl { void retrieve() { cbsClient.queryReport(); } }",
        }
        draft_result = LLMGenerateResult(
            payload={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {"text": "Draft: CreditReviewCbsServiceImpl calls cbsClient.queryReport [S1]."}
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 4, "totalTokenCount": 14},
            },
            usage={"promptTokenCount": 10, "candidatesTokenCount": 4, "totalTokenCount": 14},
            model="gemini-3.1-pro-preview",
            attempts=1,
            latency_ms=100,
            attempt_log=({"model": "gemini-3.1-pro-preview", "status": "ok"},),
        )
        final_result = LLMGenerateResult(
            payload={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "direct_answer": "The monthly credit review retrieves the CBS report through CreditReviewCbsServiceImpl calling cbsClient.queryReport [S1].",
                                            "claims": [
                                                {
                                                    "text": "CreditReviewCbsServiceImpl calls cbsClient.queryReport",
                                                    "citations": ["S1"],
                                                }
                                            ],
                                            "missing_evidence": [],
                                            "confidence": "high",
                                        }
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 20, "candidatesTokenCount": 8, "totalTokenCount": 28},
            },
            usage={"promptTokenCount": 20, "candidatesTokenCount": 8, "totalTokenCount": 28},
            model="gemini-3.1-pro-preview",
            attempts=1,
            latency_ms=120,
            attempt_log=({"model": "gemini-3.1-pro-preview", "status": "ok"},),
        )

        with patch.object(service.llm_provider, "ready", return_value=True), patch.object(
            service.llm_provider,
            "generate",
            side_effect=[draft_result, final_result],
        ) as mocked_generate, patch.object(
            service,
            "_expand_answer_retry_matches",
            return_value=[match],
        ) as mocked_second_pass:
            payload = service._build_llm_answer(
                entries=[],
                key="CRMS:SG",
                pm_team="CRMS",
                country="SG",
                question="When will system retrieve CBS report for monthly credit review?",
                matches=[match],
                llm_budget_mode="balanced",
                requested_answer_mode="gemini_flash",
                request_cache={},
            )

        self.assertEqual(mocked_generate.call_count, 2)
        first_payload = mocked_generate.call_args_list[0].kwargs["payload"]
        second_payload = mocked_generate.call_args_list[1].kwargs["payload"]
        self.assertNotIn("responseSchema", first_payload["generationConfig"])
        self.assertNotIn("responseMimeType", first_payload["generationConfig"])
        self.assertIn("responseSchema", second_payload["generationConfig"])
        self.assertEqual(second_payload["generationConfig"]["responseMimeType"], "application/json")
        self.assertIn("First-pass task for Vertex AI", first_payload["contents"][0]["parts"][0]["text"])
        self.assertIn("Vertex first-pass draft", second_payload["contents"][0]["parts"][0]["text"])
        self.assertTrue(mocked_second_pass.called)
        self.assertTrue(payload["llm_route"]["vertex_two_pass"])
        self.assertTrue(payload["llm_route"]["vertex_second_pass"])
        self.assertEqual(payload["llm_usage"]["total_tokens"], 42)
        self.assertIn("CreditReviewCbsServiceImpl", payload["llm_answer"])
        context = service._build_compressed_llm_context(
            {"intent": {}, "entry_points": ["PortalController.createIssue [S1]"]},
            {"status": "sufficient", "confidence": "high", "missing": []},
            {"version": 2, "items": [{"type": "entry_point", "confidence": "high", "hop": "direct", "source_id": "S1", "claim": "PortalController calls BPMIS"}]},
            [
                {
                    "repo": "Portal",
                    "path": "src/PortalController.java",
                    "line_start": 10,
                    "line_end": 20,
                    "trace_stage": "direct",
                    "retrieval": "file_scan",
                    "reason": "symbol match",
                    "score": 10,
                    "snippet": "class PortalController { void createIssue() { bpmis.batchCreateJiraIssue(); } }",
                }
            ],
            snippet_line_budget=20,
            snippet_char_budget=5_000,
        )
        self.assertLess(context.index("Primary raw code evidence:"), context.index("Evidence pack v2"))
        self.assertIn("Use these snippets as the source of truth", context)

    def test_codex_cli_bridge_provider_returns_answer(self):
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=Path(self.temp_dir.name),
            timeout_seconds=20,
            codex_binary="codex",
        )
        calls = []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text(
                '{"direct_answer":"Codex found the answer.","claims":[{"text":"Codex found the answer","citations":["S1"]}],"missing_evidence":[],"confidence":"high"}',
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        payload = {
            "systemInstruction": {"parts": [{"text": "system"}]},
            "contents": [{"parts": [{"text": "Question: where is createIssue"}]}],
        }
        image_path = Path(self.temp_dir.name) / "screenshot.png"
        image_path.write_bytes(b"fake-png")
        payload["_codex_image_paths"] = [str(image_path)]
        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_run,
        ):
            result = provider.generate(payload=payload, primary_model="codex-cli", fallback_model="codex-cli")

        self.assertIn("Codex found the answer", provider.extract_text(result.payload))
        exec_command = calls[-1][0]
        self.assertIn("--sandbox", exec_command)
        self.assertIn("read-only", exec_command)
        self.assertIn("--ephemeral", exec_command)
        self.assertIn("--json", exec_command)
        self.assertIn("--skip-git-repo-check", exec_command)
        self.assertIn("--image", exec_command)
        self.assertIn(str(image_path), exec_command)
        self.assertEqual(result.model, "codex-cli")
        self.assertEqual(result.attempt_log[0]["exit_code"], 0)
        self.assertEqual(result.attempt_log[0]["workspace_root"], self.temp_dir.name)
        self.assertEqual(result.attempt_log[0]["concurrency_limit"], 1)
        self.assertIn("queue_wait_ms", result.attempt_log[0])
        self.assertIn("--sandbox", result.attempt_log[0]["command"])

    def test_codex_cli_bridge_adds_rg_directory_to_exec_path(self):
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=Path(self.temp_dir.name),
            timeout_seconds=20,
            codex_binary="codex",
        )
        exec_envs = []

        def fake_run(command, **kwargs):
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            exec_envs.append(kwargs.get("env") or {})
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text(
                '{"direct_answer":"ok","claims":[],"missing_evidence":[],"confidence":"high"}',
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/opt/tools/rg"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_run,
        ):
            provider.generate(payload={"contents": [{"parts": [{"text": "hi"}]}]}, primary_model="codex-cli", fallback_model="codex-cli")

        self.assertTrue(exec_envs)
        self.assertTrue(str(exec_envs[-1].get("PATH") or "").startswith("/opt/tools"))

    def test_codex_cli_bridge_resume_mode_uses_resume_command(self):
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=Path(self.temp_dir.name),
            timeout_seconds=20,
            codex_binary="codex",
            session_mode="resume",
        )
        calls = []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            output_path = command[command.index("--output-last-message") + 1]
            Path(output_path).write_text(
                '{"direct_answer":"ok","claims":[],"missing_evidence":[],"confidence":"high"}',
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout='{"session_id":"session-123","type":"done"}\n', stderr="")

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_run,
        ):
            result = provider.generate(
                payload={"contents": [{"parts": [{"text": "hi"}]}], "codex_cli_session_id": "session-123"},
                primary_model="codex-cli",
                fallback_model="codex-cli",
            )

        exec_command = calls[-1][0]
        self.assertIn("resume", exec_command)
        self.assertIn("session-123", exec_command)
        self.assertNotIn("--ephemeral", exec_command)
        self.assertEqual(result.attempt_log[0]["session_mode"], "resume")
        self.assertEqual(result.payload["codex_cli_trace"]["session_id"], "session-123")

    def test_codex_cli_bridge_streams_json_progress_events(self):
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=Path(self.temp_dir.name),
            timeout_seconds=20,
            codex_binary="codex",
        )
        streamed = []

        class FakePipe:
            def __init__(self, lines):
                self.lines = list(lines)
                self.exhausted = False

            def readline(self):
                if self.lines:
                    return self.lines.pop(0)
                self.exhausted = True
                return ""

            def close(self):
                self.exhausted = True

        class FakeStdin:
            def write(self, _value):
                return None

            def close(self):
                return None

        class FakeProcess:
            def __init__(self, *_args, **_kwargs):
                self.stdout = FakePipe(['{"message":"Reading src/App.java"}\n', '{"type":"done"}\n'])
                self.stderr = FakePipe([])
                self.stdin = FakeStdin()
                self.returncode = 0

            def poll(self):
                if self.stdout.exhausted and self.stderr.exhausted:
                    return self.returncode
                return None

            def kill(self):
                self.returncode = -9
                self.stdout.exhausted = True
                self.stderr.exhausted = True

        def fake_run(command, **_kwargs):
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            raise AssertionError("streaming path should use Popen for codex exec")

        def progress(stage, message, _current, _total):
            streamed.append((stage, message))

        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as output_file:
            output_file.write('{"direct_answer":"Final answer","claims":[],"missing_evidence":[],"confidence":"high"}')
            output_path = output_file.name

        def fake_named_temp(*_args, **_kwargs):
            return open(output_path, "r+", encoding="utf-8")

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_run,
        ), patch("bpmis_jira_tool.source_code_qa.subprocess.Popen", side_effect=FakeProcess), patch(
            "bpmis_jira_tool.source_code_qa.tempfile.NamedTemporaryFile",
            side_effect=fake_named_temp,
        ):
            try:
                result = provider.generate(
                    payload={"contents": [{"parts": [{"text": "hi"}]}], "_progress_callback": progress},
                    primary_model="codex-cli",
                    fallback_model="codex-cli",
                )
            finally:
                Path(output_path).unlink(missing_ok=True)

        self.assertIn(("codex_stream", "Reading src/App.java"), streamed)
        self.assertIn("Final answer", provider.extract_text(result.payload))

    def test_codex_cli_provider_uses_configured_model(self):
        with patch.dict(os.environ, {"SOURCE_CODE_QA_CODEX_MODEL": "gpt-5.5"}, clear=False):
            service = SourceCodeQAService(
                data_root=Path(self.temp_dir.name),
                team_profiles=TEAM_PROFILE_DEFAULTS,
                llm_provider="codex_cli_bridge",
                gitlab_token="secret-token",
                git_timeout_seconds=5,
                max_file_bytes=200_000,
            )

        self.assertEqual(service.llm_budgets["cheap"]["model"], "gpt-5.5")
        self.assertEqual(service.llm_budgets["balanced"]["model"], "gpt-5.5")
        self.assertEqual(service.llm_budgets["deep"]["model"], "gpt-5.5")
        self.assertEqual(service._llm_fallback_model(), "gpt-5.5")

    def test_codex_cli_provider_uses_codex_specific_timeout(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="codex_cli_bridge",
            llm_timeout_seconds=90,
            codex_timeout_seconds=260,
            codex_concurrency=2,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

        self.assertEqual(service.llm_provider.timeout_seconds, 260)
        self.assertEqual(service.llm_provider.concurrency_limit, 2)

    def test_codex_investigation_brief_uses_paths_not_long_snippet_context(self):
        entry = RepositoryEntry("Portal Repo", "https://git.example.com/team/portal.git")
        key = "AF:All"
        repo_path = self.service._repo_path(key, entry)
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "repository" / "IssueRepository.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class IssueRepository {\n  void find() {}\n}\n", encoding="utf-8")
        matches = [
            {
                "repo": "Portal Repo",
                "path": "repository/IssueRepository.java",
                "line_start": 1,
                "line_end": 3,
                "retrieval": "persistent_index",
                "trace_stage": "direct",
                "reason": "IssueRepository matched query",
                "snippet": "x" * 5000,
            }
        ]

        candidate_paths = self.service._codex_candidate_paths(entries=[entry], key=key, matches=matches)
        brief = self.service._codex_investigation_brief(
            pm_team="AF",
            country="All",
            question="where is IssueRepository",
            candidate_paths=candidate_paths,
            evidence_pack={"entry_points": ["IssueRepository [S1]"]},
            quality_gate={"status": "sufficient", "confidence": "high", "missing": []},
            followup_context={
                "question": "previous question",
                "answer": "previous answer",
                "codex_candidate_paths": [
                    {"repo": "Portal Repo", "repo_root": str(repo_path), "path": "service/PriorService.java", "line_start": 5, "line_end": 9}
                ],
                "codex_citation_validation": {
                    "status": "ok",
                    "cited_path_count": 1,
                    "direct_file_refs": [{"path": "service/PriorService.java", "line_start": 5, "line_end": 9}],
                },
                "codex_cli_summary": {"prompt_mode": "codex_investigation_brief_v1", "candidate_path_count": 1, "repair_attempted": False},
                "matches_snapshot": [{"repo": "Portal Repo", "path": "repository/IssueRepository.java", "line_start": 1, "line_end": 3}],
                "recent_turns": [
                    {
                        "question": "earlier session question",
                        "answer": "earlier session answer",
                        "trace_id": "trace-earlier",
                        "matches_snapshot": [{"repo": "Portal Repo", "path": "service/EarlierService.java", "line_start": 2, "line_end": 4}],
                        "codex_candidate_paths": [{"repo": "Portal Repo", "repo_root": str(repo_path), "path": "service/EarlierService.java"}],
                    }
                ],
            },
        )

        self.assertIn("Prompt mode: codex_investigation_brief_v3", brief)
        self.assertIn("Three-stage investigation required:", brief)
        self.assertIn("Stage 2 gap verification", brief)
        self.assertIn("full rule/config definitions from status-only migration updates", brief)
        self.assertIn("Candidate path layers:", brief)
        self.assertIn("current_high_confidence_paths", brief)
        self.assertIn(str(repo_path), brief)
        self.assertIn("relative_root=AF-All", brief)
        self.assertIn("path=repository/IssueRepository.java", brief)
        self.assertIn("file_exists=True", brief)
        self.assertIn("path_status=exact", brief)
        self.assertIn("Follow-up context:", brief)
        self.assertIn("previous question", brief)
        self.assertIn("Previous Codex candidate paths:", brief)
        self.assertIn("service/PriorService.java", brief)
        self.assertIn("Previous citation validation: status=ok", brief)
        self.assertIn("Previous Codex run: prompt_mode=codex_investigation_brief_v1", brief)
        self.assertIn("Earlier session turns:", brief)
        self.assertIn("earlier session question", brief)
        self.assertIn("service/EarlierService.java", brief)
        self.assertNotIn("x" * 100, brief)

    def test_codex_candidate_paths_resolve_stale_path_by_filename(self):
        entry = RepositoryEntry("Portal Repo", "https://git.example.com/team/portal.git")
        key = "AF:All"
        repo_path = self.service._repo_path(key, entry)
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "src" / "main" / "java" / "IssueRepository.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class IssueRepository {}\n", encoding="utf-8")
        self.service._build_repo_index(key, entry, repo_path)

        candidate_paths = self.service._codex_candidate_paths(
            entries=[entry],
            key=key,
            matches=[
                {
                    "repo": "Portal Repo",
                    "path": "repository/IssueRepository.java",
                    "line_start": 1,
                    "line_end": 1,
                }
            ],
        )

        self.assertEqual(candidate_paths[0]["path"], "src/main/java/IssueRepository.java")
        self.assertEqual(candidate_paths[0]["original_path"], "repository/IssueRepository.java")
        self.assertTrue(candidate_paths[0]["repo_relative_root"].startswith("AF-All/"))
        self.assertTrue(candidate_paths[0]["file_exists"])
        self.assertEqual(candidate_paths[0]["path_status"], "resolved_by_filename")

    def test_codex_followup_candidate_paths_are_carried_forward(self):
        entry = RepositoryEntry("Portal Repo", "https://git.example.com/team/portal.git")
        key = "AF:All"
        repo_path = self.service._repo_path(key, entry)
        current_file = repo_path / "controller" / "IssueController.java"
        prior_file = repo_path / "repository" / "IssueRepository.java"
        earlier_file = repo_path / "service" / "EarlierService.java"
        current_file.parent.mkdir(parents=True)
        prior_file.parent.mkdir(parents=True)
        earlier_file.parent.mkdir(parents=True)
        current_file.write_text("class IssueController {}\n", encoding="utf-8")
        prior_file.write_text("class IssueRepository {}\n", encoding="utf-8")
        earlier_file.write_text("class EarlierService {}\n", encoding="utf-8")
        current = self.service._codex_candidate_paths(
            entries=[entry],
            key=key,
            matches=[{"repo": "Portal Repo", "path": "controller/IssueController.java", "line_start": 1, "line_end": 1}],
        )

        merged = self.service._merge_codex_followup_candidate_paths(
            current,
            {
                "codex_candidate_paths": [
                    {
                        "repo": "Portal Repo",
                        "repo_root": str(repo_path),
                        "path": "repository/IssueRepository.java",
                        "line_start": 1,
                        "line_end": 1,
                    }
                ],
                "recent_turns": [
                    {
                        "codex_candidate_paths": [
                            {
                                "repo": "Portal Repo",
                                "repo_root": str(repo_path),
                                "path": "service/EarlierService.java",
                                "line_start": 1,
                                "line_end": 1,
                            }
                        ]
                    }
                ],
            },
        )

        self.assertEqual(
            [item["path"] for item in merged],
            ["controller/IssueController.java", "repository/IssueRepository.java", "service/EarlierService.java"],
        )
        self.assertEqual(merged[1]["retrieval"], "previous_codex_context")

    def test_codex_citation_validation_accepts_direct_file_reference(self):
        entry = RepositoryEntry("Portal Repo", "https://git.example.com/team/portal.git")
        key = "AF:All"
        repo_path = self.service._repo_path(key, entry)
        source_file = repo_path / "repository" / "IssueRepository.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("line1\nline2\nline3\n", encoding="utf-8")
        candidate_paths = self.service._codex_candidate_paths(
            entries=[entry],
            key=key,
            matches=[{"repo": "Portal Repo", "path": "repository/IssueRepository.java", "line_start": 1, "line_end": 3}],
        )
        answer = (
            '{"direct_answer":"IssueRepository reads issue_table.",'
            '"claims":[{"text":"IssueRepository reads issue_table",'
            '"citations":["repository/IssueRepository.java:1-3"]}],'
            '"missing_evidence":[],"confidence":"high"}'
        )

        validation = self.service._validate_codex_citations(answer, candidate_paths, [{"repo": "Portal Repo", "path": "repository/IssueRepository.java"}])

        self.assertEqual(validation["status"], "ok")
        self.assertEqual(validation["cited_path_count"], 1)

    def test_codex_deep_investigation_triggers_on_missing_business_chain(self):
        structured = {
            "direct_answer": "可能是 merchantUid 被当成 shopeeUid 后查 UC。",
            "confirmed_from_code": ["UserInfoSPIAdapter.queryMerchantInfo calls user-proxy [S1]"],
            "inferred_from_code": ["merchantUid may be copied into shopeeUid"],
            "not_found": ["No caller chain from report ingestion to queryMerchantInfo was found."],
            "missing_evidence": [],
            "claims": [{"text": "queryMerchantInfo uses shopeeUid", "citations": ["S1"]}],
            "confidence": "medium",
        }

        needed = self.service._codex_deep_investigation_needed(
            question="开发说 v2 上报拿 merchantUid 查 UC 填充 merchantInfo 是什么意思，为什么失败？",
            answer=structured["direct_answer"],
            structured_answer=structured,
            quality_gate={"status": "sufficient", "confidence": "medium"},
            answer_judge={"status": "ok", "issues": []},
            codex_validation={"status": "ok", "issues": []},
        )
        terms = self.service._codex_deep_investigation_terms(
            question="v2 report merchantUid queryMerchantInfo",
            answer=structured["direct_answer"],
            structured_answer=structured,
            answer_judge={"issues": []},
            codex_validation={"unsupported_claims": []},
        )

        self.assertTrue(needed)
        self.assertIn("querymerchantinfo", terms)
        self.assertIn("merchantuid", terms)

    def test_codex_answer_runs_citation_repair(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            llm_provider="codex_cli_bridge",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        entry = RepositoryEntry("Portal Repo", "https://git.example.com/team/portal.git")
        key = "AF:All"
        repo_path = service._repo_path(key, entry)
        source_file = repo_path / "repository" / "IssueRepository.java"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("class IssueRepository {\n  void find(){ jdbc.query(\"select * from issue_table\"); }\n}\n", encoding="utf-8")
        matches = [
            {
                "repo": "Portal Repo",
                "path": "repository/IssueRepository.java",
                "line_start": 1,
                "line_end": 3,
                "retrieval": "persistent_index",
                "trace_stage": "direct",
                "reason": "repository and table matched",
                "score": 10,
                "snippet": "class IssueRepository { void find(){ jdbc.query(\"select * from issue_table\"); } }",
            }
        ]
        calls = []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            if "login" in command and "status" in command:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            output_path = command[command.index("--output-last-message") + 1]
            exec_count = len([item for item in calls if "exec" in item[0]])
            if exec_count == 1:
                Path(output_path).write_text(
                    '{"direct_answer":"IssueRepository reads issue_table.",'
                    '"claims":[{"text":"IssueRepository reads issue_table","citations":[]}],'
                    '"missing_evidence":[],"confidence":"high"}',
                    encoding="utf-8",
                )
            else:
                Path(output_path).write_text(
                    '{"direct_answer":"IssueRepository reads issue_table.",'
                    '"claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],'
                    '"missing_evidence":[],"confidence":"high"}',
                    encoding="utf-8",
                )
            return SimpleNamespace(returncode=0, stdout='{"type":"done"}\n', stderr="")

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_run,
        ):
            payload = service._build_llm_answer(
                entries=[entry],
                key=key,
                pm_team="AF",
                country="All",
                question="what table does IssueRepository read",
                matches=matches,
                llm_budget_mode="auto",
                followup_context={"question": "previous", "answer": "previous answer"},
                requested_answer_mode="auto",
            )

        exec_calls = [item for item in calls if "exec" in item[0]]
        self.assertEqual(len(exec_calls), 2)
        self.assertEqual(payload["llm_route"]["prompt_mode"], "codex_investigation_brief_v3")
        self.assertIn("candidate_path_layers", payload["llm_route"])
        self.assertTrue(payload["llm_route"]["codex_repair_attempted"])
        self.assertEqual(payload["answer_claim_check"]["status"], "ok")
        self.assertEqual(payload["codex_cli_summary"]["citation_validation_status"], "ok")
        self.assertEqual(payload["answer_contract"]["status"], "model_answer")
        self.assertIn("IssueRepository reads issue_table", payload["llm_answer"])

    def test_codex_cli_bridge_requires_chatgpt_login(self):
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=Path(self.temp_dir.name),
            timeout_seconds=20,
            codex_binary="codex",
        )
        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            return_value=SimpleNamespace(returncode=0, stdout="Logged in using API key\n", stderr=""),
        ):
            self.assertFalse(provider.ready())

    def test_codex_cli_bridge_nonzero_exit_falls_back(self):
        provider = CodexCliBridgeSourceCodeQALLMProvider(
            workspace_root=Path(self.temp_dir.name),
            timeout_seconds=20,
            codex_binary="codex",
        )

        def fake_run(command, **kwargs):
            if command[:3] == ["codex", "login", "status"]:
                return SimpleNamespace(returncode=0, stdout="Logged in using ChatGPT\n", stderr="")
            return SimpleNamespace(returncode=2, stdout="", stderr="quota exhausted")

        with patch("bpmis_jira_tool.source_code_qa.shutil.which", return_value="/usr/local/bin/codex"), patch(
            "bpmis_jira_tool.source_code_qa.subprocess.run",
            side_effect=fake_run,
        ):
            with self.assertRaisesRegex(ToolError, "Codex unavailable"):
                provider.generate(payload={"contents": [{"parts": [{"text": "hi"}]}]}, primary_model="codex-cli", fallback_model="codex-cli")

    def test_auto_simple_lookup_uses_flash_lite_with_zero_thinking(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )
        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"batchCreateJiraIssue is in BPMISClient.",'
                                        '"claims":[{"text":"BPMISClient defines batchCreateJiraIssue","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 20, "candidatesTokenCount": 8, "totalTokenCount": 28},
            },
            text='{"ok":true}',
            raise_for_status=lambda: None,
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="auto",
                llm_budget_mode="auto",
            )

        self.assertEqual(payload["llm_budget_mode"], "cheap")
        self.assertEqual(payload["llm_model"], "gemini-2.5-flash-lite")
        self.assertEqual(payload["llm_thinking_budget"], 0)
        self.assertIn("gemini-2.5-flash-lite", mocked_post.call_args.args[0])
        self.assertEqual(mocked_post.call_args.kwargs["json"]["generationConfig"]["thinkingConfig"]["thinkingBudget"], 0)

    def test_gemini_thinking_budget_is_clamped_to_supported_minimum(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

        self.assertEqual(service._normalize_thinking_budget_for_provider(0), 0)
        self.assertEqual(service._normalize_thinking_budget_for_provider(256), 512)
        self.assertEqual(service._normalize_thinking_budget_for_provider(999999), 24576)

    def test_token_heavy_auto_route_uses_compact_deep_budget(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"IssueRepository reads issue_table.",'
                                        '"claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 12000, "candidatesTokenCount": 50, "totalTokenCount": 12050},
            },
            text='{"ok":true}',
            raise_for_status=lambda: None,
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post, patch.object(
            service,
            "_estimate_llm_tokens",
            side_effect=[25000, 12000],
        ):
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="what data source does issue creation use",
                answer_mode="auto",
                llm_budget_mode="auto",
            )

        self.assertEqual(payload["llm_budget_mode"], "compact_deep")
        self.assertEqual(payload["llm_route"]["original_budget"], "deep")
        self.assertEqual(payload["llm_route"]["token_pressure"]["status"], "tight")
        self.assertIn("token_pressure_tight", payload["llm_route"]["reason"])
        request_payload = mocked_post.call_args_list[0].kwargs["json"]
        self.assertEqual(request_payload["generationConfig"]["thinkingConfig"]["thinkingBudget"], 0)
        self.assertEqual(request_payload["generationConfig"]["maxOutputTokens"], 2400)
        self.assertEqual(payload["llm_thinking_budget"], 0)

    def test_auto_answer_mode_routes_data_source_questions_to_deep_budget(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        repository_file = repo_path / "repository" / "IssueRepository.java"
        repository_file.parent.mkdir(parents=True)
        repository_file.write_text(
            "public class IssueRepository {\n"
            "    public Issue findIssue() {\n"
            "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        fake_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"direct_answer":"IssueRepository reads issue_table.",'
                                        '"claims":[{"text":"IssueRepository reads issue_table","citations":["S1"]}],'
                                        '"missing_evidence":[],"confidence":"high"}'
                                    )
                                }
                            ]
                        }
                    }
                ],
                "usageMetadata": {"promptTokenCount": 123, "candidatesTokenCount": 45, "totalTokenCount": 168},
            },
            text='{"ok":true}',
            raise_for_status=lambda: None,
        )

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=fake_response) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="what data source does issue creation use",
                answer_mode="auto",
                llm_budget_mode="auto",
            )

        self.assertEqual(payload["answer_mode"], "auto")
        self.assertEqual(payload["llm_budget_mode"], "deep")
        self.assertEqual(payload["llm_route"]["mode"], "auto")
        self.assertIn("data_source_trace", payload["llm_route"]["reason"])
        self.assertIn("IssueRepository reads issue_table", payload["llm_answer"])
        self.assertIn("gemini-2.5-flash", mocked_post.call_args_list[0].args[0])
        request_payload = mocked_post.call_args_list[0].kwargs["json"]
        self.assertEqual(request_payload["generationConfig"]["thinkingConfig"]["thinkingBudget"], 1024)
        request_text = request_payload["contents"][0]["parts"][0]["text"]
        system_text = request_payload["systemInstruction"]["parts"][0]["text"]
        self.assertIn("Domain guidance: Anti-Fraud", request_text)
        self.assertIn("Answer blueprint", request_text)
        self.assertIn("final source/table/API", request_text)
        self.assertIn("Evidence priority", request_text)
        self.assertIn("domain guidance and answer blueprint", system_text)
        self.assertIn(payload["llm_thinking_budget"], {1024, 2048})

    def test_gemini_failure_does_not_fallback_to_retrieval_only(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )

        class FakeErrorResponse:
            text = '{"error":"quota exceeded"}'

            def raise_for_status(self):
                import requests
                raise requests.HTTPError(response=self)

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=FakeErrorResponse()):
            self._build_all_indexes(service)
            with self.assertRaises(SourceCodeQALLMError):
                service.query(
                    pm_team="AF",
                    country="All",
                    question="where is batchCreateJiraIssue",
                    answer_mode="gemini_flash",
                    llm_budget_mode="balanced",
                )

    def test_gemini_429_resource_exhausted_keeps_llm_retry_enabled(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            llm_max_retries=0,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )

        class RateLimitedResponse:
            ok = False
            status_code = 429
            text = '{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"quota exceeded"}}'
            headers = {"Retry-After": "3"}

        with patch("bpmis_jira_tool.source_code_qa.requests.post", return_value=RateLimitedResponse()):
            self._build_all_indexes(service)
            with self.assertRaises(SourceCodeQALLMError):
                service.query(
                    pm_team="AF",
                    country="All",
                    question="where is batchCreateJiraIssue",
                    answer_mode="gemini_flash",
                    llm_budget_mode="balanced",
                )

    def test_gemini_503_retries_and_falls_back_to_lite_model(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            gemini_model="gemini-2.5-flash",
            gemini_fallback_model="gemini-2.5-flash-lite",
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )
        service.save_mapping(
            pm_team="AF",
            country="All",
            repositories=[{"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"}],
        )
        entry = service.load_config()["mappings"]["AF:All"][0]
        repo_path = service._repo_path("AF:All", type("Entry", (), entry)())
        (repo_path / ".git").mkdir(parents=True)
        source_file = repo_path / "bpmis" / "jira_client.py"
        source_file.parent.mkdir(parents=True)
        source_file.write_text(
            "class BPMISClient:\n"
            "    def batchCreateJiraIssue(self):\n"
            "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
            encoding="utf-8",
        )

        class RetryableResponse:
            ok = False
            status_code = 503
            text = '{"error":{"code":503,"status":"UNAVAILABLE"}}'

            def raise_for_status(self):
                import requests
                raise requests.HTTPError(response=self)

        success_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [{"content": {"parts": [{"text": "Recovered answer: batchCreateJiraIssue is handled in the BPMIS client method and grounded by the retrieved source reference."}]}}],
                "usageMetadata": {"promptTokenCount": 50, "candidatesTokenCount": 20},
            },
            text='{"ok":true}',
            raise_for_status=lambda: None,
        )

        with patch("bpmis_jira_tool.source_code_qa.time.sleep"), patch(
            "bpmis_jira_tool.source_code_qa.requests.post",
            side_effect=[RetryableResponse(), RetryableResponse(), RetryableResponse(), success_response],
        ) as mocked_post:
            self._build_all_indexes(service)
            payload = service.query(
                pm_team="AF",
                country="All",
                question="where is batchCreateJiraIssue",
                answer_mode="gemini_flash",
                llm_budget_mode="balanced",
            )

        self.assertEqual(payload["answer_mode"], "gemini_flash")
        self.assertEqual(payload["llm_model"], "gemini-2.5-flash-lite")
        self.assertEqual(payload["llm_attempts"], 4)
        self.assertIn("Recovered answer", payload["llm_answer"])
        self.assertEqual(mocked_post.call_count, 4)

    def test_llm_runtime_honors_retry_after_and_records_attempts(self):
        service = SourceCodeQAService(
            data_root=Path(self.temp_dir.name),
            team_profiles=TEAM_PROFILE_DEFAULTS,
            gemini_api_key="gemini-key",
            llm_timeout_seconds=45,
            llm_max_retries=1,
            llm_backoff_seconds=1.0,
            llm_max_backoff_seconds=5.0,
            gitlab_token="secret-token",
            git_timeout_seconds=5,
            max_file_bytes=200_000,
        )

        class RateLimitedResponse:
            ok = False
            status_code = 429
            text = '{"error":{"code":429,"status":"RESOURCE_EXHAUSTED"}}'
            headers = {"Retry-After": "0.25"}

        success_response = SimpleNamespace(
            ok=True,
            json=lambda: {
                "candidates": [{"content": {"parts": [{"text": '{"direct_answer":"ok","claims":[],"missing_evidence":[],"confidence":"high"}'}]}}],
                "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2, "totalTokenCount": 5},
            },
            text='{"ok":true}',
        )

        with patch("bpmis_jira_tool.source_code_qa.time.sleep") as mocked_sleep, patch(
            "bpmis_jira_tool.source_code_qa.requests.post",
            side_effect=[RateLimitedResponse(), success_response],
        ) as mocked_post:
            result = service.llm_provider.generate(
                payload={"contents": [{"parts": [{"text": "hi"}]}], "generationConfig": {"maxOutputTokens": 20}},
                primary_model="gemini-2.5-flash",
                fallback_model="",
            )

        self.assertEqual(result.attempts, 2)
        mocked_sleep.assert_called_once_with(0.25)
        self.assertEqual(mocked_post.call_args.kwargs["timeout"], 45)
        self.assertEqual(result.attempt_log[0]["status"], 429)
        self.assertEqual(result.attempt_log[1]["status"], "ok")
        self.assertEqual(result.usage["totalTokenCount"], 5)


if __name__ == "__main__":
    unittest.main()
