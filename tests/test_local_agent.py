import base64
import json
import os
import tempfile
import threading
import time
import unittest
import zipfile
import io
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from cryptography.fernet import Fernet

from bpmis_jira_tool.daily_brief_archive import DailyBriefArchiveStore, daily_brief_archive_path
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.job_store import JobStore
from bpmis_jira_tool.local_agent_client import LocalAgentClient
from bpmis_jira_tool.local_agent_protocol import sign_headers, verify_signature
from bpmis_jira_tool.local_agent_server import create_local_agent_app
from bpmis_jira_tool.models import CreatedTicket
from bpmis_jira_tool.bpmis_client import build_bpmis_client
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.vpn_manager import VPNProfileStore


class LocalAgentProtocolTests(unittest.TestCase):
    def test_signature_round_trip_and_tamper_rejection(self):
        body = b'{"question":"hello"}'
        headers = sign_headers(secret="shared-secret", method="POST", path="/api/local-agent/source-code-qa/query", body=body)

        verify_signature(
            secret="shared-secret",
            method="POST",
            path="/api/local-agent/source-code-qa/query",
            body=body,
            timestamp=headers["X-Local-Agent-Timestamp"],
            nonce=headers["X-Local-Agent-Nonce"],
            signature=headers["X-Local-Agent-Signature"],
        )

        with self.assertRaises(ToolError):
            verify_signature(
                secret="shared-secret",
                method="POST",
                path="/api/local-agent/source-code-qa/query",
                body=b'{"question":"changed"}',
                timestamp=headers["X-Local-Agent-Timestamp"],
                nonce=headers["X-Local-Agent-Nonce"],
                signature=headers["X-Local-Agent-Signature"],
            )

    def test_stale_signature_is_rejected(self):
        with self.assertRaises(ToolError):
            verify_signature(
                secret="shared-secret",
                method="POST",
                path="/api/local-agent/source-code-qa/query",
                body=b"{}",
                timestamp=str(int(time.time()) - 1000),
                nonce="abc",
                signature="bad",
                max_skew_seconds=10,
            )


class LocalAgentServerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        env = {
            "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
            "LOCAL_AGENT_BPMIS_ENABLED": "true",
            "LOCAL_AGENT_SEATALK_ENABLED": "true",
            "TEAM_PORTAL_DATA_DIR": self.temp_dir.name,
            "SOURCE_CODE_QA_LLM_PROVIDER": "codex_cli_bridge",
        }
        self.env_patch = patch.dict(os.environ, env, clear=True)
        self.env_patch.start()
        self.dotenv_patch = patch("bpmis_jira_tool.config.find_dotenv", return_value="")
        self.dotenv_patch.start()
        self.app = create_local_agent_app()

    def tearDown(self):
        self.dotenv_patch.stop()
        self.env_patch.stop()
        self.temp_dir.cleanup()

    def test_team_dashboard_jobs_are_isolated_from_portal_jobs_file(self):
        job_store = self.app.config["TEAM_DASHBOARD_JOB_STORE"]
        self.assertEqual(Path(job_store.storage_path).name, "team_dashboard_jobs.json")

        job = job_store.create("monthly-report", "Monthly Report")
        job_store.update(job.job_id, state="running", stage="generating", message="Running")
        portal_store = JobStore(Path(self.temp_dir.name) / "run" / "jobs.json")

        self.assertIsNone(portal_store.get(job.job_id))
        self.assertEqual(job_store.get(job.job_id).state, "running")

    def test_missing_monthly_report_job_returns_restart_interruption_status(self):
        response = self._get_signed("/api/local-agent/team-dashboard/monthly-report/jobs/missing-job")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["state"], "failed")
        self.assertEqual(payload["error_category"], "server_restart")
        self.assertEqual(payload["error_code"], "monthly_report_job_interrupted")
        self.assertTrue(payload["error_retryable"])
        self.assertIn("interrupted", payload["message"])
        self.assertEqual(payload["progress"]["stage"], "failed")

    def _post_signed(self, path, payload):
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        headers = sign_headers(secret="shared-secret", method="POST", path=path, body=body)
        headers["Content-Type"] = "application/json"
        return self.app.test_client().post(path, data=body, headers=headers)

    def _get_signed(self, path):
        headers = sign_headers(secret="shared-secret", method="GET", path=path, body=b"")
        return self.app.test_client().get(path, headers=headers)

    def _get_signed_with_query(self, route_path, query_string):
        headers = sign_headers(secret="shared-secret", method="GET", path=route_path, body=b"")
        return self.app.test_client().get(f"{route_path}?{query_string}", headers=headers)

    def _wait_for_meeting_process_job(self, job_id, *, owner_email="owner@npt.sg", terminal_state="completed", timeout=2.0):
        route_path = f"/api/local-agent/meeting-recorder/process-jobs/{job_id}"
        deadline = time.time() + timeout
        last_payload = {}
        while time.time() < deadline:
            response = self._get_signed_with_query(route_path, f"owner_email={owner_email}")
            self.assertEqual(response.status_code, 200)
            last_payload = response.get_json()
            if last_payload.get("state") == terminal_state:
                return last_payload
            time.sleep(0.02)
        self.fail(f"Meeting Recorder local-agent job did not reach {terminal_state}: {last_payload}")

    def test_vpn_failed_connect_does_not_record_success(self):
        class FakeCiscoVPNClient:
            def __init__(self):
                self.connect_calls = []

            def status(self):
                return {"status": "ok", "connected": False, "state": "Disconnected", "message": "state: Disconnected"}

            def hosts(self):
                return ["ShopeeVPN"]

            def connect(self, *, host, username, password, second_password=""):
                self.connect_calls.append((host, username, password, second_password))
                return {"status": "ok", "connected": False, "state": "Disconnected", "message": "state: Disconnected"}

        self.app.config["VPN_PROFILE_STORE"] = VPNProfileStore(
            Path(self.temp_dir.name) / "vpn.db",
            encryption_key=Fernet.generate_key().decode("utf-8"),
        )
        fake_cisco = FakeCiscoVPNClient()
        self.app.config["CISCO_VPN_CLIENT"] = fake_cisco
        save_response = self._post_signed(
            "/api/local-agent/vpn/profiles",
            {
                "display_name": "Shopee VPN",
                "vpn_host": "ShopeeVPN",
                "username": "vpn-user",
                "password": "vpn-secret",
            },
        )
        profile_id = save_response.get_json()["profile"]["id"]

        connect_response = self._post_signed(f"/api/local-agent/vpn/profiles/{profile_id}/connect", {})

        self.assertEqual(connect_response.status_code, 400)
        self.assertIn("Disconnected", connect_response.get_json()["message"])
        profile = self.app.config["VPN_PROFILE_STORE"].get_profile(profile_id)
        self.assertIsNone(profile["last_connected_at"])

    def test_vpn_connect_passes_second_password_to_cisco_client(self):
        class FakeCiscoVPNClient:
            def __init__(self):
                self.connect_calls = []

            def status(self):
                return {"status": "ok", "connected": False, "state": "Disconnected", "message": "state: Disconnected"}

            def hosts(self):
                return ["Seabank PH"]

            def connect(self, *, host, username, password, second_password=""):
                self.connect_calls.append((host, username, password, second_password))
                return {"status": "ok", "connected": True, "state": "Connected", "message": "state: Connected"}

        self.app.config["VPN_PROFILE_STORE"] = VPNProfileStore(
            Path(self.temp_dir.name) / "vpn.db",
            encryption_key=Fernet.generate_key().decode("utf-8"),
        )
        fake_cisco = FakeCiscoVPNClient()
        self.app.config["CISCO_VPN_CLIENT"] = fake_cisco
        save_response = self._post_signed(
            "/api/local-agent/vpn/profiles",
            {
                "display_name": "Seabank PH VPN",
                "vpn_host": "Seabank PH",
                "username": "vpn-user",
                "password": "vpn-secret",
            },
        )
        profile_id = save_response.get_json()["profile"]["id"]

        connect_response = self._post_signed(
            f"/api/local-agent/vpn/profiles/{profile_id}/connect",
            {"second_password": "second-secret"},
        )

        self.assertEqual(connect_response.status_code, 200)
        self.assertEqual(fake_cisco.connect_calls, [("Seabank PH", "vpn-user", "vpn-secret", "second-secret")])
        payload = connect_response.get_json()
        self.assertTrue(payload["vpn_status"]["connected"])
        self.assertEqual(payload["profiles"][0]["id"], profile_id)
        self.assertIsNotNone(payload["profiles"][0]["last_connected_at"])
        self.assertNotIn("second-secret", str(payload))

    def test_healthz_is_public_and_reports_capabilities(self):
        response = self.app.test_client().get("/healthz")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["capabilities"]["source_code_qa"])

    def test_proxy_style_healthz_alias_is_public_for_direct_agent_url(self):
        response = self.app.test_client().get("/api/local-agent/healthz")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["capabilities"]["source_code_qa"])

    def test_signed_source_code_query_delegates_to_local_service(self):
        with patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
            return_value={"status": "ok", "summary": "agent answer", "matches": []},
        ) as query:
            response = self._post_signed(
                "/api/local-agent/source-code-qa/query",
                {"pm_team": "AF", "country": "All", "question": "where is createIssue", "answer_mode": "auto", "llm_budget_mode": "auto"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["summary"], "agent answer")
        query.assert_called_once()

    def test_signed_source_code_query_forwards_attachments(self):
        with patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
            return_value={"status": "ok", "summary": "agent answer", "matches": []},
        ) as query:
            response = self._post_signed(
                "/api/local-agent/source-code-qa/query",
                {
                    "pm_team": "AF",
                    "country": "All",
                    "question": "use file",
                    "attachments": [{"id": "att-1", "filename": "notes.txt", "kind": "text"}],
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(query.call_args.kwargs["attachments"][0]["id"], "att-1")

    def test_signed_source_code_attachment_save_and_resolve(self):
        save_response = self._post_signed(
            "/api/local-agent/source-code-qa/attachments/save",
            {
                "owner_email": "teammate@npt.sg",
                "session_id": "session1234",
                "filename": "notes.txt",
                "mime_type": "text/plain",
                "content_base64": "aGVsbG8gYXR0YWNobWVudA==",
            },
        )
        attachment_id = save_response.get_json()["attachment"]["id"]
        resolve_response = self._post_signed(
            "/api/local-agent/source-code-qa/attachments/resolve",
            {
                "owner_email": "teammate@npt.sg",
                "session_id": "session1234",
                "attachment_ids": [attachment_id],
            },
        )

        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(resolve_response.status_code, 200)
        resolved = resolve_response.get_json()["attachments"][0]
        self.assertEqual(resolved["filename"], "notes.txt")
        self.assertIn("hello attachment", resolved["text"])

    def test_signed_source_code_generated_artifact_save_and_get(self):
        save_response = self._post_signed(
            "/api/local-agent/source-code-qa/generated-artifacts/save",
            {
                "owner_email": "teammate@npt.sg",
                "session_id": "session1234",
                "pm_team": "GRC",
                "country": "SG",
                "question": "write SQL",
                "sql": "select lock_key from bcf_global_lock;",
                "readme": "# SQL package\n",
            },
        )
        artifact_id = save_response.get_json()["artifact"]["id"]
        get_response = self._post_signed(
            "/api/local-agent/source-code-qa/generated-artifacts/get",
            {
                "owner_email": "teammate@npt.sg",
                "session_id": "session1234",
                "artifact_id": artifact_id,
            },
        )

        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(get_response.status_code, 200)
        encoded = get_response.get_json()["content_base64"]
        with zipfile.ZipFile(io.BytesIO(base64.b64decode(encoded))) as archive:
            sql = archive.read("query.sql").decode("utf-8")
            self.assertIn("SELECT lock_key", sql)
            self.assertRegex(sql, r"\n\s+FROM bcf_global_lock")

    def test_signed_source_code_async_query_streams_progress(self):
        class ImmediateThread:
            def __init__(self, *, target, args=(), daemon=False):
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self):
                self.target(*self.args)

        def fake_query(**kwargs):
            kwargs["progress_callback"]("codex_stream", "Reading src/App.java", 0, 0)
            return {"summary": "agent answer", "matches": []}

        with patch("bpmis_jira_tool.local_agent_server.threading.Thread", side_effect=lambda target, args=(), daemon=False: ImmediateThread(target=target, args=args, daemon=daemon)), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.query",
            side_effect=fake_query,
        ):
            create_response = self._post_signed(
                "/api/local-agent/source-code-qa/query-async",
                {"pm_team": "AF", "country": "All", "question": "where is createIssue", "answer_mode": "auto", "llm_budget_mode": "auto"},
            )
            job_id = create_response.get_json()["job_id"]
            status_response = self._get_signed(f"/api/local-agent/source-code-qa/query-jobs/{job_id}")

        self.assertEqual(create_response.status_code, 200)
        self.assertEqual(status_response.status_code, 200)
        payload = status_response.get_json()
        self.assertEqual(payload["state"], "completed")
        self.assertEqual(payload["result"]["summary"], "agent answer")

    def test_source_code_async_query_jobs_cleanup_old_terminal_snapshots(self):
        from bpmis_jira_tool.local_agent_server import _snapshot_query_job

        with self.app.app_context():
            jobs = self.app.config["SOURCE_CODE_QA_QUERY_JOBS"]
            jobs["old-completed"] = {"state": "completed", "updated_at": time.time() - 4000}
            jobs["running"] = {"state": "running", "updated_at": time.time() - 4000}

            self.assertIsNone(_snapshot_query_job("old-completed"))
            self.assertIn("running", jobs)

    def test_signed_source_code_auto_sync_can_queue_background_refresh(self):
        class ImmediateThread:
            def __init__(self, *, target, daemon):
                self.target = target
                self.daemon = daemon

            def start(self):
                self.target()

        with patch("bpmis_jira_tool.local_agent_server.threading.Thread", side_effect=lambda target, daemon: ImmediateThread(target=target, daemon=daemon)), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": True, "status": "ok"},
        ) as ensure_synced:
            response = self._post_signed(
                "/api/local-agent/source-code-qa/ensure-synced-today",
                {"pm_team": "AF", "country": "All", "background": True},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["status"], "background_queued")
        ensure_synced.assert_called_once_with(pm_team="AF", country="All")

    def test_signed_productization_llm_descriptions_delegates_to_local_codex(self):
        generated = [{"jira_ticket_number": "ABC-1", "detailed_feature": "Generated feature."}]
        with patch(
            "bpmis_jira_tool.local_agent_server._generate_productization_detailed_features_with_local_codex",
            return_value=generated,
        ) as generate:
            response = self._post_signed(
                "/api/local-agent/productization/llm-descriptions",
                {"items": [{"jira_ticket_number": "ABC-1", "jira_description": "Raw Jira description"}]},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["items"], generated)
        generate.assert_called_once()

    def test_signed_prd_briefing_image_proxy_fetches_confluence_image(self):
        class FakeConfluence:
            def __init__(self):
                self.requested_url = None
                self.accept = None

            def _request(self, url, accept):
                self.requested_url = url
                self.accept = accept

                class Response:
                    content = b"png-bytes"
                    status_code = 200
                    headers = {"content-type": "image/png"}

                return Response()

        class FakeService:
            def __init__(self):
                self.confluence = FakeConfluence()

        fake_service = FakeService()
        src = "https://confluence.shopee.io/download/attachments/123/mock.png?api=v2"
        with patch("bpmis_jira_tool.local_agent_server._build_prd_briefing_service", return_value=fake_service):
            response = self._post_signed("/api/local-agent/prd-briefing/image-proxy", {"src": src})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, "image/png")
        self.assertEqual(response.data, b"png-bytes")
        self.assertEqual(fake_service.confluence.requested_url, src)
        self.assertEqual(fake_service.confluence.accept, "image/*,*/*;q=0.8")

    def test_signed_prd_briefing_image_proxy_rejects_non_confluence_image(self):
        response = self._post_signed(
            "/api/local-agent/prd-briefing/image-proxy",
            {"src": "https://example.com/download/attachments/123/mock.png"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported image source", response.get_json()["message"])

    def test_signed_prd_self_assessment_review_forwards_selected_sections(self):
        class FakePRDReviewService:
            def __init__(self):
                self.request = None

            def review_url(self, request):
                self.request = request
                return {
                    "status": "ok",
                    "cached": False,
                    "language": request.language,
                    "review": {"result_markdown": "### Review"},
                    "prd": {"title": "PRD"},
                    "coverage": {"selected_section_indexes": request.selected_section_indexes},
                }

        fake_service = FakePRDReviewService()
        with patch("bpmis_jira_tool.local_agent_server._build_prd_review_service", return_value=fake_service):
            response = self._post_signed(
                "/api/local-agent/prd-self-assessment/review",
                {
                    "owner_key": "google:teammate@npt.sg",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "language": "en",
                    "selected_section_indexes": [27],
                    "google_credentials": {"token": "drive-token", "scopes": ["https://www.googleapis.com/auth/drive.readonly"]},
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["coverage"]["selected_section_indexes"], [27])
        self.assertEqual(fake_service.request.selected_section_indexes, [27])
        self.assertEqual(fake_service.request.google_credentials["token"], "drive-token")

    def test_signed_prd_self_assessment_review_async_completes_job(self):
        class FakePRDReviewService:
            def review_url(self, request):
                return {
                    "status": "ok",
                    "cached": False,
                    "language": request.language,
                    "review": {"result_markdown": "### Async Review"},
                    "prd": {"title": "PRD"},
                    "coverage": {"selected_section_indexes": request.selected_section_indexes},
                }

        with patch("bpmis_jira_tool.local_agent_server._build_prd_review_service", return_value=FakePRDReviewService()):
            response = self._post_signed(
                "/api/local-agent/prd-self-assessment/review-async",
                {
                    "owner_key": "google:teammate@npt.sg",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "language": "en",
                    "selected_section_indexes": [27],
                },
            )
            self.assertEqual(response.status_code, 200)
            job_id = response.get_json()["job_id"]
            payload = {}
            for _ in range(20):
                payload = self._get_signed(f"/api/local-agent/prd-jobs/{job_id}").get_json()
                if payload.get("state") == "completed":
                    break
                time.sleep(0.05)

        self.assertEqual(payload["state"], "completed")
        self.assertEqual(payload["results"][0]["review"]["result_markdown"], "### Async Review")
        self.assertEqual(payload["results"][0]["coverage"]["selected_section_indexes"], [27])

    def test_seatalk_service_uses_agent_daily_cache_dir(self):
        from bpmis_jira_tool.local_agent_server import _build_seatalk_service

        service = _build_seatalk_service(Settings.from_env())

        self.assertEqual(str(service.daily_cache_dir), os.path.join(self.temp_dir.name, "seatalk", "cache"))
        self.assertEqual(service.codex_model, "gpt-5.5")

    def test_signed_team_dashboard_daily_briefs_read_agent_archive(self):
        store = DailyBriefArchiveStore(daily_brief_archive_path(Path(self.temp_dir.name)))
        saved = store.save(
            run_date="2026-05-05",
            run_slot="midday",
            recipient="xiaodong.zheng@npt.sg",
            subject="Daily Brief - 2026-05-05 (2026-05-05 13:00 - 2026-05-05 19:00)",
            text_body="Subject: Daily Brief\n\nPlain archive body",
            html_body="<html><body><h3>Archive HTML</h3><ul><li><strong>Formatted</strong> archive item</li></ul></body></html>",
            message_id="msg-1",
            status="sent",
            sent_at=datetime(2026, 5, 5, 19, 0),
            window_start=datetime(2026, 5, 5, 13, 0),
            window_end=datetime(2026, 5, 5, 19, 0),
        )

        list_response = self._get_signed("/api/local-agent/team-dashboard/daily-briefs")
        download_response = self._get_signed(f"/api/local-agent/team-dashboard/daily-briefs/{saved['brief_id']}/download")

        self.assertEqual(list_response.status_code, 200)
        payload = list_response.get_json()
        self.assertEqual(payload["briefs"][0]["time_period"], "2026-05-05 13:00-19:00")
        self.assertNotIn("text_body", payload["briefs"][0])
        self.assertEqual(download_response.status_code, 200)
        self.assertEqual(download_response.headers["Content-Type"], "application/pdf")
        self.assertIn("daily-brief-2026-05-05-midday.pdf", download_response.headers["Content-Disposition"])
        self.assertGreater(len(download_response.data), 100)
        self.assertIn(b"/Helvetica-Bold", download_response.data)
        self.assertIn(b"Archive", download_response.data)
        self.assertIn(b"Formatted", download_response.data)
        self.assertNotIn(b"Plain archive body", download_response.data)

    def test_signed_seatalk_open_todos_returns_agent_store_items(self):
        seed = self._post_signed(
            "/api/local-agent/seatalk/todos/merge-open",
            {
                "owner_email": "xiaodong.zheng@npt.sg",
                "todos": [
                    {
                        "task": "Follow up rollout",
                        "domain": "Anti-fraud",
                        "priority": "high",
                        "due": "2026-04-30",
                        "evidence": "Apr 21",
                    }
                ],
            },
        )
        response = self._post_signed("/api/local-agent/seatalk/todos/open", {"owner_email": "xiaodong.zheng@npt.sg"})

        self.assertEqual(seed.status_code, 200)
        self.assertEqual(response.status_code, 200)
        self.assertEqual([todo["task"] for todo in response.get_json()["todos"]], ["Follow up rollout"])

    def test_unsigned_source_code_query_is_rejected(self):
        response = self.app.test_client().post("/api/local-agent/source-code-qa/query", json={"question": "hello"})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["status"], "error")

    def test_signed_meeting_recorder_asset_supports_range_streaming(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Playback",
            platform="google_meet",
            meeting_link="https://meet.google.com/abc-defg-hij",
        )
        asset_path = store.record_dir(record["record_id"]) / "meeting.mp4"
        asset_path.write_bytes(bytes(range(256)) * 4)
        route_path = f"/api/local-agent/meeting-recorder/assets/{record['record_id']}/meeting.mp4"
        headers = sign_headers(secret="shared-secret", method="GET", path=route_path, body=b"")
        headers["Range"] = "bytes=0-99"

        response = self.app.test_client().get(f"{route_path}?owner_email=owner@npt.sg", headers=headers)

        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.headers.get("Accept-Ranges"), "bytes")
        self.assertEqual(response.headers.get("Content-Type"), "video/mp4")
        self.assertEqual(response.headers.get("Content-Range"), f"bytes 0-99/{asset_path.stat().st_size}")
        self.assertEqual(len(response.data), 100)
        self.assertEqual(response.data, asset_path.read_bytes()[:100])
        response.close()

    def test_signed_meeting_recorder_asset_supports_head_and_full_get(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Playback",
            platform="google_meet",
            meeting_link="https://meet.google.com/abc-defg-hij",
        )
        asset_path = store.record_dir(record["record_id"]) / "meeting.mp4"
        asset_path.write_bytes(b"video-bytes")
        route_path = f"/api/local-agent/meeting-recorder/assets/{record['record_id']}/meeting.mp4"
        query_path = f"{route_path}?owner_email=owner@npt.sg"
        get_headers = sign_headers(secret="shared-secret", method="GET", path=route_path, body=b"")
        head_headers = sign_headers(secret="shared-secret", method="HEAD", path=route_path, body=b"")

        get_response = self.app.test_client().get(query_path, headers=get_headers)
        head_response = self.app.test_client().head(query_path, headers=head_headers)

        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(get_response.data, b"video-bytes")
        self.assertEqual(head_response.status_code, 200)
        self.assertEqual(head_response.data, b"")
        self.assertEqual(head_response.headers.get("Content-Length"), str(asset_path.stat().st_size))
        get_response.close()
        head_response.close()

    def test_signed_meeting_recorder_asset_download_sets_attachment_header(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Playback",
            platform="google_meet",
            meeting_link="https://meet.google.com/abc-defg-hij",
        )
        asset_path = store.record_dir(record["record_id"]) / "meeting.mp4"
        asset_path.write_bytes(b"video-bytes")
        route_path = f"/api/local-agent/meeting-recorder/assets/{record['record_id']}/meeting.mp4"
        headers = sign_headers(secret="shared-secret", method="GET", path=route_path, body=b"")

        response = self.app.test_client().get(f"{route_path}?owner_email=owner@npt.sg&download=1", headers=headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"video-bytes")
        self.assertIn("attachment", response.headers.get("Content-Disposition", ""))
        self.assertIn("meeting.mp4", response.headers.get("Content-Disposition", ""))
        response.close()

    def test_signed_meeting_recorder_asset_rejects_active_recording_media(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Active",
            platform="unknown",
            meeting_link="",
        )
        asset_path = store.record_dir(record["record_id"]) / "meeting.wav"
        asset_path.write_bytes(b"partial-audio")
        record["status"] = "recording"
        record["media"] = {
            "recording_mode": "audio_only",
            "audio_path": str(asset_path.relative_to(store.root_dir)),
        }
        store.save_record(record)
        route_path = f"/api/local-agent/meeting-recorder/assets/{record['record_id']}/meeting.wav"
        headers = sign_headers(secret="shared-secret", method="GET", path=route_path, body=b"")

        response = self.app.test_client().get(f"{route_path}?owner_email=owner@npt.sg&download=1", headers=headers)

        self.assertEqual(response.status_code, 409)
        self.assertIn("Stop the recording", response.get_json()["message"])
        response.close()

    def test_signed_meeting_recorder_start_allows_blank_link_for_sck_f2f(self):
        fake_runtime = Mock()
        fake_runtime.start_recording.return_value = {
            "record_id": "meeting-f2f",
            "title": "Face to face",
            "platform": "unknown",
            "meeting_link": "",
            "status": "recording",
            "media": {
                "recording_mode": "audio_only",
                "audio_capture_profile": "screencapturekit_audio_v1",
                "screencapture_capture_source": "screencapturekit_f2f",
            },
        }
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        response = self._post_signed(
            "/api/local-agent/meeting-recorder/start",
            {
                "owner_email": "owner@npt.sg",
                "title": "Face to face",
                "meeting_link": "",
                "recording_mode": "audio_only",
                "transcript_language": "en",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["record"]["media"]["screencapture_capture_source"], "screencapturekit_f2f")
        fake_runtime.start_recording.assert_called_once_with(
            owner_email="owner@npt.sg",
            title="Face to face",
            platform="unknown",
            meeting_link="",
            recording_mode="audio_only",
            calendar_event_id="",
            scheduled_start="",
            scheduled_end="",
            attendees=[],
            transcript_language="en",
        )

    def test_signed_meeting_recorder_process_async_returns_job_and_completes(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Async Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/async",
        )
        record["status"] = "recorded"
        store.save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.return_value = {
            "record_id": record["record_id"],
            "title": "Async Review",
            "platform": "zoom",
            "status": "completed",
        }
        fake_processing.send_minutes_email.return_value = {
            "status": "sent",
            "recipient": "owner@npt.sg",
            "message_id": "msg-1",
        }

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": record["record_id"], "owner_email": "owner@npt.sg", "send_email_on_complete": True},
            )
            payload = response.get_json()
            completed = self._wait_for_meeting_process_job(payload["job_id"])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["results"][0]["email"]["status"], "sent")
        fake_processing.process_recording.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="owner@npt.sg",
        )
        fake_processing.send_minutes_email.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="owner@npt.sg",
            recipient="owner@npt.sg",
        )

    def test_signed_meeting_recorder_process_async_recovers_stale_processing_record(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Recoverable Async Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/recover",
        )
        record["status"] = "processing"
        store.save_record(record)
        recovered_record = {
            **record,
            "status": "completed",
            "transcript": {"status": "completed"},
            "minutes": {"status": "completed"},
            "processing_recovery": {"status": "recovered_from_segments", "segment_count": 2},
        }
        fake_processing = Mock()
        fake_processing.recover_stale_processing_record.return_value = {
            "status": "recovered",
            "record": recovered_record,
        }

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["state"], "completed")
        self.assertEqual(payload["job_id"], "")
        self.assertEqual(payload["record"]["status"], "completed")
        self.assertEqual(payload["record"]["processing_recovery"]["status"], "recovered_from_segments")
        fake_processing.recover_stale_processing_record.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="owner@npt.sg",
        )
        fake_processing.process_recording.assert_not_called()

    def test_scheduled_auto_stop_queues_local_processing_and_email(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Scheduled Local Auto Stop",
            platform="google_meet",
            meeting_link="https://meet.google.com/abc-defg-hij",
            calendar_event_id="event-1",
            scheduled_start="2026-05-04T09:00:00+08:00",
            scheduled_end="2026-05-04T09:30:00+08:00",
        )
        record["status"] = "recording"
        record["recording_started_at"] = "2026-05-04T01:00:00+00:00"
        record["scheduled_auto_stop"] = {
            "status": "scheduled",
            "mode": "scheduled_end_plus_grace",
            "grace_seconds": 1200,
            "scheduled_for": "2026-05-04T01:50:00+00:00",
        }
        store.save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.return_value = {
            "record_id": record["record_id"],
            "title": "Scheduled Local Auto Stop",
            "platform": "google_meet",
            "status": "completed",
        }
        fake_processing.send_minutes_email.return_value = {
            "status": "sent",
            "recipient": "owner@npt.sg",
            "message_id": "msg-1",
        }

        with self.app.app_context(), patch(
            "bpmis_jira_tool.local_agent_server._build_meeting_processing_service",
            return_value=fake_processing,
        ), patch.object(
            self.app.config["MEETING_RECORDER_RUNTIME"],
            "_terminate_persisted_recorder_process",
        ):
            self.app.config["MEETING_RECORDER_RUNTIME"]._scheduled_auto_stop_callback(
                record_id=record["record_id"],
                owner_email="owner@npt.sg",
                scheduled_for="2026-05-04T01:50:00+00:00",
            )
            updated = store.get_record(record["record_id"])
            completed = self._wait_for_meeting_process_job(updated["scheduled_auto_stop"]["process_job_id"])

        self.assertEqual(updated["recording_stop_reason"], "scheduled_auto_stop")
        self.assertEqual(updated["scheduled_auto_stop"]["process_queue_status"], "queued")
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["results"][0]["email"]["status"], "sent")
        fake_processing.process_recording.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="owner@npt.sg",
        )
        fake_processing.send_minutes_email.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="owner@npt.sg",
            recipient="owner@npt.sg",
        )

    def test_signed_meeting_recorder_process_job_is_owner_scoped(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Scoped Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/scoped",
        )
        record["status"] = "recorded"
        store.save_record(record)
        release_processing = threading.Event()
        fake_processing = Mock()

        def process_recording(**kwargs):
            release_processing.wait(timeout=1)
            return {
                "record_id": kwargs["record_id"],
                "title": "Scoped Review",
                "platform": "zoom",
                "status": "completed",
            }

        fake_processing.process_recording.side_effect = process_recording

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
            )
            job_id = response.get_json()["job_id"]
            route_path = f"/api/local-agent/meeting-recorder/process-jobs/{job_id}"
            denied = self._get_signed_with_query(route_path, "owner_email=other@npt.sg")
            release_processing.set()
            completed = self._wait_for_meeting_process_job(job_id)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(denied.status_code, 404)
        self.assertEqual(completed["state"], "completed")

    def test_signed_meeting_recorder_process_async_failure_is_sanitized(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Failure Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/failure",
        )
        record["status"] = "recorded"
        store.save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.side_effect = RuntimeError("Traceback token=secret")

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
            )
            failed = self._wait_for_meeting_process_job(response.get_json()["job_id"], terminal_state="failed")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(failed["state"], "failed")
        self.assertNotIn("Traceback", failed["error"])
        self.assertNotIn("secret", failed["error"])

    def test_signed_bpmis_config_save_and_load_use_agent_data_dir(self):
        save_response = self._post_signed(
            "/api/local-agent/bpmis/config/save",
            {"user_key": "google:teammate@npt.sg", "config": {"pm_team": "AF", "sync_pm_email": "teammate@npt.sg"}},
        )
        load_response = self._post_signed(
            "/api/local-agent/bpmis/config/load",
            {"user_key": "google:teammate@npt.sg"},
        )

        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(load_response.status_code, 200)
        payload = load_response.get_json()
        self.assertEqual(payload["config"]["pm_team"], "AF")
        self.assertEqual(payload["config"]["sync_pm_email"], "teammate@npt.sg")

    def test_signed_bpmis_project_reorder_uses_agent_store(self):
        for bpmis_id in ("225159", "225160", "225161"):
            self._post_signed(
                "/api/local-agent/bpmis/projects/upsert",
                {
                    "user_key": "google:teammate@npt.sg",
                    "bpmis_id": bpmis_id,
                    "project_name": f"Project {bpmis_id}",
                    "brd_link": "",
                    "market": "SG",
                },
            )

        response = self._post_signed(
            "/api/local-agent/bpmis/projects/reorder",
            {"user_key": "google:teammate@npt.sg", "bpmis_ids": ["225161", "225159", "225160"]},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(["225161", "225159", "225160"], [project["bpmis_id"] for project in response.get_json()["projects"]])

    def test_signed_bpmis_call_delegates_to_direct_client(self):
        class FakeBPMISClient:
            def __init__(self, settings, access_token=None):
                self.access_token = access_token
                self.request_stats = {"api_call_count": 1}
                self.request_timings = {"issue_tree_reporter": 1.2}

            def list_biz_projects_for_pm_email(self, email):
                return [{"issue_id": "123", "project_name": email, "market": "SG"}]

        with patch("bpmis_jira_tool.local_agent_server.BPMISDirectApiClient", FakeBPMISClient):
            with self.assertLogs(self.app.logger.name, level="INFO") as captured:
                response = self._post_signed(
                    "/api/local-agent/bpmis/call",
                    {
                        "operation": "list_biz_projects_for_pm_email",
                        "access_token": "user-token",
                        "args": ["pm@npt.sg"],
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["result"][0]["issue_id"], "123")
        self.assertEqual(response.get_json()["request_stats"], {"api_call_count": 1})
        self.assertEqual(response.get_json()["request_timings"], {"issue_tree_reporter": 1.2})
        log_text = "\n".join(captured.output)
        self.assertIn('"event": "local_agent_bpmis_call_start"', log_text)
        self.assertIn('"event": "local_agent_bpmis_call_done"', log_text)
        self.assertIn('"request_timings": {"issue_tree_reporter": 1.2}', log_text)
        self.assertIn('"operation": "list_biz_projects_for_pm_email"', log_text)
        self.assertIn('"has_access_token": true', log_text)
        self.assertNotIn("user-token", log_text)

    def test_signed_bpmis_call_rehydrates_dataclass_arguments(self):
        class FakeBPMISClient:
            def __init__(self, settings, access_token=None):
                pass

            def create_jira_ticket(self, project, fields, *, preformatted_summary=False):
                self.project = project
                self.fields = fields
                self.preformatted_summary = preformatted_summary
                return CreatedTicket(ticket_key="AF-1", ticket_link="https://jira/AF-1", raw={"ok": True})

        with patch("bpmis_jira_tool.local_agent_server.BPMISDirectApiClient", FakeBPMISClient):
            response = self._post_signed(
                "/api/local-agent/bpmis/call",
                {
                    "operation": "create_jira_ticket",
                    "access_token": "user-token",
                    "args": [{"project_id": "225159", "raw": {"issueId": "225159"}}, {"Summary": "Test"}],
                    "kwargs": {"preformatted_summary": True},
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["result"]["ticket_key"], "AF-1")

    def test_unknown_bpmis_operation_is_rejected(self):
        response = self._post_signed(
            "/api/local-agent/bpmis/call",
            {"operation": "_api_request", "args": []},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["status"], "error")

    def test_unexpected_bpmis_error_returns_json(self):
        class FakeBPMISClient:
            def __init__(self, settings, access_token=None):
                pass

            def list_biz_projects_for_pm_email(self, email):
                raise RuntimeError("upstream exploded")

        with patch("bpmis_jira_tool.local_agent_server.BPMISDirectApiClient", FakeBPMISClient):
            response = self._post_signed(
                "/api/local-agent/bpmis/call",
                {
                    "operation": "list_biz_projects_for_pm_email",
                    "access_token": "user-token",
                    "args": ["pm@npt.sg"],
                },
            )

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")
        self.assertIn("upstream exploded", payload["message"])

    def test_signed_storage_and_memory_contract_routes_return_stable_payloads(self):
        class FakeWorkMemoryStore:
            def health(self):
                return {"status": "ok", "item_count": 1}

            def query_work_memory(self, **kwargs):
                return [{"summary": kwargs["query"], "owner_email": kwargs["owner_email"]}]

            def review_candidates(self, **kwargs):
                return [{"item_id": "review-1"}]

            def project_timeline(self, **kwargs):
                return [{"project_ref": kwargs["project_ref"]}]

            def resolve_work_entity(self, **kwargs):
                return {"status": "ok", "entities": [{"name": kwargs["query"]}]}

            def record_memory_feedback(self, **kwargs):
                return {"status": "ok", "item_id": kwargs["item_id"], "action": kwargs["action"]}

            def distill_work_memory(self, **kwargs):
                return {"distilled": True, "owner_email": kwargs["owner_email"]}

            def record_ingestion_run(self, **kwargs):
                return {"run_id": "ingest-1", **kwargs}

            def superagent_health(self, **kwargs):
                return {"status": "ok", "owner_email": kwargs["owner_email"]}

            def query_superagent_context(self, **kwargs):
                return {"items": [{"summary": kwargs["query"]}]}

            def generate_llm_superagent_answer(self, **kwargs):
                return {"answer": "Use the cited memory.", "confidence": "high"}

            def record_superagent_audit_log(self, **kwargs):
                return {"audit_id": "audit-1", "query": kwargs["query"]}

            def explain_superagent_answer(self, **kwargs):
                return {"status": "ok", "explanation": kwargs["query"]}

            def run_superagent_eval_cases(self, **kwargs):
                return {"status": "ok", "total": 1, "failed": 0}

            def run_superagent_quality_gate(self, **kwargs):
                return {"status": "pass", "total": kwargs["min_cases"]}

            def superagent_audit_log(self, **kwargs):
                return [{"audit_id": "audit-1"}]

        class FakeSeaTalkService:
            def build_overview(self):
                return {"messages": 1}

            def build_insights(self, **kwargs):
                return {"insights": [{"todo_since": kwargs.get("todo_since")}]}

            def build_project_updates(self):
                return {"updates": [{"project": "AF"}]}

            def build_todos(self, **kwargs):
                return {"todos": [{"id": "todo-1"}]}

            def build_name_mappings(self, **kwargs):
                return {"mappings": {"group-1": "Risk PM"}, "force_refresh": kwargs.get("force_refresh")}

            def export_history_text(self):
                return "history", "history.txt"

            def export_history_since(self, **kwargs):
                return f"history since {kwargs['since'].date().isoformat()}"

        class FakeTodoStore:
            def completed_ids(self, **kwargs):
                return {"todo-2", "todo-1"}

            def open_todos(self, **kwargs):
                return [{"id": "todo-1"}]

            def processed_until(self, **kwargs):
                return "2026-05-01T00:00:00Z"

            def mark_processed_until(self, **kwargs):
                self.last_processed = kwargs["processed_until"]

            def merge_open_todos(self, **kwargs):
                return kwargs["todos"] or [{"id": "todo-1"}]

            def mark_completed(self, **kwargs):
                return {"status": "ok", "todo": kwargs["todo"]}

        class FakeNameMappingStore:
            def mappings(self):
                return {"group-1": "Risk PM"}

            def merge_mappings(self, mappings):
                return {**mappings, "group-1": "Risk PM"}

        self.app.config["WORK_MEMORY_STORE"] = FakeWorkMemoryStore()
        patchers = [
            patch("bpmis_jira_tool.local_agent_server._build_seatalk_service", return_value=FakeSeaTalkService()),
            patch("bpmis_jira_tool.local_agent_server._build_seatalk_todo_store", return_value=FakeTodoStore()),
            patch("bpmis_jira_tool.local_agent_server._build_seatalk_name_mapping_store", return_value=FakeNameMappingStore()),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

        post_cases = [
            ("/api/local-agent/work-memory/health", {}),
            ("/api/local-agent/work-memory/recent", {"owner_email": "Owner@NPT.SG", "query": "risk", "filters": {"source_type": "gmail"}, "limit": 2}),
            ("/api/local-agent/work-memory/review-candidates", {"owner_email": "owner@npt.sg", "limit": 1}),
            ("/api/local-agent/work-memory/project-timeline", {"owner_email": "owner@npt.sg", "project_ref": "AF-1"}),
            ("/api/local-agent/work-memory/entity-resolution", {"owner_email": "owner@npt.sg", "query": "AF-1", "entity_type": "jira"}),
            ("/api/local-agent/work-memory/feedback", {"owner_email": "owner@npt.sg", "item_id": "item-1", "action": "accept"}),
            ("/api/local-agent/work-memory/distill", {"owner_email": "owner@npt.sg", "sources": ["gmail"], "project_refs": ["AF-1"]}),
            ("/api/local-agent/work-memory/backfill-existing", {"owner_email": "owner@npt.sg", "sources": ["meeting_recorder", "team_dashboard"]}),
            ("/api/local-agent/work-memory/ingest-incremental", {"owner_email": "owner@npt.sg", "reconciliation": True}),
            ("/api/local-agent/superagent/health", {"owner_email": "owner@npt.sg"}),
            ("/api/local-agent/superagent/query", {"owner_email": "owner@npt.sg", "user_email": "user@npt.sg", "query": "risk", "task_type": "decision"}),
            ("/api/local-agent/superagent/explain", {"owner_email": "owner@npt.sg", "query": "risk"}),
            ("/api/local-agent/superagent/eval", {"owner_email": "owner@npt.sg", "cases": [{"query": "risk"}], "limit": 1}),
            ("/api/local-agent/superagent/quality-gate", {"owner_email": "owner@npt.sg", "min_cases": 1}),
            ("/api/local-agent/superagent/audit", {"owner_email": "owner@npt.sg"}),
            ("/api/local-agent/source-code-qa/sessions/list", {"owner_email": "owner@npt.sg"}),
            ("/api/local-agent/source-code-qa/sessions/create", {"owner_email": "owner@npt.sg", "pm_team": "AF", "country": "All", "title": "Question"}),
            ("/api/local-agent/source-code-qa/sessions/get", {"owner_email": "owner@npt.sg", "session_id": "missing"}),
            ("/api/local-agent/source-code-qa/sessions/archive", {"owner_email": "owner@npt.sg", "session_id": "missing"}),
            ("/api/local-agent/source-code-qa/sessions/context", {"owner_email": "owner@npt.sg", "session_id": "missing"}),
            ("/api/local-agent/source-code-qa/sessions/append", {"owner_email": "owner@npt.sg", "session_id": "missing", "question": "q", "result": {"answer": "a"}}),
            ("/api/local-agent/source-code-qa/sessions/pending", {"owner_email": "owner@npt.sg", "session_id": "missing", "question": "q", "job_id": "job-1"}),
            ("/api/local-agent/source-code-qa/attachments/save", {"owner_email": "owner@npt.sg", "session_id": "s1", "filename": "a.txt", "content_base64": base64.b64encode(b"a").decode("ascii")}),
            ("/api/local-agent/source-code-qa/attachments/resolve", {"owner_email": "owner@npt.sg", "session_id": "s1", "attachment_ids": []}),
            ("/api/local-agent/source-code-qa/attachments/get", {"owner_email": "owner@npt.sg", "session_id": "s1", "attachment_id": "missing"}),
            ("/api/local-agent/source-code-qa/generated-artifacts/save", {"owner_email": "owner@npt.sg", "session_id": "s1", "pm_team": "AF", "country": "All", "question": "q", "sql": "select 1", "readme": "r"}),
            ("/api/local-agent/source-code-qa/generated-artifacts/get", {"owner_email": "owner@npt.sg", "session_id": "s1", "artifact_id": "missing"}),
            ("/api/local-agent/source-code-qa/runtime-evidence/list", {"pm_team": "AF", "country": "All"}),
            ("/api/local-agent/source-code-qa/runtime-evidence/save", {"pm_team": "AF", "country": "All", "source_type": "log", "uploaded_by": "owner", "filename": "log.txt", "content_base64": base64.b64encode(b"log").decode("ascii")}),
            ("/api/local-agent/source-code-qa/runtime-evidence/resolve", {"pm_team": "AF", "country": "All"}),
            ("/api/local-agent/source-code-qa/runtime-evidence/delete", {"pm_team": "AF", "country": "All", "evidence_id": "missing"}),
            ("/api/local-agent/seatalk/overview", {}),
            ("/api/local-agent/seatalk/insights", {"name_mappings": {"group-1": "Risk PM"}, "todo_since": "2026-05-01"}),
            ("/api/local-agent/seatalk/project-updates", {"name_mappings": {"group-1": "Risk PM"}}),
            ("/api/local-agent/seatalk/todos", {"name_mappings": {"group-1": "Risk PM"}}),
            ("/api/local-agent/seatalk/name-mappings", {"force_refresh": True}),
            ("/api/local-agent/seatalk/todos/completed-ids", {"owner_email": "owner@npt.sg"}),
            ("/api/local-agent/seatalk/todos/open", {"owner_email": "owner@npt.sg"}),
            ("/api/local-agent/seatalk/todos/processed-until", {"owner_email": "owner@npt.sg"}),
            ("/api/local-agent/seatalk/todos/mark-processed-until", {"owner_email": "owner@npt.sg", "processed_until": "2026-05-01"}),
            ("/api/local-agent/seatalk/todos/merge-open", {"owner_email": "owner@npt.sg", "todos": [{"id": "todo-1"}]}),
            ("/api/local-agent/seatalk/todos/complete", {"owner_email": "owner@npt.sg", "todo": {"id": "todo-1"}}),
            ("/api/local-agent/seatalk/name-mappings/store/get", {}),
            ("/api/local-agent/seatalk/name-mappings/store/merge", {"mappings": {"group-2": "Ops"}}),
            ("/api/local-agent/seatalk/export", {"name_mappings": {"group-1": "Risk PM"}, "since": "2026-05-01T00:00:00", "days": 7}),
            ("/api/local-agent/bpmis/config/load", {"user_key": "google:owner@npt.sg"}),
            ("/api/local-agent/bpmis/config/save", {"user_key": "google:owner@npt.sg", "config": {"pm_team": "AF"}}),
            ("/api/local-agent/bpmis/config/migrate", {"from_user_key": "anon:1", "to_user_key": "google:owner@npt.sg"}),
            ("/api/local-agent/bpmis/team-profiles/load", {}),
            ("/api/local-agent/bpmis/team-profiles/save", {"team_key": "AF", "profile": {"member_emails": ["owner@npt.sg"]}}),
            ("/api/local-agent/team-dashboard/config/load", {}),
            ("/api/local-agent/team-dashboard/config/save", {"config": {"teams": {"AF": {}}}}),
            ("/api/local-agent/bpmis/projects/list", {"user_key": "google:owner@npt.sg"}),
            ("/api/local-agent/bpmis/projects/reorder", {"user_key": "google:owner@npt.sg", "bpmis_ids": ["225159"]}),
            ("/api/local-agent/bpmis/projects/upsert", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "project_name": "Risk", "brd_link": "", "market": "ID"}),
            ("/api/local-agent/bpmis/projects/comment", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "pm_comment": "ok"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/add", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_key": "AF-1", "ticket_link": "https://jira/AF-1"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/upsert-synced", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_key": "AF-2", "ticket_link": "https://jira/AF-2"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/status", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_id": "AF-1", "status": "Done"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/version", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_id": "AF-1", "version_name": "26Q2"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/delete", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_id": "AF-1"}),
            ("/api/local-agent/bpmis/projects/delete", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159"}),
        ]

        for path, payload in post_cases:
            with self.subTest(path=path):
                response = self._post_signed(path, payload)
                self.assertIn(response.status_code, {200, 400, 404}, msg=response.get_data(as_text=True))
                self.assertIsInstance(response.get_json(), dict)


class LocalAgentClientTests(unittest.TestCase):
    def test_source_code_query_with_progress_uses_local_async_job(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        progress = []
        responses = [
            {"status": "ok", "job_id": "job-1"},
            {"status": "ok", "state": "running", "stage": "codex_stream", "message": "Reading src/App.java", "current": 0, "total": 0},
            {"status": "ok", "state": "completed", "stage": "completed", "message": "Done", "current": 1, "total": 1, "result": {"summary": "agent answer"}},
        ]

        def fake_request(_method, _path, _payload=None, **_kwargs):
            return responses.pop(0)

        with patch.object(client, "_request", side_effect=fake_request) as request:
            result = client.source_code_qa_query(
                {"question": "where is createIssue"},
                progress_callback=lambda stage, message, current, total: progress.append((stage, message, current, total)),
            )

        self.assertEqual(result["summary"], "agent answer")
        self.assertEqual(progress[0], ("codex_stream", "Reading src/App.java", 0, 0))
        self.assertEqual(request.call_args_list[0].args[1], "/api/local-agent/source-code-qa/query-async")

    def test_health_prefers_proxied_agent_capabilities(self):
        class FakeResponse:
            status_code = 200
            text = '{"status":"ok"}'

            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        with patch(
            "bpmis_jira_tool.local_agent_client._LOCAL_AGENT_SESSION.request",
            return_value=FakeResponse({"status": "ok", "capabilities": {"seatalk_configured": True}}),
        ) as request:
            payload = client.get_health()

        self.assertTrue(payload["capabilities"]["seatalk_configured"])
        self.assertEqual(request.call_args.args[1], "https://portal.example/api/local-agent/healthz")
        self.assertEqual(request.call_args.kwargs["timeout"], (10, 300))

    def test_client_uses_configured_connect_timeout(self):
        class FakeResponse:
            status_code = 200
            text = '{"status":"ok"}'

            def json(self):
                return {"status": "ok", "capabilities": {"source_code_qa": True}}

        client = LocalAgentClient(
            base_url="https://portal.example",
            hmac_secret="shared-secret",
            timeout_seconds=120,
            connect_timeout_seconds=4,
        )
        with patch("bpmis_jira_tool.local_agent_client._LOCAL_AGENT_SESSION.request", return_value=FakeResponse()) as request:
            client.get_health()

        self.assertEqual(request.call_args.kwargs["timeout"], (4, 120))

    def test_seatalk_name_mappings_can_force_refresh(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        with patch.object(client, "_request", return_value={"status": "ok", "unknown_ids": []}) as request:
            payload = client.seatalk_name_mappings(force_refresh=True)

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(request.call_args.args[1], "/api/local-agent/seatalk/name-mappings")
        self.assertEqual(request.call_args.args[2], {"force_refresh": True})

    def test_seatalk_export_since_passes_monthly_highlight_scope(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        with patch.object(client, "_request", return_value={"status": "ok", "content": "history"}) as request:
            content = client.seatalk_export_since(
                since=datetime(2026, 4, 20, 0, 0),
                now=datetime(2026, 5, 4, 0, 0),
                days=15,
                conversation_scope="monthly-highlight",
                name_mappings={"group-1": "Risk Group"},
            )

        self.assertEqual(content, "history")
        self.assertEqual(request.call_args.args[1], "/api/local-agent/seatalk/export")
        self.assertEqual(
            request.call_args.args[2],
            {
                "name_mappings": {"group-1": "Risk Group"},
                "since": "2026-04-20T00:00:00",
                "now": "2026-05-04T00:00:00",
                "days": 15,
                "conversation_scope": "monthly-highlight",
            },
        )

    def test_unreadable_response_includes_status_and_preview(self):
        class FakeResponse:
            status_code = 404
            text = "<!doctype html>\n<title>Not Found</title>"

            def json(self):
                raise ValueError("not json")

        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        with patch("bpmis_jira_tool.local_agent_client._LOCAL_AGENT_SESSION.request", return_value=FakeResponse()):
            with self.assertRaises(ToolError) as context:
                client.bpmis_call(operation="ping", access_token="token")

        message = str(context.exception)
        self.assertIn("HTTP 404 from portal.example", message)
        self.assertIn("<!doctype html>", message)

    def test_ngrok_offline_unreadable_response_is_retried(self):
        class FakeResponse:
            def __init__(self, status_code, text, payload=None):
                self.status_code = status_code
                self.text = text
                self._payload = payload

            def json(self):
                if self._payload is None:
                    raise ValueError("not json")
                return self._payload

        responses = [
            FakeResponse(
                404,
                "The endpoint breeze-lung-clunky.ngrok-free.dev is offline. ERR_NGROK_3200",
            ),
            FakeResponse(200, '{"status":"ok","result":"pong"}', {"status": "ok", "result": "pong"}),
        ]
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        with patch("bpmis_jira_tool.local_agent_client._LOCAL_AGENT_SESSION.request", side_effect=responses) as request, patch(
            "bpmis_jira_tool.local_agent_client.time.sleep"
        ) as sleep_mock:
            result = client.bpmis_call(operation="ping", access_token="token")

        self.assertEqual(result, "pong")
        self.assertEqual(request.call_count, 2)
        sleep_mock.assert_called_once_with(1.0)

    def test_local_agent_client_contract_wrappers_send_expected_paths_and_payload_shapes(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        calls = []

        def fake_request(method, path, payload=None, **kwargs):
            calls.append((method, path, payload, kwargs))
            return {
                "status": "ok",
                "config": {"pm_team": "AF"},
                "profiles": {"AF": {"member_emails": ["owner@npt.sg"]}},
                "profile": {"member_emails": ["owner@npt.sg"]},
                "projects": [{"bpmis_id": "225159"}],
                "ticket": {"ticket_key": "AF-1"},
                "deleted": True,
                "updated": True,
                "result": "stored-id",
                "sessions": [{"session_id": "s1"}],
                "session": {"session_id": "s1"},
                "archived": {"session_id": "s1", "archived": True},
                "context": {"messages": []},
                "attachment": {"id": "att-1"},
                "attachments": [{"id": "att-1"}],
                "artifact": {"id": "artifact-1"},
                "evidence": [{"id": "e1"}] if path.endswith("/list") or path.endswith("/resolve") else {"id": "e1"},
                "content_base64": base64.b64encode(b"payload").decode("ascii"),
                "records": [{"record_id": "r1"}],
                "briefs": [{"id": "brief-1"}],
                "items": [{"id": "item-1"}],
                "completed_ids": ["todo-1", 2],
                "todos": [{"id": "todo-1"}],
                "processed_until": "2026-05-01T00:00:00Z",
                "mappings": {"group-1": "Risk PM"},
            }

        with patch.object(client, "_request", side_effect=fake_request):
            self.assertEqual(client.source_code_qa_config()["config"]["pm_team"], "AF")
            self.assertEqual(client.source_code_qa_save_mapping(pm_team="AF", country="All", repositories=[]), fake_request("", "") | {"status": "ok"})
            self.assertEqual(client.source_code_qa_sync(pm_team="AF", country="All")["status"], "ok")
            self.assertEqual(client.source_code_qa_ensure_synced_today(pm_team="AF", country="All", background=True)["status"], "ok")
            self.assertEqual(client.productization_llm_descriptions(items=[{"id": "1"}]), [{"id": "item-1"}])
            self.assertEqual(client.prd_review({"prd_url": "https://c"}), fake_request("", "") | {"status": "ok"})
            self.assertEqual(client.prd_summary({"prd_url": "https://c"})["status"], "ok")
            self.assertEqual(client.prd_briefing_review({"session_id": "s"})["status"], "ok")
            self.assertEqual(client.prd_self_assessment_review({"prd": "text"})["status"], "ok")
            self.assertEqual(client.prd_self_assessment_summary({"prd": "text"})["status"], "ok")
            self.assertEqual(client.prd_self_assessment_sections({"prd_url": "https://c"})["status"], "ok")
            self.assertEqual(client.prd_self_assessment_latest(owner_key="owner")["status"], "ok")
            self.assertEqual(client.prd_briefing_process_prd({"page_ref": "123"})["status"], "ok")
            self.assertEqual(client.prd_briefing_latest(owner_key="owner")["status"], "ok")
            self.assertEqual(client.prd_briefing_generate_audio({"text": "hello"})["status"], "ok")
            self.assertEqual(client.team_dashboard_monthly_report_draft({"topic": "Risk"})["status"], "ok")
            self.assertEqual(client.team_dashboard_monthly_report_draft_start({"topic": "Risk"})["status"], "ok")
            self.assertEqual(client.team_dashboard_monthly_report_job("job 1")["status"], "ok")
            self.assertEqual(client.team_dashboard_monthly_report_latest_draft()["status"], "ok")
            self.assertEqual(client.team_dashboard_monthly_report_send({"recipient": "pm@npt.sg"})["status"], "ok")
            self.assertEqual(client.team_dashboard_daily_briefs(), [{"id": "brief-1"}])
            self.assertEqual(client.meeting_recorder_diagnostics()["status"], "ok")
            self.assertEqual(client.meeting_recorder_records(owner_email="owner@npt.sg"), [{"record_id": "r1"}])
            self.assertEqual(client.meeting_recorder_record(record_id="r1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.meeting_recorder_start({"owner_email": "owner@npt.sg"})["status"], "ok")
            self.assertEqual(client.meeting_recorder_stop(record_id="r1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.meeting_recorder_signal_check(record_id="r1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.meeting_recorder_process(record_id="r1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.meeting_recorder_process_start(record_id="r1", owner_email="owner@npt.sg", send_email_on_complete=True)["status"], "ok")
            self.assertEqual(client.meeting_recorder_process_job(job_id="job 1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.meeting_recorder_send_email(record_id="r1", owner_email="owner@npt.sg", recipient="pm@npt.sg")["status"], "ok")
            self.assertEqual(client.meeting_recorder_delete(record_id="r1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.meeting_translation_start({"owner_email": "owner@npt.sg"})["status"], "ok")
            self.assertEqual(client.meeting_translation_stop(session_id="s1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.work_memory_health()["status"], "ok")
            self.assertEqual(client.work_memory_recent(owner_email="owner@npt.sg"), [{"id": "item-1"}])
            self.assertEqual(client.work_memory_review_candidates(owner_email="owner@npt.sg"), [{"id": "item-1"}])
            self.assertEqual(client.work_memory_project_timeline(project_ref="AF-1", owner_email="owner@npt.sg"), [{"id": "item-1"}])
            self.assertEqual(client.work_memory_entity_resolution(query="AF-1", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.work_memory_feedback(item_id="i1", action="accept", owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.work_memory_distill(owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.work_memory_backfill_existing(owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.work_memory_ingest_incremental(owner_email="owner@npt.sg", reconciliation=True)["status"], "ok")
            self.assertEqual(client.superagent_health(owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.superagent_query(owner_email="owner@npt.sg", user_email="owner@npt.sg", query="risk")["status"], "ok")
            self.assertEqual(client.superagent_explain(owner_email="owner@npt.sg", query="risk")["status"], "ok")
            self.assertEqual(client.superagent_eval(owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.superagent_quality_gate(owner_email="owner@npt.sg")["status"], "ok")
            self.assertEqual(client.superagent_audit(owner_email="owner@npt.sg"), [{"id": "item-1"}])
            self.assertEqual(client.seatalk_overview()["status"], "ok")
            self.assertEqual(client.seatalk_insights()["status"], "ok")
            self.assertEqual(client.seatalk_project_updates()["status"], "ok")
            self.assertEqual(client.seatalk_todos()["status"], "ok")
            self.assertEqual(client.seatalk_name_mappings(force_refresh=True)["status"], "ok")
            self.assertEqual(client.seatalk_export(), ("", "seatalk-history-last-7-days.txt"))
            self.assertEqual(client.seatalk_export_since(since=datetime(2026, 5, 1), days=7), "")
            self.assertEqual(client.bpmis_config_load(user_key="google:owner")["pm_team"], "AF")
            self.assertEqual(client.bpmis_config_save(user_key="google:owner", config={"pm_team": "AF"})["pm_team"], "AF")
            self.assertIsNone(client.bpmis_config_migrate(from_user_key="anon", to_user_key="google:owner"))
            self.assertEqual(client.bpmis_team_profiles_load()["AF"]["member_emails"], ["owner@npt.sg"])
            self.assertEqual(client.bpmis_team_profile_save(team_key="AF", profile={"member_emails": []})["member_emails"], ["owner@npt.sg"])
            self.assertEqual(client.team_dashboard_config_load()["pm_team"], "AF")
            self.assertEqual(client.team_dashboard_config_save({"pm_team": "AF"})["pm_team"], "AF")
            self.assertEqual(client.bpmis_projects_list(user_key="u"), [{"bpmis_id": "225159"}])
            self.assertEqual(client.bpmis_projects_reorder(user_key="u", bpmis_ids=["225159"]), [{"bpmis_id": "225159"}])
            self.assertEqual(client.bpmis_project_upsert(user_key="u", bpmis_id="225159", project_name="P", brd_link="", market="ID"), "stored-id")
            self.assertTrue(client.bpmis_project_delete(user_key="u", bpmis_id="225159"))
            self.assertTrue(client.bpmis_project_comment_update(user_key="u", bpmis_id="225159", pm_comment="ok"))
            self.assertEqual(client.bpmis_project_ticket_add(user_key="u", bpmis_id="225159")["ticket_key"], "AF-1")
            self.assertEqual(client.bpmis_project_ticket_upsert_synced(user_key="u", bpmis_id="225159")["ticket_key"], "AF-1")
            self.assertTrue(client.bpmis_project_ticket_delete(user_key="u", bpmis_id="225159", ticket_id="1"))
            self.assertTrue(client.bpmis_project_ticket_status_update(user_key="u", bpmis_id="225159", ticket_id="1", status="Done"))
            self.assertTrue(client.bpmis_project_ticket_version_update(user_key="u", bpmis_id="225159", ticket_id="1", version_name="26Q2"))
            self.assertEqual(client.source_code_qa_sessions_list(owner_email="owner@npt.sg"), [{"session_id": "s1"}])
            self.assertEqual(client.source_code_qa_session_create(owner_email="owner@npt.sg")["session_id"], "s1")
            self.assertEqual(client.source_code_qa_session_get(session_id="s1", owner_email="owner@npt.sg")["session_id"], "s1")
            self.assertTrue(client.source_code_qa_session_archive(session_id="s1", owner_email="owner@npt.sg")["archived"])
            self.assertEqual(client.source_code_qa_session_context(session_id="s1", owner_email="owner@npt.sg")["messages"], [])
            self.assertEqual(client.source_code_qa_session_append(session_id="s1", owner_email="owner@npt.sg")["session_id"], "s1")
            self.assertEqual(client.source_code_qa_session_pending(session_id="s1", owner_email="owner@npt.sg")["session_id"], "s1")
            self.assertEqual(client.source_code_qa_attachment_save(owner_email="owner@npt.sg", session_id="s1", filename="a.txt", mime_type="text/plain", content=b"x")["id"], "att-1")
            self.assertEqual(client.source_code_qa_attachments_resolve(owner_email="owner@npt.sg", session_id="s1", attachment_ids=["att-1"]), [{"id": "att-1"}])
            metadata, content = client.source_code_qa_attachment_get(owner_email="owner@npt.sg", session_id="s1", attachment_id="att-1")
            self.assertEqual(metadata["id"], "att-1")
            self.assertEqual(content, b"payload")
            self.assertEqual(client.source_code_qa_generated_artifact_save(owner_email="owner@npt.sg", session_id="s1", pm_team="AF", country="All", question="q", sql="select 1", readme="r")["id"], "artifact-1")
            artifact, artifact_content = client.source_code_qa_generated_artifact_get(owner_email="owner@npt.sg", session_id="s1", artifact_id="artifact-1")
            self.assertEqual(artifact["id"], "artifact-1")
            self.assertEqual(artifact_content, b"payload")
            self.assertEqual(client.source_code_qa_runtime_evidence_list(pm_team="AF", country="All"), [{"id": "e1"}])
            self.assertEqual(client.source_code_qa_runtime_evidence_save(pm_team="AF", country="All", source_type="log", uploaded_by="owner", filename="log.txt", mime_type="text/plain", content=b"x")["id"], "e1")
            self.assertEqual(client.source_code_qa_runtime_evidence_resolve(pm_team="AF", country="All"), [{"id": "e1"}])
            self.assertTrue(client.source_code_qa_runtime_evidence_delete(pm_team="AF", country="All", evidence_id="e1"))
            self.assertEqual(client.seatalk_todos_completed_ids(owner_email="owner@npt.sg"), ["todo-1", "2"])
            self.assertEqual(client.seatalk_todos_open(owner_email="owner@npt.sg"), [{"id": "todo-1"}])
            self.assertEqual(client.seatalk_todos_processed_until(owner_email="owner@npt.sg"), "2026-05-01T00:00:00Z")
            self.assertIsNone(client.seatalk_todos_mark_processed_until(owner_email="owner@npt.sg", processed_until="2026-05-01T00:00:00Z"))
            self.assertEqual(client.seatalk_todos_merge_open(owner_email="owner@npt.sg", todos=[]), [{"id": "todo-1"}])
            self.assertEqual(client.seatalk_todo_complete(owner_email="owner@npt.sg", todo={"id": "todo-1"})["status"], "ok")
            self.assertEqual(client.seatalk_name_mappings_get(), {"group-1": "Risk PM"})
            self.assertEqual(client.seatalk_name_mappings_merge({"group-1": "Risk PM"}), {"group-1": "Risk PM"})

        paths = [path for _method, path, _payload, _kwargs in calls]
        self.assertIn("/api/local-agent/source-code-qa/config", paths)
        self.assertIn("/api/local-agent/meeting-recorder/start", paths)
        self.assertIn("/api/local-agent/work-memory/recent", paths)
        self.assertIn("/api/local-agent/bpmis/config/save", paths)
        self.assertIn("/api/local-agent/source-code-qa/runtime-evidence/save", paths)

    def test_remote_bpmis_client_exposes_live_jira_operations(self):
        calls = []

        class FakeClient:
            last_bpmis_request_stats = {"api_call_count": 2}
            last_bpmis_request_timings = {"issue_tree_reporter": 1.2}

            def bpmis_call(self, *, operation, access_token, args=None, kwargs=None):
                calls.append((operation, access_token, args or [], kwargs or {}))
                if operation == "get_jira_ticket_detail":
                    return {"status": {"label": "In Progress"}}
                if operation == "search_biz_projects_by_title_keywords":
                    return [{"bpmis_id": "221664", "project_name": "Project Match"}]
                if operation == "search_versions":
                    return [{"name": "Planning_26Q2"}]
                if operation == "get_brd_doc_links_for_projects":
                    return {"221664": ["https://brd"]}
                if operation == "update_jira_ticket_status":
                    return {"status": {"label": "Testing"}}
                if operation == "update_jira_ticket_fix_version":
                    return {"fixVersions": ["Planning_26Q4"]}
                if operation == "link_jira_ticket_to_project":
                    return {"parentIds": [221664]}
                if operation == "delink_jira_ticket_from_project":
                    return {"parentIds": []}
                return None

        from bpmis_jira_tool.local_agent_client import RemoteBPMISClient

        remote = RemoteBPMISClient(FakeClient(), access_token="token")

        self.assertEqual(remote.get_jira_ticket_detail("SPDBP-95742")["status"]["label"], "In Progress")
        self.assertEqual(remote.search_biz_projects_by_title_keywords("Project Match", max_pages=2)[0]["bpmis_id"], "221664")
        self.assertEqual(remote.search_versions("Planning")[0]["name"], "Planning_26Q2")
        self.assertEqual(remote.get_brd_doc_links_for_projects(["221664"])["221664"], ["https://brd"])
        self.assertEqual(remote.update_jira_ticket_status("SPDBP-95742", "Testing")["status"]["label"], "Testing")
        self.assertEqual(remote.update_jira_ticket_fix_version("SPDBP-95742", "Planning_26Q4")["fixVersions"], ["Planning_26Q4"])
        self.assertEqual(remote.link_jira_ticket_to_project("SPDBP-95742", "221664")["parentIds"], [221664])
        self.assertEqual(remote.delink_jira_ticket_from_project("SPDBP-95742", "221664")["parentIds"], [])
        self.assertEqual(calls[0], ("get_jira_ticket_detail", "token", ["SPDBP-95742"], {}))
        self.assertGreaterEqual(remote.request_stats["api_call_count"], 2)
        self.assertGreaterEqual(remote.request_timings["issue_tree_reporter"], 1.2)

    def test_remote_source_code_qa_service_reuses_config_payload_within_instance(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def source_code_qa_config(self, *, llm_provider=None):
                self.calls += 1
                return {
                    "status": "ok",
                    "config": {"mappings": {"AF:All": []}},
                    "llm_ready": True,
                    "git_auth_ready": True,
                    "llm_policy": {"provider": llm_provider},
                    "index_health": {"status": "ok"},
                    "domain_knowledge": {"enabled": True},
                }

            def source_code_qa_save_mapping(self, *, pm_team, country, repositories):
                return {"status": "ok", "pm_team": pm_team, "country": country, "repositories": repositories}

        class FakeFallbackService:
            llm_provider_name = "codex_cli_bridge"
            llm_budgets = {}

            def normalize_query_llm_provider(self, llm_provider):
                return llm_provider or self.llm_provider_name

            def llm_policy_payload(self):
                return {"provider": "fallback"}

            def with_llm_provider(self, llm_provider):
                return self

        from bpmis_jira_tool.local_agent_client import RemoteSourceCodeQAService

        client = FakeClient()
        remote = RemoteSourceCodeQAService(client, FakeFallbackService(), llm_provider="codex_cli_bridge")

        self.assertTrue(remote.llm_ready())
        self.assertTrue(remote.git_auth_ready())
        self.assertEqual(remote.llm_policy_payload()["provider"], "codex_cli_bridge")
        self.assertEqual(remote.index_health_payload()["status"], "ok")
        self.assertTrue(remote.domain_knowledge_payload()["enabled"])
        self.assertIn("AF:All", remote.load_config()["mappings"])
        self.assertEqual(client.calls, 1)
        remote.save_mapping(pm_team="AF", country="All", repositories=[])
        self.assertIn("AF:All", remote.load_config()["mappings"])
        self.assertEqual(client.calls, 2)


class LocalAgentBPMISClientSelectionTests(unittest.TestCase):
    def test_build_bpmis_client_uses_local_agent_when_enabled(self):
        settings = Settings(
            flask_secret_key="secret",
            google_oauth_client_secret_file="/tmp/client.json",
            google_oauth_redirect_uri=None,
            team_portal_host="127.0.0.1",
            team_portal_port=5000,
            team_portal_base_url=None,
            team_allowed_emails=(),
            team_allowed_email_domains=(),
            team_portal_data_dir="/workspace/team-portal-runtime",
            spreadsheet_id="sheet",
            common_tab_name="Common",
            input_tab_name="Sheet1",
            bpmis_base_url="https://bpmis.example",
            bpmis_api_access_token=None,
            local_agent_base_url="https://agent.example",
            local_agent_hmac_secret="shared-secret",
            local_agent_bpmis_enabled=True,
            bpmis_call_mode="local_agent",
        )

        client = build_bpmis_client(settings, access_token="user-token")

        self.assertEqual(client.__class__.__name__, "RemoteBPMISClient")


if __name__ == "__main__":
    unittest.main()
