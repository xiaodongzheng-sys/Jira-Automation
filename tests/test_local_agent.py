import json
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_client import LocalAgentClient
from bpmis_jira_tool.local_agent_protocol import sign_headers, verify_signature
from bpmis_jira_tool.local_agent_server import create_local_agent_app
from bpmis_jira_tool.models import CreatedTicket
from bpmis_jira_tool.service import build_bpmis_client
from bpmis_jira_tool.config import Settings


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

    def test_seatalk_service_uses_agent_daily_cache_dir(self):
        from bpmis_jira_tool.local_agent_server import _build_seatalk_service

        service = _build_seatalk_service(Settings.from_env())

        self.assertEqual(str(service.daily_cache_dir), os.path.join(self.temp_dir.name, "seatalk", "cache"))

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

    def test_signed_meeting_recorder_repair_video_is_unsupported(self):
        fake_runtime = Mock()
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime

        response = self._post_signed(
            "/api/local-agent/meeting-recorder/repair-video",
            {"record_id": "meeting-1", "owner_email": "owner@npt.sg"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("audio-only", response.get_json()["message"])
        fake_runtime.repair_video_playback.assert_not_called()

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

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
            )
            payload = response.get_json()
            completed = self._wait_for_meeting_process_job(payload["job_id"])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "queued")
        self.assertEqual(completed["state"], "completed")
        fake_processing.process_recording.assert_called_once_with(
            record_id=record["record_id"],
            owner_email="owner@npt.sg",
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
