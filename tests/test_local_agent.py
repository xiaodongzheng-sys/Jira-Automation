import base64
import json
import os
import tempfile
import threading
import time
import unittest
import zipfile
import io
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from cryptography.fernet import Fernet

import bpmis_jira_tool.local_agent_server as local_agent_server
from bpmis_jira_tool.daily_brief_archive import DailyBriefArchiveStore, daily_brief_archive_path
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.job_store import JobStore
from bpmis_jira_tool.local_agent_client import LocalAgentClient, _is_transient_unreadable_local_agent_response
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

    def test_vpn_snapshot_save_delete_and_disconnect_tolerate_host_lookup_failure(self):
        class FakeCiscoVPNClient:
            def __init__(self):
                self.connect_calls = 0
                self.disconnect_calls = 0

            def status(self):
                return {"status": "ok", "connected": False, "state": "Disconnected"}

            def hosts(self):
                raise ToolError("vpn host lookup failed")

            def connect(self, **kwargs):
                self.connect_calls += 1
                return {"status": "ok", "connected": True, "state": "Connected"}

            def disconnect(self):
                self.disconnect_calls += 1
                return {"status": "ok", "connected": False, "state": "Disconnected"}

        self.app.config["VPN_PROFILE_STORE"] = VPNProfileStore(
            Path(self.temp_dir.name) / "vpn.db",
            encryption_key=Fernet.generate_key().decode("utf-8"),
        )
        fake_cisco = FakeCiscoVPNClient()
        self.app.config["CISCO_VPN_CLIENT"] = fake_cisco

        profiles_response = self._get_signed("/api/local-agent/vpn/profiles")
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
        connect_response = self._post_signed(
            f"/api/local-agent/vpn/profiles/{profile_id}/connect",
            {"second_password": "second-secret"},
        )
        delete_response = self.app.test_client().delete(
            f"/api/local-agent/vpn/profiles/{profile_id}",
            headers=sign_headers(
                secret="shared-secret",
                method="DELETE",
                path=f"/api/local-agent/vpn/profiles/{profile_id}",
                body=b"",
            ),
        )
        disconnect_response = self._post_signed("/api/local-agent/vpn/disconnect", {})

        self.assertEqual(profiles_response.status_code, 200)
        self.assertEqual(profiles_response.get_json()["hosts"], [])
        self.assertEqual(save_response.status_code, 200)
        self.assertEqual(save_response.get_json()["hosts"], [])
        self.assertEqual(connect_response.status_code, 200)
        self.assertEqual(connect_response.get_json()["hosts"], [])
        self.assertNotIn("vpn-secret", str(save_response.get_json()))
        self.assertEqual(delete_response.status_code, 200)
        self.assertEqual(delete_response.get_json()["profiles"], [])
        self.assertEqual(disconnect_response.status_code, 200)
        self.assertEqual(disconnect_response.get_json()["hosts"], [])
        self.assertEqual(fake_cisco.connect_calls, 1)
        self.assertEqual(fake_cisco.disconnect_calls, 1)

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

    def test_signed_source_code_config_save_sync_and_direct_ensure_routes(self):
        with patch("bpmis_jira_tool.source_code_qa.SourceCodeQAService.llm_ready", return_value=True), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.llm_policy_payload",
            return_value={"provider": "codex_cli_bridge"},
        ), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.index_health_payload",
            return_value={"status": "ok"},
        ), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.domain_knowledge_payload",
            return_value={"enabled": True},
        ), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.load_config",
            return_value={"mappings": {"AF:All": []}},
        ), patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.save_mapping",
            return_value={"saved": True},
        ) as save_mapping, patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.sync",
            return_value={"synced": True},
        ) as sync, patch(
            "bpmis_jira_tool.source_code_qa.SourceCodeQAService.ensure_synced_today",
            return_value={"attempted": True, "status": "ok"},
        ) as ensure:
            config_response = self._post_signed(
                "/api/local-agent/source-code-qa/config",
                {"llm_provider": "codex_cli_bridge"},
            )
            save_response = self._post_signed(
                "/api/local-agent/source-code-qa/config/save",
                {"pm_team": "AF", "country": "All", "repositories": [{"name": "repo"}]},
            )
            sync_response = self._post_signed(
                "/api/local-agent/source-code-qa/sync",
                {"pm_team": "AF", "country": "All"},
            )
            ensure_response = self._post_signed(
                "/api/local-agent/source-code-qa/ensure-synced-today",
                {"pm_team": "AF", "country": "All", "background": False},
            )

        self.assertEqual(config_response.status_code, 200)
        self.assertTrue(config_response.get_json()["llm_ready"])
        self.assertEqual(config_response.get_json()["config"]["mappings"], {"AF:All": []})
        self.assertEqual(save_response.get_json()["saved"], True)
        self.assertEqual(sync_response.get_json()["synced"], True)
        self.assertEqual(ensure_response.get_json()["attempted"], True)
        save_mapping.assert_called_once_with(pm_team="AF", country="All", repositories=[{"name": "repo"}])
        sync.assert_called_once_with(pm_team="AF", country="All")
        ensure.assert_called_once_with(pm_team="AF", country="All")

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

    def test_missing_source_code_async_query_job_returns_not_found(self):
        response = self._get_signed("/api/local-agent/source-code-qa/query-jobs/missing-job")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["status"], "error")
        self.assertIn("not found", response.get_json()["message"])

    def test_source_code_query_job_cleanup_tolerates_bad_updated_at(self):
        from bpmis_jira_tool.local_agent_server import _snapshot_query_job

        with self.app.app_context():
            jobs = self.app.config["SOURCE_CODE_QA_QUERY_JOBS"]
            jobs["bad-updated-at"] = {"state": "completed", "updated_at": {"not": "a-number"}}

            self.assertIsNone(_snapshot_query_job("bad-updated-at"))

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

    def test_source_code_auto_sync_reports_duplicate_and_logs_background_failure(self):
        class FailingThread:
            def __init__(self, *, target, daemon):
                self.target = target
                self.daemon = daemon

            def start(self):
                self.target()

        class FakeService:
            def mapping_key(self, pm_team, country):
                return f"{pm_team}:{country}"

            def ensure_synced_today(self, **kwargs):
                raise RuntimeError(f"sync failed for {kwargs['pm_team']}")

        fake_service = FakeService()
        with self.app.app_context():
            local_agent_server._SOURCE_CODE_QA_AUTO_SYNC_KEYS.clear()
            local_agent_server._SOURCE_CODE_QA_AUTO_SYNC_KEYS.add("AF:All")
            duplicate = local_agent_server._queue_source_code_qa_auto_sync(fake_service, pm_team="AF", country="All")
            local_agent_server._SOURCE_CODE_QA_AUTO_SYNC_KEYS.clear()
            with patch("bpmis_jira_tool.local_agent_server.threading.Thread", side_effect=lambda target, daemon: FailingThread(target=target, daemon=daemon)), self.assertLogs(self.app.logger.name, level="ERROR") as captured:
                failed = local_agent_server._queue_source_code_qa_auto_sync(fake_service, pm_team="AF", country="All")

        self.assertEqual(duplicate["status"], "background_running")
        self.assertEqual(failed["status"], "background_queued")
        self.assertIn("background auto-sync failed", "\n".join(captured.output))
        self.assertNotIn("AF:All", local_agent_server._SOURCE_CODE_QA_AUTO_SYNC_KEYS)

    def test_signed_productization_llm_descriptions_rejects_non_list_items(self):
        response = self._post_signed(
            "/api/local-agent/productization/llm-descriptions",
            {"items": {"not": "a-list"}},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("items must be a list", response.get_json()["message"])

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

    def test_signed_prd_review_summary_briefing_sections_and_latest_routes(self):
        class FakePRDReviewService:
            def __init__(self):
                self.review_request = None
                self.summary_request = None
                self.briefing_request = None
                self.sections_request = None

            def review(self, request):
                self.review_request = request
                return {"status": "ok", "review": {"result_markdown": "Team review"}}

            def summarize(self, request):
                self.summary_request = request
                return {"status": "ok", "summary": "Team summary"}

            def review_url(self, request):
                self.briefing_request = request
                return {"status": "ok", "review": {"result_markdown": "Briefing review"}, "language": request.language}

            def summarize_url(self, request):
                self.summary_request = request
                return {"status": "ok", "summary": "Self-assessment summary", "language": request.language}

            def list_url_sections(self, request):
                self.sections_request = request
                return {"status": "ok", "sections": [{"index": 1, "title": "Scope"}]}

        fake_service = FakePRDReviewService()
        with patch("bpmis_jira_tool.local_agent_server._build_prd_review_service", return_value=fake_service):
            review_response = self._post_signed(
                "/api/local-agent/prd-review",
                {
                    "owner_key": "google:owner@npt.sg",
                    "jira_id": "AF-1",
                    "jira_link": "https://jira/AF-1",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "force_refresh": True,
                    "google_credentials": {"token": "drive-token"},
                },
            )
            summary_response = self._post_signed(
                "/api/local-agent/prd-summary",
                {
                    "owner_key": "google:owner@npt.sg",
                    "jira_id": "AF-1",
                    "jira_link": "https://jira/AF-1",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "force_refresh": True,
                },
            )
            briefing_response = self._post_signed(
                "/api/local-agent/prd-briefing-review",
                {
                    "owner_key": "google:owner@npt.sg",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "language": "en",
                    "force_refresh": True,
                },
            )
            sections_response = self._post_signed(
                "/api/local-agent/prd-self-assessment/sections",
                {
                    "owner_key": "google:owner@npt.sg",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "language": "en",
                },
            )
            self_summary_response = self._post_signed(
                "/api/local-agent/prd-self-assessment/summary",
                {
                    "owner_key": "google:owner@npt.sg",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "language": "en",
                    "force_refresh": True,
                },
            )
            missing_latest_response = self._post_signed(
                "/api/local-agent/prd-self-assessment/latest",
                {"owner_key": "missing-owner"},
            )
            latest_response = self._post_signed(
                "/api/local-agent/prd-self-assessment/latest",
                {"owner_key": "google:owner@npt.sg"},
            )

        self.assertEqual(review_response.status_code, 200)
        self.assertEqual(summary_response.status_code, 200)
        self.assertEqual(briefing_response.status_code, 200)
        self.assertEqual(sections_response.status_code, 200)
        self.assertEqual(self_summary_response.status_code, 200)
        self.assertEqual(review_response.get_json()["review"]["result_markdown"], "Team review")
        self.assertEqual(summary_response.get_json()["summary"], "Team summary")
        self.assertEqual(briefing_response.get_json()["language"], "en")
        self.assertEqual(sections_response.get_json()["sections"][0]["title"], "Scope")
        self.assertEqual(self_summary_response.get_json()["summary"], "Self-assessment summary")
        self.assertIsNone(missing_latest_response.get_json()["latest"])
        self.assertEqual(latest_response.get_json()["latest"]["payload"]["action"], "summary")
        self.assertEqual(fake_service.review_request.google_credentials["token"], "drive-token")
        self.assertTrue(fake_service.summary_request.force_refresh)
        self.assertEqual(fake_service.briefing_request.language, "en")
        self.assertEqual(fake_service.sections_request.language, "en")

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

    def test_signed_prd_briefing_process_generate_audio_and_latest_routes(self):
        class FakeStore:
            root_dir = Path(self.temp_dir.name) / "prd-briefing"

        class FakeBriefingService:
            def __init__(self):
                self.store = FakeStore()
                self.process_payload = None
                self.audio_payload = None

            def process_prd_for_presentation(self, **kwargs):
                self.process_payload = kwargs
                return {"status": "ok", "session": {"session_id": "briefing-session"}, "chunks": []}

            def generate_presentation_audio(self, **kwargs):
                self.audio_payload = kwargs
                return {"status": "ok", "chunk": {"id": "chunk-1", "audioUrl": "/external/audio.mp3"}}

        fake_service = FakeBriefingService()
        with patch("bpmis_jira_tool.local_agent_server._build_prd_briefing_service", return_value=fake_service):
            missing_latest_response = self._post_signed(
                "/api/local-agent/prd-briefing/latest",
                {"owner_key": "google:owner@npt.sg"},
            )
            process_response = self._post_signed(
                "/api/local-agent/prd-briefing/process-prd",
                {
                    "owner_key": "google:owner@npt.sg",
                    "prd_url": "https://confluence.shopee.io/display/SPDB/PRD",
                    "text": "fallback PRD text",
                    "language": "en",
                },
            )
            latest_response = self._post_signed(
                "/api/local-agent/prd-briefing/latest",
                {"owner_key": "google:owner@npt.sg"},
            )
            audio_response = self._post_signed(
                "/api/local-agent/prd-briefing/generate-audio",
                {
                    "owner_key": "google:owner@npt.sg",
                    "sessionId": "briefing-session",
                    "id": "chunk-1",
                    "content": "Narration",
                },
            )
            missing_src_response = self._post_signed("/api/local-agent/prd-briefing/image-proxy", {"src": ""})

        self.assertEqual(missing_latest_response.get_json()["latest"], None)
        self.assertEqual(process_response.status_code, 200)
        self.assertEqual(process_response.get_json()["session"]["session_id"], "briefing-session")
        self.assertEqual(latest_response.get_json()["latest"]["payload"]["payload"]["session"]["session_id"], "briefing-session")
        self.assertEqual(audio_response.status_code, 200)
        self.assertEqual(audio_response.get_json()["chunk"]["audioUrl"], "/external/audio.mp3")
        self.assertEqual(missing_src_response.status_code, 400)
        self.assertEqual(fake_service.process_payload["page_ref"], "https://confluence.shopee.io/display/SPDB/PRD")
        self.assertEqual(fake_service.audio_payload["session_id"], "briefing-session")

    def test_signed_prd_job_missing_and_async_summary_routes(self):
        class ImmediateThread:
            def __init__(self, *, target, args=(), daemon=False):
                self.target = target
                self.args = args

            def start(self):
                self.target(*self.args)

        class FakePRDReviewService:
            def review(self, request):
                return {"status": "ok", "review": {"result_markdown": "Async team review"}}

            def summarize(self, request):
                return {"status": "ok", "summary": "Async team summary"}

            def summarize_url(self, request):
                return {"status": "ok", "summary": "Async self summary"}

        with patch("bpmis_jira_tool.local_agent_server.threading.Thread", side_effect=lambda target, args=(), daemon=False: ImmediateThread(target=target, args=args, daemon=daemon)), patch(
            "bpmis_jira_tool.local_agent_server._build_prd_review_service",
            return_value=FakePRDReviewService(),
        ):
            missing_job_response = self._get_signed("/api/local-agent/prd-jobs/missing-job")
            review_response = self._post_signed(
                "/api/local-agent/prd-review-async",
                {"owner_key": "google:owner@npt.sg", "prd_url": "https://confluence.shopee.io/display/SPDB/PRD"},
            )
            summary_response = self._post_signed(
                "/api/local-agent/prd-summary-async",
                {"owner_key": "google:owner@npt.sg", "prd_url": "https://confluence.shopee.io/display/SPDB/PRD"},
            )
            self_summary_response = self._post_signed(
                "/api/local-agent/prd-self-assessment/summary-async",
                {"owner_key": "google:owner@npt.sg", "prd_url": "https://confluence.shopee.io/display/SPDB/PRD"},
            )
            review_job = self._get_signed(f"/api/local-agent/prd-jobs/{review_response.get_json()['job_id']}").get_json()
            summary_job = self._get_signed(f"/api/local-agent/prd-jobs/{summary_response.get_json()['job_id']}").get_json()
            self_summary_job = self._get_signed(f"/api/local-agent/prd-jobs/{self_summary_response.get_json()['job_id']}").get_json()

        self.assertEqual(missing_job_response.status_code, 200)
        self.assertEqual(missing_job_response.get_json()["error_category"], "server_restart")
        self.assertEqual(review_job["state"], "completed")
        self.assertEqual(review_job["results"][0]["review"]["result_markdown"], "Async team review")
        self.assertEqual(summary_job["results"][0]["summary"], "Async team summary")
        self.assertEqual(self_summary_job["results"][0]["summary"], "Async self summary")

    def test_prd_async_worker_failure_paths_are_json_readable(self):
        class ToolErrorPRDReviewService:
            def review_url(self, request, progress_callback=None):
                raise ToolError("Confluence token=secret is unavailable")

        class TypeErrorPRDReviewService:
            def review_url(self, request, progress_callback=None):
                raise TypeError("bad signature")

        with self.app.app_context():
            job_store = self.app.config["TEAM_DASHBOARD_JOB_STORE"]
            unsupported_job = job_store.create("prd-review", title="Unsupported PRD Job")
            with patch(
                "bpmis_jira_tool.local_agent_server._build_prd_review_service",
                return_value=Mock(),
            ):
                local_agent_server._run_prd_job(
                    self.app,
                    unsupported_job.job_id,
                    {"owner_key": "google:owner@npt.sg", "prd_url": "https://confluence.shopee.io/display/SPDB/PRD"},
                    "unsupported",
                )
            unsupported_snapshot = job_store.snapshot(unsupported_job.job_id)

            tool_error_job = job_store.create("prd-review", title="Tool Error PRD Job")
            with patch(
                "bpmis_jira_tool.local_agent_server._build_prd_review_service",
                return_value=ToolErrorPRDReviewService(),
            ):
                local_agent_server._run_prd_job(
                    self.app,
                    tool_error_job.job_id,
                    {"owner_key": "google:owner@npt.sg", "prd_url": "https://confluence.shopee.io/display/SPDB/PRD"},
                    "self_review",
                )
            tool_error_snapshot = job_store.snapshot(tool_error_job.job_id)

            unexpected_job = job_store.create("prd-review", title="Unexpected PRD Job")
            with patch(
                "bpmis_jira_tool.local_agent_server._build_prd_review_service",
                return_value=TypeErrorPRDReviewService(),
            ):
                local_agent_server._run_prd_job(
                    self.app,
                    unexpected_job.job_id,
                    {"owner_key": "google:owner@npt.sg", "prd_url": "https://confluence.shopee.io/display/SPDB/PRD"},
                    "self_review",
                )
            unexpected_snapshot = job_store.snapshot(unexpected_job.job_id)

        self.assertEqual(unsupported_snapshot["state"], "failed")
        self.assertEqual(unsupported_snapshot["error_code"], "prd_job_failed")
        self.assertIn("Unsupported PRD job action", unsupported_snapshot["error"])
        self.assertEqual(tool_error_snapshot["state"], "failed")
        self.assertEqual(tool_error_snapshot["error_category"], "prd_job_failed")
        self.assertIn("token=secret", tool_error_snapshot["error"])
        self.assertEqual(unexpected_snapshot["state"], "failed")
        self.assertEqual(unexpected_snapshot["error_code"], "prd_job_unexpected_error")
        self.assertIn("unexpectedly", unexpected_snapshot["error"])

    def test_seatalk_service_uses_agent_daily_cache_dir(self):
        from bpmis_jira_tool.local_agent_server import _build_seatalk_service

        service = _build_seatalk_service(Settings.from_env())

        self.assertEqual(str(service.daily_cache_dir), os.path.join(self.temp_dir.name, "seatalk", "cache"))
        self.assertEqual(service.codex_model, "gpt-5.6")

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
        missing_download_response = self._get_signed("/api/local-agent/team-dashboard/daily-briefs/missing/download")

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
        self.assertEqual(missing_download_response.status_code, 404)
        self.assertIn("not found", missing_download_response.get_json()["message"])

    def test_signed_monthly_report_draft_latest_and_send_routes(self):
        from bpmis_jira_tool.monthly_report import MonthlyReportSendResult

        class FakeMonthlyReportService:
            def __init__(self):
                self.request = None

            def generate_draft(self, **kwargs):
                self.request = kwargs
                return {
                    "status": "ok",
                    "draft_markdown": "## Monthly Report",
                    "subject": "Monthly Report Subject",
                    "generation_summary": {
                        "generation_version": "v2",
                        "period_start": "2026-05-01",
                        "period_end": "2026-05-31",
                        "period_end_exclusive": "2026-06-01",
                        "highlight_topics": ["VPN"],
                    },
                    "evidence_review": [{"topic": "VPN"}],
                    "evidence_debug": [{"source": "test"}],
                }

        fake_service = FakeMonthlyReportService()
        with patch("bpmis_jira_tool.local_agent_server._build_monthly_report_service", return_value=fake_service), patch(
            "bpmis_jira_tool.local_agent_server.send_monthly_report_email",
            return_value=MonthlyReportSendResult(
                status="sent",
                recipient="pm@npt.sg",
                subject="Monthly Report Subject",
                message_id="msg-1",
            ),
        ) as send_email:
            empty_response = self._get_signed("/api/local-agent/team-dashboard/monthly-report/latest-draft")
            draft_response = self._post_signed(
                "/api/local-agent/team-dashboard/monthly-report/draft",
                {
                    "template": "## Template",
                    "team_payloads": [{"team": "AF"}],
                    "report_intelligence_config": {"priority_keywords": ["VPN"]},
                    "period_start": "2026-05-01",
                    "period_end": "2026-05-31",
                    "period_end_exclusive": "2026-06-01",
                    "highlight_topics": ["VPN"],
                    "highlight_topic_sources": {"VPN": ["manual"]},
                    "product_scope": ["AF"],
                    "historical_report_style_guide": {"tone": "concise"},
                },
            )
            job_store = self.app.config["TEAM_DASHBOARD_JOB_STORE"]
            job = job_store.create("team-dashboard-monthly-report-draft", title="Generate Monthly Report Draft")
            job_store.complete(job.job_id, results=[draft_response.get_json()], notice={})
            latest_response = self._get_signed("/api/local-agent/team-dashboard/monthly-report/latest-draft")
            send_response = self._post_signed(
                "/api/local-agent/team-dashboard/monthly-report/send",
                {
                    "recipient": "pm@npt.sg",
                    "subject": "Monthly Report Subject",
                    "draft_markdown": "## Monthly Report",
                },
            )

        self.assertEqual(empty_response.get_json()["status"], "empty")
        self.assertEqual(draft_response.status_code, 200)
        self.assertEqual(fake_service.request["product_scope"], ["AF"])
        self.assertEqual(latest_response.status_code, 200)
        latest_payload = latest_response.get_json()
        self.assertEqual(latest_payload["status"], "ok")
        self.assertEqual(latest_payload["generation_version"], "v2")
        self.assertEqual(latest_payload["highlight_topics"], ["VPN"])
        self.assertEqual(send_response.get_json()["message_id"], "msg-1")
        send_email.assert_called_once()

    def test_signed_monthly_report_async_draft_persists_completed_job(self):
        class ImmediateThread:
            def __init__(self, *, target, args=(), daemon=False):
                self.target = target
                self.args = args

            def start(self):
                self.target(*self.args)

        class FakeMonthlyReportService:
            def __init__(self):
                self.request = None

            def generate_draft(self, **kwargs):
                self.request = kwargs
                kwargs["progress_callback"](
                    "drafting",
                    "Drafting Monthly Report.",
                    1,
                    2,
                    estimated_prompt_tokens=123,
                    token_risk="low",
                )
                return {
                    "status": "ok",
                    "draft_markdown": "## Async Monthly Report",
                    "generation_summary": {"generation_version": "v2"},
                }

        fake_service = FakeMonthlyReportService()
        with patch("bpmis_jira_tool.local_agent_server.threading.Thread", side_effect=lambda target, args=(), daemon=False: ImmediateThread(target=target, args=args, daemon=daemon)), patch(
            "bpmis_jira_tool.local_agent_server._build_monthly_report_service",
            return_value=fake_service,
        ):
            start_response = self._post_signed(
                "/api/local-agent/team-dashboard/monthly-report/draft-async",
                {
                    "template": "## Template",
                    "team_payloads": [{"team": "AF"}, "ignored"],
                    "product_scope": ["AF", 123],
                    "period_start": "2026-05-01",
                },
            )
            job_response = self._get_signed(
                f"/api/local-agent/team-dashboard/monthly-report/jobs/{start_response.get_json()['job_id']}"
            )

        self.assertEqual(start_response.status_code, 200)
        self.assertEqual(job_response.status_code, 200)
        payload = job_response.get_json()
        self.assertEqual(payload["state"], "completed")
        self.assertEqual(payload["results"][0]["draft_markdown"], "## Async Monthly Report")
        self.assertEqual(fake_service.request["team_payloads"], [{"team": "AF"}])
        self.assertEqual(fake_service.request["product_scope"], ["AF", "123"])

    def test_monthly_report_async_worker_failure_paths_are_classified(self):
        class ToolErrorMonthlyReportService:
            def generate_draft(self, **kwargs):
                raise ToolError("LLM rate limit exceeded")

        class UnexpectedMonthlyReportService:
            def generate_draft(self, **kwargs):
                raise RuntimeError("template parser exploded")

        with self.app.app_context():
            job_store = self.app.config["TEAM_DASHBOARD_JOB_STORE"]
            tool_error_job = job_store.create("team-dashboard-monthly-report-draft", title="Generate Monthly Report Draft")
            with patch(
                "bpmis_jira_tool.local_agent_server._build_monthly_report_service",
                return_value=ToolErrorMonthlyReportService(),
            ):
                local_agent_server._run_monthly_report_draft_job(self.app, tool_error_job.job_id, {"template": "## Template"})
            tool_error_snapshot = job_store.snapshot(tool_error_job.job_id)

            unexpected_job = job_store.create("team-dashboard-monthly-report-draft", title="Generate Monthly Report Draft")
            with patch(
                "bpmis_jira_tool.local_agent_server._build_monthly_report_service",
                return_value=UnexpectedMonthlyReportService(),
            ):
                local_agent_server._run_monthly_report_draft_job(self.app, unexpected_job.job_id, {"template": "## Template"})
            unexpected_snapshot = job_store.snapshot(unexpected_job.job_id)

        self.assertEqual(tool_error_snapshot["state"], "failed")
        self.assertEqual(tool_error_snapshot["error_category"], "codex_timeout_or_rate_limit")
        self.assertEqual(tool_error_snapshot["error_code"], "llm_rate_limited")
        self.assertTrue(tool_error_snapshot["error_retryable"])
        self.assertEqual(unexpected_snapshot["state"], "failed")
        self.assertEqual(unexpected_snapshot["error_category"], "unexpected_internal")
        self.assertEqual(unexpected_snapshot["error_code"], "server_error")

    def test_latest_monthly_report_draft_rejects_older_format(self):
        job_store = self.app.config["TEAM_DASHBOARD_JOB_STORE"]
        job = job_store.create("team-dashboard-monthly-report-draft", title="Generate Monthly Report Draft")
        job_store.complete(job.job_id, results=[{"draft_markdown": "Legacy draft"}], notice={})

        response = self._get_signed("/api/local-agent/team-dashboard/monthly-report/latest-draft")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "empty")
        self.assertIn("older format", response.get_json()["message"])

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

    def test_signed_meeting_recorder_direct_routes_delegate_to_runtime_and_processing_service(self):
        record = {
            "record_id": "meeting-direct",
            "owner_email": "owner@npt.sg",
            "title": "Direct Meeting",
            "platform": "zoom",
            "meeting_link": "https://zoom.us/j/123",
            "status": "recorded",
            "transcript_language": "en",
            "media": {},
            "transcript": {"status": "ready"},
            "minutes": {"status": "ready"},
            "email": {"status": "not_sent"},
        }

        class FakeRuntime:
            def __init__(self):
                self.start_payload = None

            def diagnostics(self):
                return {"audio": "ok"}

            def start_recording(self, **kwargs):
                self.start_payload = kwargs
                return {**record, "status": "recording", "recording_mode": kwargs["recording_mode"]}

            def stop_recording(self, **kwargs):
                return {**record, "status": "recorded"}

            def check_recording_signal(self, **kwargs):
                return {**record, "recording_health": {"status": "ok"}}

        class FakeProcessingService:
            def process_recording(self, **kwargs):
                return {**record, "status": "completed", "minutes": {"status": "ready"}}

            def send_minutes_email(self, **kwargs):
                return {"status": "sent", "recipient": kwargs["recipient"]}

        fake_runtime = FakeRuntime()
        self.app.config["MEETING_RECORDER_RUNTIME"] = fake_runtime
        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=FakeProcessingService()):
            diagnostics_response = self._post_signed("/api/local-agent/meeting-recorder/diagnostics", {})
            start_response = self._post_signed(
                "/api/local-agent/meeting-recorder/start",
                {
                    "owner_email": "OWNER@npt.sg",
                    "title": "Direct Meeting",
                    "meetingLink": "https://zoom.us/j/123",
                    "calendarEventId": "cal-1",
                    "scheduledStart": "2026-05-23T10:00:00+08:00",
                    "scheduledEnd": "2026-05-23T11:00:00+08:00",
                    "attendees": ["a@npt.sg"],
                    "transcriptLanguage": "en",
                },
            )
            stop_response = self._post_signed(
                "/api/local-agent/meeting-recorder/stop",
                {"record_id": "meeting-direct", "owner_email": "owner@npt.sg"},
            )
            signal_response = self._post_signed(
                "/api/local-agent/meeting-recorder/signal-check",
                {"record_id": "meeting-direct", "owner_email": "owner@npt.sg"},
            )
            process_response = self._post_signed(
                "/api/local-agent/meeting-recorder/process",
                {"record_id": "meeting-direct", "owner_email": "owner@npt.sg"},
            )
            email_response = self._post_signed(
                "/api/local-agent/meeting-recorder/send-email",
                {"record_id": "meeting-direct", "owner_email": "owner@npt.sg", "recipient": "pm@npt.sg"},
            )

        self.assertEqual(diagnostics_response.get_json()["audio"], "ok")
        self.assertEqual(start_response.get_json()["record"]["status"], "recording")
        self.assertEqual(fake_runtime.start_payload["owner_email"], "owner@npt.sg")
        self.assertEqual(fake_runtime.start_payload["recording_mode"], "audio_only")
        self.assertEqual(fake_runtime.start_payload["calendar_event_id"], "cal-1")
        self.assertEqual(stop_response.get_json()["record"]["status"], "recorded")
        self.assertEqual(signal_response.get_json()["record"]["recording_health"]["status"], "ok")
        self.assertEqual(process_response.get_json()["record"]["minutes_status"], "ready")
        self.assertEqual(email_response.get_json()["email"]["recipient"], "pm@npt.sg")

    def test_signed_meeting_recorder_records_record_delete_and_post_asset(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Asset POST",
            platform="zoom",
            meeting_link="https://zoom.us/j/asset",
        )
        asset_path = store.record_dir(record["record_id"]) / "notes.txt"
        asset_path.write_text("meeting notes", encoding="utf-8")

        records_response = self._post_signed("/api/local-agent/meeting-recorder/records", {"owner_email": "owner@npt.sg"})
        record_response = self._post_signed(
            "/api/local-agent/meeting-recorder/record",
            {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
        )
        asset_response = self._post_signed(
            "/api/local-agent/meeting-recorder/asset",
            {
                "record_id": record["record_id"],
                "owner_email": "owner@npt.sg",
                "relative_path": "notes.txt",
            },
        )
        traversal_response = self._post_signed(
            "/api/local-agent/meeting-recorder/asset",
            {
                "record_id": record["record_id"],
                "owner_email": "owner@npt.sg",
                "relative_path": "../secret.txt",
            },
        )
        missing_asset_response = self._post_signed(
            "/api/local-agent/meeting-recorder/asset",
            {
                "record_id": record["record_id"],
                "owner_email": "owner@npt.sg",
                "relative_path": "missing.txt",
            },
        )
        missing_job_response = self._get_signed_with_query(
            "/api/local-agent/meeting-recorder/process-jobs/missing-job",
            "owner_email=owner@npt.sg",
        )
        delete_response = self._post_signed(
            "/api/local-agent/meeting-recorder/delete",
            {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
        )

        self.assertEqual(records_response.status_code, 200)
        self.assertEqual(records_response.get_json()["records"][0]["record_id"], record["record_id"])
        self.assertEqual(record_response.get_json()["record"]["record_id"], record["record_id"])
        self.assertEqual(asset_response.status_code, 200)
        self.assertEqual(asset_response.data, b"meeting notes")
        asset_response.close()
        self.assertEqual(traversal_response.status_code, 400)
        self.assertIn("Invalid meeting asset path", traversal_response.get_json()["message"])
        self.assertEqual(missing_asset_response.status_code, 400)
        self.assertIn("Meeting asset not found", missing_asset_response.get_json()["message"])
        self.assertEqual(missing_job_response.status_code, 404)
        self.assertEqual(missing_job_response.get_json()["error_category"], "job_not_found")
        self.assertEqual(delete_response.status_code, 200)

    def test_signed_meeting_translation_routes_cover_success_and_failure(self):
        class FakeTranslationRuntime:
            def __init__(self):
                self.fail = False

            def start_session(self, **kwargs):
                if self.fail:
                    raise ToolError("cannot start")
                return {"status": "ok", "session": {"session_id": "translation-1", "status": "running"}}

            def stop_session(self, **kwargs):
                if self.fail:
                    raise ToolError("cannot stop")
                return {"status": "ok", "session": {"session_id": kwargs["session_id"], "status": "stopped"}}

            def event_stream(self, **kwargs):
                if self.fail:
                    raise ToolError("missing stream")
                return iter([{"event": "ready", "data": {"session_id": kwargs["session_id"]}}])

        fake_runtime = FakeTranslationRuntime()
        self.app.config["MEETING_TRANSLATION_RUNTIME"] = fake_runtime
        start_response = self._post_signed(
            "/api/local-agent/meeting-translation/start",
            {"owner_email": "owner@npt.sg", "target_language": "en"},
        )
        stop_response = self._post_signed(
            "/api/local-agent/meeting-translation/stop",
            {"owner_email": "owner@npt.sg", "session_id": "translation-1"},
        )
        events_response = self._get_signed_with_query(
            "/api/local-agent/meeting-translation/events/translation-1",
            "owner_email=owner@npt.sg",
        )

        fake_runtime.fail = True
        failed_start = self._post_signed(
            "/api/local-agent/meeting-translation/start",
            {"owner_email": "owner@npt.sg", "target_language": "en"},
        )
        failed_stop = self._post_signed(
            "/api/local-agent/meeting-translation/stop",
            {"owner_email": "owner@npt.sg", "session_id": "translation-1"},
        )
        failed_events = self._get_signed_with_query(
            "/api/local-agent/meeting-translation/events/translation-1",
            "owner_email=owner@npt.sg",
        )

        self.assertEqual(start_response.status_code, 200)
        self.assertEqual(stop_response.get_json()["session"]["status"], "stopped")
        self.assertEqual(events_response.status_code, 200)
        self.assertEqual(events_response.mimetype, "text/event-stream")
        self.assertIn(b"ready", events_response.data)
        self.assertEqual(failed_start.status_code, 400)
        self.assertEqual(failed_stop.status_code, 404)
        self.assertEqual(failed_events.status_code, 404)

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

    def test_signed_meeting_recorder_process_async_handles_stale_failed_and_running_recovery(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        failed_record = store.create_record(
            owner_email="owner@npt.sg",
            title="Stale Failed Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/stale-failed",
        )
        failed_record["status"] = "processing"
        store.save_record(failed_record)
        running_record = store.create_record(
            owner_email="owner@npt.sg",
            title="Stale Running Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/stale-running",
        )
        running_record["status"] = "processing"
        store.save_record(running_record)
        fake_processing = Mock()
        fake_processing.recover_stale_processing_record.side_effect = [
            {
                "status": "failed",
                "record": {
                    **failed_record,
                    "status": "failed",
                    "error": "Segment combine stalled",
                },
            },
            {
                "status": "running",
                "record": {
                    **running_record,
                    "status": "processing",
                    "processing": {"status": "running"},
                },
            },
        ]

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            failed_response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": failed_record["record_id"], "owner_email": "owner@npt.sg"},
            )
            running_response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": running_record["record_id"], "owner_email": "owner@npt.sg"},
            )

        failed_payload = failed_response.get_json()
        running_payload = running_response.get_json()
        self.assertEqual(failed_response.status_code, 200)
        self.assertEqual(failed_payload["state"], "failed")
        self.assertEqual(failed_payload["message"], "Segment combine stalled")
        self.assertTrue(failed_payload["stalled_retryable"])
        self.assertEqual(running_response.status_code, 200)
        self.assertEqual(running_payload["state"], "running")
        self.assertEqual(running_payload["job_id"], "")
        fake_processing.process_recording.assert_not_called()

    def test_signed_meeting_recorder_process_async_returns_existing_active_job(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Active Processing Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/active",
        )
        record["status"] = "recorded"
        store.save_record(record)
        job_store = self.app.config["MEETING_RECORDER_JOB_STORE"]
        job = job_store.create(
            local_agent_server.MEETING_RECORDER_PROCESS_ACTION,
            title="Process Meeting Recording",
            owner_email="owner@npt.sg",
            record_id=record["record_id"],
        )
        job_store.update(job.job_id, state="running", stage="processing", message="Already processing")

        response = self._post_signed(
            "/api/local-agent/meeting-recorder/process-async",
            {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
        )

        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["job_id"], job.job_id)
        self.assertEqual(payload["state"], "running")

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

    def test_public_meeting_recorder_process_job_snapshot_defaults_for_queued_and_running(self):
        queued_payload = local_agent_server._public_meeting_recorder_process_job_snapshot(
            {
                "job_id": "queued-job",
                "action": local_agent_server.MEETING_RECORDER_PROCESS_ACTION,
                "owner_email": "owner@npt.sg",
                "state": "queued",
            }
        )
        running_payload = local_agent_server._public_meeting_recorder_process_job_snapshot(
            {
                "job_id": "running-job",
                "action": local_agent_server.MEETING_RECORDER_PROCESS_ACTION,
                "owner_email": "owner@npt.sg",
                "state": "running",
            }
        )

        self.assertNotIn("owner_email", queued_payload)
        self.assertEqual(queued_payload["message"], "Meeting processing is queued.")
        self.assertEqual(queued_payload["error_category"], "job_queued")
        self.assertEqual(queued_payload["progress"]["stage"], "")
        self.assertEqual(running_payload["message"], "Meeting processing is running.")
        self.assertEqual(running_payload["error_category"], "job_running")
        self.assertTrue(running_payload["error_retryable"])

    def test_signed_meeting_recorder_process_async_email_failure_is_sanitized_and_saved(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Email Failure Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/email-failure",
        )
        record["status"] = "recorded"
        store.save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.return_value = {
            "record_id": record["record_id"],
            "title": "Email Failure Review",
            "platform": "zoom",
            "status": "completed",
        }
        fake_processing.send_minutes_email.side_effect = ToolError("SMTP token=secret rejected")

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": record["record_id"], "owner_email": "owner@npt.sg", "send_email_on_complete": True},
            )
            completed = self._wait_for_meeting_process_job(response.get_json()["job_id"])
            updated_record = store.get_record(record["record_id"])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(completed["state"], "completed")
        self.assertEqual(completed["results"][0]["email"]["status"], "failed")
        self.assertNotIn("token", completed["results"][0]["email"]["error"].lower())
        self.assertIn("Email was not sent automatically.", completed["notice"]["details"])
        self.assertEqual(updated_record["email"]["status"], "failed")
        self.assertEqual(updated_record["email"]["recipient"], "owner@npt.sg")

    def test_signed_meeting_recorder_process_async_tool_error_marks_record_failed(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Tool Failure Review",
            platform="zoom",
            meeting_link="https://zoom.us/j/tool-failure",
        )
        record["status"] = "recorded"
        store.save_record(record)
        fake_processing = Mock()
        fake_processing.process_recording.side_effect = ToolError("ffmpeg combine failed")

        with patch("bpmis_jira_tool.local_agent_server._build_meeting_processing_service", return_value=fake_processing):
            response = self._post_signed(
                "/api/local-agent/meeting-recorder/process-async",
                {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
            )
            failed = self._wait_for_meeting_process_job(response.get_json()["job_id"], terminal_state="failed")
            updated_record = store.get_record(record["record_id"])

        self.assertEqual(response.status_code, 200)
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["error_code"], "meeting_processing_failed")
        self.assertIn("ffmpeg combine failed", failed["error"])
        self.assertEqual(updated_record["status"], "failed")
        self.assertEqual(updated_record["error"], "ffmpeg combine failed")

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

    def test_signed_meeting_recorder_process_async_rejects_unstopped_recording(self):
        store = self.app.config["MEETING_RECORD_STORE"]
        record = store.create_record(
            owner_email="owner@npt.sg",
            title="Not Ready",
            platform="zoom",
            meeting_link="https://zoom.us/j/not-ready",
        )

        response = self._post_signed(
            "/api/local-agent/meeting-recorder/process-async",
            {"record_id": record["record_id"], "owner_email": "owner@npt.sg"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Stop the recording", response.get_json()["message"])

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

    def test_signed_bpmis_project_ticket_routes_cover_crud_boundaries(self):
        user_key = "google:teammate@npt.sg"
        bpmis_id = "225159"
        missing_user_response = self._post_signed("/api/local-agent/bpmis/projects/list", {"user_key": ""})
        self._post_signed(
            "/api/local-agent/bpmis/projects/upsert",
            {
                "user_key": user_key,
                "bpmis_id": bpmis_id,
                "project_name": "Risk Control",
                "brd_link": "https://confluence/brd",
                "market": "SG",
            },
        )

        comment_response = self._post_signed(
            "/api/local-agent/bpmis/projects/comment",
            {"user_key": user_key, "bpmis_id": bpmis_id, "pm_comment": "Needs checker signoff"},
        )
        add_ticket_response = self._post_signed(
            "/api/local-agent/bpmis/projects/jira-tickets/add",
            {
                "user_key": user_key,
                "bpmis_id": bpmis_id,
                "component": "Risk",
                "market": "SG",
                "system": "AF",
                "jira_title": "Initial ticket",
                "prd_link": "https://confluence/prd",
                "description": "Create workflow",
                "fix_version_name": "v1",
                "fix_version_id": "10001",
                "ticket_key": "AF-1",
                "ticket_link": "https://jira/AF-1",
                "raw_response": {"ticket": "secret-free"},
            },
        )
        ticket_id = add_ticket_response.get_json()["ticket"]["id"]
        synced_response = self._post_signed(
            "/api/local-agent/bpmis/projects/jira-tickets/upsert-synced",
            {
                "user_key": user_key,
                "bpmis_id": bpmis_id,
                "component": "Risk Updated",
                "ticket_key": "AF-1",
                "ticket_link": "https://jira/AF-1",
                "status": "synced",
                "message": "Imported from BPMIS",
            },
        )
        status_response = self._post_signed(
            "/api/local-agent/bpmis/projects/jira-tickets/status",
            {"user_key": user_key, "bpmis_id": bpmis_id, "ticket_id": ticket_id, "status": "Done"},
        )
        version_response = self._post_signed(
            "/api/local-agent/bpmis/projects/jira-tickets/version",
            {"user_key": user_key, "bpmis_id": bpmis_id, "ticket_id": ticket_id, "version_name": "v2", "version_id": "10002"},
        )
        list_response = self._post_signed("/api/local-agent/bpmis/projects/list", {"user_key": user_key})
        delete_ticket_response = self._post_signed(
            "/api/local-agent/bpmis/projects/jira-tickets/delete",
            {"user_key": user_key, "bpmis_id": bpmis_id, "ticket_id": ticket_id},
        )
        delete_project_response = self._post_signed(
            "/api/local-agent/bpmis/projects/delete",
            {"user_key": user_key, "bpmis_id": bpmis_id},
        )
        empty_list_response = self._post_signed("/api/local-agent/bpmis/projects/list", {"user_key": user_key})

        self.assertEqual(missing_user_response.status_code, 400)
        self.assertTrue(comment_response.get_json()["updated"])
        self.assertEqual(add_ticket_response.get_json()["ticket"]["ticket_key"], "AF-1")
        self.assertEqual(synced_response.get_json()["ticket"]["component"], "Risk Updated")
        self.assertTrue(status_response.get_json()["updated"])
        self.assertTrue(version_response.get_json()["updated"])
        project = list_response.get_json()["projects"][0]
        self.assertEqual(project["pm_comment"], "Needs checker signoff")
        self.assertEqual(project["jira_tickets"][0]["status"], "Done")
        self.assertEqual(project["jira_tickets"][0]["fix_version_name"], "v2")
        self.assertTrue(delete_ticket_response.get_json()["deleted"])
        self.assertTrue(delete_project_response.get_json()["deleted"])
        self.assertEqual(empty_list_response.get_json()["projects"], [])

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

    def test_signed_bpmis_local_store_validation_errors_are_json(self):
        cases = [
            ("/api/local-agent/bpmis/config/load", {}, "user_key is required"),
            ("/api/local-agent/bpmis/config/save", {}, "user_key is required"),
            ("/api/local-agent/bpmis/config/save", {"user_key": "google:owner@npt.sg", "config": []}, "config must be an object"),
            ("/api/local-agent/bpmis/config/migrate", {"from_user_key": "anon:1"}, "from_user_key and to_user_key"),
            ("/api/local-agent/bpmis/team-profiles/save", {"profile": {}}, "team_key is required"),
            ("/api/local-agent/bpmis/team-profiles/save", {"team_key": "AF", "profile": []}, "profile must be an object"),
            ("/api/local-agent/team-dashboard/config/save", {"config": []}, "config must be an object"),
            ("/api/local-agent/bpmis/projects/list", {}, "user_key is required"),
            ("/api/local-agent/bpmis/projects/reorder", {}, "user_key is required"),
        ]

        for path, payload, message in cases:
            with self.subTest(path=path, payload=payload):
                response = self._post_signed(path, payload)

                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.get_json()["status"], "error")
                self.assertIn(message, response.get_json()["message"])

    def test_signed_disabled_bpmis_and_seatalk_routes_fail_fast(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        env = {
            "LOCAL_AGENT_HMAC_SECRET": "shared-secret",
            "LOCAL_AGENT_BPMIS_ENABLED": "false",
            "LOCAL_AGENT_SEATALK_ENABLED": "false",
            "TEAM_PORTAL_DATA_DIR": temp_dir.name,
        }
        with patch.dict(os.environ, env, clear=True), patch("bpmis_jira_tool.config.find_dotenv", return_value=""):
            disabled_app = create_local_agent_app()

        def post(path, payload):
            body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            headers = sign_headers(secret="shared-secret", method="POST", path=path, body=body)
            headers["Content-Type"] = "application/json"
            return disabled_app.test_client().post(path, data=body, headers=headers)

        def get(path):
            headers = sign_headers(secret="shared-secret", method="GET", path=path, body=b"")
            return disabled_app.test_client().get(path, headers=headers)

        bpmis_post_cases = [
            ("/api/local-agent/bpmis/call", {"operation": "list_biz_projects_for_pm_email"}),
            ("/api/local-agent/bpmis/config/load", {"user_key": "google:owner@npt.sg"}),
            ("/api/local-agent/bpmis/config/save", {"user_key": "google:owner@npt.sg", "config": {}}),
            ("/api/local-agent/bpmis/config/migrate", {"from_user_key": "anon:1", "to_user_key": "google:owner@npt.sg"}),
            ("/api/local-agent/bpmis/team-profiles/load", {}),
            ("/api/local-agent/bpmis/team-profiles/save", {"team_key": "AF", "profile": {}}),
            ("/api/local-agent/team-dashboard/config/load", {}),
            ("/api/local-agent/team-dashboard/config/save", {"config": {}}),
            ("/api/local-agent/bpmis/projects/list", {"user_key": "google:owner@npt.sg"}),
            ("/api/local-agent/bpmis/projects/reorder", {"user_key": "google:owner@npt.sg", "bpmis_ids": []}),
            ("/api/local-agent/bpmis/projects/upsert", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159"}),
            ("/api/local-agent/bpmis/projects/delete", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159"}),
            ("/api/local-agent/bpmis/projects/comment", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/add", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/upsert-synced", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/delete", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_id": "1"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/status", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_id": "1", "status": "Done"}),
            ("/api/local-agent/bpmis/projects/jira-tickets/version", {"user_key": "google:owner@npt.sg", "bpmis_id": "225159", "ticket_id": "1", "version_name": "v1"}),
        ]

        for path, payload in bpmis_post_cases:
            with self.subTest(path=path):
                response = post(path, payload)

                self.assertEqual(response.status_code, 400)
                self.assertIn("proxy is disabled", response.get_json()["message"])

        for path in (
            "/api/local-agent/team-dashboard/daily-briefs",
            "/api/local-agent/team-dashboard/daily-briefs/missing/download",
        ):
            with self.subTest(path=path):
                response = get(path)

                self.assertEqual(response.status_code, 400)
                self.assertIn("SeaTalk local-agent proxy is disabled", response.get_json()["message"])

    def test_bpmis_proxy_summary_and_serialization_helpers_cover_risk_shapes(self):
        summary_cases = {
            "list_jira_tasks_created_by_emails": (["pm@npt.sg", "qa@npt.sg"], "email_count"),
            "list_jira_tasks_for_projects_created_by_emails": (["225159"], ["pm@npt.sg"]),
            "list_jira_tasks_for_project_created_by_email": ("225159", "pm@npt.sg"),
            "list_biz_projects_for_pm_emails": (["pm@npt.sg"], "email_count"),
            "list_biz_projects_for_pm_email": ("pm@npt.sg", "email_present"),
            "search_biz_projects_by_title_keywords": ("risk workflow", "keyword_present"),
            "get_jira_ticket_details": (["AF-1"], "ticket_count"),
            "get_issue_detail": ("225159", "lookup_id_present"),
            "get_jira_ticket_detail": ("AF-1", "lookup_id_present"),
            "list_issues_for_version": ("10001", "lookup_id_present"),
            "update_biz_project_status": ("225159", "Completed"),
            "list_actual_mandays_for_projects": (["225159"], "project_count"),
            "link_jira_ticket_to_project": ("AF-1", "225159"),
            "delink_jira_ticket_from_project": ("AF-1", "225159"),
        }

        for operation, raw_args in summary_cases.items():
            args = list(raw_args) if isinstance(raw_args, tuple) else [raw_args]
            with self.subTest(operation=operation):
                summary = local_agent_server._summarize_bpmis_proxy_args(operation, args)
                self.assertEqual(summary["arg_count"], len(args))
                self.assertGreaterEqual(len(summary), 2)

        created_ticket = CreatedTicket(ticket_key="AF-1", ticket_link="https://jira/AF-1", raw={"ok": True})
        serialized = local_agent_server._serialize_bpmis_result(
            {
                "ticket": created_ticket,
                "rows": (created_ticket, {"nested": [created_ticket]}),
            }
        )

        self.assertEqual(
            local_agent_server._summarize_bpmis_proxy_result([{"issue_id": "225159"}]),
            {"result_type": "list", "result_count": 1},
        )
        self.assertEqual(local_agent_server._summarize_bpmis_proxy_result({"rows": [1, 2]})["row_count"], 2)
        self.assertEqual(local_agent_server._summarize_bpmis_proxy_result(created_ticket)["result_type"], "CreatedTicket")
        self.assertEqual(local_agent_server._summarize_bpmis_proxy_result("ok")["result_type"], "str")
        self.assertEqual(serialized["ticket"]["ticket_key"], "AF-1")
        self.assertEqual(serialized["rows"][0]["ticket_link"], "https://jira/AF-1")
        self.assertEqual(serialized["rows"][1]["nested"][0]["raw"], {"ok": True})
        self.assertEqual(
            local_agent_server._sanitize_meeting_recorder_job_error("Traceback token=secret"),
            "Meeting processing failed. Check server logs for details.",
        )
        self.assertEqual(
            local_agent_server._sanitize_meeting_recorder_job_error("boom", unexpected=True),
            "Meeting processing failed unexpectedly. Check server logs for details.",
        )
        self.assertEqual(
            local_agent_server._classify_local_agent_tool_error(ToolError("LLM rate limit exceeded"))["error_code"],
            "llm_rate_limited",
        )
        self.assertEqual(
            local_agent_server._classify_local_agent_tool_error(ToolError("request timed out"))["error_code"],
            "llm_timeout",
        )
        self.assertEqual(
            local_agent_server._classify_local_agent_tool_error(ToolError("validation failed"))["error_code"],
            "monthly_report_failed",
        )
        relative_settings = replace(self.app.config["SETTINGS"], team_portal_data_dir=Path("relative-team-data"))
        self.assertTrue(local_agent_server._data_root(relative_settings).is_absolute())
        self.assertTrue(local_agent_server._build_config_store(relative_settings).db_path.is_absolute())
        self.assertFalse(local_agent_server._is_allowed_confluence_image_source("file:///tmp/a.png", "https://confluence.shopee.io"))
        self.assertFalse(local_agent_server._is_allowed_confluence_image_source("https://confluence.shopee.io/download/attachments/1/a.png", ""))
        self.assertTrue(
            local_agent_server._is_allowed_confluence_image_source(
                "https://confluence.shopee.io/download/thumbnails/1/a.png",
                "https://confluence.shopee.io",
            )
        )
        self.assertIsNone(
            local_agent_server._save_prd_latest_result(
                self.app.config["SETTINGS"],
                owner_key="",
                tool_key="prd_self_assessment",
                payload={"status": "ok"},
            )
        )
        self.assertIsNone(
            local_agent_server._get_prd_latest_result(
                self.app.config["SETTINGS"],
                owner_key="",
                tool_key="prd_self_assessment",
            )
        )

    def test_meeting_record_for_owner_rejects_cross_owner_access(self):
        class FakeMeetingRecordStore:
            def get_record(self, record_id):
                return {"record_id": record_id, "owner_email": "owner@npt.sg"}

        self.app.config["MEETING_RECORD_STORE"] = FakeMeetingRecordStore()

        with self.app.app_context(), self.assertRaisesRegex(ToolError, "not available"):
            local_agent_server._meeting_record_for_owner(record_id="rec-1", owner_email="other@npt.sg")

    def test_source_code_query_worker_failure_paths_are_json_readable(self):
        class FakeSourceCodeQAService:
            def __init__(self, error):
                self.error = error

            def with_llm_provider(self, llm_provider):
                return self

            def with_codex_timeout_seconds(self, timeout_seconds):
                return self

            def query(self, **kwargs):
                raise self.error

        with self.app.app_context():
            self.app.config["SOURCE_CODE_QA_SERVICE"] = FakeSourceCodeQAService(ToolError("repo index unavailable"))
            local_agent_server._run_source_code_qa_query_job(
                self.app,
                "tool-error-job",
                {"pm_team": "AF", "country": "All", "question": "Where is auth?"},
            )
            tool_error_snapshot = local_agent_server._snapshot_query_job("tool-error-job")

            self.app.config["SOURCE_CODE_QA_SERVICE"] = FakeSourceCodeQAService(RuntimeError("worker crashed"))
            local_agent_server._run_source_code_qa_query_job(
                self.app,
                "unexpected-job",
                {"pm_team": "AF", "country": "All", "question": "Where is auth?"},
            )
            unexpected_snapshot = local_agent_server._snapshot_query_job("unexpected-job")

        self.assertEqual(tool_error_snapshot["state"], "failed")
        self.assertEqual(tool_error_snapshot["error"], "repo index unavailable")
        self.assertEqual(unexpected_snapshot["state"], "failed")
        self.assertIn("failed unexpectedly", unexpected_snapshot["error"])

    def test_local_agent_service_builders_wire_expected_dependencies(self):
        settings = self.app.config["SETTINGS"]
        # Meeting minutes and PRD briefing both wire the Codex client.
        with self.app.app_context(), patch(
            "bpmis_jira_tool.local_agent_server.CodexTextGenerationClient",
            side_effect=lambda **kwargs: {"text_client": kwargs},
        ) as text_client, patch(
            "bpmis_jira_tool.local_agent_server.MeetingProcessingService",
            side_effect=lambda **kwargs: {"meeting_processing": kwargs},
        ) as meeting_processing, patch(
            "bpmis_jira_tool.local_agent_server._build_google_credential_store",
            return_value={"credential_store": True},
        ):
            processing_service = local_agent_server._build_meeting_processing_service(settings)

        with patch("bpmis_jira_tool.local_agent_server.BriefingStore", side_effect=lambda root_dir: {"root_dir": root_dir}), patch(
            "bpmis_jira_tool.local_agent_server.ConfluenceConnector",
            side_effect=lambda **kwargs: {"confluence": kwargs},
        ), patch(
            "bpmis_jira_tool.local_agent_server.PRDReviewService",
            side_effect=lambda **kwargs: {"prd_review": kwargs},
        ) as review_service:
            prd_service = local_agent_server._build_prd_review_service(settings)

        with patch("bpmis_jira_tool.local_agent_server.BriefingStore", side_effect=lambda root_dir: {"root_dir": root_dir}), patch(
            "bpmis_jira_tool.local_agent_server.CodexTextGenerationClient",
            side_effect=lambda **kwargs: {"text_client": kwargs},
        ), patch(
            "bpmis_jira_tool.local_agent_server.ConfluenceConnector",
            side_effect=lambda **kwargs: {"confluence": kwargs},
        ), patch(
            "bpmis_jira_tool.local_agent_server.VoiceService",
            side_effect=lambda **kwargs: {"voice": kwargs},
        ), patch(
            "bpmis_jira_tool.local_agent_server.PRDBriefingService",
            side_effect=lambda **kwargs: {"prd_briefing": kwargs},
        ) as briefing_service:
            briefing = local_agent_server._build_prd_briefing_service(settings)

        with patch("bpmis_jira_tool.local_agent_server.BriefingStore", side_effect=lambda root_dir: {"root_dir": root_dir}), patch(
            "bpmis_jira_tool.local_agent_server.ConfluenceConnector",
            side_effect=lambda **kwargs: {"confluence": kwargs},
        ), patch(
            "bpmis_jira_tool.local_agent_server._build_seatalk_service",
            return_value={"seatalk": True},
        ), patch(
            "bpmis_jira_tool.local_agent_server.MonthlyReportService",
            side_effect=lambda **kwargs: {"monthly": kwargs},
        ) as monthly_service:
            monthly = local_agent_server._build_monthly_report_service(settings)

        name_store = local_agent_server._build_seatalk_name_mapping_store(settings)

        self.assertEqual(processing_service["meeting_processing"]["portal_base_url"], settings.team_portal_base_url)
        self.assertEqual(text_client.call_args.kwargs["prompt_mode"], "meeting_recorder_minutes_codex")
        self.assertTrue(meeting_processing.called)
        self.assertIs(prd_service["prd_review"]["settings"], settings)
        self.assertTrue(review_service.called)
        self.assertFalse(briefing["prd_briefing"]["walkthrough_prewarm_enabled"])
        self.assertTrue(briefing_service.called)
        self.assertIs(monthly["monthly"]["settings"], settings)
        self.assertTrue(monthly_service.called)
        self.assertTrue(str(name_store.storage_path).endswith("seatalk/name_overrides.json"))

    def test_prd_briefing_audio_inline_helper_handles_empty_external_and_local_assets(self):
        class FakeStore:
            def __init__(self, root_dir):
                self.root_dir = root_dir

        store = FakeStore(Path(self.temp_dir.name) / "prd-assets")
        audio_path = store.root_dir / "sessions" / "audio.mp3"
        audio_path.parent.mkdir(parents=True)
        audio_path.write_bytes(b"mp3-bytes")
        empty_payload = {}
        external_payload = {"chunk": {"audioUrl": "https://cdn.example/audio.mp3"}}
        local_payload = {"chunk": {"audioUrl": "/prd-briefing/assets/sessions/audio.mp3"}}
        missing_payload = {"chunk": {"audioUrl": "/prd-briefing/assets/sessions/missing.mp3"}}
        unreadable_payload = {"chunk": {"audioUrl": "/prd-briefing/assets/sessions/audio.mp3"}}

        local_agent_server._inline_prd_briefing_audio_data_url(store, empty_payload)
        local_agent_server._inline_prd_briefing_audio_data_url(store, external_payload)
        local_agent_server._inline_prd_briefing_audio_data_url(store, missing_payload)
        with patch.object(Path, "read_bytes", side_effect=OSError("unreadable")):
            local_agent_server._inline_prd_briefing_audio_data_url(store, unreadable_payload)
        local_agent_server._inline_prd_briefing_audio_data_url(store, local_payload)

        self.assertEqual(empty_payload, {})
        self.assertEqual(external_payload["chunk"]["audioUrl"], "https://cdn.example/audio.mp3")
        self.assertEqual(missing_payload["chunk"]["audioUrl"], "/prd-briefing/assets/sessions/missing.mp3")
        self.assertEqual(unreadable_payload["chunk"]["audioUrl"], "/prd-briefing/assets/sessions/audio.mp3")
        self.assertTrue(local_payload["chunk"]["audioUrl"].startswith("data:audio/mpeg;base64,"))

    def test_seatalk_name_overrides_ignores_cleanup_errors(self):
        with local_agent_server._seatalk_name_overrides({}) as empty_overrides_path:
            self.assertIsNone(empty_overrides_path)
        manager = local_agent_server._seatalk_name_overrides({"group": "Risk PM"})
        overrides_path = manager.__enter__()
        self.assertTrue(Path(overrides_path).exists())
        cleanup_path = Path(overrides_path)
        with patch.object(Path, "unlink", side_effect=OSError("locked")):
            manager.__exit__(None, None, None)
        cleanup_path.unlink(missing_ok=True)

    def test_signed_storage_contract_routes_return_stable_payloads(self):
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

        patchers = [
            patch("bpmis_jira_tool.local_agent_server._build_seatalk_service", return_value=FakeSeaTalkService()),
            patch("bpmis_jira_tool.local_agent_server._build_seatalk_todo_store", return_value=FakeTodoStore()),
            patch("bpmis_jira_tool.local_agent_server._build_seatalk_name_mapping_store", return_value=FakeNameMappingStore()),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

        post_cases = [
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
            ("/api/local-agent/seatalk/export", {"name_mappings": {"group-1": "Risk PM"}}),
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

    def test_signed_source_code_storage_routes_round_trip_artifacts_and_evidence(self):
        session_response = self._post_signed(
            "/api/local-agent/source-code-qa/sessions/create",
            {"owner_email": "owner@npt.sg", "pm_team": "AF", "country": "All", "title": "Storage round trip"},
        )
        session_id = session_response.get_json()["session"]["id"]
        attachment_save = self._post_signed(
            "/api/local-agent/source-code-qa/attachments/save",
            {
                "owner_email": "owner@npt.sg",
                "session_id": session_id,
                "filename": "notes.txt",
                "mime_type": "text/plain",
                "content_base64": base64.b64encode(b"hello attachment").decode("ascii"),
            },
        )
        attachment_id = attachment_save.get_json()["attachment"]["id"]
        attachment_get = self._post_signed(
            "/api/local-agent/source-code-qa/attachments/get",
            {"owner_email": "owner@npt.sg", "session_id": session_id, "attachment_id": attachment_id},
        )
        artifact_save = self._post_signed(
            "/api/local-agent/source-code-qa/generated-artifacts/save",
            {
                "owner_email": "owner@npt.sg",
                "session_id": session_id,
                "pm_team": "AF",
                "country": "All",
                "question": "Find data",
                "sql": "select * from risk_events",
                "readme": "Run in readonly mode.",
            },
        )
        artifact_id = artifact_save.get_json()["artifact"]["id"]
        artifact_get = self._post_signed(
            "/api/local-agent/source-code-qa/generated-artifacts/get",
            {"owner_email": "owner@npt.sg", "session_id": session_id, "artifact_id": artifact_id},
        )
        evidence_save = self._post_signed(
            "/api/local-agent/source-code-qa/runtime-evidence/save",
            {
                "pm_team": "AF",
                "country": "SG",
                "source_type": "other",
                "uploaded_by": "owner@npt.sg",
                "filename": "runtime.log",
                "mime_type": "text/plain",
                "content_base64": base64.b64encode(b"error=none").decode("ascii"),
            },
        )
        evidence_id = evidence_save.get_json()["evidence"]["id"]
        evidence_list = self._post_signed("/api/local-agent/source-code-qa/runtime-evidence/list", {"pm_team": "AF", "country": "SG"})
        evidence_resolve = self._post_signed("/api/local-agent/source-code-qa/runtime-evidence/resolve", {"pm_team": "AF", "country": "SG"})
        evidence_delete = self._post_signed(
            "/api/local-agent/source-code-qa/runtime-evidence/delete",
            {"pm_team": "AF", "country": "SG", "evidence_id": evidence_id},
        )
        evidence_list_after_delete = self._post_signed(
            "/api/local-agent/source-code-qa/runtime-evidence/list",
            {"pm_team": "AF", "country": "SG"},
        )

        self.assertEqual(session_response.status_code, 200)
        self.assertEqual(attachment_save.status_code, 200)
        self.assertEqual(attachment_get.status_code, 200)
        self.assertEqual(base64.b64decode(attachment_get.get_json()["content_base64"]), b"hello attachment")
        self.assertEqual(artifact_save.status_code, 200)
        self.assertEqual(artifact_get.status_code, 200)
        self.assertGreater(len(base64.b64decode(artifact_get.get_json()["content_base64"])), 100)
        self.assertEqual(evidence_save.status_code, 200)
        self.assertEqual(evidence_list.get_json()["evidence"][0]["id"], evidence_id)
        self.assertEqual(evidence_resolve.status_code, 200)
        self.assertEqual(evidence_resolve.get_json()["evidence"][0]["id"], evidence_id)
        self.assertTrue(evidence_delete.get_json()["deleted"])
        self.assertEqual(evidence_list_after_delete.get_json()["evidence"], [])

    def test_signed_source_code_runtime_evidence_scope_errors_are_json(self):
        list_response = self._post_signed("/api/local-agent/source-code-qa/runtime-evidence/list", {"pm_team": "BAD", "country": "SG"})
        resolve_response = self._post_signed("/api/local-agent/source-code-qa/runtime-evidence/resolve", {"pm_team": "BAD", "country": "SG"})

        self.assertEqual(list_response.status_code, 400)
        self.assertIn("PM Team", list_response.get_json()["message"])
        self.assertEqual(resolve_response.status_code, 400)
        self.assertIn("PM Team", resolve_response.get_json()["message"])

class LocalAgentClientTests(unittest.TestCase):
    def test_client_validates_required_config_and_clamps_timeouts(self):
        with self.assertRaisesRegex(ToolError, "LOCAL_AGENT_BASE_URL"):
            LocalAgentClient(base_url="", hmac_secret="shared-secret")
        with self.assertRaisesRegex(ToolError, "LOCAL_AGENT_HMAC_SECRET"):
            LocalAgentClient(base_url="https://portal.example", hmac_secret="")

        client = LocalAgentClient(
            base_url="https://portal.example/root/",
            hmac_secret="shared-secret",
            timeout_seconds=2,
            connect_timeout_seconds=999,
        )

        self.assertEqual(client.base_url, "https://portal.example/root/")
        self.assertEqual(client.timeout_seconds, 5)
        self.assertEqual(client.connect_timeout_seconds, 5)
        self.assertFalse(_is_transient_unreadable_local_agent_response(status_code=500, body_preview="ngrok offline"))

    def test_health_falls_back_to_legacy_endpoint_or_reraises_proxied_error(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        with patch.object(
            client,
            "_request",
            side_effect=[
                {"status": "ok"},
                {"status": "ok", "legacy": True},
            ],
        ) as request:
            self.assertEqual(client.get_health(), {"status": "ok", "legacy": True})

        self.assertEqual([call.args[1] for call in request.call_args_list], ["/api/local-agent/healthz", "/healthz"])

        with patch.object(
            client,
            "_request",
            side_effect=[ToolError("proxied down"), ToolError("legacy down")],
        ):
            with self.assertRaisesRegex(ToolError, "proxied down"):
                client.get_health()

        with patch.object(
            client,
            "_request",
            side_effect=[{"status": "ok"}, ToolError("legacy only down")],
        ):
            with self.assertRaisesRegex(ToolError, "legacy only down"):
                client.get_health()

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

    def test_source_code_query_async_handles_missing_job_results_fallback_and_failure(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        with patch.object(client, "_request", return_value={"status": "ok"}):
            with self.assertRaisesRegex(ToolError, "Source Code Q&A job id"):
                client.source_code_qa_query({"question": "q"}, progress_callback=lambda *_args: None)

        responses = [
            {"status": "ok", "job_id": "job-2"},
            {"status": "ok", "state": "running", "stage": "read", "message": "Reading", "current": 1, "total": 2},
            {"status": "ok", "state": "completed", "results": [{"summary": "fallback result"}]},
        ]
        progress = []
        with patch.object(client, "_request", side_effect=lambda *_args, **_kwargs: responses.pop(0)), patch(
            "bpmis_jira_tool.local_agent_client.time.sleep"
        ):
            result = client.source_code_qa_query({"question": "q"}, progress_callback=lambda *args: progress.append(args))

        self.assertEqual(result["summary"], "fallback result")
        self.assertEqual(progress, [("read", "Reading", 1, 2)])

        responses = [
            {"status": "ok", "job_id": "job-3"},
            {"status": "ok", "state": "failed", "stage": "failed", "message": "Model failed"},
        ]
        with patch.object(client, "_request", side_effect=lambda *_args, **_kwargs: responses.pop(0)):
            with self.assertRaisesRegex(ToolError, "Model failed"):
                client.source_code_qa_query({"question": "q"}, progress_callback=lambda *_args: None)

    def test_prd_async_polling_covers_typeerror_callback_missing_job_and_failure(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")

        with patch.object(client, "_request", return_value={"status": "ok"}):
            with self.assertRaisesRegex(ToolError, "PRD job id"):
                client.prd_review({"prd": "text"}, progress_callback=lambda *_args: None)

        with patch.object(client, "_request", return_value={"status": "ok", "job_id": "prd-job"}), patch.object(
            client,
            "_poll_prd_job",
            return_value={"score": 95},
        ) as poll_job:
            self.assertEqual(client.prd_review({"prd": "text"}, progress_callback=lambda *_args: None)["score"], 95)
        poll_job.assert_called_once()

        with patch.object(client, "_start_prd_job", return_value={"summary": "ok"}) as start_job:
            self.assertEqual(client.prd_summary({"prd": "text"}, progress_callback=lambda *_args: None)["summary"], "ok")
            self.assertEqual(client.prd_self_assessment_review({"prd": "text"}, progress_callback=lambda *_args: None)["summary"], "ok")
            self.assertEqual(client.prd_self_assessment_summary({"prd": "text"}, progress_callback=lambda *_args: None)["summary"], "ok")
        self.assertEqual(start_job.call_count, 3)

        responses = [
            {
                "status": "ok",
                "state": "running",
                "stage": "review",
                "message": "Reviewing",
                "current": 1,
                "total": 3,
                "estimated_prompt_tokens": 100,
                "token_risk": "low",
            },
            {"status": "ok", "state": "completed", "results": [{"score": 90}]},
        ]
        progress = []
        with patch.object(client, "_request", side_effect=lambda *_args, **_kwargs: responses.pop(0)), patch(
            "bpmis_jira_tool.local_agent_client.time.sleep"
        ):
            result = client._poll_prd_job("prd-job", progress_callback=lambda stage, message, current, total: progress.append((stage, message, current, total)))

        self.assertEqual(result["score"], 90)
        self.assertEqual(progress, [("review", "Reviewing", 1, 3)])

        with patch.object(
            client,
            "_request",
            return_value={"status": "ok", "state": "failed", "message": "", "error": ""},
        ):
            with self.assertRaisesRegex(ToolError, "PRD job failed"):
                client._poll_prd_job("prd-job")

    def test_monthly_report_async_covers_progress_results_fallback_and_failure(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")

        with patch.object(client, "_request", return_value={"status": "ok"}):
            with self.assertRaisesRegex(ToolError, "Monthly Report job id"):
                client.team_dashboard_monthly_report_draft({"topic": "risk"}, progress_callback=lambda *_args, **_kwargs: None)

        responses = [
            {"status": "ok", "job_id": "job-monthly"},
            {
                "status": "ok",
                "state": "running",
                "stage": "draft",
                "message": "Drafting",
                "current": 2,
                "total": 4,
                "estimated_prompt_tokens": 200,
                "token_risk": "medium",
            },
            {"status": "ok", "state": "completed", "results": [{"subject": "Monthly"}]},
        ]
        progress = []
        with patch.object(client, "_request", side_effect=lambda *_args, **_kwargs: responses.pop(0)), patch(
            "bpmis_jira_tool.local_agent_client.time.sleep"
        ):
            result = client.team_dashboard_monthly_report_draft(
                {"topic": "risk"},
                progress_callback=lambda *args, **kwargs: progress.append((args, kwargs)),
            )

        self.assertEqual(result["subject"], "Monthly")
        self.assertEqual(progress[0][0], ("draft", "Drafting", 2, 4))
        self.assertEqual(progress[0][1]["token_risk"], "medium")

        responses = [
            {"status": "ok", "job_id": "job-fail"},
            {"status": "ok", "state": "failed", "message": "", "error": ""},
        ]
        with patch.object(client, "_request", side_effect=lambda *_args, **_kwargs: responses.pop(0)):
            with self.assertRaisesRegex(ToolError, "Monthly Report job failed"):
                client.team_dashboard_monthly_report_draft({"topic": "risk"}, progress_callback=lambda *_args, **_kwargs: None)

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

    def test_raw_response_helpers_build_safe_paths_headers_and_default_asset_metadata(self):
        class FakeRawResponse:
            status_code = 200
            text = ""
            content = b"asset-bytes"
            headers = {}

            def json(self):
                raise AssertionError("raw asset responses should not require JSON")

        class FakeSession:
            def __init__(self):
                self.calls = []

            def request(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                return FakeRawResponse()

        session = FakeSession()
        client = LocalAgentClient(base_url="https://portal.example/base", hmac_secret="shared-secret", session=session)

        content, content_type, filename = client.meeting_recorder_asset(
            record_id="record 1",
            owner_email="owner+npt@npt.sg",
            relative_path="/notes/final transcript.txt",
        )
        response = client.meeting_recorder_asset_response(
            record_id="record 1",
            owner_email="owner+npt@npt.sg",
            relative_path="/notes/final transcript.txt",
            range_header="bytes=0-99",
            method="HEAD",
            download=True,
        )
        events_response = client.meeting_translation_events_response(session_id="session 1", owner_email="owner+npt@npt.sg")
        image_response = client.prd_briefing_image_proxy("https://example.test/image.png")
        brief_response = client.team_dashboard_daily_brief_download("brief 1")

        self.assertEqual(content, b"asset-bytes")
        self.assertEqual(content_type, "application/octet-stream")
        self.assertEqual(filename, "final transcript.txt")
        self.assertIs(response.__class__, FakeRawResponse)
        self.assertIs(events_response.__class__, FakeRawResponse)
        self.assertIs(image_response.__class__, FakeRawResponse)
        self.assertIs(brief_response.__class__, FakeRawResponse)
        asset_call = session.calls[1]
        self.assertIn("/meeting-recorder/assets/record%201/notes/final%20transcript.txt", asset_call[0][1])
        self.assertIn("download=1", asset_call[0][1])
        self.assertEqual(asset_call[1]["headers"]["Range"], "bytes=0-99")
        self.assertEqual(session.calls[2][1]["headers"]["Accept"], "text/event-stream")
        self.assertEqual(session.calls[3][1]["headers"]["Accept"], "image/*,*/*;q=0.8")

    def test_request_maps_transport_and_error_payload_failures(self):
        class FakeSession:
            def __init__(self, response=None, error=None):
                self.response = response
                self.error = error

            def request(self, *_args, **_kwargs):
                if self.error:
                    raise self.error
                return self.response

        class FakeResponse:
            def __init__(self, status_code, payload, text=""):
                self.status_code = status_code
                self._payload = payload
                self.text = text
                self.headers = {}
                self.content = b""
                self.url = "https://portal.example/api/local-agent/test"

            def json(self):
                if isinstance(self._payload, Exception):
                    raise self._payload
                return self._payload

        client = LocalAgentClient(
            base_url="https://portal.example",
            hmac_secret="shared-secret",
            session=FakeSession(error=requests.RequestException("connection refused")),
        )
        with self.assertRaisesRegex(ToolError, "connection refused"):
            client.seatalk_overview()

        client = LocalAgentClient(
            base_url="https://portal.example",
            hmac_secret="shared-secret",
            session=FakeSession(response=FakeResponse(400, {"status": "error", "message": "bad request"})),
        )
        with self.assertRaisesRegex(ToolError, "bad request"):
            client.seatalk_overview()

        client = LocalAgentClient(
            base_url="https://portal.example",
            hmac_secret="shared-secret",
            session=FakeSession(response=FakeResponse(200, {"status": "error", "message": "agent refused"})),
        )
        with self.assertRaisesRegex(ToolError, "agent refused"):
            client.seatalk_overview()

        client = LocalAgentClient(
            base_url="https://portal.example",
            hmac_secret="shared-secret",
            session=FakeSession(response=FakeResponse(200, ValueError("bad json"), text="<html>bad</html>")),
        )
        with self.assertRaisesRegex(ToolError, "unreadable response"):
            client.seatalk_overview()

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
        self.assertIn("/api/local-agent/bpmis/config/save", paths)
        self.assertIn("/api/local-agent/source-code-qa/runtime-evidence/save", paths)

    def test_vpn_and_direct_query_wrappers_cover_payload_shapes(self):
        client = LocalAgentClient(base_url="https://portal.example", hmac_secret="shared-secret")
        calls = []

        def fake_request(method, path, payload=None, **kwargs):
            calls.append((method, path, payload, kwargs))
            return {"status": "ok", "profiles": [{"id": "vpn"}], "answer": "direct"}

        with patch.object(client, "_request", side_effect=fake_request):
            self.assertEqual(client.vpn_profiles()["profiles"][0]["id"], "vpn")
            self.assertEqual(client.vpn_save_profile({"name": "VPN"})["status"], "ok")
            self.assertEqual(client.vpn_delete_profile("profile / 1")["status"], "ok")
            self.assertEqual(client.vpn_connect("profile / 1")["status"], "ok")
            self.assertEqual(client.vpn_connect("profile / 1", second_password="otp")["status"], "ok")
            self.assertEqual(client.vpn_disconnect()["status"], "ok")
            self.assertEqual(client.source_code_qa_query({"question": "direct"})["answer"], "direct")

        paths = [call[1] for call in calls]
        self.assertIn("/api/local-agent/vpn/profiles/profile%20%2F%201", paths)
        self.assertIn("/api/local-agent/vpn/profiles/profile%20%2F%201/connect", paths)
        self.assertEqual(calls[3][2], {})
        self.assertEqual(calls[4][2], {"second_password": "otp"})

    def test_remote_store_adapters_delegate_to_local_agent_client(self):
        from bpmis_jira_tool.local_agent_client import (
            RemoteBPMISProjectStore,
            RemoteSeaTalkDashboardService,
            RemoteSeaTalkNameMappingStore,
            RemoteSeaTalkTodoStore,
            RemoteSourceCodeQAAttachmentStore,
            RemoteSourceCodeQAGeneratedArtifactStore,
            RemoteSourceCodeQARuntimeEvidenceStore,
            RemoteSourceCodeQASessionStore,
            RemoteTeamDashboardConfigStore,
        )

        class FakeStoreClient:
            def __init__(self):
                self.calls = []

            def _record(self, name, *args, **kwargs):
                self.calls.append((name, args, kwargs))

            def seatalk_overview(self):
                self._record("seatalk_overview")
                return {"status": "ok", "overview": True}

            def seatalk_insights(self, **kwargs):
                self._record("seatalk_insights", **kwargs)
                return {"status": "ok", "insights": True}

            def seatalk_project_updates(self, **kwargs):
                self._record("seatalk_project_updates", **kwargs)
                return {"status": "ok", "projects": True}

            def seatalk_todos(self, **kwargs):
                self._record("seatalk_todos", **kwargs)
                return {"status": "ok", "todos": True}

            def seatalk_name_mappings(self, **kwargs):
                self._record("seatalk_name_mappings", **kwargs)
                return {"status": "ok", "mappings": {"group": "Risk"}}

            def seatalk_export(self, **kwargs):
                self._record("seatalk_export", **kwargs)
                return ("history", "history.txt")

            def seatalk_export_since(self, **kwargs):
                self._record("seatalk_export_since", **kwargs)
                return "history since"

            def bpmis_project_upsert(self, **kwargs):
                self._record("bpmis_project_upsert", **kwargs)
                return "stored-id"

            def bpmis_projects_list(self, **kwargs):
                self._record("bpmis_projects_list", **kwargs)
                return [{"bpmis_id": "225159"}]

            def bpmis_projects_reorder(self, **kwargs):
                self._record("bpmis_projects_reorder", **kwargs)
                return [{"bpmis_id": "225159"}]

            def bpmis_project_delete(self, **kwargs):
                self._record("bpmis_project_delete", **kwargs)
                return True

            def bpmis_project_comment_update(self, **kwargs):
                self._record("bpmis_project_comment_update", **kwargs)
                return True

            def bpmis_project_ticket_add(self, **kwargs):
                self._record("bpmis_project_ticket_add", **kwargs)
                return {"ticket_key": "AF-1"}

            def bpmis_project_ticket_upsert_synced(self, **kwargs):
                self._record("bpmis_project_ticket_upsert_synced", **kwargs)
                return {"ticket_key": "AF-1"}

            def bpmis_project_ticket_delete(self, **kwargs):
                self._record("bpmis_project_ticket_delete", **kwargs)
                return True

            def bpmis_project_ticket_status_update(self, **kwargs):
                self._record("bpmis_project_ticket_status_update", **kwargs)
                return True

            def bpmis_project_ticket_version_update(self, **kwargs):
                self._record("bpmis_project_ticket_version_update", **kwargs)
                return True

            def team_dashboard_config_load(self):
                self._record("team_dashboard_config_load")
                return {"teams": {}}

            def team_dashboard_config_save(self, config):
                self._record("team_dashboard_config_save", config)
                return config

            def source_code_qa_sessions_list(self, **kwargs):
                self._record("source_code_qa_sessions_list", **kwargs)
                return [{"session_id": "s1"}]

            def source_code_qa_session_create(self, **kwargs):
                self._record("source_code_qa_session_create", **kwargs)
                return {"session_id": "s1"}

            def source_code_qa_session_get(self, **kwargs):
                self._record("source_code_qa_session_get", **kwargs)
                return {"session_id": "s1"}

            def source_code_qa_session_archive(self, **kwargs):
                self._record("source_code_qa_session_archive", **kwargs)
                return {"archived": True}

            def source_code_qa_session_context(self, **kwargs):
                self._record("source_code_qa_session_context", **kwargs)
                return {"messages": []}

            def source_code_qa_session_append(self, **kwargs):
                self._record("source_code_qa_session_append", **kwargs)
                return {"session_id": "s1"}

            def source_code_qa_session_pending(self, **kwargs):
                self._record("source_code_qa_session_pending", **kwargs)
                return {"session_id": "s1"}

            def source_code_qa_attachment_save(self, **kwargs):
                self._record("source_code_qa_attachment_save", **kwargs)
                return {"id": "att-1"}

            def source_code_qa_attachments_resolve(self, **kwargs):
                self._record("source_code_qa_attachments_resolve", **kwargs)
                return [{"id": "att-1"}]

            def source_code_qa_attachment_get(self, **kwargs):
                self._record("source_code_qa_attachment_get", **kwargs)
                return {"id": "att-1"}, b"attachment"

            def source_code_qa_generated_artifact_save(self, **kwargs):
                self._record("source_code_qa_generated_artifact_save", **kwargs)
                return {"id": "artifact-1"}

            def source_code_qa_generated_artifact_get(self, **kwargs):
                self._record("source_code_qa_generated_artifact_get", **kwargs)
                return {"id": "artifact-1"}, b"artifact"

            def source_code_qa_runtime_evidence_list(self, **kwargs):
                self._record("source_code_qa_runtime_evidence_list", **kwargs)
                return [{"id": "e1"}]

            def source_code_qa_runtime_evidence_save(self, **kwargs):
                self._record("source_code_qa_runtime_evidence_save", **kwargs)
                return {"id": "e1"}

            def source_code_qa_runtime_evidence_resolve(self, **kwargs):
                self._record("source_code_qa_runtime_evidence_resolve", **kwargs)
                return [{"id": "e1"}]

            def source_code_qa_runtime_evidence_delete(self, **kwargs):
                self._record("source_code_qa_runtime_evidence_delete", **kwargs)
                return True

            def seatalk_todos_completed_ids(self, **kwargs):
                self._record("seatalk_todos_completed_ids", **kwargs)
                return ["todo-1"]

            def seatalk_todos_open(self, **kwargs):
                self._record("seatalk_todos_open", **kwargs)
                return [{"id": "todo-1"}]

            def seatalk_todos_processed_until(self, **kwargs):
                self._record("seatalk_todos_processed_until", **kwargs)
                return "2026-05-01T00:00:00Z"

            def seatalk_todos_mark_processed_until(self, **kwargs):
                self._record("seatalk_todos_mark_processed_until", **kwargs)

            def seatalk_todos_merge_open(self, **kwargs):
                self._record("seatalk_todos_merge_open", **kwargs)
                return [{"id": "todo-1"}]

            def seatalk_todo_complete(self, **kwargs):
                self._record("seatalk_todo_complete", **kwargs)
                return {"completed": True}

            def seatalk_name_mappings_get(self):
                self._record("seatalk_name_mappings_get")
                return {"group": "Risk"}

            def seatalk_name_mappings_merge(self, mappings):
                self._record("seatalk_name_mappings_merge", mappings)
                return mappings

        fake = FakeStoreClient()
        dashboard = RemoteSeaTalkDashboardService(fake, name_mappings_provider=lambda: {"group": "Risk"})
        self.assertTrue(dashboard.build_overview()["overview"])
        self.assertTrue(dashboard.build_insights(todo_since="2026-05-01")["insights"])
        self.assertTrue(dashboard.build_project_updates()["projects"])
        self.assertTrue(dashboard.build_todos(todo_since="2026-05-01")["todos"])
        self.assertEqual(dashboard.build_name_mappings(force_refresh=True)["mappings"]["group"], "Risk")
        self.assertEqual(dashboard.export_history_text(), ("history", "history.txt"))
        self.assertEqual(dashboard.export_history_since(since=datetime(2026, 5, 1), conversation_scope="monthly"), "history since")

        projects = RemoteBPMISProjectStore(fake)
        self.assertEqual(projects.upsert_project(user_key="u", bpmis_id="225159", project_name="P", brd_link="", market="SG"), "stored-id")
        self.assertEqual(projects.list_projects(user_key="u")[0]["bpmis_id"], "225159")
        self.assertEqual(projects.reorder_projects(user_key="u", bpmis_ids=["225159"])[0]["bpmis_id"], "225159")
        self.assertEqual(projects.get_project(user_key="u", bpmis_id="225159")["bpmis_id"], "225159")
        self.assertIsNone(projects.get_project(user_key="u", bpmis_id="missing"))
        self.assertTrue(projects.soft_delete_project(user_key="u", bpmis_id="225159"))
        self.assertTrue(projects.update_project_comment(user_key="u", bpmis_id="225159", pm_comment="ok"))
        self.assertEqual(projects.add_jira_ticket(user_key="u")["ticket_key"], "AF-1")
        self.assertEqual(projects.upsert_synced_jira_ticket(user_key="u")["ticket_key"], "AF-1")
        self.assertTrue(projects.delete_jira_ticket(user_key="u", bpmis_id="225159", ticket_id="1"))
        self.assertTrue(projects.update_jira_ticket_status(user_key="u", bpmis_id="225159", ticket_id="1", status="Done"))
        self.assertTrue(projects.update_jira_ticket_version(user_key="u", bpmis_id="225159", ticket_id="1", version_name="26Q2", version_id="88"))

        team_config = RemoteTeamDashboardConfigStore(fake)
        self.assertEqual(team_config.load()["teams"], {})
        self.assertEqual(team_config.save({"teams": {"AF": {}}})["teams"], {"AF": {}})

        sessions = RemoteSourceCodeQASessionStore(fake)
        self.assertEqual(sessions.list(owner_email="owner@npt.sg")[0]["session_id"], "s1")
        self.assertEqual(sessions.create(owner_email="owner@npt.sg")["session_id"], "s1")
        self.assertEqual(sessions.get("s1", owner_email="owner@npt.sg")["session_id"], "s1")
        self.assertTrue(sessions.archive("s1", owner_email="owner@npt.sg")["archived"])
        self.assertEqual(sessions.get_context("s1", owner_email="owner@npt.sg")["messages"], [])
        self.assertEqual(sessions.append_exchange("s1", owner_email="owner@npt.sg")["session_id"], "s1")
        self.assertEqual(sessions.append_pending_question("s1", owner_email="owner@npt.sg")["session_id"], "s1")

        attachments = RemoteSourceCodeQAAttachmentStore(fake)
        self.assertEqual(attachments.save_bytes(owner_email="owner@npt.sg", session_id="s1", filename="a.txt", content=b"x")["id"], "att-1")
        self.assertEqual(attachments.resolve_many(owner_email="owner@npt.sg", session_id="s1", attachment_ids=["att-1"])[0]["id"], "att-1")
        self.assertEqual(attachments.get_bytes(owner_email="owner@npt.sg", session_id="s1", attachment_id="att-1")[1], b"attachment")

        artifacts = RemoteSourceCodeQAGeneratedArtifactStore(fake)
        self.assertEqual(artifacts.save_sql_package(owner_email="owner@npt.sg", session_id="s1", pm_team="AF", country="All", question="q", sql="select 1", readme="r")["id"], "artifact-1")
        self.assertEqual(artifacts.get_bytes(owner_email="owner@npt.sg", session_id="s1", artifact_id="artifact-1")[1], b"artifact")

        evidence = RemoteSourceCodeQARuntimeEvidenceStore(fake)
        self.assertEqual(evidence.list(pm_team="AF", country="All")[0]["id"], "e1")
        self.assertEqual(evidence.save_bytes(pm_team="AF", country="All", source_type="log", uploaded_by="owner", filename="log.txt", content=b"x")["id"], "e1")
        self.assertEqual(evidence.resolve_scope(pm_team="AF", country="All")[0]["id"], "e1")
        self.assertTrue(evidence.delete(pm_team="AF", country="All", evidence_id="e1"))

        todos = RemoteSeaTalkTodoStore(fake)
        self.assertEqual(todos.completed_ids(owner_email="owner@npt.sg"), {"todo-1"})
        self.assertEqual(todos.open_todos(owner_email="owner@npt.sg")[0]["id"], "todo-1")
        self.assertEqual(todos.processed_until(owner_email="owner@npt.sg"), "2026-05-01T00:00:00Z")
        self.assertIsNone(todos.mark_processed_until(owner_email="owner@npt.sg", processed_until="2026-05-01T00:00:00Z"))
        self.assertEqual(todos.merge_open_todos(owner_email="owner@npt.sg", todos=[])[0]["id"], "todo-1")
        self.assertTrue(todos.mark_completed(owner_email="owner@npt.sg", todo={"id": "todo-1"})["completed"])

        mappings = RemoteSeaTalkNameMappingStore(fake)
        self.assertEqual(mappings.mappings()["group"], "Risk")
        self.assertEqual(mappings.merge_mappings({"group": "Risk"})["group"], "Risk")

    def test_remote_bpmis_client_exposes_live_jira_operations(self):
        calls = []

        class FakeClient:
            last_bpmis_request_stats = {"api_call_count": 2}
            last_bpmis_request_timings = {"issue_tree_reporter": 1.2}

            def bpmis_call(self, *, operation, access_token, args=None, kwargs=None):
                calls.append((operation, access_token, args or [], kwargs or {}))
                if operation == "get_jira_ticket_detail":
                    return {"status": {"label": "In Progress"}}
                if operation == "find_project":
                    return {"project_id": "221664", "raw": {"project_name": "Project Match"}}
                if operation == "create_jira_ticket":
                    return {"ticket_key": "AF-1", "ticket_link": "https://jira/AF-1", "raw": {"ok": True}}
                if operation == "search_biz_projects_by_title_keywords":
                    return [{"bpmis_id": "221664", "project_name": "Project Match"}]
                if operation == "search_versions":
                    return [{"name": "Planning_26Q2"}]
                if operation == "get_brd_doc_links_for_projects":
                    return {"221664": ["https://brd"]}
                if operation == "update_jira_ticket_status":
                    return {"status": {"label": "Testing"}}
                if operation == "update_biz_project_status":
                    return {"status": {"label": "Developing"}}
                if operation == "update_jira_ticket_fix_version":
                    return {"fixVersions": ["Planning_26Q4"]}
                if operation == "link_jira_ticket_to_project":
                    return {"parentIds": [221664]}
                if operation == "delink_jira_ticket_from_project":
                    return {"parentIds": []}
                return None

        from bpmis_jira_tool.local_agent_client import RemoteBPMISClient

        remote = RemoteBPMISClient(FakeClient(), access_token="token")

        self.assertIsNone(remote.ping())
        project = remote.find_project("221664")
        self.assertEqual(project.project_id, "221664")
        created = remote.create_jira_ticket(project, {"summary": "Task"}, preformatted_summary=True)
        self.assertEqual(created.ticket_key, "AF-1")
        self.assertEqual(remote.list_biz_projects_for_pm_email("pm@npt.sg"), [])
        self.assertEqual(remote.list_biz_projects_for_pm_emails(["pm@npt.sg"]), [])
        self.assertEqual(remote.get_jira_ticket_detail("SPDBP-95742")["status"]["label"], "In Progress")
        self.assertEqual(remote.search_biz_projects_by_title_keywords("Project Match", max_pages=2)[0]["bpmis_id"], "221664")
        self.assertEqual(remote.list_jira_tasks_for_project_created_by_email("221664", "pm@npt.sg"), [])
        self.assertEqual(remote.list_jira_tasks_for_projects_created_by_emails(["221664"], ["pm@npt.sg"]), {})
        self.assertEqual(
            remote.list_jira_tasks_created_by_emails(
                ["pm@npt.sg"],
                max_pages=2,
                enrich_missing_parent=False,
                created_after="2026-01-01",
                release_after="2026-02-01",
                release_before="2026-03-01",
            ),
            [],
        )
        self.assertEqual(remote.get_single_brd_doc_link_for_project("221664"), "")
        self.assertEqual(remote.get_single_brd_doc_links_for_projects(["221664"]), {})
        self.assertEqual(remote.search_versions("Planning")[0]["name"], "Planning_26Q2")
        self.assertEqual(remote.list_issues_for_version("88"), [])
        self.assertEqual(remote.list_actual_mandays_for_projects(["221664"]), {})
        self.assertEqual(remote.get_issue_detail("221664"), {})
        self.assertEqual(remote.get_brd_doc_links_for_projects(["221664"])["221664"], ["https://brd"])
        self.assertEqual(remote.get_jira_ticket_details(["SPDBP-95742"]), {})
        self.assertEqual(remote.update_jira_ticket_status("SPDBP-95742", "Testing")["status"]["label"], "Testing")
        self.assertEqual(remote.update_biz_project_status("221664", "Developing")["status"]["label"], "Developing")
        self.assertEqual(remote.update_jira_ticket_fix_version("SPDBP-95742", "Planning_26Q4")["fixVersions"], ["Planning_26Q4"])
        self.assertEqual(remote.link_jira_ticket_to_project("SPDBP-95742", "221664")["parentIds"], [221664])
        self.assertEqual(remote.delink_jira_ticket_from_project("SPDBP-95742", "221664")["parentIds"], [])
        self.assertGreaterEqual(remote.request_stats["api_call_count"], 2)
        self.assertGreaterEqual(remote.request_timings["issue_tree_reporter"], 1.2)
        remote._merge_request_stats("not-dict")
        remote._merge_request_stats({"bad": "NaN", "max_seen": 3})
        remote._merge_request_timings("not-dict")
        remote._merge_request_timings({"bad": "not-a-number", "elapsed": 0.2})
        self.assertEqual(remote.request_stats["max_seen"], 3)
        self.assertGreaterEqual(remote.request_timings["elapsed"], 0.2)

    def test_remote_source_code_qa_service_reuses_config_payload_within_instance(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0
                self.query_payloads = []

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

            def source_code_qa_sync(self, *, pm_team, country):
                return {"status": "ok", "pm_team": pm_team, "country": country}

            def source_code_qa_ensure_synced_today(self, *, pm_team, country, background=False):
                return {"status": "ok", "background": background}

            def source_code_qa_query(self, payload, *, progress_callback=None):
                self.query_payloads.append((payload, progress_callback))
                return {"status": "ok", "answer": "remote"}

        class FakeFallbackService:
            llm_provider_name = "codex_cli_bridge"
            llm_budgets = {}
            codex_timeout_seconds = 0

            def normalize_query_llm_provider(self, llm_provider):
                return llm_provider or self.llm_provider_name

            def options_payload(self):
                return {"providers": ["codex_cli_bridge"]}

            def llm_policy_payload(self):
                return {"provider": "fallback"}

            def _llm_fallback_model(self):
                return "gpt-fallback"

            def with_llm_provider(self, llm_provider):
                clone = FakeFallbackService()
                clone.llm_provider_name = llm_provider
                clone.codex_timeout_seconds = self.codex_timeout_seconds
                return clone

            def with_codex_timeout_seconds(self, codex_timeout_seconds):
                clone = FakeFallbackService()
                clone.llm_provider_name = self.llm_provider_name
                clone.codex_timeout_seconds = codex_timeout_seconds or 0
                return clone

        from bpmis_jira_tool.local_agent_client import RemoteSourceCodeQAService

        client = FakeClient()
        remote = RemoteSourceCodeQAService(client, FakeFallbackService(), llm_provider="codex_cli_bridge")

        self.assertEqual(remote.options_payload()["providers"], ["codex_cli_bridge"])
        self.assertTrue(remote.llm_ready())
        self.assertTrue(remote.git_auth_ready())
        self.assertEqual(remote.llm_policy_payload()["provider"], "codex_cli_bridge")
        self.assertEqual(remote.index_health_payload()["status"], "ok")
        self.assertTrue(remote.domain_knowledge_payload()["enabled"])
        self.assertEqual(remote._llm_fallback_model(), "gpt-fallback")
        self.assertIn("AF:All", remote.load_config()["mappings"])
        self.assertEqual(client.calls, 1)
        self.assertEqual(remote.sync(pm_team="AF", country="All")["pm_team"], "AF")
        self.assertFalse(remote.ensure_synced_today(pm_team="AF", country="All")["background"])
        self.assertTrue(remote.ensure_synced_today_background(pm_team="AF", country="All")["background"])
        remote.save_mapping(pm_team="AF", country="All", repositories=[])
        self.assertIn("AF:All", remote.load_config()["mappings"])
        self.assertEqual(client.calls, 2)
        progress_callback = lambda *_args: None
        remote_with_timeout = remote.with_codex_timeout_seconds(45)
        self.assertEqual(
            remote_with_timeout.query(question="Where?", progress_callback=progress_callback)["answer"],
            "remote",
        )
        self.assertEqual(client.query_payloads[-1][0]["codex_timeout_seconds"], 45)
        self.assertIs(client.query_payloads[-1][1], progress_callback)
        remote.with_llm_provider("openai").query(question="No callback", progress_callback="ignore")
        self.assertEqual(client.query_payloads[-1][0]["llm_provider"], "openai")
        self.assertIsNone(client.query_payloads[-1][1])


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
