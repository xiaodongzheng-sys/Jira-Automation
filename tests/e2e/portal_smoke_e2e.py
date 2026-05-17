from __future__ import annotations

import os
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.error import URLError
from urllib.request import urlopen
from urllib.parse import urlparse

from cryptography.fernet import Fernet


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ADMIN_EMAIL = "xiaodong.zheng@npt.sg"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_healthz(base_url: str, *, timeout_seconds: float = 20.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urlopen(f"{base_url}/healthz", timeout=1.0) as response:
                if response.status == 200:
                    return
        except (OSError, URLError) as error:
            last_error = error
        time.sleep(0.2)
    raise RuntimeError(f"Portal did not become healthy at {base_url}: {last_error}")


def _admin_session_cookie_value(env: dict[str, str]) -> str:
    from bpmis_jira_tool.web import create_app

    previous = os.environ.copy()
    os.environ.update(env)
    try:
        app = create_app()
        serializer = app.session_interface.get_signing_serializer(app)
        if serializer is None:
            raise RuntimeError("Could not create Flask session serializer.")
        return serializer.dumps(
            {
                "google_profile": {"email": ADMIN_EMAIL, "name": "Xiaodong Zheng"},
                "google_credentials": {"token": "e2e-token"},
            }
        )
    finally:
        os.environ.clear()
        os.environ.update(previous)


class PortalE2ESmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as error:
            raise unittest.SkipTest("Install Playwright with: ./.venv/bin/python -m pip install -r requirements-e2e.txt") from error

        cls._temp_dir = tempfile.TemporaryDirectory()
        cls._base_url = os.getenv("BROWSER_E2E_BASE_URL", "").rstrip("/")
        cls._server_process: subprocess.Popen[str] | None = None
        cls._managed_server = not cls._base_url
        cls._env = {
            **os.environ,
            "ENV_FILE": os.devnull,
            "FLASK_SECRET_KEY": "browser-e2e-secret",
            "TEAM_PORTAL_DATA_DIR": cls._temp_dir.name,
            "TEAM_PORTAL_BASE_URL": "",
            "TEAM_ALLOWED_EMAIL_DOMAINS": "",
            "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": Fernet.generate_key().decode("utf-8"),
            "PYTHONPATH": str(PROJECT_ROOT),
        }
        if cls._managed_server:
            port = _free_port()
            cls._base_url = f"http://127.0.0.1:{port}"
            cls._server_process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "flask",
                    "--app",
                    "app",
                    "run",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                ],
                cwd=PROJECT_ROOT,
                env=cls._env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            _wait_for_healthz(cls._base_url)

        cls._session_cookie = _admin_session_cookie_value(cls._env)
        cls._playwright = sync_playwright().start()
        browser_name = os.getenv("BROWSER_E2E_BROWSER", "chromium").strip() or "chromium"
        browser_type = getattr(cls._playwright, browser_name, None)
        if browser_type is None:
            raise unittest.SkipTest(f"Unsupported Playwright browser: {browser_name}")
        headless = os.getenv("BROWSER_E2E_HEADLESS", "1") != "0"
        try:
            cls._browser = browser_type.launch(headless=headless)
        except Exception as error:  # noqa: BLE001 - produce an actionable local setup hint.
            cls._playwright.stop()
            raise unittest.SkipTest(
                "Install Playwright browsers with: ./.venv/bin/python -m playwright install chromium"
            ) from error

    @classmethod
    def tearDownClass(cls) -> None:
        browser = getattr(cls, "_browser", None)
        if browser is not None:
            browser.close()
        playwright = getattr(cls, "_playwright", None)
        if playwright is not None:
            playwright.stop()
        process = getattr(cls, "_server_process", None)
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        temp_dir = getattr(cls, "_temp_dir", None)
        if temp_dir is not None:
            temp_dir.cleanup()

    def _new_admin_page(self):
        context = self._browser.new_context(base_url=self._base_url, viewport={"width": 1280, "height": 900})
        context.add_cookies(
            [
                {
                    "name": "session",
                    "value": self._session_cookie,
                    "url": self._base_url,
                    "httpOnly": True,
                    "sameSite": "Lax",
                }
            ]
        )
        page = context.new_page()
        page_errors: list[str] = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        self.addCleanup(context.close)
        self.addCleanup(lambda: self.assertEqual([], page_errors))
        return page

    def test_admin_homepage_loads_without_browser_errors(self) -> None:
        page = self._new_admin_page()

        page.goto("/", wait_until="domcontentloaded")

        self.assertIn("Source Code Q&A", page.locator("body").inner_text(timeout=5000))

    def test_source_code_qa_chat_answer_and_followup_context_smoke(self) -> None:
        page = self._new_admin_page()
        session_id = "e2e-session-1"
        captured_queries: list[dict[str, object]] = []
        session_payload = {
            "id": session_id,
            "title": "New Source Code Chat",
            "pm_team": "AF",
            "country": "All",
            "llm_provider": "codex_cli_bridge",
            "messages": [],
            "last_context": {},
            "updated_at": "2026-05-17T04:00:00Z",
        }

        def json_response(payload: dict[str, object]):
            return {
                "status": 200,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def source_config(route):
            route.fulfill(**json_response({
                "status": "ok",
                "answer_mode": "auto",
                "query_mode": "deep",
                "can_manage": True,
                "auth": {"mode": "google", "email": ADMIN_EMAIL},
                "git_auth_ready": True,
                "llm_ready": True,
                "llm_provider": "codex_cli_bridge",
                "llm_providers": {
                    "codex_cli_bridge": {"ready": True, "label": "Codex", "available": True},
                },
                "llm_model": "codex-e2e",
                "llm_policy": {"provider": {"provider": "codex_cli_bridge"}, "router": {"version": "e2e"}},
                "index_health": {
                    "status": "ok",
                    "totals": {"ready": 1, "repos": 1, "files": 12, "lines": 1200, "definitions": 24, "semantic_chunks": 8},
                    "keys": {"AF:All": {"freshness": {"warning": ""}}},
                },
                "release_gate": {"status": "pass"},
                "domain_knowledge": {},
                "config": {
                    "mappings": {
                        "AF:All": [{"display_name": "AF Portal", "url": "https://gitlab.example/af.git"}],
                    },
                },
                "options": {
                    "all_country": "All",
                    "countries": ["SG", "ID", "PH"],
                    "runtime_capabilities": {},
                    "llm_providers": [{"value": "codex_cli_bridge", "label": "Codex", "available": True}],
                },
            }))

        def sessions(route):
            request = route.request
            if request.method == "GET":
                parsed = urlparse(request.url)
                if parsed.path.endswith(f"/sessions/{session_id}"):
                    route.fulfill(**json_response({"status": "ok", "session": session_payload}))
                    return
                route.fulfill(**json_response({"status": "ok", "sessions": [session_payload] if session_payload["messages"] else []}))
                return
            if request.method == "POST":
                route.fulfill(**json_response({"status": "ok", "session": session_payload}))
                return
            route.fallback()

        def query(route):
            payload = route.request.post_data_json
            captured_queries.append(payload)
            question = str(payload.get("question") or "")
            answer = (
                "First e2e answer cites the route handler [bpmis_jira_tool/web_source_code_qa_routes.py:1]."
                if len(captured_queries) == 1
                else "Follow-up e2e answer kept prior chat context."
            )
            session_payload["title"] = question[:60] or "Source Code Chat"
            session_payload["messages"] = [
                *session_payload["messages"],
                {"role": "user", "text": question, "created_at": "2026-05-17T04:00:01Z", "attachments": []},
                {
                    "role": "assistant",
                    "text": answer,
                    "created_at": "2026-05-17T04:00:02Z",
                    "payload": {
                        "llm_provider": "codex_cli_bridge",
                        "llm_model": "codex-e2e",
                        "trace_id": f"trace-e2e-{len(captured_queries)}",
                    },
                },
            ]
            session_payload["last_context"] = {
                "question": question,
                "summary": f"Summary for {question}",
                "answer": answer,
                "trace_id": f"trace-e2e-{len(captured_queries)}",
            }
            route.fulfill(**json_response({
                "status": "ok",
                "summary": f"Search completed for {question}",
                "llm_answer": answer,
                "answer_mode": "auto",
                "query_mode": "deep",
                "llm_provider": "codex_cli_bridge",
                "llm_model": "codex-e2e",
                "trace_id": f"trace-e2e-{len(captured_queries)}",
                "matches": [
                    {
                        "repo": "AF Portal",
                        "path": "bpmis_jira_tool/web_source_code_qa_routes.py",
                        "line_start": 1,
                        "line_end": 20,
                        "score": 0.97,
                        "reason": "Route registration evidence",
                        "snippet": "def register_source_code_qa_routes(...):",
                    }
                ],
                "repo_status": [{"display_name": "AF Portal", "state": "ready", "message": "Indexed", "path": "/tmp/af"}],
                "session": session_payload,
                "session_id": session_id,
            }))

        page.route("**/api/source-code-qa/config", source_config)
        page.route("**/api/source-code-qa/sessions", sessions)
        page.route(f"**/api/source-code-qa/sessions/{session_id}", sessions)
        page.route("**/api/source-code-qa/query", query)

        page.goto("/source-code-qa", wait_until="domcontentloaded")
        page.locator("[data-source-config-status]").wait_for(timeout=5000)
        page.locator("[data-source-new-session]").click()
        page.wait_for_function(
            "() => document.querySelector('[data-source-query-status]')?.textContent?.includes('New chat ready.')",
            timeout=5000,
        )

        page.locator("[data-source-question]").fill("Where is Source Code QA registered?")
        page.locator("[data-source-query]").click()
        page.locator("[data-source-session-messages]").get_by_text("First e2e answer cites the route handler").wait_for(timeout=5000)
        self.assertEqual(captured_queries[0].get("session_id"), session_id)
        initial_context = captured_queries[0].get("conversation_context") or {}
        self.assertNotIn("question", initial_context)
        self.assertNotIn("answer", initial_context)

        page.locator("[data-source-question]").fill("Can you explain the previous answer?")
        page.locator("[data-source-query]").click()
        page.locator("[data-source-session-messages]").get_by_text("Follow-up e2e answer kept prior chat context.").wait_for(timeout=5000)

        self.assertEqual(len(captured_queries), 2)
        followup_context = captured_queries[1].get("conversation_context")
        self.assertIsInstance(followup_context, dict)
        self.assertEqual(followup_context.get("question"), "Where is Source Code QA registered?")
        self.assertIn("First e2e answer", str(followup_context.get("answer") or ""))

    def test_team_dashboard_version_plan_remarks_edit_persists_after_render(self) -> None:
        page = self._new_admin_page()
        captured_cell_updates: list[dict[str, object]] = []
        version_plan_payload = {
            "status": "ok",
            "priority_order": ["SP", "P0", "P1", "P2", "P3"],
            "pm_options": ["Wang Chang", "Zoey", "Rene", "TBC"],
            "sync_state": {"state": "idle", "last_synced_date_sgt": "2026-05-17"},
            "bundles": [
                {
                    "version_id": "af-20260520",
                    "af_version_name": "AF_1.0.76_20260520",
                    "prd_final_date": "2026-05-08",
                    "af_release_date": "2026-05-20",
                    "mapped_versions": {
                        "DBPSG": {"version_name": "DBPSG_v2.85_0526"},
                        "DBPID": {"version_name": "DBPID_v2.85_0526"},
                        "DBPPH": {"version_name": "DBPPH_v2.85_0526"},
                    },
                    "synced_rows": [
                        {
                            "row_id": "sync-af-20260520-SPDBP-94945",
                            "row_type": "synced",
                            "jira_id": "SPDBP-94945",
                            "jira_link": "https://jira.shopee.io/browse/SPDBP-94945",
                            "jira_summary": "AMR UIUX Improvement",
                            "market": "Regional",
                            "priority": "P0",
                            "pm": ["Wang Chang"],
                            "productization_efforts": "Y",
                            "remarks": "Keep this note",
                        }
                    ],
                    "manual_rows": [],
                }
            ],
            "pipeline_rows": [
                {
                    "row_id": "pipe-1",
                    "row_type": "manual",
                    "feature": "Pipeline fallback",
                    "priority": "SP",
                    "pm": ["TBC"],
                    "productization_efforts": "",
                    "remarks": "",
                }
            ],
            "archived_bundles": [],
        }

        def json_response(payload: dict[str, object]):
            return {
                "status": 200,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def config(route):
            route.fulfill(**json_response({
                "status": "ok",
                "config": {
                    "teams": {
                        "AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]},
                        "CRMS": {"label": "Credit Risk", "member_emails": []},
                        "GRC": {"label": "Ops Risk", "member_emails": []},
                    },
                    "task_cache": {},
                },
            }))

        def version_plan(route):
            route.fulfill(**json_response(version_plan_payload))

        def save_cell(route):
            payload = route.request.post_data_json
            captured_cell_updates.append(payload)
            target_row_id = str(payload.get("row_id") or "")
            field = str(payload.get("field") or "")
            for bundle in version_plan_payload["bundles"]:
                for row in bundle["synced_rows"]:
                    if row["row_id"] == target_row_id and field:
                        row[field] = payload.get("value")
            route.fulfill(**json_response(version_plan_payload))

        page.route("**/api/team-dashboard/config", config)
        page.route("**/api/team-dashboard/version-plan/af", version_plan)
        page.route("**/api/team-dashboard/version-plan/af/cell", save_cell)

        page.goto("/team-dashboard", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-tab="version-plan"]').click()
        row = page.locator('[data-version-plan-row-id="sync-af-20260520-SPDBP-94945"]')
        row.get_by_text("AMR UIUX Improvement").wait_for(timeout=5000)

        remarks = row.locator('[data-version-plan-cell="remarks"]')
        self.assertEqual(remarks.input_value(timeout=5000), "Keep this note")
        remarks.fill("Updated via Playwright")
        remarks.dispatch_event("change")

        page.wait_for_function(
            "() => document.querySelector('[data-version-plan-status]')?.textContent?.includes('Saved.')",
            timeout=5000,
        )
        updated_row = page.locator('[data-version-plan-row-id="sync-af-20260520-SPDBP-94945"]')
        self.assertEqual(updated_row.locator('[data-version-plan-cell="remarks"]').input_value(timeout=5000), "Updated via Playwright")
        self.assertEqual(captured_cell_updates[-1]["scope"], "bundle")
        self.assertEqual(captured_cell_updates[-1]["version_id"], "af-20260520")
        self.assertEqual(captured_cell_updates[-1]["row_id"], "sync-af-20260520-SPDBP-94945")
        self.assertEqual(captured_cell_updates[-1]["field"], "remarks")
        self.assertEqual(captured_cell_updates[-1]["value"], "Updated via Playwright")

    def test_admin_vpn_entrypoint_renders_navigation_and_assets(self) -> None:
        page = self._new_admin_page()

        page.goto("/vpn-connection", wait_until="domcontentloaded")

        self.assertEqual("VPN Connection", page.locator("h1").first.inner_text(timeout=5000))
        self.assertEqual("Others", page.locator(".site-switcher-tab.is-active").first.inner_text(timeout=5000))
        self.assertTrue(page.locator("[data-vpn-root]").is_visible(timeout=5000))

    def test_vpn_save_connect_second_password_does_not_leak_secrets(self) -> None:
        page = self._new_admin_page()
        saved_profiles: list[dict[str, object]] = []
        captured_save_payloads: list[dict[str, object]] = []
        captured_connect_payloads: list[dict[str, object]] = []
        profile_id = "vpn-e2e-1"
        password = "vpn-secret-e2e"
        second_password = "second-secret-e2e"

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def public_profiles(*, connected: bool = False):
            profiles = []
            for profile in saved_profiles:
                public_profile = {key: value for key, value in profile.items() if key != "password"}
                if connected and public_profile["id"] == profile_id:
                    public_profile["last_connected_at"] = "2026-05-17T04:30:00Z"
                profiles.append(public_profile)
            return profiles

        def vpn_api(route):
            request = route.request
            parsed = urlparse(request.url)
            path = parsed.path
            if request.method == "GET" and path.endswith("/api/vpn-connection/profiles"):
                route.fulfill(**json_response({
                    "profiles": public_profiles(),
                    "vpn_status": {"status": "ok", "connected": False, "state": "Disconnected", "message": "state: Disconnected"},
                    "hosts": ["Seabank PH", "ShopeeVPN"],
                }))
                return
            if request.method == "POST" and path.endswith("/api/vpn-connection/profiles"):
                payload = request.post_data_json
                captured_save_payloads.append(payload)
                saved_profiles[:] = [
                    {
                        "id": profile_id,
                        "display_name": payload.get("display_name") or "",
                        "vpn_host": payload.get("vpn_host") or "",
                        "username": payload.get("username") or "",
                        "password": payload.get("password") or "",
                    }
                ]
                route.fulfill(**json_response({
                    "profile": {key: value for key, value in saved_profiles[0].items() if key != "password"},
                    "profiles": public_profiles(),
                    "vpn_status": {"status": "ok", "connected": False, "state": "Disconnected", "message": "state: Disconnected"},
                    "hosts": ["Seabank PH", "ShopeeVPN"],
                }))
                return
            if request.method == "POST" and path.endswith(f"/api/vpn-connection/profiles/{profile_id}/connect"):
                payload = request.post_data_json
                captured_connect_payloads.append(payload)
                route.fulfill(**json_response({
                    "profiles": public_profiles(connected=True),
                    "vpn_status": {"status": "ok", "connected": True, "state": "Connected", "message": "state: Connected"},
                    "hosts": ["Seabank PH", "ShopeeVPN"],
                }))
                return
            if request.method == "POST" and path.endswith("/api/vpn-connection/disconnect"):
                route.fulfill(**json_response({
                    "profiles": public_profiles(),
                    "vpn_status": {"status": "ok", "connected": False, "state": "Disconnected", "message": "state: Disconnected"},
                    "hosts": ["Seabank PH", "ShopeeVPN"],
                }))
                return
            route.fulfill(**json_response({"status": "error", "message": f"Unhandled VPN route: {request.method} {path}"}, status=404))

        page.route("**/api/vpn-connection/**", vpn_api)
        page.goto("/vpn-connection", wait_until="domcontentloaded")
        page.locator('[name="display_name"]').fill("Seabank PH VPN")
        page.locator('[name="vpn_host"]').fill("Seabank PH")
        page.locator('[name="username"]').fill("vpn-user")
        page.locator('[name="password"]').fill(password)
        page.locator('[data-vpn-form] button[type="submit"]').click()
        page.locator("[data-vpn-inline-status]").get_by_text("Profile saved.").wait_for(timeout=5000)
        page.locator(f'[data-profile-id="{profile_id}"]').get_by_text("Seabank PH VPN").wait_for(timeout=5000)

        page.once("dialog", lambda dialog: dialog.accept(second_password))
        page.locator(f'[data-vpn-connect="{profile_id}"]').click()
        page.locator("[data-vpn-inline-status]").get_by_text("VPN connected.").wait_for(timeout=5000)
        self.assertTrue(page.locator(f'[data-vpn-disconnect-profile="{profile_id}"]').is_visible(timeout=5000))

        self.assertEqual(captured_save_payloads[-1]["password"], password)
        self.assertEqual(captured_connect_payloads[-1]["second_password"], second_password)
        dom_surface = page.evaluate(
            "() => [document.body.innerText, ...Array.from(document.querySelectorAll('input, textarea')).map((node) => node.value || '')].join('\\n')"
        )
        self.assertNotIn(password, dom_surface)
        self.assertNotIn(second_password, dom_surface)

    def test_meeting_recorder_calendar_start_stop_archive_smoke(self) -> None:
        page = self._new_admin_page()
        record_id = "meeting-e2e-1"
        captured_start_payloads: list[dict[str, object]] = []
        captured_stop_ids: list[str] = []
        records: list[dict[str, object]] = []

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def active_record() -> dict[str, object]:
            return {
                "record_id": record_id,
                "title": "Regional Risk Standup",
                "platform": "google_meet",
                "status": "recording",
                "recording_started_at": "2026-05-17T02:00:00Z",
                "recording_stopped_at": "",
                "transcript_language": "zh",
                "transcript_language_label": "Chinese",
                "media": {},
                "transcript": {},
                "minutes": {},
                "diagnostics_snapshot": {
                    "audio_capture_label": "ScreenCaptureKit",
                    "audio_input": "System Audio",
                },
                "audio_preflight": {"status": "ok"},
                "recording_health": {"status": "ok"},
            }

        def stopped_record() -> dict[str, object]:
            record = dict(active_record())
            record.update(
                {
                    "status": "recorded",
                    "recording_stopped_at": "2026-05-17T02:08:00Z",
                    "media": {"audio_url": f"/meeting-recorder/assets/{record_id}/meeting.wav"},
                }
            )
            return record

        def meeting_recorder_api(route):
            request = route.request
            parsed = urlparse(request.url)
            path = parsed.path
            if request.method == "GET" and path.endswith("/api/meeting-recorder/diagnostics"):
                route.fulfill(**json_response({
                    "status": "ok",
                    "ffmpeg_configured": True,
                    "whisper_cpp_configured": True,
                    "whisper_model_exists": True,
                    "system_audio_configured": True,
                    "audio_capture_label": "ScreenCaptureKit",
                    "audio_input": "System Audio",
                    "meeting_audio_setup_note": "System audio and microphone ready.",
                    "audio_devices": ["System Audio", "MacBook Microphone"],
                }))
                return
            if request.method == "GET" and path.endswith("/api/meeting-recorder/calendar/upcoming"):
                route.fulfill(**json_response({
                    "status": "ok",
                    "meetings": [
                        {
                            "title": "Regional Risk Standup",
                            "platform": "google_meet",
                            "meeting_link": "https://meet.google.com/abc-defg-hij",
                            "calendar_event_id": "calendar-e2e-1",
                            "start": "2026-05-17T02:00:00Z",
                            "end": "2026-05-17T02:30:00Z",
                            "attendees": [{"email": "pm@example.com"}],
                        }
                    ],
                }))
                return
            if request.method == "GET" and path.endswith("/api/meeting-recorder/records"):
                route.fulfill(**json_response({"status": "ok", "records": records}))
                return
            if request.method == "POST" and path.endswith("/api/meeting-recorder/start"):
                payload = request.post_data_json
                captured_start_payloads.append(payload)
                records[:] = [active_record()]
                route.fulfill(**json_response({"status": "ok", "record": records[0]}))
                return
            if request.method == "GET" and path.endswith(f"/api/meeting-recorder/records/{record_id}"):
                route.fulfill(**json_response({"status": "ok", "record": records[0] if records else active_record()}))
                return
            if request.method == "POST" and path.endswith(f"/api/meeting-recorder/records/{record_id}/stop"):
                captured_stop_ids.append(record_id)
                records[:] = [stopped_record()]
                route.fulfill(**json_response({"status": "ok", "record": records[0], "auto_process_error": ""}))
                return
            route.fulfill(**json_response({"status": "error", "message": f"Unhandled Meeting Recorder route: {request.method} {path}"}, status=404))

        page.route("**/api/meeting-recorder/**", meeting_recorder_api)
        page.goto("/meeting-recorder", wait_until="domcontentloaded")

        self.assertEqual("Meeting Recorder", page.locator("h1").first.inner_text(timeout=5000))
        page.locator("[data-meeting-recorder-diagnostic]").get_by_text("Ready", exact=True).wait_for(timeout=5000)
        page.locator("[data-meeting-calendar-status]").get_by_text("1 upcoming meeting(s).").wait_for(timeout=5000)
        page.get_by_text("Regional Risk Standup").first.wait_for(timeout=5000)

        page.locator('[data-meeting-start-index="0"]').click()
        page.locator("[data-meeting-recording-status]").get_by_text("Recording: Regional Risk Standup").wait_for(timeout=5000)
        page.locator("[data-meeting-record-detail]").get_by_text("Audio download will be available after stopping the recording.").wait_for(timeout=5000)
        self.assertEqual(captured_start_payloads[-1]["title"], "Regional Risk Standup")
        self.assertEqual(captured_start_payloads[-1]["platform"], "google_meet")
        self.assertEqual(captured_start_payloads[-1]["meeting_link"], "https://meet.google.com/abc-defg-hij")
        self.assertEqual(captured_start_payloads[-1]["recording_mode"], "audio_only")
        self.assertEqual(captured_start_payloads[-1]["transcript_language"], "zh")

        page.locator(f'[data-record-stop="{record_id}"]').click()
        page.locator("[data-meeting-recording-status]").get_by_text("No active recording.").wait_for(timeout=5000)
        page.locator("[data-meeting-record-detail]").get_by_text("Download audio file").wait_for(timeout=5000)
        page.locator("[data-meeting-record-detail]").get_by_text("Process").wait_for(timeout=5000)
        self.assertEqual(captured_stop_ids, [record_id])

    def test_work_memory_superagent_feedback_timeline_and_gate_smoke(self) -> None:
        page = self._new_admin_page()
        captured_feedback: list[dict[str, object]] = []
        captured_superagent_queries: list[dict[str, object]] = []
        captured_quality_gates: list[dict[str, object]] = []

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def work_memory_api(route):
            request = route.request
            parsed = urlparse(request.url)
            path = parsed.path
            if request.method == "GET" and path.endswith("/api/work-memory/health"):
                route.fulfill(**json_response({
                    "status": "ok",
                    "item_count": 12,
                    "feedback_count": 3,
                    "materialized_count": 2,
                    "by_source": [{"source_type": "meeting_recorder", "count": 7}, {"source_type": "team_dashboard", "count": 5}],
                    "ingestion_runs": [
                        {
                            "source_type": "meeting_recorder",
                            "status": "completed",
                            "completed_at": "2026-05-17T03:00:00Z",
                            "scanned_count": 4,
                            "matched_count": 3,
                            "recorded_count": 2,
                            "duplicate_count": 1,
                            "failed_count": 0,
                        }
                    ],
                }))
                return
            if request.method == "GET" and path.endswith("/api/work-memory/review-candidates"):
                route.fulfill(**json_response({
                    "status": "ok",
                    "items": [
                        {
                            "item_id": "memory-e2e-1",
                            "source_type": "meeting_recorder",
                            "item_type": "decision",
                            "visibility": "owner",
                            "summary": "SPDBP-12345 launch scope changed after risk review.",
                            "observed_at": "2026-05-17T02:40:00Z",
                            "metadata": {"attribution_scope": "meeting"},
                        }
                    ],
                }))
                return
            if request.method == "POST" and path.endswith("/api/work-memory/feedback"):
                payload = request.post_data_json
                captured_feedback.append(payload)
                route.fulfill(**json_response({"status": "ok", "item_id": payload.get("item_id"), "action": payload.get("action")}))
                return
            if request.method == "GET" and path.endswith("/api/work-memory/project-timeline"):
                self.assertEqual(parsed.query, "project_ref=SPDBP-12345")
                route.fulfill(**json_response({
                    "status": "ok",
                    "items": [
                        {
                            "item_id": "timeline-e2e-1",
                            "source_type": "team_dashboard",
                            "item_type": "project_update",
                            "visibility": "owner",
                            "summary": "Risk launch timeline event is ready for PM follow-up.",
                            "observed_at": "2026-05-17T03:15:00Z",
                            "metadata": {},
                        }
                    ],
                }))
                return
            route.fulfill(**json_response({"status": "error", "message": f"Unhandled Work Memory route: {request.method} {path}"}, status=404))

        def superagent_api(route):
            request = route.request
            parsed = urlparse(request.url)
            path = parsed.path
            if request.method == "POST" and path.endswith("/api/superagent/query"):
                payload = request.post_data_json
                captured_superagent_queries.append(payload)
                route.fulfill(**json_response({
                    "status": "ok",
                    "answer_contract_version": "superagent_quality_gate_v1",
                    "confidence": "high",
                    "direct_answer": "Risk project is on track with one follow-up on launch readiness.",
                    "sections": [
                        {"title": "Next actions", "items": ["Confirm launch owner", "Close scope note"]}
                    ],
                    "evidence": [
                        {
                            "source_type": "meeting_recorder",
                            "item_type": "decision",
                            "visibility": "owner",
                            "summary": "Decision evidence from Meeting Recorder.",
                            "excerpt": "Launch scope was agreed.",
                        }
                    ],
                    "unknowns": ["Final UAT owner not confirmed"],
                }))
                return
            if request.method == "POST" and path.endswith("/api/superagent/quality-gate"):
                payload = request.post_data_json
                captured_quality_gates.append(payload)
                route.fulfill(**json_response({
                    "status": "ok",
                    "answer_contract_version": "superagent_quality_gate_v1",
                    "quality_gate": {
                        "gate_status": "pass",
                        "passed_count": 3,
                        "failed_count": 0,
                        "case_count": 3,
                        "failed_cases": [],
                    },
                }))
                return
            route.fulfill(**json_response({"status": "error", "message": f"Unhandled Superagent route: {request.method} {path}"}, status=404))

        page.route("**/api/work-memory/**", work_memory_api)
        page.route("**/api/superagent/**", superagent_api)
        page.goto("/work-memory", wait_until="domcontentloaded")

        self.assertEqual("AI Memory", page.locator("h1").first.inner_text(timeout=5000))
        self.assertEqual(page.locator("[data-memory-item-count]").inner_text(timeout=5000), "12")
        page.locator("[data-memory-status]").get_by_text("meeting_recorder: 7").wait_for(timeout=5000)
        page.locator("[data-memory-candidates]").get_by_text("SPDBP-12345 launch scope changed").wait_for(timeout=5000)

        page.locator('[data-memory-action="accept"][data-item-id="memory-e2e-1"]').click()
        page.wait_for_function("() => document.querySelector('[data-memory-feedback-count]')?.textContent === '3'", timeout=5000)
        self.assertEqual(captured_feedback[-1]["item_id"], "memory-e2e-1")
        self.assertEqual(captured_feedback[-1]["action"], "accept")

        page.locator('[data-memory-timeline-form] input[name="project_ref"]').fill("SPDBP-12345")
        page.locator('[data-memory-timeline-form] button[type="submit"]').click()
        page.locator("[data-memory-timeline]").get_by_text("Risk launch timeline event is ready").wait_for(timeout=5000)

        page.locator('[data-superagent-form] select[name="task_type"]').select_option("project_status")
        page.locator('[data-superagent-form] input[name="query"]').fill("SPDBP-12345 launch status")
        page.locator('[data-superagent-form] button[type="submit"]').click()
        page.locator("[data-superagent-answer]").get_by_text("Risk project is on track").wait_for(timeout=5000)
        page.locator("[data-superagent-answer]").get_by_text("answer contract").wait_for(timeout=5000)
        self.assertEqual(captured_superagent_queries[-1]["task_type"], "project_status")
        self.assertEqual(captured_superagent_queries[-1]["visibility_scope"], "owner")

        page.locator("[data-superagent-quality-gate]").click()
        page.locator("[data-superagent-answer]").get_by_text("Quality gate: pass").wait_for(timeout=5000)
        self.assertEqual(captured_quality_gates[-1]["suite_id"], "gold_v1")
        self.assertEqual(captured_quality_gates[-1]["min_cases"], 1)


if __name__ == "__main__":
    unittest.main()
