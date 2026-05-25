from __future__ import annotations

import os
import json
from pathlib import Path
import re
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
TEAMMATE_EMAIL = "jireh.tanyx@npt.sg"


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


def _session_cookie_value(
    env: dict[str, str],
    *,
    email: str,
    name: str,
    credentials: dict[str, object] | None = None,
) -> str:
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
                "google_profile": {"email": email, "name": name},
                "google_credentials": credentials or {"token": "e2e-token"},
            }
        )
    finally:
        os.environ.clear()
        os.environ.update(previous)


def _admin_session_cookie_value(env: dict[str, str]) -> str:
    return _session_cookie_value(env, email=ADMIN_EMAIL, name="Xiaodong Zheng")


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
            "TEAM_ALLOWED_EMAIL_DOMAINS": "npt.sg",
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
        cls._teammate_session_cookie = _session_cookie_value(
            cls._env,
            email=TEAMMATE_EMAIL,
            name="Team Mate",
        )
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

    def _new_page(self, *, session_cookie: str | None = None):
        context = self._browser.new_context(base_url=self._base_url, viewport={"width": 1280, "height": 900})
        if session_cookie:
            context.add_cookies(
                [
                    {
                        "name": "session",
                        "value": session_cookie,
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

    def _new_admin_page(self):
        return self._new_page(session_cookie=self._session_cookie)

    def _new_teammate_page(self):
        return self._new_page(session_cookie=self._teammate_session_cookie)

    @staticmethod
    def _fetch_json(page, method: str, path: str, payload: dict[str, object] | None = None) -> dict[str, object]:
        return page.evaluate(
            """async ({method, path, payload}) => {
                const response = await fetch(path, {
                    method,
                    headers: {
                        "Accept": "application/json",
                        "Content-Type": "application/json"
                    },
                    body: payload === null ? undefined : JSON.stringify(payload)
                });
                let body = null;
                try {
                    body = await response.json();
                } catch (error) {
                    body = {raw: await response.text()};
                }
                return {status: response.status, body};
            }""",
            {"method": method, "path": path, "payload": payload},
        )

    def test_admin_homepage_loads_without_browser_errors(self) -> None:
        page = self._new_admin_page()

        page.goto("/", wait_until="domcontentloaded")

        self.assertIn("Source Code Q&A", page.locator("body").inner_text(timeout=5000))

    def test_cloud_home_opens_standalone_version_plan_smoke(self) -> None:
        cloud_temp_dir = tempfile.TemporaryDirectory()
        process: subprocess.Popen[str] | None = None
        context = None
        try:
            port = _free_port()
            cloud_base_url = f"http://127.0.0.1:{port}"
            cloud_env = {
                **self._env,
                "TEAM_PORTAL_DATA_DIR": cloud_temp_dir.name,
                "TEAM_PORTAL_CLOUD_HOME_ENABLED": "true",
                "TEAM_PORTAL_MAC_FULL_PORTAL_URL": "http://127.0.0.1:9/portal-home",
                "TEAM_PORTAL_BASE_URL": "",
            }
            process = subprocess.Popen(
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
                env=cloud_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            _wait_for_healthz(cloud_base_url)
            session_cookie = _session_cookie_value(cloud_env, email="jireh.tanyx@npt.sg", name="AF Team Mate")
            context = self._browser.new_context(base_url=cloud_base_url, viewport={"width": 1280, "height": 900})
            context.add_cookies(
                [
                    {
                        "name": "session",
                        "value": session_cookie,
                        "url": cloud_base_url,
                        "httpOnly": True,
                        "sameSite": "Lax",
                    }
                ]
            )
            page = context.new_page()
            page_errors: list[str] = []
            page.on("pageerror", lambda error: page_errors.append(str(error)))

            def version_plan(route):
                route.fulfill(
                    status=200,
                    content_type="application/json",
                    body=json.dumps(
                        {
                            "status": "ok",
                            "can_sync": False,
                            "document_revision": "cloud-home-rev-1",
                            "store_backend": "firestore",
                            "store_environment": "uat",
                            "priority_order": ["SP", "P0", "P1", "P2", "P3"],
                            "pm_options": ["TBC"],
                            "sync_state": {"state": "idle", "last_synced_date_sgt": "2026-05-17"},
                            "bundles": [],
                            "pipeline_rows": [
                                {
                                    "row_id": "cloud-home-pipe-1",
                                    "row_type": "manual",
                                    "feature": "Cloud homepage version plan row",
                                    "priority": "SP",
                                    "pm": ["TBC"],
                                    "productization_efforts": "",
                                    "remarks": "",
                                }
                            ],
                            "archived_bundles": [],
                        }
                    ),
                )

            page.route(re.compile(r".*/api/team-dashboard/version-plan/af(?:\?.*)?$"), version_plan)
            page.goto("/", wait_until="domcontentloaded")
            self.assertIn("Risk PM Workspace", page.locator("body").inner_text(timeout=5000))
            self.assertEqual(page.locator(".site-switcher").count(), 0)
            page.get_by_role("link", name="Open Version Plan").click()
            page.wait_for_url("**/version-plan", timeout=5000)
            self.assertEqual(page.locator(".site-switcher").count(), 0)
            page.locator('[data-version-plan-row-id="cloud-home-pipe-1"]').get_by_text(
                "Cloud homepage version plan row"
            ).wait_for(timeout=5000)
            self.assertEqual(page.locator('[data-team-dashboard-tab="tasks"]').count(), 0)

            non_af_cookie = _session_cookie_value(cloud_env, email="sophia.wangzj@npt.sg", name="Sophia Wang")
            non_af_context = self._browser.new_context(base_url=cloud_base_url, viewport={"width": 1280, "height": 900})
            try:
                non_af_context.add_cookies(
                    [
                        {
                            "name": "session",
                            "value": non_af_cookie,
                            "url": cloud_base_url,
                            "httpOnly": True,
                            "sameSite": "Lax",
                        }
                    ]
                )
                non_af_page = non_af_context.new_page()
                non_af_errors: list[str] = []
                non_af_page.on("pageerror", lambda error: non_af_errors.append(str(error)))
                non_af_page.goto("/", wait_until="domcontentloaded")
                non_af_body = non_af_page.locator("body").inner_text(timeout=5000)
                self.assertIn("Risk PM Workspace", non_af_body)
                self.assertNotIn("Open Version Plan", non_af_body)
                self.assertEqual(non_af_page.locator("[data-version-plan-card]").count(), 0)
                non_af_page.goto("/version-plan", wait_until="domcontentloaded")
                self.assertEqual(urlparse(non_af_page.url).path, "/access-denied")
                self.assertEqual([], non_af_errors)
            finally:
                non_af_context.close()
            self.assertEqual([], page_errors)
        finally:
            if context is not None:
                context.close()
            if process is not None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            cloud_temp_dir.cleanup()

    def test_auth_and_access_control_browser_smoke(self) -> None:
        logged_out = self._new_page()

        logged_out.goto("/source-code-qa", wait_until="domcontentloaded")
        logged_out_body = logged_out.locator("body").inner_text(timeout=5000)
        self.assertIn("Continue with Google", logged_out_body)
        self.assertFalse(logged_out.locator("[data-source-question]").is_visible())

        logged_out_api = self._fetch_json(logged_out, "GET", "/api/team-dashboard/config")
        self.assertEqual(logged_out_api["status"], 401)
        self.assertEqual(logged_out_api["body"]["status"], "error")
        self.assertIn("Sign in with your NPT Google account", logged_out_api["body"]["message"])

        teammate = self._new_teammate_page()
        teammate.goto("/source-code-qa", wait_until="domcontentloaded")
        teammate.locator("[data-source-question]").wait_for(timeout=5000)
        self.assertEqual(teammate.locator("[data-source-view-tab='admin']").count(), 0)
        self.assertEqual(teammate.locator("[data-source-view-tab='effort']").count(), 0)

        source_config = self._fetch_json(teammate, "GET", "/api/source-code-qa/config")
        self.assertEqual(source_config["status"], 200)
        self.assertFalse(source_config["body"]["auth"]["can_manage"])

        def teammate_version_plan(route):
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "status": "ok",
                        "can_sync": False,
                        "priority_order": ["SP", "P0", "P1", "P2", "P3"],
                        "pm_options": ["TBC"],
                        "sync_state": {"state": "idle", "last_synced_date_sgt": "2026-05-17"},
                        "bundles": [],
                        "pipeline_rows": [
                            {
                                "row_id": "pipe-readonly-1",
                                "row_type": "manual",
                                "feature": "Visible version plan row",
                                "priority": "SP",
                                "pm": ["TBC"],
                                "productization_efforts": "",
                                "remarks": "",
                            }
                        ],
                        "archived_bundles": [],
                    }
                ),
            )

        teammate.route(re.compile(r".*/api/team-dashboard/version-plan/af(?:\?.*)?$"), teammate_version_plan)
        teammate.goto("/team-dashboard", wait_until="domcontentloaded")
        teammate.locator('[data-team-dashboard-tab="version-plan"]').wait_for(timeout=5000)
        self.assertEqual(teammate.locator('[data-team-dashboard-tab="tasks"]').count(), 0)
        self.assertEqual(teammate.locator('[data-team-dashboard-tab="admin"]').count(), 0)
        self.assertEqual(teammate.locator('[data-team-dashboard-tab="monthly-report"]').count(), 0)
        teammate.locator('[data-version-plan-row-id="pipe-readonly-1"]').get_by_text("Visible version plan row").wait_for(timeout=5000)
        self.assertFalse(teammate.locator("[data-version-plan-sync]").is_visible())

        blocked_pages = [
            ("/reports", "/access-denied"),
            ("/prd-briefing/", "/access-denied"),
            ("/vpn-connection", "/access-denied"),
            ("/meeting-recorder", "/access-denied"),
            ("/meeting-translation", "/access-denied"),
        ]
        for path, expected_final_path in blocked_pages:
            with self.subTest(path=path):
                teammate.goto(path, wait_until="domcontentloaded")
                self.assertEqual(urlparse(teammate.url).path, expected_final_path)

        removed_surface_response = teammate.goto("/work-memory", wait_until="domcontentloaded")
        self.assertIsNotNone(removed_surface_response)
        self.assertEqual(removed_surface_response.status, 404)

        blocked_apis = [
            ("GET", "/api/team-dashboard/config", None),
            ("GET", "/api/team-dashboard/monthly-report/template", None),
            ("GET", "/api/meeting-recorder/diagnostics", None),
            ("GET", "/api/vpn-connection/profiles", None),
            ("POST", "/api/meeting-translation/start", {"target_language": "en"}),
            ("POST", "/api/source-code-qa/sync", {"pm_team": "AF", "country": "All"}),
        ]
        for method, path, payload in blocked_apis:
            with self.subTest(path=path):
                result = self._fetch_json(teammate, method, path, payload)
                self.assertEqual(result["status"], 403)
                self.assertEqual(result["body"]["status"], "error")

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

    def test_source_code_qa_local_agent_unavailable_graceful_degradation_smoke(self) -> None:
        page = self._new_admin_page()
        session_id = "e2e-local-agent-offline-session"
        job_id = "e2e-local-agent-offline-job"
        captured_queries: list[dict[str, object]] = []
        session_payload = {
            "id": session_id,
            "title": "New Source Code Chat",
            "pm_team": "AF",
            "country": "All",
            "llm_provider": "codex_cli_bridge",
            "messages": [],
            "last_context": {},
            "updated_at": "2026-05-17T05:00:00Z",
        }

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
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
                    "totals": {"ready": 1, "repos": 1, "files": 8, "lines": 600, "definitions": 12, "semantic_chunks": 4},
                    "keys": {"AF:All": {"freshness": {"warning": ""}}},
                },
                "release_gate": {"status": "pass"},
                "domain_knowledge": {},
                "config": {"mappings": {"AF:All": [{"display_name": "AF Portal", "url": "https://gitlab.example/af.git"}]}},
                "options": {
                    "all_country": "All",
                    "countries": ["SG", "ID", "PH"],
                    "runtime_capabilities": {},
                    "llm_providers": [{"value": "codex_cli_bridge", "label": "Codex", "available": True}],
                },
            }))

        def sessions(route):
            if route.request.method == "GET":
                route.fulfill(**json_response({"status": "ok", "sessions": []}))
                return
            if route.request.method == "POST":
                route.fulfill(**json_response({"status": "ok", "session": session_payload}))
                return
            route.fallback()

        def query(route):
            captured_queries.append(route.request.post_data_json)
            route.fulfill(**json_response({
                "status": "queued",
                "state": "queued",
                "message": "Queued Source Code Q&A query.",
                "job_id": job_id,
                "session_id": session_id,
                "error_retryable": True,
            }))

        def job_status(route):
            if route.request.url.endswith("/events"):
                route.fulfill(status=503, content_type="text/plain", body="local-agent offline")
                return
            route.fulfill(**json_response({
                "status": "error",
                "state": "failed",
                "job_id": job_id,
                "message": "Mac local-agent is unavailable: connection refused",
                "error": "Mac local-agent is unavailable: connection refused",
                "error_category": "local_agent_offline",
                "error_code": "local_agent_unavailable",
                "error_retryable": True,
            }))

        page.route("**/api/source-code-qa/config", source_config)
        page.route("**/api/source-code-qa/sessions", sessions)
        page.route("**/api/source-code-qa/query", query)
        page.route("**/api/source-code-qa/query-jobs/**", job_status)

        page.goto("/source-code-qa", wait_until="domcontentloaded")
        page.locator("[data-source-config-status]").wait_for(timeout=5000)
        page.locator("[data-source-new-session]").click()
        page.wait_for_function(
            "() => document.querySelector('[data-source-query-status]')?.textContent?.includes('New chat ready.')",
            timeout=5000,
        )

        page.locator("[data-source-question]").fill("Why is Source Code QA offline?")
        page.locator("[data-source-query]").click()
        page.wait_for_function(
            "() => document.querySelector('[data-source-query-status]')?.textContent?.includes('Mac local-agent is unavailable')",
            timeout=8000,
        )
        page.locator("[data-source-session-messages]").get_by_text("Confirm the host stack is online").wait_for(timeout=5000)
        page.locator("[data-source-reconnect-job]").get_by_text("Reconnect").wait_for(timeout=5000)
        page.locator("[data-source-retry-question]").get_by_text("Retry query").wait_for(timeout=5000)

        self.assertEqual(captured_queries[-1]["session_id"], session_id)
        self.assertTrue(captured_queries[-1]["async"])

    def test_prd_self_assessment_non_admin_review_async_smoke(self) -> None:
        page = self._new_teammate_page()
        captured_review_payloads: list[dict[str, object]] = []
        prd_url = "https://confluence.example/display/AF/Risk+Launch+PRD"
        job_id = "prd-self-assessment-review-e2e-job"

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def latest(route):
            route.fulfill(**json_response({"status": "empty", "latest": {}}))

        def sections(route):
            payload = route.request.post_data_json
            self.assertEqual(payload["prd_url"], prd_url)
            route.fulfill(**json_response({
                "status": "ok",
                "prd": {"title": "Risk Launch PRD", "source_url": prd_url},
                "sections": [
                    {"index": 1, "title": "Background", "char_count": 1200, "linked_spreadsheet_count": 0},
                    {"index": 2, "title": "Launch Controls", "char_count": 2400, "linked_spreadsheet_count": 1},
                ],
            }))

        def review(route):
            payload = route.request.post_data_json
            captured_review_payloads.append(payload)
            route.fulfill(**json_response({
                "status": "queued",
                "job_id": job_id,
                "message": "PRD review queued.",
            }))

        def job_status(route):
            route.fulfill(**json_response({
                "status": "ok",
                "state": "completed",
                "message": "PRD review completed.",
                "results": [
                    {
                        "status": "ok",
                        "language": "en",
                        "prd": {"title": "Risk Launch PRD", "source_url": prd_url},
                        "review": {
                            "result_markdown": "### Review Result\n- Delivery logic is mostly ready.\n- Confirm rollback owner before launch.",
                            "updated_at": "2026-05-17T07:00:00Z",
                        },
                        "coverage": {
                            "mode": "section_selected",
                            "sections_assessed": 2,
                            "selected_sections_total": 2,
                            "sections_total": 2,
                            "selected_section_titles": ["Background", "Launch Controls"],
                            "report_templates_total": 1,
                            "report_templates_reviewed": 1,
                            "confluence_tables_total": 1,
                            "confluence_tables_reviewed": 1,
                            "google_sheet_screenshots_total": 1,
                            "google_sheet_screenshots_reviewed": 1,
                        },
                    }
                ],
            }))

        page.route("**/api/prd-self-assessment/latest", latest)
        page.route("**/api/prd-self-assessment/sections", sections)
        page.route("**/api/prd-self-assessment/review", review)
        page.route("**/api/jobs/*", job_status)

        page.goto("/prd-self-assessment", wait_until="domcontentloaded")
        page.locator("[data-prd-self-assessment]").wait_for(timeout=5000)
        self.assertEqual("PRD Self-Assessment", page.locator("h1").first.inner_text(timeout=5000))
        self.assertEqual(page.locator('[data-prd-self-assessment-action="summary"]').count(), 0)

        summary_api = self._fetch_json(page, "POST", "/api/prd-self-assessment/summary", {"prd_url": prd_url})
        self.assertEqual(summary_api["status"], 403)

        page.locator("[data-prd-self-assessment-url]").fill("not-a-url")
        page.locator('[data-prd-self-assessment-action="review"]').click()
        page.locator("[data-prd-self-assessment-status]").get_by_text("Enter a valid Confluence page URL.").wait_for(timeout=5000)

        page.locator("[data-prd-self-assessment-url]").fill(prd_url)
        page.locator("[data-prd-self-assessment-language]").select_option("en")
        page.locator("[data-prd-self-assessment-load-sections]").click()
        page.locator("[data-prd-self-assessment-section-summary]").get_by_text("2/2 sections selected").wait_for(timeout=5000)
        page.locator('[data-prd-self-assessment-action="review"]').click()

        page.locator("[data-prd-self-assessment-status]").get_by_text("AI PRD review generated.").wait_for(timeout=5000)
        page.locator("[data-prd-self-assessment-result]").get_by_text("Delivery logic is mostly ready").wait_for(timeout=5000)
        page.locator("[data-prd-self-assessment-result]").get_by_text("Reviewed sections: 2/2 selected").wait_for(timeout=5000)
        self.assertEqual(captured_review_payloads[-1]["prd_url"], prd_url)
        self.assertEqual(captured_review_payloads[-1]["language"], "en")
        self.assertTrue(captured_review_payloads[-1]["async"])
        self.assertEqual(captured_review_payloads[-1]["selected_section_indexes"], [1, 2])

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
        page.route(re.compile(r".*/api/team-dashboard/version-plan/af(?:\?.*)?$"), version_plan)
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

    def test_team_dashboard_version_plan_manual_row_controls_smoke(self) -> None:
        page = self._new_admin_page()
        captured_row_actions: list[dict[str, object]] = []
        captured_cell_updates: list[dict[str, object]] = []
        version_plan_payload = {
            "status": "ok",
            "can_sync": True,
            "priority_order": ["SP", "P0", "P1", "P2", "P3"],
            "pm_options": ["Wang Chang", "Zoey", "Rene", "TBC"],
            "sync_state": {"state": "idle", "last_synced_date_sgt": "2026-05-17"},
            "bundles": [],
            "pipeline_rows": [
                {
                    "row_id": "pipe-1",
                    "row_type": "manual",
                    "feature": "Existing pipeline row",
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
                    "teams": {"AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]}},
                    "task_cache": {},
                },
            }))

        def version_plan(route):
            route.fulfill(**json_response(version_plan_payload))

        def save_rows(route):
            payload = route.request.post_data_json
            captured_row_actions.append(payload)
            action = payload.get("action")
            if action == "add":
                version_plan_payload["pipeline_rows"].append(
                    {
                        "row_id": payload.get("row_id"),
                        "row_type": "manual",
                        "feature": "",
                        "priority": "SP",
                        "pm": ["TBC"],
                        "productization_efforts": "",
                        "remarks": "",
                    }
                )
            elif action == "reorder":
                order = [str(item) for item in payload.get("row_ids", [])]
                by_id = {row["row_id"]: row for row in version_plan_payload["pipeline_rows"]}
                version_plan_payload["pipeline_rows"] = [by_id[row_id] for row_id in order if row_id in by_id]
            elif action == "delete":
                row_id = payload.get("row_id")
                version_plan_payload["pipeline_rows"] = [
                    row for row in version_plan_payload["pipeline_rows"] if row["row_id"] != row_id
                ]
            route.fulfill(**json_response(version_plan_payload))

        def save_cell(route):
            payload = route.request.post_data_json
            captured_cell_updates.append(payload)
            for row in version_plan_payload["pipeline_rows"]:
                if row["row_id"] == payload.get("row_id"):
                    row[str(payload.get("field") or "")] = payload.get("value")
            route.fulfill(**json_response(version_plan_payload))

        page.route("**/api/team-dashboard/config", config)
        page.route(re.compile(r".*/api/team-dashboard/version-plan/af(?:\?.*)?$"), version_plan)
        page.route("**/api/team-dashboard/version-plan/af/rows", save_rows)
        page.route("**/api/team-dashboard/version-plan/af/cell", save_cell)

        page.goto("/team-dashboard", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-tab="version-plan"]').click()
        page.locator('[data-version-plan-row-id="pipe-1"]').get_by_text("Existing pipeline row").wait_for(timeout=5000)

        page.locator('[data-version-plan-row-action="add"][data-version-plan-scope="pipeline"]').click()
        added_row = page.locator('[data-version-plan-row-id^="manual-client-"]').first
        added_row.wait_for(timeout=5000)
        page.wait_for_function(
            "() => document.querySelector('[data-version-plan-status]')?.textContent?.includes('Saved.')",
            timeout=5000,
        )
        added_row_id = added_row.get_attribute("data-version-plan-row-id")
        self.assertTrue(added_row_id)
        self.assertEqual(captured_row_actions[-1]["action"], "add")
        self.assertEqual(captured_row_actions[-1]["scope"], "pipeline")
        self.assertEqual(captured_row_actions[-1]["row_id"], added_row_id)

        feature_input = added_row.locator('[data-version-plan-cell="feature"]')
        feature_input.fill("Manual row from browser smoke")
        feature_input.dispatch_event("change")
        page.wait_for_function(
            "() => document.querySelector('[data-version-plan-status]')?.textContent?.includes('Saved.')",
            timeout=5000,
        )
        self.assertEqual(captured_cell_updates[-1]["row_id"], added_row_id)
        self.assertEqual(captured_cell_updates[-1]["field"], "feature")
        self.assertEqual(captured_cell_updates[-1]["value"], "Manual row from browser smoke")

        self.assertEqual(added_row.locator('[data-version-plan-row-action="up"]').count(), 0)
        self.assertEqual(added_row.locator('[data-version-plan-row-action="down"]').count(), 0)
        self.assertEqual(added_row.locator(".team-dashboard-version-plan-drag").count(), 1)

        page.locator(f'[data-version-plan-row-id="{added_row_id}"] [data-version-plan-row-action="delete"]').click()
        page.locator(f'[data-version-plan-row-id="{added_row_id}"]').wait_for(state="detached", timeout=5000)
        self.assertEqual(captured_row_actions[-1]["action"], "delete")
        self.assertEqual(captured_row_actions[-1]["row_id"], added_row_id)

    def test_team_dashboard_version_plan_conflict_refreshes_and_retries_row_action_smoke(self) -> None:
        page = self._new_admin_page()
        captured_row_actions: list[dict[str, object]] = []
        version_plan_gets = 0
        conflict_seen = False
        version_plan_payload = {
            "status": "ok",
            "can_sync": True,
            "document_revision": "rev-old",
            "priority_order": ["SP", "P0", "P1", "P2", "P3"],
            "pm_options": ["TBC"],
            "sync_state": {"state": "idle", "last_synced_date_sgt": "2026-05-22"},
            "bundles": [],
            "pipeline_rows": [
                {
                    "row_id": "pipe-1",
                    "row_type": "manual",
                    "feature": "Existing conflict row",
                    "priority": "SP",
                    "pm": ["TBC"],
                    "productization_efforts": "",
                    "remarks": "",
                }
            ],
            "archived_bundles": [],
        }

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def config(route):
            route.fulfill(**json_response({
                "status": "ok",
                "config": {
                    "teams": {"AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]}},
                    "task_cache": {},
                },
            }))

        def version_plan(route):
            nonlocal version_plan_gets
            version_plan_gets += 1
            if version_plan_gets > 1 and "sync=0" in route.request.url:
                version_plan_payload["document_revision"] = "rev-new"
            route.fulfill(**json_response(version_plan_payload))

        def save_rows(route):
            nonlocal conflict_seen
            payload = route.request.post_data_json
            captured_row_actions.append(payload)
            if not conflict_seen:
                conflict_seen = True
                route.fulfill(**json_response({
                    "status": "error",
                    "message": "Version Plan was updated by another session. Refresh and try again.",
                    "error_category": "version_plan_conflict",
                }, status=409))
                return
            self.assertEqual(payload.get("document_revision"), "rev-new")
            version_plan_payload["pipeline_rows"].append(
                {
                    "row_id": payload.get("row_id"),
                    "row_type": "manual",
                    "feature": "Added after conflict refresh",
                    "priority": "SP",
                    "pm": ["TBC"],
                    "productization_efforts": "",
                    "remarks": "",
                }
            )
            route.fulfill(**json_response(version_plan_payload))

        page.route("**/api/team-dashboard/config", config)
        page.route(re.compile(r".*/api/team-dashboard/version-plan/af(?:\?.*)?$"), version_plan)
        page.route("**/api/team-dashboard/version-plan/af/rows", save_rows)

        page.goto("/team-dashboard", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-tab="version-plan"]').click()
        page.locator('[data-version-plan-row-id="pipe-1"]').get_by_text("Existing conflict row").wait_for(timeout=5000)

        page.locator('[data-version-plan-row-action="add"][data-version-plan-scope="pipeline"]').click()

        added_row = page.locator('[data-version-plan-row-id^="manual-client-"]').first
        added_row.wait_for(timeout=5000)
        page.wait_for_function(
            "() => document.querySelector('[data-version-plan-status]')?.textContent?.includes('Saved.')",
            timeout=5000,
        )
        added_row_id = added_row.get_attribute("data-version-plan-row-id")
        self.assertTrue(added_row_id)
        self.assertEqual([item["document_revision"] for item in captured_row_actions], ["rev-old", "rev-new"])
        self.assertEqual(captured_row_actions[-1]["row_id"], added_row_id)
        self.assertGreaterEqual(version_plan_gets, 2)

    def test_team_dashboard_version_plan_pm_filter_smoke(self) -> None:
        page = self._new_admin_page()
        version_plan_request_count = 0
        version_plan_payload = {
            "status": "ok",
            "can_sync": True,
            "priority_order": ["SP", "P0", "P1", "P2", "P3"],
            "pm_options": ["Wang Chang", "Zoey", "Jireh", "Ker Yin", "Rene", "Jun Wei"],
            "sync_state": {"state": "idle", "last_synced_date_sgt": "2026-05-17"},
            "bundles": [
                {
                    "version_id": "af-20260520",
                    "af_version_name": "AF_v1.0.80_20260529",
                    "prd_final_date": "2026-04-22",
                    "mapped_versions": {},
                    "synced_rows": [
                        {
                            "row_id": "sync-zoey",
                            "row_type": "synced",
                            "jira_id": "SGDB-1",
                            "jira_link": "https://jira.example/SGDB-1",
                            "jira_summary": "Zoey synced row",
                            "market": "SG",
                            "priority": "SP",
                            "pm": ["Zoey"],
                            "productization_efforts": "N",
                            "remarks": "",
                        },
                    ],
                    "manual_rows": [
                        {
                            "row_id": "manual-rene",
                            "row_type": "manual",
                            "feature": "Rene manual row",
                            "priority": "P1",
                            "pm": ["Rene"],
                            "productization_efforts": "Y",
                            "remarks": "",
                        }
                    ],
                }
            ],
            "pipeline_rows": [
                {
                    "row_id": "pipe-empty",
                    "row_type": "manual",
                    "feature": "No PM pipeline row",
                    "priority": "SP",
                    "pm": ["TBC"],
                    "productization_efforts": "",
                    "remarks": "",
                },
                {
                    "row_id": "pipe-zoey",
                    "row_type": "manual",
                    "feature": "Zoey pipeline row",
                    "priority": "P0",
                    "pm": ["Zoey"],
                    "productization_efforts": "N",
                    "remarks": "",
                },
            ],
            "archived_bundles": [
                {
                    "version_id": "af-archived",
                    "af_version_name": "AF_v1.0.79_20260515",
                    "prd_final_date": "2026-04-08",
                    "mapped_versions": {},
                    "synced_rows": [
                        {
                            "row_id": "arch-junwei",
                            "row_type": "synced",
                            "jira_id": "SPDBP-2",
                            "jira_link": "https://jira.example/SPDBP-2",
                            "jira_summary": "Jun Wei archived row",
                            "market": "Regional",
                            "priority": "P2",
                            "pm": ["Jun Wei"],
                            "productization_efforts": "Y",
                            "remarks": "",
                        },
                    ],
                }
            ],
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
                    "teams": {"AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]}},
                    "task_cache": {},
                },
            }))

        def version_plan(route):
            nonlocal version_plan_request_count
            version_plan_request_count += 1
            route.fulfill(**json_response(version_plan_payload))

        page.route("**/api/team-dashboard/config", config)
        page.route(re.compile(r".*/api/team-dashboard/version-plan/af(?:\?.*)?$"), version_plan)

        page.goto("/team-dashboard", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-tab="version-plan"]').click()
        page.locator('[data-version-plan-row-id="sync-zoey"]').get_by_text("Zoey synced row").wait_for(timeout=5000)

        pm_filter = page.locator('[data-version-plan-pm-filter]')
        self.assertEqual(
            pm_filter.locator("option").all_text_contents(),
            ["All PMs", "-", "Wang Chang", "Zoey", "Jireh", "Ker Yin", "Rene", "Jun Wei"],
        )

        pm_filter.select_option("Zoey")
        page.locator('[data-version-plan-row-id="sync-zoey"]').get_by_text("Zoey synced row").wait_for(timeout=5000)
        page.locator('[data-version-plan-row-id="pipe-zoey"]').get_by_text("Zoey pipeline row").wait_for(timeout=5000)
        self.assertEqual(page.locator('[data-version-plan-row-id="manual-rene"]').count(), 0)
        self.assertEqual(page.locator('[data-version-plan-row-id="pipe-empty"]').count(), 0)
        self.assertEqual(page.locator('[data-version-plan-row-id="arch-junwei"]').count(), 0)
        self.assertGreaterEqual(page.locator('[data-version-plan-row-action="add"]').count(), 2)

        pm_filter.select_option("-")
        page.locator('[data-version-plan-row-id="pipe-empty"]').get_by_text("No PM pipeline row").wait_for(timeout=5000)
        self.assertEqual(page.locator('[data-version-plan-row-id="sync-zoey"]').count(), 0)
        self.assertGreaterEqual(page.locator('[data-version-plan-row-action="add"]').count(), 2)

        pm_filter.select_option("All PMs")
        page.locator('[data-version-plan-row-id="manual-rene"]').get_by_text("Rene manual row").wait_for(timeout=5000)
        page.locator('[data-version-plan-row-id="arch-junwei"]').get_by_text("Jun Wei archived row").wait_for(timeout=5000)
        self.assertEqual(version_plan_request_count, 1)

    def test_team_dashboard_project_status_editing_smoke(self) -> None:
        page = self._new_admin_page()
        captured_status_updates: list[dict[str, object]] = []
        current_status = "Pending Review"
        task_request_count = 0

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def config(route):
            route.fulfill(**json_response({
                "status": "ok",
                "config": {
                    "teams": {"AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]}},
                    "task_cache": {},
                },
            }))

        def tasks(route):
            nonlocal task_request_count
            task_request_count += 1
            route.fulfill(**json_response({
                "status": "ok",
                "teams": [
                    {
                        "team_key": "AF",
                        "label": "Anti-fraud",
                        "member_emails": [ADMIN_EMAIL],
                        "loaded": True,
                        "elapsed_seconds": 1,
                        "fetch_stats": {"api_call_count": 1},
                        "under_prd": [
                            {
                                "bpmis_id": "BPMIS-STATUS-E2E",
                                "project_name": "Status Editable Project",
                                "status": current_status,
                                "release_date": "2026-05-29",
                                "release_date_sort": "2026-05-29",
                                "market": "Regional",
                                "priority": "P0",
                                "regional_pm_pic": "Xiaodong Zheng",
                                "actual_mandays": 3,
                                "is_key_project": True,
                                "key_project_source": "priority_default",
                                "matched_pm_emails": [ADMIN_EMAIL],
                                "jira_tickets": [],
                            }
                        ],
                        "pending_live": [],
                    }
                ],
            }))

        def save_status(route):
            nonlocal current_status
            payload = route.request.post_data_json
            captured_status_updates.append(payload)
            if payload.get("status") == "Testing":
                route.fulfill(**json_response({
                    "status": "error",
                    "message": "BPMIS rejected Testing",
                    "error_category": "bpmis_error",
                }, status=400))
                return
            current_status = str(payload.get("status") or current_status)
            route.fulfill(**json_response({
                "status": "ok",
                "bpmis_id": payload.get("bpmis_id"),
                "project_status": current_status,
                "cached_updates": 1,
            }))

        page.route("**/api/team-dashboard/config", config)
        page.route("**/api/team-dashboard/tasks**", tasks)
        page.route("**/api/team-dashboard/project-status", save_status)

        page.goto("/team-dashboard", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-track="AF"]').wait_for(timeout=5000)
        page.locator("[data-team-dashboard-load-team]").click()
        page.locator("[data-team-dashboard-task-status]").get_by_text("Reloaded Jira for Anti-fraud.").wait_for(timeout=5000)
        page.get_by_text("Status Editable Project").wait_for(timeout=5000)

        status_select = page.locator('[data-team-dashboard-project-status][data-bpmis-id="BPMIS-STATUS-E2E"]')
        self.assertEqual(status_select.input_value(timeout=5000), "Pending Review")
        self.assertEqual(task_request_count, 1)
        status_select.select_option("Developing")
        page.locator("[data-team-dashboard-task-status]").get_by_text(
            "Updated BPMIS status for BPMIS-STATUS-E2E to Developing."
        ).wait_for(timeout=5000)
        status_select = page.locator('[data-team-dashboard-project-status][data-bpmis-id="BPMIS-STATUS-E2E"]')
        self.assertEqual(status_select.input_value(timeout=5000), "Developing")
        self.assertEqual(captured_status_updates[-1], {"bpmis_id": "BPMIS-STATUS-E2E", "status": "Developing"})
        self.assertEqual(task_request_count, 1)

        status_select.select_option("Testing")
        page.locator("[data-team-dashboard-task-status]").get_by_text("BPMIS rejected Testing").wait_for(timeout=5000)
        status_select = page.locator('[data-team-dashboard-project-status][data-bpmis-id="BPMIS-STATUS-E2E"]')
        self.assertEqual(status_select.input_value(timeout=5000), "Developing")
        self.assertEqual(captured_status_updates[-1], {"bpmis_id": "BPMIS-STATUS-E2E", "status": "Testing"})

    def test_team_dashboard_prd_summary_and_review_async_smoke(self) -> None:
        page = self._new_admin_page()
        captured_summary_payloads: list[dict[str, object]] = []
        captured_review_payloads: list[dict[str, object]] = []
        requested_job_ids: list[str] = []
        job_results: dict[str, dict[str, object]] = {}
        prd_url = "https://confluence.example/display/AF/Risk+Launch+PRD"

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def config(route):
            route.fulfill(**json_response({
                "status": "ok",
                "config": {
                    "teams": {
                        "AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]},
                    },
                    "task_cache": {},
                },
            }))

        def tasks(route):
            route.fulfill(**json_response({
                "status": "ok",
                "teams": [
                    {
                        "team_key": "AF",
                        "label": "Anti-fraud",
                        "member_emails": [ADMIN_EMAIL],
                        "loaded": True,
                        "elapsed_seconds": 1,
                        "fetch_stats": {"api_call_count": 2},
                        "under_prd": [
                            {
                                "bpmis_id": "BPMIS-PRD-E2E",
                                "project_name": "Risk Launch Workflow",
                                "release_date": "2026-05-29",
                                "release_date_sort": "2026-05-29",
                                "market": "Regional",
                                "priority": "P0",
                                "regional_pm_pic": "Xiaodong Zheng",
                                "actual_mandays": 8,
                                "is_key_project": True,
                                "key_project_source": "priority_default",
                                "matched_pm_emails": [ADMIN_EMAIL],
                                "jira_tickets": [
                                    {
                                        "jira_id": "SPDBP-PRD-1",
                                        "jira_link": "https://jira.shopee.io/browse/SPDBP-PRD-1",
                                        "jira_title": "Risk launch PRD workflow",
                                        "pm_email": ADMIN_EMAIL,
                                        "jira_status": "Pending Review",
                                        "release_date": "2026-05-29",
                                        "version": "AF_1.0.88",
                                        "prd_links": [{"url": prd_url, "label": "Risk Launch PRD"}],
                                    }
                                ],
                            }
                        ],
                        "pending_live": [],
                    }
                ],
            }))

        def queue_prd_action(route, *, action: str):
            payload = route.request.post_data_json
            target = captured_summary_payloads if action == "summary" else captured_review_payloads
            target.append(payload)
            index = len(target)
            job_id = f"prd-{action}-job-{index}"
            result_key = "summary" if action == "summary" else "review"
            result_title = "PRD Summary" if action == "summary" else "PRD Review"
            result_text = (
                "### Executive Summary\n- Launch flow is ready for PM review."
                if action == "summary"
                else "### 执行逻辑体检结论\n- 主流程闭环通过。\n- 异常分支需要确认 rollback owner。"
            )
            job_results[job_id] = {
                "status": "ok",
                "cached": False,
                result_key: {
                    "result_markdown": result_text,
                    "updated_at": "2026-05-17T06:00:00Z",
                },
                "coverage": {
                    "mode": "async_e2e",
                    "sections_covered": 4,
                    "sections_total": 5,
                    "estimated_prompt_tokens": 12800,
                    "token_risk": "medium",
                    "report_templates_total": 1 if action == "review" else 0,
                    "report_templates_reviewed": 1 if action == "review" else 0,
                    "confluence_tables_total": 2 if action == "review" else 0,
                    "confluence_tables_reviewed": 2 if action == "review" else 0,
                    "google_sheet_screenshots_total": 1 if action == "review" else 0,
                    "google_sheet_screenshots_reviewed": 1 if action == "review" else 0,
                },
            }
            route.fulfill(**json_response({"status": "queued", "job_id": job_id}))

        def job_status(route):
            job_id = urlparse(route.request.url).path.rsplit("/", 1)[-1]
            requested_job_ids.append(job_id)
            route.fulfill(**json_response({
                "status": "ok",
                "state": "completed",
                "message": "PRD generation completed.",
                "results": [job_results[job_id]],
            }))

        page.route("**/api/team-dashboard/config", config)
        page.route("**/api/team-dashboard/tasks**", tasks)
        page.route("**/api/team-dashboard/prd-summary", lambda route: queue_prd_action(route, action="summary"))
        page.route("**/api/team-dashboard/prd-review", lambda route: queue_prd_action(route, action="review"))
        page.route("**/api/jobs/*", job_status)

        page.goto("/team-dashboard", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-track="AF"]').wait_for(timeout=5000)
        page.locator("[data-team-dashboard-load-team]").click()
        page.locator("[data-team-dashboard-task-status]").get_by_text("Reloaded Jira for Anti-fraud.").wait_for(timeout=5000)
        page.get_by_text("Risk Launch Workflow").wait_for(timeout=5000)
        page.locator("[data-team-dashboard-toggle]").click()
        row = page.locator("tr", has_text="SPDBP-PRD-1").first
        row.get_by_text("Summary").wait_for(timeout=5000)

        row.locator('[data-prd-action="summary"]').click()
        page.locator("[data-prd-review-panel]").get_by_text("PRD Summary").wait_for(timeout=5000)
        page.locator("[data-prd-review-panel]").get_by_text("Launch flow is ready for PM review.").wait_for(timeout=5000)
        self.assertEqual(captured_summary_payloads[-1]["jira_id"], "SPDBP-PRD-1")
        self.assertEqual(captured_summary_payloads[-1]["prd_url"], prd_url)
        self.assertTrue(captured_summary_payloads[-1]["async"])
        self.assertFalse(captured_summary_payloads[-1]["force_refresh"])

        page.locator("[data-prd-refresh]").click()
        page.locator("[data-prd-review-panel]").get_by_text("Launch flow is ready for PM review.").wait_for(timeout=5000)
        self.assertTrue(captured_summary_payloads[-1]["force_refresh"])

        row.locator('[data-prd-action="review"]').click()
        page.locator("[data-prd-review-panel]").get_by_text("PRD Review").wait_for(timeout=5000)
        page.locator("[data-prd-review-panel]").get_by_text("主流程闭环通过").wait_for(timeout=5000)
        page.locator("[data-prd-review-panel]").get_by_text("Report templates reviewed: 1/1").wait_for(timeout=5000)
        self.assertEqual(captured_review_payloads[-1]["jira_id"], "SPDBP-PRD-1")
        self.assertEqual(captured_review_payloads[-1]["prd_url"], prd_url)
        self.assertTrue(captured_review_payloads[-1]["async"])
        self.assertIn("prd-summary-job-1", requested_job_ids)
        self.assertIn("prd-summary-job-2", requested_job_ids)
        self.assertIn("prd-review-job-1", requested_job_ids)

    def test_team_dashboard_monthly_report_async_generation_smoke(self) -> None:
        page = self._new_admin_page()
        captured_draft_payloads: list[dict[str, object]] = []
        captured_send_payloads: list[dict[str, object]] = []
        requested_job_ids: list[str] = []
        job_polls: dict[str, int] = {}
        job_id = "monthly-report-e2e-job-1"
        draft_markdown = "## Monthly Report\n- Risk launch summary is ready.\n- Follow up on UAT owner."

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def config(route):
            route.fulfill(**json_response({
                "status": "ok",
                "config": {
                    "teams": {"AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]}},
                    "task_cache": {},
                },
            }))

        def template(route):
            route.fulfill(**json_response({
                "status": "ok",
                "subject": "Banking Product Update",
                "recipient": ADMIN_EMAIL,
                "template": "## Monthly Report\n- {{highlights}}",
                "highlight_topics": ["Risk launch"],
                "highlight_topic_sources": [{"topic": "Risk launch", "sources": ["seatalk", "team_dashboard"]}],
                "period_start": "2026-05-01",
                "period_end": "2026-05-17",
            }))

        def latest_draft(route):
            route.fulfill(**json_response({"status": "ok", "draft_markdown": ""}))

        def draft(route):
            payload = route.request.post_data_json
            captured_draft_payloads.append(payload)
            route.fulfill(**json_response({"status": "queued", "job_id": job_id}))

        def job_status(route):
            requested_job_ids.append(urlparse(route.request.url).path.rsplit("/", 1)[-1])
            count = job_polls.get(job_id, 0) + 1
            job_polls[job_id] = count
            if count == 1:
                route.fulfill(**json_response({
                    "status": "ok",
                    "state": "running",
                    "stage": "searching_topic_gmail",
                    "message": "Searching topic evidence.",
                    "current": 1,
                    "total": 3,
                    "estimated_prompt_tokens": 18500,
                    "token_risk": "warning",
                    "progress": {
                        "stage": "searching_topic_gmail",
                        "message": "Searching topic evidence.",
                        "current": 1,
                        "total": 3,
                        "estimated_prompt_tokens": 18500,
                        "token_risk": "warning",
                    },
                }))
                return
            route.fulfill(**json_response({
                "status": "ok",
                "state": "completed",
                "message": "Monthly Report draft generated.",
                "progress": {
                    "state": "completed",
                    "stage": "done",
                    "message": "Monthly Report draft generated.",
                    "current": 3,
                    "total": 3,
                    "estimated_prompt_tokens": 18500,
                    "token_risk": "warning",
                },
                "results": [
                    {
                        "status": "ok",
                        "draft_markdown": draft_markdown,
                        "subject": "Banking Product Update",
                        "highlight_topics": ["Risk launch"],
                        "highlight_topic_sources": [{"topic": "Risk launch", "sources": ["seatalk", "team_dashboard"]}],
                        "period_start": "2026-05-01",
                        "period_end": "2026-05-17",
                        "evidence_summary": {"key_project_count": 2, "jira_ticket_count": 5},
                        "generation_summary": {
                            "elapsed_seconds": 12,
                            "estimated_prompt_tokens": 18500,
                            "token_risk": "warning",
                            "period_start": "2026-05-01",
                            "period_end": "2026-05-17",
                            "highlight_topic_sources": [{"topic": "Risk launch", "sources": ["seatalk", "team_dashboard"]}],
                        },
                        "evidence_review": [
                            {
                                "topic": "Risk launch",
                                "status": "ready",
                                "confidence": "high",
                                "primary_topic": "Risk launch",
                                "intent": "Monthly update",
                                "source_counts": {"seatalk": 3, "gmail": 1, "google_sheet": 0, "project": 2, "prd": 1},
                                "seatalk_conversation_labels": ["AF PM Sync"],
                                "glossary_matches": [{"domain": "AF", "canonical": "Risk Launch"}],
                                "gaps": [],
                            }
                        ],
                        "evidence_debug": [
                            {
                                "topic": "Risk launch",
                                "confidence": "high",
                                "topic_intent": "Monthly update",
                                "source_counts": {"seatalk": 3, "gmail": 1, "project": 2, "prd": 1},
                                "seatalk_raw_match_count": 5,
                                "seatalk_filtered_match_count": 3,
                                "seatalk_compact_count": 2,
                                "qualifier_marker_groups": [["risk", "launch"]],
                                "seatalk_conversation_labels": ["AF PM Sync"],
                                "alias_sample": ["SPDBP-PRD-1"],
                                "gaps": [],
                                "glossary_matches": [{"domain": "AF", "canonical": "Risk Launch"}],
                            }
                        ],
                    }
                ],
            }))

        def send(route):
            payload = route.request.post_data_json
            captured_send_payloads.append(payload)
            route.fulfill(**json_response({
                "status": "error",
                "message": "Gmail send failed. Reconnect Google Mail and retry.",
                "debug_secret": "secret-token-should-not-render",
            }, status=400))

        page.route("**/api/team-dashboard/config", config)
        page.route("**/api/team-dashboard/monthly-report/template", template)
        page.route("**/api/team-dashboard/monthly-report/latest-draft", latest_draft)
        page.route("**/api/team-dashboard/monthly-report/draft", draft)
        page.route("**/api/team-dashboard/monthly-report/send", send)
        page.route("**/api/jobs/*", job_status)

        page.goto("/reports", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-tab="monthly-report"]').wait_for(timeout=5000)
        page.locator('[data-monthly-report-topic-input]').first.fill("Risk launch")
        page.locator("[data-monthly-report-period-start]").fill("2026-05-01")
        page.locator("[data-monthly-report-period-end]").fill("2026-05-17")
        page.locator("[data-monthly-report-generate]").click()

        page.locator("[data-monthly-report-progress-message]").get_by_text("Searching topic evidence.").wait_for(timeout=5000)
        page.locator("[data-monthly-report-status]").get_by_text("Draft generated in 12s").wait_for(timeout=5000)
        self.assertEqual(page.locator("[data-monthly-report-draft]").input_value(timeout=5000), draft_markdown)
        page.locator("[data-monthly-report-preview]").get_by_text("Risk launch summary is ready.").wait_for(timeout=5000)
        page.locator("[data-monthly-report-evidence-review-body] strong").get_by_text("Risk launch", exact=True).wait_for(timeout=5000)
        page.locator("[data-monthly-report-evidence-review-body]").get_by_text("SeaTalk 3").wait_for(timeout=5000)
        self.assertFalse(page.locator("[data-monthly-report-send]").is_disabled(timeout=5000))
        self.assertEqual(captured_draft_payloads[-1]["highlight_topics"], ["Risk launch"])
        self.assertEqual(captured_draft_payloads[-1]["period_start"], "2026-05-01")
        self.assertEqual(captured_draft_payloads[-1]["period_end"], "2026-05-17")
        self.assertEqual(captured_draft_payloads[-1]["highlight_topic_sources"][0]["topic"], "Risk launch")
        self.assertIn(job_id, requested_job_ids)
        self.assertGreaterEqual(job_polls[job_id], 2)

        page.locator("[data-monthly-report-send]").click()
        page.locator("[data-monthly-report-status]").get_by_text("Gmail send failed. Reconnect Google Mail and retry.").wait_for(timeout=5000)
        self.assertEqual(page.locator("[data-monthly-report-draft]").input_value(timeout=5000), draft_markdown)
        self.assertFalse(page.locator("[data-monthly-report-send]").is_disabled(timeout=5000))
        self.assertEqual(captured_send_payloads[-1]["draft_markdown"], draft_markdown)
        self.assertEqual(captured_send_payloads[-1]["subject"], "Banking Product Update")
        self.assertEqual(captured_send_payloads[-1]["recipient"], ADMIN_EMAIL)
        self.assertNotIn("secret-token-should-not-render", page.locator("body").inner_text(timeout=5000))

    def test_team_dashboard_monthly_report_empty_highlights_browser_smoke(self) -> None:
        page = self._new_admin_page()
        captured_draft_payloads: list[dict[str, object]] = []
        requested_job_ids: list[str] = []
        job_id = "monthly-report-empty-highlights-e2e-job-1"
        draft_markdown = "## Monthly Report\n- Project tables are generated from dashboard evidence."

        def json_response(payload: dict[str, object], *, status: int = 200):
            return {
                "status": status,
                "content_type": "application/json",
                "body": json.dumps(payload),
            }

        def config(route):
            route.fulfill(**json_response({
                "status": "ok",
                "config": {
                    "teams": {"AF": {"label": "Anti-fraud", "member_emails": [ADMIN_EMAIL]}},
                    "task_cache": {},
                },
            }))

        def template(route):
            route.fulfill(**json_response({
                "status": "ok",
                "subject": "Banking Product Update",
                "recipient": ADMIN_EMAIL,
                "template": "## Monthly Report\n- {{highlights}}",
                "highlight_topics": [],
                "highlight_topic_sources": [],
                "period_start": "2026-05-01",
                "period_end": "2026-05-17",
            }))

        def latest_draft(route):
            route.fulfill(**json_response({"status": "ok", "draft_markdown": ""}))

        def draft(route):
            captured_draft_payloads.append(route.request.post_data_json)
            route.fulfill(**json_response({"status": "queued", "job_id": job_id}))

        def job_status(route):
            requested_job_ids.append(urlparse(route.request.url).path.rsplit("/", 1)[-1])
            route.fulfill(**json_response({
                "status": "ok",
                "state": "completed",
                "message": "Monthly Report draft generated.",
                "progress": {
                    "state": "completed",
                    "stage": "done",
                    "message": "Monthly Report draft generated.",
                    "current": 1,
                    "total": 1,
                },
                "results": [
                    {
                        "status": "ok",
                        "draft_markdown": draft_markdown,
                        "subject": "Banking Product Update",
                        "highlight_topics": [],
                        "highlight_topic_sources": [],
                        "period_start": "2026-05-01",
                        "period_end": "2026-05-17",
                        "evidence_summary": {"key_project_count": 1, "jira_ticket_count": 2},
                        "generation_summary": {"elapsed_seconds": 4, "highlight_topic_sources": []},
                        "evidence_review": [],
                        "evidence_debug": [],
                    }
                ],
            }))

        page.route("**/api/team-dashboard/config", config)
        page.route("**/api/team-dashboard/monthly-report/template", template)
        page.route("**/api/team-dashboard/monthly-report/latest-draft", latest_draft)
        page.route("**/api/team-dashboard/monthly-report/draft", draft)
        page.route("**/api/jobs/*", job_status)

        page.goto("/reports", wait_until="domcontentloaded")
        page.locator('[data-team-dashboard-tab="monthly-report"]').wait_for(timeout=5000)
        self.assertEqual(page.locator("[data-monthly-report-topic-input]").first.input_value(timeout=5000), "")
        page.locator("[data-monthly-report-period-start]").fill("2026-05-01")
        page.locator("[data-monthly-report-period-end]").fill("2026-05-17")
        page.locator("[data-monthly-report-generate]").click()

        page.locator("[data-monthly-report-status]").get_by_text("Draft generated in 4s").wait_for(timeout=5000)
        self.assertEqual(page.locator("[data-monthly-report-draft]").input_value(timeout=5000), draft_markdown)
        page.locator("[data-monthly-report-preview]").get_by_text("Project tables are generated").wait_for(timeout=5000)
        self.assertFalse(page.locator("[data-monthly-report-send]").is_disabled(timeout=5000))
        self.assertEqual(captured_draft_payloads[-1]["highlight_topics"], [])
        self.assertEqual(captured_draft_payloads[-1]["highlight_topic_sources"], [])
        self.assertEqual(captured_draft_payloads[-1]["period_start"], "2026-05-01")
        self.assertEqual(captured_draft_payloads[-1]["period_end"], "2026-05-17")
        self.assertIn(job_id, requested_job_ids)

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

if __name__ == "__main__":
    unittest.main()
