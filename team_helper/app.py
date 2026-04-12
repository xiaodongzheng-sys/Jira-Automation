from __future__ import annotations

import json
from pathlib import Path

from flask import Flask, jsonify, request
from playwright.sync_api import sync_playwright

from bpmis_jira_tool.bpmis import BPMISPageApiClient
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.models import ProjectMatch


def create_app() -> Flask:
    app = Flask(__name__)
    capture_template_path = Path(__file__).resolve().parent.parent / "tmp" / "last_sdlc_api_template.json"

    def _capture_script() -> str:
        return """
(() => {
  if (window.__codexSdlcCaptureInstalled) {
    return "already-installed";
  }
  window.__codexSdlcCaptureInstalled = true;
  window.__codexSdlcCapturedRequests = window.__codexSdlcCapturedRequests || [];

  const pushRecord = (record) => {
    try {
      window.__codexSdlcCapturedRequests.push({
        timestamp: Date.now(),
        ...record
      });
    } catch (error) {
      console.warn("Failed to capture SDLC request", error);
    }
  };

  const originalFetch = window.fetch.bind(window);
  window.fetch = async (...args) => {
    const [input, init = {}] = args;
    const url = typeof input === "string" ? input : (input && input.url) || "";
    const method = (init.method || "GET").toUpperCase();
    const body = init.body || null;
    const headers = init.headers || null;
    if (url.includes("/api/")) {
      pushRecord({ transport: "fetch", url, method, headers, body });
    }
    return originalFetch(...args);
  };

  const originalOpen = XMLHttpRequest.prototype.open;
  const originalSend = XMLHttpRequest.prototype.send;
  const originalSetHeader = XMLHttpRequest.prototype.setRequestHeader;
  XMLHttpRequest.prototype.open = function(method, url) {
    this.__codexMethod = method;
    this.__codexUrl = url;
    this.__codexHeaders = {};
    return originalOpen.apply(this, arguments);
  };
  XMLHttpRequest.prototype.setRequestHeader = function(key, value) {
    this.__codexHeaders = this.__codexHeaders || {};
    this.__codexHeaders[key] = value;
    return originalSetHeader.apply(this, arguments);
  };
  XMLHttpRequest.prototype.send = function(body) {
    const url = this.__codexUrl || "";
    if (url.includes("/api/")) {
      pushRecord({
        transport: "xhr",
        url,
        method: String(this.__codexMethod || "GET").toUpperCase(),
        headers: this.__codexHeaders || {},
        body: body || null
      });
    }
    return originalSend.apply(this, arguments);
  };
  return "installed";
})();
"""

    def _normalize_captured_body(body):
        if body is None:
            return None
        if isinstance(body, (dict, list)):
            return body
        if isinstance(body, str):
            stripped = body.strip()
            if not stripped:
                return ""
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return stripped
        return body

    def _templateify_captured_sdlc_body(body):
        if not isinstance(body, dict):
            return body

        template = json.loads(json.dumps(body))
        jira_array = template.get("jiraArray")
        if isinstance(jira_array, list) and jira_array:
            jira_array[0] = "__JIRA_TICKET_KEY__"
        if "title" in template:
            template["title"] = "__TITLE__"
        if "content" in template:
            template["content"] = "<p>__CONTENT__</p>"
        node_array = template.get("nodeArray")
        if isinstance(node_array, dict):
            for key, value in node_array.items():
                if isinstance(value, list) and value:
                    value[0] = "__BUSINESS_LEAD__"
                    break
        return template

    def _pick_capture_candidate(records):
        api_records = [record for record in records if "/api/" in str(record.get("url") or "")]
        post_records = [record for record in api_records if str(record.get("method") or "").upper() == "POST"]
        candidates = post_records or api_records
        if not candidates:
            return None
        return candidates[-1]

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    @app.route("/health", methods=["GET", "OPTIONS"])
    def health():
        if request.method == "OPTIONS":
            return ("", 204)
        return jsonify(
            {
                "status": "ok",
                "service": "team-helper",
                "mode": "prototype",
            }
        )

    @app.route("/diagnostics", methods=["GET", "OPTIONS"])
    def diagnostics():
        if request.method == "OPTIONS":
            return ("", 204)

        settings = Settings.from_env()
        client = BPMISPageApiClient(settings)
        checks = {
            "cdp": {"ok": False, "detail": ""},
            "bpmis_tab": {"ok": False, "detail": ""},
        }
        try:
            with sync_playwright() as playwright:
                browser = client._connect_browser(playwright)
                checks["cdp"] = {"ok": True, "detail": "Chrome remote session is reachable."}

                context = browser.contexts[0] if browser.contexts else browser.new_context()
                bpmis_page = None
                for existing in context.pages:
                    if "bpmis-uat1.uat.npt.seabank.io" in (existing.url or ""):
                        bpmis_page = existing
                        break

                if bpmis_page is None:
                    checks["bpmis_tab"] = {
                        "ok": False,
                        "detail": "No open BPMIS tab was found in Chrome. Open BPMIS and log in first.",
                    }
                else:
                    try:
                        client._api_request(bpmis_page, "/api/v1/issueField/list")
                        checks["bpmis_tab"] = {
                            "ok": True,
                            "detail": "BPMIS tab is open and the session looks usable.",
                        }
                    except BPMISError as error:
                        checks["bpmis_tab"] = {
                            "ok": False,
                            "detail": f"BPMIS tab was found, but the session is not ready: {error}",
                        }
        except BPMISError as error:
            checks["cdp"] = {"ok": False, "detail": str(error)}

        overall_ok = all(check["ok"] for check in checks.values())
        return jsonify(
            {
                "status": "ok" if overall_ok else "warn",
                "service": "team-helper",
                "mode": "prototype",
                "message": "All local checks passed." if overall_ok else "Some local checks still need attention.",
                "checks": checks,
            }
        )

    @app.route("/bpmis/create-jira", methods=["POST", "OPTIONS"])
    def create_jira():
        if request.method == "OPTIONS":
            return ("", 204)

        payload = request.get_json(silent=True) or {}
        issue_id = str(payload.get("issue_id") or "").strip()
        fields = payload.get("fields") or {}
        if not issue_id:
            return jsonify({"status": "error", "message": "Missing issue_id"}), 400
        if not isinstance(fields, dict):
            return jsonify({"status": "error", "message": "fields must be an object"}), 400

        settings = Settings.from_env()
        client = BPMISPageApiClient(settings)
        project = ProjectMatch(project_id=issue_id, raw={"issueId": issue_id})

        try:
            ticket = client.create_jira_ticket(project, {str(key): str(value) for key, value in fields.items()})
        except BPMISError as error:
            return jsonify({"status": "error", "message": str(error)}), 400

        return jsonify(
            {
                "status": "created",
                "ticket_key": ticket.ticket_key,
                "ticket_link": ticket.ticket_link,
            }
        )

    @app.route("/sdlc/submit-approval", methods=["POST", "OPTIONS"])
    def submit_sdlc_approval():
        if request.method == "OPTIONS":
            return ("", 204)

        payload = request.get_json(silent=True) or {}
        issue_id = str(payload.get("issue_id") or "").strip()
        jira_ticket_link = str(payload.get("jira_ticket_link") or "").strip()
        market = str(payload.get("market") or "").strip()
        title = str(payload.get("title") or "").strip()
        content = str(payload.get("content") or "").strip()
        business_lead = str(payload.get("business_lead") or "").strip()

        if not issue_id:
            return jsonify({"status": "error", "message": "Missing issue_id"}), 400
        if not jira_ticket_link:
            return jsonify({"status": "error", "message": "Missing jira_ticket_link"}), 400
        if not market:
            return jsonify({"status": "error", "message": "Missing market"}), 400
        if not title:
            return jsonify({"status": "error", "message": "Missing title"}), 400
        if not content:
            return jsonify({"status": "error", "message": "Missing content"}), 400
        if not business_lead:
            return jsonify({"status": "error", "message": "Missing business_lead"}), 400

        settings = Settings.from_env()
        client = BPMISPageApiClient(settings)
        try:
            result = client.submit_sdlc_approval({str(key): str(value) for key, value in payload.items()})
        except BPMISError as error:
            return jsonify({"status": "error", "message": str(error)}), 400

        return jsonify({"status": "submitted", "result": result})

    @app.route("/sdlc/capture/start", methods=["POST", "OPTIONS"])
    def start_sdlc_capture():
        if request.method == "OPTIONS":
            return ("", 204)

        payload = request.get_json(silent=True) or {}
        market = str(payload.get("market") or "").strip().upper()
        if not market:
            return jsonify({"status": "error", "message": "Missing market"}), 400

        settings = Settings.from_env()
        client = BPMISPageApiClient(settings)
        try:
            with sync_playwright() as playwright:
                browser = client._connect_browser(playwright)
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                page = client._pick_existing_sdlc_page(context, market)
                page.evaluate(_capture_script())
        except BPMISError as error:
            return jsonify({"status": "error", "message": str(error)}), 400

        return jsonify(
            {
                "status": "ready",
                "message": "SDLC capture is armed. Complete one manual submission in Chrome, then call finish capture.",
                "market": market,
            }
        )

    @app.route("/sdlc/capture/finish", methods=["POST", "OPTIONS"])
    def finish_sdlc_capture():
        if request.method == "OPTIONS":
            return ("", 204)

        payload = request.get_json(silent=True) or {}
        market = str(payload.get("market") or "").strip().upper()
        if not market:
            return jsonify({"status": "error", "message": "Missing market"}), 400

        settings = Settings.from_env()
        client = BPMISPageApiClient(settings)
        try:
            with sync_playwright() as playwright:
                browser = client._connect_browser(playwright)
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                page = client._pick_existing_sdlc_page(context, market)
                records = page.evaluate("window.__codexSdlcCapturedRequests || []")
        except BPMISError as error:
            return jsonify({"status": "error", "message": str(error)}), 400

        candidate = _pick_capture_candidate(records or [])
        if not candidate:
            return jsonify(
                {
                    "status": "error",
                    "message": "No captured SDLC API request was found. Start capture, submit once manually, then finish capture.",
                }
            ), 400

        template = {
            "market": market,
            "url": candidate.get("url"),
            "method": candidate.get("method") or "POST",
            "headers": candidate.get("headers") or {},
            "body": _templateify_captured_sdlc_body(_normalize_captured_body(candidate.get("body"))),
        }
        capture_template_path.parent.mkdir(parents=True, exist_ok=True)
        capture_template_path.write_text(json.dumps(template, indent=2, ensure_ascii=False), encoding="utf-8")

        return jsonify(
            {
                "status": "captured",
                "message": "Captured the latest SDLC API request template.",
                "template_path": str(capture_template_path),
                "request_url": template["url"],
            }
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8787)
