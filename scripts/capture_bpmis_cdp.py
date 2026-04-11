from __future__ import annotations

import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bpmis_jira_tool.config import Settings

load_dotenv(PROJECT_ROOT / ".env")

OUTPUT_PATH = PROJECT_ROOT / "bpmis_cdp_capture.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write(record: dict) -> None:
    with OUTPUT_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _pick_bpmis_page(browser):
    for context in browser.contexts:
        for page in context.pages:
            try:
                if "bpmis-uat1.uat.npt.seabank.io" in page.url:
                    return context, page
            except Exception:
                continue
    raise RuntimeError("Could not find an open BPMIS page in the attached Chrome session.")


def main() -> int:
    settings = Settings.from_env()
    if not settings.bpmis_browser_cdp_url:
        print("BPMIS_BROWSER_CDP_URL is missing.", file=sys.stderr)
        return 1

    OUTPUT_PATH.write_text("", encoding="utf-8")
    running = True

    def stop_handler(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(settings.bpmis_browser_cdp_url)
        context, page = _pick_bpmis_page(browser)
        cdp = context.new_cdp_session(page)
        cdp.send("Network.enable")

        request_map: dict[str, dict] = {}

        def on_request_sent(params):
            request = params.get("request", {})
            url = request.get("url", "")
            if "bpmis-uat1.uat.npt.seabank.io" not in url:
                return
            record = {
                "timestamp": _now_iso(),
                "kind": "request",
                "request_id": params.get("requestId"),
                "url": url,
                "method": request.get("method"),
                "headers": request.get("headers"),
                "post_data": request.get("postData"),
                "resource_type": params.get("type"),
            }
            request_map[params.get("requestId")] = record
            _write(record)
            print(f"[request] {record['method']} {url}")

        def on_response_received(params):
            response = params.get("response", {})
            url = response.get("url", "")
            if "bpmis-uat1.uat.npt.seabank.io" not in url:
                return
            record = {
                "timestamp": _now_iso(),
                "kind": "response",
                "request_id": params.get("requestId"),
                "url": url,
                "status": response.get("status"),
                "headers": response.get("headers"),
                "mime_type": response.get("mimeType"),
                "resource_type": params.get("type"),
            }
            req = request_map.get(params.get("requestId"))
            if req:
                record["request_method"] = req.get("method")
                record["request_post_data"] = req.get("post_data")
            try:
                body = cdp.send("Network.getResponseBody", {"requestId": params.get("requestId")})
                record["response_body"] = body.get("body")
                record["response_body_base64"] = body.get("base64Encoded")
            except Exception:
                record["response_body"] = None
            _write(record)
            print(f"[response] {record['status']} {url}")

        cdp.on("Network.requestWillBeSent", on_request_sent)
        cdp.on("Network.responseReceived", on_response_received)

        print(f"Capturing CDP network events for BPMIS page: {page.url}")
        print(f"Output file: {OUTPUT_PATH}")
        print("Now manually create one Jira ticket in BPMIS, then press Ctrl+C here.")

        while running:
            time.sleep(0.25)

    print("CDP capture stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
