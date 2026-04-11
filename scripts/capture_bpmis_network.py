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

OUTPUT_PATH = PROJECT_ROOT / "bpmis_network_capture.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _should_capture(url: str, resource_type: str) -> bool:
    lowered = url.lower()
    if "bpmis-uat1" not in lowered and "seabank.io" not in lowered:
        return False
    if resource_type not in {"xhr", "fetch"}:
        return False
    return True


def _write_record(record: dict) -> None:
    with OUTPUT_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _attach_context(context) -> None:
    def on_request(request):
        if not _should_capture(request.url, request.resource_type):
            return
        record = {
            "timestamp": _now_iso(),
            "kind": "request",
            "resource_type": request.resource_type,
            "method": request.method,
            "url": request.url,
            "headers": request.headers,
            "post_data": request.post_data,
        }
        _write_record(record)
        print(f"[request] {request.method} {request.url}")

    def on_response(response):
        request = response.request
        if not _should_capture(request.url, request.resource_type):
            return
        try:
            body = response.text()
        except Exception:
            body = None
        record = {
            "timestamp": _now_iso(),
            "kind": "response",
            "resource_type": request.resource_type,
            "method": request.method,
            "url": request.url,
            "status": response.status,
            "request_headers": request.headers,
            "request_post_data": request.post_data,
            "response_headers": response.headers,
            "response_text": body,
        }
        _write_record(record)
        print(f"[response] {response.status} {request.method} {request.url}")

    context.on("request", on_request)
    context.on("response", on_response)


def main() -> int:
    settings = Settings.from_env()
    cdp_url = settings.bpmis_browser_cdp_url
    if not cdp_url:
        print("BPMIS_BROWSER_CDP_URL is not configured in .env", file=sys.stderr)
        return 1

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text("", encoding="utf-8")
    running = True

    def stop_handler(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(cdp_url)
        contexts = browser.contexts or [browser.new_context()]
        for context in contexts:
            _attach_context(context)

        print("Capturing BPMIS XHR/fetch traffic from the attached Chrome session.")
        print(f"Output file: {OUTPUT_PATH}")
        print("Now manually create one Jira ticket in BPMIS, then press Ctrl+C here.")

        while running:
            time.sleep(0.25)

    print("Capture stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
