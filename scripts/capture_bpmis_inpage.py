from __future__ import annotations

import json
import signal
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bpmis_jira_tool.config import Settings

load_dotenv(PROJECT_ROOT / ".env")

OUTPUT_PATH = PROJECT_ROOT / "bpmis_inpage_capture.jsonl"


INJECT_SCRIPT = r"""
(() => {
  if (window.__codexCaptureInstalled) return;
  window.__codexCaptureInstalled = true;
  window.__codexCapturedRequests = [];

  const pushRecord = (record) => {
    try {
      window.__codexCapturedRequests.push({
        timestamp: new Date().toISOString(),
        ...record,
      });
    } catch (error) {}
  };

  const originalFetch = window.fetch.bind(window);
  window.fetch = async (...args) => {
    const [input, init] = args;
    const url = typeof input === 'string' ? input : input?.url;
    const method = init?.method || input?.method || 'GET';
    const body = init?.body || null;
    try {
      const response = await originalFetch(...args);
      let text = null;
      try {
        text = await response.clone().text();
      } catch (error) {}
      pushRecord({
        kind: 'fetch',
        url,
        method,
        requestBody: body,
        status: response.status,
        responseText: text,
      });
      return response;
    } catch (error) {
      pushRecord({
        kind: 'fetch-error',
        url,
        method,
        requestBody: body,
        error: String(error),
      });
      throw error;
    }
  };

  const OriginalXHR = window.XMLHttpRequest;
  function WrappedXHR() {
    const xhr = new OriginalXHR();
    let requestUrl = '';
    let requestMethod = 'GET';
    let requestBody = null;

    const originalOpen = xhr.open;
    xhr.open = function(method, url, ...rest) {
      requestMethod = method;
      requestUrl = url;
      return originalOpen.call(this, method, url, ...rest);
    };

    const originalSend = xhr.send;
    xhr.send = function(body) {
      requestBody = body ?? null;
      xhr.addEventListener('loadend', function() {
        pushRecord({
          kind: 'xhr',
          url: requestUrl,
          method: requestMethod,
          requestBody,
          status: xhr.status,
          responseText: xhr.responseText,
        });
      });
      return originalSend.call(this, body);
    };

    return xhr;
  }

  window.XMLHttpRequest = WrappedXHR;
})();
"""


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
        bpmis_page = None
        for context in browser.contexts:
            for page in context.pages:
                try:
                    if "bpmis-uat1.uat.npt.seabank.io" in page.url:
                        bpmis_page = page
                        break
                except Exception:
                    continue
            if bpmis_page:
                break

        if bpmis_page is None:
            print("Could not find an open BPMIS page.", file=sys.stderr)
            return 1

        bpmis_page.evaluate(INJECT_SCRIPT)
        print(f"Injected capture hooks into: {bpmis_page.url}")
        print(f"Output file: {OUTPUT_PATH}")
        print("Now manually create one Jira ticket in BPMIS, then press Ctrl+C here.")

        already_written = 0
        while running:
            records = bpmis_page.evaluate(
                """
                () => {
                  const items = window.__codexCapturedRequests || [];
                  return items.splice(0, items.length);
                }
                """
            )
            if records:
                with OUTPUT_PATH.open("a", encoding="utf-8") as handle:
                    for record in records:
                        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                already_written += len(records)
                print(f"[captured] +{len(records)} events (total {already_written})")
            time.sleep(0.25)

    print("In-page capture stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
