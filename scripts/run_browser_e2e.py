#!/usr/bin/env python3
"""Run browser-level Playwright E2E smoke tests for the team portal."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


ROOT_DIR = Path(__file__).resolve().parents[1]


def _playwright_available() -> tuple[bool, str]:
    try:
        import playwright  # noqa: F401
    except ImportError as error:
        return False, str(error)
    return True, ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="", help="Use an existing portal instead of starting a temporary local server.")
    parser.add_argument("--browser", default="chromium", choices=["chromium", "firefox", "webkit"], help="Playwright browser.")
    parser.add_argument("--headed", action="store_true", help="Run with a visible browser window.")
    parser.add_argument(
        "--skip-if-unavailable",
        action="store_true",
        help="Return success when Playwright is not installed. Intended only for optional local workflows.",
    )
    args = parser.parse_args(argv)

    available, reason = _playwright_available()
    if not available:
        message = (
            "Playwright is not installed. Run:\n"
            "  ./.venv/bin/python -m pip install -r requirements-e2e.txt\n"
            "  ./.venv/bin/python -m playwright install chromium"
        )
        if reason:
            message = f"{message}\n\nImport error: {reason}"
        print(message, file=sys.stderr)
        return 0 if args.skip_if_unavailable else 1

    env = dict(os.environ)
    env.setdefault("ENV_FILE", os.devnull)
    env["PYTHONPATH"] = str(ROOT_DIR) if not env.get("PYTHONPATH") else f"{ROOT_DIR}{os.pathsep}{env['PYTHONPATH']}"
    env["BROWSER_E2E_BROWSER"] = args.browser
    env["BROWSER_E2E_HEADLESS"] = "0" if args.headed else "1"
    if args.base_url:
        env["BROWSER_E2E_BASE_URL"] = args.base_url.rstrip("/")

    completed = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests/e2e", "-p", "*_e2e.py"],
        cwd=ROOT_DIR,
        env=env,
        check=False,
    )
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
