# Local Edition Baseline

This document describes the current single-user local edition that should remain usable even while the team edition is being explored.

## What It Does

- Runs a local Flask web portal on `127.0.0.1:5000`
- Uses Google OAuth to access Google Sheets
- Reads one configurable spreadsheet and one configurable input tab
- Treats rows as eligible when:
  - the configured Issue ID header has a value
  - the configured Jira Ticket Link header is blank
- Uses the saved web config as the only field-mapping source
- Creates Jira tickets through BPMIS
- Writes the full Jira browse URL back to the Jira Ticket Link column

## Current Runtime Model

- Single user
- Single machine
- Local config file: `jira_web_config.json`
- Google session stored in Flask session cookies
- BPMIS access depends on the logged-in Chrome session on the same machine

## Local Setup

1. Create `.env` from `.env.example`
2. Install dependencies
3. Start the server

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./scripts/run_server.sh start
```

If the launch script is unstable on the machine, the fallback is:

```bash
./.venv/bin/python -m flask --app app run --host 127.0.0.1 --port 5000
```

## Local Constraints

- Not intended for multiple users
- Config is not user-isolated
- BPMIS integration assumes the local user has an already logged-in Chrome session
- The app is appropriate as a personal automation tool, not yet as a shared internal service
