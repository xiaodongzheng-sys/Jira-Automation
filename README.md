# Jira Creation Automation Tool

This repository currently tracks a local-first edition of the tool.

The current recommended usage is still local per user. Each person runs the portal on their own Mac.

The current local-first edition is a Flask web app that:

- connects to Google Sheets with Google OAuth
- reads one configurable Spreadsheet and Input tab
- previews eligible rows whose `Jira Ticket Link` is blank
- creates Jira tickets through BPMIS
- writes the full Jira URL back to the configured Jira link column

Current local assumptions:

- the app runs on one machine
- Google auth is tied to the current browser session
- BPMIS access uses a configured Bearer token via `BPMIS_API_ACCESS_TOKEN`
- web config is stored locally in `jira_web_config.json`

## Upload-Style Python Script

For single-ticket creation in a tool-upload style flow, use the standalone single-file script
[`scripts/create_bpmis_jira_ticket.py`](/Users/NPTSG0388/Documents/New%20project/scripts/create_bpmis_jira_ticket.py).

It expects one JSON object on stdin and prints one JSON result to stdout:

When uploaded to the tool platform, the entrypoint should be `main(input)`, where `input` is the parameter dictionary from Step 2.

```bash
echo '{
  "access_token": "your-bpmis-token",
  "issue_id": "12345",
  "market": "SG",
  "summary": "Investigate login failure",
  "task_type": "Feature",
  "bpmis_base_url": "https://bpmis-uat1.uat.npt.seabank.io"
}' | ./.venv/bin/python scripts/create_bpmis_jira_ticket.py
```

Required input fields:

- `access_token`
- `issue_id`
- `market`
- `summary`

Optional input fields:

- `task_type` (defaults to `Feature`)
- `description`
- `prd_links`
- `td_links`
- `fix_version`
- `component`
- `priority`
- `assignee`
- `reporter`
- `product_manager`
- `dev_pic`
- `qa_pic`
- `biz_pic`
- `need_uat`
- `involved_tracks`

Detailed local usage lives in [docs/local-edition.md](/Users/NPTSG0388/Documents/New%20project/docs/local-edition.md).

The team edition prototype and future deployment guidance live in:

- [docs/team-edition.md](/Users/NPTSG0388/Documents/New%20project/docs/team-edition.md)
- [docs/team-deployment.md](/Users/NPTSG0388/Documents/New%20project/docs/team-deployment.md)
- [docs/team-member-quickstart.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-quickstart.md)
- [docs/team-member-local-setup.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-local-setup.md)

## Tests

```bash
./.venv/bin/python -m unittest discover -s tests
```
