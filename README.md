# BPMIS Automation Tool

This repository now supports both:

- a local-first edition for individual use
- a shared portal edition hosted on one Mac and exposed through Cloudflare Tunnel

The current local-first edition is a Flask web app that:

- lets a user sync BPMIS projects into portal-owned storage
- manages project-level Jira creation from the My Projects view
- creates Jira tickets through BPMIS
- stores project rows and created Jira links in the portal data store

Current local assumptions:

- the app runs on one machine
- Google auth is tied to the current browser session
- BPMIS access uses a configured Bearer token via `BPMIS_API_ACCESS_TOKEN`
- web config is stored locally per user in the team portal config store

Current shared-team assumptions:

- one host Mac runs the portal
- teammates open the Cloudflare Tunnel URL as the primary shared portal and sign in with `@npt.sg` Google accounts
- each teammate stores their own BPMIS token and team routing config in the portal
- BPMIS tokens saved through the shared portal are encrypted at rest with `TEAM_PORTAL_CONFIG_ENCRYPTION_KEY`
- Default release requests deploy UAT only. If a request says to publish Live without mentioning Cloud Run, publish only the Mac-hosted Cloudflare Tunnel portal. Deploy Cloud Run live traffic only when the request explicitly says "live Cloud Run" or equivalent.
- New Cloud Run services default to Mac local-agent-backed cache, DB, and durable state. Do not use Cloud Run's container-local team portal data directory as the system of record unless explicitly requested.

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
- [docs/release-checklist.md](/Users/NPTSG0388/Documents/New%20project/docs/release-checklist.md)
- [docs/team-member-quickstart.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-quickstart.md)
- [docs/team-member-local-setup.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-local-setup.md)

## Tests

```bash
./.venv/bin/python -m unittest discover -s tests
```
