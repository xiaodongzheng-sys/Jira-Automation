# Jira Creation Automation Tool

This repository currently tracks the local single-user edition of the tool.

The local edition is a Flask web app that:

- connects to Google Sheets with Google OAuth
- reads one configurable Spreadsheet and Input tab
- previews eligible rows whose `Jira Ticket Link` is blank
- creates Jira tickets through BPMIS
- writes the full Jira URL back to the configured Jira link column

Current local assumptions:

- the app runs on one machine
- Google auth is tied to the current browser session
- BPMIS access uses the current logged-in Chrome session on that machine
- web config is stored locally in `jira_web_config.json`

Detailed local usage lives in [docs/local-edition.md](/Users/NPTSG0388/Documents/New%20project/docs/local-edition.md).

## Tests

```bash
./.venv/bin/python -m unittest discover -s tests
```
