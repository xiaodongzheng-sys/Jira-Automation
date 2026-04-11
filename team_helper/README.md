# Team Helper Prototype

This folder contains the first local-helper prototype for the team edition.

The helper is intended to run on each user's machine and bridge that user's own BPMIS browser session back to the shared internal portal.

## Current Prototype Contract

- `GET /health`
- `POST /bpmis/create-jira`

The current prototype now:

- exposes CORS headers for browser-based health checks
- returns helper status from `/health`
- accepts a normalized Jira create payload on `/bpmis/create-jira`
- reuses the local BPMIS API client on the same machine to create Jira

## Run

```bash
./scripts/run_team_helper.sh
```

Useful commands:

```bash
./scripts/run_team_helper.sh start
./scripts/run_team_helper.sh status
./scripts/run_team_helper.sh logs
./scripts/run_team_helper.sh stop
```

First-time teammate setup:

```bash
./scripts/install_team_helper_local.sh
```
