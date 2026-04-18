# Team Deployment Guide

Current status:

- The default rollout is still local-first.
- Each teammate runs the portal on their own Mac.
- The shared internal Portal deployment in this document is a future mode for after the team gets a valid domain/hostname for Google OAuth.

This guide describes the future shared deployment shape for a team of around 10-15 users.

## Deployment Model

- One shared team portal runs on an existing internal Mac.
- Each teammate has their own BPMIS API token.
- The portal calls BPMIS APIs directly with that configured token.

This keeps infrastructure simple and avoids buying a new machine.

## What the Portal Host Needs

Use one Mac that is:

- reachable on the same internal network as the team
- usually online during working hours
- configured not to sleep during the day
- able to run Python and a local Flask process

Recommended host assumptions:

- fixed or stable internal IP
- project checked out locally
- `.venv` already created
- `.env` already configured

## Required Environment Variables

At minimum, configure these in `.env` on the portal host:

```bash
FLASK_SECRET_KEY=change-me
GOOGLE_OAUTH_CLIENT_SECRET_FILE=/absolute/path/to/google-client-secret.json
TEAM_PORTAL_HOST=0.0.0.0
TEAM_PORTAL_PORT=5000
TEAM_PORTAL_BASE_URL=http://<internal-ip>:5000
TEAM_ALLOWED_EMAILS=user1@npt.sg,user2@npt.sg
TEAM_PORTAL_DATA_DIR=/absolute/path/to/team-portal-data
```

Notes:

- `TEAM_PORTAL_HOST=0.0.0.0` allows teammates on the same network to access the portal.
- `TEAM_PORTAL_BASE_URL` should match the real shared URL used by teammates, because Google OAuth callback generation uses it when no explicit callback override is set.
- `TEAM_ALLOWED_EMAILS` is a comma-separated Google email whitelist for basic internal access control.
- `TEAM_PORTAL_DATA_DIR` stores SQLite config, PID files, and logs.

## Start the Shared Portal

Manual production-style startup:

```bash
./scripts/run_team_portal_prod.sh start
./scripts/run_team_portal_prod.sh status
./scripts/run_team_portal_prod.sh logs
```

Useful operations:

```bash
./scripts/run_team_portal_prod.sh stop
./scripts/run_team_portal_prod.sh restart
```

After start, teammates should be able to access:

```bash
http://<internal-ip>:5000
```

## Install launchd Auto-Start on the Portal Host

To make the portal auto-start for the host user on login:

```bash
./scripts/install_team_portal_launchd.sh
```

Then start it:

```bash
launchctl start io.npt.jira-creation-portal
```

The generated plist is installed under:

```bash
~/Library/LaunchAgents/io.npt.jira-creation-portal.plist
```

Portal logs will be written under `TEAM_PORTAL_DATA_DIR/logs`.

## What Teammates Need To Run Locally

Each teammate still needs their own BPMIS API token because BPMIS access now depends on that token.

They should follow the quickstart in:

- [docs/team-member-quickstart.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-quickstart.md)

## Health Checks

Portal host checks:

- open `http://<internal-ip>:5000`
- run `./scripts/run_team_portal_prod.sh status`
- check `./scripts/run_team_portal_prod.sh logs`

Teammate BPMIS checks:

- run `./scripts/run_team_stack.sh status`
- confirm Self-Check shows `BPMIS API`

## Common Failures

### Portal cannot be opened

Check:

- the host Mac is awake
- the host Mac is on the same network
- `./scripts/run_team_portal_prod.sh status`
- macOS firewall is not blocking port `5000`

### Google login works but the user is blocked

Check:

- the teammate's Google email is present in `TEAM_ALLOWED_EMAILS`

### BPMIS API check fails

Check:

- teammate has started `./scripts/run_team_stack.sh start`
- teammate has configured `BPMIS_API_ACCESS_TOKEN`
- the token is still valid

### Preview or Run returns Spreadsheet errors

Check:

- Spreadsheet link or ID
- Input tab name
- Issue ID header
- Jira Ticket Link header

## v1 Tradeoff

This deployment shape is intentionally simple:

- no extra infrastructure
- no new hardware
- no company-wide SSO
- no 24x7 uptime guarantee

For a fixed team during working hours, this is usually enough to validate adoption before moving to a more formal server or VM.
