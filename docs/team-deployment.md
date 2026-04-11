# Team Deployment Guide

Current status:

- The default rollout is still local-first.
- Each teammate runs the portal and helper on their own Mac.
- The shared internal Portal deployment in this document is a future mode for after the team gets a valid domain/hostname for Google OAuth.

This guide describes the future shared deployment shape for a team of around 10-15 users.

## Deployment Model

- One shared team portal runs on an existing internal Mac.
- Each teammate runs their own local helper on their own Mac.
- Each teammate keeps their own BPMIS login in their own Chrome.
- Teammates open the same portal URL, but the Jira creation request is bridged back to their own helper.

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

Each teammate still needs their own helper because BPMIS access depends on their own Chrome login session.

They should follow the quickstart in:

- [docs/team-member-quickstart.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-quickstart.md)

## Health Checks

Portal host checks:

- open `http://<internal-ip>:5000`
- run `./scripts/run_team_portal_prod.sh status`
- check `./scripts/run_team_portal_prod.sh logs`

Teammate helper checks:

- open `http://127.0.0.1:8787/health`
- confirm the portal shows `Local Helper = Connected`

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

### Local Helper shows Offline

Check:

- teammate has started `./scripts/run_team_stack.sh start`
- teammate can open `http://127.0.0.1:8787/health`
- teammate is still logged into BPMIS in Chrome

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
