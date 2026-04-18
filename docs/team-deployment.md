# Team Deployment Guide

This guide describes the supported **shared-team** setup:

- one Mac hosts the Flask portal
- Cloudflare Tunnel exposes the portal through one stable HTTPS URL
- teammates sign in with `@npt.sg` Google accounts
- each teammate stores their own config and BPMIS token inside the portal

## Host Configuration

Configure these values in `.env` on the host Mac:

```bash
FLASK_SECRET_KEY=change-me
GOOGLE_OAUTH_CLIENT_SECRET_FILE=/absolute/path/to/google-client-secret.json
TEAM_PORTAL_HOST=127.0.0.1
TEAM_PORTAL_PORT=5000
TEAM_PORTAL_BASE_URL=https://jira-tool.example.com
TEAM_ALLOWED_EMAIL_DOMAINS=npt.sg
TEAM_PORTAL_DATA_DIR=/absolute/path/to/team-portal-data
TEAM_PORTAL_CONFIG_ENCRYPTION_KEY=<fernet-key>
```

Generate the encryption key with:

```bash
python3 - <<'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
```

Notes:

- `TEAM_PORTAL_BASE_URL` must match the exact Cloudflare hostname teammates will open
- Google OAuth callback generation uses `TEAM_PORTAL_BASE_URL`
- `TEAM_PORTAL_CONFIG_ENCRYPTION_KEY` is required if teammates will save BPMIS tokens in the shared portal

## Google OAuth Setup

In Google Cloud Console, configure the OAuth client with this callback:

```text
https://jira-tool.example.com/auth/google/callback
```

Replace the hostname with your real Cloudflare hostname.

## Start the Shared Portal

Use the production-style script already included in the repo:

```bash
./scripts/run_team_portal_prod.sh start
./scripts/run_team_portal_prod.sh status
./scripts/run_team_portal_prod.sh logs
```

To stop or restart:

```bash
./scripts/run_team_portal_prod.sh stop
./scripts/run_team_portal_prod.sh restart
```

## Enable Auto-Start on the Host Mac

Install the launchd job:

```bash
./scripts/install_team_portal_launchd.sh
```

Then start it:

```bash
launchctl start io.npt.jira-creation-portal
```

## Cloudflare Tunnel Setup

Create one tunnel on the host Mac that forwards the public hostname to the local Flask port:

```text
https://jira-tool.example.com  ->  http://127.0.0.1:5000
```

Owner-run checklist:

1. Start the portal with `./scripts/run_team_portal_prod.sh start`
2. Start the Cloudflare Tunnel
3. Open the public URL and confirm the homepage loads
4. Confirm Google sign-in returns to the same public URL
5. Run `Self-Check` after signing in with an `@npt.sg` account

## What Teammates Need

Teammates only need:

- the shared URL
- an `@npt.sg` Google account
- their own BPMIS API token

They do not need to install anything locally.

See:

- [docs/team-member-quickstart.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-quickstart.md)

## Health Checks

Host-side checks:

- `./scripts/run_team_portal_prod.sh status`
- `./scripts/run_team_portal_prod.sh logs`
- open the public Cloudflare URL
- verify Google login callback works through the public hostname

Teammate acceptance check:

- open the shared URL on a fresh laptop
- sign in with `@npt.sg`
- save config
- run `Self-Check`
- preview rows
- create Jira successfully

## Common Failures

### Portal cannot be opened

Check:

- the host Mac is awake
- the portal process is running
- the Cloudflare Tunnel is connected

### Google login succeeds but user is denied

Check:

- the teammate is using an `@npt.sg` Google account
- `TEAM_ALLOWED_EMAIL_DOMAINS=npt.sg` is present in `.env`

### BPMIS token cannot be saved

Check:

- `TEAM_PORTAL_CONFIG_ENCRYPTION_KEY` is configured on the host

### BPMIS API check fails

Check:

- the teammate saved a valid BPMIS token in the portal
- the token is still active

## v1 Tradeoff

This shared deployment is intentionally lightweight:

- one host Mac
- one Cloudflare Tunnel
- Google login restricted by domain
- uptime depends on the host Mac staying online

That is enough for an internal team rollout before moving to a more formal server or VM.
