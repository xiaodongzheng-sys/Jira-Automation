# Team Deployment Guide

This guide describes the supported **shared-team** setup:

- one Mac hosts the Flask portal
- ngrok exposes the portal through one stable HTTPS URL
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

- `TEAM_PORTAL_BASE_URL` must match the exact ngrok hostname teammates will open
- Google OAuth callback generation uses `TEAM_PORTAL_BASE_URL`
- `TEAM_PORTAL_CONFIG_ENCRYPTION_KEY` is required if teammates will save BPMIS tokens in the shared portal

## Google OAuth Setup

In Google Cloud Console, configure the OAuth client with this callback:

```text
https://jira-tool.example.com/auth/google/callback
```

Replace the hostname with your real ngrok hostname.

## Recommended Host Layout

For the final macOS host setup, do not run the long-lived stack from a repo under protected folders such as:

- `~/Documents`
- `~/Desktop`
- `~/Downloads`
- iCloud Drive paths under `~/Library/Mobile Documents`

Recommended host workspace:

```bash
~/Workspace/jira-creation-stack-host
```

If your current checkout is under a protected folder, use the one-shot setup script:

```bash
./scripts/setup_team_stack_host_workspace.sh
```

That script will:

- create or reuse the recommended host workspace
- copy `.env` on first setup
- install the `launchd` job from the host workspace
- try to start the `launchd` job

## Start the Shared Portal

Use the stack guard as the fixed entrypoint. It keeps the Flask portal and ngrok alive together and is now the recommended day-to-day start command:

```bash
./scripts/run_team_stack.sh start
./scripts/run_team_stack.sh status
./scripts/run_team_stack.sh logs
```

By default this will also enable `caffeinate` on macOS when available, so the host Mac is less likely to sleep and silently drop the portal. You can force the mode explicitly:

```bash
./scripts/run_team_stack.sh start --caffeinate
./scripts/run_team_stack.sh start --no-caffeinate
```

To stop or restart:

```bash
./scripts/run_team_stack.sh stop
./scripts/run_team_stack.sh restart
```

If you need to manage the pieces manually for debugging:

```bash
./scripts/run_team_portal_prod.sh start
./scripts/run_ngrok_tunnel.sh start
```

The stack guard now runs as a lightweight supervisor. It keeps the Flask portal and ngrok as child processes, restarts them with backoff when they crash, probes `/healthz` for the portal, and validates the ngrok inspector API before it reports the stack as healthy.

## Enable Auto-Start on the Host Mac

Install the launchd job for the portal only:

```bash
./scripts/install_team_portal_launchd.sh
```

Or install one launchd job that restores the whole supervised stack:

```bash
./scripts/install_team_stack_launchd.sh
```

Then start it:

```bash
launchctl start io.npt.jira-creation-stack
```

Note:

- if your repo is under a protected macOS folder, `install_team_stack_launchd.sh` will now stop and tell you to use `./scripts/setup_team_stack_host_workspace.sh`
- if you still choose to force-install from a protected path, use:

```bash
TEAM_STACK_ALLOW_PROTECTED_ROOT=1 ./scripts/install_team_stack_launchd.sh
```

## ngrok Setup

Create one tunnel on the host Mac that forwards the public hostname to the local Flask port:

```text
https://jira-tool.example.com  ->  http://127.0.0.1:5000
```

Owner-run checklist:

1. Start the portal with `./scripts/run_team_portal_prod.sh start`
2. Start the public ngrok tunnel
3. Open the public URL and confirm the homepage loads
4. Confirm Google sign-in returns to the same public URL
5. Confirm teammates can finish `Setup`, then use `Run`

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
- `curl http://127.0.0.1:5000/healthz`
- open the public ngrok URL
- verify Google login callback works through the public hostname
- inspect `.team-portal/run/team_stack_status.json` for the latest guard view of portal and ngrok health
- run `./scripts/run_team_stack.sh doctor` for a one-shot end-to-end stack diagnosis
- `doctor` now also checks whether the repo path is launchd-friendly and whether the `launchd` job is installed

Teammate acceptance check:

- open the shared URL on a fresh laptop
- sign in with `@npt.sg`
- save config
- finish `Setup`
- preview rows
- create Jira successfully

## Common Failures

### Portal cannot be opened

Check:

- the host Mac is awake
- the portal process is running
- the ngrok tunnel is connected
- `./scripts/run_team_stack.sh doctor`

### launchd install fails on macOS

Check:

- whether the repo is under `Documents`, `Desktop`, `Downloads`, or iCloud Drive
- whether `GOOGLE_OAUTH_CLIENT_SECRET_FILE` also points into a protected folder

Recommended fix:

```bash
./scripts/setup_team_stack_host_workspace.sh
```

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
- one ngrok tunnel
- Google login restricted by domain
- uptime depends on the host Mac staying online

That is enough for an internal team rollout before moving to a more formal server or VM.
