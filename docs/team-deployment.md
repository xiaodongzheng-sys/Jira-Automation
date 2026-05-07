# Team Deployment Guide

This guide describes the supported **shared-team** setup:

- one Mac hosts the Flask portal
- Cloudflare Tunnel exposes the portal through one stable HTTPS URL
- teammates sign in with `@npt.sg` Google accounts
- each teammate stores their own config and BPMIS token inside the portal

This Mac-hosted Cloudflare Tunnel URL is the primary teammate entrypoint and the default release target. Cloud Run can remain available as a UAT/backup surface, but routine deploy/release/live requests should update and verify only the Mac-hosted tunnel portal. Deploy or validate Cloud Run only when the request explicitly says Cloud Run.

## Host Configuration

Configure these values in `.env` on the host Mac:

```bash
FLASK_SECRET_KEY=change-me
GOOGLE_OAUTH_CLIENT_SECRET_FILE=/absolute/path/to/google-client-secret.json
TEAM_PORTAL_HOST=127.0.0.1
TEAM_PORTAL_PORT=5000
TEAM_PORTAL_BASE_URL=https://app.bankpmtool.uk
TEAM_PORTAL_TUNNEL_PROVIDER=cloudflare
TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME=bankpmtool-live
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

- `TEAM_PORTAL_BASE_URL` must match the exact Cloudflare Tunnel hostname teammates will open
- Google OAuth callback generation uses `TEAM_PORTAL_BASE_URL`
- `TEAM_PORTAL_CONFIG_ENCRYPTION_KEY` is required if teammates will save BPMIS tokens in the shared portal

## Google OAuth Setup

In Google Cloud Console, configure the OAuth client with this callback:

```text
https://app.bankpmtool.uk/auth/google/callback
```

Replace the hostname with your real Cloudflare Tunnel hostname.

The portal requests Google Drive and Google Docs read scopes so it can read shared Google Doc links during Work Memory / Gmail evidence ingestion. After scope changes, existing browser sessions and stored owner credentials are not upgraded automatically; sign out and reconnect Google to grant the new `drive.readonly` and `documents.readonly` permissions.

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

Use the stack guard as the fixed entrypoint. It keeps the Flask portal and selected public tunnel alive together and is the recommended day-to-day start command:

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

When the launchd job is installed, `restart` uses `launchctl kickstart -k` so launchd remains the single owner of the guard process and the public tunnel does not get claimed by competing restarts.

If you need to manage the pieces manually for debugging:

```bash
./scripts/run_team_portal_prod.sh start
./scripts/run_cloudflare_tunnel.sh start
```

The stack guard now runs as a lightweight supervisor. It keeps the Flask portal and selected public tunnel as child processes, restarts them with backoff when they crash, probes `/healthz` for the portal, and validates the public tunnel health before it reports the stack as healthy.

For the primary-entry setup, the host `.env` should point `TEAM_PORTAL_BASE_URL` at the same Cloudflare Tunnel hostname that teammates open. Google OAuth callback configuration must use that hostname too:

```text
https://app.bankpmtool.uk/auth/google/callback
```

Cloud Run-specific local-agent settings can stay in `.env` for explicit fallback deployments, but they are not part of the normal Mac portal request path or default release validation.

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

## Cloudflare Tunnel Setup

Create one named tunnel on the host Mac that forwards the public hostname to the local Flask port:

```text
https://app.bankpmtool.uk  ->  http://127.0.0.1:5000
```

Owner-run checklist:

1. Start the portal with `./scripts/run_team_portal_prod.sh start`
2. Start the public Cloudflare Tunnel with `./scripts/run_cloudflare_tunnel.sh start`
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
- open the public Cloudflare Tunnel URL
- verify Google login callback works through the public hostname
- inspect `.team-portal/run/team_stack_status.json` for the latest guard view of portal and tunnel health
- run `./scripts/run_team_stack.sh doctor` for a one-shot end-to-end stack diagnosis
- `doctor` now also checks whether the repo path is launchd-friendly and whether the `launchd` job is installed

Primary-entry acceptance checks:

- Source Code Q&A answers from the Cloudflare Tunnel portal URL.
- BPMIS setup and run flows work from the Cloudflare Tunnel portal URL.
- SeaTalk features read Mac desktop data from the Cloudflare Tunnel portal URL.
- Cloud Run URL is checked only for explicit Cloud Run deployments or validation requests.

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
- the Cloudflare Tunnel is connected
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
- one Cloudflare Tunnel
- Google login restricted by domain
- uptime depends on the host Mac staying online

That is enough for an internal team rollout before moving to a more formal server or VM.
