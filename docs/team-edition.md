# Team Edition Experiment

This document defines the first implementation target for a shared internal version of the tool.

## Goal

Support a fixed team of around 10-15 users through one internal web portal without breaking the existing local edition.

## Chosen Direction

Current rollout mode:

- local-first per user

Future rollout mode:

- Internal shared web app
- Per-user Google OAuth restricted to `@npt.sg`
- Per-user saved config
- Per-user BPMIS API access token
- Shared URL through Cloudflare Tunnel
- Encrypted BPMIS token storage on the host
- No formal audit log in v1

## Why Access Token Matters

The BPMIS source code shows that API access is officially supported through `Authorization: Bearer <token>`.
That means the most reliable integration is to use a BPMIS access token directly instead of reusing a browser session.

So the team edition needs:

- a central web portal
- one BPMIS API token per teammate

The central portal handles spreadsheet access and orchestration.
The direct BPMIS API client handles Jira creation with the configured token.

The current prototype on this branch now includes:

- user-scoped config persistence on the portal side
- direct BPMIS API creation from the portal
- shared-mode guardrails that require Google login before use
- `@npt.sg` domain allowlisting support
- encrypted-at-rest storage for portal-saved BPMIS tokens
- self-check coverage for BPMIS API readiness
- a production-oriented portal startup script for an internal Mac host
- a launchd installation script for auto-start on the host Mac
- a teammate quickstart and deployment guide

## First-Phase Scope

Phase 1 is a proof-of-path, not a full production release.

It should validate:

- different users can keep different Spreadsheet / Header / field config
- the web portal can identify the current user and load that user's config
- the web portal can detect whether BPMIS API access is ready
- the portal can use the configured BPMIS token to create a Jira ticket

## Planned Components

### 1. Central Web Portal

Responsibilities:

- Google OAuth
- spreadsheet read/write
- preview/run UI
- per-user config storage
- BPMIS API readiness status
- dispatch Jira create requests directly to BPMIS

## Current Recommended Run Commands

Start the full team-edition prototype locally:

```bash
./scripts/run_team_stack.sh start
```

Useful commands:

```bash
./scripts/run_team_stack.sh status
./scripts/run_team_stack.sh restart
./scripts/run_team_stack.sh stop
./scripts/run_team_stack.sh logs
```

If needed, the web portal can also be managed separately:

```bash
./scripts/run_team_portal.sh start
```

For the low-cost shared deployment shape, see:

- [docs/team-deployment.md](/Users/NPTSG0388/Documents/New%20project/docs/team-deployment.md)
- [docs/team-member-quickstart.md](/Users/NPTSG0388/Documents/New%20project/docs/team-member-quickstart.md)

## First-Phase Non-Goals

- company-wide rollout
- SSO integration
- full RBAC
- full audit pipeline
- replacing the local edition baseline
