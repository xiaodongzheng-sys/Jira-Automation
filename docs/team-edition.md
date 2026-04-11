# Team Edition Experiment

This document defines the first implementation target for a shared internal version of the tool.

## Goal

Support a fixed team of around 10-15 users through one internal web portal without breaking the existing local edition.

## Chosen Direction

- Internal shared web app
- Per-user Google OAuth
- Per-user saved config
- Per-user BPMIS session bridge from the user's own machine
- No formal access control in v1
- No formal audit log in v1

## Why a Local Helper Is Needed

The current BPMIS flow relies on a logged-in Chrome session that lives on the user's own machine.
An internal shared server cannot directly reuse each user's browser session.

So the team edition needs:

- a central web portal
- a lightweight per-user local helper

The central portal handles spreadsheet access and orchestration.
The local helper handles BPMIS access with the user's own local browser session.

The current prototype on this branch now includes:

- user-scoped config persistence on the portal side
- a configurable local helper URL per user
- browser-visible helper health status
- a first helper create endpoint that reuses the local BPMIS API client

## First-Phase Scope

Phase 1 is a proof-of-path, not a full production release.

It should validate:

- different users can keep different Spreadsheet / Header / field config
- the web portal can identify the current user and load that user's config
- the web portal can detect whether that user's local helper is reachable
- the local helper exposes a stable API for health checks and Jira creation
- the helper can use the user's local BPMIS session to create a Jira ticket

## Planned Components

### 1. Central Web Portal

Responsibilities:

- Google OAuth
- spreadsheet read/write
- preview/run UI
- per-user config storage
- helper connection status
- dispatch Jira create requests to the current user's helper

### 2. Local Helper

Responsibilities:

- run on each user's machine
- expose a local HTTP API
- reuse the user's logged-in BPMIS browser/session state
- call BPMIS APIs and return normalized results

Suggested initial endpoints:

- `GET /health`
- `POST /bpmis/create-jira`

## First-Phase Non-Goals

- company-wide rollout
- SSO integration
- full RBAC
- full audit pipeline
- replacing the local edition baseline
