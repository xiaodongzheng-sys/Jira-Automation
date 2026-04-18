# Team Member Quick Start

This guide is for teammates using the **shared portal** hosted on one Mac.

You do **not** need to install Python, edit `.env`, or run any terminal commands.

## First Time Use

1. Open the shared portal URL from your teammate
2. Click `Connect Google`
3. Sign in with your `@npt.sg` Google account
4. In `Configuration Studio`, fill in:
   - Spreadsheet Link or ID
   - Input Tab Name
   - BPMIS API Access Token
   - your sheet header names and Jira mapping fields
5. Click `Save Web Config`
6. Run `Self-Check`

## Daily Use

1. Open the shared portal URL
2. Sign in with your `@npt.sg` Google account if needed
3. Click `Preview Eligible Rows`
4. If the preview looks correct, click `Run Ticket Creation`

## Notes

- Your saved config is stored under your Google email and does not affect other teammates.
- Your BPMIS token is stored for your user only.
- If your BPMIS token changes, update it in the portal and save again.

## Troubleshooting

### I cannot open the portal

Ask the portal owner to confirm:

- the host Mac is online
- the shared portal process is running
- the Cloudflare Tunnel is connected

### Google sign-in says I am not allowed

Use your `@npt.sg` Google account.

### Self-Check fails on BPMIS API

Update your BPMIS API token in `Configuration Studio`, save again, then rerun `Self-Check`.
