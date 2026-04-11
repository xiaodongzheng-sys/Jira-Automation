# Jira Tool Quick Start

This guide is for teammates using the tool on their own Mac.

## Before You Start

Please make sure:

- you are using a Mac
- you can open BPMIS in Chrome
- your Google account can access the Spreadsheet you need

## First Time Setup

### Step 1: Open BPMIS in Chrome

Open Chrome and make sure you are already logged in to BPMIS.

### Step 2: Start the Tool

Open Terminal in the project folder, then run:

```bash
./scripts/install_team_helper_local.sh
./scripts/run_team_helper.sh start
```

Then open this page in your browser:

[http://127.0.0.1:5000](http://127.0.0.1:5000)

### Step 3: Connect Google

On the page, click `Connect Google`.

Use the Google account that can access your Spreadsheet.

### Step 4: Check the Status Box

You should see:

- `Google Sheets = Connected`
- `Local Helper = Connected`

If both are connected, you can continue.

### Step 5: Fill in Your Settings

On the page, fill in:

- your Spreadsheet link
- your Input tab name
- your Issue ID column name
- your Jira Ticket Link column name
- the remaining mapping fields your team uses

Then click `Save Web Config`.

## How To Use It Each Time

1. Open BPMIS in Chrome
2. Open [http://127.0.0.1:5000](http://127.0.0.1:5000)
3. Click `Preview Eligible Rows`
4. Check the preview result
5. If the preview looks correct, click `Run Ticket Creation`

## If You See a Problem

### The page does not open

Run this again:

```bash
./scripts/run_team_helper.sh start
```

Then reopen:

[http://127.0.0.1:5000](http://127.0.0.1:5000)

### Local Helper shows Offline

Run:

```bash
./scripts/run_team_helper.sh start
```

### BPMIS error

Go back to Chrome and make sure BPMIS is still logged in.

### Google or Spreadsheet error

Check:

- your Google account can open the Spreadsheet
- the Spreadsheet link is correct
- the tab name is correct
- the column header names are correct

### Still stuck

Send the maintainer:

- a screenshot of the error
- the row number that failed
- what you clicked just before the error happened
