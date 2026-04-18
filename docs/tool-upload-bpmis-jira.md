# BPMIS Jira Tool Upload Config

Use [create_bpmis_jira_ticket.py](/Users/NPTSG0388/Documents/New%20project/scripts/create_bpmis_jira_ticket.py:1) as the upload file.

## Step 1

- `Dependency Library`: select `requests`
- `Tool name`: `Create Jira Ticket via BPMIS`
- `Tool description`:
  Create one Jira ticket through BPMIS API using explicit input fields such as issue ID, market, summary, task type, assignee, and optional metadata. Use this when the workflow needs to create a single Jira ticket and return the created Jira key and link.

## Step 2 Input Parameters

- `access_token`
  - Type: `string`
  - Required: `true`
  - Description: BPMIS bearer token
- `issue_id`
  - Type: `string`
  - Required: `true`
  - Description: BPMIS parent issue ID
- `market`
  - Type: `string`
  - Required: `true`
  - Description: Market label such as `SG`
- `summary`
  - Type: `string`
  - Required: `true`
  - Description: Jira summary
- `task_type`
  - Type: `string`
  - Required: `false`
  - Description: `Feature`, `Tech`, or `Support`
- `description`
  - Type: `string`
  - Required: `false`
  - Description: Jira description
- `prd_links`
  - Type: `string`
  - Required: `false`
  - Description: PRD link field
- `td_links`
  - Type: `string`
  - Required: `false`
  - Description: TD link field
- `fix_version`
  - Type: `string`
  - Required: `false`
  - Description: Fix version name, multiple values split by `|`
- `component`
  - Type: `string`
  - Required: `false`
  - Description: BPMIS component label
- `priority`
  - Type: `string`
  - Required: `false`
  - Description: BPMIS priority label
- `assignee`
  - Type: `string`
  - Required: `false`
  - Description: Jira assignee email, display name, or username
- `reporter`
  - Type: `string`
  - Required: `false`
  - Description: Jira reporter email, display name, or username
- `product_manager`
  - Type: `string`
  - Required: `false`
  - Description: Product Manager user
- `dev_pic`
  - Type: `string`
  - Required: `false`
  - Description: Dev PIC user
- `qa_pic`
  - Type: `string`
  - Required: `false`
  - Description: QA PIC user
- `biz_pic`
  - Type: `string`
  - Required: `false`
  - Description: Biz PIC user
- `need_uat`
  - Type: `string`
  - Required: `false`
  - Description: Need UAT option label
- `involved_tracks`
  - Type: `string`
  - Required: `false`
  - Description: Involved product track label
- `bpmis_base_url`
  - Type: `string`
  - Required: `false`
  - Description: BPMIS base URL, default `https://bpmis-uat1.uat.npt.seabank.io`

## Step 2 Output Parameters

- `success`
  - Type: `boolean`
- `message`
  - Type: `string`
- `ticket_key`
  - Type: `string`
- `ticket_link`
  - Type: `string`
- `issue_id`
  - Type: `string`
- `resolved_task_type`
  - Type: `string`
- `debug_payload_path`
  - Type: `string`

## Example Input

```json
{
  "access_token": "your-bpmis-token",
  "issue_id": "12345",
  "market": "SG",
  "summary": "Investigate login failure",
  "task_type": "Feature",
  "description": "User cannot log in after password reset"
}
```
