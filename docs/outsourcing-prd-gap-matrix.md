# Outsourcing PRD/Figma Gap Matrix

This file is the working source-of-truth for the GRC Outsourcing demo.

Scope rules:
- Source files:
  - `/Users/NPTSG0388/Desktop/8.1 Part 1 of PRD - Digital Banking - Shopee Confluence.pdf`
  - `/Users/NPTSG0388/Desktop/8.2 Part 2 of PRD - Digital Banking - Shopee Confluence.pdf`
  - `/Users/NPTSG0388/Desktop/Data Portal.pdf`
- Ignore strikethrough content in the PRD.
- Prefer PRD for behavior and field definitions.
- Use Figma PDF for layout, search fields, visible columns, and modal presence.

## Confirmed source pages

Part 1:
- Page 10: function list covering F05-F13
- Page 12: overview search criteria and list behavior
- Page 22: outsourcing management details layout and arrangement status logic
- Page 35: save behavior and withdrawn arrangement update behavior
- Page 39: SSA submit comment tab behavior
- Page 71: DD layout and tab composition
- Page 75: DD action buttons
- Page 83: DD risk committee tab

Part 2:
- Page 14: on-site visit tabs
- Page 26: termination tabs
- Page 42: report behavior
- Page 56: report confirmation workflow
- Page 58: parameter management additions
- Page 62: authorization management visibility and status display
- Page 64: audit history fields and events
- Page 67: access control role management additions

Figma PDF:
- Page 1 and 3: outsourcing management overview search form and list layout
- Page 2 and 7: full list columns including event and status
- Page 6, 8, 9, 10: arrangement details list, add assessment button, withdraw modal, withdrawn state

## Page matrix

### F01 Outsourcing Management Overview

Confirmed from PRD:
- Search criteria include:
  - `Assessment ID`
  - `Outsourcing Arrangement Identifier`
  - `Full Name`
  - `Description of Outsourced Service(s)`
  - `Category of Provider`
  - `First Commencement`
  - `Other Commencements`
  - `Is the Service Expiry / Next Contract Renewal Date available?`
  - `Service Expiry / Next Contract Renewal Date (Remarks)`
  - `Date of Request`
  - `Event`
  - `Status of Assessment`
- Date range validation is required.
- Search criteria relationship is `AND`.
- `Clear` resets the query condition.

Confirmed from Figma:
- Overview page is a search + table page.
- Primary CTA is `Add New Outsourcing Arrangement`.
- List columns visible in the design include:
  - `Assessment ID`
  - `Outsourcing Arrangement Identifier`
  - `Full Name`
  - `Description of Outsourced Service(s)`
  - `Category of Provider`
  - `First Commencement`
  - `Other Commencement`
  - `Is the Service Expiry / Next Contract Renewal Date available?`
  - `Service Expiry / Next Contract Renewal Date (Remarks)`
  - `Date of Request`
  - `Event`
  - `Status of Assessment`
  - `Action`

Current demo status:
- Partially aligned.
- Current page still uses arrangement-centric summary cards and arrangement-centric columns that are not the primary PRD/Figma structure.

Required corrections:
- Make overview page assessment-list centric, not arrangement-summary centric.
- Replace current search fields with the PRD/Figma search set above.
- Replace current list columns with the PRD/Figma columns above.
- Keep `Add New Outsourcing Arrangement` as the main CTA.

### F02 Outsourcing Management Details

Confirmed from PRD:
- Clicking `Outsourcing Arrangement Identifier` opens `Outsourcing Arrangement Details` in a new tab.
- This page shows all assessments created under the arrangement.
- Arrangement-level fields include at least:
  - `Outsourcing Arrangement Status`
  - `Withdrawal Date`
  - `Termination Date`
- Status logic:
  - `Active` is the default after arrangement creation.
  - `Withdrawn` when all linked assessments are withdrawn.
  - `Terminated` when termination assessment reaches terminal approved state.
- When arrangement status becomes `Withdrawn`, hide:
  - `Add New Assessment`
  - assessment `Edit`
  - assessment `Withdraw`
  - assessment `Reopen`

Confirmed from Figma:
- Detail page header shows arrangement identifier and arrangement status.
- Header may also show `Withdrawal Date` and `Termination Date`.
- The main body is a list of linked assessments.
- `Add New Assessment` is visible while arrangement is active.
- `Withdraw` action opens a confirmation modal.
- Withdrawn state removes `Add New Assessment` and action affordances.

Current demo status:
- Partially aligned.
- Current detail page still uses custom tabs and custom sections not matching the simpler Figma structure.

Required corrections:
- Restructure detail page into:
  - arrangement header fields
  - linked assessment list
  - withdraw modal/state handling
- Apply withdrawn-state behavior to hide buttons.
- Remove extra custom sections that are not part of the PRD/Figma core structure.

### F03 SSA

Confirmed from PRD:
- SSA uses:
  - `Details`
  - `Submit Comment`
  - `Review Comment`
  - optional `Withdraw Comment`
- Submit comment rules:
  - one row per functional unit owning the arrangement
  - only latest submission data remains visible after reassess/reopen/reject
- SSA flow also supports:
  - `Edit SSA`
  - `Submit SSA`
  - `Withdraw SSA`
  - `Reopen SSA`
  - `Add New DD`

Current demo status:
- Partially aligned.
- Needs tighter tab/table structure and less freeform content.

### F04 DD

Confirmed from PRD:
- SG/PH DD tabs:
  - `Details`
  - `Submit Comment`
  - `Review Comment`
  - `Approve Comment`
  - `Risk Committee`
  - optional `Withdraw Comment`
- ID DD excludes `Risk Committee`.
- DD supports:
  - `Edit`
  - `Submit`
  - `Withdraw`
  - `Reopen`
- Risk committee tab is editable and stays visible after first close for SG/PH.

Current demo status:
- Partially aligned.
- Risk committee presence is not yet shown as a first-class tabular area.

### F05 PIR

Confirmed from PRD:
- PIR tabs:
  - `Details`
  - `Submit Comment`
  - `Approve Comment`
  - optional `Withdraw Comment`
- PIR supports:
  - `Edit`
  - `Submit`
  - `Withdraw`

Current demo status:
- Broadly aligned on tab names for new form.
- Needs stronger detail-view structure to mirror the PRD tables.

### F06 On-site Visit

Confirmed from PRD:
- On-site Visit tabs:
  - `Details`
  - `Submit Comment`
  - `Approve Comment`
  - optional `Withdraw Comment`

Current demo status:
- Broadly aligned on tab names.
- Still needs field-order alignment to the PRD-defined add/view layout.

### F07 Termination

Confirmed from PRD:
- SG/PH tabs:
  - `Details`
  - `Submit Comment`
  - `Approve Comment`
  - optional `Withdraw Comment`
- ID tabs:
  - `Details`
  - `Submit Comment`
  - `Approve Comment`
  - `Review Comment`
  - optional `Withdraw Comment`

Current demo status:
- Not market-aware.
- Current implementation should not assume one universal tab set.

Required corrections:
- Reflect SG/PH vs ID differences explicitly.
- If market switching is not implemented, label the assumption clearly and avoid mixing tab sets.

### F08 Report

Confirmed from PRD:
- Reports are user-requested from portal, not fully pre-generated.
- FU access depends on data access control.
- Reports download in `XLSX`.
- `Today` means real-time data up to the current timestamp.

Current demo status:
- Partially aligned.
- Current page has the right direction but still needs explicit XLSX / FU access / real-time semantics in the visible flow.

### F09 Report Confirmation

Confirmed from PRD:
- Applies to SG and PH only.
- System generates two reports on 1 Jun and 1 Dec:
  - `MAS Outsourcing Register - Service Provider and Material Subcontractors`
  - `MAS Outsourcing Register - Non-material Subcontractors`
- Authorization IDs are created per FU approver.

Current demo status:
- Partially aligned.
- Current page still uses generic outsourcing report wording instead of these explicit MAS report names and FU-level confirmation semantics.

### F10 Parameter Management

Confirmed from PRD:
- `Module` dropdown must include `Outsourcing Management`.
- `Field Name` dropdown must include outsourcing fields such as:
  - `Service Scope`
  - `IT Scope`
  - `What is the outsourcing categorization?`
  - `Outsourced Relevant Services / Outsourced Relevant Services - Additional`
  - `Frequency of audit on the provider`
- Outsourcing-related shared parameters should appear when filtering by `Outsourcing Management`.

Current demo status:
- Partially aligned.
- Supporting page exists, but visible fields still need to reflect the actual PRD field list instead of generic placeholders.

### F11 Authorization Management

Confirmed from PRD:
- Authorization page is for in-process requests only.
- Visibility depends on role:
  - `Maker`
  - `Approver`
  - `Reviewer`
- Maker cannot approve/review own request.
- Display rules depend on assessment status:
  - `Pending Approval`
  - `Pending Review`
  - `Closed`
  - `Verified`
  - `Withdraw - Pending Approval`
  - `Withdraw - Pending Review`
  - `Terminated`

Current demo status:
- Partially aligned.
- Needs table semantics that are closer to PRD status-based visibility rules.

### F12 Audit History

Confirmed from PRD:
- Audit fields include:
  - `Audit S/N`
  - `Authorization S/N`
  - `Biz Ref No`
  - `Branch`
  - `System Module`
  - `Resource Name`
  - `Event`
  - `Operator`
  - `Operation Date`
  - `Service Ticket Number`
- Event catalog explicitly includes:
  - view overview
  - add arrangement
  - view details
  - view assessment
  - edit assessment
  - submit withdraw assessment
  - approve withdraw assessment
  - reject withdraw assessment
  - reopen assessment
  - add new DD
  - download report
  - view parameter management
  - submit edited parameter
  - approve edited parameter
  - reject edited parameter

Current demo status:
- Not aligned enough.
- Current authorization-page audit section is only a small subset of full PRD audit history.

### F13 User Access Control

Confirmed from PRD:
- Role management must include outsourcing menu permissions such as:
  - `Overview - View`
  - `Overview - Add`
  - `Details - View`
- This is role-management specific, not just a generic supporting page.

Current demo status:
- Partially aligned.
- Page exists but still mixes incident-first wording and needs tighter outsourcing permission naming.

## Immediate implementation order

1. Rebuild `Outsourcing Management Overview` to match the PRD/Figma search form and assessment list columns.
2. Rebuild `Outsourcing Arrangement Details` into the simpler arrangement-header + linked-assessment-list structure from Figma.
3. Tighten assessment detail pages so tab composition follows PRD by assessment type and market assumptions are explicit.
4. Rename and restructure report confirmation flow around the explicit MAS reports.
5. Expand parameter/access/audit pages using the exact PRD field and event names above.
