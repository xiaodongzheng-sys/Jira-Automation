from __future__ import annotations

from flask import Flask, abort, redirect, render_template, request, session, url_for


INCIDENTS = [
    {
        "id": "IM-SG-2026-00428",
        "title": "FAST transfer confirmation delay",
        "type": "Operational Risk Incident",
        "country": "SG",
        "functional_unit": "Payments Operations",
        "status": "Pending Review",
        "owner": "Alicia Tan",
        "materiality": "Material",
        "incident_date": "13 Apr 2026 09:07",
        "reported_date": "13 Apr 2026 09:18",
        "classification": "Service Disruption",
        "summary": "Customers can complete transfers, but confirmation and history refresh are delayed.",
        "impact": "Customer reassurance issue with rising contact-center volume. No fund loss observed.",
        "review_route": "ORM, Legal, RC",
    },
    {
        "id": "IM-SG-2026-00412",
        "title": "Card dispute case upload failure",
        "type": "Operational Risk Incident",
        "country": "SG",
        "functional_unit": "Card Operations",
        "status": "Pending Approval",
        "owner": "Marcus Lee",
        "materiality": "High",
        "incident_date": "11 Apr 2026 14:32",
        "reported_date": "11 Apr 2026 15:04",
        "classification": "Process Failure",
        "summary": "Batch upload to dispute handling queue failed for one processing window.",
        "impact": "Backlog created for operations team; no customer balance impact.",
        "review_route": "Approver only",
    },
    {
        "id": "IM-SG-2026-00397",
        "title": "Ops dashboard access issue",
        "type": "IT Incident",
        "country": "SG",
        "functional_unit": "Operations Technology",
        "status": "Closed",
        "owner": "Yvonne Ng",
        "materiality": "Moderate",
        "incident_date": "07 Apr 2026 08:10",
        "reported_date": "07 Apr 2026 08:30",
        "classification": "System Access",
        "summary": "Internal ops dashboard unavailable after certificate rollover.",
        "impact": "Internal user delay only; no external customer impact.",
        "review_route": "No review tab for IT incident in SG",
    },
]

OUTSOURCING_ARRANGEMENTS = [
    {
        "id": "OUT-SG-2026-0018",
        "name": "Core Payment Switch Managed Service",
        "full_name": "FinStream Technologies Pte Ltd",
        "vendor": "FinStream Technologies Pte Ltd",
        "description_of_services": "Managed operations, monitoring, and failover readiness support for the bank's payment switch.",
        "category_of_provider": "Managed service provider",
        "first_commencement": "01 Jan 2024",
        "other_commencements": "Scope uplift on 01 Mar 2026 for active-active switch rollout",
        "service_expiry_available": "Yes",
        "service_expiry_remarks": "31 Dec 2026 / Annual renewal cycle",
        "service_type": "Managed Service / Critical Banking Operations",
        "arrangement_type": "External outsourcing arrangement",
        "country": "SG",
        "status": "Active",
        "withdrawal_date": "-",
        "termination_date": "-",
        "materiality": "Material",
        "owner": "Operations Technology",
        "relationship_manager": "Rene Chong",
        "service_recipient": "Payments Operations",
        "contract_end": "31 Dec 2026",
        "next_assessment": "15 May 2026",
        "pending_assessments": 2,
        "risk_level": "High",
        "summary": "Managed operations and monitoring for the bank's payment switch, including 24/7 support and failover readiness.",
        "monitoring_note": "Weekly service review with monthly scorecard and open remediation tracker.",
        "critical_service": "FAST / GIRO payment transaction routing",
        "approval_route": "Business Owner, ORM, RC",
        "subcontracting": "Allowed with prior approval and disclosure",
        "mas_register": "Included in MAS outsourcing register",
        "linked_dd": "DD-SG-2026-0015",
        "reporting_market": "SG",
        "data_storage": "Customer and payment transaction data in SG region only",
        "exit_plan": "Tested yearly with fallback switchbook",
    },
    {
        "id": "OUT-SG-2026-0012",
        "name": "Customer Contact Center BPO",
        "full_name": "Seabreeze CX Solutions",
        "vendor": "Seabreeze CX Solutions",
        "description_of_services": "Inbound contact-center support for card and deposit servicing during business hours.",
        "category_of_provider": "Business process outsourcing provider",
        "first_commencement": "01 Oct 2023",
        "other_commencements": "Weekend support scope proposed for FY2026 renewal",
        "service_expiry_available": "Yes",
        "service_expiry_remarks": "30 Sep 2026 / Contractual renewal in progress",
        "service_type": "Business Process Outsourcing",
        "arrangement_type": "External outsourcing arrangement",
        "country": "SG",
        "status": "Pending Review",
        "withdrawal_date": "-",
        "termination_date": "-",
        "materiality": "Moderate",
        "owner": "Customer Service",
        "relationship_manager": "Marcus Lee",
        "service_recipient": "Customer Service",
        "contract_end": "30 Sep 2026",
        "next_assessment": "28 Apr 2026",
        "pending_assessments": 1,
        "risk_level": "Moderate",
        "summary": "Inbound contact-center support for card and deposit servicing during business hours.",
        "monitoring_note": "Quarterly SLA review with complaint trend analysis.",
        "critical_service": "Inbound customer operations support",
        "approval_route": "Business Owner, ORM",
        "subcontracting": "Not allowed",
        "mas_register": "Included in MAS outsourcing register",
        "linked_dd": "DD-SG-2025-0098",
        "reporting_market": "SG",
        "data_storage": "Voice recording metadata retained in SG",
        "exit_plan": "90-day transition plan with sample call replay validation",
    },
    {
        "id": "OUT-PH-2026-0007",
        "name": "Cloud Security Monitoring Retainer",
        "full_name": "GuardRail Cyber Labs",
        "vendor": "GuardRail Cyber Labs",
        "description_of_services": "Supplemental cloud workload monitoring and incident escalation support.",
        "category_of_provider": "Specialist security service provider",
        "first_commencement": "01 Apr 2026",
        "other_commencements": "-",
        "service_expiry_available": "No",
        "service_expiry_remarks": "Pilot arrangement pending first approved assessment",
        "service_type": "Security Monitoring / Specialist Service",
        "arrangement_type": "Specialist security service",
        "country": "PH",
        "status": "Draft",
        "withdrawal_date": "-",
        "termination_date": "-",
        "materiality": "Low",
        "owner": "Information Security",
        "relationship_manager": "Alicia Tan",
        "service_recipient": "Information Security",
        "contract_end": "31 Mar 2027",
        "next_assessment": "TBD",
        "pending_assessments": 0,
        "risk_level": "Low",
        "summary": "Supplemental cloud workload monitoring and incident escalation support.",
        "monitoring_note": "Pilot arrangement pending first SSA submission.",
        "critical_service": "Advisory monitoring support",
        "approval_route": "Business Owner, CISO",
        "subcontracting": "Not declared",
        "mas_register": "Not required for current PH advisory scope",
        "linked_dd": "Not yet created",
        "reporting_market": "PH",
        "data_storage": "No production data shared",
        "exit_plan": "Simple access revocation checklist",
    },
]

OUTSOURCING_ASSESSMENTS = [
    {
        "id": "SSA-SG-2026-0042",
        "arrangement_id": "OUT-SG-2026-0018",
        "event": "Service Supplier Assessment",
        "short_label": "SSA",
        "request_date": "19 Apr 2026",
        "status": "Pending Approval",
        "owner": "Operations Technology",
        "assignee": "Rene Chong",
        "target_date": "22 Apr 2026",
        "updated_at": "19 Apr 2026 10:45",
        "title": "2026 Annual SSA for payment switch managed service",
        "summary": "Re-assessment of operational resiliency, data handling, subcontracting, and exit plan readiness.",
        "authorization_type": "Outsourcing - Authorize New Assessment",
        "route": "Business Owner, ORM, RC",
        "sections": [
            ("Assessment Profile", [("Assessment ID", "SSA-SG-2026-0042"), ("Event", "Service Supplier Assessment"), ("Arrangement ID", "OUT-SG-2026-0018"), ("Status", "Pending Approval")]),
            ("Ownership and Timeline", [("Business Owner", "Operations Technology"), ("Maker", "Rene Chong"), ("Due Date", "22 Apr 2026"), ("Review Route", "Business Owner → ORM → RC")]),
            ("Assessment Notes", [("Scope", "Critical service continuity, data protection, subcontractor transparency, and recovery readiness."), ("Materiality", "Material"), ("Residual Risk", "High"), ("Supporting Documents", "2026_SSA_PaymentSwitch.pdf, BCM_Evidence.zip")]),
        ],
        "comment_tabs": ["Submit Comment", "Review Comment", "Withdraw Comment"],
        "supporting_documents": ["2026_SSA_PaymentSwitch.pdf", "BCM_Evidence.zip"],
        "workflow_actions": ["Edit SSA", "Submit SSA", "Withdraw SSA", "Reopen SSA", "Add New DD"],
        "field_highlights": [
            ("Supplier concentration", "Single strategic vendor for national payment switch support."),
            ("Subcontractor disclosure", "One approved network specialist subcontractor declared."),
            ("Exit readiness", "Annual fallback switch rehearsal completed in March 2026."),
        ],
        "comment_guidance": {
            "submit": "Maker confirms the SSA package, supporting evidence, and outsourcing control responses before sending to authorization.",
            "review": "Reviewer uses this tab to request clarifications on subcontracting, data handling, and resilience evidence before the final decision.",
            "withdraw": "Withdrawal comments record why the submitted SSA should be pulled back and what evidence gap remains open.",
        },
    },
    {
        "id": "DD-SG-2026-0015",
        "arrangement_id": "OUT-SG-2026-0018",
        "event": "Due Diligence",
        "short_label": "DD",
        "request_date": "05 Jan 2026",
        "status": "Verified",
        "owner": "Operations Technology",
        "assignee": "Marcus Lee",
        "target_date": "05 Jan 2026",
        "updated_at": "11 Jan 2026 17:20",
        "title": "Expanded due diligence after platform scope extension",
        "summary": "Expanded DD covering new managed failover module and additional privileged support users.",
        "authorization_type": "Outsourcing - Authorize Edited DD",
        "route": "Business Owner, ORM, CISO",
        "sections": [
            ("Due Diligence Snapshot", [("Assessment ID", "DD-SG-2026-0015"), ("Event", "Due Diligence"), ("Vendor", "FinStream Technologies Pte Ltd"), ("Status", "Verified")]),
            ("Review Focus", [("Security Review", "Completed"), ("Financial Health", "Completed"), ("Subcontractor Disclosure", "No material issue"), ("Risk Committee", "Not required")]),
            ("Decision", [("Decision", "Proceed with compensating controls"), ("Open Items", "Privileged access review every quarter"), ("Last Approved", "11 Jan 2026"), ("Approver", "ORM Lead")]),
        ],
        "comment_tabs": ["Submit Comment", "Review Comment", "Approve Comment", "Withdraw Comment"],
        "supporting_documents": ["Expanded_DD_Pack.pdf", "Vendor_Financials_2026.xlsx"],
        "workflow_actions": ["Edit DD", "Submit DD", "Withdraw DD", "Reopen DD"],
        "field_highlights": [
            ("Financial due diligence", "2026 audited accounts uploaded and reviewed."),
            ("Privileged access governance", "Quarterly access review added as compensating control."),
            ("Risk committee trigger", "Only required when residual risk exceeds approved tolerance."),
        ],
        "comment_guidance": {
            "submit": "Maker documents the DD conclusion, unresolved gaps, and proposed compensating controls before routing onward.",
            "review": "Reviewer validates DD completeness and can request more evidence on control, financial, or subcontractor topics.",
            "approve": "Approver records the formal DD decision and any conditions attached to continued use of the vendor.",
            "withdraw": "Used when the DD package must be withdrawn for rework or because the scope change is no longer proceeding.",
        },
    },
    {
        "id": "PIR-SG-2026-0006",
        "arrangement_id": "OUT-SG-2026-0018",
        "event": "Post-Implementation Review",
        "short_label": "PIR",
        "request_date": "19 Apr 2026",
        "status": "Draft",
        "owner": "Operations Technology",
        "assignee": "Rene Chong",
        "target_date": "15 May 2026",
        "updated_at": "19 Apr 2026 09:02",
        "title": "Post go-live review after active-active switch rollout",
        "summary": "Review whether implemented controls and service KPIs match the approved design after rollout.",
        "authorization_type": "Outsourcing - Authorize New Assessment",
        "route": "Business Owner, ORM",
        "sections": [
            ("PIR Overview", [("Assessment ID", "PIR-SG-2026-0006"), ("Event", "Post-Implementation Review"), ("Implementation Window", "Mar 2026"), ("Status", "Draft")]),
            ("Execution Check", [("Control Design", "Aligned to approved DD"), ("Operational Readiness", "Evidence uploaded"), ("Issue Summary", "No critical gap"), ("Follow-up", "Track failover rehearsal cadence")]),
        ],
        "comment_tabs": ["Submit Comment", "Approve Comment", "Review Comment"],
        "supporting_documents": ["GoLive_Checklist.pdf", "PIR_Evidence_March2026.zip"],
        "workflow_actions": ["Edit PIR", "Submit PIR", "Withdraw PIR"],
        "field_highlights": [
            ("Go-live validation", "Production cutover, failover rehearsal, and operating procedure walk-through captured."),
            ("Issue follow-up", "No critical gaps; medium issue tracked in remediation log."),
            ("Downstream update", "Approved PIR feeds latest status into the outsourcing register."),
        ],
        "comment_guidance": {
            "submit": "Maker confirms implemented controls and KPI outcomes now match the approved design after rollout.",
            "approve": "Approver signs off that post-implementation validation is complete and downstream status can be updated.",
            "withdraw": "Withdrawal comments explain why PIR evidence was incomplete or why the implementation scope changed mid-review.",
        },
    },
    {
        "id": "PR-SG-2026-0004",
        "arrangement_id": "OUT-SG-2026-0012",
        "event": "Periodic Review / Contractual Renewal / Change Management",
        "short_label": "Periodic Review",
        "request_date": "18 Apr 2026",
        "status": "Pending Review",
        "owner": "Customer Service",
        "assignee": "Marcus Lee",
        "target_date": "28 Apr 2026",
        "updated_at": "18 Apr 2026 14:05",
        "title": "FY2026 renewal package for contact center BPO",
        "summary": "Bundled renewal package covering periodic review results, updated commercial terms, and headcount scope change.",
        "authorization_type": "Outsourcing - Authorize New Assessment",
        "route": "Business Owner, ORM",
        "sections": [
            ("Renewal Snapshot", [("Assessment ID", "PR-SG-2026-0004"), ("Event", "Periodic Review / Contractual Renewal / Change Management"), ("Arrangement ID", "OUT-SG-2026-0012"), ("Status", "Pending Review")]),
            ("Commercial and Control Change", [("Renewal Scope", "Extend for 12 months"), ("Commercial Change", "Indexed SLA pricing"), ("Operational Change", "Weekend support coverage added"), ("Pending Items", "Updated BCP walkthrough")]),
        ],
        "comment_tabs": ["Submit Comment", "Review Comment", "Withdraw Comment"],
        "supporting_documents": ["BPO_Renewal_Pack_v3.pdf", "SLA_Addendum.docx"],
        "workflow_actions": ["Edit Periodic Review", "Submit Periodic Review", "Withdraw Periodic Review"],
        "field_highlights": [
            ("Renewal package", "Periodic review, contractual renewal, and change impact bundled in one assessment."),
            ("Commercial change", "Indexed pricing and weekend support coverage added."),
            ("Control uplift", "Updated BCP walkthrough remains the only open item."),
        ],
        "comment_guidance": {
            "submit": "Maker summarizes annual monitoring results, contract renewal changes, and material operational deltas in one submission.",
            "review": "Reviewer comments focus on whether renewal terms or scope changes introduce new outsourcing risk.",
            "withdraw": "Used when renewal negotiations or scope changes are paused and the package must be withdrawn from approval.",
        },
    },
    {
        "id": "OSV-SG-2026-0003",
        "arrangement_id": "OUT-SG-2026-0018",
        "event": "On-site Visit",
        "short_label": "On-site Visit",
        "request_date": "19 Apr 2026",
        "status": "Pending Approval",
        "owner": "Operations Technology",
        "assignee": "Rene Chong",
        "target_date": "06 Jun 2026",
        "updated_at": "19 Apr 2026 11:18",
        "title": "Primary operations center on-site validation",
        "summary": "On-site visit to validate physical access controls, support staffing model, and resilience evidence for the managed service provider.",
        "authorization_type": "Outsourcing - Authorize New Assessment",
        "route": "Business Owner, ORM, CISO",
        "sections": [
            ("Visit Plan", [("Assessment ID", "OSV-SG-2026-0003"), ("Event", "On-site Visit"), ("Location", "FinStream SG operations center"), ("Status", "Pending Approval")]),
            ("Visit Scope", [("Physical Security", "Badge, CCTV, visitor logging"), ("BCP Evidence", "Hot-site failover walkthrough"), ("People Review", "Shift coverage and privileged access"), ("Outcome", "1 medium issue pending closure")]),
        ],
        "comment_tabs": ["Submit Comment", "Approve Comment", "Review Comment"],
        "supporting_documents": ["Visit_Agenda.pdf", "Physical_Security_Photos.zip"],
        "workflow_actions": ["Edit On-site Visit", "Submit On-site Visit", "Withdraw On-site Visit"],
        "field_highlights": [
            ("Physical access", "Badge control, CCTV, and visitor logging tested on-site."),
            ("People and coverage", "Shift roster and privileged operator coverage validated."),
            ("Evidence type", "Photos, walkthrough notes, and issue log retained in one pack."),
        ],
        "comment_guidance": {
            "submit": "Maker submits the on-site visit observations, issue severity, and evidence of walkthrough completion.",
            "approve": "Approver confirms on-site observations are sufficient and the remaining issue log is acceptable.",
            "withdraw": "Used when the visit was postponed, evidence was incomplete, or the issue set needs revalidation.",
        },
    },
    {
        "id": "TERM-ID-2026-0002",
        "arrangement_id": "OUT-PH-2026-0007",
        "event": "Termination",
        "short_label": "Termination",
        "request_date": "18 Apr 2026",
        "status": "Withdraw - Pending Approval",
        "owner": "Information Security",
        "assignee": "Alicia Tan",
        "target_date": "30 Apr 2026",
        "updated_at": "18 Apr 2026 19:15",
        "title": "Withdraw termination request for cloud monitoring retainer",
        "summary": "Termination was requested during scope review, then withdrawn after service scope was narrowed to advisory support.",
        "authorization_type": "Outsourcing - Authorize Withdraw Assessment",
        "route": "Business Owner, CISO",
        "sections": [
            ("Termination Request", [("Assessment ID", "TERM-ID-2026-0002"), ("Event", "Termination"), ("Reason", "Scope reduction"), ("Status", "Withdraw - Pending Approval")]),
            ("Exit Planning", [("Data Return", "Not started"), ("Access Revocation", "Draft checklist"), ("Replacement Vendor", "Not required"), ("Approval Route", "Business Owner → CISO")]),
        ],
        "comment_tabs": ["Submit Comment", "Approve Comment", "Withdraw Comment"],
        "supporting_documents": ["Termination_Request.docx", "Access_Revocation_Checklist.xlsx"],
        "workflow_actions": ["Edit Termination", "Submit Termination", "Withdraw Termination"],
        "field_highlights": [
            ("Data return and destruction", "Checklist tracks asset return, data purge, and confirmation evidence."),
            ("Access revocation", "System and privileged vendor access decommissioned through owner attestation."),
            ("Replacement readiness", "Not required for this advisory scope after withdrawal request."),
        ],
        "comment_guidance": {
            "submit": "Maker records exit readiness, access revocation, and data return status before routing the termination package.",
            "approve": "Approver confirms the exit controls are sufficient or that the withdrawal of termination is acceptable.",
            "review": "Reviewer comments capture whether termination readiness, data return, and access revocation details are sufficient before approval.",
        },
    },
]


def _find_incident(incident_id: str) -> dict | None:
    for incident in INCIDENTS:
        if incident["id"] == incident_id:
            return incident
    return None


def _find_arrangement(arrangement_id: str) -> dict | None:
    for arrangement in OUTSOURCING_ARRANGEMENTS:
        if arrangement["id"] == arrangement_id:
            return arrangement
    return None


def _find_assessment(assessment_id: str) -> dict | None:
    for assessment in OUTSOURCING_ASSESSMENTS:
        if assessment["id"] == assessment_id:
            return assessment
    return None


def _assessment_event_key(assessment: dict) -> str:
    if assessment["id"].startswith("SSA"):
        return "ssa"
    if assessment["id"].startswith("DD"):
        return "dd"
    if assessment["id"].startswith("PIR"):
        return "pir"
    if assessment["id"].startswith("PR-"):
        return "periodic-review"
    if assessment["id"].startswith("OSV"):
        return "on-site-visit"
    if assessment["id"].startswith("TERM"):
        return "termination"
    return "ssa"


def _outsourcing_activity_rows(assessment: dict) -> list[tuple[str, str]]:
    event_key = _assessment_event_key(assessment)
    activity_map = {
        "ssa": [
            ("Added SSA", 'Upon user clicks on "Add New Assessment", selected "Event = Service Supplier Assessment" and Assessment ID created.'),
            ("Edited SSA", 'Upon user clicks on "Edit" and changes have been saved.'),
            ("Submitted new SSA", 'Upon user clicks "Submit" and SSA submitted for approval.'),
            ("Submitted withdraw SSA", 'Upon user clicks "Withdraw" and SSA submitted for approval.'),
            ("Withdrawn", 'Upon user clicks "Withdraw" while SSA is Draft, or after withdrawal approval is granted.'),
        ],
        "dd": [
            ("Added DD", 'Upon user clicks on "Add New DD" and DD Assessment ID created.'),
            ("Edited DD", 'Upon user clicks on "Edit" and changes have been saved.'),
            ("Submitted new DD", 'Upon user clicks "Submit" and DD submitted for approval.'),
            ("Approved new DD", "Upon Approver / Reviewer approves the DD submission. Each authorization action is shown separately."),
            ("Submitted withdraw DD", 'Upon user clicks "Withdraw" and DD submitted for approval.'),
            ("Withdrawn", 'Upon user clicks "Withdraw" while DD is Draft, or after withdrawal approval is granted.'),
        ],
        "pir": [
            ("Added PIR Assessment", 'Upon user clicks on "Add New Assessment", selected "Event = Post-Implementation Review" and Assessment ID created.'),
            ("Edited PIR Assessment", 'Upon user clicks on "Edit" and changes have been saved.'),
            ("Submitted new PIR Assessment", 'Upon user clicks "Submit" and PIR Assessment submitted for approval.'),
            ("Re-assess PIR Assessment", 'Upon user clicks on "Re-assess".'),
            ("Approved new PIR Assessment", "Upon Approver / Reviewer approves the new PIR Assessment. Each authorization action is shown separately."),
            ("Rejected new PIR Assessment", "Upon Approver / Reviewer rejects the new PIR Assessment. Each authorization action is shown separately."),
            ("Submitted withdraw PIR Assessment", 'Upon user clicks "Withdraw" and PIR Assessment submitted for approval.'),
            ("Approved withdraw PIR Assessment", "Upon Approver approves PIR Assessment withdrawal request."),
            ("Rejected withdraw PIR Assessment", "Upon Approver rejects PIR Assessment withdrawal request."),
            ("Withdrawn", 'Upon user clicks "Withdraw" while PIR is Draft, or after withdrawal approval is granted for a closed PIR.'),
        ],
        "periodic-review": [
            ("Added Periodic Review Assessment", 'Upon user clicks on "Add New Assessment", selected "Event = Periodic Review / Contractual Renewal / Change Management" and Assessment ID created.'),
            ("Edited Periodic Review Assessment", 'Upon user clicks on "Edit" and changes have been saved.'),
            ("Submitted new Periodic Review Assessment", 'Upon user clicks "Submit" and the assessment submitted for approval.'),
            ("Submitted withdraw Periodic Review Assessment", 'Upon user clicks "Withdraw" and the assessment submitted for approval.'),
            ("Withdrawn", 'Upon user clicks "Withdraw" while the assessment is Draft, or after withdrawal approval is granted.'),
        ],
        "on-site-visit": [
            ("Added On-site Visit Assessment", 'Upon user clicks on "Add New Assessment", selected "Event = On-site Visit" and Assessment ID created.'),
            ("Edited On-site Visit Assessment", 'Upon user clicks on "Edit" and changes have been saved.'),
            ("Submitted new On-site Visit Assessment", 'Upon user clicks "Submit" and On-site Visit Assessment submitted for approval.'),
            ("Approved new On-site Visit Assessment", "Upon Approver / Reviewer approves the On-site Visit Assessment. Each authorization action is shown separately."),
            ("Submitted withdraw On-site Visit Assessment", 'Upon user clicks "Withdraw" and On-site Visit Assessment submitted for approval.'),
            ("Withdrawn", 'Upon user clicks "Withdraw" while the assessment is Draft, or after withdrawal approval is granted.'),
        ],
        "termination": [
            ("Added Termination Assessment", 'Upon user clicks on "Add New Assessment", selected "Event = Termination" and Assessment ID created.'),
            ("Edited Termination Assessment", 'Upon user clicks on "Edit" and changes have been saved.'),
            ("Submitted new Termination Assessment", 'Upon user clicks "Submit" and Termination Assessment submitted for approval.'),
            ("Submitted withdraw Termination Assessment", 'Upon user clicks "Withdraw" and Termination Assessment withdrawal request submitted for approval.'),
            ("Approved withdraw Termination Assessment", "Upon Approver approves Termination Assessment withdrawal request."),
            ("Withdrawn", 'Upon user clicks "Withdraw" while the assessment is Draft, or after withdrawal approval is granted.'),
        ],
    }
    return activity_map.get(event_key, activity_map["ssa"])


def _build_outsourcing_overview_rows() -> list[dict]:
    rows: list[dict] = []
    for assessment in OUTSOURCING_ASSESSMENTS:
        arrangement = _find_arrangement(assessment["arrangement_id"])
        if arrangement is None:
            continue
        rows.append(
            {
                "assessment_id": assessment["id"],
                "arrangement_id": arrangement["id"],
                "full_name": arrangement["full_name"],
                "description_of_services": arrangement["description_of_services"],
                "category_of_provider": arrangement["category_of_provider"],
                "first_commencement": arrangement["first_commencement"],
                "other_commencement": arrangement["other_commencements"],
                "service_expiry_available": arrangement["service_expiry_available"],
                "service_expiry_remarks": arrangement["service_expiry_remarks"],
                "date_of_request": assessment["request_date"],
                "event": assessment["event"],
                "status": assessment["status"],
            }
        )
    return rows


def _build_assessment_actions(arrangement: dict, assessment: dict) -> list[dict]:
    actions = [
        {
            "label": "View",
            "href": url_for(
                "outsourcing_assessment_detail",
                arrangement_id=arrangement["id"],
                assessment_id=assessment["id"],
            ),
        }
    ]
    if arrangement["status"] == "Withdrawn":
        return actions

    actions.append(
        {
            "label": "Edit",
            "href": url_for(
                "outsourcing_assessment_edit",
                arrangement_id=arrangement["id"],
                assessment_id=assessment["id"],
            ),
        }
    )
    if "Withdraw" in assessment["status"] or assessment["status"] == "Withdrawn":
        actions.append({"label": "Reopen", "href": "#"})
    else:
        actions.append({"label": "Withdraw", "href": "#"})
    return actions


def _build_ssa_detail_sections(arrangement: dict, assessment: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    return [
        (
            "Key Assessment Details",
            [
                ("Assessment ID", assessment["id"]),
                ("Event", "Onboarding of New Arrangement"),
                ("Status of Assessment", assessment["status"]),
                ("Date of Request", assessment["request_date"]),
            ],
        ),
        (
            "General Information",
            [
                ("Outsourcing Arrangement Identifier", arrangement["id"]),
                ("Name of Contracting Entity", "SeaBank Singapore"),
                ("Business Self Assessment Identifier", "BSA-2026-0041"),
                ("Functional Unit Owning the Outsourcing Arrangement", "Operations Technology, Payments Operations"),
                ("Functional Unit Owner (i.e. Head of Department)", "Head of Operations Technology, Head of Payments Operations"),
                ("Main PIC (Unit User who created the SSA)", assessment["assignee"]),
                ("Co-PIC", "melissa.tan@npt.sg"),
                ("Full Name of Service Provider / Subcontractor", arrangement["full_name"]),
                ("Business Registration Number of Service Provider / Subcontractor", "201801234D"),
                ("Country of Registration of Service Provider / Subcontractor", "SG - Singapore"),
                ("Category of Service Provider / Subcontractor", "Service Provider"),
                ("If “Category of Service Provider” is Subcontractor, list out the full name of service provider", "-"),
            ],
        ),
        (
            "Nature and Scope of the Third Party Service",
            [
                (
                    "Service Scope",
                    "Managed operations, monitoring, and failover readiness support for the bank's payment switch, including BAU break-fix handling and daily health-check routines.",
                ),
                (
                    "IT Scope",
                    "Vendor has monitored access to dashboards and secure operational interfaces. No standing production write access is granted after go live.",
                ),
            ],
        ),
        (
            "Categorization and Materiality",
            [
                ("What is the outsourcing categorization?", "Material Outsourced Relevant Service"),
            ],
        ),
    ]


def _build_dd_detail_sections(arrangement: dict, assessment: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    return [
        (
            "Key Assessment Details",
            [
                ("Assessment ID", assessment["id"]),
                ("Assessment ID (tagged SSA)", arrangement["linked_dd"].replace("DD", "SSA") if arrangement.get("linked_dd", "").startswith("DD") else "SSA-SG-2026-0042"),
                ("Event", "Due Diligence"),
                ("Status of Assessment", assessment["status"]),
                ("Date of Request", assessment["request_date"]),
                ("Date of Approval", "2026-01-11"),
                ("Reopen Date", "-"),
                ("Reopen Justification", "-"),
                ("Reopen Supporting Document", "-"),
                ("Withdrawal Date", "-"),
                ("Withdrawal Justification", "-"),
            ],
        ),
        (
            "General Information",
            [
                ("Outsourcing Arrangement Identifier", arrangement["id"]),
                ("Name of Contracting Entity", "SeaBank Singapore"),
                ("Business Self Assessment Identifier", "BSA-2026-0041"),
                ("Functional Unit Owning the Outsourcing Arrangement", "Operations Technology, Payments Operations"),
                ("Functional Unit Owner (i.e. Head of Department)", "Head of Operations Technology, Head of Payments Operations"),
                ("Main PIC", assessment["assignee"]),
                ("Co-PIC", "melissa.tan@npt.sg"),
                ("Full Name of Service Provider / Subcontractor", arrangement["full_name"]),
                ("Business Registration Number of Service Provider / Subcontractor", "201801234D"),
                ("Country of Registration of Service Provider / Subcontractor", "SG - Singapore"),
                ("Category of Service Provider / Subcontractor", "Service Provider"),
                ("If “Category of Service Provider” is Subcontractor, list out the full name of service provider", "-"),
            ],
        ),
        (
            "Nature and Scope of the Third Party Service",
            [
                ("Is this an ongoing outsourced relevant service?", "Yes"),
                ("What is the materiality of the outsourcing arrangement?", "Material"),
                ("When was the materiality assessment performed?", "2024-01-01"),
                ("Does the provider support business operations that are critical to the institution?", "Yes"),
                ("Was there a change in materiality resulting from the latest assessment performed for the outsourcing arrangement?", "Yes (please elaborate in the Other Comments column)"),
                ("Other Comments", "Vendor changed management."),
                ("Is customer information disclosed to, accessed, collected, copied, modified, used, stored or processed by the provider?", "Yes"),
                (
                    "Service Scope",
                    "Managed operations, monitoring, and failover readiness support for the payment switch including run-book execution, alert investigation, and service restoration support.",
                ),
                (
                    "IT Scope",
                    "Vendor handles monitored operational tooling access, secure file transfer, and break-fix support processes. No standing production write privilege is granted.",
                ),
                ("Input all the countries or jurisdictions where customer information will be disclosed to, or accessed, collected, copied, modified, used, stored or processed", "BJ - Benxi; GU - Guam"),
                ("List all the countries or jurisdictions where the service will be carried out", "BT - Bhutan; GU - Guam"),
                ("Input all the cities where the service will be carried out", "1. Singapore"),
                ("Has one alternate service provider been identified?", "No"),
                ("Other Comments (alternate provider)", "No other solution for now."),
                ("When was the independent audit last conducted on the provider?", "2023-07-01"),
                ("Who conducted the audit?", "KPMG"),
                ("When was the last Business Continuity Plan test?", "2023-06-30"),
                ("Have all the objectives of the Business Continuity Plan test been successfully met?", "No (please elaborate in the Other Comments column)"),
                ("Other Comments (BCP test)", "Flash flood on that execution day."),
            ],
        ),
        (
            "Categorization and Materiality",
            [
                ("What is the outsourcing categorization?", "Material Outsourced Relevant Service"),
                ("Frequency of audit on the provider", "Annually"),
            ],
        ),
    ]


def _build_pir_detail_sections(arrangement: dict, assessment: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    return [
        (
            "Key Assessment Details",
            [
                ("Assessment ID", assessment["id"]),
                ("Event", "Post-Implementation Review"),
                ("Status of Assessment", assessment["status"]),
                ("Date of Request", assessment["request_date"]),
                ("Date of Approval", "-"),
                ("Withdrawal Date", "-"),
            ],
        ),
        (
            "General Information",
            [
                ("Outsourcing Arrangement Identifier", arrangement["id"]),
                ("Full Name of Service Provider / Subcontractor", arrangement["full_name"]),
                ("Functional Unit Owning the Outsourcing Arrangement", "Operations Technology, Payments Operations"),
                ("Functional Unit Owner (i.e. Head of Department)", "Head of Operations Technology, Head of Payments Operations"),
                ("Main PIC", assessment["assignee"]),
                ("Co-PIC", "melissa.tan@npt.sg"),
            ],
        ),
        (
            "Implementation Review",
            [
                ("Implemented control design", "Aligned to approved DD and go-live scope."),
                ("Post go-live observation", "No critical issue observed after active-active switch rollout."),
                ("Operational KPI outcome", "Stable transaction routing and reconciliation performance."),
                ("Follow-up", "Continue quarterly failover rehearsal tracking."),
            ],
        ),
    ]


def _build_periodic_review_detail_sections(arrangement: dict, assessment: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    return [
        (
            "Key Assessment Details",
            [
                ("Assessment ID", assessment["id"]),
                ("Event", "Periodic Review / Contractual Renewal / Change Management"),
                ("Status of Assessment", assessment["status"]),
                ("Date of Request", assessment["request_date"]),
                ("Date of Approval", "-"),
                ("Withdrawal Date", "-"),
            ],
        ),
        (
            "General Information",
            [
                ("Outsourcing Arrangement Identifier", arrangement["id"]),
                ("Full Name of Service Provider / Subcontractor", arrangement["full_name"]),
                ("Functional Unit Owning the Outsourcing Arrangement", arrangement["owner"]),
                ("Functional Unit Owner (i.e. Head of Department)", "Head of Customer Service"),
                ("Main PIC", assessment["assignee"]),
                ("Co-PIC", "marcus.lee@npt.sg"),
            ],
        ),
        (
            "Periodic Review / Contractual Renewal / Change Management Details",
            [
                ("Commercial Change", "Indexed SLA pricing"),
                ("Operational Change", "Weekend support coverage added"),
                ("Business continuity walkthrough", "Pending refreshed evidence"),
                ("Report Confirmation", "Required for SG / PH report pack"),
            ],
        ),
    ]


def _build_on_site_detail_sections(arrangement: dict, assessment: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    return [
        (
            "Key Assessment Details",
            [
                ("Assessment ID", assessment["id"]),
                ("Event", "On-site Visit"),
                ("Status of Assessment", assessment["status"]),
                ("Date of Request", assessment["request_date"]),
                ("Date of Approval", "-"),
                ("Withdrawal Date", "-"),
            ],
        ),
        (
            "General Information",
            [
                ("Outsourcing Arrangement Identifier", arrangement["id"]),
                ("Full Name of Service Provider / Subcontractor", arrangement["full_name"]),
                ("Functional Unit Owning the Outsourcing Arrangement", "Operations Technology"),
                ("Functional Unit Owner (i.e. Head of Department)", "Head of Operations Technology"),
                ("Main PIC", assessment["assignee"]),
                ("Co-PIC", "melissa.tan@npt.sg"),
            ],
        ),
        (
            "On-site Visit Details",
            [
                ("Visit Location", "FinStream SG operations center"),
                ("Physical security walkthrough", "Badge access, CCTV coverage, and visitor logging were verified on-site."),
                ("Business continuity evidence", "Hot-site failover walkthrough and recovery seat readiness were demonstrated."),
                ("Follow-up", "One medium issue tracked for privileged operator roster refresh."),
            ],
        ),
    ]


def _build_termination_detail_sections(arrangement: dict, assessment: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    return [
        (
            "Key Assessment Details",
            [
                ("Assessment ID", assessment["id"]),
                ("Event", "Termination"),
                ("Status of Assessment", assessment["status"]),
                ("Date of Request", assessment["request_date"]),
                ("Date of Approval", "-"),
                ("Withdrawal Date", "-"),
            ],
        ),
        (
            "General Information",
            [
                ("Outsourcing Arrangement Identifier", arrangement["id"]),
                ("Full Name of Service Provider / Subcontractor", arrangement["full_name"]),
                ("Functional Unit Owning the Outsourcing Arrangement", arrangement["owner"]),
                ("Functional Unit Owner (i.e. Head of Department)", "Head of Information Security"),
                ("Main PIC", assessment["assignee"]),
                ("Co-PIC", "andrew.lim@npt.sg"),
            ],
        ),
        (
            "Termination Details",
            [
                ("Termination rationale", "Service scope was reduced and advisory support no longer requires the existing outsourcing arrangement setup."),
                ("Data return and destruction", "No production data return is required for the narrowed advisory scope."),
                ("Access revocation", "Checklist prepared for revoking vendor access if termination proceeds."),
                ("Replacement or closure impact", "No replacement vendor is required for the current advisory-only model."),
            ],
        ),
    ]


def _default_assessment_for_event(arrangement: dict, event_key: str) -> dict:
    defaults = {
        "ssa": {
            "id": "SSA-SG-2026-0049",
            "event": "Service Supplier Assessment",
            "status": "Draft",
            "request_date": "19 Apr 2026",
            "assignee": "rene.chong@npt.sg",
            "short_label": "SSA",
        },
        "dd": {
            "id": "DD-SG-2026-0019",
            "event": "Due Diligence",
            "status": "Draft",
            "request_date": "19 Apr 2026",
            "assignee": "marcus.lee@npt.sg",
            "short_label": "DD",
        },
        "pir": {
            "id": "PIR-SG-2026-0009",
            "event": "Post-Implementation Review",
            "status": "Draft",
            "request_date": "19 Apr 2026",
            "assignee": "rene.chong@npt.sg",
            "short_label": "PIR",
        },
        "periodic-review": {
            "id": "PR-SG-2026-0005",
            "event": "Periodic Review / Contractual Renewal / Change Management",
            "status": "Draft",
            "request_date": "19 Apr 2026",
            "assignee": "marcus.lee@npt.sg",
            "short_label": "Periodic Review",
        },
        "on-site-visit": {
            "id": "OSV-SG-2026-0004",
            "event": "On-site Visit",
            "status": "Draft",
            "request_date": "19 Apr 2026",
            "assignee": "rene.chong@npt.sg",
            "short_label": "On-site Visit",
        },
        "termination": {
            "id": "TERM-ID-2026-0003",
            "event": "Termination",
            "status": "Draft",
            "request_date": "19 Apr 2026",
            "assignee": "alicia.tan@npt.sg",
            "short_label": "Termination",
        },
    }
    return defaults.get(event_key, defaults["ssa"]).copy()


def _build_assessment_detail_sections(event_key: str, arrangement: dict, assessment: dict) -> list[tuple[str, list[tuple[str, str]]]]:
    if event_key == "ssa":
        return _build_ssa_detail_sections(arrangement, assessment)
    if event_key == "dd":
        return _build_dd_detail_sections(arrangement, assessment)
    if event_key == "pir":
        return _build_pir_detail_sections(arrangement, assessment)
    if event_key == "periodic-review":
        return _build_periodic_review_detail_sections(arrangement, assessment)
    if event_key == "on-site-visit":
        return _build_on_site_detail_sections(arrangement, assessment)
    if event_key == "termination":
        return _build_termination_detail_sections(arrangement, assessment)
    return assessment.get("sections", [])


def _build_risk_committee_view(event_key: str, arrangement: dict, review_condition_rows: list[tuple[str, str]] | None = None) -> dict | None:
    if event_key != "dd" or arrangement["country"] not in {"SG", "PH"}:
        return None
    return {
        "outcome": "Approved",
        "rmc_date": "2026-01-10" if arrangement["country"] == "SG" else None,
        "roc_date": "2026-01-12" if arrangement["country"] == "PH" else None,
        "conditions": review_condition_rows or [
            ("Quarterly privileged access review evidence", "2026-03-31"),
            ("Refresh network segmentation attestation", "2026-06-30"),
        ],
        "review_comment": "RMC noted the residual risk and accepted the compensating controls.",
        "supporting_documents": ["RMC_Minutes_Jan2026.pdf", "DD_RiskCommittee_Tracker.xlsx"],
    }


def _build_assessment_comment_payload(event_key: str, arrangement: dict, assessment: dict) -> dict:
    submit_comment_rows = [
        ("Operations Technology", "rene.chong@npt.sg", "Submitted", "Initial SSA pack and supporting evidence are complete for Operations Technology."),
        ("Payments Operations", "", "", ""),
    ]
    review_comment_rows = [
        ("ORM", "cui.qiang@npt.sg", "Approved/Reviewed", "2026-04-19", "Residual risk remains high, but compensating controls are in place."),
        ("RC", "cheryl.tan@npt.sg", "Approved/Reviewed", "2026-04-19", "No regulatory notification gap identified for this cycle."),
        ("CISO", "", "", "", ""),
        ("IT Controls", "", "", "", ""),
    ]
    withdraw_comment_rows = [
        ("ORM", "", "", "", ""),
        ("RC", "", "", "", ""),
        ("CISO", "", "", "", ""),
        ("IT Controls", "", "", "", ""),
    ]
    approve_comment_rows = [("Approver", "", "", "", "")]
    review_condition_rows: list[tuple[str, str]] = []

    if event_key == "dd":
        submit_comment_rows = [
            ("Operations Technology", "marcus.lee@npt.sg", "Submitted", "DD package, vendor finance review, and security evidence are ready for routing."),
            ("Payments Operations", "cheryl.tan@npt.sg", "Submitted", "Operational dependency and contingency planning were reviewed by Payments Operations."),
        ]
        review_comment_rows = [
            ("ORM", "cui.qiang@npt.sg", "Approved/Reviewed", "2026-01-09", "Proceed with quarterly privileged access review as a condition."),
            ("RC", "cheryl.tan@npt.sg", "Approved/Reviewed", "2026-01-09", "No regulatory submission is required for the current scope uplift."),
            ("CISO", "vincent.ong@npt.sg", "Approved/Reviewed", "2026-01-10", "Security review accepted with one compensating control."),
            ("IT Controls", "helen.lim@npt.sg", "Approved/Reviewed", "2026-01-10", "Control evidence is sufficient for the managed switch environment."),
        ]
        approve_comment_rows = [
            ("Approver", "orm.approver@npt.sg", "Approved/Reviewed", "2026-01-11", "MAS register confirmation is accepted for the latest DD pack."),
        ]
        review_condition_rows = [
            ("Quarterly privileged access review evidence", "2026-03-31"),
            ("Refresh network segmentation attestation", "2026-06-30"),
        ]
    elif event_key == "pir":
        submit_comment_rows = [
            ("Operations Technology", "rene.chong@npt.sg", "Submitted", "PIR evidence pack is complete for Operations Technology."),
            ("Payments Operations", "", "", ""),
        ]
        approve_comment_rows = [
            ("Operations Technology", "", "", "", ""),
            ("Payments Operations", "", "", "", ""),
        ]
        withdraw_comment_rows = [
            ("Operations Technology", "", "", "", ""),
            ("Payments Operations", "", "", "", ""),
        ]
    elif event_key == "on-site-visit":
        submit_comment_rows = [
            ("Operations Technology", "rene.chong@npt.sg", "Submitted", "On-site visit agenda, evidence pack, and issue log are ready for approval."),
        ]
        approve_comment_rows = [
            ("Operations Technology", "ops.approver@npt.sg", "Approved/Reviewed", "2026-04-19", "On-site observations are acceptable and follow-up can be tracked offline."),
        ]
        withdraw_comment_rows = [("Operations Technology", "", "", "", "")]
    elif event_key == "termination":
        submit_comment_rows = [
            ("Information Security", "alicia.tan@npt.sg", "Submitted", "Termination proposal and withdrawal rationale were prepared for Information Security review."),
        ]
        approve_comment_rows = [("Information Security", "", "", "", "")]
        withdraw_comment_rows = [("Information Security", "", "", "", "")]
        if arrangement["country"] == "ID":
            review_comment_rows = [("Legal", "", "", "", "")]

    return {
        "submit_comment_rows": submit_comment_rows,
        "review_comment_rows": review_comment_rows,
        "approve_comment_rows": approve_comment_rows,
        "withdraw_comment_rows": withdraw_comment_rows,
        "review_condition_rows": review_condition_rows,
        "risk_committee_view": _build_risk_committee_view(event_key, arrangement, review_condition_rows),
    }


def _assessment_detail_actions(arrangement: dict, assessment: dict) -> list[str]:
    event_key = _assessment_event_key(assessment)
    actions = ["Download"]
    status = assessment["status"]
    if arrangement["status"] in {"Withdrawn", "Terminated"}:
        return actions
    if event_key == "ssa":
        if status in {"Draft", "Pending Review", "Pending Approval", "Closed"}:
            actions.append("Withdraw")
        if status in {"Draft", "Closed"}:
            actions.append("Edit")
        if status == "Closed":
            actions.append("Reopen")
            actions.append("Add New DD")
        return actions
    if event_key == "dd":
        if status in {"Draft", "Pending Review", "Pending Approval", "Closed", "Verified"}:
            actions.append("Withdraw")
        actions.append("Download")
        if status in {"Draft", "Closed", "Verified"}:
            actions.append("Edit")
        if status in {"Closed", "Verified"}:
            actions.append("Reopen")
        return list(dict.fromkeys(actions))
    if event_key == "pir":
        if status in {"Draft", "Pending Approval", "Closed"}:
            actions.append("Withdraw")
        actions.append("Download")
        if status in {"Draft", "Closed"}:
            actions.append("Edit")
        return list(dict.fromkeys(actions))
    if event_key == "on-site-visit":
        if status in {"Draft", "Pending Approval", "Closed"}:
            actions.append("Withdraw")
        actions.append("Download")
        if status in {"Draft", "Closed"}:
            actions.append("Edit")
        return list(dict.fromkeys(actions))
    if event_key == "termination":
        if status in {"Draft", "Pending Approval", "Pending Review"}:
            actions.append("Withdraw")
        actions.append("Download")
        if status == "Draft":
            actions.append("Edit")
        return list(dict.fromkeys(actions))
    if status in {"Draft", "Pending Review", "Pending Approval", "Closed", "Verified"}:
        actions.append("Withdraw")
    if status in {"Draft", "Closed", "Verified"}:
        actions.append("Edit")
    if event_key == "dd" and status in {"Closed", "Verified"}:
        actions.append("Reopen")
    return actions


def _assessment_tab_labels(event_key: str, country: str) -> list[str]:
    country = (country or "").upper()
    if event_key == "ssa":
        return ["Details", "Submit Comment", "Review Comment", "Withdraw Comment"]
    if event_key == "dd":
        tabs = ["Details", "Submit Comment", "Review Comment", "Approve Comment"]
        if country in {"SG", "PH"}:
            tabs.append("Risk Committee")
        tabs.append("Withdraw Comment")
        return tabs
    if event_key == "pir":
        return ["Details", "Submit Comment", "Approve Comment", "Withdraw Comment"]
    if event_key == "periodic-review":
        return ["Details", "Submit Comment", "Review Comment", "Withdraw Comment"]
    if event_key == "on-site-visit":
        return ["Details", "Submit Comment", "Approve Comment", "Withdraw Comment"]
    if event_key == "termination":
        tabs = ["Details", "Submit Comment", "Approve Comment"]
        if country == "ID":
            tabs.append("Review Comment")
        tabs.append("Withdraw Comment")
        return tabs
    return ["Details", "Submit Comment"]


def _assessment_tab_key(label: str) -> str:
    return {
        "Details": "details",
        "Submit Comment": "submit",
        "Review Comment": "review",
        "Approve Comment": "approve",
        "Risk Committee": "risk-committee",
        "Withdraw Comment": "withdraw",
    }.get(label, "details")


def _build_outsourcing_assessment_form(arrangement: dict, event_key: str, *, assessment: dict | None = None) -> dict:
    event_specs = {
        "ssa": {
            "label": "Service Supplier Assessment",
            "short_label": "SSA",
            "page_kicker": "F03 / 3.6",
            "summary": "Annual supplier assessment covering control environment, resilience, subcontracting, and exit readiness.",
            "workflow_hint": "SSA supports submit, withdraw, reopen, and Add New DD when ongoing monitoring finds a material change.",
            "timeline": [("Assessment Due Date", "22 Apr 2026"), ("Review Route", "Business Owner → ORM → RC"), ("Latest Approved DD", arrangement["linked_dd"])],
            "sections": [
                ("Assessment Profile", [("Assessment ID", assessment["id"] if assessment else "SSA-SG-2026-0049"), ("Event", "Service Supplier Assessment"), ("Arrangement ID", arrangement["id"]), ("Status", assessment["status"] if assessment else "Draft")]),
                ("Supplier Oversight", [("Subcontracting Declared", arrangement["subcontracting"]), ("Data Storage", arrangement["data_storage"]), ("Exit Plan", arrangement["exit_plan"]), ("Critical Service", arrangement["critical_service"])]),
                ("Control Narrative", [("Operational Resilience", "Annual failover rehearsal completed"), ("Incident / Breach History", "No material incident in the last 12 months"), ("Residual Risk", arrangement["risk_level"]), ("Maker Comment", "Supplier remains suitable with compensating controls in place")]),
            ],
            "document_rows": [("2026_SSA_PaymentSwitch.pdf", "Primary assessment pack"), ("BCM_Evidence.zip", "Resilience and failover evidence")],
            "default_actions": ["Save Draft SSA", "Submit SSA", "Withdraw SSA", "Reopen SSA", "Add New DD"],
        },
        "dd": {
            "label": "Due Diligence",
            "short_label": "DD",
            "page_kicker": "F04 / 3.7",
            "summary": "Event-based DD used when scope changes, new subcontracting appears, or control concerns trigger deeper review.",
            "workflow_hint": "DD includes submit, review, approve, withdraw, and reopen. Risk Committee is surfaced when market rules require extra decisioning.",
            "timeline": [("Assessment Due Date", "05 Jan 2026"), ("Review Route", "Business Owner → ORM → CISO"), ("Risk Committee Trigger", "Residual risk above approved threshold only")],
            "sections": [
                ("DD Snapshot", [("Assessment ID", assessment["id"] if assessment else "DD-SG-2026-0019"), ("Event", "Due Diligence"), ("Arrangement ID", arrangement["id"]), ("Status", assessment["status"] if assessment else "Draft")]),
                ("Vendor Validation", [("Financial Review", "2026 audited statements uploaded"), ("Security Review", "Control evidence refreshed"), ("Subcontractor Review", "One approved subcontractor retained"), ("Risk Committee", "Not required")]),
                ("Decisioning Notes", [("Open Items", "Quarterly privileged access review"), ("Approval Condition", "Annual penetration testing evidence"), ("Residual Risk", arrangement["risk_level"]), ("Maker Comment", "Proceed with compensating controls")]),
            ],
            "document_rows": [("Expanded_DD_Pack.pdf", "DD pack"), ("Vendor_Financials_2026.xlsx", "Audited vendor financials")],
            "default_actions": ["Save Draft DD", "Submit DD", "Withdraw DD", "Reopen DD"],
        },
        "pir": {
            "label": "Post-Implementation Review",
            "short_label": "PIR",
            "page_kicker": "F05 / 3.8",
            "summary": "Post go-live validation that approved design, control intent, and production outcomes match after implementation.",
            "workflow_hint": "PIR focuses on implemented controls, production evidence, and whether downstream register/report state can be updated.",
            "timeline": [("Assessment Due Date", "15 May 2026"), ("Implementation Window", "Mar 2026"), ("Review Route", "Business Owner → ORM")],
            "sections": [
                ("PIR Overview", [("Assessment ID", assessment["id"] if assessment else "PIR-SG-2026-0009"), ("Event", "Post-Implementation Review"), ("Arrangement ID", arrangement["id"]), ("Status", assessment["status"] if assessment else "Draft")]),
                ("Implementation Validation", [("Go-live Checklist", "Completed"), ("Operational KPI", "Stable"), ("Issue Summary", "No critical gap"), ("Downstream Register Update", "Required after approval")]),
                ("Evidence Notes", [("Control Design", "Matches approved DD"), ("Follow-up", "Track quarterly failover cadence"), ("Residual Risk", arrangement["risk_level"]), ("Maker Comment", "Implementation outcome acceptable")]),
            ],
            "document_rows": [("GoLive_Checklist.pdf", "Go-live validation"), ("PIR_Evidence_March2026.zip", "Production evidence pack")],
            "default_actions": ["Save Draft PIR", "Submit PIR", "Withdraw PIR"],
        },
        "periodic-review": {
            "label": "Periodic Review / Contractual Renewal / Change Management",
            "short_label": "Periodic Review",
            "page_kicker": "F02 / 3.5.2",
            "summary": "One governed assessment for annual review, contractual renewal, and material operational or commercial change.",
            "workflow_hint": "This event bundles monitoring results, renewal terms, and change impact into one route instead of separate disconnected tickets.",
            "timeline": [("Assessment Due Date", "28 Apr 2026"), ("Renewal Window", "FY2026"), ("Review Route", "Business Owner → ORM")],
            "sections": [
                ("Renewal Snapshot", [("Assessment ID", assessment["id"] if assessment else "PR-SG-2026-0005"), ("Event", "Periodic Review / Contractual Renewal / Change Management"), ("Arrangement ID", arrangement["id"]), ("Status", assessment["status"] if assessment else "Draft")]),
                ("Renewal and Change", [("Commercial Change", "Indexed SLA pricing"), ("Operational Change", "Weekend support added"), ("BCP Walkthrough", "Pending refreshed evidence"), ("Report Confirmation", "Required for SG / PH report pack")]),
                ("Monitoring Summary", [("SLA Performance", "Within tolerance"), ("Complaint Trend", "Stable"), ("Residual Risk", arrangement["risk_level"]), ("Maker Comment", "Renewal can proceed with minor follow-up")]),
            ],
            "document_rows": [("BPO_Renewal_Pack_v3.pdf", "Renewal package"), ("SLA_Addendum.docx", "Commercial addendum")],
            "default_actions": ["Save Draft Periodic Review", "Submit Periodic Review", "Withdraw Periodic Review"],
        },
        "on-site-visit": {
            "label": "On-site Visit",
            "short_label": "On-site Visit",
            "page_kicker": "F06 / 3.9",
            "summary": "Physical visit flow covering access controls, staffing, evidence walk-through, and site issue tracking.",
            "workflow_hint": "On-site Visit keeps visit plan, evidence capture, and approver comments together so the outcome is reusable in governance tracking.",
            "timeline": [("Visit Date", "06 Jun 2026"), ("Site", "FinStream SG operations center"), ("Review Route", "Business Owner → ORM → CISO")],
            "sections": [
                ("Visit Plan", [("Assessment ID", assessment["id"] if assessment else "OSV-SG-2026-0004"), ("Event", "On-site Visit"), ("Arrangement ID", arrangement["id"]), ("Status", assessment["status"] if assessment else "Draft")]),
                ("Walkthrough Coverage", [("Physical Security", "Badge, CCTV, visitor logging"), ("People Review", "Shift coverage validated"), ("BCP Evidence", "Hot-site walkthrough complete"), ("Issue Summary", "1 medium issue open")]),
                ("Follow-up Notes", [("Target Closure", "30 Jun 2026"), ("Escalation Required", "No"), ("Residual Risk", arrangement["risk_level"]), ("Maker Comment", "Site controls broadly effective")]),
            ],
            "document_rows": [("Visit_Agenda.pdf", "Visit agenda"), ("Physical_Security_Photos.zip", "Walkthrough photo evidence")],
            "default_actions": ["Save Draft On-site Visit", "Submit On-site Visit", "Withdraw On-site Visit"],
        },
        "termination": {
            "label": "Termination",
            "short_label": "Termination",
            "page_kicker": "F07 / 3.10",
            "summary": "Exit governance flow covering data return, access revocation, replacement readiness, and withdrawal handling when termination is reversed.",
            "workflow_hint": "Termination keeps exit checklist, withdrawal reason, and terminal export/report impact in one governed page.",
            "timeline": [("Target Exit Date", "30 Apr 2026"), ("Approval Route", "Business Owner → CISO"), ("Report Impact", "Termination withdrawal report available after terminal decision")],
            "sections": [
                ("Termination Request", [("Assessment ID", assessment["id"] if assessment else "TERM-ID-2026-0003"), ("Event", "Termination"), ("Arrangement ID", arrangement["id"]), ("Status", assessment["status"] if assessment else "Draft")]),
                ("Exit Readiness", [("Data Return", "Checklist in progress"), ("Access Revocation", "Draft checklist"), ("Replacement Vendor", "Not required"), ("Withdrawal Requested", "No")]),
                ("Closure Notes", [("Exit Owner", arrangement["owner"]), ("Residual Risk", arrangement["risk_level"]), ("Downstream Export", "Termination Withdrawal Report"), ("Maker Comment", "Exit controls aligned to scope")]),
            ],
            "document_rows": [("Termination_Request.docx", "Termination or withdrawal request"), ("Access_Revocation_Checklist.xlsx", "Exit checklist")],
            "default_actions": ["Save Draft Termination", "Submit Termination", "Withdraw Termination"],
        },
    }
    spec = event_specs.get(event_key, event_specs["ssa"])
    mode = "Edit" if assessment else "Add New"
    form_assessment = assessment.copy() if assessment else _default_assessment_for_event(arrangement, event_key)
    tab_labels = _assessment_tab_labels(event_key, arrangement["country"])
    comment_payload = _build_assessment_comment_payload(event_key, arrangement, form_assessment)
    return {
        "event_key": event_key,
        "mode": mode,
        "page_kicker": spec["page_kicker"],
        "title": f"{mode} {spec['label']}",
        "summary": spec["summary"],
        "workflow_hint": spec["workflow_hint"],
        "timeline": spec["timeline"],
        "sections": _build_assessment_detail_sections(event_key, arrangement, form_assessment),
        "document_rows": spec["document_rows"],
        "tab_labels": tab_labels,
        "action_buttons": (assessment.get("workflow_actions") if assessment else spec["default_actions"]),
        "status": form_assessment["status"],
        "assessment_id": form_assessment["id"],
        "submit_comment_rows": comment_payload["submit_comment_rows"],
        "review_comment_rows": comment_payload["review_comment_rows"],
        "approve_comment_rows": comment_payload["approve_comment_rows"],
        "withdraw_comment_rows": comment_payload["withdraw_comment_rows"],
        "review_condition_rows": comment_payload["review_condition_rows"],
        "risk_committee_view": comment_payload["risk_committee_view"],
    }


def _find_approval_row(auth_id: str) -> tuple[str, str, str, str, str, str] | None:
    approval_rows = [
        ("20260413000001", "Incident - Authorize Edit Email Management", "EML-0005", "Pending", "Operational Risk Management", "13-04-2026 10:32"),
        ("20260413000002", "Incident - Reopen Incident", "IM-SG-2026-00401", "Pending", "Customer Service", "13-04-2026 11:10"),
        ("20260413000003", "Incident - Complete Action Plan", "AP-20260413-1", "Pending", "Payments Operations", "13-04-2026 11:48"),
        ("20260419000004", "Outsourcing - Authorize New Assessment", "SSA-SG-2026-0042", "Pending", "Operations Technology", "19-04-2026 10:45"),
        ("20260419000005", "Outsourcing - Authorize Withdraw Assessment", "TERM-ID-2026-0002", "Pending", "Information Security", "18-04-2026 19:15"),
    ]
    for row in approval_rows:
        if row[0] == auth_id:
            return row
    return None


def _current_google_email() -> str:
    profile = session.get("google_profile") or {}
    return str(profile.get("email") or "").strip().lower()


def create_app(
    *,
    owner_email: str = "xiaodong.zheng@npt.sg",
    secret_key: str | None = None,
) -> Flask:
    app = Flask(
        __name__,
        template_folder="templates",
        static_folder="static",
    )
    if secret_key:
        app.config["SECRET_KEY"] = secret_key

    @app.context_processor
    def inject_navigation():
        current_path = request.path or "/"
        return {
            "site_tabs": [
                {
                    "label": "BPMIS Automation Tool",
                    "href": "/",
                    "active": False,
                },
                {
                    "label": "PRD Briefing Tool",
                    "href": "/prd-briefing/",
                    "active": False,
                },
                {
                    "label": "FE Demo",
                    "href": "/grc-demo/outsourcing-management",
                    "active": True,
                },
                {
                    "label": "Gmail & SeaTalk Demo",
                    "href": "/gmail-sea-talk-demo",
                    "active": False,
                },
            ],
            "nav_items": [
                {"label": "Outsourcing Management", "endpoint": "outsourcing_overview"},
                {"label": "GRC Access Control", "endpoint": "access_control"},
                {"label": "Incident Overview", "endpoint": "overview"},
                {"label": "Add Incident", "endpoint": "add_incident"},
                {"label": "Authorization Management", "endpoint": "authorization"},
                {"label": "Parameter Management", "endpoint": "parameter_management"},
                {"label": "Email Management", "endpoint": "email_management"},
                {"label": "Report", "endpoint": "reports"},
            ],
        }

    @app.before_request
    def enforce_owner_access():
        email = _current_google_email()
        if not email or email != owner_email.strip().lower():
            return redirect("/")

    @app.get("/")
    def root():
        return redirect(url_for("outsourcing_overview"))

    @app.get("/overview")
    def overview():
        return render_template(
            "overview.html",
            page_title="Incident Overview",
            page_kicker="SG Version",
            incidents=INCIDENTS,
        )

    @app.get("/outsourcing-management")
    def outsourcing_overview():
        return render_template(
            "outsourcing_overview.html",
            page_title="Outsourcing Management",
            page_kicker="F01 / 3.4",
            overview_rows=_build_outsourcing_overview_rows(),
        )

    @app.get("/outsourcing-management/new")
    def outsourcing_new():
        draft = {
            "assessment_id": "Generated upon first Save",
            "event": "Onboarding of New Arrangement",
            "status": "Draft",
            "date_of_request": "2026-04-19",
            "arrangement_id": "Generated upon first Save",
            "contracting_entity": "SeaBank Singapore",
            "business_self_assessment_id": "BSA-2026-0041",
            "functional_units": "Treasury Operations, Shared Services",
            "functional_unit_owner": "Heads of Treasury Operations, Head of Shared Services",
            "main_pic": "rene.chong@npt.sg",
            "co_pic": "melissa.tan@npt.sg",
            "service_provider_name": "NorthStar Backoffice Services Pte Ltd",
            "business_registration_number": "201912345K",
            "country_of_registration": "SG - Singapore",
            "category_of_provider": "Service Provider",
            "service_provider_parent_name": "",
            "service_scope": "Daily treasury reconciliation support, exception investigation, and break resolution workflow between SeaBank and the vendor operations team.",
            "it_scope": "Vendor accesses exception dashboards and secure file exchange during BAU support. No production write access is granted after go live.",
            "outsourcing_categorization": "Material Outsourced Relevant Service",
            "supporting_documents": [
                ("NorthStar_Proposal.pdf", "Commercial and service scope pack"),
                ("NorthStar_Information_Security_Response.xlsx", "Initial onboarding due diligence response"),
            ],
        }
        return render_template(
            "outsourcing_new.html",
            page_title="Add New Outsourcing Arrangement",
            page_kicker="F01 / 3.4.2",
            draft=draft,
        )

    @app.get("/outsourcing-management/<arrangement_id>")
    def outsourcing_detail(arrangement_id: str):
        arrangement = _find_arrangement(arrangement_id)
        if arrangement is None:
            abort(404)
        assessments = [item for item in OUTSOURCING_ASSESSMENTS if item["arrangement_id"] == arrangement_id]
        arrangement_header_items = [
            ("Outsourcing Arrangement Identifier", arrangement["id"]),
            ("Outsourcing Arrangement Status", arrangement["status"]),
            ("Withdrawal Date", arrangement["withdrawal_date"]),
            ("Termination Date", arrangement["termination_date"]),
        ]
        launch_events = [
            {"key": "periodic-review", "label": "Periodic Review / Contractual Renewal / Change Management", "status": "Pending exists" if any(item["event"] == "Periodic Review / Contractual Renewal / Change Management" and "Pending" in item["status"] for item in assessments) else "Ready", "note": "Only one pending assessment can exist for the same event at a time."},
            {"key": "ssa", "label": "Service Supplier Assessment (SSA)", "status": "Pending exists" if any(item["short_label"] == "SSA" and "Pending" in item["status"] for item in assessments) else "Ready", "note": "Annual supplier assessment with control, performance, and subcontracting review."},
            {"key": "dd", "label": "Due Diligence (DD)", "status": "Ready", "note": "Launch when there is scope change, renewal risk uplift, or control concern."},
            {"key": "pir", "label": "Post-Implementation Review (PIR)", "status": "Draft exists" if any(item["short_label"] == "PIR" for item in assessments) else "Ready", "note": "Post go-live validation for new service or major implementation."},
            {"key": "on-site-visit", "label": "On-site Visit", "status": "Pending exists" if any(item["event"] == "On-site Visit" and "Pending" in item["status"] for item in assessments) else "Ready", "note": "Use for physical site validation and evidence walkthrough."},
            {"key": "termination", "label": "Termination", "status": "Pending exists" if any(item["event"] == "Termination" and "Pending" in item["status"] for item in assessments) else "Ready", "note": "Exit governance, data return, access revocation, and replacement readiness."},
        ]
        assessment_rows = [
            {
                "id": assessment["id"],
                "arrangement_id": arrangement["id"],
                "full_name": arrangement["full_name"],
                "description_of_services": arrangement["description_of_services"],
                "category_of_provider": arrangement["category_of_provider"],
                "first_commencement": arrangement["first_commencement"],
                "actions": (
                    [
                        {
                            "label": "Edit",
                            "href": url_for(
                                "outsourcing_assessment_edit",
                                arrangement_id=arrangement["id"],
                                assessment_id=assessment["id"],
                            ),
                        }
                    ]
                    if arrangement["status"] == "Active"
                    else []
                ),
            }
            for assessment in assessments
        ]
        can_manage_assessments = arrangement["status"] == "Active"
        return render_template(
            "outsourcing_detail.html",
            page_title=arrangement["id"],
            page_kicker="F02 / 3.5",
            arrangement=arrangement,
            arrangement_header_items=arrangement_header_items,
            launch_events=launch_events,
            assessment_rows=assessment_rows,
            can_manage_assessments=can_manage_assessments,
        )

    @app.get("/outsourcing-management/<arrangement_id>/assessment/new")
    def outsourcing_assessment_new(arrangement_id: str):
        arrangement = _find_arrangement(arrangement_id)
        if arrangement is None:
            abort(404)
        event_key = request.args.get("event", "ssa")
        form = _build_outsourcing_assessment_form(arrangement, event_key)
        return render_template(
            "outsourcing_assessment_form.html",
            page_title=form["title"],
            page_kicker=form["page_kicker"],
            arrangement=arrangement,
            form=form,
        )

    @app.get("/outsourcing-management/<arrangement_id>/assessment/<assessment_id>/edit")
    def outsourcing_assessment_edit(arrangement_id: str, assessment_id: str):
        arrangement = _find_arrangement(arrangement_id)
        assessment = _find_assessment(assessment_id)
        if arrangement is None or assessment is None or assessment["arrangement_id"] != arrangement_id:
            abort(404)
        form = _build_outsourcing_assessment_form(arrangement, _assessment_event_key(assessment), assessment=assessment)
        return render_template(
            "outsourcing_assessment_form.html",
            page_title=form["title"],
            page_kicker=form["page_kicker"],
            arrangement=arrangement,
            assessment=assessment,
            form=form,
        )

    @app.get("/outsourcing-management/<arrangement_id>/assessment/<assessment_id>")
    def outsourcing_assessment_detail(arrangement_id: str, assessment_id: str):
        arrangement = _find_arrangement(arrangement_id)
        assessment = _find_assessment(assessment_id)
        if arrangement is None or assessment is None or assessment["arrangement_id"] != arrangement_id:
            abort(404)
        event_key = _assessment_event_key(assessment)
        tab_labels = _assessment_tab_labels(event_key, arrangement["country"])
        tab_keys = {label: _assessment_tab_key(label) for label in tab_labels}
        active_tab = request.args.get("tab", request.args.get("comment_tab", "details"))
        valid_tabs = set(tab_keys.values())
        if active_tab not in valid_tabs:
            active_tab = "details"
        comment_guidance = assessment.get("comment_guidance") or {}
        detail_sections = _build_assessment_detail_sections(event_key, arrangement, assessment)
        comment_payload = _build_assessment_comment_payload(event_key, arrangement, assessment)
        activity_rows = _outsourcing_activity_rows(assessment)
        return render_template(
            "outsourcing_assessment_detail.html",
            page_title=assessment["id"],
            page_kicker=f"{assessment['short_label']} / {assessment['event']}",
            arrangement=arrangement,
            assessment=assessment,
            assessment_action_buttons=_assessment_detail_actions(arrangement, assessment),
            assessment_tabs=tab_labels,
            comment_tab_keys=tab_keys,
            active_assessment_tab=active_tab,
            comment_guidance=comment_guidance,
            detail_sections=detail_sections,
            submit_comment_rows=comment_payload["submit_comment_rows"],
            review_comment_rows=comment_payload["review_comment_rows"],
            approve_comment_rows=comment_payload["approve_comment_rows"],
            withdraw_comment_rows=comment_payload["withdraw_comment_rows"],
            review_condition_rows=comment_payload["review_condition_rows"],
            activity_rows=activity_rows,
            risk_committee_view=comment_payload["risk_committee_view"],
        )

    @app.get("/access-control")
    def access_control():
        role_permissions = [
            ("Incident Management Overview", "View", "View information and download report"),
            ("Incident Management Overview", "Edit", "Add new incident and launch edit flow"),
            ("Incident Management Details Tab", "View", "View incident details and documents"),
            ("Incident Management Details Tab", "Edit", "Edit details, withdraw, reopen"),
            ("Incident Management Action Plan Tab", "Edit", "Add, edit, withdraw, complete action plan"),
            ("Incident Management Operational Loss Tab", "Edit", "Edit operational loss"),
            ("Incident Management Review Comment Tab", "View / Edit / Reply", "SG supports RC edit and review replies"),
            ("Incident Management Comment Tab", "View / Comment", "Shared comment section"),
            ("Incident Management History Tab", "View", "View history"),
            ("Report", "View", "Download report"),
            ("Parameter Management", "View / Edit", "Manage incident parameters"),
            ("Email Management", "View / Edit", "Manage incident emails"),
        ]
        outsourcing_role_permissions = [
            ("Outsourcing Management Overview", "View", "Overview - View permission for arrangement and assessment search results."),
            ("Outsourcing Management Overview", "Add", "Overview - Add permission for Add New Outsourcing Arrangement."),
            ("Outsourcing Management Details", "View", "Details - View permission for arrangement header and linked assessment list."),
            ("Outsourcing Management Details", "Edit", "Details - Edit permission for withdraw arrangement and assessment actions while active."),
            ("Outsourcing Assessment", "View / Edit", "Create, edit, submit, withdraw, reopen, and Add New DD based on routed ownership."),
            ("Authorization Management", "View", "In-process authorization visibility based on Maker / Reviewer / Approver rules."),
            ("Report", "View / Export", "Generate XLSX reports based on functional unit data access control."),
        ]
        org_rows = [
            ("Operations", "Payments Operations", "Active", "Approver: FU HOD / Checker: ORM"),
            ("Operations", "Card Operations", "Active", "Approver: FU HOD / Checker: ORM"),
            ("Technology", "Operations Technology", "Active", "Approver: Tech Lead / Checker: ORM"),
            ("Risk", "Operational Risk Management", "Active", "Approver: ORM Lead / Checker: RC"),
        ]
        reviewer_rows = [
            ("ORM", "ROLE-102 / Operational Risk Management Lead", "Non-editable reviewer group"),
            ("Legal", "ROLE-143 / Legal Counsel", "Non-editable reviewer group"),
            ("RC", "ROLE-188 / Regulatory Compliance Lead", "Non-editable reviewer group"),
            ("CISO", "ROLE-173 / Information Security Lead", "Non-editable reviewer group"),
        ]
        outsourcing_access_rows = [
            ("Operations Technology", "Operations Technology Lead", "ORM Checker", "Overview / Details / Assessment", "Create and submit SSA, DD, PIR, On-site Visit", "Own arrangements tagged to Operations Technology only"),
            ("Customer Service", "Customer Service Lead", "ORM Checker", "Overview / Details / Assessment", "Create periodic review and contractual renewal assessments", "Own outsourcing arrangements and linked approvals"),
            ("Operational Risk Management", "ORM Lead", "RC Checker", "Authorization / Report", "Review, approve, confirm report, export XLSX", "Cross-FU visibility for tagged in-process outsourcing authorizations"),
            ("Information Security", "CISO Delegate", "ORM Checker", "Assessment / Authorization", "Approve On-site Visit and Termination workflows", "Visible only when event route includes CISO"),
        ]
        approval_center = [
            ("Organization Structure", "Add / Edit Organization Structure"),
            ("Reviewer List", "Add / Edit Reviewer List"),
            ("Email Management", "Authorize Edit Email Management"),
            ("Incident Management", "Withdraw / Reopen / Complete AP / Edit AP"),
            ("Outsourcing Management", "Authorize New Assessment / Withdraw Assessment / Report Confirmation"),
        ]
        return render_template(
            "access_control.html",
            page_title="GRC Access Control",
            page_kicker="Incident Management Support Setup",
            role_permissions=role_permissions,
            outsourcing_role_permissions=outsourcing_role_permissions,
            org_rows=org_rows,
            reviewer_rows=reviewer_rows,
            outsourcing_access_rows=outsourcing_access_rows,
            approval_center=approval_center,
        )

    @app.get("/access-control/organization")
    def access_control_organization():
        org_rows = [
            ("Regulatory Compliance", "Compliance", "Active"),
            ("Finance", "Finance Operations", "Active"),
            ("Technology", "IT PMO", "Active"),
            ("Technology", "Loan PM", "Inactive"),
            ("Risk Management", "ORM", "Active"),
            ("Technology", "Payment PM", "Active"),
            ("Risk Management", "Retail Risk", "Active"),
            ("Risk Management", "Risk Data", "Active"),
            ("Risk Management", "Risk Operations", "Active"),
            ("Technology", "Risk PM", "Inactive"),
        ]
        return render_template(
            "access_control_organization.html",
            page_title="Organization Structure",
            page_kicker="GRC Access Control",
            org_rows=org_rows,
        )

    @app.get("/access-control/data-access")
    def access_control_data_access():
        rows = [
            ("Compliance", "RC Team Lead", "RC Checker", "ORM Team Member", "RC Team Member", "RC Team Member", "RC Team Member", "RC Team Member"),
            ("Finance Operations", "Fin Team Lead", "Fin Checker", "Fin Team Member", "Fin Team Member", "Fin Team Member", "Fin Team Member", "Fin Team Member"),
            ("IT PMO", "PMO Team Lead", "PMO Checker", "PMO Team Member", "PMO Team Member", "PMO Team Member", "PMO Team Member", "PMO Team Member"),
            ("Loan PM", "Loan Team Lead", "Loan Checker", "Loan Team Member", "Loan Team Member", "Loan Team Member", "Loan Team Member", "Loan Team Member"),
            ("ORM", "ORM Team Lead", "ORM Checker", "ORM Team Member", "ORM Team Member", "ORM Team Member", "ORM Team Member", "ORM Team Member"),
            ("Payment PM", "Pay Team Lead", "Pay Checker", "Pay Team Member", "Pay Team Member", "Pay Team Member", "Pay Team Member", "Pay Team Member"),
            ("Retail Risk", "Risk Team Lead", "Risk Checker", "Risk Team Member", "Risk Team Member", "Risk Team Member", "Risk Team Member", "Risk Team Member"),
            ("Risk Data", "Risk Team Lead", "Risk Checker", "Risk Team Member", "Risk Team Member", "Risk Team Member", "Risk Team Member", "Risk Team Member"),
            ("Risk Operations", "RiskTeam Lead", "Risk Checker", "Risk Team Member", "Risk Team Member", "Risk Team Member", "Risk Team Member", "Risk Team Member"),
            ("Risk PM", "Tech Team Lead", "Tech Checker", "Tech Team Member", "Tech Team Member", "Tech Team Member", "Tech Team Member", "Tech Team Member"),
        ]
        return render_template(
            "access_control_data_access.html",
            page_title="Data Access Control",
            page_kicker="GRC Access Control",
            rows=rows,
        )

    @app.get("/access-control/reviewer-list")
    def access_control_reviewer_list():
        rows = [
            ("CISO", ["CISO Team Lead"]),
            ("CRO", ["Risk Team Lead"]),
            ("IT PMO", ["IT PMO Team Lead"]),
            ("Legal", ["Legal Team Lead"]),
            ("ORM", ["ORM Team Lead"]),
            ("Payment PM", ["Payment Team Lead"]),
            ("RC", ["AML Team Lead", "RC Team Lead", "Anti Fraud Team Lead"]),
            ("Risk Data", ["Risk Data Team Lead"]),
            ("Risk Operations", ["Risk Ops Team Lead"]),
            ("Risk PM", ["Risk Team Lead"]),
        ]
        return render_template(
            "access_control_reviewer_list.html",
            page_title="Reviewer List",
            page_kicker="GRC Access Control",
            rows=rows,
        )

    @app.get("/approval-center")
    def approval_center():
        rows = [
            ("20260413000001", "Incident - Authorize Edit Email Management", "EML-0005", "Pending", "Operational Risk Management", "13-04-2026 10:32"),
            ("20260413000002", "Incident - Reopen Incident", "IM-SG-2026-00401", "Pending", "Customer Service", "13-04-2026 11:10"),
            ("20260413000003", "Incident - Complete Action Plan", "AP-20260413-1", "Pending", "Payments Operations", "13-04-2026 11:48"),
            ("20260419000004", "Outsourcing - Authorize New Assessment", "SSA-SG-2026-0042", "Pending", "Operations Technology", "19-04-2026 10:45"),
            ("20260419000005", "Outsourcing - Authorize Withdraw Assessment", "TERM-ID-2026-0002", "Pending", "Information Security", "18-04-2026 19:15"),
        ]
        return render_template(
            "approval_center.html",
            page_title="Approval Center",
            page_kicker="Incident-Related Authorizations",
            rows=rows,
        )

    @app.get("/approval-center/<auth_id>")
    def approval_detail(auth_id: str):
        approval_row = _find_approval_row(auth_id)
        auth_type = approval_row[1] if approval_row else "Incident - Authorize New Incident"
        event_id = approval_row[2] if approval_row else "ORI00001"
        is_outsourcing_auth = event_id.startswith(("SSA", "DD", "PIR", "PR-", "OSV", "TERM"))
        linked_assessment = _find_assessment(event_id) if is_outsourcing_auth else None
        review_comment_rows = [
            ("ORM", "Cui Qiang", "Approved", "NA", "NA", "Kindly ensure the action plans are completed on time.", "Comment (10 replies)"),
            ("Legal", "BBB", "Rejected", "NA", "NA", "Kindly ensure the no data breach.", "Comment (0 replies)"),
            ("CISO", "AAA", "Approved", "NA", "NA", "OK!", "Comment (0 replies)"),
            ("RC", "CCC", "NA", "Yes", "Yes", "Done reported top MAS.", "Comment (2 replies)"),
        ]
        outsourcing_detail_rows = []
        approval_tabs = ["Authorization Details"]
        if linked_assessment is not None:
            review_comment_rows = [
                ("Business Owner", "Rene Chong", "Approved", "Assessment scope and renewal rationale are acceptable.", "Comment (3 replies)"),
                ("ORM", "Cui Qiang", "Approved", "Residual risk remains within tolerance with compensating controls.", "Comment (1 reply)"),
                ("RC", "Cheryl Tan", "Pending", "Waiting for confirmation whether regulator notification is triggered.", "Comment (0 replies)"),
            ]
            outsourcing_detail_rows = [
                ("Authorization Type", auth_type),
                ("Assessment ID", linked_assessment["id"]),
                ("Event", linked_assessment["event"]),
                ("Arrangement ID", linked_assessment["arrangement_id"]),
                ("Authorization Route", linked_assessment["route"]),
                ("Supporting Documents", ", ".join(linked_assessment.get("supporting_documents") or [])),
            ]
            approval_tabs = ["Authorization Details", "Assessment Snapshot", "Comment Tabs"]
        return render_template(
            "approval_detail.html",
            page_title=auth_id,
            page_kicker="Approval Detail",
            auth_id=auth_id,
            auth_type=auth_type,
            event_id=event_id,
            is_outsourcing_auth=is_outsourcing_auth,
            linked_assessment=linked_assessment,
            outsourcing_detail_rows=outsourcing_detail_rows,
            approval_tabs=approval_tabs,
            review_comment_rows=review_comment_rows,
        )

    @app.get("/incident/new")
    def add_incident():
        active_tab = request.args.get("tab", "details")
        if active_tab not in {"details", "action-plan", "operational-loss"}:
            active_tab = "details"

        incident_type = "Operational Risk Incident"
        key_detail_rows = [
            [
                {"label": "Incident ID", "value": "ORI00001", "kind": "input", "required": True},
            ],
            [
                {"label": "Incident Title", "value": "Critical Server Downtime", "kind": "input", "required": True, "span": 3},
            ],
            [
                {"label": "Incident Occurred Date", "value": "04-04-2025", "kind": "date", "required": True},
                {"label": "Detection Date", "value": "04-04-2025", "kind": "date", "required": True},
                {"label": "Near Miss", "value": "Yes", "kind": "select", "required": True},
            ],
            [
                {"label": "Creation Date", "value": "06-04-2025", "kind": "date", "required": True},
                {"label": "Created within Timeline", "value": "Yes", "kind": "select", "required": True},
                {"label": "Justification for Late Lodgement", "value": "", "kind": "input"},
            ],
            [
                {"label": "Closure Date", "value": "", "kind": "date"},
                {"label": "Closed within Timeline", "value": "", "kind": "select"},
                {"label": "Justification for Late Closure", "value": "", "kind": "input"},
            ],
            [
                {"label": "Reopen Date", "value": "", "kind": "date"},
                {"label": "Reopen Justification", "value": "", "kind": "input", "span": 2},
            ],
            [
                {"label": "Withdrawal Date", "value": "", "kind": "date"},
                {"label": "Withdrawal Justification", "value": "", "kind": "input", "span": 2},
            ],
            [
                {"label": "Personal Data Involved", "value": "Yes", "kind": "select", "required": True},
            ],
            [
                {"label": "Incident Summary", "value": "At approximately 10:15 AM UTC, monitoring systems detected a complete service outage across the production server cluster in the Singapore region. The downtime affected all external-facing web services and APIs, resulting in disrupted user access, failed transactions, and interrupted integrations.", "kind": "textarea", "required": True, "span": 3},
            ],
            [
                {"label": "Root Cause", "value": "An underlying hardware failure in the primary storage node triggered a cascading failure in the cluster, leading to loss of quorum and service unavailability. A secondary issue in the automated failover mechanism delayed service recovery.", "kind": "textarea", "required": True, "span": 3},
            ],
            [
                {"label": "Parties Involved", "value": "Site Reliability Engineering (SRE), Infrastructure/DevOps, Cloud Provider Support, Application Engineering, IT Operations, Incident Commander, Customer Support", "kind": "textarea", "required": True, "span": 3},
            ],
            [
                {"label": "Sequence of Events", "value": "15-05-2025 - Manual restart of affected nodes initiated by SRE and Infrastructure teams.\n16-05-2025 - Services began coming online in a staggered sequence to prevent overload.\n20-05-2025 - Core services (web frontend, authentication, and APIs) reached partial operational state.\n22-05-2025 - Manual cluster rebalancing completed to ensure even load distribution.\n24-05-2025 - Automated health checks passed for all critical services.\n27-05-2025 - Manual verification began.", "kind": "textarea", "required": True, "span": 3},
            ],
        ]
        impact_header_fields = [
            {"label": "Incident Impact Classification", "value": "Significant", "kind": "select", "required": True},
            {"label": "Financial Impact", "value": "Significant", "kind": "select", "required": True},
            {"label": "Non-Financial Impact", "value": "Significant", "kind": "select", "required": True},
            {"label": "Potential Loss", "value": "$", "kind": "input"},
            {"label": "Gross Loss", "value": "$ 500,000", "kind": "input", "required": True},
            {"label": "Recovered Amount", "value": "$ 450,000", "kind": "input", "required": True},
            {"label": "Net Financial Impact", "value": "$ 50,000", "kind": "input", "required": True},
        ]
        non_financial_rows = [
            ("Client Impact", "Moderate", "Clients experienced full service disruption due to the unavailability of core systems."),
            ("Business or IT Operations Impact", "Significant", "Delaying service delivery and incident response."),
            ("Legal & Regulatory Impact", "Moderate", "May require regulatory reporting and client notification under compliance obligations."),
            ("Reputational Impact", "Minimal", "The downtime affected customer trust and may impact client satisfaction and future business relationship."),
        ]
        functional_unit_fields = [
            {"label": "Functional Unit", "value": "SG - Risk Operations", "kind": "select", "required": True},
            {"label": "Incident Report Submitter", "value": "Rene Chong", "kind": "input", "required": True},
            {"label": "Incident Report Owner", "value": "Sukie Liu", "kind": "input", "required": True},
            {"label": "Approval Date", "value": "", "kind": "input"},
            {"label": "Business Line Classification", "value": "", "kind": "input", "span": 2},
        ]
        additional_info_fields = [
            {"label": "CRC / IT Incident ID", "value": "INC-20250408-CRIT-001", "kind": "input"},
            {"label": "Post Closure Remediation", "value": "", "kind": "select"},
            {"label": "Risk Taxonomy (Basel L3)", "value": "6.6.1 Business disruption and business execution", "kind": "input", "required": True},
            {"label": "Risk Taxonomy Description (Basel L3)", "value": "Interruption or disruption of the availability of business products, execution of business processes and execution of business activities.", "kind": "input", "span": 2},
            {"label": "Is This Due to Failure of an Outsourced Service Provider and/or their sub-contractors?", "value": "No", "kind": "select", "required": True, "span": 2},
            {"label": "Name of Outsourced Service Provider and/or Sub-contractor", "value": "N/A", "kind": "input"},
            {"label": "Timeline for Notification to IET", "value": "1 Calendar Day(s)", "kind": "select", "required": True},
            {"label": "Notification to IET Completed", "value": "Yes", "kind": "select", "required": True},
            {"label": "Date of Notification to IET", "value": "07-04-2025", "kind": "date", "required": True},
            {"label": "Notification Within Timeline", "value": "Yes", "kind": "select", "required": True},
            {"label": "Additional Info", "value": "", "kind": "textarea", "span": 3},
        ]
        new_action_plans = [
            {"id": "AP00001", "title": "Manual restart and rebalance of affected nodes. This included bringing services back in a staged sequence.", "status": "In Progress", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "Yes", "reviewer": "ORM"},
            {"id": "AP00002", "title": "Strengthen failover validation and automate quorum safeguard before release.", "status": "Withdrawn", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "No", "reviewer": "ORM"},
            {"id": "AP00003", "title": "Review incident communication workflow and escalation matrix for RC cases.", "status": "Close", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "Yes", "reviewer": "RC"},
        ]
        action_plan_modal = {
            "id": "AP00002",
            "status": "In Progress",
            "plan": "Restoration of services verified through automated and manual checks. The team performed additional manual verifications, including API endpoint testing, user journey validation, and database integrity checks, to ensure that all systems were functioning as expected before declaring full recovery.",
            "owner": "Rene Chong",
            "department": "SG - Risk Operations",
            "reviewer": "ORM",
            "validation_date": "",
            "target_date": "31-05-2025",
            "completion_date": "",
            "overdue": "Yes",
            "extended_date": "",
            "board_date": "",
            "status_update": "15-05-2025 - Manual restart of affected nodes initiated by SRE and Infrastructure teams.\n16-05-2025 - Services began coming online in a staggered sequence to prevent overload.\n20-05-2025 - Core services (web frontend, authentication, and APIs) reached partial operational state.\n22-05-2025 - Manual cluster rebalancing completed to ensure even load distribution.\n24-05-2025 - Automated health checks passed for all critical services.\n27-05-2025 - Manual verification began.",
            "documents": ["PDI Assessment Form.pdf", "Picture from customer.jpg", "PDI Assessment Form.pdf", "Picture from customer.jpg"],
        }
        op_loss_fields = [
            {"label": "Operational Loss ID", "value": "OL00001", "kind": "input", "required": True},
            {"label": "Posting Required", "value": "", "kind": "select"},
            {"label": "Accounting date", "value": "", "kind": "date"},
            {"label": "Booking Amount", "value": "SGD", "kind": "input"},
            {"label": "Expenses GL", "value": "", "kind": "input"},
            {"label": "Loss Provision", "value": "SGD", "kind": "input"},
            {"label": "Loss Provision GL", "value": "", "kind": "input"},
            {"label": "Finance Sign Off By", "value": "", "kind": "input"},
            {"label": "Finance Sign Off Date", "value": "", "kind": "date"},
            {"label": "Remarks", "value": "", "kind": "textarea", "span": 4},
        ]
        recovery_rows = []
        return render_template(
            "add_incident.html",
            page_title="Add New Incident",
            page_kicker="F01 / 3.4.2",
            active_tab=active_tab,
            incident_type=incident_type,
            key_detail_rows=key_detail_rows,
            impact_header_fields=impact_header_fields,
            non_financial_rows=non_financial_rows,
            functional_unit_fields=functional_unit_fields,
            additional_info_fields=additional_info_fields,
            new_action_plans=new_action_plans,
            action_plan_modal=action_plan_modal,
            op_loss_fields=op_loss_fields,
            recovery_rows=recovery_rows,
        )

    @app.get("/incident/<incident_id>/edit")
    def edit_incident(incident_id: str):
        incident = _find_incident(incident_id)
        if not incident:
            abort(404)

        active_tab = request.args.get("tab", "details")
        if active_tab not in {"details", "action-plan", "operational-loss"}:
            active_tab = "details"

        editable_fields = [
            {"label": "Incident Summary", "value": "Critical production server downtime affecting customer-facing services and internal operations.", "kind": "textarea", "span": 3},
            {"label": "Root Cause", "value": "Storage-node failure cascaded through the cluster and delayed failover automation.", "kind": "textarea", "span": 3},
            {"label": "Sequence of Events", "value": "Recovery actions were taken in staged sequence after the incident bridge was activated.", "kind": "textarea", "span": 3},
        ]
        edit_action_plans = [
            {"id": "AP00001", "title": "Rollback queue transformer release", "status": "Completed - Pending Review", "rule": "Non-editable"},
            {"id": "AP00002", "title": "Perform root cause analysis and control gap review", "status": "In Progress", "rule": "Only Status Update and Supporting Document editable"},
            {"id": "AP00003", "title": "Update customer operations fallback script", "status": "Edit - Pending Approval", "rule": "Only Status Update and Supporting Document editable"},
        ]
        edit_op_loss_fields = [
            {"label": "Operational Loss ID", "value": "OL00001", "kind": "input", "required": True},
            {"label": "Posting Required", "value": "", "kind": "select"},
            {"label": "Accounting date", "value": "", "kind": "date"},
            {"label": "Booking Amount", "value": "SGD", "kind": "input"},
            {"label": "Expenses GL", "value": "", "kind": "input"},
            {"label": "Loss Provision", "value": "SGD", "kind": "input"},
            {"label": "Loss Provision GL", "value": "", "kind": "input"},
            {"label": "Finance Sign Off By", "value": "", "kind": "input"},
            {"label": "Finance Sign Off Date", "value": "", "kind": "date"},
            {"label": "Remarks", "value": "", "kind": "textarea", "span": 4},
        ]
        edit_recovery_rows = []
        return render_template(
            "edit_incident.html",
            page_title=f"Edit {incident['id']}",
            page_kicker="F01 / 3.4.3",
            incident=incident,
            active_tab=active_tab,
            editable_fields=editable_fields,
            edit_action_plans=edit_action_plans,
            edit_op_loss_fields=edit_op_loss_fields,
            edit_recovery_rows=edit_recovery_rows,
        )

    @app.get("/incident/<incident_id>")
    def incident_detail(incident_id: str):
        incident = _find_incident(incident_id)
        if not incident:
            abort(404)

        active_tab = request.args.get("tab", "details")
        allowed_tabs = {"details", "action-plan", "operational-loss", "review-comment"}
        if incident["type"] == "IT Incident":
            allowed_tabs = {"details", "action-plan", "operational-loss"}
        if active_tab not in allowed_tabs:
            active_tab = "details"

        summary_cards = [
            ("Incident Summary", "At approximately 10:15 AM UTC, monitoring systems detected a complete service outage across the production server cluster in the Singapore region. The downtime affected all external-facing web services and APIs, resulting in disrupted user access, failed transactions, and interrupted integrations.", "summary"),
            ("Root Cause", "An underlying hardware failure in the primary storage node triggered a cascading failure in the cluster, leading to loss of quorum and service unavailability. A secondary issue in the automated failover mechanism delayed service recovery.", "cause"),
        ]
        key_detail_items = [
            ("Incident ID", "00001"),
            ("Incident Type", "Operational Risk Incident"),
            ("Incident Title", "(Sample only) Critical Server Downtime Critical Server Downtime Critical Server Downtime Critical Server Downtime Critical Server Downtime Critical Server Downtime Critical Server Downtime"),
            ("Incident Occurred Date", "04-04-2025"),
            ("Detection Date", "04-04-2025"),
            ("Near Miss", "Yes"),
            ("Creation Date", "06-04-2025"),
            ("Created within Timeline", "Yes"),
            ("Justification for Late Lodgement", ""),
            ("Closure Date", "20-04-2025"),
            ("Closed within Timeline", "Yes"),
            ("Justification for Late Closure", ""),
            ("Reopen Date", ""),
            ("Reopen Justification", ""),
            ("Withdrawal Date", ""),
            ("Withdrawal Justification", ""),
            ("Personal Data Involved", "Yes"),
        ]
        impact_items = [
            ("Incident Impact Classification", "Significant"),
            ("Financial Impact", "Significant"),
            ("Non-Financial Impact", "Significant"),
            ("Potential Loss", "$"),
            ("Gross Loss", "$ 500,000"),
            ("Recovered Amount", "$ 450,000"),
            ("Net Financial Impact", "$ 50,000"),
        ]
        action_plans = [
            {"id": "AP00001", "title": "Manual restart and rebalance of affected nodes. This included bri...", "status": "In Progress", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "Yes", "reviewer": "ORM"},
            {"id": "AP00002", "title": "Manual restart and rebalance of affected nodes. This included bri...", "status": "Withdrawn", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "No", "reviewer": "ORM"},
            {"id": "AP00003", "title": "Manual restart and rebalance of affected nodes. This included bri...", "status": "Close", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "Yes", "reviewer": "RC"},
            {"id": "AP00004", "title": "Manual restart and rebalance of affected nodes. This included bri...", "status": "In Progress", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "No", "reviewer": "RC"},
            {"id": "AP00005", "title": "Manual restart and rebalance of affected nodes. This included bri...", "status": "In Progress", "target": "20-04-2025", "completion": "20-04-2025", "overdue": "No", "reviewer": "RC"},
        ]
        op_loss_cards = [
            {"label": "Operational Loss ID", "value": "OL00001", "note": "Linked finance booking record"},
            {"label": "Booking Amount", "value": "SGD 500,000", "note": "Original reported operational loss"},
            {"label": "Recovered Amount", "value": "SGD 450,000", "note": "Recoveries reported"},
            {"label": "Net Reported Operational Loss", "value": "SGD 50,000", "note": "Latest net position"},
        ]
        review_rows = [
            {"group": "ORM", "reviewer": "Cui Qiang", "authorization": "Approved", "reg_breach": "NA", "report_reg": "NA", "comment": "Kindly ensure the action plans are completed on time.", "action": "Replies (10)"},
            {"group": "Legal", "reviewer": "BBB", "authorization": "Rejected", "reg_breach": "NA", "report_reg": "NA", "comment": "Kindly ensure the no data breach.", "action": "Replies (0)"},
            {"group": "CISO", "reviewer": "AAA", "authorization": "Approved", "reg_breach": "NA", "report_reg": "NA", "comment": "OK!", "action": "Replies (0)"},
            {"group": "CRO", "reviewer": "XXX", "authorization": "Approved", "reg_breach": "NA", "report_reg": "NA", "comment": "OK to approve.", "action": "Replies (2)"},
            {"group": "RC", "reviewer": "CCC", "authorization": "NA", "reg_breach": "Yes", "report_reg": "Yes", "comment": "Done reported top MAS.", "action": "Edit / Replies (1)"},
        ]
        timeline = [
            {"user": "Sukie Liu", "time": "09-04-2025 12:20:05PM", "event": "@Rene Chong As discussed, please add the additional supporting document.", "replies": 10},
            {"user": "Rene Chong", "time": "09-04-2025 15:00:05PM", "event": "@Sukie Liu Done updated!", "replies": 0},
            {"user": "Anne Lim", "time": "12-04-2025 12:40:05PM", "event": "@Rene Chong Can further elaborate the root cause?", "replies": 0},
            {"user": "Rene Chong", "time": "13-04-2025 17:30:30PM", "event": "@Anne Lim The downtime was caused by a misconfigured database connection pool in the authentication service, which led to resource exhaustion and service crashes under peak load.", "replies": 0},
        ]
        action_plan_modal = {
            "id": "AP00002",
            "status": "In Progress",
            "plan": "Restoration of services verified through automated and manual checks. The team performed additional manual verifications, including API endpoint testing, user journey validation, and database integrity checks, to ensure that all systems were functioning as expected before declaring full recovery.",
            "owner": "Rene Chong",
            "department": "SG - Risk Operations",
            "reviewer": "ORM",
            "validation_date": "",
            "target_date": "31-05-2025",
            "completion_date": "",
            "overdue": "Yes",
            "extended_date": "",
            "board_date": "",
            "status_update": "15-05-2025 - Manual restart of affected nodes initiated by SRE and Infrastructure teams.\n16-05-2025 - Services began coming online in a staggered sequence to prevent overload.\n20-05-2025 - Core services (web frontend, authentication, and APIs) reached partial operational state.\n22-05-2025 - Manual cluster rebalancing completed to ensure even load distribution.\n24-05-2025 - Automated health checks passed for all critical services.\n27-05-2025 - Manual verification began.",
            "documents": ["PDI Assessment Form.pdf", "Picture from customer.jpg", "PDI Assessment Form.pdf", "Picture from customer.jpg"],
        }
        return render_template(
            "incident_detail.html",
            page_title=incident["id"],
            page_kicker="Incident Details",
            incident=incident,
            active_tab=active_tab,
            show_review_comment=incident["type"] != "IT Incident",
            summary_cards=summary_cards,
            key_detail_items=key_detail_items,
            impact_items=impact_items,
            action_plans=action_plans,
            op_loss_cards=op_loss_cards,
            review_rows=review_rows,
            timeline=timeline,
            action_plan_modal=action_plan_modal,
        )

    @app.get("/authorization")
    def authorization():
        queues = [
            ("20250520-010", "Incident - Authorize New Incident", "Pending Approval", "ORI00003", "Critical Server Downtime", ["ORM", "Legal", "CISO"]),
            ("20250520-008", "Incident - Authorize Edit Action Plan", "Pending Review", "AP00015", "Urgent Restart Server", ["Risk PM"]),
            ("20250520-006", "Incident - Authorize New Incident", "Closed", "ORI00007", "Phishing Attach", ["RC"]),
            ("20250519-007", "Incident - Authorize Withdraw Incident", "Withdraw - Pending Approval", "ORI00009", "Add New Cloud Storage", ["Risk PM"]),
            ("20250518-006", "Incident - Authorize Complete Action Plan", "Verified", "AP00014", "Add New Core and RAM", ["ORM"]),
            ("20260419-002", "Outsourcing - Authorize New Assessment", "Pending Approval", "SSA-SG-2026-0042", "2026 Annual SSA for payment switch managed service", ["Business Owner", "ORM", "RC"]),
            ("20260418-005", "Outsourcing - Authorize Withdraw Assessment", "Withdraw - Pending Approval", "TERM-ID-2026-0002", "Withdraw termination request for cloud monitoring retainer", ["Business Owner", "CISO"]),
            ("20260419-009", "Outsourcing - Report Confirmation", "Pending Review", "MAS-ORM-2026-06", "MAS outsourcing register report confirmation", ["SG ORM FU"]),
        ]
        outsourcing_queue_rows = [
            ("Outsourcing - Authorize New Assessment", "SSA / DD / PIR / On-site / Periodic Review", "Visible only when request is in-process and user is Maker, assigned Reviewer, or Approver."),
            ("Outsourcing - Authorize Withdraw Assessment", "Termination / assessment withdrawal", "Maker cannot approve or review own request; visibility remains status-driven while request is in-process."),
            ("Outsourcing - Report Confirmation", "SG / PH report confirmation", "Visible to SG ORM FU approver queue before email routing and export confirmation."),
        ]
        outsourcing_audit_rows = [
            ("A-20260419-001", "20260419-009", "MAS-ORM-2026-06", "Authorization Management", "view authorization overview", "Rene Chong", "19 Apr 2026 11:40", "GRC-88421"),
            ("A-20260419-002", "20260419-002", "SSA-SG-2026-0042", "Outsourcing Management", "view assessment", "Cui Qiang", "19 Apr 2026 10:52", "GRC-88405"),
            ("A-20260419-003", "20260418-005", "TERM-ID-2026-0002", "Outsourcing Management", "submit withdraw assessment", "Alicia Tan", "18 Apr 2026 19:15", "GRC-88371"),
        ]
        return render_template(
            "authorization.html",
            page_title="Authorization Management",
            page_kicker="F08 / 3.11",
            queues=queues,
            outsourcing_queue_rows=outsourcing_queue_rows,
            outsourcing_audit_rows=outsourcing_audit_rows,
        )

    @app.get("/reports")
    def reports():
        report_rows = [
            ("Daily Incident Register", "Daily", "Ready by 6AM next calendar day"),
            ("Daily Action Plan Consolidation Tracker", "Daily", "Real-time if report date is today"),
            ("Daily Op Loss Booking", "Daily", "Merged by selected FU"),
            ("Monthly Overdue Actions for KRI Reporting", "Monthly", "Ready on first calendar day of following month"),
            ("Quarterly Trend Analysis [Risk Taxonomy]", "Quarterly", "Ready after quarter end"),
            ("Quarterly Trend Analysis [Impact Classification]", "Quarterly", "Ready after quarter end"),
            ("Outsourcing Register", "Daily", "Includes active arrangements and latest approved assessment status"),
            ("Termination Withdrawal Report", "On demand", "Available once withdrawal workflow reaches a terminal decision"),
            ("SG / PH Outsourcing ORM Report", "Scheduled", "Send SG and PH reports to SG ORM FU email"),
        ]
        outsourcing_report_rows = [
            ("Outsourcing Register", "Operational / Compliance", "User-requested XLSX download using current data access control and current timestamp when Today is selected."),
            ("MAS Outsourcing Register - Service Provider and Material Subcontractors", "Regulatory oversight", "Generated for SG / PH report confirmation cycle and authorized per FU approver."),
            ("MAS Outsourcing Register - Non-material Subcontractors", "Regulatory oversight", "Generated together with the material subcontractor report on 1 Jun and 1 Dec for SG / PH only."),
            ("Termination Withdrawal Report", "Exit management", "Tracks terminated or withdrawn arrangements, data return, and access revocation progress."),
        ]
        report_confirmation_rows = [
            ("MAS Outsourcing Register - Service Provider and Material Subcontractors", "Pending Confirmation", "SG / PH only. Generated on 1 Jun and 1 Dec, then confirmed by FU approver before distribution."),
            ("MAS Outsourcing Register - Non-material Subcontractors", "Pending Confirmation", "Generated together with the material subcontractor report and tied to the same FU confirmation route."),
            ("Termination Withdrawal Report", "Ready for Export", "Export confirmation captures who generated the XLSX file and the exit status snapshot used."),
        ]
        email_routing_rows = [
            ("MAS Outsourcing Register - Service Provider and Material Subcontractors", "sg-orm-fu@npt.sg", "SG ORM FU mailbox", "Email routed only after report confirmation is complete"),
            ("MAS Outsourcing Register - Non-material Subcontractors", "sg-orm-fu@npt.sg", "SG ORM FU mailbox", "Email routed together with the material subcontractor report"),
            ("Termination Withdrawal Report", "ops-risk-governance@npt.sg", "Operations Risk Governance", "Only sent after confirmation and terminal decision"),
            ("Outsourcing Register", "Portal only", "No email distribution", "Viewed online or exported on demand"),
        ]
        export_audit_rows = [
            ("19 Apr 2026 11:40", "MAS Outsourcing Register - Service Provider and Material Subcontractors", "Report Confirmation completed by Rene Chong; email queued to SG ORM FU mailbox."),
            ("19 Apr 2026 11:40", "MAS Outsourcing Register - Non-material Subcontractors", "Generated in the same cycle and queued with identical recipient route."),
            ("18 Apr 2026 18:12", "Termination Withdrawal Report", "Export generated for approver review with exit checklist snapshot attached."),
            ("18 Apr 2026 07:05", "Outsourcing Register", "Daily register refreshed after approved DD and SSA status synchronization."),
        ]
        return render_template(
            "reports.html",
            page_title="Report",
            page_kicker="F08 / F09",
            report_rows=report_rows,
            outsourcing_report_rows=outsourcing_report_rows,
            report_confirmation_rows=report_confirmation_rows,
            email_routing_rows=email_routing_rows,
            export_audit_rows=export_audit_rows,
        )

    @app.get("/parameter-management")
    def parameter_management():
        parameter_rows = [
            ("Para00001", "Incident Management", "Financial Impact / Non-Financial Impact", "Tooltip and dropdown matrix"),
            ("Para00002", "Incident Management", "Personal Data Involved", "Tooltip text"),
            ("Para00003", "Incident Management", "Parties Involved", "Tooltip text"),
            ("Para00004", "Incident Management", "Non-Financial Impact Detailed Description", "Impact description table"),
            ("Para00005", "Incident Management", "Completion Date", "Shared with RCSA"),
            ("Para00006", "Incident Management", "Target Completion Date", "Shared with RCSA"),
        ]
        outsourcing_parameter_rows = [
            ("Para10001", "Outsourcing Management", "Service Scope", "Outsourcing arrangement service scope options."),
            ("Para10002", "Outsourcing Management", "IT Scope", "IT scope values applied to outsourcing arrangements."),
            ("Para10003", "Outsourcing Management", "What is the outsourcing categorization?", "Categorization values used in arrangement details and reports."),
            ("Para10004", "Outsourcing Management", "Outsourced Relevant Services / Outsourced Relevant Services - Additional", "Relevant services list used in arrangement profile and report outputs."),
            ("Para10005", "Outsourcing Management", "Frequency of audit on the provider", "Review frequency used by oversight and reporting flows."),
        ]
        return render_template(
            "parameter_management.html",
            page_title="Parameter Management",
            page_kicker="Incident Management Parameters",
            parameter_rows=parameter_rows,
            outsourcing_parameter_rows=outsourcing_parameter_rows,
        )

    @app.get("/parameter-management/<parameter_id>")
    def parameter_view(parameter_id: str):
        if parameter_id.startswith("Para1"):
            parameter = {
                "id": parameter_id,
                "module": "Outsourcing Management",
                "field_name": "IT Scope" if parameter_id == "Para10002" else "Service Scope",
                "country": "SG / PH",
                "usage": "Outsourcing arrangement field options used across overview, details, and reporting.",
                "status": "Active",
                "content": "Infrastructure / Application / Security / Data / Operations" if parameter_id == "Para10002" else "Payment operations / customer support / security monitoring / reconciliation support",
            }
        else:
            parameter = {
                "id": parameter_id,
                "module": "Incident Management",
                "field_name": "Financial Impact / Non-Financial Impact",
                "country": "SG",
                "usage": "Tooltip and dropdown matrix",
                "status": "Active",
                "content": "Significant / Moderate / Low / Minimal / NA",
            }
        return render_template(
            "parameter_view.html",
            page_title=f"View {parameter_id}",
            page_kicker="Parameter Management",
            parameter=parameter,
        )

    @app.get("/parameter-management/<parameter_id>/edit")
    def parameter_edit(parameter_id: str):
        if parameter_id.startswith("Para1"):
            parameter = {
                "id": parameter_id,
                "module": "Outsourcing Management",
                "field_name": "Frequency of audit on the provider" if parameter_id == "Para10005" else "What is the outsourcing categorization?",
                "country": "SG / PH",
                "status": "Active",
                "content": "Annually / Half-yearly / Quarterly / Event-based" if parameter_id == "Para10005" else "Material outsourcing / Non-material outsourcing / Specialist service",
                "tooltip": "Used by outsourcing overview, details, report confirmation, and report export logic.",
            }
        else:
            parameter = {
                "id": parameter_id,
                "module": "Incident Management",
                "field_name": "Financial Impact / Non-Financial Impact",
                "country": "SG",
                "status": "Active",
                "content": "Significant / Moderate / Low / Minimal / NA",
                "tooltip": "Display impact guidance to makers during incident creation and edit.",
            }
        return render_template(
            "parameter_edit.html",
            page_title=f"Edit {parameter_id}",
            page_kicker="Parameter Management",
            parameter=parameter,
        )

    @app.get("/email-management")
    def email_management():
        email_rows = [
            ("EML-0005", "Notification: Overdue Operational Risk Incident", "One time", "Active", "Reviewer (ORM)"),
            ("EML-0012", "Notification: Pending Review Reminder", "Daily", "Active", "Reviewer Groups"),
            ("EML-0018", "Notification: Action Plan Overdue", "Weekly", "Active", "Action Owner and Approver"),
            ("EML-0026", "Notification: Reopen Request Submitted", "One time", "Active", "Maker and Approver"),
        ]
        outsourcing_email_rows = [
            ("EML-1001", "Notification: SG / PH Outsourcing ORM Report", "Scheduled", "Active", "SG ORM FU Mailbox"),
            ("EML-1002", "Notification: Report Confirmation Reminder", "One time", "Active", "Report Confirmer / SG ORM FU"),
            ("EML-1003", "Notification: Termination Withdrawal Report", "On demand", "Active", "Operations Risk Governance"),
        ]
        return render_template(
            "email_management.html",
            page_title="Email Management",
            page_kicker="Incident Notification Templates",
            email_rows=email_rows,
            outsourcing_email_rows=outsourcing_email_rows,
        )

    @app.get("/email-management/<email_id>/edit")
    def email_edit(email_id: str):
        if email_id.startswith("EML-10"):
            email = {
                "id": email_id,
                "title": "Notification: SG / PH Outsourcing ORM Report" if email_id == "EML-1001" else "Notification: Termination Withdrawal Report",
                "frequency": "Scheduled" if email_id == "EML-1001" else "On demand",
                "status": "Active",
                "recipient_to": "SG ORM FU Mailbox" if email_id == "EML-1001" else "Operations Risk Governance",
                "recipient_cc": "Report Confirmer / Business Owner",
                "content_fixed": "<<This is an auto-generated message, please do not reply.>>\n\nRefer to {Link to the GRC Portal Report}.",
                "content_editable": "Dear team,\n\nPlease find the outsourcing report pack generated after report confirmation. The report uses the approved outsourcing arrangement and assessment snapshot.\n\nThanks.",
            }
        else:
            email = {
                "id": email_id,
                "title": "Notification: Overdue Operational Risk Incident",
                "frequency": "One time",
                "status": "Active",
                "recipient_to": "Custom Role: Reviewer (Reviewer Group = ORM)",
                "recipient_cc": "Incident FU's Maker, Incident FU's Approver",
                "content_fixed": "<<This is an auto-generated message, please do not reply.>>\n\nRefer to {Link to the GRC Portal with Incident ID}.",
                "content_editable": "Dear ORM team,\n\nPlease note that there is an Operational Risk Incident created beyond the 7 calendar days timeline from the date of detection.\n\nPlease refer to the GRC System for full details of the incident.\n\nThanks.",
            }
        return render_template(
            "email_edit.html",
            page_title=f"Edit {email_id}",
            page_kicker="Email Management",
            email=email,
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, port=5011)
