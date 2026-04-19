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


def _find_incident(incident_id: str) -> dict | None:
    for incident in INCIDENTS:
        if incident["id"] == incident_id:
            return incident
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
                    "label": "GRC Demo",
                    "href": "/grc-demo/",
                    "active": current_path == "/" or current_path.startswith("/"),
                },
            ],
            "nav_items": [
                {"label": "GRC Access Control", "endpoint": "access_control"},
                {"label": "Incident Overview", "endpoint": "overview"},
                {"label": "Add Incident", "endpoint": "add_incident"},
                {"label": "Authorization Management", "endpoint": "authorization"},
                {"label": "Parameter Management", "endpoint": "parameter_management"},
                {"label": "Email Management", "endpoint": "email_management"},
                {"label": "Report", "endpoint": "reports"},
            ],
            "workspace_tabs": [
                {"label": "Governance, Risk & Compliance (GRC)", "href": "#", "active": False},
                {"label": "Incident Management", "href": url_for("overview"), "active": True},
            ],
        }

    @app.before_request
    def enforce_owner_access():
        email = _current_google_email()
        if not email or email != owner_email.strip().lower():
            return redirect("/")

    @app.get("/")
    def root():
        return overview()

    @app.get("/overview")
    def overview():
        return render_template(
            "overview.html",
            page_title="Incident Overview",
            page_kicker="SG Version",
            incidents=INCIDENTS,
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
        approval_center = [
            ("Organization Structure", "Add / Edit Organization Structure"),
            ("Reviewer List", "Add / Edit Reviewer List"),
            ("Email Management", "Authorize Edit Email Management"),
            ("Incident Management", "Withdraw / Reopen / Complete AP / Edit AP"),
        ]
        return render_template(
            "access_control.html",
            page_title="GRC Access Control",
            page_kicker="Incident Management Support Setup",
            role_permissions=role_permissions,
            org_rows=org_rows,
            reviewer_rows=reviewer_rows,
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
        ]
        return render_template(
            "approval_center.html",
            page_title="Approval Center",
            page_kicker="Incident-Related Authorizations",
            rows=rows,
        )

    @app.get("/approval-center/<auth_id>")
    def approval_detail(auth_id: str):
        review_comment_rows = [
            ("ORM", "Cui Qiang", "Approved", "NA", "NA", "Kindly ensure the action plans are completed on time.", "Comment (10 replies)"),
            ("Legal", "BBB", "Rejected", "NA", "NA", "Kindly ensure the no data breach.", "Comment (0 replies)"),
            ("CISO", "AAA", "Approved", "NA", "NA", "OK!", "Comment (0 replies)"),
            ("RC", "CCC", "NA", "Yes", "Yes", "Done reported top MAS.", "Comment (2 replies)"),
        ]
        return render_template(
            "approval_detail.html",
            page_title=auth_id,
            page_kicker="Approval Detail",
            auth_id=auth_id,
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
            ("20250520-010", "Incident - Authorize New Incident", "Pending", "ORI00003", "Critical Server Downtime", ["ORM", "Legal", "CISO"]),
            ("20250520-008", "Incident - Authorize Edit Action Plan", "Pending", "AP00015", "Urgent Restart Server", ["Risk PM"]),
            ("20250520-006", "Incident - Authorize New Incident", "Pending", "ORI00007", "Phishing Attach", ["RC"]),
            ("20250519-007", "Incident - Authorize Withdraw Incident", "Pending", "ORI00009", "Add New Cloud Storage", ["Risk PM"]),
            ("20250518-006", "Incident - Authorize Complete Action Plan", "Pending", "AP00014", "Add New Core and RAM", ["ORM"]),
        ]
        return render_template(
            "authorization.html",
            page_title="Authorization Management",
            page_kicker="F08 / 3.11",
            queues=queues,
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
        ]
        return render_template(
            "reports.html",
            page_title="Report",
            page_kicker="F07 / 3.10",
            report_rows=report_rows,
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
        return render_template(
            "parameter_management.html",
            page_title="Parameter Management",
            page_kicker="Incident Management Parameters",
            parameter_rows=parameter_rows,
        )

    @app.get("/parameter-management/<parameter_id>")
    def parameter_view(parameter_id: str):
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
        return render_template(
            "email_management.html",
            page_title="Email Management",
            page_kicker="Incident Notification Templates",
            email_rows=email_rows,
        )

    @app.get("/email-management/<email_id>/edit")
    def email_edit(email_id: str):
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
