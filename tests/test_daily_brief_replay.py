from __future__ import annotations

from scripts.evaluate_daily_brief_replay import _quality_gates


FIVE_EMPTY_SECTIONS = """Xiaodong Action Required
- 无
Watch / Delegate
- 无
Project Updates
- 无
Other Update
- 无
Suggested Team Follow-up
- 无
"""


def _quality(
    briefing: dict,
    *,
    raw_seatalk: str = "",
    raw_gmail: str = "",
    evidence_refs: list[dict] | None = None,
) -> dict:
    return _quality_gates(
        briefing=briefing,
        text_body=FIVE_EMPTY_SECTIONS,
        raw_seatalk=raw_seatalk,
        raw_gmail=raw_gmail,
        filtered_gmail=raw_gmail,
        candidates=[],
        resolved_candidates=[],
        evidence_refs=evidence_refs,
    )


def test_quality_gate_rejects_same_section_mari_duplicates() -> None:
    summary = (
        "State: Mari Stock Trading is still closing Payment BC and account integration decisions. "
        "Impact: dependencies affect readiness. Next: close the API decisions."
    )
    briefing = {
        "project_updates": [
            {"domain": "General", "summary": summary, "evidence": "[PM x Dev] Mari Stock Trading / thread: Stock Asset API"},
            {"domain": "General", "summary": summary, "evidence": "Mari Stock Trading Project Group / thread: MTA notifications"},
        ]
    }

    quality = _quality(briefing)

    assert "duplicate_topic" in quality["findings"]


def test_quality_gate_rejects_source_domain_and_routine_reminder_leaks() -> None:
    briefing = {
        "direct_action_todos": [
            {
                "domain": "General",
                "task": "Change the password after receiving Please Change Password notice.",
                "evidence": "Gmail: Please Change Password - SeaBank Admin Portal",
            },
            {
                "domain": "General",
                "task": "Ensure the PH GPay team and CIF rebaseline against 29 Oct.",
                "evidence": "PH GPay UAT Support / thread: supporting doc screenshot",
            },
        ],
        "project_updates": [
            {
                "domain": "Credit Risk",
                "summary": "The KYC queue changed. Next: confirm owners and delivery dates.",
                "evidence": "ID PM x KYC PM / thread: LexisNexis Screening",
            }
        ],
    }

    quality = _quality(briefing)

    assert "low_value_reminder_leak" in quality["findings"]
    assert "source_coherence_or_domain_leak" in quality["findings"]
    assert "generic_executive_filler" in quality["findings"]


def test_quality_gate_does_not_require_ordinary_mari_stock_chatter() -> None:
    quality = _quality(
        {},
        raw_seatalk=(
            "=== Mari Stock Trading Project Group ===\n"
            "[2026-08-07 14:00:00] Invest PM: Here are the notification templates already in production."
        ),
    )

    assert "known_high_signal_not_visible" not in quality["findings"]


def test_quality_gate_rejects_evidence_text_not_backed_by_ref_id() -> None:
    briefing = {
        "direct_action_todos": [
            {
                "domain": "General",
                "task": "Review the Denise payment estimate.",
                "evidence": "Private SeaTalk chat with Denise",
                "evidence_ref_id": "st-ref-001",
            }
        ]
    }
    evidence_refs = [
        {
            "id": "st-ref-001",
            "source_type": "seatalk",
            "evidence": "[ID Auth] FV Error Monitoring Alert_error",
        }
    ]

    quality = _quality(briefing, evidence_refs=evidence_refs)

    assert "evidence_ref_mismatch" in quality["findings"]
    assert quality["evidence_ref_mismatch_count"] == 1


def test_quality_gate_accepts_merged_evidence_with_matching_refs() -> None:
    briefing = {
        "project_updates": [
            {
                "domain": "General",
                "summary": "A shared dependency remains open.",
                "status": "in_progress",
                "evidence": "Group A / thread: Dependency; Group B / thread: API contract",
                "evidence_ref_id": "st-ref-001, st-ref-002",
            }
        ]
    }
    evidence_refs = [
        {"id": "st-ref-001", "source_type": "seatalk", "evidence": "Group A / thread: Dependency"},
        {"id": "st-ref-002", "source_type": "seatalk", "evidence": "Group B / thread: API contract"},
    ]

    quality = _quality(briefing, evidence_refs=evidence_refs)

    assert "evidence_ref_mismatch" not in quality["findings"]


def test_quality_gate_rejects_third_person_voice_in_xiaodong_action() -> None:
    quality = _quality(
        {
            "direct_action_todos": [
                {
                    "domain": "Ops Risk",
                    "task": "Provide Xiaodong's root-cause input for the incident RCA.",
                    "evidence": "Gmail: Incident RCA / Alice",
                }
            ]
        }
    )

    assert "third_person_xiaodong_action" in quality["findings"]


def test_quality_gate_rejects_project_update_without_state_impact_next() -> None:
    quality = _quality(
        {
            "project_updates": [
                {
                    "domain": "General",
                    "title": "Project update",
                    "summary": "The PRD review is progressing and technical design has started.",
                    "status": "in_progress",
                    "evidence": "Project group",
                }
            ]
        }
    )

    assert "unstructured_project_update" in quality["findings"]
