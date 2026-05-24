from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import bpmis_jira_tool.work_memory as work_memory
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.work_memory import (
    VISIBILITY_PRIVATE,
    VISIBILITY_TEAM,
    WorkMemoryStore,
    gmail_attachment_memory_item,
    gmail_drive_link_memory_item,
    gmail_message_memory_item,
    meeting_record_memory_items,
    sent_monthly_report_memory_item_from_gmail_record,
    team_dashboard_memory_items,
)
from bpmis_jira_tool.web import create_app, _backfill_gmail_work_memory, _build_google_drive_service, _is_sent_monthly_report_subject


class WorkMemoryStoreTests(unittest.TestCase):
    def test_memory_store_health_defaults_are_stable_without_portal_surface(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            health = store.health()

        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["item_count"], 0)
        self.assertEqual(health["feedback_count"], 0)
        self.assertEqual(health["materialized_count"], 0)

    def test_private_items_are_owner_scoped_and_corrections_win(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            item = store.record_memory_item(
                source_type="meeting_recorder",
                source_id="record-1",
                item_type="decision",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Alice owns rollout",
                content="Alice owns rollout",
            )

            self.assertEqual(store.query_work_memory(owner_email="other@npt.sg", visibility_scope="owner"), [])
            self.assertEqual(len(store.query_work_memory(owner_email="owner@npt.sg", visibility_scope="owner")), 1)

            result = store.record_memory_feedback(
                item_id=item["item_id"],
                action="correct",
                owner_email="owner@npt.sg",
                correction_text="Bob owns rollout",
            )

            self.assertEqual(result["item"]["summary"], "Bob owns rollout")
            self.assertEqual(store.query_work_memory(owner_email="owner@npt.sg", query="Bob")[0]["summary"], "Bob owns rollout")

    def test_private_feedback_hides_team_visible_item_from_team_scope(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            item = store.record_memory_item(
                source_type="team_dashboard",
                source_id="AF:project:225159",
                item_type="project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="AF project",
                content="AF project",
            )
            self.assertEqual(len(store.query_work_memory(owner_email="teammate@npt.sg", visibility_scope="team")), 1)

            store.record_memory_feedback(item_id=item["item_id"], action="private", owner_email="owner@npt.sg")

            self.assertEqual(store.query_work_memory(owner_email="teammate@npt.sg", visibility_scope="team"), [])

    def test_ingestion_ledger_is_exposed_in_health(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")

            store.record_ingestion_run(
                source_type="gmail_sent_monthly_report",
                owner_email="owner@npt.sg",
                cursor="newer_than:90d",
                status="ok",
                scanned_count=3,
                matched_count=1,
                recorded_count=1,
                duplicate_count=0,
                failed_count=0,
            )

            health = store.health()
            self.assertEqual(health["ingestion_runs"][0]["source_type"], "gmail_sent_monthly_report")
            self.assertEqual(health["ingestion_runs"][0]["cursor"], "newer_than:90d")
            self.assertEqual(health["ingestion_runs"][0]["recorded_count"], 1)

    def test_existing_source_ids_returns_owner_scoped_matches(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="gmail",
                source_id="msg-1",
                item_type="decision",
                owner_email="owner@npt.sg",
                summary="Recorded message",
                content="Recorded message",
            )
            store.record_memory_item(
                source_type="gmail",
                source_id="msg-2",
                item_type="decision",
                owner_email="other@npt.sg",
                summary="Other owner",
                content="Other owner",
            )

            existing = store.existing_source_ids(
                source_type="gmail",
                owner_email="owner@npt.sg",
                source_ids=["msg-1", "msg-2", "msg-3"],
            )

        self.assertEqual(existing, {"msg-1"})

    def test_processed_source_ids_are_owner_scoped(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_processed_source_ids(
                source_type="gmail",
                owner_email="owner@npt.sg",
                source_ids=["msg-1", "msg-1"],
                metadata={"event": "gmail_backfill"},
            )
            store.record_processed_source_ids(
                source_type="gmail",
                owner_email="other@npt.sg",
                source_ids=["msg-2"],
            )

            existing = store.processed_source_ids(
                source_type="gmail",
                owner_email="owner@npt.sg",
                source_ids=["msg-1", "msg-2", "msg-3"],
            )

        self.assertEqual(existing, {"msg-1"})

    def test_about_me_gate_controls_personal_work_profile(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            decision = store.record_memory_item(
                source_type="meeting_recorder",
                source_id="rec-1:decision:1",
                item_type="decision",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Use phased rollout",
                content="Use phased rollout",
                metadata={"attribution_scope": "meeting", "personal_profile_eligible": False},
            )
            owner_candidate = store.record_memory_item(
                source_type="meeting_recorder",
                source_id="rec-1:owner_speech_candidate",
                item_type="owner_speech_candidate",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Owner speech candidates",
                content="I prefer short launch updates.",
                metadata={"attribution_scope": "owner_speech_candidate", "personal_profile_eligible": "candidate_after_review"},
            )

            store.record_memory_feedback(item_id=decision["item_id"], action="accept", owner_email="owner@npt.sg")
            profile = store.materialize_personal_work_profile(owner_email="owner@npt.sg")
            self.assertFalse(profile["materialized"])

            store.record_memory_feedback(item_id=owner_candidate["item_id"], action="about_me", owner_email="owner@npt.sg")
            profile = store.materialize_personal_work_profile(owner_email="owner@npt.sg")
            self.assertTrue(profile["materialized"])
            self.assertEqual(profile["metadata"]["eligible_count"], 1)

            store.record_memory_feedback(item_id=owner_candidate["item_id"], action="not_about_me", owner_email="owner@npt.sg")
            profile = store.materialize_personal_work_profile(owner_email="owner@npt.sg")
            self.assertFalse(profile["materialized"])
            self.assertEqual(profile["rejected_count"], 1)

    def test_review_candidates_are_prioritized_and_exclude_reviewed_items(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            project = store.record_memory_item(
                source_type="team_dashboard",
                source_id="project-1",
                item_type="project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="Project baseline",
                content="Project baseline",
            )
            risk = store.record_memory_item(
                source_type="meeting_recorder",
                source_id="risk-1",
                item_type="risk",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Launch risk",
                content="Launch risk",
            )
            store.record_memory_feedback(item_id=project["item_id"], action="accept", owner_email="owner@npt.sg")

            candidates = store.review_candidates(owner_email="owner@npt.sg")

            self.assertEqual(candidates[0]["item_id"], risk["item_id"])
            self.assertNotIn(project["item_id"], {item["item_id"] for item in candidates})

    def test_query_filters_before_applying_result_limit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            for index in range(10):
                store.record_memory_item(
                    source_type="meeting_recorder",
                    source_id=f"newer-{index}",
                    item_type="todo",
                    owner_email="owner@npt.sg",
                    visibility=VISIBILITY_PRIVATE,
                    observed_at=f"2026-05-04T10:{index:02d}:00Z",
                    summary=f"Generic newer item {index}",
                    content="No project reference.",
                )
            store.record_memory_item(
                source_type="meeting_recorder",
                source_id="older-sloan",
                item_type="decision",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                observed_at="2026-05-03T10:00:00Z",
                summary="SLoan disbursement decision",
                content="SLoan should use a dedicated fraud scenario.",
            )

            items = store.project_timeline(project_ref="SLoan", owner_email="owner@npt.sg", limit=3)

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["summary"], "SLoan disbursement decision")

    def test_sent_report_precedence_beats_meeting_fact_in_superagent_context(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="meeting_recorder",
                source_id="rec-1:todo:1",
                item_type="todo",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                observed_at="2026-05-02T10:00:00Z",
                summary="SLoan is waiting for discussion",
                content="SLoan is waiting for discussion",
                weight=1.0,
            )
            store.record_memory_item(
                source_type="gmail_sent_monthly_report",
                source_id="msg-1",
                item_type="curated_report",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                observed_at="2026-05-03T10:00:00Z",
                summary="[Banking] Product Update (01 May - 07 May) - Anti-Fraud, Credit Risk & Ops Risk",
                content="SLoan final status is ready for UAT.",
                weight=2.0,
            )

            context = store.query_superagent_context(query="SLoan", owner_email="owner@npt.sg", task_type="project_status")
            answer = store.generate_superagent_answer(task_type="project_status", query="SLoan", context=context)

            self.assertEqual(context["items"][0]["source_type"], "gmail_sent_monthly_report")
            self.assertIn("Final sent report says", answer["answer"])
            self.assertTrue(answer["evidence"])

    def test_superagent_does_not_invent_without_evidence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")

            context = store.query_superagent_context(query="unknown project", owner_email="owner@npt.sg", task_type="project_status")
            answer = store.generate_superagent_answer(task_type="project_status", query="unknown project", context=context)

            self.assertEqual(answer["confidence"], "none")
            self.assertEqual(answer["evidence"], [])
            self.assertIn("do not have enough", answer["answer"])

    def test_materialized_project_profile_stays_private_when_private_evidence_is_used(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="team_dashboard",
                source_id="project-1",
                item_type="project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="SLoan team-visible project",
                content="SLoan team-visible project",
            )
            store.record_memory_item(
                source_type="meeting_recorder",
                source_id="rec-1:todo:1",
                item_type="todo",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="SLoan private follow-up",
                content="SLoan private follow-up",
            )

            profile = store.materialize_project_profile(project_key="SLoan", owner_email="owner@npt.sg")

            self.assertTrue(profile["materialized"])
            self.assertEqual(profile["visibility"], VISIBILITY_PRIVATE)
            team_context = store.query_superagent_context(query="SLoan", owner_email="teammate@npt.sg", visibility_scope="team")
            self.assertTrue(all(item["visibility"] == VISIBILITY_TEAM for item in team_context["items"]))

    def test_entity_resolution_merges_project_and_person_aliases(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="team_dashboard",
                source_id="AF:project:225159",
                item_type="project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="CRC Revamp owned by Bob",
                content="CRC Revamp owned by Bob",
                entities=[
                    {"entity_type": "project", "entity_key": "225159", "label": "CRC Revamp", "relation": "describes"},
                    {"entity_type": "bpmis_id", "entity_key": "225159", "label": "225159", "relation": "identifies"},
                    {"entity_type": "person", "entity_key": "bob@npt.sg", "label": "Bob Tan", "relation": "owner", "metadata": {"seatalk_name": "Bob T", "jira_user": "btan"}},
                ],
            )

            project = store.resolve_work_entity(query="CRC Revamp", owner_email="owner@npt.sg", entity_type="project")
            person = store.resolve_work_entity(query="Bob T", owner_email="owner@npt.sg", entity_type="person")

            self.assertEqual(project["canonical_key"], "225159")
            self.assertEqual(person["canonical_key"], "bob@npt.sg")

    def test_superagent_audit_explain_and_eval_cases(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="gmail_sent_monthly_report",
                source_id="msg-1",
                item_type="curated_report",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="SLoan is ready for UAT",
                content="SLoan is ready for UAT",
                weight=2.0,
            )

            context = store.query_superagent_context(query="SLoan", owner_email="owner@npt.sg", task_type="project_status")
            answer = store.generate_llm_superagent_answer(task_type="project_status", query="SLoan", context=context)
            audit = store.record_superagent_audit_log(
                owner_email="owner@npt.sg",
                user_email="owner@npt.sg",
                query="SLoan",
                task_type="project_status",
                visibility_scope="owner",
                context=context,
                answer=answer,
            )
            explanation = store.explain_superagent_answer(owner_email="owner@npt.sg", query="SLoan", task_type="project_status")
            eval_result = store.run_superagent_eval_cases(
                owner_email="owner@npt.sg",
                cases=[
                    {
                        "question": "SLoan status?",
                        "task_type": "project_status",
                        "expected_source_type": "gmail_sent_monthly_report",
                        "expected_text": "ready for uat",
                    }
                ],
            )

            self.assertTrue(audit["used_private_evidence"])
            self.assertEqual(store.superagent_audit_log(owner_email="owner@npt.sg")[0]["audit_id"], audit["audit_id"])
            self.assertEqual(explanation["evidence"][0]["source_type"], "gmail_sent_monthly_report")
            self.assertEqual(eval_result["passed_count"], 1)

    def test_gold_eval_tracks_answer_points_without_using_gold_as_answer_source(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="gmail_sent_monthly_report",
                source_id="msg-1",
                item_type="curated_report",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="PH AFASA Money Lock update",
                content="Money Lock and Kill Switch are targeted for 2026 June end. Contact lead@example.com. See https://example.com/private.",
                weight=2.0,
            )

            context = store.query_superagent_context(query="Money Lock AFASA deadline", owner_email="owner@npt.sg", task_type="project_status")
            answer = store.generate_llm_superagent_answer(task_type="project_status", query="Money Lock AFASA deadline", context=context)
            result = store.run_superagent_eval_cases(
                owner_email="owner@npt.sg",
                cases=[
                    {
                        "question": "Money Lock AFASA deadline?",
                        "task_type": "project_status",
                        "expected_answer_points": ["2026 June end", "not in evidence"],
                        "expected_sources": ["Gmail Sent Report"],
                        "expected_links": ["https://jira.shopee.io/browse/SPDBK-129093"],
                        "domain": "Anti-Fraud",
                    }
                ],
            )

            self.assertIn("2026 June end", answer["direct_answer"])
            self.assertNotIn("lead@example.com", answer["direct_answer"])
            self.assertNotIn("https://example.com/private", answer["direct_answer"])
            self.assertFalse(result["results"][0]["passed"])
            self.assertEqual(result["results"][0]["missing_answer_points"], ["not in evidence"])
            self.assertEqual(result["results"][0]["expected_links"], ["https://jira.shopee.io/browse/SPDBK-129093"])

    def test_superagent_quality_gate_v1_contract_and_pm_brief(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="team_dashboard",
                source_id="project-1",
                item_type="project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                observed_at="2026-05-05T09:00:00Z",
                summary="CRC Revamp is under PRD",
                content="CRC Revamp is under PRD and planned for May review.",
                weight=1.4,
            )
            store.record_memory_item(
                source_type="meeting_recorder",
                source_id="meeting-1:todo:1",
                item_type="todo",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                observed_at="2026-05-05T10:00:00Z",
                summary="Follow up with RC on CRC Revamp",
                content="Follow up with RC on CRC Revamp before sign-off.",
            )
            store.record_memory_item(
                source_type="gmail",
                source_id="msg-1",
                item_type="risk",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                observed_at="2026-05-05T11:00:00Z",
                summary="CRC Revamp has sign-off risk",
                content="CRC Revamp has sign-off risk if RC feedback is late.",
            )

            context = store.query_superagent_context(query="", owner_email="owner@npt.sg", task_type="pm_brief")
            answer = store.generate_llm_superagent_answer(task_type="pm_brief", query="", context=context)
            store.upsert_superagent_eval_cases(
                owner_email="owner@npt.sg",
                cases=[
                    {
                        "question": "CRC Revamp status?",
                        "task_type": "project_status",
                        "expected_sources": ["Team Dashboard"],
                        "expected_answer_points": ["under PRD"],
                        "suite_id": "gold_v1",
                    }
                ],
            )
            gate = store.run_superagent_quality_gate(owner_email="owner@npt.sg", suite_id="gold_v1", min_cases=1)

            self.assertEqual(answer["answer_contract_version"], "superagent_quality_gate_v1")
            self.assertIn("PM brief", [section["title"] for section in answer["sections"]])
            self.assertIn("Follow-ups", [section["title"] for section in answer["sections"]])
            self.assertIn("Risks and blockers", [section["title"] for section in answer["sections"]])
            self.assertEqual(gate["quality_gate"]["gate_status"], "pass")
            self.assertFalse(gate["quality_gate"]["release_blocking"])

    def test_superagent_quality_gate_reports_failure_diagnostics(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="team_dashboard",
                source_id="project-1",
                item_type="project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="CRC Revamp is under PRD",
                content="CRC Revamp is under PRD.",
            )
            result = store.run_superagent_eval_cases(
                owner_email="owner@npt.sg",
                cases=[
                    {
                        "question": "CRC Revamp status?",
                        "task_type": "project_status",
                        "expected_sources": ["Gmail Sent Report"],
                        "expected_answer_points": ["ready for live"],
                        "suite_id": "gold_v1",
                    }
                ],
                suite_id="gold_v1",
            )

            self.assertEqual(result["quality_gate"]["gate_status"], "fail")
            self.assertIn("wrong_source", result["results"][0]["failure_reasons"])
            self.assertIn("missing_answer_points", result["results"][0]["failure_reasons"])
            self.assertEqual(result["results"][0]["diagnostics"]["answer_contract_version"], "superagent_quality_gate_v1")

    def test_team_visible_superagent_does_not_show_private_excerpt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="gmail_sent_monthly_report",
                source_id="msg-1",
                item_type="curated_report",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Private Project Alpha update",
                content="Private Project Alpha detail should not leak.",
                weight=2.0,
            )

            context = store.query_superagent_context(
                query="Private Project Alpha",
                owner_email="teammate@npt.sg",
                visibility_scope="team",
                task_type="project_status",
            )
            answer = store.generate_llm_superagent_answer(task_type="project_status", query="Private Project Alpha", context=context)

            self.assertEqual(answer["confidence"], "none")
            self.assertNotIn("Private Project Alpha detail", answer["answer"])

    def test_team_dashboard_project_items_include_entities(self):
        items = team_dashboard_memory_items(
            {
                "team_key": "AF",
                "label": "Anti-Fraud",
                "member_emails": ["pm@npt.sg"],
                "under_prd": [
                    {
                        "bpmis_id": "225159",
                        "project_name": "CRC Revamp",
                        "status": "Pending Review",
                        "is_key_project": True,
                        "matched_pm_emails": ["pm@npt.sg"],
                        "jira_tickets": [
                            {
                                "jira_id": "AF-123",
                                "prd_links": [{"url": "https://confluence/pages/1"}],
                            }
                        ],
                    }
                ],
                "pending_live": [],
            },
            owner_email="owner@npt.sg",
        )

        self.assertEqual(items[0]["item_type"], "key_project")
        entity_types = {item["entity_type"] for item in items[0]["entities"]}
        self.assertIn("bpmis_id", entity_types)
        self.assertIn("jira_key", entity_types)
        self.assertEqual(items[0]["visibility"], VISIBILITY_TEAM)

    def test_meeting_minutes_extract_decisions_and_todos(self):
        items = meeting_record_memory_items(
            {
                "record_id": "rec-1",
                "owner_email": "owner@npt.sg",
                "title": "AF Launch",
                "minutes": {
                    "status": "completed",
                    "markdown": "## Decisions\n- Use phased rollout for AF-123.\n\n## Action Items\n- Bob to confirm UAT date.",
                    "asset_url": "/meeting-recorder/assets/rec-1/minutes.md",
                },
                "transcript": {"status": "completed", "text": "Discussed AF-123 and 225159."},
            }
        )

        self.assertTrue(any(item["item_type"] == "decision" for item in items))
        self.assertTrue(any(item["item_type"] == "todo" for item in items))
        self.assertTrue(all(item["visibility"] == VISIBILITY_PRIVATE for item in items))
        decision = next(item for item in items if item["item_type"] == "decision")
        self.assertEqual(decision["metadata"]["attribution_scope"], "meeting")
        self.assertEqual(decision["metadata"]["speaker_attribution"], "unknown")
        self.assertFalse(decision["metadata"]["personal_profile_eligible"])

    def test_meeting_owner_speech_candidates_are_distinct_from_meeting_level_facts(self):
        items = meeting_record_memory_items(
            {
                "record_id": "rec-2",
                "owner_email": "owner@npt.sg",
                "title": "AF Launch",
                "minutes": {"status": "completed", "markdown": "## Decisions\n- Team agreed to launch."},
                "transcript": {
                    "status": "completed",
                    "text": "Team agreed to launch.",
                    "owner_speech_asset_url": "/meeting-recorder/assets/rec-2/owner-microphone-transcript.txt",
                    "owner_speech_candidates": [
                        {
                            "start_seconds": 2,
                            "end_seconds": 4,
                            "text": "I will confirm the launch date.",
                            "speaker": "me_candidate",
                        }
                    ],
                },
            }
        )

        owner_item = next(item for item in items if item["item_type"] == "owner_speech_candidate")

        self.assertEqual(owner_item["metadata"]["attribution_scope"], "owner_speech_candidate")
        self.assertEqual(owner_item["metadata"]["speaker_attribution"], "local_microphone_candidate")
        self.assertEqual(owner_item["metadata"]["personal_profile_eligible"], "candidate_after_review")
        self.assertIn("I will confirm the launch date.", owner_item["content"])

    def test_sent_monthly_report_record_becomes_curated_report(self):
        record = SimpleNamespace(
            headers={"subject": "[Banking] Product Update (01 Apr - 30 Apr) - Anti-Fraud, Credit Risk & Ops Risk", "to": "leaders@npt.sg"},
            body_text="AF-123 and BPMIS 225159 are blocked by approval.",
            message_id="msg-1",
            internal_date=SimpleNamespace(isoformat=lambda: "2026-05-04T10:00:00+08:00"),
        )

        item = sent_monthly_report_memory_item_from_gmail_record(owner_email="owner@npt.sg", record=record)

        self.assertEqual(item["item_type"], "curated_report")
        self.assertEqual(item["source_type"], "gmail_sent_monthly_report")
        self.assertEqual(item["visibility"], VISIBILITY_PRIVATE)

    def test_gmail_message_and_vip_evidence_items_are_private_and_searchable(self):
        record = SimpleNamespace(
            headers={
                "from": "Boss <boss@npt.sg>",
                "to": "owner@npt.sg",
                "cc": "",
                "subject": "AF-123 approval for BPMIS 225159",
            },
            body_text="Approved launch scope. Please follow up on risk.",
            message_id="msg-1",
            thread_id="thread-1",
            label_ids={"INBOX"},
            drive_links=["https://docs.google.com/presentation/d/slide123/edit"],
            attachments=[],
            internal_date=SimpleNamespace(isoformat=lambda: "2026-05-04T10:00:00+08:00"),
        )
        attachment = SimpleNamespace(filename="Design.pdf", mime_type="application/pdf", attachment_id="att-1", size=100)

        message_item = gmail_message_memory_item(
            owner_email="owner@npt.sg",
            record=record,
            matched_vips=[{"display_name": "Boss", "emails": ["boss@npt.sg"]}],
            vip_email_roles={"Boss": ["from"]},
        )
        attachment_item = gmail_attachment_memory_item(
            owner_email="owner@npt.sg",
            record=record,
            attachment=attachment,
            text="PDF mentions AF-123 and BPMIS 225159.",
            sha256="abc",
            matched_vips=[{"display_name": "Boss"}],
        )
        drive_item = gmail_drive_link_memory_item(
            owner_email="owner@npt.sg",
            record=record,
            url="https://docs.google.com/presentation/d/slide123/edit",
            title="Design Review",
            text="Slides mention approval chain.",
            access_status="ok",
            matched_vips=[{"display_name": "Boss"}],
        )

        self.assertEqual(message_item["source_type"], "gmail")
        self.assertEqual(message_item["visibility"], VISIBILITY_PRIVATE)
        self.assertEqual(message_item["item_type"], "decision")
        self.assertGreater(message_item["weight"], 1.0)
        self.assertEqual(attachment_item["source_type"], "gmail_attachment")
        self.assertEqual(drive_item["source_type"], "gmail_drive_link")
        self.assertTrue(any(entity["entity_key"] == "AF-123" for entity in attachment_item["entities"]))

    def test_sent_monthly_report_subject_requires_banking_product_update_format(self):
        self.assertTrue(_is_sent_monthly_report_subject("[Banking] Product Update (01 Apr - 30 Apr) - Anti-Fraud, Credit Risk & Ops Risk"))
        self.assertTrue(_is_sent_monthly_report_subject("[Banking] Product Update (1 Apr - 7 Apr) - Anti-Fraud, Credit Risk & Ops Risk"))
        self.assertTrue(_is_sent_monthly_report_subject("[Banking] Product Update (05 May - 11 May) - Anti-Fraud, Credit Risk & Ops Risk"))
        self.assertFalse(_is_sent_monthly_report_subject("Monthly Report 2026-05"))
        self.assertFalse(_is_sent_monthly_report_subject("[Banking] Product Update (01 Apr - 30 Apr) - Credit Risk"))
    def test_google_drive_service_uses_bounded_http_timeout(self):
        credentials = object()
        with patch.dict("os.environ", {"GOOGLE_DRIVE_HTTP_TIMEOUT_SECONDS": "9"}):
            with patch("bpmis_jira_tool.web.httplib2.Http") as http_cls:
                with patch("bpmis_jira_tool.web.google_auth_httplib2.AuthorizedHttp") as auth_http_cls:
                    with patch("bpmis_jira_tool.web.build_google_api") as build_mock:
                        result = _build_google_drive_service(credentials)

        http_cls.assert_called_once_with(timeout=9)
        auth_http_cls.assert_called_once_with(credentials, http=http_cls.return_value)
        build_mock.assert_called_once_with("drive", "v3", http=auth_http_cls.return_value, cache_discovery=False)
        self.assertEqual(result, build_mock.return_value)

    def test_gmail_backfill_records_vip_pdf_and_drive_evidence_without_crashing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            }
            with patch.dict(os.environ, env, clear=False):
                app = create_app()

            class FakeGmailService:
                def __init__(self, **_kwargs):
                    pass

                def list_work_memory_message_refs(self, *, days, max_messages, now=None):
                    return [{"id": "msg-1"}]

                def fetch_work_memory_message(self, message_id):
                    return SimpleNamespace(
                        headers={
                            "from": "Boss <boss@npt.sg>",
                            "to": "owner@npt.sg",
                            "subject": "AF launch approval",
                        },
                        body_text="Boss approved AF launch. See https://docs.google.com/presentation/d/slide123/edit",
                        message_id=message_id,
                        thread_id="thread-1",
                        label_ids={"INBOX"},
                        attachments=[
                            SimpleNamespace(filename="AF Design.pdf", mime_type="application/pdf", attachment_id="att-1", size=100)
                        ],
                        drive_links=["https://docs.google.com/presentation/d/slide123/edit"],
                        internal_date=SimpleNamespace(isoformat=lambda: "2026-05-04T10:00:00+08:00"),
                    )

                def is_export_noise(self, headers):
                    return False

                def download_attachment(self, *, message_id, attachment_id):
                    return b"%PDF fake"

            with app.app_context(), patch("bpmis_jira_tool.web.GmailDashboardService", FakeGmailService), patch(
                "bpmis_jira_tool.web._extract_pdf_text_for_work_memory",
                return_value="PDF evidence says AF launch is approved.",
            ), patch(
                "bpmis_jira_tool.web._read_google_drive_link_text",
                return_value=("AF Slides", "Slides evidence says deadline is June."),
            ):
                app.config["TEAM_DASHBOARD_CONFIG_STORE"].save(
                    {
                        "report_intelligence_config": {
                            "vip_people": [{"display_name": "Boss", "emails": ["boss@npt.sg"]}],
                        }
                    }
                )
                job = app.config["JOB_STORE"].create("work-memory-gmail-backfill", "Gmail Backfill", owner_email="owner@npt.sg")
                result = _backfill_gmail_work_memory(
                    owner_email="owner@npt.sg",
                    credentials_payload={"token": "x"},
                    report_intelligence_config={"vip_people": [{"display_name": "Boss", "emails": ["boss@npt.sg"]}]},
                    days=90,
                    max_messages=None,
                    drive_read_enabled=True,
                    job_id=job.job_id,
                )
                items = app.config["WORK_MEMORY_STORE"].query_work_memory(owner_email="owner@npt.sg", visibility_scope="owner", limit=20)

        self.assertEqual(result["failed"], 0)
        self.assertTrue(any(item["source_type"] == "gmail" for item in items))
        self.assertTrue(any(item["source_type"] == "gmail_attachment" for item in items))
        self.assertTrue(any(item["source_type"] == "gmail_drive_link" for item in items))

    def test_gmail_backfill_skips_already_recorded_message_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            }
            with patch.dict(os.environ, env, clear=False):
                app = create_app()

            fetched: list[str] = []

            class FakeGmailService:
                def __init__(self, **_kwargs):
                    pass

                def list_work_memory_message_refs(self, *, days, max_messages, now=None):
                    return [{"id": "msg-1"}, {"id": "msg-2"}]

                def fetch_work_memory_message(self, message_id):
                    fetched.append(message_id)
                    return SimpleNamespace(
                        headers={
                            "from": "PM <pm@npt.sg>",
                            "to": "owner@npt.sg",
                            "subject": f"Project update {message_id}",
                        },
                        body_text=f"{message_id} is on track.",
                        message_id=message_id,
                        thread_id=f"thread-{message_id}",
                        label_ids={"INBOX"},
                        attachments=[],
                        drive_links=[],
                        internal_date=SimpleNamespace(isoformat=lambda: "2026-05-04T10:00:00+08:00"),
                    )

                def is_export_noise(self, headers):
                    return False

            with app.app_context(), patch("bpmis_jira_tool.web.GmailDashboardService", FakeGmailService):
                app.config["WORK_MEMORY_STORE"].record_processed_source_ids(
                    source_type="gmail",
                    owner_email="owner@npt.sg",
                    source_ids=["msg-1"],
                )
                job = app.config["JOB_STORE"].create("work-memory-gmail-backfill", "Gmail Backfill", owner_email="owner@npt.sg")
                result = _backfill_gmail_work_memory(
                    owner_email="owner@npt.sg",
                    credentials_payload={"token": "x"},
                    report_intelligence_config={},
                    days=90,
                    max_messages=None,
                    drive_read_enabled=False,
                    job_id=job.job_id,
                )
                processed = app.config["WORK_MEMORY_STORE"].processed_source_ids(
                    source_type="gmail",
                    owner_email="owner@npt.sg",
                    source_ids=["msg-1", "msg-2"],
                )

        self.assertEqual(fetched, ["msg-2"])
        self.assertEqual(result["original_message_count"], 2)
        self.assertEqual(result["skipped_existing"], 1)
        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(processed, {"msg-1", "msg-2"})

    def test_gmail_backfill_isolates_single_message_fetch_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            }
            with patch.dict(os.environ, env, clear=False):
                app = create_app()

            class FakeGmailService:
                def __init__(self, **_kwargs):
                    pass

                def list_work_memory_message_refs(self, *, days, max_messages, now=None):
                    return [{"id": "msg-ok"}, {"id": "msg-fail"}]

                def fetch_work_memory_message(self, message_id):
                    if message_id == "msg-fail":
                        raise ToolError("temporary Gmail failure")
                    return SimpleNamespace(
                        headers={
                            "from": "PM <pm@npt.sg>",
                            "to": "owner@npt.sg",
                            "subject": "AF project update",
                        },
                        body_text="AF-123 is on track.",
                        message_id=message_id,
                        thread_id="thread-ok",
                        label_ids={"INBOX"},
                        attachments=[],
                        drive_links=[],
                        internal_date=SimpleNamespace(isoformat=lambda: "2026-05-04T10:00:00+08:00"),
                    )

                def is_export_noise(self, headers):
                    return False

            with app.app_context(), patch("bpmis_jira_tool.web.GmailDashboardService", FakeGmailService):
                job = app.config["JOB_STORE"].create("work-memory-gmail-backfill", "Gmail Backfill", owner_email="owner@npt.sg")
                result = _backfill_gmail_work_memory(
                    owner_email="owner@npt.sg",
                    credentials_payload={"token": "x"},
                    report_intelligence_config={},
                    days=90,
                    max_messages=None,
                    drive_read_enabled=False,
                    job_id=job.job_id,
                )
                items = app.config["WORK_MEMORY_STORE"].query_work_memory(owner_email="owner@npt.sg", visibility_scope="owner", limit=20)

        self.assertEqual(result["scanned"], 2)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertTrue(any(item["source_type"] == "gmail" and item["source_id"] == "msg-ok" for item in items))

    def test_store_helper_and_defensive_storage_branches(self):
        self.assertEqual(work_memory._stable_json({"bad": {1, 2}}), "{}")
        self.assertEqual(work_memory._load_json(None, {"fallback": True}), {"fallback": True})
        self.assertEqual(work_memory._load_json("{bad json", []), [])
        self.assertEqual(work_memory._normalize_string_list("one"), ["one"])
        self.assertEqual(work_memory._normalize_string_list(123), [])

        class FtsFailingConnection:
            def __init__(self, db_path: Path) -> None:
                self._connection = sqlite3.connect(db_path)
                self._connection.row_factory = sqlite3.Row

            def __enter__(self):
                self._connection.__enter__()
                return self

            def __exit__(self, exc_type, exc, tb):
                return self._connection.__exit__(exc_type, exc, tb)

            def execute(self, sql, *args, **kwargs):
                if "CREATE VIRTUAL TABLE" in str(sql):
                    raise sqlite3.Error("fts disabled")
                return self._connection.execute(sql, *args, **kwargs)

            def commit(self):
                return self._connection.commit()

        class FtsFailingStore(WorkMemoryStore):
            def _connect(self):
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                return FtsFailingConnection(self.db_path)

        class BrokenFtsConnection:
            def execute(self, *_args, **_kwargs):
                raise sqlite3.Error("fts unavailable")

        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertEqual(FtsFailingStore(Path(temp_dir) / "fts-fallback.db").health()["status"], "ok")
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            fallback = store.record_memory_item(
                source_type="gmail",
                source_id="msg-empty-summary",
                owner_email="owner@npt.sg",
                content="x" * 600,
                entities=[{}, {"type": "person", "key": "lead@npt.sg", "label": "Lead"}],
            )

            store._refresh_fts(BrokenFtsConnection(), "item", "summary", "content", "{}")
            with store._connect() as connection:
                self.assertEqual(store._upsert_entity(connection, {}), "")

            filtered = store.existing_source_ids(
                source_type="gmail",
                owner_email="owner@npt.sg",
                source_ids=["msg-empty-summary"],
                item_type="evidence",
            )
            source_filtered = store.query_work_memory(
                owner_email="owner@npt.sg",
                filters={"source_type": "gmail"},
            )

            self.assertEqual(fallback["summary"], ("x" * 600)[:500])
            self.assertEqual(filtered, {"msg-empty-summary"})
            self.assertEqual(source_filtered[0]["source_type"], "gmail")
            self.assertEqual(store.existing_source_ids(source_type="", owner_email="owner@npt.sg", source_ids=[]), set())
            self.assertEqual(store.processed_source_ids(source_type="gmail", owner_email="", source_ids=["x"]), set())
            self.assertEqual(store.record_processed_source_ids(source_type="", owner_email="owner@npt.sg", source_ids=["x"]), 0)
            self.assertEqual(store.record_processed_source_ids(source_type="gmail", owner_email="owner@npt.sg", source_ids=[]), 0)
            self.assertEqual(store.project_timeline(project_ref="", owner_email="owner@npt.sg"), [])
            self.assertEqual(
                store.resolve_work_entity(query="", owner_email="owner@npt.sg"),
                {"status": "ok", "query": "", "entity_type": "unknown", "canonical_key": "", "aliases": [], "candidates": []},
            )
            store.record_memory_item(
                source_type="team_dashboard",
                source_id="entity-aliases",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="Entity with aliases",
                content="Entity with aliases",
                entities=[
                    {
                        "entity_type": "project",
                        "entity_key": "alias-key",
                        "label": "Alias Project",
                        "metadata": {"aliases": ["Project Alias", "Legacy Alias"]},
                    }
                ],
            )
            self.assertEqual(
                store.resolve_work_entity(query="Legacy Alias", owner_email="owner@npt.sg", entity_type="project")["canonical_key"],
                "alias-key",
            )
            self.assertEqual(store.materialize_project_profile(project_key="", owner_email="owner@npt.sg")["reason"], "missing_project_key")
            self.assertEqual(store.materialize_project_profile(project_key="Missing", owner_email="owner@npt.sg")["reason"], "no_evidence")
            preference = store.record_memory_item(
                source_type="meeting_recorder",
                source_id="preference-1",
                item_type="personal_preference",
                owner_email="owner@npt.sg",
                summary="Prefers short launch notes",
                content="Prefers short launch notes",
            )
            store.record_memory_feedback(item_id=preference["item_id"], action="accept", owner_email="owner@npt.sg")
            self.assertTrue(store.materialize_personal_work_profile(owner_email="owner@npt.sg")["materialized"])

    def test_feedback_error_and_weight_branches(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            item = store.record_memory_item(
                source_type="gmail",
                source_id="msg-1",
                item_type="todo",
                owner_email="owner@npt.sg",
                summary="Follow up on CRC",
                content="Follow up on CRC",
                weight=0.2,
            )

            with self.assertRaises(ValueError):
                store.record_memory_feedback(item_id=item["item_id"], action="unsupported", owner_email="owner@npt.sg")
            with self.assertRaises(KeyError):
                store.record_memory_feedback(item_id="missing", action="accept", owner_email="owner@npt.sg")

            important = store.record_memory_feedback(item_id=item["item_id"], action="important", owner_email="owner@npt.sg")
            ignored = store.record_memory_feedback(item_id=item["item_id"], action="ignore", owner_email="owner@npt.sg")

            self.assertGreaterEqual(important["item"]["weight"], 1.5)
            self.assertLessEqual(ignored["item"]["weight"], 0.2)

    def test_superagent_edge_paths_and_eval_diagnostics(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="meeting_recorder",
                source_id="rec-1",
                item_type="meeting",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="My decision was discussed in meeting",
                content="My decision was discussed in meeting.",
                metadata={"attribution_scope": "meeting"},
            )
            store.record_memory_item(
                source_type="team_dashboard",
                source_id="project-1",
                item_type="key_project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="AFASA project is active",
                content="AFASA project is active with scam controls.",
                metadata={"team_label": "Anti-Fraud", "is_key_project": True, "bpmis_id": "225159", "jira_keys": ["AF-123"]},
                entities=[
                    {"entity_type": "project", "entity_key": "225159", "label": "AFASA"},
                    {"entity_type": "person", "entity_key": "lead@npt.sg", "label": "Lead", "relation": "owner"},
                ],
            )
            store.record_memory_item(
                source_type="gmail",
                source_id="todo-1",
                item_type="todo",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Follow up with Lead",
                content="Follow up with Lead.",
            )
            store.record_memory_item(
                source_type="gmail",
                source_id="todo-2",
                item_type="todo",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Confirm active fallback owner",
                content="Confirm active fallback owner.",
            )
            store.record_memory_feedback(
                item_id=store.query_work_memory(owner_email="owner@npt.sg", query="Follow up")[0]["item_id"],
                action="stale",
                owner_email="owner@npt.sg",
            )

            follow_up = store.query_superagent_context(query="NoMatchingFollowUp", owner_email="owner@npt.sg", task_type="follow_up")
            direct_fallback = store.generate_llm_superagent_answer(
                task_type="general",
                query="",
                context={
                    "items": [{"item_id": "", "source_type": "", "item_type": "", "summary": "", "content": "", "visibility": VISIBILITY_TEAM}],
                    "materialized": [],
                    "visibility_scope": "owner",
                },
            )
            materialized = store._upsert_materialized(
                owner_email="owner@npt.sg",
                graph_type="project_profile",
                graph_key="AFASA",
                visibility=VISIBILITY_TEAM,
                summary="Project profile: AFASA",
                content="AFASA profile",
                evidence={},
                metadata={},
            )
            with_materialized = store.generate_llm_superagent_answer(
                task_type="project_status",
                query="AFASA",
                context={"items": [], "materialized": [materialized], "visibility_scope": "owner"},
            )
            attribution = store.generate_llm_superagent_answer(
                task_type="project_status",
                query="my decision",
                context=store.query_superagent_context(query="my decision", owner_email="owner@npt.sg", task_type="project_status"),
            )
            stored = store.upsert_superagent_eval_cases(
                owner_email="owner@npt.sg",
                cases=[
                    "not a dict",
                    {"question": ""},
                    {"question": "Skip suite", "task_type": "general", "suite_id": "other"},
                    {"question": "My decision?", "task_type": "project_status", "expected_sources": ["meeting"], "suite_id": "suite-a"},
                    {"question": "No evidence?", "task_type": "project_status", "expected_answer_points": ["missing"], "suite_id": "suite-a"},
                ],
            )
            eval_result = store.run_superagent_eval_cases(owner_email="owner@npt.sg", suite_id="suite-a", limit=1)
            missing_evidence_eval = store.run_superagent_eval_cases(
                owner_email="owner@npt.sg",
                cases=[{"question": "Completely unknown evidence", "task_type": "project_status", "expected_answer_points": ["missing"]}],
            )
            health = store.superagent_health(owner_email="owner@npt.sg")
            insufficient = store._superagent_quality_gate_from_eval({"case_count": 0, "passed_count": 0, "failed_count": 0, "results": []}, min_cases=2)

            private_item = store.query_work_memory(owner_email="owner@npt.sg", query="Follow up", visibility_scope="owner")[0]
            store.query_superagent_context = lambda **_kwargs: {  # type: ignore[method-assign]
                "query": "private",
                "task_type": "general",
                "visibility_scope": "team",
                "items": [private_item],
                "materialized": [],
            }
            privacy_eval = store.run_superagent_eval_cases(
                owner_email="owner@npt.sg",
                cases=[{"question": "private", "task_type": "general", "visibility_scope": "team", "expected_sources": ["gmail"]}],
            )

            self.assertTrue(follow_up["items"])
            self.assertIn("does not contain enough detail", direct_fallback["direct_answer"])
            self.assertIn("Materialized memory available", with_materialized["answer"])
            self.assertIn("attribution_unknown", attribution["quality_warnings"])
            self.assertEqual(len(stored), 3)
            self.assertLessEqual(eval_result["case_count"], 1)
            self.assertTrue(any(result["missing_evidence"] for result in missing_evidence_eval["results"]))
            self.assertIn("insufficient_gold_cases", insufficient["failure_summary"])
            self.assertEqual(health["guardrails"]["answer_contract_version"], work_memory.SUPERAGENT_ANSWER_CONTRACT_VERSION)
            self.assertTrue(any(result["privacy_risk"] for result in privacy_eval["results"]))

    def test_superagent_section_and_ranking_helpers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            items = [
                {
                    "item_id": "status",
                    "source_type": "team_dashboard",
                    "item_type": "project",
                    "summary": "Credit Risk CRMS status",
                    "content": "Credit Risk CRMS status.",
                    "visibility": VISIBILITY_TEAM,
                    "metadata": {"team_label": "Credit Risk"},
                },
                {
                    "item_id": "risk",
                    "source_type": "gmail",
                    "item_type": "risk",
                    "summary": "GRC blocker",
                    "content": "GRC blocker.",
                    "visibility": VISIBILITY_TEAM,
                    "metadata": {"team_key": "GRC"},
                },
                {
                    "item_id": "stakeholder",
                    "source_type": "source_code_qa",
                    "item_type": "stakeholder",
                    "summary": "Lead owns source code review",
                    "content": "Lead owns source code review.",
                    "visibility": VISIBILITY_TEAM,
                    "metadata": {},
                },
                {"item_id": "", "source_type": "", "item_type": "unknown", "summary": "", "content": "", "visibility": VISIBILITY_TEAM},
            ]
            many_projects = [
                {
                    "item_id": f"project-{index}",
                    "source_type": "team_dashboard",
                    "item_type": "project",
                    "summary": f"Project {index}",
                    "content": f"Project {index}",
                    "visibility": VISIBILITY_TEAM,
                    "metadata": {},
                }
                for index in range(6)
            ]
            many_followups = [
                {
                    "item_id": f"todo-{index}",
                    "source_type": "gmail",
                    "item_type": "todo",
                    "summary": f"Todo {index}",
                    "content": f"Todo {index}",
                    "visibility": VISIBILITY_TEAM,
                    "metadata": {},
                }
                for index in range(7)
            ]

            self.assertEqual(store._detect_entity_type("lead@npt.sg"), "person")
            self.assertEqual(store._detect_entity_type("BPMIS 225159"), "project")
            self.assertEqual(store._detect_entity_type("project PRD"), "project")
            self.assertEqual(
                store._canonical_key_from_candidates(
                    normalized_query="project",
                    entity_type="project",
                    candidates=[{"entity_type": "bpmis_id", "entity_key": "225159"}],
                ),
                "225159",
            )
            self.assertEqual(
                store._canonical_key_from_candidates(normalized_query="see AF-123", entity_type="project", candidates=[]),
                "AF-123",
            )
            self.assertEqual(
                store._canonical_key_from_candidates(normalized_query="BPMIS 225159", entity_type="project", candidates=[]),
                "225159",
            )
            self.assertEqual(
                store._canonical_key_from_candidates(normalized_query="lead@npt.sg", entity_type="person", candidates=[]),
                "lead@npt.sg",
            )
            self.assertEqual(store._domain_from_query("Money Lock scam control"), "Anti-Fraud")
            self.assertEqual(store._domain_from_query("credit underwriting"), "Credit Risk")
            self.assertEqual(store._domain_from_query("ops risk issue management"), "GRC")
            self.assertEqual(store._normalize_expected_sources(["Meeting", "Team Dashboard", "PRD", "SeaTalk", "Source Code"]), {"meeting_recorder", "team_dashboard", "bpmis", "jira", "confluence", "seatalk", "source_code_qa"})
            self.assertTrue(store._answer_point_matches("", "anything"))
            self.assertTrue(store._answer_point_matches("Credit Risk", "credit risk status"))
            self.assertFalse(store._answer_point_matches("的", "no tokens"))
            self.assertIn("...", store._truncate_text("abcdef", limit=5))
            self.assertEqual(store._evidence_excerpt({"visibility": VISIBILITY_PRIVATE, "content": "secret"}, query="", allow_private=False), "")
            self.assertEqual(store._evidence_excerpt({"visibility": VISIBILITY_TEAM, "content": ""}, query="", allow_private=True), "")
            self.assertIn("Credit Risk", store._evidence_excerpt(items[0], query="Credit", allow_private=True))
            self.assertIn("Private evidence", store._evidence_summary({"visibility": VISIBILITY_PRIVATE, "summary": "secret"}, include_private_summary=False)["summary"])
            self.assertEqual(store._answer_prefix({"source_type": "gmail_sent_monthly_report"}), "Final sent report says: ")
            self.assertEqual(store._answer_prefix({"source_type": "meeting_recorder", "metadata": {"attribution_scope": "owner_speech_candidate"}}), "Owner speech candidate says: ")
            self.assertEqual(store._answer_prefix({"source_type": "source_code_qa"}), "Technical evidence says: ")

            self.assertTrue(store._superagent_answer_sections(task_type="meeting_prep", query="Credit", evidence_items=items, owner_view=True))
            self.assertTrue(store._superagent_answer_sections(task_type="monthly_focus", query="Credit", evidence_items=items, owner_view=True))
            self.assertTrue(store._superagent_answer_sections(task_type="stakeholder_brief", query="Lead", evidence_items=items, owner_view=True))
            self.assertTrue(store._superagent_answer_sections(task_type="general", query="Lead", evidence_items=items, owner_view=True))
            self.assertEqual(
                len(store._superagent_answer_sections(task_type="project_status", query="Project", evidence_items=many_projects, owner_view=True)[0]["items"]),
                4,
            )
            self.assertEqual(
                len(store._superagent_answer_sections(task_type="follow_up", query="Todo", evidence_items=many_followups, owner_view=True)[0]["items"]),
                5,
            )
            self.assertEqual(store._superagent_item_line(items[-1], query="", owner_view=True), "")
            self.assertEqual(store._format_superagent_sections([{"title": "", "items": ["x"]}, {"title": "T", "items": [""]}]), "")
            self.assertEqual(store._domain_from_query("plain update"), "Other")
            self.assertEqual(store._rerank_items_for_query(items[:3], query="GRC", task_type="project_status", domain="GRC")[0]["item_id"], "risk")
            self.assertEqual(
                store._rerank_items_for_query(
                    [{"item_id": "ops", "source_type": "gmail", "item_type": "project", "summary": "Ops Risk review", "content": "", "visibility": VISIBILITY_TEAM, "metadata": {"team_label": "Ops Risk"}}],
                    query="outsourcing",
                    task_type="project_status",
                    domain="GRC",
                )[0]["item_id"],
                "ops",
            )
            self.assertEqual(
                store._rerank_items_for_query(
                    [{"item_id": "crms", "source_type": "gmail", "item_type": "project", "summary": "CRMS review", "content": "", "visibility": VISIBILITY_TEAM, "metadata": {"team_label": "CRMS"}}],
                    query="underwriting",
                    task_type="project_status",
                    domain="Credit Risk",
                )[0]["item_id"],
                "crms",
            )
            self.assertGreater(store._review_score({"source_type": "team_dashboard", "item_type": "key_project", "weight": 1, "metadata": {"is_key_project": True}}), 100)
            self.assertGreater(
                store._review_score({"source_type": "meeting_recorder", "item_type": "owner_speech_candidate", "weight": 1, "metadata": {"personal_profile_eligible": "candidate_after_review"}}),
                80,
            )
            stakeholders = store._stakeholders_from_items([{"entities": ["bad", {"entity_type": "team"}, {"entity_type": "person", "entity_key": "lead@npt.sg", "label": "Lead"}]}])
            self.assertEqual(stakeholders[0]["email"], "lead@npt.sg")
            self.assertEqual(
                store._evidence_excerpt({"visibility": VISIBILITY_TEAM, "content": "\nRelevant Credit Risk sentence."}, query="Credit", allow_private=True),
                "Relevant Credit Risk sentence.",
            )

    def test_candidate_refs_distill_and_query_fallbacks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WorkMemoryStore(Path(temp_dir) / "memory.db")
            store.record_memory_item(
                source_type="team_dashboard",
                source_id="project-1",
                item_type="project",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_TEAM,
                summary="Project Alpha",
                content="Project Alpha",
                metadata={"bpmis_id": "225159", "project_name": "Project Alpha", "jira_keys": ["AF-123"]},
                entities=[{"entity_type": "jira_key", "entity_key": "AF-123", "label": "AF-123"}],
            )
            store.record_memory_item(
                source_type="gmail",
                source_id="ignored-1",
                item_type="risk",
                owner_email="owner@npt.sg",
                visibility=VISIBILITY_PRIVATE,
                summary="Ignored Project Alpha risk",
                content="Ignored Project Alpha risk",
            )
            ignored = store.query_work_memory(owner_email="owner@npt.sg", query="Ignored")[0]
            store.record_memory_feedback(item_id=ignored["item_id"], action="ignore", owner_email="owner@npt.sg")

            self.assertIn("225159", store._candidate_project_refs(owner_email="owner@npt.sg"))
            self.assertEqual(store._candidate_project_refs(owner_email="owner@npt.sg", sources=["gmail"]), [])
            self.assertTrue(store._query_by_resolved_terms(query="", owner_email="owner@npt.sg", visibility_scope="owner", task_type="monthly_focus", limit=5))
            self.assertEqual(store._query_by_resolved_terms(query="", owner_email="owner@npt.sg", visibility_scope="owner", task_type="general", limit=5), [])
            self.assertTrue(store._query_by_resolved_terms(query="NoHit", owner_email="owner@npt.sg", visibility_scope="owner", task_type="monthly_focus", limit=5))
            self.assertTrue(store.distill_work_memory(owner_email="owner@npt.sg", sources=["team_dashboard"])["project_profiles"])
            store.query_work_memory = lambda **_kwargs: [  # type: ignore[method-assign]
                {"source_type": "team_dashboard", "metadata": {}, "entities": ["bad", {"entity_type": "project", "entity_key": "Project Beta"}]}
            ]
            self.assertEqual(store._candidate_project_refs(owner_email="owner@npt.sg"), ["Project Beta"])

    def test_memory_item_builders_cover_edge_inputs(self):
        dashboard_items = team_dashboard_memory_items(
            {
                "team_key": "AF",
                "label": "Anti-Fraud",
                "member_emails": ["pm@npt.sg"],
                "under_prd": ["bad", {}],
                "pending_live": [
                    {
                        "project_name": "No BPMIS Project",
                        "status": "Pending Live",
                        "matched_pm_emails": ["pm@npt.sg"],
                        "jira_tickets": [{"issue_id": "AF-321", "prd_links": [{"url": "https://confluence/pages/2"}]}],
                    }
                ],
            },
            owner_email="owner@npt.sg",
        )
        meeting_items = meeting_record_memory_items(
            {
                "record_id": "rec-edges",
                "owner_email": "owner@npt.sg",
                "title": "AF-123 225159 launch",
                "attendees": ["bad", {"email": "lead@npt.sg", "name": "Lead"}],
                "minutes": {
                    "status": "completed",
                    "markdown": "Intro\n## Risks\n* Approval block remains\n- \n## Follow-up\n* Confirm due date",
                },
                "transcript": {"status": "completed", "text": "BPMIS 225159 and AF-123 were discussed."},
            }
        )
        qa_item = work_memory.source_code_qa_memory_item(
            owner_email="owner@npt.sg",
            pm_team="",
            country="SG",
            question="How does Source Code QA work?",
            result={"matches": "bad", "citations": "bad", "summary": "It retrieves indexed evidence."},
        )
        record = SimpleNamespace(
            headers={"subject": "Project deadline", "to": "owner@npt.sg"},
            body_text="Launch target is June.",
            message_id="",
            thread_id="thread-1",
            label_ids=[],
            drive_links=[],
            attachments=[],
            internal_date=None,
        )
        gmail_item = gmail_message_memory_item(owner_email="owner@npt.sg", record=record)
        risk_record = SimpleNamespace(headers={"subject": "Risk delay", "to": "owner@npt.sg"}, body_text="There is a blocker risk.", message_id="risk", thread_id="thread-risk", label_ids=[], drive_links=[], attachments=[], internal_date=None)
        todo_record = SimpleNamespace(headers={"subject": "Next step", "to": "owner@npt.sg"}, body_text="Follow up with owner due Friday.", message_id="todo", thread_id="thread-todo", label_ids=[], drive_links=[], attachments=[], internal_date=None)
        drive_item = gmail_drive_link_memory_item(
            owner_email="owner@npt.sg",
            record=record,
            url="https://docs.google.com/document/d/1",
            access_status="permission_denied",
        )

        self.assertEqual(len(dashboard_items), 1)
        self.assertEqual(dashboard_items[0]["source_id"], "AF:pending_live:no bpmis project")
        self.assertTrue(any(entity["entity_type"] == "confluence_page" for entity in dashboard_items[0]["entities"]))
        self.assertTrue(any(item["item_type"] == "blocker" for item in meeting_items))
        self.assertTrue(any(entity["entity_type"] == "person" for entity in meeting_items[0]["entities"]))
        self.assertEqual(qa_item["entities"], [])
        self.assertEqual(qa_item["metadata"]["match_count"], 0)
        self.assertEqual(gmail_item["item_type"], "project")
        self.assertEqual(gmail_message_memory_item(owner_email="owner@npt.sg", record=risk_record)["item_type"], "risk")
        self.assertEqual(gmail_message_memory_item(owner_email="owner@npt.sg", record=todo_record)["item_type"], "todo")
        self.assertEqual(drive_item["ingestion_status"], "partial")

if __name__ == "__main__":
    unittest.main()
