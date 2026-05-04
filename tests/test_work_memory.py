from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bpmis_jira_tool.work_memory import (
    VISIBILITY_PRIVATE,
    VISIBILITY_TEAM,
    WorkMemoryStore,
    meeting_record_memory_items,
    sent_monthly_report_memory_item_from_gmail_record,
    team_dashboard_memory_items,
)
from bpmis_jira_tool.web import create_app, _is_sent_monthly_report_subject


class WorkMemoryStoreTests(unittest.TestCase):
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

    def test_sent_monthly_report_subject_requires_banking_product_update_format(self):
        self.assertTrue(_is_sent_monthly_report_subject("[Banking] Product Update (01 Apr - 30 Apr) - Anti-Fraud, Credit Risk & Ops Risk"))
        self.assertTrue(_is_sent_monthly_report_subject("[Banking] Product Update (1 Apr - 7 Apr) - Anti-Fraud, Credit Risk & Ops Risk"))
        self.assertTrue(_is_sent_monthly_report_subject("[Banking] Product Update (05 May - 11 May) - Anti-Fraud, Credit Risk & Ops Risk"))
        self.assertFalse(_is_sent_monthly_report_subject("Monthly Report 2026-05"))
        self.assertFalse(_is_sent_monthly_report_subject("[Banking] Product Update (01 Apr - 30 Apr) - Credit Risk"))


class WorkMemoryRouteTests(unittest.TestCase):
    def test_sent_monthly_report_ingestion_scans_gmail_sent_mail(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            }
            with patch.dict(os.environ, env, clear=False):
                app = create_app()

            class FakeGmailService:
                def _list_message_ids(self, *, query, max_messages):
                    return ["msg-1", "msg-2"] if "Product Update" in query else []

                def _fetch_message_full(self, message_id):
                    subject = "[Banking] Product Update (01 Apr - 30 Apr) - Anti-Fraud, Credit Risk & Ops Risk"
                    if message_id == "msg-2":
                        subject = "Monthly Report 2026-05"
                    return SimpleNamespace(
                        headers={
                            "from": "Xiaodong Zheng <xiaodong.zheng@npt.sg>",
                            "to": "leaders@npt.sg",
                            "subject": subject,
                        },
                        body_text="Final report body for AF-123 and BPMIS 225159.",
                        message_id=message_id,
                        internal_date=SimpleNamespace(isoformat=lambda: "2026-05-04T10:00:00+08:00"),
                    )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Owner"}
                    session["google_credentials"] = {"token": "x", "scopes": ["https://www.googleapis.com/auth/gmail.readonly"]}
                with patch("bpmis_jira_tool.web._build_gmail_dashboard_service", return_value=FakeGmailService()):
                    response = client.post("/api/work-memory/ingest-sent-monthly-reports")
                    duplicate_response = client.post("/api/work-memory/ingest-sent-monthly-reports")

                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertEqual(payload["matched"], 1)
                self.assertEqual(payload["recorded"], 1)

                recent = client.get("/api/work-memory/recent?item_type=curated_report").get_json()
                self.assertEqual(recent["items"][0]["source_type"], "gmail_sent_monthly_report")

                self.assertEqual(duplicate_response.status_code, 200)
                duplicate_payload = duplicate_response.get_json()
                self.assertEqual(duplicate_payload["duplicate"], 1)

                health = client.get("/api/work-memory/health").get_json()
                self.assertTrue(any(run["source_type"] == "gmail_sent_monthly_report_scan" for run in health["ingestion_runs"]))

    def test_superagent_query_route_is_readonly_and_requires_evidence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            }
            with patch.dict(os.environ, env, clear=False):
                app = create_app()

            with app.app_context():
                store = app.config["WORK_MEMORY_STORE"]
                store.record_memory_item(
                    source_type="team_dashboard",
                    source_id="project-1",
                    item_type="project",
                    owner_email="xiaodong.zheng@npt.sg",
                    visibility=VISIBILITY_TEAM,
                    summary="SLoan is under PRD",
                    content="SLoan is under PRD",
                )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Owner"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post("/api/superagent/query", json={"task_type": "project_status", "query": "SLoan"})

                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertTrue(payload["readonly"])
                self.assertTrue(payload["evidence"])
                self.assertIn("read-only", payload["answer"])

    def test_distill_route_materializes_project_profiles(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            }
            with patch.dict(os.environ, env, clear=False):
                app = create_app()

            with app.app_context():
                store = app.config["WORK_MEMORY_STORE"]
                store.record_memory_item(
                    source_type="team_dashboard",
                    source_id="project-1",
                    item_type="project",
                    owner_email="xiaodong.zheng@npt.sg",
                    visibility=VISIBILITY_TEAM,
                    summary="SLoan is under PRD",
                    content="SLoan is under PRD",
                    metadata={"project_name": "SLoan"},
                    entities=[{"entity_type": "project", "entity_key": "SLoan", "label": "SLoan", "relation": "describes"}],
                )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Owner"}
                    session["google_credentials"] = {"token": "x"}
                response = client.post("/api/work-memory/distill", json={"date_range": "90d"})

                self.assertEqual(response.status_code, 200)
                payload = response.get_json()
                self.assertTrue(payload["project_profiles"])
                health = client.get("/api/superagent/health").get_json()
                self.assertEqual(health["guardrails"]["external_writes_enabled"], False)
                self.assertGreaterEqual(health["materialized_count"], 1)

    def test_superagent_support_routes_expose_resolution_audit_explain_eval_and_incremental_ingest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env = {
                "TEAM_PORTAL_DATA_DIR": temp_dir,
                "TEAM_PORTAL_BASE_URL": "",
                "TEAM_PORTAL_CONFIG_ENCRYPTION_KEY": "",
            }
            with patch.dict(os.environ, env, clear=False):
                app = create_app()

            with app.app_context():
                store = app.config["WORK_MEMORY_STORE"]
                store.record_memory_item(
                    source_type="team_dashboard",
                    source_id="project-225159",
                    item_type="project",
                    owner_email="xiaodong.zheng@npt.sg",
                    visibility=VISIBILITY_TEAM,
                    summary="CRC Revamp is under PRD",
                    content="CRC Revamp is under PRD",
                    entities=[
                        {"entity_type": "project", "entity_key": "225159", "label": "CRC Revamp", "relation": "describes"},
                        {"entity_type": "bpmis_id", "entity_key": "225159", "label": "225159", "relation": "identifies"},
                    ],
                )

            with app.test_client() as client:
                with client.session_transaction() as session:
                    session["google_profile"] = {"email": "xiaodong.zheng@npt.sg", "name": "Owner"}
                    session["google_credentials"] = {"token": "x"}

                resolution = client.get("/api/work-memory/entity-resolution?q=CRC%20Revamp&entity_type=project")
                self.assertEqual(resolution.status_code, 200)
                self.assertEqual(resolution.get_json()["canonical_key"], "225159")

                query = client.post("/api/superagent/query", json={"task_type": "project_status", "query": "CRC Revamp"})
                self.assertEqual(query.status_code, 200)
                self.assertTrue(query.get_json()["audit"]["audit_id"])

                explain = client.post("/api/superagent/explain", json={"task_type": "project_status", "query": "CRC Revamp"})
                self.assertEqual(explain.status_code, 200)
                self.assertTrue(explain.get_json()["evidence"])

                audit = client.get("/api/superagent/audit")
                self.assertEqual(audit.status_code, 200)
                self.assertTrue(audit.get_json()["items"])

                eval_response = client.post(
                    "/api/superagent/eval",
                    json={
                        "cases": [
                            {
                                "question": "CRC Revamp status?",
                                "task_type": "project_status",
                                "expected_source_type": "team_dashboard",
                            }
                        ]
                    },
                )
                self.assertEqual(eval_response.status_code, 200)
                self.assertEqual(eval_response.get_json()["case_count"], 1)

                incremental = client.post("/api/work-memory/ingest-incremental", json={"window": "7d"})
                self.assertEqual(incremental.status_code, 200)
                self.assertIn("sources", incremental.get_json())


if __name__ == "__main__":
    unittest.main()
