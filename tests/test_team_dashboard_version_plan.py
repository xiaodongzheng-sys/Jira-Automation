from __future__ import annotations

from datetime import datetime
import unittest

from bpmis_jira_tool.team_dashboard_version_plan import (
    PIPELINE_SEED_ROWS,
    normalize_version_plan_state,
    update_version_plan_cell,
    update_version_plan_rows,
    version_plan_auto_sync_attempted_today,
    version_plan_payload,
    version_plan_sync,
    version_plan_synced_today,
)


class FakeBPMISVersionPlanClient:
    def __init__(self) -> None:
        self.search_calls: list[str] = []
        self.release_window_calls: list[dict] = []

    def search_versions(self, query: str) -> list[dict]:
        self.search_calls.append(query)
        if query == "AF_":
            return [
                {
                    "id": "af-20260520",
                    "fullName": "AF_1.0.76_20260520",
                    "timeline": {"prdDueDate": "2026-05-10", "release": "2026-05-20"},
                    "timelineStart": "2026-05-01T00:00:00+08:00",
                    "timelineEnd": "2026-05-20T00:00:00+08:00",
                },
                {
                    "id": "af-20261001",
                    "fullName": "AF_1.0.99_20261001",
                    "timelineStart": "2026-09-01T00:00:00+08:00",
                    "timelineEnd": "2026-10-01T00:00:00+08:00",
                },
            ]
        if query == "DBPSG_":
            return [
                {
                    "id": "dbpsg-0528",
                    "fullName": "DBPSG_v2.85_0528",
                    "timelineEnd": "2026-05-28T00:00:00+08:00",
                }
            ]
        if query == "DBPID_":
            return [
                {
                    "id": "dbpid-0528",
                    "fullName": "DBPID_v3.41_0528",
                    "timelineEnd": "2026-05-28T00:00:00+08:00",
                }
            ]
        if query == "DBPPH_":
            return [
                {
                    "id": "dbpph-0528",
                    "fullName": "DBPPH_v3.17_0528",
                    "timelineEnd": "2026-05-28T00:00:00+08:00",
                }
            ]
        return []

    def list_issues_for_version(self, version_id: str) -> list[dict]:
        if version_id != "dbpsg-0528":
            return []
        return [
            {
                "jiraKey": "SPDBP-94945",
                "summary": "[Feature] Antifraud - UIUX Improvement for AMR",
                "status": "Developing",
                "reporter": {"email": "chang.wang@npt.sg"},
                "jiraRegionalPmPicId": [{"email": "chang.wang@npt.sg"}],
                "parentIds": ["biz-1"],
            },
            {
                "jiraKey": "SPDBP-rene",
                "summary": "[Feature] Rene owner mapping",
                "status": "Developing",
                "reporter": {"email": "chongzj@npt.sg"},
                "jiraRegionalPmPicId": [{"email": "chongzj@npt.sg"}],
                "parentIds": ["biz-2"],
            },
            {
                "jiraKey": "SPDBP-closed",
                "summary": "Closed task",
                "status": "Closed",
                "reporter": {"email": "chang.wang@npt.sg"},
            },
            {
                "jiraKey": "SPDBP-xiaodong",
                "summary": "Excluded reporter",
                "status": "Developing",
                "reporter": {"email": "xiaodong.zheng@npt.sg"},
            },
        ]

    def list_jira_tasks_created_by_emails(self, emails: list[str], **kwargs) -> list[dict]:
        self.release_window_calls.append({"emails": emails, **kwargs})
        return [
            {
                "jira_id": "SPDBP-94945",
                "jira_title": "[Feature] Antifraud - UIUX Improvement for AMR",
                "status": "Developing",
                "pm_email": "chang.wang@npt.sg",
                "market": "SG",
                "parent_project": {"priority": "P0", "market": "SG"},
            },
            {
                "jira_id": "SPDBP-rene",
                "jira_title": "[Feature] Rene owner mapping",
                "status": "Developing",
                "pm_email": "chongzj@npt.sg",
                "market": "ID",
                "parent_project": {"priority": "P1", "market": "ID"},
            },
            {
                "jira_id": "SPDBP-closed",
                "jira_title": "Closed task",
                "status": "Closed",
                "pm_email": "chang.wang@npt.sg",
            },
        ]

    def get_issue_detail(self, issue_id: str) -> dict:
        if issue_id != "biz-1":
            if issue_id == "biz-2":
                return {"bizPriorityId": "P1", "market": "ID"}
            raise AssertionError(f"Unexpected issue id: {issue_id}")
        return {"bizPriorityId": "P0", "market": "SG"}


class FakeNotStartedDevVersionPlanClient(FakeBPMISVersionPlanClient):
    def __init__(self) -> None:
        super().__init__()
        self.list_issue_calls: list[str] = []

    def search_versions(self, query: str) -> list[dict]:
        self.search_calls.append(query)
        if query == "AF_":
            return [
                {
                    "id": "af-20260626",
                    "fullName": "AF_v1.0.82_20260626",
                    "timeline": {"prdDueDate": "2026-05-29", "release": "2026-06-26"},
                    "timelineStart": "2026-06-01T00:00:00+08:00",
                    "timelineEnd": "2026-06-26T00:00:00+08:00",
                }
            ]
        if query in {"DBPSG_", "DBPID_", "DBPPH_"}:
            return [
                {
                    "id": f"{query.lower()}0730",
                    "fullName": f"{query}v1.00_0730",
                    "timelineEnd": "2026-07-30T00:00:00+08:00",
                }
            ]
        return []

    def list_issues_for_version(self, version_id: str) -> list[dict]:
        self.list_issue_calls.append(version_id)
        return [{"jiraKey": "SPDBP-should-not-sync", "summary": "Not started Dev"}]


class FakeEmptyVersionPlanClient(FakeBPMISVersionPlanClient):
    def search_versions(self, query: str) -> list[dict]:
        self.search_calls.append(query)
        return []


class TeamDashboardVersionPlanTest(unittest.TestCase):
    def test_pipeline_seed_is_global_and_not_deduped(self) -> None:
        plan = normalize_version_plan_state({})
        rows = plan["af"]["pipeline_rows"]

        self.assertEqual(len(rows), len(PIPELINE_SEED_ROWS))
        self.assertEqual(len(rows), 45)
        self.assertEqual(rows[0]["feature"], "[ID] Corporate Internet Banking Phase 2")
        self.assertEqual(rows[-1]["feature"], "[ID] Credit Card - Txn Scenario / In-App Auth / Rules Changes")

    def test_sync_builds_active_bundle_seen_versions_and_synced_rows(self) -> None:
        config = {
            "version_plan": {
                "af": {
                    "bundles": {
                        "af-20260520": {
                            "manual_rows": [
                                {"row_id": "manual-1", "feature": "Manual item", "priority": "P1", "pm": ["Zoey"]}
                            ]
                        }
                    },
                    "pipeline_rows": [{"row_id": "pipe-1", "feature": "Keep pipeline", "priority": "SP"}],
                }
            }
        }
        client = FakeBPMISVersionPlanClient()
        synced = version_plan_sync(
            config,
            client,
            now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
        )
        payload = version_plan_payload(synced, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))

        self.assertTrue(version_plan_synced_today(synced, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00")))
        self.assertEqual([bundle["af_version_name"] for bundle in payload["bundles"]], ["AF_1.0.76_20260520"])
        bundle = payload["bundles"][0]
        self.assertEqual(bundle["prd_deadline_date"], "2026-05-10")
        self.assertEqual(bundle["prd_initial_date"], "2026-05-06")
        self.assertEqual(bundle["prd_final_date"], "2026-05-08")
        self.assertEqual(bundle["synced_rows"][0]["jira_id"], "SPDBP-94945")
        self.assertEqual(bundle["synced_rows"][0]["market"], "SG")
        self.assertEqual(bundle["synced_rows"][0]["priority"], "P0")
        self.assertEqual(bundle["synced_rows"][0]["pm"], ["Wang Chang"])
        self.assertEqual(len(client.release_window_calls), 1)
        self.assertEqual(client.release_window_calls[0]["release_after"], "2026-05-20")
        self.assertEqual(client.release_window_calls[0]["release_before"], "2026-05-28")
        rene_row = next(row for row in bundle["synced_rows"] if row["jira_id"] == "SPDBP-rene")
        self.assertEqual(rene_row["pm"], ["Rene"])
        self.assertEqual(bundle["manual_rows"][0]["feature"], "Manual item")
        self.assertEqual(payload["pipeline_rows"][0]["feature"], "Keep pipeline")

    def test_seen_past_version_moves_to_archived_without_manual_rows(self) -> None:
        config = {
            "version_plan": {
                "af": {
                    "seen_versions": {
                        "af-20260520": {
                            "version_id": "af-20260520",
                            "version_name": "AF_1.0.76_20260520",
                            "release_date": "2026-05-20",
                            "timeline_start": "2026-05-01T00:00:00+08:00",
                        }
                    },
                    "bundles": {
                        "af-20260520": {
                            "manual_rows": [
                                {"row_id": "manual-1", "feature": "Should not archive", "priority": "P1"}
                            ]
                        }
                    },
                    "pipeline_rows": [{"row_id": "pipe-1", "feature": "Pipeline", "priority": "P2"}],
                }
            }
        }
        synced = version_plan_sync(
            config,
            FakeBPMISVersionPlanClient(),
            now=datetime.fromisoformat("2026-05-22T09:00:00+08:00"),
        )
        payload = version_plan_payload(synced, now=datetime.fromisoformat("2026-05-22T09:00:00+08:00"))

        self.assertEqual(payload["bundles"], [])
        self.assertEqual(len(payload["archived_bundles"]), 1)
        archived = payload["archived_bundles"][0]
        self.assertEqual(archived["af_version_name"], "AF_1.0.76_20260520")
        self.assertEqual(archived["manual_rows"], [])
        self.assertEqual(archived["synced_rows"][0]["jira_id"], "SPDBP-94945")

    def test_manual_cell_add_delete_and_priority_order_persist(self) -> None:
        config = normalize_version_plan_state(
            {"af": {"pipeline_rows": [{"row_id": "pipe-1", "feature": "Existing", "priority": "P0"}]}}
        )
        wrapped = {"version_plan": config}
        first_row_id = config["af"]["pipeline_rows"][0]["row_id"]

        wrapped = update_version_plan_cell(
            wrapped,
            {"scope": "pipeline", "row_id": first_row_id, "field": "priority", "value": "P3"},
        )
        wrapped = update_version_plan_rows(wrapped, {"scope": "pipeline", "action": "add"})
        rows = wrapped["version_plan"]["af"]["pipeline_rows"]
        new_row_id = rows[-1]["row_id"]
        wrapped = update_version_plan_cell(
            wrapped,
            {"scope": "pipeline", "row_id": new_row_id, "field": "priority", "value": "SP"},
        )
        payload = version_plan_payload(wrapped)

        self.assertEqual(payload["pipeline_rows"][0]["row_id"], new_row_id)
        self.assertEqual(payload["pipeline_rows"][-1]["row_id"], first_row_id)

        wrapped = update_version_plan_rows(wrapped, {"scope": "pipeline", "action": "delete", "row_id": new_row_id})
        self.assertNotIn(
            new_row_id,
            [row["row_id"] for row in wrapped["version_plan"]["af"]["pipeline_rows"]],
        )

    def test_pipeline_row_can_move_to_version_bundle(self) -> None:
        config = normalize_version_plan_state(
            {
                "af": {
                    "bundles": {
                        "af-1": {
                            "version_id": "af-1",
                            "version_name": "AF_1.0.84_20260724",
                            "release_date": "2026-07-24",
                            "manual_rows": [{"row_id": "bundle-1", "feature": "Bundle item", "priority": "P0"}],
                        }
                    },
                    "pipeline_rows": [{"row_id": "pipe-1", "feature": "Pipeline item", "priority": "P1"}],
                }
            }
        )
        wrapped = {"version_plan": config}

        wrapped = update_version_plan_rows(
            wrapped,
            {
                "action": "move",
                "row_id": "pipe-1",
                "source_scope": "pipeline",
                "target_scope": "bundle",
                "target_version_id": "af-1",
            },
        )
        payload = version_plan_payload(wrapped, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))
        bundle = payload["bundles"][0]

        self.assertEqual(payload["pipeline_rows"], [])
        self.assertEqual([row["row_id"] for row in bundle["manual_rows"]], ["bundle-1", "pipe-1"])
        self.assertEqual(bundle["manual_rows"][1]["feature"], "Pipeline item")

    def test_not_started_dev_version_is_manual_only_after_sync(self) -> None:
        config = {
            "version_plan": {
                "af": {
                    "bundles": {
                        "af-20260626": {
                            "manual_rows": [
                                {"row_id": "manual-1", "feature": "[ID][PH] AMR Fix", "priority": "P1"}
                            ],
                            "synced_rows": [
                                {"row_id": "sync-old", "jira_id": "SPDBP-old", "jira_summary": "Old synced row"}
                            ],
                        }
                    },
                    "pipeline_rows": [],
                }
            }
        }
        client = FakeNotStartedDevVersionPlanClient()

        synced = version_plan_sync(
            config,
            client,
            now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
        )
        payload = version_plan_payload(synced, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))
        bundle = payload["bundles"][0]

        self.assertFalse(bundle["in_dev"])
        self.assertEqual(bundle["af_version_name"], "AF_v1.0.82_20260626")
        self.assertEqual(bundle["synced_rows"], [])
        self.assertEqual(bundle["manual_rows"][0]["feature"], "[ID][PH] AMR Fix")
        self.assertEqual(client.list_issue_calls, [])
        self.assertEqual(client.release_window_calls, [])

    def test_not_started_dev_version_hides_cached_synced_rows_without_sync(self) -> None:
        payload = version_plan_payload(
            {
                "version_plan": {
                    "af": {
                        "bundles": {
                            "af-20260626": {
                                "version_id": "af-20260626",
                                "version_name": "AF_v1.0.82_20260626",
                                "release_date": "2026-06-26",
                                "prd_final_date": "2026-05-27",
                                "synced_at": "2026-05-16 08:00:00 SGT",
                                "synced_rows": [
                                    {
                                        "row_id": "sync-old",
                                        "jira_id": "SPDBP-old",
                                        "jira_summary": "Old cached row",
                                    }
                                ],
                                "manual_rows": [
                                    {"row_id": "manual-1", "feature": "[ID][PH] AMR Fix", "priority": "P1"}
                                ],
                            }
                        }
                    }
                }
            },
            now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
        )

        bundle = payload["bundles"][0]

        self.assertEqual(bundle["af_version_name"], "AF_v1.0.82_20260626")
        self.assertEqual(bundle["synced_rows"], [])
        self.assertEqual(bundle["synced_at"], "")
        self.assertEqual(bundle["manual_rows"][0]["feature"], "[ID][PH] AMR Fix")

    def test_synced_jira_remarks_are_editable_and_preserved_after_sync(self) -> None:
        config = {
            "version_plan": {
                "af": {
                    "bundles": {
                        "af-20260520": {
                            "synced_rows": [
                                {
                                    "row_id": "sync-af-20260520-SPDBP-94945",
                                    "jira_id": "SPDBP-94945",
                                    "jira_summary": "Old summary",
                                    "remarks": "Keep this note",
                                }
                            ]
                        }
                    },
                    "pipeline_rows": [],
                }
            }
        }

        synced = version_plan_sync(
            config,
            FakeBPMISVersionPlanClient(),
            now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
        )
        payload = version_plan_payload(synced, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))
        row = next(row for row in payload["bundles"][0]["synced_rows"] if row["jira_id"] == "SPDBP-94945")
        self.assertEqual(row["remarks"], "Keep this note")

        updated = update_version_plan_cell(
            synced,
            {
                "scope": "bundle",
                "version_id": "af-20260520",
                "row_id": row["row_id"],
                "field": "remarks",
                "value": "Updated note",
            },
        )
        payload = version_plan_payload(updated, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))
        row = next(row for row in payload["bundles"][0]["synced_rows"] if row["jira_id"] == "SPDBP-94945")
        self.assertEqual(row["remarks"], "Updated note")

    def test_empty_bpmis_version_search_preserves_cached_bundles(self) -> None:
        config = {
            "version_plan": {
                "af": {
                    "bundles": {
                        "af-cached": {
                            "version_id": "af-cached",
                            "version_name": "AF_cached",
                            "release_date": "2026-06-26",
                            "manual_rows": [{"row_id": "manual-1", "feature": "Cached manual", "priority": "P1"}],
                            "synced_rows": [{"row_id": "sync-1", "jira_id": "SPDBP-1", "jira_summary": "Cached Jira"}],
                        }
                    },
                    "pipeline_rows": [{"row_id": "pipe-1", "feature": "Pipeline", "priority": "P0"}],
                }
            }
        }

        synced = version_plan_sync(
            config,
            FakeEmptyVersionPlanClient(),
            now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
        )
        payload = version_plan_payload(synced, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))

        self.assertEqual(payload["sync_state"]["state"], "error")
        self.assertIn("cached Version Plan data was preserved", payload["sync_state"]["error"])
        self.assertEqual(payload["bundles"][0]["af_version_name"], "AF_cached")
        self.assertEqual(payload["bundles"][0]["manual_rows"][0]["feature"], "Cached manual")
        self.assertEqual(payload["bundles"][0]["synced_rows"][0]["jira_id"], "SPDBP-1")

    def test_auto_sync_attempt_guard_blocks_same_day_error_loop(self) -> None:
        config = {
            "version_plan": {
                "af": {
                    "sync_state": {
                        "state": "error",
                        "started_at": "2026-05-16T20:50:29+08:00",
                        "finished_at": "2026-05-16T20:50:31+08:00",
                        "error": "No AF versions were returned from BPMIS.",
                    }
                }
            }
        }

        self.assertTrue(
            version_plan_auto_sync_attempted_today(
                config,
                now=datetime.fromisoformat("2026-05-16T21:00:00+08:00"),
            )
        )
        self.assertFalse(
            version_plan_auto_sync_attempted_today(
                config,
                now=datetime.fromisoformat("2026-05-17T09:00:00+08:00"),
            )
        )


if __name__ == "__main__":
    unittest.main()
