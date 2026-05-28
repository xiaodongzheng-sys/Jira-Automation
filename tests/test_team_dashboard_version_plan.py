from __future__ import annotations

from datetime import date, datetime
import unittest

import bpmis_jira_tool.team_dashboard_version_plan as vplan
from bpmis_jira_tool.team_dashboard_version_plan import (
    PIPELINE_SEED_ROWS,
    append_version_plan_audit,
    merge_version_plan_editable_state,
    mark_version_plan_sync_error,
    mark_version_plan_sync_running,
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
                    "id": "dbpsg-0525",
                    "fullName": "DBPSG_v2.85_0525",
                    "timelineEnd": "2026-05-25T00:00:00+08:00",
                },
                {
                    "id": "dbpsg-0526-adhoc",
                    "fullName": "DBPSG_v2.84.1_0526_adhoc",
                    "timelineEnd": "2026-05-26T00:00:00+08:00",
                },
                {
                    "id": "dbpsg-0526",
                    "fullName": "DBPSG_v2.85_0526",
                    "timelineEnd": "2026-05-26T00:00:00+08:00",
                },
                {
                    "id": "dbpsg-0528",
                    "fullName": "DBPSG_v2.85_0528",
                    "timelineEnd": "2026-05-28T00:00:00+08:00",
                }
            ]
        if query == "DBPID_":
            return [
                {
                    "id": "dbpid-0526",
                    "fullName": "DBPID_v3.41_0526",
                    "timelineEnd": "2026-05-26T00:00:00+08:00",
                },
                {
                    "id": "dbpid-0528",
                    "fullName": "DBPID_v3.41_0528",
                    "timelineEnd": "2026-05-28T00:00:00+08:00",
                }
            ]
        if query == "DBPPH_":
            return [
                {
                    "id": "dbpph-0526",
                    "fullName": "DBPPH_v3.17_0526",
                    "timelineEnd": "2026-05-26T00:00:00+08:00",
                },
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
                "version": "AF_1.0.76_20260520",
                "parent_project": {"priority": "P0", "market": "SG"},
            },
            {
                "jira_id": "SPDBK-130825",
                "jira_title": "[Feature] Rene owner mapping",
                "status": "Developing",
                "pm_email": "chongzj@npt.sg",
                "market": "ID",
                "version": "DBPID_v3.41_0526",
                "parent_project": {"priority": "P1", "market": "ID"},
            },
            {
                "jira_id": "SGDB-75128",
                "jira_title": "[Feature] SG owner mapping",
                "status": "Developing",
                "pm_email": "zoey.luxy@npt.sg",
                "market": "PH",
                "version": "DBPSG_v2.85_0526",
                "parent_project": {"priority": "P0", "market": "PH"},
            },
            {
                "jira_id": "SGDB-wrong-version",
                "jira_title": "Different Version",
                "status": "Developing",
                "pm_email": "zoey.luxy@npt.sg",
                "market": "SG",
                "version": "DBPSG_v2.85_0608",
                "parent_project": {"priority": "SP", "market": "SG"},
            },
            {
                "jira_id": "SPDBP-closed",
                "jira_title": "Closed task",
                "status": "Closed",
                "pm_email": "chang.wang@npt.sg",
                "version": "AF_1.0.76_20260520",
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
        self.assertEqual(bundle["synced_rows"][0]["market"], "Regional")
        self.assertEqual(bundle["synced_rows"][0]["priority"], "P0")
        self.assertEqual(bundle["synced_rows"][0]["pm"], ["Wang Chang"])
        self.assertEqual(bundle["synced_rows"][0]["productization_efforts"], "Y")
        self.assertEqual([row["pm"][0] for row in bundle["synced_rows"] if row["priority"] == "P0"], ["Wang Chang", "Zoey"])
        sg_row = next(row for row in bundle["synced_rows"] if row["jira_id"] == "SGDB-75128")
        self.assertEqual(sg_row["market"], "SG")
        self.assertEqual(sg_row["productization_efforts"], "N")
        self.assertNotIn("SGDB-wrong-version", [row["jira_id"] for row in bundle["synced_rows"]])
        self.assertEqual(bundle["mapped_versions"]["DBPSG"]["version_name"], "DBPSG_v2.85_0526")
        self.assertEqual(len(client.release_window_calls), 1)
        self.assertEqual(client.release_window_calls[0]["release_after"], "2026-05-20")
        self.assertEqual(client.release_window_calls[0]["release_before"], "2026-05-26")
        rene_row = next(row for row in bundle["synced_rows"] if row["jira_id"] == "SPDBK-130825")
        self.assertEqual(rene_row["pm"], ["Rene"])
        self.assertEqual(rene_row["market"], "ID")
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

    def test_manual_rows_can_move_between_pipeline_and_version_bundles(self) -> None:
        config = normalize_version_plan_state(
            {
                "af": {
                    "bundles": {
                        "af-1": {
                            "version_id": "af-1",
                            "version_name": "AF_1.0.84_20260724",
                            "release_date": "2026-07-24",
                            "manual_rows": [{"row_id": "bundle-1", "feature": "Bundle item", "priority": "P0"}],
                        },
                        "af-2": {
                            "version_id": "af-2",
                            "version_name": "AF_1.0.85_20260807",
                            "release_date": "2026-08-07",
                            "manual_rows": [{"row_id": "bundle-2", "feature": "Second bundle item", "priority": "P1"}],
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
        wrapped = update_version_plan_rows(
            wrapped,
            {
                "action": "move",
                "row_id": "bundle-1",
                "source_scope": "bundle",
                "source_version_id": "af-1",
                "target_scope": "bundle",
                "target_version_id": "af-2",
                "target_before_row_id": "bundle-2",
            },
        )
        wrapped = update_version_plan_rows(
            wrapped,
            {
                "action": "move",
                "row_id": "pipe-1",
                "source_scope": "bundle",
                "source_version_id": "af-1",
                "target_scope": "pipeline",
            },
        )
        payload = version_plan_payload(wrapped, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))
        first_bundle, second_bundle = payload["bundles"]

        self.assertEqual([row["row_id"] for row in payload["pipeline_rows"]], ["pipe-1"])
        self.assertEqual(first_bundle["manual_rows"], [])
        self.assertEqual([row["row_id"] for row in second_bundle["manual_rows"]], ["bundle-1", "bundle-2"])

    def test_sync_result_preserves_concurrent_manual_moves_and_synced_remarks(self) -> None:
        synced_config = {
            "version_plan": normalize_version_plan_state(
                {
                    "af": {
                        "bundles": {
                            "af-1": {
                                "version_id": "af-1",
                                "version_name": "AF_1.0.84_20260724",
                                "release_date": "2026-07-24",
                                "manual_rows": [],
                                "synced_rows": [
                                    {
                                        "row_id": "sync-af-1-SPDBP-1",
                                        "jira_id": "SPDBP-1",
                                        "jira_summary": "Synced row",
                                        "remarks": "old",
                                    }
                                ],
                            }
                        },
                        "pipeline_rows": [{"row_id": "pipe-1", "feature": "Pipeline item", "priority": "P1"}],
                        "sync_state": {"state": "fresh_today", "last_synced_date_sgt": "2026-05-17"},
                    }
                }
            )
        }
        current_config = {
            "version_plan": normalize_version_plan_state(
                {
                    "af": {
                        "bundles": {
                            "af-1": {
                                "version_id": "af-1",
                                "version_name": "AF_1.0.84_20260724",
                                "release_date": "2026-07-24",
                                "manual_rows": [{"row_id": "pipe-1", "feature": "Pipeline item", "priority": "P1"}],
                                "synced_rows": [
                                    {
                                        "row_id": "sync-af-1-SPDBP-1",
                                        "jira_id": "SPDBP-1",
                                        "jira_summary": "Synced row",
                                        "remarks": "current note",
                                    }
                                ],
                            }
                        },
                        "pipeline_rows": [],
                        "sync_state": {"state": "running"},
                    }
                }
            )
        }

        merged = merge_version_plan_editable_state(synced_config, current_config)
        af = merged["version_plan"]["af"]

        self.assertEqual(af["sync_state"]["state"], "fresh_today")
        self.assertEqual(af["pipeline_rows"], [])
        self.assertEqual([row["row_id"] for row in af["bundles"]["af-1"]["manual_rows"]], ["pipe-1"])
        self.assertEqual(af["bundles"]["af-1"]["synced_rows"][0]["remarks"], "current note")

    def test_manual_rows_sort_by_priority_then_manual_order(self) -> None:
        payload = version_plan_payload(
            {
                "version_plan": {
                    "af": {
                        "pipeline_rows": [
                            {"row_id": "zoey", "feature": "Zoey P0", "priority": "P0", "pm": ["Zoey"], "sort_order": 0},
                            {"row_id": "rene", "feature": "Rene P0", "priority": "P0", "pm": ["Rene"], "sort_order": 1},
                            {"row_id": "sp", "feature": "SP item", "priority": "SP", "pm": ["TBC"], "sort_order": 2},
                            {"row_id": "wang", "feature": "Wang P0", "priority": "P0", "pm": ["Wang Chang"], "sort_order": 3},
                        ]
                    }
                }
            },
            now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
        )

        self.assertEqual([row["row_id"] for row in payload["pipeline_rows"]], ["sp", "zoey", "rene", "wang"])
        self.assertEqual(payload["pipeline_rows"][0]["pm"], [])

    def test_pipeline_reorder_persists_across_different_pm_values(self) -> None:
        config = {
            "version_plan": {
                "af": {
                    "pipeline_rows": [
                        {"row_id": "ker-yin", "feature": "Ker Yin item", "priority": "SP", "pm": ["Ker Yin"], "sort_order": 0},
                        {"row_id": "rene", "feature": "Rene item", "priority": "SP", "pm": ["Rene"], "sort_order": 1},
                        {"row_id": "zoey", "feature": "Zoey item", "priority": "SP", "pm": ["Zoey"], "sort_order": 2},
                    ]
                }
            }
        }

        updated = update_version_plan_rows(
            config,
            {"scope": "pipeline", "action": "reorder", "row_ids": ["zoey", "ker-yin", "rene"]},
        )
        payload = version_plan_payload(updated, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))

        self.assertEqual([row["row_id"] for row in payload["pipeline_rows"]], ["zoey", "ker-yin", "rene"])

    def test_manual_pm_options_exclude_tbc_and_keep_first_valid_value(self) -> None:
        payload = version_plan_payload(
            {
                "version_plan": {
                    "af": {
                        "pipeline_rows": [
                            {
                                "row_id": "multi-pm",
                                "feature": "Multi PM item",
                                "priority": "P0",
                                "pm": ["Wang Chang", "Ker Yin", "TBC", "xiaodong.zheng@npt.sg"],
                            }
                        ]
                    }
                }
            },
            now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
        )

        self.assertNotIn("TBC", payload["pm_options"])
        self.assertEqual(payload["pipeline_rows"][0]["pm"], ["Wang Chang"])

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
                                },
                                {
                                    "row_id": "sync-af-20260520-SPDBP-deleted",
                                    "jira_id": "SPDBP-deleted",
                                    "jira_summary": "No longer in Jira response",
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
        jira_ids = [row["jira_id"] for row in payload["bundles"][0]["synced_rows"]]
        self.assertNotIn("SPDBP-deleted", jira_ids)
        row = next(row for row in payload["bundles"][0]["synced_rows"] if row["jira_id"] == "SPDBP-94945")
        self.assertEqual(row["jira_summary"], "[Feature] Antifraud - UIUX Improvement for AMR")
        self.assertEqual(row["market"], "Regional")
        self.assertEqual(row["productization_efforts"], "Y")
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

    def test_version_plan_audit_log_is_global_and_capped(self) -> None:
        config = {"version_plan": normalize_version_plan_state({})}

        for index in range(505):
            config = append_version_plan_audit(
                config,
                action="row_add",
                actor={"email": "teammate@npt.sg", "name": "Teammate"},
                details={"scope": "pipeline", "row_id": f"manual-{index}", "value": "x" * 600},
            )

        audit_log = config["version_plan"]["af"]["audit_log"]
        self.assertEqual(len(audit_log), 500)
        self.assertEqual(audit_log[0]["details"]["row_id"], "manual-5")
        self.assertEqual(audit_log[-1]["actor"]["email"], "teammate@npt.sg")
        self.assertEqual(len(audit_log[-1]["details"]["value"]), 500)

    def test_merge_preserves_latest_audit_log_during_sync(self) -> None:
        synced = {"version_plan": normalize_version_plan_state({})}
        current = append_version_plan_audit(
            {"version_plan": normalize_version_plan_state({})},
            action="cell_update",
            actor={"email": "teammate@npt.sg"},
            details={"scope": "pipeline", "field": "remarks"},
        )

        merged = merge_version_plan_editable_state(synced, current)

        self.assertEqual(merged["version_plan"]["af"]["audit_log"][0]["action"], "cell_update")

    def test_sync_state_and_normalization_edge_branches(self) -> None:
        self.assertEqual(vplan.singapore_today(datetime(2026, 5, 16, 23, 30)), date(2026, 5, 16))

        normalized = normalize_version_plan_state(
            {
                "af": {
                    "bundles": {
                        "": {"manual_rows": []},
                        "bad": "not-a-bundle",
                        "good": {"manual_rows": [{"row_id": "m1", "feature": "Good"}]},
                    },
                    "pipeline_rows": [],
                }
            }
        )
        self.assertEqual(list(normalized["af"]["bundles"]), ["good"])

        running = mark_version_plan_sync_running({"version_plan": normalized}, message="Manual sync.")
        self.assertEqual(running["version_plan"]["af"]["sync_state"]["state"], "running")
        self.assertEqual(running["version_plan"]["af"]["sync_state"]["message"], "Manual sync.")

        failed = mark_version_plan_sync_error(running, "")
        self.assertEqual(failed["version_plan"]["af"]["sync_state"]["state"], "error")
        self.assertEqual(failed["version_plan"]["af"]["sync_state"]["error"], "Sync failed.")

        self.assertTrue(
            version_plan_auto_sync_attempted_today(
                {"version_plan": {"af": {"sync_state": {"last_synced_date_sgt": "2026-05-16"}}}},
                now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
            )
        )
        self.assertFalse(
            version_plan_auto_sync_attempted_today(
                {"version_plan": {"af": {"sync_state": {"state": "running"}}}},
                now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"),
            )
        )

    def test_version_plan_sync_and_merge_preserve_manual_or_safe_fallback_edges(self) -> None:
        class RaisingSearchClient:
            def search_versions(self, query: str) -> list[dict]:
                raise RuntimeError(query)

        synced = version_plan_sync({}, RaisingSearchClient(), now=datetime(2026, 5, 16, 9, 0))
        payload = version_plan_payload(synced, now=datetime.fromisoformat("2026-05-16T09:00:00+08:00"))

        self.assertEqual(payload["sync_state"]["state"], "error")
        self.assertIn("No AF versions", payload["sync_state"]["error"])

        merged = merge_version_plan_editable_state(
            {"version_plan": normalize_version_plan_state({"af": {"bundles": {}}})},
            {
                "version_plan": normalize_version_plan_state(
                    {
                        "af": {
                            "bundles": {
                                "af-manual": {
                                    "version_id": "af-manual",
                                    "version_name": "AF_manual",
                                    "release_date": "2026-07-01",
                                    "manual_rows": [{"row_id": "manual-only", "feature": "Manual only"}],
                                    "synced_rows": [{"row_id": "sync-1", "jira_id": "SGDB-1"}],
                                }
                            }
                        }
                    }
                )
            },
        )

        self.assertIn("af-manual", merged["version_plan"]["af"]["bundles"])
        self.assertEqual(merged["version_plan"]["af"]["bundles"]["af-manual"]["manual_rows"][0]["row_id"], "manual-only")

    def test_update_cell_and_row_error_edges(self) -> None:
        config = {
            "version_plan": normalize_version_plan_state(
                {
                    "af": {
                        "bundles": {
                            "af-1": {
                                "version_id": "af-1",
                                "version_name": "AF_1",
                                "release_date": "2026-06-01",
                                "manual_rows": [{"row_id": "bundle-row", "feature": "Bundle"}],
                                "synced_rows": [{"row_id": "sync-row", "jira_id": "SPDBP-1"}],
                            }
                        },
                        "pipeline_rows": [{"row_id": "pipe-1", "feature": "Pipeline", "pm": ["Zoey"]}],
                    }
                }
            )
        }

        updated = update_version_plan_cell(
            config,
            {"scope": "pipeline", "row_id": "pipe-1", "field": "pm", "value": "chang.wang@npt.sg"},
        )
        self.assertEqual(updated["version_plan"]["af"]["pipeline_rows"][0]["pm"], ["Wang Chang"])

        with self.assertRaisesRegex(ValueError, "Unsupported Version Plan field"):
            update_version_plan_cell(config, {"scope": "pipeline", "row_id": "pipe-1", "field": "owner"})
        with self.assertRaisesRegex(ValueError, "manual row was not found"):
            update_version_plan_cell(config, {"scope": "pipeline", "row_id": "missing", "field": "priority"})
        with self.assertRaisesRegex(ValueError, "row was not found"):
            update_version_plan_cell(config, {"scope": "pipeline", "row_id": "missing", "field": "remarks"})
        with self.assertRaisesRegex(ValueError, "row was not found"):
            update_version_plan_cell(config, {"scope": "bundle", "row_id": "sync-row", "field": "remarks"})
        with self.assertRaisesRegex(ValueError, "row was not found"):
            update_version_plan_cell(config, {"scope": "bundle", "version_id": "missing", "row_id": "sync-row", "field": "remarks"})
        with self.assertRaisesRegex(ValueError, "row was not found"):
            update_version_plan_cell(config, {"scope": "bundle", "version_id": "af-1", "row_id": "missing", "field": "remarks"})

        with self.assertRaisesRegex(ValueError, "Unsupported Version Plan row action"):
            update_version_plan_rows(config, {"scope": "pipeline", "action": "duplicate"})
        with self.assertRaisesRegex(ValueError, "row_id is required"):
            update_version_plan_rows(config, {"action": "move", "source_scope": "pipeline", "target_scope": "bundle", "target_version_id": "af-1"})
        with self.assertRaisesRegex(ValueError, "manual row was not found"):
            update_version_plan_rows(
                config,
                {"action": "move", "row_id": "missing", "source_scope": "pipeline", "target_scope": "bundle", "target_version_id": "af-1"},
            )
        with self.assertRaisesRegex(ValueError, "version_id is required"):
            update_version_plan_rows(config, {"scope": "bundle", "action": "add"})
        with self.assertRaisesRegex(ValueError, "Unsupported Version Plan row scope"):
            update_version_plan_rows(config, {"scope": "unknown", "action": "add"})

        same_scope = update_version_plan_rows(
            config,
            {"action": "move", "row_id": "pipe-1", "source_scope": "pipeline", "target_scope": "pipeline"},
        )
        self.assertEqual(same_scope["version_plan"]["af"]["pipeline_rows"][0]["row_id"], "pipe-1")

    def test_sync_helpers_handle_missing_clients_duplicates_and_parent_failures(self) -> None:
        class ReleaseWindowClient:
            def __init__(self, rows: list[dict] | Exception) -> None:
                self.rows = rows

            def list_jira_tasks_created_by_emails(self, emails: list[str], **kwargs) -> list[dict]:
                if isinstance(self.rows, Exception):
                    raise self.rows
                return self.rows

            def get_issue_detail(self, issue_id: str) -> dict:
                if issue_id == "bad":
                    raise RuntimeError("boom")
                if issue_id == "nondict":
                    return []  # type: ignore[return-value]
                return {"priority": "P2"}

        self.assertEqual(vplan._safe_list_jira_tasks_for_release_window(object(), (), release_after="", release_before=""), [])
        self.assertEqual(
            vplan._safe_list_jira_tasks_for_release_window(ReleaseWindowClient(RuntimeError("bad")), (), release_after="", release_before=""),
            [],
        )
        self.assertEqual(vplan._parent_project_detail(object(), {}), {})
        self.assertEqual(vplan._parent_project_detail(object(), {"parentIds": ["parent"]}), {})
        self.assertEqual(vplan._parent_project_detail(ReleaseWindowClient([]), {"parentIds": [""]}), {})
        self.assertEqual(vplan._parent_project_detail(ReleaseWindowClient([]), {"parentIds": ["bad"]}), {})
        self.assertEqual(vplan._parent_project_detail(ReleaseWindowClient([]), {"parentIds": ["nondict"]}), {})

        rows = vplan._sync_rows_for_bundle(
            ReleaseWindowClient(
                [
                    {"jiraLink": "https://jira/browse/SPDBP-100", "summary": "Linked", "parentIds": ["ok"], "version": "AF_1"},
                    {"jira_id": "SPDBP-100", "summary": "Duplicate", "version": "AF_1"},
                    {"jiraLink": "https://example.invalid/no-ticket", "summary": "No id"},
                ]
            ),
            {"version_id": "af-1", "version_name": "AF_1", "release_date": "2026-05-20"},
            {},
            [{"jira_id": "SPDBP-100", "remarks": "Existing note"}],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["jira_id"], "SPDBP-100")
        self.assertEqual(rows[0]["remarks"], "Existing note")
        self.assertEqual(rows[0]["jira_link"], "https://jira/browse/SPDBP-100")

        self.assertEqual(vplan._safe_search_versions(ReleaseWindowClient([]), "AF_"), [])
        self.assertEqual(vplan._safe_list_issues_for_version(object(), "v1"), [])

        class RaisingIssuesClient:
            def list_issues_for_version(self, version_id: str) -> list[dict]:
                raise RuntimeError(version_id)

        self.assertEqual(vplan._safe_list_issues_for_version(RaisingIssuesClient(), "v1"), [])

    def test_parser_market_and_date_helper_edges(self) -> None:
        self.assertEqual(vplan._bundle_payload({"prd_deadline_date": "2026-05-10"}, today=date(2026, 5, 16))["synced_rows"], [])
        self.assertEqual(vplan._latest_mapped_release_date({"bad": {}, "good": {"release_date": "2026-05-26"}}), "2026-05-26")
        self.assertEqual(
            vplan._mapped_dbp_versions(
                {"release_date": ""},
                {"DBPSG": [{"version_id": "dbp", "version_name": "DBPSG_v1.0_0526", "release_date": "2026-05-26"}]},
            )["DBPSG"]["version_name"],
            "-",
        )
        self.assertEqual(vplan._normalize_pm_values(["", "unknown", "tbc", "xiaodong.zheng@npt.sg", "jun wei"]), ["Jun Wei"])
        self.assertTrue(vplan._is_af_reporter({"reporter": {"name": "Jireh.Tanyx@npt.sg"}}))
        self.assertTrue(vplan._is_af_reporter({"pm_email": "keryin.lim@npt.sg"}))
        self.assertEqual(vplan._extract_jira_id({"jiraUrl": "https://jira/browse/SGDB-88"}), "SGDB-88")
        self.assertEqual(vplan._extract_jira_id({"jiraUrl": "https://jira/no-ticket"}), "")
        self.assertEqual(vplan._extract_jira_link({}, "SGDB-88"), "https://jira.shopee.io/browse/SGDB-88")
        self.assertEqual(vplan._flatten_people({"displayName": "Zoey", "email": "zoey.luxy@npt.sg"}), ["Zoey"])
        self.assertEqual(vplan._flatten_people([{"email": "chongzj@npt.sg"}, "Ker Yin"]), ["chongzj@npt.sg", "Ker Yin"])
        self.assertEqual(vplan._flatten_people(None), [])
        self.assertEqual(vplan._extract_market({"country": "ph"}), "PH")
        self.assertEqual(vplan._extract_jira_board({"board": "SGDB"}), "SGDB")
        self.assertEqual(vplan._extract_jira_board({"raw_response": {"projectKey": "SPPHDB"}}), "SPPHDB")
        self.assertEqual(vplan._market_from_jira_board(""), "")
        self.assertEqual(vplan._market_from_jira_board("SPPHDB-123"), "PH")
        self.assertEqual(vplan._market_from_jira_board("REG-1"), "Regional")
        self.assertEqual(vplan._market_from_jira_board("UNKNOWN"), "")
        self.assertEqual(vplan._market_from_version_name("legacy"), "")
        self.assertEqual(vplan._extract_first_text({"field": {"value": "Chosen"}}, "field"), "Chosen")
        self.assertEqual(vplan._extract_first_text({"field": [{"email": "zoey.luxy@npt.sg"}]}, "field"), "zoey.luxy@npt.sg")
        self.assertEqual(vplan._dedupe_versions([{"version_id": ""}, {"version_id": "v1"}, {"version_id": "v1"}]), [{"version_id": "v1"}])
        self.assertEqual(vplan._next_quarter_end(date(2026, 8, 10)), date(2026, 12, 31))
        self.assertEqual(vplan._next_quarter_end(date(2026, 11, 10)), date(2027, 3, 31))
        self.assertEqual(vplan._parse_datetime("2026-05-16T01:00:00Z").isoformat(), "2026-05-16T09:00:00+08:00")
        self.assertIsNone(vplan._parse_datetime("not-a-date"))
        self.assertEqual(vplan._parse_date(date(2026, 5, 16)), date(2026, 5, 16))
        self.assertIsNone(vplan._parse_date("not-a-date"))
        self.assertEqual(vplan._sanitize_audit_value([1, "x"]), [1, "x"])
        self.assertEqual(vplan._sanitize_audit_value(True), True)
        self.assertIn("+08:00", vplan._now_text(datetime(2026, 5, 16, 9, 0)))
        self.assertEqual(vplan._next_sort_order([]), 0)


if __name__ == "__main__":
    unittest.main()
