from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bpmis_jira_tool.report_intelligence import (
    DEFAULT_PRIORITY_KEYWORDS,
    build_daily_match_summary,
    build_monthly_evidence_sidecar,
    filter_text_by_noise,
    is_gmail_noise,
    key_project_candidates_from_team_config,
    load_report_intelligence_config_from_data_root,
    load_team_dashboard_config_from_data_root,
    match_report_intelligence,
    normalize_report_intelligence_config,
    report_intelligence_from_team_config,
)
from bpmis_jira_tool.user_config import DB_FILE


class ReportIntelligenceTests(unittest.TestCase):
    def test_default_keywords_include_regulators(self):
        config = normalize_report_intelligence_config({})

        self.assertIn("BSP", config["priority_keywords"])
        self.assertIn("OJK", config["priority_keywords"])
        self.assertIn("BSP", DEFAULT_PRIORITY_KEYWORDS)
        self.assertIn("OJK", DEFAULT_PRIORITY_KEYWORDS)

    def test_daily_summary_only_includes_matched_config_items(self):
        config = normalize_report_intelligence_config(
            {
                "vip_people": [
                    {"display_name": "Boss", "role_tags": ["直属 Boss"], "emails": ["boss@npt.sg"]},
                    {"display_name": "Unused VIP", "role_tags": ["Finance"]},
                ],
                "priority_keywords": ["BSP", "OJK", "延期"],
            }
        )
        matches = match_report_intelligence(
            "Boss asked whether BSP launch approval blocks Project Alpha BPMIS-1.",
            config=config,
            key_projects=[{"bpmis_id": "BPMIS-1", "project_name": "Project Alpha", "jira_ids": ["AF-1"]}],
        )
        summary = build_daily_match_summary(matches)

        self.assertIn("Boss", summary)
        self.assertIn("BSP", summary)
        self.assertIn("BPMIS-1", summary)
        self.assertNotIn("Unused VIP", summary)
        self.assertNotIn("OJK", summary)

    def test_seatalk_noise_filter_excludes_entire_blacklisted_group_block(self):
        config = normalize_report_intelligence_config(
            {
                "noise": {
                    "seatalk_group_blacklist": ["4293495"],
                },
            }
        )
        history = "\n".join(
            [
                "SeaTalk Chat History Export",
                "Window: since 2026-05-11T13:00:00+08:00",
                "",
                "=== CRS Task检查-提醒~! (group-4293495) ===",
                "[2026-05-11 13:01] Alice: noisy task reminder",
                "[2026-05-11 13:02] Bob: another noisy reminder",
                "=== Keep This Group (group-123456) ===",
                "[2026-05-11 13:03] Carol: useful update",
            ]
        )

        filtered = filter_text_by_noise(history, config=config, source="seatalk")

        self.assertNotIn("4293495", filtered)
        self.assertNotIn("noisy task reminder", filtered)
        self.assertNotIn("another noisy reminder", filtered)
        self.assertIn("Keep This Group", filtered)
        self.assertIn("useful update", filtered)

    def test_load_report_intelligence_config_handles_storage_boundaries(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            self.assertEqual(load_team_dashboard_config_from_data_root(root), {})
            self.assertEqual(load_report_intelligence_config_from_data_root(root)["vip_people"], [])

            (root / DB_FILE).write_text("not sqlite", encoding="utf-8")
            self.assertEqual(load_team_dashboard_config_from_data_root(root), {})

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / DB_FILE
            with sqlite3.connect(db_path) as connection:
                connection.execute("CREATE TABLE team_dashboard_configs (config_key TEXT, config_json TEXT)")
            self.assertEqual(load_team_dashboard_config_from_data_root(root), {})

            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    "INSERT INTO team_dashboard_configs (config_key, config_json) VALUES (?, ?)",
                    ("team_dashboard", "{bad json"),
                )
            self.assertEqual(load_team_dashboard_config_from_data_root(root), {})

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / DB_FILE
            payload = {
                "report_intelligence_config": {
                    "vip_people": [{"name": "Boss", "emails": "boss@npt.sg"}],
                    "priority_keywords": "MAS, MAS; OJK\n",
                }
            }
            with sqlite3.connect(db_path) as connection:
                connection.execute("CREATE TABLE team_dashboard_configs (config_key TEXT, config_json TEXT)")
                connection.execute(
                    "INSERT INTO team_dashboard_configs (config_key, config_json) VALUES (?, ?)",
                    ("team_dashboard", json.dumps(["wrong"])),
                )
            self.assertEqual(load_team_dashboard_config_from_data_root(root), {})

            with sqlite3.connect(db_path) as connection:
                connection.execute("DELETE FROM team_dashboard_configs")
                connection.execute(
                    "INSERT INTO team_dashboard_configs (config_key, config_json) VALUES (?, ?)",
                    ("team_dashboard", json.dumps(payload)),
                )
            loaded = load_report_intelligence_config_from_data_root(root)
            self.assertEqual(loaded["vip_people"][0]["display_name"], "Boss")
            self.assertEqual(loaded["priority_keywords"], ["MAS", "OJK"])

        self.assertIn("priority_keywords", report_intelligence_from_team_config(["not dict"]))

    def test_key_project_candidates_normalizes_team_cache_boundaries(self):
        self.assertEqual(key_project_candidates_from_team_config(["not dict"]), [])

        candidates = key_project_candidates_from_team_config(
            {
                "task_cache": {
                    "teams": {
                        "bad": "skip",
                        "risk": {
                            "under_prd": [
                                "skip",
                                {"is_key_project": True},
                                {
                                    "is_key_project": True,
                                    "bpmis_id": "225159",
                                    "project_name": "AF Launch",
                                    "market": "ID",
                                    "priority": "P0",
                                    "jira_tickets": [
                                        "skip",
                                        {"issue_id": "AF-1", "jira_title": "Launch approval"},
                                        {"jira_id": "AF-2", "jira_title": "Launch approval"},
                                    ],
                                },
                            ],
                            "pending_live": [
                                {
                                    "is_key_project": True,
                                    "bpmis_id": "225159",
                                    "project_name": "Duplicate ignored by bpmis",
                                    "market": "SG",
                                },
                                {
                                    "is_key_project": True,
                                    "project_name": "Name Only",
                                    "jira_tickets": [{"jira_id": "NO-1"}],
                                },
                            ],
                        },
                    }
                }
            }
        )

        self.assertEqual(len(candidates), 2)
        launch = candidates[0]
        self.assertEqual(launch["bpmis_id"], "225159")
        self.assertEqual(launch["jira_ids"], ["AF-1", "AF-2"])
        self.assertEqual(launch["aliases"], ["ID", "P0", "Launch approval", "SG"])
        self.assertEqual(candidates[1]["project_name"], "Name Only")

    def test_matching_and_summary_limits_and_empty_inputs(self):
        vip_config = {
            "vip_people": [
                {"display_name": f"Boss {index}", "aliases": ["shared-alias"], "roles": ["Risk"]}
                for index in range(20)
            ],
            "priority_keywords": [f"K{index}" for index in range(30)],
        }
        key_projects = [
            {"bpmis_id": f"BPMIS-{index}", "project_name": f"Project {index}", "jira_ids": ["AF-1", "AF-1"]}
            for index in range(20)
        ]

        matches = match_report_intelligence(
            "shared-alias " + " ".join(f"K{index}" for index in range(30)) + " " + " ".join(f"BPMIS-{index}" for index in range(20)),
            config=vip_config,
            key_projects=["skip", {}, *key_projects],
        )

        self.assertEqual(len(matches["matched_vips"]), 12)
        self.assertEqual(len(matches["matched_keywords"]), 18)
        self.assertEqual(len(matches["matched_key_projects"]), 16)
        self.assertEqual(matches["matched_key_projects"][0]["jira_ids"], ["AF-1"])
        self.assertEqual(build_daily_match_summary({}), "")
        self.assertEqual(build_daily_match_summary(["not dict"]), "")
        self.assertIn("Boss 0 (Risk)", build_daily_match_summary(matches))

    def test_noise_helpers_cover_gmail_and_non_seatalk_boundaries(self):
        config = normalize_report_intelligence_config(
            {
                "noise": {
                    "seatalk_group_blacklist": "noisy,skip-line",
                    "gmail_sender_blacklist": ["Noise@Example.COM"],
                    "gmail_subject_hints": ["newsletter", " digest "],
                }
            }
        )

        self.assertEqual(filter_text_by_noise("keep\nskip-line\nalso keep", config=config, source="gmail"), "keep\nskip-line\nalso keep")
        self.assertEqual(filter_text_by_noise("keep\nskip-line\nalso keep", config=config, source="seatalk"), "keep\nalso keep")
        self.assertTrue(is_gmail_noise({"from": "Noise Sender <noise@example.com>", "subject": "Launch"}, config=config))
        self.assertTrue(is_gmail_noise({"from": "other@example.com", "subject": "Weekly Newsletter"}, config=config))
        self.assertTrue(is_gmail_noise({"from": "other@example.com", "subject": "Daily digest"}, config=config))
        self.assertFalse(is_gmail_noise({"from": "No address", "subject": "Launch"}, config=config))

    def test_monthly_evidence_sidecar_sources_limits_and_risk_levels(self):
        config = {
            "vip_people": [{"display_name": "Boss", "emails": ["boss@npt.sg"]}],
            "priority_keywords": ["BSP", "low-priority"],
        }
        key_projects = [
            {
                "bpmis_id": "225159",
                "project_name": "AF Launch",
                "market": "ID",
                "jira_tickets": [{"jira_id": "AF-1", "jira_title": "BSP launch"}],
            }
        ]
        items = build_monthly_evidence_sidecar(
            seatalk_history_text="Boss flagged BSP delay\nordinary line",
            key_projects=key_projects,
            prd_sources=[{"title": "AF Launch PRD", "url": "https://example.test/prd"}],
            config=config,
        )

        self.assertEqual([item["source"] for item in items], ["seatalk", "key_project_jira", "prd"])
        self.assertEqual(items[0]["risk_level"], "high")
        self.assertEqual(items[1]["evidence"], "225159 / AF Launch / ID")
        self.assertEqual(items[2]["risk_level"], "medium")
        bad_ticket_items = build_monthly_evidence_sidecar(
            seatalk_history_text="",
            key_projects=[{"bpmis_id": "bad-ticket", "jira_tickets": ["skip"]}],
            prd_sources=[],
            config=config,
        )
        self.assertEqual(bad_ticket_items[0]["matched_key_projects"][0]["jira_ids"], [])

        with patch("bpmis_jira_tool.report_intelligence.MAX_MONTHLY_SIDECAR_ITEMS", 1):
            self.assertEqual(
                len(
                    build_monthly_evidence_sidecar(
                        seatalk_history_text="BSP one\nBSP two",
                        key_projects=key_projects,
                        prd_sources=[],
                        config={"priority_keywords": ["BSP"]},
                    )
                ),
                1,
            )
            self.assertEqual(
                len(
                    build_monthly_evidence_sidecar(
                        seatalk_history_text="",
                        key_projects=key_projects * 2,
                        prd_sources=[{"title": "BSP PRD"}],
                        config={"priority_keywords": ["BSP"]},
                    )
                ),
                1,
            )

    def test_normalization_boundaries_are_stable(self):
        config = normalize_report_intelligence_config(
            {
                "vip_people": [
                    "skip",
                    {},
                    {"name": "Boss", "emails": "Boss@NPT.SG; boss@npt.sg", "roles": "Risk, Risk"},
                    {"display_name": "Boss", "aliases": ["duplicate"]},
                    {"seatalk_ids": ["st-1"]},
                    {"aliases": ["Alias Only"]},
                ],
                "priority_keywords": ["", "MAS", "mas", "OJK"],
                "noise": "bad",
            }
        )

        self.assertEqual([vip["display_name"] for vip in config["vip_people"]], ["Boss", "", ""])
        self.assertEqual(config["vip_people"][0]["emails"], ["boss@npt.sg"])
        self.assertEqual(config["vip_people"][0]["role_tags"], ["Risk"])
        self.assertEqual(config["priority_keywords"], ["MAS", "OJK"])
        self.assertEqual(
            config["noise"],
            {"seatalk_group_blacklist": [], "gmail_sender_blacklist": [], "gmail_subject_hints": []},
        )


if __name__ == "__main__":
    unittest.main()
