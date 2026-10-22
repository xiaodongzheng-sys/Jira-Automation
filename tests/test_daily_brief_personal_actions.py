from __future__ import annotations

import json
import unittest
from datetime import datetime
from unittest.mock import patch

from bpmis_jira_tool import seatalk_daily_email as brief
from bpmis_jira_tool.seatalk_dashboard import SeaTalkDashboardService


class PersonalActionTests(unittest.TestCase):
    def test_parser_preserves_routing_and_evidence_for_every_section(self):
        metadata = {
            "source_type": "seatalk",
            "evidence_ref_id": "st-ref-001",
            "evidence_timestamp": "2026-10-21 17:17:20",
            "priority_reason": "Explicit request",
            "matched_keywords": ["review"],
        }
        payload = {
            "my_todos": [{**metadata, "task": "跟进文档评审", "action_type": "direct_action", "priority": "high"}],
            "project_updates": [{**metadata, "title": "Release"}],
            "other_updates": [{**metadata, "title": "Policy", "signal_type": "policy_process"}],
            "team_member_reminders": [{**metadata, "person": "Rene Chong", "reminder": "确认上线范围"}],
        }
        parsed = SeaTalkDashboardService._parse_insights_response(json.dumps(payload))
        for section in payload:
            for field, value in metadata.items():
                self.assertEqual(parsed[section][0][field], value)
        self.assertEqual(parsed["my_todos"][0]["action_type"], "direct_action")
        self.assertEqual(parsed["other_updates"][0]["signal_type"], "policy_process")

    def test_private_request_does_not_need_xiaodong_name(self):
        history = (
            "=== Rene Chong (buddy-341874) ===\n"
            "[2026-10-21 17:17:20] Rene Chong: Do you know which requirement is this? Is it related to the other request?\n"
        )
        candidates = brief._build_direct_xiaodong_request_candidates(history)
        self.assertEqual(len(candidates), 1)
        refs = brief._build_daily_brief_evidence_refs(history, xiaodong_followup_candidates=candidates)
        self.assertTrue(brief._seatalk_ref_supports_xiaodong_action(refs[0]))
        self.assertEqual(brief._build_team_member_reminder_candidates(history), [])

    def test_private_fyi_and_meeting_logistics_are_not_owned_actions(self):
        for text in (
            "Hi Xiaodong, just an update: I raised the change request and will update the team.",
            "Thanks!",
            "Can we move the meeting? I will join late.",
        ):
            with self.subTest(text=text):
                history = f"=== Rene Chong (buddy-341874) ===\n[2026-10-21 17:00:00] Rene Chong: {text}\n"
                self.assertEqual(brief._build_direct_xiaodong_request_candidates(history), [])

    def test_personal_commitments_remain_open_after_acknowledgement(self):
        for text in (
            "我晚上再看下吧",
            "好的我再看下让你知道",
            "好呀，我看看",
            "我找一下告诉你",
            "Sorry too busy with meetings today. Let me try out tonight and let you know?",
        ):
            with self.subTest(text=text):
                history = (
                    "=== Wang Zhenhua (buddy-266176) ===\n"
                    "[2026-10-21 18:00:00] Wang Zhenhua: Can you re-check these five issues?\n"
                    f"[2026-10-21 18:10:47] Zheng Xiaodong: {text}\n"
                    "[2026-10-21 18:11:43] Wang Zhenhua: okay\n"
                )
                self.assertEqual(len(brief._build_xiaodong_followup_candidates(history)), 1)
                self.assertEqual(len(brief._build_direct_xiaodong_request_candidates(history)), 1)

    def test_xiaodong_acknowledgement_does_not_close_inbound_request(self):
        history = (
            "=== Rene Chong (buddy-341874) ===\n"
            "[2026-10-21 17:17:20] Rene Chong: Can you review this requirement?\n"
            "[2026-10-21 17:20:00] Zheng Xiaodong: OK, noted.\n"
        )
        self.assertEqual(len(brief._build_direct_xiaodong_request_candidates(history)), 1)
        history += "[2026-10-21 18:00:00] Zheng Xiaodong: Confirmed, use the existing API.\n"
        self.assertEqual(brief._build_direct_xiaodong_request_candidates(history), [])

    def test_long_link_prefix_does_not_hide_the_request(self):
        text = " ".join(f"https://example.invalid/feedback/{1000000000000000000 + index}" for index in range(5))
        text += " can help re-check all these 5 issues if they still exist"
        history = f"=== Wang Zhenhua (buddy-266176) ===\n[2026-10-21 18:15:09] Wang Zhenhua: {text}\n"
        refs = brief._build_daily_brief_evidence_refs(history, include_all_records=True)
        self.assertNotIn("re-check", refs[0]["snippet"])
        self.assertTrue(brief._seatalk_ref_supports_xiaodong_action(refs[0]))
        self.assertIn("re-check", refs[0]["_full_text"])

    def test_full_source_validation_does_not_cap_the_evidence_index(self):
        history = "=== Rene Chong (buddy-341874) ===\n" + "\n".join(
            f"[2026-10-21 17:{index // 60:02}:{index % 60:02}] Rene Chong: Information item {index}."
            for index in range(150)
        )
        refs = brief._build_daily_brief_evidence_refs(history, include_all_records=True)
        self.assertEqual(len(refs), 150)

    def test_own_acknowledgement_is_not_evidence_of_an_action(self):
        self.assertFalse(brief._seatalk_ref_supports_xiaodong_action({
            "source_type": "seatalk", "sender": "Zheng Xiaodong",
            "group": "Rene Chong (buddy-341874)", "snippet": "OK, noted.",
        }))

    def test_timestamp_binds_translated_private_task_without_keyword_overlap(self):
        history = (
            "=== Rene Chong (buddy-341874) ===\n"
            "[2026-10-21 17:17:20] Rene Chong: Do you know which requirement is this?\n"
        )
        refs = brief._build_daily_brief_evidence_refs(history, include_all_records=True)
        items = [{
            "task": "说明该链接对应的需求及与另一事项的关系。",
            "domain": "Anti-fraud", "source_type": "seatalk", "action_type": "direct_action",
            "evidence": "Rene Chong", "evidence_timestamp": "2026-10-21 17:17:20",
        }]
        metrics = brief._apply_daily_brief_evidence_refs(
            project_updates=[], other_updates=[], my_todos=items, reminders=[], evidence_refs=refs,
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(metrics["dropped_invalid_evidence_count"], 0)
        brief._validate_and_repair_seatalk_evidence(items, history_text=history)
        self.assertEqual(len(items), 1)

    def test_timestamp_does_not_accept_wrong_contact_or_owner(self):
        history = (
            "=== Rene Chong (buddy-341874) ===\n"
            "[2026-10-21 17:17:20] Rene Chong: Can you review the requirements?\n"
            "=== AF PM Team (group-1) ===\n"
            "[2026-10-21 18:00:00] Alice: @Wang Chang please review the API.\n"
        )
        refs = brief._build_daily_brief_evidence_refs(history, include_all_records=True)
        for evidence, timestamp in (("Wang Zhenhua", "2026-10-21 17:17:20"), ("AF PM Team", "2026-10-21 18:00:00")):
            with self.subTest(evidence=evidence):
                items = [{"task": "评审需求", "action_type": "direct_action", "source_type": "seatalk",
                          "evidence": evidence, "evidence_timestamp": timestamp}]
                metrics = brief._apply_daily_brief_evidence_refs(
                    project_updates=[], other_updates=[], my_todos=items, reminders=[], evidence_refs=refs,
                )
                self.assertEqual(items, [])
                self.assertTrue(metrics["evidence_rejections"])

    def test_private_contact_suffix_and_lookup_commitment_are_supported(self):
        history = (
            "=== Guo Lingzhi (郭凌志) (buddy-778985) ===\n"
            "[2026-10-21 20:00:05] Zheng Xiaodong: 我找一下告诉你\n"
        )
        refs = brief._build_daily_brief_evidence_refs(history, include_all_records=True)
        items = [{"task": "查找并发送问题对应的任务链接。", "action_type": "direct_action", "source_type": "seatalk",
                  "evidence": "Guo Lingzhi（郭凌志） / Private SeaTalk chat", "evidence_timestamp": "2026-10-21 20:00:05"}]
        brief._apply_daily_brief_evidence_refs(
            project_updates=[], other_updates=[], my_todos=items, reminders=[], evidence_refs=refs,
        )
        self.assertEqual(len(items), 1)

    def test_explicit_ref_cannot_override_a_wrong_timestamp(self):
        refs = [{"id": "st-ref-001", "source_type": "seatalk", "group": "Rene Chong (buddy-341874)",
                 "evidence": "Rene Chong", "sender": "Rene Chong", "snippet": "Can you review?",
                 "timestamp": "2026-10-21 17:17:20"}]
        items = [{"task": "评审需求", "action_type": "direct_action", "source_type": "seatalk",
                  "evidence": "Rene Chong", "evidence_ref_id": "st-ref-001",
                  "evidence_timestamp": "2026-10-21 16:00:00"}]
        metrics = brief._apply_daily_brief_evidence_refs(
            project_updates=[], other_updates=[], my_todos=items, reminders=[], evidence_refs=refs,
        )
        self.assertEqual(items, [])
        self.assertEqual(metrics["evidence_rejections"][0]["reason"], "timestamp_or_source_mismatch")

    def test_empty_personal_actions_raise_a_quality_note_when_candidates_were_dropped(self):
        for metrics, expected in (
            ({"input_direct_action_count": 2}, "个人待办候选全部被过滤"),
            ({"unanswered_private_request_count": 2}, "源记录有未答复的私聊请求"),
        ):
            with self.subTest(metrics=metrics):
                metadata = brief._build_quality_metadata(
                    project_updates=[], other_updates=[], my_todos=[], direct_action_todos=[],
                    watch_delegate_todos=[], reminders=[], source_texts=[], deduped_topic_count=0,
                    evidence_quality_metrics=metrics,
                )
                self.assertIn(expected, metadata["manual_review_notes"][0])

    def test_one_pass_keeps_real_private_requests_through_parsing_and_rendering(self):
        history = (
            "=== Rene Chong (buddy-341874) ===\n"
            "[2026-10-21 17:17:20] Rene Chong: Do you know which requirement is this?\n"
            "=== Wang Zhenhua (buddy-266176) ===\n"
            "[2026-10-21 18:15:09] Wang Zhenhua: Can help re-check these five issues?\n"
            "=== Chen Lingyun (陈凌云) | 15679336318 (buddy-42740) ===\n"
            "[2026-10-21 15:31:55] Chen Lingyun: We fixed it. Can you check if it still reproduces?\n"
            "[2026-10-21 18:11:18] Zheng Xiaodong: 好的我再看下让你知道\n"
        )
        tasks = [
            ("Rene Chong", "2026-10-21 17:17:20", "确认链接对应的需求并回复 Rene。"),
            ("Wang Zhenhua", "2026-10-21 18:15:09", "复查五个问题并反馈验证结果。"),
            ("Private SeaTalk chat with Chen Lingyun (陈凌云)", "2026-10-21 18:11:18", "验证修复是否生效并反馈结果。"),
        ]

        class Service:
            def _run_codex_insights_prompt(self, **kwargs):
                self.prompt = kwargs["prompt"]
                return None, SeaTalkDashboardService._parse_insights_response(json.dumps({
                    "my_todos": [
                        {"task": task, "domain": "General", "priority": "high", "due": "无",
                         "action_type": "direct_action", "source_type": "seatalk",
                         "evidence": name, "evidence_timestamp": timestamp}
                        for name, timestamp, task in tasks
                    ],
                }))

        now = datetime.fromisoformat("2026-10-21T19:00:00+08:00")
        service = Service()
        with patch.object(brief, "_load_seatalk_name_mappings", return_value={}):
            result = brief._build_daily_briefing_one_pass(
                service=service, raw_source_text=history, now=now,
                window_start=now.replace(hour=13), window_end=now, window_label="13:00-19:00",
                period_hours=6, include_debug_evidence_refs=False,
            )
        self.assertEqual(len(result["direct_action_todos"]), 3)
        self.assertEqual(result["watch_delegate_todos"], [])
        self.assertEqual(result["team_member_reminders"], [])
        _, body, _ = brief.render_email(briefing=result, now=now)
        for _, _, task in tasks:
            self.assertIn(task, body)
        self.assertIn("evidence_timestamp", service.prompt)


if __name__ == "__main__":
    unittest.main()
