import tempfile
import unittest
from pathlib import Path

from scripts.wechat_notification_to_seatalk import (
    ClassifiedAlert,
    DedupeStore,
    NotificationEvent,
    ReplyTargetStore,
    classify_wechat_notification,
    format_alert,
)


class WeChatNotificationToSeaTalkTests(unittest.TestCase):
    def test_classifies_private_message(self) -> None:
        event = NotificationEvent(
            event_id="one",
            app_id="com.tencent.xinWeChat",
            title="Alice",
            subtitle="",
            body="hello",
            delivered_at=1_777_777_777,
            source="fixture",
        )

        alert = classify_wechat_notification(event)

        self.assertIsNotNone(alert)
        assert alert is not None
        self.assertEqual("private_message", alert.event_type)
        self.assertEqual("Alice", alert.conversation)
        self.assertEqual("hello", alert.preview)

    def test_classifies_group_mention(self) -> None:
        event = NotificationEvent(
            event_id="two",
            app_id="com.tencent.xinWeChat",
            title="Project Group",
            subtitle="Bob",
            body="Bob: @你 please check",
            delivered_at=1_777_777_777,
            source="fixture",
        )

        alert = classify_wechat_notification(event)

        self.assertIsNotNone(alert)
        assert alert is not None
        self.assertEqual("group_mention", alert.event_type)
        self.assertEqual("Project Group", alert.conversation)

    def test_ignores_normal_group_message(self) -> None:
        event = NotificationEvent(
            event_id="three",
            app_id="com.tencent.xinWeChat",
            title="Project Group",
            subtitle="",
            body="Bob: normal update",
            delivered_at=1_777_777_777,
            source="fixture",
        )

        self.assertIsNone(classify_wechat_notification(event))

    def test_format_can_hide_preview(self) -> None:
        event = NotificationEvent(
            event_id="four",
            app_id="com.tencent.xinWeChat",
            title="Alice",
            subtitle="",
            body="secret",
            delivered_at=1_777_777_777,
            source="fixture",
        )
        alert = ClassifiedAlert(event, "private_message", "Alice", "secret")

        text = format_alert(alert, include_preview=False)

        self.assertIn("WX DM | Alice", text)
        self.assertNotIn("secret", text)

    def test_dedupe_store_persists_seen_alert(self) -> None:
        event = NotificationEvent(
            event_id="five",
            app_id="com.tencent.xinWeChat",
            title="Alice",
            subtitle="",
            body="hello",
            delivered_at=1_777_777_777,
            source="fixture",
        )
        alert = ClassifiedAlert(event, "private_message", "Alice", "hello")

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            first = DedupeStore(path, ttl_seconds=600)
            self.assertTrue(first.mark_if_new(alert))

            second = DedupeStore(path, ttl_seconds=600)
            self.assertFalse(second.mark_if_new(alert))

    def test_reply_store_remembers_target(self) -> None:
        event = NotificationEvent(
            event_id="six",
            app_id="com.tencent.xinWeChat",
            title="Alice",
            subtitle="",
            body="hello",
            delivered_at=1_777_777_777,
            source="fixture",
        )
        alert = ClassifiedAlert(event, "private_message", "Alice", "hello")

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "replies.json"
            store = ReplyTargetStore(path, ttl_seconds=600)
            target = store.remember(alert)

            self.assertTrue(target.reply_id.startswith("wx_"))
            self.assertEqual("Alice", target.conversation)
            self.assertEqual(target.conversation, ReplyTargetStore(path, ttl_seconds=600).get(target.reply_id).conversation)

    def test_format_includes_reply_command(self) -> None:
        event = NotificationEvent(
            event_id="seven",
            app_id="com.tencent.xinWeChat",
            title="Alice",
            subtitle="",
            body="hello",
            delivered_at=1_777_777_777,
            source="fixture",
        )
        alert = ClassifiedAlert(event, "private_message", "Alice", "hello")

        text = format_alert(alert, include_preview=True, reply_id="wx_abcdef1234")

        self.assertIn("Reply: /reply wx_abcdef1234 ", text)


if __name__ == "__main__":
    unittest.main()
