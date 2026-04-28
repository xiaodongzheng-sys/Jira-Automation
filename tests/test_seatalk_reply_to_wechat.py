import unittest

from scripts.seatalk_reply_to_wechat import (
    extract_seatalk_challenge,
    extract_sender,
    extract_text,
    parse_reply_payload,
)


class SeaTalkReplyToWeChatTests(unittest.TestCase):
    def test_extracts_nested_text(self) -> None:
        payload = {"event": {"message": {"text": {"content": "/reply wx_abcdef1234 hello"}}}}

        self.assertEqual("/reply wx_abcdef1234 hello", extract_text(payload))

    def test_extracts_sender(self) -> None:
        payload = {"event": {"sender": {"email": "owner@example.com"}}}

        self.assertEqual("owner@example.com", extract_sender(payload))

    def test_parses_reply_command(self) -> None:
        payload = {
            "event": {
                "sender": {"email": "owner@example.com"},
                "message": {"text": {"content": "/reply wx_abcdef1234 收到"}},
            }
        }

        parsed = parse_reply_payload(payload)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual("wx_abcdef1234", parsed.reply_id)
        self.assertEqual("收到", parsed.message)
        self.assertEqual("owner@example.com", parsed.sender)

    def test_parses_group_mention_reply_command(self) -> None:
        payload = {
            "event": {
                "message": {
                    "text": {
                        "plain_text": "@Wechat /reply wx_abcdef1234 收到",
                    }
                },
                "sender": {"email": "owner@example.com"},
            }
        }

        parsed = parse_reply_payload(payload)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual("wx_abcdef1234", parsed.reply_id)
        self.assertEqual("收到", parsed.message)

    def test_ignores_non_reply_text(self) -> None:
        payload = {"message": {"text": {"content": "hello"}}}

        self.assertIsNone(parse_reply_payload(payload))

    def test_extracts_verification_challenge(self) -> None:
        payload = {"event_type": "event_verification", "event": {"seatalk_challenge": "abc123"}}

        self.assertEqual("abc123", extract_seatalk_challenge(payload))


if __name__ == "__main__":
    unittest.main()
