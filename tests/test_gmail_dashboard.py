from __future__ import annotations

import base64
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import httplib2
from googleapiclient.errors import HttpError

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.gmail_dashboard import (
    GMAIL_EXPORT_EXCLUDED_SENDERS,
    GMAIL_EXPORT_MAX_TOTAL_MESSAGES,
    GmailDashboardService,
    build_gmail_api_service,
    _build_export_query,
    _build_thread_export_query,
    _clean_export_body_text,
    _extract_message_text_from_payload,
)


class _Execute:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error

    def execute(self):
        if self.error is not None:
            raise self.error
        return self.payload or {}


class _FakeMessagesApi:
    def __init__(self, list_payloads, message_payloads):
        self.list_payloads = list_payloads
        self.message_payloads = message_payloads

    def list(self, **kwargs):
        query = kwargs.get("q")
        page_token = kwargs.get("pageToken")
        key = (query, page_token)
        return _Execute(self.list_payloads[key])

    def get(self, **kwargs):
        return _Execute(self.message_payloads[kwargs["id"]])


class _FakeUsersApi:
    def __init__(self, messages_api, threads_api=None):
        self._messages_api = messages_api
        self._threads_api = threads_api

    def messages(self):
        return self._messages_api

    def threads(self):
        return self._threads_api


class _FakeGmailService:
    def __init__(self, list_payloads, message_payloads, thread_payloads=None):
        self._users_api = _FakeUsersApi(
            _FakeMessagesApi(list_payloads, message_payloads),
            _FakeThreadsApi(thread_payloads or {}),
        )

    def users(self):
        return self._users_api


class _FakeThreadsApi:
    def __init__(self, thread_payloads):
        self.thread_payloads = thread_payloads

    def get(self, **kwargs):
        return _Execute(self.thread_payloads[kwargs["id"]])


class GmailDashboardServiceTests(unittest.TestCase):
    def setUp(self):
        GmailDashboardService.clear_cache()

    def test_gmail_api_service_uses_bounded_http_timeout(self):
        credentials = object()
        with patch.dict("os.environ", {"GMAIL_HTTP_TIMEOUT_SECONDS": "7"}):
            with patch("bpmis_jira_tool.gmail_dashboard.httplib2.Http") as http_cls:
                with patch("bpmis_jira_tool.gmail_dashboard.google_auth_httplib2.AuthorizedHttp") as auth_http_cls:
                    with patch("bpmis_jira_tool.gmail_dashboard.build") as build_mock:
                        result = build_gmail_api_service(credentials)

        http_cls.assert_called_once_with(timeout=7)
        auth_http_cls.assert_called_once_with(credentials, http=http_cls.return_value)
        build_mock.assert_called_once_with("gmail", "v1", http=auth_http_cls.return_value, cache_discovery=False)
        self.assertEqual(result, build_mock.return_value)

    def test_build_dashboard_aggregates_mailbox_metrics(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        period_start = datetime.combine(now.date() - timedelta(days=1), datetime.min.time(), tzinfo=now.tzinfo)
        today_start = datetime.combine(now.date(), datetime.min.time(), tzinfo=now.tzinfo)
        tomorrow_start = today_start + timedelta(days=1)
        period_start_timestamp = int(period_start.timestamp())
        list_payloads = {
            (f"after:{period_start_timestamp} -from:me", None): {
                "messages": [{"id": "r1"}, {"id": "r2"}, {"id": "r3"}],
            },
            (f"after:{period_start_timestamp} in:sent", None): {
                "messages": [{"id": "s1"}, {"id": "s2"}],
            },
            ("is:unread in:inbox", None): {
                "messages": [{"id": "u1"}, {"id": "u2"}],
            },
            (f"after:{int(period_start.timestamp())} before:{int(today_start.timestamp())} -from:me", None): {
                "messages": [{"id": "r2"}, {"id": "r3"}],
            },
            (f"after:{int(today_start.timestamp())} before:{int(tomorrow_start.timestamp())} -from:me", None): {
                "messages": [{"id": "r1"}],
            },
            (f"after:{int(period_start.timestamp())} before:{int(today_start.timestamp())} in:sent", None): {
                "messages": [{"id": "s2"}],
            },
            (f"after:{int(today_start.timestamp())} before:{int(tomorrow_start.timestamp())} in:sent", None): {
                "messages": [{"id": "s1"}],
            },
            (f"after:{period_start_timestamp} -from:me in:inbox", None): {
                "messages": [{"id": "r1"}, {"id": "r2"}, {"id": "r3"}],
            },
            (f"after:{period_start_timestamp} -from:me in:inbox -is:unread", None): {
                "messages": [{"id": "r1"}, {"id": "r3"}],
            },
        }
        message_payloads = {
            "r1": {
                "internalDate": str(int(datetime(2026, 4, 21, 9, 0).timestamp() * 1000)),
                "labelIds": ["INBOX"],
                "payload": {"headers": [{"name": "From", "value": "Alice Example <alice@example.com>"}]},
            },
            "r2": {
                "internalDate": str(int(datetime(2026, 4, 20, 10, 0).timestamp() * 1000)),
                "labelIds": ["INBOX", "UNREAD"],
                "payload": {"headers": [{"name": "From", "value": "Bob Example <bob@example.com>"}]},
            },
            "r3": {
                "internalDate": str(int(datetime(2026, 4, 15, 8, 30).timestamp() * 1000)),
                "labelIds": ["INBOX"],
                "payload": {"headers": [{"name": "From", "value": "Alice Example <alice@example.com>"}]},
            },
            "s1": {
                "internalDate": str(int(datetime(2026, 4, 21, 12, 0).timestamp() * 1000)),
                "labelIds": ["SENT"],
                "payload": {
                    "headers": [
                        {"name": "To", "value": "Charlie <charlie@example.com>"},
                        {"name": "Cc", "value": "Dana <dana@example.com>"},
                    ]
                },
            },
            "s2": {
                "internalDate": str(int(datetime(2026, 4, 19, 15, 0).timestamp() * 1000)),
                "labelIds": ["SENT"],
                "payload": {
                    "headers": [
                        {"name": "To", "value": "Charlie <charlie@example.com>, Erin <erin@example.com>"},
                    ]
                },
            },
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads),
        )

        dashboard = service.build_dashboard(days=2, now=now)

        self.assertEqual(dashboard["summary"]["received_today"], 1)
        self.assertEqual(dashboard["summary"]["current_unread"], 2)
        self.assertEqual(dashboard["summary"]["read_rate_percent"], 67)
        self.assertEqual(dashboard["summary"]["received_period_total"], 3)
        self.assertEqual(dashboard["summary"]["sent_period_total"], 2)
        self.assertEqual(dashboard["leaderboards"]["top_senders"][0]["label"], "Alice Example <alice@example.com>")
        self.assertEqual(dashboard["leaderboards"]["top_senders"][0]["count"], 2)
        self.assertEqual(dashboard["leaderboards"]["top_recipients"][0]["label"], "Charlie <charlie@example.com>")
        self.assertEqual(dashboard["leaderboards"]["top_recipients"][0]["count"], 2)
        self.assertEqual(len(dashboard["trends"]["received"]), 2)
        self.assertEqual(len(dashboard["trends"]["sent"]), 2)
        self.assertFalse(dashboard["data_quality"]["used_fallback_cache"])

    def test_build_overview_uses_count_queries_without_network_rankings(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        previous_day_start = datetime.combine(now.date() - timedelta(days=1), datetime.min.time(), tzinfo=now.tzinfo)
        today_start = datetime.combine(now.date(), datetime.min.time(), tzinfo=now.tzinfo)
        tomorrow_start = today_start + timedelta(days=1)
        service = GmailDashboardService(credentials=object(), gmail_service=object())
        counts = {
            f"after:{int(previous_day_start.timestamp())} before:{int(today_start.timestamp())} -from:me": 3,
            f"after:{int(today_start.timestamp())} before:{int(tomorrow_start.timestamp())} -from:me": 4,
            f"after:{int(previous_day_start.timestamp())} before:{int(today_start.timestamp())} in:sent": 1,
            f"after:{int(today_start.timestamp())} before:{int(tomorrow_start.timestamp())} in:sent": 2,
            "is:unread in:inbox": 5,
            f"after:{int(previous_day_start.timestamp())} -from:me in:inbox": 6,
            f"after:{int(previous_day_start.timestamp())} -from:me in:inbox -is:unread": 4,
        }
        service._count_messages = lambda query: counts.get(query, 0)  # type: ignore[assignment]

        overview = service.build_overview(days=2, now=now)

        self.assertEqual(overview["summary"]["received_today"], 4)
        self.assertEqual(overview["summary"]["received_period_total"], 7)
        self.assertEqual(overview["summary"]["sent_period_total"], 3)
        self.assertEqual(overview["summary"]["read_rate_percent"], 67)
        self.assertEqual(overview["leaderboards"]["top_senders"], [])

    def test_gmail_permission_error_is_raised_as_reconnect_message(self):
        error = HttpError(
            resp=type("Resp", (), {"status": 403, "reason": "Forbidden"})(),
            content=b'{"error":{"status":"PERMISSION_DENIED","message":"ACCESS_TOKEN_SCOPE_INSUFFICIENT"}}',
        )

        class _ErrorMessagesApi:
            def list(self, **_kwargs):
                return _Execute(error=error)

        class _ErrorUsersApi:
            def messages(self):
                return _ErrorMessagesApi()

        class _ErrorService:
            def users(self):
                return _ErrorUsersApi()

        service = GmailDashboardService(credentials=object(), gmail_service=_ErrorService())

        with self.assertRaises(ToolError) as context:
            service.build_dashboard(now=datetime(2026, 4, 21, 16, 0).astimezone())

        self.assertIn("grant Gmail read access", str(context.exception))

    def test_gmail_service_disabled_error_is_raised_as_enable_api_message(self):
        error = HttpError(
            resp=type("Resp", (), {"status": 403, "reason": "Forbidden"})(),
            content=b'{"error":{"status":"PERMISSION_DENIED","message":"Gmail API has not been used in project 123 before or it is disabled. Enable it by visiting ...","details":[{"reason":"SERVICE_DISABLED"}]}}',
        )

        class _ErrorMessagesApi:
            def list(self, **_kwargs):
                return _Execute(error=error)

        class _ErrorUsersApi:
            def messages(self):
                return _ErrorMessagesApi()

        class _ErrorService:
            def users(self):
                return _ErrorUsersApi()

        service = GmailDashboardService(credentials=object(), gmail_service=_ErrorService())

        with self.assertRaises(ToolError) as context:
            service.build_dashboard(now=datetime(2026, 4, 21, 16, 0).astimezone())

        self.assertIn("Gmail API is not enabled", str(context.exception))

    def test_gmail_list_retries_transient_dns_failure(self):
        class _FlakyMessagesApi:
            def __init__(self):
                self.calls = 0

            def list(self, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    return _Execute(error=httplib2.ServerNotFoundError("Unable to find the server at gmail.googleapis.com"))
                return _Execute(payload={"messages": [{"id": "m1"}]})

        class _FlakyUsersApi:
            def __init__(self):
                self.messages_api = _FlakyMessagesApi()

            def messages(self):
                return self.messages_api

        class _FlakyService:
            def __init__(self):
                self.users_api = _FlakyUsersApi()

            def users(self):
                return self.users_api

        gmail_service = _FlakyService()
        service = GmailDashboardService(credentials=object(), gmail_service=gmail_service)

        with patch("bpmis_jira_tool.gmail_dashboard.time_module.sleep") as sleep_mock:
            message_ids = service._list_message_ids(query="after:1", max_messages=10)

        self.assertEqual(message_ids, ["m1"])
        self.assertEqual(gmail_service.users_api.messages_api.calls, 2)
        sleep_mock.assert_called_once_with(1.0)

    def test_dashboard_uses_short_ttl_cache_for_same_user(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="xiaodong.zheng@npt.sg")
        calls = {"count": 0}

        def _count_messages(*, query):
            calls["count"] += 1
            return 1

        service._count_messages = _count_messages  # type: ignore[assignment]

        first = service.build_overview(now=now)
        second = service.build_overview(now=now + timedelta(seconds=30))

        self.assertEqual(first, second)
        self.assertGreater(calls["count"], 0)

    def test_dashboard_falls_back_to_stale_cache_when_live_fetch_fails(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        seeded = {
            "summary": {
                "received_today": 5,
                "current_unread": 7,
                "read_rate_percent": 80,
                "received_period_total": 40,
                "sent_period_total": 12,
            },
            "trends": {"received": [], "sent": []},
            "leaderboards": {"top_senders": [], "top_recipients": []},
            "generated_at": now.isoformat(),
            "period_days": 7,
            "data_quality": {"used_fallback_cache": False, "truncated": False},
        }
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="xiaodong.zheng@npt.sg")
        service._store_cached_dashboard(kind="overview", days=7, now=now - timedelta(minutes=10), dashboard=seeded)

        def _raise(*_args, **_kwargs):
            raise ToolError("boom")

        service._count_messages = _raise  # type: ignore[assignment]

        payload = service.build_overview(now=now + timedelta(minutes=6))

        self.assertTrue(payload["data_quality"]["used_fallback_cache"])
        self.assertEqual(payload["summary"]["received_today"], 5)

    def test_export_manifest_uses_short_ttl_cache_for_same_user(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="xiaodong.zheng@npt.sg")
        calls = {"count": 0}

        def _list_message_ids(*, query, max_messages):
            calls["count"] += 1
            return ["m1", "m2"]

        service._list_message_ids = _list_message_ids  # type: ignore[assignment]

        first = service.build_export_manifest(now=now)
        second = service.build_export_manifest(now=now + timedelta(seconds=30))

        self.assertEqual(first["total_messages"], 2)
        self.assertEqual(first, second)
        self.assertEqual(calls["count"], 1)

    def test_export_history_text_uses_short_ttl_cache_for_same_user_and_batch(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="xiaodong.zheng@npt.sg")
        calls = {"count": 0}

        def _list_export_messages(*, query, batch, days=7, now=None):
            calls["count"] += 1
            return (
                [
                    type(
                        "Record",
                        (),
                        {
                            "internal_date": now,
                            "headers": {"from": "Alice <alice@example.com>", "to": "xiaodong.zheng@npt.sg", "subject": "Cached"},
                            "body_text": "Hello",
                            "body_truncated": False,
                        },
                    )()
                ],
                {"batch": batch, "included_messages": 1, "batch_count": 1, "total_messages": 1, "truncated_bodies": 0, "capped": False},
            )

        service._list_export_messages = _list_export_messages  # type: ignore[assignment]

        first = service.export_history_text(now=now, batch=1)
        second = service.export_history_text(now=now + timedelta(seconds=30), batch=1)

        self.assertEqual(first, second)
        self.assertEqual(calls["count"], 1)

    def test_export_candidate_cache_reuses_scanned_ids_across_batches(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="xiaodong.zheng@npt.sg")
        source_ids = [f"m{index}" for index in range(1, 121)]
        fetch_calls = {"count": 0, "sizes": []}

        service._list_message_ids = lambda *, query, max_messages: source_ids  # type: ignore[assignment]

        def _fetch_message_metadata_many(message_ids):
            fetch_calls["count"] += 1
            fetch_calls["sizes"].append(len(message_ids))
            records = []
            for message_id in message_ids:
                index = int(message_id[1:])
                if index % 10 == 0:
                    headers = {"from": "Meltwater Newsletters <newsletters@meltwater.com>", "subject": "Weekly newsletter"}
                else:
                    headers = {"from": f"Sender {index} <sender{index}@example.com>", "subject": f"Mail {index}"}
                records.append(
                    type(
                        "Record",
                        (),
                        {
                            "message_id": message_id,
                            "internal_date": now - timedelta(minutes=index),
                            "headers": headers,
                        },
                    )()
                )
            return records

        service._fetch_message_metadata_many = _fetch_message_metadata_many  # type: ignore[assignment]

        first_batch_ids = service._list_export_candidate_ids(query="after:1 in:inbox", required_count=50, now=now)
        second_batch_ids = service._list_export_candidate_ids(query="after:1 in:inbox", required_count=100, now=now + timedelta(seconds=30))

        self.assertEqual(len(first_batch_ids), 50)
        self.assertEqual(len(second_batch_ids), 108)
        self.assertEqual(fetch_calls["count"], 3)
        self.assertEqual(fetch_calls["sizes"], [50, 50, 20])

    def test_export_history_text_includes_key_fields_and_plain_text_body(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        period_start = datetime.combine(now.date() - timedelta(days=6), datetime.min.time(), tzinfo=now.tzinfo)
        export_query = _build_export_query(period_start)
        list_payloads = {
            (export_query, None): {
                "messages": [{"id": "m1"}],
            },
        }
        message_payloads = {
            "m1": {
                "internalDate": str(int(datetime(2026, 4, 21, 9, 15).timestamp() * 1000)),
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Alice Example <alice@example.com>"},
                        {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                        {"name": "Subject", "value": "Status update"},
                    ],
                    "parts": [
                        {
                            "mimeType": "text/plain",
                            "body": {"data": "SGVsbG8gdGVhbSwKVGhpcyBpcyBhbiB1cGRhdGUu"},
                        }
                    ],
                },
            },
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads),
        )

        content, filename = service.export_history_text(now=now)

        self.assertEqual(filename, "gmail-history-last-7-days-batch-1.txt")
        self.assertIn("Included messages: 1", content)
        self.assertIn("Date: 2026-04-21T09:15:00", content)
        self.assertIn("From: Alice Example <alice@example.com>", content)
        self.assertIn("To: xiaodong.zheng@npt.sg", content)
        self.assertIn("Subject: Status update", content)
        self.assertIn("Hello team,", content)
        self.assertIn("This is an update.", content)

    def test_extract_message_text_from_payload_falls_back_to_html(self):
        payload = {
            "mimeType": "multipart/alternative",
            "parts": [
                {
                    "mimeType": "text/html",
                    "body": {"data": "PGRpdj5IZWxsbyA8Yi50ZWFtPC9iPjxicj5TdGF0dXMgb2s8L2Rpdj4="},
                }
            ],
        }

        text = _extract_message_text_from_payload(payload)

        self.assertEqual(text, "Hello \nStatus ok")

    def test_clean_export_body_text_removes_quoted_history_and_disclaimer(self):
        raw = (
            "Hello team,\nLatest update is below.\n\n"
            "On Fri, Apr 17, 2026 at 9:26 AM Someone wrote:\nOlder thread\n"
            "CONFIDENTIALITY NOTICE\nPlease delete.\n"
        )

        text = _clean_export_body_text(raw)

        self.assertEqual(text, "Hello team,\nLatest update is below.")

    def test_export_history_text_uses_safe_placeholders_for_missing_fields(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        period_start = datetime.combine(now.date() - timedelta(days=6), datetime.min.time(), tzinfo=now.tzinfo)
        export_query = _build_export_query(period_start)
        list_payloads = {
            (export_query, None): {
                "messages": [{"id": "m2"}],
            },
        }
        message_payloads = {
            "m2": {
                "internalDate": str(int(datetime(2026, 4, 20, 7, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [],
                    "body": {"data": ""},
                },
            },
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads),
        )

        content, _filename = service.export_history_text(now=now)

        self.assertIn("From: [unknown sender]", content)
        self.assertIn("To: [no recipients listed]", content)
        self.assertIn("Subject: [no subject]", content)
        self.assertIn("[body unavailable]", content)

    def test_export_thread_history_since_groups_window_messages_by_thread(self):
        now = datetime(2026, 4, 21, 19, 0).astimezone()
        since = now - timedelta(hours=24)
        query = _build_thread_export_query(since, now)
        in_window = datetime(2026, 4, 21, 9, 15, tzinfo=now.tzinfo)
        older = datetime(2026, 4, 20, 8, 0, tzinfo=now.tzinfo)
        list_payloads = {
            (query, None): {
                "messages": [{"id": "m1", "threadId": "t1"}],
            },
        }
        message_payloads = {}
        thread_payloads = {
            "t1": {
                "id": "t1",
                "messages": [
                    {
                        "id": "older",
                        "threadId": "t1",
                        "internalDate": str(int(older.timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "Bob Example <bob@example.com>"},
                                {"name": "Subject", "value": "Old context subject"},
                            ],
                            "parts": [{"mimeType": "text/plain", "body": {"data": base64.urlsafe_b64encode(b"Old context").decode("utf-8")}}],
                        },
                    },
                    {
                        "id": "m1",
                        "threadId": "t1",
                        "internalDate": str(int(in_window.timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "Alice Example <alice@example.com>"},
                                {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                                {"name": "Subject", "value": "CR rollout"},
                            ],
                            "parts": [
                                {
                                    "mimeType": "text/plain",
                                    "body": {"data": base64.urlsafe_b64encode(b"Please confirm rollout owner.").decode("utf-8")},
                                }
                            ],
                        },
                    },
                ],
            }
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads, thread_payloads),
        )

        content = service.export_thread_history_since(since=since, now=now)

        self.assertIn("Thread ID: t1", content)
        self.assertIn("Gmail Thread Link: https://mail.google.com/mail/u/0/#all/t1", content)
        self.assertIn("Subject: CR rollout", content)
        self.assertNotIn("Subject: Old context subject", content)
        self.assertIn("Alice Example <alice@example.com>", content)
        self.assertIn("Please confirm rollout owner.", content)
        self.assertIn("Message 1 (context only)", content)
        self.assertIn("Use: context only; do not summarize as a new item", content)
        self.assertIn("Old context", content)

    def test_export_history_text_marks_truncated_bodies(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        period_start = datetime.combine(now.date() - timedelta(days=6), datetime.min.time(), tzinfo=now.tzinfo)
        export_query = _build_export_query(period_start)
        list_payloads = {
            (export_query, None): {
                "messages": [{"id": "m3"}],
            },
        }
        message_payloads = {
            "m3": {
                "internalDate": str(int(datetime(2026, 4, 20, 11, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [{"name": "From", "value": "Long Mail <long@example.com>"}],
                    "parts": [
                        {
                            "mimeType": "text/plain",
                            "body": {"data": base64.urlsafe_b64encode(("A" * 13050).encode("utf-8")).decode("utf-8")},
                        }
                    ],
                },
            },
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads),
        )

        content, _filename = service.export_history_text(now=now)

        self.assertIn("message bodies were truncated", content)
        self.assertIn("[body truncated]", content)

    def test_export_manifest_and_batching_exclude_configured_senders(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        period_start = datetime.combine(now.date() - timedelta(days=6), datetime.min.time(), tzinfo=now.tzinfo)
        export_query = _build_export_query(period_start)
        list_payloads = {
            (export_query, None): {
                "messages": [{"id": f"m{index}"} for index in range(1, 106)],
            },
        }
        message_payloads = {
            f"m{index}": {
                "internalDate": str(int(datetime(2026, 4, 21, 8, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [
                        {"name": "From", "value": f"Sender {index} <sender{index}@example.com>"},
                        {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                        {"name": "Subject", "value": f"Mail {index}"},
                    ],
                    "parts": [{"mimeType": "text/plain", "body": {"data": "VGVzdA=="}}],
                },
            }
            for index in range(1, 106)
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads),
        )

        manifest = service.build_export_manifest(now=now)
        content, filename = service.export_history_text(now=now, batch=2)

        self.assertEqual(manifest["total_messages"], 105)
        self.assertEqual(manifest["batch_count"], 3)
        self.assertEqual(manifest["batch_size"], 50)
        self.assertEqual(manifest["excluded_senders"], list(GMAIL_EXPORT_EXCLUDED_SENDERS))
        self.assertEqual(manifest["max_total_messages"], GMAIL_EXPORT_MAX_TOTAL_MESSAGES)
        self.assertFalse(manifest["capped"])
        self.assertEqual(filename, "gmail-history-last-7-days-batch-2.txt")
        self.assertIn("Batch: 2", content)
        self.assertIn("At least 105 exportable messages were identified so far", content)
        self.assertIn("Included messages: 50", content)
        self.assertIn("Mail 55", content)
        self.assertIn("Mail 100", content)
        self.assertNotIn("Mail 101", content)

    def test_export_manifest_filters_calendar_newsletter_and_access_request_noise(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        period_start = datetime.combine(now.date() - timedelta(days=6), datetime.min.time(), tzinfo=now.tzinfo)
        export_query = _build_export_query(period_start)
        list_payloads = {
            (export_query, None): {
                "messages": [{"id": "keep"}, {"id": "calendar"}, {"id": "newsletter"}, {"id": "share"}],
            },
        }
        message_payloads = {
            "keep": {
                "internalDate": str(int(datetime(2026, 4, 21, 10, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Alice Example <alice@example.com>"},
                        {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                        {"name": "Subject", "value": "Project update"},
                    ],
                    "parts": [{"mimeType": "text/plain", "body": {"data": "VXBkYXRl"}}],
                },
            },
            "calendar": {
                "internalDate": str(int(datetime(2026, 4, 21, 9, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Micaela Ang <micaela.angsq@maribank.com.sg>"},
                        {"name": "Subject", "value": "Updated invitation: Weekly Status Review"},
                    ],
                    "parts": [{"mimeType": "text/plain", "body": {"data": "Q2FsZW5kYXI="}}],
                },
            },
            "newsletter": {
                "internalDate": str(int(datetime(2026, 4, 21, 8, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Meltwater Newsletters <newsletters@meltwater.com>"},
                        {"name": "Subject", "value": "Weekly newsletter"},
                    ],
                    "parts": [{"mimeType": "text/plain", "body": {"data": "TmV3cw=="}}],
                },
            },
            "share": {
                "internalDate": str(int(datetime(2026, 4, 21, 7, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [
                        {"name": "From", "value": "Maria Patrice Bautista (via Google Sheets) <drive-shares-dm-noreply@google.com>"},
                        {"name": "Subject", "value": "requests access to an item"},
                    ],
                    "parts": [{"mimeType": "text/plain", "body": {"data": "U2hhcmU="}}],
                },
            },
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads),
        )

        manifest = service.build_export_manifest(now=now)
        content, _filename = service.export_history_text(now=now)

        self.assertEqual(manifest["total_messages"], 4)
        self.assertTrue(manifest["estimated"])
        self.assertIn("Included messages: 1", content)
        self.assertIn("Project update", content)
        self.assertNotIn("Weekly Status Review", content)
        self.assertNotIn("Weekly newsletter", content)
        self.assertNotIn("requests access to an item", content)

    def test_export_manifest_caps_total_scan_to_keep_download_stable(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        period_start = datetime.combine(now.date() - timedelta(days=6), datetime.min.time(), tzinfo=now.tzinfo)
        export_query = _build_export_query(period_start)
        total_source_messages = GMAIL_EXPORT_MAX_TOTAL_MESSAGES + 35
        list_payloads = {
            (export_query, None): {
                "messages": [{"id": f"m{index}"} for index in range(1, total_source_messages + 1)],
            },
        }
        message_payloads = {
            f"m{index}": {
                "internalDate": str(int(datetime(2026, 4, 21, 8, 0).timestamp() * 1000)),
                "payload": {
                    "headers": [
                        {"name": "From", "value": f"Sender {index} <sender{index}@example.com>"},
                        {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                        {"name": "Subject", "value": f"Mail {index}"},
                    ],
                    "parts": [{"mimeType": "text/plain", "body": {"data": "VGVzdA=="}}],
                },
            }
            for index in range(1, total_source_messages + 1)
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads),
        )

        manifest = service.build_export_manifest(now=now)
        content, _filename = service.export_history_text(now=now, batch=4)

        self.assertEqual(manifest["total_messages"], GMAIL_EXPORT_MAX_TOTAL_MESSAGES)
        self.assertTrue(manifest["capped"])
        self.assertEqual(manifest["batch_count"], 4)
        self.assertIn(f"first {GMAIL_EXPORT_MAX_TOTAL_MESSAGES} matching emails", content)
        self.assertIn("Included messages: 50", content)
        self.assertIn(f"Mail {GMAIL_EXPORT_MAX_TOTAL_MESSAGES}", content)
        self.assertNotIn(f"Mail {GMAIL_EXPORT_MAX_TOTAL_MESSAGES + 1}", content)


if __name__ == "__main__":
    unittest.main()
