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
    GMAIL_TOPIC_MAX_GOOGLE_SHEET_CHARS,
    GmailAttachmentRecord,
    GmailDashboardService,
    GmailExportRecord,
    GmailMessageRecord,
    build_drive_api_service,
    build_gmail_api_service,
    _build_contact_thread_export_query,
    _build_export_query,
    _build_monthly_requirements_thread_export_query,
    _build_topic_thread_export_query,
    _build_thread_export_query,
    _bytes_to_text,
    _clean_export_body_text,
    _decode_gmail_body_data,
    _extract_drive_links_from_payload,
    _extract_drive_links_from_text,
    _extract_gmail_attachments_from_payload,
    _extract_message_text_from_payload,
    _format_contact_label,
    _gmail_http_timeout_seconds,
    _gmail_thread_link,
    _google_drive_file_id_from_url,
    _html_to_text,
    _is_export_noise,
    _safe_datetime_from_epoch_ms,
    _thread_has_contact_window_message,
    _thread_matches_topic,
    _thread_monthly_requirements_market,
    _trim_preview_text,
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

    def test_gmail_service_helpers_cover_timeout_drive_and_id_edges(self):
        credentials = object()
        with patch.dict("os.environ", {"GMAIL_HTTP_TIMEOUT_SECONDS": ""}):
            self.assertEqual(_gmail_http_timeout_seconds(), 20)
        with patch.dict("os.environ", {"GMAIL_HTTP_TIMEOUT_SECONDS": "bad"}):
            self.assertEqual(_gmail_http_timeout_seconds(), 20)
        with patch.dict("os.environ", {"GMAIL_HTTP_TIMEOUT_SECONDS": "999"}):
            self.assertEqual(_gmail_http_timeout_seconds(), 120)

        with patch("bpmis_jira_tool.gmail_dashboard.httplib2.Http") as http_cls:
            with patch("bpmis_jira_tool.gmail_dashboard.google_auth_httplib2.AuthorizedHttp") as auth_http_cls:
                with patch("bpmis_jira_tool.gmail_dashboard.build") as build_mock:
                    result = build_drive_api_service(credentials, cache_discovery=True)
        http_cls.assert_called_once_with(timeout=20)
        auth_http_cls.assert_called_once_with(credentials, http=http_cls.return_value)
        build_mock.assert_called_once_with("drive", "v3", http=auth_http_cls.return_value, cache_discovery=True)
        self.assertEqual(result, build_mock.return_value)

        self.assertEqual(_bytes_to_text(b"hello"), "hello")
        self.assertEqual(_bytes_to_text(None), "")
        self.assertEqual(_google_drive_file_id_from_url("https://drive.google.com/file/d/file-123/view"), "file-123")
        self.assertEqual(_google_drive_file_id_from_url("https://drive.google.com/open?id=file-456"), "file-456")
        self.assertEqual(_google_drive_file_id_from_url("https://drive.google.com/open?file=file-789"), "file-789")
        self.assertEqual(_format_contact_label("", "", "  Raw   Sender  "), "Raw Sender")
        self.assertEqual(_safe_datetime_from_epoch_ms("bad", datetime.now().astimezone().tzinfo).tzinfo, datetime.now().astimezone().tzinfo)

    def test_contact_thread_query_skips_invalid_and_dedupes_contacts(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        since = now - timedelta(days=1)

        base = _build_thread_export_query(since, now)
        self.assertEqual(_build_contact_thread_export_query(since, now, ["", "not-an-email"]), base)

        query = _build_contact_thread_export_query(
            since,
            now,
            ["Alice@Example.com", "alice@example.com", "bob@example.com"],
        )

        self.assertIn("from:alice@example.com", query)
        self.assertIn("to:alice@example.com", query)
        self.assertIn("cc:bob@example.com", query)
        self.assertEqual(query.count("from:alice@example.com"), 1)

    def test_gmail_text_noise_and_thread_helpers_cover_edge_branches(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        self.assertEqual(_build_topic_thread_export_query(now, now + timedelta(hours=1), ""), _build_thread_export_query(now, now + timedelta(hours=1)))
        long_topic_terms = _build_topic_thread_export_query(
            now,
            now + timedelta(hours=1),
            "Alpha Beta Gamma Delta Epsilon Zeta Eta Theta Iota Kappa Lambda",
        )
        self.assertIn("Alpha", long_topic_terms)
        self.assertEqual(_gmail_thread_link(""), "https://mail.google.com/mail/u/0/#all")
        self.assertEqual(_decode_gmail_body_data("A==="), "")
        self.assertEqual(_html_to_text(""), "")
        self.assertEqual(_clean_export_body_text(""), "[body unavailable]")
        self.assertEqual(_clean_export_body_text("Hello\n> quoted"), "Hello")
        self.assertEqual(_clean_export_body_text("Hello\nFrom: older@example.com"), "Hello")
        self.assertEqual(_clean_export_body_text("Hello\n-----Original Message-----\nold"), "Hello")
        self.assertEqual(_clean_export_body_text("Hello\nRegards,\nAlice"), "Hello")
        self.assertEqual(_clean_export_body_text("Hello\nunsubscribe here"), "Hello")
        self.assertTrue(_is_export_noise({"from": "reports.dwh@maribank.com.sg", "subject": "Data"}))
        self.assertTrue(_is_export_noise({"from": "Alice <alice@example.com>", "subject": "requests access to an item"}))
        self.assertTrue(_is_export_noise({"from": "calendar@example.com", "subject": "Project invitation"}))
        self.assertTrue(_is_export_noise({"from": "calendar@example.com", "subject": "Invitation: review"}))
        with patch("bpmis_jira_tool.gmail_dashboard.is_gmail_noise", return_value=True):
            self.assertTrue(_is_export_noise({"from": "Alice <alice@example.com>", "subject": "Project"}, {"rules": []}))
        self.assertEqual(_trim_preview_text(""), "")
        self.assertEqual(_extract_message_text_from_payload("not-a-dict"), "[body unavailable]")
        self.assertEqual(_extract_gmail_attachments_from_payload("not-a-dict"), [])
        self.assertEqual(_extract_drive_links_from_payload("not-a-dict"), [])

        context_message = GmailExportRecord(
            internal_date=now,
            headers={"from": "VIP <vip@example.com>", "subject": "Alpha"},
            body_text="Alpha body",
            context_only=True,
        )
        plain_message = GmailExportRecord(
            internal_date=now,
            headers={"from": "Other <other@example.com>", "subject": "Other"},
            body_text="Other body",
        )
        self.assertFalse(_thread_has_contact_window_message([], []))
        self.assertFalse(_thread_has_contact_window_message([context_message], ["vip@example.com"]))
        self.assertFalse(_thread_matches_topic([plain_message], ""))
        self.assertFalse(_thread_matches_topic([plain_message], "the and for"))
        self.assertFalse(_thread_matches_topic([context_message], "id"))
        self.assertEqual(
            _thread_monthly_requirements_market(
                [plain_message],
                [{"market": "PH", "sender": "owner@npt.sg", "subject": "Monthly"}],
            ),
            "",
        )
        self.assertEqual(
            _thread_monthly_requirements_market(
                [GmailExportRecord(internal_date=now, headers={"from": "Owner <owner@npt.sg>", "subject": "Different"}, body_text="")],
                [{"market": "PH", "sender": "owner@npt.sg", "subject": "Monthly"}],
            ),
            "",
        )

    def test_extracts_attachment_metadata_and_drive_links_from_full_payload(self):
        payload = {
            "mimeType": "multipart/mixed",
            "parts": [
                {
                    "partId": "0",
                    "filename": "Design Review.pdf",
                    "mimeType": "application/pdf",
                    "body": {"attachmentId": "att-1", "size": 1234},
                },
                {
                    "mimeType": "text/plain",
                    "body": {"data": base64.urlsafe_b64encode(b"See https://docs.google.com/presentation/d/slide123/edit").decode("utf-8")},
                },
                {
                    "mimeType": "text/html",
                    "body": {
                        "data": base64.urlsafe_b64encode(
                            b'<a href="https://drive.google.com/file/d/pdf456/view?usp=drive_link&amp;resourcekey=abc">Review deck</a>'
                        ).decode("utf-8"),
                    },
                },
            ],
        }

        attachments = _extract_gmail_attachments_from_payload(payload)
        links = _extract_drive_links_from_text(_extract_message_text_from_payload(payload))
        payload_links = _extract_drive_links_from_payload(payload)

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].filename, "Design Review.pdf")
        self.assertEqual(attachments[0].attachment_id, "att-1")
        self.assertEqual(links, ["https://docs.google.com/presentation/d/slide123/edit"])
        self.assertEqual(
            payload_links,
            [
                "https://docs.google.com/presentation/d/slide123/edit",
                "https://drive.google.com/file/d/pdf456/view?usp=drive_link&resourcekey=abc",
            ],
        )

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

    def test_network_cache_and_stale_fallback_paths(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="xiaodong.zheng@npt.sg")
        calls = {"count": 0}
        records = [
            GmailMessageRecord(
                message_id="m1",
                thread_id="t1",
                internal_date=now,
                label_ids=set(),
                headers={"from": "Alice <alice@example.com>", "to": "Bob <bob@example.com>", "cc": "Carol <carol@example.com>"},
            )
        ]

        def _list_message_metadata(*, query):
            calls["count"] += 1
            return records

        service._list_message_metadata = _list_message_metadata  # type: ignore[assignment]
        first = service.build_network(now=now)
        second = service.build_network(now=now + timedelta(seconds=30))

        self.assertEqual(first, second)
        self.assertEqual(calls["count"], 2)
        self.assertEqual(first["leaderboards"]["top_recipients"][0]["label"], "Bob <bob@example.com>")

        stale_service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="fallback@npt.sg")
        stale_service._store_cached_dashboard(kind="network", days=7, now=now - timedelta(minutes=10), dashboard=first)

        def _raise(*_args, **_kwargs):
            raise ToolError("network down")

        stale_service._list_message_metadata = _raise  # type: ignore[assignment]
        fallback = stale_service.build_network(now=now + timedelta(minutes=6))

        self.assertTrue(fallback["data_quality"]["used_fallback_cache"])

        no_stale_service = GmailDashboardService(credentials=object(), gmail_service=object())
        no_stale_service._list_message_metadata = _raise  # type: ignore[assignment]
        with self.assertRaisesRegex(ToolError, "network down"):
            no_stale_service.build_network(now=now)

        self.assertTrue(no_stale_service.is_export_noise({"from": "drive-shares-dm-noreply@google.com"}))

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

    def test_export_history_rejects_invalid_batch_and_empty_batch_has_message(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object())
        with self.assertRaisesRegex(ToolError, "Invalid Gmail export batch"):
            service.export_history_text(now=now, batch=0)

        service._list_export_messages = lambda **_kwargs: (  # type: ignore[assignment]
            [],
            {"batch": 1, "included_messages": 0, "batch_count": 0, "total_messages": 0, "truncated_bodies": 0, "capped": False, "estimated": False},
        )
        content, _filename = service.export_history_text(now=now)

        self.assertIn("No inbox messages were found in this export batch.", content)

        service._list_export_messages = lambda **_kwargs: (  # type: ignore[assignment]
            [],
            {"batch": 1, "included_messages": 0, "batch_count": 2, "total_messages": 60, "truncated_bodies": 0, "capped": False, "estimated": False},
        )
        multi_batch_content, _filename = service.export_history_text(now=now + timedelta(seconds=1))

        self.assertIn("Total batches: 2", multi_batch_content)

    def test_cached_export_content_rejects_malformed_payload_and_prewarm_delegates(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="cache@npt.sg")
        token = service._cache_token(kind="export_batch_1", days=7)
        self.assertIsNotNone(token)
        service._export_content_cache[token] = type("Entry", (), {"payload": ["bad"], "expires_at": now + timedelta(minutes=1)})()

        self.assertIsNone(service.get_cached_export_history_text(now=now))

        service._list_export_messages = lambda **_kwargs: (  # type: ignore[assignment]
            [],
            {"batch": 1, "included_messages": 0, "batch_count": 0, "total_messages": 0, "truncated_bodies": 0, "capped": False, "estimated": False},
        )
        content, filename = service.prewarm_export_history_text(now=now + timedelta(minutes=2))

        self.assertIn("No inbox messages were found", content)
        self.assertEqual(filename, "gmail-history-last-7-days-batch-1.txt")

    def test_export_internal_helpers_cover_empty_overflow_and_failure_edges(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()
        service = GmailDashboardService(credentials=object(), gmail_service=object(), cache_key="internal@npt.sg")
        service._list_message_ids = lambda **_kwargs: []  # type: ignore[assignment]

        self.assertEqual(service._list_message_metadata(query="q"), [])
        self.assertEqual(service._list_export_candidate_ids(query="q", required_count=10, now=now), [])
        self.assertEqual(service._list_export_messages(query="q", batch=1, now=now)[0], [])

        service._list_message_ids = lambda **_kwargs: ["m1"]  # type: ignore[assignment]
        service._fetch_message_metadata_many = lambda _ids: [  # type: ignore[assignment]
            GmailMessageRecord("m1", "t1", now, set(), {"from": "Alice <alice@example.com>"})
        ]
        with self.assertRaisesRegex(ToolError, "no longer available"):
            service._list_export_messages(query="q", batch=2, now=now)

        empty_many_service = GmailDashboardService(credentials=object(), gmail_service=object())
        self.assertEqual(empty_many_service._fetch_message_metadata_many([]), [])
        self.assertEqual(empty_many_service._fetch_message_full_many([]), [])
        self.assertEqual(
            service._build_daily_series(
                [GmailMessageRecord("m1", "t1", now - timedelta(hours=1), set(), {})],
                start=datetime.combine(now.date(), datetime.min.time(), tzinfo=now.tzinfo),
                days=1,
            )[0]["count"],
            1,
        )

    def test_message_ref_pagination_and_full_fetch_edges(self):
        now = datetime(2026, 4, 21, 16, 0).astimezone()

        class _PagingMessagesApi:
            def __init__(self):
                self.calls = []

            def list(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs.get("pageToken") is None:
                    return _Execute(payload={"messages": [{"id": ""}, {"id": "m1", "threadId": "t1"}], "nextPageToken": "next"})
                return _Execute(payload={"messages": [{"id": "m2", "threadId": ""}]})

            def get(self, **kwargs):
                return _Execute(
                    payload={
                        "id": kwargs["id"],
                        "threadId": "thread-full",
                        "internalDate": str(int(now.timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [{"name": "From", "value": "Alice <alice@example.com>"}],
                            "body": {"data": base64.urlsafe_b64encode(b"Body").decode("utf-8")},
                        },
                    }
                )

        class _PagingUsersApi:
            def __init__(self):
                self.messages_api = _PagingMessagesApi()

            def messages(self):
                return self.messages_api

        class _PagingService:
            def __init__(self):
                self.users_api = _PagingUsersApi()

            def users(self):
                return self.users_api

        gmail_service = _PagingService()
        service = GmailDashboardService(credentials=object(), gmail_service=gmail_service)

        refs = service._list_message_refs(query="newer_than:2d", max_messages=2)
        record = service._fetch_message_full("m1")

        self.assertEqual(refs, [{"id": "m1", "threadId": "t1"}, {"id": "m2", "threadId": ""}])
        self.assertEqual(record.message_id, "m1")
        self.assertEqual(record.body_text, "Body")

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

    def test_export_thread_history_since_empty_fallback_future_and_truncated_edges(self):
        now = datetime(2026, 4, 21, 19, 0).astimezone()
        since = now - timedelta(hours=24)
        empty_service = GmailDashboardService(credentials=object(), gmail_service=object())
        empty_service._list_message_refs = lambda **_kwargs: []  # type: ignore[assignment]

        empty = empty_service.export_thread_history_since(since=since, now=now)

        self.assertIn("No Gmail messages were found in this window.", empty)

        service = GmailDashboardService(credentials=object(), gmail_service=object())
        service._list_message_refs = lambda **_kwargs: [{"id": "fallback"}]  # type: ignore[assignment]
        service._fetch_message_metadata_many = lambda _ids: [  # type: ignore[assignment]
            GmailMessageRecord("fallback", "thread-from-metadata", now, set(), {"from": "Fallback <fallback@example.com>"})
        ]
        long_body = "A" * 20
        service._fetch_thread_messages = lambda **_kwargs: [  # type: ignore[assignment]
            GmailExportRecord(
                internal_date=now - timedelta(hours=1),
                headers={"from": "Fallback <fallback@example.com>", "subject": "Fallback subject"},
                body_text=long_body,
                body_truncated=True,
                message_id="fallback",
                thread_id="thread-from-metadata",
                label_ids={"INBOX"},
            )
        ]

        content = service.export_thread_history_since(since=since, now=now)

        self.assertIn("Thread ID: thread-from-metadata", content)
        self.assertIn("[body truncated]", content)

    def test_fetch_thread_messages_skips_future_and_truncates_body(self):
        now = datetime(2026, 4, 21, 19, 0).astimezone()
        since = now - timedelta(hours=24)
        long_body = "B" * 20

        thread_payloads = {
            "t1": {
                "messages": [
                    {
                        "id": "future",
                        "threadId": "t1",
                        "internalDate": str(int((now + timedelta(minutes=1)).timestamp() * 1000)),
                        "payload": {"headers": [{"name": "Subject", "value": "Future"}], "body": {"data": base64.urlsafe_b64encode(b"future").decode()}},
                    },
                    {
                        "id": "past",
                        "threadId": "t1",
                        "internalDate": str(int((now - timedelta(hours=1)).timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [{"name": "Subject", "value": "Past"}],
                            "body": {"data": base64.urlsafe_b64encode(long_body.encode()).decode()},
                        },
                    },
                ]
            }
        }
        service = GmailDashboardService(credentials=object(), gmail_service=_FakeGmailService({}, {}, thread_payloads))

        records = service._fetch_thread_messages(thread_id="t1", since=since, now=now, max_body_chars=5)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].message_id, "past")
        self.assertTrue(records[0].body_truncated)
        self.assertEqual(records[0].body_text, "BBBBB\n...")

    def test_export_topic_thread_history_since_searches_full_gmail_by_topic(self):
        now = datetime(2026, 4, 21, 19, 0).astimezone()
        since = now - timedelta(hours=24)
        topic = "CIB Phase 2"
        query = _build_topic_thread_export_query(since, now, topic)
        in_window = datetime(2026, 4, 21, 9, 15, tzinfo=now.tzinfo)
        list_payloads = {
            (query, None): {
                "messages": [{"id": "m1", "threadId": "t1"}],
            },
        }
        thread_payloads = {
            "t1": {
                "id": "t1",
                "messages": [
                    {
                        "id": "m1",
                        "threadId": "t1",
                        "internalDate": str(int(in_window.timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "Alice Example <alice@example.com>"},
                                {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                                {"name": "Subject", "value": "CIB Phase 2"},
                            ],
                            "parts": [
                                {
                                    "mimeType": "text/plain",
                                    "body": {"data": base64.urlsafe_b64encode(b"CIB Phase 2 release owner confirmed.").decode("utf-8")},
                                }
                            ],
                        },
                    }
                ],
            }
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, {}, thread_payloads),
        )

        payload = service.export_topic_thread_history_since(since=since, now=now, topic=topic, max_threads=3)

        self.assertIn("after:", query)
        self.assertIn("before:", query)
        self.assertIn("-in:spam -in:trash", query)
        self.assertIn('"CIB Phase 2"', query)
        self.assertEqual(payload["thread_count"], 1)
        self.assertEqual(payload["message_count"], 1)
        self.assertEqual(payload["query"], query)
        self.assertIn("Gmail topic thread history export", payload["text"])
        self.assertIn("CIB Phase 2 release owner confirmed.", payload["text"])

    def test_contact_topic_and_monthly_thread_exports_cover_empty_and_fallback_edges(self):
        now = datetime(2026, 5, 8, 12, 0).astimezone()
        since = now - timedelta(days=7)
        service = GmailDashboardService(credentials=object(), gmail_service=object())

        self.assertEqual(service.export_contact_thread_history_since(since=since, now=now, contact_emails=[])["thread_count"], 0)
        self.assertEqual(service.export_topic_thread_history_since(since=since, now=now, topic=" ")["query"], _build_thread_export_query(since, now))
        self.assertEqual(service.export_monthly_requirements_thread_history_since(since=since, now=now, configs={})["thread_count"], 0)

        vip_message = GmailExportRecord(
            internal_date=now - timedelta(hours=1),
            headers={
                "from": "VIP <vip@example.com>",
                "to": "Owner <owner@npt.sg>",
                "cc": "",
                "subject": "VIP Risk Review",
                "message-id": "<vip-1>",
            },
            body_text="VIP update body",
            body_truncated=True,
            message_id="vip-1",
            thread_id="vip-thread",
            label_ids={"INBOX"},
        )
        non_vip_message = GmailExportRecord(
            internal_date=now - timedelta(hours=1),
            headers={"from": "Other <other@example.com>", "subject": "Other"},
            body_text="Other update body",
            message_id="other-1",
            thread_id="other-thread",
        )
        service._list_message_refs = lambda **_kwargs: [{"id": "fallback"}]  # type: ignore[assignment]
        service._fetch_message_metadata_many = lambda _ids: [  # type: ignore[assignment]
            GmailMessageRecord("fallback", "empty-thread", now, set(), {}),
            GmailMessageRecord("fallback", "other-thread", now, set(), {}),
            GmailMessageRecord("fallback", "vip-thread", now, set(), {}),
        ]

        def _fetch_thread_messages(**kwargs):
            if kwargs["thread_id"] == "empty-thread":
                return []
            if kwargs["thread_id"] == "other-thread":
                return [non_vip_message]
            return [vip_message]

        service._fetch_thread_messages = _fetch_thread_messages  # type: ignore[assignment]

        vip_payload = service.export_contact_thread_history_since(
            since=since,
            now=now,
            contact_emails=["VIP@EXAMPLE.com", "bad"],
            max_threads=5,
        )

        self.assertEqual(vip_payload["thread_count"], 1)
        self.assertEqual(vip_payload["message_count"], 1)
        self.assertIn("VIP Gmail thread history export", vip_payload["text"])
        self.assertIn("[body truncated]", vip_payload["text"])
        self.assertNotIn("Other update body", vip_payload["text"])

        no_vip_service = GmailDashboardService(credentials=object(), gmail_service=object())
        no_vip_service._list_message_refs = lambda **_kwargs: [{"id": "m1", "threadId": "other-thread"}]  # type: ignore[assignment]
        no_vip_service._fetch_thread_messages = lambda **_kwargs: [non_vip_message]  # type: ignore[assignment]

        no_vip = no_vip_service.export_contact_thread_history_since(
            since=since,
            now=now,
            contact_emails=["vip@example.com"],
        )

        self.assertIn("No VIP Gmail threads were found", no_vip["text"])

    def test_topic_thread_export_collects_drive_links_and_no_match_message(self):
        now = datetime(2026, 5, 8, 12, 0).astimezone()
        since = now - timedelta(days=7)
        service = GmailDashboardService(credentials=object(), gmail_service=object())
        topic_message = GmailExportRecord(
            internal_date=now - timedelta(hours=1),
            headers={"from": "Owner <owner@npt.sg>", "subject": "Alpha rollout"},
            body_text="Alpha rollout evidence",
            body_truncated=True,
            message_id="topic-1",
            thread_id="topic-thread",
            drive_links=["https://docs.google.com/spreadsheets/d/sheet-1/edit"],
        )
        service._list_message_refs = lambda **_kwargs: [{"id": "fallback"}]  # type: ignore[assignment]
        service._fetch_message_metadata_many = lambda _ids: [  # type: ignore[assignment]
            GmailMessageRecord("fallback", "topic-thread", now, set(), {})
        ]
        service._fetch_thread_messages = lambda **_kwargs: [topic_message]  # type: ignore[assignment]

        payload = service.export_topic_thread_history_since(since=since, now=now, topic="Alpha rollout")

        self.assertEqual(payload["thread_count"], 1)
        self.assertEqual(payload["drive_links"], ["https://docs.google.com/spreadsheets/d/sheet-1/edit"])
        self.assertIn("[body truncated]", payload["text"])

        no_match_service = GmailDashboardService(credentials=object(), gmail_service=object())
        no_match_service._list_message_refs = lambda **_kwargs: [{"id": "m1", "threadId": "t1"}]  # type: ignore[assignment]
        no_match_service._fetch_thread_messages = lambda **_kwargs: [
            GmailExportRecord(
                internal_date=now - timedelta(hours=1),
                headers={"subject": "Different"},
                body_text="Different body",
                message_id="m1",
                thread_id="t1",
            )
        ]

        no_match = no_match_service.export_topic_thread_history_since(since=since, now=now, topic="Alpha rollout")

        self.assertIn("No Gmail threads were found for this topic", no_match["text"])

    def test_monthly_requirements_query_uses_subject_tokens_for_suffixed_subjects(self):
        now = datetime(2026, 5, 8, 0, 0).astimezone()
        since = now - timedelta(days=25)

        query = _build_monthly_requirements_thread_export_query(
            since,
            now,
            sender="yuanfang.zhou@npt.sg",
            subject="PH_2026 Monthly Requirements Biweekly Update",
        )

        self.assertIn("from:yuanfang.zhou@npt.sg", query)
        self.assertIn("subject:PH_2026", query)
        self.assertIn("subject:Requirements", query)
        self.assertNotIn('subject:"PH_2026 Monthly Requirements Biweekly Update"', query)

    def test_monthly_requirements_export_keeps_long_body_target_rows(self):
        now = datetime(2026, 5, 8, 0, 0).astimezone()
        since = now - timedelta(days=25)
        query = _build_monthly_requirements_thread_export_query(
            since,
            now,
            sender="yuanfang.zhou@npt.sg",
            subject="PH_2026 Monthly Requirements Biweekly Update",
        )
        body = (
            "Intro\n"
            + ("filler line\n" * 450)
            + "[Strategic Project] [PH] MariBank Card on Google Pay - On Track\n"
            + "Timeline: Tech GoLive: 2026.05.21 -> 2026.06.09, LV: 2026.05.18 ~ 2026.07.17, Public: 2026.07.24\n"
        )
        message_time = now - timedelta(days=1)
        thread_payloads = {
            "t1": {
                "id": "t1",
                "messages": [
                    {
                        "id": "m1",
                        "threadId": "t1",
                        "internalDate": str(int(message_time.timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "Yuanfang Zhou <yuanfang.zhou@npt.sg>"},
                                {"name": "Subject", "value": "PH_2026 Monthly Requirements Biweekly Update_0430"},
                            ],
                            "parts": [{"mimeType": "text/plain", "body": {"data": base64.urlsafe_b64encode(body.encode()).decode("utf-8")}}],
                        },
                    }
                ],
            }
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService({(query, None): {"messages": [{"id": "m1", "threadId": "t1"}]}}, {}, thread_payloads),
        )

        payload = service.export_monthly_requirements_thread_history_since(
            since=since,
            now=now,
            configs={"PH": {"sender": "yuanfang.zhou@npt.sg", "subject": "PH_2026 Monthly Requirements Biweekly Update"}},
        )

        self.assertEqual(payload["thread_count"], 1)
        self.assertIn("MariBank Card on Google Pay", payload["text"])
        self.assertIn("Tech GoLive: 2026.05.21 -> 2026.06.09", payload["text"])
        self.assertNotIn("[body truncated]", payload["text"])

    def test_monthly_requirements_export_fallback_empty_and_truncated_edges(self):
        now = datetime(2026, 5, 8, 12, 0).astimezone()
        since = now - timedelta(days=25)
        service = GmailDashboardService(credentials=object(), gmail_service=object())
        service._list_message_refs = lambda **_kwargs: [{"id": "fallback"}]  # type: ignore[assignment]
        service._fetch_message_metadata_many = lambda _ids: [  # type: ignore[assignment]
            GmailMessageRecord("fallback", "empty-thread", now, set(), {}),
            GmailMessageRecord("fallback", "monthly-thread", now, set(), {}),
            GmailMessageRecord("fallback", "other-thread", now, set(), {}),
        ]

        monthly_message = GmailExportRecord(
            internal_date=now - timedelta(days=1),
            headers={
                "from": "Yuanfang Zhou <yuanfang.zhou@npt.sg>",
                "subject": "PH_2026 Monthly Requirements Biweekly Update_0508",
                "message-id": "<monthly-1>",
            },
            body_text="PH monthly requirements evidence",
            body_truncated=True,
            message_id="monthly-1",
            thread_id="monthly-thread",
            label_ids={"INBOX"},
        )
        other_message = GmailExportRecord(
            internal_date=now - timedelta(days=1),
            headers={"from": "Other <other@example.com>", "subject": "Other"},
            body_text="Other body",
            message_id="other-1",
            thread_id="other-thread",
        )

        def _fetch_thread_messages(**kwargs):
            if kwargs["thread_id"] == "empty-thread":
                return []
            if kwargs["thread_id"] == "other-thread":
                return [other_message]
            return [monthly_message]

        service._fetch_thread_messages = _fetch_thread_messages  # type: ignore[assignment]

        payload = service.export_monthly_requirements_thread_history_since(
            since=since,
            now=now,
            configs={"PH": {"sender": "yuanfang.zhou@npt.sg", "subject": "PH_2026 Monthly Requirements Biweekly Update"}},
            max_threads=5,
        )

        self.assertEqual(payload["thread_count"], 1)
        self.assertEqual(payload["message_count"], 1)
        self.assertIn("Market: PH", payload["text"])
        self.assertIn("[body truncated]", payload["text"])

        no_match_service = GmailDashboardService(credentials=object(), gmail_service=object())
        no_match_service._list_message_refs = lambda **_kwargs: [{"id": "m1", "threadId": "other-thread"}]  # type: ignore[assignment]
        no_match_service._fetch_thread_messages = lambda **_kwargs: [other_message]  # type: ignore[assignment]

        no_match = no_match_service.export_monthly_requirements_thread_history_since(
            since=since,
            now=now,
            configs={"PH": {"sender": "yuanfang.zhou@npt.sg", "subject": "PH_2026 Monthly Requirements Biweekly Update"}},
        )

        self.assertIn("No Monthly Requirements Gmail threads were found", no_match["text"])

    def test_topic_thread_export_requires_multiple_meaningful_topic_terms(self):
        now = datetime(2026, 5, 9, 0, 0).astimezone()
        since = now - timedelta(days=26)
        topic = "ID database capacity issue, impact and follow up actions"
        query = _build_topic_thread_export_query(since, now, topic)
        in_window = datetime(2026, 5, 8, 18, 0, tzinfo=now.tzinfo)
        list_payloads = {
            (query, None): {
                "messages": [
                    {"id": "m-noise", "threadId": "t-noise"},
                    {"id": "m-match", "threadId": "t-match"},
                ],
            },
        }
        thread_payloads = {
            "t-noise": {
                "id": "t-noise",
                "messages": [
                    {
                        "id": "m-noise",
                        "threadId": "t-noise",
                        "internalDate": str(int(in_window.timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [{"name": "Subject", "value": "General follow-up issue"}],
                            "body": {"data": base64.urlsafe_b64encode(b"Please follow up this open issue.").decode("utf-8")},
                        },
                    }
                ],
            },
            "t-match": {
                "id": "t-match",
                "messages": [
                    {
                        "id": "m-match",
                        "threadId": "t-match",
                        "internalDate": str(int(in_window.timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [{"name": "Subject", "value": "ID database capacity"}],
                            "body": {"data": base64.urlsafe_b64encode(b"ID database capacity issue impact and action owner confirmed.").decode("utf-8")},
                        },
                    }
                ],
            },
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, {}, thread_payloads),
        )

        payload = service.export_topic_thread_history_since(since=since, now=now, topic=topic, max_threads=3)

        self.assertIn("ID", query)
        self.assertIn("database", query)
        self.assertIn("capacity", query)
        self.assertEqual(payload["thread_count"], 1)
        self.assertIn("ID database capacity issue impact", payload["text"])
        self.assertNotIn("Please follow up this open issue.", payload["text"])

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

    def test_export_thread_history_excludes_self_sent_daily_briefs(self):
        since = datetime(2026, 5, 1, 8, 0).astimezone()
        now = datetime(2026, 5, 1, 13, 0).astimezone()
        thread_query = _build_thread_export_query(since, now)
        list_payloads = {
            (thread_query, None): {
                "messages": [{"id": "brief", "threadId": "daily"}, {"id": "keep", "threadId": "project"}],
            },
        }
        message_payloads = {}
        thread_payloads = {
            "daily": {
                "messages": [
                    {
                        "id": "brief",
                        "threadId": "daily",
                        "internalDate": str(int(datetime(2026, 5, 1, 12, 30).timestamp() * 1000)),
                        "labelIds": ["SENT"],
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "Xiaodong Zheng <xiaodong.zheng@npt.sg>"},
                                {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                                {"name": "Subject", "value": "Daily Brief - 2026-05-01"},
                            ],
                            "parts": [{"mimeType": "text/plain", "body": {"data": base64.urlsafe_b64encode(b"Repeated brief item").decode("utf-8")}}],
                        },
                    }
                ],
            },
            "project": {
                "messages": [
                    {
                        "id": "keep",
                        "threadId": "project",
                        "internalDate": str(int(datetime(2026, 5, 1, 10, 0).timestamp() * 1000)),
                        "labelIds": ["INBOX"],
                        "payload": {
                            "headers": [
                                {"name": "From", "value": "Alice Example <alice@example.com>"},
                                {"name": "To", "value": "xiaodong.zheng@npt.sg"},
                                {"name": "Subject", "value": "Project update"},
                            ],
                            "parts": [{"mimeType": "text/plain", "body": {"data": base64.urlsafe_b64encode(b"Fresh project signal").decode("utf-8")}}],
                        },
                    }
                ],
            },
        }
        service = GmailDashboardService(
            credentials=object(),
            gmail_service=_FakeGmailService(list_payloads, message_payloads, thread_payloads),
        )

        content = service.export_thread_history_since(since=since, now=now)

        self.assertIn("Subject: Project update", content)
        self.assertIn("Fresh project signal", content)
        self.assertNotIn("Subject: Daily Brief - 2026-05-01", content)
        self.assertNotIn("Repeated brief item", content)
        self.assertNotIn("Thread ID: daily", content)

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

    def test_drive_sheet_export_and_attachment_download_edges(self):
        class _FakeFilesApi:
            def __init__(self):
                self.calls = []

            def get(self, **kwargs):
                self.calls.append(("get", kwargs["fileId"]))
                if kwargs["fileId"] == "denied":
                    return _Execute(
                        error=HttpError(
                            resp=type("Resp", (), {"status": 403, "reason": "Forbidden"})(),
                            content=b"denied",
                        )
                    )
                if kwargs["fileId"] == "boom":
                    raise RuntimeError("drive down")
                if kwargs["fileId"] == "doc":
                    return _Execute(payload={"id": "doc", "name": "Doc", "mimeType": "application/vnd.google-apps.document"})
                return _Execute(payload={"id": kwargs["fileId"], "name": "Sheet", "mimeType": "application/vnd.google-apps.spreadsheet"})

            def export_media(self, **kwargs):
                if kwargs["fileId"] == "empty":
                    return _Execute(payload=b"")
                return _Execute(payload=("x" * (GMAIL_TOPIC_MAX_GOOGLE_SHEET_CHARS + 5)).encode())

        class _FakeDriveService:
            def __init__(self):
                self.files_api = _FakeFilesApi()

            def files(self):
                return self.files_api

        service = GmailDashboardService(credentials=object(), gmail_service=object())
        self.assertEqual(service.export_google_sheet_link_texts([], max_links=5), [])
        with patch("bpmis_jira_tool.gmail_dashboard.build_drive_api_service", return_value=_FakeDriveService()):
            items = service.export_google_sheet_link_texts(
                [
                    "",
                    "https://docs.google.com/spreadsheets/d/sheet/edit",
                    "https://docs.google.com/spreadsheets/d/sheet/edit",
                    "https://drive.google.com/file/d/doc/view",
                    "https://drive.google.com/open?id=empty",
                    "https://drive.google.com/open?id=denied",
                    "https://drive.google.com/open?id=boom",
                    "https://drive.google.com/open",
                ],
                max_links=10,
            )

        self.assertEqual(items[0]["access_status"], "ok")
        self.assertEqual(len(items[0]["text"]), GMAIL_TOPIC_MAX_GOOGLE_SHEET_CHARS)
        self.assertEqual(items[1]["access_status"], "empty")
        self.assertEqual(items[2]["access_status"], "permission_denied")
        self.assertEqual(items[3]["access_status"], "unavailable")

        class _AttachmentMessagesApi:
            def attachments(self):
                return self

            def get(self, **kwargs):
                attachment_id = kwargs["id"]
                if attachment_id == "empty":
                    return _Execute(payload={})
                if attachment_id == "bad":
                    return _Execute(payload={"data": "A==="})
                return _Execute(payload={"data": base64.urlsafe_b64encode(b"attachment bytes").decode("utf-8").rstrip("=")})

        class _AttachmentUsersApi:
            def messages(self):
                return _AttachmentMessagesApi()

        class _AttachmentService:
            def users(self):
                return _AttachmentUsersApi()

        attachment_service = GmailDashboardService(credentials=object(), gmail_service=_AttachmentService())

        self.assertEqual(attachment_service.download_attachment(message_id="m1", attachment_id="ok"), b"attachment bytes")
        self.assertEqual(attachment_service.download_attachment(message_id="m1", attachment_id="empty"), b"")
        with self.assertRaisesRegex(ToolError, "attachment payload was unreadable"):
            attachment_service.download_attachment(message_id="m1", attachment_id="bad")

    def test_retry_boundaries_and_metadata_failure_thresholds(self):
        service = GmailDashboardService(credentials=object(), gmail_service=object())
        retryable = HttpError(resp=type("Resp", (), {"status": 503, "reason": "Unavailable"})(), content=b"temporary")
        denied = HttpError(resp=type("Resp", (), {"status": 403, "reason": "Forbidden"})(), content=b"admin_policy_enforced")
        generic_denied = HttpError(resp=type("Resp", (), {"status": 401, "reason": "Unauthorized"})(), content=b"")

        self.assertTrue(service._is_retryable_gmail_http_error(retryable))
        self.assertFalse(service._is_retryable_gmail_http_error(denied))
        self.assertIn("Workspace admin policy", str(service._build_gmail_error(denied)))
        self.assertIn("Gmail API access was still denied", str(service._build_gmail_error(generic_denied)))
        self.assertIn(
            "could not be loaded",
            str(service._build_gmail_error(HttpError(resp=type("Resp", (), {"status": 418, "reason": "Teapot"})(), content=b"teapot"))),
        )

        attempts = {"count": 0}

        def _flaky_factory():
            attempts["count"] += 1
            if attempts["count"] < 3:
                return _Execute(error=TimeoutError("timeout"))
            return _Execute(payload={"ok": True})

        with patch("bpmis_jira_tool.gmail_dashboard.time_module.sleep") as sleep_mock:
            self.assertEqual(service._execute_gmail_request(_flaky_factory, transient_message="transient"), {"ok": True})
        self.assertEqual(sleep_mock.call_count, 2)

        http_attempts = {"count": 0}

        def _flaky_http_factory():
            http_attempts["count"] += 1
            if http_attempts["count"] == 1:
                return _Execute(error=retryable)
            return _Execute(payload={"ok": "http"})

        with patch("bpmis_jira_tool.gmail_dashboard.time_module.sleep") as sleep_mock:
            self.assertEqual(service._execute_gmail_request(_flaky_http_factory, transient_message="transient"), {"ok": "http"})
        sleep_mock.assert_called_once_with(1.0)

        with self.assertRaisesRegex(ToolError, "transient"):
            service._execute_gmail_request(lambda: _Execute(error=TimeoutError("timeout")), transient_message="transient")

        self.assertTrue(service._can_tolerate_metadata_failures(total=10, failures=4))
        self.assertFalse(service._can_tolerate_metadata_failures(total=10, failures=5))

        metadata_service = GmailDashboardService(credentials=object(), gmail_service=object())
        metadata_service._list_message_ids = lambda **_kwargs: ["m1", "m2"]  # type: ignore[assignment]
        metadata_service._fetch_message_metadata = lambda _message_id: (_ for _ in ()).throw(ToolError("metadata tool error"))  # type: ignore[assignment]
        with self.assertRaisesRegex(ToolError, "metadata tool error"):
            metadata_service._list_message_metadata(query="q")
        with self.assertRaisesRegex(ToolError, "metadata tool error"):
            metadata_service._fetch_message_metadata_many(["m1", "m2"])

        runtime_metadata_service = GmailDashboardService(credentials=object(), gmail_service=object())
        runtime_metadata_service._list_message_ids = lambda **_kwargs: ["ok", "bad", "bad2"]  # type: ignore[assignment]

        def _metadata_runtime_error(message_id):
            if message_id == "ok":
                return GmailMessageRecord("ok", "t1", datetime(2026, 4, 21, 16, 0).astimezone(), set(), {})
            raise RuntimeError("metadata runtime error")

        runtime_metadata_service._fetch_message_metadata = _metadata_runtime_error  # type: ignore[assignment]
        with self.assertRaisesRegex(ToolError, "Gmail data could not be loaded"):
            runtime_metadata_service._list_message_metadata(query="q")
        with self.assertRaisesRegex(ToolError, "export candidates"):
            runtime_metadata_service._fetch_message_metadata_many(["ok", "bad", "bad2"])

        full_service = GmailDashboardService(credentials=object(), gmail_service=object())
        full_service._fetch_message_full = lambda _message_id: (_ for _ in ()).throw(ToolError("full tool error"))  # type: ignore[assignment]
        with self.assertRaisesRegex(ToolError, "full tool error"):
            full_service._fetch_message_full_many(["m1", "m2"])

        runtime_full_service = GmailDashboardService(credentials=object(), gmail_service=object())

        def _full_runtime_error(message_id):
            if message_id == "ok":
                return GmailExportRecord(
                    internal_date=datetime(2026, 4, 21, 16, 0).astimezone(),
                    headers={},
                    body_text="ok",
                    message_id="ok",
                )
            raise RuntimeError("full runtime error")

        runtime_full_service._fetch_message_full = _full_runtime_error  # type: ignore[assignment]
        with self.assertRaisesRegex(ToolError, "mail history"):
            runtime_full_service._fetch_message_full_many(["ok", "bad", "bad2"])


if __name__ == "__main__":
    unittest.main()
