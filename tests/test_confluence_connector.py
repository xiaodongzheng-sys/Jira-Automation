import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from bs4 import BeautifulSoup

from prd_briefing.confluence import ConfluenceConnector, ParsedSection, ResolvedPageRef, SpreadsheetLink
from prd_briefing.storage import BriefingStore


class ConfluenceConnectorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = BriefingStore(Path(self.temp_dir.name))
        self.connector = ConfluenceConnector(
            base_url="https://confluence.shopee.io",
            email="user@example.com",
            api_token="token",
            bearer_token=None,
            store=self.store,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_resolve_display_url_extracts_space_and_title(self):
        resolved = self.connector._resolve_page("https://confluence.shopee.io/display/SPDB/8.1+Part+1+of+PRD")

        self.assertEqual(resolved.base_url, "https://confluence.shopee.io")
        self.assertEqual(resolved.space_key, "SPDB")
        self.assertEqual(resolved.title_hint, "8.1 Part 1 of PRD")
        self.assertIsNone(resolved.page_id)

    def test_resolve_viewpage_url_extracts_space_and_title_query(self):
        resolved = self.connector._resolve_page(
            "https://confluence.shopee.io/pages/viewpage.action?spaceKey=SPDB&title=8.1+Part+1+of+PRD"
        )

        self.assertEqual(resolved.base_url, "https://confluence.shopee.io")
        self.assertEqual(resolved.space_key, "SPDB")
        self.assertEqual(resolved.title_hint, "8.1 Part 1 of PRD")
        self.assertIsNone(resolved.page_id)

    @patch("prd_briefing.confluence.requests.get")
    def test_resolve_short_link_follows_redirect_to_display_url(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.url = "https://confluence.shopee.io/display/SPDB/8.1+Part+1+of+PRD"
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        resolved = self.connector._resolve_page("https://confluence.shopee.io/x/tUdvuw")

        self.assertEqual(resolved.base_url, "https://confluence.shopee.io")
        self.assertEqual(resolved.space_key, "SPDB")
        self.assertEqual(resolved.title_hint, "8.1 Part 1 of PRD")
        self.assertIsNone(resolved.page_id)
        self.assertEqual(mock_get.call_args.kwargs["headers"]["Accept"], "text/html")
        self.assertTrue(mock_get.call_args.kwargs["allow_redirects"])

    @patch("prd_briefing.confluence.requests.get")
    def test_resolve_short_link_follows_redirect_to_page_id_url(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.url = "https://confluence.shopee.io/pages/viewpage.action?pageId=12345"
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        resolved = self.connector._resolve_page("https://confluence.shopee.io/x/tUdvuw")

        self.assertEqual(resolved.base_url, "https://confluence.shopee.io")
        self.assertEqual(resolved.page_id, "12345")
        self.assertIsNone(resolved.space_key)

    @patch("prd_briefing.confluence.requests.get")
    def test_ingest_display_url_uses_rest_api_search(self, mock_get):
        payload = {
            "results": [
                {
                    "id": "12345",
                    "title": "8.1 Part 1 of PRD",
                    "version": {"when": "2026-04-15T12:00:00Z", "number": 7},
                    "body": {
                        "storage": {"value": "<h1>Overview</h1><p>Hello Storage PRD</p>"},
                        "export_view": {"value": "<h1>Overview</h1><p>Hello Export PRD</p>"},
                    },
                }
            ]
        }
        response = Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        page = self.connector.ingest_page("https://confluence.shopee.io/display/SPDB/8.1+Part+1+of+PRD", "session-1")

        self.assertEqual(page.page_id, "12345")
        self.assertEqual(page.version_number, "7")
        self.assertEqual(page.title, "8.1 Part 1 of PRD")
        self.assertEqual(page.sections[0].content, "Hello Storage PRD")
        request_url = mock_get.call_args.kwargs.get("url") or mock_get.call_args.args[0]
        self.assertTrue(request_url.endswith("/rest/api/content"))

    @patch("prd_briefing.confluence.requests.get")
    def test_ingest_falls_back_to_export_view_when_storage_missing(self, mock_get):
        response = Mock()
        response.json.return_value = {
            "id": "12345",
            "title": "Fallback PRD",
            "version": {"when": "2026-04-15T12:00:00Z", "number": 3},
            "body": {"export_view": {"value": "<h1>Overview</h1><p>Export body</p>"}},
        }
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        page = self.connector.ingest_page("https://confluence.shopee.io/pages/viewpage.action?pageId=12345", "session-1")

        self.assertEqual(page.version_number, "3")
        self.assertEqual(page.sections[0].content, "Export body")

    @patch("prd_briefing.confluence.requests.get")
    def test_ingest_uses_export_view_when_storage_contains_image_macros(self, mock_get):
        response = Mock()
        response.json.return_value = {
            "id": "12345",
            "title": "Image Macro PRD",
            "version": {"when": "2026-04-15T12:00:00Z", "number": 3},
            "body": {
                "storage": {
                    "value": """
                    <h2>Status Machine</h2>
                    <table>
                      <tr><th>Status Machine</th><th>Remarks</th></tr>
                      <tr><td><ac:image><ri:attachment ri:filename="status.png" /></ac:image></td><td>Storage text</td></tr>
                    </table>
                    """
                },
                "export_view": {
                    "value": """
                    <h2>Status Machine</h2>
                    <div class="table-wrap"><table>
                      <tr><th>Status Machine</th><th>Remarks</th></tr>
                      <tr><td><img src="https://confluence.shopee.io/download/attachments/embedded-page/SPDB/Page/status.png?api=v2" /></td><td>Export text</td></tr>
                    </table></div>
                    """
                },
            },
        }
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        page = self.connector.ingest_page("https://confluence.shopee.io/pages/viewpage.action?pageId=12345", "session-1")

        self.assertIn("[MEDIA_ID_1]", page.sections[0].content)
        table_html = page.media_dict["MEDIA_ID_1"]["content"]
        self.assertIn("Export text", table_html)
        self.assertNotIn("Storage text", table_html)
        self.assertIn("<img", table_html)
        self.assertIn("/prd-briefing/image-proxy?src=", table_html)
        self.assertIn("embedded-page", table_html)

    def test_media_extraction_filters_noise_and_sanitizes_tables(self):
        media = {}
        sections = self.connector._parse_sections(
            html="""
            <h1>Media</h1>
            <p>Useful diagram below</p>
            <img src="/download/attachments/1/flow.png" width="640" height="360" />
            <img src="/images/icons/comment_16.png" width="16" height="16" />
            <img src="/profilepics/avatar.png" width="120" height="120" />
            <table><tr><td>Left</td><td>Right</td></tr></table>
            <table class="x" onclick="bad()">
              <tr><th class="c">Field</th><th style="color:red">Rule</th></tr>
              <tr><td>Assessment Status</td><td><script>alert(1)</script>Must be Draft before submit.</td></tr>
            </table>
            """,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/pages/viewpage.action?pageId=1",
            session_id="session-1",
            media_dict=media,
        )

        self.assertEqual(len(media), 2)
        self.assertIn("[MEDIA_ID_1]", sections[0].content)
        self.assertIn("[MEDIA_ID_2]", sections[0].content)
        self.assertEqual(media["MEDIA_ID_1"]["type"], "image")
        self.assertEqual(media["MEDIA_ID_2"]["type"], "table")
        self.assertIn("/prd-briefing/image-proxy?src=", media["MEDIA_ID_1"]["content"])
        table_html = media["MEDIA_ID_2"]["content"]
        self.assertIn("<table>", table_html)
        self.assertIn("<th>Field</th>", table_html)
        self.assertNotIn("script", table_html)
        self.assertNotIn("class=", table_html)
        self.assertNotIn("style=", table_html)
        self.assertNotIn("onclick", table_html)

    def test_table_media_preserves_cell_images_through_proxy(self):
        media = {}
        sections = self.connector._parse_sections(
            html="""
            <h1>Status Machine</h1>
            <table>
              <tr><th>Status Machine</th><th>Remarks</th></tr>
              <tr>
                <td><img src="/download/attachments/1/status-machine.png" width="720" height="420" /></td>
                <td>Show state transitions.</td>
              </tr>
            </table>
            """,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/pages/viewpage.action?pageId=1",
            session_id="session-1",
            media_dict=media,
        )

        self.assertIn("[MEDIA_ID_1]", sections[0].content)
        self.assertEqual(media["MEDIA_ID_1"]["type"], "table")
        table_html = media["MEDIA_ID_1"]["content"]
        self.assertIn("<img", table_html)
        self.assertIn("/prd-briefing/image-proxy?src=", table_html)
        self.assertIn("https%3A%2F%2Fconfluence.shopee.io%2Fdownload%2Fattachments%2F1%2Fstatus-machine.png", table_html)
        self.assertIn('loading="lazy"', table_html)
        self.assertNotIn("/download/attachments/1/status-machine.png", table_html)

    @patch("prd_briefing.confluence.requests.get")
    def test_ingest_display_url_follows_confluence_renamed_page_suggestion(self, mock_get):
        empty_search = Mock()
        empty_search.json.return_value = {"results": []}
        empty_search.raise_for_status.return_value = None
        login_html = Mock()
        login_html.json.side_effect = ValueError("not json")
        login_html.raise_for_status.return_value = None
        renamed_hint = Mock()
        renamed_hint.text = """
        <html><body>
          <p>The page you were looking for may have been renamed to the following:</p>
          <a href="/display/SPDB/%5BID%5D%5BDWH%5D+Group+Employee+Tag+Data">[ID][DWH] Group Employee Tag Data</a>
        </body></html>
        """
        renamed_hint.raise_for_status.return_value = None
        renamed_payload = Mock()
        renamed_payload.json.return_value = {
            "results": [
                {
                    "id": "67890",
                    "title": "[ID][DWH] Group Employee Tag Data",
                    "version": {"when": "2026-04-29T12:00:00Z"},
                    "body": {"export_view": {"value": "<h1>Overview</h1><p>Renamed PRD</p>"}},
                }
            ]
        }
        renamed_payload.raise_for_status.return_value = None
        mock_get.side_effect = [empty_search, login_html, renamed_hint, renamed_payload]

        page = self.connector.ingest_page(
            "https://confluence.shopee.io/display/SPDB/%5BID%5D%5BDWH%5D+Group+Employee+Tag+Table",
            "session-1",
        )

        self.assertEqual(page.page_id, "67890")
        self.assertEqual(page.title, "[ID][DWH] Group Employee Tag Data")
        self.assertEqual(page.sections[0].content, "Renamed PRD")
        request_urls = [
            call.kwargs.get("url") or call.args[0]
            for call in mock_get.call_args_list
        ]
        self.assertIn("/display/SPDB/%5BID%5D%5BDWH%5D+Group+Employee+Tag+Table", request_urls[2])
        self.assertIn("/rest/api/content", request_urls[3])

    @patch("prd_briefing.confluence.requests.get")
    def test_ingest_display_url_searches_similar_title_when_renamed_hint_is_unavailable(self, mock_get):
        empty_search = Mock()
        empty_search.json.return_value = {"results": []}
        empty_search.raise_for_status.return_value = None
        login_html = Mock()
        login_html.json.side_effect = ValueError("not json")
        login_html.raise_for_status.return_value = None
        no_hint = Mock()
        no_hint.text = "<html><body><p>Page Not Found</p></body></html>"
        no_hint.raise_for_status.return_value = None
        cql_result = Mock()
        cql_result.json.return_value = {
            "results": [
                {
                    "url": "/display/SPDB/%5BID%5D%5BDWH%5D+Group+Employee+Tag+Data",
                    "content": {"id": "67890", "title": "[ID][DWH] Group Employee Tag Data"},
                }
            ]
        }
        cql_result.raise_for_status.return_value = None
        renamed_payload = Mock()
        renamed_payload.json.return_value = {
            "id": "67890",
            "title": "[ID][DWH] Group Employee Tag Data",
            "version": {"when": "2026-04-29T12:00:00Z"},
            "body": {"export_view": {"value": "<h1>Overview</h1><p>Renamed PRD</p>"}},
        }
        renamed_payload.raise_for_status.return_value = None
        mock_get.side_effect = [empty_search, login_html, no_hint, cql_result, renamed_payload]

        page = self.connector.ingest_page(
            "https://confluence.shopee.io/display/SPDB/%5BID%5D%5BDWH%5D+Group+Employee+Tag+Table",
            "session-1",
        )

        self.assertEqual(page.page_id, "67890")
        self.assertEqual(page.title, "[ID][DWH] Group Employee Tag Data")
        self.assertEqual(page.sections[0].content, "Renamed PRD")
        request_urls = [
            call.kwargs.get("url") or call.args[0]
            for call in mock_get.call_args_list
        ]
        self.assertTrue(request_urls[3].endswith("/rest/api/search"))
        self.assertTrue(request_urls[4].endswith("/rest/api/content/67890"))

    def test_title_search_phrases_include_tail_when_version_prefix_changed(self):
        phrases = self.connector._title_search_phrases(
            "Antifraud V3.45_0608 - UI improvements on FV instruction to discourage wrong person attempts"
        )

        self.assertIn("discourage wrong person attempts", phrases)
        self.assertIn("instruction to discourage wrong person attempts", phrases)
        self.assertIn("UI improvements on FV instruction to discourage wrong person attempts", phrases)

    def test_parse_sections_skips_toc_heading_and_keeps_real_sections(self):
        html = """
        <h1><div class='toc-macro'><ul><li><a href='#x'>1. Project Management</a></li><li><a href='#y'>2. Introduction</a></li></ul></div></h1>
        <h1>1. Project Management</h1>
        <h2>1.1 Version Control</h2>
        <p>Version control details.</p>
        <h2>1.2 People Involved</h2>
        <p>People involved details.</p>
        """
        sections = self.connector._parse_sections(
            html=html,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Test",
            session_id="session-1",
        )

        self.assertEqual(sections[0].section_path, "1.1 Version Control")
        self.assertEqual(sections[0].content, "Version control details.")
        self.assertEqual(sections[1].section_path, "1.2 People Involved")

    def test_parse_sections_preserves_table_html(self):
        html = """
        <h2>Requirements</h2>
        <table>
          <tr><th>Field</th><th>Meaning</th></tr>
          <tr><td>Status</td><td>Required</td></tr>
        </table>
        """

        sections = self.connector._parse_sections(
            html=html,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Test",
            session_id="session-1",
        )

        self.assertIn("<table>", sections[0].html_content)
        self.assertIn("<th>Field</th>", sections[0].html_content)

    def test_parse_sections_removes_strikethrough_content(self):
        html = """
        <h2>Requirements <s>Old Heading</s></h2>
        <p>Keep this <s>remove old text</s><span style="text-decoration: line-through;">remove styled text</span></p>
        <ul><li>Keep item <del>delete item part</del></li></ul>
        <table>
          <tr><th>Field</th><th>Meaning</th></tr>
          <tr><td>Status</td><td><strike>Old</strike>Required</td></tr>
        </table>
        """

        sections = self.connector._parse_sections(
            html=html,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Test",
            session_id="session-1",
        )

        self.assertEqual(sections[0].section_path, "Requirements")
        self.assertIn("Keep this", sections[0].content)
        self.assertIn("Keep item", sections[0].content)
        self.assertIn("Meaning: Required", sections[0].content)
        self.assertNotIn("Old Heading", sections[0].content)
        self.assertNotIn("remove old text", sections[0].content)
        self.assertNotIn("remove styled text", sections[0].content)
        self.assertNotIn("delete item part", sections[0].content)
        self.assertNotIn("Old", sections[0].html_content)
        self.assertNotIn("remove old text", sections[0].html_content)
        self.assertNotIn("remove styled text", sections[0].html_content)

    def test_parse_sections_drops_table_when_strikethrough_leaves_only_markers(self):
        html = """
        <h2>Requirements</h2>
        <p>Keep visible requirement.</p>
        <div class="table-wrap">
          <table>
            <tr><th><s>Function</s></th><th><s>Steps</s></th></tr>
            <tr><td><s>Old function</s></td><td><ol><li><s>Removed step</s></li></ol></td></tr>
            <tr><td><span style="text-decoration: line-through;">Removed field</span></td><td>1.<br/>2.<br/>3.</td></tr>
            <tr><td>○</td><td>o</td></tr>
          </table>
        </div>
        <p>Keep after table.</p>
        """

        sections = self.connector._parse_sections(
            html=html,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Test",
            session_id="session-1",
        )

        self.assertEqual(len(sections), 1)
        self.assertIn("Keep visible requirement.", sections[0].content)
        self.assertIn("Keep after table.", sections[0].content)
        self.assertNotIn("<table", sections[0].html_content)
        self.assertNotIn("Removed step", sections[0].html_content)
        self.assertNotIn("1.", sections[0].content)

    def test_parse_sections_drops_marker_only_rows_after_strikethrough(self):
        html = """
        <h2>Requirements</h2>
        <table>
          <tr><th>Requirement</th><th>UI Reference</th></tr>
          <tr><td>Keep this requirement.</td><td><img src="/download/attachments/123/keep.png" /></td></tr>
          <tr>
            <td><ol><li><s>Removed first option</s></li><li><s>Removed second option</s></li></ol></td>
            <td>↓<br/>↓</td>
          </tr>
          <tr><td>Keep another requirement.</td><td>Required</td></tr>
        </table>
        """

        sections = self.connector._parse_sections(
            html=html,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Test",
            session_id="session-1",
        )

        self.assertIn("<table", sections[0].html_content)
        self.assertIn("Keep this requirement.", sections[0].html_content)
        self.assertIn("Keep another requirement.", sections[0].html_content)
        self.assertNotIn("Removed first option", sections[0].html_content)
        self.assertNotIn("<td><ol></ol></td>", sections[0].html_content)
        self.assertNotIn("↓", sections[0].html_content)

    def test_parse_sections_rewrites_image_src_to_proxy(self):
        html = """
        <h2>Screenshots</h2>
        <p><img src="/download/attachments/123/test.png" /></p>
        """

        sections = self.connector._parse_sections(
            html=html,
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Test",
            session_id="session-1",
        )

        self.assertIn("/prd-briefing/image-proxy?src=", sections[0].html_content)
        self.assertIn("https%3A%2F%2Fconfluence.shopee.io%2Fdownload%2Fattachments%2F123%2Ftest.png", sections[0].html_content)

    def test_resolve_page_supports_raw_ids_path_ids_and_rejects_unknown_urls(self):
        resolved = self.connector._resolve_page("12345")

        self.assertEqual(resolved.page_id, "12345")
        self.assertEqual(resolved.source_url, "https://confluence.shopee.io/pages/viewpage.action?pageId=12345")

        path_resolved = self.connector._resolve_page("https://confluence.shopee.io/pages/67890")
        self.assertEqual(path_resolved.page_id, "67890")

        no_base_connector = ConfluenceConnector(
            base_url=None,
            email=None,
            api_token=None,
            bearer_token=None,
            store=self.store,
        )
        with self.assertRaisesRegex(ValueError, "raw Confluence page ID"):
            no_base_connector._resolve_page("12345")
        with self.assertRaisesRegex(ValueError, "supported Confluence page reference"):
            self.connector._resolve_page("https://confluence.shopee.io/not/a/page")

    @patch("prd_briefing.confluence.requests.get")
    def test_resolve_short_link_rejects_unresolved_redirect(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.url = "https://confluence.shopee.io/x/tUdvuw"
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        with self.assertRaisesRegex(ValueError, "short link"):
            self.connector._resolve_page("https://confluence.shopee.io/x/tUdvuw")

    def test_fetch_page_payload_reports_page_id_and_display_failures(self):
        by_id = ResolvedPageRef(
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/pages/viewpage.action?pageId=404",
            page_id="404",
        )
        with patch.object(self.connector, "_request", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, "Could not fetch Confluence page by ID"):
                self.connector._fetch_page_payload(by_id)

        by_title = ResolvedPageRef(
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Missing",
            space_key="SPDB",
            title_hint="Missing",
        )
        empty = Mock()
        empty.json.return_value = {"results": []}
        with patch.object(self.connector, "_request", return_value=empty), \
            patch.object(self.connector, "_resolve_renamed_display_page", return_value=None), \
            patch.object(self.connector, "_search_similar_display_page", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "Could not resolve Confluence display URL"):
                self.connector._fetch_page_payload(by_title)

    def test_extract_ancestor_titles_and_rest_candidates_handle_edges(self):
        titles = self.connector._extract_ancestor_titles(
            {
                "ancestors": [
                    {"title": "Root"},
                    {"title": "Root"},
                    "bad",
                    {"title": "Child"},
                    {"title": " "},
                ]
            }
        )

        self.assertEqual(titles, ["Root", "Child"])
        self.assertEqual(self.connector._extract_ancestor_titles({"ancestors": "bad"}), [])
        self.assertEqual(self.connector._extract_ancestor_titles(None), [])
        self.assertEqual(
            self.connector._rest_api_candidates("https://example.atlassian.net/wiki"),
            ["https://example.atlassian.net/wiki/rest/api", "https://example.atlassian.net/wiki/wiki/rest/api"],
        )

    def test_resolve_renamed_display_page_ignores_bad_hints(self):
        resolved = ResolvedPageRef(
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Old",
            space_key="SPDB",
            title_hint="Old",
        )

        with patch.object(self.connector, "_request", side_effect=RuntimeError("offline")):
            self.assertIsNone(self.connector._resolve_renamed_display_page(resolved))

        html = """
        <a href="">No href</a>
        <a href="/display/SPDB/Old">Old</a>
        <a href="/bad/path">New Page</a>
        <a href="/pages/viewpage.action?pageId=987">Useful Page</a>
        """
        response = Mock(text=html)
        with patch.object(self.connector, "_request", return_value=response):
            renamed = self.connector._resolve_renamed_display_page(resolved)

        self.assertEqual(renamed.page_id, "987")

    def test_search_similar_display_page_handles_malformed_results(self):
        missing_title = ResolvedPageRef(
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Missing",
            space_key="SPDB",
            title_hint=None,
        )
        self.assertIsNone(self.connector._search_similar_display_page(missing_title))

        payload = Mock()
        payload.json.return_value = {
            "results": [
                "bad",
                {"content": "bad"},
                {"content": {}},
                {"link": "/display/SPDB/Fallback", "content": {"title": "Fallback Title"}},
            ]
        }
        resolved = ResolvedPageRef(
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Missing",
            space_key="SPDB",
            title_hint="Missing Page",
        )
        with patch.object(self.connector, "_request", side_effect=[RuntimeError("search down"), payload]):
            similar = self.connector._search_similar_display_page(resolved)

        self.assertEqual(similar.title_hint, "Fallback Title")
        self.assertEqual(similar.space_key, "SPDB")

        empty_payload = Mock()
        empty_payload.json.return_value = {"results": [{"content": {}}]}
        with patch.object(self.connector, "_request", return_value=empty_payload):
            self.assertIsNone(self.connector._search_similar_display_page(resolved))

    def test_spreadsheet_links_include_regular_google_and_attachment_links(self):
        soup = BeautifulSoup(
            """
            <div>
              <a href="/download/report.xlsx">Report Sheet</a>
              <a href="https://docs.google.com/spreadsheets/d/abc">Google Sheet</a>
              <ri:attachment ri:filename="evidence.xlsm"></ri:attachment>
              <ri:attachment ri:filename="notes.pdf"></ri:attachment>
            </div>
            """,
            "html.parser",
        )

        links = self.connector._extract_spreadsheet_links(
            soup.div,
            base_url="https://confluence.shopee.io",
            page_id="12345",
            section_path="Evidence",
        )

        self.assertEqual([link.kind for link in links], ["link", "link", "confluence_attachment"])
        self.assertEqual(links[0].filename, "report.xlsx")
        self.assertEqual(links[1].filename, "Google Sheet")
        self.assertIn("/download/attachments/12345/evidence.xlsm", links[2].url)

    def test_dedupes_spreadsheet_links_and_filename_fallbacks(self):
        links = self.connector._dedupe_spreadsheet_links(
            [
                SpreadsheetLink(title="", url="", source_section="S", filename=""),
                SpreadsheetLink(title="B", url="https://x/report.xls", source_section="S", filename=""),
                SpreadsheetLink(title="C", url="https://x/report.xls", source_section="S", filename=""),
                SpreadsheetLink(title="D", url="", source_section="S", filename="manual.xlsx"),
            ]
        )

        self.assertEqual([link.title for link in links], ["B", "D"])
        self.assertEqual(self.connector._filename_from_url_or_title("https://x/download?id=1", "fallback.xls"), "fallback.xls")
        self.assertEqual(self.connector._filename_from_url_or_title("", "Plain title"), "Plain title")

    def test_media_registration_and_confluence_image_resolution_edges(self):
        media = {}
        soup = BeautifulSoup(
            """
            <div>
              <img />
              <img src="/$icon.png" width="640" height="360" />
              <img src="/download/full.png" width="640" height="360" />
              <ac:image ac:width="640"><ri:url ri:value="/download/external.png"></ri:url></ac:image>
              <ac:image><ri:attachment ri:filename="diagram.png"></ri:attachment></ac:image>
              <ac:image><ri:attachment></ri:attachment></ac:image>
            </div>
            """,
            "html.parser",
        )

        images = soup.find_all("img")
        self.assertIsNone(self.connector._register_image_media(images[0], base_url="https://confluence.shopee.io", media_dict=media, section_path="S"))
        self.assertIsNone(self.connector._register_image_media(images[1], base_url="https://confluence.shopee.io", media_dict=media, section_path="S"))
        self.assertEqual(
            self.connector._register_image_media(images[2], base_url="https://confluence.shopee.io", media_dict=media, section_path="S"),
            "MEDIA_ID_1",
        )
        self.assertIsNone(self.connector._register_confluence_image_media(soup.find("ac:image"), image_ref="/x.png", media_dict=None, section_path="S"))
        confluence_images = soup.find_all(self.connector._is_confluence_image_tag)
        self.assertEqual(
            self.connector._resolve_confluence_image_ref(confluence_images[0], base_url="https://confluence.shopee.io", page_id="123"),
            "https://confluence.shopee.io/download/external.png",
        )
        self.assertIn(
            "/download/attachments/123/diagram.png",
            self.connector._resolve_confluence_image_ref(confluence_images[1], base_url="https://confluence.shopee.io", page_id="123"),
        )
        self.assertEqual(
            self.connector._resolve_confluence_image_ref(confluence_images[2], base_url="https://confluence.shopee.io", page_id=""),
            "",
        )

    def test_noise_image_table_and_sanitizer_fallback_branches(self):
        soup = BeautifulSoup(
            """
            <div>
              <img src="/download/full.png" style="width: 24px; height: 24px" />
              <ac:image ac:height="24"><ri:url ri:value="/download/full.png"></ri:url></ac:image>
              <table><tr><td>Short</td><td>Pair</td></tr></table>
              <table><tr><th>Name</th><th>Rule</th></tr><tr><td>Status</td><td>Must be reviewed carefully</td></tr></table>
            </div>
            """,
            "html.parser",
        )

        self.assertTrue(self.connector._is_noise_image(soup.find("img")))
        self.assertTrue(self.connector._is_noise_confluence_image(soup.find(self.connector._is_confluence_image_tag), "/download/full.png"))
        self.assertTrue(self.connector._is_noise_confluence_image(soup.find(self.connector._is_confluence_image_tag), "/images/icons/comment_16.png"))
        tables = soup.find_all("table")
        self.assertFalse(self.connector._is_presentation_table(tables[0]))
        self.assertTrue(self.connector._is_presentation_table(tables[1]))
        self.assertIsNone(self.connector._register_table_media(tables[1], base_url="https://confluence.shopee.io", media_dict=None, section_path="S"))
        with patch.object(self.connector, "_sanitize_table_html", return_value=""):
            self.assertIsNone(self.connector._register_table_media(tables[1], base_url="https://confluence.shopee.io", media_dict={}, section_path="S"))

        with patch("prd_briefing.confluence.bleach", None):
            sanitized = self.connector._sanitize_table_html(tables[1], base_url="https://confluence.shopee.io")
        self.assertIn("<table>", sanitized)
        self.assertNotIn("onclick", sanitized)
        self.assertIsNone(self.connector._dimension_px("auto"))

        long_pair = BeautifulSoup(
            "<table><tr><td>Long enough left cell</td><td>Long enough right cell</td></tr></table>",
            "html.parser",
        ).table
        self.assertFalse(self.connector._is_presentation_table(long_pair))

        table_with_noise = BeautifulSoup(
            "<table><tr><th>Name</th><th>Image</th></tr><tr><td>Status</td><td><img src='/images/icons/comment_16.png' /></td></tr></table>",
            "html.parser",
        ).table
        self.assertNotIn("comment_16", self.connector._sanitize_table_html(table_with_noise, base_url="https://confluence.shopee.io"))

        table_with_span = BeautifulSoup(
            "<table><tr><th>Name</th><th>Rule</th></tr><tr><td><span>Status</span></td><td><div>Must be reviewed carefully</div></td></tr></table>",
            "html.parser",
        ).table
        with patch("prd_briefing.confluence.bleach", None):
            fallback = self.connector._sanitize_table_html(table_with_span, base_url="https://confluence.shopee.io")
        self.assertIn("Status", fallback)
        self.assertNotIn("<span", fallback)
        self.assertNotIn("<div", fallback)

    def test_extract_block_content_covers_confluence_lists_tables_text_and_recursion(self):
        media = {}
        base_url = "https://confluence.shopee.io"

        struck = BeautifulSoup('<p style="text-decoration: line-through;">Old</p>', "html.parser").p
        self.assertEqual(self.connector._extract_block_content(struck, base_url=base_url), ([], [], [], []))

        confluence_image = BeautifulSoup(
            '<ac:image><ri:attachment ri:filename="diagram.png"></ri:attachment></ac:image>',
            "html.parser",
        ).find(self.connector._is_confluence_image_tag)
        lines, blocks, images, refs = self.connector._extract_block_content(
            confluence_image,
            base_url=base_url,
            page_id="123",
            media_dict=media,
            section_path="S",
        )
        self.assertEqual(lines, ["[MEDIA_ID_1]"])
        self.assertEqual(blocks, [])
        self.assertIn("/download/attachments/123/diagram.png", images[0])
        self.assertEqual(refs, ["MEDIA_ID_1"])

        confluence_noise = BeautifulSoup(
            '<ac:image ac:width="16"><ri:attachment ri:filename="tiny.png"></ri:attachment></ac:image>',
            "html.parser",
        ).find(self.connector._is_confluence_image_tag)
        self.assertEqual(
            self.connector._extract_block_content(confluence_noise, base_url=base_url, page_id="123", media_dict=media),
            ([], [], [], []),
        )

        ul = BeautifulSoup(
            """
            <ul>
              <li>First <img src="/download/list.png" width="300" height="200" /></li>
              <li><ac:image><ri:attachment ri:filename="nested.png"></ri:attachment></ac:image></li>
            </ul>
            """,
            "html.parser",
        ).ul
        lines, blocks, images, refs = self.connector._extract_block_content(
            ul,
            base_url=base_url,
            page_id="123",
            media_dict=media,
            section_path="S",
        )
        self.assertIn("First", lines)
        self.assertIn("[MEDIA_ID_2]", lines)
        self.assertIn("[MEDIA_ID_3]", lines)
        self.assertTrue(blocks)
        self.assertEqual(refs[-2:], ["MEDIA_ID_2", "MEDIA_ID_3"])

        table = BeautifulSoup(
            """
            <div class="table-wrap"><table>
              <tr><th>Name</th><th>Rule</th></tr>
              <tr><td>Missing image src</td><td><img /></td></tr>
              <tr><td>Image</td><td><img src="/download/cell.png" width="640" height="360" /></td></tr>
              <tr><td>Noise macro</td><td><ac:image ac:width="16"><ri:attachment ri:filename="tiny.png"></ri:attachment></ac:image></td></tr>
              <tr><td>Macro</td><td><ac:image><ri:attachment ri:filename="macro.png"></ri:attachment></ac:image></td></tr>
            </table></div>
            """,
            "html.parser",
        ).div
        lines, blocks, images, refs = self.connector._extract_block_content(
            table,
            base_url=base_url,
            page_id="123",
            media_dict=media,
            section_path="S",
        )
        self.assertEqual(lines[0], "[MEDIA_ID_4]")
        self.assertIn("MEDIA_ID_4", refs)
        self.assertIn("MEDIA_ID_5", refs)
        self.assertIn("MEDIA_ID_6", refs)

        table_without_media = BeautifulSoup(
            """
            <table>
              <tr><th>Name</th><th>Rule</th></tr>
              <tr><td>Macro</td><td><ac:image><ri:attachment ri:filename="macro.png"></ri:attachment></ac:image></td></tr>
            </table>
            """,
            "html.parser",
        ).table
        lines, blocks, images, refs = self.connector._extract_block_content(
            table_without_media,
            base_url=base_url,
            page_id="123",
            media_dict=None,
            section_path="S",
        )
        self.assertEqual(refs, [])
        self.assertEqual(images, [])

        paragraph = BeautifulSoup(
            '<p>Keep <img src="/download/p.png" width="300" height="200" /><ac:image><ri:attachment ri:filename="p-macro.png"></ri:attachment></ac:image></p>',
            "html.parser",
        ).p
        lines, blocks, images, refs = self.connector._extract_block_content(
            paragraph,
            base_url=base_url,
            page_id="123",
            media_dict=media,
            section_path="S",
        )
        self.assertIn("Keep", lines)
        self.assertIn("[MEDIA_ID_7]", lines)
        self.assertIn("[MEDIA_ID_8]", lines)

        generic = BeautifulSoup('<div>Loose text<span>Child text</span></div>', "html.parser").div
        self.assertEqual(
            self.connector._extract_block_content(generic, base_url=base_url)[0],
            ["Loose text", "Child text"],
        )

        generic_with_comment = BeautifulSoup("<div><!-- hidden --><span>Shown</span></div>", "html.parser").div
        self.assertEqual(
            self.connector._extract_block_content(generic_with_comment, base_url=base_url)[0],
            ["hidden", "Shown"],
        )

        nested_noise = BeautifulSoup(
            '<div><ac:image ac:width="16"><ri:attachment ri:filename="tiny.png"></ri:attachment></ac:image></div>',
            "html.parser",
        ).div
        self.assertEqual(
            self.connector._extract_nested_confluence_images(
                nested_noise,
                base_url=base_url,
                page_id="123",
                media_dict=media,
                section_path="S",
            ),
            ([], [], []),
        )

    def test_table_line_rendering_and_html_cleanup_edges(self):
        empty_table = BeautifulSoup("<table><tr><td>1.</td><td>○</td></tr></table>", "html.parser").table
        self.assertEqual(self.connector._extract_table_lines(empty_table), [])

        image_only_table = BeautifulSoup("<table><tr><td><img src='/x.png' /></td></tr></table>", "html.parser").table
        self.assertEqual(self.connector._extract_table_lines(image_only_table), [])

        one_row = BeautifulSoup("<table><tr><td>Single</td><td>Row</td></tr></table>", "html.parser").table
        self.assertEqual(self.connector._extract_table_lines(one_row), ["Single | Row"])

        mismatched = BeautifulSoup(
            "<table><tr><th>A</th><th>B</th></tr><tr><td>Only A</td></tr></table>",
            "html.parser",
        ).table
        self.assertEqual(self.connector._extract_table_lines(mismatched), ["Only A"])
        self.assertEqual(self.connector._dedupe_lines([" Alpha ", "alpha", "", "Beta"]), ["Alpha", "Beta"])

        fragment = BeautifulSoup(
            """
            <div>
              <div class="toc-macro">TOC</div>
              <a href="/display/SPDB/Page">Page</a>
              <img src="/images/icons/comment_16.png" />
              <div class="table-wrap"><table><tr><td>○</td></tr></table></div>
              <p>Visible</p>
            </div>
            """,
            "html.parser",
        ).div
        rendered = self.connector._render_html_fragment(fragment, base_url="https://confluence.shopee.io")
        self.assertIn('href="https://confluence.shopee.io/display/SPDB/Page"', rendered)
        self.assertIn('target="_blank"', rendered)
        self.assertNotIn("toc-macro", rendered)
        self.assertNotIn("comment_16", rendered)
        self.assertNotIn("<table", rendered)

        plain_empty = BeautifulSoup("<div><table><tr><td>○</td></tr></table></div>", "html.parser").div
        plain_rendered = self.connector._render_html_fragment(plain_empty, base_url="https://confluence.shopee.io")
        self.assertNotIn("<table", plain_rendered)

        soup_for_cleanup = BeautifulSoup("<div><p>1.</p><p>Keep this paragraph</p></div>", "html.parser")
        self.connector._drop_marker_only_blocks(soup_for_cleanup)
        self.assertNotIn("<p>1.</p>", str(soup_for_cleanup))
        self.assertIn("Keep this paragraph", str(soup_for_cleanup))

    def test_parse_sections_fallback_and_toc_detection_edges(self):
        sections = self.connector._parse_sections(
            html='<h2><span></span></h2><div class="toc-macro">TOC</div>',
            base_url="https://confluence.shopee.io",
            source_url="https://confluence.shopee.io/display/SPDB/Test",
            session_id="session-1",
        )

        self.assertEqual(sections[0].title, "Overview")
        self.assertEqual(sections[0].content, "TOC")
        generated_toc = BeautifulSoup(
            "<h1>1. Project Management 1.1 Version Control 2. Introduction</h1>",
            "html.parser",
        ).h1
        self.assertTrue(self.connector._is_toc_block(generated_toc))

    def test_parse_sections_and_recursion_ignore_unexpected_non_tag_children(self):
        class FakeWrapper:
            children = [object()]
            contents = []

            def find_all(self, *args, **kwargs):
                return []

            def get_text(self, *args, **kwargs):
                return ""

        class FakeSoup:
            body = FakeWrapper()

        with patch("prd_briefing.confluence.BeautifulSoup", return_value=FakeSoup()):
            sections = self.connector._parse_sections(
                html="<ignored />",
                base_url="https://confluence.shopee.io",
                source_url="https://confluence.shopee.io/display/SPDB/Test",
                session_id="session-1",
            )
        self.assertEqual(sections[0].title, "Overview")

        generic = BeautifulSoup("<div><span>Shown</span></div>", "html.parser").div
        generic.contents.insert(0, object())
        with patch.object(self.connector, "_is_toc_block", return_value=False), \
            patch.object(self.connector, "_is_struck_node", return_value=False):
            self.assertEqual(
                self.connector._extract_block_content(generic, base_url="https://confluence.shopee.io")[0],
                ["Shown"],
            )

    def test_cell_text_and_struck_node_helpers(self):
        self.assertFalse(self.connector._is_meaningful_cell_text(""))
        self.assertFalse(self.connector._is_meaningful_cell_text("\u2000"))
        self.assertFalse(self.connector._is_meaningful_cell_text("1."))
        self.assertFalse(self.connector._is_meaningful_cell_text("A)"))
        self.assertFalse(self.connector._is_meaningful_cell_text("IV."))
        self.assertFalse(self.connector._is_meaningful_cell_text("○"))
        self.assertTrue(self.connector._is_meaningful_cell_text("需要复核"))
        self.assertFalse(self.connector._is_struck_node("not a tag"))

    @patch("prd_briefing.confluence.requests.get")
    def test_request_retries_auth_candidates_and_surfaces_errors(self, mock_get):
        unauthorized = Mock(status_code=401)
        ok = Mock(status_code=200)
        ok.raise_for_status.return_value = None
        mock_get.side_effect = [unauthorized, ok]

        response = self.connector._request("https://confluence.shopee.io/rest/api/content")

        self.assertIs(response, ok)
        self.assertEqual(mock_get.call_count, 2)

        unauthorized_again = Mock(status_code=401)
        unauthorized_again.raise_for_status.side_effect = requests.HTTPError("unauthorized")
        mock_get.side_effect = [unauthorized_again, unauthorized_again, unauthorized_again]
        with self.assertRaises(requests.HTTPError):
            self.connector._request("https://confluence.shopee.io/rest/api/content")

        mock_get.side_effect = requests.ConnectionError("down")
        with self.assertRaises(requests.ConnectionError):
            self.connector._request("https://confluence.shopee.io/rest/api/content")

        with patch.object(self.connector, "_headers_candidates", return_value=[]):
            with self.assertRaisesRegex(RuntimeError, "did not return a response"):
                self.connector._request("https://confluence.shopee.io/rest/api/content")

    def test_header_candidates_support_bearer_basic_and_unauthenticated_modes(self):
        bearer_connector = ConfluenceConnector(
            base_url="https://confluence.shopee.io",
            email="user@example.com",
            api_token="api-token",
            bearer_token="bearer-token",
            store=self.store,
        )
        headers = bearer_connector._headers_candidates(accept="text/html")
        self.assertEqual(headers[0]["Authorization"], "Bearer bearer-token")
        self.assertEqual(headers[0]["Accept"], "text/html")
        self.assertEqual(headers[1]["Authorization"], "Bearer api-token")
        self.assertTrue(headers[2]["Authorization"].startswith("Basic "))

        anonymous = ConfluenceConnector(
            base_url="https://confluence.shopee.io",
            email=None,
            api_token=None,
            bearer_token=None,
            store=self.store,
        )
        self.assertEqual(anonymous._headers_candidates(), [{"Accept": "application/json"}])

    def test_build_source_text_with_media_limits_and_skips_existing_refs(self):
        section = ParsedSection(
            title="T",
            section_path="Path",
            content="Body [MEDIA_ID_1]",
            image_refs=[f"image-{index}" for index in range(8)],
            media_refs=[f"MEDIA_ID_{index}" for index in range(1, 30)],
        )

        text = self.connector._build_source_text_with_media([section])

        self.assertIn("[IMAGE] image-5", text)
        self.assertNotIn("[IMAGE] image-6", text)
        self.assertNotIn("[MEDIA_ID_1]\n[MEDIA_ID_1]", text)
        self.assertIn("[MEDIA_ID_24]", text)
        self.assertNotIn("[MEDIA_ID_25]", text)


if __name__ == "__main__":
    unittest.main()
