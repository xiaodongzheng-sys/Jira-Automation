import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from prd_briefing.confluence import ConfluenceConnector
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


if __name__ == "__main__":
    unittest.main()
