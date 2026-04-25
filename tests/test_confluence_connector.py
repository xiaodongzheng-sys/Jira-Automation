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

    @patch("prd_briefing.confluence.requests.get")
    def test_ingest_display_url_uses_rest_api_search(self, mock_get):
        payload = {
            "results": [
                {
                    "id": "12345",
                    "title": "8.1 Part 1 of PRD",
                    "version": {"when": "2026-04-15T12:00:00Z"},
                    "body": {"export_view": {"value": "<h1>Overview</h1><p>Hello PRD</p>"}},
                }
            ]
        }
        response = Mock()
        response.json.return_value = payload
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        page = self.connector.ingest_page("https://confluence.shopee.io/display/SPDB/8.1+Part+1+of+PRD", "session-1")

        self.assertEqual(page.page_id, "12345")
        self.assertEqual(page.title, "8.1 Part 1 of PRD")
        self.assertEqual(page.sections[0].content, "Hello PRD")
        request_url = mock_get.call_args.kwargs.get("url") or mock_get.call_args.args[0]
        self.assertTrue(request_url.endswith("/rest/api/content"))

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
