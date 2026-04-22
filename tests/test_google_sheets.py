import unittest
from unittest.mock import patch

from googleapiclient.errors import HttpError

from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.errors import ToolError


class GoogleSheetsParsingTests(unittest.TestCase):
    def test_create_template_spreadsheet_prefills_header_row(self):
        captured = {}

        class _Execute:
            def __init__(self, payload=None):
                self.payload = payload

            def execute(self):
                return self.payload or {}

        class _FakeSpreadsheets:
            def create(self, **kwargs):
                captured["create"] = kwargs
                return _Execute(
                    {
                        "spreadsheetId": "sheet-123",
                        "spreadsheetUrl": "https://docs.google.com/spreadsheets/d/sheet-123/edit",
                    }
                )

        class _FakeService:
            def spreadsheets(self):
                return _FakeSpreadsheets()

        with patch("bpmis_jira_tool.google_sheets.build", return_value=_FakeService()):
            created = GoogleSheetsService.create_template_spreadsheet(
                credentials=object(),
                spreadsheet_title="BPMIS Automation Tool",
                input_tab="Sheet1",
                headers=["A", "B"],
            )

        self.assertEqual(created["spreadsheet_id"], "sheet-123")
        self.assertEqual(created["spreadsheet_url"], "https://docs.google.com/spreadsheets/d/sheet-123/edit")
        row_data = captured["create"]["body"]["sheets"][0]["data"][0]["rowData"][0]["values"]
        self.assertEqual(
            [cell["userEnteredValue"]["stringValue"] for cell in row_data],
            ["A", "B"],
        )
        self.assertTrue(all(cell["userEnteredFormat"]["textFormat"]["bold"] for cell in row_data))

    def test_parses_field_mappings(self):
        values = [
            ["Field Name", "How to fill"],
            ["Summary", "column:Title"],
            ["Issue Type", "literal:Task"],
        ]

        mappings = GoogleSheetsService._parse_field_mappings(values)

        self.assertEqual(len(mappings), 2)
        self.assertEqual(mappings[0].jira_field, "Summary")
        self.assertEqual(mappings[1].source, "literal:Task")

    def test_parses_rows_and_tracks_sheet_row_number(self):
        values = [
            ["Issue ID", "Jira Ticket Link"],
            ["ISS-1", ""],
            ["ISS-2", "JIRA-10"],
        ]

        rows, headers = GoogleSheetsService._parse_input_rows(values)

        self.assertEqual(headers, ["Issue ID", "Jira Ticket Link"])
        self.assertEqual(rows[0].row_number, 2)
        self.assertEqual(rows[1].jira_ticket_link, "JIRA-10")

    def test_parses_rows_with_configured_issue_and_ticket_headers(self):
        values = [
            ["Req ID", "Ticket URL"],
            ["ISS-9", ""],
        ]

        rows, headers = GoogleSheetsService._parse_input_rows(
            values,
            issue_id_header="Req ID",
            jira_ticket_link_header="Ticket URL",
        )

        self.assertEqual(headers, ["Req ID", "Ticket URL"])
        self.assertEqual(rows[0].issue_id, "ISS-9")
        self.assertEqual(rows[0].jira_ticket_link, "")

    def test_get_values_raises_friendly_error_when_tab_name_is_invalid(self):
        class _FakeExecute:
            def execute(self):
                raise HttpError(resp=type("Resp", (), {"status": 400, "reason": "Bad Request"})(), content=b'{"error":{"message":"Unable to parse range: Renamed Input"}}')

        class _FakeValues:
            def get(self, **_kwargs):
                return _FakeExecute()

        class _FakeSpreadsheets:
            def values(self):
                return _FakeValues()

        class _FakeService:
            def spreadsheets(self):
                return _FakeSpreadsheets()

        sheets = GoogleSheetsService.__new__(GoogleSheetsService)
        sheets.service = _FakeService()
        sheets.spreadsheet_id = "sheet"

        with self.assertRaises(ToolError) as context:
            sheets._get_values("Renamed Input")

        self.assertIn('Could not find sheet tab "Renamed Input"', str(context.exception))

    def test_append_records_explicitly_turns_off_bold_for_new_rows(self):
        captured = {}

        class _Execute:
            def __init__(self, payload=None):
                self.payload = payload

            def execute(self):
                return self.payload or {}

        class _FakeValues:
            def get(self, **_kwargs):
                return _Execute({"values": [["Issue ID", "Project Name"], ["ISS-1", "Existing"]]})

            def append(self, **kwargs):
                captured["append"] = kwargs
                return _Execute({})

        class _FakeSpreadsheets:
            def values(self):
                return _FakeValues()

            def get(self, **_kwargs):
                return _Execute({"sheets": [{"properties": {"title": "Input", "sheetId": 7}}]})

            def batchUpdate(self, **kwargs):
                captured["batch_update"] = kwargs
                return _Execute({})

        class _FakeService:
            def spreadsheets(self):
                return _FakeSpreadsheets()

        sheets = GoogleSheetsService.__new__(GoogleSheetsService)
        sheets.service = _FakeService()
        sheets.spreadsheet_id = "sheet"
        sheets.input_tab = "Input"

        sheets.append_records(
            ["Issue ID", "Project Name", "BRD Link", "Jira Ticket Link"],
            [{"Issue ID": "ISS-2", "Project Name": "New Project"}],
        )

        requests = captured["batch_update"]["body"]["requests"]
        repeat_cell = requests[0]["repeatCell"]
        self.assertEqual(repeat_cell["range"]["sheetId"], 7)
        self.assertEqual(repeat_cell["range"]["startRowIndex"], 2)
        self.assertEqual(repeat_cell["range"]["endRowIndex"], 3)
        self.assertEqual(repeat_cell["cell"]["userEnteredFormat"]["textFormat"]["bold"], False)
        self.assertEqual(repeat_cell["fields"], "userEnteredFormat.textFormat.bold")

        brd_format = requests[1]["repeatCell"]
        self.assertEqual(brd_format["range"]["startColumnIndex"], 2)
        self.assertEqual(brd_format["range"]["endColumnIndex"], 3)
        self.assertEqual(brd_format["cell"]["userEnteredFormat"]["wrapStrategy"], "CLIP")
        self.assertEqual(brd_format["cell"]["userEnteredFormat"]["textFormat"]["underline"], True)
        self.assertEqual(brd_format["cell"]["userEnteredFormat"]["textFormat"]["fontSize"], 10)

        self.assertEqual(len(requests), 2)

    def test_update_success_formats_jira_ticket_link_cell_like_standard_hyperlink(self):
        captured = {}

        class _Execute:
            def __init__(self, payload=None):
                self.payload = payload

            def execute(self):
                return self.payload or {}

        class _FakeValues:
            def batchUpdate(self, **kwargs):
                captured["values_batch_update"] = kwargs
                return _Execute({})

        class _FakeSpreadsheets:
            def values(self):
                return _FakeValues()

            def get(self, **_kwargs):
                return _Execute({"sheets": [{"properties": {"title": "Input", "sheetId": 7}}]})

            def batchUpdate(self, **kwargs):
                captured["format_batch_update"] = kwargs
                return _Execute({})

        class _FakeService:
            def spreadsheets(self):
                return _FakeSpreadsheets()

        sheets = GoogleSheetsService.__new__(GoogleSheetsService)
        sheets.service = _FakeService()
        sheets.spreadsheet_id = "sheet"
        sheets.input_tab = "Input"
        sheets.jira_ticket_link_header = "Jira Ticket Link"

        sheets.update_success(
            row_number=12,
            headers=["Issue ID", "Jira Ticket Link"],
            ticket_value="https://jira.shopee.io/browse/SPDBP-1",
        )

        self.assertEqual(
            captured["values_batch_update"]["body"]["data"][0]["range"],
            "Input!B12",
        )
        repeat_cell = captured["format_batch_update"]["body"]["requests"][0]["repeatCell"]
        self.assertEqual(repeat_cell["range"]["startRowIndex"], 11)
        self.assertEqual(repeat_cell["range"]["endRowIndex"], 12)
        self.assertEqual(repeat_cell["range"]["startColumnIndex"], 1)
        self.assertEqual(repeat_cell["range"]["endColumnIndex"], 2)
        self.assertEqual(repeat_cell["cell"]["userEnteredFormat"]["wrapStrategy"], "CLIP")
        self.assertEqual(repeat_cell["cell"]["userEnteredFormat"]["textFormat"]["underline"], True)
        self.assertEqual(repeat_cell["cell"]["userEnteredFormat"]["textFormat"]["fontSize"], 12)


if __name__ == "__main__":
    unittest.main()
