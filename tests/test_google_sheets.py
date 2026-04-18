import unittest

from googleapiclient.errors import HttpError

from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.errors import ToolError


class GoogleSheetsParsingTests(unittest.TestCase):
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
            ["Issue ID", "Project Name"],
            [{"Issue ID": "ISS-2", "Project Name": "New Project"}],
        )

        repeat_cell = captured["batch_update"]["body"]["requests"][0]["repeatCell"]
        self.assertEqual(repeat_cell["range"]["sheetId"], 7)
        self.assertEqual(repeat_cell["range"]["startRowIndex"], 2)
        self.assertEqual(repeat_cell["range"]["endRowIndex"], 3)
        self.assertEqual(repeat_cell["cell"]["userEnteredFormat"]["textFormat"]["bold"], False)
        self.assertEqual(repeat_cell["fields"], "userEnteredFormat.textFormat.bold")


if __name__ == "__main__":
    unittest.main()
