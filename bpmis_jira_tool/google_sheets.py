from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.models import FieldMapping, InputRow


def _normalize_header(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


def _column_letter(index: int) -> str:
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


@dataclass
class SheetSnapshot:
    field_mappings: list[FieldMapping]
    rows: list[InputRow]
    headers: list[str]

    @property
    def eligible_rows(self) -> list[InputRow]:
        return [
            row
            for row in self.rows
            if row.issue_id and not row.jira_ticket_link
        ]


class GoogleSheetsService:
    def __init__(
        self,
        credentials,
        spreadsheet_id: str,
        common_tab: str,
        input_tab: str,
        issue_id_header: str = "Issue ID",
        jira_ticket_link_header: str = "Jira Ticket Link",
    ):
        self.spreadsheet_id = spreadsheet_id
        self.common_tab = common_tab
        self.input_tab = input_tab
        self.issue_id_header = issue_id_header
        self.jira_ticket_link_header = jira_ticket_link_header
        self.service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    @classmethod
    def create_template_spreadsheet(
        cls,
        credentials,
        *,
        spreadsheet_title: str,
        input_tab: str,
        headers: list[str],
    ) -> dict[str, str]:
        service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        try:
            response = (
                service.spreadsheets()
                .create(
                    body={
                        "properties": {"title": spreadsheet_title},
                        "sheets": [
                            {
                                "properties": {"title": input_tab},
                                "data": [
                                    {
                                        "rowData": [
                                            {
                                                "values": [
                                                    {
                                                        "userEnteredValue": {"stringValue": header},
                                                        "userEnteredFormat": {
                                                            "textFormat": {
                                                                "bold": True,
                                                            }
                                                        },
                                                    }
                                                    for header in headers
                                                ]
                                            }
                                        ]
                                    }
                                ],
                            }
                        ],
                    }
                )
                .execute()
            )
        except HttpError as error:
            message = getattr(error, "reason", "") or getattr(getattr(error, "resp", None), "reason", "") or ""
            raise ToolError(f"Google Sheets request failed: {message or error}") from error

        spreadsheet_id = str(response.get("spreadsheetId") or "").strip()
        spreadsheet_url = str(response.get("spreadsheetUrl") or "").strip()
        return {
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_url": spreadsheet_url,
            "input_tab_name": input_tab,
            "spreadsheet_title": spreadsheet_title,
        }

    def read_snapshot(self) -> SheetSnapshot:
        input_values = self._get_values(self.input_tab)
        rows, headers = self._parse_input_rows(
            input_values,
            issue_id_header=self.issue_id_header,
            jira_ticket_link_header=self.jira_ticket_link_header,
        )
        return SheetSnapshot(field_mappings=[], rows=rows, headers=headers)

    def update_success(self, row_number: int, headers: list[str], ticket_value: str) -> None:
        jira_ticket_link_header = getattr(self, "jira_ticket_link_header", "Jira Ticket Link")
        jira_ticket_link_index = self._find_header_index(headers, jira_ticket_link_header, "Jira Ticket Link")
        jira_ticket_link_col = _column_letter(jira_ticket_link_index + 1)
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": [
                {
                    "range": f"{self.input_tab}!{jira_ticket_link_col}{row_number}",
                    "values": [[ticket_value]],
                },
            ],
        }
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body,
        ).execute()
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={
                "requests": [
                    self._build_link_display_request(
                        sheet_id=self._get_sheet_id(self.input_tab),
                        start_row_index=row_number - 1,
                        end_row_index=row_number,
                        column_index=jira_ticket_link_index,
                        clip_text=True,
                        font_size=10,
                    )
                ]
            },
        ).execute()

    def append_records(self, headers: list[str], records: list[dict[str, str]]) -> None:
        if not records:
            return
        existing_values = self._get_values(self.input_tab)
        values = [[record.get(header, "") for header in headers] for record in records]
        self.service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=self.input_tab,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
        start_row_index = len(existing_values)
        end_row_index = start_row_index + len(records)
        sheet_id = self._get_sheet_id(self.input_tab)
        format_requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row_index,
                        "endRowIndex": end_row_index,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(headers),
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {
                                "bold": False,
                            }
                        }
                    },
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            }
        ]
        brd_link_index = self._find_header_index_if_present(headers, "BRD Link")
        if brd_link_index is not None:
            format_requests.append(
                self._build_link_display_request(
                    sheet_id=sheet_id,
                    start_row_index=start_row_index,
                    end_row_index=end_row_index,
                    column_index=brd_link_index,
                    clip_text=True,
                    font_size=10,
                )
            )
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body={
                "requests": format_requests
            },
        ).execute()

    def _get_values(self, range_name: str) -> list[list[str]]:
        try:
            response = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=range_name)
                .execute()
            )
            return response.get("values", [])
        except HttpError as error:
            message = ""
            content = getattr(error, "content", b"")
            if hasattr(error, "reason") and error.reason:
                message = str(error.reason)
            elif getattr(error, "resp", None) is not None:
                message = getattr(error.resp, "reason", "") or ""
            if content:
                try:
                    content_text = content.decode("utf-8", errors="ignore")
                except Exception:
                    content_text = str(content)
                if "Unable to parse range" in content_text:
                    message = content_text
            if "Unable to parse range" in message:
                raise ToolError(
                    f'Could not find sheet tab "{range_name}". Please update "Input Tab Name" in the web config.'
                ) from error
            raise ToolError(f"Google Sheets request failed: {message or error}") from error

    def _get_sheet_id(self, range_name: str) -> int:
        response = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
        sheets = response.get("sheets", [])
        for sheet in sheets:
            properties = sheet.get("properties", {})
            if str(properties.get("title", "")).strip() == range_name:
                return int(properties["sheetId"])
        raise ToolError(f'Could not find sheet tab "{range_name}". Please update "Input Tab Name" in the web config.')

    @staticmethod
    def _find_header_index(headers: list[str], *candidates: str) -> int:
        normalized_headers = {_normalize_header(header): index for index, header in enumerate(headers)}
        for candidate in candidates:
            index = normalized_headers.get(_normalize_header(candidate))
            if index is not None:
                return index
        raise ValueError(f"Could not find any of the expected headers: {', '.join(candidates)}")

    @staticmethod
    def _find_header_index_if_present(headers: list[str], *candidates: str) -> int | None:
        normalized_headers = {_normalize_header(header): index for index, header in enumerate(headers)}
        for candidate in candidates:
            index = normalized_headers.get(_normalize_header(candidate))
            if index is not None:
                return index
        return None

    @staticmethod
    def _build_link_display_request(
        *,
        sheet_id: int,
        start_row_index: int,
        end_row_index: int,
        column_index: int,
        clip_text: bool,
        font_size: int,
    ) -> dict[str, object]:
        return {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row_index,
                    "endRowIndex": end_row_index,
                    "startColumnIndex": column_index,
                    "endColumnIndex": column_index + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "CLIP" if clip_text else "OVERFLOW_CELL",
                        "textFormat": {
                            "bold": False,
                            "underline": True,
                            "fontSize": font_size,
                            "foregroundColorStyle": {
                                "rgbColor": {
                                    "red": 0.067,
                                    "green": 0.333,
                                    "blue": 0.8,
                                }
                            },
                        },
                    }
                },
                "fields": (
                    "userEnteredFormat.wrapStrategy,"
                    "userEnteredFormat.textFormat.bold,"
                    "userEnteredFormat.textFormat.underline,"
                    "userEnteredFormat.textFormat.fontSize,"
                    "userEnteredFormat.textFormat.foregroundColorStyle"
                ),
            }
        }

    @staticmethod
    def _parse_field_mappings(values: Iterable[Iterable[str]]) -> list[FieldMapping]:
        mappings: list[FieldMapping] = []
        for row in values:
            cells = list(row)
            if not cells:
                continue
            jira_field = cells[0].strip()
            source = cells[1].strip() if len(cells) > 1 else ""
            if not jira_field or _normalize_header(jira_field) in {"fieldname", "jirafield"}:
                continue
            mappings.append(FieldMapping(jira_field=jira_field, source=source))
        return mappings

    @staticmethod
    def _parse_input_rows(
        values: list[list[str]],
        issue_id_header: str = "Issue ID",
        jira_ticket_link_header: str = "Jira Ticket Link",
    ) -> tuple[list[InputRow], list[str]]:
        if not values:
            return [], []

        headers = [header.strip() for header in values[0]]
        rows: list[InputRow] = []

        for offset, raw_row in enumerate(values[1:], start=2):
            padded_row = list(raw_row) + [""] * max(0, len(headers) - len(raw_row))
            row_dict = {header: padded_row[index].strip() for index, header in enumerate(headers)}
            if issue_id_header and issue_id_header in row_dict:
                row_dict.setdefault("Issue ID", row_dict[issue_id_header])
            if jira_ticket_link_header and jira_ticket_link_header in row_dict:
                row_dict.setdefault("Jira Ticket Link", row_dict[jira_ticket_link_header])
            rows.append(InputRow(row_number=offset, values=row_dict, ordered_values=tuple(padded_row)))

        return rows, headers
