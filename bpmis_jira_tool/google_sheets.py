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
        sdlc_approval_status_header: str = "SDLC Approval Status",
        business_lead_header: str = "Business Lead",
    ):
        self.spreadsheet_id = spreadsheet_id
        self.common_tab = common_tab
        self.input_tab = input_tab
        self.issue_id_header = issue_id_header
        self.jira_ticket_link_header = jira_ticket_link_header
        self.sdlc_approval_status_header = sdlc_approval_status_header
        self.business_lead_header = business_lead_header
        self.service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    def read_snapshot(self) -> SheetSnapshot:
        input_values = self._get_values(self.input_tab)
        rows, headers = self._parse_input_rows(
            input_values,
            issue_id_header=self.issue_id_header,
            jira_ticket_link_header=self.jira_ticket_link_header,
            sdlc_approval_status_header=self.sdlc_approval_status_header,
            business_lead_header=self.business_lead_header,
        )
        return SheetSnapshot(field_mappings=[], rows=rows, headers=headers)

    def update_success(self, row_number: int, headers: list[str], ticket_value: str) -> None:
        jira_ticket_link_col = _column_letter(
            self._find_header_index(headers, self.jira_ticket_link_header, "Jira Ticket Link") + 1
        )
        self._batch_update_cells(
            [
                {
                    "range": f"{self.input_tab}!{jira_ticket_link_col}{row_number}",
                    "values": [[ticket_value]],
                },
            ]
        )

    def update_sdlc_status(self, row_number: int, headers: list[str], status_value: str) -> None:
        sdlc_status_col = _column_letter(
            self._find_header_index(
                headers,
                self.sdlc_approval_status_header,
                "SDLC Approval Status",
            )
            + 1
        )
        self._batch_update_cells(
            [
                {
                    "range": f"{self.input_tab}!{sdlc_status_col}{row_number}",
                    "values": [[status_value]],
                },
            ]
        )

    def _batch_update_cells(self, data: list[dict[str, object]]) -> None:
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": data,
        }
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body,
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

    @staticmethod
    def _find_header_index(headers: list[str], *candidates: str) -> int:
        normalized_headers = {_normalize_header(header): index for index, header in enumerate(headers)}
        for candidate in candidates:
            index = normalized_headers.get(_normalize_header(candidate))
            if index is not None:
                return index
        raise ValueError(f"Could not find any of the expected headers: {', '.join(candidates)}")

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
        sdlc_approval_status_header: str = "SDLC Approval Status",
        business_lead_header: str = "Business Lead",
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
            if sdlc_approval_status_header and sdlc_approval_status_header in row_dict:
                row_dict.setdefault("SDLC Approval Status", row_dict[sdlc_approval_status_header])
            if business_lead_header and business_lead_header in row_dict:
                row_dict.setdefault("Business Lead", row_dict[business_lead_header])
            rows.append(InputRow(row_number=offset, values=row_dict, ordered_values=tuple(padded_row)))

        return rows, headers
