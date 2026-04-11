from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _normalize_header(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


@dataclass(frozen=True)
class FieldMapping:
    jira_field: str
    source: str


@dataclass(frozen=True)
class InputRow:
    row_number: int
    values: dict[str, str]
    ordered_values: tuple[str, ...] = ()

    def _get_first(self, *headers: str) -> str:
        normalized_values = {_normalize_header(key): value for key, value in self.values.items()}
        for header in headers:
            match = normalized_values.get(_normalize_header(header))
            if match is not None:
                return match.strip()
        return ""

    def get_by_column_letter(self, column_letter: str) -> str:
        index = 0
        for character in column_letter.strip().upper():
            if not character.isalpha():
                return ""
            index = index * 26 + (ord(character) - 64)
        if index <= 0:
            return ""
        zero_based = index - 1
        if zero_based >= len(self.ordered_values):
            return ""
        return self.ordered_values[zero_based].strip()

    @property
    def issue_id(self) -> str:
        return self._get_first("Issue ID")

    @property
    def jira_created(self) -> str:
        return self._get_first("Jira Created", "Jira Created?")

    @property
    def jira_ticket_link(self) -> str:
        return self._get_first("Jira Ticket Link")


@dataclass(frozen=True)
class ProjectMatch:
    project_id: str
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CreatedTicket:
    ticket_key: str | None
    ticket_link: str | None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunResult:
    row_number: int
    issue_id: str
    status: str
    message: str
    project_label: str | None = None
    matched_project_id: str | None = None
    ticket_key: str | None = None
    ticket_link: str | None = None
