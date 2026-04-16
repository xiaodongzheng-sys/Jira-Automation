from __future__ import annotations

from bpmis_jira_tool.bpmis import (
    BPMISClient,
    BPMISHelperClient,
    BPMISPageApiClient,
)
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError, FieldResolutionError
from bpmis_jira_tool.field_resolver import resolve_fields
from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.models import FieldMapping
from bpmis_jira_tool.models import RunResult


def build_bpmis_client(settings: Settings, access_token: str | None = None) -> BPMISClient:
    return BPMISPageApiClient(settings)


class JiraCreationService:
    def __init__(
        self,
        settings: Settings,
        sheets_service: GoogleSheetsService,
        access_token: str | None = None,
        field_mappings_override: list[FieldMapping] | None = None,
        bpmis_client: BPMISClient | None = None,
    ):
        self.settings = settings
        self.sheets_service = sheets_service
        self.bpmis_client = bpmis_client or build_bpmis_client(settings, access_token)
        self.field_mappings_override = field_mappings_override

    def preview(self, progress_callback=None) -> tuple[list[RunResult], list[str]]:
        self._emit_progress(progress_callback, "loading", "Reading your Spreadsheet.", 0, 0)
        snapshot = self.sheets_service.read_snapshot()
        if self.field_mappings_override:
            snapshot.field_mappings = self.field_mappings_override
        total = len(snapshot.rows)
        results: list[RunResult] = []
        for index, row in enumerate(snapshot.rows, start=1):
            self._emit_progress(
                progress_callback,
                "previewing",
                f"Checking row {row.row_number} for Jira eligibility.",
                index,
                total,
            )
            results.append(self._preview_row(snapshot.field_mappings, row))
        self._emit_progress(progress_callback, "completed", "Preview finished.", total, total)
        return results, snapshot.headers

    def run(self, dry_run: bool = False, progress_callback=None) -> list[RunResult]:
        self._emit_progress(progress_callback, "loading", "Reading your Spreadsheet.", 0, 0)
        snapshot = self.sheets_service.read_snapshot()
        if self.field_mappings_override:
            snapshot.field_mappings = self.field_mappings_override
        results: list[RunResult] = []
        total = len(snapshot.rows)

        for index, row in enumerate(snapshot.rows, start=1):
            self._emit_progress(
                progress_callback,
                "processing",
                f"Processing row {row.row_number}.",
                index,
                total,
            )
            if not row.issue_id:
                results.append(
                    RunResult(
                        row_number=row.row_number,
                        issue_id="",
                        status="skipped",
                        message="Skipped because Issue ID is missing.",
                    )
                )
                continue

            if row.jira_ticket_link:
                results.append(
                    RunResult(
                        row_number=row.row_number,
                        issue_id=row.issue_id,
                        status="skipped",
                        message="Skipped because Jira Ticket Link already has a value.",
                        ticket_link=row.jira_ticket_link,
                    )
                )
                continue

            try:
                project = self.bpmis_client.find_project(row.issue_id) if self.bpmis_client else None
                self._emit_progress(
                    progress_callback,
                    "mapping",
                    f"Resolving Jira fields for row {row.row_number}.",
                    index,
                    total,
                )
                fields = resolve_fields(snapshot.field_mappings, row)
                project_label = fields.get("Summary") or row.issue_id
                if dry_run:
                    results.append(
                        RunResult(
                            row_number=row.row_number,
                            issue_id=row.issue_id,
                            status="preview",
                            message="Ready to create Jira ticket.",
                            project_label=project_label,
                            matched_project_id=project.project_id if project else None,
                        )
                    )
                    continue

                self._emit_progress(
                    progress_callback,
                    "creating",
                    f"Creating Jira ticket for row {row.row_number}.",
                    index,
                    total,
                )
                ticket = self.bpmis_client.create_jira_ticket(project, fields)
                stored_value = ticket.ticket_link or ticket.ticket_key or ""
                self._emit_progress(
                    progress_callback,
                    "writing",
                    f"Writing Jira link back to row {row.row_number}.",
                    index,
                    total,
                )
                self.sheets_service.update_success(row.row_number, snapshot.headers, stored_value)
                results.append(
                    RunResult(
                        row_number=row.row_number,
                        issue_id=row.issue_id,
                        status="created",
                        message="Created Jira ticket successfully.",
                        project_label=project_label,
                        matched_project_id=project.project_id,
                        ticket_key=ticket.ticket_key,
                        ticket_link=ticket.ticket_link or ticket.ticket_key,
                    )
                )
            except (FieldResolutionError, BPMISError) as error:
                results.append(
                    RunResult(
                        row_number=row.row_number,
                        issue_id=row.issue_id,
                        status="error",
                        message=str(error),
                    )
                )

        self._emit_progress(progress_callback, "completed", "Run finished.", total, total)
        return results

    def _preview_row(self, field_mappings, row) -> RunResult:
        if not row.issue_id:
            return RunResult(
                row_number=row.row_number,
                issue_id="",
                status="skipped",
                message="Issue ID is missing.",
            )
        if row.jira_ticket_link:
            return RunResult(
                row_number=row.row_number,
                issue_id=row.issue_id,
                status="skipped",
                message="Jira Ticket Link already has a value.",
                ticket_link=row.jira_ticket_link,
            )

        try:
            fields = resolve_fields(field_mappings, row)
        except FieldResolutionError as error:
            return RunResult(
                row_number=row.row_number,
                issue_id=row.issue_id,
                status="error",
                message=str(error),
            )

        return RunResult(
            row_number=row.row_number,
            issue_id=row.issue_id,
            status="preview",
            message="Eligible for Jira creation.",
            project_label=fields.get("Summary") or row.issue_id,
        )

    @staticmethod
    def _emit_progress(progress_callback, stage: str, message: str, current: int, total: int) -> None:
        if progress_callback is not None:
            progress_callback(stage, message, current, total)
