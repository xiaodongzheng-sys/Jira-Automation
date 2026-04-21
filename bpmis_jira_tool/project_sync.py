from __future__ import annotations

from bpmis_jira_tool.bpmis import BPMISClient
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.google_sheets import GoogleSheetsService
from bpmis_jira_tool.models import RunResult


class BPMISProjectSyncService:
    def __init__(
        self,
        sheets_service: GoogleSheetsService,
        bpmis_client: BPMISClient,
    ):
        self.sheets_service = sheets_service
        self.bpmis_client = bpmis_client

    def sync_projects(
        self,
        *,
        pm_email: str,
        issue_id_header: str,
        project_name_header: str,
        market_header: str,
        brd_link_header: str = "",
        progress_callback=None,
    ) -> list[RunResult]:
        if not pm_email.strip():
            raise ToolError("PM email is required before syncing BPMIS projects.")

        self._emit_progress(progress_callback, "loading", "Reading your Spreadsheet.", 0, 0)
        snapshot = self.sheets_service.read_snapshot()
        self._require_header(snapshot.headers, issue_id_header, "Issue ID sync target")
        self._require_header(snapshot.headers, project_name_header, "Project Name sync target")
        self._require_header(snapshot.headers, market_header, "Market sync target")
        if brd_link_header.strip():
            self._require_header(snapshot.headers, brd_link_header, "BRD Link sync target")

        existing_issue_ids = {
            row.values.get(issue_id_header, "").strip()
            for row in snapshot.rows
            if row.values.get(issue_id_header, "").strip()
        }

        self._emit_progress(progress_callback, "fetching", "Fetching BPMIS Biz Projects.", 0, 0)
        projects = self.bpmis_client.list_biz_projects_for_pm_email(pm_email)
        total = len(projects)
        results: list[RunResult] = []
        records_to_append: list[dict[str, str]] = []
        appended_issue_ids: set[str] = set()
        brd_links_by_issue_id: dict[str, list[str]] = {}

        if brd_link_header.strip():
            candidate_issue_ids = []
            for project in projects:
                issue_id = project["issue_id"].strip()
                if issue_id and issue_id not in existing_issue_ids:
                    candidate_issue_ids.append(issue_id)
            if candidate_issue_ids:
                self._emit_progress(
                    progress_callback,
                    "fetching",
                    "Fetching BRD links for synced BPMIS projects.",
                    0,
                    len(candidate_issue_ids),
                )
                brd_links_by_issue_id = self.bpmis_client.get_brd_doc_links_for_projects(candidate_issue_ids)

        next_row_number = len(snapshot.rows) + 2
        for index, project in enumerate(projects, start=1):
            issue_id = project["issue_id"].strip()
            project_name = project["project_name"].strip()
            market = project["market"].strip()
            brd_links: list[str] = []
            self._emit_progress(
                progress_callback,
                "syncing",
                f"Checking BPMIS Issue ID {issue_id}.",
                index,
                total,
            )

            if not issue_id:
                results.append(
                    RunResult(
                        row_number=0,
                        issue_id="",
                        status="error",
                        message="BPMIS returned a row without Issue ID.",
                    )
                )
                continue

            if issue_id in existing_issue_ids or issue_id in appended_issue_ids:
                results.append(
                    RunResult(
                        row_number=0,
                        issue_id=issue_id,
                        status="skipped",
                        message="Skipped because this BPMIS Issue ID already exists in the sheet.",
                        project_label=project_name or issue_id,
                        matched_project_id=market or None,
                    )
                )
                continue

            if brd_link_header.strip():
                brd_links = [link.strip() for link in brd_links_by_issue_id.get(issue_id, []) if link.strip()]

            appended_issue_ids.add(issue_id)
            record = {
                issue_id_header: issue_id,
                project_name_header: project_name,
                market_header: market,
            }
            if brd_link_header.strip():
                record[brd_link_header] = "\n".join(brd_links)
            records_to_append.append(record)
            results.append(
                RunResult(
                    row_number=next_row_number,
                    issue_id=issue_id,
                    status="created",
                    message="Added a new row from BPMIS.",
                    project_label=project_name or issue_id,
                    matched_project_id=market or None,
                )
            )
            next_row_number += 1

        self._emit_progress(progress_callback, "writing", "Writing new rows to Google Sheets.", total, total)
        self.sheets_service.append_records(snapshot.headers, records_to_append)
        self._emit_progress(progress_callback, "completed", "BPMIS sync finished.", total, total)
        return results

    @staticmethod
    def _require_header(headers: list[str], header: str, label: str) -> None:
        if not header.strip():
            raise ToolError(f"{label} header is required in the saved web config.")
        try:
            GoogleSheetsService._find_header_index(headers, header)
        except ValueError as error:
            raise ToolError(f'Could not find header "{header}" in the Input tab.') from error

    @staticmethod
    def _emit_progress(progress_callback, stage: str, message: str, current: int, total: int) -> None:
        if progress_callback is not None:
            progress_callback(stage, message, current, total)
