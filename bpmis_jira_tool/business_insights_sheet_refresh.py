from __future__ import annotations

from datetime import datetime
import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import uuid
from zoneinfo import ZoneInfo

import google.auth
from google.auth.credentials import Credentials as GoogleAuthCredentials
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuthCredentials
from googleapiclient.discovery import build

from bpmis_jira_tool.business_insights import (
    AF_BLACK_WHITE_LIST_TABLE,
    AF_CARD_3DS_REPORT_ID,
    AF_DEVICE_RISK_REPORT_ID,
    AF_FACIAL_VERIFICATION_REPORT_ID,
    AF_FRAUD_LOSS_REPORT_ID,
    AF_LIST_USAGE_REPORT_ID,
    AF_RULE_EFFECTIVENESS_REPORT_ID,
    AF_RULES_FEATURES_REPORT_ID,
    AF_SCENARIOS_ACTIONS_REPORT_ID,
)
from bpmis_jira_tool.errors import ConfigError, ToolError
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials
from bpmis_jira_tool.google_auth import GOOGLE_SCOPES

DEFAULT_BUSINESS_INSIGHTS_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1F5MSUwnxg8AbGr3rQN1l8nXYkxrBU680FJYhTGzL9qo/edit?gid=2125394335#gid=2125394335"
)
GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"

ANTI_FRAUD_SHEET_REPORT_ORDER: tuple[tuple[int, str], ...] = (
    (1, AF_SCENARIOS_ACTIONS_REPORT_ID),
    (2, AF_RULES_FEATURES_REPORT_ID),
    (3, AF_RULE_EFFECTIVENESS_REPORT_ID),
    (4, AF_FRAUD_LOSS_REPORT_ID),
    (5, AF_FACIAL_VERIFICATION_REPORT_ID),
    (6, AF_DEVICE_RISK_REPORT_ID),
    (7, AF_CARD_3DS_REPORT_ID),
    (8, AF_LIST_USAGE_REPORT_ID),
)
_REPORT_PREFIX_BY_ID = {report_id: prefix for prefix, report_id in ANTI_FRAUD_SHEET_REPORT_ORDER}
ANTI_FRAUD_SHEET_REPORT_IDS = tuple(report_id for _prefix, report_id in ANTI_FRAUD_SHEET_REPORT_ORDER)


def spreadsheet_id_from_url(value: str) -> str:
    clean = str(value or "").strip()
    if not clean:
        raise ConfigError("Business Insights Google Sheet URL is missing.")
    match = re.search(r"/spreadsheets/d/([^/?#]+)", clean)
    if match:
        return match.group(1)
    parsed = urlparse(clean)
    query_id = parse_qs(parsed.query).get("id", [""])[0].strip()
    if query_id:
        return query_id
    if re.fullmatch(r"[-_A-Za-z0-9]{20,}", clean):
        return clean
    raise ConfigError("Business Insights Google Sheet URL does not contain a spreadsheet id.")


def scheduled_sheet_name(report_id: str, section_name: str) -> str:
    prefix = _REPORT_PREFIX_BY_ID.get(str(report_id))
    if prefix is None:
        raise ToolError(f"Unsupported Anti-fraud sheet-backed report id: {report_id}")
    slug = re.sub(r"[^a-z0-9]+", "_", str(section_name or "").strip().lower()).strip("_")
    if not slug:
        raise ToolError(f"Could not derive sheet name for report {report_id}.")
    return f"{prefix}_{slug}"


def _quoted_sheet_range(sheet_name: str) -> str:
    escaped = str(sheet_name).replace("'", "''")
    return f"'{escaped}'!A:ZZ"


def _normalise_values(values: list[list[Any]]) -> tuple[list[str], list[list[Any]]]:
    if not values:
        return [], []
    headers = [str(value or "").strip() for value in values[0]]
    rows = [list(row) for row in values[1:] if any(value not in (None, "") for value in row)]
    return headers, rows


def _ensure_sheets_scope(credentials_payload: dict[str, Any]) -> None:
    scopes = {str(scope).strip() for scope in (credentials_payload.get("scopes") or []) if str(scope).strip()}
    if GOOGLE_SHEETS_SCOPE not in scopes:
        raise ConfigError(
            "Google Sheets permission is missing. Reconnect Google once to grant spreadsheets access."
        )


def load_stored_google_sheets_credentials(
    *,
    portal_data_dir: Path,
    owner_email: str,
    encryption_key: str,
) -> OAuthCredentials:
    store = StoredGoogleCredentials(
        Path(portal_data_dir) / "google" / "credentials.json",
        encryption_key=encryption_key,
    )
    credentials_payload = store.load(owner_email=owner_email)
    _ensure_sheets_scope(credentials_payload)
    return OAuthCredentials(**credentials_payload)


def load_oauth_google_sheets_credentials(credentials_json: str) -> OAuthCredentials:
    try:
        credentials_payload = json.loads(credentials_json)
    except json.JSONDecodeError as error:
        raise ConfigError("BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON is not valid JSON.") from error
    if not isinstance(credentials_payload, dict):
        raise ConfigError("BUSINESS_INSIGHTS_GOOGLE_OAUTH_CREDENTIALS_JSON must be a JSON object.")
    _ensure_sheets_scope(credentials_payload)
    return OAuthCredentials(**credentials_payload)


def load_service_account_google_sheets_credentials(
    *,
    service_account_json: str = "",
    service_account_file: str = "",
) -> GoogleAuthCredentials:
    if service_account_json.strip():
        try:
            payload = json.loads(service_account_json)
        except json.JSONDecodeError as error:
            raise ConfigError("BUSINESS_INSIGHTS_GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from error
        return service_account.Credentials.from_service_account_info(
            payload,
            scopes=[GOOGLE_SHEETS_SCOPE],
        )
    clean_file = str(service_account_file or "").strip()
    if not clean_file:
        raise ConfigError("Google service account JSON or file path is required.")
    return service_account.Credentials.from_service_account_file(
        clean_file,
        scopes=[GOOGLE_SHEETS_SCOPE],
    )


def load_application_default_google_sheets_credentials() -> GoogleAuthCredentials:
    credentials, _project_id = google.auth.default(scopes=[GOOGLE_SHEETS_SCOPE])
    return credentials


def build_sheets_service(credentials: GoogleAuthCredentials) -> Any:
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _fetch_sheet_values(
    sheets_service: Any,
    *,
    spreadsheet_id: str,
    sheet_names: list[str],
) -> dict[str, tuple[list[str], list[list[Any]]]]:
    ranges = [_quoted_sheet_range(sheet_name) for sheet_name in sheet_names]
    response = (
        sheets_service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges, majorDimension="ROWS")
        .execute()
    )
    value_ranges = response.get("valueRanges") if isinstance(response, dict) else []
    result: dict[str, tuple[list[str], list[list[Any]]]] = {}
    for sheet_name, value_range in zip(sheet_names, value_ranges or [], strict=False):
        headers, rows = _normalise_values(value_range.get("values") or [])
        result[sheet_name] = (headers, rows)
    missing = [name for name in sheet_names if name not in result]
    if missing:
        raise ToolError(f"Google Sheet tabs were not returned: {', '.join(missing[:8])}")
    empty = [name for name, (headers, _rows) in result.items() if not headers]
    if empty:
        raise ToolError(f"Google Sheet tabs have no header row: {', '.join(empty[:8])}")
    return result


def refresh_anti_fraud_reports_from_google_sheet(
    *,
    portal_data_dir: Path,
    sheets_service: Any,
    sheet_url: str = DEFAULT_BUSINESS_INSIGHTS_SHEET_URL,
    report_ids: list[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    from scripts.generate_business_insights_live_reports import (
        REPORT_BUILDERS,
        extract_sql_sections,
        normalize_product_labels,
        update_report_artifact,
        write_visualization,
        write_workbook,
    )

    active_now = now or datetime.now(ZoneInfo("Asia/Singapore"))
    selected_report_ids = report_ids or list(ANTI_FRAUD_SHEET_REPORT_IDS)
    unsupported = [report_id for report_id in selected_report_ids if report_id not in _REPORT_PREFIX_BY_ID]
    if unsupported:
        raise ToolError(f"Unsupported sheet-backed report ids: {', '.join(unsupported)}")

    report_sections: dict[str, list[tuple[str, str]]] = {}
    sheet_names: list[str] = []
    sql_by_report: dict[str, str] = {}
    for report_id in selected_report_ids:
        config = REPORT_BUILDERS.get(report_id)
        if config is None:
            raise ToolError(f"No Business Insights builder configured for {report_id}.")
        _title, builder = config
        sql = builder(snapshot_pt_date=None, now=active_now)
        sql_by_report[report_id] = sql
        sections = []
        for section in extract_sql_sections(sql):
            google_tab = scheduled_sheet_name(report_id, section.sheet_name)
            sections.append((section.sheet_name, google_tab))
            sheet_names.append(google_tab)
        report_sections[report_id] = sections

    fetched = _fetch_sheet_values(
        sheets_service,
        spreadsheet_id=spreadsheet_id_from_url(sheet_url),
        sheet_names=sheet_names,
    )

    artifact_root = Path(portal_data_dir) / "business_insights" / "artifacts"
    artifact_root.mkdir(parents=True, exist_ok=True)
    refreshed: list[dict[str, Any]] = []
    for report_id in selected_report_ids:
        title, _builder = REPORT_BUILDERS[report_id]
        sheets = []
        source_tabs = []
        for display_name, google_tab in report_sections[report_id]:
            headers, rows = fetched[google_tab]
            sheets.append((display_name, headers, rows))
            source_tabs.append({"sheet": display_name, "google_tab": google_tab, "rows": len(rows)})

        artifact_id = uuid.uuid4().hex
        xlsx_filename = f"{report_id}-{artifact_id[:8]}.xlsx"
        html_filename = f"{report_id}-{artifact_id[:8]}.html"
        display_sheets = normalize_product_labels(sheets)
        write_workbook(artifact_root / xlsx_filename, display_sheets)
        write_visualization(
            artifact_root / html_filename,
            report_title=title,
            snapshot_pt_date="Google Sheet latest scheduled output",
            sheets=sheets,
            report_id=report_id,
        )
        metadata = {
            "id": artifact_id,
            "report_id": report_id,
            "filename": xlsx_filename,
            "visualization_filename": html_filename,
            "source_filename": "google-sheet-scheduled-output",
            "source_google_sheet_url": sheet_url,
            "source_google_tabs": source_tabs,
            "row_count": sum(len(rows) for _sheet, _headers, rows in sheets),
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "snapshot_pt_date": "google_sheet_latest",
            "sql": sql_by_report[report_id],
            "workbench_executions": [],
        }
        update_report_artifact(Path(portal_data_dir), report_id=report_id, metadata=metadata)
        refreshed.append(
            {
                "report_id": report_id,
                "artifact_id": artifact_id,
                "filename": xlsx_filename,
                "visualization_filename": html_filename,
                "row_count": metadata["row_count"],
            }
        )

    return {
        "status": "ok",
        "sheet_url": sheet_url,
        "report_count": len(refreshed),
        "reports": refreshed,
    }


def google_scopes_include_sheets() -> bool:
    return GOOGLE_SHEETS_SCOPE in GOOGLE_SCOPES
