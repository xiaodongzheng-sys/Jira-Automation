from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import io
import json
import os
from pathlib import Path
import re
import threading
import time as time_module
from typing import Any
import uuid
from zoneinfo import ZoneInfo

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from bpmis_jira_tool.errors import ToolError

BUSINESS_INSIGHTS_TIMEZONE = ZoneInfo("Asia/Singapore")
BUSINESS_INSIGHTS_DOMAINS: tuple[dict[str, str], ...] = (
    {"key": "anti-fraud", "label": "Anti-fraud"},
    {"key": "credit-risk", "label": "Credit Risk"},
    {"key": "ops-risk", "label": "Ops Risk"},
)
UNDERWRITING_FUNNEL_REPORT_ID = "credit-risk-ph-underwriting-funnel"
UNDERWRITING_FUNNEL_TABLE = "ods.crms_ph_seabank_crms_db_underwriting_record_tab_ss_d"
UNDERWRITING_FUNNEL_FIELDS: tuple[str, ...] = (
    "underwriting_id",
    "underwriting_purpose",
    "product_code",
    "sub_product_code",
    "application_submission_time",
    "borrower_id",
    "apply_loan_amount",
    "apply_loan_tenor",
    "credit_score_partner",
    "underwriting_status",
    "current_stage",
    "step",
    "reject_reason",
    "create_date",
    "modify_date",
)

SEEDED_REPORTS: tuple[dict[str, str], ...] = (
    {
        "id": UNDERWRITING_FUNNEL_REPORT_ID,
        "domain": "credit-risk",
        "name": "Credit Risk PH - Underwriting Funnel",
        "type": "underwriting_funnel",
        "status": "generator_ready",
    },
    {
        "id": "credit-risk-ph-portfolio-repayment",
        "domain": "credit-risk",
        "name": "Portfolio Repayment",
        "type": "planned",
        "status": "planned",
    },
    {
        "id": "credit-risk-ph-limit-utilization",
        "domain": "credit-risk",
        "name": "Limit Utilization",
        "type": "planned",
        "status": "planned",
    },
)


@dataclass(frozen=True)
class BusinessInsightsPeriod:
    previous_month_start: date
    current_month_start: date
    end_exclusive: date

    @property
    def start_date(self) -> date:
        return self.previous_month_start

    @property
    def current_date(self) -> date:
        return self.end_exclusive - timedelta(days=1)

    @property
    def previous_month_label(self) -> str:
        return self.previous_month_start.strftime("%b %Y")

    @property
    def current_month_label(self) -> str:
        return f"{self.current_month_start.strftime('%b %Y')} MTD"

    @property
    def title(self) -> str:
        return f"{self.previous_month_label} + {self.current_month_label}"


def business_insights_period(now: datetime | None = None) -> BusinessInsightsPeriod:
    active_now = now.astimezone(BUSINESS_INSIGHTS_TIMEZONE) if now else datetime.now(BUSINESS_INSIGHTS_TIMEZONE)
    current_month_start = active_now.date().replace(day=1)
    previous_month_last_day = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_last_day.replace(day=1)
    return BusinessInsightsPeriod(
        previous_month_start=previous_month_start,
        current_month_start=current_month_start,
        end_exclusive=active_now.date() + timedelta(days=1),
    )


def _date_to_epoch_millis(value: date) -> int:
    instant = datetime.combine(value, time.min, tzinfo=BUSINESS_INSIGHTS_TIMEZONE)
    return int(instant.timestamp() * 1000)


def build_underwriting_funnel_sql(now: datetime | None = None) -> str:
    period = business_insights_period(now)
    start_ms = _date_to_epoch_millis(period.start_date)
    current_month_start_ms = _date_to_epoch_millis(period.current_month_start)
    end_ms = _date_to_epoch_millis(period.end_exclusive)
    columns = ",\n    ".join(UNDERWRITING_FUNNEL_FIELDS)
    return f"""-- Credit Risk PH - Underwriting Funnel
-- Duration: {period.title}
-- Run this SQL in Data Workbench, download the result, then upload it back to Business Insights.
select
    case
        when application_submission_time >= {current_month_start_ms} then '{period.current_month_label}'
        else '{period.previous_month_label}'
    end as report_period,
    {columns}
from {UNDERWRITING_FUNNEL_TABLE}
where application_submission_time >= {start_ms}
  and application_submission_time < {end_ms}
;"""


def build_underwriting_funnel_mis_sql(*, snapshot_pt_date: str | None = None, now: datetime | None = None) -> str:
    period = business_insights_period(now)
    start_ms = _date_to_epoch_millis(period.start_date)
    current_month_start_ms = _date_to_epoch_millis(period.current_month_start)
    end_ms = _date_to_epoch_millis(period.end_exclusive)
    active_now = now.astimezone(BUSINESS_INSIGHTS_TIMEZONE) if now else datetime.now(BUSINESS_INSIGHTS_TIMEZONE)
    active_now_ms = int(active_now.timestamp() * 1000)
    snapshot_filter = (
        f"pt_date = '{snapshot_pt_date}'"
        if snapshot_pt_date
        else f"pt_date = (select max(pt_date) from {UNDERWRITING_FUNNEL_TABLE})"
    )
    period_expr = (
        f"case when application_submission_time >= {current_month_start_ms} "
        f"then '{period.current_month_label}' else '{period.previous_month_label}' end"
    )
    product_expr = "coalesce(nullif(product_code, ''), 'UNKNOWN')"
    subproduct_expr = "coalesce(nullif(sub_product_code, ''), '-')"
    status_bucket_expr = """case
  when upper(coalesce(underwriting_status, '')) rlike '(APPROV|PASS|SUCCESS|COMPLETED)' then 'APPROVED'
  when upper(coalesce(underwriting_status, '')) rlike '(REJECT|DECLIN|FAIL|DENY)' then 'REJECTED'
  else 'PENDING'
end"""
    base_where = (
        f"{snapshot_filter}\n"
        f"  and application_submission_time >= {start_ms}\n"
        f"  and application_submission_time < {end_ms}"
    )
    return f"""-- Credit Risk PH - Underwriting Funnel MIS
-- Duration: {period.title}
-- Snapshot: {snapshot_pt_date or "latest available pt_date at run time"}
-- Run each query in Data Workbench. These are the aggregation queries used to build the Portal Excel.

-- 1. Summary by Product
select {period_expr} as period, {product_expr} as product,
  count(1) as applications,
  sum(case when {status_bucket_expr} = 'APPROVED' then 1 else 0 end) as approved,
  sum(case when {status_bucket_expr} = 'REJECTED' then 1 else 0 end) as rejected,
  sum(case when {status_bucket_expr} = 'PENDING' then 1 else 0 end) as pending,
  round(avg(cast(apply_loan_amount as double)), 2) as avg_applied_amount
from {UNDERWRITING_FUNNEL_TABLE}
where {base_where}
group by {period_expr}, {product_expr}
order by period, product;

-- 2. Product Funnel
select {period_expr} as period, {product_expr} as product,
  coalesce(nullif(underwriting_status, ''), 'PENDING') as status,
  count(1) as count
from {UNDERWRITING_FUNNEL_TABLE}
where {base_where}
group by {period_expr}, {product_expr}, coalesce(nullif(underwriting_status, ''), 'PENDING')
order by period, product, status;

-- 3. Product Reject Reasons
select {period_expr} as period, {product_expr} as product,
  coalesce(nullif(reject_reason, ''), 'Unspecified') as reject_reason,
  count(1) as count
from {UNDERWRITING_FUNNEL_TABLE}
where {base_where}
  and {status_bucket_expr} = 'REJECTED'
group by {period_expr}, {product_expr}, coalesce(nullif(reject_reason, ''), 'Unspecified')
order by period, product, count desc;

-- 4. Product Stage Backlog
select {period_expr} as period, {product_expr} as product,
  coalesce(nullif(current_stage, ''), 'Unspecified') as current_stage,
  count(1) as count,
  min(create_date) as oldest_create_date_ms,
  round(avg(case when create_date is not null then ({active_now_ms} - cast(create_date as double)) / 86400000.0 else null end), 2) as avg_age_days
from {UNDERWRITING_FUNNEL_TABLE}
where {base_where}
group by {period_expr}, {product_expr}, coalesce(nullif(current_stage, ''), 'Unspecified')
order by period, product, count desc;

-- 5. Sub-product Funnel
select {period_expr} as period, {product_expr} as product, {subproduct_expr} as sub_product,
  coalesce(nullif(underwriting_status, ''), 'PENDING') as status,
  count(1) as count
from {UNDERWRITING_FUNNEL_TABLE}
where {base_where}
group by {period_expr}, {product_expr}, {subproduct_expr}, coalesce(nullif(underwriting_status, ''), 'PENDING')
order by period, product, sub_product, status;
"""


def _normalize_header(value: Any) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return normalized


def _display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value).strip()


def _read_csv_export(content: bytes) -> list[dict[str, Any]]:
    text = content.decode("utf-8-sig", errors="replace")
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows: list[dict[str, Any]] = []
    for row in reader:
        rows.append({_normalize_header(key): value for key, value in (row or {}).items() if key is not None})
    return rows


def _read_xlsx_export(content: bytes) -> list[dict[str, Any]]:
    workbook = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    if not workbook.worksheets:
        return []
    sheet = workbook.worksheets[0]
    raw_rows = list(sheet.iter_rows(values_only=True))
    if not raw_rows:
        return []
    headers = [_normalize_header(value) for value in raw_rows[0]]
    rows: list[dict[str, Any]] = []
    for raw_row in raw_rows[1:]:
        item = {
            headers[index]: value
            for index, value in enumerate(raw_row)
            if index < len(headers) and headers[index]
        }
        if any(_display_value(value) for value in item.values()):
            rows.append(item)
    return rows


def read_underwriting_export(content: bytes, filename: str) -> list[dict[str, Any]]:
    suffix = Path(filename or "").suffix.lower()
    if suffix in {".xlsx", ".xlsm"}:
        return _read_xlsx_export(content)
    if suffix in {".csv", ".txt"}:
        return _read_csv_export(content)
    raise ToolError("Upload a Data Workbench export as .csv or .xlsx.")


def _parse_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(BUSINESS_INSIGHTS_TIMEZONE) if value.tzinfo else value.replace(tzinfo=BUSINESS_INSIGHTS_TIMEZONE)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=BUSINESS_INSIGHTS_TIMEZONE)
    numeric = _parse_number(value)
    if numeric is not None:
        if numeric > 10_000_000_000:
            return datetime.fromtimestamp(numeric / 1000, tz=BUSINESS_INSIGHTS_TIMEZONE)
        if numeric > 100_000_000:
            return datetime.fromtimestamp(numeric, tz=BUSINESS_INSIGHTS_TIMEZONE)
    text = str(value).strip()
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed.astimezone(BUSINESS_INSIGHTS_TIMEZONE) if parsed.tzinfo else parsed.replace(tzinfo=BUSINESS_INSIGHTS_TIMEZONE)
        except ValueError:
            continue
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            parsed = datetime.strptime(text, pattern)
            return parsed.replace(tzinfo=BUSINESS_INSIGHTS_TIMEZONE)
        except ValueError:
            continue
    return None


def _row_text(row: dict[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        text = _display_value(value)
        if text:
            return text
    return default


def _row_datetime(row: dict[str, Any]) -> datetime | None:
    for key in ("application_submission_time", "create_date", "modify_date"):
        parsed = _parse_datetime(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _status_bucket(status: str) -> str:
    normalized = status.strip().upper()
    if not normalized:
        return "PENDING"
    if any(token in normalized for token in ("APPROV", "PASS", "SUCCESS", "COMPLETED")):
        return "APPROVED"
    if any(token in normalized for token in ("REJECT", "DECLIN", "FAIL", "DENY")):
        return "REJECTED"
    return "PENDING"


def _period_label(row: dict[str, Any], period: BusinessInsightsPeriod) -> str:
    explicit = _row_text(row, "report_period")
    if explicit:
        return explicit
    row_date = _row_datetime(row)
    if row_date and row_date.date() >= period.current_month_start:
        return period.current_month_label
    return period.previous_month_label


def _pct(part: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return round(float(part) / float(total), 4)


def _append_rows(sheet: Any, headers: list[str], rows: list[list[Any]]) -> None:
    sheet.append(headers)
    for row in rows:
        sheet.append(row)
    _style_sheet(sheet)


def _style_sheet(sheet: Any) -> None:
    header_fill = PatternFill("solid", fgColor="EAF2FF")
    header_font = Font(bold=True, color="1F2937")
    for cell in sheet[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for row in sheet.iter_rows():
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
    for index, column_cells in enumerate(sheet.columns, start=1):
        width = 12
        for cell in column_cells:
            width = max(width, min(len(_display_value(cell.value)) + 2, 42))
        sheet.column_dimensions[get_column_letter(index)].width = width
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions


def build_underwriting_funnel_workbook(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> bytes:
    if not rows:
        raise ToolError("The uploaded export has no data rows.")
    period = business_insights_period(now)
    active_now = now.astimezone(BUSINESS_INSIGHTS_TIMEZONE) if now else datetime.now(BUSINESS_INSIGHTS_TIMEZONE)
    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    summary: dict[tuple[str, str], dict[str, Any]] = {}
    funnel: dict[tuple[str, str, str], int] = {}
    reject_reasons: dict[tuple[str, str, str], int] = {}
    stage_backlog: dict[tuple[str, str, str], dict[str, Any]] = {}
    subproduct_funnel: dict[tuple[str, str, str, str], int] = {}

    normalized_rows: list[dict[str, Any]] = []
    for raw_row in rows:
        row = {_normalize_header(key): value for key, value in raw_row.items()}
        product = _row_text(row, "product_code", default="UNKNOWN")
        sub_product = _row_text(row, "sub_product_code", default="-") or "-"
        status = _row_text(row, "underwriting_status", default="PENDING").upper()
        status_bucket = _status_bucket(status)
        report_period = _period_label(row, period)
        amount = _parse_number(row.get("apply_loan_amount"))
        row_dt = _row_datetime(row)
        row_date = row_dt.date().isoformat() if row_dt else ""
        reason = _row_text(row, "reject_reason", default="Unspecified")
        stage = _row_text(row, "current_stage", "step", default="Unspecified")

        summary_item = summary.setdefault(
            (report_period, product),
            {"applications": 0, "approved": 0, "rejected": 0, "pending": 0, "amount_sum": 0.0, "amount_count": 0},
        )
        summary_item["applications"] += 1
        summary_item[status_bucket.lower()] += 1
        if amount is not None:
            summary_item["amount_sum"] += amount
            summary_item["amount_count"] += 1

        funnel[(report_period, product, status or "PENDING")] = funnel.get((report_period, product, status or "PENDING"), 0) + 1
        if status_bucket == "REJECTED":
            reject_reasons[(report_period, product, reason)] = reject_reasons.get((report_period, product, reason), 0) + 1
        stage_item = stage_backlog.setdefault(
            (report_period, product, stage),
            {"count": 0, "oldest": None, "age_days_sum": 0.0, "age_count": 0},
        )
        stage_item["count"] += 1
        if row_dt is not None:
            oldest = stage_item["oldest"]
            stage_item["oldest"] = row_dt if oldest is None or row_dt < oldest else oldest
            stage_item["age_days_sum"] += max(0.0, (active_now - row_dt).total_seconds() / 86400)
            stage_item["age_count"] += 1
        subproduct_funnel[(report_period, product, sub_product, status or "PENDING")] = (
            subproduct_funnel.get((report_period, product, sub_product, status or "PENDING"), 0) + 1
        )

        normalized_row = {"report_period": report_period, "application_date": row_date}
        normalized_row.update(row)
        normalized_rows.append(normalized_row)

    summary_rows = []
    for (report_period, product), item in sorted(summary.items()):
        applications = item["applications"]
        avg_amount = round(item["amount_sum"] / item["amount_count"], 2) if item["amount_count"] else ""
        summary_rows.append(
            [
                report_period,
                product,
                applications,
                item["approved"],
                item["rejected"],
                item["pending"],
                _pct(item["approved"], applications),
                avg_amount,
            ]
        )
    _append_rows(
        workbook.create_sheet("Summary by Product"),
        ["Period", "Product", "Applications", "Approved", "Rejected", "Pending", "Approval Rate", "Avg Applied Amount"],
        summary_rows,
    )

    product_totals = {(period_label, product): sum(count for (p, prod, _status), count in funnel.items() if p == period_label and prod == product) for period_label, product, _status in funnel}
    funnel_rows = [
        [report_period, product, status, count, _pct(count, product_totals.get((report_period, product), 0))]
        for (report_period, product, status), count in sorted(funnel.items())
    ]
    _append_rows(workbook.create_sheet("Product Funnel"), ["Period", "Product", "Status", "Count", "% Within Product"], funnel_rows)

    rejected_totals = {
        (period_label, product): sum(count for (p, prod, _reason), count in reject_reasons.items() if p == period_label and prod == product)
        for period_label, product, _reason in reject_reasons
    }
    reason_rows = [
        [report_period, product, reason, count, _pct(count, rejected_totals.get((report_period, product), 0))]
        for (report_period, product, reason), count in sorted(reject_reasons.items())
    ]
    _append_rows(workbook.create_sheet("Product Reject Reasons"), ["Period", "Product", "Reject Reason", "Count", "% of Product Rejections"], reason_rows)

    stage_rows = []
    for (report_period, product, stage), item in sorted(stage_backlog.items()):
        oldest_dt = item["oldest"]
        avg_age = round(item["age_days_sum"] / item["age_count"], 2) if item["age_count"] else ""
        stage_rows.append([report_period, product, stage, item["count"], oldest_dt.date().isoformat() if oldest_dt else "", avg_age])
    _append_rows(workbook.create_sheet("Product Stage Backlog"), ["Period", "Product", "Current Stage", "Count", "Oldest Create Date", "Avg Age Days"], stage_rows)

    subproduct_totals = {
        (period_label, product, sub_product): sum(
            count
            for (p, prod, sub, _status), count in subproduct_funnel.items()
            if p == period_label and prod == product and sub == sub_product
        )
        for period_label, product, sub_product, _status in subproduct_funnel
    }
    subproduct_rows = [
        [report_period, product, sub_product, status, count, _pct(count, subproduct_totals.get((report_period, product, sub_product), 0))]
        for (report_period, product, sub_product, status), count in sorted(subproduct_funnel.items())
    ]
    _append_rows(
        workbook.create_sheet("Sub-product Funnel"),
        ["Period", "Product", "Sub-product", "Status", "Count", "% Within Sub-product"],
        subproduct_rows,
    )

    raw_headers = ["report_period", "application_date", *UNDERWRITING_FUNNEL_FIELDS]
    extra_headers = sorted({key for row in normalized_rows for key in row.keys()} - set(raw_headers))
    raw_sheet = workbook.create_sheet("Raw Export")
    _append_rows(raw_sheet, [*raw_headers, *extra_headers], [[_display_value(row.get(header)) for header in [*raw_headers, *extra_headers]] for row in normalized_rows])

    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


class BusinessInsightsStore:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = Path(root_dir)
        self.metadata_path = self.root_dir / "reports.json"
        self.artifacts_dir = self.root_dir / "artifacts"
        self._lock = threading.Lock()

    def _load(self) -> dict[str, Any]:
        if not self.metadata_path.exists():
            return {"artifacts": {}}
        try:
            payload = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"artifacts": {}}
        return payload if isinstance(payload, dict) else {"artifacts": {}}

    def _persist_locked(self, payload: dict[str, Any]) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        temp_path = self.metadata_path.with_name(f".{self.metadata_path.name}.{os.getpid()}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        os.replace(temp_path, self.metadata_path)

    def domains(self) -> list[dict[str, str]]:
        return [dict(item) for item in BUSINESS_INSIGHTS_DOMAINS]

    def reports(self, domain: str = "") -> list[dict[str, Any]]:
        domain_key = str(domain or "").strip().lower()
        payload = self._load()
        artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
        reports: list[dict[str, Any]] = []
        for report in SEEDED_REPORTS:
            if domain_key and report["domain"] != domain_key:
                continue
            item = dict(report)
            artifact = artifacts.get(report["id"]) if isinstance(artifacts.get(report["id"]), dict) else None
            item["artifact"] = dict(artifact) if artifact else None
            reports.append(item)
        return reports

    def report(self, report_id: str) -> dict[str, Any] | None:
        normalized_id = str(report_id or "").strip()
        return next((report for report in self.reports() if report["id"] == normalized_id), None)

    def sql_for_report(self, report_id: str, *, now: datetime | None = None) -> str:
        if report_id != UNDERWRITING_FUNNEL_REPORT_ID:
            raise ToolError("SQL is not configured for this report yet.")
        report = self.report(report_id) or {}
        artifact = report.get("artifact") if isinstance(report.get("artifact"), dict) else {}
        saved_sql = str(artifact.get("sql") or "").strip() if isinstance(artifact, dict) else ""
        if saved_sql:
            return saved_sql
        return build_underwriting_funnel_mis_sql(now=now)

    def sql_filename_for_report(self, report_id: str) -> str:
        if report_id != UNDERWRITING_FUNNEL_REPORT_ID:
            raise ToolError("SQL is not configured for this report yet.")
        return f"{UNDERWRITING_FUNNEL_REPORT_ID}.sql"

    def save_underwriting_export(self, *, content: bytes, filename: str, now: datetime | None = None) -> dict[str, Any]:
        rows = read_underwriting_export(content, filename)
        workbook_bytes = build_underwriting_funnel_workbook(rows, now=now)
        artifact_id = uuid.uuid4().hex
        safe_filename = f"{UNDERWRITING_FUNNEL_REPORT_ID}-{artifact_id[:8]}.xlsx"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.artifacts_dir / safe_filename
        artifact_path.write_bytes(workbook_bytes)
        metadata = {
            "id": artifact_id,
            "report_id": UNDERWRITING_FUNNEL_REPORT_ID,
            "filename": safe_filename,
            "source_filename": Path(filename or "export").name,
            "row_count": len(rows),
            "created_at": time_module.strftime("%Y-%m-%dT%H:%M:%SZ", time_module.gmtime()),
            "sql": build_underwriting_funnel_sql(now),
        }
        with self._lock:
            payload = self._load()
            artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
            artifacts[UNDERWRITING_FUNNEL_REPORT_ID] = metadata
            payload["artifacts"] = artifacts
            self._persist_locked(payload)
        return dict(metadata)

    def artifact_path(self, artifact_id: str) -> tuple[dict[str, Any], Path]:
        payload = self._load()
        artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
        for metadata in artifacts.values():
            if not isinstance(metadata, dict):
                continue
            if str(metadata.get("id") or "") == str(artifact_id or ""):
                path = (self.artifacts_dir / str(metadata.get("filename") or "")).resolve()
                root = self.artifacts_dir.resolve()
                if root not in path.parents or not path.exists():
                    raise ToolError("Business Insights artifact was not found.")
                return dict(metadata), path
        raise ToolError("Business Insights artifact was not found.")
