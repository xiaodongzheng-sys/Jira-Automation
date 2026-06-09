#!/usr/bin/env python3
"""Generate Business Insights live report artifacts from Data Workbench.

This is an operational helper for the Mac-hosted live portal. It uses the
current Chrome Data Admin live session, runs aggregate SparkSQL sections in
Data Workbench, and writes Excel plus self-contained HTML visualization
artifacts into TEAM_PORTAL_DATA_DIR/business_insights.
"""
from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
from datetime import UTC, datetime
import html
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
from typing import Any, Callable
import uuid
from zoneinfo import ZoneInfo

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.business_insights import (  # noqa: E402
    AF_REQUEST_STATISTIC_TABLE,
    AF_RULE_CONFIG_TABLE,
    AF_RULE_EFFECTIVENESS_REPORT_ID,
    AF_RULES_FEATURES_REPORT_ID,
    AF_SCENARIO_FLOW_CONFIG_TABLE,
    AF_SCENARIOS_ACTIONS_REPORT_ID,
    APPLICATION_DISBURSEMENT_FUNNEL_REPORT_ID,
    CREDIT_LIMIT_TABLE,
    LIMIT_UTILIZATION_REPORT_ID,
    LOAN_APPLICATION_TABLE,
    PRODUCT_LABELS,
    PRODUCT_LABEL_COLUMNS,
    PORTFOLIO_REPAYMENT_REPORT_ID,
    REPAY_PLAN_TABLE,
    UNDERWRITING_FUNNEL_REPORT_ID,
    build_af_rule_effectiveness_sql,
    build_af_rules_features_sql,
    build_af_scenarios_actions_sql,
    build_application_disbursement_funnel_sql,
    build_limit_utilization_sql,
    build_portfolio_repayment_sql,
    product_label,
)

DATA_ADMIN_BASE_URL = "https://data-admin.ph.seabank.io"
DATA_ADMIN_TEAM_ID = "T0300036"
LIVE_HOST_ROOT = Path("/Users/NPTSG0388/Workspace/jira-creation-stack-host")
DEFAULT_PORTAL_DATA_DIR = LIVE_HOST_ROOT / ".team-portal"
REPORT_BUILDERS: dict[str, tuple[str, Callable[..., str]]] = {
    PORTFOLIO_REPAYMENT_REPORT_ID: ("Credit Risk PH - Portfolio Repayment", build_portfolio_repayment_sql),
    LIMIT_UTILIZATION_REPORT_ID: ("Credit Risk PH - Limit Utilization", build_limit_utilization_sql),
    APPLICATION_DISBURSEMENT_FUNNEL_REPORT_ID: (
        "Credit Risk PH - Application to Disbursement Funnel",
        build_application_disbursement_funnel_sql,
    ),
    AF_SCENARIOS_ACTIONS_REPORT_ID: (
        "Anti-fraud PH - L1+L2 Scenarios, Actions & Auth Steps",
        build_af_scenarios_actions_sql,
    ),
    AF_RULES_FEATURES_REPORT_ID: (
        "Anti-fraud PH - Rules & Features",
        build_af_rules_features_sql,
    ),
    AF_RULE_EFFECTIVENESS_REPORT_ID: (
        "Anti-fraud PH - Rule Effectiveness / Hit-Rate",
        build_af_rule_effectiveness_sql,
    ),
}

# Anchor table per report used to resolve the latest available pt_date when the
# snapshot is "latest". Tables in a report share a daily snapshot, so the anchor's
# max(pt_date) is used to pin every section to the same snapshot.
REPORT_SNAPSHOT_ANCHOR_TABLE: dict[str, str] = {
    PORTFOLIO_REPAYMENT_REPORT_ID: REPAY_PLAN_TABLE,
    LIMIT_UTILIZATION_REPORT_ID: CREDIT_LIMIT_TABLE,
    APPLICATION_DISBURSEMENT_FUNNEL_REPORT_ID: LOAN_APPLICATION_TABLE,
    AF_SCENARIOS_ACTIONS_REPORT_ID: AF_SCENARIO_FLOW_CONFIG_TABLE,
    AF_RULES_FEATURES_REPORT_ID: AF_RULE_CONFIG_TABLE,
    AF_RULE_EFFECTIVENESS_REPORT_ID: AF_REQUEST_STATISTIC_TABLE,
}

LATEST_SNAPSHOT = "latest"

PREFERRED_PRODUCT_CODES: dict[str, str] = {
    "Credit Card": "812F",
}
PREFERRED_SUB_PRODUCT_CODES: dict[str, str] = {
    "Employee Loan": "108",
}
@dataclass(frozen=True)
class WorkbenchSection:
    sheet_name: str
    query: str


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    try:
        payload = token.split(".")[1]
        payload += "=" * ((4 - len(payload) % 4) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("ascii"))
        data = json.loads(decoded)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _chrome_safe_storage_secret() -> bytes:
    return subprocess.check_output(
        ["security", "find-generic-password", "-w", "-s", "Chrome Safe Storage"],
        stderr=subprocess.DEVNULL,
    ).strip()


def _chrome_cookie_key() -> bytes:
    return PBKDF2HMAC(
        algorithm=hashes.SHA1(),
        length=16,
        salt=b"saltysalt",
        iterations=1003,
        backend=default_backend(),
    ).derive(_chrome_safe_storage_secret())


def _decrypt_chrome_cookie(encrypted_value: bytes, key: bytes) -> str:
    if not encrypted_value.startswith(b"v10"):
        raise RuntimeError("Unsupported Chrome cookie encryption format.")
    decryptor = Cipher(algorithms.AES(key), modes.CBC(b" " * 16), backend=default_backend()).decryptor()
    plaintext = decryptor.update(encrypted_value[3:])
    pad = plaintext[-1]
    if 1 <= pad <= 16:
        plaintext = plaintext[:-pad]
    # Chrome prefixes v10 cookie plaintext with a 32-byte host digest.
    return plaintext[32:].decode("utf-8")


def load_data_admin_token(*, chrome_profile: str = "Default") -> str:
    cookies_path = Path.home() / "Library/Application Support/Google/Chrome" / chrome_profile / "Cookies"
    if not cookies_path.exists():
        raise RuntimeError(f"Chrome cookie DB not found: {cookies_path}")
    key = _chrome_cookie_key()
    connection = sqlite3.connect(cookies_path)
    try:
        row = connection.execute(
            """
            select encrypted_value
            from cookies
            where host_key = 'data-admin.ph.seabank.io'
              and name = 'bank-admin-token'
            order by expires_utc desc
            limit 1
            """
        ).fetchone()
    finally:
        connection.close()
    if not row:
        raise RuntimeError("Data Admin live bank-admin-token cookie was not found.")
    token = _decrypt_chrome_cookie(bytes(row[0]), key)
    if not token.startswith("eyJ"):
        raise RuntimeError("Decrypted Data Admin token is not a JWT.")
    return token


def build_data_admin_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "portal-request": "true",
            "cache": "no-store",
            "bank-admin-token": token,
            "dual-active-mark": "live",
            "content-type": "application/json",
            "portal-permission-code": "workbench_process",
        }
    )
    session.cookies.update({"bank-admin-token": token, "dual-active-mark": "live"})
    return session


def validate_data_admin_session(session: requests.Session) -> str:
    response = session.get(f"{DATA_ADMIN_BASE_URL}/api/session/session/v1/userInfo", timeout=10)
    if response.status_code != 200:
        raise RuntimeError(f"Data Admin session is not valid: HTTP {response.status_code} {response.text[:160]}")
    payload = response.json()
    if payload.get("code") != 0:
        raise RuntimeError(f"Data Admin userInfo failed: {payload}")
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    return str(data.get("email") or data.get("userId") or "")


def extract_sql_sections(sql: str) -> list[WorkbenchSection]:
    matches = list(re.finditer(r"^--\s+\d+\.\s+(.+?)\s*$", sql, flags=re.M))
    sections: list[WorkbenchSection] = []
    for index, match in enumerate(matches):
        sheet_name = match.group(1).strip()[:31]
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(sql)
        query = sql[start:end].strip()
        query = re.sub(r"^--.*?$", "", query, flags=re.M).strip().rstrip(";").strip()
        if query:
            sections.append(WorkbenchSection(sheet_name=sheet_name, query=query))
    return sections


def resolve_snapshot_pt_date(
    session: requests.Session,
    report_id: str,
    *,
    poll_seconds: int,
    max_polls: int,
) -> str | None:
    """Return the latest available pt_date for the report's anchor table.

    Returns None when no anchor is configured; callers then let each SQL section
    fall back to its own ``max(pt_date)`` subquery.
    """
    table = REPORT_SNAPSHOT_ANCHOR_TABLE.get(report_id)
    if not table:
        return None
    _schema, rows, _execution_id = run_workbench_query(
        session,
        WorkbenchSection(sheet_name="resolve snapshot", query=f"select max(pt_date) as pt_date from {table}"),
        poll_seconds=poll_seconds,
        max_polls=max_polls,
    )
    if rows and rows[0] and rows[0][0]:
        return str(rows[0][0]).strip()
    return None


def run_workbench_query(
    session: requests.Session,
    section: WorkbenchSection,
    *,
    poll_seconds: int,
    max_polls: int,
) -> tuple[list[str], list[list[Any]], str]:
    run_response = session.post(
        f"{DATA_ADMIN_BASE_URL}/api/workbench-service/adhoc/run",
        json={
            "engine": "SparkSQL",
            "script": section.query,
            "teamId": DATA_ADMIN_TEAM_ID,
            "parameterList": [],
            "sparkParam": "",
        },
        timeout=30,
    )
    run_response.raise_for_status()
    run_payload = run_response.json()
    if run_payload.get("code") != 0:
        raise RuntimeError(f"{section.sheet_name}: Data Workbench run failed: {run_payload}")
    data = run_payload.get("data") if isinstance(run_payload.get("data"), dict) else {}
    task = (data.get("taskList") or [{}])[0]
    execution_id = str(task.get("executionId") or data.get("runId") or "")
    if not execution_id:
        raise RuntimeError(f"{section.sheet_name}: Data Workbench did not return executionId.")
    print(f"{section.sheet_name}: execution_id={execution_id}", flush=True)

    final_status = ""
    last_log = ""
    for _attempt in range(max_polls):
        log_response = session.post(
            f"{DATA_ADMIN_BASE_URL}/api/workbench-service/adhoc/log",
            json={"executionId": execution_id, "startOffset": 0},
            timeout=60,
        )
        if log_response.status_code == 401:
            raise RuntimeError(f"{section.sheet_name}: Data Admin session became invalid while polling.")
        log_response.raise_for_status()
        log_payload = log_response.json()
        log_data = log_payload.get("data") if isinstance(log_payload.get("data"), dict) else {}
        final_status = str(log_data.get("status") or "")
        last_log = str(log_data.get("log") or log_response.text)
        if final_status in {"SUCCESS", "FAILED", "CANCELED"}:
            break
        time.sleep(poll_seconds)
    if final_status != "SUCCESS":
        raise RuntimeError(f"{section.sheet_name}: Data Workbench ended with {final_status}: {last_log[-1200:]}")

    result_response = session.post(
        f"{DATA_ADMIN_BASE_URL}/api/workbench-service/adhoc/result",
        json={"executionId": execution_id},
        timeout=90,
    )
    if result_response.status_code == 401:
        raise RuntimeError(f"{section.sheet_name}: Data Admin session became invalid while fetching result.")
    result_response.raise_for_status()
    result_payload = result_response.json()
    if result_payload.get("code") != 0:
        raise RuntimeError(f"{section.sheet_name}: Data Workbench result failed: {result_payload}")
    table_data = (result_payload.get("data") or {}).get("tableData") or {}
    schema = [str(item) for item in (table_data.get("schema") or [])]
    rows = table_data.get("data") or []
    print(f"{section.sheet_name}: rows={len(rows)}", flush=True)
    return schema, rows, execution_id


def style_sheet(sheet: Any) -> None:
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
            width = max(width, min(len(str(cell.value or "")) + 2, 44))
        sheet.column_dimensions[get_column_letter(index)].width = width
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions


def _excel_sheet_title(sheet_name: str) -> str:
    # openpyxl rejects : \ / ? * [ ] in sheet titles, which are capped at 31 chars.
    return re.sub(r"[:\\/?*\[\]]", " ", str(sheet_name)).strip()[:31] or "Sheet"


def write_workbook(path: Path, sheets: list[tuple[str, list[str], list[list[Any]]]]) -> None:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for sheet_name, schema, rows in sheets:
        sheet = workbook.create_sheet(_excel_sheet_title(sheet_name))
        sheet.append(schema)
        for row in rows:
            sheet.append(row)
        style_sheet(sheet)
    workbook.save(path)


def normalize_product_labels(
    sheets: list[tuple[str, list[str], list[list[Any]]]],
    *,
    preserve_raw_export: bool = True,
) -> list[tuple[str, list[str], list[list[Any]]]]:
    normalized_sheets: list[tuple[str, list[str], list[list[Any]]]] = []
    for sheet_name, headers, rows in sheets:
        if preserve_raw_export and sheet_name == "Raw Export":
            normalized_sheets.append((sheet_name, headers, rows))
            continue
        product_offsets = {
            offset
            for offset, header in enumerate(headers)
            if str(header).strip().lower() in PRODUCT_LABEL_COLUMNS
        }
        if not product_offsets:
            normalized_sheets.append((sheet_name, headers, rows))
            continue
        normalized_rows = []
        for row in rows:
            normalized_row = list(row)
            for offset in product_offsets:
                if offset < len(normalized_row):
                    normalized_row[offset] = product_label(normalized_row[offset])
            normalized_rows.append(normalized_row)
        normalized_sheets.append((sheet_name, headers, normalized_rows))
    return normalized_sheets


def _number(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(str(value).replace(",", ""))
    except Exception:
        return 0.0


def _is_number(value: Any) -> bool:
    if isinstance(value, bool) or value in (None, ""):
        return False
    if isinstance(value, (int, float)):
        return True
    try:
        float(str(value).replace(",", ""))
    except Exception:
        return False
    return True


def _format_number(value: Any, *, suffix: str = "") -> str:
    if not _is_number(value):
        return str(value or "")
    number = _number(value)
    sign = "-" if number < 0 else ""
    absolute = abs(number)
    if absolute >= 1_000_000_000_000:
        text = f"{sign}{absolute / 1_000_000_000_000:.2f}T"
    elif absolute >= 1_000_000_000:
        text = f"{sign}{absolute / 1_000_000_000:.2f}B"
    elif absolute >= 1_000_000:
        text = f"{sign}{absolute / 1_000_000:.2f}M"
    elif absolute >= 10_000:
        text = f"{sign}{absolute:,.0f}"
    elif absolute == int(absolute):
        text = f"{sign}{absolute:,.0f}"
    else:
        text = f"{sign}{absolute:,.2f}"
    return f"{text}{suffix}"


def _preferred_code_for_label(label: str, *, header: str = "") -> str:
    normalized_header = header.strip().lower()
    if normalized_header in {"sub-product", "sub_product_code"} and label in PREFERRED_SUB_PRODUCT_CODES:
        return PREFERRED_SUB_PRODUCT_CODES[label]
    if normalized_header in {"product", "product_code"} and label in PREFERRED_PRODUCT_CODES:
        return PREFERRED_PRODUCT_CODES[label]
    candidates = [code for code, mapped in PRODUCT_LABELS.items() if mapped == label]
    if not candidates:
        return ""
    if normalized_header in {"product", "product_code"}:
        product_candidates = [code for code in candidates if code.upper().startswith("8")]
        if product_candidates:
            return sorted(product_candidates)[0]
    if normalized_header in {"sub-product", "sub_product_code"}:
        subproduct_candidates = [code for code in candidates if not code.upper().startswith("8")]
        if subproduct_candidates:
            return sorted(subproduct_candidates)[0]
    return sorted(candidates)[0]


def _product_display_label(product: Any, *, header: str = "product") -> str:
    raw = str(product or "").strip()
    if not raw:
        return ""
    if " - " in raw:
        return raw
    label = product_label(raw)
    if label != raw:
        return f"{raw} - {label}"
    preferred_code = _preferred_code_for_label(raw, header=header)
    return f"{preferred_code} - {raw}" if preferred_code else raw


def _format_cell(header: str, value: Any) -> str:
    lowered = header.lower()
    if lowered in PRODUCT_LABEL_COLUMNS:
        return _product_display_label(value, header=lowered)
    if lowered.endswith("rate") or lowered.startswith("%") or "% " in lowered:
        number = _number(value)
        if value in (None, ""):
            return ""
        if abs(number) <= 1:
            number *= 100
        return f"{number:.1f}%"
    return _format_number(value) if _is_number(value) else str(value or "")


def _product_data_attr(product: Any, *, header: str = "product") -> str:
    label = _product_display_label(product, header=header)
    return f' data-product="{html.escape(label, quote=True)}"' if label else ""


def _product_filter_options(sheets: list[tuple[str, list[str], list[list[Any]]]]) -> list[str]:
    products: set[str] = set()
    for sheet_name, headers, rows in sheets:
        if sheet_name == "Raw Export":
            continue
        product_offsets = [
            offset
            for offset, header in enumerate(headers)
            if str(header).strip().lower() in {"product", "product_code"}
        ]
        for row in rows:
            for offset in product_offsets:
                if offset < len(row):
                    products.add(_product_display_label(row[offset], header="product"))
    return sorted(product for product in products if product and product != "UNKNOWN")


def _product_filter_html(products: list[str]) -> str:
    if len(products) < 2:
        return ""
    options = ['<option value="">All products</option>']
    options.extend(
        f'<option value="{html.escape(product, quote=True)}">{html.escape(product)}</option>'
        for product in products
    )
    return (
        '<section class="filter-card wide">'
        '<div><p class="eyebrow">View Controls</p><h2>Product Filter</h2>'
        '<p>Filters product-level charts and tables. All-product visuals are hidden when a single product is selected.</p></div>'
        f'<label><span>Product</span><select data-product-filter>{"".join(options)}</select></label>'
        "</section>"
    )


def _table_html(headers: list[str], rows: list[list[Any]], *, page_size: int = 50) -> str:
    header_html = "".join(f"<th>{html.escape(str(header))}</th>" for header in headers)
    product_offset = next(
        (
            offset
            for offset, header in enumerate(headers)
            if str(header).strip().lower() in {"product", "product_code"}
        ),
        None,
    )
    body_rows = []
    for row in rows:
        cells = []
        for index, header in enumerate(headers):
            value = row[index] if index < len(row) else ""
            class_name = "num" if _is_number(value) else ""
            cells.append(f'<td class="{class_name}">{html.escape(_format_cell(str(header), value))}</td>')
        product_attr = _product_data_attr(row[product_offset], header=str(headers[product_offset]).strip().lower()) if product_offset is not None and product_offset < len(row) else ""
        body_rows.append(f"<tr{product_attr}>" + "".join(cells) + "</tr>")
    controls = (
        '<div class="table-pagination" data-table-pagination>'
        '<button type="button" data-page-prev>Previous</button>'
        '<span data-page-info></span>'
        '<button type="button" data-page-next>Next</button>'
        '</div>'
        if len(rows) > page_size
        else ""
    )
    return (
        f'<div class="table-wrap"><table class="bi-table" data-page-size="{page_size}">'
        f'<thead><tr>{header_html}</tr></thead><tbody>{"".join(body_rows)}</tbody></table></div>{controls}'
    )


def _bar_chart(
    title: str,
    labels: list[Any],
    values: list[Any],
    *,
    value_suffix: str = "",
    labels_are_products: bool = False,
) -> str:
    pairs = [
        (_product_display_label(label, header="product") if labels_are_products else str(label or "UNKNOWN"), _number(value))
        for label, value in zip(labels, values, strict=False)
    ]
    pairs = [item for item in pairs if item[1] != 0][:12]
    if not pairs:
        return ""
    maximum = max(value for _label, value in pairs) or 1
    rows = []
    for label, value in pairs:
        width = max(2.0, value / maximum * 100.0)
        product_attr = _product_data_attr(label) if labels_are_products else ""
        rows.append(
            f'<div class="bar-row"{product_attr}>'
            f"<span>{html.escape(label)}</span>"
            f'<div class="bar-track"><div class="bar" style="width:{width:.1f}%"></div></div>'
            f"<b>{html.escape(_format_number(value, suffix=value_suffix))}</b>"
            "</div>"
        )
    scope = ' data-product-visual="1"' if labels_are_products else ' data-global-visual="1"'
    return f'<section class="panel"{scope}><h2>{html.escape(title)}</h2>{"".join(rows)}</section>'


def _group_sum(headers: list[str], rows: list[list[Any]], label_column: str, value_column: str) -> list[tuple[str, float]]:
    index = _sheet_index(headers)
    if label_column not in index or value_column not in index:
        return []
    grouped: dict[str, float] = {}
    label_offset = index[label_column]
    value_offset = index[value_column]
    for row in rows:
        label = str(row[label_offset] if label_offset < len(row) and row[label_offset] not in (None, "") else "UNKNOWN")
        if label_column.lower() in {"product", "product_code", "sub-product", "sub_product_code"}:
            label = _product_display_label(label, header=label_column.lower())
        value = _number(row[value_offset] if value_offset < len(row) else None)
        grouped[label] = grouped.get(label, 0.0) + value
    return sorted(grouped.items(), key=lambda item: item[1], reverse=True)


def _donut_chart(title: str, values: list[tuple[str, float, str]]) -> str:
    filtered = [(label, value, color) for label, value, color in values if value > 0]
    total = sum(value for _label, value, _color in filtered)
    if total <= 0:
        return ""
    start = 0.0
    segments = []
    legend = []
    for label, value, color in filtered:
        end = start + value / total * 100.0
        segments.append(f"{color} {start:.2f}% {end:.2f}%")
        legend.append(
            '<div class="legend-row">'
            f'<span class="legend-dot" style="background:{html.escape(color)}"></span>'
            f"<span>{html.escape(label)}</span><b>{html.escape(_format_number(value))}</b>"
            "</div>"
        )
        start = end
    return (
        f'<section class="panel" data-global-visual="1"><h2>{html.escape(title)}</h2>'
        '<div class="donut-layout">'
        f'<div class="donut" style="background:conic-gradient({",".join(segments)})"><span>{html.escape(_format_number(total))}</span></div>'
        f'<div class="legend">{"".join(legend)}</div>'
        "</div></section>"
    )


def _stacked_bar_chart(
    title: str,
    labels: list[str],
    series: list[tuple[str, list[float], str]],
    *,
    value_suffix: str = "",
    labels_are_products: bool = False,
) -> str:
    if not labels or not series:
        return ""
    rows = []
    for row_index, label in enumerate(labels[:12]):
        display_label = _product_display_label(label, header="product") if labels_are_products else label
        total = sum(values[row_index] for _name, values, _color in series if row_index < len(values))
        if total <= 0:
            continue
        segments = []
        for name, values, color in series:
            value = values[row_index] if row_index < len(values) else 0.0
            if value <= 0:
                continue
            width = max(2.0, value / total * 100.0)
            segments.append(
                f'<span class="stack-segment" title="{html.escape(name)}: {html.escape(_format_number(value, suffix=value_suffix))}" '
                f'style="width:{width:.2f}%;background:{html.escape(color)}"></span>'
            )
        product_attr = _product_data_attr(display_label, header="product") if labels_are_products else ""
        rows.append(
            f'<div class="stack-row"{product_attr}>'
            f"<span>{html.escape(display_label)}</span>"
            f'<div class="stack-track">{"".join(segments)}</div>'
            f"<b>{html.escape(_format_number(total, suffix=value_suffix))}</b>"
            "</div>"
        )
    legend = "".join(
        f'<span class="stack-legend"><i style="background:{html.escape(color)}"></i>{html.escape(name)}</span>'
        for name, _values, color in series
    )
    scope = ' data-product-visual="1"' if labels_are_products else ' data-global-visual="1"'
    return f'<section class="panel"{scope}><h2>{html.escape(title)}</h2><div class="stack-legend-wrap">{legend}</div>{"".join(rows)}</section>' if rows else ""


def _heatmap_table(
    title: str,
    headers: list[str],
    rows: list[list[Any]],
    *,
    row_column: str,
    column_column: str,
    value_column: str,
    max_columns: int = 8,
    value_suffix: str = "",
) -> str:
    index = _sheet_index(headers)
    if row_column not in index or column_column not in index or value_column not in index:
        return ""
    matrix: dict[str, dict[str, float]] = {}
    column_totals: dict[str, float] = {}
    row_totals: dict[str, float] = {}
    for row in rows:
        row_label = str(row[index[row_column]] if index[row_column] < len(row) and row[index[row_column]] not in (None, "") else "UNKNOWN")
        col_label = str(row[index[column_column]] if index[column_column] < len(row) and row[index[column_column]] not in (None, "") else "UNKNOWN")
        if row_column.lower() in {"product", "product_code", "sub-product", "sub_product_code"}:
            row_label = _product_display_label(row_label, header=row_column.lower())
        if column_column.lower() in {"product", "product_code", "sub-product", "sub_product_code"}:
            col_label = _product_display_label(col_label, header=column_column.lower())
        value = _number(row[index[value_column]] if index[value_column] < len(row) else None)
        matrix.setdefault(row_label, {})[col_label] = matrix.setdefault(row_label, {}).get(col_label, 0.0) + value
        column_totals[col_label] = column_totals.get(col_label, 0.0) + value
        row_totals[row_label] = row_totals.get(row_label, 0.0) + value
    row_labels = [label for label, _total in sorted(row_totals.items(), key=lambda item: item[1], reverse=True)[:10]]
    col_labels = [label for label, _total in sorted(column_totals.items(), key=lambda item: item[1], reverse=True)[:max_columns]]
    if not row_labels or not col_labels:
        return ""
    max_value = max((matrix.get(row_label, {}).get(col_label, 0.0) for row_label in row_labels for col_label in col_labels), default=0.0)
    if max_value <= 0:
        return ""
    header_html = "".join(f"<th>{html.escape(label)}</th>" for label in col_labels)
    body = []
    for row_label in row_labels:
        product_attr = _product_data_attr(row_label, header=row_column.lower()) if row_column.lower() in PRODUCT_LABEL_COLUMNS else ""
        cells = []
        for col_label in col_labels:
            value = matrix.get(row_label, {}).get(col_label, 0.0)
            alpha = 0.08 + (value / max_value * 0.54 if value > 0 else 0)
            cells.append(
                f'<td class="num heat" style="background:rgba(23,105,224,{alpha:.2f})">{html.escape(_format_number(value, suffix=value_suffix))}</td>'
            )
        body.append(f"<tr{product_attr}><th>{html.escape(row_label)}</th>{''.join(cells)}</tr>")
    scope = ' data-product-visual="1"' if row_column.lower() in PRODUCT_LABEL_COLUMNS else ' data-global-visual="1"'
    return (
        f'<section class="panel wide"{scope}><h2>{html.escape(title)}</h2>'
        f'<div class="table-wrap heatmap"><table><thead><tr><th>{html.escape(row_column)}</th>{header_html}</tr></thead>'
        f"<tbody>{''.join(body)}</tbody></table></div></section>"
    )


def _comparison_cards(title: str, metrics: list[tuple[str, float, float, str]]) -> str:
    cards = []
    for label, previous, current, suffix in metrics:
        delta = current - previous
        favorable_delta = delta <= 0 if "outstanding" in label.lower() else delta >= 0
        tone = "good" if favorable_delta else "watch"
        cards.append(
            f'<div class="comparison-card {tone}">'
            f"<span>{html.escape(label)}</span>"
            f"<strong>{html.escape(_format_number(current, suffix=suffix))}</strong>"
            f"<small>Apr: {html.escape(_format_number(previous, suffix=suffix))} | Change: {html.escape(_format_number(delta, suffix=suffix))}</small>"
            "</div>"
        )
    return f'<section class="panel wide" data-global-visual="1"><h2>{html.escape(title)}</h2><div class="comparison-grid">{"".join(cards)}</div></section>' if cards else ""


def _insights_panel(insights: list[tuple[str, str, str]]) -> str:
    if not insights:
        return ""
    cards = []
    for title, value, detail in insights[:4]:
        cards.append(
            '<div class="insight-card">'
            f"<span>{html.escape(title)}</span>"
            f"<strong>{html.escape(value)}</strong>"
            f"<small>{html.escape(detail)}</small>"
            "</div>"
        )
    return f'<section class="insights-card"><p class="eyebrow">Business Insights</p><h2>What to Watch</h2><div class="insight-grid">{"".join(cards)}</div></section>'


def _sheet_index(headers: list[str]) -> dict[str, int]:
    return {str(header): offset for offset, header in enumerate(headers)}


def _column_values(headers: list[str], rows: list[list[Any]], column_name: str) -> list[Any]:
    index = _sheet_index(headers)
    if column_name not in index:
        return []
    offset = index[column_name]
    return [row[offset] if offset < len(row) else None for row in rows]


def _sum_column(headers: list[str], rows: list[list[Any]], column_name: str) -> float:
    return sum(_number(value) for value in _column_values(headers, rows, column_name))


def _kpi_card(label: str, value: Any, *, tone: str = "neutral", suffix: str = "") -> str:
    return (
        f'<div class="kpi {html.escape(tone)}">'
        f"<span>{html.escape(label)}</span>"
        f"<strong>{html.escape(_format_number(value, suffix=suffix))}</strong>"
        "</div>"
    )


def _analyze_sheets(sheets: list[tuple[str, list[str], list[list[Any]]]]) -> list[str]:
    notes: list[str] = []
    for sheet_name, headers, rows in sheets:
        header_index = {str(header).lower(): offset for offset, header in enumerate(headers)}
        for column_index, header in enumerate(headers):
            numeric_values = []
            for row in rows:
                value = row[column_index] if column_index < len(row) else None
                if _is_number(value):
                    numeric_values.append(_number(value))
            if not numeric_values:
                continue
            gt_int64 = sum(1 for value in numeric_values if abs(value) > 9_223_372_036_854_775_807)
            negatives = sum(1 for value in numeric_values if value < 0)
            lowered = str(header).lower()
            if gt_int64:
                notes.append(f"{sheet_name}.{header}: {gt_int64} values exceed signed 64-bit integer range.")
            if negatives and any(token in lowered for token in ("available", "outstanding", "amount", "limit")):
                notes.append(f"{sheet_name}.{header}: {negatives} negative aggregate values detected; validate business definition before using as available capacity.")
            if lowered.endswith("rate"):
                outside = sum(1 for value in numeric_values if value < 0 or value > 1)
                if outside:
                    notes.append(f"{sheet_name}.{header}: {outside} rate values are outside 0-100%.")
    return notes


def _overview_cards(report_title: str, sheets: list[tuple[str, list[str], list[list[Any]]]]) -> str:
    summary = next(((headers, rows) for sheet, headers, rows in sheets if sheet in {"Summary by Product", "Funnel Summary by Product"}), None)
    if not summary:
        return ""
    headers, rows = summary
    cards: list[str] = []
    if "Applications" in headers:
        cards.extend(
            [
                _kpi_card("Applications", _sum_column(headers, rows, "Applications")),
                _kpi_card("Approved", _sum_column(headers, rows, "Approved"), tone="good"),
                _kpi_card("Rejected", _sum_column(headers, rows, "Rejected"), tone="watch"),
            ]
        )
    elif "applications" in headers:
        cards.extend(
            [
                _kpi_card("Applications", _sum_column(headers, rows, "applications")),
                _kpi_card("Approved Cases", _sum_column(headers, rows, "approved_cases"), tone="good"),
                _kpi_card("Disbursed Loans", _sum_column(headers, rows, "disbursed_loans"), tone="good"),
            ]
        )
    elif "due_amount" in headers:
        cards.extend(
            [
                _kpi_card("Due Amount", _sum_column(headers, rows, "due_amount")),
                _kpi_card("Repaid Amount", _sum_column(headers, rows, "repaid_amount"), tone="good"),
                _kpi_card("Outstanding", _sum_column(headers, rows, "outstanding_amount"), tone="watch"),
            ]
        )
    elif "total_limit" in headers:
        cards.extend(
            [
                _kpi_card("Customers", _sum_column(headers, rows, "customers")),
                _kpi_card("Total Limit", _sum_column(headers, rows, "total_limit")),
                _kpi_card("Used Limit", _sum_column(headers, rows, "used_limit"), tone="watch"),
            ]
        )
    if not cards:
        return ""
    return f'<section class="hero-card"><p class="eyebrow">Executive View</p><h2>{html.escape(report_title)}</h2><div class="kpi-grid">{"".join(cards)}</div></section>'


def _period_metric(headers: list[str], rows: list[list[Any]], period: str, metric: str) -> float:
    index = _sheet_index(headers)
    if "period" not in index or metric not in index:
        return 0.0
    return sum(_number(row[index[metric]] if index[metric] < len(row) else None) for row in rows if str(row[index["period"]] if index["period"] < len(row) else "") == period)


def _underwriting_sections(lookup: dict[str, tuple[list[str], list[list[Any]]]]) -> list[str]:
    sections: list[str] = []
    summary = lookup.get("Summary by Product")
    if summary:
        headers, rows = summary
        sections.append(
            _donut_chart(
                "Application Decision Mix",
                [
                    ("Approved", _sum_column(headers, rows, "Approved"), "#087443"),
                    ("Rejected", _sum_column(headers, rows, "Rejected"), "#b42318"),
                    ("Pending", _sum_column(headers, rows, "Pending"), "#b54708"),
                ],
            )
        )
        product_col = "Product" if "Product" in headers else "product"
        approved_col = "Approved" if "Approved" in headers else "approved"
        applications_col = "Applications" if "Applications" in headers else "applications"
        if product_col in headers and approved_col in headers and applications_col in headers:
            product_index = headers.index(product_col)
            approved_index = headers.index(approved_col)
            applications_index = headers.index(applications_col)
            aggregates: dict[str, list[float]] = {}
            for row in rows:
                product = _product_display_label(row[product_index] if product_index < len(row) else "UNKNOWN")
                bucket = aggregates.setdefault(product, [0.0, 0.0])
                bucket[0] += _number(row[approved_index] if approved_index < len(row) else None)
                bucket[1] += _number(row[applications_index] if applications_index < len(row) else None)
            pairs = [
                (product, approved / applications * 100.0)
                for product, (approved, applications) in aggregates.items()
                if applications > 0
            ]
            pairs = sorted(pairs, key=lambda item: item[1], reverse=True)[:12]
            sections.append(
                _bar_chart(
                    "Approval Rate by Product",
                    [item[0] for item in pairs],
                    [item[1] for item in pairs],
                    value_suffix="%",
                    labels_are_products=True,
                )
            )
    funnel = lookup.get("Product Funnel")
    if funnel:
        headers, rows = funnel
        index = _sheet_index(headers)
        if {"Product", "Status", "Count"}.issubset(index):
            products = [label for label, _total in _group_sum(headers, rows, "Product", "Count")[:10]]
            statuses = ["APPROVED", "REJECTED", "PENDING"]
            colors = {"APPROVED": "#087443", "REJECTED": "#b42318", "PENDING": "#b54708"}
            series = []
            for status in statuses:
                values = []
                for product in products:
                    values.append(
                        sum(
                            _number(row[index["Count"]] if index["Count"] < len(row) else None)
                            for row in rows
                            if _product_display_label(row[index["Product"]] if index["Product"] < len(row) else "") == product
                            and str(row[index["Status"]] if index["Status"] < len(row) else "") == status
                        )
                    )
                series.append((status.title(), values, colors[status]))
            sections.append(_stacked_bar_chart("Funnel Mix by Product", products, series, labels_are_products=True))
    rejects = lookup.get("Product Reject Reasons")
    if rejects:
        headers, rows = rejects
        top = _group_sum(headers, rows, "Reject Reason", "Count")[:12]
        sections.append(_bar_chart("Top Reject Reasons", [item[0] for item in top], [item[1] for item in top]))
    backlog = lookup.get("Product Stage Backlog")
    if backlog:
        headers, rows = backlog
        sections.append(
            _heatmap_table(
                "Backlog Heatmap by Product and Stage",
                headers,
                rows,
                row_column="Product",
                column_column="Current Stage",
                value_column="Count",
                max_columns=6,
            )
        )
    return [section for section in sections if section]


def _portfolio_sections(lookup: dict[str, tuple[list[str], list[list[Any]]]]) -> list[str]:
    sections: list[str] = []
    summary = lookup.get("Summary by Product")
    if summary:
        headers, rows = summary
        sections.append(
            _comparison_cards(
                "Apr 2026 vs May 2026 MTD",
                [
                    ("Due Amount", _period_metric(headers, rows, "Apr 2026", "due_amount"), _period_metric(headers, rows, "May 2026 MTD", "due_amount"), ""),
                    ("Repaid Amount", _period_metric(headers, rows, "Apr 2026", "repaid_amount"), _period_metric(headers, rows, "May 2026 MTD", "repaid_amount"), ""),
                    ("Outstanding", _period_metric(headers, rows, "Apr 2026", "outstanding_amount"), _period_metric(headers, rows, "May 2026 MTD", "outstanding_amount"), ""),
                ],
            )
        )
        index = _sheet_index(headers)
        if {"product", "repayment_rate"}.issubset(index):
            pairs = [
                (
                    str(row[index["period"]] if index["period"] < len(row) else ""),
                    _product_display_label(row[index["product"]] if index["product"] < len(row) else "UNKNOWN"),
                    _number(row[index["repayment_rate"]] if index["repayment_rate"] < len(row) else None),
                )
                for row in rows
            ]
            mtd_pairs = [(product, rate * 100 if rate <= 1 else rate) for period, product, rate in pairs if period == "May 2026 MTD"]
            mtd_pairs = sorted(mtd_pairs, key=lambda item: item[1], reverse=True)[:12]
            sections.append(
                _bar_chart(
                    "May MTD Repayment Rate by Product",
                    [item[0] for item in mtd_pairs],
                    [item[1] for item in mtd_pairs],
                    value_suffix="%",
                    labels_are_products=True,
                )
            )
    dpd = lookup.get("DPD Buckets")
    if dpd:
        headers, rows = dpd
        sections.append(
            _heatmap_table(
                "Outstanding Amount Heatmap by DPD Bucket",
                headers,
                rows,
                row_column="product",
                column_column="dpd_bucket",
                value_column="outstanding_amount",
                max_columns=8,
            )
        )
    flow = lookup.get("Repay Flow Status")
    if flow:
        headers, rows = flow
        top = _group_sum(headers, rows, "repay_status", "repay_amount")[:10]
        sections.append(_bar_chart("Repay Flow Amount by Status", [item[0] for item in top], [item[1] for item in top]))
    return [section for section in sections if section]


def _limit_sections(lookup: dict[str, tuple[list[str], list[list[Any]]]]) -> list[str]:
    sections: list[str] = []
    summary = lookup.get("Summary by Product")
    if summary:
        headers, rows = summary
        index = _sheet_index(headers)
        valid_rows = [row for row in rows if "total_limit" in index and _number(row[index["total_limit"]] if index["total_limit"] < len(row) else None) > 0]
        if valid_rows and {"product", "used_limit", "available_limit_estimate"}.issubset(index):
            labels = [_product_display_label(row[index["product"]] if index["product"] < len(row) else "UNKNOWN") for row in valid_rows[:10]]
            used = [_number(row[index["used_limit"]] if index["used_limit"] < len(row) else None) for row in valid_rows[:10]]
            available = [_number(row[index["available_limit_estimate"]] if index["available_limit_estimate"] < len(row) else None) for row in valid_rows[:10]]
            sections.append(
                _stacked_bar_chart(
                    "Used vs Available Limit by Product",
                    labels,
                    [("Used", used, "#b54708"), ("Available", available, "#087443")],
                    labels_are_products=True,
                )
            )
        if {"product", "utilization_rate"}.issubset(index):
            pairs = []
            for row in rows:
                rate = _number(row[index["utilization_rate"]] if index["utilization_rate"] < len(row) else None)
                if rate > 0:
                    pairs.append((_product_display_label(row[index["product"]] if index["product"] < len(row) else "UNKNOWN"), rate * 100 if rate <= 1 else rate))
            pairs = sorted(pairs, key=lambda item: item[1], reverse=True)[:12]
            sections.append(
                _bar_chart(
                    "Utilization Rate by Product",
                    [item[0] for item in pairs],
                    [item[1] for item in pairs],
                    value_suffix="%",
                    labels_are_products=True,
                )
            )
    buckets = lookup.get("Utilization Buckets")
    if buckets:
        headers, rows = buckets
        sections.append(
            _heatmap_table(
                "Customer Count Heatmap by Utilization Bucket",
                headers,
                rows,
                row_column="product",
                column_column="utilization_bucket",
                value_column="customers",
                max_columns=7,
            )
        )
    available = lookup.get("EOD Available Limit")
    if available:
        headers, rows = available
        top = _group_sum(headers, rows, "status", "available_limit")[:8]
        sections.append(_bar_chart("Available Limit by Status", [item[0] for item in top], [item[1] for item in top]))
    return [section for section in sections if section]


def _specialized_sections(report_title: str, lookup: dict[str, tuple[list[str], list[list[Any]]]]) -> list[str]:
    normalized = report_title.lower()
    if "underwriting" in normalized:
        return _underwriting_sections(lookup)
    if "portfolio repayment" in normalized:
        return _portfolio_sections(lookup)
    if "limit utilization" in normalized:
        return _limit_sections(lookup)
    return []


def _business_insights(report_title: str, lookup: dict[str, tuple[list[str], list[list[Any]]]]) -> str:
    normalized = report_title.lower()
    insights: list[tuple[str, str, str]] = []
    summary = lookup.get("Summary by Product")
    funnel_summary = lookup.get("Funnel Summary by Product")
    if "application to disbursement" in normalized and funnel_summary:
        headers, rows = funnel_summary
        index = _sheet_index(headers)
        if {"applications", "disbursed_loans", "disbursed_principal", "application_to_disbursement_rate", "product"}.issubset(index):
            applications = _sum_column(headers, rows, "applications")
            disbursed_loans = _sum_column(headers, rows, "disbursed_loans")
            disbursed_principal = _sum_column(headers, rows, "disbursed_principal")
            conversion_rate = disbursed_loans / applications * 100 if applications else 0
            product_rates = []
            product_dropoffs = []
            for row in rows:
                product = _product_display_label(row[index["product"]] if index["product"] < len(row) else "UNKNOWN")
                product_apps = _number(row[index["applications"]] if index["applications"] < len(row) else None)
                product_disbursed = _number(row[index["disbursed_loans"]] if index["disbursed_loans"] < len(row) else None)
                if product_apps > 0:
                    product_rates.append((product, product_disbursed / product_apps * 100))
                    product_dropoffs.append((product, product_apps - product_disbursed))
            lowest_conversion = min(product_rates, key=lambda item: item[1], default=("UNKNOWN", 0))
            largest_dropoff = max(product_dropoffs, key=lambda item: item[1], default=("UNKNOWN", 0))
            insights.append(("Application to disbursement", f"{conversion_rate:.1f}%", f"{_format_number(disbursed_loans)} disbursed loans from {_format_number(applications)} applications."))
            insights.append(("Disbursed principal", _format_number(disbursed_principal), "Total principal disbursed in the covered period."))
            insights.append(("Lowest conversion product", lowest_conversion[0], f"{lowest_conversion[1]:.1f}% application-to-disbursement rate."))
            insights.append(("Largest funnel drop-off", largest_dropoff[0], f"{_format_number(largest_dropoff[1])} applications did not become disbursed loans."))
    elif "underwriting" in normalized and summary:
        headers, rows = summary
        index = _sheet_index(headers)
        product_col = "Product" if "Product" in index else "product"
        applications_col = "Applications" if "Applications" in index else "applications"
        approved_col = "Approved" if "Approved" in index else "approved"
        rejected_col = "Rejected" if "Rejected" in index else "rejected"
        if {product_col, applications_col, approved_col, rejected_col}.issubset(index):
            total_apps = _sum_column(headers, rows, applications_col)
            total_approved = _sum_column(headers, rows, approved_col)
            approval_rate = total_approved / total_apps * 100 if total_apps else 0
            product_totals = []
            for row in rows:
                product_totals.append((
                    _product_display_label(row[index[product_col]] if index[product_col] < len(row) else "UNKNOWN"),
                    _number(row[index[applications_col]] if index[applications_col] < len(row) else None),
                    _number(row[index[approved_col]] if index[approved_col] < len(row) else None),
                ))
            top_product = max(product_totals, key=lambda item: item[1], default=("UNKNOWN", 0, 0))
            low_approval = min(
                ((product, approved / apps * 100) for product, apps, approved in product_totals if apps > 0),
                key=lambda item: item[1],
                default=("UNKNOWN", 0),
            )
            insights.append(("Overall approval", f"{approval_rate:.1f}%", f"{_format_number(total_approved)} approvals from {_format_number(total_apps)} applications."))
            insights.append(("Largest application source", top_product[0], f"{_format_number(top_product[1])} applications in the covered period."))
            insights.append(("Lowest approval product", low_approval[0], f"{low_approval[1]:.1f}% approval rate, worth checking policy/rejection mix."))
        rejects = lookup.get("Product Reject Reasons")
        if rejects:
            top = _group_sum(rejects[0], rejects[1], "Reject Reason", "Count")[:1]
            if top:
                insights.append(("Top reject driver", top[0][0], f"{_format_number(top[0][1])} rejects across products."))
    elif "portfolio repayment" in normalized and summary:
        headers, rows = summary
        due = _sum_column(headers, rows, "due_amount")
        repaid = _sum_column(headers, rows, "repaid_amount")
        outstanding = _sum_column(headers, rows, "outstanding_amount")
        rate = repaid / due * 100 if due else 0
        by_product = _group_sum(headers, rows, "product", "outstanding_amount")
        insights.append(("Portfolio repayment", f"{rate:.1f}%", f"{_format_number(repaid)} repaid against {_format_number(due)} due."))
        insights.append(("Outstanding exposure", _format_number(outstanding), "Use DPD heatmap below to locate aging concentration."))
        if by_product:
            insights.append(("Largest outstanding product", by_product[0][0], f"{_format_number(by_product[0][1])} outstanding amount."))
        dpd = lookup.get("DPD Buckets")
        if dpd:
            top = _group_sum(dpd[0], dpd[1], "dpd_bucket", "outstanding_amount")[:1]
            if top:
                insights.append(("Highest DPD exposure bucket", top[0][0], f"{_format_number(top[0][1])} outstanding amount."))
    elif "limit utilization" in normalized and summary:
        headers, rows = summary
        by_used = _group_sum(headers, rows, "product", "used_limit")
        index = _sheet_index(headers)
        inconsistent = 0
        valid_total_limit = 0.0
        valid_used_limit = 0.0
        if {"total_limit", "used_limit"}.issubset(index):
            for row in rows:
                row_total = _number(row[index["total_limit"]] if index["total_limit"] < len(row) else None)
                row_used = _number(row[index["used_limit"]] if index["used_limit"] < len(row) else None)
                if row_total == 0 and row_used != 0:
                    inconsistent += 1
                if row_total > 0:
                    valid_total_limit += row_total
                    valid_used_limit += row_used
        utilization = valid_used_limit / valid_total_limit * 100 if valid_total_limit else 0
        insights.append(("Booked utilization", f"{utilization:.1f}%", "Only products with positive total_limit are included."))
        if by_used:
            insights.append(("Largest used limit product", by_used[0][0], f"{_format_number(by_used[0][1])} used limit."))
        if inconsistent:
            insights.append(("Data definition caveat", f"{inconsistent} products", "total_limit is zero while used limit is non-zero; availability is undefined."))
        available = lookup.get("EOD Available Limit")
        if available:
            top = _group_sum(available[0], available[1], "status", "available_limit")[:1]
            if top:
                insights.append(("Largest available status", top[0][0], f"{_format_number(top[0][1])} available limit."))
    return _insights_panel(insights)


def _searchable_table_panel(
    title: str,
    headers: list[str],
    rows: list[list[Any]],
    *,
    placeholder: str,
) -> str:
    header_html = "".join(
        (
            f'<th><span class="th-label">{html.escape(str(header))}</span>'
            f'<input class="col-filter" type="text" data-col="{index}" '
            f'placeholder="Filter" aria-label="Filter {html.escape(str(header))}"></th>'
        )
        for index, header in enumerate(headers)
    )

    def _cell(value: Any) -> str:
        return "" if value is None else str(value)

    body_rows = []
    for row in rows:
        cells = "".join(
            f"<td>{html.escape(_cell(row[index] if index < len(row) else ''))}</td>"
            for index in range(len(headers))
        )
        body_rows.append(f"<tr>{cells}</tr>")
    table_html = (
        f'<div class="table-wrap"><table class="search-table"><thead><tr>{header_html}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody></table></div>'
        if headers
        else '<p class="empty">No rows were returned.</p>'
    )
    total = len(rows)
    return (
        f'<section class="panel"><h2>{html.escape(title)}</h2>'
        '<div class="search-bar">'
        f'<input type="search" data-search placeholder="{html.escape(placeholder)}" aria-label="{html.escape(placeholder)}">'
        f'<span class="count" data-count>{total} of {total} rows</span>'
        f"</div>{table_html}</section>"
    )


def _kpi_cards_panel(title: str, pairs: list[tuple[str, str]]) -> str:
    if not pairs:
        return ""
    cards = "".join(
        f'<div class="kpi"><span>{html.escape(label)}</span><strong>{html.escape(value)}</strong></div>'
        for label, value in pairs
    )
    return f'<section class="panel"><h2>{html.escape(title)}</h2><div class="kpi-grid">{cards}</div></section>'


def _expandable_rule_panel(
    title: str,
    placeholder: str,
    summary: tuple[list[str], list[list[Any]]],
    detail: tuple[list[str], list[list[Any]]],
    *,
    key_columns: tuple[str, ...],
    main_columns: tuple[str, ...],
    detail_columns: tuple[str, ...],
    name_column: str,
) -> str:
    """Render a per-rule table whose rows expand to a nested scene breakdown.

    `summary` is one row per rule; `detail` is one row per rule x scene. Rows are
    linked by `key_columns`. A single search box filters rules by their own text
    plus their child scene names.
    """
    s_head, s_rows = summary
    d_head, d_rows = detail
    si = {str(col): offset for offset, col in enumerate(s_head)}
    di = {str(col): offset for offset, col in enumerate(d_head)}

    def cell(value: Any) -> str:
        return "" if value is None else str(value)

    def sval(row: list[Any], col: str) -> str:
        return cell(row[si[col]]) if col in si and si[col] < len(row) else ""

    def dval(row: list[Any], col: str) -> str:
        return cell(row[di[col]]) if col in di and di[col] < len(row) else ""

    grouped: dict[tuple[str, ...], list[list[Any]]] = {}
    for row in d_rows:
        grouped.setdefault(tuple(dval(row, col) for col in key_columns), []).append(row)

    head_html = "<th></th>" + "".join(f"<th>{html.escape(col)}</th>" for col in main_columns)
    body: list[str] = []
    for srow in s_rows:
        key = tuple(sval(srow, col) for col in key_columns)
        key_attr = html.escape("|".join(key), quote=True)
        kids = grouped.get(key, [])
        scene_text = " ".join(dval(k, name_column) for k in kids)
        data_text = html.escape(" ".join([*key, scene_text]).lower(), quote=True)
        cells = "".join(f"<td>{html.escape(sval(srow, col))}</td>" for col in main_columns)
        expander = (
            '<button class="expander" type="button" aria-expanded="false" aria-label="Toggle scene breakdown">&#9654;</button>'
            if kids
            else ""
        )
        body.append(
            f'<tr class="rule-row" data-key="{key_attr}" data-text="{data_text}">'
            f'<td class="exp-cell">{expander}</td>{cells}</tr>'
        )
        if kids:
            d_head_html = "".join(f"<th>{html.escape(col)}</th>" for col in detail_columns)
            d_body = "".join(
                "<tr>" + "".join(f"<td>{html.escape(dval(k, col))}</td>" for col in detail_columns) + "</tr>"
                for k in kids
            )
            inner = f'<table class="detail-table"><thead><tr>{d_head_html}</tr></thead><tbody>{d_body}</tbody></table>'
            body.append(
                f'<tr class="detail-row" data-key="{key_attr}" hidden>'
                f'<td class="detail-cell" colspan="{len(main_columns) + 1}">{inner}</td></tr>'
            )
    total = len(s_rows)
    table_html = (
        f'<div class="table-wrap"><table class="rule-table"><thead><tr>{head_html}</tr></thead>'
        f'<tbody>{"".join(body)}</tbody></table></div>'
        if s_rows
        else '<p class="empty">No rows were returned.</p>'
    )
    return (
        f'<section class="panel rule-panel"><h2>{html.escape(title)}</h2>'
        '<div class="search-bar">'
        f'<input type="search" data-search placeholder="{html.escape(placeholder)}" aria-label="{html.escape(placeholder)}">'
        f'<span class="count" data-count>{total} of {total} rules</span>'
        f"</div>{table_html}</section>"
    )


def _searchable_tables_document(
    report_title: str,
    snapshot_pt_date: str,
    panels: list[str],
    *,
    intro_html: str = "",
) -> str:
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    body = (intro_html + "".join(panels)) or '<section class="panel"><p class="empty">No data was returned.</p></section>'
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(report_title)} Visualization</title>
<style>
:root{{--ink:#182230;--muted:#667085;--line:#d9e2ec;--bg:#f5f7fb;--blue:#1769e0;}}
*{{box-sizing:border-box;}}
body{{margin:0;background:var(--bg);color:var(--ink);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}}
header{{background:linear-gradient(135deg,#102a43,#173b5f);color:#fff;padding:30px 38px;}}
header h1{{margin:0 0 8px;font-size:28px;overflow-wrap:anywhere;}} header p{{margin:0;color:#dbeafe;}}
main{{padding:24px 34px 38px;}}
.panel{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:18px;box-shadow:0 1px 2px rgba(16,42,67,.06);}}
.panel + .panel{{margin-top:18px;}}
h2{{margin:0 0 14px;font-size:18px;}}
.search-bar{{display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap;}}
.search-bar input{{flex:1;min-width:240px;height:40px;border:1px solid #cbd5e1;border-radius:6px;padding:0 12px;font-size:14px;}}
.search-bar .count{{color:var(--muted);font-size:13px;white-space:nowrap;}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;}}
.kpi{{border:1px solid #e4e7ec;border-radius:8px;padding:14px;background:#fafcff;}}
.kpi span{{display:block;color:var(--muted);font-size:12px;margin-bottom:6px;}}
.kpi strong{{display:block;font-size:22px;}}
.table-wrap{{overflow:auto;border:1px solid #edf1f7;border-radius:6px;max-height:74vh;}}
table{{width:100%;border-collapse:collapse;font-size:13px;}}
th,td{{border-bottom:1px solid #edf1f7;padding:8px 10px;text-align:left;white-space:nowrap;vertical-align:top;}}
th{{background:#f1f6ff;font-weight:700;color:#344054;position:sticky;top:0;}}
th .th-label{{display:block;margin-bottom:6px;}}
th .col-filter{{display:block;width:100%;min-width:90px;font-weight:400;font-size:12px;padding:3px 6px;border:1px solid #cbd5e1;border-radius:4px;background:#fff;}}
.exp-cell{{width:34px;text-align:center;}}
.expander{{border:none;background:none;cursor:pointer;font-size:11px;color:var(--blue);padding:2px 4px;line-height:1;}}
td.detail-cell{{padding:0;background:#f8fbff;}}
.detail-table{{width:100%;border-collapse:collapse;}}
.detail-table th{{position:static;background:#eef4ff;font-size:12px;}}
.detail-table th,.detail-table td{{border-bottom:1px solid #e6eefb;}}
tr.no-match{{display:none;}}
.empty{{padding:18px;color:var(--muted);}}
</style></head><body>
<header><h1>{html.escape(report_title)}</h1><p>Snapshot {html.escape(snapshot_pt_date)}. Generated {generated_at} UTC from Data Workbench output.</p></header>
<main>{body}</main>
<script>
(() => {{
  document.querySelectorAll(".panel").forEach((panel) => {{
    if (!panel.querySelector("table.search-table")) return;
    const input = panel.querySelector("[data-search]");
    const counter = panel.querySelector("[data-count]");
    const colFilters = Array.from(panel.querySelectorAll(".col-filter"));
    const rows = Array.from(panel.querySelectorAll("table.search-table tbody tr"));
    const total = rows.length;
    rows.forEach((row) => {{
      row.dataset.text = row.textContent.toLowerCase();
      row._cells = Array.from(row.cells).map((cell) => cell.textContent.toLowerCase());
    }});
    const apply = () => {{
      const query = (input?.value || "").trim().toLowerCase();
      const active = colFilters
        .map((f) => [Number(f.dataset.col), (f.value || "").trim().toLowerCase()])
        .filter((pair) => pair[1]);
      let visible = 0;
      rows.forEach((row) => {{
        let match = !query || row.dataset.text.includes(query);
        if (match) {{
          for (const [col, val] of active) {{
            if (!(row._cells[col] || "").includes(val)) {{ match = false; break; }}
          }}
        }}
        row.classList.toggle("no-match", !match);
        if (match) visible += 1;
      }});
      if (counter) counter.textContent = `${{visible}} of ${{total}} rows`;
    }};
    input?.addEventListener("input", apply);
    colFilters.forEach((f) => f.addEventListener("input", apply));
    apply();
  }});
  document.querySelectorAll(".rule-panel").forEach((panel) => {{
    const table = panel.querySelector("table.rule-table");
    if (!table) return;
    const input = panel.querySelector("[data-search]");
    const counter = panel.querySelector("[data-count]");
    const mainRows = Array.from(table.querySelectorAll("tr.rule-row"));
    const details = {{}};
    table.querySelectorAll("tr.detail-row").forEach((d) => {{ details[d.dataset.key] = d; }});
    const total = mainRows.length;
    table.querySelectorAll(".expander").forEach((btn) => {{
      btn.addEventListener("click", () => {{
        const tr = btn.closest("tr");
        const d = details[tr.dataset.key];
        const open = btn.getAttribute("aria-expanded") === "true";
        btn.setAttribute("aria-expanded", String(!open));
        btn.innerHTML = open ? "&#9654;" : "&#9660;";
        if (d) d.hidden = open;
      }});
    }});
    const apply = () => {{
      const query = (input?.value || "").trim().toLowerCase();
      let visible = 0;
      mainRows.forEach((tr) => {{
        const match = !query || (tr.dataset.text || "").includes(query);
        tr.hidden = !match;
        const d = details[tr.dataset.key];
        if (d && !match) {{
          d.hidden = true;
          const btn = tr.querySelector(".expander");
          if (btn) {{ btn.setAttribute("aria-expanded", "false"); btn.innerHTML = "&#9654;"; }}
        }}
        if (match) visible += 1;
      }});
      if (counter) counter.textContent = `${{visible}} of ${{total}} rules`;
    }};
    input?.addEventListener("input", apply);
    apply();
  }});
}})();
</script>
</body></html>"""


def _scenario_auth_flow_document(
    report_title: str,
    snapshot_pt_date: str,
    sheets: list[tuple[str, list[str], list[list[Any]]]],
) -> str:
    lookup = {sheet_name: (headers, rows) for sheet_name, headers, rows in sheets}
    flow = lookup.get("Scenario Action Auth Flow")
    if flow is not None:
        headers, rows = flow
    elif sheets:
        _name, headers, rows = sheets[-1]
    else:
        headers, rows = [], []
    step_offsets = {
        index for index, header in enumerate(headers) if str(header).strip().lower().endswith("_step")
    }
    header_html = "".join(
        f'<th{" class=\"step\"" if index in step_offsets else ""}>{html.escape(str(header))}</th>'
        for index, header in enumerate(headers)
    )

    def _cell(value: Any) -> str:
        return "" if value is None else str(value)

    body_rows = []
    for row in rows:
        cells = "".join(
            f'<td{" class=\"step\"" if index in step_offsets else ""}>'
            f"{html.escape(_cell(row[index] if index < len(row) else ''))}</td>"
            for index in range(len(headers))
        )
        body_rows.append(f"<tr>{cells}</tr>")
    table_html = (
        f'<div class="table-wrap"><table data-flow-table><thead><tr>{header_html}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody></table></div>'
        if headers
        else '<p class="empty">No scenario flow rows were returned.</p>'
    )
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    row_count = len(rows)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(report_title)} Visualization</title>
<style>
:root{{--ink:#182230;--muted:#667085;--line:#d9e2ec;--bg:#f5f7fb;--blue:#1769e0;}}
*{{box-sizing:border-box;}}
body{{margin:0;background:var(--bg);color:var(--ink);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}}
header{{background:linear-gradient(135deg,#102a43,#173b5f);color:#fff;padding:30px 38px;}}
header h1{{margin:0 0 8px;font-size:28px;overflow-wrap:anywhere;}} header p{{margin:0;color:#dbeafe;}}
main{{padding:24px 34px 38px;}}
.panel{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:18px;box-shadow:0 1px 2px rgba(16,42,67,.06);}}
h2{{margin:0 0 14px;font-size:18px;}}
.search-bar{{display:flex;align-items:center;gap:12px;margin-bottom:14px;flex-wrap:wrap;}}
.search-bar input{{flex:1;min-width:240px;height:40px;border:1px solid #cbd5e1;border-radius:6px;padding:0 12px;font-size:14px;}}
.search-bar .count{{color:var(--muted);font-size:13px;white-space:nowrap;}}
.table-wrap{{overflow:auto;border:1px solid #edf1f7;border-radius:6px;max-height:74vh;}}
table{{width:100%;border-collapse:collapse;font-size:13px;}}
th,td{{border-bottom:1px solid #edf1f7;padding:8px 10px;text-align:left;white-space:nowrap;vertical-align:top;}}
th{{background:#f1f6ff;font-weight:700;color:#344054;position:sticky;top:0;}}
th.step,td.step{{width:50ch;min-width:50ch;max-width:50ch;white-space:normal;overflow-wrap:anywhere;word-break:break-word;}}
tr.no-match{{display:none;}}
.empty{{padding:18px;color:var(--muted);}}
</style></head><body>
<header><h1>{html.escape(report_title)}</h1><p>Snapshot {html.escape(snapshot_pt_date)}. Generated {generated_at} UTC from Data Workbench output.</p></header>
<main><section class="panel">
<h2>Scenario Action Auth Flow</h2>
<div class="search-bar">
<input type="search" data-search placeholder="Search scene name or step…" aria-label="Search scene name or step">
<span class="count" data-count>{row_count} of {row_count} rows</span>
</div>
{table_html}
</section></main>
<script>
(() => {{
  const input = document.querySelector("[data-search]");
  const counter = document.querySelector("[data-count]");
  const rows = Array.from(document.querySelectorAll("[data-flow-table] tbody tr"));
  const total = rows.length;
  rows.forEach((row) => {{ row.dataset.text = row.textContent.toLowerCase(); }});
  const apply = () => {{
    const query = (input?.value || "").trim().toLowerCase();
    let visible = 0;
    rows.forEach((row) => {{
      const match = !query || row.dataset.text.includes(query);
      row.classList.toggle("no-match", !match);
      if (match) visible += 1;
    }});
    if (counter) counter.textContent = `${{visible}} of ${{total}} rows`;
  }};
  input?.addEventListener("input", apply);
  apply();
}})();
</script>
</body></html>"""


def write_visualization(
    path: Path,
    *,
    report_title: str,
    snapshot_pt_date: str,
    sheets: list[tuple[str, list[str], list[list[Any]]]],
    report_id: str = "",
) -> None:
    if report_id == AF_SCENARIOS_ACTIONS_REPORT_ID:
        path.write_text(
            _scenario_auth_flow_document(report_title, snapshot_pt_date, sheets),
            encoding="utf-8",
        )
        return
    if report_id == AF_RULES_FEATURES_REPORT_ID:
        flow_lookup = {sheet_name: (headers, rows) for sheet_name, headers, rows in sheets}
        panels: list[str] = []
        rules = flow_lookup.get("Rules")
        if rules:
            panels.append(
                _searchable_table_panel("Rules", rules[0], rules[1], placeholder="Search rule id or name…")
            )
        features = flow_lookup.get("Features")
        if features:
            panels.append(
                _searchable_table_panel("Features", features[0], features[1], placeholder="Search feature id or name…")
            )
        path.write_text(
            _searchable_tables_document(report_title, snapshot_pt_date, panels),
            encoding="utf-8",
        )
        return
    if report_id == AF_RULE_EFFECTIVENESS_REPORT_ID:
        eff_lookup = {sheet_name: (headers, rows) for sheet_name, headers, rows in sheets}
        # KPI summary cards from the single-row Request Outcome Summary sheet.
        kpis: list[tuple[str, str]] = []
        scope_label = ""
        summary = eff_lookup.get("Request Outcome Summary")
        if summary and summary[1]:
            cols = {str(col): offset for offset, col in enumerate(summary[0])}
            srow = summary[1][0]

            def _v(col: str) -> Any:
                return srow[cols[col]] if col in cols and cols[col] < len(srow) else None

            scope_label = str(_v("period") or "").strip()

            kpi_specs = [
                ("Total outcomes", "total_outcomes", ""),
                ("Pass", "pass_num", ""),
                ("Challenge", "challenge_num", ""),
                ("Reject", "reject_num", ""),
                ("Action rate", "action_rate_pct", "%"),
            ]
            for label, col, suffix in kpi_specs:
                value = _v(col)
                if value in (None, ""):
                    continue
                text = f"{_format_number(value)}{suffix}" if _is_number(value) else f"{value}{suffix}"
                kpis.append((label, text))
        intro = _kpi_cards_panel(f"Hit-Rate Summary — {scope_label}" if scope_label else "Hit-Rate Summary", kpis)
        placeholders = {
            "Request Outcome Summary": "Filter…",
            "Daily Challenge/Reject/Punish": "Search date…",
        }
        breakdown = eff_lookup.get("Reject Rule Scene Breakdown")
        panels = []
        for sheet_name, headers, rows in sheets:
            if sheet_name == "Reject Rule Scene Breakdown":
                # Consumed into the expandable Reject Rule Hit Summary panel.
                continue
            if sheet_name == "Reject Rule Hit Summary" and breakdown:
                panels.append(
                    _expandable_rule_panel(
                        "Reject Rule Hit Summary",
                        "Search rule, type or scene…",
                        (headers, rows),
                        breakdown,
                        key_columns=("reject_rule", "reject_type"),
                        main_columns=(
                            "reject_rule",
                            "reject_type",
                            "reject_count",
                            "distinct_users",
                            "distinct_scenes",
                            "rejected_amount_php",
                        ),
                        detail_columns=("scene_name", "reject_count", "distinct_users", "rejected_amount_php"),
                        name_column="scene_name",
                    )
                )
                continue
            panels.append(
                _searchable_table_panel(
                    sheet_name,
                    headers,
                    rows,
                    placeholder=placeholders.get(sheet_name, "Search…"),
                )
            )
        path.write_text(
            _searchable_tables_document(report_title, snapshot_pt_date, panels, intro_html=intro),
            encoding="utf-8",
        )
        return
    lookup = {sheet_name: (headers, rows) for sheet_name, headers, rows in sheets}
    sections: list[str] = []
    product_options = _product_filter_options(sheets)
    product_filter = _product_filter_html(product_options)
    if product_filter:
        sections.append(product_filter)
    sections.append(_overview_cards(report_title, sheets))
    insight_panel = _business_insights(report_title, lookup)
    if insight_panel:
        sections.append(insight_panel)
    quality_notes = _analyze_sheets(sheets)
    if quality_notes:
        notes = "".join(f"<li>{html.escape(note)}</li>" for note in quality_notes[:10])
        sections.append(f'<section class="quality-card"><h2>Data Quality Notes</h2><ul>{notes}</ul></section>')
    else:
        sections.append('<section class="quality-card good"><h2>Data Quality Notes</h2><p>No signed 64-bit overflow, negative capacity, or rate-bound anomalies detected in aggregate sheets.</p></section>')
    specialized = _specialized_sections(report_title, lookup)
    sections.extend(specialized)
    summary = lookup.get("Summary by Product") or lookup.get("Funnel Summary by Product")
    if summary:
        headers, rows = summary
        index = {header: offset for offset, header in enumerate(headers)}
        product_index = index.get("product", 1 if len(headers) > 1 else 0)
        if not specialized:
            metric = next(
                (candidate for candidate in ("repayment_rate", "utilization_rate", "application_to_disbursement_rate") if candidate in index),
                None,
            )
            if metric:
                multiplier = 100 if rows and _number(rows[0][index[metric]]) <= 1 else 1
                sections.append(
                    _bar_chart(
                        f"{metric.replace('_', ' ').title()} by Product",
                        [row[product_index] for row in rows],
                        [_number(row[index[metric]]) * multiplier for row in rows],
                        value_suffix="%",
                        labels_are_products=True,
                    )
                )
            amount_metric = next(
                (candidate for candidate in ("outstanding_amount", "used_limit", "disbursed_principal", "applications") if candidate in index),
                None,
            )
            if amount_metric:
                sections.append(
                    _bar_chart(
                        f"{amount_metric.replace('_', ' ').title()} by Product",
                        [row[product_index] for row in rows],
                        [row[index[amount_metric]] for row in rows],
                        labels_are_products=True,
                    )
                )
        sections.append(f'<section class="panel wide"><h2>Summary by Product</h2>{_table_html(headers, rows)}</section>')
    for sheet_name, headers, rows in sheets:
        if sheet_name == "Summary by Product":
            continue
        sections.append(f'<section class="panel wide"><h2>{html.escape(sheet_name)}</h2>{_table_html(headers, rows)}</section>')
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(report_title)} Visualization</title>
<style>
:root{{--ink:#182230;--muted:#667085;--line:#d9e2ec;--bg:#f5f7fb;--blue:#1769e0;--green:#087443;--amber:#b54708;--red:#b42318;}}
*{{box-sizing:border-box;}}
body{{margin:0;background:var(--bg);color:var(--ink);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}}
header{{background:linear-gradient(135deg,#102a43,#173b5f);color:#fff;padding:30px 38px;}}
header h1{{margin:0 0 8px;font-size:30px;letter-spacing:0;overflow-wrap:anywhere;word-break:break-word;}} header p{{margin:0;color:#dbeafe;overflow-wrap:anywhere;}}
main{{padding:24px 34px 38px;display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:18px;}}
.hero-card,.quality-card,.insights-card,.filter-card,.panel{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:18px;box-shadow:0 1px 2px rgba(16,42,67,.06);}}
.hero-card,.quality-card,.insights-card,.filter-card,.panel,main>*{{min-width:0;}}
.hero-card,.quality-card,.insights-card,.filter-card,.wide{{grid-column:1/-1;}} .panel{{grid-column:span 6;}}
.eyebrow{{margin:0 0 6px;color:var(--blue);font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;}}
h2{{margin:0 0 14px;font-size:18px;letter-spacing:0;overflow-wrap:anywhere;word-break:break-word;}}
.hero-card h2{{font-size:22px;margin-bottom:16px;}}
.kpi-grid{{display:grid;grid-template-columns:repeat(3,minmax(160px,1fr));gap:12px;}}
.kpi{{border:1px solid #e4e7ec;border-radius:8px;padding:14px;background:#fafcff;}}
.kpi span{{display:block;color:var(--muted);font-size:12px;margin-bottom:6px;}} .kpi strong{{display:block;font-size:24px;}}
.kpi.good strong{{color:var(--green);}} .kpi.watch strong{{color:var(--amber);}}
.filter-card{{display:flex;align-items:flex-end;justify-content:space-between;gap:18px;}}
.filter-card h2{{margin-bottom:8px;}} .filter-card p{{margin:0;color:#475467;line-height:1.35;}}
.filter-card label{{display:grid;gap:6px;min-width:220px;color:#475467;font-size:12px;font-weight:700;}}
.filter-card select{{height:40px;border:1px solid #cbd5e1;border-radius:6px;background:#fff;color:var(--ink);font-size:14px;padding:0 12px;}}
.insights-card{{border-left:4px solid var(--blue);background:linear-gradient(180deg,#fff,#f8fbff);}}
.insight-grid{{display:grid;grid-template-columns:repeat(4,minmax(160px,1fr));gap:12px;}}
.insight-card{{border:1px solid #dbeafe;border-radius:8px;padding:14px;background:#fff;}}
.insight-card span{{display:block;color:var(--muted);font-size:12px;margin-bottom:6px;}} .insight-card strong{{display:block;font-size:21px;line-height:1.2;overflow-wrap:anywhere;}}
.insight-card small{{display:block;margin-top:8px;color:#475467;line-height:1.35;overflow-wrap:anywhere;}}
.quality-card{{border-left:4px solid var(--amber);}} .quality-card.good{{border-left-color:var(--green);}}
.quality-card ul{{margin:0;padding-left:18px;color:#344054;}} .quality-card li{{margin:6px 0;}}
.bar-row{{display:grid;grid-template-columns:minmax(120px,220px) 1fr minmax(90px,auto);gap:12px;align-items:center;margin:10px 0;}}
.bar-row span{{color:#344054;font-weight:600;min-width:0;overflow-wrap:anywhere;}} .bar-row b{{text-align:right;font-variant-numeric:tabular-nums;}}
.bar-track{{height:18px;background:#e5e7eb;border-radius:4px;overflow:hidden;}} .bar{{height:100%;background:linear-gradient(90deg,#1769e0,#39a0ff);}}
.donut-layout{{display:grid;grid-template-columns:180px 1fr;gap:20px;align-items:center;}}
.donut{{width:156px;height:156px;border-radius:50%;display:grid;place-items:center;position:relative;box-shadow:inset 0 0 0 1px #e4e7ec;}}
.donut:after{{content:"";position:absolute;width:92px;height:92px;border-radius:50%;background:#fff;box-shadow:0 0 0 1px #edf1f7;}}
.donut span{{position:relative;z-index:1;font-weight:800;font-size:18px;}}
.legend-row{{display:grid;grid-template-columns:14px 1fr auto;gap:8px;align-items:center;margin:8px 0;}}
.legend-row b,.comparison-card strong{{font-variant-numeric:tabular-nums;}} .legend-dot{{width:10px;height:10px;border-radius:50%;display:inline-block;}}
.stack-legend-wrap{{display:flex;gap:14px;flex-wrap:wrap;margin:0 0 12px;color:#475467;font-size:12px;}}
.stack-legend i{{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;vertical-align:-1px;}}
.stack-row{{display:grid;grid-template-columns:minmax(90px,160px) 1fr minmax(90px,auto);gap:12px;align-items:center;margin:10px 0;}}
.stack-row>span{{font-weight:600;color:#344054;min-width:0;overflow-wrap:anywhere;}} .stack-row>b{{text-align:right;font-variant-numeric:tabular-nums;}}
.stack-track{{height:20px;background:#e5e7eb;border-radius:4px;overflow:hidden;display:flex;}} .stack-segment{{display:block;height:100%;min-width:2px;}}
.comparison-grid{{display:grid;grid-template-columns:repeat(3,minmax(180px,1fr));gap:12px;}}
.comparison-card{{border:1px solid #e4e7ec;border-radius:8px;padding:14px;background:#fbfdff;}}
.comparison-card span{{display:block;color:var(--muted);font-size:12px;margin-bottom:6px;}} .comparison-card strong{{display:block;font-size:22px;}}
.comparison-card small{{display:block;margin-top:7px;color:#475467;}} .comparison-card.good strong{{color:var(--green);}} .comparison-card.watch strong{{color:var(--amber);}}
.table-wrap{{overflow:auto;border:1px solid #edf1f7;border-radius:6px;}} table{{width:100%;border-collapse:collapse;font-size:13px;}} th,td{{border-bottom:1px solid #edf1f7;padding:8px 10px;text-align:left;white-space:nowrap;}} th{{background:#f1f6ff;font-weight:700;color:#344054;position:sticky;top:0;}} td.num{{text-align:right;font-variant-numeric:tabular-nums;}}
.table-pagination{{display:flex;align-items:center;justify-content:flex-end;gap:10px;margin-top:10px;color:#475467;font-size:12px;}}
.table-pagination button{{height:32px;border:1px solid #cbd5e1;border-radius:6px;background:#fff;color:#344054;padding:0 10px;font-weight:650;cursor:pointer;}}
.table-pagination button:disabled{{color:#98a2b3;background:#f8fafc;cursor:not-allowed;}}
.heatmap td.heat{{color:#12263f;font-weight:650;}}
.note{{color:var(--muted);font-size:12px;margin:10px 0 0;}}
@media(max-width:900px){{body{{overflow-x:hidden;}}header{{padding:28px 18px;}}header h1{{font-size:28px;}}main{{grid-template-columns:1fr;padding-left:16px;padding-right:16px;}}.panel{{grid-column:1/-1;}}.filter-card{{display:grid;align-items:stretch;}}.filter-card label{{min-width:0;}}.kpi-grid,.comparison-grid,.insight-grid{{grid-template-columns:1fr;}}.bar-row,.stack-row,.donut-layout{{grid-template-columns:1fr;gap:6px;}}.bar-row b,.stack-row>b{{text-align:left;}}}}
</style></head><body><header><h1>{html.escape(report_title)}</h1><p>Snapshot {html.escape(snapshot_pt_date)}. Generated {generated_at} UTC from Data Workbench aggregate output.</p></header><main>{"".join(sections)}</main><script>
(() => {{
  const filter = document.querySelector("[data-product-filter]");
  const tables = Array.from(document.querySelectorAll("table.bi-table"));
  const productMatches = (node, selected) => !selected || !node.hasAttribute("data-product") || node.getAttribute("data-product") === selected;
  const updateTables = (selected) => {{
    tables.forEach((table) => {{
      const pageSize = Number(table.getAttribute("data-page-size") || "50");
      const rows = Array.from(table.tBodies[0]?.rows || []);
      const visibleRows = rows.filter((row) => productMatches(row, selected));
      const pages = Math.max(1, Math.ceil(visibleRows.length / pageSize));
      const currentPage = Math.min(Number(table.dataset.page || "1"), pages);
      table.dataset.page = String(currentPage);
      const start = (currentPage - 1) * pageSize;
      const end = start + pageSize;
      rows.forEach((row) => {{
        const filtered = !productMatches(row, selected);
        const pageIndex = visibleRows.indexOf(row);
        row.hidden = filtered || pageIndex < start || pageIndex >= end;
      }});
      const panel = table.closest(".panel");
      const controls = panel?.querySelector("[data-table-pagination]");
      if (!controls) return;
      const info = controls.querySelector("[data-page-info]");
      const previous = controls.querySelector("[data-page-prev]");
      const next = controls.querySelector("[data-page-next]");
      const first = visibleRows.length ? start + 1 : 0;
      const last = Math.min(end, visibleRows.length);
      if (info) info.textContent = `${{first}}-${{last}} of ${{visibleRows.length}}`;
      if (previous) previous.disabled = currentPage <= 1;
      if (next) next.disabled = currentPage >= pages;
    }});
  }};
  document.querySelectorAll("[data-table-pagination]").forEach((controls) => {{
    const table = controls.closest(".panel")?.querySelector("table.bi-table");
    if (!table) return;
    controls.querySelector("[data-page-prev]")?.addEventListener("click", () => {{
      table.dataset.page = String(Math.max(1, Number(table.dataset.page || "1") - 1));
      apply();
    }});
    controls.querySelector("[data-page-next]")?.addEventListener("click", () => {{
      table.dataset.page = String(Number(table.dataset.page || "1") + 1);
      apply();
    }});
  }});
  const apply = () => {{
    const selected = filter?.value || "";
    document.querySelectorAll("[data-product]").forEach((node) => {{
      if (node.closest("table.bi-table")) return;
      node.hidden = !productMatches(node, selected);
    }});
    document.querySelectorAll("[data-global-visual]").forEach((node) => {{
      node.hidden = Boolean(selected);
    }});
    document.querySelectorAll("[data-product-visual]").forEach((panel) => {{
      const productNodes = Array.from(panel.querySelectorAll("[data-product]"));
      panel.hidden = Boolean(selected) && !productNodes.some((node) => productMatches(node, selected));
    }});
    updateTables(selected);
  }};
  filter?.addEventListener("change", () => {{
    tables.forEach((table) => {{ table.dataset.page = "1"; }});
    apply();
  }});
  apply();
}})();
</script></body></html>"""
    path.write_text(document, encoding="utf-8")


def reports_metadata_path(portal_data_dir: Path) -> Path:
    return portal_data_dir / "business_insights" / "reports.json"


def artifacts_dir(portal_data_dir: Path) -> Path:
    return portal_data_dir / "business_insights" / "artifacts"


def load_metadata(portal_data_dir: Path) -> dict[str, Any]:
    path = reports_metadata_path(portal_data_dir)
    if not path.exists():
        return {"artifacts": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"artifacts": {}}
    return payload if isinstance(payload, dict) else {"artifacts": {}}


def persist_metadata(portal_data_dir: Path, payload: dict[str, Any]) -> None:
    path = reports_metadata_path(portal_data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    temp_path.replace(path)


def report_has_artifacts(portal_data_dir: Path, report_id: str) -> bool:
    payload = load_metadata(portal_data_dir)
    artifact = (payload.get("artifacts") or {}).get(report_id)
    if not isinstance(artifact, dict):
        return False
    root = artifacts_dir(portal_data_dir)
    return bool(
        artifact.get("filename")
        and artifact.get("visualization_filename")
        and (root / str(artifact["filename"])).exists()
        and (root / str(artifact["visualization_filename"])).exists()
    )


def sheets_from_workbook(
    path: Path,
    *,
    include_raw_export: bool = False,
    normalize_products: bool = True,
) -> list[tuple[str, list[str], list[list[Any]]]]:
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        sheets: list[tuple[str, list[str], list[list[Any]]]] = []
        for sheet in workbook.worksheets:
            raw_rows = list(sheet.iter_rows(values_only=True))
            if not raw_rows:
                continue
            headers = [str(value or "") for value in raw_rows[0]]
            rows = [list(row) for row in raw_rows[1:] if any(value not in (None, "") for value in row)]
            if sheet.title == "Raw Export" and not include_raw_export:
                continue
            sheets.append((sheet.title, headers, rows))
        return normalize_product_labels(sheets) if normalize_products else sheets
    finally:
        workbook.close()


def refresh_existing_visualizations(portal_data_dir: Path, *, report_ids: list[str] | None = None) -> None:
    payload = load_metadata(portal_data_dir)
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    selected = set(report_ids or artifacts.keys())
    root = artifacts_dir(portal_data_dir)
    title_by_report = {
        UNDERWRITING_FUNNEL_REPORT_ID: "Credit Risk PH - Underwriting Funnel",
        **{report_id: title for report_id, (title, _builder) in REPORT_BUILDERS.items()},
    }
    for report_id, artifact in artifacts.items():
        if report_id not in selected or not isinstance(artifact, dict):
            continue
        filename = str(artifact.get("filename") or "")
        if not filename:
            continue
        workbook_path = root / filename
        if not workbook_path.exists():
            print(f"{report_id}: workbook missing; skipping visualization refresh.", flush=True)
            continue
        visualization_filename = str(artifact.get("visualization_filename") or filename.replace(".xlsx", ".html"))
        write_visualization(
            root / visualization_filename,
            report_title=title_by_report.get(report_id, report_id),
            snapshot_pt_date=str(artifact.get("snapshot_pt_date") or "2026-05-25"),
            sheets=sheets_from_workbook(workbook_path, normalize_products=False),
            report_id=report_id,
        )
        artifact["visualization_filename"] = visualization_filename
        print(f"{report_id}: refreshed visualization={visualization_filename}", flush=True)
    payload["artifacts"] = artifacts
    persist_metadata(portal_data_dir, payload)


def normalize_existing_product_labels(portal_data_dir: Path, *, report_ids: list[str] | None = None) -> None:
    payload = load_metadata(portal_data_dir)
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    selected = set(report_ids or artifacts.keys())
    root = artifacts_dir(portal_data_dir)
    title_by_report = {
        UNDERWRITING_FUNNEL_REPORT_ID: "Credit Risk PH - Underwriting Funnel",
        **{report_id: title for report_id, (title, _builder) in REPORT_BUILDERS.items()},
    }
    for report_id, artifact in artifacts.items():
        if report_id not in selected or not isinstance(artifact, dict):
            continue
        filename = str(artifact.get("filename") or "")
        if not filename:
            continue
        workbook_path = root / filename
        if not workbook_path.exists():
            print(f"{report_id}: workbook missing; skipping product label normalization.", flush=True)
            continue
        sheets = sheets_from_workbook(workbook_path, include_raw_export=True, normalize_products=False)
        write_workbook(workbook_path, normalize_product_labels(sheets))
        visualization_filename = str(artifact.get("visualization_filename") or filename.replace(".xlsx", ".html"))
        write_visualization(
            root / visualization_filename,
            report_title=title_by_report.get(report_id, report_id),
            snapshot_pt_date=str(artifact.get("snapshot_pt_date") or "2026-05-25"),
            sheets=[sheet for sheet in sheets if sheet[0] != "Raw Export"],
            report_id=report_id,
        )
        artifact["visualization_filename"] = visualization_filename
        print(f"{report_id}: normalized product labels in excel={filename} visualization={visualization_filename}", flush=True)
    payload["artifacts"] = artifacts
    persist_metadata(portal_data_dir, payload)


def update_report_artifact(
    portal_data_dir: Path,
    *,
    report_id: str,
    metadata: dict[str, Any],
) -> None:
    payload = load_metadata(portal_data_dir)
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    artifacts[report_id] = metadata
    payload["artifacts"] = artifacts
    persist_metadata(portal_data_dir, payload)


def generate_report(
    *,
    session: requests.Session,
    portal_data_dir: Path,
    report_id: str,
    snapshot_pt_date: str,
    now: datetime,
    skip_existing: bool,
    poll_seconds: int,
    max_polls: int,
) -> None:
    if skip_existing and report_has_artifacts(portal_data_dir, report_id):
        print(f"{report_id}: existing Excel and visualization found; skipping.", flush=True)
        return
    report_config = REPORT_BUILDERS.get(report_id)
    if report_config is None:
        raise RuntimeError(f"Unsupported report id: {report_id}")
    title, builder = report_config
    sql = builder(snapshot_pt_date=snapshot_pt_date, now=now)
    display_snapshot = snapshot_pt_date or "latest available pt_date at run time"
    sections = extract_sql_sections(sql)
    if not sections:
        raise RuntimeError(f"{report_id}: no SQL sections found.")

    print(f"{report_id}: generating {len(sections)} sheets", flush=True)
    sheets: list[tuple[str, list[str], list[list[Any]]]] = []
    executions = []
    for section in sections:
        schema, rows, execution_id = run_workbench_query(session, section, poll_seconds=poll_seconds, max_polls=max_polls)
        sheets.append((section.sheet_name, schema, rows))
        executions.append({"sheet": section.sheet_name, "execution_id": execution_id, "rows": len(rows)})

    artifact_id = uuid.uuid4().hex
    xlsx_filename = f"{report_id}-{artifact_id[:8]}.xlsx"
    html_filename = f"{report_id}-{artifact_id[:8]}.html"
    root = artifacts_dir(portal_data_dir)
    root.mkdir(parents=True, exist_ok=True)
    display_sheets = normalize_product_labels(sheets)
    write_workbook(root / xlsx_filename, display_sheets)
    write_visualization(root / html_filename, report_title=title, snapshot_pt_date=display_snapshot, sheets=sheets, report_id=report_id)
    metadata = {
        "id": artifact_id,
        "report_id": report_id,
        "filename": xlsx_filename,
        "visualization_filename": html_filename,
        "source_filename": f"data-workbench-aggregates-{snapshot_pt_date or 'latest'}.xlsx",
        "row_count": sum(len(rows) for _sheet, _headers, rows in sheets),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "snapshot_pt_date": snapshot_pt_date,
        "sql": sql,
        "workbench_executions": executions,
    }
    update_report_artifact(portal_data_dir, report_id=report_id, metadata=metadata)
    print(f"{report_id}: completed rows={metadata['row_count']} excel={xlsx_filename} visualization={html_filename}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report-id",
        action="append",
        default=[],
        help="Report id to generate. Repeatable. Defaults to missing generator-backed reports.",
    )
    parser.add_argument("--all", action="store_true", help="Generate all supported generator-backed reports.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip reports that already have Excel and visualization artifacts.")
    parser.add_argument("--check-session", action="store_true", help="Only validate the Data Admin live session and exit.")
    parser.add_argument(
        "--refresh-visualizations",
        action="store_true",
        help="Regenerate HTML visualizations from existing Excel artifacts without calling Data Workbench.",
    )
    parser.add_argument(
        "--normalize-product-labels",
        action="store_true",
        help="Rewrite existing Excel and HTML artifacts so known product codes display Apollo product names.",
    )
    parser.add_argument(
        "--snapshot-pt-date",
        default=LATEST_SNAPSHOT,
        help="Data Workbench pt_date snapshot to use, or 'latest' (default) to resolve the newest available partition at run time.",
    )
    parser.add_argument(
        "--portal-data-dir",
        default=os.environ.get("TEAM_PORTAL_DATA_DIR") or str(DEFAULT_PORTAL_DATA_DIR),
        help="Portal data dir containing business_insights/reports.json.",
    )
    parser.add_argument("--chrome-profile", default="Default", help="Chrome profile directory for Data Admin cookies.")
    parser.add_argument("--poll-seconds", type=int, default=3, help="Seconds between Data Workbench log polls.")
    parser.add_argument("--max-polls", type=int, default=240, help="Maximum Data Workbench log polls per SQL section.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    portal_data_dir = Path(args.portal_data_dir).expanduser().resolve()
    if args.refresh_visualizations:
        selected_report_ids = args.report_id if args.report_id else None
        refresh_existing_visualizations(portal_data_dir, report_ids=selected_report_ids)
        return 0
    if args.normalize_product_labels:
        selected_report_ids = args.report_id if args.report_id else None
        normalize_existing_product_labels(portal_data_dir, report_ids=selected_report_ids)
        return 0

    token = load_data_admin_token(chrome_profile=args.chrome_profile)
    payload = _decode_jwt_payload(token)
    user = (payload.get("user") or {}).get("email") if isinstance(payload.get("user"), dict) else ""
    print(f"loaded_data_admin_token_for={user or 'unknown'}", flush=True)
    session = build_data_admin_session(token)
    validated_user = validate_data_admin_session(session)
    print(f"validated_data_admin_session={validated_user}", flush=True)
    if args.check_session:
        return 0

    if args.all:
        report_ids = list(REPORT_BUILDERS)
    elif args.report_id:
        report_ids = args.report_id
    else:
        report_ids = [report_id for report_id in REPORT_BUILDERS if not report_has_artifacts(portal_data_dir, report_id)]
    if not report_ids:
        print("No reports selected.", flush=True)
        return 0
    now = datetime.now(ZoneInfo("Asia/Singapore"))
    for report_id in report_ids:
        snapshot_pt_date = args.snapshot_pt_date
        if snapshot_pt_date == LATEST_SNAPSHOT:
            snapshot_pt_date = resolve_snapshot_pt_date(
                session, report_id, poll_seconds=args.poll_seconds, max_polls=args.max_polls
            )
            print(f"{report_id}: resolved latest snapshot pt_date={snapshot_pt_date}", flush=True)
        generate_report(
            session=session,
            portal_data_dir=portal_data_dir,
            report_id=report_id,
            snapshot_pt_date=snapshot_pt_date,
            now=now,
            skip_existing=args.skip_existing,
            poll_seconds=args.poll_seconds,
            max_polls=args.max_polls,
        )
    if report_has_artifacts(portal_data_dir, UNDERWRITING_FUNNEL_REPORT_ID):
        print(f"{UNDERWRITING_FUNNEL_REPORT_ID}: existing artifact present.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
