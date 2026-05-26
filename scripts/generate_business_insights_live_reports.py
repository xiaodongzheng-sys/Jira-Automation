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
    APPLICATION_DISBURSEMENT_FUNNEL_REPORT_ID,
    LIMIT_UTILIZATION_REPORT_ID,
    PORTFOLIO_REPAYMENT_REPORT_ID,
    UNDERWRITING_FUNNEL_REPORT_ID,
    build_application_disbursement_funnel_sql,
    build_limit_utilization_sql,
    build_portfolio_repayment_sql,
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


def write_workbook(path: Path, sheets: list[tuple[str, list[str], list[list[Any]]]]) -> None:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for sheet_name, schema, rows in sheets:
        sheet = workbook.create_sheet(sheet_name[:31])
        sheet.append(schema)
        for row in rows:
            sheet.append(row)
        style_sheet(sheet)
    workbook.save(path)


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


def _format_cell(header: str, value: Any) -> str:
    lowered = header.lower()
    if lowered.endswith("rate") or lowered.startswith("%") or "% " in lowered:
        number = _number(value)
        if value in (None, ""):
            return ""
        if abs(number) <= 1:
            number *= 100
        return f"{number:.1f}%"
    return _format_number(value) if _is_number(value) else str(value or "")


def _table_html(headers: list[str], rows: list[list[Any]], *, max_rows: int = 30) -> str:
    header_html = "".join(f"<th>{html.escape(str(header))}</th>" for header in headers)
    body_rows = []
    for row in rows[:max_rows]:
        cells = []
        for index, header in enumerate(headers):
            value = row[index] if index < len(row) else ""
            class_name = "num" if _is_number(value) else ""
            cells.append(f'<td class="{class_name}">{html.escape(_format_cell(str(header), value))}</td>')
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    more = f'<p class="note">Showing top {max_rows} of {len(rows)} rows. Full data is in Excel.</p>' if len(rows) > max_rows else ""
    return f'<div class="table-wrap"><table><thead><tr>{header_html}</tr></thead><tbody>{"".join(body_rows)}</tbody></table></div>{more}'


def _bar_chart(title: str, labels: list[Any], values: list[Any], *, value_suffix: str = "") -> str:
    pairs = [(str(label or "UNKNOWN"), _number(value)) for label, value in zip(labels, values, strict=False)]
    pairs = [item for item in pairs if item[1] != 0][:12]
    if not pairs:
        return ""
    maximum = max(value for _label, value in pairs) or 1
    rows = []
    for label, value in pairs:
        width = max(2.0, value / maximum * 100.0)
        rows.append(
            '<div class="bar-row">'
            f"<span>{html.escape(label)}</span>"
            f'<div class="bar-track"><div class="bar" style="width:{width:.1f}%"></div></div>'
            f"<b>{html.escape(_format_number(value, suffix=value_suffix))}</b>"
            "</div>"
        )
    return f'<section class="panel"><h2>{html.escape(title)}</h2>{"".join(rows)}</section>'


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
        if {"total_limit", "used_limit"}.issubset(header_index):
            total_index = header_index["total_limit"]
            used_index = header_index["used_limit"]
            frozen_index = header_index.get("frozen_limit")
            inconsistent_limit_rows = 0
            for row in rows:
                total = _number(row[total_index] if total_index < len(row) else None)
                used = _number(row[used_index] if used_index < len(row) else None)
                frozen = _number(row[frozen_index] if frozen_index is not None and frozen_index < len(row) else None)
                if total == 0 and (used != 0 or frozen != 0):
                    inconsistent_limit_rows += 1
            if inconsistent_limit_rows:
                notes.append(
                    f"{sheet_name}: {inconsistent_limit_rows} products have zero total_limit but non-zero used/frozen limit; treat availability and utilization as undefined for those rows."
                )
        for column_index, header in enumerate(headers):
            numeric_values = []
            for row in rows:
                value = row[column_index] if column_index < len(row) else None
                if _is_number(value):
                    numeric_values.append(_number(value))
            if not numeric_values:
                continue
            gt_int32 = sum(1 for value in numeric_values if abs(value) > 2_147_483_647)
            gt_int64 = sum(1 for value in numeric_values if abs(value) > 9_223_372_036_854_775_807)
            negatives = sum(1 for value in numeric_values if value < 0)
            lowered = str(header).lower()
            if gt_int64:
                notes.append(f"{sheet_name}.{header}: {gt_int64} values exceed signed 64-bit integer range.")
            elif gt_int32 and any(token in lowered for token in ("amount", "limit", "principal", "interest")):
                notes.append(f"{sheet_name}.{header}: {gt_int32} values exceed signed 32-bit range; this is expected for aggregate currency but should be stored as 64-bit/decimal.")
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


def write_visualization(
    path: Path,
    *,
    report_title: str,
    snapshot_pt_date: str,
    sheets: list[tuple[str, list[str], list[list[Any]]]],
) -> None:
    lookup = {sheet_name: (headers, rows) for sheet_name, headers, rows in sheets}
    sections: list[str] = []
    sections.append(_overview_cards(report_title, sheets))
    quality_notes = _analyze_sheets(sheets)
    if quality_notes:
        notes = "".join(f"<li>{html.escape(note)}</li>" for note in quality_notes[:10])
        sections.append(f'<section class="quality-card"><h2>Data Quality Notes</h2><ul>{notes}</ul></section>')
    else:
        sections.append('<section class="quality-card good"><h2>Data Quality Notes</h2><p>No signed 64-bit overflow, negative capacity, or rate-bound anomalies detected in aggregate sheets.</p></section>')
    summary = lookup.get("Summary by Product") or lookup.get("Funnel Summary by Product")
    if summary:
        headers, rows = summary
        index = {header: offset for offset, header in enumerate(headers)}
        product_index = index.get("product", 1 if len(headers) > 1 else 0)
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
body{{margin:0;background:var(--bg);color:var(--ink);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}}
header{{background:linear-gradient(135deg,#102a43,#173b5f);color:#fff;padding:30px 38px;}}
header h1{{margin:0 0 8px;font-size:30px;letter-spacing:0;}} header p{{margin:0;color:#dbeafe;}}
main{{padding:24px 34px 38px;display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:18px;}}
.hero-card,.quality-card,.panel{{background:#fff;border:1px solid var(--line);border-radius:8px;padding:18px;box-shadow:0 1px 2px rgba(16,42,67,.06);}}
.hero-card,.quality-card,.wide{{grid-column:1/-1;}} .panel{{grid-column:span 6;}}
.eyebrow{{margin:0 0 6px;color:var(--blue);font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;}}
h2{{margin:0 0 14px;font-size:18px;letter-spacing:0;}}
.hero-card h2{{font-size:22px;margin-bottom:16px;}}
.kpi-grid{{display:grid;grid-template-columns:repeat(3,minmax(160px,1fr));gap:12px;}}
.kpi{{border:1px solid #e4e7ec;border-radius:8px;padding:14px;background:#fafcff;}}
.kpi span{{display:block;color:var(--muted);font-size:12px;margin-bottom:6px;}} .kpi strong{{display:block;font-size:24px;}}
.kpi.good strong{{color:var(--green);}} .kpi.watch strong{{color:var(--amber);}}
.quality-card{{border-left:4px solid var(--amber);}} .quality-card.good{{border-left-color:var(--green);}}
.quality-card ul{{margin:0;padding-left:18px;color:#344054;}} .quality-card li{{margin:6px 0;}}
.bar-row{{display:grid;grid-template-columns:minmax(120px,220px) 1fr minmax(90px,auto);gap:12px;align-items:center;margin:10px 0;}}
.bar-row span{{color:#344054;font-weight:600;}} .bar-row b{{text-align:right;font-variant-numeric:tabular-nums;}}
.bar-track{{height:18px;background:#e5e7eb;border-radius:4px;overflow:hidden;}} .bar{{height:100%;background:linear-gradient(90deg,#1769e0,#39a0ff);}}
.table-wrap{{overflow:auto;border:1px solid #edf1f7;border-radius:6px;}} table{{width:100%;border-collapse:collapse;font-size:13px;}} th,td{{border-bottom:1px solid #edf1f7;padding:8px 10px;text-align:left;white-space:nowrap;}} th{{background:#f1f6ff;font-weight:700;color:#344054;position:sticky;top:0;}} td.num{{text-align:right;font-variant-numeric:tabular-nums;}}
.note{{color:var(--muted);font-size:12px;margin:10px 0 0;}}
@media(max-width:900px){{main{{grid-template-columns:1fr;padding-left:16px;padding-right:16px;}}.panel{{grid-column:1/-1;}}.kpi-grid{{grid-template-columns:1fr;}}.bar-row{{grid-template-columns:1fr;gap:6px;}}.bar-row b{{text-align:left;}}}}
</style></head><body><header><h1>{html.escape(report_title)}</h1><p>Snapshot {html.escape(snapshot_pt_date)}. Generated {generated_at} UTC from Data Workbench aggregate output.</p></header><main>{"".join(sections)}</main></body></html>"""
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


def sheets_from_workbook(path: Path) -> list[tuple[str, list[str], list[list[Any]]]]:
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        sheets: list[tuple[str, list[str], list[list[Any]]]] = []
        for sheet in workbook.worksheets:
            raw_rows = list(sheet.iter_rows(values_only=True))
            if not raw_rows:
                continue
            headers = [str(value or "") for value in raw_rows[0]]
            rows = [list(row) for row in raw_rows[1:] if any(value not in (None, "") for value in row)]
            if sheet.title == "Raw Export":
                continue
            sheets.append((sheet.title, headers, rows))
        return sheets
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
            sheets=sheets_from_workbook(workbook_path),
        )
        artifact["visualization_filename"] = visualization_filename
        print(f"{report_id}: refreshed visualization={visualization_filename}", flush=True)
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
    write_workbook(root / xlsx_filename, sheets)
    write_visualization(root / html_filename, report_title=title, snapshot_pt_date=snapshot_pt_date, sheets=sheets)
    metadata = {
        "id": artifact_id,
        "report_id": report_id,
        "filename": xlsx_filename,
        "visualization_filename": html_filename,
        "source_filename": f"data-workbench-aggregates-{snapshot_pt_date}.xlsx",
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
    parser.add_argument("--snapshot-pt-date", default="2026-05-25", help="Data Workbench pt_date snapshot to use.")
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
        generate_report(
            session=session,
            portal_data_dir=portal_data_dir,
            report_id=report_id,
            snapshot_pt_date=args.snapshot_pt_date,
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
