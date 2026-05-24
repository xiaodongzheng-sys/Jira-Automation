"""Monthly Report historical sent-report lookup.

This module owns the small slice of Gmail history that Monthly Report needs for
style guidance. It intentionally has no dependency on shared memory storage.
"""
from __future__ import annotations

import re
from typing import Any


SENT_MONTHLY_REPORT_SUBJECT_PATTERN = re.compile(
    r"^\[Banking\]\s+Product\s+Update\s+\("
    r"\d{1,2}\s+[A-Za-z]{3}\s*-\s*\d{1,2}\s+[A-Za-z]{3}"
    r"\)\s+-\s+Anti-Fraud,\s+Credit\s+Risk\s+&\s+Ops\s+Risk$",
    re.IGNORECASE,
)


def is_sent_monthly_report_subject(subject: str) -> bool:
    return bool(SENT_MONTHLY_REPORT_SUBJECT_PATTERN.match(str(subject or "").strip()))


def monthly_report_history_item_from_gmail_record(*, owner_email: str, record: Any) -> dict[str, Any]:
    headers = getattr(record, "headers", {}) or {}
    subject = str(headers.get("subject") or "").strip()
    sent_at = getattr(record, "internal_date", None)
    if hasattr(sent_at, "isoformat"):
        sent_at_value = sent_at.isoformat()
    else:
        sent_at_value = str(sent_at or "")
    body = str(getattr(record, "body_text", "") or "").strip()
    message_id = str(getattr(record, "message_id", "") or "").strip()
    return {
        "source_type": "gmail_sent_monthly_report",
        "item_type": "curated_report",
        "source_id": message_id,
        "owner_email": str(owner_email or "").strip().lower(),
        "summary": subject,
        "subject": subject,
        "content": body,
        "metadata": {
            "subject": subject,
            "message_id": message_id,
            "sent_at": sent_at_value,
        },
    }


def scan_sent_monthly_reports_from_gmail(
    *,
    service: Any,
    owner_email: str,
    max_messages_per_query: int = 50,
) -> dict[str, Any]:
    queries = [
        'in:sent newer_than:365d from:me subject:"[Banking] Product Update"',
        'in:sent newer_than:365d from:me subject:"Anti-Fraud, Credit Risk & Ops Risk"',
        'in:sent newer_than:365d from:me "[Banking] Product Update" "Anti-Fraud, Credit Risk & Ops Risk"',
    ]
    normalized_owner = str(owner_email or "").strip().lower()
    seen_ids: set[str] = set()
    records: list[Any] = []
    for query in queries:
        for message_id in service._list_message_ids(query=query, max_messages=max_messages_per_query):
            message_id = str(message_id or "").strip()
            if not message_id or message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            records.append(service._fetch_message_full(message_id))

    items: list[dict[str, Any]] = []
    for record in records:
        headers = getattr(record, "headers", {}) or {}
        sender_text = str(headers.get("from") or "").casefold()
        if normalized_owner and normalized_owner not in sender_text:
            continue
        if not is_sent_monthly_report_subject(str(headers.get("subject") or "")):
            continue
        items.append(monthly_report_history_item_from_gmail_record(owner_email=normalized_owner, record=record))
    return {"scanned": len(records), "matched": len(items), "items": items}
