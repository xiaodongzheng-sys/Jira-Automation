from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any


class DailyBriefArchiveStore:
    def __init__(self, storage_path: Path) -> None:
        self.storage_path = Path(storage_path)

    def save(
        self,
        *,
        run_date: str,
        run_slot: str,
        recipient: str,
        subject: str,
        text_body: str,
        html_body: str,
        message_id: str,
        status: str,
        sent_at: datetime,
        window_start: datetime | str | None = None,
        window_end: datetime | str | None = None,
    ) -> dict[str, Any]:
        payload = self._load()
        briefs = payload.setdefault("briefs", {})
        brief_id = daily_brief_id(run_date=run_date, run_slot=run_slot, recipient=recipient)
        item = {
            "brief_id": brief_id,
            "run_date": str(run_date or "").strip(),
            "run_slot": str(run_slot or "").strip(),
            "recipient": str(recipient or "").strip().lower(),
            "subject": str(subject or "").strip(),
            "text_body": str(text_body or ""),
            "html_body": str(html_body or ""),
            "message_id": str(message_id or "").strip(),
            "status": str(status or "sent").strip() or "sent",
            "sent_at": sent_at.isoformat(),
            "window_start": _datetime_value(window_start),
            "window_end": _datetime_value(window_end),
        }
        item["time_period"] = format_daily_brief_period(item)
        briefs[brief_id] = item
        self._write(payload)
        return dict(item)

    def list_recent(self, *, limit: int = 30) -> list[dict[str, Any]]:
        briefs = self._load().get("briefs", {})
        items = [dict(item) for item in briefs.values() if isinstance(item, dict)]
        for item in items:
            item["time_period"] = str(item.get("time_period") or format_daily_brief_period(item))
        return sorted(
            items,
            key=lambda item: (
                _timestamp_sort_key(item.get("window_end")),
                _timestamp_sort_key(item.get("sent_at")),
                str(item.get("brief_id") or ""),
            ),
            reverse=True,
        )[: max(1, int(limit or 30))]

    def get(self, brief_id: str) -> dict[str, Any] | None:
        item = self._load().get("briefs", {}).get(str(brief_id or "").strip())
        if not isinstance(item, dict):
            return None
        result = dict(item)
        result["time_period"] = str(result.get("time_period") or format_daily_brief_period(result))
        return result

    def _load(self) -> dict[str, Any]:
        if not self.storage_path.exists():
            return {"briefs": {}}
        try:
            payload = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"briefs": {}}
        return payload if isinstance(payload, dict) else {"briefs": {}}

    def _write(self, payload: dict[str, Any]) -> None:
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.storage_path.with_name(f".{self.storage_path.name}.{os.getpid()}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self.storage_path)


def daily_brief_archive_path(data_root: Path) -> Path:
    return Path(data_root) / "seatalk" / "daily_briefs.json"


def daily_brief_id(*, run_date: str, run_slot: str, recipient: str) -> str:
    normalized_date = re.sub(r"[^0-9-]", "", str(run_date or "").strip()) or "unknown-date"
    normalized_slot = re.sub(r"[^a-z0-9_-]", "-", str(run_slot or "").strip().lower()) or "daily"
    recipient_hash = hashlib.sha256(str(recipient or "").strip().lower().encode("utf-8")).hexdigest()[:10]
    return f"{normalized_date}-{normalized_slot}-{recipient_hash}"


def format_daily_brief_period(item: dict[str, Any]) -> str:
    start = _parse_datetime(item.get("window_start"))
    end = _parse_datetime(item.get("window_end"))
    if not start or not end:
        return ""
    if start.date() == end.date():
        return f"{start:%Y-%m-%d %H:%M}-{end:%H:%M}"
    return f"{start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}"


def daily_brief_pdf_bytes(*, title: str, body: str) -> bytes:
    lines = _wrap_pdf_lines(str(title or "Daily Brief"), max_chars=78)
    lines.append("")
    lines.extend(_wrap_pdf_lines(str(body or ""), max_chars=92))
    pages = [lines[index : index + 48] for index in range(0, len(lines), 48)] or [["Daily Brief"]]
    objects: list[bytes] = [b"<< /Type /Catalog /Pages 2 0 R >>"]
    page_refs: list[str] = []
    content_objects: list[bytes] = []
    for page_index, page_lines in enumerate(pages):
        page_obj_num = 3 + page_index * 2
        content_obj_num = page_obj_num + 1
        page_refs.append(f"{page_obj_num} 0 R")
        stream = _pdf_page_stream(page_lines)
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents {content_obj_num} 0 R >>".encode("latin-1")
        )
        content_objects.append(f"<< /Length {len(stream)} >>\nstream\n".encode("latin-1") + stream + b"\nendstream")
    font_obj_num = 3 + len(pages) * 2
    objects.insert(1, f"<< /Type /Pages /Kids [{' '.join(page_refs)}] /Count {len(page_refs)} >>".encode("latin-1"))
    merged: list[bytes] = [objects[0], objects[1]]
    for index in range(len(pages)):
        merged.append(objects[2 + index])
        merged.append(content_objects[index])
    merged.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    # Rewrite font object references if the page count moved the font object number.
    merged = [
        item.replace(b"/F1 5 0 R", f"/F1 {font_obj_num} 0 R".encode("latin-1"))
        for item in merged
    ]
    return _build_pdf(merged)


def _datetime_value(value: datetime | str | None) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "").strip()


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _timestamp_sort_key(value: Any) -> float:
    parsed = _parse_datetime(value)
    return parsed.timestamp() if parsed else 0.0


def _wrap_pdf_lines(value: str, *, max_chars: int) -> list[str]:
    output: list[str] = []
    for raw_line in str(value or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw_line.strip()
        if not line:
            output.append("")
            continue
        while len(line) > max_chars:
            split_at = line.rfind(" ", 0, max_chars)
            if split_at < max_chars // 2:
                split_at = max_chars
            output.append(line[:split_at].rstrip())
            line = line[split_at:].strip()
        output.append(line)
    return output


def _pdf_escape(value: str) -> str:
    return str(value or "").encode("latin-1", errors="replace").decode("latin-1").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _pdf_page_stream(lines: list[str]) -> bytes:
    commands = ["BT", "/F1 10 Tf", "14 TL", "50 750 Td"]
    first = True
    for line in lines:
        if not first:
            commands.append("T*")
        commands.append(f"({_pdf_escape(line)}) Tj")
        first = False
    commands.append("ET")
    return "\n".join(commands).encode("latin-1", errors="replace")


def _build_pdf(objects: list[bytes]) -> bytes:
    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(output))
        output.extend(f"{index} 0 obj\n".encode("latin-1"))
        output.extend(obj)
        output.extend(b"\nendobj\n")
    xref_offset = len(output)
    output.extend(f"xref\n0 {len(objects) + 1}\n".encode("latin-1"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))
    output.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode("latin-1")
    )
    return bytes(output)
