from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup, NavigableString, Tag


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


@dataclass(frozen=True)
class _PdfSegment:
    text: str
    bold: bool = False


@dataclass(frozen=True)
class _PdfLine:
    segments: tuple[_PdfSegment, ...]
    size: int = 10
    indent: int = 0
    bold: bool = False


def daily_brief_pdf_bytes(*, title: str, body: str, html_body: str = "") -> bytes:
    lines = _daily_brief_pdf_lines(title=title, body=body, html_body=html_body)
    pages = [lines[index : index + 44] for index in range(0, len(lines), 44)] or [[_PdfLine((_PdfSegment("Daily Brief"),), bold=True)]]
    objects: list[bytes] = [b"<< /Type /Catalog /Pages 2 0 R >>"]
    page_refs: list[str] = []
    content_objects: list[bytes] = []
    for page_index, page_lines in enumerate(pages):
        page_obj_num = 3 + page_index * 2
        content_obj_num = page_obj_num + 1
        page_refs.append(f"{page_obj_num} 0 R")
        stream = _pdf_page_stream(page_lines)
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> /Contents {content_obj_num} 0 R >>".encode("latin-1")
        )
        content_objects.append(f"<< /Length {len(stream)} >>\nstream\n".encode("latin-1") + stream + b"\nendstream")
    font_obj_num = 3 + len(pages) * 2
    bold_font_obj_num = font_obj_num + 1
    objects.insert(1, f"<< /Type /Pages /Kids [{' '.join(page_refs)}] /Count {len(page_refs)} >>".encode("latin-1"))
    merged: list[bytes] = [objects[0], objects[1]]
    for index in range(len(pages)):
        merged.append(objects[2 + index])
        merged.append(content_objects[index])
    merged.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    merged.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")
    # Rewrite font object references if the page count moved the font object number.
    merged = [
        item.replace(b"/F1 5 0 R", f"/F1 {font_obj_num} 0 R".encode("latin-1")).replace(
            b"/F2 6 0 R",
            f"/F2 {bold_font_obj_num} 0 R".encode("latin-1"),
        )
        for item in merged
    ]
    return _build_pdf(merged)


def _daily_brief_pdf_lines(*, title: str, body: str, html_body: str) -> list[_PdfLine]:
    lines = [_PdfLine((_PdfSegment(str(title or "Daily Brief")),), size=16, bold=True), _PdfLine(tuple(), size=6)]
    html_text = str(html_body or "").strip()
    if html_text:
        parsed = _html_pdf_lines(html_text)
        if parsed:
            return lines + _strip_daily_brief_body_header(parsed, title=title)
    text_lines = []
    for item in _wrap_pdf_lines(str(body or ""), max_chars=92):
        text_lines.append(_PdfLine((_PdfSegment(item),) if item else tuple()))
    return lines + _strip_daily_brief_body_header(text_lines, title=title)


def _strip_daily_brief_body_header(lines: list[_PdfLine], *, title: str) -> list[_PdfLine]:
    output = list(lines)
    output = _drop_leading_blank_pdf_lines(output)
    output = _strip_leading_subject_line(output)
    output = _drop_leading_blank_pdf_lines(output)
    output = _strip_leading_daily_brief_heading(output, title=title)
    output = _drop_leading_blank_pdf_lines(output)
    output = _strip_leading_window_line(output)
    return _drop_leading_blank_pdf_lines(output)


def _drop_leading_blank_pdf_lines(lines: list[_PdfLine]) -> list[_PdfLine]:
    index = 0
    while index < len(lines) and not _line_text(lines[index]):
        index += 1
    return lines[index:]


def _strip_leading_subject_line(lines: list[_PdfLine]) -> list[_PdfLine]:
    if lines and _line_text(lines[0]).lower().startswith("subject: daily brief"):
        return lines[1:]
    return lines


def _strip_leading_daily_brief_heading(lines: list[_PdfLine], *, title: str) -> list[_PdfLine]:
    if not lines:
        return lines
    first_text = _line_text(lines[0])
    if not first_text.lower().startswith("daily brief"):
        return lines

    normalized_title = _collapse_spaces(str(title or "Daily Brief")).lower()
    candidate = first_text
    consume_until = 1
    for next_index in range(1, min(len(lines), 5)):
        next_text = _line_text(lines[next_index])
        if not next_text:
            consume_until = next_index + 1
            break
        combined = _collapse_spaces(f"{candidate} {next_text}")
        combined_lower = combined.lower()
        if normalized_title.startswith(combined_lower) or combined_lower.startswith(normalized_title):
            candidate = combined
            consume_until = next_index + 1
            continue
        if normalized_title.startswith(candidate.lower()) and _looks_like_wrapped_title_remainder(next_text):
            candidate = combined
            consume_until = next_index + 1
            continue
        break
    return lines[consume_until:]


def _looks_like_wrapped_title_remainder(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9:() \-]+", _collapse_spaces(value)))


def _strip_leading_window_line(lines: list[_PdfLine]) -> list[_PdfLine]:
    output = list(lines)
    while output and _line_text(output[0]).lower().startswith("window:"):
        output = _drop_leading_blank_pdf_lines(output[1:])
    return output


def _html_pdf_lines(html_body: str) -> list[_PdfLine]:
    soup = BeautifulSoup(html_body, "html.parser")
    root = soup.body or soup
    lines: list[_PdfLine] = []
    for child in root.children:
        lines.extend(_html_node_pdf_lines(child))
    return lines


def _html_node_pdf_lines(node: Any, *, indent: int = 0) -> list[_PdfLine]:
    if isinstance(node, NavigableString):
        text = _collapse_spaces(str(node))
        return [_PdfLine((_PdfSegment(text),), indent=indent)] if text else []
    if not isinstance(node, Tag):
        return []
    name = node.name.lower()
    if name in {"style", "script"}:
        return []
    if name in {"h1", "h2"}:
        return _wrapped_segment_lines(_inline_segments(node), size=15, indent=indent, bold=True) + [_PdfLine(tuple(), size=4)]
    if name == "h3":
        return _wrapped_segment_lines(_inline_segments(node), size=13, indent=indent, bold=True) + [_PdfLine(tuple(), size=3)]
    if name == "h4":
        return _wrapped_segment_lines(_inline_segments(node), size=11, indent=indent, bold=True)
    if name == "p":
        segments = _inline_segments(node)
        return (_wrapped_segment_lines(segments, indent=indent) if segments else []) + [_PdfLine(tuple(), size=4)]
    if name in {"ul", "ol"}:
        output: list[_PdfLine] = []
        for child in node.children:
            if isinstance(child, Tag) and child.name and child.name.lower() == "li":
                output.extend(_html_node_pdf_lines(child, indent=indent + 14))
        output.append(_PdfLine(tuple(), size=3))
        return output
    if name == "li":
        segments = [_PdfSegment("- ")] + _inline_segments(node, stop_at_block=True)
        output = _wrapped_segment_lines(segments, indent=indent)
        for child in node.children:
            if isinstance(child, Tag) and child.name and child.name.lower() in {"ul", "ol"}:
                output.extend(_html_node_pdf_lines(child, indent=indent + 12))
        return output
    if name in {"br"}:
        return [_PdfLine(tuple())]
    output: list[_PdfLine] = []
    for child in node.children:
        output.extend(_html_node_pdf_lines(child, indent=indent))
    return output


def _inline_segments(node: Tag, *, bold: bool = False, stop_at_block: bool = False) -> list[_PdfSegment]:
    segments: list[_PdfSegment] = []
    for child in node.children:
        if isinstance(child, NavigableString):
            text = _collapse_spaces(str(child))
            if text:
                segments.append(_PdfSegment(text, bold=bold))
            continue
        if not isinstance(child, Tag):
            continue
        name = child.name.lower()
        if stop_at_block and name in {"ul", "ol", "p", "h1", "h2", "h3", "h4"}:
            continue
        if name == "br":
            segments.append(_PdfSegment("\n", bold=bold))
            continue
        child_bold = bold or name in {"strong", "b"}
        segments.extend(_inline_segments(child, bold=child_bold, stop_at_block=stop_at_block))
    return _normalize_segments(segments)


def _normalize_segments(segments: list[_PdfSegment]) -> list[_PdfSegment]:
    normalized: list[_PdfSegment] = []
    for segment in segments:
        text = segment.text
        if not text:
            continue
        if normalized and normalized[-1].bold == segment.bold and "\n" not in normalized[-1].text and "\n" not in text:
            previous = normalized.pop()
            normalized.append(_PdfSegment((previous.text + " " + text).strip(), bold=segment.bold))
        else:
            normalized.append(_PdfSegment(text, bold=segment.bold))
    return normalized


def _wrapped_segment_lines(
    segments: list[_PdfSegment],
    *,
    size: int = 10,
    indent: int = 0,
    bold: bool = False,
    max_width: float = 512,
) -> list[_PdfLine]:
    width_limit = max(180.0, max_width - indent)
    lines: list[_PdfLine] = []
    current: list[_PdfSegment] = []
    current_width = 0.0
    for segment in segments:
        parts = segment.text.split("\n")
        for part_index, part in enumerate(parts):
            words = part.split()
            for word in words:
                prefix = " " if current else ""
                token = f"{prefix}{word}"
                token_width = _segment_width(token, size=size, bold=segment.bold or bold)
                if current and current_width + token_width > width_limit:
                    lines.append(_PdfLine(tuple(current), size=size, indent=indent, bold=bold))
                    current = []
                    current_width = 0.0
                    token = word
                    token_width = _segment_width(token, size=size, bold=segment.bold or bold)
                current.append(_PdfSegment(token, bold=segment.bold))
                current_width += token_width
            if part_index < len(parts) - 1:
                lines.append(_PdfLine(tuple(current), size=size, indent=indent, bold=bold))
                current = []
                current_width = 0.0
    if current:
        lines.append(_PdfLine(tuple(current), size=size, indent=indent, bold=bold))
    return lines


def _collapse_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _line_text(line: _PdfLine) -> str:
    return _collapse_spaces("".join(segment.text for segment in line.segments))


def _segment_width(value: str, *, size: int, bold: bool) -> float:
    multiplier = 0.57 if bold else 0.53
    return len(value) * size * multiplier


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


def _pdf_page_stream(lines: list[_PdfLine]) -> bytes:
    commands = ["BT"]
    y = 750
    for line in lines:
        if not line.segments:
            y -= max(8, line.size)
            continue
        x = 50 + line.indent
        commands.append(f"1 0 0 1 {x} {y} Tm")
        for segment in line.segments:
            font = "/F2" if line.bold or segment.bold else "/F1"
            commands.append(f"{font} {line.size} Tf")
            commands.append(f"({_pdf_escape(segment.text)}) Tj")
        y -= max(12, int(line.size * 1.45))
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
