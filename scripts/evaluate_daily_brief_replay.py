"""Replay historical Daily Brief windows without sending or archiving results."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from dataclasses import replace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.gmail_dashboard import GMAIL_READONLY_SCOPE, GmailDashboardService
from bpmis_jira_tool.gmail_sender import StoredGoogleCredentials, credentials_from_payload
from bpmis_jira_tool.report_intelligence import (
    key_project_candidates_from_team_config,
    load_report_intelligence_config_from_data_root,
    load_team_dashboard_config_from_data_root,
)
from bpmis_jira_tool.seatalk_daily_email import (
    DAILY_EMAIL_WEEKDAY_RUNS,
    _build_daily_brief_evidence_refs,
    _build_high_signal_review_hints,
    _build_resolved_team_member_reminder_candidates,
    _build_team_member_reminder_candidates,
    _filter_daily_brief_meeting_logistics,
    _filter_gmail_calendar_history,
    _filter_team_member_coverage_items,
    _brief_items_refer_to_same_topic,
    build_daily_briefing,
    build_seatalk_service,
    data_root_from_settings,
    export_window_gmail_threads,
    export_window_history,
    render_email,
    resolve_daily_email_window,
    seatalk_name_overrides_path,
)
from bpmis_jira_tool.seatalk_dashboard import SEATALK_INSIGHTS_TIMEZONE


SECTION_NAMES = (
    "Xiaodong Action Required",
    "Watch / Delegate",
    "Project Updates",
    "Other Update",
    "Suggested Team Follow-up",
)
RAW_SOURCE_ID_RE = re.compile(r"\b(?:group|buddy)-\d+\b|\bUID\s+\d+\b", re.IGNORECASE)
CALENDAR_LEAK_CUES = (
    "accepted:",
    "declined:",
    "tentative:",
    "updated invitation",
    "invitation:",
    "you’re invited",
    "you're invited",
    "rescheduled",
    "rsvp",
    "google calendar",
    "event reminder",
)
RESOLVED_FOLLOWUP_CUES = (
    "already fixed",
    "already resolved",
    "already closed",
    "fixed and tested",
    "resolved and tested",
    "no longer reproduce",
    "issue is gone",
    "has been handled",
    "已修复",
    "已解决",
    "已关闭",
    "已完成",
)
KNOWN_SIGNAL_EXPECTATIONS = (
    ("Mari Stock / fallback", ("mari stock", "fallback", "mas")),
    ("MAS/compliance", ("mas",)),
    ("QRIS dependency", ("qris", "dependency", "sgdb-81072")),
    ("Rene version timeline", ("1.0.88", "dev starts", "deviceModel", "f30")),
    ("ATM v3.07/v3.08", ("v3.07", "v3.08", "atm")),
    ("PH recurring incident", ("querytransferrecipient", "swp-31174", "recurring")),
    ("Ker Yin edit access", ("edit access", "rows 548", "548-549")),
    ("PH translation request", ("translation", "configure", "copywriting")),
)


def _sha256(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _date_range(start: date, end: date) -> list[date]:
    dates: list[date] = []
    current = start
    while current <= end:
        if current.weekday() in DAILY_EMAIL_WEEKDAY_RUNS:
            dates.append(current)
        current += timedelta(days=1)
    return dates


def _iter_brief_items(briefing: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    direct = briefing.get("direct_action_todos") or []
    watch = briefing.get("watch_delegate_todos") or []
    if not direct and not watch:
        todos = briefing.get("my_todos") or []
        direct = [item for item in todos if str(item.get("action_type") or "") != "watch_delegate"]
        watch = [item for item in todos if str(item.get("action_type") or "") == "watch_delegate"]
    sections: list[tuple[str, list[Any]]] = [
        ("Xiaodong Action Required", direct),
        ("Watch / Delegate", watch),
        ("Project Updates", briefing.get("project_updates") or []),
        ("Other Update", briefing.get("other_updates") or []),
        ("Suggested Team Follow-up", briefing.get("team_member_reminders") or []),
    ]
    return [(section, item) for section, items in sections for item in items if isinstance(item, dict)]


def _item_text(item: dict[str, Any], *, include_evidence: bool = True) -> str:
    fields = ("task", "title", "summary", "reminder", "person", "evidence") if include_evidence else (
        "task", "title", "summary", "reminder", "person"
    )
    return " ".join(str(item.get(field) or "") for field in fields).casefold()


def _quality_gates(
    *,
    briefing: dict[str, Any],
    text_body: str,
    raw_seatalk: str,
    raw_gmail: str,
    filtered_gmail: str,
    candidates: list[dict[str, str]] | None,
    resolved_candidates: list[dict[str, str]],
) -> dict[str, Any]:
    items = _iter_brief_items(briefing)
    findings: list[str] = []
    sections_present = {section for section in SECTION_NAMES if section in text_body}
    if sections_present != set(SECTION_NAMES):
        findings.append("fixed_five_sections_missing")
    if text_body.count("- 无") < 5 - sum(1 for _, item in items if item):
        findings.append("empty_section_marker_missing")

    raw_id_fields: list[str] = []
    invalid_domain: list[str] = []
    invalid_status: list[str] = []
    calendar_leaks: list[str] = []
    resolved_leaks: list[str] = []
    for section, item in items:
        evidence_text = str(item.get("evidence") or "")
        if RAW_SOURCE_ID_RE.search(evidence_text):
            raw_id_fields.append(section)
        if item.get("domain") not in {"Anti-fraud", "Credit Risk", "Ops Risk", "General", None, ""}:
            invalid_domain.append(section)
        if section in {"Project Updates", "Other Update"} and item.get("status") not in {"done", "in_progress", "blocked", "unknown", None, ""}:
            invalid_status.append(section)
        if section != "Project Updates" and any(cue in _item_text(item) for cue in CALENDAR_LEAK_CUES):
            calendar_leaks.append(section)
        if section in {"Xiaodong Action Required", "Watch / Delegate", "Suggested Team Follow-up"}:
            action_text = _item_text(item, include_evidence=False)
            unresolved_context = any(
                cue in action_text
                for cue in ("follow up", "confirm", "require", "root cause", "investigate", "pending", "whether", "still")
            )
            completed_artifact_review = "completed" in action_text and any(
                cue in action_text for cue in ("review", "support", "walkthrough")
            )
            if (
                any(cue in action_text for cue in RESOLVED_FOLLOWUP_CUES)
                and not unresolved_context
                and not completed_artifact_review
            ):
                resolved_leaks.append(section)
    if raw_id_fields:
        findings.append("raw_source_id")
    if invalid_domain:
        findings.append("invalid_domain")
    if invalid_status:
        findings.append("invalid_status")
    if calendar_leaks:
        findings.append("calendar_or_meeting_leak")
    if resolved_leaks:
        findings.append("resolved_followup_leak")

    duplicates: list[dict[str, str]] = []
    for index, (left_section, left_item) in enumerate(items):
        for right_section, right_item in items[index + 1 :]:
            if left_section == right_section:
                continue
            if _brief_items_refer_to_same_topic(left_item, right_item):
                duplicates.append({"left": left_section, "right": right_section})
    if duplicates:
        findings.append("cross_section_duplicate")

    source_lines = f"{raw_seatalk}\n{raw_gmail}".casefold().splitlines()
    source_joined = "\n".join(source_lines)
    output_joined = json.dumps(briefing, ensure_ascii=False).casefold()
    known_signal_checks: list[dict[str, Any]] = []
    for name, cues in KNOWN_SIGNAL_EXPECTATIONS:
        source_hit = [cue for cue in cues if cue.casefold() in source_joined]
        if name == "QRIS dependency":
            source_hit = [cue for cue in cues if cue.casefold() in source_joined] if any(
                all(cue.casefold() in line for cue in ("qris", "dependency")) or "sgdb-81072" in line
                for line in source_lines
            ) else []
        elif name == "PH recurring incident":
            source_hit = [cue for cue in cues if cue.casefold() in source_joined] if any(
                any(marker in line for marker in ("swp-31174", "querytransferrecipient"))
                or (
                    "recurring" in line
                    and "incident" in line
                    and any(term in line for term in ("ph", "payment", "transfer", "card", "autopayment"))
                )
                for line in source_lines
            ) else []
        elif name == "PH translation request":
            source_hit = [cue for cue in cues if cue.casefold() in source_joined] if any(
                "translation" in line and ("ker yin" in line or "configure" in line)
                for line in source_lines
            ) else []
        elif name == "ATM v3.07/v3.08":
            source_hit = [cue for cue in cues if cue.casefold() in source_joined] if any(
                "atm" in line and ("v3.07" in line or "v3.08" in line)
                for line in source_lines
            ) else []
        elif name == "Mari Stock / fallback":
            source_hit = [cue for cue in cues if cue.casefold() in source_joined] if any(
                "mari stock" in line
                or ("fallback" in line and any(term in line for term in ("mas", "fraud", "anti-fraud")))
                for line in source_lines
            ) else []
        elif name == "MAS/compliance":
            source_hit = ["mas"] if any(
                re.search(r"(?<![A-Za-z0-9_])mas(?![A-Za-z0-9_])", line, flags=re.IGNORECASE)
                and any(
                    term in line
                    for term in (
                        "regulatory", "compliance", "requirement", "reporting", "register",
                        "deadline", "risk", "incident", "system down", "fallback", "强监管", "上报", "监管", "合规",
                    )
                )
                for line in source_lines
            ) else []
        elif name == "Rene version timeline":
            matched_cues: list[str] = []
            for line in source_lines:
                lowered_line = line.casefold()
                if "rene" not in lowered_line or not any(
                    term in lowered_line
                    for term in ("timeline", "version", "release", "launch", "start", "delay", "延期", "上线")
                ):
                    continue
                if any(
                    marker in lowered_line
                    for marker in ("1.0.88", "dev starts", "device model", "devicemodel", "v3.07", "v3.08")
                ):
                    matched_cues.extend(
                        cue for cue in cues if cue.casefold() != "f30" and cue.casefold() in lowered_line
                    )
                if re.search(r"(?<![A-Za-z0-9])f30(?![A-Za-z0-9])", line, flags=re.IGNORECASE) and any(
                    term in lowered_line for term in ("f30 function", "f30 dropdown", "f30 feature", "risk tag")
                ):
                    matched_cues.append("f30")
            source_hit = list(dict.fromkeys(matched_cues))
        elif name == "Ker Yin edit access":
            source_hit = [cue for cue in cues if cue.casefold() in source_joined] if any(
                "edit access" in line
                and any(term in line for term in ("ker yin", "548", "row ", "rows "))
                for line in source_lines
            ) else []
        if not source_hit:
            continue
        if name == "MAS/compliance":
            output_hit = ["mas"] if re.search(r"(?<![A-Za-z0-9_])mas(?![A-Za-z0-9_])", output_joined, flags=re.IGNORECASE) else []
        else:
            output_hit = [cue for cue in source_hit if cue.casefold() in output_joined]
        known_signal_checks.append({"name": name, "source_cues": source_hit, "output_cues": output_hit, "covered": bool(output_hit)})
    missing_known = [item["name"] for item in known_signal_checks if not item["covered"]]
    if missing_known:
        findings.append("known_high_signal_not_visible")

    raw_calendar_filtered_count = max(0, len(raw_gmail.splitlines()) - len(filtered_gmail.splitlines()))
    filter_summary = {
        "calendar_messages_suppressed": briefing.get("quality_metadata", {}).get("evidence_quality_metrics", {}).get("calendar_suppressed_count", 0),
        "gmail_lines_suppressed": raw_calendar_filtered_count,
        "seatalk_meeting_lines_suppressed": max(0, len(raw_seatalk.splitlines()) - len(_filter_daily_brief_meeting_logistics(raw_seatalk).splitlines())),
        "coverage_candidates_suppressed": max(
            0,
            len(_build_team_member_reminder_candidates(raw_seatalk) or [])
            - len(_filter_team_member_coverage_items(_build_team_member_reminder_candidates(raw_seatalk) or [])),
        ),
    }
    passed = not findings
    return {
        "passed": passed,
        "findings": findings,
        "sections_present": sorted(sections_present),
        "calendar_leak_count": len(calendar_leaks),
        "resolved_followup_leak_count": len(resolved_leaks),
        "duplicate_count": len(duplicates),
        "duplicates": duplicates,
        "known_signal_checks": known_signal_checks,
        "missing_known_high_signal": missing_known,
        "candidate_count": len(candidates or []),
        "resolved_candidate_count": len(resolved_candidates),
        "filter_summary": filter_summary,
    }


def _prepare_credentials(settings: Settings) -> tuple[Any, str, dict[str, Any]]:
    data_root = data_root_from_settings(settings)
    owner_email = str(settings.gmail_seatalk_demo_owner_email or settings.seatalk_owner_email or "").strip().lower()
    store = StoredGoogleCredentials(
        data_root / "google" / "credentials.json",
        encryption_key=settings.team_portal_config_encryption_key,
    )
    payload = store.load(owner_email=owner_email)
    scopes = {str(scope).strip() for scope in payload.get("scopes") or []}
    if GMAIL_READONLY_SCOPE not in scopes:
        raise RuntimeError("Gmail read-only scope is missing from stored credentials.")
    return credentials_from_payload(payload), owner_email, payload


def _isolated_seatalk_service(settings: Settings, temp_root: Path):
    temp_root.mkdir(parents=True, exist_ok=True)
    source_mapping = seatalk_name_overrides_path(data_root=data_root_from_settings(settings))
    target_mapping = temp_root / "seatalk" / "name_overrides.json"
    target_mapping.parent.mkdir(parents=True, exist_ok=True)
    if source_mapping.exists():
        shutil.copy2(source_mapping, target_mapping)
    service = build_seatalk_service(settings, data_root=temp_root)
    service.name_overrides_path = target_mapping
    service.daily_cache_dir = temp_root / "seatalk" / "cache"
    return service


def run_replay(*, start: date, end: date, output_dir: Path, max_windows: int = 0, data_root: Path | None = None) -> dict[str, Any]:
    settings = Settings.from_env()
    if data_root is not None:
        settings = replace(settings, team_portal_data_dir=data_root)
    actual_root = data_root_from_settings(settings)
    report_config = load_report_intelligence_config_from_data_root(actual_root)
    team_config = load_team_dashboard_config_from_data_root(actual_root)
    key_projects = key_project_candidates_from_team_config(team_config)
    credentials, owner_email, _ = _prepare_credentials(settings)
    gmail_service = GmailDashboardService(
        credentials=credentials,
        cache_key=owner_email,
        report_intelligence_config=report_config,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    windows = [(day, slot) for day in _date_range(start, end) for slot in ("morning", "midday")]
    if max_windows > 0:
        windows = windows[:max_windows]
    summary: dict[str, Any] = {
        "phase": "july" if end.month == 7 else "aug",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "window_count": len(windows),
        "windows": [],
        "passed_count": 0,
        "failed_count": 0,
        "error_count": 0,
        "production_side_effects": {"gmail_sent": 0, "trello_created": 0, "archive_written": 0},
    }
    with tempfile.TemporaryDirectory(prefix="daily-brief-replay-") as temp_dir:
        service = _isolated_seatalk_service(settings, Path(temp_dir))
        for day, slot in windows:
            now = datetime.combine(day, datetime.min.time(), tzinfo=SEATALK_INSIGHTS_TIMEZONE).replace(hour=19 if slot == "midday" else 13)
            window = resolve_daily_email_window(now=now, slot=slot)
            window_id = f"{day.isoformat()}-{slot}"
            record: dict[str, Any] = {
                "window_id": window_id,
                "run_date": day.isoformat(),
                "slot": slot,
                "window_start": window.start.isoformat(),
                "window_end": window.end.isoformat(),
            }
            try:
                raw_seatalk = export_window_history(service, window_start=window.start, window_end=window.end)
                raw_gmail = export_window_gmail_threads(gmail_service, window_start=window.start, window_end=window.end)
                filtered_gmail, suppressed_calendar_count = _filter_gmail_calendar_history(raw_gmail)
                candidates = _build_team_member_reminder_candidates(_filter_daily_brief_meeting_logistics(raw_seatalk))
                resolved_candidates = _build_resolved_team_member_reminder_candidates(_filter_daily_brief_meeting_logistics(raw_seatalk))
                evidence_refs = _build_daily_brief_evidence_refs(
                    _filter_daily_brief_meeting_logistics(raw_seatalk),
                    gmail_history_text=filtered_gmail,
                    team_member_reminder_candidates=candidates,
                )
                briefing = build_daily_briefing(
                    service,
                    now=window.end,
                    gmail_history_text=raw_gmail,
                    window_start=window.start,
                    window_end=window.end,
                    report_intelligence_config=report_config,
                    key_project_candidates=key_projects,
                )
                subject, text_body, html_body = render_email(briefing=briefing, now=window.end, window_label=window.label)
                quality = _quality_gates(
                    briefing=briefing,
                    text_body=text_body,
                    raw_seatalk=raw_seatalk,
                    raw_gmail=raw_gmail,
                    filtered_gmail=filtered_gmail,
                    candidates=candidates,
                    resolved_candidates=resolved_candidates,
                )
                record.update(
                    {
                        "status": "passed" if quality["passed"] else "failed",
                        "subject": subject,
                        "source_summary": {
                            "seatalk_chars": len(raw_seatalk),
                            "gmail_chars": len(raw_gmail),
                            "seatalk_sha256": _sha256(raw_seatalk),
                            "gmail_sha256": _sha256(raw_gmail),
                            "filtered_gmail_sha256": _sha256(filtered_gmail),
                            "evidence_sha256": _sha256(json.dumps(evidence_refs, ensure_ascii=False, sort_keys=True)),
                        },
                        "candidate_followups": candidates or [],
                        "resolved_candidates": resolved_candidates,
                        "high_signal_candidates": _build_high_signal_review_hints(_filter_daily_brief_meeting_logistics(raw_seatalk)).splitlines(),
                        "evidence_refs": evidence_refs,
                        "suppressed_calendar_count": suppressed_calendar_count,
                        "briefing": briefing,
                        "text_body": text_body,
                        "html_body": html_body,
                        "quality": quality,
                    }
                )
                summary["passed_count" if quality["passed"] else "failed_count"] += 1
            except Exception as error:  # Keep the batch report complete if one source window fails.
                record.update({"status": "error", "error": f"{type(error).__name__}: {error}"})
                summary["error_count"] += 1
            (output_dir / f"{window_id}.json").write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
            summary["windows"].append({"window_id": window_id, "status": record.get("status"), "findings": (record.get("quality") or {}).get("findings", []), "error": record.get("error", "")})
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("aug", "july", "all"), default="aug")
    parser.add_argument("--start", type=_parse_date)
    parser.add_argument("--end", type=_parse_date)
    parser.add_argument("--output", type=Path, default=Path("/tmp/daily-brief-replay"))
    parser.add_argument("--max-windows", type=int, default=0)
    parser.add_argument("--data-root", type=Path, help="Read-only Live data root containing the Google credentials and SeaTalk mappings.")
    args = parser.parse_args()
    current_date = datetime.now(SEATALK_INSIGHTS_TIMEZONE).date()
    phases = []
    if args.phase in {"aug", "all"}:
        phases.append((date(2026, 8, 3), current_date if current_date >= date(2026, 8, 3) else date(2026, 8, 3), "aug"))
    if args.phase in {"july", "all"}:
        phases.append((date(2026, 7, 1), date(2026, 7, 31), "july"))
    if args.start and args.end:
        phases = [(args.start, args.end, "custom")]
    for phase_start, phase_end, phase_name in phases:
        phase_output = args.output / phase_name
        summary = run_replay(start=phase_start, end=phase_end, output_dir=phase_output, max_windows=args.max_windows, data_root=args.data_root)
        print(json.dumps({"phase": phase_name, "output": str(phase_output), "window_count": summary["window_count"], "passed": summary["passed_count"], "failed": summary["failed_count"], "errors": summary["error_count"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
