#!/usr/bin/env python3
"""Compress old Meeting Recorder ScreenCaptureKit raw CAF files."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.meeting_recorder import MeetingRecordStore


RAW_AUDIO_FIELDS = ("system_audio_path", "microphone_audio_path")
DEFAULT_MIN_AGE_DAYS = 3
DEFAULT_SYSTEM_BITRATE = "128k"
DEFAULT_MICROPHONE_BITRATE = "64k"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _relative_to_root(path: Path, root: Path) -> str:
    return str(path.resolve().relative_to(root.resolve()))


def _safe_resolve_under(root: Path, relative_path: str) -> Path | None:
    if not relative_path:
        return None
    path = (root / relative_path).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        return None
    return path


def _probe_duration_seconds(ffprobe_bin: str, path: Path) -> float:
    completed = subprocess.run(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        return 0.0
    try:
        return float((completed.stdout or "").strip() or "0")
    except ValueError:
        return 0.0


def _run_ffmpeg_compress(
    *,
    ffmpeg_bin: str,
    source_path: Path,
    output_path: Path,
    bitrate: str,
    timeout_seconds: int,
) -> subprocess.CompletedProcess[str]:
    temp_path = output_path.with_name(f".{output_path.name}.tmp.m4a")
    temp_path.unlink(missing_ok=True)
    command = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(source_path),
        "-vn",
        "-c:a",
        "aac",
        "-b:a",
        bitrate,
        "-movflags",
        "+faststart",
        str(temp_path),
    ]
    completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout_seconds)
    if completed.returncode == 0:
        temp_path.replace(output_path)
    else:
        temp_path.unlink(missing_ok=True)
    return completed


def _record_finished(record: dict[str, Any]) -> bool:
    status = str(record.get("status") or "").strip().lower()
    return status not in {"recording", "processing"}


def _iter_record_metadata(store: MeetingRecordStore) -> list[tuple[Path, dict[str, Any]]]:
    records: list[tuple[Path, dict[str, Any]]] = []
    for metadata_path in sorted(store.records_dir.glob("*/metadata.json")):
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            records.append((metadata_path, payload))
    return records


def _metadata_raw_audio_paths(store: MeetingRecordStore, record: dict[str, Any]) -> dict[Path, str]:
    media = record.get("media") if isinstance(record.get("media"), dict) else {}
    paths: dict[Path, str] = {}
    for field in RAW_AUDIO_FIELDS:
        path = _safe_resolve_under(store.root_dir, str(media.get(field) or ""))
        if path is not None and path.suffix.lower() == ".caf":
            paths[path] = field
    return paths


def _candidate_raw_audio_files(store: MeetingRecordStore) -> list[dict[str, Any]]:
    candidates: dict[Path, dict[str, Any]] = {}
    for metadata_path, record in _iter_record_metadata(store):
        if not _record_finished(record):
            continue
        for path, field in _metadata_raw_audio_paths(store, record).items():
            candidates[path] = {
                "path": path,
                "field": field,
                "record": record,
                "metadata_path": metadata_path,
            }
    for path in sorted(store.records_dir.glob("*/screencapture-*.caf")):
        candidates.setdefault(path.resolve(), {"path": path.resolve(), "field": "", "record": None, "metadata_path": None})
    return list(candidates.values())


def _bitrate_for_path(path: Path, *, system_bitrate: str, microphone_bitrate: str) -> str:
    name = path.name.lower()
    if "microphone" in name or "mic" in name:
        return microphone_bitrate
    return system_bitrate


def _eligible_by_age(path: Path, *, min_age_days: float, now: datetime | None = None) -> bool:
    now = now or _utc_now()
    try:
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    except OSError:
        return False
    return modified_at <= now - timedelta(days=max(float(min_age_days), 0.0))


def compress_old_meeting_audio(
    *,
    data_root: Path,
    ffmpeg_bin: str,
    ffprobe_bin: str | None = None,
    min_age_days: float = DEFAULT_MIN_AGE_DAYS,
    system_bitrate: str = DEFAULT_SYSTEM_BITRATE,
    microphone_bitrate: str = DEFAULT_MICROPHONE_BITRATE,
    delete_original: bool = True,
    dry_run: bool = True,
    timeout_seconds: int = 7200,
) -> dict[str, Any]:
    meeting_root = data_root / "meeting_records"
    store = MeetingRecordStore(meeting_root)
    ffmpeg_path = shutil.which(ffmpeg_bin) or ffmpeg_bin
    ffprobe_path = shutil.which(ffprobe_bin or "ffprobe") or (ffprobe_bin or "ffprobe")
    compressed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[str] = []
    now = _utc_now()
    for candidate in _candidate_raw_audio_files(store):
        source_path = Path(candidate["path"])
        if not source_path.exists() or not source_path.is_file():
            continue
        try:
            source_bytes = source_path.stat().st_size
        except OSError:
            continue
        output_path = source_path.with_suffix(".m4a")
        if not _eligible_by_age(source_path, min_age_days=min_age_days, now=now):
            skipped.append({"path": str(source_path), "reason": "not_old_enough", "bytes": source_bytes})
            continue
        if output_path.exists() and output_path.stat().st_size > 0:
            skipped.append({"path": str(source_path), "reason": "compressed_output_exists", "bytes": source_bytes, "output_path": str(output_path)})
            continue
        bitrate = _bitrate_for_path(source_path, system_bitrate=system_bitrate, microphone_bitrate=microphone_bitrate)
        item = {
            "path": str(source_path),
            "output_path": str(output_path),
            "bytes": source_bytes,
            "bitrate": bitrate,
            "field": str(candidate.get("field") or ""),
        }
        if dry_run:
            compressed.append({**item, "status": "dry_run"})
            continue
        before_duration = _probe_duration_seconds(ffprobe_path, source_path)
        try:
            completed = _run_ffmpeg_compress(
                ffmpeg_bin=ffmpeg_path,
                source_path=source_path,
                output_path=output_path,
                bitrate=bitrate,
                timeout_seconds=timeout_seconds,
            )
        except (OSError, subprocess.SubprocessError) as error:
            errors.append(f"{source_path}: {type(error).__name__}: {error}")
            continue
        if completed.returncode != 0:
            errors.append(f"{source_path}: ffmpeg failed: {(completed.stderr or completed.stdout or '').strip()[:500]}")
            continue
        if not output_path.exists() or output_path.stat().st_size <= 0:
            errors.append(f"{source_path}: compressed output is missing or empty")
            continue
        after_duration = _probe_duration_seconds(ffprobe_path, output_path)
        if before_duration > 0 and after_duration > 0:
            tolerance = max(3.0, before_duration * 0.02)
            if abs(before_duration - after_duration) > tolerance:
                output_path.unlink(missing_ok=True)
                errors.append(f"{source_path}: compressed duration changed from {before_duration:.2f}s to {after_duration:.2f}s")
                continue
        output_bytes = output_path.stat().st_size
        metadata_path = candidate.get("metadata_path")
        record = candidate.get("record")
        if isinstance(record, dict) and isinstance(metadata_path, Path):
            media = record.get("media") if isinstance(record.get("media"), dict) else {}
            media = dict(media)
            field = str(candidate.get("field") or "")
            if field:
                media[field] = _relative_to_root(output_path, store.root_dir)
            history = list(media.get("raw_audio_compression") or [])
            history.append(
                {
                    "compressed_at": now.isoformat(),
                    "source_path": _relative_to_root(source_path, store.root_dir),
                    "output_path": _relative_to_root(output_path, store.root_dir),
                    "source_bytes": source_bytes,
                    "output_bytes": output_bytes,
                    "codec": "aac",
                    "bitrate": bitrate,
                }
            )
            media["raw_audio_compression"] = history[-20:]
            record["media"] = media
            store.save_record(record)
        if delete_original:
            source_path.unlink(missing_ok=True)
        compressed.append(
            {
                **item,
                "status": "compressed",
                "output_bytes": output_bytes,
                "saved_bytes": max(source_bytes - output_bytes, 0),
                "source_deleted": bool(delete_original),
            }
        )
    return {
        "status": "ok" if not errors else "failed",
        "mode": "dry-run" if dry_run else "apply",
        "data_root": str(data_root),
        "meeting_root": str(meeting_root),
        "min_age_days": min_age_days,
        "compressed_count": len([item for item in compressed if item.get("status") == "compressed"]),
        "candidate_count": len(compressed),
        "candidate_bytes": sum(int(item.get("bytes") or 0) for item in compressed),
        "saved_bytes": sum(int(item.get("saved_bytes") or 0) for item in compressed),
        "skipped_count": len(skipped),
        "compressed": compressed,
        "skipped": skipped,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compress old Meeting Recorder ScreenCaptureKit raw CAF files.")
    parser.add_argument("--data-root", help="Override TEAM_PORTAL_DATA_DIR.")
    parser.add_argument("--min-age-days", type=float, default=DEFAULT_MIN_AGE_DAYS, help="Compress CAF files older than this many days. Default: 3.")
    parser.add_argument("--ffmpeg-bin", default=None, help="ffmpeg executable. Defaults to MEETING_RECORDER_FFMPEG_BIN.")
    parser.add_argument("--ffprobe-bin", default=None, help="ffprobe executable. Default: ffprobe on PATH.")
    parser.add_argument("--system-bitrate", default=DEFAULT_SYSTEM_BITRATE, help="AAC bitrate for system audio. Default: 128k.")
    parser.add_argument("--microphone-bitrate", default=DEFAULT_MICROPHONE_BITRATE, help="AAC bitrate for microphone audio. Default: 64k.")
    parser.add_argument("--timeout-seconds", type=int, default=7200, help="Per-file ffmpeg timeout. Default: 7200.")
    parser.add_argument("--apply", action="store_true", help="Compress and delete original CAF files. Default is dry-run.")
    parser.add_argument("--keep-original", action="store_true", help="Keep the source CAF after compression.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = replace(settings, team_portal_data_dir=Path(args.data_root).expanduser().resolve())
    result = compress_old_meeting_audio(
        data_root=settings.team_portal_data_dir,
        ffmpeg_bin=args.ffmpeg_bin or settings.meeting_recorder_ffmpeg_bin,
        ffprobe_bin=args.ffprobe_bin,
        min_age_days=args.min_age_days,
        system_bitrate=args.system_bitrate,
        microphone_bitrate=args.microphone_bitrate,
        delete_original=not args.keep_original,
        dry_run=not args.apply,
        timeout_seconds=args.timeout_seconds,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"Meeting Recorder audio compression: {result['status']} mode={result['mode']}")
        print(f"data_root={result['data_root']}")
        print(f"candidates={result['candidate_count']} bytes={result['candidate_bytes']}")
        print(f"compressed={result['compressed_count']} saved_bytes={result['saved_bytes']}")
        print(f"skipped={result['skipped_count']}")
        for item in result["compressed"]:
            print(f"{str(item.get('status') or '').upper()}: {item['path']} -> {item['output_path']} bytes={item['bytes']}")
        for error in result["errors"]:
            print(f"ERROR: {error}", file=sys.stderr)
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
