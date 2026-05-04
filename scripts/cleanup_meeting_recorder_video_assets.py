from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.meeting_recorder import (
    MeetingRecordStore,
    MeetingRecorderConfig,
    cleanup_legacy_video_assets,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert legacy Meeting Recorder video records to audio-only records.")
    parser.add_argument("--data-root", default="", help="Override the team portal data root. Defaults to Settings.team_portal_data_dir.")
    parser.add_argument("--owner-email", default="", help="Optional owner email filter.")
    args = parser.parse_args()

    settings = Settings.from_env()
    data_root = Path(args.data_root).expanduser() if args.data_root else settings.team_portal_data_dir
    store = MeetingRecordStore(data_root / "meeting_records")
    config = MeetingRecorderConfig(
        ffmpeg_bin=settings.meeting_recorder_ffmpeg_bin,
        audio_input=settings.meeting_recorder_audio_input,
        whisper_cpp_bin=settings.meeting_recorder_whisper_cpp_bin,
        whisper_model=settings.meeting_recorder_whisper_model,
        whisper_language=settings.meeting_recorder_whisper_language,
        transcript_segment_workers=settings.meeting_recorder_transcript_segment_workers,
        whisper_threads=settings.meeting_recorder_whisper_threads,
    )
    summary = cleanup_legacy_video_assets(store=store, config=config, owner_email=args.owner_email)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
