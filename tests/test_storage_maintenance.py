from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from bpmis_jira_tool.meeting_recorder import MeetingRecordStore
from scripts.meeting_recorder_compress_audio import compress_old_meeting_audio
from scripts.source_code_qa_rebuild_indexes import _scan_old_index_backups, _scan_stale_temp_index_files


class SourceCodeQAIndexCleanupTests(unittest.TestCase):
    def test_scan_stale_temp_index_files_only_includes_old_temp_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stale = root / "repo.sqlite3.tmp"
            fresh = root / "fresh.sqlite3.tmp"
            regular = root / "repo.sqlite3"
            stale.write_bytes(b"old-temp")
            fresh.write_bytes(b"fresh-temp")
            regular.write_bytes(b"index")
            old_time = (datetime.now(timezone.utc) - timedelta(hours=8)).timestamp()
            os.utime(stale, (old_time, old_time))

            result = _scan_stale_temp_index_files(root, max_age_hours=6)

        self.assertEqual(result["stale_temp_count"], 1)
        self.assertEqual(Path(result["stale_temp_files"][0]["path"]).name, stale.name)

    def test_scan_old_index_backups_keeps_newest_n(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backups = []
            for index in range(3):
                path = root / f"indexes.backup.v30.2026050{index}T000000Z"
                path.mkdir()
                (path / "index.sqlite3").write_bytes(b"x" * (index + 1))
                timestamp = (datetime.now(timezone.utc) - timedelta(days=index)).timestamp()
                os.utime(path, (timestamp, timestamp))
                backups.append(path)

            result = _scan_old_index_backups(root, keep_backups=1)

        self.assertEqual(result["kept_backup_count"], 1)
        self.assertEqual(result["old_backup_count"], 2)
        self.assertNotIn(str(backups[0]), {item["path"] for item in result["old_backup_dirs"]})


class MeetingRecorderAudioCompressionTests(unittest.TestCase):
    def test_compress_old_caf_updates_metadata_and_deletes_original(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            store = MeetingRecordStore(data_root / "meeting_records")
            record = store.create_record(
                owner_email="owner@npt.sg",
                title="Review",
                platform="zoom",
                meeting_link="https://zoom.us/j/123",
            )
            record["status"] = "completed"
            record_dir = store.record_dir(record["record_id"])
            caf_path = record_dir / "screencapture-system.caf"
            caf_path.write_bytes(b"raw-audio" * 128)
            old_time = (datetime.now(timezone.utc) - timedelta(days=4)).timestamp()
            os.utime(caf_path, (old_time, old_time))
            record["media"] = {"system_audio_path": str(caf_path.relative_to(store.root_dir))}
            store.save_record(record)

            def fake_compress(**kwargs):
                output_path = kwargs["output_path"]
                output_path.write_bytes(b"compressed")
                return Mock(returncode=0, stdout="", stderr="")

            with patch("scripts.meeting_recorder_compress_audio._run_ffmpeg_compress", side_effect=fake_compress), patch(
                "scripts.meeting_recorder_compress_audio._probe_duration_seconds",
                return_value=120.0,
            ):
                result = compress_old_meeting_audio(
                    data_root=data_root,
                    ffmpeg_bin="ffmpeg",
                    min_age_days=3,
                    dry_run=False,
                    delete_original=True,
                )

            loaded = store.get_record(record["record_id"])
            media = loaded["media"]

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["compressed_count"], 1)
            self.assertFalse(caf_path.exists())
            self.assertTrue((record_dir / "screencapture-system.m4a").exists())
            self.assertEqual(media["system_audio_path"], f"records/{record['record_id']}/screencapture-system.m4a")
            self.assertEqual(media["raw_audio_compression"][0]["source_path"], f"records/{record['record_id']}/screencapture-system.caf")

    def test_compress_old_caf_dry_run_keeps_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir)
            store = MeetingRecordStore(data_root / "meeting_records")
            record_dir = store.records_dir / "meeting-20260501000000-abcdef"
            record_dir.mkdir(parents=True)
            caf_path = record_dir / "screencapture-microphone.caf"
            caf_path.write_bytes(b"raw-audio")
            old_time = (datetime.now(timezone.utc) - timedelta(days=4)).timestamp()
            os.utime(caf_path, (old_time, old_time))

            result = compress_old_meeting_audio(data_root=data_root, ffmpeg_bin="ffmpeg", min_age_days=3, dry_run=True)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["candidate_count"], 1)
            self.assertTrue(caf_path.exists())


if __name__ == "__main__":
    unittest.main()
