#!/usr/bin/env python3
"""Migrate Credit Risk to All; optionally purge the retired upload store.

Read-only by default. Run --apply after the new release is ready. Run
--apply --purge-uploads only after web and local-agent upload routes are retired.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bpmis_jira_tool.source_code_qa_factory import build_source_code_qa_service_from_settings
from bpmis_jira_tool.config import Settings


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name('.' + path.name + '.migration.tmp')
    temp.write_text(json.dumps(value, indent=2, sort_keys=True))
    os.replace(temp, path)


def merged_entries(mappings):
    rows = []
    seen = set()
    for country in ['All', 'SG', 'ID', 'PH']:
        entries = mappings.get('CRMS:' + country, [])
        counts = {}
        for entry in entries:
            name = entry['display_name']
            counts[name] = counts.get(name, 0) + 1
        for entry in entries:
            url = entry['url'].strip()
            if url.rstrip('/') in seen:
                continue
            seen.add(url.rstrip('/'))
            name = entry['display_name']
            if country != 'All':
                name = country + ' - ' + name
                if counts[entry['display_name']] > 1:
                    name += ' [' + url.rsplit('/', 2)[-2] + ']'
            rows.append(('CRMS:' + country, entry, {'display_name': name, 'url': url}))
    return rows


def clone_path(source, target):
    """Use an APFS copy-on-write clone while retaining the old usable paths."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == 'darwin':
        args = ['cp', '-c', '-p']
        if source.is_dir():
            args += ['-R']
        subprocess.run([*args, str(source), str(target)], check=True, capture_output=True)
    elif source.is_dir():
        shutil.copytree(source, target, copy_function=shutil.copy2)
    else:
        shutil.copy2(source, target)


def migrate(service, *, apply=False, purge_uploads=False):
    config = service.load_config()
    mappings = config['mappings']
    rows = merged_entries(mappings)
    report = {'applied': apply, 'crms_repositories': len(rows), 'repos': []}
    if not rows:
        raise RuntimeError('No CRMS mapping exists; migration was not applied.')
    for old_key, old, new in rows:
        if service.sync_job_status(old_key).get('status') == 'running':
            raise RuntimeError('A CRMS sync is running; do not migrate active repositories.')
        old_entry = service._normalize_entry(old)
        new_entry = service._normalize_entry(new)
        source = service._repo_path(old_key, old_entry)
        target = service._repo_path('CRMS:All', new_entry)
        info = service._repo_index_info(old_key, old_entry, source)
        if info['state'] != 'ready' or not service._repo_worktree_clean(source):
            raise RuntimeError('Source repository/index is not ready: ' + old_key + '/' + old_entry.display_name)
        revision = service._repo_git_revision(source)
        report['repos'].append({'display_name': new_entry.display_name, 'revision': revision})
        if apply and source != target:
            if not target.exists():
                clone_path(source, target)
            index_target = service._index_path(target)
            if not index_target.exists():
                clone_path(service._index_path(source), index_target)
            migrated = service._repo_index_info('CRMS:All', new_entry, target)
            if migrated['state'] != 'ready' or service._repo_git_revision(target) != revision:
                raise RuntimeError('Migrated repository/index did not validate: ' + new_entry.display_name)
    if apply:
        # Abort if a mapping was saved while the source copies were prepared.
        if service.load_config() != config:
            raise RuntimeError('Mapping changed during migration; no config was replaced.')
        if any(key in mappings for key in ['CRMS:SG', 'CRMS:ID', 'CRMS:PH']):
            backup = service.data_root / 'migrations' / 'crms-before-all-config.json'
            if not backup.exists():
                atomic_json(backup, config)
            mappings = {key: value for key, value in mappings.items() if key not in ['CRMS:SG', 'CRMS:ID', 'CRMS:PH']}
            mappings['CRMS:All'] = [new for _, _, new in rows]
            atomic_json(service.config_path, {**config, 'mappings': mappings, 'updated_at': datetime.now(timezone.utc).isoformat()})
    if apply and service.sync_job_status('CRMS:All').get('status') == 'idle':
        old_jobs = [service.sync_job_status('CRMS:' + c) for c in ['SG', 'ID', 'PH']]
        started = [j['started_at'] for j in old_jobs if j.get('started_at')]
        finished = [j['finished_at'] for j in old_jobs if j.get('finished_at')]
        service._write_sync_job('CRMS:All', {
            'key': 'CRMS:All', 'job_id': 'crms-all-migration', 'status': 'ok',
            'mode': 'index_migration', 'repo_count': len(rows),
            'started_at': min(started) if started else None,
            'finished_at': min(finished) if finished else None,
            'migrated_at': datetime.now(timezone.utc).isoformat(),
            'results': [service._sync_result(service._normalize_entry(new),
                service._repo_path('CRMS:All', service._normalize_entry(new)), 'ok',
                'Existing source and ready index reused; no upstream pull was performed.')
                for _, _, new in rows],
        })
    uploads = service.data_root / 'runtime_evidence'
    files = [p for p in uploads.rglob('*') if p.is_file()] if uploads.exists() else []
    report['retired_upload_files'] = sum(p.name != 'metadata.json' for p in files)
    report['retired_upload_metadata'] = sum(p.name == 'metadata.json' for p in files)
    if apply and purge_uploads:
        if uploads.is_symlink():
            raise RuntimeError('Refusing to purge a symlinked upload root.')
        if uploads.exists():
            shutil.rmtree(uploads)
        # Clear reusable answers and contexts while preserving chat messages and attachments.
        cache = service.data_root / 'answer_cache'
        if cache.exists():
            shutil.rmtree(cache)
        sessions = service.data_root / 'sessions.json'
        if sessions.exists():
            payload = json.loads(sessions.read_text())
            for session in payload.get('sessions', {}).values():
                session['last_context'] = None
                if session.get('pm_team') in {'CRMS', 'GRC'}:
                    session['country'] = 'All'
            atomic_json(sessions, payload)
        report['uploads_purged'] = not uploads.exists()
    if apply:
        # Counts and source revisions only. Never back up the deleted upload contents.
        atomic_json(service.data_root / 'migrations' / 'crms-all-result.json', report)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--purge-uploads', action='store_true')
    args = parser.parse_args()
    service = build_source_code_qa_service_from_settings(Settings.from_env())
    print(json.dumps(migrate(service, apply=args.apply, purge_uploads=args.purge_uploads), indent=2))
