import base64
import io
import json
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch
import zipfile

from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.local_agent_protocol import sign_headers
from bpmis_jira_tool.local_agent_server import create_local_agent_app
from bpmis_jira_tool.source_code_qa_scope import current_source_context
from bpmis_jira_tool.source_code_qa import RepositoryEntry
from bpmis_jira_tool import source_code_qa_repo_downloads as bundles
from scripts.migrate_source_code_qa_all import migrate, merged_entries
from tests import test_source_code_qa_market_teams as market_fixtures


class RiskAllTests(unittest.TestCase):
    setUp = market_fixtures.MarketTeamTests.setUp
    login = staticmethod(market_fixtures.MarketTeamTests.login)
    seed = market_fixtures.MarketTeamTests.seed

    def test_country_and_download_compatibility(self):
        options = {item['code']: item for item in self.service.options_payload()['pm_teams']}
        for team in ['CRMS', 'GRC']:
            self.assertTrue(options[team]['all_country_only'])
            for country in ['SG', 'ID', 'PH', 'All', '']:
                self.assertEqual(self.service.mapping_key(team, country), team + ':All')
        self.assertFalse(options['AF']['all_country_only'])
        crms = [scope for scope in bundles.repo_download_scope_definitions() if scope['scope_key'].startswith('CRMS:')]
        self.assertEqual([scope['scope_key'] for scope in crms], ['CRMS:All'])
        self.assertEqual(crms[0]['label'], 'CRMS-All')
        for scope in ['CRMS:SG', 'CRMS:ID', 'CRMS:PH', 'Credit Risk-SG', 'credit-risk-ph']:
            self.assertEqual(bundles.resolve_repo_download_scope(scope), crms[0])

    def test_retired_upload_routes_require_auth_and_never_write(self):
        root = self.service.data_root / 'runtime_evidence'
        with self.app.test_client() as client:
            self.assertEqual(client.post('/api/source-code-qa/runtime-evidence').status_code, 401)
            self.login(client)
            for method, path in [('get', '/api/source-code-qa/runtime-evidence?pm_team=CRMS&country=All'),
                                 ('post', '/api/source-code-qa/runtime-evidence'),
                                 ('delete', '/api/source-code-qa/runtime-evidence/old?pm_team=AF&country=SG')]:
                self.assertEqual(getattr(client, method)(path).status_code, 410)
            html = client.get('/source-code-qa').get_data(as_text=True)
            self.assertNotIn('DB / Apollo Uploads', html)
            self.assertNotIn('data-source-runtime-evidence-input', html)
            self.assertIn('data-source-attachment-input', html)
            self.assertEqual(client.post('/api/source-code-qa/query', json={
                'pm_team': 'AF', 'country': 'All', 'runtime_evidence': [{'text': 'old upload'}],
            }).status_code, 400)
        self.assertFalse(root.exists())

    def test_signed_agent_routes_are_retired_after_signature_check(self):
        with patch.dict(os.environ, {'ENV_FILE': os.devnull, 'TEAM_PORTAL_DATA_DIR': self.temp.name,
                                    'LOCAL_AGENT_HMAC_SECRET': 'test-secret'}, clear=True):
            app = create_local_agent_app()
            with app.test_client() as client:
                for action in ['list', 'save', 'resolve', 'delete']:
                    route = '/api/local-agent/source-code-qa/runtime-evidence/' + action
                    body = b'{"pm_team":"AF","country":"SG"}'
                    self.assertEqual(client.post(route, data=body).status_code, 401)
                    headers = sign_headers(secret='test-secret', method='POST', path=route, body=body)
                    headers['Content-Type'] = 'application/json'
                    self.assertEqual(client.post(route, data=body, headers=headers).status_code, 410)

    def test_bundle_excludes_upload_store_but_keeps_repo_configuration(self):
        self.seed('CRMS')
        uploads = self.service.data_root / 'runtime_evidence' / 'CRMS' / 'SG'
        uploads.mkdir(parents=True)
        (uploads / 'secret-upload.sql').write_text('UPLOAD_SENTINEL_DO_NOT_PACKAGE')
        _, content = bundles.build_repo_download_zip(self.service, 'CRMS:SG')
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            self.assertIsNone(archive.testzip())
            self.assertTrue(any(name.endswith('/application.yml') for name in archive.namelist()))
            self.assertFalse(any('secret-upload' in name or 'runtime_evidence' in name for name in archive.namelist()))
            self.assertFalse(any(b'UPLOAD_SENTINEL' in archive.read(name) for name in archive.namelist()))
        self.assertLess(len(content), 20_000_000)
        with self.assertRaisesRegex(ToolError, 'removed'):
            self.service.query(pm_team='CRMS', country='SG', question='q', runtime_evidence=[{'text': 'old'}])

    def test_old_context_is_discarded_current_context_can_resume(self):
        self.assertIsNone(current_source_context({'codex_cli_session': {'session_id': 'old'}}))
        current = {'source_context_version': 1, 'question': 'source only'}
        self.assertIs(current_source_context(current), current)

    def test_explicit_country_repository_phrase_selects_that_countrys_repositories(self):
        repos = [RepositoryEntry('ID - credit-risk-management', 'https://git.example/id/credit-risk-management.git'),
                 RepositoryEntry('ID - credit-risk', 'https://git.example/id/credit-risk.git'),
                 RepositoryEntry('SG - credit-risk [backend]', 'https://git.example/sg/backend/credit-risk.git'),
                 RepositoryEntry('SG - credit-risk [frontend]', 'https://git.example/sg/frontend/credit-risk.git'),
                 RepositoryEntry('PH - credit-risk', 'https://git.example/ph/credit-risk.git')]
        for country in ['SG', 'ID', 'PH']:
            selected, _ = self.service._filter_entries_for_question_repository_scope(
                f'Which {country} Credit Risk API runs precheck?', repos)
            self.assertTrue(selected)
            self.assertTrue(all(entry.display_name.startswith(country) for entry in selected))
        selected, _ = self.service._filter_entries_for_question_repository_scope('Where is the customer id column?', repos)
        self.assertEqual(selected, repos)

    def test_oversized_bundle_keeps_source_from_each_repository(self):
        manifest = {'selection': {}, 'repos': []}
        full = io.BytesIO()
        with zipfile.ZipFile(full, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            for repo in ['A', 'B', 'C']:
                files = []
                for number in range(4):
                    path = f'{repo}/src/{number}.py'
                    content = base64.b64encode(os.urandom(2048))
                    files.append({'path': path, 'bytes': len(content)})
                    archive.writestr(path, content)
                item = {'archive_root': repo, 'display_name': repo, 'source_files': len(files),
                        'file_index': f'indexes/{repo}.files.jsonl',
                        'symbol_index': f'indexes/{repo}.symbols.jsonl'}
                manifest['repos'].append(item)
                archive.writestr(item['file_index'], bundles._json_lines(files))
                archive.writestr(item['symbol_index'], '')
            archive.writestr('manifest.json', json.dumps(manifest))
        with patch.object(bundles, 'MAX_BUNDLE_BYTES', 12_000):
            content = bundles._fit_bundle_to_limit(full.getvalue(), manifest)
        self.assertLess(len(content), 12_000)
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            self.assertIsNone(archive.testzip())
            repos = json.loads(archive.read('manifest.json'))['repos']
            self.assertEqual(len(repos), 3)
            self.assertTrue(all(repo['source_files'] > 0 for repo in repos))

    def test_migration_preserves_indexes_and_history_and_purges_all_uploads(self):
        entries = {
            'CRMS:SG': [{'display_name': 'risk', 'url': 'https://git.example/sg/backend/risk.git'},
                        {'display_name': 'risk', 'url': 'https://git.example/sg/frontend/risk.git'}],
            'CRMS:ID': [{'display_name': 'risk', 'url': 'https://git.example/id/risk.git'}],
        }
        self.service.data_root.mkdir(parents=True, exist_ok=True)
        self.service.config_path.write_text(json.dumps({'version': 1, 'mappings': entries}))
        for key, rows in entries.items():
            for raw in rows:
                entry = self.service._normalize_entry(raw)
                repo = self.service._repo_path(key, entry)
                repo.mkdir(parents=True)
                (repo / 'logic.py').write_text('def decide():\n    return True\n')
                subprocess.run(['git', 'init', '-q', str(repo)], check=True)
                subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
                subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-qm', 'fixture'], check=True)
                self.service._build_repo_index(key, entry, repo)
        for team in ['AF', 'CRMS', 'GRC']:
            folder = self.service.data_root / 'runtime_evidence' / team / 'SG'
            folder.mkdir(parents=True)
            (folder / 'old.txt').write_text('old upload')
            (folder / 'metadata.json').write_text('{"evidence": {}}')
        sessions = self.service.data_root / 'sessions.json'
        sessions.write_text(json.dumps({'sessions': {'old': {'pm_team': 'CRMS', 'country': 'SG',
            'messages': [{'answer': 'history'}], 'last_context': {'question': 'old evidence'}}}}))
        preview = migrate(self.service)
        self.assertFalse(preview['applied'])
        self.assertIn('CRMS:SG', self.service.load_config()['mappings'])
        result = migrate(self.service, apply=True, purge_uploads=True)
        self.assertEqual(result['retired_upload_files'], 3)
        self.assertFalse((self.service.data_root / 'runtime_evidence').exists())
        self.assertEqual(set(self.service.load_config()['mappings']), {'CRMS:All'})
        migrated = self.service._load_entries_for_key('CRMS:All')
        self.assertEqual(len({entry.display_name for entry in migrated}), 3)
        for entry in migrated:
            self.assertEqual(self.service._repo_index_info('CRMS:All', entry, self.service._repo_path('CRMS:All', entry))['state'], 'ready')
        history = json.loads(sessions.read_text())['sessions']['old']
        self.assertIsNone(history['last_context'])
        self.assertEqual(history['messages'], [{'answer': 'history'}])
        self.assertEqual(history['country'], 'All')
        self.assertEqual(migrate(self.service, apply=True)['crms_repositories'], 3)
