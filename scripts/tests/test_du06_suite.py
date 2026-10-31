"""Harness regressions only: all target/DB operations in these tests are substitutes."""
import asyncio
from contextlib import ExitStack, redirect_stdout
import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
import httpx
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from du06_archive import encoded_line, inspect_archive
from du06_cases import Cases, preserved
from du06_controls import NotExercised
import persistence_qualification as runner
from persistence_ledger import TABLES, verify, capture
from test_persistence_ledger import ledger
from persistence_support import APIReplay
from du06_client import Client


class OutcomeAssertions(unittest.TestCase):
    def test_definitive_ownership_rejection_cannot_hide_as_uncertain(self):
        suite = object.__new__(Cases)
        suite.completed, suite.interrupted = 's', 's'
        suite.c = SimpleNamespace(
            post=Mock(return_value=SimpleNamespace(status_code=409)),
            settled=Mock(return_value={**ledger()['agent_commands'][0], 'status': 'failed'}),
            check_response=Mock(), command_row=Mock(return_value={'status': 'uncertain'}))
        suite.snapshot = Mock(return_value=(ledger(), {}))
        suite.replay = Mock(return_value={})
        suite.rejections = [{'sid': 's', 'key': 'conflict', 'route': '/sessions/s/run',
                             'body': {'max_steps': 1}, 'response': SimpleNamespace(status_code=409)}]
        with self.assertRaisesRegex(AssertionError, 'definitive admitted ownership rejection'):
            suite.c10()
        self.assertEqual(suite.c.post.call_count, 1)
        self.assertEqual(suite.replay.call_count, 1)  # No retry of the improperly unresolved conflict.

    def test_unresolved_status_cannot_claim_terminal_outcome(self):
        for status in ('pending', 'started', 'uncertain', 'blocked'):
            with self.subTest(status=status):
                data = ledger()
                data['agent_commands'][0]['status'] = status
                with self.assertRaises(AssertionError):
                    verify(data)
                data['agent_commands'][0].update(response_status=None, response_body=None, completed_at=None)
                verify(data)

    def test_definitive_failure_requires_durable_response_and_timestamp(self):
        for missing in ('response_status', 'response_body', 'completed_at'):
            data = ledger()
            data['agent_commands'][0]['status'] = 'failed'
            data['agent_commands'][0][missing] = None
            with self.subTest(missing=missing), self.assertRaises(AssertionError):
                verify(data)

    def test_response_change_is_detected_across_snapshots(self):
        before = ledger()
        after = copy.deepcopy(before)
        after['agent_commands'][0]['response_body'] = {'changed': True}
        with self.assertRaises(AssertionError):
            preserved(before, after)

    def test_snapshot_column_revision_must_match_json_revision(self):
        data = ledger()
        data['agent_sessions'][0]['state']['revision'] = 2
        with self.assertRaises(AssertionError):
            verify(data)


class ArchiveAssertions(unittest.TestCase):
    def archive(self):
        sid = str(uuid4())
        header = {'format': 'sda-archive-2', 'session_id': sid, 'archive_id': str(uuid4()), 'snapshot_revision': 3}
        record = {'table': 'agent_session_events', 'row': {'session_id': sid, 'event_id': 7, 'event': {'type': 'status'}}}
        hashes = {name: hashlib.sha256() for name in TABLES}
        hashes[record['table']].update(encoded_line(record))
        payload = encoded_line(header)+encoded_line(record)
        manifest = {'sha256': hashlib.sha256(payload).hexdigest(),
                    'table_sha256': {k: v.hexdigest() for k, v in hashes.items()},
                    'counts': {k: int(k == record['table']) for k in TABLES},
                    'watermarks': {'events': 7, 'diagnostics': {}}}
        return payload+encoded_line({'manifest': manifest})

    def test_valid_archive_and_tampering_truncation_trailing_data(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'archive.jsonl'
            original = self.archive()
            path.write_bytes(original)
            self.assertEqual(inspect_archive(path)[1]['counts']['agent_session_events'], 1)
            for bad in (original.replace(b'"status"', b'"edited"'), original.splitlines(keepends=True)[0],
                        original+b'{}\n'):
                path.write_bytes(bad)
                with self.subTest(bad=bad[:50]), self.assertRaises((AssertionError, ValueError)):
                    inspect_archive(path)

    def test_cross_session_archive_record_is_rejected(self):
        content = self.archive().splitlines()
        record = json.loads(content[1])
        record['row']['session_id'] = str(uuid4())
        content[1] = encoded_line(record).rstrip(b'\n')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bad.jsonl'
            path.write_bytes(b'\n'.join(content)+b'\n')
            with self.assertRaises(AssertionError):
                inspect_archive(path)



class ReadOnlyConnections(unittest.TestCase):
    def test_snapshot_and_queries_preserve_search_path_and_enforce_read_only(self):
        calls = []
        class Connection:
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def execute(self, query, params=()):
                calls.append(query)
                return self
            def fetchall(self):
                return []
            def __iter__(self):
                return iter(())
        dsn = "dbname=unit options='-c search_path=isolated'"
        with patch('psycopg.connect', return_value=Connection()) as connect, tempfile.TemporaryDirectory() as directory:
            rows, evidence = capture(dsn, 'unit', Path(directory) / 'snapshot.json')
            self.assertEqual(len(rows), 12)
            options = connect.call_args.kwargs['options']
            self.assertIn('search_path=isolated', options)
            self.assertIn('default_transaction_read_only=on', options)
            self.assertIn('REPEATABLE READ READ ONLY', calls[0])
            probe = APIReplay('http://unused', dsn)
            try:
                probe.rows('SELECT 1')
                self.assertIn('default_transaction_read_only=on', connect.call_args.kwargs['options'])
            finally:
                probe.close()
        self.assertTrue(all(query.startswith(('SELECT ', 'SET ')) for query in calls))



class RequestEvidence(unittest.TestCase):
    def test_every_created_session_is_retained_even_for_unexpected_replay_response(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client('http://unit', 'dbname=unused', directory, 5)
            client.http.close()
            client.http = httpx.Client(base_url='http://unit', transport=httpx.MockTransport(lambda request: httpx.Response(
                201, json={'session': {'id': 'unexpected-session'}})))
            try:
                client.post('/sessions', {'user_prompt': 'unit'}, 'same-key')
                self.assertEqual(client.sessions, ['unexpected-session'])
                self.assertEqual(client.requests[0]['key'], 'same-key')
                self.assertTrue(Path(client.requests[0]['evidence_path']).is_file())
            finally:
                client.close()

    def test_missing_response_retains_request_key_and_failure_evidence(self):
        def disconnected(request):
            raise httpx.ReadError('unit disconnect', request=request)
        with tempfile.TemporaryDirectory() as directory:
            client = Client('http://unit', 'dbname=unused', directory, 5)
            client.http.close()
            client.http = httpx.Client(base_url='http://unit', transport=httpx.MockTransport(disconnected))
            try:
                with self.assertRaises(httpx.ReadError):
                    client.post('/sessions', {'user_prompt': 'unit'}, 'retained-key')
                self.assertEqual(client.requests[0]['key'], 'retained-key')
                self.assertEqual(client.requests[0]['transport_error'], 'ReadError')
                self.assertTrue(Path(client.requests[0]['evidence_path']).is_file())
            finally:
                client.close()


class FakeClient:
    def __init__(self, *_):
        self.requests, self.snapshots, self.observations, self.sessions = [], [], [], []
        self.http = SimpleNamespace(get=lambda *_: SimpleNamespace(status_code=200, json=lambda: {'contract_version': '2.0.0', 'storage_schema': 6}))
    def close(self):
        pass


class RunnerAccounting(unittest.TestCase):
    def run_case(self, failure=None, exception=None, setup_error=None):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory)
            for name in ('persist.sh', 'scripts/pyproject.toml', 'scripts/uv.lock', 'schema/postgres.sql',
                         'schema/layout-v6.json', 'schema/idempotency-vectors.json'):
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text('unit fixture')
            class FakeCases:
                def __init__(self, *_args, **_kwargs):
                    self.subchecks = {}
            for index in range(1, 13):
                def method(self, index=index):
                    if index == failure:
                        raise exception('unit failure')
                    return {'case': index}
                setattr(FakeCases, f'c{index:02d}', method)
            stack.enter_context(patch.object(runner, 'ROOT', root))
            stack.enter_context(patch.object(runner, 'Client', FakeClient))
            stack.enter_context(patch.object(runner, 'Cases', FakeCases))
            stack.enter_context(patch.object(runner, 'dotenv_values', return_value={'AGENT_SESSION_DATABASE_URL': 'unused'}))
            stack.enter_context(patch.object(runner, 'setup_checks', AsyncMock(return_value={'schema': 'unit'}, side_effect=setup_error)))
            stack.enter_context(patch.object(sys, 'argv', ['runner', '--non-interactive']))
            stack.enter_context(redirect_stdout(io.StringIO()))
            if exception is KeyboardInterrupt:
                with self.assertRaises(KeyboardInterrupt):
                    runner.main()
                code = None
            else:
                code = runner.main()
            result = json.loads(next((root / 'archive/persistence').glob('*/result.json')).read_text())
            return code, result

    def test_all_twelve_declared_cases_must_pass(self):
        code, result = self.run_case()
        self.assertEqual(code, 0)
        self.assertEqual(result['coverage'], {'passed': 12, 'failed': 0, 'not_exercised': 0, 'required': 12})
        self.assertEqual(set(result['cases']), {f'DU-06-V2-C{i:02d}' for i in range(1, 13)})
        self.assertEqual(result['agent_conformance'], 'not_assessed')

    def test_unexercised_operator_step_prevents_pass(self):
        code, result = self.run_case(8, NotExercised)
        self.assertEqual(code, 2)
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result['cases']['DU-06-V2-C08']['status'], 'not_exercised')

    def test_failed_assertion_prevents_pass(self):
        code, result = self.run_case(7, AssertionError)
        self.assertEqual(code, 1)
        self.assertEqual(result['status'], 'failed')

    def test_interrupted_suite_never_reports_pass(self):
        _, result = self.run_case(7, KeyboardInterrupt)
        self.assertEqual(result['status'], 'incomplete')
        self.assertLess(result['coverage']['passed'], 12)

    def test_failed_schema_validation_starts_no_cases(self):
        code, result = self.run_case(setup_error=RuntimeError('invalid schema'))
        self.assertEqual(code, 1)
        self.assertEqual(result['coverage']['passed'], 0)
        self.assertIn('invalid schema', result['harness_error'])


if __name__ == '__main__':
    unittest.main()
