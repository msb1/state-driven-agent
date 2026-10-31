"""Isolated assertion regressions: no PostgreSQL or agent connections."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from persistence_ledger import TABLES, immutable_prefix, verify
from idempotency_contract import action_key, payload_fingerprint, request_fingerprint


def ledger():
    rows = {table: [] for table in TABLES}
    rows['agent_sessions'] = [{'session_id': 's', 'revision': 1, 'last_commit_id': 'commit',
                              'last_commit_sha256': 'hash', 'storage_version': 6,
                              'state': {'id': 's', 'revision': 1, 'goal': 'goal', 'next_transcript_sequence': 2}}]
    rows['agent_session_context'] = [{'revision': 1, 'goal_message_id': 'goal-id',
                                     'raw_message_ids': ['goal-id'], 'ledger_record_id': None}]
    rows['agent_commands'] = [{'command_id': 'command', 'status': 'succeeded', 'response_body': {},
        'response_status': 200, 'completed_at': 'settled', 'owner': None, 'lease_expires_at': None,
        'method': 'POST', 'route': '/unit', 'request': {'input': {}},
        'request_sha256': request_fingerprint('POST', '/unit', {})}]
    rows['agent_commit_receipts'] = [{'commit_id': 'commit', 'fingerprint': 'hash',
                                     'command_id': 'command', 'expected_revision': 0, 'committed_revision': 1}]
    rows['agent_session_transcript'] = [{'sequence': 1, 'message_id': 'goal-id', 'payload': {'content': 'goal'}}]
    return rows


class LedgerAssertions(unittest.TestCase):
    def test_valid_snapshot(self):
        self.assertEqual(verify(ledger())['revision'], 1)

    def test_corrupt_relationships_are_rejected(self):
        mutations = [
            ('agent_session_context', 'revision', 2),
            ('agent_session_context', 'raw_message_ids', ['missing']),
            ('agent_commit_receipts', 'expected_revision', 4),
            ('agent_commit_receipts', 'command_id', 'missing'),
            ('agent_sessions', 'last_commit_id', 'missing'),
            ('agent_session_transcript', 'sequence', 2),
            ('agent_session_transcript', 'payload', {'content': 'changed'}),
        ]
        for table, key, value in mutations:
            with self.subTest(table=table, key=key):
                rows = ledger()
                rows[table][0][key] = value
                with self.assertRaises(AssertionError):
                    verify(rows)

    def test_event_requires_matching_commit(self):
        rows = ledger()
        rows['agent_session_events'] = [{'commit_id': 'commit', 'command_id': 'command', 'revision': 2}]
        with self.assertRaises(AssertionError):
            verify(rows)

    def test_diagnostic_counter_requires_all_rows(self):
        rows = ledger()
        rows['agent_sessions'][0]['state']['diagnostic_counts'] = {'compaction_attempts': 1}
        with self.assertRaises(AssertionError):
            verify(rows)

    def test_prefix_allows_append_and_rejects_edits_and_deletions(self):
        before = ledger()
        after = copy.deepcopy(before)
        after['agent_session_transcript'].append({'message_id': 'later', 'payload': {}})
        immutable_prefix(before, after)
        after['agent_session_transcript'][0]['payload'] = {'content': 'edited'}
        with self.assertRaises(AssertionError):
            immutable_prefix(before, after)
        after['agent_session_transcript'] = []
        with self.assertRaises(AssertionError):
            immutable_prefix(before, after)

    def test_verification_attempt_can_belong_to_resume_command(self):
        rows = ledger()
        sid, eid = '11111111-1111-4111-8111-111111111111', '22222222-2222-4222-8222-222222222222'
        rows['agent_sessions'][0]['session_id'] = sid
        rows['agent_sessions'][0]['state']['id'] = sid
        rows['agent_commands'].append({**rows['agent_commands'][0], 'command_id': 'resume'})
        rows['agent_runs'] = [{'run_id': 'run', 'command_id': 'command', 'status': 'interrupted', 'ended_at': 'settled'}]
        fingerprint = payload_fingerprint('fixture', 'fixture://recipient', {})
        rows['agent_tool_executions'] = [{'execution_id': eid, 'run_id': 'run', 'command_id': 'command',
            'namespace': 'namespace', 'tool': 'fixture', 'target': 'fixture://recipient', 'arguments': {},
            'payload_sha256': fingerprint, 'idempotency_key': action_key('namespace', sid, eid), 'attempts': 1,
            'status': 'succeeded', 'result': 'confirmed', 'policy': {'max_attempts': 2}}]
        rows['agent_action_attempts'] = [
            {'execution_id': eid, 'attempt_number': 1, 'kind': 'dispatch', 'status': 'uncertain',
             'finished_at': 'settled', 'command_id': 'command', 'payload_sha256': fingerprint},
            {'execution_id': eid, 'attempt_number': 2, 'kind': 'verify', 'status': 'succeeded',
             'finished_at': 'settled', 'command_id': 'resume', 'payload_sha256': fingerprint}]
        self.assertEqual(verify(rows)['executions'], 1)
        rows['agent_action_attempts'][1]['payload_sha256'] = 'changed'
        with self.assertRaises(AssertionError):
            verify(rows)


if __name__ == '__main__':
    unittest.main()
