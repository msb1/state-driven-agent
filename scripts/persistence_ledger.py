"""Read-only, consistent PostgreSQL evidence and cross-table assertions."""
import hashlib
import json
from pathlib import Path

from psycopg.conninfo import conninfo_to_dict

import psycopg

from idempotency_contract import action_key, payload_fingerprint, request_fingerprint
from persistence_support import require

TABLES = ('agent_sessions', 'agent_commands', 'agent_commit_receipts',
          'agent_session_transcript', 'agent_session_context', 'agent_session_events',
          'agent_runs', 'agent_tool_executions', 'agent_action_attempts',
          'agent_session_diagnostics', 'agent_session_calls', 'agent_archive_receipts')


def capture(database_url, sid, path):
    """One repeatable-read snapshot; server-enforced read-only on connection."""
    options = conninfo_to_dict(database_url).get('options', '')+' -c default_transaction_read_only=on -c statement_timeout=30000'
    with psycopg.connect(database_url, options=options, connect_timeout=10) as db:
        db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        db.execute("SET LOCAL TIME ZONE 'UTC'")
        rows = {table: [item[0] for item in db.execute(
            f'SELECT to_jsonb(t) FROM {table} t WHERE session_id=%s ORDER BY to_jsonb(t)::text',
            (sid,))] for table in TABLES}
    data = (json.dumps(rows, sort_keys=True, indent=2, ensure_ascii=False) + '\n').encode()
    Path(path).write_bytes(data)
    return rows, {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest(),
                  'session_id': sid, 'counts': {k: len(v) for k, v in rows.items()}}


def verify(rows):
    require(len(rows['agent_sessions']) == len(rows['agent_session_context']) == 1,
            'Missing session snapshot or context')
    session, context = rows['agent_sessions'][0], rows['agent_session_context'][0]
    revision = session['revision']
    require(session['storage_version'] == 6 and session['state']['revision'] == revision and
            session['state']['id'] == session['session_id'], 'Snapshot identity/version/revision differs from storage columns')
    receipts = sorted(rows['agent_commit_receipts'], key=lambda r: r['committed_revision'])
    require([(r['expected_revision'], r['committed_revision']) for r in receipts] ==
            [(n-1, n) for n in range(1, revision+1)], 'Broken commit revision chain')
    require(context['revision'] == revision, 'Context revision differs from snapshot')
    require(receipts and session['last_commit_id'] == receipts[-1]['commit_id'] and
            session['last_commit_sha256'] == receipts[-1]['fingerprint'], 'Last receipt anchor differs')
    commands = {r['command_id']: r for r in rows['agent_commands']}
    states = {'pending', 'started', 'succeeded', 'failed', 'uncertain', 'blocked'}
    for command in commands.values():
        require(command['status'] in states, 'Unknown command status')
        require(command['request_sha256'] == request_fingerprint(command['method'], command['route'], command['request']['input']),
                'Durable command request fingerprint differs from its frozen request')
        terminal = command['status'] in ('succeeded', 'failed')
        require(terminal == (command['response_body'] is not None and command['response_status'] is not None and
                             command['completed_at'] is not None), 'Command status/outcome fields disagree')
        if not terminal:
            require(all(command[name] is None for name in ('response_body', 'response_status', 'completed_at')),
                    'Unresolved command claims a terminal response')
        else:
            require(200 <= command['response_status'] <= 599, 'Terminal command has invalid response status')
        require((command['owner'] is None) == (command['lease_expires_at'] is None), 'Command lease fields disagree')
    require(all(r['command_id'] in commands for r in receipts), 'Receipt lacks origin command')
    transcript = sorted(rows['agent_session_transcript'], key=lambda r: r['sequence'])
    require([r['sequence'] for r in transcript] == list(range(1, len(transcript)+1)),
            'Transcript has gaps or duplicate sequences')
    messages = {r['message_id']: r for r in transcript}
    require(len(messages) == len(transcript), 'Duplicate transcript identity')
    refs = [context['goal_message_id'], *context['raw_message_ids']]
    if context['ledger_record_id']:
        refs.append(context['ledger_record_id'])
    require(all(ref in messages for ref in refs), 'Dangling active context reference')
    require(messages[context['goal_message_id']]['payload'].get('content') == session['state']['goal'],
            'Goal anchor differs from immutable transcript')
    require(session['state']['next_transcript_sequence'] == len(transcript)+1,
            'Snapshot transcript counter differs from durable history')
    for family, count in session['state'].get('diagnostic_counts', {}).items():
        ordinals = sorted(r['ordinal'] for r in rows['agent_session_diagnostics'] if r['family'] == family)
        # Archived sessions may have a retained suffix; never infer pruning coverage here.
        if not session['state'].get('last_archive_id'):
            require(ordinals == list(range(1, count+1)), 'Diagnostic family counter differs from ledger')
    for call in rows['agent_session_calls']:
        require(call['message_id'] in messages, 'Call map has dangling transcript identity')
    receipts_by_id = {r['commit_id']: r for r in receipts}
    for event in rows['agent_session_events']:
        if event['commit_id']:
            receipt = receipts_by_id.get(event['commit_id'])
            require(receipt is not None and event['revision'] == receipt['committed_revision'] and
                    event['command_id'] == receipt['command_id'], 'Event/receipt revision or command differs')
    runs = {r['run_id']: r for r in rows['agent_runs']}
    for run in runs.values():
        require(run['command_id'] in commands, 'Run lacks origin command')
        require((run['status'] == 'running') == (run['ended_at'] is None), 'Run termination is inconsistent')
    for execution in rows['agent_tool_executions']:
        require(execution['status'] in states and
                ((execution['status'] in ('succeeded', 'failed')) == (execution['result'] is not None)),
                'Action status/result fields disagree')
        require(execution['attempts'] <= execution['policy']['max_attempts'], 'Action exceeded its attempt bound')
        run = runs.get(execution['run_id'])
        require(run is not None and run['command_id'] == execution['command_id'], 'Execution origin differs from run')
        require(execution['idempotency_key'] == action_key(execution['namespace'], session['session_id'], execution['execution_id']),
                'Action key differs from durable identity')
        require(execution['payload_sha256'] == payload_fingerprint(execution['tool'], execution['target'], execution['arguments']),
                'Frozen action fingerprint differs')
        attempts = sorted((a for a in rows['agent_action_attempts'] if a['execution_id'] == execution['execution_id']),
                          key=lambda a: a['attempt_number'])
        require([a['attempt_number'] for a in attempts] == list(range(1, len(attempts)+1)) and
                sum(a['kind'] == 'dispatch' for a in attempts) == execution['attempts'],
                'Attempt counter differs from recorded attempts')
        for attempt in attempts:
            require(attempt['payload_sha256'] == execution['payload_sha256'] and
                    attempt['command_id'] in commands, 'Attempt changed payload or lacks its initiating command')
            require((attempt['status'] == 'started') == (attempt['finished_at'] is None), 'Attempt settlement is inconsistent')
    require(all(a['execution_id'] in {e['execution_id'] for e in rows['agent_tool_executions']}
                for a in rows['agent_action_attempts']), 'Orphan action attempt')
    return {'revision': revision, 'transcript_entries': len(transcript), 'runs': len(runs),
            'executions': len(rows['agent_tool_executions']), 'receipts': len(receipts)}


def immutable_prefix(before, after, *, allow_telemetry_prune=False):
    for table, keys in (('agent_session_transcript', ('message_id',)),
                        ('agent_commit_receipts', ('commit_id',)),
                        ('agent_session_calls', ('call_id', 'message_id')),
                        ('agent_session_diagnostics', ('family', 'ordinal'))):
        if allow_telemetry_prune and table == 'agent_session_diagnostics':
            continue
        current = {tuple(r[k] for k in keys): r for r in after[table]}
        require(all(current.get(tuple(r[k] for k in keys)) == r for r in before[table]),
                f'Existing immutable records changed in {table}')
