"""Shared live HTTP scenarios; all durable verification uses PostgreSQL reads."""
import json
from uuid import uuid4

from persistence_ledger import capture, immutable_prefix, verify
from persistence_support import require


class Scenarios:
    def __init__(self, probe, directory):
        self.probe, self.directory = probe, directory
        self.sequence = 0
        self.workflow = None
        self.evidence = []

    def snapshot(self, sid, label):
        self.sequence += 1
        rows, evidence = capture(self.probe.database_url, sid,
                                 self.directory / f'{self.sequence:03d}-{label}.json')
        self.evidence.append(evidence)
        return rows, {**evidence, 'verification': verify(rows)}

    def api_replay(self):
        result = self.probe.run()
        result['ledger'] = [self.snapshot(sid, 'command-ledger')[1] for sid in self.probe.sessions]
        return result

    def strict_requests(self):
        failures = []
        for raw in ('{"user_prompt":"first","user_prompt":"second"}',
                    '{"user_prompt":"strict","extra":1}',
                    '{"user_prompt":"strict","unexpected":NaN}'):
            key = str(uuid4())
            response = self.probe.http.post('/sessions', content=raw,
                headers={'Content-Type': 'application/json', 'Idempotency-Key': key})
            require(response.status_code == 422, 'Malformed/ambiguous JSON was accepted')
            require(self.probe.rows('SELECT count(*) FROM agent_commands WHERE idempotency_key=%s', (key,))[0][0] == 0,
                    'Rejected JSON created a durable command')
            failures.append({'status': response.status_code, 'key': key})
        return {'rejected_without_admission': failures}

    def workflow_resume(self):
        body = {'config_ref': 'log-analysis.yaml', 'user_prompt':
                'Inspect and robustly analyze the HTTP access log. Validate the most frequent '
                'HTTP 500 IP and its count. In the final answer state the validated IP and count.'}
        key = str(uuid4())
        created = self.probe.post('/sessions', body, key)
        require(created.status_code == 201, f'Workflow creation returned {created.status_code}')
        sid = created.json()['session']['id']
        self.probe.sessions.append(sid)
        self.probe.command(key, sid, '/sessions', body, created)
        before, initial = self.snapshot(sid, 'before-run')
        route = f'/sessions/{sid}/run'
        run_body, run_key = {'max_steps': 1}, str(uuid4())
        stopped = self.probe.post(route, run_body, run_key)
        require(stopped.status_code == 200, f'Bounded run returned {stopped.status_code}')
        state = stopped.json()['session']
        require(state['run_status'] != 'running' and state['run_id'], 'Bounded run did not settle')
        require(state.get('last_error', {}).get('category') != 'provider' if state.get('last_error') else True,
                'Model provider failed during bounded run')
        self.probe.command(run_key, sid, route, run_body, stopped)
        old_run = state['run_id']
        bounded, bounded_evidence = self.snapshot(sid, 'bounded-run')
        immutable_prefix(before, bounded)
        self.probe.replay(route, run_body, run_key, stopped, sid)
        cancel_route, cancel_key = f'/sessions/{sid}/cancel', str(uuid4())
        cancel_body = {'run_id': old_run}
        cancel = self.probe.post(cancel_route, cancel_body, cancel_key)
        require(cancel.status_code == 200 and cancel.json()['cancel_requested_for'] == [],
                'Cancellation unexpectedly targeted a settled run')
        self.probe.command(cancel_key, sid, cancel_route, cancel_body, cancel)
        resume_key = str(uuid4())
        resumed = self.probe.post(f'/sessions/{sid}/resume', {'max_steps': 20}, resume_key)
        require(resumed.status_code == 200, f'Resume returned {resumed.status_code}')
        (self.directory / 'workflow-resume.sse').write_text(resumed.text)
        response = self.probe.http.get(f'/sessions/{sid}')
        require(response.status_code == 200, 'Cannot read resumed session')
        state = response.json()
        require(state['status'] == state['run_status'] == 'complete',
                f"Workflow failed: {state.get('last_error')}")
        require(state['run_id'] != old_run, 'Intentional resume reused an old run')
        final, final_evidence = self.snapshot(sid, 'resumed-run')
        immutable_prefix(bounded, final)
        require(len(final['agent_runs']) == 2, 'Bounded run and resume did not create exactly two runs')
        tools = {e['tool'] for e in final['agent_tool_executions'] if e['status'] == 'succeeded'}
        require({'inspect_log', 'analyze_500_ips', 'validate_top_offending_ip'} <= tools,
                'Completed workflow lacks required durable tool outcomes')
        self.probe.replay(cancel_route, cancel_body, cancel_key, cancel, sid)
        self.probe.replay(route, run_body, run_key, stopped, sid)
        self.probe.replay('/sessions', body, key, created, sid)
        self.workflow = sid
        return {'session_id': sid, 'before': initial, 'bounded': bounded_evidence, 'final': final_evidence,
                'checks': ['bounded run', 'intentional resume', 'immutable history prefix',
                           'required tool outcomes', 'old run response replay', 'cancellation target replay']}

    def history_pages(self):
        require(self.workflow, 'Workflow scenario did not complete')
        sid = self.workflow
        rows, evidence = self.snapshot(sid, 'before-history-pages')
        specifications = [
            ('transcript', 'entries', 'after_sequence', 'next_after_sequence', 'agent_session_transcript',
             'sequence', ('sequence', 'message_id', 'kind', 'payload', 'run_id', 'decision_step')),
            ('events', 'entries', 'after', 'next_after', 'agent_session_events',
             'event_id', ('event_id', 'event')),
            ('runs', 'runs', 'after', 'next_after', 'agent_runs',
             'run_id', ('run_id', 'status', 'fence')),
            ('executions', 'executions', 'after', 'next_after', 'agent_tool_executions',
             'execution_id', ('execution_id', 'run_id', 'call_id', 'tool', 'arguments', 'status', 'result',
                              'idempotency_key', 'payload_sha256', 'command_id')),
        ]
        summaries = {}
        for route, field, parameter, cursor_field, table, identity, fields in specifications:
            cursor, entries, seen = None, [], set()
            for _ in range(len(rows[table])+2):
                params = {'limit': 1}
                if cursor is not None:
                    params[parameter] = cursor
                response = self.probe.http.get(f'/sessions/{sid}/{route}', params=params)
                require(response.status_code == 200, f'{route} pagination returned {response.status_code}')
                page = response.json()
                if not page[field]:
                    break
                entries.extend(page[field])
                cursor = page[cursor_field]
                require(cursor not in seen, f'{route} pagination cursor did not advance')
                seen.add(cursor)
            else:
                raise AssertionError(f'{route} pagination did not terminate')
            expected = {r[identity]: {k: r[k] for k in fields} for r in rows[table]}
            actual = {r[identity]: {k: r[k] for k in fields} for r in entries}
            require(len(entries) == len(actual) and actual == expected, f'{route} API differs from PostgreSQL')
            summaries[route] = len(entries)
        for family in sorted({r['family'] for r in rows['agent_session_diagnostics']}):
            expected = sorted((r for r in rows['agent_session_diagnostics'] if r['family'] == family),
                              key=lambda r: r['ordinal'])
            after, actual = 0, []
            for _ in range(len(expected)+1):
                response = self.probe.http.get(f'/sessions/{sid}/diagnostics/{family}',
                                               params={'after': after, 'limit': 1})
                require(response.status_code == 200, 'Diagnostic pagination failed')
                page = response.json()
                if not page['entries']:
                    break
                require(page['next_after'] > after, 'Diagnostic cursor did not advance')
                actual.extend(page['entries'])
                after = page['next_after']
            require([(r['ordinal'], r['payload']) for r in actual] ==
                    [(r['ordinal'], r['payload']) for r in expected], 'Diagnostic API differs from PostgreSQL')
            summaries['diagnostics/' + family] = len(actual)
        after, _ = self.snapshot(sid, 'after-history-pages')
        require(after == rows, 'History reads changed durable state')
        return {'pages': summaries, 'ledger': evidence}

    def event_reconnect(self):
        require(self.workflow, 'Workflow scenario did not complete')
        sid = self.workflow
        before, evidence = self.snapshot(sid, 'before-event-reconnect')
        run_id = before['agent_sessions'][0]['state']['run_id']
        expected = sorted((e for e in before['agent_session_events'] if e['event'].get('run_id') == run_id),
                          key=lambda e: e['event_id'])
        require(len(expected) > 1, 'No durable run events to reconnect')
        watermark = expected[0]['event_id']
        response = self.probe.http.get(f'/sessions/{sid}/events/stream',
                                      params={'run_id': run_id, 'after_event_id': watermark})
        require(response.status_code == 200, 'Event reconnection failed')
        (self.directory / 'reconnected-events.sse').write_text(response.text)
        actual = [json.loads(line[5:].strip()) for line in response.text.splitlines() if line.startswith('data:')]
        require(actual == [{**e['event'], 'event_id': e['event_id']} for e in expected[1:]],
                'Reconnected SSE differs from retained PostgreSQL events')
        after, _ = self.snapshot(sid, 'after-event-reconnect')
        require(before == after, 'Event reconnect created or changed run/effect/ledger records')
        return {'events_replayed': len(actual), 'ledger': evidence}

    def retention_boundary(self):
        require(self.workflow, 'Workflow scenario did not complete')
        sid = self.workflow
        before, evidence = self.snapshot(sid, 'before-retention-plan')
        response = self.probe.http.get(f'/sessions/{sid}/retention')
        require(response.status_code == 200, 'Retention inspection failed')
        plan = response.json()
        require(plan['default_policy'] == 'keep_all' and plan['eligible'] and
                plan['unresolved_executions'] == plan['running_lifecycles'] == 0,
                'Retention plan differs from completed ledger')
        require({'agent_commands', 'agent_commit_receipts', 'agent_action_attempts',
                 'agent_session_transcript', 'agent_tool_executions'} <= set(plan['protected_tables']),
                'Retention plan omits protected evidence')
        require(set(plan['prunable_tables']) == {'agent_session_events', 'agent_session_diagnostics'},
                'Retention permits pruning protected data')
        after, _ = self.snapshot(sid, 'after-retention-plan')
        require(after == before, 'Retention inspection changed ledger records')
        return {'plan': plan, 'ledger': evidence, 'scope': 'inspection; no pruning performed'}
