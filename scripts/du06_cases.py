"""Contract 2.0.0 DU-06 cases: common HTTP operations and PostgreSQL evidence only."""
from concurrent.futures import ThreadPoolExecutor
import asyncio
import json
from pathlib import Path
import threading
import time
from uuid import uuid4

import httpx

from disconnect_integration_test import Harness, ANSWER
from du06_archive import inspect_archive
from du06_client import normalized_input
from du06_controls import NotExercised, listener_identity, suppress_response
from idempotency_contract import canonical, request_fingerprint
from persistence_ledger import immutable_prefix, verify
from persistence_scenarios import Scenarios
from persistence_support import require, setup_checks


FROZEN_ACTION = ('execution_id', 'run_id', 'command_id', 'namespace', 'call_id', 'tool', 'target',
                 'arguments', 'policy', 'key_algorithm', 'idempotency_key', 'payload_sha256', 'created_at')


def stable_while_running(rows):
    """Admission and independent lease renewal are allowed during ownership conflicts."""
    value = json.loads(json.dumps(rows))
    value.pop('agent_commands')
    for session in value['agent_sessions']:
        session.pop('lease_expires_at')
    return value


def preserved(before, after, *, pruning=False):
    immutable_prefix(before, after, allow_telemetry_prune=pruning)
    commands = {row['command_id']: row for row in after['agent_commands']}
    for row in before['agent_commands']:
        if row['status'] in ('succeeded', 'failed'):
            require(commands.get(row['command_id']) == row, 'A completed command identity/outcome changed')
    executions = {row['execution_id']: row for row in after['agent_tool_executions']}
    for row in before['agent_tool_executions']:
        current = executions.get(row['execution_id'])
        require(current is not None and all(current[name] == row[name] for name in FROZEN_ACTION),
                'A durable action identity/payload/policy changed')
    attempts = {row['attempt_id']: row for row in after['agent_action_attempts']}
    for row in before['agent_action_attempts']:
        current = attempts.get(row['attempt_id'])
        require(current is not None, 'An action attempt was removed')
        keys = tuple(row) if row['status'] != 'started' else (
            'attempt_id', 'execution_id', 'session_id', 'command_id', 'attempt_number', 'kind',
            'payload_sha256', 'owner', 'fence', 'started_at')
        require(all(current[key] == row[key] for key in keys), 'An action attempt identity/settled outcome changed')


class Cases:
    def __init__(self, client, model_url, second_url, *, interactive=True, skip_restart=False, skip_pruning=False, fixture_url=None):
        self.c = client
        self.model_url, self.second_url = model_url, second_url.rstrip('/')
        self.interactive, self.skip_restart, self.skip_pruning = interactive, skip_restart, skip_pruning
        self.anchor = self.interrupted = self.completed = self.tool_session = None
        self.history = {}
        self.records = []
        self.pruned = set()
        self.subchecks = {}
        self.fixture_url = fixture_url or client.database_url
        self.rejections = []

    @staticmethod
    def prerequisite(condition, message):
        if not condition:
            raise NotExercised(message)

    def prompt(self, message):
        if not self.interactive:
            raise NotExercised('Operator step required: '+message)
        input(message+'\nPress Enter after completing this operation: ')

    def snapshot(self, sid, label):
        rows, evidence = self.c.observe(sid, label)
        self.history.setdefault(sid, []).append(rows)
        return rows, evidence

    def create(self, *args, **kwargs):
        record = self.c.create(*args, **kwargs)
        self.records.append(record)
        return record

    def parallel(self, work):
        barrier = threading.Barrier(len(work))
        def invoke(item):
            barrier.wait(timeout=self.c.timeout)
            return self.c.post_to(*item)
        with ThreadPoolExecutor(max_workers=len(work)) as pool:
            return list(pool.map(invoke, work))

    def replay(self, record, route='/sessions', target=None):
        return self.c.replay_record(record, route, target)

    def record_external(self, h, reader, action):
        """Account for mutations sent by the shared real-socket reader."""
        created = json.loads(next(h.output.glob('*-created.json')).read_text())
        sid = created['session']['id']
        if sid not in self.c.sessions:
            self.c.sessions.append(sid)
        for line in (h.output / 'request-keys.jsonl').read_text().splitlines():
            entry = json.loads(line)
            if not any(row['key'] == entry['key'] for row in self.c.requests):
                body = {'config_ref': 'socket-integration.yaml', 'user_prompt': created['session']['goal']}
                self.c.requests.append({'case': self.c.case_id, 'method': 'POST', 'route': entry['path'],
                    'key': entry['key'], 'target': h.agent_url, 'http_status': 201, 'external_reader': True,
                    'request_sha256': request_fingerprint('POST', entry['path'], body)})
        if not any(row['key'] == reader.key for row in self.c.requests):
            route = f'/sessions/{sid}/{action}'
            self.c.requests.append({'case': self.c.case_id, 'method': 'POST', 'route': route, 'key': reader.key,
                'target': h.agent_url, 'http_status': 200, 'external_reader': True,
                'request_sha256': request_fingerprint('POST', route, normalized_input(route, {'max_steps': 2}))})
        return sid

    def trial(self, label):
        return Harness(str(self.c.http.base_url), self.model_url, 'socket-integration.yaml',
                       self.c.directory / label, min(self.c.timeout, 180))

    def close_trial(self, harness):
        try:
            harness.diagnostics()
        finally:
            try:
                harness.close()
            finally:
                # A gate can fail after creation but before start() returns.
                for created in harness.output.glob('*-created.json'):
                    sid = json.loads(created.read_text())['session']['id']
                    if sid not in self.c.sessions:
                        self.c.sessions.append(sid)

    def c01(self):
        self.anchor = self.create()
        rows, evidence = self.snapshot(self.anchor['sid'], 'identity')
        require(len(rows['agent_commands']) == len(rows['agent_commit_receipts']) == 1,
                'Creation did not create exactly one command and one committed revision')
        require(rows['agent_commands'][0]['session_id'] == self.anchor['sid'], 'Command session association differs')
        return {'expected_effects': {'session': 1, 'command': 1, 'initial_revision': 1}, 'ledger': evidence}

    def c02(self):
        self.prerequisite(self.anchor, 'C01 prerequisite failed')
        return {'replays': [self.replay(self.anchor) for _ in range(3)], 'expected_replay_effects': 0}

    def c03(self):
        self.prerequisite(self.anchor, 'C01 prerequisite failed')
        record = self.anchor
        before, baseline = self.snapshot(record['sid'], 'key-reuse-before')
        changed = self.c.post('/sessions', {**record['body'], 'user_prompt': 'changed-'+uuid4().hex}, record['key'])
        route = self.c.post(f"/sessions/{record['sid']}/messages", {'content': 'must not append'}, record['key'])
        require(all(r.status_code == 409 and r.json().get('code') == 'idempotency_key_reused' for r in (changed, route)),
                'Changed payload/route did not return the specified key conflict')
        after, evidence = self.snapshot(record['sid'], 'key-reuse-after')
        require(before == after, 'Rejected key reuse changed committed data')
        return {'before': baseline, 'after': evidence, 'expected_rejected_effects': 0}

    def c04(self):
        record = self.create()
        sid = record['sid']
        before, baseline = self.snapshot(sid, 'concurrent-before')
        route, body, key = f'/sessions/{sid}/messages', {'content': 'concurrent-'+uuid4().hex}, str(uuid4())
        responses = self.parallel([(str(self.c.http.base_url), route, body, key)]*4)
        require(all(r.status_code in (200, 202) for r in responses), 'Identical concurrent commands conflicted or failed')
        row = self.c.settled(key)
        require(row['status'] == 'succeeded', 'Concurrent command did not succeed')
        for response in responses:
            if response.status_code == 200:
                self.c.check_response(row, route, body, response)
            else:
                require(response.json()['command_id'] == row['command_id'], 'In-progress duplicate identifies another command')
        after, evidence = self.snapshot(sid, 'concurrent-after')
        preserved(before, after)
        require(after['agent_sessions'][0]['revision'] == before['agent_sessions'][0]['revision']+1,
                'One message operation did not advance exactly one revision')
        messages = [r for r in after['agent_session_transcript'] if r['payload'].get('content') == body['content']]
        require(len(messages) == 1, 'Concurrent duplicates produced more than one message effect')
        duplicate = {'sid': sid, 'key': key, 'body': body}
        return {'before': baseline, 'after': evidence, 'concurrent_requests': 4,
                'expected_message_effects': 1, 'observed_message_effects': len(messages),
                'replay': self.replay(duplicate, route)}

    def c05(self):
        record = self.create()
        route = f"/sessions/{record['sid']}/messages"
        first = None
        for content in ('first', 'later'):
            key, body = str(uuid4()), {'content': content+'-'+uuid4().hex}
            response = self.c.post(route, body, key)
            row = self.c.settled(key)
            require(response.status_code == 200, 'Intentional message append failed')
            self.c.check_response(row, route, body, response)
            if first is None:
                first = {'sid': record['sid'], 'body': body, 'key': key}
        return {'old_message_replay': self.replay(first, route), 'old_creation_replay': self.replay(record),
                'expected_replay_revision_delta': 0}

    def c06(self):
        key = str(uuid4())
        body = {'config_ref': 'completion-no-workflow.yaml', 'user_prompt': 'missing-response-'+uuid4().hex}
        with suppress_response(str(self.c.http.base_url), self.c.timeout) as control:
            try:
                self.c.post_to(control['url'], '/sessions', body, key)
            except (httpx.RemoteProtocolError, httpx.ReadError):
                pass
            else:
                raise AssertionError('Caller unexpectedly received a response from suppression control')
            require(control['finished'].wait(self.c.timeout) and not control['errors'] and len(control['events']) == 1,
                    'External response suppression did not finish correctly')
            require(control['events'][0]['upstream_status'] == 201 and not control['events'][0]['downstream_response_sent'],
                    'Missing-response scenario was not established')
            row = self.c.settled(key)
            require(row['status'] == 'succeeded', 'Suppressed create response has no successful recorded command')
            record = {'sid': row['session_id'], 'key': key, 'body': body}
            self.records.append(record)
            effect_count = self.c.rows("SELECT count(*) FROM agent_sessions WHERE state->>'goal'=%s", (body['user_prompt'],))[0][0]
            require(effect_count == 1, 'Missing response produced duplicate session effects')
            return {'control': control['events'], 'command_id': row['command_id'], 'observed_session_effects': effect_count,
                    'recovery': self.replay(record), 'scope': 'caller response suppressed; no database COMMIT-ACK claim'}

    def c07(self):
        h = self.trial('interruption')
        reader = resume = None
        try:
            gate, reader, active = h.start('interruption')
            sid = self.record_external(h, reader, 'run/stream')
            before, baseline = self.snapshot(sid, 'before-disconnect')
            reader.disconnect(h.timeout)
            h.mark('actual TCP shutdown', run_id=active['run_id'])
            h.wait('durable cancellation request', h.runs,
                   lambda page: any(r['run_id'] == active['run_id'] and r['cancel_requested'] for r in page['runs']))
            require(not gate.release.is_set(), 'Invocation completed before the connection interruption')
            gate.release.set()
            h.wait('cancelled run', h.state, lambda state: state['run_status'] == 'cancelled')
            row = self.c.settled(reader.key)
            cancelled, cancelled_evidence = self.snapshot(sid, 'after-disconnect')
            preserved(before, cancelled)
            require(len(cancelled['agent_runs']) == 1 and gate.requests == 1, 'Interruption duplicated the run or model invocation')
            original = {'sid': sid, 'key': reader.key, 'body': {'max_steps': 2}}
            replay = self.replay(original, f'/sessions/{sid}/run/stream')
            marker = active['goal'].split(':', 1)[0]
            new_gate = h.model.arm('resume', marker)
            resume = h.stream('resume', 'resume')
            self.record_external(h, resume, 'resume')
            h.signal(new_gate.entered, 'resume reached controlled model')
            new_gate.release.set()
            final = h.completed(resume, new_gate, 'resume')
            self.c.settled(resume.key)
            after, evidence = self.snapshot(sid, 'after-interruption-resume')
            preserved(cancelled, after)
            require(len(after['agent_runs']) == 2 and final['run_id'] != active['run_id'], 'Intentional resume did not create exactly one new run')
            self.interrupted, self.completed = sid, sid
            return {'session_id': sid, 'before': baseline, 'cancelled': cancelled_evidence, 'after': evidence,
                    'interruption': {'type': 'TCP shutdown', 'run_id': active['run_id'], 'signals': str(h.output / 'signals.json')},
                    'original_command_status': row['status'], 'original_request_replay': replay,
                    'scope': 'connection interruption at a live invocation boundary; committed ledger atomicity'}
        finally:
            self.close_trial(h)

    def c08(self):
        if self.skip_restart:
            raise NotExercised('Live-server restart was explicitly skipped')
        identity = listener_identity(str(self.c.http.base_url))
        before = {sid: self.snapshot(sid, 'restart-before')[0] for sid in self.c.sessions}
        self.prompt(f'Restart the same agent server at {self.c.http.base_url}, using the same configuration and database.')
        health = self.c.http.get('/health')
        require(health.status_code == 200 and health.json().get('contract_version') == '2.0.0' and
                health.json().get('storage_schema') == 6, 'Restarted server unavailable/incompatible')
        layout = asyncio.run(setup_checks(self.c.database_url, self.fixture_url))
        after_identity = listener_identity(str(self.c.http.base_url))
        old = {(p['pid'], p['started']) for p in identity['processes']}
        new = {(p['pid'], p['started']) for p in after_identity['processes']}
        require(old.isdisjoint(new), 'Listening process identity did not change; restart is not established')
        evidence = []
        for sid, original in before.items():
            response = self.c.http.get('/sessions/'+sid)
            require(response.status_code == 200 and response.json()['revision'] == original['agent_sessions'][0]['revision'],
                    'Restarted agent did not hydrate original session revision')
            after, artifact = self.snapshot(sid, 'restart-after')
            require(after == original, 'Restart changed durable records for a settled test session')
            evidence.append(artifact)
        return {'before_process': identity, 'after_process': after_identity, 'sessions': evidence, 'schema_after_restart': layout,
                'replays': [self.replay({'sid': sid, 'key': command['idempotency_key'], 'body': command['request']['input']}, command['route'])
                            for sid, original in before.items() for command in original['agent_commands']
                            if command['status'] in ('succeeded', 'failed')]}

    def c09(self):
        try:
            ready = self.c.http.get(self.second_url+'/health')
        except httpx.HTTPError:
            ready = None
        if ready is None or ready.status_code != 200:
            self.prompt(f'Start a second instance of the same agent at {self.second_url}, sharing the configuration, namespace and PostgreSQL database.')
            ready = self.c.http.get(self.second_url+'/health')
        require(ready.status_code == 200 and ready.json().get('contract_version') == '2.0.0' and ready.json().get('storage_schema') == 6,
                'Second worker is unavailable or incompatible')
        identities = [listener_identity(url) for url in (str(self.c.http.base_url), self.second_url)]
        require({p['pid'] for p in identities[0]['processes']}.isdisjoint({p['pid'] for p in identities[1]['processes']}),
                'The two endpoints do not identify distinct worker processes')
        key = str(uuid4())
        body = {'config_ref': 'completion-no-workflow.yaml', 'user_prompt': 'two-workers-'+uuid4().hex}
        responses = self.parallel([(url, '/sessions', body, key) for url in (str(self.c.http.base_url), self.second_url)])
        require(all(r.status_code in (201, 202) for r in responses), 'Two-worker duplicate creation conflicted or failed')
        row = self.c.settled(key)
        for response in responses:
            if response.status_code == 201:
                self.c.check_response(row, '/sessions', body, response)
            else:
                require(response.json()['command_id'] == row['command_id'], 'Workers admitted different command identities')
        require(self.c.rows("SELECT count(*) FROM agent_sessions WHERE state->>'goal'=%s", (body['user_prompt'],))[0][0] == 1,
                'Two workers created duplicate session effects')
        record = {'sid': row['session_id'], 'key': key, 'body': body}
        self.records.append(record)
        self.replay(record, target=self.second_url)
        h = self.trial('two-workers')
        reader = None
        try:
            gate, reader, active = h.start('ownership')
            sid = self.record_external(h, reader, 'run/stream')
            require(self.c.http.get(self.second_url+'/sessions/'+sid).json()['run_id'] == active['run_id'],
                    'Second worker does not observe the same database/session')
            before, baseline = self.snapshot(sid, 'ownership-before')
            route = f'/sessions/{sid}/run/stream'
            duplicates = self.parallel([(url, route, {'max_steps': 2}, reader.key) for url in (str(self.c.http.base_url), self.second_url)])
            origin = self.c.command_row(key=reader.key)
            require(all(r.status_code == 202 and r.json()['command_id'] == origin['command_id'] for r in duplicates),
                    'Active duplicate did not identify the original in-progress run command')
            competing_key, message_key = str(uuid4()), str(uuid4())
            competing = self.c.post_to(self.second_url, f'/sessions/{sid}/run', {'max_steps': 1}, competing_key)
            message = self.c.post_to(self.second_url, f'/sessions/{sid}/messages', {'content': 'must not append'}, message_key)
            require(competing.status_code == message.status_code == 409, 'Competing worker acquired an active session or appended input')
            self.rejections.extend([
                {'sid': sid, 'key': competing_key, 'route': f'/sessions/{sid}/run', 'body': {'max_steps': 1}, 'response': competing},
                {'sid': sid, 'key': message_key, 'route': f'/sessions/{sid}/messages', 'body': {'content': 'must not append'}, 'response': message},
            ])
            during, conflict_evidence = self.snapshot(sid, 'ownership-conflicts')
            require(stable_while_running(before) == stable_while_running(during), 'Conflicting operations changed the owned aggregate')
            gate.release.set()
            h.completed(reader, gate, 'ownership')
            self.c.settled(reader.key)
            after, evidence = self.snapshot(sid, 'ownership-after')
            require(len(after['agent_runs']) == 1 and gate.requests == 1, 'Two workers duplicated the owned run or model invocation')
            replay = self.replay({'sid': sid, 'key': reader.key, 'body': {'max_steps': 2}}, route, self.second_url)
            self.completed = self.completed or sid
            return {'workers': identities, 'session_id': sid, 'concurrent_create_command': row['command_id'],
                    'before': baseline, 'conflicts': conflict_evidence, 'after': evidence, 'replay': replay,
                    'expected_create_effects': 1, 'expected_owned_runs': 1}
        finally:
            self.close_trial(h)

    def c10(self):
        self.prerequisite(self.completed, 'A completed controlled session is required')
        sid, key = self.completed, str(uuid4())
        before, baseline = self.snapshot(sid, 'failure-before')
        route, body = f'/sessions/{sid}/run/stream', {'max_steps': 2}
        response = self.c.post(route, body, key)
        require(response.status_code == 409, 'Running a terminal session did not return the defined failure')
        row = self.c.settled(key)
        require(row['status'] == 'failed', 'Definitive rejected command has no recorded failed outcome')
        self.c.check_response(row, route, body, response)
        after, evidence = self.snapshot(sid, 'failure-after')
        preserved(before, after)
        require({k: v for k, v in before.items() if k != 'agent_commands'} ==
                {k: v for k, v in after.items() if k != 'agent_commands'}, 'Definitive command rejection created aggregate effects')
        replay = self.replay({'sid': sid, 'key': key, 'body': body}, route)
        conflict_replays = []
        for rejection in self.rejections:
            recorded = self.c.command_row(key=rejection['key'])
            require(recorded is not None and recorded['status'] == 'failed',
                    'A definitive admitted ownership rejection was not recorded as a failed command')
            self.c.check_response(recorded, rejection['route'], rejection['body'], rejection['response'])
            conflict_replays.append(self.replay(rejection, rejection['route']))
        observed = []
        for session in self.c.sessions:
            rows, artifact = self.snapshot(session, 'state-consistency')
            observed.extend({'command_id': r['command_id'], 'status': r['status']} for r in rows['agent_commands'])
        require({'succeeded', 'failed'} <= {r['status'] for r in observed}, 'Success and definitive-failure states were not both exercised')
        self.prerequisite(self.interrupted is not None, 'Interruption state scenario was not exercised')
        return {'before': baseline, 'after': evidence, 'failure_command_id': row['command_id'], 'replay': replay,
                'ownership_conflict_replays': conflict_replays,
                'observed_states': observed, 'interrupted_session': self.interrupted,
                'scope': 'valid observed states; no requirement for every command to traverse every enum value'}

    def c11(self):
        target = self.c.directory / 'tool-history'
        target.mkdir()
        scenarios = Scenarios(self.c, target)
        try:
            workflow = scenarios.workflow_resume()
            history = scenarios.history_pages()
            events = scenarios.event_reconnect()
            self.tool_session = scenarios.workflow
        finally:
            self.c.snapshots.extend(scenarios.evidence)
        require(self.tool_session is not None, 'No completed tool session was created')
        evidence = []
        for sid in self.c.sessions:
            current, artifact = self.snapshot(sid, 'ledger-relationships')
            history_rows = self.history[sid]
            for earlier in history_rows[:-1]:
                preserved(earlier, current)
            evidence.append(artifact)
        tools, _ = self.snapshot(self.tool_session, 'positive-actions')
        require(tools['agent_tool_executions'] and tools['agent_action_attempts'], 'Action/attempt checks would be vacuous')
        return {'workflow': workflow, 'history_pages': history, 'SSE_reconnect': events, 'ledger': evidence,
                'positive_execution_count': len(tools['agent_tool_executions']),
                'positive_attempt_count': len(tools['agent_action_attempts']), 'invariant_violations': 0}

    def c12(self):
        self.prerequisite(self.tool_session, 'C11 must create a resolved terminal session with action evidence')
        sid = self.tool_session
        before, baseline = self.snapshot(sid, 'protection-before')
        response = self.c.http.get(f'/sessions/{sid}/retention')
        require(response.status_code == 200, 'Retention plan is unavailable')
        plan = response.json()
        protected = {'agent_commands', 'agent_commit_receipts', 'agent_action_attempts', 'agent_runs',
                     'agent_tool_executions', 'agent_session_transcript', 'agent_session_calls',
                     'agent_sessions', 'agent_session_context', 'agent_archive_receipts'}
        require(plan['default_policy'] == 'keep_all' and plan['eligible'] and
                protected <= set(plan['protected_tables']) and
                set(plan['prunable_tables']) == {'agent_session_events', 'agent_session_diagnostics'},
                'Retention declarations violate the protection boundary')
        self.subchecks['C12-retention-declarations'] = {'status': 'passed', 'plan': plan}
        if self.skip_pruning:
            raise NotExercised('Retention declarations passed; actual administrative pruning explicitly skipped')
        export = self.c.directory / 'protected-telemetry-export.jsonl'
        self.prompt(f'Use the deployment\'s normal administrative tooling to export session {sid} to {export}, then prune only its telemetry while retaining one event (and at least 32 diagnostics per family). Use a fresh verified export; do not issue direct SQL or change schema.')
        after, evidence = self.snapshot(sid, 'protection-after')
        require(len(after['agent_archive_receipts']) == len(before['agent_archive_receipts'])+1, 'No new administrative archive receipt was recorded')
        receipt = next(r for r in after['agent_archive_receipts'] if r['archive_id'] not in {v['archive_id'] for v in before['agent_archive_receipts']})
        header, manifest, archived = inspect_archive(export)
        require(header['session_id'] == sid and header['archive_id'] == receipt['archive_id'] and
                header['snapshot_revision'] == before['agent_sessions'][0]['revision'] and
                manifest['sha256'] == receipt['sha256'], 'Verified archive does not match the tested session/revision/receipt')
        require({k: len(v) for k, v in before.items()} == manifest['counts'], 'Export omitted pre-pruning ledger records')
        for table in before:
            require(sorted(canonical(row) for row in before[table]) == sorted(canonical(row) for row in archived[table]),
                    'Archive changed or omitted pre-pruning content in '+table)
        preserved(before, after, pruning=True)
        for table in ('agent_runs', 'agent_tool_executions', 'agent_action_attempts'):
            require(before[table] == after[table], 'Pruning changed protected '+table)
        for table in ('agent_session_events', 'agent_session_diagnostics'):
            ids = 'event_id' if table == 'agent_session_events' else None
            key = (lambda r: r[ids]) if ids else (lambda r: (r['family'], r['ordinal']))
            exported = {key(r): r for r in archived[table]}
            require(all(exported.get(key(r)) == r for r in before[table]), 'Archive did not preserve original telemetry')
        require(len(after['agent_session_events']) < len(before['agent_session_events']), 'Telemetry pruning did not remove any events')
        self.pruned.add(sid)
        replays = []
        for command in before['agent_commands']:
            request = command['request']['input']
            replays.append(self.replay({'sid': sid, 'key': command['idempotency_key'], 'body': request}, command['route']))
        self.subchecks['C12-actual-pruning'] = {'status': 'passed', 'archive_id': receipt['archive_id']}
        return {'session_id': sid, 'plan': plan, 'before': baseline, 'after': evidence,
                'archive': str(export), 'archive_sha256': manifest['sha256'], 'original_command_replays': replays,
                'expected_protected_record_loss': 0, 'observed_protected_record_loss': 0}
