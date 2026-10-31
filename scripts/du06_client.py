"""Shared request accounting, scoped command reads and consistent ledger evidence."""
import json
from pathlib import Path
import threading
import time
from uuid import uuid4

import httpx

from idempotency_contract import canonical, request_fingerprint
from persistence_ledger import capture, verify
from persistence_support import APIReplay, require


def normalized_input(route, body):
    """Defaults in the common contract request schemas, independent of any runtime."""
    value = dict(body)
    if route == '/sessions':
        value.setdefault('config_ref', 'log-analysis.yaml')
    elif route.endswith(('/run', '/run/stream', '/resume')):
        value.setdefault('max_steps', None)
        if route.endswith('/resume'):
            value.setdefault('content', None)
    return value


class Client(APIReplay):
    def __init__(self, base_url, database_url, directory, timeout):
        super().__init__(base_url, database_url)
        self.http.timeout = httpx.Timeout(timeout)
        self.directory, self.timeout = Path(directory), timeout
        self.case_id = 'preflight'
        self.requests, self.snapshots, self.observations = [], [], []
        self.settlements = []
        self.namespace = self.client_scope = None
        self.lock = threading.RLock()
        self.sequence = 0
        self.creation = None

    def post_to(self, target, route, body, key):
        start = time.monotonic()
        entry = {'case': self.case_id, 'key': key, 'method': 'POST', 'route': route,
                 'target': target, 'request_sha256': request_fingerprint('POST', route, normalized_input(route, body)),
                 'started_at': time.time(), 'started_monotonic': start}
        chunks = []
        try:
            with self.http.stream('POST', target.rstrip('/')+route, json=body,
                                  headers={'Idempotency-Key': key}) as incoming:
                entry['http_status'] = incoming.status_code
                progress = ''
                for chunk in incoming.iter_bytes():
                    if time.monotonic()-start > self.timeout:
                        raise TimeoutError('Response exceeded the absolute request watchdog')
                    chunks.append(chunk)
                    if incoming.headers.get('Content-Type', '').startswith('text/event-stream'):
                        progress += chunk.decode('utf-8', errors='replace')
                        while '\n' in progress:
                            line, progress = progress.split('\n', 1)
                            if line.startswith('data:'):
                                try:
                                    event = json.loads(line[5:])
                                    detail = event.get('message') if event.get('type') == 'status' else event.get('step') or event.get('code', '')
                                    print(f"  {self.case_id} stream: {event.get('type')} {detail or ''}", flush=True)
                                except (ValueError, TypeError):
                                    pass  # Progress printing is not the SSE assertion/parser.
                if time.monotonic()-start > self.timeout:
                    raise TimeoutError('Response exceeded the absolute request watchdog')
                # iter_bytes has decoded transport compression; preserve semantic
                # response headers without applying that decoding a second time.
                headers = dict(incoming.headers)
                headers.pop('content-encoding', None)
                headers.pop('content-length', None)
                response = httpx.Response(incoming.status_code, headers=headers,
                                          content=b''.join(chunks), request=incoming.request)
            entry.update({'http_status': response.status_code, 'replayed': response.headers.get('Idempotency-Replayed'),
                          'command_id': response.headers.get('Idempotency-Command-Id')})
            if route == '/sessions' and response.status_code == 201:
                returned_sid = response.json()['session']['id']
                with self.lock:
                    if returned_sid not in self.sessions:
                        self.sessions.append(returned_sid)
            self.save_request(entry, {'request': body, 'response_status': response.status_code,
                                     'response_body': response.text})
            return response
        except Exception as error:
            entry['transport_error'] = type(error).__name__
            self.save_request(entry, {'request': body, 'transport_error': type(error).__name__,
                                      'partial_response': b''.join(chunks).decode('utf-8', errors='replace')})
            raise
        finally:
            entry['elapsed_seconds'] = time.monotonic()-start
            with self.lock:
                self.requests.append(entry)
                with (self.directory / 'requests.jsonl').open('a') as stream:
                    stream.write(json.dumps(entry)+'\n')

    def post(self, route, body, key=None):
        return self.post_to(str(self.http.base_url).rstrip('/'), route, body, key or str(uuid4()))

    def save_request(self, entry, payload):
        with self.lock:
            self.sequence += 1
            path = self.directory / f'request-{self.sequence:04d}.json'
            path.write_text(json.dumps({**entry, **payload}, indent=2)+'\n')
            entry['evidence_path'] = str(path)

    def observe(self, sid, label):
        with self.lock:
            self.sequence += 1
            path = self.directory / f'ledger-{self.sequence:04d}-{label}.json'
        rows, evidence = capture(self.database_url, sid, path)
        # Retain evidence even when an assertion below fails.
        evidence['case'] = self.case_id
        self.snapshots.append(evidence)
        evidence['verification'] = verify(rows)
        evidence['command_states'] = [{'command_id': row['command_id'], 'status': row['status']} for row in rows['agent_commands']]
        return rows, evidence

    def command_row(self, key=None, command_id=None):
        clauses, params = [], []
        if key is not None:
            clauses.append('idempotency_key=%s')
            params.append(key)
        else:
            clauses.append('command_id=%s')
            params.append(command_id)
        if self.namespace is not None:
            clauses.extend(('namespace=%s', 'client_scope=%s'))
            params.extend((self.namespace, self.client_scope))
        rows = self.rows('SELECT to_jsonb(c) FROM agent_commands c WHERE '+' AND '.join(clauses), params)
        require(len(rows) <= 1, 'Multiple commands share one scoped request key')
        if not rows:
            return None
        row = rows[0][0]
        self.observations.append({'case': self.case_id, 'command_id': row['command_id'],
                                  'status': row['status'], 'observed_at': time.time()})
        return row

    def wait(self, label, read, predicate):
        deadline = time.monotonic()+self.timeout
        while True:
            value = read()
            if predicate(value):
                return value
            require(time.monotonic() < deadline, 'Watchdog expired: '+label)
            time.sleep(.1)

    def settled(self, key):
        started = time.monotonic()
        row = self.wait('command settlement', lambda: self.command_row(key=key),
                        lambda r: r is not None and r['status'] in ('succeeded', 'failed') and r['owner'] is None)
        if row['session_id'] not in self.sessions:
            self.sessions.append(row['session_id'])
        self.wait('session ownership release', lambda: self.rows(
            'SELECT lease_owner FROM agent_sessions WHERE session_id=%s', (row['session_id'],)),
            lambda values: values and values[0][0] is None)
        if not any(item['key'] == key for item in self.settlements):
            submissions = [item['started_monotonic'] for item in self.requests if item['key'] == key and 'started_monotonic' in item]
            self.settlements.append({'case': self.case_id, 'key': key, 'command_id': row['command_id'],
                'status': row['status'], 'verification_wait_seconds': time.monotonic()-started,
                'time_to_observed_terminal_seconds': time.monotonic()-min(submissions) if submissions else None,
                'watchdog_seconds': self.timeout})
        return row

    def create(self, prompt=None, config='completion-no-workflow.yaml', target=None, key=None):
        body = {'config_ref': config, 'user_prompt': prompt or 'du06-'+uuid4().hex}
        key = key or str(uuid4())
        response = self.post_to(target or str(self.http.base_url), '/sessions', body, key)
        require(response.status_code == 201, f'Session creation returned HTTP {response.status_code}')
        sid = response.json()['session']['id']
        if sid not in self.sessions:
            self.sessions.append(sid)
        row = self.settled(key)
        self.check_response(row, '/sessions', body, response)
        if self.namespace is None:
            self.namespace, self.client_scope = row['namespace'], row['client_scope']
        return {'sid': sid, 'key': key, 'body': body, 'response': response, 'command_id': row['command_id']}

    def check_response(self, row, route, body, response):
        require(row['request_sha256'] == request_fingerprint('POST', route, normalized_input(route, body)), 'Request fingerprint differs')
        require(row['response_status'] == response.status_code and canonical(row['response_body']) == canonical(response.json()),
                'Recorded command response differs from API response')
        observed = self.http.get('/commands/'+row['command_id'])
        require(observed.status_code == 200 and observed.json()['command_id'] == row['command_id'] and
                observed.json()['status'] == row['status'], 'Command API differs from PostgreSQL')

    def replay_record(self, record, route='/sessions', target=None):
        sid, key, body = record['sid'], record['key'], record['body']
        before, baseline = self.observe(sid, 'before-replay')
        row = self.command_row(key=key)
        response = self.post_to(target or str(self.http.base_url), route, body, key)
        self.check_response(row, route, body, response)
        require(response.headers.get('Idempotency-Replayed', '').lower() == 'true', 'Missing replay header')
        after, evidence = self.observe(sid, 'after-replay')
        require(before == after, 'Settled replay changed durable records')
        return {'before': baseline, 'after': evidence, 'command_id': row['command_id'],
                'effect_delta': 0, 'revision_delta': 0}
