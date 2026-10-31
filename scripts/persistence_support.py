"""Shared contract-2 persistence qualification with retained PostgreSQL evidence."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time
from uuid import UUID, uuid4

import httpx
import psycopg
from psycopg.conninfo import conninfo_to_dict

from idempotency_contract import CONTRACT, SCHEMA_VERSION, canonical, request_fingerprint, vectors
from schema_layout import validate

ROOT = Path(__file__).resolve().parents[1]
BUSINESS = {
    'agent_fixture_access_logs': 'line_number',
    'agent_fixture_customers': 'id',
    'agent_fixture_orders': 'order_id',
    'agent_fixture_employees': 'emp_id',
}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def json_value(value):
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (tuple, list)):
        return [json_value(item) for item in value]
    return value


async def fixture_hashes(connection):
    result = {}
    for table, order in BUSINESS.items():
        cursor = await connection.execute(f'SELECT * FROM {table} ORDER BY {order}')
        digest = hashlib.sha256()
        count = 0
        async for row in cursor:
            digest.update(canonical(json_value(row)) + b'\n')
            count += 1
        result[table] = {'count': count, 'sha256': digest.hexdigest()}
    return result


async def setup_checks(session_url, fixture_url):
    async with await psycopg.AsyncConnection.connect(session_url, connect_timeout=10) as connection:
        await connection.execute('SET TRANSACTION READ ONLY')
        await validate(connection, 'postgres.sql', 'validate.sql')
    async with await psycopg.AsyncConnection.connect(fixture_url, connect_timeout=10) as connection:
        await connection.execute('SET TRANSACTION READ ONLY')
        await validate(connection, 'fixtures.sql', 'validate-fixtures.sql')
        expected = json.loads((ROOT / 'schema' / 'fixture-oracles.json').read_text())
        require(await fixture_hashes(connection) == expected['tables'], 'Business fixture contents differ from pinned fingerprints')
        cursor = await connection.execute('''SELECT vector_id,namespace,session_id::text,execution_id::text,action_type,target,arguments,
            expected_key,expected_payload_sha256,canonical_key,canonical_payload FROM agent_fixture_idempotency_vectors ORDER BY vector_id''')
        actual = await cursor.fetchall()
        names = ['vector_id','namespace','session_id','execution_id','action_type','target','arguments',
                 'expected_key','expected_payload_sha256','canonical_key','canonical_payload']
        expected_vectors = sorted(vectors()['actions'], key=lambda v: v['vector_id'])
        require(canonical([dict(zip(names, row)) for row in actual]) == canonical(expected_vectors), 'Seeded identity vectors differ from the fixed vectors')
    return {'layout': 'schema 6, 12 shared / 6 fixture tables', 'business_fingerprints': 'matched', 'identity_vectors': 'matched'}


class APIReplay:
    def __init__(self, base_url, database_url):
        self.http = httpx.Client(base_url=base_url.rstrip('/'), timeout=30, trust_env=False)
        self.database_url = database_url
        self.sessions = []
        self.commands = []
        self.marker = 'persist-v2-' + uuid4().hex

    def rows(self, query, params=()):
        options = conninfo_to_dict(self.database_url).get('options', '')+' -c default_transaction_read_only=on -c statement_timeout=30000'
        with psycopg.connect(self.database_url, options=options, connect_timeout=10) as connection:
            connection.execute("SET LOCAL TIME ZONE 'UTC'")
            return connection.execute(query, params).fetchall()

    def image(self, sid):
        tables = ['agent_sessions','agent_commands','agent_commit_receipts','agent_session_transcript',
                  'agent_session_context','agent_session_events','agent_runs','agent_tool_executions',
                  'agent_action_attempts','agent_session_diagnostics','agent_session_calls','agent_archive_receipts']
        return canonical({table: self.rows(f'SELECT to_jsonb(t) FROM {table} t WHERE session_id=%s ORDER BY to_jsonb(t)::text', (sid,)) for table in tables})

    def post(self, route, body, key=None):
        return self.http.post(route, json=body, headers={'Idempotency-Key': key} if key else {})

    def command(self, key, sid, route, body, response):
        rows = self.rows('''SELECT command_id::text,request_sha256,status,response_status,response_body
            FROM agent_commands WHERE idempotency_key=%s AND session_id=%s''', (key, sid))
        require(len(rows) == 1, 'Expected one command for a request key')
        command_id, fingerprint, status, code, saved = rows[0]
        require(fingerprint == request_fingerprint('POST', route, body), 'Command request fingerprint differs')
        require(status in ('succeeded', 'failed'), 'Synchronous operation did not settle its command')
        require(code == response.status_code and canonical(saved) == canonical(response.json()), 'Command did not store the original JSON response')
        observed = self.http.get('/commands/' + command_id)
        require(observed.status_code == 200 and observed.json().get('command_id') == command_id, 'Command status GET is missing or incorrect')
        self.commands.append(command_id)
        return command_id

    def replay(self, route, body, key, original, sid):
        before = self.image(sid)
        response = self.post(route, body, key)
        require(response.status_code == original.status_code and canonical(response.json()) == canonical(original.json()), 'Replay changed the original response')
        require(response.headers.get('Idempotency-Replayed', '').lower() == 'true', 'Replay header missing')
        require(self.image(sid) == before, 'Replay changed durable state or history')

    def run(self):
        health = self.http.get('/health')
        require(health.status_code == 200 and health.json().get('contract_version') == CONTRACT
                and health.json().get('storage_schema') == SCHEMA_VERSION,
                'Target runtime has not implemented/advertised contract 2.0.0 / schema 6')
        body = {'config_ref': 'completion-no-workflow.yaml', 'user_prompt': self.marker}
        key = str(uuid4())
        response = self.post('/sessions', body, key)
        require(response.status_code == 201, f'Keyed creation failed: HTTP {response.status_code}')
        sid = response.json()['session']['id']
        stored = self.rows('SELECT state FROM agent_sessions WHERE session_id=%s', (sid,))
        require(len(stored) == 1 and stored[0][0]['goal'] == self.marker, 'Created session identity is incorrect')
        self.sessions.append(sid)
        self.command(key, sid, '/sessions', body, response)
        self.replay('/sessions', dict(reversed(list(body.items()))), key, response, sid)

        before = self.image(sid)
        conflict = self.post('/sessions', {**body, 'user_prompt': self.marker + '-changed'}, key)
        require(conflict.status_code == 409 and conflict.json().get('code') == 'idempotency_key_reused', 'Changed payload was not rejected with the defined conflict')
        wrong_route = self.post(f'/sessions/{sid}/messages', {'content': 'wrong-route'}, key)
        require(wrong_route.status_code == 409 and wrong_route.json().get('code') == 'idempotency_key_reused', 'Same key on another route was not rejected')
        require(self.image(sid) == before, 'Rejected key reuse changed the ledger')
        missing = self.post('/sessions', {**body, 'user_prompt': self.marker + '-missing'})
        require(missing.status_code == 422 and missing.json().get('code') == 'idempotency_key_required', 'Missing request key was accepted')
        invalid = self.post('/sessions', body, 'contains space')
        require(invalid.status_code == 422 and invalid.json().get('code') == 'idempotency_key_invalid', 'Invalid request key was accepted')
        leaked = self.rows("SELECT count(*) FROM agent_sessions WHERE state->>'goal'=%s", (self.marker + '-missing',))
        require(leaked[0][0] == 0, 'Missing-key request created a session')

        route = f'/sessions/{sid}/messages'
        message = {'content': self.marker + '-first'}
        message_key = str(uuid4())
        first = self.post(route, message, message_key)
        require(first.status_code == 200, 'Keyed message append failed')
        self.command(message_key, sid, route, message, first)
        second_body = {'content': self.marker + '-second'}
        second_key = str(uuid4())
        second = self.post(route, second_body, second_key)
        require(second.status_code == 200, 'Second intentional message append failed')
        self.command(second_key, sid, route, second_body, second)
        self.replay(route, message, message_key, first, sid)
        self.replay('/sessions', body, key, response, sid)

        concurrent_key = str(uuid4())
        concurrent_body = {'content': self.marker + '-concurrent'}
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.post, route, concurrent_body, concurrent_key) for _ in range(2)]
            responses = [future.result() for future in futures]
        require(all(r.status_code in (200,202) for r in responses), 'Concurrent identical requests conflicted or failed')
        settled = next((r for r in responses if r.status_code == 200), None)
        deadline = time.monotonic() + 30
        while settled is None and time.monotonic() < deadline:
            retry = self.post(route, concurrent_body, concurrent_key)
            if retry.status_code == 200:
                settled = retry
            else:
                require(retry.status_code == 202, 'In-progress duplicate returned an unexpected response')
                time.sleep(0.1)
        require(settled is not None, 'Concurrent command did not settle within the watchdog')
        concurrent_command = self.command(concurrent_key, sid, route, concurrent_body, settled)
        for response in responses:
            if response.status_code == 200:
                require(canonical(response.json()) == canonical(settled.json()), 'Concurrent duplicate returned a different terminal response')
            else:
                require(response.json().get('command_id') == concurrent_command, 'Concurrent duplicate returned another command identity')
        self.replay(route, concurrent_body, concurrent_key, settled, sid)
        count = self.rows("SELECT count(*) FROM agent_session_transcript WHERE session_id=%s AND payload->>'role'='user' AND payload->>'content'=%s", (sid, concurrent_body['content']))[0][0]
        require(count == 1, 'Concurrent command appended multiple messages')
        receipts = self.rows('SELECT expected_revision,committed_revision FROM agent_commit_receipts WHERE session_id=%s ORDER BY committed_revision', (sid,))
        current = self.rows('SELECT revision FROM agent_sessions WHERE session_id=%s', (sid,))[0][0]
        context = self.rows('SELECT revision FROM agent_session_context WHERE session_id=%s', (sid,))[0][0]
        require(receipts == [(n-1,n) for n in range(1,current+1)] and context == current, 'Receipt revisions/context do not form the committed chain')
        self.creation = (sid, body, key, response)
        return {'commands': len(self.commands), 'checks': ['keyed creation','reordered replay','payload/route conflict',
                'missing/invalid key','message replay after later writes','concurrent duplicates','command status','receipt/revision chain'],
                'retained_sessions': list(self.sessions), 'retained_commands': list(self.commands)}

    def close(self):
        self.http.close()

