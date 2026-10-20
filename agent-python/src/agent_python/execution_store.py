"""PostgreSQL ownership and execution rows; snapshots use the same transaction."""
from __future__ import annotations

import json
import psycopg
from psycopg.rows import dict_row

from .execution import OwnershipConflict, LeaseLost


DDL = [
    'ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS lease_owner UUID',
    'ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS lease_fence BIGINT NOT NULL DEFAULT 0',
    'ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ',
    '''CREATE TABLE IF NOT EXISTS agent_runs (
        run_id UUID PRIMARY KEY, session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
        owner UUID NOT NULL, fence BIGINT NOT NULL, status TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ, termination_reason TEXT,
        last_decision_step BIGINT NOT NULL, cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
        cancel_requested_at TIMESTAMPTZ,
        UNIQUE(session_id,run_id))''',
    'CREATE INDEX IF NOT EXISTS agent_runs_active_idx ON agent_runs(session_id,status)',
    '''CREATE TABLE IF NOT EXISTS agent_tool_executions (
        execution_id UUID PRIMARY KEY, session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
        run_id UUID NOT NULL, call_id TEXT NOT NULL, tool TEXT NOT NULL, arguments JSONB NOT NULL,
        policy JSONB NOT NULL, idempotency_key UUID NOT NULL, attempts INTEGER NOT NULL CHECK(attempts>=0),
        status TEXT NOT NULL CHECK(status IN ('pending','started','succeeded','failed','uncertain')),
        result TEXT, owner UUID NOT NULL, fence BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
        UNIQUE(session_id,call_id),
        FOREIGN KEY(session_id,run_id) REFERENCES agent_runs(session_id,run_id),
        CHECK((status IN ('succeeded','failed')) = (result IS NOT NULL)))''',
    'ALTER TABLE agent_runs ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ',
    'ALTER TABLE agent_tool_executions ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ',
    'ALTER TABLE agent_tool_executions ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ',
    "INSERT INTO agent_schema_migrations(component,version) VALUES ('agent-python',4) ON CONFLICT DO NOTHING",
]


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


class ExecutionStore:
    async def acquire(self, session_id, owner, seconds=30):
        if not 0 < seconds <= 3600:
            raise ValueError('Lease duration must be in (0,3600] seconds')
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor = await conn.execute('''UPDATE agent_sessions SET lease_owner=%s,
                lease_fence=lease_fence+1,lease_expires_at=clock_timestamp()+(%s * interval '1 second')
                WHERE session_id=%s AND (lease_owner IS NULL OR lease_expires_at<=clock_timestamp())
                RETURNING lease_fence''', (owner, seconds, session_id))
            row = await cursor.fetchone()
            if row is None:
                raise OwnershipConflict('Session is owned by another worker or does not exist; retry after release/expiry.')
            return row[0]

    async def renew(self, session_id, owner, fence, seconds=30):
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor = await conn.execute('''UPDATE agent_sessions SET lease_expires_at=clock_timestamp()+(%s * interval '1 second')
                WHERE session_id=%s AND lease_owner=%s AND lease_fence=%s AND lease_expires_at>clock_timestamp()
                RETURNING lease_fence''', (seconds, session_id, owner, fence))
            if await cursor.fetchone() is None:
                raise LeaseLost('Lease expired or ownership changed; stale worker must stop.')

    async def release(self, session_id, owner, fence):
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            await conn.execute('''UPDATE agent_sessions SET lease_owner=NULL,lease_expires_at=NULL
                WHERE session_id=%s AND lease_owner=%s AND lease_fence=%s''', (session_id, owner, fence))

    async def check_owner(self, session_id, owner, fence, run_id=None):
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor = await conn.execute('''SELECT lease_owner=%s AND lease_fence=%s AND lease_expires_at>clock_timestamp(),
                EXISTS(SELECT 1 FROM agent_runs WHERE run_id=%s AND session_id=%s AND cancel_requested)
                FROM agent_sessions WHERE session_id=%s''', (owner, fence, run_id, session_id, session_id))
            row = await cursor.fetchone()
            if not row or row[0] is not True:
                raise LeaseLost('Lease no longer valid; reload before recovery.')
            return row[1]

    async def cancel_run(self, session_id):
        # Serialize with run-start/finish, so an acknowledged cancel targets a
        # committed running identity rather than racing an invisible new run.
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            await conn.execute('SELECT session_id FROM agent_sessions WHERE session_id=%s FOR UPDATE', (session_id,))
            cursor = await conn.execute("""UPDATE agent_runs SET cancel_requested=TRUE,
                cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp())
                WHERE session_id=%s AND status='running' RETURNING run_id::text""", (session_id,))
            return [row[0] for row in await cursor.fetchall()]

    async def execution(self, session_id, call_id):
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT execution_id::text,run_id::text,call_id,tool,arguments,policy,
                idempotency_key::text,attempts,status,result FROM agent_tool_executions
                WHERE session_id=%s AND call_id=%s''', (session_id, call_id))
            return await cursor.fetchone()

    async def call_seen(self, session_id, call_id):
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor = await conn.execute('SELECT EXISTS(SELECT 1 FROM agent_session_calls WHERE session_id=%s AND call_id=%s)', (session_id,call_id))
            return (await cursor.fetchone())[0]

    async def execution_history(self, session_id, after=None, limit=100):
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT execution_id::text,run_id::text,call_id,tool,arguments,policy,
                idempotency_key::text,attempts,status,result,fence,created_at,updated_at,started_at,finished_at
                FROM agent_tool_executions WHERE session_id=%s AND (%s::uuid IS NULL OR execution_id>%s::uuid)
                ORDER BY execution_id LIMIT %s''', (session_id, after, after, limit))
            return await cursor.fetchall()

    async def runs(self, session_id):
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT run_id::text,fence,status,started_at,ended_at,termination_reason,
                last_decision_step,cancel_requested,cancel_requested_at FROM agent_runs WHERE session_id=%s ORDER BY started_at,run_id''', (session_id,))
            return await cursor.fetchall()

    async def abandoned(self, limit=100):
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT r.session_id::text,r.run_id::text,r.status,r.last_decision_step
                FROM agent_runs r JOIN agent_sessions s USING(session_id)
                WHERE r.status='running' AND (s.lease_owner IS NULL OR s.lease_expires_at<=clock_timestamp())
                ORDER BY r.started_at LIMIT %s''', (limit,))
            return await cursor.fetchall()

    async def _guard_execution(self, cursor, state):
        await cursor.execute('''SELECT lease_owner::text,lease_fence,lease_expires_at>clock_timestamp()
            FROM agent_sessions WHERE session_id=%s''', (state.id,))
        row = await cursor.fetchone()
        if state.execution_owner is not None:
            if not row or row != (state.execution_owner, state.execution_fence, True):
                raise LeaseLost('Fenced commit rejected: lease expired or owner changed.')
        elif row and row[0] is not None and row[2]:
            raise OwnershipConflict('Unowned mutation rejected while a worker owns this session.')

    async def _write_execution(self, cursor, state):
        if state.execution_owner and not state.execution_maintenance and state.run_id and state.run_started_at:
            await cursor.execute('''INSERT INTO agent_runs(run_id,session_id,owner,fence,status,started_at,ended_at,termination_reason,last_decision_step)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_id) DO UPDATE SET
                owner=EXCLUDED.owner,fence=EXCLUDED.fence,status=EXCLUDED.status,ended_at=EXCLUDED.ended_at,
                termination_reason=EXCLUDED.termination_reason,last_decision_step=EXCLUDED.last_decision_step
                WHERE agent_runs.session_id=EXCLUDED.session_id''',
                (state.run_id,state.id,state.execution_owner,state.execution_fence,state.run_status,
                 state.run_started_at,state.run_ended_at,state.run_termination_reason,state.step_count))
        for item in state.execution_pending:
            if not 0 <= item['attempts'] <= item['policy']['max_attempts']:
                raise ValueError('Execution attempts exceed the immutable policy limit')
            if item['status'] in {'succeeded','failed'}:
                results = [m for m in state.memory if m.role == 'tool' and m.tool_call_id == item['call_id']]
                if (len(results) != 1 or results[0].name != item['tool'] or results[0].content != item['result']
                        or any(c['id'] == item['call_id'] for c in state.pending_tool_calls)):
                    raise ValueError('Tool outcome must commit with exactly one correlated result and consumed pending call')
            await cursor.execute('SELECT execution_id::text,tool,arguments,policy,idempotency_key::text,attempts,status,result FROM agent_tool_executions WHERE session_id=%s AND call_id=%s FOR UPDATE', (state.id,item['call_id']))
            old = await cursor.fetchone()
            identity = (item['execution_id'],item['tool'],item['arguments'],item['policy'],item['idempotency_key'])
            if old and (encode(old[:5]) != encode(identity) or item['attempts'] < old[5]
                        or item['attempts'] > old[5]+1
                        or (old[6] in {'succeeded','failed'} and (item['attempts'],item['status'],item['result']) != old[5:8])):
                raise ValueError('Tool execution identity/attempt/result cannot be rewritten')
            if state.execution_owner is None:
                raise OwnershipConflict('Tool execution writes require ownership')
            await cursor.execute('''INSERT INTO agent_tool_executions(execution_id,session_id,run_id,call_id,tool,arguments,policy,idempotency_key,attempts,status,result,owner,fence)
                VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(session_id,call_id) DO UPDATE SET run_id=EXCLUDED.run_id,attempts=EXCLUDED.attempts,
                status=EXCLUDED.status,result=EXCLUDED.result,owner=EXCLUDED.owner,fence=EXCLUDED.fence,updated_at=clock_timestamp()''',
                (item['execution_id'],state.id,item['run_id'],item['call_id'],item['tool'],encode(item['arguments']),
                 encode(item['policy']),item['idempotency_key'],item['attempts'],item['status'],item['result'],
                 state.execution_owner,state.execution_fence))
            await cursor.execute('''UPDATE agent_tool_executions SET
                started_at=CASE WHEN status='started' THEN clock_timestamp() ELSE started_at END,
                finished_at=CASE WHEN status IN ('succeeded','failed') THEN COALESCE(finished_at,clock_timestamp()) ELSE NULL END
                WHERE execution_id=%s''', (item['execution_id'],))
