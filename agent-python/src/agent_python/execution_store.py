"""PostgreSQL ownership and execution rows; snapshots use the same transaction."""
from __future__ import annotations

import json
from datetime import datetime
import psycopg
from psycopg.rows import dict_row

from .execution import OwnershipConflict, LeaseLost


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

    async def cancel_run(self, session_id, run_id=None):
        # Serialize with run-start/finish, so an acknowledged cancel targets a
        # committed running identity rather than racing an invisible new run.
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            await conn.execute('SELECT session_id FROM agent_sessions WHERE session_id=%s FOR UPDATE', (session_id,))
            cursor = await conn.execute("""UPDATE agent_runs SET cancel_requested=TRUE,
                cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp())
                WHERE session_id=%s AND status='running' AND (%s::uuid IS NULL OR run_id=%s::uuid) RETURNING run_id::text""", (session_id,run_id,run_id))
            return [row[0] for row in await cursor.fetchall()]

    async def execution(self, session_id, call_id):
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT execution_id::text,run_id::text,call_id,tool,arguments,policy,
                idempotency_key,attempts,status,result,command_id::text,namespace,target,payload_sha256,key_algorithm,created_at FROM agent_tool_executions
                WHERE session_id=%s AND call_id=%s''', (session_id, call_id))
            row = await cursor.fetchone()
            if row:
                row['created_at'] = row['created_at'].isoformat()
            return row

    async def call_seen(self, session_id, call_id):
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor = await conn.execute('SELECT EXISTS(SELECT 1 FROM agent_session_calls WHERE session_id=%s AND call_id=%s)', (session_id,call_id))
            return (await cursor.fetchone())[0]

    async def execution_history(self, session_id, after=None, limit=100):
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT execution_id::text,run_id::text,call_id,tool,arguments,policy,
                idempotency_key,attempts,status,result,fence,created_at,updated_at,started_at,finished_at,command_id::text,namespace,target,payload_sha256,key_algorithm
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
        from .commands import current_command
        from .identity import canonical, action_key, payload_fingerprint
        command = current_command.get()
        if state.execution_owner and not state.execution_maintenance and state.run_id and state.run_started_at:
            await cursor.execute('''INSERT INTO agent_runs(run_id,session_id,command_id,owner,fence,status,started_at,ended_at,termination_reason,last_decision_step)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_id) DO UPDATE SET
                status=EXCLUDED.status,ended_at=EXCLUDED.ended_at,termination_reason=EXCLUDED.termination_reason,
                last_decision_step=EXCLUDED.last_decision_step
                WHERE agent_runs.session_id=EXCLUDED.session_id AND agent_runs.command_id=EXCLUDED.command_id''',
                (state.run_id,state.id,state.run_command_id,state.execution_owner,state.execution_fence,state.run_status,
                 state.run_started_at,state.run_ended_at,state.run_termination_reason,state.step_count))
        fields = ['execution_id','run_id','call_id','tool','arguments','policy','command_id','namespace','target','idempotency_key','payload_sha256','key_algorithm']
        for item in state.execution_pending:
            if not 0 <= item['attempts'] <= item['policy']['max_attempts']:
                raise ValueError('Execution attempts exceed immutable policy')
            if item['namespace'] != command.namespace or item['idempotency_key'] != action_key(item['namespace'],state.id,item['execution_id']):
                raise ValueError('Invalid deterministic action identity')
            if item['payload_sha256'] != payload_fingerprint(item['tool'],item['target'],item['arguments']):
                raise ValueError('Frozen action fingerprint changed')
            if item['status'] in {'succeeded','failed'}:
                results = [m for m in state.memory if m.role=='tool' and m.tool_call_id==item['call_id']]
                if (len(results)!=1 or results[0].name!=item['tool'] or results[0].content!=item['result']
                        or any(c['id']==item['call_id'] for c in state.pending_tool_calls)):
                    raise ValueError('Tool outcome requires correlated result and consumed pending call')
            await cursor.execute('SELECT payload,attempts,status,result FROM agent_tool_executions WHERE session_id=%s AND call_id=%s FOR UPDATE', (state.id,item['call_id']))
            old = await cursor.fetchone()
            if old:
                if (canonical([old[0][key] for key in fields]) != canonical([item[key] for key in fields])
                        or item['attempts']<old[1] or item['attempts']>old[1]+1
                        or old[2] in ('succeeded','failed') and (item['attempts'],item['status'],item['result'])!=old[1:]):
                    raise ValueError('Execution identity, attempts or terminal outcome changed')
            if not state.execution_owner:
                raise OwnershipConflict('Execution writes require session ownership')
            await cursor.execute('''INSERT INTO agent_tool_executions(execution_id,session_id,run_id,call_id,tool,arguments,policy,
                command_id,namespace,target,idempotency_key,payload_sha256,key_algorithm,attempts,status,result,owner,fence,payload)
                VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT(session_id,call_id) DO UPDATE SET attempts=EXCLUDED.attempts,status=EXCLUDED.status,
                result=EXCLUDED.result,owner=EXCLUDED.owner,fence=EXCLUDED.fence,payload=EXCLUDED.payload,updated_at=clock_timestamp()''',
                (item['execution_id'],state.id,item['run_id'],item['call_id'],item['tool'],encode(item['arguments']),encode(item['policy']),
                 item['command_id'],item['namespace'],item['target'],item['idempotency_key'],item['payload_sha256'],item['key_algorithm'],
                 item['attempts'],item['status'],item['result'],state.execution_owner,state.execution_fence,encode(item)))
            await cursor.execute('''UPDATE agent_tool_executions SET
                started_at=CASE WHEN status='started' THEN COALESCE(started_at,clock_timestamp()) ELSE started_at END,
                finished_at=CASE WHEN status IN ('succeeded','failed') THEN COALESCE(finished_at,clock_timestamp()) ELSE NULL END
                WHERE execution_id=%s''', (item['execution_id'],))
        for item in state.attempts_pending:
            await cursor.execute('''SELECT attempt_id::text,payload_sha256,command_id::text,owner::text,fence,kind,status
                ,external_reference,evidence,error,started_at,finished_at
                FROM agent_action_attempts WHERE execution_id=%s AND attempt_number=%s FOR UPDATE''', (item['execution_id'],item['attempt_number']))
            old = await cursor.fetchone()
            identity = (item['attempt_id'],item['payload_sha256'],item['command_id'],item['owner'],item['fence'],item['kind'])
            if old and (old[:6]!=identity or old[6]!='started' and old[6]!=item['status']):
                raise ValueError('Attempt identity or settled outcome changed')
            if old and old[10] != datetime.fromisoformat(item['started_at']):
                raise ValueError('Attempt start time changed')
            if old and old[6]!='started':
                if (canonical(list(old[7:10])) != canonical([item.get('external_reference'),item.get('evidence'),item.get('error')])
                        or old[11] != datetime.fromisoformat(item['finished_at'])):
                    raise ValueError('Settled attempt evidence changed')
            if not old:
                await cursor.execute('SELECT COALESCE(max(attempt_number),0)+1 FROM agent_action_attempts WHERE execution_id=%s',(item['execution_id'],))
                if (await cursor.fetchone())[0] != item['attempt_number']:
                    raise ValueError('Attempt number contains a gap')
            await cursor.execute('''INSERT INTO agent_action_attempts(attempt_id,session_id,execution_id,command_id,attempt_number,
                kind,status,payload_sha256,owner,fence,external_reference,evidence,error,started_at,finished_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s)
                ON CONFLICT(execution_id,attempt_number) DO UPDATE SET status=EXCLUDED.status,
                external_reference=EXCLUDED.external_reference,evidence=EXCLUDED.evidence,error=EXCLUDED.error,finished_at=EXCLUDED.finished_at''',
                (item['attempt_id'],state.id,item['execution_id'],item['command_id'],item['attempt_number'],item['kind'],item['status'],
                 item['payload_sha256'],item['owner'],item['fence'],item.get('external_reference'),encode(item.get('evidence')),
                 encode(item.get('error')),item['started_at'],item.get('finished_at')))

    async def next_attempt(self, execution_id):
        async with await psycopg.AsyncConnection.connect(self.database_url) as db:
            cursor = await db.execute('SELECT COALESCE(max(attempt_number),0)+1 FROM agent_action_attempts WHERE execution_id=%s', (execution_id,))
            return (await cursor.fetchone())[0]
