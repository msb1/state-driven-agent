"""Atomic PostgreSQL snapshots, transcript, active-context references, and revisions."""
from __future__ import annotations
import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4
import psycopg
from psycopg.rows import dict_row
from .config import AgentConfig, resolved_config_hash
from .models import Message, SessionState
from .transcript import stage_messages
from .execution_store import ExecutionStore
from .storage import SelectiveStorage, HYDRATE
from .storage_state import hot_snapshot, stage_diagnostics, RECENT
from .retention import RetentionStorage
from .commands import CommandStore, current_command
from .identity import digest

DEFAULT_DATABASE_URL = "postgresql://user:password@192.168.1.50:5432/universal_state"

class RevisionConflict(Exception):
    def __init__(self, session_id, expected, actual):
        self.session_id, self.expected, self.actual = session_id, expected, actual
        super().__init__(f"Session revision conflict: expected {expected}, current {actual}; reload before retrying.")

class TranscriptIntegrityError(Exception):
    """Transcript identity/payload or active references would be corrupted."""

@dataclass(frozen=True)
class PersistedSession:
    state: SessionState
    config: AgentConfig

class SessionRepository(SelectiveStorage, RetentionStorage, ExecutionStore):
    def __init__(self, database_url: str | None = None, *, measure_io=False):
        self.measure_io, self.io_samples = measure_io, []
        self.database_url = database_url or os.getenv('AGENT_SESSION_DATABASE_URL', DEFAULT_DATABASE_URL)
        self.commands = CommandStore(self.database_url)

    async def validate_schema(self):
        from .schema_validation import validate
        try:
            async with await psycopg.AsyncConnection.connect(self.database_url) as connection:
                await connection.execute('SET TRANSACTION READ ONLY')
                await validate(connection,'postgres.sql','validate.sql')
        except (psycopg.errors.UndefinedTable,psycopg.errors.UndefinedColumn,RuntimeError) as error:
            raise RuntimeError('Shared schema 6 is missing or incompatible; run initialize_database.py explicitly. Servers do not change the schema.') from error

    async def initialize(self):
        await self.validate_schema()

    async def startup(self,limit=100,*,recover=True):
        from .execution import reconcile_abandoned
        await self.validate_schema()
        return await reconcile_abandoned(self,limit) if recover else await self.abandoned(limit)

    @staticmethod
    def _snapshot(state):
        return hot_snapshot(state)

    async def _write_transcript(self, cursor, state):
        await cursor.execute('SELECT COALESCE(max(sequence),0)+1 FROM agent_session_transcript WHERE session_id=%s', (state.id,))
        next_sequence = (await cursor.fetchone())[0]
        for entry in state.transcript_pending:
            if entry.sequence > next_sequence:
                raise TranscriptIntegrityError('Transcript sequence contains a gap')
            if entry.kind in {'compaction_summary', 'compaction_rejected'}:
                await self._validate_summary_sources(cursor, state, entry)
            await cursor.execute('''INSERT INTO agent_session_transcript
                (session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at)
                VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING message_id''',
                (state.id, entry.sequence, entry.message_id, entry.kind, _json(entry.payload), entry.run_id, entry.decision_step, entry.created_at))
            inserted = await cursor.fetchone()
            if inserted is not None:
                if entry.sequence != next_sequence:
                    raise TranscriptIntegrityError('New transcript record is out of order')
                next_sequence += 1
            else:
                await cursor.execute('''SELECT sequence,message_id::text,kind,payload,run_id::text,decision_step,created_at
                    FROM agent_session_transcript WHERE session_id=%s AND message_id=%s''', (state.id, entry.message_id))
                row = await cursor.fetchone()
                if row is None or (row[0], row[1], row[2], _json(row[3]), row[4], row[5], row[6]) != (
                        entry.sequence, entry.message_id, entry.kind, _json(entry.payload), entry.run_id, entry.decision_step, entry.created_at):
                    raise TranscriptIntegrityError('Transcript identity/sequence collision or changed immutable record')
        await cursor.execute('SELECT COALESCE(max(sequence),0)+1 FROM agent_session_transcript WHERE session_id=%s', (state.id,))
        if (await cursor.fetchone())[0] != state.next_transcript_sequence:
            raise TranscriptIntegrityError('Transcript sequence counter differs from archive')

    async def _validate_summary_sources(self, cursor, state, entry):
        from .context import source_text
        ids = entry.payload.get('source_message_ids', [])
        if not ids or len(ids) != len(set(ids)):
            raise TranscriptIntegrityError('Summary requires distinct source message identities')
        await cursor.execute('SELECT message_id::text,kind,payload FROM agent_session_transcript WHERE session_id=%s AND message_id=ANY(%s::uuid[])', (state.id, ids))
        rows = {row[0]: row for row in await cursor.fetchall()}
        if any(identity not in rows or rows[identity][1] != 'message' for identity in ids):
            raise TranscriptIntegrityError('Summary references unavailable source messages')
        previous = None
        previous_id = entry.payload.get('previous_ledger_id')
        if previous_id:
            await cursor.execute('SELECT kind,payload FROM agent_session_transcript WHERE session_id=%s AND message_id=%s', (state.id, previous_id))
            row = await cursor.fetchone()
            if row is None or row[0] not in {'compaction_summary', 'legacy_summary'}:
                raise TranscriptIntegrityError('Previous ledger reference is unavailable')
            previous = row[1].get('content')
        source = source_text(state.goal, [Message.model_validate(rows[identity][2]) for identity in ids], previous)
        if hashlib.sha256(source.encode()).hexdigest() != entry.payload.get('source_sha256'):
            raise TranscriptIntegrityError('Summary source fingerprint does not match archived inputs')

    async def _write_context(self, cursor, state):
        if not state.memory or state.memory[0].role != 'user' or state.memory[0].content != state.goal:
            raise TranscriptIntegrityError('Active context must retain its original user goal')
        ids = [message.message_id for message in state.memory]
        if len(ids) != len(set(ids)):
            raise TranscriptIntegrityError('Duplicate identity in active context')
        sequences = [message.sequence for message in state.memory]
        if any(value is None for value in sequences) or sequences != sorted(sequences):
            raise TranscriptIntegrityError('Active raw messages are not in transcript order')
        await cursor.execute('SELECT message_id::text,sequence,kind,payload,created_at FROM agent_session_transcript WHERE session_id=%s AND message_id=ANY(%s::uuid[])', (state.id, ids))
        rows = {row[0]: row for row in await cursor.fetchall()}
        for message in state.memory:
            row = rows.get(message.message_id)
            if (row is None or row[1] != message.sequence or row[2] != 'message'
                    or _json(row[3]) != _json(message.wire_payload())
                    or row[4] != message.created_at):
                raise TranscriptIntegrityError('Active message identity, payload, or timestamp is missing or changed')
        if state.active_ledger_id:
            await cursor.execute('SELECT kind,payload FROM agent_session_transcript WHERE session_id=%s AND message_id=%s', (state.id, state.active_ledger_id))
            ledger = await cursor.fetchone()
            if ledger is None or ledger[0] not in {'compaction_summary', 'legacy_summary'} or ledger[1].get('content') != state.compacted_context:
                raise TranscriptIntegrityError('Active ledger missing or changed')
            if ledger[0] == 'compaction_summary' and _json(ledger[1].get('ledger')) != _json(state.ledger_state):
                raise TranscriptIntegrityError('Typed ledger differs from its immutable transcript record')
        elif state.compacted_context is not None:
            raise TranscriptIntegrityError('Compacted context requires an archived ledger identity')
        await cursor.execute('''INSERT INTO agent_session_context(session_id,revision,goal_message_id,ledger_record_id,raw_message_ids,ledger_state)
            VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb) ON CONFLICT(session_id) DO UPDATE SET revision=EXCLUDED.revision,
            goal_message_id=EXCLUDED.goal_message_id,ledger_record_id=EXCLUDED.ledger_record_id,raw_message_ids=EXCLUDED.raw_message_ids,ledger_state=EXCLUDED.ledger_state''',
            (state.id, state.revision, ids[0], state.active_ledger_id, _json(ids[1:]),_json(state.ledger_state)))

    async def save(self, state, config, event=None):
        started = time.perf_counter()
        command = current_command.get()
        if command is None:
            raise ValueError('A durable command is required for every save')
        if state.needs_reload:
            raise RevisionConflict(state.id, state.revision, 'reload required after historical receipt')
        if event is not None:
            event = {**event, 'command_id': command.id, 'run_id': state.run_id}
        stage_diagnostics(state)
        stage_messages(state)
        config_hash = resolved_config_hash(config)
        if state.resolved_config_sha256 != config_hash:
            raise ValueError('Resolved configuration hash does not match session anchor')
        if state.pending_commit_id is None:
            state.pending_commit_id = str(uuid4())
        fingerprint = digest(['agent-commit-v2', state.id, command.id, state.revision,
            self._snapshot(state), [m.model_dump(mode='json') for m in state.memory],
            state.compacted_context, state.ledger_state,
            [e.model_dump(mode='json') for e in state.transcript_pending],
            {'executions': state.execution_pending, 'attempts': state.attempts_pending},
            state.diagnostics_pending, state.retention_pending, config_hash, event])
        # Every retry first takes the same transaction lock and checks the receipt,
        # resolving the old transaction before concluding that no receipt exists.
        for attempt in range(3):
            try:
                receipt = await self._save_once(state, config, event, command, fingerprint)
                state.revision = receipt['revision']
                state.needs_reload = receipt.get('needs_reload',False)
                self._acknowledge(state)
                self._measure('save',started,time.perf_counter()-started,
                              snapshot_json_bytes=len(_json(self._snapshot(state)).encode()))
                return receipt
            except (psycopg.OperationalError, psycopg.InterfaceError):
                if attempt == 2:
                    raise

    async def _save_once(self, state, config, event, command, fingerprint):
        async with await psycopg.AsyncConnection.connect(self.database_url) as connection:
            async with connection.cursor() as cursor:
                receipt = await self._committed_receipt(cursor,state,command,fingerprint)
                if receipt:
                    return receipt
                await self.commands.guard(cursor, command)
                await cursor.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,734910003))', (state.id,))
                receipt = await self._committed_receipt(cursor,state,command,fingerprint)
                if receipt:
                    return receipt
                await cursor.execute('SELECT state,config,config_ref,config_sha256,revision,storage_version FROM agent_sessions WHERE session_id=%s FOR UPDATE', (state.id,))
                existing = await cursor.fetchone()
                await self._guard_execution(cursor,state)
                if existing:
                    if existing[5] != 6:
                        raise TranscriptIntegrityError('Schema-6 session required')
                    await cursor.execute(HYDRATE,(False,RECENT,False,state.id))
                    values = await cursor.fetchone()
                    if values is None:
                        raise TranscriptIntegrityError('Persisted active context is missing')
                    self._decode_row(dict(zip((c.name for c in cursor.description),values)))
                    if existing[4] != state.revision:
                        raise RevisionConflict(state.id,state.revision,existing[4])
                    if (resolved_config_hash(AgentConfig.model_validate(existing[1])) != resolved_config_hash(config)
                            or existing[2] != state.config_ref or existing[3] != state.config_sha256 or existing[0]['goal'] != state.goal):
                        raise ValueError('Cannot replace immutable session configuration or goal')
                elif state.revision != 0:
                    raise RevisionConflict(state.id,state.revision,0)
                proposed = state.model_copy(deep=True)
                proposed.revision += 1
                if existing:
                    await cursor.execute('''UPDATE agent_sessions SET state=%s::jsonb,revision=%s,last_commit_id=%s,last_commit_sha256=%s,updated_at=now()
                        WHERE session_id=%s AND revision=%s RETURNING revision''',
                        (_json(self._snapshot(proposed)),proposed.revision,state.pending_commit_id,fingerprint,state.id,state.revision))
                    if await cursor.fetchone() is None:
                        raise RevisionConflict(state.id,state.revision,existing[4])
                else:
                    await cursor.execute('''INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256,resolved_config_sha256,
                        revision,last_commit_id,last_commit_sha256,storage_version) VALUES (%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,6)''',
                        (state.id,_json(self._snapshot(proposed)),_json(config.model_dump(mode='json')),state.config_ref,state.config_sha256,
                         state.resolved_config_sha256,proposed.revision,state.pending_commit_id,fingerprint))
                await cursor.execute('''UPDATE agent_commands SET session_id=%s,status='started',updated_at=clock_timestamp()
                    WHERE command_id=%s AND status IN ('pending','started') AND (session_id IS NULL OR session_id=%s) RETURNING command_id''', (state.id,command.id,state.id))
                if await cursor.fetchone() is None:
                    raise ValueError('Command is terminal or belongs to another session')
                await self._write_transcript(cursor,proposed)
                await self._write_calls(cursor,proposed)
                await self._write_diagnostics(cursor,proposed)
                await self._write_execution(cursor,proposed)
                event_id = None
                if event is not None:
                    await cursor.execute('''INSERT INTO agent_session_events(session_id,event,revision,commit_id,command_id)
                        VALUES (%s,%s::jsonb,%s,%s,%s) RETURNING event_id''',
                        (state.id,_json(event),proposed.revision,state.pending_commit_id,command.id))
                    event_id = (await cursor.fetchone())[0]
                await self._write_context(cursor,proposed)
                if proposed.retention_pending:
                    await self._write_retention(cursor,proposed)
                await cursor.execute('''INSERT INTO agent_commit_receipts(session_id,commit_id,command_id,fingerprint,expected_revision,committed_revision)
                    VALUES (%s,%s,%s,%s,%s,%s)''', (state.id,state.pending_commit_id,command.id,fingerprint,state.revision,proposed.revision))
                terminal = command.terminal or (proposed.run_command_id == command.id and proposed.run_status not in ('idle','running'))
                await self._guard_execution(cursor,state)
                await self.commands.guard(cursor,command)
                if terminal:
                    if proposed.run_status=='blocked' or proposed.run_status=='interrupted' and proposed.pending_tool_calls:
                        await self.commands.block(cursor,command,'blocked' if proposed.run_status=='blocked' else 'uncertain')
                    else:
                        message = command.response_message or proposed.final_answer or 'Run stopped.'
                        response_state = proposed.model_copy(deep=True)
                        self._acknowledge(response_state)
                        body = {'session': response_state.model_dump(mode='json'), 'message': message}
                        await self.commands.complete(cursor,command,command.response_status,body,
                            'succeeded' if proposed.status != 'failed' else 'failed')
            await connection.commit()
        return {'revision':proposed.revision,'event_id':event_id}

    async def _committed_receipt(self,cursor,state,command,fingerprint):
        await cursor.execute('SELECT command_id::text,fingerprint,committed_revision FROM agent_commit_receipts WHERE session_id=%s AND commit_id=%s',(state.id,state.pending_commit_id))
        receipt = await cursor.fetchone()
        if not receipt:
            return None
        if receipt[:2] != (command.id,fingerprint):
            raise TranscriptIntegrityError('Commit identity reused with changed content')
        await cursor.execute('SELECT revision FROM agent_sessions WHERE session_id=%s',(state.id,))
        latest = (await cursor.fetchone())[0]
        await cursor.execute('SELECT event_id FROM agent_session_events WHERE session_id=%s AND commit_id=%s',(state.id,state.pending_commit_id))
        event_row = await cursor.fetchone()
        return {'revision':receipt[2],'event_id':event_row[0] if event_row else None,'needs_reload':latest != receipt[2]}

    @staticmethod
    def _acknowledge(state):
        state.transcript_pending,state.execution_pending,state.attempts_pending,state.diagnostics_pending = [],[],[],[]
        state.pending_commit_id,state.retention_pending = None,None

    async def transcript(self, session_id, after_sequence=0, limit=100, through_sequence=None):
        if after_sequence < 0 or not 1 <= limit <= 1000:
            raise ValueError('Transcript cursor must be nonnegative; limit must be 1..1000')
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute('''SELECT sequence,message_id::text,kind,payload,run_id::text,decision_step,created_at,recorded_at
                    FROM agent_session_transcript WHERE session_id=%s AND sequence>%s
                    AND (%s::bigint IS NULL OR sequence<=%s) ORDER BY sequence LIMIT %s''', (session_id, after_sequence, through_sequence, through_sequence, limit))
                return await cursor.fetchall()

def _json(value: Any):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
