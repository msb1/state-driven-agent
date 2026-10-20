"""Atomic PostgreSQL snapshots, transcript, active-context references, and revisions."""
from __future__ import annotations
import hashlib
import json
import os
import time
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4
import psycopg
from psycopg.rows import dict_row
from .config import AgentConfig, resolved_config_hash
from .models import Message, SessionState
from .transcript import stage_messages, archive_record
from .execution_store import ExecutionStore, DDL
from .storage import SelectiveStorage, DDL as STORAGE_DDL
from .storage_state import hot_snapshot, stage_diagnostics
from .retention import RetentionStorage

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

    @asynccontextmanager
    async def _schema_guard(self):
        # Session-scoped lock spans the DDL commit and recovery's separate
        # connections. All initializers use the same key, including CLI setup.
        async with await psycopg.AsyncConnection.connect(self.database_url,autocommit=True) as connection:
            await connection.execute('SELECT pg_advisory_lock(734910003)')
            try:
                yield connection
            finally:
                await connection.execute('SELECT pg_advisory_unlock(734910003)')

    async def _schema_current(self,connection):
        tables=('agent_sessions','agent_session_events','agent_session_transcript',
            'agent_session_context','agent_schema_migrations','agent_runs',
            'agent_tool_executions','agent_session_diagnostics','agent_session_calls',
            'agent_archive_receipts')
        cursor=await connection.execute('SELECT bool_and(to_regclass(name) IS NOT NULL) FROM unnest(%s::text[]) AS items(name)',(list(tables),))
        if not (await cursor.fetchone())[0]:
            return False
        cursor=await connection.execute("SELECT EXISTS(SELECT 1 FROM agent_schema_migrations WHERE component='agent-python' AND version=5)")
        if not (await cursor.fetchone())[0]:
            return False
        # A synthetic/legacy snapshot may still need importing even when a
        # prior startup has recorded the schema marker. Do not skip that work.
        cursor=await connection.execute('''SELECT NOT EXISTS(SELECT 1 FROM agent_sessions s
            LEFT JOIN agent_session_context c USING(session_id)
            WHERE s.storage_version<5 OR c.session_id IS NULL)''')
        return (await cursor.fetchone())[0]

    async def initialize(self):
        async with self._schema_guard() as connection:
            await self._initialize(connection)

    async def startup(self,limit=100,*,recover=True):
        from .execution import reconcile_abandoned
        async with self._schema_guard() as connection:
            await self._initialize(connection)
            # Keep the guard after DDL commits: another worker/CLI initializer
            # must not take exclusive relation locks during recovery reads.
            return await reconcile_abandoned(self,limit) if recover else await self.abandoned(limit)

    async def _initialize(self,connection):
        if await self._schema_current(connection):
            return
        async with connection.transaction():
            async with connection.cursor() as cursor:
                statements = [
                    '''CREATE TABLE IF NOT EXISTS agent_sessions (
                        session_id UUID PRIMARY KEY, state JSONB NOT NULL, config JSONB NOT NULL,
                        config_ref TEXT, config_sha256 TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())''',
                    'ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0',
                    'ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS last_commit_id UUID',
                    'ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS last_commit_sha256 TEXT',
                    '''CREATE TABLE IF NOT EXISTS agent_session_events (
                        event_id BIGSERIAL PRIMARY KEY, session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
                        event JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())''',
                    'ALTER TABLE agent_session_events ADD COLUMN IF NOT EXISTS revision BIGINT',
                    'ALTER TABLE agent_session_events ADD COLUMN IF NOT EXISTS commit_id UUID',
                    'CREATE INDEX IF NOT EXISTS agent_session_events_session_id_idx ON agent_session_events(session_id,event_id)',
                    'CREATE UNIQUE INDEX IF NOT EXISTS agent_session_events_commit_idx ON agent_session_events(session_id,commit_id) WHERE commit_id IS NOT NULL',
                    '''CREATE TABLE IF NOT EXISTS agent_session_transcript (
                        session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
                        sequence BIGINT NOT NULL CHECK(sequence>0), message_id UUID NOT NULL, kind TEXT NOT NULL,
                        payload JSONB NOT NULL, run_id UUID, decision_step BIGINT NOT NULL CHECK(decision_step>=0),
                        created_at TIMESTAMPTZ NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY(session_id,sequence), UNIQUE(session_id,message_id))''',
                    '''CREATE TABLE IF NOT EXISTS agent_session_context (
                        session_id UUID PRIMARY KEY REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
                        revision BIGINT NOT NULL, goal_message_id UUID NOT NULL, ledger_record_id UUID,
                        raw_message_ids JSONB NOT NULL CHECK(jsonb_typeof(raw_message_ids)='array'),
                        FOREIGN KEY(session_id,goal_message_id) REFERENCES agent_session_transcript(session_id,message_id),
                        FOREIGN KEY(session_id,ledger_record_id) REFERENCES agent_session_transcript(session_id,message_id))''',
                    '''CREATE TABLE IF NOT EXISTS agent_schema_migrations (
                        component TEXT NOT NULL, version INTEGER NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(component,version))''',
                ]
                for statement in statements:
                    await cursor.execute(statement)
                for statement in [*DDL,*STORAGE_DDL]:
                    await cursor.execute(statement)
                await cursor.execute('''SELECT session_id::text,state,revision FROM agent_sessions s
                    WHERE NOT EXISTS(SELECT 1 FROM agent_session_context c WHERE c.session_id=s.session_id)
                    ORDER BY session_id FOR UPDATE''')
                for sid, data, revision in await cursor.fetchall():
                    state = SessionState.model_validate(data)
                    if state.id != sid:
                        raise TranscriptIntegrityError('Legacy snapshot identity differs from its session row')
                    state.revision, state.history_complete = revision + 1, False
                    state.next_transcript_sequence, state.transcript_pending, state.pending_commit_id = 1, [], None
                    archive_record(state, 'history_gap', {
                        'reason': 'Complete pre-migration history unavailable; only retained snapshot messages imported.',
                        'legacy_step_count': state.step_count, 'legacy_compaction_count': len(state.compaction_events)})
                    if not state.memory:
                        state.memory = [Message(role='user', content=state.goal)]
                    for message in state.memory:
                        message.sequence = None
                    stage_messages(state)
                    state.active_ledger_id = None
                    for index, text in enumerate(state.compaction_events):
                        entry = archive_record(state, 'legacy_summary', {'content': text,
                            'verification': 'unverified_model_summary', 'source_history_unavailable': True,
                            'legacy_summary_index': index})
                        if text == state.compacted_context:
                            state.active_ledger_id = entry.message_id
                    if state.compacted_context is not None and state.active_ledger_id is None:
                        entry = archive_record(state, 'legacy_summary', {'content': state.compacted_context,
                            'verification': 'unverified_model_summary', 'source_history_unavailable': True})
                        state.active_ledger_id = entry.message_id
                    await self._write_transcript(cursor, state)
                    stage_diagnostics(state)
                    await self._write_diagnostics(cursor,state)
                    await self._write_context(cursor, state)
                    await cursor.execute('UPDATE agent_sessions SET state=%s::jsonb,revision=%s,storage_version=5,updated_at=now() WHERE session_id=%s',
                        (_json(self._snapshot(state)), state.revision, sid))
                    await cursor.execute('INSERT INTO agent_session_events(session_id,event,revision) VALUES (%s,%s::jsonb,%s)',
                        (sid, _json({'type': 'history_migrated', 'history_complete': False}), state.revision))
                await cursor.execute("INSERT INTO agent_schema_migrations(component,version) VALUES ('agent-python',3) ON CONFLICT DO NOTHING")
                await self._migrate_storage(cursor)

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
        await cursor.execute('SELECT message_id::text,sequence,kind,payload FROM agent_session_transcript WHERE session_id=%s AND message_id=ANY(%s::uuid[])', (state.id, ids))
        rows = {row[0]: row for row in await cursor.fetchall()}
        for message in state.memory:
            row = rows.get(message.message_id)
            if row is None or row[1] != message.sequence or row[2] != 'message' or _json(row[3]) != _json(message.wire_payload()):
                raise TranscriptIntegrityError('Active message missing or immutable payload changed')
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
        stage_diagnostics(state)
        stage_messages(state)
        config_hash = resolved_config_hash(config)
        if state.resolved_config_sha256 is not None and state.resolved_config_sha256 != config_hash:
            raise ValueError('Resolved configuration hash does not match session anchor')
        if state.pending_commit_id is None:
            state.pending_commit_id = str(uuid4())
        commit_id, expected = state.pending_commit_id, state.revision
        fingerprint = hashlib.sha256(_json([self._snapshot(state),
            [m.model_dump(mode='json') for m in state.memory],state.compacted_context,state.ledger_state,
            [entry.model_dump(mode='json') for entry in state.transcript_pending], state.execution_pending, state.diagnostics_pending, state.retention_pending, config_hash, event]).encode()).hexdigest()
        committed = expected + 1
        bytes_written = 0
        db_start = time.perf_counter()
        async with await psycopg.AsyncConnection.connect(self.database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute('SELECT state,config,config_ref,config_sha256,revision,last_commit_id::text,last_commit_sha256,storage_version FROM agent_sessions WHERE session_id=%s FOR UPDATE', (state.id,))
                existing = await cursor.fetchone()
                await self._guard_execution(cursor, state)
                if existing is not None and existing[7]!=5:
                    raise TranscriptIntegrityError('Storage migration required before writing a legacy session')
                if existing is not None and existing[5] == commit_id and existing[6] == fingerprint:
                    committed = existing[4]  # Identical retry after uncertain commit acknowledgment.
                else:
                    if existing is not None:
                        old_state, old_config, old_ref, old_sha, revision, _, _, _ = existing
                        if revision != expected:
                            raise RevisionConflict(state.id, expected, revision)
                        if (resolved_config_hash(AgentConfig.model_validate(old_config)) != config_hash
                                or old_ref != state.config_ref or old_sha != state.config_sha256 or old_state.get('goal') != state.goal):
                            raise ValueError('Cannot replace immutable session configuration or goal')
                    elif expected != 0:
                        raise RevisionConflict(state.id, expected, 0)
                    proposed = state.model_copy(deep=True)
                    proposed.revision = committed
                    bytes_written = len(_json(self._snapshot(proposed)).encode())
                    if existing is None:
                        await cursor.execute('''INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256,revision,last_commit_id,last_commit_sha256,storage_version)
                            VALUES (%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,5) ON CONFLICT(session_id) DO NOTHING RETURNING revision''',
                            (state.id, _json(self._snapshot(proposed)), _json(config.model_dump(mode='json')), state.config_ref, state.config_sha256, committed, commit_id, fingerprint))
                        if await cursor.fetchone() is None:
                            await cursor.execute('SELECT revision FROM agent_sessions WHERE session_id=%s', (state.id,))
                            raise RevisionConflict(state.id, expected, (await cursor.fetchone())[0])
                    else:
                        await cursor.execute('''UPDATE agent_sessions SET state=%s::jsonb,revision=%s,last_commit_id=%s,last_commit_sha256=%s,updated_at=now()
                            WHERE session_id=%s AND revision=%s RETURNING revision''',
                            (_json(self._snapshot(proposed)), committed, commit_id, fingerprint, state.id, expected))
                        if await cursor.fetchone() is None:
                            raise RevisionConflict(state.id, expected, existing[4])
                    await self._write_transcript(cursor, proposed)
                    await self._write_calls(cursor,proposed)
                    await self._write_diagnostics(cursor,proposed)
                    await self._write_execution(cursor, proposed)
                    if event is not None:
                        await cursor.execute('INSERT INTO agent_session_events(session_id,event,revision,commit_id) VALUES (%s,%s::jsonb,%s,%s)',
                            (state.id, _json(event), committed, commit_id))
                    await self._write_context(cursor, proposed)
                    if proposed.retention_pending:
                        await self._write_retention(cursor,proposed)
                    await self._guard_execution(cursor, state)
            await connection.commit()
        self._measure('save',started,time.perf_counter()-db_start,snapshot_json_bytes=bytes_written,
            diagnostic_append_json_bytes=sum(len(_json(d).encode()) for d in state.diagnostics_pending),
            transcript_append_json_bytes=sum(len(_json(e.model_dump(mode="json")).encode()) for e in state.transcript_pending))
        state.diagnostics_pending,state.retention_pending = [],None
        state.revision, state.transcript_pending, state.pending_commit_id = committed, [], None
        state.execution_pending = []

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
