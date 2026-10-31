"""Schema-6 storage, consistent selective hydration, telemetry and audit pages."""
import hashlib
import json
import time
import psycopg
from psycopg.rows import dict_row
from .models import SessionState
from .storage_state import FAMILIES, RECENT, hot_snapshot, stage_diagnostics
from .state_integrity import validate_snapshot, validate_context

# One SQL statement gives state/context/config/messages/ledger/diagnostics the
# same MVCC snapshot. UUID and sequence indexes already support active lookup.
HYDRATE = '''SELECT s.session_id::text AS session_id,s.state,s.config,s.config_ref,s.config_sha256,s.revision,s.storage_version,
    c.goal_message_id::text,c.ledger_record_id::text,c.raw_message_ids,c.revision AS context_revision,
    c.ledger_state,l.kind AS ledger_kind,l.payload AS ledger_payload,
    (SELECT COALESCE(max(sequence),0)+1 FROM agent_session_transcript
        WHERE session_id=s.session_id) AS archived_next_sequence,
    (SELECT jsonb_agg(m.payload || jsonb_build_object('message_id',m.message_id::text,
        'sequence',m.sequence,'created_at',m.created_at) ORDER BY ids.ord)
     FROM jsonb_array_elements_text(jsonb_build_array(c.goal_message_id::text)||c.raw_message_ids)
         WITH ORDINALITY ids(id,ord)
     LEFT JOIN agent_session_transcript m ON m.session_id=s.session_id AND m.message_id=ids.id::uuid AND m.kind='message') AS messages,
    (SELECT COALESCE(jsonb_agg(jsonb_build_object('family',d.family,'ordinal',d.ordinal,'payload',d.payload)
        ORDER BY d.family,d.ordinal),'[]'::jsonb)
     FROM (VALUES ('compaction_events'),('compaction_attempts'),('compaction_decisions')) f(family)
     CROSS JOIN LATERAL (SELECT family,ordinal,payload FROM agent_session_diagnostics
         WHERE session_id=s.session_id AND family=f.family
         AND (%s OR ordinal>COALESCE((s.state->'diagnostic_counts'->>f.family)::bigint,0)-%s)
         ORDER BY ordinal DESC LIMIT CASE WHEN %s THEN NULL ELSE 32 END) d) AS diagnostics
    FROM agent_sessions s JOIN agent_session_context c USING(session_id)
    LEFT JOIN agent_session_transcript l ON l.session_id=s.session_id AND l.message_id=c.ledger_record_id
    WHERE s.session_id=%s'''


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


class SelectiveStorage:
    def _measure(self, operation, started, database_seconds, **values):
        if self.measure_io:
            self.io_samples.append({'operation':operation,'wall_seconds':time.perf_counter()-started,
                'database_roundtrip_seconds':database_seconds,**values})
            self.io_samples = self.io_samples[-256:]

    async def _write_diagnostics(self,cursor,state):
        from .persistence import TranscriptIntegrityError
        for item in state.diagnostics_pending:
            if item['family'] not in FAMILIES:
                raise TranscriptIntegrityError('Unknown diagnostic family')
            await cursor.execute('''INSERT INTO agent_session_diagnostics(session_id,family,ordinal,payload)
                VALUES (%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING RETURNING ordinal''',
                (state.id,item['family'],item['ordinal'],encoded(item['payload'])))
            if await cursor.fetchone() is None:
                await cursor.execute('SELECT payload FROM agent_session_diagnostics WHERE session_id=%s AND family=%s AND ordinal=%s',
                    (state.id,item['family'],item['ordinal']))
                row = await cursor.fetchone()
                if row is None or encoded(row[0])!=encoded(item['payload']):
                    raise TranscriptIntegrityError('Immutable diagnostic identity collision')
        for family,count in state.diagnostic_counts.items():
            await cursor.execute('SELECT COALESCE(max(ordinal),0) FROM agent_session_diagnostics WHERE session_id=%s AND family=%s',(state.id,family))
            if (await cursor.fetchone())[0]!=count:
                raise TranscriptIntegrityError('Diagnostic counter differs from archived history')

    async def _write_calls(self,cursor,state):
        for entry in state.transcript_pending:
            if entry.kind=='message':
                for call in entry.payload.get('tool_calls') or []:
                    await cursor.execute('''INSERT INTO agent_session_calls(session_id,call_id,message_id)
                        VALUES (%s,%s,%s) ON CONFLICT DO NOTHING''',(state.id,call['id'],entry.message_id))

    async def get(self,session_id,*,include_history=False):
        started = time.perf_counter()
        db_start = time.perf_counter()
        async with await psycopg.AsyncConnection.connect(self.database_url,row_factory=dict_row) as connection:
            cursor = await connection.execute(HYDRATE,(include_history,RECENT,include_history,session_id))
            row = await cursor.fetchone()
        db_seconds = time.perf_counter()-db_start
        if row is None:
            return None
        persisted = self._decode_row(row)
        state = persisted.state
        self._measure('get',started,db_seconds,snapshot_json_bytes=len(encoded(row['state']).encode()),
            selected_json_bytes=len(encoded(row).encode()),active_messages_loaded=len(state.memory),
            diagnostic_rows_loaded=len(row['diagnostics']),include_history=include_history)
        return persisted

    @staticmethod
    def _decode_row(row, *, verify_integrity=True):
        from .persistence import PersistedSession, TranscriptIntegrityError
        from .config import AgentConfig, resolved_config_hash
        if row['storage_version']!=6:
            raise TranscriptIntegrityError('Unsupported stored session format; automatic migration is disabled')
        data = dict(row['state'])
        data['memory'] = row['messages']
        data['compacted_context'] = (row['ledger_payload'] or {}).get('content')
        data['ledger_state'] = row['ledger_state']
        for family in FAMILIES:
            data[family] = [d['payload'] for d in row['diagnostics'] if d['family']==family]
        data['diagnostics_complete'] = all(len(data[k])==data.get('diagnostic_counts',{}).get(k,0) for k in FAMILIES)
        try:
            if verify_integrity:
                validate_snapshot(row['state'])
            state = SessionState.model_validate(data)
            config = AgentConfig.model_validate(row['config'])
            if verify_integrity:
                validate_context(row['state'], state)
        except ValueError as error:
            raise TranscriptIntegrityError('Invalid active context, integrity anchor, or stored configuration') from error
        mismatches = []
        if state.id != row['session_id']:
            mismatches.append('session_id')
        if state.revision != row['revision']:
            mismatches.append('session_revision')
        if state.revision != row['context_revision']:
            mismatches.append('context_revision')
        if state.config_ref != row['config_ref']:
            mismatches.append('config_ref')
        if state.config_sha256 != row['config_sha256']:
            mismatches.append('config_sha256')
        if (state.resolved_config_sha256
                and state.resolved_config_sha256 != resolved_config_hash(config)):
            mismatches.append('resolved_config_sha256')
        if state.next_transcript_sequence != row['archived_next_sequence']:
            mismatches.append('next_transcript_sequence')
        if not state.memory or state.memory[0].role != 'user' or state.memory[0].content != state.goal:
            mismatches.append('goal_message')
        if [m.message_id for m in state.memory] != [row['goal_message_id'], *row['raw_message_ids']]:
            mismatches.append('active_message_ids')
        if len({m.message_id for m in state.memory}) != len(state.memory):
            mismatches.append('duplicate_active_message_ids')
        if [m.sequence for m in state.memory] != sorted(m.sequence for m in state.memory):
            mismatches.append('active_message_order')
        if state.active_ledger_id != row['ledger_record_id']:
            mismatches.append('active_ledger_id')
        if state.active_ledger_id and row['ledger_kind'] not in {'compaction_summary', 'legacy_summary'}:
            mismatches.append('ledger_kind')
        if (row['ledger_kind'] == 'compaction_summary'
                and encoded(state.ledger_state) != encoded(row['ledger_payload'].get('ledger'))):
            mismatches.append('ledger_state')
        if mismatches:
            raise TranscriptIntegrityError(
                'Snapshot/context/config/ledger references disagree '
                f"(session {row['session_id']}; fields: {', '.join(mismatches)})"
            )
        return PersistedSession(state,config)

    async def diagnostic_page(self,session_id,family,after=0,limit=100,through=None):
        if family not in FAMILIES or after<0 or not 1<=limit<=1000:
            raise ValueError('Invalid diagnostic family/cursor/limit')
        async with await psycopg.AsyncConnection.connect(self.database_url,row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT ordinal,payload FROM agent_session_diagnostics
                WHERE session_id=%s AND family=%s AND ordinal>%s AND (%s::bigint IS NULL OR ordinal<=%s)
                ORDER BY ordinal LIMIT %s''',(session_id,family,after,through,through,limit))
            return await cursor.fetchall()

    async def event_page(self,session_id,after=0,limit=100,through=None):
        if after<0 or not 1<=limit<=1000:
            raise ValueError('Invalid event cursor/limit')
        async with await psycopg.AsyncConnection.connect(self.database_url,row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT event_id,event,revision,created_at FROM agent_session_events
                WHERE session_id=%s AND event_id>%s AND (%s::bigint IS NULL OR event_id<=%s)
                ORDER BY event_id LIMIT %s''',(session_id,after,through,through,limit))
            return await cursor.fetchall()

    async def event_watermark(self,session_id):
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor = await conn.execute('SELECT COALESCE(max(event_id),0) FROM agent_session_events WHERE session_id=%s',(session_id,))
            return (await cursor.fetchone())[0]

    async def archive_page(self,session_id,after=None,limit=100):
        if not 1<=limit<=1000:
            raise ValueError('Invalid archive page limit')
        async with await psycopg.AsyncConnection.connect(self.database_url,row_factory=dict_row) as conn:
            cursor=await conn.execute('''SELECT archive_id::text,snapshot_revision,sha256,manifest,created_at
                FROM agent_archive_receipts WHERE session_id=%s AND (%s::uuid IS NULL OR archive_id>%s::uuid)
                ORDER BY archive_id LIMIT %s''',(session_id,after,after,limit))
            return await cursor.fetchall()

    async def run_page(self,session_id,after=None,limit=100):
        if not 1<=limit<=1000:
            raise ValueError('Invalid run page limit')
        async with await psycopg.AsyncConnection.connect(self.database_url,row_factory=dict_row) as conn:
            cursor = await conn.execute('''SELECT run_id::text,fence,status,started_at,ended_at,termination_reason,
                last_decision_step,cancel_requested,cancel_requested_at FROM agent_runs r WHERE session_id=%s
                AND (%s::uuid IS NULL OR (started_at,run_id)>(SELECT started_at,run_id FROM agent_runs WHERE session_id=%s AND run_id=%s))
                ORDER BY started_at,run_id LIMIT %s''',(session_id,after,session_id,after,limit))
            return await cursor.fetchall()
