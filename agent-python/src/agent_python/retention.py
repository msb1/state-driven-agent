"""Keep-all by default; verified exports permit terminal-session telemetry pruning."""
import hashlib
import json
import os
from pathlib import Path
from uuid import UUID,uuid4
import psycopg
from .storage import encoded
from .storage_state import FAMILIES,RECENT

TABLES = {
    'agent_sessions':'session_id','agent_session_context':'session_id',
    'agent_session_transcript':'sequence','agent_session_events':'event_id',
    'agent_session_diagnostics':'family,ordinal','agent_runs':'started_at,run_id',
    'agent_tool_executions':'execution_id','agent_session_calls':'call_id,message_id',
    'agent_archive_receipts':'archive_id'}


def line(value):
    return (encoded(value)+'\n').encode()


def verify_archive(path):
    """Streaming canonical-content verification; rejects truncation and trailing data."""
    digest = hashlib.sha256()
    hashes = {name:hashlib.sha256() for name in TABLES}
    counts = {name:0 for name in TABLES}
    with Path(path).open('rb') as stream:
        header = json.loads(next(stream,b''))
        if header.get('format')!='sda-archive-1':
            raise ValueError('Unsupported archive format')
        UUID(header['archive_id']); UUID(header['session_id'])
        if set(header)!={'format','archive_id','session_id','snapshot_revision'} or type(header['snapshot_revision']) is not int or header['snapshot_revision']<1:
            raise ValueError('Invalid archive header')
        digest.update(line(header))
        watermarks={'events':0,'diagnostics':{}}
        manifest = None
        for raw in stream:
            value = json.loads(raw)
            if manifest is not None:
                raise ValueError('Trailing archive data')
            if 'manifest' in value:
                manifest = value['manifest']
                continue
            table = value.get('table')
            if table not in TABLES or value.get('row',{}).get('session_id')!=header['session_id']:
                raise ValueError('Unknown table or cross-session archive row')
            if table=='agent_session_events':
                watermarks['events']=max(watermarks['events'],value['row']['event_id'])
            elif table=='agent_session_diagnostics':
                family=value['row']['family']
                if family not in FAMILIES:
                    raise ValueError('Unknown archived diagnostic family')
                watermarks['diagnostics'][family]=max(watermarks['diagnostics'].get(family,0),value['row']['ordinal'])
            data = line(value)
            digest.update(data); hashes[table].update(data); counts[table]+=1
        if (manifest is None or set(manifest)!={'sha256','counts','table_sha256','watermarks'}
                or manifest['watermarks']!=watermarks or manifest['sha256']!=digest.hexdigest() or manifest['counts']!=counts
                or manifest['table_sha256']!={k:v.hexdigest() for k,v in hashes.items()}):
            raise ValueError('Archive checksum/count mismatch or truncated file')
    return {**header,**manifest,'archive_path':str(Path(path).resolve())}


class RetentionStorage:
    async def export_archive(self,session_id,path):
        # Exclusive creation and restrictive permissions protect audit payloads.
        fd = os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        try:
            with os.fdopen(fd,'wb') as stream:
                async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
                    await conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                    await conn.execute("SET LOCAL TIME ZONE 'UTC'")
                    cursor = await conn.execute('SELECT revision FROM agent_sessions WHERE session_id=%s',(session_id,))
                    row = await cursor.fetchone()
                    if row is None:
                        raise ValueError('Unknown session')
                    header = {'format':'sda-archive-1','archive_id':str(uuid4()),'session_id':session_id,'snapshot_revision':row[0]}
                    digest = hashlib.sha256(); hashes={k:hashlib.sha256() for k in TABLES};counts={k:0 for k in TABLES}
                    watermarks={'events':0,'diagnostics':{}}
                    data=line(header);stream.write(data);digest.update(data)
                    for table,order in TABLES.items():
                        async with conn.cursor(name='archive_rows') as rows:
                            await rows.execute(f'SELECT to_jsonb(t) FROM {table} t WHERE session_id=%s ORDER BY {order}',(session_id,))
                            async for (record,) in rows:
                                value={'table':table,'row':record};data=line(value)
                                stream.write(data);digest.update(data);hashes[table].update(data);counts[table]+=1
                                if table=='agent_session_events':
                                    watermarks['events']=record['event_id']
                                elif table=='agent_session_diagnostics':
                                    watermarks['diagnostics'][record['family']]=record['ordinal']
                    manifest={'sha256':digest.hexdigest(),'table_sha256':{k:v.hexdigest() for k,v in hashes.items()},
                        'counts':counts,'watermarks':watermarks}
                    stream.write(line({'manifest':manifest}));stream.flush();os.fsync(stream.fileno())
            return verify_archive(path)
        except BaseException:
            # A partial file has no valid final manifest and cannot authorize pruning.
            raise

    async def retention_plan(self,session_id,keep_diagnostics=RECENT,keep_events=100):
        if keep_diagnostics<RECENT or keep_events<1:
            raise ValueError('Retain at least 32 diagnostics per family and one event')
        persisted = await self.get(session_id)
        if persisted is None:
            raise ValueError('Unknown session')
        state=persisted.state
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor=await conn.execute("SELECT count(*) FROM agent_tool_executions WHERE session_id=%s AND status IN ('pending','started','uncertain')",(session_id,))
            unresolved=(await cursor.fetchone())[0]
            cursor=await conn.execute("SELECT count(*) FROM agent_runs WHERE session_id=%s AND status='running'",(session_id,))
            running=(await cursor.fetchone())[0]
        return {'session_id':session_id,'revision':state.revision,'default_policy':'keep_all',
            'eligible':state.status!='active' and state.run_status!='running' and not state.pending_tool_calls and unresolved==0 and running==0,
            'unresolved_executions':unresolved,'running_lifecycles':running,'keep_diagnostics':keep_diagnostics,'keep_events':keep_events,
            'protected_tables':['agent_sessions','agent_session_context','agent_session_transcript',
                'agent_session_calls','agent_runs','agent_tool_executions','agent_archive_receipts'],
            'prunable_tables':['agent_session_events','agent_session_diagnostics'],
            'requires':'Verified fresh export, terminal session, ownership/fencing, matching revision and database digests'}

    async def prune_telemetry(self,session_id,path,keep_diagnostics=RECENT,keep_events=100):
        from .execution import ExecutionCoordinator
        manifest=verify_archive(path)
        if manifest['session_id']!=session_id:
            raise ValueError('Archive belongs to another session')
        plan=await self.retention_plan(session_id,keep_diagnostics,keep_events)
        if not plan['eligible']:
            raise ValueError('Active/pending/uncertain execution prevents retention')
        # Prior exports are dependencies too. Keep their receipts permanently
        # and refuse further cleanup if an archive chain member is unavailable.
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            cursor=await conn.execute('SELECT sha256,manifest FROM agent_archive_receipts WHERE session_id=%s',(session_id,))
            previous=await cursor.fetchall()
        for checksum,receipt in previous:
            if verify_archive(receipt['archive_path'])['sha256']!=checksum:
                raise ValueError('A prior archive dependency is unavailable or corrupt')
        persisted=await self.get(session_id)
        async with ExecutionCoordinator(self,persisted.state,persisted.config,lease_seconds=300) as execution:
            persisted=await self.get(session_id)
            state=persisted.state
            state.execution_owner,state.execution_fence=execution.owner,execution.fence
            state.execution_maintenance=True
            execution.state=state
            if state.revision!=manifest['snapshot_revision']:
                raise ValueError('Stale archive; export the current revision again')
            state.last_archive_id=manifest['archive_id']
            state.retention_pending={**manifest,'keep_diagnostics':keep_diagnostics,'keep_events':keep_events}
            await self.save(state,persisted.config,{'type':'telemetry_archived','archive_id':manifest['archive_id']})
        return {'session_id':session_id,'revision':state.revision,'archive_id':manifest['archive_id']}

    async def _write_retention(self,cursor,state):
        item=state.retention_pending
        if (state.execution_owner is None or state.status=='active' or state.pending_tool_calls
                or item['snapshot_revision']!=state.revision-1 or item['keep_diagnostics']<RECENT or item['keep_events']<1):
            raise ValueError('Retention requires a terminal owned session and matching exported revision')
        await cursor.execute("SELECT count(*) FROM agent_tool_executions WHERE session_id=%s AND status IN ('pending','started','uncertain')",(state.id,))
        if (await cursor.fetchone())[0]:
            raise ValueError('Unresolved execution prevents pruning')
        await cursor.execute("SELECT count(*) FROM agent_runs WHERE session_id=%s AND status='running'",(state.id,))
        if (await cursor.fetchone())[0]:
            raise ValueError('A running lifecycle prevents pruning')
        await cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        for table,condition,params in [
            ('agent_session_events','event_id<=%s',[item['watermarks']['events']]),
            ('agent_session_diagnostics',"ordinal<=COALESCE((%s::jsonb->>family)::bigint,0)",[encoded(item['watermarks']['diagnostics'])])]:
            digest=hashlib.sha256()
            async with cursor.connection.cursor(name='verify_archive_rows') as rows:
                await rows.execute(f'SELECT to_jsonb(t) FROM {table} t WHERE session_id=%s AND {condition} ORDER BY {TABLES[table]}',(state.id,*params))
                async for (record,) in rows:
                    digest.update(line({'table':table,'row':record}))
            if digest.hexdigest()!=item['table_sha256'][table]:
                raise ValueError('Export does not match current database telemetry; nothing pruned')
        await cursor.execute('''INSERT INTO agent_archive_receipts(archive_id,session_id,snapshot_revision,sha256,manifest)
            VALUES (%s,%s,%s,%s,%s::jsonb)''',(item['archive_id'],state.id,item['snapshot_revision'],item['sha256'],encoded(item)))
        for family in FAMILIES:
            cutoff=min(item['watermarks']['diagnostics'].get(family,0),state.diagnostic_counts.get(family,0)-item['keep_diagnostics'])
            await cursor.execute('DELETE FROM agent_session_diagnostics WHERE session_id=%s AND family=%s AND ordinal<=%s',(state.id,family,cutoff))
        await cursor.execute('''DELETE FROM agent_session_events WHERE session_id=%s AND event_id<=%s
            AND event_id NOT IN(SELECT event_id FROM agent_session_events WHERE session_id=%s ORDER BY event_id DESC LIMIT %s)''',
            (state.id,item['watermarks']['events'],state.id,item['keep_events']))
