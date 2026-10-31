"""FastAPI surface, PostgreSQL session persistence, and SSE lifecycle output."""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, JSONResponse
import httpx
import anyio
import psycopg

from .config import ConfigRepository, resolved_config_hash
from .engine import AgentEngine
from .models import CancelSessionRequest, AppendUserMessageRequest, CreateSessionRequest, Message, ResumeSessionRequest, RunRequest, SessionResponse, SessionState
from .persistence import SessionRepository, RevisionConflict
from .transcript import append_message
from .tools import TOOLS
from .commands import current_command, CommandError, envelope
from .command_api import command_dependency, ReplayCommand, replay_response
from .identity import CONTRACT, SCHEMA_VERSION
from .execution import ExecutionCoordinator, OwnershipConflict, LeaseLost, RunCancelled, RecoveryBlocked, reconcile_abandoned

config_repository = ConfigRepository()
session_repository = SessionRepository()


@dataclass
class SessionRecord:
    state: SessionState
    engine: AgentEngine


# PostgreSQL is authoritative; these are only a hot cache and process-local locks.
sessions: dict[str, SessionRecord] = {}
locks: dict[str, asyncio.Lock] = {}


@asynccontextmanager
async def lifespan(_: FastAPI):
    session_repository.commands.namespace
    await session_repository.startup()
    yield


app = FastAPI(title="State-Driven AI Agent", version=CONTRACT, lifespan=lifespan, dependencies=[Depends(command_dependency)])
app.state.repository = lambda: session_repository


@app.middleware('http')
async def command_headers(request, call_next):
    response = await call_next(request)
    if hasattr(request.state,'command_id'):
        response.headers['Idempotency-Command-Id'] = request.state.command_id
    return response


async def _prepare_command(path, body):
    if path == '/sessions':
        try:
            snapshot = config_repository.resolve(body['config_ref'])
            unknown = {tool.name for tool in snapshot.config.tools} - set(TOOLS)
            if unknown:
                raise ValueError('Unavailable compiled tools: ' + ', '.join(sorted(unknown)))
        except (OSError,ValueError) as error:
            raise HTTPException(422,'Invalid config_ref: ' + str(error)) from error
        engine = AgentEngine(snapshot.config)
        state = engine.create_session(body['user_prompt'],snapshot.ref,snapshot.sha256)
        return {'state':state.model_dump(mode='json'),'config':snapshot.config.model_dump(mode='json')}
    session_id = path.split('/')[2]
    record = await _record(session_id)
    return {'session_id':session_id,'config':record.engine.config.model_dump(mode='json')}


app.state.prepare_command = _prepare_command


@app.exception_handler(CommandError)
async def command_error_handler(_,error):
    return JSONResponse(status_code=error.status,content={'code':error.code,'message':error.message})


@app.exception_handler(ReplayCommand)
async def command_replay_handler(_,error):
    return replay_response(error.row)


@app.get('/commands/{command_id}')
async def command_status(command_id: str):
    try:
        command_id = str(UUID(command_id))
    except ValueError as error:
        raise HTTPException(422,'command_id must be a UUID') from error
    row = await session_repository.commands.get(command_id=command_id)
    if row is None:
        raise HTTPException(404,'Command not found')
    return envelope(row)


async def _record(session_id: str) -> SessionRecord:
    # Validate once for every session route before touching PostgreSQL.
    try:
        UUID(session_id)
    except (ValueError,TypeError,AttributeError) as error:
        raise HTTPException(422,'session_id must be a UUID') from error
    # Reload authoritative revision/state on every API access, including reads.
    persisted = await session_repository.get(session_id)
    if persisted is None:
        raise HTTPException(404, "Session not found")
    cached = sessions.get(session_id)
    engine = cached.engine if cached and resolved_config_hash(cached.engine.config) == resolved_config_hash(persisted.config) else AgentEngine(persisted.config)
    record = SessionRecord(state=persisted.state, engine=engine)
    sessions[session_id] = record
    locks.setdefault(session_id, asyncio.Lock())
    return record


async def _save(record: SessionRecord, event: dict[str, object] | None = None) -> None:
    try:
        await session_repository.save(record.state, record.engine.config, event)
    except RevisionConflict:
        sessions.pop(record.state.id, None)
        raise


def _conflict_event(error: RevisionConflict) -> dict:
    return {'type': 'error', 'code': 'revision_conflict', 'message': str(error),
            'expected_revision': error.expected, 'current_revision': error.actual, 'recoverable': True}


@app.exception_handler(RevisionConflict)
async def revision_conflict_handler(_, error: RevisionConflict):
    return JSONResponse(status_code=409, content=_conflict_event(error))


@app.get("/health")
async def health() -> dict[str, str | int]:
    return {"status": "ok", "contract_version": CONTRACT, "storage_schema": SCHEMA_VERSION, "config_root": str(config_repository.root), "available_configs": len(config_repository.list())}


@app.get("/configs")
async def list_configs() -> list[dict[str, object]]:
    return [{"config_ref": s.ref, "sha256": s.sha256, "agent_name": s.config.name,
             "tools": [tool.name for tool in s.config.tools],
             "workflow_phases": [p.id for p in s.config.workflow.phases] if s.config.workflow else [],
             "verbose_setup": s.config.output.verbose_setup}
            for s in config_repository.list() if all(tool.name in TOOLS for tool in s.config.tools)]


@app.post("/sessions", response_model=SessionResponse, status_code=201)
async def create_session(request: CreateSessionRequest) -> SessionResponse:
    from .config import AgentConfig
    command = current_command.get()
    admitted = command.row['request']['admission']
    engine = AgentEngine(AgentConfig.model_validate(admitted['config']))
    state = SessionState.model_validate(admitted['state'])
    record = SessionRecord(state,engine)
    sessions[state.id],locks[state.id] = record,asyncio.Lock()
    command.terminal,command.response_status = True,201
    command.response_message = f"Session created with immutable config {state.config_ref}@{state.config_sha256[:12]}."
    await _save(record,{'type':'session_created','config_ref':state.config_ref,'config_sha256':state.config_sha256})
    return SessionResponse(session=state,message=command.response_message)


@app.get("/sessions/{session_id}", response_model=SessionState)
async def get_session(session_id: str) -> SessionState:
    return (await _record(session_id)).state


@app.get('/sessions/{session_id}/transcript')
async def get_transcript(session_id: str, after_sequence: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=1000), through_sequence: int | None = Query(default=None,ge=0)) -> dict:
    record = await _record(session_id)
    through_sequence = min(through_sequence,record.state.next_transcript_sequence-1) if through_sequence is not None else record.state.next_transcript_sequence-1
    entries = await session_repository.transcript(session_id, after_sequence, limit, through_sequence=through_sequence)
    return {'session_id': session_id, 'snapshot_revision': record.state.revision,
            'history_complete': record.state.history_complete, 'through_sequence': through_sequence, 'entries': entries,
            'next_after_sequence': entries[-1]['sequence'] if entries else after_sequence}


@app.post("/sessions/{session_id}/messages", response_model=SessionResponse)
async def append_user_message(session_id: str, request: AppendUserMessageRequest) -> SessionResponse:
    record = await _record(session_id)
    command = current_command.get()
    command.response_message = 'User input appended to session memory.'
    async with ExecutionCoordinator(session_repository, record.state, record.engine.config) as execution:
        persisted = await session_repository.get(session_id)
        record.state = persisted.state
        execution.state = record.state
        record.state.execution_owner, record.state.execution_fence = execution.owner, execution.fence
        if record.state.status != "active" or record.state.pending_tool_calls:
            raise HTTPException(409, "Use /resume to resolve terminal or pending execution before adding input")
        if record.state.run_status == 'running':
            await execution.finish('interrupted', 'abandoned_before_new_user_message')
        record.state.run_id = None
        record.state.run_status = 'idle'
        record.state.run_started_at = record.state.run_ended_at = None
        append_message(record.state, Message(role="user", content=request.content))
        record.state.no_progress_steps, record.state.progress_fingerprint = 0, None
        command.terminal = True
        await execution.persist({"type": "user_message_appended"})
    return SessionResponse(session=record.state, message="User input appended to session memory.")


@app.exception_handler(OwnershipConflict)
async def ownership_handler(_, error: OwnershipConflict):
    return JSONResponse(status_code=409, content={'type': 'error', 'code': error.code,
        'message': str(error), 'recoverable': True})


@app.get('/sessions/{session_id}/runs')
async def get_runs(session_id: str, after: str | None = None, limit: int = Query(default=100,ge=1,le=1000)):
    from uuid import UUID
    if after is not None:
        try:
            UUID(after)
        except ValueError as error:
            raise HTTPException(422,'after must be a run UUID') from error
    await _record(session_id)
    rows=await session_repository.run_page(session_id,after,limit)
    return {'session_id':session_id,'runs':rows,'next_after':rows[-1]['run_id'] if rows else after}


@app.get('/sessions/{session_id}/diagnostics/{family}')
async def get_diagnostics(session_id: str,family: str,after: int = Query(default=0,ge=0),
        limit: int = Query(default=100,ge=1,le=1000),through: int | None = Query(default=None,ge=0)):
    from .storage_state import FAMILIES
    if family not in FAMILIES:
        raise HTTPException(422,'Unknown diagnostic family')
    state=(await _record(session_id)).state
    watermark=state.diagnostic_counts.get(family,0)
    through=min(through,watermark) if through is not None else watermark
    rows=await session_repository.diagnostic_page(session_id,family,after,limit,through)
    return {'session_id':session_id,'family':family,'through':through,'total_recorded':watermark,
        'entries':rows,'next_after':rows[-1]['ordinal'] if rows else after,'archive_id':state.last_archive_id,
        'history_complete':state.diagnostic_history_complete and state.last_archive_id is None}


@app.get('/sessions/{session_id}/events')
async def get_events(session_id: str,after: int = Query(default=0,ge=0),
        limit: int = Query(default=100,ge=1,le=1000),through: int | None = Query(default=None,ge=0)):
    state=(await _record(session_id)).state
    watermark=await session_repository.event_watermark(session_id)
    through=min(through,watermark) if through is not None else watermark
    rows=await session_repository.event_page(session_id,after,limit,through)
    return {'session_id':session_id,'through':through,'entries':rows,
        'next_after':rows[-1]['event_id'] if rows else after,'archive_id':state.last_archive_id}


@app.get('/sessions/{session_id}/archives')
async def get_archives(session_id: str,after: str | None = None,limit: int = Query(default=100,ge=1,le=1000)):
    from uuid import UUID
    if after is not None:
        try:
            UUID(after)
        except ValueError as error:
            raise HTTPException(422,'after must be an archive UUID') from error
    await _record(session_id)
    rows=await session_repository.archive_page(session_id,after,limit)
    return {'session_id':session_id,'archives':rows,'next_after':rows[-1]['archive_id'] if rows else after}


@app.get('/sessions/{session_id}/retention')
async def retention_plan(session_id: str):
    await _record(session_id)
    return await session_repository.retention_plan(session_id)


@app.get('/sessions/{session_id}/executions')
async def get_executions(session_id: str, after: str | None = None, limit: int = Query(default=100, ge=1, le=1000)):
    from uuid import UUID
    if after is not None:
        try:
            UUID(after)
        except ValueError as error:
            raise HTTPException(422, 'after must be an execution UUID') from error
    await _record(session_id)
    rows = await session_repository.execution_history(session_id, after, limit)
    return {'session_id': session_id, 'executions': rows,
        'next_after': rows[-1]['execution_id'] if rows else after}


@app.post('/sessions/{session_id}/cancel')
async def cancel_session(session_id: str, request: CancelSessionRequest):
    await _record(session_id)
    command = current_command.get()
    async with await psycopg.AsyncConnection.connect(session_repository.database_url) as db:
        async with db.cursor() as cursor:
            await session_repository.commands.guard(cursor,command)
            await cursor.execute('SELECT session_id FROM agent_sessions WHERE session_id=%s FOR UPDATE',(session_id,))
            await cursor.execute("UPDATE agent_runs SET cancel_requested=TRUE,cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp()) WHERE session_id=%s AND run_id=%s AND status='running' RETURNING run_id::text",(session_id,request.run_id))
            ids = [row[0] for row in await cursor.fetchall()]
            body = {'session_id':session_id,'cancel_requested_for':ids,'message':'Cancellation is cooperative; in-flight effects are not undone.'}
            await cursor.execute('UPDATE agent_commands SET session_id=%s WHERE command_id=%s',(session_id,command.id))
            await session_repository.commands.complete(cursor,command,200,body)
    return body


@app.get('/runs/abandoned')
async def abandoned_runs(limit: int = Query(default=100, ge=1, le=1000)):
    return {'runs': await session_repository.abandoned(limit)}


def _sse(event: dict[str, object]) -> str:
    identity = f"id: {event['event_id']}\n" if event.get('event_id') is not None else ''
    return identity + f"data: {json.dumps(event, default=str)}\n\n"


async def _execute_run(record, request, emit=None, resume=False, stop_requested=None):
    async with ExecutionCoordinator(session_repository, record.state, record.engine.config) as execution:
        # Claim first, then hydrate. A pre-claim snapshot is never used to run.
        persisted = await session_repository.get(record.state.id)
        record.state = persisted.state
        execution.state = record.state
        record.state.execution_owner, record.state.execution_fence = execution.owner, execution.fence
        sessions[record.state.id] = record
        if not resume and record.state.status != 'active':
            body = SessionResponse(session=record.state,message=record.state.final_answer or 'Run stopped.').model_dump(mode='json')
            await session_repository.commands.finish_without_save(current_command.get(),200,body)
            return record
        if resume:
            record.state.status, record.state.final_answer = 'active', None
            record.state.consecutive_invalid_responses, record.state.last_error = 0, None
            record.state.no_progress_steps, record.state.progress_fingerprint = 0, None
            record.state.last_compaction_attempt_sha256 = None
            if request.content and record.state.pending_tool_calls:
                raise OwnershipConflict('Recover pending tool calls with /resume without content before adding new instructions')
        await execution.start()
        if stop_requested and stop_requested.is_set():
            await session_repository.cancel_run(record.state.id,record.state.run_id)
        if resume and request.content:
            run_id = record.state.run_id
            record.state.run_id = None
            append_message(record.state, Message(role='user', content=request.content))
            record.state.run_id = run_id
            await execution.persist({'type': 'session_resumed', 'has_new_user_message': True})

        async def persist(event):
            await execution.persist(event)
            if emit:
                await emit(event)

        try:
            await record.engine.run(record.state, request.max_steps, persist, execution=execution)
        except RunCancelled:
            record.state.status, record.state.final_answer = 'failed', 'Run cancelled; use /resume to recover committed work.'
            record.state.last_error = {'code': 'run_cancelled', 'message': record.state.final_answer, 'recoverable': True}
            await execution.finish('cancelled', 'cancel_requested')
            if emit:
                await emit({'type': 'error', **record.state.last_error})
            return record
        except RecoveryBlocked as error:
            record.state.status, record.state.final_answer = 'failed', str(error)
            record.state.last_error = {'code': 'recovery_required', 'message': str(error), 'recoverable': True}
            await execution.finish('blocked', 'uncertain_tool_outcome')
            if emit:
                await emit({'type': 'error', **record.state.last_error})
            return record
        except asyncio.CancelledError:
            record.state.status, record.state.final_answer = 'failed', 'Worker task interrupted; recover with /resume.'
            record.state.last_error = {'code': 'run_interrupted', 'message': record.state.final_answer, 'recoverable': True}
            try:
                await asyncio.shield(execution.finish('interrupted', 'task_cancelled'))
            except Exception:
                pass  # committed started records remain authoritative
            raise
        except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError):
            record.state.status, record.state.final_answer = 'failed', 'Agent run failed; resume to retry.'
            record.state.last_error = {'code': 'controller_error', 'message': record.state.final_answer, 'recoverable': True}
            await persist({'type': 'error', **record.state.last_error})
        await execution.finish()
        return record


async def _watch_disconnect(request, session_id, done, stop_requested):
    while not done.is_set():
        if await request.is_disconnected():
            stop_requested.set()
            await session_repository.cancel_run(session_id, sessions[session_id].state.run_id)
            return
        try:
            await asyncio.wait_for(done.wait(), timeout=0.25)
        except TimeoutError:
            pass


@app.post("/sessions/{session_id}/run", response_model=SessionResponse)
async def run_session(session_id: str, request: RunRequest, connection: Request) -> SessionResponse:
    record = await _record(session_id)
    done = asyncio.Event()
    stop_requested = asyncio.Event()
    watcher = asyncio.create_task(_watch_disconnect(connection, session_id, done, stop_requested))
    try:
        await _execute_run(record, request, stop_requested=stop_requested)
    finally:
        done.set()
        watcher.cancel()
        from contextlib import suppress
        with suppress(asyncio.CancelledError):
            await watcher
    return SessionResponse(session=record.state, message=record.state.final_answer or 'Run stopped.')


async def _run_stream(record, request, resume=False):
    queue = asyncio.Queue()
    disconnected = False
    stop_requested = asyncio.Event()

    async def emit(event):
        if not disconnected:
            await queue.put(event)

    task = asyncio.create_task(_execute_run(record, request, emit, resume, stop_requested))
    try:
        while not task.done() or not queue.empty():
            try:
                yield _sse(await asyncio.wait_for(queue.get(), timeout=0.25))
            except TimeoutError:
                continue
        await task
    except (RevisionConflict, OwnershipConflict) as error:
        if isinstance(error, RevisionConflict):
            yield _sse(_conflict_event(error))
        else:
            yield _sse({'type': 'error', 'code': error.code, 'message': str(error), 'recoverable': True})
    finally:
        disconnected = True
        stop_requested.set()
        if not task.done():
            # Let the current invocation finish and commit its known result.
            # The owner acknowledges cancellation before scheduling more work.
            with anyio.CancelScope(shield=True):
                await session_repository.cancel_run(record.state.id,record.state.run_id)
                try:
                    await asyncio.shield(task)
                except (RevisionConflict, OwnershipConflict, asyncio.CancelledError):
                    pass


@app.post("/sessions/{session_id}/run/stream")
async def stream_run_session(session_id: str, request: RunRequest) -> StreamingResponse:
    record = await _record(session_id)
    if record.state.status != 'active':
        raise HTTPException(409, 'Session is terminal; use /resume')
    return StreamingResponse(_run_stream(record, request), media_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'})


@app.post("/sessions/{session_id}/resume")
async def resume_session(session_id: str, request: ResumeSessionRequest) -> StreamingResponse:
    record = await _record(session_id)
    if request.content and record.state.pending_tool_calls:
        raise HTTPException(409, 'Recover pending calls with /resume without content first')
    return StreamingResponse(_run_stream(record, request, resume=True), media_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'})


@app.get('/sessions/{session_id}/events/stream')
async def reconnect_events(session_id: str, connection: Request, run_id: UUID,
                           after_event_id: int = Query(default=0,ge=0)):
    await _record(session_id)
    async with await psycopg.AsyncConnection.connect(session_repository.database_url) as db:
        cursor = await db.execute('SELECT status FROM agent_runs WHERE session_id=%s AND run_id=%s',(session_id,run_id))
        if await cursor.fetchone() is None:
            raise HTTPException(404,'Run not found')

    async def read():
        after = after_event_id
        while not await connection.is_disconnected():
            async with await psycopg.AsyncConnection.connect(session_repository.database_url) as db:
                cursor = await db.execute("SELECT event_id,event FROM agent_session_events WHERE session_id=%s AND event_id>%s AND event->>'run_id'=%s ORDER BY event_id LIMIT 100",(session_id,after,str(run_id)))
                rows = await cursor.fetchall()
                cursor = await db.execute('SELECT status FROM agent_runs WHERE session_id=%s AND run_id=%s',(session_id,run_id))
                status = await cursor.fetchone()
            for identity,event in rows:
                after = identity
                yield _sse({**event,'event_id':identity})
            if not rows and (not status or status[0]!='running'):
                return
            if not rows:
                await asyncio.sleep(0.25)
    return StreamingResponse(read(),media_type='text/event-stream',headers={'Cache-Control':'no-cache'})


def contract_openapi():
    from fastapi.openapi.utils import get_openapi
    if app.openapi_schema is None:
        schema = get_openapi(title=app.title,version=app.version,routes=app.routes)
        for path in schema['paths'].values():
            if 'post' in path:
                operation = path['post']
                operation.setdefault('parameters',[]).append({'in':'header','name':'Idempotency-Key','required':True,
                    'description':'Retain the same key for retries of the same logical request.',
                    'schema':{'type':'string','minLength':1,'maxLength':255,'pattern':'^[!-~]+$'}})
                operation['responses'].update({'202':{'description':'Existing command is in progress or requires recovery'},
                    '409':{'description':'Key reused with different content, or ownership conflict'}})
        app.openapi_schema = schema
    return app.openapi_schema


app.openapi = contract_openapi
