"""FastAPI surface, PostgreSQL session persistence, and SSE lifecycle output."""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import httpx

from .config import ConfigRepository
from .engine import AgentEngine
from .models import AppendUserMessageRequest, CreateSessionRequest, Message, ResumeSessionRequest, RunRequest, SessionResponse, SessionState
from .persistence import SessionRepository
from .tools import TOOLS

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
    await session_repository.initialize()
    yield


app = FastAPI(title="State-Driven AI Agent", version="0.2.0", lifespan=lifespan)


async def _record(session_id: str) -> SessionRecord:
    if session_id in sessions:
        return sessions[session_id]
    persisted = await session_repository.get(session_id)
    if persisted is None:
        raise HTTPException(404, "Session not found")
    record = SessionRecord(state=persisted.state, engine=AgentEngine(persisted.config))
    sessions[session_id], locks[session_id] = record, asyncio.Lock()
    return record


async def _save(record: SessionRecord, event: dict[str, object] | None = None) -> None:
    await session_repository.save(record.state, record.engine.config, event)


@app.get("/health")
async def health() -> dict[str, str | int]:
    return {"status": "ok", "config_root": str(config_repository.root), "available_configs": len(config_repository.list())}


@app.get("/configs")
async def list_configs() -> list[dict[str, object]]:
    return [{"config_ref": s.ref, "sha256": s.sha256, "agent_name": s.config.name,
             "tools": [tool.name for tool in s.config.tools],
             "workflow_phases": [p.id for p in s.config.workflow.phases] if s.config.workflow else [],
             "verbose_setup": s.config.output.verbose_setup}
            for s in config_repository.list() if all(tool.name in TOOLS for tool in s.config.tools)]


@app.post("/sessions", response_model=SessionResponse, status_code=201)
async def create_session(request: CreateSessionRequest) -> SessionResponse:
    try:
        snapshot = config_repository.resolve(request.config_ref)
        unknown = sorted({tool.name for tool in snapshot.config.tools} - set(TOOLS))
        if unknown:
            raise ValueError(f"config references unavailable compiled-in tools: {', '.join(unknown)}")
    except (OSError, ValueError) as error:
        raise HTTPException(422, f"Invalid config_ref: {error}") from error
    engine = AgentEngine(snapshot.config)
    state = engine.create_session(request.user_prompt, snapshot.ref, snapshot.sha256)
    record = SessionRecord(state, engine)
    sessions[state.id], locks[state.id] = record, asyncio.Lock()
    await _save(record, {"type": "session_created", "config_ref": snapshot.ref, "config_sha256": snapshot.sha256})
    return SessionResponse(session=state, message=f"Session created with immutable config {snapshot.ref}@{snapshot.sha256[:12]}.")


@app.get("/sessions/{session_id}", response_model=SessionState)
async def get_session(session_id: str) -> SessionState:
    return (await _record(session_id)).state


@app.post("/sessions/{session_id}/messages", response_model=SessionResponse)
async def append_user_message(session_id: str, request: AppendUserMessageRequest) -> SessionResponse:
    record = await _record(session_id)
    if record.state.status != "active":
        raise HTTPException(409, "Cannot append to a terminal session; use /resume with content instead")
    record.state.memory.append(Message(role="user", content=request.content))
    await _save(record, {"type": "user_message_appended"})
    return SessionResponse(session=record.state, message="User input appended to session memory.")


def _sse(event: dict[str, object]) -> str:
    return f"data: {json.dumps(event, default=str)}\n\n"


async def _run_stream(record: SessionRecord, request: RunRequest, setup: str | None = None) -> AsyncIterator[str]:
    queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()

    async def emit(event: dict[str, object]) -> None:
        await _save(record, event)  # persist before exposing it to the client
        await queue.put(event)

    if setup and record.engine.config.output.verbose_setup:
        await emit({"type": "status", "message": setup, "config_ref": record.state.config_ref})
    task = asyncio.create_task(record.engine.run(record.state, request.max_steps, emit))
    try:
        while not task.done() or not queue.empty():
            try:
                yield _sse(await asyncio.wait_for(queue.get(), timeout=0.25))
            except TimeoutError:
                continue
        await task
        await _save(record, {"type": "run_finished", "status": record.state.status})
        yield _sse({"type": "status", "message": "Agent run finished.", "status": record.state.status})
    except (httpx.HTTPError, ValueError, KeyError, IndexError) as error:
        record.state.status, record.state.final_answer = "failed", f"Agent run failed: {error}"
        event = {"type": "error", "message": record.state.final_answer, "recoverable": True}
        await _save(record, event)
        yield _sse(event)


@app.post("/sessions/{session_id}/run", response_model=SessionResponse)
async def run_session(session_id: str, request: RunRequest) -> SessionResponse:
    record = await _record(session_id)
    async with locks.setdefault(session_id, asyncio.Lock()):
        if record.state.status != "active":
            return SessionResponse(session=record.state, message="Session is terminal; use POST /sessions/{id}/resume to continue it.")
        async def persist_milestone(event: dict[str, object]) -> None:
            # JSON clients do not consume milestones, but saving them here
            # makes a running session recoverable if this worker disappears.
            await _save(record, event)
        try:
            await record.engine.run(record.state, request.max_steps, persist_milestone)
        except httpx.HTTPStatusError as error:
            raise HTTPException(502, f"Model service returned {error.response.status_code} for {error.request.url}") from error
        except (httpx.RequestError, ValueError, KeyError, IndexError) as error:
            raise HTTPException(502, f"Model service response was invalid: {error}") from error
        await _save(record, {"type": "run_finished", "status": record.state.status})
    return SessionResponse(session=record.state, message=record.state.final_answer or "Run stopped.")


@app.post("/sessions/{session_id}/run/stream")
async def stream_run_session(session_id: str, request: RunRequest) -> StreamingResponse:
    record = await _record(session_id)
    if record.state.status != "active":
        raise HTTPException(409, "Session is terminal; use POST /sessions/{id}/resume to continue it")

    async def guarded() -> AsyncIterator[str]:
        async with locks.setdefault(session_id, asyncio.Lock()):
            async for event in _run_stream(record, request, "Agent initialized from persisted session state."):
                yield event
    return StreamingResponse(guarded(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})


@app.post("/sessions/{session_id}/resume")
async def resume_session(session_id: str, request: ResumeSessionRequest) -> StreamingResponse:
    """Restore a terminal session, compact if needed, and stream its continuation."""
    record = await _record(session_id)

    async def guarded() -> AsyncIterator[str]:
        async with locks.setdefault(session_id, asyncio.Lock()):
            if request.content:
                record.state.memory.append(Message(role="user", content=request.content))
            record.state.status, record.state.final_answer = "active", None
            await _save(record, {"type": "session_resumed", "has_new_user_message": bool(request.content)})
            yield _sse({"type": "status", "message": "Session restored from PostgreSQL; resuming agent work."})
            async for event in _run_stream(record, request, "Restored immutable config, workflow evidence, memory, and compaction ledger."):
                yield event
    return StreamingResponse(guarded(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive"})
