"""FastAPI surface with built-in Swagger UI at /docs."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .config import load_config
from .engine import AgentEngine
from .models import AppendUserMessageRequest, CreateSessionRequest, Message, RunRequest, SessionResponse, SessionState

config = load_config()
engine = AgentEngine(config)
sessions: dict[str, SessionState] = {}
locks: dict[str, asyncio.Lock] = {}


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield


app = FastAPI(title="State-Driven AI Agent", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "model": config.model}


@app.post("/sessions", response_model=SessionResponse, status_code=201)
async def create_session(request: CreateSessionRequest) -> SessionResponse:
    state = engine.create_session(request.user_prompt)
    sessions[state.id], locks[state.id] = state, asyncio.Lock()
    return SessionResponse(session=state, message="Session created; user goal is the first memory message.")


@app.get("/sessions/{session_id}", response_model=SessionState)
async def get_session(session_id: str) -> SessionState:
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")
    return sessions[session_id]


@app.post("/sessions/{session_id}/messages", response_model=SessionResponse)
async def append_user_message(session_id: str, request: AppendUserMessageRequest) -> SessionResponse:
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")
    state = sessions[session_id]
    if state.status != "active":
        raise HTTPException(409, "Cannot append to a completed or failed session")
    state.memory.append(Message(role="user", content=request.content))
    return SessionResponse(session=state, message="User input appended to session memory.")


@app.post("/sessions/{session_id}/run", response_model=SessionResponse)
async def run_session(session_id: str, request: RunRequest) -> SessionResponse:
    if session_id not in sessions:
        raise HTTPException(404, "Session not found")
    async with locks[session_id]:
        state = await engine.run(sessions[session_id], request.max_steps)
    return SessionResponse(session=state, message=state.final_answer or "Run stopped.")
