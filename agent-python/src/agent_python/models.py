"""Data structures shared by the state machine and HTTP API."""
from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


class Message(BaseModel):
    role: Literal["system", "user", "assistant", "tool"]
    content: str
    name: str | None = None
    tool_call_id: str | None = None


class SessionState(BaseModel):
    id: str
    goal: str
    memory: list[Message] = Field(default_factory=list)
    compacted_context: str | None = None
    step_count: int = 0
    status: Literal["active", "complete", "failed"] = "active"
    final_answer: str | None = None


class CreateSessionRequest(BaseModel):
    user_prompt: str = Field(min_length=1, description="The user's goal; becomes the first session-memory message.")


class RunRequest(BaseModel):
    max_steps: int | None = Field(default=None, ge=1, le=50)


class AppendUserMessageRequest(BaseModel):
    content: str = Field(min_length=1, description="Additional user input appended to the active session memory.")


class SessionResponse(BaseModel):
    session: SessionState
    message: str
