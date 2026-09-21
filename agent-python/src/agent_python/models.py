"""Data structures shared by the state machine and HTTP API."""
from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


class Message(BaseModel):
    role: Literal["system", "user", "assistant", "tool"]
    content: str | None = None
    name: str | None = None
    tool_call_id: str | None = None
    # OpenAI-compatible chat APIs require the assistant's function call to be
    # represented here, rather than inferred from an assistant message's name.
    tool_calls: list[dict[str, Any]] | None = None


class WorkflowTransition(BaseModel):
    from_phase: str
    to_phase: str
    reason: str


class WorkflowState(BaseModel):
    """Persistent workflow facts kept outside compactable raw message history."""

    active_phase: str | None
    completed_phases: list[str] = Field(default_factory=list)
    observed_evidence: list[str] = Field(default_factory=list)
    transitions: list[WorkflowTransition] = Field(default_factory=list)


class SessionState(BaseModel):
    id: str
    goal: str
    config_ref: str | None = None
    config_sha256: str | None = None
    memory: list[Message] = Field(default_factory=list)
    compacted_context: str | None = None
    # Audit telemetry remains outside the model's raw working buffer.
    compaction_events: list[str] = Field(default_factory=list)
    workflow: WorkflowState | None = None
    step_count: int = 0
    run_count: int = 0
    status: Literal["active", "complete", "failed"] = "active"
    final_answer: str | None = None


class CreateSessionRequest(BaseModel):
    user_prompt: str = Field(min_length=1, description="The user's goal; becomes the first session-memory message.")
    config_ref: str = Field(default="test-case-1.yaml", min_length=1, description="Relative YAML path inside the server-approved config repository.")


class RunRequest(BaseModel):
    max_steps: int | None = Field(default=None, ge=1, le=50)


class ResumeSessionRequest(RunRequest):
    """Continue a terminal session, optionally with a new user turn."""

    content: str | None = Field(default=None, min_length=1)


class AppendUserMessageRequest(BaseModel):
    content: str = Field(min_length=1, description="Additional user input appended to the active session memory.")


class SessionResponse(BaseModel):
    session: SessionState
    message: str
