"""Data structures shared by the state machine and HTTP API."""
from __future__ import annotations

from typing import Any, Literal
from datetime import datetime, timezone
from uuid import uuid4
from pydantic import BaseModel, Field


class Message(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid4()), frozen=True)
    sequence: int | None = Field(default=None, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), frozen=True)
    role: Literal["system", "user", "assistant", "tool"]
    content: str | None = None
    name: str | None = None
    tool_call_id: str | None = None
    # OpenAI-compatible chat APIs require the assistant's function call to be
    # represented here, rather than inferred from an assistant message's name.
    tool_calls: list[dict[str, Any]] | None = None

    def wire_payload(self) -> dict[str, Any]:
        return self.model_dump(include={'role', 'content', 'name', 'tool_call_id', 'tool_calls'}, exclude_none=True)


class TranscriptEntry(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid4()), frozen=True)
    sequence: int = Field(ge=1)
    kind: str
    payload: dict[str, Any]
    run_id: str | None = None
    decision_step: int = Field(ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


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
    goal: str = Field(frozen=True)
    config_ref: str | None = Field(default=None, frozen=True)
    config_sha256: str | None = Field(default=None, frozen=True)
    resolved_config_sha256: str | None = None
    revision: int = Field(default=0, ge=0)
    next_transcript_sequence: int = Field(default=1, ge=1)
    transcript_pending: list[TranscriptEntry] = Field(default_factory=list)
    active_ledger_id: str | None = None
    history_complete: bool = True
    run_id: str | None = None
    pending_commit_id: str | None = None
    # Durable continuation/lifecycle. Ownership and write outboxes are private.
    run_status: Literal['idle', 'running', 'complete', 'failed', 'cancelled', 'interrupted', 'blocked'] = 'idle'
    run_started_at: datetime | None = None
    run_ended_at: datetime | None = None
    run_termination_reason: str | None = None
    pending_tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    execution_owner: str | None = Field(default=None, exclude=True)
    execution_fence: int | None = Field(default=None, exclude=True)
    execution_maintenance: bool = Field(default=False, exclude=True)
    execution_pending: list[dict[str, Any]] = Field(default_factory=list, exclude=True)
    consecutive_invalid_responses: int = 0
    no_progress_steps: int = 0
    progress_fingerprint: str | None = None
    progress_seen_outputs: list[str] = Field(default_factory=list)
    last_error: dict[str, Any] | None = None
    completion_artifacts: dict[str, dict[str, Any]] = Field(default_factory=dict)
    memory: list[Message] = Field(default_factory=list)
    compacted_context: str | None = None
    ledger_state: dict[str, Any] | None = None
    last_compaction_step: int = 0
    last_compaction_attempt_step: int | None = None
    last_compaction_estimated_tokens: int | None = None
    last_compaction_attempt_sha256: str | None = None
    compaction_decisions: list[dict[str, Any]] = Field(default_factory=list)
    compaction_skip_counts: dict[str, int] = Field(default_factory=dict)
    compaction_trigger_counts: dict[str, int] = Field(default_factory=dict)
    compaction_attempts: list[dict[str, Any]] = Field(default_factory=list)
    request_budget: dict[str, Any] | None = None
    # Audit telemetry remains outside the model's raw working buffer.
    compaction_events: list[str] = Field(default_factory=list)
    diagnostic_counts: dict[str, int] = Field(default_factory=dict)
    diagnostics_complete: bool = True
    diagnostic_history_complete: bool = True
    diagnostic_rejections: int = 0
    compaction_savings_total: int = 0
    compaction_savings_known: bool = True
    diagnostics_pending: list[dict[str, Any]] = Field(default_factory=list, exclude=True)
    retention_pending: dict[str, Any] | None = Field(default=None, exclude=True)
    last_archive_id: str | None = None
    workflow: WorkflowState | None = None
    step_count: int = 0
    run_count: int = 0
    status: Literal["active", "complete", "failed"] = "active"
    final_answer: str | None = None


class CreateSessionRequest(BaseModel):
    user_prompt: str = Field(min_length=1, description="The user's goal; becomes the first session-memory message.")
    config_ref: str = Field(default="log-analysis.yaml", min_length=1, description="Relative YAML path inside the server-approved config repository.")


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
