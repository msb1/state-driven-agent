"""The framework-free state-machine loop: compact -> decide -> tool -> append."""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

from .config import AgentConfig, PhaseTransitionConfig, ToolEvidenceConfig, WorkflowPhaseConfig
from .llm import LlmClient
from .models import Message, SessionState, WorkflowState, WorkflowTransition
from .tools import execute


class AgentEngine:
    def __init__(self, config: AgentConfig) -> None:
        self.config, self.llm = config, LlmClient(config)

    def create_session(self, goal: str, config_ref: str | None = None, config_sha256: str | None = None) -> SessionState:
        # This is the immutable anchor in mutable session memory.
        workflow = WorkflowState(active_phase=self.config.workflow.entry_phase) if self.config.workflow else None
        return SessionState(
            id=str(uuid4()), goal=goal, config_ref=config_ref, config_sha256=config_sha256,
            memory=[Message(role="user", content=goal)], workflow=workflow,
        )

    async def run(
        self,
        state: SessionState,
        max_steps: int | None = None,
        emit: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> SessionState:
        """Run up to ``max_steps`` additional decisions and emit safe milestones."""
        limit = state.step_count + (max_steps or self.config.memory.max_steps)
        state.run_count += 1
        await self._emit(emit, {"type": "status", "message": "Agent run started.", "run_count": state.run_count})
        while state.status == "active" and state.step_count < limit:
            transition = self._advance_workflow(state)
            if transition:
                await self._emit(emit, {"type": "interim_result", "step": "phase_completed", **transition})
            compacted = await self._compact_if_needed(state)
            if compacted:
                await self._emit(emit, {"type": "interim_result", "step": "compaction", "data": {"compaction_count": len(state.compaction_events)}})
            await self._emit(emit, {"type": "status", "message": "Agent deciding next action.", "step_count": state.step_count})
            decision = await self.llm.decide(self._payload(state))
            state.step_count += 1
            if decision["type"] == "final":
                state.memory.append(Message(role="assistant", content=decision["content"]))
                if not self.config.workflow or self._workflow_complete(state):
                    state.final_answer, state.status = decision["content"], "complete"
                    await self._emit(emit, {"type": "final_result", "answer": state.final_answer})
                    break
                # A phase workflow never treats progress prose as completion.
                # The next payload names the outstanding evidence; max_steps
                # remains the finite failure boundary if the model will not act.
                continue
            arguments = decision["arguments"]
            state.memory.append(
                Message(
                    role="assistant",
                    content=None,
                    tool_calls=[
                        {
                            "id": decision["id"],
                            "type": "function",
                            "function": {
                                "name": decision["name"],
                                "arguments": json.dumps(arguments),
                            },
                        }
                    ],
                )
            )
            allowed_tools = self._allowed_tools(state)
            if allowed_tools is not None and decision["name"] not in allowed_tools:
                output = (
                    f"ERROR: Workflow phase '{state.workflow.active_phase}' cannot execute tool "
                    f"'{decision['name']}'. Complete the active phase first. "
                    f"Allowed tools: {', '.join(sorted(allowed_tools))}."
                )
            else:
                output = await execute(decision["name"], arguments)
            state.memory.append(Message(role="tool", name=decision["name"], tool_call_id=decision["id"], content=output))
            self._record_workflow_evidence(state, decision["name"], arguments, output)
            await self._emit(emit, {"type": "interim_result", "step": "tool_completed", "data": {"tool": decision["name"], "output": output}})
        if state.status == "active" and state.step_count >= limit:
            state.status = "failed"
            state.final_answer = f"Step limit ({max_steps or self.config.memory.max_steps}) reached without a final answer. Resume with another run request."
            await self._emit(emit, {"type": "error", "message": state.final_answer, "recoverable": True})
        return state

    @staticmethod
    async def _emit(emit: Callable[[dict[str, Any]], Awaitable[None]] | None, event: dict[str, Any]) -> None:
        if emit is not None:
            await emit(event)

    def _payload(self, state: SessionState) -> list[Message]:
        payload = [Message(role="system", content=self.config.system_prompt)]
        # Goal is always raw, regardless of compaction.
        payload.append(Message(role="user", content=f"Original user goal (immutable): {state.goal}"))
        if state.compacted_context:
            payload.append(Message(role="system", content=f"[COMPACTED CONTEXT STATE]\n{state.compacted_context}"))
        workflow_instruction = self._workflow_instruction(state)
        if workflow_instruction:
            payload.append(Message(role="system", content=workflow_instruction))
        if state.memory[1:]:
            payload.append(Message(role="system", content="[RAW WORKING BUFFER — PRESERVE VERBATIM]"))
        payload.extend(state.memory[1:])  # avoid duplicating the initial goal
        return payload

    def _workflow_instruction(self, state: SessionState) -> str | None:
        if not self.config.workflow or not state.workflow:
            return None
        if state.workflow.active_phase is None:
            return "[WORKFLOW STATE]\nAll configured phases have verified completion evidence. You may now provide the final answer."
        phase = self._phase(state.workflow.active_phase)
        missing = self._missing_phase_evidence(state, phase)
        return (
            "[WORKFLOW STATE]\n"
            f"Active phase: {phase.name} ({phase.id}).\n"
            f"Phase instruction: {phase.instruction}\n"
            "Do not provide prose progress updates or a final answer while this phase is incomplete; make a tool call instead.\n"
            "Outstanding required evidence:\n"
            + "\n".join(f"- {item}" for item in missing)
        )

    def _workflow_complete(self, state: SessionState) -> bool:
        return bool(self.config.workflow) and bool(state.workflow) and state.workflow.active_phase is None

    def _allowed_tools(self, state: SessionState) -> set[str] | None:
        if not self.config.workflow or not state.workflow or state.workflow.active_phase is None:
            return None
        phase = self._phase(state.workflow.active_phase)
        if phase.allowed_tools is not None:
            return set(phase.allowed_tools)
        return {evidence.tool for evidence in phase.completion}

    def _phase(self, phase_id: str) -> WorkflowPhaseConfig:
        assert self.config.workflow is not None
        return next(phase for phase in self.config.workflow.phases if phase.id == phase_id)

    def _advance_workflow(self, state: SessionState) -> dict[str, str] | None:
        if not self.config.workflow:
            return None
        if state.workflow is None:
            state.workflow = WorkflowState(active_phase=self.config.workflow.entry_phase)
        if state.workflow.active_phase is None:
            return None
        phase = self._phase(state.workflow.active_phase)
        for index, transition in enumerate(phase.transitions):
            if self._transition_applies(state, phase, transition, index):
                previous = phase.id
                if previous not in state.workflow.completed_phases:
                    state.workflow.completed_phases.append(previous)
                state.workflow.active_phase = None if transition.to == "complete" else transition.to
                state.workflow.transitions.append(
                    WorkflowTransition(from_phase=previous, to_phase=transition.to, reason=transition.when)
                )
                return {"phase_id": previous, "next_phase": transition.to, "reason": transition.when}
        return None

    def _transition_applies(self, state: SessionState, phase: WorkflowPhaseConfig, transition: PhaseTransitionConfig, index: int) -> bool:
        if transition.when == "phase_complete":
            return not self._missing_phase_evidence(state, phase)
        if transition.when == "always":
            return True
        return all(self._evidence_key(phase.id, f"transition-{index}", item_index) in state.workflow.observed_evidence
                   for item_index, _ in enumerate(transition.evidence))

    def _missing_phase_evidence(self, state: SessionState, phase: WorkflowPhaseConfig) -> list[str]:
        if state.workflow is None:
            return [evidence.description for evidence in phase.completion]
        return [
            evidence.description
            for index, evidence in enumerate(phase.completion)
            if evidence.required and self._evidence_key(phase.id, "completion", index) not in state.workflow.observed_evidence
        ]

    def _record_workflow_evidence(self, state: SessionState, tool_name: str, arguments: dict[str, object], output: str) -> None:
        if not self.config.workflow or not state.workflow or state.workflow.active_phase is None:
            return
        phase = self._phase(state.workflow.active_phase)
        candidates: list[tuple[str, int, ToolEvidenceConfig]] = [("completion", index, evidence) for index, evidence in enumerate(phase.completion)]
        for transition_index, transition in enumerate(phase.transitions):
            candidates.extend((f"transition-{transition_index}", index, evidence) for index, evidence in enumerate(transition.evidence))
        for category, index, evidence in candidates:
            key = self._evidence_key(phase.id, category, index)
            if key not in state.workflow.observed_evidence and self._matches_evidence(evidence, tool_name, arguments, output):
                state.workflow.observed_evidence.append(key)

    @staticmethod
    def _evidence_key(phase_id: str, category: str, index: int) -> str:
        return f"{phase_id}:{category}:{index}"

    @staticmethod
    def _matches_evidence(evidence: ToolEvidenceConfig, tool_name: str, arguments: dict[str, object], output: str) -> bool:
        if evidence.tool != tool_name or not AgentEngine._contains(arguments, evidence.arguments):
            return False
        if any(text.lower() not in output.lower() for text in evidence.content_contains):
            return False
        if not evidence.result:
            return True
        try:
            result = json.loads(output)
        except json.JSONDecodeError:
            return False
        return isinstance(result, dict) and AgentEngine._contains(result, evidence.result)

    @staticmethod
    def _contains(actual: object, expected: object) -> bool:
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(key in actual and AgentEngine._contains(actual[key], value) for key, value in expected.items())
        if isinstance(expected, list):
            return isinstance(actual, list) and all(item in actual for item in expected)
        return actual == expected

    async def _compact_if_needed(self, state: SessionState) -> bool:
        approximate_tokens = sum(len(message.content or "") // 4 + 1 for message in state.memory)
        due = approximate_tokens >= self.config.memory.max_tokens or state.step_count >= self.config.memory.max_steps
        keep = self.config.memory.raw_turns_to_keep
        if not due or len(state.memory) <= keep + 1:
            return False
        # Preserve goal plus the newest high-fidelity working buffer.
        historical, raw = state.memory[1:-keep], state.memory[-keep:]
        state.compacted_context = await self.llm.compact(
            state.goal,
            historical,
            state.compacted_context,
            hybrid_reflection=self.config.memory.hybrid_reflection,
        )
        state.compaction_events.append(state.compacted_context)
        state.memory = [state.memory[0], *raw]
        return True
