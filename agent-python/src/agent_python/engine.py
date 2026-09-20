"""The framework-free state-machine loop: compact -> decide -> tool -> append."""
from __future__ import annotations

import json
from uuid import uuid4

from .config import AgentConfig
from .llm import LlmClient
from .models import Message, SessionState
from .tools import execute


class AgentEngine:
    def __init__(self, config: AgentConfig) -> None:
        self.config, self.llm = config, LlmClient(config)

    def create_session(self, goal: str) -> SessionState:
        # This is the immutable anchor in mutable session memory.
        return SessionState(id=str(uuid4()), goal=goal, memory=[Message(role="user", content=goal)])

    async def run(self, state: SessionState, max_steps: int | None = None) -> SessionState:
        limit = max_steps or self.config.memory.max_steps
        while state.status == "active" and state.step_count < limit:
            await self._compact_if_needed(state)
            decision = await self.llm.decide(self._payload(state))
            state.step_count += 1
            if decision["type"] == "final":
                state.memory.append(Message(role="assistant", content=decision["content"]))
                state.final_answer, state.status = decision["content"], "complete"
                break
            arguments = decision["arguments"]
            state.memory.append(Message(role="assistant", name=decision["name"], tool_call_id=decision["id"], content=json.dumps(arguments)))
            output = await execute(decision["name"], arguments)
            state.memory.append(Message(role="tool", name=decision["name"], tool_call_id=decision["id"], content=output))
        if state.status == "active" and state.step_count >= limit:
            state.status = "failed"
            state.final_answer = f"Step limit ({limit}) reached without a final answer. Resume with another run request."
        return state

    def _payload(self, state: SessionState) -> list[Message]:
        payload = [Message(role="system", content=self.config.system_prompt)]
        # Goal is always raw, regardless of compaction.
        payload.append(Message(role="user", content=f"Original user goal (immutable): {state.goal}"))
        if state.compacted_context:
            payload.append(Message(role="system", content=f"[COMPACTED CONTEXT STATE]\n{state.compacted_context}"))
        payload.extend(state.memory[1:])  # avoid duplicating the initial goal
        return payload

    async def _compact_if_needed(self, state: SessionState) -> None:
        approximate_tokens = sum(len(message.content) // 4 + 1 for message in state.memory)
        due = approximate_tokens >= self.config.memory.max_tokens or state.step_count >= self.config.memory.max_steps
        keep = self.config.memory.raw_turns_to_keep
        if not due or len(state.memory) <= keep + 1:
            return
        # Preserve goal plus the newest high-fidelity working buffer.
        historical, raw = state.memory[1:-keep], state.memory[-keep:]
        state.compacted_context = await self.llm.compact(state.goal, historical, state.compacted_context)
        state.memory = [state.memory[0], *raw]
