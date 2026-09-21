"""Case 5: reload a completed session and continue it with a new user message."""
from __future__ import annotations

import asyncio

from agent_python.config import ConfigRepository
from agent_python.engine import AgentEngine
from agent_python.models import Message
from agent_python.persistence import SessionRepository
from verify_test_case_3 import GOAL, REPLAY_STEPS, ReplayLlm


def replay_after(step_count: int, compacted: bool) -> ReplayLlm:
    replay = ReplayLlm()
    replay.index, replay.compacted = step_count, compacted
    return replay


async def main_async() -> None:
    repository = SessionRepository()
    await repository.initialize()
    snapshot = ConfigRepository().resolve("test-case-3.yaml")
    engine = AgentEngine(snapshot.config)
    engine.llm = replay_after(0, False)  # type: ignore[assignment]
    state = engine.create_session(GOAL, snapshot.ref, snapshot.sha256)
    await engine.run(state, max_steps=len(REPLAY_STEPS) + 4)
    if state.status != "complete":
        raise AssertionError("Case 5 fixture must first complete")
    await repository.save(state, engine.config, {"type": "test_case_5_completed"})

    restored = await SessionRepository().get(state.id)
    if restored is None or restored.state.status != "complete":
        raise AssertionError("Completed session did not survive reload")
    # This mirrors POST /sessions/{id}/resume with a content field.
    restored.state.memory.append(Message(role="user", content="Briefly restate the validated final report."))
    restored.state.status, restored.state.final_answer = "active", None
    resumed_engine = AgentEngine(restored.config)
    resumed_engine.llm = replay_after(len(REPLAY_STEPS), bool(restored.state.compaction_events))  # type: ignore[assignment]
    await resumed_engine.run(restored.state, max_steps=2)
    await repository.save(restored.state, resumed_engine.config, {"type": "test_case_5_resumed_completed_session"})

    if restored.state.status != "complete" or not restored.state.final_answer:
        raise AssertionError("Completed session did not accept a continued run")
    if restored.state.run_count != 2 or restored.state.memory[-1].role != "assistant":
        raise AssertionError("Continuation history or run count was not preserved")
    print(f"✅ Case 5 resumed completed session {state.id} with preserved history and a new user turn.")


if __name__ == "__main__":
    asyncio.run(main_async())
