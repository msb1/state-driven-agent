"""Case 4: persist an interrupted Case 3 session, reload it, then finish it."""
from __future__ import annotations

import asyncio

from agent_python.config import ConfigRepository
from agent_python.engine import AgentEngine
from agent_python.persistence import SessionRepository
from verify_test_case_3 import GOAL, REPLAY_STEPS, ReplayLlm, verify_audit_environment


def replay_for_step(step_count: int, compacted: bool) -> ReplayLlm:
    replay = ReplayLlm()
    replay.index = step_count
    replay.compacted = compacted
    return replay


async def main_async() -> None:
    repository = SessionRepository()
    await repository.initialize()
    snapshot = ConfigRepository().resolve("test-case-3.yaml")
    first_engine = AgentEngine(snapshot.config)
    first_engine.llm = replay_for_step(0, False)  # type: ignore[assignment]
    state = first_engine.create_session(GOAL, snapshot.ref, snapshot.sha256)

    # Simulates a bounded worker run that dies/restarts before Phase 3.
    await first_engine.run(state, max_steps=7)
    if state.status != "failed" or not state.workflow or not state.workflow.completed_phases:
        raise AssertionError("Case 4 must persist a non-terminal workflow after the bounded first run")
    await repository.save(state, first_engine.config, {"type": "test_case_4_interrupted"})

    # A fresh repository/engine represents a new API process: no in-memory state.
    restored = await SessionRepository().get(state.id)
    if restored is None or restored.state.model_dump() != state.model_dump():
        raise AssertionError("Persisted state was not restored exactly")
    if restored.config.model_dump() != snapshot.config.model_dump():
        raise AssertionError("Resolved immutable config snapshot was not restored")
    resumed_engine = AgentEngine(restored.config)
    resumed_engine.llm = replay_for_step(restored.state.step_count, bool(restored.state.compaction_events))  # type: ignore[assignment]
    restored.state.status, restored.state.final_answer = "active", None
    await resumed_engine.run(restored.state, max_steps=len(REPLAY_STEPS) + 4)
    await repository.save(restored.state, resumed_engine.config, {"type": "test_case_4_resumed"})

    payload = restored.state.model_dump()
    if not verify_audit_environment(payload["final_answer"] or "", payload["memory"], payload["compaction_events"], payload["workflow"]):
        raise AssertionError("Case 4 resumed workflow did not complete")
    print(f"✅ Case 4 persisted and resumed session {state.id} across a simulated process restart.")


if __name__ == "__main__":
    asyncio.run(main_async())
