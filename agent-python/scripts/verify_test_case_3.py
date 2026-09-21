"""Run or verify the deterministic Case 3 workflow and compaction regression."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from agent_python.config import ConfigRepository
from agent_python.engine import AgentEngine
from agent_python.models import Message, SessionState


GOAL = """Perform a complete 3-Phase Financial Audit on the provided employee dataset:

Phase 1: Identify all rows with corrupted 'age' or 'salary' fields. Log their IDs.
Phase 2: Normalize the 'salary' field to a standard float. Calculate the exact median salary for valid employees living in 'New York' (case-insensitive).
Phase 3: Output a clean, final markdown table breaking down the total valid headcount and average age per unique city.

You must execute your steps incrementally. Do not try to solve all phases in a single script."""

REPLAY_STEPS: list[tuple[str, dict[str, Any]]] = [
    ("inspect_employee_audit_schema", {}),
    ("audit_employee_phase_1", {"parser": "naive"}),
    ("audit_employee_phase_1", {"parser": "currency_cleaned"}),
    ("audit_employee_phase_1", {"parser": "null_cleaned"}),
    ("audit_employee_phase_1", {"parser": "robust"}),
    ("calculate_new_york_median_salary", {"normalize_city": False, "exclude_invalid_salary": False}),
    ("calculate_new_york_median_salary", {"normalize_city": False, "exclude_invalid_salary": True}),
    ("calculate_new_york_median_salary", {"normalize_city": True, "exclude_invalid_salary": True}),
    ("generate_employee_phase_3_report", {"normalize_city": False, "drop_invalid_age": True}),
    ("generate_employee_phase_3_report", {"normalize_city": True, "drop_invalid_age": False}),
    ("generate_employee_phase_3_report", {"normalize_city": True, "drop_invalid_age": True}),
    ("validate_employee_audit", {"phase_1_logged": True, "median_salary": 92500.0, "normalize_city": True, "drop_invalid_age": True}),
]


class ReplayLlm:
    """Fixed model/reflection sequence for deterministic cross-language testing."""

    def __init__(self) -> None:
        self.index = 0
        self.compacted = False

    async def decide(self, messages: list[Message]) -> dict[str, Any]:
        if self.compacted and not all(
            term.lower() in "\n".join(message.content or "" for message in messages).lower()
            for term in ("GLOBAL_LESSON_LEDGER", "currency", "NULL", "semicolon", "UNKNOWN", "case-insensit")
        ):
            raise AssertionError("Hybrid lessons were not carried into a later decision payload")
        if self.index >= len(REPLAY_STEPS):
            return {"type": "final", "content": "Phase 3\n\n| City | Valid headcount | Average age |\n| --- | ---: | ---: |\n| Austin | validated | validated |\n| Chicago | validated | validated |\n| New York | validated | validated |\n| Seattle | validated | validated |"}
        name, arguments = REPLAY_STEPS[self.index]
        self.index += 1
        return {"type": "tool", "id": f"case3_replay_{self.index}", "name": name, "arguments": arguments}

    async def compact(self, goal: str, historical: list[Message], existing: str | None, hybrid_reflection: bool = True) -> str:
        if not hybrid_reflection:
            raise AssertionError("Case 3 requires hybrid_reflection=true")
        self.compacted = True
        return (
            "<COMPACTED_STATE>\n"
            f"<USER_GOAL>\n{goal}\n</USER_GOAL>\n"
            "<GLOBAL_LESSON_LEDGER>\n"
            "- Strip currency symbols and commas before converting salary to float.\n"
            "- Treat NULL and semicolon-delimited age values as invalid.\n"
            "- Exclude UNKNOWN salaries and compare cities case-insensitively.\n"
            "</GLOBAL_LESSON_LEDGER>\n"
            "<DEAD_ENDS>\n- Naive parsing failed on currency, NULL, and semicolon-corrupted values.\n</DEAD_ENDS>\n"
            "<CURRENT_LOCAL_PIVOT>\nContinue the next audit phase using the verified normalization rules.\n</CURRENT_LOCAL_PIVOT>\n"
            "</COMPACTED_STATE>"
        )


async def run_replay() -> SessionState:
    snapshot = ConfigRepository().resolve("test-case-3.yaml")
    if not snapshot.config.memory.hybrid_reflection:
        raise AssertionError("test-case-3.yaml must enable hybrid_reflection")
    engine = AgentEngine(snapshot.config)
    engine.llm = ReplayLlm()  # type: ignore[assignment]
    events: list[dict[str, Any]] = []

    async def emit(event: dict[str, Any]) -> None:
        events.append(event)

    state = await engine.run(engine.create_session(GOAL, snapshot.ref, snapshot.sha256), max_steps=30, emit=emit)
    if len(state.compaction_events) < 3:
        raise AssertionError(f"Expected at least 3 compactions, got {len(state.compaction_events)}")
    phase_events = [event for event in events if event.get("type") == "interim_result" and event.get("step") == "phase_completed"]
    if len(phase_events) != 3 or not any(event.get("type") == "final_result" for event in events):
        raise AssertionError("Expected one SSE-compatible interim event per phase and a final result event")
    return state


def verify_audit_environment(
    agent_output_string: str,
    current_history: list[dict[str, Any]],
    compaction_events: list[str] | None = None,
    workflow: dict[str, Any] | None = None,
    minimum_compactions: int = 3,
) -> bool:
    success = "Phase 3" in agent_output_string and "|" in agent_output_string
    legacy_events = [message for message in current_history if "SUMMARY OF PREVIOUS FAILURES" in (message.get("content") or "")]
    events = compaction_events if compaction_events is not None else legacy_events
    ledger_text = "\n".join(events)
    lessons = ("currency", "NULL", "semicolon", "UNKNOWN", "case-insensit")
    has_lessons = all(term.lower() in ledger_text.lower() for term in lessons)
    enough_compactions = len(events) >= minimum_compactions
    expected_phases = {"phase_1_corruption_audit", "phase_2_salary_median", "phase_3_city_report"}
    completed_phases = set((workflow or {}).get("completed_phases") or [])
    workflow_complete = bool(workflow) and (workflow or {}).get("active_phase") is None and expected_phases <= completed_phases
    print("✅ Success: Agent generated the final matrix report." if success else "❌ Missing Phase 3 markdown matrix.")
    print(f"📊 Integration Metric: Compaction was triggered {len(events)} times (minimum {minimum_compactions}).")
    print("✅ Hybrid reflection preserved required lessons." if has_lessons else "❌ Hybrid lesson ledger is incomplete.")
    print("✅ All configured workflow phases completed." if workflow_complete else "❌ Case 3 workflow is incomplete.")
    return success and enough_compactions and has_lessons and workflow_complete


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("session", nargs="?", type=Path, help="Saved API session JSON to verify")
    parser.add_argument("--replay", action="store_true", help="Run the deterministic Case 3 workflow, then verify it")
    parser.add_argument("--output", type=Path, default=None, help="Write replay session JSON to this path")
    args = parser.parse_args()
    if args.replay:
        state = asyncio.run(run_replay())
        document = json.dumps(state.model_dump(), indent=2)
        if args.output:
            args.output.write_text(document + "\n", encoding="utf-8")
        session = state.model_dump()
        ok = verify_audit_environment(session["final_answer"] or "", session["memory"], session["compaction_events"], session["workflow"])
        raise SystemExit(0 if ok else 1)
    if not args.session:
        raise SystemExit("Usage: python scripts/verify_test_case_3.py --replay [--output SESSION.json] | SESSION.json")
    session = json.loads(args.session.read_text(encoding="utf-8"))
    session = session.get("session", session)
    raise SystemExit(0 if verify_audit_environment(session.get("final_answer") or "", session.get("memory", []), session.get("compaction_events"), session.get("workflow")) else 1)


if __name__ == "__main__":
    main()
