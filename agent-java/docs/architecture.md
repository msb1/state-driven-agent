# Java architecture: Universal State-Driven AI Agent

`agent-java` implements the same framework-free state machine as `agent-python`. `AgentEngine` owns session transitions, workflow evidence, compaction, and bounded execution; Spring WebFlux owns HTTP/SSE orchestration and `SessionRepository` owns persistence.

## State and loop

The persisted `goal` and first `user` memory entry are immutable anchors. Jackson message nodes preserve ordered model history. Tool failures become tool-message content. Workflow evidence, completed phases, and transitions are persisted outside compactable memory.

Each run advances workflow transitions, checks compaction, builds the payload, asks `ModelClient` for a tool or final decision, appends the action/result, and records evidence. A final answer is accepted only when the workflow is absent or `active_phase` is null; otherwise the configured run bound produces a recoverable failure.

## Exact compaction contract

Java is validated against `agent-python` with the same rules:

```text
approximate_tokens = sum(len(message.content or "") // 4 + 1 for message in memory)
due = approximate_tokens >= memory.max_tokens
      OR step_count >= memory.max_steps
if due AND len(memory) > raw_turns_to_keep + 1:
    historical = memory[1:-raw_turns_to_keep]
    raw = memory[-raw_turns_to_keep:]
    ledger = compact(goal, historical, prior_ledger, hybrid_reflection)
    memory = [memory[0], *raw]
```

The retained unit is a message count. Payload order is system prompt, immutable goal, compacted ledger, workflow instruction, raw-buffer delimiter, then the untouched raw tail. Workflow evidence is not part of the splice and therefore survives compaction.

`memory.hybrid_reflection` defaults to `true` and is passed through `AgentEngine` to `ModelClient`. Hybrid compaction requests `COMPACTED_STATE` with `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, and `CURRENT_LOCAL_PIVOT`, while subsequent calls receive the previous ledger. `false` selects the same four-heading prompt as Python, with a matching deterministic fallback.

## Validation

`mvn test` validates Java compilation/tests and the persisted state boundary. The Case 3 replay contract must match Python for threshold and step-triggered compaction, raw-tail retention, hybrid reflection, original-goal preservation, and workflow completion. Changes to the trigger, splice, prompt, or fallback must be compared with Python and mirrored in Go and Rust.
