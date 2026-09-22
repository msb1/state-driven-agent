# Go architecture: Universal State-Driven AI Agent

`agent-go` implements the same framework-free state machine as `agent-python`. `Engine` owns the portable session contract, workflow evidence, compaction, and the bounded decision loop; the HTTP server owns persistence and SSE delivery.

## State and loop

The immutable `Session.Goal` and the first `user` memory message are anchors. Ordered `Message` values contain model-visible history. Tool errors are converted to tool-message content, while workflow evidence and transitions live in `WorkflowState` outside compactable memory.

Each run advances eligible workflow transitions, checks compaction, builds the payload, asks the OpenAI-compatible model for a tool or final decision, appends the decision/result, and records evidence. A final answer is accepted only when no workflow exists or its active phase is `nil`; a bounded run otherwise ends as recoverable failure.

## Exact compaction contract

The Go implementation is validated against `agent-python` with these invariant rules:

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

`raw_turns_to_keep` counts messages, not semantic turns. The system prompt, immutable goal, compacted ledger, workflow instruction, raw-buffer delimiter, and raw tail are sent in that order. Workflow evidence cannot be erased by compaction.

`hybrid_reflection` defaults to `true`. The hybrid prompt requests exactly `COMPACTED_STATE`, `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, and `CURRENT_LOCAL_PIVOT`; the previous ledger is supplied on every later compaction. If reflection is disabled, the Python-compatible four-heading prompt is used. Model failure produces the same deterministic fallback shape as Python.

## Validation

`go test ./...` exercises workflow progression, raw-tail retention, durable JSON state, and compaction. The test fixture asserts that hybrid reflection is enabled and that compaction happens before model decisions when the threshold is due. The implementation also uses the shared YAML names `max_tokens`, `max_steps`, `raw_turns_to_keep`, and `hybrid_reflection`.

Portability invariant: changes to trigger conditions, slice boundaries, fallback text, or reflection mode must be compared with `agent-python/src/agent_python/engine.py` and `llm.py`, then mirrored in Rust and Java.
