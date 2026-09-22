# Rust architecture: Universal State-Driven AI Agent

`agent-rust` implements the same framework-free state machine as `agent-python`. `engine.rs` owns the portable session contract, workflow evidence, compaction, and bounded decision loop; Axum and SQLx provide the API and persistence boundary.

## State and loop

`Session.goal` and the first `user` memory message are immutable anchors. `Vec<Message>` is ordered model-visible history. Tool failures become tool-message content. `Workflow` stores observed evidence, completed phases, and transitions outside compactable history.

Each run advances workflow transitions, checks compaction, builds the payload, asks the OpenAI-compatible endpoint for a tool or final decision, appends the result, and records evidence. A final answer is accepted only after the workflow reaches `active_phase: null`; otherwise the bounded run fails recoverably.

## Exact compaction contract

Rust is validated against `agent-python` with the same rules:

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

`raw_turns_to_keep` is a message count. The system prompt, immutable goal, ledger, workflow instruction, raw-buffer delimiter, and untouched raw tail are sent in that order. Workflow evidence remains durable even when old tool messages are removed.

Hybrid reflection is enabled by default and requests the four XML-style sections `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, and `CURRENT_LOCAL_PIVOT` inside `COMPACTED_STATE`. The previous ledger is included in every compaction request. Setting `hybrid_reflection: false` selects Python’s four-heading fallback prompt; unavailable model output uses the corresponding deterministic fallback.

## Validation

`cargo test` and `cargo check` validate compilation and the portable state model. Case 3 replay validation must confirm the same trigger points, at least three compactions, preserved raw tail, hybrid sections, original goal, and workflow completion as the Python replay. Any change to thresholds, slice boundaries, prompt mode, or fallback behavior must be checked against Python first and mirrored in Go and Java.
