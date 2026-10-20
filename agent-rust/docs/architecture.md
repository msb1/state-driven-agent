> Contract-1.1.0 port status: pending. The implementation described below is the earlier baseline, including its superseded compaction algorithm; it is not the upgraded Python design. Implement the [current upgrade specification](../../design_update.md), [observable contract](../../contract.md) and [shared tests](../../test-cases.md). The [frozen Python 1.0.0 baseline](../../archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md) is historical and predates the latest completion changes.

# Rust architecture: Universal State-Driven AI Agent

See the shared [observable contract and conformance audit](../../contract.md) for cross-language guarantees, known differences, and the common Python HTTP test harness. Parity claims below describe intent and must be read with that audit.

The Rust baseline contains the earlier state-driven loop. `engine.rs` owns workflow evidence, compaction and bounded decisions; Axum and SQLx provide the API and earlier whole-snapshot persistence. Upgrade requirements are in [design_update.md](../../design_update.md).

## State and loop

`Session.goal` and the first `user` memory message are immutable anchors. `Vec<Message>` is ordered model-visible history. Tool failures become tool-message content. `Workflow` stores observed evidence, completed phases, and transitions outside compactable history.

Each run advances workflow transitions, checks compaction, builds the payload, asks the OpenAI-compatible endpoint for a tool or final decision, appends the result, and records evidence. A final answer is accepted only after the workflow reaches `active_phase: null`; otherwise the bounded run fails recoverably.

## Earlier compaction algorithm to replace

The current baseline uses the following older algorithm. It does not match upgraded Python; implement DU-04 in [design_update.md](../../design_update.md) and the current [compaction specification](../../agent-python/docs/compaction.md).

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

Hybrid reflection is enabled by default and requests the four XML-style sections `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, and `CURRENT_LOCAL_PIVOT` inside `COMPACTED_STATE`. The previous ledger is included in every compaction request. Setting `hybrid_reflection: false` selects the earlier four-heading prompt; unavailable model output uses the earlier deterministic fallback. Upgraded Python instead validates proposals and preserves history on rejection.

## Validation

The user runs checks from CLI. The current [shared curl guide](../../test-cases.md) applies in full after porting: all six profiles, extended storage/recovery APIs, restart, disconnect and two-process ownership. Implement and qualify the controlled action/context/SQL/crash gates in [design_update.md](../../design_update.md) as well. Older tests/compaction-count expectations validate only the earlier baseline; they do not certify contract 1.1.0. No target tests were run during this documentation review.
