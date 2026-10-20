> Contract-1.1.0 port status: pending. The implementation described below is the earlier baseline, including its superseded compaction algorithm; it is not the upgraded Python design. Implement the [current upgrade specification](../../design_update.md), [observable contract](../../contract.md) and [shared tests](../../test-cases.md). The [frozen Python 1.0.0 baseline](../../archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md) is historical and predates the latest completion changes.

# Go architecture: Universal State-Driven AI Agent

See the shared [observable contract and conformance audit](../../contract.md) for cross-language guarantees, known differences, and the common Python HTTP test harness. Parity claims below describe intent and must be read with that audit.

The Go baseline contains the earlier state-driven loop. `Engine` owns workflow evidence, compaction and bounded decisions; the HTTP server owns its earlier whole-snapshot persistence and SSE delivery. Upgrade requirements are in [design_update.md](../../design_update.md).

## State and loop

The immutable `Session.Goal` and the first `user` memory message are anchors. Ordered `Message` values contain model-visible history. Tool errors are converted to tool-message content, while workflow evidence and transitions live in `WorkflowState` outside compactable memory.

Each run advances eligible workflow transitions, checks compaction, builds the payload, asks the OpenAI-compatible model for a tool or final decision, appends the decision/result, and records evidence. A final answer is accepted only when no workflow exists or its active phase is `nil`; a bounded run otherwise ends as recoverable failure.

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

`raw_turns_to_keep` counts messages, not semantic turns. The system prompt, immutable goal, compacted ledger, workflow instruction, raw-buffer delimiter, and raw tail are sent in that order. Workflow evidence cannot be erased by compaction.

`hybrid_reflection` defaults to `true`. The hybrid prompt requests exactly `COMPACTED_STATE`, `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, and `CURRENT_LOCAL_PIVOT`; the previous ledger is supplied on every later compaction. If reflection is disabled, the Python-compatible four-heading prompt is used. Model failure produces the earlier deterministic fallback shape. The upgraded Python implementation preserves raw history and its previous ledger instead of installing that fallback.

## Validation

The user runs checks from CLI. The current [shared curl guide](../../test-cases.md) applies in full after porting: all six profiles, extended storage/recovery APIs, restart, disconnect and two-process ownership. Implement and qualify the controlled action/context/SQL/crash gates in [design_update.md](../../design_update.md) as well. Older tests/compaction-count expectations validate only the earlier baseline; they do not certify contract 1.1.0. No target tests were run during this documentation review.
