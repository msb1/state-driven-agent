> Contract-1.1.0 port status: pending. The implementation described below is the earlier baseline, including its superseded compaction algorithm; it is not the upgraded Python design. Implement the [current upgrade specification](../../design_update.md), [observable contract](../../contract.md) and [shared tests](../../test-cases.md). The [frozen Python 1.0.0 baseline](../../archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md) is historical and predates the latest completion changes.

# Java architecture: Universal State-Driven AI Agent

See the shared [observable contract and conformance audit](../../contract.md) for cross-language guarantees, known differences, and the common Python HTTP test harness. Parity claims below describe intent and must be read with that audit.

The Java baseline contains the earlier state-driven loop. `AgentEngine` owns workflow evidence, compaction and bounded decisions; WebFlux and `SessionRepository` provide the API and earlier whole-snapshot persistence. Upgrade requirements are in [design_update.md](../../design_update.md).

## State and loop

The persisted `goal` and first `user` memory entry are immutable anchors. Jackson message nodes preserve ordered model history. Tool failures become tool-message content. Workflow evidence, completed phases, and transitions are persisted outside compactable memory.

Each run advances workflow transitions, checks compaction, builds the payload, asks `ModelClient` for a tool or final decision, appends the action/result, and records evidence. A final answer is accepted only when the workflow is absent or `active_phase` is null; otherwise the configured run bound produces a recoverable failure.

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

The retained unit is a message count. Payload order is system prompt, immutable goal, compacted ledger, workflow instruction, raw-buffer delimiter, then the untouched raw tail. Workflow evidence is not part of the splice and therefore survives compaction.

`memory.hybrid_reflection` defaults to `true` and is passed through `AgentEngine` to `ModelClient`. Hybrid compaction requests `COMPACTED_STATE` with `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, and `CURRENT_LOCAL_PIVOT`, while subsequent calls receive the previous ledger. `false` selects the earlier four-heading prompt and fallback. Upgraded Python instead validates proposals and preserves history on rejection.

## Validation

The user runs checks from CLI. The current [shared curl guide](../../test-cases.md) applies in full after porting: all six profiles, extended storage/recovery APIs, restart, disconnect and two-process ownership. Implement and qualify the controlled action/context/SQL/crash gates in [design_update.md](../../design_update.md) as well. Older tests/compaction-count expectations validate only the earlier baseline; they do not certify contract 1.1.0. No target tests were run during this documentation review.
