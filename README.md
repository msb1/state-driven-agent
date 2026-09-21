# Universal State-Driven AI Agent

This repository defines a framework-independent architecture for building AI agents that remain understandable, portable, and operationally testable across Python, Go, Rust, Java, TypeScript, or any language with lists, maps, and HTTP/JSON support.

The central idea is simple: an agent is a state machine around a mutable session-memory array. The model proposes the next structured action, the environment executes that action, and the result—success or failure—is appended to memory as data. The application owns this loop; no orchestration framework is required.

## Definition

A Universal State-Driven AI Agent is a process with four explicit components:

1. **System prompt** — immutable operational policy, boundaries, output rules, and tool-use instructions.
2. **Environment and tools** — independent functions that perform local or remote work and return serializable strings or JSON. Exceptions are converted into error results at this boundary.
3. **Session memory** — an ordered list of role/content messages containing the original user goal, model actions, tool calls, tool results, and intermediate conclusions.
4. **Compactor** — a threshold-triggered operation that compresses older history into a structured context ledger while preserving the original goal and a recent raw-turn buffer.
5. **Workflow graph (optional)** — immutable, evidence-gated phases that control when an agent may complete a multi-step task.

The system prompt and original user goal are anchors. The working memory is mutable. A session selects and snapshots one trusted YAML configuration at creation time, including any workflow graph. Tool implementations are replaceable business logic, while the core agent coordinates state, HTTP/JSON model calls, compaction, and workflow transitions.

## Generic architecture

```text
                 ┌──────────────────────────────┐
                 │ Immutable session config      │
                 └──────────────┬───────────────┘
                                │
User goal ──► Initialize memory │
                                ▼
                    ┌─────────────────────────┐
                    │ Session state            │
                    │ goal + workflow + ledger │
                    └────────────┬────────────┘
                                 │
                 threshold? ─────┴───── yes ──► Compactor
                                 │ no              │
                                 ▼                 ▼
                    Assemble system + memory ◄────┘
                                 │
                                 ▼
                     OpenAI-compatible HTTP/JSON
                                 │
                  ┌──────────────┴──────────────┐
                  │                             │
             final answer                   structured tool call
                  │                             │
              return/stop                 execute environment tool
                                                │
                                                ▼
                                  append call + result to memory
                                                │
                                                └── repeat
```

Language-neutral pseudocode:

```text
memory = [{ role: "user", content: user_goal }]
while session_is_active:
    if token_count(memory) >= max_tokens or steps >= max_steps:
        ledger, recent_raw = compact(memory[:-raw_turns_to_keep], memory[-raw_turns_to_keep:])
        memory = [original_goal, ledger, recent_raw...]

    decision = llm(system_prompt + workflow_state + memory)
    if decision.kind == "final" and workflow_is_complete:
        return decision.content

    result = run_tool_safely(decision.name, decision.arguments)
    memory.append({ role: "assistant", content: decision.action })
    memory.append({ role: "tool", content: result })
    steps += 1
```

## Why this pattern is production-friendly

- **No framework lock-in:** the durable contract is ordinary data and HTTP, so the loop can be reimplemented in another language without migrating an agent framework.
- **Error isolation:** a tool failure becomes a tool message such as `ERROR: ...`; the model can inspect it, change strategy, and retry.
- **Separation of concerns:** the loop manages state and model calls; domain behavior lives in independently testable tools.
- **Observable state:** every decision, result, failure, compaction, and final answer is represented in the session history.
- **No-restart workflow onboarding:** a session resolves a trusted relative YAML config reference and snapshots its content hash; adding a new workflow YAML does not interrupt existing sessions.
- **Evidence-gated completion:** configured phase graphs retain transition evidence outside compactable raw history and never convert intermediate prose into a successful final answer.

## Memory weighting and compaction

Compaction is intentionally asymmetric. Keep the system prompt and original goal untouched, summarize only the historical prefix, and retain the newest two or three turns verbatim. The summary should use a structured lesson ledger rather than a generic paragraph:

```text
[COMPACTED CONTEXT STATE]
- Core Objective: invariant user goal
- Universal Truths Discovered: verified environment facts
- Dead Ends: failed approaches and their errors
- Current Local Pivot: the next hypothesis or action
```

This sliding splice preserves both foundational intent and high-fidelity recency. A separate reflection pass may also distill one durable lesson into an operational rule when a task benefits from explicit self-correction.

## Repository layout

| Path | Purpose |
| --- | --- |
| `config/` | Trusted immutable workflow-config repository. New YAML workflows are available to new sessions without a server restart. |
| `.env` | Shared endpoint, model, and secret defaults (keep real credentials out of source control). |
| `agent-python/` | Current FastAPI implementation using only standard data structures and `httpx`. |
| `agent-python/docs/architect.md` | Python mapping of the universal architecture and implementation decisions. |
| `agent-go/`, `agent-rust/`, `agent-java/` | Reserved sibling implementations that will use the same contracts. |

The Python implementation includes three simulated environments: a malformed HTTP log parser, a messy multi-table revenue join, and a multi-phase HR/financial compliance audit that stress-tests workflow progression and hybrid compaction. Its FastAPI service persists sessions and immutable resolved config snapshots in PostgreSQL, and can resume completed or interrupted runs through SSE endpoints with phase-level interim events. See [the test-case runbook](test-cases.md) for the exact commands, including persistence Cases 4 and 5.
