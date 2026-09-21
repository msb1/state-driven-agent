# Python architecture: Universal State-Driven AI Agent

This document maps the repository's Python implementation to the language-agnostic state-machine design. It is intentionally free of LangChain, LangGraph, LlamaIndex, or another agent framework. The durable abstraction is the event loop and its data contracts.

## 1. Contracts and responsibilities

### Immutable configuration repository

`ConfigRepository` resolves a caller's relative `config_ref` beneath the server-owned `config/` root (or the administrator-set `AGENT_CONFIG_ROOT`). It rejects absolute paths, traversal outside the root, non-YAML files, and symlink escapes. The file is expanded, Pydantic-validated, and SHA-256 hashed at session creation.

The API creates one `AgentEngine` from that immutable snapshot and stores the reference/hash in `SessionState`. PostgreSQL also stores the fully resolved `AgentConfig` JSON, so an API restart does not need to re-read a possibly changed YAML file before resuming. Adding a new versioned YAML workflow is immediately available to new sessions without restarting the server. Config YAML may only name tools already compiled into the server; new executable tool code is a deployment concern, not a session input.

### Session memory

`Message` is the portable message shape:

```python
{"role": "user" | "system" | "assistant" | "tool", "content": str}
```

`AgentEngine.create_session(goal)` initializes the anchor explicitly:

```python
SessionState(
    id="...",
    goal=goal,
    config_ref="test-case-3.yaml",
    config_sha256="...",
    memory=[Message(role="user", content=goal)],
)
```

The `goal` field is retained separately so compaction cannot accidentally erase it. `POST /sessions/{id}/messages` appends later user input; it does not mutate the original goal.

### Environment and tools

Tools in `agent_python.tools` are independent async functions with a common contract: accept a JSON-compatible argument mapping and return a string. `execute()` is the fault boundary. Unknown tools and exceptions become strings beginning with `ERROR:` and are appended to session memory. A tool cannot crash the agent's API loop.

The configured tool schemas are sent to the model as OpenAI-compatible function definitions. Descriptions explain the purpose of each function, and `parameters` declares the slots the model must fill.

### Model adapter

`agent_python.llm.LlmClient` uses `httpx.AsyncClient` against `/chat/completions`. It accepts any local or hosted OpenAI-compatible endpoint. A response is normalized to one of two decisions:

```text
{type: "tool", id, name, arguments}
{type: "final", content}
```

The core engine is therefore independent of a vendor SDK and can be ported using the same JSON payload.

## 2. State transition loop

`AgentEngine.run()` performs the following transition until completion or a step limit:

1. Advance one configured workflow edge if the current phase has all required evidence.
2. Check token and step thresholds.
3. Compact the historical prefix if required.
4. Assemble system prompt, immutable goal, workflow state, compacted ledger, and raw memory.
5. Ask the model for a structured tool call or final answer.
6. If workflow phases remain, retain prose-only output but continue rather than marking completion.
7. Execute a tool through the safe boundary and record matching phase evidence outside compactable history.
8. Mark complete only when the terminal workflow node has been reached and the model returns a final answer; otherwise the finite step limit marks failure.

The FastAPI layer owns session lookup, PostgreSQL writes, per-process locks, and SSE conversion. The engine emits structured milestones (`tool_completed`, `phase_completed`, `compaction`, and `final_result`) through an optional callback. Every workflow phase transition produces an interim event; setup status events are additionally enabled with `output.verbose_setup: true`.

## 3. Optional workflow graphs

An agent configuration may omit `workflow`, preserving the original single-task behavior, or declare an explicit phase graph. A phase has durable tool-evidence requirements and transitions to another phase or the terminal `complete` node. The current YAML configurations use linear graphs, while transition types reserve `evidence` and `always` edges for future branching or skipping.

Workflow evidence is recorded in `SessionState.workflow`, outside compactable message history. This is essential: a compaction may remove an old raw tool result, but it cannot erase proof that a phase was completed. Evidence entries may be `required: false` diagnostics; only required entries gate transitions. While a phase is active, the payload names the phase, its instruction, and missing required evidence. An optional `allowed_tools` list on a phase rejects out-of-phase tool calls with a workflow error, preventing later-phase tools from being used prematurely. A prose-only model response is retained as history but cannot complete the session. Only reaching the terminal workflow node permits a final answer; otherwise the normal bounded step limit marks the session failed.

## 4. Phased compaction

The compactor uses three zones:

```text
[system prompt] + [immutable original goal]
                 + [structured historical ledger]
                 + [last raw turns]
```

When `max_tokens` (approximated locally by content length) or `max_steps` is reached, the engine passes the historical prefix to `LlmClient.compact()`. The newest `raw_turns_to_keep` messages remain untouched. With `memory.hybrid_reflection: true` (the default), the compactor is prompted to emit XML-style sections for:

- `USER_GOAL`
- `GLOBAL_LESSON_LEDGER`, expressed as imperative operational rules
- `DEAD_ENDS`
- `CURRENT_LOCAL_PIVOT`

The raw working buffer is retained by the engine, not copied by the reflector. A delimiter marks the raw buffer in the next decision payload, while the retained messages remain verbatim. If `hybrid_reflection` is set to `false`, the older four-heading compaction prompt is used while phased raw-buffer retention remains active. If the compaction endpoint is unavailable, a deterministic fallback ledger is produced. This keeps the state machine operational while making degraded behavior visible in context.

## 5. API surface

The FastAPI application exposes OpenAPI/Swagger documentation at `/docs`:

| Endpoint | State-machine operation |
| --- | --- |
| `GET /configs` | List safe metadata for YAML configurations currently available in the trusted repository. |
| `POST /sessions` | Resolve `config_ref`, snapshot/hash it, create the corresponding engine, and initialize memory with the user goal. |
| `POST /sessions/{id}/messages` | Append additional user input. |
| `POST /sessions/{id}/run` | Execute the loop for a bounded number of steps. |
| `POST /sessions/{id}/run/stream` | Execute an active session as `text/event-stream`, flushing persisted phase and tool milestones. |
| `POST /sessions/{id}/resume` | Restore durable state/config, reactivate a complete or failed session, optionally append `content`, then compact and stream another run. |
| `GET /sessions/{id}` | Inspect current state, memory, ledger, workflow phase/evidence/transitions, and final status. |
| `GET /health` | Verify service availability and selected model. |

## 6. Simulated validation environments

`scripts/generate_fixtures.py` creates deterministic fixtures under `agent-python/data/`:

- `access.log` has 650 HTTP-like lines, ten malformed rows, and varied status codes. The fragile parser fails with an index error; the robust parser validates the record shape before reading the status and identifies the top offending IP.
- `commerce.sqlite3` has 2,200 customers and 6,500 orders. City casing and customer keys are intentionally inconsistent. The first join can omit rows; normalization of `lower(city)` and `ID_###` keys is required before validation. It also contains the 1,000-row Case 3 `employees` fixture with layered age, salary, and city corruption.

These environments demonstrate the important behavior: a failed first attempt is not an unhandled exception. It is evidence in session memory that informs the next model action.

## 7. Portability rules

Other implementations should preserve these invariants:

1. Keep the system prompt and original goal immutable.
2. Represent memory as ordered role/content records.
3. Make tools independently callable and return serializable output.
4. Convert tool failures into context data.
5. Use structured model decisions rather than parsing prose where possible.
6. Compact only the historical prefix and retain a recent raw buffer.
7. Resolve a trusted relative config reference at session creation, then persist its content hash.
8. Keep YAML tool descriptions, workflow graph semantics, and threshold names stable across languages.
9. When configured, preserve workflow phase state, observed tool evidence, and transition history outside compactable raw memory.

With those invariants, a Go `[]Message`, Rust `Vec<Message>`, Java `List<Message>`, or TypeScript message array can participate in the same architecture and configuration contract.
