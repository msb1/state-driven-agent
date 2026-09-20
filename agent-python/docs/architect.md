# Python architecture: Universal State-Driven AI Agent

This document maps the repository's Python implementation to the language-agnostic state-machine design. It is intentionally free of LangChain, LangGraph, LlamaIndex, or another agent framework. The durable abstraction is the event loop and its data contracts.

## 1. Contracts and responsibilities

### Immutable configuration

`config/agent.yaml` contains the system prompt, model name, OpenAI-compatible endpoint settings, compaction thresholds, and tool schemas. `agent_python.config.load_config()` expands `${ENV_VAR:-default}` values and validates the result with Pydantic models. The prompt is never stored in mutable session memory.

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

1. Check token and step thresholds.
2. Compact the historical prefix if required.
3. Assemble system prompt, immutable goal, compacted ledger, and raw memory.
4. Ask the model for a structured tool call or final answer.
5. Append the assistant action to memory.
6. Execute the named tool through the safe boundary.
7. Append the tool output, including errors, to memory.
8. Repeat; or persist the final answer and mark the session complete.

The FastAPI layer owns session lookup and per-session locks. It does not contain agent reasoning, so the engine can also be embedded in a CLI, worker, or another HTTP service.

## 3. Phased compaction

The compactor uses three zones:

```text
[system prompt] + [immutable original goal]
                 + [structured historical ledger]
                 + [last raw turns]
```

When `max_tokens` (approximated locally by content length) or `max_steps` is reached, the engine passes the historical prefix to `LlmClient.compact()`. The newest `raw_turns_to_keep` messages remain untouched. The compactor is prompted to emit:

- Core Objective
- Universal Truths Discovered
- Dead Ends
- Current Local Pivot

If the compaction endpoint is unavailable, a deterministic fallback ledger is produced. This keeps the state machine operational while making the degraded behavior visible in context.

## 4. API surface

The FastAPI application exposes OpenAPI/Swagger documentation at `/docs`:

| Endpoint | State-machine operation |
| --- | --- |
| `POST /sessions` | Create a session and initialize memory with the user goal. |
| `POST /sessions/{id}/messages` | Append additional user input. |
| `POST /sessions/{id}/run` | Execute the loop for a bounded number of steps. |
| `GET /sessions/{id}` | Inspect current state, memory, ledger, and final status. |
| `GET /health` | Verify service availability and selected model. |

## 5. Simulated validation environments

`scripts/generate_fixtures.py` creates deterministic fixtures under `agent-python/data/`:

- `access.log` has 650 HTTP-like lines, ten malformed rows, and varied status codes. The fragile parser fails with an index error; the robust parser validates the record shape before reading the status and identifies the top offending IP.
- `commerce.sqlite3` has 2,200 customers and 6,500 orders. City casing and customer keys are intentionally inconsistent. The first join can omit rows; normalization of `lower(city)` and `ID_###` keys is required before validation.

These environments demonstrate the important behavior: a failed first attempt is not an unhandled exception. It is evidence in session memory that informs the next model action.

## 6. Portability rules

Other implementations should preserve these invariants:

1. Keep the system prompt and original goal immutable.
2. Represent memory as ordered role/content records.
3. Make tools independently callable and return serializable output.
4. Convert tool failures into context data.
5. Use structured model decisions rather than parsing prose where possible.
6. Compact only the historical prefix and retain a recent raw buffer.
7. Keep YAML tool descriptions and threshold names stable across languages.

With those invariants, a Go `[]Message`, Rust `Vec<Message>`, Java `List<Message>`, or TypeScript message array can participate in the same architecture and configuration contract.
