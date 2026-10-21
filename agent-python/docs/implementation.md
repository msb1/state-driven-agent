# Python implementation guide

This guide maps the current Python runtime to its source modules and explains how requests move through the implementation. For component and database diagrams, see [architecture.md](architecture.md). For the exact compaction algorithm and payload layout, see [compaction.md](compaction.md); for setup and administration commands, see [operations.md](operations.md).

## Start the service

From the repository root, with PostgreSQL, fixture data, and the configured model endpoint available:

```sh
uv sync --project agent-python
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --host 127.0.0.1 --port 8081
```

The application lifespan initializes/checks PostgreSQL schema and reconciles abandoned runs before serving requests. Root `.env` supplies database and model settings. Shared profiles live in `config/`; the resolved config is snapshotted at session creation so later environment or YAML changes do not rewrite an existing session. See [operations.md](operations.md) for database initialization, fixtures, recovery, pagination, and retention administration.

## Source map

| Module | Implementation role |
| --- | --- |
| `api.py` | FastAPI routes, JSON/SSE response lifecycle, authoritative state reloads, disconnect/cancel handling |
| `config.py`, `models.py` | Trusted YAML resolution, defaults and validation, config identity, API and persisted state models |
| `engine.py` | Decision loop, workflow transitions/evidence, tool policy, result artifacts, final answer and progress gates |
| `llm.py`, `model_errors.py` | OpenAI-compatible asynchronous HTTP, native tool-call normalization, usage/request diagnostics, provider error classification |
| `context.py` | Complete interaction grouping, request budget, summary ledger validation and rendering |
| `completion.py`, `validation.py` | Strict JSON/schema validation, result extraction and independent final deliverable checks |
| `transcript.py`, `storage_state.py` | Stable transcript identity/sequence, audit records, hot snapshot projection and staged diagnostic records |
| `execution.py`, `execution_store.py` | Database lease ownership, heartbeat/fencing, run lifecycle, tool intent and policy-aware recovery |
| `persistence.py`, `storage.py` | Schema coordination, atomic revision-checked commit, consistent selective hydration and paged history |
| `retention.py` | Verified JSONL archive, archive receipts, retention eligibility and guarded telemetry pruning |
| `tools.py` | Registered asynchronous PostgreSQL fixture tools and per-tool recovery policy |
| `scripts/` | Explicit database initialization, fixture generation, abandoned-run reconciliation and retention CLI |

## Request and ownership flow

Session routes validate UUID syntax and reload authoritative state from PostgreSQL. Process-local maps and locks are caches/concurrency aids; they do not establish ownership. Mutating operations claim a database lease, receive an increasing fence, and reload after claiming. Revision checks protect ordinary concurrent writes, while lease/fence checks protect long-running execution and reject stale workers.

`POST /sessions/{id}/run`, `/run/stream`, and `/resume` execute through `ExecutionCoordinator`. The coordinator persists a run identity before calling `AgentEngine`, renews ownership in the background, and provides checkpoints for lease loss and cancellation. The engine performs each decision and tool boundary; the coordinator/repository make those effects durable. The final state and run lifecycle are committed before the API exposes terminal completion. Cancellation is cooperative: already-known tool results may settle, but no later calls should be scheduled.

## Decision and tool lifecycle

`AgentEngine` owns a deep copy of the resolved configuration and verifies its identity against the session anchor. It preserves the original goal, active workflow/evidence, completion artifacts, counters, raw conversation, compacted ledger, and pending calls in `SessionState`. Before each model request it assembles the full prompt and requires the conservative context budget to fit. Workflow guidance is generated from current structured state; a summary cannot satisfy evidence or result-artifact requirements.

The LLM adapter converts internal messages into provider wire messages and removes persistence metadata. Native multi-call responses are normalized in order. Before execution, the assistant call batch and pending-call state are persisted. Each call is checked against its declared JSON schema, configured policy, workflow allow-list, and any result-bound argument values. The coordinator and execution store persist an execution identity and policy before invocation. Results are appended with their call IDs and committed with corresponding evidence/artifacts. Read-only calls may be retried, idempotent calls require an idempotent adapter, and uncertain effects require reconciliation or block safely.

Final responses pass runtime checks for nonempty content, workflow completion, forbidden content, exact/pattern/JSON presentation, and configured result artifacts. A rejected response is retained and followed by bounded corrective feedback. Step count is lifetime state; the run allowance limits additional decisions for that run. Optional no-progress policy detects repeated decisions that add no new task evidence.

## Context compaction

`context.py` measures the complete projected request, including tool declarations and protected instructions. It groups assistant calls with all corresponding tool results and only considers complete historical groups. Optional compaction follows pressure, growth, interval, spacing, and useful-reduction gates. Hard overflow protection can trigger immediately and fails safely if no affordable valid reduction is available.

Summaries are staged on a copy, validated as typed ledgers, and checked against source numeric claims and configured size. The engine evaluates the candidate ledger together with the retained raw tail as a full model request. It installs the candidate only when the request is reduced and fits the configured budget. A rejected proposal leaves the current memory and ledger intact; attempts and provenance are retained in diagnostic/transcript history. The immutable goal, structured evidence, and executed artifacts remain outside summary authority. Refer to [compaction.md](compaction.md) for exact defaults, payload order, source fingerprint, and bounded retry algorithm.

## Commit and hydration boundaries

`SessionRepository.save` stages transcript and diagnostic outboxes, validates the immutable configuration identity, and computes a commit fingerprint. In one transaction it locks the session, checks expected revision and active lease/fence, writes the hot state/config anchor, validates and appends transcript/context/diagnostics, writes run and tool-execution updates, and appends the associated event. A matching prior commit identity and fingerprint is acknowledged idempotently. Memory revision and outboxes are advanced/cleared only after commit acknowledgement.

Hydration uses `storage.HYDRATE`, a single SQL statement that reads the snapshot/config, context references, active goal/raw messages, ledger state, and bounded diagnostic tails from one database view. Historical transcript and diagnostic pages use separate cursor APIs. Compaction changes active references and preserves immutable source records; retention is a separate explicit administration path, defaulting to keep-all.

## HTTP route inventory

The current routes are declared in `api.py`:

- `GET /health`, `GET /configs`
- `POST /sessions`, `GET /sessions/{session_id}`
- `GET /sessions/{session_id}/transcript`, `POST /sessions/{session_id}/messages`
- `POST /sessions/{session_id}/run`, `POST /sessions/{session_id}/run/stream`, `POST /sessions/{session_id}/resume`, `POST /sessions/{session_id}/cancel`
- `GET /sessions/{session_id}/runs`, `/events`, `/archives`, `/executions`, `/retention`
- `GET /sessions/{session_id}/diagnostics/{family}`
- `GET /runs/abandoned`

Cursor pages include watermarks where the history family needs a stable upper bound. Ordinary session reads expose current state and bounded recent diagnostic projections rather than unbounded history.

## Checks

Use the shared [test-cases.md](../../test-cases.md) curl scenarios against a running service to verify observable behavior. This guide itself does not claim that tests or database/model checks were run.
