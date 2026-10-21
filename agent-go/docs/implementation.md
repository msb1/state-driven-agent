# Go implementation notes

This document describes the checked-in implementation in `agent-go`. The Go port includes the current context budgeting and compaction path, normalized durable history, execution recovery, database ownership/fencing, and archive/retention operations.

## Build and run

Run from the `agent-go` directory. The repository `.env` is loaded when the server starts; already exported environment variables take precedence. Set `AGENT_CONFIG_ROOT` to the config directory (default `../config`) and `AGENT_SESSION_DATABASE_URL` to the PostgreSQL connection string. The default port is `8000`.

```sh
cd agent-go
go run ./cmd/server
```

To build a binary:

```sh
mkdir -p bin
go build -trimpath -ldflags='-s -w' -o bin/agent-go ./cmd/server
./bin/agent-go
```

The server uses `AGENT_FIXTURE_DATABASE_URL` for fixture tool queries when set; otherwise it uses the session database pool. Configuration is loaded on demand from `AGENT_CONFIG_ROOT`, and the server rejects config references that escape that directory. `/health` reports service/config-root status, `/configs` lists loadable configurations, `/docs` serves Swagger UI, and `/openapi.json` returns the embedded API schema.

## Agent loop and context handling

`Engine.Run` creates a run identity and timestamps, recovers persisted pending tool calls, and iterates while the session remains active and the configured step limit permits. Each iteration checks cancellation and ownership, advances workflow transitions, evaluates compaction, requests a model decision, and records model diagnostics. Native multi-tool-call decisions are retained and processed in order. Tool outputs are paired with their call IDs in memory and successful calls are checked against workflow evidence and configured result checks.

The context manager estimates the full projected chat-completions request, including system and workflow instructions, completion constraints, artifacts, and active memory. It can compact when hard context pressure is reached, or when configured optional interval/size/growth gates pass. History is split into complete assistant/tool interaction groups; the newest configured raw groups remain intact. A candidate ledger is checked for required structure, source-grounded content and size, and then evaluated as part of the complete projected request. It is accepted only if it reduces estimated input and fits the hard request budget (and meets configured optional reduction thresholds). Attempts and decisions are recorded as diagnostics with source hashes and message indexes. A rejected proposal leaves active history and the previous ledger intact; if the request still exceeds the hard limit after compaction attempts, the run returns a context-budget error.

Workflow phases, transitions, evidence, result artifacts, and execution argument bindings are structured state. Workflow evidence survives compaction. Final answers are checked for nonempty content, completed workflow, forbidden patterns, exact or regex presentation constraints, JSON schema constraints, and configured outputs derived from executed tool results. Rejected finals and invalid model responses are fed back into the conversation. Consecutive invalid-response and optional no-progress limits bound repeated failures.

Tool recovery modes are configured per tool: read-only operations may be retried, idempotent operations receive stable keys, and reconcilable operations query for an existing outcome. A potentially completed side effect without a safe resolution path is marked uncertain and blocks the run for explicit recovery. `/resume` first reconciles pending calls before asking the model for another decision.

## Durable session and history model

The public session document retains explicit nullable values and defaults. Messages have stable UUIDs, UTC timestamps, and a transcript sequence. Audit records share that sequence, so retained transcript sequence values can have gaps after compaction. Internal wire messages are a separate projection used to make model requests.

`StageMessages` and `StageDiagnostics` move new records into detached persistence outboxes. The save path prepares a detached proposal, computes a commit fingerprint, locks the session row, checks immutable configuration identity and lease/fence ownership, and writes the snapshot and normalized rows in one PostgreSQL transaction. The transaction includes revision and commit identity, active context references, transcript, diagnostics, run/tool-execution lifecycle, tool-call mapping, retention state, and the corresponding event. If the transaction fails, caller state and its outboxes remain available for retry; an already committed matching identity can be acknowledged idempotently.

Fetch reads the hot snapshot, immutable config, active context, active message rows, typed ledger, and bounded diagnostic projections together, validates their identities/revisions, and reconstructs only tool executions referenced by active tool-call messages. Full transcript, event, diagnostic, run, execution, archive, and retention data are served from their durable tables through separate cursor domains. Old snapshot imports make an explicit history gap rather than claiming transcript completeness.

## Ownership, cancellation, and recovery

Database leases establish the single active worker for a session. Acquiring ownership increments the fence; the worker renews the lease periodically and carries owner/fence values into saves. Engine checkpoints verify that the lease remains current and that a run has not been cancelled. A stale worker cannot commit using an expired or superseded fence. Cancellation is recorded against running lifecycles while serialized with session run state. At startup, expired running lifecycles are marked interrupted or recovered from a terminal snapshot; startup reconciliation does not replay model or tool work. Explicit resume is the recovery path.

## HTTP API and retention commands

The routes are defined in `cmd/server/main.go` and the checked-in OpenAPI documents. In addition to session create/read and message/run/stream/resume/cancel, the server exposes paged endpoints for executions, runs, events, transcript, archives, and diagnostic families, a read-only retention plan, and abandoned-run listing. Run and stream handlers persist state through the transactional store; SSE events report status, model requests, tool outcomes, compaction, completion, and errors.

Retention administration runs as a server binary subcommand:

```sh
go run ./cmd/server retention plan --session-id UUID
go run ./cmd/server retention export --session-id UUID --archive /path/session.jsonl
go run ./cmd/server retention verify --archive /path/session.jsonl
go run ./cmd/server retention prune-telemetry --session-id UUID --archive /path/session.jsonl
```

Export uses a read-only repeatable-read snapshot and creates the archive file exclusively with restrictive permissions. Verification checks canonical JSONL framing, archive and table digests, session identity, ordering, counts, and watermarks. Pruning is explicit and transactional; it requires a verified fresh archive, a matching session revision and digests, a terminal session with no active or unresolved work, and valid ownership/fencing. The default retention policy is keep-all. The HTTP retention endpoint only reports eligibility and requirements.

## Source map

- `internal/agent/config.go`: YAML config loading, defaults, validation, hashing, and identity.
- `internal/agent/engine.go`: run state machine, workflow, completion, tool execution, recovery, and compaction orchestration.
- `internal/agent/context.go`: interaction grouping, request budgeting, ledger validation, and rendering.
- `internal/agent/openai.go`: model request/response adapter and request diagnostics.
- `internal/agent/transcript.go`: session normalization, message/audit sequencing, persistence outboxes, and diagnostic projections.
- `internal/agent/recovery.go`: pluggable idempotent and reconciliation adapters.
- `internal/agent/tools_postgres.go`: compiled-in fixture database tools.
- `cmd/server/main.go`: HTTP routes, lifecycle handlers, and configuration discovery.
- `cmd/server/history_store.go`: schema, transactional commit, history hydration, and paging.
- `cmd/server/ownership.go`: leases, fencing, cancellation, and abandoned-run reconciliation.
- `cmd/server/retention.go`: verified archive and explicit telemetry pruning.
