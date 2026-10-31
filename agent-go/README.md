# State-Driven Agent — Go


Initialize PostgreSQL explicitly with `agent-python/scripts/initialize_database.py` from the repository root before starting any server. The common definitions live in `schema/`. Server startup validates the existing shared schema in a read-only transaction; it never creates, alters, drops, or migrates tables or columns.
Standalone Go HTTP service implementing the shared REST, JSON session document, PostgreSQL event ledger, and SSE lifecycle contract. It uses `net/http` (Go's concurrent production HTTP server), `pgx` pooling, and YAML snapshots.

## Build and run

```sh
cd agent-go
go mod tidy
AGENT_CONFIG_ROOT=../config AGENT_SESSION_DATABASE_URL='postgresql://…' go run ./cmd/server
go build -trimpath -ldflags='-s -w' -o bin/agent-go ./cmd/server
```

It listens on `PORT` (default `8000`). Swagger UI is served at `/docs` and its OpenAPI document at `/openapi.json`; the checked-in contract is [`openapi.yaml`](openapi.yaml). Persistence/SSE invariants are documented in [`docs/implementation.md`](docs/implementation.md).

The server deliberately does not expose config paths outside `AGENT_CONFIG_ROOT`; `config_ref` remains relative and is hashed at session creation.

## Contract 2.0.0 runtime upgrade

The runtime targets the existing shared schema 6. Set a stable `AGENT_DEPLOYMENT_NAMESPACE` shared with the other implementations. Startup performs read-only schema checks and does not provision tables. All mutating HTTP requests require `Idempotency-Key`; retry the same logical request with the same key. `/commands/{command_id}` exposes durable command status and stored responses. Cancellation requires an explicit `run_id`. Command leases, atomic terminal responses, per-commit receipts, frozen SHA-256 action keys/fingerprints and action-attempt records replace the schema-5 identities. Unresolved effects require recorded recovery policy; changing keys does not authorize repeating an effect.

Retention exports use `sda-archive-2` and include command, commit and attempt evidence. The existing `retention` CLI remains the administrative entrypoint; use the exact C12 session and path displayed by `persist.sh`, export first, then `prune-telemetry --keep-events 1 --keep-diagnostics 32`. The shared harness does not invoke Go internals.

When `persist.sh` pauses for C12, run these commands from the repository root in a separate terminal, with `SESSION_ID` and `SESSION_EXPORT` set to the values printed by that run:

```sh
go -C agent-go run ./cmd/server retention plan --session-id "$SESSION_ID"
go -C agent-go run ./cmd/server retention export --session-id "$SESSION_ID" --archive "$SESSION_EXPORT"
go -C agent-go run ./cmd/server retention prune-telemetry --session-id "$SESSION_ID" --archive "$SESSION_EXPORT" --keep-diagnostics 32 --keep-events 1
```

Use Go's retention CLI for Go-created sessions. Python's `storage_retention.py` loads snapshots through Python's integrity validator and cannot currently administer Go snapshots. A shared schema and archive format do not yet provide interchangeable snapshot readers. An export alone does not prune records; wait for the successful prune result before continuing `persist.sh`.

Compaction accepts complete fenced XML and nested text markup, canonicalizes reflection sections and restores the immutable goal in code. Incomplete or truncated responses, missing sections, unsupported numbers and byte-budget violations remain rejected. See the shared [porting requirements](../agent-python/docs/compaction.md#2026-10-29-normalization-update--porting-requirements). Workflow requests use automatic tool choice; phase constraints are enforced in the engine rather than forced provider flags.

Live contract-2.0.0 qualification remains pending the user-run `test-cases.sh`, `persist.sh`, and `aux-tests.sh`. Earlier 1.1.0 results do not establish schema-6 qualification. Go request goldens are regenerated from current Python with `internal/agent/testdata/regenerate_python_parity.py`; obsolete schema-5 repository tests are retained under `archive/contract-1.1.0-tests/`. Current native regressions cover normalized admission, replay semantics, fixed identity vectors, recovery and compaction. Shared user-run shell qualification remains required.
