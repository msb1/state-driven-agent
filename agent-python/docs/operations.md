# Python operations and storage

[Contract 2.0.0 / schema 6](../../contract.md), [architecture](architecture.md), and the [implementation guide](implementation.md) describe service semantics and code boundaries. All commands are user-run from the repository root. No database/model operations were performed during consolidation.

## Fresh dedicated database

The user-created `universal_state` database is the application database. Root `.env` supplies `AGENT_SESSION_DATABASE_URL`; optional `AGENT_FIXTURE_DATABASE_URL` and `AGENT_TEST_DATABASE_URL` deliberately override fixture/test placement. Do not use a stale override from another project. Never paste credentials into reports. `information_schema` is a PostgreSQL schema containing views such as `information_schema.columns`, not a table or mocked database.

Only for a fresh database or intentional fixture reset:

```sh
uv run --project agent-python python agent-python/scripts/initialize_database.py
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
```

The initializer creates nine shared session/history tables and four fixture tables from `schema/postgres.sql` and `schema/fixtures.sql`. It validates existing tables without changing them and refuses incompatible schemas. The fixture generator atomically replaces fixture rows without modifying tables or columns. Python, Go, and Rust servers perform read-only schema validation and never create, alter, drop, or migrate schema objects.

Install dependencies and start the Python API (foreground worker):

```sh
uv sync --project agent-python
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --host 127.0.0.1 --port 8081
```

Keep the model service and PostgreSQL running separately. Root `.env` owns `MAIN_MODEL`; no CLI model flag is required. Existing sessions retain their resolved model/configuration snapshot. Stop the foreground API with Ctrl-C. Keep PostgreSQL and the model service available during API restart validation.

Provider HTTP failures emit a bounded `model_provider_failure` JSON record to the server log. It can include allow-listed provider error type/code/parameter, a redacted message and provider request ID; the raw provider body, request payload and headers are never logged or returned by public session/API/SSE diagnostics. Restrict and retain server logs as operator data. See [OBS-01](../../contract.md#provider-error-visibility--obs-01).

## State, ownership and pagination

Session ID, owner ID, run ID, tool idempotency key, configuration hash and fence are different identities. Ownership columns are `lease_owner`, `lease_fence`, `lease_expires_at` in `agent_sessions`. They are internal concurrency controls, not an authentication/session-token feature. Inspect the intended database/schema with your SQL client; table creation occurs in the connection's current schema/search path.

Set `AGENT_URL=http://127.0.0.1:8081` and use a real UUID from a created session:

```sh
curl -fsS "$AGENT_URL/sessions/$SESSION_ID"
curl -fsS "$AGENT_URL/sessions/$SESSION_ID/transcript?limit=100&after_sequence=0"
curl -fsS "$AGENT_URL/sessions/$SESSION_ID/events?limit=100&after=0"
curl -fsS "$AGENT_URL/sessions/$SESSION_ID/diagnostics/compaction_attempts?limit=100&after=0"
curl -fsS "$AGENT_URL/sessions/$SESSION_ID/runs?limit=100"
curl -fsS "$AGENT_URL/sessions/$SESSION_ID/executions?limit=100"
curl -fsS "$AGENT_URL/sessions/$SESSION_ID/retention"
```

Reuse returned `through_sequence` / `through` while advancing append-only cursors. Runs/executions have UUID cursors and current-record semantics per page. Default GET returns active context and at most 32 diagnostics per family, not every historical row. No old `is_compacted` message-table query is used.

## Abandoned work

```sh
uv run --project agent-python python agent-python/scripts/reconcile_runs.py
```

Default lists abandoned runs after coordinated schema readiness; `--apply` marks abandoned lifecycle records interrupted without executing user tools/models. Explicit `/resume` performs policy-aware reconciliation/retry. Do not clear leases manually to bypass fencing or replay uncertain external effects without their adapter's recovery policy.

## Retention: explicit administration only

Keep-all is the default. These operations are optional administration, not routine validation. The following uses a real terminal session created by the parent curl guide (run its setup and scenarios first):

```sh
SESSION_ID=$(jq -er '.id' "$OUT/03-state.json")
mkdir -p "$PWD/archive/session-exports"
SESSION_EXPORT="$PWD/archive/session-exports/${SESSION_ID}-$(date +%Y%m%d-%H%M%S).jsonl"
uv run --project agent-python python agent-python/scripts/storage_retention.py plan \
  --session-id "$SESSION_ID"
uv run --project agent-python python agent-python/scripts/storage_retention.py export \
  --session-id "$SESSION_ID" --archive "$SESSION_EXPORT"
```

Export requires a new destination with an existing parent directory. An export contains session context; store it appropriately. Only if deliberately removing exported historical telemetry, with the same verified export still present:

```sh
uv run --project agent-python python agent-python/scripts/storage_retention.py prune-telemetry \
  --session-id "$SESSION_ID" --archive "$SESSION_EXPORT" \
  --keep-diagnostics 32 --keep-events 100
```

For the shared DU-06 C12 checkpoint, substitute the runner's displayed session ID and export path in these deployment administration commands, and use `--keep-events 1` for pruning so that actual event removal is exercised. The shared harness verifies the resulting database records/archive; it does not invoke Python internals or choose an implementation.

Pruning protects transcript, context, run/execution and provenance dependencies, and retains at least 32 diagnostic entries per family. It is not a full session deletion operation. Index experiments and migration/benchmark helpers are archived; they are not required to operate the service. Only the explicit initializer creates the shared tables and indexes; runtime schema changes are prohibited.

## Fixtures and validation

Expected fixture rows are 650 logs, 2200 customers, 6502 orders and 1000 employees. SQLite and old flat log files are archived and unused by all four current fixture implementations. Use the parent [test-cases.md](../../test-cases.md) for independent expected outputs and curl validation. The archived frozen fixture recipe/payload is available for historical comparison; curl business-output checks alone do not establish a complete row-by-row database fingerprint.

## Contract 2.0.0 request/recovery operations

Keep `AGENT_DEPLOYMENT_NAMESPACE` stable across every worker and language that shares this ledger. Development configuration now supplies `state-driven-agent-development`. A worker/process/run ID must not replace this namespace. New API intents receive new client keys; a retry keeps its original key and payload. `GET /commands/{command_id}` reports the original command; `GET /sessions/{id}` reports current state. Responses expose `Idempotency-Command-Id`; terminal duplicate responses also expose `Idempotency-Replayed:true`.

All mutating POSTs require `Idempotency-Key`. Cancel bodies are `{"run_id":"<the originally targeted run UUID>"}`. Resume is a new intentional request/key/run; recovery retains old pending execution IDs and action keys. Read-only reconnection uses `GET /sessions/{id}/events/stream?run_id=...&after_event_id=...`; it neither invokes nor resumes work. The first stream POST retains the existing socket-disconnect cancellation rule.

Command rows store `{input, admission}`: the normalized request is fingerprinted independently, while admission preserves the initial configuration and creation state/message IDs. Dynamic YAML/environment edits cannot alter an admitted creation when its key is replayed. Synchronous original responses and terminal run outcomes commit with their protected state. Blocked/uncertain actions remain blocked/uncertain commands rather than cached definitive failures.

Archive format is `sda-archive-2`. Command/commit/action-attempt evidence is included in exports and protected from telemetry pruning. Cancellation is a metadata-only operation: its target flag and command outcome commit together without advancing the hot session revision underneath the running owner.

The shell clients retain keys under their evidence directory. They have been adapted to the new API but are reserved for user-run final validation. A full `persist.sh` result cannot pass while its shared fault/process/lifecycle checks remain pending; use `--setup-only` and `--api-only` for their explicitly scoped checks.

### C12 export-path checkpoint

When `persist.sh` pauses at C12, use the **current prompt's** session UUID and absolute export path in a separate terminal at the repository root. Set `SESSION_ID` and `SESSION_EXPORT` to those displayed values; do not reuse a prior run's path. Run `plan`, then `export`, then `prune-telemetry` as shown above with `--keep-diagnostics 32 --keep-events 1`. Wait for each command to succeed. Keep the export at the exact displayed path and press Enter in the persistence terminal only after pruning succeeds. The harness checks that file against its pre-pruning snapshot. A successful prune alone does not pass C12 if the export is at another path. A fresh `persist.sh` run creates new sessions and does not require rerunning `test-cases.sh` or deleting old data.
