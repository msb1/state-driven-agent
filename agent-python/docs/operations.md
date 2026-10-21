# Python operations and storage

[Contract 1.1.0](../../contract.md), [architecture](architecture.md), and the [implementation guide](implementation.md) describe service semantics and code boundaries. All commands are user-run from the repository root. No database/model operations were performed during consolidation.

## Fresh dedicated database

The user-created `universal_state` database is the application database. Root `.env` supplies `AGENT_SESSION_DATABASE_URL`; optional `AGENT_FIXTURE_DATABASE_URL` and `AGENT_TEST_DATABASE_URL` deliberately override fixture/test placement. Do not use a stale override from another project. Never paste credentials into reports. `information_schema` is a PostgreSQL schema containing views such as `information_schema.columns`, not a table or mocked database.

Only for a fresh database or intentional fixture reset:

```sh
uv run --project agent-python python agent-python/scripts/initialize_database.py
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
```

The first creates the current ten session/storage tables and columns using the runtime initializer. The second atomically replaces the four fixture tables' data. It is not a normal benchmark prerequisite when fixtures already exist. There is no required elite_rag migration, SQLite import or historical data preservation operation. The unused `migrate_sessions.py` wrapper is now archived. It only invoked the same initializer; use `initialize_database.py` for the supported explicit initialization operation. No active runtime/test imports that wrapper.

Install dependencies and start the Python API (foreground worker):

```sh
uv sync --project agent-python
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --host 127.0.0.1 --port 8081
```

Keep the model service and PostgreSQL running separately. Root `.env` owns `MAIN_MODEL`; no CLI model flag is required. Existing sessions retain their resolved model/configuration snapshot. Stop the foreground API with Ctrl-C. Keep PostgreSQL and the model service available during API restart validation.

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

Pruning protects transcript, context, run/execution and provenance dependencies, and retains at least 32 diagnostic entries per family. It is not a full session deletion operation. Index experiments and migration/benchmark helpers are archived; they are not required to operate the service. The runtime owns required schema/index initialization.

## Fixtures and validation

Expected fixture rows are 650 logs, 2200 customers, 6502 orders and 1000 employees. SQLite and old flat log files are archived and unused by all four current fixture implementations. Use the parent [test-cases.md](../../test-cases.md) for independent expected outputs and curl validation. The archived frozen fixture recipe/payload is available for historical comparison; curl business-output checks alone do not establish a complete row-by-row database fingerprint.
