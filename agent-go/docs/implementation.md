> Contract-1.1.0 port status: pending. The implementation described below is the earlier baseline, including its superseded compaction algorithm; it is not the upgraded Python design. Implement the [current upgrade specification](../../design_update.md), [observable contract](../../contract.md) and [shared tests](../../test-cases.md). The [frozen Python 1.0.0 baseline](../../archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md) is historical and predates the latest completion changes.

# Implementation notes

See the shared [observable contract and conformance audit](../../contract.md) for current differences and the common Python HTTP test harness. Some migration notes below are historical; the audit records where current source differs.

## Build and run

Run these commands from the `agent-go` directory. Go 1.27 or newer and access
to the PostgreSQL database are required.

The repository-level `.env` file is loaded automatically when the server
starts. Values already exported in the shell take precedence over `.env`.
The `.env` file should contain the local model endpoint, model, API key, and
session database URL. No database export is needed before starting the server:

```sh
cd agent-go
```

The repository `.env` contains defaults for `OPENAI_BASE_URL`,
`OPENAI_API_KEY`, and `MAIN_MODEL`. You can override any of them in the shell.
`AGENT_CONFIG_ROOT` defaults to `../config`, and `PORT` defaults to `8000`.

### Run directly (development)

`go run` compiles the server and starts it in one command:

```sh
go run ./cmd/server
```

### Build a reusable binary

Create the output directory, compile the server, then run the compiled binary:

```sh
mkdir -p bin
go build -trimpath -ldflags='-s -w' -o bin/agent-go ./cmd/server
./bin/agent-go
```

After changing Go source files, rebuild the binary and restart the server.

### Verify the build

Run the tests and whitespace check before handing the service to another user:

```sh
GOCACHE=/tmp/state-driven-agent-go-cache go test ./...
git diff --check
```

The `GOCACHE` setting is optional; it is useful in restricted environments
where Go's default cache directory is not writable. Once the server is
running, confirm it is serving the expected configuration:

```sh
curl -sS http://127.0.0.1:8000/health
curl -sS http://127.0.0.1:8000/configs
```

The current baseline stores whole-session/config JSONB and separate events. It does not implement Python's schema-5 state, atomic commit protocol or database ownership/fencing. Its local run/stream mutex does not protect append or other processes. Some save errors are ignored; source review must not treat event emission as proof of a successful durable write.

The checked-in server already uses the OpenAI-compatible adapter in `internal/agent/openai.go` through `execute`. It currently handles only the first native call and the earlier workflow/completion/compaction rules. The upgrade work is specified in [design_update.md](../../design_update.md); an adapter is present, but the new contract is not yet implemented.

The user runs `go test ./...` and the complete [shared curl suite](../../test-cases.md) from the CLI during the port. All six profiles and the additional storage/recovery API checks apply; compilation or older unit tests alone do not establish upgraded parity.

## DU-05 state and history

Messages have stable UUIDs, UTC timestamps and a shared transcript sequence. Audit records consume the same counter, so retained message sequences remain unchanged and can have gaps after compaction. The model adapter uses a separate `WireMessage` projection. The public `Session` retains explicit nullable fields and defaults; write outboxes and ownership fields are private.

`transcript.go` stages detached transcript and diagnostic outboxes. `history_store.go` persists transcript rows, active context references, recent diagnostic projections, immutable configuration anchors and the revision in one transaction. Active state reload uses a consistent SQL statement; `GET /sessions/{id}/transcript` and durable diagnostic pages retrieve archived records independently of recent state tails. Old snapshots are imported with an explicit history gap, not a completeness claim.

The implementation and offline commands are in [du05-validation.md](du05-validation.md). DU-05 qualification remains pending until user-run controlled, PostgreSQL and service results are recorded. Complete commit acknowledgement/fencing, execution recovery, ownership/cancellation and the full history API remain assigned to subsequent work packages.

## DU-09 selective hydration and pages

`fetch` reads the hot snapshot, immutable config, active context references, active message rows, typed ledger, and bounded diagnostic tails in one PostgreSQL statement. It validates their revisions and identities before returning active state. The hot/public snapshot omits unbounded tool execution history; the engine reconstructs only execution rows referenced by active tool-call messages, while the execution API reads durable history by UUID cursor. Transcript, events, diagnostics, runs, executions, and archive receipts use separate cursor domains and return fixed watermarks where applicable. Retention planning is read-only and defaults to keep-all; abandoned-run listing does not replay work.

The expanded API surface and the seven-cell PostgreSQL workload are documented in [du09-validation.md](du09-validation.md). That workload records logical JSON bytes and database/wall timings, checks active payload and revision stability, and logs query plans without changing production indexes. Controlled and SQL qualification remain pending until the user runs the commands against the configured PostgreSQL service.

## DU-10 verified archives and explicit telemetry retention

The default remains keep-all. `retention.go` exports canonical `sda-archive-1` JSONL from a read-only repeatable-read snapshot, verifies archive framing, table and row order, session identity, counts, watermarks, and SHA-256 digests, and supports explicit terminal-session telemetry pruning. The prune path requires a fresh matching revision, current database lease/fence, no active or unresolved execution, intact earlier archive dependencies, and matching event/diagnostic row digests. Receipt, state/event update, and telemetry deletes are part of one transaction. The service exposes only the read-only retention plan; Go admin subcommands perform explicit archive export and prune outside HTTP.

See [du10-validation.md](du10-validation.md) for C01–C05, the export workload grid, shared CLI admin commands, and regression/full-suite commands. Qualification against the configured PostgreSQL database and live service remains pending until the user runs those commands.
