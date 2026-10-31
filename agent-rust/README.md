# State-Driven Agent — Rust


Initialize PostgreSQL explicitly with `agent-python/scripts/initialize_database.py` from the repository root before starting any server. The common definitions live in `schema/`. Server startup validates the existing shared schema in a read-only transaction; it never creates, alters, drops, or migrates tables or columns.
This standalone Tokio/Axum server implements the `agent-python` REST and SSE contract and uses `sqlx` PostgreSQL JSONB persistence. Swagger UI is available at `/docs`; `/openapi.json` is generated from the Python API schema and checked in as [`openapi.json`](openapi.json).

From the repository root, start the server with:

```sh
cd agent-rust
cargo run
```

The crate pins Rust 1.97.1, which includes a fix for a macOS linker alignment
problem in Rust 1.97.0 that can prevent SQLx procedural macros from loading.
The shared `config/` directory is selected automatically; the server listens
on port 8000 unless `PORT` is set.

To compile or check the crate separately, run these optional commands from
`agent-rust/`:

```sh
cargo check
cargo build
cargo build --release
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

Run the Rust implementation for its CLI qualification. A second Rust worker may
use the same configuration and database for the cross-worker checks. Configure
`AGENT_CONFIG_ROOT`, `AGENT_SESSION_DATABASE_URL`, and optionally
`AGENT_FIXTURE_DATABASE_URL` as needed. Swagger UI is
available at `http://127.0.0.1:8000/docs`; its OpenAPI document is served at
`/openapi.json` and is checked in as [`openapi.json`](openapi.json).


## Contract 2.0.0 operation

The coordinated runtime targets contract 2.0.0 and schema 6. It requires a stable
`AGENT_DEPLOYMENT_NAMESPACE` shared with the other implementations and recipient.
Existing schema-5 snapshots are not migrated or accepted. See
[architecture.md](docs/architecture.md) and [implementation.md](docs/implementation.md).

Start the main worker from the repository root:

```sh
PORT=8081 cargo run --offline --manifest-path agent-rust/Cargo.toml
```

When prompted for a second worker, run the same command in another terminal with
`PORT=8082`. The user runs the unchanged shared shell scripts against these workers.

At the C12 pruning prompt in `persist.sh`, use its exact new session and archive
path in a separate terminal. From the repository root:

```sh
export SESSION_ID='the-session-id-printed-by-persist'
export SESSION_EXPORT='the-exact-export-path-printed-by-persist'

cargo run --offline --manifest-path agent-rust/Cargo.toml -- retention plan --session-id "$SESSION_ID"
cargo run --offline --manifest-path agent-rust/Cargo.toml -- retention export --session-id "$SESSION_ID" --archive "$SESSION_EXPORT"
cargo run --offline --manifest-path agent-rust/Cargo.toml -- retention prune-telemetry --session-id "$SESSION_ID" --archive "$SESSION_EXPORT" --keep-diagnostics 32 --keep-events 1
```

Wait for successful pruning before continuing `persist.sh`. Export refuses to
overwrite an existing file. `retention verify --archive PATH` verifies a file
offline. Use Rust's CLI for Rust qualification so its own administration path is
exercised. Exporting a file alone does not prune telemetry.

Explicit abandoned-run administration is available without model/tool invocation:

```sh
cargo run --offline --manifest-path agent-rust/Cargo.toml -- reconcile-runs --limit 100
```
