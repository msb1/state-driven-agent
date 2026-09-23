# State-Driven Agent — Rust

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

Rust and Python are interchangeable implementations, so run only one of them
at a time. Configure
`AGENT_CONFIG_ROOT`, `AGENT_SESSION_DATABASE_URL`, and optionally
`AGENT_FIXTURE_DATABASE_URL` as needed. Swagger UI is
available at `http://127.0.0.1:8000/docs`; its OpenAPI document is served at
`/openapi.json` and is checked in as [`openapi.json`](openapi.json).
