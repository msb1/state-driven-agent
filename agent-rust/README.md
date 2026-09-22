# State-Driven Agent — Rust

This standalone Tokio/Axum server exposes the shared REST and SSE paths and uses `sqlx` PostgreSQL JSONB persistence.

```sh
cd /Users/msb/Code/state-driven-agent
cargo fmt --check
cargo clippy --all-targets -- -D warnings
(cd agent-rust && AGENT_CONFIG_ROOT=../config PORT=8000 cargo run --release)
```

It listens on `PORT` (default `8000`). Rust and Python are interchangeable
implementations, so run only one of them at a time. Configure
`AGENT_CONFIG_ROOT` and `AGENT_SESSION_DATABASE_URL` as needed. Swagger UI is
available at `http://127.0.0.1:8000/docs`; its OpenAPI document is served at
`/openapi.json` and is the shared REST contract in
[`../agent-go/openapi.yaml`](../agent-go/openapi.yaml).
