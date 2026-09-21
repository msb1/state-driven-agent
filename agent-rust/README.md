# State-Driven Agent — Rust

This standalone Tokio/Axum server exposes the shared REST and SSE paths and uses `sqlx` PostgreSQL JSONB persistence.

```sh
cd agent-rust
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo run --release
```

It listens on `PORT` (default `8082`). Configure `AGENT_CONFIG_ROOT` and `AGENT_SESSION_DATABASE_URL`. Its REST contract is [`../agent-go/openapi.yaml`](../agent-go/openapi.yaml); details are in [`docs/implementation.md`](docs/implementation.md).
