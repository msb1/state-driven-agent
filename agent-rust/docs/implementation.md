# Tokio implementation

Axum handlers are async and SQLx is Tokio-native. The session and events tables are compatible with the Python service. YAML is resolved only below the configured root and raw bytes are SHA-256 hashed at creation.

## Build and compile

The Rust crate lives in `agent-rust/` and has its own `Cargo.toml`, lockfile,
and `rust-toolchain.toml`. The toolchain file selects Rust 1.97.1, which fixes
a macOS linker alignment issue in Rust 1.97.0 that can prevent SQLx procedural
macros from loading. Rustup selects this version automatically (and installs
it if needed).

From the repository root, the normal build-and-run command is:

```sh
cd agent-rust
cargo run
```

This uses the shared `config/` directory automatically and listens on port
8000 by default. No environment variables are needed for the standard setup.

For compile-only and build commands, run these from `agent-rust/`:

```sh
cd agent-rust
cargo check
cargo build
```

`cargo check` compiles the crate without producing an executable. `cargo build` creates the debug executable at `target/debug/agent-rust`. For an optimized release build, use:

```sh
cargo build --release
```

That creates `target/release/agent-rust`. The standard `cargo run` command
uses the debug build; to explicitly run the optimized release build with the
shared config directory:

```sh
cargo run --release
```

Optional formatting and lint checks, also run from `agent-rust/`:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

## Runtime contract

The Rust server implements the same REST paths and JSON state contract as `agent-python`, including session create/read/message/run/resume, streamed run and resume, config discovery, and health. `/docs` uses Swagger UI and `/openapi.json` serves the checked-in schema snapshot generated from the Python FastAPI application. From the repository root, refresh the snapshot after changing Python request or response models with:

```sh
uv run --project agent-python python -c 'import json; from agent_python.api import app; print(json.dumps(app.openapi(), indent=2))' > agent-rust/openapi.json
```

Rust uses PostgreSQL for session state and its event log. `AGENT_FIXTURE_DATABASE_URL` selects the database used by the deterministic tools; when unset it uses `AGENT_SESSION_DATABASE_URL`, matching Python's fallback. The OpenAI-compatible completion client supports bearer authentication, `/v1` and root route fallback, tool calls, workflow evidence, compaction, and hybrid reflection.

The Rust implementation is intended to remain behaviorally interchangeable with Python. Changes to either implementation should be checked against the same config files, database fixtures, and model endpoint, especially for SSE timing, tool output strings, and compaction behavior.
