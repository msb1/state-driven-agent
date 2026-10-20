> Contract-1.1.0 port status: pending. The implementation described below is the earlier baseline, including its superseded compaction algorithm; it is not the upgraded Python design. Implement the [current upgrade specification](../../design_update.md), [observable contract](../../contract.md) and [shared tests](../../test-cases.md). The [frozen Python 1.0.0 baseline](../../archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md) is historical and predates the latest completion changes.

# Tokio implementation

See the shared [observable contract and conformance audit](../../contract.md) for current differences, qualification requirements, and the common Python HTTP test harness.

Axum handlers are async and SQLx is Tokio-native. The current session/event tables implement the earlier whole-snapshot design and are not sufficient for Python's schema-5 repository or ownership guarantees. YAML is resolved only below the configured root and raw bytes are SHA-256 hashed at creation.

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
8000 by default. Use the [shared test guide's startup procedure](../../test-cases.md) for validation, including the root `.env` connection to the dedicated `universal_state` database. Do not rely on the earlier server's legacy-project fallback.

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

The Rust baseline implements eight earlier functional API routes: session create/read/message/run/resume, streamed run, config discovery, and health. The additional Python storage/recovery routes and extended state remain porting work. `/docs` uses Swagger UI and `/openapi.json` serves a checked-in schema snapshot generated from the Python FastAPI application. Refreshing that schema advertises a target interface; it does not implement its routes. From the repository root, the snapshot refresh command is:

```sh
uv run --project agent-python python -c 'import json; from agent_python.api import app; print(json.dumps(app.openapi(), indent=2))' > agent-rust/openapi.json
```

Rust uses PostgreSQL for session state and its event log. `AGENT_FIXTURE_DATABASE_URL` selects the database used by the deterministic tools; when unset it uses `AGENT_SESSION_DATABASE_URL`, matching Python's fallback. The OpenAI-compatible completion client supports bearer authentication, `/v1` and root route fallback, tool calls, workflow evidence, compaction, and hybrid reflection.

The Rust implementation is intended to remain behaviorally interchangeable with Python. Changes to either implementation should be checked against the same config files, database fixtures, and model endpoint, especially for SSE timing, tool output strings, and compaction behavior.
