# Integrated Rust contract 2.0.0 implementation

The coordinated upgrade targets schema 6 and current Python behavior, including the applicable 1.1.0 workflow/configuration/context requirements and recent compaction changes. Production code has no fault-injection hooks, test callbacks, or test-specific branches. Startup never creates, alters, drops, or migrates database objects.

## Verification

Rust regressions cover fixed request/action identity vectors; JCS boundaries and strict JSON; Python-compatible integrity anchors and archives; configuration and request wire behavior; workflow/completion gates; context grouping, compaction normalization and pressure handling; stable diagnostic journals; checkpoint metadata acknowledgement; redacted provider diagnostics; and real socket completion/disconnect behavior. The deterministic 54-trial compaction workload remains part of the native suite.

The obsolete schema-5 SQL regression is preserved as non-executable historical source in `contract-1.1.0-sql-regression.rs.txt`. It created a disposable schema and bypassed durable command admission, so it is not a schema-6 regression. Current live database qualification uses the unchanged shared `persist.sh` suite.

Run native regressions from the repository root:

```sh
cargo test --manifest-path agent-rust/Cargo.toml --offline -- --test-threads=1
cargo check --manifest-path agent-rust/Cargo.toml --tests --offline
cargo fmt --manifest-path agent-rust/Cargo.toml --check
```

User-run `test-cases.sh`, `persist.sh`, and `aux-tests.sh` remain the final live validation. Native regression success and SQL planning checks do not establish that those live scripts passed. Follow the scripts' prompts for restart, the second Rust worker, and explicit pruning. See the Rust README for the administration commands.
