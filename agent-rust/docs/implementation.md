# Tokio implementation

Axum handlers are async and SQLx is Tokio-native. The session and events tables are compatible with the Python service. YAML is resolved only below the configured root and raw bytes are SHA-256 hashed at creation.

This initial port deliberately has a bounded-run placeholder instead of asserting an unsupported LLM result. To complete parity, implement the OpenAI-compatible client, generic YAML workflow/evidence evaluator, compaction ledger, SQLite deterministic tools, and the five shared regression cases. The same limitation currently applies to the Go and Java baselines.
