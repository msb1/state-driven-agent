# Universal state-driven agent

Python, Go, Java and Rust implementations demonstrate the same language-independent agent architecture. Python is the upgraded runtime reference; full contract-1.1.0 parity for the other implementations remains to be established after porting.

| Document | Purpose |
| --- | --- |
| [contract.md](contract.md) | Required observable behavior, persistence and recovery contract |
| [design_update.md](design_update.md) | Source-reviewed upgrade specification and per-language work packages for Go, Rust and Java |
| [test-cases.md](test-cases.md) | Shared curl validation against any running implementation |
| [Python architecture](agent-python/docs/architect.md) | Implemented service, schema, transactions, ownership and recovery |
| [Compaction](agent-python/docs/compaction.md) | Implemented context/budget/summary/provenance algorithms |
| [Operations](agent-python/docs/operations.md) | Python startup, dedicated PostgreSQL setup and administration |
| [answers.md](answers.md) | Design answers and implementation/porting plan |
| [digest.md](digest.md) | Reviewed qualification history and limits |
| [archive/](archive/README.md) | All development scripts, milestone notes, frozen reference and historical results |

Root `config/` contains the six shared YAML profiles. Root `.env` controls MAIN_MODEL and endpoints/database settings; model choice remains editable. Python uses the dedicated `universal_state` PostgreSQL database for sessions and fixtures. Current fixture tools in all four languages use PostgreSQL; old SQLite/log artifacts are archived.

The Python application directory contains only runtime/dependency files, four operational scripts and current operation/implementation docs. Shared validation lives in the parent test guide; new curl evidence is written under `archive/curl-validation/`. Historical benchmark scripts are preserved but are not active commands or runtime dependencies. All testing is user-run from CLI.
