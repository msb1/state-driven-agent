# State-driven agent — Python

Python implements [observable contract 1.0.0](../contract.md), with immutable session configuration/goal, structured actions, evidence-gated workflows, budget-aware compaction, durable ordered history, PostgreSQL revisions/leases/fencing and recoverable execution.

From the repository root, with PostgreSQL, fixtures and the model service available:

```sh
uv sync --project agent-python
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --host 127.0.0.1 --port 8081
```

Root `.env` supplies `MAIN_MODEL`, model endpoint and database settings. Model choice is unrestricted; existing sessions retain their resolved configuration. Shared YAML profiles live in root `config/`. The dedicated PostgreSQL database is `universal_state`; SQLite is unused.

See [operations](docs/operations.md) for fresh database setup and administration, [architecture](docs/architecture.md) and [implementation](docs/implementation.md) for runtime details, [compaction](docs/compaction.md) for context handling, and parent [test-cases.md](../test-cases.md) for curl-based validation against any running language implementation.

## Directory scope

| Path | Kept because |
| --- | --- |
| `src/agent_python/` | Runtime implementation |
| `pyproject.toml`, `uv.lock`, `.python-version`, local `.venv/` | Installation, reproducibility and execution environment |
| `scripts/initialize_database.py` | Explicit current-schema creation |
| `scripts/generate_fixtures.py` | Shared PostgreSQL fixture initialization/reset |
| `scripts/reconcile_runs.py` | Abandoned-run administration |
| `scripts/storage_retention.py` | Retention planning, export and guarded telemetry cleanup |
| `docs/` and this README | Current operation and implementation guidance |

All other scripts, qualification tooling, benchmark results and milestone documents have been moved to the parent [archive](../archive/README.md). Tests are user-run from CLI; no testing/model/database calls were run as part of this cleanup. Go/Java/Rust conformance to the upgraded contract remains to be established after porting.
