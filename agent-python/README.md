# State-driven agent — Python

Python implements [observable contract 2.0.0 / schema 6](../contract.md), with immutable session configuration/goal, structured actions, evidence-gated workflows, budget-aware compaction, durable ordered history, PostgreSQL revisions/leases/fencing and recoverable execution.

From the repository root, with PostgreSQL, fixtures and the model service available:

```sh
uv sync --project agent-python
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --host 127.0.0.1 --port 8081
```

Root `.env` supplies `MAIN_MODEL`, model endpoint, database settings and the stable `AGENT_DEPLOYMENT_NAMESPACE`. Every mutating request requires a retained `Idempotency-Key`; cancellation also names its target `run_id`. Model choice is unrestricted; existing sessions retain their resolved configuration. Shared YAML profiles live in root `config/`. The dedicated PostgreSQL database is `universal_state`; SQLite is unused.

See [operations](docs/operations.md) for fresh database setup and administration, [architecture](docs/architecture.md) and [implementation](docs/implementation.md) for runtime details, [compaction](docs/compaction.md) for context handling, and parent [test-cases.md](../test-cases.md) for curl-based validation against any running language implementation.

## Directory scope

| Path | Kept because |
| --- | --- |
| `src/agent_python/` | Runtime implementation |
| `pyproject.toml`, `uv.lock`, `.python-version`, local `.venv/` | Installation, reproducibility and execution environment |
| `scripts/initialize_database.py` | Explicit current-schema creation |
| `scripts/generate_fixtures.py` | Shared PostgreSQL fixture initialization/reset |
| `scripts/drop_database_tables.py` | Explicit operator-requested table-drop administration |
| `scripts/reconcile_runs.py` | Abandoned-run administration |
| `scripts/storage_retention.py` | Retention planning, export and guarded telemetry cleanup |
| `docs/` and this README | Current operation and implementation guidance |

Historical scripts, benchmarks and milestone documents are preserved in the parent [archive](../archive/README.md) and the local [retired DU-06 archive](archive/du06-contract-1.1.0/README.md). Current shared qualification tooling lives under repository-root `scripts/`. Tests are user-run from CLI; no testing/model/database calls were run as part of this cleanup. Go/Java/Rust conformance to the upgraded contract remains to be established after porting.

## Contract 2.0.0 runtime update

Command admission and leases, original JSON-response replay, historical commit receipts, deterministic action keys, frozen recipient capabilities, dispatch/verification attempts, uncertainty/blocking, startup recovery and read-only event streaming are implemented. The runtime uses `identity.py` and `schema_validation.py`; the language-independent qualification harness has separate pinned-schema/RFC8785 oracles. `commands.py` owns request identity; `ExecutionCoordinator` continues to own session execution. No production fault hooks were added.

The new `idempotency-fixture.yaml` profile uses the real controlled receipt/effect table through a keyed adapter and authoritative verifier. The existing business fixture tools remain read-only. This fixture is not a guarantee about arbitrary external services.

Focused regressions use disposable PostgreSQL schemas and controlled models. Final user-run business/CLI qualification remains pending. `test-cases.sh` and `aux-tests.sh` now record request keys before dispatch; their assertions remain intact. `./persist.sh` runs the twelve contract-2.0.0 DU-06 checks through the common API and PostgreSQL ledger, with no language selector. It prompts for restart, second-worker readiness and approved telemetry export/pruning. Those operator steps must be completed for a full pass. The superseded persistence runner is preserved in `archive/du06-contract-1.1.0/`; all five Python files retained in `scripts/` are active operational tools. Native regression results are not relabeled as shared qualification.
