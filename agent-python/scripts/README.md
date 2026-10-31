# Current operational scripts

These five scripts operate contract 2.0.0 / schema 6. They remain active and are not obsolete DU-06 qualification tests.

| Script | Current use |
| --- | --- |
| `initialize_database.py` | Explicit schema initialization or existing-layout validation |
| `generate_fixtures.py` | Explicit deterministic fixture generation |
| `drop_database_tables.py` | Explicit operator-requested destructive table administration |
| `reconcile_runs.py` | Inspect or reconcile abandoned lifecycle records |
| `storage_retention.py` | Plan, export and deliberately prune telemetry under runtime guards |

Shared acceptance starts at repository-root `persist.sh`; its Python harness lives in root `scripts/` and calls the common API plus read-only PostgreSQL queries. Retired 1.1.0 DU-06 tests are preserved in [archive/du06-contract-1.1.0](../archive/du06-contract-1.1.0/README.md). Operational command examples and preconditions are in [operations.md](../docs/operations.md). No operational script is run automatically to initialize/reset the database during qualification.
