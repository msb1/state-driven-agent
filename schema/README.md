# Contract 2.0.0 / schema 6

The active definitions are `postgres.sql` (12 shared session/command/recovery tables) and `fixtures.sql` (6 fixture tables). There is no schema-5 compatibility mode and no per-language/migration table. Each target runtime must implement and advertise contract 2.0.0; setup alone does not certify it.

The explicit initializer creates absent complete table groups or validates an existing exact layout. `layout-v6.json` pins column types, defaults, nullability, constraints and indexes; the source hashes must match the reviewed DDL. Validation performs no DDL. Servers must only validate and use the initialized tables.

With all agents stopped, from the repository root:

```bash
uv run --project agent-python python agent-python/scripts/initialize_database.py --reset
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
```

Omit `--reset` for fresh initialization or repeat validation. Reset drops known agent-owned tables and obsolete `agent_schema_migrations` / `agent_go_schema_control` tables in the selected schema(s). It never drops the database/schema or uses CASCADE into unrelated objects. Incompatible layouts fail without being migrated unless reset is explicit. Use `.env` session/fixture URLs; distinct databases commit setup separately. No setup command invokes a model.

The generator preserves the original four business datasets and expected outputs, adds pinned `idempotency-vectors.json` action vectors, and preserves existing `agent_fixture_action_receipts`. The receipt row represents an atomic fixture effect/result; fixture population does not install a recipient tool in any runtime. `fixture-oracles.json` pins actual business row hashes. A routine reseed never resets session/command/action/recipient receipt history.

After setup, start the selected agent and run `./persist.sh` from the repository root. It executes the current contract-2.0.0 DU-06 C01–C12 matrix through common APIs and read-only PostgreSQL inspection. C08/C09/C12 include operator-controlled restart, a second worker and normal administrative export/pruning steps. Required failures or skipped steps prevent qualification. The obsolete 1.1.0 runner is archived under `agent-python/archive/du06-contract-1.1.0/`; it is not invoked. See `test-cases.md` section 19 for prerequisites, evidence and exit statuses.

Versioned canonical identity vectors are fixed inputs, never regenerated as expected answers by qualification. Intentional DDL changes need a reviewed new layout manifest and corresponding tests; changing a manifest to match a corrupt database is not validation. Refer to contract.md and design_update.md section 19 for semantics and remaining runtime work.
