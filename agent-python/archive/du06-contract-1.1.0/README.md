# Deprecated DU-06 qualification scripts

These files preserve the retired contract-1.1.0 testing approach and the superseded shared runner that carried its gates forward. They are historical evidence, not current CLI entrypoints. Some files were adapted during contract-2.0.0 development but still used the obsolete internal-stage matrix.

- `persist_c02_c03.py`: obsolete DU-05/DU-06 runner, formerly in `agent-python/tests`; it directly mutates/cleans database rows and must not be used for current qualification.
- `retired_stage_fault_test.txt`: retired trigger-based per-write-family test method, removed from the active native regression file.
- `shared-runner/persistence_qualification.py`: superseded runner snapshot, including retired internal-stage/stale-proposal pending gates.

Current qualification starts at repository-root `persist.sh` and uses the common API and read-only PostgreSQL verification scripts under root `scripts/`. No archived script is invoked by that launcher.

The five files retained in `agent-python/scripts` are current operational tools: initialization, fixture generation, explicit table-drop administration, run reconciliation, and retention administration. They are not obsolete qualification tests.
