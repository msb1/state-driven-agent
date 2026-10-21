# Python documentation

These are the current operation and implementation guides.

| Document | Purpose |
| --- | --- |
| [architecture.md](architecture.md) | Runtime modules, lifecycle and persistence diagrams, schema, ownership and transaction boundaries |
| [implementation.md](implementation.md) | Source map, execution details, commit/hydration boundaries and HTTP routes |
| [compaction.md](compaction.md) | Context assembly, budget protection, compaction algorithm and provenance |
| [operations.md](operations.md) | Installation/startup, PostgreSQL initialization, inspection, recovery and retention |

Use the parent [test-cases.md](../../test-cases.md) for the same curl validation scenarios against Python, Go, Java or Rust. [contract.md](../../contract.md) defines the observable behavior required of a port. Architecture and compaction describe the implemented Python algorithms, including failure boundaries and serialization details needed by port authors.

The parent [design_update.md](../../design_update.md) is the active implementation upgrade specification for Go, Rust and Java. It includes the reviewed per-language differences, current contract-1.1.0 additions and implementation/acceptance work packages.

Development scripts, qualification matrices, milestone notes, frozen source/fixtures/schema and all benchmark results are preserved in the repository [archive](../../archive/README.md). They are historical reference material, not runtime dependencies. There is no active Python qualification runner in this application directory.
