# WebFlux implementation

The service uses `DatabaseClient`/R2DBC; no JDBC is used on request paths. State uses the same JSON names and PostgreSQL tables as Python. The controller persists each engine milestone before adding it to the SSE output sequence, so a reconnect/restart can restore the last durable state.

`AgentEngine` is the generic YAML workflow/evidence evaluator: workflow phase transitions and observed evidence are kept outside the compactable raw-memory buffer. `FixtureTools` implements Cases 1–3 against the shared PostgreSQL fixture tables. Completed and bounded/failed sessions can be resumed through the SSE endpoint, retaining immutable config snapshots, memory, workflow facts, and compaction events for Cases 4–5.
