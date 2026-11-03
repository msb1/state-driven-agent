# Build and operate the Java runtime

Use JDK 21 or newer and Maven. The build targets Java 21; regressions also run on the installed JDK 25. The executable artifact is `agent-java/target/agent-java-2.0.0.jar`.

```sh
mvn -f agent-java/pom.xml clean verify
AGENT_CONFIG_ROOT="$PWD/config" PORT=8000 mvn -f agent-java/pom.xml spring-boot:run
```

The root `.env` is imported from repository-root or `agent-java` working directories. Session persistence uses JDBC and `AGENT_SESSION_DATABASE_URL`; fixture reads use R2DBC and `AGENT_FIXTURE_DATABASE_URL`, falling back to the session URL. The keyed controlled recipient uses JDBC against the fixture URL. `AGENT_SESSION_DB_USER` / `AGENT_SESSION_DB_PASSWORD` supply credentials when the URL omits them. No live credentials are stored in source or test fixtures.

A stable nonempty `AGENT_DEPLOYMENT_NAMESPACE` is mandatory. Existing sessions retain resolved configuration and goal anchors. New sessions read current profile/environment settings; the root `.env` controls `MAIN_MODEL`. The server does not pin or substitute model names.

Only an already initialized compatible shared schema 6 is accepted. Startup uses a read-only transaction to check columns, types, defaults, constraints and indexes against the pinned manifest. It recovers abandoned run metadata using normal command/session fencing. Failed startup validation propagates; no DDL or migration is attempted. HTTP traffic is held at 503 until startup completes.

All mutation endpoints require `Idempotency-Key` with 1..255 printable ASCII characters without spaces. Reuse the same key only for the identical normalized method/route/body. The command envelope includes `command_id`, `session_id`, `run_id`, status URI, status, cached response and recovery reason. An active duplicate returns 202; a terminal duplicate returns the original status/body with replay headers.

Session routes include current state, messages, JSON/SSE runs, resume, explicit-run cancellation, transcript, events, diagnostics, runs, executions, archives, retention, and read-only event reconnection. `/runs/abandoned` exposes bounded inspection. UUIDs, limits and cursors are validated before SQL. Stream disconnects request cancellation while allowing the in-flight invocation to settle. Resume creates a new run while retaining pending action identities.

Archive administration is available through the executable jar's `admin export`, `admin verify`, and `admin prune` operations; exact commands are in the [README](../README.md). Export creates a new restricted-permission file and never replaces an existing file. Pruning changes only eligible telemetry rows through a guarded transaction; it does not change the schema. Keep every dependency archive available for subsequent pruning checks.

Java tests use local files, mocked model/tool interfaces, WebFlux HTTP binding and scripted JDBC. Transaction regressions inject aggregate-write failures and lost commit acknowledgements; these are controlled regressions, not live PostgreSQL crash/partition qualification. No database connection is opened by these tests.

The user runs the unchanged repository `test-cases.sh`, `persist.sh` and `aux-tests.sh` against the Java server. Those results, including required restart, second-worker, socket and actual export/pruning controls, determine shared qualification. Historical Python or other-language passes are not Java evidence.
