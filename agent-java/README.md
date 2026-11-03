# State-driven agent — Java

Native Java 21 / Spring Boot WebFlux implementation targeting **contract 2.0.0 and storage schema 6**, including applicable 1.1.0 completion rules and current Python compaction normalization. Java regressions cover the integrated runtime; shared CLI qualification remains operator-run and is not claimed by these tests.

From the repository root:

```sh
mvn -f agent-java/pom.xml clean verify
AGENT_CONFIG_ROOT="$PWD/config" PORT=8000 mvn -f agent-java/pom.xml spring-boot:run
```

Alternatively, after packaging:

```sh
java -jar agent-java/target/agent-java-2.0.0.jar
```

The root `.env` supplies `AGENT_DEPLOYMENT_NAMESPACE`, `AGENT_SESSION_DATABASE_URL`, optional `AGENT_FIXTURE_DATABASE_URL`, `MAIN_MODEL`, `OPENAI_BASE_URL`, and the configured API-key variable. Keep the deployment namespace stable across restarts and workers. The root `.env` owns `MAIN_MODEL`; no automatic model substitution is performed. Existing sessions retain their admitted resolved configuration and original goal.

Startup validates the existing shared schema in a read-only transaction and recovers abandoned lifecycle metadata without invoking models or tools. The runtime performs no DDL, initialization, migration, or automatic backfill. The temporary upgrade startup block has been removed. Workers on different ports can share the database and coordinate through expiring leases and increasing fences.

All POST requests require one `Idempotency-Key` header. Retain the key and original request body when retrying the same operation. Terminal duplicates replay their stored response; active/uncertain/blocked duplicates return the existing command handle. Cancellation requires an explicit `run_id`. GET `/commands/{id}` inspects command state; GET `/sessions/{id}/events/stream?run_id=...&after_event_id=...` reconnects to durable events without creating a run.

Swagger UI: `/docs`; OpenAPI: `/openapi.json`.

Archive administration uses the packaged Java application:

```sh
java -jar agent-java/target/agent-java-2.0.0.jar admin export SESSION_UUID /absolute/new-export.jsonl
java -jar agent-java/target/agent-java-2.0.0.jar admin verify /absolute/new-export.jsonl
java -jar agent-java/target/agent-java-2.0.0.jar admin prune SESSION_UUID /absolute/new-export.jsonl 32 100
```

Exports use consistent read-only snapshots and exclusive file creation. Pruning is explicit, permits only old events/diagnostics, and requires a fresh verified export, a terminal resolved session, valid archive dependencies, ownership/fencing, matching revision and telemetry digests. Commands, commit receipts, action attempts, transcript, context, runs, executions and archive receipts remain protected.

Run the unchanged `test-cases.sh`, `persist.sh`, and `aux-tests.sh` from your CLI against the Java listener. See [implementation](docs/implementation.md), [architecture](docs/architecture.md), and [upgrade evidence](../digest-java.md). No shared shell test modifications are required.
