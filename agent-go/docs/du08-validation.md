# DU-08: run lifecycle, cancellation, and service boundaries

Run the controlled checks from `agent-go`. PostgreSQL checks read `AGENT_SESSION_DATABASE_URL` from the repository `.env`; no database URL export is needed. The tests create disposable `du05_<uuid>` schemas and remove them after each test. DU-08 has no throughput benchmark; retain per-test timings and lifecycle artifacts.

## Controlled checks

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
mkdir -p archive/du08
go test ./cmd/server -run '^TestDU08C01' -count=1 -v -timeout=3m 2>&1 | tee archive/du08/c01-resume-identity.log
go test ./cmd/server -run '^TestDU08C02' -count=1 -v -timeout=3m 2>&1 | tee archive/du08/c02-cancel-target.log
go test ./cmd/server -run '^TestDU08C03' -count=1 -v -timeout=3m 2>&1 | tee archive/du08/c03-disconnect-resume.log
go test ./cmd/server -run '^TestDU08C04' -count=1 -v -timeout=3m 2>&1 | tee archive/du08/c04-startup-reconcile.log
go test ./cmd/server -run '^TestDU08C05' -count=1 -v -timeout=3m 2>&1 | tee archive/du08/c05-terminal-commit.log
go test ./internal/agent -run '^TestDU08C02' -count=1 -v -timeout=3m 2>&1 | tee archive/du08/c02-engine-checkpoint.log
```

Run all DU-08 controlled cases together:

```sh
go test ./cmd/server -run '^TestDU08' -count=1 -v -timeout=10m 2>&1 | tee archive/du08/controlled-server.log
go test ./internal/agent -run '^TestDU08' -count=1 -v -timeout=5m 2>&1 | tee archive/du08/controlled-agent.log
```

C01 checks new run identity, count, and fence while preserving goal/config. C02 confirms `/cancel` records the targeted committed run and the engine stops at a scheduling checkpoint. C03 closes a real HTTP stream while a synthetic tool is in flight, verifies its result settles, then resumes with a new run and the exact prior transcript prefix. C04 starts up after lease expiry and verifies reconciliation does not execute a model or tool. C05 injects terminal-commit failure and verifies a successful final event was not emitted.

## Regression and full suite

```sh
go test ./internal/agent -run '^(TestDU01|TestDU02|TestDU03|TestDU04|TestDU05|TestDU07|TestDU08)' -count=1 -v -timeout=10m 2>&1 | tee archive/du08/agent-regression.log
go test ./cmd/server -run '^(TestDU05|TestDU06|TestDU07|TestDU08)' -count=1 -v -timeout=15m 2>&1 | tee archive/du08/server-regression.log
go test ./... -count=1 -v -timeout=20m 2>&1 | tee archive/du08/full-suite.log
git -C .. diff --check -- agent-go
```

## Live service checks

Restart the updated Go server in terminal A. Start a second process in terminal B to verify shared database ownership. Both processes load the repository `.env` and use the same config directory.

Terminal A:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
PORT=8000 AGENT_CONFIG_ROOT=../config go run ./cmd/server
```

Terminal B:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
PORT=8001 AGENT_CONFIG_ROOT=../config go run ./cmd/server
```

With a known running session owned by terminal A, send cancellation through terminal B:

```sh
AGENT_URL=http://127.0.0.1:8001
curl -fsS -X POST "$AGENT_URL/sessions/$SESSION_ID/cancel" | tee archive/du08/cancel-response.json
jq -e --arg id "$RUN_ID" '.cancel_requested_for | index($id) != null' archive/du08/cancel-response.json
```

Then read the session and runs from either worker and confirm the targeted run is `cancelled`, the task remains resumable, and continuation creates a different run ID with a higher fence. Use a deliberately held invocation or the controlled disconnect test to ensure cancellation occurs before the workflow naturally finishes; a fast completed run does not exercise cancellation.

For SSE disconnect, run the stream from a client that can close its socket only after observing an actual in-flight tool call. After close, verify cancellation is recorded, the same run reaches a stable cancelled snapshot, wait three seconds and confirm it is unchanged, then resume and compare the transcript prefix. Preserve the stream, state, run-list, transcript, and server logs. A curl timeout by itself does not prove the disconnect/cancellation contract.

The new endpoint is `POST /sessions/{session_id}/cancel`. It returns the run IDs whose durable cancellation flags were set; an empty list means no committed run was running when cancellation serialized with the session row.
