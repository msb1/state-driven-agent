# DU-06: PostgreSQL schema and atomic commit

Run these commands from `agent-go`. PostgreSQL connection settings are read by the tests from the repository `.env` (`AGENT_SESSION_DATABASE_URL`); no database URL export is needed. SQL checks create random disposable schemas and remove them after each test. They do not call the model. Keep the logs in `archive/du06/` so each check has a separate progress record.

## Controlled SQL checks

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
mkdir -p archive/du06
go test ./cmd/server -run '^TestDU06C01' -count=1 -v -timeout=3m 2>&1 | tee archive/du06/c01-schema.log
go test ./cmd/server -run '^TestDU06C02' -count=1 -v -timeout=5m 2>&1 | tee archive/du06/c02-rollback.log
go test ./cmd/server -run '^TestDU06C03' -count=1 -v -timeout=3m 2>&1 | tee archive/du06/c03-cas-anchors.log
go test ./cmd/server -run '^TestDU06C04' -count=1 -v -timeout=3m 2>&1 | tee archive/du06/c04-commit-retry.log
go test ./cmd/server -run '^TestDU06C05' -count=1 -v -timeout=3m 2>&1 | tee archive/du06/c05-startup-lock.log
```

- **C01** checks all ten tables, constraints and indexes, compatibility markers, repeated initialization, and an old snapshot import that records a `history_gap` instead of claiming complete history.
- **C02** injects a failure after each aggregate write stage and checks that revision, snapshot, transcript, calls, tool executions, diagnostics, events, context and archive receipts all roll back while the caller keeps the commit ID and staged outboxes.
- **C03** checks the revision CAS with two copies and rejects changes to goal and raw goal message.
- **C04** simulates a lost acknowledgement after commit, retries the same identity and content, checks no duplicate effects and verifies the reloaded context. It also rejects changed content under an already-used commit identity.
- **C05** starts two independent server instances against a new disposable schema at the same time, then repeats initialization on the current schema. Both use the shared advisory lock.

Run the controlled checks together while retaining readable progress:

```sh
go test ./cmd/server -run '^TestDU06C0[1-5]' -count=1 -v -timeout=10m 2>&1 | tee archive/du06/controlled.log
```

## Regression, full suite, and repository check

```sh
go test ./internal/agent -run '^(TestDU01|TestDU02|TestDU03|TestDU04|TestDU05)' -count=1 -v -timeout=5m 2>&1 | tee archive/du06/agent-regression.log
go test ./cmd/server -run '^(TestDU05|TestDU06)' -count=1 -v -timeout=10m 2>&1 | tee archive/du06/storage-regression.log
go test ./... -count=1 -v -timeout=15m 2>&1 | tee archive/du06/full-suite.log
git -C .. diff --check -- agent-go
```

DU-06 has no standalone throughput benchmark. Record per-check elapsed times, revisions, rollback counts, and the C05 concurrent startup duration from test logs. DU-09-B01 measures repository growth after the atomic correctness gates are qualified.

## C06 live service check

Start the updated Go service in one terminal; it reads the same `.env` and serializes schema initialization under advisory key `734910003`:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
PORT=8000 AGENT_CONFIG_ROOT=../config go run ./cmd/server
```

In another terminal, create and append to a session, then capture its immutable transcript. These C06 operations do not call the model:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
mkdir -p archive/du06
AGENT_URL=http://127.0.0.1:8000
DU06_OUT=$(mktemp -d "$PWD/archive/du06/service.XXXXXX")
printf 'DU-06 live artifacts: %s\n' "$DU06_OUT"
curl -fsS "$AGENT_URL/health" | jq -e '.status == "ok"'
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"log-analysis.yaml","user_prompt":"Offline DU-06 persistence check."}' \
  "$AGENT_URL/sessions" -o "$DU06_OUT/created.json"
DU06_ID=$(jq -er '.session.id' "$DU06_OUT/created.json")
curl -fsS -H 'Content-Type: application/json' \
  -d '{"content":"Append-only persistence marker."}' \
  "$AGENT_URL/sessions/$DU06_ID/messages" -o "$DU06_OUT/appended.json"
curl -fsS "$AGENT_URL/sessions/$DU06_ID" -o "$DU06_OUT/state-before-restart.json"
jq -e '.status == "active" and .revision == 2 and (.memory | length) == 2' "$DU06_OUT/state-before-restart.json"
curl -fsS "$AGENT_URL/sessions/$DU06_ID/transcript?after_sequence=0&limit=1000" -o "$DU06_OUT/transcript-before-restart.json"
jq -e '.history_complete == true and (.entries | length) == .through_sequence and .through_sequence >= 2' "$DU06_OUT/transcript-before-restart.json"
jq -S '{id,goal,revision,next_transcript_sequence,memory,status}' "$DU06_OUT/state-before-restart.json" > "$DU06_OUT/state-before-summary.json"
```

Stop the server with Ctrl-C, start it again with the same command, then verify the same committed snapshot and transcript survived restart:

```sh
curl -fsS "$AGENT_URL/health" | jq -e '.status == "ok"'
curl -fsS "$AGENT_URL/sessions/$DU06_ID" -o "$DU06_OUT/state-after-restart.json"
jq -S '{id,goal,revision,next_transcript_sequence,memory,status}' "$DU06_OUT/state-after-restart.json" > "$DU06_OUT/state-after-summary.json"
diff -u "$DU06_OUT/state-before-summary.json" "$DU06_OUT/state-after-summary.json"
curl -fsS "$AGENT_URL/sessions/$DU06_ID/transcript?after_sequence=0&limit=1000" -o "$DU06_OUT/transcript-after-restart.json"
jq -e --slurpfile before "$DU06_OUT/transcript-before-restart.json" '.entries == $before[0].entries and .through_sequence == $before[0].through_sequence' "$DU06_OUT/transcript-after-restart.json"
```

Every `jq -e` should print `true`; `diff` should be empty. Keep failed artifacts for diagnosis rather than replacing them with a rerun. For the established append/stop/restart/resume and compaction history probes, also run the DU-05 live commands in [du05-validation.md](du05-validation.md); those exercise the existing history and paging routes under the new commit protocol.

### Current boundary

DU-06 provides the lease/fence columns and save-time checks used by the DU-07 ownership coordinator. The user reports C01–C06, regression checks, and the full Go suite passed, so DU-06 is qualified. Lease acquisition/renewal and tool intent/recovery are implemented and validated separately under [du07-validation.md](du07-validation.md). Cancellation remains DU-08 work. Retention is represented by the transactionally written receipt table and outbox hook; archive execution and pruning remain DU-10 work.
