# DU-05: Go immutable history validation

These commands are for user execution. No runtime tests, model calls, or database calls were executed by the implementation agent. Package and test-binary compilation were checked without executing test binaries. Run the commands in order and retain every log, including failures. No separate benchmark is required for DU-05.

## 1. Controlled checks (C01)

Start in a client terminal:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
set -o pipefail
mkdir -p archive/du05
go test ./internal/agent -run '^TestDU05C01' -count=1 -v -timeout=60s 2>&1 | tee archive/du05/c01-controlled.log
```

Expected: every `TestDU05C01...` case passes. The cases cover shared message/audit sequencing, copied immutable payloads, public nullable fields, private outbox exclusion, protocol-only requests, numeric artifact precision, diagnostic tails/counters, run/tool/rejection records, interrupted batch journaling, and preserved goal/raw identities during compaction.

## 2. PostgreSQL checks (C02, C03 and cursor coverage)

These tests require PostgreSQL and permission to create a schema. Each test creates an isolated `du05_<random UUID>` schema, sets its search path to that schema, and removes it afterward. They do not use fixture tables or a model service. The test helper loads the repository `.env`, reads `AGENT_SESSION_DATABASE_URL`, and falls back to the same URL as the server. No URL setup is needed.

```sh
go test ./cmd/server -run '^TestDU05C02' -count=1 -v -timeout=3m 2>&1 | tee archive/du05/c02-postgres.log
go test ./cmd/server -run '^TestDU05C03' -count=1 -v -timeout=3m 2>&1 | tee archive/du05/c03-integrity.log
go test ./cmd/server -run '^TestDU05C04' -count=1 -v -timeout=3m 2>&1 | tee archive/du05/c04-pagination.log
```

Expected: actual `PASS` results with isolated schema names in the logs. `SKIP` is not SQL qualification. C02 checks append/compaction/bounded stop/resume/reconnect, hot/context state round trips, immutable transcript prefixes, legacy imports and all 40 archived diagnostic records despite a recent tail of 32. C03 rejects changed-payload and timestamp reuse, sequence collisions/gaps, cross-session summary sources, inconsistent prior ledgers, changed goal/config anchors, replaced goal identities, invalid context/ledger references, diagnostic collisions/gaps and next-sequence disagreement. It compares the session/context/transcript/diagnostic/event rows before and after rejection, and checks permitted identical record replay does not duplicate history. C04 checks fixed watermarks, page boundaries and invalid cursor limits against PostgreSQL. C02 reconnects through a fresh pool; the live procedure below supplies the actual process restart check.

## 3. Regression and full suite

The full suite loads the same `.env` URL for its SQL cases.

```sh
go test ./internal/agent -run '^(TestDU01|TestDU02|TestDU03|TestDU04|TestDU05)' -count=1 -v -timeout=3m 2>&1 | tee archive/du05/regression.log
go test ./... -count=1 -v -timeout=5m 2>&1 | tee archive/du05/full-suite.log
git -C .. diff --check -- agent-go
```

The SQL helper uses `DU05_TEST_DATABASE_URL` only as an optional override. Otherwise it reads `AGENT_SESSION_DATABASE_URL` from the repository `.env`, then uses the same fallback as server startup. The regression includes the existing deterministic DU-04 workload; no additional DU-05 performance benchmark is needed.

## 4. Live service, append, bounded stop, restart and resume (C04)

In the server terminal, stop the previous Go process with Ctrl-C and start the updated service. The server loads the repository `.env` and uses the existing PostgreSQL fixtures:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
PORT=8000 AGENT_CONFIG_ROOT=../config go run ./cmd/server
```

In the client terminal:

```sh
AGENT_URL=http://127.0.0.1:8000
DU05_OUT=$(mktemp -d "$PWD/archive/du05/service.XXXXXX")
printf 'DU-05 artifacts: %s\n' "$DU05_OUT"
curl -fsS "$AGENT_URL/health" | jq -e '.status == "ok"'
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"tool-recovery.yaml","user_prompt":"Complete the configured log recovery workflow and return exactly RECOVERY_OK 10.0.0.5 83."}' \
  "$AGENT_URL/sessions" -o "$DU05_OUT/created.json"
DU05_ID=$(jq -er '.session.id' "$DU05_OUT/created.json")
jq -e '.session.revision == 1 and .session.next_transcript_sequence == 2 and .session.history_complete == true and .session.memory[0].sequence == 1 and .session.memory[0].message_id and .session.memory[0].created_at and .session.resolved_config_sha256' "$DU05_OUT/created.json"
curl -fsS -H 'Content-Type: application/json' \
  -d '{"content":"Follow the configured diagnostic and recovery workflow."}' \
  "$AGENT_URL/sessions/$DU05_ID/messages" -o "$DU05_OUT/appended.json"
jq -e --slurpfile created "$DU05_OUT/created.json" '.session.goal == $created[0].session.goal and .session.memory[0].message_id == $created[0].session.memory[0].message_id and .session.memory[1].sequence == 2' "$DU05_OUT/appended.json"
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":1}' \
  "$AGENT_URL/sessions/$DU05_ID/run/stream" | tee "$DU05_OUT/first-run.sse"
curl -fsS "$AGENT_URL/sessions/$DU05_ID" -o "$DU05_OUT/before-restart.json"
jq -e '.status == "failed" and .run_status == "failed" and .step_count == 1 and .run_count == 1 and .run_id and .run_ended_at' "$DU05_OUT/before-restart.json"
curl -fsS "$AGENT_URL/sessions/$DU05_ID/transcript?after_sequence=0&limit=1000" -o "$DU05_OUT/prefix.json"
jq -e '.history_complete == true and (.entries | length) == .through_sequence and ([.entries[].sequence] == [range(1; .through_sequence + 1)]) and any(.entries[]; .kind == "run_started") and any(.entries[]; .kind == "tool_intent") and any(.entries[]; .kind == "tool_outcome") and any(.entries[]; .kind == "run_finished")' "$DU05_OUT/prefix.json"
DU05_THROUGH=$(jq -er '.through_sequence' "$DU05_OUT/prefix.json")
jq -S '{id,goal,config_ref,config_sha256,resolved_config_sha256,revision,next_transcript_sequence,memory,active_ledger_id,run_id,run_count,status,run_status,final_answer}' "$DU05_OUT/before-restart.json" > "$DU05_OUT/before-restart-summary.json"
```

Now stop the server with Ctrl-C in its own terminal and run the same port-8000 startup command again. After it is listening, continue in the client terminal:

```sh
curl -fsS "$AGENT_URL/health" | jq -e '.status == "ok"'
curl -fsS "$AGENT_URL/sessions/$DU05_ID" -o "$DU05_OUT/after-restart.json"
jq -S '{id,goal,config_ref,config_sha256,resolved_config_sha256,revision,next_transcript_sequence,memory,active_ledger_id,run_id,run_count,status,run_status,final_answer}' "$DU05_OUT/after-restart.json" > "$DU05_OUT/after-restart-summary.json"
diff -u "$DU05_OUT/before-restart-summary.json" "$DU05_OUT/after-restart-summary.json"
curl -fsS "$AGENT_URL/sessions/$DU05_ID/transcript?after_sequence=0&limit=1000&through_sequence=$DU05_THROUGH" -o "$DU05_OUT/restarted-prefix.json"
jq -e --slurpfile before "$DU05_OUT/prefix.json" '.entries == $before[0].entries and .through_sequence == $before[0].through_sequence' "$DU05_OUT/restarted-prefix.json"
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":30}' \
  "$AGENT_URL/sessions/$DU05_ID/resume" | tee "$DU05_OUT/resume.sse"
curl -fsS "$AGENT_URL/sessions/$DU05_ID" -o "$DU05_OUT/final-state.json"
jq -e --slurpfile before "$DU05_OUT/before-restart.json" '.status == "complete" and .run_status == "complete" and .final_answer == "RECOVERY_OK 10.0.0.5 83" and .run_count == 2 and .run_id != $before[0].run_id and .goal == $before[0].goal and .memory[0].message_id == $before[0].memory[0].message_id' "$DU05_OUT/final-state.json"
curl -fsS "$AGENT_URL/sessions/$DU05_ID/transcript?after_sequence=0&limit=1000" -o "$DU05_OUT/final-transcript.json"
jq -e --slurpfile before "$DU05_OUT/prefix.json" --slurpfile state "$DU05_OUT/final-state.json" '.history_complete == true and (.entries[0:($before[0].entries | length)] == $before[0].entries) and (.entries | length) == .through_sequence and $state[0].next_transcript_sequence == .through_sequence + 1' "$DU05_OUT/final-transcript.json"
curl -fsS "$AGENT_URL/sessions/$DU05_ID/transcript?after_sequence=0&limit=1" -o "$DU05_OUT/page-1.json"
DU05_PAGE_THROUGH=$(jq -er '.through_sequence' "$DU05_OUT/page-1.json")
curl -fsS "$AGENT_URL/sessions/$DU05_ID/transcript?after_sequence=1&limit=1&through_sequence=$DU05_PAGE_THROUGH" -o "$DU05_OUT/page-2.json"
jq -e --slurpfile first "$DU05_OUT/page-1.json" '.entries[0].sequence == 2 and .entries[0].message_id != $first[0].entries[0].message_id and .through_sequence == $first[0].through_sequence' "$DU05_OUT/page-2.json"
```

Expected: every `jq -e` returns `true`, both `diff` inputs match, the stopped transcript remains an identical prefix after restart and resume, and the final answer is the exact recovery string. Audit records share the sequence counter: message sequences alone may have gaps.

## 5. Live compaction preserves archived message identities

In the same client terminal:

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"compaction-stress.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$DU05_OUT/stress-created.json"
DU05_STRESS=$(jq -er '.session.id' "$DU05_OUT/stress-created.json")
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$DU05_STRESS/run/stream" | tee "$DU05_OUT/stress-run.sse"
curl -fsS "$AGENT_URL/sessions/$DU05_STRESS" -o "$DU05_OUT/stress-state.json"
jq -e '.status == "complete" and .run_status == "complete" and (.workflow.observed_evidence | length) == 11 and .diagnostic_counts.compaction_events > 0 and .active_ledger_id != null and (.final_answer | startswith("Phase 3\n")) and (.final_answer | test("New York median salary:[ \\t]*92500(\\.0+)?([ \\t\\r\\n]|$)")) and ([.final_answer | split("\n")[] | gsub("[ \\t\\r]"; "") | select(test("^\\|(Austin|Chicago|NewYork|Seattle)\\|"))] | sort) == (["|Austin|199|42.62|","|Chicago|181|42.01|","|NewYork|329|43.02|","|Seattle|183|43.46|"] | sort)' "$DU05_OUT/stress-state.json"
curl -fsS "$AGENT_URL/sessions/$DU05_STRESS/transcript?after_sequence=0&limit=1000" -o "$DU05_OUT/stress-transcript.json"
jq -e --slurpfile state "$DU05_OUT/stress-state.json" '(.entries | length) == .through_sequence and any(.entries[]; .kind == "compaction_summary" and .message_id == $state[0].active_ledger_id) and ([.entries[] | select(.kind == "message") | .message_id] as $messages | all(.entries[] | select(.kind == "compaction_summary" or .kind == "compaction_rejected"); (.payload.source_message_ids | length) > 0 and all(.payload.source_message_ids[]; . as $id | $messages | index($id) != null)))' "$DU05_OUT/stress-transcript.json"
jq '{revision,next_transcript_sequence,active_ledger_id,history_complete,diagnostic_counts,diagnostics_complete,diagnostic_rejections,compaction_savings_total,run_count,status}' "$DU05_OUT/stress-state.json"
```

If a live assertion fails, keep the failed session and its stream/state/transcript files. Do not replace them with a rerun before reporting the failure.

For the additional real-disconnect immutable-prefix observation, retain the transcript captures from shared `test-cases.md` section 15 when running the DU-08 cancellation qualification. The full disconnect/cancellation lifecycle and two-worker ownership checks belong to DU-07/08; this DU-05 procedure proves prefix preservation for bounded stop, process restart, resume and compaction through the routes implemented here.
