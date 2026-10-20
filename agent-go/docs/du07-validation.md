# DU-07: database ownership and tool recovery

Run the SQL checks from `agent-go`. They load `AGENT_SESSION_DATABASE_URL` from the repository `.env` and create disposable `du05_<uuid>` schemas. No database URL export is required. C01–C03 use PostgreSQL; C04 is model-free and in-memory. DU-07 has no separate throughput benchmark: keep the per-test timing, fence, invocation, and attempt measurements in the logs.

## Controlled checks

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
mkdir -p archive/du07
go test ./cmd/server -run '^TestDU07C01' -count=1 -v -timeout=3m 2>&1 | tee archive/du07/c01-owner-race.log
go test ./cmd/server -run '^TestDU07C02' -count=1 -v -timeout=3m 2>&1 | tee archive/du07/c02-durable-execution.log
go test ./cmd/server -run '^TestDU07C03' -count=1 -v -timeout=3m 2>&1 | tee archive/du07/c03-heartbeat-fence.log
go test ./internal/agent -run '^TestDU07C04' -count=1 -v -timeout=3m 2>&1 | tee archive/du07/c04-recovery-policy.log
```

Run the controlled set together after the individual logs are retained:

```sh
go test ./cmd/server -run '^TestDU07C0[1-3]' -count=1 -v -timeout=10m 2>&1 | tee archive/du07/controlled-sql.log
go test ./internal/agent -run '^TestDU07C04' -count=1 -v -timeout=3m 2>&1 | tee archive/du07/controlled-recovery.log
```

C01 races two claimants, waits for expiry/takeover, verifies a monotonic fence, then proves the old owner cannot renew, release, or save over the successor. C02 verifies that intent and started markers are committed before adapter entry and that the final execution row and correlated tool message persist. C03 scales the lease to two seconds and verifies multiple independent renewals before forcing a successor fence; the old worker is stopped and its commit rejected. C04 covers explicit transient retries, ordinary error strings without retry, real idempotency-key adapters, missing adapters, unresolved reconciliation, and confirmed reconciliation without replay.

## Regression and full suite

```sh
go test ./internal/agent -run '^(TestDU01|TestDU02|TestDU03|TestDU04|TestDU05|TestDU07)' -count=1 -v -timeout=5m 2>&1 | tee archive/du07/agent-regression.log
go test ./cmd/server -run '^(TestDU05|TestDU06|TestDU07)' -count=1 -v -timeout=15m 2>&1 | tee archive/du07/storage-regression.log
go test ./... -count=1 -v -timeout=20m 2>&1 | tee archive/du07/full-suite.log
git -C .. diff --check -- agent-go
```

## Two-worker service check (C05)

Start the same service in two terminals using the repository `.env` and the shared config directory:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
PORT=8000 AGENT_CONFIG_ROOT=../config go run ./cmd/server
```

Use `PORT=8001` in terminal B. In terminal A, create a compaction-stress session and begin the streamed run:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
mkdir -p archive/du07
AGENT_URL=http://127.0.0.1:8000
DU07_OUT=$(mktemp -d "$PWD/archive/du07/service.XXXXXX")
printf 'DU-07 live artifacts: %s\n' "$DU07_OUT"
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"compaction-stress.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$DU07_OUT/created.json"
DU07_ID=$(jq -er '.session.id' "$DU07_OUT/created.json")
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$DU07_ID/run/stream" | tee "$DU07_OUT/owner-run.sse"
```

While terminal A is still streaming, run in terminal B:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
AGENT_URL=http://127.0.0.1:8001
DU07_OUT=$(ls -dt "$PWD"/archive/du07/service.* | head -1)
DU07_ID=$(jq -er '.session.id' "$DU07_OUT/created.json")
curl -fsS "$AGENT_URL/sessions/$DU07_ID" -o "$DU07_OUT/running-state.json"
jq -e '.run_status == "running" and .run_id' "$DU07_OUT/running-state.json"
HTTP_CODE=$(curl -sS -o "$DU07_OUT/competing-run.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' -d '{"max_steps":1}' "$AGENT_URL/sessions/$DU07_ID/run")
[ "$HTTP_CODE" = 409 ] && printf 'PASS competing run rejected by database lease\n'
HTTP_CODE=$(curl -sS -o "$DU07_OUT/competing-message.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' -d '{"content":"Concurrent input must not be appended."}' \
  "$AGENT_URL/sessions/$DU07_ID/messages")
[ "$HTTP_CODE" = 409 ] && printf 'PASS competing message rejected by database fence\n'
curl -fsS "$AGENT_URL/sessions/$DU07_ID/runs?limit=100" | jq -e --slurpfile state "$DU07_OUT/running-state.json" \
  'any(.runs[]; .run_id == $state[0].run_id and .status == "running" and .owner_id and .fence > 0)'
```

After the owner stream finishes, inspect the execution ledger:

```sh
curl -fsS "$AGENT_URL/sessions/$DU07_ID/executions?limit=100" -o "$DU07_OUT/executions.json"
jq -e 'all(.executions[]; .execution_id and .call_id and .tool and .policy and .attempts >= 0 and .fence > 0)' "$DU07_OUT/executions.json"
```

This is the DU-07 ownership and execution-history slice of shared test-case section 16. Cancellation and disconnect continuation are DU-08 checks. Keep both server logs, SSE output, and the JSON artifacts, including failures.
