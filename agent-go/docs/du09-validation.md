# DU-09: selective hydration, bounded history, and API pages

Run these commands from `agent-go`. PostgreSQL tests load `AGENT_SESSION_DATABASE_URL` from the repository `.env`; no URL export is required. They create disposable schemas and remove them afterward. The B01 workload uses 21 batches and 60 fresh sessions, runs three repetitions of each of seven cells, and does not invoke models or tools.

## Controlled checks

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
mkdir -p archive/du09
go test ./cmd/server -run '^TestDU09C01' -count=1 -v -timeout=3m 2>&1 | tee archive/du09/c01-payload-hydration.log
go test ./cmd/server -run '^TestDU09C02' -count=1 -v -timeout=10m 2>&1 | tee archive/du09/c02-bounded-history.log
go test ./cmd/server -run '^TestDU09C03' -count=1 -v -timeout=5m 2>&1 | tee archive/du09/c03-pages-and-cursors.log
```

C01 compares the active memory payload, resolved configuration identity, and revision before and after hydration. C02 seeds 150 archived records and 40 diagnostics, then verifies that active hydration retains the full transcript while loading only the active memory and 32 latest diagnostic records; it also checks normalized historical call-ID mapping and that the hot/public session state does not embed the growing execution archive. C03 exercises fixed event watermarks, run/execution/archive/diagnostic pagination, empty pages, invalid UUIDs/families/cursors, and the read-only retention and abandoned-run routes.

## Required PostgreSQL benchmark B01

```sh
go test ./cmd/server -run '^TestDU09B01PostgresQualificationGrid$' -count=1 -v -timeout=30m 2>&1 | tee archive/du09/b01-postgres-grid.log
```

The test prints one `DU09_B01_CELL` record per batch repetition, a `DU09_B01_TOTAL` record per cell, and `DU09_B01_PLAN` query plans at 0/100/1000 archived pairs. Cells are `(clients, archived pairs, current output bytes)`: `(1,0,1024)`, `(4,0,1024)`, `(8,0,1024)`, `(1,100,1024)`, `(1,1000,1024)`, `(1,0,8192)`, `(4,100,8192)`. Each independent session has fixed active context, a current synthetic call/result pair, and optional archived pairs/diagnostics seeded before timing. Each client performs two get/save/get cycles. Raw per-client storage samples include wall time, database round-trip or transaction time, snapshot/read/write logical JSON bytes, revision, fence, and payload check.

The test fails if any client loses a revision, changes its active payload, or errors; it also enforces the `<8192` byte hot-snapshot range across archive growth and the `<8192` byte selected-read range from 100 to 1000 historical pairs at fixed 1 KiB output. These are logical JSON-size bounds, not latency guarantees.

## Regression and full suite

```sh
go test ./internal/agent -run '^(TestDU01|TestDU02|TestDU03|TestDU04|TestDU05|TestDU07|TestDU08)' -count=1 -v -timeout=10m 2>&1 | tee archive/du09/agent-regression.log
go test ./cmd/server -run '^(TestDU05|TestDU06|TestDU07|TestDU08|TestDU09)' -count=1 -v -timeout=30m 2>&1 | tee archive/du09/server-regression.log
go test ./... -count=1 -v -timeout=40m 2>&1 | tee archive/du09/full-suite.log
git -C .. diff --check -- agent-go
```

## Live endpoint smoke check

Start the updated server in one terminal using the repository `.env`:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
PORT=8000 AGENT_CONFIG_ROOT=../config go run ./cmd/server
```

In another terminal, create one session and check the new read endpoints and all cursor domains:

```sh
cd /Users/msb/Code/state-driven-agent/agent-go
mkdir -p archive/du09/live
DU09_OUT="$PWD/archive/du09/live"
AGENT_URL=http://127.0.0.1:8000
curl -fsS "$AGENT_URL/health" | tee "$DU09_OUT/health.json"
curl -fsS "$AGENT_URL/configs" | tee "$DU09_OUT/configs.json"
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"log-analysis.yaml","user_prompt":"Inspect the access log and report the validated top HTTP 500 IP."}' \
  "$AGENT_URL/sessions" -o "$DU09_OUT/created.json"
DU09_ID=$(jq -er '.session.id' "$DU09_OUT/created.json")
curl -fsS "$AGENT_URL/sessions/$DU09_ID" | jq -e --arg id "$DU09_ID" '.id == $id'
curl -fsS "$AGENT_URL/sessions/$DU09_ID/transcript?after_sequence=0&limit=1" | tee "$DU09_OUT/transcript.json"
curl -fsS "$AGENT_URL/sessions/$DU09_ID/events?after=0&limit=1" | tee "$DU09_OUT/events.json"
curl -fsS "$AGENT_URL/sessions/$DU09_ID/diagnostics/compaction_events?after=0&limit=1" | tee "$DU09_OUT/diagnostics.json"
curl -fsS "$AGENT_URL/sessions/$DU09_ID/runs?limit=1" | tee "$DU09_OUT/runs.json"
curl -fsS "$AGENT_URL/sessions/$DU09_ID/executions?limit=1" | tee "$DU09_OUT/executions.json"
curl -fsS "$AGENT_URL/sessions/$DU09_ID/archives?limit=1" | tee "$DU09_OUT/archives.json"
curl -fsS "$AGENT_URL/sessions/$DU09_ID/retention" | tee "$DU09_OUT/retention.json"
curl -fsS "$AGENT_URL/runs/abandoned?limit=1" | tee "$DU09_OUT/abandoned.json"
```

The controlled tests cover the remaining write/stream routes, request validation, and empty-page behavior. No destructive retention operation is added by DU-09.
