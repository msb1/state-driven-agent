# DU-10 archive and retention validation

DU-10 keeps all session data by default. `cmd/server/retention.go` exports a
repeatable-read, read-only PostgreSQL snapshot as canonical `sda-archive-1`
JSONL, verifies its header, table order, row order, session identity, manifest,
row counts, watermarks, and SHA-256 digests, and supports an explicit telemetry
prune operation for terminal sessions. Prune requires a fresh archive, current
revision, a database lease/fence, no running or unresolved work, and intact
prior archive dependencies. Receipt insertion, diagnostic/event deletion, and
the state/event update share one database transaction. No destructive HTTP
route is registered. Normal operation remains keep-all.

The Go binary also has explicit admin subcommands for plan/export/prune. They
load the repository `.env`, initialize the canonical schema, then exit without
starting a listener or reconciling abandoned runs:

```sh
SESSION_ID='paste a terminal session UUID here'
mkdir -p archive/du10
go run ./cmd/server retention plan --session-id "$SESSION_ID"
go run ./cmd/server retention export --session-id "$SESSION_ID" --archive "archive/du10/$SESSION_ID.jsonl"
go run ./cmd/server retention verify --archive "archive/du10/$SESSION_ID.jsonl"
go run ./cmd/server retention prune-telemetry --session-id "$SESSION_ID" --archive "archive/du10/$SESSION_ID.jsonl" --keep-diagnostics 32 --keep-events 100
```

`verify` is file-only and does not connect to PostgreSQL; the plan, export, and
prune subcommands use the database configured in `.env`.

Export and prune are admin actions. Review the plan and verify the exported
archive before invoking prune. The Go storage implementation writes receipts
that include the resolved archive path so later prune operations can verify the
permanent archive chain.

## Controlled and PostgreSQL commands

Run from `agent-go`. The repository `.env` is loaded by the tests; no manual
database environment export is needed. `set -o pipefail` keeps test failures
visible when logs are captured with `tee`.

```sh
set -o pipefail
mkdir -p archive/du10

go test ./cmd/server -run '^TestDU10C01' -count=1 -v -timeout=5m 2>&1 | tee archive/du10/c01-repeatable-read-export.log
go test ./cmd/server -run '^TestDU10C02' -count=1 -v -timeout=5m 2>&1 | tee archive/du10/c02-archive-verification.log
go test ./cmd/server -run '^TestDU10C03' -count=1 -v -timeout=10m 2>&1 | tee archive/du10/c03-prune-guards-and-rollback.log
go test ./cmd/server -run '^TestDU10C04' -count=1 -v -timeout=10m 2>&1 | tee archive/du10/c04-atomic-prune-and-chain.log
go test ./cmd/server -run '^TestDU10C05' -count=1 -v -timeout=5m 2>&1 | tee archive/du10/c05-keep-all-default.log
go test ./cmd/server -run '^TestDU10B01' -count=1 -v -timeout=20m 2>&1 | tee archive/du10/b01-export-qualification-grid.log

go test ./internal/agent -run '^(TestDU07|TestDU08)' -count=1 -v -timeout=10m 2>&1 | tee archive/du10/agent-regression.log
go test ./cmd/server -run '^(TestDU05|TestDU06|TestDU07|TestDU08|TestDU09|TestDU10)' -count=1 -v -timeout=40m 2>&1 | tee archive/du10/server-regression.log
go test ./... -count=1 -v -timeout=50m 2>&1 | tee archive/du10/full-suite.log
git diff --check -- .
```

The qualification grid reports row counts, JSONL bytes, snapshot revision,
SHA-256, and export elapsed time for 0, 100, and 1,000 event workloads. It is a
measurement only and sets no throughput threshold. C01–C05 cover archive
format and corruption rejection, stale/active guards, rollback after the prune
stage, the 32-row diagnostic and 100-event tails, protected history, permanent
receipts, chaining, and the no-prune keep-all path. The live server check is
read-only: `GET /sessions/{id}/retention` should report `default_policy` as
`keep_all`; destructive retention remains an explicit admin driver operation.

## Reported live qualification

The user-reported C01–C05, regression, and full-suite runs passed. The live
session `03eb9f60-fdef-49d4-b099-3cddd35fb4aa` was eligible at revision 92,
with zero running lifecycles and unresolved executions. Export and verify
matched archive SHA-256
`2ca45ac4ac8b97825eb29a45d2f8c32dedc3013479e0e2782eeb6bfff42f77cb`.
Prune recorded receipt `0e9a88f7-6d52-499d-9631-a24e6d135a0a` at revision 93.
This live run did not delete telemetry: it contained 92 events (below the
100-event tail), and diagnostic family watermarks were 11, 26, and 1 (each
within the 32-row tail). The controlled C04 test exercises actual deletion
using a larger fixture.
