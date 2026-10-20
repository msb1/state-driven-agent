# DU-11 fixture and serialization compatibility

DU-11 aligns the Go fixture-tool text with the shared Python fixtures, keeps
fixture-derived validation independent of hard-coded answer constants, and
uses Python-compatible config/source/commit JSON preimages. The stable profile
golden test uses `MAIN_MODEL=qwen2.5-7b-instruct-mlx` and
`OPENAI_BASE_URL=https://du11-fixture.invalid/v1`; this makes the hashes
independent of a developer's `.env`. Fixture tools remain read-only and are
wired to `AGENT_FIXTURE_DATABASE_URL` when the server starts.

Run from `agent-go`. `C02` creates and drops an isolated schema using the
configured session database. `C03` reads the configured fixture database and
expects the shared row counts (650 logs, 2,200 customers, 6,502 orders, and
1,000 employees). Its row fingerprint hashes PostgreSQL `to_jsonb(row)::text`
in primary-key order, with one UTF-8 newline after each row. It reports only
the database and schema names, never a connection string.

```sh
set -o pipefail
mkdir -p archive/du11

# C01: Python golden config hashes, source wire format, Unicode, float,
# exponent, negative-zero, HTML/XML escaping, and default-field omissions.
go test ./internal/agent -run '^TestDU11C01' -count=1 -v -timeout=5m 2>&1 | tee archive/du11/c01-serialization-vectors.log

# C02: fixture tools, normal and diagnostic outputs, parsing windows, report
# trace, normalized metrics, and validators on an isolated PostgreSQL schema.
go test ./cmd/server -run '^TestDU11C02' -count=1 -v -timeout=5m 2>&1 | tee archive/du11/c02-fixture-tools.log

# C03: fixture DB override target identity, canonical row fingerprints,
# and shared fixture counts.
go test ./cmd/server -run '^TestDU11C03' -count=1 -v -timeout=5m 2>&1 | tee archive/du11/c03-fixture-fingerprints.log

# C04: current shared YAML profile resolution and identities.
go test ./internal/agent -run '^TestDU11C04' -count=1 -v -timeout=5m 2>&1 | tee archive/du11/c04-profiles.log

# Existing DU-01 through DU-10 regressions and full suite.
go test ./internal/agent -run '^(TestDU01|TestDU02|TestDU03|TestDU04|TestDU07|TestDU08|TestDU11)' -count=1 -v -timeout=15m 2>&1 | tee archive/du11/agent-regression.log
go test ./cmd/server -run '^(TestDU05|TestDU06|TestDU07|TestDU08|TestDU09|TestDU10|TestDU11)' -count=1 -v -timeout=45m 2>&1 | tee archive/du11/server-regression.log
go test ./... -count=1 -v -timeout=60m 2>&1 | tee archive/du11/full-suite.log
git diff --check -- .
```

For the live C04 profile run, start the Go service in Terminal A as usual. In
Terminal B, set `AGENT_URL`, then check the six advertised profiles and create
one session per profile. Keep the returned session ID with its stream log.
These are read-only fixture workflows; `tool-recovery.yaml` intentionally
captures and recovers from its first parser error.

```sh
AGENT_URL=http://127.0.0.1:8001
mkdir -p archive/du11/live
curl -fsS "$AGENT_URL/configs" -o archive/du11/live/configs.json
jq -e '[.[] | .config_ref] | (contains(["log-analysis.yaml","revenue-analysis.yaml","employee-audit.yaml","compaction-stress.yaml","completion-no-workflow.yaml","tool-recovery.yaml"]))' archive/du11/live/configs.json

for CONFIG_REF in log-analysis.yaml revenue-analysis.yaml employee-audit.yaml compaction-stress.yaml completion-no-workflow.yaml tool-recovery.yaml; do
  SAFE_NAME=${CONFIG_REF%.yaml}
  curl -fsS -H 'Content-Type: application/json' \
    -d "$(jq -nc --arg ref "$CONFIG_REF" --arg prompt "Complete the configured workflow, use its validators, and report only the validated result." '{config_ref:$ref,user_prompt:$prompt}')" \
    "$AGENT_URL/sessions" -o "archive/du11/live/$SAFE_NAME-created.json"
  SESSION_ID=$(jq -er '.session.id' "archive/du11/live/$SAFE_NAME-created.json")
  printf '%s %s\n' "$CONFIG_REF" "$SESSION_ID" | tee "archive/du11/live/$SAFE_NAME-session.txt"
  curl -N -sS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
    "$AGENT_URL/sessions/$SESSION_ID/run/stream" \
    | tee "archive/du11/live/$SAFE_NAME-run.sse"
done
```

The live prompt above is deliberately generic and the server executes the
versioned profile instructions. Review tool-result strings in each SSE log;
expected-error diagnostic calls are required in `compaction-stress.yaml` and
`tool-recovery.yaml`. Verify the recovery profile's independent exact-answer
oracles with:

```sh
rg -n '"total": 271017\.77|"valid": true' archive/du11/live/revenue-analysis-run.sse
rg -n 'RECOVERY_OK 10\.0\.0\.5 83' archive/du11/live/tool-recovery-run.sse
```

DU-11 does not claim cross-language session-writer interchangeability. C05
remains unassessed until Go/Python pair tests prove SQL state and fault
recovery compatibility; keep writers on their existing isolated service
paths meanwhile. No throughput sweep is part of DU-11; C03 records row counts
and deterministic fixture fingerprints instead.
