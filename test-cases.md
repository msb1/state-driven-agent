# Run the shared agent test cases with curl

This guide is for someone starting with a fresh checkout. It explains prerequisites, how to start each server, and how to run the same complete acceptance suite against Python, Go, Rust, or Java. The progression is Python first, then Go, Rust, and Java. Each port must implement the same APIs and behavior described here before it can pass. Run one server at a time; the commands use port `8081` for every implementation so the HTTP requests stay identical.

## Read this API note first

The required API includes health/config discovery, session create/read/append/run/resume, ordered transcript, run/event/diagnostic/execution/archive history, retention inspection, cancellation, and abandoned-run inspection. These routes are required of **all four upgraded servers**. A missing route, field, or behavior is a conformance failure for that port. Python is the implementation reference; this suite records its acceptance contract for the subsequent Go, Rust, and Java ports.

These are live end-to-end tests. A passing test means the selected server completed that scenario against its configured model service and PostgreSQL fixtures. Results can vary with model/provider behavior; keep failed output and investigate it rather than rerunning until it passes. The suite exercises public API behavior and durable history; SQL transaction/fencing and process crash-injection still require their dedicated integration checks.

The six profile tests below are six separate session executions, one for each YAML file. Run every one in order for each implementation. Keep the same curl terminal open for sections 3–16 so shell variables such as `AGENT_URL`, `OUT`, and `C1`–`C6` remain set. Copy each complete command block, including its `jq -e` assertions. A case passes only when the curl request succeeds and every assertion exits with code 0; an SSE stream ending by itself does not prove success.

## 1. Prerequisites

From the repository root, have the following ready:

- PostgreSQL reachable using the root `.env` value `AGENT_SESSION_DATABASE_URL`, pointing to the dedicated `universal_state` database. Do not start a language server if this variable is missing or points elsewhere.
- The model service running at the root `.env` `OPENAI_BASE_URL`, with the configured `MAIN_MODEL` available. The root `.env` remains the source of model selection; no model name is specified in these commands.
- `curl`, `jq`, `uv`, Go, Rust/Cargo, Java 21, and Maven. These are only needed for the implementation you choose, except `uv` is used for the optional fresh-database initialization below.
- All six shared YAML profiles in the root `config/` directory.

Keep the `.env` private. The Go and Rust services load it from the repository root. Python loads it from the repository. Java imports it for model settings, but its PostgreSQL variables use Java-specific names; the Java startup section below translates the database URL without printing its credentials.

### Fresh database setup (only for a new empty database)

Skip this if `universal_state` already has the current agent tables and fixtures. From the repository root, these are the only commands needed to create the Python reference schema and generate the shared fixture tables:

```sh
uv sync --project agent-python
uv run --project agent-python python agent-python/scripts/initialize_database.py
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
```

Fixture generation replaces the four fixture tables' rows. Run it only when creating/resetting test fixtures, not before every test. This creates the reference PostgreSQL schema and shared fixtures that every port must use; each implementation must preserve the same durable state and API behavior. Java uses the connection-variable conversion below. Stop if a server startup error indicates that it cannot connect to PostgreSQL or find the required tables.

## 2. Start exactly one server

Open a dedicated terminal from the repository root. Use **one** of the following commands. Leave that terminal running. Each command binds the service to port `8081`; if that port is busy, stop the other server or choose another port and update `AGENT_URL` in the curl terminal.

### Python

```sh
uv sync --project agent-python
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --host 127.0.0.1 --port 8081
```

### Go

Go loads the root `.env` while starting from `agent-go` and reads `../config` for profiles.

```sh
cd agent-go
AGENT_CONFIG_ROOT=../config PORT=8081 go run ./cmd/server
```

### Rust

Rust loads the root `.env`; set its config directory and port explicitly.

```sh
cd agent-rust
AGENT_CONFIG_ROOT=../config PORT=8081 cargo run
```

### Java

Java expects `AGENT_SESSION_R2DBC_URL`, `AGENT_SESSION_DB_USER`, and `AGENT_SESSION_DB_PASSWORD`; the shared root `.env` instead defines `AGENT_SESSION_DATABASE_URL`. In a **second terminal**, from the repository root, run this block. It reads `.env`, verifies that the database is `universal_state`, converts the URL, and puts the Java connection settings into the shell environment without printing credentials:

```sh
eval "$(python3 - <<'PY'
from pathlib import Path
from urllib.parse import unquote, urlsplit
import shlex

values = {}
for raw in Path('.env').read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith('#'):
        continue
    key, sep, value = line.partition('=')
    if sep:
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key.strip()] = value

url = urlsplit(values['AGENT_SESSION_DATABASE_URL'])
if url.scheme not in ('postgresql', 'postgres') or url.path.lstrip('/') != 'universal_state':
    raise SystemExit('AGENT_SESSION_DATABASE_URL must point to the universal_state database')
if not url.hostname or not url.username:
    raise SystemExit('Database URL must include a host and username')
port = url.port or 5432
host = '[' + url.hostname + ']' if ':' in url.hostname else url.hostname
settings = {
    'AGENT_SESSION_R2DBC_URL': f'r2dbc:postgresql://{host}:{port}/universal_state',
    'AGENT_SESSION_DB_USER': unquote(url.username),
    'AGENT_SESSION_DB_PASSWORD': unquote(url.password or ''),
    'AGENT_CONFIG_ROOT': '../config',
    'PORT': '8081',
}
for key, value in settings.items():
    print(f'export {key}={shlex.quote(value)}')
PY
)"
cd agent-java
mvn spring-boot:run
```

If the block stops because the database URL is not in `.env`, correct the local `.env` first; do not accept the Java defaults, which point at a different database. If you close the Java terminal, rerun the environment block before restarting Java.

## 3. Prepare the curl terminal

Open another terminal from the repository root. Set `AGENT_URL` to the running server. Choose an evidence label so runs from each language are easy to distinguish. Each run gets a new output directory under `archive/curl-validation/`; sessions are not deleted by this guide.

```sh
export AGENT_URL='http://127.0.0.1:8081'
IMPLEMENTATION='python'  # Change to go, rust, or java for that server.
mkdir -p "$PWD/archive/curl-validation"
export OUT=$(mktemp -d "$PWD/archive/curl-validation/${IMPLEMENTATION}.XXXXXX")
printf '%s\n' "$OUT" > "$PWD/archive/curl-validation/current-run.txt"
printf 'Saving responses under %s\n' "$OUT"
set -o pipefail
```

Run the two preflight requests. Both must pass before continuing. The health response should report `status: ok`; the profile list must include all six config refs.

```sh
curl -fsS "$AGENT_URL/health" -o "$OUT/health.json"
jq -e '.status == "ok"' "$OUT/health.json"
curl -fsS "$AGENT_URL/configs" -o "$OUT/configs.json"
jq -e '[.[].config_ref] | contains([
  "log-analysis.yaml", "revenue-analysis.yaml", "employee-audit.yaml",
  "compaction-stress.yaml", "completion-no-workflow.yaml", "tool-recovery.yaml"
])' "$OUT/configs.json"
```

For each numbered case below, run the commands in order. The create request saves its JSON response; set the case ID from that file; run the agent; fetch the persisted session; then evaluate the displayed `jq -e` assertion. A `true` result and exit code 0 is a pass. A false result, curl error, missing field, or nonzero exit code is a failure. The SSE `tee` output shows progress as it arrives.

## 4. Test 1 — log analysis (`log-analysis.yaml`)

**Goal:** Robustly analyze the fixture and identify the most frequent HTTP 500 IP. The successful response must include `10.0.0.5` and `83`, and the session must finish with `status=complete`.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"log-analysis.yaml","user_prompt":"Inspect and robustly analyze the HTTP access log. Validate the most frequent HTTP 500 IP and its count. In your final answer, state the IP and count."}' \
  "$AGENT_URL/sessions" -o "$OUT/01-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "log-analysis.yaml"' "$OUT/01-created.json"
C1=$(jq -er '.session.id' "$OUT/01-created.json")
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":30}' \
  "$AGENT_URL/sessions/$C1/run/stream" | tee "$OUT/01-run.sse"
curl -fsS "$AGENT_URL/sessions/$C1" -o "$OUT/01-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.final_answer | type == "string" and contains("10.0.0.5") and contains("83")) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[]; .function.name == "inspect_log")) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "analyze_500_ips" and (.function.arguments | fromjson | .robust_parser == true))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "validate_top_offending_ip" and (.function.arguments | fromjson | .ip == "10.0.0.5" and .count == 83))) and
  any(.memory[]; .role == "tool" and .name == "analyze_500_ips" and
    (.content | contains("10.0.0.5") and contains("83") and contains("malformed_records_ignored"))) and
  any(.memory[]; .role == "tool" and .name == "validate_top_offending_ip" and (.content | contains("\"valid\": true")))
' "$OUT/01-state.json"
```

**Required evidence:** the agent inspected the log, used the robust parser, proposed `10.0.0.5` with count `83`, and received `valid: true` from the validator. The validator is the pass gate; answer text alone is insufficient.

## 5. Test 2 — normalized Chicago revenue (`revenue-analysis.yaml`)

**Goal:** Calculate and validate normalized Chicago Q1 2026 revenue. The final response must include the value `271017.77`, either as `271017.77` or formatted as `$271,017.77`, and the session must finish with `status=complete`.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"revenue-analysis.yaml","user_prompt":"Inspect the schema. Calculate Chicago revenue for Q1 2026, normalizing city casing and foreign keys. Validate the result, then report the validated total."}' \
  "$AGENT_URL/sessions" -o "$OUT/02-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "revenue-analysis.yaml"' "$OUT/02-created.json"
C2=$(jq -er '.session.id' "$OUT/02-created.json")
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":30}' \
  "$AGENT_URL/sessions/$C2/run/stream" | tee "$OUT/02-run.sse"
curl -fsS "$AGENT_URL/sessions/$C2" -o "$OUT/02-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.final_answer | type == "string" and (contains("271017.77") or contains("271,017.77"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[]; .function.name == "inspect_sql_schema")) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "query_chicago_q1_revenue" and
    (.function.arguments | fromjson | .normalize_keys == true and .normalize_city == true))) and
  any(.memory[]; .role == "tool" and .name == "query_chicago_q1_revenue" and (.content | contains("271017.77"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "validate_chicago_q1_revenue" and (.function.arguments | fromjson | .total == 271017.77))) and
  any(.memory[]; .role == "tool" and .name == "validate_chicago_q1_revenue" and (.content | contains("\"valid\": true")))
' "$OUT/02-state.json"
```

This expected value assumes the canonical generated fixture rows. It is not an oracle for arbitrary customer data.

**Required evidence:** schema inspection, both normalizations enabled on the revenue query, result `271017.77`, and validator output `valid: true`.

## 6. Test 3 — employee audit (`employee-audit.yaml`)

**Goal:** Complete the corruption audit, valid New York salary median, and city report. Require the final answer to begin with `Phase 3`, contain all four city rows, and report median salary `92500`.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"employee-audit.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$OUT/03-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "employee-audit.yaml"' "$OUT/03-created.json"
C3=$(jq -er '.session.id' "$OUT/03-created.json")
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C3/run/stream" | tee "$OUT/03-run.sse"
curl -fsS "$AGENT_URL/sessions/$C3" -o "$OUT/03-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.workflow.completed_phases | sort) == (["phase_1_corruption_audit","phase_2_salary_median","phase_3_city_report"] | sort) and
  (.final_answer | startswith("Phase 3\n")) and
  ([.final_answer | split("\n")[] | gsub("[ \\t\\r]"; "") | select(test("^\\|(Austin|Chicago|NewYork|Seattle)\\|"))] | sort) ==
    (["|Austin|199|42.62|","|Chicago|181|42.01|","|NewYork|329|43.02|","|Seattle|183|43.46|"] | sort) and
  (.final_answer | test("New York median salary:[ \\t]*92500(\\.0+)?([ \\t\\r\\n]|$)"))
' "$OUT/03-state.json"
```

Expected table values are counts and average ages from the generated fixture, excluding employees with invalid ages:

| City | Count | Average age |
| --- | ---: | ---: |
| Austin | 199 | 42.62 |
| Chicago | 181 | 42.01 |
| New York | 329 | 43.02 |
| Seattle | 183 | 43.46 |

## 7. Test 4 — compaction stress (`compaction-stress.yaml`)

**Goal:** Complete the same employee audit under a profile that requires diagnostics and compaction. Require the same correct report, all eleven workflow evidence records, and at least one accepted compaction event. This checks that compaction preserves the evidence needed to finish.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"compaction-stress.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$OUT/04-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "compaction-stress.yaml"' "$OUT/04-created.json"
C4=$(jq -er '.session.id' "$OUT/04-created.json")
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C4/run/stream" | tee "$OUT/04-run.sse"
curl -fsS "$AGENT_URL/sessions/$C4" -o "$OUT/04-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and
  (.workflow.observed_evidence | length) == 11 and
  (.compaction_events | length) > 0 and
  (.final_answer | startswith("Phase 3\n")) and
  ([.final_answer | split("\n")[] | gsub("[ \\t\\r]"; "") | select(test("^\\|(Austin|Chicago|NewYork|Seattle)\\|"))] | sort) ==
    (["|Austin|199|42.62|","|Chicago|181|42.01|","|NewYork|329|43.02|","|Seattle|183|43.46|"] | sort) and
  (.final_answer | test("New York median salary:[ \\t]*92500(\\.0+)?([ \\t\\r\\n]|$)"))
' "$OUT/04-state.json"
```

No fixed summary count or latency is required. A finished correct result and retained workflow evidence are required. If one implementation omits `observed_evidence` or `compaction_events`, record a parity failure; do not remove the assertion for that implementation.

## 8. Test 5 — workflow-free completion and continuation (`completion-no-workflow.yaml`)

**Goal:** Verify direct completion through `POST /run`, then use `POST /resume` with a new user turn. The first answer must be exactly `CURL_OK`, and the second exactly `CURL_NEXT`. The original goal and config hash must remain unchanged, and the run count must increase by one.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"completion-no-workflow.yaml","user_prompt":"Reply with exactly CURL_OK. Do not call tools."}' \
  "$AGENT_URL/sessions" -o "$OUT/05-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "completion-no-workflow.yaml"' "$OUT/05-created.json"
C5=$(jq -er '.session.id' "$OUT/05-created.json")
curl -fsS -H 'Content-Type: application/json' \
  -d '{"content":"The required response is still exactly CURL_OK."}' \
  "$AGENT_URL/sessions/$C5/messages" -o "$OUT/05-message.json"
jq -e '.session.status == "active" and .session.goal == "Reply with exactly CURL_OK. Do not call tools." and
  any(.session.memory[]; .role == "user" and .content == "The required response is still exactly CURL_OK.")' \
  "$OUT/05-message.json"
curl -fsS -H 'Content-Type: application/json' -d '{"max_steps":5}' \
  "$AGENT_URL/sessions/$C5/run" -o "$OUT/05-run.json"
jq -e '.session.status == "complete" and .session.run_status == "complete" and .session.final_answer == "CURL_OK"' "$OUT/05-run.json"
curl -fsS "$AGENT_URL/sessions/$C5" -o "$OUT/05-before-resume.json"
curl -N -fsS -H 'Content-Type: application/json' \
  -d '{"content":"Now reply with exactly CURL_NEXT. Do not call tools.","max_steps":5}' \
  "$AGENT_URL/sessions/$C5/resume" | tee "$OUT/05-resume.sse"
curl -fsS "$AGENT_URL/sessions/$C5" -o "$OUT/05-after-resume.json"
jq -e --slurpfile before "$OUT/05-before-resume.json" '
  .status == "complete" and .run_status == "complete" and .final_answer == "CURL_NEXT" and
  .id == $before[0].id and .goal == $before[0].goal and
  .config_sha256 == $before[0].config_sha256 and
  .run_count == ($before[0].run_count + 1)
' "$OUT/05-after-resume.json"
```

## 9. Test 6 — tool error, bounded stop, and resume (`tool-recovery.yaml`)

**Goal:** Stop a run after one model decision, confirm the bounded run did not complete, then resume. The final response must be exactly `RECOVERY_OK 10.0.0.5 83`; all three phases must be complete and the error from the naive parser must appear in session memory. After the workflow reaches `complete`, any further tool call is rejected before execution. The profile's exact-output rule rejects prose or formatting around the required answer.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"tool-recovery.yaml","user_prompt":"Follow the failure, robust retry, and validation workflow. Return the exact recovery answer required by the profile."}' \
  "$AGENT_URL/sessions" -o "$OUT/06-created.json"
jq -e '.session.id and .session.status == "active" and .session.config_ref == "tool-recovery.yaml"' "$OUT/06-created.json"
C6=$(jq -er '.session.id' "$OUT/06-created.json")
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":1}' \
  "$AGENT_URL/sessions/$C6/run/stream" | tee "$OUT/06-first-run.sse"
curl -fsS "$AGENT_URL/sessions/$C6" -o "$OUT/06-after-stop.json"
jq -e '.status == "failed" and .run_status == "failed" and .step_count <= 1 and .final_answer != "RECOVERY_OK 10.0.0.5 83"' "$OUT/06-after-stop.json"
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":30}' \
  "$AGENT_URL/sessions/$C6/resume" | tee "$OUT/06-resume.sse"
curl -fsS "$AGENT_URL/sessions/$C6" -o "$OUT/06-final-state.json"
jq -e '
  .status == "complete" and .run_status == "complete" and .final_answer == "RECOVERY_OK 10.0.0.5 83" and
  (.workflow.completed_phases | sort) == (["diagnose_failure","robust_retry","validate_result"] | sort) and
  any(.memory[]; .role == "tool" and .name == "analyze_500_ips" and (.content | contains("ERROR: naive parser failed"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "analyze_500_ips" and (.function.arguments | fromjson | .robust_parser == false))) and
  any(.memory[]; .role == "tool" and .name == "analyze_500_ips" and
    (.content | contains("10.0.0.5") and contains("83"))) and
  any(.memory[]; .tool_calls != null and any(.tool_calls[];
    .function.name == "validate_top_offending_ip" and (.function.arguments | fromjson | .ip == "10.0.0.5" and .count == 83))) and
  any(.memory[]; .role == "tool" and .name == "validate_top_offending_ip" and (.content | contains("\"valid\": true")))
' "$OUT/06-final-state.json"
curl -fsS "$AGENT_URL/sessions/$C6/executions?limit=100" -o "$OUT/06-executions.json"
jq -e '
  (.executions | length) == 3 and
  ([.executions[] | select(.tool == "analyze_500_ips")] | length) == 2 and
  ([.executions[] | select(.tool == "validate_top_offending_ip")] | length) == 1 and
  any(.executions[]; .tool == "analyze_500_ips" and .status == "failed" and
    (.result | contains("ERROR: naive parser failed"))) and
  any(.executions[]; .tool == "analyze_500_ips" and .status == "succeeded" and
    (.result | contains("10.0.0.5") and contains("83"))) and
  any(.executions[]; .tool == "validate_top_offending_ip" and .status == "succeeded" and
    (.result | contains("\"valid\": true")))
' "$OUT/06-executions.json"
```

The first decision may be a correction to an invalid model response rather than the naive-parser tool call. The success criterion is that the resumed session contains the expected tool error, robust retry, validator-confirmed result, exact final answer and complete status. The durable execution check requires exactly the three intended tool invocations; rejected post-completion calls must not create execution records.

## 10. Save diagnostic evidence when a profile test fails

Do not delete or reset the failed session. Set `CASE_ID` to its saved ID (`C1` through `C6`) and save the state and durable transcript. This distinguishes an agent failure from an HTTP, model-service, fixture, or persistence failure:

```sh
CASE_ID="$C1"  # Replace C1 with the failed case variable: C2, C3, C4, C5, or C6.
curl -sS -w '\nHTTP %{http_code}\n' "$AGENT_URL/sessions/$CASE_ID" \
  -o "$OUT/failure-state.json"
jq '{id,status,run_status,run_id,run_count,step_count,revision,config_ref,config_sha256,
     active_phase:.workflow.active_phase,completed_phases:.workflow.completed_phases,
     last_error,final_answer}' "$OUT/failure-state.json"
curl -sS -w '\nHTTP %{http_code}\n' \
  "$AGENT_URL/sessions/$CASE_ID/transcript?after_sequence=0&limit=1000" \
  -o "$OUT/failure-transcript.json"
jq '{history_complete,through_sequence,entries}' "$OUT/failure-transcript.json"
curl -sS -w '\nHTTP %{http_code}\n' "$AGENT_URL/sessions/$CASE_ID/executions?limit=100" \
  -o "$OUT/failure-executions.json"
jq '{executions,next_after}' "$OUT/failure-executions.json"
```

For the compaction profile (`C4`), also save the three diagnostic families:

```sh
for FAMILY in compaction_attempts compaction_events compaction_decisions; do
  curl -sS -w '\nHTTP %{http_code}\n' \
    "$AGENT_URL/sessions/$C4/diagnostics/$FAMILY?after=0&limit=1000" \
    -o "$OUT/failure-$FAMILY.json"
done
```

Review these files together with the corresponding `*.sse` stream and `curl` status. `status` is the session outcome; `run_status` is the lifecycle outcome. For example, a step limit intentionally leaves a recoverable failed session, while a completed session must have `status: complete` and `run_status: complete`. Preserve the full `$OUT` directory when reporting a failure.

## 11. API and restart checks

Run these against the same backend after tests 1–6.

### Reject an invalid request

A reference to a profile outside the approved config folder must be rejected with HTTP 422 by the shared API. Save the response body and status for review:

```sh
HTTP_CODE=$(curl -sS -o "$OUT/10-invalid-config.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' \
  -d '{"config_ref":"../.env","user_prompt":"This request must be rejected."}' \
  "$AGENT_URL/sessions")
printf 'HTTP %s\n' "$HTTP_CODE"
[ "$HTTP_CODE" = 422 ] && printf 'PASS invalid config path rejected\n'
```

### Verify state survives a real server restart

Save a completed session's identity and answer, stop the server with Ctrl-C in its server terminal, restart the **same implementation** using its command in section 2, and wait until health responds. Then retrieve the same session. The id, goal, config hash, completed status and final answer must match. This proves completed-state hydration across a process restart; it does not test a process crash during a tool call.

```sh
curl -fsS "$AGENT_URL/sessions/$C3" -o "$OUT/10-before-restart.json"
jq '{id,goal,config_ref,config_sha256,status,final_answer}' "$OUT/10-before-restart.json" \
  > "$OUT/10-before-restart-summary.json"
```

After restarting the server:

```sh
curl -fsS "$AGENT_URL/health" | jq -e '.status == "ok"'
curl -fsS "$AGENT_URL/sessions/$C3" -o "$OUT/10-after-restart.json"
jq '{id,goal,config_ref,config_sha256,status,final_answer}' "$OUT/10-after-restart.json" \
  > "$OUT/10-after-restart-summary.json"
diff -u "$OUT/10-before-restart-summary.json" "$OUT/10-after-restart-summary.json"
```

## 12. Test transcript pagination and stable message IDs

Use the completed continuation session from Test 5. Transcript pages use a sequence cursor and a fixed `through_sequence` watermark. The first two entries must have consecutive sequence numbers and different stable message IDs.

```sh
curl -fsS "$AGENT_URL/sessions/$C5/transcript?after_sequence=0&limit=1" \
  -o "$OUT/11-transcript-page-1.json"
jq -e '.history_complete == true and (.entries | length) == 1 and
  .entries[0].sequence == 1 and .entries[0].message_id and
  .through_sequence >= 2 and .next_after_sequence == 1' "$OUT/11-transcript-page-1.json"
TRANSCRIPT_THROUGH=$(jq -er '.through_sequence' "$OUT/11-transcript-page-1.json")
curl -fsS "$AGENT_URL/sessions/$C5/transcript?after_sequence=1&limit=1&through_sequence=$TRANSCRIPT_THROUGH" \
  -o "$OUT/11-transcript-page-2.json"
jq -e '.entries | length == 1 and .[0].sequence == 2 and .[0].message_id' \
  "$OUT/11-transcript-page-2.json"
jq -e --slurpfile first "$OUT/11-transcript-page-1.json" \
  '.entries[0].message_id != $first[0].entries[0].message_id and
   .entries[0].sequence > $first[0].entries[0].sequence' \
  "$OUT/11-transcript-page-2.json"
```

**Pass:** transcript history is complete, the watermark covers both pages, sequence values are consecutive, and message IDs remain distinct.

## 13. Test event/run history pagination

Test 5 creates two completed runs (`/run`, then `/resume`). These commands verify stable cursors, ordered pages, and distinct run identities.

```sh
curl -fsS "$AGENT_URL/sessions/$C5/events?after=0&limit=1" -o "$OUT/12-events-page-1.json"
jq -e '.through >= 1 and (.entries | length) == 1 and
  .entries[0].event_id > 0 and .next_after == .entries[0].event_id' "$OUT/12-events-page-1.json"
EVENT_THROUGH=$(jq -er '.through' "$OUT/12-events-page-1.json")
EVENT_AFTER=$(jq -er '.next_after' "$OUT/12-events-page-1.json")
curl -fsS "$AGENT_URL/sessions/$C5/events?after=$EVENT_AFTER&limit=1&through=$EVENT_THROUGH" \
  -o "$OUT/12-events-page-2.json"
jq -e --argjson after "$EVENT_AFTER" '.entries | length == 1 and .[0].event_id > $after' \
  "$OUT/12-events-page-2.json"

curl -fsS "$AGENT_URL/sessions/$C5/runs?limit=1" -o "$OUT/12-runs-page-1.json"
jq -e '.runs | length == 1 and .[0].run_id and .[0].fence >= 1 and .[0].status == "complete"' \
  "$OUT/12-runs-page-1.json"
RUN_AFTER=$(jq -er '.next_after' "$OUT/12-runs-page-1.json")
curl -fsS "$AGENT_URL/sessions/$C5/runs?limit=1&after=$RUN_AFTER" -o "$OUT/12-runs-page-2.json"
jq -e --arg previous "$RUN_AFTER" '.runs | length == 1 and .[0].run_id != $previous and .[0].status == "complete"' \
  "$OUT/12-runs-page-2.json"
```

**Pass:** event pagination advances within the original watermark. Both run pages return distinct completed run IDs and positive fencing numbers; no record is duplicated or skipped.

## 14. Test execution and compaction diagnostics, archive and retention APIs

Test 4 should generate multiple tool executions and compaction diagnostic entries. These endpoints return bounded pages, independent of the active prompt history.

```sh
curl -fsS "$AGENT_URL/sessions/$C4/executions?limit=1" -o "$OUT/13-executions-page-1.json"
jq -e '(.executions | length) == 1 and .executions[0].execution_id and
  .executions[0].run_id and .executions[0].call_id and
  (.executions[0].status == "succeeded" or .executions[0].status == "failed") and
  .next_after == .executions[0].execution_id' \
  "$OUT/13-executions-page-1.json"
EXECUTION_AFTER=$(jq -er '.next_after' "$OUT/13-executions-page-1.json")
curl -fsS "$AGENT_URL/sessions/$C4/executions?limit=1&after=$EXECUTION_AFTER" \
  -o "$OUT/13-executions-page-2.json"
jq -e --arg previous "$EXECUTION_AFTER" '.executions | length == 1 and
  .[0].execution_id != $previous and
  (.[0].status == "succeeded" or .[0].status == "failed")' \
  "$OUT/13-executions-page-2.json"

curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_attempts?limit=1&after=0" \
  -o "$OUT/13-attempts-page-1.json"
jq -e '.total_recorded > 0 and .through >= 1 and (.entries | length) == 1 and
  .entries[0].ordinal == 1 and .next_after == 1' "$OUT/13-attempts-page-1.json"
DIAGNOSTICS_THROUGH=$(jq -er '.through' "$OUT/13-attempts-page-1.json")
curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_attempts?limit=1&after=1&through=$DIAGNOSTICS_THROUGH" \
  -o "$OUT/13-attempts-page-2.json"
jq -e '.entries | length == 1 and .[0].ordinal == 2' "$OUT/13-attempts-page-2.json"
curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_events?limit=10&after=0" \
  -o "$OUT/13-compaction-events.json"
curl -fsS "$AGENT_URL/sessions/$C4/diagnostics/compaction_decisions?limit=10&after=0" \
  -o "$OUT/13-compaction-decisions.json"
jq -e '.total_recorded > 0 and (.entries | length) > 0' "$OUT/13-compaction-events.json"
jq -e '.total_recorded > 0 and (.entries | length) > 0' "$OUT/13-compaction-decisions.json"

curl -fsS "$AGENT_URL/sessions/$C4/archives?limit=10" -o "$OUT/13-archives.json"
jq -e '.archives | type == "array"' "$OUT/13-archives.json"
curl -fsS "$AGENT_URL/sessions/$C4/retention" -o "$OUT/13-retention.json"
jq -e '.default_policy == "keep_all" and .eligible == true and
  .unresolved_executions == 0 and .running_lifecycles == 0 and
  (.protected_tables | length) > 0 and (.prunable_tables | length) > 0' \
  "$OUT/13-retention.json"
curl -fsS "$AGENT_URL/runs/abandoned?limit=10" -o "$OUT/13-abandoned-runs.json"
jq -e '.runs | type == "array"' "$OUT/13-abandoned-runs.json"
```

**Pass:** execution/event/diagnostic cursors advance; the compaction stress session has event, attempt, and decision entries; archive and abandoned-run APIs return arrays; the completed stress session is eligible under the keep-all default with no running lifecycle or unresolved execution. Do not prune or delete data in this test suite.

## 15. Test actual SSE socket disconnect and resume

This test closes curl's client socket after two seconds while leaving the server and model service running. Curl exit code `28` is expected for this deliberate timeout. The run must become `cancelled`, remain stable for three seconds, then finish on `/resume` with all eleven evidence records and a higher run fence.

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"compaction-stress.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$OUT/14-created.json"
C14=$(jq -er '.session.id' "$OUT/14-created.json")
curl -N -sS --max-time 2 -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C14/run/stream" 2> "$OUT/14-disconnect.stderr" \
  | tee "$OUT/14-disconnect.sse"
DISCONNECT_CODE=$?
printf '%s\n' "$DISCONNECT_CODE" > "$OUT/14-disconnect-curl-code.txt"
[ "$DISCONNECT_CODE" = 28 ] && printf 'PASS deliberate client socket closure\n'

POLL=0
while [ "$POLL" -lt 120 ]; do
  curl -fsS "$AGENT_URL/sessions/$C14" -o "$OUT/14-cancelled-state.json" || break
  [ "$(jq -r '.run_status' "$OUT/14-cancelled-state.json")" = 'cancelled' ] && break
  sleep 1
  POLL=$((POLL + 1))
done
jq -e '.run_status == "cancelled" and .last_error.code == "run_cancelled"' "$OUT/14-cancelled-state.json"
curl -fsS "$AGENT_URL/sessions/$C14/runs?limit=100" -o "$OUT/14-cancelled-runs.json"
curl -fsS "$AGENT_URL/sessions/$C14/transcript?limit=1000&after_sequence=0" \
  -o "$OUT/14-before-resume-transcript.json"
cp "$OUT/14-cancelled-state.json" "$OUT/14-before-resume-state.json"
sleep 3
curl -fsS "$AGENT_URL/sessions/$C14" -o "$OUT/14-stable-state.json"
diff -u "$OUT/14-before-resume-state.json" "$OUT/14-stable-state.json"
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C14/resume" | tee "$OUT/14-resume.sse"
curl -fsS "$AGENT_URL/sessions/$C14" -o "$OUT/14-resumed-state.json"
jq -e '.status == "complete" and .run_status == "complete" and
  .workflow.active_phase == null and (.workflow.observed_evidence | length) == 11 and
  (.final_answer | startswith("Phase 3\n"))' "$OUT/14-resumed-state.json"
jq -e '
  ([.final_answer | split("\n")[] | gsub("[ \\t\\r]"; "") | select(test("^\\|(Austin|Chicago|NewYork|Seattle)\\|"))] | sort) ==
    (["|Austin|199|42.62|", "|Chicago|181|42.01|", "|NewYork|329|43.02|", "|Seattle|183|43.46|"] | sort) and
  (.final_answer | test("New York median salary:[ \\t]*92500(\\.0+)?([ \\t\\r\\n]|$)"))
' "$OUT/14-resumed-state.json"
curl -fsS "$AGENT_URL/sessions/$C14/runs?limit=100" -o "$OUT/14-resumed-runs.json"
jq -e --slurpfile cancelled "$OUT/14-cancelled-state.json" \
  --slurpfile resumed "$OUT/14-resumed-state.json" '
  [.runs[] | select(.run_id == $cancelled[0].run_id)] as $old |
  [.runs[] | select(.run_id == $resumed[0].run_id)] as $new |
  ($old | length) == 1 and ($new | length) == 1 and
  $old[0].status == "cancelled" and $new[0].status == "complete" and
  $new[0].fence > $old[0].fence
' "$OUT/14-resumed-runs.json"
curl -fsS "$AGENT_URL/sessions/$C14/transcript?limit=1000&after_sequence=0" \
  -o "$OUT/14-after-resume-transcript.json"
jq -e --slurpfile before "$OUT/14-before-resume-transcript.json" '
  .history_complete == true and
  (.entries[0:($before[0].entries | length)] == $before[0].entries)
' "$OUT/14-after-resume-transcript.json"
```

If the task finishes before curl closes, or the server does not report `cancelled`, keep the output and mark this trial failed/unexercised. Do not treat curl's timeout alone as proof of server cancellation.

## 16. Test `/cancel`, competing ownership, and continuation across workers

This test uses two server processes on the same PostgreSQL database. Keep the first on port `8081`; start a second copy of the same implementation on port `8082` using the same environment and the matching command below. Keep both server terminals open. Two processes are required to test database ownership/fencing across workers.

| Implementation | Second server command (run from repository root unless shown) |
| --- | --- |
| Python | `uv run --project agent-python uvicorn agent_python.api:app --app-dir agent-python/src --host 127.0.0.1 --port 8082` |
| Go | `cd agent-go` then `AGENT_CONFIG_ROOT=../config PORT=8082 go run ./cmd/server` |
| Rust | `cd agent-rust` then `AGENT_CONFIG_ROOT=../config PORT=8082 cargo run` |
| Java | In the Java server terminal, set `PORT=8082` after the environment block, then run `mvn spring-boot:run` from `agent-java`. |

In terminal A, with `AGENT_URL` still set to port `8081`:

```sh
curl -fsS -H 'Content-Type: application/json' \
  -d '{"config_ref":"compaction-stress.yaml","user_prompt":"Audit employee corruption, calculate the normalized New York median valid salary, and generate and validate the clean city report. Complete all workflow phases. In the final answer, start with Phase 3, include the actual city table, and include the labeled New York median salary."}' \
  "$AGENT_URL/sessions" -o "$OUT/15-created.json"
C15=$(jq -er '.session.id' "$OUT/15-created.json")
printf 'Session ID: %s\nEvidence directory: %s\n' "$C15" "$OUT"
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C15/run/stream" | tee "$OUT/15-owner.sse"
```

While terminal A is still streaming, run these commands in terminal B from the repository root. The saved path and session ID are read from terminal A's evidence files, so no values need to be copied or edited.

```sh
export AGENT_URL='http://127.0.0.1:8082'
OUT=$(cat "$PWD/archive/curl-validation/current-run.txt")
C15=$(jq -er '.session.id' "$OUT/15-created.json")
curl -fsS "$AGENT_URL/sessions/$C15" -o "$OUT/15-running-state.json"
jq -e '.run_status == "running" and .run_id' "$OUT/15-running-state.json"
HTTP_CODE=$(curl -sS -o "$OUT/15-competing-run.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' -d '{"max_steps":1}' "$AGENT_URL/sessions/$C15/run")
[ "$HTTP_CODE" = 409 ] && printf 'PASS competing run rejected\n'
HTTP_CODE=$(curl -sS -o "$OUT/15-competing-message.json" -w '%{http_code}' \
  -H 'Content-Type: application/json' -d '{"content":"Concurrent input must not be appended."}' \
  "$AGENT_URL/sessions/$C15/messages")
[ "$HTTP_CODE" = 409 ] && printf 'PASS competing message rejected\n'
curl -fsS -X POST "$AGENT_URL/sessions/$C15/cancel" -o "$OUT/15-cancel-response.json"
jq -e --slurpfile running "$OUT/15-running-state.json" \
  '.cancel_requested_for | index($running[0].run_id) != null' "$OUT/15-cancel-response.json"
```

After terminal A's stream ends, switch terminal A to port `8082` and continue:

```sh
export AGENT_URL='http://127.0.0.1:8082'
curl -fsS "$AGENT_URL/sessions/$C15" -o "$OUT/15-cancelled-state.json"
jq -e '.run_status == "cancelled" and .last_error.code == "run_cancelled"' "$OUT/15-cancelled-state.json"
curl -N -fsS -H 'Content-Type: application/json' -d '{"max_steps":40}' \
  "$AGENT_URL/sessions/$C15/resume" | tee "$OUT/15-resume.sse"
curl -fsS "$AGENT_URL/sessions/$C15" -o "$OUT/15-final-state.json"
jq -e --slurpfile old "$OUT/15-cancelled-state.json" \
  '.status == "complete" and .run_status == "complete" and
   .run_id != $old[0].run_id and .goal == $old[0].goal and
   .config_sha256 == $old[0].config_sha256 and
   .workflow.active_phase == null and (.workflow.observed_evidence | length) == 11' \
  "$OUT/15-final-state.json"
```

**Pass:** the second process sees the active run, rejects competing run/input with HTTP 409, the cancel response names that run, and resume completes with unchanged goal/configuration and all evidence. Stop both servers after this test.

## 17. Repeat the full suite for all four languages

Run sections 1–16 first against Python, then Go, Rust, and Java. For each language, stop the previous server, start the selected implementation on `8081`, start its second process on `8082` for Test 16, and run the same curl commands and assertions. Change only `IMPLEMENTATION` and create a fresh evidence directory:

```sh
IMPLEMENTATION='go'  # Repeat with python, rust, and java in separate full runs.
export OUT=$(mktemp -d "$PWD/archive/curl-validation/${IMPLEMENTATION}.XXXXXX")
printf '%s\n' "$OUT" > "$PWD/archive/curl-validation/current-run.txt"
printf 'Saving responses under %s\n' "$OUT"
```

Every route and assertion applies to every implementation. A missing endpoint is a failed porting requirement, not a test to skip. Keep all evidence including failures. Compare route availability, pass/fail, profile identity, outputs, workflow evidence, history pagination, ownership/cancellation, and restart behavior. Do not compare generated UUIDs between different language runs.

## 18. Required API surface

| Method and route | Acceptance coverage |
| --- | --- |
| `GET /health`, `GET /configs` | Preflight health and all six profile refs |
| `POST /sessions`, `GET /sessions/{id}` | All six scenarios and restart hydration |
| `POST /sessions/{id}/messages` | Test 5 appends a turn while preserving the original goal |
| `POST /sessions/{id}/run` | Test 5 direct completion |
| `POST /sessions/{id}/run/stream` | Workflow scenarios, active run and client disconnect |
| `POST /sessions/{id}/resume` | New-turn and cancelled-run continuation |
| `GET /sessions/{id}/transcript` | Test 12 stable sequence/ID pagination |
| `GET /sessions/{id}/events`, `GET /sessions/{id}/runs` | Test 13 event/run pagination |
| `GET /sessions/{id}/executions` | Tests 10 and 14 durable tool-intent/result history |
| `GET /sessions/{id}/diagnostics/{family}` | Tests 10 and 14 compaction event/attempt/decision pages |
| `GET /sessions/{id}/archives`, `GET /sessions/{id}/retention` | Test 14 archive/retention inspection |
| `GET /runs/abandoned` | Test 14 abandoned-run inspection |
| `POST /sessions/{id}/cancel` | Test 16 cancellation request and acknowledgement |

The servers also publish API documentation (`/docs` and `/openapi.json`; `/redoc` where provided). Documentation pages do not replace the behavioral assertions above. This suite defines the expected endpoint set for every upgraded port; qualification is complete only after the same full suite passes against Python, Go, Rust, and Java.

Keep every `$OUT` folder, including failed requests, SSE logs and state snapshots. Do not save `.env`, print database credentials, or delete sessions during routine qualification. The historic benchmark scripts and reports remain preserved in [archive](archive/README.md).
