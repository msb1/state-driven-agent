# Universal State-Driven AI Agent Test Cases

This is the shared migration runbook for the deterministic test cases. The Python, Go, Rust, and Java services expose the same REST and Swagger API; select one implementation at a time, and one running server can host every case without a restart.

## Configuration repository and phase workflows

The server reads approved workflow YAML files from `config/` by default. Override that directory only for server administration with `AGENT_CONFIG_ROOT`; callers may supply only a relative `config_ref` inside that root. Session state and the resolved configuration snapshot are stored in PostgreSQL. The default connection is the supplied `elite_rag` URL; deployers can override it with `AGENT_SESSION_DATABASE_URL`.

```text
config/
├── test-case-1.yaml    # Test Case 1
├── test-case-2.yaml    # Test Case 2
└── test-case-3.yaml    # Test Case 3
```

At `POST /sessions`, the server resolves the file, validates it, hashes its bytes, creates an engine from that snapshot, and records `config_ref` and `config_sha256` in the session. Existing sessions cannot change behavior if a new YAML file is later added.

Add a new workflow without restarting the server by placing a versioned YAML file such as `config/customer-onboarding-v1.yaml` in this directory, then creating a session with `"config_ref":"customer-onboarding-v1.yaml"`. Configurations may compose only tools compiled into the server; adding new tool code still requires a deployment.

Workflow YAML may omit `workflow` for legacy single-task behavior. When it declares a workflow graph, a final answer is allowed only after the terminal `complete` node is reached. Each phase records tool evidence and transitions outside compactable raw message history. Current test cases use linear graphs; the schema also reserves evidence and unconditional edges for future branching or skipping.

## Start the server once

Run the shell commands below from the repository root—the directory containing
`agent-python/`, `config/`, and this file.

Generate every deterministic fixture from the repository root:

```sh
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
```

Start one implementation on the same API address and leave it running. The test-case curl commands below are deliberately identical for every server:

```sh
export API_BASE_URL=http://127.0.0.1:8000
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --reload
```

Or start the Go server from the repository root (it uses the same port, REST paths, SSE endpoints, `/docs` Swagger UI, and `/openapi.json` document):

```sh
export API_BASE_URL=http://127.0.0.1:8000
(cd agent-go && AGENT_CONFIG_ROOT=../config PORT=8000 go run ./cmd/server)
```

Or start the Java Spring Boot server from the repository root. It intentionally uses port 8000 too: Java and Python are interchangeable implementations and must not run at the same time.

```sh
export API_BASE_URL=http://127.0.0.1:8000
(cd agent-java && AGENT_CONFIG_ROOT=../config PORT=8000 mvn spring-boot:run)
```

Or start the Rust Axum server from the repository root. It uses the same port, REST paths, SSE endpoints, `/docs` Swagger UI, and `/openapi.json` document as the Python server. Run Rust and Python separately; they are interchangeable implementations for their respective infrastructure.

```sh
export API_BASE_URL=http://127.0.0.1:8000
(cd agent-rust && AGENT_CONFIG_ROOT=../config PORT=8000 cargo run --release)
```

For each choice, verify the same interactive API before running a case:

```sh
curl -sS "${API_BASE_URL}/openapi.json" | jq '.info, (.paths | keys)'
# Open http://127.0.0.1:8000/docs for Swagger UI.
```

List currently available immutable configurations:

```sh
curl -sS "${API_BASE_URL}/configs" | jq
```

The examples use `jq` to capture session IDs. Set `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `MAIN_MODEL` in `.env` or the environment before starting the server.

## Test Case 1 — Broken Log Parser

Create a session using `test-case-1.yaml`:

```sh
SESSION_ID=$(curl -sS -X POST "${API_BASE_URL}/sessions" \
  -H 'content-type: application/json' \
  -d '{"config_ref":"test-case-1.yaml","user_prompt":"Find all IP addresses in the access log that generated an HTTP 500 response, count their occurrences, and report the top offending IP."}' \
  | jq -r '.session.id')
echo "SESSION_ID=${SESSION_ID}"
```

Run and inspect it:

```sh
curl -sS -X POST "${API_BASE_URL}/sessions/${SESSION_ID}/run" \
  -H 'content-type: application/json' -d '{"max_steps":12}' | jq
curl -sS "${API_BASE_URL}/sessions/${SESSION_ID}" | jq
```

Expected result: the validator confirms top IP `10.0.0.5` with `83` HTTP 500 responses. Its single workflow phase reaches `complete` only after `validate_top_offending_ip` returns `valid: true`.

## Test Case 2 — Multi-Table SQL Join

Create a separate session using `test-case-2.yaml`:

```sh
SESSION_ID=$(curl -sS -X POST "${API_BASE_URL}/sessions" \
  -H 'content-type: application/json' \
  -d '{"config_ref":"test-case-2.yaml","user_prompt":"Find the total revenue generated by customers from Chicago in Q1."}' \
  | jq -r '.session.id')
echo "SESSION_ID=${SESSION_ID}"
```

Run and inspect it:

```sh
curl -sS -X POST "${API_BASE_URL}/sessions/${SESSION_ID}/run" \
  -H 'content-type: application/json' -d '{"max_steps":12}' | jq
curl -sS "${API_BASE_URL}/sessions/${SESSION_ID}" | jq
```

Expected result: the agent normalizes Chicago casing and `ID_###` foreign keys, then the validator confirms Q1 revenue `271017.77`.

## Test Case 3 — Multi-Phase HR and Financial Compliance Audit

The Python-only fixture generator creates the dedicated PostgreSQL tables `agent_fixture_access_logs`, `agent_fixture_customers`, `agent_fixture_orders`, and `agent_fixture_employees`. The employee table has 1,000 records (`emp_id` 1–1000). Age corruption includes `NULL` and semicolon-delimited values; salary corruption includes currency strings and `UNKNOWN`; city casing includes `new york`. The valid, case-insensitive New York median is `92500.0`. It uses `AGENT_FIXTURE_DATABASE_URL`, falling back to `AGENT_SESSION_DATABASE_URL`.

Create the Case 3 session:

```sh
SESSION_ID=$(curl -sS -X POST "${API_BASE_URL}/sessions" \
  -H 'content-type: application/json' \
  -d '{"config_ref":"test-case-3.yaml","user_prompt":"Perform a complete 3-Phase Financial Audit on the provided employee dataset:\n\nPhase 1: Identify all rows with corrupted age or salary fields. Log their IDs.\nPhase 2: Normalize the salary field to a standard float. Calculate the exact median salary for valid employees living in New York (case-insensitive).\nPhase 3: Output a clean, final markdown table breaking down the total valid headcount and average age per unique city.\n\nYou must execute your steps incrementally. Do not try to solve all phases in a single script."}' \
  | jq -r '.session.id')
echo "SESSION_ID=${SESSION_ID}"
```

Run the agent, retrieve state, and verify the result:

```sh
curl -sS -X POST "${API_BASE_URL}/sessions/${SESSION_ID}/run" \
  -H 'content-type: application/json' -d '{"max_steps":30}' | jq
curl -sS "${API_BASE_URL}/sessions/${SESSION_ID}" > session-case-3.json
uv run --project agent-python python agent-python/scripts/verify_test_case_3.py \
  session-case-3.json
```

Case 3 has three mandatory workflow phases:

1. Phase 1 may record the currency, `NULL`, and semicolon parser failures, and must log IDs with the robust parser.
2. Phase 2 may record the `UNKNOWN` salary and city-casing metric failures, and must calculate the normalized `92500.0` median.
3. Phase 3 may record city-normalization and invalid-age failures, and must generate the clean markdown table and receive successful audit validation.

The diagnostic failure evidence is optional for live model runs so a model that chooses the correct normalized path is not deadlocked. The deterministic replay deliberately executes every diagnostic path to stress compaction and hybrid reflection.

Progress prose cannot mark this session complete. A successful session has `workflow.active_phase: null`, all three phase IDs in `workflow.completed_phases`, a final Phase 3 markdown answer, and at least three `compaction_events`. A model that refuses outstanding tools reaches the finite step limit with `status: "failed"`; it is never reported as completed.

Each phase also has an explicit tool allow-list. If the model tries to call a later-phase tool early, the engine returns a workflow error and does not execute that tool; the model must finish the active phase first.

## Deterministic Case 3 compaction regression

The live API run tests model behavior and may take a shortcut through the optional diagnostic failures. Use the replay harness for the repeatable regression of workflow progression, compaction, and hybrid reflection. Run it from the repository root after regenerating fixtures if necessary:

```sh
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
uv run --project agent-python python agent-python/scripts/verify_test_case_3.py \
  --replay --output session-case-3-replay.json
```

The first command is safe to repeat and atomically recreates only the dedicated PostgreSQL fixture tables. The second command does not call the LLM server: it uses a fixed decision sequence and a deterministic reflection response, writes a complete session artifact, and exits nonzero if any assertion fails. On success it reports:

```text
✅ Success: Agent generated the final matrix report.
📊 Integration Metric: Compaction was triggered 3 times (minimum 3).
✅ Hybrid reflection preserved required lessons.
✅ All configured workflow phases completed.
```

Inspect the generated artifact when porting the test:

```sh
jq '.workflow, .compaction_events, .final_answer' session-case-3-replay.json
```

A non-Python implementation should reproduce the same replay sequence and assertions. The fixed tool calls are, in order: schema inspection; Phase 1 parsers `naive`, `currency_cleaned`, `null_cleaned`, `robust`; Phase 2 calls `(false,false)`, `(false,true)`, `(true,true)`; Phase 3 reports `(false,true)`, `(true,false)`, `(true,true)`; and final audit validation. It must preserve the original goal, retain workflow evidence outside compactable raw history, perform at least three hybrid compactions, retain lessons for currency, `NULL`, semicolon corruption, `UNKNOWN` salary values, and case-insensitive cities, reach `active_phase: null`, and emit the final Phase 3 markdown table.

## Test Case 4 — Persisted interrupted Case 3 resume

This is the recommended persistence regression: start Case 3, stop at a bounded run limit after Phase 1 has evidence, reload it through a fresh repository/engine (simulating an API restart), and finish it. It verifies the persisted memory, compaction ledger, workflow evidence, configuration snapshot, and step counter rather than only a saved JSON export.

The following is a complete live-API walkthrough. It deliberately uses seven steps for the first run: that is enough for Case 3 to finish Phase 1, while leaving the session non-terminal in Phase 2. Run these commands from the repository root in one terminal. Keep the `SESSION_ID` value; it is the same ID used after the server restart.

First ensure the deterministic fixture exists and start the selected API without an auto-reloader so the restart boundary is unambiguous. Use one of the server commands in [Start the server once](#start-the-server-once); for Java, use the `mvn spring-boot:run` command above.

```sh
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
```

In a second terminal, create a Case 3 session. This is the same goal used by the deterministic replay:

```sh
export SESSION_ID=$(curl -sS -X POST "${API_BASE_URL}/sessions" \
  -H 'content-type: application/json' \
  -d '{"config_ref":"test-case-3.yaml","user_prompt":"Perform a complete 3-Phase Financial Audit on the provided employee dataset:\n\nPhase 1: Identify all rows with corrupted age or salary fields. Log their IDs.\nPhase 2: Normalize the salary field to a standard float. Calculate the exact median salary for valid employees living in New York (case-insensitive).\nPhase 3: Output a clean, final markdown table breaking down the total valid headcount and average age per unique city.\n\nYou must execute your steps incrementally. Do not try to solve all phases in a single script."}' \
  | jq -r '.session.id')
printf 'SESSION_ID=%s\n' "$SESSION_ID"
printf '%s\n' "$SESSION_ID" > /tmp/state-driven-case-4-session-id
```

Run exactly seven bounded steps and inspect the durable state:

```sh
curl -N -X POST "${API_BASE_URL}/sessions/${SESSION_ID}/run/stream" \
  -H 'content-type: application/json' -d '{"max_steps":7}'
curl -sS "${API_BASE_URL}/sessions/${SESSION_ID}" | \
  jq '{status, step_count, config_ref, config_sha256, active_phase: .workflow.active_phase, completed_phases: .workflow.completed_phases, compactions: (.compaction_events | length)}'
```

The inspection should show `status: "failed"` because the intentionally small bound was reached, `step_count: 7`, `completed_phases: ["phase_1_corruption_audit"]`, and `active_phase: "phase_2_salary_median"`. This is an expected recoverable stop, not a data-loss failure. The stream also demonstrates the SSE `status`, `interim_result`, and setup events.

Now stop the API process with `Ctrl-C`. Start it again using the same command. Do not create a new session. In the second terminal, restore the ID if needed and confirm that PostgreSQL, rather than the old Python process, supplies the state:

```sh
export SESSION_ID=$(cat /tmp/state-driven-case-4-session-id)
curl -sS "${API_BASE_URL}/sessions/${SESSION_ID}" | \
  jq '{status, step_count, config_ref, config_sha256, active_phase: .workflow.active_phase, completed_phases: .workflow.completed_phases, memory_messages: (.memory | length), compactions: (.compaction_events | length)}'
```

Finally resume that same ID. The endpoint reactivates the failed session, restores its immutable config and workflow evidence, performs any needed compaction, and continues from Phase 2:

```sh
curl -N -X POST "${API_BASE_URL}/sessions/${SESSION_ID}/resume" \
  -H 'content-type: application/json' -d '{"max_steps":30}'
curl -sS "${API_BASE_URL}/sessions/${SESSION_ID}" | \
  jq '{status, step_count, run_count, active_phase: .workflow.active_phase, completed_phases: .workflow.completed_phases, compactions: (.compaction_events | length), final_answer}'
```

The final inspection should show `status: "complete"`, `active_phase: null`, all three completed phase IDs, and the Phase 3 markdown report. The `config_sha256`, original goal, prior memory, and compaction events remain associated with the same session ID. For a no-network deterministic check of the same persistence boundary, run `uv run --project agent-python python agent-python/scripts/verify_test_case_4.py`; it performs the interrupted run, fresh repository reload, resume, and assertions automatically.

## Test Case 5 — Resume a completed session with a new turn

Case 5 proves the requested completed-session behavior. It first completes Case 3, persists it, reloads it, appends a new user turn during resume, and runs again while retaining the whole prior conversation and immutable workflow/config state.

The following commands perform the live API test from start to finish. Use a separate session ID from Case 4. If the API is already running, keep it running; otherwise start it in one terminal and run the remaining commands in a second terminal.

```sh
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
# Start one implementation using the shared port-8000 command above.
```

In the second terminal, create a fresh Case 3 session and save its ID:

```sh
export CASE5_SESSION_ID=$(curl -sS -X POST "${API_BASE_URL}/sessions" \
  -H 'content-type: application/json' \
  -d '{"config_ref":"test-case-3.yaml","user_prompt":"Perform a complete 3-Phase Financial Audit on the provided employee dataset:\n\nPhase 1: Identify all rows with corrupted age or salary fields. Log their IDs.\nPhase 2: Normalize the salary field to a standard float. Calculate the exact median salary for valid employees living in New York (case-insensitive).\nPhase 3: Output a clean, final markdown table breaking down the total valid headcount and average age per unique city.\n\nYou must execute your steps incrementally. Do not try to solve all phases in a single script."}' \
  | jq -r '.session.id')
printf 'CASE5_SESSION_ID=%s\n' "$CASE5_SESSION_ID"
printf '%s\n' "$CASE5_SESSION_ID" > /tmp/state-driven-case-5-session-id
```

Run the session with the normal JSON endpoint. Use a generous bound so the three workflow phases can finish:

```sh
curl -sS -X POST "${API_BASE_URL}/sessions/${CASE5_SESSION_ID}/run" \
  -H 'content-type: application/json' -d '{"max_steps":30}' | tee /tmp/state-driven-case-5-initial-run.json | jq
```

Confirm that the first run is actually complete before attempting the completed-session resume:

```sh
curl -sS "${API_BASE_URL}/sessions/${CASE5_SESSION_ID}" | \
  jq '{id, status, step_count, run_count, active_phase: .workflow.active_phase, completed_phases: .workflow.completed_phases, config_ref, config_sha256, memory_messages: (.memory | length), compactions: (.compaction_events | length), final_answer}'
```

The required precondition for Case 5 is `status: "complete"`, `active_phase: null`, all three phase IDs in `completed_phases`, and a non-null `final_answer`. If it is `failed`, inspect the model/API error and rerun the same `/run` request with a larger bound before proceeding; `/resume` is also able to recover a failed session, but that would test Case 4 semantics instead.

To prove that the completed state is durable, optionally stop the API with `Ctrl-C`, start it again with the same command, and reload the saved ID:

```sh
export CASE5_SESSION_ID=$(cat /tmp/state-driven-case-5-session-id)
curl -sS "${API_BASE_URL}/sessions/${CASE5_SESSION_ID}" | \
  jq '{id, status, step_count, run_count, active_phase: .workflow.active_phase, completed_phases: .workflow.completed_phases, final_answer}'
```

Now invoke the new resume API with a new user turn. `-N` keeps the SSE events visible as each phase/tool milestone is persisted and emitted:

```sh
curl -N -X POST "${API_BASE_URL}/sessions/${CASE5_SESSION_ID}/resume" \
  -H 'content-type: application/json' \
  -d '{"content":"Briefly restate the validated final report.","max_steps":4}' \
  | tee /tmp/state-driven-case-5-resume-events.txt
```

Finally verify that the same session ID now has a second run, the new user message, preserved workflow evidence, and a new final answer:

```sh
curl -sS "${API_BASE_URL}/sessions/${CASE5_SESSION_ID}" | \
  jq '{id, status, step_count, run_count, active_phase: .workflow.active_phase, completed_phases: .workflow.completed_phases, last_messages: .memory[-3:], final_answer}'
```

Expected results are `status: "complete"`, `run_count: 2`, `active_phase: null`, the original three completed phases, and a `last_messages` entry containing the new user request followed by the assistant response. The response stream should contain `status`, `interim_result` (where applicable), `final_result`, and a terminal status event. If you only want to inspect the stream without saving it, omit the final `tee` command.

```sh
uv run --project agent-python python agent-python/scripts/verify_test_case_5.py
```

`content` is optional. Without it, the model receives the restored state and is asked to continue from where it stopped; with it, the new message is appended before compaction and the next model decision. Both `/run/stream` and `/resume` use `text/event-stream`; clients should render `status`, `interim_result`, `final_result`, and recoverable `error` events as they arrive. Add this to a selected YAML profile for optional setup detail:

```yaml
output:
  verbose_setup: true
```

## Portability contract

A Go, Rust, Java, or TypeScript implementation should preserve these observable contracts:

- resolve a trusted immutable config reference at session creation and store its content hash;
- preserve the original goal and recent raw buffer during compaction;
- keep workflow state, observed evidence, and transition history outside compactable raw memory;
- serialize tool failures as context data;
- permit final completion only after the configured workflow reaches `complete`; and
- apply a finite step limit to stalled sessions.
