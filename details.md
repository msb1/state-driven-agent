# What agent-python implements

This document describes the current working-tree implementation as reviewed on October 19, 2026. It covers the original Python agent, the subsequent durable reference implementation, and the latest observable-contract updates. It is based on runtime source, shared YAML profiles, current architecture/operations documentation, and the original implementation commit (`7a097fd`). No database, model, or qualification runs were performed to prepare this description.

The current authority is [contract.md](contract.md), **observable contract 1.1.0**, using **storage schema 5**. The archived reference is named `python-1.0.0`; the FastAPI application advertises version `0.5.0`, and Python package metadata advertises `0.1.0`. These identify different things. The Python README's reference to contract 1.0.0 is older than the current contract and code. [design_update.md](design_update.md) describes the corresponding upgrade work for other language implementations; it does not establish their conformance.

## 1. Purpose and overall approach

`agent-python` is a framework-free, state-driven agent service. A language model proposes actions and final answers; Python controls which actions may execute, records their actual outcomes, advances configured workflows, manages context, and decides whether an answer meets the configured completion conditions.

It uses FastAPI for HTTP and server-sent events (SSE), HTTPX for asynchronous OpenAI-compatible chat-completions calls, Pydantic for typed state/configuration, YAML for profiles, and Psycopg for asynchronous PostgreSQL access. It does not depend on an agent orchestration framework.

The central separation is between model-generated claims and authoritative execution state. A model can suggest that a phase is finished, but only matching executed tool evidence advances an evidence-gated phase. A summary can record a lesson, but cannot manufacture completed work. A final response can look plausible, but still fail exact-output, schema, or captured-result checks.

The built-in tasks demonstrate this approach with dirty logs, inconsistent revenue data, and multi-stage employee auditing. These are controlled fixture environments, with intentionally exposed parsing failures and validators. The runtime is extensible, but the current tool registry is not a general shell, arbitrary SQL executor, web browser, or autonomous code editor.

## 2. Original implementation and how it evolved

The initial Python implementation already contained the main architectural idea:

1. Store the original goal as an immutable anchor and the first user message.
2. Build a model request from a fixed system prompt, the original goal, any compacted context, and recent raw messages.
3. Ask an OpenAI-compatible model for the next action.
4. Execute a selected local tool and append its string result to memory.
5. Repeat until the model returns a final answer or the step limit is reached.
6. Summarize older messages when memory pressure requires it, preserving the newest raw messages.

The original service offered session creation, state retrieval, user-message append, and a bounded run endpoint. Session storage was process-local. Its example environment used a generated access log and SQLite commerce fixture. Configuration selected a model, system prompt, tools, and basic memory settings.

The original engine was much smaller and relied heavily on the model following instructions. It accepted a final decision directly, handled one selected tool action per loop iteration, estimated context from message character counts, and retained a raw message slice during compaction. Its step limit was compared against the lifetime decision count. It did not contain the current PostgreSQL execution protocol, workflow evidence machinery, independent deliverable validation, or safe staged compaction protocol.

The current implementation retains the loop but makes its boundaries explicit:

| Area | Original design | Current implementation |
| --- | --- | --- |
| Configuration | Load agent YAML | Trusted relative profile selection, validated graph/tools/policies, immutable persisted raw and resolved identities |
| Session state | Process memory | PostgreSQL authority, revision checks, selective hydration, local cache only |
| Model actions | One selected action | Complete ordered batches, strict arguments, stable call/result correlation, correction feedback |
| Task progress | Model-directed | Durable workflow evidence, completion artifacts, invalid-response and no-progress bounds |
| Final answer | Accept returned final | Require nonempty content and all configured workflow/format/result checks |
| Compaction | Character estimate and raw slice | Complete-request budget, complete interaction groups, staged validated ledger, provenance and atomic installation |
| Run allowance | Lifetime-count limit | Additional decisions per run, distinct lifetime/run counters, explicit resume |
| Execution safety | Direct tool call | Durable intent/start/outcome, recovery policies, cancellation, leases and fencing |
| History | Active memory | Immutable ordered transcript plus paged events, diagnostics, runs and executions |
| Fixtures | Files and SQLite | Four PostgreSQL fixture tables |

These changes address separate failure modes: losing state on restart, accepting unsupported success claims, forgetting required work after summarization, repeating actions without progress, racing concurrent workers, and blindly retrying a tool whose outcome is uncertain.

## 3. A session from creation to completion

### Create and snapshot

`POST /sessions` accepts `user_prompt` and an optional `config_ref`, defaulting to `log-analysis.yaml`. The server resolves and validates the profile, checks that its tools exist in the compiled Python registry, and creates a session UUID.

The saved session includes the exact original goal, configuration reference, SHA-256 of the YAML bytes, resolved configuration identity, workflow state, active memory, counters, and lifecycle metadata. Creation commits revision 1. The initial goal message has sequence 1; messages and audit records subsequently share an ordered transcript sequence.

The raw YAML hash identifies source bytes. The resolved hash identifies effective configuration after interpolation/defaults and canonicalization. These are separate identities: identical YAML can resolve differently in a different environment. Existing sessions use their saved resolved configuration rather than rereading edited YAML during resume.

### Claim execution and reload

A run acquires PostgreSQL ownership with an internal owner UUID, increasing fence, and expiring lease. It then reloads the authoritative session after acquisition. An earlier API read or cached object is insufficient authority to execute.

The default lease is 30 seconds, renewed every 10 seconds. Asynchronous model/database/tool operations allow the heartbeat to run while waiting. These defaults are constructor settings, not YAML configuration fields. Ownership and revision conflicts produce recoverable conflicts instead of allowing stale state to overwrite current work.

### Make a bounded decision

The effective run allowance is the request's `max_steps`, otherwise `memory.run_max_steps`, otherwise `memory.max_steps`. It means additional model decisions for this run. The API accepts explicit allowances from 1 through 50; `step_count` remains a lifetime counter.

Before each decision, the engine checks cancellation/ownership, advances eligible workflow state, considers compaction, and verifies that the complete model request fits the configured context budget. It then sends the request asynchronously and archives the parsed decision.

A decision becomes either a final candidate, an ordered collection of tool calls, or recoverable correction feedback. Empty/malformed responses and rejected actions do not count as success. Repeated invalid responses stop at the configured consecutive limit, default 3. Optional no-progress detection also stops loops that repeatedly produce no new evidence, including redundant successful calls interleaved with rejected actions.

### Validate and execute tools

The adapter retains every returned call in order. The engine normalizes call identities, detects reused IDs, validates strict JSON/schema arguments, checks result-bound arguments, and enforces global and active-phase tool policy. Rejected calls receive correlated tool feedback marked `executed:false` without invoking the environment.

For valid calls, the controller persists the scheduled batch, then durable execution intent and started attempt before invoking the adapter. Calls execute sequentially. A known result is committed together with its correlated tool message, evidence/artifact updates, and removal from pending work. A later model request therefore sees the actual result associated with the correct call.

Tool results are strings, including expected diagnostic errors. A built-in parsing error can help the model correct its next action. This does not mean every error is a successful result: configured diagnostic evidence and successful deliverable capture have different purposes.

### Accept a final answer or stop recoverably

A final candidate must be nonempty, finish any required workflow, and satisfy every configured completion rule. Rejected candidates are archived and receive corrective guidance.

Task status is `active`, `complete`, or `failed`. Run status is separate: `idle`, `running`, `complete`, `failed`, `cancelled`, `interrupted`, or `blocked`. A cancelled task may be failed while its run is cancelled. Step exhaustion is a resumable failure, not successful completion.

The controller commits terminal task/run state before exposing successful final output. `/resume` creates a new run identity and increasing fence, reactivates continuation, and reconciles pending execution. It preserves the original goal and configuration. Pending calls must be recovered without new content before new instructions can be added.

## 4. Workflow evidence and completion enforcement

A workflow is an optional directed graph with an entry phase, unique phase IDs, instructions, allowed tools, evidence requirements, and transitions. Transitions may depend on phase completion, their own evidence, or an `always` edge. The engine preserves those meanings and considers eligible edges in declaration order.

Evidence matches the actual executed tool name and configured typed argument/result containment or required output text. For example, a validator result `{valid: true}` can complete a log-analysis phase. An expected parser failure containing `NULL` can satisfy a diagnostic requirement in the stress profile. A model saying “validated” cannot satisfy either requirement.

Observed evidence keys, completed phases, transitions, and captured deliverables persist outside compactable model history. Missing constraints are rendered near the next model action and in premature-final feedback, including the exact required argument/result conditions.

Completion supports:

| Setting | Enforced behavior |
| --- | --- |
| `exact` | Entire answer equals the configured literal |
| `pattern` | Entire answer matches the regular expression |
| `json_schema` | Answer parses as bare strict JSON and satisfies the supported schema subset |
| `forbidden_patterns` | Reject matching diagnostic or other prohibited content |
| `result_checks` | Required table, labeled number, or JSON value agrees with a captured executed result |
| `instructions` | Friendly presentation guidance; deterministic checks remain separate |

Markdown checks require the captured headers and complete row set, reject duplicate row keys, and compare cells with defined numeric semantics. Number checks require one correctly labeled matching number. JSON checks compare the selected answer value with the captured result using typed equality. Argument bindings can require a downstream argument to equal a previously captured executed value, such as the salary median supplied to the final employee validator.

Not every profile enables all these checks. In particular, `completion-no-workflow.yaml` intentionally relies on prompt guidance for the demonstrated task rather than enforcing a workflow success gate. Workflow-free operation still receives action validation and any explicitly configured completion checks.

## 5. Latest contract 1.1.0 changes

The durable reference freeze is 1.0.0. Current code includes two explicit additions after that freeze:

1. **Final-only workflow state.** Once the configured workflow follows its terminal `complete` edge, the active phase is absent and no tool may execute. Even a valid registered call is rejected before invocation, with guidance to return the final answer. Workflow guidance and allowed-tool calculation reinforce the guard. This prevents a model from continuing to call tools after it has already collected the required evidence.
2. **Exact recovery completion.** `config/tool-recovery.yaml` requires the final response to equal `RECOVERY_OK 10.0.0.5 83`. Completing the recovery workflow alone no longer permits arbitrary final prose.

A workflow-free session does not enter final-only workflow state. Resuming a completed workflow preserves its completed evidence and remains final-only; resume does not implicitly reset its graph or authorize rerunning its tools.

These changes are present in active code/configuration. Older frozen sources and archived passing reports do not by themselves establish coverage of these newer behaviors. The contract describes Python as the qualified reference, while qualification evidence remains historical collections with documented scope and limitations.

## 6. Context construction and protected compaction

The model request preserves a deliberate order: immutable system instructions; original goal; final-answer requirements; any unverified compacted ledger; captured deliverables; recent raw messages; result argument bindings; current workflow/missing constraints; and final synthesis guidance when applicable. Persistence IDs and timestamps are omitted from wire messages.

The adapter adds actual model/tool settings, tool-policy guidance, and an output limit. Budgeting measures that complete request, including tool schemas and instructions. The estimator is compact UTF-8 JSON byte length plus 32 per message. Despite field names containing `tokens`, this is a conservative reproducible estimate, not a tokenizer or reported provider usage.

The fit condition includes estimated input, the larger applicable output reserve, and a safety margin. A hard overflow must be addressed before a model decision request is sent. Summarizer requests are also budgeted.

Compaction groups assistant calls with all their tool results. `raw_turns_to_keep` is a minimum message count expanded to whole interaction boundaries; it is not permission to split a call from its result. Incomplete/orphaned groups fail integrity checks.

Optional compaction can be triggered by a soft input threshold with sufficient growth or an independent decision cadence. Spacing, pressure, reclaimable-history, and useful-reduction gates can suppress unnecessary summarizer calls. Hard protection bypasses optional skips. Completed workflows skip optional compaction, but still require a fitting decision request.

The summarizer receives an affordable historical prefix, the original goal, and the prior ledger. The normal hybrid format contains four ordered text sections under `COMPACTED_STATE`: `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, and `CURRENT_LOCAL_PIVOT`. A nonhybrid four-heading format is also supported.

Before replacement, the engine validates structure, unchanged goal, canonical byte limit, source numeric containment, unchanged source/state, and useful reduction of the complete next decision request. Numeric containment prevents invented statistics but does not establish the truth of all prose. The typed ledger is explicitly labeled `unverified_model_summary` and supplied as memory data.

Rejected proposals leave the previous raw memory and ledger intact. Attempts/passes are bounded, and an unchanged failed source is not retried indefinitely. Accepted and rejected proposals retain source IDs/hash, retained IDs, prior-ledger identity, and diagnostics. Installation is atomic with session/context/history updates. Source transcript records remain available; compaction does not delete history.

If safe compaction cannot make a hard-overflow request fit, the task stops with `context_budget_exceeded`. The implementation does not send an unaffordable decision request or fabricate a destructive fallback summary.

## 7. Persistence, concurrency, recovery, and retention

PostgreSQL schema 5 separates hot active state from archival records:

| Table | Purpose |
| --- | --- |
| `agent_sessions` | State/configuration, hashes, revision/commit identity, lease and fence |
| `agent_session_context` | Current raw message references, goal and active ledger |
| `agent_session_transcript` | Immutable ordered messages, decisions and provenance |
| `agent_session_events` | Durable lifecycle/tool/phase events |
| `agent_session_diagnostics` | Historical diagnostic families and ordinals |
| `agent_session_calls` | Historical call identity lookup, including compacted-away calls |
| `agent_runs` | Durable run lifecycle, ownership and cancellation |
| `agent_tool_executions` | Intent, attempts, policy, idempotency key and outcome |
| `agent_archive_receipts` | Verified export/dependency receipts |
| `agent_schema_migrations` | Applied storage/component versions |

Active hydration uses one MVCC SQL statement for a consistent state/configuration/context/message/diagnostic view. Ordinary state responses contain current context and up to 32 recent diagnostics per family, with lifetime counters. Full history is retrieved separately through bounded pages.

Saving locks the session, checks ownership/fence/expiry and expected revision, protects immutable goal/configuration anchors, and writes the new snapshot, context, transcript, diagnostics, execution/run updates, and event in one transaction. A final ownership check precedes commit. Identical commit UUID/content retries are handled idempotently; inconsistent identities or sequences fail. Failed transactions do not partially install a summary or tool outcome.

Leases prevent concurrent execution; revision comparison prevents stale snapshots from committing; fences prevent an old owner from writing after a new owner takes over. These mechanisms are distinct from authentication and external tool idempotency.

Tool recovery policies are `read_only`, `repeatable`, `idempotent`, and `reconcile`, with bounded attempts and stable keys. Built-in fixture tools are read-only. New unknown adapters default to reconciliation. Idempotent execution requires a real adapter; the current idempotent/reconciler registries are empty extension points. Explicit transient adapter signals may authorize retries. An arbitrary error string from the model or tool does not prove safe replay.

When an external outcome is uncertain and no safe reconciliation is available, execution blocks with a recovery-required reason. There is no generic external exactly-once guarantee or rollback of external effects.

Cancellation is cooperative. A known result from an in-flight invocation can settle and commit before scheduling stops; expired ownership still prevents stale commits. Disconnect requests cancellation. Startup coordinates schema readiness and abandoned-run lifecycle recovery across workers using a PostgreSQL advisory guard. It does not silently execute user models/tools; explicit resume performs pending-work recovery.

Retention defaults to keep-all. Optional administration can export a consistent snapshot, verify its counts/digests/dependencies, and prune eligible historical telemetry under fenced revision checks. Transcript, active context, provenance, runs, executions, and archive receipts remain protected. This is not full session deletion or automatic pruning during compaction.

## 8. Implemented problem-solving profiles and tools

The registry contains 11 async tools. All current fixture tools read PostgreSQL; the historical SQLite database and flat log are unused.

| Profile | Problem and implemented approach |
| --- | --- |
| `log-analysis.yaml` | Inspect dirty access logs, count HTTP 500 responses with robust parsing, validate the highest-count IP before workflow completion |
| `revenue-analysis.yaml` | Inspect schemas, normalize city case and `ID_` foreign keys, aggregate Chicago revenue for January–March 2026, validate the total |
| `employee-audit.yaml` | Identify corrupt age/salary fields, calculate normalized New York median salary, generate clean city headcounts/average ages, validate and enforce final deliverables |
| `compaction-stress.yaml` | Require multiple known failed and successful employee-tool attempts, producing verbose history to exercise compaction while preserving phase evidence and final artifacts |
| `completion-no-workflow.yaml` | Demonstrate direct answers and prompt-guided log analysis without an enforced workflow graph |
| `tool-recovery.yaml` | Exercise required diagnostic/success evidence and bounded continuation, ending with the exact recovery marker |

The log tools are `inspect_log`, `analyze_500_ips`, and `validate_top_offending_ip`. The naive parser intentionally fails on malformed input; the robust parser ignores invalid records and computes counts. The recovery profile's expected top result is `10.0.0.5`, count `83`.

Revenue tools are `inspect_sql_schema`, `query_chicago_q1_revenue`, and `validate_chicago_q1_revenue`. They demonstrate why a query can fail or undercount when keys and case are inconsistent. SQL expressions are selected by validated flags; the model does not supply arbitrary SQL.

Employee tools are `inspect_employee_audit_schema`, `audit_employee_phase_1`, `calculate_new_york_median_salary`, `generate_employee_phase_3_report`, and `validate_employee_audit`. They expose currency-formatted/`UNKNOWN` salary fields, `NULL`/semicolon-corrupted ages, and inconsistent city casing. The successful median is `92500.0`. The report calculates valid-age headcount and average age by normalized city. Some diagnostic paths are deliberately scripted failures, not arbitrary code execution over user-provided data.

The deterministic fixture recipe creates 650 log rows, 2,200 customers, 6,502 orders, and 1,000 employees in `agent_fixture_access_logs`, `agent_fixture_customers`, `agent_fixture_orders`, and `agent_fixture_employees`. The order count includes two initial rows plus 6,500 generated rows.

## 9. Configuration

### Environment and profile selection

Root `.env` supplies model and database settings. Do not copy real credentials into documentation. A representative configuration is:

```dotenv
MAIN_MODEL=your-model-name
OPENAI_BASE_URL=http://127.0.0.1:8080/v1
OPENAI_API_KEY=your-key
AGENT_SESSION_DATABASE_URL=postgresql://app_user:your-password@localhost:5432/universal_state
# Optional fixture placement override:
# AGENT_FIXTURE_DATABASE_URL=postgresql://app_user:your-password@localhost:5432/universal_state
# Optional trusted profile root:
# AGENT_CONFIG_ROOT=/absolute/path/to/state-driven-agent/config
```

The loader supports recursive `${VAR}` and `${VAR:-default}` interpolation. Root `.env` explicitly owns `MAIN_MODEL` even over an inherited value; other dotenv values normally supply defaults. All six current profiles interpolate `MAIN_MODEL`, with a profile fallback. The implementation does not pin, safeguard, or silently substitute a different model.

Session database configuration uses `AGENT_SESSION_DATABASE_URL`; tools prefer `AGENT_FIXTURE_DATABASE_URL`, then the session URL. Set these explicitly for the intended deployment. `AGENT_TEST_DATABASE_URL` is a separate testing override described in operations guidance. The legacy `load_config()` helper recognizes `STATE_DRIVEN_CONFIG`; HTTP session creation uses `config_ref` through `ConfigRepository` instead.

Only nonempty relative YAML paths inside the trusted config root are accepted. Absolute paths, escapes through traversal/symlinks, invalid YAML/configuration, unknown tools, invalid edges, duplicate labels/bindings, and unsupported schema keywords fail validation. Profiles are discovered for new sessions without restarting the server. YAML declares policies and registered tool metadata; it does not install executable code.

### Profile sections

| Section | Configuration responsibility |
| --- | --- |
| `agent.name`, `system_prompt`, `model` | Identity, immutable behavior instructions and selected model |
| `llm` | Endpoint, API-key environment variable, temperature, timeout, tool-call mode and optional output cap |
| `memory` | Run allowance, raw tail, request budget and compaction policies |
| `execution` | Global tool policy, invalid/no-progress limits, result argument bindings |
| `completion` | Presentation instructions and deterministic final-answer checks |
| `tools` | Names, descriptions, parameter schemas and optional recovery policies |
| `workflow` | Optional phase graph, allowed tools, evidence and transitions |
| `output.verbose_setup` | Optional setup detail in SSE lifecycle output |

`llm.tool_call_mode` defaults to `native`. `single` requests one call per response and adapts historical multi-call wire groups into single-call/result pairs without discarding calls or changing stored history. `execution.tool_policy: none` removes tool declarations and instructs direct answering, with controller enforcement. The adapter accepts a base API root or exact chat-completions URL and tries a root-route compatibility fallback only after a `/v1` route returns 404.

Strict JSON validation rejects duplicate keys, nonfinite numbers, coercion, and booleans used as numbers. The supported schema subset includes type/properties/required/additionalProperties/items/enum/const, numeric bounds, string length/pattern, and array length. Integer-valued finite JSON numbers satisfy integer schemas. This is a defined subset, not an unrestricted JSON Schema implementation.

### Important memory defaults

| Setting | Default / meaning |
| --- | --- |
| `max_tokens`, `max_steps`, `raw_turns_to_keep` | Required: soft input threshold, fallback run allowance, minimum retained raw messages |
| `run_max_steps` | Optional explicit default run allowance |
| `context_window_tokens` | 32768, conservative total request budget |
| `reserved_output_tokens`, `safety_margin_tokens` | 1024 and 256 |
| `hybrid_reflection` | `true`, structured XML ledger |
| `max_summary_tokens` | 4096; despite the name, validates canonical UTF-8 bytes |
| `summary_output_tokens` | 512, summarizer output allowance |
| `compaction_max_attempts`, `compaction_max_passes` | 2 and 3 |
| `compaction_interval_steps` | Optional independent cadence |
| `compaction_min_growth_tokens` | 512 before another soft-threshold trigger |
| `compaction_min_interval_steps` | 0; optional spacing |
| `optional_compaction_min_pressure_ratio` | 0; optional pressure gate |
| `optional_compaction_min_reclaimable_tokens` | 0; optional reclaimable-input gate |
| `optional_compaction_min_reduction_tokens`, `_ratio` | 0 and 0; useful-reduction minima, with positive reduction always required |

Current profile overrides matter: employee audit uses a 30-decision default allowance and no-progress limit 6; stress uses 40 decisions, a two-step cadence, and explicit reclaim/reduction thresholds. These conservative budget values should not be interpreted as measured model-token consumption.

## 10. Install, initialize, and use

Python requires version 3.14 or newer according to `pyproject.toml`. From the repository root:

```sh
uv sync --project agent-python
```

With the configured PostgreSQL database available, explicitly initialize its current schema:

```sh
uv run --project agent-python python agent-python/scripts/initialize_database.py
```

For a fresh fixture environment or an intentional fixture reset:

```sh
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
```

Fixture generation atomically replaces fixture data; it is not required before every run. The dedicated database described by current operations is `universal_state`. No SQLite import or migration from another project's database is required for a fresh setup.

Start the API while the model service and PostgreSQL run separately:

```sh
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --host 127.0.0.1 --port 8081
```

The packaged `agent-python` entry point instead starts loopback port 8000 with reload enabled. The explicit command above selects port 8081. FastAPI's interactive API documentation is available at `/docs`.

Create a session and use its returned ID:

```sh
curl -fsS http://127.0.0.1:8081/sessions \
  -H 'Content-Type: application/json' \
  -d '{"user_prompt":"Find and validate the IP with the most HTTP 500 responses.","config_ref":"log-analysis.yaml"}'

curl -fsS http://127.0.0.1:8081/sessions/SESSION_UUID/run \
  -H 'Content-Type: application/json' -d '{"max_steps":12}'

curl -N http://127.0.0.1:8081/sessions/SESSION_UUID/resume \
  -H 'Content-Type: application/json' -d '{"max_steps":12}'
```

Replace `SESSION_UUID` with the created session UUID. Resume is SSE, including when continuing without a new message. Appending a message requires an active unowned task with no pending tool work; terminal/pending work uses resume as described above.

## 11. HTTP interface and observability

| Method and route | Purpose |
| --- | --- |
| `GET /health` | Service/config discovery metadata, not proof of task/model/database success |
| `GET /configs` | Valid registered profiles, hashes, tools, phases and verbosity |
| `POST /sessions` | Create session and immutable goal/configuration snapshot |
| `GET /sessions/{id}` | Authoritative active state and recent diagnostics |
| `POST /sessions/{id}/messages` | Append user context while preserving original goal |
| `POST /sessions/{id}/run` | Bounded JSON run; terminal tasks return without a new run |
| `POST /sessions/{id}/run/stream` | SSE run for an active task; terminal tasks return 409 |
| `POST /sessions/{id}/resume` | SSE continuation/recovery, optional new content |
| `POST /sessions/{id}/cancel` | Request cooperative cancellation of committed running work |
| `GET /sessions/{id}/transcript` | Immutable ordered transcript pages |
| `GET /sessions/{id}/events` | Durable event pages |
| `GET /sessions/{id}/diagnostics/{family}` | Diagnostic pages and lifetime counts |
| `GET /sessions/{id}/runs` | Run/fence/cancellation history |
| `GET /sessions/{id}/executions` | Tool intent/attempt/outcome/recovery records |
| `GET /sessions/{id}/archives` | Archive receipt pages |
| `GET /sessions/{id}/retention` | Retention eligibility and protected-data information |
| `GET /runs/abandoned` | Abandoned-run inspection |

Malformed session UUIDs return 422 before SQL; valid absent IDs return 404. Ownership/revision conflicts return structured recoverable 409s, or corresponding SSE errors after streaming starts. History-page limits default to 100 and range from 1 to 1000.

Transcript uses `after_sequence`/`through_sequence`; events and diagnostics use their supported ordinal/event cursors and `through` watermarks. Reuse returned watermarks to bound append-only traversal. Runs/executions return current lifecycle records per page, not a frozen multi-page snapshot.

SSE sends JSON `data:` events for status, interim tool/phase results, final results, and errors. Milestones follow relevant persistence boundaries. Diagnostics distinguish transport, timeout, provider/template, empty response, invalid decision, and context failures. Request estimates and timing do not establish provider usage/cost; unknown values must not be invented.

Operational scripts include `reconcile_runs.py` for abandoned-run inspection or explicit lifecycle reconciliation and `storage_retention.py` for plan/export/guarded telemetry pruning. Reconciliation administration does not execute user tools/models. See [operations.md](agent-python/docs/operations.md) for commands and preconditions.

## 12. Source map and limits of the description

| Source | Main responsibility |
| --- | --- |
| `config.py`, `models.py` | Configuration resolution, validated policy and state/wire models |
| `api.py` | Routes, authoritative reloads, startup, streaming and cancellation |
| `engine.py` | Decision loop, workflows, evidence, artifacts, progress and compaction |
| `llm.py`, `model_errors.py` | Async model transport, wire adaptation and error classification |
| `validation.py`, `completion.py` | Strict JSON/schema checks and executed-result comparison |
| `context.py` | Complete interaction groups, request budgets and typed ledger validation |
| `transcript.py` | Stable message identities, sequences and history staging |
| `execution.py`, `execution_store.py` | Leases, fences, run/tool lifecycle and recovery |
| `persistence.py`, `storage.py`, `storage_state.py` | Atomic saves, schema, hydration, diagnostic/history storage |
| `tools.py` | Registered fixture tools and recovery adapter extension points |
| `retention.py` | Verified export and protected telemetry-only pruning |

Paths above are relative to [agent-python/src/agent_python](agent-python/src/agent_python). Detailed algorithm/transaction references are [architecture](agent-python/docs/architect.md) and [compaction](agent-python/docs/compaction.md). The shared [test-cases.md](test-cases.md) provides current curl validation scenarios.

Historical regression scripts, benchmarks, manifests, frozen source/schema and qualification reports are preserved in [archive](archive/README.md). They are not active runtime dependencies, and relocated test tooling may require restoration/adaptation before use. The active application directory contains four supported operational scripts rather than an active qualification runner.

The implementation provides explicit control and auditability around model-driven work, but structural summary checks do not prove semantic fidelity, fixture examples do not prove arbitrary real-world data quality, and historical benchmarks do not establish production capacity or population latency percentiles. Authentication/multitenancy, multi-host partition/failover qualification, managed archive restore, and generic external exactly-once execution are outside the documented claims. This document records implementation behavior and existing evidence boundaries; it does not claim a new successful test run.

## 13. Relevant and timely applications

The strongest opportunities are investigations and operational workflows that must collect evidence, survive interruptions, and produce a verifiable result. The latest `agent-python` is particularly useful when the model saying it is done is insufficient. These are proposed applications, not existing integrations: each requires new tools and profiles beyond the current fixtures. The ranking below reflects implementation fit and practical value as of October 19, 2026.

| Problem | Why it fits especially well | Suggested first scope |
| --- | --- | --- |
| Data pipeline failure investigation | Evidence gates, preserved diagnostic history, validated totals | Diagnose failed or incorrect PostgreSQL ingestion |
| Industrial IoT incident investigation | Long investigations, multiple evidence sources, durable continuation | Explain a sensor anomaly and validate its cause |
| AI/cloud cost anomaly investigation | Exact numeric artifacts, result bindings, repeatable analysis | Explain unexpected inference or infrastructure spending |
| Security vulnerability remediation verification | Explicit completion evidence and durable execution records | Verify whether affected assets are actually remediated |
| AI release evaluation and regression triage | Immutable configuration, resumable test execution, strict final checks | Determine whether a model/prompt release meets acceptance criteria |
| Compliance evidence collection | Ordered audit history, missing-evidence gates, protected source records | Assemble evidence for a defined control checklist |

### 13.1 Data pipeline failure investigation

This is the best immediate extension of the existing dirty-data examples. A pipeline may succeed while silently dropping rows, duplicating events, mishandling time zones, or changing business totals.

A workflow could:

1. Capture the failed run, schema versions, source manifest, and expected destination.
2. Inspect rejected records and transformation errors.
3. Test candidate explanations through bounded queries.
4. Run a repair in a staging destination.
5. Validate row counts, uniqueness, null rates, time ranges, and aggregate reconciliation.
6. Produce the verified findings and repair artifact.

Diagnostic errors can become evidence, compaction preserves lessons from earlier failed attempts, and completion artifacts can require the final report to contain the actual reconciliation results.

**Completion rule:** the agent cannot declare the pipeline repaired until a validation tool confirms all required invariants.

**First scope:** a PostgreSQL ingestion investigation using read-only tools, followed by a staged replay adapter.

### 13.2 Industrial IoT incident investigation

Potential incidents include an airlock pressure excursion, unexplained battery discharge, refrigeration temperature spike, or a sensor that appears stuck. The agent could correlate telemetry with device configuration, maintenance events, neighboring sensors, and ingestion quality.

A workflow could:

1. Capture the incident window, affected devices, device configuration, and expected operating limits.
2. Verify telemetry completeness, timestamps, units, calibration history, and duplicate or delayed events.
3. Characterize the anomaly and compare it with historical baselines and neighboring sensors.
4. Test candidate explanations against maintenance events, configuration changes, and correlated measurements.
5. Assess the duration, affected equipment, and operational impact using executed calculations.
6. Validate the findings and produce an incident report separating confirmed causes, unresolved hypotheses, and maintenance recommendations.

The investigation must distinguish physical faults from data problems: a temperature excursion might reflect equipment failure, a calibration change, delayed packets, or duplicated events.

Evidence gates can require investigation of those alternatives before allowing a conclusion. Durable continuation helps when evidence arrives later, while compaction preserves earlier findings across lengthy investigations.

**Completion rule:** every reported measurement must come from executed analysis, and the final output must distinguish confirmed findings from unresolved hypotheses.

**First scope:** investigation and maintenance recommendations using simulator telemetry. Physical actuation would require additional control and authorization mechanisms beyond the current runtime.

### 13.3 AI and cloud cost anomaly investigation

FinOps explicitly covers AI alongside other technology categories, and its 2026 framework emphasizes connecting spending to business value. This makes explainable cost investigations a timely application. See [FinOps Framework 2026](https://www.finops.org/insights/2026-finops-framework/).

A representative question is: “Why did inference spending increase while completed customer requests stayed flat?” Tools would examine billing exports, application request logs, model routing, retries, caching, and workload ownership.

A workflow could:

1. Capture the billing period, anomaly threshold, comparison baseline, and applicable pricing data.
2. Reconcile billing exports with measured usage, recording discrepancies and missing attribution.
3. Identify responsible workloads, owners, models, endpoints, and infrastructure resources.
4. Test explanations such as retry amplification, routing changes, cache misses, idle resources, or increased input/output sizes.
5. Calculate candidate savings using captured usage and rates, explicitly separating observed costs from estimated savings.
6. Validate that explained and unexplained spending reconcile to the captured total, then produce the findings and recommendations.

Result bindings could require savings calculations to use previously captured usage and rate data. Completion checks could prevent the final report from inventing savings or substituting an estimate for an observed cost.

**Completion rule:** explained spending plus explicitly unexplained spending must reconcile to the captured total within a configured tolerance.

**First scope:** read-only cost attribution and recommendations. Resource changes need dedicated effectful adapters with reconciliation.

### 13.4 Security vulnerability remediation verification

Security teams need to know whether a vulnerable asset was actually fixed, rather than merely whether a ticket was closed. CISA guidance emphasizes asset visibility and vulnerability detection as foundational operational requirements. See [CISA's asset visibility directive](https://www.cisa.gov/news-events/directives/bod-23-01-improving-asset-visibility-and-vulnerability-detection-federal-networks).

A workflow could:

1. Capture the vulnerability advisory, affected product/version criteria, and required verification conditions.
2. Correlate the advisory with the asset inventory and identify potentially affected systems.
3. Confirm installed versions, exposure, asset ownership, and available pre-change evidence.
4. Associate existing remediation jobs and change records with each asset, reconciling uncertain job outcomes before any retry.
5. Collect fresh post-change scan or version evidence and test the required remediation postconditions.
6. Produce a per-asset report of verified fixes, exceptions, unresolved exposure, and missing evidence.

The execution ledger is particularly relevant when a patch job starts but the connection disappears. A reconciliation adapter could look up that existing job instead of starting it again.

**Completion rule:** a closed ticket or successful deployment command is insufficient; fresh verification evidence must establish the required postcondition.

**First scope:** read-only remediation verification, with controlled execution added separately.

### 13.5 AI release evaluation and regression triage

This application would investigate whether a new model, prompt, retrieval configuration, or tool definition breaks established behavior. NIST's generative AI profile emphasizes evaluation, monitoring, and retention of testing and validation history, aligning with the runtime's durable evidence model. See [NIST's Generative AI Profile](https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence).

A workflow could:

1. Freeze the candidate model, prompt, retrieval/tool configuration, evaluation-set version, and acceptance criteria.
2. Capture the baseline configuration and results under comparable evaluation conditions.
3. Execute every required evaluation case, durably recording inputs, outputs, tool behavior, and measured outcomes.
4. Reproduce failures and investigate regressions while preserving the original failed attempts.
5. Compare candidate and baseline results, accounting explicitly for missing, failed, and interrupted cases.
6. Validate the final metrics and required coverage, then produce a release assessment with evidence for each acceptance criterion.

Relevant capabilities include immutable configuration identities for reproducibility, durable execution for lengthy evaluation suites, recovery after worker interruption, exact/table/JSON checks for final metrics, and final-only state once all required evaluation evidence is complete.

**Completion rule:** all required evaluation cells must have recorded outcomes; missing tests cannot silently disappear from the final denominator.

**First scope:** contract tests for tool-using agents, including malformed calls, premature finals, duplicate effects, and interruption recovery. The existing state-driven-agent contract provides an initial test domain.

### 13.6 Compliance evidence collection

The practical problem is collecting current evidence across systems and identifying what is missing. Examples include access reviews, backup restoration evidence, change approvals, and AI-system inventory records.

AI governance is a timely instance: the European Commission's AI Act guidance describes transparency obligations and their applicable timelines. The agent could support evidence gathering against an organization's reviewed requirements. See [European Commission transparency guidance](https://digital-strategy.ec.europa.eu/en/faqs/transparency-obligations-under-article-50-ai-act).

Each control could require specific executed checks, source identifiers, timestamps, and reviewer records. The agent would produce an evidence matrix with explicit gaps.

A workflow could:

1. Capture the reviewed control checklist, its version, organizational scope, evidence period, and designated reviewers.
2. Map each control to required evidence sources, deterministic checks, and freshness requirements.
3. Collect source records and execute the required checks, preserving source identifiers and timestamps.
4. Identify missing, stale, contradictory, or out-of-scope evidence and record the outstanding requirements.
5. Incorporate additional evidence and reviewer records without replacing the original collection history.
6. Validate checklist coverage and produce an evidence matrix distinguishing supported controls, evidence gaps, and items awaiting review.

**Completion rule:** collecting the required evidence completes the collection task; it does not automatically establish legal compliance.

**First scope:** one narrowly defined internal control set, using versioned requirements and deterministic checks.

### 13.7 Recommended first project

Build a data pipeline investigation agent using IoT telemetry as its initial domain. It combines the existing data-quality approach with simulator workloads and exercises diagnostic evidence, multi-stage workflows, protected compaction, durable recovery, and verified final artifacts.

A concrete demonstration task would be:

> Investigate why yesterday's device-health report undercounted incidents. Identify the faulty transformation, validate a staged correction, and produce reconciled before-and-after results. Interrupt the worker halfway through and demonstrate successful recovery.

A demonstration workflow could:

1. Capture the report version, source telemetry manifest, transformation configuration, and expected incident definition.
2. Reconcile source incidents with report rows and identify where records were dropped or misclassified.
3. Test the suspected transformation defect and record the evidence supporting the diagnosis.
4. Interrupt the worker after committed diagnostic progress, then resume and verify that prior evidence and pending execution are recovered correctly.
5. Apply the candidate correction in staging and validate counts, incident identities, time windows, and aggregate totals.
6. Produce verified before-and-after results, the staged repair artifact, and a recovery record showing preserved evidence and no duplicated effects.

Success should require a supported diagnosis, measured reconciliation, validated staged output, and recovery without losing committed evidence or duplicating effects. This makes the latest implementation's operational advantages directly observable.

## 14. Architecture of the upgraded Python agent

The diagram shows the current contract 1.1.0 architecture and schema 5 persistence boundary. The original compact–decide–execute loop remains at its center, surrounded by deterministic validation, protected context management, and durable execution coordination. Arrows describe logical interactions; execution is asynchronous, and tool calls within a returned batch execute sequentially.

```mermaid
flowchart TB
    Client["Client / operator<br/>HTTP JSON and SSE"]
    Profiles["Trusted YAML profiles + root .env<br/>New-session configuration"]

    subgraph Service["agent-python service"]
        API["FastAPI lifecycle and history routes<br/>api.py"]
        Config["Profile validation + source/resolved hashes<br/>Immutable configuration snapshot<br/>config.py / models.py"]
        Coordinator["Execution coordinator<br/>Claim, reload, lease heartbeat, fence<br/>Cancel, resume and reconcile<br/>execution.py / execution_store.py"]
        Engine["Bounded decision loop<br/>Workflow transitions and progress limits<br/>engine.py"]
        Context["Complete-request budget + compaction<br/>Whole call/result groups<br/>Validated unverified ledger + provenance<br/>context.py / engine.py"]
        LLM["Async OpenAI-compatible adapter<br/>Ordered calls, wire correlation, errors<br/>llm.py / model_errors.py"]
        ActionGate["Before invocation<br/>Strict arguments + result bindings<br/>Tool/phase policy + final-only guard<br/>validation.py / engine.py"]
        Tools["Registered async tool adapters<br/>Durable intent and start before invocation<br/>tools.py + execution coordinator"]
        Evidence["Executed evidence + captured artifacts<br/>Protected from model compaction"]
        FinalGate["Final-answer validation<br/>Workflow, exact, regex, JSON, table/number<br/>completion.py / engine.py"]
        Store["Atomic repository boundary<br/>Revision CAS + owner/fence/expiry checks<br/>Snapshot, history and outcomes together<br/>persistence.py / storage.py / storage_state.py"]
        Admin["Startup and administration<br/>Schema readiness, abandoned runs<br/>Verified export / guarded telemetry pruning"]
    end

    Model["External model service<br/>Decisions and summary proposals"]
    PG[("PostgreSQL: schema 5<br/>State/config, active context, transcript<br/>Events, diagnostics, calls, runs<br/>Executions, archives, migrations")]
    Fixtures[("PostgreSQL fixture data<br/>Logs, customers, orders, employees")]

    Client --> API
    Profiles --> Config
    Config -->|"Snapshot on creation"| API
    API -->|"Run / resume / append"| Coordinator
    Coordinator -->|"Owned, freshly hydrated state"| Engine
    Coordinator <-->|"Ownership and durable lifecycle"| Store
    Engine <-->|"Budget and stage context"| Context
    Context -->|"Bounded summary request"| LLM
    Engine -->|"Fitting decision request"| LLM
    LLM <-->|"Async HTTP"| Model
    LLM -->|"Normalized decisions"| Engine
    Engine -->|"Tool calls"| ActionGate
    ActionGate -->|"Rejected: correlated correction"| Engine
    ActionGate -->|"Allowed"| Tools
    Tools <-->|"Persist intent / start / outcome"| Coordinator
    Tools -->|"Read / compute / validate"| Fixtures
    Tools -->|"Known string result"| Evidence
    Evidence --> Engine
    Engine -->|"Final candidate"| FinalGate
    FinalGate -->|"Rejected: correction"| Engine
    FinalGate -->|"Accepted terminal state"| Coordinator
    Engine -->|"Milestones, context and evidence"| Coordinator
    Store <--> PG
    API -->|"Authoritative reads / paged history"| Store
    Coordinator -->|"Committed milestones / terminal outcome"| API
    API -->|"JSON / SSE"| Client
    Admin --> Store
```

### 14.1 Configuration and request entry

The API creates sessions from trusted profiles and stores both source and resolved configuration identities. The immutable goal, system instructions, model selection, tool declarations, workflow, and policies belong to that session snapshot. Editing `.env` or YAML changes new sessions rather than rewriting existing ones. The configuration arrow in the diagram therefore represents creation, not configuration reload on every decision.

Every session access reloads authoritative state through the repository. Process-local engine caches and locks are conveniences; PostgreSQL owns the revision and execution authority. A mutating operation acquires ownership and then hydrates again before using state to schedule work.

### 14.2 Deterministic control around model decisions

The engine decides when to request a model action, but the model only proposes the next action or final answer. The action gate checks strict argument types, configured tool membership, phase restrictions, global policy, and bindings to captured executed values. Rejection produces correlated correction context without environment invocation.

The latest contract 1.1.0 final-only guard is inside this invocation boundary: after the workflow reaches its terminal `complete` edge, all subsequent tool calls are rejected. The model must synthesize a final answer that passes the independent final gate. The recovery profile additionally requires the exact answer `RECOVERY_OK 10.0.0.5 83`. Workflow-free sessions continue to use their configured tool policy.

Executed evidence and captured artifacts feed the engine separately from model summaries. Evidence advances workflow state; artifacts validate final deliverables and downstream argument bindings. Neither a confident model claim nor an accepted summary replaces these records.

### 14.3 Context protection and compaction

The context component measures the complete adapter request, including tools, instructions, evidence guidance, output reserve, and safety margin. It retains whole assistant-call/tool-result interactions and preserves immutable anchors. Both decision and summarizer requests must fit their configured budgets.

Compaction stages a model-generated proposal, validates its ordered structure, goal, numeric containment, source freshness, byte bound, and useful request reduction, then installs it through the same atomic repository boundary. It remains labeled unverified model memory. Rejected proposals preserve the prior context; source transcript and proposal provenance remain durable. A hard overflow that cannot be safely reduced stops execution before sending an oversized decision request.

### 14.4 Durable execution and transaction boundaries

The execution coordinator surrounds the loop with PostgreSQL ownership, heartbeat renewal, monotonic fencing, cancellation checks, and durable run identities. For valid calls, it commits intent and started-attempt records before invocation. After a known result returns, its outcome, correlated message, consumed pending call, evidence, and artifacts commit together.

The repository checks the expected revision and current owner/fence/expiry, writes related state/history records transactionally, and acknowledges the new revision only after commit. This prevents partial context replacement and stale-owner result installation. Terminal task/run metadata is committed before the API exposes a successful final event.

The diagram's schema 5 store and fixture store are logical boundaries. They may share the configured PostgreSQL database, or fixture placement may use its explicit database override. Current fixture adapters are read-only; production effectful adapters would need appropriate idempotency/reconciliation implementations.

### 14.5 Recovery, history, and administration

Startup coordinates schema readiness and abandoned-run lifecycle recovery without invoking user models/tools. Explicit resume creates a new run/fence and reconciles pending work under its recorded policy. If a tool may have acted but its outcome is unknown, the coordinator requires safe lookup/reconciliation or blocks rather than assuming a retry is harmless.

Cancellation and disconnects stop scheduling at cooperative boundaries while allowing a known in-flight result to settle under valid ownership. History endpoints expose ordered transcript, events, diagnostics, runs, executions, and archive receipts independently of the compact active context.

Keep-all retention remains the default. Administrative export and eligible telemetry pruning pass through verified snapshot/dependency and fenced revision checks; compaction never invokes pruning. These boundaries are what allow a long investigation to resume with its evidence and audit trail intact.
