# Design update: port the upgraded Python agent to Go, Rust and Java

Review date: **2026-10-18**. Target: **observable contract 1.1.0, storage schema 5**. This is the implementation upgrade specification for `agent-go`, `agent-rust` and `agent-java`. It covers the entire upgraded Python runtime, including the final-answer enforcement added after the original Python reference freeze.

## 1. Decision and document authority

We have substantial design material, but before this review it was distributed across the contract, Python architecture/compaction manuals, historical milestone notes and an archived porting guide. There was no current active document combining a source-reviewed delta for each target language with a complete implementation sequence. This document fills that gap.

| Document | Role in the port |
| --- | --- |
| [contract.md](contract.md) | Normative observable behavior required of every implementation |
| **This document** | Current design changes, verified gaps, implementation work packages and acceptance gates |
| [Python architecture](agent-python/docs/architect.md) | Detailed state, repository, transaction and lifecycle algorithm |
| [Python compaction](agent-python/docs/compaction.md) | Detailed payload order, budgeting, grouping, ledger and compaction algorithm |
| [Python operations](agent-python/docs/operations.md) | Dedicated database, startup, inspection, recovery and retention administration |
| [test-cases.md](test-cases.md) | Same complete curl/jq API acceptance suite for all four servers |
| [answers.md](answers.md) | Design rationale and Python-first progression; not a field-by-field implementation specification |
| [digest.md](digest.md) | Testing history and limits of the recorded evidence |
| [Archived porting guide](archive/agent-python-cleanup-20261016/agent-python/docs/porting.md) | Useful historical roadmap; older paths and contract version must not override this document |
| [Frozen Python 1.0.0 reference](archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md) | Immutable earlier source/schema/configuration/fixture/evidence baseline |

The active Python source and current shared `config/` files are the implementation reference for contract 1.1.0. The archived 1.0.0 source alone is insufficient: it predates the final-only workflow guard and `tool-recovery.yaml` exact-output setting. Preserve the archive unchanged. A reproducible release freeze of the newer implementation is separate from this source-review document.

The user reports successful completion of the entire current `test-cases.md` suite against agent-python. This review accepts that as user-reported HTTP acceptance. It did not rerun tests, inspect every new test artifact, or convert that report into new PostgreSQL crash/rollback/performance qualification. Older deeper qualification evidence remains scoped as recorded in the digest.

## 2. Reviewed source and scope

The review inspected the four runtime implementations, configuration/model adapters, fixture tools, PostgreSQL repositories and HTTP controllers, their implementation docs, and the parent contract/planning/testing/digest documents. Findings below describe the working-tree source at review time; they are not live probes of the target servers.

| Reviewed group | SHA-256 of source inventory |
| --- | --- |
| Python runtime (`agent-python/src/agent_python/*.py`) | `e0c2f8d2c41642c6ea6378c5e6deb24bc39719dd1eeedeeaae5129e67018a022` |
| Shared six profiles (`config/*.yaml`) | `1023cdd4fc0af9713743d958eec5d188506f40aed9388b04de2470adb6f5d728` |
| Go application (`agent-go/cmd/server/*.go`, `agent-go/internal/agent/*.go`, excluding tests) | `f1b3ba25dfabdeb6665da2cd32a60c459e97d5a254a59746d04beed4b461141d` |
| Rust application (`agent-rust/src/*.rs`) | `6c71e68dc34597e69229920f19a2414708c561069d77bde9f71cb76d7f27eff7` |
| Java application (`agent-java/src/main/java/**/*.java`, `agent-java/src/main/resources/application.*`) | `9bc8a675f084031095f740df94331626ad4e60ea63f8196b9a9e0c8617fc20b0` |

Each inventory hash is SHA-256 over sorted records `repository-relative-path`, a NUL byte, lowercase SHA-256 of the file's bytes, and a newline. Only the specified source files are included; no `.env`, database contents, credentials or model weights are read into these identities. These hashes identify this review's inputs, not a claim that those exact bytes were independently requalified here.

No target runtime is upgraded by this document. Python is the implemented reference; all target-language work packages remain pending until implemented and tested. Internal organization may be idiomatic to each language. The required behavior, JSON types, configuration policies, database guards and shared assertions cannot vary by language.

Equivalence is assessed under the same controlled inputs, configuration, model responses and environment. Generated UUID values, timestamps, transport chunks, live natural-language wording and latency need not be identical. A configured `completion.exact`, typed tool result or protected state invariant is still strict. Live-model business success and controlled runtime equivalence are complementary checks.

### How to execute the checks specified under each DU

Every DU section below has focused checks with stable IDs, pass criteria, execution availability and a measurement policy. The same checks apply to Go, Rust and Java. Run focused checks after implementing the corresponding coherent slice; run the complete shared suite at final qualification. A package is not complete while its required checks are unimplemented, failed, skipped or missing evidence.

| Mode in the check tables | Execution and current availability |
| --- | --- |
| **Curl** | Commands already exist in [test-cases.md](test-cases.md). Run its prerequisites/startup/curl-terminal setup, then the named cases/sections. Available now against Python; a target must implement the required behavior before it can pass. |
| **Controlled** | Required shared Python checks with scripted model/tool responses or golden input/output vectors. Target logic must be exposed through an appropriate test adapter. Restore/adapt archived checks or implement missing ones during the relevant DU; no new consolidated runner is implemented by this document. |
| **SQL** | Required Python-controlled tests using real disposable PostgreSQL schemas and the actual target repository/service/adapter. Python observes and injects failures; it must not perform the target's writes through Python's own repository and label that a Go/Rust/Java test. Adapters/checks remain to be implemented or restored. |
| **Fault** | Required Python process/socket controller launching the actual target worker(s), exercising deliberate failures and saving partial evidence. Target launch/fault/cleanup adapters remain to be implemented or restored. |
| **Workload** | Bounded measurement drivers defined below, to be adapted from the archived Python workload design. These are not runnable new script commands until the shared runner and target adapters are implemented. |

All test assertions and orchestration remain Python; small target-language inspection/fault adapters may expose the actual implementation without adding public production test endpoints. Existing archive scripts are source material, not directly runnable port qualification. Each implementation turn must provide the user with **exact executable CLI commands**, prerequisites, expected exit/output and artifact directory for its ready checks. Do not invent filenames/commands for runners that do not exist, or run tests on the user's behalf.

Save check ID, target source/config identity, scenario/seed, pass/failure/timeout, expected/actual observations, logs and relevant state/events/run/transcript records. Save partial evidence on failure. No automatic retries that turn failed trials into passes. A dependency not yet implemented is recorded as pending, not passed. A live curl scenario may corroborate a controlled check but cannot replace deliberate invalid-response, transaction or crash injection.

During a development stage, run checks whose dependencies are ready and carry later integration checks as explicit pending work; that does not prevent implementation of the next dependent slice. Mark the whole DU complete only when all its required checks pass. A check explicitly conditional on a cross-language storage-interchange claim is reported as not assessed while that claim is excluded; it never becomes a passed interoperability check by omission.

Required bounded performance workloads are concentrated in **DU-04 (context)** and **DU-09 (storage)**. DU-07/08 require observed renewal/cancellation/recovery timings, and DU-10 records export/prune timings. Other packages need focused correctness checks and counters, not a fresh live-model benchmark after every edit. Share one workload result across the DU requirements it actually covers rather than rerunning the same workload for each section. No unchanged Python benchmark rerun or compaction tuning is requested.

## 3. Current implementation comparison

**Present** means source contains the stated facility, not that it was requalified in this review. **Earlier/partial** means useful code exists but does not satisfy the upgraded contract. **Missing** means the reviewed target runtime has no equivalent facility.

| Capability | Python | Go | Rust | Java |
| --- | --- | --- | --- | --- |
| Trusted relative YAML references and raw file hash | Present | Earlier/partial | Earlier/partial | Earlier/partial |
| All configuration policies/defaults and canonical resolved identity | Present | Missing new fields | Missing new fields | JSON retained, new policies not enforced |
| Strict tool schema/JSON validation; actionable feedback | Present | Missing equivalent validator | Missing equivalent validator | Missing equivalent validator |
| All native tool calls preserved and correlated | Present | Processes first call | Processes first call | Processes first call |
| Workflow evidence/phase transitions | Present | Earlier/partial | Earlier/partial | Earlier/partial |
| Exact/pattern/JSON/artifact final validation and argument bindings | Present | Missing | Missing | Missing |
| No tool invocation after terminal workflow transition | Present, latest change | Missing | Missing | Missing |
| Invalid/empty/no-progress bounds and classified model errors | Present | Missing equivalent controls | Missing equivalent controls | Missing equivalent controls |
| Full-request hard budgets and safe compaction | Present | Earlier content estimate/slice | Earlier content estimate/slice | Earlier content estimate/slice |
| Validated typed ledger, non-destructive rejection, provenance | Present | Missing | Missing | Missing |
| Stable transcript IDs/order and revision/commit identity | Present | Missing | Missing | Missing |
| Atomic state/context/history/event/run/tool writes | Present | Separate snapshot/event writes | Separate snapshot/event writes | Separate reactive snapshot/event statements |
| PostgreSQL leases, heartbeat and stale-owner fencing | Present | Missing | Missing | Missing |
| Durable run/tool intent/attempt/result and recovery policies | Present | Missing | Missing | Missing |
| Cooperative cancel/disconnect and abandoned-run recovery | Present | Request context only | Spawn/channel lifecycle only | Reactive stream lifecycle only |
| Consistent active-context hydration and external diagnostics | Present | Whole snapshot | Whole snapshot | Whole snapshot |
| Seventeen required functional API routes | Present | Eight earlier routes | Eight earlier routes | Eight earlier routes |
| Archive/retention eligibility, verified telemetry pruning | Present | Missing | Missing | Missing |
| Native asynchronous model/database operations | Present | Useful goroutine/pgx foundation | Useful Tokio/SQLx foundation | Useful WebFlux/R2DBC foundation |
| PostgreSQL fixture tools | Present | Present, substantive output differences | Present, validation/format differences | Present, typed-output/diagnostic differences |

All targets currently define only the earlier two-table session/event initializer. None implements the upgraded storage repository merely because the Python-created additional tables exist in the database. Existing target writers can overwrite snapshot JSON without the new guards; do not use an earlier binary to mutate upgraded Python sessions.

### 3.1 Go-specific findings

Source: [engine.go](agent-go/internal/agent/engine.go), [config.go](agent-go/internal/agent/config.go), [openai.go](agent-go/internal/agent/openai.go), [tools_postgres.go](agent-go/internal/agent/tools_postgres.go), [server main](agent-go/cmd/server/main.go), [environment loader](agent-go/cmd/server/env.go).

- The engine and HTTP server maintain separate `Session`/`State` and message/workflow representations, converting through JSON. Both must carry every new field or be replaced by one authoritative representation; otherwise new state silently disappears during conversion.
- `Config`, `Memory`, `LLM` and tool definitions lack upgraded execution/completion/recovery/budget policy fields. YAML decoding can therefore succeed while ignoring behavior the profile requires. Add explicit defaults and semantic validation before exposing a profile.
- The model adapter reads only `ToolCalls[0]`. Invalid tool arguments become final-answer text. Replace this with complete batch normalization and recoverable structured rejection.
- `allowed` grants tool access when no phase is active, including a completed workflow. Final acceptance only checks workflow completion. Port the final-only gate and all completion checks.
- The current dotenv loader lets existing process variables win, including `MAIN_MODEL`. Python explicitly gives the repository `.env` `MAIN_MODEL` priority. Preserve the user's freely editable model setting without pinning a particular model.
- Run/stream locks are process-local; append has no equivalent database ownership. Several save/stream errors are ignored. Replace ignored errors and independent writes with authoritative transactions and database conflicts.
- Fixture differences are substantive: log/revenue validators use fixture constants rather than deriving expected results as Python does; the unnormalized-key revenue branch returns zero instead of attempting the failing SQL; schema inspection is abbreviated; Phase 1 omits `corrupted_row_ids` and full parser diagnostics; the city report omits the large normalization trace. These alter model feedback and stress pressure even if final answers match.

### 3.2 Rust-specific findings

Source: [engine.rs](agent-rust/src/engine.rs), [main.rs](agent-rust/src/main.rs), [tools.rs](agent-rust/src/tools.rs).

- Typed configuration/session structures omit the new runtime fields. Preserve the complete defaulted config and state rather than deserialize away settings needed for restart/hash/policy behavior.
- `decide` uses only the first native call and can turn invalid arguments or absent final content into apparent final text. Normalize all calls and classify empty/malformed responses separately.
- `compact` slices the last message count, uses content-only estimates plus lifetime step triggers, and installs a generated fallback on summary failure. Replace the entire algorithm; extending its trigger alone is insufficient.
- `allowed` permits tools once the workflow has no active phase. Add exact/artifact completion validation and the terminal final-only gate.
- `load` converts database/decoding failures to absence via `.ok()?`; callers can return 404 for an actual storage failure. Distinguish invalid ID, unknown ID and database/integrity failures.
- Local Tokio mutexes serialize only a single process. The spawned SSE task and channel are not durable cancellation acknowledgement, fencing or recovery. Keep renewal independent of awaited model/tool work and supervise disconnected tasks through durable run state.
- The dotenv loader also skips keys already in the process environment. Align `.env` `MAIN_MODEL` precedence. Existing Python-style string/JSON helpers are useful starting points, but do not implement the newer hash/budget/ledger protocols by themselves.

### 3.3 Java-specific findings

Source: [AgentEngine.java](agent-java/src/main/java/com/statedriven/agent/AgentEngine.java), [AgentController.java](agent-java/src/main/java/com/statedriven/agent/AgentController.java), [SessionRepository.java](agent-java/src/main/java/com/statedriven/agent/SessionRepository.java), [ConfigRepository.java](agent-java/src/main/java/com/statedriven/agent/ConfigRepository.java), [ModelClient.java](agent-java/src/main/java/com/statedriven/agent/ModelClient.java), [FixtureTools.java](agent-java/src/main/java/com/statedriven/agent/FixtureTools.java).

- Retaining a Jackson config tree preserves fields but does not apply their validation/defaults or implement their semantics. Materialize and validate a complete canonical config before storing it.
- The engine/model adapter implements the earlier single-call/final/compaction logic and permits tools after workflow completion. Replace those control paths and add strict completion/artifact/binding checks.
- Repository `save` chains an upsert and event insertion using `.then`; it does not establish one transaction for the complete aggregate. Use a single transactional connection/context for all writes and guards.
- `within` synchronizes construction of a `Mono` and is not a lock over its asynchronous execution; the controller paths also do not establish the required database ownership. Use PostgreSQL leases/fences rather than treating that helper as concurrency protection.
- The SSE path subscribes work inside `Flux.create`. Explicit cancellation, subscriber disposal and durable in-flight settlement must be coordinated; dropping a subscriber or leaving an inner subscription running is not the required cancellation lifecycle.
- `/configs` currently returns full tool definitions in its `tools` field instead of Python's tool-name list. Correct the response shape as well as adding the missing routes. The synchronous `try/catch` around a returned reactive tool chain also does not convert later asynchronous SQL failures into the required read-only tool-error feedback.
- Fixture Phase 1 currently emits employee IDs as strings and sorts the combined IDs lexicographically, whereas Python emits numerically ordered integers. Some diagnostic bodies are shortened, and some validator hints do not depend on success. Align keys, types, order, data and required diagnostic text with Python.
- Java currently uses R2DBC-specific URL/user/password variables and a legacy database fallback. The current shared CLI guide translates the root URL. The port must use the same `.env`-selected `universal_state` database; any connection-variable bridge remains startup administration, not different session semantics. Verify Spring property precedence for `.env` `MAIN_MODEL` explicitly.

## 4. DU-01 — Configuration, defaults and strict values

Reference: [config.py](agent-python/src/agent_python/config.py), [validation.py](agent-python/src/agent_python/validation.py).

1. Resolve the real trusted config root and requested real path. Reject empty/absolute/non-YAML/non-file/traversal/symlink escapes. Hash the original bytes before expansion. Expand supported `${VAR:-default}` syntax in parsed string values, recursively; Python parses YAML before expansion.
2. Load root `.env` with Python's model precedence. New sessions use the current `MAIN_MODEL`; resumed sessions use their stored resolved configuration. Never introduce a Qwen/Llama identity safeguard or silently substitute models.
3. Deep-copy engine configuration and protect raw ref/hash, canonical resolved config/hash and original goal. Validate registered tool references, unique tool/phase IDs/result labels/binding targets, edges, evidence references and binding source/target validity.
4. Implement every field below even when none of the six profiles currently exercises a nondefault value. Preserve `null`, an omitted allow-list and an explicitly empty allow-list as different cases where Python does.

Honor `AGENT_CONFIG_ROOT` and the session/fixture connection settings. The current Python default database is `universal_state`; replace Go/Rust/Java's legacy `elite_rag` fallbacks when porting. Optional `AGENT_FIXTURE_DATABASE_URL` deliberately selects a separate fixture database; Rust already has a separate pool, while Go and Java currently use their session client for fixture operations and need equivalent override support. No administrative example should redirect work to another project's database or override the `.env` model with a placeholder.

| Block | Required fields / defaults |
| --- | --- |
| `llm` | `base_url`; `api_key_env=OPENAI_API_KEY`; `temperature=0.1`; positive `timeout_seconds=90`; `tool_call_mode=native` or `single`; optional positive `max_output_tokens` |
| `memory` base | positive required `max_tokens`, `max_steps`; required `raw_turns_to_keep>=1`; `hybrid_reflection=true`; optional positive `run_max_steps`, `compaction_interval_steps` |
| `memory` hard budget | `context_window_tokens=32768`, `reserved_output_tokens=1024`, `safety_margin_tokens=256`; output reserve + margin must be less than window |
| `memory` summary bounds | `max_summary_tokens=4096` canonical UTF-8 bytes; `summary_output_tokens=512`; `compaction_max_attempts=2` (1..5), `compaction_max_passes=3` (1..10) |
| `memory` efficiency | `compaction_min_growth_tokens=512`; `compaction_min_interval_steps=0`; `optional_compaction_min_pressure_ratio=0`, `optional_compaction_min_reclaimable_tokens=0`, `optional_compaction_min_reduction_tokens=0`, `optional_compaction_min_reduction_ratio=0`; ratios in 0..1 |
| `execution` | `tool_policy=auto` or `none`; `max_consecutive_invalid_responses=3` (1..50); optional `max_no_progress_steps>=2`; ordered `argument_bindings=[]` |
| `completion` | nullable `instructions`, `exact`, `pattern`, `json_schema`; `forbidden_patterns=[]`, `result_checks=[]` |
| Result check | `kind` is `markdown_table`, `number`, `json_value`; tool, typed argument/result constraints, source `path`, `answer_path`, unique `label` |
| Tool recovery | nullable declaration; modes `read_only`, `repeatable`, `idempotent`, `reconcile`; `max_attempts=2` (1..10) |
| Workflow/output | optional graph; evidence `required=true`; transition `when=phase_complete`; optional phase `allowed_tools`; `output.verbose_setup=false` |

Strict tool argument validation supports only the schema assertions in `validation.py`: type/properties/required/additionalProperties/items/enum/const, numeric bounds, string length/pattern and array length. Unknown assertion keywords fail configuration loading. Reject duplicate JSON keys and nonfinite/out-of-range numbers. Do not coerce strings into booleans/numbers or booleans into numbers. A finite integer-valued JSON number satisfies an integer schema. This tool-argument rule must not be confused with the framework's separate HTTP request parsing behavior.

### DU-01 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-01-C01 | Controlled | Resolve all six current profiles against the same synthetic environment; compare complete defaulted config and raw/resolved hash preimages with Python vectors. Missing/default/null/nondefault policies retain their specified meanings. |
| DU-01-C02 | Controlled | Reject traversal, absolute/non-YAML paths, symlink escapes, unknown tools, duplicate tool/phase/label/binding targets, invalid graph edges/references, unsupported schema assertions and invalid bounds. Valid unrelated profiles still list normally. |
| DU-01-C03 | Controlled | Use temporary environment/config fixtures to give inherited shell and dotenv different model values. Dotenv `MAIN_MODEL` wins for new sessions; changing it does not alter an existing session's snapshot. Do not edit the user's live `.env` or pin a particular model. |
| DU-01-C04 | Controlled | Feed boolean/string/numeric type confusions, duplicate JSON keys, nonfinite values, integer-valued numbers and boundary enum/range/length cases through the target validator. Invalid arguments produce actionable errors and zero adapter invocations; valid cases remain valid. |
| DU-01-C05 | Curl | Run the shared preflight and section 11 invalid-config request. Six profile refs are discoverable and traversal returns 422. This confirms HTTP metadata/rejection, not the complete default/hash checks above. |

**Benchmark:** none required. Record the negative/positive case matrix, resolved identity observations and adapter invocation count. Historical starting points: archived `test_model_configuration.py`, `verify_test_profiles.py`, `test_milestone_1.py` and the current config/validation source. Their port adapters and exact CLI commands must be supplied during implementation.

**Done:** all current profiles resolve to equivalent defaulted configuration, identity bytes and validation behavior; mistyped actions are rejected before a tool adapter can observe them. DU-01-C01 through C05 pass when their dependencies are available.

## 5. DU-02 — Model adapter, wire history and diagnostics

Reference: [llm.py](agent-python/src/agent_python/llm.py), [model_errors.py](agent-python/src/agent_python/model_errors.py), `AgentEngine.run`.

- Build the complete OpenAI-compatible payload: model, wire messages, public tool schemas, tool choice, temperature and output cap. Never send private recovery policy as a function schema.
- Native mode preserves every function call in order. Normalize each call independently, retaining raw argument text and any parsing error. Generate fresh IDs for missing/invalid IDs; reject reused IDs using both active memory and historical call lookup. Supply a correlated string result for every call, including rejected actions.
- Single mode sets `parallel_tool_calls=false` and splits only outgoing historical wire groups into one assistant-call/result pair each. Keep original session batch identities/order/results intact. Disabled-tool policy removes the tool list and sets `tool_choice=none`; the runtime still rejects returned tool actions.
- Empty/missing/malformed choice/content is a recoverable invalid response, not a fabricated successful answer. Invalid argument JSON is an invalid action, never a final answer containing an error sentence.
- Preserve bounded error categories: `model_timeout`, `model_transport_error`, `model_http_error`, `model_template_error`, `invalid_model_response`, `empty_model_response`, context errors and action/completion rejection. Keep provider bodies/credentials out of public diagnostics.
- Retain exact endpoint/root fallback behavior: alternate completion route only on 404. Do not retry other provider failures as a hidden model or policy substitution.
- Preserve request timing/byte/budget diagnostics and known-versus-unknown usage. Async HTTP and database waits must not prevent lease renewal.

### DU-02 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-02-C01 | Controlled | Script one, multiple and mixed valid/invalid native calls in one response. Every call is retained in order with a unique correlated result; malformed arguments never become accepted final prose and no valid call is silently dropped. |
| DU-02-C02 | Controlled | Test native, single and disabled tool policies. Compare complete outgoing request/message vectors, not just answers: single-mode pairs preserve every original call/result; disabled mode removes tools and uses `tool_choice:none`. Private recovery settings do not leak into schemas. |
| DU-02-C03 | Controlled | Return absent/empty/malformed choices/content, invalid call structures and duplicate/missing call IDs. Correct categories and bounded correction occur; no fabricated successful final is returned. Historical ID reuse is also exercised once DU-05/09 storage is available. |
| DU-02-C04 | Controlled | Inject HTTP 404, other HTTP failures, non-JSON bodies, template errors, transport failure and timeout. Only 404 triggers the documented alternate route; public diagnostics remain bounded and omit credentials/provider bodies. Count actual attempted requests. |
| DU-02-C05 | Curl | Run profile Tests 1, 2 and 6 after engine integration. Required action/result/validator traces are present. These live cases do not force multi-call, malformed-provider or wire-policy paths. |

**Benchmark:** no independent throughput/live-model benchmark. Capture attempted decision/summary HTTP calls, fallback count, error category, serialized request bytes and elapsed samples in the controlled cases. A slow-call renewal check is shared with DU-07-C03 rather than duplicated here. Historical starting points: archived `test_milestone_2.py`, `test_milestone_2_followups.py`, `test_milestone_1_followups.py` and provider-wire checks in the archived runner.

**Done:** controlled multi-call, malformed/empty, single-wire, disabled-tool, provider failure and timeout cases give the same semantic decisions/correlations/errors as Python. DU-02-C01 through C05 pass.

## 6. DU-03 — Engine, evidence, completion and progress

Reference: [engine.py](agent-python/src/agent_python/engine.py), [completion.py](agent-python/src/agent_python/completion.py).

```text
run allowance = requested max_steps OR config.run_max_steps OR config.max_steps
limit = lifetime step_count + allowance
recover committed pending calls before new model decisions
while task active and lifetime step_count < limit:
  checkpoint ownership/cancellation
  advance at most one applicable workflow edge in declaration order
  prepare safe context; require complete request budget
  checkpoint; invoke model; increment lifetime decision count; archive decision
  malformed/empty decision -> bounded corrective feedback
  final candidate -> workflow + format + executed-artifact checks
    reject/archive invalid candidate; accept only if every gate passes
  tool batch -> normalize IDs; archive assistant calls; persist pending batch
    for each call, using the phase fixed for this batch:
      validate declaration, strict args, bindings, global/phase policy
      if workflow already terminal: reject without invocation
      otherwise persist intent/start, invoke/reconcile, stage outcome
      append correlated result; update evidence/artifacts only for executed calls
      consume pending call; atomically persist outcome/message/evidence/state
    update invalid/no-progress accounting
exhaustion -> failed task with step_limit, recoverable through explicit resume
```

Workflow evidence uses exact tool name, typed argument containment, configured case-insensitive text markers and typed parsed result containment. Evidence keys remain `phase_id:completion:index` or `phase_id:transition-N:index`. `required:false` diagnostics do not gate completion. `always` edges remain unconditional; do not convert them into evidence edges. Retain array-containment semantics from `_contains`, including the distinction between booleans and numbers.

Current missing argument/result/text constraints must appear after raw history near the next decision, and in feedback for premature finals. Phase labels are not tool names. Preserve recent guidance over historical summary claims.

### Final-only state, including the latest Test 6 fix

A configured workflow reaching its `complete` edge has `active_phase=null`. Its allowed tool set is **empty**, not unrestricted. Reject every subsequent tool invocation before creating execution intent or calling the adapter; record correlated error feedback and ask for the final answer. The state transition occurs between model decisions; a returned batch retains the phase under which it was accepted until that batch settles.

A workflow-free session does not enter this terminal workflow state. Its normal configured tool policy remains in force. Resuming a completed workflow preserves its completed evidence and therefore remains final-only; it is not an implicit request to rerun its tools or reset the graph.

`completion.exact` uses literal equality, including surrounding prose/whitespace. `pattern` is a full match; forbidden patterns are checked separately. JSON output must parse strictly and satisfy its schema. Markdown tables must match executed headers/row keys/values without missing, extra or duplicate rows; numeric cells use the reference decimal/integer semantics. Labeled numbers must occur once and equal the captured executed result. JSON-value checks compare typed values at the configured paths. Capture artifacts from successful executed results satisfying their declared tool/argument/result constraints; model prose cannot supply an artifact. Tool arguments bound to artifacts must equal the executed source value without coercion.

Current `tool-recovery.yaml` has `completion.exact: RECOVERY_OK 10.0.0.5 83`. Its normal run executes naive analysis, robust analysis and validation. Post-completion attempts may appear as rejected feedback, but must not create more durable execution records. Do not hardcode this string or the fixture answers into the generic engine.

Consecutive invalid decisions stop at their configured bound. Optional no-progress detection fingerprints durable workflow/artifacts and newly observed successful outputs for workflow-free tasks; call IDs/new prose/summary acceptance alone are not progress. Preserve bounded correction warnings, invalid counters, progress history and resume resets.

### DU-03 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-03-C01 | Controlled | Exercise required/optional error evidence, missing exact argument constraints, out-of-phase calls, `always`/`evidence`/`phase_complete` edges and one fixed phase per returned batch. Only actual matching executions establish evidence; model/summary claims do not. |
| DU-03-C02 | Controlled | Reject premature/empty finals; extra prose around an exact literal; bad regex/JSON outputs; wrong/duplicate/extra/missing table rows or numbers; fabricated artifacts and incorrect bound arguments. Accept valid deliverables from captured executed results. |
| DU-03-C03 | Controlled | After a terminal transition, deliberately return a registered tool call, an unknown call and a batch. All receive correlated rejection with zero dispatch/intent records. Follow with a malformed final, then a valid exact final; only the valid one completes. Repeat with a resumed completed workflow and a workflow-free session to verify their different policies. |
| DU-03-C04 | Controlled | Feed repeated empty decisions and alternating rejected/redundant successful actions. Verify configured invalid/no-progress limits, warning, counters and resume resets. Run allowance advances from lifetime count; it never triggers compaction by itself. |
| DU-03-C05 | Curl | Run all six profile tests, especially Test 5 continuation and updated Test 6 exact answer plus three durable executions. This is the live end-to-end engine check after storage/API dependencies are implemented. |

**Benchmark:** focused cases record task success, accepted/rejected decisions, tool dispatches and model calls. No separate full live benchmark per engine edit; retain these counts for P7's shared profile comparison. Historical starting points: archived `test_deliverable_validation.py`, `test_result_bindings.py`, `test_workflow_guidance.py`, `test_milestone_2_followups.py`. The latest final-only behavior also needs new controlled coverage; earlier archived passes do not cover it automatically.

**Done:** Test 6's exact answer and three intended durable executions pass, alongside the same general action/workflow/artifact/progress tests for all profiles. DU-03-C01 through C05 pass.

## 7. DU-04 — Context integrity, budgets and compaction

Reference: [context.py](agent-python/src/agent_python/context.py), `AgentEngine._payload`, `_compact_if_needed`, [full compaction specification](agent-python/docs/compaction.md).

Replace the targets' content-length/4 estimate, lifetime-step compaction trigger, arbitrary raw slice and destructive summary fallback. The replacement has these required stages:

1. Group every assistant tool-call batch with all correlated tool results. Expand the raw-message minimum backward to complete interaction boundaries. Reject orphaned/missing/duplicate correlation rather than truncating a broken group.
2. Assemble protected system/goal, configured final requirements, unverified ledger, executed artifacts, raw messages, known bindings, current workflow guidance and final synthesis in Python's documented order. Persistent message metadata is absent from wire messages. Summary data is a user-context block explicitly marked unverified, not a new trusted system rule.
3. Estimate the **actual complete** decision or summarizer payload as compact UTF-8 JSON bytes plus 32 per message. Fit requires input estimate + max(output reserve, payload output cap) + margin <= configured window. Names saying tokens do not make this a provider tokenizer.
4. Keep optional soft threshold/growth/cadence separate from run allowance. Optional spacing/pressure/reclaimable/minimum-reduction gates skip low-value summaries. Hard overflow bypasses optional gates immediately. The configured largest affordable complete historical prefix is summarized; excluded complete groups remain raw.
5. Validate a proposed ledger on a copy: exact goal, ordered populated XML/headings, no attributes/nesting/entities/extra prose, canonical UTF-8 byte cap, source numeric containment using decimal semantics, unchanged captured source/state and useful full-request reduction. The ledger records `goal`, ordered `model_claims`, `dead_ends`, `hypothesis`, `source_sha256`, and `verification=unverified_model_summary`.
6. Rejection retains original active raw messages and previous ledger, journals available proposal/reason/provenance and supplies bounded feedback. There is no fabricated fallback summary installed over history. Suppress repeated requests against an unchanged failed source; bound attempts and passes.
7. Acceptance replaces only active references/ledger, retains immutable source messages, and records accepted proposal/source IDs/hash/prior ledger/retained IDs and reduction metadata. Recheck complete decision fit after all attempts/passes; an accepted reduction alone does not authorize an oversized request.

Neither structural nor numeric summary validation proves semantic truth. Workflow/evidence/artifacts remain authoritative outside model-generated claims. Optional compaction skips completed workflows; hard-budget protection remains active in final synthesis.

### DU-04 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-04-C01 | Controlled | Cross raw-tail boundaries with single/multiple calls and results. Preserve complete groups, exact raw text, goal and result correlation; orphaned/incomplete/duplicate groups fail before a model request. Compare the complete decision and summary payloads/budget calculations with reference vectors. |
| DU-04-C02 | Controlled | Inject empty/truncated/malformed XML, attributes/nesting/entities, wrong/extra sections, changed goal, unsupported numbers, excessive canonical bytes, stale proposals and nonreducing summaries. Active history/ledger remain unchanged on rejection; available proposal/reason/source provenance is retained. |
| DU-04-C03 | Controlled | Exercise cadence/growth/spacing/pressure/reclaimable/reduction gates, unchanged failed source, retry/pass bounds and completed-workflow optional skip. Hard pressure bypasses optional gates. Irreducible overflow sends zero decision requests and fails explicitly. |
| DU-04-C04 | Curl | Run Tests 3 and 4, then inspect section 14 diagnostics. Both deliver the same verified employee result; stress preserves eleven evidence records and has accepted compaction. No fixed summary count or latency is substituted for task success. |
| DU-04-B01 | Workload | Run the bounded context grid below through the actual target context/engine adapter with deterministic responses. Every cell/repetition is present once, with no hidden retry or skipped failure. |

**Required benchmark:** use the [existing bounded workload design](archive/agent-python-cleanup-20261016/agent-python/docs/qualification-workloads.md): headroom / fitting soft pressure / one estimated input unit beyond the hard allowance × historical result size 1 / 8 / 32 KiB × valid / invalid summary, three repetitions = **54 trials**. Keep four historical call/result pairs and one retained raw pair at 1 KiB as in that design; hold source groups and scripted summaries constant across languages and calibrate pressure using the complete target payload and the same estimator. Headroom makes zero summary calls; valid useful summaries permit fitting soft/hard completion; invalid soft summaries preserve history and may continue only while fitting; invalid hard summaries preserve history and stop before decision dispatch. Match the workload's two-attempt bound.

Record raw success/error, decision/summary invocation counts, before/after budget, accepted/rejected summaries, reduction, preserved source/raw identities and elapsed samples. Report per-cell sample count, median, maximum and observed percentile with the small-sample limitation. No arbitrary faster-than-Python SLA or new compaction tuning is introduced. Adapt archived `test_compaction_efficiency.py`, context checks and `benchmark_qualification_workload.py`; direct Python-engine execution is not a target-language benchmark.

**Done:** equivalent controlled payloads and group boundaries, preserved history on rejected summaries, bounded useful compaction, correct hard-overflow stop and intact provenance. DU-04-C01 through C04 and B01 pass. No compaction retuning is part of the port.

## 8. DU-05 — State representations and immutable history

Reference: [models.py](agent-python/src/agent_python/models.py), [transcript.py](agent-python/src/agent_python/transcript.py), [storage_state.py](agent-python/src/agent_python/storage_state.py).

| Representation | Fields/invariants to implement |
| --- | --- |
| Config snapshot | Original config ref/source hash, fully defaulted resolved config and canonical resolved hash; protected goal/system/policies |
| Message | Stable UUID `message_id`, shared transcript `sequence`, UTC `created_at`, role/content/name/tool-call ID/calls; wire serialization emits only non-null protocol fields |
| Transcript record | Session sequence, message/record UUID, `kind`, JSON payload, nullable run UUID, lifetime `decision_step`, original timestamp; append-only identity/content |
| Hot session | ID/goal/config anchors, `revision`, next sequence, ledger ID/history-complete flag, task/run statuses/IDs/times/reason, step/run counters, pending calls, last error |
| Durable control state | Workflow/evidence/transitions, captured completion artifacts, invalid/no-progress counters/fingerprint/output history, compaction steps/failed-source fingerprint, budget and lifetime diagnostic counters |
| Active context projection | Goal message ID, ordered raw message IDs, active ledger ID/typed ledger and the exact session revision |
| Diagnostics | Three append-only families: `compaction_events`, `compaction_attempts`, `compaction_decisions`; recent 32/family in normal reads, lifetime counts/savings/rejections separately |
| Transient write outboxes | Pending transcript, execution, diagnostic and retention records, pending commit UUID; ownership UUID/fence; do not expose ownership/outbox internals as model context |

One session sequence numbers messages **and** audit records. Do not renumber retained raw messages or summaries after compaction; message sequences need not be consecutive because intervening audit records share the counter. Initial goal is sequence 1 for a fresh session; engine revision 0 becomes persisted creation revision 1.

Public session/message responses preserve Python's declared defaults and nullable fields. The omit-null, protocol-only `wire_payload` is a separate projection for the model; do not use it to strip IDs, counters, default values or required null fields from the HTTP state response.

Journal at least the Python record kinds: model decisions, rejected finals/decisions, run start/finish/interruption, tool intent/attempt/failure/outcome/uncertainty, summary acceptance/rejection and legacy history-gap records. Deep-copy staged payloads so later state mutation cannot alter immutable pending history.

### DU-05 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-05-C01 | Controlled | Stage messages, decisions, rejected actions/finals, summaries and run/tool audit records. IDs are stable, the shared sequence advances once per record, copied payloads do not mutate with later state, and wire messages exclude persistence metadata. Public state still exposes declared fields/defaults. |
| DU-05-C02 | SQL | Write/reload across append, compaction, bounded stop, resume and process restart. Existing immutable rows and IDs remain identical; retained raw/context references and next-sequence counter agree with the transcript. |
| DU-05-C03 | SQL | Attempt changed-payload ID reuse, sequence collisions/gaps, cross-session sources and inconsistent prior-ledger references. Reject atomically; identical permitted replay produces no duplicate history. |
| DU-05-C04 | Curl | Run section 12 transcript paging, section 11 restart and Test 6 resume. For full immutable-prefix proof, retain the transcript evidence from section 15 disconnect/resume as well. |

**Benchmark:** none separate. Record transcript count, next sequence and stable identity/prefix observations. History-growth performance belongs to DU-09-B01; do not rewrite or reload all history just to collect these counts. Historical starting points: archived `test_milestone_3.py`, `test_milestone_3_postgres.py` and transcript checks in the archived runner.

**Done:** same message/record identities survive compaction, resume and restart; snapshots no longer serve as the only historical record. DU-05-C01 through C04 pass.

## 9. DU-06 — PostgreSQL schema and atomic commit

Reference: [persistence.py](agent-python/src/agent_python/persistence.py), [execution_store.py](agent-python/src/agent_python/execution_store.py), [storage.py](agent-python/src/agent_python/storage.py). Exact earlier DDL is preserved in [schema.sql](archive/agent-python-cleanup-20261016/reference/python-1.0.0/schema.sql); compare current initializer code before adopting a schema snapshot.

| Table | Required contents/keys beyond the old baseline |
| --- | --- |
| `agent_sessions` | UUID PK; hot state/resolved config; raw ref/hash; revision; last commit UUID/fingerprint; lease owner/fence/expiry; storage version; timestamps |
| `agent_session_transcript` | PK `(session_id,sequence)`; unique `(session_id,message_id)`; kind/payload/run/step/timestamps |
| `agent_session_context` | Session PK; matching revision; goal/ledger FKs into session transcript; ordered JSON raw IDs; typed ledger |
| `agent_session_events` | Global serial event ID; session FK; payload/revision/commit UUID; unique non-null session/commit identity |
| `agent_runs` | Run UUID PK; session/owner/fence/status/start/end/reason/last decision step/cancel flag/time; unique session/run |
| `agent_tool_executions` | Execution UUID; unique session/call; session/run FK; tool/typed args/policy/key/attempts/status/result; owner/fence/times; terminal result-nullability constraint |
| `agent_session_diagnostics` | PK session/family/positive ordinal; immutable JSON payload |
| `agent_session_calls` | Historical session/call/message mapping with transcript FK; no scanning all historical messages for reused call IDs |
| `agent_archive_receipts` | Archive UUID; session/revision/checksum/manifest/timestamp; dependency receipt |
| `agent_schema_migrations` | Component/version key and applied time; preserve shared schema-5 readiness semantics |

All ten tables are required. Existing primary/unique/page indexes and foreign keys are part of the design, not optional performance decoration. The runtime initializer contains legacy import logic; fresh `universal_state` setup does not require data from another project. Legacy import, if exercised, must mark unavailable history honestly and remain transactional/idempotent.

### Save protocol

1. Stage message/diagnostic outboxes; validate the protected resolved configuration identity. Allocate a pending commit UUID once and retain it until acknowledgement.
2. Compute the complete commit fingerprint using Python's ordered preimage: hot snapshot, active full messages, canonical ledger, typed ledger, transcript outbox, execution outbox, diagnostics outbox, retention outbox, resolved config hash, event.
3. Begin **one transaction on one connection**. Lock the session row. Guard owner/fence/expiry. Reject writes to legacy storage projections that have not been imported.
4. An identical commit UUID **and identical fingerprint** acknowledges its already committed revision. Otherwise check expected revision and immutable goal/config/ref/source hash. Creation requires expected revision zero; existing writes advance revision by one with CAS.
5. Write proposed hot snapshot, ordered immutable transcript, call lookup, diagnostics and run/tool changes; write the event and context at that same revision; write an explicitly staged retention action if present. Validate summary source/prior-ledger fingerprints and referenced active message/ledger identities.
6. Recheck fencing and commit. Only after acknowledgement update the caller's revision and clear pending outboxes/commit identity. Rollback or uncertain acknowledgement must not discard the recovery identity or partially clear pending work.

Do not implement this as multiple autocommit statements or swallow a failed save and then emit success. Identical replay is not permission to replace immutable transcript content. Conflicting UUID/sequence/payload, gaps, invalid summary references, changed config/goal or stale revision must fail.

Startup takes **session-level advisory lock `734910003`** across DDL commit **and subsequent abandoned-run metadata recovery**. Release in cleanup. A transaction-only DDL lock does not preserve this boundary. Check current schema before repeating DDL; propagate failures rather than retrying until a startup test happens to pass. The current compatibility marker uses `component='agent-python', version=5`; a port must recognize shared readiness rather than create conflicting per-language storage meanings.

### DU-06 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-06-C01 | SQL | Initialize a disposable empty schema, inspect all ten tables/constraints/indexes, repeat initialization and exercise supported legacy imports. Repetition is safe; genuine unavailable history is marked; the normal fresh database path needs no other-project migration. |
| DU-06-C02 | SQL | Inject failure after each aggregate-write stage: snapshot, transcript, call map, diagnostics, run/tool updates, event, context and staged retention. Every affected table rolls back; caller revision/outboxes are not falsely acknowledged. |
| DU-06-C03 | SQL | Load two copies at one revision, save one and try the stale one. Exactly one revision transition succeeds; stale writer gets the specified conflict and cannot overwrite committed state. Also reject changed goal/config/raw anchor. |
| DU-06-C04 | SQL/Fault | Lose a commit acknowledgement and retry the same commit identity/content. Acknowledge the original revision without duplicate transcript/event/outcome; changed identity/content is rejected. Validate restored full payload and context revision. |
| DU-06-C05 | Fault | Start at least two actual target workers together on an empty and an already-current disposable schema. DDL commit plus abandoned recovery stays under the shared advisory guard; all workers start without hidden retries/deadlocks and real failures propagate. |
| DU-06-C06 | Curl | Run section 11 server restart and sections 12–14 history/page inspection. These corroborate durable reads but do not replace the rollback, duplicate-commit or startup fault cases. |

**Benchmark:** no standalone SQL throughput target. Capture commits/revisions, transaction failures and elapsed observations. Use DU-09-B01 for measured repository growth/concurrency after these correctness gates; do not benchmark an unsafe repository first. Historical starting points: archived `test_milestone_3_postgres.py`, `test_milestone_5_postgres.py`, `test_milestone_5_startup_postgres.py` and process/startup workers.

**Done:** real PostgreSQL rollback, identical duplicate acknowledgement, identity collision, stale writer, restart hydration and concurrent startup evidence pass before port conformance is claimed. DU-06-C01 through C06 pass.

## 10. DU-07 — Ownership, durable tool execution and recovery

Reference: [execution.py](agent-python/src/agent_python/execution.py), [execution_store.py](agent-python/src/agent_python/execution_store.py), recovery registries in [tools.py](agent-python/src/agent_python/tools.py).

Claim a generated owner UUID with an atomic conditional update; increment the fence on each acquisition. Defaults are a 30-second lease and renewal every 10 seconds. Use database clock/expiry guards. Renew/release/save/outcome operations must match owner and fence; expiry or heartbeat failure stops the old worker. Never execute from a snapshot loaded before acquisition: **claim, then reload authoritative state**. Local locks may optimize, but cannot grant execution authority or queue stale work in place of the database 409.

Every validated call has a stable execution UUID, session/call identity, typed arguments, immutable recovery policy, idempotency UUID and bounded attempt count. Commit pending intent before execution; commit a started attempt before entering the environment. On return, commit outcome, correlated message, consumed pending call, evidence/artifacts, run state and snapshot together. A tool error string is still a known terminal outcome, and configured diagnostic-error evidence can match it.

| Stored state/policy | Required recovery behavior |
| --- | --- |
| No intent | Validate; persist intent/key, then attempt/start |
| Pending | Continue under current owner and immutable attempt bound |
| Succeeded or failed with known result | Reuse stored result; do not invoke the effect again |
| Started/uncertain and reconciler confirms a string result | Stage confirmed outcome and atomically correlate it with state/evidence |
| Started/uncertain read-only or repeatable | Retry only within the stored permitted policy/attempt bound |
| Idempotent | Require a registered adapter accepting the persisted key; the policy name alone supplies no guarantee |
| Reconcile mode without known outcome | Persist uncertainty and block the run with `recovery_required` |
| Explicit transient adapter signal | Retry only as allowed; normal `ERROR:` text is not an automatic retry signal |
| Lease lost while effect/result is outstanding | Reject stale database commit; next owner reloads and applies the recovery policy |

All current fixture tools are registered read-only; unspecified future tools default to reconciliation. Python's idempotent/reconciler registries are extension points, not proof that every adapter already implements them. Fencing protects database commits; it cannot undo an external effect or provide generic exactly-once execution. Document adapter guarantees honestly.

### DU-07 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-07-C01 | SQL/Fault | Race two actual owners for one session. One claim wins; the other receives conflict. After expiry/takeover, old owner renew/release/save/result operations cannot change the successor's lease/state. Verify increasing fence and claim-then-reload. |
| DU-07-C02 | SQL/Fault | Observe intent/start before invocation and atomic outcome/message/evidence/pending-call consumption afterward. Crash before intent, after intent, after started, after a synthetic effect and after outcome commit/before acknowledgement. Recovery follows stored policy and known terminal results are not invoked again. |
| DU-07-C03 | Fault | Deliberately slow model, database and tool waits beyond one 30-second lease period while renewal runs independently. Ownership remains valid. Separately disable/fail renewal to force genuine expiry; stale work stops and its commit is fenced. |
| DU-07-C04 | Controlled/SQL | Exercise read-only, repeatable, idempotent-with-real-key-adapter, missing idempotent adapter, confirmed/unconfirmed reconciliation, explicit transient signal and ordinary error-string cases. Count invocations/attempts; stable key/policy and retry bounds hold; unresolved effects block. |
| DU-07-C05 | Curl | Run section 16 two-worker competing run/message checks and Test 6 execution history. HTTP 409 and the intended execution records support the ownership/recovery observation; they do not prove every stale-owner or uncertain-effect boundary. |

**Measurements:** record acquisition winner/conflict, owner/fence/run IDs, renewal timestamps, actual invocation/effect count, attempts, durable outcome and recovery duration. These are required concurrency/recovery probes, not a new live-model throughput sweep. Reuse DU-06/08 crash artifacts when the same trial proves both boundaries. Historical starting points: archived `test_milestone_4.py`, `test_milestone_4_postgres.py`, `test_milestone_4_crashes.py`, `test_async_io.py` and `test_async_io_postgres.py`; adapt real worker/fault/effect adapters for each language.

**Done:** a stale process cannot renew/release/commit over its successor, known outcomes are not replayed, uncertain effects block without an actual safe policy, and slow async operations do not starve heartbeats. DU-07-C01 through C05 pass.

## 11. DU-08 — Run lifecycle, cancellation and service boundaries

Reference: [api.py](agent-python/src/agent_python/api.py), `ExecutionCoordinator`, `reconcile_abandoned`.

Keep task status (`active`, `complete`, `failed`) distinct from run status (`idle`, `running`, `complete`, `failed`, `cancelled`, `interrupted`, `blocked`). Each resume starts a new run UUID and increasing ownership fence; original goal/config, workflow evidence and committed transcript remain intact. Resume resets transient invalid/no-progress/final-error state as Python does. Recover pending calls before accepting new user content. Appending ordinary messages is only allowed to an active, unowned session without pending calls.

`/cancel` serializes against committed run-start/finish so its response names the run it actually targeted. JSON request disconnect and SSE disconnect both request cooperative cancellation. Let a current invocation settle and persist a known result; then stop at the next scheduling checkpoint and commit cancelled lifecycle. Keep renewal active during settlement. Do not abort an effect and report it as safely undone. Do not leave an independent detached worker continuing new decisions after cancellation.

For SSE, separate the run task from the reader sufficiently to settle known work after socket closure. Once headers are sent, an ownership/revision conflict is a structured SSE error rather than a new HTTP status. Before streaming begins use the specified HTTP 409 where applicable. A successful final event must follow the atomic terminal task/run commit, and post-restart reads must show the same committed answer.

Startup and the operational reconciliation command detect abandoned running rows and mark lifecycle metadata interrupted without silently invoking user models/tools. Explicit resume decides how pending execution is recovered. Preserve a committed terminal result if startup discovers an incomplete lifecycle acknowledgement.

### DU-08 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-08-C01 | Curl | Run Test 5 continuation, Test 6 bounded stop/resume and section 11 restart. Goal/config remain unchanged; new runs advance identity/count/fence; completed workflow continuation stays final-only. |
| DU-08-C02 | Curl/Fault | Run section 16 explicit cancel against the second process while the first owns a committed run. Cancel response names the target run, known results settle, no new actions execute after cancellation acknowledgement, and continuation uses a new owner/run. |
| DU-08-C03 | Curl/Fault | Run section 15 with an SSE reader kept alive until deliberate socket closure. Confirm an actually running trial, same-run cancellation within the existing 120-second probe ceiling, an unchanged three-second stopped snapshot, then error-free completion with a new run/higher fence, all eleven evidence records and preserved transcript prefix. A task that finishes before deliberate close is unexercised, not passed. |
| DU-08-C04 | Fault | Exercise JSON-request disconnect, cancellation during an in-flight synthetic invocation, worker loss and abandoned startup. Preserve the known result or uncertain state as appropriate; startup never invokes user tools/models; explicit resume applies recovery policy. |
| DU-08-C05 | Controlled/SQL | Delay/fail the terminal commit while observing output. No successful final event is exposed before task/run/result state commits. Test HTTP pre-stream conflicts and post-header SSE conflicts separately. |

**Measurements:** save closure/cancel-request, durable acknowledgement and resume-completion timestamps; per-trial observed durations; run/fence/revision pairs; pre-close/stopped/stable/final snapshots; SSE/events/transcript and worker logs, including failures. The 120-second ceiling permits an invocation to settle and is not a production cancellation SLA. No additional throughput benchmark. Historical starting points: archived `test_disconnect_probe.py`, managed deployment/startup workers and `verify_milestone_5_deployment.py`. Port-specific lifecycle adapters remain required.

**Done:** true two-process conflicts produce 409; cancellation targets the right identity; actual socket close yields a durable stable stop; resume uses a new run/fence and preserves the exact transcript prefix. DU-08-C01 through C05 pass. A client curl timeout alone is insufficient proof.

## 12. DU-09 — Selective hydration, bounded history and API pages

Reference: `storage.HYDRATE`, `SelectiveStorage.get`, page methods in [storage.py](agent-python/src/agent_python/storage.py), API methods in [api.py](agent-python/src/agent_python/api.py).

Normal reads select state/config/context, ordered active goal/raw messages, active ledger and up to 32 diagnostics per family from one MVCC statement snapshot. An explicitly equivalent consistent read transaction is acceptable, but independent uncoordinated reads can mix revisions. Validate session/state/context revisions, immutable goal, ordered distinct message IDs/sequences, resolved config hash, ledger kind/content and typed-ledger identity before running or returning state.

Hot snapshot excludes active message bodies, compacted text/typed ledger and growing diagnostic arrays; these are hydrated from references. Keep lifetime counters, completeness/archive metadata and progress/workflow state separately. Full history is an explicit paginated read. Instrument logical snapshot/read/write bytes, database roundtrip time and wall time before optimizing; establish pre/post-change model-payload equivalence first.

### Required public API: all seventeen routes apply to every upgraded port

| Method/path | Request or page controls | Response/behavior to preserve |
| --- | --- | --- |
| `GET /health` | None | status/config-root/available-count metadata |
| `GET /configs` | None | Array of ref/source hash/name/tool names/phase IDs/verbosity |
| `POST /sessions` | `user_prompt`, optional `config_ref` | HTTP 201 `{session,message}`; protected anchors |
| `GET /sessions/{id}` | UUID | Current hydrated `SessionState` |
| `POST /sessions/{id}/messages` | nonempty `content` | `{session,message}`; active/unowned/no-pending rule |
| `POST /sessions/{id}/run` | optional `max_steps` in 1..50 | Bounded JSON `{session,message}`; already-terminal session does not create a new run |
| `POST /sessions/{id}/run/stream` | Same run body | SSE; terminal session rejected with 409 |
| `POST /sessions/{id}/resume` | optional nonempty `content`, optional `max_steps` | SSE continuation/new run; pending calls prohibit new content |
| `POST /sessions/{id}/cancel` | No body required | session ID, `cancel_requested_for` run-ID array, message |
| `GET /sessions/{id}/transcript` | `after_sequence=0`, `limit=100`, optional `through_sequence` | session ID, snapshot revision, history-complete, watermark, `entries`, `next_after_sequence` |
| `GET /sessions/{id}/events` | `after=0`, `limit=100`, optional `through` | session ID/watermark/entries/next-after/archive ID |
| `GET /sessions/{id}/diagnostics/{family}` | ordinal `after=0`, `limit=100`, optional `through` | family/watermark/lifetime total/entries/next-after/archive/completeness |
| `GET /sessions/{id}/runs` | optional UUID `after`, `limit=100` | `runs`, `next_after`; order by `(started_at,run_id)` |
| `GET /sessions/{id}/executions` | optional UUID `after`, `limit=100` | `executions`, `next_after`; order by execution UUID, not invocation time |
| `GET /sessions/{id}/archives` | optional UUID `after`, `limit=100` | `archives`, `next_after`; UUID order; empty array is valid |
| `GET /sessions/{id}/retention` | No body | keep-all default, eligibility, unresolved/running counts, protected/prunable tables and prerequisites |
| `GET /runs/abandoned` | `limit=100` | Bounded `runs` array; no implicit replay |

Page limit range is 1..1000. Sequence/event/ordinal cursors and watermarks are nonnegative; diagnostic families are exactly the three listed above. Malformed session/cursor UUIDs are 422, unknown valid sessions 404. A supplied watermark is capped to the current maximum. Empty pages return the input cursor as the next cursor; do not invent a null end marker where Python returns the prior cursor. Run/execution pages expose current lifecycle rows, not a fixed multipage snapshot. Preserve `{type,code,message,recoverable}` ownership errors and expected/current revision fields for revision conflicts.

Event IDs are global serial IDs and need not be consecutive within one session. Transcript sequences share messages/audit records; diagnostic ordinals are per session/family. Keep these distinct cursor domains.

SSE `data` JSON includes status, interim tool/phase/compaction/correction events, final result and errors. Tool-completed events include tool, call ID, typed arguments, string output and `executed` flag. Update embedded/generated OpenAPI schemas with the routes, query constraints and new state/error fields; an advertised endpoint without implementation is a failed requirement.

### DU-09 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-09-C01 | Controlled/SQL | Before optimizing hydration, capture equivalent full-history and active-projection decision payloads at the same revision. After selective hydration they remain identical, including artifact/current-workflow guidance and wire policy. Verify another writer cannot cause mixed state/context/message/ledger revisions. |
| DU-09-C02 | SQL | Seed growing archived messages/diagnostics with fixed active context. Normal reads load only active referenced messages and at most 32 diagnostics per family while lifetime counters/completeness remain accurate. Historical call-ID lookup uses the normalized mapping. |
| DU-09-C03 | Curl/SQL | Run sections 12–14 and endpoint validation cases: limit 1/1000, empty/last pages, cursor domains, fixed watermarks, wrong UUID/family, malformed versus absent session. All seventeen routes/required state fields exist; cursors advance without missing/duplicating bounded append-only records. |
| DU-09-B01 | Workload | Execute the seven-cell PostgreSQL slices described below against the target repository, using real disposable schemas. All cells/repetitions/clients complete without hidden retry, lost update or payload mismatch. |

**Required benchmark:** reuse the [bounded SQL workload](archive/agent-python-cleanup-20261016/agent-python/docs/qualification-workloads.md), three repetitions of seven cells: `(clients, archived pairs, output bytes)` = `(1,0,1024)`, `(4,0,1024)`, `(8,0,1024)`, `(1,100,1024)`, `(1,1000,1024)`, `(1,0,8192)`, `(4,100,8192)`. This is **21 batches / 60 fresh sessions**, independent-session concurrency, not a same-session ownership-race benchmark. Hold active context/output constant in history comparisons, seed outside the measured batch, and retain the two get/save/get cycles per client.

Every revision advances exactly once per save and complete model payloads survive hydration. At fixed 1 KiB output, hot-snapshot logical size range across 0/100/1000 archived pairs stays below 8192 bytes; selected-read size range between 100 and 1000 history, after recent diagnostics saturate, also stays below 8192 bytes. These are the existing bounded payload gates, not latency guarantees.

Record raw snapshot/selected-read/write JSON bytes; get/save wall and database-roundtrip samples; client revision/fence/elapsed; per-cell n, median, maximum and observed percentiles. Keep output levels separate. Capture query plans/index observations at the declared history levels; do not alter production indexes for an experiment. Logical JSON bytes exclude protocol/WAL/disk amplification. Adapt archived `benchmark_storage.py`, `benchmark_qualification_workload.py`, `test_milestone_5_postgres.py` and storage-projection checks. Run the measured target repository; using Python's repository would measure Python again.

**Done:** every route and assertion in the shared curl suite applies unchanged; old history growth does not force normal reads/writes to deserialize/rewrite it all. DU-09-C01 through C03 and B01 pass.

## 13. DU-10 — Retention and operational capabilities

Reference: [retention.py](agent-python/src/agent_python/retention.py), four [Python operational scripts](agent-python/scripts).

Keep all data by default. Export a consistent repeatable-read, read-only snapshot using `sda-archive-1` canonical JSONL with explicit table order, row order, counts, watermarks and per-table/whole-content SHA-256. Reject partial/truncated/trailing/cross-session archive data. An export is not authorization to prune an active or unresolved session.

An explicit telemetry prune requires a current verified export matching session revision and database row digests, a terminal task, no running lifecycle/pending/uncertain execution, current ownership/fence and all previous archive dependencies available and valid. Commit its receipt, state/revision/event and allowed deletions together. Retain at least 32 diagnostics per family and the configured event tail. Protect transcript, active context, calls, runs, executions and archive receipts. Compaction never initiates pruning.

Provide equivalent operational abilities for current-schema initialization, deterministic fixture generation, abandoned-run inspection/metadata reconciliation, export/plan/explicit prune. Administrative drivers can remain shared Python CLI tooling, as requested; they are not substitutes for implementing the runtime API/storage/recovery semantics in the port's language. Do not expose new destructive HTTP endpoints just to duplicate an administrative script.

### DU-10 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-10-C01 | Curl | Run section 14 archive/retention inspection. A fresh completed session exposes keep-all policy, no unresolved/running lifecycle and protected/prunable table metadata; no archive receipt is required until export/prune administration is deliberately exercised. |
| DU-10-C02 | SQL | Through the target retention adapter, export a known disposable terminal session at a consistent revision. Independently verify JSONL header/rows/table order/counts/checksums/watermarks and roundtrip receipt compatibility. Reject truncation, trailing data, cross-session rows and corrupt manifests. |
| DU-10-C03 | SQL/Fault | Attempt prune with active/running/pending/uncertain state, stale revision, expired/stale owner, missing/corrupt prior archive and mismatched database digests. Every request fails without partial deletion/receipt/state change. |
| DU-10-C04 | SQL | On disposable data, perform one valid explicit telemetry prune. Keep required tails, counters/completeness and permanent dependency receipts correct; all protected transcript/context/call/run/execution rows remain identical. Repeated/chained archive dependencies remain verifiable. |
| DU-10-C05 | Controlled/SQL | Drive normal runs, compaction, restart and reads without administrative prune. No history is silently removed; administrative operations require explicit selection. |

**Measurements:** record exported/retained/pruned row counts, canonical bytes, manifest hashes, source/receipt revisions and export/verification/prune elapsed time. No independent export-throughput SLA or production cleanup benchmark. Tests that mutate/prune operate only on disposable test data. The existing Python administrative scripts can inspect common tables, but running their Python retention implementation does not qualify the target's implementation; the shared test driver must invoke its actual adapter. Historical starting points: archived storage/retention cases in `test_milestone_5.py`, `test_milestone_5_postgres.py` and the current reference `retention.py`.

**Done:** export/receipt formats and retention dependencies interoperate, protected rows remain intact, and the normal service never silently prunes. DU-10-C01 through C05 pass.

## 14. DU-11 — Fixtures and serialization compatibility

The six shared current profiles are `log-analysis.yaml`, `revenue-analysis.yaml`, `employee-audit.yaml`, `compaction-stress.yaml`, `completion-no-workflow.yaml`, and `tool-recovery.yaml`. All ports use these files and the same PostgreSQL fixture recipe/data. Expected fixture sizes are 650 logs, 2200 customers, 6502 orders and 1000 employees. SQLite/flat-log archives are not active dependencies.

Keep fixture computation/validation outside the generic engine. Do not add fixture answer constants to the loop, artifacts, compactor or model adapter. Preserve result keys, scalar types, ordering, formatting consumed by text evidence and diagnostic payload sizes. In particular restore Go's omitted error windows/report trace before comparing stress behavior. A lower compaction count caused by shorter inputs is not an efficiency improvement.

Current curl assertions inspect nested tool-result strings such as `\"valid\": true`; minified JSON returned by some target tools does not satisfy that literal substring. Match the Python fixture-tool string representation for the current suite. A future shared change to parse nested JSON semantically must be applied to all servers' assertions and versioned consistently, not introduced as a per-language exception.

### Byte identities are separate protocols

| Identity/estimate | Required recipe |
| --- | --- |
| `config_sha256` | Original YAML bytes before expansion |
| `resolved_config_sha256` | Defaulted config with only `resolved_config_hash`'s listed default omissions; sorted compact JSON, UTF-8, no NaN |
| Summary source SHA-256 | Exact goal/previous-ledger/prefix wrapper from `context.source_text`; sorted historical **wire** JSON with Python's spaced separators |
| Commit fingerprint | Ordered complete preimage from `SessionRepository.save`; sorted compact JSON |
| Archive checksum | Canonical JSONL/newline/table-order and manifest boundaries from `retention.py` |
| Request estimate | Compact request JSON byte length plus 32/message, using actual adapter policy/output fields |

Do not apply one general serializer indiscriminately. Python's recipes are not a claim of RFC 8785 canonicalization. Float/negative-zero/exponent rendering, Unicode/HTML/XML escaping, null/default omission, timestamps and array order can change bytes across languages even when JSONB values look similar. Go's default HTML escaping and float rendering, Java's numeric node/decimal rendering and Rust's null/default/order behavior need explicit compatibility checks. Java/Go/Rust regular-expression engines also differ from Python; implement the supported matching behavior or fail configuration explicitly rather than silently weakening it.

Use cross-language golden vectors from the current Python outputs for configuration/source/commit/archive bytes, numeric/boolean equality, non-ASCII and special-character strings. Cross-server session hydration requires matching these identities in addition to matching final business outputs. Never silently change the meaning of already-stored hashes.

### DU-11 checks and measurements

| Check ID | Mode | Required exercise and pass criterion |
| --- | --- | --- |
| DU-11-C01 | Controlled | Compare canonical config/source/commit/archive preimage bytes and SHA-256 against current Python golden vectors. Include Unicode/XML/HTML escapes, key/array ordering, null/default omission, integer/float/exponent/negative-zero cases and the exact default omissions. Compare bytes as well as parsed JSON. |
| DU-11-C02 | Controlled/SQL | Invoke each actual target fixture tool with canonical valid, expected-error and invalid argument inputs. Match result keys/types/order, numeric employee IDs, diagnostic markers/windows/report trace size and validator behavior; generic engines contain no fixture constants. Validate strict argument rejection before invoking coercive adapters. |
| DU-11-C03 | SQL | Read-only fingerprint all four fixture tables using the common canonical row recipe and counts, not counts alone. Record actual database/schema identity without credentials. Verify the deliberate fixture-database override routes tool reads to the selected database. |
| DU-11-C04 | Curl | Run all six profile tests with the current shared YAML and independent oracles, including formatted revenue and strict recovery output. No language-conditioned assertions, abbreviated stress payloads or model pins. |
| DU-11-C05 | SQL/Fault | Before claiming cross-language session interchange, create with one server and hydrate/resume with another for the required writer/reader pairs. Config/source/hash/provenance, typed state, revision/owner/fence and archives remain valid. Until this gate passes, keep incompatible writers isolated. |

**Benchmark:** no separate fixture throughput sweep. Record canonical byte/hash matches, fixture row fingerprints, tool-result byte counts and profile outcomes. Use the same corrected diagnostic payloads in DU-04/09 workloads and P7 comparisons; short Go traces must not be credited as a performance gain. Historical starting points: archived `verify_reference.py`, `test_reference_integrity.py`, `verify_test_profiles.py` and fixture payload/oracle snapshots. Regenerate current golden vectors deliberately where contract 1.1.0 differs from the old freeze; do not rewrite archived identities.

**Done:** source/config/fixture identity and typed outputs match the chosen current reference; incompatible legacy snapshots are not accidentally accepted as schema-5 state. DU-11-C01 through C04 pass. Cross-language storage-interchange claims additionally require the pairwise C05 evidence explicitly; otherwise record that claim as not assessed.

## 15. Proposed implementation modules by language

These are suggested boundaries, not required filenames. Keep dependencies explicit and avoid rebuilding the entire runtime in one controller.

| Responsibility | Go change | Rust change | Java change |
| --- | --- | --- | --- |
| Config/state/strict values | Expand `config.go`; one authoritative session/message model; dedicated validation | Expand models/config; separate validation from `main.rs` | Validated/defaulted config/state models around Jackson; strict parser/validator |
| Adapter/errors | Replace first-call `Decision`; update `openai.go`; structured errors | Extract adapter/error logic from `engine.rs`; normalize all calls | Expand `ModelClient`; typed normalized batch/error outcomes |
| Engine/artifacts | Extend engine; separate completion/bindings/progress | Split engine/completion modules | Split compressed `AgentEngine` into reviewable control/completion components |
| Context/ledger | New context/budget/ledger components | New context/budget/ledger modules | New context/budget/ledger components with decimal/XML handling |
| Repository/transcript | Move SQL from `cmd/server` into transactional repository | Extract SQL repository from `main.rs` | Replace two-statement `SessionRepository` with complete R2DBC transactional aggregate |
| Execution coordinator | Independent goroutine heartbeat, owner/fence and supervised cancellation | Tokio heartbeat task/ownership coordinator with explicit task lifetime | Reactive scheduled heartbeat/ownership context independent of model/tool chains |
| API/pages/admin | Thin handlers and matching embedded OpenAPI; propagate write errors | Thin Axum handlers/pages; distinguish absent versus failed storage | Thin controller/pages; supervise subscriptions/disposal and propagate transaction failures |

Ports must be native implementations of this state machine. Do not shell out to the Python agent or proxy all work to it and call that a language port. Shared Python testing/administration is allowed; Python is not an embedded runtime dependency of the agent service.

## 16. Delivery order and acceptance gates

Upgrade and qualify **Go, then Rust, then Java**, reusing the same tests and reference vectors. Finish one language's complete qualification before claiming it as the reference for the next. Continue treating the upgraded Python source/contract as the target for all three.

| Stage | Work packages | Required reviewable output before next stage |
| --- | --- | --- |
| P0: pin inputs | Review inventory, current profiles, source/config/fixture vectors | Recorded reference identity and source/delta checklist; compatible test database/schema plan |
| P1: values and adapters | DU-01, DU-02, fixture corrections from DU-11 | Same defaults/types/hashes; controlled multi-call/errors/wire cases |
| P2: deterministic engine | DU-03 | Workflow evidence, exact/artifact checks, bindings, no-progress and final-only guard |
| P3: context | DU-04 | Complete groups, budget/payload equivalence, non-destructive ledger and provenance cases |
| P4: atomic history | DU-05, DU-06 | Schema-5 repository, stable identities/revisions, rollback/replay/restart tests |
| P5: durable ownership | DU-07, DU-08 | Leases/fencing, durable intent/outcomes, cancellation/startup/crash behavior |
| P6: bounded storage/API | DU-09, DU-10 | Equivalent active payload, all API routes/pages, bounded reads/writes and protected retention |
| P7: full qualification | All packages | Shared complete curl suite plus controlled action/context/SQL/crash measurements with the actual port running |

Implement focused coherent slices and run their CLI checks as work proceeds; the final full pass remains distinct. The user runs every test/check/benchmark from CLI. Provide the actual commands and progress/evidence paths for each stage when that stage is implemented. Do not launch checks automatically or repeat unchanged successful Python workloads as part of a port.

## 17. Qualification requirements and limits of curl acceptance

The current [shared curl guide](test-cases.md) applies in full to all four servers. It covers all six YAML profiles plus invalid config/restart, ordered history/pages, diagnostics/executions/retention inspection, actual SSE disconnect/resume and two-process ownership/cancel/resume. Missing routes or fields are failed port requirements, not skipped tests.

Keep automated test orchestration and assertions in **Python**, shared by all four agent languages. Backend selection may change launch/stop/fault/storage adapters and report labels, never expected answers/policy/pass criteria. Existing archived internal scripts import Python modules or assume Uvicorn and their old paths; do not relabel them as successful target-language runs. Restore/adapt relevant tests deliberately into shared test tooling before executing them against a port.

| Qualification class | Required focused evidence for each port |
| --- | --- |
| Controlled model/actions | Every call in a batch; malformed/empty/provider failures; strict types; invalid-ID correction; exact/JSON/table/number checks; binding/progress/final-only behavior |
| Context/summary | Complete groups and wire equivalence; hard overflow sends no unaffordable request; malformed/stale/invented-number summaries preserve history; bounded attempts/useful reduction |
| PostgreSQL atomicity | Inject failure within aggregate writes and observe complete rollback; replay identical commit; reject conflicting identities/gaps; restart/hydrate; expected stale-revision conflict |
| Ownership/concurrency | Two real processes; competing run/input; old owner renew/release/save/outcome rejected; renewal survives deliberately slow model/SQL/tool waits |
| Crash boundaries | Kill actual target worker before intent, after intent, after started, after effect/before outcome, after committed outcome/before acknowledgement; verify policy-aware resume without fabricated exactly-once claims |
| Cancellation/startup | True socket reader kept alive until deliberate close; durable cancelled run/stable stop/new fence; simultaneous worker startup/DDL/recovery; abandoned metadata recovery without effect execution |
| Hydration/retention | Payload-equivalence at one revision; archive/history growth with fixed active output; measured bytes/time; paging boundaries; stale/corrupt/missing-dependency exports cannot prune |
| Cross-language storage | If resuming another server's sessions is claimed, exact canonical/config/source/archive identities, state types and owner/revision guards across writer/reader combinations |

Passing the curl guide establishes its HTTP scenarios. It does not by itself establish the injected rollback/crash cases, row-by-row fixture fingerprints or production performance. Record final task success, decisions/model calls, known usage, latency, compaction attempts/rejections/reduction and logical repository bytes/database time under the same declared factors/levels. Preserve failures. Exclude unmeasured WAL/bloat, production p95/p99, model training, external exactly-once, multi-host failover and authentication claims. No new compaction tuning is requested.

## 18. Completion checklist and handoff

The source-review status board starts as follows. Update a cell only with the corresponding implementation and user-run evidence; an older successful business example is not completion of the package.

| Work package | Check set | Go | Rust | Java |
| --- | --- | --- | --- | --- |
| DU-01 Configuration/strict values | C01–C05 | Complete (user-run evidence in `digest-go.md`) | Pending | Pending |
| DU-02 Model adapter/wire/errors | C01–C05 | Complete: controlled suite, full `go test ./...`, and C05 Tests 1, 2 and 6 passed in user-run evidence (see `digest-go.md`) | Pending | Pending |
| DU-03 Engine/evidence/completion/progress | C01–C05 | Complete: focused DU-03 tests, full `go test ./...`, and C05 Tests 1–6 passed in user-run evidence (see `digest-go.md`) | Pending | Pending |
| DU-04 Context/budgets/compaction | C01–C04, B01 | Complete: controlled checks, 54-trial B01 grid, regression/full suite, `git diff --check`, and C04 Tests 3/4 with compaction diagnostics passed in user-run evidence (`digest-go.md`) | Pending | Pending |
| DU-05 State/immutable transcript | C01–C04 | Complete: controlled/SQL checks, regression/full suite, live append-stop-restart-resume-pagination and compaction transcript integrity passed in user-run evidence (`digest-go.md`) | Pending | Pending |
| DU-06 Schema/atomic repository | C01–C06 | Pending | Pending | Pending |
| DU-07 Ownership/tool recovery | C01–C05 | Pending | Pending | Pending |
| DU-08 Lifecycle/cancellation/service | C01–C05 | Pending | Pending | Pending |
| DU-09 Hydration/history/API pages | C01–C03, B01 | Pending | Pending | Pending |
| DU-10 Retention/operations | C01–C05 | Pending | Pending | Pending |
| DU-11 Fixtures/serialization | C01–C04; C05 for storage interchange | Pending | Pending | Pending |

For each target language record implementation status and evidence location for DU-01 through DU-11 and P0 through P7. A port is complete only when:

- Every current configuration policy is implemented and restart-safe, not merely accepted by YAML parsing.
- All seventeen functional routes and their wire/state/error/page fields satisfy the same shared assertions.
- Context, evidence/artifacts, final-only completion, transcript/provenance, revisions, leases/fences, intent/result recovery, disconnect and retention rules survive the corresponding failure tests.
- Fixture keys/types/text/diagnostic pressure and canonical identities are matched; no language-specific answer constants, model pins or relaxed assertions are introduced.
- The user-run full curl evidence and required deeper qualification are recorded with the actual target source/config/model identities and scoped measurements.
- That language's docs/OpenAPI describe its upgraded implementation, and its old pending/historical banner is removed only after conformance review.

This review changes design/navigation documentation only. The implementation tables remain a port backlog. The Python HTTP pass is recorded as the user's report; historical freezes and test results remain unmodified. Future implementation turns should cite this document and the specific work-package IDs they complete.
