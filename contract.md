# Universal state-driven agent observable contract

**Contract version: 1.1.0. Python reference baseline: python-1.0.0. Storage schema: 5.** Contract 1.1.0 adds a final-only workflow state: after all required phases complete, no tool may execute and the model must produce a final answer. The recovery profile also requires exact output `RECOVERY_OK 10.0.0.5 83`. The frozen 1.0.0 files remain the prior source/evidence baseline; the current implementation and contract include these additions. The reference's API advertises 0.5.0 and package metadata advertises 0.1.0; these are separate version namespaces, not parity claims. Frozen files, qualification evidence and exceptions are in [reference/python-1.0.0](archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md).

Python is the reviewed, qualified runtime reference. Go, Java and Rust share the earlier design and fixture tools but have **not** been ported/qualified to this contract. The [historical audit](archive/pre-python-reference-1.0.0/contract.md#12-differences-requiring-conformance-decisions-or-fixes) retains the earlier differences; it is not the current normative contract. Do not infer new parity from an old successful example or copy a Python defect merely to match it.

[design_update.md](design_update.md) is the current source-reviewed implementation upgrade specification for these ports. It maps this contract to the active Python code, concrete target-language gaps and acceptance work packages, including changes after the 1.0.0 freeze.

## Interpretation and conformance

An implementation conforms when the same controlled input/configuration/model responses/environment produce the specified observable state transitions, tool/result correlations, validation, completion, persistence/recovery and API behavior. Internal language/library layout, UUID values, timestamps, transport chunk boundaries and live prose may differ. Natural-language wording and latency are not required to be identical. Array order, JSON types, error categories, durable identities/relations and protected invariants are meaningful.

Use identical HTTP scenarios/assertions against every language. The active guide uses curl/jq; the original shared **Python** scenario/measurement harness is preserved in the archive for future automated qualification. Implementation labels select report/source identity only, never weaker assertions, missing features or different policy. Python engine/SQL/internal crash tests are reference-specific implementation tests; black-box runs do not replace atomicity/fencing/crash evidence for a port. A language port must provide equivalent integration/failure-injection evidence. Any automated controller/adapters should remain Python as requested; the archived managed probes launch Python Uvicorn and require target-specific process adapters for other languages.

The current qualification package consolidates multiple previously user-executed collections; it is not a newly executed monolithic release run. Consolidation/integrity tooling and the original harness are now archived with their reports. Current routine HTTP validation is in [test-cases.md](test-cases.md); deeper SQL/crash requirements remain part of this contract. Existing reports retain their original statuses and hashes.

## Configuration — CFG

- Resolve a nonempty relative YAML `config_ref` below the trusted administrator-controlled root. Default: `log-analysis.yaml`. Reject absolute/traversal/symlink escapes, missing/non-YAML/invalid configurations and unregistered tools with a client error.
- Persist raw-source SHA-256 and fully resolved immutable configuration; retain a separate canonical resolved-config identity. Preserve original goal/configuration across append, resume, compaction and restart. Editing YAML/environment later affects new sessions, not existing snapshots.
- Source hashes identify bytes, not environment-resolved semantics. Shared comparisons record configuration/environment settings without recording credentials. Root `.env` owns `MAIN_MODEL`; no model pin, model-name safeguard or automatic substitution of a different model is permitted. All six reference profiles interpolate this setting. `.env` itself is not frozen or copied.
- Preserve `llm`, `memory`, `execution`, `completion`, `tools`, optional `workflow`, and `output` semantics. Duplicate tool/phase/result-label/binding targets, invalid graph edges, unknown evidence/tools/bindings and unsupported schema assertion keywords fail validation. No executable tool code is installed via YAML.
- Tool JSON validation uses the supported subset: type/properties/required/additionalProperties/items/enum/const, numeric boundaries, string length/pattern, array length. No coercion; booleans are not numbers, numeric strings are not numbers, duplicate JSON keys and nonfinite numbers fail. Integer-valued finite JSON numbers satisfy integer schemas. Source definitions/defaults are frozen in `config.py` and [schema/config source snapshot](archive/agent-python-cleanup-20261016/reference/python-1.0.0/README.md).

## HTTP and lifecycle — API / RUN

| Route | Required observable behavior |
| --- | --- |
| `GET /health` | Status/config discovery metadata; does not certify model or database task success |
| `GET /configs` | Valid registered profiles with relative ref, source hash, agent name, tool names, phase IDs and verbose flag |
| `POST /sessions` | `{user_prompt, config_ref?}`; 201 with `{session,message}` and initial protected goal; prompt/ref nonempty |
| `GET /sessions/{id}` | Current authoritative state, 200; active context/recent diagnostics, not full archival history |
| `POST /sessions/{id}/messages` | Append nonempty content to unowned active session without pending tool work; preserve goal; otherwise 409/use resume |
| `POST /sessions/{id}/run` | Optional `max_steps` in 1..50; bounded JSON result. Already-terminal state returns without a new run |
| `POST /sessions/{id}/run/stream` | Active-session SSE run; terminal session returns 409 |
| `POST /sessions/{id}/resume` | SSE new run, optional nonempty content; reactivate terminal/interrupted work and reconcile pending execution under policy |
| `POST /sessions/{id}/cancel` | Target committed running run identity; cooperative cancellation, known results retained |
| `GET /sessions/{id}/transcript` | Ordered immutable records with sequence cursor/watermark and history-completeness metadata |
| `GET /sessions/{id}/events` | Event cursor/watermark and bounded page |
| `GET /sessions/{id}/diagnostics/{family}` | Bounded family/ordinal page with lifetime count, watermark/archive/completeness metadata |
| `GET /sessions/{id}/runs` | Durable lifecycle/fence/cancel state; ordered bounded page |
| `GET /sessions/{id}/executions` | Tool intent/attempt/outcome/policy/key state; ordered bounded page |
| `GET /sessions/{id}/archives`, `/retention` | Archive receipt pages and keep-all/prune eligibility metadata |
| `GET /runs/abandoned` | Bounded abandoned-run inspection |

All thirteen session routes reject malformed UUIDs before SQL (422); a valid absent UUID returns 404. Concurrent ownership/revision conflicts return structured recoverable 409s. Query limits default to 100, range 1..1000 for paged session histories; supported cursors/watermarks are specified in the frozen `api.py`. Transcript/event/diagnostic watermarks bound append-only retrieval. Run/execution pages are current records per page, not a single frozen multipage lifecycle snapshot.

SSE uses JSON `data:` events for status, interim results, final result and errors. Preserve causal event order and flush tool/phase milestones after the relevant durable boundaries. Setup verbosity is configurable; byte chunk boundaries and status wording are not normative. No successful final event may precede terminal state/run commit. Disconnect requests cancellation; acknowledgement can wait for an in-flight invocation to settle and preserve its known result. Resuming uses a new run UUID and increasing fence. User goals survive new-turn continuation.

Session task status is `active`, `complete` or `failed`; durable run status separately includes `idle`, `running`, `complete`, `failed`, `cancelled`, `interrupted`, `blocked`. A cancelled resumable task can therefore expose task status failed/run status cancelled; that is not final task success. Reaching a step limit is a resumable failure. Startup recovers abandoned lifecycle metadata without silently executing user tools/models.

## Model actions, tools and completion — ACT / LOOP / FIN

- Preserve the immutable system/goal anchors and deterministic evidence outside compacted claims. Model payload order and complete tool-call/result groups must match the reference semantics, including current exact missing constraints near the next action and final synthesis guidance after raw working context.
- Normalize provider single/native function responses, preserving every returned call in order, distinct IDs and correlated results. Invalid JSON/actions/unknown tools/arguments/phase violations become actionable correction context before execution. Do not infer valid arguments by coercion or execute only the first call of an otherwise valid returned batch.
- Respect configured global tool policy and phase `allowed_tools`. Tool results are strings. Expected parsing/validation failures become feedback data, not unhandled API failures. Provider/template/transport/timeout/empty/invalid-decision categories remain distinguishable; do not fabricate success or provider usage.
- Once a configured workflow reaches its terminal `complete` edge, reject all subsequent tool calls before invocation and direct the model to final synthesis. Workflow-free sessions retain their configured tool policy.
- Final candidates must be nonempty and satisfy workflow and configured exact/pattern/JSON/output-format/result-artifact checks. Pattern validation is separate from friendly presentation instructions. Required tables/numbers/JSON are matched to captured executed results, with duplicate/extra/missing/placeholder values rejected as specified by `completion.py`. Result-bound tool arguments must use their executed source values.
- Repeated empty/invalid responses stop at the configured consecutive limit (default 3). Configurable no-progress detection counts repeated/redundant actions even when successful calls alternate with rejected ones; durable progress/resume accounting must agree. Do not spend an entire run on unbounded empty outputs.

## Workflow evidence — WF

Workflow is optional. Validate entry/unique IDs/edges and registered evidence tools. `phase_complete`, `evidence` and `always` edges retain their configured meaning; an always edge is not converted into an evidence gate. Required evidence matches executed tool name, typed argument/result containment and configured text conditions. Numeric equivalence must not collapse boolean/number types. Evidence keys, completed phases and transitions persist independently of compactable history. Error-shaped outputs can intentionally establish configured diagnostic evidence; prose/model claims cannot. Missing required constraints are exposed both near the next action and in premature-final feedback.

When the workflow has no active phase because its configured terminal edge completed, it is in final-only state. The implementation must not invoke any tool from this state, even if the model returns a valid registered tool call. Reject such decisions with actionable final-answer guidance. A final decision must then pass the configured exact/pattern/other completion checks.

## Context and compaction — MEM

[compaction.md](agent-python/docs/compaction.md) specifies the single current implementation/policy guide. Normative invariants:

1. Budget the complete decision and summarizer request, including instructions/tools/evidence/guidance/output reserve/margin. The reference estimator is compact UTF-8 JSON bytes + 32 per message, not provider token usage. Hard overflow must stop before an unaffordable decision request.
2. Keep immutable goal/system and complete ordered recent call/result groups. `raw_turns_to_keep` is a minimum message count expanded to interaction boundaries. Never drop orphaned/partial groups to fit.
3. Separate run allowance/lifetime steps from optional soft/cadence/growth/spacing/pressure/reclaimable/reduction policy. Hard protection bypasses optional skips immediately.
4. Stage summaries and validate ordered structure, unchanged goal, canonical byte bound, source numeric containment, freshness and useful complete-request reduction before replacement. Preserve prior raw history/ledger on rejection. No destructive generated fallback.
5. Mark summary content `unverified_model_summary`; structure/numbers do not establish semantic truth. Preserve executed workflow evidence and completion artifacts separately. A stale provisional phase claim is not authoritative.
6. Archive accepted/rejected proposal, source IDs/hash, prior ledger chain and retained IDs. Install new context/summary identity atomically with snapshot/history/diagnostics/events under revisions and fencing. Keep source records.
7. Bound attempts/passes, avoid repeated requests against unchanged failed source and retain honest diagnostics/savings counters. Never infer useful progress merely from accepted summary count.

## Persistence, fencing and recovery — STATE / STORE / OWN / EXEC

Schema 5's ten session/storage tables and four fixture tables are described in [architect.md](agent-python/docs/architect.md) and frozen [schema.sql](archive/agent-python-cleanup-20261016/reference/python-1.0.0/schema.sql). Ports may use idiomatic internal models; shared-table interoperability requires the same schema, canonical hashing, identifiers and transaction guards. Do not run incompatible legacy writers on upgraded sessions.

Messages have stable IDs, per-session ordered sequences and separate wire payloads. Append-only transcript includes decisions, run lifecycle, intent/attempt/outcome, rejected finals/summaries and provenance. Canonical resolved configuration/goal and existing transcript identities are protected; inconsistent duplicates/gaps/sources fail. Identical duplicate commits have explicit idempotent handling. Snapshot/context/transcript/event/diagnostic/call/run/execution updates commit together or roll back together. Revision CAS rejects stale writers. Active hydration is one consistent MVCC statement; explicit history retrieval is separate. Recent diagnostics are 32 per family with lifetime counters; old diagnostics stay outside hot state/prompt.

Ownership is an internal owner UUID, monotonic fence and expiring lease in PostgreSQL, not a client session token or configuration hash. The reference defaults to 30-second leases and 10-second renewal. Native async model/SQL/tool operations allow renewal; genuine expiry/heartbeat loss fences commits. Old owner release/renew/save/result operations cannot invalidate or overwrite a new owner. Startup schema initialization and abandoned recovery share a cross-worker advisory guard spanning DDL commit; failures propagate and are not retried into a false pass.

Validated tool intent/start is durable before invocation. Result, correlated message, evidence and state are atomic. Policy modes are read-only, repeatable, idempotent and reconcile, with bounded attempts and stable idempotency keys. New unknown adapters default to reconciliation. Idempotent mode needs a real adapter; uncertain external outcomes require lookup/reconciliation or block. Explicit transient adapter signals authorize bounded retries; arbitrary model strings do not. Cancellation preserves known outcomes and halts scheduling at a boundary. There is no generic external exactly-once guarantee or rollback of tool effects.

Default retention is keep-all. Explicit telemetry-only pruning requires a current verified consistent export, terminal/resolved session, protected dependency chain and atomic fenced revision checks. Transcript, active context, provenance, runs, executions and archive receipts remain protected. Legacy imports are transactional/repeatable and mark unavailable prior history honestly; they are exercised in disposable schemas. The fresh dedicated database does not require moving another project's data.

## Fixtures, measurements and exclusions

Six shared YAML profiles are used by the current suite. The 1.0.0 freeze preserves its earlier profile and fixture specification/payload hashes; current profiles include the subsequent 1.1.0 recovery completion rule. All four current tool sets query PostgreSQL; archived SQLite/log files are not required. No `.env`, live credentials, mutable session data or model weight files are part of the source freeze.

Historical HTTP reports used real shared fixture tables but did not fully fingerprint their contents. The reference pins the deterministic recipe, canonical rows and expected outputs; its read-only fixture-identity checker is now archived with the historical tooling and needs deliberate restoration/adaptation before reuse. Do not claim that old reports independently attested a full-table hash.

[qualification-workloads.md](archive/agent-python-cleanup-20261016/agent-python/docs/qualification-workloads.md) fixes factors/levels and required measurements: raw outcomes and per-cell client timings, empirical small-sample distributions, model invocation/known usage, repository get/save timing/logical bytes, query-plan observations and paired cancellation/recovery evidence. Logical bytes exclude WAL/wire/physical amplification. Production capacity, population p95/p99, physical WAL/bloat, statement counts, full tool/model critical-path decomposition, auth/multitenancy, multi-host partitions/failover and managed archive restore are excluded claims, not measured zeros.

## Reference freeze and ports

The archived pre-cleanup reference manifest pins runtime, the then-active test/helpers/operations scripts, six YAML profiles, schema DDL, fixture recipe/payloads/oracles, current contract/docs, dependencies/lock and evidence hashes. Historical reports stay immutable and are mapped to requirements with backend/source-match limitations. A reference freeze is not a claim of a clean committed Git tree, a fresh full benchmark run, or current Go/Java/Rust parity.

For each port: implement this contract, use the same frozen profiles/fixtures/model responses and Python HTTP tests, add equivalent transaction/concurrency/crash/deployment evidence, review differences, and only then mark that language conformant. Do not weaken tests, substitute fixtures or special-case assertions by language. Ports remain pending; separate schema/database isolation is needed until storage compatibility is qualified.

## Archive/layout clarification — 2026-10-16

Application cleanup does not change this behavioral contract or certify new results. All benchmark results, regression/qualification scripts and frozen reference material are now under [archive](archive/README.md). The frozen manifest describes its original layout; follow relocation inventories when resolving original paths. The current application contains runtime/dependencies, four operational scripts and operation/implementation docs. Shared curl validation remains at parent test-cases.md. Archived test tooling is preserved for future engineering work but is not supported for execution in its relocated layout without restoration/adaptation.
