# Universal state-driven agent observable contract

**Active target: contract 2.0.0, storage schema 6.** The final section, **Contract 2.0.0 — durable commands and action recovery**, overrides conflicting statements in the retained 1.1.0 baseline below. There is no schema-5 compatibility mode. Existing runtime qualification does not certify 2.0.0. Python now has the runtime implementation and focused regressions described in the final implementation-status section; its user-run CLI qualification and the other language runtime ports remain pending.

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

The shared schema contains nine session/storage tables and four fixture tables, defined in [schema/postgres.sql](schema/postgres.sql) and [schema/fixtures.sql](schema/fixtures.sql). Only the explicit `agent-python/scripts/initialize_database.py` setup command may create them. After initialization, every agent must use the existing shared schema without creating, altering, dropping, or migrating tables, columns, indexes, or other schema objects. Language-specific tables and schema-migration tables are prohibited. Servers validate [schema/validate.sql](schema/validate.sql) in a read-only transaction and fail startup with setup guidance if the schema is incompatible. Ports may use idiomatic internal models; shared-table interoperability requires the same schema, canonical hashing, identifiers and transaction guards. Do not run incompatible legacy writers on upgraded sessions.

Messages have stable IDs, per-session ordered sequences and separate wire payloads. Append-only transcript includes decisions, run lifecycle, intent/attempt/outcome, rejected finals/summaries and provenance. Canonical resolved configuration/goal and existing transcript identities are protected; inconsistent duplicates/gaps/sources fail. Identical duplicate commits have explicit idempotent handling. Snapshot/context/transcript/event/diagnostic/call/run/execution updates commit together or roll back together. Revision CAS rejects stale writers. Active hydration is one consistent MVCC statement; explicit history retrieval is separate. Recent diagnostics are 32 per family with lifetime counters; old diagnostics stay outside hot state/prompt.

Ownership is an internal owner UUID, monotonic fence and expiring lease in PostgreSQL, not a client session token or configuration hash. The reference defaults to 30-second leases and 10-second renewal. Native async model/SQL/tool operations allow renewal; genuine expiry/heartbeat loss fences commits. Old owner release/renew/save/result operations cannot invalidate or overwrite a new owner. Startup schema validation is read-only. Abandoned-run recovery updates normal lifecycle rows under ownership/fencing rules; failures propagate and are not retried into a false pass.

Validated tool intent/start is durable before invocation. Result, correlated message, evidence and state are atomic. Policy modes are read-only, repeatable, idempotent and reconcile, with bounded attempts and stable idempotency keys. New unknown adapters default to reconciliation. Idempotent mode needs a real adapter; uncertain external outcomes require lookup/reconciliation or block. Explicit transient adapter signals authorize bounded retries; arbitrary model strings do not. Cancellation preserves known outcomes and halts scheduling at a boundary. There is no generic external exactly-once guarantee or rollback of tool effects.

Default retention is keep-all. Explicit telemetry-only pruning requires a current verified consistent export, terminal/resolved session, protected dependency chain and atomic fenced revision checks. Transcript, active context, provenance, runs, executions and archive receipts remain protected. Automatic legacy-session imports, schema migrations, and integrity-anchor backfills are disabled. The explicit initializer validates an existing schema without modifying it and refuses incompatible tables.

## Fixtures, measurements and exclusions

Six shared YAML profiles are used by the current suite. The 1.0.0 freeze preserves its earlier profile and fixture specification/payload hashes; current profiles include the subsequent 1.1.0 recovery completion rule. All four current tool sets query PostgreSQL; archived SQLite/log files are not required. No `.env`, live credentials, mutable session data or model weight files are part of the source freeze.

Historical HTTP reports used real shared fixture tables but did not fully fingerprint their contents. The reference pins the deterministic recipe, canonical rows and expected outputs; its read-only fixture-identity checker is now archived with the historical tooling and needs deliberate restoration/adaptation before reuse. Do not claim that old reports independently attested a full-table hash.

[qualification-workloads.md](archive/agent-python-cleanup-20261016/agent-python/docs/qualification-workloads.md) fixes factors/levels and required measurements: raw outcomes and per-cell client timings, empirical small-sample distributions, model invocation/known usage, repository get/save timing/logical bytes, query-plan observations and paired cancellation/recovery evidence. Logical bytes exclude WAL/wire/physical amplification. Production capacity, population p95/p99, physical WAL/bloat, statement counts, full tool/model critical-path decomposition, auth/multitenancy, multi-host partitions/failover and managed archive restore are excluded claims, not measured zeros.

## Reference freeze and ports

The archived pre-cleanup reference manifest pins runtime, the then-active test/helpers/operations scripts, six YAML profiles, schema DDL, fixture recipe/payloads/oracles, current contract/docs, dependencies/lock and evidence hashes. Historical reports stay immutable and are mapped to requirements with backend/source-match limitations. A reference freeze is not a claim of a clean committed Git tree, a fresh full benchmark run, or current Go/Java/Rust parity.

For each port: implement this contract, use the same frozen profiles/fixtures/model responses and Python HTTP tests, add equivalent transaction/concurrency/crash/deployment evidence, review differences, and only then mark that language conformant. Do not weaken tests, substitute fixtures or special-case assertions by language. Ports remain pending; separate schema/database isolation is needed until storage compatibility is qualified.

## Archive/layout clarification — 2026-10-16

Application cleanup does not change this behavioral contract or certify new results. All benchmark results, regression/qualification scripts and frozen reference material are now under [archive](archive/README.md). The frozen manifest describes its original layout; follow relocation inventories when resolving original paths. The current application contains runtime/dependencies, four operational scripts and operation/implementation docs. Shared curl validation remains at parent test-cases.md. Archived test tooling is preserved for future engineering work but is not supported for execution in its relocated layout without restoration/adaptation.

## Contract 2.0.0 — durable commands and action recovery

### Authority, scope and implementation status

This revision supersedes the 1.1.0 baseline for request replay, command identity, storage, commit replay, action recovery, streaming reconnection, retention and qualification. Unchanged configuration, workflow, completion, compaction and fixture-output requirements remain binding. The development database uses **schema 6 only**; no runtime compatibility or automatic migration from schema 5 is required. Historical freezes and reports remain historical evidence. Python, Go, Rust and later Java must implement the same requirements; none is certified as 2.0.0 by these document/setup changes.

The executable setup definitions are `schema/postgres.sql` and `schema/fixtures.sql`. The explicit initializer creates or validates them; `--reset` rebuilds only the known agent-owned development tables. Servers never create, alter, migrate or drop schema objects. Stop all servers before rebuilding. Unrelated database objects are preserved; an external dependency prevents reset rather than being removed with CASCADE. When session and fixture databases differ, each database is transactional separately; setup is not a distributed transaction.

### Identity and canonicalization — IDEM-01

Three identities have distinct responsibilities:

| Identity | Source and lifetime | Responsibility |
| --- | --- | --- |
| Request key / command ID | Caller retains its request key before sending; server persists one command UUID on first admission | Deduplicate a logical API operation across retries, processes and restarts |
| Commit UUID | Allocate once per proposed repository transaction and retain until its outcome is resolved | Acknowledge the same committed transaction without duplicating records or advancing revision |
| Execution UUID / action key | Allocate the execution UUID and freeze the action in the ledger before dispatch | Retry or reconcile one logical external action across attempts, resumes and owner changes |

`AGENT_DEPLOYMENT_NAMESPACE` is a required stable administrator-controlled namespace, shared by every language accessing the same ledger/recipient. It is not a worker ID, run ID, language name, model-generated value or transient hostname. Request keys are scoped by `(namespace, client_scope, idempotency_key)`. Until authentication exists, the trusted client scope is `local`; future identity integration must derive scope server-side, never trust an arbitrary tenant header. A key grants no authorization.

Canonical encoding is UTF-8 JSON Canonicalization Scheme, **RFC 8785**, with no trailing newline. Reject duplicate object keys, nonfinite numbers, invalid Unicode and values outside the supported JCS/I-JSON number domain before admission. Preserve string content without Unicode normalization, JSON types and array order; object property order does not affect identity. Represent exact monetary amounts, arbitrary-precision integers and decimals as strings. UUID strings used in identity preimages are canonical lowercase hyphenated UUIDs. Existing configuration/source hashing is unchanged; the following versioned request, action, payload and commit fingerprints use JCS.

```text
request_sha256 = lowercase_hex(SHA256(JCS([
  "agent-command-v1", "POST", normalized_route, normalized_request
])))

action_key = lowercase_hex(SHA256(JCS([
  "agent-action-v1", namespace, session_id, execution_id
])))

payload_sha256 = lowercase_hex(SHA256(JCS([
  "agent-payload-v1", action_type, target, frozen_arguments
])))
```

The session UUID is the workflow-instance identity even when workflow configuration is absent. The execution UUID identifies the logical action; it remains fixed when run, worker, implementation, owner or fence changes. An intentionally repeated identical action receives a new execution UUID. A model step number, provider call ID or payload hash alone is not a logical action identity. The payload fingerprint is separate from the key so that changed content under an existing key is rejected rather than becoming a second effect.

`schema/idempotency-vectors.json` pins canonical preimages and expected hashes for all languages. Tooling must compare against those fixed values, not regenerate expected answers at qualification time. `scripts/idempotency_contract.py` is the setup/qualification reference implementation, not an implicit upgrade to agent runtime code.

### Request admission and replay — IDEM-02

All mutating routes in the baseline require `Idempotency-Key`: a case-sensitive opaque value of 1..255 printable ASCII characters excluding spaces. Missing or invalid keys return HTTP 422 with `code=idempotency_key_required` or `idempotency_key_invalid`, before any mutation or model/tool invocation. Generate a new key for a new intent; retain the same key when retrying an uncertain request. Do not hash the body alone to deduplicate distinct intentional requests.

Normalize the route by parsing UUID path parameters into their canonical form. The method, route (including the target session), and admitted request fields participate in the fingerprint. Reject unknown fields and duplicate JSON properties. Normalize static defaults: creation includes the default `config_ref`; run/resume includes `max_steps:null` when absent; resume includes `content:null` when absent. The cancel request names an explicit `run_id` so a delayed retry cannot cancel a successor run. Dynamic model/configuration decisions are frozen at first admission and are not recalculated to decide whether an old request matches. Retrying session creation after a YAML/environment edit returns its original session/configuration, not a fresh resolution.

After request validation, atomically register or find one command under its unique key. Registration, payload equality, command ownership and any session mutation must be coordinated by PostgreSQL constraints and transactions; a check followed by an unguarded insert is insufficient. A command lease (30-second default, 10-second renewal, increasing fence) protects work before a session exists. Acquire command authority before session authority, then reload; duplicate requests do not steal an unexpired claim. All finalization is fenced. Session ownership/fencing remains required.

| Existing command | Response / effect |
| --- | --- |
| No match | Admit one command and perform its specified operation |
| Same key, different fingerprint, method, target or route | HTTP 409, `code=idempotency_key_reused`; no new work |
| Same key/content, pending or running | HTTP 202 with existing `command_id`, `session_id` when assigned, `run_id` when assigned, status URI and `Retry-After`; no second run or invocation |
| Same key/content, definitive terminal JSON outcome | Return stored status/body and `Idempotency-Replayed:true`; do not return a newer mutable session snapshot instead |
| Same key/content, uncertain or blocked | Return the existing operation and explicit uncertainty/recovery status; apply only its recorded recovery policy |

`GET /commands/{command_id}` returns authoritative command status, identifiers, outcome or recovery reason, using the same caller scope. Malformed IDs are 422 and absent/inaccessible IDs are 404. `/health` advertises `contract_version:"2.0.0"` and `storage_schema:6` only after that runtime implements this contract. No implementation may advertise these merely because the tables exist.

Validation failures before admission do not consume a key. Definitive failures after admission are recorded and replayed. Transport errors and unconfirmed external outcomes are not recorded as definitive failures. Changing the key to bypass an uncertain operation is not an authorized recovery strategy.

### Repository commit receipts — IDEM-03

Persist the complete command input and every frozen action needed for recovery before dispatch. Database-only mutations, their command outcome, session snapshot/context/revision, transcript, call map, diagnostics, runs/executions, event and commit receipt commit together. A long-running command may be admitted pending in an earlier transaction, but its definitive outcome must commit with the corresponding terminal state. Allocate stable record IDs before submission; retain the commit ID, exact proposed payload and outboxes across an uncertain acknowledgement.

The commit fingerprint is SHA-256 over JCS of this ordered preimage:

```text
["agent-commit-v2", session_id, command_id, expected_revision,
 hot_snapshot_without_pending_outboxes_or_commit_id,
 active_full_messages, canonical_ledger, typed_ledger,
 transcript_outbox, execution_outbox, diagnostics_outbox,
 retention_outbox, resolved_config_hash, event]
```

Here `execution_outbox` is the object `{executions: [...], attempts: [...]}` so attempt changes are part of the exact commit identity. Command `request` stores `{input: normalized_request, admission: frozen_creation_state_and_config}`; only `input` enters the request fingerprint. Admission data is immutable. Metadata-only cancellation commits its explicit run flag and command outcome atomically without changing the hot session revision.

`agent_commit_receipts` is durable history keyed by `(session_id,commit_id)` with the fingerprint, command ID, expected and committed revisions. `last_commit_id/sha256` are a latest-commit cache, not the only deduplication evidence. Read a matching receipt before rejecting a stale expected revision: identical content acknowledges the original committed revision; changed content under that commit UUID is rejected. Each new commit advances exactly one revision through CAS. A receipt for an old revision does not authorize writing old state over newer committed data. Return its original receipt and require a reload before further work.

On an uncertain database commit, reconnect and query the receipt. If present and matching, acknowledge it. If the database establishes rollback/no receipt after resolving the original transaction, retry the exact proposal under normal CAS/fencing. While the outcome remains unknown, retain uncertainty and stop advancing the plan. Never change the proposal under a retained commit UUID or fabricate success.

### External actions and recovery — IDEM-04

Before dispatch, commit `execution_id`, origin command/run, namespace, target, tool/action type, frozen arguments, payload fingerprint, derived key and immutable recovery policy. Persist a started attempt before crossing the external boundary. Model regeneration cannot replace a pending action. A successor uses the existing execution identity and key; its attempt records identify the recovery command and current owner/fence while preserving the action's original command/run association.

| Execution state | Meaning | Allowed recovery |
| --- | --- | --- |
| `pending` | Frozen durable intent; no started attempt | Dispatch after obtaining authority |
| `started` | Dispatch may have reached recipient | Resolve through supported idempotency or authoritative reconciliation |
| `uncertain` | Result cannot be established | Keep identity/payload; retry only under a proven safe policy or verify |
| `succeeded` / `failed` | Definitive outcome recorded | Reuse outcome; do not invoke again |
| `blocked` | Safe automatic resolution unavailable | Report `recovery_required`; await explicit resolution |

`agent_action_attempts` records each dispatch/verification with an ordered attempt number, current command/owner/fence, payload fingerprint, timestamps, external reference and evidence/error. The execution's `attempts` counter counts dispatch attempts; verification attempts do not silently consume or reset the dispatch bound. Record verification as a `kind=verify` attempt and immutable audit events; it is not necessary to replace the execution status with VERIFYING. Uncommitted planning may be regenerated; a durable pending or started action may not be discarded as mere thinking.

An idempotent adapter must actually send the persisted key to a recipient that atomically deduplicates concurrent requests and rejects changed payloads. Record its key scope, retention guarantee and outcome-query capability in trusted adapter metadata. Never automatically replay outside a recipient's guaranteed deduplication window; reconcile or block. Bounded retry/backoff follows recorded policy and deadline; ordinary model/error strings do not authorize retries. Lost ACK does not imply failure, and `pending` alone does not universally authorize blind replay.

Without recipient idempotency, verification must use a unique operation/business reference and authoritative evidence. Title matching, time-window searches and a single absent eventually-consistent read do not prove nonexecution. Repeat only after definitive evidence that the original cannot still complete and policy permits it; otherwise block. Persist the evidence and decision. Known terminal outcomes are reused across restart/resume; stale owners cannot commit over their successors. Local fencing alone does not fence an external service.

For effects inside the same PostgreSQL transaction, write effect and receipt atomically. For independently committed services, use an idempotent recipient/inbox and durable outbox/receipt protocol; polling and notifications retrieve committed truth, and notifications are hints. Logging intent, performing an external effect, then logging completion is not true two-phase commit and leaves an uncertainty window. This contract does not require PostgreSQL prepared transactions or promise global exactly-once effects.

### Streaming, cancellation and retained evidence — IDEM-05

The first admitted stream may serve the existing SSE protocol, with `command_id`, `run_id` and durable event IDs in its events. A repeated streaming POST returns the existing command envelope (202 while nonterminal; stored JSON outcome when terminal), not a new run or a fabricated replay of transient bytes. Add `GET /sessions/{id}/events/stream?run_id=...&after_event_id=...` for read-only reconnection to durable events. It must not invoke a model/tool, change run identity or advance revision. A stream reader disconnect retains the baseline cooperative cancellation rule; reattachment observes the existing run and does not silently resume it. An intentional resume uses a new request key and run UUID, while unresolved logical tool actions retain their execution IDs and keys.

Cancellation captures its explicit target run on admission. Replaying cancellation never retargets a newer run. Command receipts, commit receipts, action identities, attempts and unresolved evidence are protected from telemetry pruning. Development retention is indefinite until an explicit administrative reset; there is no automatic expiration that turns an old key into a new action. Future bounded retention needs its own contract revision and tombstone/rejection behavior.

### Provider error visibility — OBS-01

Provider failures have separate public and operator visibility. Public session state, command response/replay bodies, transcript/events, diagnostics and SSE may expose only bounded stable error categories/codes, HTTP status, safe request metadata and generic messages. They must not expose raw provider response bodies, request payloads, authorization headers, credentials, or unredacted provider messages. This boundary applies equally to errors returned during decisions and compaction.

For diagnosis, the server may write one structured provider-failure record to an operator-only log. It may contain HTTP status, allow-listed provider error `type`, `code`, and `param`, a bounded redacted `message`, and a validated provider request ID. Strip credentials, bearer/API tokens, URLs, quoted/request-echoed content and control characters; cap all retained strings. Never log the raw response/request body or headers. Non-JSON/unrecognized errors record status and safe metadata only. Operator logs must have access control and retention appropriate for potentially sensitive diagnostic text, and must not be retrievable through agent APIs or SSE. This is an operator observability channel, not part of the public PostgreSQL session ledger.

### Schema, fixtures and shared qualification — IDEM-06

Schema 6 contains **12 shared tables**: the existing nine plus `agent_commands`, `agent_commit_receipts` and `agent_action_attempts`. `agent_runs.command_id` links one run to its origin command. Executions add origin command/namespace, target, versioned key algorithm, 64-character text key and payload fingerprint. The storage version is 6; UUID tool keys and schema-5 writers are unsupported. Foreign keys, unique identities and outcome/lease checks are specified by `schema/postgres.sql`; `schema/layout-v6.json` pins columns/types/defaults/constraints/indexes for read-only verification.

The six fixture tables retain the four original business datasets and add fixed `agent_fixture_idempotency_vectors` and controlled-recipient `agent_fixture_action_receipts`. The latter's receipt row is the fixture effect: insert result and receipt atomically, return an existing matching receipt, reject a mismatching fingerprint. The existing built-in fixture adapter uses this controlled recipient for runtime qualification; its behavior is not evidence about unrelated external services. Fixture generation preserves existing recipient receipts; only an explicit database reset clears them. The business workload answers remain unchanged.

`persist.sh` is the common qualification entrypoint. Setup-only success must be labeled **setup only**, not agent/DU-06 conformance. Full qualification requires actual 2.0.0 runtime evidence; old servers must fail clearly. Tests and assertions are identical across implementations. Qualification does not create/drop/alter database objects or write/delete rows through direct SQL. It may create evidence through normal agent API operations; protected command/session/history rows are retained for inspection. Database-side qualification reads use `SELECT` only. PostgreSQL ledger inspection verifies command/action/attempt/commit relationships, original outcomes and effect counts. Required cases include:

- Fixed canonical/hash vectors, reordered JSON equivalence, numeric/Unicode/type boundaries and rejected duplicate keys.
- Same request/key repeated, replay after later operations, changed payload/route rejection, missing-key rejection and concurrent identical requests.
- Atomic database-only effects and receipts, rollback, stale revisions and immutable identity checks.
- Lost response after a confirmed commit followed by replay of the same command, without duplicated session/message/run/effect; separately exercise uncertain repository commits without replacing their identity.
- Worker restart/takeover, safe concurrent startup, uncertain keyed action recovery with one recipient effect, authoritative verification and blocked ambiguous recovery.
- Frozen tool arguments/key across attempts and resume, recorded attempt bounds and recipient-window expiry behavior.
- Streaming reconnection without another run/invocation, cancellation retry without retargeting, and protected receipt history.

DU-06 testing for contract 2.0.0 uses the current [DU-06 checks and measurements](design_update.md#du-06-contract-200-checks-and-measurements--current), IDs `DU-06-V2-C01` through `DU-06-V2-C12`. The contract-1.1.0 DU-06 C01–C06 testing matrix and its benchmark/done criteria are deprecated for current qualification. This testing revision preserves the atomicity, identity and recovery guarantees above; it changes how shared acceptance evidence establishes them.

Every result names its scope, target contract/runtime identity, source inputs and evidence paths. Untested/unimplemented cases remain pending and cannot be converted into a whole-contract pass by schema-only or SQL-fixture tests. Runtime work is specified in section 19 of `design_update.md`.

### Python implementation status — 2026-10-27

The Python runtime now implements the schema-6 command, commit-receipt, action/attempt, cancellation, event-reconnection and protected-retention paths described above. This supersedes the earlier setup-only implementation status for Python. Focused isolated regressions are engineering evidence; final user-run CLI qualification is pending. Go, Rust and Java still require the 2.0.0 runtime port. Shared persistence qualification uses the same API operations and PostgreSQL assertions for every implementation. Repository-specific diagnostics remain separate and cannot qualify another implementation.
