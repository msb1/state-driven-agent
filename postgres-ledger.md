# PostgreSQL tables and the durable agent ledger

This guide describes the shared **contract 2.0.0 / storage schema 6** design. The authoritative table definitions, constraints and indexes are [schema/postgres.sql](schema/postgres.sql) and [schema/fixtures.sql](schema/fixtures.sql); [schema/layout-v6.json](schema/layout-v6.json) pins the expected layout. The normative behavior and identity preimages are in [contract.md](contract.md#contract-200--durable-commands-and-action-recovery). The contract is the target for all four runtimes; a shared schema alone does not establish that a runtime is qualified. See the [implementation status](README.md#universal-state-driven-agent).

The ledger is **not a single table**. It is the related set of current state, ordered history, commands, commits, runs, actions, attempts and associated records described below. Some rows represent current lifecycle state and are updated; the transcript, commit receipts and other protected history preserve durable evidence. The model's compacted reflection is a different, unverified working-context ledger, referenced by the session context and backed by transcript provenance. It is not the authoritative record of tool evidence.

## Shared session and recovery tables

| Table | Role and important relationships |
| --- | --- |
| `agent_sessions` | One row per session: current JSON state and resolved configuration, configuration identities, revision, timestamps and schema version. Lease owner, expiry and increasing fence coordinate session ownership. The latest commit ID/hash are a cache; historical receipts are in `agent_commit_receipts`. Primary key: `session_id`. |
| `agent_commands` | One admitted mutating API request: namespace, client scope, caller key, normalized method/route/request and fingerprint, status, optional session, command lease/fence and stored terminal response. `(namespace, client_scope, idempotency_key)` is unique. A command can exist before its session is assigned. |
| `agent_commit_receipts` | Durable receipt for a session save: command, commit UUID, proposal fingerprint, expected revision and committed revision. Primary key `(session_id, commit_id)`; `(session_id, committed_revision)` is unique. A new receipt advances exactly one revision. It identifies an already committed proposal even after later saves. |
| `agent_session_transcript` | Ordered, durable messages and audit/provenance records, including decisions and compaction records. Primary key `(session_id, sequence)` and unique `(session_id, message_id)` preserve order and identity. Records have kind, JSON payload, optional run, decision step and timestamps. |
| `agent_session_context` | Current context projection for a session and revision: references to the goal and active reflection ledger in the transcript, ordered raw message IDs and typed ledger state. This lets compaction replace the active context while retaining source transcript records. |
| `agent_session_events` | Ordered event stream for a session, with a global event ID, JSON event, timestamp and optional revision, command and commit references. A non-null session/commit pair is unique; its receipt foreign key is deferred so the related records can be saved atomically. |
| `agent_runs` | Run lifecycle: session, unique originating command, owner/fence, status, start/end, termination, last decision step, cancellation and metadata. One admitted run command cannot create a second run. A resume creates a new run and command. |
| `agent_tool_executions` | One logical tool action: originating session/run/command, tool and frozen arguments, target, recovery policy, stable action key, payload fingerprint, dispatch-attempt count, status, result and owner/fence. `(namespace, idempotency_key)` and `(session_id, call_id)` are unique. Its origin remains associated with the action when later commands recover it. |
| `agent_action_attempts` | Ordered dispatch or verification attempts for an execution, including the current command, owner/fence, payload hash, timestamps, external reference, evidence and errors. `(execution_id, attempt_number)` is unique. Verification attempts are recorded but do not increment the execution's dispatch-attempt count. |
| `agent_session_diagnostics` | Per-session, per-family ordered JSON diagnostics, including compaction attempts, decisions and events. Primary key `(session_id, family, ordinal)` supports paginated inspection without putting all diagnostics into model context. |
| `agent_session_calls` | Historical mapping of a session's tool-call IDs to transcript message IDs. Its transcript foreign key supports call-identity checks without searching the entire transcript. |
| `agent_archive_receipts` | Archive identity, session, snapshot revision, checksum, manifest, optional dependency receipt and creation time. These records support verified export and guarded retention operations. |

The table definitions specify the actual keys and foreign keys; some relationships described by the contract are also enforced by runtime transaction and integrity checks, not solely by SQL constraints.

## How a task is recorded

1. **Admit a request.** The client sends a mutating request with an `Idempotency-Key`. After validation, the runtime atomically registers or finds `agent_commands` using the namespace, trusted client scope and key. The normalized request fingerprint distinguishes a retry from conflicting key reuse. The command lease protects work even before a session exists.
2. **Create or update state.** Session state and configuration live in `agent_sessions`. A run initiated by a command has its own `agent_runs` row. Current context references transcript records through `agent_session_context`; workflow evidence and other active state are preserved in the state and related durable records.
3. **Commit a transition.** A protected save coordinates the session revision, context, transcript, call mappings, diagnostics, run/execution changes, event and `agent_commit_receipts` in a transaction. A lease/fence and revision comparison prevent stale owners or stale revisions from overwriting newer state. Command admission can be an earlier transaction; a definitive command outcome commits with the corresponding terminal state. Metadata-only cancellation has its own atomic path and need not advance the hot session revision.
4. **Record an action before dispatch.** A tool's execution identity, arguments, target, recovery policy, action key and payload fingerprint are frozen in `agent_tool_executions`; a started dispatch is recorded in `agent_action_attempts` before crossing the external boundary. Result, correlated transcript and state/evidence are recorded under the contract's atomicity rules.
5. **Recover and inspect.** On resume or a new owner, the existing execution and key remain the same. New dispatch or verification attempts identify the current recovery command and fence. Operators can inspect ordered transcript, events, runs, executions and diagnostics through the API; command and receipt history supports replay and uncertain-outcome resolution.

A successful terminal response is not exposed as successful final state before its durable commit. The contract protects original goal/configuration and ordered historical identities. Retention defaults to keep-all; explicit telemetry pruning requires a verified export and protects the records needed for history and recovery. Consult the [retention contract](contract.md#streaming-cancellation-and-retained-evidence--idem-05) and deployment tooling before pruning.

## Three identities for idempotency

Contract 2.0 uses separate identities for API requests, database saves and external actions. They solve different retry problems and should not be substituted for one another.

### Request key and command

The caller generates a **new** `Idempotency-Key` for each new intended mutation and retains the **same** key for a retry whose outcome is unknown. Keys are case-sensitive, 1–255 printable ASCII characters without spaces. Their scope is `(AGENT_DEPLOYMENT_NAMESPACE, client_scope, idempotency_key)` in `agent_commands`. The deployment namespace is stable across workers and languages sharing the ledger/recipient; the current trusted client scope is `local`, not an authentication mechanism.

The stored request fingerprint is the lowercase SHA-256 of RFC 8785 canonical JSON bytes for:

```text
["agent-command-v1", "POST", normalized_route, normalized_request]
```

An identical request/key finds the existing command: in-progress requests return its handle, and terminal JSON outcomes replay the stored status/body. The same key with a different normalized request, route or target is rejected with a conflict. Validation failures before admission do not consume the key. A key is not authorization. A stream retry observes the existing command rather than creating another run or replaying transient stream bytes; durable events support read-only reconnection.

### Commit UUID and receipt

Each proposed session save receives a stable `commit_id`. `agent_commit_receipts` stores that ID, command, fingerprint and revisions atomically with the save. On an uncertain database acknowledgement, the runtime looks up the receipt: a matching receipt acknowledges the original commit; a changed proposal under the same ID is rejected. A confirmed rollback can be retried with the identical proposal under normal fencing and revision checks. An unresolved outcome must remain uncertain. Replaying an old receipt never restores an old snapshot over newer work. The [contract's commit section](contract.md#repository-commit-receipts--idem-03) defines the full fingerprint preimage and exceptions.

### Execution UUID and action key

A logical action has an `execution_id` and a deterministic key, stored before dispatch. Its key and separate payload fingerprint are lowercase SHA-256 hashes over RFC 8785 canonical JSON bytes:

```text
["agent-action-v1", namespace, session_id, execution_id]
["agent-payload-v1", action_type, target, frozen_arguments]
```

The stable key identifies the action across attempts, resumes, workers and language implementations; a deliberately repeated action gets a new execution UUID. The separate fingerprint detects changed action content. Fixed cross-language examples are in [schema/idempotency-vectors.json](schema/idempotency-vectors.json).

The database key alone does not make an external effect exactly once. An idempotent adapter must send the retained key to a recipient that atomically deduplicates requests and rejects changed payloads within a known guarantee. Otherwise, recovery needs authoritative reconciliation; if safe resolution is unavailable, the action remains uncertain or blocked. A local fence does not fence an external recipient. See [external actions and recovery](contract.md#external-actions-and-recovery--idem-04).

### How the implementation decides whether to act again

There are two separate duplicate decisions. The **client chooses the request key**; the agent does not derive it from the prompt or guess whether two requests mean the same thing. The **agent derives the action key** from a newly allocated execution UUID, then saves that identity for every attempt to carry out that same logical action. Consequently, two intentionally separate requests or two intentionally separate executions can have identical content and still run twice. To prevent a repeated business operation submitted as a new intent, the caller or recipient must retain and enforce an appropriate business identity as well.

For example, consider a client requesting a session run that invokes the controlled `fixture_idempotent_write` tool:

1. **Admit the request once.** The client stores a key such as `run-request-42` before sending it. Python's [`CommandStore.register`](agent-python/src/agent_python/commands.py) attempts an `INSERT ... ON CONFLICT (namespace, client_scope, idempotency_key) DO NOTHING`, then reads the command and compares its stored request fingerprint. The unique constraint resolves concurrent admission; the fingerprint detects a changed route or body. The same request/key refers to the existing command, including its stored terminal response when complete. A different key is a new command, even if its body is identical.
2. **Allocate one action identity.** Before dispatch, [`ExecutionCoordinator.invoke`](agent-python/src/agent_python/execution.py) looks up an existing execution for the session and tool-call ID. For a new call, it allocates an `execution_id`, calculates `action_key(namespace, session_id, execution_id)` and a separate `payload_fingerprint(tool, target, arguments)`, freezes the arguments and policy, and persists the pending intent. These identity functions use canonical JSON followed by SHA-256 in [`identity.py`](agent-python/src/agent_python/identity.py). PostgreSQL's unique `(namespace, idempotency_key)` constraint protects the stored action identity; [`execution_store.py`](agent-python/src/agent_python/execution_store.py) also checks the derived key and payload fingerprint when saving.
3. **Reuse a known outcome.** If the execution already has a definitive `succeeded` or `failed` result, the coordinator returns that recorded result instead of invoking the tool again. A changed tool or arguments for the frozen execution are blocked. A started or uncertain execution retains the same UUID, key and payload while the coordinator checks its recorded policy and, where available, invokes a verifier.
4. **Record and perform an attempt.** Before crossing the recipient boundary, the coordinator persists a `dispatch` row in `agent_action_attempts`. For an idempotent adapter, it sends the **persisted action key**, not a newly generated retry key, with the frozen arguments. A recovery attempt has a new attempt record but uses the original execution and action key. The command, owner and fence on the attempt identify who performed that attempt.
5. **Enforce the key at the recipient.** The controlled fixture's [`fixture_idempotent_write`](agent-python/src/agent_python/tools.py) inserts `(namespace, action_key, payload_sha256, request, result)` into `agent_fixture_action_receipts` using `ON CONFLICT DO NOTHING`, then reads the receipt. The primary key allows one receipt for that namespace/key. If the fingerprint matches, it returns the stored result; if it differs, it raises a recipient conflict. In this fixture, the receipt row is the effect and result, so creating the effect and deduplication record is atomic. A separate real recipient must implement equivalent atomic effect-and-receipt behavior for its own side effect.
6. **Resolve a lost response safely.** If the recipient applied the fixture effect but the response was lost, [`fixture_verify`](agent-python/src/agent_python/tools.py) looks up the same receipt and fingerprint. A matching receipt supplies the original result, which the coordinator records; a missing or mismatched receipt is reported as unknown by this verifier, not proof that the effect never happened. An allowed retry through a genuinely keyed adapter uses the same key and is bounded by the recorded policy and recipient's deduplication window. If neither safe retry nor authoritative reconciliation can establish the result, recovery blocks rather than inventing success or issuing an unsafe new action.

As a concrete retry, if execution `E` in session `S` produced key `K` and the fixture stored a receipt for `K`, repeating the dispatch for **that same execution** sends `K` again and reads the existing result. A new execution `E2` produces a different key even for the same arguments: the system treats it as a new action. Similarly, retrying the original HTTP request with `run-request-42` finds the original command; sending an identical request under `run-request-43` does not deduplicate it against the first request. These distinctions are intentional and are essential when deciding where the application must supply a durable business-operation identifier.

The Python paths above illustrate the reference implementation. Go's corresponding identity and fixture-recipient code is in [`identity_v2.go`](agent-go/internal/agent/identity_v2.go) and [`fixture_recipient_v2.go`](agent-go/internal/agent/fixture_recipient_v2.go); Rust's is in [`identity.rs`](agent-rust/src/identity.rs) and [`actions.rs`](agent-rust/src/actions.rs). Consult the current [implementation status](README.md#universal-state-driven-agent): Java implements contract 2.0.0 and has passed the shared shell suites; see [digest-java.md](digest-java.md) for evidence.

**RFC 8785 canonicalization by language:** Inspection of the runtime code shows that only Python currently delegates canonicalization to a dedicated RFC 8785 library. Go and Rust implement canonicalization directly, using general-purpose JSON libraries for parts of the work. The fixed vectors and boundary tests provide evidence for the cases they cover; they do not by themselves prove complete RFC 8785 conformance.

| Language | Canonicalization implementation used for contract 2.0 identities |
| --- | --- |
| Python | The [`rfc8785`](agent-python/pyproject.toml) package: [`canonical()`](agent-python/src/agent_python/identity.py) calls `rfc8785.dumps`. |
| Go | Custom [`CanonicalV2`](agent-go/internal/agent/identity_v2.go), using the standard `encoding/json`, `strconv`, and UTF-16 utilities. It is not delegated to a dedicated JCS library. |
| Rust | Custom [`canonical`](agent-rust/src/identity.rs), using `serde_json` for JSON values and some serialization. It is not delegated to a dedicated JCS library. |
| Java | Implemented using RFC 8785 canonicalization; see [JsonSupport.java](agent-java/src/main/java/com/statedriven/agent/JsonSupport.java) and [digest-java.md](digest-java.md). |

## Fixture tables

These six tables support demonstrations and qualification; they are separate from the twelve shared session/recovery tables and are not general-purpose business integrations.

| Table | Purpose |
| --- | --- |
| `agent_fixture_access_logs` | Numbered log lines used by the log-analysis and recovery tools. |
| `agent_fixture_customers` | Customer IDs, names and cities for the revenue fixture. |
| `agent_fixture_orders` | Order customer keys, amounts and dates for revenue calculations. |
| `agent_fixture_employees` | Employee records with text age/salary fields and city values for the audit and cleaning scenarios. |
| `agent_fixture_idempotency_vectors` | Fixed namespace, action, canonical preimage and expected hash/key vectors for identity qualification. |
| `agent_fixture_action_receipts` | Controlled recipient fixture keyed by `(namespace, idempotency_key)`, storing the request fingerprint and result; its one-effect constraint models a keyed receipt/effect. It does not guarantee idempotency for other recipients. |

For setup and the explicit reset's effects, see [schema/README.md](schema/README.md). For the common API and PostgreSQL qualification scenarios, see [test-cases.md](test-cases.md#19-shared-postgresql-persistence-qualification).
