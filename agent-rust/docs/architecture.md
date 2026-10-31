# Rust runtime — contract 2.0.0 / schema 6

The Rust runtime targets current `agent-python`, `contract.md`, and `design_update.md`. Contract 2.0.0 overrides conflicting 1.1.0 behavior; unchanged configuration, workflow, completion, context, and fixture requirements still apply. This is an integrated implementation. User-run shell qualification remains separate from regression results.

## Runtime boundaries

- `main.rs` provides HTTP admission handlers, session ownership, transactional persistence, bounded history APIs, and explicit administration entrypoints. Startup validates the existing schema in a read-only transaction and performs no schema migration.
- `commands.rs` validates keyed requests before admission, freezes creation configuration, coordinates command leases/fences, replays stored JSON outcomes, and exposes authoritative command status. A repeated streaming POST returns its command rather than starting another run.
- `identity.rs` implements JCS request/action/commit identities and keeps the older Python configuration/snapshot/archive encoding separate. Fixed contract vectors cover both request and action hashes.
- `engine.rs` controls workflow evidence, completion, pending tool batches, bounded decisions, and progress. Native model requests use automatic tool choice and the complete public tool list. Only explicitly configured single-call mode adds its own wire constraints. Rejected assistant drafts remain paired with their corrective feedback.
- `context.rs` groups complete call/result interactions and measures full decision and summary requests. Compaction normalizes complete fenced XML, reordered sections, and nested lesson markup into canonical text. Code restores the immutable goal. Missing/duplicate sections, attributes, malformed XML, empty reflection, placeholders, unsupported numbers, excessive size, and nonreducing replacements remain invalid. Provider truncation never installs a summary.
- `actions.rs` commits frozen action intent and started attempts before dispatch. Logical identities survive resume and owner changes. The compiled fixture recipient atomically deduplicates the persisted key and validates the separate payload fingerprint. Trusted recipient metadata declares deployment scope, indefinite retention, and receipt verification. Unresolved actions use their saved policy, preserve evidence, and block when safe recovery cannot be established.
- `diagnostics.rs` maintains stable append-only ordinals and bounded recent caches. `integrity.rs` seals hot snapshots and active projections with Python-compatible version-1 integrity anchors. Active messages and ledgers are loaded from transcript/context references; archived raw history is not duplicated into the hot snapshot.
- `events.rs` reconnects readers to retained run events without creating a run or invoking models/tools. `sse.rs` separates producer completion from response-drop notification so a completed stream reaches EOF and a disconnected invocation can settle cooperatively.
- `retention.rs` implements `sda-archive-2` export/verification and explicit terminal-session telemetry pruning. Commands, commit receipts, actions, attempts, runs, transcript, context, and archive receipts remain protected. Archive verification accepts the common format without a language-specific table ordering restriction.
- `reconcile.rs` implements explicit abandoned-run reconciliation without model/tool invocation. `provider_log.rs` records bounded redacted provider fields only in the operator log.

## Transaction authority

Command authority precedes session writes. Each repository proposal retains a commit UUID, fingerprint, and stable outboxes. A matching durable receipt is consulted before CAS rejection; a receipt for an older revision requires reload. New writes advance exactly one revision. Session/context, transcript, call map, diagnostics, run/action/attempt records, events, commit receipt, and a definitive command outcome commit together. Cancellation records its explicit target run and command outcome without advancing the hot revision.

Checkpoint acknowledgements return committed sequence metadata to the engine. Failed writes preserve the proposed outboxes; unresolved commit outcomes stop advancement. An expired or superseded owner cannot acknowledge another worker's work.

See [implementation.md](implementation.md) for verification scope and [README.md](../README.md) for operator commands. Historical progress and earlier failures are retained in [digest-rust.md](../../digest-rust.md).
