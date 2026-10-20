# Compaction, budgeting and provenance

Updated implementation/porting manual, 2026-10-16. The full algorithm, payload order and acceptance mapping below are intended for the Go/Java/Rust ports.

This document combines the current implementation and former compaction-efficiency notes. Historical descriptions of unvalidated summaries, destructive fallback, simple message slicing and step-limit compaction are superseded. Reference: [contract 1.1.0](../../contract.md) and the current [design upgrade specification](../../design_update.md).

## Context and authority

The prompt contains immutable instructions/goal, optional durable workflow/evidence and result-binding guidance, a structured compacted ledger, complete recent raw interactions, and final/current-missing-requirement guidance. Ledger claims are explicitly `unverified_model_summary`. Structural/numeric validation does not prove semantic truth or freshness. Workflow evidence and required artifacts come from actual executed results outside the summary; repeated or stale ledger prose cannot satisfy them.

`raw_turns_to_keep` is a minimum number of messages, expanded backward to retain complete assistant-call/result groups. User message order and exact raw text are preserved. The goal stays first and is never summarized away. Incomplete/orphaned groups are integrity errors; they are not truncated to make a request fit. Diagnostic attempt/event arrays do not enter model context.

## Complete-request budgeting

`context.estimate_tokens` measures compact UTF-8 request JSON bytes plus 32 per message. Despite field names, it is a conservative application estimate, not a provider tokenizer. A request fits only if estimated input + max(configured output reserve, payload output limit) + safety margin is within the context window. Both decision and summarizer payloads are assessed, including protected instructions/tools/ledger/guidance. Provider-reported usage remains separate; unknown usage/cost is never invented.

If history cannot be safely reduced enough, hard overflow stops before an unaffordable decision call with `context_budget_exceeded`. Hard protection is immediate even when optional spacing/pressure/reduction policies would skip compaction. Run allowance (`max_steps` / `run_max_steps`) is separate from lifetime steps and optional compaction cadence.

## Triggers and useful reduction

A fitting request may compact when soft estimated input exceeds `max_tokens` with required growth, or independent `compaction_interval_steps` is due. Optional policy uses the protected-context floor, reclaimable historical input, pressure ratio, minimum interval, absolute minimum reduction and reduction ratio. Protected system/tool/current-task overhead is not treated as reclaimable history. Completed workflows, low pressure, insufficient reclaimable context, minimum spacing and unchanged failed source can skip optional calls with bounded diagnostics.

The summarizer receives the largest affordable prefix of complete historical groups; unaffordable groups stay raw. Replacement must reduce the complete decision request by at least one estimated token, and optional replacements must meet configured absolute/fractional minima. Hard protection may make bounded additional passes when a useful prefix reduction is insufficient to fit. Attempt/pass counts are bounded by configuration. Source changes allow new attempts; an unchanged failed source is not retried indefinitely.

| Memory setting | Default | Meaning |
| --- | ---: | --- |
| `context_window_tokens` | 32768 | Total conservative request allowance |
| `reserved_output_tokens`, `safety_margin_tokens` | 1024, 256 | Decision output reserve and margin |
| `max_tokens`, `max_steps`, `raw_turns_to_keep` | Required | Soft input threshold, fallback run allowance, minimum raw messages |
| `run_max_steps` | null | Optional explicit default run allowance |
| `compaction_interval_steps` | null | Independent optional cadence |
| `hybrid_reflection` | true | Structured XML rather than four-heading format |
| `max_summary_tokens`, `summary_output_tokens` | 4096, 512 | Canonical summary UTF-8 byte cap; model summary output reserve |
| `compaction_max_attempts`, `compaction_max_passes` | 2, 3 | Bounded retries/passes |
| `compaction_min_growth_tokens` | 512 | Required growth after accepted reduction for soft-threshold retrigger |
| `compaction_min_interval_steps` | 0 | Optional spacing since last attempt |
| `optional_compaction_min_pressure_ratio` | 0 | Optional pressure gate |
| `optional_compaction_min_reclaimable_tokens` | 0 | Historical input available for optional reduction |
| `optional_compaction_min_reduction_tokens`, `_ratio` | 0, 0 | Optional useful-reduction minima |

Profiles may override defaults; the current six root `config/` YAML files are authoritative for new scenario sessions. The archived freeze preserves its earlier profile bytes. No model choice or efficiency tuning is imposed by reference consolidation.

## Staging and validation

Hybrid format requires exactly one root and four ordered, nonempty text sections: `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, `CURRENT_LOCAL_PIVOT`. Nonhybrid mode requires the corresponding ordered headings `Core Objective`, `Universal Truths Discovered`, `Dead Ends`, `Current Local Pivot`. Validation rejects malformed structure, extra/nested content/attributes, changed goal, excessive canonical bytes, numeric claims absent from source, stale source/state and insufficient reduction. The goal section comparison uses the original goal's stripped text; the immutable original field remains unchanged.

A proposed replacement is staged on a copy. Failure preserves active raw messages and previous ledger; rejected proposal/reason/source provenance is archived. There is no generated destructive fallback. A soft failure may continue only while the complete decision request fits. A hard failure stops safely. Acceptance installs canonical text, typed ledger and context IDs, preserving the complete raw tail and deterministic evidence/artifacts.

## Atomic storage and history

Each accepted/rejected proposal records source message IDs/SHA-256, prior ledger identity, retained IDs and relevant budget/reduction metadata. Source records remain immutable in `agent_session_transcript`; repository validation checks identities/order/fingerprints/ledger chains before commit. Current context lives in `agent_session_context`; compaction does not mass-update flags or delete source messages. Hot snapshot/context/transcript/diagnostic/event changes commit atomically under revision CAS and fencing. Stale writers cannot install a proposal. History availability gaps during genuine legacy imports are explicitly marked rather than fabricated.

`agent_session_diagnostics` appends compaction events/attempts/decisions; ordinary hydration returns 32 recent entries per family and lifetime counters. Estimated savings are reported only when known. Skip decisions do not themselves add extra emit-driven commits. Full diagnostics/transcript are explicit/paginated. Default retention keeps all records; explicit verified telemetry-only pruning protects transcript, active context, provenance, execution and archive dependencies.

## Limits

Structural/numeric summary acceptance establishes neither universal semantic fidelity nor an optimal compaction rate. Executed workflow/artifact evidence remains authoritative. Inspect diagnostic attempts and final task completion together; repeated accepted summaries alone do not demonstrate progress. Historical qualification measurements are preserved in the parent archive, separate from these operating instructions.

## Exact decision-payload order for ports

`AgentEngine._payload` assembles the following sequence. Optional blocks are omitted when absent; persistence metadata is never added to wire messages.

1. System: immutable configured `system_prompt`.
2. User: `Original user goal (immutable): ` plus the exact original goal. Do not append the initial goal message again from raw history.
3. System: `[FINAL ANSWER REQUIREMENTS]` presentation/literal/schema/deliverable guidance when configured. A regex's raw syntax is not printed as presentation guidance.
4. User: `[COMPACTED CONTEXT STATE — UNVERIFIED MODEL CLAIMS]` wrapper plus canonical ledger, when present. It is memory data, not a system instruction or executed evidence.
5. System: captured executed-result deliverable blocks, in configured result-check order.
6. System raw-buffer marker when raw messages exist, followed by exact `state.memory[1:]` messages in order.
7. System: known result argument bindings, in configured binding order.
8. System: current workflow instruction and **current missing exact constraints**, after historical material.
9. System: final synthesis requirements/artifact renderings when result checks exist and workflow is absent or complete.

The LLM adapter may prepend its tool-policy instruction before this sequence: disabled tools remove the tool list/set `tool_choice:none`; single-call mode sets `parallel_tool_calls:false` and preserves every correlated historical result. The complete request also includes model, tools (excluding private recovery policy), tool choice, temperature and output limit. Budget the adapter's actual output, not just the engine's message list.

## Language-neutral compaction algorithm

The following corresponds to `engine._compact_if_needed`, `context.split_history` and `context.validate_ledger`. A port should implement these transitions explicitly rather than rely on a framework's generic summary-memory behavior.

```text
stage diagnostic outbox
repeat at most compaction_max_passes:
  groups, tail = split complete interactions from memory excluding goal
  measured = budget(actual complete decision payload)
  save measured request_budget
  cadence_due = interval configured AND step - last_success_step >= interval
  grown = no previous successful input estimate OR
          estimated_input >= previous_success_input + min_growth
  soft_due = estimated_input >= max_tokens AND grown
  hard = NOT measured.fits
  if fitting AND neither cadence_due nor soft_due: stop
  if no historical group: record skip; stop
  if not hard AND (workflow complete OR min spacing not reached): skip; stop

  historical = all candidate historical groups
  remaining = empty
  while historical summarizer request does not fit:
    move newest candidate historical group to front of remaining
    preserve group and source order
  if historical is empty: record unaffordable-source skip; stop
  raw = remaining + tail

  protected_floor = estimated decision input with goal + raw, no ledger
  reclaimable = max(0, before_input - protected_floor)
  required_reduction = 1 if hard else
      max(1, optional_absolute_min, ceil(before_input * optional_ratio_min))
  if not hard AND pressure/reclaimable gates fail: skip; stop

  source = exact source_text(goal, historical, previous_ledger)
  fingerprint = SHA256(UTF8(source))
  if fingerprint equals unchanged failed source: skip; stop
  capture before-memory/previous-ledger/step; record attempt step
  attempt at most compaction_max_attempts:
    ask summarizer for proposal using only historical + previous ledger + goal
    validate shape/goal/source numbers/byte cap; canonicalize typed ledger
    require captured memory/previous ledger/step still unchanged
    stage candidate copy with goal + raw and candidate ledger
    after = budget(actual complete candidate decision payload)
    reduction = before_input - after_input
    require reduction > 0 AND reduction >= required_reduction
    on failure:
      archive rejected proposal/reason/source IDs/prior ledger identity
      append bounded attempt diagnostics and correction feedback
      leave active memory/ledger unchanged; try next bounded attempt
    on success:
      replace active raw/ledger/typed ledger on state
      archive summary/proposal/source IDs/hash/retained IDs/prior-ledger chain
      update active ledger ID, successful step/input estimate, diagnostics
      clear unchanged-failed-source marker; stop attempts
  if no accepted candidate: remember failed fingerprint; stop passes
  if accepted request fits: stop passes
return whether a replacement was accepted
```

An accepted hard-pressure replacement may still leave an oversized request; remaining passes can reduce another complete prefix. The caller **always recomputes/requires the complete decision budget** afterward. Acceptance alone does not authorize sending an oversized request. A soft failure can continue only because that final budget check still fits.

## Boundary example: raw messages are not slices of arbitrary turns

```text
goal
assistant: calls A and B
tool: result A
tool: result B
user: clarification
assistant: call C
tool: result C
```

With a two-message raw minimum, retain the complete C group. With a three-message minimum, retain the clarification and C group. A minimum crossing the A/B boundary must retain the complete assistant+A+B group; never summarize its assistant call while leaving its results raw. An orphaned result, duplicate call ID within a batch or missing result is an integrity error. Summary input may shrink only by moving complete groups from its end into the retained prefix before the raw tail.

## Ledger data and byte identities

The typed ledger contains original `goal`, ordered `model_claims`, ordered `dead_ends`, `hypothesis`, `source_sha256`, and verification label `unverified_model_summary`. Hybrid canonical rendering uses root `COMPACTED_STATE` and exactly ordered text-only children `USER_GOAL`, `GLOBAL_LESSON_LEDGER`, `DEAD_ENDS`, `CURRENT_LOCAL_PIVOT`, with newline-delimited text and XML escaping. Reject declarations/entities/comments (`<!` / `<?`), root/child attributes, nested elements, duplicate/extra/out-of-order sections and extra prose. Goal comparison is against `goal.strip()`; the immutable stored goal remains exact.

Numeric grounding extracts signed decimal/scientific values with supported thousands separators and compares decimal numeric sets from all sections against the exact historical-source string. It does not permit calculating a new statistic just because it is mathematically plausible. Regex and formatting details are in `context.numbers`; ports should copy the matching semantics and decimal comparison, not use floating-point substring heuristics. This numeric gate does not verify nonnumeric claims.

The source fingerprint preimage is exactly:

```text
Goal: <original goal>
Previous ledger: <canonical previous ledger, or literal none>
Historical prefix:
<JSON array of historical wire payloads>
```

That JSON sorts keys, preserves array order, uses UTF-8/non-ASCII characters directly, and uses Python's default JSON separators (comma-space and colon-space). This differs from compact commit/config JSON. Whitespace/XML escaping differences therefore change provenance hashes. Ports sharing Python-created sessions must reproduce these exact bytes, canonical XML rendering and decimal semantics; a matching business answer alone does not establish storage compatibility.

## Operational validation

Use scenarios 3 and 4 in the parent [test-cases.md](../../test-cases.md) to compare verified output with and without stress, inspect accepted/rejected compaction diagnostics, and validate cancellation/resume. A lower compaction count is not a pass criterion by itself. These HTTP scenarios do not inject malformed summaries or prove rollback/fencing under database crashes; those deeper regression tools and their original evidence are preserved in the [archive](../../archive/README.md).
