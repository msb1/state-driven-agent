# Universal state-driven agent

The universal state-driven agent is a configurable agent runtime for tasks that require multiple model decisions, tools, verifiable progress, and recovery. Instead of treating the model's conversation as the source of truth, it keeps an explicit session state and durable execution history. The model proposes actions; the runtime validates them, records what happened, and decides whether the task may advance or finish.

We are developing the same language-independent agent in **Python, Go, Java, and Rust** against a shared observable contract. [Contract 1.1.0](contract.md) established the baseline; **contract 2.0.0 and PostgreSQL schema 6** are the active target, adding durable command identity, replay, commit receipts, and external action recovery. The contract defines the same behavior and qualification expectations for every language; it does not imply that the ports are already equivalent.

Python is the reference for the 2.0 runtime. Java implements contract 2.0.0 and schema 6; the user-run `test-cases.sh`, `persist.sh`, and `aux-tests.sh` qualification has passed. See [digest-java.md](digest-java.md) for evidence and scope. Go and Rust implementation and qualification status is recorded in their digests. See [contract.md](contract.md), [design_update.md](design_update.md), and the language READMEs for requirements, evidence, and current limitations.

## What it is for

The agent is suited to work where an answer depends on a sequence of inspect, act, check, and possibly retry steps, and where operators need to understand or resume that work. A profile can define a phased workflow or a bounded task without one. The repository's examples include:

- **Operational analysis:** inspect malformed access logs, identify the leading source of HTTP 500 errors, and validate the result.
- **Data and financial analysis:** inspect a database schema, normalize inconsistent keys and city names, calculate revenue, and validate the total.
- **Multi-phase audits:** detect corrupt employee records, calculate a valid salary statistic, and produce a checked report from cleaned data.
- **Recovery and long-running work:** retain evidence through context compaction, stop at a step limit, resume after interruption, and handle tool failures or uncertain action outcomes.

These are fixture-backed demonstrations, not claims that arbitrary integrations are production-ready. Other applications can use the same architecture when their tools, evidence requirements, recovery policies, and completion checks are implemented and configured.

## How an agent iterates

1. A client creates a session with a goal and a configuration profile. The resolved configuration and original goal become durable session anchors.
2. A run loads the current state, workflow phase, relevant history, and verified evidence. The model receives that context and proposes structured tool calls or a final answer.
3. The runtime checks the proposed action against the registered tools, argument schemas, global policy, and any phase-specific permissions. Invalid actions become feedback for a later decision; they are not executed.
4. For an admitted tool call, the runtime records intent before dispatch and records the outcome and related state. Executed results can satisfy configured evidence requirements and permit workflow transitions. A model's assertion alone does not establish evidence.
5. The agent repeats until the completion rules are satisfied or a limit, cancellation, failure, or unresolved action stops progress. After a workflow reaches its terminal edge, it enters a final-only state: further tool calls are rejected and the answer must pass the configured checks.

Runs are bounded by step and no-progress limits. Sessions can be resumed with their goal and recorded state intact. During iteration, the agent also manages the context it sends to the model: compaction and hybrid reflection carry useful working context forward as the history grows.

## How the next step is chosen

There are two cooperating decision makers. The **model** chooses a proposed tool function and arguments, or proposes a final answer, using the goal, instructions, tool descriptions, recent results, current phase, and missing requirements. The **runtime** controls which proposals are admissible, evaluates evidence and transitions, manages context, and decides whether to continue or stop. A tool function is an implementation registered by the agent; a validator is a tool whose result can be required as evidence. The runtime does not assume that every successful function call validates the task.

At each iteration the runtime evaluates the current state and follows the applicable path:

| Situation | What happens next |
| --- | --- |
| A previously recorded action needs recovery | Resolve the pending action under its recorded policy before replacing it with newly generated work. An uncertain effect may require reconciliation or block the run. |
| A workflow transition is satisfied | Record the transition and provide the next phase's instructions, allowed tools, and missing evidence to the model. A `phase_complete` transition requires that phase's required evidence; an `evidence` transition uses its configured evidence; an `always` transition is unconditional. |
| Context pressure or configured cadence calls for compaction | Attempt the validated compaction described below, then check the complete decision request against its budget. Compaction is runtime-managed; the model does not choose it as a tool function. |
| The model proposes tool calls | Check their names, arguments, policy, phase permissions, and configured result bindings. Execute admitted calls and record results; return errors and corrections as context for another decision. Evidence is matched to the actual tool name, arguments, and result or text conditions. |
| The model proposes a final answer | Check workflow completion and the profile's final-answer rules. Accept it only if those checks pass; otherwise record the rejection and give the model actionable feedback about missing requirements. |
| The run cannot safely continue | Stop on a step limit, configured repeated-invalid or no-progress limit, context budget failure, cancellation, model failure, or unresolved action. The resulting status and diagnostics explain the stop; where allowed, a later run can resume. |

Progress is measured from changes to workflow state and captured completion artifacts, and, for workflow-free tasks, distinct observed tool outputs. Repeating a call, generating a new call ID, or producing another summary is not by itself evidence of progress. If configured, the no-progress policy warns the model and eventually stops repeated decisions that do not change the relevant state. The runtime also limits consecutive invalid responses. Limits and feedback help prevent indefinite iteration, but do not guarantee the model will find a solution.

## What counts as success

Success is defined by the selected profile, not by the model simply saying it is done. For a workflow, tool results must establish the configured evidence and satisfy the transitions leading to `complete`. A result matches evidence only when the configured tool and argument conditions and any required result or text conditions match; an error can count as evidence only when the profile explicitly calls for it. A model statement or a compacted reflection cannot substitute for a tool result. Once the terminal edge is reached, the agent allows final synthesis but rejects further tool calls.

The proposed final answer must be nonempty and meet any configured checks: an exact string or presentation pattern, forbidden-pattern exclusions, a JSON schema, and/or required artifacts extracted from specified executed tool results. For example, a configured table or number can be checked against its captured source result. Argument bindings can also require a later validator call to use a value from an earlier result. If a check fails, the agent records the rejected answer and can try again within its limits. Only an accepted final answer completes the task; a stream ending, a successful individual tool call, or a stopped run does not establish success. Workflow-free profiles still apply their configured tool policy and final-answer checks.

For a concrete example, the [revenue profile](config/revenue-analysis.yaml) asks the model to inspect the schema, calculate Chicago Q1 revenue with normalized city and keys, and call the validator. The phase requires the validator's executed result to contain `valid: true`. Until that evidence is recorded, a final answer is premature; after the phase reaches `complete`, the model can synthesize the answer under the profile's completion rules. The more involved [employee audit](config/employee-audit.yaml) requires evidence across three phases, binds a validator argument to the captured salary result, and checks the final report against the executed table and salary artifacts. The strength of a pass depends on the configured checks and the correctness of the tools and their data; it is not a universal proof of factual correctness.

## Compaction and hybrid reflection

An iterative task can accumulate many decisions, tool calls, results, errors, and corrections. Sending all of that history on every decision eventually consumes the model's context budget. The agent measures the **complete request**, including instructions, tool definitions, evidence, recent messages, output reserve, and a safety margin. This is an application estimate, not a provider token count. Configured context pressure or a compaction interval can trigger an attempt to reduce older history; hard budget protection applies even when optional compaction would otherwise be skipped.

With `hybrid_reflection` enabled, the runtime asks the model to reflect on a prefix of older, complete interactions and any previous reflection. It produces a structured working ledger with four parts:

| Section | What it carries forward |
| --- | --- |
| `USER_GOAL` | The original goal, restored from the immutable session state by the runtime. |
| `GLOBAL_LESSON_LEDGER` | Concise, source-grounded lessons from the work so far. |
| `DEAD_ENDS` | Approaches that failed or proved unhelpful, so subsequent decisions can take those failures into account. |
| `CURRENT_LOCAL_PIVOT` | A provisional direction or hypothesis for what to try next. |

The reflection combines retained lessons and failed approaches with a current direction; recent interactions remain available as raw messages. On the next iteration, the model receives this compacted context alongside the original goal, protected instructions, current workflow requirements, authoritative evidence, and the complete recent tool-call/result groups. For example, during the employee audit it can carry forward lessons from earlier parsing failures while continuing with the current phase and its outstanding evidence. This gives the agent room to continue reasoning and acting without having to resend every earlier interaction, while preserving the context needed to avoid repeating unproductive work. Compaction can recur as the task progresses; it does not itself advance a workflow or guarantee a correct solution.

The runtime stages each proposed reflection and checks its structure, size, source-grounded numeric values, freshness, and actual reduction of the complete decision request before accepting it. Rejected proposals leave the previous working context intact; retries and passes are bounded. If the request cannot be reduced enough to fit safely, the agent stops rather than sending an oversized request. The original transcript and compaction provenance remain in the durable ledger, and recent tool interactions are retained as complete groups.

Reflection is **unverified model-generated context**, not proof that its lessons or proposed pivot are correct. Executed tool results, workflow evidence, and required output artifacts remain separate and authoritative; a reflection cannot satisfy an evidence gate or override the goal. The exact implementation, configuration, validation rules, and porting status are described in [compaction](agent-python/docs/compaction.md).

## Guardrails, ownership, and fencing

Configuration controls which tools are registered and allowed, the permitted arguments, workflow phases and transitions, required evidence, output checks, execution limits, and recovery policies. The runtime validates structured arguments without silently coercing types; checks final answers against configured rules and captured results; and rejects premature completion or out-of-phase tools. Tool errors can provide diagnostic evidence when a profile explicitly requires it. These controls constrain agent behavior but do not make model output inherently trustworthy.

PostgreSQL coordinates session ownership with an expiring lease and a monotonically increasing **fence**. A worker must hold current authority to commit state; a stale worker cannot overwrite a successor after its lease expires. Revision checks reject stale writes, and related state, history, and event updates commit atomically. Command leases provide similar protection for requests before a session exists. Cancellation is cooperative: an in-flight operation may need to settle, and known results are preserved. A database fence protects the agent's ledger; it cannot by itself prevent an independent external service from applying an effect.

## Configuration

Shared YAML profiles live in [config/](config/). They describe the agent instructions and model connection, memory and execution budgets, registered tool metadata and argument schemas, optional workflow and evidence rules, completion criteria, and output behavior. Profiles reference tools implemented by the runtime; YAML does not install executable tool code. A `config_ref` is resolved under a trusted configuration root, with invalid paths and invalid profiles rejected.

Environment settings supply deployment-specific values such as `MAIN_MODEL`, the model endpoint, credentials, PostgreSQL connection settings, and the stable `AGENT_DEPLOYMENT_NAMESPACE` required by contract 2.0.0. The agent uses an OpenAI-compatible model endpoint and does not require a particular model. At session creation the runtime stores the source hash and resolved configuration; subsequent profile or environment changes apply to new sessions, not existing snapshots. See each language's README for its startup settings and [operations](agent-python/docs/operations.md) for Python database setup.

## Ledger, idempotency, and recovery

The PostgreSQL ledger is the durable record of the session and its work. It retains ordered transcript and event history, workflow evidence, runs, tool execution intent and outcomes, diagnostics, and compaction provenance alongside state snapshots. Contract 2.0 adds durable commands, commit receipts, and action attempts. These records support inspection, recovery, and audit; they are not merely a copy of the model's current context. Default retention is keep-all, with guarded, explicit telemetry pruning after a verified export.

See [PostgreSQL tables and the durable agent ledger](postgres-ledger.md) for each table, its relationships, and the contract 2.0 request, commit, and action identities.

Contract 2.0 separates three identities:

| Identity | Purpose |
| --- | --- |
| **Request idempotency key** | The client supplies `Idempotency-Key` on each mutating request and retains it for retries of the same intent. The server scopes it by deployment namespace and client scope, stores the command, and replays a completed JSON response or returns the existing in-progress command. Reusing the key with a different request is rejected. A new intent needs a new key. |
| **Commit UUID** | Identifies one proposed database transaction so an uncertain commit can be resolved from its receipt without duplicating records or advancing the revision twice. |
| **Execution UUID and action key** | Identify one logical tool action. The runtime freezes its arguments, payload fingerprint, recovery policy, and derived key before dispatch. The same identity survives retries, resumes, and ownership changes; an intentionally repeated action gets a new execution identity. |

For an uncertain external action, safe recovery depends on the recipient and the recorded policy. A genuinely idempotent adapter must pass the stable key to a recipient that atomically deduplicates it and checks the payload; otherwise an authoritative reconciliation may establish the outcome. If neither can safely resolve it, the action remains uncertain or blocked for resolution. The ledger does **not** promise global exactly-once effects or roll back effects in external systems. An idempotency key is not an authorization credential. See the [2.0 contract](contract.md#contract-200--durable-commands-and-action-recovery) for exact identities, replay rules, and recovery requirements.

## Implementation and security

The agent's orchestration and state machinery are written directly in the four implementation languages, independently of agent frameworks such as LangChain, LangGraph, and LlamaIndex. This avoids a dependency on those frameworks and therefore exposure to vulnerabilities in their code when they are not installed or used. Community-developed frameworks can have serious CVEs, as can any software dependency; avoiding them is not a guarantee that this agent is free of vulnerabilities. The implementations still use libraries and application frameworks, PostgreSQL, model services, and tools, all of which require their own security review, updates, access controls, and operational safeguards. In particular, the contract's current trusted `local` client scope is not authentication or authorization.

## Getting started and documentation

Initialize PostgreSQL and fixtures as described in [schema/README.md](schema/README.md), configure the model and database settings, and start the implementation you want to use. The language guides contain their respective commands and prerequisites:

- [Python](agent-python/README.md) · [Go](agent-go/README.md) · [Java](agent-java/README.md) · [Rust](agent-rust/README.md)
- [Shared test cases](test-cases.md) and [persistence qualification](test-cases.md#19-shared-postgresql-persistence-qualification) describe user-run checks against a selected server.
- [Python architecture](agent-python/docs/architecture.md) and [implementation guide](agent-python/docs/implementation.md) explain the reference runtime and persistence boundaries.
- [Design update](design_update.md), [answers](answers.md), and [digest](digest.md) provide porting plans and qualification history; [archive/](archive/README.md) retains historical material.
