# Implementation notes

`agent_sessions.state` and `.config` are JSONB documents identical to the Python wire schema, while `agent_session_events` is append-only. Every SSE event is written before `data:` is flushed. A per-session mutex prevents an append/run/resume race.

The intended completion adapter is an OpenAI-compatible tool-calling loop: turn config tools into the `tools` request array, append assistant tool calls and tool results exactly as persisted in the state document, apply the YAML phase/evidence guard, compact before deciding, and only then set `complete`. This boundary is isolated in `execute`; it must be supplied before production use. The current checked-in server validates the full persistence/SSE contract but intentionally stops at its requested bound rather than claiming a model answer. This is safer than presenting an incomplete migration as an equivalent agent.

Use `go test ./...` after adding the decision adapter; verify all five cases against the shared [`../test-cases.md`](../test-cases.md).
