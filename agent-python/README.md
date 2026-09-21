# State-Driven AI Agent (Python)

A framework-free, OpenAI-compatible agent loop with immutable session-selected configuration, evidence-gated workflow phases, hybrid memory compaction, PostgreSQL persistence, and SSE progress events.

## Run once; select workflows per session

From the repository root, generate fixtures and start the API:

```sh
uv run --project agent-python python agent-python/scripts/generate_fixtures.py
uv run --project agent-python uvicorn agent_python.api:app \
  --app-dir agent-python/src --reload
```

The API reads immutable YAML workflow configs from `config/` by default. Add a new versioned YAML file to that directory without restarting the server, then select it in `POST /sessions` with a relative `config_ref`. Override the trusted directory only for server administration with `AGENT_CONFIG_ROOT`.

```sh
curl -sS http://127.0.0.1:8000/configs | jq
```

Each session is stored in PostgreSQL with its fully resolved immutable config snapshot, selected `config_ref`, and SHA-256 hash. A restart can reload memory, compaction ledger, workflow evidence, status, and final answer without trusting a later edit to YAML. Set `AGENT_SESSION_DATABASE_URL` to override the supplied PostgreSQL default.

`POST /sessions` accepts `user_prompt` and optional `config_ref` (default `test-case-1.yaml`). `POST /sessions/{id}/run` executes a bounded JSON run. `POST /sessions/{id}/run/stream` streams SSE lifecycle and phase events. `POST /sessions/{id}/resume` restores even a completed or failed session, optionally accepts `content` as the next user turn, compacts if required, and streams continuation output. Phase-complete interim events are always sent; YAML `output.verbose_setup: true` enables extra setup status events.

Set `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `MAIN_MODEL` in the root `.env` for the local OpenAI-compatible endpoint. See [the shared test runbook](../test-cases.md) for the Case 1, 2, and 3 curl commands and the deterministic Case 3 replay harness.
