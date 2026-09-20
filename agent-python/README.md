# State-Driven AI Agent (Python)

A framework-free, OpenAI-compatible agent loop: immutable system prompt and goal; mutable message array; asynchronous HTTP model calls; tool results appended as messages; and phased compaction that retains the newest raw turns.

## Run

From the repository root, generate the deterministic test fixtures and start the API:

```sh
cd agent-python
uv run python scripts/generate_fixtures.py
cd ..
uv run --project agent-python uvicorn agent_python.api:app --app-dir agent-python/src --reload
```

Swagger UI is at `http://127.0.0.1:8000/docs`. Set `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `MAIN_MODEL` in the root `.env` for the local OpenAI-compatible endpoint. `config/agent.yaml` enables Test Case 1. Test Case 2 is supplied as a fully commented config template (uncomment it when Test Case 1 is vetted) and its local SQLite fixture is generated alongside the log.

The primary endpoints are `POST /sessions` (initializes memory from `user_prompt`), `POST /sessions/{id}/messages` (adds user context), `POST /sessions/{id}/run`, and `GET /sessions/{id}`.
