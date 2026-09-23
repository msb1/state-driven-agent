# Implementation notes

## Build and run

Run these commands from the `agent-go` directory. Go 1.27 or newer and access
to the PostgreSQL database are required.

The repository-level `.env` file is loaded automatically when the server
starts. Values already exported in the shell take precedence over `.env`.
The `.env` file should contain the local model endpoint, model, API key, and
session database URL. No database export is needed before starting the server:

```sh
cd agent-go
```

The repository `.env` contains defaults for `OPENAI_BASE_URL`,
`OPENAI_API_KEY`, and `MAIN_MODEL`. You can override any of them in the shell.
`AGENT_CONFIG_ROOT` defaults to `../config`, and `PORT` defaults to `8000`.

### Run directly (development)

`go run` compiles the server and starts it in one command:

```sh
go run ./cmd/server
```

### Build a reusable binary

Create the output directory, compile the server, then run the compiled binary:

```sh
mkdir -p bin
go build -trimpath -ldflags='-s -w' -o bin/agent-go ./cmd/server
./bin/agent-go
```

After changing Go source files, rebuild the binary and restart the server.

### Verify the build

Run the tests and whitespace check before handing the service to another user:

```sh
GOCACHE=/tmp/state-driven-agent-go-cache go test ./...
git diff --check
```

The `GOCACHE` setting is optional; it is useful in restricted environments
where Go's default cache directory is not writable. Once the server is
running, confirm it is serving the expected configuration:

```sh
curl -sS http://127.0.0.1:8000/health
curl -sS http://127.0.0.1:8000/configs
```

`agent_sessions.state` and `.config` are JSONB documents identical to the Python wire schema, while `agent_session_events` is append-only. Every SSE event is written before `data:` is flushed. A per-session mutex prevents an append/run/resume race.

The intended completion adapter is an OpenAI-compatible tool-calling loop: turn config tools into the `tools` request array, append assistant tool calls and tool results exactly as persisted in the state document, apply the YAML phase/evidence guard, compact before deciding, and only then set `complete`. This boundary is isolated in `execute`; it must be supplied before production use. The current checked-in server validates the full persistence/SSE contract but intentionally stops at its requested bound rather than claiming a model answer. This is safer than presenting an incomplete migration as an equivalent agent.

Use `go test ./...` after adding the decision adapter; verify all five cases against the shared [`../test-cases.md`](../test-cases.md).
