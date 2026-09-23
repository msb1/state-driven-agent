# State-Driven Agent — Go

Standalone Go HTTP service implementing the shared REST, JSON session document, PostgreSQL event ledger, and SSE lifecycle contract. It uses `net/http` (Go's concurrent production HTTP server), `pgx` pooling, and YAML snapshots.

## Build and run

```sh
cd agent-go
go mod tidy
AGENT_CONFIG_ROOT=../config AGENT_SESSION_DATABASE_URL='postgresql://…' go run ./cmd/server
go build -trimpath -ldflags='-s -w' -o bin/agent-go ./cmd/server
```

It listens on `PORT` (default `8000`). Swagger UI is served at `/docs` and its OpenAPI document at `/openapi.json`; the checked-in contract is [`openapi.yaml`](openapi.yaml). Persistence/SSE invariants are documented in [`docs/implementation.md`](docs/implementation.md).

The server deliberately does not expose config paths outside `AGENT_CONFIG_ROOT`; `config_ref` remains relative and is hashed at session creation.
