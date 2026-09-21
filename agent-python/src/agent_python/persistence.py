"""PostgreSQL persistence for resumable agent sessions.

The session state is intentionally stored as one JSONB document: the Pydantic
model remains the portable cross-language contract.  A separate append-only
event table provides an audit trail for streamed lifecycle events.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import AgentConfig
from .models import SessionState

DEFAULT_DATABASE_URL = "postgresql://user:password@192.168.1.50:5432/elite_rag"


@dataclass(frozen=True)
class PersistedSession:
    state: SessionState
    config: AgentConfig


class SessionRepository:
    """Small async repository; connections are short-lived and transaction-scoped."""

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or os.getenv("AGENT_SESSION_DATABASE_URL", DEFAULT_DATABASE_URL)

    async def initialize(self) -> None:
        async with await psycopg.AsyncConnection.connect(self.database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_sessions (
                        session_id UUID PRIMARY KEY,
                        state JSONB NOT NULL,
                        config JSONB NOT NULL,
                        config_ref TEXT,
                        config_sha256 TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                await cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_session_events (
                        event_id BIGSERIAL PRIMARY KEY,
                        session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
                        event JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                await cursor.execute(
                    "CREATE INDEX IF NOT EXISTS agent_session_events_session_id_idx "
                    "ON agent_session_events (session_id, event_id)"
                )
            await connection.commit()

    async def save(self, state: SessionState, config: AgentConfig, event: dict[str, Any] | None = None) -> None:
        async with await psycopg.AsyncConnection.connect(self.database_url) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT INTO agent_sessions (session_id, state, config, config_ref, config_sha256)
                    VALUES (%s, %s::jsonb, %s::jsonb, %s, %s)
                    ON CONFLICT (session_id) DO UPDATE SET
                        state = EXCLUDED.state,
                        config = EXCLUDED.config,
                        config_ref = EXCLUDED.config_ref,
                        config_sha256 = EXCLUDED.config_sha256,
                        updated_at = now()
                    """,
                    (state.id, _json(state.model_dump(mode="json")), _json(config.model_dump(mode="json")), state.config_ref, state.config_sha256),
                )
                if event is not None:
                    await cursor.execute(
                        "INSERT INTO agent_session_events (session_id, event) VALUES (%s, %s::jsonb)",
                        (state.id, _json(event)),
                    )
            await connection.commit()

    async def get(self, session_id: str) -> PersistedSession | None:
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT state, config FROM agent_sessions WHERE session_id = %s", (session_id,))
                row = await cursor.fetchone()
        if row is None:
            return None
        return PersistedSession(state=SessionState.model_validate(row["state"]), config=AgentConfig.model_validate(row["config"]))


def _json(value: Any) -> str:
    # psycopg JSON adapters are optional; serializing explicitly keeps the DB
    # representation stable across driver versions.
    import json
    return json.dumps(value, separators=(",", ":"))
