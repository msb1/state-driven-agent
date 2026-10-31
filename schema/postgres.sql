CREATE TABLE agent_sessions (
    session_id UUID PRIMARY KEY, state JSONB NOT NULL, config JSONB NOT NULL,
    config_ref TEXT, config_sha256 TEXT, resolved_config_sha256 TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revision BIGINT NOT NULL DEFAULT 0, last_commit_id UUID, last_commit_sha256 TEXT,
    storage_version INTEGER NOT NULL DEFAULT 6 CHECK (storage_version = 6),
    lease_owner UUID, lease_fence BIGINT NOT NULL DEFAULT 0, lease_expires_at TIMESTAMPTZ
);
CREATE TABLE agent_commands (
    command_id UUID PRIMARY KEY,
    namespace TEXT NOT NULL CHECK (namespace <> ''),
    client_scope TEXT NOT NULL CHECK (client_scope <> ''),
    idempotency_key TEXT NOT NULL CHECK (idempotency_key ~ '^[!-~]{1,255}$'),
    method TEXT NOT NULL CHECK (method = 'POST'),
    route TEXT NOT NULL, request JSONB NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('pending','started','succeeded','failed','uncertain','blocked')),
    session_id UUID REFERENCES agent_sessions(session_id),
    response_status INTEGER CHECK (response_status BETWEEN 200 AND 599), response_body JSONB,
    owner UUID, fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0), lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT agent_commands_key UNIQUE (namespace,client_scope,idempotency_key),
    CONSTRAINT agent_commands_namespace UNIQUE (command_id,namespace),
    CONSTRAINT agent_commands_session UNIQUE (session_id,command_id),
    CONSTRAINT agent_commands_outcome CHECK (
        ((status IN ('succeeded','failed')) AND response_status IS NOT NULL AND response_body IS NOT NULL AND completed_at IS NOT NULL) OR
        ((status NOT IN ('succeeded','failed')) AND response_status IS NULL AND response_body IS NULL AND completed_at IS NULL)),
    CONSTRAINT agent_commands_lease CHECK ((owner IS NULL) = (lease_expires_at IS NULL))
);
CREATE INDEX agent_commands_recovery_idx ON agent_commands(status,lease_expires_at);
CREATE INDEX agent_commands_session_idx ON agent_commands(session_id,created_at,command_id);
CREATE TABLE agent_commit_receipts (
    session_id UUID NOT NULL REFERENCES agent_sessions(session_id),
    commit_id UUID NOT NULL,
    command_id UUID NOT NULL,
    fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    expected_revision BIGINT NOT NULL CHECK (expected_revision >= 0),
    committed_revision BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id,commit_id), UNIQUE(session_id,committed_revision),
    FOREIGN KEY(session_id,command_id) REFERENCES agent_commands(session_id,command_id),
    CHECK (committed_revision = expected_revision + 1)
);
CREATE TABLE agent_session_events (
    event_id BIGSERIAL PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    event JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revision BIGINT, commit_id UUID, command_id UUID,
    FOREIGN KEY(session_id,command_id) REFERENCES agent_commands(session_id,command_id),
    FOREIGN KEY(session_id,commit_id) REFERENCES agent_commit_receipts(session_id,commit_id)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX agent_session_events_session_id_idx ON agent_session_events(session_id,event_id);
CREATE UNIQUE INDEX agent_session_events_commit_idx ON agent_session_events(session_id,commit_id)
    WHERE commit_id IS NOT NULL;
CREATE TABLE agent_session_transcript (
    session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL CHECK(sequence>0), message_id UUID NOT NULL, kind TEXT NOT NULL,
    payload JSONB NOT NULL, run_id UUID, decision_step BIGINT NOT NULL CHECK(decision_step>=0),
    created_at TIMESTAMPTZ NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(session_id,sequence), UNIQUE(session_id,message_id)
);
CREATE TABLE agent_session_context (
    session_id UUID PRIMARY KEY REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    revision BIGINT NOT NULL, goal_message_id UUID NOT NULL, ledger_record_id UUID,
    raw_message_ids JSONB NOT NULL CHECK(jsonb_typeof(raw_message_ids)='array'), ledger_state JSONB,
    FOREIGN KEY(session_id,goal_message_id) REFERENCES agent_session_transcript(session_id,message_id),
    FOREIGN KEY(session_id,ledger_record_id) REFERENCES agent_session_transcript(session_id,message_id)
);
CREATE TABLE agent_runs (
    run_id UUID PRIMARY KEY, session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    command_id UUID NOT NULL UNIQUE,
    owner UUID NOT NULL, fence BIGINT NOT NULL, status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ, termination_reason TEXT,
    last_decision_step BIGINT NOT NULL, cancel_requested BOOLEAN NOT NULL DEFAULT false,
    cancel_requested_at TIMESTAMPTZ, metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(session_id,run_id), UNIQUE(session_id,run_id,command_id),
    FOREIGN KEY(session_id,command_id) REFERENCES agent_commands(session_id,command_id)
);
CREATE INDEX agent_runs_active_idx ON agent_runs(session_id,status);
CREATE INDEX agent_runs_page_idx ON agent_runs(session_id,started_at,run_id);
CREATE TABLE agent_tool_executions (
    execution_id UUID PRIMARY KEY, session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    run_id UUID NOT NULL, call_id TEXT NOT NULL, tool TEXT NOT NULL, arguments JSONB NOT NULL,
    policy JSONB NOT NULL,
    command_id UUID NOT NULL, namespace TEXT NOT NULL,
    target TEXT NOT NULL CHECK (target <> ''),
    key_algorithm TEXT NOT NULL DEFAULT 'agent-action-v1' CHECK (key_algorithm = 'agent-action-v1'),
    idempotency_key TEXT NOT NULL CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    attempts INTEGER NOT NULL CHECK(attempts>=0),
    status TEXT NOT NULL CHECK(status IN ('pending','started','succeeded','failed','uncertain','blocked')),
    result TEXT, owner UUID NOT NULL, fence BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE(session_id,call_id), UNIQUE(session_id,execution_id),
    UNIQUE(namespace,idempotency_key),
    FOREIGN KEY(session_id,run_id,command_id) REFERENCES agent_runs(session_id,run_id,command_id),
    FOREIGN KEY(command_id,namespace) REFERENCES agent_commands(command_id,namespace),
    CHECK((status IN ('succeeded','failed')) = (result IS NOT NULL))
);
CREATE INDEX agent_tool_executions_recovery_idx ON agent_tool_executions(session_id,status,started_at);
CREATE INDEX agent_tool_executions_page_idx ON agent_tool_executions(session_id,execution_id);
CREATE TABLE agent_session_diagnostics (
    session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    family TEXT NOT NULL, ordinal BIGINT NOT NULL CHECK(ordinal>0), payload JSONB NOT NULL,
    PRIMARY KEY(session_id,family,ordinal)
);
CREATE TABLE agent_session_calls (
    session_id UUID NOT NULL, call_id TEXT NOT NULL, message_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(session_id,call_id,message_id),
    FOREIGN KEY(session_id,message_id) REFERENCES agent_session_transcript(session_id,message_id) ON DELETE CASCADE
);
CREATE TABLE agent_archive_receipts (
    archive_id UUID PRIMARY KEY, session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    snapshot_revision BIGINT NOT NULL, sha256 TEXT NOT NULL, manifest JSONB NOT NULL,
    dependency_receipt UUID, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX agent_archive_receipts_page_idx ON agent_archive_receipts(session_id,created_at,archive_id);

CREATE TABLE agent_action_attempts (
    attempt_id UUID PRIMARY KEY,
    session_id UUID NOT NULL, execution_id UUID NOT NULL,
    command_id UUID NOT NULL,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    kind TEXT NOT NULL CHECK (kind IN ('dispatch','verify')),
    status TEXT NOT NULL CHECK (status IN ('started','succeeded','failed','uncertain')),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    owner UUID NOT NULL, fence BIGINT NOT NULL CHECK (fence >= 0),
    external_reference TEXT, evidence JSONB, error JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ,
    UNIQUE(execution_id,attempt_number),
    FOREIGN KEY(session_id,execution_id) REFERENCES agent_tool_executions(session_id,execution_id),
    FOREIGN KEY(session_id,command_id) REFERENCES agent_commands(session_id,command_id),
    CHECK ((status = 'started') = (finished_at IS NULL))
);
CREATE INDEX agent_action_attempts_page_idx ON agent_action_attempts(session_id,execution_id,attempt_number);
