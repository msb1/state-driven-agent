SELECT s.session_id,s.state,s.config,s.config_ref,s.config_sha256,s.resolved_config_sha256,
    s.created_at,s.updated_at,s.revision,s.last_commit_id,s.last_commit_sha256,s.storage_version,
    s.lease_owner,s.lease_fence,s.lease_expires_at FROM agent_sessions s WHERE FALSE;
SELECT c.command_id,c.namespace,c.client_scope,c.idempotency_key,c.method,c.route,c.request,c.request_sha256,
    c.status,c.session_id,c.response_status,c.response_body,c.owner,c.fence,c.lease_expires_at,
    c.created_at,c.updated_at,c.completed_at FROM agent_commands c WHERE FALSE;
SELECT r.session_id,r.commit_id,r.command_id,r.fingerprint,r.expected_revision,r.committed_revision,
    r.committed_at FROM agent_commit_receipts r WHERE FALSE;
SELECT t.session_id,t.sequence,t.message_id,t.kind,t.payload,t.run_id,t.decision_step,t.created_at,t.recorded_at
    FROM agent_session_transcript t WHERE FALSE;
SELECT c.session_id,c.revision,c.goal_message_id,c.ledger_record_id,c.raw_message_ids,c.ledger_state
    FROM agent_session_context c WHERE FALSE;
SELECT e.event_id,e.session_id,e.event,e.created_at,e.revision,e.commit_id,e.command_id
    FROM agent_session_events e WHERE FALSE;
SELECT r.run_id,r.session_id,r.command_id,r.owner,r.fence,r.status,r.started_at,r.ended_at,r.termination_reason,
    r.last_decision_step,r.cancel_requested,r.cancel_requested_at,r.metadata FROM agent_runs r WHERE FALSE;
SELECT x.execution_id,x.session_id,x.run_id,x.call_id,x.tool,x.arguments,x.policy,x.command_id,x.namespace,
    x.target,x.key_algorithm,x.idempotency_key,x.payload_sha256,x.attempts,x.status,x.result,x.owner,x.fence,
    x.created_at,x.updated_at,x.started_at,x.finished_at,x.payload FROM agent_tool_executions x WHERE FALSE;
SELECT a.attempt_id,a.session_id,a.execution_id,a.command_id,a.attempt_number,a.kind,a.status,a.payload_sha256,
    a.owner,a.fence,a.external_reference,a.evidence,a.error,a.started_at,a.finished_at
    FROM agent_action_attempts a WHERE FALSE;
SELECT d.session_id,d.family,d.ordinal,d.payload FROM agent_session_diagnostics d WHERE FALSE;
SELECT c.session_id,c.call_id,c.message_id,c.created_at FROM agent_session_calls c WHERE FALSE;
SELECT a.archive_id,a.session_id,a.snapshot_revision,a.sha256,a.manifest,a.dependency_receipt,a.created_at
    FROM agent_archive_receipts a WHERE FALSE;
DO $$
DECLARE required TEXT;
BEGIN
    FOREACH required IN ARRAY ARRAY[
        'agent_sessions','agent_commands','agent_commit_receipts','agent_session_transcript',
        'agent_session_context','agent_session_events','agent_runs','agent_tool_executions',
        'agent_action_attempts','agent_session_diagnostics','agent_session_calls','agent_archive_receipts'
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=current_schema() AND c.relname=required AND c.relkind='r') THEN
            RAISE EXCEPTION 'Schema 6 table missing from current schema: %', required;
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema()
        AND table_name='agent_tool_executions' AND column_name='idempotency_key' AND data_type='text') THEN
        RAISE EXCEPTION 'Schema 6 requires text action idempotency keys';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema()
        AND table_name='agent_sessions' AND column_name='storage_version' AND column_default='6') THEN
        RAISE EXCEPTION 'Schema 6 storage version is missing';
    END IF;
END $$;
