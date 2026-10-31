package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

var ErrRevisionConflict = errors.New("session revision conflict")
var ErrFencingConflict = errors.New("session fencing conflict")

func commitFingerprint(state agent.Session, configHash string, event any) (string, error) {
	hot, err := state.HotSnapshot()
	if err != nil {
		return "", err
	}
	// Keep the exact ordered list preimage used by Python SessionRepository.save.
	preimage := []any{"agent-commit-v2", state.ID, state.CommandID, state.Revision, hot, state.Memory, state.CompactedContext, state.CompactionLedger, state.TranscriptPending, map[string]any{"executions": state.ExecutionPending, "attempts": state.AttemptsPending}, state.DiagnosticsPending, state.RetentionPending, configHash, event}
	return agent.DigestV2(preimage)
}

func (s *server) save(ctx context.Context, p *persisted, event any) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = s.saveOnce(ctx, p, event)
		if err == nil {
			return nil
		}
		var network net.Error
		if !errors.As(err, &network) && !errors.Is(err, io.EOF) && !pgconn.SafeToRetry(err) && !strings.Contains(err.Error(), "conn closed") {
			return err
		}
	}
	return err
}
func (s *server) saveOnce(ctx context.Context, p *persisted, event any) error {
	c := currentCommand(ctx)
	if c == nil {
		return fmt.Errorf("durable command required for every save")
	}
	if p.State.NeedsReload {
		return ErrRevisionConflict
	}
	p.State.CommandID = c.ID
	p.State.DeploymentNamespace = c.Namespace
	if mapped, ok := event.(map[string]any); ok {
		mapped["command_id"] = c.ID
		mapped["run_id"] = p.State.RunID
	}
	started := time.Now()
	dbStarted := time.Time{}
	snapshotBytes, writtenJSONBytes := 0, 0
	defer func() {
		dbDuration := time.Duration(0)
		if !dbStarted.IsZero() {
			dbDuration = time.Since(dbStarted)
		}
		s.recordIOSample(map[string]any{"operation": "save", "session_id": p.State.ID, "wall_ms": float64(time.Since(started).Microseconds()) / 1000,
			"database_transaction_ms": float64(dbDuration.Microseconds()) / 1000, "snapshot_json_bytes": snapshotBytes,
			"written_json_bytes": writtenJSONBytes})
	}()
	// Work on a detached proposal: rollback leaves caller outboxes/revision intact.
	if p.State.PendingCommitID == nil {
		id := uuid.NewString()
		p.State.PendingCommitID = &id
	}
	agent.StageMessages(&p.State)
	agent.StageDiagnostics(&p.State)
	proposal := agent.Clone(p.State)
	agent.StageMessages(&proposal)
	agent.StageDiagnostics(&proposal)
	var config agent.Config
	if err := json.Unmarshal(p.Config, &config); err != nil {
		return err
	}
	identity, err := agent.ConfigIdentity(config)
	if err != nil {
		return err
	}
	if proposal.ResolvedConfigSHA256 != "" && proposal.ResolvedConfigSHA256 != identity {
		return fmt.Errorf("immutable resolved configuration changed")
	}
	proposal.ResolvedConfigSHA256 = identity
	fingerprint, err := commitFingerprint(proposal, identity, event)
	if err != nil {
		return err
	}
	dbStarted = time.Now()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT command_id FROM agent_commands WHERE command_id=$1 FOR UPDATE`, c.ID); err != nil {
		return err
	}
	if c.Route == "/admin/reconcile" && proposal.RunCommandID != nil {
		if _, err = tx.Exec(ctx, `SELECT command_id FROM agent_commands WHERE command_id=$1 FOR UPDATE`, *proposal.RunCommandID); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE agent_commands SET status='uncertain',owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE command_id=$1 AND status IN ('pending','started') AND (owner IS NULL OR lease_expires_at<=clock_timestamp())`, *proposal.RunCommandID); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,734910003))`, proposal.ID); err != nil {
		return err
	}
	var receiptCommand, receiptHash string
	var committed int64
	err = tx.QueryRow(ctx, `SELECT command_id::text,fingerprint,committed_revision FROM agent_commit_receipts WHERE session_id=$1 AND commit_id=$2`, proposal.ID, *proposal.PendingCommitID).Scan(&receiptCommand, &receiptHash, &committed)
	if err == nil {
		if receiptCommand != c.ID || receiptHash != fingerprint {
			return fmt.Errorf("commit identity reused with changed content")
		}
		var latest int64
		if err = tx.QueryRow(ctx, `SELECT revision FROM agent_sessions WHERE session_id=$1`, proposal.ID).Scan(&latest); err != nil {
			return err
		}
		proposal.Revision = committed
		proposal.NeedsReload = latest != committed
		proposal.TranscriptPending = nil
		proposal.ExecutionPending = nil
		proposal.AttemptsPending = nil
		proposal.DiagnosticsPending = nil
		proposal.RetentionPending = nil
		proposal.PendingCommitID = nil
		p.State = proposal
		var commandStatus string
		if e := tx.QueryRow(ctx, `SELECT status FROM agent_commands WHERE command_id=$1`, c.ID).Scan(&commandStatus); e == nil && (commandStatus == "succeeded" || commandStatus == "failed") {
			c.mu.Lock()
			c.completed = true
			c.mu.Unlock()
		}
		return nil
	}
	if err != pgx.ErrNoRows {
		return err
	}
	if err = s.guardCommand(ctx, tx, c); err != nil {
		return err
	}
	initial, _ := json.Marshal(proposal)
	writtenJSONBytes = len(initial) + len(p.Config)
	inserted, err := tx.Exec(ctx, `INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256,resolved_config_sha256) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, proposal.ID, initial, p.Config, proposal.ConfigRef, proposal.ConfigSHA256, identity)
	if err != nil {
		return err
	}
	var revision int64
	var priorState, priorConfig []byte
	var priorRef, priorHash *string
	var lastCommit, priorFingerprint, leaseOwner *string
	var leaseFence int64
	var leaseValid bool
	if err = tx.QueryRow(ctx, `SELECT revision,state,config,config_ref,config_sha256,last_commit_id::text,last_commit_sha256,lease_owner::text,lease_fence,lease_owner IS NULL OR lease_expires_at>clock_timestamp() FROM agent_sessions WHERE session_id=$1 FOR UPDATE`, proposal.ID).Scan(&revision, &priorState, &priorConfig, &priorRef, &priorHash, &lastCommit, &priorFingerprint, &leaseOwner, &leaseFence, &leaseValid); err != nil {
		return err
	}
	if leaseOwner != nil {
		if proposal.ExecutionOwner == nil || proposal.ExecutionFence == nil || *proposal.ExecutionOwner != *leaseOwner || *proposal.ExecutionFence != leaseFence || !leaseValid {
			return ErrFencingConflict
		}
	} else if proposal.ExecutionOwner != nil || proposal.ExecutionFence != nil {
		return ErrFencingConflict
	}
	if lastCommit != nil && *lastCommit == *proposal.PendingCommitID {
		if priorFingerprint == nil || *priorFingerprint != fingerprint {
			return fmt.Errorf("commit identity reused with different content")
		}
		proposal.Revision = revision
		proposal.TranscriptPending, proposal.DiagnosticsPending, proposal.ExecutionPending, proposal.RetentionPending, proposal.PendingCommitID = nil, nil, nil, nil, nil
		p.State = proposal
		return nil
	}
	if revision != proposal.Revision {
		return fmt.Errorf("%w: expected %d, stored %d", ErrRevisionConflict, proposal.Revision, revision)
	}
	if inserted.RowsAffected() == 0 {
		var prior agent.Session
		if err = json.Unmarshal(priorState, &prior); err != nil {
			return err
		}
		var oldConfig agent.Config
		if err = json.Unmarshal(priorConfig, &oldConfig); err != nil {
			return err
		}
		oldIdentity, e := agent.ConfigIdentity(oldConfig)
		if e != nil {
			return e
		}
		if prior.Goal != proposal.Goal || oldIdentity != identity || priorRef == nil || *priorRef != proposal.ConfigRef || priorHash == nil || *priorHash != proposal.ConfigSHA256 {
			return fmt.Errorf("immutable session anchors changed")
		}
	}
	proposal.Revision = revision + 1
	hot, err := proposal.HotSnapshot()
	if err != nil {
		return err
	}
	snapshot, err := json.Marshal(hot)
	if err != nil {
		return err
	}
	snapshotBytes = len(snapshot)
	writtenJSONBytes += len(snapshot)
	for _, staged := range []any{proposal.TranscriptPending, proposal.ExecutionPending, proposal.DiagnosticsPending, event} {
		if encoded, marshalErr := json.Marshal(staged); marshalErr == nil {
			writtenJSONBytes += len(encoded)
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE agent_sessions SET state=$2,revision=$3,last_commit_id=$4,last_commit_sha256=$5,storage_version=6,updated_at=now() WHERE session_id=$1`, proposal.ID, snapshot, proposal.Revision, *proposal.PendingCommitID, fingerprint); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE agent_commands SET session_id=$2 WHERE command_id=$1 AND (session_id IS NULL OR session_id=$2)`, c.ID, proposal.ID); err != nil {
		return err
	}
	if err = writeTranscript(ctx, tx, &proposal); err != nil {
		return err
	}
	if err = writeCallMap(ctx, tx, &proposal); err != nil {
		return err
	}
	if err = writeDiagnostics(ctx, tx, &proposal); err != nil {
		return err
	}
	if err = writeRunAndExecutions(ctx, tx, &proposal); err != nil {
		return err
	}
	if event != nil {
		b, e := json.Marshal(event)
		if e != nil {
			return e
		}
		if _, err = tx.Exec(ctx, `INSERT INTO agent_session_events(session_id,event,revision,commit_id,command_id) VALUES($1,$2,$3,$4,$5)`, proposal.ID, b, proposal.Revision, *proposal.PendingCommitID, c.ID); err != nil {
			return err
		}
	}
	if err = writeActionAttempts(ctx, tx, &proposal); err != nil {
		return err
	}
	if err = writeContext(ctx, tx, &proposal); err != nil {
		return err
	}
	if err = writeRetention(ctx, tx, &proposal); err != nil {
		return err
	}
	var ownerStill *string
	var fenceStill int64
	var unexpired bool
	if err = tx.QueryRow(ctx, `SELECT lease_owner::text,lease_fence,lease_owner IS NULL OR lease_expires_at>clock_timestamp() FROM agent_sessions WHERE session_id=$1`, proposal.ID).Scan(&ownerStill, &fenceStill, &unexpired); err != nil {
		return err
	}
	if !unexpired || fenceStill != leaseFence || !equalJSON(ownerStill, leaseOwner) {
		return ErrFencingConflict
	}
	if _, err = tx.Exec(ctx, `INSERT INTO agent_commit_receipts(session_id,commit_id,command_id,fingerprint,expected_revision,committed_revision) VALUES($1,$2,$3,$4,$5,$6)`, proposal.ID, *proposal.PendingCommitID, c.ID, fingerprint, proposal.Revision-1, proposal.Revision); err != nil {
		return err
	}
	terminal := false
	eventType := ""
	if m, ok := event.(map[string]any); ok {
		eventType, _ = m["type"].(string)
	}
	terminal = eventType == "session_created" || eventType == "user_message_appended" || eventType == "run_finished" || proposal.RetentionPending != nil || c.Route == "/admin/reconcile"
	if terminal && (proposal.RunStatus == "blocked" || (proposal.RunStatus == "interrupted" && len(proposal.PendingToolCalls) > 0)) {
		outcome := "uncertain"
		if proposal.RunStatus == "blocked" {
			outcome = "blocked"
		}
		if err = s.guardCommand(ctx, tx, c); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE agent_commands SET status=$2,owner=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE command_id=$1`, c.ID, outcome); err != nil {
			return err
		}
	} else if terminal {
		status := 200
		message := any("Run stopped.")
		if proposal.FinalAnswer != nil {
			message = *proposal.FinalAnswer
		}
		if eventType == "session_created" {
			status = 201
			message = "Session created with immutable config " + proposal.ConfigRef + "@" + proposal.ConfigSHA256[:12] + "."
		}
		if eventType == "user_message_appended" {
			message = "User input appended to session memory."
		}
		outcome := "succeeded"
		if proposal.Status == "failed" {
			outcome = "failed"
		}
		if err = s.completeCommand(ctx, tx, c, status, map[string]any{"session": proposal, "message": message}, outcome); err != nil {
			return err
		}
	}
	if !terminal {
		if err = s.guardCommand(ctx, tx, c); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	proposal.TranscriptPending, proposal.DiagnosticsPending, proposal.ExecutionPending, proposal.RetentionPending, proposal.PendingCommitID = nil, nil, nil, nil, nil
	proposal.AttemptsPending = nil
	if terminal {
		c.mu.Lock()
		c.completed = true
		c.mu.Unlock()
	}
	if m, ok := event.(map[string]any); ok {
		var id int64
		if e := s.db.QueryRow(ctx, `SELECT event_id FROM agent_session_events WHERE session_id=$1 AND commit_id=$2`, proposal.ID, p.State.PendingCommitID).Scan(&id); e == nil {
			m["event_id"] = id
		}
	}
	p.State = proposal
	return nil
}

func writeCallMap(ctx context.Context, tx pgx.Tx, state *agent.Session) error {
	for _, entry := range state.TranscriptPending {
		if entry.Kind != "message" {
			continue
		}
		rawCalls, present := entry.Payload["tool_calls"]
		if !present || rawCalls == nil {
			continue
		}
		calls, ok := rawCalls.([]any)
		if !ok {
			return fmt.Errorf("tool call payload is not an array")
		}
		for _, item := range calls {
			call, _ := item.(map[string]any)
			id, _ := call["id"].(string)
			if id == "" {
				return fmt.Errorf("tool call is missing its identity")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO agent_session_calls(session_id,call_id,message_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, state.ID, id, entry.MessageID); err != nil {
				return err
			}
			var same bool
			if err := tx.QueryRow(ctx, `SELECT COALESCE(bool_and(message_id=$3),false) FROM agent_session_calls WHERE session_id=$1 AND call_id=$2`, state.ID, id, entry.MessageID).Scan(&same); err != nil || !same {
				return fmt.Errorf("tool call identity collision")
			}
		}
	}
	return nil
}

func writeRunAndExecutions(ctx context.Context, tx pgx.Tx, state *agent.Session) error {
	stagedExecutionCalls := map[string]bool{}
	for _, item := range state.ExecutionPending {
		if callID, ok := item["call_id"].(string); ok {
			stagedExecutionCalls[callID] = true
		}
	}
	if state.RunID != nil {
		id, err := uuid.Parse(*state.RunID)
		if err != nil {
			return fmt.Errorf("invalid run identity: %w", err)
		}
		status := state.RunStatus
		if status == "" {
			status = "running"
			if state.RunEndedAt != nil {
				status = "complete"
				if state.RunTerminationReason != nil && *state.RunTerminationReason != "" {
					status = "interrupted"
				}
			}
		}
		metadata, _ := json.Marshal(map[string]any{"run_count": state.RunCount, "step_count": state.StepCount})
		var owner any
		if state.ExecutionOwner != nil {
			owner = *state.ExecutionOwner
		} else {
			owner = uuid.Nil
		}
		fence := int64(0)
		if state.ExecutionFence != nil {
			fence = *state.ExecutionFence
		}
		// A run's owner and fence identify the lease that started it. In
		// particular, resume emits a status event before assigning a new run ID;
		// that event still carries the previous run ID and must not rewrite its
		// original fencing identity with the newly acquired lease.
		if _, err = tx.Exec(ctx, `INSERT INTO agent_runs(session_id,run_id,status,owner,fence,started_at,ended_at,termination_reason,last_decision_step,metadata,command_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(session_id,run_id) DO UPDATE SET status=EXCLUDED.status,ended_at=EXCLUDED.ended_at,termination_reason=EXCLUDED.termination_reason,last_decision_step=EXCLUDED.last_decision_step,metadata=EXCLUDED.metadata`, state.ID, id, status, owner, fence, state.RunStartedAt, state.RunEndedAt, state.RunTerminationReason, state.StepCount, metadata, state.RunCommandID); err != nil {
			return err
		}
	}
	for _, item := range state.ExecutionPending {
		b, err := json.Marshal(item)
		if err != nil {
			return err
		}
		var fields map[string]any
		if err = json.Unmarshal(b, &fields); err != nil {
			return err
		}
		get := func(key string) string { v, _ := fields[key].(string); return v }
		attempts := intValue(fields["attempts"])
		if attempts < 0 {
			return fmt.Errorf("tool execution attempts cannot be negative")
		}
		if policy, ok := fields["policy"].(map[string]any); ok {
			maximum := intValue(policy["max_attempts"])
			if maximum > 0 && attempts > maximum {
				return fmt.Errorf("tool execution attempts exceed immutable policy limit")
			}
		}
		tool := get("tool")
		if tool == "" {
			tool = get("tool_name") // Accept snapshots written by pre-contract Go builds.
		}
		if err = persistToolExecution(ctx, tx, state, get("execution_id"), get("call_id"), tool, fields["arguments"], fields["policy"], get("idempotency_key"), attempts, get("status"), fields["result"], fields["started_at"], fields["finished_at"], fields); err != nil {
			return err
		}
	}
	return nil
}

func persistToolExecution(ctx context.Context, tx pgx.Tx, state *agent.Session, executionID, callID, tool string, args, policy any, key string, attempts int, status string, result, started, finished, payload any) error {
	if callID == "" {
		return fmt.Errorf("tool execution is missing its call identity")
	}
	var mapped bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_session_calls WHERE session_id=$1 AND call_id=$2)`, state.ID, callID).Scan(&mapped); err != nil {
		return err
	}
	if !mapped {
		return fmt.Errorf("tool execution references an unavailable call identity")
	}
	if executionID == "" {
		executionID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(state.ID+"/"+callID)).String()
	}
	eid, err := uuid.Parse(executionID)
	if err != nil {
		return fmt.Errorf("invalid execution identity: %w", err)
	}
	if status == "" || status == "intent" {
		status = "pending"
	}
	argJSON, err := json.Marshal(args)
	if err != nil {
		return err
	}
	if string(argJSON) == "null" {
		argJSON = []byte(`{}`)
	}
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	if string(policyJSON) == "null" {
		policyJSON = []byte(`{}`)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var metadata map[string]any
	if err = json.Unmarshal(payloadJSON, &metadata); err != nil {
		return err
	}
	resultText := any(nil)
	if result != nil {
		if text, ok := result.(string); ok {
			resultText = text
		} else {
			encoded, marshalErr := json.Marshal(result)
			if marshalErr != nil {
				return marshalErr
			}
			resultText = string(encoded)
		}
	}
	var runID any
	if state.RunID != nil {
		runID = *state.RunID
	}
	var owner any
	if state.ExecutionOwner != nil {
		owner = *state.ExecutionOwner
	} else {
		zeroOwner := uuid.Nil
		owner = zeroOwner
	}
	fence := int64(0)
	if state.ExecutionFence != nil {
		fence = *state.ExecutionFence
	}
	var existingStatus, existingID, existingTool, existingKey string
	var frozenMetadata []byte
	var existingResult *string
	var existingArgs, existingPolicy []byte
	var existingAttempts int
	err = tx.QueryRow(ctx, `SELECT status,result,attempts,execution_id::text,tool,arguments,policy,COALESCE(idempotency_key::text,''),jsonb_build_object('command_id',command_id,'namespace',namespace,'target',target,'payload_sha256',payload_sha256,'key_algorithm',key_algorithm,'run_id',run_id) FROM agent_tool_executions WHERE session_id=$1 AND call_id=$2 FOR UPDATE`, state.ID, callID).Scan(&existingStatus, &existingResult, &existingAttempts, &existingID, &existingTool, &existingArgs, &existingPolicy, &existingKey, &frozenMetadata)
	if err == nil {
		var frozen map[string]any
		if err = json.Unmarshal(frozenMetadata, &frozen); err != nil {
			return err
		}
		for _, field := range []string{"command_id", "namespace", "target", "payload_sha256", "key_algorithm", "run_id"} {
			if !equalJSON(frozen[field], metadata[field]) {
				return fmt.Errorf("frozen action %s changed", field)
			}
		}
		if existingID != executionID || existingTool != tool || existingKey != key || !sameJSONBytes(existingArgs, argJSON) || !sameJSONBytes(existingPolicy, policyJSON) || attempts < existingAttempts || attempts > existingAttempts+1 {
			return fmt.Errorf("tool execution identity, policy, or attempt count changed")
		}
		if existingStatus == "succeeded" || existingStatus == "failed" || existingStatus == "cancelled" {
			if existingStatus == status && equalJSON(existingResult, resultText) && attempts == existingAttempts {
				return nil
			}
			return fmt.Errorf("immutable terminal tool outcome changed")
		}
		_, err = tx.Exec(ctx, `UPDATE agent_tool_executions SET status=$3,result=$4,finished_at=$5,payload=$6,attempts=$7,started_at=COALESCE(started_at,$8),owner=$9,fence=$10,updated_at=clock_timestamp() WHERE session_id=$1 AND call_id=$2`, state.ID, callID, status, resultText, finished, payloadJSON, attempts, started, owner, fence)
		return err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if key == "" {
		return fmt.Errorf("frozen schema-6 action key required")
	}
	_, err = tx.Exec(ctx, `INSERT INTO agent_tool_executions(session_id,execution_id,run_id,call_id,tool,arguments,policy,idempotency_key,attempts,status,result,owner,fence,started_at,finished_at,payload,command_id,namespace,target,payload_sha256,key_algorithm) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,'agent-action-v1')`, state.ID, eid, runID, callID, tool, argJSON, policyJSON, key, attempts, status, resultText, owner, fence, started, finished, payloadJSON, metadata["command_id"], metadata["namespace"], metadata["target"], metadata["payload_sha256"])
	return err
}

func sameJSONBytes(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	x, _ := json.Marshal(left)
	y, _ := json.Marshal(right)
	return bytes.Equal(x, y)
}

func writeRetention(ctx context.Context, tx pgx.Tx, state *agent.Session) error {
	if state.RetentionPending == nil {
		return nil
	}
	b, err := json.Marshal(state.RetentionPending)
	if err != nil {
		return err
	}
	id := state.RetentionPending["archive_id"]
	archiveID, ok := id.(string)
	if !ok || archiveID == "" {
		archiveID = uuid.NewString()
	}
	parsed, err := uuid.Parse(archiveID)
	if err != nil {
		return fmt.Errorf("invalid archive receipt identity: %w", err)
	}
	checksum, _ := state.RetentionPending["sha256"].(string)
	if checksum == "" {
		checksum, _ = state.RetentionPending["checksum"].(string)
	}
	receiptRevision := state.Revision + 1 // compatibility for legacy receipt-only outboxes
	if prune, _ := state.RetentionPending["prune"].(bool); prune {
		item := state.RetentionPending
		receiptRevision = int64(intValue(item["snapshot_revision"]))
		keepDiagnostics, keepEvents := intValue(item["keep_diagnostics"]), intValue(item["keep_events"])
		if state.ExecutionOwner == nil || state.ExecutionFence == nil || !state.ExecutionMaintenance || state.Status == "active" || state.RunStatus == "running" || len(state.PendingToolCalls) != 0 || keepDiagnostics < 32 || keepEvents < 1 || int64(intValue(item["snapshot_revision"])) != state.Revision-1 {
			return fmt.Errorf("retention requires a terminal owned session and matching exported revision")
		}
		var unresolved, running int64
		if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_tool_executions WHERE session_id=$1 AND status IN ('pending','started','uncertain','blocked')),(SELECT count(*) FROM agent_runs WHERE session_id=$1 AND status='running')`, state.ID).Scan(&unresolved, &running); err != nil {
			return err
		}
		if unresolved != 0 || running != 0 {
			return fmt.Errorf("unresolved execution or running lifecycle prevents pruning")
		}
		// Check archive telemetry against the locked session snapshot before any
		// mutation. Deletes and the receipt below share this save transaction.
		wm, ok := item["watermarks"].(map[string]any)
		if !ok {
			return fmt.Errorf("archive watermarks missing")
		}
		diagWM, ok := wm["diagnostics"].(map[string]int64)
		if !ok {
			raw, _ := json.Marshal(wm["diagnostics"])
			_ = json.Unmarshal(raw, &diagWM)
		}
		for _, table := range []string{"agent_session_events", "agent_session_diagnostics"} {
			hash := sha256.New()
			var rows pgx.Rows
			if table == "agent_session_events" {
				rows, err = tx.Query(ctx, `SELECT to_jsonb(t) FROM agent_session_events t WHERE session_id=$1 AND event_id<=$2 ORDER BY event_id`, state.ID, intValue(wm["events"]))
			} else {
				rows, err = tx.Query(ctx, `SELECT to_jsonb(t) FROM agent_session_diagnostics t WHERE session_id=$1 AND ordinal<=COALESCE(($2::jsonb->>family)::bigint,0) ORDER BY family,ordinal`, state.ID, mustJSON(diagWM))
			}
			if err != nil {
				return err
			}
			for rows.Next() {
				var raw []byte
				if err = rows.Scan(&raw); err != nil {
					rows.Close()
					return err
				}
				var row any
				if err = decodeArchiveJSON(raw, &row); err != nil {
					rows.Close()
					return err
				}
				line, e := archiveLine(map[string]any{"table": table, "row": row})
				if e != nil {
					rows.Close()
					return e
				}
				_, _ = hash.Write(line)
			}
			if err = rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
			tableHashes, _ := item["table_sha256"].(map[string]string)
			if tableHashes == nil {
				raw, _ := json.Marshal(item["table_sha256"])
				_ = json.Unmarshal(raw, &tableHashes)
			}
			if shaHex(hash) != tableHashes[table] {
				return fmt.Errorf("export does not match current database telemetry; nothing pruned")
			}
		}
	}
	result, err := tx.Exec(ctx, `INSERT INTO agent_archive_receipts(archive_id,session_id,snapshot_revision,sha256,manifest) VALUES($1,$2,$3,$4,$5) ON CONFLICT(archive_id) DO NOTHING`, parsed, state.ID, receiptRevision, checksum, b)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		if prune, _ := state.RetentionPending["prune"].(bool); prune {
			item := state.RetentionPending
			wm := item["watermarks"].(map[string]any)
			diagWM := map[string]int64{}
			raw, _ := json.Marshal(wm["diagnostics"])
			_ = json.Unmarshal(raw, &diagWM)
			for _, family := range []string{"compaction_events", "compaction_attempts", "compaction_decisions"} {
				cutoff := state.DiagnosticCounts[family] - int64(intValue(item["keep_diagnostics"]))
				if cutoff < 0 {
					cutoff = 0
				}
				if cutoff > diagWM[family] {
					cutoff = diagWM[family]
				}
				if _, err = tx.Exec(ctx, `DELETE FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2 AND ordinal<=$3`, state.ID, family, cutoff); err != nil {
					return err
				}
			}
			// The archive receipt event was inserted earlier in this same save.
			// Keep it as one of the configured event tail, so retain N-1 older rows.
			keepPriorEvents := intValue(item["keep_events"]) - 1
			if keepPriorEvents < 0 {
				keepPriorEvents = 0
			}
			if _, err = tx.Exec(ctx, `DELETE FROM agent_session_events WHERE session_id=$1 AND event_id<=$2 AND event_id NOT IN (SELECT event_id FROM agent_session_events WHERE session_id=$1 AND event_id<=$2 ORDER BY event_id DESC LIMIT $3)`, state.ID, intValue(wm["events"]), keepPriorEvents); err != nil {
				return err
			}
			state.DiagnosticHistoryComplete = false
		}
		return nil
	}
	var same bool
	err = tx.QueryRow(ctx, `SELECT session_id=$2 AND snapshot_revision=$3 AND sha256=$4 AND manifest=$5::jsonb FROM agent_archive_receipts WHERE archive_id=$1`, parsed, state.ID, receiptRevision, checksum, b).Scan(&same)
	if err != nil || !same {
		return fmt.Errorf("archive receipt identity collision")
	}
	return nil
}

func writeTranscript(ctx context.Context, tx pgx.Tx, state *agent.Session) error {
	var next int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(sequence),0)+1 FROM agent_session_transcript WHERE session_id=$1`, state.ID).Scan(&next); err != nil {
		return err
	}
	for _, entry := range state.TranscriptPending {
		if entry.Sequence > next || entry.Sequence < 1 {
			return fmt.Errorf("transcript sequence gap")
		}
		if entry.Kind == "compaction_summary" || entry.Kind == "compaction_rejected" {
			if err := validateSummary(ctx, tx, state, entry); err != nil {
				return err
			}
		}
		b, err := json.Marshal(entry.Payload)
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `INSERT INTO agent_session_transcript(session_id,sequence,message_id,kind,payload,run_id,decision_step,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`, state.ID, entry.Sequence, entry.MessageID, entry.Kind, b, entry.RunID, entry.DecisionStep, entry.CreatedAt)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 1 {
			if entry.Sequence != next {
				return fmt.Errorf("out of order transcript record")
			}
			next++
		} else {
			var same bool
			err = tx.QueryRow(ctx, `SELECT sequence=$3 AND kind=$4 AND payload=$5::jsonb AND run_id IS NOT DISTINCT FROM $6::uuid AND decision_step=$7 AND created_at=$8 FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2`, state.ID, entry.MessageID, entry.Sequence, entry.Kind, b, entry.RunID, entry.DecisionStep, entry.CreatedAt).Scan(&same)
			if err != nil || !same {
				return fmt.Errorf("changed immutable transcript identity or sequence collision")
			}
		}
	}
	if next != state.NextTranscriptSequence {
		return fmt.Errorf("transcript next sequence differs from archive: %d != %d", state.NextTranscriptSequence, next)
	}
	return nil
}
func validateSummary(ctx context.Context, tx pgx.Tx, state *agent.Session, entry agent.TranscriptEntry) error {
	var info struct {
		Source   []string `json:"source_message_ids"`
		Retained []string `json:"retained_message_ids"`
		Previous *string  `json:"previous_ledger_id"`
		Hash     string   `json:"source_sha256"`
	}
	raw, err := json.Marshal(entry.Payload)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &info); err != nil {
		return err
	}
	if len(info.Source) == 0 {
		return fmt.Errorf("summary requires source identities")
	}
	var previousID, previousContent *string
	err = tx.QueryRow(ctx, `SELECT message_id::text,payload->>'content' FROM agent_session_transcript WHERE session_id=$1 AND sequence<$2 AND kind IN ('compaction_summary','legacy_summary') ORDER BY sequence DESC LIMIT 1`, state.ID, entry.Sequence).Scan(&previousID, &previousContent)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if !equalJSON(previousID, info.Previous) {
		return fmt.Errorf("inconsistent previous ledger reference")
	}
	messages := []agent.Message{}
	seen := map[string]bool{}
	last := int64(0)
	for _, id := range append(append([]string{}, info.Source...), info.Retained...) {
		if seen[id] {
			return fmt.Errorf("duplicate summary source or retained identity")
		}
		seen[id] = true
		var payload []byte
		var sequence int64
		if err = tx.QueryRow(ctx, `SELECT sequence,payload FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2 AND kind='message' AND sequence<$3`, state.ID, id, entry.Sequence).Scan(&sequence, &payload); err != nil {
			return fmt.Errorf("unavailable summary message: %w", err)
		}
		if sequence <= last {
			return fmt.Errorf("summary sources not in transcript order")
		}
		last = sequence
		var m agent.Message
		if err = json.Unmarshal(payload, &m); err != nil {
			return err
		}
		if len(messages) < len(info.Source) {
			messages = append(messages, m)
		}
	}
	hash, err := agent.SourceFingerprint(state.Goal, messages, previousContent)
	if err != nil {
		return err
	}
	if hash != info.Hash {
		return fmt.Errorf("summary source fingerprint mismatch")
	}
	return nil
}
func writeContext(ctx context.Context, tx pgx.Tx, state *agent.Session) error {
	if len(state.Memory) == 0 || state.Memory[0].Role != "user" || state.Memory[0].Content == nil || *state.Memory[0].Content != state.Goal || state.Memory[0].Sequence == nil || *state.Memory[0].Sequence != 1 {
		return fmt.Errorf("active context lost immutable goal")
	}
	seen := map[string]bool{}
	last := int64(0)
	for _, m := range state.Memory {
		if seen[m.MessageID] || m.Sequence == nil || *m.Sequence <= last {
			return fmt.Errorf("duplicate or unordered active context identity")
		}
		seen[m.MessageID] = true
		last = *m.Sequence
		payload, err := json.Marshal(m.WirePayload())
		if err != nil {
			return err
		}
		var same bool
		err = tx.QueryRow(ctx, `SELECT kind='message' AND sequence=$3 AND payload=$4::jsonb AND created_at=$5 FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2`, state.ID, m.MessageID, *m.Sequence, payload, m.CreatedAt).Scan(&same)
		if err != nil || !same {
			return fmt.Errorf("active message missing or changed")
		}
	}
	if state.ActiveLedgerID != nil {
		var kind string
		var payload []byte
		if err := tx.QueryRow(ctx, `SELECT kind,payload FROM agent_session_transcript WHERE session_id=$1 AND message_id=$2`, state.ID, *state.ActiveLedgerID).Scan(&kind, &payload); err != nil {
			return err
		}
		var ledger struct {
			Content *string       `json:"content"`
			Ledger  *agent.Ledger `json:"ledger"`
		}
		if err := json.Unmarshal(payload, &ledger); err != nil {
			return err
		}
		if (kind != "compaction_summary" && kind != "legacy_summary") || !equalJSON(ledger.Content, state.CompactedContext) || (kind == "compaction_summary" && !equalJSON(ledger.Ledger, state.CompactionLedger)) {
			return fmt.Errorf("active ledger content or typed state changed")
		}
	} else if state.CompactedContext != nil || state.CompactionLedger != nil {
		return fmt.Errorf("compacted context requires archived ledger identity")
	}
	projection := state.ContextProjection()
	ids, _ := json.Marshal(projection.RawMessageIDs)
	ledger, _ := json.Marshal(projection.LedgerState)
	_, err := tx.Exec(ctx, `INSERT INTO agent_session_context(session_id,revision,goal_message_id,ledger_record_id,raw_message_ids,ledger_state) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(session_id) DO UPDATE SET revision=EXCLUDED.revision,goal_message_id=EXCLUDED.goal_message_id,ledger_record_id=EXCLUDED.ledger_record_id,raw_message_ids=EXCLUDED.raw_message_ids,ledger_state=EXCLUDED.ledger_state`, state.ID, state.Revision, projection.GoalMessageID, projection.LedgerRecordID, ids, ledger)
	return err
}
func equalJSON(a, b any) bool {
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	if err != nil {
		return false
	}
	var left, right any
	dx := json.NewDecoder(bytes.NewReader(x))
	dx.UseNumber()
	dy := json.NewDecoder(bytes.NewReader(y))
	dy.UseNumber()
	if dx.Decode(&left) != nil || dy.Decode(&right) != nil {
		return false
	}
	return bytes.Equal(mustJSON(left), mustJSON(right))
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
func writeDiagnostics(ctx context.Context, tx pgx.Tx, state *agent.Session) error {
	for _, d := range state.DiagnosticsPending {
		if d.Family != "compaction_attempts" && d.Family != "compaction_events" && d.Family != "compaction_decisions" {
			return fmt.Errorf("unknown diagnostic family")
		}
		b, err := json.Marshal(d.Payload)
		if err != nil {
			return err
		}
		var next int64
		if err = tx.QueryRow(ctx, `SELECT COALESCE(max(ordinal),0)+1 FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2`, state.ID, d.Family).Scan(&next); err != nil {
			return err
		}
		if d.Ordinal > next {
			return fmt.Errorf("diagnostic ordinal gap")
		}
		result, err := tx.Exec(ctx, `INSERT INTO agent_session_diagnostics(session_id,family,ordinal,payload) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, state.ID, d.Family, d.Ordinal, b)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 0 {
			var same bool
			if err = tx.QueryRow(ctx, `SELECT payload=$4::jsonb FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2 AND ordinal=$3`, state.ID, d.Family, d.Ordinal, b).Scan(&same); err != nil || !same {
				return fmt.Errorf("immutable diagnostic collision")
			}
		}
	}
	for family, count := range state.DiagnosticCounts {
		var stored int64
		if err := tx.QueryRow(ctx, `SELECT COALESCE(max(ordinal),0) FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2`, state.ID, family).Scan(&stored); err != nil {
			return err
		}
		if stored != count {
			return fmt.Errorf("diagnostic lifetime counter mismatch")
		}
	}
	return nil
}

// One MVCC statement hydrates each state, context, active message and ledger revision.
const hydrateHistory = `SELECT s.state,s.config,s.config_ref,s.config_sha256,s.revision,c.revision,
 (SELECT jsonb_agg(t.payload || jsonb_build_object('message_id',t.message_id::text,'sequence',t.sequence,'created_at',t.created_at) ORDER BY ids.ord)
  FROM jsonb_array_elements_text(jsonb_build_array(c.goal_message_id::text)||c.raw_message_ids) WITH ORDINALITY ids(id,ord)
  JOIN agent_session_transcript t ON t.session_id=s.session_id AND t.message_id=ids.id::uuid AND t.kind='message'),
 l.payload,c.ledger_state,c.goal_message_id::text,c.raw_message_ids,c.ledger_record_id::text,l.kind,
 (SELECT COALESCE(jsonb_agg(jsonb_build_object('family',d.family,'ordinal',d.ordinal,'payload',d.payload) ORDER BY d.family,d.ordinal),'[]'::jsonb)
 FROM (VALUES('compaction_events'),('compaction_attempts'),('compaction_decisions')) f(family)
 CROSS JOIN LATERAL (SELECT family,ordinal,payload FROM agent_session_diagnostics WHERE session_id=s.session_id AND family=f.family ORDER BY ordinal DESC LIMIT 32) d)
 FROM agent_sessions s LEFT JOIN agent_session_context c USING(session_id)
 LEFT JOIN agent_session_transcript l ON l.session_id=s.session_id AND l.message_id=c.ledger_record_id WHERE s.session_id=$1`

func (s *server) fetch(ctx context.Context, id string, forExecution ...bool) (persisted, error) {
	started := time.Now()
	var p persisted
	loadRunExecutions := len(forExecution) > 0 && forExecution[0]
	parsed, parseErr := uuid.Parse(id)
	if parseErr != nil {
		return p, parseErr
	}
	id = parsed.String()
	var raw, messages, ledger, typed, diagnostics, rawIDs []byte
	var goalID, ledgerID, ledgerKind, configRef, configSHA256 *string
	var revision int64
	var contextRevision *int64
	databaseDuration := time.Duration(0)
	queryStarted := time.Now()
	err := s.db.QueryRow(ctx, hydrateHistory, id).Scan(&raw, &p.Config, &configRef, &configSHA256, &revision, &contextRevision, &messages, &ledger, &typed, &goalID, &rawIDs, &ledgerID, &ledgerKind, &diagnostics)
	databaseDuration += time.Since(queryStarted)
	if err != nil {
		return p, err
	}
	if err = json.Unmarshal(raw, &p.State); err != nil {
		return p, err
	}
	if p.State.ID != id {
		return p, fmt.Errorf("snapshot identity differs from session row")
	}
	if (configRef != nil && p.State.ConfigRef != *configRef) || (configSHA256 != nil && p.State.ConfigSHA256 != *configSHA256) {
		return p, fmt.Errorf("snapshot configuration anchors differ from session row")
	}
	if contextRevision == nil {
		return p, fmt.Errorf("active session context is missing; automatic legacy import is disabled")
	}
	if p.State.Revision != revision || *contextRevision != revision {
		return p, fmt.Errorf("state/context revision mismatch")
	}
	if err = json.Unmarshal(messages, &p.State.Memory); err != nil {
		return p, err
	}
	for i := range p.State.Memory {
		p.State.Memory[i].CreatedAt = p.State.Memory[i].CreatedAt.UTC()
	}
	// created_at and the other immutable message fields are hydrated from the
	// transcript table. Compare them with the independently persisted hot
	// snapshot anchor before trusting this projection; comparing only against
	// the just-hydrated Message would bless a timestamp tampered in that table.
	var snapshot map[string]json.RawMessage
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		return p, err
	}
	if encodedIdentity, ok := snapshot["_active_message_identity_sha256"]; ok {
		var expectedIdentity string
		if err = json.Unmarshal(encodedIdentity, &expectedIdentity); err != nil {
			return p, fmt.Errorf("invalid active message identity anchor: %w", err)
		}
		actualIdentity, identityErr := agent.ActiveMessageIdentity(p.State.Memory)
		if identityErr != nil {
			return p, identityErr
		}
		if expectedIdentity == "" || expectedIdentity != actualIdentity {
			return p, fmt.Errorf("active transcript message identity differs from snapshot anchor")
		}
	}
	if encodedIdentity, ok := snapshot["_session_integrity_sha256"]; ok {
		var expectedIdentity string
		if err = json.Unmarshal(encodedIdentity, &expectedIdentity); err != nil {
			return p, fmt.Errorf("invalid session integrity anchor: %w", err)
		}
		actualIdentity, identityErr := agent.SessionIntegrityIdentity(p.State)
		if identityErr != nil {
			return p, identityErr
		}
		if expectedIdentity == "" || expectedIdentity != actualIdentity {
			return p, fmt.Errorf("snapshot session anchors differ from integrity anchor")
		}
	}
	if len(ledger) > 0 {
		var value struct {
			Content *string `json:"content"`
		}
		if err = json.Unmarshal(ledger, &value); err != nil {
			return p, err
		}
		p.State.CompactedContext = value.Content
	}
	if len(typed) > 0 && string(typed) != "null" {
		if err = json.Unmarshal(typed, &p.State.CompactionLedger); err != nil {
			return p, err
		}
	}
	var records []agent.DiagnosticRecord
	if err = json.Unmarshal(diagnostics, &records); err != nil {
		return p, err
	}
	p.State.CompactionEvents = []string{}
	p.State.CompactionAttempts = []map[string]any{}
	p.State.CompactionDecisions = []map[string]any{}
	for _, d := range records {
		switch d.Family {
		case "compaction_events":
			value, ok := d.Payload.(string)
			if !ok {
				return p, fmt.Errorf("invalid compaction event")
			}
			p.State.CompactionEvents = append(p.State.CompactionEvents, value)
		case "compaction_attempts", "compaction_decisions":
			value, ok := d.Payload.(map[string]any)
			if !ok {
				return p, fmt.Errorf("invalid compaction diagnostic")
			}
			if d.Family == "compaction_attempts" {
				p.State.CompactionAttempts = append(p.State.CompactionAttempts, value)
			} else {
				p.State.CompactionDecisions = append(p.State.CompactionDecisions, value)
			}
		}
	}
	agent.NormalizeSession(&p.State)
	var expectedIDs []string
	if err = json.Unmarshal(rawIDs, &expectedIDs); err != nil {
		return p, err
	}
	if goalID == nil || len(p.State.Memory) != len(expectedIDs)+1 || p.State.Memory[0].MessageID != *goalID || !equalJSON(ledgerID, p.State.ActiveLedgerID) {
		return p, fmt.Errorf("active context references disagree")
	}
	seen := map[string]bool{}
	last := int64(0)
	for i, m := range p.State.Memory {
		if seen[m.MessageID] || m.Sequence == nil || *m.Sequence <= last || *m.Sequence >= p.State.NextTranscriptSequence || m.CreatedAt.IsZero() {
			return p, fmt.Errorf("invalid active message identity or sequence")
		}
		seen[m.MessageID] = true
		last = *m.Sequence
		if i > 0 && m.MessageID != expectedIDs[i-1] {
			return p, fmt.Errorf("active message order changed")
		}
	}
	if p.State.ActiveLedgerID == nil && (p.State.CompactedContext != nil || p.State.CompactionLedger != nil) {
		return p, fmt.Errorf("ledger has no immutable record")
	}
	if p.State.ActiveLedgerID != nil && (ledgerKind == nil || (*ledgerKind != "compaction_summary" && *ledgerKind != "legacy_summary")) {
		return p, fmt.Errorf("active ledger references wrong record kind")
	}
	if len(ledger) > 0 {
		var archived struct {
			Ledger *agent.Ledger `json:"ledger"`
		}
		if err = json.Unmarshal(ledger, &archived); err != nil {
			return p, err
		}
		if !equalJSON(archived.Ledger, p.State.CompactionLedger) {
			return p, fmt.Errorf("typed ledger differs from immutable record")
		}
	}
	if len(p.State.Memory) == 0 || p.State.Memory[0].Sequence == nil || *p.State.Memory[0].Sequence != 1 || p.State.Memory[0].Content == nil || *p.State.Memory[0].Content != p.State.Goal {
		return p, fmt.Errorf("invalid immutable goal projection")
	}
	var config agent.Config
	if err = json.Unmarshal(p.Config, &config); err != nil {
		return p, err
	}
	identity, err := agent.ConfigIdentity(config)
	if err != nil || identity != p.State.ResolvedConfigSHA256 {
		return p, fmt.Errorf("resolved config identity mismatch")
	}
	if loadRunExecutions {
		queryStarted = time.Now()
		if err = s.loadExecutions(ctx, &p.State); err != nil {
			return p, err
		}
		databaseDuration += time.Since(queryStarted)
	}
	s.recordHydrationSample(started, databaseDuration, raw, p.Config, messages, ledger, typed, diagnostics, rawIDs, p.State)
	return p, nil
}

func (s *server) recordHydrationSample(started time.Time, databaseDuration time.Duration, raw, config, messages, ledger, typed, diagnostics, rawIDs []byte, state agent.Session) {
	executionBytes, _ := json.Marshal(state.Executions)
	selectedBytes := len(raw) + len(config) + len(messages) + len(ledger) + len(typed) + len(diagnostics) + len(rawIDs) + len(executionBytes)
	s.recordIOSample(map[string]any{"operation": "get", "session_id": state.ID, "wall_ms": float64(time.Since(started).Microseconds()) / 1000,
		"database_roundtrip_ms": float64(databaseDuration.Microseconds()) / 1000, "snapshot_json_bytes": len(raw),
		"selected_read_json_bytes": selectedBytes, "active_messages_loaded": len(state.Memory), "diagnostic_rows_loaded": len(state.CompactionEvents) + len(state.CompactionAttempts) + len(state.CompactionDecisions)})
}

func (s *server) loadExecutions(ctx context.Context, state *agent.Session) error {
	callIDs := activeToolCallIDs(*state)
	state.Executions = []agent.ToolExecution{}
	if len(callIDs) == 0 {
		return nil
	}
	rows, err := s.db.Query(ctx, `SELECT execution_id::text,COALESCE(run_id::text,''),call_id,tool,arguments,policy,COALESCE(idempotency_key::text,''),attempts,COALESCE(owner::text,''),fence,status,result,started_at,finished_at,command_id::text,namespace,target,payload_sha256,key_algorithm,(SELECT COALESCE(max(attempt_number),0) FROM agent_action_attempts a WHERE a.execution_id=agent_tool_executions.execution_id) FROM agent_tool_executions WHERE session_id=$1 AND call_id=ANY($2::text[]) ORDER BY execution_id`, state.ID, callIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var execution agent.ToolExecution
		var args, policy, result []byte
		if err = rows.Scan(&execution.ExecutionID, &execution.RunID, &execution.CallID, &execution.Tool, &args, &policy, &execution.IdempotencyKey, &execution.Attempts, &execution.OwnerID, &execution.Fence, &execution.Status, &result, &execution.StartedAt, &execution.FinishedAt, &execution.CommandID, &execution.Namespace, &execution.Target, &execution.PayloadSHA256, &execution.KeyAlgorithm, &execution.AttemptNumber); err != nil {
			return err
		}
		if err = json.Unmarshal(args, &execution.Arguments); err != nil {
			return err
		}
		if err = json.Unmarshal(policy, &execution.Policy); err != nil {
			return err
		}
		if len(result) > 0 {
			execution.Result = string(result)
		}
		state.Executions = append(state.Executions, execution)
	}
	return rows.Err()
}

func activeToolCallIDs(state agent.Session) []string {
	seen := map[string]bool{}
	ids := []string{}
	add := func(value any) {
		encoded, _ := json.Marshal(value)
		var calls []map[string]any
		if json.Unmarshal(encoded, &calls) != nil {
			return
		}
		for _, call := range calls {
			id, _ := call["id"].(string)
			if id == "" {
				if function, ok := call["function"].(map[string]any); ok {
					id, _ = function["call_id"].(string)
				}
			}
			if id != "" && !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	for _, message := range state.Memory {
		add(message.ToolCalls)
	}
	for _, call := range state.PendingToolCalls {
		if id, ok := call["id"].(string); ok && id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

func intValue(value any) int {
	switch n := value.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		v, _ := strconv.Atoi(string(n))
		return v
	default:
		return 0
	}
}

func pageInt(r *http.Request, key string, fallback, min, max int64) (int64, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < min || v > max {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}
	return v, nil
}

func parseSessionID(value string) (string, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func uuidCursor(r *http.Request, key string) (*string, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return nil, nil
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be a UUID", key)
	}
	value := id.String()
	return &value, nil
}

func (s *server) transcript(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("session_id")
	if _, err := uuid.Parse(id); err != nil {
		fail(w, 422, "session_id must be a UUID")
		return
	}
	after, err := pageInt(r, "after_sequence", 0, 0, 1<<62)
	if err != nil {
		fail(w, 422, err.Error())
		return
	}
	limit, err := pageInt(r, "limit", 100, 1, 1000)
	if err != nil {
		fail(w, 422, err.Error())
		return
	}
	p, err := s.fetch(r.Context(), id)
	if err != nil {
		fail(w, 404, "Session not found")
		return
	}
	through, err := pageInt(r, "through_sequence", p.State.NextTranscriptSequence-1, 0, 1<<62)
	if err != nil {
		fail(w, 422, err.Error())
		return
	}
	if through >= p.State.NextTranscriptSequence {
		through = p.State.NextTranscriptSequence - 1
	}
	rows, err := s.db.Query(r.Context(), `SELECT sequence,message_id::text,kind,payload,run_id::text,decision_step,created_at,recorded_at FROM agent_session_transcript WHERE session_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence LIMIT $4`, id, after, through, limit)
	if err != nil {
		fail(w, 500, err.Error())
		return
	}
	defer rows.Close()
	entries := []map[string]any{}
	next := after
	for rows.Next() {
		var entry agent.TranscriptEntry
		var payload []byte
		var recorded time.Time
		if err = rows.Scan(&entry.Sequence, &entry.MessageID, &entry.Kind, &payload, &entry.RunID, &entry.DecisionStep, &entry.CreatedAt, &recorded); err != nil {
			fail(w, 500, err.Error())
			return
		}
		if err = json.Unmarshal(payload, &entry.Payload); err != nil {
			fail(w, 500, err.Error())
			return
		}
		b, _ := json.Marshal(entry)
		var item map[string]any
		_ = json.Unmarshal(b, &item)
		item["created_at"] = entry.CreatedAt.UTC()
		item["recorded_at"] = recorded.UTC()
		entries = append(entries, item)
		next = entry.Sequence
	}
	if err = rows.Err(); err != nil {
		fail(w, 500, err.Error())
		return
	}
	reply(w, 200, map[string]any{"session_id": id, "snapshot_revision": p.State.Revision, "history_complete": p.State.HistoryComplete, "through_sequence": through, "entries": entries, "next_after_sequence": next})
}
