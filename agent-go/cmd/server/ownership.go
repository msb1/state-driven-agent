package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

const (
	defaultLeaseSeconds = 30
	leaseRenewInterval  = 10 * time.Second
)

var ErrOwnershipConflict = errors.New("session is owned by another worker or lease is no longer valid")
var ErrLeaseLost = errors.New("session lease lost; stale worker must stop")
var ErrSessionNotFound = errors.New("session not found")
var ErrInvalidSessionID = errors.New("session id must be a UUID")

func stringPointer(value string) *string { return &value }

func (s *server) acquire(ctx context.Context, sessionID, ownerID string, seconds int) (int64, error) {
	if seconds <= 0 || seconds > 3600 {
		return 0, fmt.Errorf("lease duration must be in (0,3600] seconds")
	}
	owner, err := uuid.Parse(ownerID)
	if err != nil {
		return 0, fmt.Errorf("invalid owner identity: %w", err)
	}
	var fence int64
	err = s.db.QueryRow(ctx, `UPDATE agent_sessions SET lease_owner=$2,lease_fence=lease_fence+1,
		lease_expires_at=clock_timestamp()+($3 * interval '1 second')
		WHERE session_id=$1 AND (lease_owner IS NULL OR lease_expires_at<=clock_timestamp())
		RETURNING lease_fence`, sessionID, owner, seconds).Scan(&fence)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrOwnershipConflict
		}
		return 0, err
	}
	return fence, nil
}

func (s *server) renew(ctx context.Context, sessionID, ownerID string, fence int64, seconds int) error {
	if s.renewFaultHook != nil {
		if err := s.renewFaultHook(); err != nil {
			return err
		}
	}
	owner, err := uuid.Parse(ownerID)
	if err != nil {
		return ErrLeaseLost
	}
	result, err := s.db.Exec(ctx, `UPDATE agent_sessions SET lease_expires_at=clock_timestamp()+($4 * interval '1 second')
		WHERE session_id=$1 AND lease_owner=$2 AND lease_fence=$3 AND lease_expires_at>clock_timestamp()`, sessionID, owner, fence, seconds)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *server) checkRun(ctx context.Context, state *agent.Session) error {
	if state.ExecutionOwner == nil || state.ExecutionFence == nil || state.RunID == nil {
		return ErrLeaseLost
	}
	owner, err := uuid.Parse(*state.ExecutionOwner)
	if err != nil {
		return ErrLeaseLost
	}
	var owned, cancelled bool
	err = s.db.QueryRow(ctx, `SELECT lease_owner=$2 AND lease_fence=$3 AND lease_expires_at>clock_timestamp(),
		EXISTS(SELECT 1 FROM agent_runs WHERE run_id=$4 AND session_id=$1 AND cancel_requested)
		FROM agent_sessions WHERE session_id=$1`, state.ID, owner, *state.ExecutionFence, *state.RunID).Scan(&owned, &cancelled)
	if err != nil || !owned {
		return ErrLeaseLost
	}
	if cancelled {
		return agent.ErrRunCancelled
	}
	return nil
}

// cancelRun serializes against the session row lock used by run start/finish,
// so a successful response identifies a committed running run.
func (s *server) cancelRun(ctx context.Context, sessionID string) ([]string, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT true FROM agent_sessions WHERE session_id=$1 FOR UPDATE`, sessionID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSessionNotFound
		}
		return nil, err
	}
	rows, err := tx.Query(ctx, `UPDATE agent_runs SET cancel_requested=true,
		cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp())
		WHERE session_id=$1 AND status='running' RETURNING run_id::text`, sessionID)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}

// reconcileAbandoned marks expired running lifecycles without invoking tools
// or models. Explicit /resume remains the only path that recovers pending work.
func (s *server) reconcileAbandoned(ctx context.Context, limit int) ([]map[string]string, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("abandoned run limit must be between 1 and 1000")
	}
	rows, err := s.db.Query(ctx, `SELECT r.session_id::text,r.run_id::text FROM agent_runs r
		JOIN agent_sessions s USING(session_id) WHERE r.status='running'
		AND (s.lease_owner IS NULL OR s.lease_expires_at<=clock_timestamp())
		ORDER BY r.started_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	type abandoned struct{ sessionID, runID string }
	candidates := []abandoned{}
	for rows.Next() {
		var candidate abandoned
		if err = rows.Scan(&candidate.sessionID, &candidate.runID); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	reconciled := []map[string]string{}
	for _, candidate := range candidates {
		p, _, cleanup, ownErr := s.ownSession(ctx, candidate.sessionID)
		if errors.Is(ownErr, ErrOwnershipConflict) || errors.Is(ownErr, ErrSessionNotFound) {
			continue
		}
		if ownErr != nil {
			return reconciled, ownErr
		}
		if p.State.RunID == nil || *p.State.RunID != candidate.runID || p.State.RunStatus != "running" {
			cleanup()
			continue
		}
		ended := time.Now().UTC().Truncate(time.Microsecond)
		p.State.RunEndedAt = &ended
		if p.State.Status == "complete" {
			p.State.RunStatus, p.State.RunTerminationReason = "complete", stringPointer("terminal_snapshot_recovered")
		} else {
			p.State.Status, p.State.RunStatus = "failed", "interrupted"
			p.State.FinalAnswer = stringPointer("Abandoned run detected; use /resume to reconcile pending work.")
			p.State.LastError = map[string]any{"code": "run_abandoned", "message": *p.State.FinalAnswer, "recoverable": true}
			p.State.RunTerminationReason = stringPointer("worker_abandoned")
		}
		event := map[string]any{"type": "run_interrupted", "run_id": candidate.runID,
			"status": p.State.RunStatus, "reason": *p.State.RunTerminationReason}
		if err = s.save(ctx, &p, event); err != nil {
			cleanup()
			return reconciled, err
		}
		cleanup()
		reconciled = append(reconciled, map[string]string{"session_id": candidate.sessionID, "run_id": candidate.runID})
	}
	return reconciled, nil
}

func (s *server) release(sessionID, ownerID string, fence int64) {
	owner, err := uuid.Parse(ownerID)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.db.Exec(ctx, `UPDATE agent_sessions SET lease_owner=NULL,lease_expires_at=NULL
		WHERE session_id=$1 AND lease_owner=$2 AND lease_fence=$3`, sessionID, owner, fence)
}

// ownSession claims database authority before hydrating state. A process-local
// mutex is deliberately not part of the ownership decision.
func (s *server) ownSession(parent context.Context, sessionID string) (persisted, context.Context, func(), error) {
	parsed, err := uuid.Parse(sessionID)
	if err != nil {
		return persisted{}, nil, nil, ErrInvalidSessionID
	}
	sessionID = parsed.String()
	var exists bool
	if err = s.db.QueryRow(parent, `SELECT EXISTS(SELECT 1 FROM agent_sessions WHERE session_id=$1)`, sessionID).Scan(&exists); err != nil {
		return persisted{}, nil, nil, err
	}
	if !exists {
		return persisted{}, nil, nil, ErrSessionNotFound
	}
	ownerID := uuid.NewString()
	leaseSeconds := s.leaseSeconds
	if leaseSeconds == 0 {
		leaseSeconds = defaultLeaseSeconds
	}
	renewInterval := s.renewInterval
	if renewInterval == 0 {
		renewInterval = leaseRenewInterval
	}
	fence, err := s.acquire(parent, sessionID, ownerID, leaseSeconds)
	if err != nil {
		return persisted{}, nil, nil, err
	}
	p, err := s.fetch(parent, sessionID, true)
	if err != nil {
		s.release(sessionID, ownerID, fence)
		return persisted{}, nil, nil, err
	}
	p.State.ExecutionOwner = &ownerID
	p.State.ExecutionFence = &fence
	runCtx, cancel := context.WithCancel(parent)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-runCtx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(context.Background(), 5*time.Second)
				renewErr := s.renew(renewCtx, sessionID, ownerID, fence, leaseSeconds)
				renewCancel()
				if renewErr != nil {
					cancel()
					return
				}
			}
		}
	}()
	cleanup := func() {
		close(stop)
		cancel()
		<-done
		s.release(sessionID, ownerID, fence)
		p.State.ExecutionOwner, p.State.ExecutionFence = nil, nil
	}
	return p, runCtx, cleanup, nil
}
