package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

func TestDU07C01DatabaseLeaseRaceExpiryAndStaleOwner(t *testing.T) {
	s, ctx := du05Repository(t)
	// Simulate an unversioned Go test schema. DU-07 performs one clean reset,
	// then startup recreates the shared canonical tables. Repeated startup is safe.
	legacy := du05Create(t, s, ctx)
	if _, err := s.db.Exec(ctx, `UPDATE agent_go_schema_control SET version=0 WHERE component='agent-go'`); err != nil {
		t.Fatal(err)
	}
	if err := s.schema(); err != nil {
		t.Fatalf("reset and recreate canonical schema: %v", err)
	}
	var preserved bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_sessions WHERE session_id=$1)`, legacy.State.ID).Scan(&preserved); err != nil || preserved {
		t.Fatalf("legacy test rows were not cleared: preserved=%v err=%v", preserved, err)
	}
	for _, column := range []string{"tool", "arguments", "policy", "idempotency_key", "attempts", "owner", "fence", "started_at", "finished_at", "payload"} {
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='agent_tool_executions' AND column_name=$1)`, column).Scan(&exists); err != nil || !exists {
			t.Fatalf("DU-07 execution column %q missing after migration: %v", column, err)
		}
	}
	for _, obsolete := range []string{"tool_name", "attempt_count", "owner_id"} {
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='agent_tool_executions' AND column_name=$1)`, obsolete).Scan(&exists); err != nil || exists {
			t.Fatalf("obsolete execution column %q remains: %v", obsolete, err)
		}
	}
	p := du05Create(t, s, ctx)
	owners := []string{uuid.NewString(), uuid.NewString()}
	type claim struct {
		owner string
		fence int64
		err   error
	}
	claims := make(chan claim, 2)
	var wg sync.WaitGroup
	for _, owner := range owners {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			fence, err := s.acquire(ctx, p.State.ID, owner, 1)
			claims <- claim{owner: owner, fence: fence, err: err}
		}(owner)
	}
	wg.Wait()
	close(claims)
	var winner, loser string
	var firstFence int64
	wins := 0
	for c := range claims {
		if c.err == nil {
			winner, firstFence = c.owner, c.fence
			wins++
		} else if errors.Is(c.err, ErrOwnershipConflict) {
			loser = c.owner
		} else {
			t.Fatalf("claim error: %v", c.err)
		}
	}
	if wins != 1 || winner == "" || loser == "" {
		t.Fatalf("claim winners=%d winner=%s loser=%s", wins, winner, loser)
	}
	time.Sleep(1100 * time.Millisecond)
	secondFence, err := s.acquire(ctx, p.State.ID, loser, 5)
	if err != nil || secondFence <= firstFence {
		t.Fatalf("takeover fence=%d previous=%d err=%v", secondFence, firstFence, err)
	}
	if err = s.renew(ctx, p.State.ID, winner, firstFence, 5); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale renew=%v", err)
	}
	s.release(p.State.ID, winner, firstFence)
	var storedOwner string
	var storedFence int64
	if err = s.db.QueryRow(ctx, `SELECT lease_owner::text,lease_fence FROM agent_sessions WHERE session_id=$1`, p.State.ID).Scan(&storedOwner, &storedFence); err != nil {
		t.Fatal(err)
	}
	if storedOwner != loser || storedFence != secondFence {
		t.Fatalf("stale release changed successor lease: owner=%s fence=%d", storedOwner, storedFence)
	}
	p.State.ExecutionOwner, p.State.ExecutionFence = &winner, &firstFence
	content := "stale write"
	agent.AppendMessage(&p.State, agent.Message{Role: "assistant", Content: &content})
	if err = s.save(ctx, &p, nil); !errors.Is(err, ErrFencingConflict) {
		t.Fatalf("stale save=%v", err)
	}
	t.Logf("DU07 C01 one acquisition winner; takeover fence %d -> %d; old renew/release/save fenced", firstFence, secondFence)
}

func TestDU07C02IntentStartAndOutcomeCommitWithToolMessage(t *testing.T) {
	s, ctx := du05Repository(t)
	cfg := du05Config()
	cfg.Tools[0].Recovery = &agent.ToolRecovery{Mode: "read_only", MaxAttempts: 2}
	configBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := persisted{State: agent.NewSession(uuid.NewString(), "goal", "du07-controlled.yaml", "source-hash"), Config: configBytes}
	if err = s.save(ctx, &p, map[string]any{"type": "session_created"}); err != nil {
		t.Fatal(err)
	}
	owner := uuid.NewString()
	fence, err := s.acquire(ctx, p.State.ID, owner, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release(p.State.ID, owner, fence)
	p.State.ExecutionOwner, p.State.ExecutionFence = &owner, &fence
	engine := agent.Engine{Config: cfg, Client: &du07Script{}, Tools: map[string]agent.Tool{"lookup": func(context.Context, map[string]any) (string, error) { return "known-result", nil }}}
	err = engine.Run(ctx, &p.State, 4, func(event map[string]any) error { return s.save(ctx, &p, event) })
	if err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	var result string
	if err = s.db.QueryRow(ctx, `SELECT status,attempts,result FROM agent_tool_executions WHERE session_id=$1 AND call_id='du07-lookup-call'`, p.State.ID).Scan(&status, &attempts, &result); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" || attempts != 1 || result != "known-result" {
		t.Fatalf("execution status=%s attempts=%d result=%s", status, attempts, result)
	}
	var milestones int
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_session_transcript WHERE session_id=$1 AND kind IN ('tool_intent','tool_attempt','tool_outcome')`, p.State.ID).Scan(&milestones); err != nil {
		t.Fatal(err)
	}
	var correlated int
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_session_transcript WHERE session_id=$1 AND kind='message' AND payload->>'role'='tool' AND payload->>'tool_call_id'='du07-lookup-call' AND payload->>'content'='known-result'`, p.State.ID).Scan(&correlated); err != nil {
		t.Fatal(err)
	}
	if milestones < 3 || correlated != 1 {
		t.Fatalf("intent/start/outcome records=%d correlated result=%d", milestones, correlated)
	}
	t.Logf("DU07 C02 durable intent/start/outcome; attempts=%d correlated result=%d", attempts, correlated)
}

func TestDU07C03HeartbeatRenewsAndFencesOnSuccessor(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	s.leaseSeconds, s.renewInterval = 2, 250*time.Millisecond
	owned, runCtx, cleanup, err := s.ownSession(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if err = runCtx.Err(); err != nil {
		cleanup()
		t.Fatalf("heartbeat did not preserve ownership: %v", err)
	}
	other := uuid.NewString()
	if _, err = s.acquire(ctx, p.State.ID, other, 2); !errors.Is(err, ErrOwnershipConflict) {
		cleanup()
		t.Fatalf("second worker acquired renewed lease: %v", err)
	}
	var nextFence int64
	otherUUID, _ := uuid.Parse(other)
	if err = s.db.QueryRow(ctx, `UPDATE agent_sessions SET lease_owner=$2,lease_fence=lease_fence+1,lease_expires_at=clock_timestamp()+interval '2 seconds' WHERE session_id=$1 RETURNING lease_fence`, p.State.ID, otherUUID).Scan(&nextFence); err != nil {
		cleanup()
		t.Fatal(err)
	}
	select {
	case <-runCtx.Done():
	case <-time.After(2 * time.Second):
		cleanup()
		t.Fatal("old worker continued after heartbeat fencing")
	}
	if err = s.save(ctx, &owned, nil); !errors.Is(err, ErrFencingConflict) {
		cleanup()
		t.Fatalf("fenced save=%v", err)
	}
	cleanup()
	t.Logf("DU07 C03 heartbeat renewed beyond lease period; successor fence=%d stopped old owner", nextFence)
}

type du07Script struct{ calls int }

func (c *du07Script) Decide(context.Context, []agent.Message) (agent.Decision, error) {
	c.calls++
	if c.calls == 1 {
		return agent.Decision{Calls: []agent.ToolCall{{ID: "du07-lookup-call", Name: "lookup", Arguments: map[string]any{}}}}, nil
	}
	return agent.Decision{Final: true, Content: "done"}, nil
}
func (*du07Script) Compact(context.Context, string, []agent.Message, *string, bool) (string, error) {
	return "", fmt.Errorf("unexpected compaction")
}
