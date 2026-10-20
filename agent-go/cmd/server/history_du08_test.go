package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/state-driven-agent/agent-go/internal/agent"
	"sync/atomic"
)

func TestDU08C02CancelTargetsCommittedRunAndPersistsAcknowledgement(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	owner := uuid.NewString()
	fence, err := s.acquire(ctx, p.State.ID, owner, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release(p.State.ID, owner, fence)
	runID, started := uuid.NewString(), time.Now().UTC()
	p.State.RunID, p.State.RunStartedAt, p.State.RunStatus = &runID, &started, "running"
	p.State.ExecutionOwner, p.State.ExecutionFence = &owner, &fence
	if err = s.save(ctx, &p, map[string]any{"type": "run_started"}); err != nil {
		t.Fatal(err)
	}
	cancelRequest := httptest.NewRequest(http.MethodPost, "/sessions/"+p.State.ID+"/cancel", nil)
	cancelRequest.SetPathValue("session_id", p.State.ID)
	cancelResponse := httptest.NewRecorder()
	s.cancel(cancelResponse, cancelRequest)
	var cancelBody struct {
		SessionID          string   `json:"session_id"`
		CancelRequestedFor []string `json:"cancel_requested_for"`
	}
	if err = json.Unmarshal(cancelResponse.Body.Bytes(), &cancelBody); err != nil || cancelResponse.Code != http.StatusOK || cancelBody.SessionID != p.State.ID || len(cancelBody.CancelRequestedFor) != 1 || cancelBody.CancelRequestedFor[0] != runID {
		t.Fatalf("cancel response=%d %s err=%v", cancelResponse.Code, cancelResponse.Body.String(), err)
	}
	if err = s.checkRun(ctx, &p.State); !errors.Is(err, agent.ErrRunCancelled) {
		t.Fatalf("checkpoint cancellation=%v", err)
	}
	ended := time.Now().UTC()
	p.State.RunEndedAt, p.State.RunTerminationReason = &ended, stringPointer("cancel_requested")
	p.State.Status, p.State.RunStatus = "failed", "cancelled"
	p.State.FinalAnswer = stringPointer("Run cancelled; use /resume to recover committed work.")
	p.State.LastError = map[string]any{"code": "run_cancelled", "message": *p.State.FinalAnswer, "recoverable": true}
	if err = s.save(ctx, &p, map[string]any{"type": "run_finished", "status": "cancelled"}); err != nil {
		t.Fatal(err)
	}
	var status string
	var requested bool
	if err = s.db.QueryRow(ctx, `SELECT status,cancel_requested FROM agent_runs WHERE run_id=$1`, runID).Scan(&status, &requested); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" || !requested {
		t.Fatalf("durable cancel status=%s requested=%v", status, requested)
	}
}

type du08ContinuationClient struct{ calls atomic.Int32 }

func (c *du08ContinuationClient) Decide(context.Context, []agent.Message) (agent.Decision, error) {
	if c.calls.Add(1) == 1 {
		return agent.Decision{Final: true, Content: "CURL_OK"}, nil
	}
	return agent.Decision{Final: true, Content: "CURL_NEXT"}, nil
}
func (*du08ContinuationClient) Compact(context.Context, string, []agent.Message, *string, bool) (string, error) {
	return "", fmt.Errorf("unexpected compaction")
}

func TestDU08C01ResumeStartsNewRunAndPreservesImmutableAnchors(t *testing.T) {
	s, ctx := du05Repository(t)
	cfg := du05Config()
	cfg.Memory.RunMaxSteps = du05Int(5)
	config, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := persisted{State: agent.NewSession(uuid.NewString(), "Reply with CURL_OK.", "du08-continuation.yaml", "config-hash"), Config: config}
	if err = s.save(ctx, &p, map[string]any{"type": "session_created"}); err != nil {
		t.Fatal(err)
	}
	goal, configHash := p.State.Goal, p.State.ConfigSHA256
	s.client = &du08ContinuationClient{}
	owner1 := uuid.NewString()
	fence1, err := s.acquire(ctx, p.State.ID, owner1, 10)
	if err != nil {
		t.Fatal(err)
	}
	p.State.ExecutionOwner, p.State.ExecutionFence = &owner1, &fence1
	emit := func(event any) error { return s.save(ctx, &p, event) }
	if err = s.execute(ctx, &p, 5, emit); err != nil {
		t.Fatal(err)
	}
	firstRun, firstCount := *p.State.RunID, p.State.RunCount
	s.release(p.State.ID, owner1, fence1)
	owner2 := uuid.NewString()
	fence2, err := s.acquire(ctx, p.State.ID, owner2, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release(p.State.ID, owner2, fence2)
	p.State.ExecutionOwner, p.State.ExecutionFence = &owner2, &fence2
	content := "Now reply with exactly CURL_NEXT."
	agent.AppendMessage(&p.State, agent.Message{Role: "user", Content: &content})
	p.State.Status, p.State.FinalAnswer, p.State.LastError = "active", nil, nil
	if err = s.execute(ctx, &p, 5, emit); err != nil {
		t.Fatal(err)
	}
	if p.State.FinalAnswer == nil || *p.State.FinalAnswer != "CURL_NEXT" || p.State.RunID == nil || *p.State.RunID == firstRun || p.State.RunCount != firstCount+1 || p.State.Goal != goal || p.State.ConfigSHA256 != configHash || fence2 <= fence1 {
		t.Fatalf("continuation state=%+v fences=%d->%d", p.State, fence1, fence2)
	}
}

type du08DisconnectClient struct{ decisions atomic.Int32 }

func (c *du08DisconnectClient) Decide(context.Context, []agent.Message) (agent.Decision, error) {
	if c.decisions.Add(1) == 1 {
		return agent.Decision{Calls: []agent.ToolCall{{ID: "du08-held-call", Name: "lookup", Arguments: map[string]any{}}}}, nil
	}
	return agent.Decision{Final: true, Content: "DU08_RESUMED"}, nil
}
func (*du08DisconnectClient) Compact(context.Context, string, []agent.Message, *string, bool) (string, error) {
	return "", fmt.Errorf("unexpected compaction")
}

func TestDU08C03DisconnectSettlesToolAndResumeKeepsTranscriptPrefix(t *testing.T) {
	s, ctx := du05Repository(t)
	cfg := du05Config()
	cfg.Memory.RunMaxSteps = du05Int(4)
	cfg.Tools[0].Recovery = &agent.ToolRecovery{Mode: "read_only", MaxAttempts: 1}
	config, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := persisted{State: agent.NewSession(uuid.NewString(), "disconnect goal", "du08-controlled.yaml", "source-hash"), Config: config}
	if err = s.save(ctx, &p, map[string]any{"type": "session_created"}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var startedOnce atomic.Bool
	s.client = &du08DisconnectClient{}
	s.tools = map[string]agent.Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		if startedOnce.CompareAndSwap(false, true) {
			close(started)
		}
		<-release // Cancellation must not interrupt the in-flight adapter.
		return "settled-after-disconnect", nil
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sessions/{session_id}/run/stream", s.stream)
	mux.HandleFunc("POST /sessions/{session_id}/resume", s.resume)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(requestCtx, http.MethodPost, httpServer.URL+"/sessions/"+p.State.ID+"/run/stream", bytes.NewBufferString(`{"max_steps":4}`))
	request.Header.Set("Content-Type", "application/json")
	response := make(chan error, 1)
	go func() {
		res, requestErr := http.DefaultClient.Do(request)
		if res != nil {
			res.Body.Close()
		}
		response <- requestErr
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancelRequest()
		t.Fatal("tool invocation did not start")
	}
	before, err := s.fetch(ctx, p.State.ID)
	if err != nil || before.State.RunID == nil {
		cancelRequest()
		t.Fatalf("running state=%+v err=%v", before.State, err)
	}
	firstRun := *before.State.RunID
	var firstFence int64
	if err = s.db.QueryRow(ctx, `SELECT fence FROM agent_runs WHERE run_id=$1`, firstRun).Scan(&firstFence); err != nil {
		cancelRequest()
		t.Fatal(err)
	}
	cancelRequest() // Real HTTP client disconnect while the tool is blocked.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var requested bool
		_ = s.db.QueryRow(ctx, `SELECT COALESCE(bool_or(cancel_requested),false) FROM agent_runs WHERE run_id=$1`, firstRun).Scan(&requested)
		if requested {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var requested bool
	if err = s.db.QueryRow(ctx, `SELECT cancel_requested FROM agent_runs WHERE run_id=$1`, firstRun).Scan(&requested); err != nil || !requested {
		close(release)
		t.Fatalf("disconnect cancellation was not recorded: requested=%v err=%v", requested, err)
	}
	close(release)
	select {
	case <-response:
	case <-time.After(5 * time.Second):
		t.Fatal("disconnected stream handler did not wait for settled work")
	}
	var cancelled persisted
	for time.Now().Before(deadline.Add(5 * time.Second)) {
		cancelled, err = s.fetch(ctx, p.State.ID, true)
		if err == nil && cancelled.State.RunStatus == "cancelled" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || cancelled.State.RunStatus != "cancelled" || len(cancelled.State.Executions) != 1 || cancelled.State.Executions[0].Result != "settled-after-disconnect" {
		t.Fatalf("settled cancellation state=%+v err=%v", cancelled.State, err)
	}
	for time.Now().Before(deadline.Add(10 * time.Second)) {
		var released bool
		_ = s.db.QueryRow(ctx, `SELECT lease_owner IS NULL FROM agent_sessions WHERE session_id=$1`, p.State.ID).Scan(&released)
		if released {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	var transcriptPrefixBeforeResume []byte
	transcriptPrefixBeforeResume = du05Rows(t, s, ctx, p.State.ID)
	resumeReq := httptest.NewRequest(http.MethodPost, "/sessions/"+p.State.ID+"/resume", bytes.NewBufferString(`{"max_steps":4}`))
	resumeReq.Header.Set("Content-Type", "application/json")
	resumeRes := httptest.NewRecorder()
	mux.ServeHTTP(resumeRes, resumeReq)
	if resumeRes.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", resumeRes.Code, resumeRes.Body.String())
	}
	after, err := s.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State.RunStatus != "complete" || after.State.Status != "complete" || after.State.RunID == nil || *after.State.RunID == firstRun || after.State.RunCount != 2 {
		t.Fatalf("resumed lifecycle=%+v", after.State)
	}
	var resumedFence int64
	if err = s.db.QueryRow(ctx, `SELECT fence FROM agent_runs WHERE run_id=$1`, *after.State.RunID).Scan(&resumedFence); err != nil || resumedFence <= firstFence {
		t.Fatalf("resumed fence=%d prior=%d err=%v", resumedFence, firstFence, err)
	}
	du05Prefix(t, transcriptPrefixBeforeResume, du05Rows(t, s, ctx, p.State.ID))
}

func TestDU08C04StartupReconcilesAbandonedRunWithoutExecutingTools(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	owner := uuid.NewString()
	fence, err := s.acquire(ctx, p.State.ID, owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	runID, started := uuid.NewString(), time.Now().UTC()
	p.State.RunID, p.State.RunStartedAt, p.State.RunStatus = &runID, &started, "running"
	p.State.ExecutionOwner, p.State.ExecutionFence = &owner, &fence
	if err = s.save(ctx, &p, map[string]any{"type": "run_started"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	invocations := 0
	s.tools = map[string]agent.Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		invocations++
		return "must not run at startup", nil
	}}
	reconciled, err := s.reconcileAbandoned(ctx, 10)
	if err != nil || len(reconciled) != 1 || reconciled[0]["run_id"] != runID || invocations != 0 {
		t.Fatalf("reconcile=%v err=%v tool_invocations=%d", reconciled, err, invocations)
	}
	loaded, err := s.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.RunStatus != "interrupted" || loaded.State.Status != "failed" || loaded.State.LastError["code"] != "run_abandoned" {
		t.Fatalf("abandoned lifecycle run=%s task=%s last_error=%v", loaded.State.RunStatus, loaded.State.Status, loaded.State.LastError)
	}
	var storedStatus string
	if err = s.db.QueryRow(ctx, `SELECT status FROM agent_runs WHERE run_id=$1`, runID).Scan(&storedStatus); err != nil || storedStatus != "interrupted" {
		t.Fatalf("run row status=%s err=%v", storedStatus, err)
	}
}

type du08FinalClient struct{}

func (du08FinalClient) Decide(context.Context, []agent.Message) (agent.Decision, error) {
	return agent.Decision{Final: true, Content: "DU08_FINAL"}, nil
}
func (du08FinalClient) Compact(context.Context, string, []agent.Message, *string, bool) (string, error) {
	return "", fmt.Errorf("unexpected compaction")
}

func TestDU08C05TerminalCommitPrecedesFinalEvent(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	owner := uuid.NewString()
	fence, err := s.acquire(ctx, p.State.ID, owner, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release(p.State.ID, owner, fence)
	p.State.ExecutionOwner, p.State.ExecutionFence = &owner, &fence
	s.client = du08FinalClient{}
	s.tools = map[string]agent.Tool{}
	faulted := false
	s.saveFault = func(stage string) error {
		if stage == "terminal_commit" && !faulted {
			faulted = true
			return fmt.Errorf("injected terminal commit failure")
		}
		return nil
	}
	var events []map[string]any
	err = s.execute(ctx, &p, 2, func(v any) error {
		if event, ok := v.(map[string]any); ok {
			if saveErr := s.save(ctx, &p, event); saveErr != nil {
				return saveErr
			}
			events = append(events, event)
		}
		return nil
	})
	if err == nil || !faulted {
		t.Fatalf("expected terminal commit failure: err=%v faulted=%v", err, faulted)
	}
	for _, event := range events {
		if event["type"] == "final_result" {
			t.Fatal("successful final event escaped before terminal state commit")
		}
	}
	loaded, loadErr := s.fetch(ctx, p.State.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if loaded.State.Status == "complete" || loaded.State.FinalAnswer != nil && *loaded.State.FinalAnswer == "DU08_FINAL" {
		t.Fatalf("failed terminal commit exposed completion: %+v", loaded.State)
	}
}
