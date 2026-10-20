package agent

import (
	"context"
	"errors"
	"testing"
)

type du07Decisions struct {
	calls []Decision
	i     int
}

func (d *du07Decisions) Decide(context.Context, []Message) (Decision, error) {
	if d.i >= len(d.calls) {
		return Decision{Final: true, Content: "done"}, nil
	}
	x := d.calls[d.i]
	d.i++
	return x, nil
}
func (*du07Decisions) Compact(context.Context, string, []Message, *string, bool) (string, error) {
	return "", errors.New("unexpected compaction")
}

func du07Config(mode string, attempts int) Config {
	return Config{SystemPrompt: "system", Memory: Memory{MaxSteps: 8, MaxTokens: 6000, ContextWindowTokens: 100000, ReservedOutputTokens: 512, RawTurnsToKeep: 4}, Execution: Execution{ToolPolicy: "auto", MaxConsecutiveInvalidResponses: 3}, Tools: []ToolDefinition{{Name: "lookup", Parameters: map[string]any{"type": "object"}, Recovery: &ToolRecovery{Mode: mode, MaxAttempts: attempts}}}}
}

func TestDU07C04OnlyExplicitTransientErrorsRetryWithinStoredBound(t *testing.T) {
	cfg := du07Config("read_only", 2)
	client := &du07Decisions{calls: []Decision{{ID: "retry-call", Name: "lookup", Arguments: map[string]any{}}}}
	calls := 0
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		calls++
		if calls == 1 {
			return "", TransientToolError{Message: "temporary"}
		}
		return "ok", nil
	}}}
	s := e.Create("du07-retry", "goal", "du07.yaml", "hash")
	if err := e.Run(context.Background(), &s, 4, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(s.Executions) != 1 || s.Executions[0].Attempts != 2 || s.Executions[0].Status != "succeeded" {
		t.Fatalf("calls=%d executions=%+v", calls, s.Executions)
	}

	calls = 0
	client = &du07Decisions{calls: []Decision{{ID: "string-error-call", Name: "lookup", Arguments: map[string]any{}}}}
	e = Engine{Config: du07Config("repeatable", 4), Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		calls++
		return "ERROR: ordinary tool output", nil
	}}}
	s = e.Create("du07-error-string", "goal", "du07.yaml", "hash")
	if err := e.Run(context.Background(), &s, 4, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(s.Executions) != 1 || s.Executions[0].Status != "failed" {
		t.Fatalf("ordinary error string was retried: calls=%d executions=%+v", calls, s.Executions)
	}
}

func TestDU07C04IdempotentAdapterUsesPersistedKey(t *testing.T) {
	remove := RegisterIdempotentTool("lookup", func(_ context.Context, _ map[string]any, key string) (string, error) {
		if key == "" {
			return "", errors.New("missing idempotency key")
		}
		return "applied", nil
	})
	defer remove()
	e := Engine{Config: du07Config("idempotent", 2), Client: &du07Decisions{calls: []Decision{{ID: "idempotent-call", Name: "lookup", Arguments: map[string]any{}}}}, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		return "ordinary adapter must not run", nil
	}}}
	s := e.Create("du07-idempotent", "goal", "du07.yaml", "hash")
	if err := e.Run(context.Background(), &s, 4, nil); err != nil {
		t.Fatal(err)
	}
	if len(s.Executions) != 1 || s.Executions[0].Status != "succeeded" || s.Executions[0].IdempotencyKey == "" {
		t.Fatalf("idempotent execution=%+v", s.Executions)
	}
}

func TestDU07C04MissingIdempotentAdapterBlocksBeforeInvocation(t *testing.T) {
	removeID, removeReconciler := RegisterIdempotentTool("lookup", nil), RegisterToolReconciler("lookup", nil)
	defer removeID()
	defer removeReconciler()
	calls := 0
	e := Engine{Config: du07Config("idempotent", 2), Client: &du07Decisions{calls: []Decision{{ID: "missing-adapter-call", Name: "lookup", Arguments: map[string]any{}}}}, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		calls++
		return "must not execute", nil
	}}}
	s := e.Create("du07-no-idempotent-adapter", "goal", "du07.yaml", "hash")
	if err := e.Run(context.Background(), &s, 4, nil); err == nil || calls != 0 || s.RunStatus != "blocked" || len(s.Executions) != 1 || s.Executions[0].Status != "uncertain" {
		t.Fatalf("missing adapter did not block: err=%v calls=%d run=%s executions=%+v", err, calls, s.RunStatus, s.Executions)
	}
}

func TestDU07C04UnresolvedEffectBlocksAndConfirmedReconcileDoesNotReplay(t *testing.T) {
	cfg := du07Config("reconcile", 3)
	makeSession := func(id string) Session {
		e := Engine{Config: cfg}
		s := e.Create(id, "goal", "du07.yaml", "hash")
		s.PendingToolCalls = []map[string]any{{"id": "recover-call", "name": "lookup", "arguments": map[string]any{}}}
		s.Executions = []ToolExecution{{ExecutionID: "exec-id", RunID: "old-run", CallID: "recover-call", Tool: "lookup", Arguments: map[string]any{}, Policy: map[string]any{"mode": "reconcile", "max_attempts": 3}, IdempotencyKey: "key-id", Attempts: 1, Status: "started"}}
		return s
	}
	invocations := 0
	e := Engine{Config: cfg, Client: &du07Decisions{}, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		invocations++
		return "unsafe replay", nil
	}}}
	s := makeSession("du07-unresolved")
	if err := e.Run(context.Background(), &s, 4, nil); err == nil || s.RunStatus != "blocked" || invocations != 0 || s.PendingToolCalls == nil || len(s.PendingToolCalls) != 1 {
		t.Fatalf("unresolved effect was not blocked: err=%v run_status=%s invocations=%d pending=%v", err, s.RunStatus, invocations, s.PendingToolCalls)
	}

	remove := RegisterToolReconciler("lookup", func(_ context.Context, _ map[string]any, key string) (string, bool, error) {
		if key != "key-id" {
			return "", false, errors.New("wrong persisted key")
		}
		return "confirmed", true, nil
	})
	defer remove()
	s = makeSession("du07-confirmed")
	if err := e.Run(context.Background(), &s, 4, nil); err != nil {
		t.Fatal(err)
	}
	if invocations != 0 || len(s.Executions) != 1 || s.Executions[0].Status != "succeeded" || s.Executions[0].Result != "confirmed" || len(s.PendingToolCalls) != 0 {
		t.Fatalf("confirmed result was replayed or lost: calls=%d execution=%+v pending=%v", invocations, s.Executions, s.PendingToolCalls)
	}
}

func TestDU08C02CancellationSettlesInFlightToolThenStopsScheduling(t *testing.T) {
	cfg := du07Config("read_only", 1)
	client := &du07Decisions{calls: []Decision{{ID: "cancel-call", Name: "lookup", Arguments: map[string]any{}}}}
	invocations, checkpoints, persistedResult := 0, 0, false
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) {
		invocations++
		return "settled-result", nil
	}}}
	s := e.Create("du08-cancel", "goal", "du08.yaml", "hash")
	ctx := WithExecutionCheckpoint(context.Background(), func() error {
		checkpoints++
		if checkpoints == 4 {
			return ErrRunCancelled
		}
		return nil
	})
	err := e.Run(ctx, &s, 8, func(event map[string]any) error {
		if event["step"] == "tool_completed" {
			persistedResult = len(s.Executions) == 1 && s.Executions[0].Result == "settled-result"
		}
		return nil
	})
	if !errors.Is(err, ErrRunCancelled) || invocations != 1 || client.i != 1 || !persistedResult {
		t.Fatalf("cancel did not settle exactly one invocation: err=%v invocations=%d decisions=%d persisted=%v", err, invocations, client.i, persistedResult)
	}
	if s.RunStatus != "cancelled" || s.Status != "failed" || s.LastError["code"] != "run_cancelled" {
		t.Fatalf("cancel lifecycle=%s task=%s last_error=%v", s.RunStatus, s.Status, s.LastError)
	}
}
