package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

type replay struct{ n int }

func (r *replay) Decide(_ context.Context, _ []Message) (Decision, error) {
	steps := []Decision{
		{ID: "1", Name: "audit", Arguments: map[string]any{"parser": "naive"}}, {ID: "2", Name: "audit", Arguments: map[string]any{"parser": "robust"}},
		{ID: "3", Name: "median", Arguments: map[string]any{"ok": true}}, {ID: "4", Name: "report", Arguments: map[string]any{"ok": true}}, {ID: "5", Name: "validate", Arguments: map[string]any{"ok": true}},
	}
	if r.n >= len(steps) {
		return Decision{Final: true, Content: "Phase 3\n\n| City | Valid headcount |"}, nil
	}
	d := steps[r.n]
	r.n++
	return d, nil
}
func (r *replay) Compact(_ context.Context, goal string, _ []Message, _ *string, _ bool) (string, error) {
	return "<COMPACTED_STATE>\n<USER_GOAL>\n" + goal + "\n</USER_GOAL>\n<GLOBAL_LESSON_LEDGER>\n- currency NULL semicolon UNKNOWN case-insensitive\n</GLOBAL_LESSON_LEDGER>\n</COMPACTED_STATE>", nil
}
func TestWorkflowCompactsAndCanResume(t *testing.T) {
	p1, p2, p3 := "one", "two", "three"
	cfg := Config{SystemPrompt: "x", Memory: Memory{MaxTokens: 1, MaxSteps: 20, RawTurnsToKeep: 1, HybridReflection: true}, Workflow: &WorkflowConfig{EntryPhase: p1, Phases: []Phase{
		{ID: p1, AllowedTools: []string{"audit"}, Completion: []Evidence{{Tool: "audit", Required: true, Arguments: map[string]any{"parser": "robust"}, Result: map[string]any{"phase": "one"}}}, Transitions: []Edge{{To: p2, When: "phase_complete"}}},
		{ID: p2, AllowedTools: []string{"median"}, Completion: []Evidence{{Tool: "median", Required: true, Arguments: map[string]any{"ok": true}, Result: map[string]any{"phase": "two"}}}, Transitions: []Edge{{To: p3, When: "phase_complete"}}},
		{ID: p3, AllowedTools: []string{"report", "validate"}, Completion: []Evidence{{Tool: "report", Required: true, Result: map[string]any{"phase": "three"}}, {Tool: "validate", Required: true, Result: map[string]any{"valid": true}}}, Transitions: []Edge{{To: "complete", When: "phase_complete"}}},
	}}}
	tools := map[string]Tool{"audit": func(_ context.Context, a map[string]any) (string, error) {
		if a["parser"] == "robust" {
			return `{"phase":"one"}`, nil
		}
		return fmt.Sprintf("ERROR currency %0100d", 1), nil
	}, "median": func(context.Context, map[string]any) (string, error) { return `{"phase":"two"}`, nil }, "report": func(context.Context, map[string]any) (string, error) { return `{"phase":"three"}`, nil }, "validate": func(context.Context, map[string]any) (string, error) { return `{"valid":true}`, nil }}
	r := &replay{}
	e := Engine{Config: cfg, Client: r, Tools: tools}
	s := e.Create("id", "goal", "case.yaml", "hash")
	if err := e.Run(context.Background(), &s, 3, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "failed" || s.Workflow.ActivePhase == nil || *s.Workflow.ActivePhase != p2 {
		t.Fatalf("first bounded run did not retain phase 2: %#v", s)
	}
	raw, _ := json.Marshal(s)
	var restored Session
	if json.Unmarshal(raw, &restored) != nil {
		t.Fatal("state must be portable JSON")
	}
	restored.Status = "active"
	restored.FinalAnswer = nil
	if err := e.Run(context.Background(), &restored, 10, nil); err != nil {
		t.Fatal(err)
	}
	if restored.Status != "complete" || restored.Workflow.ActivePhase != nil || len(restored.CompactionEvents) < 1 {
		t.Fatalf("resume did not complete durable workflow: %#v", restored)
	}
}
