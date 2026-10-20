package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDU04InteractionGroupsRejectBrokenCorrelation(t *testing.T) {
	a := Message{Role: "assistant", ToolCalls: []any{map[string]any{"id": "a"}, map[string]any{"id": "b"}}}
	tool := func(id string) Message { return Message{Role: "tool", ToolCallID: &id} }
	groups, err := interactionGroups([]Message{a, tool("b"), tool("a")})
	if err != nil || len(groups) != 1 || len(groups[0].messages) != 3 {
		t.Fatalf("groups=%v err=%v", groups, err)
	}
	for name, history := range map[string][]Message{
		"orphan":             {tool("a")},
		"missing":            {a, tool("a")},
		"duplicate result":   {a, tool("a"), tool("a")},
		"duplicate call IDs": {{Role: "assistant", ToolCalls: []any{map[string]any{"id": "x"}, map[string]any{"id": "x"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := interactionGroups(history); err == nil {
				t.Fatal("expected context integrity error")
			}
		})
	}
}

func TestDU04BudgetUsesCompactUTF8WirePayload(t *testing.T) {
	payload := map[string]any{"messages": wireMessages([]Message{{Role: "user", Content: str("é")}}), "max_tokens": 12}
	b, err := compactJSON(payload)
	if err != nil {
		t.Fatal(err)
	}
	memory := Memory{ContextWindowTokens: len(b) + 32 + 12 + 5, ReservedOutputTokens: 8, SafetyMarginTokens: 5}
	got, err := estimatePayload(payload, memory)
	if err != nil {
		t.Fatal(err)
	}
	if got.EstimatedInputTokens != len(b)+32 || got.ReservedOutputTokens != 12 || !got.Fits {
		t.Fatalf("unexpected budget: %+v bytes=%d", got, len(b))
	}
	if string(b) != `{"max_tokens":12,"messages":[{"role":"user","content":"é"}]}` {
		t.Fatalf("compact wire JSON changed: %s", b)
	}
}

func TestDU04LedgerRejectsClaimsAndPreservesStrictShape(t *testing.T) {
	goal := "Find the answer"
	source := "verified count 83; answer is 12.5"
	valid := "<COMPACTED_STATE><USER_GOAL>Find the answer</USER_GOAL><GLOBAL_LESSON_LEDGER>count 83</GLOBAL_LESSON_LEDGER><DEAD_ENDS>- None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>answer 12.50</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>"
	ledger, canonical, err := validateLedger(valid, goal, source, 4096, true)
	if err != nil || ledger.SourceSHA256 == "" || canonical == "" {
		t.Fatalf("valid ledger rejected: ledger=%+v err=%v", ledger, err)
	}
	for _, bad := range []string{
		strings.Replace(valid, "answer 12.50", "answer 99", 1),
		strings.Replace(valid, "</USER_GOAL>", "<extra>x</extra></USER_GOAL>", 1),
		strings.Replace(valid, "<DEAD_ENDS>", "<DEAD_ENDS bad=\"1\">", 1),
	} {
		if _, _, err := validateLedger(bad, goal, source, 4096, true); err == nil {
			t.Fatalf("accepted invalid ledger: %s", bad)
		}
	}
}

type du04Client struct {
	proposal  string
	decisions int
	summaries int
}

func (c *du04Client) Decide(context.Context, []Message) (Decision, error) {
	c.decisions++
	return Decision{Final: true, Content: "done"}, nil
}
func (c *du04Client) Compact(_ context.Context, goal string, _ []Message, _ *string, hybrid bool) (string, error) {
	c.summaries++
	return c.proposal, nil
}

func TestDU04RejectedSummaryDoesNotChangeActiveHistory(t *testing.T) {
	goal := "Find the answer"
	prior := "old ledger"
	original := []Message{{Role: "user", Content: &goal}, {Role: "assistant", Content: str("count 83")}}
	client := &du04Client{proposal: "<COMPACTED_STATE><USER_GOAL>Find the answer</USER_GOAL><GLOBAL_LESSON_LEDGER>count 83</GLOBAL_LESSON_LEDGER><DEAD_ENDS>- None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>invented value 900</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>"}
	cfg := Config{SystemPrompt: "system", Model: "model", LLM: LLM{ToolCallMode: "native"}, Memory: Memory{MaxTokens: 1, RawTurnsToKeep: 0, ContextWindowTokens: 100000, ReservedOutputTokens: 64, SafetyMarginTokens: 16, MaxSummaryTokens: 2048, SummaryOutputTokens: 64, CompactionMaxAttempts: 1, CompactionMaxPasses: 1, CompactionMinGrowthTokens: 1, HybridReflection: true}}
	s := Session{Goal: goal, Memory: original, CompactedContext: &prior, Status: "active"}
	e := Engine{Config: cfg, Client: client}
	if err := e.compact(context.Background(), &s, nil); err != nil {
		t.Fatal(err)
	}
	if len(s.Memory) != len(original) || s.Memory[1].Content == nil || *s.Memory[1].Content != "count 83" || s.CompactedContext == nil || *s.CompactedContext != prior {
		t.Fatalf("rejected summary mutated active state: %+v", s)
	}
	if len(s.CompactionAttempts) == 0 || s.CompactionAttempts[0]["accepted"] != false {
		t.Fatalf("missing rejection provenance: %+v", s.CompactionAttempts)
	}
	client.summaries = 0
	if err := e.compact(context.Background(), &s, nil); err != nil {
		t.Fatal(err)
	}
	if len(s.CompactionAttempts) != 1 || client.summaries != 0 {
		t.Fatalf("unchanged failed source was retried: attempts=%+v summaries=%d", s.CompactionAttempts, client.summaries)
	}
}

func TestDU04PayloadOrderPlacesArtifactsAndGuidanceAfterRawHistory(t *testing.T) {
	goal := "Complete the audit"
	phase := "p"
	check := ResultCheck{Label: "table", Tool: "report", Kind: "markdown_table"}
	cfg := Config{SystemPrompt: "SYSTEM", Completion: Completion{Instructions: str("REQUIREMENTS"), ResultChecks: []ResultCheck{check}}, Execution: Execution{ArgumentBindings: []Binding{{Tool: "validate", Argument: "result", SourceLabel: "table", Path: []any{}}}}, Workflow: &WorkflowConfig{Phases: []Phase{{ID: "p", Name: "Phase", Instruction: "GUIDANCE", AllowedTools: []string{}}}}}
	e := Engine{Config: cfg}
	artifact := CompletionArtifact{Tool: "report", Value: "| City | Count |\n| --- | ---: |\n| A | 1 |"}
	s := Session{Goal: goal, Memory: []Message{{Role: "user", Content: &goal}, {Role: "assistant", Content: str("raw-turn-content")}}, CompactedContext: str("LEDGER"), Workflow: &WorkflowState{ActivePhase: &phase}, CompletionArtifacts: map[string]CompletionArtifact{"0": artifact}}
	p := e.payload(&s)
	joined := ""
	for _, m := range p {
		if m.Content != nil {
			joined += *m.Content + "\n"
		}
	}
	positions := []int{strings.Index(joined, "SYSTEM"), strings.Index(joined, "Original user goal"), strings.Index(joined, "REQUIREMENTS"), strings.Index(joined, "LEDGER"), strings.Index(joined, "EXECUTED RESULT ARTIFACT"), strings.Index(joined, "RAW WORKING BUFFER"), strings.Index(joined, "raw-turn-content"), strings.Index(joined, "KNOWN ARGUMENT BINDING"), strings.Index(joined, "GUIDANCE")}
	for _, position := range positions {
		if position < 0 {
			t.Fatalf("missing payload section: %q", joined)
		}
	}
	for i := 1; i < len(positions); i++ {
		if positions[i] <= positions[i-1] {
			t.Fatalf("payload order=%v", positions)
		}
	}
}

func TestDU04HardBudgetStopsBeforeDecision(t *testing.T) {
	goal := "goal"
	client := &du04Client{}
	cfg := Config{SystemPrompt: "system", Model: "model", LLM: LLM{ToolCallMode: "native"}, Memory: Memory{ContextWindowTokens: 32, ReservedOutputTokens: 16, SafetyMarginTokens: 8, RawTurnsToKeep: 0, MaxSummaryTokens: 64, SummaryOutputTokens: 8, CompactionMaxAttempts: 1, CompactionMaxPasses: 1, CompactionMinGrowthTokens: 1}}
	s := Session{Goal: goal, Memory: []Message{{Role: "user", Content: &goal}, {Role: "user", Content: str(strings.Repeat("x", 256))}}, Status: "active"}
	e := Engine{Config: cfg, Client: client}
	_ = e.Run(context.Background(), &s, 1, nil)
	if client.decisions != 0 || s.Status != "failed" || s.RequestBudget == nil || s.RequestBudget.Fits {
		t.Fatalf("decisions=%d status=%s budget=%+v", client.decisions, s.Status, s.RequestBudget)
	}
}

type du04BatchClient struct{ calls int }

func (c *du04BatchClient) Decide(context.Context, []Message) (Decision, error) {
	c.calls++
	if c.calls == 1 {
		return Decision{Calls: []ToolCall{{ID: "one", Name: "lookup", Arguments: map[string]any{}}, {ID: "two", Name: "lookup", Arguments: map[string]any{}}}}, nil
	}
	return Decision{Final: true, Content: "done"}, nil
}
func (*du04BatchClient) Compact(context.Context, string, []Message, *string, bool) (string, error) {
	return "", nil
}

func TestDU04BatchHistoryRemainsCompleteWhenStreamCloses(t *testing.T) {
	client := &du04BatchClient{}
	cfg := Config{SystemPrompt: "system", Memory: Memory{ContextWindowTokens: 100000, ReservedOutputTokens: 128, SafetyMarginTokens: 16}, Execution: Execution{ToolPolicy: "auto"}, Tools: []ToolDefinition{{Name: "lookup", Parameters: map[string]any{"type": "object"}}}}
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) { return `{"ok":true}`, nil }}}
	goal := "goal"
	s := Session{Goal: goal, Memory: []Message{{Role: "user", Content: &goal}}, Status: "active"}
	err := e.Run(context.Background(), &s, 3, func(event map[string]any) error {
		if event["step"] == "tool_completed" {
			return errors.New("stream closed")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected stream error")
	}
	groups, groupErr := interactionGroups(s.Memory)
	if groupErr != nil || len(groups) != 2 || len(groups[1].messages) != 3 {
		t.Fatalf("batch history is incomplete: groups=%d err=%v", len(groups), groupErr)
	}
}
