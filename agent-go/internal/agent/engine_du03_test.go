package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestDU03ResultArtifactBindingAndNumberAnswer(t *testing.T) {
	client := &du02SequenceClient{decisions: []Decision{
		{ID: "calc", Name: "calculate", Arguments: map[string]any{}},
		{ID: "wrong", Name: "validate", Arguments: map[string]any{"total": 10.0}},
		{ID: "right", Name: "validate", Arguments: map[string]any{"total": 12.5}},
		{Final: true, Content: "Total 13.00"},
		{Final: true, Content: "Total $12.50"},
	}}
	cfg := Config{SystemPrompt: "test", Memory: Memory{MaxSteps: 8, RawTurnsToKeep: 8}, Execution: Execution{ToolPolicy: "auto"},
		Tools: []ToolDefinition{
			{Name: "calculate", Parameters: map[string]any{"type": "object"}},
			{Name: "validate", Parameters: map[string]any{"type": "object", "properties": map[string]any{"total": map[string]any{"type": "number"}}, "required": []any{"total"}}},
		},
		Completion: Completion{ResultChecks: []ResultCheck{{Kind: "number", Tool: "calculate", Label: "Total", Path: []any{"total"}, Result: map[string]any{"total": float64(12.5)}}}},
	}
	// ResultCheck.Path extracts the number from the tool's JSON result. Binding.Path
	// is then relative to that extracted value, so it must be empty here.
	cfg.Execution.ArgumentBindings = []Binding{{Tool: "validate", Argument: "total", SourceLabel: "Total"}}
	toolCalls := 0
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{
		"calculate": func(context.Context, map[string]any) (string, error) { return `{"total":12.50}`, nil },
		"validate": func(_ context.Context, args map[string]any) (string, error) {
			toolCalls++
			if args["total"] != 12.5 {
				t.Fatalf("bound argument=%#v", args["total"])
			}
			return `{"valid":true}`, nil
		},
	}}
	s := e.Create("du03", "goal", "profile", "hash")
	if err := e.Run(context.Background(), &s, 8, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "complete" || s.FinalAnswer == nil || *s.FinalAnswer != "Total $12.50" {
		t.Fatalf("status=%s answer=%v", s.Status, s.FinalAnswer)
	}
	if toolCalls != 1 || len(s.Executions) != 2 || len(s.RejectedFinals) != 1 || len(s.CompletionArtifacts) != 1 {
		t.Fatalf("tool_calls=%d executions=%d rejected=%d artifacts=%d", toolCalls, len(s.Executions), len(s.RejectedFinals), len(s.CompletionArtifacts))
	}
	foundBindingRejection := false
	for _, message := range s.Memory {
		if strings.Contains(message.ContentValue(), "must equal its bound executed result") {
			foundBindingRejection = true
		}
	}
	if !foundBindingRejection {
		t.Fatalf("wrong binding result was not rejected: %#v", s.Memory)
	}
}

func TestDU03PatternForbiddenAndJSONSchemaCompletion(t *testing.T) {
	t.Run("pattern_and_forbidden", func(t *testing.T) {
		client := &du02SequenceClient{decisions: []Decision{{Final: true, Content: "OK 12 secret"}, {Final: true, Content: "OK 12"}}}
		pattern := `OK [0-9]+`
		cfg := testOpenAIConfig()
		cfg.Memory.MaxSteps = 3
		cfg.Completion.Pattern = &pattern
		cfg.Completion.ForbiddenPatterns = []string{"secret"}
		e := Engine{Config: cfg, Client: client}
		s := e.Create("pattern", "goal", "profile", "hash")
		if err := e.Run(context.Background(), &s, 3, nil); err != nil || s.Status != "complete" || *s.FinalAnswer != "OK 12" || len(s.RejectedFinals) != 1 {
			t.Fatalf("state=%#v err=%v", s, err)
		}
	})
	t.Run("json_schema", func(t *testing.T) {
		client := &du02SequenceClient{decisions: []Decision{
			{Final: true, Content: `Here is the result: {"ok":true,"count":3}`},
			{Final: true, Content: `{"ok":"true","count":3}`},
			{Final: true, Content: `{"ok":true,"count":3}`},
		}}
		cfg := testOpenAIConfig()
		cfg.Memory.MaxSteps = 4
		cfg.Completion.JSONSchema = map[string]any{"type": "object", "properties": map[string]any{"ok": map[string]any{"type": "boolean"}, "count": map[string]any{"type": "integer"}}, "required": []any{"ok", "count"}, "additionalProperties": false}
		e := Engine{Config: cfg, Client: client}
		s := e.Create("json", "goal", "profile", "hash")
		if err := e.Run(context.Background(), &s, 4, nil); err != nil || s.Status != "complete" || *s.FinalAnswer != `{"ok":true,"count":3}` || len(s.RejectedFinals) != 2 {
			t.Fatalf("state=%#v err=%v", s, err)
		}
	})
}

func TestDU03NoProgressLimitAndWarning(t *testing.T) {
	client := &du02SequenceClient{decisions: []Decision{
		{ID: "1", Name: "lookup", Arguments: map[string]any{}},
		{ID: "2", Name: "lookup", Arguments: map[string]any{}},
		{ID: "3", Name: "lookup", Arguments: map[string]any{}},
	}}
	limit := 2
	cfg := testOpenAIConfig()
	cfg.Memory.MaxSteps = 5
	cfg.Execution.MaxNoProgressSteps = &limit
	cfg.Tools[0].Parameters = map[string]any{"type": "object"}
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) { return `{"same":true}`, nil }}}
	s := e.Create("progress", "goal", "profile", "hash")
	warnings := 0
	err := e.Run(context.Background(), &s, 5, func(event map[string]any) error {
		if event["step"] == "no_progress_warning" {
			warnings++
		}
		return nil
	})
	if err != nil || s.Status != "failed" || s.NoProgressSteps != 2 || warnings != 1 {
		t.Fatalf("status=%s no_progress=%d warnings=%d err=%v", s.Status, s.NoProgressSteps, warnings, err)
	}
}

func TestDU03RunMaxStepsDefaultAndExplicitOverride(t *testing.T) {
	makeConfig := func() Config {
		runMax := 1
		cfg := testOpenAIConfig()
		cfg.Memory.MaxSteps = 5
		cfg.Memory.RunMaxSteps = &runMax
		cfg.Tools[0].Parameters = map[string]any{"type": "object"}
		return cfg
	}
	t.Run("configured_default", func(t *testing.T) {
		client := &du02SequenceClient{decisions: []Decision{{ID: "1", Name: "lookup", Arguments: map[string]any{}}, {ID: "2", Name: "lookup", Arguments: map[string]any{}}}}
		cfg := makeConfig()
		e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) { return "ok", nil }}}
		s := e.Create("run-default", "goal", "profile", "hash")
		_ = e.Run(context.Background(), &s, 0, nil)
		if s.StepCount != 1 || s.Status != "failed" {
			t.Fatalf("step_count=%d status=%s", s.StepCount, s.Status)
		}
	})
	t.Run("explicit_override", func(t *testing.T) {
		client := &du02SequenceClient{decisions: []Decision{{ID: "1", Name: "lookup", Arguments: map[string]any{}}, {ID: "2", Name: "lookup", Arguments: map[string]any{}}}}
		cfg := makeConfig()
		e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) { return "ok", nil }}}
		s := e.Create("run-override", "goal", "profile", "hash")
		_ = e.Run(context.Background(), &s, 2, nil)
		if s.StepCount != 2 || s.Status != "failed" {
			t.Fatalf("step_count=%d status=%s", s.StepCount, s.Status)
		}
	})
}

func TestDU03CompactionIntervalTriggersCompaction(t *testing.T) {
	interval := 2
	cfg := testOpenAIConfig()
	cfg.Memory = Memory{MaxTokens: 10000, MaxSteps: 10, RawTurnsToKeep: 4, CompactionIntervalSteps: &interval, HybridReflection: true, ContextWindowTokens: 100000, ReservedOutputTokens: 512, MaxSummaryTokens: 2048, SummaryOutputTokens: 512, CompactionMaxAttempts: 1, CompactionMaxPasses: 1, CompactionMinGrowthTokens: 1}
	cfg.Tools[0].Parameters = map[string]any{"type": "object"}
	decisions := make([]Decision, 0, 6)
	for i := 0; i < 5; i++ {
		decisions = append(decisions, Decision{ID: fmt.Sprintf("call-%d", i), Name: "lookup", Arguments: map[string]any{}})
	}
	decisions = append(decisions, Decision{Final: true, Content: "done"})
	e := Engine{Config: cfg, Client: &du02SequenceClient{decisions: decisions}, Tools: map[string]Tool{
		"lookup": func(context.Context, map[string]any) (string, error) { return "ok", nil },
	}}
	s := e.Create("interval", "goal", "profile", "hash")
	if err := e.Run(context.Background(), &s, 8, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "complete" || len(s.CompactionEvents) == 0 || s.LastCompactionStep < interval {
		t.Fatalf("status=%s compactions=%d last_step=%d", s.Status, len(s.CompactionEvents), s.LastCompactionStep)
	}
}

func TestDU03ResultArtifactJSONAndMarkdownChecks(t *testing.T) {
	jsonArtifact, err := extractResult(ResultCheck{Kind: "json_value", Path: []any{"result", "count"}}, `{"result":{"count":4}}`)
	if err != nil || !jsonEqual(jsonArtifact, json.Number("4")) {
		t.Fatalf("json artifact=%#v err=%v", jsonArtifact, err)
	}
	if !checkAnswer(ResultCheck{Kind: "json_value", AnswerPath: []any{"count"}}, `{"count":4}`, jsonArtifact) {
		t.Fatal("typed JSON answer should match artifact")
	}
	table, err := extractResult(ResultCheck{Kind: "markdown_table"}, "| City | Revenue |\n| --- | ---: |\n| Chicago | 271017.77 |\n")
	if err != nil {
		t.Fatal(err)
	}
	if !checkAnswer(ResultCheck{Kind: "markdown_table"}, "| City | Revenue |\n| --- | --- |\n| Chicago | 271017.770 |", table) {
		t.Fatal("decimal-equivalent table cell should match executed artifact")
	}
	if checkAnswer(ResultCheck{Kind: "markdown_table"}, "| City | Revenue |\n| --- | --- |\n| Chicago | 271017.77 |\n| Chicago | 271017.77 |", table) {
		t.Fatal("duplicate table row keys must be rejected")
	}
	actualFromCase4 := "Phase 3\n\n| City       | Valid headcount | Average age |\n|------------|----------------:|-------------:\n| Austin     | 199             | 42.62        |\n| Chicago    | 181             | 42.01        |\n| New York   | 329             | 43.02        |\n| Seattle    | 183             | 43.46        |"
	expectedFromCase4 := [][]string{
		{"City", "Valid headcount", "Average age"},
		{"Austin", "199", "42.62"},
		{"Chicago", "181", "42.01"},
		{"New York", "329", "43.02"},
		{"Seattle", "183", "43.46"},
	}
	if !checkAnswer(ResultCheck{Kind: "markdown_table"}, actualFromCase4, expectedFromCase4) {
		t.Fatal("valid Markdown table with an omitted trailing separator pipe should match the executed result")
	}
}

func TestDU03CompletedWorkflowRepeatsArtifactsInFinalSynthesis(t *testing.T) {
	pattern := `Phase 3[\s\S]*`
	cfg := testOpenAIConfig()
	cfg.Completion.Pattern = &pattern
	cfg.Completion.Instructions = str("Start with Phase 3.")
	cfg.Completion.ResultChecks = []ResultCheck{{Kind: "markdown_table", Label: "Clean city table", Tool: "report"}}
	s := Session{Goal: "finish audit", Memory: []Message{{Role: "user", Content: str("finish audit")}}, CompletionArtifacts: map[string]CompletionArtifact{
		"0": {Tool: "report", Value: [][]string{{"City", "Headcount"}, {"New York", "329"}}},
	}}
	payload := (Engine{Config: cfg}).payload(&s)
	last := payload[len(payload)-1].Content
	if last == nil || !strings.Contains(*last, "[FINAL SYNTHESIS]") || !strings.Contains(*last, "| New York | 329 |") || !strings.Contains(*last, "Start with Phase 3.") {
		t.Fatalf("final synthesis did not repeat instructions and artifact: %#v", payload)
	}
}

func (m Message) ContentValue() string {
	if m.Content == nil {
		return ""
	}
	return *m.Content
}
