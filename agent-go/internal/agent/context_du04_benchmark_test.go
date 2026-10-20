package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

type du04GridClient struct {
	valid                bool
	decisions, summaries int
}
type du04Sample struct {
	ElapsedMS     int64  `json:"elapsed_ms"`
	DecisionCalls int    `json:"decision_calls"`
	SummaryCalls  int    `json:"summary_calls"`
	Accepted      int    `json:"accepted_summaries"`
	Rejected      int    `json:"rejected_summaries"`
	Before        int    `json:"before_estimated_tokens"`
	After         int    `json:"after_estimated_tokens"`
	Status        string `json:"status"`
}

func (c *du04GridClient) Decide(context.Context, []Message) (Decision, error) {
	c.decisions++
	return Decision{Final: true, Content: "complete"}, nil
}
func (c *du04GridClient) Compact(_ context.Context, goal string, _ []Message, _ *string, hybrid bool) (string, error) {
	c.summaries++
	if !c.valid {
		return "<truncated>", nil
	}
	return "<COMPACTED_STATE><USER_GOAL>" + xmlEscape(goal) + "</USER_GOAL><GLOBAL_LESSON_LEDGER>- None.</GLOBAL_LESSON_LEDGER><DEAD_ENDS>- None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>Continue with the retained evidence.</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>", nil
}

// TestDU04B01QualificationGrid is a deterministic 54-trial context workload.
// It emits JSONL rows via t.Log for archival without contacting a model service.
func TestDU04B01QualificationGrid(t *testing.T) {
	for _, pressure := range []string{"headroom", "soft", "hard"} {
		for _, size := range []int{1024, 8192, 32768} {
			for _, valid := range []bool{true, false} {
				cell := make([]du04Sample, 0, 3)
				for repetition := 1; repetition <= 3; repetition++ {
					goal := "Preserve verified findings"
					memory := Memory{MaxTokens: 1, MaxSteps: 4, RawTurnsToKeep: 2, HybridReflection: true, ReservedOutputTokens: 64, SafetyMarginTokens: 32, MaxSummaryTokens: 4096, SummaryOutputTokens: 64, CompactionMaxAttempts: 2, CompactionMaxPasses: 2, CompactionMinGrowthTokens: 1}
					state := Session{Goal: goal, Status: "active", Memory: []Message{{Role: "user", Content: &goal}}}
					for i := 0; i < 5; i++ {
						id := fmt.Sprintf("pair-%d", i)
						args, _ := json.Marshal(map[string]any{"index": i})
						call := map[string]any{"id": id, "type": "function", "function": map[string]any{"name": "record", "arguments": string(args)}}
						state.Memory = append(state.Memory, Message{Role: "assistant", ToolCalls: []any{call}})
						payloadBytes := size
						if i == 4 {
							payloadBytes = 1024
						}
						result := strings.Repeat("x", payloadBytes)
						state.Memory = append(state.Memory, Message{Role: "tool", ToolCallID: &id, Content: &result})
					}
					client := &du04GridClient{valid: valid}
					cfg := Config{Model: "grid", SystemPrompt: "context workload", LLM: LLM{ToolCallMode: "native"}, Memory: memory, Tools: []ToolDefinition{{Name: "record", Parameters: map[string]any{"type": "object"}}}, Execution: Execution{ToolPolicy: "auto"}}
					engine := Engine{Config: cfg, Client: client}
					payload, _ := (OpenAIClient{Config: cfg}).decisionPayload(engine.payload(&state))
					budget, _ := estimatePayload(payload, memory)
					switch pressure {
					case "headroom":
						memory.MaxTokens = budget.EstimatedInputTokens * 2
						memory.ContextWindowTokens = budget.EstimatedInputTokens + memory.ReservedOutputTokens + memory.SafetyMarginTokens + 2048
					case "soft":
						memory.ContextWindowTokens = budget.EstimatedInputTokens + memory.ReservedOutputTokens + memory.SafetyMarginTokens + 2048
					case "hard":
						memory.ContextWindowTokens = budget.EstimatedInputTokens + memory.ReservedOutputTokens + memory.SafetyMarginTokens - 1
					}
					cfg.Memory = memory
					engine.Config = cfg
					client.valid = valid
					start := time.Now()
					_ = engine.Run(context.Background(), &state, 1, nil)
					elapsed := time.Since(start).Milliseconds()
					accepted := 0
					for _, p := range state.CompactionProvenance {
						if p.Accepted {
							accepted++
						}
					}
					cell = append(cell, du04Sample{ElapsedMS: elapsed, DecisionCalls: client.decisions, SummaryCalls: client.summaries, Accepted: accepted, Rejected: client.summaries - accepted, Before: budget.EstimatedInputTokens, After: state.RequestBudget.EstimatedInputTokens, Status: state.Status})
					if pressure == "headroom" && client.summaries != 0 {
						t.Fatalf("headroom summary call unexpectedly occurred: size=%d valid=%v", size, valid)
					}
					if pressure == "soft" && state.Status != "complete" {
						t.Fatalf("soft trial failed: size=%d valid=%v status=%s", size, valid, state.Status)
					}
					if pressure == "hard" && valid && state.Status != "complete" {
						t.Fatalf("valid hard trial failed: size=%d status=%s", size, state.Status)
					}
					if pressure == "hard" && !valid && (client.decisions != 0 || state.Status != "failed") {
						t.Fatalf("invalid hard trial dispatched decision: size=%d calls=%d status=%s", size, client.decisions, state.Status)
					}
				}
				t.Logf("DU04_B01_CELL %s", mustJSON(map[string]any{"pressure": pressure, "historical_result_bytes": size, "summary_valid": valid, "n": len(cell), "samples": cell, "median_elapsed_ms": medianMS(cell), "max_elapsed_ms": maxMS(cell), "p95_observed_elapsed_ms": percentile95MS(cell)}))
			}
		}
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
func medianMS(x []du04Sample) int64 {
	a := sampleTimes(x)
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return a[len(a)/2]
}
func maxMS(x []du04Sample) int64 {
	a := sampleTimes(x)
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return a[len(a)-1]
}
func percentile95MS(x []du04Sample) int64 {
	a := sampleTimes(x)
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return a[(95*len(a)+99)/100-1]
}
func sampleTimes(x []du04Sample) []int64 {
	a := make([]int64, len(x))
	for i, s := range x {
		a[i] = s.ElapsedMS
	}
	return a
}
