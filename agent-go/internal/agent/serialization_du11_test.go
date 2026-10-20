package agent

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestDU11C01SourceWireSerializationMatchesPythonContract(t *testing.T) {
	content := "A<&\u2028"
	messages := []Message{{Role: "assistant", Content: &content, CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}}
	got, err := sourceText("goal", messages, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "Goal: goal\nPrevious ledger: none\nHistorical prefix:\n[{\"content\": \"A<&\\u2028\", \"role\": \"assistant\"}]"
	if got != want {
		t.Fatalf("source wire mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestDU11C01ResolvedConfigSerializationPreservesPythonNumbersAndEscaping(t *testing.T) {
	minusZero := math.Copysign(0, -1)
	c := Config{Name: "<>&\u2028", SystemPrompt: "café", Model: "m", LLM: LLM{BaseURL: "https://example.test", APIKeyEnv: "KEY", Temperature: minusZero, TimeoutSeconds: 1e-7, ToolCallMode: "native"}, Memory: Memory{MaxTokens: 10, MaxSteps: 2, RawTurnsToKeep: 1, HybridReflection: true, ContextWindowTokens: 32768, ReservedOutputTokens: 1024, SafetyMarginTokens: 256, MaxSummaryTokens: 4096, SummaryOutputTokens: 512, CompactionMaxAttempts: 2, CompactionMaxPasses: 3, CompactionMinGrowthTokens: 512, CompactionMinIntervalSteps: 0}, Output: Output{}, Execution: Execution{ToolPolicy: "auto", MaxConsecutiveInvalidResponses: 3}, Tools: []ToolDefinition{}, Completion: Completion{ForbiddenPatterns: []string{}, ResultChecks: []ResultCheck{}}, Workflow: nil}
	got, err := canonicalConfigJSON(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"\"name\":\"<>&\u2028\"", "\"system_prompt\":\"café\"", "\"temperature\":-0.0", "\"timeout_seconds\":1e-07"} {
		if !strings.Contains(string(got), required) {
			t.Fatalf("canonical config missing %q: %s", required, got)
		}
	}
	for _, omitted := range []string{"tool_call_mode", "tool_policy", "context_window_tokens", "max_output_tokens"} {
		if strings.Contains(string(got), omitted) {
			t.Fatalf("default field %q was not omitted: %s", omitted, got)
		}
	}
}

func TestDU11C04AllSharedWorkflowProfilesResolve(t *testing.T) {
	refs := []string{"log-analysis.yaml", "revenue-analysis.yaml", "employee-audit.yaml", "compaction-stress.yaml", "completion-no-workflow.yaml", "tool-recovery.yaml"}
	// Match a stable Python golden-vector environment so local .env values cannot
	// make an otherwise equivalent config appear to have a different identity.
	t.Setenv("MAIN_MODEL", "qwen2.5-7b-instruct-mlx")
	t.Setenv("OPENAI_BASE_URL", "https://du11-fixture.invalid/v1")
	wantHashes := []string{"28aaed6c55ad854d72dc0ad0c069253c85c949e987b8c6f09b27b2e3aaa59132", "740bedd1e2989ed32c261b2664006ea734902cd12fa1f842e074ddee29692731", "f4c88391e56765d03af5f4d49b9e79f73b7ae70a979fe3aef857ab7c3fb0c202", "6c894a09bb5d0523c2cc3f43780d3be13a310e798b165d769301ff3851e5f141", "0b32fac762ae37dbb234644acfb9272587f316769f16d7be4a7037848bbd8389", "eaf52c77d961801c3c9d89bd66c0ccebeecc999576048328507a9594dbd34dab"}
	for i, ref := range refs {
		s, err := LoadSnapshot("../../../config", ref)
		if err != nil {
			t.Fatalf("%s: %v", ref, err)
		}
		if s.Ref != ref || len(s.SHA256) != 64 || len(s.ResolvedSHA256) != 64 {
			t.Errorf("%s identity incomplete: %#v", ref, s)
		}
		if s.ResolvedSHA256 != wantHashes[i] {
			t.Errorf("%s resolved hash=%s want Python golden %s", ref, s.ResolvedSHA256, wantHashes[i])
		}
		t.Logf("DU11_C04_PROFILE ref=%s raw_sha256=%s resolved_sha256=%s", ref, s.SHA256, s.ResolvedSHA256)
	}
}
