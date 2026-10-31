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
	// Python source_text uses ensure_ascii=False, including literal U+2028.
	want := "Goal: goal\nPrevious ledger: none\nHistorical prefix:\n[{\"content\": \"A<&\u2028\", \"role\": \"assistant\"}]"
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
	// Regenerated from Python for the current shared YAML profiles (including
	// the 180-second log-analysis timeout and current compaction stress policy).
	wantHashes := []string{"256639f4b06108358687ded0ac3f7b081a20056ddee798125d049468b0a84a3d", "4c53575fc80cae25f36e92777d37875874086c7258b41608a9f1e6b2b121083e", "f4c88391e56765d03af5f4d49b9e79f73b7ae70a979fe3aef857ab7c3fb0c202", "2a55de0ce96681129de80c25afb69c12316e0a29ff3540b9c6398f59077d61b4", "0b32fac762ae37dbb234644acfb9272587f316769f16d7be4a7037848bbd8389", "eaf52c77d961801c3c9d89bd66c0ccebeecc999576048328507a9594dbd34dab"}
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
