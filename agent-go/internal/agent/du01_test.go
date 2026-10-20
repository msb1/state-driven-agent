package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDU01C01ProfilesAndIdentity(t *testing.T) {
	t.Setenv("MAIN_MODEL", "du01-synthetic-model")
	t.Setenv("OPENAI_BASE_URL", "https://synthetic.invalid/v1")
	refs := []string{"log-analysis.yaml", "revenue-analysis.yaml", "employee-audit.yaml", "compaction-stress.yaml", "completion-no-workflow.yaml", "tool-recovery.yaml"}
	for _, ref := range refs {
		s, err := LoadSnapshot("../../../config", ref)
		if err != nil {
			t.Fatalf("resolve %s: %v", ref, err)
		}
		if s.Config.Model != "du01-synthetic-model" || s.Config.LLM.BaseURL != "https://synthetic.invalid/v1" {
			t.Errorf("%s did not resolve synthetic environment: model=%q base_url=%q", ref, s.Config.Model, s.Config.LLM.BaseURL)
		}
		b, err := os.ReadFile(filepath.Join("../../../config", ref))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if s.SHA256 != hex.EncodeToString(sum[:]) || len(s.ResolvedSHA256) != 64 {
			t.Errorf("%s raw/resolved config identity is invalid", ref)
		}
		preimage, err := canonicalConfigJSON(s.Config)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("DU01-C01 ref=%s raw_sha256=%s resolved_sha256=%s resolved_preimage=%s", ref, s.SHA256, s.ResolvedSHA256, preimage)
	}
	dir := t.TempDir()
	writeDU01Config(t, dir, "omitted.yaml", minimalDU01Config)
	explicit := strings.Replace(minimalDU01Config, "    timeout_seconds: 90\n", "    timeout_seconds: 90\n    tool_call_mode: native\n", 1)
	explicit = strings.Replace(explicit, "    raw_turns_to_keep: 2\n", "    raw_turns_to_keep: 2\n    context_window_tokens: 32768\n    reserved_output_tokens: 1024\n    safety_margin_tokens: 256\n", 1)
	writeDU01Config(t, dir, "explicit.yaml", explicit)
	a, err := LoadSnapshot(dir, "omitted.yaml")
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadSnapshot(dir, "explicit.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if a.ResolvedSHA256 != b.ResolvedSHA256 {
		t.Errorf("omitted defaults and explicit defaults should have identical resolved identity: %s != %s", a.ResolvedSHA256, b.ResolvedSHA256)
	}
	if a.Config.Memory.ContextWindowTokens != 32768 || a.Config.Memory.ReservedOutputTokens != 1024 || a.Config.Memory.SafetyMarginTokens != 256 || a.Config.Execution.ToolPolicy != "auto" || !a.Config.Memory.HybridReflection {
		t.Errorf("contract defaults missing: %#v", a.Config)
	}
	workflowBase := strings.Replace(minimalDU01Config, "  tools:", "  workflow:\n    entry_phase: phase\n    phases:\n      - id: phase\n        completion:\n          - tool: sample_tool\n            description: evidence\n  tools:", 1)
	for _, tc := range []struct {
		name, allow string
		want        bool
	}{{"omitted", "", true}, {"null", "        allowed_tools: null\n", true}, {"empty", "        allowed_tools: []\n", false}} {
		text := strings.Replace(workflowBase, "        completion:", tc.allow+"        completion:", 1)
		writeDU01Config(t, dir, tc.name+"-allow.yaml", text)
		snap, err := LoadSnapshot(dir, tc.name+"-allow.yaml")
		if err != nil {
			t.Fatal(err)
		}
		engine := Engine{Config: snap.Config}
		session := engine.Create("id", "goal", "ref", "hash")
		if got := engine.allowed(&session, "sample_tool"); got != tc.want {
			t.Errorf("allowed_tools %s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestDU01C02RejectsUntrustedAndInvalidConfigs(t *testing.T) {
	dir := t.TempDir()
	writeDU01Config(t, dir, "valid.yaml", minimalDU01Config)
	if err := os.Mkdir(filepath.Join(dir, "folder.yaml"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{"", "../valid.yaml", "/etc/passwd", "valid.json", "folder.yaml"} {
		if _, err := LoadSnapshot(dir, ref); err == nil {
			t.Errorf("expected rejection for ref %q", ref)
		}
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(dir, "escape.yaml")); err == nil {
		if _, err := LoadSnapshot(dir, "escape.yaml"); err == nil {
			t.Error("expected symlink escape rejection")
		}
	}
	unknownTool := strings.Replace(minimalDU01Config, "  tools:", "  workflow:\n    entry_phase: phase\n    phases:\n      - id: phase\n        completion:\n          - tool: not_registered\n  tools:", 1)
	duplicateLabel := strings.Replace(minimalDU01Config, "  tools:", "  completion:\n    result_checks:\n      - kind: number\n        tool: sample_tool\n        label: duplicate\n      - kind: number\n        tool: sample_tool\n        label: duplicate\n  tools:", 1)
	bindingBase := strings.Replace(minimalDU01Config, "properties: {}", "properties:\n          x:\n            type: string", 1)
	duplicateBinding := strings.Replace(bindingBase, "  tools:", "  completion:\n    result_checks:\n      - kind: number\n        tool: sample_tool\n        label: source\n  execution:\n    argument_bindings:\n      - tool: sample_tool\n        argument: x\n        source_label: source\n      - tool: sample_tool\n        argument: x\n        source_label: source\n  tools:", 1)
	bad := []struct{ name, source string }{
		{"unknown-tool", unknownTool},
		{"unsupported-schema", strings.Replace(minimalDU01Config, "type: object", "type: object\n        unsupportedKeyword: true", 1)},
		{"invalid-budget", strings.Replace(minimalDU01Config, "    raw_turns_to_keep: 2", "    raw_turns_to_keep: 2\n    context_window_tokens: 1000", 1)},
		{"duplicate-tool", strings.Replace(minimalDU01Config, "  tools:", "  tools:\n    - name: sample_tool\n      description: duplicate\n      parameters: {type: object}", 1)},
		{"duplicate-phase", strings.Replace(workflowConfigDU01(), "  tools:", "      - id: phase\n        name: duplicate\n        completion: []\n        transitions: []\n  tools:", 1)},
		{"bad-edge", strings.Replace(workflowConfigDU01(), "        completion:", "        transitions:\n          - to: missing\n        completion:", 1)},
		{"duplicate-label", duplicateLabel},
		{"duplicate-binding-target", duplicateBinding},
	}
	for _, x := range bad {
		t.Run(x.name, func(t *testing.T) {
			writeDU01Config(t, dir, "bad.yaml", x.source)
			if _, err := LoadSnapshot(dir, "bad.yaml"); err == nil {
				t.Error("expected invalid configuration rejection")
			}
		})
	}
}

func workflowConfigDU01() string {
	return strings.Replace(minimalDU01Config, "  tools:", "  workflow:\n    entry_phase: phase\n    phases:\n      - id: phase\n        completion:\n          - tool: sample_tool\n            description: evidence\n  tools:", 1)
}

func TestDU01C03ParsedEnvironmentExpansion(t *testing.T) {
	t.Setenv("DU01_VALUE", "shell-value")
	text := strings.Replace(minimalDU01Config, "model: test-model", "model: ${DU01_VALUE:-fallback}", 1)
	dir := t.TempDir()
	writeDU01Config(t, dir, "env.yaml", text)
	s, err := LoadSnapshot(dir, "env.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if s.Config.Model != "shell-value" {
		t.Fatalf("expected inherited environment value, got %q", s.Config.Model)
	}
	storedConfig := s.Config
	t.Setenv("DU01_VALUE", "changed-model")
	newSnapshot, err := LoadSnapshot(dir, "env.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if storedConfig.Model != "shell-value" || newSnapshot.Config.Model != "changed-model" {
		t.Fatalf("stored configuration changed with environment: stored=%q new=%q", storedConfig.Model, newSnapshot.Config.Model)
	}
	t.Setenv("DU01_VALUE", "")
	s, err = LoadSnapshot(dir, "env.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if s.Config.Model != "" {
		t.Fatalf("set empty environment variable must not use fallback, got %q", s.Config.Model)
	}
}

func TestDU01C04StrictArgumentsAndNoAdapterCall(t *testing.T) {
	for _, raw := range []string{`{"x":true,"x":false}`, `{"x":NaN}`, `{"x":1e999}`, `{"x":9223372036854775808}`} {
		if _, err := strictJSON([]byte(raw)); err == nil {
			t.Errorf("expected strict JSON rejection for %s", raw)
		}
	}
	value, err := strictJSON([]byte(`{"x":1.0}`))
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "integer", "minimum": 1}}, "required": []any{"x"}, "additionalProperties": false}
	if issues := validateArguments(value, schema, "$"); len(issues) != 0 {
		t.Fatalf("integer-valued number should pass integer schema: %v", issues)
	}
	for _, raw := range []string{`{"x":true}`, `{"x":"1"}`, `{"x":0}`, `{"x":1,"extra":2}`} {
		v, e := strictJSON([]byte(raw))
		if e != nil {
			t.Fatal(e)
		}
		if issues := validateArguments(v, schema, "$"); len(issues) == 0 {
			t.Errorf("expected schema rejection for %s", raw)
		}
	}
	boundary := map[string]any{"type": "object", "properties": map[string]any{"mode": map[string]any{"type": "string", "enum": []any{"safe", "strict"}}, "text": map[string]any{"type": "string", "minLength": 2, "maxLength": 4, "pattern": "^ok"}, "items": map[string]any{"type": "array", "minItems": 1, "maxItems": 2, "items": map[string]any{"type": "integer"}}}, "required": []any{"mode", "text", "items"}, "additionalProperties": false}
	valid, err := strictJSON([]byte(`{"mode":"safe","text":"okay","items":[1,2.0]}`))
	if err != nil {
		t.Fatal(err)
	}
	if issues := validateArguments(valid, boundary, "$"); len(issues) > 0 {
		t.Fatalf("valid boundary values rejected: %v", issues)
	}
	invalid, err := strictJSON([]byte(`{"mode":"unsafe","text":"x","items":[true,2,3]}`))
	if err != nil {
		t.Fatal(err)
	}
	if issues := validateArguments(invalid, boundary, "$"); len(issues) < 3 {
		t.Fatalf("expected enum/string/array/item validation failures, got %v", issues)
	}
	var calls atomic.Int32
	e := Engine{Config: Config{Tools: []ToolDefinition{{Name: "sample_tool", Parameters: schema}}}, Tools: map[string]Tool{"sample_tool": func(_ context.Context, _ map[string]any) (string, error) { calls.Add(1); return "ok", nil }}}
	if got, _ := e.call(context.Background(), &Session{}, Decision{Name: "sample_tool", Arguments: map[string]any{"x": "1"}}, nil); !strings.Contains(got, "invalid arguments") {
		t.Fatalf("expected actionable validation error, got %q", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid arguments reached adapter %d times", calls.Load())
	}
}

const minimalDU01Config = `agent:
  name: sample
  system_prompt: test
  model: test-model
  llm:
    base_url: https://example.invalid/v1
    timeout_seconds: 90
  memory:
    max_tokens: 100
    max_steps: 5
    raw_turns_to_keep: 2
  tools:
    - name: sample_tool
      description: sample
      parameters:
        type: object
        properties: {}
        required: []
`

func writeDU01Config(t *testing.T, dir, name, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
}
