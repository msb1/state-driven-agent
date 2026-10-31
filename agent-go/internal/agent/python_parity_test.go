package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
)

func storedParityConfig(t *testing.T, cfg Config) Config {
	t.Helper()
	before, err := ConfigIdentity(cfg)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var stored Config
	if err := json.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	after, err := ConfigIdentity(stored)
	if err != nil || before != after {
		t.Fatalf("JSON reload changed config identity: %s != %s (%v)", before, after, err)
	}
	return stored
}

func TestWorkflowAllowedToolsJSONRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		explicit    bool
		tools       []string
	}{
		{"omitted", "", false, []string{"validate"}},
		{"null", `,"allowed_tools":null`, false, []string{"validate"}},
		{"empty", `,"allowed_tools":[]`, true, []string{}},
		{"explicit", `,"allowed_tools":["inspect","validate"]`, true, []string{"inspect", "validate"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var phase Phase
			if err := json.Unmarshal([]byte(`{"id":"phase","completion":[{"tool":"validate","required":true}]`+tc.field+`}`), &phase); err != nil {
				t.Fatal(err)
			}
			for reload := 0; reload < 2; reload++ {
				if phase.AllowedToolsSet != tc.explicit {
					t.Fatalf("explicit flag after reload %d: %v", reload, phase.AllowedToolsSet)
				}
				e := Engine{Config: Config{Workflow: &WorkflowConfig{EntryPhase: "phase", Phases: []Phase{phase}}}}
				s := e.Create("id", "goal", "ref", "hash")
				policy := e.decisionPolicy(&s)
				if !reflect.DeepEqual(policy.AllowedTools, tc.tools) {
					t.Fatalf("advertised tools = %#v, want %#v", policy.AllowedTools, tc.tools)
				}
				for _, tool := range []string{"inspect", "validate", "unknown"} {
					if e.allowed(&s, tool) != has(tc.tools, tool) {
						t.Fatalf("scheduler/model tool policy differs for %s", tool)
					}
				}
				data, _ := json.Marshal(phase)
				if err := json.Unmarshal(data, &phase); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestPythonModelRequestParityAfterConfigReload(t *testing.T) {
	t.Setenv("MAIN_MODEL", "parity-model")
	data, err := os.ReadFile("testdata/python_request_parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Ref, Mode, Variant string
			Payload            map[string]any
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, tc := range fixture.Cases {
		t.Run(tc.Ref+"/"+tc.Mode+"/"+tc.Variant, func(t *testing.T) {
			snap, err := LoadSnapshot("../../../config", tc.Ref)
			if err != nil {
				t.Fatal(err)
			}
			cfg := storedParityConfig(t, snap.Config)
			cfg.LLM.ToolCallMode = tc.Mode
			e := Engine{Config: cfg}
			s := e.Create("id", "Parity regression goal.", tc.Ref, snap.SHA256)
			switch tc.Variant {
			case "initial":
			case "complete", "complete_artifacts":
				s.Workflow.ActivePhase = nil
				for _, phase := range cfg.Workflow.Phases {
					s.Workflow.CompletedPhases = append(s.Workflow.CompletedPhases, phase.ID)
				}
				if tc.Variant == "complete_artifacts" {
					s.CompletionArtifacts = map[string]CompletionArtifact{
						"0": {Value: [][]string{{"City", "Valid headcount", "Average age"}, {"Austin", "199", "42.62"}}},
						"1": {Value: float64(92500)},
					}
				}
			case "after_inspection":
				AppendMessage(&s, Message{Role: "assistant", ToolCalls: []map[string]any{{"id": "inspection", "type": "function", "function": map[string]any{"name": "inspect_log", "arguments": `{"start_line":1,"line_count":10}`}}}})
				AppendMessage(&s, Message{Role: "tool", Name: str("inspect_log"), ToolCallID: str("inspection"), Content: str("First log excerpt.")})
			case "compacted":
				s.CompactedContext = str("Retained historical claims.")
			default:
				s.Workflow.ActivePhase = str(tc.Variant)
			}
			payload, err := e.requestPayload(&s)
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(payload)
			var actual map[string]any
			if err := json.Unmarshal(encoded, &actual); err != nil {
				t.Fatal(err)
			}
			for key, want := range tc.Payload {
				if !reflect.DeepEqual(actual[key], want) {
					gotJSON, _ := json.MarshalIndent(actual[key], "", "  ")
					wantJSON, _ := json.MarshalIndent(want, "", "  ")
					t.Errorf("%s differs from Python\nGot: %s\nWant: %s", key, gotJSON, wantJSON)
				}
			}
			if len(actual) != len(tc.Payload) {
				t.Errorf("Go added or omitted request fields: got %d, want %d", len(actual), len(tc.Payload))
			}
		})
	}
}

type parityTransport func(*http.Request) (*http.Response, error)

func (f parityTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWorkflowStoredConfigRealModelClient(t *testing.T) {
	snap, err := LoadSnapshot("../../../config", "log-analysis.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg := storedParityConfig(t, snap.Config)
	requests := 0
	transport := parityTransport(func(r *http.Request) (*http.Response, error) {
		var payload struct {
			Tools []struct {
				Function struct{ Name string }
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		names := []string{}
		for _, tool := range payload.Tools {
			names = append(names, tool.Function.Name)
		}
		if !reflect.DeepEqual(names, []string{"inspect_log", "analyze_500_ips", "validate_top_offending_ip"}) {
			t.Fatalf("HTTP model request lost configured tools: %v", names)
		}
		requests++
		message := map[string]any{"role": "assistant", "content": "10.0.0.5 83"}
		if requests == 1 {
			message = map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{
				"id": "validation", "type": "function", "function": map[string]any{
					"name": "validate_top_offending_ip", "arguments": `{"ip":"10.0.0.5","count":83}`,
				},
			}}}
		}
		body, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": message}}})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
	})
	e := Engine{Config: cfg, Client: OpenAIClient{Config: cfg, HTTP: &http.Client{Transport: transport}}, Tools: map[string]Tool{
		"validate_top_offending_ip": func(context.Context, map[string]any) (string, error) { return `{"valid":true}`, nil },
	}}
	s := e.Create("id", "Find and validate the top IP.", snap.Ref, snap.SHA256)
	if err := e.Run(context.Background(), &s, 3, nil); err != nil || s.Status != "complete" || requests != 2 {
		t.Fatalf("stored-config model client failed: state=%s requests=%d err=%v", s.Status, requests, err)
	}
}

func TestPythonWorkflowFeedbackAndSummaryParity(t *testing.T) {
	t.Setenv("MAIN_MODEL", "parity-model")
	data, err := os.ReadFile("testdata/python_request_parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Feedback  map[string]string `json:"argument_feedback"`
		Summaries []struct {
			Hybrid  bool
			Payload map[string]any
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	snap, err := LoadSnapshot("../../../config", "log-analysis.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg := storedParityConfig(t, snap.Config)
	e := Engine{Config: cfg}
	s := e.Create("id", "Parity regression goal.", snap.Ref, snap.SHA256)
	for tool, want := range fixture.Feedback {
		if got := e.argumentFeedback(&s, tool, map[string]any{}); got != want {
			t.Errorf("%s feedback differs from Python\nGot: %s\nWant: %s", tool, got, want)
		}
	}
	for _, summary := range fixture.Summaries {
		payload := (OpenAIClient{Config: cfg}).summaryPayload(s.Goal, []Message{{Role: "user", Content: str("Historical observation: café — 😀.")}}, nil, summary.Hybrid)
		encoded, _ := json.Marshal(payload)
		var actual map[string]any
		if err := json.Unmarshal(encoded, &actual); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, summary.Payload) {
			t.Errorf("summary hybrid=%v differs from Python: %#v", summary.Hybrid, actual)
		}
	}
}

func TestWorkflowPrematureDraftPreservesAssistantBeforeCorrection(t *testing.T) {
	snap, err := LoadSnapshot("../../../config", "log-analysis.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg := storedParityConfig(t, snap.Config)
	client := &du02SequenceClient{decisions: []Decision{
		{Final: true, Content: "PREMATURE DRAFT"},
		{ID: "validate", Name: "validate_top_offending_ip", Arguments: map[string]any{"ip": "10.0.0.5", "count": 83}},
		{Final: true, Content: "Validated 10.0.0.5 83"},
	}}
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{
		"validate_top_offending_ip": func(context.Context, map[string]any) (string, error) { return `{"valid":true}`, nil },
	}}
	s := e.Create("id", "goal", snap.Ref, snap.SHA256)
	if err := e.Run(context.Background(), &s, 3, nil); err != nil || s.Status != "complete" {
		t.Fatalf("status=%s err=%v", s.Status, err)
	}
	if len(s.RejectedFinals) != 1 || s.RejectedFinals[0].Content != "PREMATURE DRAFT" {
		t.Fatal("Premature draft missing from audit")
	}
	found := false
	for i, m := range s.Memory {
		if m.ContentValue() == "PREMATURE DRAFT" {
			found = true
			if m.Role != "assistant" || i+1 >= len(s.Memory) || s.Memory[i+1].Role != "user" {
				t.Fatal("Rejected draft lost assistant/user correction pairing")
			}
		}
	}
	if !found {
		t.Fatal("Rejected assistant turn missing before correction")
	}
}

func TestWorkflowInvalidArgumentsHaveBoundedRecovery(t *testing.T) {
	snap, err := LoadSnapshot("../../../config", "log-analysis.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg := storedParityConfig(t, snap.Config)
	cfg.Execution.MaxConsecutiveInvalidResponses = 2
	client := &du02SequenceClient{decisions: []Decision{
		{ID: "bad1", Name: "inspect_log", Arguments: map[string]any{}},
		{ID: "bad2", Name: "inspect_log", Arguments: map[string]any{}},
		{Final: true, Content: "Must not reach this"},
	}}
	e := Engine{Config: cfg, Client: client}
	s := e.Create("id", "goal", snap.Ref, snap.SHA256)
	if err := e.Run(context.Background(), &s, 3, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "failed" || s.LastError["code"] != "invalid_response_limit" || s.StepCount != 2 {
		t.Fatalf("invalid arguments did not stop at the configured limit: %#v", s.LastError)
	}
	if !strings.Contains(s.Memory[2].ContentValue(), "Argument-shape example") {
		t.Fatal("Missing actionable schema feedback")
	}
}

func TestConfigJSONPreservesNumericSchemasAndResultPaths(t *testing.T) {
	cfg := Config{Tools: []ToolDefinition{{Name: "lookup", Parameters: map[string]any{
		"type": "object", "properties": map[string]any{
			"label": map[string]any{"type": "string", "minLength": 2, "maxLength": 4},
			"rows":  map[string]any{"type": "array", "minItems": 1, "maxItems": 2},
		},
	}}}, Completion: Completion{ResultChecks: []ResultCheck{{Kind: "number", Tool: "lookup", Path: []any{0}, Result: map[string]any{"median": 92500.0}}}}}
	stored := storedParityConfig(t, cfg)
	for _, invalid := range []map[string]any{
		{"label": "x"}, {"label": "longer"}, {"rows": []any{}}, {"rows": []any{1, 2, 3}},
	} {
		if len(validateArguments(invalid, stored.Tools[0].Parameters, "$")) == 0 {
			t.Fatalf("JSON reload disabled schema bounds for %#v", invalid)
		}
	}
	if v, ok := valueAtPath([]any{"first"}, stored.Completion.ResultChecks[0].Path); !ok || v != "first" {
		t.Fatal("JSON reload changed integer result-path semantics")
	}
	if got := promptJSON(stored.Completion.ResultChecks[0].Result); got != `{"median": 92500.0}` {
		t.Fatalf("JSON reload changed decimal spelling: %s", got)
	}
}
