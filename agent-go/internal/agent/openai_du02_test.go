package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testOpenAIConfig() Config {
	return Config{Model: "model-x", LLM: LLM{Temperature: .1, TimeoutSeconds: 2, ToolCallMode: "native"}, Memory: Memory{ReservedOutputTokens: 1024}, Execution: Execution{ToolPolicy: "auto"}, Tools: []ToolDefinition{{Name: "lookup", Description: "look up", Parameters: map[string]any{"type": "object"}, Recovery: &ToolRecovery{Mode: "read_only", MaxAttempts: 2}}}}
}

type du02SequenceClient struct {
	decisions []Decision
	index     int
}

type du02BlockingTransport struct{}

func (du02BlockingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func (c *du02SequenceClient) Decide(context.Context, []Message) (Decision, error) {
	d := c.decisions[c.index]
	c.index++
	return d, nil
}
func (*du02SequenceClient) Compact(_ context.Context, goal string, _ []Message, _ *string, _ bool) (string, error) {
	return "<COMPACTED_STATE><USER_GOAL>" + goal + "</USER_GOAL><GLOBAL_LESSON_LEDGER>- None.</GLOBAL_LESSON_LEDGER><DEAD_ENDS>- None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>Continue the task.</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>", nil
}

func TestDU02NativeRetainsEveryCallAndCorrelatesInvalidArguments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if len(payload["tools"].([]any)) != 1 {
			t.Errorf("expected public tool schema")
		}
		b, _ := json.Marshal(payload["tools"])
		if strings.Contains(string(b), "recovery") {
			t.Errorf("private recovery leaked into schema: %s", b)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"tool_calls":[{"id":"dup","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"a\"}"}},{"id":"dup","type":"function","function":{"name":"lookup","arguments":"{bad"}},{"id":null,"type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]}`))
	}))
	defer server.Close()
	cfg := testOpenAIConfig()
	cfg.LLM.BaseURL = server.URL
	d, err := (OpenAIClient{Config: cfg}).Decide(context.Background(), []Message{{Role: "assistant", ToolCalls: []any{map[string]any{"id": "dup"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Calls) != 3 {
		t.Fatalf("got %d calls", len(d.Calls))
	}
	if d.Calls[0].Arguments["q"] != "a" || d.Calls[0].Invalid != "" {
		t.Fatalf("first call lost: %#v", d.Calls[0])
	}
	if d.Calls[1].Invalid == "" || d.Calls[1].RawArguments != "{bad" {
		t.Fatalf("invalid raw arguments not retained: %#v", d.Calls[1])
	}
	ids := map[string]bool{}
	for _, call := range d.Calls {
		if call.ID == "" || ids[call.ID] {
			t.Fatalf("call IDs not unique: %#v", d.Calls)
		}
		ids[call.ID] = true
	}
}

func TestDU02IntegerToolArgumentsRemainNumeric(t *testing.T) {
	parsed, err := strictJSON([]byte(`{"ip":"10.0.0.5","count":83}`))
	if err != nil {
		t.Fatal(err)
	}
	args, ok := parsed.(map[string]any)
	if !ok {
		t.Fatalf("arguments parsed as %T", parsed)
	}
	result, err := PostgresTools(nil)["validate_top_offending_ip"](context.Background(), args)
	if err != nil || result != `{"valid": true, "message": "Validated."}` {
		t.Fatalf("validator result=%s err=%v", result, err)
	}
}

func TestDU02FixtureValidatorJSONMatchesSharedSpacing(t *testing.T) {
	tools := PostgresTools(nil)
	cases := []struct {
		name, tool string
		args       map[string]any
		want       string
	}{
		{"chicago_revenue", "validate_chicago_q1_revenue", map[string]any{"total": 271017.77}, `{"valid": true, "expected_hint": "Validated."}`},
		{"employee_audit", "validate_employee_audit", map[string]any{"phase_1_logged": true, "normalize_city": true, "drop_invalid_age": true, "median_salary": 92500.0}, `{"valid": true, "expected_median_salary": 92500.0}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tools[tc.tool](context.Background(), tc.args)
			if err != nil || got != tc.want {
				t.Fatalf("validator result=%s err=%v; want %s", got, err, tc.want)
			}
		})
	}
}

func TestDU02ExactCompletionRejectsSurroundingProse(t *testing.T) {
	exact := "RECOVERY_OK 10.0.0.5 83"
	cfg := testOpenAIConfig()
	cfg.Memory.MaxSteps = 3
	cfg.Completion.Exact = &exact
	client := &du02SequenceClient{decisions: []Decision{{Final: true, Content: "The exact answer is:\n\n" + exact}, {Final: true, Content: exact}}}
	e := Engine{Config: cfg, Client: client}
	s := e.Create("s", "goal", "tool-recovery.yaml", "hash")
	if err := e.Run(context.Background(), &s, 3, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "complete" || s.FinalAnswer == nil || *s.FinalAnswer != exact {
		t.Fatalf("status=%s final=%v", s.Status, s.FinalAnswer)
	}
}

func TestDU02CompletedWorkflowRejectsFurtherToolExecution(t *testing.T) {
	calls := 0
	toolDef := ToolDefinition{Name: "lookup", Parameters: map[string]any{"type": "object"}}
	cfg := testOpenAIConfig()
	cfg.Tools = []ToolDefinition{toolDef}
	cfg.Workflow = &WorkflowConfig{}
	e := Engine{Config: cfg, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) { calls++; return "ok", nil }}}
	s := Session{Workflow: &WorkflowState{ActivePhase: nil}}
	result, invoked := e.call(context.Background(), &s, Decision{Name: "lookup", Arguments: map[string]any{}}, nil)
	if invoked || calls != 0 || !strings.Contains(result, "Workflow is complete") {
		t.Fatalf("result=%q invoked=%t calls=%d", result, invoked, calls)
	}
}

func TestDU02EmptyAndMalformedResponsesAreRecoverable(t *testing.T) {
	for _, tc := range []struct{ name, body, code string }{{"empty", `{"choices":[]}`, "empty_model_response"}, {"missing", `{}`, "invalid_model_response"}, {"empty_content", `{"choices":[{"message":{"content":" "}}]}`, "empty_model_response"}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			defer server.Close()
			cfg := testOpenAIConfig()
			cfg.LLM.BaseURL = server.URL
			d, err := (OpenAIClient{Config: cfg}).Decide(context.Background(), nil)
			if err != nil || !strings.HasPrefix(d.Invalid, tc.code) {
				t.Fatalf("decision=%#v err=%v", d, err)
			}
		})
	}
}

func TestDU02WirePoliciesAnd404OnlyFallback(t *testing.T) {
	for _, tc := range []struct {
		name, mode, policy, choice string
		tools, parallel            bool
	}{{"native", "native", "auto", "auto", true, false}, {"single", "single", "auto", "auto", true, true}, {"disabled", "native", "none", "none", false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var p map[string]any
				_ = json.NewDecoder(r.Body).Decode(&p)
				if p["model"] != "model-x" || p["temperature"] != 0.1 || p["max_tokens"] != float64(1024) {
					t.Errorf("request defaults were not serialized: %#v", p)
				}
				_, has := p["tools"]
				if has != tc.tools {
					t.Errorf("tools presence=%v", has)
				}
				if p["tool_choice"] != tc.choice {
					t.Errorf("tool_choice=%v", p["tool_choice"])
				}
				_, hasParallel := p["parallel_tool_calls"]
				if hasParallel != tc.parallel {
					t.Errorf("parallel flag missing/misapplied")
				}
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
			}))
			defer server.Close()
			cfg := testOpenAIConfig()
			cfg.LLM.BaseURL = server.URL
			cfg.LLM.ToolCallMode = tc.mode
			cfg.Execution.ToolPolicy = tc.policy
			_, err := (OpenAIClient{Config: cfg}).Decide(context.Background(), nil)
			if err != nil || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
	for _, status := range []int{http.StatusNotFound, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			primary, alternate := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/v1/") {
					primary++
					w.WriteHeader(status)
					_, _ = w.Write([]byte("secret provider detail"))
					return
				}
				alternate++
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
			}))
			defer server.Close()
			cfg := testOpenAIConfig()
			cfg.LLM.BaseURL = server.URL + "/v1"
			diagnostics := &ModelDiagnostics{}
			_, err := (OpenAIClient{Config: cfg, Diagnostics: diagnostics}).Decide(context.Background(), nil)
			if len(diagnostics.Snapshot()) != primary+alternate {
				t.Fatalf("diagnostics do not count actual attempts: %#v", diagnostics.Snapshot())
			}
			if status == http.StatusNotFound {
				if err != nil || primary != 1 || alternate != 1 {
					t.Fatalf("404 fallback counts %d/%d err=%v", primary, alternate, err)
				}
			} else if err == nil || primary != 1 || alternate != 0 || strings.Contains(err.Error(), "secret") || !strings.Contains(err.Error(), "model_http_error") {
				t.Fatalf("non-404 behavior counts %d/%d err=%v", primary, alternate, err)
			}
		})
	}
}

func TestDU02SingleWirePairsKeepBatchIdentityAndOrder(t *testing.T) {
	a, b := "one", "two"
	history := []Message{{Role: "assistant", ToolCalls: []map[string]any{
		{"id": "one", "type": "function", "function": map[string]any{"name": "lookup", "arguments": "{\"q\":1}"}},
		{"id": "two", "type": "function", "function": map[string]any{"name": "lookup", "arguments": "{\"q\":2}"}},
	}}, {Role: "tool", Name: str("lookup"), ToolCallID: &a, Content: str("first result")}, {Role: "tool", Name: str("lookup"), ToolCallID: &b, Content: str("second result")}}
	wire, err := singleCallWire(history)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != 4 {
		t.Fatalf("expected two assistant/result pairs, got %#v", wire)
	}
	for i, want := range []string{"one", "two"} {
		var calls []map[string]any
		raw, _ := json.Marshal(wire[i*2].ToolCalls)
		_ = json.Unmarshal(raw, &calls)
		if len(calls) != 1 || calls[0]["id"] != want {
			t.Fatalf("pair %d call order/identity: %#v", i, calls)
		}
		if wire[i*2+1].ToolCallID == nil || *wire[i*2+1].ToolCallID != want {
			t.Fatalf("pair %d result correlation lost: %#v", i, wire[i*2+1])
		}
	}
}

func TestDU02NonJSONAndTimeoutDiagnosticsAreBounded(t *testing.T) {
	t.Run("non_json", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("provider secret body")) }))
		defer server.Close()
		cfg := testOpenAIConfig()
		cfg.LLM.BaseURL = server.URL
		diag := &ModelDiagnostics{}
		_, err := (OpenAIClient{Config: cfg, Diagnostics: diag}).Decide(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "invalid_model_response") || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unexpected diagnostic: %v", err)
		}
		if len(diag.Snapshot()) != 1 || diag.Snapshot()[0].Failure != "invalid_model_response" {
			t.Fatalf("request diagnostics: %#v", diag.Snapshot())
		}
	})
	t.Run("timeout", func(t *testing.T) {
		cfg := testOpenAIConfig()
		cfg.LLM.BaseURL = "http://model.test"
		diag := &ModelDiagnostics{}
		client := OpenAIClient{Config: cfg, HTTP: &http.Client{Timeout: 5 * time.Millisecond, Transport: du02BlockingTransport{}}, Diagnostics: diag}
		_, err := client.Decide(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "model_timeout") {
			t.Fatalf("expected bounded timeout category, got %v", err)
		}
		if got := diag.Snapshot(); len(got) != 1 || got[0].Failure != "model_timeout" {
			t.Fatalf("timeout diagnostics: %#v", got)
		}
	})
	t.Run("transport", func(t *testing.T) {
		cfg := testOpenAIConfig()
		cfg.LLM.BaseURL = "http://127.0.0.1:1"
		diag := &ModelDiagnostics{}
		_, err := (OpenAIClient{Config: cfg, HTTP: &http.Client{Timeout: 100 * time.Millisecond}, Diagnostics: diag}).Decide(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "model_transport_error") {
			t.Fatalf("expected transport category, got %v", err)
		}
		if len(diag.Snapshot()) != 1 || diag.Snapshot()[0].Failure != "model_transport_error" {
			t.Fatalf("transport diagnostics %#v", diag.Snapshot())
		}
	})
	t.Run("template", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("private template failure detail"))
		}))
		defer server.Close()
		cfg := testOpenAIConfig()
		cfg.LLM.BaseURL = server.URL
		_, err := (OpenAIClient{Config: cfg}).Decide(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "model_template_error") || strings.Contains(err.Error(), "private") {
			t.Fatalf("template diagnostics not bounded: %v", err)
		}
	})
}

func TestDU02EngineRunsValidBatchCallsAndStoresEveryResult(t *testing.T) {
	cfg := testOpenAIConfig()
	cfg.Memory.MaxSteps = 4
	cfg.Memory.MaxTokens = 100000
	cfg.Memory.RawTurnsToKeep = 2
	cfg.Execution.MaxConsecutiveInvalidResponses = 3
	client := &du02SequenceClient{decisions: []Decision{{Calls: []ToolCall{{ID: "bad", Name: "lookup", RawArguments: "{bad", Invalid: "invalid JSON"}, {ID: "good", Name: "lookup", RawArguments: `{"q":"ok"}`, Arguments: map[string]any{"q": "ok"}}}}, {Final: true, Content: "done"}}}
	invocations := 0
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(_ context.Context, args map[string]any) (string, error) {
		invocations++
		return `{"found":true}`, nil
	}}}
	s := e.Create("s", "goal", "ref", "hash")
	if err := e.Run(context.Background(), &s, 2, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "complete" || s.RunStatus != "complete" || invocations != 1 {
		t.Fatalf("status=%s run_status=%s invocations=%d", s.Status, s.RunStatus, invocations)
	}
	ids := map[string]bool{}
	for _, m := range s.Memory {
		if m.Role == "tool" && m.ToolCallID != nil {
			ids[*m.ToolCallID] = true
		}
	}
	if !ids["bad"] || !ids["good"] {
		t.Fatalf("each batch call needs a result: %#v", s.Memory)
	}
}

func TestDU02RequestDiagnosticsPreserveKnownAndUnknownUsage(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		known      bool
	}{{"known", `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`, true}, {"unknown", `{"choices":[{"message":{"content":"ok"}}]}`, false}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			defer server.Close()
			cfg := testOpenAIConfig()
			cfg.LLM.BaseURL = server.URL
			diag := &ModelDiagnostics{}
			_, err := (OpenAIClient{Config: cfg, Diagnostics: diag}).Decide(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			got := diag.Snapshot()
			if len(got) != 1 || got[0].RequestBytes == 0 || got[0].ElapsedMS < 0 || got[0].MaxOutputTokens != 1024 || got[0].UsageKnown != tc.known {
				t.Fatalf("unexpected request diagnostic: %#v", got)
			}
			if tc.known && (got[0].PromptTokens == nil || *got[0].PromptTokens != 11 || got[0].TotalTokens == nil || *got[0].TotalTokens != 18) {
				t.Fatalf("usage fields lost: %#v", got[0])
			}
		})
	}
}

func TestDU02InvalidResponseCorrectionIsBoundedAcrossRunResume(t *testing.T) {
	cfg := testOpenAIConfig()
	cfg.Memory.MaxSteps = 5
	cfg.Memory.MaxTokens = 100000
	cfg.Memory.RawTurnsToKeep = 1
	cfg.Execution.MaxConsecutiveInvalidResponses = 2
	client := &du02SequenceClient{decisions: []Decision{{Invalid: "empty_model_response: supply content"}, {Invalid: "invalid_model_response: missing choices"}}}
	e := Engine{Config: cfg, Client: client}
	s := e.Create("s", "goal", "ref", "hash")
	if err := e.Run(context.Background(), &s, 1, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "failed" || s.RunStatus != "failed" || s.ConsecutiveInvalidResponses != 1 {
		t.Fatalf("first invalid response state: %#v", s)
	}
	s.Status = "active"
	if err := e.Run(context.Background(), &s, 1, nil); err != nil {
		t.Fatal(err)
	}
	if s.Status != "failed" || s.RunStatus != "failed" || s.ConsecutiveInvalidResponses != 2 || client.index != 2 {
		t.Fatalf("invalid-response limit not retained: state=%#v decisions=%d", s, client.index)
	}
}
