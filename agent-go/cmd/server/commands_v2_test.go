package main

import (
	"encoding/json"
	"github.com/state-driven-agent/agent-go/internal/agent"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestV2CommandNormalizationAndStrictAdmission(t *testing.T) {
	cases := []struct {
		route, raw string
		valid      bool
	}{
		{"/sessions", `{"user_prompt":"goal"}`, true},
		{"/sessions", `{"user_prompt":"goal","user_prompt":"changed"}`, false},
		{"/sessions", `{"user_prompt":"goal","unknown":1}`, false},
		{"/sessions", `{"user_prompt":"goal","config_ref":null}`, false},
		{"/sessions/{session_id}/run", `{}`, true},
		{"/sessions/{session_id}/run", `{"max_steps":0}`, false},
		{"/sessions/{session_id}/resume", `{"content":null}`, true},
		{"/sessions/{session_id}/cancel", `{}`, false},
		{"/sessions/{session_id}/cancel", `{"run_id":"11111111-1111-4111-8111-111111111111"}`, true},
	}
	for _, c := range cases {
		t.Run(c.route+c.raw, func(t *testing.T) {
			_, err := normalizedCommand(c.route, []byte(c.raw))
			if (err == nil) != c.valid {
				t.Fatalf("valid=%v error=%v", c.valid, err)
			}
		})
	}
	a, err := normalizedCommand("/sessions/{session_id}/resume", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := a["content"]; !ok || v != nil {
		t.Fatal("missing static resume content default")
	}
	if v, ok := a["max_steps"]; !ok || v != nil {
		t.Fatal("missing static step default")
	}
}
func TestV2TerminalReplayPreservesOriginalResponse(t *testing.T) {
	original := map[string]any{"session": map[string]any{"id": "original", "revision": json.Number("4")}, "message": "done"}
	row := map[string]any{"command_id": "command-1", "status": "failed", "response_status": float64(409), "response_body": original}
	w := httptest.NewRecorder()
	commandReplay(w, row)
	if w.Code != 409 || w.Header().Get("Idempotency-Replayed") != "true" || w.Header().Get("Idempotency-Command-Id") != "command-1" {
		t.Fatalf("replay status/headers: %d %v", w.Code, w.Header())
	}
	var body any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	actual, _ := agent.CanonicalV2(body)
	expected, _ := agent.CanonicalV2(original)
	if string(actual) != string(expected) {
		t.Fatal("replay changed original response")
	}
	for _, status := range []string{"pending", "started", "uncertain", "blocked"} {
		row["status"] = status
		w = httptest.NewRecorder()
		commandReplay(w, row)
		if w.Code != 202 || w.Header().Get("Retry-After") != "1" {
			t.Fatalf("unresolved replay status %s: %d", status, w.Code)
		}
	}
}
func TestV2KeyValidationPrecedesDatabaseAccess(t *testing.T) {
	s := &server{}
	for _, key := range []string{"", "bad key", "\n", string(make([]byte, 256))} {
		r := httptest.NewRequest("POST", "/sessions", nil)
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		called := false
		s.commandHandler("/sessions", func(_ http.ResponseWriter, _ *http.Request) { called = true })(w, r)
		if called || w.Code != 422 {
			t.Fatalf("invalid key reached handler: %q", key)
		}
	}
}

func TestV2ExplicitEmptyKeyIsInvalidRatherThanMissing(t *testing.T) {
	s := &server{}
	r := httptest.NewRequest("POST", "/sessions", nil)
	r.Header["Idempotency-Key"] = []string{""}
	w := httptest.NewRecorder()
	s.commandHandler("/sessions", func(http.ResponseWriter, *http.Request) { t.Fatal("empty key reached handler") })(w, r)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["code"] != "idempotency_key_invalid" {
		t.Fatalf("wrong empty key error: %v", body)
	}
}
