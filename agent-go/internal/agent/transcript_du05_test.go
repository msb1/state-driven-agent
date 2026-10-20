package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestDU05C01StableIdentitySharedSequenceAndDetachedRecords(t *testing.T) {
	s := NewSession(uuid.NewString(), "original goal", "profile.yaml", "hash")
	goal := Clone(s.Memory[0])
	payload := map[string]any{"arguments": map[string]any{"ids": []any{1, 2}}}
	record := ArchiveRecord(&s, "model_decision", payload)
	payload["arguments"].(map[string]any)["ids"].([]any)[0] = 999
	record.Payload["arguments"].(map[string]any)["ids"].([]any)[1] = 888
	AppendMessage(&s, Message{Role: "assistant", Content: str("answer")})
	StageMessages(&s)
	if len(s.TranscriptPending) != 3 || *s.Memory[0].Sequence != 1 || s.TranscriptPending[1].Sequence != 2 || *s.Memory[1].Sequence != 3 || s.NextTranscriptSequence != 4 {
		t.Fatalf("shared sequence did not advance once per record: %+v", s)
	}
	if s.Memory[0].MessageID != goal.MessageID || !s.Memory[0].CreatedAt.Equal(goal.CreatedAt) {
		t.Fatal("goal identity changed")
	}
	ids := s.TranscriptPending[1].Payload["arguments"].(map[string]any)["ids"].([]any)
	if ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("staged record mutated: %v", ids)
	}
	seen := map[string]bool{}
	for _, entry := range s.TranscriptPending {
		if _, err := uuid.Parse(entry.MessageID); err != nil || seen[entry.MessageID] || entry.CreatedAt.Location().String() != "UTC" {
			t.Fatalf("invalid identity/timestamp: %+v", entry)
		}
		seen[entry.MessageID] = true
	}
}
func TestDU05C01PublicStateAndWireAreSeparate(t *testing.T) {
	s := NewSession(uuid.NewString(), "goal", "profile.yaml", "hash")
	owner := uuid.NewString()
	s.ExecutionOwner = &owner
	s.PendingCommitID = &owner
	s.ExecutionPending = []map[string]any{{"private": "execution"}}
	public, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err = json.Unmarshal(public, &state); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"revision", "next_transcript_sequence", "run_id", "active_ledger_id", "final_answer", "last_error", "ledger_state", "request_budget", "workflow", "last_compaction_attempt_step"} {
		if _, ok := state[key]; !ok {
			t.Fatalf("missing public field %s", key)
		}
	}
	for _, key := range []string{"run_id", "active_ledger_id", "final_answer", "last_error", "ledger_state", "last_compaction_attempt_step"} {
		if state[key] != nil {
			t.Fatalf("expected nullable default for %s: %v", key, state[key])
		}
	}
	for _, key := range []string{"execution_owner", "execution_pending", "diagnostics_pending"} {
		if _, ok := state[key]; ok {
			t.Fatalf("private field exposed: %s", key)
		}
	}
	if state["pending_commit_id"] != nil || len(state["transcript_pending"].([]any)) != 0 || strings.Contains(string(public), owner) {
		t.Fatal("private outbox content exposed")
	}
	message := state["memory"].([]any)[0].(map[string]any)
	for _, key := range []string{"message_id", "sequence", "created_at", "content", "name", "tool_calls", "tool_call_id"} {
		if _, ok := message[key]; !ok {
			t.Fatalf("missing message field %s", key)
		}
	}
	payload, err := (OpenAIClient{Config: Config{}}).decisionPayload(s.Memory)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := compactJSON(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"message_id", "sequence", "created_at", "execution_owner"} {
		if strings.Contains(string(wire), key) {
			t.Fatalf("persistence field in wire: %s", wire)
		}
	}
	if !strings.Contains(string(wire), `"messages":[{"role":"user","content":"goal"}]`) {
		t.Fatalf("unexpected wire messages: %s", wire)
	}
	hot, err := s.HotSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"memory", "compacted_context", "ledger_state", "compaction_attempts", "compaction_events", "compaction_decisions"} {
		if _, ok := hot[key]; ok {
			t.Fatalf("growing context present in hot snapshot: %s", key)
		}
	}
}

func TestDU05C01ConfigIdentityAcceptsZeroValueConfig(t *testing.T) {
	if _, err := ConfigIdentity(Config{}); err != nil {
		t.Fatalf("zero value config identity: %v", err)
	}
}

func TestDU05C01DiagnosticsKeepRecentTailAndLifetimeOutbox(t *testing.T) {
	s := NewSession(uuid.NewString(), "goal", "profile.yaml", "hash")
	for i := 0; i < 40; i++ {
		payload := map[string]any{"accepted": false, "nested": map[string]any{"value": i}}
		RecordDiagnostic(&s, "compaction_attempts", payload)
		payload["nested"].(map[string]any)["value"] = -1
	}
	if len(s.CompactionAttempts) != 32 || len(s.DiagnosticsPending) != 40 || s.DiagnosticCounts["compaction_attempts"] != 40 || s.DiagnosticRejections != 40 || s.DiagnosticsComplete {
		t.Fatalf("bad counters/tails: %+v", s)
	}
	if s.CompactionAttempts[0]["nested"].(map[string]any)["value"] != 8 || s.DiagnosticsPending[0].Payload.(map[string]any)["nested"].(map[string]any)["value"] != 0 {
		t.Fatal("diagnostic mutation or wrong tail")
	}
	for i, entry := range s.DiagnosticsPending {
		if entry.Ordinal != int64(i+1) {
			t.Fatal("diagnostic ordinal gap")
		}
	}
}
func TestDU05C01EngineJournalsRejectionsToolsAndRunBoundaries(t *testing.T) {
	cfg := Config{SystemPrompt: "system", Execution: Execution{ToolPolicy: "auto", MaxConsecutiveInvalidResponses: 3}, Tools: []ToolDefinition{{Name: "lookup", Parameters: map[string]any{"type": "object"}}}}
	exact := "done"
	cfg.Completion.Exact = &exact
	client := &du02SequenceClient{decisions: []Decision{{Invalid: "invalid_model_response: bad"}, {Final: true, Content: "wrong"}, {Calls: []ToolCall{{ID: "call", Name: "lookup", Arguments: map[string]any{}}}}, {Final: true, Content: "done"}}}
	e := Engine{Config: cfg, Client: client, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) { return "known result", nil }}}
	s := e.Create(uuid.NewString(), "goal", "profile.yaml", "hash")
	if err := e.Run(context.Background(), &s, 5, nil); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, entry := range s.TranscriptPending {
		kinds[entry.Kind]++
	}
	for _, kind := range []string{"run_started", "run_finished", "model_decision", "decision_rejected", "rejected_final", "tool_intent", "tool_attempt", "tool_outcome"} {
		if kinds[kind] == 0 {
			t.Fatalf("missing audit kind %s: %v", kind, kinds)
		}
	}
	if s.RunID == nil || s.RunStartedAt == nil || s.RunEndedAt == nil || s.RunStatus != "complete" || len(s.PendingToolCalls) != 0 {
		t.Fatalf("missing terminal run fields: %+v", s)
	}
	if int64(len(s.TranscriptPending)+1) != s.NextTranscriptSequence {
		t.Fatal("journal not contiguous")
	}
}
func TestDU05C01SummaryKeepsGoalIdentityAndArchivesRemovedMessages(t *testing.T) {
	cfg := Config{SystemPrompt: "system", Memory: Memory{ContextWindowTokens: 100000, MaxTokens: 1, RawTurnsToKeep: 1, MaxSummaryTokens: 4096, HybridReflection: true}}
	client := &du04Client{proposal: "<COMPACTED_STATE><USER_GOAL>goal</USER_GOAL><GLOBAL_LESSON_LEDGER>fact</GLOBAL_LESSON_LEDGER><DEAD_ENDS>None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>finish</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>"}
	s := NewSession(uuid.NewString(), "goal", "profile.yaml", "hash")
	for i := 0; i < 4; i++ {
		AppendMessage(&s, Message{Role: "assistant", Content: str(strings.Repeat("fact ", 1000))})
	}
	goalID := s.Memory[0].MessageID
	old := Clone(s.Memory)
	before := s.NextTranscriptSequence
	if err := (Engine{Config: cfg, Client: client}).compact(context.Background(), &s, nil); err != nil {
		t.Fatal(err)
	}
	if s.ActiveLedgerID == nil || s.Memory[0].MessageID != goalID || len(s.Memory) >= len(old) {
		t.Fatalf("compaction lost identity or did not compact: %+v", s)
	}
	if s.NextTranscriptSequence != before+1 {
		t.Fatal("summary did not occupy exactly one shared sequence")
	}
	archived := map[string]bool{}
	for _, entry := range s.TranscriptPending {
		archived[entry.MessageID] = true
	}
	for _, m := range old {
		if !archived[m.MessageID] {
			t.Fatal("removed message lost from transcript")
		}
	}
	if s.Memory[1].MessageID != old[len(old)-1].MessageID || *s.Memory[1].Sequence != *old[len(old)-1].Sequence {
		t.Fatal("retained raw message renumbered")
	}
}

func TestDU05C01StreamFailureJournalsCompleteBatchAndInterruption(t *testing.T) {
	cfg := Config{SystemPrompt: "system", Memory: Memory{ContextWindowTokens: 100000}, Execution: Execution{ToolPolicy: "auto"}, Tools: []ToolDefinition{{Name: "lookup", Parameters: map[string]any{"type": "object"}}}}
	engine := Engine{Config: cfg, Client: &du04BatchClient{}, Tools: map[string]Tool{"lookup": func(context.Context, map[string]any) (string, error) { return "known result", nil }}}
	s := engine.Create(uuid.NewString(), "goal", "profile.yaml", "hash")
	err := engine.Run(context.Background(), &s, 3, func(event map[string]any) error {
		if event["step"] == "tool_completed" {
			return fmt.Errorf("closed reader")
		}
		return nil
	})
	if err == nil || s.RunStatus != "interrupted" || len(s.PendingToolCalls) != 0 {
		t.Fatal("stream failure did not preserve settled batch and interrupted run")
	}
	if s.TranscriptPending[len(s.TranscriptPending)-1].Kind != "run_interrupted" {
		t.Fatal("missing run interruption record")
	}
	if _, err := interactionGroups(s.Memory); err != nil {
		t.Fatalf("interrupted batch lost correlation: %v", err)
	}
}
func TestDU05C01NumericArtifactsSurvivePublicRoundTrip(t *testing.T) {
	s := NewSession(uuid.NewString(), "goal", "profile.yaml", "hash")
	s.CompletionArtifacts["0"] = CompletionArtifact{Value: json.Number("9007199254740993")}
	payload, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored Session
	if err = json.Unmarshal(payload, &restored); err != nil {
		t.Fatal(err)
	}
	if numberString(restored.CompletionArtifacts["0"].Value) != "9007199254740993" {
		t.Fatalf("numeric artifact rounded: %s", payload)
	}
}
