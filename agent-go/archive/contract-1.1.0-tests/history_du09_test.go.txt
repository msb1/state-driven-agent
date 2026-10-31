package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

func du09Request(t *testing.T, handler http.HandlerFunc, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if id, _, ok := strings.Cut(strings.TrimPrefix(path, "/sessions/"), "/"); ok {
		req.SetPathValue("session_id", id)
	}
	if strings.Contains(path, "/diagnostics/") {
		req.SetPathValue("kind", strings.Split(strings.Split(path, "/diagnostics/")[1], "?")[0])
	}
	response := httptest.NewRecorder()
	handler(response, req)
	var value map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &value)
	return response.Code, value
}

func TestDU09C01HydrationPreservesActiveDecisionPayload(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	beforeMemory, err := json.Marshal(p.State.Memory)
	if err != nil {
		t.Fatal(err)
	}
	identity, revision := p.State.ResolvedConfigSHA256, p.State.Revision
	loaded, err := s.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterMemory, err := json.Marshal(loaded.State.Memory)
	if err != nil {
		t.Fatal(err)
	}
	var loadedConfig agent.Config
	if err = json.Unmarshal(loaded.Config, &loadedConfig); err != nil {
		t.Fatal(err)
	}
	loadedIdentity, err := agent.ConfigIdentity(loadedConfig)
	if err != nil {
		t.Fatal(err)
	}
	if string(beforeMemory) != string(afterMemory) || loadedIdentity != identity || loaded.State.ResolvedConfigSHA256 != identity || loaded.State.Revision != revision {
		t.Fatalf("selective hydration changed decision inputs or identity: before=%s after=%s config_identity=%s/%s revision=%d/%d", beforeMemory, afterMemory, identity, loadedIdentity, revision, loaded.State.Revision)
	}
}

func TestDU09C02SelectiveHydrationKeepsActivePayloadAndBoundsHistory(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	goal, rawCount := p.State.Goal, len(p.State.Memory)
	for i := 0; i < 150; i++ {
		agent.ArchiveRecord(&p.State, "model_decision", map[string]any{"decision": i, "old_output": strings.Repeat("x", 128)})
	}
	historicalCallID := "du09-historical-call"
	agent.ArchiveRecord(&p.State, "message", map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": historicalCallID, "type": "function", "function": map[string]any{"name": "lookup", "arguments": "{}"}}}})
	for i := 0; i < 40; i++ {
		agent.RecordDiagnostic(&p.State, "compaction_events", fmt.Sprintf("event-%d", i))
		agent.RecordDiagnostic(&p.State, "compaction_attempts", map[string]any{"attempt": i})
		agent.RecordDiagnostic(&p.State, "compaction_decisions", map[string]any{"step": i})
	}
	if err := s.save(ctx, &p, map[string]any{"type": "du09_seed_history"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Goal != goal || len(loaded.State.Memory) != rawCount || len(loaded.State.CompactionEvents) != 32 || len(loaded.State.CompactionAttempts) != 32 || len(loaded.State.CompactionDecisions) != 32 {
		t.Fatalf("hydrated active state goal=%q memory=%d diagnostic tails=(%d,%d,%d)", loaded.State.Goal, len(loaded.State.Memory), len(loaded.State.CompactionEvents), len(loaded.State.CompactionAttempts), len(loaded.State.CompactionDecisions))
	}
	for _, family := range []string{"compaction_events", "compaction_attempts", "compaction_decisions"} {
		if loaded.State.DiagnosticCounts[family] != 40 {
			t.Errorf("family %s lifetime count=%d want 40", family, loaded.State.DiagnosticCounts[family])
		}
	}
	var transcriptRows int
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_session_transcript WHERE session_id=$1`, p.State.ID).Scan(&transcriptRows); err != nil || transcriptRows < 151 {
		t.Fatalf("archived history rows=%d err=%v", transcriptRows, err)
	}
	var callMapRows int
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_session_calls WHERE session_id=$1 AND call_id=$2`, p.State.ID, historicalCallID).Scan(&callMapRows); err != nil || callMapRows != 1 {
		t.Fatalf("normalized historical call mapping rows=%d err=%v", callMapRows, err)
	}
	stateJSON, err := json.Marshal(loaded.State)
	if err != nil {
		t.Fatal(err)
	}
	var public map[string]any
	if err = json.Unmarshal(stateJSON, &public); err != nil {
		t.Fatal(err)
	}
	if _, ok := public["executions"]; ok {
		t.Fatal("normal SessionState response exposed the growing execution archive")
	}
}

func TestDU09C03PagesValidateCursorsAndReturnEmptyArrayPages(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	for i := 0; i < 3; i++ {
		if err := s.save(ctx, &p, map[string]any{"type": "page_event", "ordinal": i}); err != nil {
			t.Fatal(err)
		}
	}
	archiveID := uuid.NewString()
	if _, err := s.db.Exec(ctx, `INSERT INTO agent_archive_receipts(archive_id,session_id,snapshot_revision,sha256,manifest) VALUES($1,$2,1,'abc','{}')`, archiveID, p.State.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(ctx, `INSERT INTO agent_runs(run_id,session_id,owner,fence,status,started_at,last_decision_step,metadata) VALUES($1,$2,$3,1,'complete',$4,1,'{}')`, uuid.NewString(), p.State.ID, uuid.NewString(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		agent.RecordDiagnostic(&p.State, "compaction_decisions", map[string]any{"ordinal": i})
	}
	if err := s.save(ctx, &p, map[string]any{"type": "seed_diagnostics"}); err != nil {
		t.Fatal(err)
	}
	status, transcript := du09Request(t, s.transcript, "/sessions/"+p.State.ID+"/transcript?after_sequence=0&limit=1")
	if status != http.StatusOK || len(transcript["entries"].([]any)) != 1 || transcript["next_after_sequence"] == nil || transcript["snapshot_revision"] == nil {
		t.Fatalf("transcript page status=%d body=%v", status, transcript)
	}
	status, emptyTranscript := du09Request(t, s.transcript, "/sessions/"+p.State.ID+"/transcript?after_sequence=999999&limit=1")
	if status != http.StatusOK || len(emptyTranscript["entries"].([]any)) != 0 || int(emptyTranscript["next_after_sequence"].(float64)) != 999999 {
		t.Fatalf("empty transcript page status=%d body=%v", status, emptyTranscript)
	}
	status, events := du09Request(t, s.events, "/sessions/"+p.State.ID+"/events?limit=1&through=999999")
	if status != http.StatusOK || events["through"] == nil || events["entries"] == nil || events["next_after"] == nil {
		t.Fatalf("event page status=%d body=%v", status, events)
	}
	firstEntries := events["entries"].([]any)
	firstEventID := int64(firstEntries[0].(map[string]any)["event_id"].(float64))
	watermark := int64(events["through"].(float64))
	if err := s.save(ctx, &p, map[string]any{"type": "late_page_event"}); err != nil {
		t.Fatal(err)
	}
	secondStatus, second := du09Request(t, s.events, fmt.Sprintf("/sessions/%s/events?after=%d&through=%d&limit=100", p.State.ID, firstEventID, watermark))
	if secondStatus != http.StatusOK || int64(second["through"].(float64)) != watermark {
		t.Fatalf("fixed-watermark page status=%d body=%v", secondStatus, second)
	}
	for _, entry := range second["entries"].([]any) {
		if entry.(map[string]any)["event"].(map[string]any)["type"] == "late_page_event" {
			t.Fatal("event appended after watermark leaked into the fixed page set")
		}
	}
	status, runs := du09Request(t, s.runs, "/sessions/"+p.State.ID+"/runs?limit=1000")
	if status != http.StatusOK || len(runs["runs"].([]any)) != 1 || runs["next_after"] == nil {
		t.Fatalf("run page status=%d body=%v", status, runs)
	}
	status, archives := du09Request(t, s.archives, "/sessions/"+p.State.ID+"/archives?limit=1")
	if status != http.StatusOK || len(archives["archives"].([]any)) != 1 {
		t.Fatalf("archive page status=%d body=%v", status, archives)
	}
	status, empty := du09Request(t, s.archives, "/sessions/"+p.State.ID+"/archives?after=ffffffff-ffff-ffff-ffff-ffffffffffff")
	if status != http.StatusOK || len(empty["archives"].([]any)) != 0 || empty["next_after"] == nil {
		t.Fatalf("empty archive page status=%d body=%v", status, empty)
	}
	for _, path := range []string{"/sessions/not-a-uuid/events", "/sessions/" + p.State.ID + "/events?after=-1", "/sessions/" + p.State.ID + "/archives?after=bad", "/sessions/" + p.State.ID + "/archives?limit=1001"} {
		handler := s.events
		if strings.Contains(path, "/archives") {
			handler = s.archives
		}
		code, _ := du09Request(t, handler, path)
		if code != http.StatusUnprocessableEntity {
			t.Errorf("%s returned %d, want 422", path, code)
		}
	}
	status, _ = du09Request(t, s.events, "/sessions/"+uuid.NewString()+"/events")
	if status != http.StatusNotFound {
		t.Errorf("absent valid session returned %d, want 404", status)
	}
	status, retention := du09Request(t, s.retention, "/sessions/"+p.State.ID+"/retention")
	if status != http.StatusOK || retention["default_policy"] != "keep_all" || retention["eligible"] != false {
		t.Fatalf("retention status=%d body=%v", status, retention)
	}
	status, executions := du09Request(t, s.executions, "/sessions/"+p.State.ID+"/executions?limit=1000")
	if status != http.StatusOK || len(executions["executions"].([]any)) != 0 || executions["next_after"] != nil {
		t.Fatalf("empty execution page status=%d body=%v", status, executions)
	}
	executionCursor := uuid.NewString()
	status, emptyExecutions := du09Request(t, s.executions, "/sessions/"+p.State.ID+"/executions?after="+executionCursor+"&limit=1")
	if status != http.StatusOK || len(emptyExecutions["executions"].([]any)) != 0 || emptyExecutions["next_after"] != executionCursor {
		t.Fatalf("empty cursor execution page status=%d body=%v", status, emptyExecutions)
	}
	status, diagnosticPage := du09Request(t, s.compactionDiagnostics, "/sessions/"+p.State.ID+"/diagnostics/compaction_decisions?after=0&limit=1")
	if status != http.StatusOK || len(diagnosticPage["entries"].([]any)) != 1 || diagnosticPage["total_recorded"] != float64(3) {
		t.Fatalf("diagnostic page status=%d body=%v", status, diagnosticPage)
	}
	status, diagnostics := du09Request(t, s.compactionDiagnostics, "/sessions/"+p.State.ID+"/diagnostics/invalid")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("unknown diagnostic family status=%d want 422 body=%v", status, diagnostics)
	}
	status, _ = du09Request(t, s.runs, "/sessions/"+p.State.ID+"/runs?after=bad")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("malformed run cursor status=%d want 422", status)
	}
	status, _ = du09Request(t, s.executions, "/sessions/"+p.State.ID+"/executions?after=bad")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("malformed execution cursor status=%d want 422", status)
	}
}

func TestDU09C03AbandonedRouteIsBoundedAndNonMutating(t *testing.T) {
	s, _ := du05Repository(t)
	code, response := du09Request(t, s.abandoned, "/runs/abandoned?limit=1")
	if code != http.StatusOK || response["runs"] == nil || len(response["runs"].([]any)) > 1 {
		t.Fatalf("abandoned response status=%d body=%v", code, response)
	}
	code, _ = du09Request(t, s.abandoned, "/runs/abandoned?limit=0")
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid abandoned limit status=%d want 422", code)
	}
}
