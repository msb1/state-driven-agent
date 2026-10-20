package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

// SQL tests use a new, automatically removed schema and never production tables.
func du05Repository(t *testing.T) (*server, context.Context) {
	t.Helper()
	if err := loadDotEnv(); err != nil {
		t.Fatalf("load repository .env: %v", err)
	}
	dsn := env("DU05_TEST_DATABASE_URL", env("AGENT_SESSION_DATABASE_URL", "postgresql://user:password@192.168.1.50:5432/universal_state"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "du05_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	var pool *pgxpool.Pool
	t.Cleanup(func() {
		if pool != nil {
			pool.Close()
		}
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, e := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); e != nil {
			t.Errorf("cleanup test schema %s: %v", schema, e)
		}
		admin.Close()
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err = pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := &server{db: pool, fixtureDB: pool}
	if err = s.schema(); err != nil {
		t.Fatal(err)
	}
	t.Logf("isolated schema: %s (removed after test)", schema)
	return s, ctx
}
func du05Config() agent.Config {
	return agent.Config{Name: "du05-controlled", Model: "scripted", SystemPrompt: "system", LLM: agent.LLM{ToolCallMode: "native"}, Memory: agent.Memory{ContextWindowTokens: 100000, MaxTokens: 6000, RawTurnsToKeep: 1, MaxSummaryTokens: 4096, HybridReflection: true, RunMaxSteps: du05Int(1)}, Execution: agent.Execution{ToolPolicy: "auto", MaxConsecutiveInvalidResponses: 3}, Tools: []agent.ToolDefinition{{Name: "lookup", Parameters: map[string]any{"type": "object"}}}}
}
func du05Int(v int) *int { return &v }
func du05Create(t *testing.T, s *server, ctx context.Context) persisted {
	t.Helper()
	cfg := du05Config()
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := persisted{State: agent.NewSession(uuid.NewString(), "goal", "du05-controlled.yaml", "source-hash"), Config: b}
	if err = s.save(ctx, &p, map[string]any{"type": "session_created"}); err != nil {
		t.Fatal(err)
	}
	if p.State.Revision != 1 || *p.State.Memory[0].Sequence != 1 {
		t.Fatal("creation revision/goal sequence")
	}
	return p
}
func du05Rows(t *testing.T, s *server, ctx context.Context, id string) []byte {
	t.Helper()
	var b []byte
	err := s.db.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY sequence),'[]'::jsonb) FROM agent_session_transcript t WHERE session_id=$1`, id).Scan(&b)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func du05Prefix(t *testing.T, before, after []byte) {
	t.Helper()
	var a, b []any
	if json.Unmarshal(before, &a) != nil || json.Unmarshal(after, &b) != nil {
		t.Fatal("invalid transcript")
	}
	if len(a) > len(b) {
		t.Fatal("transcript shrank")
	}
	for i := range a {
		if !equalJSON(a[i], b[i]) {
			t.Fatalf("immutable prefix changed at row %d", i+1)
		}
	}
}

type du05Script struct{ decisions int }

func (c *du05Script) Decide(context.Context, []agent.Message) (agent.Decision, error) {
	c.decisions++
	if c.decisions == 1 {
		return agent.Decision{Calls: []agent.ToolCall{{ID: "lookup-call", Name: "lookup", Arguments: map[string]any{}}}}, nil
	}
	return agent.Decision{Final: true, Content: "done"}, nil
}
func (*du05Script) Compact(context.Context, string, []agent.Message, *string, bool) (string, error) {
	return "<COMPACTED_STATE><USER_GOAL>goal</USER_GOAL><GLOBAL_LESSON_LEDGER>fact</GLOBAL_LESSON_LEDGER><DEAD_ENDS>None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>finish</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>", nil
}
func TestDU05C02PostgresAppendCompactStopResumeAndReload(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	initial := du05Rows(t, s, ctx, p.State.ID)
	goal := agent.Clone(p.State.Memory[0])
	for i := 0; i < 4; i++ {
		content := strings.Repeat("fact ", 1000)
		agent.AppendMessage(&p.State, agent.Message{Role: "assistant", Content: &content})
	}
	if err := s.save(ctx, &p, nil); err != nil {
		t.Fatal(err)
	}
	before := du05Rows(t, s, ctx, p.State.ID)
	var cfg agent.Config
	if err := json.Unmarshal(p.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	client := &du05Script{}
	e := agent.Engine{Config: cfg, Client: client, Tools: map[string]agent.Tool{"lookup": func(context.Context, map[string]any) (string, error) { return "fact", nil }}}
	if err := e.Run(ctx, &p.State, 1, nil); err != nil {
		t.Fatal(err)
	}
	if p.State.Status != "failed" || p.State.ActiveLedgerID == nil {
		t.Fatalf("did not exercise bounded stop and compaction: status=%s ledger=%v", p.State.Status, p.State.ActiveLedgerID)
	}
	firstRun := *p.State.RunID
	if err := s.save(ctx, &p, nil); err != nil {
		t.Fatal(err)
	}
	stopped := du05Rows(t, s, ctx, p.State.ID)
	du05Prefix(t, before, stopped)
	cfgPool := s.db.Config().Copy()
	reopened, err := pgxpool.NewWithConfig(ctx, cfgPool)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := &server{db: reopened, fixtureDB: reopened}
	loaded, err := restarted.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Memory[0].MessageID != goal.MessageID || !loaded.State.Memory[0].CreatedAt.Equal(goal.CreatedAt) || loaded.State.ActiveLedgerID == nil || *loaded.State.ActiveLedgerID != *p.State.ActiveLedgerID {
		t.Fatal("identity/context changed after reconnect")
	}
	if !equalJSON(loaded.State.Memory, p.State.Memory) || !equalJSON(loaded.State.CompactionLedger, p.State.CompactionLedger) {
		t.Fatal("active projection differs after reload")
	}
	loaded.State.Status = "active"
	loaded.State.FinalAnswer = nil
	if err = e.Run(ctx, &loaded.State, 5, nil); err != nil {
		t.Fatal(err)
	}
	if err = restarted.save(ctx, &loaded, nil); err != nil {
		t.Fatal(err)
	}
	final, err := restarted.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State.Status != "complete" || final.State.RunID == nil || *final.State.RunID == firstRun || final.State.RunCount != 2 {
		t.Fatal("resume lifecycle identity did not advance")
	}
	after := du05Rows(t, s, ctx, p.State.ID)
	du05Prefix(t, initial, after)
	du05Prefix(t, stopped, after)
	var count, max int64
	if err = s.db.QueryRow(ctx, `SELECT count(*),max(sequence) FROM agent_session_transcript WHERE session_id=$1`, p.State.ID).Scan(&count, &max); err != nil {
		t.Fatal(err)
	}
	if count != max || final.State.NextTranscriptSequence != max+1 {
		t.Fatal("transcript counter gap")
	}
	var memoryPresent bool
	if err = s.db.QueryRow(ctx, `SELECT state ? 'memory' FROM agent_sessions WHERE session_id=$1`, p.State.ID).Scan(&memoryPresent); err != nil || memoryPresent {
		t.Fatal("hot state contains message bodies")
	}
	t.Logf("records=%d next_sequence=%d revision=%d runs=%d; original/stop prefixes preserved", count, final.State.NextTranscriptSequence, final.State.Revision, final.State.RunCount)
}
func du05Image(t *testing.T, s *server, ctx context.Context, id string) []byte {
	t.Helper()
	var b []byte
	err := s.db.QueryRow(ctx, `SELECT jsonb_build_object('state',s.state,'revision',s.revision,'context',(SELECT to_jsonb(c) FROM agent_session_context c WHERE c.session_id=s.session_id),'transcript',(SELECT jsonb_agg(to_jsonb(t) ORDER BY sequence) FROM agent_session_transcript t WHERE t.session_id=s.session_id),'diagnostics',(SELECT jsonb_agg(to_jsonb(d) ORDER BY family,ordinal) FROM agent_session_diagnostics d WHERE d.session_id=s.session_id),'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id) FROM agent_session_events e WHERE e.session_id=s.session_id)) FROM agent_sessions s WHERE session_id=$1`, id).Scan(&b)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestDU05C03PostgresRejectsImmutableCollisionsAtomically(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	other := du05Create(t, s, ctx)
	agent.RecordDiagnostic(&p.State, "compaction_decisions", map[string]any{"accepted": false, "reason": "original"})
	if err := s.save(ctx, &p, nil); err != nil {
		t.Fatal(err)
	}
	var original agent.TranscriptEntry
	var payload []byte
	if err := s.db.QueryRow(ctx, `SELECT message_id::text,sequence,kind,payload,run_id::text,decision_step,created_at FROM agent_session_transcript WHERE session_id=$1 AND sequence=1`, p.State.ID).Scan(&original.MessageID, &original.Sequence, &original.Kind, &payload, &original.RunID, &original.DecisionStep, &original.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &original.Payload); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*persisted)
	}{
		{"changed_payload_id_reuse", func(q *persisted) {
			v := agent.Clone(original)
			v.Payload["content"] = "changed"
			q.State.TranscriptPending = []agent.TranscriptEntry{v}
		}},
		{"sequence_collision", func(q *persisted) {
			v := agent.Clone(original)
			v.MessageID = uuid.NewString()
			q.State.TranscriptPending = []agent.TranscriptEntry{v}
		}},
		{"sequence_gap", func(q *persisted) {
			v := agent.Clone(original)
			v.MessageID = uuid.NewString()
			v.Sequence = 3
			q.State.NextTranscriptSequence = 4
			q.State.TranscriptPending = []agent.TranscriptEntry{v}
		}},
		{"cross_session_summary_source", func(q *persisted) {
			agent.ArchiveRecord(&q.State, "compaction_rejected", map[string]any{"source_message_ids": []string{other.State.Memory[0].MessageID}, "previous_ledger_id": nil, "source_sha256": "bad"})
		}},
		{"inconsistent_previous_ledger", func(q *persisted) {
			agent.ArchiveRecord(&q.State, "compaction_rejected", map[string]any{"source_message_ids": []string{q.State.Memory[0].MessageID}, "previous_ledger_id": uuid.NewString(), "source_sha256": "bad"})
		}},
		{"changed_active_message", func(q *persisted) { v := "changed goal"; q.State.Memory[0].Content = &v }},
		{"duplicate_active_identity", func(q *persisted) { q.State.Memory = append(q.State.Memory, q.State.Memory[0]) }},
		{"replaced_original_goal_identity", func(q *persisted) {
			goal := "goal"
			agent.AppendMessage(&q.State, agent.Message{Role: "user", Content: &goal})
			q.State.Memory = q.State.Memory[1:]
		}},
		{"changed_message_timestamp", func(q *persisted) {
			v := agent.Clone(original)
			v.CreatedAt = v.CreatedAt.Add(time.Microsecond)
			q.State.TranscriptPending = []agent.TranscriptEntry{v}
		}},
		{"active_ledger_wrong_kind", func(q *persisted) {
			id := original.MessageID
			q.State.ActiveLedgerID = &id
			text := "goal"
			q.State.CompactedContext = &text
		}},
		{"typed_ledger_without_reference", func(q *persisted) { q.State.CompactionLedger = &agent.Ledger{Goal: "goal"} }},
		{"next_sequence_mismatch", func(q *persisted) { q.State.NextTranscriptSequence++ }},
		{"changed_goal_anchor", func(q *persisted) { q.State.Goal = "new goal" }},
		{"changed_config_anchor", func(q *persisted) { q.State.ConfigRef = "different.yaml" }},
		{"diagnostic_identity_collision", func(q *persisted) {
			q.State.DiagnosticsPending = []agent.DiagnosticRecord{{Family: "compaction_decisions", Ordinal: 1, Payload: map[string]any{"accepted": true, "reason": "changed"}}}
		}},
		{"diagnostic_gap", func(q *persisted) {
			q.State.DiagnosticCounts["compaction_decisions"] = 3
			q.State.DiagnosticsPending = []agent.DiagnosticRecord{{Family: "compaction_decisions", Ordinal: 3, Payload: map[string]any{"accepted": false}}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := du05Image(t, s, ctx, p.State.ID)
			q := agent.Clone(p)
			tc.mutate(&q)
			revision := q.State.Revision
			pending := agent.Clone(q.State.TranscriptPending)
			if err := s.save(ctx, &q, map[string]any{"type": "must_rollback"}); err == nil {
				t.Fatal("invalid write accepted")
			}
			if !equalJSON(before, du05Image(t, s, ctx, p.State.ID)) { // Compare byte slices directly; JSON semantics are covered below as well.
				if string(before) != string(du05Image(t, s, ctx, p.State.ID)) {
					t.Fatal("rollback changed durable rows")
				}
			}
			if q.State.Revision != revision || !equalJSON(pending, q.State.TranscriptPending) {
				t.Fatal("rollback consumed caller outbox/revision")
			}
		})
	}
	t.Run("identical_record_replay", func(t *testing.T) {
		q := agent.Clone(p)
		q.State.TranscriptPending = []agent.TranscriptEntry{original}
		before := du05Rows(t, s, ctx, p.State.ID)
		if err := s.save(ctx, &q, nil); err != nil {
			t.Fatal(err)
		}
		after := du05Rows(t, s, ctx, p.State.ID)
		if string(before) != string(after) {
			t.Fatal("identical replay duplicated history")
		}
	})
}
func TestDU05C04PostgresTranscriptWatermarkAndPagination(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	agent.ArchiveRecord(&p.State, "model_decision", map[string]any{"content": "first"})
	if err := s.save(ctx, &p, nil); err != nil {
		t.Fatal(err)
	}
	page := func(query string) (int, map[string]any) {
		req := httptest.NewRequest("GET", "/sessions/"+p.State.ID+"/transcript"+query, nil)
		req.SetPathValue("session_id", p.State.ID)
		rr := httptest.NewRecorder()
		s.transcript(rr, req)
		var value map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		return rr.Code, value
	}
	code, first := page("?limit=1")
	if code != 200 || first["next_after_sequence"] != float64(1) || first["through_sequence"] != float64(2) {
		t.Fatalf("bad first page: %v", first)
	}
	agent.ArchiveRecord(&p.State, "run_started", map[string]any{})
	if err := s.save(ctx, &p, nil); err != nil {
		t.Fatal(err)
	}
	code, second := page("?after_sequence=1&limit=1000&through_sequence=2")
	if code != 200 || len(second["entries"].([]any)) != 1 || second["next_after_sequence"] != float64(2) {
		t.Fatalf("watermark leaked later append: %v", second)
	}
	_, last := page("?after_sequence=2&through_sequence=2")
	if len(last["entries"].([]any)) != 0 {
		t.Fatal("expected empty last page")
	}
	for _, query := range []string{"?limit=0", "?limit=1001", "?after_sequence=-1", "?through_sequence=bad"} {
		code, _ = page(query)
		if code != 422 {
			t.Fatalf("invalid query %s status %d", query, code)
		}
	}
	t.Log(fmt.Sprintf("fixed watermark=%v; later append excluded", first["through_sequence"]))
}

func TestDU05C02PostgresLegacyImportMarksHistoryGap(t *testing.T) {
	s, ctx := du05Repository(t)
	id := uuid.NewString()
	cfg, _ := json.Marshal(du05Config())
	legacy := map[string]any{"id": id, "goal": "goal", "config_ref": "du05-controlled.yaml", "config_sha256": "source-hash", "memory": []map[string]any{{"role": "user", "content": "goal"}, {"role": "assistant", "content": "retained turn"}}, "compacted_context": "legacy notes", "compaction_events": []string{"legacy notes"}, "status": "failed", "step_count": 12, "run_count": 1}
	state, _ := json.Marshal(legacy)
	if _, err := s.db.Exec(ctx, `INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256) VALUES($1,$2,$3,$4,$5)`, id, state, cfg, "du05-controlled.yaml", "source-hash"); err != nil {
		t.Fatal(err)
	}
	imported, err := s.fetch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if imported.State.HistoryComplete || imported.State.ActiveLedgerID == nil {
		t.Fatal("legacy import claimed complete history or lost ledger")
	}
	before := du05Rows(t, s, ctx, id)
	again, err := s.fetch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !equalJSON(before, du05Rows(t, s, ctx, id)) || again.State.Revision != imported.State.Revision {
		t.Fatal("legacy import replay duplicated history")
	}
	var count int
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_session_transcript WHERE session_id=$1 AND kind='history_gap'`, id).Scan(&count); err != nil || count != 1 {
		t.Fatal("legacy history gap not recorded once")
	}
}
func TestDU05C02PostgresDiagnosticTailKeepsFullArchive(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	for i := 0; i < 40; i++ {
		agent.RecordDiagnostic(&p.State, "compaction_attempts", map[string]any{"accepted": false, "index": i})
	}
	if err := s.save(ctx, &p, nil); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.DiagnosticCounts["compaction_attempts"] != 40 || len(loaded.State.CompactionAttempts) != 32 || loaded.State.DiagnosticsComplete || loaded.State.DiagnosticRejections != 40 {
		t.Fatal("normal read lost lifetime metadata or returned unbounded diagnostics")
	}
	var rows int
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_session_diagnostics WHERE session_id=$1 AND family='compaction_attempts'`, p.State.ID).Scan(&rows); err != nil || rows != 40 {
		t.Fatal("diagnostic archive was trimmed")
	}
	req := httptest.NewRequest("GET", "/sessions/"+p.State.ID+"/diagnostics/compaction_attempts?limit=1000", nil)
	req.SetPathValue("session_id", p.State.ID)
	req.SetPathValue("kind", "compaction_attempts")
	response := httptest.NewRecorder()
	s.compactionDiagnostics(response, req)
	var page struct {
		Total   int   `json:"total_recorded"`
		Entries []any `json:"entries"`
	}
	if err = json.Unmarshal(response.Body.Bytes(), &page); err != nil || response.Code != 200 || page.Total != 40 || len(page.Entries) != 40 {
		t.Fatalf("diagnostic page did not read full archive: %s", response.Body.String())
	}
}
