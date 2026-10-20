package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

func TestDU06C01SchemaIsCompleteAndIdempotent(t *testing.T) {
	s, ctx := du05Repository(t)
	want := []string{"agent_sessions", "agent_session_transcript", "agent_session_context", "agent_session_events", "agent_runs", "agent_tool_executions", "agent_session_diagnostics", "agent_session_calls", "agent_archive_receipts", "agent_schema_migrations"}
	for _, table := range want {
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT to_regclass(current_schema()||'.'||$1) IS NOT NULL`, table).Scan(&exists); err != nil || !exists {
			t.Fatalf("table %s missing: %v", table, err)
		}
	}
	if err := s.schema(); err != nil {
		t.Fatalf("repeat schema init: %v", err)
	}
	var migrations int
	if err := s.db.QueryRow(ctx, `SELECT count(*) FROM agent_schema_migrations WHERE (component,version) IN (('agent-python',5),('agent-go',7))`).Scan(&migrations); err != nil || migrations != 2 {
		t.Fatalf("compatibility markers=%d err=%v", migrations, err)
	}
	var uniqueConstraints int
	if err := s.db.QueryRow(ctx, `SELECT count(*) FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND contype IN ('p','u','f','c')`).Scan(&uniqueConstraints); err != nil || uniqueConstraints < 20 {
		t.Fatalf("expected schema constraints, count=%d err=%v", uniqueConstraints, err)
	}
	var indexes int
	if err := s.db.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema()`).Scan(&indexes); err != nil || indexes < 15 {
		t.Fatalf("expected schema indexes, count=%d err=%v", indexes, err)
	}
	t.Logf("DU06 C01 schema tables=%d constraints=%d indexes=%d repeated initialization safe", len(want), uniqueConstraints, indexes)
	// Import a pre-journal projection and verify the data loss is represented,
	// rather than marking a reconstructed transcript complete.
	cfgJSON, err := json.Marshal(du05Config())
	if err != nil {
		t.Fatal(err)
	}
	legacy := agent.NewSession(uuid.NewString(), "legacy goal", "legacy.yaml", "legacy-source")
	legacy.Memory[0].Sequence = nil
	legacy.NextTranscriptSequence = 1
	legacyJSON, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(ctx, `INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256) VALUES($1,$2,$3,$4,$5)`, legacy.ID, legacyJSON, cfgJSON, legacy.ConfigRef, legacy.ConfigSHA256); err != nil {
		t.Fatal(err)
	}
	imported, err := s.fetch(ctx, legacy.ID)
	if err != nil {
		t.Fatalf("legacy import: %v", err)
	}
	var gaps int
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_session_transcript WHERE session_id=$1 AND kind='history_gap'`, legacy.ID).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	if imported.State.HistoryComplete || gaps != 1 {
		t.Fatalf("legacy history falsely complete=%v gaps=%d", imported.State.HistoryComplete, gaps)
	}
}

func TestDU06C02EveryAggregateWriteStageRollsBack(t *testing.T) {
	stages := []string{"snapshot", "transcript", "call_map", "diagnostics", "run_tool", "event", "context", "retention"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			s, ctx := du05Repository(t)
			p := du05Create(t, s, ctx)
			var beforeRevision, beforeTranscript, beforeEvents, beforeContext int64
			var beforeState []byte
			if err := s.db.QueryRow(ctx, `SELECT state,revision,(SELECT count(*) FROM agent_session_transcript WHERE session_id=$1),(SELECT count(*) FROM agent_session_events WHERE session_id=$1),(SELECT revision FROM agent_session_context WHERE session_id=$1) FROM agent_sessions WHERE session_id=$1`, p.State.ID).Scan(&beforeState, &beforeRevision, &beforeTranscript, &beforeEvents, &beforeContext); err != nil {
				t.Fatal(err)
			}
			content := "staged data"
			callID := "du06-call-" + uuid.NewString()
			agent.AppendMessage(&p.State, agent.Message{Role: "assistant", Content: &content, ToolCalls: []any{map[string]any{"id": callID, "type": "function", "function": map[string]any{"name": "lookup", "arguments": "{}"}}}})
			agent.RecordDiagnostic(&p.State, "compaction_decisions", map[string]any{"stage": stage})
			p.State.ExecutionPending = []map[string]any{{"execution_id": uuid.NewString(), "call_id": callID, "tool_name": "lookup", "arguments": map[string]any{}, "status": "intent"}}
			runID, runStarted := uuid.NewString(), time.Now().UTC()
			p.State.RunID, p.State.RunStartedAt, p.State.RunCount, p.State.StepCount = &runID, &runStarted, 1, 1
			p.State.RetentionPending = map[string]any{"archive_id": uuid.NewString(), "checksum": "rollback-check"}
			s.saveFault = func(got string) error {
				if got == stage {
					return fmt.Errorf("injected %s", stage)
				}
				return nil
			}
			err := s.save(ctx, &p, map[string]any{"type": "du06-stage", "stage": stage})
			if err == nil {
				t.Fatal("injected save failure was acknowledged")
			}
			var revision, transcript, events, diagnostics, receipts, calls, executions, runs, contextRevision int64
			var afterState []byte
			if e := s.db.QueryRow(ctx, `SELECT state,revision,(SELECT count(*) FROM agent_session_transcript WHERE session_id=$1),(SELECT count(*) FROM agent_session_events WHERE session_id=$1),(SELECT count(*) FROM agent_session_diagnostics WHERE session_id=$1),(SELECT count(*) FROM agent_archive_receipts WHERE session_id=$1),(SELECT count(*) FROM agent_session_calls WHERE session_id=$1),(SELECT count(*) FROM agent_tool_executions WHERE session_id=$1),(SELECT count(*) FROM agent_runs WHERE session_id=$1),(SELECT revision FROM agent_session_context WHERE session_id=$1) FROM agent_sessions WHERE session_id=$1`, p.State.ID).Scan(&afterState, &revision, &transcript, &events, &diagnostics, &receipts, &calls, &executions, &runs, &contextRevision); e != nil {
				t.Fatal(e)
			}
			if revision != beforeRevision || !equalJSON(beforeState, afterState) || contextRevision != beforeContext || transcript != beforeTranscript || events != beforeEvents || diagnostics != 0 || receipts != 0 || calls != 0 || executions != 0 || runs != 0 {
				t.Fatalf("partial commit stage=%s revision=%d context_revision=%d transcript=%d events=%d diagnostics=%d receipts=%d calls=%d executions=%d runs=%d", stage, revision, contextRevision, transcript, events, diagnostics, receipts, calls, executions, runs)
			}
			if len(p.State.TranscriptPending) == 0 || len(p.State.DiagnosticsPending) == 0 || p.State.PendingCommitID == nil || p.State.RetentionPending == nil {
				t.Fatal("failed save discarded caller outboxes or commit identity")
			}
		})
	}
}

func TestDU06C03RevisionCASAndImmutableAnchors(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	stale := agent.Clone(p)
	left, right := "winner", "stale"
	agent.AppendMessage(&p.State, agent.Message{Role: "assistant", Content: &left})
	agent.AppendMessage(&stale.State, agent.Message{Role: "assistant", Content: &right})
	if err := s.save(ctx, &p, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.save(ctx, &stale, nil); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale save error=%v", err)
	}
	loaded, err := s.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.Revision != p.State.Revision || len(loaded.State.Memory) < 2 || *loaded.State.Memory[1].Content != left {
		t.Fatal("stale writer changed committed snapshot")
	}
	changed := agent.Clone(p)
	*changed.State.Memory[0].Content = "altered raw goal"
	if err = s.save(ctx, &changed, nil); err == nil {
		t.Fatal("changed immutable raw goal accepted")
	}
	changed = agent.Clone(p)
	changed.State.Goal = "altered goal"
	if err = s.save(ctx, &changed, nil); err == nil {
		t.Fatal("changed goal anchor accepted")
	}
	changed = agent.Clone(p)
	changed.State.ConfigRef = "replacement.yaml"
	if err = s.save(ctx, &changed, nil); err == nil {
		t.Fatal("changed config reference accepted")
	}
	changed = agent.Clone(p)
	changed.State.ConfigSHA256 = "replacement-source-hash"
	if err = s.save(ctx, &changed, nil); err == nil {
		t.Fatal("changed source config hash accepted")
	}
	changed = agent.Clone(p)
	var altered agent.Config
	if err = json.Unmarshal(changed.Config, &altered); err != nil {
		t.Fatal(err)
	}
	altered.Name = "replacement-config"
	changed.Config, err = json.Marshal(altered)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.save(ctx, &changed, nil); err == nil {
		t.Fatal("changed resolved config accepted")
	}
	t.Logf("DU06 C03 winner revision=%d; stale and immutable writes rejected", p.State.Revision)
}

func TestDU06C04LostCommitAckRetriesWithoutDuplicateEffects(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du05Create(t, s, ctx)
	content := "ack boundary"
	agent.AppendMessage(&p.State, agent.Message{Role: "assistant", Content: &content})
	agent.RecordDiagnostic(&p.State, "compaction_decisions", map[string]any{"ack": "uncertain"})
	event := map[string]any{"type": "du06-ack-boundary"}
	var original persisted
	var firstCommit string
	s.saveFault = func(stage string) error {
		if stage == "commit_ack" {
			s.saveFault = nil
			firstCommit = *p.State.PendingCommitID
			return errors.New("lost commit acknowledgment")
		}
		return nil
	}
	if err := s.save(ctx, &p, event); err == nil {
		t.Fatal("fault after commit was not exposed")
	}
	if p.State.PendingCommitID == nil || *p.State.PendingCommitID != firstCommit {
		t.Fatal("pending commit identity lost after uncertain acknowledgment")
	}
	original = agent.Clone(p)
	if err := s.save(ctx, &p, event); err != nil {
		t.Fatalf("same commit retry: %v", err)
	}
	var rev, transcript, events, diagnostics int64
	if err := s.db.QueryRow(ctx, `SELECT revision,(SELECT count(*) FROM agent_session_transcript WHERE session_id=$1),(SELECT count(*) FROM agent_session_events WHERE session_id=$1),(SELECT count(*) FROM agent_session_diagnostics WHERE session_id=$1) FROM agent_sessions WHERE session_id=$1`, p.State.ID).Scan(&rev, &transcript, &events, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if rev != 2 || transcript != 2 || events != 2 || diagnostics != 1 || p.State.PendingCommitID != nil {
		t.Fatalf("duplicate or lost commit effects: rev=%d transcript=%d events=%d diagnostics=%d pending=%v", rev, transcript, events, diagnostics, p.State.PendingCommitID)
	}
	loaded, err := s.fetch(ctx, p.State.ID)
	if err != nil || !equalJSON(loaded.State.Memory, p.State.Memory) || loaded.State.Revision != p.State.Revision {
		t.Fatalf("full payload/context restore: revision=%d err=%v", loaded.State.Revision, err)
	}
	// Reusing the acknowledged identity for changed content is a hard collision.
	changed := original
	changed.State.PendingCommitID = &firstCommit
	changed.State.FinalAnswer = stringPtr("different content")
	if err := s.save(ctx, &changed, nil); err == nil {
		t.Fatal("changed content under an existing commit identity accepted")
	}
	t.Logf("DU06 C04 revision=%d; same identity acknowledged; transcript=%d events=%d", rev, transcript, events)
}

func stringPtr(v string) *string { return &v }

func TestDU06C05ConcurrentSchemaStartupUsesSharedAdvisoryGuard(t *testing.T) {
	if err := loadDotEnv(); err != nil {
		t.Fatal(err)
	}
	dsn := env("DU05_TEST_DATABASE_URL", env("AGENT_SESSION_DATABASE_URL", "postgresql://user:password@192.168.1.50:5432/universal_state"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "du06_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	firstPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	secondPool, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		firstPool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		firstPool.Close()
		secondPool.Close()
		cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
		defer done()
		if _, e := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); e != nil {
			t.Errorf("cleanup DU06 schema: %v", e)
		}
		admin.Close()
	})
	s := &server{db: firstPool, fixtureDB: firstPool}
	other := &server{db: secondPool, fixtureDB: secondPool}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, worker := range []*server{s, other} {
		wg.Add(1)
		go func(w *server) { defer wg.Done(); <-start; errs <- w.schema() }(worker)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent schema startup: %v", err)
		}
	}
	started := time.Now()
	if err = s.schema(); err != nil {
		t.Fatal(err)
	}
	t.Logf("DU06 C05 two independent workers initialized; current-schema repeat=%s", time.Since(started))
}
