package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/state-driven-agent/agent-go/internal/agent"
)

func du10Terminal(t *testing.T, s *server, ctx context.Context, diagnostics, events int) persisted {
	t.Helper()
	p := du05Create(t, s, ctx)
	p.State.Status = "complete"
	p.State.RunStatus = "complete"
	for i := 0; i < diagnostics; i++ {
		agent.RecordDiagnostic(&p.State, "compaction_decisions", map[string]any{"ordinal": i})
	}
	if err := s.save(ctx, &p, map[string]any{"type": "du10_seed"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < events; i++ {
		if err := s.save(ctx, &p, map[string]any{"type": "du10_event", "ordinal": i}); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestDU10C01RepeatableReadArchiveIsCanonicalAndVerified(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du10Terminal(t, s, ctx, 40, 105)
	path := filepath.Join(t.TempDir(), "session.jsonl")
	a, err := s.exportArchive(ctx, p.State.ID, path)
	if err != nil {
		t.Fatal(err)
	}
	again, err := verifyArchive(path)
	if err != nil {
		t.Fatal(err)
	}
	if a.SHA256 == "" || a.SHA256 != again.SHA256 || a.SnapshotRevision != p.State.Revision || a.Counts["agent_session_events"] < 100 {
		t.Fatalf("archive metadata mismatch: %#v", a)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("archive mode=%o want 0600", info.Mode().Perm())
	}
}

func TestDU10C02ArchiveVerifierRejectsTruncationTrailingAndCrossSession(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du10Terminal(t, s, ctx, 2, 3)
	path := filepath.Join(t.TempDir(), "source.jsonl")
	if _, err := s.exportArchive(ctx, p.State.ID, path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(b), "\n")
	for name, content := range map[string]string{"truncated": strings.Join(lines[:len(lines)-2], "\n"), "trailing": string(b) + "{}\n", "cross-session": strings.Replace(string(b), p.State.ID, "00000000-0000-0000-0000-000000000001", 1)} {
		t.Run(name, func(t *testing.T) {
			bad := filepath.Join(t.TempDir(), "bad.jsonl")
			if err := os.WriteFile(bad, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := verifyArchive(bad); err == nil {
				t.Fatal("invalid archive accepted")
			}
		})
	}
}

func TestDU10C03StaleAndActiveArchivesCannotPrune(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du10Terminal(t, s, ctx, 40, 110)
	path := filepath.Join(t.TempDir(), "stale.jsonl")
	if _, err := s.exportArchive(ctx, p.State.ID, path); err != nil {
		t.Fatal(err)
	}
	if err := s.save(ctx, &p, map[string]any{"type": "advance_revision"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pruneTelemetry(ctx, p.State.ID, path, 32, 100); err == nil || !strings.Contains(err.Error(), "stale archive") {
		t.Fatalf("stale archive prune err=%v", err)
	}
	active := du05Create(t, s, ctx)
	activePath := filepath.Join(t.TempDir(), "active.jsonl")
	if _, err := s.exportArchive(ctx, active.State.ID, activePath); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pruneTelemetry(ctx, active.State.ID, activePath, 32, 100); err == nil {
		t.Fatal("active session pruned")
	}
}

func TestDU10C03RetentionFailureRollsBackReceiptAndDeletes(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du10Terminal(t, s, ctx, 80, 110)
	path := filepath.Join(t.TempDir(), "fault.jsonl")
	if _, err := s.exportArchive(ctx, p.State.ID, path); err != nil {
		t.Fatal(err)
	}
	var beforeEvents, beforeDiagnostics, beforeReceipts int
	if err := s.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_session_events WHERE session_id=$1),(SELECT count(*) FROM agent_session_diagnostics WHERE session_id=$1),(SELECT count(*) FROM agent_archive_receipts WHERE session_id=$1)`, p.State.ID).Scan(&beforeEvents, &beforeDiagnostics, &beforeReceipts); err != nil {
		t.Fatal(err)
	}
	s.saveFault = func(stage string) error {
		if stage == "retention" {
			return fmt.Errorf("injected after retention mutations")
		}
		return nil
	}
	if _, err := s.pruneTelemetry(ctx, p.State.ID, path, 32, 100); err == nil {
		t.Fatal("injected retention failure accepted")
	}
	s.saveFault = nil
	var events, diagnostics, receipts int
	if err := s.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_session_events WHERE session_id=$1),(SELECT count(*) FROM agent_session_diagnostics WHERE session_id=$1),(SELECT count(*) FROM agent_archive_receipts WHERE session_id=$1)`, p.State.ID).Scan(&events, &diagnostics, &receipts); err != nil {
		t.Fatal(err)
	}
	if events != beforeEvents || diagnostics != beforeDiagnostics || receipts != beforeReceipts {
		t.Fatalf("retention transaction partially committed: before=(%d,%d,%d) after=(%d,%d,%d)", beforeEvents, beforeDiagnostics, beforeReceipts, events, diagnostics, receipts)
	}
}

func TestDU10C04VerifiedPruneKeepsTailsAndProtectedRows(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du10Terminal(t, s, ctx, 80, 125)
	var protectedBefore []byte
	if err := s.db.QueryRow(ctx, `SELECT jsonb_build_object('transcript',(SELECT jsonb_agg(to_jsonb(x) ORDER BY sequence) FROM agent_session_transcript x WHERE session_id=$1),'calls',(SELECT jsonb_agg(to_jsonb(x) ORDER BY call_id) FROM agent_session_calls x WHERE session_id=$1),'runs',(SELECT jsonb_agg(to_jsonb(x) ORDER BY run_id) FROM agent_runs x WHERE session_id=$1),'executions',(SELECT jsonb_agg(to_jsonb(x) ORDER BY execution_id) FROM agent_tool_executions x WHERE session_id=$1))`, p.State.ID).Scan(&protectedBefore); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "terminal.jsonl")
	archive, err := s.exportArchive(ctx, p.State.ID, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pruneTelemetry(ctx, p.State.ID, path, 32, 100); err != nil {
		t.Fatal(err)
	}
	var afterEvents, afterDiagnostics, receipts int
	if err = s.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_session_events WHERE session_id=$1),(SELECT count(*) FROM agent_session_diagnostics WHERE session_id=$1),(SELECT count(*) FROM agent_archive_receipts WHERE session_id=$1)`, p.State.ID).Scan(&afterEvents, &afterDiagnostics, &receipts); err != nil {
		t.Fatal(err)
	}
	if afterEvents != 100 || afterDiagnostics != 32 || receipts != 1 {
		t.Fatalf("retained events=%d diagnostics=%d receipts=%d", afterEvents, afterDiagnostics, receipts)
	}
	loaded, err := s.fetch(ctx, p.State.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State.LastArchiveID == nil || *loaded.State.LastArchiveID != archive.ArchiveID || loaded.State.DiagnosticCounts["compaction_decisions"] != 80 || loaded.State.DiagnosticHistoryComplete {
		t.Fatalf("archive state not recorded correctly: %#v", loaded.State)
	}
	var protectedAfter []byte
	if err = s.db.QueryRow(ctx, `SELECT jsonb_build_object('transcript',(SELECT jsonb_agg(to_jsonb(x) ORDER BY sequence) FROM agent_session_transcript x WHERE session_id=$1),'calls',(SELECT jsonb_agg(to_jsonb(x) ORDER BY call_id) FROM agent_session_calls x WHERE session_id=$1),'runs',(SELECT jsonb_agg(to_jsonb(x) ORDER BY run_id) FROM agent_runs x WHERE session_id=$1),'executions',(SELECT jsonb_agg(to_jsonb(x) ORDER BY execution_id) FROM agent_tool_executions x WHERE session_id=$1))`, p.State.ID).Scan(&protectedAfter); err != nil {
		t.Fatal(err)
	}
	if !sameJSONBytes(protectedBefore, protectedAfter) {
		t.Fatal("protected transcript/call/run/execution data changed during telemetry pruning")
	}
	// A later archive depends on the first receipt and must preserve its file.
	secondPath := filepath.Join(t.TempDir(), "chained.jsonl")
	second, err := s.exportArchive(ctx, p.State.ID, secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pruneTelemetry(ctx, p.State.ID, secondPath, 32, 100); err != nil {
		t.Fatalf("second verified archive could not chain: %v", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("prior archive dependency missing: %v", err)
	}
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM agent_archive_receipts WHERE session_id=$1`, p.State.ID).Scan(&receipts); err != nil || receipts != 2 {
		t.Fatalf("chained receipts=%d err=%v", receipts, err)
	}
	if second.SHA256 == archive.SHA256 {
		t.Fatal("new archive did not capture the receipt/revision change")
	}
}

func TestDU10C05NormalKeepAllDoesNotRequireArchiveOrPrune(t *testing.T) {
	s, ctx := du05Repository(t)
	p := du10Terminal(t, s, ctx, 40, 105)
	var events, diagnostics, receipts int
	if err := s.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_session_events WHERE session_id=$1),(SELECT count(*) FROM agent_session_diagnostics WHERE session_id=$1),(SELECT count(*) FROM agent_archive_receipts WHERE session_id=$1)`, p.State.ID).Scan(&events, &diagnostics, &receipts); err != nil {
		t.Fatal(err)
	}
	if events < 100 || diagnostics != 40 || receipts != 0 {
		t.Fatalf("keep-all default changed data: events=%d diagnostics=%d receipts=%d", events, diagnostics, receipts)
	}
}

func TestDU10B01ArchiveExportQualificationGrid(t *testing.T) {
	s, ctx := du05Repository(t)
	for _, n := range []int{0, 100, 1000} {
		t.Run(fmt.Sprintf("events_%d", n), func(t *testing.T) {
			p := du10Terminal(t, s, ctx, 32, n)
			path := filepath.Join(t.TempDir(), "grid.jsonl")
			started := time.Now()
			a, err := s.exportArchive(ctx, p.State.ID, path)
			elapsed := time.Since(started)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = verifyArchive(path); err != nil {
				t.Fatal(err)
			}
			line, _ := json.Marshal(map[string]any{"rows": a.Counts, "bytes": info.Size(), "sha256": a.SHA256, "snapshot_revision": a.SnapshotRevision, "elapsed_ms": float64(elapsed.Microseconds()) / 1000})
			t.Logf("DU10_B01_CELL %s", line)
		})
	}
}
