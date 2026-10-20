package main

// Archive export and telemetry pruning are explicit storage operations. There
// is intentionally no destructive HTTP endpoint.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type archiveTable struct{ name, order string }

var archiveTables = []archiveTable{
	{"agent_sessions", "session_id"}, {"agent_session_context", "session_id"}, {"agent_session_transcript", "sequence"},
	{"agent_session_events", "event_id"}, {"agent_session_diagnostics", "family,ordinal"}, {"agent_runs", "started_at,run_id"},
	{"agent_tool_executions", "execution_id"}, {"agent_session_calls", "call_id,message_id"}, {"agent_archive_receipts", "archive_id"},
}
var archiveFamilies = []string{"compaction_events", "compaction_attempts", "compaction_decisions"}

type verifiedArchive struct {
	Format           string            `json:"format"`
	ArchiveID        string            `json:"archive_id"`
	SessionID        string            `json:"session_id"`
	SnapshotRevision int64             `json:"snapshot_revision"`
	SHA256           string            `json:"sha256"`
	Counts           map[string]int64  `json:"counts"`
	TableSHA256      map[string]string `json:"table_sha256"`
	Watermarks       map[string]any    `json:"watermarks"`
	ArchivePath      string            `json:"archive_path,omitempty"`
}

func archiveLine(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	err := e.Encode(v)
	return b.Bytes(), err
}

func decodeArchiveJSON(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(target)
}
func shaHex(h io.Writer) string {
	return hex.EncodeToString(h.(interface{ Sum([]byte) []byte }).Sum(nil))
}

// exportArchive uses one repeatable-read, read-only snapshot and exclusive 0600
// file creation. A partial file has no valid manifest and cannot authorize prune.
func (s *server) exportArchive(ctx context.Context, sessionID, path string) (verifiedArchive, error) {
	var out verifiedArchive
	id, err := uuid.Parse(sessionID)
	if err != nil {
		return out, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return out, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return out, err
	}
	defer f.Close()
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL TIME ZONE 'UTC'`); err != nil {
		return out, err
	}
	if err = tx.QueryRow(ctx, `SELECT revision FROM agent_sessions WHERE session_id=$1`, id).Scan(&out.SnapshotRevision); err != nil {
		return out, err
	}
	out.Format = "sda-archive-1"
	out.ArchiveID = uuid.NewString()
	out.SessionID = id.String()
	out.Counts = map[string]int64{}
	out.TableSHA256 = map[string]string{}
	diagWM := map[string]int64{}
	out.Watermarks = map[string]any{"events": int64(0), "diagnostics": diagWM}
	whole := sha256.New()
	hashes := map[string]io.Writer{}
	for _, t := range archiveTables {
		out.Counts[t.name] = 0
		hashes[t.name] = sha256.New()
	}
	write := func(v any, table string) error {
		b, e := archiveLine(v)
		if e != nil {
			return e
		}
		if _, e = f.Write(b); e != nil {
			return e
		}
		_, _ = whole.Write(b)
		if table != "" {
			_, _ = hashes[table].Write(b)
		}
		return nil
	}
	if err = write(map[string]any{"format": out.Format, "archive_id": out.ArchiveID, "session_id": out.SessionID, "snapshot_revision": out.SnapshotRevision}, ""); err != nil {
		return out, err
	}
	for _, t := range archiveTables {
		rows, e := tx.Query(ctx, fmt.Sprintf(`SELECT to_jsonb(t) FROM %s t WHERE session_id=$1 ORDER BY %s`, t.name, t.order), id)
		if e != nil {
			return out, e
		}
		for rows.Next() {
			var raw []byte
			if e = rows.Scan(&raw); e != nil {
				rows.Close()
				return out, e
			}
			var row any
			if e = decodeArchiveJSON(raw, &row); e != nil {
				rows.Close()
				return out, e
			}
			if e = write(map[string]any{"table": t.name, "row": row}, t.name); e != nil {
				rows.Close()
				return out, e
			}
			out.Counts[t.name]++
			var m map[string]any
			_ = decodeArchiveJSON(raw, &m)
			if t.name == "agent_session_events" {
				out.Watermarks["events"] = intValue(m["event_id"])
			}
			if t.name == "agent_session_diagnostics" {
				family, _ := m["family"].(string)
				diagWM[family] = int64(intValue(m["ordinal"]))
			}
		}
		if e = rows.Err(); e != nil {
			rows.Close()
			return out, e
		}
		rows.Close()
	}
	for _, t := range archiveTables {
		out.TableSHA256[t.name] = shaHex(hashes[t.name])
	}
	out.SHA256 = shaHex(whole)
	manifest := map[string]any{"sha256": out.SHA256, "table_sha256": out.TableSHA256, "counts": out.Counts, "watermarks": out.Watermarks}
	if err = write(map[string]any{"manifest": manifest}, ""); err != nil {
		return out, err
	}
	if err = f.Sync(); err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, err
	}
	return verifyArchive(path)
}

func (s *server) retentionPlan(ctx context.Context, id string) (map[string]any, error) {
	p, err := s.fetch(ctx, id)
	if err != nil {
		return nil, err
	}
	var unresolved, running int64
	if err = s.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_tool_executions WHERE session_id=$1 AND status IN ('pending','started','uncertain')),(SELECT count(*) FROM agent_runs WHERE session_id=$1 AND status='running')`, id).Scan(&unresolved, &running); err != nil {
		return nil, err
	}
	eligible := p.State.Status != "active" && p.State.RunStatus != "running" && len(p.State.PendingToolCalls) == 0 && unresolved == 0 && running == 0
	return map[string]any{"session_id": id, "revision": p.State.Revision, "default_policy": "keep_all", "eligible": eligible, "unresolved_executions": unresolved, "running_lifecycles": running, "keep_diagnostics": 32, "keep_events": 100, "protected_tables": []string{"agent_sessions", "agent_session_context", "agent_session_transcript", "agent_session_calls", "agent_runs", "agent_tool_executions", "agent_archive_receipts"}, "prunable_tables": []string{"agent_session_events", "agent_session_diagnostics"}, "requires": "Verified fresh export, terminal session, ownership/fencing, matching revision and database digests"}, nil
}

// retention CLI is an explicit database admin driver, kept outside HTTP.
func runRetentionCLI(ctx context.Context, s *server, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: retention {plan|export|verify|prune-telemetry} [--session-id UUID] [--archive PATH]")
	}
	action := args[0]
	fs := flag.NewFlagSet("retention "+action, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	id := fs.String("session-id", "", "session UUID")
	path := fs.String("archive", "", "archive JSONL path")
	keepDiag := fs.Int("keep-diagnostics", 32, "diagnostic rows per family to keep")
	keepEvents := fs.Int("keep-events", 100, "event rows to keep")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	var parsed uuid.UUID
	var err error
	if *id != "" {
		parsed, err = uuid.Parse(*id)
		if err != nil {
			return fmt.Errorf("--session-id must be a UUID")
		}
	}
	var result any
	switch action {
	case "verify":
		if *path == "" {
			return fmt.Errorf("--archive is required")
		}
		var archive verifiedArchive
		archive, err = verifyArchive(*path)
		if err == nil && parsed != uuid.Nil && parsed.String() != archive.SessionID {
			err = fmt.Errorf("archive belongs to another session")
		}
		result = archive
	case "plan":
		if parsed == uuid.Nil {
			return fmt.Errorf("--session-id is required")
		}
		result, err = s.retentionPlan(ctx, parsed.String())
	case "export":
		if parsed == uuid.Nil {
			return fmt.Errorf("--session-id is required")
		}
		if *path == "" {
			return fmt.Errorf("--archive is required")
		}
		result, err = s.exportArchive(ctx, parsed.String(), *path)
	case "prune-telemetry":
		if parsed == uuid.Nil {
			return fmt.Errorf("--session-id is required")
		}
		if *path == "" {
			return fmt.Errorf("--archive is required")
		}
		result, err = s.pruneTelemetry(ctx, parsed.String(), *path, *keepDiag, *keepEvents)
	default:
		return fmt.Errorf("unknown retention action %q", action)
	}
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// verifyArchive validates every row and refuses truncation, extra data, invalid
// table order, malformed manifests, and cross-session rows.
func verifyArchive(path string) (verifiedArchive, error) {
	var out verifiedArchive
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4096), 64<<20)
	if !sc.Scan() {
		return out, fmt.Errorf("archive header missing")
	}
	var header map[string]json.RawMessage
	if err = json.Unmarshal(sc.Bytes(), &header); err != nil {
		return out, err
	}
	if len(header) != 4 {
		return out, fmt.Errorf("invalid archive header shape")
	}
	if err = json.Unmarshal(header["format"], &out.Format); err != nil || out.Format != "sda-archive-1" {
		return out, fmt.Errorf("unsupported archive format")
	}
	if err = json.Unmarshal(header["archive_id"], &out.ArchiveID); err != nil {
		return out, err
	}
	if _, err = uuid.Parse(out.ArchiveID); err != nil {
		return out, err
	}
	if err = json.Unmarshal(header["session_id"], &out.SessionID); err != nil {
		return out, err
	}
	if _, err = uuid.Parse(out.SessionID); err != nil {
		return out, err
	}
	if err = json.Unmarshal(header["snapshot_revision"], &out.SnapshotRevision); err != nil || out.SnapshotRevision < 1 {
		return out, fmt.Errorf("invalid snapshot revision")
	}
	whole := sha256.New()
	headerLine, _ := archiveLine(map[string]any{"format": out.Format, "archive_id": out.ArchiveID, "session_id": out.SessionID, "snapshot_revision": out.SnapshotRevision})
	_, _ = whole.Write(headerLine)
	counts := map[string]int64{}
	hashes := map[string]io.Writer{}
	positions := map[string]int{}
	for i, t := range archiveTables {
		counts[t.name] = 0
		hashes[t.name] = sha256.New()
		positions[t.name] = i
	}
	watermarks := map[string]any{"events": int64(0), "diagnostics": map[string]int64{}}
	lastTable := -1
	lastOrderKey := ""
	manifestSeen := false
	var manifest map[string]json.RawMessage
	for sc.Scan() {
		raw := append([]byte(nil), sc.Bytes()...)
		var item map[string]json.RawMessage
		if err = json.Unmarshal(raw, &item); err != nil {
			return out, err
		}
		if manifestSeen {
			return out, fmt.Errorf("trailing archive data")
		}
		if m, ok := item["manifest"]; ok {
			if len(item) != 1 {
				return out, fmt.Errorf("invalid manifest envelope")
			}
			manifestSeen = true
			if err = json.Unmarshal(m, &manifest); err != nil {
				return out, err
			}
			continue
		}
		if len(item) != 2 {
			return out, fmt.Errorf("invalid archive row envelope")
		}
		var table string
		if err = json.Unmarshal(item["table"], &table); err != nil {
			return out, err
		}
		pos, ok := positions[table]
		if !ok || pos < lastTable {
			return out, fmt.Errorf("archive table order invalid")
		}
		if pos != lastTable {
			lastOrderKey = ""
		}
		lastTable = pos
		var row map[string]any
		if err = decodeArchiveJSON(item["row"], &row); err != nil {
			return out, err
		}
		if fmt.Sprint(row["session_id"]) != out.SessionID {
			return out, fmt.Errorf("cross-session archive row")
		}
		orderKey, e := archiveRowOrderKey(row, archiveTables[pos].order)
		if e != nil {
			return out, e
		}
		if lastOrderKey != "" && orderKey < lastOrderKey {
			return out, fmt.Errorf("archive row order invalid for %s", table)
		}
		lastOrderKey = orderKey
		value := map[string]any{"table": table, "row": row}
		line, e := archiveLine(value)
		if e != nil {
			return out, e
		}
		_, _ = whole.Write(line)
		_, _ = hashes[table].Write(line)
		counts[table]++
		if table == "agent_session_events" {
			watermarks["events"] = intValue(row["event_id"])
		}
		if table == "agent_session_diagnostics" {
			family, _ := row["family"].(string)
			if family != "compaction_events" && family != "compaction_attempts" && family != "compaction_decisions" {
				return out, fmt.Errorf("unknown diagnostic family")
			}
			watermarks["diagnostics"].(map[string]int64)[family] = int64(intValue(row["ordinal"]))
		}
	}
	if err = sc.Err(); err != nil {
		return out, err
	}
	if !manifestSeen || len(manifest) != 4 {
		return out, fmt.Errorf("archive manifest missing or invalid")
	}
	var gotCounts map[string]int64
	var gotHashes map[string]string
	var gotWM map[string]any
	if err = json.Unmarshal(manifest["counts"], &gotCounts); err != nil {
		return out, err
	}
	if err = json.Unmarshal(manifest["table_sha256"], &gotHashes); err != nil {
		return out, err
	}
	if err = json.Unmarshal(manifest["watermarks"], &gotWM); err != nil {
		return out, err
	}
	if err = json.Unmarshal(manifest["sha256"], &out.SHA256); err != nil {
		return out, err
	}
	expectedHashes := map[string]string{}
	for _, t := range archiveTables {
		expectedHashes[t.name] = shaHex(hashes[t.name])
	}
	if out.SHA256 != shaHex(whole) || !equalJSON(gotCounts, counts) || !equalJSON(gotHashes, expectedHashes) || !equalJSON(gotWM, watermarks) {
		return out, fmt.Errorf("archive digest, counts, or watermarks mismatch")
	}
	out.Counts = counts
	out.TableSHA256 = expectedHashes
	out.Watermarks = watermarks
	abs, e := filepath.Abs(path)
	if e == nil {
		out.ArchivePath = abs
	}
	return out, nil
}

func archiveRowOrderKey(row map[string]any, order string) (string, error) {
	parts := strings.Split(order, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		key := strings.TrimSpace(part)
		value, ok := row[key]
		if !ok {
			return "", fmt.Errorf("archive row missing order key %s", key)
		}
		switch v := value.(type) {
		case string:
			out = append(out, "s"+v)
		case float64:
			out = append(out, fmt.Sprintf("n%020.0f", v))
		case json.Number:
			n, e := strconv.ParseInt(string(v), 10, 64)
			if e != nil {
				return "", e
			}
			out = append(out, fmt.Sprintf("n%020d", n))
		default:
			return "", fmt.Errorf("unsupported archive order key %s", key)
		}
	}
	return strings.Join(out, "\x00"), nil
}

// pruneTelemetry applies the receipt and telemetry deletion in one fenced save.
func (s *server) pruneTelemetry(ctx context.Context, id, path string, keepDiagnostics, keepEvents int) (map[string]any, error) {
	if keepDiagnostics < 32 || keepEvents < 1 {
		return nil, fmt.Errorf("retain at least 32 diagnostics per family and one event")
	}
	archive, err := verifyArchive(path)
	if err != nil {
		return nil, err
	}
	if archive.SessionID != id {
		return nil, fmt.Errorf("archive belongs to another session")
	}
	p, err := s.fetch(ctx, id)
	if err != nil {
		return nil, err
	}
	if p.State.Status == "active" || p.State.RunStatus == "running" || len(p.State.PendingToolCalls) > 0 {
		return nil, fmt.Errorf("active or pending session cannot be pruned")
	}
	var unresolved, running int64
	if err = s.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM agent_tool_executions WHERE session_id=$1 AND status IN ('pending','started','uncertain')),(SELECT count(*) FROM agent_runs WHERE session_id=$1 AND status='running')`, id).Scan(&unresolved, &running); err != nil {
		return nil, err
	}
	if unresolved != 0 || running != 0 {
		return nil, fmt.Errorf("unresolved execution or running lifecycle prevents pruning")
	}
	// Archive receipts are permanent chain dependencies. Each previous path must
	// still verify against the stored receipt before another prune can proceed.
	prior, err := s.db.Query(ctx, `SELECT sha256,manifest FROM agent_archive_receipts WHERE session_id=$1 ORDER BY created_at,archive_id`, id)
	if err != nil {
		return nil, err
	}
	type priorArchive struct {
		checksum string
		manifest []byte
	}
	var dependencies []priorArchive
	for prior.Next() {
		var d priorArchive
		if err = prior.Scan(&d.checksum, &d.manifest); err != nil {
			prior.Close()
			return nil, err
		}
		dependencies = append(dependencies, d)
	}
	if err = prior.Err(); err != nil {
		prior.Close()
		return nil, err
	}
	prior.Close()
	for _, dep := range dependencies {
		var receipt map[string]any
		if err = json.Unmarshal(dep.manifest, &receipt); err != nil {
			return nil, err
		}
		previousPath, _ := receipt["archive_path"].(string)
		if previousPath == "" {
			return nil, fmt.Errorf("prior archive dependency path missing")
		}
		verified, e := verifyArchive(previousPath)
		if e != nil || verified.SHA256 != dep.checksum || verified.SessionID != id {
			return nil, fmt.Errorf("prior archive dependency unavailable or corrupt")
		}
	}
	owner := uuid.NewString()
	fence, err := s.acquire(ctx, id, owner, 300)
	if err != nil {
		return nil, err
	}
	defer s.release(id, owner, fence)
	p, err = s.fetch(ctx, id)
	if err != nil {
		return nil, err
	}
	if p.State.Revision != archive.SnapshotRevision {
		return nil, fmt.Errorf("stale archive; export current revision")
	}
	p.State.ExecutionOwner = &owner
	p.State.ExecutionFence = &fence
	p.State.ExecutionMaintenance = true
	p.State.LastArchiveID = &archive.ArchiveID
	p.State.DiagnosticHistoryComplete = false
	ret := map[string]any{"format": archive.Format, "archive_id": archive.ArchiveID, "session_id": archive.SessionID, "snapshot_revision": archive.SnapshotRevision, "sha256": archive.SHA256, "counts": archive.Counts, "table_sha256": archive.TableSHA256, "watermarks": archive.Watermarks, "archive_path": archive.ArchivePath, "keep_diagnostics": keepDiagnostics, "keep_events": keepEvents, "prune": true}
	p.State.RetentionPending = ret
	if err = s.save(ctx, &p, map[string]any{"type": "telemetry_archived", "archive_id": archive.ArchiveID}); err != nil {
		return nil, err
	}
	return map[string]any{"session_id": id, "revision": p.State.Revision, "archive_id": archive.ArchiveID}, nil
}
