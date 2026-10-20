// agent-go is a standalone, concurrent implementation of the State-Driven Agent API.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/state-driven-agent/agent-go/internal/agent"
	"gopkg.in/yaml.v3"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

//go:embed openapi.yaml
var openAPIYAML []byte

const swaggerUI = `<!doctype html><html><head><meta charset="utf-8"><title>State-Driven AI Agent - Swagger UI</title><link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui'})</script></body></html>`

type Message = agent.Message
type Workflow = agent.WorkflowState
type State = agent.Session
type persisted struct {
	State  State
	Config json.RawMessage
}
type server struct {
	db             *pgxpool.Pool
	fixtureDB      *pgxpool.Pool
	root           string
	client         agent.Client
	tools          map[string]agent.Tool
	ioMu           sync.Mutex
	ioSamples      []map[string]any
	saveFault      saveFaultHook
	leaseSeconds   int
	renewInterval  time.Duration
	renewFaultHook func() error
}

func (s *server) recordIOSample(sample map[string]any) {
	s.ioMu.Lock()
	defer s.ioMu.Unlock()
	s.ioSamples = append(s.ioSamples, agent.Clone(sample))
	if len(s.ioSamples) > 4096 {
		s.ioSamples = s.ioSamples[len(s.ioSamples)-4096:]
	}
}

func main() {
	if err := loadDotEnv(); err != nil {
		log.Fatal(err)
	}
	if len(os.Args) > 2 && os.Args[1] == "retention" && os.Args[2] == "verify" {
		if err := runRetentionCLI(context.Background(), nil, os.Args[2:], os.Stdout); err != nil {
			log.Fatal(err)
		}
		return
	}
	root := env("AGENT_CONFIG_ROOT", "../config")
	db, err := pgxpool.New(context.Background(), env("AGENT_SESSION_DATABASE_URL", "postgresql://user:password@192.168.1.50:5432/universal_state"))
	if err != nil {
		log.Fatal(err)
	}
	fixtureDB := db
	if fixtureURL := os.Getenv("AGENT_FIXTURE_DATABASE_URL"); fixtureURL != "" {
		fixtureDB, err = pgxpool.New(context.Background(), fixtureURL)
		if err != nil {
			log.Fatal(err)
		}
		if err = fixtureDB.Ping(context.Background()); err != nil {
			log.Fatal(err)
		}
	}
	s := &server{db: db, fixtureDB: fixtureDB, root: root}
	if err = s.schema(); err != nil {
		log.Fatal(err)
	}
	if len(os.Args) > 1 && os.Args[1] == "retention" {
		if err = runRetentionCLI(context.Background(), s, os.Args[2:], os.Stdout); err != nil {
			log.Fatal(err)
		}
		return
	}
	if _, err = s.reconcileAbandoned(context.Background(), 1000); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /openapi.json", openapi)
	mux.HandleFunc("GET /docs", docs)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /configs", s.configs)
	mux.HandleFunc("POST /sessions", s.create)
	mux.HandleFunc("GET /sessions/{session_id}", s.get)
	mux.HandleFunc("GET /sessions/{session_id}/executions", s.executions)
	mux.HandleFunc("GET /sessions/{session_id}/runs", s.runs)
	mux.HandleFunc("GET /sessions/{session_id}/events", s.events)
	mux.HandleFunc("GET /sessions/{session_id}/archives", s.archives)
	mux.HandleFunc("GET /sessions/{session_id}/retention", s.retention)
	mux.HandleFunc("GET /sessions/{session_id}/transcript", s.transcript)
	mux.HandleFunc("GET /sessions/{session_id}/diagnostics/{kind}", s.compactionDiagnostics)
	mux.HandleFunc("POST /sessions/{session_id}/messages", s.message)
	mux.HandleFunc("POST /sessions/{session_id}/cancel", s.cancel)
	mux.HandleFunc("POST /sessions/{session_id}/run", s.run)
	mux.HandleFunc("POST /sessions/{session_id}/run/stream", s.stream)
	mux.HandleFunc("POST /sessions/{session_id}/resume", s.resume)
	mux.HandleFunc("GET /runs/abandoned", s.abandoned)
	log.Fatal(http.ListenAndServe(":"+env("PORT", "8000"), jsonErrors(mux)))
}
func openapi(w http.ResponseWriter, _ *http.Request) {
	var document any
	if err := yaml.Unmarshal(openAPIYAML, &document); err != nil {
		fail(w, http.StatusInternalServerError, "embedded OpenAPI document is invalid")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	reply(w, http.StatusOK, document)
}
func docs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUI))
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func jsonErrors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		h.ServeHTTP(w, r)
	})
}
func (s *server) schema() error {
	ctx := context.Background()
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(734910003)`); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(734910003)`) }()
	// DU-07 replaces the incompatible Go-era tables with the canonical shared
	// schema exactly once. Keep this marker outside the resettable tables so a
	// normal restart never wipes persisted sessions.
	if _, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS agent_go_schema_control (component TEXT PRIMARY KEY, version INTEGER NOT NULL)`); err != nil {
		return err
	}
	var schemaVersion int
	err = conn.QueryRow(ctx, `SELECT version FROM agent_go_schema_control WHERE component='agent-go'`).Scan(&schemaVersion)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && schemaVersion < 1) {
		tx, beginErr := conn.Begin(ctx)
		if beginErr != nil {
			return beginErr
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, `DROP TABLE IF EXISTS agent_session_context,agent_session_calls,agent_tool_executions,agent_runs,agent_session_diagnostics,agent_session_transcript,agent_session_events,agent_archive_receipts,agent_sessions,agent_schema_migrations CASCADE`); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO agent_go_schema_control(component,version) VALUES('agent-go',1) ON CONFLICT(component) DO UPDATE SET version=EXCLUDED.version`); err != nil {
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	base := `CREATE TABLE IF NOT EXISTS agent_sessions (session_id UUID PRIMARY KEY,state JSONB NOT NULL,config JSONB NOT NULL,config_ref TEXT,config_sha256 TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),revision BIGINT NOT NULL DEFAULT 0,last_commit_id UUID,last_commit_sha256 TEXT,storage_version INTEGER NOT NULL DEFAULT 5,lease_owner UUID,lease_fence BIGINT NOT NULL DEFAULT 0,lease_expires_at TIMESTAMPTZ); CREATE TABLE IF NOT EXISTS agent_session_events (event_id BIGSERIAL PRIMARY KEY,session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,event JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),revision BIGINT,commit_id UUID); CREATE INDEX IF NOT EXISTS agent_session_events_session_id_idx ON agent_session_events(session_id,event_id); CREATE UNIQUE INDEX IF NOT EXISTS agent_session_events_commit_idx ON agent_session_events(session_id,commit_id) WHERE commit_id IS NOT NULL`
	if _, err = conn.Exec(ctx, base); err != nil {
		return err
	}
	if _, err = conn.Exec(ctx, historyDDL); err != nil {
		return err
	}
	return nil
}
func reply(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, msg string) {
	reply(w, status, map[string]any{"detail": msg})
}
func (s *server) health(w http.ResponseWriter, r *http.Request) {
	files, _ := filepath.Glob(filepath.Join(s.root, "*.yaml"))
	reply(w, 200, map[string]any{"status": "ok", "config_root": s.root, "available_configs": len(files)})
}
func (s *server) load(ref string) (json.RawMessage, string, error) {
	snapshot, err := agent.LoadSnapshot(s.root, ref)
	if err != nil {
		return nil, "", err
	}
	available := agent.PostgresTools(s.fixtureDB)
	for i := range snapshot.Config.Tools {
		tool := &snapshot.Config.Tools[i]
		if available[tool.Name] == nil {
			return nil, "", fmt.Errorf("config references unavailable compiled-in tools: %s", tool.Name)
		}
		// These fixture adapters are read-only. Unknown future adapters retain
		// the safe reconcile default in Engine.call.
		if tool.Recovery == nil && readOnlyFixtureTool(tool.Name) {
			tool.Recovery = &agent.ToolRecovery{Mode: "read_only", MaxAttempts: 1}
		}
	}
	encoded, err := json.Marshal(snapshot.Config)
	return encoded, snapshot.SHA256, err
}

func readOnlyFixtureTool(name string) bool {
	switch name {
	case "inspect_log", "analyze_500_ips", "validate_top_offending_ip",
		"inspect_sql_schema", "query_chicago_q1_revenue", "validate_chicago_q1_revenue",
		"inspect_employee_audit_schema", "audit_employee_phase_1", "calculate_new_york_median_salary",
		"generate_employee_phase_3_report", "validate_employee_audit":
		return true
	default:
		return false
	}
}

func (s *server) configs(w http.ResponseWriter, r *http.Request) {
	out := []any{}
	_ = filepath.WalkDir(s.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
			return nil
		}
		ref, err := filepath.Rel(s.root, path)
		if err != nil {
			return nil
		}
		c, h, err := s.load(filepath.ToSlash(ref))
		if err != nil {
			return nil
		}
		var a agent.Config
		if json.Unmarshal(c, &a) != nil {
			return nil
		}
		tools := []string{}
		for _, tool := range a.Tools {
			tools = append(tools, tool.Name)
		}
		phases := []string{}
		if a.Workflow != nil {
			for _, phase := range a.Workflow.Phases {
				phases = append(phases, phase.ID)
			}
		}
		out = append(out, map[string]any{"config_ref": filepath.ToSlash(ref), "sha256": h, "agent_name": a.Name, "tools": tools, "workflow_phases": phases, "verbose_setup": a.Output.VerboseSetup})
		return nil
	})
	reply(w, 200, out)
}
func (s *server) create(w http.ResponseWriter, r *http.Request) {
	var q struct {
		UserPrompt string `json:"user_prompt"`
		ConfigRef  string `json:"config_ref"`
	}
	if json.NewDecoder(r.Body).Decode(&q) != nil || q.UserPrompt == "" {
		fail(w, 422, "user_prompt is required")
		return
	}
	if q.ConfigRef == "" {
		q.ConfigRef = "log-analysis.yaml"
	}
	cfg, hash, e := s.load(q.ConfigRef)
	if e != nil {
		fail(w, 422, "Invalid config_ref: "+e.Error())
		return
	}
	id := uuid.NewString()
	goal := q.UserPrompt
	st := agent.NewSession(id, goal, q.ConfigRef, hash)
	var a map[string]any
	json.Unmarshal(cfg, &a)
	if wf, ok := a["workflow"].(map[string]any); ok {
		entry := fmt.Sprint(wf["entry_phase"])
		st.Workflow = &Workflow{ActivePhase: &entry, CompletedPhases: []string{}, ObservedEvidence: []string{}, Transitions: []agent.Transition{}}
	}
	p := persisted{State: st, Config: cfg}
	if e = s.save(r.Context(), &p, map[string]any{"type": "session_created", "config_ref": q.ConfigRef, "config_sha256": hash}); e != nil {
		fail(w, 500, e.Error())
		return
	}
	reply(w, 201, map[string]any{"session": p.State, "message": "Session created with immutable config " + q.ConfigRef + "@" + hash[:12] + "."})
}
func (s *server) get(w http.ResponseWriter, r *http.Request) {
	id, parseErr := parseSessionID(r.PathValue("session_id"))
	if parseErr != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	p, e := s.fetch(r.Context(), id)
	if e != nil {
		fail(w, 404, "Session not found")
		return
	}
	reply(w, 200, p.State)
}
func (s *server) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("session_id"))
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, ErrInvalidSessionID.Error())
		return
	}
	runs, err := s.cancelRun(r.Context(), id.String())
	if err != nil {
		if errors.Is(err, ErrSessionNotFound) {
			fail(w, http.StatusNotFound, "Session not found")
		} else {
			fail(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	reply(w, http.StatusOK, map[string]any{"session_id": id.String(), "cancel_requested_for": runs,
		"message": "Cancellation is cooperative; in-flight effects are not undone."})
}
func (s *server) executions(w http.ResponseWriter, r *http.Request) {
	id, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	if _, err = s.fetch(r.Context(), id); err != nil {
		fail(w, http.StatusNotFound, "Session not found")
		return
	}
	after, err := uuidCursor(r, "after")
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	limit, err := pageInt(r, "limit", 100, 1, 1000)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT execution_id::text,run_id::text,call_id,tool,arguments,policy,idempotency_key::text,attempts,status,result,fence,created_at,updated_at,started_at,finished_at FROM agent_tool_executions WHERE session_id=$1 AND ($2::uuid IS NULL OR execution_id>$2::uuid) ORDER BY execution_id LIMIT $3`, id, after, limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	entries := []map[string]any{}
	for rows.Next() {
		var executionID, runID, callID, tool, idempotencyKey, status string
		var arguments, policy []byte
		var result *string
		var attempts int
		var fence int64
		var created, updated time.Time
		var started, finished *time.Time
		if err = rows.Scan(&executionID, &runID, &callID, &tool, &arguments, &policy, &idempotencyKey, &attempts, &status, &result, &fence, &created, &updated, &started, &finished); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		var args, policyValue any
		if err = json.Unmarshal(arguments, &args); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err = json.Unmarshal(policy, &policyValue); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		entries = append(entries, map[string]any{"execution_id": executionID, "run_id": runID, "call_id": callID, "tool": tool, "arguments": args, "policy": policyValue, "idempotency_key": idempotencyKey, "attempts": attempts, "status": status, "result": result, "fence": fence, "created_at": created, "updated_at": updated, "started_at": started, "finished_at": finished})
	}
	if err = rows.Err(); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	next := any(after)
	if len(entries) > 0 {
		next = entries[len(entries)-1]["execution_id"]
	}
	reply(w, http.StatusOK, map[string]any{"session_id": id, "executions": entries, "next_after": next})
}

func (s *server) runs(w http.ResponseWriter, r *http.Request) {
	id, parseErr := parseSessionID(r.PathValue("session_id"))
	if parseErr != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	if _, err := s.fetch(r.Context(), id); err != nil {
		fail(w, http.StatusNotFound, "Session not found")
		return
	}
	after, err := uuidCursor(r, "after")
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	limit, err := pageInt(r, "limit", 100, 1, 1000)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT run_id::text,status,owner::text,fence,started_at,ended_at,termination_reason,last_decision_step,cancel_requested,cancel_requested_at,metadata FROM agent_runs r WHERE session_id=$1 AND ($2::uuid IS NULL OR (started_at,run_id)>(SELECT started_at,run_id FROM agent_runs WHERE session_id=$1 AND run_id=$2::uuid)) ORDER BY started_at,run_id LIMIT $3`, id, after, limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	runs := []map[string]any{}
	for rows.Next() {
		var runID, status string
		var owner, reason *string
		var fence, step int64
		var started, ended, cancelAt *time.Time
		var cancelled bool
		var metadata []byte
		if err = rows.Scan(&runID, &status, &owner, &fence, &started, &ended, &reason, &step, &cancelled, &cancelAt, &metadata); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		var meta any
		_ = json.Unmarshal(metadata, &meta)
		runs = append(runs, map[string]any{"run_id": runID, "status": status, "owner_id": owner, "fence": fence, "started_at": started, "ended_at": ended, "termination_reason": reason, "last_decision_step": step, "cancel_requested": cancelled, "cancel_requested_at": cancelAt, "metadata": meta})
	}
	if err = rows.Err(); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	next := any(after)
	if len(runs) > 0 {
		next = runs[len(runs)-1]["run_id"]
	}
	reply(w, http.StatusOK, map[string]any{"session_id": id, "runs": runs, "next_after": next})
}

func (s *server) events(w http.ResponseWriter, r *http.Request) {
	id, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	p, err := s.fetch(r.Context(), id)
	if err != nil {
		fail(w, http.StatusNotFound, "Session not found")
		return
	}
	after, err := pageInt(r, "after", 0, 0, 1<<62)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	limit, err := pageInt(r, "limit", 100, 1, 1000)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	var watermark int64
	if err = s.db.QueryRow(r.Context(), `SELECT COALESCE(max(event_id),0) FROM agent_session_events WHERE session_id=$1`, id).Scan(&watermark); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	through := watermark
	if raw := r.URL.Query().Get("through"); raw != "" {
		through, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || through < 0 {
			fail(w, http.StatusUnprocessableEntity, "through must be a nonnegative integer")
			return
		}
		if through > watermark {
			through = watermark
		}
	}
	rows, err := s.db.Query(r.Context(), `SELECT event_id,event,revision,created_at FROM agent_session_events WHERE session_id=$1 AND event_id>$2 AND event_id<=$3 ORDER BY event_id LIMIT $4`, id, after, through, limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	entries := []map[string]any{}
	for rows.Next() {
		var eventID, revision int64
		var raw []byte
		var created time.Time
		if err = rows.Scan(&eventID, &raw, &revision, &created); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		var event any
		if err = json.Unmarshal(raw, &event); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		entries = append(entries, map[string]any{"event_id": eventID, "event": event, "revision": revision, "created_at": created.UTC()})
	}
	if err = rows.Err(); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	next := after
	if len(entries) > 0 {
		next = entries[len(entries)-1]["event_id"].(int64)
	}
	reply(w, http.StatusOK, map[string]any{"session_id": id, "through": through, "entries": entries, "next_after": next, "archive_id": p.State.LastArchiveID})
}

func (s *server) archives(w http.ResponseWriter, r *http.Request) {
	id, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	if _, err = s.fetch(r.Context(), id); err != nil {
		fail(w, http.StatusNotFound, "Session not found")
		return
	}
	after, err := uuidCursor(r, "after")
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	limit, err := pageInt(r, "limit", 100, 1, 1000)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT archive_id::text,snapshot_revision,sha256,manifest,created_at FROM agent_archive_receipts WHERE session_id=$1 AND ($2::uuid IS NULL OR archive_id>$2::uuid) ORDER BY archive_id LIMIT $3`, id, after, limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	entries := []map[string]any{}
	for rows.Next() {
		var archiveID, checksum string
		var revision int64
		var manifest []byte
		var created time.Time
		if err = rows.Scan(&archiveID, &revision, &checksum, &manifest, &created); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		var value any
		if err = json.Unmarshal(manifest, &value); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		entries = append(entries, map[string]any{"archive_id": archiveID, "snapshot_revision": revision, "sha256": checksum, "manifest": value, "created_at": created.UTC()})
	}
	if err = rows.Err(); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	next := any(after)
	if len(entries) > 0 {
		next = entries[len(entries)-1]["archive_id"]
	}
	reply(w, http.StatusOK, map[string]any{"session_id": id, "archives": entries, "next_after": next})
}

func (s *server) retention(w http.ResponseWriter, r *http.Request) {
	id, err := parseSessionID(r.PathValue("session_id"))
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	plan, err := s.retentionPlan(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			fail(w, http.StatusNotFound, "Session not found")
		} else {
			fail(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	reply(w, http.StatusOK, plan)
}

func (s *server) abandoned(w http.ResponseWriter, r *http.Request) {
	limit, err := pageInt(r, "limit", 100, 1, 1000)
	if err != nil {
		fail(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	rows, err := s.db.Query(r.Context(), `SELECT r.session_id::text,r.run_id::text,r.status,r.last_decision_step,r.started_at,r.fence FROM agent_runs r JOIN agent_sessions s USING(session_id) WHERE r.status='running' AND (s.lease_owner IS NULL OR s.lease_expires_at<=clock_timestamp()) ORDER BY r.started_at,r.run_id LIMIT $1`, limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	runs := []map[string]any{}
	for rows.Next() {
		var sessionID, runID, status string
		var step, fence int64
		var started time.Time
		if err = rows.Scan(&sessionID, &runID, &status, &step, &started, &fence); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		runs = append(runs, map[string]any{"session_id": sessionID, "run_id": runID, "status": status, "last_decision_step": step, "started_at": started.UTC(), "fence": fence})
	}
	if err = rows.Err(); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *server) compactionDiagnostics(w http.ResponseWriter, r *http.Request) {
	id, parseErr := parseSessionID(r.PathValue("session_id"))
	if parseErr != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	p, err := s.fetch(r.Context(), id)
	if err != nil {
		fail(w, http.StatusNotFound, "Session not found")
		return
	}
	family := r.PathValue("kind")
	if family != "compaction_events" && family != "compaction_attempts" && family != "compaction_decisions" {
		fail(w, http.StatusUnprocessableEntity, "Unknown diagnostic family")
		return
	}
	after, err := pageInt(r, "after", 0, 0, 1<<62)
	if err != nil {
		fail(w, 422, err.Error())
		return
	}
	limit, err := pageInt(r, "limit", 100, 1, 1000)
	if err != nil {
		fail(w, 422, err.Error())
		return
	}
	total := p.State.DiagnosticCounts[family]
	through, err := pageInt(r, "through", total, 0, 1<<62)
	if err != nil {
		fail(w, 422, err.Error())
		return
	}
	if through > total {
		through = total
	}
	rows, err := s.db.Query(r.Context(), `SELECT ordinal,payload FROM agent_session_diagnostics WHERE session_id=$1 AND family=$2 AND ordinal>$3 AND ordinal<=$4 ORDER BY ordinal LIMIT $5`, p.State.ID, family, after, through, limit)
	if err != nil {
		fail(w, 500, err.Error())
		return
	}
	defer rows.Close()
	entries := []map[string]any{}
	next := after
	for rows.Next() {
		var ordinal int64
		var b []byte
		if err = rows.Scan(&ordinal, &b); err != nil {
			fail(w, 500, err.Error())
			return
		}
		var payload any
		if err = json.Unmarshal(b, &payload); err != nil {
			fail(w, 500, err.Error())
			return
		}
		entries = append(entries, map[string]any{"ordinal": ordinal, "data": payload})
		next = ordinal
	}
	if err = rows.Err(); err != nil {
		fail(w, 500, err.Error())
		return
	}
	reply(w, 200, map[string]any{"session_id": p.State.ID, "family": family, "total_recorded": total, "through": through, "entries": entries, "next_after": next, "archive_id": p.State.LastArchiveID, "history_complete": p.State.DiagnosticHistoryComplete && p.State.LastArchiveID == nil})
}

func (s *server) message(w http.ResponseWriter, r *http.Request) {
	id, parseErr := parseSessionID(r.PathValue("session_id"))
	if parseErr != nil {
		fail(w, http.StatusUnprocessableEntity, "session_id must be a UUID")
		return
	}
	p, e := s.fetch(r.Context(), id)
	if e != nil {
		fail(w, 404, "Session not found")
		return
	}
	if p.State.Status != "active" {
		fail(w, 409, "Cannot append to a terminal session; use /resume with content instead")
		return
	}
	if len(p.State.PendingToolCalls) > 0 {
		fail(w, http.StatusConflict, "Pending tool calls must be recovered before appending new content.")
		return
	}
	var q struct {
		Content string `json:"content"`
	}
	json.NewDecoder(r.Body).Decode(&q)
	if q.Content == "" {
		fail(w, 422, "content is required")
		return
	}
	agent.AppendMessage(&p.State, Message{Role: "user", Content: &q.Content})
	p.State.NoProgressSteps, p.State.ProgressFingerprint = 0, ""
	if e = s.save(r.Context(), &p, map[string]any{"type": "user_message_appended"}); e != nil {
		if errors.Is(e, ErrFencingConflict) || errors.Is(e, ErrRevisionConflict) || errors.Is(e, ErrOwnershipConflict) || errors.Is(e, ErrLeaseLost) {
			fail(w, http.StatusConflict, "Session is owned by a running worker; retry after release or lease expiry.")
		} else {
			fail(w, 500, e.Error())
		}
		return
	}
	reply(w, 200, map[string]any{"session": p.State, "message": "User input appended to session memory."})
}
func (s *server) execute(ctx context.Context, p *persisted, max int, emit func(any) error) error {
	var config agent.Config
	if err := json.Unmarshal(p.Config, &config); err != nil {
		return err
	}
	client := s.client
	if client == nil {
		client = agent.OpenAIClient{Config: config, Diagnostics: &agent.ModelDiagnostics{}}
	}
	tools := s.tools
	if tools == nil {
		tools = agent.PostgresTools(s.fixtureDB)
	}
	engine := agent.Engine{Config: config, Client: client, Tools: tools}
	ctx = agent.WithExecutionCheckpoint(ctx, func() error { return s.checkRun(ctx, &p.State) })
	if p.State.RunID != nil && p.State.RunStatus == "running" {
		oldRun := *p.State.RunID
		ended := time.Now().UTC().Truncate(time.Microsecond)
		reason := "previous_worker_abandoned"
		p.State.RunStatus, p.State.RunEndedAt, p.State.RunTerminationReason = "interrupted", &ended, &reason
		agent.ArchiveRecord(&p.State, "run_interrupted", map[string]any{"run_id": oldRun, "reason": reason})
		if err := s.save(ctx, p, map[string]any{"type": "run_interrupted", "run_id": oldRun, "reason": reason}); err != nil {
			return err
		}
	}
	err := engine.Run(ctx, &p.State, max, func(event map[string]any) error { return emit(event) })
	cancelled := errors.Is(err, agent.ErrRunCancelled)
	if err != nil && !cancelled {
		p.State.Status = "failed"
		message := "Agent run failed: " + err.Error()
		p.State.LastError = map[string]any{"message": message}
		p.State.FinalAnswer = &message
	}
	if ctx.Err() != nil && p.State.ExecutionOwner != nil {
		return ctx.Err()
	}
	// Run's deferred terminal audit record must be durable as well.
	if saveErr := s.save(context.WithoutCancel(ctx), p, map[string]any{"type": "run_finished", "status": p.State.Status}); saveErr != nil {
		return saveErr
	}
	if cancelled {
		return nil
	}
	return err
}
func (s *server) run(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("session_id")
	p, runCtx, cleanup, e := s.ownSession(context.Background(), id)
	if e != nil {
		if errors.Is(e, ErrOwnershipConflict) {
			fail(w, http.StatusConflict, e.Error())
		} else if errors.Is(e, ErrInvalidSessionID) {
			fail(w, http.StatusUnprocessableEntity, e.Error())
		} else if errors.Is(e, ErrSessionNotFound) {
			fail(w, http.StatusNotFound, e.Error())
		} else {
			fail(w, http.StatusInternalServerError, e.Error())
		}
		return
	}
	defer cleanup()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-r.Context().Done():
			cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = s.cancelRun(cancelCtx, p.State.ID)
		case <-done:
		}
	}()
	if p.State.Status != "active" {
		reply(w, 200, map[string]any{"session": p.State, "message": "Session is terminal; use POST /sessions/{id}/resume to continue it."})
		return
	}
	var q struct {
		MaxSteps int `json:"max_steps"`
	}
	json.NewDecoder(r.Body).Decode(&q)
	if e = s.execute(runCtx, &p, q.MaxSteps, func(e any) error { return s.save(runCtx, &p, e) }); e != nil {
		if errors.Is(e, ErrFencingConflict) || errors.Is(e, ErrRevisionConflict) || errors.Is(e, ErrOwnershipConflict) || errors.Is(e, ErrLeaseLost) {
			fail(w, http.StatusConflict, e.Error())
		} else {
			fail(w, http.StatusInternalServerError, e.Error())
		}
		return
	}
	reply(w, 200, map[string]any{"session": p.State, "message": p.State.FinalAnswer})
}
func (s *server) stream(w http.ResponseWriter, r *http.Request) { s.streamCommon(w, r, false) }
func (s *server) resume(w http.ResponseWriter, r *http.Request) { s.streamCommon(w, r, true) }
func (s *server) streamCommon(w http.ResponseWriter, r *http.Request, resume bool) {
	id := r.PathValue("session_id")
	p, runCtx, cleanup, e := s.ownSession(context.Background(), id)
	if e != nil {
		if errors.Is(e, ErrOwnershipConflict) {
			fail(w, http.StatusConflict, e.Error())
		} else if errors.Is(e, ErrInvalidSessionID) {
			fail(w, http.StatusUnprocessableEntity, e.Error())
		} else if errors.Is(e, ErrSessionNotFound) {
			fail(w, http.StatusNotFound, e.Error())
		} else {
			fail(w, http.StatusInternalServerError, e.Error())
		}
		return
	}
	defer cleanup()
	var q struct {
		MaxSteps int    `json:"max_steps"`
		Content  string `json:"content"`
	}
	json.NewDecoder(r.Body).Decode(&q)
	if !resume && p.State.Status != "active" {
		fail(w, 409, "Session is terminal; use POST /sessions/{id}/resume to continue it")
		return
	}
	if resume {
		if q.Content != "" && len(p.State.PendingToolCalls) > 0 {
			fail(w, http.StatusConflict, "Pending tool calls must be recovered before accepting new content; resume without content first.")
			return
		}
		if q.Content != "" {
			agent.AppendMessage(&p.State, Message{Role: "user", Content: &q.Content})
		}
		p.State.Status = "active"
		p.State.FinalAnswer = nil
		p.State.ConsecutiveInvalidResponses = 0
		p.State.NoProgressSteps = 0
		p.State.ProgressFingerprint = ""
		p.State.LastError = nil
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	f := w.(http.Flusher)
	events := make(chan map[string]any, 512)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		defer close(events)
		emit := func(event any) error {
			if err := s.save(runCtx, &p, event); err != nil {
				return err
			}
			if mapped, ok := event.(map[string]any); ok {
				events <- mapped
			}
			return nil
		}
		if resume {
			if err := emit(map[string]any{"type": "status", "message": "Session restored from PostgreSQL; resuming agent work."}); err != nil {
				events <- sseError(err)
				return
			}
		}
		var cfg agent.Config
		if json.Unmarshal(p.Config, &cfg) == nil && cfg.Output.VerboseSetup {
			if err := emit(map[string]any{"type": "status", "message": "Agent initialized from persisted session state."}); err != nil {
				events <- sseError(err)
				return
			}
		}
		if err := s.execute(runCtx, &p, q.MaxSteps, emit); err != nil {
			events <- sseError(err)
			return
		}
		if err := emit(map[string]any{"type": "status", "message": "Agent run finished.", "status": p.State.Status}); err != nil {
			events <- sseError(err)
		}
	}()
	disconnected := false
	disconnect := r.Context().Done()
	for eventCh := events; eventCh != nil; {
		select {
		case event, ok := <-eventCh:
			if !ok {
				eventCh = nil
				continue
			}
			if disconnected {
				continue
			}
			b, _ := json.Marshal(event)
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				disconnected, disconnect = true, nil
				cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, _ = s.cancelRun(cancelCtx, p.State.ID)
				cancel()
				continue
			}
			f.Flush()
		case <-disconnect:
			disconnected, disconnect = true, nil
			cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = s.cancelRun(cancelCtx, p.State.ID)
			cancel()
		}
	}
	<-runDone
}

func sseError(err error) map[string]any {
	code := "run_error"
	if errors.Is(err, ErrFencingConflict) || errors.Is(err, ErrRevisionConflict) || errors.Is(err, ErrOwnershipConflict) || errors.Is(err, ErrLeaseLost) {
		code = "ownership_conflict"
	}
	return map[string]any{"type": "error", "code": code, "message": err.Error(), "recoverable": true}
}
