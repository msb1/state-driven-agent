// agent-go is a standalone, concurrent implementation of the State-Driven Agent API.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/state-driven-agent/agent-go/internal/agent"
	"gopkg.in/yaml.v3"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Message struct {
	Role       string  `json:"role"`
	Content    *string `json:"content,omitempty"`
	Name       *string `json:"name,omitempty"`
	ToolCallID *string `json:"tool_call_id,omitempty"`
	ToolCalls  any     `json:"tool_calls,omitempty"`
}
type Workflow struct {
	ActivePhase      *string  `json:"active_phase"`
	CompletedPhases  []string `json:"completed_phases"`
	ObservedEvidence []string `json:"observed_evidence"`
	Transitions      []any    `json:"transitions"`
}
type State struct {
	ID               string    `json:"id"`
	Goal             string    `json:"goal"`
	ConfigRef        string    `json:"config_ref,omitempty"`
	ConfigSHA        string    `json:"config_sha256,omitempty"`
	Memory           []Message `json:"memory"`
	CompactedContext *string   `json:"compacted_context,omitempty"`
	CompactionEvents []string  `json:"compaction_events"`
	Workflow         *Workflow `json:"workflow,omitempty"`
	StepCount        int       `json:"step_count"`
	RunCount         int       `json:"run_count"`
	Status           string    `json:"status"`
	FinalAnswer      *string   `json:"final_answer,omitempty"`
}
type persisted struct {
	State  State
	Config json.RawMessage
}
type server struct {
	db   *pgxpool.Pool
	root string
	mu   sync.Map
}

func main() {
	root := env("AGENT_CONFIG_ROOT", "../config")
	db, err := pgxpool.New(context.Background(), env("AGENT_SESSION_DATABASE_URL", "postgresql://user:password@192.168.1.50:5432/elite_rag"))
	if err != nil {
		log.Fatal(err)
	}
	s := &server{db: db, root: root}
	if err = s.schema(); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /configs", s.configs)
	mux.HandleFunc("POST /sessions", s.create)
	mux.HandleFunc("GET /sessions/{id}", s.get)
	mux.HandleFunc("POST /sessions/{id}/messages", s.message)
	mux.HandleFunc("POST /sessions/{id}/run", s.run)
	mux.HandleFunc("POST /sessions/{id}/run/stream", s.stream)
	mux.HandleFunc("POST /sessions/{id}/resume", s.resume)
	log.Fatal(http.ListenAndServe(":"+env("PORT", "8080"), jsonErrors(mux)))
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
	_, e := s.db.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS agent_sessions (session_id UUID PRIMARY KEY,state JSONB NOT NULL,config JSONB NOT NULL,config_ref TEXT,config_sha256 TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now()); CREATE TABLE IF NOT EXISTS agent_session_events (event_id BIGSERIAL PRIMARY KEY,session_id UUID NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,event JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now()); CREATE INDEX IF NOT EXISTS agent_session_events_session_id_idx ON agent_session_events(session_id,event_id)`)
	return e
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
	if ref == "" || filepath.IsAbs(ref) || strings.Contains(filepath.Clean(ref), "..") {
		return nil, "", fmt.Errorf("config_ref must be a relative YAML path")
	}
	b, e := os.ReadFile(filepath.Join(s.root, ref))
	if e != nil {
		return nil, "", e
	}
	var raw map[string]any
	if e = yaml.Unmarshal(b, &raw); e != nil {
		return nil, "", e
	}
	a, ok := raw["agent"]
	if !ok {
		return nil, "", fmt.Errorf("missing agent mapping")
	}
	j, e := json.Marshal(a)
	sum := sha256.Sum256(b)
	return j, hex.EncodeToString(sum[:]), e
}
func (s *server) configs(w http.ResponseWriter, r *http.Request) {
	paths, _ := filepath.Glob(filepath.Join(s.root, "*.yaml"))
	out := []any{}
	for _, p := range paths {
		ref := filepath.Base(p)
		c, h, e := s.load(ref)
		if e != nil {
			continue
		}
		var a map[string]any
		json.Unmarshal(c, &a)
		tools := []string{}
		if ts, ok := a["tools"].([]any); ok {
			for _, v := range ts {
				if x, ok := v.(map[string]any); ok {
					tools = append(tools, fmt.Sprint(x["name"]))
				}
			}
		}
		phases := []string{}
		if wf, ok := a["workflow"].(map[string]any); ok {
			if ps, ok := wf["phases"].([]any); ok {
				for _, v := range ps {
					phases = append(phases, fmt.Sprint(v.(map[string]any)["id"]))
				}
			}
		}
		out = append(out, map[string]any{"config_ref": ref, "sha256": h, "agent_name": a["name"], "tools": tools, "workflow_phases": phases, "verbose_setup": false})
	}
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
		q.ConfigRef = "test-case-1.yaml"
	}
	cfg, hash, e := s.load(q.ConfigRef)
	if e != nil {
		fail(w, 422, "Invalid config_ref: "+e.Error())
		return
	}
	id := uuid.NewString()
	goal := q.UserPrompt
	st := State{ID: id, Goal: goal, ConfigRef: q.ConfigRef, ConfigSHA: hash, Memory: []Message{{Role: "user", Content: &goal}}, CompactionEvents: []string{}, Status: "active"}
	var a map[string]any
	json.Unmarshal(cfg, &a)
	if wf, ok := a["workflow"].(map[string]any); ok {
		entry := fmt.Sprint(wf["entry_phase"])
		st.Workflow = &Workflow{ActivePhase: &entry, CompletedPhases: []string{}, ObservedEvidence: []string{}, Transitions: []any{}}
	}
	if e = s.save(r.Context(), persisted{st, cfg}, map[string]any{"type": "session_created", "config_ref": q.ConfigRef, "config_sha256": hash}); e != nil {
		fail(w, 500, e.Error())
		return
	}
	reply(w, 201, map[string]any{"session": st, "message": "Session created with immutable config " + q.ConfigRef + "@" + hash[:12] + "."})
}
func (s *server) fetch(ctx context.Context, id string) (persisted, error) {
	var st, cfg []byte
	e := s.db.QueryRow(ctx, "SELECT state,config FROM agent_sessions WHERE session_id=$1", id).Scan(&st, &cfg)
	var p persisted
	if e == nil {
		e = json.Unmarshal(st, &p.State)
		p.Config = cfg
	}
	return p, e
}
func (s *server) save(ctx context.Context, p persisted, event any) error {
	st, _ := json.Marshal(p.State)
	_, e := s.db.Exec(ctx, `INSERT INTO agent_sessions(session_id,state,config,config_ref,config_sha256) VALUES($1,$2,$3,$4,$5) ON CONFLICT(session_id) DO UPDATE SET state=EXCLUDED.state,config=EXCLUDED.config,config_ref=EXCLUDED.config_ref,config_sha256=EXCLUDED.config_sha256,updated_at=now()`, p.State.ID, st, p.Config, p.State.ConfigRef, p.State.ConfigSHA)
	if e == nil && event != nil {
		b, _ := json.Marshal(event)
		_, e = s.db.Exec(ctx, "INSERT INTO agent_session_events(session_id,event) VALUES($1,$2)", p.State.ID, b)
	}
	return e
}
func (s *server) get(w http.ResponseWriter, r *http.Request) {
	p, e := s.fetch(r.Context(), r.PathValue("id"))
	if e != nil {
		fail(w, 404, "Session not found")
		return
	}
	reply(w, 200, p.State)
}
func (s *server) message(w http.ResponseWriter, r *http.Request) {
	p, e := s.fetch(r.Context(), r.PathValue("id"))
	if e != nil {
		fail(w, 404, "Session not found")
		return
	}
	if p.State.Status != "active" {
		fail(w, 409, "Cannot append to a terminal session; use /resume with content instead")
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
	p.State.Memory = append(p.State.Memory, Message{Role: "user", Content: &q.Content})
	s.save(r.Context(), p, map[string]any{"type": "user_message_appended"})
	reply(w, 200, map[string]any{"session": p.State, "message": "User input appended to session memory."})
}
func lock(s *server, id string) *sync.Mutex {
	v, _ := s.mu.LoadOrStore(id, &sync.Mutex{})
	return v.(*sync.Mutex)
}
func (s *server) execute(ctx context.Context, p *persisted, max int, emit func(any)) {
	var config agent.Config
	if err := json.Unmarshal(p.Config, &config); err != nil {
		emit(map[string]any{"type": "error", "message": "Persisted config is invalid: " + err.Error(), "recoverable": true})
		return
	}
	var state agent.Session
	raw, _ := json.Marshal(p.State)
	if err := json.Unmarshal(raw, &state); err != nil {
		emit(map[string]any{"type": "error", "message": "Persisted state is invalid: " + err.Error(), "recoverable": true})
		return
	}
	engine := agent.Engine{Config: config, Client: agent.OpenAIClient{Config: config}, Tools: agent.PostgresTools(s.db)}
	err := engine.Run(ctx, &state, max, func(event map[string]any) error {
		updated, _ := json.Marshal(state)
		_ = json.Unmarshal(updated, &p.State)
		emit(event)
		return nil
	})
	updated, _ := json.Marshal(state)
	_ = json.Unmarshal(updated, &p.State)
	if err != nil {
		message := "Agent run failed: " + err.Error()
		p.State.Status, p.State.FinalAnswer = "failed", &message
		emit(map[string]any{"type": "error", "message": message, "recoverable": true})
	}
}
func (s *server) run(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	lock(s, id).Lock()
	defer lock(s, id).Unlock()
	p, e := s.fetch(r.Context(), id)
	if e != nil {
		fail(w, 404, "Session not found")
		return
	}
	if p.State.Status != "active" {
		reply(w, 200, map[string]any{"session": p.State, "message": "Session is terminal; use POST /sessions/{id}/resume to continue it."})
		return
	}
	var q struct {
		MaxSteps int `json:"max_steps"`
	}
	json.NewDecoder(r.Body).Decode(&q)
	s.execute(r.Context(), &p, q.MaxSteps, func(e any) { s.save(r.Context(), p, e) })
	s.save(r.Context(), p, map[string]any{"type": "run_finished", "status": p.State.Status})
	reply(w, 200, map[string]any{"session": p.State, "message": p.State.FinalAnswer})
}
func (s *server) stream(w http.ResponseWriter, r *http.Request) { s.streamCommon(w, r, false) }
func (s *server) resume(w http.ResponseWriter, r *http.Request) { s.streamCommon(w, r, true) }
func (s *server) streamCommon(w http.ResponseWriter, r *http.Request, resume bool) {
	id := r.PathValue("id")
	l := lock(s, id)
	l.Lock()
	defer l.Unlock()
	p, e := s.fetch(r.Context(), id)
	if e != nil {
		fail(w, 404, "Session not found")
		return
	}
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
		if q.Content != "" {
			p.State.Memory = append(p.State.Memory, Message{Role: "user", Content: &q.Content})
		}
		p.State.Status = "active"
		p.State.FinalAnswer = nil
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	f := w.(http.Flusher)
	emit := func(e any) {
		s.save(r.Context(), p, e)
		b, _ := json.Marshal(e)
		fmt.Fprintf(w, "data: %s\n\n", b)
		f.Flush()
	}
	if resume {
		emit(map[string]any{"type": "status", "message": "Session restored from PostgreSQL; resuming agent work."})
	}
	emit(map[string]any{"type": "status", "message": "Agent initialized from persisted session state."})
	s.execute(r.Context(), &p, q.MaxSteps, emit)
	s.save(r.Context(), p, map[string]any{"type": "run_finished", "status": p.State.Status})
	emit(map[string]any{"type": "status", "message": "Agent run finished.", "status": p.State.Status})
	time.Sleep(1 * time.Millisecond)
}
