package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

type commandContextKey struct{}
type durableCommand struct {
	ID, Owner, Route, Namespace string
	Fence                       int64
	Admission                   map[string]any
	mu                          sync.Mutex
	lost                        error
	completed                   bool
}

func currentCommand(ctx context.Context) *durableCommand {
	c, _ := ctx.Value(commandContextKey{}).(*durableCommand)
	return c
}
func (s *server) commandRow(ctx context.Context, key, id string) (map[string]any, error) {
	var raw []byte
	err := s.db.QueryRow(ctx, `SELECT to_jsonb(c)||jsonb_build_object('run_id',r.run_id,'recovery_reason',r.termination_reason) FROM agent_commands c LEFT JOIN agent_runs r ON r.command_id=c.command_id WHERE c.namespace=$1 AND c.client_scope='local' AND (($2<>'' AND c.idempotency_key=$2) OR ($3<>'' AND c.command_id::text=$3))`, env("AGENT_DEPLOYMENT_NAMESPACE", ""), key, id).Scan(&raw)
	if err != nil {
		return nil, err
	}
	var row map[string]any
	err = json.Unmarshal(raw, &row)
	return row, err
}
func commandEnvelope(row map[string]any) map[string]any {
	return map[string]any{"command_id": row["command_id"], "session_id": row["session_id"], "run_id": row["run_id"], "status": row["status"], "status_uri": "/commands/" + fmt.Sprint(row["command_id"]), "response_status": row["response_status"], "response_body": row["response_body"], "recovery_reason": row["recovery_reason"]}
}
func commandReplay(w http.ResponseWriter, row map[string]any) {
	w.Header().Set("Idempotency-Command-Id", fmt.Sprint(row["command_id"]))
	if row["status"] == "succeeded" || row["status"] == "failed" {
		w.Header().Set("Idempotency-Replayed", "true")
		reply(w, int(row["response_status"].(float64)), row["response_body"])
	} else {
		w.Header().Set("Retry-After", "1")
		reply(w, 202, commandEnvelope(row))
	}
}
func (s *server) getCommand(w http.ResponseWriter, r *http.Request) {
	id, e := uuid.Parse(r.PathValue("command_id"))
	if e != nil {
		fail(w, 422, "command_id must be a UUID")
		return
	}
	row, e := s.commandRow(r.Context(), "", id.String())
	if e != nil {
		fail(w, 404, "Command not found")
		return
	}
	reply(w, 200, commandEnvelope(row))
}
func normalizedCommand(route string, raw []byte) (map[string]any, error) {
	v, e := agent.StrictJSON(raw)
	if e != nil {
		return nil, e
	}
	body, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("request must be an object")
	}
	allowed := map[string]bool{}
	switch route {
	case "/sessions":
		allowed["config_ref"], allowed["user_prompt"] = true, true
		if _, ok := body["config_ref"]; !ok {
			body["config_ref"] = "log-analysis.yaml"
		}
	case "/sessions/{session_id}/messages":
		allowed["content"] = true
	case "/sessions/{session_id}/cancel":
		allowed["run_id"] = true
	default:
		allowed["max_steps"] = true
		if _, ok := body["max_steps"]; !ok {
			body["max_steps"] = nil
		}
		if strings.HasSuffix(route, "/resume") {
			allowed["content"] = true
			if _, ok := body["content"]; !ok {
				body["content"] = nil
			}
		}
	}
	for k := range body {
		if !allowed[k] {
			return nil, fmt.Errorf("unknown field %s", k)
		}
	}
	for _, k := range []string{"config_ref", "user_prompt", "content"} {
		if v, ok := body[k]; ok && v != nil {
			if text, ok := v.(string); !ok || strings.TrimSpace(text) == "" {
				return nil, fmt.Errorf("%s must be nonempty text", k)
			}
		}
	}
	if route == "/sessions" && body["config_ref"] == nil {
		return nil, fmt.Errorf("config_ref must be nonempty text")
	}
	if route == "/sessions" && body["user_prompt"] == nil {
		return nil, fmt.Errorf("user_prompt is required")
	}
	if strings.HasSuffix(route, "/messages") && body["content"] == nil {
		return nil, fmt.Errorf("content is required")
	}
	if v := body["max_steps"]; v != nil {
		n, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("invalid max_steps")
		}
		i, e := n.Int64()
		if e != nil || i < 1 {
			return nil, fmt.Errorf("max_steps must be a positive integer")
		}
	}
	if strings.HasSuffix(route, "/cancel") {
		text, ok := body["run_id"].(string)
		if !ok {
			return nil, fmt.Errorf("run_id is required")
		}
		id, e := uuid.Parse(text)
		if e != nil {
			return nil, e
		}
		body["run_id"] = id.String()
	}
	return body, nil
}
func (s *server) guardCommand(ctx context.Context, tx pgx.Tx, c *durableCommand) error {
	c.mu.Lock()
	lost := c.lost
	c.mu.Unlock()
	if lost != nil {
		return ErrLeaseLost
	}
	var valid bool
	e := tx.QueryRow(ctx, `SELECT owner=$2 AND fence=$3 AND lease_expires_at>clock_timestamp() FROM agent_commands WHERE command_id=$1 FOR UPDATE`, c.ID, c.Owner, c.Fence).Scan(&valid)
	if e != nil {
		return e
	}
	if !valid {
		return ErrLeaseLost
	}
	return nil
}
func (s *server) completeCommand(ctx context.Context, tx pgx.Tx, c *durableCommand, status int, body any, outcome string) error {
	if e := s.guardCommand(ctx, tx, c); e != nil {
		return e
	}
	b, e := agent.CanonicalV2(body)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `UPDATE agent_commands SET status=$2,response_status=$3,response_body=$4,completed_at=clock_timestamp(),updated_at=clock_timestamp(),owner=NULL,lease_expires_at=NULL WHERE command_id=$1`, c.ID, outcome, status, b)
	return e
}

type commandWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *commandWriter) WriteHeader(n int) {
	if w.status == 0 {
		w.status = n
		w.ResponseWriter.WriteHeader(n)
	}
}
func (w *commandWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
		w.body.Write(b)
	}
	return w.ResponseWriter.Write(b)
}
func (w *commandWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (s *server) commandHandler(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if len(r.Header.Values("Idempotency-Key")) == 0 {
			reply(w, 422, map[string]any{"code": "idempotency_key_required", "message": "Idempotency-Key is required"})
			return
		}
		valid := len(r.Header.Values("Idempotency-Key")) == 1 && len(key) >= 1 && len(key) <= 255
		for _, c := range key {
			valid = valid && c >= 33 && c <= 126
		}
		if !valid {
			reply(w, 422, map[string]any{"code": "idempotency_key_invalid", "message": "Invalid Idempotency-Key"})
			return
		}
		raw, e := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		if e != nil {
			fail(w, 422, "Invalid body")
			return
		}
		body, e := normalizedCommand(route, raw)
		if e != nil {
			fail(w, 422, e.Error())
			return
		}
		path := r.URL.Path
		sid := ""
		if route != "/sessions" {
			id, e := uuid.Parse(r.PathValue("session_id"))
			if e != nil {
				fail(w, 422, "session_id must be a UUID")
				return
			}
			sid = id.String()
			path = strings.Replace(path, r.PathValue("session_id"), sid, 1)
			r.SetPathValue("session_id", sid)
		}
		fingerprint, e := agent.DigestV2([]any{"agent-command-v1", "POST", path, body})
		if e != nil {
			fail(w, 422, e.Error())
			return
		}
		row, e := s.commandRow(r.Context(), key, "")
		if e != nil && e != pgx.ErrNoRows {
			fail(w, 500, "Command lookup failed")
			return
		}
		admission := map[string]any{"session_id": sid}
		if row == nil {
			if route == "/sessions" {
				cfg, hash, e := s.load(body["config_ref"].(string))
				if e != nil {
					fail(w, 422, "Invalid config_ref: "+e.Error())
					return
				}
				var config any
				json.Unmarshal(cfg, &config)
				admission = map[string]any{"session_id": uuid.NewString(), "config": config, "config_ref": body["config_ref"], "config_sha256": hash}
			} else {
				if _, e := s.fetch(r.Context(), sid); e != nil {
					fail(w, 404, "Session not found")
					return
				}
			}
			request, _ := json.Marshal(map[string]any{"input": body, "admission": admission})
			var session any
			if sid != "" {
				session = sid
			}
			_, e = s.db.Exec(r.Context(), `INSERT INTO agent_commands(command_id,namespace,client_scope,idempotency_key,method,route,request,request_sha256,status,session_id) VALUES($1,$2,'local',$3,'POST',$4,$5,$6,'pending',$7) ON CONFLICT(namespace,client_scope,idempotency_key) DO NOTHING`, uuid.NewString(), env("AGENT_DEPLOYMENT_NAMESPACE", ""), key, path, request, fingerprint, session)
			if e != nil {
				fail(w, 500, "Command admission failed")
				return
			}
			row, e = s.commandRow(r.Context(), key, "")
			if e != nil {
				fail(w, 500, "Command lookup failed")
				return
			}
		}
		if row["request_sha256"] != fingerprint {
			reply(w, 409, map[string]any{"code": "idempotency_key_reused", "message": "The key identifies different content or route"})
			return
		}
		if (row["status"] != "pending" && row["status"] != "started") || row["run_id"] != nil {
			commandReplay(w, row)
			return
		}
		c := &durableCommand{ID: row["command_id"].(string), Owner: uuid.NewString(), Route: route, Namespace: env("AGENT_DEPLOYMENT_NAMESPACE", "")}
		request := row["request"].(map[string]any)
		c.Admission, _ = request["admission"].(map[string]any)
		e = s.db.QueryRow(r.Context(), `UPDATE agent_commands SET owner=$2,fence=fence+1,status='started',lease_expires_at=clock_timestamp()+interval '30 seconds',updated_at=clock_timestamp() WHERE command_id=$1 AND status IN ('pending','started') AND (owner IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING fence`, c.ID, c.Owner).Scan(&c.Fence)
		if e != nil {
			row, _ = s.commandRow(r.Context(), key, "")
			commandReplay(w, row)
			return
		}
		ctx := context.WithValue(r.Context(), commandContextKey{}, c)
		done := make(chan struct{})
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					renewCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					tag, e := s.db.Exec(renewCtx, `UPDATE agent_commands SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE command_id=$1 AND owner=$2 AND fence=$3 AND lease_expires_at>clock_timestamp()`, c.ID, c.Owner, c.Fence)
					cancel()
					if e != nil || tag.RowsAffected() != 1 {
						c.mu.Lock()
						c.lost = ErrLeaseLost
						c.mu.Unlock()
						return
					}
				}
			}
		}()
		defer func() {
			close(done)
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = s.db.Exec(cleanup, `UPDATE agent_commands SET owner=NULL,lease_expires_at=NULL WHERE command_id=$1 AND owner=$2 AND fence=$3`, c.ID, c.Owner, c.Fence)
		}()
		w.Header().Set("Idempotency-Command-Id", c.ID)
		encoded, _ := json.Marshal(body)
		r = r.WithContext(ctx)
		r.Body = io.NopCloser(bytes.NewReader(encoded))
		out := &commandWriter{ResponseWriter: w}
		next(out, r)
		c.mu.Lock()
		completed := c.completed
		c.mu.Unlock()
		if completed {
			return
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		var result any
		if out.status >= 200 && out.status < 500 && json.Unmarshal(out.body.Bytes(), &result) == nil {
			tx, e := s.db.Begin(finishCtx)
			if e == nil {
				defer tx.Rollback(finishCtx)
				outcome := "succeeded"
				if out.status >= 400 {
					outcome = "failed"
				}
				if e = s.completeCommand(finishCtx, tx, c, out.status, result, outcome); e == nil {
					_ = tx.Commit(finishCtx)
				}
			}
		} else {
			_, _ = s.db.Exec(finishCtx, `UPDATE agent_commands SET status='uncertain',updated_at=clock_timestamp() WHERE command_id=$1 AND owner=$2 AND fence=$3 AND status='started'`, c.ID, c.Owner, c.Fence)
		}
	}
}

func (s *server) administrativeCommand(ctx context.Context, sid, route string, input map[string]any) (context.Context, func(), error) {
	c := &durableCommand{ID: uuid.NewString(), Owner: uuid.NewString(), Namespace: env("AGENT_DEPLOYMENT_NAMESPACE", ""), Route: route, Fence: 1}
	fingerprint, e := agent.DigestV2([]any{"agent-command-v1", "POST", route, input})
	if e != nil {
		return nil, nil, e
	}
	request, _ := json.Marshal(map[string]any{"input": input, "admission": map[string]any{"session_id": sid}})
	_, e = s.db.Exec(ctx, `INSERT INTO agent_commands(command_id,namespace,client_scope,idempotency_key,method,route,request,request_sha256,status,session_id,owner,fence,lease_expires_at) VALUES($1,$2,'local',$3,'POST',$4,$5,$6,'started',$7,$8,1,clock_timestamp()+interval '30 seconds')`, c.ID, c.Namespace, uuid.NewString(), route, request, fingerprint, sid, c.Owner)
	if e != nil {
		return nil, nil, e
	}
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = s.db.Exec(cleanupCtx, `UPDATE agent_commands SET status=CASE WHEN status='started' THEN 'uncertain' ELSE status END,owner=NULL,lease_expires_at=NULL WHERE command_id=$1 AND owner=$2 AND fence=$3`, c.ID, c.Owner, c.Fence)
	}
	return context.WithValue(ctx, commandContextKey{}, c), cleanup, nil
}
