package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"time"
)

// eventStream replays committed events only; reconnecting cannot start work.
func (s *server) eventStream(w http.ResponseWriter, r *http.Request) {
	sid, e := uuid.Parse(r.PathValue("session_id"))
	if e != nil {
		fail(w, 422, "session_id must be a UUID")
		return
	}
	run, e := uuid.Parse(r.URL.Query().Get("run_id"))
	if e != nil {
		fail(w, 422, "run_id must be a UUID")
		return
	}
	after, e := pageInt(r, "after_event_id", 0, 0, 1<<62)
	if e != nil {
		fail(w, 422, e.Error())
		return
	}
	var status string
	if e = s.db.QueryRow(r.Context(), `SELECT status FROM agent_runs WHERE session_id=$1 AND run_id=$2`, sid.String(), run.String()).Scan(&status); e != nil {
		fail(w, 404, "Run not found")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flush, ok := w.(http.Flusher)
	if !ok {
		return
	}
	for r.Context().Err() == nil {
		rows, e := s.db.Query(r.Context(), `SELECT event_id,event FROM agent_session_events WHERE session_id=$1 AND event_id>$2 AND event->>'run_id'=$3 ORDER BY event_id LIMIT 100`, sid.String(), after, run.String())
		if e != nil {
			return
		}
		count := 0
		for rows.Next() {
			var id int64
			var raw []byte
			if e = rows.Scan(&id, &raw); e != nil {
				rows.Close()
				return
			}
			var event map[string]any
			if e = json.Unmarshal(raw, &event); e != nil {
				rows.Close()
				return
			}
			event["event_id"] = id
			encoded, _ := json.Marshal(event)
			if _, e = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", id, encoded); e != nil {
				rows.Close()
				return
			}
			after = id
			count++
			flush.Flush()
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return
		}
		if count == 0 {
			if e = s.db.QueryRow(r.Context(), `SELECT status FROM agent_runs WHERE session_id=$1 AND run_id=$2`, sid.String(), run.String()).Scan(&status); e != nil || status != "running" {
				return
			}
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-r.Context().Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}
