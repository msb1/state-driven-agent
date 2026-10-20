package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

type du09WorkloadCell struct {
	name                                string
	clients, archivedPairs, outputBytes int
}

func TestDU09B01PostgresQualificationGrid(t *testing.T) {
	s, ctx := du05Repository(t)
	cells := []du09WorkloadCell{
		{"baseline", 1, 0, 1024}, {"concurrency-4", 4, 0, 1024}, {"concurrency-8", 8, 0, 1024},
		{"history-100", 1, 100, 1024}, {"history-1000", 1, 1000, 1024}, {"output-8192", 1, 0, 8192}, {"interaction", 4, 100, 8192},
	}
	all := map[string][]map[string]any{}
	var hotLow, hotHigh, selected100, selected1000 int
	for _, cell := range cells {
		for repetition := 1; repetition <= 3; repetition++ {
			samples := du09RunBatch(t, s, ctx, cell)
			if len(samples) != cell.clients {
				t.Fatalf("cell %s repetition %d got %d samples, want %d", cell.name, repetition, len(samples), cell.clients)
			}
			for _, sample := range samples {
				if sample["status"] != "complete" || intValue(sample["revision_advances"]) != 2 || sample["payload_match"] != true {
					t.Fatalf("cell %s failed sample: %v", cell.name, sample)
				}
			}
			all[cell.name] = append(all[cell.name], samples...)
			readBytes, hotBytes, elapsed := []int{}, []int{}, []float64{}
			for _, sample := range samples {
				readBytes = append(readBytes, sample["selected_read_json_bytes"].(int))
				hotBytes = append(hotBytes, sample["snapshot_json_bytes"].(int))
				elapsed = append(elapsed, sample["elapsed_ms"].(float64))
			}
			sort.Float64s(elapsed)
			median, p95, maximum := du09Median(elapsed), du09NearestRank(elapsed, .95), elapsed[len(elapsed)-1]
			encoded, _ := json.Marshal(map[string]any{"cell": cell.name, "repetition": repetition, "clients": cell.clients, "archived_pairs": cell.archivedPairs, "output_bytes": cell.outputBytes, "n": len(samples), "median_elapsed_ms": median, "p95_observed_elapsed_ms": p95, "max_elapsed_ms": maximum, "samples": samples})
			t.Logf("DU09_B01_CELL %s", encoded)
			if cell.outputBytes == 1024 && (cell.archivedPairs == 0 || cell.archivedPairs == 100 || cell.archivedPairs == 1000) {
				if hotLow == 0 || minInt(hotBytes) < hotLow {
					hotLow = minInt(hotBytes)
				}
				if maxInt(hotBytes) > hotHigh {
					hotHigh = maxInt(hotBytes)
				}
				if cell.archivedPairs == 100 && maxInt(readBytes) > selected100 {
					selected100 = maxInt(readBytes)
				} else if cell.archivedPairs == 1000 && maxInt(readBytes) > selected1000 {
					selected1000 = maxInt(readBytes)
				}
			}
		}
	}
	for _, cell := range cells {
		values := []float64{}
		for _, sample := range all[cell.name] {
			values = append(values, sample["elapsed_ms"].(float64))
		}
		sort.Float64s(values)
		encoded, _ := json.Marshal(map[string]any{"cell": cell.name, "n": len(values), "median_elapsed_ms": du09Median(values), "p95_observed_elapsed_ms": du09NearestRank(values, .95), "max_elapsed_ms": values[len(values)-1], "samples": all[cell.name]})
		t.Logf("DU09_B01_TOTAL %s", encoded)
	}
	if hotHigh-hotLow >= 8192 {
		t.Fatalf("hot snapshot size varied by %d bytes across archive growth; want <8192", hotHigh-hotLow)
	}
	selectedReadDelta := selected1000 - selected100
	if selectedReadDelta < 0 {
		selectedReadDelta = -selectedReadDelta
	}
	if selectedReadDelta >= 8192 {
		t.Fatalf("selected read size varied by %d bytes between 100 and 1000 history; want <8192", selectedReadDelta)
	}
	if len(all) != 7 {
		t.Fatalf("ran %d cells, want 7", len(all))
	}
}

func du09RunBatch(t *testing.T, s *server, ctx context.Context, cell du09WorkloadCell) []map[string]any {
	t.Helper()
	states := make([]persisted, cell.clients)
	for i := range states {
		states[i] = du05Create(t, s, ctx)
		state := &states[i].State
		for pair := 0; pair < cell.archivedPairs; pair++ {
			callID := fmt.Sprintf("du09-%s-%d-%d", cell.name, i, pair)
			agent.ArchiveRecord(state, "tool_intent", map[string]any{"call_id": callID, "tool": "synthetic_lookup"})
			agent.ArchiveRecord(state, "tool_outcome", map[string]any{"call_id": callID, "output": strings.Repeat("x", cell.outputBytes)})
			agent.RecordDiagnostic(state, "compaction_decisions", map[string]any{"historical_pair": pair})
		}
		callID := fmt.Sprintf("du09-current-%s-%d", cell.name, i)
		runID := uuid.NewString()
		runStarted := time.Now().UTC()
		runEnded := runStarted.Add(time.Millisecond)
		state.RunID, state.RunStatus, state.RunCount = &runID, "complete", 1
		state.RunStartedAt, state.RunEndedAt = &runStarted, &runEnded
		toolCalls := []any{map[string]any{"id": callID, "type": "function", "function": map[string]any{"name": "synthetic_lookup", "arguments": "{}"}}}
		agent.AppendMessage(state, agent.Message{Role: "assistant", ToolCalls: toolCalls})
		result := strings.Repeat("y", cell.outputBytes)
		agent.AppendMessage(state, agent.Message{Role: "tool", Name: stringPointer("synthetic_lookup"), ToolCallID: stringPointer(callID), Content: stringPointer(result)})
		if err := s.save(ctx, &states[i], map[string]any{"type": "du09_workload_seed"}); err != nil {
			t.Fatalf("seed workload cell %s: %v", cell.name, err)
		}
	}
	if cell.outputBytes == 1024 && (cell.archivedPairs == 0 || cell.archivedPairs == 100 || cell.archivedPairs == 1000) {
		du09LogPlans(t, s, ctx, states[0].State.ID, cell)
	}
	start := make(chan struct{})
	results := make([]map[string]any, cell.clients)
	s.ioMu.Lock()
	ioStart := len(s.ioSamples)
	s.ioMu.Unlock()
	var group sync.WaitGroup
	for i := range states {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			began := time.Now()
			owner := uuid.NewString()
			fence, err := s.acquire(ctx, states[index].State.ID, owner, 30)
			if err != nil {
				results[index] = map[string]any{"status": "failed", "error": err.Error()}
				return
			}
			defer s.release(states[index].State.ID, owner, fence)
			initialRevision := states[index].State.Revision
			finalRevision := initialRevision
			for cycle := 0; cycle < 2; cycle++ {
				p, getErr := s.fetch(ctx, states[index].State.ID)
				if getErr != nil {
					results[index] = map[string]any{"status": "failed", "error": getErr.Error()}
					return
				}
				p.State.ExecutionOwner, p.State.ExecutionFence = &owner, &fence
				if saveErr := s.save(ctx, &p, map[string]any{"type": "du09_workload_touch", "cycle": cycle}); saveErr != nil {
					results[index] = map[string]any{"status": "failed", "error": saveErr.Error()}
					return
				}
				p, getErr = s.fetch(ctx, states[index].State.ID)
				if getErr != nil {
					results[index] = map[string]any{"status": "failed", "error": getErr.Error()}
					return
				}
				if len(p.State.Memory) != len(states[index].State.Memory) || !equalJSON(p.State.Memory, states[index].State.Memory) {
					results[index] = map[string]any{"status": "failed", "error": "active payload changed after hydration"}
					return
				}
				finalRevision = p.State.Revision
			}
			results[index] = map[string]any{"status": "complete", "revision_advances": finalRevision - initialRevision, "fence": fence, "revision": finalRevision, "elapsed_ms": float64(time.Since(began).Microseconds()) / 1000, "payload_match": true}
		}(i)
	}
	close(start)
	group.Wait()
	s.ioMu.Lock()
	ioSamples := agent.Clone(s.ioSamples[ioStart:])
	s.ioMu.Unlock()
	for i, result := range results {
		sessionID := states[i].State.ID
		selectedBytes, snapshotBytes := 0, 0
		clientSamples := []map[string]any{}
		for _, sample := range ioSamples {
			if sample["session_id"] != sessionID {
				continue
			}
			clientSamples = append(clientSamples, sample)
			if sample["operation"] == "get" {
				if n, ok := sample["selected_read_json_bytes"].(int); ok && n > selectedBytes {
					selectedBytes = n
				}
				if n, ok := sample["snapshot_json_bytes"].(int); ok && n > snapshotBytes {
					snapshotBytes = n
				}
			}
		}
		if result == nil {
			results[i] = map[string]any{"status": "failed", "error": "worker returned no result"}
			continue
		}
		result["selected_read_json_bytes"], result["snapshot_json_bytes"], result["io_samples"] = selectedBytes, snapshotBytes, clientSamples
		if result["status"] == "complete" && (selectedBytes == 0 || snapshotBytes == 0 || len(clientSamples) == 0) {
			result["status"] = "failed"
			result["error"] = "missing storage instrumentation sample"
		}
	}
	return results
}

func du09LogPlans(t *testing.T, s *server, ctx context.Context, sessionID string, cell du09WorkloadCell) {
	t.Helper()
	callID := fmt.Sprintf("du09-current-%s-0", cell.name)
	queries := []struct {
		name, sql string
		args      []any
	}{
		{"transcript", `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT sequence FROM agent_session_transcript WHERE session_id=$1 ORDER BY sequence LIMIT 32`, []any{sessionID}},
		{"diagnostics", `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT ordinal FROM agent_session_diagnostics WHERE session_id=$1 AND family='compaction_decisions' ORDER BY ordinal LIMIT 32`, []any{sessionID}},
		{"call_map", `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT message_id FROM agent_session_calls WHERE session_id=$1 AND call_id=$2`, []any{sessionID, callID}},
	}
	for _, query := range queries {
		var plan []byte
		if err := s.db.QueryRow(ctx, query.sql, query.args...).Scan(&plan); err != nil {
			t.Fatalf("query plan %s: %v", query.name, err)
		}
		t.Logf("DU09_B01_PLAN cell=%s table=%s explain=%s", cell.name, query.name, plan)
	}
}

func du09Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	if len(copyValues)%2 == 1 {
		return copyValues[len(copyValues)/2]
	}
	return (copyValues[len(copyValues)/2-1] + copyValues[len(copyValues)/2]) / 2
}

func du09NearestRank(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values))*quantile+.999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func minInt(values []int) int {
	if len(values) == 0 {
		return 0
	}
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}

func maxInt(values []int) int {
	if len(values) == 0 {
		return 0
	}
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}
