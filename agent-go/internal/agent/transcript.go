package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Message is the public persisted representation. Model payloads use WirePayload.
type Message struct {
	MessageID  string    `json:"message_id"`
	Sequence   *int64    `json:"sequence"`
	CreatedAt  time.Time `json:"created_at"`
	Role       string    `json:"role"`
	Content    *string   `json:"content"`
	Name       *string   `json:"name"`
	ToolCallID *string   `json:"tool_call_id"`
	ToolCalls  any       `json:"tool_calls"`
}
type WireMessage struct {
	Role       string  `json:"role"`
	Content    *string `json:"content,omitempty"`
	Name       *string `json:"name,omitempty"`
	ToolCallID *string `json:"tool_call_id,omitempty"`
	ToolCalls  any     `json:"tool_calls,omitempty"`
}

func (m Message) WirePayload() WireMessage {
	return WireMessage{m.Role, m.Content, m.Name, m.ToolCallID, m.ToolCalls}
}
func wireMessages(messages []Message) []WireMessage {
	result := make([]WireMessage, len(messages))
	for i, m := range messages {
		result[i] = m.WirePayload()
	}
	return result
}

type TranscriptEntry struct {
	MessageID    string         `json:"message_id"`
	Sequence     int64          `json:"sequence"`
	Kind         string         `json:"kind"`
	Payload      map[string]any `json:"payload"`
	RunID        *string        `json:"run_id"`
	DecisionStep int            `json:"decision_step"`
	CreatedAt    time.Time      `json:"created_at"`
}
type DiagnosticRecord struct {
	Family  string `json:"family"`
	Ordinal int64  `json:"ordinal"`
	Payload any    `json:"payload"`
}
type ActiveContext struct {
	Revision       int64    `json:"revision"`
	GoalMessageID  string   `json:"goal_message_id"`
	RawMessageIDs  []string `json:"raw_message_ids"`
	LedgerRecordID *string  `json:"ledger_record_id"`
	LedgerState    *Ledger  `json:"ledger_state"`
}

// Clone preserves concrete numeric types while recursively detaching mutable data.
func Clone[T any](value T) T { return cloneReflect(reflect.ValueOf(value)).Interface().(T) }
func cloneReflect(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		x := reflect.New(v.Type()).Elem()
		x.Set(cloneReflect(v.Elem()))
		return x
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		x := reflect.New(v.Type().Elem())
		x.Elem().Set(cloneReflect(v.Elem()))
		return x
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		x := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			x.SetMapIndex(iter.Key(), cloneReflect(iter.Value()))
		}
		return x
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		x := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			x.Index(i).Set(cloneReflect(v.Index(i)))
		}
		return x
	case reflect.Struct:
		x := reflect.New(v.Type()).Elem()
		x.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if x.Field(i).CanSet() && v.Type().Field(i).IsExported() {
				x.Field(i).Set(cloneReflect(v.Field(i)))
			}
		}
		return x
	default:
		return v
	}
}
func ConfigIdentity(config Config) (string, error) {
	b, err := canonicalConfigJSON(config)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}
func NewSession(id, goal, ref, hash string) Session {
	s := Session{ID: id, Goal: goal, ConfigRef: ref, ConfigSHA256: hash, Status: "active", HistoryComplete: true, DiagnosticHistoryComplete: true, CompactionSavingsKnown: true}
	NormalizeSession(&s)
	AppendMessage(&s, Message{Role: "user", Content: &goal})
	return s
}
func NormalizeSession(s *Session) {
	if s.NextTranscriptSequence == 0 {
		s.NextTranscriptSequence = 1
		s.HistoryComplete = true
		s.DiagnosticHistoryComplete = true
		s.CompactionSavingsKnown = true
	}
	if s.Status == "" {
		s.Status = "active"
	}
	if s.RunStatus == "" {
		s.RunStatus = "idle"
	}
	if s.Memory == nil {
		s.Memory = []Message{}
	}
	if s.PendingToolCalls == nil {
		s.PendingToolCalls = []map[string]any{}
	}
	if s.CompactionEvents == nil {
		s.CompactionEvents = []string{}
	}
	if s.CompactionAttempts == nil {
		s.CompactionAttempts = []map[string]any{}
	}
	if s.CompactionDecisions == nil {
		s.CompactionDecisions = []map[string]any{}
	}
	if s.CompactionTriggerCounts == nil {
		s.CompactionTriggerCounts = map[string]int{}
	}
	if s.CompactionSkipCounts == nil {
		s.CompactionSkipCounts = map[string]int{}
	}
	if s.DiagnosticCounts == nil {
		s.DiagnosticCounts = map[string]int64{}
	}
	if s.CompletionArtifacts == nil {
		s.CompletionArtifacts = map[string]CompletionArtifact{}
	}
	if s.ProgressSeenOutputs == nil {
		s.ProgressSeenOutputs = []string{}
	}
	if s.Executions == nil {
		s.Executions = []ToolExecution{}
	}
	if s.RejectedFinals == nil {
		s.RejectedFinals = []RejectedFinal{}
	}
	if s.CompactionProvenance == nil {
		s.CompactionProvenance = []CompactionProvenance{}
	}
	if s.Workflow != nil {
		if s.Workflow.CompletedPhases == nil {
			s.Workflow.CompletedPhases = []string{}
		}
		if s.Workflow.ObservedEvidence == nil {
			s.Workflow.ObservedEvidence = []string{}
		}
		if s.Workflow.Transitions == nil {
			s.Workflow.Transitions = []Transition{}
		}
	}
	s.DiagnosticsComplete = true
	for _, f := range []string{"compaction_events", "compaction_attempts", "compaction_decisions"} {
		n := len(s.CompactionEvents)
		if f == "compaction_attempts" {
			n = len(s.CompactionAttempts)
		}
		if f == "compaction_decisions" {
			n = len(s.CompactionDecisions)
		}
		if s.DiagnosticCounts[f] > int64(n) {
			s.DiagnosticsComplete = false
		}
	}
}
func (s Session) MarshalJSON() ([]byte, error) {
	s.Workflow = Clone(s.Workflow)
	NormalizeSession(&s)
	type public Session
	b, err := json.Marshal(public(s))
	if err != nil {
		return nil, err
	}
	var values map[string]any
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if err = decoder.Decode(&values); err != nil {
		return nil, err
	}
	values["ledger_state"] = s.CompactionLedger
	values["transcript_pending"] = []TranscriptEntry{}
	values["pending_commit_id"] = nil
	for _, key := range []string{"resolved_config_sha256", "config_ref", "config_sha256", "progress_fingerprint", "last_compaction_attempt_sha256"} {
		if values[key] == "" {
			values[key] = nil
		}
	}
	if s.LastCompactionAttemptStep == 0 && s.LastCompactionAttemptSHA256 == "" {
		values["last_compaction_attempt_step"] = nil
	}
	if s.LastCompactionEstimatedTokens == 0 {
		values["last_compaction_estimated_tokens"] = nil
	}
	return json.Marshal(values)
}
func (s *Session) UnmarshalJSON(data []byte) error {
	type public Session
	value := public{HistoryComplete: true, DiagnosticHistoryComplete: true, CompactionSavingsKnown: true}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	*s = Session(value)
	if s.CompactionLedger == nil {
		var alias struct {
			Ledger *Ledger `json:"ledger_state"`
		}
		if err := json.Unmarshal(data, &alias); err != nil {
			return err
		}
		s.CompactionLedger = alias.Ledger
	}
	NormalizeSession(s)
	return nil
}

func StageMessages(s *Session) {
	NormalizeSession(s)
	for i := range s.Memory {
		m := &s.Memory[i]
		if m.Sequence != nil {
			continue
		}
		if m.MessageID == "" {
			m.MessageID = uuid.NewString()
		}
		if m.CreatedAt.IsZero() {
			m.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
		}
		seq := s.NextTranscriptSequence
		m.Sequence = &seq
		s.NextTranscriptSequence++
		b, _ := json.Marshal(m.WirePayload())
		var payload map[string]any
		_ = json.Unmarshal(b, &payload)
		s.TranscriptPending = append(s.TranscriptPending, TranscriptEntry{MessageID: m.MessageID, Sequence: seq, Kind: "message", Payload: payload, RunID: Clone(s.RunID), DecisionStep: s.StepCount, CreatedAt: m.CreatedAt})
	}
}
func AppendMessage(s *Session, m Message) {
	StageMessages(s)
	s.Memory = append(s.Memory, Clone(m))
	StageMessages(s)
}
func ArchiveRecord(s *Session, kind string, payload map[string]any) TranscriptEntry {
	StageMessages(s)
	entry := TranscriptEntry{MessageID: uuid.NewString(), Sequence: s.NextTranscriptSequence, Kind: kind, Payload: Clone(payload), RunID: Clone(s.RunID), DecisionStep: s.StepCount, CreatedAt: time.Now().UTC().Truncate(time.Microsecond)}
	s.NextTranscriptSequence++
	s.TranscriptPending = append(s.TranscriptPending, entry)
	return Clone(entry)
}
func (s *Session) ContextProjection() ActiveContext {
	ids := make([]string, 0, len(s.Memory))
	for i := 1; i < len(s.Memory); i++ {
		ids = append(ids, s.Memory[i].MessageID)
	}
	goal := ""
	if len(s.Memory) > 0 {
		goal = s.Memory[0].MessageID
	}
	return ActiveContext{s.Revision, goal, ids, Clone(s.ActiveLedgerID), Clone(s.CompactionLedger)}
}
func (s Session) HotSnapshot() (map[string]any, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	err = decoder.Decode(&result)
	for _, key := range []string{"transcript_pending", "pending_commit_id", "memory", "compacted_context", "compaction_ledger", "ledger_state", "compaction_events", "compaction_attempts", "compaction_decisions", "diagnostics_complete"} {
		delete(result, key)
	}
	return result, err
}
func RecordDiagnostic(s *Session, family string, payload any) {
	NormalizeSession(s)
	switch family {
	case "compaction_events", "compaction_attempts", "compaction_decisions":
	default:
		panic("unknown diagnostic family: " + family)
	}
	s.DiagnosticCounts[family]++
	s.DiagnosticsPending = append(s.DiagnosticsPending, DiagnosticRecord{family, s.DiagnosticCounts[family], Clone(payload)})
	switch family {
	case "compaction_events":
		s.CompactionEvents = append(s.CompactionEvents, payload.(string))
		if len(s.CompactionEvents) > 32 {
			s.CompactionEvents = s.CompactionEvents[len(s.CompactionEvents)-32:]
		}
	case "compaction_attempts":
		value := Clone(payload.(map[string]any))
		s.CompactionAttempts = append(s.CompactionAttempts, value)
		if len(s.CompactionAttempts) > 32 {
			s.CompactionAttempts = s.CompactionAttempts[len(s.CompactionAttempts)-32:]
		}
		if value["accepted"] != true {
			s.DiagnosticRejections++
		} else if n, ok := value["reduction_tokens"].(int); ok {
			s.CompactionSavingsTotal += int64(n)
		} else {
			s.CompactionSavingsKnown = false
		}
	case "compaction_decisions":
		s.CompactionDecisions = append(s.CompactionDecisions, Clone(payload.(map[string]any)))
		if len(s.CompactionDecisions) > 32 {
			s.CompactionDecisions = s.CompactionDecisions[len(s.CompactionDecisions)-32:]
		}
	}
	NormalizeSession(s)
}
func StageDiagnostics(s *Session) {
	if len(s.DiagnosticCounts) > 0 {
		return
	}
	events, attempts, decisions := s.CompactionEvents, s.CompactionAttempts, s.CompactionDecisions
	s.CompactionEvents = nil
	s.CompactionAttempts = nil
	s.CompactionDecisions = nil
	for _, v := range events {
		RecordDiagnostic(s, "compaction_events", v)
	}
	for _, v := range attempts {
		RecordDiagnostic(s, "compaction_attempts", v)
	}
	for _, v := range decisions {
		RecordDiagnostic(s, "compaction_decisions", v)
	}
}
func canonicalSummary(s *Session) string {
	if s.CompactedContext != nil {
		return *s.CompactedContext
	}
	return ""
}
func recordSummaryAttempt(s *Session, entry map[string]any, proposal string, source, retained []Message, canonical string, accepted bool) {
	RecordDiagnostic(s, "compaction_attempts", entry)
	ids := func(ms []Message) []string {
		out := []string{}
		for _, m := range ms {
			out = append(out, m.MessageID)
		}
		return out
	}
	payload := Clone(entry)
	payload["source_message_ids"] = ids(source)
	payload["retained_message_ids"] = ids(retained)
	payload["previous_ledger_id"] = Clone(s.ActiveLedgerID)
	payload["proposal"] = proposal
	kind := "compaction_rejected"
	if accepted {
		kind = "compaction_summary"
		payload["content"] = canonical
		payload["ledger"] = Clone(s.CompactionLedger)
		payload["efficiency"] = Clone(entry)
	}
	record := ArchiveRecord(s, kind, payload)
	if accepted {
		s.ActiveLedgerID = &record.MessageID
	}
}

// SourceFingerprint uses the same compact wire source as the compactor.
func SourceFingerprint(goal string, messages []Message, previous *string) (string, error) {
	source, err := sourceText(goal, messages, previous)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(source))), nil
}

func recordDecision(s *Session, d Decision) {
	kind := "tool"
	var code any
	calls := d.Calls
	if len(calls) == 0 && d.Name != "" {
		calls = []ToolCall{{ID: d.ID, Name: d.Name, Arguments: d.Arguments}}
	}
	parsed := map[string]any{}
	if d.Final {
		kind = "final"
		parsed["content"] = d.Content
	} else if d.Invalid != "" {
		kind = "recoverable_error"
		value, _, _ := strings.Cut(d.Invalid, ":")
		code = value
		parsed["code"] = value
		parsed["content"] = d.Invalid
	} else if len(d.Calls) > 0 {
		kind = "tools"
	}
	observed := []map[string]any{}
	for _, call := range calls {
		raw := call.RawArguments
		if raw == "" {
			b, _ := json.Marshal(call.Arguments)
			raw = string(b)
		}
		observed = append(observed, map[string]any{"id": call.ID, "name": call.Name, "raw_arguments": raw})
	}
	parsed["type"] = kind
	parsed["calls"] = observed
	encoded, _ := json.Marshal(parsed)
	var content any
	if d.Content != "" {
		content = d.Content
	}
	ArchiveRecord(s, "model_decision", map[string]any{"parsed_decision_json": string(encoded), "type": kind, "code": code, "content": content, "calls": observed})
}
