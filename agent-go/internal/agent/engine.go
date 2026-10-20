// Package agent contains the language-independent state-machine semantics.
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"strconv"
	"strings"
	"time"
)

type Transition struct {
	FromPhase string `json:"from_phase"`
	ToPhase   string `json:"to_phase"`
	Reason    string `json:"reason"`
}
type WorkflowState struct {
	ActivePhase      *string      `json:"active_phase"`
	CompletedPhases  []string     `json:"completed_phases"`
	ObservedEvidence []string     `json:"observed_evidence"`
	Transitions      []Transition `json:"transitions"`
}
type ToolExecution struct {
	ExecutionID    string         `json:"execution_id,omitempty"`
	RunID          string         `json:"run_id,omitempty"`
	CallID         string         `json:"call_id,omitempty"`
	Tool           string         `json:"tool"`
	Arguments      map[string]any `json:"arguments,omitempty"`
	Policy         map[string]any `json:"policy,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Attempts       int            `json:"attempts,omitempty"`
	OwnerID        string         `json:"owner_id,omitempty"`
	Fence          int64          `json:"fence,omitempty"`
	Status         string         `json:"status"`
	Result         string         `json:"result,omitempty"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
}
type CompletionArtifact struct {
	Tool       string         `json:"tool"`
	Arguments  map[string]any `json:"arguments"`
	ToolCallID string         `json:"tool_call_id"`
	Step       int            `json:"step"`
	Value      any            `json:"value"`
}
type RejectedFinal struct {
	Content string   `json:"content"`
	Errors  []string `json:"errors"`
}
type Session struct {
	ResolvedConfigSHA256      string             `json:"resolved_config_sha256"`
	Revision                  int64              `json:"revision"`
	NextTranscriptSequence    int64              `json:"next_transcript_sequence"`
	ActiveLedgerID            *string            `json:"active_ledger_id"`
	HistoryComplete           bool               `json:"history_complete"`
	RunID                     *string            `json:"run_id"`
	RunStartedAt              *time.Time         `json:"run_started_at"`
	RunEndedAt                *time.Time         `json:"run_ended_at"`
	RunTerminationReason      *string            `json:"run_termination_reason"`
	PendingToolCalls          []map[string]any   `json:"pending_tool_calls"`
	LastError                 map[string]any     `json:"last_error"`
	DiagnosticCounts          map[string]int64   `json:"diagnostic_counts"`
	DiagnosticsComplete       bool               `json:"diagnostics_complete"`
	DiagnosticHistoryComplete bool               `json:"diagnostic_history_complete"`
	DiagnosticRejections      int64              `json:"diagnostic_rejections"`
	CompactionSavingsTotal    int64              `json:"compaction_savings_total"`
	CompactionSavingsKnown    bool               `json:"compaction_savings_known"`
	LastArchiveID             *string            `json:"last_archive_id"`
	TranscriptPending         []TranscriptEntry  `json:"-"`
	DiagnosticsPending        []DiagnosticRecord `json:"-"`
	ExecutionPending          []map[string]any   `json:"-"`
	RetentionPending          map[string]any     `json:"-"`
	PendingCommitID           *string            `json:"-"`
	ExecutionOwner            *string            `json:"-"`
	ExecutionFence            *int64             `json:"-"`
	ExecutionMaintenance      bool               `json:"-"`

	ID                            string                        `json:"id"`
	Goal                          string                        `json:"goal"`
	ConfigRef                     string                        `json:"config_ref"`
	ConfigSHA256                  string                        `json:"config_sha256"`
	Memory                        []Message                     `json:"memory"`
	CompactedContext              *string                       `json:"compacted_context"`
	CompactionEvents              []string                      `json:"compaction_events"`
	LastCompactionStep            int                           `json:"last_compaction_step"`
	LastCompactionEstimatedTokens int                           `json:"last_compaction_estimated_tokens"`
	CompactionLedger              *Ledger                       `json:"compaction_ledger"`
	CompactionProvenance          []CompactionProvenance        `json:"compaction_provenance"`
	CompactionAttempts            []map[string]any              `json:"compaction_attempts"`
	CompactionDecisions           []map[string]any              `json:"compaction_decisions"`
	CompactionTriggerCounts       map[string]int                `json:"compaction_trigger_counts"`
	CompactionSkipCounts          map[string]int                `json:"compaction_skip_counts"`
	LastCompactionAttemptStep     int                           `json:"last_compaction_attempt_step"`
	LastCompactionAttemptSHA256   string                        `json:"last_compaction_attempt_sha256"`
	RequestBudget                 *RequestBudget                `json:"request_budget"`
	Workflow                      *WorkflowState                `json:"workflow"`
	StepCount                     int                           `json:"step_count"`
	RunCount                      int                           `json:"run_count"`
	RunStatus                     string                        `json:"run_status"`
	ConsecutiveInvalidResponses   int                           `json:"consecutive_invalid_responses"`
	Executions                    []ToolExecution               `json:"-"`
	CompletionArtifacts           map[string]CompletionArtifact `json:"completion_artifacts"`
	RejectedFinals                []RejectedFinal               `json:"rejected_finals"`
	NoProgressSteps               int                           `json:"no_progress_steps"`
	ProgressFingerprint           string                        `json:"progress_fingerprint"`
	ProgressSeenOutputs           []string                      `json:"progress_seen_outputs"`
	Status                        string                        `json:"status"`
	FinalAnswer                   *string                       `json:"final_answer"`
}
type Decision struct {
	Final             bool
	Content, ID, Name string
	Arguments         map[string]any
	Calls             []ToolCall
	Invalid           string
}
type ToolCall struct {
	ID, Name, RawArguments string
	Arguments              map[string]any
	Invalid                string
}
type Client interface {
	Decide(context.Context, []Message) (Decision, error)
	Compact(context.Context, string, []Message, *string, bool) (string, error)
}
type Tool func(context.Context, map[string]any) (string, error)
type Engine struct {
	Config Config
	Client Client
	Tools  map[string]Tool
}
type Emit func(map[string]any) error

var ErrRunCancelled = errors.New("run cancellation requested")

type executionCheckpointKey struct{}

// WithExecutionCheckpoint lets a service attach ownership/cancellation checks
// to scheduler boundaries without coupling the state machine to a database.
func WithExecutionCheckpoint(ctx context.Context, checkpoint func() error) context.Context {
	return context.WithValue(ctx, executionCheckpointKey{}, checkpoint)
}

func checkExecutionCheckpoint(ctx context.Context) error {
	if checkpoint, ok := ctx.Value(executionCheckpointKey{}).(func() error); ok {
		return checkpoint()
	}
	return nil
}

func (e Engine) Create(id, goal, ref, hash string) Session {
	s := NewSession(id, goal, ref, hash)
	s.ResolvedConfigSHA256, _ = ConfigIdentity(e.Config)
	if e.Config.Workflow != nil {
		p := e.Config.Workflow.EntryPhase
		s.Workflow = &WorkflowState{ActivePhase: &p, CompletedPhases: []string{}, ObservedEvidence: []string{}, Transitions: []Transition{}}
	}
	return s
}
func (e Engine) Run(ctx context.Context, s *Session, max int, emit Emit) (runErr error) {
	NormalizeSession(s)
	StageMessages(s)
	runID := uuid.NewString()
	s.RunID = &runID
	started := time.Now().UTC()
	s.RunStartedAt, s.RunEndedAt, s.RunTerminationReason = &started, nil, nil
	ArchiveRecord(s, "run_started", map[string]any{"run_count": s.RunCount + 1})
	defer func() {
		if runErr != nil && s.RunStatus != "blocked" && s.RunStatus != "cancelled" {
			s.RunStatus = "interrupted"
			s.LastError = map[string]any{"message": runErr.Error()}
		}
		ended := time.Now().UTC()
		s.RunEndedAt = &ended
		reason := s.RunStatus
		if runErr != nil {
			reason = runErr.Error()
		}
		s.RunTerminationReason = &reason
		kind := "run_finished"
		if s.RunStatus == "interrupted" {
			kind = "run_interrupted"
		}
		ArchiveRecord(s, kind, map[string]any{"status": s.RunStatus, "reason": reason})
	}()
	s.RunStatus = "running"
	defer func() {
		if s.RunStatus == "running" {
			if s.Status == "complete" {
				s.RunStatus = "complete"
			} else {
				s.RunStatus = "failed"
			}
		}
	}()
	if max <= 0 {
		if e.Config.Memory.RunMaxSteps != nil && *e.Config.Memory.RunMaxSteps > 0 {
			max = *e.Config.Memory.RunMaxSteps
		} else {
			max = e.Config.Memory.MaxSteps
		}
	}
	limit := s.StepCount + max
	if s.ProgressSeenOutputs == nil {
		s.ProgressSeenOutputs = []string{}
	}
	if s.ProgressFingerprint == "" {
		s.ProgressFingerprint = e.progressFingerprint(s)
	}
	diagnosticCursor := 0
	emitDiagnostics := func() error {
		client, ok := e.Client.(interface {
			RequestDiagnosticSnapshot() []ModelRequestDiagnostic
		})
		if !ok {
			return nil
		}
		requests := client.RequestDiagnosticSnapshot()
		for diagnosticCursor < len(requests) {
			if err := out(emit, map[string]any{"type": "model_request_diagnostic", "data": requests[diagnosticCursor]}); err != nil {
				return err
			}
			diagnosticCursor++
		}
		return nil
	}
	s.RunCount++
	if err := out(emit, map[string]any{"type": "status", "message": "Agent run started.", "run_count": s.RunCount}); err != nil {
		return err
	}
	if len(s.PendingToolCalls) > 0 {
		if err := checkExecutionCheckpoint(ctx); err != nil {
			if errors.Is(err, ErrRunCancelled) {
				s.Status, s.RunStatus = "failed", "cancelled"
				s.FinalAnswer = str("Run cancelled; use /resume to recover committed work.")
				s.LastError = map[string]any{"code": "run_cancelled", "message": *s.FinalAnswer, "recoverable": true}
			}
			return err
		}
		if err := e.recoverPending(ctx, s, emit); err != nil {
			return err
		}
	}
	for s.Status == "active" && s.StepCount < limit {
		if err := checkExecutionCheckpoint(ctx); err != nil {
			if errors.Is(err, ErrRunCancelled) {
				s.Status, s.RunStatus = "failed", "cancelled"
				s.FinalAnswer = str("Run cancelled; use /resume to recover committed work.")
				s.LastError = map[string]any{"code": "run_cancelled", "message": *s.FinalAnswer, "recoverable": true}
				ArchiveRecord(s, "run_cancel_requested", map[string]any{"run_id": s.RunID})
				_ = out(emit, map[string]any{"type": "error", "code": "run_cancelled", "message": *s.FinalAnswer, "recoverable": true})
			}
			return err
		}
		if x := e.advance(s); x != nil {
			if err := out(emit, map[string]any{"type": "interim_result", "step": "phase_completed", "phase_id": x.FromPhase, "next_phase": x.ToPhase, "reason": x.Reason}); err != nil {
				return err
			}
		}
		compactErr := e.compact(ctx, s, emit)
		if err := emitDiagnostics(); err != nil {
			return err
		}
		if compactErr != nil {
			s.Status, s.RunStatus = "failed", "failed"
			s.FinalAnswer = str(compactErr.Error())
			s.LastError = map[string]any{"code": "context_error", "message": compactErr.Error()}
			code := "context_integrity_error"
			if strings.Contains(compactErr.Error(), "context_budget_exceeded") {
				code = "context_budget_exceeded"
			}
			return out(emit, map[string]any{"type": "error", "code": code, "message": compactErr.Error(), "recoverable": true})
		}
		if err := out(emit, map[string]any{"type": "status", "message": "Agent deciding next action.", "step_count": s.StepCount}); err != nil {
			return err
		}
		progressBefore := e.progressFingerprint(s)
		d, err := e.Client.Decide(ctx, e.payload(s))
		if diagnosticErr := emitDiagnostics(); diagnosticErr != nil {
			return diagnosticErr
		}
		if err != nil {
			return err
		}
		if err = checkExecutionCheckpoint(ctx); err != nil {
			if errors.Is(err, ErrRunCancelled) {
				s.Status, s.RunStatus = "failed", "cancelled"
				s.FinalAnswer = str("Run cancelled; use /resume to recover committed work.")
				s.LastError = map[string]any{"code": "run_cancelled", "message": *s.FinalAnswer, "recoverable": true}
				ArchiveRecord(s, "run_cancel_requested", map[string]any{"run_id": s.RunID})
				_ = out(emit, map[string]any{"type": "error", "code": "run_cancelled", "message": *s.FinalAnswer, "recoverable": true})
			}
			return err
		}
		s.StepCount++
		recordDecision(s, d)
		if d.Invalid != "" {
			s.ConsecutiveInvalidResponses++
			ArchiveRecord(s, "decision_rejected", map[string]any{"message": d.Invalid})
			feedback := "Runtime correction: " + d.Invalid + ". Return a valid tool call or a nonempty final answer."
			AppendMessage(s, Message{Role: "user", Content: str(feedback)})
			responseCode, _, _ := strings.Cut(d.Invalid, ":")
			if responseCode == "" {
				responseCode = "invalid_model_response"
			}
			if err := out(emit, map[string]any{"type": "interim_result", "step": "response_rejected", "data": map[string]any{"code": responseCode, "message": d.Invalid, "consecutive_invalid_responses": s.ConsecutiveInvalidResponses}}); err != nil {
				return err
			}
			if s.ConsecutiveInvalidResponses >= e.Config.Execution.MaxConsecutiveInvalidResponses {
				s.Status = "failed"
				s.RunStatus = "failed"
				s.FinalAnswer = str("Consecutive invalid-response limit reached. Resume to retry.")
				s.LastError = map[string]any{"code": "invalid_response_limit", "message": *s.FinalAnswer}
				return out(emit, map[string]any{"type": "error", "code": "invalid_response_limit", "message": "Consecutive invalid-response limit reached. Resume to retry.", "recoverable": true})
			}
			if err := e.observeProgress(s, progressBefore, emit); err != nil {
				return err
			}
			continue
		}
		s.ConsecutiveInvalidResponses = 0
		if d.Final {
			AppendMessage(s, Message{Role: "assistant", Content: &d.Content})
			errors := e.completionErrors(s, d.Content)
			if len(errors) == 0 {
				s.Status = "complete"
				s.RunStatus = "complete"
				s.FinalAnswer = &d.Content
				s.NoProgressSteps = 0
				s.LastError = nil
				if err := out(emit, map[string]any{"type": "final_result", "answer": d.Content}); err != nil {
					return err
				}
			} else {
				s.RejectedFinals = append(s.RejectedFinals, RejectedFinal{Content: d.Content, Errors: errors})
				ArchiveRecord(s, "rejected_final", map[string]any{"content": d.Content, "errors": errors})
				feedback := "Runtime correction: " + strings.Join(errors, " ")
				AppendMessage(s, Message{Role: "user", Content: str(feedback)})
				if err := out(emit, map[string]any{"type": "interim_result", "step": "response_rejected", "data": map[string]any{"code": "completion_rejected", "message": feedback, "errors": errors}}); err != nil {
					return err
				}
				if err := e.observeProgress(s, progressBefore, emit); err != nil {
					return err
				}
			}
			continue
		}
		items := d.Calls
		if len(items) == 0 {
			items = []ToolCall{{ID: d.ID, Name: d.Name, Arguments: d.Arguments}}
		}
		calls := make([]map[string]any, 0, len(items))
		for _, call := range items {
			if err := checkExecutionCheckpoint(ctx); err != nil {
				if errors.Is(err, ErrRunCancelled) {
					s.Status, s.RunStatus = "failed", "cancelled"
					s.FinalAnswer = str("Run cancelled; use /resume to recover committed work.")
					s.LastError = map[string]any{"code": "run_cancelled", "message": *s.FinalAnswer, "recoverable": true}
					ArchiveRecord(s, "run_cancel_requested", map[string]any{"run_id": s.RunID})
					_ = out(emit, map[string]any{"type": "error", "code": "run_cancelled", "message": *s.FinalAnswer, "recoverable": true})
				}
				return err
			}
			args := call.RawArguments
			if args == "" {
				b, _ := json.Marshal(call.Arguments)
				args = string(b)
			}
			calls = append(calls, map[string]any{"id": call.ID, "type": "function", "function": map[string]any{"name": call.Name, "arguments": args}})
		}
		AppendMessage(s, Message{Role: "assistant", ToolCalls: calls})
		s.PendingToolCalls = []map[string]any{}
		var batchEmitErr error
		for _, call := range items {
			s.PendingToolCalls = append(s.PendingToolCalls, map[string]any{"id": call.ID, "name": call.Name, "arguments": Clone(call.Arguments), "raw_arguments": call.RawArguments})
		}

		for _, call := range items {
			result := "ERROR: invalid tool arguments."
			invoked := false
			if call.Invalid == "" && call.Arguments != nil {
				result, invoked = e.call(ctx, s, Decision{ID: call.ID, Name: call.Name, Arguments: call.Arguments}, emit)
				if strings.HasPrefix(result, "ERROR: recovery_required:") {
					s.Status, s.RunStatus = "failed", "blocked"
					s.LastError = map[string]any{"code": "recovery_required", "message": result, "recoverable": true}
					if err := out(emit, map[string]any{"type": "tool_uncertain", "tool_call_id": call.ID, "message": result}); err != nil {
						return err
					}
					return fmt.Errorf("recovery_required: unresolved tool outcome")
				}
				if invoked {
					e.captureArtifacts(s, call, result)
					if e.Config.Workflow == nil && !strings.HasPrefix(result, "ERROR:") && !strings.HasPrefix(result, "AssertionError:") {
						b, _ := json.Marshal([]any{call.Name, call.Arguments, result})
						digest := fmt.Sprintf("%x", sha256.Sum256(b))
						if !has(s.ProgressSeenOutputs, digest) {
							s.ProgressSeenOutputs = append(s.ProgressSeenOutputs, digest)
							if len(s.ProgressSeenOutputs) > 64 {
								s.ProgressSeenOutputs = s.ProgressSeenOutputs[len(s.ProgressSeenOutputs)-64:]
							}
						}
					}
				}
			}
			AppendMessage(s, Message{Role: "tool", Name: str(call.Name), ToolCallID: str(call.ID), Content: &result})
			if invoked {
				e.record(s, call.Name, call.Arguments, result)
			}
			if !invoked {
				ArchiveRecord(s, "decision_rejected", map[string]any{"tool_call_id": call.ID, "message": result})
			}
			s.PendingToolCalls = s.PendingToolCalls[1:]
			if err := out(emit, map[string]any{"type": "interim_result", "step": "tool_completed", "data": map[string]any{"tool": call.Name, "output": result, "tool_call_id": call.ID}}); err != nil && batchEmitErr == nil {
				// The tool result is already known and settled. Finish the batch so
				// every response remains paired with the assistant's multi-call turn.
				batchEmitErr = err
			}
		}
		if batchEmitErr != nil {
			return batchEmitErr
		}
		if err := e.observeProgress(s, progressBefore, emit); err != nil {
			return err
		}
	}
	if s.Status == "active" && s.StepCount >= limit {
		x := fmt.Sprintf("Step limit (%d) reached without a final answer. Resume with another run request.", max)
		s.Status = "failed"
		s.RunStatus = "failed"
		s.FinalAnswer = &x
		s.LastError = map[string]any{"code": "step_limit", "message": x}
		return out(emit, map[string]any{"type": "error", "message": x, "recoverable": true})
	}
	return nil
}

func (e Engine) recoverPending(ctx context.Context, s *Session, emit Emit) error {
	for len(s.PendingToolCalls) > 0 {
		pending := s.PendingToolCalls[0]
		id, _ := pending["id"].(string)
		name, _ := pending["name"].(string)
		args, _ := pending["arguments"].(map[string]any)
		if id == "" || name == "" || args == nil {
			return fmt.Errorf("recovery_required: persisted pending tool call is malformed")
		}
		result, invoked := e.call(ctx, s, Decision{ID: id, Name: name, Arguments: args}, emit)
		if strings.HasPrefix(result, "ERROR: recovery_required:") {
			s.Status, s.RunStatus = "failed", "blocked"
			s.LastError = map[string]any{"code": "recovery_required", "message": result, "recoverable": true}
			if err := out(emit, map[string]any{"type": "tool_uncertain", "tool_call_id": id, "message": result}); err != nil {
				return err
			}
			return fmt.Errorf("recovery_required: unresolved tool outcome")
		}
		e.captureArtifacts(s, ToolCall{ID: id, Name: name, Arguments: args}, result)
		AppendMessage(s, Message{Role: "tool", Name: str(name), ToolCallID: str(id), Content: &result})
		if invoked {
			e.record(s, name, args, result)
		}
		s.PendingToolCalls = s.PendingToolCalls[1:]
		if err := out(emit, map[string]any{"type": "interim_result", "step": "tool_recovered", "data": map[string]any{"tool": name, "output": result, "tool_call_id": id}}); err != nil {
			return err
		}
	}
	return nil
}

func out(f Emit, v map[string]any) error {
	if f != nil {
		return f(v)
	}
	return nil
}
func (e Engine) payload(s *Session) []Message {
	p := []Message{{Role: "system", Content: &e.Config.SystemPrompt}, {Role: "user", Content: str("Original user goal (immutable): " + s.Goal)}}
	if w := e.completionInstructions(s); w != "" {
		p = append(p, Message{Role: "system", Content: &w})
	}
	if s.CompactedContext != nil {
		p = append(p, Message{Role: "user", Content: str("[COMPACTED CONTEXT STATE — UNVERIFIED MODEL CLAIMS]\n" + *s.CompactedContext)})
	}
	for i, check := range e.Config.Completion.ResultChecks {
		if artifact, ok := s.CompletionArtifacts[strconv.Itoa(i)]; ok {
			p = append(p, Message{Role: "system", Content: str("[EXECUTED RESULT ARTIFACT — " + check.Label + "]\n" + describeArtifact(check, artifact.Value))})
		}
	}
	if len(s.Memory) > 1 {
		p = append(p, Message{Role: "system", Content: str("[RAW WORKING BUFFER — PRESERVE VERBATIM]")})
	}
	p = append(p, s.Memory[1:]...)
	for _, binding := range e.Config.Execution.ArgumentBindings {
		for i, check := range e.Config.Completion.ResultChecks {
			if check.Label != binding.SourceLabel {
				continue
			}
			if artifact, ok := s.CompletionArtifacts[strconv.Itoa(i)]; ok {
				value, found := valueAtPath(artifact.Value, binding.Path)
				if found {
					b, _ := json.Marshal(value)
					p = append(p, Message{Role: "system", Content: str(fmt.Sprintf("[KNOWN ARGUMENT BINDING]\n%s.%s must equal executed result %s.%v = %s", binding.Tool, binding.Argument, binding.SourceLabel, binding.Path, b))})
				}
			}
		}
	}
	if w := e.instruction(s); w != "" {
		p = append(p, Message{Role: "system", Content: &w})
	}
	if len(e.Config.Completion.ResultChecks) > 0 && e.complete(s) {
		parts := []string{"[FINAL SYNTHESIS] All required workflow phases have verified completion evidence. Return the required deliverables now; do not call tools or provide phase-status commentary."}
		if e.Config.Completion.Instructions != nil && *e.Config.Completion.Instructions != "" {
			parts = append(parts, *e.Config.Completion.Instructions)
		}
		for i, check := range e.Config.Completion.ResultChecks {
			if artifact, ok := s.CompletionArtifacts[strconv.Itoa(i)]; ok {
				parts = append(parts, "Verified result artifact for "+check.Label+":\n"+describeArtifact(check, artifact.Value))
			}
		}
		p = append(p, Message{Role: "system", Content: str(strings.Join(parts, "\n"))})
	}
	return p
}
func str(s string) *string { return &s }
func (e Engine) phase(id string) *Phase {
	if e.Config.Workflow == nil {
		return nil
	}
	for i := range e.Config.Workflow.Phases {
		if e.Config.Workflow.Phases[i].ID == id {
			return &e.Config.Workflow.Phases[i]
		}
	}
	return nil
}
func (e Engine) instruction(s *Session) string {
	if s.Workflow == nil || s.Workflow.ActivePhase == nil {
		return ""
	}
	p := e.phase(*s.Workflow.ActivePhase)
	return "[WORKFLOW STATE]\nActive phase: " + p.Name + " (" + p.ID + ").\nPhase instruction: " + p.Instruction + "\nDo not provide prose progress updates or a final answer while this phase is incomplete; make a tool call instead.\n" + e.workflowGuidance(s)
}
func (e Engine) complete(s *Session) bool {
	return e.Config.Workflow == nil || (s.Workflow != nil && s.Workflow.ActivePhase == nil)
}
func (e Engine) progressFingerprint(s *Session) string {
	var workflow any
	if s.Workflow != nil {
		workflow = s.Workflow
	}
	artifacts := map[string]any{}
	for key, artifact := range s.CompletionArtifacts {
		artifacts[key] = map[string]any{"tool": artifact.Tool, "arguments": artifact.Arguments, "value": artifact.Value}
	}
	outputs := []string{}
	if e.Config.Workflow == nil {
		outputs = s.ProgressSeenOutputs
	}
	data, _ := json.Marshal([]any{workflow, artifacts, outputs})
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
func (e Engine) observeProgress(s *Session, before string, emit Emit) error {
	current := e.progressFingerprint(s)
	if current != before {
		s.NoProgressSteps = 0
		s.ProgressFingerprint = current
		return nil
	}
	s.NoProgressSteps++
	s.ProgressFingerprint = current
	limit := e.Config.Execution.MaxNoProgressSteps
	if limit == nil {
		return nil
	}
	guidance := e.workflowGuidance(s)
	if s.Workflow == nil || s.Workflow.ActivePhase == nil {
		guidance = "Obtain new tool evidence or return the verified final deliverables."
	}
	warningAt := *limit / 2
	if warningAt < 1 {
		warningAt = 1
	}
	if s.NoProgressSteps == warningAt {
		feedback := fmt.Sprintf("No new task evidence in %d decisions. Repeating a successful call does not complete missing requirements.\n%s", s.NoProgressSteps, guidance)
		AppendMessage(s, Message{Role: "user", Content: str(feedback)})
		if err := out(emit, map[string]any{"type": "interim_result", "step": "no_progress_warning", "data": map[string]any{"no_progress_steps": s.NoProgressSteps, "message": feedback}}); err != nil {
			return err
		}
	}
	if s.NoProgressSteps >= *limit {
		message := fmt.Sprintf("No new task evidence in %d decisions. %s Resume after correcting the workflow or instructions.", *limit, guidance)
		s.Status, s.RunStatus, s.FinalAnswer = "failed", "failed", &message
		s.LastError = map[string]any{"code": "no_progress_limit", "message": message}
		return out(emit, map[string]any{"type": "error", "code": "no_progress_limit", "message": message, "recoverable": true})
	}
	return nil
}
func (e Engine) allowed(s *Session, name string) bool {
	if s.Workflow == nil {
		return true
	}
	if s.Workflow.ActivePhase == nil {
		return false
	}
	p := e.phase(*s.Workflow.ActivePhase)
	a := p.AllowedTools
	// An omitted allow-list falls back to completion tools, as in the Python
	// implementation. An explicitly empty list deliberately allows no tools.
	if !p.AllowedToolsSet {
		for _, x := range p.Completion {
			a = append(a, x.Tool)
		}
	}
	for _, x := range a {
		if x == name {
			return true
		}
	}
	return false
}
func (e Engine) call(ctx context.Context, s *Session, d Decision, emit Emit) (string, bool) {
	if !e.allowed(s, d.Name) {
		if s.Workflow != nil && s.Workflow.ActivePhase != nil {
			return fmt.Sprintf("ERROR: Workflow phase '%s' cannot execute tool '%s'. Complete the active phase first.", *s.Workflow.ActivePhase, d.Name), false
		}
		return "ERROR: Workflow is complete; no further tools can execute.", false
	}
	f := e.Tools[d.Name]
	if f == nil {
		return "ERROR: unknown tool '" + d.Name + "'.", false
	}
	if e.Config.Execution.ToolPolicy == "none" {
		return "ERROR: tool execution is disabled by execution.tool_policy.", false
	}
	var declaration *ToolDefinition
	for i := range e.Config.Tools {
		if e.Config.Tools[i].Name == d.Name {
			declaration = &e.Config.Tools[i]
			break
		}
	}
	if declaration == nil {
		return "ERROR: tool '" + d.Name + "' has no configured declaration.", false
	}
	if d.Arguments == nil {
		return "ERROR: tool arguments must be a JSON object.", false
	}
	if problems := validateArguments(d.Arguments, declaration.Parameters, "$"); len(problems) > 0 {
		return "ERROR: invalid arguments for tool '" + d.Name + "': " + strings.Join(problems, "; "), false
	}
	if issue := e.bindingError(s, d.Name, d.Arguments); issue != "" {
		return "ERROR: action rejected before execution: " + issue, false
	}
	policy := map[string]any{"mode": "reconcile", "max_attempts": 1}
	for _, definition := range e.Config.Tools {
		if definition.Name == d.Name && definition.Recovery != nil {
			policy = map[string]any{"mode": definition.Recovery.Mode, "max_attempts": definition.Recovery.MaxAttempts}
		}
	}
	mode, _ := policy["mode"].(string)
	idempotent, reconciler := recoveryAdapter(d.Name)
	maxAttempts, _ := policy["max_attempts"].(int)
	if maxAttempts == 0 { // JSON round trips may normalize integer values to float64.
		if numeric, ok := policy["max_attempts"].(float64); ok {
			maxAttempts = int(numeric)
		}
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	executionIndex := -1
	for i := range s.Executions {
		if s.Executions[i].CallID == d.ID {
			executionIndex = i
			break
		}
	}
	if executionIndex >= 0 {
		execution := s.Executions[executionIndex]
		if execution.Tool != d.Name || !equalJSON(execution.Arguments, d.Arguments) || !equalJSON(execution.Policy, policy) {
			s.Executions[executionIndex].Status = "uncertain"
			s.ExecutionPending = []map[string]any{executionRecord(s.Executions[executionIndex])}
			return "ERROR: recovery_required: persisted tool identity or recovery policy changed.", false
		}
		if execution.Status == "succeeded" || execution.Status == "failed" {
			return execution.Result, true
		}
		if execution.Status == "started" || execution.Status == "uncertain" {
			if reconciler != nil {
				result, found, err := reconciler(ctx, Clone(d.Arguments), execution.IdempotencyKey)
				if err != nil {
					return e.markUncertain(s, executionIndex, "Reconciliation failed: "+err.Error())
				}
				if found {
					x := &s.Executions[executionIndex]
					x.Status, x.Result = resultStatus(result), result
					finished := time.Now().UTC().Truncate(time.Microsecond)
					x.FinishedAt = &finished
					s.ExecutionPending = []map[string]any{executionRecord(*x)}
					ArchiveRecord(s, "tool_outcome", map[string]any{"execution_id": x.ExecutionID, "status": x.Status, "result": result, "reconciled": true})
					return result, true
				}
			}
			if mode == "reconcile" {
				return e.markUncertain(s, executionIndex, "Tool outcome is uncertain; reconciliation found no confirmed result.")
			}
			if mode == "idempotent" && idempotent == nil {
				return e.markUncertain(s, executionIndex, "Idempotent adapter is not registered.")
			}
		}
	} else {
		id, key := uuid.NewString(), uuid.NewString()
		runID := ""
		if s.RunID != nil {
			runID = *s.RunID
		}
		s.Executions = append(s.Executions, ToolExecution{ExecutionID: id, RunID: runID, CallID: d.ID, Tool: d.Name, Arguments: Clone(d.Arguments), Policy: Clone(policy), IdempotencyKey: key, Status: "pending"})
		executionIndex = len(s.Executions) - 1
		execution := s.Executions[executionIndex]
		ArchiveRecord(s, "tool_intent", map[string]any{"execution_id": id, "tool_call_id": d.ID, "tool": d.Name, "arguments": d.Arguments, "policy": policy, "idempotency_key": key})
		s.ExecutionPending = []map[string]any{executionRecord(execution)}
		if err := out(emit, map[string]any{"type": "tool_intent", "execution_id": id, "tool_call_id": d.ID}); err != nil {
			return e.markUncertain(s, executionIndex, "Tool intent commit was not acknowledged: "+err.Error())
		}
	}
	if mode == "idempotent" {
		if idempotent == nil {
			if executionIndex >= 0 {
				return e.markUncertain(s, executionIndex, "Idempotent adapter is not registered.")
			}
			return "ERROR: recovery_required: idempotent adapter is not registered; tool was not invoked.", false
		}
	}
	for s.Executions[executionIndex].Attempts < maxAttempts {
		execution := &s.Executions[executionIndex]
		execution.Attempts++
		execution.Status = "started"
		started := time.Now().UTC().Truncate(time.Microsecond)
		execution.StartedAt = &started
		if s.RunID != nil {
			execution.RunID = *s.RunID
		}
		ArchiveRecord(s, "tool_attempt", map[string]any{"execution_id": execution.ExecutionID, "attempt": execution.Attempts})
		s.ExecutionPending = []map[string]any{executionRecord(*execution)}
		if err := out(emit, map[string]any{"type": "tool_started", "execution_id": execution.ExecutionID, "attempt": execution.Attempts}); err != nil {
			return e.markUncertain(s, executionIndex, "Tool start commit was not acknowledged: "+err.Error())
		}
		execution = &s.Executions[executionIndex]
		var result string
		var err error
		if mode == "idempotent" {
			result, err = idempotent(ctx, Clone(d.Arguments), execution.IdempotencyKey)
		} else {
			result, err = f(ctx, d.Arguments)
		}
		if err != nil {
			ArchiveRecord(s, "tool_attempt_failed", map[string]any{"execution_id": execution.ExecutionID, "attempt": execution.Attempts, "error": err.Error()})
			var transient TransientToolError
			if errors.As(err, &transient) {
				if mode == "reconcile" {
					return e.markUncertain(s, executionIndex, "Transient failure may have produced an external effect.")
				}
				if execution.Attempts < maxAttempts {
					continue
				}
				result = "ERROR: transient tool failure; automatic attempt limit reached."
			} else {
				if mode == "reconcile" || mode == "idempotent" {
					return e.markUncertain(s, executionIndex, "Tool raised without a confirmed external outcome: "+err.Error())
				}
				result = "ERROR: " + err.Error()
			}
		}
		// Adapter-returned strings, including ERROR strings, are known outcomes.
		status := resultStatus(result)
		execution = &s.Executions[executionIndex]
		execution.Status, execution.Result = status, result
		finished := time.Now().UTC().Truncate(time.Microsecond)
		execution.FinishedAt = &finished
		s.ExecutionPending = []map[string]any{executionRecord(*execution)}
		ArchiveRecord(s, "tool_outcome", map[string]any{"execution_id": execution.ExecutionID, "status": status, "result": result})
		return result, true
	}
	return e.markUncertain(s, executionIndex, "Permitted tool attempts are exhausted; reconciliation required.")
}

func resultStatus(result string) string {
	if strings.HasPrefix(result, "ERROR:") || strings.HasPrefix(result, "AssertionError:") {
		return "failed"
	}
	return "succeeded"
}

func (e Engine) markUncertain(s *Session, index int, reason string) (string, bool) {
	x := &s.Executions[index]
	x.Status = "uncertain"
	s.ExecutionPending = []map[string]any{executionRecord(*x)}
	ArchiveRecord(s, "tool_uncertain", map[string]any{"execution_id": x.ExecutionID, "reason": reason})
	return "ERROR: recovery_required: " + reason, false
}

func equalJSON(a, b any) bool {
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	return err == nil && string(x) == string(y)
}

func executionRecord(x ToolExecution) map[string]any {
	var result any
	if x.Status == "succeeded" || x.Status == "failed" {
		result = x.Result
	}
	return map[string]any{"execution_id": x.ExecutionID, "run_id": x.RunID, "call_id": x.CallID, "tool": x.Tool, "arguments": Clone(x.Arguments), "policy": Clone(x.Policy), "idempotency_key": x.IdempotencyKey, "attempts": x.Attempts, "status": x.Status, "result": result, "started_at": x.StartedAt, "finished_at": x.FinishedAt}
}
func (e Engine) missing(s *Session, p *Phase) []string {
	r := []string{}
	for i, x := range p.Completion {
		if x.Required && !has(s.Workflow.ObservedEvidence, key(p.ID, "completion", i)) {
			r = append(r, x.Description)
		}
	}
	return r
}
func (e Engine) advance(s *Session) *Transition {
	if s.Workflow == nil || s.Workflow.ActivePhase == nil {
		return nil
	}
	p := e.phase(*s.Workflow.ActivePhase)
	for n, x := range p.Transitions {
		ok := x.When == "always" || (x.When == "phase_complete" && len(e.missing(s, p)) == 0)
		if x.When == "evidence" {
			ok = true
			for i := range x.Evidence {
				ok = ok && has(s.Workflow.ObservedEvidence, key(p.ID, fmt.Sprintf("transition-%d", n), i))
			}
		}
		if ok {
			z := Transition{FromPhase: p.ID, ToPhase: x.To, Reason: x.When}
			s.Workflow.CompletedPhases = appendUnique(s.Workflow.CompletedPhases, p.ID)
			if x.To == "complete" {
				s.Workflow.ActivePhase = nil
			} else {
				s.Workflow.ActivePhase = str(x.To)
			}
			s.Workflow.Transitions = append(s.Workflow.Transitions, z)
			return &z
		}
	}
	return nil
}
func (e Engine) record(s *Session, name string, args map[string]any, output string) {
	if s.Workflow == nil || s.Workflow.ActivePhase == nil {
		return
	}
	p := e.phase(*s.Workflow.ActivePhase)
	for i, x := range p.Completion {
		e.recordOne(s, p.ID, "completion", i, x, name, args, output)
	}
	for n, t := range p.Transitions {
		for i, x := range t.Evidence {
			e.recordOne(s, p.ID, fmt.Sprintf("transition-%d", n), i, x, name, args, output)
		}
	}
}
func (e Engine) recordOne(s *Session, phase, cat string, i int, x Evidence, name string, args map[string]any, out string) {
	k := key(phase, cat, i)
	if has(s.Workflow.ObservedEvidence, k) || x.Tool != name || !contains(args, x.Arguments) {
		return
	}
	for _, q := range x.ContentContains {
		if !strings.Contains(strings.ToLower(out), strings.ToLower(q)) {
			return
		}
	}
	if len(x.Result) > 0 {
		v, err := strictJSON([]byte(out))
		if err != nil || !contains(v, x.Result) {
			return
		}
	}
	s.Workflow.ObservedEvidence = append(s.Workflow.ObservedEvidence, k)
}
func key(p, c string, i int) string { return fmt.Sprintf("%s:%s:%d", p, c, i) }
func has(a []string, x string) bool {
	for _, v := range a {
		if v == x {
			return true
		}
	}
	return false
}
func appendUnique(a []string, x string) []string {
	if !has(a, x) {
		return append(a, x)
	}
	return a
}
func contains(a, b any) bool {
	if b == nil {
		return true
	}
	switch y := b.(type) {
	case map[string]any:
		x, ok := a.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range y {
			if q, ok := x[k]; !ok || !contains(q, v) {
				return false
			}
		}
		return true
	case []any:
		x, ok := a.([]any)
		if !ok {
			return false
		}
		for _, v := range y {
			found := false
			for _, q := range x {
				if contains(q, v) {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return jsonEqual(a, b)
	}
}
func (e Engine) compact(ctx context.Context, s *Session, emit Emit) error {
	StageMessages(s)
	policy := e.Config.Memory
	if policy.ContextWindowTokens <= 0 {
		policy.ContextWindowTokens = 32768
	}
	if policy.ReservedOutputTokens <= 0 {
		policy.ReservedOutputTokens = 1024
	}
	if policy.SafetyMarginTokens <= 0 {
		policy.SafetyMarginTokens = 256
	}
	if policy.MaxSummaryTokens <= 0 {
		policy.MaxSummaryTokens = 4096
	}
	if policy.SummaryOutputTokens <= 0 {
		policy.SummaryOutputTokens = 512
	}
	if policy.CompactionMaxAttempts <= 0 {
		policy.CompactionMaxAttempts = 2
	}
	if policy.CompactionMaxPasses <= 0 {
		policy.CompactionMaxPasses = 3
	}
	if policy.CompactionMinGrowthTokens <= 0 {
		policy.CompactionMinGrowthTokens = 512
	}
	_, err := interactionGroups(s.Memory)
	if err != nil {
		return err
	}
	payload, err := (OpenAIClient{Config: e.Config}).decisionPayload(e.payload(s))
	if err != nil {
		return err
	}
	budget, err := estimatePayload(payload, policy)
	if err != nil {
		return err
	}
	s.RequestBudget = &budget
	hardPressure := !budget.Fits
	intervalDue := policy.CompactionIntervalSteps != nil && s.StepCount-s.LastCompactionStep >= *policy.CompactionIntervalSteps
	thresholdDue := policy.MaxTokens > 0 && budget.EstimatedInputTokens >= policy.MaxTokens
	growthDue := s.LastCompactionEstimatedTokens == 0 || budget.EstimatedInputTokens-s.LastCompactionEstimatedTokens >= policy.CompactionMinGrowthTokens
	spacingOK := s.LastCompactionStep == 0 || s.StepCount-s.LastCompactionStep >= policy.CompactionMinIntervalSteps
	softDue := spacingOK && growthDue && (intervalDue || thresholdDue)
	if e.Config.Workflow != nil && e.complete(s) {
		softDue = false
	}
	if softDue && policy.OptionalCompactionMinPressureRatio > 0 {
		ratio := float64(budget.EstimatedInputTokens+budget.ReservedOutputTokens+budget.SafetyMarginTokens) / float64(maxInt(1, budget.ContextWindowTokens))
		if ratio < policy.OptionalCompactionMinPressureRatio {
			softDue = false
		}
	}
	if softDue && policy.OptionalCompactionMinReclaimableTokens > 0 {
		minimum := policy.RawTurnsToKeep
		_, raw, splitErr := splitHistory(s.Memory[1:], minimum)
		if splitErr != nil {
			return splitErr
		}
		candidate := *s
		candidate.Memory = append([]Message{s.Memory[0]}, flattenMessages(raw)...)
		floorPayload, payloadErr := (OpenAIClient{Config: e.Config}).decisionPayload(e.payload(&candidate))
		if payloadErr != nil {
			return payloadErr
		}
		floorBudget, budgetErr := estimatePayload(floorPayload, policy)
		if budgetErr != nil {
			return budgetErr
		}
		if budget.EstimatedInputTokens-floorBudget.EstimatedInputTokens < policy.OptionalCompactionMinReclaimableTokens {
			softDue = false
		}
	}
	due := hardPressure || softDue
	if !due {
		reason := "optional_gates_not_met"
		if e.Config.Workflow != nil && e.complete(s) {
			reason = "workflow_complete"
		}
		if s.CompactionSkipCounts == nil {
			s.CompactionSkipCounts = map[string]int{}
		}
		s.CompactionSkipCounts[reason]++
		RecordDiagnostic(s, "compaction_decisions", map[string]any{"step": s.StepCount, "accepted": false, "reason": reason, "estimated_tokens": budget.EstimatedInputTokens})
		return nil
	}
	trigger := "optional"
	if hardPressure {
		trigger = "hard_context_pressure"
	}
	if s.CompactionTriggerCounts == nil {
		s.CompactionTriggerCounts = map[string]int{}
	}
	s.CompactionTriggerCounts[trigger]++
	if policy.ContextWindowTokens <= 0 {
		return fmt.Errorf("context_budget_exceeded: context window is not configured")
	}
	minimum := policy.RawTurnsToKeep
	history := s.Memory[1:]
	oldGroups, rawGroups, err := splitHistory(history, minimum)
	if err != nil {
		return err
	}
	if len(oldGroups) == 0 {
		if hardPressure {
			return budgetError(budget)
		}
		return nil
	}
	var lastFailure string
	maxPasses := policy.CompactionMaxPasses
	if maxPasses < 1 {
		maxPasses = 1
	}
	maxAttempts := policy.CompactionMaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	attemptsUsed := 0
	for pass := 0; pass < maxPasses; pass++ {
		if len(oldGroups) == 0 {
			break
		}
		historical, sourceIndexes := flattenGroups(oldGroups)
		_, retainedIndexes := flattenGroups(rawGroups)
		for i := range sourceIndexes {
			sourceIndexes[i]++
		}
		for i := range retainedIndexes {
			retainedIndexes[i]++
		}
		source, err := sourceText(s.Goal, historical, s.CompactedContext)
		if err != nil {
			return err
		}
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(source)))
		if hash == s.LastCompactionAttemptSHA256 {
			lastFailure = "unchanged failed source suppressed"
			break
		}
		before := budget.EstimatedInputTokens
		reason := "hard_context_pressure"
		if !hardPressure {
			reason = "optional_interval"
			if thresholdDue {
				reason = "optional_size_threshold"
			}
		}
		var candidateBudget RequestBudget
		accepted := false
		summaryPayload := (OpenAIClient{Config: e.Config}).summaryPayload(s.Goal, historical, s.CompactedContext, policy.HybridReflection)
		summaryBudget, summaryBudgetErr := estimatePayload(summaryPayload, policy)
		if summaryBudgetErr != nil {
			return summaryBudgetErr
		}
		summaryFits := summaryBudget.Fits
		if !summaryFits {
			lastFailure = "summary request exceeds configured context budget"
		}
		for attempt := 0; attempt < maxAttempts && attemptsUsed < maxAttempts && summaryFits; attempt++ {
			attemptsUsed++
			s.LastCompactionAttemptStep = s.StepCount
			s.LastCompactionAttemptSHA256 = hash
			proposed, callErr := e.Client.Compact(ctx, s.Goal, historical, s.CompactedContext, policy.HybridReflection)
			entry := map[string]any{"pass": pass + 1, "attempt": attempt + 1, "source_sha256": hash, "source_indexes": sourceIndexes, "retained_indexes": retainedIndexes, "step": s.StepCount}
			if callErr != nil {
				lastFailure = callErr.Error()
				entry["accepted"] = false
				entry["reason"] = lastFailure
				recordSummaryAttempt(s, entry, proposed, historical, flattenMessages(rawGroups), canonicalSummary(s), false)
				continue
			}
			ledger, canonical, validationErr := validateLedger(proposed, s.Goal, source, policy.MaxSummaryTokens, policy.HybridReflection)
			if validationErr != nil {
				lastFailure = validationErr.Error()
				entry["accepted"] = false
				entry["reason"] = lastFailure
				recordSummaryAttempt(s, entry, proposed, historical, flattenMessages(rawGroups), canonicalSummary(s), false)
				continue
			}
			oldLedger := s.CompactedContext
			oldLedgerState := s.CompactionLedger
			s.CompactedContext = &canonical
			s.CompactionLedger = &ledger
			candidateMessages := append([]Message{s.Memory[0]}, flattenMessages(rawGroups)...)
			candidate := *s
			candidate.Memory = candidateMessages
			stagedPayload, stageErr := (OpenAIClient{Config: e.Config}).decisionPayload(e.payload(&candidate))
			if stageErr != nil {
				s.CompactedContext, s.CompactionLedger = oldLedger, oldLedgerState
				return stageErr
			}
			candidateBudget, err = estimatePayload(stagedPayload, policy)
			if err != nil {
				s.CompactedContext, s.CompactionLedger = oldLedger, oldLedgerState
				return err
			}
			oldEstimate := budget.EstimatedInputTokens
			reduction := oldEstimate - candidateBudget.EstimatedInputTokens
			if reduction <= 0 || !candidateBudget.Fits {
				s.CompactedContext = oldLedger
				s.CompactionLedger = oldLedgerState
				lastFailure = "candidate ledger did not reduce the complete request below context budget"
				entry["accepted"] = false
				entry["reason"] = lastFailure
				entry["after_estimated_tokens"] = candidateBudget.EstimatedInputTokens
				recordSummaryAttempt(s, entry, proposed, historical, flattenMessages(rawGroups), canonicalSummary(s), false)
				continue
			}
			minimumReduction := policy.OptionalCompactionMinReductionTokens
			reductionRatio := float64(reduction) / float64(maxInt(1, oldEstimate))
			if !hardPressure && (minimumReduction > 0 && reduction < minimumReduction || policy.OptionalCompactionMinReductionRatio > 0 && reductionRatio < policy.OptionalCompactionMinReductionRatio) {
				s.CompactedContext = oldLedger
				s.CompactionLedger = oldLedgerState
				lastFailure = "candidate reduction is below configured optional minimum"
				entry["accepted"] = false
				entry["reason"] = lastFailure
				recordSummaryAttempt(s, entry, proposed, historical, flattenMessages(rawGroups), canonicalSummary(s), false)
				continue
			}
			accepted = true
			entry["accepted"] = true
			entry["after_estimated_tokens"] = candidateBudget.EstimatedInputTokens
			entry["reduction_tokens"] = reduction
			recordSummaryAttempt(s, entry, proposed, historical, flattenMessages(rawGroups), canonical, true)
			prov := CompactionProvenance{SourceSHA256: hash, SourceIndexes: sourceIndexes, RetainedIndexes: retainedIndexes, Accepted: true, BeforeEstimate: before, AfterEstimate: candidateBudget.EstimatedInputTokens, Reduction: reduction, Reason: reason}
			if oldLedgerState != nil {
				prov.PreviousLedgerSHA = oldLedgerState.SourceSHA256
			}
			s.CompactionProvenance = append(s.CompactionProvenance, prov)
			RecordDiagnostic(s, "compaction_decisions", map[string]any{"source_sha256": hash, "accepted": true, "reason": reason, "before_estimated_tokens": before, "after_estimated_tokens": candidateBudget.EstimatedInputTokens, "reduction_tokens": reduction, "reduction_ratio": reductionRatio})
			s.Memory = candidateMessages
			s.RequestBudget = &candidateBudget
			s.LastCompactionEstimatedTokens = before
			RecordDiagnostic(s, "compaction_events", canonical)
			s.LastCompactionStep = s.StepCount
			if err := out(emit, map[string]any{"type": "interim_result", "step": "compaction", "data": map[string]any{"compaction_count": s.DiagnosticCounts["compaction_events"], "source_sha256": hash, "reduction_tokens": reduction, "request_budget": candidateBudget}}); err != nil {
				return err
			}
			break
		}
		if !accepted {
			entry := map[string]any{"pass": pass + 1, "source_sha256": hash, "accepted": false, "reason": lastFailure, "step": s.StepCount}
			RecordDiagnostic(s, "compaction_decisions", entry)
			s.CompactionProvenance = append(s.CompactionProvenance, CompactionProvenance{SourceSHA256: hash, SourceIndexes: sourceIndexes, RetainedIndexes: retainedIndexes, BeforeEstimate: before, Reason: lastFailure})
			if pass+1 >= maxPasses {
				break
			}
			// A rejected proposal never changes active history. A later pass may summarize
			// a smaller complete prefix only when a distinct source hash is produced.
			if len(oldGroups) > 1 {
				rawGroups = append(oldGroups[len(oldGroups)-1:], rawGroups...)
				oldGroups = oldGroups[:len(oldGroups)-1]
			} else {
				break
			}
		}
		if accepted {
			break
		}
	}
	// Rebuild and enforce the hard request boundary after any staged compaction.
	payload, err = (OpenAIClient{Config: e.Config}).decisionPayload(e.payload(s))
	if err != nil {
		return err
	}
	budget, err = estimatePayload(payload, policy)
	if err != nil {
		return err
	}
	s.RequestBudget = &budget
	if !budget.Fits {
		return budgetError(budget)
	}
	return nil
}

func flattenMessages(groups []historyGroup) []Message { m, _ := flattenGroups(groups); return m }
func budgetError(b RequestBudget) error {
	raw, _ := json.Marshal(b)
	return fmt.Errorf("context_budget_exceeded: request exceeds configured context budget; no model request was sent (%s)", raw)
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
