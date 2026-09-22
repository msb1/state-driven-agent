// Package agent contains the language-independent state-machine semantics.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Message struct {
	Role       string  `json:"role"`
	Content    *string `json:"content,omitempty"`
	Name       *string `json:"name,omitempty"`
	ToolCallID *string `json:"tool_call_id,omitempty"`
	ToolCalls  any     `json:"tool_calls,omitempty"`
}
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
type Session struct {
	ID               string         `json:"id"`
	Goal             string         `json:"goal"`
	ConfigRef        string         `json:"config_ref,omitempty"`
	ConfigSHA256     string         `json:"config_sha256,omitempty"`
	Memory           []Message      `json:"memory"`
	CompactedContext *string        `json:"compacted_context,omitempty"`
	CompactionEvents []string       `json:"compaction_events"`
	Workflow         *WorkflowState `json:"workflow,omitempty"`
	StepCount        int            `json:"step_count"`
	RunCount         int            `json:"run_count"`
	Status           string         `json:"status"`
	FinalAnswer      *string        `json:"final_answer,omitempty"`
}
type Evidence struct {
	Description, Tool string
	Required          bool
	Arguments         map[string]any
	Result            map[string]any
	ContentContains   []string `yaml:"content_contains" json:"content_contains"`
}
type Edge struct {
	To, When string
	Evidence []Evidence
}
type Phase struct {
	ID, Name, Instruction string
	AllowedTools          []string `yaml:"allowed_tools" json:"allowed_tools"`
	Completion            []Evidence
	Transitions           []Edge
}
type Memory struct {
	MaxTokens        int  `yaml:"max_tokens" json:"max_tokens"`
	MaxSteps         int  `yaml:"max_steps" json:"max_steps"`
	RawTurnsToKeep   int  `yaml:"raw_turns_to_keep" json:"raw_turns_to_keep"`
	HybridReflection bool `yaml:"hybrid_reflection" json:"hybrid_reflection"`
}
type WorkflowConfig struct {
	EntryPhase string  `yaml:"entry_phase" json:"entry_phase"`
	Phases     []Phase `json:"phases"`
}
type Config struct {
	Name         string           `yaml:"name" json:"name"`
	SystemPrompt string           `yaml:"system_prompt" json:"system_prompt"`
	Model        string           `yaml:"model" json:"model"`
	LLM          LLM              `yaml:"llm" json:"llm"`
	Memory       Memory           `yaml:"memory" json:"memory"`
	Output       Output           `yaml:"output" json:"output"`
	Tools        []ToolDefinition `yaml:"tools" json:"tools"`
	Workflow     *WorkflowConfig  `yaml:"workflow" json:"workflow"`
}
type Decision struct {
	Final             bool
	Content, ID, Name string
	Arguments         map[string]any
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

func (e Engine) Create(id, goal, ref, hash string) Session {
	s := Session{ID: id, Goal: goal, ConfigRef: ref, ConfigSHA256: hash, Memory: []Message{{Role: "user", Content: &goal}}, CompactionEvents: []string{}, Status: "active"}
	if e.Config.Workflow != nil {
		p := e.Config.Workflow.EntryPhase
		s.Workflow = &WorkflowState{ActivePhase: &p, CompletedPhases: []string{}, ObservedEvidence: []string{}, Transitions: []Transition{}}
	}
	return s
}
func (e Engine) Run(ctx context.Context, s *Session, max int, emit Emit) error {
	if max <= 0 {
		max = e.Config.Memory.MaxSteps
	}
	limit := s.StepCount + max
	s.RunCount++
	if err := out(emit, map[string]any{"type": "status", "message": "Agent run started.", "run_count": s.RunCount}); err != nil {
		return err
	}
	for s.Status == "active" && s.StepCount < limit {
		if x := e.advance(s); x != nil {
			if err := out(emit, map[string]any{"type": "interim_result", "step": "phase_completed", "phase_id": x.FromPhase, "next_phase": x.ToPhase, "reason": x.Reason}); err != nil {
				return err
			}
		}
		if err := e.compact(ctx, s, emit); err != nil {
			return err
		}
		if err := out(emit, map[string]any{"type": "status", "message": "Agent deciding next action.", "step_count": s.StepCount}); err != nil {
			return err
		}
		d, err := e.Client.Decide(ctx, e.payload(s))
		if err != nil {
			return err
		}
		s.StepCount++
		if d.Final {
			s.Memory = append(s.Memory, Message{Role: "assistant", Content: &d.Content})
			if e.complete(s) {
				s.Status = "complete"
				s.FinalAnswer = &d.Content
				if err := out(emit, map[string]any{"type": "final_result", "answer": d.Content}); err != nil {
					return err
				}
			}
			continue
		}
		args, _ := json.Marshal(d.Arguments)
		calls := []map[string]any{{"id": d.ID, "type": "function", "function": map[string]any{"name": d.Name, "arguments": string(args)}}}
		s.Memory = append(s.Memory, Message{Role: "assistant", ToolCalls: calls})
		result := e.call(ctx, s, d)
		s.Memory = append(s.Memory, Message{Role: "tool", Name: &d.Name, ToolCallID: &d.ID, Content: &result})
		e.record(s, d.Name, d.Arguments, result)
		if err := out(emit, map[string]any{"type": "interim_result", "step": "tool_completed", "data": map[string]any{"tool": d.Name, "output": result}}); err != nil {
			return err
		}
	}
	if s.Status == "active" && s.StepCount >= limit {
		x := fmt.Sprintf("Step limit (%d) reached without a final answer. Resume with another run request.", max)
		s.Status = "failed"
		s.FinalAnswer = &x
		return out(emit, map[string]any{"type": "error", "message": x, "recoverable": true})
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
	if s.CompactedContext != nil {
		p = append(p, Message{Role: "system", Content: str("[COMPACTED CONTEXT STATE]\n" + *s.CompactedContext)})
	}
	if w := e.instruction(s); w != "" {
		p = append(p, Message{Role: "system", Content: &w})
	}
	if len(s.Memory) > 1 {
		p = append(p, Message{Role: "system", Content: str("[RAW WORKING BUFFER — PRESERVE VERBATIM]")})
	}
	return append(p, s.Memory[1:]...)
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
	m := e.missing(s, p)
	return "[WORKFLOW STATE]\nActive phase: " + p.Name + " (" + p.ID + ").\nPhase instruction: " + p.Instruction + "\nDo not provide prose progress updates or a final answer while this phase is incomplete; make a tool call instead.\nOutstanding required evidence:\n- " + strings.Join(m, "\n- ")
}
func (e Engine) complete(s *Session) bool {
	return e.Config.Workflow == nil || (s.Workflow != nil && s.Workflow.ActivePhase == nil)
}
func (e Engine) allowed(s *Session, name string) bool {
	if s.Workflow == nil || s.Workflow.ActivePhase == nil {
		return true
	}
	p := e.phase(*s.Workflow.ActivePhase)
	a := p.AllowedTools
	// An omitted allow-list falls back to completion tools, as in the Python
	// implementation. An explicitly empty list deliberately allows no tools.
	if a == nil {
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
func (e Engine) call(ctx context.Context, s *Session, d Decision) string {
	if !e.allowed(s, d.Name) {
		return fmt.Sprintf("ERROR: Workflow phase '%s' cannot execute tool '%s'. Complete the active phase first.", *s.Workflow.ActivePhase, d.Name)
	}
	f := e.Tools[d.Name]
	if f == nil {
		return "ERROR: unknown tool '" + d.Name + "'."
	}
	x, err := f(ctx, d.Arguments)
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return x
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
		var v any
		if json.Unmarshal([]byte(out), &v) != nil || !contains(v, x.Result) {
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
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}
func (e Engine) compact(ctx context.Context, s *Session, emit Emit) error {
	tokens := 0
	for _, m := range s.Memory {
		if m.Content != nil {
			tokens += len(*m.Content)/4 + 1
		}
	}
	k := e.Config.Memory.RawTurnsToKeep
	due := tokens >= e.Config.Memory.MaxTokens || s.StepCount >= e.Config.Memory.MaxSteps
	if !due || len(s.Memory) <= k+1 {
		return nil
	}
	cut := len(s.Memory) - k
	v, err := e.Client.Compact(ctx, s.Goal, s.Memory[1:cut], s.CompactedContext, e.Config.Memory.HybridReflection)
	if err != nil {
		return err
	}
	s.CompactedContext = &v
	s.CompactionEvents = append(s.CompactionEvents, v)
	s.Memory = append([]Message{s.Memory[0]}, s.Memory[cut:]...)
	return out(emit, map[string]any{"type": "interim_result", "step": "compaction", "data": map[string]any{"compaction_count": len(s.CompactionEvents)}, "at": time.Now().UTC().Format(time.RFC3339Nano)})
}
