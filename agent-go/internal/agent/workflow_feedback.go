package agent

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func (e Engine) boundArgument(s *Session, binding Binding) (any, bool) {
	for i, check := range e.Config.Completion.ResultChecks {
		if check.Label == binding.SourceLabel {
			if artifact, ok := s.CompletionArtifacts[strconv.Itoa(i)]; ok {
				return valueAtPath(artifact.Value, binding.Path)
			}
		}
	}
	return nil, false
}

func (e Engine) argumentFeedback(s *Session, name string, arguments map[string]any) string {
	if e.Config.Workflow != nil && e.complete(s) {
		return "All workflow phases are complete. Do not call tools; return the final answer now."
	}
	if e.Config.Execution.ToolPolicy == "none" {
		return "Do not call any function; this task has no tool access."
	}
	var definition *ToolDefinition
	for i := range e.Config.Tools {
		if e.Config.Tools[i].Name == name {
			definition = &e.Config.Tools[i]
			break
		}
	}
	if definition == nil {
		return "Use an advertised function name, not a workflow phase ID."
	}
	if !e.allowed(s, name) {
		return "Choose a function that supplies the outstanding active-phase evidence.\n" + e.instruction(s)
	}
	schema := definition.Parameters
	example := map[string]any{}
	properties, _ := schema["properties"].(map[string]any)
	required, _ := schema["required"].([]any)
	for key, value := range properties {
		rules, _ := value.(map[string]any)
		if actual, present := arguments[key]; present && len(validateArguments(actual, rules, "$")) == 0 {
			example[key] = actual
			continue
		}
		needed := false
		for _, field := range required {
			needed = needed || field == key
		}
		if !needed {
			continue
		}
		if options, ok := rules["enum"].([]any); ok && len(options) > 0 {
			example[key] = options[0]
			continue
		}
		switch rules["type"] {
		case "boolean":
			example[key] = true
		case "integer", "number":
			example[key] = 1
			if minimum, ok := rules["minimum"]; ok {
				example[key] = minimum
			}
		case "string":
			example[key] = "value"
		case "object":
			example[key] = map[string]any{}
		case "array":
			example[key] = []any{}
		default:
			example[key] = nil
		}
	}
	requirement := ""
	if s.Workflow != nil && s.Workflow.ActivePhase != nil {
		phase := e.phase(*s.Workflow.ActivePhase)
		for i, evidence := range phase.Completion {
			if evidence.Required && evidence.Tool == name && !has(s.Workflow.ObservedEvidence, key(phase.ID, "completion", i)) {
				for argument, value := range evidence.Arguments {
					example[argument] = value
				}
				requirement = " Next required evidence: " + evidence.Description + "."
				break
			}
		}
	}
	notes := []string{}
	for _, binding := range e.Config.Execution.ArgumentBindings {
		if binding.Tool != name {
			continue
		}
		if value, known := e.boundArgument(s, binding); known {
			example[binding.Argument] = value
			notes = append(notes, binding.Argument+" comes from executed result "+pythonString(binding.SourceLabel)+".")
		} else {
			delete(example, binding.Argument)
			notes = append(notes, "Obtain result "+pythonString(binding.SourceLabel)+" first; no placeholder is valid for "+binding.Argument+".")
		}
	}
	return "Argument schema: " + promptJSON(schema) +
		"\nArgument-shape example (replace placeholder values with evidence): " + promptJSON(example) +
		". JSON booleans are true/false without quotes." + requirement + " " + strings.Join(notes, " ")
}

func (e Engine) actionErrors(s *Session, d Decision) []string {
	issues := []string{}
	var definition *ToolDefinition
	names := []string{}
	for i := range e.Config.Tools {
		names = append(names, e.Config.Tools[i].Name)
		if e.Config.Tools[i].Name == d.Name {
			definition = &e.Config.Tools[i]
		}
	}
	if definition == nil {
		sort.Strings(names)
		issues = append(issues, "Tool '"+d.Name+"' is not configured. Available tools: "+strings.Join(names, ", ")+".")
	} else if d.Arguments == nil {
		issues = append(issues, "Tool arguments must be a JSON object.")
	} else {
		issues = append(issues, validateArguments(d.Arguments, definition.Parameters, "$")...)
		if issue := e.bindingError(s, d.Name, d.Arguments); issue != "" {
			issues = append(issues, issue)
		}
	}
	if e.Config.Execution.ToolPolicy == "none" {
		issues = append(issues, "Tools are disabled by this session policy. Return a final answer directly.")
	}
	if e.Config.Workflow != nil && e.complete(s) {
		issues = append(issues, "All workflow phases are complete. Tool calls are no longer allowed; return the final answer now.")
	} else if !e.allowed(s, d.Name) {
		allowed := e.decisionPolicy(s).AllowedTools
		sort.Strings(allowed)
		issues = append(issues, "Active workflow phase permits only: "+strings.Join(allowed, ", ")+". Complete that phase first.")
	}
	if len(issues) > 0 {
		issues = append(issues, e.argumentFeedback(s, d.Name, d.Arguments))
	}
	return issues
}

func (e Engine) rejectResponse(s *Session, emit Emit, code, message string, appendFeedback bool) error {
	ArchiveRecord(s, "decision_rejected", map[string]any{"code": code, "message": message})
	s.ConsecutiveInvalidResponses++
	s.LastError = map[string]any{"code": code, "message": message, "recoverable": true}
	if appendFeedback {
		AppendMessage(s, Message{Role: "user", Content: str("Runtime correction: " + message)})
	}
	if err := out(emit, map[string]any{"type": "interim_result", "step": "response_rejected", "data": map[string]any{
		"code": code, "message": message, "recoverable": true, "consecutive_invalid_responses": s.ConsecutiveInvalidResponses,
	}}); err != nil {
		return err
	}
	limit := e.Config.Execution.MaxConsecutiveInvalidResponses
	if limit < 1 {
		limit = 3
	}
	if s.ConsecutiveInvalidResponses >= limit {
		failure := fmt.Sprintf("Consecutive invalid-response limit reached. %s Resume to retry.", message)
		s.Status, s.RunStatus, s.FinalAnswer = "failed", "failed", &failure
		s.LastError = map[string]any{"code": "invalid_response_limit", "message": failure, "recoverable": true}
		return out(emit, map[string]any{"type": "error", "code": "invalid_response_limit", "message": failure, "recoverable": true})
	}
	return nil
}
