package agent

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func (e Engine) completionErrors(s *Session, answer string) []string {
	errors := []string{}
	if strings.TrimSpace(answer) == "" {
		errors = append(errors, "Return a nonempty final answer or valid tool call.")
	}
	if e.Config.Workflow != nil && !e.complete(s) {
		errors = append(errors, "Workflow is incomplete. Obtain the outstanding evidence before answering.\n"+e.workflowGuidance(s))
	}
	checks := e.Config.Completion
	for _, pattern := range checks.ForbiddenPatterns {
		re, err := regexp.Compile("(?i)(" + pattern + ")")
		if err == nil && re.MatchString(answer) {
			errors = append(errors, "Final answer includes forbidden diagnostic content; return only the configured deliverables.")
		}
	}
	if checks.Exact != nil && answer != *checks.Exact {
		errors = append(errors, "Final answer must equal the configured exact text without surrounding prose or whitespace.")
	}
	if checks.Pattern != nil {
		re, err := regexp.Compile("\\A(" + *checks.Pattern + ")\\z")
		if err != nil || !re.MatchString(answer) {
			errors = append(errors, "Final answer does not satisfy the configured presentation pattern.")
		}
	}
	if checks.JSONSchema != nil {
		value, err := strictJSON([]byte(answer))
		if err != nil {
			errors = append(errors, "Final answer must be bare valid JSON without prose or code fences.")
		} else if problems := validateArguments(value, checks.JSONSchema, "final_answer"); len(problems) > 0 {
			errors = append(errors, problems...)
		}
	}
	for i, check := range checks.ResultChecks {
		artifact, ok := s.CompletionArtifacts[strconv.Itoa(i)]
		if !ok {
			errors = append(errors, "Missing executed tool result for "+check.Label+"; obtain it using "+check.Tool+".")
			continue
		}
		if !checkAnswer(check, answer, artifact.Value) {
			errors = append(errors, "Final answer must include the verified deliverable for "+check.Label+":\n"+describeArtifact(check, artifact.Value))
		}
	}
	return errors
}

func (e Engine) completionInstructions(s *Session) string {
	checks := e.Config.Completion
	parts := []string{}
	if checks.Instructions != nil && *checks.Instructions != "" {
		parts = append(parts, *checks.Instructions)
	}
	if checks.Exact != nil {
		parts = append(parts, "Return exactly the literal text between BEGIN_LITERAL and END_LITERAL, with no markers or explanation.\nBEGIN_LITERAL\n"+*checks.Exact+"\nEND_LITERAL")
	}
	if checks.Pattern != nil {
		parts = append(parts, "Final answer must match the configured presentation pattern exactly; do not print validation syntax.")
	}
	if len(checks.ForbiddenPatterns) > 0 {
		parts = append(parts, "Do not include diagnostic or recovery internals in the final answer; the runtime applies configured forbidden-pattern checks.")
	}
	if checks.JSONSchema != nil {
		b, _ := json.Marshal(checks.JSONSchema)
		parts = append(parts, "Return only a bare JSON value satisfying this schema, without prose or code fences: "+string(b))
	}
	for _, check := range checks.ResultChecks {
		parts = append(parts, "Required final deliverable "+check.Label+" must be derived from the executed "+check.Tool+" result.")
	}
	if len(parts) == 0 {
		return ""
	}
	return "[FINAL ANSWER REQUIREMENTS]\n" + strings.Join(parts, "\n")
}

func (e Engine) workflowGuidance(s *Session) string {
	if s.Workflow == nil || s.Workflow.ActivePhase == nil {
		return "Obtain all required workflow evidence before answering."
	}
	p := e.phase(*s.Workflow.ActivePhase)
	if p == nil {
		return "Obtain all required workflow evidence before answering."
	}
	allowed := append([]string{}, p.AllowedTools...)
	if !p.AllowedToolsSet {
		for _, item := range p.Completion {
			allowed = appendUnique(allowed, item.Tool)
		}
	}
	sort.Strings(allowed)
	allowedText := strings.Join(allowed, ", ")
	if allowedText == "" {
		allowedText = "none"
	}
	missing := []string{}
	for i, item := range p.Completion {
		if !item.Required || has(s.Workflow.ObservedEvidence, key(p.ID, "completion", i)) {
			continue
		}
		line := item.Description + ". Call " + item.Tool + "; required argument constraints="
		arguments, _ := json.Marshal(item.Arguments)
		line += string(arguments) + "; expected result constraints="
		result, _ := json.Marshal(item.Result)
		line += string(result) + "; required output text="
		content, _ := json.Marshal(item.ContentContains)
		line += string(content)
		line += ". Supply any additional schema-required arguments. Configured diagnostic errors count as evidence."
		missing = append(missing, line)
	}
	if len(missing) == 0 {
		return "[WORKFLOW STATE]\nCurrent authoritative phase: " + p.Name + " (" + p.ID + ").\nAllowed tool names: " + allowedText + ".\nPhase instruction: " + p.Instruction + "\nAll required evidence is observed; follow the configured transition before returning a final answer."
	}
	return "[WORKFLOW STATE]\nThis is the current authoritative execution state. Phase IDs are workflow labels, not function names.\nCurrent phase: " + p.Name + " (" + p.ID + ").\nAllowed tool names: " + allowedText + ".\nPhase instruction: " + p.Instruction + "\nDo not provide progress prose or a final answer while required evidence is incomplete.\nChoose a call that supplies outstanding evidence; repeating an already observed outcome does not satisfy a different argument constraint.\nOutstanding required evidence:\n- " + strings.Join(missing, "\n- ")
}

func (e Engine) captureArtifacts(s *Session, call ToolCall, output string) {
	if strings.HasPrefix(output, "ERROR:") || strings.HasPrefix(output, "AssertionError:") {
		return
	}
	for i, check := range e.Config.Completion.ResultChecks {
		if check.Tool != call.Name || !contains(call.Arguments, check.Arguments) {
			continue
		}
		value, err := extractResult(check, output)
		if err != nil {
			continue
		}
		if len(check.Result) > 0 {
			parsed, err := strictJSON([]byte(output))
			if err != nil || !contains(parsed, check.Result) {
				continue
			}
		}
		if s.CompletionArtifacts == nil {
			s.CompletionArtifacts = map[string]CompletionArtifact{}
		}
		s.CompletionArtifacts[strconv.Itoa(i)] = CompletionArtifact{Tool: call.Name, Arguments: call.Arguments, ToolCallID: call.ID, Step: s.StepCount, Value: value}
	}
}

func (e Engine) bindingError(s *Session, name string, args map[string]any) string {
	for _, binding := range e.Config.Execution.ArgumentBindings {
		if binding.Tool != name {
			continue
		}
		index := -1
		for i, check := range e.Config.Completion.ResultChecks {
			if check.Label == binding.SourceLabel {
				index = i
				break
			}
		}
		artifact, ok := s.CompletionArtifacts[strconv.Itoa(index)]
		if !ok {
			return "Obtain executed result " + binding.SourceLabel + " before calling " + name + "."
		}
		value, ok := valueAtPath(artifact.Value, binding.Path)
		if !ok {
			return "Bound result path for " + binding.SourceLabel + " is unavailable."
		}
		if actual, present := args[binding.Argument]; present && !jsonEqual(actual, value) {
			b, _ := json.Marshal(value)
			return binding.Argument + " must equal its bound executed result: " + string(b) + "."
		}
	}
	return ""
}

func extractResult(check ResultCheck, output string) (any, error) {
	if check.Kind == "markdown_table" {
		return parseTable(output)
	}
	value, err := strictJSON([]byte(output))
	if err != nil {
		return nil, err
	}
	value, ok := valueAtPath(value, check.Path)
	if !ok {
		return nil, fmt.Errorf("result path is unavailable")
	}
	if check.Kind == "number" {
		if _, ok := schemaNumber(value); !ok {
			return nil, fmt.Errorf("result is not a finite number")
		}
	}
	return value, nil
}

func valueAtPath(value any, path []any) (any, bool) {
	for _, part := range path {
		switch p := part.(type) {
		case string:
			m, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			value, ok = m[p]
			if !ok {
				return nil, false
			}
		case int:
			a, ok := value.([]any)
			if !ok || p < 0 || p >= len(a) {
				return nil, false
			}
			value = a[p]
		case int64:
			a, ok := value.([]any)
			if !ok || p < 0 || p >= int64(len(a)) {
				return nil, false
			}
			value = a[p]
		default:
			return nil, false
		}
	}
	return value, true
}

func parseTable(text string) ([][]string, error) {
	rows := [][]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "|") {
			// Markdown tables permit either edge pipe to be omitted. Normalize
			// them independently so a separator such as `|---|---:` remains
			// part of the table instead of terminating parsing.
			if strings.HasPrefix(line, "|") {
				line = line[1:]
			}
			if strings.HasSuffix(line, "|") {
				line = line[:len(line)-1]
			}
			cells := strings.Split(line, "|")
			row := make([]string, len(cells))
			for i, cell := range cells {
				row[i] = strings.TrimSpace(cell)
			}
			separator := len(row) > 0
			for _, cell := range row {
				if !regexp.MustCompile(`^:?-{3,}:?$`).MatchString(cell) {
					separator = false
				}
			}
			if !separator {
				rows = append(rows, row)
			}
		} else if len(rows) > 0 {
			break
		}
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("table requires header and data rows")
	}
	for _, row := range rows {
		if len(row) != len(rows[0]) {
			return nil, fmt.Errorf("table row width mismatch")
		}
	}
	keys := map[string]bool{}
	for _, row := range rows[1:] {
		if keys[row[0]] {
			return nil, fmt.Errorf("duplicate table row key")
		}
		keys[row[0]] = true
	}
	return rows, nil
}

func checkAnswer(check ResultCheck, answer string, value any) bool {
	switch check.Kind {
	case "markdown_table":
		actual, err := parseTable(answer)
		if err != nil {
			return false
		}
		wanted, ok := tableValue(value)
		if !ok || len(actual[0]) != len(wanted[0]) {
			return false
		}
		for i := range actual[0] {
			if actual[0][i] != wanted[0][i] {
				return false
			}
		}
		gotRows, wantRows := map[string][]string{}, map[string][]string{}
		for _, row := range actual[1:] {
			gotRows[row[0]] = row[1:]
		}
		for _, row := range wanted[1:] {
			wantRows[row[0]] = row[1:]
		}
		if len(gotRows) != len(wantRows) {
			return false
		}
		for key, expected := range wantRows {
			got, ok := gotRows[key]
			if !ok || len(got) != len(expected) {
				return false
			}
			for i := range got {
				if !equivalentCell(got[i], expected[i]) {
					return false
				}
			}
		}
		return true
	case "json_value":
		actual, err := strictJSON([]byte(answer))
		if err != nil {
			return false
		}
		actual, ok := valueAtPath(actual, check.AnswerPath)
		return ok && jsonEqual(actual, value)
	case "number":
		pattern := regexp.MustCompile(regexp.QuoteMeta(check.Label) + `\s*\$?\s*[-+]?[0-9][0-9,]*(\.[0-9]+)?`)
		matches := pattern.FindAllStringIndex(answer, -1)
		if len(matches) != 1 {
			return false
		}
		end := matches[0][1]
		if end < len(answer) && (answer[end] == ',' || answer[end] == '.' || answer[end] >= '0' && answer[end] <= '9') {
			return false
		}
		matched := answer[matches[0][0]:matches[0][1]]
		labelEnd := strings.Index(matched, check.Label) + len(check.Label)
		number := strings.TrimSpace(matched[labelEnd:])
		number = strings.TrimSpace(strings.TrimPrefix(number, "$"))
		validNumber := regexp.MustCompile(`^[-+]?[0-9]+(\.[0-9]+)?$`).MatchString(number) ||
			regexp.MustCompile(`^[-+]?[0-9]{1,3}(,[0-9]{3})+(\.[0-9]+)?$`).MatchString(number)
		if !validNumber {
			return false
		}
		actual := strings.ReplaceAll(number, ",", "")
		return decimalEqual(actual, numberString(value))
	default:
		return false
	}
}

func equivalentCell(actual, expected string) bool {
	if regexp.MustCompile(`^[-+]?[0-9]+$`).MatchString(expected) && !regexp.MustCompile(`^[-+]?[0-9]+$`).MatchString(actual) {
		return false
	}
	if decimalEqual(actual, expected) {
		return true
	}
	return actual == expected
}

func numberString(value any) string {
	switch n := value.(type) {
	case json.Number:
		return n.String()
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.FormatInt(n, 10)
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	default:
		return ""
	}
}

func decimalEqual(a, b string) bool {
	x, ok := new(big.Float).SetPrec(256).SetString(a)
	if !ok {
		return false
	}
	y, ok := new(big.Float).SetPrec(256).SetString(b)
	return ok && x.Cmp(y) == 0
}

func describeArtifact(check ResultCheck, value any) string {
	switch check.Kind {
	case "number":
		return check.Label + " " + numberString(value)
	case "json_value":
		b, _ := json.Marshal(value)
		return "Expected JSON value at " + fmt.Sprint(check.AnswerPath) + ": " + string(b)
	case "markdown_table":
		rows, _ := tableValue(value)
		if len(rows) == 0 {
			return ""
		}
		separator := make([]string, len(rows[0]))
		for i := range separator {
			separator[i] = "---"
		}
		lines := []string{"| " + strings.Join(rows[0], " | ") + " |", "| " + strings.Join(separator, " | ") + " |"}
		for _, row := range rows[1:] {
			lines = append(lines, "| "+strings.Join(row, " | ")+" |")
		}
		return strings.Join(lines, "\n")
	default:
		return ""
	}
}

func tableValue(value any) ([][]string, bool) {
	switch rows := value.(type) {
	case [][]string:
		return rows, true
	case []any:
		out := make([][]string, len(rows))
		for i, rowValue := range rows {
			row, ok := rowValue.([]any)
			if !ok {
				return nil, false
			}
			out[i] = make([]string, len(row))
			for j, cell := range row {
				text, ok := cell.(string)
				if !ok {
					return nil, false
				}
				out[i][j] = text
			}
		}
		return out, true
	default:
		return nil, false
	}
}
