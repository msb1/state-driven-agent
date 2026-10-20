package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type Ledger struct {
	Goal         string   `json:"goal"`
	ModelClaims  []string `json:"model_claims"`
	DeadEnds     []string `json:"dead_ends"`
	Hypothesis   string   `json:"hypothesis"`
	SourceSHA256 string   `json:"source_sha256"`
	Verification string   `json:"verification"`
}

type RequestBudget struct {
	EstimatedInputTokens int    `json:"estimated_input_tokens"`
	ReservedOutputTokens int    `json:"reserved_output_tokens"`
	SafetyMarginTokens   int    `json:"safety_margin_tokens"`
	ContextWindowTokens  int    `json:"context_window_tokens"`
	Fits                 bool   `json:"fits"`
	Estimator            string `json:"estimator"`
}

type CompactionProvenance struct {
	SourceSHA256      string `json:"source_sha256"`
	SourceIndexes     []int  `json:"source_indexes"`
	RetainedIndexes   []int  `json:"retained_indexes"`
	PreviousLedgerSHA string `json:"previous_ledger_sha256,omitempty"`
	Accepted          bool   `json:"accepted"`
	BeforeEstimate    int    `json:"before_estimated_tokens"`
	AfterEstimate     int    `json:"after_estimated_tokens,omitempty"`
	Reduction         int    `json:"reduction_tokens,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

type historyGroup struct {
	messages []Message
	indexes  []int
}

func messageWireMap(m Message) map[string]any {
	out := map[string]any{"role": m.Role}
	if m.Content != nil {
		out["content"] = *m.Content
	}
	if m.Name != nil {
		out["name"] = *m.Name
	}
	if m.ToolCallID != nil {
		out["tool_call_id"] = *m.ToolCallID
	}
	if !isPythonFalsy(m.ToolCalls) {
		out["tool_calls"] = m.ToolCalls
	}
	return out
}

func interactionGroups(messages []Message) ([]historyGroup, error) {
	groups := []historyGroup{}
	for i := 0; i < len(messages); {
		m := messages[i]
		if m.Role == "tool" {
			return nil, fmt.Errorf("context_integrity_error: orphaned tool result at history index %d", i)
		}
		if m.Role != "assistant" || isPythonFalsy(m.ToolCalls) {
			groups = append(groups, historyGroup{messages: []Message{m}, indexes: []int{i}})
			i++
			continue
		}
		encoded, err := json.Marshal(m.ToolCalls)
		if err != nil {
			return nil, fmt.Errorf("context_integrity_error: malformed tool-call batch")
		}
		var calls []map[string]any
		if err := json.Unmarshal(encoded, &calls); err != nil || len(calls) == 0 {
			return nil, fmt.Errorf("context_integrity_error: malformed or empty tool-call batch at history index %d", i)
		}
		ids := make([]string, len(calls))
		seen := map[string]bool{}
		for n, call := range calls {
			id, ok := call["id"].(string)
			if !ok || id == "" || seen[id] {
				return nil, fmt.Errorf("context_integrity_error: missing or duplicate tool-call ID at history index %d", i)
			}
			seen[id], ids[n] = true, id
		}
		if i+len(ids) >= len(messages) {
			return nil, fmt.Errorf("context_integrity_error: incomplete tool-call batch at history index %d", i)
		}
		results := map[string]Message{}
		for j := 1; j <= len(ids); j++ {
			r := messages[i+j]
			if r.Role != "tool" || r.ToolCallID == nil || *r.ToolCallID == "" {
				return nil, fmt.Errorf("context_integrity_error: incomplete tool-call result group at history index %d", i)
			}
			id := *r.ToolCallID
			if _, duplicate := results[id]; duplicate {
				return nil, fmt.Errorf("context_integrity_error: duplicate tool result ID %q", id)
			}
			results[id] = r
		}
		if len(results) != len(ids) {
			return nil, fmt.Errorf("context_integrity_error: tool-call results do not correlate at history index %d", i)
		}
		g := historyGroup{messages: []Message{m}, indexes: []int{i}}
		for _, id := range ids {
			if _, ok := results[id]; !ok {
				return nil, fmt.Errorf("context_integrity_error: missing result for tool call %q", id)
			}
		}
		for j := 1; j <= len(ids); j++ {
			g.messages = append(g.messages, messages[i+j])
			g.indexes = append(g.indexes, i+j)
		}
		groups = append(groups, g)
		i += 1 + len(ids)
	}
	return groups, nil
}

func splitHistory(messages []Message, minimumMessages int) ([]historyGroup, []historyGroup, error) {
	groups, err := interactionGroups(messages)
	if err != nil {
		return nil, nil, err
	}
	boundary, count := len(groups), 0
	for boundary > 0 && count < minimumMessages {
		boundary--
		count += len(groups[boundary].messages)
	}
	return groups[:boundary], groups[boundary:], nil
}

func flattenGroups(groups []historyGroup) ([]Message, []int) {
	var messages []Message
	var indexes []int
	for _, group := range groups {
		messages = append(messages, group.messages...)
		indexes = append(indexes, group.indexes...)
	}
	return messages, indexes
}

func compactJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

func estimatePayload(payload map[string]any, memory Memory) (RequestBudget, error) {
	b, err := compactJSON(payload)
	if err != nil {
		return RequestBudget{}, err
	}
	messages := 0
	if xs, ok := payload["messages"].([]WireMessage); ok {
		messages = len(xs)
	}
	if xs, ok := payload["messages"].([]Message); ok {
		messages = len(xs)
	}
	if xs, ok := payload["messages"].([]any); ok {
		messages = len(xs)
	}
	reserved := memory.ReservedOutputTokens
	if cap, ok := payload["max_tokens"].(int); ok && cap > reserved {
		reserved = cap
	}
	if cap, ok := payload["max_tokens"].(float64); ok && int(cap) > reserved {
		reserved = int(cap)
	}
	estimate := len(b) + 32*messages
	return RequestBudget{EstimatedInputTokens: estimate, ReservedOutputTokens: reserved, SafetyMarginTokens: memory.SafetyMarginTokens, ContextWindowTokens: memory.ContextWindowTokens, Fits: estimate+reserved+memory.SafetyMarginTokens <= memory.ContextWindowTokens, Estimator: "compact UTF-8 JSON bytes + 32 per message; conservative estimate, not provider usage"}, nil
}

func sourceText(goal string, historical []Message, previous *string) (string, error) {
	prev := "none"
	if previous != nil && *previous != "" {
		prev = *previous
	}
	wire := make([]map[string]any, len(historical))
	for i, message := range historical {
		wire[i] = messageWireMap(message)
	}
	return "Goal: " + goal + "\nPrevious ledger: " + prev + "\nHistorical prefix:\n" + pythonStyleJSON(wire), nil
}

func ledgerNumberSet(text string) map[string]bool {
	re := regexp.MustCompile(`[-+]?[0-9]+(,[0-9]{3})*(\.[0-9]+)?([eE][-+]?[0-9]+)?`)
	set := map[string]bool{}
	for _, loc := range re.FindAllStringIndex(text, -1) {
		if loc[0] > 0 {
			c := text[loc[0]-1]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == ',' || c == '+' || c == '-' {
				continue
			}
		}
		if loc[1] < len(text) {
			c := text[loc[1]]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == ',' {
				continue
			}
		}
		match := text[loc[0]:loc[1]]
		v, ok := exactDecimal(match)
		if ok {
			set[v] = true
		}
	}
	return set
}

func exactDecimal(s string) (string, bool) {
	s = strings.ReplaceAll(s, ",", "")
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	parts := strings.SplitN(strings.ToLower(s), "e", 2)
	exponent := 0
	if len(parts) == 2 {
		var err error
		_, err = fmt.Sscanf(parts[1], "%d", &exponent)
		if err != nil || exponent > 10000 || exponent < -10000 {
			return "", false
		}
	}
	base := strings.SplitN(parts[0], ".", 2)
	whole, frac := base[0], ""
	if len(base) == 2 {
		frac = base[1]
	}
	digits := strings.TrimLeft(whole+frac, "0")
	if digits == "" {
		return "0", true
	}
	scale := len(frac) - exponent
	for scale > 0 && strings.HasSuffix(digits, "0") {
		digits = strings.TrimSuffix(digits, "0")
		scale--
	}
	for scale < 0 {
		digits += "0"
		scale++
	}
	value := digits
	if scale > 0 {
		if scale >= len(digits) {
			value = "0." + strings.Repeat("0", scale-len(digits)) + digits
		} else {
			value = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
		}
	}
	if negative {
		value = "-" + value
	}
	return value, true
}

func validateLedger(proposal, goal, source string, maxBytes int, hybrid bool) (Ledger, string, error) {
	var sections []string
	if strings.TrimSpace(proposal) == "" {
		return Ledger{}, "", fmt.Errorf("summary is empty")
	}
	if len([]byte(proposal)) > maxBytes {
		return Ledger{}, "", fmt.Errorf("summary exceeds ledger budget")
	}
	if hybrid {
		if strings.Contains(proposal, "<!") || strings.Contains(proposal, "<?") {
			return Ledger{}, "", fmt.Errorf("XML declarations, entities, and comments are forbidden")
		}
		decoder := xml.NewDecoder(strings.NewReader(strings.TrimSpace(proposal)))
		decoder.Strict = true
		rootSeen, rootDone := false, false
		want := []string{"USER_GOAL", "GLOBAL_LESSON_LEDGER", "DEAD_ENDS", "CURRENT_LOCAL_PIVOT"}
		sectionIndex := -1
		var texts []strings.Builder
		stack := []string{}
		for {
			tok, err := decoder.Token()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				return Ledger{}, "", fmt.Errorf("summary XML is malformed")
			}
			switch t := tok.(type) {
			case xml.StartElement:
				if !rootSeen {
					if t.Name.Local != "COMPACTED_STATE" || t.Name.Space != "" || len(t.Attr) != 0 {
						return Ledger{}, "", fmt.Errorf("summary root must be COMPACTED_STATE without attributes")
					}
					rootSeen = true
					stack = append(stack, t.Name.Local)
					continue
				}
				if rootDone || len(stack) != 1 || sectionIndex+1 >= len(want) || t.Name.Local != want[sectionIndex+1] || t.Name.Space != "" || len(t.Attr) != 0 {
					return Ledger{}, "", fmt.Errorf("summary sections must be exact, ordered, and attribute-free")
				}
				sectionIndex++
				texts = append(texts, strings.Builder{})
				stack = append(stack, t.Name.Local)
			case xml.CharData:
				if !rootSeen || rootDone {
					if strings.TrimSpace(string(t)) != "" {
						return Ledger{}, "", fmt.Errorf("extra prose outside summary root")
					}
				} else if sectionIndex < 0 {
					if strings.TrimSpace(string(t)) != "" {
						return Ledger{}, "", fmt.Errorf("extra content inside summary root")
					}
				} else {
					texts[sectionIndex].Write([]byte(t))
				}
			case xml.EndElement:
				if len(stack) == 0 || stack[len(stack)-1] != t.Name.Local || t.Name.Space != "" {
					return Ledger{}, "", fmt.Errorf("summary XML nesting is invalid")
				}
				stack = stack[:len(stack)-1]
				if t.Name.Local == "COMPACTED_STATE" {
					rootDone = true
				}
			case xml.Comment, xml.Directive, xml.ProcInst:
				return Ledger{}, "", fmt.Errorf("comments, directives, and processing instructions are forbidden")
			}
		}
		if !rootSeen || !rootDone || len(stack) != 0 || sectionIndex != len(want)-1 {
			return Ledger{}, "", fmt.Errorf("summary must contain exactly four ordered sections")
		}
		sections = make([]string, len(texts))
		for i := range texts {
			sections[i] = strings.TrimSpace(texts[i].String())
		}
	} else {
		names := []string{"Core Objective", "Universal Truths Discovered", "Dead Ends", "Current Local Pivot"}
		re := regexp.MustCompile(`(?m)^- (Core Objective|Universal Truths Discovered|Dead Ends|Current Local Pivot):[ \t]*`)
		matches := re.FindAllStringSubmatchIndex(proposal, -1)
		if len(matches) != 4 || strings.TrimSpace(proposal[:matches[0][0]]) != "" {
			return Ledger{}, "", fmt.Errorf("summary must contain the four required ordered headings")
		}
		for i, m := range matches {
			if m[2] < 0 || proposal[m[2]:m[3]] != names[i] {
				return Ledger{}, "", fmt.Errorf("summary headings are missing, duplicated, or out of order")
			}
			end := len(proposal)
			if i+1 < len(matches) {
				end = matches[i+1][0]
			}
			sections = append(sections, strings.TrimSpace(proposal[m[1]:end]))
		}
	}
	if len(sections) != 4 || sections[0] != strings.TrimSpace(goal) {
		return Ledger{}, "", fmt.Errorf("summary must preserve the original goal exactly")
	}
	for _, section := range sections {
		if section == "" {
			return Ledger{}, "", fmt.Errorf("all summary sections must be populated")
		}
	}
	sourceNumbers := ledgerNumberSet(source)
	for number := range ledgerNumberSet(strings.Join(sections, "\n")) {
		if !sourceNumbers[number] {
			return Ledger{}, "", fmt.Errorf("summary introduces numeric claim %s absent from source", number)
		}
	}
	hash := sha256.Sum256([]byte(source))
	ledger := Ledger{Goal: goal, ModelClaims: strings.Split(sections[1], "\n"), DeadEnds: strings.Split(sections[2], "\n"), Hypothesis: sections[3], SourceSHA256: fmt.Sprintf("%x", hash[:]), Verification: "unverified_model_summary"}
	canonical := renderLedger(ledger, hybrid)
	if len([]byte(canonical)) > maxBytes {
		return Ledger{}, "", fmt.Errorf("canonical summary exceeds configured byte limit")
	}
	return ledger, canonical, nil
}

func renderLedger(l Ledger, hybrid bool) string {
	claims := strings.Join(l.ModelClaims, "\n")
	dead := strings.Join(l.DeadEnds, "\n")
	if strings.TrimSpace(claims) == "" {
		claims = "- None."
	}
	if strings.TrimSpace(dead) == "" {
		dead = "- None."
	}
	if hybrid {
		return "<COMPACTED_STATE>\n<USER_GOAL>\n" + xmlEscape(strings.TrimSpace(l.Goal)) + "\n</USER_GOAL>\n<GLOBAL_LESSON_LEDGER>\n" + xmlEscape(claims) + "\n</GLOBAL_LESSON_LEDGER>\n<DEAD_ENDS>\n" + xmlEscape(dead) + "\n</DEAD_ENDS>\n<CURRENT_LOCAL_PIVOT>\n" + xmlEscape(l.Hypothesis) + "\n</CURRENT_LOCAL_PIVOT>\n</COMPACTED_STATE>"
	}
	return "- Core Objective:\n" + strings.TrimSpace(l.Goal) + "\n- Universal Truths Discovered:\n" + claims + "\n- Dead Ends:\n" + dead + "\n- Current Local Pivot:\n" + l.Hypothesis
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

func sortedStrings(m map[string]bool) []string {
	r := make([]string, 0, len(m))
	for x := range m {
		r = append(r, x)
	}
	sort.Strings(r)
	return r
}
