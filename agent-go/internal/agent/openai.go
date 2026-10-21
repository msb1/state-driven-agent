package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var generatedCallID atomic.Uint64

// OpenAIClient supports an OpenAI-compatible /chat/completions endpoint.
type OpenAIClient struct {
	Config      Config
	HTTP        *http.Client
	Diagnostics *ModelDiagnostics
}

type ModelRequestDiagnostic struct {
	Kind              string `json:"kind"`
	RequestBytes      int    `json:"serialized_request_bytes"`
	ElapsedMS         int64  `json:"elapsed_ms"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	Failure           string `json:"failure,omitempty"`
	ToolCallMode      string `json:"tool_call_mode,omitempty"`
	InputBudgetTokens int    `json:"input_budget_tokens,omitempty"`
	MaxOutputTokens   int    `json:"max_output_tokens,omitempty"`
	UsageKnown        bool   `json:"usage_known"`
	PromptTokens      *int   `json:"prompt_tokens,omitempty"`
	CompletionTokens  *int   `json:"completion_tokens,omitempty"`
	TotalTokens       *int   `json:"total_tokens,omitempty"`
}
type ModelDiagnostics struct {
	mu       sync.Mutex
	Requests []ModelRequestDiagnostic `json:"requests"`
}

func (d *ModelDiagnostics) add(x ModelRequestDiagnostic) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Requests = append(d.Requests, x)
}
func (d *ModelDiagnostics) Snapshot() []ModelRequestDiagnostic {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]ModelRequestDiagnostic(nil), d.Requests...)
}
func (c OpenAIClient) RequestDiagnosticSnapshot() []ModelRequestDiagnostic {
	return c.Diagnostics.Snapshot()
}

func (c OpenAIClient) Decide(ctx context.Context, m []Message) (Decision, error) {
	payload, err := c.decisionPayload(m)
	if err != nil {
		return Decision{}, err
	}
	var raw json.RawMessage
	if err := c.post(ctx, payload, &raw); err != nil {
		return Decision{}, err
	}
	return parseDecision(raw, m)
}

func (c OpenAIClient) decisionPayload(m []Message) (map[string]any, error) {
	tools := make([]any, 0, len(c.Config.Tools))
	for _, t := range c.Config.Tools {
		tools = append(tools, map[string]any{"type": "function", "function": map[string]any{"name": t.Name, "description": t.Description, "parameters": t.Parameters}})
	}
	wire := m
	if c.Config.LLM.ToolCallMode == "single" {
		var err error
		wire, err = singleCallWire(m)
		if err != nil {
			return nil, err
		}
	}
	payload := map[string]any{"model": c.Config.Model, "messages": wire, "temperature": c.Config.LLM.Temperature}
	maxTokens := c.Config.Memory.ReservedOutputTokens
	if maxTokens < 1 {
		maxTokens = 1024
	}
	if c.Config.LLM.MaxOutputTokens != nil {
		maxTokens = *c.Config.LLM.MaxOutputTokens
	}
	payload["max_tokens"] = maxTokens
	finalSynthesis := false
	for _, message := range m {
		if message.Role == "system" && message.Content != nil && strings.Contains(*message.Content, "[FINAL SYNTHESIS]") {
			finalSynthesis = true
			break
		}
	}
	if c.Config.Execution.ToolPolicy == "none" || finalSynthesis {
		payload["tool_choice"] = "none"
		if c.Config.Execution.ToolPolicy == "none" {
			payload["messages"] = append([]Message{{Role: "system", Content: str("Tools are disabled for this task. Return the requested final answer directly.")}}, wire...)
		}
	} else {
		payload["tools"] = tools
		payload["tool_choice"] = "auto"
		if c.Config.LLM.ToolCallMode == "single" {
			payload["parallel_tool_calls"] = false
			payload["messages"] = append([]Message{{Role: "system", Content: str("Call at most one configured function per response. Workflow phase labels are not function names.")}}, wire...)
		}
	}
	payload["messages"] = wireMessages(payload["messages"].([]Message))
	return payload, nil
}

func (c OpenAIClient) summaryPayload(goal string, historical []Message, previous *string, hybrid bool, correction ...string) map[string]any {
	prompt := `Create a compacted context state for an agent. Preserve only verified facts.
Use exactly these headings:
- Core Objective
- Universal Truths Discovered
- Dead Ends
- Current Local Pivot
Do not invent facts. The original goal is protected separately. Do not include raw history.`
	if hybrid {
		prompt = `You are the hybrid compaction and reflection engine for an autonomous agent.

Analyze the historical prefix only. The primary agent separately preserves its recent raw
working buffer, so do not reproduce, summarize, truncate, or invent raw messages here.
Extract durable environmental constraints and lessons as imperative operational rules.
Keep dead ends precise and short. Preserve only verified facts; mark uncertainty instead
of promoting guesses to rules. Return exactly this structure and no surrounding prose:

<COMPACTED_STATE>
<USER_GOAL>
Restate the original goal without changing its parameters or definitions.
</USER_GOAL>
<GLOBAL_LESSON_LEDGER>
- Imperative rules for permanent constraints or discoveries; none if no verified lessons.
</GLOBAL_LESSON_LEDGER>
<DEAD_ENDS>
- One-sentence failed approaches and why they failed; none if no verified dead ends.
</DEAD_ENDS>
<CURRENT_LOCAL_PIVOT>
State the most important active hypothesis or next operational focus in one sentence.
</CURRENT_LOCAL_PIVOT>
</COMPACTED_STATE>

The raw working buffer is retained by the primary agent outside this response.`
	}
	if len(correction) > 0 && correction[0] != "" {
		prompt += "\n\nRetry correction: " + correction[0]
	}
	source, _ := sourceText(goal, historical, previous)
	user := source
	maxTokens := c.Config.Memory.SummaryOutputTokens
	if maxTokens < 1 {
		maxTokens = 512
	}
	return map[string]any{"model": c.Config.Model, "temperature": 0, "max_tokens": maxTokens, "messages": wireMessages([]Message{{Role: "system", Content: &prompt}, {Role: "user", Content: &user}})}
}

func parseDecision(raw json.RawMessage, history []Message) (Decision, error) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil {
		return Decision{Invalid: "invalid_model_response: response body is not valid JSON"}, nil
	}
	choicesRaw, exists := envelope["choices"]
	if !exists || string(choicesRaw) == "null" {
		return Decision{Invalid: "invalid_model_response: choices are missing"}, nil
	}
	var choices []json.RawMessage
	if json.Unmarshal(choicesRaw, &choices) != nil {
		return Decision{Invalid: "invalid_model_response: choices must be an array"}, nil
	}
	if len(choices) == 0 {
		return Decision{Invalid: "empty_model_response: choices array is empty"}, nil
	}
	var choice map[string]json.RawMessage
	if json.Unmarshal(choices[0], &choice) != nil {
		return Decision{Invalid: "invalid_model_response: choice is malformed"}, nil
	}
	var msg map[string]json.RawMessage
	if json.Unmarshal(choice["message"], &msg) != nil {
		return Decision{Invalid: "invalid_model_response: choice message is missing or malformed"}, nil
	}
	var content string
	if len(msg["content"]) > 0 && string(msg["content"]) != "null" && json.Unmarshal(msg["content"], &content) != nil {
		return Decision{Invalid: "invalid_model_response: message content must be a string or null"}, nil
	}
	if len(msg["tool_calls"]) == 0 || string(msg["tool_calls"]) == "null" || string(msg["tool_calls"]) == "[]" {
		if strings.TrimSpace(content) == "" {
			return Decision{Invalid: "empty_model_response: content and tool calls are empty"}, nil
		}
		return Decision{Final: true, Content: content}, nil
	}
	var rawCalls []json.RawMessage
	if json.Unmarshal(msg["tool_calls"], &rawCalls) != nil {
		return Decision{Invalid: "invalid_model_response: tool_calls must be an array"}, nil
	}
	if len(rawCalls) == 0 {
		return Decision{Invalid: "empty_model_response: tool_calls array is empty"}, nil
	}
	used := historicalCallIDs(history)
	calls := make([]ToolCall, 0, len(rawCalls))
	for _, rc := range rawCalls {
		call := ToolCall{}
		var obj map[string]json.RawMessage
		if json.Unmarshal(rc, &obj) != nil {
			call.Invalid = "tool call is malformed"
			obj = map[string]json.RawMessage{}
		}
		_ = json.Unmarshal(obj["id"], &call.ID)
		if call.ID == "" || used[call.ID] {
			call.ID = freshCallID(used)
		}
		used[call.ID] = true
		var fn map[string]json.RawMessage
		if json.Unmarshal(obj["function"], &fn) != nil {
			call.Invalid = "function payload is missing or malformed"
			fn = map[string]json.RawMessage{}
		}
		var callType string
		_ = json.Unmarshal(obj["type"], &callType)
		if callType != "" && callType != "function" {
			call.Invalid = "tool call type is not function"
		}
		_ = json.Unmarshal(fn["name"], &call.Name)
		if len(fn["arguments"]) > 0 {
			if json.Unmarshal(fn["arguments"], &call.RawArguments) != nil {
				call.RawArguments = string(fn["arguments"])
			}
		}
		if call.Name == "" {
			call.Invalid = "function name is missing"
		}
		value, err := strictJSON([]byte(call.RawArguments))
		args, ok := value.(map[string]any)
		if err != nil || !ok {
			if call.Invalid == "" {
				call.Invalid = "arguments are not a valid JSON object"
			}
		} else {
			call.Arguments = args
		}
		calls = append(calls, call)
	}
	return Decision{Calls: calls}, nil
}

func singleCallWire(messages []Message) ([]Message, error) {
	var out []Message
	consumed := map[int]bool{}
	for i, m := range messages {
		if m.Role != "assistant" {
			if m.Role == "tool" && !consumed[i] {
				return nil, fmt.Errorf("context_integrity_error: orphaned tool result in retained history")
			}
			if !consumed[i] {
				out = append(out, m)
			}
			continue
		}
		encoded, _ := json.Marshal(m.ToolCalls)
		var calls []json.RawMessage
		if json.Unmarshal(encoded, &calls) != nil || len(calls) == 0 {
			out = append(out, m)
			continue
		}
		if i+len(calls) >= len(messages) {
			return nil, fmt.Errorf("context_integrity_error: tool-call history is missing results")
		}
		results := map[string]Message{}
		for offset := 1; offset <= len(calls); offset++ {
			reply := messages[i+offset]
			if reply.Role != "tool" || reply.ToolCallID == nil {
				return nil, fmt.Errorf("context_integrity_error: tool-call results are incomplete")
			}
			if _, duplicate := results[*reply.ToolCallID]; duplicate {
				return nil, fmt.Errorf("context_integrity_error: duplicate tool result ID")
			}
			results[*reply.ToolCallID] = reply
		}
		seen := map[string]bool{}
		for position, raw := range calls {
			var item map[string]any
			_ = json.Unmarshal(raw, &item)
			id, _ := item["id"].(string)
			if id == "" || seen[id] {
				return nil, fmt.Errorf("context_integrity_error: tool-call history has missing or duplicate IDs")
			}
			seen[id] = true
			reply, ok := results[id]
			if !ok {
				return nil, fmt.Errorf("context_integrity_error: tool-call history is missing a result")
			}
			for offset := 1; offset <= len(calls); offset++ {
				if messages[i+offset].ToolCallID != nil && *messages[i+offset].ToolCallID == id {
					consumed[i+offset] = true
				}
			}
			assistant := Message{Role: "assistant", ToolCalls: json.RawMessage("[" + string(raw) + "]")}
			if position == 0 {
				assistant.Content = m.Content
			}
			out = append(out, assistant, reply)
		}
		consumed[i] = true
	}
	return out, nil
}
func historicalCallIDs(messages []Message) map[string]bool {
	r := map[string]bool{}
	for _, m := range messages {
		if m.ToolCallID != nil {
			r[*m.ToolCallID] = true
		}
		b, _ := json.Marshal(m.ToolCalls)
		var xs []map[string]any
		if json.Unmarshal(b, &xs) == nil {
			for _, x := range xs {
				if id, ok := x["id"].(string); ok && id != "" {
					r[id] = true
				}
			}
		}
	}
	return r
}
func freshCallID(used map[string]bool) string {
	for {
		id := fmt.Sprintf("call_go_%d_%d", time.Now().UnixNano(), generatedCallID.Add(1))
		if !used[id] {
			return id
		}
	}
}
func (c OpenAIClient) Compact(ctx context.Context, goal string, h []Message, old *string, hybrid bool) (string, error) {
	return c.compact(ctx, c.summaryPayload(goal, h, old, hybrid))
}

func (c OpenAIClient) CompactWithFeedback(ctx context.Context, goal string, h []Message, old *string, hybrid bool, feedback string) (string, error) {
	return c.compact(ctx, c.summaryPayload(goal, h, old, hybrid, feedback))
}

func (c OpenAIClient) compact(ctx context.Context, payload map[string]any) (string, error) {
	r := struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}{}
	postErr := c.post(ctx, payload, &r)
	if postErr != nil {
		return "", postErr
	}
	if len(r.Choices) == 0 || r.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("invalid_model_response: compaction summary is empty")
	}
	return r.Choices[0].Message.Content, nil
}

func compactMessageContent(message Message) string {
	if message.Content != nil && *message.Content != "" {
		return *message.Content
	}
	if isPythonFalsy(message.ToolCalls) {
		return ""
	}
	return pythonRepr(message.ToolCalls)
}

func isPythonFalsy(value any) bool {
	switch x := value.(type) {
	case nil:
		return true
	case string:
		return x == ""
	case bool:
		return !x
	case []any:
		return len(x) == 0
	case []map[string]any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	default:
		return false
	}
}

func compactFallback(goal string, historical []Message, hybrid bool) string {
	if hybrid {
		return hybridFallback(goal)
	}
	parts := make([]string, 0, len(historical))
	for _, message := range historical {
		parts = append(parts, compactMessageContent(message))
	}
	text := strings.Join(parts, " ")
	runes := []rune(text)
	if len(runes) > 1800 {
		text = string(runes[len(runes)-1800:])
	}
	return "- Core Objective: " + goal + "\n- Universal Truths Discovered: none verified\n- Dead Ends: " + text + "\n- Current Local Pivot: Review retained raw turns."
}

// pythonRepr matches the Python repr used when Message.tool_calls is interpolated
// into the compaction request. Tool-call maps use their protocol field order;
// other map keys are sorted because Go maps do not retain insertion order.
func pythonRepr(value any) string {
	switch x := value.(type) {
	case nil:
		return "None"
	case string:
		return pythonString(x)
	case bool:
		if x {
			return "True"
		}
		return "False"
	case []any:
		parts := make([]string, len(x))
		for i, item := range x {
			parts[i] = pythonRepr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case []map[string]any:
		parts := make([]string, len(x))
		for i, item := range x {
			parts[i] = pythonRepr(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(x))
		for key := range x {
			keys = append(keys, key)
		}
		preferred := []string{"id", "type", "function", "name", "arguments"}
		ordered := make([]string, 0, len(keys))
		used := map[string]bool{}
		for _, key := range preferred {
			if _, ok := x[key]; ok {
				ordered = append(ordered, key)
				used[key] = true
			}
		}
		rest := make([]string, 0, len(keys)-len(ordered))
		for _, key := range keys {
			if !used[key] {
				rest = append(rest, key)
			}
		}
		sort.Strings(rest)
		ordered = append(ordered, rest...)
		parts := make([]string, 0, len(ordered))
		for _, key := range ordered {
			parts = append(parts, pythonString(key)+": "+pythonRepr(x[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case fmt.Stringer:
		return pythonString(x.String())
	default:
		return fmt.Sprint(x)
	}
}

func pythonString(value string) string {
	quote := byte('\'')
	if strings.Contains(value, "'") && !strings.Contains(value, `"`) {
		quote = '"'
	}
	var out strings.Builder
	out.WriteByte(quote)
	for _, r := range value {
		switch r {
		case '\\':
			out.WriteString(`\\`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r == rune(quote) {
				out.WriteByte('\\')
			}
			out.WriteRune(r)
		}
	}
	out.WriteByte(quote)
	return out.String()
}
func hybridFallback(goal string) string {
	return "<COMPACTED_STATE>\n<USER_GOAL>\n" + goal + "\n</USER_GOAL>\n<GLOBAL_LESSON_LEDGER>\n- No verified global lessons extracted.\n</GLOBAL_LESSON_LEDGER>\n<DEAD_ENDS>\n- No verified dead ends extracted; inspect the retained raw working buffer.\n</DEAD_ENDS>\n<CURRENT_LOCAL_PIVOT>\nReview the retained raw working buffer and continue from the latest verified state.\n</CURRENT_LOCAL_PIVOT>\n</COMPACTED_STATE>"
}
func (c OpenAIClient) post(ctx context.Context, p any, out any) error {
	base := strings.TrimRight(c.Config.LLM.BaseURL, "/")
	primary := base
	if !strings.HasSuffix(primary, "/chat/completions") {
		primary += "/chat/completions"
	}
	urls := []string{primary}
	if strings.HasSuffix(base, "/v1") {
		alternate := strings.TrimSuffix(base, "/v1") + "/chat/completions"
		if alternate != primary {
			urls = append(urls, alternate)
		}
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: time.Duration(c.Config.LLM.TimeoutSeconds * float64(time.Second))}
	}
	var err error
	for i, url := range urls {
		err = c.postURL(ctx, client, url, p, out)
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound && i < len(urls)-1 {
			continue
		}
		return categorizedModelError(err)
	}
	return categorizedModelError(err)
}

func categorizedModelError(err error) error {
	if err == nil {
		return nil
	}
	var status *httpStatusError
	if errors.As(err, &status) {
		if status.Template {
			return fmt.Errorf("model_template_error: provider rejected the request template")
		}
		return fmt.Errorf("model_http_error: request failed with HTTP %d", status.StatusCode)
	}
	var responseErr *modelResponseError
	if errors.As(err, &responseErr) {
		return fmt.Errorf("invalid_model_response: provider returned a malformed response")
	}
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("model_timeout: model request timed out")
	}
	return fmt.Errorf("model_transport_error: model request could not be completed")
}

func modelFailureCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "model_timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "model_timeout"
	}
	return "model_transport_error"
}

type modelResponseError struct{}

func (*modelResponseError) Error() string { return "malformed model response" }

type httpStatusError struct {
	StatusCode int
	Status     string
	Template   bool
}

func (e *httpStatusError) Error() string { return "model service returned " + e.Status }

func (c OpenAIClient) postURL(ctx context.Context, client *http.Client, url string, p any, out any) error {
	started := time.Now()
	b, err := compactJSON(p)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	key := os.Getenv(defaultString(c.Config.LLM.APIKeyEnv, "OPENAI_API_KEY"))
	req.Header.Set("Authorization", "Bearer "+defaultString(key, "local-not-required"))
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		c.Diagnostics.add(c.requestDiagnostic(p, len(b), time.Since(started), 0, modelFailureCode(err)))
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		template := strings.Contains(strings.ToLower(string(body)), "template")
		failure := "model_http_error"
		if template {
			failure = "model_template_error"
		}
		c.Diagnostics.add(c.requestDiagnostic(p, len(b), time.Since(started), res.StatusCode, failure))
		return &httpStatusError{StatusCode: res.StatusCode, Status: res.Status, Template: template}
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if readErr != nil || json.Unmarshal(responseBody, out) != nil {
		c.Diagnostics.add(c.requestDiagnostic(p, len(b), time.Since(started), res.StatusCode, "invalid_model_response"))
		return &modelResponseError{}
	}
	requestDiagnostic := c.requestDiagnostic(p, len(b), time.Since(started), res.StatusCode, "")
	requestDiagnostic.UsageKnown, requestDiagnostic.PromptTokens, requestDiagnostic.CompletionTokens, requestDiagnostic.TotalTokens = parseUsage(responseBody)
	c.Diagnostics.add(requestDiagnostic)
	return nil
}
func (c OpenAIClient) requestDiagnostic(p any, size int, elapsed time.Duration, status int, failure string) ModelRequestDiagnostic {
	kind := "decision"
	if payload, ok := p.(map[string]any); ok {
		if _, isDecision := payload["tool_choice"]; !isDecision {
			kind = "compaction"
		}
	}
	maxTokens := c.Config.Memory.ReservedOutputTokens
	if c.Config.LLM.MaxOutputTokens != nil {
		maxTokens = *c.Config.LLM.MaxOutputTokens
	}
	return ModelRequestDiagnostic{Kind: kind, RequestBytes: size, ElapsedMS: elapsed.Milliseconds(), HTTPStatus: status, Failure: failure, ToolCallMode: c.Config.LLM.ToolCallMode, InputBudgetTokens: c.Config.Memory.ContextWindowTokens - c.Config.Memory.ReservedOutputTokens - c.Config.Memory.SafetyMarginTokens, MaxOutputTokens: maxTokens}
}

func parseUsage(body []byte) (bool, *int, *int, *int) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil {
		return false, nil, nil, nil
	}
	raw, ok := envelope["usage"]
	if !ok || string(raw) == "null" {
		return false, nil, nil, nil
	}
	var usage map[string]json.RawMessage
	if json.Unmarshal(raw, &usage) != nil {
		return false, nil, nil, nil
	}
	read := func(key string) *int {
		var n int
		if json.Unmarshal(usage[key], &n) != nil {
			return nil
		}
		return &n
	}
	prompt, completion, total := read("prompt_tokens"), read("completion_tokens"), read("total_tokens")
	if prompt == nil && completion == nil && total == nil {
		return false, nil, nil, nil
	}
	return true, prompt, completion, total
}
func defaultString(a, b string) string {
	if a == "" {
		return b
	}
	return a
}
