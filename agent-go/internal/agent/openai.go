package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// OpenAIClient supports an OpenAI-compatible /chat/completions endpoint.
type OpenAIClient struct {
	Config Config
	HTTP   *http.Client
}

func (c OpenAIClient) Decide(ctx context.Context, m []Message) (Decision, error) {
	tools := make([]any, 0, len(c.Config.Tools))
	for _, t := range c.Config.Tools {
		tools = append(tools, map[string]any{"type": "function", "function": map[string]any{"name": t.Name, "description": t.Description, "parameters": t.Parameters}})
	}
	var r struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string                           `json:"id"`
					Function struct{ Name, Arguments string } `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := c.post(ctx, map[string]any{"model": c.Config.Model, "messages": m, "tools": tools, "tool_choice": "auto", "temperature": c.Config.LLM.Temperature}, &r); err != nil {
		return Decision{}, err
	}
	if len(r.Choices) == 0 {
		return Decision{}, fmt.Errorf("model returned no choices")
	}
	x := r.Choices[0].Message
	if len(x.ToolCalls) == 0 {
		return Decision{Final: true, Content: defaultString(x.Content, "No final answer was returned.")}, nil
	}
	var a map[string]any
	if err := json.Unmarshal([]byte(x.ToolCalls[0].Function.Arguments), &a); err != nil {
		return Decision{Final: true, Content: "LLM emitted invalid tool arguments: " + err.Error()}, nil
	}
	return Decision{ID: defaultString(x.ToolCalls[0].ID, "call_1"), Name: x.ToolCalls[0].Function.Name, Arguments: a}, nil
}
func (c OpenAIClient) Compact(ctx context.Context, goal string, h []Message, old *string, hybrid bool) (string, error) {
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
	r := struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}{}
	previous := "none"
	if old != nil && *old != "" {
		previous = *old
	}
	var source strings.Builder
	for i, message := range h {
		if i > 0 {
			source.WriteByte('\n')
		}
		source.WriteString(message.Role + ": " + compactMessageContent(message))
	}
	user := "Goal: " + goal + "\nPrevious ledger: " + previous + "\nHistorical prefix:\n" + source.String()
	postErr := c.post(ctx, map[string]any{"model": c.Config.Model, "temperature": 0, "messages": []Message{{Role: "system", Content: &prompt}, {Role: "user", Content: str(user)}}}, &r)
	if postErr != nil || len(r.Choices) == 0 || r.Choices[0].Message.Content == "" {
		return compactFallback(goal, h, hybrid), nil
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
		return err
	}
	return err
}

type httpStatusError struct {
	StatusCode int
	Status     string
}

func (e *httpStatusError) Error() string { return "model service returned " + e.Status }

func (c OpenAIClient) postURL(ctx context.Context, client *http.Client, url string, p any, out any) error {
	b, err := json.Marshal(p)
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
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return &httpStatusError{StatusCode: res.StatusCode, Status: res.Status}
	}
	return json.NewDecoder(res.Body).Decode(out)
}
func defaultString(a, b string) string {
	if a == "" {
		return b
	}
	return a
}
