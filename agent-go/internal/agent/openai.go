package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
	prompt := "Create a compacted context state for an agent. Preserve only verified facts. Use exactly these headings: - Core Objective - Universal Truths Discovered - Dead Ends - Current Local Pivot. Do not invent facts. The original goal is protected separately. Do not include raw history."
	if hybrid {
		prompt = "You are the hybrid compaction and reflection engine for an autonomous agent. Analyze the historical prefix only. The primary agent separately preserves its recent raw working buffer, so do not reproduce, summarize, truncate, or invent raw messages here. Extract durable environmental constraints and lessons as imperative operational rules. Keep dead ends precise and short. Preserve only verified facts; mark uncertainty instead of promoting guesses to rules. Return exactly this structure and no surrounding prose: <COMPACTED_STATE><USER_GOAL>Restate the original goal without changing its parameters or definitions.</USER_GOAL><GLOBAL_LESSON_LEDGER>- Imperative rules for permanent constraints or discoveries; none if no verified lessons.</GLOBAL_LESSON_LEDGER><DEAD_ENDS>- One-sentence failed approaches and why they failed; none if no verified dead ends.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>State the most important active hypothesis or next operational focus in one sentence.</CURRENT_LOCAL_PIVOT></COMPACTED_STATE> The raw working buffer is retained by the primary agent outside this response."
	}
	r := struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}{}
	previous := "none"
	if old != nil {
		previous = *old
	}
	err := c.post(ctx, map[string]any{"model": c.Config.Model, "temperature": 0, "messages": []Message{{Role: "system", Content: &prompt}, {Role: "user", Content: str("Goal: " + goal + "\nPrevious ledger: " + previous + "\nHistorical prefix:\n" + fmt.Sprint(h))}}}, &r)
	if err != nil || len(r.Choices) == 0 {
		if hybrid {
			return hybridFallback(goal), nil
		}
		return "- Core Objective: " + goal + "\n- Universal Truths Discovered: none verified\n- Dead Ends: none verified\n- Current Local Pivot: Review retained raw turns.", nil
	}
	return r.Choices[0].Message.Content, nil
}
func hybridFallback(goal string) string {
	return "<COMPACTED_STATE>\n<USER_GOAL>\n" + goal + "\n</USER_GOAL>\n<GLOBAL_LESSON_LEDGER>\n- No verified global lessons extracted.\n</GLOBAL_LESSON_LEDGER>\n<DEAD_ENDS>\n- No verified dead ends extracted; inspect the retained raw working buffer.\n</DEAD_ENDS>\n<CURRENT_LOCAL_PIVOT>\nReview the retained raw working buffer and continue from the latest verified state.\n</CURRENT_LOCAL_PIVOT>\n</COMPACTED_STATE>"
}
func (c OpenAIClient) post(ctx context.Context, p any, out any) error {
	b, _ := json.Marshal(p)
	base := strings.TrimRight(c.Config.LLM.BaseURL, "/")
	url := base
	if !strings.HasSuffix(url, "/chat/completions") {
		url += "/chat/completions"
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: time.Duration(c.Config.LLM.TimeoutSeconds * float64(time.Second))}
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
		return fmt.Errorf("model service returned %s", res.Status)
	}
	return json.NewDecoder(res.Body).Decode(out)
}
func defaultString(a, b string) string {
	if a == "" {
		return b
	}
	return a
}
