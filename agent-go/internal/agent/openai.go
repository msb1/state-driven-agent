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
	prompt := "Create a compacted context state with verified facts only."
	if hybrid {
		prompt = "Return a <COMPACTED_STATE> with a <GLOBAL_LESSON_LEDGER>; preserve verified lessons about failures."
	}
	r := struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}{}
	err := c.post(ctx, map[string]any{"model": c.Config.Model, "temperature": 0, "messages": []Message{{Role: "system", Content: &prompt}, {Role: "user", Content: str("Goal: " + goal + "\nHistorical prefix: " + fmt.Sprint(h))}}}, &r)
	if err != nil || len(r.Choices) == 0 {
		return "<COMPACTED_STATE>\n<USER_GOAL>\n" + goal + "\n</USER_GOAL>\n<GLOBAL_LESSON_LEDGER>\n- Preserve verified tool results.\n</GLOBAL_LESSON_LEDGER>\n</COMPACTED_STATE>", nil
	}
	return r.Choices[0].Message.Content, nil
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
