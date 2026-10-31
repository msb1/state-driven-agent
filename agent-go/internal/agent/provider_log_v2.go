package agent

import (
	"encoding/json"
	"log"
	"regexp"
	"strings"
	"unicode"
)

var providerFieldV2 = regexp.MustCompile(`^[A-Za-z0-9_.\[\]/:-]{1,160}$`)

func providerMessageV2(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	for _, pattern := range []string{"\"[^\"\\r\\n]{0,2000}\"", "'[^'\\r\\n]{0,2000}'", "`[^`\\r\\n]{0,2000}`", `(?i)\b(messages?|prompt|request|body|content)\b.*$`, `(?i)\b(authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)\b\s*[:=]\s*[^\s,;]+`, `(?i)\bbearer\s+[A-Za-z0-9._~+/-]+=*`, `(?i)\b(sk|pk|rk|xox[baprs])-[-A-Za-z0-9_]{8,}\b`, `(?i)https?://[^\s\])}>]+`} {
		value = regexp.MustCompile(pattern).ReplaceAllString(value, "[REDACTED]")
	}
	value = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}
		return -1
	}, value)
	runes := []rune(value)
	if len(runes) > 500 {
		value = string(runes[:500])
	}
	return value
}
func logProviderFailureV2(status int, raw []byte, requestID string) {
	record := map[string]any{"event": "model_provider_failure", "http_status": status}
	var body map[string]any
	if json.Unmarshal(raw, &body) == nil {
		fields := map[string]any{}
		switch e := body["error"].(type) {
		case map[string]any:
			for _, key := range []string{"type", "code", "param"} {
				if value, ok := e[key].(string); ok && providerFieldV2.MatchString(strings.TrimSpace(value)) {
					fields[key] = strings.TrimSpace(value)
				}
			}
			if value, ok := e["message"].(string); ok {
				fields["message"] = providerMessageV2(value)
			}
		case string:
			fields["message"] = providerMessageV2(e)
		}
		if len(fields) > 0 {
			record["provider_error"] = fields
		}
	}
	if providerFieldV2.MatchString(requestID) {
		record["provider_request_id"] = requestID
	}
	encoded, _ := json.Marshal(record)
	log.Print(string(encoded))
}
