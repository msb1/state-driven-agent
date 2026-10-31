package agent

import (
	"encoding/json"
	"os"
	"testing"
)

func TestV2PinnedCommandAndActionIdentityVectors(t *testing.T) {
	b, err := os.ReadFile("../../../schema/idempotency-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Actions  []map[string]any
		Commands []map[string]any
	}
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	for _, a := range v.Actions {
		for _, check := range []struct {
			preimage []any
			expected any
		}{{[]any{"agent-action-v1", a["namespace"], a["session_id"], a["execution_id"]}, a["expected_key"]}, {[]any{"agent-payload-v1", a["action_type"], a["target"], a["arguments"]}, a["expected_payload_sha256"]}} {
			hash, err := DigestV2(check.preimage)
			if err != nil || hash != check.expected {
				t.Fatalf("action %s hash=%s error=%v", a["vector_id"], hash, err)
			}
		}
	}
	for _, c := range v.Commands {
		hash, err := DigestV2([]any{"agent-command-v1", c["method"], c["route"], c["request"]})
		if err != nil || hash != c["expected_sha256"] {
			t.Fatalf("command %s hash=%s error=%v", c["vector_id"], hash, err)
		}
	}
}
func TestV2StrictJSONAndJCSBoundaries(t *testing.T) {
	for _, raw := range []string{`{"x":1,"x":2}`, `{"nested":{"x":1,"x":2}}`, `{"x":9007199254740992}`, `{"x":"\ud800"}`, `{"x":"\udc00"}`, `{"x":NaN}`, `{"x":1e999}`, `{} {}`} {
		if _, err := StrictJSON([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid JSON %s", raw)
		}
	}
	for _, c := range []struct{ raw, want string }{{`{"b":1.0,"a":-0}`, `{"a":0,"b":1}`}, {`[1e-7,1e20,1e21]`, `[1e-7,100000000000000000000,1e+21]`}, {`{"\ue000":1,"\ud83d\ude00":2}`, "{\"😀\":2,\"\ue000\":1}"}} {
		value, err := StrictJSON([]byte(c.raw))
		if err != nil {
			t.Fatal(err)
		}
		got, err := CanonicalV2(value)
		if err != nil || string(got) != c.want {
			t.Fatalf("canonical %s != %s error=%v", got, c.want, err)
		}
	}
}
