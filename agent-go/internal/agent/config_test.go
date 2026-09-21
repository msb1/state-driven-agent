package agent

import "testing"

func TestLoadCaseThreeSnapshot(t *testing.T) {
	s, err := LoadSnapshot("../../../config", "test-case-3.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if s.Config.Memory.MaxTokens != 2000 || s.Config.Workflow == nil || len(s.Config.Workflow.Phases) != 3 || s.Config.LLM.BaseURL == "" {
		t.Fatalf("config was not faithfully parsed: %#v", s.Config)
	}
}
