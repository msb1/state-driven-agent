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
	phase := s.Config.Workflow.Phases[0]
	if len(phase.Completion) != 4 || !phase.Completion[3].Required || phase.Completion[3].Tool != "audit_employee_phase_1" {
		t.Fatalf("required workflow evidence was not parsed: %#v", phase.Completion)
	}
	if !s.Config.Workflow.Phases[1].Completion[2].Required || !s.Config.Workflow.Phases[2].Completion[2].Required || !s.Config.Workflow.Phases[2].Completion[3].Required {
		t.Fatalf("omitted required flags must default to true: %#v", s.Config.Workflow.Phases)
	}
}
