package agent

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type LLM struct {
	BaseURL        string  `yaml:"base_url" json:"base_url"`
	APIKeyEnv      string  `yaml:"api_key_env" json:"api_key_env"`
	Temperature    float64 `json:"temperature"`
	TimeoutSeconds float64 `yaml:"timeout_seconds" json:"timeout_seconds"`
}
type Output struct {
	VerboseSetup bool `yaml:"verbose_setup" json:"verbose_setup"`
}
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}
type fileConfig struct {
	Agent Config `yaml:"agent"`
}
type Snapshot struct {
	Ref, SHA256 string
	Config      Config
}

var environment = regexp.MustCompile(`\$\{([A-Z0-9_]+)(?::-([^}]*))?\}`)

// LoadSnapshot accepts only a YAML config reference contained by root and hashes unexpanded bytes.
func LoadSnapshot(root, ref string) (Snapshot, error) {
	if ref == "" || filepath.IsAbs(ref) || strings.HasPrefix(filepath.Clean(ref), ".."+string(filepath.Separator)) {
		return Snapshot{}, fmt.Errorf("config_ref must be a non-empty relative YAML path")
	}
	path := filepath.Join(root, ref)
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Snapshot{}, err
	}
	base, err := filepath.EvalSymlinks(root)
	if err != nil {
		return Snapshot{}, err
	}
	relative, err := filepath.Rel(base, real)
	if err != nil || strings.HasPrefix(relative, "..") {
		return Snapshot{}, fmt.Errorf("config_ref must remain within AGENT_CONFIG_ROOT")
	}
	raw, err := os.ReadFile(real)
	if err != nil {
		return Snapshot{}, err
	}
	expanded := environment.ReplaceAllStringFunc(string(raw), func(x string) string {
		p := environment.FindStringSubmatch(x)
		if v, ok := os.LookupEnv(p[1]); ok {
			return v
		}
		return p[2]
	})
	var f fileConfig
	if err := yaml.Unmarshal([]byte(expanded), &f); err != nil {
		return Snapshot{}, err
	}
	if f.Agent.Name == "" {
		return Snapshot{}, fmt.Errorf("invalid agent configuration")
	}
	if err := validateWorkflow(f.Agent.Workflow); err != nil {
		return Snapshot{}, err
	}
	sum := sha256.Sum256(raw)
	return Snapshot{Ref: filepath.ToSlash(relative), SHA256: fmt.Sprintf("%x", sum), Config: f.Agent}, nil
}
func validateWorkflow(w *WorkflowConfig) error {
	if w == nil {
		return nil
	}
	seen := map[string]bool{}
	for _, p := range w.Phases {
		if p.ID == "" || seen[p.ID] {
			return fmt.Errorf("workflow phase IDs must be unique")
		}
		seen[p.ID] = true
	}
	if !seen[w.EntryPhase] {
		return fmt.Errorf("workflow.entry_phase must name a configured phase")
	}
	for _, p := range w.Phases {
		for _, t := range p.Transitions {
			if t.To != "complete" && !seen[t.To] {
				return fmt.Errorf("workflow transition from %s targets unknown phase %s", p.ID, t.To)
			}
		}
	}
	return nil
}
