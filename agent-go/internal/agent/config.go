package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type LLM struct {
	BaseURL         string  `yaml:"base_url" json:"base_url"`
	APIKeyEnv       string  `yaml:"api_key_env" json:"api_key_env"`
	Temperature     float64 `yaml:"temperature" json:"temperature"`
	TimeoutSeconds  float64 `yaml:"timeout_seconds" json:"timeout_seconds"`
	ToolCallMode    string  `yaml:"tool_call_mode" json:"tool_call_mode"`
	MaxOutputTokens *int    `yaml:"max_output_tokens" json:"max_output_tokens"`
}
type Output struct {
	VerboseSetup bool `yaml:"verbose_setup" json:"verbose_setup"`
}
type ToolRecovery struct {
	Mode        string `yaml:"mode" json:"mode"`
	MaxAttempts int    `yaml:"max_attempts" json:"max_attempts"`
}
type ToolDefinition struct {
	Name        string         `yaml:"name" json:"name"`
	Description string         `yaml:"description" json:"description"`
	Parameters  map[string]any `yaml:"parameters" json:"parameters"`
	Recovery    *ToolRecovery  `yaml:"recovery" json:"recovery"`
}
type Memory struct {
	MaxTokens                              int     `yaml:"max_tokens" json:"max_tokens"`
	MaxSteps                               int     `yaml:"max_steps" json:"max_steps"`
	RawTurnsToKeep                         int     `yaml:"raw_turns_to_keep" json:"raw_turns_to_keep"`
	HybridReflection                       bool    `yaml:"hybrid_reflection" json:"hybrid_reflection"`
	RunMaxSteps                            *int    `yaml:"run_max_steps" json:"run_max_steps"`
	CompactionIntervalSteps                *int    `yaml:"compaction_interval_steps" json:"compaction_interval_steps"`
	ContextWindowTokens                    int     `yaml:"context_window_tokens" json:"context_window_tokens"`
	ReservedOutputTokens                   int     `yaml:"reserved_output_tokens" json:"reserved_output_tokens"`
	SafetyMarginTokens                     int     `yaml:"safety_margin_tokens" json:"safety_margin_tokens"`
	MaxSummaryTokens                       int     `yaml:"max_summary_tokens" json:"max_summary_tokens"`
	SummaryOutputTokens                    int     `yaml:"summary_output_tokens" json:"summary_output_tokens"`
	CompactionMaxAttempts                  int     `yaml:"compaction_max_attempts" json:"compaction_max_attempts"`
	CompactionMaxPasses                    int     `yaml:"compaction_max_passes" json:"compaction_max_passes"`
	CompactionMinGrowthTokens              int     `yaml:"compaction_min_growth_tokens" json:"compaction_min_growth_tokens"`
	CompactionMinIntervalSteps             int     `yaml:"compaction_min_interval_steps" json:"compaction_min_interval_steps"`
	OptionalCompactionMinPressureRatio     float64 `yaml:"optional_compaction_min_pressure_ratio" json:"optional_compaction_min_pressure_ratio"`
	OptionalCompactionMinReclaimableTokens int     `yaml:"optional_compaction_min_reclaimable_tokens" json:"optional_compaction_min_reclaimable_tokens"`
	OptionalCompactionMinReductionTokens   int     `yaml:"optional_compaction_min_reduction_tokens" json:"optional_compaction_min_reduction_tokens"`
	OptionalCompactionMinReductionRatio    float64 `yaml:"optional_compaction_min_reduction_ratio" json:"optional_compaction_min_reduction_ratio"`
}
type Binding struct {
	Tool        string `yaml:"tool" json:"tool"`
	Argument    string `yaml:"argument" json:"argument"`
	SourceLabel string `yaml:"source_label" json:"source_label"`
	Path        []any  `yaml:"path" json:"path"`
}
type Execution struct {
	ArgumentBindings               []Binding `yaml:"argument_bindings" json:"argument_bindings"`
	ToolPolicy                     string    `yaml:"tool_policy" json:"tool_policy"`
	MaxConsecutiveInvalidResponses int       `yaml:"max_consecutive_invalid_responses" json:"max_consecutive_invalid_responses"`
	MaxNoProgressSteps             *int      `yaml:"max_no_progress_steps" json:"max_no_progress_steps"`
}
type ResultCheck struct {
	Kind       string         `yaml:"kind" json:"kind"`
	Tool       string         `yaml:"tool" json:"tool"`
	Arguments  map[string]any `yaml:"arguments" json:"arguments"`
	Result     map[string]any `yaml:"result" json:"result"`
	Path       []any          `yaml:"path" json:"path"`
	AnswerPath []any          `yaml:"answer_path" json:"answer_path"`
	Label      string         `yaml:"label" json:"label"`
}
type Completion struct {
	Instructions      *string        `yaml:"instructions" json:"instructions"`
	ForbiddenPatterns []string       `yaml:"forbidden_patterns" json:"forbidden_patterns"`
	ResultChecks      []ResultCheck  `yaml:"result_checks" json:"result_checks"`
	Exact             *string        `yaml:"exact" json:"exact"`
	Pattern           *string        `yaml:"pattern" json:"pattern"`
	JSONSchema        map[string]any `yaml:"json_schema" json:"json_schema"`
}
type Evidence struct {
	Description     string         `yaml:"description" json:"description"`
	Tool            string         `yaml:"tool" json:"tool"`
	Required        bool           `yaml:"required" json:"required"`
	Arguments       map[string]any `yaml:"arguments" json:"arguments"`
	Result          map[string]any `yaml:"result" json:"result"`
	ContentContains []string       `yaml:"content_contains" json:"content_contains"`
}

func (e *Evidence) UnmarshalYAML(n *yaml.Node) error {
	var v struct {
		Description     string         `yaml:"description"`
		Tool            string         `yaml:"tool"`
		Required        *bool          `yaml:"required"`
		Arguments       map[string]any `yaml:"arguments"`
		Result          map[string]any `yaml:"result"`
		ContentContains []string       `yaml:"content_contains"`
	}
	if err := n.Decode(&v); err != nil {
		return err
	}
	e.Description = v.Description
	e.Tool = v.Tool
	e.Required = v.Required == nil || *v.Required
	e.Arguments = v.Arguments
	e.Result = v.Result
	e.ContentContains = v.ContentContains
	return nil
}

type Edge struct {
	To       string     `yaml:"to" json:"to"`
	When     string     `yaml:"when" json:"when"`
	Evidence []Evidence `yaml:"evidence" json:"evidence"`
}
type Phase struct {
	ID              string     `yaml:"id" json:"id"`
	Name            string     `yaml:"name" json:"name"`
	Instruction     string     `yaml:"instruction" json:"instruction"`
	AllowedTools    []string   `yaml:"allowed_tools" json:"allowed_tools"`
	AllowedToolsSet bool       `yaml:"-" json:"-"`
	Completion      []Evidence `yaml:"completion" json:"completion"`
	Transitions     []Edge     `yaml:"transitions" json:"transitions"`
}

func (p *Phase) UnmarshalYAML(n *yaml.Node) error {
	type raw Phase
	var v raw
	if err := n.Decode(&v); err != nil {
		return err
	}
	*p = Phase(v)
	for i := 0; i < len(n.Content); i += 2 {
		if n.Content[i].Value == "allowed_tools" && n.Content[i+1].Tag != "!!null" {
			p.AllowedToolsSet = true
		}
	}
	return nil
}

type WorkflowConfig struct {
	EntryPhase string  `yaml:"entry_phase" json:"entry_phase"`
	Phases     []Phase `yaml:"phases" json:"phases"`
}
type Config struct {
	Name         string           `yaml:"name" json:"name"`
	SystemPrompt string           `yaml:"system_prompt" json:"system_prompt"`
	Model        string           `yaml:"model" json:"model"`
	LLM          LLM              `yaml:"llm" json:"llm"`
	Memory       Memory           `yaml:"memory" json:"memory"`
	Output       Output           `yaml:"output" json:"output"`
	Execution    Execution        `yaml:"execution" json:"execution"`
	Completion   Completion       `yaml:"completion" json:"completion"`
	Tools        []ToolDefinition `yaml:"tools" json:"tools"`
	Workflow     *WorkflowConfig  `yaml:"workflow" json:"workflow"`
}
type fileConfig struct {
	Agent Config `yaml:"agent"`
}
type Snapshot struct {
	Ref, SHA256    string
	ResolvedSHA256 string
	Config         Config
}

var environment = regexp.MustCompile(`\$\{([A-Z0-9_]+)(?::-([^}]*))?\}`)

func LoadSnapshot(root, ref string) (Snapshot, error) {
	if ref == "" || filepath.IsAbs(ref) || filepath.Ext(ref) != ".yaml" && filepath.Ext(ref) != ".yml" {
		return Snapshot{}, fmt.Errorf("config_ref must be a non-empty relative YAML path")
	}
	rootReal, err := filepath.EvalSymlinks(root)
	if err != nil {
		return Snapshot{}, err
	}
	path, err := filepath.EvalSymlinks(filepath.Join(root, ref))
	if err != nil {
		return Snapshot{}, err
	}
	rel, err := filepath.Rel(rootReal, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return Snapshot{}, fmt.Errorf("config_ref must remain within AGENT_CONFIG_ROOT")
	}
	info, err := os.Stat(path)
	if err != nil {
		return Snapshot{}, err
	}
	if !info.Mode().IsRegular() {
		return Snapshot{}, fmt.Errorf("config_ref must identify a YAML file within AGENT_CONFIG_ROOT")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	var node yaml.Node
	if err = yaml.Unmarshal(raw, &node); err != nil {
		return Snapshot{}, err
	}
	expandNode(&node)
	var f fileConfig
	if err = node.Decode(&f); err != nil {
		return Snapshot{}, err
	}
	if err = applyDefaults(&f.Agent, &node); err != nil {
		return Snapshot{}, err
	}
	if err = validateConfig(&f.Agent); err != nil {
		return Snapshot{}, err
	}
	rawHash := sha256.Sum256(raw)
	canonical, err := canonicalConfigJSON(f.Agent)
	if err != nil {
		return Snapshot{}, err
	}
	resolved := sha256.Sum256(canonical)
	return Snapshot{Ref: filepath.ToSlash(rel), SHA256: hex.EncodeToString(rawHash[:]), ResolvedSHA256: hex.EncodeToString(resolved[:]), Config: f.Agent}, nil
}
func expandNode(n *yaml.Node) {
	if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
		n.Value = environment.ReplaceAllStringFunc(n.Value, func(x string) string {
			m := environment.FindStringSubmatch(x)
			if v, ok := os.LookupEnv(m[1]); ok {
				return v
			}
			return m[2]
		})
	}
	for _, c := range n.Content {
		expandNode(c)
	}
}
func applyDefaults(c *Config, root *yaml.Node) error {
	if !yamlPathExists(root, "agent", "llm", "api_key_env") {
		c.LLM.APIKeyEnv = "OPENAI_API_KEY"
	}
	if !yamlPathExists(root, "agent", "llm", "temperature") {
		c.LLM.Temperature = .1
	}
	if !yamlPathExists(root, "agent", "llm", "timeout_seconds") {
		c.LLM.TimeoutSeconds = 90
	}
	if !yamlPathExists(root, "agent", "llm", "tool_call_mode") {
		c.LLM.ToolCallMode = "native"
	}
	m := &c.Memory
	if !yamlPathExists(root, "agent", "memory", "context_window_tokens") {
		m.ContextWindowTokens = 32768
	}
	if !yamlPathExists(root, "agent", "memory", "reserved_output_tokens") {
		m.ReservedOutputTokens = 1024
	}
	if !yamlPathExists(root, "agent", "memory", "safety_margin_tokens") {
		m.SafetyMarginTokens = 256
	}
	if !yamlPathExists(root, "agent", "memory", "max_summary_tokens") {
		m.MaxSummaryTokens = 4096
	}
	if !yamlPathExists(root, "agent", "memory", "summary_output_tokens") {
		m.SummaryOutputTokens = 512
	}
	if !yamlPathExists(root, "agent", "memory", "compaction_max_attempts") {
		m.CompactionMaxAttempts = 2
	}
	if !yamlPathExists(root, "agent", "memory", "compaction_max_passes") {
		m.CompactionMaxPasses = 3
	}
	if !yamlPathExists(root, "agent", "memory", "compaction_min_growth_tokens") {
		m.CompactionMinGrowthTokens = 512
	}
	if !yamlPathExists(root, "agent", "memory", "hybrid_reflection") {
		m.HybridReflection = true
	}
	e := &c.Execution
	if !yamlPathExists(root, "agent", "execution", "tool_policy") {
		e.ToolPolicy = "auto"
	}
	if !yamlPathExists(root, "agent", "execution", "max_consecutive_invalid_responses") {
		e.MaxConsecutiveInvalidResponses = 3
	}
	if e.ArgumentBindings == nil {
		e.ArgumentBindings = []Binding{}
	}
	for i := range e.ArgumentBindings {
		if e.ArgumentBindings[i].Path == nil {
			e.ArgumentBindings[i].Path = []any{}
		}
	}
	if c.Completion.ForbiddenPatterns == nil {
		c.Completion.ForbiddenPatterns = []string{}
	}
	if c.Completion.ResultChecks == nil {
		c.Completion.ResultChecks = []ResultCheck{}
	}
	for i := range c.Completion.ResultChecks {
		if c.Completion.ResultChecks[i].Arguments == nil {
			c.Completion.ResultChecks[i].Arguments = map[string]any{}
		}
		if c.Completion.ResultChecks[i].Result == nil {
			c.Completion.ResultChecks[i].Result = map[string]any{}
		}
		if c.Completion.ResultChecks[i].Path == nil {
			c.Completion.ResultChecks[i].Path = []any{}
		}
		if c.Completion.ResultChecks[i].AnswerPath == nil {
			c.Completion.ResultChecks[i].AnswerPath = []any{}
		}
	}
	if c.Tools == nil {
		c.Tools = []ToolDefinition{}
	}
	if c.Workflow != nil {
		for pi := range c.Workflow.Phases {
			p := &c.Workflow.Phases[pi]
			if p.Completion == nil {
				p.Completion = []Evidence{}
			}
			if p.Transitions == nil {
				p.Transitions = []Edge{}
			}
			for ti := range p.Transitions {
				if p.Transitions[ti].When == "" {
					p.Transitions[ti].When = "phase_complete"
				}
				if p.Transitions[ti].Evidence == nil {
					p.Transitions[ti].Evidence = []Evidence{}
				}
				for ei := range p.Transitions[ti].Evidence {
					normalizeEvidence(&p.Transitions[ti].Evidence[ei])
				}
			}
			for ei := range p.Completion {
				normalizeEvidence(&p.Completion[ei])
			}
		}
	}
	if !yamlPathExists(root, "agent", "output", "verbose_setup") {
		c.Output.VerboseSetup = false
	}
	for i := range c.Tools {
		if c.Tools[i].Recovery != nil {
			r := c.Tools[i].Recovery
			if !yamlPathExists(root, "agent", "tools", fmt.Sprint(i), "recovery", "mode") {
				r.Mode = "reconcile"
			}
			if !yamlPathExists(root, "agent", "tools", fmt.Sprint(i), "recovery", "max_attempts") {
				r.MaxAttempts = 2
			}
		}
	}
	if !yamlPathExists(root, "agent", "name") || !yamlPathExists(root, "agent", "system_prompt") || !yamlPathExists(root, "agent", "model") || !yamlPathExists(root, "agent", "llm", "base_url") || m.MaxTokens <= 0 || m.MaxSteps <= 0 || m.RawTurnsToKeep < 1 {
		return fmt.Errorf("invalid required config fields")
	}
	if math.IsNaN(c.LLM.Temperature) || math.IsInf(c.LLM.Temperature, 0) || c.LLM.TimeoutSeconds <= 0 || c.LLM.MaxOutputTokens != nil && *c.LLM.MaxOutputTokens < 1 || m.ContextWindowTokens <= 0 || m.ReservedOutputTokens <= 0 || m.SafetyMarginTokens < 0 || m.ReservedOutputTokens+m.SafetyMarginTokens >= m.ContextWindowTokens {
		return fmt.Errorf("invalid memory or LLM budget bounds")
	}
	if m.MaxSummaryTokens <= 0 || m.SummaryOutputTokens <= 0 || m.CompactionMaxAttempts < 1 || m.CompactionMaxAttempts > 5 || m.CompactionMaxPasses < 1 || m.CompactionMaxPasses > 10 || m.CompactionMinGrowthTokens < 1 || m.CompactionMinIntervalSteps < 0 || m.OptionalCompactionMinReclaimableTokens < 0 || m.OptionalCompactionMinReductionTokens < 0 {
		return fmt.Errorf("invalid compaction bounds")
	}
	for _, v := range []float64{m.OptionalCompactionMinPressureRatio, m.OptionalCompactionMinReductionRatio} {
		if math.IsNaN(v) || v < 0 || v > 1 {
			return fmt.Errorf("compaction ratios must be in 0..1")
		}
	}
	if e.ToolPolicy != "auto" && e.ToolPolicy != "none" || e.MaxConsecutiveInvalidResponses < 1 || e.MaxConsecutiveInvalidResponses > 50 || e.MaxNoProgressSteps != nil && *e.MaxNoProgressSteps < 2 || m.RunMaxSteps != nil && *m.RunMaxSteps < 1 || m.CompactionIntervalSteps != nil && *m.CompactionIntervalSteps < 1 {
		return fmt.Errorf("invalid execution policy")
	}
	if c.LLM.ToolCallMode != "native" && c.LLM.ToolCallMode != "single" {
		return fmt.Errorf("invalid llm.tool_call_mode")
	}
	return nil
}

func normalizeEvidence(e *Evidence) {
	if e.Arguments == nil {
		e.Arguments = map[string]any{}
	}
	if e.Result == nil {
		e.Result = map[string]any{}
	}
	if e.ContentContains == nil {
		e.ContentContains = []string{}
	}
}

func yamlPathExists(n *yaml.Node, path ...string) bool {
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	for _, key := range path {
		if n.Kind == yaml.SequenceNode {
			index, err := strconv.Atoi(key)
			if err != nil || index < 0 || index >= len(n.Content) {
				return false
			}
			n = n.Content[index]
			continue
		}
		if n.Kind != yaml.MappingNode {
			return false
		}
		found := false
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == key {
				n = n.Content[i+1]
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func canonicalConfigJSON(c Config) ([]byte, error) {
	value, err := canonicalValue(reflect.ValueOf(c))
	if err != nil {
		return nil, err
	}
	v := value.(map[string]any)
	llm := v["llm"].(map[string]any)
	if llm["tool_call_mode"] == "native" {
		delete(llm, "tool_call_mode")
	}
	if llm["max_output_tokens"] == nil {
		delete(llm, "max_output_tokens")
	}
	ex := v["execution"].(map[string]any)
	if ex["tool_policy"] == "auto" {
		delete(ex, "tool_policy")
	}
	if ex["max_no_progress_steps"] == nil {
		delete(ex, "max_no_progress_steps")
	}
	if canonicalArrayLen(ex["argument_bindings"]) == 0 {
		delete(ex, "argument_bindings")
	}
	co := v["completion"].(map[string]any)
	if canonicalArrayLen(co["result_checks"]) == 0 {
		delete(co, "result_checks")
	}
	if co["instructions"] == nil {
		delete(co, "instructions")
	}
	if canonicalArrayLen(co["forbidden_patterns"]) == 0 {
		delete(co, "forbidden_patterns")
	}
	if tools, ok := v["tools"].([]any); ok {
		for _, tool := range tools {
			t, ok := tool.(map[string]any)
			if ok && t["recovery"] == nil {
				delete(t, "recovery")
			}
		}
	}
	m := v["memory"].(map[string]any)
	defaults := map[string]float64{"context_window_tokens": 32768, "reserved_output_tokens": 1024, "safety_margin_tokens": 256, "max_summary_tokens": 4096, "summary_output_tokens": 512, "compaction_max_attempts": 2, "compaction_max_passes": 3, "compaction_min_growth_tokens": 512, "compaction_min_interval_steps": 0, "optional_compaction_min_pressure_ratio": 0, "optional_compaction_min_reclaimable_tokens": 0, "optional_compaction_min_reduction_tokens": 0, "optional_compaction_min_reduction_ratio": 0}
	for k, d := range defaults {
		if n, ok := schemaNumber(m[k]); ok && n == d {
			delete(m, k)
		}
	}
	if m["run_max_steps"] == nil {
		delete(m, "run_max_steps")
	}
	if m["compaction_interval_steps"] == nil {
		delete(m, "compaction_interval_steps")
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err = enc.Encode(v); err != nil {
		return nil, err
	}
	canonical := bytes.TrimSuffix(out.Bytes(), []byte("\n"))
	canonical = bytes.ReplaceAll(canonical, []byte(`\u2028`), []byte("\u2028"))
	canonical = bytes.ReplaceAll(canonical, []byte(`\u2029`), []byte("\u2029"))
	return canonical, nil
}

func canonicalArrayLen(value any) int {
	items, _ := value.([]any)
	return len(items)
}

func canonicalValue(v reflect.Value) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}
		return canonicalValue(v.Elem())
	}
	switch v.Kind() {
	case reflect.Struct:
		out := map[string]any{}
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			tag := strings.Split(field.Tag.Get("json"), ",")[0]
			if tag == "-" {
				continue
			}
			if tag == "" {
				tag = field.Name
			}
			value, err := canonicalValue(v.Field(i))
			if err != nil {
				return nil, err
			}
			out[tag] = value
		}
		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}
		out := map[string]any{}
		iter := v.MapRange()
		for iter.Next() {
			if iter.Key().Kind() != reflect.String {
				return nil, fmt.Errorf("canonical config maps must use string keys")
			}
			value, err := canonicalValue(iter.Value())
			if err != nil {
				return nil, err
			}
			out[iter.Key().String()] = value
		}
		return out, nil
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil, nil
		}
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			value, err := canonicalValue(v.Index(i))
			if err != nil {
				return nil, err
			}
			out[i] = value
		}
		return out, nil
	case reflect.Float32, reflect.Float64:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("nonfinite config number")
		}
		s := strconv.FormatFloat(f, 'g', -1, v.Type().Bits())
		if !strings.ContainsAny(s, ".eE") {
			s += ".0"
		}
		return json.Number(s), nil
	default:
		return v.Interface(), nil
	}
}
func validateConfig(c *Config) error {
	tools := map[string]ToolDefinition{}
	for _, t := range c.Tools {
		if t.Name == "" {
			return fmt.Errorf("tool name is required")
		}
		if _, ok := tools[t.Name]; ok {
			return fmt.Errorf("tool names must be unique")
		}
		tools[t.Name] = t
		if t.Parameters["type"] != "object" {
			return fmt.Errorf("tool %s parameters must be an object schema", t.Name)
		}
		if err := validateSchema(t.Parameters); err != nil {
			return fmt.Errorf("tool %s: %w", t.Name, err)
		}
		if t.Recovery != nil {
			r := t.Recovery
			if r.Mode != "read_only" && r.Mode != "repeatable" && r.Mode != "idempotent" && r.Mode != "reconcile" || r.MaxAttempts < 1 || r.MaxAttempts > 10 {
				return fmt.Errorf("invalid recovery policy for %s", t.Name)
			}
		}
	}
	labels := map[string]bool{}
	for _, r := range c.Completion.ResultChecks {
		if _, ok := tools[r.Tool]; !ok {
			return fmt.Errorf("completion check references unconfigured tool %s", r.Tool)
		}
		if r.Label == "" || labels[r.Label] {
			return fmt.Errorf("completion result labels must be unique")
		}
		labels[r.Label] = true
		if r.Kind != "markdown_table" && r.Kind != "number" && r.Kind != "json_value" {
			return fmt.Errorf("invalid result check kind")
		}
	}
	bindings := map[string]bool{}
	for _, b := range c.Execution.ArgumentBindings {
		t, ok := tools[b.Tool]
		if !ok {
			return fmt.Errorf("argument binding targets unknown tool")
		}
		props, _ := t.Parameters["properties"].(map[string]any)
		if _, ok = props[b.Argument]; !ok || !labels[b.SourceLabel] {
			return fmt.Errorf("argument binding has invalid target or source")
		}
		key := b.Tool + "\x00" + b.Argument
		if bindings[key] {
			return fmt.Errorf("argument binding targets must be unique")
		}
		bindings[key] = true
	}
	for _, p := range c.Completion.ForbiddenPatterns {
		if _, err := regexp.Compile(p); err != nil {
			return err
		}
	}
	if c.Completion.Pattern != nil {
		if _, err := regexp.Compile(*c.Completion.Pattern); err != nil {
			return err
		}
	}
	if c.Completion.Exact != nil && strings.TrimSpace(*c.Completion.Exact) == "" {
		return fmt.Errorf("completion.exact must not be empty")
	}
	if c.Completion.JSONSchema != nil {
		if err := validateSchema(c.Completion.JSONSchema); err != nil {
			return err
		}
	}
	return validateWorkflow(c.Workflow, tools)
}
func validateWorkflow(w *WorkflowConfig, tools map[string]ToolDefinition) error {
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
		refs := append([]Evidence{}, p.Completion...)
		if p.AllowedToolsSet {
			for _, n := range p.AllowedTools {
				if _, ok := tools[n]; !ok {
					return fmt.Errorf("workflow references unknown tool %s", n)
				}
			}
		}
		for _, edge := range p.Transitions {
			if edge.To != "complete" && !seen[edge.To] {
				return fmt.Errorf("workflow transition targets unknown phase %s", edge.To)
			}
			if edge.When == "" {
				edge.When = "phase_complete"
			}
			if edge.When != "phase_complete" && edge.When != "always" && edge.When != "evidence" {
				return fmt.Errorf("invalid transition condition")
			}
			if edge.When == "evidence" && len(edge.Evidence) == 0 {
				return fmt.Errorf("evidence transition requires evidence")
			}
			refs = append(refs, edge.Evidence...)
		}
		for _, ev := range refs {
			if _, ok := tools[ev.Tool]; !ok {
				return fmt.Errorf("workflow references unknown tool %s", ev.Tool)
			}
		}
	}
	return nil
}
func validateSchema(s map[string]any) error {
	return checkSchemaValue(s)
}
