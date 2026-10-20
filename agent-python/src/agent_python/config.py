"""YAML configuration loader with a tiny ${VAR:-default} interpolator."""
from __future__ import annotations

import os
import re
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator
from dotenv import dotenv_values, load_dotenv

from .validation import check_schema

_ENV = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")
_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


class LlmConfig(BaseModel):
    base_url: str
    api_key_env: str = "OPENAI_API_KEY"
    temperature: float = 0.1
    timeout_seconds: float = Field(default=90, gt=0)
    tool_call_mode: Literal["native", "single"] = "native"
    max_output_tokens: int | None = Field(default=None, ge=1)


class MemoryConfig(BaseModel):
    max_tokens: int = Field(gt=0)
    max_steps: int = Field(gt=0)
    raw_turns_to_keep: int = Field(ge=1)
    hybrid_reflection: bool = True
    run_max_steps: int | None = Field(default=None, ge=1)
    compaction_interval_steps: int | None = Field(default=None, ge=1)
    context_window_tokens: int = Field(default=32768, ge=1)
    reserved_output_tokens: int = Field(default=1024, ge=1)
    safety_margin_tokens: int = Field(default=256, ge=0)
    max_summary_tokens: int = Field(default=4096, ge=1)
    summary_output_tokens: int = Field(default=512, ge=1)
    compaction_max_attempts: int = Field(default=2, ge=1, le=5)
    compaction_max_passes: int = Field(default=3, ge=1, le=10)
    compaction_min_growth_tokens: int = Field(default=512, ge=1)
    compaction_min_interval_steps: int = Field(default=0, ge=0)
    optional_compaction_min_pressure_ratio: float = Field(default=0, ge=0, le=1)
    optional_compaction_min_reclaimable_tokens: int = Field(default=0, ge=0)
    optional_compaction_min_reduction_tokens: int = Field(default=0, ge=0)
    optional_compaction_min_reduction_ratio: float = Field(default=0, ge=0, le=1)

    @model_validator(mode="after")
    def valid_budget(self):
        if self.reserved_output_tokens + self.safety_margin_tokens >= self.context_window_tokens:
            raise ValueError("Context window must exceed reserved output and safety margin")
        return self


class OutputConfig(BaseModel):
    """Controls optional, non-sensitive lifecycle events in SSE responses."""

    verbose_setup: bool = False


class ArgumentBindingConfig(BaseModel):
    tool: str = Field(min_length=1)
    argument: str = Field(min_length=1)
    source_label: str = Field(min_length=1)
    path: list[str | int] = Field(default_factory=list)


class ExecutionConfig(BaseModel):
    argument_bindings: list[ArgumentBindingConfig] = Field(default_factory=list)
    tool_policy: Literal["auto", "none"] = "auto"
    max_consecutive_invalid_responses: int = Field(default=3, ge=1, le=50)
    max_no_progress_steps: int | None = Field(default=None, ge=2)


class ResultCheckConfig(BaseModel):
    kind: Literal["markdown_table", "number", "json_value"]
    tool: str = Field(min_length=1)
    arguments: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] = Field(default_factory=dict)
    path: list[str | int] = Field(default_factory=list)
    answer_path: list[str | int] = Field(default_factory=list)
    label: str = Field(min_length=1)


class CompletionConfig(BaseModel):
    instructions: str | None = None
    forbidden_patterns: list[str] = Field(default_factory=list)
    result_checks: list[ResultCheckConfig] = Field(default_factory=list)
    exact: str | None = None
    pattern: str | None = None
    json_schema: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_checks(self):
        for pattern in self.forbidden_patterns:
            try:
                re.compile(pattern)
            except re.error as error:
                raise ValueError(f"Invalid forbidden completion pattern: {error}") from error
        labels = [check.label for check in self.result_checks]
        if len(labels) != len(set(labels)):
            raise ValueError("Completion result labels must be unique")
        if self.exact is not None and not self.exact.strip():
            raise ValueError("completion.exact must not be empty")
        if self.pattern is not None:
            try:
                re.compile(self.pattern)
            except re.error as error:
                raise ValueError(f"Invalid completion pattern: {error}") from error
        if self.json_schema is not None:
            check_schema(self.json_schema)
        return self


class ToolRecoveryConfig(BaseModel):
    """Recovery is an environment capability, never a model-supplied argument."""
    mode: Literal['read_only', 'repeatable', 'idempotent', 'reconcile'] = 'reconcile'
    max_attempts: int = Field(default=2, ge=1, le=10)


class ToolConfig(BaseModel):
    name: str = Field(min_length=1)
    description: str
    parameters: dict[str, Any]
    recovery: ToolRecoveryConfig | None = None

    @model_validator(mode="after")
    def validate_parameters(self):
        check_schema(self.parameters)
        if self.parameters.get("type") != "object":
            raise ValueError("Tool parameters must have type object")
        return self


class ToolEvidenceConfig(BaseModel):
    """A durable fact produced by one successful or failed tool invocation."""

    description: str
    tool: str
    required: bool = True
    arguments: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] = Field(default_factory=dict)
    content_contains: list[str] = Field(default_factory=list)


class PhaseTransitionConfig(BaseModel):
    """An edge in the optional workflow graph.

    ``phase_complete`` supports the current linear workflows. ``evidence`` and
    ``always`` deliberately reserve branch/skip behavior for later configs.
    """

    to: str
    when: Literal["phase_complete", "evidence", "always"] = "phase_complete"
    evidence: list[ToolEvidenceConfig] = Field(default_factory=list)


class WorkflowPhaseConfig(BaseModel):
    id: str
    name: str
    instruction: str
    # Optional guardrail restricting tools while this phase is active.
    allowed_tools: list[str] | None = None
    completion: list[ToolEvidenceConfig] = Field(default_factory=list)
    transitions: list[PhaseTransitionConfig] = Field(default_factory=list)


class WorkflowConfig(BaseModel):
    entry_phase: str
    phases: list[WorkflowPhaseConfig]

    @model_validator(mode="after")
    def validate_graph(self) -> "WorkflowConfig":
        phase_ids = [phase.id for phase in self.phases]
        if len(phase_ids) != len(set(phase_ids)):
            raise ValueError("workflow phase IDs must be unique")
        if self.entry_phase not in phase_ids:
            raise ValueError("workflow.entry_phase must name a configured phase")
        for phase in self.phases:
            for transition in phase.transitions:
                if transition.to != "complete" and transition.to not in phase_ids:
                    raise ValueError(f"workflow transition from {phase.id} targets unknown phase {transition.to}")
                if transition.when == "evidence" and not transition.evidence:
                    raise ValueError(f"workflow evidence transition from {phase.id} requires evidence")
        return self


class AgentConfig(BaseModel):
    name: str
    system_prompt: str
    model: str
    llm: LlmConfig
    memory: MemoryConfig
    output: OutputConfig = Field(default_factory=OutputConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    completion: CompletionConfig = Field(default_factory=CompletionConfig)
    tools: list[ToolConfig]
    # Omit this for the legacy one-task behavior used by existing agents.
    workflow: WorkflowConfig | None = None


    @model_validator(mode="after")
    def unique_tools(self):
        names = [tool.name for tool in self.tools]
        if len(names) != len(set(names)):
            raise ValueError("Tool names must be unique")
        for check in self.completion.result_checks:
            if check.tool not in names:
                raise ValueError(f"Completion check references unconfigured tool {check.tool}")
        labels = {check.label for check in self.completion.result_checks}
        binding_keys = set()
        for binding in self.execution.argument_bindings:
            declaration = next((tool for tool in self.tools if tool.name == binding.tool), None)
            if declaration is None or binding.argument not in declaration.parameters.get("properties", {}):
                raise ValueError("Argument binding must target a configured tool parameter")
            if binding.source_label not in labels:
                raise ValueError("Argument binding must reference a completion result label")
            key = (binding.tool, binding.argument)
            if key in binding_keys:
                raise ValueError("Argument binding targets must be unique")
            binding_keys.add(key)
        if self.workflow:
            for phase in self.workflow.phases:
                referenced = set(phase.allowed_tools or []) | {item.tool for item in phase.completion}
                for transition in phase.transitions:
                    referenced.update(item.tool for item in transition.evidence)
                if referenced - set(names):
                    raise ValueError(f"Workflow phase {phase.id} references unconfigured tools: {sorted(referenced - set(names))}")
        return self


def resolved_config_hash(config: AgentConfig) -> str:
    import json
    value = config.model_dump(mode="json")
    # Preserve existing milestone-1 anchors when additive provider capabilities
    # are omitted/defaulted. Nondefault policies remain part of the identity.
    if value["llm"].get("tool_call_mode") == "native":
        value["llm"].pop("tool_call_mode", None)
    if value["llm"].get("max_output_tokens") is None:
        value["llm"].pop("max_output_tokens", None)
    if value["execution"].get("tool_policy") == "auto":
        value["execution"].pop("tool_policy", None)
    if value["execution"].get("max_no_progress_steps") is None:
        value["execution"].pop("max_no_progress_steps", None)
    if not value["completion"].get("result_checks"):
        value["completion"].pop("result_checks", None)
    if not value["execution"].get("argument_bindings"):
        value["execution"].pop("argument_bindings", None)
    if value["completion"].get("instructions") is None:
        value["completion"].pop("instructions", None)
    if not value["completion"].get("forbidden_patterns"):
        value["completion"].pop("forbidden_patterns", None)
    for tool in value['tools']:
        if tool.get('recovery') is None:
            tool.pop('recovery', None)
    memory_defaults = {"run_max_steps": None, "compaction_interval_steps": None,
        "context_window_tokens": 32768, "reserved_output_tokens": 1024, "safety_margin_tokens": 256,
        "max_summary_tokens": 4096, "summary_output_tokens": 512, "compaction_max_attempts": 2,
        "compaction_max_passes": 3, "compaction_min_growth_tokens": 512,
        "compaction_min_interval_steps": 0, "optional_compaction_min_pressure_ratio": 0,
        "optional_compaction_min_reclaimable_tokens": 0, "optional_compaction_min_reduction_tokens": 0,
        "optional_compaction_min_reduction_ratio": 0}
    for key, default in memory_defaults.items():
        if value["memory"].get(key) == default:
            value["memory"].pop(key, None)
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ConfigSnapshot:
    """An immutable configuration selected for one session."""

    ref: str
    sha256: str
    config: AgentConfig


class ConfigRepository:
    """Resolve trusted, versioned YAML workflow configs from one directory.

    A caller supplies a relative reference, never an arbitrary server path. The
    resolved config is validated and hashed when a session is created, allowing
    new YAML workflows to be added without restarting the API process.
    """

    def __init__(self, root: str | Path | None = None) -> None:
        _load_environment()
        default_root = Path(__file__).resolve().parents[3] / "config"
        self.root = Path(root or os.getenv("AGENT_CONFIG_ROOT", default_root)).resolve()

    def resolve(self, ref: str) -> ConfigSnapshot:
        if not ref or Path(ref).is_absolute():
            raise ValueError("config_ref must be a non-empty relative YAML path")
        candidate = (self.root / ref).resolve(strict=True)
        try:
            candidate.relative_to(self.root)
        except ValueError as error:
            raise ValueError("config_ref must remain within AGENT_CONFIG_ROOT") from error
        if not candidate.is_file() or candidate.suffix.lower() not in {".yaml", ".yml"}:
            raise ValueError("config_ref must identify a YAML file within AGENT_CONFIG_ROOT")
        raw = candidate.read_bytes()
        try:
            config = _load_raw_config(raw.decode("utf-8"), candidate)
        except (UnicodeDecodeError, yaml.YAMLError, ValidationError, ValueError) as error:
            raise ValueError(f"invalid YAML workflow config {ref}: {error}") from error
        return ConfigSnapshot(ref=candidate.relative_to(self.root).as_posix(), sha256=hashlib.sha256(raw).hexdigest(), config=config)

    def list(self) -> list[ConfigSnapshot]:
        if not self.root.is_dir():
            raise ValueError(f"AGENT_CONFIG_ROOT does not exist or is not a directory: {self.root}")
        snapshots: list[ConfigSnapshot] = []
        for path in sorted((*self.root.rglob("*.yaml"), *self.root.rglob("*.yml"))):
            try:
                snapshots.append(self.resolve(path.relative_to(self.root).as_posix()))
            except (OSError, ValueError):
                # Invalid configs are surfaced when explicitly requested; they
                # do not prevent unrelated immutable profiles from being used.
                continue
        return snapshots


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        return _ENV.sub(lambda match: os.getenv(match.group(1), match.group(2) or ""), value)
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    return value


def _load_raw_config(text: str, config_path: Path) -> AgentConfig:
    raw = yaml.safe_load(text)
    if not raw or "agent" not in raw:
        raise ValueError(f"{config_path} must contain an 'agent' mapping")
    return AgentConfig.model_validate(_expand(raw["agent"]))


def load_config(path: str | Path | None = None) -> AgentConfig:
    _load_environment()
    config_path = Path(path or os.getenv("STATE_DRIVEN_CONFIG", "config/log-analysis.yaml"))
    if not config_path.is_absolute():
        # Support both `uv run` from agent-python and a repository-root server command.
        candidates = [Path.cwd() / config_path, *[parent / config_path for parent in Path(__file__).resolve().parents]]
        config_path = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
    return _load_raw_config(config_path.read_text(encoding="utf-8"), config_path)


def _load_environment() -> None:
    """Load defaults; the repository .env owns MAIN_MODEL selection."""
    load_dotenv(_ENV_FILE)
    main_model = dotenv_values(_ENV_FILE).get("MAIN_MODEL")
    if main_model is not None:
        os.environ["MAIN_MODEL"] = main_model
