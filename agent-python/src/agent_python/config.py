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
from dotenv import load_dotenv

_ENV = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")
_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


class LlmConfig(BaseModel):
    base_url: str
    api_key_env: str = "OPENAI_API_KEY"
    temperature: float = 0.1
    timeout_seconds: float = 90


class MemoryConfig(BaseModel):
    max_tokens: int = Field(gt=0)
    max_steps: int = Field(gt=0)
    raw_turns_to_keep: int = Field(ge=1)
    hybrid_reflection: bool = True


class OutputConfig(BaseModel):
    """Controls optional, non-sensitive lifecycle events in SSE responses."""

    verbose_setup: bool = False


class ToolConfig(BaseModel):
    name: str
    description: str
    parameters: dict[str, Any]


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
    tools: list[ToolConfig]
    # Omit this for the legacy one-task behavior used by existing agents.
    workflow: WorkflowConfig | None = None


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
    config_path = Path(path or os.getenv("STATE_DRIVEN_CONFIG", "config/test-case-1.yaml"))
    if not config_path.is_absolute():
        # Support both `uv run` from agent-python and a repository-root server command.
        candidates = [Path.cwd() / config_path, *[parent / config_path for parent in Path(__file__).resolve().parents]]
        config_path = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
    return _load_raw_config(config_path.read_text(encoding="utf-8"), config_path)


def _load_environment() -> None:
    """Load repository defaults before resolving session-selected YAML profiles."""
    load_dotenv(_ENV_FILE)
