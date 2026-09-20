"""YAML configuration loader with a tiny ${VAR:-default} interpolator."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from dotenv import load_dotenv

_ENV = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


class LlmConfig(BaseModel):
    base_url: str
    api_key_env: str = "OPENAI_API_KEY"
    temperature: float = 0.1
    timeout_seconds: float = 90


class MemoryConfig(BaseModel):
    max_tokens: int = Field(gt=0)
    max_steps: int = Field(gt=0)
    raw_turns_to_keep: int = Field(ge=1)


class ToolConfig(BaseModel):
    name: str
    description: str
    parameters: dict[str, Any]


class AgentConfig(BaseModel):
    name: str
    system_prompt: str
    model: str
    llm: LlmConfig
    memory: MemoryConfig
    tools: list[ToolConfig]


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        return _ENV.sub(lambda match: os.getenv(match.group(1), match.group(2) or ""), value)
    if isinstance(value, list):
        return [_expand(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    return value


def load_config(path: str | Path | None = None) -> AgentConfig:
    # A root .env supplies shared defaults without overriding process-level secrets.
    load_dotenv(Path(__file__).resolve().parents[3] / ".env")
    config_path = Path(path or os.getenv("STATE_DRIVEN_CONFIG", "config/agent.yaml"))
    if not config_path.is_absolute():
        # Support both `uv run` from agent-python and a repository-root server command.
        candidates = [Path.cwd() / config_path, *[parent / config_path for parent in Path(__file__).resolve().parents]]
        config_path = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not raw or "agent" not in raw:
        raise ValueError(f"{config_path} must contain an 'agent' mapping")
    return AgentConfig.model_validate(_expand(raw["agent"]))
