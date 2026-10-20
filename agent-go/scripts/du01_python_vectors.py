"""Print Python contract 1.1.0 config identity vectors for DU-01-C01 comparison."""
from __future__ import annotations

import hashlib
import json
import os

from agent_python.config import ConfigRepository, resolved_config_hash


def preimage(config) -> str:
    value = config.model_dump(mode="json")
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
    for tool in value["tools"]:
        if tool.get("recovery") is None:
            tool.pop("recovery", None)
    defaults = {
        "run_max_steps": None, "compaction_interval_steps": None,
        "context_window_tokens": 32768, "reserved_output_tokens": 1024,
        "safety_margin_tokens": 256, "max_summary_tokens": 4096,
        "summary_output_tokens": 512, "compaction_max_attempts": 2,
        "compaction_max_passes": 3, "compaction_min_growth_tokens": 512,
        "compaction_min_interval_steps": 0,
        "optional_compaction_min_pressure_ratio": 0,
        "optional_compaction_min_reclaimable_tokens": 0,
        "optional_compaction_min_reduction_tokens": 0,
        "optional_compaction_min_reduction_ratio": 0,
    }
    for key, default in defaults.items():
        if value["memory"].get(key) == default:
            value["memory"].pop(key, None)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


repo = ConfigRepository()
# Use a synthetic common environment for C01; C03 separately checks dotenv precedence.
os.environ["MAIN_MODEL"] = "du01-synthetic-model"
os.environ["OPENAI_BASE_URL"] = "https://synthetic.invalid/v1"
for snapshot in repo.list():
    source = (repo.root / snapshot.ref).read_bytes()
    canonical = preimage(snapshot.config)
    resolved_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    assert resolved_hash == resolved_config_hash(snapshot.config)
    print(json.dumps({
        "ref": snapshot.ref,
        "raw_sha256": hashlib.sha256(source).hexdigest(),
        "resolved_sha256": resolved_hash,
        "resolved_preimage": canonical,
        "config": snapshot.config.model_dump(mode="json"),
    }, sort_keys=True, ensure_ascii=False))
