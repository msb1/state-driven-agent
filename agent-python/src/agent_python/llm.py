"""Minimal async client for any OpenAI-compatible chat-completions endpoint."""
from __future__ import annotations

import json
import os
from typing import Any

import httpx

from .config import AgentConfig
from .models import Message


class LlmClient:
    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        key = os.getenv(config.llm.api_key_env, "local-not-required")
        self.headers = {"Authorization": f"Bearer {key}"}

    async def decide(self, messages: list[Message]) -> dict[str, Any]:
        payload = {
            "model": self.config.model,
            "messages": [message.model_dump(exclude_none=True) for message in messages],
            "tools": [{"type": "function", "function": tool.model_dump()} for tool in self.config.tools],
            "tool_choice": "auto",
            "temperature": self.config.llm.temperature,
        }
        response = await self._post(payload)
        choice = response["choices"][0]["message"]
        if choice.get("tool_calls"):
            call = choice["tool_calls"][0]
            try:
                arguments = json.loads(call["function"]["arguments"])
            except json.JSONDecodeError as error:
                return {"type": "final", "content": f"LLM emitted invalid tool arguments: {error}"}
            return {"type": "tool", "id": call.get("id", "call_1"), "name": call["function"]["name"], "arguments": arguments}
        return {"type": "final", "content": choice.get("content") or "No final answer was returned."}

    async def compact(self, goal: str, historical: list[Message], existing: str | None) -> str:
        prompt = """Create a compacted context state for an agent. Preserve only verified facts.
Use exactly these headings:
- Core Objective
- Universal Truths Discovered
- Dead Ends
- Current Local Pivot
Do not invent facts. The original goal is protected separately."""
        source = "\n".join(f"{m.role}: {m.content}" for m in historical)
        messages = [Message(role="system", content=prompt), Message(role="user", content=f"Goal: {goal}\nPrevious ledger: {existing or 'none'}\nHistory:\n{source}")]
        try:
            result = await self._post({"model": self.config.model, "messages": [m.model_dump() for m in messages], "temperature": 0})
            return result["choices"][0]["message"].get("content") or self._fallback_ledger(goal, historical)
        except (httpx.HTTPError, KeyError, IndexError):
            return self._fallback_ledger(goal, historical)

    async def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        url = self.config.llm.base_url.rstrip("/") + "/chat/completions"
        async with httpx.AsyncClient(timeout=self.config.llm.timeout_seconds, headers=self.headers) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return response.json()

    @staticmethod
    def _fallback_ledger(goal: str, historical: list[Message]) -> str:
        text = " ".join(message.content for message in historical)[-1800:]
        return f"- Core Objective: {goal}\n- Universal Truths Discovered: none verified\n- Dead Ends: {text}\n- Current Local Pivot: Review retained raw turns."
