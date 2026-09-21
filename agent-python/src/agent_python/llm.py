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

    async def compact(
        self,
        goal: str,
        historical: list[Message],
        existing: str | None,
        hybrid_reflection: bool = True,
    ) -> str:
        if hybrid_reflection:
            prompt = """You are the hybrid compaction and reflection engine for an autonomous agent.

Analyze the historical prefix only. The primary agent separately preserves its recent raw
working buffer, so do not reproduce, summarize, truncate, or invent raw messages here.
Extract durable environmental constraints and lessons as imperative operational rules.
Keep dead ends precise and short. Preserve only verified facts; mark uncertainty instead
of promoting guesses to rules. Return exactly this structure and no surrounding prose:

<COMPACTED_STATE>
<USER_GOAL>
Restate the original goal without changing its parameters or definitions.
</USER_GOAL>
<GLOBAL_LESSON_LEDGER>
- Imperative rules for permanent constraints or discoveries; none if no verified lessons.
</GLOBAL_LESSON_LEDGER>
<DEAD_ENDS>
- One-sentence failed approaches and why they failed; none if no verified dead ends.
</DEAD_ENDS>
<CURRENT_LOCAL_PIVOT>
State the most important active hypothesis or next operational focus in one sentence.
</CURRENT_LOCAL_PIVOT>
</COMPACTED_STATE>

The raw working buffer is retained by the primary agent outside this response."""
        else:
            prompt = """Create a compacted context state for an agent. Preserve only verified facts.
Use exactly these headings:
- Core Objective
- Universal Truths Discovered
- Dead Ends
- Current Local Pivot
Do not invent facts. The original goal is protected separately. Do not include raw history."""
        source = "\n".join(f"{m.role}: {m.content or m.tool_calls or ''}" for m in historical)
        messages = [
            Message(role="system", content=prompt),
            Message(
                role="user",
                content=f"Goal: {goal}\nPrevious ledger: {existing or 'none'}\nHistorical prefix:\n{source}",
            ),
        ]
        try:
            result = await self._post({"model": self.config.model, "messages": [m.model_dump() for m in messages], "temperature": 0})
            return result["choices"][0]["message"].get("content") or self._fallback_ledger(goal, historical, hybrid_reflection)
        except (httpx.HTTPError, KeyError, IndexError):
            return self._fallback_ledger(goal, historical, hybrid_reflection)

    async def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        urls = self._completion_urls()
        async with httpx.AsyncClient(timeout=self.config.llm.timeout_seconds, headers=self.headers) as client:
            last_response: httpx.Response | None = None
            for url in urls:
                response = await client.post(url, json=payload)
                if response.status_code != httpx.codes.NOT_FOUND or url == urls[-1]:
                    response.raise_for_status()
                    return response.json()
                # Some local OpenAI-compatible servers expose the route at the
                # root while others use the conventional /v1 prefix. A 404 is
                # safe to retry at the alternate route; all other errors are
                # returned immediately with their original status and body.
                last_response = response
            assert last_response is not None
            last_response.raise_for_status()
            return last_response.json()

    def _completion_urls(self) -> list[str]:
        """Return the configured endpoint and a root-route compatibility fallback.

        ``base_url`` is normally an API root such as ``.../v1``. Accepting an
        exact ``.../chat/completions`` URL too makes configuration less brittle.
        The fallback covers local servers that implement the same OpenAI payload
        at ``/chat/completions`` instead of ``/v1/chat/completions``.
        """
        base = self.config.llm.base_url.rstrip("/")
        suffix = "/chat/completions"
        if base.endswith(suffix):
            return [base]

        primary = base + suffix
        if base.endswith("/v1"):
            return [primary, base[:-3] + suffix]
        return [primary]

    @staticmethod
    def _fallback_ledger(goal: str, historical: list[Message], hybrid_reflection: bool = True) -> str:
        if hybrid_reflection:
            return (
                "<COMPACTED_STATE>\n"
                f"<USER_GOAL>\n{goal}\n</USER_GOAL>\n"
                "<GLOBAL_LESSON_LEDGER>\n- No verified global lessons extracted.\n</GLOBAL_LESSON_LEDGER>\n"
                "<DEAD_ENDS>\n- No verified dead ends extracted; inspect the retained raw working buffer.\n</DEAD_ENDS>\n"
                "<CURRENT_LOCAL_PIVOT>\nReview the retained raw working buffer and continue from the latest verified state.\n"
                "</CURRENT_LOCAL_PIVOT>\n</COMPACTED_STATE>"
            )
        text = " ".join(message.content or str(message.tool_calls or "") for message in historical)[-1800:]
        return f"- Core Objective: {goal}\n- Universal Truths Discovered: none verified\n- Dead Ends: {text}\n- Current Local Pivot: Review retained raw turns."
