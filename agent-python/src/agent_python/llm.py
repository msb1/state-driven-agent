"""Minimal async client for any OpenAI-compatible chat-completions endpoint."""
from __future__ import annotations

import json
import os
import copy
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from .config import AgentConfig
from .models import Message
from .validation import strict_json
from .context import require_budget, source_text
from .model_errors import ModelServiceError, classify_exchange, exception_diagnostic


class LlmClient:
    _transport = None  # Optional injected HTTP transport; production defaults to network.

    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self.request_diagnostics: list[dict] = []
        self.compaction_feedback: str | None = None
        key = os.getenv(config.llm.api_key_env, "local-not-required")
        self.headers = {"Authorization": f"Bearer {key}"}

    def decision_payload(self, messages: list[Message]) -> dict[str, Any]:
        payload = {
            "model": self.config.model,
            "messages": self._wire_messages(messages),
            "tools": [{"type": "function", "function": tool.model_dump(exclude={'recovery'})} for tool in self.config.tools],
            "tool_choice": "auto",
            "temperature": self.config.llm.temperature,
        }
        if self.config.execution.tool_policy == "none":
            payload.pop("tools")
            payload["tool_choice"] = "none"
            payload["messages"].insert(0, {"role": "system", "content": "Tools are disabled for this task. Return the requested final answer directly."})
        elif self.config.llm.tool_call_mode == "single":
            payload["parallel_tool_calls"] = False
            payload["messages"].insert(0, {"role": "system", "content": "Call at most one configured function per response. Workflow phase labels are not function names."})
        payload["max_tokens"] = self.config.llm.max_output_tokens or self.config.memory.reserved_output_tokens
        return payload

    async def decide(self, messages: list[Message]) -> dict[str, Any]:
        response = await self._post(self.decision_payload(messages))
        try:
            choice = response["choices"][0]["message"]
            if not isinstance(choice, dict):
                raise ValueError("message must be an object")
        except (KeyError, IndexError, TypeError, ValueError):
            return {"type": "recoverable_error", "code": "invalid_model_response", "content": "Model response must contain choices[0].message."}
        calls = choice.get("tool_calls")
        if calls:
            if not isinstance(calls, list):
                return {"type": "recoverable_error", "code": "invalid_tool_calls", "content": "tool_calls must be an array."}
            normalized = []
            for index, call in enumerate(calls):
                item = {"id": None, "name": "invalid_tool_call", "arguments": None}
                try:
                    if isinstance(call.get("id"), str) and call["id"]:
                        item["id"] = call["id"]
                    function = call["function"]
                    item["name"] = function["name"]
                    item["raw_arguments"] = function["arguments"]
                    item["arguments"] = strict_json(function["arguments"])
                    if not isinstance(item["arguments"], dict):
                        raise ValueError("tool arguments must be a JSON object")
                    if not isinstance(item["name"], str) or not item["name"]:
                        raise ValueError("tool name must be a nonempty string")
                except (AttributeError, KeyError, TypeError, ValueError) as error:
                    item["error"] = f"Invalid tool call: {error}. Supply a configured function and JSON object arguments."
                    if not isinstance(item["name"], str) or not item["name"]:
                        item["name"] = "invalid_tool_call"
                normalized.append(item)
            return {"type": "tools", "calls": normalized}
        content = choice.get("content")
        if not isinstance(content, str) or not content.strip():
            return {"type": "recoverable_error", "code": "empty_model_response", "content": "Return a nonempty final answer or a valid tool call.",
                    "candidate_content": content if isinstance(content, str) else None}
        return {"type": "final", "content": content}

    def compaction_payload(self, goal, historical, existing, hybrid_reflection=True):
        if hybrid_reflection:
            structure = "<COMPACTED_STATE><USER_GOAL>...</USER_GOAL><GLOBAL_LESSON_LEDGER>...</GLOBAL_LESSON_LEDGER><DEAD_ENDS>...</DEAD_ENDS><CURRENT_LOCAL_PIVOT>...</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>"
        else:
            structure = "- Core Objective:\n...\n- Universal Truths Discovered:\n...\n- Dead Ends:\n...\n- Current Local Pivot:\n..."
        prompt = ("Summarize only the supplied historical prefix and previous ledger. Copy the original goal exactly. "
                  "Do not reconstruct recent raw messages. Carry forward prior useful lessons and dead ends. "
                  "Keep provisional hypotheses separate from observations; do not present guesses as verified facts. "
                  "Use only numbers appearing in the source. Do not calculate or invent statistics. "
                  "Fill every section, using '- None.' if empty. No prose outside the structure. "
                  "Escape XML special characters when using the XML format. "
                  f"Keep output short, within {self.config.memory.max_summary_tokens} UTF-8 bytes. Return exactly: " + structure)
        if self.compaction_feedback:
            prompt += " Previous replacement rejected: " + self.compaction_feedback
        return {"model": self.config.model, "messages": [{"role": "system", "content": prompt},
                {"role": "user", "content": source_text(goal, historical, existing)}], "temperature": 0,
                "max_tokens": self.config.memory.summary_output_tokens}

    async def compact(self, goal, historical, existing, hybrid_reflection=True):
        result = await self._post(self.compaction_payload(goal, historical, existing, hybrid_reflection))
        content = None
        try:
            choice = result["choices"][0]
            content = choice["message"]["content"]
            if choice.get("finish_reason") == "length" or not isinstance(content, str) or not content.strip():
                raise ValueError("Empty or truncated summary")
            return content
        except (KeyError, IndexError, TypeError, ValueError) as error:
            failure = ModelServiceError("invalid_summary", "Summarizer returned an empty, malformed, or truncated response.", category="context")
            # Archive available model text privately; public diagnostics remain bounded labels.
            failure.summary_proposal = content if isinstance(content, str) else None
            raise failure from error

    def _wire_messages(self, messages: list[Message]) -> list[dict]:
        raw = [message.wire_payload() for message in messages]
        if self.config.llm.tool_call_mode == "native":
            return raw
        wire = []
        index = 0
        while index < len(raw):
            message = raw[index]
            calls = message.get("tool_calls") if message.get("role") == "assistant" else None
            if calls:
                results = raw[index + 1:index + 1 + len(calls)]
                ids = [call.get("id") for call in calls]
                if (len(set(ids)) != len(ids) or len(results) != len(calls)
                        or any(result.get("role") != "tool" for result in results)
                        or {result.get("tool_call_id") for result in results} != set(ids)):
                    raise ModelServiceError("context_integrity_error", "Tool-call history is incomplete; no model request was sent.", category="context")
                by_id = {result["tool_call_id"]: result for result in results}
                for position, call in enumerate(calls):
                    assistant = copy.deepcopy(message)
                    assistant["tool_calls"] = [copy.deepcopy(call)]
                    if position:
                        assistant.pop("content", None)
                    wire.extend([assistant, copy.deepcopy(by_id[call["id"]])])
                index += 1 + len(calls)
                continue
            if message.get("role") == "tool":
                raise ModelServiceError("context_integrity_error", "Orphaned tool result in retained history; no model request was sent.", category="context")
            wire.append(copy.deepcopy(message))
            index += 1
        # Split only the wire representation. The session's original batch and
        # result strings, IDs, order, execution decisions, and evidence stay intact.
        return wire

    async def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        request_budget = require_budget(payload, self.config.memory)
        urls = self._completion_urls()
        async with httpx.AsyncClient(timeout=self.config.llm.timeout_seconds, headers=self.headers, transport=self._transport) as client:
            last_response: httpx.Response | None = None
            for url in urls:
                started = time.monotonic()
                metadata = {"budget": request_budget, "started_at": datetime.now(timezone.utc).isoformat(), "kind": "decision" if "tool_choice" in payload else "compaction",
                            "message_count": len(payload.get("messages", [])),
                            "serialized_request_bytes": len(json.dumps(payload, ensure_ascii=False).encode()),
                            "tool_call_mode": self.config.llm.tool_call_mode,
                            "timeout_seconds": self.config.llm.timeout_seconds,
                            "max_output_tokens": payload.get("max_tokens")}
                try:
                    response = await client.post(url, json=payload)
                except httpx.RequestError as error:
                    elapsed = time.monotonic() - started
                    failure = exception_diagnostic(error)
                    details = {**failure.get("details", {}), "request": metadata, "elapsed_seconds": elapsed}
                    self.request_diagnostics.append({**metadata, "elapsed_seconds": elapsed, "failure": failure})
                    raise ModelServiceError(failure["code"], failure["message"], category=failure["category"], details=details) from error
                try:
                    body = response.json()
                except ValueError:
                    body = None
                failure = classify_exchange(response.status_code, body)
                self.request_diagnostics.append({**metadata, "elapsed_seconds": time.monotonic() - started,
                                                 "http_status": response.status_code, "failure": failure})
                if response.status_code != httpx.codes.NOT_FOUND or url == urls[-1]:
                    if failure:
                        failure["details"] = {**failure["details"], "request": metadata, "elapsed_seconds": time.monotonic() - started}
                        raise ModelServiceError(**failure)
                    if body is None:
                        raise ModelServiceError("invalid_model_response", "Model service returned invalid JSON.", category="response", http_status=response.status_code)
                    return body
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

