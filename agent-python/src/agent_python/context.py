"""Deterministic request budgets, complete interaction boundaries, and typed ledgers."""
from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal
from xml.etree import ElementTree as ET

from pydantic import BaseModel, Field
from .models import Message
from .model_errors import ModelServiceError


class Ledger(BaseModel):
    goal: str
    model_claims: list[str] = Field(default_factory=list)
    dead_ends: list[str] = Field(default_factory=list)
    hypothesis: str
    source_sha256: str
    verification: str = 'unverified_model_summary'


def estimate_tokens(payload):
    # Deliberately conservative, reproducible byte-based estimate, not a tokenizer.
    return len(json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')) + 32 * len(payload.get('messages', []))


def budget(payload, memory):
    estimate = estimate_tokens(payload)
    reserved = max(memory.reserved_output_tokens, payload.get('max_tokens') or 0)
    return {'estimated_input_tokens': estimate, 'reserved_output_tokens': reserved,
            'safety_margin_tokens': memory.safety_margin_tokens, 'context_window_tokens': memory.context_window_tokens,
            'fits': estimate + reserved + memory.safety_margin_tokens <= memory.context_window_tokens,
            'estimator': 'UTF-8 compact JSON bytes + 32 per message; conservative estimate, not provider usage'}


def require_budget(payload, memory):
    measured = budget(payload, memory)
    if not measured['fits']:
        raise ModelServiceError('context_budget_exceeded', 'Request exceeds configured context budget; no model request was sent.', category='context', details=measured)
    return measured


def interaction_groups(messages):
    groups = []
    index = 0
    while index < len(messages):
        message = messages[index]
        if message.role == 'assistant' and message.tool_calls:
            ids = [call.get('id') for call in message.tool_calls]
            results = messages[index + 1:index + 1 + len(ids)]
            if (any(not isinstance(value, str) or not value for value in ids) or len(set(ids)) != len(ids)
                    or len(results) != len(ids) or any(result.role != 'tool' for result in results)
                    or {result.tool_call_id for result in results} != set(ids)):
                raise ModelServiceError('context_integrity_error', 'Incomplete tool interaction in memory; preserve history and repair it before continuing.', category='context')
            groups.append([message, *results])
            index += 1 + len(ids)
        elif message.role == 'tool':
            raise ModelServiceError('context_integrity_error', 'Orphaned tool result in memory; no model request was sent.', category='context')
        else:
            groups.append([message])
            index += 1
    return groups


def split_history(messages, minimum_messages):
    groups = interaction_groups(messages)
    count, boundary = 0, len(groups)
    while boundary and count < minimum_messages:
        boundary -= 1
        count += len(groups[boundary])
    return groups[:boundary], [message for group in groups[boundary:] for message in group]


def source_text(goal, historical, existing):
    return f'Goal: {goal}\nPrevious ledger: {existing or "none"}\nHistorical prefix:\n' + json.dumps(
        [message.wire_payload() for message in historical], ensure_ascii=False, sort_keys=True)


def render_ledger(ledger, hybrid=True):
    sections = [ledger.goal, '\n'.join(ledger.model_claims) or '- None.', '\n'.join(ledger.dead_ends) or '- None.', ledger.hypothesis]
    if not hybrid:
        names = ['Core Objective', 'Universal Truths Discovered', 'Dead Ends', 'Current Local Pivot']
        return '\n'.join(f'- {name}:\n{value}' for name, value in zip(names, sections))
    root = ET.Element('COMPACTED_STATE')
    for name, value in zip(['USER_GOAL', 'GLOBAL_LESSON_LEDGER', 'DEAD_ENDS', 'CURRENT_LOCAL_PIVOT'], sections):
        ET.SubElement(root, name).text = '\n' + value + '\n'
    return ET.tostring(root, encoding='unicode')


def numbers(text):
    return {Decimal(value.replace(',', '')) for value in re.findall(r'(?<![\w.,+-])[-+]?\d+(?:,\d{3})*(?:\.\d+)?(?:[eE][-+]?\d+)?(?!\w|[.,]\d)', text)}


def validate_ledger(text, goal, source, max_tokens, hybrid=True):
    if not isinstance(text, str) or not text.strip():
        raise ValueError('Summary is empty')
    if len(text.encode()) > max_tokens:
        raise ValueError('Summary exceeds ledger budget')
    if hybrid:
        if '<!' in text or '<?' in text:
            raise ValueError('XML declarations/entities/comments are forbidden')
        try:
            root = ET.fromstring(text.strip())
        except ET.ParseError as error:
            raise ValueError('Summary must be one complete COMPACTED_STATE document') from error
        names = ['USER_GOAL', 'GLOBAL_LESSON_LEDGER', 'DEAD_ENDS', 'CURRENT_LOCAL_PIVOT']
        if root.tag != 'COMPACTED_STATE' or root.attrib or [child.tag for child in root] != names or (root.text or '').strip():
            raise ValueError('Summary sections must occur exactly once in the required order')
        if any(child.attrib or len(child) or (child.tail or '').strip() for child in root):
            raise ValueError('Summary sections must contain only text')
        sections = [(child.text or '').strip() for child in root]
    else:
        names = ['Core Objective', 'Universal Truths Discovered', 'Dead Ends', 'Current Local Pivot']
        pattern = r'(?m)^- (' + '|'.join(map(re.escape, names)) + r'):\s*'
        matches = list(re.finditer(pattern, text))
        if [match.group(1) for match in matches] != names or text[:matches[0].start()].strip():
            raise ValueError('Summary must contain the four required ordered headings')
        sections = [text[match.end():matches[i + 1].start() if i + 1 < len(matches) else len(text)].strip() for i, match in enumerate(matches)]
    if any(not section for section in sections) or sections[0] != goal.strip():
        raise ValueError('Summary must preserve the original goal exactly and populate each section')
    if not numbers('\n'.join(sections)).issubset(numbers(source)):
        raise ValueError('Summary introduces numeric claims absent from its source')
    ledger = Ledger(goal=goal, model_claims=sections[1].splitlines(), dead_ends=sections[2].splitlines(),
                    hypothesis=sections[3], source_sha256=hashlib.sha256(source.encode()).hexdigest())
    canonical = render_ledger(ledger, hybrid)
    if len(canonical.encode()) > max_tokens:
        raise ValueError('Canonical ledger exceeds its budget')
    return ledger, canonical
