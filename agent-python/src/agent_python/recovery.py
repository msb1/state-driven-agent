"""Trusted adapter capabilities and authoritative verification outcomes."""
from dataclasses import dataclass
from typing import Awaitable, Callable


class RecipientConflict(Exception):
    pass


@dataclass(frozen=True)
class IdempotentAdapter:
    target: str
    invoke: Callable[[dict, str], Awaitable[str]]
    key_scope: str
    retention_seconds: float | None  # None is an explicit indefinite guarantee.

    def __post_init__(self):
        if not self.target or not self.key_scope or (self.retention_seconds is not None and self.retention_seconds <= 0):
            raise ValueError('Recipient target, key scope and positive retention guarantee are required')


@dataclass(frozen=True)
class Verification:
    outcome: str  # succeeded, failed, not_executed, unknown
    evidence: dict
    result: str | None = None
    external_reference: str | None = None

    def __post_init__(self):
        if self.outcome not in ('succeeded','failed','not_executed','unknown') or not self.evidence:
            raise ValueError('Verification requires a defined outcome and authoritative evidence')
        if self.outcome in ('succeeded','failed') and not isinstance(self.result,str):
            raise ValueError('A confirmed verification needs its original string result')
        if self.outcome in ('succeeded','failed') and (self.outcome=='failed') != self.result.startswith(('ERROR:','AssertionError:')):
            raise ValueError('Verified status must match the adapter string-result error convention')
