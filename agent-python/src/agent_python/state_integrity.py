"""Integrity anchors for the bounded hot snapshot and its active projection.

These digests are stored with the snapshot, independently of the transcript
rows used during hydration. They detect a changed row before its values become
the next write's accepted baseline, without loading archived history.
"""
from __future__ import annotations

import hashlib
import hmac
import json


INTEGRITY_KEY = "_active_integrity"


def digest(value):
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def snapshot_digest(snapshot):
    return digest({key: value for key, value in snapshot.items() if key != INTEGRITY_KEY})


def context_digest(state):
    return digest([[message.model_dump(mode="json") for message in state.memory],
                   state.compacted_context, state.ledger_state])


def integrity_anchor(snapshot, state):
    return {"version": 1, "snapshot_sha256": snapshot_digest(snapshot),
            "context_sha256": context_digest(state)}


def validate_snapshot(snapshot):
    anchor = snapshot.get(INTEGRITY_KEY)
    if (not isinstance(anchor, dict) or type(anchor.get("version")) is not int
            or anchor["version"] != 1):
        raise ValueError("Active projection integrity anchor is missing; automatic backfill is disabled")
    expected = anchor.get("snapshot_sha256")
    if not isinstance(expected, str) or not hmac.compare_digest(expected, snapshot_digest(snapshot)):
        raise ValueError("Persisted snapshot differs from its integrity anchor")


def validate_context(snapshot, state):
    expected = snapshot[INTEGRITY_KEY].get("context_sha256")
    if not isinstance(expected, str) or not hmac.compare_digest(expected, context_digest(state)):
        raise ValueError("Active transcript or ledger differs from its integrity anchor")
