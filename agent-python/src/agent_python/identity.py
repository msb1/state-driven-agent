"""Contract 2.0.0 canonical identities and strict JSON input."""

import hashlib
import json
from uuid import UUID

import rfc8785

CONTRACT = "2.0.0"
SCHEMA_VERSION = 6


def canonical(value):
    return rfc8785.dumps(value)


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def action_key(namespace, session_id, execution_id):
    return digest(["agent-action-v1", namespace, str(UUID(session_id)), str(UUID(execution_id))])


def payload_fingerprint(action_type, target, arguments):
    return digest(["agent-payload-v1", action_type, target, arguments])


def request_fingerprint(method, route, request):
    return digest(["agent-command-v1", method, route, request])


def strict_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate JSON property: {key}")
            result[key] = value
        return result

    def invalid(value):
        raise ValueError(f"Invalid JSON number: {value}")

    value = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)
    canonical(value)
    return value

