"""Shared RFC 8785 oracle and pinned vectors; independent of agent packages."""
import hashlib
import json
from pathlib import Path
from uuid import UUID

import rfc8785

CONTRACT = '2.0.0'
SCHEMA_VERSION = 6


def canonical(value):
    return rfc8785.dumps(value)


def _digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def action_key(namespace, session_id, execution_id):
    return _digest(['agent-action-v1', namespace, str(UUID(session_id)), str(UUID(execution_id))])


def payload_fingerprint(action_type, target, arguments):
    return _digest(['agent-payload-v1', action_type, target, arguments])


def request_fingerprint(method, route, request):
    return _digest(['agent-command-v1', method, route, request])


def strict_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate JSON property: ' + key)
            result[key] = value
        return result
    def invalid(value):
        raise ValueError('Invalid JSON number: ' + value)
    value = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)
    canonical(value)
    return value


def vectors():
    data = strict_json((Path(__file__).resolve().parents[1] / 'schema' / 'idempotency-vectors.json').read_text())
    for vector in data['actions']:
        key_preimage = canonical(['agent-action-v1', vector['namespace'], str(UUID(vector['session_id'])), str(UUID(vector['execution_id']))]).decode()
        payload_preimage = canonical(['agent-payload-v1', vector['action_type'], vector['target'], vector['arguments']]).decode()
        if (vector['canonical_key'] != key_preimage or vector['canonical_payload'] != payload_preimage
                or vector['expected_key'] != action_key(vector['namespace'], vector['session_id'], vector['execution_id'])
                or vector['expected_payload_sha256'] != payload_fingerprint(vector['action_type'], vector['target'], vector['arguments'])):
            raise ValueError('Action vector mismatch: ' + vector['vector_id'])
    for vector in data['commands']:
        preimage = canonical(['agent-command-v1', vector['method'], vector['route'], vector['request']]).decode()
        if vector['canonical_request'] != preimage or vector['expected_sha256'] != request_fingerprint(vector['method'], vector['route'], vector['request']):
            raise ValueError('Command vector mismatch: ' + vector['vector_id'])
    return data
