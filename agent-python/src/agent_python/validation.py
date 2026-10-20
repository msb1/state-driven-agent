"""Portable, strict JSON validation for the supported configuration schema subset.

No coercion: strings are never booleans, and booleans are never numbers.
Unsupported assertion keywords are rejected at configuration load time.
"""
from __future__ import annotations

import json
import math
import re
from typing import Any

ASSERTIONS = {'type', 'properties', 'required', 'additionalProperties', 'items', 'enum', 'const',
              'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'minLength',
              'maxLength', 'pattern', 'minItems', 'maxItems'}
ANNOTATIONS = {'description', 'title', 'default', 'examples', '$schema'}
TYPES = {'object', 'array', 'string', 'boolean', 'integer', 'number', 'null'}


def check_schema(schema: dict) -> None:
    if not isinstance(schema, dict):
        raise ValueError('JSON schema must be an object')
    unknown = set(schema) - ASSERTIONS - ANNOTATIONS
    if unknown:
        raise ValueError(f'Unsupported JSON schema keywords: {sorted(unknown)}')
    kind = schema.get('type')
    if kind is not None and (not isinstance(kind, str) or kind not in TYPES):
        raise ValueError('Schema type must be one supported JSON type')
    properties = schema.get('properties', {})
    if not isinstance(properties, dict):
        raise ValueError('Schema properties must be an object')
    for value in properties.values():
        check_schema(value)
    required = schema.get('required', [])
    if not isinstance(required, list) or any(not isinstance(key, str) for key in required):
        raise ValueError('Schema required must be an array of property names')
    extra = schema.get('additionalProperties', True)
    if isinstance(extra, dict):
        check_schema(extra)
    elif type(extra) is not bool:
        raise ValueError('additionalProperties must be boolean or a schema')
    if 'items' in schema:
        check_schema(schema['items'])
    if 'enum' in schema and (not isinstance(schema['enum'], list) or not schema['enum']):
        raise ValueError('Schema enum must be a nonempty array')
    for key in ('minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum'):
        if key in schema and (type(schema[key]) not in (int, float) or (type(schema[key]) is float and not math.isfinite(schema[key]))):
            raise ValueError(f'{key} must be a finite number')
    for key in ('minLength', 'maxLength', 'minItems', 'maxItems'):
        if key in schema and (type(schema[key]) is not int or schema[key] < 0):
            raise ValueError(f'{key} must be a nonnegative integer')
    if 'pattern' in schema:
        if not isinstance(schema['pattern'], str):
            raise ValueError('Schema pattern must be a string')
        try:
            re.compile(schema['pattern'])
        except re.error as error:
            raise ValueError(f'Invalid schema pattern: {error}') from error


def json_equal(actual: Any, expected: Any) -> bool:
    if isinstance(expected, bool) or isinstance(actual, bool):
        return type(actual) is type(expected) and actual == expected
    if isinstance(expected, dict):
        return isinstance(actual, dict) and actual.keys() == expected.keys() and all(json_equal(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(json_equal(a, b) for a, b in zip(actual, expected))
    return actual == expected


def validate_value(value: Any, schema: dict, path: str = '$') -> list[str]:
    numeric = type(value) is int or (type(value) is float and math.isfinite(value))
    matches = {'object': isinstance(value, dict), 'array': isinstance(value, list),
               'string': isinstance(value, str), 'boolean': type(value) is bool,
               'number': numeric, 'integer': numeric and value == int(value), 'null': value is None}
    kind = schema.get('type')
    if kind and not matches[kind]:
        return [f'{path}: expected {kind}, received {type(value).__name__}; supply a JSON {kind} without coercion']
    errors = []
    if 'enum' in schema and not any(json_equal(value, item) for item in schema['enum']):
        errors.append(f'{path}: expected one of {schema["enum"]!r}')
    if 'const' in schema and not json_equal(value, schema['const']):
        errors.append(f'{path}: expected constant {schema["const"]!r}')
    if isinstance(value, dict):
        errors.extend(f'{path}.{key}: required property is missing' for key in schema.get('required', []) if key not in value)
        properties = schema.get('properties', {})
        for key, item in value.items():
            if key in properties:
                errors.extend(validate_value(item, properties[key], f'{path}.{key}'))
            elif schema.get('additionalProperties') is False:
                errors.append(f'{path}.{key}: additional property is forbidden')
            elif isinstance(schema.get('additionalProperties'), dict):
                errors.extend(validate_value(item, schema['additionalProperties'], f'{path}.{key}'))
    if numeric:
        for key, valid in [('minimum', lambda x: value >= x), ('maximum', lambda x: value <= x),
                           ('exclusiveMinimum', lambda x: value > x), ('exclusiveMaximum', lambda x: value < x)]:
            if key in schema and not valid(schema[key]):
                errors.append(f'{path}: violates {key}={schema[key]}')
    if isinstance(value, str):
        if len(value) < schema.get('minLength', 0) or len(value) > schema.get('maxLength', len(value)):
            errors.append(f'{path}: string length is outside configured bounds')
        if 'pattern' in schema and not re.search(schema['pattern'], value):
            errors.append(f'{path}: string does not match configured pattern')
    if isinstance(value, list):
        if len(value) < schema.get('minItems', 0) or len(value) > schema.get('maxItems', len(value)):
            errors.append(f'{path}: array length is outside configured bounds')
        if 'items' in schema:
            for index, item in enumerate(value):
                errors.extend(validate_value(item, schema['items'], f'{path}[{index}]'))
    return errors


def strict_json(text: str) -> Any:
    def invalid_constant(value):
        raise ValueError(f'Nonfinite JSON number: {value}')
    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'Duplicate JSON property: {key}')
            result[key] = value
        return result
    def finite_float(text):
        value = float(text)
        if not math.isfinite(value):
            raise ValueError("JSON number exceeds finite numeric range")
        return value
    return json.loads(text, parse_constant=invalid_constant, parse_float=finite_float, object_pairs_hook=object_pairs)
