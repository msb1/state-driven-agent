"""Generic deliverable checks derived from executed tool results, not fixture constants."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re

from .validation import strict_json, json_equal


def table_rows(text):
    """First contiguous Markdown table; retain headers, reject duplicate row keys."""
    rows = []
    for line in text.splitlines():
        if line.strip().startswith('|') and line.strip().endswith('|'):
            cells = [cell.strip() for cell in line.strip()[1:-1].split('|')]
            if all(re.fullmatch(r':?-{3,}:?', cell) for cell in cells):
                continue
            rows.append(cells)
        elif rows:
            break
    if len(rows) < 2 or any(len(row) != len(rows[0]) for row in rows):
        raise ValueError('A header and complete data rows are required')
    keys = [row[0] for row in rows[1:]]
    if len(keys) != len(set(keys)):
        raise ValueError('Duplicate table row keys')
    return rows


def equivalent_cell(actual, expected):
    if re.fullmatch(r"[-+]?\d+", expected) and not re.fullmatch(r"[-+]?\d+", actual):
        return False
    try:
        a, b = Decimal(actual), Decimal(expected)
        return a.is_finite() and b.is_finite() and a == b
    except InvalidOperation:
        return actual == expected


def table_matches(answer, wanted):
    try:
        actual = table_rows(answer)
    except ValueError:
        return False
    if actual[0] != wanted[0]:
        return False
    rows = {row[0]: row[1:] for row in actual[1:]}
    expected = {row[0]: row[1:] for row in wanted[1:]}
    return rows.keys() == expected.keys() and all(len(rows[key]) == len(values) and
        all(equivalent_cell(a, b) for a, b in zip(rows[key], values)) for key, values in expected.items())


def extract_result(check, output):
    if check.kind == 'markdown_table':
        return table_rows(output)
    value = strict_json(output)
    for part in check.path:
        if not isinstance(value, (dict, list)):
            raise ValueError('Invalid result path')
        value = value[part]
    if check.kind == 'number':
        if type(value) not in (int, float) or not Decimal(str(value)).is_finite():
            raise ValueError('Result is not a finite number')
    return value


def check_answer(check, answer, value):
    if check.kind == 'markdown_table':
        return table_matches(answer, value)
    if check.kind == 'json_value':
        try:
            actual = strict_json(answer)
            for part in check.answer_path:
                actual = actual[part]
            return json_equal(actual, value)
        except (ValueError, TypeError, KeyError, IndexError):
            return False
    matches = re.findall(re.escape(check.label) + r'\s*\$?\s*([-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)(?![\d,]|\.\d)', answer)
    return len(matches) == 1 and Decimal(matches[0].replace(',', '')) == Decimal(str(value))


def describe_artifact(check, value):
    if check.kind == 'markdown_table':
        return '\n'.join(['| ' + ' | '.join(row) + ' |' for row in [value[0], ['---'] * len(value[0]), *value[1:]]])
    if check.kind == 'number':
        return f'{check.label} {value}'
    import json
    return f'Expected JSON value at {check.answer_path}: {json.dumps(value)}'
