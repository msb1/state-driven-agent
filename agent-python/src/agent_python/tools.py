"""Local simulated environment. Tools return strings, including all failures."""
from __future__ import annotations

import json
import re
import os
from collections import Counter
from pathlib import Path
from typing import Any, Awaitable, Callable

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]
IP = re.compile(r"^(?P<ip>(?:\d{1,3}\.){3}\d{1,3})\s+.*?\s(?P<status>\d{3})$")
DEFAULT_DATABASE_URL = "postgresql://user:password@192.168.1.50:5432/elite_rag"
Tool = Callable[[dict[str, Any]], Awaitable[str]]


def _connection() -> psycopg.Connection[Any]:
    load_dotenv(ROOT / ".env")
    return psycopg.connect(os.getenv("AGENT_FIXTURE_DATABASE_URL") or os.getenv("AGENT_SESSION_DATABASE_URL") or DEFAULT_DATABASE_URL)


def _log_lines() -> list[str]:
    with _connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT line FROM agent_fixture_access_logs ORDER BY line_number")
        return [row[0] for row in cursor.fetchall()]


async def inspect_log(args: dict[str, Any]) -> str:
    start, count = int(args["start_line"]), int(args["line_count"])
    lines = _log_lines()
    return "\n".join(f"{index}: {line}" for index, line in enumerate(lines[start - 1:start - 1 + count], start))


async def analyze_500_ips(args: dict[str, Any]) -> str:
    robust = bool(args["robust_parser"])
    counts: Counter[str] = Counter()
    try:
        for line_number, line in enumerate(_log_lines(), 1):
            if robust:
                match = IP.match(line)
                if not match:
                    continue
                if match.group("status") == "500":
                    counts[match.group("ip")] += 1
            else:
                # Deliberately fragile baseline: malformed data raises instead of being hidden.
                fields = line.split(" ")
                if fields[7] == "500":
                    counts[fields[0]] += 1
    except (IndexError, ValueError) as error:
        return f"ERROR: naive parser failed at line {line_number}: {type(error).__name__}: {error}. Try robust_parser=true."
    return json.dumps({"top": counts.most_common(5), "malformed_records_ignored": 10 if robust else 0})


async def validate_top_offending_ip(args: dict[str, Any]) -> str:
    expected = _expected_log_result()
    valid = args["ip"] == expected[0] and int(args["count"]) == expected[1]
    return json.dumps({"valid": valid, "message": "Validated." if valid else "Incorrect; inspect parsing and retry."})


def _expected_log_result() -> tuple[str, int]:
    counts: Counter[str] = Counter()
    for line in _log_lines():
        match = IP.match(line)
        if match and match.group("status") == "500":
            counts[match.group("ip")] += 1
    return counts.most_common(1)[0]


async def inspect_sql_schema(_: dict[str, Any]) -> str:
    with _connection() as conn, conn.cursor() as cursor:
        schemas: dict[str, list[tuple[Any, ...]]] = {}
        for table in ("agent_fixture_customers", "agent_fixture_orders"):
            cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table,))
            schemas[table.removeprefix("agent_fixture_")] = cursor.fetchall()
        cursor.execute("SELECT id, name, city FROM agent_fixture_customers WHERE lower(city) = 'chicago' LIMIT 6")
        examples = cursor.fetchall()
        cursor.execute("SELECT order_id, cust_id, amount, ordered_at FROM agent_fixture_orders WHERE cust_id LIKE 'ID_%%' LIMIT 6")
        bad_keys = cursor.fetchall()
    return json.dumps({"schema": schemas, "chicago_examples": examples, "string_key_examples": bad_keys}, default=str)


async def query_chicago_q1_revenue(args: dict[str, Any]) -> str:
    keys, city = bool(args["normalize_keys"]), bool(args["normalize_city"])
    customer_key = "CAST(REPLACE(o.cust_id, 'ID_', '') AS INTEGER)" if keys else "o.cust_id::INTEGER"
    city_condition = "lower(c.city) = 'chicago'" if city else "c.city = 'Chicago'"
    sql = f"""SELECT ROUND(COALESCE(SUM(o.amount), 0), 2) FROM agent_fixture_orders o
              JOIN agent_fixture_customers c ON c.id = {customer_key}
              WHERE {city_condition} AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01'"""
    with _connection() as conn, conn.cursor() as cursor:
        cursor.execute(sql)
        total = cursor.fetchone()[0]
    return json.dumps({"total": float(total), "normalize_keys": keys, "normalize_city": city})


async def validate_chicago_q1_revenue(args: dict[str, Any]) -> str:
    expected = json.loads(await query_chicago_q1_revenue({"normalize_keys": True, "normalize_city": True}))["total"]
    valid = round(float(args["total"]), 2) == expected
    return json.dumps({"valid": valid, "expected_hint": "Normalize city case and ID_ foreign keys." if not valid else "Validated."})


def _employee_rows() -> list[tuple[int, str, str, str, str]]:
    with _connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT emp_id, name, age, salary, city FROM agent_fixture_employees ORDER BY emp_id")
        return cursor.fetchall()


def _employee_failure(row: int, field: str, value: str) -> str:
    """A deliberately verbose, deterministic failure for compaction testing."""
    window = "\n".join(
        f"  {number:>4} | parsed_{field}[{number}] = {field}_parser(record[{number!r}])"
        for number in range(max(1, row - 8), row + 8)
    )
    return (
        f"ERROR: Phase 1 parser crashed at employee {row}; {field}={value!r}.\n"
        "Traceback (most recent call last):\n"
        "  File \"/simulated/audit.py\", line 84, in parse_employee_batch\n"
        f"    normalized_{field} = {field}_parser(raw_{field})\n"
        f"  File \"/simulated/audit.py\", line 39, in {field}_parser\n"
        f"    raise ValueError('unsupported {field} representation')\n"
        f"ValueError: unsupported {field} representation {value!r}\n"
        "Parser window (not a successful audit):\n"
        f"{window}\n"
        "Revise only the parser assumption exposed by this failure, then retry incrementally."
    )


def _employee_metric_failure(message: str) -> str:
    """Emit assertion detail representative of a failed metric test run."""
    attempts = "\n".join(
        f"  assertion-frame {index:02d}: candidate=[raw salary values]; city predicate and invalid-value filter under test"
        for index in range(1, 33)
    )
    return f"{message}\nMetric test runner context:\n{attempts}\nRevise the stated normalization rule and retry the metric."


async def inspect_employee_audit_schema(_: dict[str, Any]) -> str:
    rows = _employee_rows()
    return json.dumps({
        "schema": {"table": "employees", "columns": ["emp_id INTEGER", "name TEXT", "age TEXT", "salary TEXT", "city TEXT"]},
        "record_count": len(rows),
        "examples": [rows[index] for index in (8, 9, 14, 21, 27)],
        "rules": "age is invalid when NULL or semicolon-delimited; salary is invalid when currency-formatted or UNKNOWN; city matching is case-insensitive.",
    })


async def audit_employee_phase_1(args: dict[str, Any]) -> str:
    parser = args["parser"]
    failures = {
        "naive": (10, "salary", "$80,000"),
        "currency_cleaned": (15, "age", "NULL"),
        "null_cleaned": (22, "age", "41; extra_field_corrupted"),
    }
    if parser in failures:
        return _employee_failure(*failures[parser])
    if parser != "robust":
        return "ERROR: parser must be naive, currency_cleaned, null_cleaned, or robust."
    rows = _employee_rows()
    bad_age = [ident for ident, _, age, _, _ in rows if age == "NULL" or ";" in age]
    bad_salary = [ident for ident, _, _, salary, _ in rows if salary == "UNKNOWN" or salary.startswith("$")]
    return json.dumps({"phase": "Phase 1", "corrupted_age_ids": bad_age, "corrupted_salary_ids": bad_salary,
                       "corrupted_row_ids": sorted(set(bad_age) | set(bad_salary)), "status": "logged"})


async def calculate_new_york_median_salary(args: dict[str, Any]) -> str:
    city_normalized, invalid_excluded = bool(args["normalize_city"]), bool(args["exclude_invalid_salary"])
    if not invalid_excluded:
        return _employee_metric_failure("AssertionError: salary array contains UNKNOWN; filter invalid salary fields before median().")
    if not city_normalized:
        return _employee_metric_failure("AssertionError: Expected median 92500, but your script calculated 71000. Match city case-insensitively.")
    salaries = [float(salary) for _, _, _, salary, city in _employee_rows()
                if city.lower() == "new york" and salary != "UNKNOWN" and not salary.startswith("$")]
    salaries.sort()
    middle = len(salaries) // 2
    median = (salaries[middle - 1] + salaries[middle]) / 2 if len(salaries) % 2 == 0 else salaries[middle]
    return json.dumps({"phase": "Phase 2", "median_salary": median, "valid_new_york_employees": len(salaries), "status": "calculated"})


async def generate_employee_phase_3_report(args: dict[str, Any]) -> str:
    city_normalized, invalid_age_dropped = bool(args["normalize_city"]), bool(args["drop_invalid_age"])
    if not city_normalized:
        return "ERROR: Phase 3 would split New York and new york. Set normalize_city=true."
    if not invalid_age_dropped:
        return "ERROR: Phase 3 cannot average age while NULL and semicolon-corrupted ages remain. Set drop_invalid_age=true."
    groups: dict[str, list[int]] = {}
    for _, _, age, _, city in _employee_rows():
        if age == "NULL" or ";" in age:
            continue
        groups.setdefault(city.title(), []).append(int(age))
    lines = ["Phase 3 — clean city audit", "| City | Valid headcount | Average age |", "| --- | ---: | ---: |"]
    for city in sorted(groups):
        ages = groups[city]
        lines.append(f"| {city} | {len(ages)} | {sum(ages) / len(ages):.2f} |")
    # This is intentionally large enough that the *next* validation action
    # crosses the 2,000-token threshold. The final model response is instructed
    # to present the compact table above, not this diagnostic trace.
    trace = "\n".join(
        f"normalization-check {index:02d}: city=lowercase, age=NULL-or-semicolon excluded, salary=UNKNOWN-or-currency excluded, aggregate=verified"
        for index in range(1, 49)
    )
    return "\n".join(lines) + "\n\nPhase 3 normalization trace (diagnostic evidence):\n" + trace


async def validate_employee_audit(args: dict[str, Any]) -> str:
    valid = (bool(args["phase_1_logged"]) and bool(args["normalize_city"]) and bool(args["drop_invalid_age"])
             and round(float(args["median_salary"]), 2) == 92500.00)
    return json.dumps({"valid": valid, "expected_median_salary": 92500.0,
                       "hint": "Log Phase 1, normalize city casing, and drop invalid ages." if not valid else "Validated all three phases."})


TOOLS: dict[str, Tool] = {
    "inspect_log": inspect_log, "analyze_500_ips": analyze_500_ips, "validate_top_offending_ip": validate_top_offending_ip,
    "inspect_sql_schema": inspect_sql_schema, "query_chicago_q1_revenue": query_chicago_q1_revenue, "validate_chicago_q1_revenue": validate_chicago_q1_revenue,
    "inspect_employee_audit_schema": inspect_employee_audit_schema, "audit_employee_phase_1": audit_employee_phase_1,
    "calculate_new_york_median_salary": calculate_new_york_median_salary,
    "generate_employee_phase_3_report": generate_employee_phase_3_report, "validate_employee_audit": validate_employee_audit,
}


async def execute(name: str, arguments: dict[str, Any]) -> str:
    try:
        tool = TOOLS.get(name)
        if tool is None:
            return f"ERROR: unknown tool '{name}'."
        return await tool(arguments)
    except Exception as error:  # boundary: errors always become context, never server crashes
        return f"ERROR: {type(error).__name__}: {error}"
