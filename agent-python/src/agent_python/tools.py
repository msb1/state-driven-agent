"""Local simulated environment. Tools return strings, including all failures."""
from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Awaitable, Callable

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "agent-python" / "data"
IP = re.compile(r"^(?P<ip>(?:\d{1,3}\.){3}\d{1,3})\s+.*?\s(?P<status>\d{3})$")
Tool = Callable[[dict[str, Any]], Awaitable[str]]


async def inspect_log(args: dict[str, Any]) -> str:
    start, count = int(args["start_line"]), int(args["line_count"])
    lines = (DATA / "access.log").read_text(encoding="utf-8").splitlines()
    return "\n".join(f"{index}: {line}" for index, line in enumerate(lines[start - 1:start - 1 + count], start))


async def analyze_500_ips(args: dict[str, Any]) -> str:
    robust = bool(args["robust_parser"])
    counts: Counter[str] = Counter()
    try:
        for line_number, line in enumerate((DATA / "access.log").read_text(encoding="utf-8").splitlines(), 1):
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
    for line in (DATA / "access.log").read_text(encoding="utf-8").splitlines():
        match = IP.match(line)
        if match and match.group("status") == "500":
            counts[match.group("ip")] += 1
    return counts.most_common(1)[0]


async def inspect_sql_schema(_: dict[str, Any]) -> str:
    with sqlite3.connect(DATA / "commerce.sqlite3") as conn:
        schemas = {table: conn.execute(f"PRAGMA table_info({table})").fetchall() for table in ("customers", "orders")}
        examples = conn.execute("SELECT id, name, city FROM customers WHERE lower(city) = 'chicago' LIMIT 6").fetchall()
        bad_keys = conn.execute("SELECT order_id, cust_id, amount, ordered_at FROM orders WHERE typeof(cust_id) = 'text' LIMIT 6").fetchall()
    return json.dumps({"schema": schemas, "chicago_examples": examples, "string_key_examples": bad_keys}, default=str)


async def query_chicago_q1_revenue(args: dict[str, Any]) -> str:
    keys, city = bool(args["normalize_keys"]), bool(args["normalize_city"])
    customer_key = "CAST(REPLACE(o.cust_id, 'ID_', '') AS INTEGER)" if keys else "o.cust_id"
    city_condition = "lower(c.city) = 'chicago'" if city else "c.city = 'Chicago'"
    sql = f"""SELECT ROUND(COALESCE(SUM(o.amount), 0), 2) FROM orders o
              JOIN customers c ON c.id = {customer_key}
              WHERE {city_condition} AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01'"""
    with sqlite3.connect(DATA / "commerce.sqlite3") as conn:
        total = conn.execute(sql).fetchone()[0]
    return json.dumps({"total": total, "normalize_keys": keys, "normalize_city": city})


async def validate_chicago_q1_revenue(args: dict[str, Any]) -> str:
    expected = json.loads(await query_chicago_q1_revenue({"normalize_keys": True, "normalize_city": True}))["total"]
    valid = round(float(args["total"]), 2) == expected
    return json.dumps({"valid": valid, "expected_hint": "Normalize city case and ID_ foreign keys." if not valid else "Validated."})


TOOLS: dict[str, Tool] = {
    "inspect_log": inspect_log, "analyze_500_ips": analyze_500_ips, "validate_top_offending_ip": validate_top_offending_ip,
    "inspect_sql_schema": inspect_sql_schema, "query_chicago_q1_revenue": query_chicago_q1_revenue, "validate_chicago_q1_revenue": validate_chicago_q1_revenue,
}


async def execute(name: str, arguments: dict[str, Any]) -> str:
    try:
        tool = TOOLS.get(name)
        if tool is None:
            return f"ERROR: unknown tool '{name}'."
        return await tool(arguments)
    except Exception as error:  # boundary: errors always become context, never server crashes
        return f"ERROR: {type(error).__name__}: {error}"
