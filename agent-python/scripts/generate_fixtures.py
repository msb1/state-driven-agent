"""Populate the initialized shared fixture tables with deterministic test data."""
from __future__ import annotations

import asyncio
import random
import sys
import os
from datetime import date, timedelta
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.types.json import Jsonb
from psycopg import sql

# scripts/ lives directly below agent-python/.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent / 'scripts'))
from schema_layout import validate
from idempotency_contract import vectors

SEED = 20260918


def make_log(rng=None) -> list[str]:
    rng = rng or random.Random(SEED)
    ips = ["10.0.0.5", "192.168.1.1", "172.16.4.21", "203.0.113.9", "198.51.100.14"]
    methods, routes, codes = ["GET", "POST", "PUT"], ["/home", "/api/v2", "/checkout", "/reports"], [200, 201, 302, 400, 404, 500, 503]
    lines: list[str] = []
    for number in range(640):
        ip = "10.0.0.5" if number % 9 == 0 else rng.choice(ips)
        status = 500 if number % 9 == 0 or rng.random() < 0.12 else rng.choice(codes[:-2])
        lines.append(f'{ip} - - [18/Sep/2026:12:{number % 60:02d}:00 +0000] "{rng.choice(methods)} {rng.choice(routes)} HTTP/1.1" {status}')
    malformed = [
        "MALFORMED_ROW_NO_IP_HERE ERROR_CRASH", "", "10.0.0.9 incomplete", "not-an-ip - - [bad] broken",
        '192.168.1.3 - - [18/Sep/2026] "GET /missing-status HTTP/1.1"',
    ]
    for index in range(10):
        lines.insert(25 + index * 57, malformed[index % len(malformed)])
    return lines


def business_rows(lines=None):
    rng = random.Random(SEED)
    generated_log = make_log(rng)
    lines = generated_log if lines is None else lines
    cities = ["Chicago", "chicago", "CHICAGO", "New York", "Austin", "Seattle", "Chicago "]
    customers = [(ident, f"Customer {ident}", cities[ident % len(cities)]) for ident in range(1, 2201)]
    customers[0], customers[1] = (1, "Alice", "Chicago"), (2, "Bob", "chicago")
    orders = [(101, '1', '150.00', '2026-02-15'), (102, 'ID_002', '90.00', '2026-03-02')]
    for order_id in range(103, 6603):
        customer = rng.randint(1, 2200)
        key = f"ID_{customer:03d}" if order_id % 5 == 0 else str(customer)
        amount = round(rng.uniform(5, 750), 2)
        ordered = date(2025, 10, 1) + timedelta(days=rng.randrange(365))
        orders.append((order_id, key, f"{amount:.2f}", ordered.isoformat()))
    base_cities = ("New York", "Chicago", "Austin", "Seattle")
    employees = []
    for emp_id in range(1, 1001):
        age = "NULL" if emp_id % 15 == 0 else ("41; extra_field_corrupted" if emp_id % 22 == 0 else str(22 + emp_id % 43))
        salary = "UNKNOWN" if emp_id % 18 == 0 else (f"${75_000 + (emp_id % 9) * 5_000:,}" if emp_id % 10 == 0 else "92500")
        city = "new york" if emp_id % 7 == 0 else base_cities[(emp_id - 1) % len(base_cities)]
        employees.append((emp_id, f"Emp_{emp_id}", age, salary, city))
    return {
        'agent_fixture_access_logs': list(enumerate(lines, 1)),
        'agent_fixture_customers': customers,
        'agent_fixture_orders': orders,
        'agent_fixture_employees': employees,
    }


async def make_database(lines=None, database_url=None):
    load_dotenv(ROOT.parent / '.env')
    database_url = database_url or os.getenv('AGENT_FIXTURE_DATABASE_URL') or os.getenv('AGENT_SESSION_DATABASE_URL')
    if not database_url:
        raise RuntimeError('Set AGENT_SESSION_DATABASE_URL or AGENT_FIXTURE_DATABASE_URL in repository .env')
    records = business_rows(lines)
    identity_vectors = vectors()['actions']
    records['agent_fixture_idempotency_vectors'] = [
        (v['vector_id'], v['namespace'], v['session_id'], v['execution_id'], v['action_type'], v['target'],
         Jsonb(v['arguments']), v['expected_key'], v['expected_payload_sha256'], v['canonical_key'], v['canonical_payload'])
        for v in identity_vectors]
    async with await psycopg.AsyncConnection.connect(database_url) as conn:
        await validate(conn, 'fixtures.sql', 'validate-fixtures.sql')
        await conn.execute(sql.SQL('TRUNCATE {}').format(sql.SQL(',').join(sql.Identifier(table) for table in records)))
        async with conn.cursor() as cursor:
            for table, rows in records.items():
                query = sql.SQL('INSERT INTO {} VALUES ({})').format(sql.Identifier(table), sql.SQL(',').join(sql.Placeholder() for _ in rows[0]))
                await cursor.executemany(query, rows)
    return {table: len(rows) for table, rows in records.items()}


if __name__ == '__main__':
    counts = asyncio.run(make_database())
    print('Generated fixtures: ' + ', '.join(f'{table}={count}' for table, count in counts.items()))
    print('Existing recipient receipts and all session/command data were preserved.')
