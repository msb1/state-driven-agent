"""Create deterministic, intentionally messy fixtures for the simulated tests."""
from __future__ import annotations

import random
import os
from datetime import date, timedelta
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# scripts/ lives directly below agent-python/.
ROOT = Path(__file__).resolve().parents[1]
RNG = random.Random(20260918)
DEFAULT_DATABASE_URL = "postgresql://user:password@192.168.1.50:5432/elite_rag"


def make_log() -> list[str]:
    ips = ["10.0.0.5", "192.168.1.1", "172.16.4.21", "203.0.113.9", "198.51.100.14"]
    methods, routes, codes = ["GET", "POST", "PUT"], ["/home", "/api/v2", "/checkout", "/reports"], [200, 201, 302, 400, 404, 500, 503]
    lines: list[str] = []
    for number in range(640):
        ip = "10.0.0.5" if number % 9 == 0 else RNG.choice(ips)
        status = 500 if number % 9 == 0 or RNG.random() < 0.12 else RNG.choice(codes[:-2])
        lines.append(f'{ip} - - [18/Sep/2026:12:{number % 60:02d}:00 +0000] "{RNG.choice(methods)} {RNG.choice(routes)} HTTP/1.1" {status}')
    malformed = [
        "MALFORMED_ROW_NO_IP_HERE ERROR_CRASH", "", "10.0.0.9 incomplete", "not-an-ip - - [bad] broken",
        '192.168.1.3 - - [18/Sep/2026] "GET /missing-status HTTP/1.1"',
    ]
    for index in range(10):
        lines.insert(25 + index * 57, malformed[index % len(malformed)])
    return lines


def make_database(lines: list[str]) -> None:
    """Atomically replace only the agent-owned PostgreSQL fixture tables."""
    load_dotenv(ROOT.parent / ".env")
    database_url = os.getenv("AGENT_FIXTURE_DATABASE_URL") or os.getenv("AGENT_SESSION_DATABASE_URL") or DEFAULT_DATABASE_URL
    with psycopg.connect(database_url) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS agent_fixture_access_logs (
                    line_number INTEGER PRIMARY KEY, line TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_fixture_customers (
                    id INTEGER PRIMARY KEY, name TEXT NOT NULL, city TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_fixture_orders (
                    order_id INTEGER PRIMARY KEY, cust_id TEXT NOT NULL, amount NUMERIC(12, 2) NOT NULL, ordered_at DATE NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_fixture_employees (
                    emp_id INTEGER PRIMARY KEY, name TEXT NOT NULL, age TEXT NOT NULL, salary TEXT NOT NULL, city TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS agent_fixture_orders_customer_date ON agent_fixture_orders(cust_id, ordered_at);
                CREATE INDEX IF NOT EXISTS agent_fixture_employees_city ON agent_fixture_employees(city);
        """)
        cursor.execute("TRUNCATE agent_fixture_access_logs, agent_fixture_customers, agent_fixture_orders, agent_fixture_employees")
        cursor.executemany("INSERT INTO agent_fixture_access_logs(line_number, line) VALUES (%s, %s)", enumerate(lines, 1))
        cities = ["Chicago", "chicago", "CHICAGO", "New York", "Austin", "Seattle", "Chicago "]
        customers = [(ident, f"Customer {ident}", cities[ident % len(cities)]) for ident in range(1, 2201)]
        customers[0], customers[1] = (1, "Alice", "Chicago"), (2, "Bob", "chicago")
        cursor.executemany("INSERT INTO agent_fixture_customers VALUES (%s, %s, %s)", customers)
        orders: list[tuple[int, int | str, float, str]] = [(101, 1, 150.0, "2026-02-15"), (102, "ID_002", 90.0, "2026-03-02")]
        for order_id in range(103, 6603):
            customer = RNG.randint(1, 2200)
            key: int | str = f"ID_{customer:03d}" if order_id % 5 == 0 else customer
            amount = round(RNG.uniform(5, 750), 2)
            ordered = date(2025, 10, 1) + timedelta(days=RNG.randrange(365))
            orders.append((order_id, key, amount, ordered.isoformat()))
        cursor.executemany("INSERT INTO agent_fixture_orders VALUES (%s, %s, %s, %s)", [(order_id, str(key), amount, ordered) for order_id, key, amount, ordered in orders])

        # Test Case 3 stores all audit inputs as text. Its 1..1000 ID range
        # resolves the source scenario's conflict between 200 IDs and 1,000 rows.
        base_cities = ("New York", "Chicago", "Austin", "Seattle")
        employees: list[tuple[int, str, str, str, str]] = []
        for emp_id in range(1, 1001):
            # NULL takes precedence where the age patterns overlap; UNKNOWN
            # does the same where the salary patterns overlap.
            age = "NULL" if emp_id % 15 == 0 else ("41; extra_field_corrupted" if emp_id % 22 == 0 else str(22 + emp_id % 43))
            salary = "UNKNOWN" if emp_id % 18 == 0 else (f"${75_000 + (emp_id % 9) * 5_000:,}" if emp_id % 10 == 0 else "92500")
            city = "new york" if emp_id % 7 == 0 else base_cities[(emp_id - 1) % len(base_cities)]
            employees.append((emp_id, f"Emp_{emp_id}", age, salary, city))
        cursor.executemany("INSERT INTO agent_fixture_employees VALUES (%s, %s, %s, %s, %s)", employees)


if __name__ == "__main__":
    log_lines = make_log()
    make_database(log_lines)
    print("Generated 650 PostgreSQL log rows (including 10 malformed), 2,200 customers / 6,500 orders, and 1,000 corrupted employees.")
