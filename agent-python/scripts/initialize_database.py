"""Explicit schema-6 setup; --reset rebuilds only the known development agent tables."""
import argparse
import asyncio
import os
from pathlib import Path
import sys

import psycopg
from psycopg import sql
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from schema_layout import SCHEMA, tables, validate

DEFINITIONS = [('postgres.sql', 'validate.sql'), ('fixtures.sql', 'validate-fixtures.sql')]
OBSOLETE = ['agent_schema_migrations', 'agent_go_schema_control']


async def initialize(connection, definition, validation):
    expected = tables(definition)
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout = '10s'")
        await connection.execute('SELECT pg_advisory_xact_lock(734910003)')
        cursor = await connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=ANY(%s)", (expected,))
        existing = {row[0] for row in await cursor.fetchall()}
        if existing and existing != set(expected):
            raise RuntimeError('Incomplete agent schema. Use a fresh schema or explicitly run --reset; nothing was migrated.')
        if not existing:
            await connection.execute((SCHEMA / definition).read_text())
        await validate(connection, definition, validation)
        print(('Validated' if existing else 'Created') + ': ' + ', '.join(expected), flush=True)


async def setup(connection, definitions, reset=False):
    allowed = {name for definition, _ in DEFINITIONS for name in tables(definition)}
    cursor = await connection.execute("SELECT current_database(),current_schema()")
    database, schema = await cursor.fetchone()
    if schema is None:
        raise RuntimeError('The selected PostgreSQL search_path has no existing schema')
    print(f'Contract 2.0.0 / schema 6 setup: database={database}, schema={schema}', flush=True)
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout = '10s'")
        await connection.execute('SELECT pg_advisory_xact_lock(734910003)')
        cursor = await connection.execute("SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND left(tablename,6)='agent_'")
        present = {row[0] for row in await cursor.fetchall()}
        unexpected = present - allowed - (set(OBSOLETE) if reset else set())
        if unexpected:
            raise RuntimeError('Unexpected agent tables preserved: ' + ', '.join(sorted(unexpected)))
        if reset:
            owned = {name for definition, _ in definitions for name in tables(definition)} | set(OBSOLETE)
            names = sorted(present & owned)
            if names:
                await connection.execute(sql.SQL('DROP TABLE {}').format(sql.SQL(',').join(sql.Identifier(schema, name) for name in names)))
                print('Reset agent-owned tables: ' + ', '.join(names), flush=True)
        for definition, validation in definitions:
            await initialize(connection, definition, validation)


async def main(reset=False):
    load_dotenv(ROOT / '.env')
    session_url = os.environ['AGENT_SESSION_DATABASE_URL']
    fixture_url = os.getenv('AGENT_FIXTURE_DATABASE_URL') or session_url
    async with await psycopg.AsyncConnection.connect(session_url, autocommit=True) as session:
        if fixture_url == session_url:
            await setup(session, DEFINITIONS, reset)
        else:
            await setup(session, DEFINITIONS[:1], reset)
            async with await psycopg.AsyncConnection.connect(fixture_url, autocommit=True) as fixtures:
                await setup(fixtures, DEFINITIONS[1:], reset)
    print('Schema 6 ready. Agent runtimes must implement contract 2.0.0 before use.', flush=True)
    print('Next: uv run --project agent-python python agent-python/scripts/generate_fixtures.py', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reset', action='store_true', help='Drop and recreate known agent tables in the selected schema(s); stop all agents first')
    asyncio.run(main(parser.parse_args().reset))
