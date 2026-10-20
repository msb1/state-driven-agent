"""Create the current Python session tables in the configured existing database.

Uses the runtime schema initializer as the single source of table definitions.
For a fresh database there is no historical data to migrate or copy. Fixture
creation/regeneration is a separate generate_fixtures.py command.
"""
import asyncio

import psycopg

from agent_python.config import ConfigRepository
from agent_python.persistence import SessionRepository


async def main():
    ConfigRepository()  # Load the repository .env without exposing credentials.
    repository = SessionRepository()
    async with await psycopg.AsyncConnection.connect(repository.database_url) as connection:
        cursor = await connection.execute('SELECT current_database(), current_schema()')
        database, schema = await cursor.fetchone()
    print(f'Initializing session tables: database={database}, schema={schema}', flush=True)
    await repository.initialize()
    async with await psycopg.AsyncConnection.connect(repository.database_url) as connection:
        cursor = await connection.execute('''SELECT tablename FROM pg_catalog.pg_tables
            WHERE schemaname=%s AND tablename LIKE 'agent_%%' ORDER BY tablename''', (schema,))
        tables = [row[0] for row in await cursor.fetchall()]
        cursor = await connection.execute('''SELECT a.attname FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=%s AND c.relname='agent_sessions' AND a.attnum>0 AND NOT a.attisdropped
            ORDER BY a.attnum''', (schema,))
        columns = [row[0] for row in await cursor.fetchall()]
    print('Current session schema initialized (version 5). No data copied from another database.')
    print('Tables: ' + ', '.join(tables))
    print('agent_sessions columns: ' + ', '.join(columns))
    print('Next: run scripts/generate_fixtures.py to create/regenerate the four test fixture tables.')


if __name__ == '__main__':
    asyncio.run(main())
