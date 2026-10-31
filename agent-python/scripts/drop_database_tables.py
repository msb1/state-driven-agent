"""Inventory or explicitly drop user tables in the configured session database.

This utility defaults to dry-run. Destruction requires both --execute and an
exact --confirm-database name. It never drops schemas or PostgreSQL system
objects and does not use CASCADE.
"""
import argparse
import asyncio
import os
from pathlib import Path

import psycopg
from psycopg import sql
from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[2]
EXCLUDED_SCHEMAS = ('pg_catalog', 'information_schema')


async def list_tables(connection):
    query = """SELECT n.nspname, c.relname, c.relispartition
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','p')
          AND n.nspname NOT IN (%s,%s)
          AND n.nspname NOT LIKE 'pg_toast%%'
          AND n.nspname NOT LIKE 'pg_temp_%%'
          AND n.nspname NOT LIKE 'pg_toast_temp_%%'
        ORDER BY n.nspname, c.relname"""
    cursor = await connection.execute(query, EXCLUDED_SCHEMAS)
    return await cursor.fetchall()


async def main(args):
    env = {**dotenv_values(ROOT / '.env'), **os.environ}
    database_url = env.get('AGENT_SESSION_DATABASE_URL')
    if not database_url:
        raise RuntimeError('AGENT_SESSION_DATABASE_URL is not set in .env or the environment')

    async with await psycopg.AsyncConnection.connect(database_url) as connection:
        cursor = await connection.execute('SELECT current_database()')
        database = (await cursor.fetchone())[0]
        if args.execute and args.confirm_database != database:
            raise RuntimeError(
                f'Confirmation mismatch: connected to {database!r}; '
                'pass --confirm-database with that exact name')

        tables = await list_tables(connection)

        print(f'Database: {database}')
        print(f'Tables found: {len(tables)}')
        for schema, name, is_partition in tables:
            suffix = ' (partition)' if is_partition else ''
            print(f'  {schema}.{name}{suffix}')

        if not args.execute:
            print('Dry run only. No tables were changed. To drop this inventory, '
                  f'rerun with --execute --confirm-database {database}')
            return

        # DROP TABLE without CASCADE fails safely if an object outside this
        # inventory depends on a selected table. Partition leaves are removed
        # with their partitioned parent; independent/inherited tables are listed.
        drop_targets = [(schema, name) for schema, name, is_partition in tables if not is_partition]
        if not drop_targets:
            print('No tables to drop.')
            return
        async with connection.transaction():
            await connection.execute('SELECT pg_advisory_xact_lock(734910003)')
            await connection.execute(sql.SQL('DROP TABLE {}').format(
                sql.SQL(', ').join(sql.Identifier(schema, name) for schema, name in drop_targets)))
        print(f'Dropped {len(drop_targets)} table(s); no CASCADE was used.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='Perform the drop; default is a read-only inventory')
    parser.add_argument('--confirm-database', help='Required with --execute; must exactly match the connected database name')
    options = parser.parse_args()
    if options.execute and not options.confirm_database:
        parser.error('--execute requires --confirm-database with the exact database name')
    asyncio.run(main(options))
