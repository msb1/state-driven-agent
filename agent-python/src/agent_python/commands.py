"""Durable command admission, fenced ownership and response replay."""
from contextvars import ContextVar
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
import asyncio
import json
import os
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from .execution import LeaseLost
from .identity import canonical, request_fingerprint

current_command = ContextVar('agent_command', default=None)


class CommandError(Exception):
    def __init__(self, code, message, status=409):
        self.code, self.message, self.status = code, message, status
        super().__init__(message)


@dataclass
class Command:
    row: dict
    owner: str
    fence: int
    response_message: str | None = None
    response_status: int = 200
    terminal: bool = False
    lost: Exception | None = None

    @property
    def id(self):
        return str(self.row['command_id'])

    @property
    def namespace(self):
        return self.row['namespace']


class CommandStore:
    def __init__(self, database_url):
        self.database_url = database_url

    @property
    def namespace(self):
        value = os.getenv('AGENT_DEPLOYMENT_NAMESPACE', '').strip()
        if not value:
            raise RuntimeError('Set a stable AGENT_DEPLOYMENT_NAMESPACE in repository .env')
        return value

    async def get(self, command_id=None, key=None):
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as db:
            cursor = await db.execute('''SELECT *,command_id::text AS command_id,session_id::text AS session_id
                FROM agent_commands WHERE namespace=%s AND client_scope='local'
                AND ((%s::uuid IS NOT NULL AND command_id=%s::uuid) OR (%s::text IS NOT NULL AND idempotency_key=%s))''',
                (self.namespace, command_id, command_id, key, key))
            row = await cursor.fetchone()
            if row and row['session_id']:
                run = await db.execute('SELECT run_id::text,termination_reason FROM agent_runs WHERE command_id=%s', (row['command_id'],))
                value = await run.fetchone()
                row['run_id'] = value['run_id'] if value else None
                row['recovery_reason'] = value['termination_reason'] if value else None
            return row

    @staticmethod
    def matches(row, route, body):
        if row['request_sha256'] != request_fingerprint('POST', route, body):
            raise CommandError('idempotency_key_reused', 'The request key already identifies different content or a different route')

    async def register(self, key, route, body, admission):
        identity = str(uuid4())
        try:
            async with await psycopg.AsyncConnection.connect(self.database_url) as db:
                await db.execute('''INSERT INTO agent_commands(command_id,namespace,client_scope,idempotency_key,
                    method,route,request,request_sha256,status,session_id) VALUES (%s,%s,'local',%s,'POST',%s,%s::jsonb,%s,'pending',%s)
                    ON CONFLICT(namespace,client_scope,idempotency_key) DO NOTHING''',
                    (identity, self.namespace, key, route,
                     json.dumps({'input': body, 'admission': admission}, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False),
                     request_fingerprint('POST', route, body), admission.get('session_id')))
        except (psycopg.OperationalError, psycopg.InterfaceError):
            row = await self.get(key=key)
            if row is None:
                raise
        row = await self.get(key=key)
        self.matches(row, route, body)
        return row

    async def claim(self, row, recover=False):
        owner = str(uuid4())
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as db:
            cursor = await db.execute('''UPDATE agent_commands SET owner=%s,fence=fence+1,status='started',
                lease_expires_at=clock_timestamp()+interval '30 seconds',updated_at=clock_timestamp()
                WHERE command_id=%s AND (status IN ('pending','started') OR (%s AND status IN ('uncertain','blocked')))
                AND (owner IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING fence''', (owner, row['command_id'], recover))
            claimed = await cursor.fetchone()
            return Command(row, owner, claimed['fence']) if claimed else None

    async def renew(self, command):
        async with await psycopg.AsyncConnection.connect(self.database_url) as db:
            cursor = await db.execute('''UPDATE agent_commands SET lease_expires_at=clock_timestamp()+interval '30 seconds'
                WHERE command_id=%s AND owner=%s AND fence=%s AND lease_expires_at>clock_timestamp() RETURNING fence''',
                (command.id, command.owner, command.fence))
            if await cursor.fetchone() is None:
                raise LeaseLost('Command lease was lost')

    async def release(self, command):
        async with await psycopg.AsyncConnection.connect(self.database_url) as db:
            await db.execute('''UPDATE agent_commands SET owner=NULL,lease_expires_at=NULL
                WHERE command_id=%s AND owner=%s AND fence=%s''', (command.id, command.owner, command.fence))

    async def guard(self, cursor, command):
        if command.lost:
            raise LeaseLost('Command heartbeat failed') from command.lost
        await cursor.execute('''SELECT owner::text,fence,lease_expires_at>clock_timestamp()
            FROM agent_commands WHERE command_id=%s FOR UPDATE''', (command.id,))
        if await cursor.fetchone() != (command.owner, command.fence, True):
            raise LeaseLost('Fenced command commit rejected')

    async def complete(self, cursor, command, status, body, outcome='succeeded'):
        await self.guard(cursor, command)
        await cursor.execute('''UPDATE agent_commands SET status=%s,response_status=%s,response_body=%s::jsonb,
            completed_at=clock_timestamp(),updated_at=clock_timestamp(),owner=NULL,lease_expires_at=NULL WHERE command_id=%s''',
            (outcome, status, canonical(body).decode(), command.id))

    async def finish_without_save(self, command, status, body, outcome='succeeded'):
        async with await psycopg.AsyncConnection.connect(self.database_url) as db:
            async with db.cursor() as cursor:
                await self.complete(cursor, command, status, body, outcome)

    async def block(self,cursor,command,status):
        await self.guard(cursor,command)
        await cursor.execute('''UPDATE agent_commands SET status=%s,updated_at=clock_timestamp(),owner=NULL,lease_expires_at=NULL
            WHERE command_id=%s''',(status,command.id))

    async def uncertain(self, command):
        async with await psycopg.AsyncConnection.connect(self.database_url) as db:
            await db.execute('''UPDATE agent_commands SET status='uncertain',updated_at=clock_timestamp()
                WHERE command_id=%s AND owner=%s AND fence=%s AND status IN ('pending','started')''',
                (command.id, command.owner, command.fence))


def envelope(row):
    return {'command_id': row['command_id'], 'session_id': row.get('session_id'), 'run_id': row.get('run_id'),
            'status': row['status'], 'status_uri': '/commands/' + row['command_id'],
            'response_status': row.get('response_status'), 'response_body': row.get('response_body'),
            'recovery_reason': row.get('recovery_reason')}


@asynccontextmanager
async def command_scope(store, command):
    token = current_command.set(command)
    async def heartbeat():
        try:
            while True:
                await asyncio.sleep(10)
                await store.renew(command)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            command.lost = error
    task = asyncio.create_task(heartbeat())
    try:
        yield command
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        try:
            await asyncio.shield(store.release(command))
        finally:
            current_command.reset(token)


@asynccontextmanager
async def administrative_command(repository, route, body):
    row = await repository.commands.register(str(uuid4()),route,body,{'session_id':body['session_id']})
    command = await repository.commands.claim(row)
    command.terminal = True
    async with command_scope(repository.commands,command):
        try:
            yield command
        except Exception:
            await repository.commands.uncertain(command)
            raise
