"""Command admission rejects ownership conflicts durably without masking lease loss."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from starlette.requests import Request
from agent_python.command_api import command_dependency, replay_response
from agent_python.commands import Command
from agent_python.execution import OwnershipConflict, LeaseLost


class CommandRejections(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, error):
        row = {'command_id': 'command-1', 'status': 'pending'}
        command = Command(row, 'owner', 1)
        store = SimpleNamespace(get=AsyncMock(return_value=row), matches=Mock(),
            claim=AsyncMock(return_value=command), release=AsyncMock(),
            uncertain=AsyncMock(), finish_without_save=AsyncMock())
        app = SimpleNamespace(state=SimpleNamespace(repository=lambda: SimpleNamespace(commands=store)))
        request = Request({'type':'http','method':'POST','path':'/sessions',
            'headers':[(b'idempotency-key',b'key')], 'app':app,
            'route':SimpleNamespace(path='/sessions'), 'path_params':{}})
        request._body = b'{"user_prompt":"hello"}'
        dependency = command_dependency(request)
        await anext(dependency)
        with self.assertRaises(type(error)):
            await dependency.athrow(error)
        return store, command

    async def test_ownership_rejection_is_failed_and_replays_exact_body(self):
        store, command = await self.exercise(OwnershipConflict('Session is already owned'))
        body = {'type':'error','code':'execution_conflict',
                'message':'Session is already owned','recoverable':True}
        store.finish_without_save.assert_awaited_once_with(command,409,body,'failed')
        store.uncertain.assert_not_awaited()
        response = replay_response({'command_id':command.id,'status':'failed',
                                    'response_status':409,'response_body':body})
        self.assertEqual(response.status_code,409)
        self.assertEqual(response.headers['Idempotency-Replayed'],'true')

    async def test_lease_loss_preserves_uncertainty(self):
        store, command = await self.exercise(LeaseLost('Lease expired during work'))
        store.uncertain.assert_awaited_once_with(command)
        store.finish_without_save.assert_not_awaited()
