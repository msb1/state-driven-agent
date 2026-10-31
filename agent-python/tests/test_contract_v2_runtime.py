"""Real PostgreSQL regressions in isolated schemas; no live model or shell suites."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT / 'scripts'))
from agent_python import api
from agent_python.persistence import SessionRepository
from agent_python.llm import LlmClient
from agent_python.identity import canonical
from agent_python.recovery import IdempotentAdapter
from agent_python.execution import TransientToolError

spec = importlib.util.spec_from_file_location('initialize_database',ROOT / 'agent-python/scripts/initialize_database.py')
initialize = importlib.util.module_from_spec(spec)
spec.loader.exec_module(initialize)


class RuntimePostgres(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.admin = await psycopg.AsyncConnection.connect(dotenv_values(ROOT / '.env')['AGENT_SESSION_DATABASE_URL'],autocommit=True)
        self.schema = 'v2_python_' + uuid4().hex
        await self.admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.dsn = make_conninfo(dotenv_values(ROOT / '.env')['AGENT_SESSION_DATABASE_URL'],options='-csearch_path='+self.schema)
        async with await psycopg.AsyncConnection.connect(self.dsn,autocommit=True) as db:
            await initialize.setup(db,initialize.DEFINITIONS)
        self.repo = SessionRepository(self.dsn)
        self.repo_patch = patch.object(api,'session_repository',self.repo)
        self.repo_patch.start()
        self.env_patch = patch.dict(os.environ,{'AGENT_DEPLOYMENT_NAMESPACE':'regression-'+self.schema,'AGENT_FIXTURE_DATABASE_URL':self.dsn})
        self.env_patch.start()
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app),base_url='http://agent',timeout=30)
        api.sessions.clear()

    async def asyncTearDown(self):
        await self.http.aclose()
        self.repo_patch.stop()
        self.env_patch.stop()
        await self.admin.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.schema)))
        await self.admin.close()

    async def rows(self,query,params=()):
        async with await psycopg.AsyncConnection.connect(self.dsn) as db:
            cursor = await db.execute(query,params)
            return await cursor.fetchall() if cursor.description else []

    async def post(self,path,body,key=None):
        return await self.http.post(path,json=body,headers={'Idempotency-Key':key or str(uuid4())})

    async def create(self):
        result = await self.post('/sessions',{'config_ref':'completion-no-workflow.yaml','user_prompt':'regression'})
        self.assertEqual(result.status_code,201,result.text)
        return result.json()['session']['id']

    async def test_creation_message_replay_and_old_response(self):
        key = str(uuid4())
        body = {'config_ref':'completion-no-workflow.yaml','user_prompt':'identity'}
        first = await self.post('/sessions',body,key)
        self.assertEqual(first.status_code,201,first.text)
        sid = first.json()['session']['id']
        message_key = str(uuid4())
        message = await self.post(f'/sessions/{sid}/messages',{'content':'first'},message_key)
        self.assertEqual(message.status_code,200,message.text)
        replay = await self.post('/sessions',dict(reversed(list(body.items()))),key)
        self.assertEqual(replay.json(),first.json())
        self.assertEqual(replay.headers['Idempotency-Replayed'],'true')
        later = await self.post(f'/sessions/{sid}/messages',{'content':'later'})
        self.assertEqual(later.status_code,200,later.text)
        old = await self.post(f'/sessions/{sid}/messages',{'content':'first'},message_key)
        self.assertEqual(old.json(),message.json())
        self.assertEqual((await self.rows('SELECT revision FROM agent_sessions WHERE session_id=%s',(sid,)))[0][0],3)
        receipts = await self.rows('SELECT expected_revision,committed_revision FROM agent_commit_receipts WHERE session_id=%s ORDER BY committed_revision',(sid,))
        self.assertEqual(receipts,[(0,1),(1,2),(2,3)])
        changed = await self.post('/sessions',{**body,'user_prompt':'different'},key)
        self.assertEqual(changed.status_code,409,changed.text)
        self.assertEqual(changed.json()['code'],'idempotency_key_reused')

    async def test_employee_config_hash_survives_command_admission_roundtrip(self):
        from agent_python.config import resolved_config_hash
        response = await self.post('/sessions',{
            'config_ref':'employee-audit.yaml','user_prompt':'audit regression'})
        self.assertEqual(response.status_code,201,response.text)
        persisted = await self.repo.get(response.json()['session']['id'])
        self.assertEqual(persisted.state.resolved_config_sha256,
                         resolved_config_hash(persisted.config))

    async def test_strict_admission_and_concurrent_duplicate(self):
        for headers,content,code in [({},'{"user_prompt":"x"}','idempotency_key_required'),
            ({'Idempotency-Key':'has space'},'{"user_prompt":"x"}','idempotency_key_invalid')]:
            response = await self.http.post('/sessions',content=content,headers={**headers,'Content-Type':'application/json'})
            self.assertEqual(response.status_code,422,response.text)
            self.assertEqual(response.json()['code'],code)
        for content in ['{"user_prompt":"x","user_prompt":"y"}','{"user_prompt":"x","extra":1}']:
            result = await self.http.post('/sessions',content=content,headers={'Idempotency-Key':str(uuid4()),'Content-Type':'application/json'})
            self.assertEqual(result.status_code,422,result.text)
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_commands'))[0][0],0)
        key = str(uuid4())
        results = await asyncio.gather(*(self.post('/sessions',{'user_prompt':'concurrent'},key) for _ in range(2)))
        self.assertTrue(all(r.status_code in (201,202) for r in results),[r.text for r in results])
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_sessions'))[0][0],1)
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_commands'))[0][0],1)

    async def test_actual_engine_run_and_replay(self):
        sid = await self.create()
        key = str(uuid4())
        with patch.object(LlmClient,'decide',AsyncMock(return_value={'type':'final','content':'REGRESSION_OK'})) as model:
            result = await self.post(f'/sessions/{sid}/run',{},key)
            self.assertEqual(result.status_code,200,result.text)
            self.assertEqual(result.json()['session']['status'],'complete')
            replay = await self.post(f'/sessions/{sid}/run',{},key)
            self.assertEqual(replay.json(),result.json())
            self.assertEqual(model.await_count,1)
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_runs'))[0][0],1)
        run_id = result.json()['session']['run_id']
        before = (await self.rows('SELECT revision FROM agent_sessions WHERE session_id=%s',(sid,)))[0][0]
        stream = await self.http.get(f'/sessions/{sid}/events/stream',params={'run_id':run_id})
        self.assertEqual(stream.status_code,200,stream.text)
        self.assertIn('REGRESSION_OK',stream.text)
        self.assertEqual((await self.rows('SELECT revision FROM agent_sessions WHERE session_id=%s',(sid,)))[0][0],before)

    async def test_keyed_recipient_lost_response_has_one_effect(self):
        from agent_python import tools
        sid = await self.create()
        keys = []
        async def recipient(arguments,key):
            keys.append(key)
            async with await psycopg.AsyncConnection.connect(self.dsn) as db:
                await db.execute('''INSERT INTO agent_fixture_action_receipts(namespace,idempotency_key,payload_sha256,request,result)
                    VALUES ('test',%s,%s,%s::jsonb,%s::jsonb) ON CONFLICT DO NOTHING''',
                    (key,'a'*64,json.dumps(arguments),json.dumps({'ok':True})))
            if len(keys)==1:
                raise TransientToolError('Committed recipient effect; response lost')
            return '{"ok":true}'
        decisions = [{'type':'tool','id':'call-1','name':'inspect_log','arguments':{'start_line':1,'line_count':1}},
                     {'type':'final','content':'DONE'}]
        adapter = IdempotentAdapter('fixture://recipient',recipient,'deployment',None)
        with patch.dict(tools.IDEMPOTENT_TOOLS,{'inspect_log':adapter}), patch.object(tools,'recovery_policy',return_value={'mode':'idempotent','max_attempts':2}), patch.object(LlmClient,'decide',AsyncMock(side_effect=decisions)):
            result = await self.post(f'/sessions/{sid}/run',{})
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(result.json()['session']['status'],'complete',result.text)
        self.assertEqual(len(keys),2)
        self.assertEqual(keys[0],keys[1])
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_fixture_action_receipts'))[0][0],1)
        execution = (await self.rows('SELECT status,attempts,length(idempotency_key) FROM agent_tool_executions'))[0]
        self.assertEqual(execution,('succeeded',2,64))
        self.assertEqual((await self.rows("SELECT count(*) FROM agent_action_attempts WHERE kind='dispatch'"))[0][0],2)

    async def test_commit_ack_loss_and_historical_receipt(self):
        from agent_python.commands import command_scope, current_command
        from agent_python.transcript import append_message
        from agent_python.models import Message
        from agent_python.persistence import RevisionConflict, TranscriptIntegrityError
        sid = await self.create()
        persisted = await self.repo.get(sid)
        row = await self.repo.commands.register(str(uuid4()),f'/sessions/{sid}/messages',{'content':'committed'}, {'session_id':sid})
        command = await self.repo.commands.claim(row)
        command.terminal, command.response_message = True, 'User input appended to session memory.'
        state = persisted.state
        append_message(state,Message(role='user',content='committed'))
        state.pending_commit_id = str(uuid4())
        original = state.model_copy(deep=True)
        original_commit = psycopg.AsyncConnection.commit
        lost = False
        async def lose_ack(db):
            nonlocal lost
            await original_commit(db)
            if not lost:
                lost = True
                raise psycopg.OperationalError('Lost commit acknowledgement after PostgreSQL committed')
        async with command_scope(self.repo.commands,command):
            with patch.object(psycopg.AsyncConnection,'commit',lose_ack):
                receipt = await self.repo.save(state,persisted.config,{'type':'user_message_appended'})
        self.assertTrue(lost)
        self.assertEqual(receipt['revision'],2)
        self.assertIsNone(state.pending_commit_id)
        later = await self.post(f'/sessions/{sid}/messages',{'content':'later'})
        self.assertEqual(later.status_code,200,later.text)
        token = current_command.set(command)
        try:
            replay = await self.repo.save(original,persisted.config,{'type':'user_message_appended'})
            self.assertEqual(replay['revision'],2)
            self.assertTrue(original.needs_reload)
            with self.assertRaises(RevisionConflict):
                await self.repo.save(original,persisted.config,{'type':'new-write'})
            from agent_python.execution import ExecutionCoordinator
            with self.assertRaises(RevisionConflict):
                await ExecutionCoordinator(self.repo,original,persisted.config).checkpoint()
        finally:
            current_command.reset(token)
        self.assertEqual((await self.rows('SELECT revision FROM agent_sessions WHERE session_id=%s',(sid,)))[0][0],3)
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_commit_receipts WHERE session_id=%s',(sid,)))[0][0],3)

    async def test_reconcile_resume_preserves_origin_and_records_verification(self):
        from agent_python import tools
        from agent_python.recovery import Verification
        sid = await self.create()
        decision = {'type':'tool','id':'uncertain-call','name':'inspect_log','arguments':{'start_line':1,'line_count':1}}
        dispatch = AsyncMock(side_effect=RuntimeError('External outcome unknown'))
        policy = {'mode':'reconcile','max_attempts':2}
        with patch.object(tools,'recovery_policy',return_value=policy), patch('agent_python.engine.execute',dispatch), patch.object(LlmClient,'decide',AsyncMock(return_value=decision)):
            first = await self.post(f'/sessions/{sid}/run',{})
        self.assertEqual(first.status_code,200,first.text)
        self.assertEqual(first.json()['session']['run_status'],'blocked',first.text)
        origin = (await self.rows('SELECT execution_id,run_id,command_id,idempotency_key FROM agent_tool_executions'))[0]
        verifier = AsyncMock(return_value=Verification('succeeded',{'operation_reference':'verified-original','authoritative':True},'original outcome'))
        with patch.object(tools,'recovery_policy',return_value=policy), patch.dict(tools.RECONCILERS,{'inspect_log':verifier}), patch('agent_python.engine.execute',dispatch), patch.object(LlmClient,'decide',AsyncMock(return_value={'type':'final','content':'RECOVERED'})):
            resumed = await self.post(f'/sessions/{sid}/resume',{})
        self.assertEqual(resumed.status_code,200,resumed.text)
        state = (await self.http.get(f'/sessions/{sid}')).json()
        self.assertEqual(state['status'],'complete',resumed.text)
        self.assertNotEqual(state['run_id'],str(origin[1]))
        self.assertEqual((await self.rows('SELECT execution_id,run_id,command_id,idempotency_key FROM agent_tool_executions'))[0],origin)
        self.assertEqual(dispatch.await_count,1)
        attempts = await self.rows('SELECT kind,command_id FROM agent_action_attempts ORDER BY attempt_number')
        self.assertEqual([a[0] for a in attempts],['dispatch','verify'])
        self.assertNotEqual(attempts[0][1],attempts[1][1])

    async def test_cancel_retry_never_targets_successor(self):
        sid = await self.create()
        entered, release = asyncio.Event(), asyncio.Event()
        async def delayed(_client,_messages):
            entered.set()
            await release.wait()
            return {'type':'final','content':'DONE'}
        with patch.object(LlmClient,'decide',delayed):
            running = asyncio.create_task(self.post(f'/sessions/{sid}/run',{}))
            await asyncio.wait_for(entered.wait(),10)
            run_id = (await self.http.get(f'/sessions/{sid}')).json()['run_id']
            key = str(uuid4())
            cancelled = await self.post(f'/sessions/{sid}/cancel',{'run_id':run_id},key)
            self.assertEqual(cancelled.status_code,200,cancelled.text)
            release.set()
            stopped = await running
        self.assertEqual(stopped.json()['session']['run_status'],'cancelled',stopped.text)
        with patch.object(LlmClient,'decide',AsyncMock(return_value={'type':'final','content':'RESUMED'})):
            resumed = await self.post(f'/sessions/{sid}/resume',{})
        self.assertEqual(resumed.status_code,200,resumed.text)
        replay = await self.post(f'/sessions/{sid}/cancel',{'run_id':run_id},key)
        self.assertEqual(replay.json(),cancelled.json())
        runs = await self.rows('SELECT run_id::text,cancel_requested FROM agent_runs ORDER BY started_at')
        self.assertEqual(runs[0],(run_id,True))
        self.assertFalse(runs[1][1])

    async def test_startup_recovers_abandoned_metadata_once_without_invocation(self):
        from agent_python.commands import command_scope
        from agent_python.execution import ExecutionCoordinator
        sid = await self.create()
        persisted = await self.repo.get(sid)
        row = await self.repo.commands.register(str(uuid4()),f'/sessions/{sid}/run',{'max_steps':None},{'session_id':sid})
        command = await self.repo.commands.claim(row)
        async with command_scope(self.repo.commands,command):
            async with ExecutionCoordinator(self.repo,persisted.state,persisted.config) as execution:
                await execution.start()
        before = (await self.rows('SELECT revision FROM agent_sessions'))[0][0]
        with patch.object(LlmClient,'decide',AsyncMock()) as model:
            await asyncio.gather(self.repo.startup(),self.repo.startup())
            model.assert_not_awaited()
        state = (await self.repo.get(sid)).state
        self.assertEqual(state.run_status,'interrupted')
        self.assertEqual(state.revision,before+1)
        await self.repo.startup()
        self.assertEqual((await self.repo.get(sid)).state.revision,before+1)
        original = await self.repo.commands.get(command_id=command.id)
        self.assertEqual(original['status'],'failed')

    async def test_retention_preserves_command_and_commit_receipts(self):
        import tempfile
        sid = await self.create()
        with patch.object(LlmClient,'decide',AsyncMock(return_value={'type':'final','content':'DONE'})):
            result = await self.post(f'/sessions/{sid}/run',{})
        self.assertEqual(result.status_code,200,result.text)
        before_commands = await self.rows('SELECT command_id,response_body FROM agent_commands ORDER BY command_id')
        before_receipts = await self.rows('SELECT commit_id,fingerprint FROM agent_commit_receipts ORDER BY committed_revision')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'archive.jsonl'
            await self.repo.export_archive(sid,path)
            await self.repo.prune_telemetry(sid,path,32,1)
        after_commands = dict(await self.rows('SELECT command_id,response_body FROM agent_commands'))
        for key,body in before_commands:
            self.assertEqual(after_commands[key],body)
        after_receipts = dict(await self.rows('SELECT commit_id,fingerprint FROM agent_commit_receipts'))
        for key,fingerprint in before_receipts:
            self.assertEqual(after_receipts[key],fingerprint)
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_archive_receipts'))[0][0],1)

    async def test_expired_recipient_guarantee_blocks_without_dispatch(self):
        from agent_python import tools
        sid = await self.create()
        invoke = AsyncMock(return_value='effect')
        adapter = IdempotentAdapter('fixture://expires',invoke,'deployment',0.000001)
        decision = {'type':'tool','id':'expired-call','name':'inspect_log','arguments':{'start_line':1,'line_count':1}}
        with patch.dict(tools.IDEMPOTENT_TOOLS,{'inspect_log':adapter}), patch.object(tools,'recovery_policy',return_value={'mode':'idempotent','max_attempts':2}), patch.object(LlmClient,'decide',AsyncMock(return_value=decision)):
            result = await self.post(f'/sessions/{sid}/run',{})
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(result.json()['session']['run_status'],'blocked',result.text)
        invoke.assert_not_awaited()
        self.assertEqual((await self.rows('SELECT status,attempts FROM agent_tool_executions'))[0],('blocked',0))

    async def test_builtin_fixture_adapter_uses_real_receipt_and_verifier(self):
        from agent_python import tools
        from agent_python.recovery import RecipientConflict
        created = await self.post('/sessions',{'config_ref':'idempotency-fixture.yaml','user_prompt':'Write test value once'})
        self.assertEqual(created.status_code,201,created.text)
        sid = created.json()['session']['id']
        decisions = [{'type':'tool','id':'fixture-call','name':'fixture_idempotent_write','arguments':{'value':'once'}}, {'type':'final','content':'DONE'}]
        with patch.object(LlmClient,'decide',AsyncMock(side_effect=decisions)):
            result = await self.post(f'/sessions/{sid}/run',{})
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(result.json()['session']['status'],'complete',result.text)
        key = (await self.rows('SELECT idempotency_key FROM agent_tool_executions'))[0][0]
        repeated = await tools.fixture_idempotent_write({'value':'once'},key)
        verification = await tools.fixture_verify({'value':'once'},key)
        self.assertEqual(verification.result,repeated)
        self.assertEqual(verification.outcome,'succeeded')
        with self.assertRaises(RecipientConflict):
            await tools.fixture_idempotent_write({'value':'different'},key)
        self.assertEqual((await self.rows('SELECT count(*) FROM agent_fixture_action_receipts'))[0][0],1)

    async def test_actual_tcp_disconnect_cancels_and_resume_preserves_history(self):
        import socket
        import tempfile
        import threading
        import uvicorn
        from agent_python.config import ConfigRepository
        from disconnect_integration_test import Harness,run_suite
        listener = socket.socket()
        listener.bind(('127.0.0.1',0))
        port = listener.getsockname()[1]
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1',0))
            model_port = reservation.getsockname()[1]
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            model_url = f'http://127.0.0.1:{model_port}/v1'
            profile = (ROOT / 'config/socket-integration.yaml').read_text().replace('${AUX_SOCKET_MODEL_URL:-http://127.0.0.1:18089/v1}',model_url)
            (directory / 'socket-integration.yaml').write_text(profile)
            (directory / 'completion-no-workflow.yaml').write_text((ROOT / 'config/completion-no-workflow.yaml').read_text())
            server = uvicorn.Server(uvicorn.Config(api.app,host='127.0.0.1',port=port,log_level='warning',timeout_graceful_shutdown=5))
            thread = threading.Thread(target=server.run,kwargs={'sockets':[listener]},daemon=True)
            harness = None
            with patch.object(api,'config_repository',ConfigRepository(directory)):
                try:
                    thread.start()
                    for _ in range(500):
                        if server.started:
                            break
                        await asyncio.sleep(0.02)
                    self.assertTrue(server.started,'Test server did not start')
                    harness = Harness(f'http://127.0.0.1:{port}',model_url,'socket-integration.yaml',directory / 'evidence',20)
                    await asyncio.to_thread(run_suite,harness)
                    from persistence_support import APIReplay
                    replay = APIReplay(f'http://127.0.0.1:{port}',self.dsn)
                    try:
                        result = await asyncio.to_thread(replay.run)
                        self.assertEqual(result['commands'],4)
                    finally:
                        await asyncio.to_thread(replay.close)
                finally:
                    if harness:
                        await asyncio.to_thread(harness.close)
                    server.should_exit = True
                    await asyncio.to_thread(thread.join,10)
                    listener.close()
                    self.assertFalse(thread.is_alive(),'Test server did not stop')


    async def test_ambiguous_verification_blocks_without_another_dispatch(self):
        from agent_python import tools
        from agent_python.recovery import Verification
        sid = await self.create()
        decision = {'type':'tool','id':'ambiguous-call','name':'inspect_log','arguments':{'start_line':1,'line_count':1}}
        dispatch = AsyncMock(side_effect=RuntimeError('Unknown external outcome'))
        policy = {'mode':'reconcile','max_attempts':2}
        with patch.object(tools,'recovery_policy',return_value=policy), patch('agent_python.engine.execute',dispatch), patch.object(LlmClient,'decide',AsyncMock(return_value=decision)):
            first = await self.post(f'/sessions/{sid}/run',{})
        self.assertEqual(first.json()['session']['run_status'],'blocked',first.text)
        verifier = AsyncMock(return_value=Verification('unknown',{'source':'primary','result':'absence is not conclusive'}))
        with patch.object(tools,'recovery_policy',return_value=policy), patch.dict(tools.RECONCILERS,{'inspect_log':verifier}), patch('agent_python.engine.execute',dispatch):
            resumed = await self.post(f'/sessions/{sid}/resume',{})
        self.assertEqual(resumed.status_code,200,resumed.text)
        state = (await self.http.get(f'/sessions/{sid}')).json()
        self.assertEqual(state['run_status'],'blocked')
        self.assertEqual(dispatch.await_count,1)
        commands = await self.rows("SELECT status,response_status,response_body FROM agent_commands WHERE route LIKE %s OR route LIKE %s",('%/run','%/resume'))
        self.assertEqual(commands,[('blocked',None,None),('blocked',None,None)])
