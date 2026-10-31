"""Durable execution coordinator. The repository owns all lease/commit authority."""
from __future__ import annotations

import asyncio
import json
import copy
from contextlib import suppress
from datetime import datetime, timezone
from uuid import uuid4

from .transcript import archive_record


class OwnershipConflict(Exception):
    code = 'execution_conflict'


class LeaseLost(OwnershipConflict):
    code = 'lease_lost'


class RunCancelled(Exception):
    pass


class RecoveryBlocked(Exception):
    pass


class TransientToolError(Exception):
    """Only an explicit environment signal authorizes an automatic retry."""


def utcnow():
    return datetime.now(timezone.utc)


class ExecutionCoordinator:
    def __init__(self, repository, state, config, lease_seconds=30):
        self.repository, self.state, self.config = repository, state, config
        self.lease_seconds = lease_seconds
        self.owner = str(uuid4())
        self.fence = None
        self.heartbeat = None
        self.lost = None

    async def __aenter__(self):
        self.fence = await self.repository.acquire(self.state.id, self.owner, self.lease_seconds)
        self.state.execution_owner, self.state.execution_fence = self.owner, self.fence
        self.heartbeat = asyncio.create_task(self._heartbeat())
        return self

    async def __aexit__(self, *_):
        if self.heartbeat:
            self.heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await self.heartbeat
        try:
            await self.repository.release(self.state.id, self.owner, self.fence)
        finally:
            self.state.execution_owner = self.state.execution_fence = None

    async def _heartbeat(self):
        try:
            while True:
                await asyncio.sleep(self.lease_seconds / 3)
                await self.repository.renew(self.state.id, self.owner, self.fence, self.lease_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.lost = error

    async def checkpoint(self, check_cancel=True):
        if self.state.needs_reload:
            from .persistence import RevisionConflict
            raise RevisionConflict(self.state.id,self.state.revision,'reload required after historical receipt')
        if self.lost:
            raise LeaseLost('Heartbeat failed; reload and recover after lease expiry.') from self.lost
        cancelled = await self.repository.check_owner(self.state.id, self.owner, self.fence, self.state.run_id)
        if cancelled and check_cancel:
            raise RunCancelled('Cancellation requested.')

    async def persist(self, event):
        # Known tool results may commit even if cancellation arrived during the
        # invocation. The next scheduling boundary acknowledges cancellation.
        await self.checkpoint(check_cancel=False)
        # A terminal answer/error and lifecycle must become durable together,
        # before the controller exposes that terminal event to a client.
        if self.state.status != 'active' and self.state.run_status == 'running':
            self.state.run_status = 'complete' if self.state.status == 'complete' else 'failed'
            self.state.run_ended_at = utcnow()
            self.state.run_termination_reason = (self.state.last_error or {}).get('code') or self.state.status
            archive_record(self.state, 'run_finished', {'status': self.state.run_status, 'reason': self.state.run_termination_reason})
        receipt = await self.repository.save(self.state, self.config, event)
        if receipt:
            from .commands import current_command
            command = current_command.get()
            event.update(event_id=receipt['event_id'],command_id=command.id,run_id=self.state.run_id)

    async def start(self):
        """Fence the previous abandoned run before creating a new run identity."""
        old_id = self.state.run_id
        if old_id and self.state.run_status == 'running':
            self.state.run_status = 'interrupted'
            self.state.run_ended_at = utcnow()
            self.state.run_termination_reason = 'lease_expired_or_worker_disconnected'
            archive_record(self.state, 'run_interrupted', {'run_id': old_id, 'reason': self.state.run_termination_reason})
            await self.repository.save(self.state, self.config, {'type': 'run_interrupted', 'run_id': old_id})
        from .commands import current_command
        self.state.run_id = str(uuid4())
        self.state.run_command_id = current_command.get().id
        self.state.run_status, self.state.run_started_at = 'running', utcnow()
        self.state.run_ended_at = self.state.run_termination_reason = None
        archive_record(self.state, 'run_started', {'fence': self.fence, 'disconnect_policy': 'cancel_at_boundary'})
        await self.persist({'type': 'run_started', 'run_id': self.state.run_id})

    async def finish(self, status=None, reason=None):
        # Cancellation flags do not prohibit committing its acknowledgement.
        target = status or ('complete' if self.state.status == 'complete' else 'failed')
        if self.state.run_status == target and self.state.run_ended_at is not None:
            return
        self.state.run_status = target
        self.state.run_ended_at = utcnow()
        self.state.run_termination_reason = reason or (self.state.last_error or {}).get('code') or self.state.status
        archive_record(self.state, 'run_finished', {'status': self.state.run_status, 'reason': self.state.run_termination_reason})
        await self.repository.save(self.state, self.config, {'type': 'run_finished', 'run_id': self.state.run_id,
            'status': self.state.run_status, 'reason': self.state.run_termination_reason})

    async def invoke(self, call, dispatch):
        from .tools import recovery_policy, IDEMPOTENT_TOOLS, RECONCILERS
        from .commands import current_command
        from .identity import canonical, action_key, payload_fingerprint
        from .recovery import IdempotentAdapter, Verification, RecipientConflict
        await self.checkpoint()
        command = current_command.get()
        row = await self.repository.execution(self.state.id,call['id'])
        policy = recovery_policy(next(t for t in self.config.tools if t.name==call['name']))
        adapter = IDEMPOTENT_TOOLS.get(call['name'])
        if policy['mode']=='idempotent' and isinstance(adapter,IdempotentAdapter):
            policy = {**policy,'recipient':{'target':adapter.target,'key_scope':adapter.key_scope,
                                          'retention_seconds':adapter.retention_seconds}}
        target = adapter.target if isinstance(adapter,IdempotentAdapter) else 'tool://' + call['name']
        if row is None:
            execution_id = str(uuid4())
            row = {'execution_id':execution_id,'run_id':self.state.run_id,'command_id':command.id,
                'namespace':command.namespace,'target':target,'key_algorithm':'agent-action-v1',
                'call_id':call['id'],'tool':call['name'],'arguments':copy.deepcopy(call['arguments']),
                'policy':policy,'attempts':0,'status':'pending','result':None,'created_at':utcnow().isoformat(),
                'idempotency_key':action_key(command.namespace,self.state.id,execution_id),
                'payload_sha256':payload_fingerprint(call['name'],target,call['arguments'])}
            self.state.execution_pending.append(copy.deepcopy(row))
            archive_record(self.state,'tool_intent',dict(row))
            await self.persist({'type':'tool_intent','execution_id':execution_id,'tool_call_id':call['id']})
        elif row['tool']!=call['name'] or canonical(row['arguments'])!=canonical(call['arguments']):
            raise RecoveryBlocked('Frozen action tool or arguments differ')
        if row['status'] in ('succeeded','failed'):
            return row['result']
        if row['policy']!=policy or row['target']!=target or row['namespace']!=command.namespace:
            raise RecoveryBlocked('Frozen action target, namespace or recipient capability differs')
        if row['status'] in ('started','uncertain','blocked'):
            reconciler = RECONCILERS.get(row['tool'])
            proven_absent = False
            if reconciler:
                attempt = await self._start_attempt(row,'verify')
                try:
                    verification = await reconciler(copy.deepcopy(row['arguments']),row['idempotency_key'])
                    if not isinstance(verification,Verification):
                        raise ValueError('Verifier must return authoritative Verification evidence')
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._end_attempt(attempt,'uncertain',error={'type':type(error).__name__})
                    await self._uncertain(row,'Verification could not establish the original outcome')
                self._end_attempt(attempt,'succeeded',evidence=verification.evidence,external_reference=verification.external_reference)
                if verification.outcome in ('succeeded','failed'):
                    self._stage_result(row,verification.result,verification.outcome)
                    return verification.result
                proven_absent = verification.outcome=='not_executed'
                await self.persist({'type':'tool_verified','execution_id':row['execution_id'],'outcome':verification.outcome})
            if policy['mode']=='reconcile' and not proven_absent:
                await self._uncertain(row,'External outcome is unknown; authoritative reconciliation required')
        if policy['mode']=='idempotent' and not isinstance(adapter,IdempotentAdapter):
            await self._uncertain(row,'No registered recipient capability enforces this action key')
        while row['attempts'] < policy['max_attempts']:
            await self.checkpoint()
            if policy['mode']=='idempotent' and adapter.retention_seconds is not None:
                age = (utcnow()-datetime.fromisoformat(row['created_at'])).total_seconds()
                if age >= adapter.retention_seconds:
                    await self._uncertain(row,'Recipient deduplication guarantee expired; verification required')
            row['attempts'] += 1
            row['status'] = 'started'
            self.state.execution_pending.append(copy.deepcopy(row))
            attempt = await self._start_attempt(row,'dispatch')
            try:
                result = await adapter.invoke(copy.deepcopy(row['arguments']),row['idempotency_key']) if policy['mode']=='idempotent' else await dispatch(row['tool'],copy.deepcopy(row['arguments']))
                if not isinstance(result,str):
                    raise TypeError('Tool results must be strings')
            except asyncio.CancelledError:
                raise
            except RecipientConflict as error:
                self._end_attempt(attempt,'failed',error={'type':'payload_conflict'})
                await self._uncertain(row,str(error))
            except Exception as error:
                self._end_attempt(attempt,'uncertain',error={'type':type(error).__name__})
                row['status'] = 'uncertain'
                self.state.execution_pending.append(copy.deepcopy(row))
                await self.persist({'type':'tool_attempt_uncertain','execution_id':row['execution_id'],'attempt':row['attempts']})
                if (isinstance(error,TransientToolError) or policy['mode']=='idempotent') and policy['mode']!='reconcile' and row['attempts']<policy['max_attempts']:
                    await asyncio.sleep(min(0.1 * 2**(row['attempts']-1),2))
                    continue
                if policy['mode'] in ('reconcile','idempotent'):
                    await self._uncertain(row,'No definitive external outcome; safe retry limit reached')
                result = 'ERROR: tool execution failed.'
            else:
                self._end_attempt(attempt,'succeeded' if not result.startswith(('ERROR:','AssertionError:')) else 'failed',evidence={'result':result})
            await self.checkpoint(check_cancel=False)
            self._stage_result(row,result)
            return result
        await self._uncertain(row,'Dispatch attempt limit exhausted; reconciliation required')

    async def _start_attempt(self,row,kind):
        from .commands import current_command
        attempt = {'attempt_id':str(uuid4()),'execution_id':row['execution_id'],'command_id':current_command.get().id,
            'attempt_number':await self.repository.next_attempt(row['execution_id']),'kind':kind,'status':'started',
            'payload_sha256':row['payload_sha256'],'owner':self.owner,'fence':self.fence,'started_at':utcnow().isoformat()}
        self.state.attempts_pending.append(copy.deepcopy(attempt))
        archive_record(self.state,'tool_attempt',dict(attempt))
        await self.persist({'type':'tool_started' if kind=='dispatch' else 'tool_verifying',
            'execution_id':row['execution_id'],'attempt':row['attempts'],'attempt_number':attempt['attempt_number']})
        await self.checkpoint()
        return attempt

    def _end_attempt(self,attempt,status,**evidence):
        attempt.update(status=status,finished_at=utcnow().isoformat(),**evidence)
        self.state.attempts_pending.append(copy.deepcopy(attempt))
        archive_record(self.state,'tool_attempt_settled',dict(attempt))

    def _stage_result(self,row,result,status=None):
        row['status'] = status or ('failed' if result.startswith(('ERROR:','AssertionError:')) else 'succeeded')
        row['result'] = result
        self.state.execution_pending.append(copy.deepcopy(row))
        archive_record(self.state,'tool_outcome',{'execution_id':row['execution_id'],'status':row['status'],'result':result})

    async def _uncertain(self,row,reason):
        row['status'] = 'blocked'
        self.state.execution_pending.append(copy.deepcopy(row))
        archive_record(self.state,'tool_uncertain',{'execution_id':row['execution_id'],'reason':reason})
        await self.persist({'type':'tool_uncertain','execution_id':row['execution_id'],'message':reason})
        raise RecoveryBlocked(reason)


async def reconcile_abandoned(repository, limit=100):
    from .commands import command_scope
    reconciled = []
    for row in await repository.abandoned(limit):
        persisted = await repository.get(row['session_id'])
        if persisted is None or not persisted.state.run_command_id:
            continue
        origin = await repository.commands.get(command_id=persisted.state.run_command_id)
        if origin is None:
            continue
        command = await repository.commands.claim(origin,recover=True)
        if command is None:
            continue
        try:
            async with command_scope(repository.commands,command):
                async with ExecutionCoordinator(repository,persisted.state,persisted.config) as execution:
                    persisted = await repository.get(row['session_id'])
                    execution.state = persisted.state
                    execution.state.execution_owner,execution.state.execution_fence = execution.owner,execution.fence
                    if execution.state.run_id!=row['run_id'] or execution.state.run_status!='running':
                        continue
                    if execution.state.status=='complete':
                        await execution.finish('complete','terminal_snapshot_recovered')
                    else:
                        execution.state.status = 'failed'
                        execution.state.final_answer = 'Abandoned run detected; use /resume to reconcile pending work.'
                        execution.state.last_error = {'code':'run_abandoned','message':execution.state.final_answer,'recoverable':True}
                        await execution.finish('interrupted','worker_abandoned')
                    reconciled.append({'session_id':row['session_id'],'run_id':row['run_id']})
        except OwnershipConflict:
            continue
    return reconciled
