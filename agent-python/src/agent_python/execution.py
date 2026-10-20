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
        await self.repository.save(self.state, self.config, event)

    async def start(self):
        """Fence the previous abandoned run before creating a new run identity."""
        old_id = self.state.run_id
        if old_id and self.state.run_status == 'running':
            self.state.run_status = 'interrupted'
            self.state.run_ended_at = utcnow()
            self.state.run_termination_reason = 'lease_expired_or_worker_disconnected'
            archive_record(self.state, 'run_interrupted', {'run_id': old_id, 'reason': self.state.run_termination_reason})
            await self.repository.save(self.state, self.config, {'type': 'run_interrupted', 'run_id': old_id})
        self.state.run_id = str(uuid4())
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
        """Commit intent and start marker before entering the environment.

        The final result is staged; engine commits it with evidence and messages.
        A lost result acknowledgement is recovered by reloading the committed row.
        """
        from .tools import recovery_policy, IDEMPOTENT_TOOLS, RECONCILERS
        await self.checkpoint()
        row = await self.repository.execution(self.state.id, call['id'])
        policy = recovery_policy(next(t for t in self.config.tools if t.name == call['name']))
        if row is None:
            row = {'execution_id': str(uuid4()), 'run_id': self.state.run_id,
                'call_id': call['id'], 'tool': call['name'], 'arguments': copy.deepcopy(call['arguments']),
                'policy': policy, 'attempts': 0, 'status': 'pending', 'result': None,
                'idempotency_key': str(uuid4())}
            self.state.execution_pending.append(dict(row))
            archive_record(self.state, 'tool_intent', dict(row))
            await self.persist({'type': 'tool_intent', 'execution_id': row['execution_id'], 'tool_call_id': call['id']})
        elif row['tool'] != call['name'] or json.dumps(row['arguments'], sort_keys=True) != json.dumps(call['arguments'], sort_keys=True) or row['policy'] != policy:
            raise RecoveryBlocked('Persisted tool identity/policy differs; refusing invocation.')
        if row['status'] in {'succeeded', 'failed'}:
            return row['result']
        if row['status'] in {'started', 'uncertain'}:
            reconciler = RECONCILERS.get(row['tool'])
            if reconciler:
                await self.checkpoint()
                result = await reconciler(copy.deepcopy(row['arguments']), row['idempotency_key'])
                if result is not None:
                    if not isinstance(result, str):
                        raise RecoveryBlocked('Reconciler must return a string outcome or None.')
                    self._stage_result(row, result)
                    return result
            if policy['mode'] == 'reconcile':
                await self._uncertain(row, 'External outcome requires reconciliation; no automatic replay.')
        if policy['mode'] == 'idempotent' and row['tool'] not in IDEMPOTENT_TOOLS:
            await self._uncertain(row, 'Idempotent adapter unavailable; no invocation or retry allowed.')
        while row['attempts'] < policy['max_attempts']:
            await self.checkpoint()
            row['attempts'] += 1
            row['status'], row['run_id'] = 'started', self.state.run_id
            self.state.execution_pending.append(dict(row))
            archive_record(self.state, 'tool_attempt', {'execution_id': row['execution_id'], 'attempt': row['attempts']})
            await self.persist({'type': 'tool_started', 'execution_id': row['execution_id'], 'attempt': row['attempts']})
            await self.checkpoint()
            try:
                if policy['mode'] == 'idempotent':
                    result = await IDEMPOTENT_TOOLS[row['tool']](copy.deepcopy(row['arguments']), row['idempotency_key'])
                else:
                    result = await dispatch(row['tool'], copy.deepcopy(row['arguments']))
                if not isinstance(result, str):
                    raise TypeError('Tool results must be strings')
            except asyncio.CancelledError:
                # started remains durable. The next owner reconciles/retries.
                raise
            except TransientToolError:
                archive_record(self.state, 'tool_attempt_failed', {'execution_id': row['execution_id'],
                    'attempt': row['attempts'], 'code': 'transient_tool_error'})
                await self.persist({'type': 'tool_attempt_failed', 'execution_id': row['execution_id'],
                    'attempt': row['attempts'], 'code': 'transient_tool_error'})
                if policy['mode'] == 'reconcile':
                    await self._uncertain(row, 'Transient failure may have produced an external effect.')
                if row['attempts'] < policy['max_attempts']:
                    continue
                result = 'ERROR: transient tool failure; automatic attempt limit reached.'
            except Exception:
                if policy['mode'] in {'reconcile', 'idempotent'}:
                    await self._uncertain(row, 'Tool raised without a confirmed external outcome.')
                result = 'ERROR: tool execution failed.'
            await self.checkpoint(check_cancel=False)
            self._stage_result(row, result)
            return result
        await self._uncertain(row, 'Automatic attempt limit exhausted; reconciliation required.')

    def _stage_result(self, row, result):
        row['status'] = 'failed' if result.startswith(('ERROR:', 'AssertionError:')) else 'succeeded'
        row['result'] = result
        row['run_id'] = self.state.run_id
        self.state.execution_pending.append(dict(row))
        archive_record(self.state, 'tool_outcome', {'execution_id': row['execution_id'], 'status': row['status'], 'result': result})

    async def _uncertain(self, row, reason):
        row['status'] = 'uncertain'
        self.state.execution_pending.append(dict(row))
        archive_record(self.state, 'tool_uncertain', {'execution_id': row['execution_id'], 'reason': reason})
        await self.persist({'type': 'tool_uncertain', 'execution_id': row['execution_id'], 'message': reason})
        raise RecoveryBlocked(reason)


async def reconcile_abandoned(repository, limit=100):
    """Discover/mark abandoned work without invoking a tool or model.

    Explicit /resume performs policy-aware continuation. Multiple workers may
    scan concurrently; claiming the lease decides who can reconcile each row.
    """
    reconciled = []
    for row in await repository.abandoned(limit):
        persisted = await repository.get(row['session_id'])
        if persisted is None:
            continue
        try:
            async with ExecutionCoordinator(repository, persisted.state, persisted.config) as execution:
                persisted = await repository.get(row['session_id'])
                execution.state = persisted.state
                execution.state.execution_owner, execution.state.execution_fence = execution.owner, execution.fence
                if execution.state.run_id != row['run_id'] or execution.state.run_status != 'running':
                    continue
                if execution.state.status == 'complete':
                    await execution.finish('complete', 'terminal_snapshot_recovered')
                else:
                    execution.state.status = 'failed'
                    execution.state.final_answer = 'Abandoned run detected; use /resume to reconcile pending work.'
                    execution.state.last_error = {'code': 'run_abandoned', 'message': execution.state.final_answer, 'recoverable': True}
                    await execution.finish('interrupted', 'worker_abandoned')
                reconciled.append({'session_id': row['session_id'], 'run_id': row['run_id']})
        except OwnershipConflict:
            continue
    return reconciled
