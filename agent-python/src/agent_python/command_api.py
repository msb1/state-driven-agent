"""Shared HTTP command admission around the normal agent routes."""
from uuid import UUID

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .commands import CommandError, command_scope, envelope
from .identity import strict_json
from .execution import OwnershipConflict, LeaseLost
from .models import CreateSessionRequest, AppendUserMessageRequest, RunRequest, ResumeSessionRequest, CancelSessionRequest


class ReplayCommand(Exception):
    def __init__(self, row):
        self.row = row


def replay_response(row):
    headers = {'Idempotency-Command-Id': row['command_id']}
    if row['status'] in ('succeeded','failed'):
        return JSONResponse(status_code=row['response_status'],content=row['response_body'],
                            headers={**headers,'Idempotency-Replayed':'true'})
    return JSONResponse(status_code=202,content=envelope(row),headers={**headers,'Retry-After':'1'})


async def command_dependency(request: Request):
    if request.method != 'POST':
        yield None
        return
    key = request.headers.get('Idempotency-Key')
    if key is None:
        raise CommandError('idempotency_key_required','Idempotency-Key is required',422)
    if len(request.headers.getlist('Idempotency-Key')) != 1 or not 1 <= len(key) <= 255 or any(not 33 <= ord(c) <= 126 for c in key):
        raise CommandError('idempotency_key_invalid','Idempotency-Key must be 1..255 printable ASCII characters without spaces',422)
    path = request.url.path
    if 'session_id' in request.path_params:
        try:
            sid = str(UUID(request.path_params['session_id']))
        except ValueError as error:
            raise HTTPException(422,'session_id must be a UUID') from error
        path = path.replace(request.path_params['session_id'],sid,1)
    route = request.scope['route'].path
    models = {'/sessions':CreateSessionRequest,'/sessions/{session_id}/messages':AppendUserMessageRequest,
              '/sessions/{session_id}/run':RunRequest,'/sessions/{session_id}/run/stream':RunRequest,
              '/sessions/{session_id}/resume':ResumeSessionRequest,'/sessions/{session_id}/cancel':CancelSessionRequest}
    try:
        body = models[route].model_validate(strict_json(await request.body())).model_dump(mode='json')
    except (ValueError,KeyError,ValidationError) as error:
        raise HTTPException(422,'Invalid command request: ' + str(error)) from error
    repository = request.app.state.repository()
    store = repository.commands
    row = await store.get(key=key)
    if row:
        store.matches(row,path,body)
    else:
        admission = await request.app.state.prepare_command(path,body)
        row = await store.register(key,path,body,admission)
    if row['status'] not in ('pending','started') or row.get('run_id'):
        raise ReplayCommand(row)
    command = await store.claim(row)
    if command is None:
        raise ReplayCommand(await store.get(key=key))
    request.state.command_id = command.id
    async with command_scope(store,command):
        try:
            yield command
        except HTTPException as error:
            await store.finish_without_save(command,error.status_code,{'detail':error.detail},'failed')
            raise
        except OwnershipConflict as error:
            if isinstance(error, LeaseLost):
                await store.uncertain(command)
            else:
                await store.finish_without_save(command,409,{'type':'error','code':error.code,
                    'message':str(error),'recoverable':True},'failed')
            raise
        except Exception:
            await store.uncertain(command)
            raise
