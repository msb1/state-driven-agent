"""Explicit keep-all plan, consistent export, or verified telemetry-only pruning."""
import argparse
import asyncio
import json
from pathlib import Path
from agent_python.config import ConfigRepository
from agent_python.persistence import SessionRepository


async def main(args):
    ConfigRepository();repo=SessionRepository()
    await repo.initialize()
    if args.action=='plan':
        result=await repo.retention_plan(args.session_id,args.keep_diagnostics,args.keep_events)
    elif args.action=='export':
        result=await repo.export_archive(args.session_id,args.archive)
    else:
        result=await repo.prune_telemetry(args.session_id,args.archive,args.keep_diagnostics,args.keep_events)
    print(json.dumps(result,indent=2,default=str))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['plan','export','prune-telemetry'])
    parser.add_argument('--session-id',required=True)
    parser.add_argument('--archive',type=Path)
    parser.add_argument('--keep-diagnostics',type=int,default=32)
    parser.add_argument('--keep-events',type=int,default=100)
    args=parser.parse_args()
    if args.action!='plan' and args.archive is None:
        parser.error('--archive is required for export/prune-telemetry')
    asyncio.run(main(args))
