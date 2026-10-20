"""Mark abandoned runs interrupted without executing tools/models; explicit resume continues work."""
import argparse
import asyncio
import json
from agent_python.config import ConfigRepository
from agent_python.persistence import SessionRepository


async def main(args):
    ConfigRepository()
    repo = SessionRepository()
    if args.apply:
        result = await repo.startup(args.limit)
        print(json.dumps({'reconciled':result},indent=2))
    else:
        print(json.dumps({'abandoned':await repo.startup(args.limit,recover=False)},indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true',help='Persist interrupted lifecycle state; otherwise only list abandoned runs')
    parser.add_argument('--limit',type=int,default=100)
    args = parser.parse_args()
    if not 1<=args.limit<=1000:
        parser.error('--limit must be 1..1000')
    asyncio.run(main(args))
