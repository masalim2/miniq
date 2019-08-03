#!/usr/bin/env python
import argparse
import json
import sys
import asyncio
import websockets
import os

DEFAULT_PORT=9876
PORT = os.environ.get('MINIQ_PORT', DEFAULT_PORT)
SERVER_URI = f"ws://localhost:{PORT}"

async def ws_request(**data):
    async with websockets.connect(SERVER_URI) as ws:
        msg = json.dumps(data)
        await ws.send(msg)
        response = json.loads(await ws.recv())
        if response["status"] == 'OK':
            return response
        else:
            raise RuntimeError(str(response))

async def request_submit(args):
    args.script = os.path.abspath(args.script)
    if not os.path.isfile(args.script):
        raise ValueError(f'{args.script} is not a file')
    resp = await ws_request(action='submit', script=args.script,
                            num_nodes=args.num_nodes, minutes=args.minutes,
                            cwd=os.getcwd())
    print(resp)

async def request_status(args):
    resp = await ws_request(action='status', id=args.id)
    if not isinstance (resp['job_state'], list):
        jobstates = [resp['job_state']]
    else:
        jobstates = resp['job_state']
    for job in jobstates:
        if job is not None:
            id = job["job_id"]
            state = job["state"]
            print(f'{id:>4} {state:>12}')

async def request_delete(args):
    resp = await ws_request(action='delete', id=args.id)
    print(resp)

async def dispatch(args):
    assert args.action in ['submit', 'status', 'delete']
    handler = getattr(sys.modules[__name__], f"request_{args.action}")
    await handler(args)

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    subparser_submit = subparsers.add_parser('submit')
    subparser_submit.set_defaults(action='submit')
    subparser_submit.add_argument('script')
    subparser_submit.add_argument('-t', dest="minutes", type=int, required=True)
    subparser_submit.add_argument('-n', dest="num_nodes", type=int, required=True)

    subparser_status = subparsers.add_parser('status')
    subparser_status.set_defaults(action='status')
    subparser_status.add_argument('--id', default=None, required=False, type=int)

    subparser_delete = subparsers.add_parser('delete')
    subparser_delete.set_defaults(action='delete')
    subparser_delete.add_argument('id', type=int)

    args = parser.parse_args()
    if 'action' not in args:
        parser.print_help()
        sys.exit(1)
    asyncio.get_event_loop().run_until_complete(dispatch(args))
