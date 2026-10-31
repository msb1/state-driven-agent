"""Real loopback controls against fake HTTP services; no agent or PostgreSQL."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from du06_controls import listener_identity, suppress_response
from du06_client import Client, normalized_input


class ExternalControls(unittest.TestCase):
    def test_resume_fingerprint_defaults_are_contract_defaults(self):
        self.assertEqual(normalized_input('/sessions/id/resume', {'max_steps': 2}), {'max_steps': 2, 'content': None})
        self.assertEqual(normalized_input('/sessions/id/run', {}), {'max_steps': None})

    def test_missing_response_occurs_after_one_real_upstream_exchange(self):
        effects = {}
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                key = self.headers['Idempotency-Key']
                effects.setdefault(key, {'session': {'id': key}, 'effect_id': key})
                data = json.dumps(effects[key]).encode()
                self.send_response(201)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        upstream.daemon_threads = True
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{upstream.server_port}'
        try:
            with suppress_response(url, 5) as proxy, tempfile.TemporaryDirectory() as directory:
                client = Client(url, 'dbname=unused', directory, 5)
                try:
                    with self.assertRaises((httpx.RemoteProtocolError, httpx.ReadError)):
                        client.post_to(proxy['url'], '/sessions', {'user_prompt': 'unit'}, 'same-key')
                    self.assertTrue(proxy['finished'].wait(5))
                    self.assertFalse(proxy['errors'])
                    self.assertEqual(proxy['events'][0]['upstream_status'], 201)
                    self.assertFalse(proxy['events'][0]['downstream_response_sent'])
                    replay = client.post('/sessions', {'user_prompt': 'unit'}, 'same-key')
                    self.assertEqual(replay.json()['effect_id'], 'same-key')
                    self.assertEqual(len(effects), 1)
                    self.assertEqual(len(client.requests), 2)
                finally:
                    client.close()
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(5)

    def test_listener_observer_distinguishes_two_real_processes(self):
        # These are plain stdlib HTTP listeners, never production agent processes.
        with tempfile.TemporaryDirectory() as directory:
            workers = []
            try:
                for index in range(2):
                    path = Path(directory) / f'{index}.json'
                    script = (
                        'from http.server import HTTPServer, BaseHTTPRequestHandler\n'
                        'import json,sys\n'
                        'from pathlib import Path\n'
                        's=HTTPServer(("127.0.0.1",0),BaseHTTPRequestHandler)\n'
                        'Path(sys.argv[1]).write_text(json.dumps({"port":s.server_port}))\n'
                        's.serve_forever()\n'
                    )
                    process = subprocess.Popen([sys.executable, '-B', '-c', script, str(path)],
                                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                    workers.append(process)
                    deadline = time.monotonic()+5
                    while not path.exists() and process.poll() is None and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue(path.exists(), process.stderr.read().decode() if process.poll() is not None else 'listener not ready')
                identities = [listener_identity(f'http://127.0.0.1:{json.loads((Path(directory) / f"{index}.json").read_text())["port"]}')
                              for index in range(2)]
                self.assertIn(workers[0].pid, {p['pid'] for p in identities[0]['processes']})
                self.assertIn(workers[1].pid, {p['pid'] for p in identities[1]['processes']})
                self.assertTrue({p['pid'] for p in identities[0]['processes']}.isdisjoint({p['pid'] for p in identities[1]['processes']}))
            finally:
                for process in workers:
                    process.terminate() if process.poll() is None else None
                    process.wait(timeout=5)
                    process.stderr.close()


if __name__ == '__main__':
    unittest.main()
