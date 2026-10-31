"""External HTTP/process observation controls; no agent imports or database writes."""
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
from urllib.parse import urlsplit

import httpx


class NotExercised(Exception):
    """A required external condition could not be exercised."""


def listener_identity(url):
    endpoint = urlsplit(url)
    if endpoint.hostname not in ('127.0.0.1', 'localhost', '::1'):
        raise NotExercised('Process identity verification currently requires local worker URLs')
    port = endpoint.port or (443 if endpoint.scheme == 'https' else 80)
    identities = []
    executable = shutil.which('lsof')
    if executable:
        result = subprocess.run([executable, '-nP', f'-iTCP:{port}', '-sTCP:LISTEN', '-t'],
                                capture_output=True, text=True, timeout=10)
        if result.returncode not in (0, 1):
            raise NotExercised('Cannot inspect listener identities with lsof')
        for pid in sorted(set(result.stdout.split())):
            started = subprocess.run(['ps', '-p', pid, '-o', 'lstart='], capture_output=True, text=True, timeout=10)
            if started.returncode == 0 and started.stdout.strip():
                identities.append({'pid': int(pid), 'started': started.stdout.strip()})
    elif Path('/proc/net/tcp').exists():
        inodes = set()
        for name in ('tcp', 'tcp6'):
            path = Path('/proc/net') / name
            if path.exists():
                for row in path.read_text().splitlines()[1:]:
                    fields = row.split()
                    if int(fields[1].rsplit(':', 1)[1], 16) == port and fields[3] == '0A':
                        inodes.add(fields[9])
        for process in Path('/proc').iterdir():
            if not process.name.isdigit():
                continue
            try:
                owned = any(os.readlink(fd) in {'socket:['+inode+']' for inode in inodes}
                            for fd in (process / 'fd').iterdir())
                if owned:
                    stat = (process / 'stat').read_text().rsplit(')', 1)[1].split()
                    identities.append({'pid': int(process.name), 'started': stat[19]})
            except (PermissionError, FileNotFoundError, ProcessLookupError):
                continue
    else:
        raise NotExercised('Install lsof to verify worker process identity on this platform')
    if not identities:
        raise NotExercised(f'Cannot establish a listening process for port {port}')
    return {'url': url, 'port': port, 'processes': sorted(identities, key=lambda row: row['pid']),
            'observed_at': time.time()}


@contextmanager
def suppress_response(target, timeout):
    """Forward one real request, consume its upstream response, then close downstream.

    The test caller receives no response. Only status/hash/timing metadata is used
    from the forwarder; the command outcome is recovered from PostgreSQL.
    """
    observations, errors = [], []
    finished = threading.Event()
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            try:
                with lock:
                    if observations:
                        raise AssertionError('Response-loss proxy received an extra request')
                body = self.rfile.read(int(self.headers['Content-Length']))
                with httpx.Client(timeout=timeout, trust_env=False) as upstream:
                    response = upstream.post(target.rstrip('/')+self.path, content=body,
                        headers={'Content-Type': 'application/json', 'Idempotency-Key': self.headers['Idempotency-Key']})
                observations.append({'key': self.headers['Idempotency-Key'], 'path': self.path,
                    'upstream_status': response.status_code,
                    'upstream_body_sha256': hashlib.sha256(response.content).hexdigest(),
                    'upstream_response_completed_at': time.time(), 'downstream_response_sent': False})
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            except Exception as error:
                errors.append(f'{type(error).__name__}: {error}')
                self.close_connection = True
            finally:
                finished.set()

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {'url': f'http://127.0.0.1:{server.server_port}', 'events': observations,
               'errors': errors, 'finished': finished}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
