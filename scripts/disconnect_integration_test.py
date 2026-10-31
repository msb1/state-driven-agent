#!/usr/bin/env python3
"""Reusable real-TCP SSE tests. Standard library only; no agent imports.

Run against an already running agent. Gates determine ordering; timeouts only
bound waits for missing signals. Never calls POST /cancel or modifies fixtures.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

ANSWER = "SOCKET_INTEGRATION_COMPLETE"
# These functions are called by run_suite, not pytest fixture injection.
__test__ = False


class FixtureError(RuntimeError):
    """The fixture or its connection to the agent could not be established."""


def require(condition, message):
    if not condition:
        raise AssertionError(message)


@dataclass
class ModelGate:
    name: str
    marker: str
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    responded: threading.Event = field(default_factory=threading.Event)
    requests: int = 0


class ControlledModel:
    """One request per named gate; unexpected calls remain visible in evidence."""

    def __init__(self, url):
        address = urlsplit(url)
        if address.scheme != "http" or address.hostname not in {"127.0.0.1", "localhost"} or not address.port:
            raise FixtureError("Model URL must be local HTTP with an explicit port")
        self.lock = threading.Lock()
        self.current = None
        self.gates = []
        self.errors = []
        model = self
        route = address.path.rstrip("/") + "/chat/completions"

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def do_POST(self):
                gate = None
                try:
                    if self.path != route:
                        raise FixtureError(f"Unexpected model route: {self.path}")
                    payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                    with model.lock:
                        gate = model.current
                        if gate is None:
                            raise FixtureError("Model request arrived without an armed gate")
                        gate.requests += 1
                        if gate.requests != 1:
                            raise AssertionError(f"Unexpected additional model call at {gate.name}")
                        if not any(gate.marker in str(m.get("content", "")) for m in payload.get("messages", [])):
                            raise FixtureError("Model request did not contain this trial's marker; check configuration")
                    gate.entered.set()
                    gate.release.wait()
                    body = json.dumps({"id": gate.name, "object": "chat.completion", "choices": [{
                        "index": 0, "message": {"role": "assistant", "content": ANSWER},
                        "finish_reason": "stop",
                    }]}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    self.wfile.flush()
                    gate.responded.set()
                except Exception as error:
                    with model.lock:
                        model.errors.append(f"{type(error).__name__}: {error}")
                    try:
                        self.send_error(500, "Controlled model fixture failed; see model.json")
                    except OSError:
                        pass

        try:
            self.server = ThreadingHTTPServer((address.hostname, address.port), Handler)
        except OSError as error:
            raise FixtureError(f"Cannot bind controlled model at {url}: {error}") from error
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def arm(self, name, marker):
        with self.lock:
            gate = ModelGate(name, marker)
            self.gates.append(gate)
            self.current = gate
            return gate

    def snapshot(self):
        with self.lock:
            return {"gates": [{"name": g.name, "requests": g.requests, "entered": g.entered.is_set(),
                               "released": g.release.is_set(), "responded": g.responded.is_set()}
                              for g in self.gates], "errors": list(self.errors)}

    def close(self):
        for gate in self.gates:
            gate.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class SSEReader:
    """Actively consume SSE; retain the real socket independently of HTTP wrappers."""

    def __init__(self, agent_url, path, output, timeout):
        endpoint = urlsplit(agent_url)
        self.connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=timeout)
        self.connection.connect()
        self.socket = self.connection.sock
        self.key = str(uuid4())
        Path(str(output) + '.request-key').write_text(self.key + '\n')
        self.connection.request("POST", endpoint.path.rstrip("/") + path,
                                body=json.dumps({"max_steps": 2}), headers={"Content-Type": "application/json", "Idempotency-Key": self.key})
        self.response = self.connection.getresponse()
        if self.response.status != 200:
            status = self.response.status
            self.response.close()
            self.connection.close()
            raise AssertionError(f"SSE endpoint returned HTTP {status}")
        if "text/event-stream" not in self.response.getheader("Content-Type", ""):
            self.response.close()
            self.connection.close()
            raise FixtureError("Endpoint did not return an SSE response")
        self.first_event = threading.Event()
        self.done = threading.Event()
        self.closing = False
        self.events = []
        self.error = None
        self.output = output
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        try:
            with self.output.open("wb") as evidence:
                data = []
                while line := self.response.readline():
                    evidence.write(line)
                    evidence.flush()
                    if line.rstrip(b"\r\n") == b"":
                        if data:
                            self.events.append(json.loads(b"\n".join(data)))
                            self.first_event.set()
                            data.clear()
                    elif line.startswith(b"data:"):
                        data.append(line[5:].strip())
        except Exception as error:
            if not self.closing:
                self.error = f"{type(error).__name__}: {error}"
        finally:
            self.done.set()

    def disconnect(self, timeout):
        require(not self.done.is_set(), "SSE reader ended before deliberate socket shutdown")
        self.closing = True
        # shutdown is essential: closing an HTTPResponse alone can leave a socket
        # held by its file wrapper or a concurrently blocked reader.
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        require(self.done.wait(timeout), "SSE reader did not exit after socket shutdown")
        self.response.close()
        self.connection.close()

    def close(self):
        self.closing = True
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()
        self.thread.join(timeout=2)
        if not self.thread.is_alive():
            self.response.close()
        self.connection.close()


class Harness:
    def __init__(self, agent_url, model_url, config_ref, output, timeout=120):
        address = urlsplit(agent_url)
        if address.scheme != "http" or address.hostname not in {"127.0.0.1", "localhost"}:
            raise FixtureError("Agent URL must use direct local HTTP, without a proxy")
        if (address.hostname, address.port) == (urlsplit(model_url).hostname, urlsplit(model_url).port):
            raise FixtureError("Agent and controlled model must use different ports")
        self.agent_url = agent_url.rstrip("/")
        self.config_ref, self.output, self.timeout = config_ref, Path(output), timeout
        self.output.mkdir(parents=True, exist_ok=False)
        self.trace, self.readers = [], []
        self.session_id = None
        self.model = ControlledModel(model_url)

    def save(self, name, value):
        (self.output / name).write_text(json.dumps(value, indent=2) + "\n")

    def mark(self, signal, **details):
        self.trace.append({"signal": signal, "monotonic_seconds": time.monotonic(), **details})
        self.save("signals.json", self.trace)
        print(f"  {signal}", flush=True)

    def api(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        key = str(uuid4())
        if method == 'POST':
            with (self.output / 'request-keys.jsonl').open('a') as stream:
                stream.write(json.dumps({'key':key,'path':path}) + '\n')
        request = Request(self.agent_url + path, data=data, method=method,
                          headers={"Content-Type": "application/json", "Idempotency-Key": key} if data else {})
        try:
            with build_opener(ProxyHandler({})).open(request, timeout=self.timeout) as response:
                return json.loads(response.read())
        except HTTPError as error:
            detail = error.read().decode("utf-8", "replace")
            raise FixtureError(f"API {method} {path} returned HTTP {error.code}: {detail}") from error

    def wait(self, label, read, predicate):
        deadline = time.monotonic() + self.timeout
        last = None
        while True:
            last = read()
            if predicate(last):
                self.mark(label)
                return last
            if self.model.errors:
                raise FixtureError(f"Controlled model failed: {self.model.snapshot()}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"Watchdog expired waiting for {label}; last observation: {last}")
            # Poll spacing reduces API load; it never determines when a gate opens.
            threading.Event().wait(min(0.1, remaining))

    def signal(self, event, label):
        if not event.wait(self.timeout):
            raise FixtureError(f"Watchdog expired waiting for {label}; model={self.model.snapshot()}")
        self.mark(label)

    def state(self):
        return self.api("GET", f"/sessions/{self.session_id}")

    def runs(self):
        return self.api("GET", f"/sessions/{self.session_id}/runs?limit=100")

    def transcript(self):
        page = self.api("GET", f"/sessions/{self.session_id}/transcript?after_sequence=0&limit=1000")
        require(page.get("history_complete") is True, "Transcript history is incomplete")
        require(len(page["entries"]) == page["through_sequence"], "Trial transcript was truncated")
        return page

    def start(self, name):
        marker = f"socket-trial-{uuid4()}"
        gate = self.model.arm(name, marker)
        created = self.api("POST", "/sessions", {"config_ref": self.config_ref,
                            "user_prompt": f"{marker}: Return the exact final answer {ANSWER}."})
        self.session_id = created["session"]["id"]
        self.save(f"{name}-created.json", created)
        reader = self.stream(name, "run/stream")
        self.signal(reader.first_event, f"{name}: SSE event received")
        self.signal(gate.entered, f"{name}: model request held")
        state = self.state()
        require(state.get("run_status") == "running" and state.get("run_id"), "No committed active run at model gate")
        require(not reader.done.is_set(), "Stream ended while model response was held")
        require(not gate.release.is_set(), "Model gate was released prematurely")
        self.save(f"{name}-active.json", state)
        return gate, reader, state

    def stream(self, name, action):
        reader = SSEReader(self.agent_url, f"/sessions/{self.session_id}/{action}",
                           self.output / f"{name}.sse", self.timeout)
        self.readers.append(reader)
        return reader

    def completed(self, reader, gate, name):
        self.signal(reader.done, f"{name}: SSE reached EOF")
        require(reader.error is None, f"SSE reader failed: {reader.error}")
        require(not any(e.get("type") == "error" for e in reader.events), f"SSE returned an error: {reader.events}")
        require(sum(e.get("type") == "final_result" for e in reader.events) == 1, "Expected exactly one SSE final result")
        state = self.state()
        self.save(f"{name}-final.json", state)
        require(state.get("status") == state.get("run_status") == "complete", "Run did not complete")
        require(state.get("final_answer") == ANSWER, "Unexpected final answer")
        require(gate.requests == 1 and gate.responded.is_set(), "Expected exactly one successful model exchange")
        return state

    def diagnostics(self):
        self.save("model.json", self.model.snapshot())
        if self.session_id:
            for name, read in (("state", self.state), ("runs", self.runs), ("transcript", self.transcript)):
                try:
                    self.save(f"diagnostic-{name}.json", read())
                except Exception as error:
                    self.save(f"diagnostic-{name}.json", {"error": str(error)})

    def close(self):
        for reader in self.readers:
            reader.close()
        self.model.close()


def test_connected_stream_completes(h):
    """Prove that this fixture/config can complete without disconnecting."""
    gate, reader, _ = h.start("connected")
    gate.release.set()
    h.completed(reader, gate, "connected")


def test_disconnect_requests_cancellation(h):
    """Close TCP with a model invocation held; synchronize on durable notification."""
    gate, reader, active = h.start("disconnect")
    reader.disconnect(h.timeout)
    h.mark("disconnect: TCP shutdown completed", run_id=active["run_id"])

    def matching_run():
        return next((r for r in h.runs()["runs"] if r["run_id"] == active["run_id"]), {})

    requested = h.wait("disconnect: durable cancel_requested", matching_run,
                       lambda row: row.get("cancel_requested") is True)
    h.save("disconnect-requested.json", requested)
    require(not gate.release.is_set() and not gate.responded.is_set(), "Invocation settled before cancellation notification")
    gate.release.set()
    h.mark("disconnect: model response released")
    cancelled = h.wait("disconnect: durable cancelled", h.state, lambda s: s.get("run_status") == "cancelled")
    require(cancelled["run_id"] == active["run_id"], "Cancellation targeted a different run")
    require((cancelled.get("last_error") or {}).get("code") == "run_cancelled", "Missing run_cancelled error")
    h.save("disconnect-cancelled.json", cancelled)
    before = h.transcript()
    h.save("disconnect-transcript-before.json", before)
    require(h.state() == cancelled, "Stopped session changed before resume")
    require(h.transcript()["entries"] == before["entries"], "Stopped transcript changed before resume")
    require(gate.requests == 1 and gate.responded.is_set(), "Cancelled run scheduled another model call or lost the response")
    return active, before, gate


def test_resume_preserves_committed_work(h, active, before, cancelled_gate):
    """Resume the cancelled trial; inspect run identities, fence and immutable history."""
    marker = before["entries"][0]["payload"]["content"].split(":", 1)[0]
    gate = h.model.arm("resume", marker)
    reader = h.stream("resume", "resume")
    h.signal(reader.first_event, "resume: SSE event received")
    h.signal(gate.entered, "resume: model request held")
    gate.release.set()
    final = h.completed(reader, gate, "resume")
    require(final["run_id"] != active["run_id"], "Resume reused the cancelled run ID")
    require(final["goal"] == active["goal"] and final["config_sha256"] == active["config_sha256"],
            "Resume changed the goal or immutable configuration")
    runs = h.runs()
    h.save("resume-runs.json", runs)
    by_id = {r["run_id"]: r for r in runs["runs"]}
    old, new = by_id[active["run_id"]], by_id[final["run_id"]]
    require(len(by_id) == 2 and old["status"] == "cancelled" and new["status"] == "complete",
            "Expected exactly one cancelled run and one completed resume")
    require(new["fence"] > old["fence"], "Resume did not advance the execution fence")
    after = h.transcript()
    h.save("resume-transcript-after.json", after)
    require(after["entries"][:len(before["entries"])] == before["entries"], "Resume rewrote committed transcript entries")
    require(cancelled_gate.requests == 1 and gate.requests == 1, "Unexpected model invocation count")
    require(not h.model.errors, f"Model fixture errors: {h.model.snapshot()}")


def run_suite(h):
    for test in (test_connected_stream_completes, test_disconnect_requests_cancellation):
        h.mark(f"START {test.__name__}")
        result = test(h)
        h.mark(f"PASS {test.__name__}")
    h.mark("START test_resume_preserves_committed_work")
    test_resume_preserves_committed_work(h, *result)
    h.mark("PASS test_resume_preserves_committed_work")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-url", default=os.getenv("AGENT_URL", "http://127.0.0.1:8081"))
    parser.add_argument("--model-url", default=os.getenv("AUX_SOCKET_MODEL_URL", "http://127.0.0.1:18089/v1"))
    parser.add_argument("--config-ref", default="socket-integration.yaml")
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--watchdog-seconds", type=float, default=120)
    args = parser.parse_args(argv)
    if not 0 < args.watchdog_seconds < 300:
        parser.error("watchdog must be greater than zero and less than 300 seconds")
    h = None
    new_output = not args.evidence_dir.exists()
    try:
        h = Harness(args.agent_url, args.model_url, args.config_ref, args.evidence_dir, args.watchdog_seconds)
        run_suite(h)
        h.diagnostics()
        h.save("result.json", {"status": "passed", "tests": 3})
        print(f"PASS socket integration suite; evidence: {h.output}", flush=True)
        return 0
    except Exception as error:
        kind = "assertion" if isinstance(error, AssertionError) else "fixture_or_transport"
        if h is not None:
            h.diagnostics()
            h.save("result.json", {"status": "failed", "kind": kind, "error": str(error)})
        elif new_output and args.evidence_dir.is_dir():
            (args.evidence_dir / "result.json").write_text(json.dumps(
                {"status": "failed", "kind": kind, "error": str(error)}, indent=2) + "\n")
        print(f"FAIL socket integration suite ({kind}): {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        if h is not None:
            h.close()


if __name__ == "__main__":
    raise SystemExit(main())
