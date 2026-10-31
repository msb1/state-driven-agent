"""Exercise the test harness's actual socket shutdown and model gate over TCP."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from disconnect_integration_test import ControlledModel, SSEReader


class TransportTests(unittest.TestCase):
    def test_shutdown_reaches_server_with_reader_blocked(self):
        server_saw_eof = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(b'data: {"type": "status", "message": "active"}\n\n')
                self.wfile.flush()
                if self.rfile.read(1) == b"":
                    server_saw_eof.set()
                self.close_connection = True

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        reader = None
        try:
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "stream.sse"
                reader = SSEReader(f"http://127.0.0.1:{server.server_port}", "/stream", output, 5)
                self.assertTrue(reader.first_event.wait(5))
                self.assertFalse(reader.done.is_set())
                self.assertFalse(server_saw_eof.is_set())
                reader.disconnect(5)
                self.assertTrue(server_saw_eof.wait(5), "Server did not receive TCP EOF")
                self.assertTrue(reader.done.is_set())
                self.assertIsNone(reader.error)
                self.assertEqual(reader.events, [{"type": "status", "message": "active"}])
                self.assertIn(b'"active"', output.read_bytes())
        finally:
            if reader is not None:
                reader.close()
            server.shutdown()
            server.server_close()
            thread.join(5)

    def test_model_waits_for_gate_and_detects_extra_requests(self):
        # Bind zero inside ThreadingHTTPServer, then hand its free port to the
        # controlled fixture. This test runs in isolation from the live suite.
        with ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler) as reservation:
            port = reservation.server_port
        model = ControlledModel(f"http://127.0.0.1:{port}/v1")
        gate = model.arm("test", "trial-marker")
        result = {}

        def request():
            payload = json.dumps({"messages": [{"role": "user", "content": "trial-marker"}]}).encode()
            req = Request(f"http://127.0.0.1:{port}/v1/chat/completions", data=payload,
                          headers={"Content-Type": "application/json"})
            with build_opener(ProxyHandler({})).open(req, timeout=5) as response:
                result["body"] = json.loads(response.read())

        def first_request():
            try:
                request()
            except Exception as error:
                result["error"] = str(error)

        thread = threading.Thread(target=first_request, daemon=True)
        try:
            thread.start()
            self.assertTrue(gate.entered.wait(5))
            self.assertFalse(gate.responded.is_set())
            self.assertNotIn("body", result)
            gate.release.set()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertNotIn("error", result)
            self.assertEqual(result["body"]["choices"][0]["message"]["content"], "SOCKET_INTEGRATION_COMPLETE")
            self.assertTrue(gate.responded.is_set())
            with self.assertRaises(HTTPError):
                request()
            self.assertEqual(gate.requests, 2)
            self.assertIn("Unexpected additional model call", model.snapshot()["errors"][0])
        finally:
            model.close()
            thread.join(5)


if __name__ == "__main__":
    unittest.main()
