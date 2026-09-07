"""Engine probe tests against a real Unix-socket HTTP server, no Docker."""
import json
import socket
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import linux_docker_engine_probe as subject  # noqa: E402


def frame(stream: int, payload: bytes) -> bytes:
    return bytes([stream, 0, 0, 0]) + len(payload).to_bytes(4, "big") + payload


class FakeEngine:
    """Answers the exact routes the probe uses; every reply is scripted."""

    def __init__(self, routes):
        self.routes, self.requests = routes, []
        self.directory = tempfile.mkdtemp()
        self.path = str(Path(self.directory) / "engine.sock")
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(self.path)
        self.server.listen(16)
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        while True:
            try:
                connection, _ = self.server.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(connection,), daemon=True).start()

    def _handle(self, connection):
        try:
            raw = b""
            while b"\r\n\r\n" not in raw:
                chunk = connection.recv(65536)
                if not chunk:
                    return
                raw += chunk
            head, rest = raw.split(b"\r\n\r\n", 1)
            length = 0
            for line in head.split(b"\r\n")[1:]:
                if line.lower().startswith(b"content-length:"):
                    length = int(line.split(b":", 1)[1])
            while len(rest) < length:
                chunk = connection.recv(65536)
                if not chunk:
                    break
                rest += chunk
            method, path, _ = head.split(b"\r\n", 1)[0].decode("latin-1").split(" ")
            self.requests.append((method, path))
            reply = self.routes.get((method, path))
            if reply is None:
                body, status, ctype = b'{"message":"no route"}', 404, "application/json"
            else:
                status, ctype, body = reply
            connection.sendall(
                f"HTTP/1.1 {status} X\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n".encode() + body)
        except OSError:
            return
        finally:
            connection.close()

    def close(self):
        self.server.close()


def json_route(document):
    return (200, "application/json", json.dumps(document).encode())


class EngineProbeTests(unittest.TestCase):
    def build(self, routes):
        engine = FakeEngine(routes)
        self.addCleanup(engine.close)
        probe = subject.EngineProbe("unix://" + engine.path)
        self.addCleanup(probe.close)
        return engine, probe

    def test_rejects_a_non_unix_endpoint(self):
        for endpoint in ("tcp://127.0.0.1:2375", "", "/plain/path"):
            with self.subTest(endpoint=endpoint), self.assertRaises(subject.ProbeError):
                subject.EngineProbe(endpoint)

    def test_reads_info_and_container_at_the_pinned_api_version(self):
        version = subject.API_VERSION
        engine, probe = self.build({
            ("GET", f"/{version}/info"): json_route({"ID": "engine-1"}),
            ("GET", f"/{version}/containers/abc/json"): json_route({"Id": "abc", "State": {"Running": True}}),
        })
        self.assertEqual(probe.info()["ID"], "engine-1")
        self.assertEqual(probe.container("abc")["State"]["Running"], True)
        self.assertEqual(engine.requests,
                         [("GET", f"/{version}/info"), ("GET", f"/{version}/containers/abc/json")])

    def test_non_200_and_invalid_json_are_errors(self):
        version = subject.API_VERSION
        _, probe = self.build({("GET", f"/{version}/info"): (500, "application/json", b'{"message":"boom"}')})
        with self.assertRaises(subject.ProbeError):
            probe.info()
        _, other = self.build({("GET", f"/{version}/info"): (200, "application/json", b"not json")})
        with self.assertRaises(subject.ProbeError):
            other.info()

    def test_missing_socket_is_an_error_not_a_hang(self):
        probe = subject.EngineProbe("unix:///nonexistent/engine.sock", timeout=1.0)
        with self.assertRaises(subject.ProbeError):
            probe.info()

    def test_exec_returns_only_stdout_and_checks_the_exit_status(self):
        version = subject.API_VERSION
        engine, probe = self.build({
            ("POST", f"/{version}/containers/abc/exec"): json_route({"Id": "exec-1"}),
            ("POST", f"/{version}/exec/exec-1/start"): (200, "application/vnd.docker.raw-stream",
                                                        frame(1, b"token\n")),
            ("GET", f"/{version}/exec/exec-1/json"): json_route({"Running": False, "ExitCode": 0}),
        })
        self.assertEqual(probe.exec_stdout("abc", ["/bin/cat", "/sentinel"]), b"token\n")
        self.assertIn(("POST", f"/{version}/containers/abc/exec"), engine.requests)

    def test_exec_rejects_a_failed_status_or_any_stderr(self):
        version = subject.API_VERSION
        _, failed = self.build({
            ("POST", f"/{version}/containers/abc/exec"): json_route({"Id": "exec-1"}),
            ("POST", f"/{version}/exec/exec-1/start"): (200, "application/vnd.docker.raw-stream", frame(1, b"x")),
            ("GET", f"/{version}/exec/exec-1/json"): json_route({"Running": False, "ExitCode": 1}),
        })
        with self.assertRaises(subject.ProbeError):
            failed.exec_stdout("abc", ["/bin/false"])
        _, noisy = self.build({
            ("POST", f"/{version}/containers/abc/exec"): json_route({"Id": "exec-1"}),
            ("POST", f"/{version}/exec/exec-1/start"): (200, "application/vnd.docker.raw-stream",
                                                        frame(1, b"x") + frame(2, b"warning")),
            ("GET", f"/{version}/exec/exec-1/json"): json_route({"Running": False, "ExitCode": 0}),
        })
        with self.assertRaises(subject.ProbeError):
            noisy.exec_stdout("abc", ["/bin/cat"])

    def test_exec_rejects_a_still_running_status(self):
        version = subject.API_VERSION
        _, probe = self.build({
            ("POST", f"/{version}/containers/abc/exec"): json_route({"Id": "exec-1"}),
            ("POST", f"/{version}/exec/exec-1/start"): (200, "application/vnd.docker.raw-stream", frame(1, b"x")),
            ("GET", f"/{version}/exec/exec-1/json"): json_route({"Running": True, "ExitCode": None}),
        })
        with self.assertRaises(subject.ProbeError):
            probe.exec_stdout("abc", ["/bin/cat"])

    def test_exec_create_without_an_id_is_an_error(self):
        version = subject.API_VERSION
        _, probe = self.build({("POST", f"/{version}/containers/abc/exec"): json_route({"Warnings": []})})
        with self.assertRaises(subject.ProbeError):
            probe.exec_stdout("abc", ["/bin/cat"])

    def test_demultiplex_splits_streams_and_rejects_truncation(self):
        stdout, stderr = subject.demultiplex(frame(1, b"out") + frame(2, b"err") + frame(1, b"more"))
        self.assertEqual((stdout, stderr), (b"outmore", b"err"))
        self.assertEqual(subject.demultiplex(b""), (b"", b""))
        for bad in (frame(1, b"abc")[:5], frame(1, b"abcdef")[:-2],
                    bytes([1, 0, 0, 0]) + (subject.MAX_FRAME_BYTES + 1).to_bytes(4, "big")):
            with self.subTest(bad=bad[:8]), self.assertRaises(subject.ProbeError):
                subject.demultiplex(bad)


if __name__ == "__main__":
    unittest.main()
