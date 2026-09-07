"""Direct Engine observations over a Machine's own Unix socket.

This is for the harness's OWN liveness observation of sibling Machines, never for
scenario evidence. Every Docker behaviour the compatibility contract requires is
proven through the Mac's installed unmodified CLI, as the release gate demands;
this module exists only because that CLI costs about 17 ms of process startup
before it contacts anything, and the sentinel monitor makes four such calls per
Machine per second for the length of a run. On a composed run that is minutes of
process spawn and, worse, continuous load on the very Engines the suites are
being timed against.

The observation itself is not weakened: the same Engine is asked the same
questions over the same endpoint the Docker context names. What it does not
exercise is the CLI's own context resolution, so the caller keeps a periodic
CLI check for that and records which path produced each observation.

No retries, no reconnection on error, bounded reads, and an explicit deadline:
an observation either completes or is reported as failed.
"""
from __future__ import annotations

import http.client
import json
import socket
import time

# The daemon's declared minimum API version, so an observation never depends on
# negotiation. The handshake suite proves the supported window separately.
API_VERSION = "v1.40"
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_FRAME_BYTES = 1 * 1024 * 1024
# How long an exec may take to report its terminal state after its stream ends.
EXEC_SETTLE_SECONDS = 5.0
STDOUT_STREAM = 1


class ProbeError(RuntimeError):
    """An observation did not complete exactly."""


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float):
        super().__init__("localhost", timeout=timeout)
        self._path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


class EngineProbe:
    """One Machine's Engine, addressed by the socket its Docker context names."""

    def __init__(self, endpoint: str, *, timeout: float = 8.0):
        if not endpoint.startswith("unix://"):
            raise ProbeError("engine probe requires a unix endpoint: " + repr(endpoint))
        self.path = endpoint[len("unix://"):]
        self.timeout = timeout
        self._connection = None

    def close(self):
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    def _request(self, method: str, path: str, body: bytes = None):
        # One connection is reused while it works; a failed exchange closes it
        # and surfaces, rather than silently retrying on a fresh socket.
        if self._connection is None:
            self._connection = _UnixConnection(self.path, self.timeout)
        headers = {"Host": "localhost", "Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            self._connection.request(method, path, body=body, headers=headers)
            response = self._connection.getresponse()
            payload = response.read(MAX_RESPONSE_BYTES + 1)
        except Exception as error:
            self.close()
            raise ProbeError(f"{method} {path} failed: {type(error).__name__}: {error}") from None
        if len(payload) > MAX_RESPONSE_BYTES:
            self.close()
            raise ProbeError(f"{method} {path} exceeded the response bound")
        return response.status, payload

    def get_json(self, path: str):
        status, payload = self._request("GET", f"/{API_VERSION}{path}")
        if status != 200:
            raise ProbeError(f"GET {path} returned {status}: {payload[:200]!r}")
        try:
            return json.loads(payload)
        except ValueError:
            raise ProbeError(f"GET {path} returned invalid JSON") from None

    def info(self):
        return self.get_json("/info")

    def container(self, container_id: str):
        return self.get_json(f"/containers/{container_id}/json")

    def exec_stdout(self, container_id: str, argv: list) -> bytes:
        """Run one command in a running container and return its stdout bytes.

        The exit status is checked and a non-empty stderr is an error, so a
        partial or failing observation can never look like a clean read.
        """
        created = self._post_json(f"/containers/{container_id}/exec", {
            "AttachStdout": True, "AttachStderr": True, "Tty": False, "Cmd": list(argv)})
        exec_id = created.get("Id")
        if not isinstance(exec_id, str) or not exec_id:
            raise ProbeError("exec create returned no id")
        stdout, stderr = self._start_exec(exec_id)
        # The stream closing means the output is complete, but the Engine can
        # still report the exec as running for a moment afterwards. Wait for the
        # terminal state within a bounded deadline; this observes one exec to
        # completion, it never retries the observation.
        deadline = time.monotonic() + EXEC_SETTLE_SECONDS
        while True:
            state = self.get_json(f"/exec/{exec_id}/json")
            if state.get("Running") is False:
                break
            if time.monotonic() >= deadline:
                raise ProbeError(f"exec never reached a terminal state: {state.get('Running')!r}")
            time.sleep(0.01)
        if state.get("ExitCode") != 0:
            raise ProbeError(f"exec exited {state.get('ExitCode')!r}")
        if stderr:
            raise ProbeError(f"exec wrote stderr: {stderr[:200]!r}")
        return stdout

    def _post_json(self, path: str, document: dict):
        status, payload = self._request("POST", f"/{API_VERSION}{path}",
                                        json.dumps(document).encode("utf-8"))
        if status not in (200, 201):
            raise ProbeError(f"POST {path} returned {status}: {payload[:200]!r}")
        try:
            return json.loads(payload)
        except ValueError:
            raise ProbeError(f"POST {path} returned invalid JSON") from None

    def _start_exec(self, exec_id: str):
        """Start an exec and read its stream straight from the socket.

        Docker hijacks this connection, so the multiplexed output follows the
        response headers on the same socket. `http.client` reports an upgraded
        response as having no body, so the exchange is written and read here
        directly and the connection is never reused.
        """
        body = json.dumps({"Detach": False, "Tty": False}).encode("utf-8")
        request = (
            f"POST /{API_VERSION}/exec/{exec_id}/start HTTP/1.1\r\n"
            "Host: localhost\r\nContent-Type: application/json\r\n"
            "Connection: Upgrade\r\nUpgrade: tcp\r\n"
            f"Content-Length: {len(body)}\r\n\r\n").encode("ascii") + body
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        try:
            connection.connect(self.path)
            connection.sendall(request)
            raw = b""
            while len(raw) <= MAX_RESPONSE_BYTES:
                chunk = connection.recv(65536)
                if not chunk:
                    break
                raw += chunk
        except Exception as error:
            raise ProbeError(f"exec start failed: {type(error).__name__}: {error}") from None
        finally:
            connection.close()
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ProbeError("exec output exceeded the response bound")
        if b"\r\n\r\n" not in raw:
            raise ProbeError("exec start returned no complete response head")
        head, stream = raw.split(b"\r\n\r\n", 1)
        status = head.split(b"\r\n", 1)[0].split(b" ")
        if len(status) < 2 or status[1] not in (b"200", b"101"):
            raise ProbeError(f"exec start returned {head.split(chr(13).encode(), 1)[0]!r}")
        return demultiplex(stream)


def demultiplex(raw: bytes):
    """Split Docker's multiplexed attach stream into stdout and stderr bytes."""
    stdout, stderr, offset = bytearray(), bytearray(), 0
    while offset < len(raw):
        if len(raw) - offset < 8:
            raise ProbeError("truncated stream frame header")
        stream = raw[offset]
        size = int.from_bytes(raw[offset + 4:offset + 8], "big")
        if size > MAX_FRAME_BYTES:
            raise ProbeError("stream frame exceeded the frame bound")
        offset += 8
        if len(raw) - offset < size:
            raise ProbeError("truncated stream frame payload")
        chunk = raw[offset:offset + size]
        offset += size
        if stream == STDOUT_STREAM:
            stdout += chunk
        else:
            stderr += chunk
    return bytes(stdout), bytes(stderr)
