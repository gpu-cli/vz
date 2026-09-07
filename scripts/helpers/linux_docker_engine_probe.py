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

# The daemon's declared minimum API version, so an observation never depends on
# negotiation. The handshake suite proves the supported window separately.
API_VERSION = "v1.40"
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_FRAME_BYTES = 1 * 1024 * 1024
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
        state = self.get_json(f"/exec/{exec_id}/json")
        if state.get("Running") is not False or state.get("ExitCode") != 0:
            raise ProbeError(f"exec did not complete cleanly: {state.get('Running')!r} {state.get('ExitCode')!r}")
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
        """Start an exec and demultiplex its hijacked stream."""
        body = json.dumps({"Detach": False, "Tty": False}).encode("utf-8")
        connection = _UnixConnection(self.path, self.timeout)
        try:
            connection.request("POST", f"/{API_VERSION}/exec/{exec_id}/start", body=body,
                               headers={"Host": "localhost", "Content-Type": "application/json",
                                        "Connection": "Upgrade", "Upgrade": "tcp"})
            response = connection.getresponse()
            if response.status not in (200, 101):
                raise ProbeError(f"exec start returned {response.status}")
            raw = response.read(MAX_RESPONSE_BYTES + 1)
        except ProbeError:
            raise
        except Exception as error:
            raise ProbeError(f"exec start failed: {type(error).__name__}: {error}") from None
        finally:
            connection.close()
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ProbeError("exec output exceeded the response bound")
        return demultiplex(raw)


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
