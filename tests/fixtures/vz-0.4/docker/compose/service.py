"""Deterministic fixture service; observations still require the host harness."""
import json
import errno
import os
from pathlib import Path
import re
import signal
import socket
import sys
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener


def owner():
    value = os.environ.get("FIXTURE_OWNER", "")
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", value) is None:
        raise ValueError("missing exact fixture owner token")
    return value


def record(role, value):
    return f"vz04|{role}|{owner()}|{value}\n".encode()


def fetch(url):
    # Fixture paths never inherit a proxy from a base image or caller environment.
    with build_opener(ProxyHandler({})).open(url, timeout=2) as response:
        return response.read(4097)


class NoTransportRedirects(HTTPRedirectHandler):
    """A reachable redirect is HTTP evidence, never a second network attempt."""

    def redirect_request(self, *_args, **_kwargs):
        return None


@contextmanager
def transport_deadline():
    """Bound DNS resolution as well as connect/header reads in this CLI mode."""
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise ValueError("foreign process timer present")

    def expired(_signal, _frame):
        raise TimeoutError("fixture transport deadline")

    previous = signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, 2.5)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def transport(url):
    """Observe one bounded HTTP connection; the host must interpret controls.

    These are transport observations, not an assertion that isolation works.
    In particular, refusal may mean a stopped service and a DNS error may mean
    an unavailable resolver. The host brackets negative attempts with exact
    live-source and live-destination payload controls.
    """
    result = {"schema_version": 1, "url": url, "outcome": "probe_error",
              "status": None, "errno": None, "exception": None}
    try:
        if not isinstance(url, str) or not 1 <= len(url) <= 320:
            raise ValueError("bounded URL required")
        parsed = urlsplit(url)
        if (parsed.scheme != "http" or parsed.port != 8080 or
                parsed.username is not None or parsed.password is not None or
                parsed.path != "/health" or parsed.query or parsed.fragment or
                not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", parsed.hostname or "") or
                url != f"http://{parsed.hostname}:8080/health"):
            raise ValueError("exact fixture HTTP health URL required")
        opener = build_opener(ProxyHandler({}), NoTransportRedirects())
        try:
            with transport_deadline():
                with opener.open(url, timeout=2) as response:
                    result["status"] = response.status
        except HTTPError as error:
            # HTTPError proves an HTTP response, including denied/redirect/error
            # statuses. Never report it as a network-denied observation.
            result["status"] = error.code
            error.close()
        if type(result["status"]) is not int or not 100 <= result["status"] <= 599:
            raise ValueError("invalid HTTP response status")
        result["outcome"] = "http_response"
    except Exception as error:
        # Once valid HTTP headers arrived, a later close/deadline error cannot
        # erase that positive reachability observation.
        if type(result["status"]) is int and 100 <= result["status"] <= 599:
            result["outcome"] = "http_response"
            return result
        result["status"] = None
        underlying = error.reason if isinstance(error, URLError) else error
        result["exception"] = type(underlying).__name__[:64]
        number = getattr(underlying, "errno", None)
        result["errno"] = number if type(number) is int else None
        if isinstance(underlying, socket.gaierror):
            result["outcome"] = "dns_failure"
        elif isinstance(underlying, TimeoutError) or (
                isinstance(underlying, OSError) and number == errno.ETIMEDOUT):
            result["outcome"] = "timeout"
        elif isinstance(underlying, OSError) and number == errno.ECONNREFUSED:
            result["outcome"] = "connection_refused"
        elif isinstance(underlying, OSError) and number in (errno.ENETUNREACH, errno.EHOSTUNREACH):
            result["outcome"] = "network_unreachable"
    return result


def serve(role):
    token = owner()
    if role == "db":
        state = Path("/data/sentinel.txt")
        expected = record("db", "persisted")
        if state.exists():
            if state.read_bytes() != expected:
                raise ValueError("foreign or corrupted persistent state")
        else:
            state.write_bytes(expected)
    if role in ("api", "worker"):
        dependency = "db" if role == "api" else "api"
        expected = record(dependency, "ready")
        if fetch(f"http://{dependency}:8080/health") != expected:
            raise ValueError("dependency did not become healthy before start")
        print(record(role, "dependency-healthy").decode(), end="", flush=True)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                ready = os.environ.get("FIXTURE_BLOCK_HEALTH", "0") == "0"
                body = record(role, "ready" if ready else "blocked")
                code = 200 if ready else 503
            elif self.path == "/value" and role == "db":
                body, code = Path("/data/sentinel.txt").read_bytes(), 200
            elif self.path == "/value" and role == "api":
                body, code = fetch("http://db:8080/value"), 200
            elif self.path == "/identity":
                body = (json.dumps({"owner": token, "role": role, "hostname": socket.gethostname()},
                                   sort_keys=True, separators=(",", ":")) + "\n").encode()
                code = 200
            else:
                body, code = record(role, "not-found"), 404
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = HTTPServer(("0.0.0.0", 8080), Handler)
    print(record(role, "listening").decode(), end="", flush=True)
    server.serve_forever()


def main(args):
    mode = args[0]
    if mode in ("db", "api", "worker", "isolated"):
        serve(mode)
    elif mode == "health":
        if fetch("http://127.0.0.1:8080/health") != record(args[1], "ready"):
            raise ValueError("unhealthy")
    elif mode == "exec":
        sys.stdout.buffer.write(record("api", "exec-stdout"))
        sys.stderr.buffer.write(record("api", "exec-stderr"))
        return 37
    elif mode == "failure":
        if fetch("http://api:8080/health") != record("api", "ready"):
            raise ValueError("failure job launched before API healthy")
        sys.stdout.buffer.write(record("failure", "exit-37"))
        return 37
    elif mode == "probe":
        sys.stdout.buffer.write(fetch(args[1]))
    elif mode == "transport":
        result = transport(args[1])
        sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        return 2 if result["outcome"] == "probe_error" else 0
    else:
        raise ValueError("unknown workload")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception as error:
        print(type(error).__name__ + ": fixture operation failed", file=sys.stderr)
        sys.exit(1)
