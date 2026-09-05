"""Deterministic fixture service; observations still require the host harness."""
import json
import os
from pathlib import Path
import re
import socket
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import ProxyHandler, build_opener


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
    else:
        raise ValueError("unknown workload")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except Exception as error:
        print(type(error).__name__ + ": fixture operation failed", file=sys.stderr)
        sys.exit(1)
