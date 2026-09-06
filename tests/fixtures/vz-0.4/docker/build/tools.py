"""Container build workload. Never call Docker or accept a mutable base pin."""
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys


def required(name, pattern):
    value = os.environ.get(name, "")
    if re.fullmatch(pattern, value) is None:
        raise ValueError("invalid or missing " + name)
    return value


def output(name, value):
    Path("/out").mkdir(exist_ok=True)
    destination = Path("/out", name)
    destination.write_bytes(value)
    # These are public fixture payloads. Their export mode must not depend on
    # the inherited BuildKit/runtime umask (and secret input modes are separate).
    destination.chmod(0o644)


def main(mode):
    required("FIXTURE_BASE", r"[^\s@]+@sha256:[0-9a-f]{64}")
    if mode == "payload":
        variant = required("FIXTURE_VARIANT", r"alpha|beta")
        required("FIXTURE_RUN", r"[a-z0-9][a-z0-9-]{0,63}")
        if Path("/fixture/input.txt").read_bytes() != b"vz04-build-input-v1\n":
            raise ValueError("input differs from fixture")
        output("payload.txt", ("vz04-build-v1\nvariant=" + variant + "\n").encode())
        print("vz04-payload-step-executed", flush=True)
    elif mode == "secret":
        expected = required("FIXTURE_SECRET_SHA256", r"[0-9a-f]{64}")
        if hashlib.sha256(Path("/run/secrets/fixture").read_bytes()).hexdigest() != expected:
            raise ValueError("secret digest mismatch")
        output("secret.txt", b"vz04-secret-mount-ok-v1\n")
    elif mode == "cache":
        owner = required("FIXTURE_OWNER", r"[a-z0-9][a-z0-9-]{0,63}")
        expected = required("FIXTURE_CACHE_EXPECT", r"cold|warm")
        step = required("FIXTURE_CACHE_STEP", r"[a-z0-9][a-z0-9-]{0,63}")
        marker = Path("/cache/owner")
        if expected == "cold":
            if marker.exists():
                raise ValueError("unexpected existing cache owner")
            marker.write_text(owner + "\n")
        elif not marker.is_file() or marker.read_text() != owner + "\n":
            raise ValueError("missing or foreign cache owner")
        output("cache.txt", f"vz04-cache-v1\nowner={owner}\nstate={expected}\nstep={step}\n".encode())
    elif mode == "ssh":
        host = required("FIXTURE_SSH_HOST", r"[a-z0-9][a-z0-9.-]{0,252}")
        port = int(required("FIXTURE_SSH_PORT", r"[0-9]{1,5}"))
        if not 1 <= port <= 65535:
            raise ValueError("invalid SSH port")
        # No fallback key/config, password prompt, host-key learning, or external command.
        result = subprocess.run([
            "ssh", "-F", "/dev/null", "-T", "-p", str(port),
            "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=yes",
            "-o", "UserKnownHostsFile=/run/secrets/known_hosts",
            "-o", "GlobalKnownHostsFile=/dev/null",
            "-o", "IdentityFile=none", "-o", "PasswordAuthentication=no",
            "-o", "KbdInteractiveAuthentication=no", "-o", "LogLevel=ERROR",
            "root@" + host, "fixture-read",
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=False)
        if result.returncode != 0:
            # Retain only a bounded public outcome: never print key/agent material.
            raise ValueError("SSH authentication failed")
        expected = Path("/fixture/ssh-response.txt").read_bytes()
        if result.stdout != expected or result.stderr:
            raise ValueError("SSH response mismatch")
        output("ssh.txt", result.stdout)
    else:
        raise ValueError("unknown workload")


if __name__ == "__main__":
    main(sys.argv[1])
