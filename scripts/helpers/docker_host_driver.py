#!/usr/bin/env python3
"""Host-client Docker fixture runner (DEV evidence, never the full release lane).

Run ``python3 scripts/helpers/docker_host_driver.py --help`` for the explicit
input boundary. No command selects or creates a global Docker context. The
caller supplies a *disposable*, already provisioned Machine client directory and
an owned BuildKit builder. Provisioning and destruction of those resources stay
with the topology harness. This module only owns its fresh Compose projects and
local output directory; it never prunes, removes base images, or removes a builder.

The input JSON is an integration contract, NOT an ownership attestation. The
eventual aggregate must independently bind it to authenticated topology receipts,
runtime provenance, release inputs and sibling-isolation evidence. This runner
deliberately cannot emit a release-scenario PASS or certify Docker compatibility.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time
from typing import Any


MAX_STREAM_BYTES = 4 * 1024 * 1024
BUILD_RECIPES = ("build-multi-stage", "build-cache-reuse", "build-arguments", "build-cache-mount", "build-secret-mount")
COMPOSE_RECIPES = ("compose-create", "compose-up-order", "compose-exec", "compose-network-paths",
                   "compose-volume-persistence", "compose-scale", "compose-blocked-health", "compose-failure")
SUITE_RECIPES = {"build": BUILD_RECIPES, "compose": COMPOSE_RECIPES, "all": BUILD_RECIPES + COMPOSE_RECIPES}


class Rejected(ValueError):
    """Fail-closed input, routing, ownership or assertion failure."""


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise Rejected(reason)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def regular(path: Path, limit: int = 512 * 1024 * 1024) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= limit,
                f"not a bounded single-link regular file: {path}")
        data = stream.read(before.st_size + 1)
        after = os.fstat(stream.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) and
                len(data) == before.st_size, f"input changed while reading: {path}")
        return data


def tree_digest(root: Path) -> str:
    require(root.is_dir() and not root.is_symlink(), "fixture root is not a directory")
    rows = []
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "fixture symlink rejected")
        if path.is_dir():
            continue
        data = regular(path)
        rows.append([path.relative_to(root).as_posix(), stat.S_IMODE(path.stat().st_mode),
                     len(data), sha256(data)])
        require(len(rows) <= 10000, "unbounded fixture tree")
    require(bool(rows), "empty fixture tree")
    return sha256(json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode())


def checked_text(value: Any, pattern: str, name: str) -> str:
    require(isinstance(value, str) and re.fullmatch(pattern, value) is not None,
            f"invalid {name}")
    return value


def digest(value: Any) -> str:
    return checked_text(value, r"[0-9a-f]{64}", "SHA-256")


def absolute(value: Any) -> Path:
    require(isinstance(value, str) and Path(value).is_absolute(), "absolute path required")
    path = Path(value)
    require(path == path.resolve(), "noncanonical path or symlink rejected")
    return path


def immutable_image(value: Any) -> str:
    return checked_text(value, r"(?:[a-z0-9][a-z0-9._:/-]*@)?sha256:[0-9a-f]{64}",
                        "immutable image")


@dataclasses.dataclass(frozen=True)
class Inputs:
    raw: dict[str, Any]
    suite: str = "all"

    def __post_init__(self) -> None:
        required = {"schema_version", "run_id", "release_sha256", "fixture_sha256",
                    "scope", "docker_config", "clients", "images"}
        require(self.suite in SUITE_RECIPES, "unknown input suite")
        optional = {"runtime_evidence"}
        if self.suite == "compose":
            optional.add("builder")
        else:
            required.add("builder")
        require(required <= set(self.raw) <= required | optional, "unknown or missing input keys")
        require(self.raw["schema_version"] == 1, "unknown input schema")
        checked_text(self.raw["run_id"], r"[a-z0-9][a-z0-9-]{7,39}", "unique run ID")
        digest(self.raw["release_sha256"])
        digest(self.raw["fixture_sha256"])
        scope = self.raw["scope"]
        require(isinstance(scope, dict) and set(scope) == {
            "project_id", "environment_id", "machine_id", "machine_incarnation",
            "runtime_identity", "docker_context", "docker_endpoint", "engine_id"},
            "exact owner/runtime/context mapping required")
        for name, value in scope.items():
            checked_text(value, r"[^\s\x00-\x1f]{1,256}", name)
        require(scope["docker_context"] not in {"default", "desktop-linux", "orbstack"},
                "global/host Docker context rejected")
        checked_text(scope["docker_context"], r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", "context")
        endpoint = scope["docker_endpoint"]
        require(endpoint.startswith("unix:///"), "exact local Machine Unix endpoint required")
        endpoint_path = absolute(endpoint[len("unix://"):])
        require(str(endpoint_path) not in {"/var/run/docker.sock", "/run/docker.sock"},
                "global Docker socket rejected")
        absolute(self.raw["docker_config"])
        clients = self.raw["clients"]
        require(isinstance(clients, dict) and set(clients) == {"docker", "compose", "buildx"},
                "all installed client pins required")
        for name, pin in clients.items():
            require(set(pin) == {"path", "sha256"}, "client path and digest required")
            path = absolute(pin["path"])
            require(sha256(regular(path)) == digest(pin["sha256"]), "client digest mismatch")
            require(os.access(path, os.X_OK), "client is not executable")
            if name != "docker":
                require(path.name == "docker-" + name, "canonical plugin basename required")
        images = self.raw["images"]
        require(isinstance(images, dict) and set(images) == {"base", "compose"},
                "base and Compose image pins required")
        for pin in images.values():
            require(set(pin) == {"reference", "id", "platform"}, "image identity fields required")
            immutable_image(pin["reference"])
            checked_text(pin["id"], r"sha256:[0-9a-f]{64}", "image configuration ID")
            require(pin["platform"] == "linux/arm64", "wrong image platform")
        require("@sha256:" in images["base"]["reference"], "base must pin repository digest")
        if "builder" in self.raw:
            builder = self.raw["builder"]
            require(isinstance(builder, dict) and set(builder) == {"name", "node", "container_id", "image_id"},
                    "exact pre-provisioned builder identity required")
            for key in ("name", "node"):
                checked_text(builder[key], r"[a-z0-9][a-z0-9-]{0,62}", "owned builder name")
            checked_text(builder["container_id"], r"[0-9a-f]{64}", "builder container ID")
            checked_text(builder["image_id"], r"sha256:[0-9a-f]{64}", "builder image ID")
        if "runtime_evidence" in self.raw:
            proof = self.raw["runtime_evidence"]
            require(isinstance(proof, dict) and set(proof) == {
                "receipt_path", "receipt_sha256", "inventory_path", "inventory_sha256", "youki_sha256"},
                "exact startup runtime evidence pins required")
            for name in ("receipt", "inventory"):
                path = absolute(proof[name + "_path"])
                require(sha256(regular(path, MAX_STREAM_BYTES)) == digest(proof[name + "_sha256"]),
                        "startup runtime evidence digest mismatch")
            digest(proof["youki_sha256"])

    @property
    def scope(self) -> dict[str, str]:
        return self.raw["scope"]

    @property
    def owner(self) -> str:
        material = json.dumps([self.raw["run_id"], self.scope], sort_keys=True).encode()
        return "vz04-" + sha256(material)[:24]

    def verify_runtime_evidence(self) -> dict[str, Any] | None:
        """Recheck parent-authenticated startup receipts, not a live cache audit."""
        proof = self.raw.get("runtime_evidence")
        if proof is None:
            return None
        evidence = {}
        for name in ("receipt", "inventory"):
            data = regular(absolute(proof[name + "_path"]), MAX_STREAM_BYTES)
            require(sha256(data) == proof[name + "_sha256"], "startup runtime proof changed")
            evidence[name] = json.loads(data)
        receipt, after = evidence["receipt"], evidence["inventory"]
        owner = {name: self.scope[name] for name in ("project_id", "environment_id", "machine_id")}
        incarnation = receipt["incarnation"]
        require(receipt["schema_version"] == 1 and receipt["state"] == "completed" and receipt["failure"] is None and
                receipt["owner"] == owner and receipt["context"] == self.scope["docker_context"] and
                receipt["client_sha256"] == self.raw["clients"]["docker"]["sha256"], "foreign/incomplete startup receipt")
        require(incarnation["schema_version"] == 1 and incarnation["machine_id"] == owner["machine_id"] and
                incarnation["incarnation_id"] == self.scope["machine_incarnation"] and
                type(incarnation["generation"]) is int and incarnation["generation"] > 0, "stale startup incarnation")
        resources = receipt["resources"]
        require(resources["engine_id"] == self.scope["engine_id"] and
                resources["cleanup_scope"] == "disposable_probe_containers_compose_objects_and_images" and
                resources["retained_buildkit_cache"] is True, "startup Engine/cleanup scope mismatch")
        require(set(after) == {"schema_version", "probe_receipt_sha256", "runtime_inventory"} and
                after["schema_version"] == 1 and after["probe_receipt_sha256"] == proof["receipt_sha256"],
                "post-startup inventory does not bind this receipt")
        inventory = after["runtime_inventory"]
        require(inventory == resources["runtime_inventory"] and
                set(inventory) == {"owner", "incarnation", "youki_sha256", "scope", "stdout"} and
                inventory["owner"] == owner and inventory["incarnation"] == incarnation and
                inventory["youki_sha256"] == proof["youki_sha256"] and
                inventory["scope"] == "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit",
                "startup runtime inventory identity differs")
        output = inventory["stdout"]
        require(isinstance(output, str) and len(output.encode()) <= 8192 and
                output.startswith("vz-startup-runtime-inventory-v1\nyouki-sha256=" + proof["youki_sha256"] + "\n") and
                output.endswith("\nalternate-runtime-binaries=absent\n") and "\nyouki version: " in output,
                "startup executable inventory does not establish pinned youki and absence of alternates")
        return inventory


@dataclasses.dataclass
class Command:
    index: int
    argv: list[str]
    returncode: int
    stdout: bytes
    stderr: bytes
    timed_out: bool = False


class OutputLimitExceeded(Rejected):
    """The retained stream prefix is evidence of excess output, not full output."""


def collect_output(process: subprocess.Popen, timeout: int, limit: int) -> tuple[bytes, bytes]:
    """Drain both pipes concurrently, retaining at most ``limit`` bytes each."""
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    observed = {"stdout": 0, "stderr": 0}
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            for name in buffers:
                pipe = getattr(process, name)
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, name)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(process.args, timeout)
                for key, _ in selector.select(remaining):
                    name = key.data
                    # Read at most one byte past the retained bound to prove
                    # overflow without allocating an unbounded discarded tail.
                    capacity = limit - len(buffers[name])
                    try:
                        chunk = os.read(key.fd, min(65536, capacity + 1))
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    observed[name] += len(chunk)
                    buffers[name].extend(chunk[:capacity])
                    if len(chunk) > capacity:
                        raise OutputLimitExceeded(f"{name} exceeded {limit} retained bytes")
            process.wait(timeout=max(0, deadline - time.monotonic()))
    except BaseException as error:
        error.stdout, error.stderr = bytes(buffers["stdout"]), bytes(buffers["stderr"])
        error.observed_bytes = observed
        raise
    return bytes(buffers["stdout"]), bytes(buffers["stderr"])


def execute(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    """Bound the exact owned CLI process group, including its plugin child."""
    timeout = kwargs.pop("timeout")
    limit = kwargs.pop("max_stream_bytes", MAX_STREAM_BYTES)
    require(type(limit) is int and 1 <= limit <= MAX_STREAM_BYTES, "invalid per-stream output bound")
    kwargs.pop("check")
    process = subprocess.Popen(argv, start_new_session=True, **kwargs)
    try:
        try:
            stdout, stderr = collect_output(process, timeout, limit)
        except BaseException as error:
            # The unreaped process remains our session leader. Never identify
            # or kill a process by a stale PID file or a guessed command name.
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError as kill_error:
                    kill_error.stdout = getattr(error, "stdout", b"")
                    kill_error.stderr = getattr(error, "stderr", b"")
                    kill_error.observed_bytes = getattr(error, "observed_bytes", {})
                    raise kill_error from error
            # Never communicate() here: that could allocate an unlimited tail.
            # collect_output attached only its bounded observed prefixes.
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired as reap_error:
                reap_error.stdout = getattr(error, "stdout", b"")
                reap_error.stderr = getattr(error, "stderr", b"")
                reap_error.observed_bytes = getattr(error, "observed_bytes", {})
                raise reap_error from error
            raise
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    finally:
        # Popen.__exit__ performs an unbounded wait; close pipes ourselves and
        # leave a failed bounded reap explicitly uncertain for the caller.
        for pipe in (process.stdin, process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()


def contains_canary(streams: tuple[bytes, ...], canaries: list[bytes]) -> bool:
    decoded_logs = []
    for stream in streams:
        for line in stream.splitlines():
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    logs = row.get("logs", [])
                    if isinstance(row.get("data"), str):
                        logs = [row]
                    if isinstance(logs, list):
                        for log in logs:
                            if isinstance(log, dict) and isinstance(log.get("data"), str):
                                try:
                                    decoded_logs.append(base64.b64decode(log["data"], validate=True))
                                except ValueError:
                                    # A malformed sibling must not conceal a
                                    # later valid encoded canary in this batch.
                                    continue
            except (ValueError, TypeError):
                continue
    # BuildKit rawjson log payloads are base64; inspect decoded bytes as well
    # as raw output. This does not replace decompressed OCI/cache blob scans.
    all_data = (*streams, b"".join(decoded_logs))
    return any(canary and canary in data for canary in canaries for data in all_data)


class Recorder:
    """Retain actual byte streams, argv, exit and timing; never retry a command."""

    def __init__(self, root: Path, env: dict[str, str], canaries: list[bytes], *, max_stream_bytes: int = MAX_STREAM_BYTES):
        self.root, self.env, self.canaries = root, env, canaries
        require(type(max_stream_bytes) is int and 1 <= max_stream_bytes <= MAX_STREAM_BYTES, "invalid output bound")
        self.max_stream_bytes = max_stream_bytes
        self.count = 0
        self.receipts: list[dict[str, Any]] = []

    def persist(self, path: Path, value: dict[str, Any], *, create: bool = False) -> None:
        """Publish a durable intent before dispatch, or a terminal observation."""
        flags = os.O_WRONLY | os.O_NOFOLLOW | os.O_CREAT | (os.O_EXCL if create else os.O_TRUNC)
        with os.fdopen(os.open(path, flags, 0o600), "w") as stream:
            stream.write(json.dumps(value, sort_keys=True, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def run(self, argv: list[str], *, executable: str, timeout: int = 120,
            extra_env: dict[str, str] | None = None, mutation: bool = True) -> Command:
        self.count += 1
        index = self.count
        start_wall, start = time.time_ns(), time.monotonic_ns()
        timed_out = False
        interrupted = False
        pending_error: BaseException | None = None
        output_limit_exceeded = False
        observed_bytes: dict[str, int] | None = None
        stem = f"command-{index:05d}"
        receipt = {"index": index, "executable": executable, "argv": argv, "argv0": argv[0],
                   "environment": extra_env or {}, "started_unix_ns": start_wall,
                   "host_outcome": "inflight", "effects_uncertain": True, "mutation": mutation,
                   "max_stream_bytes": self.max_stream_bytes,
                   "timed_out": False, "interrupted": False, "exit_code": None}
        # Register uncertainty in memory first, then fsync intent before spawn.
        # An exception at any later boundary must never make cleanup assume
        # that the absence of a completed receipt means no dispatch occurred.
        self.receipts.append(receipt)
        self.persist(self.root / (stem + ".intent.json"), receipt, create=True)
        # No shell, terminal, inherited stdin, Docker environment or user SSH agent.
        try:
            result = execute(argv, executable=executable, env=self.env | (extra_env or {}),
                                    cwd=self.root, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=timeout, max_stream_bytes=self.max_stream_bytes, check=False)
            code, stdout, stderr = result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired as error:
            timed_out = True
            code, stdout, stderr = -1, error.stdout or b"", error.stderr or b""
            observed_bytes = getattr(error, "observed_bytes", None)
        except OSError as error:
            code, stdout, stderr = -1, getattr(error, "stdout", b"") or b"", getattr(error, "stderr", b"") or b""
            pending_error = error
            observed_bytes = getattr(error, "observed_bytes", None)
        except BaseException as error:
            interrupted = isinstance(error, KeyboardInterrupt)
            output_limit_exceeded = isinstance(error, OutputLimitExceeded)
            pending_error = error
            code, stdout, stderr = -1, getattr(error, "stdout", b"") or b"", getattr(error, "stderr", b"") or b""
            observed_bytes = getattr(error, "observed_bytes", None)
        elapsed = time.monotonic_ns() - start
        leaked = contains_canary((stdout, stderr, json.dumps(argv).encode()), self.canaries)
        if leaked:
            # Never publish a discovered credential. The receipt records a failure
            # and original hashes, not a false assertion that sanitized bytes were raw.
            retained_stdout, retained_stderr = b"[secret leak withheld]\n", b"[secret leak withheld]\n"
        else:
            retained_stdout, retained_stderr = stdout, stderr
        (self.root / (stem + ".stdout")).write_bytes(retained_stdout)
        (self.root / (stem + ".stderr")).write_bytes(retained_stderr)
        terminal = {"index": index, "executable": executable, "argv": argv, "argv0": argv[0],
                   "environment": extra_env or {},
                   "started_unix_ns": start_wall, "elapsed_ns": elapsed,
                   "exit_code": code, "timed_out": timed_out,
                   "interrupted": interrupted, "host_outcome": "interrupted" if interrupted else
                   "output_limit_exceeded" if output_limit_exceeded else
                   "timed_out" if timed_out else "unknown" if pending_error or code < 0 else "exited",
                   "mutation": mutation,
                   "effects_uncertain": timed_out or pending_error is not None or code < 0 or (mutation and code != 0),
                   "max_stream_bytes": self.max_stream_bytes, "output_limit_exceeded": output_limit_exceeded,
                   "capture_complete": not timed_out and pending_error is None,
                   "observed_bytes": observed_bytes or {"stdout": len(stdout), "stderr": len(stderr)},
                   "retained_observed_stdout_bytes": len(stdout), "retained_observed_stderr_bytes": len(stderr),
                   "retained_observed_stdout_sha256": sha256(stdout), "retained_observed_stderr_sha256": sha256(stderr),
                   "stdout": stem + ".stdout", "stderr": stem + ".stderr",
                   "stdout_sha256": sha256(retained_stdout),
                   "stderr_sha256": sha256(retained_stderr),
                   "raw_stdout_sha256": sha256(stdout) if not timed_out and pending_error is None else None,
                   "raw_stderr_sha256": sha256(stderr) if not timed_out and pending_error is None else None,
                   "dispatch_error": str(pending_error) if pending_error else None,
                   "secret_leak_detected": leaked, "raw_streams_retained": not leaked and not timed_out and pending_error is None}
        self.persist(self.root / (stem + ".json"), terminal, create=True)
        # Only a durably retained normal host completion clears in-memory
        # uncertainty. The separate intent remains for crash/recovery auditing.
        receipt.update(terminal)
        if pending_error is not None:
            raise pending_error
        require(not leaked, "secret canary appeared in command output; bytes withheld")
        return Command(index, argv, code, stdout, stderr, timed_out)

    def acknowledge_negative(self, command: Command, assertion: str) -> None:
        """Clear failed mutation uncertainty only after its semantic proof."""
        receipt = self.receipts[command.index - 1]
        require(receipt["index"] == command.index and receipt["host_outcome"] == "exited" and
                receipt["exit_code"] == command.returncode and command.returncode > 0 and
                receipt["capture_complete"] and not receipt["secret_leak_detected"] and bool(assertion),
                "cannot reconcile incomplete/unknown host command")
        acknowledgement = {"command_index": command.index, "assertion": assertion,
                           "terminal_receipt_sha256": sha256(regular(self.root / f"command-{command.index:05d}.json")),
                           "effects_uncertain": False}
        self.persist(self.root / f"command-{command.index:05d}.acknowledgement.json", acknowledgement, create=True)
        receipt["effects_uncertain"] = False
        receipt["semantic_acknowledgement"] = acknowledgement


class Driver:
    def __init__(self, inputs: Inputs, fixture: Path, output: Path):
        require(sys.platform == "darwin" and os.uname().machine == "arm64", "this DEV runner requires Apple-silicon macOS")
        self.inputs, self.fixture, self.output = inputs, fixture, output
        require(tree_digest(fixture) == inputs.raw["fixture_sha256"], "fixture tree digest mismatch")
        require(not output.exists() and output.is_absolute(), "fresh absolute output directory required")
        require(output.parent == output.parent.resolve(), "output ancestor symlink rejected")
        output.mkdir(mode=0o700)
        self.temporary = output / "private-tmp"
        self.temporary.mkdir(mode=0o700)
        self.env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                    "TMPDIR": str(self.temporary), "LC_ALL": "C", "NO_COLOR": "1"}
        if "HOME" in os.environ:
            self.env["HOME"] = os.environ["HOME"]
        secret = regular(fixture / "inputs/secret.txt")
        self.record = Recorder(output, self.env, [secret, secret.rstrip(b"\n")])
        self.fixture_spec = json.loads(regular(fixture / "fixture.json"))
        require(sha256(secret) == self.fixture_spec["secret_input_sha256"], "fixture secret mismatch")
        self.observations: list[dict[str, Any]] = []
        self.projects: dict[str, dict[str, set[str]]] = {}
        self.config_snapshot = self.validate_config()
        definition = json.loads(regular(fixture / "compose/compose.json"))
        label = {"dev.vz.fixture-owner": inputs.owner}
        self.overlay = output / "compose-owner.json"
        self.overlay.write_text(json.dumps({kind: {name: {"labels": label} for name in definition[kind]}
                                           for kind in ("services", "networks", "volumes")}) + "\n")
        self.overlay_digest = sha256(regular(self.overlay))

    def validate_config(self) -> str:
        config = Path(self.inputs.raw["docker_config"])
        require(config.is_dir() and not config.is_symlink(), "private client config directory required")
        require(stat.S_IMODE(config.stat().st_mode) == 0o700, "client config requires mode 0700")
        require(config != Path(os.environ.get("HOME", "")) / ".docker", "user Docker config rejected")
        data = regular(config / "config.json")
        parsed = json.loads(data)
        require(set(parsed) <= {"currentContext", "cliPluginsExtraDirs"},
                "credentials, proxies, helpers and unknown client settings rejected")
        clients = self.inputs.raw["clients"]
        directories = sorted({str(Path(clients[name]["path"]).parent) for name in ("compose", "buildx")})
        installed = config / "cli-plugins"
        if os.path.lexists(installed):
            require(installed.is_dir() and not installed.is_symlink() and
                    stat.S_IMODE(installed.stat().st_mode) == 0o700 and
                    directories == [str(installed)] and "cliPluginsExtraDirs" not in parsed,
                    "mixed or foreign installed plugin layout")
            require({path.name for path in installed.iterdir()} == {"docker-compose", "docker-buildx"},
                    "unknown/shadow installed plugin")
        else:
            require(parsed.get("cliPluginsExtraDirs") == directories,
                    "plugin directories must exactly match pinned installed clients")
        for directory in directories:
            directory_path = absolute(directory)
            for candidate in directory_path.iterdir():
                if candidate.name.startswith("docker-"):
                    require(candidate.name in {"docker-compose", "docker-buildx"}, "unknown discovery plugin")
                    name = candidate.name[len("docker-"):]
                    require(candidate == Path(clients[name]["path"]), "shadow plugin rejected")
            for name in ("compose", "buildx"):
                candidate = directory_path / ("docker-" + name)
                if os.path.lexists(candidate):
                    require(candidate == Path(clients[name]["path"]) and
                            absolute(str(candidate)) == candidate and os.access(candidate, os.X_OK) and
                            sha256(regular(candidate)) == clients[name]["sha256"], "changed or redirected plugin executable")
        return sha256(data)

    def command(self, args: list[str], *, expected: int | None = 0, timeout: int = 120,
                env: dict[str, str] | None = None) -> Command:
        require(self.validate_config() == self.config_snapshot, "client config changed")
        for pin in self.inputs.raw["clients"].values():
            require(sha256(regular(Path(pin["path"]))) == pin["sha256"], "client executable changed")
        executable = self.inputs.raw["clients"]["docker"]["path"]
        argv = ["docker", "--config",
                self.inputs.raw["docker_config"], "--context", self.inputs.scope["docker_context"], *args]
        readonly = args[0] in {"version", "info", "events", "logs"} or args[:2] in [
            ["context", "inspect"], ["image", "inspect"], ["container", "inspect"], ["container", "ls"],
            ["network", "inspect"], ["network", "ls"], ["volume", "inspect"], ["volume", "ls"],
            ["buildx", "inspect"], ["compose", "version"]]
        result = self.record.run(argv, executable=executable, timeout=timeout, extra_env=env, mutation=not readonly)
        if expected is not None:
            require(not result.timed_out and result.returncode == expected,
                    f"command {result.index} exit {result.returncode}, expected {expected}")
        return result

    def json_command(self, args: list[str]) -> Any:
        return json.loads(self.command(args).stdout)

    def guard(self) -> None:
        """Recheck current routing and identity before each fixture operation."""
        scope = self.inputs.scope
        contexts = self.json_command(["context", "inspect", scope["docker_context"]])
        require(len(contexts) == 1 and contexts[0]["Name"] == scope["docker_context"],
                "context identity mismatch")
        endpoint = contexts[0]["Endpoints"]["docker"]
        require(endpoint["Host"] == scope["docker_endpoint"] and not endpoint.get("SkipTLSVerify", False),
                "context endpoint mismatch")
        require(stat.S_ISSOCK(Path(scope["docker_endpoint"][7:]).stat().st_mode),
                "Machine socket unavailable")
        info = self.json_command(["info", "--format", "{{json .}}"])
        require(info["ID"] == scope["engine_id"] and info["OSType"] == "linux" and
                info["Architecture"] in {"aarch64", "arm64"}, "wrong Engine identity or target")
        require(info["DefaultRuntime"] == "youki", "Engine default runtime is not youki")
        runtimes = info["Runtimes"]
        require(isinstance(runtimes, dict) and runtimes.get("youki", {}).get("path") == "/mnt/linux-bin/youki",
                "Engine does not select the pinned youki path")
        inert = {"runc", "io.containerd.runc.v2"} & set(runtimes)
        require(set(runtimes) <= {"youki", "io.containerd.youki.v2", "runc", "io.containerd.runc.v2"} and
                all(runtimes[name] == {"path": "runc"} for name in inert), "unexpected or executable alternate runtime metadata")
        inventory = self.inputs.verify_runtime_evidence()
        require(not inert or inventory is not None, "inert stock runtime metadata requires authenticated startup executable inventory")

    def verify_images(self) -> None:
        for pin in self.inputs.raw["images"].values():
            inspected = self.json_command(["image", "inspect", pin["reference"]])
            require(len(inspected) == 1, "ambiguous pinned image")
            item = inspected[0]
            require(item["Id"] == pin["id"] and item["Os"] == "linux" and
                    item["Architecture"] == "arm64", "image content/platform mismatch")
            if "@" in pin["reference"]:
                require(pin["reference"] in item.get("RepoDigests", []), "repository digest absent")

    def observe(self, name: str, related: list[str], assertion: Any) -> None:
        first = self.record.count + 1
        result = {"recipe": name, "related_scenario_ids": related, "first_command": first,
                  "last_command": first - 1, "outcome": "failed", "assertions": []}
        self.observations.append(result)
        try:
            assertions = assertion()
            require(isinstance(assertions, list) and assertions, "empty observation cannot pass")
            result.update(outcome="fixture_assertions_passed", assertions=assertions)
        finally:
            result["last_command"] = self.record.count

    def inspect_project(self, project: str) -> dict[str, list[dict[str, Any]]]:
        label = "label=com.docker.compose.project=" + project
        result: dict[str, list[dict[str, Any]]] = {}
        for kind, list_args in (("container", ["container", "ls", "--all", "--quiet", "--no-trunc"]),
                                ("network", ["network", "ls", "--quiet", "--no-trunc"]),
                                ("volume", ["volume", "ls", "--quiet"])):
            ids = self.command([*list_args, "--filter", label]).stdout.decode().split()
            result[kind] = self.json_command([kind, "inspect", *ids]) if ids else []
            for item in result[kind]:
                labels = item["Config"].get("Labels", {}) if kind == "container" else item.get("Labels", {})
                require(labels.get("com.docker.compose.project") == project, "foreign project resource")
                require(labels.get("dev.vz.fixture-owner") == self.inputs.owner,
                        "project namespace contains resource not created by this fixture owner")
        return result

    @staticmethod
    def identities(inventory: dict[str, list[dict[str, Any]]]) -> dict[str, set[str]]:
        return {kind: {item["Name"] if kind == "volume" else item["Id"] for item in items}
                for kind, items in inventory.items()}

    def compose(self, project: str, args: list[str], *, blocked: bool = False,
                expected: int | None = 0, timeout: int = 120) -> Command:
        self.guard()
        require(tree_digest(self.fixture) == self.inputs.raw["fixture_sha256"], "fixture changed")
        require(sha256(regular(self.overlay)) == self.overlay_digest, "owner overlay changed")
        cmd = ["compose", "--project-name", project, "--file", str(self.fixture / "compose/compose.json"),
               "--file", str(self.overlay)]
        if blocked:
            cmd.extend(["--file", str(self.fixture / "compose/blocked-health.json")])
        return self.command([*cmd, *args], expected=expected, timeout=timeout,
                            env={"FIXTURE_IMAGE": self.inputs.raw["images"]["compose"]["id"],
                                 "FIXTURE_OWNER": self.inputs.owner})

    def new_project(self, suffix: str) -> str:
        project = self.inputs.owner + "-" + suffix
        self.guard()
        inventory = self.inspect_project(project)
        require(not any(inventory.values()), "project name already exists; adoption forbidden")
        self.verify_named_resources(project, inventory)
        self.projects[project] = {kind: set() for kind in inventory}
        return project

    def verify_named_resources(self, project: str, inventory: dict[str, list[dict[str, Any]]]) -> None:
        """Exact names are collision authority even when owner labels are absent."""
        for kind, suffixes in (("volume", ("state",)), ("network", ("frontend", "backend", "isolated"))):
            names = {project + "_" + suffix for suffix in suffixes}
            existing = set(self.command([kind, "ls", "--format", "{{.Name}}"]).stdout.decode().splitlines()) & names
            owned = {item["Name"] for item in inventory[kind]}
            require(owned <= names and existing == owned, "unlabelled/foreign exact-name resource collision")
        volumes = {item["Name"] for item in inventory["volume"]}
        networks = {item["Name"]: item for item in inventory["network"]}
        require(len(networks) == len(inventory["network"]) and
                len({network["Id"] for network in networks.values()}) == len(networks),
                "ambiguous owned network names or identities")
        for container in inventory["container"]:
            for mount in container.get("Mounts", []):
                require(mount["Type"] == "volume" and mount["Name"] in volumes,
                        "container mounts a resource outside the owned inventory")
            declared = container["NetworkSettings"]["Networks"]
            for name, network in declared.items():
                require(name in networks, "container declares a network outside the owned inventory")
                if network["NetworkID"] != "":
                    require(network["NetworkID"] == networks[name]["Id"],
                            "container network name and owned identity disagree")
                    continue
                # Moby leaves IDs empty for Compose's never-started dependency
                # containers. A declaration is not yet an attached endpoint;
                # authenticate its exact owned name and prove the narrow state.
                state = container["State"]
                require(state.get("Status") == "created" and
                        all(state.get(flag) is False for flag in ("Running", "Paused", "Restarting")) and
                        type(state.get("Pid")) is int and state["Pid"] == 0 and
                        state.get("StartedAt") == "0001-01-01T00:00:00Z" and
                        state.get("FinishedAt") == "0001-01-01T00:00:00Z" and
                        container["HostConfig"].get("NetworkMode") in declared and
                        all(network.get(field) == "" for field in (
                            "EndpointID", "Gateway", "IPAddress", "MacAddress", "IPv6Gateway", "GlobalIPv6Address")) and
                        all(type(network.get(field)) is int and network[field] == 0
                            for field in ("IPPrefixLen", "GlobalIPv6PrefixLen")) and
                        all(isinstance(owned.get("Containers"), dict) and container["Id"] not in owned["Containers"]
                            for owned in networks.values()),
                        "empty network identity lacks exact never-started unattached ownership proof")

    def capture(self, project: str) -> dict[str, list[dict[str, Any]]]:
        inventory = self.inspect_project(project)
        # The random run-owned namespace was proven empty before first create.
        # Retain every observed ID, including partial creations after command failure.
        for kind, ids in self.identities(inventory).items():
            self.projects[project][kind].update(ids)
        return inventory

    def cleanup(self) -> list[str]:
        errors = []

        def uncertain():
            return any(receipt.get("effects_uncertain", True) or receipt.get("timed_out", False) or
                       receipt.get("interrupted", False) for receipt in self.record.receipts)

        if uncertain():
            # A timed-out host client does not prove the daemon-side operation
            # stopped. Do not race cleanup against possibly continuing creation.
            return ["Unknown, interrupted or inflight host command effects: retained owned project names require topology-harness reconciliation"]
        for project in self.projects:
            if uncertain():
                errors.append("Cleanup observation became uncertain; remaining owned projects retained")
                break
            try:
                self.guard()
                inventory = self.capture(project)
                self.verify_named_resources(project, inventory)
                require(not uncertain(), "cleanup observations do not establish a safe destructive dispatch")
                self.compose(project, ["--profile", "failure", "down", "--volumes", "--remove-orphans"])
                remaining = self.inspect_project(project)
                require(not any(remaining.values()), "owned Compose resources survived down")
                self.verify_named_resources(project, remaining)
                for container in inventory["container"]:
                    container_id = container["Id"]
                    missing = self.command(["container", "inspect", container_id], expected=1)
                    require(json.loads(missing.stdout) == [] and missing.stderr.decode().strip() in {
                        "Error response from daemon: No such container: " + container_id,
                        "Error: No such container: " + container_id,
                        "Error: No such object: " + container_id},
                        "exact pre-down container absence was not proven")
            except (Rejected, KeyError, OSError, ValueError) as error:
                errors.append(f"{project}: {type(error).__name__}: {error}")
        return errors

    @staticmethod
    def by_service(inventory: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
        services: dict[str, list[dict[str, Any]]] = {}
        for item in inventory["container"]:
            service = item["Config"]["Labels"]["com.docker.compose.service"]
            services.setdefault(service, []).append(item)
        return services

    def exec_container(self, item: dict[str, Any], args: list[str], *, expected: int | None = 0) -> Command:
        self.guard()
        return self.command(["exec", item["Id"], *args], expected=expected)

    def volume_persistence(self, project: str) -> list[str]:
        before = self.capture(project)
        db = self.by_service(before)["db"][0]
        mounts = [mount for mount in db["Mounts"] if mount["Destination"] == "/data"]
        require(len(mounts) == 1 and mounts[0]["Type"] == "volume" and mounts[0]["RW"] and
                mounts[0]["Name"] == project + "_state", "wrong database volume binding")
        # Startup only reconstructs sentinel.txt. This exclusive host-written
        # marker is never an input to service startup and cannot be recreated.
        marker = "/data/host-persistence-" + self.inputs.owner + ".txt"
        payload = f"vz04|host-written|{self.inputs.owner}|{self.inputs.raw['run_id']}|persisted\n".encode()
        code = "import os,sys;f=open(sys.argv[1],'xb');f.write(sys.argv[2].encode());f.flush();os.fsync(f.fileno());f.close()"
        self.exec_container(db, ["python3", "-c", code, marker, payload.decode()])
        read = ["python3", "-c", "import pathlib,sys;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes())", marker]
        require(self.exec_container(db, read).stdout == payload, "host persistence marker was not written exactly")
        self.compose(project, ["stop"])
        self.compose(project, ["up", "--detach", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "30"], timeout=40)
        after = self.capture(project)
        require(self.identities(before) == self.identities(after), "Compose stop/up changed resource identity")
        require(self.exec_container(self.by_service(after)["db"][0], read).stdout == payload,
                "host-written persistence marker changed after Compose stop/up")
        return ["exact owned named volume mount", "Compose stop/up preserved IDs and exclusive host-written marker bytes; Machine recovery still required"]

    def compose_workloads(self) -> None:
        project = self.new_project("compose")
        started = str(int(time.time()))

        def create() -> list[str]:
            self.compose(project, ["create", "--no-build", "--pull", "never"])
            services = self.by_service(self.capture(project))
            require(set(services) == {"db", "api", "worker", "isolated"}, "wrong created services")
            require(all(len(items) == 1 and items[0]["State"]["Status"] == "created" and
                        not items[0]["State"]["Running"] for items in services.values()),
                    "services ran before up")
            return ["four exact fixture services created; none running"]

        self.observe("compose-create", ["docker.compose.create"], create)

        def up() -> list[str]:
            self.compose(project, ["up", "--detach", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "30"], timeout=40)
            services = self.by_service(self.capture(project))
            require(set(services) == {"db", "api", "worker", "isolated"}, "wrong ready services")
            require(all(len(items) == 1 and items[0]["State"].get("Health", {}).get("Status") == "healthy"
                        for items in services.values()), "service unhealthy")
            # Engine timestamps, not arrival ordering or workload print statements.
            events = self.command(["events", "--since", started, "--until", str(int(time.time()) + 1),
                                   "--filter", "label=com.docker.compose.project=" + project,
                                   "--format", "{{json .}}"], timeout=10)
            rows = [json.loads(line) for line in events.stdout.splitlines() if line]
            assert_health_order(rows, {name: items[0]["Id"] for name, items in services.items()}, project)
            return ["four services healthy", "Engine dependency healthy timestamps precede dependent start timestamps"]

        self.observe("compose-up-order", ["docker.compose.up", "docker.compose.dependency_ordering",
                                          "docker.compose.health_ordering"], up)

        def execute() -> list[str]:
            result = self.compose(project, ["exec", "--no-TTY", "api", "python3", "/fixture/service.py", "exec"], expected=37)
            require(result.stdout == f"vz04|api|{self.inputs.owner}|exec-stdout\n".encode() and
                    result.stderr == f"vz04|api|{self.inputs.owner}|exec-stderr\n".encode(), "exec stream mismatch")
            self.record.acknowledge_negative(result, "exact Compose exec stdout/stderr and exit 37")
            return ["Compose exec API stdout/stderr exact; exit 37"]

        self.observe("compose-exec", ["docker.compose.exec"], execute)

        def networks() -> list[str]:
            inventory = self.capture(project)
            services = {name: items[0] for name, items in self.by_service(inventory).items()}
            for role, networks in self.fixture_spec["expected"]["networks"].items():
                require(set(services[role]["NetworkSettings"]["Networks"]) ==
                        {project + "_" + name for name in networks}, "unexpected network membership")
            for source, destination, port, path in self.fixture_spec["expected"]["allowed_paths"]:
                response = self.exec_container(services[source], ["python3", "/fixture/service.py", "probe",
                                                                  f"http://{destination}:{port}{path}"])
                require(response.stdout == f"vz04|db|{self.inputs.owner}|persisted\n".encode(), "declared service path returned wrong data")
            for source, destination, port in self.fixture_spec["expected"]["denied_paths"]:
                addresses = [destination, *[network["IPAddress"] for network in services[destination]["NetworkSettings"]["Networks"].values()]]
                require(all(addresses), "destination IP evidence absent")
                for address in addresses:
                    url = f"http://{address}:{port}/health"
                    def controls():
                        for role, target in ((source, "127.0.0.1"), (destination, address)):
                            live = self.exec_container(services[role], ["python3", "/fixture/service.py", "probe",
                                                                      f"http://{target}:{port}/health"])
                            require(live.stdout == f"vz04|{role}|{self.inputs.owner}|ready\n".encode() and not live.stderr,
                                    "denial control source/destination is not healthy")
                    controls()
                    response = self.exec_container(services[source], ["python3", "/fixture/service.py", "transport", url])
                    assert_transport_denied(response, url, dns_name=address == destination)
                    controls()
            return ["exact network memberships", "declared paths returned exact database bytes",
                    "forbidden paths denied by DNS name and every inspected destination IP"]

        self.observe("compose-network-paths", ["docker.compose.networks"], networks)

        self.observe("compose-volume-persistence", ["docker.compose.volumes"], lambda: self.volume_persistence(project))

        def scale() -> list[str]:
            self.compose(project, ["up", "--detach", "--no-build", "--pull", "never", "--scale", "worker=3",
                                   "--wait", "--wait-timeout", "30"], timeout=40)
            items = self.by_service(self.capture(project))["worker"]
            require(len(items) == 3 and len({x["Id"] for x in items}) == 3, "three distinct replicas required")
            for item in items:
                result = self.exec_container(item, ["python3", "/fixture/service.py", "probe", "http://127.0.0.1:8080/identity"])
                require(json.loads(result.stdout) == {"owner": self.inputs.owner, "role": "worker",
                                                       "hostname": item["Config"]["Hostname"]}, "replica identity mismatch")
            before = {x["Id"] for x in items}
            self.compose(project, ["up", "--detach", "--no-build", "--pull", "never", "--scale", "worker=1",
                                   "--wait", "--wait-timeout", "30"], timeout=40)
            after = self.by_service(self.capture(project))["worker"]
            require(len(after) == 1 and after[0]["Id"] in before, "scaled-down survivor changed identity")
            for removed_id in sorted(before - {after[0]["Id"]}):
                missing = self.command(["container", "inspect", removed_id], expected=1)
                require(("No such container: " + removed_id).encode() in missing.stderr or
                        ("No such object: " + removed_id).encode() in missing.stderr,
                        "removed replica absence was not proven")
            return ["three exact worker identities observed", "scale down retained one identity and removed two"]

        self.observe("compose-scale", ["docker.compose.scaling"], scale)

        def blocked() -> list[str]:
            blocked_project = self.new_project("blocked")
            began = str(int(time.time()))
            result = self.compose(blocked_project, ["up", "--detach", "--no-build", "--pull", "never", "--wait",
                                                    "--wait-timeout", "30"], blocked=True, expected=None, timeout=40)
            require(not result.timed_out and result.returncode > 0, "blocked dependency must fail normally")
            services = self.by_service(self.capture(blocked_project))
            require(set(services) == {"db", "api", "worker", "isolated"} and
                    all(len(items) == 1 for items in services.values()) and
                    services["db"][0]["State"].get("Health", {}).get("Status") == "unhealthy",
                    "negative control never observed unhealthy database")
            for role in ("api", "worker"):
                require(all(x["State"]["Status"] == "created" and not x["State"]["Running"]
                            for x in services[role]), "dependent started despite unhealthy database")
            events = self.command(["events", "--since", began, "--until", str(int(time.time()) + 1),
                                   "--filter", "label=com.docker.compose.project=" + blocked_project,
                                   "--format", "{{json .}}"], timeout=10)
            assert_blocked_events([json.loads(line) for line in events.stdout.splitlines() if line],
                                  {role: items[0]["Id"] for role, items in services.items()}, blocked_project)
            self.record.acknowledge_negative(result, "exact created dependents and Engine create/start/unhealthy history prove blocked dependency")
            return ["unhealthy database observed; dependent containers never started"]

        self.observe("compose-blocked-health", ["docker.compose.health_ordering"], blocked)

        def failure() -> list[str]:
            failing_project = self.new_project("failure")
            result = self.compose(failing_project, ["--profile", "failure", "up", "--no-build", "--pull", "never",
                                            "--abort-on-container-exit", "--exit-code-from", "failure"], expected=37, timeout=40)
            services = self.by_service(self.capture(failing_project))
            require(len(services.get("failure", [])) == 1, "failure container evidence missing")
            job = services["failure"][0]
            require(job["State"]["Status"] == "exited" and job["State"]["ExitCode"] == 37,
                    "failure job exit mismatch")
            require(set(services) == {"db", "api", "worker", "isolated", "failure"} and
                    all(len(items) == 1 and not items[0]["State"]["Running"] and
                        items[0]["State"]["Status"] in {"exited", "created"} for items in services.values()),
                    "abort-on-container-exit left a service running or missing")
            logs = self.command(["logs", job["Id"]])
            require(logs.stdout == f"vz04|failure|{self.inputs.owner}|exit-37\n".encode() and not logs.stderr,
                    "failure job log mismatch")
            self.record.acknowledge_negative(result, "exact failure job and Compose exit 37, exact logs, all other services stopped")
            return ["Compose and exact failure job exit 37; all five services captured and none running"]

        self.observe("compose-failure", ["docker.compose.failure_propagation"], failure)

    def builder_guard(self) -> None:
        self.guard()
        builder = self.inputs.raw["builder"]
        inspected = self.command(["buildx", "inspect", builder["name"]])
        assert_builder_inspect(inspected.stdout, builder, self.inputs.scope["docker_context"])
        item = self.json_command(["container", "inspect", builder["container_id"]])[0]
        require(item["Id"] == builder["container_id"] and item["Image"] == builder["image_id"] and
                item["Name"] == "/buildx_buildkit_" + builder["node"] and item["State"]["Running"],
                "builder container content/identity mismatch")

    def build(self, suffix: str, dockerfile: str, arguments: dict[str, str], *,
              extra: list[str] | None = None, expected: int | None = 0) -> tuple[Command, Path]:
        self.builder_guard()
        require(tree_digest(self.fixture) == self.inputs.raw["fixture_sha256"], "fixture changed")
        dest = self.output / ("export-" + suffix)
        require(not dest.exists(), "build destination already exists")
        args = ["buildx", "build", "--builder", self.inputs.raw["builder"]["name"], "--platform", "linux/arm64",
                "--progress", "rawjson", "--file", str(self.fixture / "build" / dockerfile),
                "--output", "type=local,dest=" + str(dest), "--build-arg",
                "FIXTURE_BASE=" + self.inputs.raw["images"]["base"]["reference"]]
        for key, value in sorted(arguments.items()):
            args.extend(["--build-arg", key + "=" + value])
        result = self.command([*args, *(extra or []), str(self.fixture / "build")], expected=expected, timeout=300)
        return result, dest

    def build_workloads(self) -> None:
        expected = self.fixture_spec["expected"]
        arguments = {"FIXTURE_RUN": self.inputs.raw["run_id"], "FIXTURE_VARIANT": "alpha"}
        first_result: list[Command] = []

        def stage() -> list[str]:
            result, dest = self.build("alpha", "Dockerfile", arguments)
            assert_export(dest, "payload.txt", expected["build_alpha"].encode())
            assert_payload_vertex(result.stderr, cached=False)
            first_result.append(result)
            return ["local final stage contains only exact alpha payload", "payload vertex actually executed"]

        self.observe("build-multi-stage", ["docker.build.multi_stage", "docker.build.output_export"], stage)

        def reuse() -> list[str]:
            result, dest = self.build("alpha-reuse", "Dockerfile", arguments)
            assert_export(dest, "payload.txt", expected["build_alpha"].encode())
            first_id = assert_payload_vertex(first_result[0].stderr, cached=False)
            second_id = assert_payload_vertex(result.stderr, cached=True)
            require(first_id == second_id, "cache observations refer to different vertices")
            return ["identical payload vertex cached on second build", "local payload unchanged"]

        self.observe("build-cache-reuse", ["docker.build.cache_reuse"], reuse)

        def variation() -> list[str]:
            _, dest = self.build("beta", "Dockerfile", arguments | {"FIXTURE_VARIANT": "beta"})
            assert_export(dest, "payload.txt", expected["build_beta"].encode())
            require(expected["build_alpha"] != expected["build_beta"], "fixture argument variants identical")
            return ["alpha and beta arguments produce exact distinct payloads; OCI layer comparison still required"]

        self.observe("build-arguments", ["docker.build.build_arguments"], variation)

        def cache_mount() -> list[str]:
            for state, step in (("cold", "first"), ("warm", "second")):
                args = {"FIXTURE_OWNER": self.inputs.owner, "FIXTURE_CACHE_EXPECT": state, "FIXTURE_CACHE_STEP": step}
                _, dest = self.build("cache-" + state, "Dockerfile.cache", args)
                payload = f"vz04-cache-v1\nowner={self.inputs.owner}\nstate={state}\nstep={step}\n".encode()
                assert_export(dest, "cache.txt", payload)
            return ["same builder cache mount cold-to-warm owner sentinel preserved; sibling isolation still required"]

        self.observe("build-cache-mount", ["docker.build.cache_isolation"], cache_mount)

        def secret() -> list[str]:
            args = {"FIXTURE_SECRET_SHA256": self.fixture_spec["secret_input_sha256"]}
            _, dest = self.build("secret", "Dockerfile.secret", args,
                                 extra=["--no-cache", "--secret", "id=fixture,src=" + str(self.fixture / "inputs/secret.txt")])
            assert_export(dest, "secret.txt", expected["secret_output"].encode())
            result, _ = self.build("secret-missing", "Dockerfile.secret", args, extra=["--no-cache"], expected=None)
            require(not result.timed_out and result.returncode > 0, "missing required secret did not fail normally")
            assert_required_secret_failure(result.stderr, regular(self.fixture / "build/Dockerfile.secret"))
            self.record.acknowledge_negative(result, "terminal BuildKit required fixture secret mount error")
            return ["secret digest checked inside mount; next RUN lacks mount; exact public payload",
                    "uncached missing-secret build rejected; image/cache blob scanning still required"]

        self.observe("build-secret-mount", ["docker.build.secrets"], secret)

    def run(self, suite: str) -> dict[str, Any]:
        require(suite in SUITE_RECIPES, "unknown selected suite")
        require(suite == self.inputs.suite, "run suite differs from admitted input suite")
        failure = None
        try:
            self.guard()
            self.verify_images()
            if suite in {"build", "all"}:
                self.build_workloads()
            if suite in {"compose", "all"}:
                self.compose_workloads()
        except (Exception, KeyboardInterrupt) as error:
            failure = f"{type(error).__name__}: {error}"
        finally:
            cleanup_errors = self.cleanup()
        result = {"schema_version": 1, "kind": "docker_host_fixture_subset", "suite": suite, "run_id": self.inputs.raw["run_id"],
                  "scope": self.inputs.scope, "release_sha256": self.inputs.raw["release_sha256"],
                  "fixture_sha256": self.inputs.raw["fixture_sha256"], "compatibility_certified": False,
                  "release_scenarios_passed": [], "test_case_retries": 0,
                  "outcome": "failed" if failure or cleanup_errors else "fixture_assertions_passed",
                  "failure": failure, "cleanup_errors": cleanup_errors,
                  "observations": self.observations, "command_count": self.record.count,
                  "owned_projects": {project: {kind: sorted(ids) for kind, ids in kinds.items()}
                                     for project, kinds in self.projects.items()},
                  "remaining": ["All 63 full release scenarios require aggregate certification and physical evidence",
                                "Immutable ownership/release/runtime attestation and sibling isolation",
                                "OCI layer/image digests, secret scans across all image/cache blobs",
                                "Fresh-builder exported-cache imports and cross-Machine cache observations",
                                "Owned SSH server/agent provisioning and positive/negative SSH builds",
                                "Compose logs-follow, Machine-level persistence and unrelated cleanup decoys",
                                "Complete registry/container/storage/network/pressure/recovery lane"]}
        validate_result(result)
        (self.output / "result.json").write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
        (self.output / "inputs.json").write_text(json.dumps(self.inputs.raw, sort_keys=True, indent=2) + "\n")
        rows = []
        for path in sorted(self.output.rglob("*")):
            if self.temporary in path.parents or path == self.temporary:
                continue
            require(not path.is_symlink(), "evidence symlink rejected")
            if path.is_file():
                rows.append(f"{sha256(regular(path))}  {path.relative_to(self.output).as_posix()}\n")
        (self.output / "checksums.sha256").write_text("".join(rows))
        return result


def assert_transport_denied(command: Command, url: str, *, dns_name: bool) -> None:
    require(command.returncode == 0 and not command.timed_out and not command.stderr and len(command.stdout) <= 2048,
            "transport probe did not complete a bounded observation")
    row = json.loads(command.stdout)
    require(set(row) == {"schema_version", "url", "outcome", "status", "errno", "exception"} and
            row["schema_version"] == 1 and row["url"] == url and row["status"] is None and
            (row["errno"] is None or type(row["errno"]) is int) and
            isinstance(row["exception"], str) and len(row["exception"]) <= 64 and
            row["outcome"] in ({"timeout", "network_unreachable", "connection_refused", "dns_failure"} if dns_name else
                               {"timeout", "network_unreachable", "connection_refused"}),
            "HTTP/application errors and unclassified observations are not isolation evidence")


def assert_blocked_events(events: list[dict[str, Any]], ids: dict[str, str], project: str) -> None:
    require(set(ids) == {"db", "api", "worker", "isolated"} and len(set(ids.values())) == 4,
            "four distinct blocked service identities required")
    times: dict[tuple[str, str], list[int]] = {}
    for event in events:
        if event.get("Type") != "container":
            continue
        actor = event["Actor"]
        require(actor["ID"] in ids.values() and actor["Attributes"]["com.docker.compose.project"] == project,
                "foreign blocked event identity")
        stamp = event["timeNano"]
        require(type(stamp) is int and stamp > 0, "missing Engine event timestamp")
        times.setdefault((actor["ID"], event["Action"]), []).append(stamp)
    # Positive create history establishes that the bounded event window reaches
    # back to the exact containers' birth; empty or tail-only history fails.
    for role, actor in ids.items():
        require(len(times.get((actor, "create"), [])) == 1, "incomplete blocked creation history")
        if role in {"api", "worker"}:
            require(not any(times.get((actor, action)) for action in ("start", "die", "destroy", "restart")),
                    "dependent has a forbidden lifecycle event")
    created = times[(ids["db"], "create")]
    started = times.get((ids["db"], "start"), [])
    unhealthy = times.get((ids["db"], "health_status: unhealthy"), [])
    require(len(started) == len(unhealthy) == 1 and created[0] < started[0] < unhealthy[0],
            "missing or invalid database create/start/unhealthy Engine sequence")


def assert_health_order(events: list[dict[str, Any]], ids: dict[str, str], project: str) -> None:
    times: dict[tuple[str, str], list[int]] = {}
    for event in events:
        if event.get("Type") != "container":
            continue
        actor = event["Actor"]
        require(actor["ID"] in ids.values(), "foreign container event")
        require(actor["Attributes"]["com.docker.compose.project"] == project, "foreign event project")
        times.setdefault((actor["ID"], event["Action"]), []).append(event["timeNano"])
    for dependency, dependent in (("db", "api"), ("api", "worker")):
        healthy = times.get((ids[dependency], "health_status: healthy"), [])
        starts = times.get((ids[dependent], "start"), [])
        require(len(healthy) == len(starts) == 1 and healthy[0] < starts[0],
                "missing, repeated or misordered dependency health/start events")


def assert_builder_inspect(raw: bytes, builder: dict[str, str], context: str) -> None:
    # buildx inspect has no --format flag. Parse only exact structural fields of
    # the retained upstream text, rejecting duplicate nodes and diagnostics.
    sections = raw.decode().split("\nNodes:\n")
    require(len(sections) == 2, "missing or repeated builder Nodes section")

    def fields(text: str, keys: set[str]) -> dict[str, str]:
        result = {}
        for line in text.splitlines():
            key, separator, value = line.partition(":")
            require(key.strip() != "Error", "builder inspection reported an error")
            if separator and key in keys:
                require(key not in result, "duplicate builder or node field")
                result[key] = value.strip()
        require(set(result) == keys, "builder inspection missing identity fields")
        return result

    require(fields(sections[0], {"Name", "Driver"}) ==
            {"Name": builder["name"], "Driver": "docker-container"}, "wrong builder identity")
    require(fields(sections[1], {"Name", "Endpoint", "Status"}) ==
            {"Name": builder["node"], "Endpoint": context, "Status": "running"},
            "builder routes to wrong Machine/node")


def assert_export(root: Path, name: str, expected: bytes) -> None:
    require(root.is_dir() and not root.is_symlink(), "local export directory missing")
    require(sorted(p.name for p in root.iterdir()) == [name], "unexpected final-stage files")
    require(regular(root / name) == expected, "exported payload differs")


def buildkit_vertices(line: str) -> list[dict[str, Any]]:
    """Decode the pinned Buildx v0.33.0 SolveStatus batch shape."""
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            require(key not in result, "duplicate BuildKit JSON field")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise Rejected("invalid BuildKit JSON constant: " + value)

    row = json.loads(line, object_pairs_hook=unique_object, parse_constant=reject_constant)
    require(isinstance(row, dict), "BuildKit progress must be an object")
    require(set(row) <= {"vertexes", "statuses", "logs", "warnings"}, "unknown BuildKit progress shape")
    for members in row.values():
        require(isinstance(members, list) and all(isinstance(member, dict) for member in members),
                "malformed BuildKit progress batch")
    vertices = row.get("vertexes", [])
    for vertex in vertices:
        checked_text(vertex.get("digest"), r"sha256:[0-9a-f]{64}", "BuildKit vertex ID")
        require(isinstance(vertex.get("name"), str), "missing BuildKit vertex name")
        require(type(vertex.get("cached", False)) is bool, "invalid BuildKit cached boolean")
        require(isinstance(vertex.get("error", ""), str), "invalid BuildKit vertex error")
        for field in ("started", "completed"):
            if field in vertex:
                checked_text(vertex[field], r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})",
                             "BuildKit " + field)
    return vertices


def buildkit_lines(raw: bytes) -> list[str]:
    require(0 < len(raw) <= MAX_STREAM_BYTES, "missing or oversized BuildKit output")
    lines = raw.decode("utf-8").splitlines()
    require(len(lines) <= 20000 and all(lines), "empty or excessive BuildKit progress lines")
    return lines


def assert_required_secret_failure(raw: bytes, dockerfile: bytes) -> str:
    """Require a terminal missing-secret vertex and the pinned CLI error trailer.

    Buildx v0.33.0 cmd/buildx/main.go prints solver source excerpts and ERROR
    after its rawjson stream. Only this fixture's exact excerpt is accepted;
    malformed JSON is never reclassified as an ignorable human diagnostic.
    """
    command = "RUN --network=none --mount=type=secret,id=fixture,required=true python3 /fixture/tools.py secret"
    failure = "secret fixture: not found"
    lines = buildkit_lines(raw)
    vertices = []
    prefix_length = 0
    for line in lines:
        if not line.startswith("{"):
            break
        vertices.extend(buildkit_vertices(line))
        prefix_length += 1
    lines = lines[prefix_length:]
    source = dockerfile.decode("utf-8").splitlines()
    require(len(source) >= 8 and source[5] == command, "required-secret fixture source changed")
    excerpt = ["Dockerfile.secret:6", "--------------------"]
    excerpt += [f" {number:3d} | {'>>>' if number == 6 else '   '} {source[number - 1]}"
                for number in range(4, 9)]
    excerpt.append("--------------------")
    trailer = ["ERROR: failed to solve: " + failure]
    require(lines in (trailer, excerpt + trailer), "missing or unrecognized required-secret error trailer")
    matches = []
    for vertex in vertices:
        target = re.fullmatch(r"\[build [0-9]+/[0-9]+\] " + re.escape(command), vertex["name"])
        if target and "completed" in vertex:
            require(vertex.get("cached", False) is False and vertex.get("error") == failure,
                    "required-secret vertex did not fail for the exact missing mount")
            matches.append(vertex["digest"])
        elif vertex.get("error"):
            # Other concurrently canceled work is not proof of this failure.
            require(vertex["error"] in {"context canceled", "context cancelled"},
                    "unrelated BuildKit failure in required-secret build")
    require(len(matches) == 1, "missing or duplicated terminal required-secret vertex")
    return matches[0]


def assert_payload_vertex(raw: bytes, *, cached: bool) -> str:
    # v0.33.0 rawjson encodes batched SolveStatus, with cached:false omitted.
    # A terminal vertex is mandatory; text/timing is never a cache-hit heuristic.
    matches = []
    for row in (vertex for line in buildkit_lines(raw) for vertex in buildkit_vertices(line)):
        if re.fullmatch(r"\[build [0-9]+/[0-9]+\] RUN --network=none python3 /fixture/tools.py payload", row["name"]) and row.get("completed"):
            require("error" not in row or not row["error"], "payload vertex failed")
            require(row.get("cached", False) is cached, "payload vertex cache status differs")
            matches.append(row["digest"])
    require(len(set(matches)) == 1 and len(matches) == 1, "missing or duplicated terminal payload vertex")
    return checked_text(matches[0], r"sha256:[0-9a-f]{64}", "BuildKit vertex ID")


def validate_result(result: dict[str, Any]) -> None:
    """Semantic checks supplement the adjacent JSON Schema."""
    require(result["schema_version"] == 1 and result["compatibility_certified"] is False and
            result["release_scenarios_passed"] == [] and result["test_case_retries"] == 0,
            "fixture runner cannot certify release scenarios")
    suite = result.get("suite")
    require(suite in SUITE_RECIPES, "missing or invalid selected suite")
    expected = SUITE_RECIPES[suite]
    count = result.get("command_count")
    require(type(count) is int and count >= 0, "invalid command count")
    seen = set()
    previous_end = 0
    for index, observation in enumerate(result["observations"]):
        require(index < len(expected) and observation["recipe"] == expected[index],
                "observations must follow the exact selected-suite recipe inventory")
        require(observation["recipe"] not in seen, "recipe executed more than once")
        seen.add(observation["recipe"])
        require(observation["outcome"] in {"failed", "fixture_assertions_passed"}, "invalid outcome")
        start, end = observation["first_command"], observation["last_command"]
        require(type(start) is int and type(end) is int and 1 <= start <= count + 1 and
                start - 1 <= end <= count and start > previous_end, "invalid or overlapping command range")
        previous_end = end
        if observation["outcome"] == "fixture_assertions_passed":
            require(observation["assertions"] and 1 <= start <= end <= count,
                    "unexecuted observation cannot pass")
        else:
            require(index == len(result["observations"]) - 1, "execution continued after a failed recipe")
    if result["outcome"] == "fixture_assertions_passed":
        require(len(result["observations"]) == len(expected) and not result["failure"] and not result["cleanup_errors"] and
                all(x["outcome"] == "fixture_assertions_passed" for x in result["observations"]),
                "incomplete or failed subset cannot pass")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True,
                        help="JSON: schema_version, run_id, release_sha256, fixture_sha256, scope, docker_config, clients, images, builder")
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="fresh absolute private evidence directory")
    parser.add_argument("--suite", choices=("compose", "build", "all"), required=True)
    args = parser.parse_args()
    try:
        inputs = Inputs(json.loads(regular(args.inputs)), suite=args.suite)
        result = Driver(inputs, absolute(str(args.fixture)), args.output).run(args.suite)
        print(json.dumps({"outcome": result["outcome"], "compatibility_certified": False,
                          "evidence": str(args.output / "result.json")}))
        return 0 if result["outcome"] == "fixture_assertions_passed" else 1
    except (Rejected, ValueError, OSError, KeyError) as error:
        print(json.dumps({"outcome": "rejected", "error": str(error), "compatibility_certified": False}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
