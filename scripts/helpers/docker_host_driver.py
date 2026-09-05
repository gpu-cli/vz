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


def regular(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= 512 * 1024 * 1024,
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

    def __post_init__(self) -> None:
        required = {"schema_version", "run_id", "release_sha256", "fixture_sha256",
                    "scope", "docker_config", "clients", "images", "builder"}
        require(set(self.raw) == required, "unknown or missing input keys")
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
        builder = self.raw["builder"]
        require(set(builder) == {"name", "node", "container_id", "image_id"},
                "exact pre-provisioned builder identity required")
        for key in ("name", "node"):
            checked_text(builder[key], r"[a-z0-9][a-z0-9-]{0,62}", "owned builder name")
        checked_text(builder["container_id"], r"[0-9a-f]{64}", "builder container ID")
        checked_text(builder["image_id"], r"sha256:[0-9a-f]{64}", "builder image ID")

    @property
    def scope(self) -> dict[str, str]:
        return self.raw["scope"]

    @property
    def owner(self) -> str:
        material = json.dumps([self.raw["run_id"], self.scope], sort_keys=True).encode()
        return "vz04-" + sha256(material)[:24]


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
    with subprocess.Popen(argv, start_new_session=True, **kwargs) as process:
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
            # Never communicate() here: that could allocate an unlimited tail.
            # collect_output attached only its bounded observed prefixes.
            process.wait()
            raise
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)


def contains_canary(streams: tuple[bytes, ...], canaries: list[bytes]) -> bool:
    decoded_logs = []
    for stream in streams:
        for line in stream.splitlines():
            try:
                row = json.loads(line)
                if isinstance(row, dict) and isinstance(row.get("data"), str):
                    decoded_logs.append(base64.b64decode(row["data"], validate=True))
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
            extra_env: dict[str, str] | None = None) -> Command:
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
                   "host_outcome": "inflight", "effects_uncertain": True,
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
                   "effects_uncertain": timed_out or pending_error is not None or code < 0,
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


class Driver:
    def __init__(self, inputs: Inputs, fixture: Path, output: Path):
        require(sys.platform == "darwin" and os.uname().machine == "arm64", "this DEV runner requires Apple-silicon macOS")
        self.inputs, self.fixture, self.output = inputs, fixture, output
        require(tree_digest(fixture) == inputs.raw["fixture_sha256"], "fixture tree digest mismatch")
        require(not output.exists() and output.is_absolute(), "fresh absolute output directory required")
        require(output.parent == output.parent.resolve(), "output ancestor symlink rejected")
        output.mkdir(mode=0o700)
        self.home = output / "private-home"
        self.home.mkdir(mode=0o700)
        self.env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(self.home),
                    "TMPDIR": str(self.home), "LC_ALL": "C", "NO_COLOR": "1"}
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
        require(parsed.get("cliPluginsExtraDirs") == directories,
                "plugin directories must exactly match pinned installed clients")
        for directory in directories:
            for name in ("compose", "buildx"):
                candidate = Path(directory) / ("docker-" + name)
                if candidate.exists():
                    require(candidate == Path(clients[name]["path"]), "shadow plugin rejected")
        require(not (config / "cli-plugins").exists(), "shadow config plugins rejected")
        return sha256(data)

    def command(self, args: list[str], *, expected: int | None = 0, timeout: int = 120,
                env: dict[str, str] | None = None) -> Command:
        require(self.validate_config() == self.config_snapshot, "client config changed")
        for pin in self.inputs.raw["clients"].values():
            require(sha256(regular(Path(pin["path"]))) == pin["sha256"], "client executable changed")
        executable = self.inputs.raw["clients"]["docker"]["path"]
        argv = ["docker", "--config",
                self.inputs.raw["docker_config"], "--context", self.inputs.scope["docker_context"], *args]
        result = self.record.run(argv, executable=executable, timeout=timeout, extra_env=env)
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
        require(set(info["Runtimes"]) <= {"youki", "io.containerd.youki.v2"},
                "unexpected OCI runtime registered")

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
        self.projects[project] = {kind: set() for kind in inventory}
        return project

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
                self.capture(project)
                require(not uncertain(), "cleanup observations do not establish a safe destructive dispatch")
                self.compose(project, ["--profile", "failure", "down", "--volumes", "--remove-orphans"])
                remaining = self.inspect_project(project)
                require(not any(remaining.values()), "owned Compose resources survived down")
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
                    response = self.exec_container(services[source], ["python3", "/fixture/service.py", "probe",
                                                                      f"http://{address}:{port}/health"], expected=1)
                    require(response.stdout == b"" and response.stderr.endswith(b": fixture operation failed\n"),
                            "network negative control did not execute fixture probe")
            return ["exact network memberships", "declared paths returned exact database bytes",
                    "forbidden paths denied by DNS name and every inspected destination IP"]

        self.observe("compose-network-paths", ["docker.compose.networks"], networks)

        def volumes() -> list[str]:
            before = self.capture(project)
            db = self.by_service(before)["db"][0]
            mounts = [mount for mount in db["Mounts"] if mount["Destination"] == "/data"]
            require(len(mounts) == 1 and mounts[0]["Type"] == "volume" and mounts[0]["RW"] and
                    mounts[0]["Name"] == project + "_state", "wrong database volume binding")
            payload = f"vz04|db|{self.inputs.owner}|persisted\n".encode()
            result = self.exec_container(db, ["python3", "-c", "import pathlib,sys;sys.stdout.buffer.write(pathlib.Path('/data/sentinel.txt').read_bytes())"])
            require(result.stdout == payload, "initial persistence bytes differ")
            self.compose(project, ["stop"])
            self.compose(project, ["up", "--detach", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "30"], timeout=40)
            after = self.capture(project)
            require(self.identities(before) == self.identities(after), "Compose stop/up changed resource identity")
            result = self.exec_container(self.by_service(after)["db"][0],
                                         ["python3", "-c", "import pathlib,sys;sys.stdout.buffer.write(pathlib.Path('/data/sentinel.txt').read_bytes())"])
            require(result.stdout == payload, "persistence sentinel changed after Compose stop/up")
            return ["exact owned named volume mount", "Compose stop/up preserved IDs and exact sentinel bytes; Machine recovery still required"]

        self.observe("compose-volume-persistence", ["docker.compose.volumes"], volumes)

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
            require(services.get("db") and services["db"][0]["State"].get("Health", {}).get("Status") == "unhealthy",
                    "negative control never observed unhealthy database")
            for role in ("api", "worker"):
                require(all(x["State"]["Status"] == "created" and not x["State"]["Running"]
                            for x in services.get(role, [])), "dependent started despite unhealthy database")
            events = self.command(["events", "--since", began, "--until", str(int(time.time()) + 1),
                                   "--filter", "label=com.docker.compose.project=" + blocked_project,
                                   "--format", "{{json .}}"], timeout=10)
            for line in events.stdout.splitlines():
                event = json.loads(line)
                if event.get("Type") == "container" and event.get("Action") == "start":
                    require(event["Actor"]["Attributes"]["com.docker.compose.service"] not in {"api", "worker"},
                            "Engine recorded forbidden dependent start")
            return ["unhealthy database observed; dependent containers never started"]

        self.observe("compose-blocked-health", ["docker.compose.health_ordering"], blocked)

        def failure() -> list[str]:
            failing_project = self.new_project("failure")
            self.compose(failing_project, ["--profile", "failure", "up", "--no-build", "--pull", "never",
                                            "--abort-on-container-exit", "--exit-code-from", "failure"], expected=37, timeout=40)
            services = self.by_service(self.capture(failing_project))
            require(len(services.get("failure", [])) == 1, "failure container evidence missing")
            job = services["failure"][0]
            require(job["State"]["Status"] == "exited" and job["State"]["ExitCode"] == 37,
                    "failure job exit mismatch")
            logs = self.command(["logs", job["Id"]])
            require(logs.stdout == f"vz04|failure|{self.inputs.owner}|exit-37\n".encode() and not logs.stderr,
                    "failure job log mismatch")
            return ["Compose and exact failure job exit 37; partial resources captured"]

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
            errors = [json.loads(line).get("error", "") for line in result.stderr.splitlines()]
            require(any("secret" in error and "fixture" in error and
                        ("not found" in error or "required" in error) for error in errors),
                    "negative build failed for an unproven reason, not the required secret mount")
            return ["secret digest checked inside mount; next RUN lacks mount; exact public payload",
                    "uncached missing-secret build rejected; image/cache blob scanning still required"]

        self.observe("build-secret-mount", ["docker.build.secrets"], secret)

    def run(self, suite: str) -> dict[str, Any]:
        require(suite in SUITE_RECIPES, "unknown selected suite")
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
            if self.home in path.parents or path == self.home:
                continue
            require(not path.is_symlink(), "evidence symlink rejected")
            if path.is_file():
                rows.append(f"{sha256(regular(path))}  {path.relative_to(self.output).as_posix()}\n")
        (self.output / "checksums.sha256").write_text("".join(rows))
        return result


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


def assert_payload_vertex(raw: bytes, *, cached: bool) -> str:
    # buildx --progress rawjson emits SolveStatus-shaped vertex objects as
    # individual lines. A terminal vertex receipt is mandatory; text/timing is
    # never used as a cache-hit heuristic. Unknown shapes fail closed.
    matches = []
    for line in raw.splitlines():
        row = json.loads(line)
        if row.get("name", "").endswith("python3 /fixture/tools.py payload") and row.get("completed"):
            require("error" not in row or not row["error"], "payload vertex failed")
            require(bool(row.get("cached", False)) is cached, "payload vertex cache status differs")
            matches.append(row["id"])
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
        inputs = Inputs(json.loads(regular(args.inputs)))
        result = Driver(inputs, absolute(str(args.fixture)), args.output).run(args.suite)
        print(json.dumps({"outcome": result["outcome"], "compatibility_certified": False,
                          "evidence": str(args.output / "result.json")}))
        return 0 if result["outcome"] == "fixture_assertions_passed" else 1
    except (Rejected, ValueError, OSError, KeyError) as error:
        print(json.dumps({"outcome": "rejected", "error": str(error), "compatibility_certified": False}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
