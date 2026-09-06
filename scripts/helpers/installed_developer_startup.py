#!/usr/bin/env python3
"""DEV physical installed Up + host Docker startup proof, not release certification.

All selectors are explicit. The normal CLI autospawns its installed sibling
daemon; this harness does not supply a Machine catalog or daemon executable
override. Failure retains the isolated installation, state and exact receipts.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import tarfile
import time
import uuid

from docker_host_driver import collect_output, execute, require

SCOPE = "DEV_INSTALLED_DEVELOPER_STARTUP_NOT_RELEASE_CERTIFICATION"
LIMIT = 4 * 1024 * 1024
MARKER = b"vz-developer-probe-v1\n"
PAYLOAD = b"vz-installed-host-buildx-v1\n"
OPTIONS = ("release-dir", "release-version", "developer-bundle", "hardened-bundle",
           "docker", "compose-plugin", "buildx-plugin", "evidence-dir")


def digest(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= 2 * 1024**3,
                f"bounded single-link regular file required: {path}")
        hasher = hashlib.sha256()
        observed = 0
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            observed += len(block)
            require(observed <= before.st_size, "input grew during hashing")
            hasher.update(block)
        after = os.fstat(stream.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns), f"input changed: {path}")
    return hasher.hexdigest()


def write(path, data):
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def document(path, value):
    write(path, json.dumps(value, indent=2, sort_keys=True).encode() + b"\n")


def bounded_json(path):
    require(path.stat().st_size <= LIMIT and not path.is_symlink(), "bounded JSON file required")
    return json.loads(path.read_bytes())


def read_private_regular(path, limit):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_uid == os.geteuid() and
                before.st_mode & 0o077 == 0 and before.st_size <= limit, "bounded private regular receipt required")
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(len(data) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_nlink) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_nlink),
                "receipt changed during bounded read")
        return data


def canonical(value, links=False):
    path = Path(value)
    require(path.is_absolute() and not any(c in str(path) for c in "\r\n\x00"), "absolute clean path required")
    resolved = path.resolve(strict=True)
    require(links or resolved == path, f"canonical path required: {path}")
    return resolved


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    for name in OPTIONS:
        require(sum(item == "--" + name or item.startswith("--" + name + "=") for item in argv) <= 1,
                f"duplicate option: --{name}")
        parser.add_argument("--" + name, required=True)
    return parser.parse_args(argv)


def bundle_inputs(directory, profile):
    version = bounded_json(directory / "version.json")
    require(version.get("profile") == profile, "wrong bundle profile")
    files = {}
    for name, key in (("vmlinux", "sha256_vmlinux"), ("initramfs.img", "sha256_initramfs"), ("youki", "sha256_youki")):
        actual = digest(directory / name)
        require(actual == version.get(key), f"declared artifact digest differs: {name}")
        files[str(directory / name)] = actual
    files[str(directory / "version.json")] = digest(directory / "version.json")
    probe = version.get("developer_probe")
    if profile == "developer":
        require(isinstance(probe, dict) and probe.get("schema_version") == 1 and
                probe.get("archive") == "developer-probe-rootfs.tar", "new normal Developer probe bundle required")
        archive = directory / probe["archive"]
        require(digest(archive) == probe.get("sha256"), "startup archive digest mismatch")
        files[str(archive)] = probe["sha256"]
    else:
        require(probe is None and not (directory / "developer-probe-rootfs.tar").exists(), "Hardened must not acquire Docker probe")
    return files


def preflight(args, require_host=True):
    if require_host:
        require(platform.system() == "Darwin" and platform.machine() == "arm64", "Apple-silicon macOS required")
    require(re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?", args.release_version), "explicit release version required")
    release = canonical(args.release_dir)
    developer, hardened = canonical(args.developer_bundle), canonical(args.hardened_bundle)
    require(all(path.is_dir() for path in (release, developer, hardened)), "input directories required")
    evidence = Path(args.evidence_dir)
    require(evidence.is_absolute() and not any(c in str(evidence) for c in "\r\n\x00") and
            not os.path.lexists(evidence), "fresh absolute evidence directory required")
    require(evidence.parent == evidence.parent.resolve(strict=True), "evidence parent must be canonical")
    inputs = bundle_inputs(developer, "developer") | bundle_inputs(hardened, "container")
    clients = {}
    for key, value in (("vz", release / "vz"), ("vz-runtimed", release / "vz-runtimed"),
                       ("docker", args.docker), ("docker-compose", args.compose_plugin), ("docker-buildx", args.buildx_plugin)):
        path = canonical(str(value), links=key.startswith("docker"))
        require(os.access(path, os.X_OK), f"executable required: {key}")
        inputs[str(path)] = digest(path)
        clients[key] = {"invocation": str(value), "canonical": str(path), "sha256": inputs[str(path)]}
    repo = Path(__file__).resolve().parents[2]
    for path in (Path(__file__).resolve(), repo / "scripts/run-installed-developer-startup-e2e.sh",
                 repo / "scripts/helpers/docker_host_driver.py"):
        inputs[str(path)] = digest(path)
    return {"schema_version": 1, "scope": SCOPE, "release_version": args.release_version,
            "developer_bundle": str(developer), "hardened_bundle": str(hardened),
            "evidence": str(evidence), "clients": clients, "inputs": inputs,
            "docker_parity_certified": False, "aggregate_release_certified": False}


def execute_observer(argv, **kwargs):
    """Reap only the CLI observer: its autospawned daemon is not a disposable plugin.

    The normal spawn implementation currently inherits the CLI process group.
    A timeout must not turn into an unverified SIGKILL of that daemon or its VMs.
    """
    timeout, limit = kwargs.pop("timeout"), kwargs.pop("max_stream_bytes")
    kwargs.pop("check")
    process = subprocess.Popen(argv, start_new_session=True, **kwargs)
    try:
        try:
            stdout, stderr = collect_output(process, timeout, limit)
        except BaseException as error:
            if process.returncode is None:
                process.kill()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired as reap_error:
                reap_error.stdout = getattr(error, "stdout", b"")
                reap_error.stderr = getattr(error, "stderr", b"")
                raise reap_error from error
            raise
        return subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    finally:
        # Popen.__exit__ would wait without a deadline even after a failed reap.
        # Do not abandon uncertainty or signal the autospawned daemon's group.
        for pipe in (process.stdin, process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()


class Recorder:
    def __init__(self, root, env):
        self.root, self.env, self.receipts = root, env, []
        self.canaries = []

    def run(self, label, argv, *, cwd, executable=None, timeout=60, stdin=None, success=True, observer_only=False):
        from docker_host_driver import contains_canary
        private_inputs = (*map(str, argv), str(executable or argv[0]), str(cwd),
                          *self.env.keys(), *self.env.values())
        require(not contains_canary(tuple(value.encode() for value in private_inputs), self.canaries),
                "private canary rejected before command intent/dispatch")
        index = len(self.receipts) + 1
        stem = f"{index:03}-{label}"
        row = {"index": index, "label": label, "argv": list(map(str, argv)), "argv0": str(argv[0]),
               "executable": str(executable or argv[0]), "cwd": str(cwd), "timeout_seconds": timeout,
               "started_unix_ns": time.time_ns(), "effects_uncertain": True, "capture_complete": False}
        row["termination_scope"] = "observer_pid_only" if observer_only else "owned_host_process_group"
        self.receipts.append(row)
        document(self.root / (stem + ".intent.json"), row)
        started = time.monotonic_ns()
        error, stdout, stderr, code = None, b"", b"", None
        try:
            executor = execute_observer if observer_only else execute
            output = executor(row["argv"], executable=row["executable"], cwd=cwd, env=self.env,
                             stdin=stdin if stdin is not None else subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             timeout=timeout, max_stream_bytes=LIMIT, check=False)
            code, stdout, stderr = output.returncode, output.stdout, output.stderr
            row.update(effects_uncertain=code < 0, capture_complete=True)
        except BaseException as exception:
            error = exception
            stdout, stderr = getattr(exception, "stdout", b"") or b"", getattr(exception, "stderr", b"") or b""
        if contains_canary((stdout, stderr), self.canaries):
            error = ValueError("private canary detected; command streams withheld")
            stdout, stderr = b"[private stream withheld]\n", b"[private stream withheld]\n"
            row.update(effects_uncertain=True, capture_complete=False, secret_leak_detected=True)
        row.update(exit_code=code, elapsed_ns=time.monotonic_ns() - started,
                   error=None if error is None else f"{type(error).__name__}: {error}",
                   stdout_sha256=hashlib.sha256(stdout).hexdigest(), stderr_sha256=hashlib.sha256(stderr).hexdigest(),
                   retained_stdout_bytes=len(stdout), retained_stderr_bytes=len(stderr),
                   hashes_cover="redacted_placeholders_not_original_streams" if row.get("secret_leak_detected") else
                   "complete_streams" if row["capture_complete"] else "retained_observed_prefixes")
        write(self.root / (stem + ".stdout"), stdout)
        write(self.root / (stem + ".stderr"), stderr)
        document(self.root / (stem + ".result.json"), row)
        if error is not None:
            raise error
        require(not success or code == 0, f"{label} failed with exit {code}; retained raw logs")
        return stdout, stderr, code


def private(path):
    path.mkdir(mode=0o700)
    return path


def snapshot_config(path):
    if not os.path.lexists(path):
        return None
    return digest(path)


def image_id(raw):
    text = raw.decode().strip()
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", text), "immutable image ID required")
    return text


def context_descriptor(environment, machine, config):
    require(environment["state"] == "ready" and machine["state"] == "ready", "Machine is not operationally ready")
    require({"docker_engine", "compose", "buildx"} <= set(machine["negotiated_capabilities"]["capabilities"]), "three Docker capabilities required")
    descriptor = machine["docker_context"]
    owner = descriptor["owner"]
    require(owner["project_id"] == environment["project_id"] and owner["environment_id"] == environment["environment_id"] and
            owner["machine_id"] == machine["machine_id"], "foreign descriptor owner")
    require(descriptor["schema_version"] == 1 and descriptor["config_dir"] == str(config) and
            descriptor["name"] != "default" and descriptor["endpoint"].startswith("unix:///"), "wrong descriptor routing")
    require(descriptor["incarnation_id"] == machine["incarnation_id"] and
            descriptor["incarnation_generation"] == machine["incarnation_generation"], "stale descriptor incarnation")
    return descriptor


def machine_config_path(runtime, owner):
    """Derive from immutable owners, never from a descriptor-supplied path."""
    kind, logical = "other:machine_runtime_store", "runtime"
    raw = b"vz.resource-name.v1\x00"
    for field in (owner["project_id"], owner["environment_id"], owner["machine_id"], kind, logical):
        encoded = field.encode("ascii")
        raw += len(encoded).to_bytes(8, "little") + encoded
    readable = re.sub(r"-+", "-", re.sub(r"[^A-Za-z0-9_.]", "-", kind + "-" + logical)).strip("-")[:26]
    key = "vzr1-" + readable + "-" + hashlib.sha256(raw).hexdigest()[:32]
    return Path(runtime) / "topology-machines" / key / "data/docker-client"


def managed_context_descriptor(environment, machine, runtime):
    owner = {"project_id": environment["project_id"], "environment_id": environment["environment_id"],
             "machine_id": machine["machine_id"]}
    config = machine_config_path(runtime, owner)
    descriptor = context_descriptor(environment, machine, config)
    require(config.resolve(strict=True) == config, "redirected Machine client configuration")
    metadata = config.lstat()
    require(stat.S_ISDIR(metadata.st_mode) and stat.S_IMODE(metadata.st_mode) == 0o700 and
            metadata.st_uid == os.geteuid(), "Machine client configuration is not private")
    store_owner = json.loads(read_private_regular(config.parent.parent / "owner.json", LIMIT))
    claim = json.loads(read_private_regular(config / "vz-owner.json", LIMIT))
    require(store_owner["owner"] == owner and claim["schema_version"] == 1 and claim["owner"] == owner and
            claim["directory"] == {"device": metadata.st_dev, "inode": metadata.st_ino} and
            re.fullmatch(r"lop_[0-9a-f]{32}", claim["nonce"]) is not None,
            "Machine client configuration ownership differs")
    require((config.lstat().st_dev, config.lstat().st_ino) == (metadata.st_dev, metadata.st_ino),
            "Machine client configuration changed during admission")
    return descriptor


def exact_developer_topology(primary, neighbor):
    require(len(primary["machines"]) == 2 and len(neighbor["machines"]) == 2, "exact two-plus-two Developer topology required")
    require(primary["project_id"] == neighbor["project_id"] and primary["environment_id"] != neighbor["environment_id"],
            "distinct named Environments in the same project required")
    machines = primary["machines"] + neighbor["machines"]
    require(len({machine["machine_id"] for machine in machines}) == 4 and
            all(machine["profile"] == "developer" for machine in machines), "four distinct Developer Machine identities required")


class Harness:
    def __init__(self, info):
        self.info = info
        self.evidence = Path(info["evidence"])
        private(self.evidence)
        self.root = Path(tempfile.mkdtemp(prefix="vzdev-", dir="/private/tmp"))
        self.prefix = private(self.root / "install")
        self.bin = private(self.prefix / "bin")
        self.runtime = private(self.root / "r")
        self.socket, self.database = self.runtime / "d.sock", self.root / "state.db"
        self.config = private(self.root / "docker")
        self.cli, self.daemon = self.bin / "vz", self.bin / "vz-runtimed"
        self.cleanup_targets, self.descriptors = [], []
        self.unresolved_up = set()
        self.staged_inputs = {}
        self.daemon_identity = None
        self.env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C", "NO_COLOR": "1",
                    "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
                    "VZ_RUNTIME_STATE_DB": str(self.database), "VZ_RUNTIME_DATA_DIR": str(self.runtime),
                    "VZ_RUNTIME_DAEMON_SOCKET": str(self.socket), "VZ_DOCKER_CONFIG": str(self.config),
                    "VZ_DOCKER_CLIENT": info["clients"]["docker"]["canonical"]}
        # Preserve HOME unchanged, never substitute a fake home or inherit a
        # credential agent, Docker routing override, Cargo daemon, or catalog.
        if "HOME" in os.environ:
            self.env["HOME"] = os.environ["HOME"]
        self.record = Recorder(self.evidence, self.env)
        self.baseline_paths = {Path(os.environ["HOME"]) / ".docker/config.json"} if "HOME" in os.environ else set()
        if os.environ.get("DOCKER_CONFIG"):
            self.baseline_paths.add(Path(os.environ["DOCKER_CONFIG"]) / "config.json")
        self.baseline = {str(path): snapshot_config(path) for path in self.baseline_paths}
        document(self.evidence / "layout.json", {"root": str(self.root), "prefix": str(self.prefix),
                 "socket": str(self.socket), "environment": self.env, "daily_docker_config_hashes": self.baseline})

    def command(self, label, args, cwd=None, **kwargs):
        kwargs["observer_only"] = str(args[0]) == str(self.cli)
        return self.record.run(label, list(map(str, args)), cwd=cwd or self.root, **kwargs)

    def stage(self):
        for key in ("vz", "vz-runtimed"):
            source = self.info["clients"][key]
            destination = self.bin / key
            shutil.copyfile(source["canonical"], destination)
            destination.chmod(0o500)
            require(digest(destination) == source["sha256"], "signed artifact staging changed bytes")
            self.staged_inputs[str(destination)] = source["sha256"]
            self.command(key + "-signature", ["/usr/bin/codesign", "--verify", "--strict", destination])
        plugins = private(self.config / "cli-plugins")
        for key in ("docker-compose", "docker-buildx"):
            source = self.info["clients"][key]
            shutil.copyfile(source["canonical"], plugins / key)
            (plugins / key).chmod(0o500)
            require(digest(plugins / key) == source["sha256"], "plugin staging changed bytes")
            self.staged_inputs[str(plugins / key)] = source["sha256"]
        write(self.config / "config.json", b'{"currentContext":"default"}\n')
        self.default_digest = digest(self.config / "config.json")
        linux = private(self.prefix / "linux")
        for key, profile in (("developer_bundle", "developer"), ("hardened_bundle", "container")):
            source, destination = Path(self.info[key]), private(linux / profile)
            for filename in bundle_inputs(source, profile):
                path = Path(filename)
                shutil.copyfile(path, destination / path.name)
                (destination / path.name).chmod(0o500 if path.name == "youki" else 0o400)
                require(digest(destination / path.name) == self.info["inputs"][filename], "bundle staging changed bytes")
                self.staged_inputs[str(destination / path.name)] = self.info["inputs"][filename]
        self.command("write-installed-catalog", [self.daemon, "--write-installed-machine-target-catalog", self.prefix,
                     "--installed-release-version", self.info["release_version"], "--installed-linux-profile", "developer",
                     "--installed-linux-profile", "container"], timeout=150)
        self.catalog = bounded_json(self.prefix / "machine-target-catalog.json")
        require(len(self.catalog["linux"]) == 2, "exact two-profile installed catalog required")
        document(self.evidence / "installed-catalog.json", self.catalog)
        self.staged_inputs[str(self.prefix / "machine-target-catalog.json")] = digest(self.prefix / "machine-target-catalog.json")
        document(self.evidence / "staged-inputs.json", self.staged_inputs)
        for args in (["--version"], ["compose", "version"], ["buildx", "version"]):
            self.docker("client-version", None, args)

    def docker(self, label, descriptor, args, **kwargs):
        if descriptor is None:
            require(args in (["--version"], ["compose", "version"], ["buildx", "version"]), "context-free Engine operation forbidden")
        name = descriptor["name"] if descriptor else "default"
        config = descriptor["config_dir"] if descriptor else self.config
        return self.command(label, ["docker", "--config", config, "--context", name, *args],
                            executable=self.info["clients"]["docker"]["canonical"], **kwargs)

    def project(self, name, profile, count):
        repository = private(self.root / (name + "-git"))
        self.command("git-init", ["/usr/bin/git", "init", "--quiet"], cwd=repository)
        self.command("git-commit", ["/usr/bin/git", "-c", "user.name=vz DEV fixture", "-c", "user.email=fixture@invalid",
                     "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "owned fixture"], cwd=repository)
        project = self.root / name
        self.command("git-worktree", ["/usr/bin/git", "worktree", "add", "--quiet", "--detach", project], cwd=repository)
        entry = next(entry for entry in self.catalog["linux"] if entry["profile"] == profile)
        definition = {"schema_version": 1, "project_id": "prj_" + uuid.uuid4().hex, "name": name,
                      "environment": {"schema_version": 1, "machines": [
                          {"schema_version": 1, "name": f"machine-{index}", "profile": profile,
                           "target": {"os": "linux", "arch": "aarch64", "image": entry["image"], "digest": entry["digest"]},
                           "resources": {"cpus": 2, "memory_mb": 4096 if profile == "developer" else 1024}}
                          for index in range(count)]}}
        document(project / "vz.json", definition)
        return project

    def status(self, project, selector):
        raw, stderr, _ = self.command("status", [self.cli, "--json", "status", "--environment", selector], cwd=project)
        require(not stderr, "status emitted stderr")
        value = json.loads(raw)
        require(len(value["environments"]) == 1 and not value["definition_drift"], "exact drift-free status required")
        environment = value["environments"][0]
        environment["project_id"] = value["project_id"]
        return environment

    def up(self, project, selector):
        request = "up-" + uuid.uuid4().hex
        expected_project = bounded_json(project / "vz.json")["project_id"]
        self.unresolved_up.add(request)
        raw, stderr, code = self.command("public-up", [self.cli, "--json", "up", "--environment", selector,
                     "--timeout", "600", "--request-id", request, "--idempotency-key", request], cwd=project, timeout=660, success=False)
        admitted, completion = None, None
        for line in raw.splitlines():
            progress = json.loads(line).get("progress")
            if progress is None:
                continue
            terminal = progress.get("completion")
            for admission in (progress.get("admission"), terminal.get("admission") if terminal else None):
                if admission is None:
                    continue
                environment_id = admission.get("environment_id")
                require(admission.get("schema_version") == 1 and admission.get("project_id") == expected_project and
                        admission.get("request_id") == request and admission.get("idempotency_key") == request and
                        isinstance(environment_id, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", environment_id),
                        "Up admission does not authenticate this exact project/request")
                require(admitted is None or admitted == environment_id, "Up stream changed admitted Environment identity")
                admitted = environment_id
                owned = (project, admitted)
                if owned not in self.cleanup_targets:
                    self.cleanup_targets.append(owned)
            if terminal is not None:
                require(completion is None, "duplicate Up terminal receipt")
                completion = terminal
        if admitted is not None:
            self.unresolved_up.remove(request)
        require(admitted is not None, "Up omitted authenticated admission; cleanup by name is forbidden")
        require(code == 0 and not stderr and completion is not None and completion.get("error") is None and
                completion.get("operation", {}).get("status") == "succeeded", "Up lacks positive aggregate receipt")
        return self.status(project, admitted)

    def inspect(self, environment):
        descriptors = []
        for machine in environment["machines"]:
            descriptor = managed_context_descriptor(environment, machine, self.runtime)
            endpoint = Path(descriptor["endpoint"][7:])
            require(endpoint.is_relative_to(self.runtime) and stat.S_ISSOCK(endpoint.lstat().st_mode), "foreign/missing Machine socket")
            raw, _, _ = self.docker("context-inspect", descriptor, ["context", "inspect", descriptor["name"]])
            item = json.loads(raw)[0]
            require(item["Name"] == descriptor["name"] and item["Endpoints"]["docker"]["Host"] == descriptor["endpoint"], "context rerouted")
            raw, _, _ = self.docker("engine-info", descriptor, ["info", "--format", "{{.ID}}"])
            require(raw.decode().strip() == descriptor["engine_id"], "Engine identity drift")
            descriptors.append(descriptor)
            if descriptor not in self.descriptors:
                self.descriptors.append(descriptor)
        return descriptors

    def stop(self, project, selector):
        request = "stop-" + uuid.uuid4().hex
        raw, _, _ = self.command("public-stop", [self.cli, "--json", "stop", "--environment", selector,
                     "--timeout", "120", "--request-id", request, "--idempotency-key", request], cwd=project, timeout=150)
        terminal = json.loads(raw.splitlines()[-1])
        require(terminal["terminal"] is True and terminal["operation"]["status"] == "succeeded", "positive Stop receipt required")
        stopped = self.status(project, selector)
        require(stopped["state"] == "stopped" and all(m["state"] == "stopped" for m in stopped["machines"]), "not stopped")
        for machine in stopped["machines"]:
            if machine.get("docker_context"):
                descriptor = machine["docker_context"]
                require(not os.path.lexists(descriptor["endpoint"][7:]), "stopped Machine socket still present")
                _, _, code = self.docker("stopped-engine-rejected", descriptor, ["info", "--format", "{{.ID}}"], timeout=15, success=False)
                require(code != 0, "stopped context reached an Engine")
        return stopped

    def workload(self, descriptor):
        token = "vzhost-" + uuid.uuid4().hex[:24]
        label = "dev.vz.installed-proof=" + token
        archive = self.prefix / "linux/developer/developer-probe-rootfs.tar"
        for tag in (token + ":rootfs", token + ":built"):
            raw, _, _ = self.docker("host-tag-absent", descriptor, ["image", "ls", "--quiet", "--filter", "reference=" + tag])
            require(not raw.strip(), "fresh workload image tag already exists")
        for name in (token, token + "-built"):
            raw, _, _ = self.docker("host-name-absent", descriptor, ["container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])
            require(not raw.strip(), "fresh workload container name already exists")
        with archive.open("rb") as stream:
            raw, _, _ = self.docker("host-import", descriptor, ["image", "import", "--change", "LABEL " + label, "-", token + ":rootfs"], stdin=stream)
        imported = image_id(raw)
        raw, _, _ = self.docker("host-run", descriptor, ["run", "--pull", "never", "--network", "none", "--detach", "--label", label,
                          "--name", token, imported, "/bin/sleep", "300"])
        cid = raw.decode().strip()
        require(re.fullmatch(r"[0-9a-f]{64}", cid), "container ID required")
        raw, _, _ = self.docker("host-exec", descriptor, ["exec", cid, "/bin/cat", "/etc/vz-developer-probe"])
        require(raw == MARKER, "host Docker exec marker differs")
        inspected, _, _ = self.docker("host-owned-inspect", descriptor, ["container", "inspect", cid])
        item = json.loads(inspected)[0]
        require(item["Id"] == cid and item["Config"]["Labels"]["dev.vz.installed-proof"] == token and item["Image"] == imported,
                "Engine container ownership differs before removal")
        self.docker("host-rm", descriptor, ["container", "rm", "--force", cid])
        directory = private(self.root / token)
        compose = directory / "compose.json"
        document(compose, {"services": {"probe": {"image": imported, "pull_policy": "never", "network_mode": "none",
                         "command": ["/bin/sleep", "300"], "labels": {"dev.vz.installed-proof": token}}}})
        compose_args = ["compose", "--project-name", token, "--file", str(compose)]
        self.docker("host-compose-up", descriptor, [*compose_args, "up", "--detach", "--no-build", "--pull", "never"])
        raw, _, _ = self.docker("host-compose-exec", descriptor, [*compose_args, "exec", "--no-TTY", "probe", "/bin/cat", "/etc/vz-developer-probe"])
        require(raw == MARKER, "host Compose exec marker differs")
        # No project-wide down: enumerate this exact label and remove each
        # positively inspected container ID, leaving unknown siblings alone.
        raw, _, _ = self.docker("host-compose-ids", descriptor, [*compose_args, "ps", "--all", "--quiet"])
        ids = raw.decode().split()
        require(len(ids) == 1 and re.fullmatch(r"[0-9a-f]{64}", ids[0]), "exact single Compose service expected")
        inspected, _, _ = self.docker("host-compose-inspect", descriptor, ["container", "inspect", ids[0]])
        item = json.loads(inspected)[0]
        require(item["Config"]["Labels"]["dev.vz.installed-proof"] == token and item["Image"] == imported, "foreign Compose container")
        self.docker("host-compose-rm", descriptor, ["container", "rm", "--force", ids[0]])
        build = private(directory / "build")
        write(build / "Dockerfile", ("FROM scratch\nCOPY payload.txt /payload.txt\nLABEL " + label + "\n").encode())
        write(build / "payload.txt", PAYLOAD)
        iid = directory / "image.id"
        self.docker("host-buildx", descriptor, ["buildx", "build", "--builder", descriptor["name"], "--network", "none",
                    "--progress", "plain", "--load", "--iidfile", str(iid), "--tag", token + ":built", str(build)], timeout=120)
        built = image_id(iid.read_bytes())
        raw, _, _ = self.docker("host-built-inspect", descriptor, ["image", "inspect", token + ":built", "--format", "{{.Id}}"])
        require(image_id(raw) == built, "Buildx loaded image identity differs")
        raw, _, _ = self.docker("host-built-create", descriptor, ["container", "create", "--network", "none", "--label", label,
                               "--name", token + "-built", built, "/not-executed"])
        built_cid = raw.decode().strip()
        require(re.fullmatch(r"[0-9a-f]{64}", built_cid), "built container ID required")
        raw, _, _ = self.docker("host-built-copy", descriptor, ["container", "cp", built_cid + ":/payload.txt", "-"])
        with tarfile.open(fileobj=io.BytesIO(raw)) as archive_stream:
            members = archive_stream.getmembers()
            require(len(members) == 1 and members[0].isfile() and members[0].size == len(PAYLOAD), "COPY payload tar inventory differs")
            require(archive_stream.extractfile(members[0]).read() == PAYLOAD, "Buildx COPY payload bytes differ")
        self.docker("host-built-rm", descriptor, ["container", "rm", built_cid])
        for tag, expected in ((token + ":built", built), (token + ":rootfs", imported)):
            raw, _, _ = self.docker("host-owned-image-inspect", descriptor, ["image", "inspect", tag])
            item = json.loads(raw)[0]
            require(item["Id"] == expected and item["Config"]["Labels"]["dev.vz.installed-proof"] == token, "image tag ownership differs before removal")
        self.docker("host-image-rm", descriptor, ["image", "rm", token + ":built", token + ":rootfs"])
        return {"context": descriptor["name"], "engine_id": descriptor["engine_id"], "imported_image_id": imported,
                "built_image_id": built, "marker_sha256": hashlib.sha256(MARKER).hexdigest(), "payload_sha256": hashlib.sha256(PAYLOAD).hexdigest()}

    def daemon_fingerprint(self):
        pidfile = self.socket.with_suffix(".pid")
        require(not pidfile.is_symlink() and pidfile.stat().st_size <= 16, "bounded owned daemon PID file required")
        text = pidfile.read_text()
        require(re.fullmatch(r"[0-9]+", text), "invalid daemon PID")
        pid = int(text)
        require(pid > 1, "unsafe daemon PID")
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        function = library.proc_pidpath
        function.argtypes, function.restype = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32], ctypes.c_int
        buffer = ctypes.create_string_buffer(4096)
        require(function(pid, buffer, len(buffer)) > 0 and buffer.value.decode() == str(self.daemon), "PID is not exact installed daemon")
        raw, _, _ = self.command("daemon-identity", ["/bin/ps", "-p", str(pid), "-o", "uid=", "-o", "lstart=", "-o", "command="])
        identity = raw.decode().strip()
        require(identity.split()[0] == str(os.geteuid()) and str(self.socket) in identity and str(self.database) in identity and
                str(self.prefix / "machine-target-catalog.json") in identity, "daemon process does not own isolated invocation")
        return {"pid": pid, "process": identity, "executable_sha256": digest(self.daemon)}

    def cleanup(self):
        require(not any(row["effects_uncertain"] for row in self.record.receipts), "uncertain dispatch: automated cleanup withheld")
        require(not self.unresolved_up, "Up admission unresolved: no Stop by name or daemon termination")
        for target in reversed(self.cleanup_targets):
            self.stop(*target)
        if self.daemon_identity is None and self.socket.with_suffix(".pid").exists():
            self.daemon_identity = self.daemon_fingerprint()
        if self.daemon_identity:
            current = self.daemon_fingerprint()
            require(current == self.daemon_identity, "daemon identity changed; refusing signal")
            document(self.evidence / "daemon-term-intent.json", current)
            os.kill(current["pid"], signal.SIGTERM)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    os.kill(current["pid"], 0)
                    alive = True
                except ProcessLookupError:
                    alive = False
                if not alive and not os.path.lexists(self.socket) and not self.socket.with_suffix(".pid").exists():
                    break
                time.sleep(0.1)
            require(not os.path.lexists(self.socket) and not self.socket.with_suffix(".pid").exists(), "daemon graceful cleanup not observed")
            raw, _, code = self.command("daemon-gone", ["/bin/ps", "-p", str(current["pid"]), "-o", "pid="], success=False)
            require(code != 0 and not raw.strip(), "daemon PID still exists; no forced kill")
            log = self.socket.with_suffix(".log")
            require(b"runtime daemon shutting down" in read_private_regular(log, 32 * 1024 * 1024),
                    "positive graceful daemon shutdown log required")
        require(digest(self.config / "config.json") == self.default_digest, "isolated default Docker config changed")
        require({str(path): snapshot_config(path) for path in self.baseline_paths} == self.baseline, "daily Docker default/config changed")
        return {"positive_stop_all": True, "daemon_graceful_shutdown_observed": self.daemon_identity is not None,
                "daily_default_unchanged": True, "isolated_default_unchanged": True}

    def scenario(self):
        project = self.project("developer", "developer", 2)
        primary = self.up(project, "primary")
        self.daemon_identity = self.daemon_fingerprint()
        primary_contexts = self.inspect(primary)
        sibling = self.up(project, "neighbor")
        exact_developer_topology(primary, sibling)
        sibling_contexts = self.inspect(sibling)
        all_contexts = primary_contexts + sibling_contexts
        for field in ("name", "endpoint", "config_dir", "engine_id"):
            require(len({item[field] for item in all_contexts}) == len(all_contexts), "Machines share " + field)
        workloads = [self.workload(descriptor) for descriptor in primary_contexts]
        self.stop(project, primary["environment_id"])
        require(self.inspect(self.status(project, sibling["environment_id"])) == sibling_contexts, "neighbor changed when primary stopped")
        workloads.append(self.workload(sibling_contexts[0]))
        restarted = self.up(project, primary["environment_id"])
        restart_contexts = self.inspect(restarted)
        require(restarted["environment_id"] == primary["environment_id"], "Environment identity replaced")
        require([m["machine_id"] for m in restarted["machines"]] == [m["machine_id"] for m in primary["machines"]], "Machine identities replaced")
        for before, after in zip(primary_contexts, restart_contexts):
            require(all(before[field] == after[field] for field in ("name", "endpoint", "config_dir", "owner")), "stable context identity changed")
            require(after["incarnation_generation"] > before["incarnation_generation"], "incarnation did not advance")
        require(self.inspect(self.status(project, sibling["environment_id"])) == sibling_contexts, "neighbor changed during restart")
        self.stop(project, primary["environment_id"])
        self.stop(project, sibling["environment_id"])
        hardened = self.project("hardened", "hardened", 1)
        restricted = self.up(hardened, "restricted")
        require(restricted["state"] == "ready" and all(not m.get("docker_context") and
                not {"docker_engine", "compose", "buildx"}.intersection(m["negotiated_capabilities"]["capabilities"]) for m in restricted["machines"]), "Hardened acquired Docker")
        raw, stderr, _ = self.command("hardened-exec", [self.cli, "exec", "--environment", restricted["environment_id"],
                     "--machine", "machine-0", "--no-stdin", "--timeout", "30", "--", "/bin/echo", "hardened-installed-proof"], cwd=hardened)
        require(raw == b"hardened-installed-proof\n" and not stderr, "Hardened raw execution failed")
        return {"primary_before": primary, "primary_after": restarted, "neighbor": sibling, "hardened": restricted, "host_workloads": workloads}


def is_private_client_path(path, root):
    """Client credentials/plugin state are never public runtime receipts."""
    return any(part == "docker-client" or part.startswith(".docker-client.pending-")
               for part in path.relative_to(root).parts)


def collect_runtime_receipts(harness):
    from docker_host_driver import contains_canary, regular
    retained = private(harness.evidence / "runtime-receipts")
    for path in harness.runtime.rglob("*"):
        # Client storage now lives under the runtime store. Its mutable auths
        # and plugin state are private inputs, not publishable runtime receipts.
        # Do not even read/hash them into evidence; public descriptors and
        # separate ownership admission already bind the selected connection.
        if is_private_client_path(path, harness.runtime):
            continue
        if path.suffix not in (".log", ".json", ".stdout", ".stderr"):
            continue
        require(not path.is_symlink() and path.is_file() and path.stat().st_size <= 32 * 1024 * 1024, "unbounded/redirected runtime receipt")
        content = regular(path, 32 * 1024 * 1024)
        require(not contains_canary((content,), getattr(harness, "sensitive_canaries", [])),
                "private canary in runtime receipt; source retained outside evidence")
        destination = retained / path.relative_to(harness.runtime)
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        write(destination, content)
def checksum_evidence(harness):
    rows = []
    for path in sorted(harness.evidence.rglob("*")):
        require(not path.is_symlink(), "symlink in evidence")
        if path.is_file():
            rows.append(f"{digest(path)}  {path.relative_to(harness.evidence)}\n")
    write(harness.evidence / "checksums.sha256", "".join(rows).encode())


def run(info):
    os.umask(0o077)
    harness = Harness(info)
    document(harness.evidence / "inputs.json", info)
    outcome = {"schema_version": 1, "scope": SCOPE, "outcome": "failed", "error": None, "cleanup_errors": [],
               "docker_parity_certified": False, "aggregate_release_certified": False, "test_case_retries": 0,
               "retained_root": str(harness.root)}
    try:
        harness.stage()
        outcome["scenario"] = harness.scenario()
        for path, expected in (info["inputs"] | harness.staged_inputs).items():
            require(digest(Path(path)) == expected, "selected input changed during physical run")
    except BaseException as error:
        outcome["error"] = f"{type(error).__name__}: {error}"
    finally:
        try:
            outcome["cleanup"] = harness.cleanup()
        except BaseException as error:
            outcome["cleanup_errors"].append(f"{type(error).__name__}: {error}")
        try:
            collect_runtime_receipts(harness)
        except BaseException as error:
            outcome["cleanup_errors"].append(f"runtime evidence collection: {type(error).__name__}: {error}")
        if outcome["error"] is None and not outcome["cleanup_errors"]:
            outcome["outcome"] = "passed_dev_installed_operational_startup"
        document(harness.evidence / "result.json", outcome)
        checksum_evidence(harness)
    print(json.dumps(outcome), flush=True)
    return 0 if outcome["outcome"].startswith("passed_") else 1


def main(argv):
    try:
        return run(preflight(arguments(argv)))
    except (Exception, KeyboardInterrupt) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
