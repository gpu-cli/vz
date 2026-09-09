"""Synthetic adversarial evidence only: no Docker, VM or guest execution."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

import docker_host_driver as driver
import linux_docker_compose_evidence as evidence


FIXTURE = Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker"
FOREIGN_ALIAS = "vz04-0123456789abcdef01234567" + evidence.DNS_SUFFIX


def data(value):
    return json.dumps(value, sort_keys=True).encode()


class SyntheticEngine:
    """In-memory response generator, never evidence of real Docker behavior."""
    def __init__(self, inputs):
        self.inputs = inputs
        self.owner = inputs.owner
        self.projects, self.markers = {}, {}
        self.generations = {}
        self.external = {kind: [] for kind in ("container", "network", "volume")}

    def identity(self, name):
        return hashlib.sha256(name.encode()).hexdigest()

    def install(self, project, failure=False):
        if project in self.projects:
            return
        networks = [{"Id": self.identity(project + name), "Name": project + "_" + name,
                     "Containers": {}, "Labels": self.labels(project)} for name in ("frontend", "backend", "isolated")]
        self.projects[project] = {"container": [], "network": networks,
                                  "volume": [{"Name": project + "_state", "Labels": self.labels(project)}]}
        for role in ("db", "api", "worker", "isolated", *(["failure"] if failure else [])):
            self.projects[project]["container"].append(self.container(project, role))

    def labels(self, project):
        return {"com.docker.compose.project": project, "dev.vz.fixture-owner": self.owner}

    def compose_config(self):
        """What Compose resolves from the fixture: the declared services, edges and owner."""
        declared = json.loads(FIXTURE.joinpath("compose/compose.json").read_bytes())["services"]
        services = {}
        # Real Compose resolves only the services no profile gates.
        for name, spec in ((n, x) for n, x in declared.items() if not x.get("profiles")):
            services[name] = {"image": self.inputs.raw["images"]["compose"]["id"], "command": spec["command"], "pull_policy": spec["pull_policy"],
                              "environment": {"FIXTURE_OWNER": self.owner},
                              "networks": {network: None for network in spec["networks"]},
                              "volumes": [{"source": item.split(":")[0], "target": item.split(":")[1]}
                                          for item in spec.get("volumes", [])]}
            if spec.get("healthcheck"):
                services[name]["healthcheck"] = dict(spec["healthcheck"])
            if spec.get("depends_on"):
                services[name]["depends_on"] = dict(spec["depends_on"])
        return data({"services": services})

    def compose_logs(self, project):
        """Interleaved services with Compose's grow-as-seen prefix padding; never a real stream."""
        items = {item["Config"]["Labels"]["com.docker.compose.service"]: item for item in self.projects[project]["container"]}
        order = [("db", "listening"), ("api", "dependency-healthy"), ("worker", "dependency-healthy"),
                 ("isolated", "listening"), ("api", "listening"), ("worker", "listening")]
        width, lines = 0, []
        for role, event in order:
            name = items[role]["Name"][len(project) + 2:]
            width = max(width, len(name))
            lines.append(f"{name:<{width}} | vz04|{role}|{self.owner}|{event}\n".encode())
        return b"".join(lines)

    # Addressing is per network, not per container: two services sharing one
    # network must hold distinct addresses, or a name that resolved to the wrong
    # container would be indistinguishable from one that resolved to the right
    # one. `generation` separates a replacement container from the one it
    # replaced, in identity and in address.
    NETWORKS = {"frontend": 0, "backend": 1, "isolated": 2}
    HOSTS = {"db": 20, "api": 30, "worker": 40, "isolated": 50, "failure": 60}

    def container(self, project, role, number=1, generation=0):
        suffix = "" if not generation else "#" + str(generation)
        identity = self.identity(project + role + str(number) + suffix)
        names = {"db": ("backend",), "api": ("frontend", "backend"), "worker": ("frontend",),
                 "isolated": ("isolated",), "failure": ("frontend",)}[role]
        host = self.HOSTS[role] + number - 1 + generation
        return {"Id": identity, "Name": f"/{project}-{role}-{number}", "Image": self.inputs.raw["images"]["compose"]["id"],
                "Config": {"Hostname": identity[:12], "Labels": self.labels(project) | {"com.docker.compose.service": role}},
                "State": {"Status": "created", "Running": False, "ExitCode": 0,
                          **({} if role == "failure" else {"Health": {"Status": "starting"}})},
                "Mounts": [{"Destination": "/data", "Type": "volume", "Name": project + "_state", "RW": True}] if role == "db" else [],
                "NetworkSettings": {"Networks": {project + "_" + name: {"IPAddress": f"172.{18 + self.NETWORKS[name]}.0.{host}",
                                                                         "NetworkID": self.identity(project + name)}
                                                  for name in names}}}

    def resolve(self, item, name):
        """Docker's embedded DNS as a property, never a recorded answer.

        A name answers only when a *running* container of the asking container's
        own project shares a network with it and carries that name, either as
        its Compose service alias or as its exact container name. A removed
        container therefore stops answering, and a name no container in this
        Engine holds never answers at all.
        """
        project = item["Config"]["Labels"]["com.docker.compose.project"]
        asking = set(item["NetworkSettings"]["Networks"])
        for other in self.projects.get(project, {}).get("container", []):
            if not other["State"].get("Running"):
                continue
            aliases = {other["Config"]["Labels"]["com.docker.compose.service"], other["Name"].lstrip("/")}
            if name not in aliases:
                continue
            shared = sorted(asking & set(other["NetworkSettings"]["Networks"]))
            if shared:
                return {"schema_version": 1, "name": name, "outcome": "resolved", "errno": None, "exception": None,
                        "addresses": [other["NetworkSettings"]["Networks"][shared[0]]["IPAddress"]]}
        return {"schema_version": 1, "name": name, "outcome": "unresolved", "addresses": [],
                "errno": -2, "exception": "gaierror"}

    def __call__(self, argv, **_kwargs):
        args = argv[5:]
        code, stdout, stderr = 0, b"", b""
        scope = self.inputs.scope
        if args[:2] == ["context", "inspect"]:
            stdout = data([{"Name": scope["docker_context"], "Endpoints": {"docker": {"Host": scope["docker_endpoint"], "SkipTLSVerify": False}}}])
        elif args[0] == "info":
            stdout = data({"ID": scope["engine_id"], "OSType": "linux", "Architecture": "arm64", "DefaultRuntime": "youki",
                           "Runtimes": {"youki": {"path": "/mnt/linux-bin/youki"}}})
        elif args[:2] == ["image", "inspect"]:
            pin = next(pin for pin in self.inputs.raw["images"].values() if pin["reference"] == args[2])
            stdout = data([{"Id": pin["id"], "Os": "linux", "Architecture": "arm64", "RepoDigests": [pin["reference"]]}])
        elif args[0] in {"container", "network", "volume"}:
            kind = args[0]
            values = [item for inventory in self.projects.values() for item in inventory[kind]] + self.external[kind]
            key = "Name" if kind == "volume" else "Id"
            if args[1] == "create":
                # An external resource: no Compose project label, so no owned
                # inventory ever contains it and every down must leave it alone.
                item = {"Name": args[-1], "Id": self.identity(args[-1]),
                        "Labels": {"dev.vz.fixture-owner": self.owner}}
                self.external[kind].append(item)
                stdout = (item[key] + "\n").encode()
            elif args[1] == "rm":
                self.external[kind] = [item for item in self.external[kind] if item[key] not in args[2:]]
                stdout = ("\n".join(args[2:]) + "\n").encode()
            elif args[1] == "ls":
                if "--filter" in args:
                    project = args[-1].split("=", 2)[2]
                    values = self.projects.get(project, {}).get(kind, [])
                stdout = ("\n".join(item["Name"] if "--format" in args else item[key] for item in values) + ("\n" if values else "")).encode()
            else:
                selected = [item for item in values if item[key] in args[2:]]
                if len(selected) != len(args[2:]):
                    code, stdout, stderr = 1, b"[]\n", ("Error: No such container: " + args[2] + "\n").encode()
                else:
                    stdout = data(selected)
        elif args[0] == "compose":
            project = args[2]
            offset = 7
            blocked = args[offset:offset + 1] == ["--file"]
            if blocked:
                offset += 2
            tail = args[offset:]
            if tail[:2] == ["--profile", "failure"]:
                tail = tail[2:]
            action = tail[0]
            if action == "down":
                self.projects.pop(project, None)
            elif action == "rm":
                assert tail[:3] == ["rm", "--stop", "--force"], tail
                for role in tail[3:]:
                    items = self.projects[project]["container"]
                    self.projects[project]["container"] = [
                        x for x in items if x["Config"]["Labels"]["com.docker.compose.service"] != role]
                    self.generations[(project, role)] = self.generations.get((project, role), 0) + 1
            elif action == "exec":
                code = 37
                stdout, stderr = (f"vz04|api|{self.owner}|exec-{stream}\n".encode() for stream in ("stdout", "stderr"))
            elif action == "logs":
                assert tail == ["logs", "--no-color"], tail
                stdout = self.compose_logs(project)
            elif action == "config":
                assert tail == ["config", "--format", "json"], tail
                stdout = self.compose_config()
            else:
                self.install(project, failure="--exit-code-from" in tail)
                # Compose recreates a service whose container is gone, under the
                # same name and with a new identity and address.
                present = {x["Config"]["Labels"]["com.docker.compose.service"]
                           for x in self.projects[project]["container"]}
                for role in ("db", "api", "worker", "isolated"):
                    if role not in present:
                        self.projects[project]["container"].append(
                            self.container(project, role, generation=self.generations[(project, role)]))
                items = self.projects[project]["container"]
                if "--scale" in tail:
                    count = int(tail[tail.index("--scale") + 1].split("=")[1])
                    items = [x for x in items if x["Config"]["Labels"]["com.docker.compose.service"] != "worker"]
                    items += [self.container(project, "worker", number) for number in range(1, count + 1)]
                    self.projects[project]["container"] = items
                for item in items:
                    role = item["Config"]["Labels"]["com.docker.compose.service"]
                    if action == "create":
                        continue
                    running = action == "up" and not "--exit-code-from" in tail and not (blocked and role in {"api", "worker"})
                    state = "running" if running else "created" if blocked and role in {"api", "worker"} else "exited"
                    item["State"] = {"Status": state, "Running": running, "ExitCode": 37 if role == "failure" else 0,
                                     "StartedAt": "2026-09-08T03:19:2%d.000000000Z" % (5 if role == "failure" else 3)}
                    if role != "failure":
                        # The fixture declares no healthcheck for the failure job,
                        # so the Engine reports no health state for it at all.
                        item["State"]["Health"] = {
                            "Status": "unhealthy" if blocked and role == "db" else "healthy",
                            "FailingStreak": 1 if blocked and role == "db" else 0,
                            "Log": [{"ExitCode": 1 if blocked and role == "db" else 0,
                                     "Start": "2026-09-08T03:19:20.000000Z", "End": "2026-09-08T03:19:20.100000Z"}]}
                    if state == "created":
                        item["State"].update(Paused=False, Restarting=False, Pid=0,
                            StartedAt="0001-01-01T00:00:00Z", FinishedAt="0001-01-01T00:00:00Z")
                        declared = item["NetworkSettings"]["Networks"]
                        item["HostConfig"] = {"NetworkMode": next(iter(declared))}
                        for endpoint in declared.values():
                            endpoint.update({key: "" for key in ("NetworkID", "EndpointID", "Gateway", "IPAddress",
                                "MacAddress", "IPv6Gateway", "GlobalIPv6Address")})
                            endpoint.update(IPPrefixLen=0, GlobalIPv6PrefixLen=0)
                code = 1 if blocked else 37 if "--exit-code-from" in tail else 0
        elif args[0] == "exec":
            item = next(x for inventory in self.projects.values() for x in inventory["container"] if x["Id"] == args[1])
            role = item["Config"]["Labels"]["com.docker.compose.service"]
            tail = args[2:]
            if tail[1] == "-c" and tail[2] == evidence.RESOLVE:
                stdout = data(self.resolve(item, tail[3]))
            elif tail[1] == "-c":
                if tail[2] == evidence.WRITE:
                    self.markers[(item["Id"], tail[3])] = tail[4].encode()
                elif tail[3].endswith("/sentinel.txt"):
                    # The service writes its own persistence sentinel at startup;
                    # only the host-written marker beside it is a recorded write.
                    stdout = f"vz04|db|{self.owner}|persisted\n".encode()
                else:
                    stdout = self.markers[(item["Id"], tail[3])]
            elif tail[2] == "transport":
                dns = not urlsplit(tail[3]).hostname[0].isdigit()
                stdout = data({"schema_version": 1, "url": tail[3], "outcome": "dns_failure" if dns else "timeout",
                               "status": None, "errno": -2 if dns else None, "exception": "gaierror" if dns else "TimeoutError"})
            elif tail[3].endswith("/identity"):
                stdout = data({"owner": self.owner, "role": role, "hostname": item["Config"]["Hostname"]})
            else:
                stdout = (f"vz04|db|{self.owner}|persisted\n" if tail[3].endswith("/value") else f"vz04|{role}|{self.owner}|ready\n").encode()
        elif args[0] == "logs":
            stdout = f"vz04|failure|{self.owner}|exit-37\n".encode()
        elif args[0] == "events":
            project = args[6].split("=", 2)[2]
            items = self.projects[project]["container"]
            blocked = project.endswith("-blocked")
            actions = [(x, "create") for x in items]
            for role in ("db", "api", "worker", "isolated"):
                item = next(x for x in items if x["Config"]["Labels"]["com.docker.compose.service"] == role)
                if not blocked or role in {"db", "isolated"}:
                    actions += [(item, "start"), (item, "health_status: unhealthy" if blocked and role == "db" else "health_status: healthy")]
            stdout = b"\n".join(data({"Type": "container", "Actor": {"ID": item["Id"], "Attributes": self.labels(project)},
                                      "Action": action, "timeNano": int(args[2]) * 10**9 + index + 1})
                                  for index, (item, action) in enumerate(actions)) + b"\n"
        else:
            raise AssertionError(f"unexpected synthetic command: {args}")
        return subprocess.CompletedProcess(argv, code, stdout, stderr)


class TamperedEngine(SyntheticEngine):
    """The synthetic Engine with exactly one DNS property broken.

    Each mode is a way `docker.network.dns` could be false while every other
    assertion in the subset still passed, so a Driver that accepts one of these
    is a Driver whose DNS recipe proves nothing.
    """

    def __init__(self, inputs, mode):
        super().__init__(inputs)
        self.mode, self.removal_seen = mode, False

    def __call__(self, argv, **kwargs):
        args = argv[5:]
        removal = args[0] == "compose" and "rm" in args
        if removal and self.mode == "no-removal":
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        result = super().__call__(argv, **kwargs)
        if removal:
            self.removal_seen = True
        return result

    def resolve(self, item, name):
        row = super().resolve(item, name)
        own = self.owner + evidence.DNS_SUFFIX
        answered = {"schema_version": 1, "name": name, "outcome": "resolved", "errno": None, "exception": None}
        if self.mode == "foreign-resolves" and name == FOREIGN_ALIAS:
            return answered | {"addresses": ["172.18.0.99"]}
        if self.mode == "stale-record" and self.removal_seen and name in ("api", own):
            return answered | {"addresses": ["172.18.0.30"]}
        if self.mode == "dead-resolver" and self.removal_seen:
            return {"schema_version": 1, "name": name, "outcome": "unresolved", "addresses": [],
                    "errno": -2, "exception": "gaierror"}
        if self.mode == "connect-deadline" and row["outcome"] == "unresolved":
            return {"schema_version": 1, "name": name, "outcome": "deadline", "addresses": [],
                    "errno": None, "exception": "TimeoutError"}
        if self.mode == "unclassified-failure" and row["outcome"] == "unresolved":
            # No address, but not "no such name" either: something else in the
            # probe went wrong, which is not evidence that the name is absent.
            return {"schema_version": 1, "name": name, "outcome": "unresolved", "addresses": [],
                    "errno": None, "exception": "OSError"}
        if self.mode == "wrong-address" and name == "api" and row["outcome"] == "resolved":
            return answered | {"addresses": ["172.18.0.40"]}
        return row


def execute_driver(fixture, raw_inputs, output, engine=None):
    """One Driver run against an in-memory Engine; never real Docker."""
    inputs = driver.Inputs(raw_inputs, suite="compose")
    original_stat = Path.stat
    socket_path = raw_inputs["scope"]["docker_endpoint"][7:]

    def fake_stat(path, *args, **kwargs):
        if str(path) == socket_path:
            return types.SimpleNamespace(st_mode=stat.S_IFSOCK)
        return original_stat(path, *args, **kwargs)

    def persist(_recorder, path, value, **_kwargs):
        # Synthetic fixtures make no fsync/durability claim.
        path.write_bytes(data(value))

    with patch.object(driver.sys, "platform", "darwin"), \
            patch.object(driver.os, "uname", return_value=types.SimpleNamespace(machine="arm64")), \
            patch.object(Path, "stat", fake_stat), \
            patch.object(driver, "execute", side_effect=engine or SyntheticEngine(inputs)), \
            patch.object(driver.Recorder, "persist", persist):
        return driver.Driver(inputs, fixture, output).run("compose")


class NetworkBindingTests(unittest.TestCase):
    def inventory(self):
        endpoint = {key: "" for key in ("NetworkID", "EndpointID", "Gateway", "IPAddress", "MacAddress",
                                       "IPv6Gateway", "GlobalIPv6Address")}
        endpoint.update(IPPrefixLen=0, GlobalIPv6PrefixLen=0)
        return {"network": [{"Id": "a" * 64, "Name": "owned_frontend", "Containers": {}},
                            {"Id": "b" * 64, "Name": "owned_backend", "Containers": {}}],
                "container": [{"Id": "c" * 64, "State": {"Status": "created", "Running": False,
                    "Paused": False, "Restarting": False, "Pid": 0, "StartedAt": "0001-01-01T00:00:00Z",
                    "FinishedAt": "0001-01-01T00:00:00Z"}, "HostConfig": {"NetworkMode": "owned_frontend"},
                    "NetworkSettings": {"Networks": {"owned_frontend": endpoint}}}]}

    def test_exact_never_started_declaration_and_attached_identity_controls(self):
        inventory = self.inventory()
        evidence.network_bindings(inventory)
        container = inventory["container"][0]
        container["State"] = {"Status": "running", "Running": True}
        container["NetworkSettings"]["Networks"]["owned_frontend"]["NetworkID"] = "a" * 64
        evidence.network_bindings(inventory)

    def test_empty_id_rejects_any_missing_or_contradictory_start_authority(self):
        for key, value in (("Status", "exited"), ("Running", True), ("Paused", True), ("Restarting", True),
                           ("Pid", 1), ("Pid", False), ("StartedAt", "2026-09-05T00:00:00Z"),
                           ("FinishedAt", "2026-09-05T00:00:00Z")):
            for missing in (False, True):
                with self.subTest(field=key, missing=missing):
                    inventory = self.inventory()
                    state = inventory["container"][0]["State"]
                    if missing:
                        state.pop(key)
                    else:
                        state[key] = value
                    with self.assertRaises(evidence.Invalid):
                        evidence.network_bindings(inventory)

    def test_empty_id_rejects_endpoint_state_or_missing_fields(self):
        for key in ("EndpointID", "Gateway", "IPAddress", "MacAddress", "IPv6Gateway", "GlobalIPv6Address",
                    "IPPrefixLen", "GlobalIPv6PrefixLen"):
            for missing in (False, True):
                with self.subTest(field=key, missing=missing):
                    inventory = self.inventory()
                    endpoint = inventory["container"][0]["NetworkSettings"]["Networks"]["owned_frontend"]
                    if missing:
                        endpoint.pop(key)
                    else:
                        endpoint[key] = 1 if "PrefixLen" in key else "present"
                    with self.assertRaises(evidence.Invalid):
                        evidence.network_bindings(inventory)

    def test_empty_id_rejects_membership_in_any_owned_network_or_missing_inventory(self):
        for index in range(2):
            for missing in (False, True):
                with self.subTest(network=index, missing=missing):
                    inventory = self.inventory()
                    network = inventory["network"][index]
                    if missing:
                        network.pop("Containers")
                    else:
                        network["Containers"]["c" * 64] = {"EndpointID": "present"}
                    with self.assertRaises(evidence.Invalid):
                        evidence.network_bindings(inventory)

    def test_declared_name_and_attached_id_cannot_borrow_other_owned_network(self):
        for kind in ("foreign-name", "wrong-owned-id", "foreign-mode", "duplicate-name", "duplicate-id"):
            with self.subTest(kind=kind):
                inventory = self.inventory()
                container = inventory["container"][0]
                declared = container["NetworkSettings"]["Networks"]
                if kind == "foreign-name":
                    declared["foreign"] = declared.pop("owned_frontend")
                elif kind == "wrong-owned-id":
                    declared["owned_frontend"]["NetworkID"] = "b" * 64
                elif kind == "foreign-mode":
                    container["HostConfig"]["NetworkMode"] = "host"
                elif kind == "duplicate-name":
                    inventory["network"][1]["Name"] = "owned_frontend"
                else:
                    inventory["network"][1]["Id"] = "a" * 64
                with self.assertRaises(evidence.Invalid):
                    evidence.network_bindings(inventory)

    def test_empty_identity_and_membership_evidence_require_exact_types(self):
        for value in (None, False, 0):
            with self.subTest(network_id=value):
                inventory = self.inventory()
                inventory["container"][0]["NetworkSettings"]["Networks"]["owned_frontend"]["NetworkID"] = value
                with self.assertRaises(evidence.Invalid):
                    evidence.network_bindings(inventory)
        for value in (None, [], ""):
            with self.subTest(membership=value):
                inventory = self.inventory()
                inventory["network"][0]["Containers"] = value
                with self.assertRaises(evidence.Invalid):
                    evidence.network_bindings(inventory)


def synthetic_environment(root):
    """A private fixture copy, client pins and admitted inputs, for one test class."""
    fixture_root = root / "fixture"
    shutil.copytree(FIXTURE, fixture_root, ignore=shutil.ignore_patterns("__pycache__"))
    config = root / "config"
    config.mkdir(mode=0o700)
    plugins = config / "cli-plugins"
    plugins.mkdir(mode=0o700)
    (config / "config.json").write_text('{"currentContext":"default"}')
    clients = {}
    for name in ("docker", "compose", "buildx"):
        path = root / "docker" if name == "docker" else plugins / ("docker-" + name)
        path.write_bytes(b"synthetic-never-executed")
        path.chmod(0o500)
        clients[name] = {"path": str(path), "sha256": evidence.sha(path.read_bytes())}
    inputs = {"schema_version": 1, "run_id": "synthetic-compose-123", "release_sha256": "a" * 64,
              "fixture_sha256": driver.tree_digest(fixture_root), "docker_config": str(config), "clients": clients,
              "scope": {"project_id": "project", "environment_id": "environment", "machine_id": "machine",
                        "machine_incarnation": "incarnation", "runtime_identity": "runtime", "docker_context": "owned-context",
                        "docker_endpoint": "unix://" + str(root / "machine.sock"), "engine_id": "engine"},
              "images": {"base": {"reference": "fixture.invalid/base@sha256:" + "b" * 64, "id": "sha256:" + "c" * 64, "platform": "linux/arm64"},
                         "compose": {"reference": "sha256:" + "d" * 64, "id": "sha256:" + "d" * 64, "platform": "linux/arm64"}},
              # Another Environment's live Compose alias. No container in this
              # synthetic Engine holds it, which is the only reason it is denied
              # here; whether it was live where it belongs is a cross-Machine
              # claim this replay deliberately cannot decide.
              "foreign_environments": [{"environment_id": "neighbour-environment", "alias": FOREIGN_ALIAS}]}
    return fixture_root, inputs


class ComposeEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="vz-compose-raw-offline-")
        cls.root = Path(cls.temp.name).resolve()
        cls.fixture, cls.fixture_inputs = synthetic_environment(cls.root)
        cls.base = cls.root / "baseline"
        result = execute_driver(cls.fixture, cls.fixture_inputs, cls.base)
        if result["outcome"] != "fixture_assertions_passed":
            raise AssertionError(result)
        evidence.validate(cls.base, cls.fixture_inputs)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.directory = self.root / ("case-" + self._testMethodName)
        shutil.copytree(self.base, self.directory)
        # The absolute overlay path is part of immutable argv; rewrite the
        # synthetic baseline paths consistently for this independent case.
        for path in self.directory.glob("command-*.json"):
            value = json.loads(path.read_bytes())
            if "argv" in value:
                value["argv"] = [word.replace(str(self.base), str(self.directory)) for word in value["argv"]]
                path.write_bytes(data(value))
        self.refresh()

    def refresh(self):
        for ack in self.directory.glob("command-*.acknowledgement.json"):
            value = json.loads(ack.read_bytes())
            terminal = self.directory / f"command-{value['command_index']:05d}.json"
            value["terminal_receipt_sha256"] = evidence.sha(terminal.read_bytes())
            ack.write_bytes(data(value))
        paths = sorted(path for path in self.directory.iterdir() if path.is_file() and path.name != "checksums.sha256")
        (self.directory / "checksums.sha256").write_text("".join(f"{evidence.sha(path.read_bytes())}  {path.name}\n" for path in paths))

    def receipts(self):
        return [(path, json.loads(path.read_bytes())) for path in sorted(self.directory.glob("command-*.json"))
                if path.name.count(".") == 1]

    def raw(self, predicate, change, stream="stdout"):
        path, receipt = next((path, receipt) for path, receipt in self.receipts() if predicate(receipt))
        raw_path = self.directory / receipt[stream]
        value = change(raw_path.read_bytes())
        raw_path.write_bytes(value)
        receipt["observed_bytes"][stream] = len(value)
        receipt["retained_observed_" + stream + "_bytes"] = len(value)
        for key in (stream + "_sha256", "raw_" + stream + "_sha256", "retained_observed_" + stream + "_sha256"):
            receipt[key] = evidence.sha(value)
        path.write_bytes(data(receipt))
        self.refresh()

    def rejected(self):
        with self.assertRaises(evidence.Invalid):
            evidence.validate(self.directory, self.raw_inputs())

    def raw_inputs(self):
        return copy.deepcopy(self.__class__.fixture_inputs)

    def test_complete_synthetic_evidence_replays_ten_without_certification(self):
        value = evidence.validate(self.directory, self.raw_inputs())
        self.assertEqual(value["recipes_validated"], list(evidence.RECIPES))
        self.assertEqual(len(evidence.RECIPES), 10)
        self.assertEqual(evidence.RELATED[evidence.RECIPES.index("compose-logs")], ["docker.compose.logs"])
        self.assertEqual(evidence.RELATED[evidence.RECIPES.index("compose-dns-boundary")], ["docker.network.dns"])
        self.assertIs(value["compatibility_certified"], False)
        self.assertEqual(len(value["owned_projects"]), 3)
        # What one slice can say on its own: the names it resolved, the foreign
        # name it did not, and the window it holds that denial inside. Whether
        # the foreign Environment was live is decided elsewhere, from every
        # slice together, and is deliberately absent here.
        dns = value["dns_boundary"]
        owner = driver.Inputs(self.raw_inputs(), suite="compose").owner
        self.assertEqual(dns["own_alias"], owner + "-compose-api-1")
        self.assertEqual(dns["local_resolutions"], 6)
        self.assertEqual([entry["alias"] for entry in dns["foreign_denials"]], [FOREIGN_ALIAS])
        self.assertEqual(dns["foreign_denials"][0]["environment_id"], "neighbour-environment")
        self.assertLessEqual(dns["resolved_from_unix_ns"], dns["foreign_denials"][0]["at_unix_ns"])
        self.assertLessEqual(dns["foreign_denials"][0]["at_unix_ns"], dns["resolved_until_unix_ns"])
        self.assertNotEqual(dns["stale"]["removed_container"], dns["stale"]["replacement_container"])
        self.assertEqual(dns["stale"]["names_unresolved_after_remove"], ["api", dns["own_alias"]])
        self.assertNotEqual(dns["stale"]["names_restored_at"], dns["own_address"])

    def logs_row(self, row):
        return row["argv"][5] == "compose" and row["argv"][-len(evidence.LOGS):] == evidence.LOGS

    def fresh(self):
        shutil.rmtree(self.directory)
        self.setUp()

    def rewrite_logs(self, change):
        """Rewrite the followed stream and re-bind the observation's raw digest claim."""
        self.raw(self.logs_row, change)
        _path, row = next(item for item in self.receipts() if self.logs_row(item[1]))
        digest = evidence.sha((self.directory / row["stdout"]).read_bytes())
        result_path = self.directory / "result.json"
        result = json.loads(result_path.read_bytes())
        item = result["observations"][evidence.RECIPES.index("compose-logs")]
        item["assertions"] = [x if not x.startswith("compose logs raw stdout sha256 ") else
                              "compose logs raw stdout sha256 " + digest for x in item["assertions"]]
        result_path.write_bytes(data(result))
        self.refresh()

    def test_compose_logs_recipe_is_a_readonly_history_read_without_follow_or_mutation(self):
        rows = [row for _path, row in self.receipts()]
        index = next(i for i, row in enumerate(rows) if self.logs_row(row))
        self.assertIs(rows[index]["mutation"], False)
        self.assertEqual(rows[index]["exit_code"], 0)
        self.assertNotIn("--follow", rows[index]["argv"])
        self.assertEqual(rows[index]["argv"][5:8], ["compose", "--project-name", driver.Inputs(self.raw_inputs(), suite="compose").owner + "-compose"])
        item = json.loads((self.directory / "result.json").read_bytes())["observations"][evidence.RECIPES.index("compose-logs")]
        span = rows[item["first_command"] - 1:item["last_command"]]
        self.assertTrue(all(row["mutation"] is False for row in span), "logs recipe must not mutate")
        self.assertEqual([row["argv"][5] for row in span if row["argv"][5] == "compose"], ["compose"])

    def test_compose_logs_padding_and_full_container_name_prefixes_are_accepted(self):
        owner = driver.Inputs(self.raw_inputs(), suite="compose").owner
        def change(raw):
            lines = []
            for line in raw.splitlines():
                name, message = line.split(b" | ", 1)
                name = name.strip()
                if name.startswith(b"api"):
                    name = (owner + "-compose-").encode() + name
                lines.append(name + b"        | " + message + b"\n")
            return b"".join(lines)
        self.rewrite_logs(change)
        evidence.validate(self.directory, self.raw_inputs())

    def test_compose_logs_foreign_owner_unknown_container_and_diagnostics_rejected(self):
        owner = driver.Inputs(self.raw_inputs(), suite="compose").owner
        for name, change in (
                ("foreign-owner", lambda raw: raw.replace(("|" + owner + "|listening").encode(), b"|vz04-foreign|listening", 1)),
                ("foreign-project-container", lambda raw: raw + b"failure-1 | vz04|failure|" + owner.encode() + b"|exit-37\n"),
                ("neighbor-machine-line", lambda raw: raw + b"db-1 | vz04|db|vz04-neighbor|listening\n"),
                ("unattributed-line", lambda raw: raw + b"vz04|db|" + owner.encode() + b"|listening\n"),
                ("unterminated", lambda raw: raw[:-1]),
                ("empty", lambda _raw: b"")):
            with self.subTest(change=name):
                self.fresh()
                self.rewrite_logs(change)
                self.rejected()
        self.fresh()
        self.raw(self.logs_row, lambda _raw: b"WARN follow stopped\n", stream="stderr")
        self.rejected()

    def test_compose_logs_missing_duplicate_or_reordered_service_lines_rejected(self):
        def reorder(raw):
            lines = raw.splitlines(keepends=True)
            api = [i for i, line in enumerate(lines) if line.lstrip().startswith(b"api-1")]
            lines[api[0]], lines[api[1]] = lines[api[1]], lines[api[0]]
            return b"".join(lines)
        for name, change in (("missing", lambda raw: b"".join(raw.splitlines(keepends=True)[:-1])),
                             ("duplicate", lambda raw: raw + raw.splitlines(keepends=True)[0]),
                             ("reordered", reorder)):
            with self.subTest(change=name):
                self.fresh()
                self.rewrite_logs(change)
                self.rejected()

    def test_compose_logs_raw_digest_claim_must_match_retained_stream(self):
        result_path = self.directory / "result.json"
        result = json.loads(result_path.read_bytes())
        item = result["observations"][evidence.RECIPES.index("compose-logs")]
        item["assertions"] = [x if not x.startswith("compose logs raw stdout sha256 ") else
                              "compose logs raw stdout sha256 " + "0" * 64 for x in item["assertions"]]
        result_path.write_bytes(data(result))
        self.refresh()
        self.rejected()

    def test_compose_logs_read_requires_exact_healthy_running_inventory(self):
        rows = [row for _path, row in self.receipts()]
        index = next(i for i, row in enumerate(rows) if self.logs_row(row))
        inspect = next(row for row in reversed(rows[:index]) if row["argv"][5:7] == ["container", "inspect"])
        def change(raw):
            items = json.loads(raw)
            next(item for item in items if item["Config"]["Labels"]["com.docker.compose.service"] == "worker")["State"].update(
                Status="exited", Running=False)
            return data(items)
        self.raw(lambda row: row["index"] == inspect["index"], change)
        self.rejected()

    def test_checksum_and_missing_raw_file_fail(self):
        (self.directory / "command-00001.stdout").write_bytes(b"changed")
        self.rejected()

    def test_schema_only_claim_without_commands_fails(self):
        path = self.directory / "result.json"
        result = json.loads(path.read_bytes())
        result["command_count"] = 0
        path.write_bytes(data(result))
        self.refresh()
        self.rejected()

    def test_cross_machine_context_raw_reply_fails_after_rehash(self):
        self.raw(lambda row: row["argv"][5:7] == ["context", "inspect"],
                 lambda raw: data([json.loads(raw)[0] | {"Name": "sibling-context"}]))
        self.rejected()

    def test_wrong_exec_stream_fails_after_rehash(self):
        self.raw(lambda row: row["exit_code"] == 37 and row["argv"][-1] == "exec", lambda _raw: b"wrong\n")
        self.rejected()

    def test_http_error_is_not_network_denial_even_with_matching_hashes(self):
        self.raw(lambda row: "transport" in row["argv"], lambda raw: data(json.loads(raw) | {"outcome": "http_response", "status": 503}))
        self.rejected()

    def test_claimed_timeout_cannot_hide_application_exception(self):
        self.raw(lambda row: "transport" in row["argv"],
                 lambda raw: data(json.loads(raw) | {"outcome": "timeout", "errno": None, "exception": "ValueError"}))
        self.rejected()

    def test_unlabelled_exact_name_collision_rejected(self):
        owner = driver.Inputs(self.raw_inputs(), suite="compose").owner
        self.raw(lambda row: row["argv"][5:] == ["volume", "ls", "--format", "{{.Name}}"],
                 lambda _raw: (owner + "-compose_state\n").encode())
        self.rejected()

    def test_extra_mutating_command_cannot_hide_in_guard_span(self):
        path, row = self.receipts()[0]
        row["argv"] = row["argv"][:5] + ["system", "prune", "--force"]
        row["mutation"] = True
        path.write_bytes(data(row))
        intent_path = self.directory / path.name.replace(".json", ".intent.json")
        intent = json.loads(intent_path.read_bytes())
        intent.update(argv=row["argv"], mutation=True)
        intent_path.write_bytes(data(intent))
        self.refresh()
        self.rejected()

    def test_inert_runtime_metadata_without_bound_inventory_rejected(self):
        self.raw(lambda row: row["argv"][5] == "info", lambda raw: data(json.loads(raw) | {
            "Runtimes": {"youki": {"path": "/mnt/linux-bin/youki"}, "runc": {"path": "runc"}}}))
        self.rejected()

    def test_runtime_proof_rejects_rehashed_foreign_owner_and_incarnation(self):
        raw = self.raw_inputs()
        owner = {key: raw["scope"][key] for key in ("project_id", "environment_id", "machine_id")}
        incarnation = {"schema_version": 1, "machine_id": owner["machine_id"], "incarnation_id": raw["scope"]["machine_incarnation"], "generation": 1}
        inventory = {"owner": owner, "incarnation": incarnation, "youki_sha256": "e" * 64,
                     "scope": "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit",
                     "stdout": "vz-startup-runtime-inventory-v1\nyouki-sha256=" + "e" * 64 + "\nyouki version: synthetic\nalternate-runtime-binaries=absent\n"}
        receipt = {"schema_version": 1, "state": "completed", "failure": None, "owner": owner,
                   "context": raw["scope"]["docker_context"], "client_sha256": raw["clients"]["docker"]["sha256"],
                   "incarnation": incarnation, "resources": {"engine_id": raw["scope"]["engine_id"],
                       "cleanup_scope": "disposable_probe_containers_compose_objects_and_images", "retained_buildkit_cache": True,
                       "runtime_inventory": inventory}}
        with tempfile.TemporaryDirectory(prefix="vz-compose-runtime-proof-") as temporary:
            root = Path(temporary).resolve()
            def publish(value):
                receipt_path, inventory_path = root / "receipt.json", root / "inventory.json"
                receipt_path.write_bytes(data(value))
                digest = evidence.sha(receipt_path.read_bytes())
                inventory_path.write_bytes(data({"schema_version": 1, "probe_receipt_sha256": digest,
                                                "runtime_inventory": value["resources"]["runtime_inventory"]}))
                raw["runtime_evidence"] = {"receipt_path": str(receipt_path), "receipt_sha256": digest,
                    "inventory_path": str(inventory_path), "inventory_sha256": evidence.sha(inventory_path.read_bytes()), "youki_sha256": "e" * 64}
            publish(receipt)
            evidence.runtime_proof(raw)
            for field in ("owner", "incarnation", "stdout"):
                changed = copy.deepcopy(receipt)
                if field == "owner":
                    changed["owner"]["machine_id"] = "sibling"
                elif field == "incarnation":
                    changed["incarnation"]["incarnation_id"] = "stale"
                else:
                    changed["resources"]["runtime_inventory"]["stdout"] = "runc is present\n"
                publish(changed)
                with self.subTest(field=field), self.assertRaises(evidence.Invalid):
                    evidence.runtime_proof(raw)

    def test_healthy_event_order_replayed_not_assertion_text(self):
        def changed(raw):
            events = [json.loads(line) for line in raw.splitlines()]
            healthy = next(event["timeNano"] for event in events if event["Action"] == "health_status: healthy")
            starts = [event for event in events if event["Action"] == "start"]
            starts[1]["timeNano"] = healthy
            return b"\n".join(data(event) for event in events)
        self.raw(lambda row: row["argv"][5] == "events" and "-compose" in " ".join(row["argv"]), changed)
        self.rejected()

    def test_failure_propagation_requires_all_other_services_stopped(self):
        def candidate(row):
            if row["argv"][5:7] != ["container", "inspect"] or row["exit_code"]:
                return False
            return any(item["Config"]["Labels"]["com.docker.compose.service"] == "failure"
                       for item in json.loads((self.directory / row["stdout"]).read_bytes()))
        def changed(raw):
            items = json.loads(raw)
            next(item for item in items if item["Config"]["Labels"]["com.docker.compose.service"] == "api")["State"]["Running"] = True
            return data(items)
        self.raw(candidate, changed)
        self.rejected()

    def test_paired_live_control_cannot_be_empty(self):
        self.raw(lambda row: "probe" in row["argv"] and row["argv"][-1] == "http://127.0.0.1:8080/health", lambda _raw: b"")
        self.rejected()

    def test_blocked_history_cannot_be_empty(self):
        self.raw(lambda row: row["argv"][5] == "events" and "-blocked" in " ".join(row["argv"]), lambda _raw: b"")
        self.rejected()

    def test_post_restart_host_marker_must_match(self):
        matches = [(path, row) for path, row in self.receipts() if evidence.READ in row["argv"]]
        index = matches[-1][1]["index"]
        self.raw(lambda row: row["index"] == index, lambda _raw: b"startup regenerated another marker\n")
        self.rejected()

    def test_replica_identity_wrong_owner_fails(self):
        self.raw(lambda row: row["argv"][-1] == "http://127.0.0.1:8080/identity", lambda raw: data(json.loads(raw) | {"owner": "foreign"}))
        self.rejected()

    def test_negative_acknowledgement_is_mandatory(self):
        next(self.directory.glob("command-*.acknowledgement.json")).unlink()
        self.refresh()
        self.rejected()

    def test_erasing_negative_uncertainty_does_not_make_it_success(self):
        path, row = next((path, row) for path, row in self.receipts() if row["effects_uncertain"])
        row["effects_uncertain"] = False
        path.write_bytes(data(row))
        self.refresh()
        self.rejected()

    def test_resource_leak_after_down_fails(self):
        index = max(row["index"] for _path, row in self.receipts() if row["argv"][5:7] == ["volume", "ls"] and "--filter" in row["argv"])
        self.raw(lambda row: row["index"] == index, lambda _raw: b"leaked-volume\n")
        self.rejected()

    def test_unlabelled_exact_name_after_down_fails(self):
        owner = driver.Inputs(self.raw_inputs(), suite="compose").owner
        index = max(row["index"] for _path, row in self.receipts() if row["argv"][5:] == ["volume", "ls", "--format", "{{.Name}}"])
        self.raw(lambda row: row["index"] == index, lambda _raw: (owner + "-failure_state\n").encode())
        self.rejected()

    def test_exact_pre_down_container_cannot_survive_without_labels(self):
        index = max(row["index"] for _path, row in self.receipts() if row["argv"][5:7] == ["container", "inspect"])
        self.raw(lambda row: row["index"] == index, lambda _raw: data([{"Id": "e" * 64}]))
        self.rejected()

    def test_fifo_and_symlink_evidence_fail_without_reading_targets(self):
        path = self.directory / "command-00001.stdout"
        path.unlink()
        os.mkfifo(path)
        self.rejected()
        path.unlink()
        path.symlink_to(self.root / "docker")
        self.rejected()

    def resolve_receipts(self):
        """Every recorded in-container resolution, in the order the recipe asked."""
        rows = []
        for path, receipt in self.receipts():
            argv = receipt["argv"]
            if len(argv) == 11 and argv[5] == "exec" and argv[7:10] == ["python3", "-c", evidence.RESOLVE]:
                rows.append((receipt["index"], argv[10]))
        return rows

    def rewrite_resolution(self, position, value):
        index, name = self.resolve_receipts()[position]
        self.raw(lambda row: row["index"] == index, lambda _raw: data(dict(value, name=name)))
        return name

    ANSWERED = {"schema_version": 1, "outcome": "resolved", "addresses": ["172.18.0.99"],
                "errno": None, "exception": None}
    DENIED = {"schema_version": 1, "outcome": "unresolved", "addresses": [], "errno": -2, "exception": "gaierror"}
    DEADLINE = {"schema_version": 1, "outcome": "deadline", "addresses": [], "errno": None, "exception": "TimeoutError"}
    # No address, but not "no such name": the probe failed for some other
    # reason, which says nothing about whether the name exists.
    UNCLASSIFIED = {"schema_version": 1, "outcome": "unresolved", "addresses": [],
                    "errno": None, "exception": "OSError"}

    def test_the_recorded_resolutions_are_the_recipe_the_claim_describes(self):
        owner = driver.Inputs(self.raw_inputs(), suite="compose").owner
        own = owner + "-compose-api-1"
        self.assertEqual([name for _index, name in self.resolve_receipts()],
                         # bracket, denial, bracket, then remove/deny/control/restore
                         ["api", own, "worker", FOREIGN_ALIAS, "api", own, "worker",
                          "api", own, "worker", "api", own])

    def test_a_foreign_environment_alias_that_answered_is_rejected(self):
        # The boundary leaking is the failure this row exists to catch.
        self.rewrite_resolution(3, self.ANSWERED)
        self.rejected()

    def test_a_denial_without_an_address_but_without_a_resolver_answer_is_rejected(self):
        # `deadline` means the probe gave up, which is what a stale record whose
        # container is gone looks like when it is measured by connecting;
        # `unresolved` without a name-resolution error is a probe that broke.
        for value in (self.DEADLINE, self.UNCLASSIFIED):
            for position in (3, 7, 8):
                with self.subTest(position=position, outcome=value["outcome"], exception=value["exception"]):
                    self.fresh()
                    self.rewrite_resolution(position, value)
                    self.rejected()

    def test_a_name_that_survived_the_removal_of_its_container_is_rejected(self):
        for position in (7, 8):
            with self.subTest(position=position):
                self.fresh()
                self.rewrite_resolution(position, self.ANSWERED)
                self.rejected()

    def test_a_stale_denial_beside_a_dead_resolver_is_rejected(self):
        # Without the live control, "api no longer resolves" is satisfied by a
        # container whose resolver stopped answering anything at all.
        self.rewrite_resolution(9, self.DENIED)
        self.rejected()

    def test_an_own_alias_that_did_not_answer_before_or_after_a_denial_is_rejected(self):
        for position in (0, 1, 2, 4, 5, 6):
            with self.subTest(position=position):
                self.fresh()
                self.rewrite_resolution(position, self.DENIED)
                self.rejected()

    def test_an_own_alias_answering_the_wrong_container_address_is_rejected(self):
        for position in (0, 1, 10, 11):
            with self.subTest(position=position):
                self.fresh()
                self.rewrite_resolution(position, self.ANSWERED)
                self.rejected()

    def test_the_replay_refuses_evidence_that_declares_no_foreign_environment(self):
        inputs = self.raw_inputs()
        inputs.pop("foreign_environments")
        with self.assertRaises(evidence.Invalid):
            evidence.validate(self.directory, inputs)

    def test_the_replay_refuses_a_foreign_alias_the_slice_did_not_probe(self):
        inputs = self.raw_inputs()
        inputs["foreign_environments"] = [{"environment_id": "neighbour-environment",
                                           "alias": "vz04-ffffffffffffffffffffffff-compose-api-1"}]
        with self.assertRaises(evidence.Invalid):
            evidence.validate(self.directory, inputs)


class ComposeDnsDriverTests(unittest.TestCase):
    """The Driver's own refusals: an Engine whose DNS boundary does not hold."""

    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="vz-compose-dns-offline-")
        cls.root = Path(cls.temp.name).resolve()
        cls.fixture, cls.inputs = synthetic_environment(cls.root)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def run_mode(self, mode):
        inputs = copy.deepcopy(self.inputs)
        engine = TamperedEngine(driver.Inputs(inputs, suite="compose"), mode)
        return execute_driver(self.fixture, inputs, self.root / ("case-" + mode), engine=engine)

    def test_each_broken_dns_property_fails_the_subset_with_its_own_cause(self):
        for mode, reason in (("foreign-resolves", "address-free denial"),
                             ("stale-record", "address-free denial"),
                             ("dead-resolver", "exact container address: worker"),
                             ("connect-deadline", "address-free denial"),
                             ("unclassified-failure", "address-free denial"),
                             ("wrong-address", "exact container address: api"),
                             ("no-removal", "expected 1")):
            with self.subTest(mode=mode):
                result = self.run_mode(mode)
                self.assertEqual(result["outcome"], "failed", mode)
                self.assertIn(reason, result["failure"] or "", mode)
                # The failure belongs to the DNS recipe and nothing after it ran.
                recipes = [item["recipe"] for item in result["observations"]]
                self.assertEqual(recipes[-1], "compose-dns-boundary", mode)

    def test_the_subset_refuses_to_start_without_a_foreign_environment(self):
        inputs = copy.deepcopy(self.inputs)
        inputs.pop("foreign_environments")
        result = execute_driver(self.fixture, inputs, self.root / "case-absent")
        self.assertEqual(result["outcome"], "failed")
        self.assertIn("live foreign Environment alias", result["failure"])
        self.assertEqual(result["observations"], [])


class ForeignAliasInputTests(unittest.TestCase):
    """Admission of the foreign-alias input, which fixes the names in advance."""

    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="vz-compose-alias-offline-")
        cls.root = Path(cls.temp.name).resolve()
        cls.fixture, cls.inputs = synthetic_environment(cls.root)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def raw(self, rows):
        inputs = copy.deepcopy(self.inputs)
        inputs["foreign_environments"] = rows
        return inputs

    def test_an_admitted_list_names_other_environments_and_distinct_aliases(self):
        driver.Inputs(self.raw([{"environment_id": "neighbour-environment", "alias": FOREIGN_ALIAS}]), suite="compose")

    def test_rejections(self):
        own = driver.owner_token(self.inputs["run_id"], self.inputs["scope"]) + evidence.DNS_SUFFIX
        cases = {
            "empty": [],
            "own-environment": [{"environment_id": "environment", "alias": FOREIGN_ALIAS}],
            "own-alias": [{"environment_id": "neighbour-environment", "alias": own}],
            "duplicate": [{"environment_id": "neighbour-environment", "alias": FOREIGN_ALIAS},
                          {"environment_id": "third-environment", "alias": FOREIGN_ALIAS}],
            "extra-field": [{"environment_id": "neighbour-environment", "alias": FOREIGN_ALIAS, "live": True}],
            "bad-alias": [{"environment_id": "neighbour-environment", "alias": "Not An Alias"}],
        }
        for name, rows in cases.items():
            with self.subTest(case=name):
                with self.assertRaises(driver.Rejected):
                    driver.Inputs(self.raw(rows), suite="compose")


if __name__ == "__main__":
    unittest.main()
