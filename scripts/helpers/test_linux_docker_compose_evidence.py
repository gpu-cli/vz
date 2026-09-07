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


def data(value):
    return json.dumps(value, sort_keys=True).encode()


class SyntheticEngine:
    """In-memory response generator, never evidence of real Docker behavior."""
    def __init__(self, inputs):
        self.inputs = inputs
        self.owner = inputs.owner
        self.projects, self.markers = {}, {}

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

    def container(self, project, role, number=1):
        identity = self.identity(project + role + str(number))
        names = {"db": ("backend",), "api": ("frontend", "backend"), "worker": ("frontend",),
                 "isolated": ("isolated",), "failure": ("frontend",)}[role]
        return {"Id": identity, "Name": f"/{project}-{role}-{number}", "Image": self.inputs.raw["images"]["compose"]["id"],
                "Config": {"Hostname": identity[:12], "Labels": self.labels(project) | {"com.docker.compose.service": role}},
                "State": {"Status": "created", "Running": False, "ExitCode": 0, "Health": {"Status": "starting"}},
                "Mounts": [{"Destination": "/data", "Type": "volume", "Name": project + "_state", "RW": True}] if role == "db" else [],
                "NetworkSettings": {"Networks": {project + "_" + name: {"IPAddress": f"172.{18 + index}.0.{10 + number}",
                                                                         "NetworkID": self.identity(project + name)}
                                                  for index, name in enumerate(names)}}}

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
            values = [item for inventory in self.projects.values() for item in inventory[kind]]
            key = "Name" if kind == "volume" else "Id"
            if args[1] == "ls":
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
            elif action == "exec":
                code = 37
                stdout, stderr = (f"vz04|api|{self.owner}|exec-{stream}\n".encode() for stream in ("stdout", "stderr"))
            elif action == "logs":
                assert tail == ["logs", "--follow", "--no-color"], tail
                assert all(not item["State"]["Running"] for item in self.projects[project]["container"]), "follow on running services"
                stdout = self.compose_logs(project)
            else:
                self.install(project, failure="--exit-code-from" in tail)
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
                                     "Health": {"Status": "unhealthy" if blocked and role == "db" else "healthy"}}
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
            if tail[1] == "-c":
                if tail[2] == evidence.WRITE:
                    self.markers[(item["Id"], tail[3])] = tail[4].encode()
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


class ComposeEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="vz-compose-raw-offline-")
        cls.root = Path(cls.temp.name).resolve()
        cls.fixture = cls.root / "fixture"
        fixture = Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker"
        shutil.copytree(fixture, cls.fixture, ignore=shutil.ignore_patterns("__pycache__"))
        config = cls.root / "config"
        config.mkdir(mode=0o700)
        plugins = config / "cli-plugins"
        plugins.mkdir(mode=0o700)
        (config / "config.json").write_text('{"currentContext":"default"}')
        clients = {}
        for name in ("docker", "compose", "buildx"):
            path = cls.root / "docker" if name == "docker" else plugins / ("docker-" + name)
            path.write_bytes(b"synthetic-never-executed")
            path.chmod(0o500)
            clients[name] = {"path": str(path), "sha256": evidence.sha(path.read_bytes())}
        cls.fixture_inputs = {"schema_version": 1, "run_id": "synthetic-compose-123", "release_sha256": "a" * 64,
                   "fixture_sha256": driver.tree_digest(cls.fixture), "docker_config": str(config), "clients": clients,
                   "scope": {"project_id": "project", "environment_id": "environment", "machine_id": "machine",
                             "machine_incarnation": "incarnation", "runtime_identity": "runtime", "docker_context": "owned-context",
                             "docker_endpoint": "unix://" + str(cls.root / "machine.sock"), "engine_id": "engine"},
                   "images": {"base": {"reference": "fixture.invalid/base@sha256:" + "b" * 64, "id": "sha256:" + "c" * 64, "platform": "linux/arm64"},
                              "compose": {"reference": "sha256:" + "d" * 64, "id": "sha256:" + "d" * 64, "platform": "linux/arm64"}}}
        inputs = driver.Inputs(cls.fixture_inputs, suite="compose")
        cls.base = cls.root / "baseline"
        original_stat = Path.stat
        def fake_stat(path, *args, **kwargs):
            if str(path) == cls.fixture_inputs["scope"]["docker_endpoint"][7:]:
                return types.SimpleNamespace(st_mode=stat.S_IFSOCK)
            return original_stat(path, *args, **kwargs)
        def persist(_recorder, path, value, **_kwargs):
            # Synthetic fixtures make no fsync/durability claim.
            path.write_bytes(data(value))
        with patch.object(driver.sys, "platform", "darwin"), patch.object(driver.os, "uname", return_value=types.SimpleNamespace(machine="arm64")), \
                patch.object(Path, "stat", fake_stat), patch.object(driver, "execute", side_effect=SyntheticEngine(inputs)), \
                patch.object(driver.Recorder, "persist", persist):
            result = driver.Driver(inputs, cls.fixture, cls.base).run("compose")
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

    def test_complete_synthetic_evidence_replays_nine_without_certification(self):
        value = evidence.validate(self.directory, self.raw_inputs())
        self.assertEqual(value["recipes_validated"], list(evidence.RECIPES))
        self.assertEqual(len(evidence.RECIPES), 9)
        self.assertEqual(evidence.RELATED[evidence.RECIPES.index("compose-logs")], ["docker.compose.logs"])
        self.assertIs(value["compatibility_certified"], False)
        self.assertEqual(len(value["owned_projects"]), 3)

    def logs_row(self, row):
        return row["argv"][-3:] == evidence.LOGS

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

    def test_compose_logs_recipe_is_readonly_follow_after_stop_and_before_restart(self):
        rows = [row for _path, row in self.receipts()]
        index = next(i for i, row in enumerate(rows) if self.logs_row(row))
        self.assertIs(rows[index]["mutation"], False)
        self.assertEqual(rows[index]["exit_code"], 0)
        self.assertEqual(rows[index]["argv"][5:9][:3], ["compose", "--project-name", driver.Inputs(self.raw_inputs(), suite="compose").owner + "-compose"])
        before = [row["argv"][-1] for row in rows[:index] if row["argv"][5] == "compose"]
        after = [row["argv"][-1] for row in rows[index + 1:] if row["argv"][5] == "compose"]
        self.assertEqual(before[-1], "stop")
        self.assertEqual(after[0], "30")  # up --wait --wait-timeout 30 restores the topology

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

    def test_compose_logs_follow_requires_every_service_stopped_first(self):
        rows = [row for _path, row in self.receipts()]
        index = next(i for i, row in enumerate(rows) if self.logs_row(row))
        inspect = next(row for row in reversed(rows[:index]) if row["argv"][5:7] == ["container", "inspect"])
        def change(raw):
            items = json.loads(raw)
            next(item for item in items if item["Config"]["Labels"]["com.docker.compose.service"] == "worker")["State"].update(
                Status="running", Running=True)
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


if __name__ == "__main__":
    unittest.main()
