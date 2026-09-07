"""Independent raw-receipt replay for the installed Compose DEV subset.

The caller authenticates expected_inputs against installed Up and runtime
receipts. This validator checks the retained host execution, not full Docker
compatibility, live health, or the authority of a caller-provided input object.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat


RECIPES = (
    "compose-create", "compose-up-order", "compose-logs", "compose-exec", "compose-network-paths",
    "compose-volume-persistence", "compose-scale", "compose-blocked-health", "compose-failure",
)
RELATED = (
    ["docker.compose.create"],
    ["docker.compose.up", "docker.compose.dependency_ordering", "docker.compose.health_ordering"],
    ["docker.compose.logs"],
    ["docker.compose.exec"], ["docker.compose.networks"], ["docker.compose.volumes"],
    ["docker.compose.scaling"], ["docker.compose.health_ordering"], ["docker.compose.failure_propagation"],
)
ROLES = {"db", "api", "worker", "isolated"}
UP = ["up", "--detach", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "30"]
LOGS = ["logs", "--follow", "--no-color"]
LOG_LINE = re.compile(rb"([^\s|]+) +\| (.*)")
# One startup sequence per service, in order; every line carries the owner token.
LOG_EVENTS = {"db": ("listening",), "api": ("dependency-healthy", "listening"),
              "worker": ("dependency-healthy", "listening"), "isolated": ("listening",)}
READ = "import pathlib,sys;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes())"
WRITE = "import os,sys;f=open(sys.argv[1],'xb');f.write(sys.argv[2].encode());f.flush();os.fsync(f.fileno());f.close()"
MAX = 4 * 1024 * 1024


class Invalid(ValueError):
    pass


def require(value, message):
    if not value:
        raise Invalid(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=unique)


def read(path, maximum=MAX):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as file:
        before = os.fstat(file.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= maximum,
                "nonregular, linked or oversized evidence")
        data = file.read(maximum + 1)
        after = os.fstat(file.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                and len(data) == before.st_size, "evidence changed while reading")
        return data


def hex64(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def network_bindings(inventory):
    """Distinguish exact owned attachments from proven never-started declarations."""
    networks = {item["Name"]: item for item in inventory["network"]}
    require(len(networks) == len(inventory["network"]) and
            len({item["Id"] for item in networks.values()}) == len(networks), "ambiguous owned network names/IDs")
    for container in inventory["container"]:
        declared = container["NetworkSettings"]["Networks"]
        require(isinstance(declared, dict), "invalid declared networks")
        for name, endpoint in declared.items():
            require(name in networks, "foreign declared network name")
            if endpoint["NetworkID"] != "":
                require(endpoint["NetworkID"] == networks[name]["Id"], "network name/ID mismatch")
                continue
            state = container["State"]
            require(state.get("Status") == "created" and
                    all(state.get(key) is False for key in ("Running", "Paused", "Restarting")) and
                    type(state.get("Pid")) is int and state["Pid"] == 0 and
                    state.get("StartedAt") == state.get("FinishedAt") == "0001-01-01T00:00:00Z" and
                    container.get("HostConfig", {}).get("NetworkMode") in declared,
                    "empty network ID without never-started authority")
            require(all(endpoint.get(key) == "" for key in
                        ("EndpointID", "Gateway", "IPAddress", "MacAddress", "IPv6Gateway", "GlobalIPv6Address")) and
                    all(type(endpoint.get(key)) is int and endpoint[key] == 0
                        for key in ("IPPrefixLen", "GlobalIPv6PrefixLen")), "unattached declaration has endpoint state")
            require(all(isinstance(network.get("Containers"), dict) and
                        container["Id"] not in network["Containers"] for network in networks.values()),
                    "unattached container remains in owned network membership")


def manifest(directory):
    require(directory.is_absolute() and directory == directory.resolve() and directory.is_dir(),
            "canonical evidence directory required")
    require(stat.S_IMODE(directory.stat().st_mode) == 0o700 and directory.stat().st_uid == os.geteuid(),
            "private owned evidence directory required")
    rows = {}
    for line in read(directory / "checksums.sha256").decode().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([^\x00-\x1f]+)", line)
        require(match is not None, "malformed checksum line")
        digest, name = match.groups()
        path = Path(name)
        require(not path.is_absolute() and all(part not in {".", ".."} for part in path.parts)
                and path.as_posix() == name and name not in rows and name != "checksums.sha256",
                "unsafe/duplicate checksum path")
        rows[name] = digest
    found = {}
    for path in directory.rglob("*"):
        require(not path.is_symlink(), "evidence symlink rejected")
        relative = path.relative_to(directory)
        if relative.parts[0] == "private-tmp":
            continue
        if path.is_dir() or relative.as_posix() == "checksums.sha256":
            continue
        require(len(found) < 20000, "unbounded evidence inventory")
        found[relative.as_posix()] = read(path)
    require(set(found) == set(rows), "checksum inventory missing or extra file")
    require(all(sha(data) == rows[name] for name, data in found.items()), "checksum mismatch")
    return found


def fixture_digest(root):
    require(root.is_absolute() and root == root.resolve() and root.is_dir(), "invalid fixture root")
    rows = []
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "fixture symlink")
        if path.is_dir():
            continue
        data = read(path)
        rows.append([path.relative_to(root).as_posix(), stat.S_IMODE(path.stat().st_mode), len(data), sha(data)])
        require(len(rows) <= 10000, "unbounded fixture tree")
    return sha(json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode())


def runtime_proof(inputs):
    """Bind optional startup executable evidence; its parent supplies authority."""
    proof = inputs.get("runtime_evidence")
    if proof is None:
        return
    require(set(proof) == {"receipt_path", "receipt_sha256", "inventory_path", "inventory_sha256", "youki_sha256"}
            and hex64(proof["youki_sha256"]), "malformed runtime proof pins")
    values = {}
    for name in ("receipt", "inventory"):
        path = Path(proof[name + "_path"])
        require(path.is_absolute() and path == path.resolve(), "redirected runtime evidence")
        raw = read(path)
        require(hex64(proof[name + "_sha256"]) and sha(raw) == proof[name + "_sha256"], "runtime evidence hash mismatch")
        values[name] = decode(raw)
    receipt, after = values["receipt"], values["inventory"]
    scope = inputs["scope"]
    owner = {key: scope[key] for key in ("project_id", "environment_id", "machine_id")}
    incarnation = receipt["incarnation"]
    require(receipt["schema_version"] == 1 and receipt["state"] == "completed" and receipt["failure"] is None
            and receipt["owner"] == owner and receipt["context"] == scope["docker_context"]
            and receipt["client_sha256"] == inputs["clients"]["docker"]["sha256"]
            and incarnation["schema_version"] == 1 and incarnation["machine_id"] == owner["machine_id"]
            and incarnation["incarnation_id"] == scope["machine_incarnation"]
            and type(incarnation["generation"]) is int and incarnation["generation"] > 0,
            "foreign or incomplete startup identity")
    resources = receipt["resources"]
    require(resources["engine_id"] == scope["engine_id"]
            and resources["cleanup_scope"] == "disposable_probe_containers_compose_objects_and_images"
            and resources["retained_buildkit_cache"] is True, "foreign startup Engine/cleanup scope")
    require(set(after) == {"schema_version", "probe_receipt_sha256", "runtime_inventory"}
            and after["schema_version"] == 1 and after["probe_receipt_sha256"] == proof["receipt_sha256"], "unbound after-inventory")
    inventory = after["runtime_inventory"]
    require(inventory == resources["runtime_inventory"] and set(inventory) == {"owner", "incarnation", "youki_sha256", "scope", "stdout"}
            and inventory["owner"] == owner and inventory["incarnation"] == incarnation
            and inventory["youki_sha256"] == proof["youki_sha256"]
            and inventory["scope"] == "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit", "foreign runtime inventory")
    output = inventory["stdout"]
    require(isinstance(output, str) and len(output.encode()) <= 8192
            and output.startswith("vz-startup-runtime-inventory-v1\nyouki-sha256=" + proof["youki_sha256"] + "\n")
            and output.endswith("\nalternate-runtime-binaries=absent\n") and "\nyouki version: " in output,
            "alternate executable runtime absence unproven")


class Replay:
    def __init__(self, directory, inputs):
        self.directory, self.inputs = directory, inputs
        self.files = manifest(directory)
        require(decode(self.files["inputs.json"]) == inputs, "foreign input object")
        self.result = decode(self.files["result.json"])
        result = self.result
        require(set(result) == {"schema_version", "kind", "suite", "run_id", "scope", "release_sha256", "fixture_sha256",
                               "compatibility_certified", "release_scenarios_passed", "test_case_retries", "outcome", "failure",
                               "cleanup_errors", "observations", "command_count", "owned_projects", "remaining"}, "unknown result fields")
        runtime_proof(inputs)
        require(result["kind"] == "docker_host_fixture_subset" and type(result["schema_version"]) is int
                and result["schema_version"] == 1 and result["suite"] == "compose"
                and result["compatibility_certified"] is False and result["release_scenarios_passed"] == []
                and type(result["test_case_retries"]) is int and result["test_case_retries"] == 0
                and result["outcome"] == "fixture_assertions_passed" and result["failure"] is None
                and result["cleanup_errors"] == [], "failed, retried or overclaimed subset")
        for key in ("scope", "run_id", "release_sha256", "fixture_sha256"):
            require(result[key] == inputs[key], "result owner or input digest mismatch")
        require(hex64(inputs["release_sha256"]) and hex64(inputs["fixture_sha256"]), "invalid input digest")
        self.scope = inputs["scope"]
        self.owner = "vz04-" + sha(json.dumps([inputs["run_id"], self.scope], sort_keys=True).encode())[:24]
        self.projects, self.rows, self.acknowledged = {}, [], set()
        self.i = 0
        count = result["command_count"]
        require(type(count) is int and 1 <= count <= 3000, "invalid command inventory")
        expected_files = {"inputs.json", "result.json", "compose-owner.json"}
        previous = 0
        for index in range(1, count + 1):
            stem = f"command-{index:05d}"
            expected_files.update(stem + ext for ext in (".json", ".intent.json", ".stdout", ".stderr"))
            row = decode(self.files[stem + ".json"])
            intent = decode(self.files[stem + ".intent.json"])
            require(row["index"] == index and type(row["index"]) is int and row["argv0"] == "docker"
                    and row["argv"][:5] == ["docker", "--config", inputs["docker_config"], "--context", self.scope["docker_context"]]
                    and row["executable"] == inputs["clients"]["docker"]["path"], "unbound command routing")
            require(row["host_outcome"] == "exited" and row["capture_complete"] is True
                    and row["raw_streams_retained"] is True and row["timed_out"] is False
                    and row["interrupted"] is False and row["output_limit_exceeded"] is False
                    and row["secret_leak_detected"] is False and row["dispatch_error"] is None
                    and type(row["exit_code"]) is int and row["exit_code"] >= 0
                    and type(row["mutation"]) is bool, "incomplete or uncertain host execution")
            require(type(row["started_unix_ns"]) is int and row["started_unix_ns"] >= previous
                    and type(row["elapsed_ns"]) is int and 0 <= row["elapsed_ns"] <= 130 * 10**9,
                    "unbounded or reordered command timing")
            previous = row["started_unix_ns"]
            require(intent["host_outcome"] == "inflight" and intent["effects_uncertain"] is True
                    and intent["exit_code"] is None and intent["timed_out"] is False and intent["interrupted"] is False,
                    "invalid pre-dispatch intent")
            for key in ("index", "executable", "argv", "argv0", "environment", "started_unix_ns", "mutation", "max_stream_bytes"):
                require(intent[key] == row[key], "intent/terminal command changed")
            require(type(row["max_stream_bytes"]) is int and 1 <= row["max_stream_bytes"] <= MAX, "output bound changed")
            for stream in ("stdout", "stderr"):
                require(row[stream] == stem + "." + stream, "redirected raw stream")
                data = self.files[row[stream]]
                require(len(data) <= row["max_stream_bytes"] and row["observed_bytes"][stream] == len(data)
                        and row["retained_observed_" + stream + "_bytes"] == len(data), "raw byte count mismatch")
                for key in (stream + "_sha256", "raw_" + stream + "_sha256", "retained_observed_" + stream + "_sha256"):
                    require(row[key] == sha(data), "raw hash mismatch")
                row["_" + stream] = data
            ack_name = stem + ".acknowledgement.json"
            uncertain = row["mutation"] and row["exit_code"] != 0
            require(row["effects_uncertain"] is uncertain, "failed mutation uncertainty erased")
            if uncertain:
                ack = decode(self.files[ack_name])
                require(set(ack) == {"command_index", "assertion", "terminal_receipt_sha256", "effects_uncertain"}
                        and ack["command_index"] == index and ack["effects_uncertain"] is False
                        and isinstance(ack["assertion"], str) and bool(ack["assertion"])
                        and ack["terminal_receipt_sha256"] == sha(self.files[stem + ".json"]), "invalid negative acknowledgement")
                expected_files.add(ack_name)
            row["_args"] = row["argv"][5:]
            self.rows.append(row)
        require(set(self.files) == expected_files, "unrecognized evidence or receipt files")
        require([item["recipe"] for item in result["observations"]] == list(RECIPES), "exact nine recipes required")
        self.fixture = None

    def take(self, args, code=0, mutation=False, env=None):
        require(self.i < len(self.rows), "missing command")
        row = self.rows[self.i]
        self.i += 1
        require(row["_args"] == args and row["mutation"] is mutation and row["environment"] == (env or {}),
                f"command {self.i}: unexpected argv/mutation/environment")
        require(row["exit_code"] == code if code is not None else row["exit_code"] > 0,
                f"command {self.i}: wrong exit")
        if row["mutation"] and row["exit_code"]:
            self.acknowledged.add(row["index"])
        return row

    def json(self, args):
        return decode(self.take(args)["_stdout"])

    def guard(self):
        contexts = self.json(["context", "inspect", self.scope["docker_context"]])
        require(len(contexts) == 1 and contexts[0]["Name"] == self.scope["docker_context"], "foreign context")
        endpoint = contexts[0]["Endpoints"]["docker"]
        require(endpoint["Host"] == self.scope["docker_endpoint"] and not endpoint.get("SkipTLSVerify", False), "foreign endpoint")
        info = self.json(["info", "--format", "{{json .}}"])
        require(info["ID"] == self.scope["engine_id"] and info["OSType"] == "linux"
                and info["Architecture"] in {"arm64", "aarch64"} and info["DefaultRuntime"] == "youki"
                and info["Runtimes"].get("youki", {}).get("path") == "/mnt/linux-bin/youki", "foreign Engine/runtime")
        inert = {"runc", "io.containerd.runc.v2"} & set(info["Runtimes"])
        require(set(info["Runtimes"]) <= {"runc", "io.containerd.runc.v2", "youki", "io.containerd.youki.v2"}
                and all(info["Runtimes"][name] == {"path": "runc"} for name in inert)
                and (not inert or "runtime_evidence" in self.inputs), "alternate runtime lacks authenticated inventory")

    def inventory(self, project, track=True):
        inventory = {}
        for kind, args in (("container", ["container", "ls", "--all", "--quiet", "--no-trunc"]),
                           ("network", ["network", "ls", "--quiet", "--no-trunc"]),
                           ("volume", ["volume", "ls", "--quiet"])):
            ids = self.take(args + ["--filter", "label=com.docker.compose.project=" + project])["_stdout"].decode().split()
            require(len(ids) == len(set(ids)), "duplicate inventory IDs")
            items = self.json([kind, "inspect", *ids]) if ids else []
            actual = []
            for item in items:
                identity = item["Name"] if kind == "volume" else item["Id"]
                require(kind == "volume" or hex64(identity), "invalid immutable resource ID")
                labels = item["Config"].get("Labels", {}) if kind == "container" else item.get("Labels", {})
                require(labels.get("com.docker.compose.project") == project
                        and labels.get("dev.vz.fixture-owner") == self.owner, "foreign resource ownership")
                if kind == "container":
                    require(item["Image"] == self.inputs["images"]["compose"]["id"], "wrong container image")
                actual.append(identity)
            require(len(actual) == len(set(actual)) and set(actual) == set(ids), "inspect/list identity mismatch")
            inventory[kind] = items
            if track:
                self.projects[project][kind].update(actual)
        return inventory

    def names(self, project, inventory):
        for kind, suffixes in (("volume", ("state",)), ("network", ("frontend", "backend", "isolated"))):
            names = {project + "_" + suffix for suffix in suffixes}
            existing = set(self.take([kind, "ls", "--format", "{{.Name}}"])["_stdout"].decode().splitlines()) & names
            owned = {item["Name"] for item in inventory[kind]}
            require(owned <= names and existing == owned, "unlabelled exact-name collision")
        for item in inventory["container"]:
            require(all(mount["Type"] == "volume" and mount["Name"] in {x["Name"] for x in inventory["volume"]}
                        for mount in item.get("Mounts", [])), "foreign volume mount")
        network_bindings(inventory)

    def new_project(self, suffix):
        project = self.owner + "-" + suffix
        self.guard()
        inventory = self.inventory(project, track=False)
        require(not any(inventory.values()), "project adoption")
        self.names(project, inventory)
        self.projects[project] = {kind: set() for kind in inventory}
        return project

    def compose(self, project, args, code=0, blocked=False, mutation=True):
        self.guard()
        row = self.rows[self.i]
        actual = row["_args"]
        require(actual[:4] == ["compose", "--project-name", project, "--file"] and len(actual) > 6, "wrong Compose scope")
        fixture = Path(actual[4]).parent.parent
        if self.fixture is None:
            require(fixture_digest(fixture) == self.inputs["fixture_sha256"], "fixture digest mismatch")
            self.fixture = fixture
            expected = {kind: {name: {"labels": {"dev.vz.fixture-owner": self.owner}} for name in names}
                        for kind, names in (("services", ROLES | {"failure"}), ("networks", {"frontend", "backend", "isolated"}), ("volumes", {"state"}))}
            require(decode(self.files["compose-owner.json"]) == expected, "changed ownership overlay")
        require(fixture == self.fixture, "mixed fixture roots")
        prefix = ["compose", "--project-name", project, "--file", str(fixture / "compose/compose.json"),
                  "--file", str(self.directory / "compose-owner.json")]
        if blocked:
            prefix += ["--file", str(fixture / "compose/blocked-health.json")]
        return self.take(prefix + args, code=code, mutation=mutation,
                         env={"FIXTURE_IMAGE": self.inputs["images"]["compose"]["id"], "FIXTURE_OWNER": self.owner})

    @staticmethod
    def services(inventory, roles=ROLES, workers=1):
        result = {}
        for item in inventory["container"]:
            result.setdefault(item["Config"]["Labels"]["com.docker.compose.service"], []).append(item)
        require(set(result) == roles and all(len(items) == (workers if role == "worker" else 1)
                                            for role, items in result.items()), "wrong exact service inventory")
        return result

    def execute(self, item, args):
        self.guard()
        return self.take(["exec", item["Id"], *args], mutation=True)

    @contextmanager
    def observation(self, name):
        position = RECIPES.index(name)
        item = self.result["observations"][position]
        require(set(item) == {"recipe", "related_scenario_ids", "first_command", "last_command", "outcome", "assertions"}
                and type(item["first_command"]) is int and type(item["last_command"]) is int
                and item["first_command"] == self.i + 1 and item["related_scenario_ids"] == RELATED[position]
                and item["outcome"] == "fixture_assertions_passed" and isinstance(item["assertions"], list)
                and item["assertions"] and all(isinstance(x, str) and x for x in item["assertions"]), "invalid recipe span/claims")
        yield
        require(item["last_command"] == self.i, "unconsumed or misattributed recipe commands")

    def events(self, project, services, blocked=False):
        args = self.rows[self.i]["_args"]
        require(len(args) == 9 and args[0:2] == ["events", "--since"] and args[3] == "--until"
                and args[5:] == ["--filter", "label=com.docker.compose.project=" + project, "--format", "{{json .}}"]
                and args[2].isdigit() and args[4].isdigit() and int(args[2]) < int(args[4]), "invalid event window")
        row = self.take(args)
        ids = {role: items[0]["Id"] for role, items in services.items()}
        times = {}
        for event in map(decode, row["_stdout"].splitlines()):
            if event["Type"] != "container":
                continue
            actor, stamp = event["Actor"], event["timeNano"]
            require(actor["ID"] in ids.values() and actor["Attributes"]["com.docker.compose.project"] == project
                    and type(stamp) is int and int(args[2]) * 10**9 <= stamp <= int(args[4]) * 10**9,
                    "foreign or out-of-window event")
            times.setdefault((actor["ID"], event["Action"]), []).append(stamp)
        def one(role, action):
            values = times.get((ids[role], action), [])
            require(len(values) == 1, "missing/repeated positive event")
            return values[0]
        if blocked:
            for role in ROLES:
                one(role, "create")
            require(one("db", "create") < one("db", "start") < one("db", "health_status: unhealthy"), "invalid blocked health order")
            require(not any(times.get((ids[role], action)) for role in ("api", "worker")
                            for action in ("start", "die", "destroy", "restart")), "blocked dependent ran")
        else:
            require(one("db", "health_status: healthy") < one("api", "start")
                    and one("api", "health_status: healthy") < one("worker", "start"), "dependency health ordering violated")

    def run(self):
        self.guard()
        images = {pin["reference"]: pin for pin in self.inputs["images"].values()}
        require(len(images) == 2, "distinct base and Compose image pins required")
        for _ in range(2):
            args = self.rows[self.i]["_args"]
            require(len(args) == 3 and args[:2] == ["image", "inspect"] and args[2] in images,
                    "missing/repeated image pin observation")
            pin = images.pop(args[2])
            items = self.json(["image", "inspect", pin["reference"]])
            require(len(items) == 1 and items[0]["Id"] == pin["id"] and items[0]["Os"] == "linux"
                    and items[0]["Architecture"] == "arm64" and ("@" not in pin["reference"] or
                    pin["reference"] in items[0].get("RepoDigests", [])), "unverified image pin")
        project = self.new_project("compose")
        with self.observation("compose-create"):
            self.compose(project, ["create", "--no-build", "--pull", "never"])
            created = self.services(self.inventory(project))
            require(all(x[0]["State"]["Status"] == "created" and not x[0]["State"]["Running"] for x in created.values()), "create started service")
        with self.observation("compose-up-order"):
            self.compose(project, UP)
            ready = self.services(self.inventory(project))
            require(all(ready[role][0]["Id"] == created[role][0]["Id"] and ready[role][0]["State"]["Health"]["Status"] == "healthy"
                        and ready[role][0]["State"]["Running"] for role in ROLES), "wrong healthy service identity")
            self.events(project, ready)
        with self.observation("compose-logs"):
            # Follow can only terminate because every container was stopped
            # first; replay demands that stopped inventory before the follow.
            self.compose(project, ["stop"])
            stopped = self.services(self.inventory(project))
            require(all(stopped[role][0]["Id"] == ready[role][0]["Id"] and stopped[role][0]["State"]["Status"] == "exited"
                        and stopped[role][0]["State"]["Running"] is False for role in ROLES), "follow precondition: services not stopped")
            row = self.compose(project, LOGS, mutation=False)
            self.logs(project, row, {role: stopped[role][0] for role in ROLES})
            self.compose(project, UP)
            restarted = self.services(self.inventory(project))
            require(all(restarted[role][0]["Id"] == ready[role][0]["Id"] and restarted[role][0]["State"]["Running"]
                        and restarted[role][0]["State"]["Health"]["Status"] == "healthy" for role in ROLES),
                    "restart after follow changed identity or health")
        with self.observation("compose-exec"):
            row = self.compose(project, ["exec", "--no-TTY", "api", "python3", "/fixture/service.py", "exec"], code=37)
            require(row["_stdout"] == f"vz04|api|{self.owner}|exec-stdout\n".encode()
                    and row["_stderr"] == f"vz04|api|{self.owner}|exec-stderr\n".encode(), "exec stream mismatch")
        self.networks(project)
        self.persistence(project)
        self.scale(project)
        with self.observation("compose-blocked-health"):
            blocked = self.new_project("blocked")
            self.compose(blocked, UP, code=None, blocked=True)
            services = self.services(self.inventory(blocked))
            require(services["db"][0]["State"]["Health"]["Status"] == "unhealthy"
                    and all(services[role][0]["State"]["Status"] == "created" and not services[role][0]["State"]["Running"]
                            for role in ("api", "worker")), "blocked state invalid")
            self.events(blocked, services, blocked=True)
        with self.observation("compose-failure"):
            failed = self.new_project("failure")
            self.compose(failed, ["--profile", "failure", "up", "--no-build", "--pull", "never", "--abort-on-container-exit", "--exit-code-from", "failure"], code=37)
            services = self.services(self.inventory(failed), ROLES | {"failure"})
            require(all(not x[0]["State"]["Running"] and x[0]["State"]["Status"] in {"created", "exited"} for x in services.values()), "abort left running service")
            job = services["failure"][0]
            require(job["State"]["Status"] == "exited" and job["State"]["ExitCode"] == 37, "wrong failure job exit")
            row = self.take(["logs", job["Id"]])
            require(row["_stdout"] == f"vz04|failure|{self.owner}|exit-37\n".encode() and not row["_stderr"], "wrong failure logs")
        for owned in self.projects:
            self.guard()
            before_down = self.inventory(owned)
            self.names(owned, before_down)
            self.compose(owned, ["--profile", "failure", "down", "--volumes", "--remove-orphans"])
            remaining = self.inventory(owned)
            require(not any(remaining.values()), "resources remain after exact-owned down")
            self.names(owned, remaining)
            for item in before_down["container"]:
                identity = item["Id"]
                row = self.take(["container", "inspect", identity], code=1)
                require(decode(row["_stdout"]) == [] and row["_stderr"].strip() in {
                    f"Error response from daemon: No such container: {identity}".encode(),
                    f"Error: No such container: {identity}".encode(),
                    f"Error: No such object: {identity}".encode()}, "known container absence after down unproven")
        require(self.i == len(self.rows), "unconsumed extra commands")
        require(self.result["owned_projects"] == {p: {k: sorted(v) for k, v in kinds.items()} for p, kinds in self.projects.items()}, "owned resource receipt mismatch")
        require(self.acknowledged == {row["index"] for row in self.rows if row["effects_uncertain"]}, "unreconciled or extra negative mutation")
        return {"schema_version": 1, "kind": "installed_compose_raw_evidence", "outcome": "fixture_assertions_passed",
                "scope": self.scope, "recipes_validated": list(RECIPES), "command_count": self.i,
                "owned_projects": self.result["owned_projects"], "compatibility_certified": False,
                "release_scenarios_passed": []}

    def logs(self, project, row, containers):
        """Every followed line must belong to an exact owned container and carry this owner's token."""
        stdout = row["_stdout"]
        require(not row["_stderr"] and 0 < len(stdout) <= MAX and stdout.endswith(b"\n"),
                "Compose logs diagnostics, empty or unterminated output")
        names = {}
        for role, item in containers.items():
            require(item["Name"] == f"/{project}-{role}-1", "unexpected owned container name")
            # Compose prints the name without the project; accept the exact full name too.
            names[f"{role}-1".encode()] = names[f"{project}-{role}-1".encode()] = role
        observed = {role: [] for role in ROLES}
        for line in stdout.split(b"\n")[:-1]:
            match = LOG_LINE.fullmatch(line)
            require(match is not None and match[1] in names, "log line not attributable to an exact owned container")
            observed[names[match[1]]].append(match[2])
        for role, events in LOG_EVENTS.items():
            require(observed[role] == [f"vz04|{role}|{self.owner}|{event}".encode() for event in events],
                    "per-service log bytes or order differ")
        assertions = self.result["observations"][RECIPES.index("compose-logs")]["assertions"]
        require("compose logs raw stdout sha256 " + sha(stdout) in assertions, "raw stdout digest claim differs")

    def networks(self, project):
        with self.observation("compose-network-paths"):
            services = {role: values[0] for role, values in self.services(self.inventory(project)).items()}
            memberships = {"db": {"backend"}, "api": {"frontend", "backend"}, "worker": {"frontend"}, "isolated": {"isolated"}}
            for role, expected in memberships.items():
                require(set(services[role]["NetworkSettings"]["Networks"]) == {project + "_" + name for name in expected}, "network membership mismatch")
            for source, destination in (("worker", "api"), ("api", "db")):
                row = self.execute(services[source], ["python3", "/fixture/service.py", "probe", f"http://{destination}:8080/value"])
                require(row["_stdout"] == f"vz04|db|{self.owner}|persisted\n".encode(), "allowed path wrong payload")
            for source, destination in (("worker", "db"), ("isolated", "api"), ("isolated", "db")):
                addresses = [destination, *[net["IPAddress"] for net in services[destination]["NetworkSettings"]["Networks"].values()]]
                require(all(addresses) and len(set(addresses)) == len(addresses), "missing/duplicate destination address")
                for address in addresses:
                    def controls():
                        for role, target in ((source, "127.0.0.1"), (destination, address)):
                            row = self.execute(services[role], ["python3", "/fixture/service.py", "probe", f"http://{target}:8080/health"])
                            require(row["_stdout"] == f"vz04|{role}|{self.owner}|ready\n".encode() and not row["_stderr"], "missing paired live network control")
                    controls()
                    url = f"http://{address}:8080/health"
                    row = self.execute(services[source], ["python3", "/fixture/service.py", "transport", url])
                    value = decode(row["_stdout"])
                    allowed = {"timeout", "network_unreachable", "connection_refused"} | ({"dns_failure"} if address == destination else set())
                    require(not row["_stderr"] and len(row["_stdout"]) <= 2048 and set(value) == {"schema_version", "url", "outcome", "status", "errno", "exception"}
                            and type(value["schema_version"]) is int and value["schema_version"] == 1 and value["url"] == url
                            and value["outcome"] in allowed and value["status"] is None
                            and (value["errno"] is None or type(value["errno"]) is int)
                            and isinstance(value["exception"], str) and 0 < len(value["exception"]) <= 64, "unproven network denial")
                    typed = {"dns_failure": ({"gaierror"}, {-2, -3}),
                             "timeout": ({"TimeoutError", "OSError"}, {None, 110}),
                             "connection_refused": ({"ConnectionRefusedError", "OSError"}, {111}),
                             "network_unreachable": ({"OSError"}, {101, 113})}
                    classes, numbers = typed[value["outcome"]]
                    require(value["exception"] in classes and value["errno"] in numbers,
                            "network outcome does not match observed Linux exception/errno")
                    controls()

    def persistence(self, project):
        with self.observation("compose-volume-persistence"):
            before = self.inventory(project)
            db = self.services(before)["db"][0]
            mounts = [mount for mount in db["Mounts"] if mount["Destination"] == "/data"]
            require(len(mounts) == 1 and mounts[0]["Type"] == "volume" and mounts[0]["RW"] is True and mounts[0]["Name"] == project + "_state", "wrong persistent mount")
            marker = "/data/host-persistence-" + self.owner + ".txt"
            payload = f"vz04|host-written|{self.owner}|{self.inputs['run_id']}|persisted\n"
            self.execute(db, ["python3", "-c", WRITE, marker, payload])
            require(self.execute(db, ["python3", "-c", READ, marker])["_stdout"] == payload.encode(), "marker not written")
            self.compose(project, ["stop"])
            self.compose(project, UP)
            after = self.inventory(project)
            for kind in before:
                key = "Name" if kind == "volume" else "Id"
                require({x[key] for x in before[kind]} == {x[key] for x in after[kind]}, "stop/up changed resource identity")
            require(self.execute(self.services(after)["db"][0], ["python3", "-c", READ, marker])["_stdout"] == payload.encode(), "host marker lost on restart")

    def scale(self, project):
        with self.observation("compose-scale"):
            def args(number):
                return ["up", "--detach", "--no-build", "--pull", "never", "--scale", f"worker={number}", "--wait", "--wait-timeout", "30"]
            self.compose(project, args(3))
            workers = self.services(self.inventory(project), workers=3)["worker"]
            before = {item["Id"] for item in workers}
            require(len(before) == 3, "replica identity collision")
            for item in workers:
                row = self.execute(item, ["python3", "/fixture/service.py", "probe", "http://127.0.0.1:8080/identity"])
                require(decode(row["_stdout"]) == {"owner": self.owner, "role": "worker", "hostname": item["Config"]["Hostname"]}, "replica identity payload mismatch")
            self.compose(project, args(1))
            survivor = self.services(self.inventory(project))["worker"][0]["Id"]
            require(survivor in before, "scale replaced survivor")
            for removed in sorted(before - {survivor}):
                row = self.take(["container", "inspect", removed], code=1)
                require(("No such container: " + removed).encode() in row["_stderr"] or ("No such object: " + removed).encode() in row["_stderr"], "replica absence unproven")


def validate(directory: Path, expected_inputs: dict) -> dict:
    """Reject incomplete/foreign evidence; replay all nine recipes and cleanup."""
    try:
        return Replay(directory, expected_inputs).run()
    except (KeyError, IndexError, TypeError, OSError, UnicodeError, json.JSONDecodeError) as error:
        raise Invalid(f"malformed or unavailable Compose evidence: {error}") from error
