"""Independent raw replay of the five installed Buildx DEV recipes.

The parent authenticates expected_inputs, builder provisioning and eventual
Machine deletion. This module never executes Docker and cannot certify those
authorities, builder/cache cleanup, secret absence in image blobs, or parity.
"""
from contextlib import contextmanager
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from linux_docker_compose_evidence import (
    Invalid, MAX, Replay as ComposeReplay, decode, fixture_digest, hex64,
    manifest, read, require, runtime_proof, sha, unique,
)

RECIPES = ("build-multi-stage", "build-cache-reuse", "build-arguments", "build-cache-mount", "build-secret-mount")
RELATED = (["docker.build.multi_stage", "docker.build.output_export"], ["docker.build.cache_reuse"],
           ["docker.build.build_arguments"], ["docker.build.cache_isolation"], ["docker.build.secrets"])
REMAINING = [
    "All 63 full release scenarios require aggregate certification and physical evidence",
    "Immutable ownership/release/runtime attestation and sibling isolation",
    "OCI layer/image digests, secret scans across all image/cache blobs",
    "Fresh-builder exported-cache imports and cross-Machine cache observations",
    "Owned SSH server/agent provisioning and positive/negative SSH builds",
    "Compose logs-follow, Machine-level persistence and unrelated cleanup decoys",
    "Complete registry/container/storage/network/pressure/recovery lane",
]
EXPORTS = {"alpha": "payload.txt", "alpha-reuse": "payload.txt", "beta": "payload.txt",
           "cache-cold": "cache.txt", "cache-warm": "cache.txt", "secret": "secret.txt"}


def progress_timestamp(value):
    """Validate RFC3339 nanoseconds using Python 3.9's microsecond parser."""
    require(isinstance(value, str), "invalid vertex timestamp")
    matched = re.fullmatch(
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
        r"(?:\.([0-9]{1,9}))?(Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])", value)
    require(matched is not None, "invalid vertex timestamp")
    calendar, fraction, offset = matched.groups()
    # Normalize only after validating the entire original value. Preserve its
    # offset, and never mutate the raw vertex or its retained timestamp bytes.
    normalized = calendar + ("." + fraction[:6].ljust(6, "0") if fraction else "")
    normalized += "+00:00" if offset == "Z" else offset
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as error:
        raise Invalid("invalid vertex timestamp calendar") from error


def progress(raw, secret_dockerfile=None):
    """Pinned Buildx SolveStatus JSON prefix; no arbitrary diagnostic fallback."""
    vertices, logs, trailer = [], [], []
    lines = raw.decode().splitlines()
    require(0 < len(raw) <= MAX and len(lines) <= 20000 and all(lines), "unbounded/empty BuildKit progress")
    def invalid_constant(_value):
        raise Invalid("nonfinite BuildKit JSON constant")
    for line in lines:
        if trailer or not line.startswith("{"):
            trailer.append(line)
            continue
        batch = json.loads(line, object_pairs_hook=unique, parse_constant=invalid_constant)
        require(isinstance(batch, dict) and set(batch) <= {"vertexes", "statuses", "logs", "warnings"}
                and all(isinstance(value, list) and all(isinstance(member, dict) for member in value)
                        for value in batch.values()), "not a batched SolveStatus record")
        for vertex in batch.get("vertexes", []):
            require(isinstance(vertex, dict), "invalid vertex record")
            require(isinstance(vertex.get("digest"), str) and re.fullmatch(r"sha256:[0-9a-f]{64}", vertex["digest"])
                    and isinstance(vertex.get("name"), str) and type(vertex.get("cached", False)) is bool
                    and isinstance(vertex.get("error", ""), str), "invalid vertex identity/cache/error")
            for key in ("started", "completed"):
                if key in vertex:
                    progress_timestamp(vertex[key])
            vertices.append(vertex)
        for log in batch.get("logs", []):
            require(isinstance(log, dict) and isinstance(log.get("data"), str), "invalid progress log data")
            logs.append(base64.b64decode(log["data"], validate=True))
    require(vertices, "missing BuildKit vertices")
    if secret_dockerfile is None:
        require(not trailer, "unstructured successful-build output")
    else:
        lines = secret_dockerfile.decode().splitlines()
        wanted = "RUN --network=none --mount=type=secret,id=fixture,required=true python3 /fixture/tools.py secret"
        numbers = [i for i, line in enumerate(lines, 1) if line == wanted]
        require(len(numbers) == 1, "ambiguous required-secret Dockerfile RUN")
        number = numbers[0]
        terminal = [v for v in vertices if re.fullmatch(r"\[build [0-9]+/[0-9]+\] " + re.escape(wanted), v["name"])
                    and v.get("completed")]
        require(len(terminal) == 1 and terminal[0].get("error") == "secret fixture: not found"
                and terminal[0].get("cached", False) is False and isinstance(terminal[0].get("digest"), str)
                and re.fullmatch(r"sha256:[0-9a-f]{64}", terminal[0]["digest"]), "unproven required-secret vertex error")
        require(all(not v.get("error") or v is terminal[0] or v["error"] in {"context canceled", "context cancelled"}
                    for v in vertices), "unrelated concurrent BuildKit failure")
        footer = "ERROR: failed to build: failed to solve: secret fixture: not found"
        excerpt = [f"Dockerfile.secret:{number}", "--------------------"]
        for n in range(max(1, number - 2), min(len(lines), number + 2) + 1):
            excerpt.append(f"{n:4} | " + (">>> " if n == number else "    ") + lines[n - 1])
        excerpt += ["--------------------", footer]
        require(trailer in ([footer], excerpt), "foreign or malformed Buildx failure trailer")
    return vertices, b"".join(logs)


def progress_ns(value):
    parsed = progress_timestamp(value)
    elapsed = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    fraction = re.search(r"\.([0-9]+)", value)
    tail = (fraction[1] if fraction else "").ljust(9, "0")[6:9]
    return (elapsed.days * 86400 + elapsed.seconds) * 10**9 + parsed.microsecond * 1000 + int(tail)


def payload_graph(raw, base_reference, cached):
    """Derive operation identity independently of solve-local progress digests.

    BuildKit 0.19 puts LocalUniqueID in each local source operation. Descendant
    progress digests are therefore not content cache keys. The caller must also
    authenticate the fixture and exact command: graph labels alone do not bind
    source bytes or build arguments.
    """
    reference, separator, pin = base_reference.partition("@sha256:")
    require(separator and hex64(pin), "unpinned payload base")
    if "/" not in reference:
        reference = "docker.io/library/" + reference
    elif not ("." in reference.split("/")[0] or ":" in reference.split("/")[0]
              or reference.split("/")[0] == "localhost"):
        reference = "docker.io/" + reference
    if reference.startswith("index.docker.io/"):
        reference = "docker.io/" + reference[len("index.docker.io/"):]
    if reference.startswith("docker.io/") and reference.count("/") == 1:
        reference = "docker.io/library/" + reference[len("docker.io/"):]
    reference += "@sha256:" + pin
    names = {
        "base": "[build 1/3] FROM " + reference,
        "context": "[internal] load build context",
        "copy": "[build 2/3] COPY tools.py input.txt intermediate-canary.txt /fixture/",
        "run": "[build 3/3] RUN --network=none python3 /fixture/tools.py payload",
        "output": "[output 1/1] COPY --from=build /out/payload.txt /payload.txt",
    }
    auxiliary = {"[internal] load build definition from Dockerfile", "[internal] load .dockerignore",
                 "[internal] load metadata for " + reference, "exporting to client directory"}
    vertices, _ = progress(raw)
    grouped, name_ids = {}, {}
    for vertex in vertices:
        identity, name = vertex["digest"], vertex["name"]
        require(name in set(names.values()) | auxiliary and not vertex.get("error"), "foreign payload operation")
        edges = vertex.get("inputs", [])
        require(isinstance(edges, list) and all(isinstance(x, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", x)
                                               for x in edges) and len(edges) == len(set(edges)), "invalid payload edges")
        prior = grouped.setdefault(identity, [])
        require(not prior or (prior[0]["name"] == name and prior[0].get("inputs", []) == edges), "payload identity drift")
        require(name not in name_ids or name_ids[name] == identity, "duplicate payload role")
        name_ids[name] = identity
        prior.append(vertex)
        if "completed" in vertex:
            require("started" in vertex and progress_ns(vertex["started"]) <= progress_ns(vertex["completed"]),
                    "invalid payload operation lifetime")
    require(set(names.values()) <= set(name_ids), "missing payload graph role")
    ids = {role: name_ids[name] for role, name in names.items()}
    edges = {"base": [], "context": [], "copy": ["base", "context"], "run": ["copy"], "output": ["run"]}
    terminal = {}
    for role, identity in ids.items():
        rows = grouped[identity]
        require(rows[0].get("inputs", []) == [ids[x] for x in edges[role]], "disconnected payload graph")
        done = [x for x in rows if "completed" in x]
        require(done, "payload role never completed")
        # Source progress includes repeated completions for download/resolve.
        # Executable/copy operations have exactly one terminal record.
        if role in ("copy", "run", "output"):
            require(len(done) == 1 and rows[-1] is done[0], "duplicate or stale payload terminal")
            starts = {x["started"] for x in rows if "started" in x}
            require(len(starts) == 1, "payload process lifetime drift")
            seen_cached = False
            for row in rows:
                require(not seen_cached or row.get("cached", False), "payload cache state regressed")
                seen_cached = seen_cached or row.get("cached", False)
        terminal[role] = done[-1]
    for name in auxiliary & set(name_ids):
        rows = grouped[name_ids[name]]
        require(not rows[0].get("inputs", []) and any("completed" in x for x in rows), "invalid auxiliary payload node")
    for role in ("copy", "run", "output"):
        require(all(max(progress_ns(x["completed"]) for x in grouped[ids[parent]] if "completed" in x)
                    <= progress_ns(terminal[role]["started"])
                    for parent in edges[role]), "payload dependency completed after consumer started")
    run = terminal["run"]
    require(run.get("cached", False) is cached, "payload cache state differs")
    if cached:
        require(all(terminal[role].get("cached", False) is True for role in ("copy", "output")),
                "cache reuse did not preserve payload chain")
    frames = []
    for line in raw.splitlines():
        batch = decode(line)
        for frame in batch.get("logs", []):
            require(set(frame) == {"vertex", "stream", "data", "timestamp"} and frame["vertex"] == ids["run"]
                    and type(frame["stream"]) is int and frame["stream"] == 1, "unbound payload execution log")
            require(progress_ns(run["started"]) <= progress_ns(frame["timestamp"]) <= progress_ns(run["completed"]),
                    "stale payload execution log")
            frames.append(base64.b64decode(frame["data"], validate=True))
    require(not frames if cached else b"".join(frames) == b"vz04-payload-step-executed\n",
            "payload execution marker missing, duplicated or replayed from cache")
    return {"normalized": [(role, names[role], edges[role]) for role in names], "base_digest": ids["base"],
            "ids": ids, "started_ns": progress_ns(run["started"]), "completed_ns": progress_ns(run["completed"]),
            "solve_started_ns": min(progress_ns(x["started"]) for x in vertices if "started" in x),
            "solve_last_observed_ns": max(progress_ns(x[key]) for x in vertices for key in ("started", "completed") if key in x)}


class Replay:
    # Shared read-only routing semantics, not the driver's assertion helpers.
    take = ComposeReplay.take
    json = ComposeReplay.json
    guard = ComposeReplay.guard

    def __init__(self, directory, inputs):
        self.directory, self.inputs = directory, inputs
        self.files = manifest(directory)
        require(decode(self.files["inputs.json"]) == inputs, "foreign input object")
        runtime_proof(inputs)
        self.result = result = decode(self.files["result.json"])
        require(set(result) == {"schema_version", "kind", "suite", "run_id", "scope", "release_sha256", "fixture_sha256",
                               "compatibility_certified", "release_scenarios_passed", "test_case_retries", "outcome", "failure",
                               "cleanup_errors", "observations", "command_count", "owned_projects", "remaining"}, "unknown result fields")
        require(type(result["schema_version"]) is int and result["schema_version"] == 1
                and result["kind"] == "docker_host_fixture_subset" and result["suite"] == "build"
                and result["compatibility_certified"] is False and result["release_scenarios_passed"] == []
                and type(result["test_case_retries"]) is int and result["test_case_retries"] == 0
                and result["outcome"] == "fixture_assertions_passed" and result["failure"] is None
                and result["cleanup_errors"] == [] and result["owned_projects"] == {}
                and result["remaining"] == REMAINING, "failed, retried, wrong-suite or overclaimed build subset")
        for key in ("scope", "run_id", "release_sha256", "fixture_sha256"):
            require(result[key] == inputs[key], "result owner or input digest mismatch")
        require(hex64(inputs["release_sha256"]) and hex64(inputs["fixture_sha256"]), "invalid digest")
        self.scope = inputs["scope"]
        require(set(self.scope) == {"project_id", "environment_id", "machine_id", "machine_incarnation", "runtime_identity",
                                   "docker_context", "docker_endpoint", "engine_id"}
                and all(isinstance(x, str) and x for x in self.scope.values()), "incomplete owner/runtime scope")
        self.builder = inputs["builder"]
        require(set(self.builder) == {"name", "node", "container_id", "image_id"}
                and all(isinstance(self.builder[k], str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", self.builder[k])
                        for k in ("name", "node")) and hex64(self.builder["container_id"])
                and re.fullmatch(r"sha256:[0-9a-f]{64}", self.builder["image_id"]), "invalid builder identity")
        self.owner = "vz04-" + sha(json.dumps([inputs["run_id"], self.scope], sort_keys=True).encode())[:24]
        self.rows, self.acknowledged, self.i, self.fixture = [], set(), 0, None
        self.builder_process = None
        require(type(result["command_count"]) is int and result["command_count"] == 39,
                "expected four initial observations and seven five-command builds")
        expected = {"inputs.json", "result.json", "compose-owner.json"}
        expected.update("export-" + suffix + "/" + name for suffix, name in EXPORTS.items())
        previous = 0
        for index in range(1, result["command_count"] + 1):
            stem = f"command-{index:05d}"
            expected.update(stem + ext for ext in (".json", ".intent.json", ".stdout", ".stderr"))
            row, intent = (decode(self.files[stem + ext]) for ext in (".json", ".intent.json"))
            require(type(row["index"]) is int and row["index"] == index and row["argv0"] == "docker"
                    and row["argv"][:5] == ["docker", "--config", inputs["docker_config"], "--context", self.scope["docker_context"]]
                    and row["executable"] == inputs["clients"]["docker"]["path"], "foreign command routing")
            require(row["host_outcome"] == "exited" and row["capture_complete"] is True
                    and row["raw_streams_retained"] is True and row["timed_out"] is False
                    and row["interrupted"] is False and row["output_limit_exceeded"] is False
                    and row["secret_leak_detected"] is False and row["dispatch_error"] is None
                    and type(row["exit_code"]) is int and row["exit_code"] >= 0
                    and type(row["mutation"]) is bool, "uncertain/incomplete host execution")
            require(type(row["started_unix_ns"]) is int and row["started_unix_ns"] >= previous
                    and type(row["elapsed_ns"]) is int and 0 <= row["elapsed_ns"] <= 310 * 10**9,
                    "unbounded/reordered command timing")
            previous = row["started_unix_ns"]
            require(intent["host_outcome"] == "inflight" and intent["effects_uncertain"] is True
                    and intent["exit_code"] is None and intent["timed_out"] is False and intent["interrupted"] is False,
                    "invalid pre-dispatch intent")
            for key in ("index", "executable", "argv", "argv0", "environment", "started_unix_ns", "mutation", "max_stream_bytes"):
                require(intent[key] == row[key], "intent/terminal drift")
            require(type(row["max_stream_bytes"]) is int and 1 <= row["max_stream_bytes"] <= MAX, "unbounded output")
            for stream in ("stdout", "stderr"):
                require(row[stream] == stem + "." + stream, "redirected stream")
                data = self.files[row[stream]]
                require(len(data) <= row["max_stream_bytes"] and row["observed_bytes"][stream] == len(data)
                        and row["retained_observed_" + stream + "_bytes"] == len(data), "raw byte mismatch")
                for key in (stream + "_sha256", "raw_" + stream + "_sha256", "retained_observed_" + stream + "_sha256"):
                    require(row[key] == sha(data), "raw hash mismatch")
                row["_" + stream] = data
            uncertain = row["mutation"] and row["exit_code"] != 0
            require(row["effects_uncertain"] is uncertain, "mutation uncertainty erased")
            if uncertain:
                ack_name = stem + ".acknowledgement.json"
                expected.add(ack_name)
                ack = decode(self.files[ack_name])
                require(set(ack) == {"command_index", "assertion", "terminal_receipt_sha256", "effects_uncertain"}
                        and type(ack["command_index"]) is int and ack["command_index"] == index
                        and ack["effects_uncertain"] is False
                        and ack["assertion"] == "terminal BuildKit required fixture secret mount error"
                        and ack["terminal_receipt_sha256"] == sha(self.files[stem + ".json"]), "unbound negative acknowledgement")
            row["_args"] = row["argv"][5:]
            self.rows.append(row)
        require(set(self.files) == expected, "missing or unexpected receipt/export files")
        require([x["recipe"] for x in result["observations"]] == list(RECIPES), "exact five recipes required")

    @contextmanager
    def observation(self, index):
        item = self.result["observations"][index]
        require(set(item) == {"recipe", "related_scenario_ids", "first_command", "last_command", "outcome", "assertions"}
                and type(item["first_command"]) is int and item["first_command"] == self.i + 1
                and type(item["last_command"]) is int and item["related_scenario_ids"] == RELATED[index]
                and item["outcome"] == "fixture_assertions_passed" and isinstance(item["assertions"], list)
                and item["assertions"] and all(isinstance(x, str) and x for x in item["assertions"]), "invalid recipe span/claims")
        yield
        require(item["last_command"] == self.i, "recipe range omitted/borrowed commands")

    def builder_guard(self):
        self.guard()
        engine = decode(self.rows[self.i - 1]["_stdout"])
        self.build_engine_ns = progress_ns(engine["SystemTime"])
        require(getattr(self, "payload_completed_ns", 0) <= self.build_engine_ns,
                "payload completion is outside subsequent Engine clock")
        raw = self.take(["buildx", "inspect", self.builder["name"]])["_stdout"].decode()
        sections = raw.split("\nNodes:\n")
        require(len(sections) == 2, "ambiguous builder nodes")
        def fields(text, wanted):
            found = {}
            for line in text.splitlines():
                if ":" not in line:
                    continue
                key, value = (part.strip() for part in line.split(":", 1))
                require(key != "Error", "builder inspection error")
                if key in wanted:
                    require(key not in found, "duplicate builder field")
                    found[key] = value
            require(set(found) == wanted, "missing builder fields")
            return found
        require(fields(sections[0], {"Name", "Driver"}) == {"Name": self.builder["name"], "Driver": "docker-container"}
                and fields(sections[1], {"Name", "Endpoint", "Status"}) ==
                {"Name": self.builder["node"], "Endpoint": self.scope["docker_context"], "Status": "running"}, "foreign builder/node")
        items = self.json(["container", "inspect", self.builder["container_id"]])
        require(len(items) == 1 and items[0]["Id"] == self.builder["container_id"]
                and items[0]["Image"] == self.builder["image_id"] and items[0]["State"]["Running"] is True
                and items[0]["Name"] == "/buildx_buildkit_" + self.builder["node"], "foreign builder container")
        item = items[0]
        require(item.get("Config", {}).get("Env") == [
            "PATH=/usr/bin:/bin", "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt",
            "BUILDKIT_SETUP_CGROUPV2_ROOT=1"], "builder setup environment is missing, duplicated or changed")
        host = item.get("HostConfig", {})
        require(host.get("CgroupnsMode") == "private" and host.get("Runtime") == "youki" and
                host.get("Privileged") is True and host.get("Init") is True, "builder cgroup/runtime policy changed")
        state = item["State"]
        started = state.get("StartedAt")
        require(type(state.get("Pid")) is int and state["Pid"] > 0 and
                type(item.get("RestartCount")) is int and item["RestartCount"] == 0 and
                state.get("Status") == "running" and state.get("Paused") is False and
                state.get("Restarting") is False and state.get("Dead") is False and
                isinstance(started, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z", started),
                "builder process identity missing or not continuously running")
        try:
            # UTC/fraction syntax was checked above; Python 3.9 does not accept
            # nanoseconds. Keep full timestamp bytes for identity comparison.
            require(datetime.fromisoformat(started[:19]).year > 1970, "invalid builder start time")
        except ValueError as error:
            raise Invalid("invalid builder start time") from error
        identity = (state["Pid"], started)
        require(self.builder_process is None or self.builder_process == identity, "builder process changed between recipes")
        self.builder_process = identity

    def build(self, suffix, dockerfile, arguments, extra=None, code=0):
        self.builder_guard()
        actual = self.rows[self.i]["_args"]
        require(len(actual) > 10 and actual[8] == "--file", "missing Dockerfile")
        fixture = Path(actual[9]).parent.parent
        if self.fixture is None:
            require(fixture_digest(fixture) == self.inputs["fixture_sha256"], "fixture digest mismatch")
            self.fixture = fixture
            self.spec = decode(read(fixture / "fixture.json"))
            secret = read(fixture / "inputs/secret.txt")
            require(secret.strip() and sha(secret) == self.spec["secret_input_sha256"], "secret input pin mismatch")
            self.secret = secret.rstrip(b"\n")
            for data in self.files.values():
                require(secret not in data and secret.rstrip(b"\n") not in data, "secret leaked into retained evidence")
            definition = decode(read(fixture / "compose/compose.json"))
            expected = {kind: {name: {"labels": {"dev.vz.fixture-owner": self.owner}} for name in definition[kind]}
                        for kind in ("services", "networks", "volumes")}
            require(decode(self.files["compose-owner.json"]) == expected, "foreign owner overlay")
        require(fixture == self.fixture, "mixed fixture roots")
        require(fixture_digest(fixture) == self.inputs["fixture_sha256"], "fixture changed between builds")
        args = ["buildx", "build", "--builder", self.builder["name"], "--platform", "linux/arm64", "--progress", "rawjson",
                "--file", str(fixture / "build" / dockerfile), "--output", "type=local,dest=" + str(self.directory / ("export-" + suffix)),
                "--build-arg", "FIXTURE_BASE=" + self.inputs["images"]["base"]["reference"]]
        for key, value in sorted(arguments.items()):
            args += ["--build-arg", key + "=" + value]
        row = self.take(args + (extra or []) + [str(fixture / "build")], code=code, mutation=True)
        require(not row["_stdout"], "unexpected Buildx stdout")
        vertices, logs = progress(row["_stderr"], read(fixture / "build/Dockerfile.secret") if code is None else None)
        require(self.secret not in logs, "secret leaked into decoded BuildKit logs")
        if code == 0:
            require(all(not x.get("error") for x in vertices), "successful build contains error")
        if dockerfile == "Dockerfile":
            proof = payload_graph(row["_stderr"], self.inputs["images"]["base"]["reference"], suffix == "alpha-reuse")
            require(proof["solve_started_ns"] >= self.build_engine_ns, "payload predates authenticated Engine clock")
            binding = list(row["_args"])
            # Exact argv was checked above; normalize only its known output
            # destination. Fixture bytes and all other arguments remain bound.
            binding[11] = "type=local,dest=<selected-export>"
            prior = getattr(self, "payload_proof", None)
            if prior is not None:
                require(proof["normalized"] == prior["normalized"] and proof["base_digest"] == prior["base_digest"],
                        "payload operation/source graph changed")
                require(proof["solve_started_ns"] >= prior["solve_last_observed_ns"], "stale cross-solve payload progress")
                if suffix == "alpha-reuse":
                    require(binding == self.payload_binding, "cache reuse command/source changed")
            self.payload_proof, self.payload_binding = proof, binding
            self.payload_completed_ns = proof["solve_last_observed_ns"]
        return vertices

    @staticmethod
    def vertex(rows, command, cached=False):
        mounts = {"python3 /fixture/tools.py cache": "--mount=type=cache,id=vz04-cache-probe,target=/cache,sharing=locked ",
                  "python3 /fixture/tools.py secret": "--mount=type=secret,id=fixture,required=true "}
        wanted = "RUN --network=none " + mounts.get(command, "") + command
        terminal = [x for x in rows if isinstance(x.get("name"), str)
                    and re.fullmatch(r"\[build [0-9]+/[0-9]+\] " + re.escape(wanted), x["name"]) and x.get("completed")]
        require(len(terminal) == 1, "missing/duplicate completed RUN vertex")
        value = terminal[0]
        require(isinstance(value.get("digest"), str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value["digest"])
                and type(value.get("cached", False)) is bool and value.get("cached", False) is cached
                and not value.get("error") and isinstance(value["completed"], str), "RUN failed or cache state invalid")
        require(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value["completed"]),
                "invalid RUN completion timestamp")
        progress_timestamp(value["completed"])
        return value["digest"]

    def export(self, suffix, payload):
        name = "export-" + suffix + "/" + EXPORTS[suffix]
        require(self.files[name] == payload, "incorrect final-stage exported payload")
        root = self.directory / ("export-" + suffix)
        require({x.name for x in root.iterdir()} == {EXPORTS[suffix]}, "extra final-stage entries")

    def run(self):
        self.guard()
        pins = {pin["reference"]: pin for pin in self.inputs["images"].values()}
        require(len(pins) == 2 and set(self.inputs["images"]) == {"base", "compose"}, "two exact image pins required")
        for _ in range(2):
            args = self.rows[self.i]["_args"]
            require(len(args) == 3 and args[:2] == ["image", "inspect"] and args[2] in pins, "foreign/repeated image")
            pin = pins.pop(args[2]); items = self.json(args)
            require(len(items) == 1 and items[0]["Id"] == pin["id"] and items[0]["Os"] == "linux"
                    and items[0]["Architecture"] == "arm64" and ("@" not in pin["reference"] or pin["reference"] in items[0].get("RepoDigests", [])), "unpinned image")
        args = {"FIXTURE_RUN": self.inputs["run_id"], "FIXTURE_VARIANT": "alpha"}
        with self.observation(0):
            rows = self.build("alpha", "Dockerfile", args)
            self.export("alpha", b"vz04-build-v1\nvariant=alpha\n")
            self.vertex(rows, "python3 /fixture/tools.py payload")
        with self.observation(1):
            rows = self.build("alpha-reuse", "Dockerfile", args)
            self.export("alpha-reuse", b"vz04-build-v1\nvariant=alpha\n")
            self.vertex(rows, "python3 /fixture/tools.py payload", True)
        with self.observation(2):
            rows = self.build("beta", "Dockerfile", args | {"FIXTURE_VARIANT": "beta"})
            self.export("beta", b"vz04-build-v1\nvariant=beta\n")
            self.vertex(rows, "python3 /fixture/tools.py payload")
        with self.observation(3):
            for state, step in (("cold", "first"), ("warm", "second")):
                rows = self.build("cache-" + state, "Dockerfile.cache", {"FIXTURE_OWNER": self.owner, "FIXTURE_CACHE_EXPECT": state, "FIXTURE_CACHE_STEP": step})
                self.export("cache-" + state, f"vz04-cache-v1\nowner={self.owner}\nstate={state}\nstep={step}\n".encode())
                self.vertex(rows, "python3 /fixture/tools.py cache")
        with self.observation(4):
            args = {"FIXTURE_SECRET_SHA256": self.spec["secret_input_sha256"]}
            rows = self.build("secret", "Dockerfile.secret", args, ["--no-cache", "--secret", "id=fixture,src=" + str(self.fixture / "inputs/secret.txt")])
            self.export("secret", b"vz04-secret-mount-ok-v1\n")
            self.vertex(rows, "python3 /fixture/tools.py secret")
            self.vertex(rows, "test ! -e /run/secrets/fixture")
            rows = self.build("secret-missing", "Dockerfile.secret", args, ["--no-cache"], code=None)
            require(any(isinstance(x.get("error"), str) and "secret" in x["error"] and "fixture" in x["error"]
                        and ("not found" in x["error"] or "required" in x["error"]) for x in rows), "negative secret failed for another reason")
            require(not (self.directory / "export-secret-missing").exists(), "failed build exported output")
        require(self.i == len(self.rows) and self.acknowledged == {39}, "unconsumed commands or unreconciled mutation")
        return {"schema_version": 1, "kind": "installed_build_raw_evidence", "outcome": "fixture_assertions_passed",
                "scope": self.scope, "builder": self.builder, "recipes_validated": list(RECIPES), "command_count": self.i,
                "compatibility_certified": False, "release_scenarios_passed": [], "owned_projects": {},
                "builder_cleanup_scope": "parent_harness_required", "retained_local_exports": sorted(EXPORTS),
                "remaining": REMAINING}


def validate(directory: Path, expected_inputs: dict) -> dict:
    """Replay only suite=build; provisioning and final topology cleanup are external."""
    try:
        return Replay(directory, expected_inputs).run()
    except (KeyError, IndexError, TypeError, OSError, ValueError) as error:
        raise Invalid(f"malformed or unavailable Buildx evidence: {error}") from error
