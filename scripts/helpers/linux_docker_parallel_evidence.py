"""Independent four-solve DEV replay; never a release or lifecycle certificate."""
import base64
import json
from pathlib import Path
import re
import stat

import linux_docker_artifact_layout as layout
from linux_docker_artifact_evidence import OCI_OPTIONS, OCI_EXPORT, export_steps
from linux_docker_build_evidence import Replay as BuildReplay, progress, progress_ns
from linux_docker_compose_evidence import Invalid, MAX, decode, fixture_digest, hex64, read, require, runtime_proof, sha

OP_KEYS = {"schema_version", "slot", "parallel_fixture", "parallel_fixture_sha256", "output", "payload", "run_id"}
PHASES = ("started", "ready", "all_ready", "released", "completed")
READY_KEYS = {"schema_version", "run_id", "slot", "started_unix_ns", "started_monotonic_ns", "ready_unix_ns", "ready_monotonic_ns"}
TRANSCRIPT_KEYS = {"schema_version", "outcome", "error_code", "run_id", "slot", "generation_sha256", "participants", "samples", "payload"}
TRANSCRIPT_KEYS |= {phase + clock for phase in PHASES for clock in ("_unix_ns", "_monotonic_ns")}
RUN_NAME = "[build 3/3] RUN --network=none --mount=type=cache,id=vz04-parallel-barrier-v1,target=/barrier,sharing=shared python3 /fixture/parallel.py"


def barrier_transcript(raw, operation, run_start, run_end):
    prefix = b"VZ_PARALLEL_BARRIER="
    require(raw.startswith(prefix) and raw.endswith(b"\n") and raw.count(b"\n") == 1,
            "not one exact parallel barrier transcript")
    value = decode(raw[len(prefix):-1])
    require(type(value) is dict and set(value) == TRANSCRIPT_KEYS and type(value["schema_version"]) is int
            and value["schema_version"] == 1 and value["outcome"] == "released" and value["error_code"] is None
            and value["run_id"] == operation["run_id"] and type(value["slot"]) is int and value["slot"] == operation["slot"],
            "parallel barrier identity or success differs")
    require(raw[len(prefix):-1] == json.dumps(value, sort_keys=True, separators=(",", ":")).encode(),
            "noncanonical barrier transcript")
    for clock in ("_unix_ns", "_monotonic_ns"):
        times = [value[phase + clock] for phase in PHASES]
        require(all(type(t) is int and t > 0 for t in times) and times == sorted(times), "barrier phase time invalid")
    require(run_start <= value["started_unix_ns"] <= value["completed_unix_ns"] <= run_end,
            "barrier transcript outside authoritative RUN")
    require(value["released_monotonic_ns"] - value["all_ready_monotonic_ns"] >= 10**9
            and value["completed_monotonic_ns"] - value["started_monotonic_ns"] <= 60 * 10**9,
            "barrier dwell or deadline unproven")
    participants = value["participants"]
    require(type(participants) is list and len(participants) == 4, "not four barrier participants")
    for slot, row in enumerate(participants):
        require(type(row) is dict and set(row) == READY_KEYS and type(row["schema_version"]) is int
                and row["schema_version"] == 1 and row["run_id"] == value["run_id"] and type(row["slot"]) is int
                and row["slot"] == slot, "foreign barrier participant")
        for clock in ("_unix_ns", "_monotonic_ns"):
            require(type(row["started" + clock]) is int and type(row["ready" + clock]) is int
                    and 0 < row["started" + clock] <= row["ready" + clock] <= value["all_ready" + clock],
                    "participant readiness outside barrier")
    require(participants[value["slot"]] == {key: value[key] for key in READY_KEYS}, "own readiness record differs")
    require(value["generation_sha256"] == sha(json.dumps(participants, sort_keys=True, separators=(",", ":")).encode()),
            "barrier generation digest differs")
    samples = value["samples"]
    require(type(samples) is list and 1 <= len(samples) <= 602, "barrier sample inventory invalid")
    previous_slots, previous_unix, previous_mono, first_full = set(), value["ready_unix_ns"], value["ready_monotonic_ns"], None
    for sample in samples:
        require(type(sample) is dict and set(sample) == {"unix_ns", "monotonic_ns", "ready_slots"}, "unknown barrier sample")
        slots = sample["ready_slots"]
        require(type(slots) is list and all(type(slot) is int and 0 <= slot < 4 for slot in slots)
                and slots == sorted(set(slots)) and previous_slots <= set(slots) and value["slot"] in slots,
                "barrier sample readiness regressed or foreign")
        require(type(sample["unix_ns"]) is int and type(sample["monotonic_ns"]) is int
                and previous_unix <= sample["unix_ns"] <= value["released_unix_ns"]
                and previous_mono <= sample["monotonic_ns"] <= value["released_monotonic_ns"], "barrier sample time invalid")
        previous_slots, previous_unix, previous_mono = set(slots), sample["unix_ns"], sample["monotonic_ns"]
        if slots == [0, 1, 2, 3] and first_full is None:
            first_full = sample
    require(first_full is not None and samples[-1]["ready_slots"] == [0, 1, 2, 3]
            and first_full["unix_ns"] == value["all_ready_unix_ns"]
            and first_full["monotonic_ns"] == value["all_ready_monotonic_ns"], "barrier full readiness unproven")
    require(samples[-1]["monotonic_ns"] >= value["all_ready_monotonic_ns"] + 10**9,
            "barrier lacks post-dwell readiness sample")
    require(value["payload"] == dict(operation["payload"], mode=0o644) and type(value["payload"]["mode"]) is int,
            "barrier output bytes or mode differs")
    return value


def parallel_progress(raw, reference, operation, lower, upper):
    vertices, _ = progress(raw)
    require(lower <= upper, "reversed parallel Engine clocks")
    base, separator, pin = reference.partition("@sha256:")
    require(separator and hex64(pin), "unpinned parallel base")
    if "/" not in base:
        base = "docker.io/library/" + base
    elif not ("." in base.split("/")[0] or ":" in base.split("/")[0] or base.startswith("localhost/")):
        base = "docker.io/" + base
    base += "@sha256:" + pin
    wanted = {"base": "[build 1/3] FROM " + base, "context": "[internal] load build context",
              "copy": "[build 2/3] COPY parallel.py /fixture/parallel.py", "run": RUN_NAME,
              "output": "[output 1/1] COPY --from=build /out/payload.txt /payload.txt", "export": OCI_EXPORT}
    auxiliary = {"[internal] load build definition from Dockerfile.parallel", "[internal] load .dockerignore",
                 "[internal] load metadata for " + base}
    grouped, names = {}, {}
    for vertex in vertices:
        require(not vertex.get("error") and set(vertex) <= {"digest", "name", "inputs", "started", "completed", "cached", "error"},
                "parallel vertex error or unknown field")
        identity, name, inputs = vertex["digest"], vertex["name"], vertex.get("inputs", [])
        require(type(inputs) is list and all(isinstance(x, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", x) for x in inputs)
                and len(inputs) == len(set(inputs)), "invalid parallel edges")
        rows = grouped.setdefault(identity, [])
        require(not rows or (rows[0]["name"] == name and rows[0].get("inputs", []) == inputs), "parallel graph drift")
        require(name not in names or names[name] == identity, "duplicate parallel graph role")
        names[name] = identity
        rows.append(vertex)
        for key in ("started", "completed"):
            if key in vertex:
                require(lower <= progress_ns(vertex[key]) <= upper, "parallel progress outside Engine clocks")
        if "completed" in vertex:
            require("started" in vertex and progress_ns(vertex["started"]) <= progress_ns(vertex["completed"]), "parallel lifetime reversed")
    require(set(wanted.values()) <= set(names) <= set(wanted.values()) | auxiliary, "missing or foreign parallel operation")
    ids = {role: names[name] for role, name in wanted.items()}
    edges = {"base": [], "context": [], "copy": ["base", "context"], "run": ["copy"], "output": ["run"], "export": []}
    terminal = {}
    for role, identity in ids.items():
        rows = grouped[identity]
        require(rows[0].get("inputs", []) == [ids[x] for x in edges[role]], "parallel graph disconnected")
        done = [row for row in rows if "completed" in row]
        require(done, "parallel graph role unfinished")
        if role not in ("base", "context"):
            require(len(done) == 1 and rows[-1] is done[0] and len({row["started"] for row in rows if "started" in row}) == 1,
                    "parallel operation repeated/lifetime drift")
            if role != "copy":
                require(all(row.get("cached", False) is False for row in rows), "parallel RUN/output/export cached")
            else:
                hit = False
                for row in rows:
                    require(not hit or row.get("cached", False) is True, "parallel COPY cache flag regressed")
                    hit = hit or row.get("cached", False)
            terminal[role] = done[0]
            for parent in edges[role]:
                require(max(progress_ns(row["completed"]) for row in grouped[ids[parent]] if "completed" in row)
                        <= progress_ns(done[0]["started"]), "parallel dependency time reversed")
    require(progress_ns(terminal["output"]["completed"]) <= progress_ns(terminal["export"]["started"]), "parallel export precedes output")
    run = terminal["run"]
    began, ended = progress_ns(run["started"]), progress_ns(run["completed"])
    require(began < ended, "parallel RUN has no positive lifetime")
    log = []
    for line in raw.splitlines():
        batch = decode(line)
        require(not batch.get("warnings"), "unexpected parallel warning")
        for category in ("statuses", "logs"):
            for row in batch.get(category, []):
                require(row.get("vertex") in grouped, "unbound parallel progress frame")
                for key in ("timestamp", "started", "completed"):
                    if key in row:
                        require(lower <= progress_ns(row[key]) <= upper, "parallel frame outside Engine clocks")
                if category == "logs":
                    require(row["vertex"] == ids["run"] and type(row.get("stream")) is int and row["stream"] == 1
                            and set(row) <= {"vertex", "stream", "data", "timestamp"}
                            and began <= progress_ns(row["timestamp"]) <= ended, "foreign parallel execution log")
                    log.append(base64.b64decode(row["data"], validate=True))
    return {"run_interval": {"digest": ids["run"], "started_ns": began, "completed_ns": ended},
            "barrier": barrier_transcript(b"".join(log), operation, began, ended)}


def absolute(value):
    require(isinstance(value, str) and value and not any(c in value for c in "\x00\n\r,"), "invalid parallel path")
    path = Path(value)
    require(path.is_absolute() and path == path.resolve(), "redirected parallel path")
    return path


def commands(directory, inputs):
    """Read exactly nine immutable command intents, captures and terminal receipts."""
    rows, previous = [], 0
    for index in range(1, 10):
        stem = directory / f"command-{index:05d}"
        row, intent = (decode(read(Path(str(stem) + ext))) for ext in (".json", ".intent.json"))
        require(type(row["index"]) is int and row["index"] == index and row["argv0"] == "docker"
                and row["argv"][:5] == ["docker", "--config", inputs["docker_config"], "--context", inputs["scope"]["docker_context"]]
                and row["executable"] == inputs["clients"]["docker"]["path"], "foreign parallel command routing")
        require(row["host_outcome"] == "exited" and row["capture_complete"] is True and row["raw_streams_retained"] is True
                and all(row[k] is False for k in ("timed_out", "interrupted", "output_limit_exceeded", "secret_leak_detected", "effects_uncertain"))
                and row["dispatch_error"] is None and type(row["exit_code"]) is int and row["exit_code"] == 0,
                "incomplete or failed parallel command")
        require(type(row["started_unix_ns"]) is int and row["started_unix_ns"] >= previous
                and type(row["elapsed_ns"]) is int and 0 <= row["elapsed_ns"] <= 310 * 10**9,
                "invalid parallel host timing")
        previous = row["started_unix_ns"]
        require(intent["host_outcome"] == "inflight" and intent["effects_uncertain"] is True and intent["exit_code"] is None,
                "missing parallel dispatch intent")
        for key in ("index", "executable", "argv", "argv0", "environment", "started_unix_ns", "mutation", "max_stream_bytes"):
            require(intent[key] == row[key], "parallel intent/terminal drift")
        require(type(row["max_stream_bytes"]) is int and 1 <= row["max_stream_bytes"] <= MAX, "unbounded parallel streams")
        for name in ("stdout", "stderr"):
            require(row[name] == stem.name + "." + name, "redirected parallel stream")
            content = read(directory / row[name])
            require(len(content) <= row["max_stream_bytes"] and row["observed_bytes"][name] == len(content)
                    and all(row[key] == sha(content) for key in (name + "_sha256", "raw_" + name + "_sha256",
                                                              "retained_observed_" + name + "_sha256"))
                    and row["retained_observed_" + name + "_bytes"] == len(content), "parallel stream hash/length mismatch")
            row["_" + name] = content
        row["_args"] = row["argv"][5:]
        rows.append(row)
    return rows


class Replay(BuildReplay):
    def __init__(self, directory, inputs, operation):
        self.directory = directory = absolute(str(directory))
        require(directory.is_dir() and stat.S_IMODE(directory.stat().st_mode) == 0o700, "private parallel receipt directory required")
        require(type(operation) is dict and set(operation) == OP_KEYS and type(operation["schema_version"]) is int
                and operation["schema_version"] == 1 and type(operation["slot"]) is int and 0 <= operation["slot"] < 4,
                "invalid parallel operation contract")
        for filename, value in (("inputs.json", inputs), ("operation.intent.json", operation), ("operation.json", operation)):
            require(decode(read(directory / filename)) == value, "parallel immutable operation/input differs")
        files = {"inputs.json", "operation.intent.json", "operation.json", "artifact-validation.json", "compose-owner.json"}
        files |= {f"command-{i:05d}{ext}" for i in range(1, 10) for ext in (".json", ".intent.json", ".stdout", ".stderr")}
        directories = {"private-tmp", "oci"}
        require({p.name for p in directory.iterdir()} == files | directories, "unexpected parallel operation inventory")
        require(all(not p.is_symlink() and (p.is_dir() if p.name in directories else p.is_file()) for p in directory.iterdir()),
                "redirected parallel operation inventory")
        require(hex64(inputs["fixture_sha256"]) and hex64(inputs["release_sha256"])
                and operation["run_id"] == inputs["run_id"] and isinstance(operation["run_id"], str)
                and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", operation["run_id"]), "parallel input identity differs")
        runtime_proof(inputs)
        self.inputs, self.operation = inputs, operation
        self.scope, self.builder = inputs["scope"], inputs["builder"]
        require(hex64(self.builder["container_id"]) and re.fullmatch(r"sha256:[0-9a-f]{64}", self.builder["image_id"]),
                "invalid parallel builder identity")
        self.rows, self.acknowledged, self.i, self.builder_process = commands(directory, inputs), set(), 0, None

    def run(self):
        op = self.operation
        fixture = absolute(op["parallel_fixture"])
        require(fixture_digest(fixture) == op["parallel_fixture_sha256"] and hex64(op["parallel_fixture_sha256"]),
                "parallel fixture digest mismatch")
        require(op["output"] == str(self.directory / "oci"), "parallel output redirected")
        payload = f"vz04-parallel-v1\nslot={op['slot']}\n".encode()
        expected = {"path": "payload.txt", "sha256": sha(payload), "size": len(payload)}
        require(op["payload"] == expected and type(op["payload"]["size"]) is int, "parallel payload contract differs")
        self.builder_guard()
        lower = self.build_engine_ns
        argv = ["buildx", "build", "--builder", self.builder["name"], "--platform", "linux/arm64", "--progress", "rawjson",
                "--file", str(fixture / "Dockerfile.parallel"), "--provenance=false", "--sbom=false", "--output",
                "type=oci,dest=" + op["output"] + OCI_OPTIONS, "--build-arg", "FIXTURE_BASE=" + self.inputs["images"]["base"]["reference"],
                "--build-arg", "FIXTURE_RUN=" + op["run_id"], "--build-arg", "FIXTURE_SLOT=" + str(op["slot"]),
                "--network=none", str(fixture)]
        build = self.take(argv, mutation=True)
        require(not build["_stdout"], "unexpected parallel build stdout")
        self.builder_guard()
        upper = self.build_engine_ns
        require(self.i == 9, "unconsumed parallel commands")
        before, after = (decode(self.rows[i]["_stdout"])[0] for i in (3, 8))
        require(all(before.get(key) == after.get(key) for key in ("Config", "HostConfig", "Mounts")),
                "parallel builder configuration/volume drift")
        require(before["State"].get("OOMKilled") is False and after["State"].get("OOMKilled") is False,
                "parallel builder OOM")
        graph = parallel_progress(build["_stderr"], self.inputs["images"]["base"]["reference"], op, lower, upper)
        image = layout.validate_oci(self.directory / "oci", expected_path="payload.txt", expected_sha256=sha(payload), expected_size=len(payload))
        require(decode(read(self.directory / "artifact-validation.json", 16 * 1024 * 1024)) == {"oci": image},
                "parallel recorded artifact proof differs")
        export_steps(build["_stderr"], image, None)
        require(fixture_digest(fixture) == op["parallel_fixture_sha256"], "parallel fixture changed during replay")
        return {"schema_version": 1, "slot": op["slot"], "run_id": op["run_id"], "scope": self.scope, "builder": self.builder,
                "builder_process": {"pid": self.builder_process[0], "started_at": self.builder_process[1]},
                "parallel_fixture_sha256": op["parallel_fixture_sha256"], "command_count": 9,
                "progress_sha256": sha(build["_stderr"]), "run_interval": graph["run_interval"],
                "barrier": graph["barrier"], "oci": image, "parent_provisioning_and_cleanup_required": True,
                "compatibility_certified": False}


def validate_slot(directory, expected_inputs, expected_operation):
    try:
        return Replay(directory, expected_inputs, expected_operation).run()
    except (OSError, ValueError, KeyError, IndexError, TypeError, UnicodeError) as error:
        raise Invalid("parallel slot evidence rejected: " + type(error).__name__) from error


def validate_group(rows):
    """Cross-check four already independently replayed slots, not caller claims."""
    try:
        require(type(rows) is list and len(rows) == 4 and all(type(row) is dict for row in rows), "not four parallel proofs")
        require(all(type(row["slot"]) is int for row in rows) and {row["slot"] for row in rows} == set(range(4)),
                "parallel slots missing or duplicated")
        selected = sorted(rows, key=lambda row: row["slot"])
        first = selected[0]
        for row in selected:
            require(type(row["schema_version"]) is int and row["schema_version"] == 1 and row["command_count"] == 9
                    and row["compatibility_certified"] is False, "invalid parallel proof schema or scope")
            for field in ("scope", "builder", "builder_process", "run_id", "parallel_fixture_sha256"):
                require(row[field] == first[field], "parallel builder/Engine/run/fixture identity differs")
            barrier = row["barrier"]
            require(barrier["slot"] == row["slot"] and barrier["run_id"] == row["run_id"]
                    and barrier["participants"] == first["barrier"]["participants"]
                    and barrier["generation_sha256"] == first["barrier"]["generation_sha256"], "parallel barrier generation differs")
        require(len({row["run_interval"]["digest"] for row in selected}) == 4, "parallel RUN identity reused")
        began = max(row["run_interval"]["started_ns"] for row in selected)
        ended = min(row["run_interval"]["completed_ns"] for row in selected)
        ready = max(row["barrier"]["ready_monotonic_ns"] for row in selected)
        release = min(row["barrier"]["released_monotonic_ns"] for row in selected)
        require(began < ended and ready < release, "four-way concurrent RUN/barrier overlap unproven")
        return {"schema_version": 1, "outcome": "four_parallel_solves_validated", "slots": [0, 1, 2, 3],
                "scope": first["scope"], "builder": first["builder"], "builder_process": first["builder_process"],
                "run_id": first["run_id"], "parallel_fixture_sha256": first["parallel_fixture_sha256"],
                "generation_sha256": first["barrier"]["generation_sha256"],
                "run_overlap": {"started_ns": began, "completed_ns": ended, "duration_ns": ended - began},
                "barrier_overlap_monotonic": {"started_ns": ready, "completed_ns": release, "duration_ns": release - ready},
                "command_count": 36, "parent_provisioning_and_cleanup_required": True, "compatibility_certified": False}
    except (ValueError, KeyError, IndexError, TypeError) as error:
        raise Invalid("parallel group evidence rejected: " + type(error).__name__) from error
