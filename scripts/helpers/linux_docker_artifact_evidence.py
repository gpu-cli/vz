"""Read-only replay of one exact DEV artifact operation, never the five recipes.

The parent authenticates inputs, role provisioning/fresh volumes and cross-role
relationships. This consumer proves nine commands and their exported bytes; it
does not certify fresh provisioning, cleanup, cross-Machine transport or parity.
"""
import json
from pathlib import Path
import re
import stat

import linux_docker_artifact_layout as layout
import linux_docker_artifact_stream as stream
from linux_docker_build_evidence import Replay as BuildReplay, payload_graph, progress, progress_ns
from linux_docker_compose_evidence import Invalid, MAX, decode, fixture_digest, hex64, read, require, runtime_proof, sha


OPERATIONS = {
    "source-alpha": ("source", "alpha", True),
    "source-beta": ("source", "beta", False),
    "source-secret": ("source", None, True),
    "fresh-cold-alpha": ("cold-control", "alpha", False),
    "fresh-import-alpha": ("importer", "alpha", False),
}
OP_KEYS = {"schema_version", "operation", "role", "output", "cache_output", "cache_import",
           "fixture_sha256", "payload", "cache_inventory_before", "cache_inventory_after"}
OCI_OPTIONS = ",tar=false,oci-mediatypes=true,compression=gzip,force-compression=true"
CACHE_OPTIONS = ",mode=max,image-manifest=true,oci-mediatypes=true,compression=gzip,force-compression=true"
OCI_EXPORT = "exporting to oci image format"
CACHE_EXPORT = "exporting cache to client directory"


def _absolute(value):
    require(isinstance(value, str) and value and not any(c in value for c in "\x00\n\r,"), "invalid artifact path")
    path = Path(value)
    require(path.is_absolute() and path == path.resolve(), "redirected artifact path")
    return path


def _terminal(rows):
    done = [v for v in rows if "completed" in v]
    require(len(done) == 1 and rows[-1] is done[0], "missing, duplicate or postterminal artifact operation")
    require(len({v["started"] for v in rows if "started" in v}) == 1, "artifact operation lifetime drift")
    hit = False
    for value in rows:
        require(not hit or value.get("cached", False), "artifact cache flag regressed")
        hit = hit or value.get("cached", False)
    return done[0]


def export_steps(raw, image, cache):
    """Require completed exporter substeps bound to the actual artifact digests."""
    batches = [decode(line) for line in raw.splitlines()]
    roots = {v["digest"]: v for b in batches for v in b.get("vertexes", [])
             if v["name"] in (OCI_EXPORT, CACHE_EXPORT) and "completed" in v}
    expected = {OCI_EXPORT: {"exporting layers", "exporting manifest " + image["manifest"]["digest"],
                             "exporting config " + image["config"]["digest"]}}
    if cache is not None:
        expected[CACHE_EXPORT] = {"preparing build cache for export", "writing config " + cache["config"]["digest"],
                                  "writing cache image manifest " + cache["manifest"]["digest"]}
        expected[CACHE_EXPORT].update("writing layer " + row["blob"] for row in cache["layers"])
    for identity, vertex in roots.items():
        rows = [row for batch in batches for row in batch.get("statuses", []) if row.get("vertex") == identity]
        require({row.get("id") for row in rows} == expected[vertex["name"]], "missing or foreign artifact export substeps")
        for name in expected[vertex["name"]]:
            updates = [row for row in rows if row["id"] == name]
            for row in updates:
                require(set(row) <= {"id", "vertex", "name", "current", "total", "timestamp", "started", "completed"},
                        "unknown exporter status field")
                for key in ("current", "total"):
                    require(key not in row or type(row[key]) is int and row[key] >= 0, "invalid exporter status counter")
                for key in ("started", "completed", "timestamp"):
                    if key in row:
                        require(progress_ns(vertex["started"]) <= progress_ns(row[key]) <= progress_ns(vertex["completed"]),
                                "export substep outside its root lifetime")
            done = [row for row in updates if "completed" in row]
            require(len(done) == 1 and updates[-1] is done[0] and "started" in done[0],
                    "export substep did not complete exactly once")


def artifact_progress(raw, *, reference, secret, cached, cache_export, cache_import, lower, upper):
    """Separate exporter/importer grammar; existing payload graph stays strict."""
    vertices, logs = progress(raw)
    require(lower <= upper, "Engine clocks reversed")
    grouped, names = {}, {}
    for v in vertices:
        require(not v.get("error"), "artifact solve contains an error")
        identity, name = v["digest"], v["name"]
        edges = v.get("inputs", [])
        require(type(edges) is list and all(isinstance(x, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", x)
                for x in edges) and len(set(edges)) == len(edges), "invalid artifact graph edges")
        prior = grouped.setdefault(identity, [])
        require(not prior or (prior[0]["name"] == name and prior[0].get("inputs", []) == edges), "artifact graph drift")
        require(name not in names or names[name] == identity, "duplicate artifact graph role")
        names[name] = identity
        prior.append(v)
        if "completed" in v:
            require("started" in v and progress_ns(v["started"]) <= progress_ns(v["completed"]), "reversed artifact lifetime")
        for key in ("started", "completed"):
            if key in v:
                require(lower <= progress_ns(v[key]) <= upper, "artifact progress outside Engine clocks")
    imports = [name for name in names if name.startswith("importing cache manifest from ")]
    require(len(imports) == int(cache_import), "missing or unexpected cache import")
    extra = {OCI_EXPORT} | ({CACHE_EXPORT} if cache_export else set()) | set(imports)
    require((CACHE_EXPORT in names) is cache_export and OCI_EXPORT in names, "missing or unexpected exporter")
    terminals = {}
    for name in extra:
        require(name in names, "missing artifact phase")
        rows = grouped[names[name]]
        require(not rows[0].get("inputs", []), "export/import root has foreign edges")
        terminals[name] = _terminal(rows)
        require(terminals[name].get("cached", False) is False, "export/import was not executed")
        if name in imports:
            require(re.fullmatch(r"importing cache manifest from local:[0-9]+", name) and
                    names[name] == "sha256:" + sha(name.encode()), "foreign local cache import identity")
    # All status and log timestamps also belong to this solve, including starts
    # without completions. Export substeps are statuses, not graph vertices.
    batches = [decode(line) for line in raw.splitlines()]
    for batch in batches:
        for category in ("statuses", "logs"):
            for row in batch.get(category, []):
                require(row.get("vertex") in grouped, "unbound artifact progress frame")
                for key in ("timestamp", "started", "completed"):
                    if key in row:
                        require(lower <= progress_ns(row[key]) <= upper, "artifact frame outside Engine clocks")
                if "completed" in row:
                    require("started" in row and progress_ns(row["started"]) <= progress_ns(row["completed"]),
                            "reversed artifact status lifetime")
                if category == "statuses" and grouped[row["vertex"]][0]["name"] in extra:
                    name = grouped[row["vertex"]][0]["name"]
                    patterns = (r"exporting layers|exporting (?:manifest|config|manifest list) sha256:[0-9a-f]{64}"
                                if name == OCI_EXPORT else
                                r"preparing build cache for export|writing (?:layer|config|cache image manifest) sha256:[0-9a-f]{64}")
                    require(isinstance(row.get("id"), str) and re.fullmatch(patterns, row["id"]), "foreign export substep")
    if not secret:
        # Explicitly remove only roots validated above. No unknown operation is
        # dropped, and the unmodified full stream remains hash-bound evidence.
        selected = []
        for batch in batches:
            item = dict(batch)
            if "vertexes" in item:
                item["vertexes"] = [v for v in item["vertexes"] if v["name"] not in extra]
            selected.append(json.dumps(item, separators=(",", ":")).encode())
        graph = payload_graph(b"\n".join(selected) + b"\n", reference, cached)
        output_end = max(progress_ns(v["completed"]) for v in grouped[graph["ids"]["output"]] if "completed" in v)
    else:
        # Normalize only Docker Hub's canonical reference spelling.
        base = reference
        if "/" not in base.split("@", 1)[0]:
            base = "docker.io/library/" + base
        elif not any(x in base.split("/")[0] for x in (".", ":")) and not base.startswith("localhost/"):
            base = "docker.io/" + base
        wanted = {"base": "[build 1/4] FROM " + base, "context": "[internal] load build context",
                  "copy": "[build 2/4] COPY tools.py /fixture/tools.py",
                  "run": "[build 3/4] RUN --network=none --mount=type=secret,id=fixture,required=true python3 /fixture/tools.py secret",
                  "absent": "[build 4/4] RUN --network=none test ! -e /run/secrets/fixture",
                  "output": "[output 1/1] COPY --from=build /out/secret.txt /secret.txt"}
        auxiliary = {"[internal] load build definition from Dockerfile.secret", "[internal] load .dockerignore",
                     "[internal] load metadata for " + base}
        require(set(wanted.values()) <= set(names) and set(names) <= set(wanted.values()) | auxiliary | extra,
                "foreign or missing secret operation")
        ids = {role: names[name] for role, name in wanted.items()}
        edges = {"base": [], "context": [], "copy": ["base", "context"], "run": ["copy"],
                 "absent": ["run"], "output": ["absent"]}
        done = {}
        for role, identity in ids.items():
            rows = grouped[identity]
            require(rows[0].get("inputs", []) == [ids[x] for x in edges[role]], "secret graph disconnected")
            require(any("completed" in v for v in rows), "secret graph unfinished")
            if role not in ("base", "context"):
                done[role] = _terminal(rows)
                for parent in edges[role]:
                    require(max(progress_ns(v["completed"]) for v in grouped[ids[parent]] if "completed" in v)
                            <= progress_ns(done[role]["started"]), "secret dependency timing reversed")
        require(done["run"].get("cached", False) is False and done["absent"].get("cached", False) is False,
                "secret RUN was not actually executed")
        require(not logs, "unexpected secret execution output")
        output_end = progress_ns(done["output"]["completed"])
        graph = {"ids": ids, "normalized": [(role, wanted[role], edges[role]) for role in wanted]}
    require(output_end <= progress_ns(terminals[OCI_EXPORT]["started"]), "OCI export precedes completed output")
    if cache_export:
        require(progress_ns(terminals[OCI_EXPORT]["completed"]) <= progress_ns(terminals[CACHE_EXPORT]["started"]),
                "cache export precedes completed OCI export")
    return {"progress_sha256": sha(raw), "graph": graph, "export_roots": sorted(extra),
            "engine_before_ns": lower, "engine_after_ns": upper}


class Replay(BuildReplay):
    def __init__(self, directory, inputs, operation):
        self.directory = _absolute(str(directory))
        require(self.directory.is_dir() and stat.S_IMODE(self.directory.stat().st_mode) == 0o700,
                "private artifact receipt directory required")
        self.inputs, self.operation = inputs, operation
        require(decode(read(directory / "inputs.json")) == inputs and
                decode(read(directory / "operation.json")) == operation, "foreign artifact expectations")
        intent_operation = dict(operation, cache_inventory_after=None)
        require(decode(read(directory / "operation.intent.json")) == intent_operation,
                "artifact operation intent differs")
        require(type(operation) is dict and set(operation) == OP_KEYS and
                type(operation["schema_version"]) is int and operation["schema_version"] == 1 and
                operation["operation"] in OPERATIONS, "unknown artifact operation schema")
        top_files = {"inputs.json", "operation.intent.json", "operation.json", "artifact-validation.json", "compose-owner.json"}
        top_directories = {"private-tmp", "oci"} | ({"cache"} if operation["cache_output"] is not None else set())
        command_files = {f"command-{i:05d}{ext}" for i in range(1, 10) for ext in (".json", ".intent.json", ".stdout", ".stderr")}
        require({p.name for p in directory.iterdir()} == top_files | top_directories | command_files,
                "unexpected artifact operation inventory")
        require(all(not p.is_symlink() and (p.is_dir() if p.name in top_directories else p.is_file())
                    for p in directory.iterdir()), "redirected artifact operation inventory")
        require(operation["fixture_sha256"] == inputs["fixture_sha256"] and hex64(inputs["fixture_sha256"])
                and hex64(inputs["release_sha256"]), "artifact input digest mismatch")
        self.scope, self.builder = inputs["scope"], inputs["builder"]
        runtime_proof(inputs)
        require(hex64(self.builder["container_id"]) and re.fullmatch(r"sha256:[0-9a-f]{64}", self.builder["image_id"]),
                "invalid builder identity")
        self.rows, self.acknowledged, self.i, self.builder_process = [], set(), 0, None
        expected = {f"command-{i:05d}{ext}" for i in range(1, 10) for ext in (".json", ".intent.json", ".stdout", ".stderr")}
        require({p.name for p in directory.glob("command-*")} == expected, "artifact operation requires exactly nine commands")
        previous = 0
        for index in range(1, 10):
            stem = directory / f"command-{index:05d}"
            row, intent = (decode(read(Path(str(stem) + ext))) for ext in (".json", ".intent.json"))
            require(type(row["index"]) is int and row["index"] == index and row["argv0"] == "docker"
                    and row["argv"][:5] == ["docker", "--config", inputs["docker_config"], "--context", self.scope["docker_context"]]
                    and row["executable"] == inputs["clients"]["docker"]["path"], "foreign artifact command routing")
            require(row["host_outcome"] == "exited" and row["capture_complete"] is True and row["raw_streams_retained"] is True
                    and all(row[k] is False for k in ("timed_out", "interrupted", "output_limit_exceeded", "secret_leak_detected", "effects_uncertain"))
                    and row["dispatch_error"] is None and type(row["exit_code"]) is int and row["exit_code"] == 0,
                    "incomplete or failed artifact command")
            require(type(row["started_unix_ns"]) is int and row["started_unix_ns"] >= previous and
                    type(row["elapsed_ns"]) is int and 0 <= row["elapsed_ns"] <= 310 * 10**9,
                    "invalid artifact host timing")
            previous = row["started_unix_ns"]
            require(intent["host_outcome"] == "inflight" and intent["effects_uncertain"] is True and intent["exit_code"] is None,
                    "missing artifact dispatch intent")
            for key in ("index", "executable", "argv", "argv0", "environment", "started_unix_ns", "mutation", "max_stream_bytes"):
                require(intent[key] == row[key], "artifact intent/terminal drift")
            require(type(row["max_stream_bytes"]) is int and 1 <= row["max_stream_bytes"] <= MAX, "unbounded artifact streams")
            for name in ("stdout", "stderr"):
                require(row[name] == stem.name + "." + name, "redirected artifact stream")
                content = read(directory / row[name])
                require(len(content) <= row["max_stream_bytes"] and row["observed_bytes"][name] == len(content) and
                        all(row[key] == sha(content) for key in (name + "_sha256", "raw_" + name + "_sha256",
                                                              "retained_observed_" + name + "_sha256")) and
                        row["retained_observed_" + name + "_bytes"] == len(content), "artifact stream hash/length mismatch")
                row["_" + name] = content
            row["_args"] = row["argv"][5:]
            self.rows.append(row)

    def run(self):
        op = self.operation
        role, variant, cache_export = OPERATIONS[op["operation"]]
        secret = variant is None
        imported = role == "importer"
        require(op["role"] == role and op["output"] == str(self.directory / "oci") and
                op["cache_output"] == (str(self.directory / "cache") if cache_export else None) and
                op["cache_import"] == (str(self.directory.parent / "source-alpha/cache") if imported else None),
                "artifact role or output/cache path mismatch")
        self.builder_guard()
        lower = self.build_engine_ns
        fixture = _absolute(self.rows[4]["_args"][-1]).parent
        require(fixture_digest(fixture) == op["fixture_sha256"], "artifact fixture changed")
        spec = decode(read(fixture / "fixture.json"))
        canary = read(fixture / "inputs/secret.txt")
        require(canary.strip() and sha(canary) == spec["secret_input_sha256"], "artifact secret pin mismatch")
        canaries = (canary, canary.rstrip(b"\n"))
        intermediate = read(fixture / "build/intermediate-canary.txt")
        require(intermediate.strip(), "empty intermediate canary")
        oci_canaries = canaries + (intermediate, intermediate.rstrip(b"\n"))
        payload = b"vz04-secret-mount-ok-v1\n" if secret else f"vz04-build-v1\nvariant={variant}\n".encode()
        expected = {"path": "secret.txt" if secret else "payload.txt", "sha256": sha(payload), "size": len(payload)}
        require(type(op["payload"]) is dict and op["payload"] == expected and type(op["payload"]["size"]) is int,
                "artifact payload expectation differs from fixture")
        args = ["buildx", "build", "--builder", self.builder["name"], "--platform", "linux/arm64", "--progress", "rawjson",
                "--file", str(fixture / "build" / ("Dockerfile.secret" if secret else "Dockerfile")),
                "--provenance=false", "--sbom=false", "--output", "type=oci,dest=" + op["output"] + OCI_OPTIONS,
                "--build-arg", "FIXTURE_BASE=" + self.inputs["images"]["base"]["reference"]]
        arguments = {"FIXTURE_SECRET_SHA256": spec["secret_input_sha256"]} if secret else {
            "FIXTURE_RUN": self.inputs["run_id"], "FIXTURE_VARIANT": variant}
        for key, value in sorted(arguments.items()):
            args += ["--build-arg", key + "=" + value]
        if cache_export:
            args += ["--cache-to", "type=local,dest=" + op["cache_output"] + CACHE_OPTIONS]
        if imported:
            args += ["--cache-from", "type=local,src=" + op["cache_import"]]
        if secret:
            args += ["--no-cache", "--secret", "id=fixture,src=" + str(fixture / "inputs/secret.txt")]
        build = self.take(args + [str(fixture / "build")], mutation=True)
        require(not build["_stdout"], "unexpected artifact build stdout")
        self.builder_guard()
        upper = self.build_engine_ns
        require(self.i == 9, "unconsumed artifact commands")
        before, after = (decode(self.rows[i]["_stdout"])[0] for i in (3, 8))
        for key in ("Config", "HostConfig", "Mounts"):
            require(before.get(key) == after.get(key), "artifact builder configuration/volume changed")
        require(before["State"].get("OOMKilled") is False and after["State"].get("OOMKilled") is False,
                "artifact builder OOM state")
        for row in self.rows:
            require(not any(value in row["_stdout"] or value in row["_stderr"] for value in canaries), "artifact command leaked secret")
        graph = artifact_progress(build["_stderr"], reference=self.inputs["images"]["base"]["reference"],
                                  secret=secret, cached=imported, cache_export=cache_export, cache_import=imported,
                                  lower=lower, upper=upper)
        require(not any(value in progress(build["_stderr"])[1] for value in canaries), "decoded artifact log leaked secret")
        image = layout.validate_oci(_absolute(op["output"]), expected_path=expected["path"],
                                    expected_sha256=expected["sha256"], expected_size=expected["size"], canaries=oci_canaries)
        cache = layout.validate_cache(_absolute(op["cache_output"]), canaries=canaries) if cache_export else None
        expected_validation = {"oci": image}
        if cache is not None:
            expected_validation["cache"] = cache
        require(decode(read(self.directory / "artifact-validation.json", 16 * 1024 * 1024)) == expected_validation,
                "recorded artifact validation differs from actual bytes")
        export_steps(build["_stderr"], image, cache)
        imported_cache = None
        if imported:
            current = stream.inventory_tree(_absolute(op["cache_import"]), canaries=canaries, limits=layout.LIMITS)
            require(op["cache_inventory_before"] == op["cache_inventory_after"] == current,
                    "source cache inventory changed across import")
            imported_cache = layout.validate_cache(_absolute(op["cache_import"]), canaries=canaries)
        else:
            require(op["cache_inventory_before"] is None and op["cache_inventory_after"] is None, "unexpected cache inventory")
        require(fixture_digest(fixture) == op["fixture_sha256"], "fixture changed during artifact replay")
        return {"schema_version": 1, "operation": op["operation"], "role": role, "command_count": 9,
                "outcome": "artifact_operation_validated", "scope": self.scope, "builder": self.builder,
                "progress": graph, "oci": image, "cache": cache, "imported_cache": imported_cache,
                "parent_role_provisioning_and_cleanup_required": True, "compatibility_certified": False}


def validate(directory: Path, expected_inputs: dict, expected_operation: dict) -> dict:
    try:
        return Replay(directory, expected_inputs, expected_operation).run()
    except (OSError, ValueError, KeyError, IndexError, TypeError, UnicodeError) as error:
        # Paths, raw exporter messages and possible secret values are not echoed.
        raise Invalid("artifact operation evidence rejected: " + type(error).__name__) from error
