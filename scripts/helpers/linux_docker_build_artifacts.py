"""Owned installed-Mac OCI/cache DEV lane; never full Docker certification.

Each operation retains its own raw host command recorder. Builders and their
normal cleanup belong to the topology harness, not to this artifact consumer.
The cold-control is never reused as the importer: its cold solve warms it.
"""
import copy
import json
import os
from pathlib import Path
import time

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_artifact_layout as layout
import linux_docker_artifact_stream as stream

require = driver.require
OPERATIONS = ("source-alpha", "source-beta", "source-secret", "fresh-cold-alpha", "fresh-import-alpha")
ROLES = ("source", "cold-control", "importer")
OCI_OPTIONS = ",tar=false,oci-mediatypes=true,compression=gzip,force-compression=true"
CACHE_OPTIONS = ",mode=max,image-manifest=true,oci-mediatypes=true,compression=gzip,force-compression=true"


def specification(operation, output, fixture, fixture_sha256, cache_import=None):
    require(operation in OPERATIONS, "unsupported artifact operation")
    output, fixture = Path(output), Path(fixture)
    require(output.is_absolute() and output.parent == output.parent.resolve(), "canonical operation directory required")
    require(all(not any(char in str(path) for char in (",", "\x00", "\n", "\r"))
                for path in (output, fixture)), "artifact path contains exporter delimiters")
    spec = json.loads(driver.regular(fixture / "fixture.json"))
    secret = operation == "source-secret"
    key = "secret_output" if secret else "build_beta" if operation == "source-beta" else "build_alpha"
    payload = spec["expected"][key].encode()
    require(driver.sha256(payload) == spec["expected_payload_sha256"][key], "fixture expected payload hash differs")
    role = "cold-control" if operation == "fresh-cold-alpha" else "importer" if operation == "fresh-import-alpha" else "source"
    expected_import = output.parent / "source-alpha" / "cache"
    require((cache_import is not None) == (role == "importer"), "unexpected cache import")
    if cache_import is not None:
        require(Path(cache_import) == expected_import, "cache import outside exact source export")
    return {"schema_version": 1, "operation": operation, "role": role,
            "output": str(output / "oci"),
            "cache_output": str(output / "cache") if operation in ("source-alpha", "source-secret") else None,
            "cache_import": str(cache_import) if cache_import is not None else None,
            "fixture_sha256": fixture_sha256,
            "payload": {"path": "secret.txt" if secret else "payload.txt", "sha256": driver.sha256(payload), "size": len(payload)},
            "cache_inventory_before": None, "cache_inventory_after": None}


def build_arguments(inputs, fixture, operation):
    """Exact pinned export policy; identity roles never become build arguments."""
    fixture = Path(fixture)
    name = operation["operation"]
    secret = name == "source-secret"
    args = ["buildx", "build", "--builder", inputs["builder"]["name"], "--platform", "linux/arm64",
            "--progress", "rawjson", "--file", str(fixture / "build" / ("Dockerfile.secret" if secret else "Dockerfile")),
            "--provenance=false", "--sbom=false", "--output", "type=oci,dest=" + operation["output"] + OCI_OPTIONS,
            "--build-arg", "FIXTURE_BASE=" + inputs["images"]["base"]["reference"]]
    if secret:
        spec = json.loads(driver.regular(fixture / "fixture.json"))
        values = {"FIXTURE_SECRET_SHA256": spec["secret_input_sha256"]}
    else:
        values = {"FIXTURE_RUN": inputs["run_id"], "FIXTURE_VARIANT": "beta" if name == "source-beta" else "alpha"}
    for key, value in sorted(values.items()):
        args += ["--build-arg", key + "=" + value]
    if operation["cache_output"] is not None:
        args += ["--cache-to", "type=local,dest=" + operation["cache_output"] + CACHE_OPTIONS]
    if operation["cache_import"] is not None:
        args += ["--cache-from", "type=local,src=" + operation["cache_import"]]
    if secret:
        args += ["--no-cache", "--secret", "id=fixture,src=" + str(fixture / "inputs/secret.txt")]
    return args + [str(fixture / "build")]


class ArtifactDriver(driver.Driver):
    def execute(self, operation):
        """One solve, one attempt, pre/post routing and continuous process proof."""
        require(self.record.count == 0, "artifact driver cannot be reused")
        require(operation == specification(operation["operation"], self.output, self.fixture,
                    self.inputs.raw["fixture_sha256"], operation["cache_import"]), "artifact operation contract differs")
        operation = copy.deepcopy(operation)
        secret = driver.regular(self.fixture / "inputs/secret.txt")
        canaries = (secret, secret.rstrip(b"\n"))
        intermediate = driver.regular(self.fixture / "build/intermediate-canary.txt")
        for key in ("output", "cache_output"):
            require(operation[key] is None or not os.path.lexists(operation[key]), "artifact export destination preexists")
        source = operation["cache_import"]
        if source is not None:
            operation["cache_inventory_before"] = stream.inventory_tree(Path(source), canaries=canaries, limits=layout.LIMITS)
            layout.validate_cache(Path(source), canaries=canaries)
        startup.document(self.output / "inputs.json", self.inputs.raw)
        startup.document(self.output / "operation.intent.json", operation)
        require(driver.tree_digest(self.fixture) == operation["fixture_sha256"], "fixture changed before artifact solve")
        self.builder_guard()
        result = self.command(build_arguments(self.inputs.raw, self.fixture, operation), timeout=300)
        self.builder_guard()
        require(not result.stdout, "unexpected artifact build stdout")
        require(driver.tree_digest(self.fixture) == operation["fixture_sha256"], "fixture changed during artifact solve")
        if source is not None:
            operation["cache_inventory_after"] = stream.inventory_tree(Path(source), canaries=canaries, limits=layout.LIMITS)
            require(operation["cache_inventory_before"] == operation["cache_inventory_after"], "imported cache changed during solve")
        payload = operation["payload"]
        proofs = {"oci": layout.validate_oci(Path(operation["output"]), expected_path=payload["path"],
                    expected_sha256=payload["sha256"], expected_size=payload["size"],
                    canaries=(*canaries, intermediate, intermediate.rstrip(b"\n")))}
        if operation["cache_output"] is not None:
            proofs["cache"] = layout.validate_cache(Path(operation["cache_output"]), canaries=canaries)
        require(self.record.count == 9, "artifact operation command inventory differs")
        startup.document(self.output / "operation.json", operation)
        startup.document(self.output / "artifact-validation.json", proofs)
        return operation, proofs


def distinct_roles(builders, descriptor):
    require(set(builders) == set(ROLES), "missing builder role")
    for role, builder in builders.items():
        require(builder.role == role and builder.descriptor == descriptor and builder.prepared,
                "builder role/owner not prepared")
    for attribute in ("name", "node", "container_id", "volume_name"):
        require(len({getattr(builder, attribute) for builder in builders.values()}) == 3,
                "builder roles share runtime/cache identity")


def run_machine(harness, descriptor, scope, proof, images, index):
    from linux_docker_e2e import input_mapping
    from linux_docker_artifact_evidence import validate
    from linux_docker_buildkit_keep import verify_cached_worker_log, verify_worker_log

    root = harness.evidence / ("artifacts-machine-" + str(index))
    require(not os.path.lexists(root), "artifact Machine directory preexists")
    root.mkdir(mode=0o700)
    builders, results, cleanup_indices = {}, [], []
    started = time.time_ns()
    # Create roles just before first use. In particular, no no-import operation
    # can warm the importer while it is waiting for the source cache.
    for name in OPERATIONS:
        imported = root / "source-alpha" / "cache" if name == "fresh-import-alpha" else None
        operation = specification(name, root / name, Path(harness.info["fixture"]), harness.info["fixture_sha256"], imported)
        role = operation["role"]
        if role not in builders:
            builders[role] = harness.prepare_builder(descriptor, role=role, keep_probe=(role == "source"))
        builder = builders[role]
        inputs = input_mapping(harness, scope, proof, images)
        inputs["builder"] = builder.mapping
        admitted = driver.Inputs(inputs, suite="build")
        admitted.verify_runtime_evidence()
        selected = ArtifactDriver(admitted, Path(harness.info["fixture"]), root / name)
        harness.drivers.append(selected)
        cleanup_indices.append(len(harness.driver_cleanup_verified))
        harness.driver_cleanup_verified.append(False)
        operation, artifact_proof = selected.execute(operation)
        replay = validate(selected.output, inputs, operation)
        runtime = builder.verify(require_invocation=(role != "importer"))
        if name in ("source-secret", "fresh-cold-alpha"):
            runtime["post_workload_log"] = verify_worker_log(builder)
        elif role == "importer":
            runtime["no_worker_execution"] = verify_cached_worker_log(builder)
        results.append({"operation": name, "role": role, "builder": builder.ownership,
                        "operation_contract": copy.deepcopy(operation),
                        "mapping": dict(builder.mapping), "artifact_validation": artifact_proof,
                        "independent_validation": replay, "runtime": runtime})
        harness.monitor.check()
    distinct_roles(builders, descriptor)
    alpha, beta, _, cold, imported = results
    layer = lambda row: row["artifact_validation"]["oci"]["layer"]["descriptor"]["digest"]
    payload = lambda row: row["artifact_validation"]["oci"]["payload"]["sha256"]
    require(layer(alpha) != layer(beta) and payload(alpha) != payload(beta), "argument variation did not change OCI payload layer")
    require(payload(alpha) == payload(cold) == payload(imported), "identical inputs changed payload across builder roles")
    require(alpha["independent_validation"]["cache"] == imported["independent_validation"]["imported_cache"],
            "importer did not consume the original source cache proof")
    # A later source solve may add worker-cache state, but must never modify its
    # earlier exported host tree. Repeat full semantic validation at the end.
    for row in results:
        replay = validate(root / row["operation"], input_mapping(harness, scope, proof, images) | {"builder": row["mapping"]},
                          row["operation_contract"])
        require(replay == row["independent_validation"], "artifact proof changed after original solve validation")
    result = {"scope": scope, "started_unix_ns": started, "ended_unix_ns": time.time_ns(),
              "operations": results, "test_case_retries": 0,
              "cache_scan_scope": "complete_exported_cache_only_not_complete_worker_cache",
              "docker_parity_certified": False}
    startup.document(root / "machine-artifact-validation.json", result)
    for position in cleanup_indices:
        harness.driver_cleanup_verified[position] = True
    return result
