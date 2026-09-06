"""Four actual concurrent BuildKit workers, separately owned and replayed.

This explicit DEV lane is not the complete Docker compatibility contract. Each
slot has an independent command recorder; a thread or client lifetime alone is
never evidence that the guest RUN workers overlapped.
"""
import copy
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import threading
import time

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_artifact_layout as layout
from linux_docker_build_artifacts import OCI_OPTIONS

require = driver.require
SLOTS = tuple(range(4))
CONTRACT = {"schema_version": 1, "scenario": "parallel_builds", "cache_id": "vz04-parallel-barrier-v1",
            "network": "none", "payload_template": "vz04-parallel-v1\nslot=N\n", "payload_mode": 420,
            "transcript_prefix": "VZ_PARALLEL_BARRIER=",
            "barrier": {"workers": 4, "timeout_ns": 60_000_000_000, "poll_interval_ns": 100_000_000,
                        "release_dwell_ns": 1_000_000_000, "max_samples": 602, "max_record_bytes": 1024},
            "health": {"samples": 60, "interval_ns": 1_000_000_000, "max_lateness_ns": 250_000_000,
                       "request_timeout_ns": 500_000_000, "observer_bound_ns": 70_000_000_000}}


def fixture_contract(fixture):
    """Fail before provisioning for unsupported fixture policy or source tree.

    The full separate fixture tree is hash-bound. In particular, generated
    bytecode is not accepted as a hidden source input in this new fixture.
    """
    expected = {"Dockerfile.parallel", "parallel.py", "health.py", "contract.json", ".dockerignore",
                "test_parallel.py", "README.md"}
    require({p.name for p in fixture.iterdir()} == expected, "parallel fixture inventory differs")
    require(all(p.is_file() and not p.is_symlink() for p in fixture.iterdir()), "redirected parallel fixture input")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate parallel contract field")
            result[key] = value
        return result
    contract = json.loads(driver.regular(fixture / "contract.json"), object_pairs_hook=unique)
    # Canonical JSON comparison distinguishes true from 1, unlike dict equality.
    require(json.dumps(contract, sort_keys=True) == json.dumps(CONTRACT, sort_keys=True),
            "unsupported parallel timing or workload contract")
    return contract


def specification(slot, output, fixture, fixture_sha256, run_id):
    require(type(slot) is int and slot in SLOTS, "invalid parallel slot")
    output, fixture = Path(output), Path(fixture)
    require(output.is_absolute() and output.parent == output.parent.resolve(), "canonical slot directory required")
    require(fixture.is_absolute() and fixture == fixture.resolve(), "canonical parallel fixture required")
    require(all(not any(c in str(p) for c in (",", "\x00", "\n", "\r")) for p in (output, fixture)),
            "parallel path contains exporter delimiters")
    require(isinstance(fixture_sha256, str) and re.fullmatch(r"[0-9a-f]{64}", fixture_sha256), "invalid parallel fixture digest")
    driver.checked_text(run_id, r"[a-z0-9][a-z0-9-]{7,39}", "parallel run ID")
    payload = f"vz04-parallel-v1\nslot={slot}\n".encode()
    return {"schema_version": 1, "slot": slot, "parallel_fixture": str(fixture),
            "parallel_fixture_sha256": fixture_sha256, "output": str(output / "oci"),
            "payload": {"path": "payload.txt", "sha256": driver.sha256(payload), "size": len(payload)},
            "run_id": run_id}


def build_arguments(inputs, operation):
    fixture = Path(operation["parallel_fixture"])
    return ["buildx", "build", "--builder", inputs["builder"]["name"], "--platform", "linux/arm64",
            "--progress", "rawjson", "--file", str(fixture / "Dockerfile.parallel"),
            "--provenance=false", "--sbom=false", "--output", "type=oci,dest=" + operation["output"] + OCI_OPTIONS,
            "--build-arg", "FIXTURE_BASE=" + inputs["images"]["base"]["reference"],
            "--build-arg", "FIXTURE_RUN=" + operation["run_id"],
            "--build-arg", "FIXTURE_SLOT=" + str(operation["slot"]),
            "--network=none", str(fixture)]


class ParallelDriver(driver.Driver):
    def execute(self, operation):
        require(self.record.count == 0, "parallel driver cannot be reused")
        require(operation == specification(operation["slot"], self.output, Path(operation["parallel_fixture"]),
                    operation["parallel_fixture_sha256"], self.inputs.raw["run_id"]), "parallel operation contract differs")
        operation = copy.deepcopy(operation)
        require(not os.path.lexists(operation["output"]), "parallel export already exists")
        fixture = Path(operation["parallel_fixture"])
        require(driver.tree_digest(fixture) == operation["parallel_fixture_sha256"], "parallel fixture changed before solve")
        startup.document(self.output / "inputs.json", self.inputs.raw)
        startup.document(self.output / "operation.intent.json", operation)
        self.builder_guard()
        result = self.command(build_arguments(self.inputs.raw, operation), timeout=300)
        self.builder_guard()
        require(not result.stdout and self.record.count == 9, "parallel command inventory/output differs")
        require(driver.tree_digest(fixture) == operation["parallel_fixture_sha256"], "parallel fixture changed during solve")
        require(driver.tree_digest(self.fixture) == self.inputs.raw["fixture_sha256"], "base fixture changed during parallel solve")
        secret = driver.regular(self.fixture / "inputs/secret.txt")
        intermediate = driver.regular(self.fixture / "build/intermediate-canary.txt")
        payload = operation["payload"]
        proofs = {"oci": layout.validate_oci(Path(operation["output"]), expected_path=payload["path"],
            expected_sha256=payload["sha256"], expected_size=payload["size"],
            canaries=(secret, secret.rstrip(b"\n"), intermediate, intermediate.rstrip(b"\n")))}
        startup.document(self.output / "operation.json", operation)
        startup.document(self.output / "artifact-validation.json", proofs)
        return operation, proofs


def execute_slots(selected, operations):
    """Join every dispatched observer, including when another slot fails.

    Each bounded command retains its own uncertainty state. No failure causes a
    retry, client restart, repair, or premature cleanup admission.
    """
    require(len(selected) == len(operations) == 4, "exactly four slot drivers required")
    require([op["slot"] for op in operations] == list(SLOTS), "parallel slot order differs")
    require(len({id(item) for item in selected}) == 4, "parallel recorder reused")
    barrier = threading.Barrier(4, timeout=10)

    def run(index):
        barrier.wait()
        return selected[index].execute(operations[index])

    results, failures = [None] * 4, []
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="vz-parallel-build") as executor:
        futures = [executor.submit(run, index) for index in SLOTS]
        for index, future in enumerate(futures):
            try:
                results[index] = future.result()
            except BaseException as error:
                failures.append((index, error))
    if failures:
        raise RuntimeError("parallel slots failed: " + ",".join(str(index) for index, _ in failures)) from failures[0][1]
    return results


def run_machine(harness, descriptor, scope, proof, images, index):
    from linux_docker_e2e import input_mapping
    from linux_docker_parallel_evidence import validate_slot, validate_group
    from linux_docker_parallel_health import Health
    from linux_docker_buildkit_keep import verify_worker_log

    root = harness.evidence / ("parallel-machine-" + str(index))
    require(not os.path.lexists(root), "parallel Machine evidence preexists")
    root.mkdir(mode=0o700)
    builder = harness.prepare_builder(descriptor)
    inputs = input_mapping(harness, scope, proof, images) | {"builder": builder.mapping}
    admitted = driver.Inputs(inputs, suite="build")
    admitted.verify_runtime_evidence()
    selected, operations, positions = [], [], []
    for slot in SLOTS:
        output = root / ("slot-" + str(slot))
        operation = specification(slot, output, Path(harness.info["parallel_fixture"]),
                                  harness.info["parallel_fixture_sha256"], inputs["run_id"])
        item = ParallelDriver(admitted, Path(harness.info["fixture"]), output)
        positions.append(len(harness.drivers))
        harness.drivers.append(item)
        harness.driver_cleanup_verified.append(False)
        selected.append(item)
        operations.append(operation)
    health = Health(harness, descriptor, images, index)
    positions.append(len(harness.drivers))
    harness.drivers.append(health)
    harness.driver_cleanup_verified.append(False)
    health.prepare()
    started = time.time_ns()
    intervals = []
    work_error = None
    try:
        health.start()
        solved = execute_slots(selected, operations)
        rows = []
        for item, (operation, artifact_proof) in zip(selected, solved):
            replay = validate_slot(item.output, inputs, operation)
            rows.append({"operation_contract": copy.deepcopy(operation), "artifact_validation": artifact_proof,
                         "independent_validation": replay})
        group = validate_group([row["independent_validation"] for row in rows])
        intervals = [(row["independent_validation"]["run_interval"]["started_ns"],
                      row["independent_validation"]["run_interval"]["completed_ns"]) for row in rows]
    except BaseException as error:
        work_error = error
        raise
    finally:
        # Even failed builds cannot abandon an active health observer. finish
        # positively joins it before checking the interval assertions.
        try:
            health_proof = health.finish(intervals)
        except BaseException as health_error:
            if work_error is not None:
                raise RuntimeError("parallel workload failed: " + str(work_error) +
                                   "; health verification also failed: " + str(health_error)) from work_error
            raise
    runtime = builder.verify(require_invocation=True)
    runtime["post_workload_log"] = verify_worker_log(builder)
    for item, row in zip(selected, rows):
        require(validate_slot(item.output, inputs, row["operation_contract"]) == row["independent_validation"],
                "parallel proof changed after original solve")
    require(validate_group([row["independent_validation"] for row in rows]) == group, "parallel group proof changed")
    harness.monitor.check()
    result = {"scope": scope, "started_unix_ns": started, "ended_unix_ns": time.time_ns(),
              "builder": builder.ownership, "operations": rows, "group": group, "health": health_proof,
              "runtime": runtime, "test_case_retries": 0, "docker_parity_certified": False}
    startup.document(root / "machine-parallel-validation.json", result)
    for position in positions:
        harness.driver_cleanup_verified[position] = True
    return result
