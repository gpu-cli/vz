"""Host-driven isolated SSH build operations; not the complete Docker gate."""
import copy
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_artifact_layout as layout
from linux_docker_build_artifacts import OCI_OPTIONS, CACHE_OPTIONS
import linux_docker_ssh_input as packages

require = driver.require
CASES = ("undeclared", "provider_omitted", "wrong_host", "declared")
FIXTURE = Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker-ssh"
FIXTURE_SHA256 = "3472d44f91ed99e3b4e32c7bbaaea2da528426889876849d25692b5e1444908a"
FIXTURE_FILES = {"Dockerfile.server", "Dockerfile.ssh", "Dockerfile.undeclared", "README.md",
                 "contract.json", "package-pins.json", "packages.py", "server.py", "ssh_probe.py",
                 "sshd_config", "test_ssh.py", ".dockerignore"}


def clean_path(value, *, existing=True):
    path = Path(value)
    require(path.is_absolute() and path == path.resolve(strict=existing)
            and not any(c in str(path) for c in ",\x00\n\r"), "canonical delimiter-free SSH path required")
    return path


def fixture_contract(fixture):
    fixture = clean_path(fixture)
    require({p.name for p in fixture.iterdir()} == FIXTURE_FILES
            and all(p.is_file() and not p.is_symlink() for p in fixture.iterdir()), "SSH fixture inventory differs")
    require(driver.tree_digest(fixture) == FIXTURE_SHA256, "unsupported SSH fixture source")
    value = packages.image_input.parse(driver.regular(fixture / "contract.json", 16384))
    require(packages.image_input.parse(driver.regular(fixture / "package-pins.json", 16384)) ==
            packages.guest_manifest(packages.load()), "SSH fixture package projection differs")
    return value


def public_request(value):
    require(type(value) is dict and set(value) == {"schema_version", "token", "host", "port", "host_key_fingerprint"},
            "SSH public request fields differ")
    require(type(value["schema_version"]) is int and value["schema_version"] == 1
            and type(value["port"]) is int and value["port"] == 2222
            and isinstance(value["token"], str) and re.fullmatch(r"vzssh-[0-9a-f]{24}", value["token"]),
            "SSH public request identity differs")
    require(isinstance(value["host"], str), "SSH host must be an inspected IPv4 string")
    address = ipaddress.IPv4Address(value["host"])
    require(str(address) == value["host"] and address.is_private and not any((address.is_loopback,
            address.is_link_local, address.is_multicast, address.is_unspecified)), "SSH server must be Machine-private IPv4")
    require(isinstance(value["host_key_fingerprint"], str) and
            re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", value["host_key_fingerprint"]), "invalid server key fingerprint")
    return copy.deepcopy(value)


def stage_context(fixture, package_source, destination, *, request=None, known_hosts=None):
    """Stage a closed public input inventory; never accept a private key path."""
    fixture = clean_path(fixture)
    fixture_contract(fixture)
    destination = clean_path(destination, existing=False)
    require(destination.parent.is_dir() and not os.path.lexists(destination), "fresh SSH context required")
    require((request is None) == (known_hosts is None), "SSH public inputs must be paired")
    if request is not None:
        request = public_request(request)
        require(type(known_hosts) is bytes and len(known_hosts) <= 256
                and known_hosts.startswith(("[" + request["host"] + "]:2222 ssh-ed25519 ").encode())
                and known_hosts.endswith(b"\n") and known_hosts.count(b"\n") == 1,
                "invalid public known-host record")
    destination.mkdir(mode=0o700)
    for name in sorted(FIXTURE_FILES):
        startup.write(destination / name, driver.regular(fixture / name, 128 * 1024))
    packages.stage_packages(package_source, destination / "packages")
    if request is not None:
        (destination / "inputs").mkdir(mode=0o700)
        startup.document(destination / "inputs/request.json", request)
        startup.write(destination / "inputs/known_hosts", known_hosts)
    require(driver.tree_digest(fixture) == FIXTURE_SHA256, "SSH source changed during staging")
    return {"path": str(destination), "sha256": driver.tree_digest(destination)}


def build_arguments(inputs, operation):
    name = "Dockerfile.undeclared" if operation["case"] == "undeclared" else "Dockerfile.ssh"
    context = Path(operation["build_context"])
    args = ["buildx", "build", "--builder", inputs["builder"]["name"], "--platform", "linux/arm64",
            "--progress", "rawjson", "--file", str(context / name), "--provenance=false", "--sbom=false",
            "--output", "type=oci,dest=" + operation["output"] + OCI_OPTIONS,
            "--build-arg", "FIXTURE_BASE=" + inputs["images"]["base"]["reference"],
            "--no-cache", "--network=default"]
    if operation["cache_output"] is not None:
        args += ["--cache-to", "type=local,dest=" + operation["cache_output"] + CACHE_OPTIONS]
    if operation["agent_socket"] is not None:
        args += ["--ssh", "fixture=" + operation["agent_socket"]]
    return args + [str(context)]


def specification(case, output, inputs, context, request, agent_socket, *, fixture=FIXTURE):
    require(case in CASES, "unknown SSH build case")
    fixture_contract(fixture)
    output, context = clean_path(output, existing=False), clean_path(context)
    require(output.parent.is_dir(), "operation parent must already exist")
    request = public_request(request)
    require(packages.image_input.parse(driver.regular(context / "inputs/request.json", 16384)) == request,
            "staged SSH request differs")
    require((agent_socket is None) == (case == "provider_omitted"), "SSH provider presence differs from case")
    if agent_socket is not None:
        socket = clean_path(agent_socket)
        require(stat.S_ISSOCK(socket.stat().st_mode) and context not in socket.parents
                and output not in socket.parents, "SSH provider must be an external owned agent socket")
        agent_socket = str(socket)
    operation = {"schema_version": 1, "case": case, "run_id": inputs["run_id"],
                 "ssh_fixture": str(fixture), "ssh_fixture_sha256": FIXTURE_SHA256,
                 "build_context": str(context), "build_context_sha256": driver.tree_digest(context),
                 "request": request, "known_hosts_sha256": driver.sha256(driver.regular(context / "inputs/known_hosts", 256)),
                 "agent_socket": agent_socket, "output": str(output / "oci"),
                 "cache_output": str(output / "cache") if case == "declared" else None}
    operation["build_argv"] = build_arguments(inputs, operation)
    return operation


class SSHDriver(driver.Driver):
    def __init__(self, inputs, fixture, output, *, canaries):
        super().__init__(inputs, fixture, output)
        require(type(canaries) in (tuple, list) and bool(canaries) and
                all(type(item) is bytes and len(item) >= 32 for item in canaries), "private SSH canaries required in memory")
        self.record.canaries.extend(canaries)

    def execute(self, operation):
        from linux_docker_ssh_evidence import validate_operation
        require(self.record.count == 0, "SSH operation cannot be reused")
        expected = specification(operation["case"], self.output, self.inputs.raw, operation["build_context"],
                                 operation["request"], operation["agent_socket"], fixture=Path(operation["ssh_fixture"]))
        require(operation == expected, "SSH operation differs from fixed specification")
        for key in ("output", "cache_output"):
            require(operation[key] is None or not os.path.lexists(operation[key]), "SSH export preexists")
        startup.document(self.output / "inputs.json", self.inputs.raw)
        startup.document(self.output / "operation.intent.json", operation)
        self.builder_guard()
        command = self.command(operation["build_argv"], expected=None, timeout=300)
        self.builder_guard()
        require(self.record.count == 9 and not command.stdout and not command.timed_out,
                "SSH operation host command inventory differs")
        require(driver.tree_digest(Path(operation["build_context"])) == operation["build_context_sha256"]
                and driver.tree_digest(Path(operation["ssh_fixture"])) == operation["ssh_fixture_sha256"]
                and driver.tree_digest(self.fixture) == self.inputs.raw["fixture_sha256"], "SSH input changed during build")
        success = operation["case"] == "declared"
        require(command.returncode == (0 if success else 1), "SSH solve host exit differs")
        proofs = {"oci": None, "cache": None}
        if success:
            payload = ("vz-ssh-response:" + operation["request"]["token"] + "\n").encode()
            proofs["oci"] = layout.validate_oci(Path(operation["output"]), expected_path="ssh.txt",
                expected_sha256=driver.sha256(payload), expected_size=len(payload), canaries=tuple(self.record.canaries))
            proofs["cache"] = layout.validate_cache(Path(operation["cache_output"]), canaries=tuple(self.record.canaries))
        else:
            exported = Path(operation["output"])
            require(not os.path.lexists(exported) or (exported.is_dir() and not exported.is_symlink()
                    and not list(exported.iterdir())), "failed SSH solve produced output")
        startup.document(self.output / "operation.json", operation)
        startup.document(self.output / "artifact-validation.json", proofs)
        # The full independent raw replay establishes the precise denial before
        # a nonzero mutation can become certain. Original terminal bytes stay.
        validate_operation(self.output, self.inputs.raw, operation,
                           secret_canaries=tuple(self.record.canaries), require_ack=False)
        if not success:
            self.record.acknowledge_negative(command, "terminal BuildKit SSH fixture " + operation["case"] + " denial")
        replay = validate_operation(self.output, self.inputs.raw, operation, secret_canaries=tuple(self.record.canaries))
        return {"operation_contract": copy.deepcopy(operation), "artifact_validation": proofs,
                "independent_validation": replay}


def run_machine(harness, descriptor, scope, proof, images, index):
    """Four actual solves and owned server/agent closure on one Linux Machine.

    The parent must still capture/scan the stopped worker cache before deleting
    its builder. This function cannot certify that later lifecycle boundary.
    """
    from linux_docker_e2e import input_mapping
    from linux_docker_ssh_agent import Agent
    from linux_docker_ssh_server import Server
    from linux_docker_ssh_evidence import validate_operation
    from linux_docker_buildkit_keep import verify_worker_log

    require(type(index) is int and 0 <= index < 3, "invalid SSH Machine ordinal")
    root = harness.evidence / ("ssh-machine-" + str(index))
    require(not os.path.lexists(root), "SSH Machine evidence preexists")
    root.mkdir(mode=0o700)
    contexts = root / "contexts"
    contexts.mkdir(mode=0o700)
    builder = harness.prepare_builder(descriptor)
    inputs = input_mapping(harness, scope, proof, images) | {"builder": builder.mapping}
    admitted = driver.Inputs(inputs, suite="build")
    admitted.verify_runtime_evidence()
    agent = Agent(harness.root / ("ssh-private-" + str(index)), root / "agent",
                  tools=harness.info["ssh_tools"], run_id=inputs["run_id"] + "-" + str(index), owner=descriptor["owner"])
    indices, results, selected = [], [], []
    original_error, closure = None, None
    started = time.time_ns()
    try:
        ready = agent.start()
        canaries = agent.canaries()
        harness.sensitive_canaries.extend(canaries)
        harness.record.canaries.extend(canaries)
        if harness.monitor is not None:
            harness.monitor.record.canaries.extend(canaries)
        fixture = Path(harness.info["ssh_fixture"])
        source = Path(harness.info["ssh_packages"])
        server_context = stage_context(fixture, source, contexts / "server")
        server = Server(admitted, Path(harness.info["fixture"]), root / "server",
                        Path(server_context["path"]), agent, "vzssh-" + uuid.uuid4().hex[:24])
        indices.append(len(harness.drivers))
        harness.drivers.append(server.driver)
        harness.driver_cleanup_verified.append(False)
        request = public_request(server.prepare())
        require(request["host_key_fingerprint"] == ready["fingerprints"]["host"], "server adopted another host key")
        for case in CASES:
            harness.monitor.check()
            server.verify()
            role = "wrong_host_public_key" if case == "wrong_host" else "host_public_key"
            fields = driver.regular(agent.paths[role], 1024).split()
            require(len(fields) == 3 and fields[0] == b"ssh-ed25519", "foreign generated host key type")
            known = ("[" + request["host"] + "]:2222 ").encode() + b" ".join(fields[:2]) + b"\n"
            staged = stage_context(fixture, source, contexts / case, request=request, known_hosts=known)
            operation = specification(case, root / case, inputs, staged["path"], request,
                                      None if case == "provider_omitted" else agent.paths["socket"], fixture=fixture)
            item = SSHDriver(admitted, Path(harness.info["fixture"]), root / case, canaries=canaries)
            indices.append(len(harness.drivers))
            harness.drivers.append(item)
            harness.driver_cleanup_verified.append(False)
            selected.append(item)
            results.append(item.execute(operation))
            server.verify()
        runtime = builder.verify(require_invocation=True)
        runtime["post_workload_log"] = verify_worker_log(builder)
        for item, result in zip(selected, results):
            require(validate_operation(item.output, inputs, result["operation_contract"], secret_canaries=canaries)
                    == result["independent_validation"], "SSH proof changed after its original solve")
        server.cleanup_authorized = True
        server_cleanup = server.cleanup()
        # Keep only in-memory canaries for the parent's stopped-cache boundary.
        # They must never be included in its public JSON input/result documents.
        harness.ssh_cache_requests.append({"builder": builder, "canaries": canaries, "index": index})
        result = {"scope": scope, "started_unix_ns": started, "ended_unix_ns": time.time_ns(),
                  "operations": results, "runtime": runtime, "server_cleanup": server_cleanup,
                  "cache_scan_scope": "exported_positive_cache_pending_stopped_worker_scan",
                  "test_case_retries": 0, "docker_parity_certified": False}
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            closure = agent.close()
            startup.document(root / "host-agent-closure.json", closure)
        except BaseException as error:
            if original_error is not None:
                raise RuntimeError("SSH workload failed; disposable host agent closure also remains unproven") from original_error
            raise error
    require(closure["agent_reaped"] and closure["private_inputs_removed"] and not closure["cleanup_errors"],
            "SSH host private-input closure incomplete")
    result["host_agent_cleanup"] = closure
    startup.document(root / "machine-ssh-validation.json", result)
    for index in indices:
        harness.driver_cleanup_verified[index] = True
    return result
