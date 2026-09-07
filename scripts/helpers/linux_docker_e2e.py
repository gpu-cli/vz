#!/usr/bin/env python3
"""DEV installed Linux-on-macOS Docker slices; not certification.

Normal installed Up provisions four private Machines. All workload commands use
their authenticated contexts. Daily installation/configuration is untouched.
Full --suite all is deliberately rejected before provisioning until the complete
63-scenario contract is implemented. Retained stopped Machine disks are NOT Delete.
Owned BuildKit builder cache volumes are removed by successful workload cleanup.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
import sys
import threading
import time
import uuid

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_lane_result as lane_result
import linux_docker_image_input as image_input

SCOPE = "DEV_INSTALLED_LINUX_COMPOSE_NOT_RELEASE_CERTIFICATION"
BUILD_SCOPE = "DEV_INSTALLED_LINUX_BUILDX_NOT_RELEASE_CERTIFICATION"
ARTIFACT_SCOPE = "DEV_INSTALLED_LINUX_BUILD_ARTIFACTS_NOT_RELEASE_CERTIFICATION"
PARALLEL_SCOPE = "DEV_INSTALLED_LINUX_PARALLEL_BUILD_NOT_RELEASE_CERTIFICATION"
SSH_SCOPE = "DEV_INSTALLED_LINUX_SSH_BUILD_NOT_RELEASE_CERTIFICATION"
LIFECYCLE_SCOPE = "DEV_INSTALLED_LINUX_CONTAINER_LIFECYCLE_NOT_RELEASE_CERTIFICATION"
IMAGES_SCOPE = "DEV_INSTALLED_LINUX_IMAGE_ROUNDTRIP_NOT_RELEASE_CERTIFICATION"
REGISTRY_SCOPE = "DEV_INSTALLED_LINUX_REGISTRY_LOGIN_PUSH_PULL_NOT_RELEASE_CERTIFICATION"
HANDSHAKE_SCOPE = "DEV_INSTALLED_LINUX_ENGINE_HANDSHAKE_NOT_RELEASE_CERTIFICATION"
LIMITS_SCOPE = "DEV_INSTALLED_LINUX_RESOURCE_LIMITS_OOM_NOT_RELEASE_CERTIFICATION"
RECOVERY_SCOPE = "DEV_INSTALLED_LINUX_PERSISTENCE_STOP_UP_RECOVERY_NOT_RELEASE_CERTIFICATION"
SUITES = ("compose", "build", "artifacts", "parallel", "ssh", "lifecycle", "images", "registry", "handshake", "limits", "recovery")
REPO = Path(__file__).resolve().parents[2]
LABEL = "dev.vz.linux-compose-proof"
require = driver.require


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    names = (*startup.OPTIONS, "suite", "fixture", "image-input", "run-id", "buildkit-archive", "parallel-fixture",
             "ssh-fixture", "ssh-packages", "ssh-gpgv", "container-fixture", "tmux", "registry-archive", "registry-layout",
             *lane_result.GATE_OPTIONS)
    for name in names:
        require(sum(x == "--" + name or x.startswith("--" + name + "=") for x in argv) <= 1,
                "duplicate option: --" + name)
    # Admit the suite before demanding provisioning inputs. `all` must fail even
    # on hosts lacking artifacts, without running a client or creating a file.
    parser.add_argument("--suite", required=True, choices=(*SUITES, "all"))
    # Accepted for every suite: the aggregate gate passes its identity through
    # argv. They select no behaviour here beyond emitting a lane result.
    for name in lane_result.GATE_OPTIONS:
        parser.add_argument("--" + name)
    for name in startup.OPTIONS:
        parser.add_argument("--" + name)
    parser.add_argument("--fixture", default=str(REPO / "tests/fixtures/vz-0.4/docker"))
    parser.add_argument("--image-input", default=str(REPO / "tests/fixtures/vz-0.4/docker/python-image-input.json"))
    parser.add_argument("--run-id")
    parser.add_argument("--buildkit-archive")
    parser.add_argument("--parallel-fixture")
    parser.add_argument("--ssh-fixture")
    parser.add_argument("--ssh-packages")
    parser.add_argument("--ssh-gpgv")
    parser.add_argument("--container-fixture")
    parser.add_argument("--tmux")
    parser.add_argument("--registry-archive")
    parser.add_argument("--registry-layout")
    args = parser.parse_args(argv)
    require(args.suite in SUITES, "full 63-scenario --suite all is not implemented; no workload dispatched")
    for name in ("registry_archive", "registry_layout"):
        require((getattr(args, name) is not None) == (args.suite == "registry"),
                "--" + name.replace("_", "-") + " is required only for the registry suite")
    require(args.container_fixture is None or args.suite == "lifecycle", "container-fixture requires the lifecycle suite")
    require((args.tmux is not None) == (args.suite == "lifecycle"),
            "--tmux is required only for the lifecycle suite")
    require(args.parallel_fixture is None or args.suite == "parallel", "parallel-fixture requires the parallel suite")
    require(args.suite == "ssh" or all(getattr(args, name) is None for name in ("ssh_fixture", "ssh_packages", "ssh_gpgv")),
            "SSH options require the ssh suite")
    require(args.suite != "ssh" or args.ssh_packages is not None, "--ssh-packages is required for the ssh suite")
    if args.run_id is None:
        args.run_id = args.suite + "-" + uuid.uuid4().hex[:24]
    for name in startup.OPTIONS:
        require(getattr(args, name.replace("-", "_")) is not None, "required option: --" + name)
    driver.checked_text(args.run_id, r"[a-z0-9][a-z0-9-]{7,39}", "run ID")
    require((args.buildkit_archive is not None) == (args.suite in {"build", "artifacts", "parallel", "ssh"}),
            "--buildkit-archive is required only for Buildx suites")
    return args


def preflight(args, require_host=True):
    require(args.suite in SUITES, "full contract unavailable")
    if args.suite in ('images', 'registry', 'handshake', 'limits', 'recovery'):
        require(all(getattr(args, name, None) is None for name in ('buildkit_archive', 'parallel_fixture',
                'ssh_fixture', 'ssh_packages', 'ssh_gpgv', 'container_fixture', 'tmux')),
                ('image' if args.suite == 'images' else args.suite) + ' suite rejects builder, foreign fixture and terminal options')
    for name in ('registry_archive', 'registry_layout'):
        require((getattr(args, name, None) is not None) == (args.suite == 'registry'),
                '--' + name.replace('_', '-') + ' is required only for the registry suite')
    if args.suite == 'registry':
        # Admit every registry input read-only before startup preflight touches
        # anything: pinned layout, exact archive bytes, unexecuted binary
        # metadata and the isolated Python dependencies behind the fixture.
        import linux_docker_registry_machine as registry_machine
        registry_archive = startup.canonical(args.registry_archive)
        registry_layout = startup.canonical(args.registry_layout)
        registry = registry_machine.admit_inputs(registry_archive, registry_layout)
    require((getattr(args, "tmux", None) is not None) == (args.suite == "lifecycle"),
            "--tmux is required only for the lifecycle suite")
    terminal = tmux_input(args.tmux) if args.suite == "lifecycle" else None
    info = startup.preflight(args, require_host=require_host)
    fixture = startup.canonical(args.fixture)
    pin_path = startup.canonical(args.image_input)
    pin = image_input.load(pin_path)
    ca_path = REPO / "linux/ca-trust/inputs.json"
    ca_pin = public_ca_input(ca_path)
    scopes = {"compose": SCOPE, "build": BUILD_SCOPE, "artifacts": ARTIFACT_SCOPE, "parallel": PARALLEL_SCOPE,
              "ssh": SSH_SCOPE, "lifecycle": LIFECYCLE_SCOPE, "images": IMAGES_SCOPE, "registry": REGISTRY_SCOPE,
              "handshake": HANDSHAKE_SCOPE, "limits": LIMITS_SCOPE, "recovery": RECOVERY_SCOPE}
    info.update(scope=scopes[args.suite], suite=args.suite,
                run_id=args.run_id, fixture=str(fixture),
                fixture_sha256=driver.tree_digest(fixture), python_image=pin, image_input=str(pin_path),
                public_ca=ca_pin)
    for path in (Path(__file__).resolve(), REPO / "scripts/run-linux-docker-e2e.sh", pin_path,
                 ca_path, REPO / "linux/ca-trust/cacert.pem", REPO / "linux/ca-trust/install.sh",
                 REPO / "linux/ca-trust.py", REPO / "linux/initramfs/init",
                 REPO / "scripts/helpers/linux_docker_image_input.py",
                 REPO / "scripts/helpers/linux_docker_compose_evidence.py"):
        info["inputs"][str(path)] = startup.digest(path)
    if args.suite in {"build", "artifacts", "parallel", "ssh"}:
        import linux_docker_buildkit_builder as builder
        archive = startup.canonical(args.buildkit_archive)
        info["buildkit"] = builder.preflight_archive(archive)
        info["inputs"][str(archive)] = startup.digest(archive)
        for path in (REPO / "scripts/helpers/linux_docker_buildkit_builder.py",
                     REPO / "scripts/helpers/linux_docker_buildkit_cgroup.py",
                     REPO / "scripts/helpers/linux_docker_buildkit_shutdown.py",
                     REPO / "scripts/helpers/linux_docker_buildkit_keep.py",
                     REPO / "scripts/helpers/linux_docker_build_evidence.py",
                     REPO / "config/buildkit-artifact-v0.19.0.json"):
            info["inputs"][str(path)] = startup.digest(path)
    if args.suite in {"artifacts", "parallel", "ssh"}:
        for name in ("linux_docker_artifact_stream.py", "linux_docker_artifact_layout.py",
                     "linux_docker_build_artifacts.py", "linux_docker_artifact_evidence.py"):
            path = REPO / "scripts/helpers" / name
            info["inputs"][str(path)] = startup.digest(path)
    if args.suite == "parallel":
        from linux_docker_build_parallel import fixture_contract
        selected = startup.canonical(getattr(args, "parallel_fixture", None) or
                                     str(REPO / "tests/fixtures/vz-0.4/docker-parallel"))
        fixture_contract(selected)
        info.update(parallel_fixture=str(selected), parallel_fixture_sha256=driver.tree_digest(selected))
        for name in ("linux_docker_build_parallel.py", "linux_docker_parallel_evidence.py", "linux_docker_parallel_health.py"):
            path = REPO / "scripts/helpers" / name
            info["inputs"][str(path)] = startup.digest(path)
        for path in selected.rglob("*"):
            if path.is_file():
                info["inputs"][str(path)] = startup.digest(path)
    if args.suite == "ssh":
        from linux_docker_build_ssh import fixture_contract
        from linux_docker_ssh_agent import tool_inputs
        import linux_docker_ssh_input as ssh_input
        selected = startup.canonical(getattr(args, "ssh_fixture", None) or str(REPO / "tests/fixtures/vz-0.4/docker-ssh"))
        fixture_contract(selected)
        source = startup.canonical(args.ssh_packages)
        pin = ssh_input.load(image_path=pin_path)
        package_rows = [pin["base"]["keyring"], pin["release"], pin["packages_index"], *pin["packages"], *pin["source_proofs"]]
        for row in package_rows:
            ssh_input.read_input(source, row)
            info["inputs"][str(source / row["filename"])] = row["sha256"]
        gpgv = startup.canonical(getattr(args, "ssh_gpgv", None) or "/opt/homebrew/bin/gpgv", links=True)
        info.update(ssh_fixture=str(selected), ssh_fixture_sha256=driver.tree_digest(selected), ssh_packages=str(source),
                    ssh_tools=tool_inputs(), ssh_gpgv={"path": str(gpgv), "sha256": startup.digest(gpgv)})
        for row in [info["ssh_gpgv"], *info["ssh_tools"].values()]:
            info["inputs"][row["path"]] = row["sha256"]
        for name in ("linux_docker_build_ssh.py", "linux_docker_ssh_agent.py", "linux_docker_ssh_server.py",
                     "linux_docker_ssh_evidence.py", "linux_docker_ssh_cache.py", "linux_docker_ssh_cache_capture.py",
                     "linux_docker_ssh_input.py", "linux_docker_debian.py", "linux_docker_parallel_evidence.py"):
            path = REPO / "scripts/helpers" / name
            info["inputs"][str(path)] = startup.digest(path)
        info["inputs"][str(ssh_input.PIN)] = startup.digest(ssh_input.PIN)
        for path in selected.iterdir():
            info["inputs"][str(path)] = startup.digest(path)
    if args.suite == 'images':
        from linux_docker_image_machine import required_source_paths
        for path in required_source_paths():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if args.suite == 'registry':
        info.update(registry=registry, registry_archive=str(registry_archive), registry_layout=str(registry_layout))
        info['inputs'][str(registry_archive)] = registry['archive_sha256']
        for path in registry_machine.required_source_paths():
            info['inputs'][str(path)] = startup.digest(Path(path))
        require(info['inputs'][str(registry_machine.PIN)] == startup.digest(registry_machine.PIN) and
                startup.digest(registry_archive) == registry['archive_sha256'], 'registry pin or archive changed')
    if args.suite == 'handshake':
        from linux_docker_handshake_machine import manifest_expectations, tool_inputs
        from linux_docker_handshake_machine import required_source_paths as handshake_sources
        # Manifest-versus-upstream pins are checked before any client execution.
        manifest_expectations()
        for path in handshake_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
        for row in tool_inputs().values():
            info['inputs'][row['path']] = row['sha256']
    if args.suite == 'limits':
        from linux_docker_limits_machine import fixture_contract as limits_fixture_contract
        from linux_docker_limits_machine import required_source_paths as limits_sources
        limits_fixture_contract()
        # The sibling health probe reuses the parallel fixture's health service.
        selected = startup.canonical(str(REPO / "tests/fixtures/vz-0.4/docker-parallel"))
        info.update(parallel_fixture=str(selected), parallel_fixture_sha256=driver.tree_digest(selected))
        for path in limits_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
        for path in selected.rglob("*"):
            if path.is_file():
                info['inputs'][str(path)] = startup.digest(path)
    if args.suite == 'recovery':
        from linux_docker_recovery_machine import fixture_contract as recovery_fixture_contract
        from linux_docker_recovery_machine import required_source_paths as recovery_sources
        recovery_fixture_contract()
        for path in recovery_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if args.suite == "lifecycle":
        from linux_docker_container_fixture import fixture_contract
        from linux_docker_container_process_evidence import required_source_paths
        from linux_docker_runtime_audit_evidence import required_source_paths as audit_source_paths
        selected = startup.canonical(getattr(args, "container_fixture", None) or
                                     str(REPO / "tests/fixtures/vz-0.4/docker-container-io"))
        fixture_contract(selected)
        info.update(container_fixture=str(selected), container_fixture_sha256=driver.tree_digest(selected))
        info['tmux'] = terminal
        info['inputs'][terminal['path']] = terminal['sha256']
        python = startup.canonical(sys.executable, links=True)
        info['inputs'][str(python)] = startup.digest(python)
        for path in [*required_source_paths(), *audit_source_paths()]:
            info['inputs'][str(path)] = startup.digest(path)
        for name in ("linux_docker_container_lifecycle.py", "linux_docker_container_state.py",
                     "linux_docker_container_commands.py", "linux_docker_container_fixture.py",
                     "linux_docker_container_exec.py", "linux_docker_container_follow.py",
                     "linux_docker_container_kill.py",
                     "linux_docker_interactive_capture.py", "linux_docker_interactive_evidence.py",
                     "linux_docker_container_tmux.py", "linux_docker_interactive_tmux.py",
                     "linux_docker_buildkit_shutdown.py"):
            path = REPO / "scripts/helpers" / name
            info["inputs"][str(path)] = startup.digest(path)
        for path in selected.iterdir():
            info["inputs"][str(path)] = startup.digest(path)
    return info


def tmux_input(value):
    """Pin an explicitly selected executable without running it or searching PATH."""
    path = startup.canonical(value)
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and
            0 < before.st_size <= 64 * 1024 * 1024 and os.access(path, os.X_OK),
            'tmux requires a bounded single-link regular executable')
    digest = startup.digest(path)
    after = path.lstat()
    signature = lambda row: (row.st_dev, row.st_ino, row.st_mode, row.st_nlink, row.st_uid,
                             row.st_size, row.st_mtime_ns, row.st_ctime_ns)
    require(signature(before) == signature(after) and path == path.resolve(strict=True),
            'tmux executable changed during admission')
    return {'path': str(path), 'sha256': digest}


def public_ca_input(path):
    # Vendored public source is normally 0644, unlike private runtime receipts.
    raw = image_input._read(path)
    require(len(raw) <= 8192, "CA metadata exceeds bound")
    pin = image_input.parse(raw)
    bundle = image_input._read(path.parent / "cacert.pem")
    require(driver.sha256(bundle) == pin["bundle_sha256"] and len(bundle) == pin["bundle_bytes"],
            "selected public CA input differs from pin")
    return pin


def secure_registry_config(value):
    require(isinstance(value, dict) and set(value) == {"InsecureRegistryCIDRs", "IndexConfigs", "Mirrors"},
            "unknown Engine registry policy")
    require(value["Mirrors"] == [] and value["IndexConfigs"] == {
        "docker.io": {"Name": "docker.io", "Mirrors": [], "Secure": True, "Official": True}},
        "public Docker Hub must use verified TLS without mirrors or alternate indexes")
    cidrs = value["InsecureRegistryCIDRs"]
    require(isinstance(cidrs, list) and len(cidrs) == 2 and
            set(cidrs) == {"::1/128", "127.0.0.0/8"}, "unexpected insecure registry range")


def image_matches(item, pin):
    # This selected containerd-backed Engine identifies images by target
    # descriptor digest, not config digest. The exact manifest binds the raw
    # config provenance; inspect below observes its semantic projection only.
    require(pin["id"] == pin["manifest_descriptor"]["digest"] and item["Id"] == pin["id"] and
            {key: item.get(key) for key in ("Os", "Architecture", "Variant")} == {
                "Os": pin["platform_detail"]["os"], "Architecture": pin["platform_detail"]["architecture"],
                "Variant": pin["platform_detail"]["variant"]},
            "pulled image target/platform differs from verified registry metadata")
    descriptor = item.get("Descriptor", {})
    require({key: descriptor.get(key) for key in ("mediaType", "digest", "size")} == pin["manifest_descriptor"],
            "Engine image descriptor differs from pinned manifest")
    config, expected = item.get("Config", {}), pin["image_config"]
    require(all(config.get(key) == expected.get(key) for key in ("Env", "Cmd")) and
            (config.get("Entrypoint") or []) == (expected.get("Entrypoint") or []) and
            all((config.get(key) or "") == (expected.get(key) or "") for key in ("User", "WorkingDir")) and
            item.get("RootFS", {}).get("Type") == pin["rootfs"]["type"] and
            item.get("RootFS", {}).get("Layers") == pin["rootfs"]["diff_ids"],
            "Engine config/rootfs projection differs from pinned config provenance")
    # Docker's canonical repository spelling may omit docker.io/library. Bind
    # only these exact equivalent names, never a digest from another repository.
    suffix = pin["reference"].split("@", 1)[1]
    accepted = {"docker.io/library/python@" + suffix, "library/python@" + suffix, "python@" + suffix}
    observed = accepted.intersection(item.get("RepoDigests", []))
    require(bool(observed), "verified Python repository manifest absent from Engine")
    return sorted(observed)[0]


def embedded_builder(raw, context):
    sections = raw.decode().split("\nNodes:\n")
    require(len(sections) == 2, "exact embedded builder node section required")
    def fields(section, keys):
        result = {}
        for line in section.splitlines():
            key, separator, value = line.partition(":")
            require(key.strip() != "Error", "builder inspection reported an error")
            if separator and key in keys:
                require(key not in result, "duplicate builder field")
                result[key] = value.strip()
        require(set(result) == keys, "missing embedded builder identity field")
        return result
    require(fields(sections[0], {"Name", "Driver"}) == {"Name": context, "Driver": "docker"},
            "builder is not the exact Machine embedded Engine")
    node = fields(sections[1], {"Name", "Endpoint", "Status"})
    require(node["Endpoint"] == context and node["Status"] == "running", "foreign/offline embedded builder")


def public_activation(harness, environment, machine):
    current_generation = environment["lifecycle_generation"]
    require(type(current_generation) is int and 0 < current_generation < 2**64,
            "invalid current Environment lifecycle generation")
    matches = []
    for command in harness.record.receipts:
        if command["label"] != "public-up":
            continue
        path = harness.evidence / f'{command["index"]:03}-public-up.stdout'
        raw = startup.read_private_regular(path, startup.LIMIT)
        require(driver.sha256(raw) == command["stdout_sha256"] and command["capture_complete"] and
                not command["effects_uncertain"] and command["exit_code"] == 0, "incomplete Up capture")
        for line in raw.splitlines():
            completion = json.loads(line).get("progress", {}).get("completion")
            if completion is None:
                continue
            admission, operation = completion["admission"], completion["operation"]
            if (admission["environment_id"] != environment["environment_id"] and
                    operation["environment_id"] != environment["environment_id"]):
                continue
            argv = command["argv"]
            request = argv[argv.index("--request-id") + 1]
            idempotency = argv[argv.index("--idempotency-key") + 1]
            require(completion["error"] is None and operation["status"] == "succeeded" and operation["kind"] == "up" and
                    admission["schema_version"] == operation["schema_version"] == 1 and
                    all(admission[key] == operation[key] == expected for key, expected in (
                        ("project_id", environment["project_id"]), ("environment_id", environment["environment_id"]),
                        ("request_id", request), ("idempotency_key", idempotency))) and
                    admission["request_hash"] == operation["request_hash"] and
                    admission["definition_digest"] == operation["definition_digest"],
                    "Up operation does not authenticate exact request/owner")
            generation = operation["generation"]
            require(type(generation) is int and 0 < generation <= current_generation,
                    "invalid or future Up lifecycle generation")
            if generation < current_generation:
                # Re-Up retains the previous completed Up commands and their
                # original incarnations. Authenticate their capture and owner,
                # but do not mistake historical activation for current status.
                continue
            require(operation["definition_digest"] == environment["definition_digest"] and
                    len(admission["machine_ids"]) == len(operation["machine_steps"]) == len(environment["machines"]) and
                    set(admission["machine_ids"]) == {step["machine_id"] for step in operation["machine_steps"]} ==
                    {m["machine_id"] for m in environment["machines"]},
                    "Up operation does not authenticate exact request/owner/topology")
            for step in operation["machine_steps"]:
                if step["machine_id"] != machine["machine_id"]:
                    continue
                activation = step["resulting_activation"]
                require(step["status"] == "succeeded" and step["target_state"] == "ready" and
                        activation["schema_version"] == 1 and activation["backend"] == machine["backend"] and
                        activation["docker_context"] == machine["docker_context"] and
                        activation["negotiated_capabilities"] == machine["negotiated_capabilities"] and
                        activation["incarnation"] == step["resulting_incarnation"], "activation differs from public status")
                incarnation = activation["incarnation"]
                require(incarnation["schema_version"] == 1 and incarnation["machine_id"] == machine["machine_id"] and
                        incarnation["incarnation_id"] == machine["incarnation_id"] and
                        incarnation["generation"] == machine["incarnation_generation"], "stale activation")
                identity = activation["runtime_identity"]
                require(set(identity) == {"schema_version", "opaque_id"} and identity["schema_version"] == 1,
                        "unknown public runtime identity")
                driver.checked_text(identity["opaque_id"], r"[^\s\x00-\x1f]{1,256}", "public runtime identity")
                decoded = json.loads(identity["opaque_id"])
                require(set(decoded) == {"schema_version", "stack_id", "incarnation_id"} and decoded["schema_version"] == 1 and
                        "inc_runtime_" + decoded["incarnation_id"] == machine["incarnation_id"] and
                        decoded["stack_id"].startswith("vzr1-other-runtime_vm-vm-"), "runtime identity incarnation differs")
                matches.append(identity)
    require(len(matches) == 1, "exact current public Up runtime identity required")
    return matches[0]


def input_mapping(harness, scope, proof, images):
    owner = {key: scope[key] for key in ("project_id", "environment_id", "machine_id")}
    config = startup.machine_config_path(harness.runtime, owner)
    clients = {"docker": {"path": harness.info["clients"]["docker"]["canonical"],
                           "sha256": harness.info["clients"]["docker"]["sha256"]}}
    for name in ("compose", "buildx"):
        path = harness.config / "cli-plugins" / ("docker-" + name)
        clients[name] = {"path": str(path), "sha256": startup.digest(path)}
    return {"schema_version": 1, "run_id": harness.info["run_id"], "release_sha256": harness.info["clients"]["vz"]["sha256"],
            "fixture_sha256": harness.info["fixture_sha256"], "scope": scope, "docker_config": str(config),
            "clients": clients, "images": images, "runtime_evidence": proof}


def authenticated_proof(harness, environment, machine):
    """Bind normal-Up receipts to the public status and selected installed bytes."""
    descriptor = startup.managed_context_descriptor(environment, machine, harness.runtime)
    matches = []
    for path in harness.runtime.rglob("receipt.json"):
        if startup.is_private_client_path(path, harness.runtime):
            continue
        data = startup.read_private_regular(path, startup.LIMIT)
        row = json.loads(data)
        if row.get("owner") == descriptor["owner"] and row.get("incarnation", {}).get("incarnation_id") == machine["incarnation_id"]:
            matches.append((path, data, row))
    require(len(matches) == 1, "exact current Machine startup receipt required")
    path, data, row = matches[0]
    configuration_bytes = startup.read_private_regular(path.parent.parent / "linux-target/configuration.json", startup.LIMIT)
    configuration = json.loads(configuration_bytes)
    require(json.dumps(configuration, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() == configuration_bytes,
            "noncanonical Machine configuration receipt")
    configuration_digest = "sha256:" + driver.sha256(b"vz.machine-configuration.v1\x00" + configuration_bytes)
    store_owner = json.loads(startup.read_private_regular(path.parent.parent.parent / "owner.json", startup.LIMIT))
    require(row["configuration_digest"] == store_owner["configuration_digest"] == configuration_digest and
            store_owner["schema_version"] == 1 and store_owner["owner"] == descriptor["owner"],
            "startup configuration/store owner binding differs")
    require(configuration["schema_version"] == 1 and configuration["backend"] == machine["backend"] and
            configuration["kernel_profile"] == "developer" and configuration["machine"]["profile"] == "developer" and
            configuration["machine"]["name"] == machine["name"] and
            configuration["machine"]["requested_capabilities"] == machine["requested_capabilities"] and
            configuration["machine"]["target"] == machine["target"] and
            configuration["host"] == {"os": "macos", "arch": "aarch64"} and
            configuration["release_version"] == harness.info["release_version"], "foreign selected Machine configuration")
    artifact = configuration["artifact"]
    require(artifact["digest"] == machine["target"]["digest"], "configuration target artifact changed")
    for key, filename in (("kernel_sha256", "vmlinux"), ("initramfs_sha256", "initramfs.img"),
                          ("version_sha256", "version.json"), ("youki_sha256", "youki")):
        require(artifact[key] == startup.digest(harness.prefix / "linux/developer" / filename),
                "configuration did not select installed artifact: " + filename)
    require(row["incarnation"]["generation"] == machine["incarnation_generation"], "receipt generation differs from status")
    require(row["archive_sha256"] == startup.digest(harness.prefix / "linux/developer/developer-probe-rootfs.tar"),
            "startup proof did not execute selected installed probe")
    after_path = path.parent / "runtime-inventory-after.json"
    after = startup.read_private_regular(after_path, startup.LIMIT)
    proof = {"receipt_path": str(path), "receipt_sha256": driver.sha256(data),
             "inventory_path": str(after_path), "inventory_sha256": driver.sha256(after),
             "youki_sha256": startup.digest(harness.prefix / "linux/developer/youki")}
    entries = [x for x in harness.catalog["linux"] if x["profile"] == "developer"]
    require(len(entries) == 1 and machine["target"]["digest"] == entries[0]["digest"] and
            machine["target"]["image"] == entries[0]["image"] and machine["backend"] == "macos_virtualization_linux" and
            machine["profile"] == "developer", "status runtime differs from installed catalog")
    identity = public_activation(harness, environment, machine)
    scope = dict(descriptor["owner"], machine_incarnation=machine["incarnation_id"],
                 runtime_identity=identity["opaque_id"],
                 docker_context=descriptor["name"], docker_endpoint=descriptor["endpoint"], engine_id=descriptor["engine_id"])
    # This is input/proof admission only; neither image is claimed present or
    # executed. Actual base+fixture image observations replace these pins before
    # Driver dispatch. Validate runtime proof BEFORE even sentinel provisioning.
    base = {key: harness.info["python_image"][key] for key in ("reference", "id", "platform")}
    admission = driver.Inputs(input_mapping(harness, scope, proof, {"base": base, "compose": base}), suite="compose")
    admission.verify_runtime_evidence()
    startup.document(harness.evidence / (machine["machine_id"] + "-runtime-binding.json"),
                     {"scope": scope, "runtime_identity_material": identity, "runtime_evidence": proof,
                      "public_status": machine, "owner": descriptor["owner"], "configuration": configuration,
                      "configuration_digest": configuration_digest, "runtime_store_owner": store_owner})
    return scope, proof


class ComposeHarness(startup.Harness):
    def __init__(self, info):
        super().__init__(info)
        self.effects_uncertain = False
        self.owned = []
        self.drivers = []
        self.driver_cleanup_verified = []
        self.monitor = None
        self.mutations = []
        self.builders = []
        self.builder_by_owner_role = {}
        self.keep_proofs_verified = []
        self.sensitive_canaries = []
        self.ssh_cache_requests = []
        self.ssh_cache_proofs = []
        self.ssh_cache_captures = []
        self.runtime_audits = []
        self.registry_sessions = []
        self.recovery_sessions, self.recovery_cycles, self.recovery_monitors = [], {}, []
        self.registry_controls = None
        self.registry_project = None

    def enroll_runtime_audits(self, contexts):
        """Fresh diagnostic sessions before this candidate's Docker mutations.

        Ready/startup and future recovery invocations are outside this window.
        Registration precedes enrollment so even a partial failure fences cleanup.
        """
        from linux_docker_runtime_audit_evidence import Session, required_source_paths
        require(not self.runtime_audits and len(contexts) == 4,
                'four fresh Machine audit sessions required')
        require(len({row['owner']['machine_id'] for row in contexts}) == 4,
                'runtime audit Machine owners must be distinct')
        pins = {path: self.info['inputs'][path] for path in required_source_paths()}
        pins[str(self.cli)] = self.staged_inputs[str(self.cli)]
        for index, descriptor in enumerate(contexts):
            session = Session(self, descriptor, self.evidence / ('runtime-audit-%d' % index),
                              pins, session_id=uuid.uuid4().hex + uuid.uuid4().hex)
            self.runtime_audits.append(session)
            session.enroll()

    def capture_runtime_audits(self):
        """Finish quiescent audit replay after owned workload removal, before Stop."""
        self.assert_certain()
        require(len(self.runtime_audits) == 4, 'all four Machine audit sessions required')
        return [session.capture() for session in self.runtime_audits]

    @staticmethod
    def builder_key(descriptor, role):
        require(type(role) is str and role in {"source", "cold-control", "importer"}, "unknown builder role")
        return (json.dumps(descriptor["owner"], sort_keys=True, separators=(",", ":")), role)

    def get_builder(self, descriptor, role="source"):
        key = self.builder_key(descriptor, role)
        require(key in self.builder_by_owner_role, "builder owner/role was not prepared")
        builder = self.builder_by_owner_role[key]
        require(builder.descriptor == descriptor, "builder descriptor changed after admission")
        return builder

    def prepare_builder(self, descriptor, role="source", keep_probe=True):
        key = self.builder_key(descriptor, role)
        require(type(keep_probe) is bool, "invalid keep probe selection")
        require(key not in self.builder_by_owner_role, "builder owner/role already registered")
        from linux_docker_buildkit_builder import Builder
        builder = Builder(self, json.loads(json.dumps(descriptor)), role=role)
        # Both inventories retain exact ownership before any partial effects.
        self.builders.append(builder)
        self.builder_by_owner_role[key] = builder
        builder.prepare()
        if role == "source" and keep_probe:
            from linux_docker_buildkit_keep import run as verify_keep
            self.keep_proofs_verified.append(False)
            verify_keep(builder)
            self.keep_proofs_verified[-1] = True
        return builder

    def driver_inputs(self, descriptor, scope, proof, images):
        inputs = input_mapping(self, scope, proof, images)
        if self.info.get("suite", "compose") == "build":
            inputs["builder"] = self.prepare_builder(descriptor).mapping
        return inputs

    def validate_driver(self, output, inputs):
        require(self.info.get("suite", "compose") in {"build", "compose"},
                "artifact replay belongs to the artifact orchestrator")
        if self.info.get("suite", "compose") == "build":
            from linux_docker_build_evidence import validate
        else:
            from linux_docker_compose_evidence import validate
        return validate(output, inputs)

    def mutate(self, label, descriptor, args, **kwargs):
        # A failed mutation is never presumed rolled back merely because the
        # host process returned a normal nonzero code.
        require(not self.effects_uncertain, "previous mutation remains uncertain")
        self.effects_uncertain = True
        sequence = len(self.mutations) + 1
        intent = {"index": sequence, "label": label, "context": descriptor["name"], "args": args,
                  "owner": descriptor["owner"], "effects_uncertain": True, "started_unix_ns": time.time_ns()}
        self.mutations.append(intent)
        startup.document(self.evidence / f"mutation-{sequence:03}.intent.json", intent)
        try:
            result = self.docker(label, descriptor, args, **kwargs)
            require(result[2] == 0, "mutating command lacks positive host completion")
        except BaseException as error:
            startup.document(self.evidence / f"mutation-{sequence:03}.result.json",
                             intent | {"error": f"{type(error).__name__}: {error}", "effects_uncertain": True})
            raise
        startup.document(self.evidence / f"mutation-{sequence:03}.result.json", intent | {"effects_uncertain": False,
                         "command_index": len(self.record.receipts), "exit_code": 0, "error": None})
        self.effects_uncertain = False
        return result

    def exact_absent(self, descriptor, kind, name):
        args = [kind, "ls", "--quiet"]
        if kind == "container":
            args += ["--all", "--no-trunc", "--filter", "name=^/" + name + "$"]
        else:
            args += ["--filter", "reference=" + name]
        raw, _, _ = self.docker("owned-name-absent", descriptor, args)
        require(not raw.strip(), "owned resource name already exists")

    def sentinel(self, descriptor):
        token = "vzlive-" + uuid.uuid4().hex[:24]
        tag = token + ":sentinel"
        self.exact_absent(descriptor, "image", tag)
        self.exact_absent(descriptor, "container", token)
        row = {"descriptor": descriptor, "token": token, "tag": tag, "kind": "sentinel", "container_id": None}
        self.owned.append(row)
        with (self.prefix / "linux/developer/developer-probe-rootfs.tar").open("rb") as stream:
            raw, _, _ = self.mutate("sentinel-import", descriptor,
                                   ["image", "import", "--change", "LABEL " + LABEL + "=" + token, "-", tag], stdin=stream)
        row["image_id"] = startup.image_id(raw)
        raw, _, _ = self.mutate("sentinel-create", descriptor,
                               ["container", "create", "--network", "none", "--label", LABEL + "=" + token,
                                "--name", token, row["image_id"], "/bin/sleep", "7200"])
        row["container_id"] = driver.checked_text(raw.decode().strip(), r"[0-9a-f]{64}", "sentinel container ID")
        self.mutate("sentinel-start", descriptor, ["container", "start", row["container_id"]])
        # Written once by the host, never in the container entrypoint. A restart
        # or replacement must not recreate a passing liveness marker.
        self.mutate("sentinel-write", descriptor, ["exec", row["container_id"], "/bin/sh", "-c",
                    'printf "%s\\n" "$1" > /sentinel', "sh", token])
        raw, _, _ = self.docker("sentinel-initial-inspect", descriptor, ["container", "inspect", row["container_id"]])
        item = json.loads(raw)[0]
        require(item["State"]["Running"] and item["RestartCount"] == 0 and item["Image"] == row["image_id"] and
                item["Config"]["Labels"][LABEL] == token, "sentinel ownership/start differs")
        row["started_at"] = item["State"]["StartedAt"]
        return row

    def prepare_image(self, descriptor):
        raw, stderr, _ = self.docker("public-registry-policy", descriptor, ["info", "--format", "{{json .}}"])
        require(not stderr, "registry policy emitted stderr")
        engine = image_input.parse(raw)
        require(engine["ID"] == descriptor["engine_id"], "registry policy came from a different Engine")
        secure_registry_config(engine["RegistryConfig"])
        pin = self.info["python_image"]
        token = "vzcompose-" + uuid.uuid4().hex[:24]
        tag = token + ":fixture"
        self.exact_absent(descriptor, "image", tag)
        # Fresh Machine only: don't adopt or later remove preexisting base tags.
        raw, _, _ = self.docker("python-base-absent", descriptor, ["image", "ls", "--all", "--quiet", "--no-trunc"])
        require(pin["id"] not in raw.decode().split(), "base Engine image preexists; no ownership inferred")
        row = {"descriptor": descriptor, "token": token, "tag": tag, "kind": "fixture", "base_reference": None}
        self.owned.append(row)
        self.mutate("python-pull", descriptor, ["pull", "--platform", "linux/arm64", pin["reference"]], timeout=300)
        raw, _, _ = self.docker("python-inspect", descriptor, ["image", "inspect", pin["reference"]])
        items = json.loads(raw)
        require(len(items) == 1, "ambiguous Python image")
        reference = image_matches(items[0], pin)
        row["base_reference"] = reference
        row["base_id"] = pin["id"]
        name = token + "-input"
        self.exact_absent(descriptor, "container", name)
        row["probe_name"] = name
        raw, stderr, _ = self.mutate("python-execution", descriptor,
                    ["run", "--pull", "never", "--network", "none", "--name", name, "--label", LABEL + "=" + token,
                     pin["id"], "/bin/sh", "-c", 'set -eu; python3 -c "import platform; print(platform.python_version()); print(platform.machine())"; printf "shell-ok\\n"'], timeout=60)
        require(raw == (pin["python_version"] + "\naarch64\nshell-ok\n").encode() and not stderr,
                "actual Python/shell/platform execution differs")
        inspected, _, _ = self.docker("python-probe-inspect", descriptor, ["container", "inspect", name])
        probe = json.loads(inspected)[0]
        require(probe["Name"] == "/" + name and probe["Image"] == pin["id"] and probe["Config"]["Labels"][LABEL] == token and
                not probe["State"]["Running"] and probe["State"]["ExitCode"] == 0, "input probe identity/completion differs")
        self.mutate("python-probe-remove", descriptor, ["container", "rm", probe["Id"]])
        row["probe_name"] = None
        raw, _, _ = self.docker("embedded-builder", descriptor, ["buildx", "inspect", descriptor["name"]])
        embedded_builder(raw, descriptor["name"])
        iid = self.root / (token + ".iid")
        fixture = Path(self.info["fixture"])
        require(driver.tree_digest(fixture) == self.info["fixture_sha256"], "fixture source changed")
        self.mutate("compose-image-build", descriptor,
                    ["buildx", "build", "--builder", descriptor["name"], "--platform", "linux/arm64", "--network", "none",
                     "--progress", "plain", "--load", "--no-cache", "--pull=false", "--iidfile", str(iid),
                     "--build-arg", "FIXTURE_BASE=" + reference, "--label", LABEL + "=" + token,
                     "--tag", tag, str(fixture / "compose")], timeout=300)
        row["image_id"] = startup.image_id(startup.read_private_regular(iid, 128))
        raw, _, _ = self.docker("compose-image-inspect", descriptor, ["image", "inspect", tag])
        item = json.loads(raw)[0]
        require(item["Id"] == row["image_id"] and item["Architecture"] == "arm64" and item["Os"] == "linux" and
                item["Config"]["Labels"][LABEL] == token, "fixture image ownership/content differs")
        return {"base": {"reference": reference, "id": pin["id"], "platform": "linux/arm64"},
                "compose": {"reference": row["image_id"], "id": row["image_id"], "platform": "linux/arm64"}}

    def assert_certain(self):
        # Final audit capture cannot be a prerequisite for Docker cleanup: those
        # cleanup invocations themselves belong in the journal. Enrollment and
        # every already-dispatched acquisition must nevertheless be certain.
        for session in getattr(self, 'runtime_audits', []):
            session.assert_enrolled_certain()
        # A registry Session that never reached its own exact cleanup, or whose
        # private receipts are uncertain, retains its network/volume/server/
        # credential state: no blind removal, force or public Stop follows.
        for session in getattr(self, 'registry_sessions', []):
            require(getattr(session, 'cleanup_complete', None) is True and getattr(session, 'failed', True) is False,
                    'registry Session lacks completed cleanup; registry state retained; cleanup withheld')
            session.commands.assert_certain()
            session.certain()
        for session in getattr(self, 'limits_sessions', []):
            require(getattr(session, 'cleanup_complete', None) is True and getattr(session, 'failed', True) is False,
                    'limits Session lacks completed cleanup; containers retained; cleanup withheld')
        for session in getattr(self, 'recovery_sessions', []):
            require(getattr(session, 'cleanup_complete', None) is True and getattr(session, 'failed', True) is False,
                    'recovery Session lacks completed cleanup; volumes/containers retained; cleanup withheld')
        for retired in getattr(self, 'recovery_monitors', []):
            require(not retired.thread.is_alive() and not retired.errors, 'retired recovery monitor live or failed; cleanup withheld')
            require(not any(x["effects_uncertain"] for x in retired.record.receipts), 'uncertain retired-monitor command; cleanup withheld')
        require(all(getattr(self, "keep_proofs_verified", [])),
                "unresolved direct-youki keep fixture; resources retained; cleanup withheld")
        recorders = [self.record, *(d.record for d in self.drivers)]
        if self.monitor is not None:
            require(not self.monitor.thread.is_alive(), "live monitor prevents cleanup")
            recorders.append(self.monitor.record)
        for selected in self.drivers:
            follower = getattr(selected, "follow_thread", None)
            require(follower is None or not follower.is_alive(), "live log follower prevents cleanup")
            owner = getattr(selected, 'terminal_owner', None)
            if owner is not None:
                require(type(getattr(owner, 'pending', None)) is list and not owner.pending,
                        'pending tmux control process prevents cleanup')
                require(hasattr(owner, 'server'), 'unknown tmux server ownership prevents cleanup')
                server = owner.server
                require(server is None or (type(server.returncode) is int and server.returncode == 0),
                        'tmux server lacks normal exit; prevents cleanup')
        require(all(not getattr(record, "pending_interactions", []) for record in recorders),
                "pending interactive process prevents cleanup")
        require(not self.effects_uncertain and all(not any(x["effects_uncertain"] for x in r.receipts) for r in recorders),
                "uncertain mutation: resources retained; cleanup withheld")
        require(len(self.driver_cleanup_verified) == len(self.drivers) and all(self.driver_cleanup_verified),
                "Docker fixture cleanup lacks successful independent replay; resources retained")

    def remove_owned(self):
        self.assert_certain()
        for builder in reversed(getattr(self, "builders", [])):
            self.assert_certain()
            jobs = [job for job in self.ssh_cache_requests if job["builder"] is builder]
            require(len(jobs) <= 1, "ambiguous SSH worker-cache ownership")
            if jobs:
                from linux_docker_ssh_cache_capture import Capture
                job = jobs[0]
                def capture(stopped, stop_proof):
                    item = Capture(builder, job["canaries"], self.root / ("ssh-cache-private-" + str(job["index"])),
                                   self.evidence / ("ssh-cache-" + str(job["index"])))
                    self.ssh_cache_captures.append(item)
                    result = item.run(stopped, stop_proof)
                    require(result["owner"] == item.owner and result["normal_stop"] == stop_proof and
                            result["scan"]["complete"] is True and result["guard_receipts_complete"] is True and
                            result["builder_restarted"] is False and
                            all(result["capture"][key] is True for key in
                                ("owned_process_reaped", "capture_complete", "archive_published")) and
                            result["capture"]["effects_uncertain"] is False,
                            "SSH worker-cache proof incomplete or foreign")
                    self.ssh_cache_proofs.append(result)
                    return result
                builder.remove_owned(before_remove=capture)
            else:
                builder.remove_owned()
        require(len(self.ssh_cache_proofs) == len(self.ssh_cache_requests), "SSH worker-cache scan not complete")
        for row in reversed(self.owned):
            self.assert_certain()
            descriptor, token = row["descriptor"], row["token"]
            require(not row.get("probe_name"), "unreconciled input probe retained")
            if row.get("container_id"):
                raw, _, _ = self.docker("owned-container-check", descriptor, ["container", "inspect", row["container_id"]])
                item = json.loads(raw)[0]
                require(item["Id"] == row["container_id"] and item["Image"] == row["image_id"] and
                        item["Config"]["Labels"][LABEL] == token, "foreign sentinel before cleanup")
                self.mutate("owned-container-remove", descriptor, ["container", "rm", "--force", row["container_id"]])
                self.exact_absent(descriptor, "container", token)
            require(row.get("image_id"), "unreconciled image mutation retained")
            raw, _, _ = self.docker("owned-image-check", descriptor, ["image", "inspect", row["tag"]])
            item = json.loads(raw)[0]
            require(item["Id"] == row["image_id"] and item["Config"]["Labels"][LABEL] == token, "foreign image before cleanup")
            self.mutate("owned-image-remove", descriptor, ["image", "rm", row["tag"]])
            self.exact_absent(descriptor, "image", row["tag"])
            if row.get("base_reference"):
                raw, _, _ = self.docker("owned-base-check", descriptor, ["image", "inspect", row["base_reference"]])
                require(image_matches(json.loads(raw)[0], self.info["python_image"]) == row["base_reference"], "base reference drift")
                self.mutate("owned-base-remove", descriptor, ["image", "rm", row["base_reference"]])

    def scenario(self):
        suite = self.info.get("suite", "compose")
        project = self.project(suite, "developer", 2)
        primary = self.up(project, "primary")
        self.daemon_identity = self.daemon_fingerprint()
        neighbor = self.up(project, "neighbor")
        startup.exact_developer_topology(primary, neighbor)
        primary_contexts, neighbor_contexts = self.inspect(primary), self.inspect(neighbor)
        contexts = primary_contexts + neighbor_contexts
        for field in ("name", "endpoint", "engine_id"):
            require(len({x[field] for x in contexts}) == 4, "Machines share " + field)
        startup.document(self.evidence / "topology.json", {"primary": primary, "neighbor": neighbor, "project": str(project)})
        bindings = {machine["machine_id"]: authenticated_proof(self, environment, machine)
                    for environment in (primary, neighbor) for machine in environment["machines"]}
        # Public Exec observes the actual Machine root, not merely the builder
        # or initramfs source. This hash is not a complete effective-trust audit.
        for environment in (primary, neighbor):
            for machine in environment["machines"]:
                raw, stderr, _ = self.command("public-machine-ca-hash", [self.cli, "exec", "--environment",
                    environment["environment_id"], "--machine", machine["name"], "--no-stdin", "--timeout", "30",
                    "--", "/bin/busybox", "sha256sum", "/etc/vz/ca-certificates.crt"], cwd=project)
                require(raw == (self.info["public_ca"]["bundle_sha256"] +
                               "  /etc/vz/ca-certificates.crt\n").encode() and not stderr,
                        "actual Machine public CA bytes differ from selected immutable input")
        if suite == 'lifecycle':
            self.enroll_runtime_audits(contexts)
        selected_machines = [(primary, m) for m in primary["machines"]] + [(neighbor, neighbor["machines"][0])]
        controls = None
        if suite == 'registry':
            # Three selected Machines run the registry; the fourth is only a
            # same-authority neighbor sentinel. Controls observe all four Docker
            # config directories before, between and after every Session.
            import linux_docker_registry_controls as registry_controls
            require(len(self.registry_sessions) == 0 and self.registry_controls is None, 'registry controls already exist')
            selected_descriptors = [machine["docker_context"] for _, machine in selected_machines]
            sentinel_descriptor = neighbor["machines"][1]["docker_context"]
            require(sentinel_descriptor not in selected_descriptors and
                    len({json.dumps(d, sort_keys=True) for d in selected_descriptors}) == 3, 'registry Machine selection')
            controls = registry_controls.Controls(self, contexts, selected_descriptors, sentinel_descriptor)
            self.registry_controls = controls
            self.registry_project = str(project)
        sentinels = [self.sentinel(descriptor) for descriptor in contexts]
        self.monitor = SentinelMonitor(self, sentinels)
        observations = []
        credential_controls = None
        try:
            self.monitor.start()
            if controls is not None:
                credential_controls = {'baseline': controls.baseline()}
            for index, (environment, machine) in enumerate(selected_machines):
                self.monitor.check()
                descriptor = machine["docker_context"]
                scope, proof = bindings[machine["machine_id"]]
                if suite in ('images', 'registry', 'handshake', 'recovery'):
                    # Driver schema compatibility only: this recipe uses tiny
                    # source-selected archives, not the Python/Compose image.
                    # Never pull/build/execute those admission-only image pins.
                    base = {key: self.info['python_image'][key] for key in ('reference', 'id', 'platform')}
                    images = {'base': dict(base), 'compose': dict(base)}
                else:
                    images = self.prepare_image(descriptor)
                if suite in {"artifacts", "parallel", "ssh", "lifecycle", "images", "registry", "handshake", "limits", "recovery"}:
                    if suite == "artifacts":
                        from linux_docker_build_artifacts import run_machine
                    elif suite == "parallel":
                        from linux_docker_build_parallel import run_machine
                    elif suite == "ssh":
                        from linux_docker_build_ssh import run_machine
                    elif suite == 'images':
                        from linux_docker_image_machine import run_machine
                    elif suite == 'registry':
                        from linux_docker_registry_machine import run_machine
                    elif suite == 'handshake':
                        from linux_docker_handshake_machine import run_machine
                    elif suite == 'limits':
                        from linux_docker_limits_machine import run_machine
                    elif suite == 'recovery':
                        from linux_docker_recovery_machine import run_machine
                    else:
                        from linux_docker_container_lifecycle import run_machine
                    begin = time.time_ns()
                    observation = run_machine(self, descriptor, scope, proof, images, index)
                    end = self.monitor.close_interval(begin, descriptor["name"])
                    self.monitor.check_interval(begin, end, descriptor["name"])
                    observations.append(observation)
                    continue
                inputs = self.driver_inputs(descriptor, scope, proof, images)
                admitted = driver.Inputs(inputs, suite=suite)
                admitted.verify_runtime_evidence()
                output = self.evidence / (suite + "-machine-" + str(index))
                selected = driver.Driver(admitted, Path(self.info["fixture"]), output)
                self.drivers.append(selected)
                self.driver_cleanup_verified.append(False)
                begin = time.time_ns()
                result = selected.run(suite)
                end = self.monitor.close_interval(begin, descriptor["name"])
                require(result["outcome"] == "fixture_assertions_passed", suite + " slice failed: " +
                        str({"failure": result.get("failure"), "cleanup_errors": result.get("cleanup_errors")}))
                require(result["cleanup_errors"] == [], "Docker fixture cleanup failed semantically")
                builder_runtime = None
                if suite == "build":
                    builder = self.get_builder(descriptor)
                    builder_runtime = builder.verify(require_invocation=True)
                    from linux_docker_buildkit_keep import verify_worker_log
                    builder_runtime["post_workload_log"] = verify_worker_log(builder)
                replay = self.validate_driver(output, inputs)
                self.driver_cleanup_verified[-1] = True
                self.monitor.check_interval(begin, end, descriptor["name"])
                observation = {"scope": scope, "started_unix_ns": begin, "ended_unix_ns": end,
                               "independent_validation": replay}
                if builder_runtime is not None:
                    observation["builder_runtime"] = builder_runtime
                observations.append(observation)
            if controls is not None:
                require(len(self.registry_sessions) == 3 and all(s.cleanup_complete is True for s in self.registry_sessions),
                        'three completed registry Sessions required')
                credential_controls['final'] = controls.final()
            if suite == 'recovery':
                # Stop/Up cycles legitimately advance incarnations; stable identity must hold.
                for environment, expected in ((primary, primary_contexts), (neighbor, neighbor_contexts)):
                    live = self.inspect(self.status(project, environment["environment_id"]))
                    require(len(live) == len(expected) and all(
                        all(a[k] == b[k] for k in ("owner", "name", "endpoint", "config_dir", "engine_id")) and
                        a["incarnation_generation"] > b["incarnation_generation"] for a, b in zip(live, expected)),
                        "recovery changed stable Machine identity or failed to advance incarnation")
            else:
                require(self.inspect(self.status(project, primary["environment_id"])) == primary_contexts and
                        self.inspect(self.status(project, neighbor["environment_id"])) == neighbor_contexts,
                        "topology identity changed during Docker fixture work")
        finally:
            self.monitor.stop()
        result = {"machine_slices": observations, "continuous_sentinels": self.monitor.summary(),
                  "runtime_inventory_scope": "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit"}
        if controls is not None:
            result["credential_controls"] = credential_controls
        return result


class SentinelMonitor:
    """Independent, bounded raw observations; no retries, restarts or repairs."""
    def __init__(self, harness, rows):
        self.harness, self.rows = harness, rows
        self.output = startup.private(harness.evidence / "sibling-liveness")
        self.record = startup.Recorder(self.output, harness.env)
        self.finished, self.first = threading.Event(), threading.Event()
        self.samples, self.errors = [], []
        self.thread = threading.Thread(target=self.loop, name="vz-compose-sibling-liveness", daemon=False)

    def command(self, descriptor, args):
        if self.finished.is_set():
            raise MonitorStopped()
        return self.record.run("sentinel", ["docker", "--config", descriptor["config_dir"], "--context", descriptor["name"], *args],
                               executable=self.harness.info["clients"]["docker"]["canonical"], cwd=self.harness.root, timeout=8)

    def sample(self, row):
        descriptor = row["descriptor"]
        raw, _, _ = self.command(descriptor, ["context", "inspect", descriptor["name"]])
        require(json.loads(raw)[0]["Endpoints"]["docker"]["Host"] == descriptor["endpoint"], "sentinel context rerouted")
        raw, _, _ = self.command(descriptor, ["info", "--format", "{{.ID}}"])
        require(raw.decode().strip() == descriptor["engine_id"], "sentinel Engine changed")
        raw, _, _ = self.command(descriptor, ["container", "inspect", row["container_id"]])
        item = json.loads(raw)[0]
        require(item["Id"] == row["container_id"] and item["Image"] == row["image_id"] and item["State"]["Running"] and
                item["State"]["StartedAt"] == row["started_at"] and item["RestartCount"] == 0 and
                item["Config"]["Labels"][LABEL] == row["token"], "sentinel stopped/restarted/replaced")
        raw, stderr, _ = self.command(descriptor, ["exec", row["container_id"], "/bin/cat", "/sentinel"])
        require(raw == (row["token"] + "\n").encode() and not stderr, "host-written sentinel changed")
        self.samples.append({"context": descriptor["name"], "unix_ns": time.time_ns(), "container_id": row["container_id"]})

    def loop(self):
        try:
            while not self.finished.is_set():
                for row in self.rows:
                    self.sample(row)
                self.first.set()
                self.finished.wait(1)
        except MonitorStopped:
            pass
        except BaseException as error:
            self.errors.append(f"{type(error).__name__}: {error}")
            self.first.set()

    def start(self):
        self.thread.start()
        require(self.first.wait(45), "initial sibling observation deadline exceeded")
        self.check()

    def check(self):
        require(not self.errors and self.thread.is_alive(), "sibling liveness failed: " + repr(self.errors))

    def close_interval(self, begin, active, deadline_seconds=5.0):
        """Return an interval end only once every sibling has a sample at or after begin.

        Fast Machine workloads can finish inside the one-second sampling cadence;
        this bounded wait is a declared readiness condition for contemporaneous
        liveness evidence, not a retry, and it never relaxes check_interval.
        """
        deadline = time.monotonic() + deadline_seconds
        while True:
            self.check()
            names = {row["descriptor"]["name"] for row in self.rows} - {active}
            if all(any(x["context"] == name and x["unix_ns"] >= begin for x in self.samples) for name in names):
                return time.time_ns()
            require(time.monotonic() < deadline, "sibling liveness sample did not arrive within the interval deadline")
            self.finished.wait(0.05)

    def check_interval(self, begin, end, active):
        self.check()
        for row in self.rows:
            name = row["descriptor"]["name"]
            if name != active:
                require(any(x["context"] == name and begin <= x["unix_ns"] <= end for x in self.samples),
                        "no contemporaneous sibling/neighbor liveness observation")

    def stop(self):
        self.finished.set()
        self.thread.join(timeout=40)
        require(not self.thread.is_alive(), "monitor did not positively terminate; no cleanup allowed")
        startup.document(self.output / "samples.json", self.summary())
        require(not self.errors, "sibling liveness failed: " + repr(self.errors))

    def summary(self):
        return {"samples": list(self.samples), "errors": list(self.errors), "test_case_retries": 0,
                "scope": "Engine_identity_and_nonrestarted_container_host_written_marker_not_network_service_conformance"}


class MonitorStopped(Exception):
    """Cooperative cancellation between bounded read-only commands."""


def run(info):
    os.umask(0o077)
    harness = ComposeHarness(info)
    startup.document(harness.evidence / "inputs.json", info)
    result = {"schema_version": 1, "scope": info["scope"], "suite": info["suite"], "outcome": "failed", "error": None,
              "cleanup_errors": [], "docker_parity_certified": False, "aggregate_release_certified": False,
              "release_scenarios_passed": [], "test_case_retries": 0, "retained_root": str(harness.root)}
    try:
        if info["suite"] == "ssh":
            import linux_docker_ssh_input as ssh_input
            result["ssh_input_verification"] = ssh_input.verify(Path(info["ssh_packages"]),
                harness.evidence / "ssh-input-verification", info["ssh_gpgv"], image_path=Path(info["image_input"]))
        harness.stage()
        result["scenario"] = harness.scenario()
        for path, expected in (info["inputs"] | harness.staged_inputs).items():
            require(startup.digest(Path(path)) == expected, "selected input changed during physical run")
        require(driver.tree_digest(Path(info["fixture"])) == info["fixture_sha256"], "fixture changed during run")
        if info["suite"] in ("parallel", "limits"):
            require(driver.tree_digest(Path(info["parallel_fixture"])) == info["parallel_fixture_sha256"],
                    "parallel fixture changed during run")
        if info["suite"] == "ssh":
            require(driver.tree_digest(Path(info["ssh_fixture"])) == info["ssh_fixture_sha256"], "SSH fixture changed during run")
        if info["suite"] == "lifecycle":
            from linux_docker_container_fixture import fixture_contract
            selected = Path(info["container_fixture"])
            fixture_contract(selected)
            require(driver.tree_digest(selected) == info["container_fixture_sha256"], "container fixture changed during run")
        if info["suite"] == "registry":
            sessions = harness.registry_sessions
            require(len(sessions) == 3 and all(s.cleanup_complete is True and s.failed is False for s in sessions),
                    "three completed registry Sessions required")
            require(startup.digest(Path(info["registry_archive"])) == info["registry"]["archive_sha256"],
                    "registry archive changed during run")
        if info["suite"] == "recovery":
            sessions = harness.recovery_sessions
            require(len(sessions) == 3 and all(s.cleanup_complete is True and s.failed is False for s in sessions),
                    "three completed recovery Sessions required")
    except BaseException as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        try:
            require(harness.monitor is None or not harness.monitor.thread.is_alive(), "live monitor prevents cleanup")
            harness.remove_owned()
            if info['suite'] == 'lifecycle':
                result['runtime_audit_validation'] = harness.capture_runtime_audits()
            if info["suite"] == "ssh":
                require(len(harness.ssh_cache_proofs) == 3, "three stopped SSH worker-cache proofs required")
                result["ssh_stopped_cache_validation"] = harness.ssh_cache_proofs
            result["cleanup"] = harness.cleanup() | {"owned_workload_objects_removed": True,
                "retained_stopped_machine_disks_and_contexts": True, "delete_certified": False}
        except BaseException as error:
            result["cleanup_errors"].append(f"{type(error).__name__}: {error}")
        try:
            startup.collect_runtime_receipts(harness)
        except BaseException as error:
            result["cleanup_errors"].append(f"runtime evidence: {type(error).__name__}: {error}")
        if result["error"] is None and not result["cleanup_errors"]:
            result["outcome"] = "passed_dev_installed_" + info["suite"] + "_slice"
        startup.document(harness.evidence / "result.json", result)
        startup.checksum_evidence(harness)
    print(json.dumps(result), flush=True)
    return 0 if result["outcome"].startswith("passed_") else 1


def main(argv):
    """Standalone DEV runs behave exactly as before; a gate invocation (identified
    by --run-id, --phase and --candidate-tuple together) additionally writes one
    schema-valid lane result beside the harness evidence, including for the
    `--suite all` rejection, so the aggregate accounting is never silent."""
    ctx = None
    try:
        ctx = lane_result.gate_context(argv)
        if ctx is not None:
            ctx.validate()
    except (Exception, KeyboardInterrupt) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    try:
        args = arguments(argv)
        if ctx is not None:
            # The gate owns the lane directory; the harness gets a fresh child.
            args.evidence_dir = str(ctx.harness_dir())
        info = preflight(args)
        code = run(info)
    except (Exception, KeyboardInterrupt) as error:
        print(f"error: {error}", file=sys.stderr)
        if ctx is not None:
            reason = "not_implemented" if ctx.suite == "all" else "input_rejected"
            lane_result.write(ctx, lane_result.failed(ctx, reason, f"{type(error).__name__}: {error}", 2))
        return 2
    if ctx is not None:
        harness = ctx.harness_dir()
        result = lane_result.load_result(harness)
        lane_result.write(ctx, lane_result.from_run(ctx, result, info, harness, code))
    return code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
