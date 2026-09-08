#!/usr/bin/env python3
"""DEV installed Linux-on-macOS Docker slices; not certification.

Normal installed Up provisions four private Machines. All workload commands use
their authenticated contexts. Daily installation/configuration is untouched.
`--suite all` composes every suite against one provisioning, in an order that
leaves the topology undisturbed until recovery runs last; it is not an alias for
any subset, and the scenario table names the IDs no suite yet proves so they are
reported missing rather than silently absent. Retained stopped disks are NOT Delete.
Owned BuildKit builder cache volumes are removed by successful workload cleanup.
"""
from __future__ import annotations

import argparse
import contextlib
import copy
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
import linux_docker_engine_probe as engine_probe
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
MOUNTS_SCOPE = "DEV_INSTALLED_LINUX_STORAGE_MOUNTS_NOT_RELEASE_CERTIFICATION"
NETPOLICY_SCOPE = "DEV_INSTALLED_LINUX_PUBLISHED_PORTS_NETWORK_CLEANUP_NOT_RELEASE_CERTIFICATION"
ISOLATION_SCOPE = "DEV_INSTALLED_LINUX_SAME_ENVIRONMENT_MACHINE_ISOLATION_NOT_RELEASE_CERTIFICATION"
CONCURRENCY_SCOPE = "DEV_INSTALLED_LINUX_CONCURRENT_CLIENTS_NOT_RELEASE_CERTIFICATION"
RECOVERY_SCOPE = "DEV_INSTALLED_LINUX_PERSISTENCE_STOP_UP_RECOVERY_NOT_RELEASE_CERTIFICATION"
ALL_SCOPE = "DEV_INSTALLED_LINUX_DOCKER_COMPOSED_SUITES_NOT_RELEASE_CERTIFICATION"
# One provisioning, every suite once, in an order that leaves the topology
# undisturbed until the end: `recovery` cycles Stop/Up and replaces the
# sentinel monitor, so it always runs last.
#
# `lifecycle` is second to last because its evidence is a youki runtime-audit
# journal bounded at 2048 records per Machine. A whole-run window overflows
# that bound on sentinel sampling alone, so in a composed run the window opens
# immediately before the lifecycle suite and closes immediately after it: the
# suite removes its own containers and image inside `run_machine`, so its own
# mutations are still journaled end to end. Everything before it is proved by
# its own suite evidence, not by this journal. `--suite lifecycle` keeps the
# whole-run window it always had.
SUITE_ORDER = ("handshake", "compose", "build", "artifacts", "parallel", "ssh",
               "images", "mounts", "netpolicy", "isolation", "limits", "concurrency", "registry",
               "lifecycle", "recovery")
# The gate's selection: both primary Machines and the neighbour's first, with
# the neighbour's second left as an untouched sentinel.
GATE_MACHINES = (0, 1, 2)
SUITES = ("compose", "build", "artifacts", "parallel", "ssh", "lifecycle", "images", "registry", "handshake", "limits",
          "mounts", "netpolicy", "isolation", "concurrency", "recovery")
# Suites whose per-Machine slices run concurrently. Every Machine owns a private
# Engine on a private socket, and mutual isolation is what this lane asserts, so
# these slices share nothing but the harness's own bookkeeping — which
# `slice_lock`, `mutation_lock` and the per-slice `Recorder` below make safe.
#
# A suite is NOT here when running two more Machines at once would change what it
# measures, or when its own claim is ordered:
#   limits       asserts resource limits and OOM behaviour on a loaded Machine;
#                two more busy Machines is a different experiment.
#   concurrency  measures a sixty-second readiness window and mutually
#                overlapping registry service windows on one Machine; two more
#                busy Machines changes exactly what it measures.
#   parallel     brackets a one-second in-guest health cadence with a 250 ms
#                lateness bound; host oversubscription perturbs exactly that,
#                and its slots now fail a shared barrier fast on a sibling death.
#   lifecycle    runs inside a bounded youki runtime-audit window that must stay
#                quiet, and drives a tmux server.
#   recovery     cycles Stop/Up, which replaces the Machines the slices run on.
#   registry     `Controls` observes all four Docker config directories before,
#                between and after each Session, in Session order.
#   images       asserts that a failing Machine is never followed by a dispatch
#                to the next one, which concurrent dispatch cannot preserve.
#   handshake, mounts, netpolicy, isolation
#                seconds each, or hold live resources until the cross-Machine
#                claim is decided; parallelising them buys nothing measurable.
PARALLEL_SUITES = frozenset({"compose", "build", "artifacts", "ssh"})
REPO = Path(__file__).resolve().parents[2]
LABEL = "dev.vz.linux-compose-proof"
require = driver.require


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    names = (*startup.OPTIONS, "suite", "fixture", "image-input", "run-id", "buildkit-archive", "parallel-fixture",
             "ssh-fixture", "ssh-packages", "ssh-gpgv", "container-fixture", "tmux", "registry-archive", "registry-layout",
             *lane_result.GATE_OPTIONS, "machines")
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
    # The gate proves each behaviour on three Machines across two Environments.
    # Fewer is a development loop only: it cannot prove the isolation family and
    # its result says so, so it can never be mistaken for gate evidence.
    parser.add_argument("--machines", type=int, default=len(GATE_MACHINES), choices=(1, 2, 3))
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
    require(args.suite in (*SUITES, "all"), "unknown suite")
    # `all` composes every suite in one provisioning, so it carries every
    # suite's inputs at once; each option otherwise belongs to exactly one suite.
    composed = args.suite == "all"
    # The concurrency suite proves four parallel pulls, which needs a real
    # registry; it serves the same offline-admitted Distribution image the
    # registry suite admits, so it carries the same two inputs.
    for name in ("registry_archive", "registry_layout"):
        require((getattr(args, name) is not None) == (args.suite in ("registry", "concurrency") or composed),
                "--" + name.replace("_", "-") + " is required for the registry suite, for the concurrency suite and for --suite all")
    require(args.container_fixture is None or args.suite == "lifecycle" or composed,
            "container-fixture requires the lifecycle suite")
    require((args.tmux is not None) == (args.suite == "lifecycle" or composed),
            "--tmux is required for the lifecycle suite and for --suite all")
    require(args.parallel_fixture is None or args.suite == "parallel" or composed,
            "parallel-fixture requires the parallel suite")
    require(args.suite == "ssh" or composed or all(getattr(args, name) is None
            for name in ("ssh_fixture", "ssh_packages", "ssh_gpgv")), "SSH options require the ssh suite")
    require(args.suite != "ssh" and not composed or args.ssh_packages is not None,
            "--ssh-packages is required for the ssh suite and for --suite all")
    if args.run_id is None:
        args.run_id = args.suite + "-" + uuid.uuid4().hex[:24]
    for name in startup.OPTIONS:
        require(getattr(args, name.replace("-", "_")) is not None, "required option: --" + name)
    driver.checked_text(args.run_id, r"[a-z0-9][a-z0-9-]{7,39}", "run ID")
    require((args.buildkit_archive is not None) == (args.suite in {"build", "artifacts", "parallel", "ssh",
                                                                   "concurrency", "all"}),
            "--buildkit-archive is required only for Buildx suites")
    return args


def preflight(args, require_host=True):
    require(args.suite in (*SUITES, "all"), "full contract unavailable")
    composed = args.suite == "all"
    if not composed and args.suite in ('images', 'registry', 'handshake', 'limits', 'mounts', 'netpolicy',
                                       'isolation', 'recovery'):
        require(all(getattr(args, name, None) is None for name in ('buildkit_archive', 'parallel_fixture',
                'ssh_fixture', 'ssh_packages', 'ssh_gpgv', 'container_fixture', 'tmux')),
                ('image' if args.suite == 'images' else args.suite) + ' suite rejects builder, foreign fixture and terminal options')
    for name in ('registry_archive', 'registry_layout'):
        require((getattr(args, name, None) is not None) == (args.suite in ('registry', 'concurrency') or composed),
                '--' + name.replace('_', '-') + ' is required for the registry suite, for the concurrency suite and for --suite all')
    if composed or args.suite in ('registry', 'concurrency'):
        # Admit every registry input read-only before startup preflight touches
        # anything: pinned layout, exact archive bytes, unexecuted binary
        # metadata and the isolated Python dependencies behind the fixture.
        import linux_docker_registry_machine as registry_machine
        registry_archive = startup.canonical(args.registry_archive)
        registry_layout = startup.canonical(args.registry_layout)
        registry = registry_machine.admit_inputs(registry_archive, registry_layout)
    require((getattr(args, "tmux", None) is not None) == (args.suite == "lifecycle" or composed),
            "--tmux is required for the lifecycle suite and for --suite all")
    terminal = tmux_input(args.tmux) if args.suite == "lifecycle" or composed else None
    info = startup.preflight(args, require_host=require_host)
    fixture = startup.canonical(args.fixture)
    pin_path = startup.canonical(args.image_input)
    pin = image_input.load(pin_path)
    ca_path = REPO / "linux/ca-trust/inputs.json"
    ca_pin = public_ca_input(ca_path)
    scopes = {"compose": SCOPE, "build": BUILD_SCOPE, "artifacts": ARTIFACT_SCOPE, "parallel": PARALLEL_SCOPE,
              "ssh": SSH_SCOPE, "lifecycle": LIFECYCLE_SCOPE, "images": IMAGES_SCOPE, "registry": REGISTRY_SCOPE,
              "handshake": HANDSHAKE_SCOPE, "limits": LIMITS_SCOPE, "mounts": MOUNTS_SCOPE,
              "netpolicy": NETPOLICY_SCOPE, "isolation": ISOLATION_SCOPE,
              "concurrency": CONCURRENCY_SCOPE,
              "recovery": RECOVERY_SCOPE, "all": ALL_SCOPE}
    machines = getattr(args, "machines", len(GATE_MACHINES))
    require(machines in (1, 2, 3), "unsupported Machine selection")
    scope = scopes[args.suite]
    if machines != len(GATE_MACHINES):
        # Say it in the scope itself: a reduced selection cannot prove the
        # isolation family, so its evidence can never read as gate evidence.
        scope = "DEV_LOOP_" + str(machines) + "_MACHINE_" + scope
    info.update(scope=scope, suite=args.suite, machines=machines,
                run_id=args.run_id, fixture=str(fixture),
                fixture_sha256=driver.tree_digest(fixture), python_image=pin, image_input=str(pin_path),
                public_ca=ca_pin)
    for path in (Path(__file__).resolve(), REPO / "scripts/run-linux-docker-e2e.sh", pin_path,
                 ca_path, REPO / "linux/ca-trust/cacert.pem", REPO / "linux/ca-trust/install.sh",
                 REPO / "linux/ca-trust.py", REPO / "linux/initramfs/init",
                 REPO / "scripts/helpers/linux_docker_image_input.py",
                 REPO / "scripts/helpers/linux_docker_compose_evidence.py"):
        info["inputs"][str(path)] = startup.digest(path)
    if composed or args.suite in {"build", "artifacts", "parallel", "ssh", "concurrency"}:
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
    if composed or args.suite in {"artifacts", "parallel", "ssh", "concurrency"}:
        for name in ("linux_docker_artifact_stream.py", "linux_docker_artifact_layout.py",
                     "linux_docker_build_artifacts.py", "linux_docker_artifact_evidence.py"):
            path = REPO / "scripts/helpers" / name
            info["inputs"][str(path)] = startup.digest(path)
    if composed or args.suite in {"parallel", "concurrency"}:
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
    if composed or args.suite == "ssh":
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
    if composed or args.suite == 'images':
        from linux_docker_image_machine import required_source_paths
        for path in required_source_paths():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if composed or args.suite in ('registry', 'concurrency'):
        info.update(registry=registry, registry_archive=str(registry_archive), registry_layout=str(registry_layout))
        info['inputs'][str(registry_archive)] = registry['archive_sha256']
        for path in registry_machine.required_source_paths():
            info['inputs'][str(path)] = startup.digest(Path(path))
        require(info['inputs'][str(registry_machine.PIN)] == startup.digest(registry_machine.PIN) and
                startup.digest(registry_archive) == registry['archive_sha256'], 'registry pin or archive changed')
    if composed or args.suite == 'handshake':
        from linux_docker_handshake_machine import manifest_expectations, tool_inputs
        from linux_docker_handshake_machine import required_source_paths as handshake_sources
        # Manifest-versus-upstream pins are checked before any client execution.
        manifest_expectations()
        for path in handshake_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
        for row in tool_inputs().values():
            info['inputs'][row['path']] = row['sha256']
    if composed or args.suite == 'isolation':
        from linux_docker_isolation_machine import required_source_paths as isolation_sources
        for path in isolation_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if composed or args.suite == 'netpolicy':
        from linux_docker_netpolicy_machine import fixture_contract as netpolicy_fixture_contract
        from linux_docker_netpolicy_machine import required_source_paths as netpolicy_sources
        netpolicy_fixture_contract()
        for path in netpolicy_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if composed or args.suite == 'concurrency':
        from linux_docker_concurrency_machine import fixture_contract as concurrency_fixture_contract
        from linux_docker_concurrency_machine import manifest_expectations as concurrency_expectations
        from linux_docker_concurrency_machine import required_source_paths as concurrency_sources
        # Fixture bytes and the manifest `expected` block are both checked
        # before any client runs, so a drifted pin fails without provisioning.
        concurrency_fixture_contract()
        concurrency_expectations()
        for path in concurrency_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if composed or args.suite == 'mounts':
        from linux_docker_mounts_machine import fixture_contract as mounts_fixture_contract
        from linux_docker_mounts_machine import required_source_paths as mounts_sources
        mounts_fixture_contract()
        for path in mounts_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if composed or args.suite == 'limits':
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
    if composed or args.suite == 'recovery':
        from linux_docker_recovery_machine import fixture_contract as recovery_fixture_contract
        from linux_docker_recovery_machine import required_source_paths as recovery_sources
        recovery_fixture_contract()
        for path in recovery_sources():
            info['inputs'][str(path)] = startup.digest(Path(path))
    if composed or args.suite == "lifecycle":
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
        # Set when a composed run closes its own audit window; the run's final
        # cleanup must publish that capture rather than attempt a second one.
        self.runtime_audit_validation = None
        self.runtime_audit_retirement = None
        self.registry_sessions = []
        self.concurrency_sessions = []
        self.prepared_images = {}
        self.active_suite = None
        self.recovery_sessions, self.recovery_cycles, self.recovery_monitors = [], {}, []
        self.builders_removed, self.live_cleanup = False, False
        self.registry_controls = None
        self.registry_project = None
        # Concurrent per-Machine slices.
        self.slice_records = []
        self.slice_concurrency_records = {}
        self._local = threading.local()

    def fence(self, name):
        """The named reentrant lock, created on first use.

        `slice_lock` pairs the two lists that are one ownership registry
        (`drivers`/`driver_cleanup_verified`) and the builder inventories;
        `mutation_lock` keeps `mutate` to one owned mutation at a time run-wide,
        so the uncertainty fence keeps meaning exactly what it meant when the
        run was serial. Created here rather than in `__init__` because a harness
        assembled field by field for an offline test never runs `__init__`, and
        a fence that silently disappears there is a fence that can silently
        disappear anywhere. `setdefault` decides the winner when two slices
        reach an unbuilt fence at once.
        """
        require(name in ("slice_lock", "mutation_lock"), "unknown harness fence")
        return self.__dict__.get(name) or self.__dict__.setdefault(name, threading.RLock())

    @property
    def record(self):
        """The Recorder this thread must use.

        `Recorder` numbers a receipt by the length of its list and then appends,
        so two threads sharing one would overwrite each other's indices and each
        other's files. A concurrent slice therefore records into a Recorder of
        its own, exactly as `parallel_health` gives its concurrent client one,
        and everything outside a slice keeps the run's single shared Recorder.
        """
        local = getattr(self, "_local", None)
        selected = getattr(local, "record", None) if local is not None else None
        return self.shared_record if selected is None else selected

    @record.setter
    def record(self, value):
        self.shared_record = value

    def slice_recorder(self, suite, index):
        """One concurrent slice's own Recorder, beside that slice's evidence.

        The canary list is deliberately the run's one list object: a secret a
        slice admits must still be refused by every command of every other
        slice, and by every command the run makes after they all finish.
        """
        output = startup.private(self.evidence / (suite + "-machine-" + str(index) + "-commands"))
        record = startup.Recorder(output, self.env)
        record.canaries = self.shared_record.canaries
        with self.fence("slice_lock"):
            self.slice_records.append(record)
        return record

    def register_driver(self, item):
        """Reserve one Driver's ownership slot and return its position.

        `drivers` and `driver_cleanup_verified` are one registry kept in two
        lists, and `assert_certain` demands they stay the same length; two
        slices appending to them independently would pair a Driver with another
        slice's cleanup flag.
        """
        with self.fence("slice_lock"):
            index = len(self.drivers)
            self.drivers.append(item)
            self.driver_cleanup_verified.append(False)
            require(len(self.drivers) == len(self.driver_cleanup_verified),
                    "Driver ownership registry differs")
            return index

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

    def retire_runtime_audits(self):
        """Close every window this run opened.

        The capture reads a journal; it does not end the session. Until the
        enrollment is removed the runtime keeps journaling into it, passes its
        record bound and then warns on every invocation — which fails the next
        operation that requires clean stderr. A run that opens a window owns
        closing it, including the run that opened one only to fail.
        """
        require(len(self.runtime_audits) == 4, 'all four Machine audit sessions required')
        return [session.retire() for session in self.runtime_audits]

    def builder_key(self, descriptor, role):
        """Keyed by the executing suite as well as owner and role: a composed run
        walks several builder-using suites over the same Machines, and suites
        that assert cold-cache behaviour must not inherit a warm builder."""
        require(type(role) is str and role in {"source", "cold-control", "importer"}, "unknown builder role")
        suite = getattr(self, "active_suite", None) or getattr(self, "info", {}).get("suite", "compose")
        return (suite, json.dumps(descriptor["owner"], sort_keys=True, separators=(",", ":")), role)

    def get_builder(self, descriptor, role="source"):
        key = self.builder_key(descriptor, role)
        require(key in self.builder_by_owner_role, "builder owner/role was not prepared")
        builder = self.builder_by_owner_role[key]
        require(builder.descriptor == descriptor, "builder descriptor changed after admission")
        return builder

    def prepare_builder(self, descriptor, role="source", keep_probe=True):
        key = self.builder_key(descriptor, role)
        require(type(keep_probe) is bool, "invalid keep probe selection")
        from linux_docker_buildkit_builder import Builder
        # Both inventories retain exact ownership before any partial effects,
        # and concurrent slices register into them one at a time: the key is
        # per owner and role, but the two inventories must agree.
        with self.fence("slice_lock"):
            require(key not in self.builder_by_owner_role, "builder owner/role already registered")
            builder = Builder(self, json.loads(json.dumps(descriptor)), role=role)
            self.builders.append(builder)
            self.builder_by_owner_role[key] = builder
        builder.prepare()
        if role == "source" and keep_probe:
            from linux_docker_buildkit_keep import run as verify_keep
            # Reserve this proof's own position: `[-1]` would resolve a
            # concurrent slice's reservation, not this one's.
            with self.fence("slice_lock"):
                position = len(self.keep_proofs_verified)
                self.keep_proofs_verified.append(False)
            verify_keep(builder)
            self.keep_proofs_verified[position] = True
        return builder

    def driver_inputs(self, descriptor, scope, proof, images, suite=None):
        """`suite` is the suite currently executing, which is not the run's own
        suite when a composed run walks them in turn."""
        suite = suite or self.info.get("suite", "compose")
        inputs = input_mapping(self, scope, proof, images)
        if suite == "build":
            inputs["builder"] = self.prepare_builder(descriptor).mapping
        return inputs

    def validate_driver(self, output, inputs, suite=None):
        suite = suite or self.info.get("suite", "compose")
        require(suite in {"build", "compose"}, "artifact replay belongs to the artifact orchestrator")
        if suite == "build":
            from linux_docker_build_evidence import validate
        else:
            from linux_docker_compose_evidence import validate
        return validate(output, inputs)

    def mutate(self, label, descriptor, args, **kwargs):
        # One owned mutation at a time, run-wide, even when per-Machine slices
        # run concurrently: `effects_uncertain` is a single fence over every
        # Machine, so a mutation whose effects are unknown must still stop the
        # next mutation on any other Machine, and the sequence numbering that
        # names the retained intent/result documents must stay unique.
        with self.fence("mutation_lock"):
            return self.mutate_once(label, descriptor, args, **kwargs)

    def mutate_once(self, label, descriptor, args, **kwargs):
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
        """Prepared once per Machine: composed runs share one owned fixture image
        rather than registering a second ownership row the first removal invalidates."""
        cached = self.prepared_images.get(descriptor["name"])
        if cached is not None:
            return copy.deepcopy(cached)
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
        images = {"base": {"reference": reference, "id": pin["id"], "platform": "linux/arm64"},
                  "compose": {"reference": row["image_id"], "id": row["image_id"], "platform": "linux/arm64"}}
        self.prepared_images[descriptor["name"]] = copy.deepcopy(images)
        return images

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
        # An isolation Session holds a live container, image, volume and
        # network until the cross-Machine claim is decided; nothing may remain
        # after `retire`.
        for session in getattr(self, 'isolation_sessions', []):
            require(getattr(session, 'cleanup_complete', None) is True,
                    'isolation Session lacks completed cleanup; resources retained; cleanup withheld')
        for session in getattr(self, 'limits_sessions', []):
            require(getattr(session, 'cleanup_complete', None) is True and getattr(session, 'failed', True) is False,
                    'limits Session lacks completed cleanup; containers retained; cleanup withheld')
        # A concurrency Session holds twenty containers, four pulled images and
        # a running private registry until its own exact removal completes.
        for session in getattr(self, 'concurrency_sessions', []):
            require(getattr(session, 'cleanup_complete', None) is True and getattr(session, 'failed', True) is False,
                    'concurrency Session lacks completed cleanup; containers/registry retained; cleanup withheld')
        for session in getattr(self, 'recovery_sessions', []):
            require(getattr(session, 'cleanup_complete', None) is True and getattr(session, 'failed', True) is False,
                    'recovery Session lacks completed cleanup; volumes/containers retained; cleanup withheld')
        for retired in getattr(self, 'recovery_monitors', []):
            require(not retired.thread.is_alive() and not retired.errors, 'retired recovery monitor live or failed; cleanup withheld')
            require(not any(x["effects_uncertain"] for x in retired.record.receipts), 'uncertain retired-monitor command; cleanup withheld')
        require(all(getattr(self, "keep_proofs_verified", [])),
                "unresolved direct-youki keep fixture; resources retained; cleanup withheld")
        # Every Recorder the run created, including the one each concurrent
        # slice recorded into: a slice's uncertainty is the run's uncertainty.
        recorders = [self.record, *getattr(self, "slice_records", []), *(d.record for d in self.drivers)]
        if self.monitor is not None:
            if getattr(self, "live_cleanup", False):
                self.monitor.check()
            else:
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

    def remove_builders(self, *, final=True):
        """Remove every owned BuildKit builder and capture its worker-cache proof.

        A composed run calls this before `recovery` cycles Stop and Up: a builder
        created before that cycle cannot be reconciled after it, because the
        Machine it lived in restarted. `assert_certain` is the final-cleanup
        guard and requires a stopped monitor, so for that window it demands a
        healthy monitor instead of a stopped one.
        """
        if getattr(self, "builders_removed", False):
            return
        self.live_cleanup = not final
        try:
            self.assert_certain()
            for builder in reversed(getattr(self, "builders", [])):
                self.assert_certain()
                jobs = [job for job in self.ssh_cache_requests if job["builder"] is builder]
                require(len(jobs) <= 1, "ambiguous SSH worker-cache ownership")
                if jobs:
                    from linux_docker_ssh_cache_capture import Capture
                    job = jobs[0]

                    def capture(stopped, stop_proof, builder=builder, job=job):
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
        finally:
            self.live_cleanup = False
        self.builders_removed = True

    def remove_owned(self):
        self.remove_builders()
        self.assert_certain()
        for row in reversed(self.owned):
            self.assert_certain()
            descriptor, token = row["descriptor"], row["token"]
            require(not row.get("probe_name"), "unreconciled input probe retained")
            if row.get("kind") == "health":
                # A health probe owns only its container; the image it runs is
                # owned by the fixture row a composed run shares between suites.
                require(row.get("container_id") and row.get("image_id"), "unreconciled health container retained")
                raw, _, _ = self.docker("owned-health-check", descriptor, ["container", "inspect", row["container_id"]])
                item = json.loads(raw)[0]
                require(item["Id"] == row["container_id"] and item["Image"] == row["image_id"] and
                        item["Config"]["Labels"][LABEL] == token, "foreign health container before cleanup")
                self.mutate("owned-health-remove", descriptor, ["container", "rm", "--force", row["container_id"]])
                self.exact_absent(descriptor, "container", token)
                continue
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

    def suites(self):
        """Suites this run executes, in composition order."""
        suite = self.info.get("suite", "compose")
        return list(SUITE_ORDER) if suite == "all" else [suite]

    def prepare_suites(self, suites, contexts, selected_machines, neighbor, project):
        """Everything a suite needs in place before any workload, including the
        sentinels: runtime audits must journal every owned mutation, and the
        registry controls must observe the Docker configs before they change."""
        # A lifecycle-only run keeps the whole-run window it always had: every
        # owned mutation it makes, sentinels included, is inside the journal. A
        # composed run cannot — sentinel sampling alone overruns the 2048-record
        # bound — so it opens its window immediately before the lifecycle suite
        # instead, in `scenario`.
        if suites == ['lifecycle']:
            self.enroll_runtime_audits(contexts)
        if 'registry' not in suites:
            return
        # Three selected Machines run the registry; the fourth is only a
        # same-authority neighbor sentinel. Controls observe all four Docker
        # config directories before, between and after every Session.
        import linux_docker_registry_controls as registry_controls
        require(len(self.registry_sessions) == 0 and self.registry_controls is None, 'registry controls already exist')
        selected_descriptors = [machine["docker_context"] for _, machine in selected_machines]
        sentinel_descriptor = neighbor["machines"][1]["docker_context"]
        require(sentinel_descriptor not in selected_descriptors and
                len({json.dumps(d, sort_keys=True) for d in selected_descriptors}) == len(selected_machines),
                'registry Machine selection')
        self.registry_controls = registry_controls.Controls(self, contexts, selected_descriptors, sentinel_descriptor)
        self.registry_project = str(project)

    def machine_images(self, suite, descriptor):
        if suite in ('images', 'registry', 'handshake', 'recovery'):
            # Driver schema compatibility only: this recipe uses tiny
            # source-selected archives, not the Python/Compose image.
            # Never pull/build/execute those admission-only image pins.
            base = {key: self.info['python_image'][key] for key in ('reference', 'id', 'platform')}
            return {'base': dict(base), 'compose': dict(base)}
        return self.prepare_image(descriptor)

    def run_machine_suite(self, suite, selected_machines, bindings):
        """One suite across its selected Machines; the caller owns the topology.

        `PARALLEL_SUITES` run their slices concurrently. Cross-Machine
        verification is defined over completed slices and stays where it was:
        after every slice, however they were scheduled.
        """
        self.active_suite = suite
        if suite in PARALLEL_SUITES and len(selected_machines) > 1:
            observations = self.run_concurrent_slices(suite, selected_machines, bindings)
        else:
            observations = self.run_serial_slices(suite, selected_machines, bindings)
        self.verify_across_machines(suite, observations, [machine["docker_context"] for _, machine in selected_machines])
        return observations

    def run_serial_slices(self, suite, selected_machines, bindings):
        """Every slice in Machine order, each the only Machine under workload."""
        observations, rows = [], []
        started = time.time_ns()
        for index, (_environment, machine) in enumerate(selected_machines):
            descriptor = machine["docker_context"]
            begin = time.time_ns()
            observations.append(self.run_machine_slice(suite, index, machine, bindings, own_exclusion=True))
            rows.append({"index": index, "context": descriptor["name"], "thread": threading.current_thread().name,
                         "started_unix_ns": begin, "ended_unix_ns": time.time_ns(), "failed": False})
        self.slice_concurrency(suite, rows, started, time.time_ns(), execution="serial")
        return observations

    def run_concurrent_slices(self, suite, selected_machines, bindings):
        """Every slice at once, one thread and one Recorder per Machine.

        Each Machine owns a private Engine on a private socket, so the slices
        themselves are independent by construction; what has to be arranged is
        the harness's own shared state. Three things carry the whole difference
        from the serial path:

        * Owned image preparation happens first, serially. It mutates a Machine
          and every mutation queues behind one run-wide fence, so warming it
          here is the same work in the same order rather than three threads
          taking turns inside their timed regions.
        * Nothing is excluded from sampling. A serial slice stops the monitor
          watching the Machine under test, because the liveness assertions
          subtract that Machine anyway and the sample would only spend its
          bounded youki journal. Here the opposite holds: every Machine in the
          window is a witness for the other two, so every one of them is
          sampled throughout and each slice's interval carries exactly the
          sibling and neighbour observations a serial slice carries. A
          parallelised suite must not prove less than the serial one it
          replaces. (No runtime-audit window is open during these suites; only
          `lifecycle` reads that journal, and it runs serially.)
        * Every thread is joined before anything is raised. A slice that fails
          does not stop its siblings — they were already dispatched — so the
          run waits for all of them and then raises the failure of the
          lowest-numbered Machine, leaving the ownership registries complete.
        """
        names = [machine["docker_context"]["name"] for _, machine in selected_machines]
        require(len(set(names)) == len(names) == len(selected_machines),
                "concurrent slices require distinct Machines")
        self.monitor.check()
        for _environment, machine in selected_machines:
            self.machine_images(suite, machine["docker_context"])
        count = len(selected_machines)
        observations, errors, rows = [None] * count, [None] * count, [None] * count
        threads = []

        local = self.__dict__.setdefault("_local", threading.local())

        def run_slice(index, machine):
            begin = time.time_ns()
            try:
                # Inside the guard: a slice that cannot even open its own
                # Recorder is a failed slice, not a missing one.
                local.record = self.slice_recorder(suite, index)
                observations[index] = self.run_machine_slice(suite, index, machine, bindings,
                                                             own_exclusion=False)
            except BaseException as error:
                errors[index] = error
            finally:
                local.record = None
                rows[index] = {"index": index, "context": names[index],
                               "thread": threading.current_thread().name, "started_unix_ns": begin,
                               "ended_unix_ns": time.time_ns(), "failed": errors[index] is not None}

        started = time.time_ns()
        for index, (_environment, machine) in enumerate(selected_machines):
            thread = threading.Thread(target=run_slice, args=(index, machine),
                                      name="vz-slice-" + suite + "-" + str(index), daemon=False)
            threads.append(thread)
            thread.start()
        for thread in threads:
            # No timeout: every command a slice runs is already bounded by its
            # own recorded timeout, and a thread abandoned here would be a live
            # thread the cleanup fence must then refuse.
            thread.join()
        ended = time.time_ns()
        require(all(not thread.is_alive() for thread in threads), "slice thread did not positively terminate")
        self.slice_concurrency(suite, rows, started, ended, execution="concurrent")
        for error in errors:
            if error is not None:
                raise error
        return observations

    def slice_concurrency(self, suite, rows, started, ended, *, execution):
        """Retain when each Machine's slice actually ran, and whether they overlapped.

        A run that claimed to parallelise and silently serialised would
        otherwise still pass and never be noticed, so the overlap is a recorded
        number and not an assumption: `min_pairwise_overlap_ns` is positive only
        if every pair of slices was in flight at the same moment. A window whose
        slices all succeeded must show that overlap; a window with a failed
        slice records what happened without hiding the failure behind it.
        """
        require(execution in ("serial", "concurrent"), "unknown slice execution mode")
        overlaps = [{"machines": [left["index"], right["index"]],
                     "overlap_ns": min(left["ended_unix_ns"], right["ended_unix_ns"]) -
                                   max(left["started_unix_ns"], right["started_unix_ns"])}
                    for position, left in enumerate(rows) for right in rows[position + 1:]]
        minimum = min((row["overlap_ns"] for row in overlaps), default=0)
        record = {"schema_version": 1, "suite": suite, "execution": execution,
                  "scope": "HARNESS_SLICE_SCHEDULING_OBSERVATION_NOT_ENGINE_CONFORMANCE",
                  "slices": rows, "started_unix_ns": started, "ended_unix_ns": ended,
                  "wall_ns": ended - started,
                  "summed_slice_ns": sum(row["ended_unix_ns"] - row["started_unix_ns"] for row in rows),
                  "pairwise_overlap": overlaps, "min_pairwise_overlap_ns": minimum,
                  "observed_concurrent": bool(overlaps) and minimum > 0}
        startup.document(self.evidence / (suite + "-machine-concurrency.json"), record)
        self.__dict__.setdefault("slice_concurrency_records", {})[suite] = record
        if execution == "concurrent" and not any(row["failed"] for row in rows):
            require(record["observed_concurrent"],
                    "parallel " + suite + " slices did not overlap: " + repr(overlaps))
        return record

    def run_machine_slice(self, suite, index, machine, bindings, *, own_exclusion):
        """One Machine's slice of one suite.

        The interval this slice closes and checks is about its own Machine, and
        every other Machine witnesses it — a concurrently running sibling
        included, so a concurrent slice's liveness evidence is the same evidence
        a serial slice's is. `own_exclusion` stops the monitor sampling this
        Machine for the duration; a serial slice takes it, because the
        assertions subtract this Machine anyway and the sample would only spend
        its bounded youki journal. A concurrent slice does not, because its
        Machine is a witness for the others running beside it.
        """
        self.monitor.check()
        descriptor = machine["docker_context"]
        active = descriptor["name"]
        scope, proof = bindings[machine["machine_id"]]
        images = self.machine_images(suite, descriptor)
        exclusion = self.monitor.excluding(active) if own_exclusion else contextlib.nullcontext()
        if suite in {"artifacts", "parallel", "ssh", "lifecycle", "images", "registry", "handshake", "limits",
                     "mounts", "netpolicy", "isolation", "concurrency", "recovery"}:
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
            elif suite == 'mounts':
                from linux_docker_mounts_machine import run_machine
            elif suite == 'netpolicy':
                from linux_docker_netpolicy_machine import run_machine
            elif suite == 'isolation':
                from linux_docker_isolation_machine import run_machine
            elif suite == 'concurrency':
                from linux_docker_concurrency_machine import run_machine
            elif suite == 'recovery':
                from linux_docker_recovery_machine import run_machine
            else:
                from linux_docker_container_lifecycle import run_machine
            with exclusion:
                begin = time.time_ns()
                observation = run_machine(self, descriptor, scope, proof, images, index)
                end = self.monitor.close_interval(begin, active)
                self.monitor.check_interval(begin, end, active)
            return observation
        inputs = self.driver_inputs(descriptor, scope, proof, images, suite)
        admitted = driver.Inputs(inputs, suite=suite)
        admitted.verify_runtime_evidence()
        output = self.evidence / (suite + "-machine-" + str(index))
        selected = driver.Driver(admitted, Path(self.info["fixture"]), output)
        position = self.register_driver(selected)
        with exclusion:
            begin = time.time_ns()
            result = selected.run(suite)
            end = self.monitor.close_interval(begin, active)
            require(result["outcome"] == "fixture_assertions_passed", suite + " slice failed: " +
                    str({"failure": result.get("failure"), "cleanup_errors": result.get("cleanup_errors")}))
            require(result["cleanup_errors"] == [], "Docker fixture cleanup failed semantically")
            builder_runtime = None
            if suite == "build":
                builder = self.get_builder(descriptor)
                builder_runtime = builder.verify(require_invocation=True)
                from linux_docker_buildkit_keep import verify_worker_log
                builder_runtime["post_workload_log"] = verify_worker_log(builder)
            replay = self.validate_driver(output, inputs, suite)
            self.driver_cleanup_verified[position] = True
            self.monitor.check_interval(begin, end, active)
        observation = {"scope": scope, "started_unix_ns": begin, "ended_unix_ns": end,
                       "independent_validation": replay}
        if builder_runtime is not None:
            observation["builder_runtime"] = builder_runtime
        return observation

    def verify_across_machines(self, suite, observations, descriptors):
        """Claims one Machine cannot prove alone, checked once the suite's slices exist.

        The record is retained beside the per-Machine evidence and is what the
        lane result cites for the cross-Machine part of the claim.
        """
        if suite == "handshake":
            from linux_docker_handshake_machine import verify_machines
            startup.document(self.evidence / "handshake-cross-machine.json",
                             verify_machines(self, observations, descriptors))
        elif suite == "build":
            from linux_docker_buildkit_builder import verify_cache_isolation
            startup.document(self.evidence / "build-cross-machine.json",
                             verify_cache_isolation([row["builder_runtime"] for row in observations],
                                                    [row["scope"] for row in observations]))
        elif suite == "mounts":
            from linux_docker_mounts_machine import verify_machines as verify_volume_isolation
            startup.document(self.evidence / "mounts-cross-machine.json",
                             verify_volume_isolation(observations))
        elif suite == "isolation":
            from linux_docker_isolation_machine import (retire, retire_sibling, retire_sibling_owned,
                                                        sibling_environment, sibling_inventory,
                                                        sibling_owned, verify_siblings,
                                                        verify_machines as verify_machine_isolation)
            proof = verify_machine_isolation(observations)
            # The sibling claim needs a third Environment the gate topology does
            # not provision. It is created here, read, and deleted again, so no
            # other suite's Machine selection or sentinels change.
            sibling = sibling_environment(self, descriptors[0])
            try:
                # Give the sibling resources of its own first: disjointness from an
                # Environment that owns nothing is true for the wrong reason.
                sibling = sibling | sibling_owned(self, sibling)
                sibling["inventory"] = sibling_inventory(self, sibling)
                proof = proof | {"siblings": verify_siblings(observations, sibling)}
            finally:
                proof = proof | {"sibling_owned_retired": retire_sibling_owned(self, sibling),
                                 "sibling_retired": retire_sibling(self, sibling)}
            startup.document(self.evidence / "isolation-cross-machine.json", proof | {"retired": retire(self)})

    def run_suite_with_audit_window(self, suite, suites, contexts, selected_machines, bindings):
        """One suite, inside its runtime-audit window when it needs its own.

        Only `lifecycle` reads the youki journal, and only a composed run has
        to bound it: a whole-run window overruns the 2048-record limit on
        sentinel sampling alone. So the window opens immediately before the
        suite and closes immediately after it, which is complete evidence for
        this suite because it removes its own containers and image before it
        returns. A `--suite lifecycle` run keeps the whole-run window
        `prepare_suites` opened and captures it after owned removal, as before.
        """
        composed = suite == 'lifecycle' and len(suites) > 1
        if composed:
            # Enrollment creates the session journal and snapshots it, and the
            # snapshot must show it empty. A sibling sample is a `docker exec`,
            # which is a youki invocation, which writes a record — so one
            # landing between those two steps enrolls a journal that is already
            # not fresh. Same requirement as the capture below, for the same
            # reason: this window has to be quiet on every Machine.
            with self.monitor.paused():
                self.enroll_runtime_audits(contexts)
        observations = self.run_machine_suite(suite, selected_machines, bindings)
        if composed:
            # Capture snapshots the journal and requires an independent replay
            # to match it exactly, so nothing may write to it in between; a
            # sibling sample is the one writer still running at this point.
            #
            # `assert_certain` is the final-cleanup guard and demands a stopped
            # monitor. This capture is mid-run, so it demands a healthy one
            # instead, the same window `remove_builders(final=False)` uses.
            self.live_cleanup = True
            try:
                with self.monitor.paused():
                    self.runtime_audit_validation = self.capture_runtime_audits()
                    # Close it here, not at the end of the run: everything after
                    # this suite keeps invoking the runtime, and a window left
                    # open fills past its bound and then warns on every
                    # invocation, which is what fails the next Up.
                    self.runtime_audit_retirement = self.retire_runtime_audits()
            finally:
                self.live_cleanup = False
        return observations

    def scenario(self):
        suites = self.suites()
        project = self.project(self.info.get("suite", "compose"), "developer", 2)
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
        selected_machines = [(primary, m) for m in primary["machines"]] + [(neighbor, neighbor["machines"][0])]
        selected_machines = selected_machines[:self.info.get("machines", len(GATE_MACHINES))]
        self.prepare_suites(suites, contexts, selected_machines, neighbor, project)
        sentinels = [self.sentinel(descriptor) for descriptor in contexts]
        self.monitor = SentinelMonitor(self, sentinels)
        slices, credential_controls = {}, None
        try:
            self.monitor.start()
            for suite in suites:
                if suite == 'recovery' and len(suites) > 1:
                    # Stop/Up replaces the Machine a builder lives in, so owned
                    # builders are reconciled and removed before that cycle.
                    self.remove_builders(final=False)
                controls = self.registry_controls if suite == 'registry' else None
                if controls is not None:
                    credential_controls = {'baseline': controls.baseline()}
                slices[suite] = self.run_suite_with_audit_window(
                    suite, suites, contexts, selected_machines, bindings)
                if controls is not None:
                    require(len(self.registry_sessions) == len(selected_machines) and
                            all(s.cleanup_complete is True for s in self.registry_sessions),
                            'every selected Machine needs a completed registry Session')
                    credential_controls['final'] = controls.final()
            if 'recovery' in suites:
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
        composed = self.info.get("suite") == "all"
        result = {"machine_slices": [row for suite in suites for row in slices.get(suite, [])],
                  "continuous_sentinels": self.monitor.summary(),
                  "runtime_inventory_scope": "startup_executable_paths_and_pinned_daemon_mounts_not_release_cache_audit"}
        if getattr(self, "slice_concurrency_records", None):
            # How each suite's Machine slices were actually scheduled, so a run
            # that claimed to parallelise and did not is visible in the result
            # rather than only in the wall clock.
            result["suite_concurrency"] = copy.deepcopy(self.slice_concurrency_records)
        if composed:
            result["suite_slices"] = slices
            result["suites_executed"] = list(suites)
        if credential_controls is not None:
            result["credential_controls"] = credential_controls
        return result


class SentinelMonitor:
    """Independent, bounded raw observations; no retries, restarts or repairs."""
    def __init__(self, harness, rows, *, output=None, thread_name="vz-compose-sibling-liveness"):
        # Subclasses vary only in where their evidence goes and what their
        # thread is called, and they must reach this initializer: a subclass
        # that rebuilt these fields itself silently missed every field added
        # here afterwards, which is how the recovery monitor lost first the
        # probe cache and then the exclusion set.
        self.harness, self.rows = harness, rows
        self.output = startup.private(
            output if output is not None else harness.evidence / "sibling-liveness"
        )
        self.record = startup.Recorder(self.output, harness.env)
        self.finished, self.first = threading.Event(), threading.Event()
        self.samples, self.errors = [], []
        self.probes = {}
        # Context names this monitor must not sample right now. Every liveness
        # assertion already excludes the Machine running the workload
        # (`close_interval` subtracts it, `check_interval` skips it), so
        # sampling it produced evidence no check reads while spending that
        # Machine's bounded youki runtime-audit journal. Rebinding the whole
        # frozenset is one attribute store, which the sampling thread reads
        # atomically; no lock is needed and none is taken on the sampling path.
        self.excluded = frozenset()
        self.thread = threading.Thread(target=self.loop, name=thread_name, daemon=False)

    @contextlib.contextmanager
    def excluding(self, *names):
        """Stop sampling these Machines for the duration of the block.

        Sampling resumes the moment the block ends, including on failure, so a
        Machine is unobserved only while it is the one under test or while its
        journal is being captured.

        Save-and-restore is deliberately not thread-safe, and concurrent slices
        never take it: two threads entering this independently would each
        restore a set captured before the other joined. A concurrent window
        excludes nothing, because every Machine in it witnesses the others.
        """
        previous = self.excluded
        self.excluded = previous | frozenset(names)
        try:
            yield
        finally:
            self.excluded = previous

    def paused(self):
        """Exclude every Machine, for work that needs a quiescent journal.

        An audit capture snapshots the journal, replays it independently and
        requires the two to be identical; a sample arriving between them would
        make that comparison fail for a reason unrelated to the evidence.
        """
        return self.excluding(*(row["descriptor"]["name"] for row in self.rows))

    def command(self, descriptor, args, *, after_stop=False):
        # The guard exists so a sampling thread stops dispatching the moment the
        # monitor is finished. The closing route check runs deliberately after
        # that point, once the thread has been joined.
        if self.finished.is_set() and not after_stop:
            raise MonitorStopped()
        return self.record.run("sentinel", ["docker", "--config", descriptor["config_dir"], "--context", descriptor["name"], *args],
                               executable=self.harness.info["clients"]["docker"]["canonical"], cwd=self.harness.root, timeout=8)

    def route_check(self, row, *, after_stop=False):
        """The context still names this Machine's endpoint. This is a property of
        the CLI's own configuration, so it is the one observation that must go
        through the client; it cannot change without the harness changing it, so
        it is checked when the monitor starts and again when it stops."""
        descriptor = row["descriptor"]
        raw, _, _ = self.command(descriptor, ["context", "inspect", descriptor["name"]], after_stop=after_stop)
        require(json.loads(raw)[0]["Endpoints"]["docker"]["Host"] == descriptor["endpoint"], "sentinel context rerouted")

    def probe_for(self, descriptor):
        # Every monitor runs `__init__`, so the cache exists. It is not created
        # on demand here: a second cache built lazily would be a monitor whose
        # probes nothing closes.
        probe = self.probes.get(descriptor["name"])
        if probe is None:
            probe = engine_probe.EngineProbe(descriptor["endpoint"], timeout=8)
            self.probes[descriptor["name"]] = probe
        return probe

    def sample(self, row):
        """One liveness observation, taken straight from the Machine's own socket.

        The Docker CLI costs about 17 ms of process startup before it reaches the
        Engine, and this runs four times a second per Machine for the length of a
        run; that load falls on the very Engines the suites are being timed
        against. The questions asked are unchanged and the endpoint is the one the
        Docker context names. This is the harness's own observation, never
        scenario evidence: every contract behaviour goes through the CLI.
        """
        if self.finished.is_set():
            raise MonitorStopped()
        descriptor = row["descriptor"]
        probe = self.probe_for(descriptor)
        require(probe.info().get("ID") == descriptor["engine_id"], "sentinel Engine changed")
        item = probe.container(row["container_id"])
        require(item["Id"] == row["container_id"] and item["Image"] == row["image_id"] and item["State"]["Running"] and
                item["State"]["StartedAt"] == row["started_at"] and item["RestartCount"] == 0 and
                item["Config"]["Labels"][LABEL] == row["token"], "sentinel stopped/restarted/replaced")
        raw = probe.exec_stdout(row["container_id"], ["/bin/cat", "/sentinel"])
        require(raw == (row["token"] + "\n").encode(),
                "host-written sentinel changed: " + repr(raw[:120]))
        self.samples.append({"context": descriptor["name"], "unix_ns": time.time_ns(), "container_id": row["container_id"]})

    def loop(self):
        try:
            while not self.finished.is_set():
                # Read once per pass so a row cannot be sampled against one
                # exclusion set and skipped against another within a pass.
                excluded = self.excluded
                for row in self.rows:
                    if row["descriptor"]["name"] in excluded:
                        continue
                    self.sample(row)
                self.first.set()
                self.finished.wait(1)
        except MonitorStopped:
            pass
        except BaseException as error:
            self.errors.append(f"{type(error).__name__}: {error}")
            self.first.set()

    def start(self):
        for row in self.rows:
            self.route_check(row)
        self.thread.start()
        require(self.first.wait(45), "initial sibling observation deadline exceeded")
        self.check()

    def check(self):
        require(not self.errors and self.thread.is_alive(), "sibling liveness failed: " + repr(self.errors))

    def observers(self, active):
        """The Machines whose liveness this interval asserts: every one but `active`.

        Concurrent slices do not narrow this. Their Machines keep being sampled
        while they work, so a slice's siblings witness it whether they are busy
        or idle, and a parallelised suite's liveness claim is the serial one.
        """
        observers = {row["descriptor"]["name"] for row in self.rows} - {active}
        require(observers, "no unobserved Machine remains to witness this interval")
        return observers

    def close_interval(self, begin, active, deadline_seconds=5.0):
        """Return an interval end only once every sibling has a sample at or after begin.

        Fast Machine workloads can finish inside the one-second sampling cadence;
        this bounded wait is a declared readiness condition for contemporaneous
        liveness evidence, not a retry, and it never relaxes check_interval.
        """
        deadline = time.monotonic() + deadline_seconds
        names = self.observers(active)
        while True:
            self.check()
            # One snapshot per pass: the sampling thread keeps appending, and a
            # name must not be judged against two different sample lists.
            samples = list(self.samples)
            if all(any(x["context"] == name and x["unix_ns"] >= begin for x in samples) for name in names):
                return time.time_ns()
            require(time.monotonic() < deadline, "sibling liveness sample did not arrive within the interval deadline")
            self.finished.wait(0.05)

    def check_interval(self, begin, end, active):
        self.check()
        samples = list(self.samples)
        for name in self.observers(active):
            require(any(x["context"] == name and begin <= x["unix_ns"] <= end for x in samples),
                    "no contemporaneous sibling/neighbor liveness observation")

    def stop(self):
        # Stopping twice must not raise: a caller that already stopped this
        # monitor and then failed would otherwise have its real error masked by
        # an exclusive write of the samples this call repeats.
        if getattr(self, "stopped", False):
            return
        self.stopped = True
        self.finished.set()
        self.thread.join(timeout=40)
        require(not self.thread.is_alive(), "monitor did not positively terminate; no cleanup allowed")
        for probe in self.probes.values():
            probe.close()
        if not self.errors:
            for row in self.rows:
                self.route_check(row, after_stop=True)
        startup.document(self.output / "samples.json", self.summary())
        require(not self.errors, "sibling liveness failed: " + repr(self.errors))

    def summary(self):
        return {"samples": list(self.samples), "errors": list(self.errors), "test_case_retries": 0,
                "scope": "Engine_identity_and_nonrestarted_container_host_written_marker_not_network_service_conformance"}


class MonitorStopped(Exception):
    """Cooperative cancellation between bounded read-only commands."""


def executes(info, suite):
    """True when this run performs that suite, directly or as part of `all`.

    A composed run performs exactly SUITE_ORDER, which is not every suite.
    """
    return info["suite"] == suite or (info["suite"] == "all" and suite in SUITE_ORDER)


def run(info):
    os.umask(0o077)
    harness = ComposeHarness(info)
    startup.document(harness.evidence / "inputs.json", info)
    result = {"schema_version": 1, "scope": info["scope"], "suite": info["suite"], "outcome": "failed", "error": None,
              "cleanup_errors": [], "docker_parity_certified": False, "aggregate_release_certified": False,
              "release_scenarios_passed": [], "test_case_retries": 0, "retained_root": str(harness.root)}
    try:
        if executes(info, "ssh"):
            import linux_docker_ssh_input as ssh_input
            result["ssh_input_verification"] = ssh_input.verify(Path(info["ssh_packages"]),
                harness.evidence / "ssh-input-verification", info["ssh_gpgv"], image_path=Path(info["image_input"]))
        harness.stage()
        result["scenario"] = harness.scenario()
        for path, expected in (info["inputs"] | harness.staged_inputs).items():
            require(startup.digest(Path(path)) == expected, "selected input changed during physical run")
        require(driver.tree_digest(Path(info["fixture"])) == info["fixture_sha256"], "fixture changed during run")
        if executes(info, "parallel") or executes(info, "limits") or executes(info, "concurrency"):
            require(driver.tree_digest(Path(info["parallel_fixture"])) == info["parallel_fixture_sha256"],
                    "parallel fixture changed during run")
        if executes(info, "ssh"):
            require(driver.tree_digest(Path(info["ssh_fixture"])) == info["ssh_fixture_sha256"], "SSH fixture changed during run")
        if executes(info, "lifecycle"):
            from linux_docker_container_fixture import fixture_contract
            selected = Path(info["container_fixture"])
            fixture_contract(selected)
            require(driver.tree_digest(selected) == info["container_fixture_sha256"], "container fixture changed during run")
        selected = info.get("machines", len(GATE_MACHINES))
        if executes(info, "registry"):
            sessions = harness.registry_sessions
            require(len(sessions) == selected and all(s.cleanup_complete is True and s.failed is False for s in sessions),
                    "every selected Machine needs a completed registry Session")
            require(startup.digest(Path(info["registry_archive"])) == info["registry"]["archive_sha256"],
                    "registry archive changed during run")
        if executes(info, "concurrency"):
            sessions = harness.concurrency_sessions
            require(len(sessions) == selected and all(s.cleanup_complete is True and s.failed is False for s in sessions),
                    "every selected Machine needs a completed concurrency Session")
            require(startup.digest(Path(info["registry_archive"])) == info["registry"]["archive_sha256"],
                    "registry archive changed during run")
        if executes(info, "recovery"):
            sessions = harness.recovery_sessions
            require(len(sessions) == selected and all(s.cleanup_complete is True and s.failed is False for s in sessions),
                    "every selected Machine needs a completed recovery Session")
    except BaseException as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        try:
            require(harness.monitor is None or not harness.monitor.thread.is_alive(), "live monitor prevents cleanup")
            harness.remove_owned()
            if executes(info, 'lifecycle'):
                # A composed run closed its own window right after the suite;
                # a lifecycle-only run closes it here, after owned removal.
                captured = getattr(harness, 'runtime_audit_validation', None)
                if captured is None:
                    captured = harness.capture_runtime_audits()
                    # A lifecycle-only run closes its window here, after owned
                    # removal, for the same reason a composed one closes its own
                    # right after the suite: an enrollment left behind keeps
                    # journaling into the Machine that is about to be stopped.
                    harness.runtime_audit_retirement = harness.retire_runtime_audits()
                result['runtime_audit_validation'] = captured
                result['runtime_audit_retirement'] = harness.runtime_audit_retirement
            if executes(info, "ssh"):
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
            lane_result.write(ctx, lane_result.failed(ctx, "input_rejected", f"{type(error).__name__}: {error}", 2))
        return 2
    if ctx is not None:
        harness = ctx.harness_dir()
        result = None
        try:
            result = lane_result.load_result(harness)
        except (Exception, KeyboardInterrupt):
            result = None
        lane_result.write(ctx, lane_result.translate_or_failure(ctx, result, info, harness, code))
    return code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
