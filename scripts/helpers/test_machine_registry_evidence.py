"""Positive and adversarial tests for the independent physical-lane validator."""

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from test_docker_time_namespace_evidence import fixture as time_namespace_fixture
from test_docker_device_policy_evidence import fixture as device_policy_fixture
from test_docker_seccomp_policy_evidence import fixture as seccomp_policy_fixture
from machine_registry_evidence import (
    InvalidEvidence,
    bundle_digest,
    configuration_digest,
    resource_name,
    unique_object,
    validate,
    validate_host_endpoint,
    validate_live_sessions,
    validate_probe_pin,
    verify_serial_files,
)


def live_session_fixture(machines):
    host = host_fixture(machines)
    commands = {}
    for name, original in [("before_a", "info_a"), ("before_b", "info_b"),
                           ("stopped_a", "stopped_a"), ("surviving_b", "surviving_b")]:
        command = copy.deepcopy(host["commands"][original])
        command["args"] = ["info", "--format", "{{json .}}"]
        for field in ["endpoint", "config"]:
            command[field] = command[field].replace("vz-de-fixture", "vz-ls-fixture")
        commands[name] = command
    return {"scope": "registered_original_runtime_stop_only", "commands": commands,
            "sockets_removed": True, "restart_recovery": False, "public_stop": False,
            "receipts": [{"owner": machine["owner"], "operation_id": "lop_stop_fixture",
                          "generation": 2, "runtime_identity": machine["first_identity"],
                          "endpoint": copy.deepcopy(host["shutdown"][0]) if name != "hardened" else None,
                          "docker_shutdown": {"request_id": "lop_stop_fixture", "data_device": "/dev/vda",
                              "data_mount": "/var/lib/docker", "supervisor_started": True, "dockerd_reaped": True,
                              "containerd_reaped": True, "filesystem_synced": True, "filesystem_unmounted": True,
                              "never_started_unmounted": False, "filesystem_state": "clean",
                              "filesystem_features": ["has_journal", "extent", "filetype"],
                              "filesystem_uuid": ("a" if name == "developer_a" else "b") + "74d56ba-57cf-4c43-bf89-f9347397e18b"}
                              if name != "hardened" else None,
                          "outcome": "stopped"} for name, machine in machines.items()]}


def host_fixture(machines):
    commands = {}
    def command(name, args, stdout, payload=b"", code=0):
        target = "b" if name.endswith("_b") else "a"
        commands[name] = {"args": args, "endpoint": "unix:///private/tmp/vz-de-fixture/" + target + ".sock",
                          "config": "/private/tmp/vz-de-fixture/client", "exit_code": code,
                          "stdout": stdout, "stderr": "connection refused" if code else "",
                          "input_bytes": len(payload), "input_sha256": hashlib.sha256(payload).hexdigest(),
                          "elapsed_ms": 1}
    command("client_version", ["--version"], "Docker version 29.4.0, build fixture\n")
    for name in ["info_a", "info_b", "surviving_b"]:
        command(name, ["info", "--format", "{{json .}}"],
                json.dumps({"ID": "engine-a" if name.endswith("_a") else "engine-b", "DefaultRuntime": "youki", "MemoryLimit": True,
                            "Runtimes": {"youki": {"status": {"org.opencontainers.runtime-spec.features":
                                                              json.dumps({"linux": {"cgroup": {"v2": True}}})}}}}))
    command("stopped_a", ["info"], "", code=1)
    image = "vz-endpoint-fixture:local"
    run = ["run", "--rm", "--network", "none", "-v", "vz-endpoint-shared:/data", image, "/bin/busybox"]
    for name, marker in [("a", "developer-a"), ("b", "developer-b")]:
        command("import_" + name, ["image", "import", "-", image], "sha256:" + "a" * 64 + "\n", payload=b"fixture-tar")
        command("volume_" + name, ["volume", "create", "--label", "dev.vz.endpoint.owner=" + marker, "vz-endpoint-shared"],
                "vz-endpoint-shared\n")
        command("write_" + name, run + ["sh", "-c", f"printf {marker} > /data/marker; /bin/busybox cat /data/marker"], marker)
        command("read_" + name, run + ["cat", "/data/marker"], marker)
        command("memory_" + name, ["run", "--rm", "--network", "none", "--memory", "64m",
                                   image, "/bin/busybox", "cat", "/sys/fs/cgroup/memory.max"], "67108864\n")
    payload = b"vz-endpoint-half-close\n" * 12_000
    command("stdin_eof", ["run", "-i", "--rm", "--network", "none", image, "/bin/busybox", "sh", "-c",
                          "/bin/busybox cat; /bin/busybox sleep 1; printf done"], payload.decode() + "done", payload)
    command("events_a", ["events", "--since", "100", "--until", "103", "--filter", "type=volume", "--filter", "event=create", "--format", "{{json .}}"],
            json.dumps({"Type": "volume", "Action": "create", "Actor": {"ID": "vz-endpoint-event"}}))
    command("event_volume_a", ["volume", "create", "--label", "dev.vz.endpoint.owner=developer-a", "vz-endpoint-event"], "vz-endpoint-event\n")
    return {"scope": "focused_host_endpoint_transport_only", "client": "/usr/local/bin/docker",
            "client_sha256": "a" * 64, "busybox_sha256": "b" * 64,
            "owners": [machines[name]["owner"] for name in ["developer_a", "developer_b"]],
            "runtime_identities": [machines[name]["first_identity"] for name in ["developer_a", "developer_b"]],
            "commands": commands, "socket_modes": [0o600, 0o600], "sockets_removed": True,
            "time_namespaces": [time_namespace_fixture(
                machines["developer_" + name], commands["info_" + name]["endpoint"],
                commands["info_" + name]["config"]) for name in ["a", "b"]],
            "device_policies": [device_policy_fixture(
                machines["developer_" + name], commands["info_" + name]["endpoint"],
                commands["info_" + name]["config"]) for name in ["a", "b"]],
            "seccomp_policies": [seccomp_policy_fixture(
                machines["developer_" + name], commands["info_" + name]["endpoint"],
                commands["info_" + name]["config"]) for name in ["a", "b"]],
            "unrelated_file_preserved": True, "managed_contexts": False, "compose_buildx": False,
            "hardened_refusal": "Developer Linux Machine required", "preexisting_path_refusal": "path already exists",
            "shutdown": [{"accepted_connections": 5, "completed_connections": 3, "cancelled_connections": 1,
                          "failed_connections": 1, "active_connections": 0, "socket_removed": True} for _ in range(2)]}


def fixture():
    def immutable(inode, mode, size, links=1):
        return {"device": 1, "inode": inode, "mode": mode, "uid": 501, "links": links,
                "size": size, "mtime_seconds": 1_780_000_000, "mtime_nanoseconds": inode,
                "ctime_seconds": 1_780_000_000, "ctime_nanoseconds": inode + 1}

    build = {"profile": "release", "test_binary_sha256": "1" * 64,
             "developer_initramfs_sha256": "2" * 64, "container_initramfs_sha256": "3" * 64,
             "docker_probe_sha256": "4" * 64, "docker_probe_source_sha256": "5" * 64,
             "docker_probe_go_version": "go version go1.25.4 darwin/arm64"}
    topology = {"project_id": "prj_fixture", "environment_id": "env_fixture",
                "attempted_activation_capabilities": ["posix_exec"], "docker_capabilities_synthesized": False}
    for key in ["creating_owned_read_only", "failed_up_owned_read_only", "stopped_owned_read_only",
                "reopened_stopped_owned_read_only", "final_stopped_owned_read_only",
                "developer_ready_without_docker_conformance_rejected", "hardened_activation_published"]:
        topology[key] = True
    machines = {}
    storage = {"data_roots": [], "data_root_identities": [], "developer_docker_disks": [],
               "developer_docker_disk_identities": [], "installed_artifacts": {},
               "private_0700_and_distinct_inodes": True, "all_writable_roots_below_machine_data": True,
               "developer_docker_disks_distinct_inodes": True, "hardened_docker_state_absent": True}
    for index, name in enumerate(["developer_a", "developer_b", "hardened"]):
        owner = {"project_id": "prj_fixture", "environment_id": "env_fixture", "machine_id": "mch_" + name}
        profile = "developer" if index < 2 else "container"
        version = {"profile": profile, "busybox": "1.37.0", "sha256_vmlinux": "6" * 64,
                   "sha256_initramfs": build["developer_initramfs_sha256" if index < 2 else "container_initramfs_sha256"],
                   "sha256_youki": "7" * 64}
        if index < 2:
            version["developer_probe"] = {"schema_version": 1, "archive": "developer-probe-rootfs.tar",
                "sha256": "a" * 64, "busybox_sha256": "b" * 64, "busybox_version": "1.37.0",
                "source_archive_sha256": "c" * 64, "source_inventory_sha256": "d" * 64,
                "build_provenance_sha256": "e" * 64,
                "marker_sha256": hashlib.sha256(b"vz-developer-probe-v1\n").hexdigest()}
        version_json = json.dumps(version, sort_keys=True, separators=(",", ":"))
        artifact_names = ["vmlinux", "initramfs.img", "youki", "version.json"] + (["developer-probe-rootfs.tar"] if index < 2 else [])
        artifact = {"profile": profile, "kernel_sha256": "6" * 64,
                    "initramfs_sha256": build["developer_initramfs_sha256" if index < 2 else "container_initramfs_sha256"],
                    "youki_sha256": "7" * 64, "version_sha256": hashlib.sha256(version_json.encode()).hexdigest()}
        artifact_identity = {key: value for key, value in artifact.items() if key != "profile"}
        artifact_identity["digest"] = bundle_digest(artifact_identity)
        resources = {"cpus": 2, "memory_mb": 4096 if index < 2 else 1024}
        resolved = {
            "schema_version": 1,
            "host": {"os": "macos", "arch": "aarch64"},
            "backend": "macos_virtualization_linux",
            "machine": {
                "schema_version": 1,
                "name": name.replace("_", "-"),
                "profile": "developer" if index < 2 else "hardened",
                "target": {
                    "os": "linux",
                    "arch": "aarch64",
                    "image": "vz-linux-appliance",
                    "version": "0.4.0-registry-e2e",
                    "channel": "local-physical-e2e",
                    "digest": artifact_identity["digest"],
                },
                "resources": resources,
                "requested_capabilities": {"capabilities": ["posix_exec"]},
            },
            "release_version": "0.4.0-registry-e2e",
            "kernel_profile": profile,
            "artifact": artifact_identity,
            "resources": resources,
        }
        configuration_bytes = json.dumps(resolved, sort_keys=True, separators=(",", ":"),
                                         ensure_ascii=False).encode()
        machine = {"owner": owner, "verified_profile": profile, "resolved_configuration": resolved,
                   "configuration_digest": configuration_digest(resolved),
                   "artifact": dict(artifact, bundle="/private/tmp/fixture/source-bundles/" +
                                    ("developer" if index < 2 else "hardened"))}
        for field, kind, logical in [("store_reservation", "machine_runtime_store", "runtime"), ("vm_reservation", "runtime_vm", "vm")]:
            machine[field] = {"schema_version": 1, "resource_kind": {"other": kind},
                              "resource_id": resource_name(owner, kind, logical),
                              "environment_id": owner["environment_id"], "machine_id": owner["machine_id"]}
        for offset, phase in enumerate(["first_identity", "reopened_identity"]):
            machine[phase] = {"schema_version": 1, "stack_id": machine["vm_reservation"]["resource_id"],
                              "incarnation_id": "00000000-0000-4000-8000-" + f"{index * 2 + offset + 1:012x}"}
        machines[name] = machine
        root = "/private/tmp/fixture/registry/topology-machines/" + machine["store_reservation"]["resource_id"] + "/data"
        storage["data_roots"].append(root)
        storage["data_root_identities"].append({"device": 1, "inode": 100 + index, "mode": 0o700, "uid": 501, "links": 5, "size": 1280})
        pin_dir = root + "/linux-target"
        bundle_dir = pin_dir + "/bundle"
        storage["installed_artifacts"][name.replace("_", "-")] = dict(
            artifact,
            pin_dir=pin_dir,
            dir=bundle_dir,
            configuration_path=pin_dir + "/configuration.json",
            configuration_sha256=hashlib.sha256(configuration_bytes).hexdigest(),
            pin_directory_identity=immutable(300 + index * 10, 0o700, 128, links=3),
            bundle_directory_identity=immutable(301 + index * 10, 0o500, 256, links=2),
            configuration_identity=immutable(302 + index * 10, 0o400, len(configuration_bytes)),
            version_json=version_json,
            developer_probe_sha256="a" * 64 if index < 2 else None,
            artifact_identities={file: immutable(303 + index * 10 + offset,
                                                 0o500 if file == "youki" else 0o400,
                                                 len(version_json.encode()) if file == "version.json" else 1024 + offset)
                                 for offset, file in enumerate(artifact_names)},
        )
        if index < 2:
            disk_key = hashlib.sha256(machine["vm_reservation"]["resource_id"].encode()).hexdigest()
            storage["developer_docker_disks"].append(root + "/docker-machines/" + disk_key + "/data.img")
            storage["developer_docker_disk_identities"].append({"device": 1, "inode": 200 + index, "mode": 0o600, "uid": 501, "links": 1, "size": 64 * 1024**3})
    outputs = {}
    for name in ["developer_a", "developer_b"]:
        marker = name.replace("_", "-")
        for suffix in ["_create", "_first_verify", "_reopened_verify"]:
            outputs[name + suffix] = {"operation": "create" if suffix == "_create" else "verify", "name": "vz-registry-shared",
                                      "mountpoint": "/var/lib/docker/engine/volumes/vz-registry-shared/_data", "marker": marker,
                                      "marker_sha256": hashlib.sha256(marker.encode()).hexdigest(), "api_owner": marker}
    lease = dict.fromkeys(["same_registry_admission_reused_arc", "same_boot_request_replayed_identity", "stale_generation_refused_before_boot",
                          "resource_drift_refused_without_replacement", "activation_retained_store_lock_after_registry_drop",
                          "activation_exec_after_registry_drop", "cold_reopen_new_identities", "old_identities_refused_as_replacements"], True)
    lease["locked_reopen_store_acquisition_refused"] = True
    serial = {"regular_nonempty": True}
    for phase, suffix in [("first_boot", ".first-boot.log"), ("second_boot", ".log")]:
        serial[phase] = [{"path": "/private/tmp/evidence/machine-registry-vm-serial/" + machine["vm_reservation"]["resource_id"] + suffix,
                          "sha256": "9" * 64} for machine in machines.values()]
    return {"schema_version": 1, "scope": "registry_boot_lease_and_host_endpoint_infrastructure_only", "build": build,
            "host_endpoint": host_fixture(machines),
            "live_sessions": live_session_fixture(machines),
            "target_resolution": {"all_machines_resolved_before_state": True,
                                  "invalid_sibling_rejected_without_state": True},
            "artifact_pinning": {"all_pins_before_runtime_construction": True,
                                 "source_bundles_removed_before_boot": True,
                                 "recovery_without_catalog_or_source": True,
                                 "pin_replay_read_only": True},
            "controller": {"environment_scoped_serialization": True,
                           "fresh_preparation_and_attachment": True,
                           "stale_attachment_refused": True,
                           "recovery_preparation_read_only": True,
                           "recovery_attachment_without_catalog": True},
            "serial_logs": serial,
            "topology": topology, "machines": machines,
            "storage": {"first": storage, "reopened": copy.deepcopy(storage),
                        "pin_snapshots": {"before_replay": copy.deepcopy(storage["installed_artifacts"]),
                                          "after_replay": copy.deepcopy(storage["installed_artifacts"]),
                                          "recovered": copy.deepcopy(storage["installed_artifacts"])},
                        "docker_api_probe_outputs": outputs,
                        "docker_api_probe_sha256": build["docker_probe_sha256"], "same_named_docker_volumes_hold_distinct_values": True,
                        "sibling_rootfs_and_setup_sentinels_invisible": True, "developer_docker_state_survived_reopen": True,
                        "orphan_rootfs_cleanup_observed_on_reopen": True},
            "lease": lease, "claims": {"production_up": False, "native_macos_machine": False, "managed_docker_context_or_full_compatibility": False}}


class EvidenceTests(unittest.TestCase):
    def test_host_transport_rejects_cross_routing_and_missing_proof(self):
        baseline = fixture()
        for mutate in [
            lambda host: host["commands"]["read_b"].update(endpoint=host["commands"]["read_a"]["endpoint"]),
            lambda host: host["commands"]["info_b"].update(stdout=host["commands"]["info_a"]["stdout"]),
            lambda host: host["commands"]["stopped_a"].update(exit_code=0),
            lambda host: host["commands"]["surviving_b"].update(stdout=host["commands"]["info_a"]["stdout"]),
            lambda host: host["commands"]["stdin_eof"].update(stdout=host["commands"]["stdin_eof"]["stdout"][:-4]),
            lambda host: host["commands"]["stdin_eof"].update(input_bytes=1),
            lambda host: host["commands"]["memory_a"].update(stdout="max\n"),
            lambda host: host["commands"]["info_a"].update(stdout=json.dumps({"ID": "engine-a", "DefaultRuntime": "youki", "MemoryLimit": False})),
            lambda host: host["commands"]["info_a"].update(stdout=json.dumps({"ID": "engine-a", "DefaultRuntime": "youki", "MemoryLimit": True,
                "Runtimes": {"youki": {"status": {"org.opencontainers.runtime-spec.features": '{"linux":{"cgroup":{"v2":false}}}'}}}})),
            lambda host: host["commands"]["import_b"].update(input_sha256="f" * 64),
            lambda host: host["commands"]["read_b"].update(stdout="developer-a"),
            lambda host: host["commands"]["events_a"].update(stdout=""),
            lambda host: host["commands"]["info_a"].update(config="/Users/user/.docker"),
            lambda host: host.update(socket_modes=[0o666, 0o600]),
            lambda host: host.update(managed_contexts=True),
            lambda host: host.update(compose_buildx=True),
            lambda host: host.update(unrelated_file_preserved=False),
            lambda host: host.update(hardened_refusal=""),
            lambda host: host["shutdown"][0].update(active_connections=1),
            lambda host: host["shutdown"][1].update(accepted_connections=50),
            lambda host: host.update(time_namespaces=[]),
            lambda host: host["time_namespaces"][0].update(time_offsets_tested=True),
            lambda host: host["time_namespaces"][1].update(owner=host["time_namespaces"][0]["owner"]),
            lambda host: host.update(device_policies=[]),
            lambda host: host.update(seccomp_policies=[]),
            lambda host: host["seccomp_policies"][0]["matrix"]["custom_exec"]["commands"]["exec"].update(exit_code=0, stdout="/bin/busybox\n"),
            lambda host: host["device_policies"][0].update(numeric_errno_measured=True),
            lambda host: host["device_policies"][1].update(owner=host["device_policies"][0]["owner"]),
        ]:
            host = copy.deepcopy(baseline["host_endpoint"])
            mutate(host)
            with self.assertRaises(InvalidEvidence):
                validate_host_endpoint(host, baseline["machines"])

    def test_device_proof_cannot_reuse_sibling_or_time_container_with_consistent_raw_ids(self):
        baseline = fixture()
        for replacement in [baseline["host_endpoint"]["device_policies"][0]["matrix"]["default_policy"]["container_id"],
                            baseline["host_endpoint"]["time_namespaces"][1]["container_id"]]:
            host = copy.deepcopy(baseline["host_endpoint"])
            case = host["device_policies"][1]["matrix"]["default_policy"]
            original = case["container_id"]
            host["device_policies"][1]["matrix"]["default_policy"] = json.loads(json.dumps(case).replace(original, replacement))
            with self.assertRaises(InvalidEvidence):
                validate_host_endpoint(host, baseline["machines"])

    def test_seccomp_proof_rejects_reused_sibling_or_other_policy_container(self):
        baseline = fixture()
        for replacement in [baseline["host_endpoint"]["device_policies"][0]["matrix"]["default_policy"]["container_id"],
                            baseline["host_endpoint"]["time_namespaces"][0]["container_id"],
                            baseline["host_endpoint"]["seccomp_policies"][0]["matrix"]["custom_exec"]["container_id"]]:
            host = copy.deepcopy(baseline["host_endpoint"])
            case = host["seccomp_policies"][1]["matrix"]["custom_exec"]
            original = case["container_id"]
            host["seccomp_policies"][1]["matrix"]["custom_exec"] = json.loads(json.dumps(case).replace(original, replacement))
            with self.assertRaises(InvalidEvidence):
                validate_host_endpoint(host, baseline["machines"])

    def test_live_session_stop_rejects_unknown_absence_cross_routing_and_leaks(self):
        baseline = fixture()
        for mutate in [
            lambda value: value["receipts"][0].update(outcome="already_absent"),
            lambda value: value["receipts"][0].update(owner=value["receipts"][1]["owner"]),
            lambda value: value["receipts"][0].update(runtime_identity=value["receipts"][1]["runtime_identity"]),
            lambda value: value["receipts"][0].update(generation=3),
            lambda value: value["receipts"][0].update(operation_id="lop_other"),
            lambda value: value["receipts"][0]["endpoint"].update(active_connections=1),
            lambda value: value["receipts"][2].update(endpoint=value["receipts"][0]["endpoint"]),
            lambda value: value["commands"]["stopped_a"].update(exit_code=0),
            lambda value: value["commands"]["surviving_b"].update(stdout=value["commands"]["before_a"]["stdout"]),
            lambda value: value["commands"]["surviving_b"].update(endpoint=value["commands"]["before_a"]["endpoint"]),
            lambda value: value.update(restart_recovery=True),
            lambda value: value.update(public_stop=True),
        ]:
            value = copy.deepcopy(baseline["live_sessions"])
            mutate(value)
            with self.assertRaises(InvalidEvidence):
                validate_live_sessions(value, baseline["machines"])

    def test_live_session_docker_closure_rejects_missing_foreign_and_hardened_authority(self):
        baseline = fixture()
        for mutate in [
            lambda r: r[0].pop("docker_shutdown"), lambda r: r[0].update(docker_shutdown=None),
            lambda r: r[0]["docker_shutdown"].update(extra=True),
            lambda r: r[0]["docker_shutdown"].update(request_id="another-operation"),
            lambda r: r[0]["docker_shutdown"].update(data_device="/dev/vdb"),
            lambda r: r[0]["docker_shutdown"].update(data_mount="/foreign"),
            lambda r: r[2].update(docker_shutdown=copy.deepcopy(r[0]["docker_shutdown"])),
            lambda r: r[1].update(docker_shutdown=copy.deepcopy(r[0]["docker_shutdown"])),
        ]:
            value = copy.deepcopy(baseline["live_sessions"])
            mutate(value["receipts"])
            with self.subTest(mutation=mutate), self.assertRaises(InvalidEvidence):
                validate_live_sessions(value, baseline["machines"])

    def test_live_session_docker_closure_requires_strict_positive_boolean_boundaries(self):
        baseline = fixture()
        for field in ["supervisor_started", "dockerd_reaped", "containerd_reaped", "filesystem_synced", "filesystem_unmounted"]:
            for changed in [False, 1, "true", None]:
                value = copy.deepcopy(baseline["live_sessions"])
                value["receipts"][0]["docker_shutdown"][field] = changed
                with self.subTest(field=field, changed=changed), self.assertRaises(InvalidEvidence):
                    validate_live_sessions(value, baseline["machines"])
        for changed in [True, 0, None]:
            value = copy.deepcopy(baseline["live_sessions"])
            value["receipts"][0]["docker_shutdown"]["never_started_unmounted"] = changed
            with self.subTest(changed=changed), self.assertRaises(InvalidEvidence):
                validate_live_sessions(value, baseline["machines"])

    def test_live_session_docker_closure_rejects_corrupt_unjournaled_or_replaced_filesystem(self):
        baseline = fixture()
        for field, variants in {
            "filesystem_uuid": [None, "", "foreign", "00000000-0000-0000-0000-000000000000"],
            "filesystem_state": [None, "not clean", "clean with errors"],
            "filesystem_features": [None, [], "has_journal extent", ["has_journal"], ["extent"],
                ["has_journal", "extent", "needs_recovery"], ["has_journal", "extent", "extent"],
                ["has_journal", "extent", True], ["has_journal", "extent", "bad feature"]],
        }.items():
            for changed in variants:
                value = copy.deepcopy(baseline["live_sessions"])
                value["receipts"][0]["docker_shutdown"][field] = changed
                with self.subTest(field=field, changed=changed), self.assertRaises(InvalidEvidence):
                    validate_live_sessions(value, baseline["machines"])

    def test_domain_separated_identity_vectors(self):
        artifact = {"kernel_sha256": "6" * 64, "initramfs_sha256": "2" * 64,
                    "youki_sha256": "7" * 64, "version_sha256": "8" * 64}
        self.assertEqual(bundle_digest(artifact),
                         "sha256:c4cec0f78f143e6d0ad95ebc436ab2f55d0f0c643eebac18cfbda677db9926bd")
        self.assertEqual(configuration_digest({"a": 1, "z": "x"}),
                         "sha256:7ac986f44f58a11f84ce2a2f8c46e3d288ea5556e9fff125331d85d78831717b")

    def test_complete_fixture_passes(self):
        value = fixture()
        validate(value, copy.deepcopy(value["build"]))

    def test_probe_metadata_is_strictly_bound_to_raw_version_and_actual_digest(self):
        original = fixture()["storage"]["first"]["installed_artifacts"]["developer-a"]
        self.assertEqual(len(validate_probe_pin(original)), 5)
        for field, changed in [("schema_version", True), ("archive", "../foreign.tar"),
                               ("sha256", "f" * 64), ("busybox_version", "1.36.0"),
                               ("source_inventory_sha256", "G" * 64), ("marker_sha256", "0" * 64),
                               ("unexpected", True)]:
            pin = copy.deepcopy(original)
            version = json.loads(pin["version_json"])
            version["developer_probe"][field] = changed
            pin["version_json"] = json.dumps(version)
            pin["version_sha256"] = hashlib.sha256(pin["version_json"].encode()).hexdigest()
            with self.subTest(field=field), self.assertRaises(InvalidEvidence):
                validate_probe_pin(pin)
        pin = copy.deepcopy(original)
        pin["version_json"] += " "
        with self.assertRaises(InvalidEvidence):
            validate_probe_pin(pin)

    def test_hardened_and_legacy_probe_absence_is_not_optional_file_adoption(self):
        pin = copy.deepcopy(fixture()["storage"]["first"]["installed_artifacts"]["developer-a"])
        version = json.loads(pin["version_json"])
        version["profile"] = pin["profile"] = "container"
        pin["version_json"] = json.dumps(version)
        pin["version_sha256"] = hashlib.sha256(pin["version_json"].encode()).hexdigest()
        with self.assertRaises(InvalidEvidence):
            validate_probe_pin(pin)
        version.pop("developer_probe")
        pin["version_json"] = json.dumps(version)
        pin["version_sha256"] = hashlib.sha256(pin["version_json"].encode()).hexdigest()
        with self.assertRaises(InvalidEvidence):
            validate_probe_pin(pin)
        pin["developer_probe_sha256"] = None
        self.assertEqual(validate_probe_pin(pin), ["vmlinux", "initramfs.img", "youki", "version.json"])

    def test_missing_fields_and_extra_fields_fail_at_every_object(self):
        baseline = fixture()
        expected = copy.deepcopy(baseline["build"])

        def visit(value, trail):
            if isinstance(value, dict):
                for key in value:
                    mutated = copy.deepcopy(baseline)
                    cursor = mutated
                    for component in trail:
                        cursor = cursor[component]
                    del cursor[key]
                    with self.subTest(missing=trail + [key]), self.assertRaises((ValueError, KeyError, TypeError)):
                        validate(mutated, expected)
                    visit(value[key], trail + [key])
                mutated = copy.deepcopy(baseline)
                cursor = mutated
                for component in trail:
                    cursor = cursor[component]
                cursor["unexpected"] = True
                with self.subTest(extra=trail), self.assertRaises((ValueError, KeyError, TypeError)):
                    validate(mutated, expected)
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    visit(item, trail + [index])

        visit(baseline, [])

    def test_wrong_values_cannot_self_certify(self):
        cases = [
            (["schema_version"], True),
            (["claims", "production_up"], True),
            (["lease", "same_boot_request_replayed_identity"], 1),
            (["lease", "locked_reopen_store_acquisition_refused"], False),
            (["lease", "locked_reopen_store_acquisition_refused"], 0),
            (["topology", "creating_owned_read_only"], "true"),
            (["topology", "docker_capabilities_synthesized"], True),
            (["topology", "developer_ready_without_docker_conformance_rejected"], False),
            (["topology", "hardened_activation_published"], False),
            (["target_resolution", "all_machines_resolved_before_state"], False),
            (["target_resolution", "invalid_sibling_rejected_without_state"], 1),
            (["artifact_pinning", "all_pins_before_runtime_construction"], False),
            (["artifact_pinning", "source_bundles_removed_before_boot"], 1),
            (["artifact_pinning", "recovery_without_catalog_or_source"], False),
            (["artifact_pinning", "pin_replay_read_only"], False),
            (["controller", "fresh_preparation_and_attachment"], False),
            (["controller", "environment_scoped_serialization"], False),
            (["controller", "stale_attachment_refused"], 1),
            (["controller", "recovery_preparation_read_only"], False),
            (["controller", "recovery_attachment_without_catalog"], False),
            (["serial_logs", "regular_nonempty"], False),
            (["serial_logs", "first_boot", 0, "path"], "/private/tmp/foreign.log"),
            (["serial_logs", "second_boot", 0, "sha256"], "not-a-sha"),
            (["build", "test_binary_sha256"], "f" * 64),
            (["machines", "developer_a", "configuration_digest"], "sha256:" + "a" * 64),
            (["machines", "developer_a", "owner", "environment_id"], "env_foreign"),
            (["machines", "developer_b", "vm_reservation", "resource_id"], "foreign"),
            (["machines", "developer_a", "artifact", "bundle"], "/private/tmp/bundles/developer"),
            (["machines", "developer_a", "resolved_configuration", "resources", "memory_mb"], 1024),
            (["machines", "developer_a", "resolved_configuration", "artifact", "digest"], "sha256:" + "a" * 64),
            (["machines", "developer_a", "resolved_configuration", "machine", "target", "image"], "ubuntu"),
            (["storage", "first", "installed_artifacts", "developer-a", "dir"], "/private/tmp/foreign"),
            (["storage", "first", "installed_artifacts", "developer-a", "configuration_sha256"], "a" * 64),
            (["storage", "first", "installed_artifacts", "developer-a", "configuration_identity", "mode"], 0o600),
            (["storage", "first", "installed_artifacts", "developer-a", "pin_directory_identity", "mode"], 0o500),
            (["storage", "first", "installed_artifacts", "developer-a", "bundle_directory_identity", "mode"], 0o700),
            (["storage", "first", "installed_artifacts", "developer-b", "artifact_identities", "vmlinux", "inode"], 303),
            (["storage", "pin_snapshots", "after_replay", "developer-a", "configuration_identity", "ctime_nanoseconds"], 999),
            (["storage", "first", "data_roots", 0], "/private/tmp/foreign/data"),
            (["storage", "first", "data_root_identities", 0, "mode"], 0o777),
            (["storage", "first", "developer_docker_disk_identities", 1, "inode"], 200),
            (["storage", "first", "developer_docker_disk_identities", 1, "links"], 2),
            (["storage", "reopened", "data_root_identities", 0, "inode"], 999),
            (["storage", "docker_api_probe_outputs", "developer_b_create", "marker"], "developer-a"),
            (["storage", "docker_api_probe_outputs", "developer_a_create", "operation"], "verify"),
            (["storage", "docker_api_probe_outputs", "developer_a_create", "marker_sha256"], "a" * 64),
        ]
        for trail, replacement in cases:
            value = fixture()
            expected = copy.deepcopy(value["build"])
            cursor = value
            for component in trail[:-1]:
                cursor = cursor[component]
            cursor[trail[-1]] = replacement
            with self.subTest(trail=trail), self.assertRaises((ValueError, KeyError, TypeError)):
                validate(value, expected)

    def test_duplicate_json_key_is_rejected(self):
        with self.assertRaises(InvalidEvidence):
            json.loads('{"schema_version":1,"schema_version":1}', object_pairs_hook=unique_object)

    def test_pinned_artifact_permissions_are_exact(self):
        for artifact, replacement in [("youki", 0o400), ("youki", 0o700), ("vmlinux", 0o500),
                                      ("developer-probe-rootfs.tar", 0o500)]:
            value = fixture()
            expected = copy.deepcopy(value["build"])
            installed_sets = [value["storage"][phase]["installed_artifacts"]
                              for phase in ["first", "reopened"]]
            installed_sets.extend(value["storage"]["pin_snapshots"].values())
            for installed in installed_sets:
                installed["developer-a"]["artifact_identities"][artifact]["mode"] = replacement
            with self.subTest(artifact=artifact, replacement=replacement), self.assertRaises(InvalidEvidence):
                validate(value, expected)

    def test_raw_serial_files_require_matching_bytes_and_regular_nonempty_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            logs = root / "machine-registry-vm-serial"
            logs.mkdir()
            value = fixture()
            for phase in ["first_boot", "second_boot"]:
                for entry in value["serial_logs"][phase]:
                    target = logs / Path(entry["path"]).name
                    content = (phase + target.name).encode()
                    target.write_bytes(content)
                    entry.update(path=str(target), sha256=hashlib.sha256(content).hexdigest())
            evidence = root / "machine-runtime-registry.json"
            verify_serial_files(value, evidence)
            target = Path(value["serial_logs"]["first_boot"][0]["path"])
            target.write_bytes(b"tampered")
            with self.assertRaises(InvalidEvidence):
                verify_serial_files(value, evidence)
            target.write_bytes(b"")
            with self.assertRaises(InvalidEvidence):
                verify_serial_files(value, evidence)
            target.unlink()
            target.symlink_to(Path(value["serial_logs"]["first_boot"][1]["path"]))
            with self.assertRaises(OSError):
                verify_serial_files(value, evidence)


if __name__ == "__main__":
    unittest.main()
