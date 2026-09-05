"""Positive and adversarial tests for the independent physical-lane validator."""

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from machine_registry_evidence import (
    InvalidEvidence,
    bundle_digest,
    configuration_digest,
    resource_name,
    unique_object,
    validate,
    verify_serial_files,
)


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
        artifact = {"profile": profile, "kernel_sha256": "6" * 64,
                    "initramfs_sha256": build["developer_initramfs_sha256" if index < 2 else "container_initramfs_sha256"],
                    "youki_sha256": "7" * 64, "version_sha256": "8" * 64}
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
            artifact_identities={file: immutable(303 + index * 10 + offset,
                                                 0o500 if file == "youki" else 0o400,
                                                 1024 + offset)
                                 for offset, file in enumerate(["vmlinux", "initramfs.img", "youki", "version.json"])},
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
    return {"schema_version": 1, "scope": "registry_and_boot_lease_infrastructure_only", "build": build,
            "target_resolution": {"all_machines_resolved_before_state": True,
                                  "invalid_sibling_rejected_without_state": True},
            "artifact_pinning": {"all_pins_before_runtime_construction": True,
                                 "source_bundles_removed_before_boot": True,
                                 "recovery_without_catalog_or_source": True,
                                 "pin_replay_read_only": True},
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
            "lease": lease, "claims": {"production_up": False, "native_macos_machine": False, "host_docker_socket_or_context": False}}


class EvidenceTests(unittest.TestCase):
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
        for artifact, replacement in [("youki", 0o400), ("youki", 0o700), ("vmlinux", 0o500)]:
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
