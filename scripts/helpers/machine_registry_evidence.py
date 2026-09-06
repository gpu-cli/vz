"""Strict validator for the focused Machine registry infrastructure evidence.

This does not certify the aggregate 0.4 release or host Docker compatibility.
"""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import uuid

from docker_time_namespace_evidence import (
    InvalidEvidence as InvalidTimeNamespaceEvidence,
    validate as validate_time_namespace,
)
from docker_device_policy_evidence import (
    InvalidEvidence as InvalidDevicePolicyEvidence,
    validate as validate_device_policy,
)
from docker_seccomp_policy_evidence import (
    InvalidEvidence as InvalidSeccompPolicyEvidence,
    validate as validate_seccomp_policy,
)

class InvalidEvidence(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def keys(value, expected):
    require(type(value) is dict and set(value) == set(expected), f"unexpected keys; expected {expected}")


def sha(value):
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), "invalid SHA-256")


def configuration_digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    value = hashlib.sha256(b"vz.machine-configuration.v1\0" + encoded).hexdigest()
    return "sha256:" + value


def bundle_digest(value):
    hasher = hashlib.sha256(b"vz.linux.kernel-bundle.v1\0")
    for name, key in [("kernel", "kernel_sha256"), ("initramfs", "initramfs_sha256"),
                      ("youki", "youki_sha256"), ("version", "version_sha256")]:
        sha(value[key])
        hasher.update(name.encode())
        hasher.update(b"\0")
        hasher.update(value[key].encode())
        hasher.update(b"\0")
    return "sha256:" + hasher.hexdigest()


def validate_probe_pin(installed):
    """Bind the exact optional file inventory to the retained version bytes."""
    raw = installed["version_json"]
    require(type(raw) is str and 0 < len(raw.encode()) <= 1024 * 1024, "bounded raw version metadata required")
    require(hashlib.sha256(raw.encode()).hexdigest() == installed["version_sha256"], "raw version bytes do not match artifact pin")
    version = json.loads(raw, object_pairs_hook=unique_object)
    require(type(version) is dict and version.get("profile") == installed["profile"], "version profile mismatch")
    for source, target in [("sha256_vmlinux", "kernel_sha256"), ("sha256_initramfs", "initramfs_sha256"), ("sha256_youki", "youki_sha256")]:
        require(version.get(source) == installed[target], "version artifact checksum mismatch")
    names = ["vmlinux", "initramfs.img", "youki", "version.json"]
    probe = version.get("developer_probe")
    if probe is None:
        require(installed["developer_probe_sha256"] is None, "undeclared startup archive")
        return names
    keys(probe, ["schema_version", "archive", "sha256", "busybox_sha256", "busybox_version",
                 "source_archive_sha256", "source_inventory_sha256", "build_provenance_sha256", "marker_sha256"])
    require(installed["profile"] == "developer" and type(probe["schema_version"]) is int
            and probe["schema_version"] == 1 and probe["archive"] == "developer-probe-rootfs.tar", "foreign probe profile/schema/path")
    for field in ["sha256", "busybox_sha256", "source_archive_sha256", "source_inventory_sha256", "build_provenance_sha256", "marker_sha256"]:
        sha(probe[field])
    require(type(probe["busybox_version"]) is str and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", probe["busybox_version"])
            and probe["busybox_version"] == version.get("busybox"), "probe BusyBox provenance mismatch")
    require(probe["marker_sha256"] == hashlib.sha256(b"vz-developer-probe-v1\n").hexdigest(), "probe marker substituted")
    require(installed["developer_probe_sha256"] == probe["sha256"], "probe bytes do not match declared digest")
    names.append(probe["archive"])
    return names


def path(value):
    require(type(value) is str, "path is not a string")
    result = PurePosixPath(value)
    require(result.is_absolute() and ".." not in result.parts and str(result) == value, "noncanonical path")
    return result


def resource_name(owner, kind, logical):
    """Independent implementation of the versioned 64-byte owner-name contract."""
    identity = "other:" + kind
    value = hashlib.sha256(b"vz.resource-name.v1\0")
    for field in [owner["project_id"], owner["environment_id"], owner["machine_id"], identity, logical]:
        encoded = field.encode()
        value.update(len(encoded).to_bytes(8, "little"))
        value.update(encoded)
    slug = re.sub(r"-+", "-", re.sub(r"[^A-Za-z0-9_.]", "-", identity + "-" + logical)).strip("-")
    return "vzr1-" + slug[:26] + "-" + value.hexdigest()[:32]


def validate_file_identity(value, mode, single_link=False):
    keys(value, ["device", "inode", "mode", "uid", "links", "size"])
    require(all(type(number) is int and number >= 0 for number in value.values()), "invalid file metadata")
    require(value["inode"] > 0 and value["links"] > 0 and value["mode"] == mode, "unsafe file identity/mode")
    if single_link:
        require(value["links"] == 1 and value["size"] == 64 * 1024**3, "Docker disk not single-link 64 GiB")


def validate_immutable_identity(value, mode, directory=False):
    keys(value, ["device", "inode", "mode", "uid", "links", "size", "mtime_seconds",
                 "mtime_nanoseconds", "ctime_seconds", "ctime_nanoseconds"])
    require(all(type(number) is int and number >= 0 for number in value.values()), "invalid immutable metadata")
    require(value["inode"] > 0 and value["mode"] == mode and value["links"] > 0,
            "unsafe immutable path identity/mode")
    require(0 <= value["mtime_nanoseconds"] < 1_000_000_000
            and 0 <= value["ctime_nanoseconds"] < 1_000_000_000, "invalid immutable timestamp")
    if directory:
        require(value["links"] >= 1, "invalid immutable directory")
    else:
        require(value["links"] == 1 and value["size"] > 0, "immutable pin file is empty or linked")


def validate(value, expected):
    keys(value, ["schema_version", "scope", "build", "target_resolution", "artifact_pinning", "controller", "topology", "machines", "storage", "lease", "claims", "serial_logs", "host_endpoint", "live_sessions"])
    require(type(value["schema_version"]) is int and value["schema_version"] == 1, "schema version")
    require(value["scope"] == "registry_boot_lease_and_host_endpoint_infrastructure_only", "scope overclaim")
    require(expected["profile"] == "release", "physical registry gate requires release")
    build = value["build"]
    keys(build, expected.keys())
    require(build == expected, "build/probe evidence does not match the executed artifacts")
    for key, number in build.items():
        if key.endswith("sha256"):
            sha(number)
    require(build["docker_probe_go_version"].startswith("go version go"), "missing Go provenance")

    target_resolution = value["target_resolution"]
    keys(target_resolution, ["all_machines_resolved_before_state", "invalid_sibling_rejected_without_state"])
    require(all(flag is True for flag in target_resolution.values()), "target resolution did not fail closed before state")
    artifact_pinning = value["artifact_pinning"]
    keys(artifact_pinning, ["all_pins_before_runtime_construction", "source_bundles_removed_before_boot",
                            "recovery_without_catalog_or_source", "pin_replay_read_only"])
    require(all(flag is True for flag in artifact_pinning.values()), "artifact pin admission/recovery proof failed")
    controller = value["controller"]
    keys(controller, ["environment_scoped_serialization", "fresh_preparation_and_attachment", "stale_attachment_refused",
                      "recovery_preparation_read_only", "recovery_attachment_without_catalog"])
    require(all(flag is True for flag in controller.values()), "Environment controller proof failed")

    topology = value["topology"]
    read_flags = ["creating_owned_read_only", "failed_up_owned_read_only", "stopped_owned_read_only",
                  "reopened_stopped_owned_read_only", "final_stopped_owned_read_only"]
    capability_flags = ["developer_ready_without_docker_conformance_rejected", "hardened_activation_published"]
    keys(topology, ["project_id", "environment_id", "attempted_activation_capabilities", "docker_capabilities_synthesized"] + read_flags + capability_flags)
    require(all(topology[name] is True for name in read_flags), "ownership read validation failed")
    require(all(topology[name] is True for name in capability_flags), "activation capability guard failed")
    require(topology["attempted_activation_capabilities"] == ["posix_exec"] and topology["docker_capabilities_synthesized"] is False,
            "unproven capabilities")
    for field in ["project_id", "environment_id"]:
        require(type(topology[field]) is str and topology[field].strip(), "empty topology identity")

    names = ["developer_a", "developer_b", "hardened"]
    keys(value["machines"], names)
    machine_ids, resources, incarnations, source_bundles = [], [], [], []
    for index, name in enumerate(names):
        machine = value["machines"][name]
        keys(machine, ["owner", "store_reservation", "vm_reservation", "configuration_digest", "resolved_configuration",
                       "verified_profile", "artifact", "first_identity", "reopened_identity"])
        owner = machine["owner"]
        keys(owner, ["project_id", "environment_id", "machine_id"])
        require(all(owner[key] == topology[key] for key in ["project_id", "environment_id"]), "owner topology mismatch")
        require(type(owner["machine_id"]) is str and owner["machine_id"].strip(), "empty Machine identity")
        machine_ids.append(owner["machine_id"])
        for field, kind, logical in [("store_reservation", "machine_runtime_store", "runtime"), ("vm_reservation", "runtime_vm", "vm")]:
            record = machine[field]
            keys(record, ["schema_version", "resource_kind", "resource_id", "environment_id", "machine_id"])
            require(type(record["schema_version"]) is int and record["schema_version"] == 1, "reservation schema")
            require(record["resource_kind"] == {"other": kind}, "reservation kind")
            require(all(record[key] == owner[key] for key in ["environment_id", "machine_id"]), "reservation owner")
            require(record["resource_id"] == resource_name(owner, kind, logical), "resource ID not derived from exact owner")
            resources.append(record["resource_id"])
        profile = "developer" if index < 2 else "container"
        artifact = machine["artifact"]
        artifact_fields = ["profile", "kernel_sha256", "initramfs_sha256", "youki_sha256", "version_sha256"]
        keys(artifact, ["bundle"] + artifact_fields)
        source_bundles.append(path(artifact["bundle"]))
        require(machine["verified_profile"] == profile and artifact["profile"] == profile, "profile mismatch")
        for field in artifact_fields[1:]:
            sha(artifact[field])
        expected_initramfs = build["developer_initramfs_sha256" if index < 2 else "container_initramfs_sha256"]
        require(artifact["initramfs_sha256"] == expected_initramfs, "wrong boot artifact")
        resolved = machine["resolved_configuration"]
        keys(resolved, ["schema_version", "host", "backend", "machine", "release_version",
                        "kernel_profile", "artifact", "resources"])
        require(type(resolved["schema_version"]) is int and resolved["schema_version"] == 1,
                "resolved configuration schema")
        require(resolved["host"] == {"os": "macos", "arch": "aarch64"}
                and resolved["backend"] == "macos_virtualization_linux",
                "wrong resolved host/backend")
        expected_name = name.replace("_", "-")
        requested = resolved["machine"]
        keys(requested, ["schema_version", "name", "profile", "target", "resources", "requested_capabilities"])
        requested_profile = "developer" if index < 2 else "hardened"
        require(type(requested["schema_version"]) is int and requested["schema_version"] == 1,
                "Machine request schema")
        require(requested["name"] == expected_name and requested["profile"] == requested_profile,
                "wrong resolved Machine name/profile")
        require(requested["requested_capabilities"] == {"capabilities": ["posix_exec"]},
                "unproven requested capabilities")
        expected_resources = {"cpus": 2, "memory_mb": 4096 if index < 2 else 1024}
        require(requested["resources"] == expected_resources and resolved["resources"] == expected_resources,
                "unproven boot resource selection")
        require(resolved["release_version"] == "0.4.0-registry-e2e"
                and resolved["kernel_profile"] == profile,
                "wrong resolved release/kernel profile")
        resolved_artifact = resolved["artifact"]
        keys(resolved_artifact, artifact_fields[1:] + ["digest"])
        artifact_identity = {field: artifact[field] for field in artifact_fields[1:]}
        require(all(resolved_artifact[field] == artifact_identity[field] for field in artifact_identity),
                "resolved artifact bytes mismatch")
        expected_bundle_digest = bundle_digest(artifact_identity)
        require(resolved_artifact["digest"] == expected_bundle_digest,
                "resolved aggregate artifact digest mismatch")
        target = requested["target"]
        keys(target, ["os", "arch", "image", "version", "channel", "digest"])
        require(target == {"os": "linux", "arch": "aarch64", "image": "vz-linux-appliance",
                           "version": "0.4.0-registry-e2e", "channel": "local-physical-e2e",
                           "digest": expected_bundle_digest},
                "target fixture does not match selected artifact")
        require(machine["configuration_digest"] == configuration_digest(resolved),
                "configuration digest mismatch")
        for phase in ["first_identity", "reopened_identity"]:
            identity = machine[phase]
            keys(identity, ["schema_version", "stack_id", "incarnation_id"])
            require(type(identity["schema_version"]) is int and identity["schema_version"] == 1, "boot identity schema")
            require(identity["stack_id"] == machine["vm_reservation"]["resource_id"], "boot identity owner mismatch")
            incarnation = uuid.UUID(identity["incarnation_id"])
            require(incarnation.version == 4 and str(incarnation) == identity["incarnation_id"], "invalid boot incarnation")
            incarnations.append(str(incarnation))
    require(len(set(machine_ids)) == 3 and len(set(resources)) == 6 and len(set(incarnations)) == 6, "duplicate owner/resource/boot")
    require(source_bundles[0] == source_bundles[1]
            and source_bundles[0].name == "developer" and source_bundles[2].name == "hardened"
            and source_bundles[0].parent == source_bundles[2].parent
            and source_bundles[0].parent.name == "source-bundles",
            "resolver did not use the exact disposable fixture sources")

    storage = value["storage"]
    storage_flags = ["same_named_docker_volumes_hold_distinct_values", "sibling_rootfs_and_setup_sentinels_invisible",
                     "developer_docker_state_survived_reopen", "orphan_rootfs_cleanup_observed_on_reopen"]
    keys(storage, ["first", "reopened", "pin_snapshots", "docker_api_probe_sha256", "docker_api_probe_outputs"] + storage_flags)
    require(all(storage[field] is True for field in storage_flags), "storage isolation/persistence failure")
    require(storage["docker_api_probe_sha256"] == build["docker_probe_sha256"], "wrong executed probe")
    for phase in ["first", "reopened"]:
        section = storage[phase]
        keys(section, ["data_roots", "data_root_identities", "private_0700_and_distinct_inodes",
                       "all_writable_roots_below_machine_data", "installed_artifacts", "developer_docker_disks",
                       "developer_docker_disk_identities", "developer_docker_disks_distinct_inodes", "hardened_docker_state_absent"])
        for field in ["private_0700_and_distinct_inodes", "all_writable_roots_below_machine_data",
                      "developer_docker_disks_distinct_inodes", "hardened_docker_state_absent"]:
            require(section[field] is True, "unsafe/missing storage boundary")
        require(type(section["data_roots"]) is list and len(section["data_roots"]) == 3, "missing data roots")
        require(type(section["data_root_identities"]) is list and len(section["data_root_identities"]) == 3, "missing directory identities")
        require(type(section["developer_docker_disks"]) is list and len(section["developer_docker_disks"]) == 2, "missing Docker disks")
        require(type(section["developer_docker_disk_identities"]) is list and len(section["developer_docker_disk_identities"]) == 2, "missing disk identities")
        machine_names = [name.replace("_", "-") for name in names]
        keys(section["installed_artifacts"], machine_names)
        namespaces, root_inodes, disk_inodes, uids = [], [], [], []
        pin_inodes, artifact_inodes = [], []
        expected_artifact_count = 0
        for index, name in enumerate(names):
            machine = value["machines"][name]
            root = path(section["data_roots"][index])
            require(root.name == "data" and root.parent.name == machine["store_reservation"]["resource_id"]
                    and root.parent.parent.name == "topology-machines", "data path owner mismatch")
            namespaces.append(root.parent.parent)
            metadata = section["data_root_identities"][index]
            validate_file_identity(metadata, 0o700)
            root_inodes.append((metadata["device"], metadata["inode"]))
            uids.append(metadata["uid"])
            installed = section["installed_artifacts"][name.replace("_", "-")]
            keys(installed, ["pin_dir", "dir", "profile", "kernel_sha256", "initramfs_sha256",
                             "youki_sha256", "version_sha256", "configuration_path",
                             "configuration_sha256", "pin_directory_identity", "bundle_directory_identity",
                             "configuration_identity", "artifact_identities", "version_json", "developer_probe_sha256"])
            pin_dir = path(installed["pin_dir"])
            bundle_dir = path(installed["dir"])
            configuration_path = path(installed["configuration_path"])
            require(pin_dir == root / "linux-target" and bundle_dir == pin_dir / "bundle"
                    and configuration_path == pin_dir / "configuration.json", "artifact pin escaped private store")
            for field in ["profile", "kernel_sha256", "initramfs_sha256", "youki_sha256", "version_sha256"]:
                require(installed[field] == machine["artifact"][field], "installed artifact differs from source identity")
            sha(installed["configuration_sha256"])
            encoded_configuration = json.dumps(machine["resolved_configuration"], sort_keys=True,
                                               separators=(",", ":"), ensure_ascii=False).encode()
            require(installed["configuration_sha256"] == hashlib.sha256(encoded_configuration).hexdigest(),
                    "raw pinned configuration hash mismatch")
            validate_immutable_identity(installed["pin_directory_identity"], 0o700, directory=True)
            validate_immutable_identity(installed["bundle_directory_identity"], 0o500, directory=True)
            validate_immutable_identity(installed["configuration_identity"], 0o400)
            require(installed["configuration_identity"]["size"] == len(encoded_configuration),
                    "pinned configuration size mismatch")
            artifact_names = validate_probe_pin(installed)
            keys(installed["artifact_identities"], artifact_names)
            expected_artifact_count += len(artifact_names)
            require(installed["artifact_identities"]["version.json"]["size"] == len(installed["version_json"].encode()), "version file size mismatch")
            if installed["developer_probe_sha256"] is not None:
                require(0 < installed["artifact_identities"]["developer-probe-rootfs.tar"]["size"] <= 32 * 1024 * 1024, "unbounded probe archive")
            for artifact_name, identity in installed["artifact_identities"].items():
                validate_immutable_identity(identity, 0o500 if artifact_name == "youki" else 0o400)
                require(identity["uid"] == metadata["uid"], "pinned artifact owner differs")
                artifact_inodes.append((identity["device"], identity["inode"]))
            require(installed["pin_directory_identity"]["uid"] == metadata["uid"]
                    and installed["bundle_directory_identity"]["uid"] == metadata["uid"]
                    and installed["configuration_identity"]["uid"] == metadata["uid"],
                    "pin owner differs from Machine store")
            pin_inodes.extend([(installed["pin_directory_identity"]["device"], installed["pin_directory_identity"]["inode"]),
                               (installed["bundle_directory_identity"]["device"], installed["bundle_directory_identity"]["inode"]),
                               (installed["configuration_identity"]["device"], installed["configuration_identity"]["inode"])])
            if index < 2:
                disk_key = hashlib.sha256(machine["vm_reservation"]["resource_id"].encode()).hexdigest()
                require(path(section["developer_docker_disks"][index]) == root / "docker-machines" / disk_key / "data.img", "Docker disk escaped Machine store")
                disk_metadata = section["developer_docker_disk_identities"][index]
                validate_file_identity(disk_metadata, 0o600, single_link=True)
                require(disk_metadata["uid"] == metadata["uid"], "Docker disk owner differs")
                disk_inodes.append((disk_metadata["device"], disk_metadata["inode"]))
        require(len(set(namespaces)) == 1 and len(set(root_inodes)) == 3 and len(set(disk_inodes)) == 2
                and len(set(uids)) == 1 and len(set(pin_inodes)) == 9 and len(set(artifact_inodes)) == expected_artifact_count,
                "shared storage/pin inode or inconsistent namespace/owner")
    for field in ["data_roots", "developer_docker_disks", "installed_artifacts"]:
        require(storage["first"][field] == storage["reopened"][field], "persistent paths/artifacts changed on reopen")
    for field in ["data_root_identities", "developer_docker_disk_identities"]:
        for before, after in zip(storage["first"][field], storage["reopened"][field]):
            require(all(before[key] == after[key] for key in ["device", "inode", "mode", "uid"]), "persistent resource was replaced")
    snapshots = storage["pin_snapshots"]
    keys(snapshots, ["before_replay", "after_replay", "recovered"])
    expected_pins = storage["first"]["installed_artifacts"]
    for snapshot in snapshots.values():
        keys(snapshot, [name.replace("_", "-") for name in names])
        require(snapshot == expected_pins, "pin replay/recovery changed immutable files or metadata")
    require(storage["reopened"]["installed_artifacts"] == expected_pins,
            "reopened runtime did not use the original immutable pins")

    outputs = storage["docker_api_probe_outputs"]
    keys(outputs, [name + suffix for name in names[:2] for suffix in ["_create", "_first_verify", "_reopened_verify"]])
    for name, output in outputs.items():
        keys(output, ["operation", "name", "mountpoint", "marker", "marker_sha256", "api_owner"])
        marker = "developer-a" if name.startswith("developer_a_") else "developer-b"
        require(output == {"operation": "create" if name.endswith("_create") else "verify", "name": "vz-registry-shared",
                           "mountpoint": "/var/lib/docker/engine/volumes/vz-registry-shared/_data", "marker": marker,
                           "marker_sha256": hashlib.sha256(marker.encode()).hexdigest(), "api_owner": marker}, "Docker API owner/content evidence mismatch")
    serial = value["serial_logs"]
    keys(serial, ["first_boot", "second_boot", "regular_nonempty"])
    require(serial["regular_nonempty"] is True, "missing raw serial evidence")
    serial_paths = []
    for phase, suffix in [("first_boot", ".first-boot.log"), ("second_boot", ".log")]:
        require(type(serial[phase]) is list and len(serial[phase]) == 3, "missing Machine serial log")
        for name, entry in zip(names, serial[phase]):
            keys(entry, ["path", "sha256"])
            log_path = path(entry["path"])
            require(log_path.name == value["machines"][name]["vm_reservation"]["resource_id"] + suffix,
                    "serial log belongs to another Machine/boot")
            sha(entry["sha256"])
            serial_paths.append(log_path)
    require(len(set(serial_paths)) == 6 and len({item.parent for item in serial_paths}) == 1,
            "serial paths alias or escape one run")

    lease_flags = ["same_registry_admission_reused_arc", "same_boot_request_replayed_identity", "stale_generation_refused_before_boot",
                   "resource_drift_refused_without_replacement", "activation_retained_store_lock_after_registry_drop",
                   "activation_exec_after_registry_drop", "cold_reopen_new_identities", "old_identities_refused_as_replacements",
                   "locked_reopen_store_acquisition_refused"]
    keys(value["lease"], lease_flags)
    require(all(value["lease"][field] is True for field in lease_flags), "missing/failed lease proof")
    validate_host_endpoint(value["host_endpoint"], value["machines"])
    validate_live_sessions(value["live_sessions"], value["machines"])
    require(value["claims"] == {"production_up": False, "native_macos_machine": False, "managed_docker_context_or_full_compatibility": False}
            and all(item is False for item in value["claims"].values()), "unsupported release claim")


def validate_live_sessions(value, machines):
    keys(value, ["scope", "receipts", "commands", "sockets_removed", "restart_recovery", "public_stop"])
    require(value["scope"] == "registered_original_runtime_stop_only", "live-session scope overclaim")
    require(value["sockets_removed"] is True and value["restart_recovery"] is False
            and value["public_stop"] is False, "live-session cleanup or unsupported claim")
    require(type(value["receipts"]) is list and len(value["receipts"]) == 3, "missing live-session receipts")
    operations = set()
    filesystems = set()
    for name, receipt in zip(["developer_a", "developer_b", "hardened"], value["receipts"]):
        keys(receipt, ["owner", "operation_id", "generation", "runtime_identity", "endpoint", "outcome", "docker_shutdown"])
        require(receipt["owner"] == machines[name]["owner"]
                and receipt["runtime_identity"] == machines[name]["first_identity"], "wrong live-session identity")
        require(receipt["outcome"] == "stopped", "live-session physical stop was not proven")
        require(type(receipt["operation_id"]) is str and receipt["operation_id"]
                and type(receipt["generation"]) is int and receipt["generation"] == 2,
                "wrong live-session Stop operation generation")
        operations.add(receipt["operation_id"])
        if name == "hardened":
            require(receipt["endpoint"] is None and receipt["docker_shutdown"] is None,
                    "Hardened session acquired a Docker endpoint or shutdown authority")
        else:
            validate_endpoint_shutdown(receipt["endpoint"])
            filesystem = validate_docker_shutdown(receipt["docker_shutdown"], receipt["operation_id"])
            require(filesystem not in filesystems, "independent Developer Machines share filesystem identity")
            filesystems.add(filesystem)
    require(len(operations) == 1, "session receipts belong to different Stop operations")
    commands = value["commands"]
    keys(commands, ["before_a", "before_b", "stopped_a", "surviving_b"])
    engines, endpoints = {}, {}
    for name, command in commands.items():
        keys(command, ["args", "endpoint", "config", "exit_code", "stdout", "stderr",
                       "input_bytes", "input_sha256", "elapsed_ms"])
        require(command["args"] == ["info", "--format", "{{json .}}"], "session probe command changed")
        require(type(command["endpoint"]) is str and command["endpoint"].startswith("unix:///private/tmp/vz-ls-"),
                "session probe escaped its private endpoint")
        socket = path(command["endpoint"][7:])
        require(path(command["config"]) == socket.parent / "client", "session probe used ambient Docker config")
        target = "b" if name.endswith("_b") else "a"
        require(target not in endpoints or endpoints[target] == socket, "session probe cross-routed")
        endpoints[target] = socket
        require(type(command["exit_code"]) is int and
                (command["exit_code"] != 0 if name == "stopped_a" else command["exit_code"] == 0),
                "session stop/survivor command failed")
        require(type(command["elapsed_ms"]) is int and 0 <= command["elapsed_ms"] < 60_000,
                "session probe exceeded deadline")
        require(type(command["input_bytes"]) is int and command["input_bytes"] == 0
                and command["input_sha256"] == hashlib.sha256(b"").hexdigest(), "session probe input changed")
        require(type(command["stdout"]) is str and type(command["stderr"]) is str, "missing session raw output")
        if name != "stopped_a":
            info = json.loads(command["stdout"])
            require(type(info.get("ID")) is str and info["ID"] and info.get("DefaultRuntime") == "youki",
                    "session probe wrong Engine/runtime")
            engines[name] = info["ID"]
    require(endpoints["a"] != endpoints["b"] and endpoints["a"].parent == endpoints["b"].parent,
            "session endpoints alias or escape one root")
    require(engines["before_a"] != engines["before_b"] == engines["surviving_b"], "sibling Engine replaced or aliased")


def validate_docker_shutdown(receipt, operation_id):
    """The started Developer guests must close their own journaled data disk.

    A never-started receipt, daemon absence, clean-looking state without journal
    closure, or another operation's completion cannot authorize physical Stop.
    """
    keys(receipt, ["request_id", "data_device", "data_mount", "supervisor_started", "dockerd_reaped",
                   "containerd_reaped", "filesystem_synced", "filesystem_unmounted", "never_started_unmounted",
                   "filesystem_uuid", "filesystem_features", "filesystem_state"])
    require(receipt["request_id"] == operation_id and receipt["data_device"] == "/dev/vda" and
            receipt["data_mount"] == "/var/lib/docker", "Docker closure request/device/mount differs")
    require(all(receipt[field] is True for field in ["supervisor_started", "dockerd_reaped", "containerd_reaped",
                                                    "filesystem_synced", "filesystem_unmounted"]) and
            receipt["never_started_unmounted"] is False, "started Docker process/filesystem closure is unproven")
    filesystem = receipt["filesystem_uuid"]
    require(type(filesystem) is str and
            re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", filesystem) and
            filesystem != "00000000-0000-0000-0000-000000000000", "invalid closed Docker filesystem identity")
    features = receipt["filesystem_features"]
    require(type(features) is list and 2 <= len(features) <= 128 and
            all(type(feature) is str and re.fullmatch(r"[a-z0-9_]+", feature) for feature in features) and
            len(set(features)) == len(features) and {"has_journal", "extent"} <= set(features) and
            "needs_recovery" not in features and receipt["filesystem_state"] == "clean",
            "Docker filesystem lacks clean journaled ext4 closure")
    return filesystem


def validate_endpoint_shutdown(receipt):
    keys(receipt, ["accepted_connections", "completed_connections", "cancelled_connections",
                   "failed_connections", "active_connections", "socket_removed"])
    for field in ["accepted_connections", "completed_connections", "cancelled_connections",
                  "failed_connections", "active_connections"]:
        require(type(receipt[field]) is int and receipt[field] >= 0, "invalid relay accounting")
    require(receipt["accepted_connections"] > 0
            and receipt["accepted_connections"] == sum(receipt[field] for field in ["completed_connections", "cancelled_connections", "failed_connections"])
            and receipt["active_connections"] == 0 and receipt["socket_removed"] is True,
            "endpoint client/socket leaked or receipt inconsistent")


def validate_host_endpoint(value, machines):
    keys(value, ["scope", "client", "client_sha256", "busybox_sha256", "owners",
                 "runtime_identities", "commands", "socket_modes", "sockets_removed",
                 "unrelated_file_preserved", "managed_contexts", "compose_buildx", "hardened_refusal", "preexisting_path_refusal", "shutdown", "time_namespaces", "device_policies", "seccomp_policies"])
    require(value["scope"] == "focused_host_endpoint_transport_only", "host endpoint scope overclaim")
    require(value["client"] == "/usr/local/bin/docker", "host client substituted")
    sha(value["client_sha256"])
    sha(value["busybox_sha256"])
    require(value["owners"] == [machines[name]["owner"] for name in ["developer_a", "developer_b"]],
            "host endpoint wrong Machine owner")
    require(value["runtime_identities"] == [machines[name]["first_identity"] for name in ["developer_a", "developer_b"]],
            "host endpoint wrong boot identity")
    require(value["socket_modes"] == [0o600, 0o600]
            and all(type(mode) is int for mode in value["socket_modes"]), "host socket permissions")
    for field in ["sockets_removed", "unrelated_file_preserved"]:
        require(value[field] is True, "host endpoint cleanup/ownership failure")
    require(value["managed_contexts"] is False and value["compose_buildx"] is False,
            "unproven context/Compose/buildx claim")
    for field in ["hardened_refusal", "preexisting_path_refusal"]:
        require(type(value[field]) is str and value[field].strip(), "missing endpoint refusal")
    require(type(value["shutdown"]) is list and len(value["shutdown"]) == 2, "missing endpoint shutdown receipt")
    for receipt in value["shutdown"]:
        validate_endpoint_shutdown(receipt)
    commands = value["commands"]
    keys(commands, ["client_version", "info_a", "info_b", "import_a", "import_b",
                    "volume_a", "volume_b", "write_a", "write_b", "read_a", "read_b",
                    "memory_a", "memory_b", "stdin_eof", "events_a", "event_volume_a", "stopped_a", "surviving_b"])
    endpoints, configs = {}, set()
    no_input = hashlib.sha256(b"").hexdigest()
    for name, command in commands.items():
        keys(command, ["args", "endpoint", "config", "exit_code", "stdout", "stderr",
                       "input_bytes", "input_sha256", "elapsed_ms"])
        require(type(command["args"]) is list and command["args"]
                and all(type(arg) is str for arg in command["args"]), "invalid host command")
        require(type(command["endpoint"]) is str and command["endpoint"].startswith("unix:///private/tmp/vz-de-"),
                "host command endpoint escaped private fixture")
        socket = path(command["endpoint"][7:])
        require(socket.suffix == ".sock", "invalid host socket path")
        config = path(command["config"])
        require(config == socket.parent / "client", "host client configuration not isolated")
        configs.add(config)
        target = "b" if name.endswith("_b") else "a"
        if target in endpoints:
            require(endpoints[target] == socket, "host command cross-routed")
        endpoints[target] = socket
        require(type(command["exit_code"]) is int, "missing host command exit")
        require(command["exit_code"] != 0 if name == "stopped_a" else command["exit_code"] == 0,
                "host command unexpected exit status")
        require(all(type(command[field]) is str for field in ["stdout", "stderr"]), "missing raw host output")
        require(type(command["elapsed_ms"]) is int and 0 <= command["elapsed_ms"] < 60_000,
                "host command exceeded deadline")
        require(type(command["input_bytes"]) is int and command["input_bytes"] >= 0, "invalid host input length")
        sha(command["input_sha256"])
        if name not in ["import_a", "import_b", "stdin_eof"]:
            require(command["input_bytes"] == 0 and command["input_sha256"] == no_input,
                    "unexpected host command stdin")
    require(len(set(endpoints.values())) == 2 and len(configs) == 1, "host endpoints alias")
    require(type(value["time_namespaces"]) is list and len(value["time_namespaces"]) == 2,
            "missing per-Machine Docker time/exec proof")
    time_containers = []
    for index, name in enumerate(["a", "b"]):
        try:
            time_containers.append(validate_time_namespace(
                value["time_namespaces"][index], machines["developer_" + name],
                "unix://" + str(endpoints[name]), str(next(iter(configs)))))
        except InvalidTimeNamespaceEvidence as error:
            raise InvalidEvidence(str(error)) from error
    require(len(set(time_containers)) == 2, "Docker time proof reused a sibling container")
    require(type(value["device_policies"]) is list and len(value["device_policies"]) == 2,
            "missing per-Machine differential Docker device policy proof")
    device_containers = []
    for index, name in enumerate(["a", "b"]):
        try:
            device_containers.extend(validate_device_policy(
                value["device_policies"][index], machines["developer_" + name],
                "unix://" + str(endpoints[name]), str(next(iter(configs)))))
        except InvalidDevicePolicyEvidence as error:
            raise InvalidEvidence(str(error)) from error
    require(len(set(time_containers + device_containers)) == 6,
            "Docker device proof reused a sibling, paired or time-proof container")
    require(type(value["seccomp_policies"]) is list and len(value["seccomp_policies"]) == 2,
            "missing per-Machine default/custom init/exec seccomp proof")
    seccomp_containers = []
    for index, name in enumerate(["a", "b"]):
        try:
            seccomp_containers.extend(validate_seccomp_policy(
                value["seccomp_policies"][index], machines["developer_" + name],
                "unix://" + str(endpoints[name]), str(next(iter(configs)))))
        except InvalidSeccompPolicyEvidence as error:
            raise InvalidEvidence(str(error)) from error
    require(len(set(time_containers + device_containers + seccomp_containers)) == 14,
            "Docker seccomp proof reused a sibling or other policy container")
    require(commands["client_version"]["args"] == ["--version"]
            and commands["client_version"]["stdout"].startswith("Docker version "), "missing host version")
    engines = {}
    for name in ["info_a", "info_b", "surviving_b"]:
        require(commands[name]["args"] == ["info", "--format", "{{json .}}"], "not an Engine info command")
        info = json.loads(commands[name]["stdout"], object_pairs_hook=unique_object)
        require(type(info.get("ID")) is str and info["ID"].strip(), "missing Engine ID")
        require(info.get("DefaultRuntime") == "youki", "host Engine selected alternate OCI runtime")
        require(info.get("MemoryLimit") is True, "Developer Engine lacks memory limit support")
        features = json.loads(info["Runtimes"]["youki"]["status"]["org.opencontainers.runtime-spec.features"],
                              object_pairs_hook=unique_object)
        require(features["linux"]["cgroup"]["v2"] is True, "youki lacks compiled cgroup v2 support")
        engines[name] = info["ID"]
    require(engines["info_a"] != engines["info_b"] and engines["surviving_b"] == engines["info_b"],
            "Engine identity alias/replacement")
    require(commands["stopped_a"]["args"] == ["info"] and commands["stopped_a"]["stderr"].strip(),
            "missing stopped endpoint refusal")
    image = "vz-endpoint-fixture:local"
    run = ["run", "--rm", "--network", "none", "-v", "vz-endpoint-shared:/data", image, "/bin/busybox"]
    for name, marker in [("a", "developer-a"), ("b", "developer-b")]:
        imported = commands["import_" + name]
        require(imported["args"] == ["image", "import", "-", image], "fixture image not imported by host")
        require(re.fullmatch(r"sha256:[0-9a-f]{64}\s*", imported["stdout"]), "invalid imported image ID")
        require(imported["input_bytes"] > 0, "missing fixture rootfs bytes")
        volume = commands["volume_" + name]
        require(volume["args"] == ["volume", "create", "--label", "dev.vz.endpoint.owner=" + marker, "vz-endpoint-shared"]
                and volume["stdout"].strip() == "vz-endpoint-shared", "host volume owner mismatch")
        write = commands["write_" + name]
        require(write["args"] == run + ["sh", "-c", f"printf {marker} > /data/marker; /bin/busybox cat /data/marker"]
                and write["stdout"] == marker, "host volume write mismatch")
        read = commands["read_" + name]
        require(read["args"] == run + ["cat", "/data/marker"] and read["stdout"] == marker,
                "host volume isolation read mismatch")
        limited = commands["memory_" + name]
        require(limited["args"] == ["run", "--rm", "--network", "none", "--memory", "64m",
                                    image, "/bin/busybox", "cat", "/sys/fs/cgroup/memory.max"]
                and limited["stdout"].strip() == "67108864", "container memory limit not applied")
    require(all(commands["import_a"][field] == commands["import_b"][field]
                for field in ["input_bytes", "input_sha256"]), "fixture tar bytes differ between Machines")
    streamed = commands["stdin_eof"]
    require(streamed["args"] == ["run", "-i", "--rm", "--network", "none", image, "/bin/busybox", "sh", "-c",
                                  "/bin/busybox cat; /bin/busybox sleep 1; printf done"], "stdin EOF scenario substituted")
    payload = b"vz-endpoint-half-close\n" * 12_000
    require(streamed["input_bytes"] == len(payload)
            and streamed["input_sha256"] == hashlib.sha256(payload).hexdigest()
            and streamed["stdout"].encode() == payload + b"done", "host stream lost bytes/half-close/trailing output")
    events = commands["events_a"]
    args = events["args"]
    require(len(args) == 11 and args[:2] == ["events", "--since"] and args[3] == "--until"
            and args[5:] == ["--filter", "type=volume", "--filter", "event=create", "--format", "{{json .}}"]
            and args[2].isdigit() and args[4].isdigit() and int(args[4]) - int(args[2]) == 3,
            "host events window or command substituted")
    created = commands["event_volume_a"]
    require(created["args"] == ["volume", "create", "--label", "dev.vz.endpoint.owner=developer-a", "vz-endpoint-event"]
            and created["stdout"].strip() == "vz-endpoint-event", "missing host event stimulus")
    records = [json.loads(line, object_pairs_hook=unique_object) for line in events["stdout"].splitlines()]
    require(any(record.get("Type") == "volume" and record.get("Action") == "create"
                and record.get("Actor", {}).get("ID") == "vz-endpoint-event" for record in records),
            "host event stream missed exact volume create")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def verify_serial_files(value, evidence_file):
    expected_parent = Path(evidence_file).absolute().parent / "machine-registry-vm-serial"
    require(not expected_parent.is_symlink(), "serial directory is a symlink")
    for phase in ["first_boot", "second_boot"]:
        for entry in value["serial_logs"][phase]:
            log_path = Path(entry["path"])
            require(log_path.parent == expected_parent, "raw log escaped this evidence run")
            descriptor = os.open(log_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as stream:
                metadata = os.fstat(stream.fileno())
                require(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and metadata.st_size > 0,
                        "raw log is not a nonempty single-link regular file")
                checksum = hashlib.sha256()
                while chunk := stream.read(1024 * 1024):
                    checksum.update(chunk)
                require(checksum.hexdigest() == entry["sha256"], "raw serial log checksum mismatch")


def main(args):
    require(len(args) == 8, "expected evidence file and seven build provenance arguments")
    evidence_file, profile, binary, developer, hardened, probe, source, go_version = args
    with open(evidence_file, "rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    require(len(raw) <= 8 * 1024 * 1024, "evidence exceeds size limit")
    value = json.loads(raw, object_pairs_hook=unique_object)
    validate(value, {"profile": profile, "test_binary_sha256": binary, "developer_initramfs_sha256": developer,
                     "container_initramfs_sha256": hardened, "docker_probe_sha256": probe,
                     "docker_probe_source_sha256": source, "docker_probe_go_version": go_version})
    verify_serial_files(value, evidence_file)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except (ValueError, KeyError, TypeError, OSError, AttributeError) as error:
        print(f"Machine registry evidence invalid: {error}", file=sys.stderr)
        sys.exit(1)
