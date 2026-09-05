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


class InvalidEvidence(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def keys(value, expected):
    require(type(value) is dict and set(value) == set(expected), f"unexpected keys; expected {expected}")


def sha(value):
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), "invalid SHA-256")


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


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


def validate(value, expected):
    keys(value, ["schema_version", "scope", "build", "topology", "machines", "storage", "lease", "claims", "serial_logs"])
    require(type(value["schema_version"]) is int and value["schema_version"] == 1, "schema version")
    require(value["scope"] == "registry_and_boot_lease_infrastructure_only", "scope overclaim")
    require(expected["profile"] == "release", "physical registry gate requires release")
    build = value["build"]
    keys(build, expected.keys())
    require(build == expected, "build/probe evidence does not match the executed artifacts")
    for key, number in build.items():
        if key.endswith("sha256"):
            sha(number)
    require(build["docker_probe_go_version"].startswith("go version go"), "missing Go provenance")

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
    machine_ids, resources, incarnations = [], [], []
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
        path(artifact["bundle"])
        require(machine["verified_profile"] == profile and artifact["profile"] == profile, "profile mismatch")
        for field in artifact_fields[1:]:
            sha(artifact[field])
        expected_initramfs = build["developer_initramfs_sha256" if index < 2 else "container_initramfs_sha256"]
        require(artifact["initramfs_sha256"] == expected_initramfs, "wrong boot artifact")
        resolved = machine["resolved_configuration"]
        keys(resolved, ["backend", "target", "profile", "resources", "artifact", "network", "oci_runtime"])
        artifact_identity = {field: artifact[field] for field in artifact_fields}
        require(resolved["backend"] == "macos-vz" and resolved["profile"] == profile and resolved["oci_runtime"] == "youki",
                "wrong resolved backend/profile/runtime")
        require(resolved["network"] is True and resolved["artifact"] == artifact_identity, "resolved artifact mismatch")
        require(resolved["resources"] == {"cpus": 2, "memory_mb": 4096 if index < 2 else 1024}, "unproven boot resource selection")
        target = resolved["target"]
        keys(target, ["os", "arch", "image", "version", "channel", "digest"])
        require(target == {"os": "linux", "arch": "aarch64", "image": "local-vz-" + name.replace("_", "-"),
                           "version": "0.4.0-registry-e2e", "channel": "local-physical-e2e", "digest": digest(artifact_identity)},
                "target fixture does not match selected artifact")
        require(machine["configuration_digest"] == digest(resolved), "configuration digest mismatch")
        for phase in ["first_identity", "reopened_identity"]:
            identity = machine[phase]
            keys(identity, ["schema_version", "stack_id", "incarnation_id"])
            require(type(identity["schema_version"]) is int and identity["schema_version"] == 1, "boot identity schema")
            require(identity["stack_id"] == machine["vm_reservation"]["resource_id"], "boot identity owner mismatch")
            incarnation = uuid.UUID(identity["incarnation_id"])
            require(incarnation.version == 4 and str(incarnation) == identity["incarnation_id"], "invalid boot incarnation")
            incarnations.append(str(incarnation))
    require(len(set(machine_ids)) == 3 and len(set(resources)) == 6 and len(set(incarnations)) == 6, "duplicate owner/resource/boot")

    storage = value["storage"]
    storage_flags = ["same_named_docker_volumes_hold_distinct_values", "sibling_rootfs_and_setup_sentinels_invisible",
                     "developer_docker_state_survived_reopen", "orphan_rootfs_cleanup_observed_on_reopen"]
    keys(storage, ["first", "reopened", "docker_api_probe_sha256", "docker_api_probe_outputs"] + storage_flags)
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
        keys(section["installed_artifacts"], [name.replace("_", "-") for name in names])
        namespaces, root_inodes, disk_inodes, uids = [], [], [], []
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
            keys(installed, ["dir", "profile", "kernel_sha256", "initramfs_sha256", "youki_sha256", "version_sha256"])
            require(path(installed["dir"]) == root / "linux-install", "artifact install escaped private store")
            require(all(installed[field] == machine["artifact"][field] for field in installed if field != "dir"), "installed artifact differs from source")
            if index < 2:
                disk_key = hashlib.sha256(machine["vm_reservation"]["resource_id"].encode()).hexdigest()
                require(path(section["developer_docker_disks"][index]) == root / "docker-machines" / disk_key / "data.img", "Docker disk escaped Machine store")
                disk_metadata = section["developer_docker_disk_identities"][index]
                validate_file_identity(disk_metadata, 0o600, single_link=True)
                require(disk_metadata["uid"] == metadata["uid"], "Docker disk owner differs")
                disk_inodes.append((disk_metadata["device"], disk_metadata["inode"]))
        require(len(set(namespaces)) == 1 and len(set(root_inodes)) == 3 and len(set(disk_inodes)) == 2 and len(set(uids)) == 1,
                "shared storage inode or inconsistent namespace/owner")
    for field in ["data_roots", "developer_docker_disks", "installed_artifacts"]:
        require(storage["first"][field] == storage["reopened"][field], "persistent paths/artifacts changed on reopen")
    for field in ["data_root_identities", "developer_docker_disk_identities"]:
        for before, after in zip(storage["first"][field], storage["reopened"][field]):
            require(all(before[key] == after[key] for key in ["device", "inode", "mode", "uid"]), "persistent resource was replaced")

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
                   "activation_exec_after_registry_drop", "cold_reopen_new_identities", "old_identities_refused_as_replacements"]
    keys(value["lease"], lease_flags + ["locked_reopen_factory_invocations"])
    require(all(value["lease"][field] is True for field in lease_flags), "missing/failed lease proof")
    require(type(value["lease"]["locked_reopen_factory_invocations"]) is int and value["lease"]["locked_reopen_factory_invocations"] == 0,
            "backend constructor ran while leased")
    require(value["claims"] == {"production_up": False, "native_macos_machine": False, "host_docker_socket_or_context": False}
            and all(item is False for item in value["claims"].values()), "unsupported release claim")


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
