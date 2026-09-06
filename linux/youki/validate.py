#!/usr/bin/env python3
"""Validate a source-built youki candidate before reuse or atomic installation."""
import hashlib
import json
import datetime
import re
from pathlib import Path
import stat
import struct
import sys

REQUIRED_TESTS = (
    "test_channel_time_offset_request", "test_channel_time_offsets_ack",
    "test_validate_spec_for_time_namespace_error", "test_validate_spec_for_time_namespace_success",
    "test_apply_namespaces", "test_bpf_load", "test_bpf_load_error", "test_bpf_attach",
    "test_bpf_query", "test_bpf_query_other_error", "test_apply_devices", "test_existing_programs",
    "test_devices_deny_all", "test_devices_allow_single", "test_devices_allow_and_deny",
)
REQUIRED_LOCAL_TESTS = (
    "test_vz_exec_seccomp_preserves_default_profile",
    "test_vz_exec_seccomp_preserves_custom_profile",
    "test_vz_exec_seccomp_preserves_absent_profile",
    "test_vz_seccomp_phase_respects_nnp_value",
    "test_vz_seccomp_non_notify_needs_no_container_state",
    "test_vz_seccomp_notify_uses_immutable_original_and_payload_pid",
)
REQUIRED_ROOT_TESTS = (
    "test_vz_tenant_root_pins_namespace_identity_and_cloexec",
    "test_vz_tenant_root_failure_stops_and_closes_handles",
    "test_vz_tenant_root_rejects_missing_process_and_invalid_fd_floor",
    "test_vz_tenant_root_refuses_exited_pinned_init",
)
REQUIRED_LOG_TESTS = (
    "test_vz_runtime_log_formatter_preserves_fields_and_escaping",
    "test_vz_runtime_log_file_captures_failure_chain",
    "test_vz_runtime_log_create_validation_failure",
    "test_vz_runtime_log_file_reopen_appends_complete_records",
    "test_vz_runtime_log_text_behavior_unchanged",
)
RUNTIME_LOG_MESSAGE = "error in executing command: container id can't be used to represent a file name (such as . or ..)"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_regular(path, limit=32 * 1024 * 1024, allow_empty=False):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, f"not a single-link regular file: {path}")
    require((allow_empty or info.st_size > 0) and info.st_size <= limit, f"invalid file size: {path}")
    return path.read_bytes()


def validate_elf(data):
    require(len(data) >= 64 and data[:7] == b"\x7fELF\x02\x01\x01", "expected ELF64 little-endian binary")
    require(struct.unpack_from("<H", data, 18)[0] == 183, "expected AArch64 ELF")
    offset = struct.unpack_from("<Q", data, 32)[0]
    entry_size, count = struct.unpack_from("<HH", data, 54)
    require(entry_size == 56 and 0 < count <= 1024, "invalid ELF program headers")
    require(offset + entry_size * count <= len(data), "truncated ELF program headers")
    for index in range(count):
        kind, _, start, _, _, size, _, _ = struct.unpack_from("<IIQQQQQQ", data, offset + index * entry_size)
        require(kind != 3, "youki requires a dynamic interpreter")
        if kind == 2:
            require(start + size <= len(data) and size % 16 == 0, "invalid ELF dynamic table")
            for position in range(start, start + size, 16):
                tag = struct.unpack_from("<q", data, position)[0]
                require(tag != 1, "youki has a dynamic NEEDED dependency")
                if tag == 0:
                    break


def validate(candidate, source):
    names = {"youki", "features.json", "elf.txt", "version.txt", "inputs.env", "apk.sha256", "source-lock.sha256", "cargo-features.txt", "upstream-tests.txt", "seccomp-exec.patch", "seccomp-exec-tests.txt", "tenant-root.patch", "tenant-root-tests.txt",
             "runtime-log.patch", "runtime-log-tests.txt", "runtime-log.json", "runtime-log-stdout.txt",
             "runtime-log-stderr.txt", "runtime-log-exit-status.txt"}
    require({path.name for path in candidate.iterdir()} == names | {"evidence.sha256"}, "unexpected candidate inventory")
    checksums = {}
    for line in read_regular(candidate / "evidence.sha256", 16384).decode().splitlines():
        digest, name = line.split()
        require(name in names and name not in checksums, f"unexpected/duplicate evidence file: {name}")
        require(len(digest) == 64 and all(c in "0123456789abcdef" for c in digest), "invalid checksum")
        checksums[name] = digest
    require(set(checksums) == names, "incomplete youki evidence manifest")
    contents = {name: read_regular(candidate / name, allow_empty=name in {"runtime-log-stdout.txt", "runtime-log-stderr.txt"}) for name in names}
    require(stat.S_IMODE((candidate / "youki").lstat().st_mode) == 0o755, "candidate youki must have mode 0755")
    for name, digest in checksums.items():
        require(hashlib.sha256(contents[name]).hexdigest() == digest, f"youki evidence mismatch: {name}")
    for name in ("inputs.env", "apk.sha256", "seccomp-exec.patch", "tenant-root.patch", "runtime-log.patch"):
        require(contents[name] == read_regular(source / name), f"stale build input: {name}")
    inputs = dict(line.split("=", 1) for line in contents["inputs.env"].decode().splitlines() if line and not line.startswith("#"))
    require(contents["source-lock.sha256"].decode().split()[0] == inputs["YOUKI_LOCK_SHA256"], "source Cargo.lock changed")
    require(inputs["YOUKI_FEATURES"] == "v2,cgroupsv2_devices,seccomp", "unexpected runtime feature selection")
    require(checksums["seccomp-exec.patch"] == inputs["YOUKI_PATCH_SHA256"], "pinned local seccomp patch mismatch")
    require(checksums["tenant-root.patch"] == inputs["YOUKI_ROOT_PATCH_SHA256"], "pinned local root patch mismatch")
    require(checksums["runtime-log.patch"] == inputs["YOUKI_LOG_PATCH_SHA256"], "pinned local runtime log patch mismatch")
    validate_runtime_log(contents)
    root_tests = contents["tenant-root-tests.txt"].decode()
    for test in REQUIRED_ROOT_TESTS:
        require(any(line.startswith("test ") and line.endswith("::" + test + " ... ok") for line in root_tests.splitlines()), f"missing passing local root regression: {test}")
    require("FAILED" not in root_tests and "test result: ok." in root_tests, "failed local root tests")
    local_tests = contents["seccomp-exec-tests.txt"].decode()
    for test in REQUIRED_LOCAL_TESTS:
        require(any(line.startswith("test ") and line.endswith("::" + test + " ... ok") for line in local_tests.splitlines()), f"missing passing local seccomp regression: {test}")
    require("FAILED" not in local_tests and "test result: ok." in local_tests, "failed local seccomp tests")
    tests = contents["upstream-tests.txt"].decode()
    for test in REQUIRED_TESTS:
        require(any(line.startswith("test ") and line.endswith("::" + test + " ... ok") for line in tests.splitlines()), f"missing passing upstream test: {test}")
    require("FAILED" not in tests and "test result: ok." in tests, "failed upstream tests")
    features = json.loads(contents["features.json"])
    cgroup = features["linux"]["cgroup"]
    require(cgroup.get("v2") is True and cgroup.get("v1") is False and cgroup.get("systemd") is False, "required cgroup runtime features not present")
    tree = contents["cargo-features.txt"].decode()
    require("libbpf-sys v1.7.0+v1.7.0" in tree and "libseccomp v0.4.0" in tree, "missing locked device-filter or seccomp dependencies")
    version = contents["version.txt"].decode().splitlines()
    require("youki version: " + inputs["YOUKI_VERSION"] in version, "wrong youki version")
    require("commit: " + inputs["YOUKI_VERSION"] + "-" + inputs["YOUKI_COMMIT"] + "+" + inputs["YOUKI_PATCH_ID"] + "+" + inputs["YOUKI_ROOT_PATCH_ID"] + "+" + inputs["YOUKI_LOG_PATCH_ID"] in version, "wrong youki commit or local patch identity")
    validate_elf(contents["youki"])
    return checksums["youki"]


def validate_runtime_log(contents):
    tests = contents["runtime-log-tests.txt"].decode()
    for test in REQUIRED_LOG_TESTS:
        require(any(line.startswith("test ") and line.endswith("::" + test + " ... ok") for line in tests.splitlines()),
                f"missing passing runtime log regression: {test}")
    require("FAILED" not in tests and "test result: ok." in tests, "failed runtime log tests")
    require(contents["runtime-log-exit-status.txt"] == b"1\n", "runtime failure probe did not exit exactly one")
    require(contents["runtime-log-stdout.txt"] == b"", "runtime failure probe emitted unexpected stdout")
    require(contents["runtime-log-stderr.txt"] == ("Error: " + RUNTIME_LOG_MESSAGE.removeprefix("error in executing command: ") + "\n").encode(),
            "runtime failure probe emitted unexpected stderr")
    raw = contents["runtime-log.json"]
    require(len(raw) <= 65536 and raw.endswith(b"\n") and len(raw.splitlines()) == 1,
            "runtime failure probe must contain one complete JSON record")
    def unique(pairs):
        row = {}
        for key, value in pairs:
            require(key not in row, "duplicate runtime log field")
            row[key] = value
        return row
    row = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    require(isinstance(row, dict) and row.get("level") == "error" and row.get("msg") == RUNTIME_LOG_MESSAGE,
            "runtime log does not preserve containerd-compatible error")
    require(set(row) == {"level", "msg", "time", "target", "fields"}
            and row["target"] == "youki" and row["fields"] == {"message": RUNTIME_LOG_MESSAGE}
            and isinstance(row.get("time"), str),
            "runtime log uses incompatible metadata")
    require(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", row["time"]) is not None,
            "runtime log time is not RFC3339")
    timestamp = datetime.datetime.fromisoformat(row["time"].replace("Z", "+00:00"))
    require(timestamp.tzinfo is not None, "runtime log time lacks timezone")
    require(timestamp != datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), "runtime log time is zero")


if __name__ == "__main__":
    try:
        print(validate(Path(sys.argv[1]), Path(sys.argv[2])))
    except (OSError, ValueError, KeyError, IndexError, struct.error) as error:
        raise SystemExit(f"youki candidate rejected: {error}") from error
