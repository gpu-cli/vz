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
REQUIRED_EXEC_TESTS = (
    "test_vz_executable_permissions_mode_matrix",
    "test_vz_executable_permissions_default_executor_mode_matrix",
    "test_vz_executable_permissions_reject_directory_and_missing",
    "test_vz_executable_permissions_owner_exec_kernel_boundary",
)
EXEC_PROBE_PREFIX = "VZ_EXECUTABLE_PERMISSIONS_PROBE="
REQUIRED_CGROUP_TESTS = tuple("test_vz_tenant_cgroup_" + name for name in (
    "parser_and_owned_subtree", "routing_preserves_explicit_and_nonfilesystem",
    "base_success_and_only_ebusy_fallback", "path_replacement_and_symlinks",
    "membership_change_and_dead_proc", "descriptors_preserved_and_closed",
))
REQUIRED_KEEP_TESTS = tuple("test_vz_run_keep_" + name for name in (
    "state_and_exit_codes", "default_cleanup_and_error_order", "explicit_normal_and_force_delete",
    "wait_error_retains_state", "save_error_retains_ownership", "invalid_run_has_no_state",
    "reaped_payload_lifecycle",
))
REQUIRED_WAIT_TESTS = tuple("test_vz_foreground_wait_" + name for name in (
    "already_exited_nonzero", "already_exited_zero", "already_signaled", "forwards_pending_signal",
))
WAIT_PROBE_PREFIX = "VZ_FOREGROUND_WAIT_PROBE="
REQUIRED_CONSOLE_TESTS = tuple("test_vz_console_size_" + name for name in (
    "real_pty_dimensions", "none_and_zero_semantics", "overflow_rejected", "spec_and_callsite_routing",
))


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
             "runtime-log-stderr.txt", "runtime-log-exit-status.txt",
             "executable-permissions.patch", "executable-permissions-tests.txt",
             "tenant-cgroup.patch", "tenant-cgroup-tests.txt", "run-keep.patch", "run-keep-tests.txt",
             "foreground-wait.patch", "foreground-wait-tests.txt", "console-size.patch", "console-size-tests.txt"}
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
    for name in ("inputs.env", "apk.sha256", "seccomp-exec.patch", "tenant-root.patch", "runtime-log.patch", "executable-permissions.patch", "tenant-cgroup.patch", "run-keep.patch", "foreground-wait.patch", "console-size.patch"):
        require(contents[name] == read_regular(source / name), f"stale build input: {name}")
    inputs = dict(line.split("=", 1) for line in contents["inputs.env"].decode().splitlines() if line and not line.startswith("#"))
    require(contents["source-lock.sha256"].decode().split()[0] == inputs["YOUKI_LOCK_SHA256"], "source Cargo.lock changed")
    require(inputs["YOUKI_FEATURES"] == "v2,cgroupsv2_devices,seccomp", "unexpected runtime feature selection")
    require(checksums["seccomp-exec.patch"] == inputs["YOUKI_PATCH_SHA256"], "pinned local seccomp patch mismatch")
    require(checksums["tenant-root.patch"] == inputs["YOUKI_ROOT_PATCH_SHA256"], "pinned local root patch mismatch")
    require(checksums["runtime-log.patch"] == inputs["YOUKI_LOG_PATCH_SHA256"], "pinned local runtime log patch mismatch")
    validate_runtime_log(contents)
    require(checksums["executable-permissions.patch"] == inputs["YOUKI_EXEC_PATCH_SHA256"], "pinned local executable patch mismatch")
    validate_executable_permissions(contents)
    require(checksums["tenant-cgroup.patch"] == inputs["YOUKI_CGROUP_PATCH_SHA256"], "pinned local tenant cgroup patch mismatch")
    require(checksums["run-keep.patch"] == inputs["YOUKI_KEEP_PATCH_SHA256"], "pinned local run keep patch mismatch")
    require(checksums["foreground-wait.patch"] == inputs["YOUKI_WAIT_PATCH_SHA256"], "pinned local foreground wait patch mismatch")
    validate_foreground_wait(contents["foreground-wait-tests.txt"])
    require(checksums["console-size.patch"] == inputs["YOUKI_CONSOLE_PATCH_SHA256"], "pinned local console size patch mismatch")
    validate_console_size(contents["console-size-tests.txt"])
    keep_tests = contents["run-keep-tests.txt"].decode()
    expected_keep = {"test commands::run::keep_tests::" + test + " ... ok" for test in REQUIRED_KEEP_TESTS}
    actual_keep = [line for line in keep_tests.splitlines() if line.startswith("test ") and not line.startswith("test result:")]
    keep_summaries = [line for line in keep_tests.splitlines() if line.startswith("test result:")]
    require(len(actual_keep) == len(expected_keep) and set(actual_keep) == expected_keep and
            "FAILED" not in keep_tests and len(keep_summaries) == 1 and re.fullmatch(
                r"test result: ok\. 7 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [0-9.]+s",
                keep_summaries[0]), "missing or failed run keep regressions")
    cgroup_tests = contents["tenant-cgroup-tests.txt"].decode()
    expected_tests = {"test container::tenant_cgroup::tests::" + test + " ... ok" for test in REQUIRED_CGROUP_TESTS}
    actual_tests = [line for line in cgroup_tests.splitlines() if line.startswith("test ") and not line.startswith("test result:")]
    summaries = [line for line in cgroup_tests.splitlines() if line.startswith("test result:")]
    require(len(actual_tests) == len(expected_tests) and set(actual_tests) == expected_tests and
            "FAILED" not in cgroup_tests and len(summaries) == 1 and re.fullmatch(
                r"test result: ok\. 6 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [0-9.]+s",
                summaries[0]),
            "missing or failed tenant cgroup regressions")
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
    require("commit: " + inputs["YOUKI_VERSION"] + "-" + inputs["YOUKI_COMMIT"] + "+" + inputs["YOUKI_PATCH_ID"] + "+" + inputs["YOUKI_ROOT_PATCH_ID"] + "+" + inputs["YOUKI_LOG_PATCH_ID"] + "+" + inputs["YOUKI_EXEC_PATCH_ID"] + "+" + inputs["YOUKI_CGROUP_PATCH_ID"] + "+" + inputs["YOUKI_KEEP_PATCH_ID"] + "+" + inputs["YOUKI_WAIT_PATCH_ID"] + "+" + inputs["YOUKI_CONSOLE_PATCH_ID"] in version, "wrong youki commit or local patch identity")
    validate_elf(contents["youki"])
    return checksums["youki"]


def validate_console_size(raw):
    tests = raw.decode("utf-8")
    actual = [line for line in tests.splitlines() if line.startswith("test ") and not line.startswith("test result:")]
    expected = {"test tty::tests::" + name + " ... ok" for name in REQUIRED_CONSOLE_TESTS}
    summaries = [line for line in tests.splitlines() if line.startswith("test result:")]
    require(len(actual) == len(expected) and set(actual) == expected and "FAILED" not in tests and
            len(summaries) == 1 and re.fullmatch(
                r"test result: ok\. 4 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [0-9.]+s",
                summaries[0]), "missing or failed console size regressions")


def validate_foreground_wait(raw):
    tests = raw.decode("utf-8")
    actual = [line for line in tests.splitlines() if line.startswith("test ") and not line.startswith("test result:")]
    require(len(actual) == 4 and all(
        len(re.findall(r"(?m)^test commands::run::foreground_wait_tests::" + name + r" \.\.\. (?:ok$|$)", tests)) == 1
        for name in REQUIRED_WAIT_TESTS), "missing or duplicate foreground wait regressions")
    summaries = [line for line in tests.splitlines() if line.startswith("test result:")]
    require("FAILED" not in tests and len(summaries) == 1 and re.fullmatch(
        r"test result: ok\. 4 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in [0-9.]+s",
        summaries[0]), "failed foreground wait regressions")
    def unique(pairs):
        row = {}
        for key, value in pairs:
            require(key not in row, "duplicate foreground wait proof field")
            row[key] = value
        return row
    rows = [json.loads(line[len(WAIT_PROBE_PREFIX):], object_pairs_hook=unique)
            for line in tests.splitlines() if line.startswith(WAIT_PROBE_PREFIX)]
    require(len(rows) == 4, "missing or duplicate foreground wait proofs")
    for row, case, code in zip(rows, ("nonzero", "zero", "signaled", "forwarded"), (37, 0, 15, 15)):
        require(type(row) is dict and set(row) == {"schema_version", "case", "exit_code", "already_waitable",
                "pending_sigchld", "reaped", "unrelated_reaped", "forwarded_sigterm"}, "wrong foreground wait proof schema")
        require(type(row["schema_version"]) is int and row["schema_version"] == 1 and
                type(row["exit_code"]) is int and row["exit_code"] == code and row["case"] == case and
                row["already_waitable"] is (case != "forwarded") and row["pending_sigchld"] is False and
                row["reaped"] is True and row["unrelated_reaped"] is True and
                row["forwarded_sigterm"] is (case == "forwarded"), "wrong actual foreground wait proof")


def validate_executable_permissions(contents):
    tests = contents["executable-permissions-tests.txt"].decode("utf-8")
    for test in REQUIRED_EXEC_TESTS:
        require(len(re.findall(r"(?m)^test workload::default::tests::" + test + r" \.\.\. (?:ok$|$)", tests)) == 1,
                f"missing passing executable regression: {test}")
    require(len(re.findall(r"(?m)^test workload::", tests)) == 4 and "FAILED" not in tests
            and "test result: ok. 4 passed; 0 failed; 0 ignored;" in tests, "failed executable tests")
    def unique(pairs):
        row = {}
        for key, value in pairs:
            require(key not in row, "duplicate executable probe field")
            row[key] = value
        return row
    rows = [json.loads(line[len(EXEC_PROBE_PREFIX):], object_pairs_hook=unique)
            for line in tests.splitlines() if line.startswith(EXEC_PROBE_PREFIX)]
    require(len(rows) == 2, "expected two actual executable probes")
    scaffold = "\nrunning 1 test\ntest workload::default::tests::" + REQUIRED_EXEC_TESTS[-1] + " ... \n"
    for row, case, mode, code, marker, opposite in zip(
            rows, ("owner-exec", "denied"), ("0700", "0600"), (37, 0),
            ("vz-owner-exec-0700", "vz-kernel-denied-0600"), ("vz-kernel-denied-0600", "vz-owner-exec-0700")):
        require(isinstance(row, dict) and set(row) == {"schema_version", "case", "mode", "uid", "gid", "exit_code", "stdout", "stderr"},
                "wrong executable probe schema")
        require(all(type(row[key]) is int for key in ("schema_version", "uid", "gid", "exit_code"))
                and (row["schema_version"], row["case"], row["mode"], row["uid"], row["gid"], row["exit_code"])
                == (1, case, mode, 0, 0, code), "wrong executable probe outcome")
        stdout = row["stdout"]
        require(isinstance(stdout, str) and len(stdout.encode()) <= 16384 and row["stderr"] == ""
                and stdout.startswith(scaffold), "unexpected executable probe streams")
        payload = stdout[len(scaffold):]
        if case == "owner-exec":
            require(payload == marker + "\n", "owner-only executable did not produce exact output")
        else:
            lines = payload.splitlines()
            require(len(lines) == 2 and lines[0].startswith("VZ_EXECUTABLE_PERMISSIONS_KERNEL_ERROR=error 'EACCES: Permission denied' executing ")
                    and lines[1] == marker and payload.endswith("\n"), "missing actual kernel execution denial")
        require(opposite not in stdout.splitlines(), "executable probe contains opposite outcome")


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
