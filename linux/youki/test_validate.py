"""Offline negative tests; real OCI behavior still requires the local VM gate."""
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest

from validate import AUDIT_NATIVE_CASES, AUDIT_NATIVE_CID, AUDIT_NATIVE_FILES, AUDIT_NATIVE_SESSION, EXEC_PROBE_PREFIX, REQUIRED_AUDIT_TESTS, REQUIRED_CGROUP_TESTS, REQUIRED_CONSOLE_TESTS, REQUIRED_EXEC_ERROR_TESTS, REQUIRED_EXEC_TESTS, REQUIRED_KEEP_TESTS, REQUIRED_LOCAL_TESTS, REQUIRED_LOG_TESTS, REQUIRED_ROOT_TESTS, REQUIRED_TESTS, REQUIRED_WAIT_TESTS, RUNTIME_LOG_MESSAGE, WAIT_PROBE_PREFIX, audit_native_argv, validate, validate_elf


def elf(kind=1, dynamic_tag=0):
    data = bytearray(136)
    data[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<H", data, 18, 183)
    struct.pack_into("<Q", data, 32, 64)
    struct.pack_into("<HH", data, 54, 56, 1)
    struct.pack_into("<IIQQQQQQ", data, 64, kind, 0, 120, 0, 0, 16, 16, 8)
    struct.pack_into("<qQ", data, 120, dynamic_tag, 0)
    return bytes(data)


def executable_probes():
    scaffold = "\nrunning 1 test\ntest workload::default::tests::" + REQUIRED_EXEC_TESTS[-1] + " ... \n"
    return [
        {"schema_version": 1, "case": "owner-exec", "mode": "0700", "uid": 0, "gid": 0, "exit_code": 37,
         "stdout": scaffold + "vz-owner-exec-0700\n", "stderr": ""},
        {"schema_version": 1, "case": "denied", "mode": "0600", "uid": 0, "gid": 0, "exit_code": 0,
         "stdout": scaffold + "VZ_EXECUTABLE_PERMISSIONS_KERNEL_ERROR=error 'EACCES: Permission denied' executing '/fixture/busybox' with args 'printf vz-owner-exec-0700'\nvz-kernel-denied-0600\n", "stderr": ""},
    ]


def executable_tests(rows):
    return ("\n".join("test workload::default::tests::" + name + " ... ok" for name in REQUIRED_EXEC_TESTS)
            + "\n" + "\n".join(EXEC_PROBE_PREFIX + json.dumps(row) for row in rows)
            + "\ntest result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 261 filtered out;\n").encode()


def wait_probes():
    return [{"schema_version": 1, "case": case, "exit_code": code, "already_waitable": case != "forwarded",
             "pending_sigchld": False, "reaped": True, "unrelated_reaped": True,
             "forwarded_sigterm": case == "forwarded"}
            for case, code in (("nonzero", 37), ("zero", 0), ("signaled", 15), ("forwarded", 15))]


def wait_tests(rows):
    return ("\n".join("test commands::run::foreground_wait_tests::" + name + " ... ok" for name in REQUIRED_WAIT_TESTS)
            + "\n" + "\n".join(WAIT_PROBE_PREFIX + json.dumps(row) for row in rows)
            + "\ntest result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n").encode()


def audit_native_files(source, version):
    boot = "12345678-1234-1234-1234-123456789abc"
    records = []
    for index, (case, outcome, code) in enumerate(AUDIT_NATIVE_CASES):
        for event_index, event in enumerate(("begin", "result")):
            records.append({"schema_version": 1, "sequence": index * 2 + event_index + 1, "event": event,
                "session_id": AUDIT_NATIVE_SESSION, "boot_id": boot,
                "invocation_id": f"{100 + index}:123:{1000 + index * 2}", "operation": case,
                "container_id": None if case == "version" else AUDIT_NATIVE_CID,
                "pid": 100 + index, "starttime_ticks": 123, "monotonic_ns": 1000 + index * 2 + event_index,
                "wall_time_ns": 2000 + index * 2 + event_index,
                "outcome": outcome if event == "result" else None,
                "exit_code": code if event == "result" else None})
    metadata = "".join("/var/lib/docker/runtime-audit" + suffix +
        ("|0|0|700|2" if index == 0 else "|0|0|600|1") + f"|41|{100 + index}\n"
        for index, suffix in enumerate(("", "/enrollment.json", "/events.jsonl", "/status"))).encode()
    result = {"runtime-audit-parser.py": (source / "../../scripts/helpers/linux_docker_runtime_audit.py").read_bytes(),
              "runtime-audit-probe.sh": (source / "runtime-audit-probe.sh").read_bytes(),
              "runtime-audit-boot-id.txt": (boot + "\n").encode(),
              "runtime-audit-metadata-before.txt": metadata, "runtime-audit-metadata-after.txt": metadata,
              "runtime-audit-enrollment.json": json.dumps({"schema_version": 1, "session_id": AUDIT_NATIVE_SESSION,
                                                           "boot_id": boot}).encode(),
              "runtime-audit-events.jsonl": b"".join(json.dumps(record).encode() + b"\n" for record in records),
              "runtime-audit-status.txt": b"complete\n"}
    for case, _, code in AUDIT_NATIVE_CASES:
        result["runtime-audit-" + case + ".argv"] = audit_native_argv(case)
        result["runtime-audit-" + case + ".stdout"] = version if case == "version" else b""
        result["runtime-audit-" + case + ".stderr"] = (
            b"ERROR youki: error in executing command: oci spec error\nError: oci spec error\n\nCaused by:\n"
            b"    0: io operation failed\n    1: No such file or directory (os error 2)\n" if case == "create" else b"")
        result["runtime-audit-" + case + ".exit-status.txt"] = (str(code) + "\n").encode()
    return result


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = Path(__file__).resolve().parent
        inputs = dict(line.split("=", 1) for line in (self.source / "inputs.env").read_text().splitlines() if line and not line.startswith("#"))
        self.files = {
            "youki": elf(),
            "features.json": json.dumps({"linux": {"cgroup": {"v2": True, "v1": False, "systemd": False}}}).encode(),
            "elf.txt": b"ELF64 AArch64",
            "version.txt": ("youki version: " + inputs["YOUKI_VERSION"] + "\ncommit: " + inputs["YOUKI_VERSION"] + "-" + inputs["YOUKI_COMMIT"] + "+" + inputs["YOUKI_PATCH_ID"] + "+" + inputs["YOUKI_ROOT_PATCH_ID"] + "+" + inputs["YOUKI_LOG_PATCH_ID"] + "+" + inputs["YOUKI_EXEC_PATCH_ID"] + "+" + inputs["YOUKI_CGROUP_PATCH_ID"] + "+" + inputs["YOUKI_KEEP_PATCH_ID"] + "+" + inputs["YOUKI_WAIT_PATCH_ID"] + "+" + inputs["YOUKI_CONSOLE_PATCH_ID"] + "+" + inputs["YOUKI_EXEC_ERROR_PATCH_ID"] + "+" + inputs["YOUKI_AUDIT_PATCH_ID"] + "\n").encode(),
            "inputs.env": (self.source / "inputs.env").read_bytes(),
            "apk.sha256": (self.source / "apk.sha256").read_bytes(),
            "source-lock.sha256": (inputs["YOUKI_LOCK_SHA256"] + "  Cargo.lock\n").encode(),
            "cargo-features.txt": b"libbpf-sys v1.7.0+v1.7.0\nlibseccomp v0.4.0\n",
            "upstream-tests.txt": ("\n".join("test fixture::" + name + " ... ok" for name in REQUIRED_TESTS) + "\ntest result: ok.\n").encode(),
            "seccomp-exec.patch": (self.source / "seccomp-exec.patch").read_bytes(),
            "seccomp-exec-tests.txt": ("\n".join("test fixture::" + name + " ... ok" for name in REQUIRED_LOCAL_TESTS) + "\ntest result: ok.\n").encode(),
            "tenant-root.patch": (self.source / "tenant-root.patch").read_bytes(),
            "tenant-root-tests.txt": ("\n".join("test fixture::" + name + " ... ok" for name in REQUIRED_ROOT_TESTS) + "\ntest result: ok.\n").encode(),
            "runtime-log.patch": (self.source / "runtime-log.patch").read_bytes(),
            "runtime-log-tests.txt": ("\n".join("test fixture::" + name + " ... ok" for name in REQUIRED_LOG_TESTS) + "\ntest result: ok.\n").encode(),
            "runtime-log.json": (json.dumps({"level": "error", "msg": RUNTIME_LOG_MESSAGE, "time": "2026-09-06T00:00:00Z", "target": "youki", "fields": {"message": RUNTIME_LOG_MESSAGE}}) + "\n").encode(),
            "runtime-log-stdout.txt": b"",
            "runtime-log-stderr.txt": ("Error: " + RUNTIME_LOG_MESSAGE.removeprefix("error in executing command: ") + "\n").encode(),
            "runtime-log-exit-status.txt": b"1\n",
            "executable-permissions.patch": (self.source / "executable-permissions.patch").read_bytes(),
            "executable-permissions-tests.txt": executable_tests(executable_probes()),
            "tenant-cgroup.patch": (self.source / "tenant-cgroup.patch").read_bytes(),
            "tenant-cgroup-tests.txt": ("\n".join("test container::tenant_cgroup::tests::" + name + " ... ok" for name in REQUIRED_CGROUP_TESTS)
                                        + "\ntest result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n").encode(),
            "run-keep.patch": (self.source / "run-keep.patch").read_bytes(),
            "run-keep-tests.txt": ("\n".join("test commands::run::keep_tests::" + name + " ... ok" for name in REQUIRED_KEEP_TESTS)
                                    + "\ntest result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n").encode(),
            "foreground-wait.patch": (self.source / "foreground-wait.patch").read_bytes(),
            "foreground-wait-tests.txt": wait_tests(wait_probes()),
            "console-size.patch": (self.source / "console-size.patch").read_bytes(),
            "console-size-tests.txt": ("\n".join("test tty::tests::" + name + " ... ok" for name in REQUIRED_CONSOLE_TESTS)
                + "\ntest result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n").encode(),
            "executable-errors.patch": (self.source / "executable-errors.patch").read_bytes(),
            "executable-errors-tests.txt": ("\n".join("test workload::default::tests::" + name + " ... ok" for name in REQUIRED_EXEC_ERROR_TESTS)
                + "\ntest result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n").encode(),
            "runtime-audit.patch": (self.source / "runtime-audit.patch").read_bytes(),
            "runtime-audit-tests.txt": ("\n".join("test runtime_audit::tests::" + name + " ... ok" for name in REQUIRED_AUDIT_TESTS)
                + "\ntest result: ok. " + str(len(REQUIRED_AUDIT_TESTS)) +
                " passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n").encode(),
        }
        self.files.update(audit_native_files(self.source, self.files["version.txt"]))
        self.publish()

    def publish(self):
        for name, data in self.files.items():
            (self.root / name).write_bytes(data)
        (self.root / "youki").chmod(0o755)
        (self.root / "evidence.sha256").write_text("".join(f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in self.files.items()))

    def test_valid_candidate(self):
        self.assertEqual(validate(self.root, self.source), hashlib.sha256(self.files["youki"]).hexdigest())

    def test_actual_binary_checksums_are_revalidated(self):
        (self.root / "youki").write_bytes(elf() + b"drift")
        with self.assertRaisesRegex(ValueError, "evidence mismatch"):
            validate(self.root, self.source)

    def test_featureless_candidate_rejected_even_with_matching_manifest(self):
        self.files["features.json"] = b'{"linux":{"cgroup":{"v2":false,"v1":false,"systemd":false}}}'
        self.publish()
        with self.assertRaisesRegex(ValueError, "cgroup runtime features"):
            validate(self.root, self.source)

    def test_dynamic_or_truncated_elf_is_rejected(self):
        for data in (elf(3), elf(2, 1), elf()[:100], b"not ELF"):
            with self.subTest(data=data[:8]), self.assertRaises(ValueError):
                validate_elf(data)

    def test_symlinked_binary_is_rejected(self):
        (self.root / "youki").unlink()
        (self.root / "target").write_bytes(elf())
        (self.root / "youki").symlink_to(self.root / "target")
        with self.assertRaises(ValueError):
            validate(self.root, self.source)

    def test_executable_mode_and_exact_inventory_are_required(self):
        for mode in (0o400, 0o600, 0o777):
            (self.root / "youki").chmod(mode)
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "mode 0755"):
                validate(self.root, self.source)
        (self.root / "youki").chmod(0o755)
        (self.root / "unexpected").write_text("foreign")
        with self.assertRaisesRegex(ValueError, "inventory"):
            validate(self.root, self.source)

    def test_feature_dependencies_and_source_lock_are_required(self):
        for name, replacement in (("cargo-features.txt", b"libcgroups without device filter"), ("source-lock.sha256", b"0" * 64 + b" Cargo.lock")):
            original = self.files[name]
            self.files[name] = replacement
            self.publish()
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate(self.root, self.source)
            self.files[name] = original

    def test_exact_upstream_identity_and_focused_tests_are_required(self):
        for name, replacement in (("inputs.env", b"YOUKI_VERSION=0.5.7\n"), ("version.txt", b"youki version: 0.7.0\ncommit: wrong\n"), ("upstream-tests.txt", b"test result: ok. 0 passed\n")):
            original = self.files[name]
            self.files[name] = replacement
            self.publish()
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate(self.root, self.source)
            self.files[name] = original

    def test_each_time_and_device_regression_is_mandatory(self):
        original = self.files["upstream-tests.txt"]
        for test in REQUIRED_TESTS:
            for status in ("FAILED", "ignored"):
                self.files["upstream-tests.txt"] = original.replace((test + " ... ok").encode(), (test + " ... " + status).encode())
                self.publish()
                with self.subTest(test=test, status=status), self.assertRaisesRegex(ValueError, "missing passing upstream test"):
                    validate(self.root, self.source)

    def test_local_seccomp_patch_and_every_regression_are_required(self):
        original = self.files["seccomp-exec-tests.txt"]
        for test in REQUIRED_LOCAL_TESTS:
            self.files["seccomp-exec-tests.txt"] = original.replace((test + " ... ok").encode(), (test + " ... ignored").encode())
            self.publish()
            with self.subTest(test=test), self.assertRaisesRegex(ValueError, "missing passing local seccomp regression"):
                validate(self.root, self.source)
        self.files["seccomp-exec-tests.txt"] = original
        self.files["seccomp-exec.patch"] += b"\nforeign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_local_root_patch_and_every_regression_are_required(self):
        original = self.files["tenant-root-tests.txt"]
        for test in REQUIRED_ROOT_TESTS:
            self.files["tenant-root-tests.txt"] = original.replace((test + " ... ok").encode(), (test + " ... ignored").encode())
            self.publish()
            with self.subTest(test=test), self.assertRaisesRegex(ValueError, "missing passing local root regression"):
                validate(self.root, self.source)
        self.files["tenant-root-tests.txt"] = original
        self.files["tenant-root.patch"] += b"\nforeign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_runtime_log_patch_and_each_formatter_regression_are_required(self):
        original = self.files["runtime-log-tests.txt"]
        for test in REQUIRED_LOG_TESTS:
            self.files["runtime-log-tests.txt"] = original.replace((test + " ... ok").encode(), (test + " ... ignored").encode())
            self.publish()
            with self.subTest(test=test), self.assertRaisesRegex(ValueError, "missing passing runtime log regression"):
                validate(self.root, self.source)
        self.files["runtime-log-tests.txt"] = original
        self.files["runtime-log.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_runtime_failure_must_be_real_complete_containerd_compatible_error(self):
        original = self.files["runtime-log.json"]
        good = json.loads(original)
        bad_records = [good | {"level": "ERROR"}, good | {"level": "info"}, good | {"msg": "generic exit1"},
                       good | {"time": "not a timestamp"}, good | {"time": "2026-09-06T00:00:00"},
                       good | {"message": RUNTIME_LOG_MESSAGE}, good | {"timestamp": good["time"]},
                       good | {"time": "2026-09-06 00:00:00Z"}, good | {"time": "0001-01-01T00:00:00Z"}, good | {"target": "other"},
                       good | {"fields": {"message": "forged"}}, good | {"fields": {"message": RUNTIME_LOG_MESSAGE, "level": "error"}}]
        for row in bad_records:
            self.files["runtime-log.json"] = (json.dumps(row) + "\n").encode()
            self.publish()
            with self.subTest(row=row), self.assertRaises(ValueError):
                validate(self.root, self.source)
        for raw in (b"", original.rstrip(b"\n"), original + original, b"\xef\xbb\xbf" + original,
                    b"\xfe\xff" + original.decode().encode("utf-16-be"),
                    original.replace(b'"level": "error"', b'"level": "info", "level": "error"')):
            self.files["runtime-log.json"] = raw
            self.publish()
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                validate(self.root, self.source)
        self.files["runtime-log.json"] = original
        for name, bad in (("runtime-log-exit-status.txt", b"0\n"), ("runtime-log-exit-status.txt", b"2\n"),
                          ("runtime-log-stdout.txt", b"unexpected success\n"), ("runtime-log-stderr.txt", b"generic error\n")):
            old = self.files[name]
            self.files[name] = bad
            self.publish()
            with self.subTest(name=name, bad=bad), self.assertRaises(ValueError):
                validate(self.root, self.source)
            self.files[name] = old

    def test_executable_patch_and_every_regression_are_required(self):
        original = self.files["executable-permissions-tests.txt"]
        for test in REQUIRED_EXEC_TESTS:
            self.files["executable-permissions-tests.txt"] = original.replace((test + " ... ok").encode(), (test + " ... ignored").encode())
            self.publish()
            with self.subTest(test=test), self.assertRaisesRegex(ValueError, "missing passing executable regression"):
                validate(self.root, self.source)
        self.files["executable-permissions-tests.txt"] = original
        self.files["executable-permissions.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_executable_probes_require_exact_mode_exit_streams_and_kernel_denial(self):
        for index in (0, 1):
            for key, value in (("schema_version", True), ("case", "unknown"), ("mode", "0755"), ("uid", 1),
                               ("gid", 1), ("exit_code", 1), ("stdout", ""), ("stderr", "unexpected"), ("extra", True)):
                rows = executable_probes()
                rows[index][key] = value
                self.files["executable-permissions-tests.txt"] = executable_tests(rows)
                self.publish()
                with self.subTest(index=index, key=key), self.assertRaises(ValueError):
                    validate(self.root, self.source)
        original = executable_tests(executable_probes())
        invalid = [executable_tests([]), executable_tests(executable_probes()[:1]),
                   executable_tests(executable_probes() * 2), executable_tests(list(reversed(executable_probes()))),
                   original.replace(b"EACCES", b"ENOENT"), original.replace(b"4 passed; 0 failed", b"3 passed; 1 failed"),
                   original.replace(b'"uid": 0', b'"uid": 1, "uid": 0')]
        for raw in invalid:
            self.files["executable-permissions-tests.txt"] = raw
            self.publish()
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                validate(self.root, self.source)

    def test_cached_install_needs_no_docker_and_repairs_mode_without_modifying_aliases(self):
        recipe_files = ("Dockerfile", "inputs.env", "apk.sha256", "build.sh", "validate.py", "lock.py", "seccomp-exec.patch", "tenant-root.patch", "runtime-log.patch", "executable-permissions.patch", "tenant-cgroup.patch", "run-keep.patch", "foreground-wait.patch", "console-size.patch", "executable-errors.patch", "runtime-audit.patch", "runtime-audit-probe.sh", "../../scripts/helpers/linux_docker_runtime_audit.py")
        digest = hashlib.sha256("".join(f"{hashlib.sha256((self.source / name).read_bytes()).hexdigest()}  {name}\n" for name in recipe_files).encode()).hexdigest()
        cache = self.root / "cache"
        candidate = cache / "builds" / digest
        candidate.mkdir(parents=True)
        for name in (*self.files, "evidence.sha256"):
            (candidate / name).write_bytes((self.root / name).read_bytes())
        (candidate / "youki").chmod(0o755)
        destination = self.root / "installed" / "youki"
        environment = dict(os.environ, YOUKI_CACHE_DIR=str(cache), DOCKER_HOST="invalid://must-not-connect", BUILDX_BUILDER="must-not-use")
        environment.pop("YOUKI_DOCKER_CONTEXT", None)

        def install():
            return subprocess.run(["bash", str(self.source / "build.sh"), "--install", str(destination)], env=environment, capture_output=True, text=True, check=False)

        self.assertEqual(install().returncode, 0)
        destination.chmod(0o400)
        self.assertEqual(install().returncode, 0)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o755)
        self.assertEqual(destination.read_bytes(), self.files["youki"])
        alias = destination.parent / "alias"
        os.link(destination, alias)
        result = install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("hardlinked", result.stderr)
        self.assertEqual(destination.stat().st_ino, alias.stat().st_ino)

    def test_tenant_cgroup_patch_and_all_regressions_required(self):
        original = self.files["tenant-cgroup-tests.txt"]
        for test in REQUIRED_CGROUP_TESTS:
            self.files["tenant-cgroup-tests.txt"] = original.replace((test + " ... ok").encode(), (test + " ... ignored").encode())
            self.publish()
            with self.subTest(test=test), self.assertRaisesRegex(ValueError, "tenant cgroup regressions"):
                validate(self.root, self.source)
        self.files["tenant-cgroup-tests.txt"] = original
        self.files["tenant-cgroup.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_tenant_cgroup_duplicate_results_and_nonzero_failures_rejected(self):
        original = self.files["tenant-cgroup-tests.txt"]
        for bad in (original + original.splitlines(keepends=True)[0],
                    original + original.splitlines(keepends=True)[-1],
                    original + b"test result: ok. 6 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n",
                    original.replace(b"0 failed", b"1 failed"),
                    original.replace(b"0 ignored", b"1 ignored"), b"test result: ok.\n"):
            self.files["tenant-cgroup-tests.txt"] = bad
            self.publish()
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "tenant cgroup regressions"):
                validate(self.root, self.source)

    def test_run_keep_patch_and_exact_native_regressions_required(self):
        original = self.files["run-keep-tests.txt"]
        for test in REQUIRED_KEEP_TESTS:
            self.files["run-keep-tests.txt"] = original.replace((test + " ... ok").encode(), (test + " ... ignored").encode())
            self.publish()
            with self.subTest(test=test), self.assertRaisesRegex(ValueError, "run keep regressions"):
                validate(self.root, self.source)
        self.files["run-keep-tests.txt"] = original
        self.files["run-keep.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_run_keep_duplicate_incomplete_or_failed_results_rejected(self):
        original = self.files["run-keep-tests.txt"]
        for bad in (original + original.splitlines(keepends=True)[0],
                    original + original.splitlines(keepends=True)[-1],
                    original.replace(b"0 failed", b"1 failed"),
                    original.replace(b"0 ignored", b"1 ignored"),
                    original.replace(b"7 passed", b"0 passed"),
                    original.replace(b"commands::run::keep_tests::", b"foreign::"),
                    b"test result: ok.\n"):
            self.files["run-keep-tests.txt"] = bad
            self.publish()
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "run keep regressions"):
                validate(self.root, self.source)

    def test_foreground_wait_patch_and_exact_regressions_required(self):
        original = self.files["foreground-wait-tests.txt"]
        for name in REQUIRED_WAIT_TESTS:
            self.files["foreground-wait-tests.txt"] = original.replace((name + " ... ok").encode(), (name + " ... ignored").encode())
            self.publish()
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "foreground wait"):
                validate(self.root, self.source)
        self.files["foreground-wait-tests.txt"] = original
        self.files["foreground-wait.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_foreground_wait_proofs_require_actual_exits_no_wakeup_and_reaping(self):
        for index in range(4):
            for key, value in (("schema_version", True), ("case", "wrong"), ("exit_code", 99),
                               ("already_waitable", index == 3), ("pending_sigchld", True),
                               ("reaped", False), ("unrelated_reaped", False),
                               ("forwarded_sigterm", index != 3), ("extra", 1)):
                rows = wait_probes()
                rows[index][key] = value
                self.files["foreground-wait-tests.txt"] = wait_tests(rows)
                self.publish()
                with self.subTest(index=index, key=key), self.assertRaisesRegex(ValueError, "foreground wait"):
                    validate(self.root, self.source)

    def test_foreground_wait_missing_duplicate_and_failed_evidence_rejected(self):
        original = self.files["foreground-wait-tests.txt"]
        for bad in (wait_tests([]), wait_tests(wait_probes() * 2), wait_tests(list(reversed(wait_probes()))),
                    original.replace(b'"schema_version": 1', b'"schema_version": 0, "schema_version": 1'),
                    original + original.splitlines(keepends=True)[0],
                    original + original.splitlines(keepends=True)[-1],
                    original.replace(b"0 failed", b"1 failed"), original.replace(b"0 ignored", b"1 ignored"),
                    original.replace(b"4 passed", b"0 passed"), original.replace(b"... ok", b"... FAILED")):
            self.files["foreground-wait-tests.txt"] = bad
            self.publish()
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "foreground wait"):
                validate(self.root, self.source)

    def test_foreground_wait_actual_nocapture_format_is_accepted(self):
        self.files["foreground-wait-tests.txt"] = wait_tests(wait_probes()).replace(b" ... ok\n", b" ... \n\nok\n")
        self.publish()
        validate(self.root, self.source)

    def test_console_size_patch_and_each_native_regression_are_required(self):
        original = self.files["console-size-tests.txt"]
        for name in REQUIRED_CONSOLE_TESTS:
            for status in ("ignored", "FAILED"):
                self.files["console-size-tests.txt"] = original.replace((name+" ... ok").encode(),
                                                                      (name+" ... "+status).encode())
                self.publish()
                with self.subTest(name=name, status=status), self.assertRaisesRegex(ValueError, "console size"):
                    validate(self.root, self.source)
        self.files["console-size-tests.txt"] = original
        self.files["console-size.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_console_size_duplicate_incomplete_or_foreign_results_rejected(self):
        original = self.files["console-size-tests.txt"]
        for bad in (original + original.splitlines(keepends=True)[0],
                    original + original.splitlines(keepends=True)[-1],
                    original.replace(b"0 failed", b"1 failed"), original.replace(b"0 ignored", b"1 ignored"),
                    original.replace(b"4 passed", b"0 passed"), original.replace(b"tty::tests::", b"foreign::"),
                    b"test result: ok.\n"):
            self.files["console-size-tests.txt"] = bad
            self.publish()
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "console size"):
                validate(self.root, self.source)

    def test_console_size_evidence_and_exact_commit_component_required(self):
        original = self.files["version.txt"]
        for replacement in (b"", b"+vz-console-size-v10", b"+vz-console-size-v1+unknown"):
            self.files["version.txt"] = original.replace(b"+vz-console-size-v1", replacement)
            self.publish()
            with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, "commit"):
                validate(self.root, self.source)
        self.files["version.txt"] = original
        self.files.pop("console-size-tests.txt")
        self.publish()
        with self.assertRaisesRegex(ValueError, "incomplete youki evidence manifest"):
            validate(self.root, self.source)

    def test_console_size_recipe_context_and_frozen_test_target(self):
        recipe = (self.source / "Dockerfile").read_text()
        build = (self.source / "build.sh").read_text()
        self.assertIn('COPY console-size.patch /inputs/console-size.patch', recipe)
        self.assertIn('patch --batch --forward --fuzz=0 -p1 -i /inputs/console-size.patch', recipe)
        self.assertIn('"$YOUKI_CONSOLE_PATCH_SHA256  /inputs/console-size.patch"', recipe)
        self.assertIn('+$YOUKI_CONSOLE_PATCH_ID', recipe)
        self.assertIn('--skip test_vz_console_size_', recipe)
        self.assertIn('--target aarch64-unknown-linux-musl -p libcontainer --lib', recipe)
        self.assertIn('tty::tests::test_vz_console_size_', recipe)
        self.assertIn('/inputs/console-size.patch /inputs/console-size-tests.txt /result/', recipe)
        self.assertIn('foreground-wait-tests.txt console-size.patch console-size-tests.txt executable-errors.patch executable-errors-tests.txt runtime-audit.patch runtime-audit-tests.txt > evidence.sha256', recipe)
        self.assertIn('foreground-wait.patch console-size.patch executable-errors.patch runtime-audit.patch runtime-audit-probe.sh ../../scripts/helpers/linux_docker_runtime_audit.py | shasum', build)
        self.assertIn('cp "$recipe_dir/console-size.patch" "$context/"', build)

    def test_executable_errors_patch_and_each_native_regression_are_required(self):
        original = self.files["executable-errors-tests.txt"]
        for name in REQUIRED_EXEC_ERROR_TESTS:
            for status in ("ignored", "FAILED"):
                self.files["executable-errors-tests.txt"] = original.replace((name+" ... ok").encode(),
                                                                            (name+" ... "+status).encode())
                self.publish()
                with self.subTest(name=name, status=status), self.assertRaisesRegex(ValueError, "executable error"):
                    validate(self.root, self.source)
        self.files["executable-errors-tests.txt"] = original
        self.files["executable-errors.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_executable_errors_duplicate_incomplete_or_foreign_results_rejected(self):
        original = self.files["executable-errors-tests.txt"]
        for bad in (original + original.splitlines(keepends=True)[0],
                    original + original.splitlines(keepends=True)[-1],
                    original.replace(b"0 failed", b"1 failed"), original.replace(b"0 ignored", b"1 ignored"),
                    original.replace(b"4 passed", b"0 passed"),
                    original.replace(b"workload::default::tests::", b"foreign::"), b"test result: ok.\n"):
            self.files["executable-errors-tests.txt"] = bad
            self.publish()
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "executable error"):
                validate(self.root, self.source)

    def test_executable_errors_evidence_and_exact_commit_component_required(self):
        original = self.files["version.txt"]
        for replacement in (b"", b"+vz-executable-errors-v10", b"+vz-executable-errors-v1+unknown"):
            self.files["version.txt"] = original.replace(b"+vz-executable-errors-v1", replacement)
            self.publish()
            with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, "commit"):
                validate(self.root, self.source)
        self.files["version.txt"] = original
        self.files.pop("executable-errors-tests.txt")
        self.publish()
        with self.assertRaisesRegex(ValueError, "incomplete youki evidence manifest"):
            validate(self.root, self.source)

    def test_executable_errors_recipe_context_and_frozen_test_target(self):
        recipe = (self.source / "Dockerfile").read_text()
        build = (self.source / "build.sh").read_text()
        self.assertIn('COPY executable-errors.patch /inputs/executable-errors.patch', recipe)
        self.assertIn('patch --batch --forward --fuzz=0 -p1 -i /inputs/executable-errors.patch', recipe)
        self.assertIn('"$YOUKI_EXEC_ERROR_PATCH_SHA256  /inputs/executable-errors.patch"', recipe)
        self.assertIn('+$YOUKI_EXEC_ERROR_PATCH_ID', recipe)
        self.assertIn('--skip test_vz_executable_errors_', recipe)
        self.assertIn('--target aarch64-unknown-linux-musl -p libcontainer --lib', recipe)
        self.assertIn('workload::default::tests::test_vz_executable_errors_', recipe)
        self.assertIn('/inputs/executable-errors.patch /inputs/executable-errors-tests.txt /result/', recipe)
        self.assertIn('console-size-tests.txt executable-errors.patch executable-errors-tests.txt runtime-audit.patch runtime-audit-tests.txt > evidence.sha256', recipe)
        self.assertIn('console-size.patch executable-errors.patch runtime-audit.patch runtime-audit-probe.sh ../../scripts/helpers/linux_docker_runtime_audit.py | shasum', build)
        self.assertIn('cp "$recipe_dir/executable-errors.patch" "$context/"', build)

    def test_runtime_audit_patch_and_each_native_regression_are_required(self):
        original = self.files["runtime-audit-tests.txt"]
        for name in REQUIRED_AUDIT_TESTS:
            for status in ("ignored", "FAILED"):
                self.files["runtime-audit-tests.txt"] = original.replace((name + " ... ok").encode(),
                                                                         (name + " ... " + status).encode())
                self.publish()
                with self.subTest(name=name, status=status), self.assertRaisesRegex(ValueError, "runtime audit"):
                    validate(self.root, self.source)
        self.files["runtime-audit-tests.txt"] = original
        self.files["runtime-audit.patch"] += b"foreign patch\n"
        self.publish()
        with self.assertRaisesRegex(ValueError, "stale build input"):
            validate(self.root, self.source)

    def test_runtime_audit_duplicate_incomplete_foreign_and_failed_results_rejected(self):
        original = self.files["runtime-audit-tests.txt"]
        for bad in (original + original.splitlines(keepends=True)[0],
                    original + original.splitlines(keepends=True)[-1],
                    b"".join(original.splitlines(keepends=True)[1:]),
                    original.replace(b"0 failed", b"1 failed"), original.replace(b"0 ignored", b"1 ignored"),
                    original.replace((str(len(REQUIRED_AUDIT_TESTS)) + " passed").encode(), b"0 passed"),
                    original.replace(b"runtime_audit::tests::", b"foreign::"), b"test result: ok.\n"):
            self.files["runtime-audit-tests.txt"] = bad
            self.publish()
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, "runtime audit"):
                validate(self.root, self.source)

    def test_runtime_audit_evidence_and_exact_commit_component_required(self):
        original = self.files["version.txt"]
        for replacement in (b"", b"+vz-runtime-audit-v10", b"+vz-runtime-audit-v1+unknown"):
            self.files["version.txt"] = original.replace(b"+vz-runtime-audit-v1", replacement)
            self.publish()
            with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, "commit"):
                validate(self.root, self.source)
        self.files["version.txt"] = original
        self.files.pop("runtime-audit-tests.txt")
        self.publish()
        with self.assertRaisesRegex(ValueError, "incomplete youki evidence manifest"):
            validate(self.root, self.source)

    def test_runtime_audit_recipe_context_and_frozen_test_target(self):
        recipe = (self.source / "Dockerfile").read_text()
        build = (self.source / "build.sh").read_text()
        self.assertIn('COPY runtime-audit.patch /inputs/runtime-audit.patch', recipe)
        self.assertIn('patch --batch --forward --fuzz=0 -p1 -i /inputs/runtime-audit.patch', recipe)
        self.assertIn('"$YOUKI_AUDIT_PATCH_SHA256  /inputs/runtime-audit.patch"', recipe)
        self.assertIn('+$YOUKI_AUDIT_PATCH_ID', recipe)
        commands = [line.replace('\\\n', ' ') for line in recipe.split('RUN ') if
                    'runtime_audit::tests::test_vz_runtime_audit_' in line]
        self.assertEqual(len(commands), 1)
        selected = commands[0]
        self.assertIn('--network=none cargo +1.96.0 test --frozen --release --locked', selected)
        self.assertIn('--target aarch64-unknown-linux-musl -p youki --bin youki', selected)
        self.assertIn('--no-default-features --features v2,cgroupsv2_devices,seccomp', selected)
        self.assertIn('-- --test-threads=1 > /inputs/runtime-audit-tests.txt 2>&1', selected)
        self.assertIn('{ cat /inputs/runtime-audit-tests.txt; exit 1; }', selected)
        self.assertIn('/inputs/runtime-audit.patch /inputs/runtime-audit-tests.txt /result/', recipe)
        self.assertIn('executable-errors-tests.txt runtime-audit.patch runtime-audit-tests.txt > evidence.sha256', recipe)
        self.assertIn('executable-errors.patch runtime-audit.patch runtime-audit-probe.sh ../../scripts/helpers/linux_docker_runtime_audit.py | shasum', build)
        self.assertIn('cp "$recipe_dir/runtime-audit.patch" "$context/"', build)

    def test_native_runtime_audit_artifacts_and_source_selected_inputs_required(self):
        for name in AUDIT_NATIVE_FILES:
            original = self.files.pop(name)
            self.publish()
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "incomplete youki evidence manifest"):
                validate(self.root, self.source)
            self.files[name] = original
        for name in ("runtime-audit-parser.py", "runtime-audit-probe.sh"):
            original = self.files[name]; self.files[name] += b"foreign source\n"
            self.publish()
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "stale build input"):
                validate(self.root, self.source)
            self.files[name] = original

    def test_native_runtime_audit_typed_outcomes_raw_argv_and_status_bound(self):
        for case, _, _ in AUDIT_NATIVE_CASES:
            for suffix, bad in ((".argv", b"/bin/true\0"), (".exit-status.txt", b"137\n"), (".stdout", b"foreign\n")):
                name = "runtime-audit-" + case + suffix
                original = self.files[name]; self.files[name] = bad
                self.publish()
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "runtime audit"):
                    validate(self.root, self.source)
                self.files[name] = original
        original = self.files["runtime-audit-events.jsonl"]
        for index, field, value in ((1, "exit_code", 37), (2, "operation", "start"),
                                    (4, "container_id", "c" * 64), (7, "outcome", "ok")):
            events = [json.loads(line) for line in original.splitlines()]
            events[index][field] = value
            self.files["runtime-audit-events.jsonl"] = b"".join(json.dumps(event).encode() + b"\n" for event in events)
            self.publish()
            with self.subTest(index=index, field=field), self.assertRaisesRegex(ValueError, "runtime audit"):
                validate(self.root, self.source)
        self.files["runtime-audit-events.jsonl"] = original

    def test_native_runtime_audit_incomplete_session_boot_and_metadata_rejected(self):
        changes = (("runtime-audit-status.txt", b"incomplete\n"),
                   ("runtime-audit-boot-id.txt", b"22345678-1234-1234-1234-123456789abc\n"),
                   ("runtime-audit-create.stderr", b"unrelated failure\n"),
                   ("runtime-audit-version.stderr", b"audit diagnostic\n"))
        for name, bad in changes:
            original = self.files[name]; self.files[name] = bad
            self.publish()
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "runtime audit"):
                validate(self.root, self.source)
            self.files[name] = original
        names = ("runtime-audit-metadata-before.txt", "runtime-audit-metadata-after.txt")
        original = self.files[names[0]]
        for old, new in ((b"|0|0|600|1", b"|0|0|666|1"), (b"|0|0|600|1", b"|0|0|600|2"),
                         (b"|0|0|700|2", b"|1|0|700|2"), (b"|41|101", b"|41|100")):
            for name in names:
                self.files[name] = original.replace(old, new)
            self.publish()
            with self.subTest(old=old, new=new), self.assertRaisesRegex(ValueError, "runtime audit"):
                validate(self.root, self.source)
        for name in names:
            self.files[name] = original

    def test_native_runtime_audit_probe_recipe_is_bounded_and_source_pinned(self):
        recipe, build, probe = [(self.source / name).read_text() for name in ("Dockerfile", "build.sh", "runtime-audit-probe.sh")]
        self.assertIn('COPY runtime-audit-parser.py /inputs/runtime-audit-parser.py', recipe)
        self.assertIn('COPY runtime-audit-probe.sh /inputs/runtime-audit-probe.sh', recipe)
        self.assertIn('"$YOUKI_AUDIT_PARSER_SHA256  /inputs/runtime-audit-parser.py"', recipe)
        self.assertIn('"$YOUKI_AUDIT_PROBE_SHA256  /inputs/runtime-audit-probe.sh"', recipe)
        self.assertIn('/bin/sh /inputs/runtime-audit-probe.sh', recipe)
        self.assertIn('cp "$recipe_dir/../../scripts/helpers/linux_docker_runtime_audit.py" "$context/runtime-audit-parser.py"', build)
        self.assertIn('/bin/busybox timeout -s KILL 30 /result/youki', probe)
        self.assertIn('cat /proc/sys/kernel/random/boot_id > /result/runtime-audit-boot-id.txt', probe)
        for case, _, code in AUDIT_NATIVE_CASES:
            self.assertIn('audit_run ' + case + ' ' + str(code) + ' ', probe)
        for name in AUDIT_NATIVE_FILES:
            self.assertIn(name, recipe)

    def test_advisory_lock_rejects_live_owner_then_releases(self):
        import fcntl
        import sys
        lock = self.root / "lock"
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
        command = [sys.executable, str(self.source / "lock.py"), str(lock), sys.executable, "-c", "print('released')"]
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("live advisory lock", result.stderr)
        finally:
            os.close(descriptor)
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "released")


if __name__ == "__main__":
    unittest.main()
