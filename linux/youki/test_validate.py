"""Offline negative tests; real OCI behavior still requires the local VM gate."""
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest

from validate import REQUIRED_LOCAL_TESTS, REQUIRED_LOG_TESTS, REQUIRED_ROOT_TESTS, REQUIRED_TESTS, RUNTIME_LOG_MESSAGE, validate, validate_elf


def elf(kind=1, dynamic_tag=0):
    data = bytearray(136)
    data[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<H", data, 18, 183)
    struct.pack_into("<Q", data, 32, 64)
    struct.pack_into("<HH", data, 54, 56, 1)
    struct.pack_into("<IIQQQQQQ", data, 64, kind, 0, 120, 0, 0, 16, 16, 8)
    struct.pack_into("<qQ", data, 120, dynamic_tag, 0)
    return bytes(data)


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
            "version.txt": ("youki version: " + inputs["YOUKI_VERSION"] + "\ncommit: " + inputs["YOUKI_VERSION"] + "-" + inputs["YOUKI_COMMIT"] + "+" + inputs["YOUKI_PATCH_ID"] + "+" + inputs["YOUKI_ROOT_PATCH_ID"] + "+" + inputs["YOUKI_LOG_PATCH_ID"] + "\n").encode(),
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
        }
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

    def test_cached_install_needs_no_docker_and_repairs_mode_without_modifying_aliases(self):
        recipe_files = ("Dockerfile", "inputs.env", "apk.sha256", "build.sh", "validate.py", "lock.py", "seccomp-exec.patch", "tenant-root.patch", "runtime-log.patch")
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
