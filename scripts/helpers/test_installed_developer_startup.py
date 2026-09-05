"""Offline adversarial harness checks; no Docker daemon, VM, or product process."""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import installed_developer_startup as driver


class InstalledStartupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def bundle(self, profile):
        root = self.root / profile
        root.mkdir()
        version = {"profile": profile}
        for filename, key in (("vmlinux", "sha256_vmlinux"), ("initramfs.img", "sha256_initramfs"), ("youki", "sha256_youki")):
            (root / filename).write_bytes(filename.encode())
            version[key] = driver.digest(root / filename)
        if profile == "developer":
            name = "developer-probe-rootfs.tar"
            (root / name).write_bytes(b"fixture archive")
            version["developer_probe"] = {"schema_version": 1, "archive": name, "sha256": driver.digest(root / name)}
        driver.document(root / "version.json", version)
        return root

    def args(self):
        release = self.root / "release"
        release.mkdir()
        for name in ("vz", "vz-runtimed", "docker", "docker-compose", "docker-buildx"):
            (release / name).write_bytes(b"fixture executable")
            (release / name).chmod(0o700)
        return argparse.Namespace(release_dir=str(release), release_version="0.4.0-dev-test",
            developer_bundle=str(self.bundle("developer")), hardened_bundle=str(self.bundle("container")),
            docker=str(release / "docker"), compose_plugin=str(release / "docker-compose"),
            buildx_plugin=str(release / "docker-buildx"), evidence_dir=str(self.root / "evidence"))

    def test_unknown_duplicate_and_abbreviated_arguments_rejected_before_effects(self):
        for argv in (["--unknown"], ["--docker", "/x", "--docker=/y"], ["--release-d", "/x"]):
            with self.subTest(argv=argv), self.assertRaises((ValueError, SystemExit)):
                driver.arguments(argv)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_preflight_has_no_filesystem_or_execution_effects(self):
        args = self.args()
        before = sorted(str(path) for path in self.root.rglob("*"))
        with patch.object(driver, "execute", side_effect=AssertionError("unexpected command")):
            result = driver.preflight(args, require_host=False)
        self.assertEqual(before, sorted(str(path) for path in self.root.rglob("*")))
        self.assertFalse(result["aggregate_release_certified"])
        self.assertFalse(result["docker_parity_certified"])

    def test_existing_and_symlink_evidence_rejected(self):
        args = self.args()
        evidence = Path(args.evidence_dir)
        evidence.mkdir()
        with self.assertRaises(ValueError):
            driver.preflight(args, require_host=False)
        evidence.rmdir()
        evidence.symlink_to(self.root / "missing")
        with self.assertRaises(ValueError):
            driver.preflight(args, require_host=False)

    def test_poisoned_kernel_missing_probe_and_redirected_archive_rejected(self):
        root = self.bundle("developer")
        inputs = driver.bundle_inputs(root, "developer")
        self.assertEqual(len(inputs), 5)
        version = json.loads((root / "version.json").read_bytes())
        for mutation in ("kernel", "missing", "redirect"):
            with self.subTest(mutation=mutation):
                if mutation == "kernel":
                    (root / "vmlinux").write_bytes(b"poison")
                elif mutation == "missing":
                    (root / "developer-probe-rootfs.tar").unlink()
                else:
                    version["developer_probe"]["archive"] = "../foreign"
                    (root / "version.json").write_text(json.dumps(version))
                with self.assertRaises((ValueError, OSError)):
                    driver.bundle_inputs(root, "developer")
                (root / "vmlinux").write_bytes(b"vmlinux")

    def test_digest_refuses_symbolic_and_hard_links(self):
        original = self.root / "original"
        original.write_bytes(b"payload")
        link = self.root / "link"
        link.symlink_to(original)
        with self.assertRaises(OSError):
            driver.digest(link)
        link.unlink()
        os.link(original, link)
        with self.assertRaises(ValueError):
            driver.digest(original)

    def test_recorder_durable_intent_precedes_dispatch_and_preserves_argv0(self):
        record = driver.Recorder(self.root, {})
        def fake(argv, **kwargs):
            intent = json.loads(next(self.root.glob("*.intent.json")).read_bytes())
            self.assertTrue(intent["effects_uncertain"])
            self.assertEqual(intent["argv0"], "docker")
            self.assertEqual(kwargs["executable"], "/pinned/docker-tools")
            return subprocess.CompletedProcess(argv, 0, b"out\n", b"err\n")
        with patch.object(driver, "execute", side_effect=fake):
            output = record.run("test", ["docker", "--version"], cwd=self.root, executable="/pinned/docker-tools")
        self.assertEqual(output, (b"out\n", b"err\n", 0))
        self.assertFalse(record.receipts[0]["effects_uncertain"])
        self.assertEqual(next(self.root.glob("*.stdout")).read_bytes(), b"out\n")

    def test_interrupt_timeout_and_spawn_failure_keep_uncertainty(self):
        for index, error in enumerate((KeyboardInterrupt(), subprocess.TimeoutExpired(["fake"], 1), OSError("spawn"))):
            root = self.root / str(index)
            root.mkdir()
            record = driver.Recorder(root, {})
            with patch.object(driver, "execute", side_effect=error), self.assertRaises(type(error)):
                record.run("interrupted", ["/fake"], cwd=root)
            self.assertTrue(record.receipts[0]["effects_uncertain"])
            self.assertFalse(record.receipts[0]["capture_complete"])
            self.assertEqual(record.receipts[0]["hashes_cover"], "retained_observed_prefixes")

    def test_real_bounded_noisy_host_process_fails_with_prefix_receipt(self):
        record = driver.Recorder(self.root, {"PATH": "/usr/bin:/bin"})
        with patch.object(driver, "LIMIT", 64), self.assertRaises(ValueError):
            record.run("noisy", [sys.executable, "-c", "import os; os.write(1, b'x' * 10000)"], cwd=self.root)
        row = record.receipts[0]
        self.assertTrue(row["effects_uncertain"])
        self.assertFalse(row["capture_complete"])
        self.assertEqual(row["retained_stdout_bytes"], 64)
        self.assertEqual(row["stdout_sha256"], hashlib.sha256(b"x" * 64).hexdigest())

    def test_cli_observer_timeout_never_kills_autospawn_daemon_group(self):
        process = MagicMock()
        process.returncode = None
        process.__enter__.return_value = process
        with patch.object(driver.subprocess, "Popen", return_value=process), \
             patch.object(driver, "collect_output", side_effect=subprocess.TimeoutExpired(["vz"], 1)), \
             patch.object(driver.os, "killpg", side_effect=AssertionError("daemon group killed")), \
             self.assertRaises(subprocess.TimeoutExpired):
            driver.execute_observer(["vz"], timeout=1, max_stream_bytes=64, check=False)
        process.kill.assert_called_once_with()
        process.wait.assert_called_once_with()

    def test_uncertain_cleanup_cannot_stop_or_signal(self):
        harness = object.__new__(driver.Harness)
        harness.record = driver.Recorder(self.root, {})
        harness.record.receipts.append({"effects_uncertain": True})
        harness.stop = lambda *args: self.fail("Stop dispatched under uncertainty")
        with patch.object(driver.os, "kill", side_effect=AssertionError("signal dispatched")), self.assertRaises(ValueError):
            harness.cleanup()

    def test_context_rejects_global_foreign_stale_and_missing_capability(self):
        environment = {"state": "ready", "project_id": "prj_one", "environment_id": "env_one"}
        machine = {"state": "ready", "machine_id": "mch_one", "incarnation_id": "inc_one", "incarnation_generation": 2,
                   "negotiated_capabilities": {"capabilities": ["docker_engine", "compose", "buildx"]},
                   "docker_context": {"schema_version": 1, "owner": {"project_id": "prj_one", "environment_id": "env_one", "machine_id": "mch_one"},
                                      "name": "owned", "endpoint": "unix:///owned.sock", "config_dir": str(self.root),
                                      "incarnation_id": "inc_one", "incarnation_generation": 2}}
        self.assertEqual(driver.context_descriptor(environment, machine, self.root), machine["docker_context"])
        for field, value in (("name", "default"), ("endpoint", "tcp://foreign"), ("incarnation_generation", 1), ("config_dir", "/foreign")):
            poisoned = copy.deepcopy(machine)
            poisoned["docker_context"][field] = value
            with self.assertRaises(ValueError):
                driver.context_descriptor(environment, poisoned, self.root)
        for kind in ("project_id", "environment_id", "machine_id"):
            poisoned = copy.deepcopy(machine)
            poisoned["docker_context"]["owner"][kind] = "foreign"
            with self.assertRaises(ValueError):
                driver.context_descriptor(environment, poisoned, self.root)
        machine["negotiated_capabilities"]["capabilities"].remove("buildx")
        with self.assertRaises(ValueError):
            driver.context_descriptor(environment, machine, self.root)

    def test_no_context_engine_call_rejected(self):
        harness = object.__new__(driver.Harness)
        harness.command = lambda *args, **kwargs: self.fail("Engine dispatch without context")
        with self.assertRaises(ValueError):
            harness.docker("wrong", None, ["info"])

    def test_isolation_preserves_home_without_catalog_or_daemon_override(self):
        args = self.args()
        info = driver.preflight(args, require_host=False)
        run = self.root / "owned-run"
        run.mkdir()
        ambient = {"HOME": str(self.root), "DOCKER_HOST": "tcp://foreign:2375", "DOCKER_CONTEXT": "foreign",
                   "VZ_MACHINE_TARGET_CATALOG": "/foreign/catalog", "CARGO_BIN_EXE_vz-runtimed": "/foreign/daemon",
                   "SSH_AUTH_SOCK": "/foreign/agent", "HTTPS_PROXY": "http://foreign"}
        with patch.dict(os.environ, ambient, clear=True), patch.object(driver.tempfile, "mkdtemp", return_value=str(run)):
            harness = driver.Harness(info)
        self.assertEqual(harness.env["HOME"], str(self.root))
        for key in ambient.keys() - {"HOME"}:
            self.assertNotIn(key, harness.env)
        self.assertEqual(harness.env["VZ_DOCKER_CONFIG"], str(run / "docker"))

    def test_failed_up_never_becomes_ready_and_keeps_exact_cleanup_admission(self):
        harness = object.__new__(driver.Harness)
        harness.cli, harness.cleanup_targets, harness.unresolved_up = self.root / "vz", [], set()
        driver.document(self.root / "vz.json", {"project_id": "prj_owned"})
        def command(label, args, **kwargs):
            self.assertFalse(kwargs["success"])
            request = args[args.index("--request-id") + 1]
            admission = {"schema_version": 1, "project_id": "prj_owned", "request_id": request,
                         "idempotency_key": request, "environment_id": "env_immutable"}
            raw = {"progress": {"admission": admission, "completion": {"operation": {"status": "failed"}, "admission": admission}}}
            return json.dumps(raw).encode(), b"", 5
        harness.command = command
        harness.status = lambda *args: self.fail("status after failed aggregate admission")
        with self.assertRaises(ValueError):
            harness.up(self.root, "exact-owned-environment")
        self.assertEqual(harness.cleanup_targets, [(self.root, "env_immutable")])
        self.assertFalse(harness.unresolved_up)

    def test_up_without_admission_never_authorizes_cleanup_by_name(self):
        harness = object.__new__(driver.Harness)
        harness.cli, harness.cleanup_targets, harness.unresolved_up = self.root / "vz", [], set()
        driver.document(self.root / "vz.json", {"project_id": "prj_owned"})
        harness.command = lambda *args, **kwargs: (b'{"error":"pre-admission failure"}\n', b"", 2)
        with self.assertRaises(ValueError):
            harness.up(self.root, "unproven-name")
        self.assertFalse(harness.cleanup_targets)
        self.assertTrue(harness.unresolved_up)
        harness.record = driver.Recorder(self.root, {})
        harness.stop = lambda *args: self.fail("Stop by unproven name")
        with patch.object(driver.os, "kill", side_effect=AssertionError("daemon signaled")), self.assertRaises(ValueError):
            harness.cleanup()

    def test_nonterminal_admission_survives_nonzero_stream_failure(self):
        harness = object.__new__(driver.Harness)
        harness.cli, harness.cleanup_targets, harness.unresolved_up = self.root / "vz", [], set()
        driver.document(self.root / "vz.json", {"project_id": "prj_owned"})
        def command(label, args, **kwargs):
            request = args[args.index("--request-id") + 1]
            raw = {"progress": {"admission": {"schema_version": 1, "project_id": "prj_owned", "request_id": request,
                    "idempotency_key": request, "environment_id": "env_admitted"}, "completion": None}}
            return json.dumps(raw).encode() + b'\n{"error":"lost completion"}\n', b"", 5
        harness.command = command
        with self.assertRaises(ValueError):
            harness.up(self.root, "untrusted-name")
        self.assertEqual(harness.cleanup_targets, [(self.root, "env_admitted")])
        self.assertFalse(harness.unresolved_up)

    def test_exact_topology_rejects_partial_duplicate_and_foreign_project(self):
        primary = {"project_id": "prj_shared", "environment_id": "env_a", "machines": [
            {"machine_id": "mch_a", "profile": "developer"}, {"machine_id": "mch_b", "profile": "developer"}]}
        neighbor = {"project_id": "prj_shared", "environment_id": "env_b", "machines": [
            {"machine_id": "mch_c", "profile": "developer"}, {"machine_id": "mch_d", "profile": "developer"}]}
        driver.exact_developer_topology(primary, neighbor)
        for kind in ("missing", "extra", "same_environment", "foreign_project", "shared_machine", "wrong_profile"):
            changed = copy.deepcopy(neighbor)
            if kind == "missing":
                changed["machines"].pop()
            elif kind == "extra":
                changed["machines"].append({"machine_id": "mch_e", "profile": "developer"})
            elif kind == "same_environment":
                changed["environment_id"] = primary["environment_id"]
            elif kind == "foreign_project":
                changed["project_id"] = "prj_foreign"
            elif kind == "shared_machine":
                changed["machines"][0]["machine_id"] = "mch_a"
            else:
                changed["machines"][0]["profile"] = "hardened"
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                driver.exact_developer_topology(primary, changed)

    def test_shutdown_log_read_is_bounded_private_and_nofollow(self):
        path = self.root / "daemon.log"
        path.write_bytes(b"runtime daemon shutting down")
        path.chmod(0o600)
        self.assertEqual(driver.read_private_regular(path, 64), path.read_bytes())
        with self.assertRaises(ValueError):
            driver.read_private_regular(path, 4)
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            driver.read_private_regular(path, 64)
        path.chmod(0o600)
        alias = self.root / "alias"
        alias.symlink_to(path)
        with self.assertRaises(OSError):
            driver.read_private_regular(alias, 64)
        alias.unlink()
        os.link(path, alias)
        with self.assertRaises(ValueError):
            driver.read_private_regular(path, 64)

    def test_cleanup_rejects_reused_daemon_pid_before_signal(self):
        harness = object.__new__(driver.Harness)
        harness.record = driver.Recorder(self.root, {})
        harness.cleanup_targets = []
        harness.unresolved_up = set()
        harness.daemon_identity = {"pid": 123, "process": "original start"}
        harness.daemon_fingerprint = lambda: {"pid": 123, "process": "replacement start"}
        with patch.object(driver.os, "kill", side_effect=AssertionError("signal dispatched")), self.assertRaises(ValueError):
            harness.cleanup()

    def test_immutable_image_id_parser(self):
        self.assertEqual(driver.image_id(b"sha256:" + b"a" * 64 + b"\n"), "sha256:" + "a" * 64)
        for raw in (b"latest", b"sha256:ABC", b"sha256:" + b"a" * 64 + b"\nextra"):
            with self.assertRaises(ValueError):
                driver.image_id(raw)


if __name__ == "__main__":
    unittest.main()
