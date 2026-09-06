"""Offline adversarial harness checks; no Docker daemon, VM, or product process."""

import argparse
import base64
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

    def test_private_canary_in_arguments_or_environment_rejected_before_intent(self):
        for env, argv in (({}, ["fake", "disposable-private-key"]),
                          ({"PRIVATE": "disposable-private-key"}, ["fake"])):
            record = driver.Recorder(self.root, env)
            record.canaries = [b"disposable-private-key"]
            with patch.object(driver, "execute") as execute, self.assertRaises(ValueError):
                record.run("private", argv, cwd=self.root)
            execute.assert_not_called()
            self.assertFalse(list(self.root.iterdir()))

    def test_escaped_private_argument_and_environment_rejected_before_intent(self):
        for secret in ('private\ncanary', 'private"canary', 'private\\canary'):
            for env, argv in (({}, ['fake', secret]), ({'PRIVATE': secret}, ['fake'])):
                with self.subTest(secret=secret, env=bool(env)):
                    record = driver.Recorder(self.root, env)
                    record.canaries = [secret.encode()]
                    with patch.object(driver, 'execute') as execute, self.assertRaises(ValueError):
                        record.run('private', argv, cwd=self.root)
                    execute.assert_not_called()
                    self.assertFalse(record.receipts)
                    self.assertFalse(list(self.root.iterdir()))

    def test_private_raw_or_decoded_buildkit_stream_is_withheld(self):
        secret = b"disposable-private-key"
        encoded = json.dumps({"logs": [{"data": base64.b64encode(secret).decode()}]}).encode() + b"\n"
        escaped = b'{"message":"\\u0064isposable-private-key"}\n'
        pretty = b'{\n  "message": "\\u0064isposable-private-key"\n}\n'
        for index, raw in enumerate((secret, encoded, escaped, pretty)):
            root = self.root / str(index)
            root.mkdir()
            record = driver.Recorder(root, {})
            record.canaries = [secret]
            output = subprocess.CompletedProcess(["fake"], 0, b"", raw)
            with patch.object(driver, "execute", return_value=output), self.assertRaisesRegex(ValueError, "withheld"):
                record.run("private", ["fake"], cwd=root)
            row = record.receipts[0]
            self.assertTrue(row["effects_uncertain"] and row["secret_leak_detected"])
            self.assertFalse(row["capture_complete"])
            self.assertEqual(row["hashes_cover"], "redacted_placeholders_not_original_streams")
            for path in root.iterdir():
                self.assertNotIn(secret, path.read_bytes())
                self.assertNotIn(base64.b64encode(secret), path.read_bytes())
                self.assertNotIn(escaped.strip(), path.read_bytes())
                self.assertNotIn(pretty.strip(), path.read_bytes())

    def test_runtime_retention_writes_exact_scanned_bytes_not_reopened_source(self):
        from types import SimpleNamespace
        runtime, evidence = self.root / 'runtime', self.root / 'evidence'
        runtime.mkdir(); evidence.mkdir()
        source = runtime / 'runtime.log'
        source.write_bytes(b'original public receipt\n')
        harness = SimpleNamespace(runtime=runtime, evidence=evidence, sensitive_canaries=[b'private-canary'])
        import docker_host_driver
        original = docker_host_driver.contains_canary
        def scan(streams, canaries):
            result = original(streams, canaries)
            source.write_bytes(b'private-canary')
            return result
        with patch.object(docker_host_driver, 'contains_canary', side_effect=scan):
            driver.collect_runtime_receipts(harness)
        self.assertEqual((evidence / 'runtime-receipts/runtime.log').read_bytes(), b'original public receipt\n')
        self.assertEqual(source.read_bytes(), b'private-canary')

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
        process.wait.assert_called_once_with(timeout=5)

    def test_cli_observer_reap_failure_remains_bounded_and_preserves_prefix(self):
        process = MagicMock()
        process.returncode = None
        process.wait.side_effect = subprocess.TimeoutExpired(["vz"], 5)
        original = subprocess.TimeoutExpired(["vz"], 1, output=b"retained", stderr=b"diagnostic")
        with patch.object(driver.subprocess, "Popen", return_value=process), \
             patch.object(driver, "collect_output", side_effect=original), \
             patch.object(driver.os, "killpg", side_effect=AssertionError("daemon group killed")), \
             self.assertRaises(subprocess.TimeoutExpired) as failure:
            driver.execute_observer(["vz"], timeout=1, max_stream_bytes=64, check=False)
        self.assertEqual(failure.exception.stdout, b"retained")
        self.assertEqual(failure.exception.stderr, b"diagnostic")
        process.wait.assert_called_once_with(timeout=5)
        process.__exit__.assert_not_called()

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

    def managed_fixture(self):
        environment = {'state': 'ready', 'project_id': 'prj_one', 'environment_id': 'env_one'}
        owner = dict(project_id='prj_one', environment_id='env_one', machine_id='mch_one')
        config = driver.machine_config_path(self.root, owner)
        config.mkdir(parents=True, mode=0o700)
        config.chmod(0o700)
        metadata = config.stat()
        driver.document(config.parent.parent / 'owner.json', {'owner': owner})
        (config.parent.parent / 'owner.json').chmod(0o600)
        claim = {'schema_version': 1, 'owner': owner,
                 'directory': {'device': metadata.st_dev, 'inode': metadata.st_ino}, 'nonce': 'lop_' + 'a' * 32}
        driver.document(config / 'vz-owner.json', claim)
        (config / 'vz-owner.json').chmod(0o600)
        descriptor = {'schema_version': 1, 'owner': owner, 'name': 'owned', 'endpoint': 'unix:///owned.sock',
                      'config_dir': str(config), 'incarnation_id': 'inc_one', 'incarnation_generation': 2}
        machine = {'state': 'ready', 'machine_id': 'mch_one', 'incarnation_id': 'inc_one', 'incarnation_generation': 2,
                   'negotiated_capabilities': {'capabilities': ['docker_engine', 'compose', 'buildx']},
                   'docker_context': descriptor}
        return environment, machine, config, claim

    def test_machine_config_derivation_binds_all_owners_and_not_incarnation(self):
        owner = dict(project_id='prj_one', environment_id='env_one', machine_id='mch_one')
        path = driver.machine_config_path('/owned/runtime', owner)
        self.assertEqual(str(path), '/owned/runtime/topology-machines/'
            'vzr1-other-machine_runtime_stor-15ba30d0036ca1f5a607c827baa784c6/data/docker-client')
        for key in owner:
            self.assertNotEqual(path, driver.machine_config_path('/owned/runtime', dict(owner, **{key: 'other'})))

    def test_managed_descriptor_accepts_only_derived_private_owned_directory(self):
        environment, machine, config, claim = self.managed_fixture()
        self.assertEqual(driver.managed_context_descriptor(environment, machine, self.root), machine['docker_context'])
        for key, value in (('config_dir', str(self.root / 'bootstrap')), ('config_dir', str(config) + '-sibling')):
            changed = copy.deepcopy(machine)
            changed['docker_context'][key] = value
            with self.assertRaises(ValueError):
                driver.managed_context_descriptor(environment, changed, self.root)
        config.chmod(0o755)
        with self.assertRaises(ValueError):
            driver.managed_context_descriptor(environment, machine, self.root)
        config.chmod(0o700)
        target = config.with_name('retained-config')
        config.rename(target)
        config.symlink_to(target, target_is_directory=True)
        with self.assertRaises(ValueError):
            driver.managed_context_descriptor(environment, machine, self.root)

    def test_managed_descriptor_rejects_foreign_claim_or_store_identity(self):
        environment, machine, config, claim = self.managed_fixture()
        for field, value in (('schema_version', 2), ('owner', dict(claim['owner'], machine_id='sibling')),
                             ('directory', {'device': claim['directory']['device'], 'inode': 0}), ('nonce', 'foreign')):
            (config / 'vz-owner.json').write_text(json.dumps(dict(claim, **{field: value})))
            with self.subTest(field=field), self.assertRaises(ValueError):
                driver.managed_context_descriptor(environment, machine, self.root)
        (config / 'vz-owner.json').write_text(json.dumps(claim))
        (config.parent.parent / 'owner.json').write_text(json.dumps({'owner': dict(claim['owner'], environment_id='foreign')}))
        with self.assertRaises(ValueError):
            driver.managed_context_descriptor(environment, machine, self.root)

    def test_runtime_collection_never_reads_or_publishes_managed_client_subtrees(self):
        import docker_host_driver as host
        runtime, evidence = self.root / 'runtime', self.root / 'evidence'
        runtime.mkdir()
        evidence.mkdir()
        canary = b'public-test-credential-must-not-enter-evidence'
        for directory in ('docker-client', '.docker-client.pending-owned'):
            selected = runtime / 'topology-machines/owned/data' / directory / 'nested'
            selected.mkdir(parents=True)
            for name in ('config.json', 'plugin.log', 'data.stderr'):
                (selected / name).write_bytes(canary)
        ordinary = runtime / 'ordinary/receipt.json'
        ordinary.parent.mkdir()
        ordinary.write_bytes(b'{"public":"receipt"}\n')
        regular = host.regular
        def guarded(path, limit):
            self.assertFalse(any(part == 'docker-client' or part.startswith('.docker-client.pending-')
                                 for part in Path(path).relative_to(runtime).parts), 'private client bytes read')
            return regular(path, limit)
        harness = type('HarnessFixture', (), {'runtime': runtime, 'evidence': evidence,
                                              'sensitive_canaries': [canary]})()
        with patch.object(host, 'regular', side_effect=guarded) as read:
            driver.collect_runtime_receipts(harness)
        read.assert_called_once_with(ordinary, 32 * 1024 * 1024)
        files = [path for path in evidence.rglob('*') if path.is_file()]
        self.assertEqual(files, [evidence / 'runtime-receipts/ordinary/receipt.json'])
        self.assertEqual(files[0].read_bytes(), ordinary.read_bytes())
        self.assertNotIn(canary, b''.join(path.read_bytes() for path in files))

    def test_scoped_docker_uses_descriptor_config_without_bootstrap_fallback(self):
        harness = object.__new__(driver.Harness)
        harness.config = self.root / 'bootstrap'
        harness.info = {'clients': {'docker': {'canonical': '/owned/docker'}}}
        harness.command = MagicMock()
        descriptor = {'name': 'owned', 'config_dir': str(self.root / 'machine')}
        harness.docker('info', descriptor, ['info'])
        self.assertEqual(harness.command.call_args.args[1],
                         ['docker', '--config', descriptor['config_dir'], '--context', 'owned', 'info'])
        harness.command.reset_mock()
        with self.assertRaises(KeyError):
            harness.docker('info', {'name': 'owned'}, ['info'])
        harness.command.assert_not_called()
        harness.docker('version', None, ['--version'])
        self.assertEqual(harness.command.call_args.args[1][2], harness.config)

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
