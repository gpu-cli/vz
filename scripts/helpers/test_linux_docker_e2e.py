"""Offline admission/ownership regressions, not physical Docker evidence."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_e2e as gate


class AdmissionTests(unittest.TestCase):
    def test_lifecycle_admission_requires_no_external_builder_and_scopes_fixture_option(self):
        common = [part for name in gate.startup.OPTIONS for part in ('--'+name, '/owned/value')]
        args = gate.arguments(['--suite', 'lifecycle', *common])
        self.assertIsNone(args.buildkit_archive)
        self.assertIsNone(args.container_fixture)
        selected = gate.arguments(['--suite', 'lifecycle', *common, '--container-fixture', '/owned/container'])
        self.assertEqual(selected.container_fixture, '/owned/container')
        with self.assertRaisesRegex(ValueError, 'Buildx suites'):
            gate.arguments(['--suite', 'lifecycle', *common, '--buildkit-archive', '/owned/archive'])
        for suite in ('compose', 'build', 'artifacts', 'parallel', 'ssh'):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, 'container-fixture'):
                gate.arguments(['--suite', suite, '--container-fixture', '/owned/container'])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            gate.arguments(['--suite', 'lifecycle', '--container-fixture=a', '--container-fixture=b'])

    def test_lifecycle_preflight_freezes_transitive_helpers_and_both_fixtures(self):
        args = types.SimpleNamespace(suite='lifecycle', fixture='/owned/base', image_input='/owned/pin',
            buildkit_archive=None, run_id='lifecycle-owned')
        with patch.object(gate.startup, 'preflight', return_value={'inputs': {}}), \
                patch.object(gate.startup, 'canonical', side_effect=Path), \
                patch.object(gate.startup, 'digest', side_effect=lambda path: str(path)), \
                patch.object(gate.image_input, 'load', return_value={}), \
                patch.object(gate, 'public_ca_input', return_value={}), \
                patch.object(gate.driver, 'tree_digest', side_effect=lambda path: 'tree:'+str(path)), \
                patch('linux_docker_buildkit_builder.preflight_archive') as archive:
            info = gate.preflight(args, require_host=False)
        archive.assert_not_called()
        self.assertNotIn('buildkit', info)
        self.assertEqual(info['scope'], gate.LIFECYCLE_SCOPE)
        self.assertEqual(info['fixture'], '/owned/base')
        selected = gate.REPO/'tests/fixtures/vz-0.4/docker-container-io'
        self.assertEqual(info['container_fixture'], str(selected))
        self.assertEqual(info['container_fixture_sha256'], 'tree:'+str(selected))
        for name in ('linux_docker_container_lifecycle.py', 'linux_docker_container_state.py',
                     'linux_docker_container_commands.py', 'linux_docker_container_fixture.py',
                     'linux_docker_container_exec.py', 'linux_docker_container_follow.py',
                     'linux_docker_interactive_capture.py', 'linux_docker_interactive_evidence.py',
                     'linux_docker_buildkit_shutdown.py', 'linux_docker_image_input.py',
                     'linux_docker_compose_evidence.py'):
            self.assertIn(str(gate.REPO/'scripts/helpers'/name), info['inputs'])
        for name in ('Dockerfile', 'README.md', 'contract.json', 'probe.py', 'test_probe.py'):
            self.assertIn(str(selected/name), info['inputs'])

    def test_lifecycle_fixture_rejection_prevents_runtime_dispatch(self):
        args = types.SimpleNamespace(suite='lifecycle', fixture='/owned/base', image_input='/owned/pin',
            buildkit_archive=None, run_id='lifecycle-owned', container_fixture='/foreign/fixture')
        with patch.object(gate.startup, 'preflight', return_value={'inputs': {}}), \
                patch.object(gate.startup, 'canonical', side_effect=Path), \
                patch.object(gate.startup, 'digest', return_value='hash'), \
                patch.object(gate.image_input, 'load', return_value={}), \
                patch.object(gate, 'public_ca_input', return_value={}), \
                patch.object(gate.driver, 'tree_digest', return_value='hash'), \
                patch('linux_docker_container_fixture.fixture_contract', side_effect=ValueError('fixture rejected')), \
                patch.object(gate, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'fixture rejected'): gate.preflight(args, require_host=False)
            run.assert_not_called()

    def test_artifact_helpers_are_hashed_only_for_explicit_artifacts_suite(self):
        names = ("linux_docker_artifact_stream.py", "linux_docker_artifact_layout.py",
                 "linux_docker_build_artifacts.py", "linux_docker_artifact_evidence.py")
        for suite, scope in (("compose", gate.SCOPE), ("build", gate.BUILD_SCOPE),
                             ("artifacts", gate.ARTIFACT_SCOPE), ("parallel", gate.PARALLEL_SCOPE)):
            with self.subTest(suite=suite):
                args = types.SimpleNamespace(suite=suite, fixture="/owned/fixture", image_input="/owned/pin",
                                             buildkit_archive="/owned/buildkit.tar", run_id="owned-run")
                with patch.object(gate.startup, "preflight", return_value={"inputs": {}}), \
                        patch.object(gate.startup, "canonical", side_effect=Path), \
                        patch.object(gate.startup, "digest", side_effect=lambda path: str(path)), \
                        patch.object(gate.image_input, "load", return_value={}), \
                        patch.object(gate, "public_ca_input", return_value={}), \
                        patch.object(gate.driver, "tree_digest", return_value="fixture-hash"), \
                        patch("linux_docker_buildkit_builder.preflight_archive", return_value={"archive": "exact"}) as archive:
                    info = gate.preflight(args, require_host=False)
                self.assertEqual(info["scope"], scope)
                for name in names:
                    self.assertEqual(str(gate.REPO / "scripts/helpers" / name) in info["inputs"], suite in {"artifacts", "parallel"})
                self.assertEqual("parallel_fixture" in info, suite == "parallel")
                if suite == "parallel":
                    self.assertEqual(info["parallel_fixture"], str(gate.REPO / "tests/fixtures/vz-0.4/docker-parallel"))
                    for name in ("linux_docker_build_parallel.py", "linux_docker_parallel_evidence.py", "linux_docker_parallel_health.py"):
                        self.assertIn(str(gate.REPO / "scripts/helpers" / name), info["inputs"])
                self.assertEqual("buildkit" in info, suite != "compose")
                self.assertEqual(archive.call_count, int(suite != "compose"))

    def test_public_ca_accepts_public_source_modes_but_not_tamper_or_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            bundle = root / "cacert.pem"
            bundle.write_bytes(b"public certificate fixture")
            bundle.chmod(0o644)
            path = root / "inputs.json"
            expected = {"bundle_sha256": driver.sha256(bundle.read_bytes()), "bundle_bytes": bundle.stat().st_size}
            path.write_text(json.dumps(expected))
            path.chmod(0o644)
            self.assertEqual(gate.public_ca_input(path), expected)
            bundle.write_bytes(b"tampered")
            with self.assertRaises(driver.Rejected):
                gate.public_ca_input(path)
            bundle.unlink()
            bundle.symlink_to(path)
            with self.assertRaises(OSError):
                gate.public_ca_input(path)

    def test_public_registry_requires_tls_no_mirrors_and_only_default_loopbacks(self):
        expected = {"InsecureRegistryCIDRs": ["::1/128", "127.0.0.0/8"], "Mirrors": [],
                    "IndexConfigs": {"docker.io": {"Name": "docker.io", "Mirrors": [], "Secure": True, "Official": True}}}
        gate.secure_registry_config(expected)
        for bad in ({"InsecureRegistryCIDRs": ["0.0.0.0/0", "::1/128"]},
                    {"InsecureRegistryCIDRs": []}, {"Mirrors": ["https://mirror.invalid"]},
                    {"extra": True}, {"IndexConfigs": {}}, {"IndexConfigs": {"docker.io": {
                        "Name": "docker.io", "Mirrors": [], "Secure": False, "Official": True}}}):
            with self.assertRaises(driver.Rejected):
                gate.secure_registry_config(expected | bad)

    def test_all_rejects_before_any_preflight_or_write(self):
        with patch.object(gate, "preflight") as preflight, patch.object(gate, "run") as run:
            with contextlib.redirect_stderr(io.StringIO()) as errors:
                self.assertEqual(gate.main(["--suite", "all"]), 2)
            self.assertIn("63-scenario", errors.getvalue())
            preflight.assert_not_called()
            run.assert_not_called()

    def test_duplicate_suite_rejected(self):
        with self.assertRaisesRegex(driver.Rejected, "duplicate"):
            gate.arguments(["--suite", "compose", "--suite=all"])

    def test_compose_requires_exact_inputs(self):
        with self.assertRaisesRegex(driver.Rejected, "release-dir"):
            gate.arguments(["--suite", "compose"])
        args = ["--suite", "compose"]
        for name in gate.startup.OPTIONS:
            args.extend(["--" + name, "/absolute/input"])
        self.assertEqual(gate.arguments(args).suite, "compose")
        with self.assertRaisesRegex(driver.Rejected, "run ID"):
            gate.arguments(args + ["--run-id", "../../foreign"])

    def test_build_requires_pinned_archive_and_compose_rejects_it(self):
        common = []
        for name in gate.startup.OPTIONS:
            common.extend(["--" + name, "/absolute/input"])
        for suite in ("build", "artifacts", "parallel"):
            with self.assertRaisesRegex(driver.Rejected, "buildkit-archive"):
                gate.arguments(["--suite", suite, *common])
            args = gate.arguments(["--suite", suite, *common, "--buildkit-archive", "/owned/buildkit.tar"])
            self.assertEqual(args.suite, suite)
            self.assertEqual(args.buildkit_archive, "/owned/buildkit.tar")
        with self.assertRaisesRegex(driver.Rejected, "buildkit-archive"):
            gate.arguments(["--suite", "compose", *common, "--buildkit-archive", "/owned/buildkit.tar"])
        with self.assertRaisesRegex(driver.Rejected, "duplicate"):
            gate.arguments(["--suite", "build", *common, "--buildkit-archive", "/owned/buildkit.tar",
                            "--buildkit-archive=/other/buildkit.tar"])

    def test_parallel_fixture_is_not_accepted_for_other_suites(self):
        for suite in ("build", "compose", "artifacts"):
            with self.assertRaisesRegex(driver.Rejected, "parallel-fixture"):
                gate.arguments(["--suite", suite, "--parallel-fixture", "/owned/parallel"])

    def test_ssh_inputs_are_explicit_and_unavailable_to_other_suites(self):
        with self.assertRaisesRegex(ValueError, "ssh-packages"):
            gate.arguments(["--suite", "ssh"])
        common = [part for name in gate.startup.OPTIONS for part in ("--" + name, "/owned/input")]
        args = gate.arguments(["--suite", "ssh", *common, "--ssh-packages", "/owned/packages",
                               "--buildkit-archive", "/owned/buildkit.tar"])
        self.assertEqual(args.suite, "ssh")
        for suite in ("compose", "build", "artifacts", "parallel"):
            for option in ("ssh-fixture", "ssh-packages", "ssh-gpgv"):
                with self.subTest(suite=suite, option=option), self.assertRaisesRegex(ValueError, "SSH options"):
                    gate.arguments(["--suite", suite, "--" + option, "/owned/value"])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            gate.arguments(["--suite", "ssh", "--ssh-packages", "/owned/a", "--ssh-packages=/owned/b"])

    def test_ssh_preflight_freezes_inputs_without_dispatched_verification(self):
        rows = [{"filename": name, "sha256": "a" * 64} for name in ("keyring", "release", "index", "package", "source")]
        pin = {"base": {"keyring": rows[0]}, "release": rows[1], "packages_index": rows[2],
               "packages": [rows[3]], "source_proofs": [rows[4]]}
        args = types.SimpleNamespace(suite="ssh", fixture="/owned/fixture", image_input="/owned/pin",
            buildkit_archive="/owned/buildkit.tar", run_id="ssh-owned", ssh_packages="/owned/packages", ssh_gpgv="/owned/gpgv")
        with patch.object(gate.startup, "preflight", return_value={"inputs": {}}), \
             patch.object(gate.startup, "canonical", side_effect=lambda value, **kwargs: Path(value)), \
             patch.object(gate.startup, "digest", return_value="a" * 64), \
             patch.object(gate.image_input, "load", return_value={}), \
             patch.object(gate, "public_ca_input", return_value={}), \
             patch.object(gate.driver, "tree_digest", return_value="a" * 64), \
             patch("linux_docker_buildkit_builder.preflight_archive", return_value={}), \
             patch("linux_docker_build_ssh.fixture_contract", return_value={}), \
             patch("linux_docker_ssh_input.load", return_value=pin), \
             patch("linux_docker_ssh_input.read_input") as read, \
             patch("linux_docker_ssh_input.verify") as verify, \
             patch("linux_docker_ssh_agent.tool_inputs", return_value={"ssh-agent": {"path": "/owned/ssh-agent", "sha256": "a" * 64}}):
            info = gate.preflight(args, require_host=False)
        self.assertEqual(info["scope"], gate.SSH_SCOPE)
        self.assertEqual(read.call_count, 5)
        verify.assert_not_called()
        for row in rows:
            self.assertIn("/owned/packages/" + row["filename"], info["inputs"])
        for name in ("linux_docker_ssh_server.py", "linux_docker_ssh_evidence.py", "linux_docker_ssh_cache.py",
                     "linux_docker_ssh_cache_capture.py", "linux_docker_ssh_agent.py", "linux_docker_build_ssh.py",
                     "linux_docker_parallel_evidence.py"):
            self.assertIn(str(gate.REPO / "scripts/helpers" / name), info["inputs"])

    def test_python_repo_aliases_are_exact(self):
        pin = {"reference": "docker.io/library/python@sha256:" + "a" * 64, "id": "sha256:" + "a" * 64,
               "config_digest": "sha256:" + "b" * 64,
               "manifest_descriptor": {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:" + "a" * 64, "size": 1754},
               "image_config": {"Env": ["PYTHON_VERSION=3.12.14"], "Cmd": ["python3"]},
               "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "c" * 64]},
               "platform_detail": {"os": "linux", "architecture": "arm64", "variant": "v8"}}
        row = {"Id": pin["id"], "Os": "linux", "Architecture": "arm64", "Variant": "v8",
               "Descriptor": pin["manifest_descriptor"], "Config": pin["image_config"],
               "RootFS": {"Type": "layers", "Layers": pin["rootfs"]["diff_ids"]},
               "RepoDigests": ["python@sha256:" + "a" * 64]}
        self.assertEqual(gate.image_matches(row, pin), row["RepoDigests"][0])
        for bad in ({"RepoDigests": ["attacker/python@sha256:" + "a" * 64]},
                    {"RepoDigests": ["python@sha256:" + "c" * 64]}, {"Architecture": "amd64"},
                    {"Id": pin["config_digest"]}, {"Id": "sha256:" + "c" * 64},
                    {"Descriptor": pin["manifest_descriptor"] | {"size": 1}},
                    {"Descriptor": pin["manifest_descriptor"] | {"digest": pin["config_digest"]}},
                    {"Config": pin["image_config"] | {"Env": []}},
                    {"Config": pin["image_config"] | {"Entrypoint": ["foreign"]}},
                    {"RootFS": {"Type": "layers", "Layers": []}}):
            with self.assertRaises(driver.Rejected):
                gate.image_matches(row | bad, pin)

    def test_embedded_builder_exact_single_running_machine(self):
        raw = b"Name: private-machine\nDriver: docker\n\nNodes:\nName: private-machine\nEndpoint: private-machine\nStatus: running\n"
        gate.embedded_builder(raw, "private-machine")
        for bad in (raw.replace(b"Driver: docker", b"Driver: docker-container"),
                    raw.replace(b"Endpoint: private-machine", b"Endpoint: default"),
                    raw.replace(b"Status: running", b"Status: stopped"), raw + b"Name: second-node\n",
                    raw + b"Error: cannot connect\n"):
            with self.assertRaises(driver.Rejected):
                gate.embedded_builder(bad, "private-machine")


class ResultScopeTests(unittest.TestCase):
    def test_lifecycle_end_rehash_cannot_promote_changed_fixture_or_full_contract(self):
        info = {'scope': gate.LIFECYCLE_SCOPE, 'suite': 'lifecycle', 'inputs': {},
                'fixture': '/owned/base', 'fixture_sha256': 'base-hash',
                'container_fixture': '/owned/container', 'container_fixture_sha256': 'container-hash'}
        for changed in (False, True):
            with self.subTest(changed=changed):
                harness = types.SimpleNamespace(evidence=Path('/owned/evidence'), root=Path('/owned/root'),
                    staged_inputs={}, monitor=None, stage=Mock(), scenario=Mock(return_value={}),
                    remove_owned=Mock(), cleanup=Mock(return_value={}))
                with patch.object(gate, 'ComposeHarness', return_value=harness), \
                        patch.object(gate.os, 'umask'), patch.object(gate.startup, 'document'), \
                        patch.object(gate.startup, 'collect_runtime_receipts'), \
                        patch.object(gate.startup, 'checksum_evidence'), \
                        patch.object(gate.driver, 'tree_digest', side_effect=['base-hash', 'changed' if changed else 'container-hash']) as tree, \
                        patch('linux_docker_container_fixture.fixture_contract') as contract, \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    code = gate.run(info)
                result = json.loads(output.getvalue())
                contract.assert_called_once_with(Path('/owned/container'))
                self.assertEqual(tree.call_args_list, [unittest.mock.call(Path('/owned/base')),
                                                     unittest.mock.call(Path('/owned/container'))])
                self.assertEqual(code, int(changed))
                self.assertFalse(result['docker_parity_certified'])
                self.assertFalse(result['aggregate_release_certified'])
                self.assertEqual(result['release_scenarios_passed'], [])
                self.assertEqual(result['outcome'], 'failed' if changed else 'passed_dev_installed_lifecycle_slice')

    def test_cleanup_reports_retained_machine_disks_not_removed_builder_cache(self):
        info = {"scope": gate.BUILD_SCOPE, "suite": "build", "inputs": {},
                "fixture": "/owned/fixture", "fixture_sha256": "fixture-digest"}
        cleanup = {"positive_stop_all": True, "daemon_graceful_shutdown_observed": True,
                   "daily_default_unchanged": True, "isolated_default_unchanged": True}
        for failed_removal in (False, True):
            with self.subTest(failed_removal=failed_removal):
                harness = types.SimpleNamespace(
                    evidence=Path("/owned/evidence"), root=Path("/owned/root"),
                    staged_inputs={}, monitor=None, stage=Mock(), scenario=Mock(return_value={}),
                    remove_owned=Mock(side_effect=RuntimeError("removal unproven") if failed_removal else None),
                    cleanup=Mock(return_value=cleanup))
                with patch.object(gate, "ComposeHarness", return_value=harness), \
                        patch.object(gate.os, "umask"), \
                        patch.object(gate.startup, "document"), \
                        patch.object(gate.startup, "collect_runtime_receipts"), \
                        patch.object(gate.startup, "checksum_evidence"), \
                        patch.object(gate.driver, "tree_digest", return_value="fixture-digest"), \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    code = gate.run(info)
                result = json.loads(output.getvalue())
                self.assertFalse(result["docker_parity_certified"])
                self.assertFalse(result["aggregate_release_certified"])
                if failed_removal:
                    self.assertEqual(code, 1)
                    harness.cleanup.assert_not_called()
                    self.assertNotIn("cleanup", result)
                else:
                    self.assertEqual(code, 0)
                    harness.remove_owned.assert_called_once_with()
                    harness.cleanup.assert_called_once_with()
                    self.assertEqual(result["cleanup"], cleanup | {
                        "owned_workload_objects_removed": True,
                        "retained_stopped_machine_disks_and_contexts": True,
                        "delete_certified": False})


class PrePullTrustTests(unittest.TestCase):
    def ca_harness(self, replies):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.cli = Path("/owned/bin/vz")
        harness.evidence = Path("/owned/evidence")
        harness.info = {"public_ca": {"bundle_sha256": "a" * 64}}
        project = Path("/owned/project")
        environments = [{"environment_id": "environment-" + str(index), "machines": [
            {"machine_id": f"machine-{index}-{sibling}", "name": "worker-" + str(sibling)}
            for sibling in range(2)]} for index in range(2)]
        contexts = [{field: field + "-" + str(index) for field in ("name", "endpoint", "engine_id")}
                    for index in range(4)]
        harness.project = Mock(return_value=project)
        harness.up = Mock(side_effect=environments)
        harness.daemon_fingerprint = Mock(return_value="owned-daemon")
        harness.inspect = Mock(side_effect=[contexts[:2], contexts[2:]])
        harness.command = Mock(side_effect=replies)
        harness.sentinel = Mock(side_effect=RuntimeError("reached first sentinel"))
        harness.mutate = Mock()
        return harness, project, environments

    def run_ca_admission(self, harness):
        # Only bypass already separately tested topology/proof admission. Execute
        # the real orchestration and CA guards, stopping before any workload.
        with patch.object(gate.startup, "exact_developer_topology"), \
                patch.object(gate.startup, "document"), \
                patch.object(gate, "authenticated_proof", return_value=({}, {})):
            harness.scenario()

    def test_all_four_exact_machine_ca_observations_precede_first_sentinel(self):
        reply = (("a" * 64 + "  /etc/vz/ca-certificates.crt\n").encode(), b"", 0)
        harness, project, environments = self.ca_harness([reply] * 4)
        with self.assertRaisesRegex(RuntimeError, "reached first sentinel"):
            self.run_ca_admission(harness)
        self.assertEqual(harness.command.call_count, 4)
        for call, (environment, machine) in zip(harness.command.call_args_list,
                [(environment, machine) for environment in environments for machine in environment["machines"]]):
            self.assertEqual(call.args, ("public-machine-ca-hash", [harness.cli, "exec", "--environment",
                environment["environment_id"], "--machine", machine["name"], "--no-stdin", "--timeout", "30",
                "--", "/bin/busybox", "sha256sum", "/etc/vz/ca-certificates.crt"]))
            self.assertEqual(call.kwargs, {"cwd": project})
        harness.sentinel.assert_called_once()
        harness.mutate.assert_not_called()

    def test_any_machine_bad_ca_observation_prevents_every_sentinel(self):
        raw = ("a" * 64 + "  /etc/vz/ca-certificates.crt\n").encode()
        good = (raw, b"", 0)
        for index in range(4):
            for bad in ((raw.replace(b"a", b"b", 1), b"", 0),
                        (raw.replace(b"ca-certificates.crt", b"other.crt"), b"", 0),
                        (raw + b"extra output\n", b"", 0), (raw, b"warning\n", 0)):
                with self.subTest(machine=index, reply=bad):
                    harness, _, _ = self.ca_harness([good] * index + [bad])
                    with self.assertRaisesRegex(driver.Rejected, "actual Machine public CA bytes"):
                        self.run_ca_admission(harness)
                    self.assertEqual(harness.command.call_count, index + 1)
                    harness.sentinel.assert_not_called()
                    harness.mutate.assert_not_called()

    def test_wrong_engine_or_insecure_registry_prevents_pull_and_resource_admission(self):
        policy = {"InsecureRegistryCIDRs": ["::1/128", "127.0.0.0/8"], "Mirrors": [],
                  "IndexConfigs": {"docker.io": {"Name": "docker.io", "Mirrors": [], "Secure": True, "Official": True}}}
        descriptor = {"engine_id": "exact-engine", "name": "exact-context"}
        for engine, stderr in (({"ID": "foreign", "RegistryConfig": policy}, b""),
                ({"ID": "exact-engine", "RegistryConfig": policy | {"Mirrors": ["https://mirror.invalid"]}}, b""),
                ({"ID": "exact-engine", "RegistryConfig": policy | {"InsecureRegistryCIDRs": ["0.0.0.0/0"]}}, b""),
                ({"ID": "exact-engine", "RegistryConfig": policy}, b"warning\n")):
            with self.subTest(engine=engine, stderr=stderr):
                harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
                harness.docker = Mock(return_value=(json.dumps(engine).encode(), stderr, 0))
                harness.mutate, harness.exact_absent = Mock(), Mock()
                harness.owned = []
                with self.assertRaises(driver.Rejected):
                    harness.prepare_image(descriptor)
                harness.docker.assert_called_once_with("public-registry-policy", descriptor,
                                                       ["info", "--format", "{{json .}}"])
                harness.mutate.assert_not_called()
                harness.exact_absent.assert_not_called()
                self.assertEqual(harness.owned, [])


class BuildDispatchTests(unittest.TestCase):
    def test_lifecycle_branch_calls_only_lifecycle_and_monitors_every_machine(self):
        self.check_owned_orchestrator('lifecycle', 'linux_docker_container_lifecycle')

    def test_artifacts_branch_calls_only_artifact_orchestrator_and_monitors_every_machine(self):
        self.check_owned_orchestrator("artifacts", "linux_docker_build_artifacts")

    def test_parallel_branch_calls_only_parallel_orchestrator_and_monitors_every_machine(self):
        self.check_owned_orchestrator("parallel", "linux_docker_build_parallel")

    def check_owned_orchestrator(self, suite, module_name):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.info = {"suite": suite, "public_ca": {"bundle_sha256": "a" * 64}}
        harness.cli, harness.evidence = Path("/owned/bin/vz"), Path("/owned/evidence")
        contexts = [{"name": "context-" + str(i), "endpoint": "endpoint-" + str(i),
                     "engine_id": "engine-" + str(i)} for i in range(4)]
        environments = [{"environment_id": "env-" + str(i), "machines": [
            {"machine_id": "machine-" + str(2*i+j), "name": "worker-" + str(j),
             "docker_context": contexts[2*i+j]} for j in range(2)]} for i in range(2)]
        project = Path("/owned/project")
        harness.project, harness.up = Mock(return_value=project), Mock(side_effect=environments)
        harness.daemon_fingerprint = Mock(return_value="daemon")
        harness.inspect = Mock(side_effect=[contexts[:2], contexts[2:], contexts[:2], contexts[2:]])
        harness.status = Mock()
        harness.command = Mock(return_value=(("a" * 64 + "  /etc/vz/ca-certificates.crt\n").encode(), b"", 0))
        harness.sentinel = Mock(side_effect=lambda descriptor: {"descriptor": descriptor})
        harness.prepare_image = Mock(return_value={"exact": "images"})
        harness.driver_inputs, harness.validate_driver = Mock(), Mock()
        monitor = Mock()
        monitor.summary.return_value = {"samples": "observed"}
        observations = [{"operation": i} for i in range(3)]
        module = types.SimpleNamespace(run_machine=Mock(side_effect=observations))
        with patch.dict("sys.modules", {module_name: module}), \
                patch.object(gate, "SentinelMonitor", return_value=monitor), \
                patch.object(gate.startup, "exact_developer_topology"), \
                patch.object(gate.startup, "document"), \
                patch.object(gate, "authenticated_proof", return_value=({"scope": "exact"}, {"proof": "exact"})), \
                patch.object(gate.time, "time_ns", side_effect=range(10, 16)), \
                patch.object(gate.driver, "Driver") as selected:
            result = harness.scenario()
        self.assertEqual(result["machine_slices"], observations)
        self.assertEqual(module.run_machine.call_args_list, [unittest.mock.call(
            harness, contexts[i], {"scope": "exact"}, {"proof": "exact"}, {"exact": "images"}, i) for i in range(3)])
        self.assertEqual(monitor.check_interval.call_args_list, [unittest.mock.call(
            10+2*i, 11+2*i, contexts[i]["name"]) for i in range(3)])
        monitor.start.assert_called_once_with()
        monitor.stop.assert_called_once_with()
        harness.driver_inputs.assert_not_called()
        harness.validate_driver.assert_not_called()
        selected.assert_not_called()
        self.assertEqual(harness.status.call_count, 2)

    def test_builder_owner_is_retained_before_prepare_effects(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.info = {"suite": "build"}
        harness.builders = []
        harness.builder_by_owner_role = {}
        descriptor = {"name": "private", "owner": {"machine_id": "exact"}}
        builder = Mock()
        def prepare():
            self.assertEqual(harness.builders, [builder])
            self.assertIs(harness.builder_by_owner_role[harness.builder_key(descriptor, "source")], builder)
            raise driver.Rejected("partial builder mutation")
        builder.prepare.side_effect = prepare
        module = types.SimpleNamespace(Builder=Mock(return_value=builder))
        with patch.dict("sys.modules", {"linux_docker_buildkit_builder": module}), \
                patch.object(gate, "input_mapping", return_value={"scope": "exact"}):
            with self.assertRaisesRegex(driver.Rejected, "partial builder"):
                harness.driver_inputs(descriptor, {}, {}, {})
        module.Builder.assert_called_once_with(harness, descriptor, role="source")
        self.assertEqual(harness.builders, [builder])

    def test_role_lookup_and_keep_probe_are_exact_not_list_position(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.builders, harness.builder_by_owner_role, harness.keep_proofs_verified = [], {}, []
        descriptors = [{"owner": {"project_id": "p", "environment_id": "e", "machine_id": name},
                        "name": name, "engine_id": name + "-engine"} for name in ("first", "second")]
        created = []
        def construct(harness, descriptor, role):
            builder = Mock(descriptor=descriptor, mapping={"name": descriptor["name"] + role})
            created.append(builder)
            return builder
        module = types.SimpleNamespace(Builder=Mock(side_effect=construct))
        keep = types.SimpleNamespace(run=Mock())
        with patch.dict("sys.modules", {"linux_docker_buildkit_builder": module,
                                         "linux_docker_buildkit_keep": keep}):
            for descriptor in descriptors:
                for role in ("source", "cold-control", "importer"):
                    selected = harness.prepare_builder(descriptor, role=role)
                    self.assertIs(harness.get_builder(descriptor, role), selected)
                    selected.prepare.assert_called_once_with()
            self.assertIs(harness.get_builder(descriptors[0]), created[0])
            self.assertEqual(keep.run.call_args_list, [unittest.mock.call(created[0]), unittest.mock.call(created[3])])
            self.assertEqual(harness.keep_proofs_verified, [True, True])
            for role in ("source", "cold-control", "importer"):
                with self.assertRaisesRegex(driver.Rejected, "already registered"):
                    harness.prepare_builder(descriptors[0], role=role)
            self.assertEqual(module.Builder.call_count, 6)
            with self.assertRaisesRegex(driver.Rejected, "unknown builder role"):
                harness.prepare_builder(descriptors[0], role="default")
            with self.assertRaisesRegex(driver.Rejected, "descriptor changed"):
                harness.get_builder(descriptors[0] | {"engine_id": "foreign"})
            with self.assertRaisesRegex(driver.Rejected, "not prepared"):
                harness.get_builder(descriptors[0] | {"owner": {"machine_id": "foreign"}})
            descriptors[0]["endpoint"] = "rerouted-in-place"
            with self.assertRaisesRegex(driver.Rejected, "descriptor changed"):
                harness.get_builder(descriptors[0])
            self.assertNotIn("endpoint", created[0].descriptor)

    def test_build_driver_mapping_remains_exact_source_mapping(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.info = {"suite": "build"}
        mapping = {"name": "owned", "node": "owned-node", "container_id": "c" * 64,
                   "image_id": "sha256:" + "a" * 64}
        harness.prepare_builder = Mock(return_value=Mock(mapping=mapping))
        descriptor = {"owner": {"machine_id": "exact"}}
        with patch.object(gate, "input_mapping", return_value={"scope": "exact"}):
            self.assertEqual(harness.driver_inputs(descriptor, {}, {}, {}),
                             {"scope": "exact", "builder": mapping})
        harness.prepare_builder.assert_called_once_with(descriptor)

    def test_failed_keep_proof_retains_owner_and_blocks_cleanup(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.builders, harness.builder_by_owner_role, harness.keep_proofs_verified = [], {}, []
        descriptor = {"owner": {"machine_id": "exact"}}
        builder = Mock(descriptor=descriptor)
        with patch.dict("sys.modules", {
                "linux_docker_buildkit_builder": types.SimpleNamespace(Builder=Mock(return_value=builder)),
                "linux_docker_buildkit_keep": types.SimpleNamespace(run=Mock(side_effect=RuntimeError("keep failed")))}):
            with self.assertRaisesRegex(RuntimeError, "keep failed"):
                harness.prepare_builder(descriptor)
        self.assertIs(harness.get_builder(descriptor), builder)
        self.assertEqual(harness.keep_proofs_verified, [False])
        with self.assertRaisesRegex(driver.Rejected, "keep"):
            harness.remove_owned()
        builder.remove_owned.assert_not_called()

    def test_compose_never_provisions_build_builder(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.info = {"suite": "compose"}
        harness.builders = []
        with patch.object(gate, "input_mapping", return_value={"scope": "exact"}):
            self.assertEqual(harness.driver_inputs({}, {}, {}, {}), {"scope": "exact"})
        self.assertEqual(harness.builders, [])

    def test_replay_dispatch_uses_only_selected_suite_validator(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        build = types.SimpleNamespace(validate=Mock(return_value={"build": True}))
        compose = types.SimpleNamespace(validate=Mock(return_value={"compose": True}))
        with patch.dict("sys.modules", {"linux_docker_build_evidence": build,
                                         "linux_docker_compose_evidence": compose}):
            for suite in ("build", "compose"):
                harness.info = {"suite": suite}
                self.assertEqual(harness.validate_driver(Path("/owned"), {"exact": True}), {suite: True})
        for module in (build, compose):
            module.validate.assert_called_once_with(Path("/owned"), {"exact": True})

    def test_exact_engine_secure_policy_reaches_only_pinned_pull(self):
        policy = {"InsecureRegistryCIDRs": ["::1/128", "127.0.0.0/8"], "Mirrors": [],
                  "IndexConfigs": {"docker.io": {"Name": "docker.io", "Mirrors": [], "Secure": True, "Official": True}}}
        descriptor = {"engine_id": "exact-engine", "name": "exact-context"}
        pin = {"id": "sha256:" + "b" * 64, "reference": "docker.io/library/python@sha256:" + "c" * 64}
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.info, harness.owned = {"python_image": pin}, []
        harness.docker = Mock(side_effect=[
            (json.dumps({"ID": "exact-engine", "RegistryConfig": policy}).encode(), b"", 0), (b"", b"", 0)])
        harness.exact_absent = Mock()
        harness.mutate = Mock(side_effect=RuntimeError("reached pinned pull"))
        with self.assertRaisesRegex(RuntimeError, "reached pinned pull"):
            harness.prepare_image(descriptor)
        self.assertEqual(harness.docker.call_count, 2)
        harness.mutate.assert_called_once_with("python-pull", descriptor,
            ["pull", "--platform", "linux/arm64", pin["reference"]], timeout=300)


class UncertaintyTests(unittest.TestCase):
    def test_live_follower_and_pending_capture_prevent_cleanup_despite_verified_flags(self):
        for live, pending in ((True, []), (False, [object()])):
            h = self.harness()
            h.drivers = [types.SimpleNamespace(follow_thread=Mock(is_alive=Mock(return_value=live)),
                record=types.SimpleNamespace(receipts=[], pending_interactions=pending))]
            h.driver_cleanup_verified = [True]
            with self.subTest(live=live), self.assertRaisesRegex(ValueError, 'prevents cleanup'):
                h.remove_owned()
            h.docker.assert_not_called()

    def test_joined_follower_and_reaped_interactions_allow_cleanup_guard(self):
        h = self.harness()
        h.drivers = [types.SimpleNamespace(follow_thread=Mock(is_alive=Mock(return_value=False)),
            record=types.SimpleNamespace(receipts=[], pending_interactions=[]))]
        h.driver_cleanup_verified = [True]
        h.assert_certain()

    def harness(self):
        h = gate.ComposeHarness.__new__(gate.ComposeHarness)
        h.effects_uncertain = False
        h.ssh_cache_requests = []
        h.ssh_cache_proofs = []
        h.ssh_cache_captures = []
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        h.evidence = Path(temporary.name)
        h.mutations = []
        h.record = Mock(receipts=[], pending_interactions=[])
        h.monitor = None
        h.drivers = []
        h.driver_cleanup_verified = []
        h.owned = []
        h.docker = Mock(return_value=(b"", b"", 0))
        return h

    def test_failed_mutation_prevents_later_dispatch_and_cleanup(self):
        h = self.harness()
        h.docker.side_effect = driver.Rejected("normal nonzero mutation")
        with self.assertRaises(driver.Rejected):
            h.mutate("build", {"name": "private", "owner": {}}, ["buildx", "build"])
        self.assertTrue(h.effects_uncertain)
        receipt = json.loads((h.evidence / "mutation-001.result.json").read_text())
        self.assertTrue(receipt["effects_uncertain"])
        self.assertIn("normal nonzero", receipt["error"])
        with self.assertRaisesRegex(driver.Rejected, "previous mutation"):
            h.mutate("rm", {}, ["image", "rm", "owned"])
        with self.assertRaisesRegex(driver.Rejected, "uncertain"):
            h.remove_owned()
        self.assertEqual(h.docker.call_count, 1)

    def test_parent_and_monitor_unknown_effects_prevent_cleanup(self):
        for source in ("parent", "monitor"):
            h = self.harness()
            unknown = Mock(receipts=[{"effects_uncertain": True}], pending_interactions=[])
            if source == "parent":
                h.record = unknown
            else:
                h.monitor = Mock(record=unknown, thread=Mock(is_alive=Mock(return_value=False)))
            with self.assertRaisesRegex(driver.Rejected, "uncertain"):
                h.remove_owned()
            h.docker.assert_not_called()

    def test_driver_uncertainty_prevents_cleanup(self):
        h = self.harness()
        h.drivers = [Mock(follow_thread=None, record=Mock(receipts=[{"effects_uncertain": True}], pending_interactions=[]))]
        h.driver_cleanup_verified = [True]
        with self.assertRaisesRegex(driver.Rejected, "uncertain"):
            h.remove_owned()
        h.docker.assert_not_called()

    def test_unverified_or_missing_driver_cleanup_blocks_parent_removal(self):
        for count, flags in ((1, []), (1, [False]), (1, [True, True]),
                             (2, [True]), (2, [True, False]), (2, [False, True])):
            with self.subTest(drivers=count, verified=flags):
                h = self.harness()
                h.drivers = [Mock(follow_thread=None, record=Mock(receipts=[{"effects_uncertain": False}], pending_interactions=[])) for _ in range(count)]
                h.driver_cleanup_verified = flags
                # An owned object ensures the test catches any cleanup dispatch,
                # not merely successful return from an empty ownership loop.
                h.owned = [{"descriptor": {"name": "exact-context"}, "token": "owned",
                            "tag": "owned:fixture", "image_id": "sha256:" + "a" * 64}]
                h.mutate, h.exact_absent = Mock(), Mock()
                with self.assertRaisesRegex(driver.Rejected, "Docker fixture cleanup lacks successful independent replay"):
                    h.remove_owned()
                h.docker.assert_not_called()
                h.mutate.assert_not_called()
                h.exact_absent.assert_not_called()

    def test_all_driver_cleanup_proofs_allow_exact_owned_parent_removal(self):
        h = self.harness()
        h.drivers = [Mock(follow_thread=None, record=Mock(receipts=[{"effects_uncertain": False}], pending_interactions=[])) for _ in range(2)]
        h.driver_cleanup_verified = [True, True]
        descriptor = {"name": "exact-context"}
        image_id = "sha256:" + "a" * 64
        h.owned = [{"descriptor": descriptor, "token": "owned", "tag": "owned:fixture", "image_id": image_id}]
        h.docker.return_value = (json.dumps([{"Id": image_id, "Config": {"Labels": {gate.LABEL: "owned"}}}]).encode(), b"", 0)
        h.mutate, h.exact_absent = Mock(), Mock()
        h.remove_owned()
        h.docker.assert_called_once_with("owned-image-check", descriptor, ["image", "inspect", "owned:fixture"])
        h.mutate.assert_called_once_with("owned-image-remove", descriptor, ["image", "rm", "owned:fixture"])
        h.exact_absent.assert_called_once_with(descriptor, "image", "owned:fixture")

    def test_builders_remove_in_reverse_order_only_after_replay_admission(self):
        h = self.harness()
        calls = []
        h.builders = [Mock(remove_owned=Mock(side_effect=lambda: calls.append("first"))),
                      Mock(remove_owned=Mock(side_effect=lambda: calls.append("second")))]
        h.drivers = [Mock(follow_thread=None, record=Mock(receipts=[{"effects_uncertain": False}], pending_interactions=[]))]
        h.driver_cleanup_verified = [False]
        with self.assertRaisesRegex(driver.Rejected, "independent replay"):
            h.remove_owned()
        self.assertEqual(calls, [])
        h.driver_cleanup_verified = [True]
        h.remove_owned()
        self.assertEqual(calls, ["second", "first"])

    def test_builder_cleanup_uncertainty_stops_later_owned_removal(self):
        h = self.harness()
        def uncertain():
            h.effects_uncertain = True
        first = Mock()
        h.builders = [first, Mock(remove_owned=Mock(side_effect=uncertain))]
        with self.assertRaisesRegex(driver.Rejected, "uncertain"):
            h.remove_owned()
        first.remove_owned.assert_not_called()
        h.docker.assert_not_called()

    def cache_cleanup(self):
        h = self.harness()
        h.root = h.evidence / 'private-root'
        calls = []
        stopped, stop_proof = {'Id': 'owned-builder'}, {'container_id': 'owned-builder', 'signal': 'SIGTERM'}
        owner = {'descriptor': {'owner': {'machine_id': 'owned-machine'}}, 'role': 'source'}
        result = {'owner': copy.deepcopy(owner), 'normal_stop': copy.deepcopy(stop_proof),
                  'scan': {'complete': True}, 'capture': {'owned_process_reaped': True,
                    'capture_complete': True, 'archive_published': True, 'effects_uncertain': False},
                  'guard_receipts_complete': True, 'builder_restarted': False}
        capture = types.SimpleNamespace(owner=owner, pending_process=None, run=Mock())
        def run(observed, receipt):
            self.assertEqual(observed, stopped)
            self.assertEqual(receipt, stop_proof)
            # Registration must precede dispatch, including an eventual throw.
            self.assertEqual(h.ssh_cache_captures, [capture])
            calls.append('capture')
            return result
        capture.run.side_effect = run
        def remove(*, before_remove):
            calls.append('positive-stop')
            accepted = before_remove(stopped, stop_proof)
            self.assertEqual(accepted, result)
            self.assertEqual(h.ssh_cache_proofs, [result])
            calls.append('builder-delete')
        builder = Mock(remove_owned=Mock(side_effect=remove))
        h.builders = [builder]
        h.ssh_cache_requests = [{'builder': builder, 'canaries': (b'private-test-canary',), 'index': 2}]
        descriptor = {'name': 'owned-context'}
        h.owned = [{'descriptor': descriptor, 'token': 'owned', 'tag': 'owned:fixture', 'image_id': 'sha256:'+'a'*64}]
        h.docker.return_value = (json.dumps([{'Id': h.owned[0]['image_id'], 'Config': {'Labels': {gate.LABEL: 'owned'}}}]).encode(), b'', 0)
        h.mutate = Mock(side_effect=lambda *args: calls.append('ordinary-delete'))
        h.exact_absent = Mock()
        return h, builder, capture, result, calls

    def test_ssh_stopped_cache_accepted_before_builder_and_ordinary_deletion(self):
        h, builder, capture, result, calls = self.cache_cleanup()
        with patch('linux_docker_ssh_cache_capture.Capture', return_value=capture) as create:
            h.remove_owned()
        self.assertEqual(calls, ['positive-stop', 'capture', 'builder-delete', 'ordinary-delete'])
        create.assert_called_once_with(builder, (b'private-test-canary',),
            h.root/'ssh-cache-private-2', h.evidence/'ssh-cache-2')
        self.assertEqual(h.ssh_cache_proofs, [result])
        self.assertEqual(h.ssh_cache_captures, [capture])

    def test_ssh_stopped_cache_incomplete_or_foreign_result_prevents_any_deletion(self):
        mutations = [('owner',), ('normal_stop',), ('scan', 'complete'),
                     ('capture', 'owned_process_reaped'), ('capture', 'capture_complete'),
                     ('capture', 'archive_published'), ('capture', 'effects_uncertain'),
                     ('guard_receipts_complete',), ('builder_restarted',)]
        for keys in mutations:
            with self.subTest(keys=keys):
                h, _, capture, result, calls = self.cache_cleanup()
                if keys == ('owner',): result['owner']['descriptor']['owner']['machine_id'] = 'foreign'
                elif keys == ('normal_stop',): result['normal_stop']['container_id'] = 'foreign'
                elif len(keys) == 1: result[keys[0]] = not result[keys[0]]
                else: result[keys[0]][keys[1]] = not result[keys[0]][keys[1]]
                with patch('linux_docker_ssh_cache_capture.Capture', return_value=capture):
                    with self.assertRaises(ValueError): h.remove_owned()
                self.assertEqual(calls, ['positive-stop', 'capture'])
                self.assertEqual(h.ssh_cache_proofs, [])
                self.assertEqual(h.ssh_cache_captures, [capture])
                h.mutate.assert_not_called(); h.docker.assert_not_called()

    def test_ssh_cache_capture_failure_retains_instance_and_pending_process_handle(self):
        h, _, capture, _, calls = self.cache_cleanup()
        pending = object()
        error = RuntimeError('bounded capture failure')
        def failed(stopped, proof):
            self.assertEqual(h.ssh_cache_captures, [capture])
            capture.pending_process = pending
            error.capture_pending_process = pending
            calls.append('capture-failed')
            raise error
        capture.run.side_effect = failed
        with patch('linux_docker_ssh_cache_capture.Capture', return_value=capture):
            with self.assertRaises(RuntimeError) as raised: h.remove_owned()
        self.assertIs(raised.exception, error)
        self.assertEqual(calls, ['positive-stop', 'capture-failed'])
        self.assertIs(h.ssh_cache_captures[0].pending_process, pending)
        self.assertEqual(h.ssh_cache_proofs, [])
        h.mutate.assert_not_called(); h.docker.assert_not_called()

    def test_liveness_requires_every_neighbor_during_exact_interval(self):
        monitor = gate.SentinelMonitor.__new__(gate.SentinelMonitor)
        monitor.check = Mock()
        monitor.rows = [{"descriptor": {"name": x}} for x in ("active", "sibling", "neighbor")]
        monitor.samples = [{"context": "sibling", "unix_ns": 15}, {"context": "neighbor", "unix_ns": 9}]
        with self.assertRaisesRegex(driver.Rejected, "contemporaneous"):
            monitor.check_interval(10, 20, "active")
        monitor.samples.append({"context": "neighbor", "unix_ns": 19})
        monitor.check_interval(10, 20, "active")

    def test_monitor_cancellation_prevents_next_command(self):
        monitor = gate.SentinelMonitor.__new__(gate.SentinelMonitor)
        monitor.finished = threading.Event()
        monitor.finished.set()
        monitor.record = Mock()
        with self.assertRaises(gate.MonitorStopped):
            monitor.command({}, ["info"])
        monitor.record.run.assert_not_called()

    def test_completed_mutation_is_durable_and_clears_uncertainty(self):
        h = self.harness()
        h.mutate("create", {"name": "private", "owner": {"machine_id": "owned"}}, ["container", "create"])
        self.assertFalse(h.effects_uncertain)
        intent = json.loads((h.evidence / "mutation-001.intent.json").read_text())
        receipt = json.loads((h.evidence / "mutation-001.result.json").read_text())
        self.assertTrue(intent["effects_uncertain"])
        self.assertFalse(receipt["effects_uncertain"])
        self.assertEqual(receipt["exit_code"], 0)


if __name__ == "__main__":
    unittest.main()
