"""Offline admission/ownership regressions, not physical Docker evidence."""
import contextlib
import copy
import io
import itertools
import json
import os
from pathlib import Path
import tempfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_e2e as gate


class AdmissionTests(unittest.TestCase):
    def test_input_mapping_derives_machine_config_but_retains_pinned_bootstrap_plugins(self):
        harness = types.SimpleNamespace(runtime=Path('/owned/runtime'), config=Path('/owned/bootstrap'),
            info={'run_id': 'owned-test', 'fixture_sha256': 'f' * 64,
                  'clients': {'docker': {'canonical': '/owned/docker', 'sha256': 'd' * 64},
                              'vz': {'sha256': 'a' * 64}}})
        scope = {'project_id': 'prj_one', 'environment_id': 'env_one', 'machine_id': 'mch_one'}
        with patch.object(gate.startup, 'digest', return_value='c' * 64):
            value = gate.input_mapping(harness, scope, {'proof': 'exact'}, {'images': 'exact'})
        self.assertEqual(value['docker_config'], str(gate.startup.machine_config_path(harness.runtime, scope)))
        self.assertNotEqual(value['docker_config'], str(harness.config))
        for name in ('compose', 'buildx'):
            self.assertEqual(value['clients'][name]['path'], str(harness.config / 'cli-plugins' / ('docker-' + name)))
        with patch.object(gate.startup, 'digest', return_value='c' * 64):
            sibling = gate.input_mapping(harness, dict(scope, machine_id='mch_other'), {}, {})
        self.assertNotEqual(value['docker_config'], sibling['docker_config'])

    def test_sentinel_monitor_has_no_shared_config_fallback(self):
        monitor = object.__new__(gate.SentinelMonitor)
        monitor.finished = threading.Event()
        monitor.harness = types.SimpleNamespace(config=Path('/bootstrap'), root=Path('/owned'),
            info={'clients': {'docker': {'canonical': '/owned/docker'}}})
        monitor.record = types.SimpleNamespace(run=Mock())
        descriptor = {'name': 'machine', 'config_dir': '/owned/machine/docker-client'}
        monitor.command(descriptor, ['info'])
        self.assertEqual(monitor.record.run.call_args.args[1],
                         ['docker', '--config', descriptor['config_dir'], '--context', 'machine', 'info'])
        monitor.record.run.reset_mock()
        with self.assertRaises(KeyError):
            monitor.command({'name': 'machine'}, ['info'])
        monitor.record.run.assert_not_called()

    def test_images_admission_has_no_builder_or_foreign_fixture_options(self):
        common = [part for name in gate.startup.OPTIONS for part in ('--' + name, '/owned/value')]
        args = gate.arguments(['--suite', 'images', *common])
        self.assertEqual(args.suite, 'images')
        self.assertTrue(args.run_id.startswith('images-'))
        for option in ('buildkit-archive', 'parallel-fixture', 'ssh-fixture', 'ssh-packages',
                       'ssh-gpgv', 'container-fixture', 'tmux'):
            self.assertIsNone(getattr(args, option.replace('-', '_')))
            with self.subTest(option=option), self.assertRaises(ValueError):
                gate.arguments(['--suite', 'images', *common, '--' + option, '/foreign/input'])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            gate.arguments(['--suite', 'images', '--suite=images'])

    def test_images_preflight_pins_exact_source_closure_without_builder_or_terminal(self):
        from linux_docker_image_machine import required_source_paths
        args = types.SimpleNamespace(suite='images', fixture='/owned/base', image_input='/owned/pin',
            run_id='images-owned')
        with patch.object(gate.startup, 'preflight', return_value={'inputs': {}}), \
                patch.object(gate.startup, 'canonical', side_effect=Path), \
                patch.object(gate.startup, 'digest', side_effect=lambda path: 'hash:' + str(path)), \
                patch.object(gate.image_input, 'load', return_value={'immutable': 'image-pin'}), \
                patch.object(gate, 'public_ca_input', return_value={}), \
                patch.object(gate.driver, 'tree_digest', return_value='fixture-hash'), \
                patch.object(gate, 'tmux_input') as terminal, \
                patch('linux_docker_buildkit_builder.preflight_archive') as builder:
            info = gate.preflight(args, require_host=False)
        self.assertEqual(info['scope'], gate.IMAGES_SCOPE)
        self.assertEqual(info['suite'], 'images')
        self.assertEqual(info['python_image'], {'immutable': 'image-pin'})
        self.assertEqual(info['fixture_sha256'], 'fixture-hash')
        for name in required_source_paths():
            self.assertEqual(info['inputs'][name], 'hash:' + name)
        for name in ('buildkit', 'container_fixture', 'parallel_fixture', 'ssh_fixture', 'tmux'):
            self.assertNotIn(name, info)
        for name in ('linux_docker_runtime_audit.py', 'linux_docker_container_lifecycle.py',
                     'linux_docker_buildkit_builder.py', 'linux_docker_interactive_tmux.py'):
            self.assertNotIn(str(gate.REPO / 'scripts/helpers' / name), info['inputs'])
        terminal.assert_not_called()
        builder.assert_not_called()

    def test_images_preflight_rejects_foreign_options_even_without_argument_parser(self):
        for option in ('buildkit_archive', 'parallel_fixture', 'ssh_fixture', 'ssh_packages',
                       'ssh_gpgv', 'container_fixture', 'tmux'):
            with self.subTest(option=option), patch.object(gate.startup, 'preflight') as startup:
                with self.assertRaisesRegex(ValueError, 'image suite rejects'):
                    gate.preflight(types.SimpleNamespace(suite='images', **{option: '/foreign/input'}), require_host=False)
                startup.assert_not_called()

    def test_lifecycle_admission_requires_no_external_builder_and_scopes_fixture_option(self):
        common = [part for name in gate.startup.OPTIONS for part in ('--'+name, '/owned/value')]
        common += ['--tmux', '/owned/tmux']
        args = gate.arguments(['--suite', 'lifecycle', *common])
        self.assertIsNone(args.buildkit_archive)
        self.assertIsNone(args.container_fixture)
        selected = gate.arguments(['--suite', 'lifecycle', *common, '--container-fixture', '/owned/container'])
        self.assertEqual(selected.container_fixture, '/owned/container')
        with self.assertRaisesRegex(ValueError, 'Buildx suites'):
            gate.arguments(['--suite', 'lifecycle', *common, '--buildkit-archive', '/owned/archive'])
        for suite in ('compose', 'build', 'artifacts', 'parallel', 'ssh', 'images'):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, 'container-fixture'):
                gate.arguments(['--suite', suite, '--container-fixture', '/owned/container'])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            gate.arguments(['--suite', 'lifecycle', '--container-fixture=a', '--container-fixture=b'])

    def test_lifecycle_preflight_freezes_transitive_helpers_and_both_fixtures(self):
        args = types.SimpleNamespace(suite='lifecycle', fixture='/owned/base', image_input='/owned/pin',
            buildkit_archive=None, run_id='lifecycle-owned', tmux='/owned/tmux')
        with patch.object(gate.startup, 'preflight', return_value={'inputs': {}}), \
                patch.object(gate.startup, 'canonical', side_effect=lambda value, **kwargs: Path(value)), \
                patch.object(gate, 'tmux_input', return_value={'path': '/owned/tmux', 'sha256': 'tmux-hash'}), \
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
                     'linux_docker_container_tmux.py', 'linux_docker_interactive_tmux.py',
                     'linux_docker_buildkit_shutdown.py', 'linux_docker_image_input.py',
                     'linux_docker_compose_evidence.py', 'linux_docker_runtime_audit.py',
                     'linux_docker_runtime_audit_capture.py', 'linux_docker_runtime_audit_evidence.py'):
            self.assertIn(str(gate.REPO/'scripts/helpers'/name), info['inputs'])
        for name in ('Dockerfile', 'README.md', 'contract.json', 'probe.py', 'test_probe.py'):
            self.assertIn(str(selected/name), info['inputs'])
        self.assertEqual(info['tmux'], {'path': '/owned/tmux', 'sha256': 'tmux-hash'})
        self.assertEqual(info['inputs']['/owned/tmux'], 'tmux-hash')
        self.assertIn(str(Path(gate.sys.executable)), info['inputs'])

    def test_lifecycle_fixture_rejection_prevents_runtime_dispatch(self):
        args = types.SimpleNamespace(suite='lifecycle', fixture='/owned/base', image_input='/owned/pin',
            buildkit_archive=None, run_id='lifecycle-owned', container_fixture='/foreign/fixture', tmux='/owned/tmux')
        with patch.object(gate.startup, 'preflight', return_value={'inputs': {}}), \
                patch.object(gate, 'tmux_input', return_value={'path': '/owned/tmux', 'sha256': 'tmux-hash'}), \
                patch.object(gate.startup, 'canonical', side_effect=Path), \
                patch.object(gate.startup, 'digest', return_value='hash'), \
                patch.object(gate.image_input, 'load', return_value={}), \
                patch.object(gate, 'public_ca_input', return_value={}), \
                patch.object(gate.driver, 'tree_digest', return_value='hash'), \
                patch('linux_docker_container_fixture.fixture_contract', side_effect=ValueError('fixture rejected')), \
                patch.object(gate, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'fixture rejected'): gate.preflight(args, require_host=False)
            run.assert_not_called()

    def test_tmux_required_explicitly_only_for_lifecycle_before_preflight(self):
        with self.assertRaisesRegex(ValueError, '--tmux'):
            gate.arguments(['--suite', 'lifecycle'])
        for suite in ('compose', 'build', 'artifacts', 'parallel', 'ssh', 'images'):
            with self.subTest(suite=suite), self.assertRaisesRegex(ValueError, '--tmux'):
                gate.arguments(['--suite', suite, '--tmux', '/owned/tmux'])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            gate.arguments(['--suite', 'lifecycle', '--tmux=a', '--tmux=b'])
        with patch.object(gate.startup, 'preflight') as startup:
            for suite, tmux in (('lifecycle', None), ('compose', '/owned/tmux'), ('all', None)):
                with self.subTest(suite=suite), self.assertRaises(ValueError):
                    gate.preflight(types.SimpleNamespace(suite=suite, tmux=tmux), require_host=False)
            startup.assert_not_called()

    def test_tmux_input_is_canonical_regular_executable_and_stable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            tool = root/'tmux'; tool.write_bytes(b'public inert executable fixture'); tool.chmod(0o700)
            self.assertEqual(gate.tmux_input(str(tool)), {'path': str(tool), 'sha256': driver.sha256(tool.read_bytes())})
            alias = root/'alias'; alias.symlink_to(tool)
            for path in (alias, root, Path('tmux')):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    gate.tmux_input(str(path))
            tool.chmod(0o600)
            with self.assertRaises(ValueError): gate.tmux_input(str(tool))
            tool.chmod(0o700)
            linked = root/'linked'; os.link(tool, linked)
            with self.assertRaises(ValueError): gate.tmux_input(str(tool))
            linked.unlink()
            fifo = root/'fifo'; os.mkfifo(fifo)
            with self.assertRaises(ValueError): gate.tmux_input(str(fifo))
            original = gate.startup.digest
            def mutate(path):
                value = original(path); path.chmod(0o500); return value
            with patch.object(gate.startup, 'digest', side_effect=mutate), self.assertRaisesRegex(ValueError, 'changed'):
                gate.tmux_input(str(tool))

    def test_bad_tmux_stops_before_startup_preflight(self):
        with patch.object(gate, 'tmux_input', side_effect=ValueError('bad tmux')), \
             patch.object(gate.startup, 'preflight') as startup:
            with self.assertRaisesRegex(ValueError, 'bad tmux'):
                gate.preflight(types.SimpleNamespace(suite='lifecycle', tmux='/bad/tmux'), require_host=False)
            startup.assert_not_called()

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

    def test_all_demands_every_suite_input_before_any_preflight_or_write(self):
        """`all` composes every suite in one provisioning, so it carries every
        suite's inputs; a bare invocation is still refused before any state."""
        with patch.object(gate, "preflight") as preflight, patch.object(gate, "run") as run:
            with contextlib.redirect_stderr(io.StringIO()) as errors:
                self.assertEqual(gate.main(["--suite", "all"]), 2)
            self.assertIn("--registry-archive is required", errors.getvalue())
            preflight.assert_not_called()
            run.assert_not_called()
        self.assertEqual(gate.SUITE_ORDER[-1], "recovery", "recovery cycles Stop/Up and must run last")
        # A composed run performs every suite exactly once.
        self.assertEqual(set(gate.SUITES), set(gate.SUITE_ORDER))
        self.assertEqual(len(gate.SUITE_ORDER), len(set(gate.SUITE_ORDER)))
        # `lifecycle` needs its runtime-audit window to hold almost nothing but
        # its own mutations, so it runs immediately before the last suite and
        # opens that window itself.
        self.assertEqual(gate.SUITE_ORDER[-2], "lifecycle")
        self.assertTrue(gate.executes({"suite": "all"}, "lifecycle"))
        self.assertTrue(gate.executes({"suite": "all"}, "registry"))
        self.assertTrue(gate.executes({"suite": "lifecycle"}, "lifecycle"))
        # Performing lifecycle means carrying its inputs: a composed run needs
        # the terminal it drives, and may pin the container fixture.
        common = []
        for name in gate.startup.OPTIONS:
            common.extend(["--" + name, "/absolute/input"])
        composed = common + ["--suite", "all", "--registry-archive", "/absolute/archive",
                             "--registry-layout", "/absolute/layout", "--buildkit-archive", "/absolute/buildkit",
                             "--ssh-packages", "/absolute/packages"]
        with self.assertRaisesRegex(driver.Rejected, "--tmux is required"):
            gate.arguments(composed)
        args = gate.arguments(composed + ["--tmux", "/absolute/tmux",
                                          "--container-fixture", "/absolute/container"])
        self.assertEqual((args.suite, args.tmux, args.container_fixture),
                         ("all", "/absolute/tmux", "/absolute/container"))
        # A suite that does not perform lifecycle still rejects both.
        with self.assertRaisesRegex(driver.Rejected, "--tmux is required"):
            gate.arguments(common + ["--suite", "compose", "--tmux", "/absolute/tmux"])

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
    def test_images_result_stays_dev_and_retains_normal_cleanup_fence_without_runtime_audit(self):
        info = {'scope': gate.IMAGES_SCOPE, 'suite': 'images', 'inputs': {'/owned/image-helper.py': 'source-hash'},
                'fixture': '/owned/base', 'fixture_sha256': 'base-hash'}
        for failed in (False, True):
            with self.subTest(failed=failed):
                harness = types.SimpleNamespace(evidence=Path('/owned/evidence'), root=Path('/owned/root'),
                    staged_inputs={}, monitor=None, stage=Mock(),
                    scenario=Mock(side_effect=ValueError('image replay failed') if failed else None, return_value={}),
                    remove_owned=Mock(side_effect=ValueError('cleanup proof unresolved') if failed else None),
                    cleanup=Mock(return_value={'positive_stop_all': True, 'daily_default_unchanged': True}),
                    capture_runtime_audits=Mock())
                with patch.object(gate, 'ComposeHarness', return_value=harness), \
                        patch.object(gate.os, 'umask'), patch.object(gate.startup, 'document'), \
                        patch.object(gate.startup, 'collect_runtime_receipts'), \
                        patch.object(gate.startup, 'checksum_evidence'), \
                        patch.object(gate.startup, 'digest', return_value='source-hash') as digest, \
                        patch.object(gate.driver, 'tree_digest', return_value='base-hash'), \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    code = gate.run(info)
                result = json.loads(output.getvalue())
                self.assertEqual(code, int(failed))
                self.assertEqual(result['scope'], gate.IMAGES_SCOPE)
                self.assertEqual(result['outcome'], 'failed' if failed else 'passed_dev_installed_images_slice')
                self.assertFalse(result['docker_parity_certified'])
                self.assertFalse(result['aggregate_release_certified'])
                self.assertEqual(result['release_scenarios_passed'], [])
                self.assertEqual(result['test_case_retries'], 0)
                harness.capture_runtime_audits.assert_not_called()
                if failed:
                    harness.cleanup.assert_not_called()
                    self.assertIn('image replay failed', result['error'])
                    self.assertIn('cleanup proof unresolved', result['cleanup_errors'][0])
                else:
                    digest.assert_called_once_with(Path('/owned/image-helper.py'))
                    harness.cleanup.assert_called_once_with()
                    self.assertTrue(result['cleanup']['positive_stop_all'])
                    self.assertTrue(result['cleanup']['daily_default_unchanged'])
                    self.assertFalse(result['cleanup']['delete_certified'])

    def test_lifecycle_end_rehash_cannot_promote_changed_fixture_or_full_contract(self):
        info = {'scope': gate.LIFECYCLE_SCOPE, 'suite': 'lifecycle', 'inputs': {},
                'fixture': '/owned/base', 'fixture_sha256': 'base-hash',
                'container_fixture': '/owned/container', 'container_fixture_sha256': 'container-hash'}
        for changed in (False, True):
            with self.subTest(changed=changed):
                harness = types.SimpleNamespace(evidence=Path('/owned/evidence'), root=Path('/owned/root'),
                    staged_inputs={}, monitor=None, stage=Mock(), scenario=Mock(return_value={}),
                    remove_owned=Mock(), capture_runtime_audits=Mock(return_value=['four replayed sessions']),
                    runtime_audit_validation=None, runtime_audit_retirement=None,
                    retire_runtime_audits=Mock(return_value=['four closed windows']),
                    cleanup=Mock(return_value={}))
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


class RuntimeAuditIntegrationTests(unittest.TestCase):
    def harness(self):
        h = gate.ComposeHarness.__new__(gate.ComposeHarness)
        h.runtime_audits = []
        h.cli, h.evidence = Path('/owned/bin/vz'), Path('/owned/evidence')
        h.info = {'inputs': {'/source/audit.py': 'source-pin'}}
        h.staged_inputs = {str(h.cli): 'cli-pin'}
        return h

    def test_registers_each_owner_before_fresh_enrollment_and_no_reenrollment(self):
        h = self.harness()
        contexts = [{'owner': {'machine_id': 'machine-%d' % i}} for i in range(4)]
        sessions = []
        def session(*args, **kwargs):
            self.assertEqual(args[:3], (h, contexts[len(sessions)],
                h.evidence / ('runtime-audit-%d' % len(sessions))))
            self.assertEqual(args[3], {'/source/audit.py': 'source-pin', str(h.cli): 'cli-pin'})
            self.assertRegex(kwargs['session_id'], '^[0-9a-f]{64}$')
            item = Mock()
            item.enroll.side_effect = lambda: self.assertIs(h.runtime_audits[-1], item)
            sessions.append(item)
            return item
        with patch('linux_docker_runtime_audit_evidence.required_source_paths', return_value=['/source/audit.py']), \
                patch('linux_docker_runtime_audit_evidence.Session', side_effect=session) as constructor:
            h.enroll_runtime_audits(contexts)
            self.assertEqual(h.runtime_audits, sessions)
            for item in sessions:
                item.enroll.assert_called_once_with()
            with self.assertRaisesRegex(driver.Rejected, 'fresh'):
                h.enroll_runtime_audits(contexts)
            self.assertEqual(constructor.call_count, 4)

    def test_partial_enrollment_is_retained_and_later_machines_are_not_dispatched(self):
        h = self.harness()
        contexts = [{'owner': {'machine_id': 'machine-%d' % i}} for i in range(4)]
        first, failed = Mock(), Mock()
        first.assert_enrolled_certain = Mock()
        failed.assert_enrolled_certain = Mock()
        failed.enroll.side_effect = ValueError('uncertain enrollment')
        failed.assert_enrolled_certain.side_effect = ValueError('uncertain enrollment')
        with patch('linux_docker_runtime_audit_evidence.required_source_paths', return_value=['/source/audit.py']), \
                patch('linux_docker_runtime_audit_evidence.Session', side_effect=[first, failed]) as constructor:
            with self.assertRaisesRegex(ValueError, 'uncertain'):
                h.enroll_runtime_audits(contexts)
            self.assertEqual(constructor.call_count, 2)
            self.assertEqual(h.runtime_audits, [first, failed])
            with self.assertRaisesRegex(ValueError, 'uncertain'):
                h.assert_certain()
        first.assert_enrolled_certain.assert_called_once_with()

    def test_capture_requires_certain_cleanup_and_all_four_sessions(self):
        h = self.harness()
        h.assert_certain = Mock()
        with self.assertRaisesRegex(driver.Rejected, 'four'):
            h.capture_runtime_audits()
        h.runtime_audits = [Mock() for _ in range(4)]
        for index, session in enumerate(h.runtime_audits):
            session.capture.return_value = {'machine': index}
        self.assertEqual(h.capture_runtime_audits(), [{'machine': i} for i in range(4)])
        h.assert_certain.side_effect = ValueError('live monitor')
        with self.assertRaisesRegex(ValueError, 'live monitor'):
            h.capture_runtime_audits()
        for session in h.runtime_audits:
            session.capture.assert_called_once_with()

    def test_final_capture_follows_owned_removal_and_failure_blocks_public_stop(self):
        info = {'scope': gate.LIFECYCLE_SCOPE, 'suite': 'lifecycle', 'inputs': {},
                'fixture': '/owned/base', 'fixture_sha256': 'hash',
                'container_fixture': '/owned/container', 'container_fixture_sha256': 'hash'}
        for failure in (False, True):
            with self.subTest(failure=failure):
                order = []
                def capture():
                    order.append('capture')
                    if failure:
                        raise ValueError('incomplete journal')
                    return ['four independently replayed journals']
                h = types.SimpleNamespace(evidence=Path('/owned/evidence'), root=Path('/owned/root'),
                    staged_inputs={}, monitor=None, stage=Mock(), scenario=Mock(return_value={}),
                    remove_owned=Mock(side_effect=lambda: order.append('remove')),
                    capture_runtime_audits=Mock(side_effect=capture),
                    runtime_audit_validation=None, runtime_audit_retirement=None,
                    retire_runtime_audits=Mock(return_value=['four closed windows']),
                    cleanup=Mock(side_effect=lambda: order.append('stop') or {}))
                with patch.object(gate, 'ComposeHarness', return_value=h), \
                        patch.object(gate.os, 'umask'), patch.object(gate.startup, 'document'), \
                        patch.object(gate.startup, 'collect_runtime_receipts'), \
                        patch.object(gate.startup, 'checksum_evidence'), \
                        patch.object(gate.driver, 'tree_digest', return_value='hash'), \
                        patch('linux_docker_container_fixture.fixture_contract'), \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    self.assertEqual(gate.run(info), int(failure))
                result = json.loads(output.getvalue())
                self.assertEqual(order, ['remove', 'capture'] + ([] if failure else ['stop']))
                self.assertIs(result['docker_parity_certified'], False)
                self.assertIs(result['aggregate_release_certified'], False)
                if failure:
                    h.cleanup.assert_not_called()
                    self.assertIn('incomplete journal', result['cleanup_errors'][0])
                else:
                    self.assertEqual(result['runtime_audit_validation'], ['four independently replayed journals'])


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
        contexts = [{field: field + "-" + str(index) for field in ("name", "endpoint", "engine_id", "config_dir")}
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
                harness.owned, harness.prepared_images = [], {}
                with self.assertRaises(driver.Rejected):
                    harness.prepare_image(descriptor)
                harness.docker.assert_called_once_with("public-registry-policy", descriptor,
                                                       ["info", "--format", "{{json .}}"])
                harness.mutate.assert_not_called()
                harness.exact_absent.assert_not_called()
                self.assertEqual(harness.owned, [])


class BuildDispatchTests(unittest.TestCase):
    def test_images_branch_skips_preparation_driver_run_and_runtime_audits(self):
        self.check_owned_orchestrator('images', 'linux_docker_image_machine')

    def test_images_failure_stops_monitor_preserves_cleanup_fence_and_never_dispatches_later_machine(self):
        for index in (0, 1, 2):
            with self.subTest(index=index):
                self.check_owned_orchestrator('images', 'linux_docker_image_machine', fail_at=index)

    def test_lifecycle_branch_calls_only_lifecycle_and_monitors_every_machine(self):
        self.check_owned_orchestrator('lifecycle', 'linux_docker_container_lifecycle')

    def test_artifacts_branch_calls_only_artifact_orchestrator_and_monitors_every_machine(self):
        self.check_owned_orchestrator("artifacts", "linux_docker_build_artifacts")

    def test_parallel_branch_calls_only_parallel_orchestrator_and_monitors_every_machine(self):
        self.check_owned_orchestrator("parallel", "linux_docker_build_parallel")

    def test_prepare_suites_opens_the_whole_run_window_only_for_a_lifecycle_only_run(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.registry_controls, harness.registry_sessions = None, []
        harness.enroll_runtime_audits = Mock()
        for suites in (["lifecycle"], ["registry", "lifecycle", "recovery"], ["compose"]):
            harness.enroll_runtime_audits.reset_mock()
            # `registry` is absent from two of these, and its own preparation
            # returns early; only the audit decision is under test here.
            harness.prepare_suites([s for s in suites if s != "registry"],
                                   ("c0",), (), None, Path("/owned/project"))
            if suites == ["lifecycle"]:
                harness.enroll_runtime_audits.assert_called_once_with(("c0",))
            else:
                harness.enroll_runtime_audits.assert_not_called()

    def test_a_composed_run_opens_the_audit_window_around_lifecycle_only(self):
        """The window must hold the lifecycle suite's mutations and little else.

        A whole-run window overruns youki's 2048-record journal on sentinel
        sampling alone, so a composed run enrolls immediately before the suite
        and captures immediately after it, with the monitor paused so the
        journal cannot move underneath the capture's own replay comparison.
        """
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.runtime_audits, harness.runtime_audit_validation = [], None
        harness.runtime_audit_retirement = None
        harness.registry_controls, harness.registry_sessions = None, []
        harness.live_cleanup = False
        order = []
        def enroll(contexts):
            # Enrollment snapshots a journal that must be empty, so it has to
            # run with every Machine unobserved, exactly like the capture.
            self.assertEqual([edge for edge, _ in paused], ["enter"],
                             "enrollment must run under the pause")
            order.append(("enroll", tuple(contexts)))
        harness.enroll_runtime_audits = Mock(side_effect=enroll)
        def capture():
            # Mid-run capture must run under the live-cleanup window, or
            # `assert_certain` refuses it because the monitor is still alive.
            self.assertTrue(harness.live_cleanup)
            order.append("capture")
            return ["four sessions"]
        harness.capture_runtime_audits = Mock(side_effect=capture)
        def retire():
            # The window must close inside the same pause that read it.
            self.assertEqual([edge for edge, _ in paused][-1], "enter",
                             "retirement must run under the pause")
            order.append("retire")
            return ["four closed windows"]
        harness.retire_runtime_audits = Mock(side_effect=retire)
        harness.run_machine_suite = Mock(side_effect=lambda suite, *_: order.append(("suite", suite)) or [suite])
        paused = []
        @contextlib.contextmanager
        def pausing():
            paused.append(("enter", tuple(order)))
            try:
                yield
            finally:
                paused.append(("exit", tuple(order)))
        harness.monitor = Mock()
        harness.monitor.paused = Mock(side_effect=pausing)
        for suites, composed in ((["registry", "lifecycle", "recovery"], True), (["lifecycle"], False)):
            order.clear(); paused.clear()
            harness.runtime_audit_validation = None
            harness.enroll_runtime_audits.reset_mock(); harness.capture_runtime_audits.reset_mock()
            harness.retire_runtime_audits.reset_mock(); harness.runtime_audit_retirement = None
            for suite in suites:
                gate.ComposeHarness.run_suite_with_audit_window(harness, suite, suites, ("c0", "c1"), None, None)
            if composed:
                self.assertEqual(order, [("suite", "registry"), ("enroll", ("c0", "c1")),
                                         ("suite", "lifecycle"), "capture", "retire",
                                         ("suite", "recovery")])
                self.assertEqual(harness.runtime_audit_validation, ["four sessions"])
                self.assertEqual(harness.runtime_audit_retirement, ["four closed windows"])
                # Paused twice and only twice: once around enrollment, once
                # around the capture. The suite itself runs observed, so the
                # Machines that are not under test keep their liveness record.
                self.assertEqual([edge for edge, _ in paused], ["enter", "exit", "enter", "exit"])
                self.assertEqual(paused[1][1][-1], ("enroll", ("c0", "c1")))
                self.assertEqual(paused[2][1][-1], ("suite", "lifecycle"))
                self.assertEqual(paused[3][1][-1], "retire")
                self.assertFalse(harness.live_cleanup, "the live-cleanup window must close")
            else:
                # A lifecycle-only run keeps the whole-run window `prepare_suites`
                # opened, so this path must neither enroll nor capture early.
                self.assertEqual(order, [("suite", "lifecycle")])
                harness.enroll_runtime_audits.assert_not_called()
                harness.capture_runtime_audits.assert_not_called()
                harness.retire_runtime_audits.assert_not_called()
                self.assertIsNone(harness.runtime_audit_validation)
                self.assertEqual(paused, [])

    def check_owned_orchestrator(self, suite, module_name, fail_at=None):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        image_pin = {'reference': 'python@sha256:' + 'b' * 64, 'id': 'sha256:' + 'c' * 64,
                     'platform': 'linux/arm64', 'extra_registry_metadata': 'not passed as image identity'}
        harness.info = {"suite": suite, "public_ca": {"bundle_sha256": "a" * 64}, 'python_image': image_pin}
        harness.drivers, harness.driver_cleanup_verified = [], []
        harness.record = types.SimpleNamespace(receipts=[], pending_interactions=[], canaries=[])
        harness.slice_records = []
        harness.env = {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}
        harness.effects_uncertain = False
        evidence = tempfile.TemporaryDirectory()
        self.addCleanup(evidence.cleanup)
        harness.cli, harness.evidence = Path("/owned/bin/vz"), Path(evidence.name)
        contexts = [{"name": "context-" + str(i), "endpoint": "endpoint-" + str(i),
                     "engine_id": "engine-" + str(i), "config_dir": "/owned/machine-config-" + str(i)} for i in range(4)]
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
        harness.enroll_runtime_audits = Mock()
        enrollment_order = Mock()
        enrollment_order.attach_mock(harness.enroll_runtime_audits, 'enroll')
        enrollment_order.attach_mock(harness.sentinel, 'sentinel')
        harness.prepare_image = Mock(return_value={"exact": "images"})
        harness.prepare_builder = Mock()
        harness.docker, harness.mutate = Mock(), Mock()
        harness.driver_inputs, harness.validate_driver = Mock(), Mock()
        monitor = Mock()
        # The monitor must stop sampling the Machine under test for exactly the
        # timed region, so the mock is a real context manager and records both
        # edges: the workload has to run inside it, and it has to end.
        exclusions = []
        @contextlib.contextmanager
        def excluding(*names):
            exclusions.append(("enter",) + names)
            try:
                yield
            finally:
                exclusions.append(("exit",) + names)
        monitor.excluding = Mock(side_effect=excluding)
        monitor.close_interval = Mock(side_effect=lambda begin, active: gate.time.time_ns())
        monitor.record = types.SimpleNamespace(receipts=[], pending_interactions=[])
        monitor.thread.is_alive.return_value = False
        monitor.summary.return_value = {"samples": "observed"}
        observations = [{"operation": i} for i in range(3)]
        # A parallel suite's slices must genuinely be in flight together. The
        # barrier is the assertion: a slice reaches it only after the other two
        # have started, so an implementation that dispatched the Machines one
        # after another never releases it and this test fails on the timeout
        # rather than passing on a claim.
        parallel = suite in gate.PARALLEL_SUITES
        barrier = threading.Barrier(3, timeout=20) if parallel else None
        def selected_machine(*args):
            index = args[-1]
            if barrier is not None:
                barrier.wait()
            if suite == 'images':
                harness.drivers.append(types.SimpleNamespace(record=types.SimpleNamespace(
                    receipts=[{'effects_uncertain': False}], pending_interactions=[])))
                harness.driver_cleanup_verified.append(index != fail_at)
            if index == fail_at:
                raise ValueError('image Machine replay failed')
            return observations[index]
        module = types.SimpleNamespace(run_machine=Mock(side_effect=selected_machine))
        with patch.dict("sys.modules", {module_name: module}), \
                patch.object(gate, "SentinelMonitor", return_value=monitor), \
                patch.object(gate.startup, "exact_developer_topology"), \
                patch.object(gate.startup, "document"), \
                patch.object(gate, "authenticated_proof", return_value=({"scope": "exact"}, {"proof": "exact"})), \
                patch.object(gate.time, "time_ns", side_effect=itertools.count(10)), \
                patch.object(gate.driver, "Driver") as selected:
            if fail_at is None:
                result = harness.scenario()
            else:
                with self.assertRaisesRegex(ValueError, 'image Machine replay failed'):
                    harness.scenario()
        if fail_at is None:
            self.assertCountEqual(result["machine_slices"], observations)
            scheduling = result["suite_concurrency"][suite]
            self.assertEqual(scheduling["execution"],
                             "concurrent" if suite in gate.PARALLEL_SUITES else "serial")
            self.assertEqual([row["index"] for row in scheduling["slices"]], [0, 1, 2])
        else:
            self.assertEqual(harness.driver_cleanup_verified, [True] * fail_at + [False])
            with self.assertRaisesRegex(ValueError, 'cleanup lacks successful independent replay'):
                harness.remove_owned()
            harness.docker.assert_not_called()
            harness.mutate.assert_not_called()
        if suite == 'lifecycle':
            harness.enroll_runtime_audits.assert_called_once_with(contexts)
            self.assertEqual(enrollment_order.mock_calls, [unittest.mock.call.enroll(contexts)] +
                             [unittest.mock.call.sentinel(context) for context in contexts])
        else:
            harness.enroll_runtime_audits.assert_not_called()
        if suite == 'images':
            base = {key: image_pin[key] for key in ('reference', 'id', 'platform')}
            expected_images = {'base': base, 'compose': base}
            harness.prepare_image.assert_not_called()
            harness.prepare_builder.assert_not_called()
        else:
            expected_images = {'exact': 'images'}
        self.assertEqual(harness.sentinel.call_args_list, [unittest.mock.call(context) for context in contexts])
        dispatched = 3 if fail_at is None else fail_at + 1
        expected_dispatch = [unittest.mock.call(harness, contexts[i], {"scope": "exact"}, {"proof": "exact"},
                                                expected_images, i) for i in range(dispatched)]
        names = [context["name"] for context in contexts[:3]]
        if suite in gate.PARALLEL_SUITES:
            # Concurrent slices are dispatched in no particular order and
            # nothing is excluded: every Machine in the window keeps being
            # sampled, so each slice's interval is witnessed by all the others.
            self.assertCountEqual(module.run_machine.call_args_list, expected_dispatch)
            self.assertEqual(exclusions, [], "a concurrent window excludes no Machine from sampling")
            self.assertCountEqual([call.args[2] for call in monitor.check_interval.call_args_list],
                                  names[:dispatched])
        else:
            self.assertEqual(module.run_machine.call_args_list, expected_dispatch)
            # A serial slice asserts liveness over every Machine but its own.
            self.assertEqual([call.args[2] for call in monitor.check_interval.call_args_list],
                             [contexts[i]["name"] for i in range(3 if fail_at is None else fail_at)])
            self.assertEqual(exclusions, [edge for i in range(dispatched)
                                          for edge in (("enter", contexts[i]["name"]),
                                                       ("exit", contexts[i]["name"]))],
                             "each Machine is unobserved for its own workload and no longer")
        for call in monitor.check_interval.call_args_list:
            self.assertLess(call.args[0], call.args[1])
        monitor.start.assert_called_once_with()
        monitor.stop.assert_called_once_with()
        harness.driver_inputs.assert_not_called()
        harness.validate_driver.assert_not_called()
        selected.assert_not_called()
        self.assertEqual(harness.status.call_count, 2 if fail_at is None else 0)

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
        harness.info, harness.owned, harness.prepared_images = {"python_image": pin}, [], {}
        harness.docker = Mock(side_effect=[
            (json.dumps({"ID": "exact-engine", "RegistryConfig": policy}).encode(), b"", 0), (b"", b"", 0)])
        harness.exact_absent = Mock()
        harness.mutate = Mock(side_effect=RuntimeError("reached pinned pull"))
        with self.assertRaisesRegex(RuntimeError, "reached pinned pull"):
            harness.prepare_image(descriptor)
        self.assertEqual(harness.docker.call_count, 2)
        harness.mutate.assert_called_once_with("python-pull", descriptor,
            ["pull", "--platform", "linux/arm64", pin["reference"]], timeout=300)


class ParallelSliceTests(unittest.TestCase):
    """Concurrent per-Machine slices: isolation, ordering and observability.

    These are scheduling assertions. Every one of them would still pass under
    serial execution if it only checked results, so each holds a barrier the
    slices must meet: a run that dispatched the Machines one after another never
    releases it, and the test fails on the barrier rather than on the claim.
    """
    def harness(self, count=3):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        evidence = tempfile.TemporaryDirectory()
        self.addCleanup(evidence.cleanup)
        harness.evidence = Path(evidence.name)
        harness.env = {"PATH": "/usr/bin:/bin", "LC_ALL": "C"}
        harness.record = types.SimpleNamespace(receipts=[], canaries=["run-canary"])
        harness.slice_records = []
        harness.prepared_images = {}
        harness.info = {"suite": "ssh"}
        harness.prepare_image = Mock(side_effect=lambda descriptor: {"compose": descriptor["name"]})
        self.exclusions = []
        monitor = types.SimpleNamespace(check=Mock(), excluded=frozenset())
        def excluding(*names):
            self.exclusions.append(("enter",) + names)
            try:
                yield
            finally:
                self.exclusions.append(("exit",) + names)
        monitor.excluding = contextlib.contextmanager(excluding)
        harness.monitor = monitor
        machines = [(None, {"machine_id": "mch_" + str(i),
                            "docker_context": {"name": "context-" + str(i),
                                               "owner": {"machine_id": "mch_" + str(i)}}})
                    for i in range(count)]
        bindings = {"mch_" + str(i): ({"machine_id": "mch_" + str(i)}, {"proof": i}) for i in range(count)}
        return harness, machines, bindings

    def test_slices_overlap_and_each_records_into_its_own_recorder(self):
        harness, machines, bindings = self.harness()
        barrier = threading.Barrier(len(machines), timeout=20)
        seen = {}
        def slice_body(suite, index, machine, _bindings, *, own_exclusion):
            # Nothing is excluded: a busy Machine still witnesses its siblings.
            self.assertFalse(own_exclusion, "a concurrent slice excludes no Machine from sampling")
            seen[index] = (harness.record, threading.current_thread().name)
            # Released only once every Machine has reached it.
            barrier.wait()
            return {"slice": index}
        harness.run_machine_slice = slice_body
        observations = harness.run_concurrent_slices("ssh", machines, bindings)
        self.assertEqual(observations, [{"slice": 0}, {"slice": 1}, {"slice": 2}])
        recorders = [seen[i][0] for i in range(3)]
        self.assertEqual(len({id(item) for item in recorders}), 3, "each slice needs a Recorder of its own")
        self.assertNotIn(id(harness.shared_record), {id(item) for item in recorders})
        self.assertEqual(harness.slice_records, recorders)
        for index, item in enumerate(recorders):
            # Numbering a receipt by list length is exactly what makes a shared
            # Recorder unsafe, so each slice records into a directory of its own.
            self.assertEqual(item.root, harness.evidence / ("ssh-machine-" + str(index) + "-commands"))
            # One canary list run-wide: a secret one slice admits stays refused
            # by every command of every other slice and of the rest of the run.
            self.assertIs(item.canaries, harness.shared_record.canaries)
        self.assertEqual(len({name for _, name in seen.values()}), 3)
        self.assertIs(harness.record, harness.shared_record, "the parent thread keeps the run's Recorder")
        self.assertEqual(self.exclusions, [], "every Machine stays observed for the whole window")
        record = harness.slice_concurrency_records["ssh"]
        self.assertEqual(record["execution"], "concurrent")
        self.assertTrue(record["observed_concurrent"])
        self.assertGreater(record["min_pairwise_overlap_ns"], 0)
        self.assertEqual([row["index"] for row in record["slices"]], [0, 1, 2])
        self.assertEqual([row["failed"] for row in record["slices"]], [False] * 3)
        self.assertLess(record["wall_ns"], record["summed_slice_ns"], "a serialised window cannot beat its own sum")

    def test_owned_image_preparation_precedes_every_thread(self):
        """The mutation fence is run-wide, so warming images inside the threads
        would only make them queue; it happens once, in Machine order, first."""
        harness, machines, bindings = self.harness()
        barrier = threading.Barrier(len(machines), timeout=20)
        def slice_body(suite, index, machine, _bindings, *, own_exclusion):
            self.assertEqual(harness.prepare_image.call_count, 3, "images are warmed before any slice starts")
            barrier.wait()
            return {"slice": index}
        harness.run_machine_slice = slice_body
        harness.run_concurrent_slices("ssh", machines, bindings)
        self.assertEqual([call.args[0]["name"] for call in harness.prepare_image.call_args_list],
                         ["context-0", "context-1", "context-2"])

    def test_a_failing_slice_joins_its_siblings_before_the_lowest_machine_raises(self):
        harness, machines, bindings = self.harness()
        barrier = threading.Barrier(len(machines), timeout=20)
        failures = {0: ValueError("machine-0 failed"), 2: ValueError("machine-2 failed")}
        finished = []
        def slice_body(suite, index, machine, _bindings, *, own_exclusion):
            barrier.wait()
            finished.append(index)
            if index in failures:
                raise failures[index]
            return {"slice": index}
        harness.run_machine_slice = slice_body
        with self.assertRaises(ValueError) as caught:
            harness.run_concurrent_slices("ssh", machines, bindings)
        self.assertIs(caught.exception, failures[0], "the lowest-numbered Machine's failure is the run's failure")
        self.assertCountEqual(finished, [0, 1, 2], "every dispatched slice is joined, not abandoned")
        record = harness.slice_concurrency_records["ssh"]
        self.assertEqual([row["failed"] for row in record["slices"]], [True, False, True])
        self.assertTrue((harness.evidence / "ssh-machine-concurrency.json").exists())
        self.assertIs(harness.record, harness.shared_record)

    def test_a_window_that_silently_serialised_is_rejected(self):
        """The recorded overlap is the check, not the intent to parallelise."""
        harness, _machines, _bindings = self.harness()
        rows = [{"index": i, "context": "context-" + str(i), "thread": "t", "started_unix_ns": 100 * i,
                 "ended_unix_ns": 100 * i + 50, "failed": False} for i in range(3)]
        with self.assertRaisesRegex(ValueError, "did not overlap"):
            harness.slice_concurrency("ssh", rows, 0, 300, execution="concurrent")
        # Written before it is judged: a serialised window's evidence is exactly
        # what a reader needs to see.
        document = json.loads((harness.evidence / "ssh-machine-concurrency.json").read_text())
        self.assertEqual(document["execution"], "concurrent")
        self.assertFalse(document["observed_concurrent"])
        # The same rows are an ordinary serial window, which claims nothing.
        self.assertFalse(harness.slice_concurrency("build", rows, 0, 300, execution="serial")["observed_concurrent"])

    def test_a_failed_window_records_without_demanding_overlap(self):
        harness, _machines, _bindings = self.harness()
        rows = [{"index": i, "context": "context-" + str(i), "thread": "t", "started_unix_ns": 100 * i,
                 "ended_unix_ns": 100 * i + 50, "failed": i == 1} for i in range(3)]
        self.assertFalse(harness.slice_concurrency("ssh", rows, 0, 300, execution="concurrent")["observed_concurrent"])

    def test_register_driver_reserves_one_slot_per_driver_under_concurrency(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.drivers, harness.driver_cleanup_verified = [], []
        writers, each = 8, 3
        barrier, errors = threading.Barrier(writers, timeout=20), []
        claimed = []
        def register(start):
            try:
                barrier.wait()
                for offset in range(each):
                    item = ("driver", start, offset)
                    claimed.append((harness.register_driver(item), item))
            except BaseException as error:
                errors.append(error)
        threads = [threading.Thread(target=register, args=(index,)) for index in range(writers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(sorted(position for position, _ in claimed), list(range(writers * each)),
                         "positions must be unique and dense")
        self.assertEqual(len(harness.drivers), len(harness.driver_cleanup_verified))
        for position, item in claimed:
            self.assertIs(harness.drivers[position], item, "a reserved position must name its own Driver")
        self.assertEqual(harness.driver_cleanup_verified, [False] * (writers * each))

    def test_the_named_fences_are_created_once_however_many_threads_race(self):
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        barrier, seen, lock = threading.Barrier(8, timeout=20), [], threading.Lock()
        def take():
            barrier.wait()
            with lock:
                seen.append(harness.fence("mutation_lock"))
        threads = [threading.Thread(target=take) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len({id(item) for item in seen}), 1)
        self.assertIsNot(harness.fence("slice_lock"), harness.fence("mutation_lock"))
        with self.assertRaisesRegex(ValueError, "unknown harness fence"):
            harness.fence("registry_lock")

    def test_one_mutation_at_a_time_run_wide_even_across_machines(self):
        """`effects_uncertain` is one fence over every Machine, so concurrent
        slices take it in turn rather than each keeping their own."""
        harness = gate.ComposeHarness.__new__(gate.ComposeHarness)
        harness.effects_uncertain, harness.mutations = False, []
        harness.evidence = Path("/owned/evidence")
        harness.record = types.SimpleNamespace(receipts=[])
        overlapping, active, lock = [], [], threading.Lock()
        started = threading.Barrier(4, timeout=20)
        def docker(label, descriptor, args, **kwargs):
            with lock:
                active.append(label)
                overlapping.append(len(active))
            gate.time.sleep(0.01)
            with lock:
                active.remove(label)
            return (b"", b"", 0)
        harness.docker = docker
        errors = []
        def mutating(index):
            try:
                started.wait()
                harness.mutate("owned-" + str(index), {"name": "context", "owner": {}}, ["image", "rm"])
            except BaseException as error:
                errors.append(error)
        with patch.object(gate.startup, "document"):
            threads = [threading.Thread(target=mutating, args=(index,)) for index in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(overlapping, [1, 1, 1, 1], "two owned mutations must never be in flight together")
        self.assertEqual(sorted(row["index"] for row in harness.mutations), [1, 2, 3, 4],
                         "mutation sequence numbers name retained documents and must stay unique")
        self.assertFalse(harness.effects_uncertain)


class LivenessWitnessTests(unittest.TestCase):
    def monitor(self, names):
        monitor = object.__new__(gate.SentinelMonitor)
        monitor.rows = [{"descriptor": {"name": name}} for name in names]
        monitor.samples, monitor.errors = [], []
        monitor.finished = threading.Event()
        monitor.thread = types.SimpleNamespace(is_alive=lambda: True)
        return monitor

    def test_one_busy_machine_is_witnessed_by_all_the_others(self):
        self.assertEqual(self.monitor(["a", "b", "c", "d"]).observers("a"), {"b", "c", "d"})

    def test_a_concurrently_busy_sibling_is_still_a_witness(self):
        """The point of excluding nothing during a concurrent window: `b` and
        `c` are under workload beside `a`, and they still have to be observed
        live inside `a`'s interval. A parallelised suite's liveness claim is
        exactly the serial suite's, not a reduced one over untouched Machines."""
        monitor = self.monitor(["a", "b", "c", "d"])
        self.assertEqual(monitor.observers("a"), {"b", "c", "d"})
        monitor.samples = [{"context": name, "unix_ns": 15} for name in ("b", "c", "d")]
        self.assertGreaterEqual(monitor.close_interval(10, "a", deadline_seconds=0.2), 15)
        monitor.check_interval(10, 20, "a")
        # Drop the busy sibling's observation and the same interval fails: it is
        # required evidence, not a nicety the concurrent path may skip.
        monitor.samples = [{"context": name, "unix_ns": 15} for name in ("c", "d")]
        with self.assertRaises(ValueError):
            monitor.check_interval(10, 20, "a")
        with self.assertRaises(ValueError):
            monitor.close_interval(10, "a", deadline_seconds=0.2)

    def test_an_interval_with_no_unobserved_machine_is_refused(self):
        with self.assertRaisesRegex(ValueError, "no unobserved Machine"):
            self.monitor(["a"]).observers("a")


class UncertaintyTests(unittest.TestCase):
    def test_pending_or_unreaped_tmux_owner_blocks_cleanup_without_polling(self):
        for pending, code in (([object()], 0), ([], None), ([], 1), ([], -15), ([], False)):
            h = self.harness()
            server = Mock(returncode=code)
            owner = types.SimpleNamespace(pending=pending, server=server)
            h.drivers = [types.SimpleNamespace(terminal_owner=owner,
                record=types.SimpleNamespace(receipts=[], pending_interactions=[]))]
            h.driver_cleanup_verified = [True]
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, 'tmux'):
                h.remove_owned()
            h.docker.assert_not_called()
            server.poll.assert_not_called(); server.wait.assert_not_called(); server.terminate.assert_not_called()

    def test_normal_retired_tmux_owner_does_not_bypass_driver_cleanup_flags(self):
        for server in (None, types.SimpleNamespace(returncode=0)):
            h = self.harness()
            h.drivers = [types.SimpleNamespace(terminal_owner=types.SimpleNamespace(pending=[], server=server),
                record=types.SimpleNamespace(receipts=[], pending_interactions=[]))]
            h.driver_cleanup_verified = [True]
            h.assert_certain()
            h.driver_cleanup_verified = [False]
            with self.assertRaisesRegex(ValueError, 'cleanup lacks'):
                h.assert_certain()

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
        h.drivers = [Mock(follow_thread=None, terminal_owner=None, record=Mock(receipts=[{"effects_uncertain": True}], pending_interactions=[]))]
        h.driver_cleanup_verified = [True]
        with self.assertRaisesRegex(driver.Rejected, "uncertain"):
            h.remove_owned()
        h.docker.assert_not_called()

    def test_unverified_or_missing_driver_cleanup_blocks_parent_removal(self):
        for count, flags in ((1, []), (1, [False]), (1, [True, True]),
                             (2, [True]), (2, [True, False]), (2, [False, True])):
            with self.subTest(drivers=count, verified=flags):
                h = self.harness()
                h.drivers = [Mock(follow_thread=None, terminal_owner=None, record=Mock(receipts=[{"effects_uncertain": False}], pending_interactions=[])) for _ in range(count)]
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
        h.drivers = [Mock(follow_thread=None, terminal_owner=None, record=Mock(receipts=[{"effects_uncertain": False}], pending_interactions=[])) for _ in range(2)]
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
        h.drivers = [Mock(follow_thread=None, terminal_owner=None, record=Mock(receipts=[{"effects_uncertain": False}], pending_interactions=[]))]
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

    def test_close_interval_waits_for_every_sibling_sample_after_begin(self):
        monitor = gate.SentinelMonitor.__new__(gate.SentinelMonitor)
        monitor.check = Mock()
        monitor.finished = threading.Event()
        monitor.rows = [{"descriptor": {"name": x}} for x in ("active", "sibling", "neighbor")]
        monitor.samples = [{"context": "sibling", "unix_ns": 15}, {"context": "neighbor", "unix_ns": 9}]
        with self.assertRaisesRegex(driver.Rejected, "interval deadline"):
            monitor.close_interval(10, "active", deadline_seconds=0.2)
        monitor.samples.append({"context": "neighbor", "unix_ns": 12})
        self.assertGreaterEqual(monitor.close_interval(10, "active", deadline_seconds=0.2), 12)

    def test_liveness_requires_every_neighbor_during_exact_interval(self):
        monitor = gate.SentinelMonitor.__new__(gate.SentinelMonitor)
        monitor.check = Mock()
        monitor.rows = [{"descriptor": {"name": x}} for x in ("active", "sibling", "neighbor")]
        monitor.samples = [{"context": "sibling", "unix_ns": 15}, {"context": "neighbor", "unix_ns": 9}]
        with self.assertRaisesRegex(driver.Rejected, "contemporaneous"):
            monitor.check_interval(10, 20, "active")
        monitor.samples.append({"context": "neighbor", "unix_ns": 19})
        monitor.check_interval(10, 20, "active")

    def monitor_loop_fixture(self, excluded=()):
        """A monitor whose loop runs exactly one pass and records what it sampled."""
        monitor = gate.SentinelMonitor.__new__(gate.SentinelMonitor)
        monitor.finished, monitor.first = threading.Event(), threading.Event()
        monitor.errors, monitor.samples = [], []
        monitor.rows = [{"descriptor": {"name": x}} for x in ("active", "sibling", "neighbor")]
        monitor.excluded = frozenset(excluded)
        sampled = []
        def sample(row):
            sampled.append(row["descriptor"]["name"])
            monitor.finished.set()
        monitor.sample = sample
        return monitor, sampled

    def test_the_loop_skips_the_machine_whose_journal_is_spent_elsewhere(self):
        monitor, sampled = self.monitor_loop_fixture(excluded=("active",))
        monitor.loop()
        # Every liveness assertion already excludes the active Machine, so
        # sampling it only consumed its bounded runtime-audit journal.
        self.assertEqual(sampled, ["sibling", "neighbor"])
        self.assertEqual(monitor.errors, [])
        self.assertTrue(monitor.first.is_set())

    def test_an_unexcluded_loop_samples_every_machine(self):
        monitor, sampled = self.monitor_loop_fixture()
        monitor.loop()
        self.assertEqual(sampled, ["active", "sibling", "neighbor"])

    def test_excluding_restores_the_previous_set_even_when_the_block_fails(self):
        monitor = gate.SentinelMonitor.__new__(gate.SentinelMonitor)
        monitor.rows = [{"descriptor": {"name": x}} for x in ("active", "sibling")]
        monitor.excluded = frozenset()
        with monitor.excluding("active"):
            self.assertEqual(monitor.excluded, frozenset({"active"}))
            # Nesting adds rather than replaces, so an inner pause cannot
            # silently re-expose the Machine an outer block is protecting.
            with monitor.paused():
                self.assertEqual(monitor.excluded, frozenset({"active", "sibling"}))
            self.assertEqual(monitor.excluded, frozenset({"active"}))
        self.assertEqual(monitor.excluded, frozenset())
        with self.assertRaisesRegex(ValueError, "workload failed"):
            with monitor.excluding("active"):
                raise ValueError("workload failed")
        self.assertEqual(monitor.excluded, frozenset())

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
