"""Offline admission/ownership regressions, not physical Docker evidence."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import docker_host_driver as driver
import linux_docker_e2e as gate


class AdmissionTests(unittest.TestCase):
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
    def harness(self):
        h = gate.ComposeHarness.__new__(gate.ComposeHarness)
        h.effects_uncertain = False
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        h.evidence = Path(temporary.name)
        h.mutations = []
        h.record = Mock(receipts=[])
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
            unknown = Mock(receipts=[{"effects_uncertain": True}])
            if source == "parent":
                h.record = unknown
            else:
                h.monitor = Mock(record=unknown, thread=Mock(is_alive=Mock(return_value=False)))
            with self.assertRaisesRegex(driver.Rejected, "uncertain"):
                h.remove_owned()
            h.docker.assert_not_called()

    def test_driver_uncertainty_prevents_cleanup(self):
        h = self.harness()
        h.drivers = [Mock(record=Mock(receipts=[{"effects_uncertain": True}]))]
        h.driver_cleanup_verified = [True]
        with self.assertRaisesRegex(driver.Rejected, "uncertain"):
            h.remove_owned()
        h.docker.assert_not_called()

    def test_unverified_or_missing_driver_cleanup_blocks_parent_removal(self):
        for count, flags in ((1, []), (1, [False]), (1, [True, True]),
                             (2, [True]), (2, [True, False]), (2, [False, True])):
            with self.subTest(drivers=count, verified=flags):
                h = self.harness()
                h.drivers = [Mock(record=Mock(receipts=[{"effects_uncertain": False}])) for _ in range(count)]
                h.driver_cleanup_verified = flags
                # An owned object ensures the test catches any cleanup dispatch,
                # not merely successful return from an empty ownership loop.
                h.owned = [{"descriptor": {"name": "exact-context"}, "token": "owned",
                            "tag": "owned:fixture", "image_id": "sha256:" + "a" * 64}]
                h.mutate, h.exact_absent = Mock(), Mock()
                with self.assertRaisesRegex(driver.Rejected, "Compose cleanup lacks successful independent replay"):
                    h.remove_owned()
                h.docker.assert_not_called()
                h.mutate.assert_not_called()
                h.exact_absent.assert_not_called()

    def test_all_driver_cleanup_proofs_allow_exact_owned_parent_removal(self):
        h = self.harness()
        h.drivers = [Mock(record=Mock(receipts=[{"effects_uncertain": False}])) for _ in range(2)]
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
