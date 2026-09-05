"""Offline adversarial tests only; no Docker invocation or physical certification."""

import copy
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import docker_host_driver as driver


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        clients = {}
        for name in ("docker", "compose", "buildx"):
            path = self.root / ("docker" if name == "docker" else "docker-" + name)
            path.write_bytes(b"#!/bin/sh\nexit 99\n")
            path.chmod(0o700)
            clients[name] = {"path": str(path), "sha256": driver.sha256(path.read_bytes())}
        config = self.root / "config"
        config.mkdir(mode=0o700)
        (config / "config.json").write_text(json.dumps({"currentContext": "unused-decoy",
                                                       "cliPluginsExtraDirs": [str(self.root)]}))
        self.raw = {
            "schema_version": 1, "run_id": "fixture-run-12345678",
            "release_sha256": "a" * 64, "fixture_sha256": "b" * 64,
            "scope": {"project_id": "p", "environment_id": "e", "machine_id": "m",
                      "machine_incarnation": "i", "runtime_identity": "r",
                      "docker_context": "vz-owned-machine", "docker_endpoint": "unix://" + str(self.root / "machine.sock"),
                      "engine_id": "owned-engine"},
            "docker_config": str(config), "clients": clients,
            "images": {"base": {"reference": "registry.test/base@sha256:" + "c" * 64,
                                "id": "sha256:" + "d" * 64, "platform": "linux/arm64"},
                       "compose": {"reference": "sha256:" + "e" * 64,
                                   "id": "sha256:" + "e" * 64, "platform": "linux/arm64"}},
            "builder": {"name": "vz-owned-builder", "node": "vz-owned-builder0",
                        "container_id": "f" * 64, "image_id": "sha256:" + "0" * 64}}

    def bare_driver(self):
        item = driver.Driver.__new__(driver.Driver)
        item.inputs = driver.Inputs(copy.deepcopy(self.raw))
        item.home = self.root / "private-home"
        item.home.mkdir(exist_ok=True)
        item.output = self.root
        item.config_snapshot = item.validate_config()
        item.record = driver.Recorder(self.root, {"HOME": str(item.home), "PATH": "/usr/bin:/bin"}, [])
        item.observations = []
        item.projects = {}
        return item

    def test_valid_boundary_requires_no_process(self):
        with patch.object(driver, "execute") as process:
            inputs = driver.Inputs(self.raw)
        process.assert_not_called()
        self.assertTrue(inputs.owner.startswith("vz04-"))

    def test_mutable_images_rejected_before_process(self):
        for reference in ("python:latest", "python:3.13", "sha256:" + "f" * 63,
                          "repo@sha256:" + "F" * 64, "--privileged"):
            with self.subTest(reference=reference), patch.object(driver, "execute") as process:
                raw = copy.deepcopy(self.raw)
                raw["images"]["base"]["reference"] = reference
                with self.assertRaises(driver.Rejected):
                    driver.Inputs(raw)
                process.assert_not_called()

    def test_wrong_platform_rejected(self):
        self.raw["images"]["base"]["platform"] = "linux/amd64"
        with self.assertRaises(driver.Rejected):
            driver.Inputs(self.raw)

    def test_global_contexts_and_remote_endpoints_rejected(self):
        for context in ("default", "desktop-linux", "orbstack", "--host"):
            raw = copy.deepcopy(self.raw)
            raw["scope"]["docker_context"] = context
            with self.subTest(context=context), self.assertRaises(driver.Rejected):
                driver.Inputs(raw)
        for endpoint in ("tcp://localhost:2375", "ssh://user@host", "unix:///var/run/docker.sock"):
            raw = copy.deepcopy(self.raw)
            raw["scope"]["docker_endpoint"] = endpoint
            with self.subTest(endpoint=endpoint), self.assertRaises(driver.Rejected):
                driver.Inputs(raw)

    def test_pin_and_symlink_rejected(self):
        raw = copy.deepcopy(self.raw)
        raw["clients"]["docker"]["sha256"] = "0" * 64
        with self.assertRaises(driver.Rejected):
            driver.Inputs(raw)
        target = self.root / "alias"
        target.symlink_to(self.root / "docker")
        raw["clients"]["docker"]["path"] = str(target)
        with self.assertRaises(driver.Rejected):
            driver.Inputs(raw)

    def test_client_config_rejects_credentials_and_helpers(self):
        item = self.bare_driver()
        path = Path(item.inputs.raw["docker_config"]) / "config.json"
        for key in ("credsStore", "credHelpers", "auths", "proxies"):
            path.write_text(json.dumps({"cliPluginsExtraDirs": [str(self.root)], key: "forbidden"}))
            with self.subTest(key=key), self.assertRaises(driver.Rejected):
                item.validate_config()

    def test_command_pins_explicit_config_context_and_records_exact_bytes(self):
        item = self.bare_driver()
        completed = subprocess.CompletedProcess([], 37, b"out\x00\n", b"err\n")
        with patch.object(driver, "execute", return_value=completed) as process:
            result = item.command(["compose", "version"], expected=37)
        self.assertEqual(result.stdout, b"out\x00\n")
        argv = process.call_args.args[0]
        self.assertEqual(argv[:5], ["docker", "--config", self.raw["docker_config"],
                                   "--context", "vz-owned-machine"])
        self.assertEqual(process.call_args.kwargs["executable"], self.raw["clients"]["docker"]["path"])
        env = process.call_args.kwargs["env"]
        self.assertFalse(any(key.startswith(("DOCKER_", "BUILDX_", "COMPOSE_", "SSH_")) for key in env))
        self.assertEqual((self.root / "command-00001.stdout").read_bytes(), b"out\x00\n")
        self.assertEqual(item.record.receipts[0]["exit_code"], 37)
        self.assertGreater(item.record.receipts[0]["elapsed_ns"], 0)
        self.assertEqual(item.record.receipts[0]["argv0"], "docker")
        self.assertEqual(item.record.receipts[0]["executable"], self.raw["clients"]["docker"]["path"])
        self.assertFalse(item.record.receipts[0]["effects_uncertain"])

    def test_real_isolated_multicall_process_receives_docker_argv0(self):
        # A compiled local fixture actually sees kernel-provided argv[0]. A
        # shebang script cannot establish this because its interpreter rewrites
        # argv. This invokes neither the user's Docker binary nor any daemon.
        binary = self.root / "docker-tools"
        source = b'''#include <stdio.h>
#include <string.h>
int main(int argc, char **argv) {
    (void)argc;
    puts(argv[0]);
    return strcmp(argv[0], "docker") == 0 ? 0 : 73;
}
'''
        compiled = subprocess.run(["/usr/bin/clang", "-x", "c", "-", "-o", str(binary)], input=source,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False)
        self.assertEqual(compiled.returncode, 0, compiled.stderr)
        self.raw["clients"]["docker"] = {"path": str(binary), "sha256": driver.sha256(binary.read_bytes())}
        item = self.bare_driver()
        result = item.command(["version"])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"docker\n")
        receipt = item.record.receipts[0]
        self.assertEqual(receipt["executable"], str(binary))
        self.assertEqual(receipt["argv"][0], "docker")
        wrong = driver.execute([str(binary)], executable=str(binary), timeout=5, check=False,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertEqual(wrong.returncode, 73)

    def test_inflight_uncertainty_is_durable_before_dispatch(self):
        item = self.bare_driver()

        def inspect_intent(*args, **kwargs):
            intent = json.loads((self.root / "command-00001.intent.json").read_text())
            self.assertEqual(intent["host_outcome"], "inflight")
            self.assertTrue(intent["effects_uncertain"])
            self.assertTrue(item.record.receipts[0]["effects_uncertain"])
            self.assertTrue(item.cleanup())
            return subprocess.CompletedProcess([], 0, b"", b"")

        with patch.object(driver, "execute", side_effect=inspect_intent):
            item.command(["version"])
        self.assertFalse(item.record.receipts[0]["effects_uncertain"])

    def test_keyboard_interrupt_retained_even_after_owned_group_is_reaped(self):
        item = self.bare_driver()
        interrupted = KeyboardInterrupt()
        interrupted.stdout, interrupted.stderr = b"dispatched-before-interrupt", b"partial"
        with patch.object(subprocess, "Popen") as popen, patch.object(os, "killpg") as kill, \
                patch.object(driver, "collect_output", side_effect=interrupted):
            process = popen.return_value.__enter__.return_value
            process.pid = 45678
            process.returncode = None
            with self.assertRaises(KeyboardInterrupt):
                item.command(["compose", "up"])
        kill.assert_called_once_with(45678, driver.signal.SIGKILL)
        self.assertEqual(process.wait.call_count, 1)
        receipt = json.loads((self.root / "command-00001.json").read_text())
        self.assertTrue(receipt["interrupted"])
        self.assertTrue(receipt["effects_uncertain"])
        self.assertEqual(receipt["host_outcome"], "interrupted")
        self.assertEqual((self.root / "command-00001.stdout").read_bytes(), b"dispatched-before-interrupt")
        with patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        destructive.assert_not_called()

    def test_unknown_spawn_observation_withholds_destructive_cleanup(self):
        item = self.bare_driver()
        with patch.object(driver, "execute", side_effect=OSError("dispatch state unknown")), self.assertRaises(OSError):
            item.command(["compose", "up"])
        receipt = json.loads((self.root / "command-00001.json").read_text())
        self.assertEqual(receipt["host_outcome"], "unknown")
        self.assertTrue(receipt["effects_uncertain"])
        with patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        destructive.assert_not_called()

    def test_interrupted_terminal_receipt_write_leaves_inflight_uncertainty(self):
        item = self.bare_driver()
        persist = item.record.persist

        def interrupt_terminal(path, value, **kwargs):
            if not path.name.endswith(".intent.json"):
                raise KeyboardInterrupt()
            persist(path, value, **kwargs)

        with patch.object(driver, "execute", return_value=subprocess.CompletedProcess([], 0, b"", b"")), \
                patch.object(item.record, "persist", side_effect=interrupt_terminal), self.assertRaises(KeyboardInterrupt):
            item.command(["compose", "up"])
        self.assertTrue(item.record.receipts[0]["effects_uncertain"])
        self.assertEqual(item.record.receipts[0]["host_outcome"], "inflight")
        self.assertTrue((self.root / "command-00001.intent.json").is_file())
        with patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        destructive.assert_not_called()

    def test_cleanup_uncertainty_stops_all_remaining_projects(self):
        item = self.bare_driver()
        item.projects = {"first": {}, "second": {}}

        def failed_observation():
            item.record.receipts.append({"effects_uncertain": True, "timed_out": True})
            raise driver.Rejected("cleanup routing observation timed out")

        with patch.object(item, "guard", side_effect=failed_observation) as guard, \
                patch.object(item, "compose") as destructive:
            self.assertTrue(item.cleanup())
        self.assertEqual(guard.call_count, 1)
        destructive.assert_not_called()

    def test_noisy_synthetic_process_is_bounded_reaped_and_never_a_success(self):
        # Invoke only the test interpreter, never Docker. Each case writes
        # forever to one chosen stream, exercising independent pipe bounds.
        for stream in (1, 2):
            with self.subTest(stream=stream):
                root = self.root / f"noise-{stream}"
                root.mkdir()
                record = driver.Recorder(root, {"PATH": "/usr/bin:/bin"}, [], max_stream_bytes=1024)
                code = f"import os; os.write({stream}, (str(os.getpid()) + '\\n').encode()); exec('while True: os.write({stream}, b\\\"x\\\" * 4096)')"
                with self.assertRaises(driver.OutputLimitExceeded):
                    record.run([sys.executable, "-c", code], executable=sys.executable, timeout=5)
                receipt = record.receipts[0]
                self.assertTrue(receipt["output_limit_exceeded"])
                self.assertTrue(receipt["effects_uncertain"])
                self.assertFalse(receipt["capture_complete"])
                self.assertFalse(receipt["raw_streams_retained"])
                self.assertIsNone(receipt["raw_stdout_sha256"])
                self.assertIsNone(receipt["raw_stderr_sha256"])
                name = "stdout" if stream == 1 else "stderr"
                raw = (root / ("command-00001." + name)).read_bytes()
                self.assertEqual(len(raw), 1024)
                self.assertEqual(receipt["observed_bytes"][name], 1025)
                self.assertEqual(receipt["retained_observed_" + name + "_sha256"], driver.sha256(raw))
                pid = int(raw.split(b"\n", 1)[0])
                with self.assertRaises(ChildProcessError):
                    os.waitpid(pid, os.WNOHANG)
                item = self.bare_driver()
                item.record = record
                with patch.object(item, "compose") as cleanup:
                    self.assertTrue(item.cleanup())
                cleanup.assert_not_called()

    def test_real_dual_stream_at_exact_limit_completes_without_truncation(self):
        code = "import os; os.write(1, b'a' * 1024); os.write(2, b'b' * 1024)"
        result = driver.execute([sys.executable, "-c", code], executable=sys.executable,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, check=False,
                                max_stream_bytes=1024)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"a" * 1024)
        self.assertEqual(result.stderr, b"b" * 1024)

    def test_real_partial_output_timeout_preserves_only_observed_prefix(self):
        root = self.root / "partial-timeout"
        root.mkdir()
        record = driver.Recorder(root, {"PATH": "/usr/bin:/bin"}, [], max_stream_bytes=1024)
        result = record.run([sys.executable, "-c", "import os,time;os.write(1,b'partial');time.sleep(10)"],
                            executable=sys.executable, timeout=0.2)
        self.assertTrue(result.timed_out)
        self.assertEqual(result.stdout, b"partial")
        self.assertIsNone(record.receipts[0]["raw_stdout_sha256"])
        self.assertEqual(record.receipts[0]["retained_observed_stdout_sha256"], driver.sha256(b"partial"))

    def test_changed_executable_rejected_before_command(self):
        item = self.bare_driver()
        (self.root / "docker-compose").write_bytes(b"modified\n")
        with patch.object(driver, "execute") as process, self.assertRaises(driver.Rejected):
            item.command(["compose", "version"])
        process.assert_not_called()

    def test_timeout_is_recorded_once_not_expected_negative_success(self):
        item = self.bare_driver()
        with patch.object(driver, "execute", side_effect=subprocess.TimeoutExpired([], 1, output=b"partial")) as process:
            with self.assertRaises(driver.Rejected):
                item.command(["version"])
        self.assertEqual(process.call_count, 1)
        self.assertTrue(item.record.receipts[0]["timed_out"])
        self.assertEqual((self.root / "command-00001.stdout").read_bytes(), b"partial")

    def test_secret_leak_is_failure_and_never_published(self):
        item = self.bare_driver()
        item.record.canaries = [b"PRIVATE-CANARY"]
        with patch.object(driver, "execute", return_value=subprocess.CompletedProcess([], 0, b"PRIVATE-CANARY", b"")):
            with self.assertRaises(driver.Rejected):
                item.command(["version"])
        for path in self.root.glob("command-*"):
            self.assertNotIn(b"PRIVATE-CANARY", path.read_bytes())
        self.assertTrue(item.record.receipts[0]["secret_leak_detected"])
        self.assertFalse(item.record.receipts[0]["raw_streams_retained"])

    def test_wrong_context_rejected_before_engine_contact(self):
        item = self.bare_driver()
        with patch.object(item, "json_command", return_value=[{
            "Name": "vz-owned-machine", "Endpoints": {"docker": {"Host": "unix:///foreign.sock"}}}]) as command:
            with self.assertRaises(driver.Rejected):
                item.guard()
        self.assertEqual(command.call_count, 1)

    def test_wrong_engine_and_runtime_rejected(self):
        item = self.bare_driver()
        context = [{"Name": "vz-owned-machine", "Endpoints": {"docker": {"Host": self.raw["scope"]["docker_endpoint"]}}}]
        info = {"ID": "owned-engine", "OSType": "linux", "Architecture": "aarch64",
                "DefaultRuntime": "youki", "Runtimes": {"youki": {}}}
        for changed in ({"ID": "foreign-engine"}, {"DefaultRuntime": "runc"},
                        {"Runtimes": {"youki": {}, "runc": {}}}):
            with self.subTest(changed=changed), patch.object(item, "json_command", side_effect=[context, info | changed]), \
                    patch.object(driver.stat, "S_ISSOCK", return_value=True), \
                    patch.object(Path, "stat"), self.assertRaises(driver.Rejected):
                item.guard()

    def test_foreign_owner_resource_cannot_be_captured_or_deleted(self):
        item = self.bare_driver()
        foreign = {"Id": "f" * 64, "Config": {"Labels": {"com.docker.compose.project": "owned-project"}}}
        with patch.object(item, "command", return_value=driver.Command(1, [], 0, b"container-id\n", b"")), \
                patch.object(item, "json_command", return_value=[foreign]), self.assertRaises(driver.Rejected):
            item.inspect_project("owned-project")

    def test_empty_existing_project_claim_rejected(self):
        item = self.bare_driver()
        with patch.object(item, "guard"), patch.object(item, "inspect_project", return_value={"container": [{}]}), \
                self.assertRaises(driver.Rejected):
            item.new_project("compose")
        self.assertEqual(item.projects, {})

    def test_observation_cannot_pass_without_assertions(self):
        item = self.bare_driver()
        with self.assertRaises(driver.Rejected):
            item.observe("bad", ["docker.compose.up"], lambda: [])
        self.assertEqual(item.observations[0]["outcome"], "failed")


class AssertionTests(unittest.TestCase):
    def test_owned_process_group_is_killed_on_timeout(self):
        with patch.object(subprocess, "Popen") as popen, patch.object(os, "killpg") as kill, \
                patch.object(driver, "collect_output", side_effect=subprocess.TimeoutExpired(["owned"], 2)):
            process = popen.return_value.__enter__.return_value
            process.pid = 12345
            process.returncode = None
            with self.assertRaises(subprocess.TimeoutExpired):
                driver.execute(["owned"], timeout=2, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            kill.assert_called_once_with(12345, driver.signal.SIGKILL)
            self.assertEqual(process.wait.call_count, 1)

    def test_base64_split_buildkit_logs_are_scanned_for_canaries(self):
        raw = b"\n".join(json.dumps({"data": base64.b64encode(part).decode()}).encode()
                         for part in (b"PRIVATE-", b"CANARY"))
        self.assertTrue(driver.contains_canary((raw,), [b"PRIVATE-CANARY"]))
        self.assertFalse(driver.contains_canary((b"ordinary output",), [b"PRIVATE-CANARY"]))

    def event(self, actor, action, timestamp):
        return {"Type": "container", "Actor": {"ID": actor, "Attributes": {"com.docker.compose.project": "owned"}},
                "Action": action, "timeNano": timestamp}

    def healthy_events(self):
        return [self.event("db", "health_status: healthy", 1), self.event("api", "start", 2),
                self.event("api", "health_status: healthy", 3), self.event("worker", "start", 4)]

    def test_event_order_is_engine_timestamp_not_arrival_order(self):
        driver.assert_health_order(list(reversed(self.healthy_events())), {"db": "db", "api": "api", "worker": "worker"}, "owned")

    def test_missing_foreign_repeated_and_wrong_order_events_rejected(self):
        for events in (self.healthy_events()[:-1], self.healthy_events() + [self.event("foreign", "start", 10)],
                       self.healthy_events() + [self.event("api", "start", 20)],
                       [self.event("db", "health_status: healthy", 5), *self.healthy_events()[1:]]):
            with self.subTest(events=events), self.assertRaises(driver.Rejected):
                driver.assert_health_order(events, {"db": "db", "api": "api", "worker": "worker"}, "owned")

    def test_cache_requires_terminal_exact_vertex_and_observed_cached_boolean(self):
        vertex = {"id": "sha256:" + "a" * 64, "name": "[build 3/3] RUN --network=none python3 /fixture/tools.py payload",
                  "completed": "2026-09-05T00:00:00Z", "cached": True}
        raw = json.dumps(vertex).encode()
        driver.assert_payload_vertex(raw, cached=True)
        with self.assertRaises(driver.Rejected):
            driver.assert_payload_vertex(raw, cached=False)
        for bad in (b"", b"CACHED", json.dumps(vertex | {"completed": None}).encode(), raw + b"\n" + raw):
            with self.subTest(bad=bad), self.assertRaises((driver.Rejected, ValueError)):
                driver.assert_payload_vertex(bad, cached=True)

    def test_builder_single_exact_node(self):
        builder = {"name": "builder", "node": "builder0"}
        raw = b"Name: builder\nDriver: docker-container\n\nNodes:\nName: builder0\nEndpoint: machine\nStatus: running\n"
        driver.assert_builder_inspect(raw, builder, "machine")
        for bad in (raw.replace(b"machine", b"foreign"), raw + b"Name: another\n", raw + b"Error: failed\n",
                    raw.replace(b"running", b"stopped")):
            with self.subTest(bad=bad), self.assertRaises(driver.Rejected):
                driver.assert_builder_inspect(bad, builder, "machine")

    def test_exports_reject_extra_files_symlinks_and_wrong_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "payload.txt"
            path.write_bytes(b"exact")
            driver.assert_export(root, "payload.txt", b"exact")
            with self.assertRaises(driver.Rejected):
                driver.assert_export(root, "payload.txt", b"wrong")
            (root / "intermediate-canary").write_bytes(b"leaked")
            with self.assertRaises(driver.Rejected):
                driver.assert_export(root, "payload.txt", b"exact")

    def test_result_cannot_claim_release_pass_or_no_work_success(self):
        result = {"schema_version": 1, "compatibility_certified": False, "release_scenarios_passed": [],
                  "test_case_retries": 0, "observations": [], "outcome": "fixture_assertions_passed",
                  "failure": None, "cleanup_errors": [], "suite": "compose", "command_count": 0}
        with self.assertRaises(driver.Rejected):
            driver.validate_result(result)
        result["outcome"] = "failed"
        driver.validate_result(result)
        for key, value in (("compatibility_certified", True), ("release_scenarios_passed", ["docker.compose.up"]),
                           ("test_case_retries", 1)):
            with self.subTest(key=key), self.assertRaises(driver.Rejected):
                driver.validate_result(result | {key: value})

    def test_result_rejects_duplicate_recipe_and_unexecuted_success(self):
        observations = [{"recipe": recipe, "outcome": "fixture_assertions_passed", "first_command": index + 1,
                         "last_command": index + 1, "assertions": ["offline fixture assertion"]}
                        for index, recipe in enumerate(driver.COMPOSE_RECIPES)]
        observation = observations[0]
        result = {"schema_version": 1, "compatibility_certified": False, "release_scenarios_passed": [],
                  "test_case_retries": 0, "observations": observations, "outcome": "fixture_assertions_passed",
                  "failure": None, "cleanup_errors": [], "suite": "compose", "command_count": len(observations)}
        driver.validate_result(result)
        with self.assertRaises(driver.Rejected):
            driver.validate_result(result | {"observations": [observation, observation]})
        with self.assertRaises(driver.Rejected):
            driver.validate_result(result | {"observations": [observation | {"last_command": 0}]})

    def test_timeout_never_dispatches_cleanup(self):
        item = driver.Driver.__new__(driver.Driver)
        item.record = type("Record", (), {"receipts": [{"timed_out": True}]})()
        item.projects = {"owned": {}}
        with patch.object(item, "compose") as compose:
            errors = item.cleanup()
        self.assertTrue(errors)
        compose.assert_not_called()


if __name__ == "__main__":
    unittest.main()
