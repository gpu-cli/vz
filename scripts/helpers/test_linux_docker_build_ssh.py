"""SSH solve specification/staging checks; no Engine, VM, or agent starts."""
import copy
from contextlib import ExitStack
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import linux_docker_build_ssh as ssh


class SSHBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-ssh-build-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.request = {"schema_version": 1, "token": "vzssh-" + "a" * 24, "host": "172.17.0.3",
                        "port": 2222, "host_key_fingerprint": "SHA256:" + "A" * 43}
        self.known = b"[172.17.0.3]:2222 ssh-ed25519 public-test-value\n"
        self.inputs = {"run_id": "ssh-" + "a" * 24, "builder": {"name": "owned-builder"},
                       "images": {"base": {"reference": ssh.packages.load()["base"]["reference"]}}}

    def stage(self, request=True):
        def fake_packages(source, destination):
            destination.mkdir(mode=0o700)
            (destination / "manifest.json").write_text("{}\n")
        with patch.object(ssh.packages, "stage_packages", side_effect=fake_packages):
            return ssh.stage_context(ssh.FIXTURE, self.root, self.root / "context",
                                     request=self.request if request else None, known_hosts=self.known if request else None)

    def test_frozen_fixture_and_denials_before_positive(self):
        contract = ssh.fixture_contract(ssh.FIXTURE)
        self.assertEqual(set(contract["recipes"]), set(ssh.CASES))
        self.assertEqual(ssh.CASES[-1], "declared")

    def test_request_rejects_nonprivate_or_ambiguous_scope(self):
        for key, value in (("host", "127.0.0.1"), ("host", "0.0.0.0"), ("host", "169.254.0.1"),
                           ("host", "8.8.8.8"), ("host", "172.017.0.3"), ("host", "::1"),
                           ("port", True), ("port", 22), ("schema_version", True),
                           ("token", "foreign"), ("host_key_fingerprint", "MD5:unknown")):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                ssh.public_request({**self.request, key: value})
        with self.assertRaises(ValueError):
            ssh.public_request({**self.request, "private_key": "forbidden"})

    def test_public_staging_excludes_unrelated_source_files(self):
        (self.root / "private-auth").write_bytes(b"private must not copy")
        proof = self.stage()
        context = Path(proof["path"])
        self.assertEqual(set(p.name for p in context.iterdir()), ssh.FIXTURE_FILES | {"packages", "inputs"})
        self.assertEqual((context / "inputs/known_hosts").read_bytes(), self.known)
        self.assertEqual(json.loads((context / "inputs/request.json").read_bytes()), self.request)
        self.assertEqual(proof["sha256"], ssh.driver.tree_digest(context))

    def test_server_context_has_no_authentication_inputs(self):
        context = Path(self.stage(request=False)["path"])
        self.assertFalse((context / "inputs").exists())
        self.assertEqual({p.name for p in context.iterdir()}, ssh.FIXTURE_FILES | {"packages"})

    def test_existing_or_redirected_context_is_never_reused(self):
        context = self.root / "context"
        context.mkdir()
        (context / "sentinel").write_bytes(b"preserve")
        with self.assertRaises(ValueError):
            self.stage()
        self.assertEqual((context / "sentinel").read_bytes(), b"preserve")

    def test_arguments_are_explicitly_scoped_cold_and_per_case(self):
        context = self.stage()["path"]
        socket = self.root / "test-socket"
        socket.write_bytes(b"not a real agent")
        for case in ssh.CASES:
            with self.subTest(case=case), patch.object(ssh.stat, "S_ISSOCK", return_value=True):
                operation = ssh.specification(case, self.root / case, self.inputs, context, self.request,
                                              None if case == "provider_omitted" else socket)
            args = operation["build_argv"]
            self.assertEqual(args, ssh.build_arguments(self.inputs, operation))
            self.assertEqual(args[args.index("--builder") + 1], "owned-builder")
            self.assertIn("--no-cache", args)
            self.assertIn("--network=default", args)
            self.assertNotIn("--cache-from", args)
            self.assertEqual("--ssh" in args, case != "provider_omitted")
            if "--ssh" in args:
                self.assertEqual(args[args.index("--ssh") + 1], "fixture=" + str(socket))
            self.assertEqual("--cache-to" in args, case == "declared")
            filename = "Dockerfile.undeclared" if case == "undeclared" else "Dockerfile.ssh"
            self.assertEqual(args[args.index("--file") + 1], str(Path(context) / filename))

    def test_private_key_file_is_not_accepted_as_ssh_provider(self):
        context = self.stage()["path"]
        private = self.root / "auth"
        private.write_bytes(b"private key placeholder")
        with self.assertRaisesRegex(ValueError, "agent socket"):
            ssh.specification("declared", self.root / "solve", self.inputs, context, self.request, private)

    def test_provider_presence_cannot_change_case(self):
        context = self.stage()["path"]
        for case, provider in (("declared", None), ("provider_omitted", self.root / "socket")):
            with self.assertRaisesRegex(ValueError, "provider presence"):
                ssh.specification(case, self.root / "solve", self.inputs, context, self.request, provider)

    def test_staged_request_tamper_rejected(self):
        context = self.stage()["path"]
        changed = copy.deepcopy(self.request)
        changed["host"] = "172.17.0.4"
        with self.assertRaisesRegex(ValueError, "request differs"):
            ssh.specification("provider_omitted", self.root / "solve", self.inputs, context, changed, None)


class SSHExecuteTests(unittest.TestCase):
    """Exercise the real execute transaction with only external boundaries mocked."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-ssh-execute-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.events = []
        self.item = object.__new__(ssh.SSHDriver)
        self.item.output = self.root
        self.item.fixture = self.root / "original-fixture"
        self.item.inputs = SimpleNamespace(raw={"fixture_sha256": "old-fixture"})
        self.item.record = SimpleNamespace(count=0, canaries=[b"disposable-test-private-canary-123456789"])
        self.command = SimpleNamespace(stdout=b"", timed_out=False, returncode=1)
        self.operation = {"case": "undeclared", "ssh_fixture": str(self.root / "ssh-fixture"),
            "ssh_fixture_sha256": "ssh-fixture", "build_context": str(self.root / "context"),
            "build_context_sha256": "context", "agent_socket": str(self.root / "socket"),
            "output": str(self.root / "oci"), "cache_output": None, "request": {"token": "token"},
            "build_argv": ["buildx", "build", "exact-test-arguments"]}
        def guard():
            self.events.append("guard")
            self.item.record.count += 4
        def command(*args, **kwargs):
            self.events.append("build")
            self.item.record.count += 1
            return self.command
        def acknowledge(*args):
            self.events.append("acknowledge")
        self.item.builder_guard = Mock(side_effect=guard)
        self.item.command = Mock(side_effect=command)
        self.item.record.acknowledge_negative = Mock(side_effect=acknowledge)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(ssh, "specification", side_effect=lambda *a, **k: copy.deepcopy(self.operation)))
        self.stack.enter_context(patch.object(ssh.driver, "tree_digest", side_effect=lambda p:
            "old-fixture" if p == self.item.fixture else "ssh-fixture" if p.name == "ssh-fixture" else "context"))
        def replay(*args, **kwargs):
            self.events.append("replay-" + str(kwargs.get("require_ack", True)))
            return {"independent": "proved"}
        self.replay = self.stack.enter_context(patch("linux_docker_ssh_evidence.validate_operation", side_effect=replay))

    def test_negative_ack_only_after_first_independent_proof_then_replayed(self):
        result = self.item.execute(self.operation)
        self.assertEqual(self.events, ["guard", "build", "guard", "replay-False", "acknowledge", "replay-True"])
        self.assertEqual(self.item.record.count, 9)
        self.assertEqual(result["independent_validation"], {"independent": "proved"})
        self.assertEqual(result["artifact_validation"], {"oci": None, "cache": None})
        self.item.record.acknowledge_negative.assert_called_once_with(
            self.command, "terminal BuildKit SSH fixture undeclared denial")

    def test_replay_failure_preserves_original_and_never_acknowledges_negative(self):
        original = ValueError("injected independent denial mismatch")
        self.replay.side_effect = original
        with self.assertRaises(ValueError) as caught:
            self.item.execute(self.operation)
        self.assertIs(caught.exception, original)
        self.item.record.acknowledge_negative.assert_not_called()
        self.assertEqual(self.item.record.count, 9)
        self.assertTrue((self.root / "operation.json").is_file())
        self.assertTrue((self.root / "artifact-validation.json").is_file())

    def test_unexpected_negative_exit_never_reaches_replay_or_ack(self):
        self.command.returncode = 0
        with self.assertRaisesRegex(ValueError, "host exit differs"):
            self.item.execute(self.operation)
        self.replay.assert_not_called()
        self.item.record.acknowledge_negative.assert_not_called()

    def test_positive_validates_both_layouts_and_never_negative_acknowledges(self):
        self.operation.update(case="declared", cache_output=str(self.root / "cache"))
        self.command.returncode = 0
        with patch.object(ssh.layout, "validate_oci", return_value={"image": "proved"}) as image, \
             patch.object(ssh.layout, "validate_cache", return_value={"cache": "proved"}) as cache:
            result = self.item.execute(self.operation)
        image.assert_called_once()
        cache.assert_called_once_with(self.root / "cache", canaries=tuple(self.item.record.canaries))
        self.item.record.acknowledge_negative.assert_not_called()
        self.assertEqual(result["artifact_validation"], {"oci": {"image": "proved"}, "cache": {"cache": "proved"}})
        self.assertEqual(self.events[-2:], ["replay-False", "replay-True"])

    def test_export_preexistence_rejects_before_any_command(self):
        (self.root / "oci").mkdir()
        with self.assertRaisesRegex(ValueError, "export preexists"):
            self.item.execute(self.operation)
        self.assertEqual(self.events, [])
        self.replay.assert_not_called()


class SSHMachineTests(unittest.TestCase):
    """Run the real four-case orchestrator, never a real agent or Docker call."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-ssh-machine-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        evidence = self.root / "evidence"
        evidence.mkdir(mode=0o700)
        self.owner = {"project_id": "project", "environment_id": "environment", "machine_id": "machine"}
        self.request = {"schema_version": 1, "token": "vzssh-" + "a" * 24, "host": "172.17.0.3",
                        "port": 2222, "host_key_fingerprint": "SHA256:" + "A" * 43}
        self.inputs = {"run_id": "ssh-" + "b" * 24}
        self.events, self.operations, self.items = [], [], []
        self.builder = SimpleNamespace(mapping={"name": "owned-builder"},
            verify=Mock(side_effect=lambda **kwargs: {"runtime": "proved"}))
        self.harness = SimpleNamespace(evidence=evidence, root=self.root,
            info={"ssh_tools": {}, "ssh_fixture": str(ssh.FIXTURE), "ssh_packages": str(self.root / "packages"),
                  "fixture": str(self.root / "old-fixture")},
            prepare_builder=Mock(return_value=self.builder), drivers=["preexisting-driver"],
            driver_cleanup_verified=[True], ssh_cache_requests=[], sensitive_canaries=[],
            record=SimpleNamespace(canaries=[]),
            monitor=SimpleNamespace(check=Mock(), record=SimpleNamespace(canaries=[])))
        self.canaries = (b"disposable-test-private-canary-123456789",)
        public = self.root / "public-key"
        public.write_bytes(b"ssh-ed25519 public-placeholder disposable-comment\n")
        self.closure = {"agent_reaped": True, "private_inputs_removed": True, "cleanup_errors": []}
        self.agent = SimpleNamespace(start=Mock(return_value={"fingerprints": {"host": self.request["host_key_fingerprint"]}}),
            canaries=Mock(return_value=self.canaries),
            paths={"host_public_key": public, "wrong_host_public_key": public, "socket": self.root / "socket"},
            close=Mock(side_effect=self.close_agent))
        self.server = SimpleNamespace(driver="server-driver", prepare=Mock(return_value=self.request),
            verify=Mock(), cleanup_authorized=False, cleanup=Mock(side_effect=self.cleanup_server))
        self.admitted = SimpleNamespace(verify_runtime_evidence=Mock())
        self.execute_failure = self.replay_failure = None
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch("linux_docker_e2e.input_mapping", return_value=self.inputs))
        self.stack.enter_context(patch.object(ssh.driver, "Inputs", return_value=self.admitted))
        self.agent_class = self.stack.enter_context(patch("linux_docker_ssh_agent.Agent", return_value=self.agent))
        self.stack.enter_context(patch("linux_docker_ssh_server.Server", return_value=self.server))
        self.stack.enter_context(patch("linux_docker_buildkit_keep.verify_worker_log", return_value={"empty": True}))
        self.stack.enter_context(patch.object(ssh, "stage_context", side_effect=lambda fixture, source, destination, **kw:
            {"path": str(destination), "sha256": "staged"}))
        def specification(case, output, inputs, context, request, socket, **kwargs):
            operation = {"case": case, "output": str(output), "agent_socket": str(socket) if socket else None}
            self.operations.append(operation)
            return operation
        self.stack.enter_context(patch.object(ssh, "specification", side_effect=specification))
        self.stack.enter_context(patch.object(ssh, "SSHDriver", side_effect=self.new_driver))
        self.replay = self.stack.enter_context(patch("linux_docker_ssh_evidence.validate_operation", side_effect=self.validate))

    def close_agent(self):
        self.events.append("agent-close")
        self.assertTrue(all(flag is False for flag in self.harness.driver_cleanup_verified[1:]))
        return copy.deepcopy(self.closure)

    def cleanup_server(self):
        self.events.append("server-cleanup")
        self.assertTrue(self.server.cleanup_authorized)
        self.assertEqual(len(self.operations), 4)
        return {"owned_server_removed": True}

    def new_driver(self, inputs, fixture, output, *, canaries):
        self.assertIs(inputs, self.admitted)
        self.assertEqual(canaries, self.canaries)
        def execute(operation):
            self.events.append("execute-" + operation["case"])
            self.assertFalse(self.server.cleanup_authorized)
            self.assertEqual(self.harness.ssh_cache_requests, [])
            self.assertTrue(all(flag is False for flag in self.harness.driver_cleanup_verified[1:]))
            if self.execute_failure and operation["case"] == "provider_omitted":
                raise self.execute_failure
            return {"operation_contract": copy.deepcopy(operation), "independent_validation": {"case": operation["case"]}}
        item = SimpleNamespace(output=output, execute=Mock(side_effect=execute))
        self.items.append(item)
        return item

    def validate(self, directory, inputs, operation, **kwargs):
        self.events.append("replay-" + operation["case"])
        self.assertFalse(self.server.cleanup_authorized)
        self.assertEqual(kwargs["secret_canaries"], self.canaries)
        self.assertEqual(self.harness.ssh_cache_requests, [])
        if self.replay_failure:
            raise self.replay_failure
        return {"case": operation["case"]}

    def run_machine(self):
        return ssh.run_machine(self.harness, {"owner": self.owner}, {"scope": "owned"}, {}, {}, 0)

    def test_four_cases_close_agent_and_admit_cleanup_only_after_full_replay(self):
        result = self.run_machine()
        self.assertEqual([op["case"] for op in self.operations], list(ssh.CASES))
        self.assertIsNone(self.operations[1]["agent_socket"])
        self.assertEqual(self.events, ["execute-" + case for case in ssh.CASES] +
                         ["replay-" + case for case in ssh.CASES] + ["server-cleanup", "agent-close"])
        self.agent.close.assert_called_once()
        self.server.cleanup.assert_called_once()
        self.builder.verify.assert_called_once_with(require_invocation=True)
        self.assertEqual(self.harness.driver_cleanup_verified, [True] * 6)
        self.assertEqual(self.harness.ssh_cache_requests, [{"builder": self.builder, "canaries": self.canaries, "index": 0}])
        self.assertEqual(self.harness.sensitive_canaries, list(self.canaries))
        self.assertEqual(self.harness.record.canaries, list(self.canaries))
        self.assertEqual(self.harness.monitor.record.canaries, list(self.canaries))
        self.assertEqual(result["host_agent_cleanup"], self.closure)
        self.assertFalse(result["docker_parity_certified"])
        self.assertTrue((self.harness.evidence / "ssh-machine-0/machine-ssh-validation.json").is_file())
        self.agent_class.assert_called_once_with(self.root / "ssh-private-0", self.harness.evidence / "ssh-machine-0/agent",
            tools={}, run_id=self.inputs["run_id"] + "-0", owner=self.owner)

    def test_middle_case_failure_closes_agent_without_cleanup_or_cache_admission(self):
        self.execute_failure = ValueError("injected second-case failure")
        with self.assertRaises(ValueError) as caught:
            self.run_machine()
        self.assertIs(caught.exception, self.execute_failure)
        self.assertEqual(self.events, ["execute-undeclared", "execute-provider_omitted", "agent-close"])
        self.agent.close.assert_called_once()
        self.server.cleanup.assert_not_called()
        self.builder.verify.assert_not_called()
        self.replay.assert_not_called()
        self.assertFalse(self.server.cleanup_authorized)
        self.assertEqual(self.harness.driver_cleanup_verified, [True, False, False, False])
        self.assertEqual(self.harness.ssh_cache_requests, [])
        self.assertTrue((self.harness.evidence / "ssh-machine-0/host-agent-closure.json").is_file())
        self.assertFalse((self.harness.evidence / "ssh-machine-0/machine-ssh-validation.json").exists())

    def test_final_replay_failure_withholds_server_cleanup_and_cache_admission(self):
        self.replay_failure = ValueError("injected final replay drift")
        with self.assertRaises(ValueError) as caught:
            self.run_machine()
        self.assertIs(caught.exception, self.replay_failure)
        self.agent.close.assert_called_once()
        self.server.cleanup.assert_not_called()
        self.assertFalse(self.server.cleanup_authorized)
        self.assertEqual(self.harness.ssh_cache_requests, [])
        self.assertEqual(self.harness.driver_cleanup_verified, [True] + [False] * 5)

    def test_middle_case_and_close_failure_keep_original_as_cause_and_no_admission(self):
        self.execute_failure = ValueError("original solve failure")
        self.agent.close.side_effect = OSError("injected host closure failure")
        with self.assertRaisesRegex(RuntimeError, "closure also remains unproven") as caught:
            self.run_machine()
        self.assertIs(caught.exception.__cause__, self.execute_failure)
        self.server.cleanup.assert_not_called()
        self.assertEqual(self.harness.ssh_cache_requests, [])
        self.assertEqual(self.harness.driver_cleanup_verified, [True, False, False, False])


if __name__ == "__main__":
    unittest.main()
