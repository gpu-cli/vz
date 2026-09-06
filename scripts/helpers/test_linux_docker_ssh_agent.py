"""Offline adversaries and actual local disposable agents; no network or VM."""
import copy
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_ssh_agent as agent


class AgentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pins = agent.tool_inputs()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vzsa-", dir="/private/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.owner = {"project_id": "prj_test", "environment_id": "env_test", "machine_id": "mch_test"}
        self.instances = []
        self.addCleanup(self.close_instances)

    def close_instances(self):
        for instance in self.instances:
            if instance._closed:
                continue
            try:
                instance.close()
            except agent.AgentError:
                # Negative path tests may intentionally preserve replaced files.
                # A real host process must nevertheless be conclusively reaped.
                self.assertTrue(instance._agent is None or instance._agent.returncode is not None)

    def make(self):
        instance = agent.Agent(self.root / "keys", self.root / "evidence", tools=self.pins,
                               run_id="unit-agent", owner=self.owner)
        self.instances.append(instance)
        return instance

    def evidence(self):
        return b"".join(path.read_bytes() for path in (self.root / "evidence").iterdir() if path.is_file())

    def test_actual_disposable_agent_identity_signature_and_normal_cleanup(self):
        inherited = {"SSH_AUTH_SOCK": str(self.root / "foreign-agent.sock"), "SSH_AGENT_PID": "1",
                     "SSH_ASKPASS": str(self.root / "must-not-run"), "DISPLAY": "must-not-open",
                     "SSH_SK_PROVIDER": "must-not-load", "APPLE_SSH_ADD_BEHAVIOR": "macos"}
        with patch.dict(os.environ, inherited):
            before = dict(os.environ)
            instance = self.make()
            proof = instance.start()
            self.assertEqual(os.environ, before)
            self.assertEqual(len(set(proof["fingerprints"].values())), 3)
            self.assertTrue(proof["sole_auth_identity_verified"])
            self.assertEqual(instance._agent.pid, proof["pid"])
            self.assertIsNone(instance._agent.poll())
            self.assertEqual(os.getpgid(proof["pid"]), proof["pid"])
            self.assertEqual(os.getsid(proof["pid"]), proof["pid"])
            self.assertEqual(stat.S_IMODE((self.root / "keys").stat().st_mode), 0o700)
            self.assertTrue(instance.paths["host_private_key"].is_file())
            secrets = instance.canaries()
            self.assertGreaterEqual(len(secrets), 6)
            self.assertEqual(instance.verify()["fingerprints"], proof["fingerprints"])
            closure = instance.close()
            self.assertTrue(closure["agent_reaped"] and closure["sigterm_dispatched"] and closure["private_inputs_removed"])
            self.assertEqual(closure["cleanup_errors"], [])
            self.assertFalse(closure["broader_cleanup_authorized"] or closure["secure_erasure_claimed"])
            self.assertEqual(instance.close(), closure)
            self.assertFalse((self.root / "keys").exists())
            terminal = json.loads((self.root / "evidence/agent.result.json").read_bytes())
            self.assertEqual(terminal["exit_code"], 2)
            self.assertFalse(terminal["effects_uncertain"])
            self.assertEqual((self.root / "evidence/agent.stderr").read_bytes(), b"exiting on signal 15\r\n")
            public = self.evidence()
            self.assertNotIn(b"PRIVATE KEY-----", public)
            for secret in secrets:
                self.assertNotIn(secret, public)
            for path in (self.root / "evidence").glob("*.intent.json"):
                intent = json.loads(path.read_bytes())
                env = intent["environment"]
                self.assertEqual(env["SSH_ASKPASS_REQUIRE"], "never")
                self.assertEqual(env["APPLE_SSH_ADD_BEHAVIOR"], "openssh")
                for key in ("HOME", "SSH_ASKPASS", "DISPLAY", "SSH_AGENT_PID", "SSH_SK_PROVIDER"):
                    self.assertNotIn(key, env)
                if "SSH_AUTH_SOCK" in env:
                    self.assertEqual(env["SSH_AUTH_SOCK"], str(self.root / "keys/agent.sock"))
                self.assertFalse(set(intent["argv"]) & {"-A", "-K", "--apple-use-keychain", "--apple-load-keychain"})

    def test_inputs_fail_before_creating_private_material(self):
        for mutation in ("hash", "tool-path", "extra-tool", "owner", "run", "nested", "long-socket"):
            pins, owner, run = copy.deepcopy(self.pins), dict(self.owner), "unit-agent"
            private, evidence = self.root / "keys", self.root / "evidence"
            if mutation == "hash": pins["ssh-agent"]["sha256"] = "f" * 64
            elif mutation == "tool-path": pins["ssh-agent"]["path"] = "/bin/sh"
            elif mutation == "extra-tool": pins["ssh"] = pins["ssh-add"]
            elif mutation == "owner": owner["foreign"] = "x"
            elif mutation == "run": run = "../../bad"
            elif mutation == "nested": evidence = private / "evidence"
            else: private = self.root / ("x" * 100)
            with self.subTest(mutation=mutation), self.assertRaises(agent.AgentError):
                agent.Agent(private, evidence, tools=pins, run_id=run, owner=owner)
            self.assertFalse((self.root / "keys").exists())

    def test_existing_and_symlink_roots_are_never_reused(self):
        private = self.root / "keys"
        private.mkdir(mode=0o700)
        marker = private / "foreign"
        marker.write_bytes(b"untouched")
        with self.assertRaises(FileExistsError): self.make()
        self.assertEqual(marker.read_bytes(), b"untouched")
        private.rename(self.root / "original")
        private.symlink_to(self.root / "original", target_is_directory=True)
        with self.assertRaises(agent.AgentError): self.make()
        self.assertEqual((self.root / "original/foreign").read_bytes(), b"untouched")

    def test_partial_generation_failure_cleans_only_authorized_disposable_files(self):
        instance = self.make()
        real = instance._command
        original = RuntimeError("preserve original generation failure")
        def command(label, *args, **kwargs):
            if label == "generate-host":
                partial = self.root / "keys/host"
                partial.write_bytes(b"partial disposable secret")
                partial.chmod(0o600)
                raise original
            return real(label, *args, **kwargs)
        with patch.object(instance, "_command", side_effect=command):
            with self.assertRaises(RuntimeError) as caught: instance.start()
        self.assertIs(caught.exception, original)
        self.assertTrue(original.ssh_agent_cleanup["private_inputs_removed"])
        self.assertIn("host", original.ssh_agent_cleanup["removed_names"])
        self.assertNotIn(b"partial disposable secret", self.evidence())

    def test_unexpected_generation_output_is_withheld_before_canaries_exist(self):
        instance = self.make()
        unknown = b"not-yet-registered-private-data"
        with patch.object(agent, "collect_output", return_value=(unknown, b"")):
            with self.assertRaises(agent.AgentError): instance.start()
        self.assertNotIn(unknown, self.evidence())
        self.assertIn(b"unexpected output withheld", self.evidence())
        self.assertTrue(instance._closure["private_inputs_removed"])

    def test_foreign_key_target_is_not_adopted_during_failure_cleanup(self):
        instance = self.make()
        foreign = self.root / "keys/host"
        foreign.write_bytes(b"foreign private file")
        foreign.chmod(0o600)
        with self.assertRaises(agent.AgentError): instance.start()
        self.assertEqual(foreign.read_bytes(), b"foreign private file")
        self.assertFalse(instance._closure["private_inputs_removed"])
        self.assertIn("private_cleanup:AgentError", instance._closure["cleanup_errors"])

    def test_actual_key_replacement_is_rejected_and_not_deleted(self):
        instance = self.make()
        instance.start()
        path = self.root / "keys/host"
        path.rename(self.root / "old-host")
        path.write_bytes(b"foreign replacement")
        path.chmod(0o600)
        with self.assertRaises(agent.AgentError): instance.verify()
        with self.assertRaises(agent.AgentError): instance.close()
        self.assertIsNotNone(instance._agent.returncode)
        self.assertEqual(path.read_bytes(), b"foreign replacement")

    def test_actual_root_replacement_stops_agent_but_preserves_foreign_directory(self):
        instance = self.make()
        instance.start()
        (self.root / "keys").rename(self.root / "retained-keys")
        (self.root / "keys").mkdir(mode=0o700)
        marker = self.root / "keys/foreign"
        marker.write_bytes(b"untouched")
        with self.assertRaises(agent.AgentError): instance.close()
        self.assertIsNotNone(instance._agent.returncode)
        self.assertEqual(marker.read_bytes(), b"untouched")

    def test_actual_dead_agent_is_not_a_normal_stop_certificate(self):
        instance = self.make()
        instance.start()
        os.kill(instance._agent.pid, signal.SIGTERM)
        instance._agent.wait(timeout=5)
        with self.assertRaises(agent.AgentError): instance.verify()
        with self.assertRaises(agent.AgentError): instance.close()
        self.assertTrue(instance._closure["agent_reaped"])
        self.assertFalse(instance._closure["sigterm_dispatched"])

    def test_process_identity_change_withholds_signals(self):
        instance = self.make()
        instance.start()
        pid = instance._agent_pid
        instance._agent_pid = pid + 1
        with patch.object(agent.os, "killpg") as kill:
            with self.assertRaises(agent.AgentError): instance._check_agent()
            with self.assertRaises(agent.AgentError): instance._terminate(instance._agent, instance._agent_pid)
            kill.assert_not_called()
        instance._agent_pid = pid
        instance.close()

    def test_unexpected_extra_agent_identity_is_rejected(self):
        instance = self.make()
        instance.start()
        env = dict(agent.ENVIRONMENT, SSH_AUTH_SOCK=str(instance.paths["socket"]))
        result = subprocess.run([agent.TOOLS["ssh-add"], "-q", "-k", str(self.root / "keys/host")],
                                env=env, stdin=subprocess.DEVNULL, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0)
        with self.assertRaises(agent.AgentError): instance.verify()
        self.assertTrue(instance.close()["agent_reaped"])

    def test_constructor_evidence_failure_creates_no_keys_and_preserves_original(self):
        original = OSError("injected evidence write failure")
        with patch.object(agent.Agent, "_document", side_effect=original):
            with self.assertRaises(OSError) as caught: self.make()
        self.assertIs(caught.exception, original)
        self.assertFalse((self.root / "keys").exists())
        self.assertEqual(list((self.root / "evidence").iterdir()), [])

    def test_normal_stop_rejects_wrong_status_signal_stream_or_announcement(self):
        for change in ("exit-zero", "wrong-signal", "extra-error", "missing-announcement", "capture-error"):
            with self.subTest(change=change):
                case = self.root / change
                case.mkdir(mode=0o700)
                instance = agent.Agent(case / "keys", case / "evidence", tools=self.pins,
                                       run_id="unit-agent", owner=self.owner)
                self.instances.append(instance)
                instance.start()
                real = instance._terminate
                def terminate(process, pid):
                    signaled = real(process, pid)
                    if process is instance._agent:
                        instance._capture_thread.join(timeout=5)
                        self.assertFalse(instance._capture_thread.is_alive())
                        stdout, stderr, error = instance._agent_capture
                        if change == "exit-zero": process.returncode = 0
                        elif change == "wrong-signal": stderr = b"exiting on signal 9\r\n"
                        elif change == "extra-error": stderr += b"unexpected error\n"
                        elif change == "missing-announcement": stdout = b""
                        else: error = "TimeoutExpired"
                        instance._agent_capture = stdout, stderr, error
                    return signaled
                with patch.object(instance, "_terminate", side_effect=terminate):
                    with self.assertRaises(agent.AgentError): instance.close()
                self.assertTrue(instance._closure["agent_reaped"])
                self.assertTrue(instance._closure["private_inputs_removed"])
                self.assertTrue(instance._closure["cleanup_errors"])
                terminal = json.loads((case / "evidence/agent.result.json").read_bytes())
                self.assertTrue(terminal["effects_uncertain"])

    def test_signal_denial_retains_exact_process_for_scoped_retry(self):
        instance = self.make()
        instance.start()
        pid = instance._agent.pid
        with patch.object(agent.os, "killpg", side_effect=PermissionError("injected signal denial")):
            with self.assertRaises(agent.AgentError): instance.close()
        self.assertFalse(instance._closed)
        self.assertFalse(instance._closure["agent_reaped"])
        self.assertFalse(instance._closure["private_inputs_removed"])
        self.assertIsNone(instance._agent.poll())
        self.assertEqual(instance._agent.pid, pid)
        self.assertTrue(instance.close()["private_inputs_removed"])


if __name__ == "__main__":
    unittest.main()
