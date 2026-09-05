"""Offline runner boundary checks; these tests never execute Docker or VMs."""

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "run-topology-up-machine-e2e.sh"
SOURCE = SCRIPT.read_text().split("<<'PYTHON_RUNNER'\n", 1)[1].rsplit("\nPYTHON_RUNNER", 1)[0]
runner = types.ModuleType("topology_up_runner")
exec(compile(SOURCE, str(SCRIPT), "exec"), runner.__dict__)


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.release = self.root / "release"
        self.release.mkdir()
        for name in ("vz", "vz-runtimed"):
            path = self.release / name
            path.write_bytes(b"test executable; never execute\n")
            path.chmod(0o700)
        self.docker = self.root / "docker-tools"
        self.docker.write_bytes(b"test multicall target; never execute\n")
        self.docker.chmod(0o700)
        self.invocation = self.root / "docker"
        self.invocation.symlink_to(self.docker)
        self.evidence = self.root / "new-evidence"
        self.args = argparse.Namespace(release_dir=str(self.release), docker=str(self.invocation),
                                       evidence_dir=str(self.evidence))

    def preflight(self):
        with patch.object(runner.platform, "system", return_value="Darwin"), \
                patch.object(runner.platform, "machine", return_value="arm64"), \
                patch.object(runner.shutil, "which", return_value="/toolchain/cargo"):
            return runner.preflight(self.args, SCRIPT)

    def test_help_and_shell_syntax_do_not_contact_docker(self):
        result = subprocess.run(["/bin/bash", str(SCRIPT), "--help"], capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"DEV physical", result.stdout)
        self.assertEqual(subprocess.run(["/bin/bash", "-n", str(SCRIPT)], capture_output=True).returncode, 0)

    def test_unknown_missing_abbreviated_and_repeated_options_rejected(self):
        for args in (["--unknown"], [], ["--release", "/tmp"],
                     ["--docker", "/one", "--docker", "/two"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises((ValueError, SystemExit)):
                    runner.arguments(args)
        self.assertFalse(self.evidence.exists())

    def test_docker_multicall_target_hashed_but_original_path_recorded(self):
        with patch.object(subprocess, "run") as command:
            info = self.preflight()
        command.assert_not_called()
        self.assertEqual(info["docker_invocation"], str(self.invocation))
        self.assertEqual(info["docker_canonical"], str(self.docker))
        self.assertEqual(info["docker_argv0"], "docker")
        self.assertEqual(info["docker_sha256"], runner.file_digest(self.docker))
        self.assertFalse(self.evidence.exists())

    def test_existing_evidence_is_never_adopted(self):
        self.evidence.mkdir()
        sentinel = self.evidence / "user-data"
        sentinel.write_text("preserve")
        with self.assertRaises(ValueError):
            self.preflight()
        self.assertEqual(sentinel.read_text(), "preserve")

    def test_release_symlink_and_relative_docker_rejected(self):
        binary = self.release / "vz"
        binary.unlink()
        binary.symlink_to(self.docker)
        with self.assertRaises(ValueError):
            self.preflight()
        binary.unlink()
        binary.write_bytes(b"fixture")
        binary.chmod(0o700)
        self.args.docker = "docker"
        with self.assertRaises(ValueError):
            self.preflight()

    def test_cargo_selects_only_exact_integration_test_artifact(self):
        source = Path("/repo/crates/vz-cli/tests/topology_up_machine_e2e.rs")
        artifact = {"reason": "compiler-artifact", "target": {"name": runner.TARGET, "kind": ["test"], "src_path": str(source)},
                    "profile": {"test": True}, "executable": str(self.docker)}
        messages = "\n".join(json.dumps(row) for row in [{"reason": "build-finished", "success": True}, artifact])
        self.assertEqual(runner.select_artifact(messages, source), self.docker)
        for changed in ({"profile": {"test": False}}, {"target": {"name": runner.TARGET, "kind": ["bin"], "src_path": str(source)}},
                        {"executable": None}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                runner.select_artifact(json.dumps(artifact | changed), source)
        with self.assertRaises(ValueError):
            runner.select_artifact(messages + "\n" + json.dumps(artifact | {"executable": str(self.release / "vz")}), source)

    def test_changed_inputs_are_rejected(self):
        inputs = {str(self.docker): runner.file_digest(self.docker)}
        runner.verify_inputs(inputs)
        self.docker.write_bytes(b"changed")
        with self.assertRaises(ValueError):
            runner.verify_inputs(inputs)

    def test_test_success_requires_exact_one_case_no_skips_and_clean_daemon(self):
        stdout = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5s\n"
        result = {"scope": runner.SCOPE, "error": None, "cleanup_errors": [], "daemon_exit": "exit status: 0",
                  "scenario": {"docker_parity_certified": False}}
        runner.validate_test_result(stdout, result)
        for bad in (stdout.replace("1 passed", "0 passed"), stdout.replace("0 ignored", "1 ignored"),
                    stdout.replace("0 filtered out", "1 filtered out"), stdout + stdout):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                runner.validate_test_result(bad, result)
        for changed in ({"daemon_exit": "shutdown-timeout"}, {"cleanup_errors": ["uncertain"]},
                        {"scenario": {"docker_parity_certified": True}}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                runner.validate_test_result(stdout, result | changed)

    def test_checksums_cover_nested_evidence_and_reject_symlinks(self):
        self.evidence.mkdir()
        nested = self.evidence / "physical"
        nested.mkdir()
        (nested / "result.json").write_text("{}\n")
        (self.evidence / "runner-result.json").write_text("{}\n")
        runner.checksums(self.evidence)
        checksums = (self.evidence / "checksums.sha256").read_text()
        self.assertIn("physical/result.json", checksums)
        self.assertIn("runner-result.json", checksums)
        (nested / "link").symlink_to(self.docker)
        with self.assertRaises(ValueError):
            runner.checksums(self.evidence)


if __name__ == "__main__":
    unittest.main()
