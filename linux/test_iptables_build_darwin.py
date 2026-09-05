"""Routing/negative tests; real compilation is a separate local-builder gate."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("iptables-build-darwin.sh")
BUILDER_ID = "sha256:" + "a" * 64
FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ["DOCKER_CALL_LOG"], "a") as stream:
    stream.write(json.dumps({"args": args, "overrides": {name: os.environ[name] for name in
        ["DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "BUILDX_BUILDER", "BUILDX_CONFIG"] if name in os.environ}}) + "\n")
assert args[:2] == ["--context", "selected-local"]
args = args[2:]
if args[:2] == ["context", "inspect"]:
    print(os.environ.get("FAKE_ENDPOINT", "unix:///private/tmp/builder.sock"))
elif args[:2] == ["image", "inspect"]:
    print("sha256:" + "a" * 64 + " linux " + os.environ.get("FAKE_ARCH", "arm64"))
elif args[0] != "run":
    raise SystemExit("unexpected Docker operation")
'''


class IptablesDarwinBuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="iptables build test ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        tools = self.root / "tools"
        tools.mkdir()
        docker = tools / "docker"
        docker.write_text(FAKE_DOCKER)
        docker.chmod(0o755)
        self.archive = self.root / "source.tar.xz"
        self.archive.write_bytes(b"pinned fixture source")
        self.log = self.root / "docker.jsonl"
        self.environment = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"],
                                IPTABLES_DOCKER_CONTEXT="selected-local", DOCKER_CALL_LOG=str(self.log))
        self.args = ["bash", str(SCRIPT), str(self.root), str(self.root / "out"), str(self.archive),
                     "1.8.13", hashlib.sha256(self.archive.read_bytes()).hexdigest(), "4"]

    def run_helper(self):
        return subprocess.run(self.args, env=self.environment, capture_output=True, text=True, check=False)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_case_sensitive_private_source_and_exact_local_builder(self):
        for name in ["DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "BUILDX_BUILDER", "BUILDX_CONFIG"]:
            self.environment[name] = "must-not-select-this"
        result = self.run_helper()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(not call["overrides"] for call in calls))
        command = calls[-1]["args"]
        self.assertIn(BUILDER_ID, command)
        self.assertIn(str(self.root) + ":/workspace:ro", command)
        self.assertIn(str(self.archive) + ":/vz-iptables-source.tar.xz:ro", command)
        self.assertIn(str(self.root / "out") + ":/vz-iptables-output", command)
        self.assertIn("IPTABLES_SRC_DIR=/tmp/vz-iptables-src", command)
        self.assertIn("IPTABLES_ARCHIVE=/vz-iptables-source.tar.xz", command)
        self.assertEqual(command[command.index("--network") + 1], "none")
        self.assertIn("flock", command)
        self.assertIn("KERNEL_PROFILE=developer", command)

    def test_bad_checksum_fails_before_docker(self):
        self.args[-2] = "0" * 64
        self.assertNotEqual(self.run_helper().returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_missing_context_does_not_use_global_default(self):
        self.environment.pop("IPTABLES_DOCKER_CONTEXT")
        self.assertNotEqual(self.run_helper().returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_remote_context_and_wrong_architecture_fail_before_build(self):
        self.environment["FAKE_ENDPOINT"] = "tcp://remote.example:2375"
        self.assertNotEqual(self.run_helper().returncode, 0)
        self.assertFalse(any("run" in call["args"] for call in self.calls()))
        self.environment.pop("FAKE_ENDPOINT")
        self.environment["FAKE_ARCH"] = "amd64"
        self.assertNotEqual(self.run_helper().returncode, 0)
        self.assertFalse(any("run" in call["args"] for call in self.calls()))

    def test_symlink_archive_fails_before_docker(self):
        alias = self.root / "alias.tar.xz"
        alias.symlink_to(self.archive)
        self.args[4] = str(alias)
        self.assertNotEqual(self.run_helper().returncode, 0)
        self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
