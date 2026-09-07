from pathlib import Path
import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr

sys.path.insert(0, str(Path(__file__).resolve().parent))

import sandbox_lane_result as lane_result  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_schema as schema  # noqa: E402

HELPER = Path(lane_result.__file__).resolve()
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
YOUKI_INVENTORY = {
    "buildkitd_executable": "/mnt/buildkit-bin/buildkitd", "buildkitd_oci_worker_binary": "/tmp/vz-buildkit-oci-runtime",
    "cgroup_filesystem": "cgroup2", "forbidden_runtime_paths": [], "observed_oci_subcommands": ["create", "start"],
    "observed_runtime_paths": ["/mnt/linux-bin/youki"], "oci_runtime_elf_paths": ["/mnt/linux-bin/youki"],
    "oci_worker_binary": "/tmp/vz-buildkit-oci-runtime", "runtime_binary": "/mnt/linux-bin/youki",
    "runtime_version": "youki version 0.5.7", "shim_target": "/usr/bin/vz-guest-agent"}
PASS_SUMMARY = "passed=runtime runtime-generation-crash-reopen runtime-generation-state-store-v7 runtimed machine-registry stack buildkit\nfailed=none\n"


def _write(path: Path, text: str) -> None:
    path.write_text(text)


class SandboxLaneResultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="sandbox-lane-result-")
        self.root = Path(self.tmp.name).resolve()
        self.run_dir = self.root / "20260906T101010Z"
        self.run_dir.mkdir()
        _write(self.run_dir / "run-info.txt", "timestamp_utc=20260906T101010Z\nhost=mac\nprofile=release\nsuites=runtime stack\n")
        _write(self.run_dir / "process-starts.tsv",
               "runtime\t4242\t/t/runtime_e2e-abc\nruntime-generation-state-store-v7\t4300\t/t/vz_stack-def\nbuildkit\tnone\t/t/buildkit_e2e\n")

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, *extra, stage="summary", exit_code=0, run_dir=True, expect=0):
        argv = ["--repo-root", str(common.REPO_ROOT), "--exit-code", str(exit_code), "--stage", stage]
        if run_dir:
            argv += ["--run-dir", str(self.run_dir)]
        argv += list(extra) + ["--", "--suite", "all", "--profile", "release"]
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = lane_result.main(argv)
        self.assertEqual(code, expect, err.getvalue())
        if expect != 0:
            return None
        result = common.load_json(Path(out.getvalue().strip()))
        self.assertEqual(schema.validate("lane-result", result), [], json.dumps(result, indent=1))
        return result

    def _write_summary(self, text, inventory=YOUKI_INVENTORY):
        inventory_path = self.run_dir / "buildkit-runtime-inventory.txt"
        if inventory is not None:
            _write(inventory_path, json.dumps(inventory))
            text += f"buildkit_runtime_inventory={inventory_path}\n"
        else:
            text += "buildkit_runtime_inventory=none\n"
        _write(self.run_dir / "summary.txt", text)

    def test_passed_standalone_run(self):
        self._write_summary(PASS_SUMMARY)
        result = self._run()
        self.assertEqual((result["outcome"], result["failure"], result["lane"], result["phase"]), ("passed", None, "sandbox-vm", "clean-provision"))
        self.assertEqual(result["run_id"], "sandbox-20260906t101010z")
        self.assertEqual(result["candidate_tuple_sha256"], lane_result.ABSENT_DIGEST)
        self.assertEqual(result["release_dir_sha256"], lane_result.ABSENT_DIGEST)
        self.assertEqual(result["fixture_sha256"], lane_result.ABSENT_DIGEST)
        self.assertEqual(result["contract_sha256"], common.digest_file(common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"]))
        self.assertEqual(result["entry_point"], {"path": "scripts/run-sandbox-vm-e2e.sh",
                                                 "sha256": common.digest_file(common.REPO_ROOT / "scripts/run-sandbox-vm-e2e.sh"),
                                                 "argv": ["--suite", "all", "--profile", "release"]})
        self.assertEqual(result["scenarios"], [])
        self.assertEqual(result["test_case_retries"], 0)
        self.assertEqual(result["process_starts"], [
            {"scenario_id": "gate.sandbox_vm.runtime", "argv0": "/t/runtime_e2e-abc", "pid": 4242},
            {"scenario_id": "gate.sandbox_vm.runtime_generation_state_store_v", "argv0": "/t/vz_stack-def", "pid": 4300},
            {"scenario_id": "gate.sandbox_vm.buildkit", "argv0": "/t/buildkit_e2e", "pid": None}])
        self.assertEqual(result["prohibited_observed"], {key: False for key in lane_result.PROHIBITED_KEYS})
        self.assertEqual(result["handoff"], {"produced": None, "consumed": None, "consumed_sha256": None})
        self.assertEqual(result["retained_root"], str(self.run_dir))
        self.assertEqual(result["evidence_files"], ["buildkit-runtime-inventory.txt", "process-starts.tsv", "run-info.txt", "summary.txt"])
        self.assertIsNone(result["result_adapter"])
        self.assertTrue((self.run_dir / "lane-result.json").is_file())
        # A second write never overwrites the document.
        self._run(expect=2)

    def test_failed_summary_is_assertion(self):
        self._write_summary("passed=runtime\nfailed=stack:1 exec-supervision-evidence:101\n", inventory=None)
        result = self._run(exit_code=1)
        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(result["failure"], {"reason": "assertion", "detail": "failed=stack:1 exec-supervision-evidence:101", "exit_code": 1})

    def test_summary_without_passes_is_crash(self):
        self._write_summary("passed=none\nfailed=none\n", inventory=None)
        result = self._run(exit_code=0)
        self.assertEqual(result["failure"]["reason"], "crash")

    def test_interrupted_run_is_crash(self):
        result = self._run("--interrupted", "SIGINT", stage="suites", exit_code=130)
        self.assertEqual(result["failure"]["reason"], "crash")
        self.assertEqual(result["failure"]["exit_code"], 130)
        self.assertIn("SIGINT", result["failure"]["detail"])
        self.assertEqual(len(result["process_starts"]), 3)
        self.assertNotIn("summary.txt", result["evidence_files"])

    def test_stage_mapping_without_summary(self):
        for stage, reason in (("preflight", "prerequisite"), ("provision", "prerequisite"), ("suites", "crash"),
                              ("arguments", "input_rejected"), ("lane-arguments", "input_rejected"), ("summary", "crash")):
            (self.run_dir / "lane-result.json").unlink(missing_ok=True)
            result = self._run(stage=stage, exit_code=1)
            self.assertEqual(result["failure"]["reason"], reason, stage)

    def test_runc_in_inventory_is_prohibited(self):
        inventory = dict(YOUKI_INVENTORY, forbidden_runtime_paths=["/usr/bin/runc"])
        self._write_summary(PASS_SUMMARY, inventory=inventory)
        result = self._run(exit_code=0)
        self.assertTrue(result["prohibited_observed"]["runc"])
        self.assertFalse(result["prohibited_observed"]["crun"])

    def _release_dir(self):
        release = self.root / "release"
        release.mkdir()
        _write(release / "release-manifest.json", '{"version": "0.4.0"}\n')
        _write(release / "vz", "binary\n")
        return release

    def test_gate_arguments_and_distinct_evidence_dir(self):
        self._write_summary(PASS_SUMMARY)
        release = self._release_dir()
        evidence = self.root / "evidence" / "sandbox-vm" / "clean-provision"
        contract = common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"]
        result = self._run("--evidence-dir", str(evidence), "--run-id", "gate-run-0001", "--phase", "clean-provision",
                           "--release-dir", str(release), "--state-root", str(self.root / "state"), "--contract", str(contract),
                           "--candidate-tuple", DIGEST_A, "--fixture-sha256", DIGEST_B, "--handoff", "none",
                           "--docker", "none", "--compose-plugin", "none", "--buildx-plugin", "none")
        self.assertEqual(result["outcome"], "passed")
        self.assertEqual((result["run_id"], result["candidate_tuple_sha256"], result["fixture_sha256"]), ("gate-run-0001", DIGEST_A, DIGEST_B))
        self.assertEqual(result["release_dir_sha256"], common.tree_digest(release))
        self.assertEqual(result["contract_sha256"], common.digest_file(contract))
        self.assertEqual(result["evidence_files"], ["sandbox-summary.txt", "sandbox-run-info.txt", "sandbox-process-starts.tsv",
                                                    "sandbox-buildkit-runtime-inventory.txt"])
        for name in result["evidence_files"]:
            self.assertTrue((evidence / name).is_file(), name)
        self.assertEqual(common.load_json(evidence / "lane-result.json"), common.load_json(self.run_dir / "lane-result.json"))

    def test_release_dir_without_manifest_is_input_rejected(self):
        self._write_summary(PASS_SUMMARY)
        release = self.root / "release"
        release.mkdir()
        result = self._run("--release-dir", str(release), "--run-id", "gate-run-0001", "--candidate-tuple", DIGEST_A,
                           "--fixture-sha256", DIGEST_B)
        self.assertEqual(result["failure"]["reason"], "input_rejected")
        self.assertIn("release-manifest.json", result["failure"]["detail"])
        self.assertEqual(result["release_dir_sha256"], lane_result.ABSENT_DIGEST)

    def test_wrong_phase_or_handoff_is_input_rejected(self):
        self._write_summary(PASS_SUMMARY)
        result = self._run("--phase", "final-cleanup", "--handoff", "/x/state-handoff.json", "--run-id", "gate-run-0001")
        self.assertEqual(result["failure"]["reason"], "input_rejected")
        self.assertEqual(result["phase"], "final-cleanup")
        self.assertIn("clean-provision", result["failure"]["detail"])
        self.assertIn("handoff", result["failure"]["detail"])

    def test_evidence_dir_without_run_dir(self):
        evidence = self.root / "evidence"
        result = self._run("--evidence-dir", str(evidence), "--run-id", "gate-run-0001", stage="lane-arguments", exit_code=1, run_dir=False)
        self.assertEqual(result["failure"]["reason"], "input_rejected")
        self.assertIsNone(result["retained_root"])
        self.assertEqual(result["evidence_files"], [])
        self.assertEqual(result["process_starts"], [])

    def test_system_python_stdlib_only(self):
        """The harness runs the helper with /usr/bin/python3, which has no jsonschema."""
        if not Path("/usr/bin/python3").exists():
            self.skipTest("no /usr/bin/python3")
        self._write_summary(PASS_SUMMARY)
        completed = subprocess.run(["/usr/bin/python3", "-B", "-S", str(HELPER), "--repo-root", str(common.REPO_ROOT), "--exit-code", "0",
                                    "--stage", "summary", "--run-dir", str(self.run_dir), "--", "--suite", "all"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=120)
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        result = common.load_json(Path(completed.stdout.decode().strip()))
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["outcome"], "passed")


if __name__ == "__main__":
    unittest.main()
