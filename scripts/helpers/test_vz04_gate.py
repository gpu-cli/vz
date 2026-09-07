import io
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_gate as gate  # noqa: E402
import vz04_validate as validate  # noqa: E402


class GateDryRunTests(unittest.TestCase):
    """End-to-end dry run against a fake release dir with the injected verifier."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix="vz04-gate-")
        cls.root = Path(cls.tmp.name).resolve()
        cls.release = fixtures.build_fake_release_dir(cls.root / "release", source_commit=common.git_head(common.REPO_ROOT))
        cls.evidence_root = cls.root / "evidence"
        cls.previous_verifier = gate.CODESIGN_VERIFIER
        gate.CODESIGN_VERIFIER = fixtures.fake_codesign_verifier
        cls.stdout = io.StringIO()
        with redirect_stdout(cls.stdout):
            cls.exit_code = gate.main(["--suite", "all", "--release-dir", str(cls.release), "--run-id", "gate-unit-dry-1",
                                       "--evidence-root", str(cls.evidence_root), "--state-root", str(cls.root / "state"), "--dry-lanes"])
        cls.run_root = cls.evidence_root / "gate-unit-dry-1"

    @classmethod
    def tearDownClass(cls):
        gate.CODESIGN_VERIFIER = cls.previous_verifier
        fixtures.make_writable(cls.release)
        cls.tmp.cleanup()

    def test_verdict_fail_with_85_missing(self):
        self.assertEqual(self.exit_code, 1)
        summary = common.load_json(self.run_root / "summary.json")
        self.assertEqual(summary["verdict"], "FAIL")
        self.assertEqual(summary["counts"]["required"], 85)
        self.assertEqual(summary["counts"]["MISSING"], 85)
        self.assertEqual(summary["counts"]["PASS"], 0)
        self.assertTrue(all(row["reason"] == "not_implemented" for row in summary["scenarios"]))
        self.assertEqual(summary["developer_overrides"], ["dry_lanes"])
        lane_rows = summary["lanes"]
        self.assertEqual(len(lane_rows), 1 + 3 * 4)
        self.assertTrue(all(row["failure_reason"] == "not_implemented" for row in lane_rows))
        codes = {row["code"] for row in summary["findings"]}
        for code in ("gate.developer_override", "sleep_wake.not_observed", "cleanup.leak_diff_not_performed", "inventory.not_captured",
                     "prerequisites.not_executed", "input.draft", "release.signing_class", "scenario.missing", "handoff.incomplete"):
            self.assertIn(code, codes)

    def test_summary_txt_lists_every_unmet_requirement(self):
        text = (self.run_root / "summary.txt").read_text()
        summary = common.load_json(self.run_root / "summary.json")
        self.assertTrue(text.startswith("verdict=FAIL\n"))
        for row in summary["scenarios"]:
            self.assertIn(f"MISSING {row['id']}", text)
        for row in summary["findings"]:
            self.assertIn(f"{row['code']} {row['subject']}", text)

    def test_manifest_index_and_checksums(self):
        manifest = common.load_json(self.run_root / "manifest.json")
        self.assertEqual(manifest["verdict"], "FAIL")
        self.assertEqual(manifest["prerequisites"]["status"], "not_executed")
        self.assertEqual([p["name"] for p in manifest["phases"]], ["clean-provision", "persisted-recovery", "final-cleanup"])
        self.assertEqual(common.verify_checksums(self.run_root), [])
        index = common.load_json(self.evidence_root / "index.json")
        self.assertEqual([(e["run_id"], e["verdict"], e["developer_overrides"]) for e in index["entries"]],
                         [("gate-unit-dry-1", "FAIL", ["dry_lanes"])])
        for lane in ("sandbox-vm", "topology", "linux-docker", "native-macos"):
            self.assertTrue((self.run_root / lane / "clean-provision" / "lane-result.json").is_file(), lane)
        self.assertTrue(any(p.name.startswith("state-handoff.") for p in (self.run_root / "phases" / "clean-provision").iterdir()))

    def test_validator_reproduces_verdict_and_findings(self):
        report = validate.validate_root(self.run_root, codesign_verifier=fixtures.fake_codesign_verifier)
        summary = common.load_json(self.run_root / "summary.json")
        self.assertEqual(report["verdict"], "FAIL")
        self.assertEqual(report["raw_findings"], summary["findings"])
        self.assertEqual(report["findings"], summary["findings"])
        self.assertEqual(report["scenarios"], summary["scenarios"])
        self.assertIn("validator_reproduced=True", self.stdout.getvalue())

    def test_repeat_run_id_is_refused_before_touching_state(self):
        with redirect_stdout(io.StringIO()):
            code = gate.main(["--suite", "all", "--release-dir", str(self.release), "--run-id", "gate-unit-dry-1",
                              "--evidence-root", str(self.evidence_root), "--dry-lanes"])
        self.assertEqual(code, 2)
        self.assertEqual(len(common.load_json(self.evidence_root / "index.json")["entries"]), 1)

    def test_other_suites_rejected_with_exit_2(self):
        with self.assertRaises(SystemExit) as caught:
            gate.parse_args(["--suite", "lifecycle", "--release-dir", "/x", "--run-id", "gate-unit-dry-2"])
        self.assertEqual(caught.exception.code, 2)

    def test_dry_lanes_refused_for_notarized_release(self):
        notarized = fixtures.build_fake_release_dir(self.root / "notarized", signing_class="developer-id-notarized",
                                                    source_commit=common.git_head(common.REPO_ROOT))
        try:
            with redirect_stdout(io.StringIO()):
                code = gate.main(["--suite", "all", "--release-dir", str(notarized), "--run-id", "gate-unit-dry-3",
                                  "--evidence-root", str(self.root / "evidence-notarized"), "--dry-lanes"])
            self.assertEqual(code, 2)
            self.assertFalse((self.root / "evidence-notarized" / "gate-unit-dry-3").exists())
        finally:
            fixtures.make_writable(notarized)

    def test_clients_required_without_dry_lanes(self):
        with redirect_stdout(io.StringIO()):
            code = gate.main(["--suite", "all", "--release-dir", str(self.release), "--run-id", "gate-unit-dry-4",
                              "--evidence-root", str(self.root / "evidence-clients")])
        self.assertEqual(code, 2)

    def test_tampered_evidence_is_detected(self):
        with tempfile.TemporaryDirectory(prefix="vz04-tamper-") as copy_root:
            import shutil
            copy = Path(copy_root).resolve() / "gate-unit-dry-1"
            shutil.copytree(self.run_root, copy)
            result = copy / "topology" / "clean-provision" / "lane-result.json"
            value = common.load_json(result)
            value["failure"]["detail"] = "edited after the fact"
            common.document(result, value, replace=True)
            report = validate.validate_root(copy, codesign_verifier=fixtures.fake_codesign_verifier)
            self.assertEqual(report["verdict"], "FAIL")
            self.assertTrue(any(r["code"] == "checksums" and "topology/clean-provision/lane-result.json" in r["detail"] for r in report["findings"]))
            self.assertTrue(any(r["code"] == "index.evidence_dir" for r in report["findings"]))
            (copy / "topology" / "clean-provision" / "lane.stdout").write_bytes(b"vz04-canary-secret\n")
            report = validate.validate_root(copy, codesign_verifier=fixtures.fake_codesign_verifier)
            self.assertTrue(any(r["code"] == "canary.present" for r in report["findings"]))


if __name__ == "__main__":
    unittest.main()
