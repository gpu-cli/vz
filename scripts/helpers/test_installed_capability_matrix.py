"""Adversarial tests for the capability-matrix validator; synthetic inputs are never evidence.

python3 -B -m unittest scripts/helpers/test_installed_capability_matrix.py
"""
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import installed_capability_matrix as matrix


def first_live_entry(document):
    for pair in document["pairs"]:
        if pair["pair_status"] == "DEV":
            return pair, pair["machine_capabilities"]["posix_exec"]
    raise AssertionError("checked-in matrix must contain a DEV pair")


def pair_with_status(document, status):
    for pair in document["pairs"]:
        if pair["pair_status"] == status:
            return pair
    raise AssertionError(f"checked-in matrix must contain a {status} pair")


class InstalledCapabilityMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = matrix.load_json(matrix.ROOT, matrix.MATRIX)
        cls.schema = matrix.load_json(matrix.ROOT, matrix.SCHEMA)

    def changed(self):
        return copy.deepcopy(self.document)

    def assertRejects(self, document, fragment):
        violations = matrix.validate_matrix(document)
        self.assertTrue(violations, "expected a violation")
        self.assertTrue(any(fragment in violation for violation in violations),
                        f"no violation mentions {fragment!r}: {violations}")

    def test_checked_in_matrix_is_valid(self):
        self.assertEqual(matrix.validate_matrix(self.document), [])

    def test_schema_enum_matches_validator_vocabulary(self):
        self.assertEqual(self.schema["$defs"]["machine_capability"]["enum"], list(matrix.MACHINE_CAPABILITIES))
        self.assertEqual(self.schema["$defs"]["backend"]["enum"], list(matrix.BACKENDS))
        self.assertEqual(self.schema["$defs"]["profile"]["enum"], list(matrix.PROFILES))
        self.assertEqual(self.schema["$defs"]["status"]["enum"], list(matrix.STATUSES))

    def test_alias_tables_cover_wire_names(self):
        self.assertEqual(self.document["vocabularies"]["profiles"]["aliases"], {"container": "hardened"})
        self.assertEqual(self.document["vocabularies"]["backends"]["aliases"]["macos-vz"], "macos_virtualization_linux")
        changed = self.changed()
        changed["vocabularies"]["profiles"]["wire_names"] = ["developer"]
        self.assertRejects(changed, "wire_names must equal")
        changed = self.changed()
        changed["vocabularies"]["backends"]["aliases"]["macos-vz"] = "macos-vz"
        self.assertRejects(changed, "unknown wire name")
        changed = self.changed()
        changed["vocabularies"]["profiles"]["aliases"]["hardened"] = "hardened"
        self.assertRejects(changed, "shadows a wire name")

    def test_machine_capability_vocabulary_is_exact(self):
        changed = self.changed()
        changed["vocabularies"]["machine_capabilities"].append("teleport")
        self.assertRejects(changed, "13 MachineCapability wire names")
        changed = self.changed()
        changed["vocabularies"]["machine_capabilities"].reverse()
        self.assertRejects(changed, "declaration order")

    def test_every_pair_lists_every_machine_capability(self):
        pair, _ = first_live_entry(self.changed())
        changed = self.changed()
        target = changed["pairs"][self.document["pairs"].index(pair)]
        del target["machine_capabilities"]["snapshot"]
        self.assertRejects(changed, "missing ['snapshot']")
        changed = self.changed()
        target = changed["pairs"][self.document["pairs"].index(pair)]
        target["machine_capabilities"]["teleport"] = copy.deepcopy(target["machine_capabilities"]["snapshot"])
        self.assertRejects(changed, "unknown ['teleport']")
        changed = self.changed()
        target = changed["pairs"][self.document["pairs"].index(pair)]
        del target["topology_capabilities"]["receipts"]
        self.assertRejects(changed, "missing ['receipts']")

    def test_duplicate_json_keys_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            (root / matrix.MATRIX).write_text('{"schema_version": 1, "schema_version": 2}')
            with self.assertRaises(matrix.InvalidMatrix):
                matrix.load_json(root, matrix.MATRIX)

    def test_live_status_requires_evidence_and_negotiation(self):
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["evidence"] = []
        self.assertRejects(changed, "DEV requires non-empty evidence")
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["negotiated_by"] = []
        self.assertRejects(changed, "DEV requires non-empty negotiated_by")

    def test_active_is_rejected_before_a_release(self):
        changed = self.changed()
        pair, entry = first_live_entry(changed)
        entry["status"] = "ACTIVE"
        pair["pair_status"] = "ACTIVE"
        self.assertRejects(changed, "ACTIVE requires a published 0.4 release")

    def test_na_requires_rejection_and_planned_requires_emptiness(self):
        changed = self.changed()
        pair = pair_with_status(changed, "NA")
        pair["machine_capabilities"]["gui"]["rejected_by"] = []
        self.assertRejects(changed, "NA requires non-empty rejected_by")
        changed = self.changed()
        pair = pair_with_status(changed, "NA")
        pair["rejected_by"] = []
        self.assertRejects(changed, "NA pair requires rejected_by")
        changed = self.changed()
        pair = pair_with_status(changed, "PLANNED")
        pair["machine_capabilities"]["posix_exec"]["negotiated_by"] = ["crates/x/src/lib.rs:1"]
        self.assertRejects(changed, "PLANNED requires empty")
        changed = self.changed()
        pair = pair_with_status(changed, "PLANNED")
        pair["machine_capabilities"]["posix_exec"]["status"] = "NA"
        pair["machine_capabilities"]["posix_exec"]["rejected_by"] = ["crates/x/src/lib.rs:1"]
        self.assertRejects(changed, "PLANNED pair must label posix_exec PLANNED")

    def test_na_pair_labels_everything_na(self):
        changed = self.changed()
        pair = pair_with_status(changed, "NA")
        pair["machine_capabilities"]["gui"]["status"] = "PLANNED"
        pair["machine_capabilities"]["gui"]["rejected_by"] = []
        self.assertRejects(changed, "NA pair must label gui NA")

    def test_source_refs_and_evidence_shapes(self):
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["negotiated_by"] = ["crates/vz-runtimed/src/environment_up/readiness.rs"]
        self.assertRejects(changed, "does not match")
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["negotiated_by"] = ["crates/vz-runtimed/src/environment_up/readiness.rs:20-10"]
        self.assertRejects(changed, "inverted line range")
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["evidence"][0]["result_sha256"] = "ABC"
        self.assertRejects(changed, "result_sha256 must be 64 lowercase hex")
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["evidence"][0]["release_certified"] = True
        self.assertRejects(changed, "release_certified must be false")
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["evidence"][0]["outcome"] = "failed"
        self.assertRejects(changed, "outcome must start with 'passed'")
        changed = self.changed()
        _, entry = first_live_entry(changed)
        entry["evidence"][0]["surprise"] = 1
        self.assertRejects(changed, "unknown keys ['surprise']")

    def test_header_constants(self):
        for key, value, fragment in (
            ("unlisted_capability_status", "ACTIVE", "unlisted_capability_status"),
            ("target_release", "0.5.0", "target_release"),
            ("contract_state", "certified", "contract_state"),
            ("source_head", "not-a-sha", "source_head"),
        ):
            changed = self.changed()
            changed[key] = value
            self.assertRejects(changed, fragment)
        changed = self.changed()
        changed["hosts"]["macos-x86_64"]["rejected_by"] = []
        self.assertRejects(changed, "NA host requires rejected_by")
        changed = self.changed()
        changed["hosts"]["macos-arm64"]["arch"] = "x86_64"
        self.assertRejects(changed, "arch must match the host id suffix")

    def test_duplicate_pairs_are_rejected(self):
        changed = self.changed()
        changed["pairs"].append(copy.deepcopy(changed["pairs"][0]))
        self.assertRejects(changed, "duplicate pair")

    def test_evidence_digests_verified_or_unverifiable_never_fabricated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_dir = root / ".artifacts" / "synthetic-lane-1"
            evidence_dir.mkdir(parents=True)
            result = evidence_dir / "result.json"
            result.write_bytes(b'{"outcome": "synthetic, not evidence"}\n')
            checksums = evidence_dir / "checksums.sha256"
            checksums.write_bytes(b"deadbeef  result.json\n")
            document = {"pairs": [{"host": "macos-arm64", "target": "linux", "profile": "developer",
                                   "machine_capabilities": {"posix_exec": {"evidence": [{
                                       "result_path": ".artifacts/synthetic-lane-1/result.json",
                                       "result_sha256": hashlib.sha256(result.read_bytes()).hexdigest(),
                                       "checksums_sha256": hashlib.sha256(checksums.read_bytes()).hexdigest(),
                                   }, {
                                       "result_path": ".artifacts/absent-lane/result.json",
                                       "result_sha256": "0" * 64,
                                       "checksums_sha256": None,
                                   }]}},
                                   "topology_capabilities": {}}]}
            verified, unverifiable, mismatched = matrix.verify_evidence_digests(document, root)
            self.assertEqual(len(verified), 2)
            self.assertEqual(len(unverifiable), 1)
            self.assertIn("unverifiable locally", unverifiable[0])
            self.assertEqual(mismatched, [])
            checksums.write_bytes(b"tampered\n")
            _, _, mismatched = matrix.verify_evidence_digests(document, root)
            self.assertEqual(len(mismatched), 1)

    def test_run_reports_and_exits(self):
        lines = []
        self.assertEqual(matrix.run(matrix.ROOT, matrix.MATRIX, lines.append), 0)
        self.assertTrue(lines[-1].endswith("0 mismatched"))
        self.assertIn("0 rule violations", lines[-1])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            broken = self.changed()
            broken["unlisted_capability_status"] = "ACTIVE"
            (root / matrix.MATRIX).write_text(json.dumps(broken))
            lines = []
            self.assertEqual(matrix.run(root, matrix.MATRIX, lines.append), 1)
            self.assertTrue(any(line.startswith("FAIL") for line in lines))
            self.assertTrue(any("unverifiable locally" in line for line in lines))

    def test_main_uses_root_argument(self):
        out = io.StringIO()
        import contextlib
        with contextlib.redirect_stdout(out):
            code = matrix.main(["--root", str(matrix.ROOT)])
        self.assertEqual(code, 0)
        self.assertIn("30 pairs", out.getvalue())


if __name__ == "__main__":
    unittest.main()
