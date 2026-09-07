import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_candidate as candidate  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_contract as contract_module  # noqa: E402


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vz04-candidate-")
        self.release = fixtures.build_fake_release_dir(Path(self.tmp.name).resolve() / "release")

    def tearDown(self):
        fixtures.make_writable(self.release)
        self.tmp.cleanup()

    def _unlock(self, relative):
        path = self.release / relative
        path.chmod(stat.S_IMODE(os.lstat(path).st_mode) | 0o200)
        return path

    def test_fake_release_dir_admits_with_injected_verifier(self):
        admitted = candidate.admit_release_dir(str(self.release), codesign_verifier=fixtures.fake_codesign_verifier)
        self.assertEqual(admitted["signing_class"], "local-test-signed")
        self.assertEqual(set(admitted["components"]), set(fixtures.COMPONENTS) | {"entitlements.plist"})
        # The fake manifest is plan-shaped, not builder-schema-complete: the only
        # admissible findings are schema findings against the builder's schema.
        self.assertEqual({code for code, _s, _d in admitted["findings"]} - {"release.schema"}, set())
        self.assertEqual(admitted["release_dir_sha256"], common.tree_digest(self.release))

    def test_codesign_failure_is_a_finding(self):
        admitted = candidate.admit_release_dir(str(self.release), codesign_verifier=lambda _p: (False, "nope"))
        codes = {(code, subject) for code, subject, _ in admitted["findings"]}
        self.assertIn(("release.codesign", "bin/vz"), codes)
        self.assertNotIn(("release.codesign", "entitlements.plist"), codes)

    def test_tampered_component_is_refused(self):
        self._unlock("bin/vz").write_bytes(b"tampered\n")
        self._unlock("bin/vz").chmod(0o555)
        with self.assertRaises(common.GateError):
            candidate.admit_release_dir(str(self.release), codesign_verifier=fixtures.fake_codesign_verifier)

    def test_writable_file_is_refused(self):
        self._unlock("codesign/vz.verify.log")
        with self.assertRaises(common.GateError) as caught:
            candidate.admit_release_dir(str(self.release), codesign_verifier=fixtures.fake_codesign_verifier)
        self.assertIn("writable", str(caught.exception))

    def test_manifest_digest_mismatch_is_refused(self):
        path = self._unlock("release-manifest.sha256")
        path.write_bytes(("1" * 64 + "  release-manifest.json\n").encode())
        with self.assertRaises(common.GateError):
            candidate.admit_release_dir(str(self.release), codesign_verifier=fixtures.fake_codesign_verifier)

    def test_missing_manifest_field_is_refused(self):
        path = self._unlock("release-manifest.json")
        manifest = json.loads(path.read_bytes())
        del manifest["buildkit"]
        data = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
        path.write_bytes(data)
        digest_path = self._unlock("release-manifest.sha256")
        digest_path.write_bytes(f"{common.sha256_bytes(data)}  release-manifest.json\n".encode())
        with self.assertRaises(common.GateError) as caught:
            candidate.admit_release_dir(str(self.release), codesign_verifier=fixtures.fake_codesign_verifier)
        self.assertIn("buildkit", str(caught.exception))

    def test_symlinked_release_path_is_refused(self):
        alias = Path(self.tmp.name) / "alias"
        alias.symlink_to(self.release)
        with self.assertRaises(common.GateError):
            candidate.admit_release_dir(str(alias), codesign_verifier=fixtures.fake_codesign_verifier)

    def test_candidate_tuple_changes_with_inputs(self):
        admitted = candidate.admit_release_dir(str(self.release), codesign_verifier=fixtures.fake_codesign_verifier)
        contract = contract_module.load_contract()
        frozen = contract_module.frozen_inputs(contract)
        first = candidate.candidate_tuple(source_commit="0" * 40, source_tree_sha256="1" * 64, release=admitted, frozen=frozen)
        second = candidate.candidate_tuple(source_commit="0" * 40, source_tree_sha256="1" * 64, release=admitted, frozen=frozen)
        self.assertEqual(first, second)
        self.assertEqual(first["sha256"], common.sha256_bytes((common.canonical_json(first["tuple"]) + "\n").encode()))
        frozen["inputs"]["e2e_contract"]["sha256"] = "f" * 64
        third = candidate.candidate_tuple(source_commit="0" * 40, source_tree_sha256="1" * 64, release=admitted, frozen=frozen)
        self.assertNotEqual(first["sha256"], third["sha256"])
        codes = {code for code, _s, _d in candidate.tuple_findings(admitted, frozen, third)}
        self.assertIn("release.signing_class", codes)
        self.assertIn("release.notarization", codes)


if __name__ == "__main__":
    unittest.main()
