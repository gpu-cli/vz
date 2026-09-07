import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vz04_common as common  # noqa: E402
import vz04_decisions as decisions  # noqa: E402


def _git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          env={"PATH": "/usr/bin:/bin", "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
                               "GIT_COMMITTER_EMAIL": "t@t", "HOME": str(repo)}).stdout.decode().strip()


class DecisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vz04-decisions-")
        self.root = Path(self.tmp.name).resolve()
        self.key = self.root / "throwaway"
        subprocess.run([decisions.ssh_keygen(), "-t", "ed25519", "-N", "", "-C", "test", "-f", str(self.key)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        public = " ".join((self.key.with_suffix(".pub")).read_text().split()[:2])
        self.repo = self.root / "repo"
        self.repo.mkdir()
        _git(self.repo, "init", "-q")
        (self.repo / "config").mkdir()
        (self.repo / "config" / "vz-0.4-decision-signatures").mkdir()
        (self.repo / "a").write_text("a")
        _git(self.repo, "add", ".")
        _git(self.repo, "commit", "-q", "-m", "first")
        self.first = _git(self.repo, "rev-parse", "HEAD")
        self.authorities = {"$schema": "../schemas/vz-0.4-decision-authorities.schema.json", "schema_version": 1,
                            "kind": "vz-0.4-decision-authorities", "target_release": "0.4.0", "signature_namespace": "vz-0.4-decision",
                            "authorities": [{"key_id": "vz04-test-1", "principal": "vz04-test", "algorithm": "ssh-ed25519",
                                             "public_key": public, "valid_from_commit": self.first, "revoked": False,
                                             "revoked_at_commit": None, "custody": "throwaway unit-test key"}]}
        self.decision = {"id": "dec-0001", "kind": "exclusion", "subject": "docker.build.ssh_mounts", "rationale": "test fixture",
                         "source_reference": "unit test", "approval": {"key_id": "vz04-test-1", "principal": "vz04-test",
                                                                       "approved_at_utc": "2026-09-07T00:00:00Z"},
                         "effective_commit": self.first,
                         "signature": {"algorithm": "ssh-ed25519", "namespace": "vz-0.4-decision",
                                       "file": "config/vz-0.4-decision-signatures/dec-0001.sig", "signed_sha256": None}}
        self.decision["signature"]["signed_sha256"] = common.sha256_bytes(decisions.signed_bytes(self.decision))
        signature = decisions.sign(self.decision, self.key)
        (self.repo / self.decision["signature"]["file"]).write_bytes(signature)
        self.decisions = {"$schema": "../schemas/vz-0.4-decisions.schema.json", "schema_version": 1, "kind": "vz-0.4-decisions",
                          "target_release": "0.4.0", "signature_namespace": "vz-0.4-decision",
                          "signatures_dir": "config/vz-0.4-decision-signatures", "decisions": [self.decision]}
        (self.repo / "config" / "vz-0.4-decisions.json").write_text(json.dumps(self.decisions))
        (self.repo / "config" / "vz-0.4-decision-authorities.json").write_text(json.dumps(self.authorities))
        _git(self.repo, "add", ".")
        _git(self.repo, "commit", "-q", "-m", "decision")
        self.decision_commit = _git(self.repo, "rev-parse", "HEAD")
        (self.repo / "b").write_text("b")
        _git(self.repo, "add", ".")
        _git(self.repo, "commit", "-q", "-m", "candidate")
        self.candidate = _git(self.repo, "rev-parse", "HEAD")

    def tearDown(self):
        self.tmp.cleanup()

    def test_signed_bytes_exclude_signature_member(self):
        body = json.loads(decisions.signed_bytes(self.decision))
        self.assertNotIn("signature", body)
        self.assertEqual(body["id"], "dec-0001")

    def test_signature_verifies_and_tamper_fails(self):
        signature = (self.repo / self.decision["signature"]["file"]).read_bytes()
        ok, _ = decisions.verify_signature(self.decision, signature, self.authorities)
        self.assertTrue(ok)
        tampered = dict(self.decision, rationale="changed")
        ok, _ = decisions.verify_signature(tampered, signature, self.authorities)
        self.assertFalse(ok)
        revoked = json.loads(json.dumps(self.authorities))
        revoked["authorities"][0]["revoked"] = True
        ok, _ = decisions.verify_signature(self.decision, signature, revoked)
        self.assertFalse(ok)

    def test_verify_decisions_with_ancestry(self):
        report = decisions.verify_decisions(self.candidate, repo_root=self.repo, decisions=self.decisions, authorities=self.authorities)
        self.assertEqual(report["findings"], [])
        self.assertEqual(report["verified"], ["dec-0001"])
        report = decisions.verify_decisions(self.decision_commit, repo_root=self.repo, decisions=self.decisions, authorities=self.authorities)
        self.assertTrue(any(code == "decision.file_ancestry" for code, _s, _d in report["findings"]))
        self.assertEqual(report["verified"], ["dec-0001"])
        report = decisions.verify_decisions(self.first, repo_root=self.repo, decisions=self.decisions, authorities=self.authorities)
        self.assertEqual(report["verified"], [])
        codes = {code for code, _s, _d in report["findings"]}
        self.assertIn("decision.authority_ancestry", codes)

    def test_wrong_signed_digest_and_missing_signature(self):
        broken = json.loads(json.dumps(self.decisions))
        broken["decisions"][0]["signature"]["signed_sha256"] = "0" * 64
        report = decisions.verify_decisions(self.candidate, repo_root=self.repo, decisions=broken, authorities=self.authorities, ancestry=False)
        self.assertEqual([code for code, _s, _d in report["findings"]], ["decision.signed_digest"])
        (self.repo / self.decision["signature"]["file"]).unlink()
        report = decisions.verify_decisions(self.candidate, repo_root=self.repo, decisions=self.decisions, authorities=self.authorities, ancestry=False)
        self.assertEqual([code for code, _s, _d in report["findings"]], ["decision.signature_missing"])

    def test_repo_decisions_are_empty_and_authority_key_is_real(self):
        report = decisions.verify_decisions(common.git_head(common.REPO_ROOT), repo_root=common.REPO_ROOT, ancestry=False)
        self.assertEqual(report["verified"], [])
        self.assertEqual(report["findings"], [])
        docker = common.load_json(common.REPO_ROOT / common.CONFIG_FILES["docker_contract"])
        self.assertEqual(decisions.exclusion_findings(docker, []), [])
        self.assertTrue(decisions.exclusion_findings({"intentional_exclusions": [{"id": "x"}]}, []))


if __name__ == "__main__":
    unittest.main()
