"""Synthetic receipt tampering tests; these are not physical probe evidence."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import docker_raw_table_evidence as evidence

VERSION = "1.8.11"
TOKEN = "vzraw0123456789"
TABLE = "-P PREROUTING ACCEPT\n-P OUTPUT ACCEPT"


def fixture(table=TABLE):
    return {"other_enclosing_fields": "validated elsewhere", "legacy_raw_prerouting": {
        "interface_token": TOKEN, "add_check_delete_and_table_preservation_proven": True,
        "probe": {"exit_code": 0, "stderr": evidence.ABSENT_RULE * 2,
                  "stdout": f"iptables_version=iptables v{VERSION} (legacy)\nraw_before_begin\n{table}\nraw_before_end\n"
                            f"raw_rule_added_and_checked={TOKEN}\nraw_after_begin\n{table}\nraw_after_end\n"
                            "developer-legacy-raw-prerouting-preserved\n"}}}


class RawProofTests(unittest.TestCase):
    def reject(self, mutate):
        value = fixture()
        mutate(value["legacy_raw_prerouting"])
        with self.assertRaises(evidence.InvalidEvidence):
            evidence.validate(value, VERSION)

    def test_valid_exact_probe_and_unrelated_table_rules(self):
        for table in (TABLE, TABLE + "\n-N unrelated\n-A PREROUTING -j unrelated",
                      TABLE.replace("OUTPUT ACCEPT", "OUTPUT DROP")):
            self.assertTrue(evidence.validate(fixture(table), VERSION)["table_preserved"])

    def test_missing_extra_fields_and_non_objects(self):
        value = fixture()["legacy_raw_prerouting"]
        for field in value:
            self.reject(lambda proof: proof.pop(field))
        for field in value["probe"]:
            self.reject(lambda proof: proof["probe"].pop(field))
        self.reject(lambda proof: proof.update(extra=True))
        self.reject(lambda proof: proof["probe"].update(extra=True))
        self.reject(lambda proof: proof.update(probe=[]))
        for document in ({}, [], None, {"legacy_raw_prerouting": None}):
            with self.assertRaises(evidence.InvalidEvidence):
                evidence.validate(document, VERSION)

    def test_status_claim_token_and_version_types(self):
        for value in (1, -1, True, False, "0", None):
            self.reject(lambda proof: proof["probe"].update(exit_code=value))
        for value in (1, False, "true", None):
            self.reject(lambda proof: proof.update(add_check_delete_and_table_preservation_proven=value))
        for token in (TOKEN.upper(), TOKEN + "a", TOKEN[:-1], "vzraw../bad", None, 1):
            self.reject(lambda proof: proof.update(interface_token=token))
        for version in ("1.8.10", "1.8.11 (legacy)", "1.8", None, 1):
            with self.assertRaises(evidence.InvalidEvidence):
                evidence.validate(fixture(), version)

    def test_before_after_changes_and_owned_baseline_rejected(self):
        self.reject(lambda proof: proof["probe"].update(stdout=proof["probe"]["stdout"].replace("OUTPUT ACCEPT", "OUTPUT DROP", 1)))
        for table in (TABLE + f"\n-A PREROUTING ! -i {TOKEN} -j DROP", TABLE + f"\n-N {TOKEN}"):
            with self.assertRaises(evidence.InvalidEvidence):
                evidence.validate(fixture(table), VERSION)

    def test_missing_duplicate_or_invalid_builtin_policies(self):
        for table in ("", "-P PREROUTING ACCEPT", "-P OUTPUT ACCEPT", TABLE + "\n-P OUTPUT ACCEPT",
                      TABLE.replace("OUTPUT ACCEPT", "FORWARD ACCEPT"), TABLE.replace("OUTPUT ACCEPT", "OUTPUT RETURN"),
                      TABLE + "\n-P extra ACCEPT", TABLE + "\n", TABLE + "\n-A "):
            with self.assertRaises(evidence.InvalidEvidence):
                evidence.validate(fixture(table), VERSION)

    def test_exact_markers_order_and_legacy_version(self):
        stdout = fixture()["legacy_raw_prerouting"]["probe"]["stdout"]
        lines = stdout.splitlines(keepends=True)
        for index, line in enumerate(lines):
            if line.startswith("-P "):
                continue
            for replacement in ("", line + line, line.upper()):
                self.reject(lambda proof: proof["probe"].update(stdout="".join(lines[:index]) + replacement + "".join(lines[index + 1:])))
        for value in (stdout.replace("(legacy)", "(nf_tables)"), stdout.rstrip("\n"),
                      "extra\n" + stdout, stdout + "extra\n", stdout.replace("raw_after_end", "raw_rollback_end"),
                      stdout.replace("raw_before_begin", "raw_after_begin")):
            self.reject(lambda proof: proof["probe"].update(stdout=value))

    def test_exact_two_absence_errors_required(self):
        for value in ("", evidence.ABSENT_RULE, evidence.ABSENT_RULE * 3,
                      evidence.ABSENT_RULE * 2 + "permission denied\n", evidence.ABSENT_RULE.upper() * 2,
                      (evidence.ABSENT_RULE * 2).rstrip("\n"), None, 0):
            self.reject(lambda proof: proof["probe"].update(stderr=value))

    def test_stream_bounds_control_bytes_and_utf8(self):
        for field, value in (("stdout", "x" * (evidence.STREAM_LIMIT + 1)), ("stderr", "x" * 4097),
                             ("stdout", None), ("stdout", "\ud800"), ("stdout", "\0"), ("stderr", "\r")):
            self.reject(lambda proof: proof["probe"].update({field: value}))

    def test_cli_valid_required_pin_and_duplicate_json_refusal(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "proof.json"
            command = [sys.executable, evidence.__file__, str(path)]
            path.write_text(json.dumps(fixture()))
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            command += ["--iptables-version", VERSION]
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            raw = json.dumps(fixture())
            for altered in (raw.replace('"exit_code": 0', '"exit_code": 0, "exit_code": 0'),
                            raw.replace('"interface_token":', '"interface_token": "wrong", "interface_token":'),
                            raw.replace('"legacy_raw_prerouting":', '"legacy_raw_prerouting": {}, "legacy_raw_prerouting":'),
                            raw.replace('"exit_code": 0', '"exit_code": NaN')):
                path.write_text(altered)
                result = subprocess.run(command, capture_output=True)
                self.assertEqual(result.returncode, 1)
                self.assertIn(b"rejected", result.stderr)

    def test_cli_rejects_non_regular_symlink_and_oversized_documents(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "proof.json"
            target = root / "target.json"
            target.write_text(json.dumps(fixture()))
            path.symlink_to(target)
            command = [sys.executable, evidence.__file__, str(path), "--iptables-version", VERSION]
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)
            path.unlink()
            path.mkdir()
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)
            path.rmdir()
            os.mkfifo(path)
            self.assertEqual(subprocess.run(command, capture_output=True, timeout=2).returncode, 1)
            path.unlink()
            os.link(target, path)
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)
            path.unlink()
            path.write_bytes(b" " * (evidence.DOCUMENT_LIMIT + 1))
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 1)


if __name__ == "__main__":
    unittest.main()
