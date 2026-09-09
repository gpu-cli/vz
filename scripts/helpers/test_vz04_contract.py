import copy
import json
from pathlib import Path
import re
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import docker_compatibility_contract as docker_contract  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_contract as contract_module  # noqa: E402


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = contract_module.load_contract()
        cls.docker = contract_module.load_docker_contract()

    def test_required_inventory_is_84_unique_ids(self):
        rows = contract_module.required_scenarios(self.contract, self.docker)
        # 84, not 85: criterion 9 (seeded network faults) was withdrawn from
        # 0.4 and criterion 8 reduced to unconditional isolation. See
        # GOAL-0.4.0.md, where the number 9 is left vacant on purpose.
        self.assertEqual(len(rows), 84)
        self.assertEqual(len({row["id"] for row in rows}), 84)
        self.assertEqual(sum(row["id"].startswith("gate.") for row in rows), 21)
        self.assertEqual({row["id"] for row in rows if row["id"].startswith("docker.")}, set(docker_contract.REQUIRED_IDS))
        self.assertTrue(all(row["lane"] == "linux-docker" for row in rows if row["id"].startswith("docker.")))
        self.assertTrue(all(row["phase"] in common.LANE_PHASES for row in rows))

    def test_gate_ids_match_plan_table(self):
        ids = [s["id"] for s in self.contract["scenarios"]]
        self.assertEqual(ids[0], "gate.instances.three_concurrent_no_collision")
        # Last, not index 21: criterion 9's scenario was withdrawn, so the list
        # is one shorter. Indexing from the end says "the plan's final row"
        # rather than restating a length this test does not own.
        self.assertEqual(ids[-1], "gate.definition.reconciliation_fencing")
        self.assertNotIn("gate.faults.measured_network_faults", ids)
        self.assertIn("gate.lifecycle.recovery_including_sleep_wake", ids)
        by_id = {s["id"]: s for s in self.contract["scenarios"]}
        self.assertEqual(by_id["gate.lifecycle.recovery_including_sleep_wake"]["phase"], "persisted-recovery/post-wake")
        self.assertEqual(by_id["gate.runtime.youki_only_provenance"]["lane"], "linux-docker")

    def test_duplicate_or_missing_criterion_rejected(self):
        broken = copy.deepcopy(self.contract)
        broken["scenarios"][1]["id"] = broken["scenarios"][0]["id"]
        with self.assertRaises(common.GateError):
            contract_module.required_scenarios(broken, self.docker)
        broken = copy.deepcopy(self.contract)
        broken["scenarios"][1]["criterion"] = 1
        with self.assertRaises(common.GateError):
            contract_module.required_scenarios(broken, self.docker)
        broken_docker = copy.deepcopy(self.docker)
        broken_docker["scenarios"].pop()
        with self.assertRaises(common.GateError):
            contract_module.required_scenarios(self.contract, broken_docker)

    def test_frozen_inputs_report_draft_state_and_stable_digests(self):
        frozen = contract_module.frozen_inputs(self.contract)
        codes = {code for code, _s, _d in frozen["findings"]}
        self.assertIn("input.draft", codes)
        self.assertIn("contract.unpinned", codes)
        self.assertEqual(frozen["inputs"]["e2e_contract"]["sha256"],
                         common.digest_file(common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"]))
        again = contract_module.frozen_inputs(self.contract)
        self.assertEqual(frozen["digests"], again["digests"])
        self.assertEqual(len(frozen["digests"]["fixture_dirs"]), len(self.contract["fixtures"]["required_dirs"]))
        for key in common.CONFIG_FILES:
            self.assertIn(key, frozen["inputs"])

    def test_missing_harness_file_is_a_finding_not_an_error(self):
        broken = copy.deepcopy(self.contract)
        broken["harness"]["files"].append("scripts/does-not-exist.sh")
        frozen = contract_module.frozen_inputs(broken)
        self.assertTrue(any(code == "harness.missing" and subject == "scripts/does-not-exist.sh" for code, subject, _ in frozen["findings"]))


if __name__ == "__main__":
    unittest.main()


class NativeMacosPinTests(unittest.TestCase):
    """The `native_macos` pins must attest a macOS build the product can produce.

    The contract previously declared 26.6.2/25G83 while the installer hardcodes
    26.3.1/25D2128 and refuses to boot a template newer than the host, so the
    pinned build was one no shipped code path could produce and no 26.3.1 host
    could run. Nothing noticed, because `vz04_contract` only checks these values
    for non-null. These tests read the truth out of the Rust source and the
    recorded hardware evidence, so a pin that drifts from either fails offline.
    """

    SETUP = common.REPO_ROOT / "crates/vz-cli/src/native_setup/mod.rs"
    EVIDENCE = common.REPO_ROOT / "planning/developer-environments/macos-swift-dev-evidence.json"

    @classmethod
    def setUpClass(cls):
        cls.native = contract_module.load_contract()["native_macos"]
        cls.evidence = json.loads(cls.EVIDENCE.read_text())

    @staticmethod
    def _rust_const(source: str, name: str) -> str:
        match = re.search(r'const ' + name + r': &str = "([^"]+)"', source)
        if match is None:
            raise AssertionError(f"{name} is no longer a string constant in native_setup/mod.rs")
        return match.group(1)

    def test_pinned_build_is_the_one_the_installer_produces(self):
        source = self.SETUP.read_text()
        self.assertEqual(self.native["guest_version"], self._rust_const(source, "VERSION"))
        self.assertEqual(self.native["guest_build"], self._rust_const(source, "BUILD"))

    def test_the_reader_notices_a_drifting_installer_constant(self):
        # Vacuity: the comparison above is only meaningful if a changed constant
        # would actually be seen. Rewrite VERSION and confirm the reader follows.
        source = self.SETUP.read_text().replace('const VERSION: &str = "26.3.1"',
                                                'const VERSION: &str = "26.6.2"')
        self.assertEqual(self._rust_const(source, "VERSION"), "26.6.2")
        self.assertNotEqual(self.native["guest_version"], self._rust_const(source, "VERSION"))

    def test_ipsw_pin_names_the_same_build_and_exists(self):
        pin = common.REPO_ROOT / self.native["ipsw_pin"]
        self.assertTrue(pin.is_file(), f"ipsw_pin does not exist: {self.native['ipsw_pin']}")
        self.assertIn(f"{self.native['guest_version']}-{self.native['guest_build']}", pin.name)
        body = json.loads(pin.read_text())
        self.assertIn(f"{self.native['guest_version']}_{self.native['guest_build']}", body["url"])
        self.assertRegex(body["sha256"], r"^[0-9a-f]{64}$")

    def test_the_pinned_build_matches_the_recorded_hardware_run(self):
        # Version and build ARE stable product facts, so the recorded run must
        # agree with the contract about which macOS this is.
        self.assertTrue(self.evidence["passed"])
        self.assertEqual(self.evidence["guest"]["version"], self.native["guest_version"])
        self.assertEqual(self.evidence["guest"]["build"], self.native["guest_build"])
        manifest = self.evidence["manifest"]
        self.assertEqual(manifest["macos_version"], self.native["guest_version"])
        self.assertEqual(manifest["macos_build"], self.native["guest_build"])

    def test_the_three_digests_describe_a_template_that_exists(self):
        """The digests must name the template a gate run would actually resolve.

        They were first pinned from `macos-swift-dev-evidence.json`, which was
        the wrong coupling: `prepared_image_sha256` is per-INSTALLATION, not a
        product constant -- every `vz-macos-setup` run installs macOS afresh and
        produces a different 80 GB image, and the guest agent and toolchain
        digests move with the build and the host's Xcode. Pinning them to a
        historical run made the contract describe a template no longer on the
        host. `macos-bootstrap-integration.md` says why there is nothing stabler
        to point at yet: the IPSW pin is "a maintainer source input, not a
        consumer release manifest", and no consumer manifest has been published.

        So the pins are compared against the LIVE registered template. On a host
        with none, they must still be well-formed and distinct -- which is all
        that can honestly be claimed there, and is stated rather than skipped.
        """
        digests = {name: self.native[name] for name in
                   ("guest_agent_sha256", "prepared_image_sha256", "xcode_toolchain_sha256")}
        for name, digest in digests.items():
            self.assertRegex(digest or "", r"^[0-9a-f]{64}$", name)
        self.assertEqual(len(set(digests.values())), 3, "three roles cannot share one digest")
        manifest = self._registered_template_manifest()
        if manifest is None:
            self.skipTest("no macOS template is registered on this host; the digests cannot be "
                          "checked against one. Register one with vz-macos-setup.")
        self.assertEqual(manifest["macos_version"], self.native["guest_version"])
        self.assertEqual(manifest["macos_build"], self.native["guest_build"])
        self.assertEqual(self.native["guest_agent_sha256"], manifest["guest_agent_sha256"])
        self.assertEqual(self.native["xcode_toolchain_sha256"], manifest["toolchain_sha256"])
        self.assertEqual(self.native["prepared_image_sha256"], manifest["prepared_image"]["sha256"])

    @staticmethod
    def _registered_template_manifest():
        """The manifest of a registered macOS template, or None.

        Read-only discovery: this never provisions a template, because setup
        downloads an Apple IPSW and takes an administrator authorisation.
        """
        for catalog_path in sorted(common.REPO_ROOT.glob(".artifacts/*/machine-target-catalog.json")) + \
                [Path.home() / ".vz" / "machine-target-catalog.json"]:
            try:
                catalog = json.loads(catalog_path.read_text())
            except (OSError, ValueError):
                continue
            for entry in catalog.get("macos") or []:
                bundle = Path(str(entry.get("installed_bundle", "")))
                digest = (entry.get("manifest") or {}).get("sha256", "")
                manifest = bundle / digest
                if manifest.is_file():
                    try:
                        return json.loads(manifest.read_text())
                    except (OSError, ValueError):
                        continue
        return None

    def test_host_could_boot_the_pinned_template(self):
        # `native_macos::artifacts::prepare` refuses a template whose
        # minimum_host_version exceeds the host, and setup stamps that field
        # with VERSION, so a pin above the recipe's own floor is unbootable.
        floor = self.evidence["manifest"]["platform"]["minimum_host_version"]
        self.assertEqual(floor, self.native["guest_version"])

    def test_setup_recipe_and_declared_steps_name_real_files(self):
        recipe = common.REPO_ROOT / self.native["setup_recipe"]
        self.assertTrue(recipe.is_file(), f"setup_recipe does not exist: {self.native['setup_recipe']}")
        self.assertIn("vz-macos-setup", recipe.read_text())
        steps = self.native["privilege_steps"] + self.native["license_steps"]
        self.assertTrue(steps)
        for step in steps:
            cited = re.findall(r"crates/[A-Za-z0-9._/-]+\.rs", step)
            self.assertTrue(cited, f"step cites no source file: {step}")
            for relative in cited:
                self.assertTrue((common.REPO_ROOT / relative).is_file(), f"step cites a missing file: {relative}")

    def test_declared_steps_describe_what_the_code_does(self):
        source = self.SETUP.read_text()
        self.assertIn("--provision-disk", source)
        self.assertIn("/usr/bin/osascript", source)
        self.assertIn("/usr/bin/sudo", source)
        toolchain = (common.REPO_ROOT / "crates/vz-cli/src/native_setup/toolchain_install.rs").read_text()
        self.assertIn("xcodebuild -license accept", toolchain)
        self.assertIn("accept_xcode_license", toolchain)



class InstallerComponentTests(unittest.TestCase):
    """The installer's binary list against the release contract's.

    `scripts/install.sh` installs the host binaries and `--uninstall` removes
    them. Criterion 19 claims uninstall removes only, and all of, vz-owned
    software, which is only true while that list is the release's list.

    It was not: install and uninstall each carried their own hand-written copy,
    and adding a sixth binary (`vz-runtime-probe`) to one left the other at
    five, so an uninstall silently stranded it. Both now read one `VZ_BINARIES`,
    and this compares that one to the contract rather than to a third copy
    written here -- a restated list would drift the same way.
    """

    INSTALL = common.REPO_ROOT / "scripts/install.sh"

    def declared_binaries(self) -> list:
        match = re.search(r'^VZ_BINARIES="([^"]*)"', self.INSTALL.read_text(), re.M)
        self.assertIsNotNone(match, "install.sh declares no VZ_BINARIES")
        return match.group(1).split()

    def test_the_installer_owns_exactly_the_release_host_binaries(self):
        contract = common.load_json(common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"])
        required = contract["release"]["required_components"]
        expected = sorted(path.split("/", 1)[1] for path in required if path.startswith("bin/"))
        self.assertEqual(sorted(self.declared_binaries()), expected)

    def test_install_and_uninstall_read_the_same_list(self):
        text = self.INSTALL.read_text()
        # One definition, and every consumer a loop over it: a second literal
        # spelling of the set is the defect this test exists to prevent.
        self.assertEqual(len(re.findall(r'^VZ_BINARIES=', text, re.M)), 1)
        self.assertEqual(len(re.findall(r'for binary in \$VZ_BINARIES; do', text)), 2)

    def test_the_reader_would_notice_a_shortened_list(self):
        """Vacuity: the comparison must be capable of failing."""
        shortened = sorted(self.declared_binaries())[:-1]
        contract = common.load_json(common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"])
        expected = sorted(path.split("/", 1)[1] for path in contract["release"]["required_components"]
                          if path.startswith("bin/"))
        self.assertNotEqual(shortened, expected)
