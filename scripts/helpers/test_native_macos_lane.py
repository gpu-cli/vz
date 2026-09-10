"""Offline tests for the `native-macos` lane translation.

The passing fixture is not invented: its `swift` block, guest version and build
are read out of `planning/developer-environments/macos-swift-dev-evidence.json`,
the recorded output of the run that actually built and ran the Swift fixture on
Apple silicon on 2026-09-06. Every negative case is that same evidence with one
thing changed, so a translation that starts accepting a failed run is caught by
the vacuity suite rather than by the next physical run.
"""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import frozen_tree  # noqa: E402
import native_macos_lane_result as lane_module  # noqa: E402
import vz04_lanes as lanes  # noqa: E402
import vz04_schema as schema  # noqa: E402
import vz04_common as common  # noqa: E402

EVIDENCE = common.REPO_ROOT / "planning/developer-environments/macos-swift-dev-evidence.json"
DIGEST = "a" * 64
RUN_ID = "native-lane-test"


def recorded():
    return json.loads(EVIDENCE.read_text())


def native_pins():
    """The contract's own `native_macos` pins, which the translation checks against."""
    return json.loads((common.REPO_ROOT / "config/vz-0.4-e2e-contract.json").read_text())["native_macos"]


def passing_summary():
    """A `summary.json` shaped exactly as the harness writes one for a passing --require-swift run."""
    evidence = recorded()
    return {"scope": lane_module.E2E_SCOPE, "root": "/private/tmp/vzmac-cli-fixture",
            "aggregate_release_certified": False, "results": [], "passed": True,
            "swift": copy.deepcopy(evidence["swift"])}


def passing_receipts():
    """One zero-exit receipt per guest Swift command, as `run(...)` writes them."""
    return {name: {"name": name, "argv": ["/private/tmp/vzmac-cli-fixture/install/bin/vz", "exec", "--"],
                   "exit_code": 0, "elapsed_seconds": 1.0}
            for name in lane_module.SWIFT_RECEIPTS}


def base_result(phase=lane_module.CLEAN_PROVISION):
    ctx = lanes.LaneContext(run_id=RUN_ID, release_dir="/nonexistent", release_dir_sha256=DIGEST,
                            state_root="/nonexistent", contract_path="/nonexistent", contract_sha256=DIGEST,
                            candidate_tuple_sha256=DIGEST, fixture_sha256=DIGEST,
                            clients={"docker": "none", "compose_plugin": "none", "buildx_plugin": "none"})
    entry = {"path": lane_module.ENTRY_POINT, "sha256": DIGEST, "argv": ["--suite", "all"]}
    return lanes.base_result(lane_module.LANE, phase, ctx, entry)


def outcome(**overrides):
    fields = {"invoked": True, "exit_code": 0, "summary": passing_summary(), "receipts": passing_receipts(),
              "started_unix_ns": 1_000, "ended_unix_ns": 2_000,
              "evidence_files": ["e2e/summary.json", "e2e/swift-build.stdout", "harness.stdout"]}
    fields.update(overrides)
    return lane_module.E2eOutcome(**fields)


def translate(**overrides):
    return lane_module.translate(base_result(), outcome(**overrides), native_pins())


class SchemaShapeTests(unittest.TestCase):
    """Every state the lane can report must be a schema-valid lane result."""

    def _valid(self, result):
        self.assertEqual(schema.validate("lane-result", result), [])

    def test_every_outcome_validates(self):
        cases = {
            "passed": translate()[0],
            "assertion": translate(exit_code=1, summary=dict(passing_summary(), passed=False, error="boom"))[0],
            "prerequisite": lane_module.translate(base_result(), outcome(invoked=False, prerequisite="no template"),
                                                  native_pins())[0],
            "crash": lane_module.translate(base_result(), outcome(harness_error="ValueError: broke"),
                                           native_pins())[0],
            "timeout": translate(exit_code=None)[0],
        }
        for name, result in cases.items():
            with self.subTest(name):
                self._valid(result)
                self.assertEqual(result["lane"], "native-macos")
                self.assertEqual(len(result["scenarios"]), 1)
                self.assertEqual(result["scenarios"][0]["id"], lane_module.SCENARIO)

    def test_scenario_evidence_is_confined_to_the_harness_directory(self):
        result, _code = translate()
        self.assertEqual(result["scenarios"][0]["evidence"], ["e2e/summary.json", "e2e/swift-build.stdout"])
        self.assertIn("harness.stdout", result["evidence_files"])


class PassingRunTests(unittest.TestCase):
    def test_a_real_recorded_run_passes_and_says_why(self):
        result, code = translate()
        self.assertEqual(result["outcome"], "passed")
        self.assertIsNone(result["failure"])
        self.assertEqual(code, lane_module.EXIT_PASSED)
        scenario = result["scenarios"][0]
        self.assertEqual(scenario["status"], "PASS")
        text = " ".join(scenario["assertions"])
        self.assertIn(lane_module.PROBE_PROTOCOL, text)
        self.assertIn(lane_module.HARDWARE_MODEL, text)
        self.assertIn("swift-test", text)
        self.assertEqual(result["prohibited_observed"], {key: False for key in lane_module.PROHIBITED_KEYS})
        self.assertEqual(result["retained_root"], "/private/tmp/vzmac-cli-fixture")


class FailingRunTests(unittest.TestCase):
    def test_harness_ran_and_failed_an_assertion(self):
        summary = dict(passing_summary(), passed=False, error="native-version: sw_vers mismatch")
        summary.pop("swift")
        result, code = translate(exit_code=1, summary=summary)
        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(result["failure"]["reason"], "assertion")
        self.assertEqual(result["failure"]["exit_code"], 1)
        self.assertEqual(code, lane_module.EXIT_FAILED)
        self.assertIn("sw_vers mismatch", result["failure"]["detail"])
        self.assertEqual(result["scenarios"][0]["status"], "FAIL")

    def test_timeout_is_not_an_assertion(self):
        result, _code = translate(exit_code=None)
        self.assertEqual(result["failure"]["reason"], "timeout")
        self.assertIsNone(result["failure"]["exit_code"])

    def test_harness_crash_is_not_an_assertion(self):
        result, _code = lane_module.translate(
            base_result(), outcome(harness_error="OSError: no such file"), native_pins())
        self.assertEqual(result["failure"]["reason"], "crash")
        self.assertIn("no such file", result["failure"]["detail"])

    def test_a_run_that_wrote_no_summary_is_a_crash_not_a_pass(self):
        result, _code = lane_module.translate(
            base_result(), outcome(summary=None, harness_error="harness exited 0 without writing e2e/summary.json"),
            native_pins())
        self.assertEqual(result["failure"]["reason"], "crash")

    def test_a_lane_that_neither_ran_nor_named_a_prerequisite_is_a_crash(self):
        result, _code = lane_module.translate(base_result(), outcome(invoked=False), native_pins())
        self.assertEqual(result["failure"]["reason"], "crash")


class AbsentTemplateTests(unittest.TestCase):
    def test_absent_template_is_prerequisite_not_assertion_and_not_pass(self):
        detail = "no installed machine-target catalog registers a native macOS template"
        result, code = lane_module.translate(base_result(), outcome(invoked=False, prerequisite=detail),
                                             native_pins())
        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(result["failure"]["reason"], "prerequisite")
        self.assertEqual(code, lane_module.EXIT_FAILED)
        self.assertIn(detail, result["failure"]["detail"])
        self.assertEqual(result["scenarios"][0]["status"], "FAIL")

    def test_prerequisite_reason_is_one_the_gate_already_understands(self):
        # `vz04_lanes.account` copies `failure.reason` verbatim into the row's
        # `reason`, and the schema fixes the vocabulary. Inventing a reason here
        # would make the row unaccountable.
        allowed = schema.load_schema("lane-result")["properties"]["failure"]["anyOf"][1]["properties"]["reason"]["enum"]
        for reason in ("prerequisite", "assertion", "crash", "timeout", "not_implemented", "input_rejected"):
            self.assertIn(reason, allowed)


class CorruptInputScopeTests(unittest.TestCase):
    """`--expect-preparation-failure` sets `passed: True` without booting a guest."""

    def test_a_preparation_failure_run_cannot_certify_criterion_4(self):
        summary = {"scope": "INSTALLED_NATIVE_CORRUPT_INPUT_DELETE", "root": "/private/tmp/x",
                   "aggregate_release_certified": False, "results": [], "passed": True}
        result, _code = translate(exit_code=0, summary=summary, receipts={})
        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(result["failure"]["reason"], "assertion")
        self.assertIn("INSTALLED_NATIVE_CORRUPT_INPUT_DELETE", result["failure"]["detail"])


class VacuityTests(unittest.TestCase):
    """Prove the translation would notice if it started reporting PASS for a failed run.

    Each case is the recorded passing evidence with exactly one thing changed
    in a way that means the capability was not demonstrated. If any of these
    still reported PASS, the checks above would be decoration.
    """

    def test_the_passing_fixture_really_does_pass(self):
        # Without this, every mutation below could "fail" for an unrelated reason.
        self.assertEqual(translate()[0]["outcome"], "passed")

    def _mutations(self):
        pins = native_pins()
        yield "harness exited non-zero", dict(exit_code=1)
        yield "summary says it failed", dict(summary=dict(passing_summary(), passed=False))
        yield "summary passed is merely truthy", dict(summary=dict(passing_summary(), passed="yes"))
        yield "summary claims release certification", dict(
            summary=dict(passing_summary(), aggregate_release_certified=True))
        summary = passing_summary()
        summary.pop("swift")
        yield "run was not --require-swift", dict(summary=summary)
        for field, value in (("protocol", "something-else"), ("protocol_version", 2),
                             ("hardware_model", "Mac16,5"), ("pid", 1),
                             ("os_version", "26.6.2"), ("os_build", "25G83")):
            summary = passing_summary()
            summary["swift"]["probe"][field] = value
            yield f"probe.{field} wrong", dict(summary=summary)
        summary = passing_summary()
        summary["swift"].pop("persisted_probe")
        yield "no probe after Stop/Up", dict(summary=summary)
        summary = passing_summary()
        summary["swift"]["toolchain_sha256"] = "b" * 64
        yield "toolchain is not the contract pin", dict(summary=summary)
        summary = passing_summary()
        summary["swift"]["source_sha256"] = {k: v for k, v in summary["swift"]["source_sha256"].items()
                                             if not k.startswith("Tests/")}
        yield "no test source transferred", dict(summary=summary)
        summary = passing_summary()
        summary["swift"]["source_sha256"] = {}
        yield "no fixture transferred at all", dict(summary=summary)
        for name in ("swift-build", "swift-test", "swift-run"):
            receipts = passing_receipts()
            receipts.pop(name)
            yield f"{name} never ran", dict(receipts=receipts)
            receipts = passing_receipts()
            receipts[name] = dict(receipts[name], exit_code=1)
            yield f"{name} exited non-zero", dict(receipts=receipts)
        yield "no receipts at all", dict(receipts={})
        yield "no summary at all", dict(summary=None)
        receipts = passing_receipts()
        receipts["cargo"] = {"name": "cargo", "argv": ["/usr/bin/cargo", "run"], "exit_code": 0}
        yield "cargo run observed", dict(receipts=receipts)
        receipts = passing_receipts()
        receipts["relative"] = {"name": "relative", "argv": ["vz", "exec"], "exit_code": 0}
        yield "PATH fallback observed", dict(receipts=receipts)
        receipts = passing_receipts()
        receipts["ssh"] = {"name": "ssh", "argv": ["/usr/bin/docker", "-H", "ssh://host"], "exit_code": 0}
        yield "ssh host observed", dict(receipts=receipts)
        self.assertTrue(pins)

    def test_every_broken_run_fails(self):
        for name, override in self._mutations():
            with self.subTest(name):
                result, code = translate(**override)
                self.assertEqual(result["outcome"], "failed", f"{name} still reported PASS")
                self.assertNotEqual(code, lane_module.EXIT_PASSED)
                self.assertEqual(result["scenarios"][0]["status"], "FAIL")

    def test_the_mutation_suite_is_load_bearing(self):
        """A permissive claim layer must actually report PASS for a failed run.

        Without this, the suite above could be passing because every mutation
        happens to trip some unrelated guard, and would keep passing if the
        claim derivation were deleted. Replace both readers of the harness
        verdict with ones that approve everything, and confirm the same failed
        run then reports PASS -- and that the real translation refuses it.
        """
        failed_run = dict(exit_code=0, summary=dict(passing_summary(), passed=False, error="swift test failed"))
        self.assertEqual(translate(**failed_run)[0]["outcome"], "failed")
        original_claims, original_verdict = lane_module.swift_claims, lane_module.summary_passed
        lane_module.swift_claims = lambda summary, receipts, native: (["approved"], [])
        lane_module.summary_passed = lambda summary: True
        try:
            approved = translate(**failed_run)[0]
        finally:
            lane_module.swift_claims, lane_module.summary_passed = original_claims, original_verdict
        self.assertEqual(approved["outcome"], "passed",
                         "the permissive stub is not permissive, so this vacuity proof shows nothing")
        self.assertEqual(translate(**failed_run)[0]["outcome"], "failed")

    def test_two_independent_readers_guard_the_harness_verdict(self):
        # Either reader alone must be enough to refuse a run the harness failed.
        failed_run = dict(exit_code=0, summary=dict(passing_summary(), passed=False))
        original = lane_module.swift_claims
        lane_module.swift_claims = lambda summary, receipts, native: (["approved"], [])
        try:
            self.assertEqual(translate(**failed_run)[0]["outcome"], "failed")
        finally:
            lane_module.swift_claims = original
        original_verdict = lane_module.summary_passed
        lane_module.summary_passed = lambda summary: True
        try:
            self.assertEqual(translate(**failed_run)[0]["outcome"], "failed")
        finally:
            lane_module.summary_passed = original_verdict


class ProhibitedObservationTests(unittest.TestCase):
    def test_flags_are_derived_from_receipts_not_asserted(self):
        receipts = passing_receipts()
        receipts["runc"] = {"name": "runc", "argv": ["/usr/local/bin/runc", "run"], "exit_code": 0}
        result, _code = translate(receipts=receipts)
        self.assertTrue(result["prohibited_observed"]["runc"])
        self.assertEqual(result["outcome"], "failed")
        self.assertIn("prohibited component observed", result["failure"]["detail"])

    def test_a_clean_run_reports_every_flag_false(self):
        result, _code = translate()
        self.assertFalse(any(result["prohibited_observed"].values()))


class TemplateDiscoveryTests(unittest.TestCase):
    """Discovery is read-only: it reads catalogs and never provisions anything."""

    def _catalog(self, root, entries):
        path = Path(root) / lane_module.CATALOG_NAME
        path.write_text(json.dumps({"schema_version": 1, "linux": [], "macos": entries}))
        return path

    def test_registered_template_is_found(self):
        with tempfile.TemporaryDirectory() as root:
            bundle = Path(root) / "bundle"
            bundle.mkdir()
            (bundle / DIGEST).write_text("{}")
            path = self._catalog(root, [{"image": "vz-macos", "version": "26.3.1",
                                         "manifest": {"sha256": DIGEST}, "installed_bundle": str(bundle),
                                         "channels": ["latest", "clean"]}])
            template, _notes = lane_module.template_from_catalog(path)
            self.assertIsNotNone(template)
            self.assertEqual(template["bundle"], str(bundle))
            self.assertEqual(template["manifest_sha256"], DIGEST)

    def test_empty_macos_array_registers_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._catalog(root, [])
            template, notes = lane_module.template_from_catalog(path)
            self.assertIsNone(template)
            self.assertTrue(any("registers no macOS template" in note for note in notes))

    def test_a_registered_entry_whose_bundle_is_gone_is_reported(self):
        with tempfile.TemporaryDirectory() as root:
            path = self._catalog(root, [{"image": "vz-macos", "version": "26.3.1",
                                         "manifest": {"sha256": DIGEST},
                                         "installed_bundle": str(Path(root) / "vanished"),
                                         "channels": ["latest"]}])
            template, notes = lane_module.template_from_catalog(path)
            self.assertIsNone(template)
            self.assertTrue(any("registered bundle directory is gone" in note for note in notes))

    def test_a_bundle_without_its_manifest_is_not_usable(self):
        with tempfile.TemporaryDirectory() as root:
            bundle = Path(root) / "bundle"
            bundle.mkdir()
            path = self._catalog(root, [{"image": "vz-macos", "version": "26.3.1",
                                         "manifest": {"sha256": DIGEST}, "installed_bundle": str(bundle),
                                         "channels": ["latest"]}])
            template, notes = lane_module.template_from_catalog(path)
            self.assertIsNone(template)
            self.assertTrue(any("holds no manifest named" in note for note in notes))

    def test_a_template_without_the_latest_channel_is_not_selected(self):
        with tempfile.TemporaryDirectory() as root:
            bundle = Path(root) / "bundle"
            bundle.mkdir()
            (bundle / DIGEST).write_text("{}")
            path = self._catalog(root, [{"image": "vz-macos", "version": "26.3.1",
                                         "manifest": {"sha256": DIGEST}, "installed_bundle": str(bundle),
                                         "channels": ["xcode"]}])
            self.assertIsNone(lane_module.template_from_catalog(path)[0])

    def test_missing_catalog_is_a_note_not_an_error(self):
        with tempfile.TemporaryDirectory() as root:
            template, notes = lane_module.template_from_catalog(Path(root) / lane_module.CATALOG_NAME)
            self.assertIsNone(template)
            self.assertTrue(any("no catalog" in note for note in notes))

    def test_the_operator_override_does_not_fall_through(self):
        # docs/installed-machine-catalog.md: "A present invalid override does
        # not fall through to installed discovery." A lane that fell through
        # could exercise a template the daemon would never select.
        with tempfile.TemporaryDirectory() as root:
            bundle = Path(root) / "bundle"
            bundle.mkdir()
            (bundle / DIGEST).write_text("{}")
            self._catalog(root, [{"image": "vz-macos", "version": "26.3.1", "manifest": {"sha256": DIGEST},
                                  "installed_bundle": str(bundle), "channels": ["latest"]}])
            environ = {"VZ_MACHINE_TARGET_CATALOG": str(Path(root) / "absent.json")}
            self.assertEqual(lane_module.catalog_locations(root, root, environ),
                             [Path(root) / "absent.json"])
            self.assertIsNone(lane_module.find_template(root, root, environ)[0])
            # ...and without the override the same registered template is found.
            self.assertIsNotNone(lane_module.find_template(root, root, {})[0])

    def test_this_host_registers_no_native_template(self):
        # The state the lane must report honestly rather than crash on.
        template, _notes = lane_module.find_template(None, str(Path.home()), environ={})
        self.assertIsNone(template, "a native macOS template is registered; the prerequisite path is no longer "
                                    "the state of this host and the physical lane can be exercised")


class UnassignedPhaseTests(unittest.TestCase):
    def test_only_clean_provision_carries_the_criterion(self):
        contract = json.loads((common.REPO_ROOT / "config/vz-0.4-e2e-contract.json").read_text())
        rows = [s for s in contract["scenarios"] if s["lane"] == lane_module.LANE]
        self.assertEqual([(row["id"], row["phase"]) for row in rows],
                         [(lane_module.SCENARIO, lane_module.CLEAN_PROVISION)])

    def test_other_phases_claim_nothing(self):
        for phase in ("persisted-recovery/pre-sleep", "persisted-recovery/post-wake", "final-cleanup"):
            with self.subTest(phase):
                result = lanes.failed_result(lane_module.LANE, phase, lanes.LaneContext(
                    run_id=RUN_ID, release_dir="/nonexistent", release_dir_sha256=DIGEST, state_root="/nonexistent",
                    contract_path="/nonexistent", contract_sha256=DIGEST, candidate_tuple_sha256=DIGEST,
                    fixture_sha256=DIGEST, clients={}), {"path": lane_module.ENTRY_POINT, "sha256": DIGEST,
                                                         "argv": []}, "not_implemented", "no scenario assigned",
                    lane_module.EXIT_NOT_IMPLEMENTED)
                self.assertEqual(schema.validate("lane-result", result), [])
                self.assertEqual(result["scenarios"], [])


class WrapperTests(unittest.TestCase):
    def test_the_entry_point_is_no_longer_the_stub(self):
        body = (common.REPO_ROOT / lane_module.ENTRY_POINT).read_text()
        self.assertNotIn("vz04_lanes.py stub", body)
        self.assertIn("native_macos_lane_result.py", body)

    def test_the_wrapper_reaches_the_lane_through_a_frozen_tree(self):
        """The wrapper runs the lane from a frozen worktree and leaves none behind.

        `test_the_entry_point_is_no_longer_the_stub` reads the wrapper as text,
        which cannot tell whether it still executes. This one runs it.

        The exit code alone proves nothing and asserting only on it made this
        test vacuous: a wrapper pointed at a file that does not exist also exits
        2, because that is what CPython returns when it cannot open the script.
        So the assertions are the two things that are true only of a real run --
        `frozen_tree` announced the tree it froze, and the REJECTION IS THE
        LANE'S OWN, in the lane's own words. Neither survives breaking the
        wrapper.

        The frozen root must also be gone afterwards: a lane that leaks a full
        worktree per run fills the volume the Machines are provisioned on.
        """
        script = common.REPO_ROOT / lane_module.ENTRY_POINT
        before = set(frozen_tree.FROZEN_BASE.glob(frozen_tree.FROZEN_PREFIX + "*"))
        completed = subprocess.run([str(script), "--suite", "lifecycle"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   timeout=600, check=False,
                                   env={"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/")})
        stdout, stderr = completed.stdout.decode(), completed.stderr.decode()
        self.assertEqual(completed.returncode, 2, stderr[-2000:])
        self.assertIn("==> frozen tree ", stdout)
        self.assertIn(f"{lane_module.LANE} lane rejected input", stderr)
        self.assertEqual(set(frozen_tree.FROZEN_BASE.glob(frozen_tree.FROZEN_PREFIX + "*")) - before, set())

    def test_the_contract_still_names_this_entry_point(self):
        contract = json.loads((common.REPO_ROOT / "config/vz-0.4-e2e-contract.json").read_text())
        declared = [entry for entry in contract["lanes"] if entry["name"] == lane_module.LANE]
        self.assertEqual(len(declared), 1)
        self.assertEqual(declared[0]["entry_point"], lane_module.ENTRY_POINT)
        self.assertEqual(declared[0]["argv"], ["--suite", "all"])


if __name__ == "__main__":
    unittest.main()
