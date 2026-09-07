"""Coverage table invariants and lane scenario translation; never Docker evidence."""
import unittest

import linux_docker_scenarios as subject


class TableTests(unittest.TestCase):
    def setUp(self):
        self.rows = subject.manifest()

    def test_every_required_id_is_claimed_once_or_explicitly_uncovered(self):
        try:
            from docker_compatibility_contract import REQUIRED_IDS
        except ImportError:
            REQUIRED_IDS = frozenset(self.rows)
        primaries = subject.check(rows=self.rows)
        gaps = dict(subject.UNCOVERED)
        self.assertEqual(set(primaries) | set(gaps), set(REQUIRED_IDS))
        self.assertFalse(set(primaries) & set(gaps))
        self.assertEqual(len(REQUIRED_IDS), 63)
        self.assertEqual(len(subject.coverage()), 63)
        self.assertEqual(set(gaps.values()), set(subject.GAP_SUITES))

    def test_no_id_is_proven_by_two_suites_and_secondaries_have_primaries(self):
        proven = [claim.id for claim in subject.TABLE if claim.status == "proven"]
        self.assertEqual(len(proven), len(set(proven)))
        for claim in subject.TABLE:
            if claim.status == "secondary":
                primary = subject.PRIMARY[claim.id]
                self.assertNotEqual(primary.suite, claim.suite, claim.id)

    def test_partial_claims_name_real_manifest_fields_only(self):
        for claim in subject.TABLE:
            expected = set(self.rows[claim.id]["expected"])
            if claim.status == "partial":
                self.assertTrue(claim.unproven, claim.id)
                self.assertTrue(set(claim.unproven) < expected, claim.id)
            else:
                self.assertEqual(claim.unproven, ())

    def test_rejections(self):
        rows = self.rows
        bad = subject.TABLE + (subject._c("docker.engine.version", "limits", "run_machine"),)
        with self.assertRaisesRegex(subject.CoverageError, "two suites"):
            subject.check(table=bad, rows=rows)
        with self.assertRaisesRegex(subject.CoverageError, "unknown scenario id"):
            subject.check(table=subject.TABLE + (subject._c("docker.engine.made_up", "limits", "x"),), rows=rows)
        with self.assertRaisesRegex(subject.CoverageError, "neither claimed nor uncovered"):
            subject.check(uncovered=subject.UNCOVERED[1:], rows=rows)
        with self.assertRaisesRegex(subject.CoverageError, "both claimed and uncovered"):
            subject.check(uncovered=subject.UNCOVERED + (("docker.engine.version", "mounts"),), rows=rows)
        with self.assertRaisesRegex(subject.CoverageError, "unknown gap suite"):
            subject.check(uncovered=subject.UNCOVERED[:-1] + ((subject.UNCOVERED[-1][0], "later"),), rows=rows)
        with self.assertRaisesRegex(subject.CoverageError, "distinct manifest expected fields"):
            table = tuple(c for c in subject.TABLE if c.id != "docker.engine.info") + (
                subject._c("docker.engine.info", "handshake", "run_machine", "partial", ("no_such_field",)),)
            subject.check(table=table, rows=rows)
        with self.assertRaisesRegex(subject.CoverageError, "two primary"):
            subject.check(table=subject.TABLE + (subject._c("docker.engine.info", "limits", "x", "partial", ("default_runtime",)),), rows=rows)
        with self.assertRaisesRegex(subject.CoverageError, "secondary claim without a primary"):
            subject.check(table=subject.TABLE + (subject._c("docker.storage.tmpfs", "limits", "x", "secondary"),),
                          uncovered=tuple(u for u in subject.UNCOVERED if u[0] != "docker.storage.tmpfs"), rows=rows)

    def test_module_declarations_come_from_the_table(self):
        self.assertEqual(subject.for_suite("limits"), ("docker.operation.resource_limits", "docker.operation.oom"))
        self.assertEqual(subject.for_suite("recovery"), ("docker.storage.persistence", "docker.operation.daemon_restart_recovery"))
        self.assertEqual(subject.for_suite("handshake", include_partial=False), ("docker.engine.version", "docker.engine.api_negotiation"))
        self.assertEqual(len(subject.for_suite("handshake")), 4)
        self.assertEqual(len(subject.for_suite("lifecycle")), 16)
        self.assertEqual(subject.for_recipe("compose", "compose-up-order"),
                         ("docker.compose.up", "docker.compose.dependency_ordering", "docker.compose.health_ordering"))
        self.assertEqual(subject.for_recipe("compose", "compose-network-paths"),
                         ("docker.compose.networks", "docker.network.user_defined_networks", "docker.network.dns"))
        self.assertEqual(subject.for_recipe("build", "build-multi-stage"), ("docker.build.output_export", "docker.build.multi_stage"))
        with self.assertRaises(subject.CoverageError):
            subject.for_recipe("build", "compose-create")
        with self.assertRaises(subject.CoverageError):
            subject.for_suite("mounts")
        for suite in subject.SUITES.values():
            self.assertIn(suite.process_scenario, subject.for_suite(suite.name))
        for suite in ("compose", "build"):
            import docker_host_driver as driver
            for recipe in driver.SUITE_RECIPES[suite]:
                self.assertTrue(subject.for_recipe(suite, recipe))


class LaneScenarioTests(unittest.TestCase):
    def test_passed_proven_is_pass_partial_is_fail_and_phase_filters(self):
        slices = [{"started_unix_ns": 10, "ended_unix_ns": 20, "workload": {"sibling_health": {"samples": 60}}},
                  {"started_unix_ns": 5, "ended_unix_ns": 30, "workload": {"sibling_health": {"samples": 60}}}]
        entries = subject.lane_scenarios("limits", slices, phase="clean-provision", passed=True, evidence_prefix="h")
        self.assertEqual([e["id"] for e in entries], ["docker.operation.resource_limits", "docker.operation.oom"])
        for entry in entries:
            self.assertEqual(entry["status"], "PASS")
            self.assertEqual((entry["started_unix_ns"], entry["ended_unix_ns"]), (5, 30))
            self.assertEqual(entry["evidence"], ["h/limits-machine-0/machine-limits-validation.json",
                                                 "h/limits-machine-1/machine-limits-validation.json"])
            self.assertEqual(entry["readiness_polls"], [{"id": "poll.service.health_probe", "samples": 120, "deadline_seconds": 60,
                                                         "satisfied": True}])
            self.assertTrue(all("asserted by limits/run_machine on 2 Machine(s)" in a for a in entry["assertions"]))
        self.assertEqual(subject.lane_scenarios("limits", slices, phase="final-cleanup", passed=True), [])
        handshake = subject.lane_scenarios("handshake", [{"started_unix_ns": 1, "ended_unix_ns": 2}], phase="clean-provision", passed=True)
        by_id = {e["id"]: e for e in handshake}
        self.assertEqual(by_id["docker.engine.version"]["status"], "PASS")
        self.assertEqual(by_id["docker.engine.info"]["status"], "FAIL")
        self.assertIn("UNPROVEN expected.daemon_unique_per_machine (no assertion in handshake)", by_id["docker.engine.info"]["assertions"])
        self.assertEqual(by_id["docker.engine.version"]["readiness_polls"], [])

    def test_failed_run_reports_every_suite_id_as_fail_with_the_error(self):
        entries = subject.lane_scenarios("recovery", [], phase="persisted-recovery/pre-sleep", passed=False, error="Rejected: x", window=(3, 9))
        self.assertEqual({e["id"] for e in entries}, set(subject.for_suite("recovery")))
        for entry in entries:
            self.assertEqual(entry["status"], "FAIL")
            self.assertEqual(entry["assertions"], ["run failed: Rejected: x"])
            self.assertEqual((entry["started_unix_ns"], entry["ended_unix_ns"]), (3, 9))
            self.assertEqual(entry["evidence"], [])
        self.assertEqual(subject.lane_scenarios("recovery", [], phase="clean-provision", passed=False), [])

    def test_lifecycle_without_slice_timings_uses_the_receipt_window(self):
        entries = subject.lane_scenarios("lifecycle", [{"workload": {}}], phase="clean-provision", passed=True, window=(100, 200))
        self.assertEqual(len(entries), 15)  # docker.container.remove belongs to final-cleanup
        self.assertTrue(all((e["started_unix_ns"], e["ended_unix_ns"]) == (100, 200) for e in entries))
        cleanup = subject.lane_scenarios("lifecycle", [{"workload": {}}], phase="final-cleanup", passed=True)
        self.assertEqual({e["id"] for e in cleanup}, {"docker.container.remove"})
        self.assertEqual({e["status"] for e in cleanup}, {"PASS"})


if __name__ == "__main__":
    unittest.main()
