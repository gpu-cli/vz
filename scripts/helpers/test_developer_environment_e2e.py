"""Unit tests for the topology lane skeleton against a fake `vz`.

Run: uv run --no-project --python /usr/bin/python3 --with-requirements scripts/helpers/gate-requirements.txt \
       python -B -m unittest scripts/helpers/test_developer_environment_e2e.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from jsonschema import Draft202012Validator

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import developer_environment_checks as checks  # noqa: E402
import developer_environment_e2e as e2e  # noqa: E402
import developer_environment_recorder as recorder  # noqa: E402
import developer_environment_test_support as support  # noqa: E402
import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_contract as contract_module  # noqa: E402
import vz04_lanes as lanes  # noqa: E402
import vz04_schema as schema  # noqa: E402

DIGEST = "b" * 64
RUN_ID = "topology-unit-run-1"
TOP21 = e2e.CRITERION_21
TOP15 = e2e.CRITERION_15
TOP1 = "gate.instances.three_concurrent_no_collision"
TOP5 = "gate.network.private_topology_paths"
TOP2 = e2e.CRITERION_2
TOP17 = e2e.CRITERION_17
TOP7 = e2e.CRITERION_7
TOP16 = "gate.reproducibility.recreate_from_definition"
TOP11 = "gate.delete.single_environment_safety"
TOP19 = e2e.CRITERION_19
TOP6 = e2e.CRITERION_6
INGRESS_SLUG = "public_like_ingress"
IMPLEMENTED = {"bare_help", "legacy_rejection", "clean_up_refuses", "bootstrap_read_only", "help_surface_exact",
               "error_envelope_agreement", "bootstrap_creates_default", "three_concurrent_no_collision",
               "status_json_field_set", "grpc_api_live_agreement", "workspace_storage_policy"}
# `private_topology_paths` proves criterion 5's Linux-to-Linux half and stops
# there. The criterion also requires a service path crossing between a Linux
# Machine and a native macOS Machine in both directions, and no fake CLI can
# stand in for that: it needs a macOS template a gate host does not provision.
# It was in IMPLEMENTED while the check reported PASS on the Linux half alone,
# which certified the criterion on evidence that never touched its macOS clause.
#
# `host_import_export_boundaries` proves every clause of criterion 7 except the
# one no runtime here can offer: "enabled egress does not create one" needs a
# Machine with non-offline egress, and this Up refuses those until the egress
# gateway lands. The check names that clause exactly and declines to claim PASS
# on the rest, rather than certifying the criterion on the clauses that ran.
# `public_like_ingress` now proves criterion 6's split-DNS, `.test`-hostname,
# edge-versus-origin, cross-Environment, TLS, routed-ingress, source-translation
# and host-listener clauses -- the TLS half became reachable when the Developer
# image gained `vz-guest-fetch`. It stops at the criterion's controlled-egress,
# host-import/export and fault-control clauses, which no adapter implements.
# `install_upgrade_rollback_uninstall` proves every clause of criterion 19
# except one: the restored store being opened again by v0.3.20 itself needs
# the pinned ~22 MiB v0.3.20 daemon, which is neither committed nor fetched
# from inside a check. These tests deliberately unstage it (see `setUp`), so
# offline the check reports the gap by name instead of claiming the criterion.
#
# `mixed_profile_topology_status` proves criterion 2 for two Developer Linux
# Machines and one Hardened Linux Machine, including the Hardened Machine's
# denial of a sibling Developer endpoint. The criterion also requires one native
# macOS Machine in the same Environment, and the fake release registers no macOS
# target -- as no release candidate built by `build-vz-0.4-release-candidate.sh`
# does. The check declines to claim PASS on the Linux subset rather than
# certifying the criterion on evidence that never built a macOS Machine.
#
# `grpc_api_live_agreement` is deliberately absent from this set: the release
# now ships vz-runtime-probe, so criterion 15's typed channel is exercised.
NOT_IMPLEMENTED = {"private_topology_paths", "install_upgrade_rollback_uninstall",
                   "mixed_profile_topology_status", "public_like_ingress",
                   "host_import_export_boundaries"}
# One component that puts the fixture's `--state-root` at the depth a real gate
# run has, so no socket can be bound anywhere under it.
DEEP_STATE_ROOT_PADDING = "private-var-folders-style-gate-state-root-depth-vz04"


class TopologyLaneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="vztl-", dir="/private/tmp"))
        self.mode_file = self.tmp / "mode"
        self.release = support.build_fake_release(self.tmp / "release", mode_file=self.mode_file)
        # A state root as deep as the gate's own: `vz04_gate` mkdtemps under
        # `/var/folders/<2>/<28>/T` and the observed run's was 115 bytes.
        # Nothing the lane binds may depend on the state root being short --
        # that is exactly what stalled every provisioning sub-check, and a short
        # fixture root hides it.
        self.state_root = self.tmp / DEEP_STATE_ROOT_PADDING / "state"
        self.contract = contract_module.load_contract()
        self.lane = contract_module.lane_by_name(self.contract)["topology"]
        self.counter = 0
        self.socket_root = recorder.socket_root_for(self.state_root)
        self.addCleanup(shutil.rmtree, self.socket_root, ignore_errors=True)
        # persisted-recovery/pre-sleep leaves its daemons running on purpose, and
        # a standalone phase can too. Left alive they outlive tearDown's rmtree
        # and keep writing into a tree the next test is using -- which showed up
        # as a different test failing each run with the same signature, its own
        # temp directory vanishing underneath it. The lane's own stopper is used
        # so this cleans up exactly what the lane started.
        self.addCleanup(self.stop_lane_daemons)
        # Criterion 19 runs the pinned v0.3.20 daemon over the store it restored
        # when it is staged. Point it at a path that is not, so these tests
        # observe one outcome whatever this machine has cached.
        unstaged = mock.patch.dict(os.environ, {checks.LEGACY_ARTIFACT_ENV: str(self.tmp / "unstaged-v0320")})
        unstaged.start()
        self.addCleanup(unstaged.stop)

    def tearDown(self):
        fixtures.make_writable(self.release)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_fixture_state_root_reproduces_the_af_unix_constraint(self):
        """The fixture must reproduce the constraint, not dodge it."""
        state = e2e.LaneState(self.state_root, self.release / "bin")
        # Both AF_UNIX paths the old layout produced are over the limit under a
        # state root of gate depth: the daemon socket and, by 45 bytes more, a
        # Machine's Docker endpoint.
        daemon_socket = self.state_root / "topology" / "boot" / "r" / "d.sock"
        endpoint = daemon_socket.parent / ("x" * recorder.ENDPOINT_NAME_BYTES)
        for path in (daemon_socket, endpoint):
            self.assertGreater(len(str(path).encode()), recorder.SOCKET_PATH_LIMIT, path)
        budget = state.socket_budget()
        self.assertTrue(budget["bindable"], budget)
        self.assertFalse(str(state.socket).startswith(str(self.state_root)))

    def stop_lane_daemons(self):
        """Stop any daemon this test's lane left supervising Machines."""
        state = e2e.LaneState(self.state_root, self.release / "bin")
        if not state.root.exists():
            return
        try:
            e2e.stop_daemons(state)
        except e2e.CleanupError:
            # Reported by the phase that owns cleanup; a test teardown that
            # raised here would mask the failure the test was making.
            pass

    def set_mode(self, mode: str):
        self.mode_file.write_text(mode)

    def argv(self, phase: str, evidence: Path, handoff=None, **overrides) -> list:
        ctx = lanes.LaneContext(run_id=RUN_ID, release_dir=self.release, release_dir_sha256=DIGEST, state_root=self.state_root,
                                contract_path=common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"], contract_sha256=DIGEST,
                                candidate_tuple_sha256=DIGEST, fixture_sha256=DIGEST, clients={})
        argv = lanes.lane_argv(self.lane, ctx, phase, evidence, handoff)
        for key, value in overrides.items():
            flag = "--" + key.replace("_", "-")
            if flag in argv:
                argv[argv.index(flag) + 1] = value
            else:
                # `--only` is optional, so the gate's argv builder does not emit
                # it and there is nothing to replace.
                argv += [flag, value]
        return argv

    def evidence(self) -> Path:
        self.counter += 1
        path = self.tmp / f"evidence-{self.counter}"
        path.mkdir()
        return path

    def run_lane(self, argv: list, evidence: Path):
        code = e2e.main(argv, codesign_verifier=fixtures.fake_codesign_verifier)
        result = common.load_json(evidence / "lane-result.json")
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["test_case_retries"], 0)
        self.assertEqual((result["lane"], result["run_id"], result["candidate_tuple_sha256"]), ("topology", RUN_ID, DIGEST))
        for relative in result["evidence_files"]:
            self.assertTrue((evidence / relative).is_file(), relative)
        return code, result

    def by_slug(self, result: dict) -> dict:
        return {s["id"].split("__", 1)[1]: s for s in result["scenarios"] if "__" in s["id"]}

    def top(self, result: dict, identifier: str) -> dict:
        return next(s for s in result["scenarios"] if s["id"] == identifier)

    # -- argument handling --------------------------------------------------------------
    def test_no_identity_means_exit_2_without_result(self):
        self.assertEqual(e2e.main(["--suite", "lifecycle"], codesign_verifier=fixtures.fake_codesign_verifier), 2)
        evidence = self.evidence()
        argv = self.argv("clean-provision", evidence)
        argv[argv.index("--phase") + 1] = "not-a-phase"
        self.assertEqual(e2e.main(argv, codesign_verifier=fixtures.fake_codesign_verifier), 2)
        self.assertFalse((evidence / "lane-result.json").exists())

    def test_rejections_emit_input_rejected_result(self):
        for overrides in ({"suite": "lifecycle"}, {"release_dir": str(self.tmp)}, {"contract": str(self.tmp / "release/checksums.sha256")},
                          {"docker": "relative/docker"}):
            evidence = self.evidence()
            code, result = self.run_lane(self.argv("clean-provision", evidence, **overrides), evidence)
            self.assertEqual(code, 2, overrides)
            self.assertEqual((result["outcome"], result["failure"]["reason"]), ("failed", "input_rejected"), overrides)
            self.assertEqual(result["scenarios"], [])
        evidence = self.evidence()
        argv = self.argv("clean-provision", evidence) + ["--extra", "x"]
        code, result = self.run_lane(argv, evidence)
        self.assertEqual((code, result["failure"]["reason"]), (2, "input_rejected"))
        self.assertIn("unknown option --extra", result["failure"]["detail"])
        self.assertFalse((self.state_root / "topology").exists())

    def test_scan_argv_contract(self):
        options, problems = e2e.scan_argv(["--suite", "all", "--run-id=x"])
        self.assertEqual((options["suite"], options["run-id"]), ("all", "x"))
        self.assertTrue(any(p.startswith("missing required option --phase") for p in problems))
        _options, problems = e2e.scan_argv(["--suite", "all", "--suite", "all", "positional"])
        self.assertIn("duplicate option --suite", problems)
        self.assertIn("unexpected positional argument 'positional'", problems)

    # -- clean-provision ----------------------------------------------------------------
    def test_conformant_cli_passes_every_implemented_sub_check(self):
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual(code, 3)
        self.assertEqual((result["outcome"], result["failure"]["reason"]), ("failed", "not_implemented"))
        subs = self.by_slug(result)
        self.assertEqual(set(subs), IMPLEMENTED | NOT_IMPLEMENTED)
        for slug in IMPLEMENTED:
            self.assertEqual(subs[slug]["status"], "PASS", (slug, subs[slug]["assertions"]))
            self.assertTrue(subs[slug]["evidence"], slug)
        for slug in NOT_IMPLEMENTED:
            self.assertEqual(subs[slug]["status"], "FAIL", slug)
            self.assertTrue(any(a.startswith("not_implemented:") for a in subs[slug]["assertions"]), slug)
        # Criterion 21's sub-checks are all implemented now, so it is the first
        # topology scenario the lane can pass. Criterion 15 joined it once the
        # release shipped a typed client for the daemon channel.
        self.assertEqual(self.top(result, TOP21)["status"], "PASS")
        self.assertEqual(self.top(result, TOP1)["status"], "PASS")
        # Criterion 17 has one sub-check and it covers every clause: the three
        # projection modes, the forbidden write, the pre-mutation refusal of a
        # writable block multi-attach, and the shared-cache fixture.
        self.assertEqual(self.top(result, TOP17)["status"], "PASS")
        # The fake applies declared networks, so criterion 5's Linux half runs to
        # completion -- and that is exactly why it must not be read as the
        # criterion. The crossing to a native macOS Machine is unexercised, so
        # the check declines to claim PASS; see check_private_topology_paths.
        self.assertEqual(self.top(result, TOP5)["status"], "FAIL")
        # Criterion 7 runs every clause it can against the fake -- the granted
        # import serves, and each denial is measured against it -- and still
        # declines to PASS while the enabled-egress clause is unexercisable.
        self.assertEqual(self.top(result, TOP7)["status"], "FAIL")
        self.assertEqual(self.top(result, TOP15)["status"], "PASS")
        assigned = {s["id"] for s in self.contract["scenarios"] if s["lane"] == "topology" and s["phase"] == "clean-provision"}
        tops = {s["id"]: s for s in result["scenarios"] if "__" not in s["id"]}
        self.assertEqual(set(tops), assigned)
        self.assertEqual(self.top(result, TOP2)["status"], "FAIL")
        for identifier in assigned - {TOP21, TOP15, TOP1, TOP17}:
            self.assertEqual(tops[identifier]["status"], "FAIL")
            self.assertIn("not_implemented", tops[identifier]["assertions"][0])
        # Criterion 19's own sub-check ran; what it could not do is named.
        self.assertTrue(any("pinned v0.3.20 daemon" in a for a in subs["install_upgrade_rollback_uninstall"]["assertions"]),
                        subs["install_upgrade_rollback_uninstall"]["assertions"])
        cli_removal = common.load_json(common.REPO_ROOT / self.contract["pins"]["cli_removal"])
        expected = (len(cli_removal["removed_roots"]) * 9 + 18 +
                    (len(cli_removal["dev_baseline"]["help_paths"]) + len(cli_removal["normative_only_paths"])) * 4)
        self.assertTrue(any(a.startswith(f"{expected}/{expected} invocations rejected") for a in subs["legacy_rejection"]["assertions"]),
                        subs["legacy_rejection"]["assertions"][-3:])
        self.assertTrue(any("0 connections" in a for a in subs["legacy_rejection"]["assertions"]))
        starts = [s["scenario_id"] for s in result["process_starts"]]
        self.assertEqual(len(starts), len(set(starts)))
        # bootstrap_creates_default dispatches the CLI too, now that it provisions.
        self.assertEqual(set(starts), {f"{TOP21}__{s}" for s in IMPLEMENTED
                                       if s in ("bare_help", "legacy_rejection", "clean_up_refuses",
                                                "bootstrap_read_only", "bootstrap_creates_default")} |
                         {f"{TOP15}__help_surface_exact", f"{TOP15}__error_envelope_agreement",
                          f"{TOP1}__three_concurrent_no_collision", f"{TOP17}__workspace_storage_policy",
                          f"{TOP5}__private_topology_paths", f"{TOP15}__status_json_field_set",
                          f"{TOP15}__grpc_api_live_agreement",
                          f"{TOP19}__install_upgrade_rollback_uninstall",
                          f"{TOP2}__mixed_profile_topology_status",
                          f"{TOP6}__public_like_ingress",
                          f"{TOP7}__host_import_export_boundaries"})
        receipts = sorted((evidence / "receipts").glob("*.json"))
        self.assertGreater(len(receipts), expected)
        for path in receipts[:5] + receipts[-5:]:
            self.assertEqual(schema.validate("receipt", common.load_json(path)), [], path.name)
        self.assertEqual(result["retained_root"], str(self.state_root / "topology"))
        self.assertTrue((self.state_root / "topology").is_dir())
        self.assertEqual(result["handoff"]["produced"], e2e.HANDOFF_SENTINEL)
        self.assertTrue((evidence / e2e.HANDOFF_SENTINEL).is_file())
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        self.assertEqual((evidence / "bare-help-observed.txt").read_bytes(),
                         (common.REPO_ROOT / "tests/fixtures/vz-0.4/cli/help-snapshot.txt").read_bytes())
        for name in ("clean-state-root-before.txt", "clean-state-root-after.txt", "bare-isolated-before.txt", "bare-isolated-after.txt"):
            self.assertTrue((evidence / "inventories" / name).is_file(), name)
        self.assertEqual((evidence / "inventories/clean-state-root-before.txt").read_bytes().split(b"\n", 2)[2],
                         (evidence / "inventories/clean-state-root-after.txt").read_bytes().split(b"\n", 2)[2])

    def test_existing_lane_state_root_is_a_prerequisite_failure(self):
        (self.state_root / "topology").mkdir(parents=True)
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "prerequisite"))

    def test_existing_socket_root_is_a_prerequisite_failure(self):
        """A leaked socket root would silently share sockets between runs."""
        self.socket_root.mkdir(mode=0o700)
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "prerequisite"))
        self.assertIn(str(self.socket_root), result["failure"]["detail"])
        self.assertFalse((self.state_root / "topology").exists())

    def test_unbindable_socket_root_is_a_prerequisite_failure(self):
        """Sockets nothing can bind are said once, not re-derived per sub-check."""
        deep = self.tmp / ("d" * 90)
        deep.mkdir()
        evidence = self.evidence()
        with mock.patch.object(recorder, "SOCKET_ROOT_BASE", deep):
            code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "prerequisite"))
        self.assertIn("sun_path", result["failure"]["detail"])
        self.assertFalse((self.state_root / "topology").exists())

    def assert_regression(self, mode: str, slug: str, needle: str, *also: str):
        """One falsifying mode, asserted against the one sub-check it breaks.

        `--only` runs that sub-check alone. Running the whole phase to assert one
        claim costs ten unrelated checks per test and, across this module, most
        of the suite's runtime -- and it does not strengthen the assertion. The
        phase still grades every scenario it is assigned, so a partial run can
        never report `passed`; `test_conformant_cli_passes_every_implemented_sub_check`
        keeps running the full set, which is the test whose job that is.
        """
        self.set_mode(mode)
        evidence = self.evidence()
        code, result = self.run_lane(
            self.argv("clean-provision", evidence, only=",".join((slug, *also))), evidence)
        self.assertEqual((code, result["outcome"], result["failure"]["reason"]), (1, "failed", "assertion"), mode)
        self.assertIn("subcheck-filter.txt", result["evidence_files"])
        sub = self.by_slug(result)[slug]
        self.assertEqual(sub["status"], "FAIL", mode)
        self.assertTrue(any(needle in a for a in sub["assertions"]), (mode, sub["assertions"]))
        owner = {"bare_help": TOP21, "legacy_rejection": TOP21, "clean_up_refuses": TOP21,
                 "bootstrap_read_only": TOP21, "bootstrap_creates_default": TOP21,
                 "workspace_storage_policy": TOP17}.get(slug, TOP15)
        self.assertEqual(self.top(result, owner)["status"], "FAIL")
        return result

    def test_bare_mutation_fails_bare_help(self):
        result = self.assert_regression("mutate", "bare_help", "isolated root changed: appeared: project/discovered",
                                        "legacy_rejection")
        self.assertEqual(self.by_slug(result)["legacy_rejection"]["status"], "PASS")

    def test_snapshot_drift_fails_bare_help(self):
        self.assert_regression("drift", "bare_help", "FAILED: bare vz: stdout == snapshot")

    def test_executable_alias_fails_legacy_rejection(self):
        result = self.assert_regression("alias", "legacy_rejection", "vz create: exit 0 (expected 2)", "bare_help")
        self.assertEqual(self.by_slug(result)["bare_help"]["status"], "PASS")

    def test_a_typed_channel_that_disagrees_fails_criterion_15(self):
        """Agreement has to be capable of failing.

        The fake typed channel is built to agree, so criterion 15 passing
        against it says nothing on its own. probe_drift mints one Environment
        identity the CLI never published and changes nothing else; the check
        must refuse it, or it is comparing shapes rather than identities.
        """
        result = self.assert_regression("probe_drift", "grpc_api_live_agreement",
                                        "Environment environment_id agrees", "status_json_field_set")
        self.assertEqual(self.by_slug(result)["status_json_field_set"]["status"], "PASS")

    def test_provisioning_up_fails_clean_directory_check(self):
        # criterion 7's check is named as well, because the second half of this
        # test is about what an Up that persists nothing does to it.
        result = self.assert_regression("provisions", "clean_up_refuses", "lane state root changed",
                                        "host_import_export_boundaries")
        self.assertTrue(any("exit 0 (expected 2)" in a for a in self.by_slug(result)["clean_up_refuses"]["assertions"]))
        # An Up that reports success and persists no topology leaves every
        # criterion-7 clause unexercised. The check must FAIL rather than return
        # early with an empty PASS: that is exactly how a criterion gets
        # certified on evidence that never touched it.
        boundaries = self.by_slug(result)["host_import_export_boundaries"]
        self.assertEqual(boundaries["status"], "FAIL")
        self.assertTrue(any("reports a readable status" in a for a in boundaries["assertions"]),
                        boundaries["assertions"])

    def test_hanging_command_is_uncertain_effects(self):
        self.set_mode("hang")
        evidence = self.evidence()
        # `hang` makes `vz ls` sleep past its deadline, and legacy_rejection is
        # the check that invokes it.
        code, result = self.run_lane(
            self.argv("clean-provision", evidence, only="legacy_rejection"), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "uncertain_effects"))
        errors = [p for p in (evidence / "receipts").glob("*.json") if common.load_json(p)["state"] == "error"]
        self.assertTrue(errors)
        self.assertTrue(all(common.load_json(p)["effects_uncertain"] for p in errors))

    def test_autospawned_daemon_is_detected_and_stopped_gracefully(self):
        self.set_mode("autospawn")
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "assertion"))
        sub = self.by_slug(result)["bootstrap_read_only"]
        self.assertEqual(sub["status"], "FAIL")
        # The spawn happens in the lane's socket root, which the state-root
        # inventory cannot see; the check names that directory itself.
        self.assertTrue(any("read-only status created" in a for a in sub["assertions"]), sub["assertions"])
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        cleanup = (evidence / "cleanup.txt").read_text()
        self.assertIn("graceful_shutdown_observed", cleanup)
        self.assertIn(str(self.release / "bin/vz-runtimed"), cleanup)
        for root in (self.state_root / "topology", self.socket_root):
            self.assertEqual(sorted(root.rglob("*.pid")), [], root)
            self.assertFalse(list(root.rglob("*.sock")), root)

    def test_unattributable_pid_file_is_a_cleanup_failure(self):
        self.set_mode("bogus_pid")
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual((code, result["failure"]["reason"]), (1, "cleanup"))
        self.assertTrue(result["cleanup_errors"])
        self.assertIn("no positively identified daemon", result["cleanup_errors"][0])

    # -- later phases -----------------------------------------------------------------
    def test_persisted_recovery_provisions_and_recovers_across_the_checkpoint(self):
        """pre-sleep leaves Environments running; post-wake finds those exact ones.

        The phase used to be a skeleton that provisioned nothing and stamped
        every scenario `not_implemented: needs provisioned Machines`. It now
        establishes three Environments, deliberately does not delete them, and
        the post-wake invocation -- a separate lane run with its own evidence
        directory, on the far side of the sleep/wake checkpoint -- addresses the
        same isolates through the state root both phases derive.
        """
        evidence = self.evidence()
        code, _result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual(code, 3)
        handoff = self.tmp / "state-handoff.deadbeef.json"
        handoff.write_bytes(b"{}\n")
        established = None
        for phase in ("persisted-recovery/pre-sleep", "persisted-recovery/post-wake"):
            evidence = self.evidence()
            code, result = self.run_lane(self.argv(phase, evidence, handoff=str(handoff)), evidence)
            self.assertEqual((code, result["failure"]["reason"], result["phase"]), (3, "not_implemented", phase),
                             result["failure"]["detail"])
            self.assertEqual(result["handoff"]["consumed"], handoff.name)
            self.assertEqual(result["handoff"]["consumed_sha256"], common.digest_file(handoff))
            self.assertEqual(result["retained_root"], str(self.state_root / "topology"))
            assigned = {s["id"] for s in self.contract["scenarios"]
                        if s["lane"] == "topology" and s["phase"] == phase}
            tops = {s["id"] for s in result["scenarios"] if "__" not in s["id"]}
            self.assertEqual(tops, assigned)
            record = common.load_json(evidence / e2e.RECOVERY_RECORD)
            self.assertEqual(record["kind"], checks.RECOVERY_RECORD_KIND)
            self.assertEqual([e["isolate"] for e in record["environments"]], list(e2e.RECOVERY_ISOLATES))
            if phase.endswith("pre-sleep"):
                established = record
                # Left running on purpose: an Environment that was torn down
                # cannot demonstrate that anything survived.
                for name in e2e.RECOVERY_ISOLATES:
                    self.assertTrue((self.state_root / "topology" / name / "project" / "vz.json").is_file(), name)
                self.assertTrue((evidence / "checks" / "establish_recovery_environments.txt").is_file())
            else:
                # Same Environments, addressed again after the checkpoint.
                self.assertEqual(record, established)
                recovery = self.by_slug(result)["lifecycle_recovery"]
                self.assertEqual(recovery["status"], "FAIL", recovery["assertions"])
                self.assertTrue(any(a.startswith("not_implemented:") for a in recovery["assertions"]),
                                recovery["assertions"])
                # Everything it *did* exercise passed; only the unexercised
                # clauses keep it from claiming the criterion.
                self.assertFalse([a for a in recovery["assertions"] if a.startswith("FAILED:")],
                                 recovery["assertions"])
                for entry in established["environments"]:
                    self.assertTrue(any(f"{entry['isolate']}: stop/up preserved the Environment identity" in a
                                        for a in recovery["assertions"]), entry["isolate"])

    def test_post_wake_without_a_pre_sleep_record_is_a_prerequisite_failure(self):
        """Post-wake must not invent what should have survived."""
        evidence = self.evidence()
        code, _result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual(code, 3)
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("persisted-recovery/post-wake", evidence), evidence)
        self.assertEqual((code, result["outcome"], result["failure"]["reason"]), (1, "failed", "prerequisite"))
        self.assertIn(e2e.RECOVERY_RECORD, result["failure"]["detail"])

    def test_final_cleanup_is_honest(self):
        evidence = self.evidence()
        code, _result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        self.assertEqual(code, 3)
        handoff = self.tmp / "state-handoff.deadbeef.json"
        handoff.write_bytes(b"{}\n")
        evidence = self.evidence()
        code, result = self.run_lane(self.argv("final-cleanup", evidence, handoff=str(handoff)), evidence)
        # final-cleanup is assigned exactly two scenarios and implements both,
        # so it passes. It reported not_implemented unconditionally while
        # criterion 11 was still unimplemented and kept doing so after criterion
        # 11 landed, which stamped both rows MISSING on evidence that had
        # already been produced.
        self.assertEqual((code, result["outcome"], result["failure"]), (0, "passed", None))
        self.assertEqual(self.top(result, TOP16)["status"], "PASS")
        self.assertEqual(self.top(result, TOP11)["status"], "PASS")
        self.assertEqual({s["id"] for s in result["scenarios"] if "__" not in s["id"]}, {TOP16, TOP11})
        # The phase runs Up, exec and delete several times over; a receipt
        # claiming it started no process would be false.
        self.assertTrue(result["process_starts"])
        self.assertIsNone(result["retained_root"])
        self.assertFalse((self.state_root / "topology").exists())
        # The socket root is owned state too: final-cleanup removes it, or the
        # lane leaks every daemon socket it ever bound.
        self.assertFalse(self.socket_root.exists(), self.socket_root)
        self.assertEqual((result["cleanup_errors"], result["leaks"]), ([], []))
        for name in ("lane-state-root-before-cleanup.txt", "lane-socket-root-before-cleanup.txt"):
            self.assertTrue((evidence / "inventories" / name).is_file(), name)

    def test_final_cleanup_stays_not_implemented_when_a_scenario_is_not(self):
        """The passing outcome is earned by the scenarios, not by the phase.

        With criterion 11 reporting `not_implemented` the phase must go back to
        exit 3 and stamp both its rows MISSING, which is what the removed
        unconditional stamp did for every input. A phase that passes no matter
        what its checks say certifies nothing.
        """
        evidence = self.evidence()

        def unimplemented(ctx, top):
            check = checks.SubCheck(top, "single_environment_safety")
            check.not_implemented = "stand-in for an unimplemented criterion 11"
            return check.finish()

        with mock.patch.object(checks, "check_delete_single_environment_safety", unimplemented):
            code, result = self.run_lane(self.argv("final-cleanup", evidence), evidence)
        self.assertEqual((code, result["outcome"], result["failure"]["reason"]), (3, "failed", "not_implemented"))
        self.assertEqual(self.top(result, TOP16)["status"], "PASS")
        self.assertEqual(self.top(result, TOP11)["status"], "FAIL")
        rows = lanes.account([s for s in self.contract["scenarios"] if s["id"] in (TOP16, TOP11)], [result])["rows"]
        self.assertEqual({row["status"] for row in rows}, {"MISSING"})

    def test_final_cleanup_reports_live_process_as_leak(self):
        root = self.state_root / "topology"
        root.mkdir(parents=True)
        (root / "leftover").write_text("x")
        # The shell must not exec away, or its argv (which names the lane state
        # root) is replaced by the child's and `ps` can no longer attribute it.
        process = subprocess.Popen(["/bin/sh", "-c", f"while :; do sleep 1; done # {root}"], stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not any(pid == process.pid for pid, _ in e2e.processes_referencing(e2e.LaneState(self.state_root, self.release / "bin"))):
                time.sleep(0.05)
            evidence = self.evidence()
            code, result = self.run_lane(self.argv("final-cleanup", evidence), evidence)
        finally:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        self.assertEqual((code, result["failure"]["reason"]), (1, "cleanup"))
        self.assertTrue(any(f"pid {process.pid}" in leak["identifier"] for leak in result["leaks"]))
        self.assertTrue(root.exists(), "survivors must retain the lane state root")

    # -- criterion 19: migration, installation, rollback and uninstall ----------------
    def criterion_19(self, mode: str = "") -> dict:
        """Run the lane in `mode` and return criterion 19's own sub-check."""
        self.set_mode(mode)
        evidence = self.evidence()
        _code, result = self.run_lane(
            self.argv("clean-provision", evidence, only="install_upgrade_rollback_uninstall"), evidence)
        return self.by_slug(result)["install_upgrade_rollback_uninstall"]

    def assert_claims(self, sub: dict, *needles: str) -> None:
        for needle in needles:
            self.assertTrue(any(needle in a and not a.startswith("FAILED:") for a in sub["assertions"]),
                            (needle, sub["assertions"]))

    def assert_fails(self, sub: dict, *needles: str) -> None:
        failures = [a for a in sub["assertions"] if a.startswith("FAILED:")]
        for needle in needles:
            self.assertTrue(any(needle in a for a in failures), (needle, failures))

    def test_the_installed_flow_clauses_of_criterion_19_are_proved(self):
        """Everything but the staged-v0.3.20-daemon clause runs and passes."""
        sub = self.criterion_19()
        self.assertEqual([a for a in sub["assertions"] if a.startswith("FAILED:")], [])
        self.assert_claims(
            sub,
            "the fixture on disk is the one the contract pins",
            "the fixture carries one legacy record of each classification",
            "a clean 0.4 installation into",
            "installed bin/vz is the release's own executable",
            "the installation records the release version",
            "the installer added its PATH entry to the shell rc",
            "migrates it off the legacy schema",
            "exactly one Project, Environment, Machine and WorkspaceBinding",
            "image and resources survived the upgrade",
            "keeps its markers, spec and backend",
            "no legacy Hardened or generic record acquired a Machine",
            "no legacy Hardened or generic record acquired Docker capabilities",
            "no environment_host_imports row",
            "no environment_machine_egress row",
            "retained a completed pre-migration backup",
            "the injected migration failure fails the installed daemon's start",
            "byte-identical to the v0.3.20 fixture",
            "records that it restored the backup",
            "uninstall succeeds",
            "uninstall removed the installed software",
            "survives uninstall",
            "the legacy project directory is byte-identical after uninstall",
            "unrelated Docker configuration is byte-identical after uninstall",
            "uninstall removed its own PATH entry",
        )
        self.assertIn("pinned v0.3.20 daemon", sub["assertions"][-1])

    def test_a_migration_that_widens_a_hardened_record_fails_criterion_19(self):
        """Vacuity: a runtime that gives the Hardened record Developer/Docker/imports/egress."""
        sub = self.criterion_19("migration_widens")
        self.assertEqual(sub["status"], "FAIL")
        self.assert_fails(
            sub,
            "exactly one Project, Environment, Machine and WorkspaceBinding",
            "no legacy Hardened or generic record acquired a Machine",
            "no legacy Hardened or generic record acquired Docker capabilities",
            "no environment_host_imports row",
            "no environment_machine_egress row",
        )
        # The gap must not swallow a real regression: with something failing, the
        # check stays a failure rather than being relabelled not_implemented.
        self.assertFalse(any(a.startswith("not_implemented:") for a in sub["assertions"]), sub["assertions"])

    def test_a_migration_without_a_backup_fails_criterion_19(self):
        """Vacuity: a runtime that migrates without taking the pre-migration backup."""
        sub = self.criterion_19("migration_no_backup")
        self.assertEqual(sub["status"], "FAIL")
        self.assert_fails(
            sub,
            "retained a completed pre-migration backup",
            "byte-identical to the fixture",
            "byte-identical to the v0.3.20 fixture",
            "records that it restored the backup",
        )
        # The upgrade itself still worked, so that claim is still made.
        self.assert_claims(sub, "exactly one Project, Environment, Machine and WorkspaceBinding")

    def test_the_pinned_fixture_is_the_one_the_contract_names(self):
        """The fixture matches its pin and is substantive, offline."""
        contract = common.load_json(common.REPO_ROOT / checks.E2E_CONTRACT)["migration"]
        fixture = common.REPO_ROOT / checks.MIGRATION_FIXTURE
        self.assertEqual(contract["legacy_state_fixture"], checks.MIGRATION_FIXTURE)
        self.assertEqual(contract["legacy_state_fixture_sha256"], common.digest_file(fixture))
        self.assertEqual(checks._schema_version(fixture, immutable=True), "1")
        records = checks._legacy_records(fixture, immutable=True)
        kinds = checks._classify(records)
        self.assertEqual({k: len(v) for k, v in kinds.items()}, {"developer": 1, "hardened": 1, "generic": 1})
        spec = records[kinds["developer"][0]]["spec"]
        self.assertTrue(spec["base_image_ref"] and spec["cpus"] and spec["memory_mb"], spec)

    def test_uninstall_refuses_an_unsafe_prefix_and_keeps_foreign_state(self):
        """The uninstaller removes what vz owns by name and nothing else."""
        installer = common.REPO_ROOT / "scripts/install.sh"
        prefix, home = self.tmp / "uninstall-prefix", self.tmp / "uninstall-home"
        (prefix / "bin").mkdir(parents=True)
        home.mkdir()
        for name in ("vz", "vz-runtimed", "vz-guest-agent", "vz-agent-loader", "vz-macos-setup"):
            (prefix / "bin" / name).write_bytes(b"vz-owned executable\n")
        (prefix / ".installed-version").write_bytes(b"0.4.0-test\n")
        (prefix / "stack-state.db").write_bytes(b"vz-owned state store\n")
        (prefix / "mine.txt").write_bytes(b"not vz's\n")
        (prefix / "bin/my-tool").write_bytes(b"also not vz's\n")
        env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(home), "VZ_INSTALL_DIR": str(prefix)}
        done = subprocess.run(["/bin/bash", str(installer), "--uninstall"], env=env, capture_output=True, timeout=120)
        self.assertEqual(done.returncode, 0, done.stderr.decode())
        self.assertFalse((prefix / "bin/vz").exists())
        self.assertFalse((prefix / "stack-state.db").exists())
        self.assertEqual((prefix / "mine.txt").read_bytes(), b"not vz's\n")
        self.assertEqual((prefix / "bin/my-tool").read_bytes(), b"also not vz's\n")
        # A prefix that is the home directory, or relative, is refused outright.
        for unsafe, message in ((str(home), b"refusing to uninstall from the home directory"),
                                ("relative/prefix", b"must be an absolute path"), ("/", b"refusing to uninstall from /")):
            refused = subprocess.run(["/bin/bash", str(installer), "--uninstall"],
                                     env={**env, "VZ_INSTALL_DIR": unsafe}, capture_output=True, timeout=120)
            self.assertEqual(refused.returncode, 1, (unsafe, refused.stdout.decode()))
            self.assertIn(message, refused.stderr)
    # -- criterion 2: the mixed-profile topology status check -------------------
    #
    # Three failing-before cases, one per claim the check makes that nothing
    # else in the lane makes. Each drives a stand-in that breaks exactly one of
    # them, so a check that asserted key presence instead of values, or that
    # read a health field it never compared, would pass here and must not.

    def assert_criterion_two_regression(self, mode: str, needle: str):
        self.set_mode(mode)
        evidence = self.evidence()
        code, result = self.run_lane(
            self.argv("clean-provision", evidence, only="mixed_profile_topology_status"), evidence)
        self.assertEqual((code, result["outcome"], result["failure"]["reason"]),
                         (1, "failed", "assertion"), mode)
        sub = self.by_slug(result)["mixed_profile_topology_status"]
        self.assertEqual(sub["status"], "FAIL", mode)
        self.assertTrue(any(a.startswith("FAILED: ") and needle in a for a in sub["assertions"]),
                        (mode, sub["assertions"]))
        # A genuine regression must not be dressed up as the criterion's own
        # unexercised macOS clause.
        self.assertFalse(any(a.startswith("not_implemented:") for a in sub["assertions"]), mode)
        self.assertEqual(self.top(result, TOP2)["status"], "FAIL")
        return sub

    def test_a_hardened_machine_holding_docker_fails_criterion_2(self):
        """The criterion's Hardened clause, made falsifiable."""
        sub = self.assert_criterion_two_regression(
            "hardened_docker", "hardened-0 holds no Docker capability at all")
        self.assertTrue(any(a.startswith("FAILED: ") and "omits every Docker context field" in a
                            for a in sub["assertions"]), sub["assertions"])

    def test_a_health_field_that_never_changes_fails_criterion_2(self):
        """Health has to be an observation. A written-in constant reads the same
        on a Ready Machine, so the check stops the Environment and requires the
        reading to follow the state."""
        self.assert_criterion_two_regression(
            "constant_health", "every Machine now reads inactive")

    def test_an_exec_that_guesses_a_machine_fails_criterion_2(self):
        """Fails closed means refused AND not run: a stand-in that silently
        picks the first Machine must break the first of those, not only the
        message."""
        sub = self.assert_criterion_two_regression(
            "ambiguous_exec_runs", "`vz exec` without --machine refuses and runs nothing")
        self.assertTrue(any("the same command with --machine runs (exit 0" in a
                            for a in sub["assertions"]), sub["assertions"])

    def test_the_mixed_profile_definition_is_the_topology_the_criterion_names(self):
        """The definition itself, before any runtime is involved.

        Criterion 2 names a Machine count and a profile mix. A definition that
        quietly dropped the Hardened Machine, or gave it a network it may not
        declare, would make every later assertion true of the wrong topology.
        """
        schema_document = common.load_json(
            common.REPO_ROOT / "schemas/vz-project-definition-v1.schema.json")

        def problems(document):
            return sorted(Draft202012Validator(schema_document).iter_errors(document),
                          key=lambda error: list(map(str, error.absolute_path)))

        definition = checks.mixed_profile_definition(self.release, None)
        machines = {machine["name"]: machine for machine in definition["environment"]["machines"]}
        self.assertEqual(sorted((machine["profile"], machine["target"]["os"])
                                for machine in machines.values()),
                         [("developer", "linux"), ("developer", "linux"), ("hardened", "linux")])
        hardened = [machine for machine in machines.values() if machine["profile"] == "hardened"]
        self.assertNotIn("networks", hardened[0])
        self.assertEqual(problems(definition), [])

        # With a registered macOS target the fourth Machine appears, is a
        # Developer native Machine, and joins the same declared network.
        entry = {"image": "vz-macos", "version": "26.3.1", "channels": ["latest", "xcode"]}
        with_macos = checks.mixed_profile_definition(self.release, entry)
        native = [machine for machine in with_macos["environment"]["machines"]
                  if machine["target"]["os"] == "macos"]
        self.assertEqual(len(native), 1)
        self.assertEqual(native[0]["profile"], "developer")
        self.assertEqual(native[0]["target"]["channel"], checks.MACOS_CHANNEL)
        self.assertEqual(native[0]["networks"], [checks.MIXED_NETWORK])
        self.assertEqual(problems(with_macos), [])

    def test_wrapper_script_runs_the_lane(self):
        evidence = self.evidence()
        script = common.REPO_ROOT / "scripts/run-developer-environment-e2e.sh"
        argv = self.argv("persisted-recovery/pre-sleep", evidence)
        completed = subprocess.run([str(script), *argv], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600, check=False,
                                   env={"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/")})
        # The fake release dir's dummy binaries fail the real codesign verifier only as findings; admission itself succeeds.
        self.assertEqual(completed.returncode, 3, completed.stderr.decode())
        result = common.load_json(evidence / "lane-result.json")
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["entry_point"]["path"], "scripts/run-developer-environment-e2e.sh")
        rejected = subprocess.run([str(script), "--suite", "lifecycle"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=600, check=False)
        self.assertEqual(rejected.returncode, 2)

    # -- criterion 17 -------------------------------------------------------------------
    def test_a_silently_multi_attached_block_volume_fails_criterion_17(self):
        """The vacuity test for `workspace_storage_policy`.

        `leaky_multi_attach` makes the stand-in admit a writable block volume on
        two Machines and write state on the way. Both halves of the criterion's
        clause must be caught: the refusal that did not happen, and the mutation
        that did. A check that asserted only "up exited non-zero" would pass on
        the first half of this input, and one that asserted only field presence
        would pass on all of it.
        """
        result = self.assert_regression("leaky_multi_attach", "workspace_storage_policy",
                                        "a writable block volume on two Machines is refused")
        sub = self.by_slug(result)["workspace_storage_policy"]
        failures = [a for a in sub["assertions"] if a.startswith("FAILED:")]
        self.assertTrue(any("vz up exit 0" in a for a in failures), failures)
        # The ordering half, which is the part a weaker check would miss: the
        # admitted declaration went on to allocate storage and persist an
        # Environment, and both are reported rather than only the absent
        # refusal. A check that stopped at the exit code would report one of
        # these four failures and none of the mutations.
        self.assertTrue(any("allocated storage under" in a for a in failures), failures)
        self.assertTrue(any("no Environment was persisted by the refused Up" in a for a in failures), failures)
        self.assertEqual(len([a for a in failures if "refusal names the volume" in a]), 1, failures)
        # Nothing beyond the refusal ran, so the file-semantics assertions are
        # absent rather than passing on an Environment that should not exist.
        self.assertFalse(any("read_write projection" in a for a in sub["assertions"]), sub["assertions"])

    def test_criterion_17_proves_each_projection_mode_by_its_own_host_side_evidence(self):
        """The three modes must be told apart, not merely mounted.

        This reads the passing run's own assertions back. `read_write` is only
        proved by the HOST file carrying the Machine's bytes, `read_only` by the
        write failing AND the host file surviving, and `snapshot` by the write
        succeeding while the host file is unchanged. If the check ever collapsed
        into "all three mounted", these disappear.
        """
        evidence = self.evidence()
        _code, result = self.run_lane(self.argv("clean-provision", evidence), evidence)
        sub = self.by_slug(result)["workspace_storage_policy"]
        self.assertEqual(sub["status"], "PASS", sub["assertions"])
        assertions = sub["assertions"]

        def stated(needle):
            self.assertTrue(any(needle in a for a in assertions), (needle, assertions))

        stated("the read_write projection is the worktree itself")
        stated("a write into a read_only projection fails")
        stated("the read_only source is byte-identical after the refused write")
        stated("creating a file in a read_only projection fails too")
        stated("machine-2 may write into its own snapshot")
        stated("the snapshot source is byte-identical on the host")
        stated("a writable block volume on two Machines is refused")
        stated("the refusal names the volume and both Machines")
        stated("the worktree across the refused Up unchanged")
        stated("no volume storage was allocated for the refused declaration")
        stated("no Environment was persisted by the refused Up")
        stated("the Machine the block volume is NOT attached to cannot read it")
        stated("observed every concurrent write within the declared")
        stated("out of the shared cache, not a")
        # Both Machines are asserted about by name, so a fixture that checked
        # one side twice would not satisfy this.
        for machine in ("machine-0", "machine-1"):
            self.assertTrue(
                any(f"{machine} observed every concurrent write" in a for a in assertions),
                (machine, assertions))

    def test_criterion_17_declares_its_own_staleness_bound_and_polls_to_it(self):
        """The fixture's deadline must come from the definition it declared.

        A bound restated in the check could drift from the one the definition
        carries, and the fixture would then be polling to a number nothing
        promised.
        """
        definition = checks.storage_definition(
            self.release, block_attachments=[checks.block_attachment("machine-0", "read_write")])
        cache = next(v for v in definition["environment"]["volumes"] if v["kind"] == "shared_cache")
        self.assertEqual(cache["consistency"],
                         {"model": "bounded_staleness",
                          "staleness_bound_millis": checks.STALENESS_BOUND_MILLIS})
        self.assertEqual({a["machine"] for a in cache["attachments"]}, {"machine-0", "machine-1"})
        self.assertTrue(all(a["mode"] == "read_write" for a in cache["attachments"]))
        # The refused definition differs from the accepted one in exactly one
        # respect: the block volume's attachment set. Anything else and the
        # refusal would not be evidence about multi-attach.
        refused = checks.storage_definition(
            self.release,
            block_attachments=[checks.block_attachment("machine-0", "read_write"),
                               checks.block_attachment("machine-1", "read_only")])
        accepted_block = next(v for v in definition["environment"]["volumes"] if v["kind"] == "block")
        refused_block = next(v for v in refused["environment"]["volumes"] if v["kind"] == "block")
        self.assertEqual(refused["environment"]["machines"], definition["environment"]["machines"])
        self.assertEqual({k: v for k, v in refused_block.items() if k != "attachments"},
                         {k: v for k, v in accepted_block.items() if k != "attachments"})
        self.assertEqual(len(accepted_block["attachments"]), 1)
        self.assertEqual(len(refused_block["attachments"]), 2)

    def test_criterion_17_declares_all_three_projection_modes_once_each(self):
        definition = checks.storage_definition(
            self.release, block_attachments=[checks.block_attachment("machine-0", "read_write")])
        modes = [m["workspace"]["mode"] for m in definition["environment"]["machines"]]
        self.assertEqual(sorted(modes), ["read_only", "read_write", "snapshot"])
        # A Machine carries at most one projection, so three modes need three
        # Machines; a definition with fewer could not exercise all three.
        self.assertEqual(len(definition["environment"]["machines"]), 3)
    # -- criterion 6 ------------------------------------------------------------------
    #
    # Every test below runs the whole `clean-provision` phase, so the sub-check
    # is exercised through the path the gate runs it through rather than called
    # directly with arguments a caller chose.


    def ingress(self, mode: str = ""):
        self.set_mode(mode)
        evidence = self.evidence()
        code, result = self.run_lane(
            self.argv("clean-provision", evidence, only=INGRESS_SLUG), evidence)
        return code, result, self.by_slug(result)[INGRESS_SLUG]

    def test_the_edge_is_proved_and_only_its_unimplemented_clauses_are_reported(self):
        code, result, sub = self.ingress()
        failures = [line for line in sub["assertions"] if line.startswith("FAILED:")]
        self.assertEqual(failures, [], failures)
        # Still not PASS, and deliberately: criterion 6 also names controlled
        # egress, host imports/exports and fault controls, and reporting PASS
        # would certify it on evidence that never touched those.
        self.assertEqual(sub["status"], "FAIL")
        unexercised = [line for line in sub["assertions"] if line.startswith("not_implemented:")]
        self.assertEqual(len(unexercised), 1, sub["assertions"])
        for named in ("controlled-egress", "host-import", "fault", "Offline"):
            self.assertIn(named, unexercised[0])
        # The clauses that used to be reported unexercised are now exercised,
        # so they must not still be named as missing. A report that kept
        # claiming them would hide the fact that they now run for real.
        for retired in ("BusyBox", "ssl_client", "no-check-certificate"):
            self.assertNotIn(retired, unexercised[0])
        # The lane's outcome is not_implemented rather than an assertion
        # failure, so a real regression in this check stays distinguishable
        # from the clause it cannot reach.
        self.assertEqual((code, result["failure"]["reason"]), (3, "not_implemented"))
        # And what it did prove is present as values, not as field presence.
        for claim in ("resolves to the edge inside the Environment",
                      "never resolves to the origin Machine",
                      "no static /etc/hosts entry for the published name",
                      "resolves through its Environment alone",
                      "does not resolve in the other Environment",
                      "an undeclared name in the same Environment does not resolve",
                      "no listener on the host LAN or a wildcard address appeared",
                      "the published authority is a certificate and carries no key",
                      "carries the Developer image's HTTPS client",
                      "the two Environments minted different authorities",
                      "spoken to directly, the origin reports the caller as its peer",
                      "answers over verified TLS from inside a Machine",
                      "the TLS session terminated at the edge, not at the origin",
                      "verified against the Environment's own published authority",
                      "the response body came from the declared origin Machine",
                      "the origin's peer on the ingress path is the edge",
                      "the client's own address never reached the origin",
                      "REFUSED against the image's pinned public CA bundle",
                      "REFUSED against the OTHER Environment's authority"):
            self.assertTrue(any(claim in line for line in sub["assertions"]),
                            (claim, sub["assertions"]))

    def assert_broken(self, mode: str, needle: str):
        code, result, sub = self.ingress(mode)
        self.assertEqual(sub["status"], "FAIL", mode)
        self.assertTrue(any(line.startswith("FAILED:") and needle in line for line in sub["assertions"]),
                        (mode, sub["assertions"]))
        # A broken clause is an assertion failure, never the not_implemented
        # report: the two must not be able to stand in for one another.
        self.assertEqual((code, result["outcome"], result["failure"]["reason"]),
                         (1, "failed", "assertion"), mode)
        self.assertEqual(self.top(result, e2e.CRITERION_6)["status"], "FAIL")
        return sub

    def test_a_name_that_resolves_to_the_machine_behind_the_edge_fails_the_criterion(self):
        """The one distinction criterion 6 exists to make.

        A published name answering with the origin's own address is a private
        shortcut wearing a public name: the client would reach the Machine
        directly, and the TLS, ingress, firewall and translation clauses would
        all be bypassed while every other observable stayed identical.
        """
        sub = self.assert_broken("edge_shortcut", "the declared `.test` name resolves to the edge")
        self.assertTrue(any(line.startswith("FAILED:") and "never resolves to the origin" in line
                            for line in sub["assertions"]), sub["assertions"])

    def test_a_published_name_in_the_static_hosts_table_fails_the_criterion(self):
        """If the name is in `/etc/hosts` the resolver is never asked.

        Every DNS assertion would then pass with no resolver in the Environment
        at all, which is exactly the way a check passes for the wrong reason.
        """
        self.assert_broken("edge_hosts_shortcut", "no static /etc/hosts entry for the published name")

    def test_a_machine_left_pointing_at_public_resolvers_fails_the_criterion(self):
        """`vz.dns` on the cmdline is not the same claim as the running resolver.

        A Machine booted with the edge named on its cmdline but still running
        with the image's public resolvers would resolve nothing local and leave
        the fabric for every lookup, while the cmdline said otherwise.
        """
        self.assert_broken("edge_public_resolver", "resolves through its Environment alone")

    def test_a_client_that_does_not_verify_the_chain_fails_the_criterion(self):
        """A TLS clause proved by a client with verification off proves nothing.

        This is the failure the whole client exists to remove: a handshake that
        completes against any certificate cannot tell this Environment's
        authority from any other, so it says nothing about who the client
        reached. The check catches it by making the SAME request against the
        image's pinned public bundle and requiring a refusal; a client that
        skipped verification would be answered there too.
        """
        sub = self.assert_broken("edge_tls_unverified",
                                 "REFUSED against the image's pinned public CA bundle")
        # The positive fetch still succeeds in this mode, which is the point:
        # only the negative distinguishes a verifying client from a credulous
        # one, so only the negative may fail here.
        self.assertTrue(any("answers over verified TLS from inside a Machine" in line
                            and not line.startswith("FAILED:") for line in sub["assertions"]),
                        sub["assertions"])

    def test_a_client_that_accepts_any_named_authority_fails_the_criterion(self):
        """Trusting a file is not the same as trusting the right file.

        An Environment's authority is per Environment. A client that accepted
        whatever anchor it was handed would let one Environment's Machines
        verify another Environment's edge, and the isolation the criterion asks
        for would exist only in the naming.
        """
        self.assert_broken("edge_foreign_anchor_accepted",
                           "REFUSED against the OTHER Environment's authority")

    def test_an_origin_that_sees_the_client_as_its_peer_fails_the_criterion(self):
        """No translation means no edge in the path, whatever TLS reported.

        If the origin's own `REMOTE_ADDR` is the client, the connection reached
        it directly and the edge terminated nothing; every other observable --
        the name, the certificate, the body -- would look identical.
        """
        sub = self.assert_broken("edge_origin_shortcut",
                                 "the origin's peer on the ingress path is the edge")
        self.assertTrue(any(line.startswith("FAILED:")
                            and "client's own address never reached the origin" in line
                            for line in sub["assertions"]), sub["assertions"])
    # -- criterion 7: host import and export boundaries -----------------------------------
    def host_boundary_sub(self, mode: str = ""):
        """Run clean-provision under `mode` and return (result, criterion-7 sub-check)."""
        if mode:
            self.set_mode(mode)
        evidence = self.evidence()
        _code, result = self.run_lane(
            self.argv("clean-provision", evidence, only="host_import_export_boundaries"), evidence)
        return result, self.by_slug(result)["host_import_export_boundaries"]

    def assert_asserted(self, sub: dict, needle: str, mode: str):
        """The sub-check FAILED for a stated reason, not for want of running."""
        self.assertEqual(sub["status"], "FAIL", mode)
        failures = [a for a in sub["assertions"] if a.startswith("FAILED: ")]
        self.assertTrue(any(needle in a for a in failures), (mode, failures))

    def test_host_boundaries_prove_every_clause_they_can_and_name_the_one_they_cannot(self):
        """The conformant run: the positive holds, every denial holds, and the
        one clause this runtime cannot offer is named rather than skipped."""
        result, sub = self.host_boundary_sub()
        self.assertEqual(result["failure"]["reason"], "not_implemented")
        self.assertEqual(sub["status"], "FAIL")
        self.assertEqual([a for a in sub["assertions"] if a.startswith("FAILED: ")], [])
        not_implemented = [a for a in sub["assertions"] if a.startswith("not_implemented:")]
        self.assertEqual(len(not_implemented), 1, sub["assertions"])
        self.assertIn("enabled egress", not_implemented[0])
        assertions = "\n".join(sub["assertions"])
        # Every clause of the criterion, each named in the evidence it produced.
        for needle in (
            "an Environment that declares no import cannot reach",
            "the authorized Machine reaches the 127.0.0.1-only host service",
            "the undeclared guest port",
            "a UDP datagram to the declared guest port is not served",
            "the sibling Machine in the SAME Environment",
            "a Machine in a sibling Environment cannot reach the granted port",
            "cannot choose a host destination",
            "a NAT alias or LAN address is not the grant",
            "declares offline egress, and its declared import served anyway",
            "the declared loopback export serves the Machine's own service",
            "is loopback and nothing else",
            "no wildcard or LAN host listener holds any port this check declared",
            "an import's guest loopback port has no host listener at all",
            "declaring the export host port already held is refused",
        ):
            self.assertIn(needle, assertions, needle)

    def test_a_guest_that_can_choose_any_host_port_fails_criterion_7(self):
        """Vacuity: make the guest relay any loopback port instead of only its
        declared grants. The positive still passes, so the failure is the
        denials and nothing else."""
        _result, sub = self.host_boundary_sub("import_any_port")
        self.assert_asserted(sub, "cannot choose a host destination", "import_any_port")
        failures = "\n".join(a for a in sub["assertions"] if a.startswith("FAILED: "))
        # Both shapes: the host port of its own declared service, and a host
        # service nothing ever declared to it.
        self.assertIn("own-host-port", failures)
        self.assertIn("undeclared-host-service", failures)
        # The positive still held, so the failure is the denials and not the
        # fixture: a mode that also broke the granted import would prove nothing.
        self.assertIn("the authorized Machine reaches the 127.0.0.1-only host service",
                      "\n".join(a for a in sub["assertions"] if not a.startswith("FAILED: ")))

    def test_a_grant_honoured_on_the_wrong_machine_fails_criterion_7(self):
        """Vacuity: make every Machine honour machine-0's grants. Only the
        wrong-Machine and sibling-Environment denials may break."""
        _result, sub = self.host_boundary_sub("import_any_machine")
        self.assert_asserted(sub, "the sibling Machine in the SAME Environment", "import_any_machine")

    def test_a_wildcard_export_listener_fails_criterion_7(self):
        """Vacuity: bind the export on 0.0.0.0. The export still serves on
        loopback, so only the listener evidence may catch it -- which is the
        whole reason that clause reads real listeners instead of asserting the
        bind address from the code that chose it."""
        _result, sub = self.host_boundary_sub("export_wildcard")
        self.assert_asserted(sub, "is loopback and nothing else", "export_wildcard")


# One `#[derive(...)]`-preceded struct body out of the Rust source, as
# (all serialized fields, the `skip_serializing_if` subset). The parser is
# deliberately literal: it refuses a `flatten`, `rename` or unconditional `skip`
# attribute rather than silently reporting a field set the wire never carries.
def _rust_serialized_fields(source: str, name: str) -> tuple[set, set]:
    marker = "struct " + name + " {"
    assert marker in source, "no struct " + name
    body = source.split(marker, 1)[1].split("\n}", 1)[0]
    for hostile in ("serde(flatten", "serde(rename", "serde(skip)", "serde(skip_serializing)"):
        assert hostile not in body, name + " carries " + hostile + ", which this comparison cannot model"
    fields, optional = set(), set()
    skipped_next = False
    for line in body.splitlines():
        line = line.strip()
        if line.startswith("#["):
            skipped_next = skipped_next or "skip_serializing_if" in line
            continue
        if not line or line.startswith("//"):
            continue
        field = line.split(":", 1)[0].strip()
        if not field.isidentifier():
            continue
        fields.add(field)
        if skipped_next:
            optional.add(field)
        skipped_next = False
    assert fields, "could not read " + name + " fields"
    return fields, optional


class StatusFieldSetAgreementTests(unittest.TestCase):
    """The harness's declared status field sets must be the CLI's actual ones.

    `check_status_field_set` compares an observed `vz status --json` payload
    against constants in `developer_environment_checks`. Those constants are the
    only thing that says what the document is supposed to contain, so a field
    added to the Rust structs and not to them leaves the gate check passing on a
    document it no longer fully describes -- and, for a `skip_serializing_if`
    field, passing without ever having seen it. Each set is therefore read out of
    `dev_status.rs` here rather than restated, so that drift fails offline.
    """

    SOURCE = "crates/vz-cli/src/commands/dev_status.rs"

    def setUp(self):
        self.source = (common.REPO_ROOT / self.SOURCE).read_text()

    def test_declared_field_sets_match_the_status_command_structs(self):
        for struct, declared, optional in (
                ("StatusOutput", checks.STATUS_FIELDS, checks.STATUS_OPTIONAL_FIELDS),
                ("EnvironmentStatus", checks.ENVIRONMENT_FIELDS, checks.ENVIRONMENT_OPTIONAL_FIELDS),
                ("MachineStatus", checks.MACHINE_FIELDS, checks.MACHINE_OPTIONAL_FIELDS),
                ("NetworkStatus", checks.NETWORK_FIELDS, checks.NETWORK_OPTIONAL_FIELDS),
                ("NetworkAttachmentStatus", checks.ATTACHMENT_FIELDS, checks.ATTACHMENT_OPTIONAL_FIELDS),
                ("EndpointStatus", checks.ENDPOINT_FIELDS, checks.ENDPOINT_OPTIONAL_FIELDS)):
            with self.subTest(struct=struct):
                fields, skipped = _rust_serialized_fields(self.source, struct)
                self.assertEqual(fields, declared)
                self.assertEqual(skipped, optional)
                self.assertLessEqual(optional, declared)

    def test_the_parser_would_notice_an_added_or_newly_optional_field(self):
        """Failing-before, in process: neither comparison above is vacuous."""
        drifted = self.source.replace(
            "    machine_id: String,\n    name: String,",
            "    machine_id: String,\n    topology: Vec<String>,\n    name: String,", 1)
        self.assertNotEqual(drifted, self.source)
        fields, _ = _rust_serialized_fields(drifted, "MachineStatus")
        self.assertEqual(fields - checks.MACHINE_FIELDS, {"topology"})
        # Anchored on the field *pair*, because `machine_id: String,` now also
        # opens `NetworkAttachmentStatus` and appears in `EndpointStatus`: a
        # single-field anchor would silently drift a different struct and this
        # falsification would stop testing what it names.
        newly_optional = self.source.replace(
            "    machine_id: String,\n    name: String,",
            '    #[serde(skip_serializing_if = "String::is_empty")]\n'
            "    machine_id: String,\n    name: String,", 1)
        _, skipped = _rust_serialized_fields(newly_optional, "MachineStatus")
        self.assertEqual(skipped - checks.MACHINE_OPTIONAL_FIELDS, {"machine_id"})


if __name__ == "__main__":
    unittest.main()


class CriterionFiveCrossingTests(unittest.TestCase):
    """Criterion 5's macOS clause must not be certified by Linux-only evidence.

    `check_private_topology_paths` proved a Linux-to-Linux private path and
    reported PASS, while GOAL-0.4.0.md:172-174 also requires that "at least one
    required service path crosses between a Linux Machine and a native macOS
    Machine in both directions permitted by its declarations". Nothing in the
    check ever built a macOS Machine, so the criterion's row could go green on
    evidence that never touched half of what it claims.
    """

    SCHEMA = common.REPO_ROOT / "schemas/vz-project-definition-v1.schema.json"

    @classmethod
    def setUpClass(cls):
        cls.schema = common.load_json(cls.SCHEMA)

    def _release(self, root: Path, macos: bool):
        """A release dir carrying only the catalog these definitions read."""
        release = root / "release"
        release.mkdir(parents=True, exist_ok=True)
        # The macOS entry is the shape `vz-macos-setup` actually registers:
        # image/version/channels naming a template bundle, with no `profile`
        # and no `digest`. An earlier fixture invented those two fields and the
        # helper agreed with the fixture rather than with a real catalog.
        catalog = {"linux": [{"profile": "developer", "image": "vz-linux",
                              "digest": "sha256:" + "a" * 64}],
                   "macos": ([{"image": "vz-macos", "version": "26.3.1",
                               "channels": ["latest", "xcode"]}] if macos else [])}
        (release / "machine-target-catalog.json").write_text(json.dumps(catalog))
        return release

    def test_no_macos_target_is_reported_rather_than_assumed(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(checks.macos_target(self._release(Path(tmp), macos=False)))

    def test_a_registered_developer_macos_target_is_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            entry = checks.macos_target(self._release(Path(tmp), macos=True))
            self.assertIsNotNone(entry)
            self.assertEqual(entry["image"], "vz-macos")

    def test_the_crossing_definition_validates_against_the_shipped_schema(self):
        # The declaration half of criterion 5: a Developer macOS Machine on a
        # declared private network, with an endpoint on each side.
        with tempfile.TemporaryDirectory() as tmp:
            release = self._release(Path(tmp), macos=True)
            definition = checks.crossing_definition(release, checks.macos_target(release))
            problems = sorted(Draft202012Validator(self.schema).iter_errors(definition),
                              key=lambda e: list(map(str, e.absolute_path)))
            self.assertEqual(problems, [], problems[0].message if problems else "")
            machines = definition["environment"]["machines"]
            self.assertEqual([m["target"]["os"] for m in machines], ["linux", "linux", "macos"])
            # Declared in both directions: an endpoint on the Linux side and on
            # the macOS side of the same network.
            endpoints = {e["machine"] for e in definition["environment"]["endpoints"]}
            self.assertEqual(endpoints, {"machine-0", "machine-mac"})
            self.assertIn(checks.PRIVATE_NETWORK, machines[2]["networks"])

    def test_that_definition_would_have_been_refused_before_macos_joined_the_fabric(self):
        # Falsifiability: the schema capped a macOS Machine's `networks` at zero
        # and forced `egress: offline`. Restore that and the same definition must
        # be refused, so the test above is evidence the schema change landed and
        # not merely that jsonschema accepts anything.
        schema = json.loads(json.dumps(self.schema))
        conditionals = schema["$defs"]["machine"]["allOf"]
        restored = False
        for rule in conditionals:
            branches = rule.get("if", {}).get("anyOf")
            then = rule.get("then", {}).get("properties", {})
            if not branches or "networks" not in then:
                continue
            for branch in branches:
                spec = branch.get("properties", {}).get("target", {}).get("properties", {}).get("os")
                if isinstance(spec, dict) and (spec.get("const") == "windows" or spec.get("enum") == ["windows"]):
                    spec.pop("const", None)
                    spec["enum"] = ["macos", "windows"]
                    restored = True
        self.assertTrue(restored, "the network conditional no longer has the shape this test restores")
        with tempfile.TemporaryDirectory() as tmp:
            release = self._release(Path(tmp), macos=True)
            definition = checks.crossing_definition(release, checks.macos_target(release))
            problems = list(Draft202012Validator(schema).iter_errors(definition))
            self.assertTrue(problems, "the pre-change schema accepted a macOS Machine on a network")

    # Real `ifconfig -a` output from the macOS guest of the 2026-09-09 crossing
    # run, trimmed to the interfaces that matter. Kept verbatim rather than
    # idealised: the parser has to survive what the guest actually prints.
    GUEST_IFCONFIG = (
        b"lo0: flags=8049<UP,LOOPBACK,RUNNING,MULTICAST> mtu 16384\n"
        b"\tinet 127.0.0.1 netmask 0xff000000\n"
        b"gif0: flags=8010<POINTOPOINT,MULTICAST> mtu 1280\n"
        b"anpi0: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n"
        b"\tether 0e:2b:40:0e:a7:84\n"
        b"en1: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n"
        b"\tether 72:38:8c:71:14:00\n"
        b"\tinet 10.85.187.215 netmask 0xffffff00 broadcast 10.85.187.255\n"
        b"en2: flags=8822<BROADCAST,SMART,SIMPLEX,MULTICAST> mtu 1500\n"
        b"\tether 0e:2b:40:0e:a7:64\n")

    def test_the_macos_fabric_port_is_found_by_address_not_by_name(self):
        port = checks.macos_fabric_port(self.GUEST_IFCONFIG, "10.85.187")
        self.assertEqual(port, ("en1", "72:38:8c:71:14:00", "10.85.187.215"))

    def test_loopback_and_addressless_interfaces_are_not_mistaken_for_the_port(self):
        # lo0 carries an inet and anpi0/en2 carry an ether; neither is the
        # fabric NIC. A parser that took the first interface with any address,
        # or the first with a MAC, would pick one of them.
        self.assertIsNone(checks.macos_fabric_port(self.GUEST_IFCONFIG, "127.0.0"))
        port = checks.macos_fabric_port(self.GUEST_IFCONFIG, "10.85.187")
        self.assertNotIn(port[0], ("lo0", "anpi0", "en2", "gif0"))

    def test_a_guest_with_no_address_on_the_subnet_reports_none(self):
        # The failure this must not paper over: the NIC exists but
        # `native_macos::fabric` never applied an address to it.
        without = self.GUEST_IFCONFIG.replace(
            b"\tinet 10.85.187.215 netmask 0xffffff00 broadcast 10.85.187.255\n", b"")
        self.assertIsNone(checks.macos_fabric_port(without, "10.85.187"))

    def test_an_address_on_a_different_fabric_is_not_accepted(self):
        # Two Environments derive different subnets. Matching on "some inet"
        # would let a Machine on a foreign fabric satisfy this Environment.
        self.assertIsNone(checks.macos_fabric_port(self.GUEST_IFCONFIG, "10.85.99"))

class PeerAddressTests(unittest.TestCase):
    """The origin's peer, compared as an address rather than as a spelling.

    BusyBox `httpd` accepts on an IPv6 socket, so an IPv4 peer reaches CGI as a
    bracketed IPv4-mapped literal. Unwrapping that is what lets the translation
    clauses compare against the address the host derived -- and it must stay
    exact, because a lenient normaliser would let a comparison succeed against
    something that is not the address at all.
    """

    def test_the_mapped_literal_busybox_writes_is_the_address_it_names(self):
        for written in ("[::ffff:10.31.71.1]", "::ffff:10.31.71.1",
                        "[::FFFF:10.31.71.1]", "10.31.71.1"):
            with self.subTest(written=written):
                self.assertEqual(checks.peer_address(written), "10.31.71.1")

    def test_anything_that_is_not_an_address_is_returned_untouched(self):
        # Returned as-is rather than coerced, so a comparison against the
        # address the host derived still fails instead of being made to pass.
        for written in ("", "::1", "[::1]", "fda6:1594:ff6b::", "10.31.71",
                        "10.31.71.1.5", "[::ffff:not.an.address]", "10.31.71.999"):
            with self.subTest(written=written):
                self.assertEqual(checks.peer_address(written), written)
        self.assertIsNone(checks.peer_address(None))

    def test_a_mapped_literal_never_collapses_two_different_addresses(self):
        """The clause it serves is `origin peer == edge` and `!= client`."""
        self.assertNotEqual(checks.peer_address("[::ffff:10.31.71.1]"),
                            checks.peer_address("[::ffff:10.31.71.200]"))


TOP18 = e2e.CRITERION_18


class CriterionEighteenTests(unittest.TestCase):
    """Criterion 18's two sub-checks, each claim broken on purpose.

    These call the checks directly against a stand-in release that DOES
    implement SecretBindings and capability negotiation. The lane's own
    `FAKE_VZ` models neither -- neither exists in the shipped definition schema
    or the runtime contract -- so against it both sub-checks report
    `not_implemented`, which is the last test here and is what keeps the
    post-wake phase honest. Everything above it exists so that verdict is not
    the only thing these checks can produce: for every assertion the check makes
    there is a mode in which the stand-in produces the wrong value, and the
    check has to report FAIL.
    """

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="vztl-c18-", dir="/private/tmp"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.mode_file = self.tmp / "mode"
        self.release = support.build_secret_release(self.tmp / "release", mode_file=self.mode_file)
        self.state = recorder.LaneState(self.tmp / "state", self.release / "bin")
        self.state.create()
        self.addCleanup(shutil.rmtree, self.state.socket_root, ignore_errors=True)
        self.contexts = 0

    def context(self, *, mode="", secret_status="DEV", snapshot_status="PLANNED", schema_secrets=True,
                release=None, state=None, repo_root=None):
        self.mode_file.write_text(mode)
        self.contexts += 1
        if repo_root is None:
            repo_root = support.build_secret_repo_root(self.tmp / f"repo-{self.contexts}",
                                                       secret_status=secret_status,
                                                       snapshot_status=snapshot_status,
                                                       schema_secrets=schema_secrets)
        evidence = self.tmp / f"evidence-{self.contexts}"
        evidence.mkdir()
        state = state or self.state
        return checks.CheckContext(repo_root=repo_root, release_dir=release or self.release, state=state,
                                   recorder=recorder.Recorder(evidence, RUN_ID), evidence_dir=evidence,
                                   cli_removal={})

    def secrets(self, **kwargs):
        return checks.check_secret_bindings_scoped_redacted(self.context(**kwargs), TOP18).scenario()

    def snapshot(self, **kwargs):
        return checks.check_snapshot_restore_capability(self.context(**kwargs), TOP18).scenario()

    @staticmethod
    def failures(scenario):
        return [line for line in scenario["assertions"] if line.startswith("FAILED: ")]

    @staticmethod
    def unproved(scenario):
        return [line for line in scenario["assertions"] if line.startswith("not_implemented:")]

    def assert_broken(self, scenario, needle):
        """FAIL for a stated reason, not for want of having run."""
        self.assertEqual(scenario["status"], "FAIL", scenario["assertions"])
        failures = self.failures(scenario)
        self.assertTrue(any(needle in line for line in failures), (needle, failures))
        return failures

    # -- the conformant runtime: every claim of the secrets half holds ---------------
    def test_the_conformant_runtime_proves_every_clause_of_the_secrets_half(self):
        scenario = self.secrets()
        self.assertEqual(self.failures(scenario), [])
        self.assertEqual(self.unproved(scenario), [])
        self.assertEqual(scenario["status"], "PASS")
        assertions = "\n".join(scenario["assertions"])
        for needle in (
            "advertises secret_bindings as 'DEV'",
            "declares environment.secret_bindings",
            "finds the sentinel in a control buffer",
            "machine-0 reads the binding at /run/vz-secrets/gate-secret",
            "machine-1, the sibling in the SAME Environment",
            "a Machine in a sibling Environment cannot read the binding's path",
            "fails CLOSED with a structured error",
            "secret_binding_used record names exactly this Environment, Machine and binding",
            "carries the binding's identity and not its value",
            "the sweep covers every declared artifact group",
            "the secret value occurs 0 times across",
        ):
            self.assertIn(needle, assertions, needle)

    def test_the_sweep_reads_every_declared_artifact_group_and_says_how_much(self):
        """The sweep must be non-empty: zero occurrences of nothing is not redaction."""
        scenario = self.secrets()
        covered = [line for line in scenario["assertions"] if "the sweep covers every declared artifact group" in line]
        self.assertEqual(len(covered), 1, scenario["assertions"])
        for group in checks.SWEEP_GROUPS:
            self.assertIn(group, covered[0], group)
        import re

        read = [line for line in scenario["assertions"] if line.startswith("the sweep read ")]
        self.assertEqual(len(read), 1, scenario["assertions"])
        count, total = re.search(r"the sweep read (\d+) artifacts totalling (\d+) bytes", read[0]).groups()
        self.assertGreaterEqual(int(count), len(checks.SWEEP_GROUPS))
        self.assertGreater(int(total), 0)

    def test_an_artifact_the_sweep_cannot_read_is_reported_not_passed_over(self):
        """A file the sweep skipped is a hole in the claim, not a clean result."""
        readable = self.tmp / "readable.txt"
        readable.write_bytes(b"plain bytes")
        missing = self.tmp / "absent.txt"
        skipped = []
        rows = checks.sweep_group("state-root", [readable, missing], skipped)
        self.assertEqual([(label, path, data) for label, path, data in rows],
                         [("state-root", readable, b"plain bytes")])
        self.assertEqual(len(skipped), 1, skipped)
        self.assertIn(str(missing), skipped[0])
        # And an over-bound file is refused by the same path rather than read
        # short, which would make "the value is not in this file" a guess.
        oversized = self.tmp / "oversized.txt"
        oversized.write_bytes(b"x" * 64)
        skipped = []
        with mock.patch.object(checks, "read_regular", side_effect=common.GateError("exceeds byte bound")):
            self.assertEqual(checks.sweep_group("evidence", [oversized], skipped), [])
        self.assertEqual(len(skipped), 1, skipped)
        self.assertIn("exceeds byte bound", skipped[0])

    def test_a_swept_artifact_that_cannot_be_read_fails_the_redaction_claim(self):
        """The runtime is conformant; only the daemon log is unreadable. The
        check must not report zero occurrences of a file it never opened."""
        real = checks.read_regular

        def refuse_the_daemon_log(path, *args, **kwargs):
            if Path(path).name == "d.log":
                raise common.GateError(f"{path}: exceeds byte bound 2147483648")
            return real(path, *args, **kwargs)

        with mock.patch.object(checks, "read_regular", side_effect=refuse_the_daemon_log):
            scenario = self.secrets()
        failures = self.assert_broken(scenario, "artifact(s) could not be read, so the value was not looked for")
        self.assertTrue(any("d.log" in line for line in failures), failures)

    def test_the_detector_finds_the_exact_bytes_and_nothing_else(self):
        sentinel = "vzsec-" + "a" * 64
        artifacts = [("one", Path("/x/a"), b"before " + sentinel.encode() + b" after " + sentinel.encode()),
                     ("two", Path("/x/b"), b"nothing here"),
                     ("three", Path("/x/c"), sentinel.upper().encode())]
        self.assertEqual(checks.secret_occurrences(artifacts, sentinel), [("one", "/x/a", 2)])

    # -- the declaration surface ----------------------------------------------------
    def test_the_shipped_schema_declares_no_binding_so_the_criterion_is_reported_unproved(self):
        """The real repository's verdict: honest, and never a vacuous PASS."""
        scenario = self.secrets(schema_secrets=False, secret_status="PLANNED")
        self.assertEqual(self.failures(scenario), [])
        self.assertEqual(scenario["status"], "FAIL")
        unproved = self.unproved(scenario)
        self.assertEqual(len(unproved), 1, scenario["assertions"])
        self.assertIn("no SecretBinding can be declared", unproved[0])
        self.assertIn("No SecretBinding type exists in vz-runtime-contract.", unproved[0])
        self.assertIn("Nothing was planted", unproved[0])

    def test_an_advertised_binding_with_no_declaration_surface_fails(self):
        """The distinction the criterion draws: advertised-but-absent is a FAILURE,
        not the same honest gap as a capability nothing advertises."""
        scenario = self.secrets(schema_secrets=False, secret_status="DEV")
        self.assert_broken(scenario, "declares no way to bind one")
        self.assertEqual(self.unproved(scenario), [])

    # -- scope ----------------------------------------------------------------------
    def test_a_sibling_machine_in_the_same_environment_that_can_read_it_fails(self):
        scenario = self.secrets(mode="sibling_machine_reads")
        failures = self.assert_broken(scenario, "machine-1, the sibling in the SAME Environment")
        # The holder still read it, so the failure is scope and not the fixture.
        self.assertIn("machine-0 reads the binding at", "\n".join(
            line for line in scenario["assertions"] if not line.startswith("FAILED: ")))
        self.assertTrue(any("status '0'" in line for line in failures), failures)

    def test_a_sibling_environment_that_can_read_the_path_fails(self):
        scenario = self.secrets(mode="foreign_env_reads")
        self.assert_broken(scenario, "a Machine in a sibling Environment cannot read the binding's path")

    # -- cross-boundary denial ------------------------------------------------------
    def test_a_cross_boundary_request_served_an_empty_result_fails(self):
        """Fail-closed means refused. Coming up with the binding quietly dropped
        is the exact shape the criterion forbids."""
        scenario = self.secrets(mode="cross_env_empty")
        failures = self.assert_broken(scenario, "fails CLOSED with a structured error")
        self.assertTrue(any("exit 0" in line for line in failures), failures)

    def test_a_cross_boundary_refusal_without_a_machine_readable_code_fails(self):
        scenario = self.secrets(mode="cross_env_unstructured")
        failures = self.assert_broken(scenario, "fails CLOSED with a structured error")
        self.assertTrue(any("error.code None" in line for line in failures), failures)

    # -- audit ----------------------------------------------------------------------
    def test_a_use_that_records_nothing_fails(self):
        scenario = self.secrets(mode="no_audit")
        self.assert_broken(scenario, "wrote an audit log at")

    def test_an_audit_record_naming_the_wrong_machine_fails(self):
        scenario = self.secrets(mode="audit_wrong_machine")
        failures = self.assert_broken(scenario, "record names exactly this Environment, Machine and binding")
        self.assertTrue(any("observed [" in line and "expected [" in line for line in failures), failures)

    def test_an_audit_record_carrying_the_value_fails_twice(self):
        """The audit claim and the redaction claim overlap on purpose: a record
        that names the value breaks both, and both must say so."""
        scenario = self.secrets(mode="audit_leaks_value")
        failures = self.assert_broken(scenario, "carries the binding's identity and not its value")
        self.assertTrue(any("the secret value leaked" in line and "audit-log" in line for line in failures),
                        failures)

    # -- redaction, one swept artifact at a time -------------------------------------
    def assert_leaked_in(self, mode: str, group: str):
        scenario = self.secrets(mode=mode)
        failures = self.assert_broken(scenario, "the secret value leaked")
        leaked = [line for line in failures if "the secret value leaked" in line][0]
        self.assertIn(f"({group})", leaked)
        self.assertRegex(leaked, r"observed \d+ in ")
        return leaked

    def test_the_value_in_the_status_json_is_found(self):
        self.assert_leaked_in("leak_status_json", "status-json")

    def test_the_value_in_the_human_status_is_found(self):
        self.assert_leaked_in("leak_status_human", "status-human")

    def test_the_value_in_the_daemon_log_is_found(self):
        self.assert_leaked_in("leak_daemon_log", "daemon-log")

    def test_the_value_in_the_evidence_directory_is_found(self):
        """A runtime that prints the value writes it into this lane's receipts."""
        self.assert_leaked_in("leak_exec_stderr", "evidence")

    def test_the_value_in_the_state_root_is_found(self):
        self.assert_leaked_in("leak_state_root", "state-root")

    # -- snapshot: the branch the matrix does NOT advertise ---------------------------
    def test_an_unadvertised_capability_must_come_back_explicitly_unsupported(self):
        scenario = self.snapshot(snapshot_status="PLANNED")
        self.assertEqual(self.failures(scenario), [])
        self.assertEqual(self.unproved(scenario), [])
        self.assertEqual(scenario["status"], "PASS")
        assertions = "\n".join(scenario["assertions"])
        self.assertIn("advertises snapshot as 'PLANNED'", assertions)
        self.assertIn("the Machine's request is projected exactly as declared", assertions)
        self.assertIn("EXPLICIT unsupported capability", assertions)

    def test_a_requested_capability_that_is_neither_granted_nor_accounted_fails(self):
        """Silence is the failure mode this clause exists for."""
        scenario = self.snapshot(mode="snapshot_silent", snapshot_status="PLANNED")
        failures = self.assert_broken(scenario, "EXPLICIT unsupported capability")
        self.assertTrue(any("= None" in line for line in failures), failures)

    def test_granting_a_capability_the_matrix_does_not_advertise_fails(self):
        scenario = self.snapshot(mode="snapshot_granted", snapshot_status="PLANNED")
        self.assert_broken(scenario, "does not advertise snapshot (PLANNED), and the Machine did not negotiate it")

    def test_a_structured_up_refusal_naming_the_capability_is_explicit_enough(self):
        scenario = self.snapshot(mode="snapshot_refuse", snapshot_status="PLANNED")
        self.assertEqual(self.failures(scenario), [])
        self.assertEqual(scenario["status"], "PASS")
        assertions = "\n".join(scenario["assertions"])
        self.assertIn("error.code 'unsupported_capability'", assertions)
        self.assertIn("names the 'snapshot' capability", assertions)

    def test_a_generic_up_refusal_is_not_an_explicit_unsupported_capability(self):
        scenario = self.snapshot(mode="snapshot_refuse_generic", snapshot_status="PLANNED")
        self.assert_broken(scenario, "the refusal is a structured error envelope")

    def test_a_request_the_runtime_never_projects_is_reported_unproved(self):
        """No echoed request means nothing to be explicit about. Reported with
        both sets, never passed over: whether the CLI republishes a declared
        request is criterion 15's claim, and this says so."""
        scenario = self.snapshot(mode="drop_request", snapshot_status="PLANNED")
        self.assertEqual(self.failures(scenario), [])
        unproved = self.unproved(scenario)
        self.assertEqual(len(unproved), 1, scenario["assertions"])
        self.assertIn("declared ['posix_exec', 'snapshot'], reported ['posix_exec']", unproved[0])
        self.assertIn("criterion 15", unproved[0])

    # -- snapshot: the branch the matrix DOES advertise -------------------------------
    def test_an_advertised_capability_round_trips_a_sentinel_written_between_them(self):
        scenario = self.snapshot(mode="snapshot_granted", snapshot_status="DEV")
        self.assertEqual(self.failures(scenario), [])
        self.assertEqual(scenario["status"], "PASS")
        assertions = "\n".join(scenario["assertions"])
        self.assertIn("snapshot returns an identity", assertions)
        self.assertIn("a sentinel is written between snapshot and restore", assertions)
        self.assertIn("restore rewound the Machine past the sentinel", assertions)

    def test_an_advertised_capability_the_runtime_does_not_provide_fails(self):
        """Advertised-but-absent is a FAILURE, never the honest gap."""
        scenario = self.snapshot(mode="snapshot_silent", snapshot_status="DEV")
        self.assert_broken(scenario, "advertises snapshot as DEV")
        self.assertEqual(self.unproved(scenario), [])

    def test_a_restore_that_rewinds_nothing_fails(self):
        scenario = self.snapshot(mode="snapshot_granted+restore_noop", snapshot_status="DEV")
        self.assert_broken(scenario, "restore rewound the Machine past the sentinel")

    # -- the shipped inputs, against the lane's own fake --------------------------------
    def test_the_shipped_inputs_report_the_criterion_unproved_without_a_single_failure(self):
        """What the post-wake phase actually produces today.

        The real capability matrix, the real definition schema and the lane's
        own `FAKE_VZ` -- which models neither SecretBindings nor capability
        negotiation, exactly as the shipped runtime does not. Both sub-checks
        must report `not_implemented` with ZERO failed assertions, because a
        failed assertion here would turn the phase's `not_implemented` outcome
        into `assertion` and claim a regression that is not there.
        """
        release = support.build_fake_release(self.tmp / "lane-release", mode_file=self.tmp / "lane-mode")
        self.addCleanup(fixtures.make_writable, release)
        state = recorder.LaneState(self.tmp / "lane-state", release / "bin")
        state.create()
        self.addCleanup(shutil.rmtree, state.socket_root, ignore_errors=True)
        secrets = checks.check_secret_bindings_scoped_redacted(
            self.context(release=release, state=state, repo_root=common.REPO_ROOT), TOP18).scenario()
        self.assertEqual(self.failures(secrets), [])
        self.assertEqual(len(self.unproved(secrets)), 1, secrets["assertions"])
        self.assertIn("no SecretBinding can be declared", self.unproved(secrets)[0])
        snapshot = checks.check_snapshot_restore_capability(
            self.context(release=release, state=state, repo_root=common.REPO_ROOT), TOP18).scenario()
        self.assertEqual(self.failures(snapshot), [])
        self.assertEqual(len(self.unproved(snapshot)), 1, snapshot["assertions"])
        self.assertIn("does not project the Machine's declared capability request", self.unproved(snapshot)[0])


class CriterionEighteenWiringTests(unittest.TestCase):
    """The contract assigns criterion 18 to this phase, and the lane answers it."""

    def test_the_post_wake_phase_answers_the_scenario_the_contract_assigns_it(self):
        contract = contract_module.load_contract()
        assigned = [entry for entry in contract["scenarios"]
                    if entry["lane"] == "topology" and entry["phase"] == "persisted-recovery/post-wake"
                    and entry["id"] == TOP18]
        self.assertEqual(len(assigned), 1, [entry["id"] for entry in contract["scenarios"]])
        self.assertEqual(assigned[0]["criterion"], 18)
        source = (common.REPO_ROOT / "scripts/helpers/developer_environment_e2e.py").read_text()
        body = source.split("def run_post_wake", 1)[1].split("def run_final_cleanup", 1)[0]
        self.assertIn("CRITERION_18: []", body)
        self.assertIn("check_secret_bindings_scoped_redacted(ctx, CRITERION_18)", body)
        self.assertIn("check_snapshot_restore_capability(ctx, CRITERION_18)", body)
