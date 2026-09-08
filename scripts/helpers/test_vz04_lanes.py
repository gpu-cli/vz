from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_common as common  # noqa: E402
import vz04_contract as contract_module  # noqa: E402
import vz04_lanes as lanes  # noqa: E402
import vz04_schema as schema  # noqa: E402

DIGEST = "a" * 64


def _ctx(root, **overrides):
    options = dict(tmux="/usr/bin/true",
                   acquired_inputs={"registry-archive": "/acquired/registry.tar",
                                    "registry-layout": "/acquired/layout",
                                    "ssh-packages": "/acquired/ssh-inputs"})
    options.update(overrides)
    return lanes.LaneContext(run_id="gate-test-run-1", release_dir=root, release_dir_sha256=DIGEST, state_root=root / "state",
                             contract_path=root / "c.json", contract_sha256=DIGEST, candidate_tuple_sha256=DIGEST, fixture_sha256=DIGEST,
                             clients={"docker": None, "compose_plugin": None, "buildx_plugin": None}, **options)


def _passed(lane, phase, ids, ctx):
    result = lanes.base_result(lane, phase, ctx, {"path": "scripts/x.sh", "sha256": DIGEST, "argv": []})
    result["outcome"] = "passed"
    result["scenarios"] = [{"id": i, "status": "PASS", "started_unix_ns": 1, "ended_unix_ns": 2, "assertions": ["ok"], "evidence": [],
                            "readiness_polls": []} for i in ids]
    return result


class LaneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="vz04-lanes-")
        self.root = Path(self.tmp.name).resolve()
        self.ctx = _ctx(self.root)
        self.contract = contract_module.load_contract()
        self.lanes = contract_module.lane_by_name(self.contract)

    def tearDown(self):
        self.tmp.cleanup()

    def test_not_implemented_result_is_schema_valid(self):
        result = lanes.failed_result("topology", "clean-provision", self.ctx, {"path": "scripts/x.sh", "sha256": DIGEST, "argv": []},
                                     "not_implemented", "stub", 3)
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["outcome"], "failed")

    def test_dry_invocation_writes_not_implemented_without_running(self):
        directory = self.root / "topology" / "clean-provision"
        result = lanes.invoke_lane(self.lanes["topology"], "clean-provision", self.ctx, directory, dry=True)
        self.assertEqual(result["failure"]["reason"], "not_implemented")
        self.assertEqual(common.load_json(directory / "lane-result.json"), result)
        self.assertFalse((directory / "lane.stdout").exists())

    def test_lane_argv_carries_everything(self):
        argv = lanes.lane_argv(self.lanes["topology"], self.ctx, "final-cleanup", self.root / "e", "/handoff.json")
        for flag in ("--suite", "--run-id", "--phase", "--release-dir", "--evidence-dir", "--state-root", "--contract",
                     "--candidate-tuple", "--fixture-sha256", "--handoff", "--docker", "--compose-plugin", "--buildx-plugin"):
            self.assertIn(flag, argv)
        sandbox = lanes.lane_argv(self.lanes["sandbox-vm"], self.ctx, "clean-provision", self.root / "e", None)
        self.assertEqual(sandbox[:4], ["--suite", "all", "--profile", "release"])
        self.assertIn("--output-dir", sandbox)

    def test_the_docker_lane_receives_the_release_inputs_its_harness_requires(self):
        """The contract gives this lane `["--suite", "all"]`, and its harness
        rejects that: a composed run needs the release version, both guest
        bundles and the BuildKit archive, and resolves `vz`/`vz-runtimed`
        directly under `--release-dir`. All four are facts about the candidate
        the gate was pointed at, so the lane reads them off it.
        """
        release = fixtures.build_fake_release_dir(self.root / "release", with_lane_inputs=True)
        try:
            ctx = _ctx(self.root)
            ctx.release_dir = release
            argv = lanes.lane_argv(self.lanes["linux-docker"], ctx, "clean-provision", self.root / "e", None)
            flags = dict(zip(argv, argv[1:]))
            self.assertEqual(flags["--release-dir"], str(release / "bin"),
                             "the harness resolves vz and vz-runtimed directly under --release-dir")
            self.assertEqual(flags["--release-version"], "0.4.0-faketest")
            self.assertEqual(flags["--developer-bundle"], str(release / "linux" / "developer"))
            self.assertEqual(flags["--hardened-bundle"], str(release / "linux" / "container"))
            self.assertEqual(flags["--buildkit-archive"],
                             str(release / "buildkit" / "vz-buildkit-v0.19.0-linux-arm64.tar"))
            # Other lanes are untouched and still receive the candidate root.
            topology = lanes.lane_argv(self.lanes["topology"], ctx, "clean-provision", self.root / "e2", None)
            self.assertEqual(dict(zip(topology, topology[1:]))["--release-dir"], str(release))
            for flag in ("--release-version", "--developer-bundle", "--hardened-bundle", "--buildkit-archive"):
                self.assertNotIn(flag, topology)
        finally:
            fixtures.make_writable(release)

    def test_a_candidate_missing_a_lane_input_is_named_not_guessed(self):
        """A candidate that cannot supply an input must say which one. Deriving
        these from the candidate is only safe if a candidate that lacks them
        fails loudly rather than passing an empty or invented path along."""
        release = fixtures.build_fake_release_dir(self.root / "release", with_lane_inputs=True)
        fixtures.make_writable(release)
        ctx = _ctx(self.root)
        ctx.release_dir = release
        for removed, expected in ((release / "linux" / "developer", "missing developer-bundle"),
                                  (release / "linux" / "container", "missing hardened-bundle"),
                                  (release / "buildkit" / "vz-buildkit-v0.19.0-linux-arm64.tar",
                                   "exactly one BuildKit archive required, found 0"),
                                  (release / "release-manifest.json", "manifest missing")):
            backup = removed.with_name(removed.name + ".moved")
            removed.rename(backup)
            try:
                with self.assertRaisesRegex(Exception, expected):
                    lanes.lane_argv(self.lanes["linux-docker"], ctx, "clean-provision", self.root / "e", None)
            finally:
                backup.rename(removed)
        # With everything back it resolves again, so each failure was the removal.
        lanes.lane_argv(self.lanes["linux-docker"], ctx, "clean-provision", self.root / "e", None)

    def test_the_docker_harness_admits_the_lane_argv(self):
        """`vz-ao8` closed. The contract gives this lane `["--suite", "all"]`,
        which the harness rejected until eight further options were supplied:
        five derived from the candidate, `--tmux` resolved by the gate, and
        three acquired from checked-in pins. This feeds the lane's own argv to
        the harness's own admission and requires it to be accepted outright.

        Assembling the argv is what this asserts; whether the acquired paths
        hold real bytes is `linux_docker_ssh_input.verify`'s job and the
        acquisition helpers' own tests.
        """
        import linux_docker_e2e as harness
        release = fixtures.build_fake_release_dir(self.root / "release", with_lane_inputs=True)
        try:
            ctx = _ctx(self.root)
            ctx.release_dir = release
            ctx.clients = {"docker": "/d", "compose_plugin": "/c", "buildx_plugin": "/b"}
            argv = lanes.lane_argv(self.lanes["linux-docker"], ctx, "clean-provision", self.root / "e", None)
            admitted = harness.arguments(list(argv))
            self.assertEqual(admitted.suite, "all")
            self.assertEqual(admitted.release_version, "0.4.0-faketest")
            self.assertEqual(admitted.tmux, "/usr/bin/true")
            self.assertEqual(admitted.ssh_packages, "/acquired/ssh-inputs")
            self.assertEqual(admitted.registry_archive, "/acquired/registry.tar")
            self.assertEqual(admitted.developer_bundle, str(release / "linux" / "developer"))
        finally:
            fixtures.make_writable(release)

    def test_the_composed_docker_lane_requires_the_terminal_it_drives(self):
        """The lifecycle suite inside a composed run drives tmux, so the gate
        resolves it like the Docker clients and passes the path. A contract
        naming a host path would be wrong; a lane silently running without it
        would be worse."""
        release = fixtures.build_fake_release_dir(self.root / "release", with_lane_inputs=True)
        try:
            ctx = _ctx(self.root, tmux="/usr/bin/true")
            ctx.release_dir = release
            argv = lanes.lane_argv(self.lanes["linux-docker"], ctx, "clean-provision", self.root / "e", None)
            self.assertEqual(dict(zip(argv, argv[1:]))["--tmux"], "/usr/bin/true")
            # Other lanes neither need it nor receive it.
            topology = lanes.lane_argv(self.lanes["topology"], ctx, "clean-provision", self.root / "e2", None)
            self.assertNotIn("--tmux", topology)
            without = _ctx(self.root, tmux=None)
            without.release_dir = release
            with self.assertRaisesRegex(Exception, "requires --tmux"):
                lanes.lane_argv(self.lanes["linux-docker"], without, "clean-provision", self.root / "e3", None)
        finally:
            fixtures.make_writable(release)

    def test_the_composed_docker_lane_requires_its_acquired_inputs(self):
        """The registry archive and layout are run-frozen inputs the candidate
        does not carry. The gate acquires them from the checked-in pin; a lane
        that did not receive them must refuse rather than invent a path."""
        release = fixtures.build_fake_release_dir(self.root / "release", with_lane_inputs=True)
        try:
            ctx = _ctx(self.root)
            ctx.release_dir = release
            argv = lanes.lane_argv(self.lanes["linux-docker"], ctx, "clean-provision", self.root / "e", None)
            flags = dict(zip(argv, argv[1:]))
            self.assertEqual(flags["--registry-archive"], "/acquired/registry.tar")
            self.assertEqual(flags["--registry-layout"], "/acquired/layout")
            self.assertEqual(flags["--ssh-packages"], "/acquired/ssh-inputs")
            for missing in ("registry-archive", "registry-layout", "ssh-packages"):
                partial = _ctx(self.root, acquired_inputs={k: v for k, v in ctx.acquired_inputs.items()
                                                           if k != missing})
                partial.release_dir = release
                with self.assertRaisesRegex(Exception, f"requires an acquired --{missing}"):
                    lanes.lane_argv(self.lanes["linux-docker"], partial, "clean-provision", self.root / "e2", None)
            # Other lanes neither need them nor receive them.
            topology = lanes.lane_argv(self.lanes["topology"], ctx, "clean-provision", self.root / "e3", None)
            for flag in ("--registry-archive", "--registry-layout", "--ssh-packages"):
                self.assertNotIn(flag, topology)
        finally:
            fixtures.make_writable(release)

    def test_a_dry_lane_still_substitutes_when_its_argv_cannot_be_built(self):
        """A dry run substitutes the lane without starting it, so an argv it
        could not have built must not change that verdict — it is recorded as
        the reason and the lane stays `not_implemented`."""
        release = fixtures.build_fake_release_dir(self.root / "release")
        try:
            ctx = _ctx(self.root, tmux=None)
            ctx.release_dir = release
            directory = self.root / "linux-docker" / "dry"
            result = lanes.invoke_lane(self.lanes["linux-docker"], "clean-provision", ctx, directory, dry=True)
            self.assertEqual(result["failure"]["reason"], "not_implemented")
            self.assertIn("argv could not be built", result["failure"]["detail"])
            self.assertEqual(result["entry_point"]["argv"], [])
            self.assertEqual(schema.validate("lane-result", result), [])
        finally:
            fixtures.make_writable(release)

    def test_a_candidate_that_cannot_supply_a_lane_input_is_accounted_not_crashed(self):
        """The gate must survive a deficient candidate. A lane whose argv cannot
        be built records `input_rejected` with the reason, so the aggregate
        still counts it rather than the run dying while building a command."""
        release = fixtures.build_fake_release_dir(self.root / "release")
        try:
            ctx = _ctx(self.root)
            ctx.release_dir = release
            directory = self.root / "linux-docker" / "clean-provision"
            result = lanes.invoke_lane(self.lanes["linux-docker"], "clean-provision", ctx, directory)
            self.assertEqual(result["outcome"], "failed")
            self.assertEqual(result["failure"]["reason"], "input_rejected")
            self.assertIn("BuildKit archive", result["failure"]["detail"])
            self.assertEqual(schema.validate("lane-result", result), [])
            self.assertEqual(common.load_json(directory / "lane-result.json"), result)
            self.assertFalse((directory / "lane.stdout").exists(), "nothing may be run")
        finally:
            fixtures.make_writable(release)

    def test_stub_script_writes_valid_result_and_exits_3(self):
        """The native-macOS lane is still a stub: it must account for itself rather
        than be absent. The topology lane is a real lane and is covered by
        `test_developer_environment_e2e`."""
        release = fixtures.build_fake_release_dir(self.root / "release")
        try:
            evidence = self.root / "evidence"
            evidence.mkdir()
            script = common.REPO_ROOT / "scripts/run-macos-developer-environment-e2e.sh"
            argv = lanes.lane_argv(self.lanes["native-macos"], lanes.LaneContext(
                run_id="gate-test-run-1", release_dir=release, release_dir_sha256=common.tree_digest(release), state_root=self.root / "state",
                contract_path=common.REPO_ROOT / common.CONFIG_FILES["e2e_contract"], contract_sha256=DIGEST, candidate_tuple_sha256=DIGEST,
                fixture_sha256=DIGEST, clients={}), "clean-provision", evidence, None)
            completed = subprocess.run([str(script), *argv], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300, check=False)
            self.assertEqual(completed.returncode, 3, completed.stderr.decode())
            result = common.load_json(evidence / "lane-result.json")
            self.assertEqual(schema.validate("lane-result", result), [])
            self.assertEqual((result["lane"], result["failure"]["reason"], result["failure"]["exit_code"]),
                             ("native-macos", "not_implemented", 3))
            self.assertEqual(result["entry_point"]["path"], "scripts/run-macos-developer-environment-e2e.sh")
            rejected = subprocess.run([str(script), "--suite", "lifecycle"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300, check=False)
            self.assertEqual(rejected.returncode, 2)
        finally:
            fixtures.make_writable(release)

    def test_sandbox_summary_translation(self):
        run_dir = self.root / "run"
        run_dir.mkdir()
        (run_dir / "summary.txt").write_text("passed=runtime stack\nfailed=none\nother=x\n")
        (run_dir / "run-info.txt").write_text("host=x\n")
        evidence = self.root / "evidence-pass"
        evidence.mkdir()
        entry = {"path": "scripts/run-sandbox-vm-e2e.sh", "sha256": DIGEST, "argv": []}
        result = lanes.translate_sandbox_summary("sandbox-vm", "clean-provision", self.ctx, entry, run_dir, 0, evidence)
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual(result["outcome"], "passed")
        self.assertEqual(result["result_adapter"]["passed"], ["runtime", "stack"])
        self.assertTrue((evidence / "sandbox-summary.txt").exists())
        (run_dir / "summary.txt").write_text("passed=runtime\nfailed=stack:1\n")
        evidence = self.root / "evidence-fail"
        evidence.mkdir()
        result = lanes.translate_sandbox_summary("sandbox-vm", "clean-provision", self.ctx, entry, run_dir, 1, evidence)
        self.assertEqual(schema.validate("lane-result", result), [])
        self.assertEqual((result["outcome"], result["failure"]["reason"]), ("failed", "assertion"))

    def test_accounting_rules(self):
        required = [{"id": "gate.a.b", "lane": "topology", "phase": "clean-provision"},
                    {"id": "gate.c.d", "lane": "topology", "phase": "clean-provision"},
                    {"id": "docker.e.f", "lane": "linux-docker", "phase": "final-cleanup"},
                    {"id": "gate.g.h", "lane": "native-macos", "phase": "clean-provision"}]
        topology = _passed("topology", "clean-provision", ["gate.a.b", "gate.x.y"], self.ctx)
        docker = lanes.failed_result("linux-docker", "final-cleanup", self.ctx, {"path": "s", "sha256": DIGEST, "argv": []}, "not_implemented", "d")
        outcome = lanes.account(required, [topology, docker])
        status = {row["id"]: (row["status"], row["reason"]) for row in outcome["rows"]}
        self.assertEqual(status["gate.a.b"], ("PASS", None))
        self.assertEqual(status["gate.c.d"], ("MISSING", "not_reported"))
        self.assertEqual(status["docker.e.f"], ("MISSING", "not_implemented"))
        self.assertEqual(status["gate.g.h"], ("MISSING", "lane_result_absent"))
        codes = {code for code, _s, _d in outcome["findings"]}
        self.assertEqual(codes, {"scenario.unknown", "scenario.missing"})
        duplicate = _passed("linux-docker", "final-cleanup", ["gate.a.b", "docker.e.f"], self.ctx)
        outcome = lanes.account(required, [topology, duplicate])
        codes = {code for code, _s, _d in outcome["findings"]}
        self.assertIn("scenario.duplicate", codes)
        self.assertIn("scenario.misassigned", codes)


if __name__ == "__main__":
    unittest.main()


class RunInputTests(unittest.TestCase):
    """The gate's run-frozen inputs come from their checked-in pins.

    Nothing here reaches the network: the acquisition helpers are exercised by
    their own tests. What matters at this level is that the cache is keyed by
    the pin that produced it, so changing a pin can never reuse the artifact of
    the previous one.
    """

    def test_a_cache_entry_is_named_by_its_pin_digest(self):
        import vz04_run_inputs as run_inputs
        root = Path("/cache")
        first = run_inputs.cache_entry(root, "registry", "a" * 64)
        second = run_inputs.cache_entry(root, "registry", "b" * 64)
        self.assertNotEqual(first, second)
        self.assertEqual(first, run_inputs.cache_entry(root, "registry", "a" * 64))
        self.assertTrue(first.name.startswith("registry-"))
        for bad in ("", "a" * 63, "z" * 64, "A" * 64):
            with self.assertRaises(Exception):
                run_inputs.cache_entry(root, "registry", bad)

    def test_every_run_frozen_input_is_offered(self):
        """All three come from checked-in pins, so a fresh checkout plus the
        pins reproduces them. Nothing may be offered that was not produced:
        handing the lane a path to something nothing acquired would let it
        run on bytes no one verified."""
        import vz04_run_inputs as run_inputs
        with tempfile.TemporaryDirectory() as cache:
            with unittest.mock.patch.object(run_inputs, "registry_inputs",
                                            return_value={"registry-archive": Path(cache),
                                                          "registry-layout": Path(cache)}), \
                    unittest.mock.patch.object(run_inputs, "ssh_inputs",
                                               return_value={"ssh-packages": Path(cache)}):
                acquired = run_inputs.acquired_inputs(common.REPO_ROOT, Path(cache))
        self.assertEqual(sorted(acquired), ["registry-archive", "registry-layout", "ssh-packages"])

    def test_an_input_that_vanished_after_acquisition_is_refused(self):
        import vz04_run_inputs as run_inputs
        with tempfile.TemporaryDirectory() as cache:
            with unittest.mock.patch.object(run_inputs, "registry_inputs",
                                            return_value={"registry-archive": Path(cache) / "gone"}), \
                    unittest.mock.patch.object(run_inputs, "ssh_inputs", return_value={}):
                with self.assertRaisesRegex(Exception, "missing after acquisition"):
                    run_inputs.acquired_inputs(common.REPO_ROOT, Path(cache))
