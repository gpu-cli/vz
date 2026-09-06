"""Offline rejection tests; these do not constitute physical cgroup evidence."""
import base64
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import linux_docker_buildkit_cgroup as cgroup


PID = 321
ROOT = "/docker/" + "a" * 64


def fixture():
    stat = f"{PID} (docker-init) S " + " ".join(["1"] * 18 + ["98765"] + ["0"] * 30) + "\n"
    values = dict.fromkeys(cgroup.FIELDS, "")
    values.update(before_stat=stat, before_membership="0::" + ROOT + "/init\n",
                  before_namespace="cgroup:[4002]\n", guest_namespace="cgroup:[4001]\n",
                  observer_namespace="cgroup:[4001]\n",
                  before_mountinfo=f"55 24 0:29 {ROOT} /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n",
                  before_process_root="42:600\n", before_root_inode="29:700\n", before_init_inode="29:701\n",
                  root_fs="63677270\n", init_fs="63677270\n", root_type="domain\n", init_type="domain\n",
                  root_controllers="cpu io memory pids\n", root_subtree="cpu io memory pids\n",
                  init_controllers="cpu io memory pids\n", init_subtree="\n", init_procs=f"{PID}\n322\n")
    for name in ("stat", "membership", "namespace", "mountinfo", "process_root", "root_inode", "init_inode"):
        values["after_" + name] = values["before_" + name]
    return values


def pack(values):
    return ("VZ_BUILDKIT_CGROUP_V1\n" + "".join(
        field + "=" + base64.b64encode(values[field].encode()).decode() + "\n" for field in cgroup.FIELDS) +
        "VZ_BUILDKIT_CGROUP_END\n").encode()


def inspected():
    return {"Id": "a" * 64, "HostConfig": {"CgroupnsMode": "private"}, "State": {"Running": True, "Pid": PID}}


class CgroupTests(unittest.TestCase):
    def capture_fixture(self):
        temporary = tempfile.TemporaryDirectory(prefix="vz-cgroup-owned-project-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        project, evidence = root / "build", root / "evidence"
        project.mkdir(mode=0o700); evidence.mkdir(mode=0o700)
        descriptor = {"owner": {"project_id": "project-exact", "environment_id": "env-exact",
                                 "machine_id": "machine-exact"},
                      "incarnation_id": "incarnation-exact", "incarnation_generation": 1}
        definition = {"schema_version": 1, "project_id": "project-exact"}
        topology = {"project": str(project),
                    "primary": {"project_id": "project-exact", "environment_id": "env-exact", "state": "ready",
                                "machines": [{"machine_id": "machine-exact", "state": "ready",
                                              "incarnation_id": "incarnation-exact", "incarnation_generation": 1,
                                              "docker_context": copy.deepcopy(descriptor)}]},
                    "neighbor": {"project_id": "project-exact", "environment_id": "env-other", "state": "ready",
                                 "machines": []}}
        for path, value in ((project / "vz.json", definition), (evidence / "topology.json", topology)):
            path.write_text(json.dumps(value)); path.chmod(0o600)

        class Harness:
            cli = "/private/owned/bin/vz"

            def __init__(self):
                self.root, self.evidence = root, evidence
                self.calls = []
                self.output = (pack(fixture()), b"", 0)
                self.after_dispatch = lambda: None

            def command(self, label, argv, **kwargs):
                self.calls.append((label, argv, kwargs))
                self.after_dispatch()
                return self.output

        return Harness(), descriptor, project

    def assert_bad(self, **changes):
        values = fixture()
        values.update(changes)
        with self.assertRaises(ValueError):
            cgroup.validate(pack(values), PID)

    def test_complete_external_guest_pid_proof(self):
        proof = cgroup.validate(pack(fixture()), PID)
        self.assertEqual(proof["root_path"], ROOT)
        self.assertEqual(proof["init_path"], ROOT + "/init")
        self.assertEqual(proof["init_pids"], [PID, 322])
        self.assertEqual(proof["process"]["starttime_ticks"], 98765)
        self.assertEqual(proof["root_pids"], [])

    def test_process_counters_may_change_but_birth_identity_cannot(self):
        values = fixture()
        values["after_stat"] = values["after_stat"].replace(" S 1", " R 2", 1)
        cgroup.validate(pack(values), PID)
        self.assert_bad(after_stat=fixture()["after_stat"].replace("98765", "98766"))
        self.assert_bad(before_stat=fixture()["before_stat"].replace(" S ", " Z "))
        self.assert_bad(before_stat=fixture()["before_stat"].replace(str(PID), "999", 1))
        for pid in (True, 0, 1, -1, "321", 2**31):
            with self.assertRaises(ValueError):
                cgroup.validate(pack(fixture()), pid)

    def test_private_namespace_and_stable_identity_required(self):
        self.assert_bad(guest_namespace="cgroup:[4002]\n")
        self.assert_bad(observer_namespace="cgroup:[4002]\n")
        self.assert_bad(observer_namespace="cgroup:[4003]\n")
        for key in ("after_namespace", "after_membership", "after_mountinfo", "after_process_root",
                    "after_root_inode", "after_init_inode"):
            self.assert_bad(**{key: fixture()[key] + "changed"})
        self.assert_bad(before_namespace="cgroup:[0]\n", after_namespace="cgroup:[0]\n")

    def test_canonical_owned_membership_and_projection_required(self):
        for path in ("/init", "/docker/../foreign/init", "//docker/x/init", "/docker/x//init", "/docker/x/leaf",
                     "/docker/x/init (deleted)", "/docker/x/init\n0::/other/init"):
            self.assert_bad(before_membership="0::" + path + "\n", after_membership="0::" + path + "\n")
        for replacement in (ROOT + "-foreign", "/", ROOT + "/init"):
            mount = fixture()["before_mountinfo"].replace(ROOT, replacement)
            self.assert_bad(before_mountinfo=mount, after_mountinfo=mount)

    def test_mount_type_access_ambiguity_and_inode(self):
        for mount in (fixture()["before_mountinfo"] * 2,
                      fixture()["before_mountinfo"].replace("cgroup2", "tmpfs"),
                      fixture()["before_mountinfo"].replace("rw,nosuid", "ro,nosuid"),
                      fixture()["before_mountinfo"].replace(" - ", " "), ""):
            self.assert_bad(before_mountinfo=mount, after_mountinfo=mount)
        self.assert_bad(root_fs="1021994\n")
        self.assert_bad(init_fs="1021994\n")
        self.assert_bad(before_init_inode="29:700\n", after_init_inode="29:700\n")
        self.assert_bad(before_init_inode="30:701\n", after_init_inode="30:701\n")

    def test_domain_initialization_and_controller_sets_required(self):
        for value in ("domain invalid\n", "domain threaded\n", "threaded\n", "domain", ""):
            self.assert_bad(root_type=value)
            self.assert_bad(init_type=value)
        for value in ("\n", "cpu pids\n", "cpu io memory pids pids\n", "cpu\tmemory\n"):
            self.assert_bad(root_subtree=value)
        self.assert_bad(root_controllers="\n", root_subtree="\n", init_controllers="\n")
        self.assert_bad(init_subtree="cpu\n")
        self.assert_bad(init_controllers="cpu pids\n")

    def test_external_pid_membership_not_namespace_local_pid(self):
        self.assert_bad(root_procs=f"{PID}\n")
        for value in ("1\n2\n", "", "0\n", "321", "321\n-2\n", "321\n2147483648\n"):
            self.assert_bad(init_procs=value)
        values = fixture()
        values["init_procs"] = f"322\n{PID}\n{PID}\n"
        self.assertEqual(cgroup.validate(pack(values), PID)["init_pids"], [PID, 322])

    def test_strict_framing_encoding_size_and_truncation(self):
        raw = pack(fixture())
        for altered in (raw[:-1], raw + b"extra\n", raw.replace(b"root_type=", b"init_type="),
                        raw.replace(b"before_stat=", b"before_stat=!"), raw.replace(b"\n", b"\r\n", 1),
                        b"x" * (cgroup.STREAM_LIMIT + 1)):
            with self.assertRaises(ValueError):
                cgroup.validate(altered, PID)
        self.assert_bad(before_mountinfo="x" * (cgroup.LIMIT + 1))
        self.assert_bad(root_type="domain\x00\n")

    def test_script_is_read_only_bounded_and_fixed_pid(self):
        script = cgroup.probe_script(PID)
        self.assertIn("p=/proc/321\n", script)
        self.assertIn("head -c " + str(cgroup.LIMIT + 1), script)
        self.assertIn("set -o pipefail", script)
        for prohibited in ("docker", "nsenter", "sleep", "mkdir", "kill", ">/", "> /", "tee "):
            self.assertNotIn(prohibited, script)
        with self.assertRaises(ValueError):
            cgroup.probe_script("321; touch /bad")
        parsed = subprocess.run(["/bin/sh", "-n"], input=script.encode(), capture_output=True, check=False)
        self.assertEqual((parsed.returncode, parsed.stdout, parsed.stderr), (0, b"", b""))

    def test_capture_records_explicit_public_exec_and_retains_diagnostic(self):
        harness, descriptor, project = self.capture_fixture()
        proof = cgroup.capture(harness, descriptor, inspected())
        self.assertEqual(proof["container_id"], "a" * 64)
        argv = harness.calls[0][1]
        self.assertEqual(argv[:6], [harness.cli, "exec", "--environment", "env-exact", "--machine", "machine-exact"])
        self.assertFalse(harness.calls[0][2]["success"])
        self.assertEqual(harness.calls[0][2]["cwd"], project)
        self.assertEqual(proof["project_path"], str(project))
        self.assertEqual(proof["project_definition_sha256"], cgroup.hashlib.sha256((project / "vz.json").read_bytes()).hexdigest())
        for output in ((b"partial", b"read failed", 1), (pack(fixture()), b"warning", 0), (b"bad", b"", 0)):
            harness.output = output
            with self.assertRaises(ValueError):
                cgroup.capture(harness, descriptor, inspected())
        self.assertEqual(len(harness.calls), 4)  # One capture, never a retry.
        for record in (dict(inspected(), Id="bad"), dict(inspected(), HostConfig={"CgroupnsMode": "host"}),
                       dict(inspected(), State={"Running": False, "Pid": PID})):
            with self.assertRaises(ValueError):
                cgroup.capture(harness, descriptor, record)
        self.assertEqual(len(harness.calls), 4)

    def test_project_or_machine_owner_mismatch_rejected_before_dispatch(self):
        changes = [lambda t: t["primary"].update(project_id="foreign"),
                   lambda t: t["neighbor"].update(project_id="foreign"),
                   lambda t: t["primary"].update(environment_id="foreign"),
                   lambda t: t["primary"].update(state="stopped"),
                   lambda t: t["primary"]["machines"][0].update(machine_id="foreign"),
                   lambda t: t["primary"]["machines"][0].update(incarnation_id="stale"),
                   lambda t: t["primary"]["machines"][0].update(incarnation_generation=2),
                   lambda t: t["primary"]["machines"][0]["docker_context"].update(name="foreign"),
                   lambda t: t["primary"]["machines"].append(copy.deepcopy(t["primary"]["machines"][0]))]
        for change in changes:
            harness, descriptor, _ = self.capture_fixture()
            path = harness.evidence / "topology.json"; topology = json.loads(path.read_bytes())
            change(topology); path.write_text(json.dumps(topology))
            with self.assertRaises(ValueError):
                cgroup.capture(harness, descriptor, inspected())
            self.assertEqual(harness.calls, [])
        harness, descriptor, project = self.capture_fixture()
        (project / "vz.json").write_text('{"schema_version":1,"project_id":"foreign"}')
        with self.assertRaisesRegex(ValueError, "definition owner"):
            cgroup.capture(harness, descriptor, inspected())
        self.assertEqual(harness.calls, [])
        for value in (True, "1", None, 2):
            harness, descriptor, project = self.capture_fixture()
            (project / "vz.json").write_text(json.dumps({"schema_version": value, "project_id": "project-exact"}))
            with self.assertRaisesRegex(ValueError, "definition owner"):
                cgroup.capture(harness, descriptor, inspected())
            self.assertEqual(harness.calls, [])

    def test_missing_duplicate_or_symlink_project_evidence_rejected(self):
        for filename in ("topology.json", "vz.json"):
            for change in ("missing", "symlink", "duplicate"):
                with self.subTest(filename=filename, change=change):
                    harness, descriptor, project = self.capture_fixture()
                    path = (harness.evidence if filename == "topology.json" else project) / filename
                    original = path.read_text()
                    if change == "duplicate":
                        path.write_text(original[:-1] + ',"project_id":"foreign","project_id":"project-exact"}')
                    else:
                        path.unlink()
                        if change == "symlink":
                            target = path.with_suffix(".original"); target.write_text(original); target.chmod(0o600)
                            path.symlink_to(target)
                    with self.assertRaises(ValueError):
                        cgroup.capture(harness, descriptor, inspected())
                    self.assertEqual(harness.calls, [])

    def test_foreign_relative_or_symlink_project_path_rejected(self):
        for kind in ("relative", "outside", "root", "symlink", "dotdot"):
            with self.subTest(kind=kind):
                harness, descriptor, project = self.capture_fixture()
                link = harness.root / "alias"; link.symlink_to(project, target_is_directory=True)
                paths = {"relative": "build", "outside": str(harness.root.parent), "root": str(harness.root),
                         "symlink": str(link), "dotdot": str(project) + "/../build"}
                path = harness.evidence / "topology.json"; topology = json.loads(path.read_bytes())
                topology["project"] = paths[kind]; path.write_text(json.dumps(topology))
                with self.assertRaises(ValueError):
                    cgroup.capture(harness, descriptor, inspected())
                self.assertEqual(harness.calls, [])

    def test_project_change_during_observation_rejects_proof_without_retry(self):
        harness, descriptor, project = self.capture_fixture()
        harness.after_dispatch = lambda: (project / "vz.json").write_text('{"schema_version":1,"project_id":"foreign"}')
        with self.assertRaises(ValueError):
            cgroup.capture(harness, descriptor, inspected())
        self.assertEqual(len(harness.calls), 1)


if __name__ == "__main__":
    unittest.main()
