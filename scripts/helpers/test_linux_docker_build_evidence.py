"""Synthetic receipt adversaries only; these tests execute no Docker or VM."""
import copy
import base64
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import docker_host_driver as driver
import linux_docker_build_evidence as evidence
from test_linux_docker_compose_evidence import SyntheticEngine, data


class SyntheticBuilder(SyntheticEngine):
    @staticmethod
    def stamp(seconds):
        return (datetime(2026, 9, 6, 6) + timedelta(seconds=seconds)).isoformat() + "Z"

    def __call__(self, argv, **kwargs):
        args, builder = argv[5:], self.inputs.raw["builder"]
        if args == ["info", "--format", "{{json .}}"]:
            result = super().__call__(argv, **kwargs)
            value = json.loads(result.stdout)
            value["SystemTime"] = self.stamp(getattr(self, "solve_count", 0) * 10)
            return subprocess.CompletedProcess(argv, 0, data(value), b"")
        if args[:2] == ["buildx", "inspect"]:
            out = f"Name: {builder['name']}\nDriver: docker-container\nNodes:\nName: {builder['node']}\nEndpoint: {self.inputs.scope['docker_context']}\nStatus: running\n".encode()
            return subprocess.CompletedProcess(argv, 0, out, b"")
        if args == ["container", "inspect", builder["container_id"]]:
            out = data([{"Id": builder["container_id"], "Image": builder["image_id"],
                         "Name": "/buildx_buildkit_" + builder["node"], "RestartCount": 0,
                         "Config": {"Env": ["PATH=/usr/bin:/bin", "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt",
                                            "BUILDKIT_SETUP_CGROUPV2_ROOT=1"]},
                         "HostConfig": {"CgroupnsMode": "private", "Runtime": "youki", "Privileged": True, "Init": True},
                         "State": {"Running": True, "Status": "running", "Pid": 737,
                                   "StartedAt": "2026-09-06T05:50:57.314727448Z", "Paused": False,
                                   "Restarting": False, "Dead": False}}])
            return subprocess.CompletedProcess(argv, 0, out, b"")
        if args[:2] != ["buildx", "build"]:
            return super().__call__(argv, **kwargs)
        dest = Path(args[args.index("--output") + 1].removeprefix("type=local,dest="))
        suffix = dest.name.removeprefix("export-")
        seconds = getattr(self, "solve_count", 0) * 10
        self.solve_count = getattr(self, "solve_count", 0) + 1
        def vertex(name, key, cached=False):
            mounts = {"python3 /fixture/tools.py cache": "--mount=type=cache,id=vz04-cache-probe,target=/cache,sharing=locked ",
                      "python3 /fixture/tools.py secret": "--mount=type=secret,id=fixture,required=true "}
            name = mounts.get(name, "") + name
            return {"digest": "sha256:" + self.identity(key), "name": "[build 2/2] RUN --network=none " + name,
                    "started": self.stamp(seconds + 3), "completed": self.stamp(seconds + 4), "cached": cached}
        if suffix == "secret-missing":
            error = vertex("--mount=type=secret,id=fixture,required=true python3 /fixture/tools.py secret", "secret")
            error["error"] = "secret fixture: not found"
            return subprocess.CompletedProcess(argv, 1, b"", data({"vertexes": [error]}) + b"\nERROR: failed to solve: secret fixture: not found\n")
        if suffix in {"alpha", "alpha-reuse", "beta"}:
            variant = "beta" if suffix == "beta" else "alpha"
            payload = f"vz04-build-v1\nvariant={variant}\n".encode()
            hit = suffix == "alpha-reuse"
            ids = {role: "sha256:" + self.identity("base" if role == "base" else suffix + role)
                   for role in ("base", "context", "copy", "run", "output")}
            names = {"base": "[build 1/3] FROM " + self.inputs.raw["images"]["base"]["reference"],
                     "context": "[internal] load build context",
                     "copy": "[build 2/3] COPY tools.py input.txt intermediate-canary.txt /fixture/",
                     "run": "[build 3/3] RUN --network=none python3 /fixture/tools.py payload",
                     "output": "[output 1/1] COPY --from=build /out/payload.txt /payload.txt"}
            edges = {"base": [], "context": [], "copy": ["base", "context"], "run": ["copy"], "output": ["run"]}
            intervals = {"base": (1, 2), "context": (1, 2), "copy": (2, 3),
                         "run": (3, 3 if hit else 4), "output": (3 if hit else 4, 5)}
            vertices = [{"digest": ids[role], "name": names[role], "inputs": [ids[x] for x in edges[role]],
                         "started": self.stamp(seconds + intervals[role][0]),
                         "completed": self.stamp(seconds + intervals[role][1]),
                         "cached": hit and role in ("copy", "run", "output")}
                        for role in ("run", "base", "context", "copy", "output")]
        elif suffix.startswith("cache-"):
            state = suffix.removeprefix("cache-"); step = "first" if state == "cold" else "second"
            payload = f"vz04-cache-v1\nowner={self.owner}\nstate={state}\nstep={step}\n".encode()
            vertices = [vertex("python3 /fixture/tools.py cache", suffix)]
        else:
            payload = b"vz04-secret-mount-ok-v1\n"
            vertices = [vertex("python3 /fixture/tools.py secret", "secret"), vertex("test ! -e /run/secrets/fixture", "secret-absent")]
        dest.mkdir(); (dest / evidence.EXPORTS[suffix]).write_bytes(payload)
        batch = {"vertexes": vertices}
        if suffix in ("alpha", "beta"):
            batch["logs"] = [{"vertex": ids["run"], "stream": 1, "timestamp": self.stamp(seconds + 3.5),
                              "data": base64.b64encode(b"vz04-payload-step-executed\n").decode()}]
        return subprocess.CompletedProcess(argv, 0, b"", data(batch) + b"\n")


class BuildEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="vz-build-raw-offline-")
        cls.root = Path(cls.temp.name).resolve(); cls.fixture = cls.root / "fixture"
        shutil.copytree(Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker", cls.fixture,
                        ignore=shutil.ignore_patterns("__pycache__"))
        config = cls.root / "config"; config.mkdir(mode=0o700)
        plugins = config / "cli-plugins"; plugins.mkdir(mode=0o700)
        (config / "config.json").write_text('{"currentContext":"default"}')
        clients = {}
        for name in ("docker", "compose", "buildx"):
            path = cls.root / "docker" if name == "docker" else plugins / ("docker-" + name)
            path.write_bytes(b"synthetic-never-executed"); path.chmod(0o500)
            clients[name] = {"path": str(path), "sha256": evidence.sha(path.read_bytes())}
        cls.inputs = {"schema_version": 1, "run_id": "synthetic-build-123", "release_sha256": "a" * 64,
            "fixture_sha256": driver.tree_digest(cls.fixture), "docker_config": str(config), "clients": clients,
            "scope": {"project_id": "project", "environment_id": "environment", "machine_id": "machine",
                      "machine_incarnation": "incarnation", "runtime_identity": "runtime", "docker_context": "owned-context",
                      "docker_endpoint": "unix://" + str(cls.root / "machine.sock"), "engine_id": "engine"},
            "images": {"base": {"reference": "fixture.invalid/base@sha256:" + "b" * 64, "id": "sha256:" + "c" * 64, "platform": "linux/arm64"},
                       "compose": {"reference": "sha256:" + "d" * 64, "id": "sha256:" + "d" * 64, "platform": "linux/arm64"}},
            "builder": {"name": "owned-builder", "node": "owned-node", "container_id": "e" * 64, "image_id": "sha256:" + "f" * 64}}
        inputs = driver.Inputs(cls.inputs, suite="build"); cls.base = cls.root / "baseline"
        original_stat = Path.stat
        def fake_stat(path, *args, **kwargs):
            if str(path) == cls.inputs["scope"]["docker_endpoint"][7:]:
                return types.SimpleNamespace(st_mode=stat.S_IFSOCK)
            return original_stat(path, *args, **kwargs)
        def persist(_recorder, path, value, **_kwargs):
            path.write_bytes(data(value))  # Synthetic tests make no durability claim.
        with patch.object(driver.sys, "platform", "darwin"), patch.object(driver.os, "uname", return_value=types.SimpleNamespace(machine="arm64")), \
                patch.object(Path, "stat", fake_stat), patch.object(driver, "execute", side_effect=SyntheticBuilder(inputs)), \
                patch.object(driver.Recorder, "persist", persist):
            result = driver.Driver(inputs, cls.fixture, cls.base).run("build")
        if result["outcome"] != "fixture_assertions_passed":
            raise AssertionError(result)
        evidence.validate(cls.base, cls.inputs)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.directory = self.root / self._testMethodName
        shutil.copytree(self.base, self.directory)
        for path in self.directory.glob("command-*.json"):
            value = json.loads(path.read_bytes())
            if "argv" in value:
                value["argv"] = [word.replace(str(self.base), str(self.directory)) for word in value["argv"]]
                path.write_bytes(data(value))
        self.refresh()

    def refresh(self):
        for ack in self.directory.glob("command-*.acknowledgement.json"):
            value = json.loads(ack.read_bytes())
            value["terminal_receipt_sha256"] = evidence.sha((self.directory / f"command-{value['command_index']:05d}.json").read_bytes())
            ack.write_bytes(data(value))
        paths = sorted(p for p in self.directory.rglob("*") if p.is_file() and p.name != "checksums.sha256" and "private-tmp" not in p.parts)
        (self.directory / "checksums.sha256").write_text("".join(f"{evidence.sha(p.read_bytes())}  {p.relative_to(self.directory).as_posix()}\n" for p in paths))

    def change(self, name, mutate):
        p = self.directory / name; value = json.loads(p.read_bytes()); mutate(value); p.write_bytes(data(value)); self.refresh()

    def raw(self, index, stream, mutate):
        name = f"command-{index:05d}.json"; row = json.loads((self.directory / name).read_bytes())
        path = self.directory / row[stream]; value = mutate(path.read_bytes()); path.write_bytes(value)
        row["observed_bytes"][stream] = row["retained_observed_" + stream + "_bytes"] = len(value)
        for key in (stream + "_sha256", "raw_" + stream + "_sha256", "retained_observed_" + stream + "_sha256"):
            row[key] = evidence.sha(value)
        (self.directory / name).write_bytes(data(row)); self.refresh()

    def rejected(self, inputs=None):
        with self.assertRaises(evidence.Invalid):
            evidence.validate(self.directory, inputs or self.inputs)

    def test_complete_synthetic_suite_is_scoped_not_certified(self):
        report = evidence.validate(self.directory, self.inputs)
        self.assertEqual(report["command_count"], 39)
        self.assertEqual(report["recipes_validated"], list(evidence.RECIPES))
        self.assertFalse(report["compatibility_certified"])
        self.assertEqual(report["builder_cleanup_scope"], "parent_harness_required")

    def test_wrong_suite(self):
        self.change("result.json", lambda x: x.update(suite="all")); self.rejected()

    def test_truncated_recipe_inventory(self):
        self.change("result.json", lambda x: x["observations"].pop()); self.rejected()

    def test_misattributed_command_range(self):
        self.change("result.json", lambda x: x["observations"][1].update(first_command=9)); self.rejected()

    def test_missing_coverage_exclusions(self):
        self.change("result.json", lambda x: x.update(remaining=[])); self.rejected()

    def test_cleanup_error(self):
        self.change("result.json", lambda x: x.update(cleanup_errors=["retained uncertainty"])); self.rejected()

    def test_foreign_expected_owner(self):
        inputs = copy.deepcopy(self.inputs); inputs["scope"]["machine_id"] = "foreign"; self.rejected(inputs)

    def test_foreign_engine(self):
        self.raw(6, "stdout", lambda b: data(json.loads(b) | {"ID": "foreign"})); self.rejected()

    def test_foreign_builder_endpoint(self):
        self.raw(7, "stdout", lambda b: b.replace(b"Endpoint: owned-context", b"Endpoint: default")); self.rejected()

    def test_ambiguous_builder_nodes(self):
        self.raw(7, "stdout", lambda b: b + b"Name: other-node\n"); self.rejected()

    def test_foreign_builder_container(self):
        self.raw(8, "stdout", lambda b: data([json.loads(b)[0] | {"Image": "sha256:" + "0" * 64}])); self.rejected()

    def test_resealed_builder_cgroup_policy_adversaries(self):
        original = json.loads((self.directory / "command-00008.stdout").read_bytes())
        cases = [
            ("Config", "Env", original[0]["Config"]["Env"][:-1]),
            ("Config", "Env", original[0]["Config"]["Env"] + ["BUILDKIT_SETUP_CGROUPV2_ROOT=1"]),
            ("Config", "Env", original[0]["Config"]["Env"] + ["BUILDKIT_SETUP_CGROUPV2_ROOT=0"]),
            ("Config", "Env", original[0]["Config"]["Env"][:-1] + ["BUILDKIT_SETUP_CGROUPV2_ROOT=0"]),
            ("Config", "Env", None),
            ("HostConfig", "CgroupnsMode", "host"), ("HostConfig", "CgroupnsMode", ""),
            ("HostConfig", "CgroupnsMode", None), ("HostConfig", "Runtime", "runc"),
            ("HostConfig", "Privileged", False), ("HostConfig", "Privileged", 1),
            ("HostConfig", "Init", False), ("HostConfig", "Init", 1),
            ("State", "Pid", 0), ("State", "Pid", -1), ("State", "Pid", True),
            ("State", "Pid", "737"), ("State", "StartedAt", "0001-01-01T00:00:00Z"),
            ("State", "StartedAt", "2026-99-06T05:50:57Z"), ("State", "StartedAt", None),
            ("State", "Paused", True), ("State", "Restarting", True), ("State", "Dead", True),
            ("State", "Status", "exited"), ("State", "Running", 1),
        ]
        for section, key, value in cases:
            with self.subTest(section=section, key=key, value=value):
                changed = copy.deepcopy(original); changed[0][section][key] = value
                self.raw(8, "stdout", lambda _, value=changed: data(value)); self.rejected()
        for value in (1, True, None):
            with self.subTest(restart_count=value):
                changed = copy.deepcopy(original); changed[0]["RestartCount"] = value
                self.raw(8, "stdout", lambda _, value=changed: data(value)); self.rejected()

    def test_resealed_builder_process_replacement_between_recipes(self):
        original = json.loads((self.directory / "command-00013.stdout").read_bytes())
        for key, value in (("Pid", 738), ("StartedAt", "2026-09-06T05:50:58.314727448Z")):
            with self.subTest(key=key):
                changed = copy.deepcopy(original); changed[0]["State"][key] = value
                self.raw(13, "stdout", lambda _, value=changed: data(value)); self.rejected()

    def test_intent_drift(self):
        self.change("command-00009.intent.json", lambda x: x["argv"].append("--load")); self.rejected()

    def test_unaccounted_command(self):
        self.change("result.json", lambda x: x.update(command_count=40)); self.rejected()

    def test_raw_hash_mismatch(self):
        (self.directory / "command-00009.stderr").write_bytes(b"{}\n"); self.refresh(); self.rejected()

    def test_actual_run_missing(self):
        self.raw(9, "stderr", lambda b: b"{}\n"); self.rejected()

    def test_duplicate_terminal_vertex(self):
        self.raw(9, "stderr", lambda b: b + b); self.rejected()

    def test_cold_run_cannot_be_cached(self):
        self.raw(9, "stderr", lambda b: data({"vertexes": [json.loads(b)["vertexes"][0] | {"cached": True}]})); self.rejected()

    def test_cache_reuse_wrong_vertex(self):
        self.raw(14, "stderr", lambda b: data({"vertexes": [json.loads(b)["vertexes"][0] | {"digest": "sha256:" + "9" * 64}]})); self.rejected()

    def test_cache_boolean_not_integer(self):
        self.raw(14, "stderr", lambda b: data({"vertexes": [json.loads(b)["vertexes"][0] | {"cached": 1}]})); self.rejected()

    def test_beta_did_not_execute(self):
        self.raw(19, "stderr", lambda b: data({"vertexes": [json.loads(b)["vertexes"][0] | {"cached": True}]})); self.rejected()

    def test_export_marker_corruption(self):
        (self.directory / "export-beta/payload.txt").write_bytes(b"alpha"); self.refresh(); self.rejected()

    def test_export_extra_empty_directory(self):
        (self.directory / "export-alpha/foreign").mkdir(); self.rejected()

    def test_cache_mount_wrong_owner(self):
        p = self.directory / "export-cache-warm/cache.txt"; p.write_bytes(p.read_bytes().replace(b"owner=vz04-", b"owner=foreign-")); self.refresh(); self.rejected()

    def test_secret_mount_next_run_missing(self):
        self.raw(34, "stderr", lambda b: data({"vertexes": json.loads(b)["vertexes"][:1]})); self.rejected()

    def test_secret_leak_rehashed_raw(self):
        secret = (self.fixture / "inputs/secret.txt").read_bytes().decode().strip()
        self.raw(34, "stderr", lambda b: b + data({"name": secret}) + b"\n"); self.rejected()

    def test_secret_leak_split_base64_logs(self):
        secret = (self.fixture / "inputs/secret.txt").read_bytes().strip()
        half = len(secret) // 2
        logs = [data({"logs": [{"data": base64.b64encode(part).decode()}]}) for part in (secret[:half], secret[half:])]
        self.raw(34, "stderr", lambda b: b + b"\n".join(logs) + b"\n"); self.rejected()

    def test_copy_named_like_payload_cannot_replace_run(self):
        self.raw(9, "stderr", lambda b: data({"vertexes": [json.loads(b)["vertexes"][0] | {"name": "COPY python3 /fixture/tools.py payload"}]})); self.rejected()

    def test_invalid_terminal_timestamp(self):
        self.raw(9, "stderr", lambda b: data({"vertexes": [json.loads(b)["vertexes"][0] | {"completed": "not-a-timestamp"}]})); self.rejected()

    def test_nanosecond_progress_and_offset_preserved_on_python39(self):
        original = json.loads((self.directory / "command-00009.stderr").read_bytes())
        for fraction in ("", ".1", ".12", ".1234", ".123456", ".123456789"):
            for offset, minutes in (("Z", 0), ("+05:30", 330), ("-07:00", -420)):
                with self.subTest(fraction=fraction, offset=offset):
                    calendar = datetime(2026, 9, 6, 6, 0, 4) + timedelta(minutes=minutes)
                    stamp = calendar.isoformat() + fraction + offset
                    parsed = evidence.progress_timestamp(stamp)
                    self.assertEqual(parsed.utcoffset(), timedelta(minutes=minutes))
                    self.assertEqual(parsed.microsecond, int(fraction[1:7].ljust(6, "0") or "0"))
                    changed = copy.deepcopy(original); changed["vertexes"][0]["completed"] = stamp
                    changed["vertexes"][4]["started"] = "2026-09-06T06:00:05Z"
                    self.raw(9, "stderr", lambda _, value=changed: data(value))
                    evidence.validate(self.directory, self.inputs)
                    vertices, _ = evidence.progress(data(changed))
                    self.assertEqual(vertices[0]["completed"], stamp)

    def test_distinct_solve_ids_preserve_content_operation_identity(self):
        a = json.loads((self.directory / "command-00009.stderr").read_bytes())
        b = json.loads((self.directory / "command-00014.stderr").read_bytes())
        self.assertNotEqual(a["vertexes"][0]["digest"], b["vertexes"][0]["digest"])
        evidence.validate(self.directory, self.inputs)

    def test_resealed_graph_missing_duplicate_disconnected_and_drifting_roles(self):
        original = json.loads((self.directory / "command-00009.stderr").read_bytes())
        cases = []
        for index in range(5):
            value = copy.deepcopy(original); value["vertexes"].pop(index); cases.append(value)
            value = copy.deepcopy(original)
            value["vertexes"].append(dict(value["vertexes"][index], digest="sha256:" + "9" * 64))
            cases.append(value)
        for edges in ([], [original["vertexes"][1]["digest"]], ["sha256:" + "9" * 64],
                      original["vertexes"][3]["inputs"] * 2, list(reversed(original["vertexes"][3]["inputs"]))):
            value = copy.deepcopy(original); value["vertexes"][3]["inputs"] = edges; cases.append(value)
        for key, replacement in (("name", "[build 3/3] RUN echo forged"), ("inputs", [])):
            value = copy.deepcopy(original)
            early = dict(value["vertexes"][0]); early.pop("completed"); early[key] = replacement
            value["vertexes"].insert(0, early); cases.append(value)
        for value in cases:
            with self.subTest(value=value):
                self.raw(9, "stderr", lambda _, value=value: data(value)); self.rejected()

    def test_coherent_source_phase_repetition_is_not_duplicate_execution(self):
        original = json.loads((self.directory / "command-00009.stderr").read_bytes())
        value = copy.deepcopy(original)
        source = dict(value["vertexes"][1], started="2026-09-06T06:00:00.1Z", completed="2026-09-06T06:00:00.9Z")
        value["vertexes"].insert(0, source)
        value["vertexes"].insert(0, dict(source))
        self.raw(9, "stderr", lambda _: data(value))
        evidence.validate(self.directory, self.inputs)
        value["vertexes"].append(copy.deepcopy(value["vertexes"][2]))
        self.raw(9, "stderr", lambda _: data(value)); self.rejected()

    def test_resealed_marker_must_belong_to_live_payload_run(self):
        original = json.loads((self.directory / "command-00009.stderr").read_bytes())
        cases = []
        for key, replacement in (("vertex", original["vertexes"][1]["digest"]), ("stream", 2), ("stream", True),
                                 ("timestamp", "2026-09-06T05:59:59Z"),
                                 ("data", base64.b64encode(b"wrong\n").decode())):
            value = copy.deepcopy(original); value["logs"][0][key] = replacement; cases.append(value)
        value = copy.deepcopy(original); value["logs"] = []; cases.append(value)
        value = copy.deepcopy(original); value["logs"] *= 2; cases.append(value)
        for value in cases:
            with self.subTest(value=value):
                self.raw(9, "stderr", lambda _, value=value: data(value)); self.rejected()

    def test_cached_run_cannot_replay_execution_logs(self):
        value = json.loads((self.directory / "command-00014.stderr").read_bytes())
        value["logs"] = [{"vertex": value["vertexes"][0]["digest"], "stream": 1,
                          "timestamp": value["vertexes"][0]["started"],
                          "data": base64.b64encode(b"vz04-payload-step-executed\n").decode()}]
        self.raw(14, "stderr", lambda _: data(value)); self.rejected()

    def test_resealed_cached_graph_cannot_predate_its_engine_clock(self):
        value = json.loads((self.directory / "command-00014.stderr").read_bytes())
        for vertex in value["vertexes"]:
            for key in ("started", "completed"):
                vertex[key] = (evidence.progress_timestamp(vertex[key]) - timedelta(seconds=10)).isoformat()
        self.raw(14, "stderr", lambda _: data(value)); self.rejected()

    def test_resealed_base_digest_drift_rejected_despite_connected_graph(self):
        value = json.loads((self.directory / "command-00014.stderr").read_bytes())
        value["vertexes"][1]["digest"] = "sha256:" + "8" * 64
        value["vertexes"][3]["inputs"][0] = "sha256:" + "8" * 64
        self.raw(14, "stderr", lambda _: data(value)); self.rejected()

    def test_resealed_same_input_command_cannot_change_variant(self):
        for name in ("command-00014.json", "command-00014.intent.json"):
            self.change(name, lambda value: value.update(argv=[x.replace("FIXTURE_VARIANT=alpha", "FIXTURE_VARIANT=beta")
                                                             for x in value["argv"]]))
        self.rejected()

    def test_resealed_engine_clock_cannot_precede_previous_run_end(self):
        self.raw(11, "stdout", lambda raw: data(dict(json.loads(raw), SystemTime="2026-09-06T06:00:03Z")))
        self.rejected()

    def test_future_output_stage_rejected_by_next_engine_clock(self):
        value = json.loads((self.directory / "command-00009.stderr").read_bytes())
        value["vertexes"][4].update(started="2030-09-06T06:00:00Z", completed="2030-09-06T06:00:01Z")
        self.raw(9, "stderr", lambda _: data(value)); self.rejected()

    def test_source_phase_cannot_predate_authenticated_engine_clock(self):
        value = json.loads((self.directory / "command-00009.stderr").read_bytes())
        value["vertexes"][1].update(started="2026-09-06T05:59:59Z")
        self.raw(9, "stderr", lambda _: data(value)); self.rejected()

    def test_source_phase_completion_cannot_be_hidden_by_later_earlier_record(self):
        value = json.loads((self.directory / "command-00009.stderr").read_bytes())
        value["vertexes"].insert(0, dict(value["vertexes"][1], completed="2026-09-06T06:00:02.9Z"))
        self.raw(9, "stderr", lambda _: data(value)); self.rejected()

    def test_cached_progress_cannot_regress_then_recover(self):
        value = json.loads((self.directory / "command-00014.stderr").read_bytes())
        early = dict(value["vertexes"][0]); early.pop("completed")
        value["vertexes"] = [early, dict(early, cached=False)] + value["vertexes"]
        self.raw(14, "stderr", lambda _: data(value)); self.rejected()

    def test_future_unfinished_source_update_is_bounded_by_next_engine_clock(self):
        value = json.loads((self.directory / "command-00009.stderr").read_bytes())
        unfinished = dict(value["vertexes"][1], started="2030-09-06T06:00:00Z")
        unfinished.pop("completed")
        value["vertexes"].append(unfinished)
        self.raw(9, "stderr", lambda _: data(value)); self.rejected()

    def test_nanoseconds_are_not_lost_at_lifecycle_boundaries(self):
        a = evidence.progress_ns("2026-09-06T06:00:03.123456788Z")
        b = evidence.progress_ns("2026-09-06T11:30:03.123456789+05:30")
        self.assertEqual(b - a, 1)

    def test_resealed_progress_invalid_calendar_fraction_and_offset(self):
        original = json.loads((self.directory / "command-00009.stderr").read_bytes())
        for stamp in ("2026-02-30T00:00:01.123456789Z", "2025-02-29T00:00:01Z",
                      "2026-99-06T00:00:01Z", "2026-09-06T24:00:01Z", "2026-09-06T00:60:01Z",
                      "2026-09-06T00:00:60Z", "0000-09-06T00:00:01Z",
                      "2026-09-06T00:00:01.1234567890Z", "2026-09-06T00:00:01.Z",
                      "2026-09-06T00:00:01.123456789+24:00", "2026-09-06T00:00:01.123456789+01:60",
                      "2026-09-06T00:00:01.123456789", "2026-09-06T00:00:01.123456789Zjunk"):
            with self.subTest(stamp=stamp):
                changed = copy.deepcopy(original); changed["vertexes"][0]["completed"] = stamp
                self.raw(9, "stderr", lambda _, value=changed: data(value)); self.rejected()

    def test_negative_error_wrong_reason(self):
        self.raw(39, "stderr", lambda b: data({"error": "network unavailable"})); self.rejected()

    def test_known_required_secret_source_excerpt_accepted(self):
        source = (self.fixture / "build/Dockerfile.secret").read_text().splitlines()
        excerpt = ["Dockerfile.secret:6", "--------------------"]
        excerpt += [f" {n:3d} | {'>>>' if n == 6 else '   '} {source[n - 1]}" for n in range(4, 9)]
        excerpt += ["--------------------", "ERROR: failed to solve: secret fixture: not found"]
        self.raw(39, "stderr", lambda b: b.splitlines()[0] + b"\n" + "\n".join(excerpt).encode() + b"\n")
        evidence.validate(self.directory, self.inputs)

    def test_negative_plain_footer_is_not_vertex_proof(self):
        self.raw(39, "stderr", lambda b: b.splitlines()[-1] + b"\n"); self.rejected()

    def test_negative_missing_cli_footer(self):
        self.raw(39, "stderr", lambda b: b.splitlines()[0] + b"\n"); self.rejected()

    def test_negative_unrecognized_trailer(self):
        self.raw(39, "stderr", lambda b: b + b"Unexpected unrelated failure\n"); self.rejected()

    def test_negative_unrelated_structured_failure(self):
        def alter(b):
            value = json.loads(b.splitlines()[0]); vertex = copy.deepcopy(value['vertexes'][0])
            vertex.update(name="[other 1/1] RUN false", error="network unavailable")
            value['vertexes'].append(vertex)
            return data(value) + b"\n" + b.splitlines()[-1] + b"\n"
        self.raw(39, "stderr", alter); self.rejected()

    def test_negative_duplicate_terminal_error(self):
        def alter(b):
            value = json.loads(b.splitlines()[0]); value['vertexes'] *= 2
            return data(value) + b"\n" + b.splitlines()[-1] + b"\n"
        self.raw(39, "stderr", alter); self.rejected()

    def test_flat_progress_shape_rejected(self):
        self.raw(9, "stderr", lambda b: data(json.loads(b)['vertexes'][0])); self.rejected()

    def test_malformed_batch_member(self):
        self.raw(9, "stderr", lambda b: data(json.loads(b) | {"statuses": [None]})); self.rejected()

    def test_progress_json_duplicate_keys(self):
        self.raw(9, "stderr", lambda b: b'{"vertexes":[],"vertexes":[]}\n'); self.rejected()

    def test_payload_named_echo_is_not_exact_run(self):
        self.raw(9, "stderr", lambda b: b.replace(b"RUN --network=none python3", b"RUN --network=none echo python3")); self.rejected()

    def test_negative_failure_not_acknowledged(self):
        (self.directory / "command-00039.acknowledgement.json").unlink(); self.refresh(); self.rejected()

    def test_negative_timeout_cannot_pass(self):
        self.change("command-00039.json", lambda x: x.update(timed_out=True)); self.rejected()

    def test_negative_export_cannot_pass(self):
        (self.directory / "export-secret-missing").mkdir(); self.rejected()

    def test_unmanifested_artifact(self):
        (self.directory / "unlisted").write_bytes(b"no"); self.rejected()

    def test_symlink_export(self):
        p = self.directory / "export-alpha/payload.txt"; p.unlink(); p.symlink_to(self.base / "export-alpha/payload.txt"); self.rejected()


if __name__ == "__main__":
    unittest.main()
