"""Four-slot raw evidence adversaries; no Docker, VM, or acceptance claims."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import shutil
import unittest

import linux_docker_parallel_evidence as evidence
import test_linux_docker_artifact_evidence as artifact_tests
from test_linux_docker_build_evidence import SyntheticBuilder


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


class ParallelEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        artifact_tests.ArtifactEvidenceTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        artifact_tests.ArtifactEvidenceTests.tearDownClass()

    def setUp(self):
        self.raw = artifact_tests.ArtifactEvidenceTests("test_all_five_exact_operations")
        self.raw.setUp()
        self.addCleanup(self.raw.doCleanups)
        self.flow = self.raw.flow
        self.inputs = self.raw.inputs
        self.fixture = self.flow / "parallel-fixture"
        self.fixture.mkdir()
        (self.fixture / "Dockerfile.parallel").write_text("ARG FIXTURE_BASE\nFROM ${FIXTURE_BASE} AS build\n")
        (self.fixture / "parallel.py").write_text("# immutable synthetic source\n")
        self.base = self.raw.directory
        self.operations = {}
        self.make(0)

    @staticmethod
    def wall(seconds):
        return evidence.progress_ns(SyntheticBuilder.stamp(seconds))

    @staticmethod
    def mono(seconds):
        return 10**9 + round(seconds * 10**9)

    def transcript(self, slot):
        participants = []
        for number in range(4):
            participants.append({"schema_version": 1, "run_id": self.inputs["run_id"], "slot": number,
                "started_unix_ns": self.wall(3.1 + number * .01), "started_monotonic_ns": self.mono(3.1 + number * .01),
                "ready_unix_ns": self.wall(3.2 + number * .05), "ready_monotonic_ns": self.mono(3.2 + number * .05)})
        value = dict(participants[slot], outcome="released", error_code=None, participants=participants)
        for phase, seconds in (("all_ready", 3.5 + slot * .01), ("released", 4.6 + slot * .01), ("completed", 4.7 + slot * .01)):
            value[phase + "_unix_ns"], value[phase + "_monotonic_ns"] = self.wall(seconds), self.mono(seconds)
        value["samples"] = [{"unix_ns": value["ready_unix_ns"], "monotonic_ns": value["ready_monotonic_ns"], "ready_slots": [slot]},
                            {"unix_ns": value["all_ready_unix_ns"], "monotonic_ns": value["all_ready_monotonic_ns"], "ready_slots": [0, 1, 2, 3]},
                            {"unix_ns": value["released_unix_ns"], "monotonic_ns": value["released_monotonic_ns"], "ready_slots": [0, 1, 2, 3]}]
        value["generation_sha256"] = hashlib.sha256(json.dumps(participants, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        value["payload"] = dict(self.operations[slot]["payload"], mode=0o644)
        return value

    def batch(self, slot):
        names = {"base": "[build 1/3] FROM " + self.inputs["images"]["base"]["reference"], "context": "[internal] load build context",
                 "copy": "[build 2/3] COPY parallel.py /fixture/parallel.py", "run": evidence.RUN_NAME,
                 "output": "[output 1/1] COPY --from=build /out/payload.txt /payload.txt", "export": evidence.OCI_EXPORT}
        identities = {role: "sha256:" + hashlib.sha256((role + str(slot)).encode()).hexdigest() for role in names}
        edges = {"base": [], "context": [], "copy": ["base", "context"], "run": ["copy"], "output": ["run"], "export": []}
        intervals = {"base": (1, 2), "context": (1, 2), "copy": (2, 3), "run": (3, 5), "output": (5, 6), "export": (7, 8)}
        vertices = [{"digest": identities[role], "name": name, "inputs": [identities[parent] for parent in edges[role]],
                     "started": SyntheticBuilder.stamp(intervals[role][0]), "completed": SyntheticBuilder.stamp(intervals[role][1])}
                    for role, name in names.items()]
        logs = [{"vertex": identities["run"], "stream": 1, "timestamp": SyntheticBuilder.stamp(4.9),
                 "data": base64.b64encode(b"VZ_PARALLEL_BARRIER=" + encode(self.transcript(slot))).decode()}]
        statuses = [{"id": name, "vertex": identities["export"], "started": SyntheticBuilder.stamp(7), "completed": SyntheticBuilder.stamp(8)}
                    for name in ("exporting layers", "exporting manifest " + self.raw.image["manifest"]["digest"],
                                 "exporting config " + self.raw.image["config"]["digest"])]
        return {"vertexes": vertices, "logs": logs, "statuses": statuses}

    def make(self, slot):
        directory = self.flow / ("slot-" + str(slot))
        if directory.exists():
            shutil.rmtree(directory)
        shutil.copytree(self.base, directory)
        self.raw.directory = directory
        self.directory, self.slot = directory, slot
        payload = f"vz04-parallel-v1\nslot={slot}\n".encode()
        operation = {"schema_version": 1, "slot": slot, "parallel_fixture": str(self.fixture),
            "parallel_fixture_sha256": evidence.fixture_digest(self.fixture), "output": str(directory / "oci"),
            "payload": {"path": "payload.txt", "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()},
            "run_id": self.inputs["run_id"]}
        self.operations[slot] = operation
        shutil.rmtree(directory / "cache")
        for name, value in (("operation.json", operation), ("operation.intent.json", operation), ("artifact-validation.json", {"oci": self.raw.image})):
            self.raw.write(name, value)
        argv = ["docker", "--config", self.inputs["docker_config"], "--context", self.inputs["scope"]["docker_context"],
                "buildx", "build", "--builder", self.inputs["builder"]["name"], "--platform", "linux/arm64", "--progress", "rawjson",
                "--file", str(self.fixture / "Dockerfile.parallel"), "--provenance=false", "--sbom=false", "--output",
                "type=oci,dest=" + operation["output"] + evidence.OCI_OPTIONS, "--build-arg", "FIXTURE_BASE=" + self.inputs["images"]["base"]["reference"],
                "--build-arg", "FIXTURE_RUN=" + operation["run_id"], "--build-arg", "FIXTURE_SLOT=" + str(slot),
                "--network=none", str(self.fixture)]
        self.raw.change(5, lambda row: row.update(argv=argv), True)
        for index, seconds in enumerate((0, .1, .2, .3, .4, 8.5, 8.6, 8.7, 8.8), 1):
            self.raw.change(index, lambda row, seconds=seconds, index=index:
                            row.update(started_unix_ns=self.wall(seconds), elapsed_ns=8 * 10**9 if index == 5 else 10**7), True)
        self.raw.stream(5, "stderr", encode(self.batch(slot)))

    def validate(self):
        return evidence.validate_slot(self.directory, self.inputs, self.operations[self.slot])

    def rewrite_barrier(self, change):
        batch = self.batch(self.slot)
        transcript = self.transcript(self.slot)
        change(transcript)
        batch["logs"][0]["data"] = base64.b64encode(b"VZ_PARALLEL_BARRIER=" + encode(transcript)).decode()
        self.raw.stream(5, "stderr", encode(batch))

    def test_all_four_exact_slots_and_overlap(self):
        proofs = []
        for slot in range(4):
            self.make(slot)
            proof = self.validate()
            self.assertEqual(proof["slot"], slot)
            self.assertEqual(proof["command_count"], 9)
            proofs.append(proof)
        group = evidence.validate_group(proofs)
        self.assertEqual(group["command_count"], 36)
        self.assertGreater(group["run_overlap"]["duration_ns"], 0)
        self.assertGreater(group["barrier_overlap_monotonic"]["duration_ns"], 0)

    def test_exact_command_and_capture_adversaries(self):
        for change in (lambda r: r["argv"].insert(-1, "--load"), lambda r: r["argv"].insert(-1, "--no-cache"),
                       lambda r: r["argv"].remove("--network=none"), lambda r: r["argv"].__setitem__(-4, "FIXTURE_SLOT=2"),
                       lambda r: r.update(environment={"DOCKER_HOST": "foreign"}), lambda r: r.update(effects_uncertain=True),
                       lambda r: r.update(capture_complete=False), lambda r: r.update(exit_code=1),
                       lambda r: r.update(executable="/foreign/docker"), lambda r: r.update(mutation=False)):
            self.make(0); self.raw.change(5, change, True)
            with self.assertRaises(evidence.Invalid): self.validate()

    def test_graph_adversaries(self):
        for mutation in ("cache", "duplicate", "drift", "edges", "error", "future", "past", "dependency", "foreign", "missing", "future-source"):
            self.make(0); batch = self.batch(0); run = batch["vertexes"][3]
            if mutation == "cache": run["cached"] = True
            elif mutation == "duplicate": batch["vertexes"].append(copy.deepcopy(run))
            elif mutation == "drift": batch["vertexes"].append(dict(run, name="foreign"))
            elif mutation == "edges": run["inputs"] = []
            elif mutation == "error": run["error"] = "failed"
            elif mutation == "future": run["completed"] = SyntheticBuilder.stamp(11)
            elif mutation == "past": run["started"] = SyntheticBuilder.stamp(-1)
            elif mutation == "dependency": run["started"] = SyntheticBuilder.stamp(2.9)
            elif mutation == "foreign": run["name"] = run["name"].replace("sharing=shared", "sharing=locked")
            elif mutation == "missing": batch["vertexes"].remove(run)
            else:
                row = dict(batch["vertexes"][0], started=SyntheticBuilder.stamp(11)); row.pop("completed"); batch["vertexes"].append(row)
            self.raw.stream(5, "stderr", encode(batch))
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()

    def test_shared_copy_cache_does_not_weaken_uncached_run(self):
        batch = self.batch(0)
        batch["vertexes"][2]["cached"] = True
        self.raw.stream(5, "stderr", encode(batch))
        self.assertEqual(self.validate()["slot"], 0)
        batch["vertexes"][3]["cached"] = True
        self.raw.stream(5, "stderr", encode(batch))
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_source_lifetimes_complete_and_do_not_overlap(self):
        for role_index in (0, 1):
            self.make(0)
            batch = self.batch(0)
            original = batch["vertexes"][role_index]
            first = dict(original, completed=SyntheticBuilder.stamp(1.2))
            second = dict(original, started=SyntheticBuilder.stamp(1.3))
            batch["vertexes"][role_index:role_index + 1] = [first, second]
            self.raw.stream(5, "stderr", encode(batch))
            self.validate()
            for mutation in ("unfinished", "changed-duplicate", "overlap", "abandoned", "reset"):
                changed = copy.deepcopy(batch)
                a, b = changed["vertexes"][role_index:role_index + 2]
                if mutation == "unfinished": b.pop("completed")
                elif mutation == "changed-duplicate": changed["vertexes"].insert(role_index + 1, dict(a, cached=True))
                elif mutation == "overlap": b["started"] = SyntheticBuilder.stamp(1.1)
                elif mutation == "abandoned": a.pop("completed")
                else: changed["vertexes"].append({"digest": b["digest"], "name": b["name"], "inputs": []})
                self.raw.stream(5, "stderr", encode(changed))
                with self.subTest(role=role_index, mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()

    def test_exact_current_source_terminal_retransmission_preserves_raw_rows(self):
        for role_index in (0, 1):
            self.make(0)
            batch = self.batch(0)
            original = batch["vertexes"][role_index]
            first = dict(original, completed=SyntheticBuilder.stamp(1.2))
            second = dict(original, started=SyntheticBuilder.stamp(1.3))
            batch["vertexes"][role_index:role_index + 1] = [first, copy.deepcopy(first), second, copy.deepcopy(second)]
            raw = encode(batch)
            self.raw.stream(5, "stderr", raw)
            proof = self.validate()
            self.assertEqual(proof["progress_sha256"], hashlib.sha256(raw).hexdigest())
            if role_index == 1:
                self.assertEqual(proof["copy_graph"]["local_context"], [first, first, second, second])
            # The same current snapshot may cross a batch boundary, but its
            # complete object must remain identical in the source's stream.
            batch["vertexes"].pop(role_index + 3)
            raw = encode(batch) + encode({"vertexes": [copy.deepcopy(second)]})
            self.raw.stream(5, "stderr", raw)
            self.assertEqual(self.validate()["progress_sha256"], hashlib.sha256(raw).hexdigest())

    def test_source_terminal_retransmission_drift_stale_and_reopen_rejected(self):
        for role_index in (0, 1):
            for mutation in ("completed", "started", "cache", "error", "inputs", "stale", "reopen"):
                self.make(0)
                batch = self.batch(0)
                original = batch["vertexes"][role_index]
                first = dict(original, completed=SyntheticBuilder.stamp(1.2))
                second = dict(original, started=SyntheticBuilder.stamp(1.3))
                repeated = copy.deepcopy(second)
                if mutation == "completed": repeated["completed"] = SyntheticBuilder.stamp(1.9)
                elif mutation == "started": repeated["started"] = SyntheticBuilder.stamp(1.4)
                elif mutation == "cache": repeated["cached"] = True
                elif mutation == "error": repeated["error"] = "context canceled"
                elif mutation == "inputs": repeated["inputs"] = ["sha256:" + "f" * 64]
                elif mutation == "stale": repeated = copy.deepcopy(first)
                else: repeated.pop("completed")
                batch["vertexes"][role_index:role_index + 1] = [first, second, repeated]
                self.raw.stream(5, "stderr", encode(batch))
                with self.subTest(role=role_index, mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()
        for role_index in (2, 3, 4, 5):
            self.make(0)
            batch = self.batch(0)
            batch["vertexes"].insert(role_index + 1, copy.deepcopy(batch["vertexes"][role_index]))
            self.raw.stream(5, "stderr", encode(batch))
            with self.subTest(operation=role_index), self.assertRaises(evidence.Invalid): self.validate()

    def merged_batch(self, slot):
        batch = self.batch(slot)
        origin = self.batch(0)
        base = origin["vertexes"][0]["digest"]
        batch["vertexes"][0]["digest"] = base
        batch["vertexes"][2]["inputs"][0] = base
        if slot == 0:
            batch["vertexes"][2]["started"] = SyntheticBuilder.stamp(2.2)
            return batch
        canceled = batch["vertexes"][2]
        canceled.update(completed=SyntheticBuilder.stamp(2.1), error="context canceled: context canceled")
        adopted = copy.deepcopy(origin["vertexes"][2])
        adopted["started"] = SyntheticBuilder.stamp(2.2)
        batch["vertexes"].insert(3, adopted)
        return batch

    def test_cross_slot_copy_merge_requires_successful_origin(self):
        proofs = []
        for slot in range(4):
            self.make(slot)
            self.raw.stream(5, "stderr", encode(self.merged_batch(slot)))
            proofs.append(self.validate())
        self.assertIsNone(proofs[0]["copy_graph"]["alias"])
        self.assertEqual(proofs[1]["copy_graph"]["alias"]["adopted"], proofs[0]["copy_graph"]["copy"]["digest"])
        self.assertEqual(evidence.validate_group(proofs)["command_count"], 36)
        for mutation in ("foreign-origin", "origin-alias", "foreign-context", "duration", "base", "extra-error"):
            changed = copy.deepcopy(proofs)
            graph = changed[1]["copy_graph"]
            if mutation == "foreign-origin": graph["alias"]["adopted"] = "sha256:" + "f" * 64
            elif mutation == "origin-alias": changed[0]["copy_graph"]["alias"] = copy.deepcopy(graph["alias"])
            elif mutation == "foreign-context": graph["copy"]["inputs"][1] = "sha256:" + "f" * 64
            elif mutation == "duration": graph["copy"]["completed"] = SyntheticBuilder.stamp(3.1)
            elif mutation == "base": graph["base"] = "sha256:" + "f" * 64
            else: graph["copy"]["error"] = "error"
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): evidence.validate_group(changed)

    def test_copy_merge_raw_adversaries(self):
        for mutation in ("wrong-error", "cached", "unfinished", "duplicate", "post-terminal", "uncanceled",
                         "foreign-base", "local-context", "late-cancel", "early-cancel", "run-winner", "third-copy"):
            self.make(1)
            batch = self.merged_batch(1)
            canceled, adopted = batch["vertexes"][2:4]
            if mutation == "wrong-error": canceled["error"] = "context canceled"
            elif mutation == "cached": canceled["cached"] = True
            elif mutation == "unfinished": canceled.pop("completed")
            elif mutation == "duplicate": batch["vertexes"].append(copy.deepcopy(canceled))
            elif mutation == "post-terminal":
                later = copy.deepcopy(canceled); later.pop("completed"); later.pop("error"); batch["vertexes"].append(later)
            elif mutation == "uncanceled": canceled.pop("error")
            elif mutation == "foreign-base": adopted["inputs"][0] = "sha256:" + "f" * 64
            elif mutation == "local-context": adopted["inputs"][1] = canceled["inputs"][1]
            elif mutation == "late-cancel": canceled["completed"] = SyntheticBuilder.stamp(2.3)
            elif mutation == "early-cancel": canceled["started"] = SyntheticBuilder.stamp(1.9)
            elif mutation == "run-winner": batch["vertexes"][4]["inputs"] = [adopted["digest"]]
            else: batch["vertexes"].append(dict(adopted, digest="sha256:" + "f" * 64))
            self.raw.stream(5, "stderr", encode(batch))
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()

    def test_barrier_adversaries(self):
        changes = (lambda t: t.update(slot=1), lambda t: t.update(run_id="foreign"), lambda t: t.update(outcome="timeout"),
                   lambda t: t.update(error_code="error"), lambda t: t.update(generation_sha256="f" * 64),
                   lambda t: t["participants"].pop(), lambda t: t["participants"][1].update(slot=0),
                   lambda t: t["participants"][0].update(ready_unix_ns=1), lambda t: t.update(ready_monotonic_ns=True),
                   lambda t: t.update(released_monotonic_ns=t["all_ready_monotonic_ns"]),
                   lambda t: t.update(completed_unix_ns=self.wall(6.1)), lambda t: t["payload"].update(mode=0o600),
                   lambda t: t["payload"].update(sha256="f" * 64), lambda t: t["samples"].clear(),
                   lambda t: t["samples"][-1].update(ready_slots=[0]),
                   lambda t: t["samples"][-1].update(unix_ns=t["all_ready_unix_ns"]-1),
                   lambda t: t["samples"].pop(),
                   lambda t: t.update(unexpected=True))
        for change in changes:
            self.make(0); self.rewrite_barrier(change)
            with self.assertRaises(evidence.Invalid): self.validate()

    def test_log_adversaries(self):
        for mutation in ("foreign-vertex", "stderr", "duplicate", "extra-output", "future", "warning", "missing"):
            self.make(0); batch = self.batch(0); log = batch["logs"][0]
            if mutation == "foreign-vertex": log["vertex"] = batch["vertexes"][0]["digest"]
            elif mutation == "stderr": log["stream"] = 2
            elif mutation == "duplicate": batch["logs"].append(copy.deepcopy(log))
            elif mutation == "extra-output": log["data"] = base64.b64encode(base64.b64decode(log["data"]) + b"other\n").decode()
            elif mutation == "future": log["timestamp"] = SyntheticBuilder.stamp(5.1)
            elif mutation == "warning": batch["warnings"] = [{}]
            else: batch["logs"] = []
            self.raw.stream(5, "stderr", encode(batch))
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()

    def test_transcript_split_across_raw_log_frames_is_preserved(self):
        batch = self.batch(0)
        log = batch["logs"][0]
        raw = base64.b64decode(log["data"])
        middle = len(raw) // 2
        batch["logs"] = [dict(log, data=base64.b64encode(part).decode()) for part in (raw[:middle], raw[middle:])]
        self.raw.stream(5, "stderr", encode(batch))
        self.assertEqual(self.validate()["barrier"], self.transcript(0))

    def shift_client_clock(self, batch, seconds):
        for category in ("vertexes", "statuses", "logs"):
            for row in batch[category]:
                for key in ("started", "completed", "timestamp"):
                    if key in row:
                        row[key] = SyntheticBuilder.stamp((evidence.progress_ns(row[key]) - self.wall(0)) / 10**9 + seconds)
        for index in range(1, 10):
            self.raw.change(index, lambda row: row.update(started_unix_ns=row["started_unix_ns"] + seconds * 10**9), True)

    def test_disjoint_client_clocks_preserve_guest_overlap_and_copy_merge(self):
        proofs = []
        for slot in range(4):
            self.make(slot)
            batch = self.merged_batch(slot)
            self.shift_client_clock(batch, slot * 100)
            self.raw.stream(5, "stderr", encode(batch))
            proofs.append(self.validate())
        self.assertGreater(proofs[3]["run_interval"]["started_ns"], proofs[0]["run_interval"]["completed_ns"])
        group = evidence.validate_group(proofs)
        self.assertEqual(group["run_overlap"]["clock"], "guest-unix")
        self.assertGreater(group["run_overlap"]["duration_ns"], 0)
        for proof in proofs:
            raw, guest = proof["run_interval"], proof["guest_script_interval"]
            duration = raw["completed_ns"] - raw["started_ns"]
            self.assertEqual(proof["guest_run_envelope"], {
                "started_ns": guest["completed_ns"] - duration,
                "completed_ns": guest["started_ns"] + duration,
                "duration_ns": 2 * duration - guest["duration_ns"]})

    def test_clock_domain_and_envelope_adversaries(self):
        for mutation in ("client-window", "short-duration", "engine-envelope", "guest-past", "guest-future"):
            self.make(0)
            batch = self.batch(0)
            if mutation == "client-window":
                self.raw.change(5, lambda row: row.update(elapsed_ns=4 * 10**9))
            elif mutation == "short-duration":
                batch["vertexes"][3]["completed"] = SyntheticBuilder.stamp(4)
                batch["logs"][0]["timestamp"] = SyntheticBuilder.stamp(3.9)
            elif mutation == "engine-envelope":
                value = json.loads((self.directory / "command-00002.stdout").read_text())
                value["SystemTime"] = SyntheticBuilder.stamp(3)
                self.raw.stream(2, "stdout", encode(value))
            else:
                transcript = self.transcript(0)
                delta = (-20 if mutation == "guest-past" else 20) * 10**9
                for key in transcript:
                    if key.endswith("_unix_ns"): transcript[key] += delta
                for participant in transcript["participants"]:
                    for key in participant:
                        if key.endswith("_unix_ns"): participant[key] += delta
                for sample in transcript["samples"]: sample["unix_ns"] += delta
                transcript["generation_sha256"] = hashlib.sha256(json.dumps(transcript["participants"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                batch["logs"][0]["data"] = base64.b64encode(b"VZ_PARALLEL_BARRIER=" + encode(transcript)).decode()
            self.raw.stream(5, "stderr", encode(batch))
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()
        with self.assertRaisesRegex(evidence.Invalid, "client command clock bounds required"):
            evidence.parallel_progress(encode(self.batch(0)), self.inputs["images"]["base"]["reference"],
                                       self.operations[0], self.wall(0), self.wall(10))

    def test_operation_schema_and_export_substeps_fail_closed(self):
        for key, value in (("schema_version", True), ("slot", True), ("slot", 4), ("run_id", "foreign"),
                           ("output", str(self.flow / "foreign")), ("payload", {"path": "other", "size": 1, "sha256": "f" * 64})):
            self.make(0)
            self.operations[0][key] = value
            for name in ("operation.json", "operation.intent.json"): self.raw.write(name, self.operations[0])
            with self.subTest(key=key, value=value), self.assertRaises(evidence.Invalid): self.validate()
        for mutation in ("missing", "duplicate", "unfinished", "digest"):
            self.make(0); batch = self.batch(0)
            if mutation == "missing": batch["statuses"] = []
            elif mutation == "duplicate": batch["statuses"].append(copy.deepcopy(batch["statuses"][0]))
            elif mutation == "unfinished": batch["statuses"][0].pop("completed")
            else: batch["statuses"][1]["id"] = "exporting manifest sha256:" + "f" * 64
            self.raw.stream(5, "stderr", encode(batch))
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()

    def test_guard_fixture_inventory_and_projection_adversaries(self):
        for mutation in ("pid", "oom", "mount", "extra", "hash", "intent", "artifact", "raw"):
            self.make(0)
            if mutation in ("pid", "oom", "mount"):
                rows = json.loads((self.directory / "command-00009.stdout").read_text())
                if mutation == "pid": rows[0]["State"]["Pid"] += 1
                elif mutation == "oom": rows[0]["State"]["OOMKilled"] = True
                else: rows[0]["Mounts"] = []
                self.raw.stream(9, "stdout", encode(rows))
            elif mutation == "extra": self.raw.write("extra.json", {})
            elif mutation == "hash":
                self.operations[0]["parallel_fixture_sha256"] = "f" * 64
                for name in ("operation.json", "operation.intent.json"): self.raw.write(name, self.operations[0])
            elif mutation == "intent": self.raw.write("operation.intent.json", {})
            elif mutation == "artifact": self.raw.write("artifact-validation.json", {"oci": {}})
            else: (self.directory / "command-00005.stderr").write_bytes(b"changed")
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()

    def test_group_adversaries(self):
        proofs = []
        for slot in range(4):
            self.make(slot); proofs.append(self.validate())
        for mutation in ("missing", "slot", "builder", "pid", "engine", "run", "fixture", "digest", "overlap", "generation", "participants", "barrier-overlap"):
            rows = [copy.deepcopy(proof) for proof in proofs]; row = rows[-1]
            if mutation == "missing": rows.pop()
            elif mutation == "slot": row["slot"] = 0
            elif mutation == "builder": row["builder"]["container_id"] = "f" * 64
            elif mutation == "pid": row["builder_process"]["pid"] += 1
            elif mutation == "engine": row["scope"]["engine_id"] = "other"
            elif mutation == "run": row["run_id"] = "other"
            elif mutation == "fixture": row["parallel_fixture_sha256"] = "f" * 64
            elif mutation == "digest": row["run_interval"]["digest"] = rows[0]["run_interval"]["digest"]
            elif mutation == "overlap": row["guest_script_interval"]["started_ns"] = rows[0]["guest_script_interval"]["completed_ns"]
            elif mutation == "generation": row["barrier"]["generation_sha256"] = "f" * 64
            elif mutation == "participants": row["barrier"]["participants"][0]["run_id"] = "other"
            else: row["barrier"]["ready_monotonic_ns"] = rows[0]["barrier"]["released_monotonic_ns"]
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): evidence.validate_group(rows)


if __name__ == "__main__":
    unittest.main()
