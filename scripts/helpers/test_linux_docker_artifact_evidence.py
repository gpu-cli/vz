"""Synthetic raw receipt adversaries; no Docker invocation or physical claims."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import linux_docker_artifact_evidence as evidence
import test_linux_docker_build_evidence as fixtures
from test_linux_docker_build_evidence import SyntheticBuilder


def encoded(value):
    return (json.dumps(value, sort_keys=True) + "\n").encode()


class ArtifactEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.BuildEvidenceTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        fixtures.BuildEvidenceTests.tearDownClass()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="vz-artifact-raw-offline-")
        self.addCleanup(self.temp.cleanup)
        self.flow = Path(self.temp.name).resolve()
        self.inputs = copy.deepcopy(fixtures.BuildEvidenceTests.inputs)
        self.fixture = fixtures.BuildEvidenceTests.fixture
        self.inventory = {"schema_version": 1, "files": [{"path": "index.json", "sha256": "a" * 64}]}
        self.image = {"kind": "synthetic-oci", "manifest": {"digest": "sha256:" + "a" * 64}, "config": {"digest": "sha256:" + "b" * 64}}
        self.cache = {"kind": "synthetic-cache", "manifest": {"digest": "sha256:" + "c" * 64},
                      "config": {"digest": "sha256:" + "d" * 64}, "layers": [{"blob": "sha256:" + "e" * 64}]}
        self.image_mock = self.enterContext(patch.object(evidence.layout, "validate_oci", return_value=self.image))
        self.cache_mock = self.enterContext(patch.object(evidence.layout, "validate_cache", return_value=self.cache))
        self.tree_mock = self.enterContext(patch.object(evidence.stream, "inventory_tree", return_value=self.inventory))
        self.make("source-alpha")

    # unittest.TestCase.enterContext arrived after the supported system Python.
    def enterContext(self, manager):
        value = manager.__enter__()
        self.addCleanup(manager.__exit__, None, None, None)
        return value

    def make(self, operation):
        self.directory = self.flow / operation
        self.directory.mkdir(mode=0o700, exist_ok=True)
        role, variant, cache = evidence.OPERATIONS[operation]
        secret = variant is None
        payload = b"vz04-secret-mount-ok-v1\n" if secret else f"vz04-build-v1\nvariant={variant}\n".encode()
        self.operation = {"schema_version": 1, "operation": operation, "role": role,
            "output": str(self.directory / "oci"), "cache_output": str(self.directory / "cache") if cache else None,
            "cache_import": str(self.flow / "source-alpha/cache") if role == "importer" else None,
            "fixture_sha256": self.inputs["fixture_sha256"],
            "payload": {"path": "secret.txt" if secret else "payload.txt", "sha256": hashlib.sha256(payload).hexdigest(), "size": len(payload)},
            "cache_inventory_before": copy.deepcopy(self.inventory) if role == "importer" else None,
            "cache_inventory_after": copy.deepcopy(self.inventory) if role == "importer" else None}
        self.write("inputs.json", self.inputs)
        self.persist_operation()
        self.write("artifact-validation.json", {"oci": self.image, **({"cache": self.cache} if cache else {})})
        self.write("compose-owner.json", {})
        (self.directory / "private-tmp").mkdir(mode=0o700, exist_ok=True)
        (self.directory / "oci").mkdir(mode=0o700, exist_ok=True)
        if cache:
            (self.directory / "cache").mkdir(mode=0o700, exist_ok=True)
        base = fixtures.BuildEvidenceTests.base
        for index, original in enumerate((5, 6, 7, 8, 9, 10, 11, 12, 13), 1):
            old = f"command-{original:05d}"
            row = json.loads((base / (old + ".json")).read_text())
            intent = json.loads((base / (old + ".intent.json")).read_text())
            stem = f"command-{index:05d}"
            row.update(index=index, stdout=stem + ".stdout", stderr=stem + ".stderr")
            intent["index"] = index
            stdout, stderr = ((base / (old + "." + stream)).read_bytes() for stream in ("stdout", "stderr"))
            if index in (4, 9):
                item = json.loads(stdout)[0]
                item["State"]["OOMKilled"] = False
                item["Mounts"] = [{"Name": "owned-volume", "Destination": "/var/lib/buildkit"}]
                stdout = encoded([item])
            if index == 5:
                argv = ["docker", "--config", self.inputs["docker_config"], "--context", self.inputs["scope"]["docker_context"],
                        "buildx", "build", "--builder", self.inputs["builder"]["name"], "--platform", "linux/arm64", "--progress", "rawjson",
                        "--file", str(self.fixture / "build" / ("Dockerfile.secret" if secret else "Dockerfile")),
                        "--provenance=false", "--sbom=false", "--output", "type=oci,dest=" + self.operation["output"] +
                        ",tar=false,oci-mediatypes=true,compression=gzip,force-compression=true", "--build-arg",
                        "FIXTURE_BASE=" + self.inputs["images"]["base"]["reference"]]
                spec = json.loads((self.fixture / "fixture.json").read_text())
                if secret:
                    argv += ["--build-arg", "FIXTURE_SECRET_SHA256=" + spec["secret_input_sha256"]]
                else:
                    argv += ["--build-arg", "FIXTURE_RUN=" + self.inputs["run_id"], "--build-arg", "FIXTURE_VARIANT=" + variant]
                if cache:
                    argv += ["--cache-to", "type=local,dest=" + self.operation["cache_output"] +
                             ",mode=max,image-manifest=true,oci-mediatypes=true,compression=gzip,force-compression=true"]
                if role == "importer":
                    argv += ["--cache-from", "type=local,src=" + self.operation["cache_import"]]
                if secret:
                    argv += ["--no-cache", "--secret", "id=fixture,src=" + str(self.fixture / "inputs/secret.txt")]
                argv += [str(self.fixture / "build")]
                row["argv"] = intent["argv"] = argv
                stderr = self.progress(secret, role == "importer", cache)
            self.write(stem + ".json", row)
            self.write(stem + ".intent.json", intent)
            self.stream(index, "stdout", stdout)
            self.stream(index, "stderr", stderr)

    def write(self, name, value):
        (self.directory / name).write_bytes(encoded(value))

    def persist_operation(self):
        self.write("operation.json", self.operation)
        self.write("operation.intent.json", dict(self.operation, cache_inventory_after=None))

    def stream(self, index, name, raw):
        stem = f"command-{index:05d}"
        (self.directory / (stem + "." + name)).write_bytes(raw)
        p = self.directory / (stem + ".json")
        row = json.loads(p.read_text())
        for field in (name + "_sha256", "raw_" + name + "_sha256", "retained_observed_" + name + "_sha256"):
            row[field] = hashlib.sha256(raw).hexdigest()
        row["observed_bytes"][name] = row["retained_observed_" + name + "_bytes"] = len(raw)
        self.write(stem + ".json", row)

    def change(self, index, fn, reseal_intent=False):
        stem = f"command-{index:05d}"
        row = json.loads((self.directory / (stem + ".json")).read_text())
        fn(row)
        self.write(stem + ".json", row)
        if reseal_intent:
            intent = json.loads((self.directory / (stem + ".intent.json")).read_text())
            for key in ("index", "executable", "argv", "argv0", "environment", "started_unix_ns", "mutation", "max_stream_bytes"):
                intent[key] = row[key]
            self.write(stem + ".intent.json", intent)

    def progress(self, secret, cached, cache_export):
        if not secret:
            batch = json.loads((fixtures.BuildEvidenceTests.base / "command-00009.stderr").read_text())
            for v in batch["vertexes"]:
                if any(x in v["name"] for x in (" COPY ", " RUN ")):
                    v["cached"] = cached
            if cached:
                batch.pop("logs", None)
        else:
            names = ["[build 1/4] FROM " + self.inputs["images"]["base"]["reference"], "[internal] load build context",
                     "[build 2/4] COPY tools.py /fixture/tools.py",
                     "[build 3/4] RUN --network=none --mount=type=secret,id=fixture,required=true python3 /fixture/tools.py secret",
                     "[build 4/4] RUN --network=none test ! -e /run/secrets/fixture",
                     "[output 1/1] COPY --from=build /out/secret.txt /secret.txt"]
            ids = ["sha256:" + hashlib.sha256(x.encode()).hexdigest() for x in names]
            edges = ([], [], ids[:2], [ids[2]], [ids[3]], [ids[4]])
            times = ((1, 2), (1, 2), (2, 3), (3, 4), (4, 4.5), (4.5, 5))
            batch = {"vertexes": [{"digest": identity, "name": name, "inputs": edge, "cached": False,
                                   "started": SyntheticBuilder.stamp(a), "completed": SyntheticBuilder.stamp(b)}
                                  for identity, name, edge, (a, b) in zip(ids, names, edges, times)]}
        extra = [(evidence.OCI_EXPORT, 6, 7)]
        if cache_export:
            extra.append((evidence.CACHE_EXPORT, 7, 8))
        if cached:
            extra.append(("importing cache manifest from local:123", 0.5, 1))
        for name, start, end in extra:
            identity = "sha256:" + hashlib.sha256(name.encode()).hexdigest()
            batch["vertexes"].append({"digest": identity, "name": name,
                                     "started": SyntheticBuilder.stamp(start), "completed": SyntheticBuilder.stamp(end)})
            if name == evidence.OCI_EXPORT:
                statuses = ["exporting layers", "exporting manifest " + self.image["manifest"]["digest"],
                            "exporting config " + self.image["config"]["digest"]]
            elif name == evidence.CACHE_EXPORT:
                statuses = ["preparing build cache for export", "writing layer " + self.cache["layers"][0]["blob"],
                            "writing config " + self.cache["config"]["digest"],
                            "writing cache image manifest " + self.cache["manifest"]["digest"]]
            else:
                statuses = []
            batch.setdefault("statuses", []).extend({"id": status, "vertex": identity,
                "started": SyntheticBuilder.stamp(start), "completed": SyntheticBuilder.stamp(end)} for status in statuses)
        return encoded(batch)

    def validate(self):
        return evidence.validate(self.directory, self.inputs, self.operation)

    def test_all_five_exact_operations(self):
        for op in evidence.OPERATIONS:
            if op != "source-alpha":
                self.make(op)
            result = self.validate()
            self.assertEqual(result["command_count"], 9)
            self.assertFalse(result["compatibility_certified"])
        self.assertEqual(self.image_mock.call_count, 5)
        self.assertEqual(self.cache_mock.call_count, 3)

    def test_command_flag_mutations_resealed_reject(self):
        for extra in (["--push"], ["--load"], ["--no-cache"], ["--cache-from", "type=registry,ref=foreign"],
                      ["--build-arg", "FIXTURE_RUN=foreign"], ["--allow", "security.insecure"]):
            self.make("source-alpha")
            self.change(5, lambda row: row["argv"].__setitem__(slice(-1, -1), extra), True)
            with self.subTest(extra=extra), self.assertRaises(evidence.Invalid):
                self.validate()

    def test_incomplete_foreign_and_uncertain_command_reject(self):
        for key, value in (("capture_complete", False), ("effects_uncertain", True), ("exit_code", 1),
                           ("timed_out", True), ("interrupted", True), ("output_limit_exceeded", True),
                           ("environment", {"DOCKER_HOST": "foreign"}), ("executable", "/foreign/docker"),
                           ("mutation", False), ("started_unix_ns", -1)):
            self.make("source-alpha")
            self.change(5, lambda row: row.update({key: value}), True)
            with self.subTest(key=key), self.assertRaises(evidence.Invalid):
                self.validate()

    def test_extra_or_missing_receipts_reject(self):
        self.write("command-00010.json", {})
        with self.assertRaises(evidence.Invalid): self.validate()
        (self.directory / "command-00010.json").unlink()
        (self.directory / "command-00005.intent.json").unlink()
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_operation_expectation_resealed_reject(self):
        for key, value in (("schema_version", True), ("role", "importer"), ("output", str(self.flow / "foreign")),
                           ("cache_import", str(self.flow / "foreign")), ("fixture_sha256", "f" * 64),
                           ("payload", {"path": "payload.txt", "sha256": "f" * 64, "size": 1})):
            self.make("source-alpha"); self.operation[key] = value; self.persist_operation()
            with self.subTest(key=key), self.assertRaises(evidence.Invalid): self.validate()

    def test_progress_future_past_error_and_duplicate_reject(self):
        for mutation in ("future", "past", "error", "duplicate", "foreign", "missing-export"):
            self.make("source-alpha")
            batch = json.loads((self.directory / "command-00005.stderr").read_text())
            if mutation == "future": batch["vertexes"][-1]["completed"] = SyntheticBuilder.stamp(11)
            elif mutation == "past": batch["vertexes"][1]["started"] = SyntheticBuilder.stamp(-1)
            elif mutation == "error": batch["vertexes"][-1]["error"] = "failed"
            elif mutation == "duplicate": batch["vertexes"].append(batch["vertexes"][-1])
            elif mutation == "foreign": batch["vertexes"][-1]["name"] = "exporting cache to registry"
            else: batch["vertexes"] = [v for v in batch["vertexes"] if v["name"] != evidence.OCI_EXPORT]
            self.stream(5, "stderr", encoded(batch))
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()

    def test_future_unfinished_source_reject(self):
        batch = json.loads((self.directory / "command-00005.stderr").read_text())
        source = copy.deepcopy(next(v for v in batch["vertexes"] if v["name"] == "[internal] load build context"))
        source.pop("completed"); source["started"] = SyntheticBuilder.stamp(11); batch["vertexes"].append(source)
        self.stream(5, "stderr", encoded(batch))
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_lifetime_mount_oom_and_engine_drift_reject(self):
        for index, field in ((9, "pid"), (9, "mount"), (9, "oom"), (7, "engine")):
            self.make("source-alpha")
            item = json.loads((self.directory / f"command-{index:05d}.stdout").read_text())
            if field == "pid": item[0]["State"]["Pid"] += 1
            elif field == "mount": item[0]["Mounts"][0]["Name"] = "other-volume"
            elif field == "oom": item[0]["State"]["OOMKilled"] = True
            else: item["ID"] = "foreign-engine"
            self.stream(index, "stdout", encoded(item))
            with self.subTest(field=field), self.assertRaises(evidence.Invalid): self.validate()

    def test_import_cache_must_remain_exact(self):
        self.make("fresh-import-alpha")
        self.operation["cache_inventory_after"] = {"foreign": True}; self.persist_operation()
        with self.assertRaises(evidence.Invalid): self.validate()
        self.operation["cache_inventory_after"] = copy.deepcopy(self.inventory); self.persist_operation()
        self.tree_mock.return_value = {"changed": True}
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_cold_control_cached_and_importer_uncached_reject(self):
        for op, cached in (("fresh-cold-alpha", True), ("fresh-import-alpha", False)):
            self.make(op)
            self.stream(5, "stderr", self.progress(False, cached, False))
            with self.subTest(op=op), self.assertRaises(evidence.Invalid): self.validate()

    def test_layout_rejection_propagates(self):
        self.image_mock.side_effect = evidence.layout.LayoutError("invalid_oci")
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_raw_hash_and_intent_mismatch_reject(self):
        (self.directory / "command-00005.stderr").write_bytes(b"different")
        with self.assertRaises(evidence.Invalid): self.validate()
        self.make("source-alpha")
        self.write("operation.intent.json", {})
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_secret_canary_never_echoed_by_exception(self):
        canary = (self.fixture / "inputs/secret.txt").read_bytes()
        self.stream(5, "stdout", canary)
        with self.assertRaises(evidence.Invalid) as caught: self.validate()
        self.assertNotIn(canary.decode().strip(), str(caught.exception))

    def test_unexplained_top_level_file_reject(self):
        self.write("ignored-evidence.json", {})
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_artifact_projection_must_match_actual(self):
        self.write("artifact-validation.json", {"oci": {"forged": True}, "cache": self.cache})
        with self.assertRaises(evidence.Invalid): self.validate()

    def test_oci_receives_secret_and_intermediate_canaries_but_cache_only_secret(self):
        self.validate()
        middle = (self.fixture / "build/intermediate-canary.txt").read_bytes()
        self.assertIn(middle.rstrip(b"\n"), self.image_mock.call_args.kwargs["canaries"])
        self.assertNotIn(middle.rstrip(b"\n"), self.cache_mock.call_args.kwargs["canaries"])

    def test_export_status_missing_unfinished_wrong_digest_or_duplicate_reject(self):
        for mutation in ("absent", "unfinished", "digest", "duplicate", "future"):
            self.make("source-alpha")
            batch = json.loads((self.directory / "command-00005.stderr").read_text())
            if mutation == "absent": batch["statuses"] = []
            elif mutation == "unfinished": batch["statuses"][0].pop("completed")
            elif mutation == "digest": batch["statuses"][1]["id"] = "exporting manifest sha256:" + "f" * 64
            elif mutation == "duplicate": batch["statuses"].append(batch["statuses"][0])
            else: batch["statuses"][0]["completed"] = SyntheticBuilder.stamp(9)
            self.stream(5, "stderr", encoded(batch))
            with self.subTest(mutation=mutation), self.assertRaises(evidence.Invalid): self.validate()
