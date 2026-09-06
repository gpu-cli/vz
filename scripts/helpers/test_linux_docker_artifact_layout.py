"""Real local OCI/cache layouts and fully resealed adversarial graph fixtures."""
import copy
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

import linux_docker_artifact_layout as layout
import linux_docker_artifact_stream as stream


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def layer_bytes(payload=b"alpha", extra=(), payload_options=None):
    output = io.BytesIO()
    entries = [("payload.txt", payload, payload_options or {})] + list(extra)
    with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name, contents, options in entries:
            info = tarfile.TarInfo(name)
            info.mode = 0o644
            info.size = len(contents)
            for key, value in options.items():
                setattr(info, key, value)
            archive.addfile(info, io.BytesIO(contents))
    raw = output.getvalue()
    return gzip.compress(raw, mtime=0), "sha256:" + sha(raw)


class Fixture:
    def __init__(self, root, *, cache=False, payload=b"alpha", extra=(), payload_options=None):
        self.root = root
        self.blobs = {}
        compressed, diff_id = layer_bytes(payload, extra, payload_options)
        self.layer = self.blob(compressed, layout.LAYER)
        self.layer["annotations"] = {layout.UNCOMPRESSED: diff_id}
        self.layers = [self.layer]
        self.cache = cache
        if cache:
            self.config = {"layers": [{"blob": self.layer["digest"], "parent": -1}],
                           "records": [{"digest": "sha256:" + sha(b"record"), "layers": [{"layer": 0}]}]}
        else:
            self.config = {"architecture": "arm64", "os": "linux", "config": {"WorkingDir": "/"},
                           "rootfs": {"type": "layers", "diff_ids": [diff_id]},
                           "history": [{"created": "2026-09-06T00:00:00Z", "created_by": "COPY payload"}]}
        self.manifest_changes = {}
        self.index_descriptor_changes = {}
        self.index_changes = {}

    def blob(self, data, media_type):
        digest = "sha256:" + sha(data)
        self.blobs[digest[7:]] = data
        return {"mediaType": media_type, "digest": digest, "size": len(data)}

    def write(self):
        config = self.blob(encoded(self.config), layout.CACHE_CONFIG if self.cache else layout.CONFIG)
        manifest = {"schemaVersion": 2, "mediaType": layout.MANIFEST, "config": config, "layers": self.layers}
        manifest.update(self.manifest_changes)
        top = self.blob(encoded(manifest), layout.MANIFEST)
        top["annotations"] = {"org.opencontainers.image.ref.name": "latest"}
        top.update(self.index_descriptor_changes)
        index = {"schemaVersion": 2, "mediaType": layout.INDEX, "manifests": [top]}
        index.update(self.index_changes)
        (self.root / "blobs/sha256").mkdir(parents=True)
        (self.root / "ingest").mkdir()
        for digest, data in self.blobs.items():
            (self.root / "blobs/sha256" / digest).write_bytes(data)
        (self.root / "oci-layout").write_bytes(encoded({"imageLayoutVersion": "1.0.0"}))
        (self.root / "index.json").write_bytes(encoded(index))
        return self.root


class ArtifactLayoutTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="vz-artifact-layout-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve() / "export"

    def oci(self, root=None, **kwargs):
        return layout.validate_oci(root or self.root, expected_path="payload.txt",
                                   expected_sha256=sha(b"alpha"), expected_size=5, **kwargs)

    def rejected(self, callback, code=None):
        with self.assertRaises((layout.LayoutError, stream.ArtifactError)) as caught:
            callback()
        if code is not None:
            self.assertEqual(str(caught.exception), code)

    def test_oci_exact_scratch_and_config_diff_id(self):
        fixture = Fixture(self.root)
        fixture.write()
        proof = self.oci()
        self.assertEqual(proof["kind"], "oci-scratch")
        self.assertEqual(proof["payload"]["sha256"], sha(b"alpha"))
        self.assertEqual(proof["layer"]["diff_id"], fixture.config["rootfs"]["diff_ids"][0])
        self.assertEqual(len(proof["inventory"]["files"]), 5)

    def test_alpha_beta_layers_differ(self):
        Fixture(self.root).write()
        first = self.oci()
        other = self.root.parent / "beta"
        Fixture(other, payload=b"beta").write()
        second = layout.validate_oci(other, expected_path="payload.txt", expected_sha256=sha(b"beta"), expected_size=4)
        self.assertNotEqual(first["layer"]["descriptor"]["digest"], second["layer"]["descriptor"]["digest"])
        self.assertNotEqual(first["payload"]["sha256"], second["payload"]["sha256"])

    def test_optional_root_directory(self):
        Fixture(self.root, extra=[(".", b"", {"type": tarfile.DIRTYPE, "mode": 0o755})]).write()
        self.oci()

    def test_oci_intermediate_file_exclusion(self):
        Fixture(self.root, extra=[("intermediate", b"hidden", {})]).write()
        self.rejected(self.oci, "unexpected_scratch_members")

    def test_oci_payload_owner_mode_digest_size_link_rejected(self):
        cases = [{"uid": 1}, {"gid": 1}, {"mode": 0o755},
                 {"type": tarfile.SYMTYPE, "linkname": "/outside", "size": 0}]
        for number, options in enumerate(cases):
            root = self.root.parent / str(number)
            Fixture(root, payload_options=options).write()
            with self.subTest(number=number):
                self.rejected(lambda: self.oci(root))
        Fixture(self.root, payload=b"bravo").write()
        self.rejected(self.oci, "unexpected_scratch_payload")

    def test_oci_platform_history_config_and_diffid_drift(self):
        mutations = [lambda f: f.config.update(os="windows"),
                     lambda f: f.config.update(architecture="amd64"),
                     lambda f: f.config["rootfs"].update(diff_ids=["sha256:" + "0" * 64]),
                     lambda f: f.config["history"].append({"created_by": "extra"}),
                     lambda f: f.config["history"][0].update(empty_layer="false"),
                     lambda f: f.config.update(unknown="semantic"),
                     lambda f: f.config["config"].update(Env="wrong"),
                     lambda f: f.config.update(created="invalid")]
        for number, mutation in enumerate(mutations):
            root = self.root.parent / str(number)
            fixture = Fixture(root)
            mutation(fixture)
            fixture.write()
            with self.subTest(number=number):
                self.rejected(lambda: self.oci(root))

    def test_config_annotation_must_match(self):
        fixture = Fixture(self.root)
        fixture.index_descriptor_changes["annotations"] = {
            "org.opencontainers.image.ref.name": "latest", "config.digest": "sha256:" + "0" * 64}
        fixture.write()
        self.rejected(self.oci, "config_annotation_mismatch")

    def test_descriptor_size_hash_missing_and_unreachable_blob(self):
        for number, kind in enumerate(("size", "hash", "missing", "extra")):
            root = self.root.parent / str(number)
            fixture = Fixture(root)
            if kind == "size":
                fixture.layer["size"] += 1
            fixture.write()
            selected = root / "blobs/sha256" / fixture.layer["digest"][7:]
            if kind == "hash":
                data = bytearray(selected.read_bytes())
                data[0] ^= 1
                selected.write_bytes(data)
            elif kind == "missing":
                selected.unlink()
            elif kind == "extra":
                (root / "blobs/sha256" / sha(b"extra")).write_bytes(b"extra")
            with self.subTest(kind=kind):
                self.rejected(lambda: self.oci(root))

    def test_external_descriptor_and_attestation_fields_rejected(self):
        for number, mutation in enumerate(({"urls": ["https://example.invalid/blob"]},
                                           {"data": "eA=="}, {"artifactType": "attestation"})):
            root = self.root.parent / str(number)
            fixture = Fixture(root)
            fixture.layer.update(mutation)
            fixture.write()
            with self.subTest(number=number):
                self.rejected(lambda: self.oci(root), "unsupported_object_fields")
        fixture = Fixture(self.root)
        fixture.manifest_changes["subject"] = copy.deepcopy(fixture.layer)
        fixture.write()
        self.rejected(self.oci, "unsupported_object_fields")

    def test_index_must_be_single_latest_image_manifest(self):
        for number, mutation in enumerate((lambda f: f.index_changes.update(manifests=[]),
                lambda f: f.index_descriptor_changes.update(mediaType=layout.INDEX),
                lambda f: f.index_descriptor_changes.update(annotations={}),
                lambda f: f.index_descriptor_changes.update(platform={"architecture": "unknown", "os": "unknown"}))):
            root = self.root.parent / str(number)
            fixture = Fixture(root)
            mutation(fixture)
            fixture.write()
            with self.subTest(number=number):
                self.rejected(lambda: self.oci(root))

    def test_complete_export_tree_no_unknown_directory_file_or_link(self):
        for number, kind in enumerate(("directory", "file", "symlink", "hardlink")):
            root = self.root.parent / str(number)
            fixture = Fixture(root)
            fixture.write()
            if kind == "directory":
                (root / "unexpected").mkdir()
            elif kind == "file":
                (root / "index.json.lock").write_bytes(b"")
            elif kind == "symlink":
                (root / "link").symlink_to(root / "index.json")
            else:
                import os
                os.link(root / "index.json", root / "hard")
            with self.subTest(kind=kind):
                self.rejected(lambda: self.oci(root))

    def test_ingest_must_be_empty_not_incomplete_transaction(self):
        Fixture(self.root).write()
        (self.root / "ingest/transaction").mkdir()
        (self.root / "ingest/transaction/data").write_bytes(b"unfinished")
        self.rejected(self.oci, "unexpected_layout_directories")

    def test_duplicate_nonfinite_invalid_and_deep_json_rejected(self):
        samples = [b'{"imageLayoutVersion":"1.0.0","imageLayoutVersion":"1.0.0"}',
                   b'{"x":NaN}', b'{"x":1e999}', b'{', b'[' * 100 + b'0' + b']' * 100,
                   b'{"x":"\\ud800"}']
        for number, data in enumerate(samples):
            root = self.root.parent / str(number)
            Fixture(root).write()
            (root / "oci-layout").write_bytes(data)
            with self.subTest(number=number):
                self.rejected(lambda: self.oci(root))

    def test_json_size_bound_without_global_reader_change(self):
        Fixture(self.root).write()
        (self.root / "oci-layout").write_bytes(b" " * (layout.JSON_BYTES + 1))
        self.rejected(self.oci, "json_size_or_inventory")

    def test_canary_raw_decoded_metadata_and_compressed_cache_layer(self):
        secret = b"private-canary-value"
        for number, kind in enumerate(("raw", "escaped", "layer")):
            root = self.root.parent / str(number)
            fixture = Fixture(root, cache=kind == "layer", payload=secret if kind == "layer" else b"alpha")
            if kind != "layer":
                fixture.config["config"]["Labels"] = {"note": secret.decode()}
                if kind == "escaped":
                    original = fixture.blob

                    def escaped(data, media_type):
                        if media_type == layout.CONFIG:
                            data = data.replace(secret, b"".join(('\\u%04x' % byte).encode() for byte in secret))
                        return original(data, media_type)

                    fixture.blob = escaped
            fixture.write()
            callback = (lambda: layout.validate_cache(root, canaries=[secret])) if kind == "layer" else (lambda: self.oci(root, canaries=[secret]))
            with self.subTest(kind=kind):
                self.rejected(callback, "secret_canary_detected")

    def test_cache_minimal_real_layout(self):
        fixture = Fixture(self.root, cache=True)
        fixture.write()
        proof = layout.validate_cache(self.root)
        self.assertEqual(proof["kind"], "buildkit-cache-v0")
        self.assertEqual(proof["layers"][0]["parent"], -1)
        self.assertEqual(len(proof["unique_blobs"]), 1)

    def graph_fixture(self, root):
        fixture = Fixture(root, cache=True)
        compressed, diff_id = layer_bytes(b"beta")
        second = fixture.blob(compressed, layout.LAYER)
        second["annotations"] = {layout.UNCOMPRESSED: diff_id}
        fixture.layers = [fixture.layer, second, copy.deepcopy(fixture.layer)]
        fixture.config["layers"] = [{"blob": fixture.layer["digest"], "parent": 1},
                                    {"blob": second["digest"], "parent": -1},
                                    {"blob": fixture.layer["digest"], "parent": -1}]
        fixture.config["records"] = [
            {"digest": "sha256:" + sha(b"r0"), "inputs": [[{"link": 1, "selector": ""}]], "layers": [{"layer": 0}]},
            {"digest": "sha256:" + sha(b"r1"), "inputs": [None], "chains": [{"layers": [2]}]}]
        return fixture

    def test_cache_forward_parents_records_duplicate_blobs_and_chain(self):
        self.graph_fixture(self.root).write()
        original = stream.scan_layer
        with mock.patch.object(stream, "scan_layer", wraps=original) as scan:
            proof = layout.validate_cache(self.root)
        self.assertEqual(scan.call_count, 2)
        self.assertEqual([item["parent"] for item in proof["layers"]], [1, -1, -1])
        self.assertEqual(len(proof["unique_blobs"]), 2)

    def test_cache_omitted_parent_means_zero(self):
        fixture = self.graph_fixture(self.root)
        fixture.config["layers"][0]["parent"] = -1
        del fixture.config["layers"][1]["parent"]
        fixture.config["records"][0]["layers"][0]["layer"] = 1
        fixture.write()
        self.assertEqual(layout.validate_cache(self.root)["layers"][1]["parent"], 0)

    def test_cache_omitted_first_parent_is_cycle_not_root(self):
        fixture = Fixture(self.root, cache=True)
        del fixture.config["layers"][0]["parent"]
        fixture.write()
        self.rejected(lambda: layout.validate_cache(self.root), "cache_graph_cycle")

    def test_resealed_cache_parent_record_result_cycles_and_bounds(self):
        mutations = [lambda f: f.config["layers"][1].update(parent=0),
                     lambda f: f.config["layers"][0].update(parent=99),
                     lambda f: f.config["layers"][0].update(parent=True),
                     lambda f: f.config["records"][1].update(inputs=[[{"link": 0}]]),
                     lambda f: f.config["records"][0]["inputs"][0][0].update(link=99),
                     lambda f: f.config["records"][0]["layers"][0].update(layer=-1),
                     lambda f: f.config["records"][1]["chains"][0].update(layers=[99]),
                     lambda f: f.config["records"][1]["chains"][0].update(layers=[])]
        for number, mutation in enumerate(mutations):
            root = self.root.parent / str(number)
            fixture = self.graph_fixture(root)
            mutation(fixture)
            fixture.write()
            with self.subTest(number=number):
                self.rejected(lambda: layout.validate_cache(root))

    def test_cache_annotations_order_diffid_and_unused_layer(self):
        mutations = [lambda f: f.layer["annotations"].update({layout.UNCOMPRESSED: "sha256:" + "0" * 64}),
                     lambda f: f.layer.update(annotations={}),
                     lambda f: f.config["layers"][0].update(annotations={"size": 1}),
                     lambda f: f.config["layers"][0].update(annotations={"diffID": "sha256:" + "0" * 64}),
                     lambda f: f.config["layers"].append(f.config["layers"].pop(0)),
                     lambda f: f.config["records"][1].update(chains=[])]
        for number, mutation in enumerate(mutations):
            root = self.root.parent / str(number)
            fixture = self.graph_fixture(root)
            mutation(fixture)
            fixture.write()
            with self.subTest(number=number):
                self.rejected(lambda: layout.validate_cache(root))

    def test_cache_consistent_optional_annotations(self):
        fixture = Fixture(self.root, cache=True)
        fixture.config["layers"][0]["annotations"] = {
            "mediaType": layout.LAYER, "diffID": fixture.layer["annotations"][layout.UNCOMPRESSED],
            "size": fixture.layer["size"], "createdAt": "0001-01-01T00:00:00Z"}
        fixture.write()
        layout.validate_cache(self.root)

    def test_go_nanosecond_times_under_python39(self):
        fixture = Fixture(self.root)
        fixture.config["created"] = "2026-09-06T00:00:00.123456789Z"
        fixture.config["history"][0]["created"] = "2026-09-06T00:00:00.123456789+00:00"
        fixture.write()
        self.oci()

    def test_final_inventory_detects_post_scan_file_drift(self):
        fixture = Fixture(self.root)
        fixture.write()
        original = stream.scan_layer

        def drift(*args, **kwargs):
            result = original(*args, **kwargs)
            (self.root / "index.json").write_bytes(b"{}")
            return result

        with mock.patch.object(stream, "scan_layer", side_effect=drift):
            self.rejected(self.oci, "layout_changed")


if __name__ == "__main__":
    unittest.main()
