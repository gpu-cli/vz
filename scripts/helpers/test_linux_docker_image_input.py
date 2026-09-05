"""Adversarial offline tests; no registry, Docker, VM, or credential access."""

import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

import linux_docker_image_input as image_input


FIXTURE = Path(__file__).resolve().parents[2] / "tests/fixtures/vz-0.4/docker/python-image-input.json"


class ImageInputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="vz-python-input-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / "input.json"
        self.value = json.loads(FIXTURE.read_text())

    def write(self, value):
        self.path.write_text(json.dumps(value))
        return self.path

    def reject(self, value):
        with self.assertRaises((ValueError, OSError)):
            image_input.load(self.write(value))

    @staticmethod
    def blob(value, name):
        return json.loads(base64.b64decode(value["metadata"][name]["data"]))

    @staticmethod
    def encode(value, name, raw):
        value["metadata"][name] = {
            "encoding": "base64", "data": base64.b64encode(raw).decode(),
            "size": len(raw), "digest": "sha256:" + hashlib.sha256(raw).hexdigest()}

    def rebind(self, value, name, content):
        """Keep digest links coherent so semantic negative tests reach their checks."""
        self.encode(value, name, json.dumps(content).encode())
        if name == "config":
            manifest = self.blob(value, "manifest")
            manifest["config"].update({key: value["metadata"][name][key] for key in ("digest", "size")})
            self.rebind(value, "manifest", manifest)
        elif name == "manifest":
            index = self.blob(value, "index")
            selected = next(item for item in index["manifests"] if item.get("platform") == image_input.PLATFORM)
            selected.update({key: value["metadata"][name][key] for key in ("digest", "size")})
            value["reference"] = value["repository"] + "@" + selected["digest"]
            self.rebind(value, "index", index)

    def test_checked_in_raw_pins_and_return_contract(self):
        result = image_input.load(FIXTURE)
        self.assertEqual(result["reference"], "docker.io/library/python@sha256:d04f49f5882f49a3b91f874e75e19f0c265f7222da8659741a9d7eab148f22a9")
        self.assertEqual(result["id"], "sha256:d04f49f5882f49a3b91f874e75e19f0c265f7222da8659741a9d7eab148f22a9")
        self.assertEqual(result["config_digest"], "sha256:a74ddf067736251b63204a4d7a26411cffa3da75600b4e67ad54c9c79f3f81cb")
        self.assertNotEqual(result["id"], result["config_digest"])
        self.assertEqual(result["id"], result["reference"].split("@", 1)[1])
        self.assertEqual(result["manifest_descriptor"], {
            "mediaType": image_input.MANIFEST, "digest": result["id"], "size": 1754})
        self.assertEqual(result["platform_detail"], image_input.PLATFORM)
        config = self.blob(self.value, "config")
        self.assertEqual(result["image_config"], config["config"])
        self.assertEqual(result["rootfs"], config["rootfs"])
        self.assertEqual(result["metadata_hashes"]["index"], "sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254")
        self.assertEqual(result["platform"], "linux/arm64")
        self.assertEqual(result["python_version"], "3.12.14")
        self.assertEqual(len(result["layers"]), 4)
        self.assertEqual([self.value["metadata"][name]["size"] for name in ("index", "manifest", "config")],
                         [6546, 1754, 5693])

    def test_config_projection_is_bound_through_the_selected_manifest(self):
        value = copy.deepcopy(self.value)
        config = self.blob(value, "config")
        config["config"]["Env"].append("FIXTURE_INPUT=bound")
        config["config"]["WorkingDir"] = "/fixture"
        self.rebind(value, "config", config)
        result = image_input.load(self.write(value))
        manifest = self.blob(value, "manifest")
        self.assertEqual(result["id"], value["metadata"]["manifest"]["digest"])
        self.assertEqual(result["config_digest"], manifest["config"]["digest"])
        self.assertNotEqual(result["id"], self.value["metadata"]["manifest"]["digest"])
        self.assertNotEqual(result["config_digest"], self.value["metadata"]["config"]["digest"])
        self.assertEqual(result["image_config"], config["config"])
        self.assertEqual(result["rootfs"], config["rootfs"])
        self.assertEqual(result["manifest_descriptor"]["digest"], result["id"])

    def test_raw_tamper_size_and_encoding_fail(self):
        for name in ("index", "manifest", "config"):
            for field, changed in (("data", "e30="), ("data", "not base64!"),
                                   ("size", True), ("size", 0), ("size", 1000000),
                                   ("digest", "sha256:" + "a" * 64), ("encoding", "path")):
                with self.subTest(name=name, field=field, changed=changed):
                    value = copy.deepcopy(self.value)
                    value["metadata"][name][field] = changed
                    self.reject(value)
        value = copy.deepcopy(self.value)
        value["metadata"]["index"]["data"] += "\n"
        self.reject(value)

    def test_mutable_reference_index_pin_and_scope_overclaims_fail(self):
        for field, changed in (("reference", self.value["source_tag"]),
                               ("reference", self.value["repository"] + "@" + self.value["metadata"]["index"]["digest"]),
                               ("reference", self.value["repository"] + "@" + self.value["metadata"]["config"]["digest"]),
                               ("compatibility_certified", True), ("state", "runtime_verified"),
                               ("role", "FIXTURE_SSH_BASE"), ("platform", {"os": "linux", "architecture": "amd64"}),
                               ("repository", "foreign.example/python"), ("schema_version", True)):
            with self.subTest(field=field):
                self.reject(self.value | {field: changed})
        self.reject(self.value | {"archive_path": "../foreign"})

    def test_input_file_and_parent_redirects_are_rejected(self):
        original = self.root / "original.json"
        original.write_text(json.dumps(self.value))
        self.path.symlink_to(original)
        with self.assertRaises(OSError):
            image_input.load(self.path)
        self.path.unlink()
        os.link(original, self.path)
        with self.assertRaises(ValueError):
            image_input.load(self.path)
        self.path.unlink()
        link = self.root / "redirect"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            image_input.load(link / original.name)
        with self.assertRaises(ValueError):
            image_input.load(self.root / "unused/../original.json")

    def test_oversized_fifo_and_duplicate_json_are_rejected(self):
        self.path.write_bytes(b" " * (image_input.MAX_INPUT + 1))
        with self.assertRaises(ValueError):
            image_input.load(self.path)
        self.path.unlink()
        os.mkfifo(self.path)
        with self.assertRaises(ValueError):
            image_input.load(self.path)
        self.path.unlink()
        self.path.write_text('{"schema_version":1,"schema_version":1}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            image_input.load(self.path)
        value = copy.deepcopy(self.value)
        self.encode(value, "index", b'{"schemaVersion":2,"schemaVersion":2}')
        self.reject(value)

    def test_wrong_or_ambiguous_index_platform_is_rejected(self):
        for duplicate in (False, True):
            value = copy.deepcopy(self.value)
            index = self.blob(value, "index")
            selected = next(item for item in index["manifests"] if item.get("platform") == image_input.PLATFORM)
            if duplicate:
                index["manifests"].append(copy.deepcopy(selected))
            else:
                selected["platform"]["variant"] = "v9"
            self.rebind(value, "index", index)
            self.reject(value)

    def test_manifest_and_config_descriptor_binding(self):
        for name in ("index", "manifest"):
            for field, changed in (("digest", "sha256:" + "b" * 64), ("size", 17)):
                value = copy.deepcopy(self.value)
                document = self.blob(value, name)
                target = document["config"] if name == "manifest" else next(
                    item for item in document["manifests"] if item.get("platform") == image_input.PLATFORM)
                target[field] = changed
                self.rebind(value, name, document)
                self.reject(value)

    def test_layers_cannot_redirect_or_change_media_type(self):
        for field, changed in (("urls", ["https://foreign.example/layer"]),
                               ("mediaType", "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip"),
                               ("size", False), ("digest", "sha256:" + "A" * 64)):
            value = copy.deepcopy(self.value)
            manifest = self.blob(value, "manifest")
            manifest["layers"][0][field] = changed
            self.rebind(value, "manifest", manifest)
            self.reject(value)

    def test_config_platform_diff_ids_and_python_defaults_are_validated(self):
        for changed in ("architecture", "variant", "diff_ids", "python", "user", "entrypoint"):
            value = copy.deepcopy(self.value)
            config = self.blob(value, "config")
            if changed == "architecture":
                config[changed] = "amd64"
            elif changed == "variant":
                config[changed] = "v9"
            elif changed == "diff_ids":
                config["rootfs"]["diff_ids"].pop()
            elif changed == "python":
                config["config"]["Env"].append("PYTHON_VERSION=3.9.1")
            elif changed == "user":
                config["config"]["User"] = "nobody"
            else:
                config["config"]["Entrypoint"] = ["foreign-command"]
            self.rebind(value, "config", config)
            self.reject(value)


if __name__ == "__main__":
    unittest.main()
