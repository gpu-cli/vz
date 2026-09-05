"""Offline OCI metadata input validation; never pulls or claims runtime readiness."""

import argparse
import base64
import binascii
import hashlib
import json
import os
from pathlib import Path
import re
import stat


MAX_INPUT = 256 * 1024
MAX_METADATA = 64 * 1024
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
CONFIG = "application/vnd.oci.image.config.v1+json"
LAYER = "application/vnd.oci.image.layer.v1.tar+gzip"
PLATFORM = {"os": "linux", "architecture": "arm64", "variant": "v8"}
SCOPE = "registry_metadata_only_no_layer_download_or_Machine_execution"


def require(value, message):
    if not value:
        raise ValueError(message)


def keys(value, expected):
    require(isinstance(value, dict) and set(value) == set(expected), "unexpected input fields")


def digest(value):
    require(isinstance(value, str) and DIGEST.fullmatch(value), "invalid sha256 digest")
    return value


def positive_integer(value, limit):
    require(type(value) is int and 0 < value <= limit, "invalid descriptor size")


def _object(pairs):
    result = {}
    for name, value in pairs:
        require(name not in result, "duplicate JSON key")
        result[name] = value
    return result


def _constant(_value):
    raise ValueError("non-finite JSON number")


def parse(raw):
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_object, parse_constant=_constant)


def _read(path):
    path = Path(path).absolute()
    require(".." not in path.parts, "input traversal is forbidden")
    directory = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=directory)
            os.close(directory)
            directory = child
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                     dir_fd=directory)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                    and info.st_uid == os.geteuid(), "input must be an owned regular single-link file")
            positive_integer(info.st_size, MAX_INPUT)
            raw = stream.read(MAX_INPUT + 1)
            require(len(raw) == info.st_size and len(raw) <= MAX_INPUT, "input changed or exceeds bound")
            return raw
    finally:
        os.close(directory)


def _metadata(value):
    keys(value, ("encoding", "digest", "size", "data"))
    require(value["encoding"] == "base64" and isinstance(value["data"], str), "raw metadata encoding")
    positive_integer(value["size"], MAX_METADATA)
    digest(value["digest"])
    require(len(value["data"]) <= 4 * ((MAX_METADATA + 2) // 3), "encoded metadata exceeds bound")
    try:
        raw = base64.b64decode(value["data"], validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError("invalid metadata base64") from error
    require(base64.b64encode(raw).decode("ascii") == value["data"], "noncanonical metadata base64")
    require(len(raw) == value["size"]
            and "sha256:" + hashlib.sha256(raw).hexdigest() == value["digest"],
            "raw metadata digest or size mismatch")
    parsed = parse(raw)
    require(isinstance(parsed, dict), "OCI metadata must be an object")
    return parsed


def _descriptor(value, media_type):
    require(isinstance(value, dict) and {"digest", "size", "mediaType"} <= set(value),
            "incomplete OCI descriptor")
    require(set(value) <= {"digest", "size", "mediaType", "platform", "annotations"},
            "redirected or unsupported descriptor")
    require(value["mediaType"] == media_type, "unexpected descriptor media type")
    positive_integer(value["size"], 4 * 1024 * 1024 * 1024)
    digest(value["digest"])


def load(path):
    """Return immutable inputs for the current containerd-backed Engine.

    Its image ID is the selected manifest target, not the config digest. Config
    fields below come from hash-verified registry metadata, not an Engine fetch.
    This does not prove downloaded layer integrity or Machine execution.
    """
    value = parse(_read(path))
    keys(value, ("schema_version", "state", "role", "repository", "source_tag", "reference",
                 "platform", "python_version", "resolution_scope", "compatibility_certified", "metadata"))
    require(type(value["schema_version"]) is int and value["schema_version"] == 1
            and value["state"] == "resolved_input_not_runtime_verified"
            and value["role"] == "FIXTURE_BASE" and value["compatibility_certified"] is False
            and value["resolution_scope"] == SCOPE, "image input scope overclaim")
    require(value["repository"] == "docker.io/library/python", "unexpected Python repository")
    require(value["platform"] == PLATFORM, "linux/arm64/v8 image required")
    version = value["python_version"]
    require(isinstance(version, str) and re.fullmatch(r"3\.[0-9]+\.[0-9]+", version), "Python version")
    require(value["source_tag"] == f"docker.io/library/python:{version}-slim-bookworm",
            "source tag provenance does not match Python variant")
    blobs = value["metadata"]
    keys(blobs, ("index", "manifest", "config"))
    index, manifest, config = (_metadata(blobs[name]) for name in ("index", "manifest", "config"))
    require(index.get("schemaVersion") == 2 and index.get("mediaType") == INDEX
            and isinstance(index.get("manifests"), list) and 0 < len(index["manifests"]) <= 64,
            "invalid OCI index")
    for descriptor in index["manifests"]:
        _descriptor(descriptor, MANIFEST)
    matches = [item for item in index["manifests"] if item.get("platform") == PLATFORM]
    require(len(matches) == 1, "index must select exactly one arm64/v8 manifest")
    selected = matches[0]
    require(selected["digest"] == blobs["manifest"]["digest"]
            and selected["size"] == blobs["manifest"]["size"], "index/manifest identity mismatch")
    require(value["reference"] == value["repository"] + "@" + selected["digest"],
            "immutable selected-manifest reference required; no tag or index fallback")
    require(manifest.get("schemaVersion") == 2 and manifest.get("mediaType") == MANIFEST,
            "invalid OCI manifest")
    _descriptor(manifest.get("config"), CONFIG)
    require(manifest["config"]["digest"] == blobs["config"]["digest"]
            and manifest["config"]["size"] == blobs["config"]["size"], "manifest/config identity mismatch")
    require({key: config.get(key) for key in PLATFORM} == PLATFORM, "config platform mismatch")
    layers = manifest.get("layers")
    require(isinstance(layers, list) and 0 < len(layers) <= 64, "invalid layer inventory")
    for layer in layers:
        _descriptor(layer, LAYER)
        keys(layer, ("digest", "size", "mediaType"))
    require(len({layer["digest"] for layer in layers}) == len(layers), "duplicate compressed layer")
    rootfs = config.get("rootfs")
    keys(rootfs, ("type", "diff_ids"))
    require(rootfs["type"] == "layers" and isinstance(rootfs["diff_ids"], list)
            and len(rootfs["diff_ids"]) == len(layers), "layer/diff-ID inventory mismatch")
    for item in rootfs["diff_ids"]:
        digest(item)
    runtime = config.get("config")
    require(isinstance(runtime, dict) and isinstance(runtime.get("Env"), list), "image config missing")
    require(all(isinstance(item, str) for item in runtime["Env"]), "invalid image environment metadata")
    require([item for item in runtime["Env"] if item.startswith("PYTHON_VERSION=")] ==
            [f"PYTHON_VERSION={version}"], "Python version metadata mismatch")
    require(runtime.get("Cmd") == ["python3"] and runtime.get("Entrypoint") in (None, [])
            and runtime.get("User") in (None, "", "0", "root"), "unexpected base execution defaults")
    return {"reference": value["reference"], "id": selected["digest"],
            "config_digest": blobs["config"]["digest"],
            "manifest_descriptor": {key: selected[key] for key in ("mediaType", "digest", "size")},
            "image_config": runtime, "rootfs": rootfs, "platform_detail": dict(PLATFORM),
            "platform": "linux/arm64", "python_version": version,
            "provenance": {"source_tag": value["source_tag"], "resolution_scope": SCOPE},
            "metadata_hashes": {name: blobs[name]["digest"] for name in blobs},
            "layers": layers}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    print(json.dumps(load(parser.parse_args().input), sort_keys=True))
