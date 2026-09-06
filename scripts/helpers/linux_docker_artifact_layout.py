"""Strict evidence consumer for this fixture's OCI and BuildKit 0.19 exports.

This is not a general OCI validator: one linux/arm64 scratch image, or one local
cache image-manifest, SHA-256 blobs, and gzip layers are supported. Attestations,
external descriptors and unknown semantic fields fail closed. Callers must keep
the producer quiescent; inventory checks detect change, not provide a snapshot.
"""
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re

import linux_docker_artifact_stream as stream


INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
CONFIG = "application/vnd.oci.image.config.v1+json"
CACHE_CONFIG = "application/vnd.buildkit.cacheconfig.v0"
LAYER = "application/vnd.oci.image.layer.v1.tar+gzip"
UNCOMPRESSED = "containerd.io/uncompressed"
JSON_BYTES = 4 * 1024 * 1024
NODES = 100000
LAYERS = 4096
# Explicit fixture-consumer budget, not a change to the generic evidence reader.
LIMITS = stream.Limits(tree_bytes=8 * 1024**3, metadata_bytes=16 * 1024**2,
                       entries=50000)


class LayoutError(ValueError):
    """Static codes only: never include paths, JSON values or secret material."""


def require(condition, code):
    if not condition:
        raise LayoutError(code)


def fields(value, required, optional=()):
    require(type(value) is dict and set(required) <= value.keys()
            and value.keys() <= set(required) | set(optional), "unsupported_object_fields")
    return value


def sequence(value, maximum=NODES):
    require(type(value) is list and len(value) <= maximum, "invalid_array")
    return value


def integer(value, minimum=0, maximum=2**63 - 1):
    require(type(value) is int and minimum <= value <= maximum, "invalid_integer")
    return value


def text(value):
    require(type(value) is str and len(value.encode("utf-8")) <= JSON_BYTES, "invalid_text")
    return value


def digest(value):
    require(type(value) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None,
            "invalid_digest")
    return value


def strings(value):
    require(type(value) is dict, "invalid_string_map")
    for key, item in value.items():
        text(key)
        text(item)
    return value


def timestamp(value):
    require(type(value) is str and re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})",
        value) is not None, "invalid_timestamp")
    try:
        # Python 3.9 accepts microseconds, while Go emits up to nanoseconds.
        calendar = re.sub(r"\.([0-9]{6})[0-9]+", r".\1", value)
        datetime.fromisoformat(calendar[:-1] + "+00:00" if calendar.endswith("Z") else calendar)
    except ValueError:
        raise LayoutError("invalid_timestamp") from None


def platform(value):
    fields(value, ("architecture", "os"), ("variant",))
    require(value["architecture"] == "arm64" and value["os"] == "linux"
            and value.get("variant", "v8") == "v8", "unexpected_platform")


def _object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate_json_key")
        value[key] = item
    return value


def _json(data, canaries):
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(LayoutError("nonfinite_json")))
        pending, count = [(value, 0)], 0
        while pending:
            item, depth = pending.pop()
            count += 1
            require(count <= NODES and depth <= 64, "json_complexity_limit")
            if type(item) is dict:
                for key, child in item.items():
                    stream.CanaryScanner(canaries).feed(key.encode("utf-8"))
                    pending.append((child, depth + 1))
            elif type(item) is list:
                pending.extend((child, depth + 1) for child in item)
            elif type(item) is str:
                stream.CanaryScanner(canaries).feed(item.encode("utf-8"))
            elif type(item) is float:
                require(math.isfinite(item), "nonfinite_json")
        return value
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise LayoutError("invalid_json") from None


class _Layout:
    def __init__(self, root, canaries):
        self.root = Path(root)
        self.canaries = stream.CanaryScanner(canaries)._canaries
        self.inventory = stream.inventory_tree(self.root, canaries=self.canaries, limits=LIMITS)
        self.files = {row["path"]: row for row in self.inventory["files"]}
        directories = {row["path"] for row in self.inventory["directories"]}
        # content/local/store.go creates ingest/; a successful commit removes
        # only its transaction child. An empty structural root is legitimate.
        require(directories in ({"blobs", "blobs/sha256"}, {"blobs", "blobs/sha256", "ingest"}),
                "unexpected_layout_directories")
        require({"oci-layout", "index.json"} <= self.files.keys(), "missing_layout_metadata")
        for name in self.files:
            require(name in ("oci-layout", "index.json") or re.fullmatch(r"blobs/sha256/[0-9a-f]{64}", name),
                    "unexpected_layout_file")
        self.reached = {"oci-layout", "index.json"}
        self.descriptors, self.layers = {}, {}
        self.uncompressed_bytes = 0
        require(self.read_json("oci-layout") == {"imageLayoutVersion": "1.0.0"}, "unsupported_layout_version")
        index = fields(self.read_json("index.json"), ("schemaVersion", "mediaType", "manifests"), ("annotations",))
        require(type(index["schemaVersion"]) is int and index["schemaVersion"] == 2
                and index["mediaType"] == INDEX, "unsupported_index")
        if "annotations" in index:
            strings(index["annotations"])
        entries = sequence(index["manifests"], 1)
        require(len(entries) == 1, "expected_single_manifest")
        self.manifest_descriptor = self.descriptor(entries[0], MANIFEST, allow_platform=True)
        require(self.manifest_descriptor.get("annotations", {}).get("org.opencontainers.image.ref.name") == "latest",
                "missing_latest_reference")
        self.manifest = fields(self.read_descriptor(self.manifest_descriptor),
                               ("schemaVersion", "mediaType", "config", "layers"), ("annotations",))
        require(type(self.manifest["schemaVersion"]) is int and self.manifest["schemaVersion"] == 2
                and self.manifest["mediaType"] == MANIFEST, "unsupported_manifest")
        if "annotations" in self.manifest:
            strings(self.manifest["annotations"])

    def read_json(self, name):
        require(name in self.files and self.files[name]["size"] <= JSON_BYTES, "json_size_or_inventory")
        # Reuse the stream primitive's pinned no-follow reader for the bounded
        # JSON read; a separate Path.read_bytes would introduce a link race.
        with stream._opened(self.root / name) as descriptor:
            chunks, size = [], 0
            while True:
                data = os.read(descriptor, 65536)
                if not data:
                    break
                size += len(data)
                require(size <= JSON_BYTES, "json_size_or_inventory")
                chunks.append(data)
        data = b"".join(chunks)
        require(len(data) == self.files[name]["size"] and hashlib.sha256(data).hexdigest() == self.files[name]["sha256"],
                "layout_changed")
        return _json(data, self.canaries)

    def descriptor(self, value, media_type, allow_platform=False):
        fields(value, ("mediaType", "digest", "size"), ("annotations", "platform") if allow_platform else ("annotations",))
        require(value["mediaType"] == media_type, "unexpected_descriptor_media_type")
        selected = digest(value["digest"])
        integer(value["size"])
        if "annotations" in value:
            strings(value["annotations"])
        if "platform" in value:
            platform(value["platform"])
        name = "blobs/sha256/" + selected[7:]
        require(name in self.files and self.files[name]["size"] == value["size"]
                and self.files[name]["sha256"] == selected[7:], "descriptor_content_mismatch")
        # Annotations may legitimately differ between roles, but content identity
        # and semantic type must never differ for a repeated digest.
        identity = (media_type, value["size"])
        require(selected not in self.descriptors or self.descriptors[selected] == identity,
                "inconsistent_descriptor")
        self.descriptors[selected] = identity
        self.reached.add(name)
        return value

    def read_descriptor(self, value):
        return self.read_json("blobs/sha256/" + value["digest"][7:])

    def layer(self, value, *, cache=False):
        self.descriptor(value, LAYER)
        selected = value["digest"]
        if selected not in self.layers:
            result = stream.scan_layer(self.root / "blobs/sha256" / selected[7:], compression="gzip",
                                       canaries=self.canaries, limits=LIMITS)
            require(result["compressed_sha256"] == selected[7:] and result["compressed_size"] == value["size"],
                    "layout_changed")
            self.uncompressed_bytes += result["uncompressed_size"]
            require(self.uncompressed_bytes <= 16 * 1024**3, "aggregate_uncompressed_limit")
            if cache:
                # Do not retain up to 16MiB of member metadata for every layer.
                # The complete metadata is scanned/validated before projection.
                result["members_sha256"] = hashlib.sha256(json.dumps(result.pop("members"), sort_keys=True,
                    separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
            self.layers[selected] = result
        result = self.layers[selected]
        annotation = value.get("annotations", {}).get(UNCOMPRESSED)
        require((not cache and annotation is None) or annotation == result["diff_id"], "layer_diff_id_mismatch")
        return result

    def finish(self, kind):
        require(self.reached == self.files.keys(), "unreachable_layout_files")
        final = stream.inventory_tree(self.root, canaries=self.canaries, limits=LIMITS)
        require(final == self.inventory, "layout_changed")
        return {"schema_version": 1, "kind": kind, "inventory_sha256": final["inventory_sha256"],
                "inventory": final, "manifest": self.manifest_descriptor}


def _image_config(value):
    fields(value, ("architecture", "os", "rootfs", "config"), ("created", "author", "variant", "history"))
    platform({key: value[key] for key in ("architecture", "os", "variant") if key in value})
    if "created" in value:
        timestamp(value["created"])
    if "author" in value:
        text(value["author"])
    fields(value["rootfs"], ("type", "diff_ids"))
    require(value["rootfs"]["type"] == "layers", "unexpected_rootfs_type")
    ids = sequence(value["rootfs"]["diff_ids"], 1)
    require(len(ids) == 1, "expected_single_diff_id")
    digest(ids[0])
    config = fields(value["config"], (), ("User", "ExposedPorts", "Env", "Entrypoint", "Cmd", "Volumes",
                                         "WorkingDir", "Labels", "StopSignal", "ArgsEscaped"))
    for key, item in config.items():
        if key in ("Env", "Entrypoint", "Cmd"):
            for string in sequence(item):
                text(string)
        elif key in ("ExposedPorts", "Volumes"):
            require(type(item) is dict and all(type(k) is str and v == {} for k, v in item.items()), "invalid_config_map")
        elif key == "Labels":
            strings(item)
        elif key == "ArgsEscaped":
            require(type(item) is bool, "invalid_config_boolean")
        else:
            text(item)
    history = sequence(value.get("history", []))
    count = 0
    for entry in history:
        fields(entry, (), ("created", "created_by", "author", "comment", "empty_layer"))
        for key, item in entry.items():
            if key == "created":
                timestamp(item)
            elif key == "empty_layer":
                require(type(item) is bool, "invalid_history_boolean")
            else:
                text(item)
        count += not entry.get("empty_layer", False)
    require(not history or count == 1, "history_layer_mismatch")
    return ids[0]


def validate_oci(root, *, expected_path, expected_sha256, expected_size, canaries=()):
    """Validate one exact final scratch file, excluding all intermediate files."""
    require(type(expected_path) is str and expected_path not in ("", ".") and not expected_path.startswith("/")
            and all(part not in ("", ".", "..") for part in expected_path.split("/")), "invalid_expected_path")
    require(type(expected_sha256) is str, "invalid_expected_digest")
    digest("sha256:" + expected_sha256)
    integer(expected_size)
    layout = _Layout(root, canaries)
    descriptor = layout.descriptor(layout.manifest["config"], CONFIG)
    config = layout.read_descriptor(descriptor)
    diff_id = _image_config(config)
    config_annotation = layout.manifest_descriptor.get("annotations", {}).get("config.digest")
    require(config_annotation is None or config_annotation == descriptor["digest"], "config_annotation_mismatch")
    layers = sequence(layout.manifest["layers"], 1)
    require(len(layers) == 1, "expected_single_scratch_layer")
    layer = layout.layer(layers[0])
    require(layer["diff_id"] == diff_id, "config_diff_id_mismatch")
    members = layer["members"]
    payload = [row for row in members if row["path"] == expected_path]
    require(len(payload) == 1 and all(row["path"] == expected_path or
            (row["path"] == "." and row["type"] == "directory") for row in members), "unexpected_scratch_members")
    payload = payload[0]
    require(payload["type"] == "file" and payload["sha256"] == expected_sha256 and payload["size"] == expected_size
            and payload["uid"] == 0 and payload["gid"] == 0 and payload["mode"] == 0o644, "unexpected_scratch_payload")
    proof = layout.finish("oci-scratch")
    proof.update(config=descriptor, platform={"os": "linux", "architecture": "arm64"},
                 layer={"descriptor": layers[0], "diff_id": diff_id, "uncompressed_size": layer["uncompressed_size"]},
                 payload=payload)
    return proof


def _acyclic(edges):
    state = [0] * len(edges)
    for root in range(len(edges)):
        if state[root]:
            continue
        stack = [(root, False)]
        while stack:
            node, leaving = stack.pop()
            if leaving:
                state[node] = 2
            elif state[node] != 2:
                require(state[node] == 0, "cache_graph_cycle")
                state[node] = 1
                stack.append((node, True))
                stack.extend((child, False) for child in edges[node])


def validate_cache(root, *, canaries=()):
    """Validate exported cache bytes and every v0 graph reference, not a cache hit."""
    layout = _Layout(root, canaries)
    descriptor = layout.descriptor(layout.manifest["config"], CACHE_CONFIG)
    config = fields(layout.read_descriptor(descriptor), ("layers", "records"))
    layers = sequence(config["layers"], LAYERS)
    records = sequence(config["records"], NODES)
    manifests = sequence(layout.manifest["layers"], LAYERS)
    require(layers and records and len(layers) == len(manifests), "empty_or_mismatched_cache")
    edges, scanned = [], []
    for entry, layer_descriptor in zip(layers, manifests):
        fields(entry, ("blob",), ("parent", "annotations"))
        layout.descriptor(layer_descriptor, LAYER)
        require(digest(entry["blob"]) == layer_descriptor["digest"], "cache_layer_order_mismatch")
        parent = integer(entry.get("parent", 0), -1, len(layers) - 1)
        edges.append([] if parent == -1 else [parent])
        result = layout.layer(layer_descriptor, cache=True)
        if "annotations" in entry:
            annotations = fields(entry["annotations"], (), ("mediaType", "diffID", "size", "createdAt"))
            for key, expected in (("mediaType", LAYER), ("diffID", result["diff_id"]), ("size", layer_descriptor["size"])):
                if key in annotations:
                    require(type(annotations[key]) is type(expected) and annotations[key] == expected, "cache_annotation_mismatch")
            if "createdAt" in annotations:
                timestamp(annotations["createdAt"])
        scanned.append({"blob": entry["blob"], "parent": parent, "diff_id": result["diff_id"]})
    _acyclic(edges)
    record_edges, referenced = [], set()
    for record in records:
        fields(record, ("digest",), ("layers", "chains", "inputs"))
        digest(record["digest"])
        children = []
        for group in sequence(record.get("inputs", [])):
            # Go's [][]CacheInput can encode an empty inner slice as null.
            for item in sequence([] if group is None else group):
                fields(item, ("link",), ("selector",))
                children.append(integer(item["link"], 0, len(records) - 1))
                if "selector" in item:
                    text(item["selector"])
        record_edges.append(children)
        for result in sequence(record.get("layers", [])):
            fields(result, ("layer",), ("createdAt",))
            referenced.add(integer(result["layer"], 0, len(layers) - 1))
            if "createdAt" in result:
                timestamp(result["createdAt"])
        for result in sequence(record.get("chains", [])):
            fields(result, ("layers",), ("createdAt",))
            chain = sequence(result["layers"], LAYERS)
            require(chain, "empty_cache_chain")
            referenced.update(integer(index, 0, len(layers) - 1) for index in chain)
            if "createdAt" in result:
                timestamp(result["createdAt"])
    _acyclic(record_edges)
    pending = list(referenced)
    while pending:
        for parent in edges[pending.pop()]:
            if parent not in referenced:
                referenced.add(parent)
                pending.append(parent)
    require(referenced == set(range(len(layers))), "unreachable_cache_layers")
    proof = layout.finish("buildkit-cache-v0")
    proof.update(config=descriptor, layers=scanned, records=config["records"],
                 unique_blobs=[{"digest": selected, "size": result["compressed_size"],
                                "diff_id": result["diff_id"], "uncompressed_size": result["uncompressed_size"],
                                "members_sha256": result["members_sha256"]}
                               for selected, result in sorted(layout.layers.items())])
    return proof
