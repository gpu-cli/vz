"""Release-directory admission and candidate-tuple computation.

The release directory is produced by `scripts/build-vz-0.4-release-candidate.sh`
(owned elsewhere). This module only reads it: canonical read-only path,
strict `release-manifest.json`, `release-manifest.sha256`, per-component
digests, recomputed content digests, `checksums.sha256` coverage and codesign
re-verification. Nothing in the release directory is ever mutated.

`codesign_verifier` is an injection point for unit tests only (they build a
fake release directory with dummy files). It is never exposed as a CLI flag;
the gate and validator always use `run_codesign_verify`.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess

import vz04_schema as schema
from vz04_common import (DIGEST_PATTERN, REPO_ROOT, GateError, canonical_json, canonical_path, checked_text,
                         digest_file, load_json, read_regular, relative_components, require, sha256_bytes, tree_digest,
                         tree_entries, verify_checksums)

MANIFEST_NAME = "release-manifest.json"
MANIFEST_DIGEST_NAME = "release-manifest.sha256"
CHECKSUMS_NAME = "checksums.sha256"
SIGNING_CLASSES = ("local-test-signed", "developer-id-notarized")
MANIFEST_REQUIRED = ("schema_version", "kind", "signing_class", "release_version", "built_at_utc", "source", "toolchain",
                     "components", "guest_bundles", "buildkit", "normalized_content_sha256", "signed_content_sha256")
SOURCE_REQUIRED = ("commit", "git_tree", "tree_sha256", "clean", "submodules")
COMPONENT_REQUIRED = ("unsigned_sha256", "signed_sha256", "kind", "cargo", "codesign")
SIGNED_KINDS = frozenset(("host_binary", "host-binary", "binary", "macho"))


def line_digest(value) -> str:
    """sha256(canonical_json(value) + LF): the builder's `canonical-sha256`
    convention (vz04_source_tree.py) and the candidate-tuple convention."""
    return sha256_bytes((canonical_json(value) + "\n").encode("utf-8"))


def run_codesign_verify(path: Path) -> tuple:
    """(ok, detail) from `codesign --verify --strict --verbose=4`."""
    completed = subprocess.run(["/usr/bin/codesign", "--verify", "--strict", "--verbose=4", str(path)],
                               stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, check=False)
    detail = (completed.stdout + completed.stderr).decode("utf-8", "replace").strip()
    return completed.returncode == 0, detail[-2000:]


def _plan_shape_findings(manifest: dict) -> list:
    """Field-list check from RELEASE-GATE-PLAN.md, used when the manifest schema
    (owned by the builder) is not yet present."""
    findings = []
    for key in MANIFEST_REQUIRED:
        if key not in manifest:
            findings.append(f"release manifest missing {key}")
    if findings:
        return findings
    if manifest["schema_version"] != 1:
        findings.append("release manifest schema_version is not 1")
    if manifest["signing_class"] not in SIGNING_CLASSES:
        findings.append(f"unknown signing_class {manifest['signing_class']!r}")
    if not isinstance(manifest["source"], dict) or any(k not in manifest["source"] for k in SOURCE_REQUIRED):
        findings.append("release manifest source lacks commit/git_tree/tree_sha256/clean/submodules")
    if not isinstance(manifest["components"], dict) or not manifest["components"]:
        findings.append("release manifest components must be a non-empty object")
    else:
        for path, component in manifest["components"].items():
            if not isinstance(component, dict) or any(k not in component for k in COMPONENT_REQUIRED):
                findings.append(f"component {path} lacks unsigned_sha256/signed_sha256/kind/cargo/codesign")
    return findings


def admit_release_dir(value, *, repo_root: Path = REPO_ROOT, codesign_verifier=run_codesign_verify) -> dict:
    """Admit a release directory without mutation.

    Raises GateError on integrity failures (unreadable manifest, digest
    mismatch, writable files, missing components). Codesign verification
    failures and contract-level shape problems are returned as `findings`
    so they appear in evidence rather than aborting before any is written.
    """
    release_dir = canonical_path(value)
    require(release_dir.is_dir(), f"release dir is not a directory: {release_dir}")
    manifest_path = release_dir / MANIFEST_NAME
    require(manifest_path.is_file() and not manifest_path.is_symlink(), f"{MANIFEST_NAME} missing in {release_dir}")
    manifest_bytes = read_regular(manifest_path)
    manifest = load_json(manifest_path)
    manifest_sha256 = sha256_bytes(manifest_bytes)
    declared = read_regular(release_dir / MANIFEST_DIGEST_NAME).decode("utf-8").split()
    require(declared and checked_text(declared[0], DIGEST_PATTERN, "manifest digest") == manifest_sha256,
            f"{MANIFEST_DIGEST_NAME} does not match {MANIFEST_NAME}")

    findings = []
    shape = _plan_shape_findings(manifest)
    require(not shape, "release manifest shape: " + "; ".join(shape))
    if schema.schema_available("release-manifest", repo_root):
        for problem in schema.validate("release-manifest", manifest, repo_root):
            findings.append(("release.schema", MANIFEST_NAME, problem))
    else:
        findings.append(("release.schema_missing", MANIFEST_NAME,
                         "schemas/vz-0.4-release-manifest.schema.json is absent; plan field list enforced instead"))

    for relative, _mode, _size, _digest in tree_entries(release_dir, excluded_dirs=frozenset()):
        metadata = os.lstat(release_dir / relative)
        require(stat.S_IMODE(metadata.st_mode) & 0o222 == 0, f"release file is writable: {relative}")

    components = {}
    for relative, component in manifest["components"].items():
        relative_components(relative)
        path = release_dir / relative
        require(path.is_file() and not path.is_symlink(), f"component missing: {relative}")
        actual = digest_file(path)
        require(actual == component["signed_sha256"], f"component digest mismatch: {relative}")
        checked_text(component["unsigned_sha256"], DIGEST_PATTERN, "unsigned digest")
        components[relative] = actual
        if component["kind"] in SIGNED_KINDS or component.get("codesign") not in (None, False):
            ok, detail = codesign_verifier(path)
            if not ok:
                findings.append(("release.codesign", relative, f"codesign --verify --strict failed: {detail[:300]}"))
    normalized = line_digest(sorted([p, c["unsigned_sha256"]] for p, c in manifest["components"].items()))
    signed = line_digest(sorted([p, c["signed_sha256"]] for p, c in manifest["components"].items()))
    require(normalized == manifest["normalized_content_sha256"], "normalized_content_sha256 does not recompute")
    require(signed == manifest["signed_content_sha256"], "signed_content_sha256 does not recompute")
    checksum_findings = verify_checksums(release_dir, CHECKSUMS_NAME)
    require(not checksum_findings, "release checksums: " + "; ".join(checksum_findings[:4]))

    return {
        "dir": str(release_dir),
        "manifest": manifest,
        "release_manifest_sha256": manifest_sha256,
        "release_dir_sha256": tree_digest(release_dir),
        "components": components,
        "signing_class": manifest["signing_class"],
        "release_version": manifest["release_version"],
        "normalized_content_sha256": normalized,
        "signed_content_sha256": signed,
        "source_commit": checked_text(manifest["source"]["commit"], r"[0-9a-f]{40}", "source commit"),
        "findings": findings,
    }


def candidate_tuple(*, source_commit: str, source_tree_sha256, release: dict, frozen: dict, notarization_ticket_sha256=None) -> dict:
    """The digest tuple that identifies a candidate. Returns {tuple, sha256}."""
    inputs = {key: entry["sha256"] for key, entry in frozen["inputs"].items()}
    inputs["decision_signatures_tree_sha256"] = frozen["digests"]["signatures_tree_sha256"]
    value = {
        "source": {"commit": source_commit, "tree_sha256": source_tree_sha256},
        "release": {
            "normalized_content_sha256": release["normalized_content_sha256"],
            "signed_content_sha256": release["signed_content_sha256"],
            "signing_class": release["signing_class"],
            "release_manifest_sha256": release["release_manifest_sha256"],
        },
        "distribution": {"signed_content_sha256": release["signed_content_sha256"],
                         "notarization_ticket_sha256": notarization_ticket_sha256},
        "inputs": inputs,
        "schemas_tree_sha256": frozen["digests"]["schemas_tree_sha256"],
        "fixtures_tree_sha256": frozen["digests"]["fixtures_tree_sha256"],
        "harness_tree_sha256": frozen["digests"]["harness_tree_sha256"],
    }
    return {"tuple": value, "sha256": line_digest(value)}


def tuple_findings(release: dict, frozen: dict, candidate: dict) -> list:
    """Requirement findings about the candidate itself (GA parity, pins)."""
    findings = []
    if release["signing_class"] != "developer-id-notarized":
        findings.append(("release.signing_class", release["dir"],
                         f"signing_class {release['signing_class']} is development evidence only; GA requires developer-id-notarized"))
    if candidate["tuple"]["distribution"]["notarization_ticket_sha256"] is None:
        findings.append(("release.notarization", release["dir"], "no notarization ticket digest; required for GA"))
    if candidate["tuple"]["source"]["tree_sha256"] is None:
        findings.append(("source.tree_digest", "candidate", "canonical source-tree digest not recorded (builder step 2)"))
    for key, entry in frozen["inputs"].items():
        if entry["sha256"] is None:
            findings.append(("input.unpinned", key, f"{entry['path']} has no digest"))
    for key in ("schemas_tree_sha256", "fixtures_tree_sha256", "harness_tree_sha256", "signatures_tree_sha256"):
        if frozen["digests"][key] is None:
            findings.append(("input.unpinned", key, "digest unavailable"))
    return findings
