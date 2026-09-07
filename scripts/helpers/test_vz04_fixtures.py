"""UNIT-TEST-ONLY fixture builders for the vz 0.4 gate modules.

`build_fake_release_dir` writes a tiny release directory with a plan-shaped
(and, when the builder's schema exists, schema-valid) manifest and dummy
component files. It is never release evidence: tests inject
`fake_codesign_verifier` into `admit_release_dir`; the gate and validator
CLIs have no such switch. When run as a script it writes a fake release dir
for the developer dry smoke and prints its path.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import stat
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import vz04_candidate as candidate  # noqa: E402
from vz04_common import digest_file, sha256_bytes  # noqa: E402

FAKE_COMMIT = "0" * 40
COMPONENTS = ("bin/vz", "bin/vz-runtimed", "bin/vz-macos-setup", "bin/vz-guest-agent", "bin/vz-agent-loader")


def fake_codesign_verifier(_path: Path) -> tuple:
    """Injected only by unit tests; dummy files are never real Mach-O binaries."""
    return True, "fake codesign verifier (unit test only)"


def build_fake_release_dir(root: Path, *, signing_class: str = "local-test-signed", source_commit: str = FAKE_COMMIT,
                           tree_sha256: str = None, copy_real_binary: bool = False) -> Path:
    """Create a read-only fake release dir at `root` and return it.

    `copy_real_binary=True` copies /usr/bin/true as every host binary so the
    real `codesign --verify --strict` succeeds (dry smoke only).
    """
    root.mkdir(parents=True)
    (root / "bin").mkdir()
    (root / "codesign").mkdir()
    components = {}
    for relative in COMPONENTS:
        path = root / relative
        if copy_real_binary:
            shutil.copyfile("/usr/bin/true", path)
        else:
            path.write_bytes(f"fake {relative}\n".encode())
        path.chmod(0o755)
        signed = digest_file(path)
        unsigned = sha256_bytes(f"unsigned {relative}\n".encode())
        stem = relative.rsplit("/", 1)[-1]
        (root / "codesign" / f"{stem}.verify.log").write_bytes(b"fake verify log\n")
        components[relative] = {
            "kind": "host-binary", "unsigned_sha256": unsigned, "signed_sha256": signed,
            "cargo": {"package_id": f"{stem}#0.4.0", "target": stem, "features": [], "profile": "release", "locked": True},
            "codesign": {"identifier": stem, "cdhash": "0" * 40, "flags": "0x10000(runtime)", "team_id": None,
                         "signature": "adhoc", "runtime_version": "26.0.0", "entitlements_sha256": sha256_bytes(b"ents"),
                         "hardened_runtime": True, "metadata_file": f"codesign/{stem}.codesign-d.txt",
                         "entitlements_file": None, "signing": "ad-hoc-hardened-runtime-entitled"},
        }
        (root / "codesign" / f"{stem}.codesign-d.txt").write_bytes(b"fake codesign -d output\n")
    (root / "entitlements.plist").write_bytes(b"<plist/>\n")
    components["entitlements.plist"] = {"kind": "entitlements", "unsigned_sha256": digest_file(root / "entitlements.plist"),
                                        "signed_sha256": digest_file(root / "entitlements.plist"), "cargo": None, "codesign": None}
    manifest = {
        "$schema": "../schemas/vz-0.4-release-manifest.schema.json",
        "schema_version": 1, "kind": "vz-0.4-release-candidate", "signing_class": signing_class, "release_version": "0.4.0-faketest",
        "built_at_utc": "2026-09-07T00:00:00Z",
        "source": {"commit": source_commit, "git_tree": "1" * 40, "tree_sha256": tree_sha256 or sha256_bytes(b"fake tree"),
                   "clean": False, "submodules": []},
        "toolchain": {"cargo": "fake", "rustc": "fake", "xcode": None},
        "components": components,
        "guest_bundles": {"note": "fake test fixture; no guest bundles"},
        "buildkit": {"note": "fake test fixture; no BuildKit archive"},
        "normalized_content_sha256": candidate.line_digest(sorted([p, c["unsigned_sha256"]] for p, c in components.items())),
        "signed_content_sha256": candidate.line_digest(sorted([p, c["signed_sha256"]] for p, c in components.items())),
    }
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
    (root / "release-manifest.json").write_bytes(manifest_bytes)
    (root / "release-manifest.sha256").write_bytes(f"{sha256_bytes(manifest_bytes)}  release-manifest.json\n".encode())
    rows = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rows.append(f"{digest_file(path)}  {path.relative_to(root).as_posix()}\n")
    (root / "checksums.sha256").write_bytes("".join(rows).encode())
    for path in root.rglob("*"):
        mode = stat.S_IMODE(os.lstat(path).st_mode)
        path.chmod(mode & ~0o222)
    return root


def make_writable(root: Path) -> None:
    """Allow test teardown to delete a read-only fake release dir."""
    for path in root.rglob("*"):
        path.chmod(stat.S_IMODE(os.lstat(path).st_mode) | 0o200)
    root.chmod(0o700)


if __name__ == "__main__":
    target = Path(sys.argv[1]).resolve()
    build_fake_release_dir(target, copy_real_binary=True)
    print(target)
