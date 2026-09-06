"""Admit frozen public OpenSSH test inputs before any Machine provisioning.

The checked-in pin selects one immutable base and Debian snapshot. Verification
is offline and replays signatures using only the explicitly selected public
archive keyring. This certifies input provenance, not SSH/Docker execution.
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat

import docker_host_driver as driver
import linux_docker_debian as debian
import linux_docker_image_input as image_input

require = driver.require
REPO = Path(__file__).resolve().parents[2]
PIN = REPO / "config/docker-ssh-packages-bookworm-arm64.json"
PIN_SHA256 = "aa751b309dfb8a0c7c0c3ab61c30bc9efd9ea2ec67fe15b7bc252a321e6cb4ca"
IMAGE = REPO / "tests/fixtures/vz-0.4/docker/python-image-input.json"
ENV = {"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"}


def canonical(path):
    path = Path(path)
    require(path.is_absolute() and path == path.resolve(strict=True)
            and not any(c in str(path) for c in "\x00\n\r"), "canonical input path required")
    return path


def load(path=PIN, *, image_path=IMAGE):
    raw = image_input._read(canonical(path))
    require(driver.sha256(raw) == PIN_SHA256, "unsupported OpenSSH package pin")
    pin = image_input.parse(raw)
    base = image_input.load(canonical(image_path))
    require(pin["base"]["reference"] == base["reference"]
            and pin["base"]["config_digest"] == base["config_digest"]
            and pin["base"]["rootfs_diff_ids"] == base["rootfs"]["diff_ids"],
            "OpenSSH package base differs from admitted image")
    keyring = pin["base"]["keyring"]
    layers = [(item["digest"], diff_id) for item, diff_id in zip(base["layers"], base["rootfs"]["diff_ids"])]
    require((keyring["layer_digest"], keyring["diff_id"]) in layers, "archive trust anchor base layer differs")
    return pin


def read_input(root, row):
    name = row["filename"]
    require(Path(name).name == name and name not in {".", ".."}, "input filename must be local")
    path = canonical(root / name)
    raw = driver.regular(path, limit=debian.MAX_METADATA)
    require(path == (root / name).resolve(strict=True), "input changed location")
    return debian.verify_bytes(raw, {key: row[key] for key in ("sha256", "size")})


def guest_manifest(pin):
    return {"schema_version": 1, "dpkg_deb_sha256": pin["base"]["dpkg_deb"]["sha256"],
            "extraction": {"aliases": dict(pin["base"]["extraction"]["aliases"]),
                           **{key: dict(pin["base"]["extraction"][key]) for key in ("tar", "loader")}},
            "packages": [{key: row[key] for key in ("architecture", "filename", "package", "sha256", "size", "version")}
                         for row in pin["packages"]]}


def inspect_packages(root, pin, release_plaintext):
    """Check the Release→Packages→DEB chain after the caller verifies gpgv."""
    root = canonical(root)
    release = debian.one_paragraph(release_plaintext)
    require(release.get("Origin") == "Debian" and release.get("Codename") == "bookworm"
            and release.get("Version") == "12.15", "unexpected signed Debian release identity")
    index = pin["packages_index"]
    expected = {key: index[key] for key in ("sha256", "size")}
    require(debian.release_entry(release_plaintext, index["release_path"]) == expected,
            "signed Packages descriptor differs from frozen pin")
    packages = debian.unxz(read_input(root, index), limit=index["uncompressed_limit"])
    results = []
    for row in pin["packages"]:
        selected = debian.package_entry(packages, name=row["package"], version=row["version"],
                                        architecture=row["architecture"])
        require(selected["filename"] == row["repository_path"]
                and all(selected[key] == row[key] for key in ("size", "sha256"))
                and selected["fields"].get("Depends") == row["depends"], "signed DEB package selection differs")
        control = debian.deb_control(read_input(root, row))
        require(all(control.get(field) == row[key] for field, key in (
            ("Package", "package"), ("Version", "version"), ("Architecture", "architecture"), ("Depends", "depends"))),
            "DEB control identity/dependencies differ from signed index")
        results.append({key: row[key] for key in ("filename", "sha256", "size", "package", "version", "architecture")})
    return {"packages": results, "packages_uncompressed_sha256": driver.sha256(packages),
            "packages_uncompressed_bytes": len(packages), "release_plaintext_sha256": driver.sha256(release_plaintext)}


def verify(source, evidence, tool, *, pin_path=PIN, image_path=IMAGE):
    """Record one real offline signature check, then verify every package byte.

    `tool` contains an exact canonical gpgv executable and its preflight SHA256.
    No shell, inherited GnuPG configuration, network, agent, or user keys are used.
    """
    implementation = [Path(__file__).resolve(), Path(driver.__file__).resolve(),
                      Path(debian.__file__).resolve(), Path(image_input.__file__).resolve(),
                      canonical(pin_path), canonical(image_path)]
    source_hashes = {str(path): driver.sha256(driver.regular(path)) for path in implementation}
    pin = load(pin_path, image_path=image_path)
    source = canonical(source)
    require(source.is_dir(), "public input directory required")
    require(isinstance(tool, dict) and set(tool) == {"path", "sha256"}, "gpgv tool descriptor")
    executable = canonical(tool["path"])
    def check_tool():
        raw = driver.regular(executable, limit=64 * 1024 * 1024)
        mode = executable.stat().st_mode
        require(stat.S_ISREG(mode) and mode & 0o111 and not mode & 0o022
                and driver.sha256(raw) == tool["sha256"], "gpgv executable differs")
    check_tool()
    keyring, release = pin["base"]["keyring"], pin["release"]
    read_input(source, keyring)
    read_input(source, release)
    # Source inspection is retained as provenance, not substituted for the real
    # signature check below or represented as guest execution/ABI validation.
    for row in pin["source_proofs"]:
        read_input(source, row)
    evidence = Path(evidence)
    require(evidence.is_absolute() and evidence.parent == evidence.parent.resolve(strict=True)
            and not os.path.lexists(evidence), "fresh canonical signature evidence required")
    evidence.mkdir(mode=0o700)
    gnupg = evidence / "empty-gnupg"
    gnupg.mkdir(mode=0o700)
    record = driver.Recorder(evidence, dict(ENV), [], max_stream_bytes=1024 * 1024)
    argv = [str(executable), "--homedir", str(gnupg), "--keyring", str(source / keyring["filename"]),
            "--status-fd", "2", "--output", "-", str(source / release["filename"])]
    result = record.run(argv, executable=str(executable), timeout=30, mutation=False)
    require(result.returncode == 0 and len(record.receipts) == 1
            and record.receipts[0]["host_outcome"] == "exited"
            and record.receipts[0]["effects_uncertain"] is False, "offline signature command unsuccessful")
    before = int(datetime.strptime(pin["snapshot"]["timestamp"], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).timestamp())
    signers = release["required_primary_fingerprints"]
    signatures = debian.signature_status(result.stderr, required=signers, allowed=signers, signed_before=before)
    proof = inspect_packages(source, pin, result.stdout)
    check_tool()
    read_input(source, keyring)
    read_input(source, release)
    require(not list(gnupg.iterdir()), "gpgv used unexpected local state")
    require({str(path): driver.sha256(driver.regular(path)) for path in implementation} == source_hashes,
            "admission implementation/input changed during verification")
    proof.update(schema_version=1, scope="authenticated_inputs_not_guest_execution", pin_sha256=PIN_SHA256,
                 base_reference=pin["base"]["reference"], signatures=signatures, gpgv=tool,
                 source=str(source), implementation_inputs=source_hashes, maintainer_scripts_executed=False)
    record.persist(evidence / "input-verification.json", proof, create=True)
    return proof


def stage_packages(source, destination, *, pin_path=PIN):
    """Copy only frozen public DEBs into a new private build-context directory.

    Admission must have succeeded first. This rechecks content while copying;
    it deliberately does not copy private keys, source reports, or whole trees.
    """
    pin = load(pin_path)
    source = canonical(source)
    destination = Path(destination)
    require(destination.is_absolute() and destination.parent == destination.parent.resolve(strict=True)
            and not os.path.lexists(destination), "fresh canonical package staging directory required")
    contents = [(row["filename"], read_input(source, row)) for row in pin["packages"]]
    contents.append(("manifest.json", (json.dumps(guest_manifest(pin), sort_keys=True, indent=2) + "\n").encode()))
    destination.mkdir(mode=0o700)
    for name, raw in contents:
        fd = os.open(destination / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    for row in pin["packages"]:
        read_input(destination, row)
    require(image_input.parse(driver.regular(destination / "manifest.json", limit=16384)) == guest_manifest(pin),
            "staged public manifest differs")
    return {"directory": str(destination), "tree_sha256": driver.tree_digest(destination),
            "guest_manifest": guest_manifest(pin)}
