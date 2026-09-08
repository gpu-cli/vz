"""Acquire the gate's run-frozen inputs from their checked-in pins.

These are inputs of the run rather than of the build, so a release candidate
does not carry them and the tree does not retain them. GOAL-0.4.0.md admits
either form — "pinned by immutable digest or retained in a content-addressed
replayable fixture" — and every byte here is reachable by digest from a pin that
is already checked in, so the digest form is the honest one: a fresh checkout
plus the pins reproduces them without a mutable tag.

Acquisition is cached by the pin's own digest. A cache entry is only ever reused
when the pin that produced it is byte-identical, so changing a pin cannot silently
reuse the artifact of the previous one.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import sys

from vz04_common import GateError, digest_file, read_regular, require

REGISTRY_PIN = "config/docker-registry-artifact-v3.1.1.json"
SSH_PIN = "config/docker-ssh-packages-bookworm-arm64.json"
SSH_PROVENANCE = "tests/fixtures/vz-0.4/docker-ssh-provenance"
HELPERS = Path(__file__).resolve().parent


def _helpers_on_path():
    if str(HELPERS) not in sys.path:
        sys.path.insert(0, str(HELPERS))


def cache_entry(cache_root: Path, kind: str, pin_sha256: str) -> Path:
    """Where an acquired input for this exact pin lives."""
    require(len(pin_sha256) == 64 and all(c in "0123456789abcdef" for c in pin_sha256), "pin digest")
    return Path(cache_root) / f"{kind}-{pin_sha256[:32]}"


def registry_inputs(repo_root: Path, cache_root: Path) -> dict:
    """The registry OCI layout and its deterministic load archive.

    Both are produced by helpers that already exist: `linux_docker_registry_
    acquire` fetches every blob by immutable descriptor and verifies its digest,
    and `linux_docker_registry_archive` builds the archive from that layout and
    replays it independently before returning. Nothing new is trusted here.
    """
    _helpers_on_path()
    import linux_docker_registry_archive as archive_module
    import linux_docker_registry_acquire as acquire_module
    import linux_docker_registry_fixture as fixture

    pin_path = Path(repo_root) / REGISTRY_PIN
    require(pin_path.is_file() and not pin_path.is_symlink(), f"registry pin missing: {pin_path}")
    entry = cache_entry(cache_root, "registry", digest_file(pin_path))
    layout, archive = entry / "public" / "layout", entry / "registry.tar"
    if layout.is_dir() and archive.is_file():
        # Reuse only what still matches the pin: validation is the same code the
        # harness admits these inputs with, so a damaged cache fails here rather
        # than inside a run.
        pins = fixture.decode(read_regular(pin_path))
        archive_module.validate_archive(archive, layout=layout, pins=pins)
        return {"registry-layout": layout, "registry-archive": archive}

    if entry.exists():
        # A partial acquisition is never adopted; it is replaced whole.
        shutil.rmtree(entry)
    # `acquire` and `create_archive` both require a canonical existing parent
    # and refuse to overwrite, so the entry is created and they fill it.
    entry.mkdir(mode=0o700, parents=True)
    pins = fixture.decode(read_regular(pin_path))
    try:
        acquire_module.acquire(entry / "public", pins=pins)
        archive_module.create_archive(layout, pins=pins, output=archive)
    except (ValueError, OSError) as error:
        raise GateError(f"registry input acquisition failed: {error}") from error
    return {"registry-layout": layout, "registry-archive": archive}


def ssh_inputs(repo_root: Path, cache_root: Path) -> dict:
    """The Debian input directory the SSH suite verifies.

    Two halves, and the pin decides which is which. Everything it gives a
    `repository_path` is fetched by digest and kept out of the tree — the
    Release, the index, the packages and the source archives, about 23 MB.
    Everything it does not is a record of the admission that produced the pin,
    has no generator here, and is retained beside the pin instead.

    Composing them is not verification: `linux_docker_ssh_input.verify` still
    performs the whole offline trust chain over the result, and checks every one
    of these bytes against the digest already in the pin.
    """
    _helpers_on_path()
    import linux_docker_ssh_acquire as acquire_module
    import linux_docker_ssh_input as ssh_input

    repo_root = Path(repo_root)
    pin_path = repo_root / SSH_PIN
    require(pin_path.is_file() and not pin_path.is_symlink(), f"SSH pin missing: {pin_path}")
    entry = cache_entry(cache_root, "ssh", digest_file(pin_path))
    inputs = entry / "inputs"
    pin = ssh_input.load(pin_path)
    retained = repo_root / SSH_PROVENANCE
    expected = {row["filename"] for row in
                [pin["base"]["keyring"], pin["release"], pin["packages_index"],
                 *pin["packages"], *pin["source_proofs"]]}
    if inputs.is_dir() and expected <= {path.name for path in inputs.iterdir()}:
        return {"ssh-packages": inputs}

    if entry.exists():
        shutil.rmtree(entry)
    entry.mkdir(mode=0o700, parents=True)
    try:
        acquire_module.acquire(inputs, pin)
    except (ValueError, OSError) as error:
        raise GateError(f"SSH input acquisition failed: {error}") from error
    for row in [pin["base"]["keyring"], *pin["source_proofs"]]:
        if row.get("repository_path"):
            continue
        source = retained / row["filename"]
        require(source.is_file() and not source.is_symlink(),
                f"retained SSH provenance missing: {source}")
        raw = read_regular(source)
        require(len(raw) == row["size"], f"retained {row['filename']} has the wrong size")
        (inputs / row["filename"]).write_bytes(raw)
    missing = sorted(expected - {path.name for path in inputs.iterdir()})
    require(not missing, "SSH input directory is incomplete: " + ", ".join(missing))
    return {"ssh-packages": inputs}


def acquired_inputs(repo_root: Path, cache_root: Path) -> dict:
    """Every run-frozen input the Docker lane needs, by option name.

    Each is acquired from a checked-in pin, so a fresh checkout plus the pins
    reproduces every one of them.
    """
    inputs = registry_inputs(Path(repo_root), Path(cache_root))
    inputs.update(ssh_inputs(Path(repo_root), Path(cache_root)))
    for name, path in inputs.items():
        require(Path(path).exists(), f"acquired input missing after acquisition: {name}")
    return inputs
