#!/usr/bin/env python3
"""Build the pinned, static ext4 tools; Docker is build infrastructure only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import struct
import subprocess
import tempfile

VERSION = "1.47.3"
# https://www.kernel.org/pub/linux/kernel/people/tytso/e2fsprogs/v1.47.3/sha256sums.asc
SOURCE_SHA256 = "857e6ef800feaa2bb4578fbc810214be5d3c88b072ea53c5384733a965737329"
TOOLS = ("mke2fs", "dumpe2fs")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_elf(path):
    """Reject dynamic or foreign binaries even on hosts without readelf."""
    if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError(f"not a regular executable: {path}")
    with path.open("rb") as binary:
        header = binary.read(64)
        if len(header) != 64 or header[:7] != b"\x7fELF\x02\x01\x01":
            raise ValueError(f"not a little-endian ELF64 executable: {path}")
        elf_type, machine = struct.unpack_from("<HH", header, 16)
        if elf_type != 2 or machine != 183:
            raise ValueError(f"not a static Linux/arm64 executable: {path}")
        offset = struct.unpack_from("<Q", header, 32)[0]
        entry_size, count = struct.unpack_from("<HH", header, 54)
        if entry_size != 56 or not 1 <= count <= 64:
            raise ValueError(f"invalid ELF program table: {path}")
        binary.seek(offset)
        table = binary.read(entry_size * count)
        if len(table) != entry_size * count:
            raise ValueError(f"truncated ELF program table: {path}")
        kinds = [struct.unpack_from("<I", table, i * entry_size)[0] for i in range(count)]
        if 1 not in kinds or 2 in kinds or 3 in kinds:
            raise ValueError(f"dynamic or unloadable ELF: {path}")


def cached(out, recipe):
    try:
        manifest_path = out / "e2fsprogs.build.json"
        if manifest_path.is_symlink():
            return False
        manifest = json.loads(manifest_path.read_text())
        if (manifest["schema_version"] != 1 or manifest["version"] != VERSION
                or manifest["source_sha256"] != SOURCE_SHA256
                or manifest["recipe_sha256"] != recipe
                or set(manifest["binaries"]) != set(TOOLS)):
            return False
        for name in TOOLS:
            verify_elf(out / name)
            if sha256(out / name) != manifest["binaries"][name]:
                return False
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


def run(command, **kwargs):
    subprocess.run(command, check=True, **kwargs)


def build_native(archive, out, jobs, recipe):
    if platform.system() != "Linux" or platform.machine() not in ("aarch64", "arm64"):
        raise ValueError("native e2fsprogs build requires Linux/arm64")
    # Source remains on the builder's case-sensitive private filesystem.
    with tempfile.TemporaryDirectory(prefix="vz-e2fsprogs-") as temporary:
        build_root = Path(temporary)
        run(["tar", "-xf", str(archive), "-C", temporary])
        source = build_root / f"e2fsprogs-{VERSION}"
        multiarch = subprocess.check_output(["musl-gcc", "-dumpmachine"], text=True).strip()
        environment = dict(os.environ, CC="musl-gcc", LDFLAGS="-static",
                           CPPFLAGS=f"-idirafter /usr/include/{multiarch} -idirafter /usr/include",
                           SOURCE_DATE_EPOCH="1752013980")
        run([str(source / "configure"), "--disable-elf-shlibs", "--disable-nls",
             "--enable-libuuid", "--enable-libblkid", "--disable-uuidd",
             "--disable-fsck", "--disable-fuse2fs",
             "--without-crond-dir", "--without-udev-rules-dir"],
            cwd=source, env=environment)
        run(["make", f"-j{jobs}", "libs"], cwd=source, env=environment)
        run(["make", f"-j{jobs}", "-C", "misc", "mke2fs.static", "dumpe2fs.static"],
            cwd=source, env=environment)
        hashes = {}
        for name in TOOLS:
            candidate = source / "misc" / f"{name}.static"
            run(["strip", str(candidate)])
            verify_elf(candidate)
            run([str(candidate), "-V"])
            hashes[name] = sha256(candidate)
        # The manifest is published last; interruption cannot validate mixed output.
        for name in TOOLS:
            staged = out / f".{name}.pending"
            shutil.copyfile(source / "misc" / f"{name}.static", staged)
            staged.chmod(0o755)
            staged.replace(out / name)
        manifest = {"schema_version": 1, "version": VERSION,
                    "source_sha256": SOURCE_SHA256, "recipe_sha256": recipe,
                    "binaries": hashes}
        staged = out / ".e2fsprogs.build.json.pending"
        staged.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
        staged.replace(out / "e2fsprogs.build.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    if args.jobs < 1 or args.archive.is_symlink() or args.out_dir.is_symlink():
        raise ValueError("invalid jobs or symlink build inputs")
    archive = args.archive.resolve(strict=True)
    out = args.out_dir.resolve()
    if out in (Path("/"), Path(__file__).resolve().parent):
        raise ValueError("output must be a dedicated artifact directory")
    if sha256(archive) != SOURCE_SHA256:
        raise ValueError("e2fsprogs source checksum mismatch")
    recipe = sha256(Path(__file__).resolve())
    out.mkdir(parents=True, exist_ok=True)
    if cached(out, recipe):
        print(f"Verified cached static e2fsprogs {VERSION}")
        return
    if platform.system() == "Darwin":
        context = os.environ.get("LINUX_DOCKER_CONTEXT")
        if not context:
            raise ValueError("set LINUX_DOCKER_CONTEXT to an explicit local builder context")
        environment = {key: value for key, value in os.environ.items()
                       if key not in {"DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS",
                                      "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"}}
        docker = ["docker", "--context", context]
        endpoint = subprocess.check_output(docker + ["context", "inspect", context,
            "--format", "{{.Endpoints.docker.Host}}"], env=environment, text=True).strip()
        if not endpoint.startswith("unix:///"):
            raise ValueError("e2fsprogs builder requires a local Unix-socket context")
        identity = subprocess.check_output(docker + ["image", "inspect",
            os.environ.get("LINUX_DOCKER_BUILDER", "vz-linux-builder"), "--format",
            "{{.Id}} {{.Os}} {{.Architecture}}"], env=environment, text=True).strip().split()
        if len(identity) != 3 or identity[1:] != ["linux", "arm64"]:
            raise ValueError("e2fsprogs builder must be Linux/arm64")
        print(f"Building e2fsprogs {VERSION} using {identity[0]}", flush=True)
        run(docker + ["run", "--rm", "--network", "none",
            "-v", f"{Path(__file__).resolve()}:/build-e2fsprogs.py:ro",
            "-v", f"{archive}:/source.tar.xz:ro", "-v", f"{out}:/output",
            identity[0], "flock", "/output/.e2fsprogs-build.lock", "python3",
            "/build-e2fsprogs.py", "--archive", "/source.tar.xz", "--out-dir",
            "/output", "--jobs", str(args.jobs)], env=environment)
    else:
        build_native(archive, out, args.jobs, recipe)
    if not cached(out, recipe):
        raise ValueError("built e2fsprogs did not pass final provenance verification")


if __name__ == "__main__":
    main()
