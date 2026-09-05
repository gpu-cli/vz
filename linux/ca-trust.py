#!/usr/bin/env python3
"""Offline pinned public CA assembly; never reads host trust or downloads."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat


def require(value, message):
    if not value:
        raise ValueError(message)


def read(path, limit=1024 * 1024):
    require(path == path.resolve(strict=True), "noncanonical CA input")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 < before.st_size <= limit,
                "bounded regular single-link CA input required")
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(len(data) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns), "CA input changed")
        return data


def verify(source):
    metadata = read(source / "inputs.json", 8192)
    pin = json.loads(metadata)
    require(set(pin) == {"schema_version", "source", "version", "metadata_url", "archive_url", "archive_sha256",
                         "bundle_member", "bundle_sha256", "bundle_bytes", "certificate_count", "license_member",
                         "license_sha256", "license", "scope"}, "unknown CA pin fields")
    require(pin["schema_version"] == 1 and pin["source"] == "certifi" and pin["license"] == "MPL-2.0" and
            pin["scope"] == "public_Mozilla_roots_only_no_host_trust" and
            re.fullmatch(r"[0-9]{4}\.[0-9]{1,2}\.[0-9]{1,2}", pin["version"]), "unexpected CA provenance")
    require(pin["metadata_url"] == "https://pypi.org/pypi/certifi/" + pin["version"] + "/json" and
            re.fullmatch(r"https://files\.pythonhosted\.org/packages/[0-9a-f/]+/certifi-" +
                         re.escape(pin["version"]) + r"-py3-none-any\.whl", pin["archive_url"]) and
            re.fullmatch(r"[0-9a-f]{64}", pin["archive_sha256"]), "unversioned CA source")
    bundle, license_bytes = read(source / "cacert.pem"), read(source / "LICENSE", 8192)
    require(pin["bundle_member"] == "certifi/cacert.pem" and
            pin["license_member"] == "certifi-" + pin["version"] + ".dist-info/licenses/LICENSE", "wrong archive member")
    for content, key in ((bundle, "bundle_sha256"), (license_bytes, "license_sha256")):
        require(hashlib.sha256(content).hexdigest() == pin[key], "CA input digest mismatch")
    require(len(bundle) == pin["bundle_bytes"] and pin["certificate_count"] > 0 and
            bundle.count(b"-----BEGIN CERTIFICATE-----") == bundle.count(b"-----END CERTIFICATE-----") ==
            pin["certificate_count"] and b"PRIVATE KEY" not in bundle, "invalid public CA bundle inventory")
    return pin, bundle, license_bytes, metadata


def install(source, root):
    pin, bundle, license_bytes, metadata = verify(source)
    script = read(source / "install.sh", 16384)
    require(root == root.resolve(strict=True) and root.is_dir(), "canonical precreated initramfs root required")
    files = {"etc/vz/ca-certificates.crt": bundle,
             "etc/vz/ca-trust.sha256": (pin["bundle_sha256"] + "\n").encode(),
             "etc/vz/install-ca-trust.sh": script,
             "usr/share/licenses/vz-ca-trust/LICENSE": license_bytes,
             "usr/share/licenses/vz-ca-trust/inputs.json": metadata}
    # Validate every destination before creating any file; no symlink traversal
    # or adoption/overwrite of an existing rootfs trust store.
    for name in files:
        current = root
        for part in Path(name).parts[:-1]:
            current = current / part
            require(not current.is_symlink() and (not current.exists() or current.is_dir()), "redirected CA destination")
        require(not os.path.lexists(root / name), "CA destination already exists")
    empty = root / "etc/vz/empty-ca-directory"
    require(not os.path.lexists(empty), "CA empty-directory destination already exists")
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o444)
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    empty.mkdir(mode=0o555)
    require(read(root / "etc/vz/ca-certificates.crt") == bundle, "assembled CA bytes differ")
    return pin


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--install-root", type=Path)
    args = parser.parse_args()
    pin = install(args.source, args.install_root) if args.install_root else verify(args.source)[0]
    print(json.dumps({"bundle_sha256": pin["bundle_sha256"], "certificate_count": pin["certificate_count"],
                      "scope": pin["scope"]}, sort_keys=True))


if __name__ == "__main__":
    main()
