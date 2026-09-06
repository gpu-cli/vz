#!/usr/bin/env python3
"""Package explicitly supplied local Apple developer tools for a DEV image.

This is maintainer preparation, not an installer download or publication step.
CLT archives contain the selected SDK, usr and Library trees; Xcode archives
contain the complete application bundle. Root ownership,
timestamps and archive ordering are normalized; source files remain untouched.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_archive(source, archive, selected_sdk, layout="clt"):
    """Write normalized inputs; reject links that escape the selected toolchain."""
    source = source.resolve(strict=True)
    root_name = "Xcode.app" if layout == "xcode" else "CommandLineTools"

    def normalize(info):
        # This obsolete CLT alias points outside the toolchain at a missing
        # SharedFrameworks tree. Swift builds do not use the crashlog utility.
        if info.name == "CommandLineTools/usr/bin/crashlog":
            return None
        assert info.isfile() or info.isdir() or info.issym() or info.islnk(), info.name
        if info.issym():
            path = source / Path(info.name).relative_to(root_name)
            assert path.resolve().is_relative_to(source), "external toolchain symlink: " + info.name
            assert not Path(info.linkname).is_absolute(), "absolute toolchain symlink: " + info.name
        info.uid = info.gid = 0
        info.uname = info.gname = "root"
        info.mtime = 0
        info.mode &= 0o777
        info.pax_headers = {}
        return info

    with archive.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", filename="",
                                                mtime=0, compresslevel=1) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as tar:
            if layout == "xcode":
                tar.add(source, arcname=root_name, filter=normalize)
            else:
                for entry in ["usr", "Library", selected_sdk, "SDKs/MacOSX.sdk"]:
                    tar.add(source / entry, arcname=root_name + "/" + entry, filter=normalize)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--layout", choices=["clt", "xcode"], default="clt")
    args = parser.parse_args()
    source = args.source.resolve(strict=True)
    output = args.output.resolve()
    assert not output.is_relative_to(source), "output must be outside source"
    output.mkdir(mode=0o700)
    developer = source / "Contents/Developer" if args.layout == "xcode" else source
    binaries = "Toolchains/XcodeDefault.xctoolchain/usr/bin" if args.layout == "xcode" else "usr/bin"
    sdks = "Platforms/MacOSX.platform/Developer/SDKs" if args.layout == "xcode" else "SDKs"
    env = {"DEVELOPER_DIR": str(developer), "LC_ALL": "C", "HOME": os.environ["HOME"],
           "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"}

    def query(*command):
        return subprocess.check_output(command, env=env, text=True).strip()

    sdk = query("/usr/bin/xcrun", "--sdk", "macosx", "--show-sdk-version")
    assert sdk and len(sdk) <= 32 and all(part.isascii() and part.isdecimal() for part in sdk.split('.'))
    selected_sdk = sdks + "/MacOSX" + sdk + ".sdk"
    assert (developer / sdks / "MacOSX.sdk").resolve() == (developer / selected_sdk).resolve()
    anchors = [binaries + "/" + name for name in
               ["swift-frontend", "swift-driver", "swift-package", "clang", "ld"]]
    anchors.append(selected_sdk + "/SDKSettings.json")
    swift_version = subprocess.check_output([str(developer / binaries / "swift"), "--version"],
                                           env=env, stderr=subprocess.STDOUT, text=True).strip()
    manifest = dict(schema_version=1, layout=args.layout,
                    swift_version=swift_version,
                    sdk_version=sdk, files={name: digest(developer / name) for name in anchors})

    archive = output / "toolchain.tar.gz"
    write_archive(source, archive, selected_sdk, args.layout)
    # Reject source drift in the actual compiler/SDK anchors during packaging.
    assert manifest["files"] == {name: digest(developer / name) for name in anchors}
    manifest["archive"] = dict(sha256=digest(archive), size_bytes=archive.stat().st_size)
    data = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    (output / "toolchain.json").write_bytes(data)
    pin = hashlib.sha256(data).hexdigest()
    (output / "toolchain-pin.json").write_text(json.dumps(dict(toolchain_sha256=pin), indent=2) + "\n")
    print(json.dumps(dict(toolchain_sha256=pin, archive=manifest["archive"],
                         swift_version=manifest["swift_version"], sdk_version=sdk)), flush=True)


if __name__ == "__main__":
    main()
