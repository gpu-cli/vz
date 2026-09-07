#!/usr/bin/env python3
"""Run fresh installed local macOS setup, reuse, and native CLI lifecycle gates.

The selected prefix must contain signed release binaries and have no setup cache.
Apple inputs are obtained by the actual setup executable. No maintainer image,
patch, source-tree helper executable, or manual catalog registration is used.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--xcode", type=Path, help="opt into Xcode; omit to test clean macOS")
    parser.add_argument("--ipsw", type=Path)
    parser.add_argument("--reuse-download-cache", action="store_true", help="retry with verified downloads only; no prepared images or setup receipts may exist")
    parser.add_argument("--accept-xcode-license", action="store_true")
    parser.add_argument("--preserve-other-template", action="store_true", help="keep an already validated template of the other kind in this installation")
    args = parser.parse_args()
    assert bool(args.xcode) == args.accept_xcode_license, "Xcode tests require explicit license authorization; clean tests do not accept a license"
    assert shutil.which("tmux", path=os.pathsep.join([
        os.environ.get("PATH", os.defpath), "/opt/homebrew/bin", "/usr/local/bin"])), "tmux is required for the installed interactive lifecycle gate"
    prefix = args.prefix.resolve(strict=True)
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700)
    cache = prefix / "macos-local"
    before_receipts = set(cache.glob("*.json"))
    if args.preserve_other_template:
        assert args.reuse_download_cache and before_receipts
        for path in before_receipts:
            prior = json.loads(path.read_text())
            manifest = json.loads((Path(prior["bundle"]) / prior["manifest_sha256"]).read_text())
            assert bool(manifest["toolchain_sha256"]) != bool(args.xcode), "this variant must be prepared fresh"
    if args.reuse_download_cache:
        assert args.preserve_other_template or not before_receipts, "no completed setup receipt may exist"
        assert not list(cache.glob("setup-*")), "no interrupted setup stage may exist"
        if not args.preserve_other_template:
            assert not (cache / "images").exists(), "no prepared images may exist"
            assert not list((cache / "cache/templates").glob("*")), "no prepared template may exist"
    else:
        assert not cache.exists(), "fresh setup cache required"
    binary = prefix / "bin/vz-macos-setup"
    env = os.environ | {"PATH": f"{prefix}/bin:/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C"}
    base = [str(binary), "--prefix", str(prefix), "--json"]
    if args.xcode:
        base += ["--xcode", str(args.xcode)]
    if args.ipsw:
        base += ["--ipsw", str(args.ipsw.resolve(strict=True))]
    prepared = base + (["--accept-xcode-license"] if args.xcode else [])
    results = []
    summary = dict(scope="INSTALLED_LOCAL_MACOS_SETUP", passed=False,
                   default_apple_acquisition=args.ipsw is None,
                   fresh_download_cache=not args.reuse_download_cache,
                   apple_https_download=args.ipsw is None and not args.reuse_download_cache,
                   prefix=str(prefix), variant="xcode" if args.xcode else "clean", results=results,
                   binary_sha256={name: hashlib.file_digest((prefix / "bin" / name).open("rb"), "sha256").hexdigest()
                                  for name in ["vz", "vz-runtimed", "vz-macos-setup", "vz-agent-loader", "vz-guest-agent"]})

    def save():
        (evidence / "summary.json").write_text(json.dumps(summary, indent=2))

    def run(name, command, expected=0):
        started = time.monotonic()
        with (evidence / f"{name}.stdout").open("wb") as out, (evidence / f"{name}.stderr").open("wb") as err:
            result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=out, stderr=err, env=env)
        record = dict(name=name, argv=list(map(str, command)), exit_code=result.returncode, elapsed_seconds=time.monotonic()-started)
        results.append(record); save(); print(json.dumps(record), flush=True)
        assert result.returncode == expected, (evidence / f"{name}.stderr").read_text()[-4000:]

    save()
    try:
        if args.xcode:
            run("requires-license", base, expected=1)
            assert "--accept-xcode-license" in (evidence / "requires-license.stderr").read_text()
        else:
            run("license-without-xcode", base + ["--accept-xcode-license"], expected=2)
            assert "--xcode" in (evidence / "license-without-xcode.stderr").read_text()
        # Interrupt preparation before any privileged operation and require clean retry.
        with (evidence / "cancel.stdout").open("wb") as out, (evidence / "cancel.stderr").open("wb") as err:
            child = subprocess.Popen(prepared, stdin=subprocess.DEVNULL, stdout=out, stderr=err, env=env)
            deadline = time.monotonic() + 60
            cancellable_phase = "Packaging local Xcode" if args.xcode else "Acquiring pinned Apple IPSW"
            while cancellable_phase not in (evidence / "cancel.stdout").read_text():
                assert child.poll() is None, (evidence / "cancel.stderr").read_text()
                assert time.monotonic() < deadline, "setup did not reach a cancellable phase"
                time.sleep(0.2)
            child.send_signal(signal.SIGINT)
            assert child.wait(timeout=120) != 0
        assert not list((prefix / "macos-local").glob("setup-*")), "cancelled stage was retained"
        assert set(cache.glob("*.json")) == before_receipts, "cancelled setup changed published receipts"
        run("cold-setup", prepared)
        receipts = list(set(cache.glob("*.json")) - before_receipts)
        assert len(receipts) == 1
        receipt = json.loads(receipts[0].read_text())
        bundle = Path(receipt["bundle"])
        manifest = receipt["manifest_sha256"]
        release = json.loads((bundle / manifest).read_text())
        assert release["schema_version"] == 2 and "base" not in release and "patch" not in release
        assert release["local_image"]["sha256"] == release["prepared_image"]["sha256"]
        assert bool(release["toolchain_sha256"]) == bool(args.xcode)
        if args.xcode:
            assert json.loads((bundle / "signature.json").read_text())["exit_code"] == 0
        else:
            assert json.loads((bundle / "clean.json").read_text())["exit_code"] == 0
        catalog = json.loads((prefix / "machine-target-catalog.json").read_text())
        assert any(entry["manifest"]["sha256"] == manifest and summary["variant"] in entry["channels"] for entry in catalog["macos"])
        for path in before_receipts:
            prior = json.loads(path.read_text())
            other = "clean" if args.xcode else "xcode"
            assert any(entry["manifest"]["sha256"] == prior["manifest_sha256"] and other in entry["channels"] for entry in catalog["macos"]), "other template selection was removed"
        summary.update(receipt=receipt, manifest=release); save()
        run("warm-setup", base)
        warm = [json.loads(line) for line in (evidence / "warm-setup.stdout").read_text().splitlines()]
        assert [event["phase"] for event in warm] == ["Using validated local macOS template"]
        assert not list((prefix / "macos-local").glob("setup-*"))
        run("native-lifecycle", [sys.executable, str(Path(__file__).with_name("run-installed-native-macos-e2e.py")),
            "--release-dir", str(prefix / "bin"), "--installed-prefix", str(prefix), "--bundle", str(bundle),
            "--manifest", manifest, "--require-swift" if args.xcode else "--require-clean",
            "--channel", summary["variant"], "--evidence", str(evidence / "lifecycle")])
        summary["passed"] = True; save()
    except BaseException as error:
        summary["error"] = repr(error); save(); raise


if __name__ == "__main__":
    main()
