#!/usr/bin/env python3
"""Physical Apple-silicon DEV gate using signed installed binaries and an exact bundle.

Requires tmux. Inputs are explicit maintainer artifacts; no guest setup or host
sudo occurs in this consumer flow. On failure retain the daemon for diagnosis
and positive cleanup through the recorded installation's public CLI.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--resume-layout", type=Path,
                        help="continue checks against an unchanged retained installation")
    parser.add_argument("--expect-preparation-failure", action="store_true",
                        help="exercise a deliberately corrupt local fixture and public Delete")
    parser.add_argument("--require-swift", action="store_true",
                        help="require a pinned toolchain and build/test/run the Swift guest fixture")
    args = parser.parse_args()
    tmux = shutil.which("tmux")
    if not tmux:
        parser.error("tmux is required for the interactive gate")
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700)
    retained = json.loads(args.resume_layout.read_text()) if args.resume_layout else None
    root = Path(retained["root"]) if retained else Path(tempfile.mkdtemp(prefix="vzmac-cli-", dir="/private/tmp"))
    binary_dir = root / "install/bin"
    binary_dir.mkdir(parents=True, mode=0o700, exist_ok=retained is not None)
    runtime = root / "r"
    runtime.mkdir(mode=0o700, exist_ok=retained is not None)
    project = root / "project"
    project.mkdir(mode=0o700, exist_ok=retained is not None)
    env = {
        "PATH": f"{binary_dir}:/usr/bin:/bin:/usr/sbin:/sbin",
        "LC_ALL": "C", "NO_COLOR": "1", "HOME": os.environ["HOME"],
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
        "VZ_RUNTIME_STATE_DB": str(root / "state.db"),
        "VZ_RUNTIME_DATA_DIR": str(runtime),
        "VZ_RUNTIME_DAEMON_SOCKET": str(runtime / "d.sock"),
    }
    results = []

    def run(name, argv, *, cwd=project, input=None, timeout=120, expected=0):
        argv = list(map(str, argv))
        started = time.monotonic()
        with (evidence / f"{name}.stdout").open("wb") as out, \
                (evidence / f"{name}.stderr").open("wb") as err:
            result = subprocess.run(argv, cwd=cwd, env=env, input=input,
                                    stdout=out, stderr=err, timeout=timeout)
        receipt = dict(name=name, argv=argv, exit_code=result.returncode,
                       elapsed_seconds=time.monotonic() - started)
        (evidence / f"{name}.json").write_text(json.dumps(receipt, indent=2))
        results.append(receipt)
        print(json.dumps(receipt), flush=True)
        assert result.returncode == expected, (
            name + ": " + (evidence / f"{name}.stderr").read_text()[-3000:])
        return (evidence / f"{name}.stdout").read_text()

    def cli(name, verb, *argv, **kwargs):
        return run(name, [binary_dir / "vz", verb, "--environment", "native-e2e",
                          *argv], **kwargs)

    def terminal_test():
        socket = str(root / "tmux.sock")
        base = [tmux, "-S", socket]

        def control(*argv):
            return subprocess.check_output([*base, *argv], env=env, text=True)

        def frame():
            return control("capture-pane", "-p", "-S", "-200", "-t", "native:0.0")

        def wait_for(text, seconds=20):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                captured = frame()
                (evidence / "terminal.txt").write_text(captured)
                if text in captured:
                    return captured
                time.sleep(0.1)
            raise AssertionError(f"terminal did not produce {text!r}")

        def send(command):
            control("send-keys", "-t", "native:0.0", "-l", command)
            control("send-keys", "-t", "native:0.0", "Enter")

        script = root / "terminal.sh"
        script.write_text(
            "#!/bin/sh\ncd " + shlex.quote(str(project)) + "\n" +
            shlex.join([str(binary_dir / "vz"), "exec", "--environment",
                        "native-e2e", "-t", "--", "/bin/sh"]) +
            '\ncode=$?\nprintf "%s\\n" "$code" > ' +
            shlex.quote(str(evidence / "terminal-exit-code")) + "\nsleep 300\n")
        control("new-session", "-d", "-s", "native", "-x", "80", "-y", "24",
                "/bin/sh " + shlex.quote(str(script)))
        control("set-option", "-t", "native", "status", "off")
        try:
            send("test -t 0 && test -t 1 && printf '%s%s\\n' terminal- ready")
            wait_for("terminal-ready")
            control("resize-window", "-t", "native:0", "-x", "100", "-y", "35")
            # The public CLI polls local dimensions every 250 ms. Observe the
            # guest's eventual size instead of racing that control round trip.
            deadline = time.monotonic() + 10
            while "35 100" not in frame():
                assert time.monotonic() < deadline, "guest terminal did not resize"
                send("stty size")
                time.sleep(0.3)
            send("sleep 120")
            time.sleep(0.5)
            control("send-keys", "-t", "native:0.0", "C-c")
            send("printf '%s%s\\n' interrupt- recovered")
            wait_for("interrupt-recovered")
            send("exit 17")
            deadline = time.monotonic() + 20
            while not (evidence / "terminal-exit-code").exists():
                assert time.monotonic() < deadline, "PTY did not terminate"
                time.sleep(0.1)
            assert (evidence / "terminal-exit-code").read_text() == "17\n"
            (evidence / "terminal.txt").write_text(frame())
        finally:
            control("kill-server")

    for name in ["vz", "vz-runtimed"]:
        destination = binary_dir / name
        if retained:
            assert hashlib.sha256(destination.read_bytes()).hexdigest() == retained["binary_sha256"][name]
        else:
            destination.write_bytes((args.release_dir / name).read_bytes())
            destination.chmod(0o700)
        run(name + "-signature", ["/usr/bin/codesign", "--verify", "--strict",
                                 destination], cwd=root)
    if not retained:
        run("catalog", [binary_dir / "vz-runtimed", "--write-installed-machine-target-catalog",
                    root / "install", "--installed-release-version", "0.4.0-dev",
                    "--installed-native-bundle", args.bundle.resolve(),
                        "--installed-native-manifest-sha256", args.manifest], cwd=root)
    else:
        assert args.manifest == retained["manifest"] and str(args.bundle.resolve()) == retained["bundle"]
    release = json.loads((args.bundle / args.manifest).read_text())
    if args.require_swift:
        assert release["toolchain_sha256"], "Swift gate requires a release-pinned toolchain"
    definition = {
        "schema_version": 1, "project_id": "prj_" + uuid.uuid4().hex,
        "name": "native-macos-user-e2e", "environment": {
            "schema_version": 1, "machines": [{
                "schema_version": 1, "name": "mac", "profile": "developer",
                "target": {"os": "macos", "arch": "aarch64", "image": "vz-macos",
                           "channel": "latest"},
                "resources": {"cpus": 4, "memory_mb": 8192},
                "requested_capabilities": {"capabilities": ["posix_exec", "posix_pty"]},
            }],
        },
    }
    if retained:
        definition = retained["definition"]
        assert json.loads((project / "vz.json").read_text()) == definition
    else:
        (project / "vz.json").write_text(json.dumps(definition, indent=2) + "\n")
        run("git-init", ["/usr/bin/git", "init", "--quiet"])
    (evidence / "layout.json").write_text(json.dumps(dict(
        root=str(root), definition=definition, environment=env, manifest=args.manifest,
        bundle=str(args.bundle.resolve()), euid=os.geteuid(), binary_sha256={
            name: hashlib.sha256((binary_dir / name).read_bytes()).hexdigest()
            for name in ["vz", "vz-runtimed"]}), indent=2))
    summary = dict(scope="INSTALLED_NATIVE_DEV_LOCAL_BUNDLE", root=str(root),
                   aggregate_release_certified=False, results=results)

    swift_directory = "/Users/dev/vz-swift-fixture"

    def swift_identity(name):
        receipt = cli(name + "-receipt", "exec", "--no-stdin", "--", "/bin/cat",
                      "/usr/local/share/vz/toolchain.json")
        assert hashlib.sha256(receipt.encode()).hexdigest() == release["toolchain_sha256"]
        identity = json.loads(receipt)
        observed = cli(name + "-version", "exec", "--user", "dev", "--no-stdin", "--",
                       "/bin/sh", "-c", "/usr/bin/xcrun swift --version 2>&1; "
                       "/usr/bin/xcrun --sdk macosx --show-sdk-version")
        assert observed == identity["swift_version"] + "\n" + identity["sdk_version"] + "\n"
        return identity

    def swift_probe(name):
        record = json.loads(cli(name, "exec", "--user", "dev", "--workdir", swift_directory,
                               "--no-stdin", "--", "./.build/release/native-probe"))
        assert record["protocol"] == "vz-native-macos-swift" and record["protocol_version"] == 1
        assert record["os_version"] == release["macos_version"]
        assert record["os_build"] == release["macos_build"]
        assert record["hardware_model"] == "VirtualMac2,1" and record["pid"] > 1
        return record

    def swift_build():
        identity = swift_identity("swift-cold")
        fixture = Path(__file__).resolve().parents[1] / "tests/fixtures/vz-0.4/native-macos-swift"
        sources = [fixture / "Package.swift", *sorted((fixture / "Sources").rglob("*.swift")),
                   *sorted((fixture / "Tests").rglob("*.swift"))]
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for source in sources:
                tar.add(source, arcname=str(source.relative_to(fixture)), recursive=False)
        cli("swift-fixture-directory", "exec", "--user", "dev", "--no-stdin", "--",
            "/bin/mkdir", "-p", swift_directory)
        cli("swift-fixture-transfer", "exec", "--user", "dev", "--workdir", swift_directory,
            "--", "/usr/bin/tar", "-xf", "-", input=archive.getvalue())
        for phase, arguments in [("build", ["build", "-c", "release"]), ("test", ["test"])]:
            cli("swift-" + phase, "exec", "--user", "dev", "--workdir", swift_directory,
                "--timeout", "600", "--no-stdin", "--", "/usr/bin/xcrun", "swift",
                *arguments, timeout=630)
        test_log = ((evidence / "swift-test.stdout").read_text() +
                    (evidence / "swift-test.stderr").read_text())
        assert "physicalMacCannotSatisfyGuestProbe" in test_log and "1 test passed" in test_log
        summary["swift"] = dict(toolchain_sha256=release["toolchain_sha256"], identity=identity,
                                source_sha256={str(p.relative_to(fixture)): hashlib.sha256(p.read_bytes()).hexdigest()
                                               for p in sources}, probe=swift_probe("swift-run"))
    if retained:
        summary["continued_from"] = str(args.resume_layout.resolve())
    audit = None
    if not args.expect_preparation_failure:
        with (evidence / "platform-audit.log").open("w") as log:
            audit = subprocess.Popen([
                sys.executable, str(Path(__file__).parent / "helpers/native_macos_platform_audit.py"),
                str(evidence)], stdout=log, stderr=log)
    try:
        if args.expect_preparation_failure:
            cli("corrupt-up", "up", "--timeout", "30", timeout=45, expected=2)
            assert "mismatch" in (evidence / "corrupt-up.stderr").read_text().lower()
            cli("delete-never-started", "delete", "--timeout", "30", timeout=45)
            run("deleted-status", [binary_dir / "vz", "--json", "status"], expected=2)
            assert "environment_selection_required" in (evidence / "deleted-status.stderr").read_text()
            summary.update(passed=True, scope="INSTALLED_NATIVE_CORRUPT_INPUT_DELETE")
            (evidence / "summary.json").write_text(json.dumps(summary, indent=2))
            return
        cli("continued-up" if retained else "cold-up", "up", timeout=3700)
        status = run("ready-status", [binary_dir / "vz", "--json", "status"])
        assert "macos_native" in status and "ready" in status
        output = cli("native-version", "exec", "--no-stdin", "--", "/bin/sh", "-c",
                     "/usr/bin/sw_vers -productVersion; /usr/bin/sw_vers -buildVersion; "
                     "/usr/sbin/sysctl -n hw.model")
        assert output == (release["macos_version"] + "\n" + release["macos_build"] +
                          "\nVirtualMac2,1\n"), repr(output)
        assert cli("stdin", "exec", "--", "/bin/cat", input=b"native-stdin\n") == "native-stdin\n"
        cli("exit-code", "exec", "--no-stdin", "--", "/bin/sh", "-c",
            "printf stdout; printf stderr >&2; exit 23", expected=23)
        assert (evidence / "exit-code.stdout").read_text() == "stdout"
        assert (evidence / "exit-code.stderr").read_text() == "stderr"
        assert cli("guest-user", "exec", "--user", "dev", "--no-stdin", "--",
                   "/usr/bin/id", "-un") == "dev\n"
        assert cli("env-cwd", "exec", "--env", "VZ_E2E=literal $HOME; value",
                   "--workdir", "/private/var/tmp", "--no-stdin", "--", "/bin/sh", "-c",
                   'printf "%s\\n" "$VZ_E2E"; pwd') == "literal $HOME; value\n/private/var/tmp\n"
        if args.require_swift:
            swift_build()
        terminal_test()
        cli("cancel", "exec", "--timeout", "2", "--no-stdin", "--", "/bin/sh", "-c",
            "trap '' TERM; sleep 120 & printf '%s %s' $$ $! > /private/var/tmp/vz-cancel-pids; wait",
            expected=137, timeout=30)
        assert "execution deadline expired" in (evidence / "cancel.stderr").read_text()
        cli("cancel-reaped", "exec", "--no-stdin", "--", "/bin/sh", "-c",
            "set -eu; for pid in $(cat /private/var/tmp/vz-cancel-pids); do "
            "if kill -0 $pid 2>/dev/null; then exit 1; fi; done")
        cli("write-marker", "exec", "--no-stdin", "--", "/bin/sh", "-c",
            "printf native-persistence > /private/var/tmp/vz-cli-marker; /bin/sync")
        cli("stop", "stop", "--timeout", "120", timeout=150)
        run("stopped-status", [binary_dir / "vz", "--json", "status"])
        cli("warm-up", "up", "--timeout", "120", timeout=150)
        assert cli("read-marker", "exec", "--no-stdin", "--", "/bin/cat",
                   "/private/var/tmp/vz-cli-marker") == "native-persistence"
        if args.require_swift:
            assert swift_identity("swift-warm") == summary["swift"]["identity"]
            summary["swift"]["persisted_probe"] = swift_probe("swift-persisted-run")
        run("second-up", [binary_dir / "vz", "up", "--environment", "native-second",
                          "--timeout", "120"], timeout=150)
        run("second-isolation", [binary_dir / "vz", "exec", "--environment", "native-second",
                                 "--no-stdin", "--", "/bin/sh", "-c",
                                 "test ! -e /private/var/tmp/vz-cli-marker && "
                                 "test ! -e /Users/dev/vz-swift-fixture"])
        if args.require_swift:
            # Deliberately alter a disposable Machine's SDK anchor, then prove
            # the next boot refuses Ready while positive Delete still works.
            sdk = summary["swift"]["identity"]["sdk_version"]
            assert sdk and all(c in "0123456789." for c in sdk)
            anchor = f"/Library/Developer/CommandLineTools/SDKs/MacOSX{sdk}.sdk/SDKSettings.json"
            run("second-alter-sdk", [binary_dir / "vz", "exec", "--environment", "native-second",
                                    "--no-stdin", "--", "/bin/sh", "-c",
                                    "printf ' ' >> " + shlex.quote(anchor) + "; sync"])
            run("second-stop", [binary_dir / "vz", "stop", "--environment", "native-second",
                                "--timeout", "120"], timeout=150)
            run("altered-sdk-up", [binary_dir / "vz", "up", "--environment", "native-second",
                                   "--timeout", "120"], timeout=150, expected=2)
            assert "native Swift/toolchain pin verification failed" in (evidence / "altered-sdk-up.stderr").read_text()
        run("second-delete", [binary_dir / "vz", "delete", "--environment", "native-second",
                              "--timeout", "120"], timeout=150)
        assert cli("first-survives", "exec", "--no-stdin", "--", "/bin/cat",
                   "/private/var/tmp/vz-cli-marker") == "native-persistence"
        cli("delete", "delete", "--timeout", "120", timeout=150)
        run("deleted-status", [binary_dir / "vz", "--json", "status"], expected=2)
        assert "environment_selection_required" in (evidence / "deleted-status.stderr").read_text()
        summary["passed"] = True
    except BaseException as error:
        summary.update(passed=False, error=str(error))
        (evidence / "summary.json").write_text(json.dumps(summary, indent=2))
        print("FAILED: retaining original daemon; use layout.json for positive Stop/Delete.", flush=True)
        time.sleep(3600)
        raise
    (evidence / "summary.json").write_text(json.dumps(summary, indent=2))
    if audit is not None and audit.wait(timeout=15) != 0:
        summary.update(passed=False, error="platform audit failed; see platform-audit.log")
        (evidence / "summary.json").write_text(json.dumps(summary, indent=2))
        raise AssertionError(summary["error"])
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
