#!/usr/bin/env python3
"""Physical Apple-silicon DEV gate using signed installed binaries and an exact bundle.

Requires tmux. Inputs are explicit maintainer artifacts; no guest setup or host
sudo occurs in this consumer flow. On failure retain the daemon for diagnosis
and positive cleanup through the recorded installation's public CLI.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--expect-preparation-failure", action="store_true",
                        help="exercise a deliberately corrupt local fixture and public Delete")
    args = parser.parse_args()
    tmux = shutil.which("tmux")
    if not tmux:
        parser.error("tmux is required for the interactive gate")
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700)
    root = Path(tempfile.mkdtemp(prefix="vzmac-cli-", dir="/private/tmp"))
    binary_dir = root / "install/bin"
    binary_dir.mkdir(parents=True, mode=0o700)
    runtime = root / "r"
    runtime.mkdir(mode=0o700)
    project = root / "project"
    project.mkdir(mode=0o700)
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
            send("stty size")
            wait_for("35 100")
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
        destination.write_bytes((args.release_dir / name).read_bytes())
        destination.chmod(0o700)
        run(name + "-signature", ["/usr/bin/codesign", "--verify", "--strict",
                                 destination], cwd=root)
    run("catalog", [binary_dir / "vz-runtimed", "--write-installed-machine-target-catalog",
                    root / "install", "--installed-release-version", "0.4.0-dev",
                    "--installed-native-bundle", args.bundle.resolve(),
                    "--installed-native-manifest-sha256", args.manifest], cwd=root)
    release = json.loads((args.bundle / args.manifest).read_text())
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
    (project / "vz.json").write_text(json.dumps(definition, indent=2) + "\n")
    run("git-init", ["/usr/bin/git", "init", "--quiet"])
    (evidence / "layout.json").write_text(json.dumps(dict(
        root=str(root), definition=definition, environment=env, manifest=args.manifest,
        bundle=str(args.bundle.resolve()), euid=os.geteuid(), binary_sha256={
            name: hashlib.sha256((binary_dir / name).read_bytes()).hexdigest()
            for name in ["vz", "vz-runtimed"]}), indent=2))
    summary = dict(scope="INSTALLED_NATIVE_DEV_LOCAL_BUNDLE", root=str(root),
                   aggregate_release_certified=False, results=results)
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
        cli("cold-up", "up", timeout=3700)
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
        terminal_test()
        cli("cancel", "exec", "--timeout", "2", "--no-stdin", "--", "/bin/sh", "-c",
            "trap '' TERM; sleep 120 & printf '%s %s' $$ $! > /private/var/tmp/vz-cancel-pids; wait",
            expected=4, timeout=30)
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
        run("second-up", [binary_dir / "vz", "up", "--environment", "native-second",
                          "--timeout", "120"], timeout=150)
        run("second-isolation", [binary_dir / "vz", "exec", "--environment", "native-second",
                                 "--no-stdin", "--", "/bin/sh", "-c",
                                 "test ! -e /private/var/tmp/vz-cli-marker"])
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
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
