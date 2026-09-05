#!/bin/bash
# Reuse exact staged signed product artifacts; build only the DEV physical driver.
set -euo pipefail
script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
exec /usr/bin/python3 - "$script_path" "$@" <<'PYTHON_RUNNER'
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import subprocess
import sys
import time


TEST = "installed_public_up_exec_stop_machine_lifecycle"
TARGET = "topology_up_machine_e2e"
SCOPE = "DEV_PHYSICAL_PUBLIC_CLI_NOT_RELEASE_CERTIFICATION"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def file_digest(path):
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1,
                f"not a single-link regular file: {path}")
        hasher = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
        after = os.fstat(stream.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), f"file changed: {path}")
    return hasher.hexdigest()


def absolute_path(value, *, resolve_links=False):
    path = Path(value)
    require(path.is_absolute() and not any(character in str(path) for character in "\r\n\x00"),
            "paths must be absolute and contain no newline/NUL")
    resolved = path.resolve(strict=True)
    require(resolve_links or path == resolved, f"canonical path without symlinks required: {path}")
    return resolved


def executable(path):
    require(path.is_file() and not path.is_symlink() and os.access(path, os.X_OK),
            f"regular executable required: {path}")
    return file_digest(path)


def arguments(argv):
    parser = argparse.ArgumentParser(
        prog="scripts/run-topology-up-machine-e2e.sh",
        description="DEV physical Up/Exec/Stop proof using exact staged signed release artifacts. "
                    "Starts three local VMs; does not certify Docker parity or the 0.4 aggregate.",
        allow_abbrev=False)
    parser.add_argument("--release-dir", required=True, help="canonical directory containing signed vz and vz-runtimed")
    parser.add_argument("--docker", required=True, help="absolute installed Docker invocation path; symlink target is hashed")
    parser.add_argument("--evidence-dir", required=True, help="new absolute directory; existing paths are rejected")
    # Reject repeated selectors as well as unknown flags before filesystem effects.
    for option in ("--release-dir", "--docker", "--evidence-dir"):
        require(sum(token == option or token.startswith(option + "=") for token in argv) <= 1,
                f"duplicate option: {option}")
    return parser.parse_args(argv)


def preflight(args, script):
    release = absolute_path(args.release_dir)
    require(release.is_dir(), "release directory is not a directory")
    cli, daemon = release / "vz", release / "vz-runtimed"
    for path in (cli, daemon):
        require(path == path.resolve(strict=True), "release artifact symlink rejected")
    cli_sha, daemon_sha = executable(cli), executable(daemon)
    docker_invocation = Path(args.docker)
    docker = absolute_path(args.docker, resolve_links=True)
    docker_sha = executable(docker)
    evidence = Path(args.evidence_dir)
    require(evidence.is_absolute() and not any(character in str(evidence) for character in "\r\n\x00"),
            "absolute evidence path without newline/NUL required")
    require(not evidence.exists() and not evidence.is_symlink(), "evidence directory already exists")
    require(evidence.parent.is_dir() and evidence.parent == evidence.parent.resolve(strict=True),
            "evidence parent must already exist and be canonical")
    require(platform.system() == "Darwin" and platform.machine() == "arm64", "Apple-silicon macOS required")
    cargo = shutil.which("cargo")
    require(cargo is not None, "Cargo toolchain unavailable")
    repo = script.parent.parent
    source = repo / "crates/vz-cli/tests" / (TARGET + ".rs")
    require(source.is_file(), "physical driver source missing")
    inputs = {str(path): file_digest(path) for path in
              (script, source, repo / "crates/Cargo.lock", repo / "crates/Cargo.toml",
               repo / "crates/vz-cli/Cargo.toml", cli, daemon, docker)}
    return {"schema_version": 1, "scope": SCOPE, "repo": str(repo), "source": str(source),
            "release_dir": str(release), "cli": str(cli), "cli_sha256": cli_sha,
            "daemon": str(daemon), "daemon_sha256": daemon_sha,
            "docker_invocation": str(docker_invocation), "docker_canonical": str(docker),
            "docker_sha256": docker_sha, "docker_argv0": "docker", "evidence": str(evidence),
            "cargo_invocation": cargo, "input_sha256": inputs, "build_profile": "release",
            "host_os": platform.system(), "host_architecture": platform.machine(),
            "docker_parity_certified": False, "aggregate_release_certified": False}


def select_artifact(data, source):
    artifacts = set()
    for line in data.splitlines():
        item = json.loads(line)
        if item.get("reason") != "compiler-artifact":
            continue
        target = item.get("target", {})
        if (target.get("name") == TARGET and target.get("kind") == ["test"] and
                target.get("src_path") == str(source) and item.get("profile", {}).get("test") is True and
                item.get("executable") is not None):
            artifacts.add(item["executable"])
    require(len(artifacts) == 1, "Cargo did not emit exactly one matching physical test executable")
    path = absolute_path(artifacts.pop())
    executable(path)
    return path


def verify_inputs(inputs):
    for filename, digest in inputs.items():
        require(file_digest(Path(filename)) == digest, f"selected input changed: {filename}")


def record_command(root, label, argv, env, cwd):
    started = time.time_ns()
    monotonic = time.monotonic_ns()
    code = None
    command = {"argv": argv, "cwd": str(cwd), "started_unix_ns": started}
    (root / (label + ".command.json")).write_text(json.dumps(command, indent=2) + "\n")
    try:
        with (root / (label + ".stdout")).open("xb") as stdout, (root / (label + ".stderr")).open("xb") as stderr:
            result = subprocess.run(argv, env=env, cwd=cwd, stdin=subprocess.DEVNULL,
                                    stdout=stdout, stderr=stderr, check=False)
            code = result.returncode
    finally:
        receipt = {"exit_code": code, "elapsed_ns": time.monotonic_ns() - monotonic,
                   "stdout_sha256": file_digest(root / (label + ".stdout")),
                   "stderr_sha256": file_digest(root / (label + ".stderr"))}
        (root / (label + ".exit.json")).write_text(json.dumps(receipt, indent=2) + "\n")
    require(code == 0, f"{label} exited {code}; see retained raw logs")


def validate_test_result(stdout, physical):
    require(len(re.findall(r"^test result:", stdout, flags=re.MULTILINE)) == 1,
            "test summary missing or duplicated")
    require(re.search(r"^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;", stdout, re.MULTILINE),
            "the sole exact physical test did not pass without skips or filtering")
    require(physical.get("scope") == SCOPE and physical.get("error") is None and
            physical.get("cleanup_errors") == [] and physical.get("daemon_exit") == "exit status: 0",
            "physical receipt is absent, failed, or retains an uncertain daemon")
    require(physical["scenario"]["docker_parity_certified"] is False,
            "DEV lifecycle proof must not certify full Docker parity")


def checksums(root):
    rows = []
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "generated evidence contains a symlink")
        if path.is_dir():
            continue
        if path == root / "checksums.sha256":
            continue
        rows.append(f"{file_digest(path)}  {path.relative_to(root).as_posix()}\n")
    (root / "checksums.sha256").write_text("".join(rows))


def run(info):
    root, repo = Path(info["evidence"]), Path(info["repo"])
    os.umask(0o077)
    root.mkdir(mode=0o700)
    (root / "inputs.json").write_text(json.dumps(info, indent=2) + "\n")
    (root / "inputs.sha256").write_text("".join(f"{digest}  {filename}\n" for filename, digest in sorted(info["input_sha256"].items())))
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("VZ_", "DOCKER_", "COMPOSE_", "BUILDX_", "GIT_"))}
    env.update(RUSTC_WRAPPER="", CC_aarch64_apple_darwin="/usr/bin/clang",
               CXX_aarch64_apple_darwin="/usr/bin/clang++", GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null")
    outcome, error = "failed", None
    try:
        for name in ("cli", "daemon"):
            record_command(root, name + "-codesign", ["/usr/bin/codesign", "--verify", "--strict", info[name]], env, repo)
        record_command(root, "source-state", ["/usr/bin/git", "status", "--porcelain=v1", "--untracked-files=no"], env, repo)
        record_command(root, "source-commit", ["/usr/bin/git", "rev-parse", "HEAD"], env, repo)
        record_command(root, "cargo-metadata", [info["cargo_invocation"], "metadata", "--manifest-path",
                                               str(repo / "crates/Cargo.toml"), "--locked", "--offline",
                                               "--no-deps", "--format-version", "1"], env, repo)
        metadata = json.loads((root / "cargo-metadata.stdout").read_text())
        target = Path(metadata["target_directory"]).resolve()
        release = Path(info["release_dir"])
        require(release != target and target not in release.parents,
                "release-dir must contain staged signed files outside Cargo's build-output directory")
        build = [info["cargo_invocation"], "test", "--manifest-path", str(repo / "crates/Cargo.toml"),
                 "--locked", "--release", "-p", "vz-cli", "--test", TARGET, "--no-run", "--message-format=json"]
        print(f"DEV physical-driver build logs: {root}", flush=True)
        record_command(root, "driver-build", build, env, repo)
        artifact = select_artifact((root / "driver-build.stdout").read_text(), Path(info["source"]))
        verify_inputs(info["input_sha256"])
        staged_driver = root / "physical-driver"
        driver_sha = executable(artifact)
        shutil.copyfile(artifact, staged_driver)
        staged_driver.chmod(0o700)
        require(file_digest(staged_driver) == driver_sha, "driver changed while staging")
        (root / "driver-artifact.json").write_text(json.dumps({"cargo_artifact": str(artifact), "sha256": driver_sha,
                                                              "staged_driver": str(staged_driver), "profile": "release"}, indent=2) + "\n")
        # The Rust driver insists on an existing, initially empty mode-0700
        # directory. Build/run logs and wrappers must never be placed inside it.
        physical = root / "physical"
        physical.mkdir(mode=0o700)
        selected_env = {"VZ_TOPOLOGY_UP_MACHINE_E2E": "1", "VZ_TEST_INSTALLED_BUILD_PROFILE": "release",
                        "VZ_TEST_INSTALLED_CLI": info["cli"], "VZ_TEST_INSTALLED_DAEMON": info["daemon"],
                        "VZ_TEST_HOST_DOCKER": info["docker_canonical"], "VZ_TEST_HOST_DOCKER_SHA256": info["docker_sha256"],
                        "VZ_TOPOLOGY_UP_MACHINE_EVIDENCE": str(physical)}
        (root / "driver-environment.json").write_text(json.dumps(selected_env, indent=2) + "\n")
        print(f"Running sole DEV physical test; raw logs: {root / 'driver-run.stdout'}", flush=True)
        record_command(root, "driver-run", [str(staged_driver), "--ignored", "--exact", TEST,
                                            "--nocapture", "--test-threads=1"], env | selected_env, repo)
        verify_inputs(info["input_sha256"])
        require(file_digest(staged_driver) == driver_sha, "staged driver changed during execution")
        validate_test_result((root / "driver-run.stdout").read_text(), json.loads((physical / "result.json").read_text()))
        outcome = "passed_dev_physical_lifecycle"
    except (Exception, KeyboardInterrupt) as exception:
        error = f"{type(exception).__name__}: {exception}"
    finally:
        result = {"schema_version": 1, "scope": SCOPE, "outcome": outcome, "error": error,
                  "docker_parity_certified": False, "aggregate_release_certified": False,
                  "test_case_retries": 0, "evidence": str(root),
                  "cleanup": "Rust driver owns exact Stop; uncertain resources and all evidence are retained"}
        (root / "runner-result.json").write_text(json.dumps(result, indent=2) + "\n")
        checksums(root)
    print(json.dumps(result), flush=True)
    return 0 if error is None else 1


def main(script, argv):
    try:
        args = arguments(argv)
        info = preflight(args, script)
        return run(info)
    except (Exception, KeyboardInterrupt) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), sys.argv[2:]))
PYTHON_RUNNER
