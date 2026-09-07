"""UNIT-TEST-ONLY fixtures for the topology lane: a POSIX-sh stand-in for the
installed `vz` CLI, a copied `/bin/sh` standing in for `vz-runtimed`, and a
fake release directory whose manifest/checksums bind those files. Behaviour
is switched through a mode file the fake reads at startup (the lane never
passes ambient environment to the CLI, so an env switch would be invisible).

Modes: "" (contract-conformant), mutate (bare vz writes ./discovered),
drift (help gains a line), alias (`create` executes), provisions (`up` writes
state and exits 0 without a definition), hang (`ls` sleeps past the
deadline), autospawn (`status --all` spawns a fake daemon that shuts down
gracefully on SIGTERM), bogus_pid (`status --all` leaves an unattributable
PID file).
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_vz04_fixtures as fixtures  # noqa: E402
import vz04_candidate as candidate  # noqa: E402
from developer_environment_checks import FLAG_MIGRATION, ROOT_MIGRATION, TYPED_API_MIGRATION  # noqa: E402
from vz04_common import REPO_ROOT, digest_file, read_regular, sha256_bytes  # noqa: E402

FAKE_VZ = r'''#!/bin/sh
# fake vz (unit tests only)
MODE_FILE=__MODE_FILE__
SNAPSHOT_FILE=__SNAPSHOT_FILE__
mode=""
[ -f "$MODE_FILE" ] && mode=$(cat "$MODE_FILE")
reject() {
  if [ "$2" = root ]; then mig='__ROOT_MIGRATION__'; else mig='__FLAG_MIGRATION__'; fi
  printf '{"error":{"code":"legacy_command_removed","command":"%s","message":"`vz %s` was removed from the 0.4 public CLI","migration":"%s","typed_api_migration":"__TYPED__"}}\n' "$1" "$1" "$mig" >&2
  exit 2
}
verb=""; sawhelp=0; version=0; all=0
for arg in "$@"; do
  case "$arg" in
    create|ls|rm|inspect|attach|close-shell|init|run|logs|stack|image|diff|checkpoint|vm|self-sign|debug)
      if [ "$mode" = alias ] && [ "$arg" = create ]; then echo created; exit 0; fi
      if [ "$mode" = hang ] && [ "$arg" = ls ]; then sleep 30; fi
      reject "$arg" root ;;
    --continue|--resume|--name|--ephemeral|--cpus|--memory|--base-image|--main-container|--control-plane) reject "$arg" flag ;;
    --continue=*|--resume=*|--name=*|--ephemeral=*|--cpus=*|--memory=*|--base-image=*|--main-container=*|--control-plane=*) reject "${arg%%=*}" flag ;;
    up|exec|status|stop|delete) [ -z "$verb" ] && verb="$arg" ;;
    --all) all=1 ;;
    --help|help) sawhelp=1 ;;
    --version) version=1 ;;
    --json|--quiet|--no-stdin|--) ;;
    --*) ;;
    -*)
      rest="${arg#-}"
      while [ -n "$rest" ]; do
        ch=$(printf %.1s "$rest")
        case "$ch" in c) reject -c flag ;; r) reject -r flag ;; h) sawhelp=1 ;; V) version=1 ;; esac
        rest="${rest#?}"
      done ;;
    *) ;;
  esac
done
if [ -n "$verb" ]; then
  if [ "$sawhelp" = 1 ]; then printf 'Usage: vz %s [OPTIONS]\n\nOptions:\n  -h, --help  Print help\n' "$verb"; exit 0; fi
  if [ "$verb" = up ] && [ "$mode" = provisions ]; then
    mkdir -p "$VZ_RUNTIME_DATA_DIR"; : > "$VZ_RUNTIME_STATE_DB"; echo '{"progress":{"completion":{}}}'; exit 0
  fi
  if [ ! -f vz.json ]; then
    if [ "$verb" = status ]; then
      printf '{"error":{"code":"definition_not_found","message":"no vz.json project definition found at or above %s"}}\n' "$PWD" >&2
    else
      printf '{"error":{"code":"definition_not_found","details":{},"idempotency_key":"k-%s","message":"no vz.json project definition found at or above %s","request_id":"req-%s"},"schema_version":1}\n' "$verb" "$PWD" "$verb" >&2
    fi
    exit 2
  fi
  if [ "$verb" = status ]; then
    sock="$VZ_RUNTIME_DAEMON_SOCKET"; pidf="${sock%.sock}.pid"; logf="${sock%.sock}.log"
    if [ "$mode" = autospawn ]; then
      mkdir -p "$(dirname "$sock")"
      daemon="$(dirname "$0")/vz-runtimed"
      "$daemon" "$sock" "$pidf" "$logf" </dev/null >/dev/null 2>&1 &
      echo $! > "$pidf"
      while [ ! -S "$sock" ]; do sleep 0.05; done
    fi
    if [ "$mode" = bogus_pid ]; then mkdir -p "$(dirname "$sock")"; echo 99999999 > "$pidf"; fi
    printf '{"error":{"code":"daemon_unavailable","message":"no compatible runtime daemon is listening on the configured socket"}}\n' >&2
    exit 2
  fi
  printf '{"error":{"code":"daemon_unavailable","message":"no compatible runtime daemon is listening on the configured socket"}}\n' >&2
  exit 2
fi
if [ "$version" = 1 ]; then echo "vz 0.4.0-fake"; exit 0; fi
[ "$mode" = mutate ] && : > ./discovered
cat "$SNAPSHOT_FILE"
[ "$mode" = drift ] && echo "extra line"
exit 0
'''

CATALOG = {"schema_version": 1, "linux": [
    {"image": "vz-linux-appliance", "version": "0.4.0-fake", "profile": "developer", "bundle_dir": "/nonexistent/developer",
     "digest": "sha256:" + "1" * 64, "channels": []},
    {"image": "vz-linux-appliance", "version": "0.4.0-fake", "profile": "hardened", "bundle_dir": "/nonexistent/container",
     "digest": "sha256:" + "2" * 64, "channels": []}], "macos": []}


def fake_vz_script(mode_file: Path, snapshot_file: Path) -> bytes:
    text = (FAKE_VZ.replace("__MODE_FILE__", json.dumps(str(mode_file))).replace("__SNAPSHOT_FILE__", json.dumps(str(snapshot_file)))
            .replace("__ROOT_MIGRATION__", ROOT_MIGRATION).replace("__FLAG_MIGRATION__", FLAG_MIGRATION)
            .replace("__TYPED__", TYPED_API_MIGRATION))
    return text.encode()


# A spawned daemon must report the release `bin/vz-runtimed` path as its own
# executable, exactly as `daemon_fingerprint` requires of the real daemon. A
# copied Apple platform binary (/bin/sh) is refused by macOS even after ad-hoc
# signing, and a shebang script reports its interpreter, so the fixture builds
# one tiny Mach-O that binds the socket and shuts down gracefully on SIGTERM.
FAKE_DAEMON_SOURCE = r"""
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static char socket_path[1024], pid_path[1024], log_path[1024];

static void on_term(int signal_number) {
    (void)signal_number;
    unlink(socket_path);
    unlink(pid_path);
    FILE *log = fopen(log_path, "a");
    if (log != NULL) {
        fputs("runtime daemon shutting down\n", log);
        fclose(log);
    }
    _exit(0);
}

int main(int argc, char **argv) {
    struct sockaddr_un address;
    int descriptor;
    if (argc != 4) {
        return 2;
    }
    snprintf(socket_path, sizeof(socket_path), "%s", argv[1]);
    snprintf(pid_path, sizeof(pid_path), "%s", argv[2]);
    snprintf(log_path, sizeof(log_path), "%s", argv[3]);
    memset(&address, 0, sizeof(address));
    address.sun_family = AF_UNIX;
    if (strlen(socket_path) >= sizeof(address.sun_path)) {
        return 3;
    }
    snprintf(address.sun_path, sizeof(address.sun_path), "%s", socket_path);
    descriptor = socket(AF_UNIX, SOCK_STREAM, 0);
    if (descriptor < 0 || bind(descriptor, (struct sockaddr *)&address, sizeof(address)) != 0) {
        return 4;
    }
    if (listen(descriptor, 1) != 0) {
        return 5;
    }
    signal(SIGTERM, on_term);
    for (;;) {
        pause();
    }
}
"""


def build_fake_daemon(destination: Path) -> None:
    """Compile the daemon stand-in, or skip the caller when no compiler exists."""
    compiler = shutil.which("cc") or shutil.which("clang")
    if compiler is None:
        raise unittest.SkipTest("no C compiler for the fake vz-runtimed stand-in")
    source = destination.parent / "fake-vz-runtimed.c"
    source.write_text(FAKE_DAEMON_SOURCE)
    completed = subprocess.run([compiler, "-O0", "-o", str(destination), str(source)], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, timeout=120, check=False)
    source.unlink()
    if completed.returncode != 0:
        raise unittest.SkipTest("cannot build the fake vz-runtimed stand-in: " +
                                completed.stdout.decode("utf-8", "replace")[-200:])


def build_fake_release(root: Path, *, mode_file: Path, snapshot_file: Path = None) -> Path:
    """A read-only fake release dir whose bin/vz is the sh stand-in and whose
    bin/vz-runtimed is a compiled stand-in (so a spawned fake daemon has the
    release path as its executable)."""
    snapshot_file = snapshot_file or (REPO_ROOT / "tests/fixtures/vz-0.4/cli/help-snapshot.txt")
    fixtures.build_fake_release_dir(root)
    fixtures.make_writable(root)
    (root / "bin/vz").write_bytes(fake_vz_script(mode_file, snapshot_file))
    (root / "bin/vz").chmod(0o755)
    build_fake_daemon(root / "bin/vz-runtimed")
    (root / "bin/vz-runtimed").chmod(0o755)
    catalog = json.dumps(CATALOG, indent=2, sort_keys=True).encode() + b"\n"
    (root / "machine-target-catalog.json").write_bytes(catalog)
    manifest = json.loads(read_regular(root / "release-manifest.json"))
    for relative in ("bin/vz", "bin/vz-runtimed"):
        manifest["components"][relative]["signed_sha256"] = digest_file(root / relative)
    components = manifest["components"]
    manifest["normalized_content_sha256"] = candidate.line_digest(sorted([p, c["unsigned_sha256"]] for p, c in components.items()))
    manifest["signed_content_sha256"] = candidate.line_digest(sorted([p, c["signed_sha256"]] for p, c in components.items()))
    manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
    (root / "release-manifest.json").write_bytes(manifest_bytes)
    (root / "release-manifest.sha256").write_bytes(f"{sha256_bytes(manifest_bytes)}  release-manifest.json\n".encode())
    (root / "checksums.sha256").unlink()
    rows = [f"{digest_file(path)}  {path.relative_to(root).as_posix()}\n" for path in sorted(p for p in root.rglob("*") if p.is_file())]
    (root / "checksums.sha256").write_bytes("".join(rows).encode())
    for path in root.rglob("*"):
        path.chmod(stat.S_IMODE(path.lstat().st_mode) & ~0o222)
    return root
