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
verb=""; sawhelp=0; version=0; all=0; endopts=0; command_tail=""
for arg in "$@"; do
  # Only `exec` takes a command payload after `--`, so a `-c` there is the
  # shell's flag rather than one of this CLI's removed ones. Everywhere else
  # `--` is just a separator and a removed root after it is still rejected.
  if [ "$endopts" = 1 ]; then command_tail=$arg; continue; fi
  if [ "$arg" = "--" ] && [ "$verb" = exec ]; then endopts=1; continue; fi
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
  # A real Up persists topology; `status` succeeds only afterwards, which is
  # what the bootstrap-creates-default check depends on.
  topology="$VZ_RUNTIME_DATA_DIR/topology.json"
  if [ "$verb" = up ] && [ -f vz.json ]; then
    pid=$(grep -o '"project_id"[^,]*' vz.json | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
    mkdir -p "$VZ_RUNTIME_DATA_DIR"; : > "$VZ_RUNTIME_STATE_DB"
    # Runtime identities are minted per Up, not derived from the definition:
    # recreating one pinned definition must hand out entirely new ones.
    inc=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')
    # Status must reflect the Machines the definition declares, not a fixed one.
    names=$(grep -o '"name": *"machine-[^"]*"' vz.json | sed 's/.*"\(machine-[^"]*\)"/\1/' | sort -u | tr '\n' ' ')
    [ -n "$names" ] || names="machine-0 "
    printf '%s %s %s' "$pid" "$inc" "$names" > "$topology"
    printf '{"schema_version":1,"progress":{"completion":{}}}\n'
    exit 0
  fi
  if [ "$verb" = exec ] && [ -f "$topology" ]; then
    # Model Machine-local mutable state: run the script with the sentinel path
    # rewritten into this isolated runtime dir, so a recreated Environment with
    # a fresh state directory genuinely has none of it.
    printf '%s' "$command_tail" \
      | sed -e "s#/run/vz-reproducibility-sentinel#$VZ_RUNTIME_DATA_DIR/sentinel#g" \
            -e "s#/bin/busybox#$(dirname "$0")/busybox-shim#g" \
            -e "s#/www#$VZ_RUNTIME_DATA_DIR/www#g" > "$VZ_RUNTIME_DATA_DIR/script.sh"
    /bin/sh "$VZ_RUNTIME_DATA_DIR/script.sh"
    exit $?
  fi
  if [ "$verb" = delete ] && [ -f "$topology" ]; then
    rm -f "$topology"; printf '{"schema_version":1,"deleted":["default"]}\n'; exit 0
  fi
  if [ "$verb" = status ] && [ -f "$topology" ]; then
    # The real success payload is a pretty-printed document, not one line, and
    # it names its own state source and per-Environment state. Every identity
    # below is derived from this project's own id, so two projects declaring
    # identical names still report distinct Environment, Machine, context and
    # endpoint identities -- which is what the no-collision check reads.
    pid=$(cut -d' ' -f1 < "$topology")
    sfx=$(cut -d' ' -f2 < "$topology")
    names=$(cut -d' ' -f3- < "$topology")
    dg="sha256:$(printf '%s' "$pid" | shasum -a 256 | cut -c1-64)"
    # The full declared success-payload field set, so an exact comparison of it
    # is exercised here and not only against the installed binaries.
    printf '{\n "schema_version": 1,\n "request_id": "req-%s",\n "topology_state_source": "persisted",\n' "$sfx"
    printf ' "definition_path": "%s/vz.json",\n "project_name": "vz04-topology-bootstrap",\n' "$PWD"
    printf ' "host": {"os": "macos", "arch": "aarch64"},\n' 
    printf ' "daemon": {"backend_name": "macos-vz", "version": "0.1.0"},\n'
    printf ' "desired_definition_digest": "%s",\n "persisted_definition_digest": "%s",\n' "$dg" "$dg"
    printf ' "definition_drift": false,\n "selection_source": "workspace",\n "project_id": "%s",\n' "$pid"
    printf ' "environments": [\n  {\n   "environment_id": "env_%s",\n   "name": "default",\n   "state": "ready",\n' "$sfx"
    printf '   "machines": ['
    sep=""
    for m in $names; do
      printf '%s{"name": "%s", "state": "ready", "docker_context": {' "$sep" "$m"
      printf '"owner": {"project_id": "%s", "environment_id": "env_%s", "machine_id": "mch_%s_%s"},' "$pid" "$sfx" "$sfx" "$m"
      printf '"name": "vzr1-ctx-%s-%s", "endpoint": "unix:///tmp/vz-%s-%s.sock",' "$sfx" "$m" "$sfx" "$m"
      printf '"engine_id": "eng-%s-%s"}, "machine_id": "mch_%s_%s",' "$sfx" "$m" "$sfx" "$m"
      printf '"incarnation_id": "inc_%s_%s", "incarnation_generation": 1}' "$sfx" "$m"
      sep=", "
    done
    printf ']\n'
    printf '  }\n ]\n}\n'
    exit 0
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


BUSYBOX_SHIM = r'''#!/bin/sh
# Stand-in for the guest BusyBox. Applets that only touch files delegate to the
# host; `httpd`, `wget` and `ip` model one Environment's private reachability:
# an address belongs to the project whose runtime directory serves it, so a
# probe from another project's directory cannot reach it. That is the property
# under test, modelled at the granularity this fake has (project == Environment).
state="$VZ_RUNTIME_DATA_DIR"
applet=$1
shift
case "$applet" in
  httpd)
    root=""
    while [ $# -gt 0 ]; do case "$1" in -h) root=$2; shift 2 ;; *) shift ;; esac; done
    printf '%s' "$root" > "$state/httpd-root"
    exit 0 ;;
  ip)
    # A deterministic private address per project runtime directory.
    n=$(printf '%s' "$state" | cksum | cut -d' ' -f1)
    # `ip -o -4 addr show` field layout, so the caller parses the shim exactly
    # the way it parses the real tool.
    printf '2: eth0    inet 10.%s.%s.2/24 brd 10.%s.%s.255 scope global eth0\n' \
      "$(( (n / 256) % 254 + 1 ))" "$(( n % 254 + 1 ))" "$(( (n / 256) % 254 + 1 ))" "$(( n % 254 + 1 ))"
    exit 0 ;;
  wget)
    url=""
    while [ $# -gt 0 ]; do case "$1" in http://*) url=$1 ;; esac; shift; done
    host=${url#http://}; host=${host%%:*}
    mine=$("$0" ip -o -4 addr show | awk '{print $4}' | cut -d/ -f1)
    if [ "$host" != "$mine" ] || [ ! -f "$state/httpd-root" ]; then exit 1; fi
    cat "$(cat "$state/httpd-root")/index.html"
    exit 0 ;;
  *) exec "$applet" "$@" ;;
esac
'''


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
    # The guest BusyBox stand-in every `vz exec` script addresses.
    (root / "bin/busybox-shim").write_text(BUSYBOX_SHIM)
    (root / "bin/busybox-shim").chmod(0o755)
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
