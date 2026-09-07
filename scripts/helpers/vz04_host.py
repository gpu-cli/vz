"""Host facts, host resource inventories and the final-cleanup leak diff.

Everything here observes the host; nothing mutates it. Inventories are
captured twice (`before` after the prerequisites, `after` after the
final-cleanup lanes) and diffed:

* listeners: `/usr/sbin/lsof -nP -iTCP -sTCP:LISTEN -iUDP -F pcPnT` cross-checked with
  `/usr/sbin/netstat -an -f inet -f inet6`, parsed into rows and classified
  against the contract's `listener_checks` (loopback / wildcard / non-loopback);
* processes: `ps -axo pid=,ppid=,command=` scoped to the run (command line
  mentions the run-id, the state root or the release directory), excluding the
  gate's own ancestry (its argv carries the run-id);
* Unix sockets under the state root;
* Docker contexts in the isolated `VZ_DOCKER_CONFIG` (`<state root>/docker`).

`diff(before, after, listener_checks)` returns the leak-diff document. Its
`survivors` list must be empty for PASS; the validator recomputes it from the
two inventories and refuses a diff that does not reproduce.
"""
from __future__ import annotations

import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import sys

from vz04_common import digest_file, document, sha256_bytes, utc_iso

LSOF = "/usr/sbin/lsof"
NETSTAT = "/usr/sbin/netstat"
PS = "/bin/ps"
SYSCTL = "/usr/sbin/sysctl"
SW_VERS = "/usr/bin/sw_vers"
SYSTEM_PYTHON = "/usr/bin/python3"
DOCKER_CONFIG_DIRNAME = "docker"
MAX_OUTPUT = 64 * 1024 * 1024
MAX_SOCKET_WALK = 50000
MAX_ROWS = 20000
GATE_REQUIREMENTS = Path(__file__).resolve().parent / "gate-requirements.txt"
HOST_BINARIES = ("vz", "vz-runtimed", "vz-macos-setup", "vz-guest-agent", "vz-agent-loader")


def run_capture(argv, timeout=60, env=None) -> dict:
    """Bounded capture of one observation command. Never raises."""
    row = {"argv": list(argv), "exit_code": None, "error": None, "stdout_sha256": None, "truncated": False}
    try:
        completed = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   timeout=timeout, check=False, env=env)
    except subprocess.TimeoutExpired:
        row["error"] = f"timeout after {timeout}s"
        return row, b""
    except OSError as error:
        row["error"] = f"{type(error).__name__}: {error}"
        return row, b""
    data = completed.stdout
    if len(data) > MAX_OUTPUT:
        data, row["truncated"] = data[:MAX_OUTPUT], True
    row["exit_code"] = completed.returncode
    row["stdout_sha256"] = sha256_bytes(data)
    if completed.returncode != 0:
        row["error"] = completed.stderr.decode("utf-8", "replace").strip()[:300] or f"exit {completed.returncode}"
    return row, data


def first_line(argv, timeout=30, env=None):
    row, data = run_capture(argv, timeout, env)
    text = data.decode("utf-8", "replace").strip()
    return text.splitlines()[0][:200] if row["exit_code"] == 0 and text else None


def sysctl(name):
    return first_line([SYSCTL, "-n", name])


def boottime_unix():
    text = sysctl("kern.boottime") or ""
    match = re.search(r"sec = (\d+)", text)
    return int(match.group(1)) if match else None


def _int(text):
    return int(text) if text is not None and text.isdigit() else None


# --------------------------------------------------------------------------- facts

def host_facts(state_root: Path) -> dict:
    try:
        usage = os.statvfs(state_root)
        free_disk = usage.f_bavail * usage.f_frsize
    except OSError:
        free_disk = None
    return {"os": platform.system(), "os_version": first_line([SW_VERS, "-productVersion"]) or platform.release(),
            "os_build": first_line([SW_VERS, "-buildVersion"]), "hardware_model": sysctl("hw.model"),
            "architecture": platform.machine(), "memory_bytes": _int(sysctl("hw.memsize")), "cpu_count": _int(sysctl("hw.ncpu")),
            "hostname_sha256": sha256_bytes(platform.node().encode("utf-8")),
            "boot_session_uuid": sysctl("kern.bootsessionuuid"), "boottime_unix": boottime_unix(),
            "state_root_free_disk_bytes": free_disk}


def _client_version(path: str, kind: str):
    if kind == "docker":
        return first_line([path, "version", "--format", "{{.Client.Version}}"])
    return _plugin_metadata_version(path)


def _plugin_metadata_version(path: str):
    row, data = run_capture([path, "docker-cli-plugin-metadata"], 30)
    if row["exit_code"] != 0:
        return None
    try:
        value = json.loads(data.decode("utf-8", "replace"))
    except json.JSONDecodeError:
        return None
    version = value.get("Version") if isinstance(value, dict) else None
    return str(version)[:200] if version else None


def client_facts(clients: dict) -> dict:
    """{docker|compose_plugin|buildx_plugin: {path, resolved, sha256, version} | None}; `path` is
    the invocation path (argv[0]), `resolved` its realpath, `sha256` the resolved file."""
    facts = {}
    for kind in ("docker", "compose_plugin", "buildx_plugin"):
        path = clients.get(kind)
        if path is None:
            facts[kind] = None
            continue
        resolved = Path(path).resolve(strict=True)
        facts[kind] = {"path": path, "resolved": str(resolved), "sha256": digest_file(resolved), "version": _client_version(path, kind)}
    return facts


def toolchain_facts() -> dict:
    return {"python": platform.python_version(), "python_executable": sys.executable,
            "system_python": first_line([SYSTEM_PYTHON, "--version"]) if os.access(SYSTEM_PYTHON, os.X_OK) else None,
            "uv": first_line(["uv", "--version"]), "cargo": first_line(["cargo", "--version"]),
            "cargo_nextest": first_line(["cargo", "nextest", "--version"]), "rustc": first_line(["rustc", "--version"]),
            "codesign": "/usr/bin/codesign" if os.access("/usr/bin/codesign", os.X_OK) else None,
            "gate_requirements_sha256": digest_file(GATE_REQUIREMENTS)}


# ----------------------------------------------------------------------- listeners

def classify_address(address: str) -> str:
    """loopback | wildcard | non_loopback for a bare host part ('*' means any)."""
    if address in ("*", "0.0.0.0", "::", "[::]"):
        return "wildcard"
    bare = address.strip("[]")
    try:
        parsed = ipaddress.ip_address(bare.split("%", 1)[0])
    except ValueError:
        return "loopback" if bare in ("localhost",) else "non_loopback"
    if parsed.is_unspecified:
        return "wildcard"
    return "loopback" if parsed.is_loopback else "non_loopback"


def _split_host_port(text: str, separator: str):
    """Split lsof ('host:port') or netstat ('host.port') endpoint text."""
    if separator not in text:
        return None
    host, port = text.rsplit(separator, 1)
    if not port.isdigit():
        return None
    return host, int(port)


def parse_lsof(data: bytes) -> list:
    """Parse `lsof -F pcPnT` field output: p<pid> c<command> then per file
    P<proto> n<name> T<ST=...>. Only TCP LISTEN and bound UDP rows are kept."""
    rows = []
    pid = command = None
    current = None

    def flush():
        if current is None or current.get("port") is None:
            return
        if current["protocol"] == "tcp" and current.get("state") != "LISTEN":
            return
        rows.append({"protocol": current["protocol"], "address": current["address"], "port": current["port"], "pid": pid,
                     "command": command, "scope": classify_address(current["address"]), "source": "lsof"})

    for line in data.decode("utf-8", "replace").splitlines():
        if not line:
            continue
        tag, value = line[0], line[1:]
        if tag == "p":
            flush()
            current = None
            pid = int(value) if value.isdigit() else None
        elif tag == "c":
            command = value[:200] or None
        elif tag == "f":
            flush()
            current = None
        elif tag == "P":
            current = {"protocol": value.lower(), "address": None, "port": None, "state": None}
        elif tag == "n" and current is not None:
            endpoint = _split_host_port(value.split("->", 1)[0], ":")
            if endpoint is not None:
                current["address"], current["port"] = endpoint
        elif tag == "T" and current is not None and value.startswith("ST="):
            current["state"] = value[3:]
    flush()
    return [row for row in rows if row["protocol"] in ("tcp", "udp")]


def parse_netstat(data: bytes) -> list:
    rows = []
    for line in data.decode("utf-8", "replace").splitlines():
        parts = line.split()
        if len(parts) < 4 or not parts[0].startswith(("tcp", "udp")):
            continue
        protocol = "tcp" if parts[0].startswith("tcp") else "udp"
        if protocol == "tcp" and (len(parts) < 6 or parts[5] != "LISTEN"):
            continue
        if protocol == "udp" and parts[4] != "*.*":
            continue
        endpoint = _split_host_port(parts[3], ".")
        if endpoint is None:
            continue
        host, port = endpoint
        rows.append({"protocol": protocol, "address": host, "port": port, "pid": None, "command": None,
                     "scope": classify_address(host), "source": "netstat"})
    return rows


def listener_key(row: dict) -> tuple:
    return (row["protocol"], row["address"], row["port"])


def merge_listeners(lsof_rows: list, netstat_rows: list) -> list:
    """lsof rows carry pid/command; netstat rows fill in listeners lsof could
    not see (other users' processes). `source` records where each came from."""
    merged = {}
    for row in lsof_rows:
        merged.setdefault((*listener_key(row), row["pid"]), dict(row))
    seen = {listener_key(row) for row in lsof_rows}
    for row in netstat_rows:
        key = listener_key(row)
        if key in seen:
            for existing in merged.values():
                if listener_key(existing) == key:
                    existing["source"] = "both"
            continue
        merged.setdefault((*key, None), dict(row))
    return sorted(merged.values(), key=lambda r: (r["protocol"], r["port"], r["address"], r["pid"] or 0))[:MAX_ROWS]


# ---------------------------------------------------------------------- processes

def own_ancestry(table: dict) -> set:
    """pids of this process and every ancestor (their argv carries the run-id)."""
    pids, pid = set(), os.getpid()
    while pid and pid not in pids and pid in table:
        pids.add(pid)
        pid = table[pid][0]
    pids.add(os.getpid())
    return pids


def parse_ps(data: bytes) -> dict:
    """{pid: (ppid, command)}."""
    table = {}
    for line in data.decode("utf-8", "replace").splitlines():
        parts = line.split(None, 2)
        if len(parts) < 3 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        table[int(parts[0])] = (int(parts[1]), parts[2].strip()[:500])
    return table


def scoped_processes(table: dict, needles: list, excluded: set) -> list:
    rows = []
    for pid in sorted(table):
        if pid in excluded:
            continue
        ppid, command = table[pid]
        matches = sorted(label for label, needle in needles if needle and needle in command)
        if matches:
            rows.append({"pid": pid, "ppid": ppid, "command": command, "matched": matches})
    return rows[:MAX_ROWS]


def unix_sockets(state_root: Path) -> list:
    found, visited = [], 0
    if not state_root.is_dir():
        return found
    for directory, dirnames, filenames in os.walk(state_root):
        dirnames[:] = [d for d in dirnames if not os.path.islink(os.path.join(directory, d))]
        for name in filenames + dirnames:
            visited += 1
            if visited > MAX_SOCKET_WALK:
                return sorted(found)
            path = os.path.join(directory, name)
            try:
                if stat.S_ISSOCK(os.lstat(path).st_mode):
                    found.append(path)
            except OSError:
                continue
    return sorted(found)


def docker_contexts(docker_client, docker_config: Path) -> tuple:
    if docker_client is None:
        return {"argv": [], "exit_code": None, "error": "no docker client recorded", "stdout_sha256": None, "truncated": False}, []
    env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": os.environ.get("HOME", "/"), "DOCKER_CONFIG": str(docker_config)}
    row, data = run_capture([docker_client, "--config", str(docker_config), "context", "ls", "--format", "{{json .}}"], 60, env)
    contexts = []
    for line in data.decode("utf-8", "replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("Name"):
            contexts.append({"name": str(value["Name"])[:200], "endpoint": str(value.get("DockerEndpoint") or "")[:300] or None})
    return row, sorted(contexts, key=lambda c: c["name"])


# ------------------------------------------------------------------------ capture

class HostScope:
    """What counts as run-owned on this host."""

    def __init__(self, *, run_id: str, state_root: Path, release_dir: Path, clients: dict):
        self.run_id = run_id
        self.state_root = Path(state_root)
        self.release_dir = Path(release_dir)
        self.clients = clients
        self.docker_config = self.state_root / DOCKER_CONFIG_DIRNAME

    def needles(self) -> list:
        return [("run_id", self.run_id), ("state_root", str(self.state_root)), ("release_bin", str(self.release_dir / "bin"))]


def capture(scope: HostScope, moment: str) -> dict:
    sources, errors = {}, []
    sources["lsof"], lsof_out = run_capture([LSOF, "-nP", "-iTCP", "-sTCP:LISTEN", "-iUDP", "-F", "pcPnT"], 60)
    sources["netstat"], netstat_out = run_capture([NETSTAT, "-an", "-f", "inet", "-f", "inet6"], 60)
    sources["ps"], ps_out = run_capture([PS, "-axo", "pid=,ppid=,command="], 60)
    sources["docker_context_ls"], contexts = docker_contexts(scope.clients.get("docker"), scope.docker_config)
    # lsof exits 1 when it merely finds nothing for one of the selectors; only a real error has empty output.
    for name in ("lsof", "netstat", "ps"):
        if sources[name]["exit_code"] is None or (sources[name]["exit_code"] != 0 and not {"lsof": lsof_out, "netstat": netstat_out, "ps": ps_out}[name]):
            errors.append({"source": name, "error": sources[name]["error"] or "no output"})
    if sources["docker_context_ls"]["exit_code"] not in (0, None) or (sources["docker_context_ls"]["exit_code"] is None and scope.clients.get("docker")):
        errors.append({"source": "docker_context_ls", "error": sources["docker_context_ls"]["error"] or "no output"})
    table = parse_ps(ps_out)
    excluded = own_ancestry(table)
    return {"schema_version": 1, "kind": "vz-0.4-host-inventory", "run_id": scope.run_id, "moment": moment,
            "captured_at_utc": utc_iso(), "capture_state": "captured" if not errors else "partial", "capture_errors": errors,
            "state_root": str(scope.state_root), "docker_config": str(scope.docker_config), "sources": sources,
            "listeners": merge_listeners(parse_lsof(lsof_out), parse_netstat(netstat_out)),
            "processes": scoped_processes(table, scope.needles(), excluded), "excluded_pids": sorted(excluded),
            "process_count": len(table), "sockets": unix_sockets(scope.state_root), "docker_contexts": contexts}


def write_inventory(root: Path, scope: HostScope, moment: str) -> str:
    directory = root / "host"
    directory.mkdir(exist_ok=True)
    path = directory / f"{moment}.json"
    document(path, capture(scope, moment))
    return str(path.relative_to(root))


# --------------------------------------------------------------------------- diff

def diff(before: dict, after: dict, listener_checks: dict) -> dict:
    """Leak diff between two inventories. `survivors` is the FAIL set."""
    before_keys = {listener_key(row) for row in before["listeners"]}
    scoped_pids = {row["pid"] for row in after["processes"]}
    new_listeners, survivors = [], []
    for row in after["listeners"]:
        if listener_key(row) in before_keys:
            continue
        owned = row["pid"] in scoped_pids or row["pid"] is None
        forbidden = row["scope"] == "wildcard" and listener_checks.get("wildcard_listeners_forbidden", True)
        row = dict(row, run_owned=owned)
        new_listeners.append(row)
        if owned or forbidden:
            survivors.append(f"listener {row['protocol']} {row['address']}:{row['port']} pid={row['pid']} command={row['command']} scope={row['scope']}")
    for row in after["processes"]:
        survivors.append(f"process pid={row['pid']} matched={','.join(row['matched'])} command={row['command'][:120]}")
    for path in after["sockets"]:
        survivors.append(f"socket {path}")
    before_contexts = {c["name"] for c in before["docker_contexts"]}
    for context in after["docker_contexts"]:
        if context["name"] != "default" and context["name"] not in before_contexts:
            survivors.append(f"docker_context {context['name']} endpoint={context['endpoint']}")
    unrelated = [row for row in new_listeners if not row["run_owned"] and row["scope"] != "wildcard"]
    return {"schema_version": 1, "kind": "vz-0.4-leak-diff", "run_id": after["run_id"], "performed": True, "reason": None,
            "survivors": sorted(survivors), "new_listeners": new_listeners,
            "unrelated_new_listener_count": len(unrelated), "listener_checks": dict(listener_checks),
            "before_capture_state": before["capture_state"], "after_capture_state": after["capture_state"]}


def write_diff(root: Path, before: dict, after: dict, listener_checks: dict) -> Path:
    directory = root / "phases" / "final-cleanup"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "leak-diff.json"
    document(path, diff(before, after, listener_checks))
    return path
