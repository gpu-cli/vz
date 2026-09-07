"""Isolated state, command receipts, inventories and daemon cleanup for the
`topology` lane (`developer_environment_e2e.py`).

Everything the installed CLI may touch is derived from the lane's
`--state-root`; `~/.vz` and the ambient environment are never consulted.
Receipts follow `schemas/vz-0.4-receipt.schema.json` so the aggregate
validator schema-checks them by kind. Inventories are plain text so they are
not mistaken for typed evidence.
"""
from __future__ import annotations

import ctypes
import hashlib
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import time

from vz04_common import GateError, digest_file, document, now_ns, require, sha256_bytes, write_exclusive
from vz04_host import DOCKER_CONFIG_DIRNAME

LANE = "topology"
STATE_SUBDIR = "topology"
STREAM_LIMIT = 4 * 1024 * 1024
LABEL_PATTERN = re.compile(r"[^a-z0-9-]+")
DAEMON_SHUTDOWN_MARKER = b"runtime daemon shutting down"
DAEMON_STOP_DEADLINE_SECONDS = 30
# macOS sockaddr_un.sun_path is 104 bytes including the terminator.
SOCKET_PATH_LIMIT = 103


class UncertainEffects(Exception):
    """A CLI observer did not terminate within its deadline; effects unknown."""


class CleanupError(Exception):
    """Owned runtime state could not be positively removed."""


class LaneState:
    """Paths under `<state-root>/topology` plus the isolated CLI environment."""

    def __init__(self, state_root: Path, release_bin: Path):
        self.state_root = Path(state_root)
        self.root = self.state_root / STATE_SUBDIR
        self.runtime = self.root / "r"
        self.socket = self.runtime / "d.sock"
        self.database = self.root / "state.db"
        self.docker_config = self.state_root / DOCKER_CONFIG_DIRNAME
        self.tmp = self.root / "tmp"
        self.absent_home = self.root / "absent-home"
        self.absent_daemon = self.root / "absent-daemon"
        self.cli = release_bin / "vz"
        self.daemon = release_bin / "vz-runtimed"

    def create(self) -> None:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.tmp.mkdir(mode=0o700)

    def socket_path_bindable(self) -> bool:
        return len(str(self.socket).encode()) <= SOCKET_PATH_LIMIT

    def env(self, **overrides) -> dict:
        env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C", "NO_COLOR": "1", "TMPDIR": str(self.tmp),
               "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
               "HOME": str(self.absent_home),
               "VZ_RUNTIME_STATE_DB": str(self.database), "VZ_RUNTIME_DATA_DIR": str(self.runtime),
               "VZ_RUNTIME_DAEMON_SOCKET": str(self.socket), "VZ_DOCKER_CONFIG": str(self.docker_config)}
        for key, value in overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = str(value)
        return env


def label_for(text: str) -> str:
    label = LABEL_PATTERN.sub("-", text.lower()).strip("-")
    if not label or not re.match(r"[a-z0-9]", label[0]):
        label = "x" + label
    return label[:60].rstrip("-") or "x"


class Receipt:
    def __init__(self, index: int, label: str, argv: list, exit_code, stdout: bytes, stderr: bytes, elapsed_ns: int,
                 pid, timed_out: bool):
        self.index = index
        self.label = label
        self.argv = argv
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.elapsed_ns = elapsed_ns
        self.pid = pid
        self.timed_out = timed_out

    @property
    def name(self) -> str:
        return f"{self.index:03}-{self.label}"


class Recorder:
    """Per-command intent/result receipts (kind `vz-0.4-receipt`)."""

    def __init__(self, evidence_dir: Path, run_id: str):
        self.evidence_dir = Path(evidence_dir)
        self.run_id = run_id
        self.receipts_dir = self.evidence_dir / "receipts"
        self.receipts_dir.mkdir(mode=0o700, exist_ok=False)
        self.receipts = []
        self.process_starts = []
        self._first_start = set()
        self.uncertain = []

    def run(self, label: str, argv: list, *, cwd: Path, env: dict, scenario_id: str, timeout: int = 5) -> Receipt:
        index = len(self.receipts) + 1
        label = label_for(label)
        name = f"{index:03}-{label}"
        argv = [str(item) for item in argv]
        row = {"schema_version": 1, "kind": "vz-0.4-receipt", "run_id": self.run_id, "index": index, "label": label,
               "argv": argv, "executable": argv[0], "cwd": str(cwd), "timeout_seconds": int(timeout), "state": "intent",
               "started_unix_ns": now_ns(), "ended_unix_ns": None, "exit_code": None, "stdout_path": None, "stderr_path": None,
               "stdout_sha256": None, "stderr_sha256": None, "error": None, "effects_uncertain": True, "canary_withheld": False,
               "not_executed_reason": None}
        started = time.monotonic_ns()
        # The observer runs in its own session so a deadline kill never reaches
        # a daemon the CLI might have spawned (that is handled by stop_daemon).
        process = subprocess.Popen(argv, cwd=str(cwd), env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        if scenario_id not in self._first_start:
            self._first_start.add(scenario_id)
            self.process_starts.append({"scenario_id": scenario_id, "argv0": argv[0], "pid": process.pid})
        timed_out = False
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            # start_new_session gave this observer its own process group, so the
            # whole group can be reaped. Killing only the CLI would leave a child
            # (or an autospawned helper) holding the inherited pipes, and the
            # second communicate() would then block for that child's lifetime.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                process.kill()
            stdout, stderr = process.communicate()
        exit_code = None if timed_out else process.returncode
        stdout, stderr = stdout[:STREAM_LIMIT], stderr[:STREAM_LIMIT]
        row["ended_unix_ns"] = now_ns()
        row["exit_code"] = exit_code
        row["stdout_sha256"] = sha256_bytes(stdout)
        row["stderr_sha256"] = sha256_bytes(stderr)
        if stdout:
            write_exclusive(self.receipts_dir / (name + ".stdout"), stdout)
            row["stdout_path"] = f"receipts/{name}.stdout"
        if stderr:
            write_exclusive(self.receipts_dir / (name + ".stderr"), stderr)
            row["stderr_path"] = f"receipts/{name}.stderr"
        if timed_out:
            row.update(state="error", error=f"TimeoutExpired: observer killed after {timeout}s; effects uncertain", effects_uncertain=True)
        else:
            row.update(state="completed", effects_uncertain=exit_code < 0)
        document(self.receipts_dir / (name + ".json"), row)
        receipt = Receipt(index, label, argv, exit_code, stdout, stderr, time.monotonic_ns() - started, process.pid, timed_out)
        self.receipts.append(receipt)
        if row["effects_uncertain"]:
            self.uncertain.append(receipt)
        return receipt

    def receipt_paths(self, receipt: Receipt) -> list:
        paths = [f"receipts/{receipt.name}.json"]
        if receipt.stdout:
            paths.append(f"receipts/{receipt.name}.stdout")
        if receipt.stderr:
            paths.append(f"receipts/{receipt.name}.stderr")
        return paths


def inventory(root: Path) -> list:
    """Sorted rows `[relative, type, mode, size, sha256]` under `root` (lstat,
    never following symlinks; sockets/fifos/devices are listed by type). An
    absent root yields an empty inventory."""
    root = Path(root)
    rows = []
    if not os.path.lexists(root):
        return rows

    def walk(directory: Path, prefix: str):
        with os.scandir(directory) as scan:
            names = sorted(entry.name for entry in scan)
        for name in names:
            child = directory / name
            relative = f"{prefix}{name}"
            metadata = os.lstat(child)
            mode = stat.S_IMODE(metadata.st_mode)
            if stat.S_ISDIR(metadata.st_mode):
                rows.append([relative, "dir", mode, 0, None])
                walk(child, relative + "/")
            elif stat.S_ISREG(metadata.st_mode):
                hasher = hashlib.sha256()
                with open(child, "rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        hasher.update(block)
                rows.append([relative, "file", mode, metadata.st_size, hasher.hexdigest()])
            elif stat.S_ISLNK(metadata.st_mode):
                rows.append([relative, "symlink", mode, 0, sha256_bytes(os.readlink(child).encode("utf-8", "surrogateescape"))])
            elif stat.S_ISSOCK(metadata.st_mode):
                rows.append([relative, "socket", mode, 0, None])
            else:
                rows.append([relative, "special", mode, 0, None])

    walk(root, "")
    return rows


def inventory_text(root: Path, rows: list) -> bytes:
    lines = [f"# inventory of {root}", f"# entries={len(rows)}"]
    for relative, kind, mode, size, digest in rows:
        lines.append(f"{kind}\t{mode:04o}\t{size}\t{digest or '-'}\t{relative}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def inventory_digest(rows: list) -> str:
    return sha256_bytes(inventory_text(Path("/"), rows).split(b"\n", 2)[2])


def write_inventory(evidence_dir: Path, name: str, root: Path) -> tuple:
    """Write `inventories/<name>.txt` and return (rows, relative evidence path)."""
    directory = evidence_dir / "inventories"
    directory.mkdir(mode=0o700, exist_ok=True)
    rows = inventory(root)
    write_exclusive(directory / f"{name}.txt", inventory_text(root, rows))
    return rows, f"inventories/{name}.txt"


def inventory_diff(before: list, after: list) -> list:
    """Human-readable differences (empty when identical)."""
    before_map = {row[0]: row for row in before}
    after_map = {row[0]: row for row in after}
    out = []
    for relative in sorted(set(after_map) - set(before_map)):
        out.append(f"appeared: {relative} ({after_map[relative][1]})")
    for relative in sorted(set(before_map) - set(after_map)):
        out.append(f"vanished: {relative} ({before_map[relative][1]})")
    for relative in sorted(set(before_map) & set(after_map)):
        if before_map[relative] != after_map[relative]:
            out.append(f"changed: {relative}")
    return out


def _proc_pidpath(pid: int):
    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError:
        return None
    function = library.proc_pidpath
    function.argtypes, function.restype = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32], ctypes.c_int
    buffer = ctypes.create_string_buffer(4096)
    if function(pid, buffer, len(buffer)) <= 0:
        return None
    return buffer.value.decode("utf-8", "replace")


def ps_rows() -> list:
    # -ww: never truncate; lane state roots live under long temporary paths.
    completed = subprocess.run(["/bin/ps", "-axww", "-o", "pid=,command="], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, timeout=60, check=False)
    rows = []
    for line in completed.stdout.decode("utf-8", "replace").splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and parts[0].isdigit():
            rows.append((int(parts[0]), parts[1]))
    return rows


def processes_referencing(state: LaneState, exclude_pids=()) -> list:
    """Live processes whose command line names this lane's state root."""
    needle = str(state.root)
    return [(pid, command) for pid, command in ps_rows() if needle in command and pid not in exclude_pids and pid != os.getpid()]


def daemon_fingerprint(state: LaneState, pidfile: Path, socket_path: Path) -> dict:
    """Positively identify an autospawned installed daemon from its PID file
    (mirrors `installed_developer_startup.Harness.daemon_fingerprint`)."""
    require(not pidfile.is_symlink() and pidfile.is_file() and pidfile.stat().st_size <= 16, f"bounded owned daemon PID file required: {pidfile}")
    text = pidfile.read_text().strip()
    require(re.fullmatch(r"[0-9]+", text) is not None, "invalid daemon PID")
    pid = int(text)
    require(pid > 1, "unsafe daemon PID")
    executable = _proc_pidpath(pid)
    require(executable == str(state.daemon), f"PID {pid} is not the exact installed daemon ({executable!r})")
    matches = [command for candidate, command in ps_rows() if candidate == pid]
    require(len(matches) == 1 and str(socket_path) in matches[0], "daemon process does not own the isolated socket")
    return {"pid": pid, "process": matches[0], "executable_sha256": digest_file(state.daemon), "socket": str(socket_path)}


def daemon_artifacts(state: LaneState) -> list:
    """(pidfile, socket) pairs for every `*.pid` file under the lane root. An
    installed daemon writes `<socket stem>.pid` beside its socket."""
    pairs = []
    for relative, kind, _mode, _size, _digest in inventory(state.root):
        path = state.root / relative
        if kind == "file" and path.suffix == ".pid":
            pairs.append((path, path.with_suffix(".sock")))
    return pairs


def stray_sockets(state: LaneState) -> list:
    """Sockets under the lane root without a daemon PID file beside them."""
    return [state.root / relative for relative, kind, _m, _s, _d in inventory(state.root)
            if kind == "socket" and not (state.root / relative).with_suffix(".pid").exists()]


def stop_daemons(state: LaneState) -> list:
    """SIGTERM every positively identified daemon under the lane root and wait
    for socket/pid removal plus the graceful shutdown log line. Returns the
    identities stopped (empty when none existed). Raises CleanupError when any
    daemon artifact cannot be attributed and stopped positively."""
    stopped = []
    for pidfile, socket_path in daemon_artifacts(state):
        try:
            identity = daemon_fingerprint(state, pidfile, socket_path)
        except GateError as error:
            raise CleanupError(f"daemon artifacts present but no positively identified daemon: {error}") from error
        os.kill(identity["pid"], signal.SIGTERM)
        deadline = time.monotonic() + DAEMON_STOP_DEADLINE_SECONDS
        while time.monotonic() < deadline:
            try:
                os.kill(identity["pid"], 0)
                alive = True
            except ProcessLookupError:
                alive = False
            if not alive and not os.path.lexists(socket_path) and not os.path.lexists(pidfile):
                break
            time.sleep(0.05)
        if os.path.lexists(socket_path) or os.path.lexists(pidfile):
            raise CleanupError(f"daemon pid {identity['pid']} did not remove its socket/pid within {DAEMON_STOP_DEADLINE_SECONDS}s; no forced kill")
        try:
            os.kill(identity["pid"], 0)
            raise CleanupError(f"daemon pid {identity['pid']} still exists after SIGTERM; no forced kill")
        except ProcessLookupError:
            pass
        log_path = socket_path.with_suffix(".log")
        log = b""
        if log_path.is_file() and not log_path.is_symlink():
            with open(log_path, "rb") as stream:
                log = stream.read(32 * 1024 * 1024)
        if DAEMON_SHUTDOWN_MARKER not in log:
            raise CleanupError(f"positive graceful daemon shutdown log line not observed in {log_path}")
        identity["graceful_shutdown_observed"] = True
        stopped.append(identity)
    return stopped
