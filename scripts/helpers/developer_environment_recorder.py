"""Isolated state, command receipts, inventories and daemon cleanup for the
`topology` lane (`developer_environment_e2e.py`).

Everything the installed CLI may touch is derived from the lane's
`--state-root` -- except its AF_UNIX sockets, which live in a short root derived
from it (`socket_root_for`) because macOS cannot bind a 103+ byte path. Both
roots are owned, scanned and removed by the lane; `~/.vz` and the ambient
environment are never consulted.
Receipts follow `schemas/vz-0.4-receipt.schema.json` so the aggregate
validator schema-checks them by kind. Inventories are plain text so they are
not mistaken for typed evidence.
"""
from __future__ import annotations

import ctypes
import errno
import hashlib
import os
from pathlib import Path
import re
import socket
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
# `vzr1-ot-` + 32 hex + `.sock`: the longest Docker endpoint name a Developer
# Machine is given inside its `VZ_RUNTIME_DATA_DIR`.
ENDPOINT_NAME_BYTES = 45
# The longest isolate name any check may ask for (`bootstrap`, `envelope`,
# `net-a`); asserted so a new check cannot silently spend the socket budget.
ISOLATE_NAME_BYTES = 16
# Where this lane's AF_UNIX sockets live. They cannot live under `--state-root`:
# the gate's own default state root is a `mkdtemp` under `/var/folders/.../T`
# (75+ bytes before this lane adds a single component) and an endpoint name
# alone spends 45 of the 103 bindable bytes, so no socket under such a root is
# ever bindable and every provisioning check fails on the budget before it runs.
# The installed user-level startup harness resolves the same constraint the same
# way (`installed_developer_startup.Harness.__init__`): a short runtime root of
# its own. Ownership is unchanged -- the lane creates this root, scans it for
# daemons and stray sockets, and removes it at final-cleanup.
SOCKET_ROOT_BASE = Path("/private/tmp")
SOCKET_ROOT_PREFIX = "vzt-"


def socket_root_for(state_root: Path) -> Path:
    """The short AF_UNIX root belonging one-to-one to `state_root`.

    Derived rather than random so every phase of one gate run addresses the same
    root, and distinct so two lane runs never share one.
    """
    digest = hashlib.sha256(str(state_root).encode("utf-8", "surrogateescape")).hexdigest()[:12]
    return SOCKET_ROOT_BASE / (SOCKET_ROOT_PREFIX + digest)


class UncertainEffects(Exception):
    """A CLI observer did not terminate within its deadline; effects unknown."""


class CleanupError(Exception):
    """Owned runtime state could not be positively removed."""


class LaneState:
    """Paths under `<state-root>/topology`, the lane's short AF_UNIX socket root,
    and the isolated CLI environment."""

    def __init__(self, state_root: Path, release_bin: Path):
        self.state_root = Path(state_root)
        self.root = self.state_root / STATE_SUBDIR
        self.socket_root = socket_root_for(self.state_root)
        self.runtime = self.socket_root / "d"
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
        self.socket_root.mkdir(mode=0o700, exist_ok=True)

    def roots(self) -> tuple:
        """Every directory this lane owns: persisted state and its AF_UNIX root."""
        return (self.root, self.socket_root)

    def isolate_runtime(self, name: str) -> Path:
        """One isolate's runtime directory: its daemon socket and Docker endpoints."""
        require(0 < len(name.encode()) <= ISOLATE_NAME_BYTES,
                f"isolate name over the {ISOLATE_NAME_BYTES}-byte AF_UNIX budget: {name!r}")
        return self.socket_root / name

    def socket_budget(self) -> dict:
        """The longest AF_UNIX paths this lane can produce, against the limit."""
        daemon = len(str(self.socket).encode())
        endpoint = len(str(self.socket_root / ("x" * ISOLATE_NAME_BYTES) / ("x" * ENDPOINT_NAME_BYTES)).encode())
        return {"daemon_socket_bytes": daemon, "worst_case_endpoint_bytes": endpoint,
                "limit_bytes": SOCKET_PATH_LIMIT, "bindable": max(daemon, endpoint) <= SOCKET_PATH_LIMIT}

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


class Held:
    """An invocation started and still running, awaiting its deliberate end.

    The process group, not the process: `start_new_session` gave the CLI its own
    group, and a signal to the leader alone would leave whatever it spawned
    holding the inherited pipes, so the drain would then block for that child's
    lifetime instead of returning what the invocation produced.
    """

    def __init__(self, index: int, label: str, name: str, argv: list, row: dict, process, started: int, timeout: int):
        self.index = index
        self.label = label
        self.name = name
        self.argv = argv
        self.row = row
        self.process = process
        self.started = started
        self.timeout = timeout

    def _signal(self, number: int) -> None:
        try:
            os.killpg(self.process.pid, number)
        except (ProcessLookupError, PermissionError):
            try:
                self.process.send_signal(number)
            except ProcessLookupError:
                pass

    def terminate(self) -> tuple:
        """`(stdout, stderr, signal)`; `signal` is `None` if it outlived SIGKILL."""
        self._signal(signal.SIGTERM)
        try:
            stdout, stderr = self.process.communicate(timeout=self.timeout)
            return stdout, stderr, signal.SIGTERM
        except subprocess.TimeoutExpired:
            pass
        self._signal(signal.SIGKILL)
        try:
            stdout, stderr = self.process.communicate(timeout=self.timeout)
            return stdout, stderr, signal.SIGKILL
        except subprocess.TimeoutExpired:
            return b"", b"", None


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
        # Issued rather than derived from `len(self.receipts)`: a held
        # invocation reserves its index when it starts and files its receipt
        # when it is released, so ordinary runs happen in between.
        self._issued = 0

    def _issue(self, label: str) -> tuple:
        self._issued += 1
        label = label_for(label)
        return self._issued, label, f"{self._issued:03}-{label}"

    def _intent(self, index: int, label: str, argv: list, cwd: Path, timeout: int) -> dict:
        return {"schema_version": 1, "kind": "vz-0.4-receipt", "run_id": self.run_id, "index": index, "label": label,
                "argv": argv, "executable": argv[0], "cwd": str(cwd), "timeout_seconds": int(timeout), "state": "intent",
                "started_unix_ns": now_ns(), "ended_unix_ns": None, "exit_code": None, "stdout_path": None,
                "stderr_path": None, "stdout_sha256": None, "stderr_sha256": None, "error": None,
                "effects_uncertain": True, "canary_withheld": False, "not_executed_reason": None}

    def _file(self, name: str, row: dict, stdout: bytes, stderr: bytes) -> None:
        row["ended_unix_ns"] = now_ns()
        row["stdout_sha256"] = sha256_bytes(stdout)
        row["stderr_sha256"] = sha256_bytes(stderr)
        if stdout:
            write_exclusive(self.receipts_dir / (name + ".stdout"), stdout)
            row["stdout_path"] = f"receipts/{name}.stdout"
        if stderr:
            write_exclusive(self.receipts_dir / (name + ".stderr"), stderr)
            row["stderr_path"] = f"receipts/{name}.stderr"
        document(self.receipts_dir / (name + ".json"), row)

    def _note_start(self, scenario_id: str, argv: list, pid: int) -> None:
        if scenario_id not in self._first_start:
            self._first_start.add(scenario_id)
            self.process_starts.append({"scenario_id": scenario_id, "argv0": argv[0], "pid": pid})

    def run(self, label: str, argv: list, *, cwd: Path, env: dict, scenario_id: str, timeout: int = 5) -> Receipt:
        index, label, name = self._issue(label)
        argv = [str(item) for item in argv]
        row = self._intent(index, label, argv, cwd, timeout)
        started = time.monotonic_ns()
        # The observer runs in its own session so a deadline kill never reaches
        # a daemon the CLI might have spawned (that is handled by stop_daemon).
        process = subprocess.Popen(argv, cwd=str(cwd), env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        self._note_start(scenario_id, argv, process.pid)
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
        row["exit_code"] = exit_code
        if timed_out:
            row.update(state="error", error=f"TimeoutExpired: observer killed after {timeout}s; effects uncertain", effects_uncertain=True)
        else:
            row.update(state="completed", effects_uncertain=exit_code < 0)
        self._file(name, row, stdout, stderr)
        receipt = Receipt(index, label, argv, exit_code, stdout, stderr, time.monotonic_ns() - started, process.pid, timed_out)
        self.receipts.append(receipt)
        if row["effects_uncertain"]:
            self.uncertain.append(receipt)
        return receipt

    def start(self, label: str, argv: list, *, cwd: Path, env: dict, scenario_id: str, timeout: int = 120) -> "Held":
        """Begin an invocation the caller holds open, and release deliberately.

        A Machine `exec` is a bounded foreground process, not a way to leave a
        daemon behind: the guest trampoline makes itself a child subreaper and
        SIGKILLs the exec's whole process group and every adopted descendant
        before it reports, so a backgrounded listener is dead before the CLI
        prints its exit code (`crates/vz-guest-agent/src/container_exec/machine.rs`,
        `reap_until_proven`). A listener that has to answer while some other
        Machine reaches it therefore has to be a foreground process held open
        for exactly that long, which is what this records.

        `timeout` is the ceiling the release enforces, not a deadline the caller
        waits on: nothing here blocks until [`release`].
        """
        index, label, name = self._issue(label)
        argv = [str(item) for item in argv]
        row = self._intent(index, label, argv, cwd, timeout)
        process = subprocess.Popen(argv, cwd=str(cwd), env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        self._note_start(scenario_id, argv, process.pid)
        return Held(index, label, name, argv, row, process, time.monotonic_ns(), timeout)

    def release(self, held: "Held") -> Receipt:
        """End a held invocation and file its receipt.

        Ending it is the point, so an invocation that stops on the signal is
        `completed` with certain effects. Only one that outlives SIGKILL is
        uncertain, because then something the lane started is still running.
        """
        stdout, stderr, signalled = held.terminate()
        stdout, stderr = stdout[:STREAM_LIMIT], stderr[:STREAM_LIMIT]
        held.row["exit_code"] = held.process.returncode
        if signalled is None:
            held.row.update(state="error", effects_uncertain=True,
                            error=f"held invocation outlived SIGKILL after {held.timeout}s; effects uncertain")
        else:
            held.row.update(state="completed", effects_uncertain=False)
        self._file(held.name, held.row, stdout, stderr)
        receipt = Receipt(held.index, held.label, held.argv, held.process.returncode, stdout, stderr,
                          time.monotonic_ns() - held.started, held.process.pid, False)
        self.receipts.append(receipt)
        if held.row["effects_uncertain"]:
            self.uncertain.append(receipt)
        return receipt

    def receipt_paths(self, receipt: Receipt) -> list:
        paths = [f"receipts/{receipt.name}.json"]
        if receipt.stdout:
            paths.append(f"receipts/{receipt.name}.stdout")
        if receipt.stderr:
            paths.append(f"receipts/{receipt.name}.stderr")
        return paths


BLOCK = 1024 * 1024
# Refuse to walk a pathological extent map forever. A file this fragmented is
# itself worth knowing about, so the fallback reads it whole rather than
# reporting a partial digest.
MAX_EXTENTS = 100_000


def _data_extents(fd: int, size: int):
    """[(offset, length)] of the regions that are not holes, or None.

    None means the filesystem would not answer, in which case the caller reads
    the file whole. An empty list means the file is entirely holes.
    """
    if not (hasattr(os, "SEEK_DATA") and hasattr(os, "SEEK_HOLE")):
        return None
    extents, offset = [], 0
    while offset < size:
        try:
            start = os.lseek(fd, offset, os.SEEK_DATA)
        except OSError as error:
            # ENXIO is "no data at or after this offset", i.e. the tail is a
            # hole and the map is complete. Anything else means the filesystem
            # does not support the query and there is no map to trust.
            if error.errno == errno.ENXIO:
                break
            return None
        try:
            end = os.lseek(fd, start, os.SEEK_HOLE)
        except OSError:
            end = size
        if end <= start:
            return None
        extents.append((start, end - start))
        if len(extents) > MAX_EXTENTS:
            return None
        offset = end
    return extents


def file_digest(path, size: int) -> str:
    """A file's content digest, without reading its holes.

    A Developer Machine's Docker `data.img` is a 64 GiB sparse file. Measured on
    one from a real lane run: 68,719,476,736 bytes logical, **29,171,712 bytes
    of actual data in 78 extents**, and `SEEK_DATA`/`SEEK_HOLE` finds that map
    in 2 milliseconds. Hashing the file whole read 2,355 times more bytes than
    it needed to, per file, and most checks inventory the state root BEFORE and
    AFTER. It was the dominant cost of a lane run.

    This is not sampling and nothing is skipped: a hole is provably zeros, and
    the extent boundaries are hashed alongside the bytes, so two files that
    differ anywhere -- in content or in where the holes are -- differ here.

    A file that is one unbroken extent, which is nearly every file, keeps the
    plain whole-file SHA-256 it has always had, so ordinary rows are unchanged
    and comparable with digests computed anywhere else. Only a file with holes
    gets the framed digest, and it is labelled `sparse:` so it can never be read
    as a number it is not.
    """
    hasher = hashlib.sha256()
    fd = os.open(path, os.O_RDONLY)
    try:
        extents = _data_extents(fd, size)
        if extents == [(0, size)] or size == 0:
            extents = None  # dense: the plain digest, exactly as before
        if extents is None:
            os.lseek(fd, 0, os.SEEK_SET)
            with open(fd, "rb", closefd=False) as stream:
                for block in iter(lambda: stream.read(BLOCK), b""):
                    hasher.update(block)
            return hasher.hexdigest()
        hasher.update(f"sparse:{size}:{len(extents)}".encode("ascii"))
        for start, length in extents:
            hasher.update(f":{start}:{length}".encode("ascii"))
            os.lseek(fd, start, os.SEEK_SET)
            remaining = length
            while remaining:
                block = os.read(fd, min(BLOCK, remaining))
                if not block:
                    break
                hasher.update(block)
                remaining -= len(block)
        return "sparse:" + hasher.hexdigest()
    finally:
        os.close(fd)


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
                rows.append([relative, "file", mode, metadata.st_size, file_digest(child, metadata.st_size)])
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
    """Live processes whose command line names either root this lane owns.

    A daemon is dispatched with both its state store (under the state root) and
    its socket (under the socket root), so neither root alone is a complete
    needle."""
    needles = [str(root) for root in state.roots()]
    return [(pid, command) for pid, command in ps_rows()
            if any(needle in command for needle in needles) and pid not in exclude_pids and pid != os.getpid()]


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
    """(pidfile, socket) pairs for every `*.pid` file under either lane root. An
    installed daemon writes `<socket stem>.pid` beside its socket, so in practice
    these are found under the socket root."""
    pairs = []
    for root in state.roots():
        for relative, kind, _mode, _size, _digest in inventory(root):
            path = root / relative
            if kind == "file" and path.suffix == ".pid":
                pairs.append((path, path.with_suffix(".sock")))
    return pairs


def stray_sockets(state: LaneState) -> list:
    """Sockets under either lane root that something is still LISTENING on.

    A socket file with no PID file beside it is the shape a leaked daemon leaves,
    which is why they are looked for. But it is also the shape an ordinary
    leftover file leaves, and the two are not the same thing: criterion 19's
    migration exercise leaves `migf/.../s` and `migr/d.sock` behind on every run
    with nothing behind either, and reporting those as leaks failed the whole
    topology lane on cleanup -- every row it would have carried -- after all
    nineteen real daemons had stopped gracefully.

    So the question asked is the one that matters: is anything accepting
    connections there? A daemon still serving answers; a leftover file refuses.
    That is the same distinction the empty-PID rule draws, decided the same way,
    by evidence rather than by the presence of an artifact.
    """
    listening = []
    for root in state.roots():
        for relative, kind, _m, _s, _d in inventory(root):
            path = root / relative
            if kind != "socket" or path.with_suffix(".pid").exists():
                continue
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            probe.settimeout(2)
            try:
                probe.connect(str(path))
            except OSError:
                # Refused, absent, or unreachable: nothing is serving here, so
                # this cannot be a daemon that outlived the sweep.
                continue
            finally:
                probe.close()
            listening.append(path)
    return listening


def _departed_daemon(pidfile: Path):
    """The PID in `pidfile` if it names no live process, else None.

    Only a well-formed PID counts. An empty or malformed file is a different
    question, answered by the vacancy rule, and a PID held by some OTHER program
    is a real problem rather than a departure.
    """
    try:
        if pidfile.is_symlink() or not pidfile.is_file() or pidfile.stat().st_size > 16:
            return None
        text = pidfile.read_text().strip()
    except OSError:
        return None
    if re.fullmatch(r"[0-9]+", text) is None:
        return None
    pid = int(text)
    return pid if pid > 1 and _proc_pidpath(pid) is None else None


def stop_daemons(state: LaneState) -> list:
    """SIGTERM every positively identified daemon under the lane root and wait
    for socket/pid removal plus the graceful shutdown log line. Returns the
    identities stopped (empty when none existed). Raises CleanupError listing
    every daemon artifact that could not be attributed and stopped positively --
    AFTER stopping the ones that could.

    The ordering matters and used to be the other way round. This raised on the
    first artifact it could not attribute, so it stopped nothing, and the
    artifact it tripped on was produced on every run: criterion 19's injected
    migration failure leaves an EMPTY `migf/d.pid`, and `migf` sorts before
    `mix`, `net-a`, `stat` and `store-deny`. So one zero-byte file left four
    healthy daemons alive with their Machines, and the next phase inherited
    them. On this host that is not merely untidy -- macOS caps concurrent
    virtual machines, so leaked daemons holding native Machines make every later
    Environment fail with `VZErrorDomain:6`, "the maximum supported number of
    active virtual machines has been reached". Three sub-checks of one run
    failed that way before this was found, and their evidence blamed the
    Environment they were creating rather than the daemons nobody stopped.

    Nothing about the contract per daemon is relaxed: each is still positively
    identified, still SIGTERMed and never force-killed, and still required to
    remove its socket and PID file and to have logged its graceful shutdown. The
    only change is that failing one no longer excuses the rest.
    """
    stopped, problems, vacant = [], [], []
    departed = []
    for pidfile, socket_path in daemon_artifacts(state):
        # A PID file naming a process that no longer exists is a daemon that is
        # GONE, which is what this sweep is for. It did not remove its own PID
        # file, so it did not exit gracefully, and that is worth recording -- but
        # it is not something left running, and failing the lane on it fails
        # every row the lane carries over a process that is already dead.
        #
        # Distinguished from the case that matters: a PID naming a DIFFERENT
        # live program is ambiguity or reuse, and still a problem below.
        gone = _departed_daemon(pidfile)
        if gone is not None:
            departed.append(f"{pidfile}: PID {gone} exited without removing its PID file")
            continue
        try:
            identity = daemon_fingerprint(state, pidfile, socket_path)
        except GateError as error:
            # A ZERO-BYTE PID file names no process, so it cannot be a daemon
            # this sweep failed to stop. It is the documented residue of
            # criterion 19's injected migration failure -- the daemon was
            # dispatched, refused to start, and never wrote its PID -- and it is
            # produced on EVERY run. Reporting it as an unattributed artifact
            # failed the whole lane on cleanup even when every real daemon had
            # been stopped and nothing leaked, which is 18 rows lost to an
            # expected empty file.
            #
            # Held aside rather than excused: the cross-check below is what
            # decides, and any other unreadable PID file -- garbage contents, a
            # symlink, an over-long file -- is still a problem here.
            if not pidfile.is_symlink() and pidfile.is_file() and pidfile.stat().st_size == 0:
                vacant.append((pidfile, socket_path))
            else:
                problems.append(f"{pidfile}: {error}")
            continue
        try:
            stopped.append(_stop_one_daemon(identity, pidfile, socket_path))
        except CleanupError as error:
            problems.append(str(error))
    if departed:
        # Reported on the receipt, never silently: an ungraceful exit is a fact
        # about this run even when nothing survived it.
        stopped.extend({"departed": note} for note in departed)
    if vacant:
        # One lane-wide question answers all of them: if nothing alive still
        # references this lane's roots, then no empty PID file can be concealing
        # a daemon, because a running daemon is dispatched with both roots on its
        # command line. If something IS alive, every empty file becomes a problem
        # again -- that is exactly the case where one might be hiding it.
        survivors = processes_referencing(state)
        if survivors:
            problems.extend(
                f"{pidfile}: empty PID file while {len(survivors)} process(es) still reference this lane: "
                + "; ".join(f"{pid} {command}" for pid, command in survivors[:4])
                for pidfile, _ in vacant
            )
    if problems:
        raise CleanupError(f"{len(stopped)} daemon(s) stopped; {len(problems)} artifact(s) not attributed and "
                           f"stopped positively: " + "; ".join(problems))
    return stopped


def _stop_one_daemon(identity: dict, pidfile: Path, socket_path: Path) -> dict:
    """SIGTERM one identified daemon and require its own positive shutdown."""
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
    return identity
