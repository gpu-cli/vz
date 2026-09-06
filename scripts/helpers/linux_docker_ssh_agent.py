"""Disposable, explicitly pinned host SSH agent; no server/network operations.

Private inputs never enter evidence. Successful host preparation is not Docker
SSH acceptance. The caller owns all Machine/server cleanup and must separately
admit that workflow; this helper only removes its exact disposable host inputs.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import threading
import time

from docker_host_driver import collect_output, contains_canary

TOOLS = {name: "/usr/bin/" + name for name in ("ssh-keygen", "ssh-agent", "ssh-add")}
LIMIT = 16 * 1024
KEY_LIFETIME = 900
ROLES = ("auth", "host", "wrong_host")
ENVIRONMENT = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C", "LANG": "C",
               "SSH_ASKPASS_REQUIRE": "never", "APPLE_SSH_ADD_BEHAVIOR": "openssh"}


class AgentError(ValueError):
    """Fixed public diagnostic; never contains a private command stream."""


def require(condition, message):
    if not condition:
        raise AgentError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(info):
    return {key: getattr(info, "st_" + key) for key in
            ("dev", "ino", "mode", "uid", "gid", "nlink", "size", "mtime_ns", "ctime_ns")}


def directory_identity(info):
    return {key: getattr(info, "st_" + key) for key in ("dev", "ino", "mode", "uid", "gid")}


def regular(path, limit, *, directory=None):
    fd = os.open(str(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 <= before.st_size <= limit,
                "bounded single-link regular input required")
        chunks, remaining = [], before.st_size + 1
        while remaining:
            block = os.read(fd, min(65536, remaining))
            if not block:
                break
            chunks.append(block)
            remaining -= len(block)
        data = b"".join(chunks)
        require(len(data) == before.st_size and identity(before) == identity(os.fstat(fd)), "input changed while reading")
        return data, identity(before)
    finally:
        os.close(fd)


def tool_inputs():
    """Read-only observation for the parent's preflight freeze, not approval."""
    result = {}
    for name, path in TOOLS.items():
        require(Path(path).resolve(strict=True) == Path(path), "redirected SSH executable")
        data, info = regular(path, 64 * 1024 * 1024)
        require(info["mode"] & 0o111 and not info["mode"] & 0o022, "unsafe SSH executable mode")
        result[name] = {"path": path, "sha256": sha(data)}
    return result


def fingerprint(public, comment):
    require(public.endswith(b"\n") and public.count(b"\n") == 1, "invalid public key line")
    fields = public[:-1].split(b" ")
    require(len(fields) == 3 and fields[0] == b"ssh-ed25519" and fields[2] == comment.encode(),
            "foreign public key type/comment")
    try:
        blob = base64.b64decode(fields[1], validate=True)
    except ValueError as error:
        raise AgentError("invalid public key encoding") from error
    require(blob[:4] == (11).to_bytes(4, "big") and blob[4:15] == b"ssh-ed25519"
            and blob[15:19] == (32).to_bytes(4, "big") and len(blob) == 51,
            "invalid Ed25519 public key wire format")
    return "SHA256:" + base64.b64encode(hashlib.sha256(blob).digest()).decode().rstrip("=")


class _Root:
    def __init__(self, path):
        self.path = Path(path)
        require(self.path.is_absolute() and self.path == self.path.resolve()
                and not any(c in str(self.path) for c in "\n\r\x00"), "canonical clean root path required")
        parent = self.path.parent.stat()
        require(stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.geteuid()
                and stat.S_IMODE(parent.st_mode) == 0o700, "owned private parent required")
        self.parent_identity = directory_identity(parent)
        self.parent_fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.fd = None
        try:
            require(directory_identity(os.fstat(self.parent_fd)) == self.parent_identity, "root parent replaced")
            os.mkdir(self.path.name, 0o700, dir_fd=self.parent_fd)
            self.fd = os.open(self.path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.parent_fd)
            self.identity = directory_identity(os.fstat(self.fd))
            self.check()
        except BaseException:
            if self.fd is not None:
                os.close(self.fd)
            os.close(self.parent_fd)
            raise

    def check(self):
        require(self.path.parent.resolve(strict=True) == self.path.parent
                and directory_identity(self.path.parent.stat()) == self.parent_identity,
                "owned root parent identity changed")
        actual = os.stat(self.path.name, dir_fd=self.parent_fd, follow_symlinks=False)
        require(directory_identity(actual) == self.identity == directory_identity(os.fstat(self.fd))
                and stat.S_ISDIR(actual.st_mode) and actual.st_uid == os.geteuid()
                and stat.S_IMODE(actual.st_mode) == 0o700, "owned root identity changed")

    def write(self, name, data):
        self.check()
        require(Path(name).name == name, "unsafe evidence filename")
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.fd)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(self.fd)
        self.check()

    def remove_empty(self):
        self.check()
        require(not os.listdir(self.fd), "private root not empty; unknown paths retained")
        os.rmdir(self.path.name, dir_fd=self.parent_fd)
        os.fsync(self.parent_fd)

    def release(self):
        os.close(self.fd)
        os.close(self.parent_fd)


class Agent:
    def __init__(self, private_root, evidence_root, *, tools, run_id, owner):
        private, evidence = Path(private_root), Path(evidence_root)
        require(private != evidence and private not in evidence.parents and evidence not in private.parents,
                "private keys must be outside evidence")
        require(len(os.fsencode(private / "agent.sock")) <= 103, "SSH agent socket path exceeds macOS limit")
        require(isinstance(run_id, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", run_id), "invalid SSH run identity")
        require(type(owner) is dict and set(owner) == {"project_id", "environment_id", "machine_id"}
                and all(isinstance(value, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", value) for value in owner.values()),
                "invalid SSH owner identity")
        require(type(tools) is dict and set(tools) == set(TOOLS), "exact three SSH tools required")
        self.tools = json.loads(json.dumps(tools))
        self.tool_identities = {}
        for name in TOOLS:
            pin = self.tools[name]
            require(type(pin) is dict and set(pin) == {"path", "sha256"} and pin["path"] == TOOLS[name]
                    and isinstance(pin["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", pin["sha256"]), "invalid SSH tool pin")
            self._tool(name)
        self.run_id, self.owner = run_id, dict(owner)
        self._private = _Root(private)
        try:
            self._evidence = _Root(evidence)
        except BaseException as original:
            try:
                self._private.remove_empty()
            except BaseException as cleanup_error:
                original.ssh_agent_cleanup_error = type(cleanup_error).__name__
            finally:
                self._private.release()
            raise
        self._files, self._fingerprints, self._canaries = {}, {}, []
        self._authorized_files = set()
        self._agent, self._agent_pid, self._agent_capture, self._capture_thread = None, None, None, None
        self._commands, self._pending, self._socket_identity = 0, [], None
        self._started, self._closed, self._failure = False, False, None
        self._closure = None
        self._close_attempts = 0
        try:
            self._document("inputs.json", {"schema_version": 1, "run_id": run_id, "owner": owner, "tools": self.tools,
                "tool_identities": self.tool_identities, "private_root": str(private), "evidence_root": str(evidence),
                "private_root_identity": self._private.identity, "environment": ENVIRONMENT,
                "key_type": "ed25519", "key_lifetime_seconds": KEY_LIFETIME,
                "private_inputs_in_evidence": False, "compatibility_certified": False})
        except BaseException as original:
            # No command can have run yet. Retain any partial evidence, close
            # descriptors, and remove only the identity-checked empty key root.
            try:
                self._private.remove_empty()
            except BaseException as cleanup_error:
                original.ssh_agent_cleanup_error = type(cleanup_error).__name__
            finally:
                self._private.release()
                self._evidence.release()
            raise

    def _tool(self, name):
        pin = self.tools[name]
        require(Path(pin["path"]).resolve(strict=True) == Path(pin["path"]), "redirected SSH executable")
        data, info = regular(pin["path"], 64 * 1024 * 1024)
        require(sha(data) == pin["sha256"] and info["mode"] & 0o111 and not info["mode"] & 0o022,
                "SSH executable pin differs")
        require(name not in self.tool_identities or self.tool_identities[name] == info, "SSH executable identity changed")
        self.tool_identities[name] = info
        return pin["path"]

    def _document(self, name, value):
        self._write(name, (json.dumps(value, sort_keys=True, indent=2) + "\n").encode())

    def _write(self, name, data):
        require(not contains_canary((data,), self._canaries)
                and b"PRIVATE KEY-----" not in data and b"openssh-key-v1" not in data, "private bytes withheld from evidence")
        self._evidence.write(name, data)

    def _capture_keys(self):
        self._private.check()
        for name in sorted(self._authorized_files):
            if name in self._files or not os.path.lexists(self._private.path / name):
                continue
            data, info = regular(name, LIMIT, directory=self._private.fd)
            require(info["uid"] == os.geteuid() and not info["mode"] & (0o077 if not name.endswith(".pub") else 0o022),
                    "unsafe generated key ownership/mode")
            self._files[name] = (info, sha(data))
            if not name.endswith(".pub") and data:
                self._canaries.extend([data, data.rstrip(b"\n")])
                body = b"".join(line for line in data.splitlines() if not line.startswith(b"-----"))
                if len(body) >= 32:
                    self._canaries.append(body)
                self._canaries.extend(line for line in data.splitlines()
                                      if len(line) >= 32 and not line.startswith(b"-----"))

    def _verify_files(self):
        self._private.check()
        require(set(os.listdir(self._private.fd)) <= set(self._files) | {"agent.sock"}, "unexpected private input path")
        for name, (info, expected) in self._files.items():
            data, actual = regular(name, LIMIT, directory=self._private.fd)
            require(actual == info and sha(data) == expected, "generated key identity/content changed")

    def _command(self, label, tool, args, *, expected_code=0, expected_stdout=b"", expected_stderr=b"", capture_keys=False):
        self._private.check()
        executable = self._tool(tool)
        self._commands += 1
        stem = "command-%03d-%s" % (self._commands, label)
        env = dict(ENVIRONMENT)
        if tool == "ssh-add":
            self._check_agent()
            require(directory_identity(os.stat("agent.sock", dir_fd=self._private.fd, follow_symlinks=False))
                    == self._socket_identity, "SSH agent socket identity changed")
            env["SSH_AUTH_SOCK"] = str(self._private.path / "agent.sock")
        intent = {"schema_version": 1, "label": label, "argv": [executable, *args], "environment": env,
                  "tool_sha256": self.tools[tool]["sha256"], "tool_identity": self.tool_identities[tool],
                  "started_unix_ns": time.time_ns(), "timeout_seconds": 10, "max_stream_bytes": LIMIT,
                  "effects_uncertain": True, "capture_complete": False}
        self._document(stem + ".intent.json", intent)
        process, stdout, stderr, error = None, b"", b"", None
        begin = time.monotonic_ns()
        try:
            process = subprocess.Popen(intent["argv"], executable=executable, cwd=self._private.path,
                env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            self._pending.append((process, process.pid))
            stdout, stderr = collect_output(process, 10, LIMIT)
        except BaseException as caught:
            error = caught
            stdout, stderr = getattr(caught, "stdout", b"") or b"", getattr(caught, "stderr", b"") or b""
        finally:
            if capture_keys and (process is None or process.returncode is not None):
                try:
                    self._capture_keys()
                except BaseException as caught:
                    error = error or caught
        complete = process is not None and process.returncode is not None and error is None
        accepted = complete and process.returncode == expected_code and stdout == expected_stdout and stderr == expected_stderr
        leaked = contains_canary((stdout, stderr), self._canaries)
        # Only exact expected public streams may be published. In particular,
        # partial keygen failures cannot leak a key before its file is captured.
        retained = accepted and not leaked
        self._write(stem + ".stdout", stdout if retained else b"[unexpected output withheld]\n")
        self._write(stem + ".stderr", stderr if retained else b"[unexpected output withheld]\n")
        terminal = dict(intent, elapsed_ns=time.monotonic_ns() - begin,
            pid=process.pid if process else None, exit_code=process.returncode if process else None,
            error_type=type(error).__name__ if error else None, effects_uncertain=not accepted,
            capture_complete=complete, raw_streams_retained=retained, secret_leak_detected=leaked,
            stdout_sha256=sha(stdout), stderr_sha256=sha(stderr), stdout_bytes=len(stdout), stderr_bytes=len(stderr))
        self._document(stem + ".json", terminal)
        if process is not None and process.returncode is not None:
            process.stdout.close()
            process.stderr.close()
        if error is not None:
            raise error
        require(accepted and not leaked, "SSH command failed or returned unexpected output: " + label)
        self._private.check()
        self._tool(tool)
        return terminal

    def _check_agent(self):
        require(self._agent is not None and self._agent.pid == self._agent_pid, "SSH agent process identity differs")
        require(self._agent.poll() is None, "SSH agent exited unexpectedly")
        # Popen is the unreaped direct child; its PID cannot be reused until
        # this owner reaps it. Never signal a PID read from a file or stdout.
        require(os.getpgid(self._agent_pid) == self._agent_pid and os.getsid(self._agent_pid) == self._agent_pid,
                "SSH agent session/group identity differs")
        require(self._agent_capture is None, "SSH agent capture ended unexpectedly")

    def _capture_agent(self):
        try:
            class StreamsOnly:
                # Only the owner thread may reap the child. Otherwise a reader
                # could release its PID between the owner's identity check and
                # SIGTERM, permitting PID/group reuse.
                stdout, stderr, args = self._agent.stdout, self._agent.stderr, self._agent.args

                def wait(self, timeout):
                    return None
            stdout, stderr = collect_output(StreamsOnly(), KEY_LIFETIME + 60, LIMIT)
            self._agent_capture = (stdout, stderr, None)
        except BaseException as error:
            self._agent_capture = (getattr(error, "stdout", b"") or b"", getattr(error, "stderr", b"") or b"", type(error).__name__)

    def start(self):
        require(not self._started and not self._closed, "SSH agent cannot be started/reused")
        self._started = True
        try:
            for role in ROLES:
                self._verify_files()
                require(all(not os.path.lexists(self._private.path / name) for name in (role, role + ".pub")),
                        "key generation target already exists")
                self._authorized_files.update((role, role + ".pub"))
                comment = "vz-ssh:" + self.run_id + ":" + role
                self._command("generate-" + role, "ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-C", comment,
                    "-f", str(self._private.path / role)], capture_keys=True)
                require(role in self._files and role + ".pub" in self._files, "key generation omitted an output")
                public, _ = regular(role + ".pub", LIMIT, directory=self._private.fd)
                self._fingerprints[role] = fingerprint(public, comment)
            require(len(set(self._fingerprints.values())) == 3, "disposable key identities repeated")
            self._verify_files()
            require(not os.path.lexists(self._private.path / "agent.sock"), "SSH socket already exists")
            executable = self._tool("ssh-agent")
            argv = [executable, "-D", "-a", str(self._private.path / "agent.sock"), "-t", str(KEY_LIFETIME)]
            self._agent_intent = {"schema_version": 1, "argv": argv, "environment": ENVIRONMENT,
                "tool_sha256": self.tools["ssh-agent"]["sha256"], "tool_identity": self.tool_identities["ssh-agent"],
                "started_unix_ns": time.time_ns(), "effects_uncertain": True, "capture_complete": False}
            self._document("agent.intent.json", self._agent_intent)
            self._agent = subprocess.Popen(argv, executable=executable, cwd=self._private.path, env=dict(ENVIRONMENT),
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            self._agent_pid = self._agent.pid
            self._capture_thread = threading.Thread(target=self._capture_agent, name="vz-private-ssh-agent", daemon=True)
            self._capture_thread.start()
            self._check_agent()
            self._document("agent.started.json", dict(self._agent_intent, pid=self._agent_pid,
                process_group=self._agent_pid, session_id=self._agent_pid, owned_direct_child=True))
            deadline = time.monotonic() + 5
            while not os.path.lexists(self._private.path / "agent.sock"):
                self._check_agent()
                require(time.monotonic() < deadline, "SSH agent socket readiness deadline")
                time.sleep(.02)
            socket = os.stat("agent.sock", dir_fd=self._private.fd, follow_symlinks=False)
            require(stat.S_ISSOCK(socket.st_mode) and socket.st_uid == os.geteuid() and stat.S_IMODE(socket.st_mode) == 0o600,
                    "unsafe SSH agent socket")
            self._socket_identity = directory_identity(socket)
            self._command("empty-identities", "ssh-add", ["-l", "-E", "sha256"], expected_code=1,
                          expected_stdout=b"The agent has no identities.\n")
            self._command("add-auth", "ssh-add", ["-q", "-k", "-t", str(KEY_LIFETIME), str(self._private.path / "auth")])
            proof = self.verify()
            self._command("prove-auth-signature", "ssh-add", ["-T", str(self._private.path / "auth.pub")])
            self._document("ready.json", proof)
            return proof
        except BaseException as error:
            self._failure = type(error).__name__
            try:
                self._document("failure.json", {"error_type": self._failure, "broader_cleanup_authorized": False})
            except BaseException:
                pass
            try:
                error.ssh_agent_cleanup = self.close()
            except BaseException as cleanup_error:
                error.ssh_agent_cleanup_error = type(cleanup_error).__name__
            raise

    def verify(self):
        require(self._started and not self._closed and self._failure is None, "SSH agent is not usable")
        self._verify_files()
        self._check_agent()
        self._tool("ssh-agent")
        actual = os.stat("agent.sock", dir_fd=self._private.fd, follow_symlinks=False)
        require(directory_identity(actual) == self._socket_identity, "SSH agent socket identity changed")
        line = "256 %s vz-ssh:%s:auth (ED25519)\n" % (self._fingerprints["auth"], self.run_id)
        self._command("sole-identity", "ssh-add", ["-l", "-E", "sha256"], expected_stdout=line.encode())
        self._check_agent()
        return {"schema_version": 1, "run_id": self.run_id, "owner": self.owner, "fingerprints": dict(self._fingerprints),
            "socket": str(self._private.path / "agent.sock"), "socket_identity": self._socket_identity,
            "pid": self._agent_pid, "process_group": self._agent_pid, "session_id": self._agent_pid,
            "owned_direct_child": True, "sole_auth_identity_verified": True, "private_inputs_in_evidence": False,
            "broader_cleanup_authorized": False, "compatibility_certified": False}

    @property
    def paths(self):
        require(self._started and not self._closed and self._failure is None, "private SSH paths unavailable")
        self._verify_files()
        self._check_agent()
        require(directory_identity(os.stat("agent.sock", dir_fd=self._private.fd, follow_symlinks=False))
                == self._socket_identity, "SSH agent socket identity changed")
        return {"auth_public_key": self._private.path / "auth.pub", "host_private_key": self._private.path / "host",
                "host_public_key": self._private.path / "host.pub", "wrong_host_public_key": self._private.path / "wrong_host.pub",
                "socket": self._private.path / "agent.sock"}

    def canaries(self):
        require(self._started and not self._closed and set(self._fingerprints) == set(ROLES),
                "private SSH canaries unavailable before readiness or after closure")
        self._verify_files()
        return tuple(self._canaries)

    @staticmethod
    def _terminate(process, pid):
        require(process.pid == pid, "owned child PID changed")
        if process.poll() is not None:
            return False
        require(os.getpgid(pid) == pid and os.getsid(pid) == pid, "owned child group/session changed")
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        return True

    def close(self):
        if self._closed:
            require(not self._closure["cleanup_errors"] and self._closure["private_inputs_removed"],
                    "SSH host cleanup remains incomplete")
            return self._closure
        errors, signaled, agent_reaped = [], False, self._agent is None
        for process, pid in self._pending:
            try:
                self._terminate(process, pid)
                process.stdout.close()
                process.stderr.close()
            except BaseException as error:
                errors.append("command_quiescence:" + type(error).__name__)
        if self._agent is not None:
            try:
                signaled = self._terminate(self._agent, self._agent_pid)
                agent_reaped = self._agent.returncode is not None
                self._capture_thread.join(timeout=5)
                require(not self._capture_thread.is_alive() and self._agent_capture is not None, "SSH agent stream closure unproven")
                stdout, stderr, capture_error = self._agent_capture
                # -D prints this exact public socket/PID announcement. Missing
                # or unrelated output cannot certify complete normal closure.
                announcement = ("SSH_AUTH_SOCK=%s; export SSH_AUTH_SOCK;\necho Agent pid %d;\n" %
                                (self._private.path / "agent.sock", self._agent_pid)).encode()
                # OpenSSH V_10_0_P2 ssh-agent.c's signal loop logs signal 15
                # and cleanup_exit(2), including for foreground -D. The
                # hash-pinned macOS executable produces this exact CRLF line.
                # Exit 2 alone (or an externally killed child) is not proof.
                raw_ok = (capture_error is None and stdout == announcement
                          and stderr == b"exiting on signal 15\r\n")
                normal_stop = signaled and agent_reaped and raw_ok and self._agent.returncode == 2
                self._write("agent.stdout", stdout if raw_ok else b"[unexpected output withheld]\n")
                self._write("agent.stderr", stderr if raw_ok else b"[unexpected output withheld]\n")
                self._document("agent.result.json", dict(self._agent_intent, pid=self._agent_pid,
                    exit_code=self._agent.returncode, sigterm_dispatched=signaled, capture_complete=capture_error is None,
                    error_type=capture_error, raw_streams_retained=raw_ok, stdout_sha256=sha(stdout), stderr_sha256=sha(stderr),
                    effects_uncertain=not normal_stop))
                require(normal_stop, "SSH agent normal closure unproven")
            except BaseException as error:
                errors.append("agent_closure:" + type(error).__name__)
            finally:
                if agent_reaped and self._agent_capture is not None:
                    self._agent.stdout.close()
                    self._agent.stderr.close()
        removed = []
        if agent_reaped and not any(item.startswith("command_quiescence:") for item in errors):
            try:
                self._capture_keys()
                self._verify_files()
                socket = self._private.path / "agent.sock"
                if os.path.lexists(socket):
                    actual = os.stat("agent.sock", dir_fd=self._private.fd, follow_symlinks=False)
                    require(self._socket_identity is not None and directory_identity(actual) == self._socket_identity,
                            "unowned socket retained")
                    os.unlink("agent.sock", dir_fd=self._private.fd)
                    removed.append("agent.sock")
                for name in sorted(self._files):
                    self._verify_files()
                    os.unlink(name, dir_fd=self._private.fd)
                    removed.append(name)
                    del self._files[name]
                self._private.remove_empty()
            except BaseException as error:
                errors.append("private_cleanup:" + type(error).__name__)
        self._closure = {"schema_version": 1, "owner": self.owner, "run_id": self.run_id,
            "pid": self._agent_pid, "agent_reaped": agent_reaped, "sigterm_dispatched": signaled,
            "private_inputs_removed": not os.path.lexists(self._private.path), "removed_names": removed,
            "cleanup_errors": errors, "original_error_type": self._failure, "broader_cleanup_authorized": False,
            "secure_erasure_claimed": False, "compatibility_certified": False, "completed_unix_ns": time.time_ns()}
        self._close_attempts += 1
        quiescent = agent_reaped and not any(item.startswith("command_quiescence:") for item in errors)
        self._document("closure.json" if not errors else "closure-attempt-%03d.json" % self._close_attempts, self._closure)
        self._closed = quiescent
        if self._closure["private_inputs_removed"]:
            self._canaries.clear()
        if quiescent:
            self._private.release()
            self._evidence.release()
        require(not errors and self._closure["private_inputs_removed"], "SSH host cleanup incomplete; original effects retained")
        return self._closure
