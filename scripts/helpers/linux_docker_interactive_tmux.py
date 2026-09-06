#!/usr/bin/env python3
"""Owned local tmux probe smoke, not Docker or release certification.

The caller supplies a fresh evidence directory and the frozen probe digest.
No user tmux server, terminal, shell configuration, Docker or SSH is selected.
All commands use an authenticated private socket and -N (never start a server).
The separately owned foreground -D server is reaped, never signaled by guessed PID.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import tempfile
import time
import uuid

import linux_docker_interactive_capture as capture

LIMIT = 65536
TIMEOUT = 10
ENV = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'TERM': 'xterm-256color'}


def require(value, reason):
    if not value:
        raise ValueError(reason)


def read(path, limit=LIMIT):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_size <= limit, 'regular input bound')
        raw = os.read(fd, limit + 1)
        after = os.fstat(fd)
        def stable(value):
            return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink,
                    value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        require(len(raw) == before.st_size and stable(before) == stable(after), 'input changed')
        return raw
    finally:
        os.close(fd)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def document(path, value):
    with path.open('xb') as stream:
        stream.write((json.dumps(value, sort_keys=True, indent=2) + '\n').encode())
        stream.flush(); os.fsync(stream.fileno())


class Smoke:
    def __init__(self, args):
        self.evidence = Path(args.evidence)
        require(self.evidence.is_absolute() and self.evidence.parent.resolve(strict=True) == self.evidence.parent and
                not os.path.lexists(self.evidence), 'fresh canonical evidence required')
        self.evidence.mkdir(mode=0o700)
        self.private = Path(tempfile.mkdtemp(prefix='vzio-tmux-', dir='/private/tmp')).resolve(strict=True)
        metadata = self.private.lstat()
        self.private_identity = (metadata.st_dev, metadata.st_ino, metadata.st_uid)
        self.socket = self.private / 'server.sock'
        self.session = 'vzio-' + uuid.uuid4().hex
        self.token = 'vzio-' + uuid.uuid4().hex[:24]
        self.tmux, self.python, self.fixture = (Path(p).resolve(strict=True) for p in
                                               (args.tmux, args.python, args.fixture))
        self.inputs = {str(p): sha(read(p, 64 * 1024 * 1024)) for p in
                      (self.tmux, self.python, self.fixture, Path(__file__).resolve(), Path(capture.__file__).resolve())}
        require(self.inputs[str(self.fixture)] == args.fixture_sha256, 'frozen fixture differs')
        require(all(os.access(p, os.X_OK) for p in (self.tmux, self.python)), 'tool not executable')
        self.server = None
        self.socket_identity = None
        self.pane = None
        self.session_id = None
        self.pane_pid = None
        self.count = 0
        self.pending = []
        self.server_streams = []
        document(self.evidence / 'inputs.json', {'schema_version': 1, 'inputs_sha256': self.inputs,
            'fixture': str(self.fixture), 'token': self.token, 'session_name': self.session,
            'private_root': str(self.private), 'socket': str(self.socket), 'environment': ENV,
            'scope': 'local_tmux_container_io_probe_not_docker_acceptance'})

    def guard(self):
        for name, digest in self.inputs.items():
            require(sha(read(Path(name), 64 * 1024 * 1024)) == digest, 'tool or fixture changed')
        metadata = self.private.lstat()
        require(stat.S_ISDIR(metadata.st_mode) and stat.S_IMODE(metadata.st_mode) == 0o700 and
                metadata.st_uid == os.getuid() and
                (metadata.st_dev, metadata.st_ino, metadata.st_uid) == self.private_identity,
                'private tmux root changed')
        if self.socket_identity is not None:
            current = self.socket.lstat()
            require(stat.S_ISSOCK(current.st_mode) and
                    (current.st_dev, current.st_ino, current.st_uid) == self.socket_identity, 'tmux socket changed')
        for stream in self.server_streams:
            require(os.fstat(stream.fileno()).st_size <= LIMIT, 'tmux server diagnostic bound')

    def command(self, label, args):
        self.guard()
        self.count += 1
        stem = '%03d-%s' % (self.count, label)
        argv = [str(self.tmux), '-N', '-f', '/dev/null', '-S', str(self.socket), *args]
        document(self.evidence / (stem + '.intent.json'), {'argv': argv, 'executable': str(self.tmux),
            'environment': ENV, 'started_unix_ns': time.time_ns(), 'socket_identity': self.socket_identity})
        result = capture.capture(argv, executable=str(self.tmux), cwd=str(self.private), env=ENV,
            plan={'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': 5,
                  'input_limit': 1, 'output_limit': LIMIT, 'actions': [{'kind': 'close_stdin'}]})
        if result.pending_process is not None:
            self.pending.append(result.pending_process)
        document(self.evidence / (stem + '.result.json'), result.receipt)
        for name, raw in (('stdout', result.stdout), ('stderr', result.stderr)):
            with (self.evidence / (stem + '.' + name)).open('xb') as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        require(result.receipt['capture_complete'] and not result.receipt['effects_uncertain'] and
                result.pending_process is None and result.returncode == 0 and not result.stderr,
                'tmux command not acknowledged: ' + label)
        return result.stdout

    def identity(self):
        raw = self.command('identity', ['display-message', '-p', '-t', self.session,
            '#{pid}|#{session_id}|#{pane_id}|#{pane_pid}|#{pane_tty}|#{pane_width}|#{pane_height}'])
        match = re.fullmatch(rb'([0-9]+)\|(\$[0-9]+)\|(%[0-9]+)\|([0-9]+)\|(/dev/ttys?[A-Za-z0-9]+)\|([0-9]+)\|([0-9]+)\n', raw)
        require(match is not None and int(match[1]) == self.server.pid and self.server.returncode is None,
                'foreground tmux process differs')
        identity = (match[2].decode(), match[3].decode(), int(match[4]))
        require(self.pane is None or identity == (self.session_id, self.pane, self.pane_pid), 'pane identity changed')
        self.session_id, self.pane, self.pane_pid = identity
        return {'server_pid': int(match[1]), 'session_id': self.session_id, 'pane_id': self.pane,
                'pane_pid': self.pane_pid, 'tty': match[5].decode(), 'cols': int(match[6]), 'rows': int(match[7])}

    def frame(self, phase):
        raw = self.command('frame-' + phase, ['capture-pane', '-p', '-J', '-S', '-', '-t', self.pane])
        # Keep actual terminal bytes in the command artifact. Joined soft wraps
        # make each canonical public JSON record independently reconstructable.
        rows = [line for line in raw.splitlines() if line]
        for line in rows:
            require(len(line) <= 512, 'pane record bound')
        self.command('ansi-' + phase, ['capture-pane', '-p', '-e', '-S', '-', '-t', self.pane])
        return rows

    def wait_rows(self, expected, phase):
        deadline = time.monotonic() + TIMEOUT
        for _ in range(50):
            self.identity()
            rows = self.frame(phase)
            complete_prefix = rows == expected[:len(rows)]
            partial_last = (0 < len(rows) <= len(expected) and rows[:-1] == expected[:len(rows)-1] and
                            expected[len(rows)-1].startswith(rows[-1]))
            require(complete_prefix or partial_last, 'unexpected pane transcript')
            if rows == expected:
                return rows
            require(time.monotonic() < deadline, 'pane readiness deadline')
            time.sleep(.05)
        raise ValueError('pane readiness sample bound')

    def retire_socket(self):
        """Retire only the authenticated socket after positive normal reap."""
        require(self.server is not None and self.server.returncode == 0,
                'socket retirement requires normal server reap')
        require(self.socket_identity is not None and self.socket.parent == self.private,
                'socket retirement identity absent')
        fd = os.open(self.private, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            def directory_guard():
                for metadata in (os.fstat(fd), self.private.lstat()):
                    require(stat.S_ISDIR(metadata.st_mode) and stat.S_IMODE(metadata.st_mode) == 0o700 and
                            (metadata.st_dev, metadata.st_ino, metadata.st_uid) == self.private_identity,
                            'socket retirement directory changed')
            def inventory():
                names = []
                with os.scandir(fd) as entries:
                    for entry in entries:
                        names.append(entry.name)
                        require(len(names) <= 1, 'unexpected private socket entries')
                require(names in ([], [self.socket.name]), 'unexpected private socket entry')
                return names
            def socket_guard():
                metadata = os.stat(self.socket.name, dir_fd=fd, follow_symlinks=False)
                require(stat.S_ISSOCK(metadata.st_mode) and
                        (metadata.st_dev, metadata.st_ino, metadata.st_uid) == self.socket_identity,
                        'socket retirement target changed')
            directory_guard()
            names = inventory()
            if names:
                socket_guard()
            intent = {'schema_version': 1, 'server_pid': self.server.pid, 'server_returncode': 0,
                      'server_reaped': True, 'private_root': str(self.private),
                      'directory_identity': self.private_identity, 'socket': str(self.socket),
                      'socket_identity': self.socket_identity, 'socket_present': bool(names),
                      'started_unix_ns': time.time_ns()}
            document(self.evidence / 'socket-retirement.intent.json', intent)
            directory_guard()
            require(inventory() == names, 'socket retirement inventory changed')
            if names:
                socket_guard()
                os.unlink(self.socket.name, dir_fd=fd)
            require(inventory() == [], 'private socket directory not empty')
            directory_guard()
            self.private.rmdir()
            require(not os.path.lexists(self.private), 'private socket directory remains')
            document(self.evidence / 'socket-retirement.result.json', {
                'schema_version': 1, 'socket_unlinked': bool(names), 'socket_removed': True,
                'private_directory_removed': True, 'finished_unix_ns': time.time_ns()})
        finally:
            os.close(fd)

    def run(self):
        outcome = {'schema_version': 1, 'scope': 'local_tmux_probe_only', 'passed': False,
                   'error': None, 'cleanup_error': None, 'server_reaped': False,
                   'socket_removed': False, 'cleanup_fallback': None,
                   'host_termios_restoration_certified': False}
        try:
            self.guard()
            self.server_streams = [(self.evidence / ('server.' + name)).open('xb') for name in ('stdout', 'stderr')]
            argv = [str(self.tmux), '-D', '-f', '/dev/null', '-S', str(self.socket)]
            document(self.evidence / 'server.intent.json', {'argv': argv, 'environment': ENV,
                     'foreground_owned_child': True, 'started_unix_ns': time.time_ns()})
            self.server = subprocess.Popen(argv, executable=str(self.tmux), cwd=self.private, env=ENV,
                stdin=subprocess.DEVNULL, stdout=self.server_streams[0], stderr=self.server_streams[1],
                start_new_session=True, close_fds=True)
            document(self.evidence / 'server.started.json', {'pid': self.server.pid,
                     'owned_direct_child': True, 'started_unix_ns': time.time_ns()})
            deadline = time.monotonic() + 5
            while not os.path.lexists(self.socket):
                require(time.monotonic() < deadline, 'tmux socket startup deadline')
                self.guard(); time.sleep(.05)
            metadata = self.socket.lstat()
            require(stat.S_ISSOCK(metadata.st_mode) and metadata.st_uid == os.getuid(), 'foreign tmux socket')
            self.socket_identity = (metadata.st_dev, metadata.st_ino, metadata.st_uid)
            self.command('status-off', ['set-option', '-g', 'status', 'off'])
            self.command('remain', ['set-window-option', '-g', 'remain-on-exit', 'on'])
            self.command('remain-format', ['set-window-option', '-g', 'remain-on-exit-format', ''])
            self.command('create', ['new-session', '-d', '-s', self.session, '-x', '80', '-y', '24',
                         str(self.python), '-B', '-u', str(self.fixture), 'tty', self.token])
            original = self.identity()
            require((original['cols'], original['rows']) == (80, 24), 'initial pane size')
            # tmux 3.6 spawn.c:142 passes NULL to default_window_size;
            # resize.c:120-122 dereferences it if the GLOBAL type is manual.
            # Set manual sizing only on the already authenticated new window.
            self.command('manual-size', ['set-window-option', '-t', self.pane, 'window-size', 'manual'])
            def record(kind, **values):
                return json.dumps(dict(schema_version=1, token=self.token, type=kind, **values),
                                  sort_keys=True, separators=(',', ':')).encode()
            ready = record('tty_ready', cols=80, rows=24, isatty=[True, True, True])
            sized = record('tty_size', cols=120, rows=40)
            done = record('tty_done', exit_code=37)
            self.wait_rows([ready], 'ready')
            self.command('resize', ['resize-window', '-t', self.pane, '-x', '120', '-y', '40'])
            resized = self.identity()
            require((resized['cols'], resized['rows']) == (120, 40), 'resized pane size')
            self.command('send-size', ['send-keys', '-t', self.pane, '-l', 'size'])
            self.command('size-enter', ['send-keys', '-t', self.pane, 'Enter'])
            self.wait_rows([ready, sized], 'size')
            self.command('send-exit', ['send-keys', '-t', self.pane, '-l', 'exit'])
            self.command('exit-enter', ['send-keys', '-t', self.pane, 'Enter'])
            self.wait_rows([ready, sized, done], 'done')
            deadline = time.monotonic() + 5
            while True:
                dead = self.command('pane-exit', ['display-message', '-p', '-t', self.pane,
                                    '#{pane_dead}|#{pane_dead_status}'])
                if dead == b'1|37\n': break
                require(dead == b'0|\n' and time.monotonic() < deadline, 'wrong or missing pane exit')
                time.sleep(.05)
            outcome.update(passed=True, original_identity=original, resized_identity=resized,
                           pane_dead_status=37, records=[json.loads(v) for v in (ready, sized, done)])
        except BaseException as error:
            outcome['error'] = type(error).__name__ + ': ' + str(error)
        finally:
            if self.server is not None:
                try:
                    self.guard()
                    # Even on workload failure require the exact foreground
                    # server identity before addressing its private socket.
                    raw = self.command('cleanup-server-identity', ['display-message', '-p', '#{pid}'])
                    require(raw == (str(self.server.pid) + '\n').encode() and self.server.returncode is None,
                            'cleanup server identity differs')
                    self.command('kill-owned-server', ['kill-server'])
                    self.server.wait(timeout=5)
                    outcome['server_reaped'] = True
                    require(self.server.returncode == 0, 'tmux server shutdown unproven')
                    for process in self.pending:
                        process.wait(timeout=5)
                    self.retire_socket()
                    outcome['socket_removed'] = True
                except BaseException as error:
                    outcome['cleanup_error'] = type(error).__name__ + ': ' + str(error)
                    outcome['passed'] = False
                    fallback = {'owned_pid': self.server.pid, 'signal_dispatched': False,
                                'signal': 'SIGTERM', 'reaped': False, 'returncode': None,
                                'socket_removed': False, 'error': None}
                    outcome['cleanup_fallback'] = fallback
                    try:
                        # A bounded wait first consumes an already exited child.
                        # If still live, signal_owned verifies the unreaped direct
                        # child's PID/session/group before addressing that group.
                        try:
                            self.server.wait(timeout=.05)
                        except subprocess.TimeoutExpired:
                            try:
                                capture.signal_owned(self.server, self.server.pid, signal.SIGTERM)
                                fallback['signal_dispatched'] = True
                            except BaseException as signal_error:
                                # Exit can race the identity check. Reap if possible,
                                # but retain the signal error rather than inventing
                                # a successful signal or trying an unverified PID.
                                fallback['error'] = type(signal_error).__name__ + ': ' + str(signal_error)
                            self.server.wait(timeout=5)
                        fallback.update(reaped=True, returncode=self.server.returncode,
                                        socket_removed=not os.path.lexists(self.socket))
                        outcome.update(server_reaped=True, socket_removed=fallback['socket_removed'])
                        for process in self.pending:
                            process.wait(timeout=5)
                        if fallback['socket_removed']:
                            metadata = self.private.lstat()
                            require(stat.S_ISDIR(metadata.st_mode) and
                                    (metadata.st_dev, metadata.st_ino, metadata.st_uid) == self.private_identity,
                                    'fallback private root changed')
                            self.private.rmdir()
                    except BaseException as fallback_error:
                        detail = type(fallback_error).__name__ + ': ' + str(fallback_error)
                        fallback['error'] = (fallback['error'] + '; ' + detail
                                             if fallback['error'] else detail)
                document(self.evidence / 'server.disposition.json', {
                    'owned_pid': self.server.pid, 'returncode': self.server.returncode,
                    'reaped': outcome['server_reaped'], 'socket_removed': outcome['socket_removed'],
                    'cleanup_error': outcome['cleanup_error'], 'fallback': outcome['cleanup_fallback'],
                    'finished_unix_ns': time.time_ns()})
            for stream in self.server_streams:
                stream.flush(); os.fsync(stream.fileno()); stream.close()
            outcome['passed'] = outcome['passed'] and outcome['server_reaped'] and outcome['socket_removed']
            document(self.evidence / 'result.json', outcome)
            manifest = []
            for path in sorted(self.evidence.iterdir()):
                require(path.is_file() and not path.is_symlink(), 'unexpected evidence object')
                manifest.append(sha(read(path, 64 * 1024 * 1024)) + '  ' + path.name + '\n')
            with (self.evidence / 'checksums.sha256').open('xb') as stream:
                stream.write(''.join(manifest).encode()); stream.flush(); os.fsync(stream.fileno())
        return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', required=True)
    parser.add_argument('--fixture', required=True)
    parser.add_argument('--fixture-sha256', required=True)
    parser.add_argument('--tmux', default='/opt/homebrew/bin/tmux')
    parser.add_argument('--python', default='/usr/bin/python3')
    args = parser.parse_args()
    os.umask(0o077)
    result = Smoke(args).run()
    print(json.dumps({'passed': result['passed'], 'evidence': args.evidence,
                      'error': result['error'], 'cleanup_error': result['cleanup_error']}, sort_keys=True))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
