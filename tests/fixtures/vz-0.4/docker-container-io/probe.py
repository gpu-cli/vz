"""Deterministic public container-I/O fixture; no Docker or network client."""
import errno
import fcntl
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import struct
import sys
import termios
import time

MAX_INPUT = 1024 * 1024
IO_TIMEOUT = 30
SERVICE_TIMEOUT = 300
HEALTH = Path('/run/vz-container-io-health.json')
ROOT_MARKER = Path('/vz-container-io-root.txt')
ROOT_BYTES = b'vz-container-io-root-v1\n'
NAMESPACES = ('cgroup', 'ipc', 'mnt', 'net', 'pid', 'user', 'uts')


def require(value):
    if not value:
        raise ValueError('container I/O fixture contract rejected')


def token(value):
    require(type(value) is str and re.fullmatch(r'vzio-[0-9a-f]{24}', value))
    return value


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def emit(kind, owner, *, stream=None, **fields):
    output = sys.stdout.buffer if stream is None else stream
    output.write(encoded({'schema_version': 1, 'type': kind, 'token': owner, **fields}))
    output.flush()


def marker(owner, name):
    return ('vzio|' + owner + '|' + name + '\n').encode()


def read_bounded(fd, limit, deadline):
    result = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        require(remaining > 0 and select.select([fd], [], [], remaining)[0])
        block = os.read(fd, min(65536, limit + 1 - len(result)))
        if not block:
            return bytes(result)
        result.extend(block)
        require(len(result) <= limit)


def stream(owner):
    sys.stderr.buffer.write(marker(owner, 'stderr-begin'))
    sys.stderr.buffer.flush()
    payload = read_bounded(0, MAX_INPUT, time.monotonic() + IO_TIMEOUT)
    # Only output after actual EOF; a host observer must half-close its input
    # without truncating these delayed bytes or substituting process timeout.
    time.sleep(0.1)
    sys.stdout.buffer.write(marker(owner, 'stdout-begin') + payload + b'\n' + marker(owner, 'stdout-end'))
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(marker(owner, 'stderr-end'))
    sys.stderr.buffer.flush()
    return 37


class ObservedSignal(Exception):
    def __init__(self, number):
        self.number = number


def size():
    rows, columns, _, _ = struct.unpack('HHHH', fcntl.ioctl(0, termios.TIOCGWINSZ, b'\0' * 8))
    require(rows > 0 and columns > 0)
    return {'rows': rows, 'cols': columns}


def tty(owner):
    require(all(os.isatty(fd) for fd in (0, 1, 2)))
    saved = termios.tcgetattr(0)
    require(saved[3] & termios.ICANON and saved[3] & termios.ISIG)
    changed = saved[:]
    changed[3] &= ~termios.ECHO
    previous = signal.getsignal(signal.SIGINT)
    def interrupt(number, frame):
        raise ObservedSignal(number)
    try:
        signal.signal(signal.SIGINT, interrupt)
        termios.tcsetattr(0, termios.TCSANOW, changed)
        emit('tty_ready', owner, isatty=[True, True, True], **size())
        deadline = time.monotonic() + IO_TIMEOUT
        pending = bytearray()
        commands = 0
        while True:
            remaining = deadline - time.monotonic()
            require(remaining > 0 and select.select([0], [], [], remaining)[0])
            block = os.read(0, 128)
            require(block and len(pending) + len(block) <= 128)
            pending.extend(block)
            while b'\n' in pending:
                commands += 1
                require(commands <= 4)
                command, _, rest = pending.partition(b'\n')
                pending = bytearray(rest)
                if command == b'size':
                    emit('tty_size', owner, **size())
                elif command == b'exit':
                    require(not pending)
                    emit('tty_done', owner, exit_code=37)
                    return 37
                else:
                    require(False)
    except ObservedSignal as observed:
        require(observed.number == signal.SIGINT)
        emit('observed_signal', owner, signal='SIGINT', exit_code=130)
        return 130
    finally:
        termios.tcsetattr(0, termios.TCSANOW, saved)
        signal.signal(signal.SIGINT, previous)


def health_document(owner, state):
    require(state in ('starting', 'healthy', 'unhealthy'))
    return {'schema_version': 1, 'type': 'health_state', 'token': owner, 'state': state}


def read_health(owner):
    fd = os.open(HEALTH, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(fd)
        require(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and
                stat.S_IMODE(metadata.st_mode) == 0o644 and metadata.st_size <= 256)
        raw = os.read(fd, 257)
        # Byte equality also refuses duplicate keys, noncanonical encodings,
        # unexpected fields, wrong tokens, and integer-for-boolean substitutions.
        for state in ('starting', 'healthy', 'unhealthy'):
            if raw == encoded(health_document(owner, state)):
                return state
        require(False)
    finally:
        os.close(fd)


def write_health(owner, state):
    if os.path.lexists(HEALTH):
        read_health(owner)
        metadata = HEALTH.lstat()
        require(metadata.st_uid == os.getuid())
    temporary = HEALTH.with_name(HEALTH.name + '.next')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    try:
        os.fchmod(fd, 0o644)
        payload = encoded(health_document(owner, state))
        require(os.write(fd, payload) == len(payload))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, HEALTH)


def service(owner):
    pending = []
    def observe(number, frame):
        require(len(pending) < 32)
        pending.append(number)
    signals = (signal.SIGTERM, signal.SIGUSR1, signal.SIGUSR2)
    previous = {number: signal.getsignal(number) for number in signals}
    try:
        for number in signals:
            signal.signal(number, observe)
        write_health(owner, 'starting')
        emit('service_ready', owner, pid=os.getpid(), health='starting', output='stdout')
        emit('service_ready', owner, stream=sys.stderr.buffer, pid=os.getpid(), health='starting', output='stderr')
        deadline = time.monotonic() + SERVICE_TIMEOUT
        while time.monotonic() < deadline:
            while pending:
                number = pending.pop(0)
                if number == signal.SIGTERM:
                    emit('observed_signal', owner, signal='SIGTERM', exit_code=143)
                    return 143
                healthy = number == signal.SIGUSR1
                write_health(owner, 'healthy' if healthy else 'unhealthy')
                emit('health_changed', owner, healthy=healthy, signal='SIGUSR1' if healthy else 'SIGUSR2')
            time.sleep(0.01)
        require(False)
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def namespace_ids(pid):
    result = {}
    for name in NAMESPACES:
        try:
            value = os.readlink('/proc/' + str(pid) + '/ns/' + name)
        except PermissionError as error:
            require(pid == 1 and os.geteuid() != 0 and error.errno in (errno.EACCES, errno.EPERM))
            result[name] = {'error': 'permission_denied', 'errno': error.errno}
            continue
        require(re.fullmatch(name + r':\[[0-9]+\]', value))
        result[name] = value
    return result


def execute(owner):
    fd = os.open(ROOT_MARKER, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(fd)
        require(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and
                metadata.st_size == len(ROOT_BYTES) and stat.S_IMODE(metadata.st_mode) == 0o644)
        require(os.read(fd, len(ROOT_BYTES) + 1) == ROOT_BYTES)
    finally:
        os.close(fd)
    emit('exec_identity', owner, uid=os.getuid(), gid=os.getgid(), cwd=os.getcwd(),
         pid=os.getpid(), pid1=1, root_marker=ROOT_BYTES.decode(),
         namespaces=namespace_ids('self'), pid1_namespaces=namespace_ids(1))
    sys.stdout.buffer.write(marker(owner, 'exec-stdout'))
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(marker(owner, 'exec-stderr'))
    sys.stderr.buffer.flush()
    return 37


def main(argv):
    try:
        require(len(argv) == 2)
        mode, value = argv
        if mode == 'exit':
            require(value in ('0', '37', '130', '137', '143'))
            return int(value)
        owner = token(value)
        require(mode in ('stream', 'tty', 'service', 'health', 'exec'))
        if mode == 'health':
            return 0 if read_health(owner) == 'healthy' else 1
        return {'stream': stream, 'tty': tty, 'service': service, 'exec': execute}[mode](owner)
    except Exception:
        print('VZ_CONTAINER_IO_CONTRACT_REJECTED', file=sys.stderr, flush=True)
        return 70


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
