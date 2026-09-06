"""Bounded public-only SSH fixture protocol; never print arbitrary SSH output."""
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

AGENT = '/run/vz-build-ssh-agent'
KNOWN_HOSTS = '/fixture/inputs/known_hosts'
REQUEST = '/fixture/inputs/request.json'
LIMIT = 8192
TIMEOUT = 15
EXIT = {'authenticated': 0, 'publickey_denied': 41, 'hostkey_denied': 42,
        'operational_failure': 70}


def require(ok):
    if not ok:
        raise ValueError('fixture contract rejected')


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def read_regular(path, limit=LIMIT):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= limit)
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(limit + 1)
        after = os.fstat(fd)
        signature = lambda row: (row.st_dev, row.st_ino, row.st_size, row.st_mode,
                                 row.st_nlink, row.st_mtime_ns, row.st_ctime_ns)
        require(len(raw) == info.st_size and signature(info) == signature(after))
        return raw
    finally:
        os.close(fd)


def request(raw):
    require(type(raw) is bytes and len(raw) <= LIMIT)
    item = json.loads(raw, object_pairs_hook=unique)
    require(type(item) is dict and set(item) == {
        'schema_version', 'token', 'host', 'port', 'host_key_fingerprint'})
    require(type(item['schema_version']) is int and item['schema_version'] == 1)
    require(type(item['token']) is str and re.fullmatch(r'vzssh-[0-9a-f]{24}', item['token']))
    require(type(item['host']) is str and str(ipaddress.IPv4Address(item['host'])) == item['host'])
    address = ipaddress.IPv4Address(item['host'])
    require(address.is_private and not address.is_loopback and not address.is_unspecified and
            not address.is_link_local and not address.is_multicast)
    require(type(item['port']) is int and item['port'] == 2222)
    require(type(item['host_key_fingerprint']) is str and
            re.fullmatch(r'SHA256:[A-Za-z0-9+/]{43}', item['host_key_fingerprint']))
    return item


def known_hosts(raw, item):
    require(type(raw) is bytes and len(raw) <= 256)
    prefix = ('[' + item['host'] + ']:2222 ssh-ed25519 ').encode()
    require(raw.startswith(prefix) and raw.endswith(b'\n'))
    encoded = raw[len(prefix):-1]
    key = base64.b64decode(encoded, validate=True)
    require(base64.b64encode(key) == encoded and len(key) == 51 and
            key[:19] == b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20')
    return 'SHA256:' + base64.b64encode(hashlib.sha256(key).digest()).decode().rstrip('=')


def response(item):
    return ('vz-ssh-response:' + item['token'] + '\n').encode()


def argv(item, mounted):
    require(type(mounted) is bool)
    options = [
        'BatchMode=yes', 'StrictHostKeyChecking=yes', 'IdentityFile=none',
        'IdentityAgent=' + (AGENT if mounted else 'none'), 'IdentitiesOnly=no',
        'UserKnownHostsFile=' + KNOWN_HOSTS, 'GlobalKnownHostsFile=/dev/null',
        'KnownHostsCommand=none', 'UpdateHostKeys=no', 'VerifyHostKeyDNS=no',
        'CheckHostIP=no', 'FingerprintHash=sha256', 'HostKeyAlgorithms=ssh-ed25519',
        'PubkeyAcceptedAlgorithms=ssh-ed25519', 'PreferredAuthentications=publickey',
        'PubkeyAuthentication=yes', 'PasswordAuthentication=no',
        'KbdInteractiveAuthentication=no', 'HostbasedAuthentication=no',
        'GSSAPIAuthentication=no', 'NumberOfPasswordPrompts=0', 'AddKeysToAgent=no',
        'ForwardAgent=no', 'ClearAllForwardings=yes', 'ForwardX11=no',
        'PermitLocalCommand=no', 'ProxyCommand=none', 'ProxyJump=none',
        'ControlMaster=no', 'ControlPath=none', 'ControlPersist=no',
        'ConnectionAttempts=1', 'ConnectTimeout=5', 'ServerAliveInterval=0',
        'LogLevel=ERROR', 'RequestTTY=no',
    ]
    return ['/usr/bin/ssh', '-F', '/dev/null', '-T', '-n', '-4',
            *[part for option in options for part in ('-o', option)],
            '-p', '2222', '-l', 'vzssh', item['host'], 'vz-public-response']


def hostkey_error(item):
    # OpenSSH 9.2p1 sshconnect.c warn_changed_key / HOST_CHANGED / fail.
    lines = [
        '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@',
        '@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @',
        '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@',
        'IT IS POSSIBLE THAT SOMEONE IS DOING SOMETHING NASTY!',
        'Someone could be eavesdropping on you right now (man-in-the-middle attack)!',
        'It is also possible that a host key has just been changed.',
        'The fingerprint for the ED25519 key sent by the remote host is\n' +
        item['host_key_fingerprint'] + '.', 'Please contact your system administrator.',
        'Add correct host key in ' + KNOWN_HOSTS + ' to get rid of this message.',
        'Offending ED25519 key in ' + KNOWN_HOSTS + ':1',
        '  remove with:',
        '  ssh-keygen -f "' + KNOWN_HOSTS + '" -R "[' + item['host'] + ']:2222"',
        'Host key for [' + item['host'] + ']:2222 has changed and you have requested strict checking.',
        'Host key verification failed.',
    ]
    # Debian 9.2p1-2+deb12u10 adds the two ssh-keygen suggestion records.
    # log.c appends CRLF per record, preserving the embedded fingerprint LF.
    return ('\r\n'.join(lines) + '\r\n').encode()


def classify(item, code, stdout, stderr):
    require(type(code) is int and type(stdout) is bytes and type(stderr) is bytes and
            len(stdout) <= LIMIT and len(stderr) <= LIMIT)
    if code == 0 and stdout == response(item) and stderr == b'':
        return 'authenticated'
    denied = ('vzssh@' + item['host'] + ': Permission denied (publickey).\r\n').encode()
    if code == 255 and not stdout and stderr == denied:
        return 'publickey_denied'
    if code == 255 and not stdout and stderr == hostkey_error(item):
        return 'hostkey_denied'
    return 'operational_failure'


def capture(command):
    """Bound both pipes while reading; on failure kill only owned process and reap."""
    env = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C',
           'SSH_ASKPASS_REQUIRE': 'never'}
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=env, start_new_session=True)
    buffers = [bytearray(), bytearray()]
    deadline = time.monotonic() + TIMEOUT
    try:
        with selectors.DefaultSelector() as selector:
            for index, stream in enumerate((process.stdout, process.stderr)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, index)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                require(remaining > 0)
                for key, _ in selector.select(remaining):
                    chunk = os.read(key.fd, 4096)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    else:
                        buffers[key.data].extend(chunk)
                        require(len(buffers[key.data]) <= LIMIT)
        code = process.wait(timeout=max(0.001, deadline - time.monotonic()))
        return code, bytes(buffers[0]), bytes(buffers[1])
    except BaseException:
        # An exited parent may still have pipe-holding descendants in its own
        # session. Kill that owned process group, not only a live leader.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        raise
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()


def emit(row):
    print(json.dumps(row, sort_keys=True, separators=(',', ':')), flush=True)


def execute(mode, item, runner=capture):
    require(mode in ('mounted', 'undeclared'))
    mounted = mode == 'mounted'
    require(not os.environ.get('SSH_AUTH_SOCK') or
            (mounted and os.environ['SSH_AUTH_SOCK'] == AGENT))
    if mounted:
        require(stat.S_ISSOCK(os.lstat(AGENT).st_mode))
    else:
        require(not os.path.lexists(AGENT))
    start = time.time_ns()
    code, stdout, stderr = runner(argv(item, mounted))
    outcome = classify(item, code, stdout, stderr)
    row = {'schema_version': 1, 'type': 'ssh_result', 'token': item['token'],
           'mode': mode, 'host': item['host'], 'port': item['port'],
           'started_unix_ns': start, 'completed_unix_ns': time.time_ns(),
           'ssh_exit_code': code, 'outcome': outcome,
           'stdout_sha256': hashlib.sha256(stdout).hexdigest(),
           'stderr_sha256': hashlib.sha256(stderr).hexdigest(),
           'stdout_bytes': len(stdout), 'stderr_bytes': len(stderr)}
    if outcome == 'authenticated':
        require(mounted)
        Path('/out').mkdir(mode=0o755, exist_ok=False)
        fd = os.open('/out/ssh.txt', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
        with os.fdopen(fd, 'wb') as output:
            os.fchmod(output.fileno(), 0o644)
            output.write(stdout)
    emit(row)
    return EXIT[outcome]


def absent(item):
    require(not os.path.lexists(AGENT) and not os.environ.get('SSH_AUTH_SOCK'))
    require(read_regular('/out/ssh.txt') == response(item))
    info = os.lstat('/out/ssh.txt')
    require(stat.S_IMODE(info.st_mode) == 0o644)
    emit({'schema_version': 1, 'type': 'ssh_mount_absent', 'token': item['token'],
          'agent_path_absent': True, 'agent_environment_absent': True,
          'unix_ns': time.time_ns()})


def main():
    try:
        require(len(sys.argv) == 2)
        item = request(read_regular(REQUEST))
        known_hosts(read_regular(KNOWN_HOSTS), item)
        if sys.argv[1] == 'absent':
            absent(item)
            return 0
        return execute(sys.argv[1], item)
    except Exception:
        # No exception string, arbitrary process output, environment, or key bytes.
        emit({'schema_version': 1, 'type': 'ssh_fixture_error', 'outcome': 'operational_failure'})
        return EXIT['operational_failure']


if __name__ == '__main__':
    raise SystemExit(main())
