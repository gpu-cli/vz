"""Frozen DEV guest protocol admission; host transport/lifecycle proof is separate.

These pure validators consume already captured bytes. They never launch a
client, infer a signal from a numeric exit alone, or certify Docker parity.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat

FIXTURE = Path(__file__).resolve().parents[2] / 'tests/fixtures/vz-0.4/docker-container-io'
FIXTURE_SHA256 = '7c964069b26ff1dac16fd1ef3a951c11c7758da97c0bf6b46a1d37d12b48e4da'
FILES = {
    'Dockerfile': 'ef2679e4bb53c0fbe99773f2f21ecc637e21b86dce5282221d4a7fab497454e3',
    'README.md': 'e2d14ae958c76510b9fd31d7bef2befc152caa3fbe3c1b15303e4f3a2b47201f',
    'contract.json': '83f17609eee73632a0b8f7660aec5442bee0bce02d3395f31f80a8ac3a183ca0',
    'probe.py': '5746e12767aab3e1b82e6e8097af01f3fbf349dce1c5a5b14940dd9628d6cc9e',
    'test_probe.py': '2a043c1ff2bc3f96ec4a0a56e58a710abdee19e7a7cee947fdda720722cb7b1b',
}
BASE = 'docker.io/library/python@sha256:d04f49f5882f49a3b91f874e75e19f0c265f7222da8659741a9d7eab148f22a9'
INPUT = bytes(range(256)) * 257
INPUT_SHA256 = '120c518a83325c66464701a6ee080302f332bc768ea3f60473b209f1bfb091df'
NAMESPACES = ('cgroup', 'ipc', 'mnt', 'net', 'pid', 'user', 'uts')
LIMIT = 65536


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def token(value):
    require(type(value) is str and re.fullmatch(r'vzio-[0-9a-f]{24}', value) is not None,
            'invalid container I/O token')
    return value


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def unique(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate JSON key')
        value[key] = item
    return value


def parse(raw):
    require(type(raw) is bytes and len(raw) <= LIMIT, 'JSON bytes exceed bound')
    try:
        return json.loads(raw, object_pairs_hook=unique,
                          parse_constant=lambda value: require(False, 'nonfinite JSON value'))
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError('malformed bounded JSON') from error


def regular(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and
                stat.S_IMODE(before.st_mode) == 0o644 and before.st_size <= LIMIT,
                'fixture input metadata differs')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(LIMIT + 1)
        after = os.fstat(fd)
        signature = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        require(signature(before) == signature(after) and len(raw) == before.st_size,
                'fixture input changed while reading')
        return raw, signature(before)
    finally:
        os.close(fd)


def fixture_contract(path=FIXTURE):
    """Return exact pinned contract after all five regular public files match."""
    path = Path(path)
    require(path.is_absolute() and path == path.resolve(strict=True) and
            stat.S_ISDIR(path.lstat().st_mode), 'canonical fixture directory required')
    root_before = path.lstat()
    root_signature = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_mtime_ns, s.st_ctime_ns)
    def inventory():
        names = set()
        with os.scandir(path) as entries:
            for entry in entries:
                names.add(entry.name)
                require(len(names) <= len(FILES), 'fixture inventory exceeds bound')
        require(names == set(FILES), 'fixture inventory differs')
    inventory()
    rows, snapshots, contents = [], {}, {}
    for name in sorted(FILES):
        raw, signature = regular(path / name)
        require(sha(raw) == FILES[name], 'fixture source bytes differ: ' + name)
        rows.append([name, 0o644, len(raw), sha(raw)])
        snapshots[name], contents[name] = signature, raw
    require(sha(encode(rows)) == FIXTURE_SHA256, 'fixture tree digest differs')
    contract = parse(contents['contract.json'])
    require(contract['base_reference'] == BASE and contents['Dockerfile'].startswith(('ARG FIXTURE_BASE='+BASE+'\n').encode()) and
            b'COPY --chmod=0644 probe.py /fixture/probe.py\n' in contents['Dockerfile'] and
            b'WORKDIR /workspace\n' in contents['Dockerfile'], 'fixture image contract differs')
    inventory()
    for name, expected in snapshots.items():
        require(regular(path / name)[1] == expected, 'fixture snapshot changed during admission')
    require(root_signature(path.lstat()) == root_signature(root_before) and path == path.resolve(strict=True),
            'fixture directory changed during admission')
    return contract


def marker(owner, name):
    return ('vzio|' + token(owner) + '|' + name + '\n').encode()


def exact_exit(value, expected):
    require(type(value) is int and value == expected, 'fixture exit code differs')


def validate_stream(stdout, stderr, exit_code, owner):
    token(owner)
    exact_exit(exit_code, 37)
    expected = marker(owner, 'stdout-begin') + INPUT + b'\n' + marker(owner, 'stdout-end')
    require(type(stdout) is bytes and stdout == expected and type(stderr) is bytes and
            stderr == marker(owner, 'stderr-begin') + marker(owner, 'stderr-end'), 'binary stream payload differs')
    return {'schema_version': 1, 'token': owner, 'input_sha256': INPUT_SHA256,
            'input_bytes': len(INPUT), 'stdout_sha256': sha(stdout), 'stderr_sha256': sha(stderr),
            'exit_code': 37, 'host_eof_timing_and_transport_proof_required': True}


def records(raw, owner, *, newline=b'\n'):
    token(owner)
    require(newline in (b'\n', b'\r\n') and type(newline) is bytes and type(raw) is bytes and
            0 < len(raw) <= LIMIT and raw.endswith(newline), 'incomplete bounded JSON records')
    lines = raw[:-len(newline)].split(newline)
    require(len(lines) <= 32, 'too many guest records')
    result = []
    for line in lines:
        row = parse(line)
        require(type(row) is dict and encode(row) == line and type(row.get('schema_version')) is int and
                row['schema_version'] == 1 and row.get('token') == owner, 'noncanonical or foreign guest record')
        result.append(row)
    return result


def validate_tty(raw, exit_code, owner, *, mode='exit', newline=b'\r\n'):
    require(mode in ('exit', 'sigint'), 'unknown TTY operation')
    rows = records(raw, owner, newline=newline)
    ready = {'schema_version': 1, 'type': 'tty_ready', 'token': owner,
             'isatty': [True, True, True], 'rows': 24, 'cols': 80}
    if mode == 'exit':
        expected = [ready, {'schema_version': 1, 'type': 'tty_size', 'token': owner, 'rows': 40, 'cols': 120},
                    {'schema_version': 1, 'type': 'tty_done', 'token': owner, 'exit_code': 37}]
        exact_exit(exit_code, 37)
    else:
        expected = [ready, {'schema_version': 1, 'type': 'observed_signal', 'token': owner,
                           'signal': 'SIGINT', 'exit_code': 130}]
        exact_exit(exit_code, 130)
    require(encode(rows) == encode(expected), 'TTY guest transcript differs')
    return {'schema_version': 1, 'token': owner, 'mode': mode, 'records': rows, 'sha256': sha(raw),
            'host_input_resize_signal_and_terminal_restoration_proof_required': True,
            'signal_delivery_certified': False}


def parse_exec(stdout, stderr, exit_code, owner):
    token(owner)
    exact_exit(exit_code, 37)
    require(type(stdout) is bytes and len(stdout) <= LIMIT and type(stderr) is bytes and
            stdout.endswith(marker(owner, 'exec-stdout')) and stderr == marker(owner, 'exec-stderr'),
            'exec streams differ')
    rows = records(stdout[:-len(marker(owner, 'exec-stdout'))], owner)
    require(len(rows) == 1, 'exec identity record count differs')
    row = rows[0]
    require(set(row) == {'schema_version', 'type', 'token', 'uid', 'gid', 'cwd', 'pid', 'pid1',
                         'root_marker', 'namespaces', 'pid1_namespaces'} and row['type'] == 'exec_identity' and
            all(type(row[k]) is int for k in ('uid', 'gid', 'pid', 'pid1')) and
            row['uid'] >= 0 and row['gid'] >= 0 and row['pid'] > 1 and row['pid1'] == 1 and
            row['cwd'] == '/workspace' and row['root_marker'] == 'vz-container-io-root-v1\n',
            'exec identity fields differ')
    for field in ('namespaces', 'pid1_namespaces'):
        require(type(row[field]) is dict and set(row[field]) == set(NAMESPACES), 'namespace inventory differs')
        for name, value in row[field].items():
            if type(value) is dict:
                require(field == 'pid1_namespaces' and row['uid'] != 0 and
                        set(value) == {'error', 'errno'} and value['error'] == 'permission_denied' and
                        type(value['errno']) is int and value['errno'] in (1, 13), 'invalid namespace denial')
            else:
                require(type(value) is str and re.fullmatch(name + r':\[[1-9][0-9]*\]', value) is not None,
                        'namespace ID differs')
    return row


def validate_exec_pair(root, nonroot, owner):
    # Re-parse the provided structures through the strict byte protocol so a
    # caller cannot bypass shape/type checks by constructing an identity dict.
    root, nonroot = [parse_exec(encode(row)+b'\n'+marker(owner, 'exec-stdout'),
                               marker(owner, 'exec-stderr'), 37, owner) for row in (root, nonroot)]
    require(root['uid'] == root['gid'] == 0 and nonroot['uid'] == nonroot['gid'] == 10001 and
            root['pid'] != nonroot['pid'] and root['namespaces'] == root['pid1_namespaces'] and
            root['namespaces'] == nonroot['namespaces'], 'paired exec identity or namespaces differ')
    for name, value in nonroot['pid1_namespaces'].items():
        require(type(value) is dict or value == root['pid1_namespaces'][name], 'nonroot PID1 namespace differs')
    return {'schema_version': 1, 'token': owner, 'root': root, 'nonroot': nonroot,
            'same_container_incarnation_and_host_exec_flags_proof_required': True}


def validate_health_state(raw, owner, state):
    require(state in ('starting', 'healthy', 'unhealthy'), 'unknown expected health state')
    expected = {'schema_version': 1, 'type': 'health_state', 'token': token(owner), 'state': state}
    require(records(raw, owner) == [expected], 'health state differs')
    return expected


def validate_health_probe(stdout, stderr, exit_code, *, state):
    require(state in ('starting', 'healthy', 'unhealthy'), 'unknown expected health state')
    require(type(stdout) is bytes and stdout == b'' and type(stderr) is bytes and stderr == b'',
            'health probe emitted operational diagnostics')
    exact_exit(exit_code, 0 if state == 'healthy' else 1)
    return {'state': state, 'exit_code': exit_code, 'engine_health_transition_proof_required': True}


def validate_service(stdout, stderr, owner, *, signals=(), exit_code=None):
    token(owner)
    require(type(signals) is tuple and len(signals) <= 16 and
            all(s in ('SIGUSR1', 'SIGUSR2', 'SIGTERM') for s in signals) and
            ('SIGTERM' not in signals or signals[-1] == 'SIGTERM' and signals.count('SIGTERM') == 1),
            'invalid expected service signals')
    ready = {'schema_version': 1, 'type': 'service_ready', 'token': owner, 'pid': 1,
             'health': 'starting', 'output': 'stdout'}
    expected = [ready]
    for name in signals:
        expected.append({'schema_version': 1, 'type': 'observed_signal', 'token': owner, 'signal': name, 'exit_code': 143}
                        if name == 'SIGTERM' else
                        {'schema_version': 1, 'type': 'health_changed', 'token': owner,
                         'healthy': name == 'SIGUSR1', 'signal': name})
    require(encode(records(stdout, owner)) == encode(expected) and
            encode(records(stderr, owner)) == encode([{**ready, 'output': 'stderr'}]),
            'service stream records differ')
    if signals and signals[-1] == 'SIGTERM':
        exact_exit(exit_code, 143)
    else:
        require(exit_code is None, 'service completion without actual signal observation')
    return {'schema_version': 1, 'token': owner, 'signals': list(signals),
            'stdout_sha256': sha(stdout), 'stderr_sha256': sha(stderr),
            'host_signal_lifecycle_and_log_follow_proof_required': True, 'signal_delivery_certified': False}
