"""Exact-Machine Docker exec through an owned tmux pane; no release claim.

Reuse Smoke's unchanged interaction and positive cleanup implementation. Only
initialization substitutes a source-selected execve launcher for the local probe.
The caller independently verifies the surrounding service generation guards.
"""
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import uuid

import linux_docker_container_fixture as fixture
import linux_docker_interactive_evidence as evidence
import linux_docker_interactive_tmux as core

LAUNCHER = b'''import json, os, pathlib, sys
binding = json.loads(pathlib.Path(__file__).with_name("binding.json").read_bytes())
if sys.argv[1:] != ["tty", binding["token"]]:
    raise SystemExit(70)
os.execve(binding["executable"], binding["argv"], binding["environment"])
'''
PLAN = {'schema_version': 1, 'mode': 'pipes', 'timeout_seconds': 5,
        'input_limit': 1, 'output_limit': core.LIMIT, 'actions': [{'kind': 'close_stdin'}]}
require = core.require


def binding(inputs, cid, token, environment):
    require(type(cid) is str and re.fullmatch(r'[0-9a-f]{64}', cid), 'exact container ID required')
    fixture.token(token)
    config = inputs['docker_config']
    context = inputs['scope']['docker_context']
    executable = inputs['clients']['docker']['path']
    require(type(config) is str and Path(config).is_absolute() and
            type(context) is str and re.fullmatch(r'vzr1-docker_context-docker-[0-9a-f]+', context) and
            type(executable) is str and Path(executable).is_absolute(), 'exact Machine client binding required')
    require(type(environment) is dict and all(type(k) is str and type(v) is str and
            '\0' not in k + v and '=' not in k for k, v in environment.items()) and
            not any(k.startswith(('DOCKER_', 'BUILDX_', 'SSH_')) or k in ('TMUX', 'TMUX_PANE')
                    for k in environment), 'explicit isolated environment required')
    return {'schema_version': 1, 'token': token, 'executable': executable,
            'argv': ['docker', '--config', config, '--context', context, 'exec',
                     '--interactive', '--tty', '--user', '0:0', '--workdir', '/workspace',
                     cid, 'python3', '-u', '/fixture/probe.py', 'tty', token],
            'environment': dict(environment)}


class DockerSmoke(core.Smoke):
    """Smoke's exact lifecycle, initialized without its unrelated random token."""
    def __init__(self, root, selected, tmux_path):
        self.evidence = root / 'smoke'
        self.evidence.mkdir(mode=0o700)
        self.private = Path(tempfile.mkdtemp(prefix='vzio-tmux-', dir='/private/tmp')).resolve(strict=True)
        metadata = self.private.lstat()
        self.private_identity = (metadata.st_dev, metadata.st_ino, metadata.st_uid)
        self.socket = self.private / 'server.sock'
        self.session = 'vzio-' + uuid.uuid4().hex
        self.token = selected['token']
        require(Path(tmux_path).is_absolute() and Path(tmux_path).resolve(strict=True) == Path(tmux_path),
                'canonical explicitly selected tmux required')
        self.tmux = Path(tmux_path).resolve(strict=True)
        metadata = self.tmux.lstat()
        require(stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and
                metadata.st_size <= 64 * 1024 * 1024, 'tmux executable identity differs')
        self.python = Path(sys.executable).resolve(strict=True)
        self.fixture = root / 'launcher.py'
        self.inputs = {str(p): core.sha(core.read(p, 64 * 1024 * 1024)) for p in (
            self.tmux, self.python, self.fixture, root / 'binding.json',
            Path(selected['executable']), Path(__file__).resolve(),
            Path(core.__file__).resolve(), Path(core.capture.__file__).resolve())}
        require(all(os.access(p, os.X_OK) for p in (self.tmux, self.python)), 'tool not executable')
        self.server = None
        self.socket_identity = None
        self.pane = self.session_id = self.pane_pid = None
        self.count = 0
        self.pending = []
        self.server_streams = []
        core.document(self.evidence / 'inputs.json', {
            'schema_version': 1, 'inputs_sha256': self.inputs, 'fixture': str(self.fixture),
            'token': self.token, 'session_name': self.session, 'private_root': str(self.private),
            'socket': str(self.socket), 'environment': core.ENV,
            'scope': 'owned_docker_exec_tmux_adapter_not_local_probe'})


def run_tmux(item, cid, token, *, tmux_path, service_guard, register_owner):
    """Register before dispatch; retain owner.server/pending on any uncertainty."""
    require(callable(service_guard) and callable(register_owner), 'ownership callbacks required')
    fixture.fixture_contract()
    selected = binding(item.inputs.raw, cid, token, item.env)
    item.guard(); service_guard(cid, token)
    root = item.output / 'tmux'
    require(root.is_absolute() and root.parent.resolve(strict=True) == root.parent and
            not os.path.lexists(root), 'fresh canonical tmux evidence required')
    root.mkdir(mode=0o700)
    with (root / 'launcher.py').open('xb') as stream:
        stream.write(LAUNCHER); stream.flush(); os.fsync(stream.fileno())
    core.document(root / 'binding.json', selected)
    owner = DockerSmoke(root, selected, tmux_path)
    register_owner(owner)
    result = owner.run()
    require(result['passed'] is True, 'owned Docker tmux smoke failed; retain owner and effects')
    item.guard(); service_guard(cid, token)
    proof = replay_tmux(root, item.inputs.raw, cid, token,
                        environment=item.env, tmux_path=tmux_path)
    core.document(root / 'proof.json', proof)
    return proof


def replay_tmux(root, inputs, cid, token, *, environment, tmux_path):
    """Reconstruct fixed commands and canonical guest records from raw artifacts.

    This does not substitute for external service identity/generation replay or
    certify host terminal restoration (tmux frames merge the guest streams).
    """
    root = Path(root)
    require(root.is_absolute() and root.resolve(strict=True) == root, 'canonical evidence required')
    selected = binding(inputs, cid, token, environment)
    require(core.read(root / 'launcher.py') == LAUNCHER, 'launcher source differs')
    parse = lambda path: evidence.parse(core.read(path, 4 * 1024 * 1024))
    require(evidence.canonical(parse(root / 'binding.json')) == evidence.canonical(selected),
            'Docker launch binding differs')
    directory = root / 'smoke'
    manifest = core.read(directory / 'checksums.sha256', 4 * 1024 * 1024)
    names = set()
    for line in manifest.decode().splitlines():
        match = re.fullmatch(r'([0-9a-f]{64})  ([A-Za-z0-9_.-]+)', line)
        require(match is not None and match[2] not in names, 'manifest grammar or duplicate')
        names.add(match[2])
        require(core.sha(core.read(directory / match[2], 64 * 1024 * 1024)) == match[1], 'raw evidence changed')
    require(names == {p.name for p in directory.iterdir() if p.name != 'checksums.sha256'} and
            all(p.is_file() and not p.is_symlink() for p in directory.iterdir()), 'raw inventory differs')
    admitted, result = parse(directory / 'inputs.json'), parse(directory / 'result.json')
    tmux = str(Path(tmux_path).resolve(strict=True))
    python = str(Path(sys.executable).resolve(strict=True))
    expected_files = (Path(tmux), Path(python), root / 'launcher.py', root / 'binding.json',
                      Path(selected['executable']), Path(__file__).resolve(),
                      Path(core.__file__).resolve(), Path(core.capture.__file__).resolve())
    require(admitted['inputs_sha256'] == {str(p): core.sha(core.read(p, 64 * 1024 * 1024))
                                        for p in expected_files}, 'source or tool pin differs')
    require(admitted['token'] == token and admitted['environment'] == core.ENV and
            admitted['fixture'] == str(root / 'launcher.py') and
            admitted['scope'] == 'owned_docker_exec_tmux_adapter_not_local_probe', 'adapter input differs')
    private, socket, session = admitted['private_root'], admitted['socket'], admitted['session_name']
    require(re.fullmatch(r'/private/tmp/vzio-tmux-[A-Za-z0-9_]+', private) and
            socket == private + '/server.sock' and re.fullmatch(r'vzio-[0-9a-f]{32}', session), 'private scope differs')
    require(result['passed'] is True and result['error'] is None and result['cleanup_error'] is None and
            result['cleanup_fallback'] is None and result['server_reaped'] is True and
            result['socket_removed'] is True and result['pane_dead_status'] == 37 and
            result['host_termios_restoration_certified'] is False, 'normal terminal completion missing')
    original, resized = result['original_identity'], result['resized_identity']
    require(original | {'cols': 120, 'rows': 40} == resized and
            original['cols'] == 80 and original['rows'] == 24, 'pane identity/size differs')
    pane = original['pane_id']
    require(re.fullmatch(r'%[0-9]+', pane), 'pane identifier differs')
    records = [dict(schema_version=1, token=token, type='tty_ready', cols=80, rows=24, isatty=[True]*3),
               dict(schema_version=1, token=token, type='tty_size', cols=120, rows=40),
               dict(schema_version=1, token=token, type='tty_done', exit_code=37)]
    require(evidence.canonical(result['records']) == evidence.canonical(records), 'canonical guest records differ')
    lines = [evidence.canonical(row).rstrip(b'\n') for row in records]
    controls = {
        'status-off': ['set-option', '-g', 'status', 'off'],
        'remain': ['set-window-option', '-g', 'remain-on-exit', 'on'],
        'remain-format': ['set-window-option', '-g', 'remain-on-exit-format', ''],
        'create': ['new-session', '-d', '-s', session, '-x', '80', '-y', '24', python,
                   '-B', '-u', str(root / 'launcher.py'), 'tty', token],
        'manual-size': ['set-window-option', '-t', pane, 'window-size', 'manual'],
        'resize': ['resize-window', '-t', pane, '-x', '120', '-y', '40'],
        'send-size': ['send-keys', '-t', pane, '-l', 'size'],
        'size-enter': ['send-keys', '-t', pane, 'Enter'],
        'send-exit': ['send-keys', '-t', pane, '-l', 'exit'],
        'exit-enter': ['send-keys', '-t', pane, 'Enter'],
        'cleanup-server-identity': ['display-message', '-p', '#{pid}'],
        'kill-owned-server': ['kill-server']}
    observed_controls, complete_frames, dead = [], {}, False
    identity_sizes, ansi_phases = set(), set()
    control_indices = {}
    previous_mono = previous_wall = 0
    intents = sorted(directory.glob('[0-9][0-9][0-9]-*.intent.json'))
    require(0 < len(intents) <= 2048, 'command inventory bound')
    expected_inventory = {'inputs.json', 'result.json', 'server.intent.json', 'server.started.json',
        'server.disposition.json', 'server.stdout', 'server.stderr',
        'socket-retirement.intent.json', 'socket-retirement.result.json'}
    expected_inventory.update(path.name[:-12] + suffix for path in intents
                              for suffix in ('.intent.json', '.result.json', '.stdout', '.stderr'))
    require(names == expected_inventory, 'unpaired or unknown raw evidence')
    for index, path in enumerate(intents, 1):
        stem = path.name[:-12]
        require(stem.startswith('%03d-' % index), 'command order differs')
        label = stem[4:]
        intent, receipt = parse(path), parse(directory / (stem + '.result.json'))
        stdout, stderr = [core.read(directory / (stem + '.' + name)) for name in ('stdout', 'stderr')]
        if label in controls:
            args = controls[label]; observed_controls.append(label)
            control_indices[label] = index
            if label == 'cleanup-server-identity':
                require(stdout == ('%d\n' % original['server_pid']).encode(), 'cleanup PID differs')
            else:
                require(not stdout, 'unexpected control output')
        elif label == 'identity':
            args = ['display-message', '-p', '-t', session,
                    '#{pid}|#{session_id}|#{pane_id}|#{pane_pid}|#{pane_tty}|#{pane_width}|#{pane_height}']
            size = (120, 40) if 'resize' in observed_controls else (80, 24)
            expected = '%s|%s|%s|%s|%s|%s|%s\n' % (original['server_pid'], original['session_id'],
                pane, original['pane_pid'], original['tty'], *size)
            require(stdout == expected.encode(), 'raw pane ownership differs')
            identity_sizes.add(size)
        elif label.startswith(('frame-', 'ansi-')):
            phase = label.split('-', 1)[1]
            require(phase in ('ready', 'size', 'done'), 'unknown pane phase')
            args = ['capture-pane', '-p', *(['-J'] if label.startswith('frame-') else ['-e']), '-S', '-', '-t', pane]
            if label.startswith('ansi-'):
                ansi_phases.add(phase)
            if label.startswith('frame-'):
                observed = [line for line in stdout.splitlines() if line]
                expected = lines[:{'ready': 1, 'size': 2, 'done': 3}[phase]]
                require(observed == expected[:len(observed)] or
                        (0 < len(observed) <= len(expected) and observed[:-1] == expected[:len(observed)-1]
                         and expected[len(observed)-1].startswith(observed[-1])), 'raw guest frame differs')
                if observed == expected: complete_frames[phase] = index
        elif label == 'pane-exit':
            args = ['display-message', '-p', '-t', pane, '#{pane_dead}|#{pane_dead_status}']
            require(stdout in (b'0|\n', b'1|37\n') and not dead, 'raw pane exit differs')
            dead = stdout == b'1|37\n'
        else:
            raise ValueError('unexpected tmux command')
        argv = [tmux, '-N', '-f', '/dev/null', '-S', socket, *args]
        require(intent['argv'] == argv and intent['executable'] == tmux and intent['environment'] == core.ENV,
                'control invocation differs')
        evidence.validate_capture(evidence.encode_plan(PLAN), receipt, stdout, stderr,
            argv=argv, executable=tmux, cwd=private, env=core.ENV, expected_exit=0)
        require(type(intent['started_unix_ns']) is int and
                previous_wall <= intent['started_unix_ns'] <= receipt['started']['unix_ns'] and
                previous_mono <= receipt['started']['monotonic_ns'], 'control ledger clock order differs')
        previous_mono = receipt['completed']['monotonic_ns']
        previous_wall = receipt['completed']['unix_ns']
        require(not stderr, 'tmux diagnostic unexpected')
    require(observed_controls == list(controls) and set(complete_frames) == {'ready', 'size', 'done'} and
            complete_frames['ready'] < complete_frames['size'] < complete_frames['done'] and dead and
            identity_sizes == {(80, 24), (120, 40)} and ansi_phases == {'ready', 'size', 'done'},
            'ordered terminal workflow incomplete')
    require(control_indices['manual-size'] < complete_frames['ready'] < control_indices['resize'] and
            control_indices['size-enter'] < complete_frames['size'] < control_indices['send-exit'] and
            control_indices['exit-enter'] < complete_frames['done'] < control_indices['cleanup-server-identity'],
            'guest completion did not precede dependent control')
    started, disposition = parse(directory / 'server.started.json'), parse(directory / 'server.disposition.json')
    server_intent = parse(directory / 'server.intent.json')
    require(server_intent['argv'] == [tmux, '-D', '-f', '/dev/null', '-S', socket] and
            server_intent['environment'] == core.ENV and server_intent['foreground_owned_child'] is True and
            type(server_intent['started_unix_ns']) is int and
            server_intent['started_unix_ns'] <= started['started_unix_ns'] <=
            parse(intents[0])['started_unix_ns'] and
            previous_wall <= disposition['finished_unix_ns'], 'foreground server source or clock differs')
    require(started['owned_direct_child'] is True and started['pid'] == original['server_pid'] and
            disposition['owned_pid'] == started['pid'] and disposition['returncode'] == 0 and
            disposition['reaped'] is True and disposition['socket_removed'] is True and
            disposition['cleanup_error'] is None and disposition['fallback'] is None, 'server retirement differs')
    retired = parse(directory / 'socket-retirement.result.json')
    retirement = parse(directory / 'socket-retirement.intent.json')
    require(retirement['server_pid'] == started['pid'] and retirement['server_returncode'] == 0 and
            retirement['server_reaped'] is True and retirement['private_root'] == private and
            retirement['socket'] == socket and retired['socket_removed'] is True and
            retired['private_directory_removed'] is True, 'socket retirement missing')
    socket_identity = parse(intents[0])['socket_identity']
    require(type(socket_identity) is list and len(socket_identity) == 3 and
            all(type(v) is int and v >= 0 for v in socket_identity) and
            retirement['socket_identity'] == socket_identity and
            all(parse(path)['socket_identity'] == socket_identity for path in intents), 'socket ownership ledger differs')
    require(previous_wall <= retirement['started_unix_ns'] <= retired['finished_unix_ns'] <=
            disposition['finished_unix_ns'], 'retirement clock order differs')
    for stream in ('stdout', 'stderr'):
        core.read(directory / ('server.' + stream), core.LIMIT)
    return {'schema_version': 1, 'scope': 'docker_exec_tmux_only_external_service_guard_replay_required',
            'container_id': cid, 'token': token, 'fixture_sha256': fixture.FIXTURE_SHA256,
            'binding_sha256': core.sha(core.read(root / 'binding.json')),
            'raw_manifest_sha256': core.sha(manifest), 'command_count': len(intents),
            'started_unix_ns': server_intent['started_unix_ns'],
            'finished_unix_ns': disposition['finished_unix_ns'],
            'records': records, 'pane_dead_status': 37, 'server_reaped': True,
            'socket_removed': True, 'host_termios_restoration_certified': False}
