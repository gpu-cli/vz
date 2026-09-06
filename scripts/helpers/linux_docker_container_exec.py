"""Five source-selected exec I/O checks, not complete container lifecycle proof.

The caller supplies an already authenticated running service and its lifetime
guard. Replay binds invocation and input to this source, never to receipt-chosen
plans. Service ownership/lifetime guard replay remains the caller's obligation.
"""
from pathlib import Path
import re

from docker_host_driver import regular
import linux_docker_container_fixture as fixture
import linux_docker_interactive_evidence as interactive

LIMIT = 4 * 1024 * 1024
TIMEOUT = 30
ACK = 'source-selected container exec I/O capture and guest semantics verified'


def require(value, reason):
    if not value:
        raise ValueError(reason)


def operations(cid, token):
    require(type(cid) is str and re.fullmatch(r'[0-9a-f]{64}', cid), 'exact container ID required')
    fixture.token(token)
    ready = fixture.encode({'schema_version': 1, 'type': 'tty_ready', 'token': token,
                            'isatty': [True, True, True], 'rows': 24, 'cols': 80}) + b'\r\n'
    sized = fixture.encode({'schema_version': 1, 'type': 'tty_size', 'token': token,
                            'rows': 40, 'cols': 120}) + b'\r\n'
    resized = fixture.encode({'schema_version': 1, 'type': 'tty_resized', 'token': token,
                              'rows': 40, 'cols': 120}) + b'\r\n'
    def plan(mode, actions, size=1):
        return {'schema_version': 1, 'mode': mode, 'timeout_seconds': TIMEOUT,
                'input_limit': size, 'output_limit': LIMIT, 'actions': actions}
    def args(mode, flags=(), user='0:0'):
        return ['exec', *flags, '--user', user, '--workdir', '/workspace', cid,
                'python3', '-u', '/fixture/probe.py', mode, token]
    return [
        {'name': 'root', 'args': args('exec'), 'exit': 37,
         'plan': plan('pipes', [{'kind': 'close_stdin'}])},
        {'name': 'nonroot', 'args': args('exec', user='10001:10001'), 'exit': 37,
         'plan': plan('pipes', [{'kind': 'close_stdin'}])},
        {'name': 'stream', 'args': args('stream', ['--interactive']), 'exit': 37,
         'plan': plan('pipes', [{'kind': 'write', 'data': fixture.INPUT,
             'after': {'stream': 'stderr', 'marker': fixture.marker(token, 'stderr-begin')}},
             {'kind': 'close_stdin'}], len(fixture.INPUT))},
        {'name': 'tty-exit', 'args': args('tty', ['--interactive', '--tty']), 'exit': 37,
         'plan': plan('pty', [{'kind': 'resize', 'rows': 40, 'cols': 120,
             'after': {'stream': 'tty', 'marker': ready}},
             {'kind': 'write', 'data': b'size\n', 'after': {'stream': 'tty', 'marker': resized}},
             {'kind': 'write', 'data': b'exit\n', 'after': {'stream': 'tty', 'marker': sized}}], 10)},
        {'name': 'tty-sigint', 'args': args('tty', ['--interactive', '--tty']), 'exit': 130,
         'plan': plan('pty', [{'kind': 'write', 'data': b'\x03',
             'after': {'stream': 'tty', 'marker': ready}}])},
    ]


def invocation(inputs, args):
    require(type(inputs) is dict, 'external invocation binding required')
    return ['docker', '--config', inputs['docker_config'], '--context',
            inputs['scope']['docker_context'], *args]


def check(output, inputs, operation, index, token, *, environment):
    proof = interactive.validate_recorded(output, index, argv=invocation(inputs, operation['args']),
        executable=inputs['clients']['docker']['path'], env=environment,
        expected_exit=operation['exit'], expected_plan=operation['plan'])
    stem = 'command-%05d' % index
    stdout, stderr = [regular(Path(output) / (stem + '.' + name), LIMIT) for name in ('stdout', 'stderr')]
    name = operation['name']
    if name in ('root', 'nonroot'):
        semantic = fixture.parse_exec(stdout, stderr, operation['exit'], token)
        uid = 0 if name == 'root' else 10001
        require(semantic['uid'] == semantic['gid'] == uid, 'exec user differs')
        if name == 'root':
            require(semantic['namespaces'] == semantic['pid1_namespaces'], 'root exec namespaces differ')
    elif name == 'stream':
        semantic = fixture.validate_stream(stdout, stderr, operation['exit'], token)
    else:
        require(stderr == b'', 'TTY capture cannot have separate stderr')
        semantic = fixture.validate_tty(stdout, operation['exit'], token,
                                        mode='exit' if name == 'tty-exit' else 'sigint')
    return {'name': name, 'command_index': index, 'capture': proof, 'semantic': semantic}


def result(cid, token, rows):
    paired = fixture.validate_exec_pair(rows[0]['semantic'], rows[1]['semantic'], token)
    return {'schema_version': 1, 'scope': 'exec_io_only_service_lifecycle_guard_replay_required',
            'container_id': cid, 'token': token, 'fixture_sha256': fixture.FIXTURE_SHA256,
            'operations': rows, 'paired_identity': paired}


def run_exec_io(item, cid, token, *, service_guard):
    """Run once; failed capture/semantics never acknowledges uncertain effects."""
    require(callable(service_guard), 'exact service lifetime guard required')
    fixture.fixture_contract()
    require(item.record.max_stream_bytes == LIMIT, 'interactive output bound differs')
    inputs = item.inputs.raw
    rows = []
    for operation in operations(cid, token):
        item.guard()
        service_guard(cid, token)
        command = item.command(operation['args'], expected=None, timeout=TIMEOUT,
                               interaction_plan=operation['plan'])
        row = check(item.output, inputs, operation, command.index, token, environment=item.env)
        if operation['name'] == 'nonroot':
            fixture.validate_exec_pair(rows[0]['semantic'], row['semantic'], token)
        item.guard()
        service_guard(cid, token)
        item.record.acknowledge_negative(command, ACK)
        rows.append(row)
    observation = result(cid, token, rows)
    # No acceptance flags or lifecycle cleanup are owned by this module.
    require(replay_exec_io(item.output, inputs, cid, token, observation, environment=item.env) == observation,
            'exec I/O replay differs')
    return observation


def replay_exec_io(output, inputs, cid, token, observations, *, environment):
    """Rebuild all five proofs from immutable raw command files and fixed plans."""
    fixture.fixture_contract()
    require(type(observations) is dict and type(observations.get('operations')) is list and
            len(observations['operations']) == 5, 'five ordered exec operations required')
    rows, previous = [], 0
    for operation, claimed in zip(operations(cid, token), observations['operations']):
        require(type(claimed) is dict and claimed.get('name') == operation['name'], 'exec phase differs')
        index = claimed.get('command_index')
        require(type(index) is int and previous < index <= 99999, 'exec command order differs')
        previous = index
        row = check(output, inputs, operation, index, token, environment=environment)
        require(interactive.canonical(row) == interactive.canonical(claimed), 'exec raw proof differs')
        ack = interactive.parse(regular(Path(output) / ('command-%05d.acknowledgement.json' % index), 65536))
        require(interactive.canonical(ack) == interactive.canonical({
            'command_index': index, 'assertion': ACK,
            'terminal_receipt_sha256': row['capture']['terminal_receipt_sha256'], 'effects_uncertain': False}),
            'exec negative acknowledgement differs')
        rows.append(row)
    proof = result(cid, token, rows)
    require(interactive.canonical(proof) == interactive.canonical(observations), 'exec observation differs')
    return proof
