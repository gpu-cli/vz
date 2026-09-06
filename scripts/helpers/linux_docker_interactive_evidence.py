"""Public interaction plans and independent replay of bounded host I/O evidence.

This verifies capture, not Docker semantics or release-scenario acceptance. In
particular a CLI signal receipt alone does not prove delivery to a container.
"""
import base64
import hashlib
import json

MAX_BYTES = 4 * 1024 * 1024
MAX_PLAN_BYTES = 8 * 1024 * 1024
CLOCK_TOLERANCE_NS = 1_000_000_000


class InvalidEvidence(ValueError):
    pass


def require(value, reason):
    if not value:
        raise InvalidEvidence(reason)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def parse(raw):
    require(type(raw) is bytes and len(raw) <= MAX_PLAN_BYTES, 'JSON exceeds bound')
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate JSON field')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(InvalidEvidence('nonfinite JSON')))


def blob(data):
    require(type(data) is bytes, 'public byte input required')
    return {'encoding': 'base64', 'bytes': len(data), 'sha256': digest(data),
            'value': base64.b64encode(data).decode('ascii')}


def unblob(value, limit):
    require(type(value) is dict and set(value) == {'encoding', 'bytes', 'sha256', 'value'} and
            value['encoding'] == 'base64' and type(value['bytes']) is int and
            0 < value['bytes'] <= limit and type(value['value']) is str and
            len(value['value']) <= 4 * ((limit + 2) // 3), 'invalid byte binding')
    data = base64.b64decode(value['value'], validate=True)
    require(blob(data) == value, 'byte input size, digest or canonical encoding differs')
    return data


def encode_plan(plan):
    """Replace only public input/marker bytes, retaining their exact hashes."""
    encoded = dict(plan, actions=[])
    for action in plan['actions']:
        row = dict(action)
        if 'data' in row:
            row['data'] = blob(row['data'])
        if 'after' in row:
            row['after'] = dict(row['after'], marker=blob(row['after']['marker']))
        encoded['actions'].append(row)
    raw = canonical(encoded)
    # Independent validation before any plan is admitted for dispatch.
    decode_plan(raw)
    return raw


def decode_plan(raw):
    plan = parse(raw)
    require(type(plan) is dict and set(plan) == {'schema_version', 'mode', 'timeout_seconds',
            'input_limit', 'output_limit', 'actions'}, 'plan fields differ')
    require(type(plan['schema_version']) is int and plan['schema_version'] == 1 and
            plan['mode'] in ('pipes', 'pty'), 'plan schema or mode differs')
    require(type(plan['timeout_seconds']) in (int, float) and 0 < plan['timeout_seconds'] <= 120,
            'invalid deadline')
    for key in ('input_limit', 'output_limit'):
        require(type(plan[key]) is int and 0 < plan[key] <= MAX_BYTES, 'invalid byte limit')
    require(type(plan['actions']) is list and len(plan['actions']) <= 32, 'action count exceeds bound')
    total, closed = 0, False
    for row in plan['actions']:
        fields = {'write': {'data'}, 'close_stdin': set(), 'resize': {'rows', 'cols'}, 'signal': {'name'}}
        require(type(row) is dict and type(row.get('kind')) is str and row['kind'] in fields,
                'unknown action')
        kind = row['kind']
        require(set(row) == {'kind'} | fields[kind] | ({'after'} if 'after' in row else set()),
                'action fields differ')
        if 'after' in row:
            after = row['after']
            require(type(after) is dict and set(after) == {'stream', 'marker'} and
                    after['stream'] in (('tty',) if plan['mode'] == 'pty' else ('stdout', 'stderr')),
                    'marker stream differs')
            after['marker'] = unblob(after['marker'], 4096)
        if kind == 'write':
            require(not closed, 'write after EOF')
            row['data'] = unblob(row['data'], plan['input_limit'])
            total += len(row['data'])
            require(total <= plan['input_limit'], 'total input exceeds bound')
        elif kind == 'close_stdin':
            require(plan['mode'] == 'pipes' and not closed, 'duplicate EOF or PTY half-close')
            closed = True
        elif kind == 'resize':
            require(plan['mode'] == 'pty' and all(type(row[k]) is int and 1 <= row[k] <= 4096
                    for k in ('rows', 'cols')), 'invalid resize')
        else:
            require(row['name'] in ('SIGINT', 'SIGTERM'), 'unapproved signal')
    return plan


def timestamp(row):
    require(type(row) is dict and set(row) == {'unix_ns', 'monotonic_ns'} and
            all(type(v) is int and v > 0 for v in row.values()), 'invalid observation time')
    return row['monotonic_ns']


def validate_capture(plan_raw, receipt, stdout, stderr, *, argv, executable, cwd, env, expected_exit):
    """Replay complete capture against externally selected invocation and exit.

    Resource ownership, Machine incarnation, guest semantics and surrounding
    guard receipts remain requirements of the caller, not assertions here.
    """
    plan = decode_plan(plan_raw)
    require(type(expected_exit) is int and 0 <= expected_exit <= 255, 'expected normal CLI exit required')
    require(type(receipt) is dict and type(receipt.get('schema_version')) is int and receipt.get('schema_version') == 1 and
            receipt.get('argv') == argv and receipt.get('executable') == executable and
            receipt.get('cwd') == str(cwd) and receipt.get('environment') == env,
            'capture invocation differs')
    require(receipt.get('capture_complete') is True and receipt.get('effects_uncertain') is False and
            receipt.get('owned_process_reaped') is True and receipt.get('owned_direct_child') is True and
            receipt.get('returncode') == expected_exit and type(receipt.get('returncode')) is int and
            receipt.get('error') is None and receipt.get('cleanup_error') is None and
            receipt.get('termination') is None, 'capture incomplete or process outcome differs')
    pid = receipt.get('pid')
    require(type(pid) is int and pid > 0 and all(type(receipt.get(k)) is int and receipt[k] == pid
            for k in ('process_group', 'session_id')), 'owned process identity differs')
    require(type(receipt.get('timeout_seconds')) in (int, float) and
            all(type(receipt.get(k)) is int for k in ('input_limit', 'output_limit_each', 'planned_action_count')),
            'capture numeric types differ')
    require(receipt.get('mode') == plan['mode'] and receipt.get('timeout_seconds') == plan['timeout_seconds'] and
            receipt.get('input_limit') == plan['input_limit'] and
            receipt.get('output_limit_each') == plan['output_limit'] and
            receipt.get('planned_action_count') == len(plan['actions']) and
            receipt.get('merged_tty') is (plan['mode'] == 'pty'), 'capture plan binding differs')
    require(type(stdout) is bytes and type(stderr) is bytes and
            max(len(stdout), len(stderr)) <= plan['output_limit'], 'raw output exceeds bound')
    outputs = {'stdout': stdout, 'stderr': stderr, 'tty': b''} if plan['mode'] == 'pipes' else {
        'stdout': b'', 'stderr': b'', 'tty': stdout}
    require(plan['mode'] == 'pipes' or stderr == b'', 'PTY cannot prove separate stderr')
    require(canonical(receipt.get('outputs')) == canonical({k: {'size': len(v), 'sha256': digest(v)} for k, v in outputs.items()}),
            'raw output bindings differ')
    start, end = timestamp(receipt['started']), timestamp(receipt['completed'])
    start_wall, end_wall = receipt['started']['unix_ns'], receipt['completed']['unix_ns']
    require(0 <= end - start <= (plan['timeout_seconds'] + 1) * 1_000_000_000,
            'capture exceeds its deadline')
    require(start_wall <= end_wall and abs((end_wall - start_wall) - (end - start)) <= CLOCK_TOLERANCE_NS,
            'capture wall clock is incoherent with monotonic time')
    actions = receipt.get('actions')
    require(type(actions) is list and len(actions) == len(plan['actions']), 'missing or extra actions')
    previous, previous_wall, eof, counts = start, start_wall, 0, {k: 0 for k in outputs}
    for index, (planned, actual) in enumerate(zip(plan['actions'], actions)):
        require(actual.get('index') == index and type(actual.get('index')) is int and
                actual.get('kind') == planned['kind'] and actual.get('complete') is True,
                'action identity or completion differs')
        triggered, completed = timestamp(actual['triggered']), timestamp(actual['completed'])
        require(previous <= triggered <= completed <= end, 'action timeline is out of order')
        trigger_wall, complete_wall = actual['triggered']['unix_ns'], actual['completed']['unix_ns']
        require(previous_wall <= trigger_wall <= complete_wall <= end_wall and
                abs((trigger_wall - start_wall) - (triggered - start)) <= CLOCK_TOLERANCE_NS and
                abs((complete_wall - start_wall) - (completed - start)) <= CLOCK_TOLERANCE_NS,
                'action wall clock is incoherent with monotonic time')
        previous = completed
        previous_wall = complete_wall
        observed = actual.get('observed_bytes')
        require(type(observed) is dict and set(observed) == set(outputs) and
                all(type(observed[k]) is int and counts[k] <= observed[k] <= len(outputs[k]) for k in outputs),
                'observed prefix counts differ')
        counts = observed
        if 'after' not in planned:
            require(actual.get('trigger') == {'kind': 'immediate'}, 'unexpected trigger')
        else:
            after, trigger = planned['after'], actual['trigger']
            offset = outputs[after['stream']][:observed[after['stream']]].find(after['marker'])
            require(offset >= 0 and canonical(trigger) == canonical({'kind': 'marker', 'stream': after['stream'],
                    'start_offset': offset, 'end_offset': offset + len(after['marker']),
                    'marker_sha256': digest(after['marker'])}), 'unobserved or foreign trigger marker')
        kind = planned['kind']
        if kind == 'write':
            require(type(actual.get('written_bytes')) is int and type(actual.get('input_size')) is int and
                    actual.get('written_bytes') == len(planned['data']) and
                    actual.get('input_size') == len(planned['data']) and
                    actual.get('input_sha256') == digest(planned['data']), 'input write differs')
        elif kind == 'close_stdin':
            eof += 1
        else:
            signal = 'SIGWINCH' if kind == 'resize' else planned['name']
            require(actual.get('signal') == signal and actual.get('signal_scope') == 'owned_cli_group',
                    'signal identity or scope differs')
            if kind == 'resize':
                require(all(type(actual.get(k)) is int and actual[k] == planned[k] for k in ('rows', 'cols')), 'resize differs')
    require(type(receipt.get('stdin_eof_count')) is int and receipt['stdin_eof_count'] == eof, 'EOF count differs')
    terminal = receipt.get('terminal')
    if plan['mode'] == 'pipes':
        require(terminal is None, 'pipe capture claims a terminal')
    else:
        require(type(terminal) is dict and terminal.get('initial_size') == [24, 80] and
                terminal.get('controlling_terminal_claimed') is False and
                terminal.get('client_restored_attributes') is True and terminal.get('restored_verified') is True and
                terminal.get('repaired_by_harness') is False, 'terminal restoration was not supplied by client')
        initial = terminal.get('initial_attributes')
        require(type(initial) is list and len(initial) == 7 and
                all(type(v) is int for v in initial[:6]) and type(initial[6]) is list and bool(initial[6]) and
                all(type(v) is int and 0 <= v <= 255 for v in initial[6]) and
                initial == terminal.get('child_exited_termios') == terminal.get('before_harness_restore_attributes') ==
                terminal.get('restored_attributes'), 'terminal attributes differ')
    return {'scope': 'host_interactive_capture_only_not_docker_semantics_or_release_acceptance',
            'plan_sha256': digest(plan_raw), 'mode': plan['mode'], 'action_count': len(actions),
            'exit_code': expected_exit, 'stdin_eof_count': eof, 'stdout_sha256': digest(stdout),
            'stderr_sha256': digest(stderr), 'owned_process_reaped': True}


def validate_recorded(root, index, *, argv, executable, env, expected_exit, expected_plan, extra_env=None):
    """Bind capture replay to the recorder's durable intent and raw files."""
    from pathlib import Path
    from docker_host_driver import regular
    root = Path(root)
    require(root.is_absolute() and root == root.resolve() and root.is_dir(), 'canonical evidence root required')
    require(type(index) is int and 0 < index <= 99999, 'invalid command index')
    stem = f'command-{index:05d}'
    intent = parse(regular(root / (stem + '.intent.json'), MAX_PLAN_BYTES))
    terminal_raw = regular(root / (stem + '.json'), MAX_PLAN_BYTES)
    terminal = parse(terminal_raw)
    for row in (intent, terminal):
        require(type(row.get('index')) is int and row['index'] == index and row.get('argv') == argv and
                row.get('argv0') == argv[0] and row.get('executable') == executable and
                row.get('environment') == (extra_env or {}) and
                row.get('interaction_plan') == stem + '.interaction-plan.json', 'recorder invocation differs')
    require(intent.get('host_outcome') == 'inflight' and intent.get('effects_uncertain') is True and
            intent.get('exit_code') is None and intent.get('timed_out') is False and
            intent.get('interrupted') is False, 'dispatch intent differs')
    require(terminal.get('host_outcome') == 'exited' and terminal.get('capture_complete') is True and
            terminal.get('raw_streams_retained') is True and terminal.get('secret_leak_detected') is False and
            terminal.get('timed_out') is False and terminal.get('interrupted') is False and
            terminal.get('output_limit_exceeded') is False and terminal.get('dispatch_error') is None and
            type(terminal.get('exit_code')) is int and terminal['exit_code'] == expected_exit,
            'recorder completion differs')
    require(type(intent.get('mutation')) is bool and terminal.get('mutation') is intent['mutation'] and
            terminal.get('effects_uncertain') is (intent['mutation'] and expected_exit != 0),
            'recorder mutation uncertainty differs')
    require(type(intent.get('started_unix_ns')) is int and intent['started_unix_ns'] > 0 and
            terminal.get('started_unix_ns') == intent['started_unix_ns'] and
            type(terminal.get('elapsed_ns')) is int and terminal['elapsed_ns'] >= 0,
            'recorder timing differs')
    plan_raw = regular(root / (stem + '.interaction-plan.json'), MAX_PLAN_BYTES)
    require(encode_plan(decode_plan(plan_raw)) == encode_plan(expected_plan),
            'recorded interaction differs from independently selected plan')
    require(intent.get('interaction_plan_sha256') == terminal.get('interaction_plan_sha256') == digest(plan_raw),
            'durable plan binding differs')
    outputs = {}
    for name in ('stdout', 'stderr'):
        require(terminal.get(name) == stem + '.' + name, 'raw output path differs')
        raw = regular(root / (stem + '.' + name), MAX_BYTES)
        require(terminal.get(name + '_sha256') == terminal.get('raw_' + name + '_sha256') ==
                terminal.get('retained_observed_' + name + '_sha256') == digest(raw) and
                type(terminal.get('retained_observed_' + name + '_bytes')) is int and
                terminal['retained_observed_' + name + '_bytes'] == len(raw), 'recorder raw output differs')
        outputs[name] = raw
    capture = terminal.get('interaction_capture')
    result = validate_capture(plan_raw, capture, outputs['stdout'], outputs['stderr'], argv=argv,
                              executable=executable, cwd=root, env=env | (extra_env or {}), expected_exit=expected_exit)
    require(type(intent.get('max_stream_bytes')) is int and
            intent['max_stream_bytes'] == terminal.get('max_stream_bytes') == capture['output_limit_each'],
            'recorder output bound differs')
    require(canonical(terminal.get('observed_bytes')) == canonical({k: len(v) for k, v in outputs.items()}),
            'recorder observed counts differ')
    require(intent['started_unix_ns'] <= capture['started']['unix_ns'] and
            capture['completed']['monotonic_ns'] - capture['started']['monotonic_ns'] <= terminal['elapsed_ns'],
            'capture is not enclosed by recorder interval')
    require(type(intent.get('started_monotonic_ns')) is int and intent['started_monotonic_ns'] > 0 and
            terminal.get('started_monotonic_ns') == intent['started_monotonic_ns'] and
            intent['started_monotonic_ns'] <= capture['started']['monotonic_ns'] <=
            capture['completed']['monotonic_ns'] <= intent['started_monotonic_ns'] + terminal['elapsed_ns'] and
            abs((capture['started']['unix_ns'] - intent['started_unix_ns']) -
                (capture['started']['monotonic_ns'] - intent['started_monotonic_ns'])) <= CLOCK_TOLERANCE_NS,
            'capture clock anchors differ from recorder')
    return result | {'command_index': index, 'terminal_receipt_sha256': digest(terminal_raw)}
