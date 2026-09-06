"""Independent ordinary Docker receipt replay, never a command dispatcher.

The parent supplies authenticated inputs and a source-derived operation order.
These readers do not infer authorization from recorded argv, certify a live
socket, or accept an interactive capture as an ordinary subprocess. A semantic
negative acknowledgement must follow the caller's separate workload proof.
"""
import json
import os
from pathlib import Path
import stat

from linux_docker_compose_evidence import MAX, read, require, runtime_proof, sha, unique

INTENT_KEYS = {'index', 'executable', 'argv', 'argv0', 'environment', 'started_unix_ns',
               'host_outcome', 'effects_uncertain', 'mutation', 'max_stream_bytes',
               'timed_out', 'interrupted', 'exit_code'}
TERMINAL_KEYS = INTENT_KEYS | {'elapsed_ns', 'output_limit_exceeded', 'capture_complete',
    'observed_bytes', 'retained_observed_stdout_bytes', 'retained_observed_stderr_bytes',
    'retained_observed_stdout_sha256', 'retained_observed_stderr_sha256', 'stdout', 'stderr',
    'stdout_sha256', 'stderr_sha256', 'raw_stdout_sha256', 'raw_stderr_sha256',
    'dispatch_error', 'secret_leak_detected', 'raw_streams_retained'}
ACK_KEYS = {'command_index', 'assertion', 'terminal_receipt_sha256', 'effects_uncertain'}
READONLY = {('context', 'inspect'), ('image', 'inspect'), ('container', 'inspect'),
            ('container', 'ls'), ('network', 'inspect'), ('network', 'ls'),
            ('volume', 'inspect'), ('volume', 'ls'), ('buildx', 'inspect'), ('compose', 'version')}


def decode(raw):
    require(type(raw) is bytes and len(raw) <= MAX, 'unbounded JSON evidence')
    def reject(_):
        raise ValueError('nonfinite JSON evidence')
    try:
        return json.loads(raw, object_pairs_hook=unique, parse_constant=reject)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError('malformed JSON evidence') from error


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def mutation_for(args):
    """Independent copy of Driver.command's source classification, not argv authority."""
    require(type(args) is list and args and all(type(v) is str and '\0' not in v for v in args),
            'invalid expected Docker argv')
    return not (args[0] in {'version', 'info', 'events', 'logs'} or tuple(args[:2]) in READONLY)


def validate_command(directory, index, inputs, *, args, expected_exit=0, mutation=None,
                     extra_env=None, require_ack=True, expected_acknowledgement=None,
                     expected_timeout_seconds=120):
    """Replay one externally expected ordinary command without inventory inference."""
    directory = Path(directory)
    require(directory.is_absolute() and directory == directory.resolve(strict=True),
            'canonical evidence directory required')
    root = directory.lstat()
    require(stat.S_ISDIR(root.st_mode) and stat.S_IMODE(root.st_mode) == 0o700 and root.st_uid == os.geteuid(),
            'private owned evidence directory required')
    require(type(index) is int and 1 <= index <= 99999 and type(expected_exit) is int and
            0 <= expected_exit <= 255 and type(require_ack) is bool, 'invalid expected command index/exit')
    require(type(expected_timeout_seconds) is int and 1 <= expected_timeout_seconds <= 300,
            'invalid source-declared command timeout')
    classified = mutation_for(args)
    require(mutation is None or (type(mutation) is bool and mutation is classified),
            'caller mutation differs from Driver source classification')
    env = {} if extra_env is None else extra_env
    require(type(env) is dict and all(type(k) is str and type(v) is str and k and
            '\0' not in k + v and '=' not in k for k, v in env.items()), 'invalid expected command environment')
    stem = 'command-%05d' % index
    raw_files = {}
    def load(suffix):
        name = stem + suffix
        raw_files[name] = read(directory / name)
        return raw_files[name]
    receipt_raw, intent_raw = load('.json'), load('.intent.json')
    receipt, intent = decode(receipt_raw), decode(intent_raw)
    require(type(receipt) is dict and set(receipt) == TERMINAL_KEYS and
            type(intent) is dict and set(intent) == INTENT_KEYS, 'unexpected ordinary command schema')
    require(not any(os.path.lexists(directory / (stem + suffix)) for suffix in
                    ('.interaction-plan.json', '.interaction-capture.json')), 'interactive command needs dedicated replay')
    argv = ['docker', '--config', inputs['docker_config'], '--context', inputs['scope']['docker_context'], *args]
    expected = {'index': index, 'argv0': 'docker', 'argv': argv,
                'executable': inputs['clients']['docker']['path'], 'environment': env, 'mutation': classified}
    for key, value in expected.items():
        require(canonical(receipt.get(key)) == canonical(value), 'command routing/metadata differs: ' + key)
    for key in ('index', 'executable', 'argv', 'argv0', 'environment', 'started_unix_ns', 'mutation', 'max_stream_bytes'):
        require(canonical(intent[key]) == canonical(receipt[key]), 'intent/terminal binding differs: ' + key)
    require(intent['host_outcome'] == 'inflight' and intent['effects_uncertain'] is True and
            intent['exit_code'] is None and intent['timed_out'] is False and intent['interrupted'] is False,
            'pre-dispatch intent not uncertain/inflight')
    require(receipt['host_outcome'] == 'exited' and receipt['capture_complete'] is True and
            receipt['raw_streams_retained'] is True and receipt['dispatch_error'] is None and
            all(receipt[k] is False for k in ('timed_out', 'interrupted', 'output_limit_exceeded', 'secret_leak_detected')) and
            type(receipt['exit_code']) is int and receipt['exit_code'] == expected_exit,
            'command not normal complete expected exit')
    require(type(receipt['started_unix_ns']) is int and receipt['started_unix_ns'] > 0 and
            type(receipt['elapsed_ns']) is int and
            0 <= receipt['elapsed_ns'] <= (expected_timeout_seconds + 10)*10**9 and
            type(receipt['max_stream_bytes']) is int and 1 <= receipt['max_stream_bytes'] <= MAX,
            'invalid command timing/output bound')
    require(type(receipt['observed_bytes']) is dict and set(receipt['observed_bytes']) == {'stdout', 'stderr'},
            'observed stream inventory differs')
    streams = {}
    for stream in ('stdout', 'stderr'):
        data = load('.' + stream)
        streams[stream] = data
        require(receipt[stream] == stem + '.' + stream and len(data) <= receipt['max_stream_bytes'] and
                type(receipt['observed_bytes'][stream]) is int and receipt['observed_bytes'][stream] == len(data) and
                type(receipt['retained_observed_' + stream + '_bytes']) is int and
                receipt['retained_observed_' + stream + '_bytes'] == len(data), 'raw stream path/count differs')
        require(all(receipt[k] == sha(data) for k in (stream + '_sha256', 'raw_' + stream + '_sha256',
                                                     'retained_observed_' + stream + '_sha256')), 'raw stream digest differs')
    uncertain = classified and expected_exit != 0
    require(receipt['effects_uncertain'] is uncertain, 'original mutation uncertainty erased or invented')
    ack_path = directory / (stem + '.acknowledgement.json')
    acknowledgement = None
    if os.path.lexists(ack_path):
        require(uncertain, 'unexpected negative acknowledgement')
        acknowledgement = decode(load('.acknowledgement.json'))
        require(type(acknowledgement) is dict and set(acknowledgement) == ACK_KEYS and
                type(acknowledgement['command_index']) is int and acknowledgement['command_index'] == index and
                acknowledgement['effects_uncertain'] is False and
                acknowledgement['terminal_receipt_sha256'] == sha(receipt_raw) and
                type(acknowledgement['assertion']) is str and 0 < len(acknowledgement['assertion']) <= 4096,
                'unbound semantic acknowledgement')
        if expected_acknowledgement is not None:
            require(acknowledgement['assertion'] == expected_acknowledgement, 'negative semantic assertion differs')
    require(not uncertain or not require_ack or acknowledgement is not None, 'negative mutation not acknowledged')
    require(expected_acknowledgement is None or (uncertain and type(expected_acknowledgement) is str and
            expected_acknowledgement), 'unexpected semantic acknowledgement expectation')
    # A command can be pre-ack replayed while its directory grows. Re-read only
    # this exact bounded set, never claim a final global inventory here.
    require(all(read(directory / name) == raw for name, raw in raw_files.items()), 'command evidence changed during replay')
    after = directory.lstat()
    require((root.st_dev, root.st_ino, root.st_mode, root.st_uid) ==
            (after.st_dev, after.st_ino, after.st_mode, after.st_uid) and directory == directory.resolve(strict=True),
            'evidence root identity changed')
    return dict(streams, receipt=receipt, intent=intent, acknowledgement=acknowledgement,
                terminal_sha256=sha(receipt_raw), host_semantic_proof_required=True)


def validate_guard(directory, inputs, context_index, info_index):
    """Replay exact context/Engine guards; no current filesystem socket lookup."""
    require(type(context_index) is int and type(info_index) is int and info_index == context_index + 1,
            'guard command indices not adjacent')
    scope = inputs['scope']
    context = validate_command(directory, context_index, inputs, args=['context', 'inspect', scope['docker_context']])
    info = validate_command(directory, info_index, inputs, args=['info', '--format', '{{json .}}'])
    require(not context['stderr'] and not info['stderr'] and
            context['receipt']['started_unix_ns'] + context['receipt']['elapsed_ns'] <= info['receipt']['started_unix_ns'],
            'guard diagnostics or overlapping command order')
    contexts, engine = decode(context['stdout']), decode(info['stdout'])
    require(type(contexts) is list and len(contexts) == 1 and type(contexts[0]) is dict and
            contexts[0].get('Name') == scope['docker_context'], 'foreign context')
    endpoints = contexts[0].get('Endpoints')
    require(type(endpoints) is dict and type(endpoints.get('docker')) is dict, 'missing Docker endpoint')
    endpoint = endpoints['docker']
    require(endpoint.get('Host') == scope['docker_endpoint'] and type(scope['docker_endpoint']) is str and
            scope['docker_endpoint'].startswith('unix:///') and endpoint.get('SkipTLSVerify', False) is False,
            'foreign/insecure Docker endpoint')
    require(type(engine) is dict and engine.get('ID') == scope['engine_id'] and engine.get('OSType') == 'linux' and
            engine.get('Architecture') in ('arm64', 'aarch64') and engine.get('DefaultRuntime') == 'youki',
            'foreign Engine/target/default runtime')
    runtimes = engine.get('Runtimes')
    require(type(runtimes) is dict and type(runtimes.get('youki')) is dict and
            runtimes['youki'].get('path') == '/mnt/linux-bin/youki', 'wrong youki runtime path')
    inert = {'runc', 'io.containerd.runc.v2'} & set(runtimes)
    require(set(runtimes) <= {'youki', 'io.containerd.youki.v2', 'runc', 'io.containerd.runc.v2'} and
            all(runtimes[name] == {'path': 'runc'} for name in inert), 'alternate executable runtime metadata')
    runtime_proof(inputs)
    require(not inert or inputs.get('runtime_evidence') is not None, 'inert metadata lacks authenticated runtime inventory')
    return {'context': contexts[0], 'info': engine, 'commands': [context, info], 'public_up_authority_required': True}
