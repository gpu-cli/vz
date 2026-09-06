"""Independent bounded SSH Buildx replay; no provisioning or lifecycle authority."""
import base64
import ipaddress
import json
from pathlib import Path
import re
import stat

import linux_docker_artifact_layout as layout
from linux_docker_artifact_stream import CanaryScanner
from linux_docker_artifact_evidence import OCI_OPTIONS, CACHE_OPTIONS, OCI_EXPORT, CACHE_EXPORT, export_steps
from linux_docker_build_evidence import Replay as BuildReplay, progress_ns
from linux_docker_compose_evidence import Invalid, MAX, decode, fixture_digest, hex64, read, require, runtime_proof, sha
from linux_docker_parallel_evidence import absolute

CASES = {'declared', 'undeclared', 'provider_omitted', 'wrong_host'}
OP_KEYS = {'schema_version', 'case', 'run_id', 'ssh_fixture', 'ssh_fixture_sha256',
           'build_context', 'build_context_sha256', 'request', 'known_hosts_sha256',
           'agent_socket', 'output', 'cache_output', 'build_argv'}
# Pinned Buildx registers its raw SSH provider even when its config map is empty.
MISSING = 'unset ssh forward key fixture'
RESULT_KEYS = {'schema_version', 'type', 'token', 'mode', 'host', 'port', 'started_unix_ns',
               'completed_unix_ns', 'ssh_exit_code', 'outcome', 'stdout_sha256', 'stderr_sha256',
               'stdout_bytes', 'stderr_bytes'}


def scan_json(value, canaries):
    """Scan decoded JSON strings too, including escaped diagnostic metadata."""
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            CanaryScanner(canaries).feed(item.encode())
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


def public_request(value):
    require(type(value) is dict and set(value) == {'schema_version', 'token', 'host', 'port', 'host_key_fingerprint'},
            'SSH public request shape')
    require(type(value['schema_version']) is int and value['schema_version'] == 1 and
            type(value['port']) is int and value['port'] == 2222 and
            isinstance(value['token'], str) and re.fullmatch(r'vzssh-[0-9a-f]{24}', value['token']), 'SSH public identity')
    host = ipaddress.IPv4Address(value['host'])
    require(str(host) == value['host'] and host.is_private and not (host.is_loopback or host.is_unspecified or
            host.is_link_local or host.is_multicast), 'SSH server address')
    require(isinstance(value['host_key_fingerprint'], str) and
            re.fullmatch(r'SHA256:[A-Za-z0-9+/]{43}', value['host_key_fingerprint']), 'SSH fingerprint')
    return value


def hostkey_diagnostic(request):
    """Independent Debian u10 source grammar, not a substring denial test."""
    path = '/fixture/inputs/known_hosts'
    host = '[' + request['host'] + ']:2222'
    records = ['@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@',
               '@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @',
               '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@',
               'IT IS POSSIBLE THAT SOMEONE IS DOING SOMETHING NASTY!',
               'Someone could be eavesdropping on you right now (man-in-the-middle attack)!',
               'It is also possible that a host key has just been changed.',
               'The fingerprint for the ED25519 key sent by the remote host is\n' + request['host_key_fingerprint'] + '.',
               'Please contact your system administrator.',
               'Add correct host key in ' + path + ' to get rid of this message.',
               'Offending ED25519 key in ' + path + ':1', '  remove with:',
               '  ssh-keygen -f "' + path + '" -R "' + host + '"',
               'Host key for ' + host + ' has changed and you have requested strict checking.',
               'Host key verification failed.']
    return ('\r\n'.join(records) + '\r\n').encode()


def transcript(raw, case, request, lower, upper):
    require(raw.endswith(b'\n') and raw.count(b'\n') == 1, 'SSH transcript framing')
    value = decode(raw)
    require(type(value) is dict and set(value) == RESULT_KEYS and
            raw == (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode(),
            'SSH transcript fields/canonical bytes')
    expected = {'declared': ('authenticated', 0, b'vz-ssh-response:' + request['token'].encode() + b'\n', b''),
                'undeclared': ('publickey_denied', 255, b'',
                    ('vzssh@' + request['host'] + ': Permission denied (publickey).\r\n').encode()),
                'wrong_host': ('hostkey_denied', 255, b'', hostkey_diagnostic(request))}
    require(case in expected, 'provider-omitted RUN must never emit a transcript')
    outcome, code, stdout, stderr = expected[case]
    require(type(value['schema_version']) is int and value['schema_version'] == 1 and value['type'] == 'ssh_result'
            and value['token'] == request['token'] and value['host'] == request['host']
            and type(value['port']) is int and value['port'] == 2222
            and value['mode'] == ('undeclared' if case == 'undeclared' else 'mounted')
            and value['outcome'] == outcome and type(value['ssh_exit_code']) is int and value['ssh_exit_code'] == code,
            'SSH transcript identity/outcome')
    for name, data in [('stdout', stdout), ('stderr', stderr)]:
        require(type(value[name + '_bytes']) is int and value[name + '_bytes'] == len(data)
                and value[name + '_sha256'] == sha(data), 'SSH exact public diagnostic/response differs')
    start, end = value['started_unix_ns'], value['completed_unix_ns']
    require(type(start) is int and type(end) is int and lower <= start < end <= upper
            and end - start <= 15 * 10**9, 'SSH transcript guest time/stale execution')
    return value


def _source(rows):
    current, finished, previous, terminal = None, False, None, None
    for row in rows:
        if 'started' not in row:
            require(current is None, 'SSH source reset')
            continue
        start = progress_ns(row['started'])
        if start != current:
            require(current is None or finished, 'SSH source abandoned lifetime')
            require(previous is None or previous <= start, 'SSH source overlap')
            current, finished = start, False
        if finished:
            require(row == terminal, 'SSH source changed terminal')
            continue
        if 'completed' in row:
            previous, finished, terminal = progress_ns(row['completed']), True, row
    require(finished, 'SSH source incomplete')


def _terminal(rows):
    done = [v for v in rows if 'completed' in v]
    require(len(done) == 1 and rows[-1] is done[0] and
            len({v['started'] for v in rows if 'started' in v}) == 1, 'SSH operation repeated/incomplete')
    require(all(v.get('cached', False) is False for v in rows), 'SSH operation cached')
    return done[0]


def ssh_progress(raw, *, reference, case, request, package_manifest, dockerfile,
                 guest_lower, guest_upper, host_lower, host_upper, secret_canaries=()):
    """Validate the exact solve graph and independently reconstruct public logs."""
    require(case in CASES and type(raw) is bytes and 0 < len(raw) <= MAX, 'SSH progress scope/bound')
    public_request(request)
    require(0 < guest_lower <= guest_upper and 0 < host_lower <= host_upper, 'SSH clock bounds')
    # Engine RepoDigests may omit Docker Hub's canonical namespace; BuildKit
    # renders the full name. Normalize names only, never the admitted digest or
    # the original command's exact build argument.
    base, separator, pin = reference.partition('@sha256:')
    require(separator and hex64(pin), 'SSH base must remain digest-pinned')
    if '/' not in base:
        base = 'docker.io/library/' + base
    elif not ('.' in base.split('/')[0] or ':' in base.split('/')[0] or base.startswith('localhost/')):
        base = 'docker.io/' + base
    reference = base + '@sha256:' + pin
    CanaryScanner(secret_canaries).feed(raw)
    batches, trailer = [], []
    for line in raw.splitlines():
        require(line, 'SSH empty progress line')
        if not line.startswith(b'{') or trailer:
            trailer.append(line.decode())
        else:
            batch = decode(line)
            scan_json(batch, secret_canaries)
            require(type(batch) is dict and set(batch) <= {'vertexes', 'statuses', 'logs', 'warnings'}
                    and all(type(v) is list and all(type(r) is dict for r in v) for v in batch.values())
                    and not batch.get('warnings'), 'SSH foreign progress/warning')
            batches.append(batch)
    require(len(batches) <= 20000, 'SSH progress batch bound')
    total = 6 if case == 'undeclared' else 7
    prefix = lambda number: '[build ' + str(number) + '/' + str(total) + '] '
    mode = 'undeclared' if case == 'undeclared' else 'mounted'
    run_line = ('RUN ' + ('' if mode == 'undeclared' else
                '--mount=type=ssh,id=fixture,required=true,target=/run/vz-build-ssh-agent ') +
                'python3 /fixture/ssh_probe.py ' + mode)
    names = {'base': prefix(1) + 'FROM ' + reference, 'context': '[internal] load build context',
             'packages': prefix(2) + 'COPY packages/ /fixture/packages/',
             'helpers': prefix(3) + 'COPY packages.py package-pins.json ssh_probe.py /fixture/',
             'extract': prefix(4) + 'RUN --network=none python3 /fixture/packages.py',
             'inputs': prefix(5) + 'COPY inputs/ /fixture/inputs/', 'run': prefix(6) + run_line,
             'absent': prefix(7) + 'RUN python3 /fixture/ssh_probe.py absent',
             'output': '[output 1/1] COPY --from=build /out/ssh.txt /ssh.txt',
             'export': OCI_EXPORT, 'cache_export': CACHE_EXPORT}
    if case == 'undeclared':
        del names['absent']
    auxiliary = {'[internal] load build definition from ' + ('Dockerfile.undeclared' if case == 'undeclared' else 'Dockerfile.ssh'),
                 '[internal] load .dockerignore', '[internal] load metadata for ' + reference}
    grouped, identities = {}, {}
    for batch in batches:
        for vertex in batch.get('vertexes', []):
            require(set(vertex) <= {'digest', 'name', 'inputs', 'started', 'completed', 'cached', 'error'}
                    and isinstance(vertex.get('digest'), str) and re.fullmatch(r'sha256:[0-9a-f]{64}', vertex['digest'])
                    and vertex.get('name') in set(names.values()) | auxiliary
                    and type(vertex.get('cached', False)) is bool and type(vertex.get('error', '')) is str,
                    'SSH unknown vertex/type')
            identity, name, edges = vertex['digest'], vertex['name'], vertex.get('inputs', [])
            require(type(edges) is list and all(isinstance(v, str) and re.fullmatch(r'sha256:[0-9a-f]{64}', v) for v in edges)
                    and len(set(edges)) == len(edges), 'SSH invalid graph edges')
            previous = grouped.setdefault(identity, [])
            require(not previous or (previous[0]['name'] == name and previous[0].get('inputs', []) == edges), 'SSH vertex drift')
            require(name not in identities or identities[name] == identity, 'SSH duplicated logical role')
            identities[name] = identity
            previous.append(vertex)
            for key in ('started', 'completed'):
                if key in vertex:
                    require(host_lower <= progress_ns(vertex[key]) <= host_upper, 'SSH shifted progress outside client clocks')
            if 'completed' in vertex:
                require('started' in vertex and progress_ns(vertex['started']) <= progress_ns(vertex['completed']),
                        'SSH reversed vertex lifetime')
    required = {'base', 'context', 'packages', 'helpers', 'extract', 'inputs', 'run'}
    if case == 'declared':
        required |= {'absent', 'output', 'export', 'cache_export'}
    require(all(names[role] in identities for role in required), 'SSH missing required graph role')
    ids = {role: identities[name] for role, name in names.items() if name in identities}
    edges = {'base': [], 'context': [], 'packages': ['base', 'context'], 'helpers': ['packages', 'context'],
             'extract': ['helpers'], 'inputs': ['extract', 'context'], 'run': ['inputs'], 'absent': ['run'],
             'output': ['absent' if case == 'declared' else 'run'], 'export': [], 'cache_export': []}
    terminal = {}
    error = MISSING if case == 'provider_omitted' else (
        'process "/bin/sh -c python3 /fixture/ssh_probe.py ' + mode + '" did not complete successfully: exit code: ' +
        ('41' if case == 'undeclared' else '42'))
    for role, identity in ids.items():
        records = grouped[identity]
        if role not in required:
            require(all(not any(k in r for k in ('started', 'completed', 'error', 'cached')) for r in records),
                    'failed SSH solve executed downstream operation')
            continue
        require(records[0].get('inputs', []) == [ids[r] for r in edges[role]], 'SSH disconnected graph')
        if role in ('base', 'context'):
            _source(records)
            done = records[-1]
        else:
            done = _terminal(records)
        require(done.get('error', '') == (error if role == 'run' and case != 'declared' else ''), 'SSH unrelated/missing vertex failure')
        require(all(not r.get('error') for r in records[:-1]), 'SSH error before terminal')
        terminal[role] = done
        for dependency in edges[role]:
            require(progress_ns(terminal[dependency]['completed']) <= progress_ns(done['started']), 'SSH dependency clock reversed')
    for name in auxiliary & set(identities):
        records = grouped[identities[name]]
        require(not records[0].get('inputs') and all(not r.get('error') for r in records), 'SSH auxiliary failure/dependency')
        _source(records)
    log_parts = {role: [] for role in ('extract', 'run', 'absent')}
    for batch in batches:
        for category in ('logs', 'statuses'):
            for row in batch.get(category, []):
                identity = row.get('vertex')
                require(identity in grouped, 'SSH foreign progress frame')
                for key in ('timestamp', 'started', 'completed'):
                    if key in row:
                        require(host_lower <= progress_ns(row[key]) <= host_upper, 'SSH frame outside client time')
                if category == 'logs':
                    role = next((r for r in log_parts if ids.get(r) == identity), None)
                    require(role is not None and role in terminal and set(row) <= {'vertex', 'stream', 'data', 'timestamp'}
                            and type(row.get('stream')) is int and row['stream'] == 1
                            and progress_ns(terminal[role]['started']) <= progress_ns(row['timestamp']) <= progress_ns(terminal[role]['completed']),
                            'SSH log ownership/stream/time')
                    part = base64.b64decode(row['data'], validate=True)
                    log_parts[role].append(part)
                else:
                    require(set(row) <= {'id', 'vertex', 'name', 'current', 'total', 'timestamp', 'started', 'completed'}
                            and isinstance(row.get('id'), str) and 0 < len(row['id']) <= 4096
                            and ('name' not in row or isinstance(row['name'], str)), 'SSH unknown status fields')
                    for key in ('current', 'total'):
                        require(key not in row or type(row[key]) is int and row[key] >= 0, 'SSH invalid status counter')
                    require(any('started' in v and 'completed' in v and all(
                        progress_ns(v['started']) <= progress_ns(row[key]) <= progress_ns(v['completed'])
                        for key in ('started', 'completed', 'timestamp') if key in row)
                        for v in grouped[identity]), 'SSH status outside vertex lifetime')
    logs = {role: b''.join(parts) for role, parts in log_parts.items()}
    for value in logs.values():
        CanaryScanner(secret_canaries).feed(value)
        if value:
            scan_json(decode(value), secret_canaries)
    package_row = {'schema_version': 1, 'type': 'openssh_packages_extracted', 'package_pins_sha256': sha(package_manifest),
                   'packages': [r['package'] for r in decode(package_manifest)['packages']], 'maintainer_scripts_executed': False}
    require(logs['extract'] == (json.dumps(package_row, sort_keys=True, separators=(',', ':')) + '\n').encode(),
            'SSH package extraction execution proof differs')
    run = terminal['run']; duration = progress_ns(run['completed']) - progress_ns(run['started'])
    envelope, result, absence = None, None, None
    if case == 'provider_omitted':
        require(logs['run'] == logs['absent'] == b'', 'SSH missing-provider probe executed')
    else:
        result = transcript(logs['run'], case, request, guest_lower, guest_upper)
        start, end = result['started_unix_ns'], result['completed_unix_ns']
        require(0 < end - start <= duration, 'SSH guest script exceeds preserved RUN duration')
        envelope = {'started_ns': end - duration, 'completed_ns': start + duration}
        require(guest_lower <= envelope['started_ns'] < envelope['completed_ns'] <= guest_upper,
                'SSH guest RUN envelope outside Engine clocks')
    if case == 'declared':
        absence = decode(logs['absent'])
        require(type(absence) is dict and set(absence) == {'schema_version', 'type', 'token', 'agent_path_absent', 'agent_environment_absent', 'unix_ns'}
                and type(absence['schema_version']) is int and absence['schema_version'] == 1
                and absence['type'] == 'ssh_mount_absent' and absence['token'] == request['token']
                and absence['agent_path_absent'] is True and absence['agent_environment_absent'] is True
                and type(absence['unix_ns']) is int and result['completed_unix_ns'] <= absence['unix_ns'] <= guest_upper
                and logs['absent'] == (json.dumps(absence, sort_keys=True, separators=(',', ':')) + '\n').encode(),
                'SSH next RUN mount-absence proof differs')
        absent_duration = progress_ns(terminal['absent']['completed']) - progress_ns(terminal['absent']['started'])
        require(absent_duration > 0 and guest_lower <= absence['unix_ns'] - absent_duration
                and absence['unix_ns'] + absent_duration <= guest_upper,
                'SSH absence RUN envelope outside Engine clocks')
        require(not trailer, 'successful SSH solve has failure trailer')
        require(progress_ns(terminal['output']['completed']) <= progress_ns(terminal['export']['started']), 'SSH export before output')
    else:
        lines = dockerfile.decode().splitlines()
        numbers = [i for i, line in enumerate(lines, 1) if line == run_line]
        require(len(numbers) == 1, 'SSH Dockerfile failure line ambiguous')
        number = numbers[0]; filename = 'Dockerfile.undeclared' if case == 'undeclared' else 'Dockerfile.ssh'
        footer = 'ERROR: failed to build: failed to solve: ' + error
        excerpt = [filename + ':' + str(number), '--------------------']
        for n in range(max(1, number - 2), min(len(lines), number + 2) + 1):
            excerpt.append(f'{n:4} | ' + ('>>> ' if n == number else '    ') + lines[n - 1])
        excerpt += ['--------------------', footer]
        require(trailer in ([footer], excerpt), 'SSH arbitrary/foreign failure trailer')
        require(not logs['absent'], 'failed SSH solve ran mount-absence stage')
    return {'case': case, 'progress_clock': 'buildx-client-translated', 'progress_sha256': sha(raw),
            'run_digest': ids['run'], 'raw_run_duration_ns': duration, 'guest_run_envelope': envelope,
            'ssh_result': result, 'mount_absence': absence, 'package_execution': package_row}


def build_argv(inputs, operation):
    op = operation
    filename = 'Dockerfile.undeclared' if op['case'] == 'undeclared' else 'Dockerfile.ssh'
    args = ['buildx', 'build', '--builder', inputs['builder']['name'], '--platform', 'linux/arm64',
            '--progress', 'rawjson', '--file', str(Path(op['build_context']) / filename),
            '--provenance=false', '--sbom=false', '--output', 'type=oci,dest=' + op['output'] + OCI_OPTIONS,
            '--build-arg', 'FIXTURE_BASE=' + inputs['images']['base']['reference'], '--no-cache', '--network=default']
    if op['case'] == 'declared':
        args += ['--cache-to', 'type=local,dest=' + op['cache_output'] + CACHE_OPTIONS]
    if op['case'] != 'provider_omitted':
        args += ['--ssh', 'fixture=' + op['agent_socket']]
    return args + [op['build_context']]


def commands(directory, inputs, case, require_ack, canaries):
    rows, previous_end = [], 0
    for index in range(1, 10):
        stem = directory / f'command-{index:05d}'
        terminal_bytes = read(Path(str(stem) + '.json'))
        row, intent = decode(terminal_bytes), decode(read(Path(str(stem) + '.intent.json')))
        scan_json(row, canaries); scan_json(intent, canaries)
        failure = index == 5 and case != 'declared'
        require(type(row['index']) is int and row['index'] == index and row['argv0'] == 'docker' and
                row['argv'][:5] == ['docker', '--config', inputs['docker_config'], '--context', inputs['scope']['docker_context']]
                and row['executable'] == inputs['clients']['docker']['path'], 'SSH command routing')
        require(row['host_outcome'] == 'exited' and row['capture_complete'] is True and row['raw_streams_retained'] is True
                and all(row[k] is False for k in ('timed_out', 'interrupted', 'output_limit_exceeded', 'secret_leak_detected'))
                and row['dispatch_error'] is None and type(row['exit_code']) is int and row['exit_code'] == int(failure)
                and row['effects_uncertain'] is failure and row['mutation'] is (index == 5), 'SSH command incomplete/wrong outcome')
        require(type(row['started_unix_ns']) is int and row['started_unix_ns'] >= previous_end and
                type(row['elapsed_ns']) is int and 0 <= row['elapsed_ns'] <= 310 * 10**9, 'SSH command chronology/bounds')
        previous_end = row['started_unix_ns'] + row['elapsed_ns']
        require(intent['host_outcome'] == 'inflight' and intent['effects_uncertain'] is True and intent['exit_code'] is None
                and intent['timed_out'] is False and intent['interrupted'] is False, 'SSH missing dispatch intent')
        for key in ('index', 'executable', 'argv', 'argv0', 'environment', 'started_unix_ns', 'mutation', 'max_stream_bytes'):
            require(intent[key] == row[key], 'SSH intent/terminal drift')
        require(type(row['max_stream_bytes']) is int and 0 < row['max_stream_bytes'] <= MAX, 'SSH stream bound')
        for name in ('stdout', 'stderr'):
            require(row[name] == stem.name + '.' + name, 'SSH redirected stream')
            content = read(directory / row[name]); CanaryScanner(canaries).feed(content)
            require(len(content) <= row['max_stream_bytes'] and type(row['observed_bytes'][name]) is int and
                    row['observed_bytes'][name] == row['retained_observed_' + name + '_bytes'] == len(content)
                    and all(row[k] == sha(content) for k in (name + '_sha256', 'raw_' + name + '_sha256',
                                                          'retained_observed_' + name + '_sha256')), 'SSH stream content binding')
            row['_' + name] = content
        require(index == 5 or not row['_stderr'], 'SSH inspection emitted unexpected diagnostics')
        acknowledgement = Path(str(stem) + '.acknowledgement.json')
        if failure and (require_ack or acknowledgement.exists()):
            ack = decode(read(acknowledgement))
            require(type(ack) is dict and set(ack) == {'command_index', 'assertion', 'terminal_receipt_sha256', 'effects_uncertain'}
                    and type(ack['command_index']) is int and ack['command_index'] == 5 and ack['effects_uncertain'] is False
                    and ack['assertion'] == 'terminal BuildKit SSH fixture ' + case + ' denial'
                    and ack['terminal_receipt_sha256'] == sha(terminal_bytes), 'SSH negative acknowledgement unbound')
        row['_args'] = row['argv'][5:]
        rows.append(row)
    return rows


class Replay(BuildReplay):
    def __init__(self, directory, inputs, operation, canaries, require_ack):
        directory = absolute(str(directory))
        require(directory.is_dir() and stat.S_IMODE(directory.stat().st_mode) == 0o700, 'SSH private evidence directory')
        require(type(operation) is dict and set(operation) == OP_KEYS and type(operation['schema_version']) is int
                and operation['schema_version'] == 1 and operation['case'] in CASES, 'SSH operation schema')
        runtime_proof(inputs)
        self.directory, self.inputs, self.operation = directory, inputs, operation
        self.receipt_snapshot = {p.name: sha(read(p)) for p in directory.iterdir() if p.is_file()}
        self.scope, self.builder, self.canaries = inputs['scope'], inputs['builder'], tuple(canaries)
        require(set(self.builder) == {'name', 'node', 'container_id', 'image_id'} and hex64(self.builder['container_id'])
                and re.fullmatch(r'sha256:[0-9a-f]{64}', self.builder['image_id']), 'SSH builder identity')
        require(operation['run_id'] == inputs['run_id'] and isinstance(operation['run_id'], str)
                and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', operation['run_id']), 'SSH run identity')
        for filename, expected in [('inputs.json', inputs), ('operation.intent.json', operation), ('operation.json', operation)]:
            require(json.dumps(decode(read(directory / filename)), sort_keys=True) == json.dumps(expected, sort_keys=True),
                    'SSH persisted operation/input differs')
        for path in ('ssh_fixture', 'build_context', 'output'):
            absolute(operation[path])
        require(operation['output'] == str(directory / 'oci'), 'SSH output path')
        if operation['case'] == 'declared':
            require(operation['cache_output'] == str(directory / 'cache'), 'SSH cache output path')
        else:
            require(operation['cache_output'] is None, 'SSH failed recipe cache export')
        if operation['case'] == 'provider_omitted':
            require(operation['agent_socket'] is None, 'SSH omitted provider supplied')
        else:
            absolute(operation['agent_socket'])
        self.rows = commands(directory, inputs, operation['case'], require_ack, canaries)
        self.i, self.acknowledged, self.builder_process = 0, set(), None
        expected = {'inputs.json', 'operation.intent.json', 'operation.json', 'artifact-validation.json', 'compose-owner.json', 'private-tmp'}
        expected |= {f'command-{i:05d}{ext}' for i in range(1, 10) for ext in ('.json', '.intent.json', '.stdout', '.stderr')}
        if operation['case'] != 'declared':
            ack = 'command-00005.acknowledgement.json'
            if require_ack or (directory / ack).exists():
                expected.add(ack)
        for name in ('oci', 'cache'):
            if (directory / name).exists():
                expected.add(name)
        require({p.name for p in directory.iterdir()} == expected, 'SSH unexpected operation inventory')
        for path in directory.iterdir():
            require(not path.is_symlink() and (path.is_dir() if path.name in {'private-tmp', 'oci', 'cache'} else path.is_file()),
                    'SSH redirected operation path')
            if path.is_file():
                CanaryScanner(canaries).feed(read(path))
        require(not list((directory / 'private-tmp').iterdir()), 'SSH observer temporary files retained')

    def run(self):
        op = self.operation; fixture, context = Path(op['ssh_fixture']), Path(op['build_context'])
        require(hex64(op['ssh_fixture_sha256']) and fixture_digest(fixture) == op['ssh_fixture_sha256']
                and hex64(op['build_context_sha256']) and fixture_digest(context) == op['build_context_sha256'],
                'SSH fixture/context digest differs')
        public_request(op['request'])
        require({p.name for p in context.iterdir()} == {p.name for p in fixture.iterdir()} | {'inputs', 'packages'}, 'SSH context extra input')
        for path in fixture.iterdir():
            require(path.is_file() and not path.is_symlink() and read(context / path.name) == read(path), 'SSH staged fixture source differs')
        require({p.name for p in (context / 'inputs').iterdir()} == {'request.json', 'known_hosts'}
                and decode(read(context / 'inputs/request.json')) == op['request'], 'SSH staged public request')
        hosts = read(context / 'inputs/known_hosts', 256)
        require(sha(hosts) == op['known_hosts_sha256'], 'SSH known-host pin differs')
        prefix = ('[' + op['request']['host'] + ']:2222 ssh-ed25519 ').encode()
        require(hosts.startswith(prefix) and hosts.endswith(b'\n'), 'SSH known-host record shape')
        wire = base64.b64decode(hosts[len(prefix):-1], validate=True)
        require(len(wire) == 51 and wire[:19] == b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20'
                and base64.b64encode(wire) == hosts[len(prefix):-1], 'SSH known-host key encoding')
        fingerprint = 'SHA256:' + base64.b64encode(bytes.fromhex(sha(wire))).decode().rstrip('=')
        require((fingerprint == op['request']['host_key_fingerprint']) is (op['case'] != 'wrong_host'), 'SSH correct/wrong host-key contract')
        pins = read(fixture / 'package-pins.json', 16384); manifest = decode(pins)
        require(decode(read(context / 'packages/manifest.json', 16384)) == manifest, 'SSH package manifest differs')
        require({p.name for p in (context / 'packages').iterdir()} == {'manifest.json'} | {r['filename'] for r in manifest['packages']},
                'SSH package inventory differs')
        for row in manifest['packages']:
            data = read(context / 'packages' / row['filename'])
            require(len(data) == row['size'] and sha(data) == row['sha256'], 'SSH package bytes differ')
        self.builder_guard(); lower = self.build_engine_ns
        argv = build_argv(self.inputs, op)
        require(op['build_argv'] == argv, 'SSH operation argv differs')
        build = self.take(argv, code=0 if op['case'] == 'declared' else 1, mutation=True)
        require(not build['_stdout'], 'SSH unexpected build stdout')
        self.builder_guard(); upper = self.build_engine_ns
        require(self.i == 9, 'SSH skipped command')
        before, after = (decode(self.rows[i]['_stdout'])[0] for i in (3, 8))
        require(all(before.get(k) == after.get(k) for k in ('Config', 'HostConfig', 'Mounts'))
                and before['State'].get('OOMKilled') is False and after['State'].get('OOMKilled') is False,
                'SSH builder lifetime/configuration changed')
        filename = 'Dockerfile.undeclared' if op['case'] == 'undeclared' else 'Dockerfile.ssh'
        graph = ssh_progress(build['_stderr'], reference=self.inputs['images']['base']['reference'], case=op['case'],
                             request=op['request'], package_manifest=pins, dockerfile=read(fixture / filename),
                             guest_lower=lower, guest_upper=upper, host_lower=build['started_unix_ns'],
                             host_upper=build['started_unix_ns'] + build['elapsed_ns'], secret_canaries=self.canaries)
        image, cache = None, None
        if op['case'] == 'declared':
            payload = ('vz-ssh-response:' + op['request']['token'] + '\n').encode()
            image = layout.validate_oci(self.directory / 'oci', expected_path='ssh.txt', expected_sha256=sha(payload),
                                        expected_size=len(payload), canaries=self.canaries)
            cache = layout.validate_cache(self.directory / 'cache', canaries=self.canaries)
            export_steps(build['_stderr'], image, cache)
        else:
            for name in ('oci', 'cache'):
                path = self.directory / name
                require(not path.exists() or not list(path.iterdir()), 'SSH failed solve exported bytes')
        require(decode(read(self.directory / 'artifact-validation.json')) == {'oci': image, 'cache': cache},
                'SSH artifact proof differs')
        require(fixture_digest(fixture) == op['ssh_fixture_sha256'] and fixture_digest(context) == op['build_context_sha256'],
                'SSH source changed during replay')
        require({p.name: sha(read(p)) for p in self.directory.iterdir() if p.is_file()} == self.receipt_snapshot,
                'SSH receipt files changed during replay')
        return {'schema_version': 1, 'case': op['case'], 'run_id': op['run_id'], 'scope': self.scope, 'builder': self.builder,
                'builder_process': {'pid': self.builder_process[0], 'started_at': self.builder_process[1]},
                'command_count': 9, 'ssh_fixture_sha256': op['ssh_fixture_sha256'], 'build_context_sha256': op['build_context_sha256'],
                'graph': graph, 'oci': image, 'cache': cache, 'parent_provisioning_and_cleanup_required': True,
                'compatibility_certified': False}


def validate_operation(directory, expected_inputs, expected_operation, *, secret_canaries=(), require_ack=True):
    try:
        require(type(require_ack) is bool, 'SSH acknowledgement mode')
        return Replay(directory, expected_inputs, expected_operation, secret_canaries, require_ack).run()
    except Invalid as error:
        raise Invalid('SSH operation evidence rejected: ' + str(error)) from error
    except (OSError, ValueError, KeyError, IndexError, TypeError, UnicodeError) as error:
        raise Invalid('SSH operation evidence rejected: ' + type(error).__name__) from error
