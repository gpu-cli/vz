"""Pure, bounded DEV container state evidence; not host replay or parity proof.

Callers authenticate Engine/Machine identity and command time windows separately.
Numeric exit codes do not certify signal delivery. No function launches Docker
or authorizes cleanup. Original inspect/event rows are returned without erasure.
"""
import copy
import re

from linux_docker_container_fixture import encode, parse, require, token as check_token
from linux_docker_buildkit_shutdown import timestamp

LABEL = 'dev.vz.container-io'
ZERO = '0001-01-01T00:00:00Z'
EVENT_FORMAT = ('{"type":{{json .Type}},"action":{{json .Action}},'
                '"id":{{json .Actor.ID}},"attributes":{{json .Actor.Attributes}},'
                '"scope":{{json .Scope}},"time_nano":{{json .TimeNano}}}')
HEALTH_INTERVAL = 1000000000
HEALTH_TIMEOUT = 1000000000
HEALTH_START_PERIOD = 30000000000
ENTRYPOINT = ['python3', '-u', '/fixture/probe.py']


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def object_at(row, key):
    value = row.get(key)
    require(type(value) is dict, 'missing object: ' + key)
    return value


def health_command(token):
    return ['CMD-SHELL', 'python3 -u /fixture/probe.py health ' + check_token(token)]


def inspect_container(raw, *, cid, name, image_id, token, command, state,
                      interactive=False, tty=False, health=False, entrypoint=ENTRYPOINT):
    """Admit one exact owned inspect row; allow unrelated Docker metadata fields."""
    check_token(token)
    require(type(cid) is str and re.fullmatch('[0-9a-f]{64}', cid) and
            type(image_id) is str and re.fullmatch('sha256:[0-9a-f]{64}', image_id) and
            type(name) is str and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}', name),
            'invalid expected container identity')
    require(type(command) is list and command and all(type(x) is str and x and '\0' not in x for x in command),
            'invalid expected command')
    require(entrypoint is None or (type(entrypoint) is list and all(type(x) is str and x and '\0' not in x for x in entrypoint)),
            'invalid expected entrypoint')
    require(all(type(x) is bool for x in (interactive, tty, health)) and state in ('created', 'running', 'exited'),
            'invalid expected flags/state')
    rows = parse(raw)
    require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict, 'inspect must contain exactly one container')
    row = rows[0]
    require(row.get('Id') == cid and row.get('Name') == '/' + name and row.get('Image') == image_id,
            'container identity differs')
    created = timestamp(row.get('Created'))
    config, host, status = [object_at(row, k) for k in ('Config', 'HostConfig', 'State')]
    require(config.get('Labels') == {LABEL: token} and config.get('Cmd') == command and
            encode(config.get('Entrypoint')) == encode(entrypoint) and 'Entrypoint' in config and
            config.get('User') in ('', '0', '0:0') and config.get('WorkingDir') == '/workspace' and
            config.get('Tty') is tty and config.get('OpenStdin') is interactive,
            'container command/ownership/interactive configuration differs')
    process = (entrypoint or []) + command
    require(row.get('Path') == process[0] and row.get('Args') == process[1:], 'resolved process differs')
    require(host.get('Runtime') == 'youki' and host.get('NetworkMode') == 'none' and
            host.get('Privileged') is False and host.get('PublishAllPorts') is False and
            host.get('Binds') in (None, []) and host.get('Mounts') in (None, []) and
            host.get('VolumesFrom') in (None, []) and host.get('Devices') in (None, []) and
            host.get('CapAdd') in (None, []) and host.get('PortBindings') in (None, {}) and
            host.get('PidMode') == '' and host.get('IpcMode') == 'private' and
            row.get('Mounts') == [] and config.get('Volumes') in (None, {}),
            'container isolation/mount configuration differs')
    require(encode(host.get('RestartPolicy')) == encode({'Name': 'no', 'MaximumRetryCount': 0}) and
            integer(row.get('RestartCount')) and row['RestartCount'] == 0, 'restart policy/count differs')
    require(status.get('Status') == state and status.get('Running') is (state == 'running') and
            all(status.get(k) is False for k in ('Paused', 'Restarting', 'Dead', 'OOMKilled')) and
            status.get('Error') == '' and integer(status.get('Pid')) and integer(status.get('ExitCode')),
            'incoherent container process state')
    started, finished = status.get('StartedAt'), status.get('FinishedAt')
    if state == 'created':
        require(status['Pid'] == 0 and started == finished == ZERO and status['ExitCode'] == 0,
                'created container already executed')
    else:
        start = timestamp(started)
        require(start >= created, 'process predates container creation')
        if state == 'running':
            require(status['Pid'] > 0 and status['ExitCode'] == 0 and
                    (finished == ZERO or timestamp(finished) <= start), 'running generation differs')
        else:
            require(status['Pid'] == 0 and timestamp(finished) >= start, 'exited process not retired')
    if health:
        hc = object_at(config, 'Healthcheck')
        require(hc.get('Test') == health_command(token) and
                type(hc.get('Retries')) is int and hc['Retries'] == 1 and
                all(type(hc.get(k)) is int and hc[k] == expected for k, expected in
                    (('Interval', HEALTH_INTERVAL), ('Timeout', HEALTH_TIMEOUT), ('StartPeriod', HEALTH_START_PERIOD))),
                'health probe configuration differs')
        require(set(hc) <= {'Test', 'Retries', 'Interval', 'Timeout', 'StartPeriod', 'StartInterval'} and
                type(hc.get('StartInterval', 0)) is int and hc.get('StartInterval', 0) in (0, HEALTH_INTERVAL),
                'unexpected health configuration')
    else:
        require(config.get('Healthcheck') is None and status.get('Health') is None, 'unexpected health probe')
    return row


def same_identity(before, after, *, start_policy=None, engine_id=None, start_acknowledged=False):
    """Compare previously admitted rows, including every configuration field."""
    keys = ('Id', 'Name', 'Image', 'Created', 'Path', 'Args', 'Config', 'HostConfig', 'Mounts')
    require(all(k in before and k in after for k in keys), 'missing identity/configuration')
    if encode([before[k] for k in keys]) != encode([after[k] for k in keys]):
        require(start_acknowledged is True and before['State']['Status'] == 'created' and
                after['State']['Status'] == 'running' and after['State']['Running'] is True and
                integer(after['State']['Pid'], 1) and type(engine_id) is str and engine_id and
                type(start_policy) is dict and set(start_policy) == {'ID', 'ServerVersion', 'CgroupVersion', 'OomKillDisable'} and
                start_policy['ID'] == engine_id and start_policy['ServerVersion'] == '29.7.2' and
                start_policy['CgroupVersion'] == '2' and start_policy['OomKillDisable'] is False and
                'OomKillDisable' in before['HostConfig'] and before['HostConfig']['OomKillDisable'] is False and
                'OomKillDisable' in after['HostConfig'] and after['HostConfig']['OomKillDisable'] is None,
                'unapproved start configuration transition')
        expected = copy.deepcopy(before)
        expected['HostConfig']['OomKillDisable'] = None
        require(encode([expected[k] for k in keys]) == encode([after[k] for k in keys]),
                'additional start configuration drift')
    return True


def same_generation(before, after):
    same_identity(before, after)
    a, b = before['State'], after['State']
    require(a['Status'] == b['Status'] == 'running' and a['Running'] is b['Running'] is True and
            integer(a['Pid'], 1) and a['Pid'] == b['Pid'] and a['StartedAt'] == b['StartedAt'],
            'running process generation changed')
    return True


def new_generation(before, after):
    same_identity(before, after)
    a, b = before['State'], after['State']
    require(a['Status'] in ('running', 'exited') and b['Status'] == 'running' and
            b['Running'] is True and integer(b['Pid'], 1) and
            timestamp(b['StartedAt']) > timestamp(a['StartedAt']), 'process did not restart')
    # A recycled numeric PID is legal; StartedAt distinguishes incarnations.
    return True


def stopped(row, exit_code):
    require(type(exit_code) is int and exit_code in (0, 37, 130, 137, 143, 126, 127), 'unexpected exit expectation')
    state = row['State']
    require(state['Status'] == 'exited' and state['Running'] is False and type(state['Pid']) is int and
            state['Pid'] == 0 and type(state['ExitCode']) is int and state['ExitCode'] == exit_code and
            timestamp(state['FinishedAt']) >= timestamp(state['StartedAt']), 'stopped state/exit differs')
    return {'exit_code': exit_code, 'signal_delivery_certified': False, 'host_replay_required': True,
            'youki_process_inventory_required': True}


def health_transition(samples, token):
    """Previously admitted running inspect samples, including intermediate polls."""
    check_token(token)
    require(type(samples) is list and 3 <= len(samples) <= 120, 'unbounded/missing health samples')
    phases, previous_end, proven = [], 0, set()
    for row in samples:
        same_generation(samples[0], row)
        require(row['Config']['Healthcheck']['Test'] == health_command(token), 'foreign health probe')
        health = object_at(row['State'], 'Health')
        require(set(health) == {'Status', 'FailingStreak', 'Log'} and health['Status'] in ('starting', 'healthy', 'unhealthy') and
                integer(health['FailingStreak']), 'malformed health state')
        logs = health['Log']
        require(type(logs) is list and len(logs) <= 5, 'health log bound differs')
        end, exits = 0, []
        for log in logs:
            require(type(log) is dict and set(log) == {'Start', 'End', 'ExitCode', 'Output'} and
                    type(log['ExitCode']) is int and log['ExitCode'] in (0, 1) and log['Output'] == '',
                    'unexpected health execution output')
            start, finish = timestamp(log['Start']), timestamp(log['End'])
            require(timestamp(row['State']['StartedAt']) <= start <= finish and start >= end,
                    'health log timestamp order differs')
            end = finish
            exits.append(log['ExitCode'])
        if end:
            require(end >= previous_end, 'health history went backwards')
            previous_end = end
        status = health['Status']
        expected = 0 if status == 'healthy' else 1
        require((logs and exits[-1] == expected) or (not logs and status == 'starting' and not previous_end),
                'health status lacks actual matching probe')
        if logs:
            proven.add(status)
        require((health['FailingStreak'] == 0) if status != 'unhealthy' else (health['FailingStreak'] >= 1),
                'health failing streak differs')
        if not phases or phases[-1] != status:
            phases.append(status)
    require(phases == ['starting', 'healthy', 'unhealthy'] and proven == set(phases),
            'health transition sequence or actual probes differ')
    return {'transitions': phases, 'samples': len(samples), 'host_replay_required': True}


def validate_events(raw, *, cid, name, image, token, since, until,
                    required=('create', 'start', 'die', 'destroy')):
    """Read explicit EVENT_FORMAT NDJSON, retaining all authenticated extras."""
    check_token(token)
    require(type(raw) is bytes and 0 < len(raw) <= 65536 and raw.endswith(b'\n') and
            integer(since, 1) and integer(until, since), 'invalid event bytes/window')
    lines = raw.splitlines()
    require(len(lines) <= 256 and all(lines), 'event record bound differs')
    rows, seen, lifecycle, running, previous = [], set(), [], False, since
    for line in lines:
        row = parse(line)
        require(type(row) is dict and set(row) == {'type', 'action', 'id', 'attributes', 'scope', 'time_nano'} and
                row['type'] == 'container' and row['scope'] == 'local' and row['id'] == cid,
                'foreign or malformed event')
        attrs = row['attributes']
        require(type(attrs) is dict and len(attrs) <= 32 and all(type(k) is str and type(v) is str and
                len(k) <= 128 and len(v) <= 4096 for k, v in attrs.items()) and
                attrs.get(LABEL) == token and attrs.get('name') == name and attrs.get('image') == image,
                'event ownership attributes differ')
        when = row['time_nano']
        require(integer(when) and previous <= when <= until and encode(row) not in seen,
                'duplicate, reordered or stale event')
        previous = when
        seen.add(encode(row))
        action = row['action']
        require(type(action) is str and (action in ('create', 'start', 'die', 'destroy', 'restart', 'kill', 'stop',
                'attach', 'resize', 'exec_die') or action in ('health_status: healthy', 'health_status: unhealthy') or
                action.startswith('exec_create: ') or action.startswith('exec_start: ')), 'unknown container event')
        if action == 'create':
            require(not lifecycle, 'duplicate/late create event')
            lifecycle.append(action)
        elif action == 'start':
            require(lifecycle and lifecycle[-1] in ('create', 'die') and not running, 'start order differs')
            running = True
            lifecycle.append(action)
        elif action == 'die':
            require(running and re.fullmatch(r'0|[1-9][0-9]{0,2}', attrs.get('exitCode', '')) and
                    int(attrs['exitCode']) <= 255, 'die lacks running generation/exit code')
            running = False
            lifecycle.append(action)
        elif action == 'destroy':
            require(lifecycle and lifecycle[-1] == 'die' and not running, 'destroy precedes process retirement')
            lifecycle.append(action)
        else:
            require(lifecycle and lifecycle[-1] != 'destroy', 'event outside owned lifecycle')
        rows.append(row)
    require(lifecycle and lifecycle[0] == 'create' and lifecycle[-1] == 'destroy', 'incomplete owned lifecycle')
    require(type(required) in (list, tuple) and tuple(required) == ('create', 'start', 'die', 'destroy'),
            'unsupported required lifecycle')
    cursor = 0
    for action in lifecycle:
        if cursor < len(required) and action == required[cursor]:
            cursor += 1
    require(cursor == len(required), 'required ordered events missing')
    return {'events': rows, 'lifecycle': lifecycle, 'host_replay_required': True,
            'signal_delivery_certified': False}
