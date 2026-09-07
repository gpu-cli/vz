"""Installed-Machine adapter for the DEV resource-limit and OOM recipe.

Covers `docker.operation.resource_limits` and `docker.operation.oom` on one
authenticated Developer Linux Machine through its private `--config`/`--context`
route. The workload image is the harness sentinel's digest-pinned developer
probe rootfs (BusyBox, imported by `image import`); nothing is pulled. Effective
limits are read inside each container from its private cgroup v2 root through
ordinary `docker exec`. The OOM subject is the checked-in allocator fixture whose
bytes are pinned here; a sibling HTTP health service from
`linux_docker_parallel_health` is probed every second for sixty seconds while it
runs. The developer kernel builds `CONFIG_SWAP=n` (linux/vz-linux.config), so
`memory.swap.max` does not exist and `--memory-swap` is not requested.

No exception path removes any container: a failed session stays registered on
`harness.limits_sessions` with `cleanup_complete` False, and its containers still
reference the sentinel image so the harness's own image removal fails closed.
Nothing here certifies a release scenario.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid

import docker_host_driver as driver
import installed_developer_startup as startup
import linux_docker_parallel_health as parallel_health

require = driver.require
LIMIT = 8 * 1024 * 1024
SCOPE = 'DEV_installed_Machine_resource_limits_oom_not_release_certification'
REPO = Path(__file__).resolve().parents[2]
HELPERS = Path(__file__).resolve().parent
FIXTURE = REPO / 'tests/fixtures/vz-0.4/docker-limits'
LABEL = 'dev.vz.linux-compose-proof'
ALLOCATOR_SHA256 = '7f65d83d8b73c2f69ea5f9395db1b67bfa5d9042af7a7c48a7d9abe69bd01192'
ALLOCATOR_BYTES = 841
MEMORY_BYTES = 1073741824
CPUS = '1'
PIDS_LIMIT = 64
SLEEP_SECONDS = 600
CGROUP_FILES = ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/cpu.max', '/sys/fs/cgroup/pids.max')
LIMITED = {'memory.max': '1073741824', 'cpu.max': '100000 100000', 'pids.max': '64'}
CONTROL = {'memory.max': 'max', 'cpu.max': 'max 100000', 'pids.max': 'max'}
OOM_EXIT_CODE = 137
SURVIVOR_EXIT_CODE = 61
EVENTS = ('create', 'start', 'oom', 'die')
EVENT_MARGIN_NS = 5_000_000_000
HEALTH = {'sibling_health_probe_seconds': 60, 'sibling_health_probe_interval_seconds': 1, 'sibling_health_failures': 0}
BUSYBOX = '/bin/busybox'


def required_source_paths():
    return [str(HELPERS / name) for name in (
        'linux_docker_limits_machine.py', 'linux_docker_parallel_health.py',
        'docker_host_driver.py', 'installed_developer_startup.py', 'linux_docker_e2e.py')] + [
        str(FIXTURE / 'allocate.sh'), str(FIXTURE / 'fixture.json')]


def verify_sources(pins):
    require(type(pins) is dict and set(pins) == set(required_source_paths()), 'exact limits source pins required')
    for name, digest in pins.items():
        require(type(digest) is str and re.fullmatch('[0-9a-f]{64}', digest) and
                driver.sha256(driver.regular(Path(name), LIMIT)) == digest, 'limits source changed: ' + name)


def unique(pairs):
    row = {}
    for key, value in pairs:
        require(key not in row, 'duplicate JSON field')
        row[key] = value
    return row


def parse(raw):
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT, 'bounded JSON stream required')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
    except (UnicodeError, ValueError) as error:
        raise ValueError('limits: malformed JSON output') from error


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def same(left, right, reason):
    require(canonical(left) == canonical(right), reason)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def fixture_contract(root=FIXTURE):
    """Pinned allocator bytes and a fixture contract that repeats every module constant."""
    root = Path(root)
    script = driver.regular(root / 'allocate.sh', LIMIT)
    require(len(script) == ALLOCATOR_BYTES and sha256(script) == ALLOCATOR_SHA256, 'allocator fixture bytes differ from pin')
    contract = parse(driver.regular(root / 'fixture.json', LIMIT))
    require(type(contract) is dict and contract.get('schema_version') == 1, 'limits fixture contract shape')
    allocator = contract['allocator']
    require(allocator['path'] == 'allocate.sh' and allocator['sha256'] == ALLOCATOR_SHA256 and
            allocator['bytes'] == ALLOCATOR_BYTES and allocator['interpreter'] == [BUSYBOX, 'sh', '-c'] and
            allocator['survivor_exit_code'] == SURVIVOR_EXIT_CODE and
            allocator['chunk_bytes'] * 2 ** allocator['doublings'] == allocator['target_bytes'] and
            allocator['target_bytes'] > MEMORY_BYTES, 'allocator contract differs from pin')
    limits = contract['resource_limits']
    require(limits['memory_bytes'] == MEMORY_BYTES and limits['cpus'] == CPUS and limits['pids_limit'] == PIDS_LIMIT and
            limits['cgroup_files'] == list(CGROUP_FILES) and limits['limited'] == LIMITED and limits['control'] == CONTROL and
            limits['sleep_seconds'] == SLEEP_SECONDS, 'resource limit contract differs from pin')
    oom = contract['oom']
    require(oom['memory_limit_bytes'] == MEMORY_BYTES and oom['oom_killed'] is True and
            oom['container_exit_code'] == OOM_EXIT_CODE and oom['events'] == list(EVENTS) and
            oom['die_exit_code'] == str(OOM_EXIT_CODE) and
            all(oom[key] == value for key, value in HEALTH.items()), 'OOM contract differs from pin')
    return {'script': script.decode('ascii'), 'contract': contract, 'script_sha256': ALLOCATOR_SHA256,
            'contract_sha256': sha256(driver.regular(root / 'fixture.json', LIMIT))}


def parse_cgroup(raw, stderr, expected):
    require(stderr == b'', 'cgroup read emitted stderr')
    require(type(raw) is bytes and raw.endswith(b'\n'), 'cgroup read incomplete')
    lines = raw.decode('ascii').split('\n')[:-1]
    require(len(lines) == len(CGROUP_FILES), 'cgroup read line count differs')
    observed = {path.rsplit('/', 1)[1]: line for path, line in zip(CGROUP_FILES, lines)}
    require(observed == expected, 'effective cgroup limits differ: ' + repr(observed))
    return observed


def parse_wait(raw, stderr):
    require(stderr == b'' and re.fullmatch(rb'[0-9]{1,3}\n', raw or b''), 'docker wait output')
    return int(raw.decode('ascii'))


def parse_progress(raw):
    """Allocator stdout: monotonic doubling reports that never reach the survivor exit."""
    require(type(raw) is bytes and len(raw) <= LIMIT, 'bounded allocator log required')
    lines = raw.decode('ascii').split('\n')
    require(lines[-1] == '', 'allocator log incomplete line')
    steps = []
    for line in lines[:-1]:
        match = re.fullmatch(r'VZ_ALLOC step=([0-9]|10|11) bytes=([1-9][0-9]*)', line)
        require(match is not None, 'unknown allocator log line: ' + repr(line))
        step, size = int(match[1]), int(match[2])
        require(step == len(steps) and size == 1048576 * 2 ** step, 'allocator progress out of order')
        steps.append(size)
    require(steps and steps[0] == 1048576 and len(steps) <= 11, 'allocator never reported or survived every doubling')
    return {'reported_steps': len(steps) - 1, 'last_reported_bytes': steps[-1]}


def timestamp(unix_ns):
    require(type(unix_ns) is int and unix_ns > 0, 'timestamp')
    return str(unix_ns // 10 ** 9) + '.' + str(unix_ns % 10 ** 9).zfill(9)


def parse_events(raw, stderr, container_id):
    require(stderr == b'', 'events emitted stderr')
    require(type(raw) is bytes and raw.endswith(b'\n'), 'events stream incomplete')
    rows = [parse(line + b'\n') for line in raw.split(b'\n')[:-1]]
    actions = []
    for row in rows:
        require(type(row) is dict and row.get('Type') == 'container' and type(row.get('Actor')) is dict and
                row['Actor'].get('ID') == container_id, 'foreign event in filtered stream')
        actions.append(row['Action'])
    require(actions.count('oom') >= 1 and actions.count('die') == 1 and actions.count('create') == 1 and
            actions.count('start') == 1 and actions.index('start') < actions.index('oom') < actions.index('die'),
            'OOM event sequence differs: ' + repr(actions))
    die = next(row for row in rows if row['Action'] == 'die')
    require(die['Actor'].get('Attributes', {}).get('exitCode') == str(OOM_EXIT_CODE), 'die event exit code differs')
    return {'actions': actions, 'die_exit_code': str(OOM_EXIT_CODE), 'event_count': len(rows)}


def image_baseline(harness, descriptor, label):
    raw, stderr, _ = harness.docker(label, descriptor, ['image', 'ls', '--all', '--quiet', '--no-trunc'])
    require(stderr == b'', 'image inventory emitted stderr')
    ids = raw.decode('ascii').split()
    require(all(re.fullmatch(r'sha256:[0-9a-f]{64}', item) for item in ids), 'image inventory shape')
    return sorted(set(ids))


def sentinel_image(harness, descriptor):
    rows = [row for row in harness.owned if row.get('kind') == 'sentinel' and row.get('descriptor') == descriptor]
    require(len(rows) == 1 and re.fullmatch(r'sha256:[0-9a-f]{64}', rows[0].get('image_id') or ''),
            'exact owned sentinel image required')
    return rows[0]['image_id']


class Session:
    """Owned containers of one Machine; never cleans up on its own."""
    def __init__(self, harness, descriptor, image_id, token):
        self.harness, self.descriptor, self.image_id, self.token = harness, descriptor, image_id, token
        self.names = {role: token + '-' + role for role in ('limited', 'control', 'oom')}
        self.ids = {}
        self.cleanup_complete = False
        self.failed = False

    def run(self, role, args, command, timeout=60):
        name = self.names[role]
        self.harness.exact_absent(self.descriptor, 'container', name)
        raw, stderr, _ = self.harness.mutate('limits-run-' + role, self.descriptor,
            ['run', '--detach', '--network', 'none', '--name', name, '--label', LABEL + '=' + self.token, *args,
             self.image_id, *command], timeout=timeout)
        require(stderr == b'', role + ' run emitted stderr (kernel limit support warning?): ' + repr(stderr))
        self.ids[role] = driver.checked_text(raw.decode().strip(), r'[0-9a-f]{64}', role + ' container ID')
        return self.ids[role]

    def inspect(self, role, label):
        raw, stderr, _ = self.harness.docker(label, self.descriptor, ['container', 'inspect', self.ids[role]])
        items = parse(raw)
        require(stderr == b'' and type(items) is list and len(items) == 1, 'ambiguous inspection')
        item = items[0]
        require(item['Id'] == self.ids[role] and item['Name'] == '/' + self.names[role] and item['Image'] == self.image_id and
                item['Config']['Labels'][LABEL] == self.token and item['HostConfig']['Runtime'] == 'youki' and
                item['HostConfig']['NetworkMode'] == 'none' and item['HostConfig']['CgroupnsMode'] == 'private' and
                item['RestartCount'] == 0, role + ' container identity differs')
        return item

    def cgroup(self, role, label, expected):
        raw, stderr, _ = self.harness.docker(label, self.descriptor,
                                             ['exec', self.ids[role], BUSYBOX, 'cat', *CGROUP_FILES])
        return parse_cgroup(raw, stderr, expected)

    def running(self, role, label):
        item = self.inspect(role, label)
        require(item['State']['Running'] is True and item['State']['Status'] == 'running' and
                item['State']['OOMKilled'] is False and type(item['State']['Pid']) is int and item['State']['Pid'] > 0,
                role + ' container is not running')
        return item

    def remove(self, role, force):
        self.harness.mutate('limits-remove-' + role, self.descriptor,
                            ['container', 'rm', *(['--force'] if force else []), self.ids[role]])
        self.harness.exact_absent(self.descriptor, 'container', self.names[role])


def sibling_snapshot(session, label):
    limited = session.running('limited', label + '-limited')
    control = session.running('control', label + '-control')
    return {'limited': {'cgroup': session.cgroup('limited', label + '-limited-cgroup', LIMITED),
                        'pid': limited['State']['Pid'], 'started_at': limited['State']['StartedAt'],
                        'host_config': {key: limited['HostConfig'][key] for key in ('Memory', 'NanoCpus', 'PidsLimit')}},
            'control': {'cgroup': session.cgroup('control', label + '-control-cgroup', CONTROL),
                        'pid': control['State']['Pid'], 'started_at': control['State']['StartedAt'],
                        'host_config': {key: control['HostConfig'][key] for key in ('Memory', 'NanoCpus', 'PidsLimit')}}}


def run_machine(harness, descriptor, scope, proof, images, index):
    """Limits, OOM under a one-second sibling probe, then exact owned cleanup.

    The caller must already authenticate descriptor/scope/proof through normal
    Up, retain the sentinel monitor and prepare the Python/Compose image that
    the health service runs. No exception path removes a container; a failed
    Session stays registered on `harness.limits_sessions`.
    """
    descriptor, scope, proof, images = copy.deepcopy((descriptor, scope, proof, images))
    require(type(index) is int and 0 <= index < 3, 'bounded limits Machine index required')
    require(descriptor in harness.descriptors, 'unregistered authenticated Machine descriptor')
    same(descriptor['owner'], {key: scope[key] for key in ('project_id', 'environment_id', 'machine_id')},
         'limits Machine owner differs')
    require(descriptor['name'] == scope['docker_context'] and descriptor['endpoint'] == scope['docker_endpoint'] and
            descriptor['engine_id'] == scope['engine_id'] and descriptor['incarnation_id'] == scope['machine_incarnation'],
            'limits Machine routing or incarnation differs')
    require(type(proof) is dict and bool(proof), 'authenticated runtime proof required')
    require(type(images) is dict and re.fullmatch(r'sha256:[0-9a-f]{64}', images.get('compose', {}).get('id') or ''),
            'prepared health image pin required')
    pins = {name: harness.info['inputs'][name] for name in required_source_paths()}
    verify_sources(pins)
    fixture = fixture_contract()
    require(not harness.effects_uncertain, 'uncertain earlier mutation prevents limits dispatch')
    sessions = getattr(harness, 'limits_sessions', None)
    if sessions is None:
        sessions = harness.limits_sessions = []
    require(len(sessions) == index and all(item.cleanup_complete is True for item in sessions),
            'earlier limits Session lacks completed cleanup')
    output = harness.evidence / ('limits-machine-' + str(index))
    require(not os.path.lexists(output), 'limits Machine evidence directory preexists')
    image_id = sentinel_image(harness, descriptor)
    harness.monitor.check()
    startup.private(output)
    started = time.time_ns()
    token = 'vzlimits-' + uuid.uuid4().hex[:24]
    session = Session(harness, descriptor, image_id, token)
    sessions.append(session)
    intent = {'schema_version': 1, 'scope': SCOPE, 'descriptor': copy.deepcopy(descriptor),
              'machine_scope': copy.deepcopy(scope), 'source_pins': pins, 'started_unix_ns': started, 'token': token,
              'image_id': image_id, 'allocator_sha256': ALLOCATOR_SHA256, 'fixture_contract_sha256': fixture['contract_sha256'],
              'limits': {'memory_bytes': MEMORY_BYTES, 'cpus': CPUS, 'pids_limit': PIDS_LIMIT, 'expected': LIMITED,
                         'control': CONTROL}, 'oom': {'memory_limit_bytes': MEMORY_BYTES, 'exit_code': OOM_EXIT_CODE, **HEALTH},
              'swap_limit_scope': 'CONFIG_SWAP=n_developer_kernel_no_memory_swap_request',
              'health_image_scope': 'prepared_python_compose_image_used_only_by_sibling_health_service'}
    startup.document(output / 'limits-machine.intent.json', intent)
    try:
        health = parallel_health.Health(harness, descriptor, images, index)
        health.prepare()
        baseline = image_baseline(harness, descriptor, 'limits-image-baseline')
        require(image_id in baseline and images['compose']['id'] in baseline, 'baseline lacks owned images')
        session.run('limited', ['--memory', str(MEMORY_BYTES), '--cpus', CPUS, '--pids-limit', str(PIDS_LIMIT)],
                    [BUSYBOX, 'sleep', str(SLEEP_SECONDS)])
        session.run('control', [], [BUSYBOX, 'sleep', str(SLEEP_SECONDS)])
        before = sibling_snapshot(session, 'limits-before')
        require(before['limited']['host_config'] == {'Memory': MEMORY_BYTES, 'NanoCpus': 1_000_000_000, 'PidsLimit': PIDS_LIMIT} and
                before['control']['host_config'] == {'Memory': 0, 'NanoCpus': 0, 'PidsLimit': None},
                'HostConfig limits differ from request')
        health.start()
        health_started = time.time_ns()
        # Envelopes handed to the health validator are host-clock brackets of
        # commands dispatched after the OOM container started, each of which
        # begins at least two Engine round trips after the first health sample.
        envelopes = []
        def bracket(call, *args, **kwargs):
            begin = time.time_ns()
            value = call(*args, **kwargs)
            envelopes.append([begin, time.time_ns()])
            return value
        events_since = time.time_ns() - EVENT_MARGIN_NS
        session.run('oom', ['--memory', str(MEMORY_BYTES)], [BUSYBOX, 'sh', '-c', fixture['script']])
        raw, stderr, _ = bracket(harness.docker, 'limits-oom-wait', descriptor, ['wait', session.ids['oom']], timeout=60)
        exit_code = parse_wait(raw, stderr)
        require(exit_code == OOM_EXIT_CODE, 'allocator exit differs from OOM kill: ' + repr(exit_code) +
                (' (survivor: limit not enforced)' if exit_code == SURVIVOR_EXIT_CODE else ''))
        item = bracket(session.inspect, 'oom', 'limits-oom-inspect')
        state = item['State']
        require(state['OOMKilled'] is True and state['ExitCode'] == OOM_EXIT_CODE and state['Status'] == 'exited' and
                state['Running'] is False and item['HostConfig']['Memory'] == MEMORY_BYTES, 'OOM container state differs')
        raw, stderr, _ = harness.docker('limits-oom-logs', descriptor, ['logs', session.ids['oom']])
        require(stderr == b'', 'allocator emitted stderr')
        progress = parse_progress(raw)
        events_until = time.time_ns() + EVENT_MARGIN_NS
        raw, stderr, _ = bracket(harness.docker, 'limits-oom-events', descriptor,
                                 ['events', '--filter', 'container=' + session.ids['oom'], '--since', timestamp(events_since),
                                  '--until', timestamp(events_until), '--format', '{{json .}}'], timeout=30)
        events = parse_events(raw, stderr, session.ids['oom'])
        during = bracket(sibling_snapshot, session, 'limits-during')
        require(during == before, 'sibling limits or identity changed during OOM')
        require(len(envelopes) == 4 and all(health_started < begin < end for begin, end in envelopes), 'envelopes')
        health_proof = health.finish(envelopes)
        require(health_proof['samples'] == HEALTH['sibling_health_probe_seconds'] and health_proof['sample_errors'] == 0 and
                health_proof['missed_deadlines'] == 0 and health_proof['timing']['interval_ns'] == 10 ** 9,
                'sibling health proof differs from contract')
        after = sibling_snapshot(session, 'limits-after')
        require(after == before, 'sibling limits or identity changed after OOM')
        harness.monitor.check()
        workload = {'limited_id': session.ids['limited'], 'control_id': session.ids['control'], 'oom_id': session.ids['oom'],
                    'before': before, 'during': during, 'after': after, 'oom_exit_code': exit_code,
                    'oom_killed': True, 'oom_state': {key: state[key] for key in ('OOMKilled', 'ExitCode', 'Status', 'Running')},
                    'allocator_progress': progress, 'events': events, 'sibling_health': health_proof,
                    'image_baseline': baseline}
        startup.document(output / 'workload.json', workload)
        # Only a complete workload admits the exact owned cleanup.
        session.remove('oom', force=False)
        session.remove('limited', force=True)
        session.remove('control', force=True)
        final = image_baseline(harness, descriptor, 'limits-image-final')
        require(final == baseline, 'image inventory changed by limits workload')
        session.cleanup_complete = True
    except BaseException:
        session.failed = True
        raise
    verify_sources(pins)
    result = {'schema_version': 1, 'scope': SCOPE, 'machine_scope': copy.deepcopy(scope), 'index': index,
              'started_unix_ns': started, 'ended_unix_ns': time.time_ns(), 'source_pins': pins, 'token': token,
              'workload': workload, 'cleanup': {'containers_removed': sorted(session.names.values()),
              'image_baseline_restored': True, 'health_service_owned_by_harness': health.container_id},
              'scenarios': {'docker.operation.resource_limits': 'dev_observed_not_release_certified',
                            'docker.operation.oom': 'dev_observed_not_release_certified'},
              'swap_limit_scope': intent['swap_limit_scope'], 'test_case_retries': 0,
              'docker_parity_certified': False, 'release_certified': False}
    startup.document(output / 'machine-limits-validation.json', result)
    same(parse(driver.regular(output / 'machine-limits-validation.json', LIMIT)), result, 'retained limits result differs')
    return result
